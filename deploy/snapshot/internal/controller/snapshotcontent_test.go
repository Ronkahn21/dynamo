// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshottypes "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

// fakeCheckpointer records calls behind the checkpointFn seam and returns a configured error.
type fakeCheckpointer struct {
	mu     sync.Mutex
	called bool
	params CheckpointParams
	err    error
}

// fn is the checkpointFn seam the NodeController invokes for the dump.
func (fc *fakeCheckpointer) fn(_ context.Context, params CheckpointParams) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.called = true
	fc.params = params
	return fc.err
}

// wasCalled reports whether the seam was invoked.
func (fc *fakeCheckpointer) wasCalled() bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.called
}

// lastParams returns the params from the most recent seam invocation.
func (fc *fakeCheckpointer) lastParams() CheckpointParams {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.params
}

// contentScheme builds a scheme with the SnapshotContent and core types registered.
func contentScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, nvidiacomv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

// makeNodeController builds a NodeController wired to a fake typed client, runtime, and seam.
func makeNodeController(t *testing.T, fc *fakeCheckpointer, objs ...client.Object) *NodeController {
	t.Helper()
	s := contentScheme(t)
	w := &NodeController{
		config:    &snapshottypes.AgentConfig{NodeName: "node-a", Storage: snapshottypes.StorageSpec{Type: "pvc", BasePath: t.TempDir()}},
		clientset: k8sfake.NewClientset(),
		client: crfake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
			WithStatusSubresource(&nvidiacomv1alpha1.SnapshotContent{}).Build(),
		runtime:  &fakeRuntime{},
		log:      logr.Discard(),
		holderID: "snapshot-agent/test",
		inFlight: make(map[string]struct{}),
	}
	w.checkpointFn = fc.fn
	return w
}

// makeWorkOrder builds a SnapshotContent work order pinned to a node and checkpoint id.
// Capture parameters now live on the source pod, so the work order carries only the node
// label and spec.
func makeWorkOrder(name, node, checkpointID string) *nvidiacomv1alpha1.SnapshotContent {
	return &nvidiacomv1alpha1.SnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{snapshotprotocol.SnapshotNodeLabel: node},
		},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: "snapshot-" + checkpointID},
			Source:      nvidiacomv1alpha1.SnapshotContentSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0", UID: types.UID("pod-uid")}, NodeName: node},
		},
	}
}

// makeSourcePod builds a ready source pod that carries the capture parameters the agent reads:
// the checkpoint-id label, the target-container annotation, and the storage/version annotations
// checkpointLocationsFromPod needs.
func makeSourcePod(checkpointID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-0",
			Namespace: "inference",
			UID:       types.UID("pod-uid"),
			Labels:    map[string]string{snapshotprotocol.CheckpointIDLabel: checkpointID},
			Annotations: map[string]string{
				snapshotprotocol.TargetContainersAnnotation:          "main",
				snapshotprotocol.CheckpointArtifactVersionAnnotation: "1",
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "main", Ready: true, ContainerID: "containerd://abc123"},
			},
		},
	}
}

// getContent reads a SnapshotContent back from the fake client.
func getContent(t *testing.T, w *NodeController, name string) *nvidiacomv1alpha1.SnapshotContent {
	t.Helper()
	c := &nvidiacomv1alpha1.SnapshotContent{}
	require.NoError(t, w.client.Get(context.Background(), types.NamespacedName{Name: name}, c))
	return c
}

func TestReconcileSnapshotContent_IgnoresOtherNode(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-b", "x")
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	assert.False(t, fc.wasCalled())
	got := getContent(t, w, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestReconcileSnapshotContent_InFlightGuard(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	w := makeNodeController(t, &fakeCheckpointer{}, content, pod)
	// Pre-mark the work order in-flight; the reconcile must short-circuit.
	w.inFlight["snapshotcontent-x"] = struct{}{}

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestReconcileSnapshotContent_MissingCheckpointIDFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := makeSourcePod("x")
	delete(pod.Labels, snapshotprotocol.CheckpointIDLabel)
	w := makeNodeController(t, &fakeCheckpointer{}, content, pod)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "MissingCheckpointID", cond.Reason)
}

func TestReconcileSnapshotContent_CheckpointIDMismatchFails(t *testing.T) {
	// Work order name embeds "abc" but the source pod label says "xyz".
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	pod := makeSourcePod("xyz")
	w := makeNodeController(t, &fakeCheckpointer{}, content, pod)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "CheckpointIDMismatch", cond.Reason)
}

func TestReconcileSnapshotContent_ResumeWritesReady(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	pod := makeSourcePod("abc")
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content, pod)
	w.runtime = &fakeRuntime{resolveContainerPID: 4242}
	// Pre-create the artifact directory at the resolved destination so the resume check fires.
	dest := filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1")
	require.NoError(t, os.MkdirAll(dest, 0o755))

	w.reconcileSnapshotContent(context.Background(), content.Name)
	assert.False(t, fc.wasCalled())
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	require.NotNil(t, cond)
}

func TestReconcileSnapshotContent_PodMountResolvesContainerPID(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	pod := makeSourcePod("abc")
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content, pod)
	w.config.Storage.AccessMode = snapshottypes.StorageAccessModePodMount
	rt := &fakeRuntime{resolveContainerPID: 4242}
	w.runtime = rt

	w.reconcileSnapshotContent(context.Background(), content.Name)

	// podMount mode resolves the container PID and feeds it through checkpointLocationsFromPod
	// (a zero PID would fail there with a different reason). The subsequent live-PID validation
	// fails in a unit test because /host/proc/<pid> does not exist, which proves the non-zero
	// PID flowed through to validatePodMountContainerPID.
	assert.Contains(t, rt.resolvedContainerIDs, "abc123")
	assert.False(t, fc.wasCalled())
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "ContainerChanged", cond.Reason)
}

func TestReconcileSnapshotContent_PodNotFoundNoOp(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	w := makeNodeController(t, &fakeCheckpointer{}, content) // no pod

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestReconcileSnapshotContent_StalePodUIDFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("different-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	w := makeNodeController(t, &fakeCheckpointer{}, content, pod)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "StalePodReference", cond.Reason)
}

func TestReconcileSnapshotContent_PodFailedFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed},
	}
	w := makeNodeController(t, &fakeCheckpointer{}, content, pod)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "SourcePodGone", cond.Reason)
}

func TestReconcileSnapshotContent_NotReadyQuiesceNoOp(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := makeSourcePod("x")
	pod.Status.ContainerStatuses[0].Ready = false
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content, pod)

	w.reconcileSnapshotContent(context.Background(), content.Name)
	assert.False(t, fc.wasCalled())
	got := getContent(t, w, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestReconcileSnapshotContent_CapturesFromPod(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	pod := makeSourcePod("abc")
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content, pod)
	w.runtime = &fakeRuntime{resolveContainerPID: 7}

	w.reconcileSnapshotContent(context.Background(), content.Name)
	require.Eventually(t, fc.wasCalled, time.Second, 5*time.Millisecond)

	// Capture parameters are read from the source pod, not from SnapshotContent metadata.
	params := fc.lastParams()
	assert.Equal(t, "abc", params.CheckpointID)
	assert.Equal(t, "main", params.ContainerName)
	assert.Equal(t, "abc123", params.ContainerID)
	assert.Equal(t, 7, params.ContainerPID)
	// agentMount: HostPath == ContainerPath == resolved destination.
	dest := filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1")
	assert.Equal(t, dest, params.HostPath)
	assert.Equal(t, dest, params.ContainerPath)

	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	require.NotNil(t, cond)
}

func TestRunCheckpoint_WritesReadyOnSuccess(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	fc := &fakeCheckpointer{}
	w := makeNodeController(t, fc, content)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")}}
	leaseKey := client.ObjectKey{Namespace: "inference", Name: content.Name}
	loc := checkpointLocations{
		HostPath:      filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1"),
		ContainerPath: filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1"),
	}

	w.runCheckpoint(context.Background(), content, pod, "main", "abc123", 7, "abc", loc, leaseKey, "snapshotcontent-abc")

	assert.True(t, fc.wasCalled())
	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	require.NotNil(t, cond)
}

func TestRunCheckpoint_WritesFailedOnError(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	fc := &fakeCheckpointer{err: errors.New("criu boom")}
	w := makeNodeController(t, fc, content)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")}}
	leaseKey := client.ObjectKey{Namespace: "inference", Name: content.Name}
	loc := checkpointLocations{
		HostPath:      filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1"),
		ContainerPath: filepath.Join(w.config.Storage.BasePath, "abc", "versions", "1"),
	}

	w.runCheckpoint(context.Background(), content, pod, "main", "abc123", 7, "abc", loc, leaseKey, "snapshotcontent-abc")

	got := getContent(t, w, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "CheckpointFailed", cond.Reason)
}
