// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshottypes "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

// fakeCheckpointer records calls and returns a configured error.
type fakeCheckpointer struct {
	called bool
	err    error
}

func (fc *fakeCheckpointer) Checkpoint(_ context.Context, _ CheckpointParams) error {
	fc.called = true
	return fc.err
}

func contentScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, nvidiacomv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

func makeContentReconciler(t *testing.T, checkpointer NodeCheckpointer, objs ...client.Object) *SnapshotContentReconciler {
	t.Helper()
	s := contentScheme(t)
	return &SnapshotContentReconciler{
		Client: crfake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
			WithStatusSubresource(&nvidiacomv1alpha1.SnapshotContent{}).Build(),
		Clientset:    k8sfake.NewClientset(),
		Config:       &snapshottypes.AgentConfig{NodeName: "node-a", Storage: snapshottypes.StorageSpec{Type: "pvc", BasePath: t.TempDir()}},
		NodeName:     "node-a",
		HolderID:     "snapshot-agent/test",
		Checkpointer: checkpointer,
		inFlight:     make(map[string]struct{}),
	}
}

func makeWorkOrder(name, node, checkpointID string) *nvidiacomv1alpha1.SnapshotContent {
	return &nvidiacomv1alpha1.SnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      map[string]string{snapshotprotocol.CheckpointIDLabel: checkpointID},
			Annotations: map[string]string{snapshotprotocol.CheckpointArtifactVersionAnnotation: "1"},
		},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: "snapshot-" + checkpointID},
			Source:      nvidiacomv1alpha1.SnapshotContentSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0", UID: types.UID("pod-uid")}, NodeName: node},
		},
	}
}

func reconcileContent(t *testing.T, r *SnapshotContentReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	return res
}

func getContent(t *testing.T, r *SnapshotContentReconciler, name string) *nvidiacomv1alpha1.SnapshotContent {
	t.Helper()
	c := &nvidiacomv1alpha1.SnapshotContent{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: name}, c))
	return c
}

func TestSnapshotContentReconciler_IgnoresOtherNode(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-b", "x")
	fc := &fakeCheckpointer{}
	r := makeContentReconciler(t, fc, content)

	reconcileContent(t, r, content.Name)
	assert.False(t, fc.called)
	got := getContent(t, r, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestSnapshotContentReconciler_InFlightGuard(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	r := makeContentReconciler(t, &fakeCheckpointer{}, content, pod)
	// Pre-mark the work order in-flight; the reconcile must short-circuit.
	r.inFlight["/snapshotcontent-x"] = struct{}{}

	res := reconcileContent(t, r, content.Name)
	assert.Zero(t, res.RequeueAfter)
	got := getContent(t, r, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestSnapshotContentReconciler_MissingCheckpointIDFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	delete(content.Labels, snapshotprotocol.CheckpointIDLabel)
	r := makeContentReconciler(t, &fakeCheckpointer{}, content)

	reconcileContent(t, r, content.Name)
	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "MissingStorageCoords", cond.Reason)
}

func TestSnapshotContentReconciler_ResumeWritesReady(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	r := makeContentReconciler(t, &fakeCheckpointer{}, content)
	// Pre-create the artifact directory at the resolved destination.
	dest := filepath.Join(r.Config.Storage.BasePath, "abc", "versions", "1")
	require.NoError(t, os.MkdirAll(dest, 0o755))

	reconcileContent(t, r, content.Name)
	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	require.NotNil(t, cond)
}

func TestSnapshotContentReconciler_PodNotFoundBacksOff(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	r := makeContentReconciler(t, &fakeCheckpointer{}, content) // no pod

	res := reconcileContent(t, r, content.Name)
	assert.Positive(t, res.RequeueAfter)
	got := getContent(t, r, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestSnapshotContentReconciler_StalePodUIDFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("different-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	r := makeContentReconciler(t, &fakeCheckpointer{}, content, pod)

	reconcileContent(t, r, content.Name)
	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "StalePodReference", cond.Reason)
}

func TestSnapshotContentReconciler_PodFailedFails(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed},
	}
	r := makeContentReconciler(t, &fakeCheckpointer{}, content, pod)

	reconcileContent(t, r, content.Name)
	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "SourcePodGone", cond.Reason)
}

func TestSnapshotContentReconciler_NotReadyQuiesceRequeue(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-x", "node-a", "x")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid"),
			Annotations: map[string]string{snapshotprotocol.TargetContainersAnnotation: "main"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "main", Ready: false}},
		},
	}
	r := makeContentReconciler(t, &fakeCheckpointer{}, content, pod)

	res := reconcileContent(t, r, content.Name)
	assert.Positive(t, res.RequeueAfter)
	got := getContent(t, r, content.Name)
	assert.Empty(t, got.Status.Conditions)
}

func TestSnapshotContentReconciler_RunCheckpointWritesReadyOnSuccess(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	fc := &fakeCheckpointer{}
	r := makeContentReconciler(t, fc, content)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")}}
	leaseKey := client.ObjectKey{Namespace: "inference", Name: content.Name}

	r.runCheckpoint(context.Background(), content, pod, "main", "abc",
		filepath.Join(r.Config.Storage.BasePath, "abc", "versions", "1"), leaseKey, "/snapshotcontent-abc")

	assert.True(t, fc.called)
	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	require.NotNil(t, cond)
}

func TestSnapshotContentReconciler_RunCheckpointWritesFailedOnError(t *testing.T) {
	content := makeWorkOrder("snapshotcontent-abc", "node-a", "abc")
	fc := &fakeCheckpointer{err: errors.New("criu boom")}
	r := makeContentReconciler(t, fc, content)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference", UID: types.UID("pod-uid")}}
	leaseKey := client.ObjectKey{Namespace: "inference", Name: content.Name}

	r.runCheckpoint(context.Background(), content, pod, "main", "abc",
		filepath.Join(r.Config.Storage.BasePath, "abc", "versions", "1"), leaseKey, "/snapshotcontent-abc")

	got := getContent(t, r, content.Name)
	cond := meta.FindStatusCondition(got.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "CheckpointFailed", cond.Reason)
}
