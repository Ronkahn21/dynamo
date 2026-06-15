/*
 * SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

func snapshotReconcilerScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = nvidiacomv1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func makeSnapshotReconciler(s *runtime.Scheme, objs ...client.Object) *SnapshotReconciler {
	return &SnapshotReconciler{
		Client: fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
			WithStatusSubresource(&nvidiacomv1alpha1.Snapshot{}, &nvidiacomv1alpha1.SnapshotContent{}).Build(),
		Recorder: record.NewFakeRecorder(10),
	}
}

func makeSnapshotForReconcile(checkpointID, podName string) *nvidiacomv1alpha1.Snapshot {
	return &nvidiacomv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "snapshot-" + checkpointID,
			Namespace:   "inference",
			UID:         types.UID("snap-uid"),
			Finalizers:  []string{snapshotFinalizer},
			Annotations: map[string]string{snapshotprotocol.CheckpointArtifactVersionAnnotation: "3"},
		},
		Spec: nvidiacomv1alpha1.SnapshotSpec{
			CheckpointID: checkpointID,
			Source:       nvidiacomv1alpha1.SnapshotSource{PodRef: nvidiacomv1alpha1.PodReference{Name: podName}},
		},
	}
}

func scheduledPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "inference", UID: types.UID("pod-uid-9")},
		Spec:       corev1.PodSpec{NodeName: node},
	}
}

func reconcileSnapshot(t *testing.T, r *SnapshotReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "inference", Name: name}})
	require.NoError(t, err)
	return res
}

func TestSnapshotReconciler_PodUnscheduledBacksOff(t *testing.T) {
	s := snapshotReconcilerScheme()
	snap := makeSnapshotForReconcile("abc123", "worker-0")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-0", Namespace: "inference"}}
	r := makeSnapshotReconciler(s, snap, pod)

	res := reconcileSnapshot(t, r, snap.Name)
	assert.Positive(t, res.RequeueAfter)

	var contents nvidiacomv1alpha1.SnapshotContentList
	require.NoError(t, r.List(context.Background(), &contents))
	assert.Empty(t, contents.Items)
}

func TestSnapshotReconciler_BuildsWorkOrderAndBinds(t *testing.T) {
	s := snapshotReconcilerScheme()
	snap := makeSnapshotForReconcile("abc123", "worker-0")
	r := makeSnapshotReconciler(s, snap, scheduledPod("worker-0", "node-a"))

	reconcileSnapshot(t, r, snap.Name)

	content := &nvidiacomv1alpha1.SnapshotContent{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "snapshotcontent-abc123"}, content))
	assert.Equal(t, "worker-0", content.Spec.Source.PodRef.Name)
	assert.Equal(t, types.UID("pod-uid-9"), content.Spec.Source.PodRef.UID)
	assert.Equal(t, "node-a", content.Spec.Source.NodeName)
	assert.Equal(t, "node-a", content.Labels[snapshotprotocol.SnapshotNodeLabel])
	assert.NotContains(t, content.Labels, snapshotprotocol.CheckpointIDLabel)
	assert.NotContains(t, content.Annotations, snapshotprotocol.CheckpointArtifactVersionAnnotation)
	assert.Empty(t, content.Finalizers)
	assert.Equal(t, "inference", content.Spec.SnapshotRef.Namespace)
	assert.Equal(t, snap.Name, content.Spec.SnapshotRef.Name)

	updated := &nvidiacomv1alpha1.Snapshot{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "inference", Name: snap.Name}, updated))
	require.NotNil(t, updated.Status.BoundSnapshotContentName)
	assert.Equal(t, "snapshotcontent-abc123", *updated.Status.BoundSnapshotContentName)
}

func TestSnapshotReconciler_MirrorsReadyAndFailed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		condType  string
		wantReady metav1.ConditionStatus
	}{
		{name: "ready", condType: nvidiacomv1alpha1.SnapshotConditionReady},
		{name: "failed", condType: nvidiacomv1alpha1.SnapshotConditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := snapshotReconcilerScheme()
			snap := makeSnapshotForReconcile("abc123", "worker-0")
			content := &nvidiacomv1alpha1.SnapshotContent{
				ObjectMeta: metav1.ObjectMeta{Name: "snapshotcontent-abc123", Finalizers: []string{snapshotFinalizer}},
				Spec: nvidiacomv1alpha1.SnapshotContentSpec{
					SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: snap.Name},
					Source: nvidiacomv1alpha1.SnapshotContentSource{
						PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0"}, NodeName: "node-a",
					},
				},
				Status: nvidiacomv1alpha1.SnapshotContentStatus{
					Conditions: []metav1.Condition{{Type: tc.condType, Status: metav1.ConditionTrue, Reason: "Agent", Message: "done"}},
				},
			}
			r := makeSnapshotReconciler(s, snap, content, scheduledPod("worker-0", "node-a"))

			reconcileSnapshot(t, r, snap.Name)

			updated := &nvidiacomv1alpha1.Snapshot{}
			require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "inference", Name: snap.Name}, updated))
			cond := meta.FindStatusCondition(updated.Status.Conditions, tc.condType)
			require.NotNil(t, cond)
			assert.Equal(t, metav1.ConditionTrue, cond.Status)
		})
	}
}

func TestSnapshotReconciler_RescheduleFailsSnapshot(t *testing.T) {
	s := snapshotReconcilerScheme()
	snap := makeSnapshotForReconcile("abc123", "worker-0")
	content := &nvidiacomv1alpha1.SnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshotcontent-abc123", Finalizers: []string{snapshotFinalizer}},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: snap.Name},
			Source:      nvidiacomv1alpha1.SnapshotContentSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0"}, NodeName: "node-a"},
		},
	}
	// Pod now runs on a different node than the bound content.
	r := makeSnapshotReconciler(s, snap, content, scheduledPod("worker-0", "node-b"))

	reconcileSnapshot(t, r, snap.Name)

	updated := &nvidiacomv1alpha1.Snapshot{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "inference", Name: snap.Name}, updated))
	cond := meta.FindStatusCondition(updated.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "PodRescheduled", cond.Reason)
}

func TestSnapshotReconciler_ComposedNameTooLongFails(t *testing.T) {
	s := snapshotReconcilerScheme()
	longID := strings.Repeat("a", 250) // "snapshotcontent-" + 250 = 266 > 253
	snap := &nvidiacomv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshot-x", Namespace: "inference", Finalizers: []string{snapshotFinalizer}},
		Spec: nvidiacomv1alpha1.SnapshotSpec{
			CheckpointID: longID,
			Source:       nvidiacomv1alpha1.SnapshotSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0"}},
		},
	}
	r := makeSnapshotReconciler(s, snap, scheduledPod("worker-0", "node-a"))

	reconcileSnapshot(t, r, snap.Name)

	updated := &nvidiacomv1alpha1.Snapshot{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "inference", Name: snap.Name}, updated))
	cond := meta.FindStatusCondition(updated.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	require.NotNil(t, cond)
	assert.Equal(t, "InvalidContentName", cond.Reason)
}

func TestSnapshotReconciler_CascadeDelete(t *testing.T) {
	s := snapshotReconcilerScheme()
	now := metav1.Now()
	snap := makeSnapshotForReconcile("abc123", "worker-0")
	snap.DeletionTimestamp = &now
	content := &nvidiacomv1alpha1.SnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshotcontent-abc123"},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: snap.Name},
			Source:      nvidiacomv1alpha1.SnapshotContentSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "worker-0"}, NodeName: "node-a"},
		},
	}
	r := makeSnapshotReconciler(s, snap, content)

	// The content carries no finalizer, so it is deleted immediately; one pass deletes
	// the content and, once confirmed gone, drops the Snapshot finalizer.
	reconcileSnapshot(t, r, snap.Name)
	err := r.Get(context.Background(), types.NamespacedName{Name: "snapshotcontent-abc123"}, &nvidiacomv1alpha1.SnapshotContent{})
	assert.True(t, apierrors.IsNotFound(err))

	gone := &nvidiacomv1alpha1.Snapshot{}
	err = r.Get(context.Background(), types.NamespacedName{Namespace: "inference", Name: snap.Name}, gone)
	if err == nil {
		assert.False(t, controllerutil.ContainsFinalizer(gone, snapshotFinalizer))
	} else {
		assert.True(t, apierrors.IsNotFound(err))
	}
}

func TestSnapshotContentToSnapshot_UnwrapsTombstone(t *testing.T) {
	content := &nvidiacomv1alpha1.SnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "snapshotcontent-abc123"},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{Namespace: "inference", Name: "snapshot-abc123"},
		},
	}

	direct := snapshotContentToSnapshot(context.Background(), content)
	require.Len(t, direct, 1)
	assert.Equal(t, "snapshot-abc123", direct[0].Name)

	tombstone := cache.DeletedFinalStateUnknown{Key: "snapshotcontent-abc123", Obj: content}
	ref, ok := snapshotRefFromContentObj(tombstone)
	require.True(t, ok)
	assert.Equal(t, "snapshot-abc123", ref.Name)
	assert.Equal(t, "inference", ref.Namespace)
}
