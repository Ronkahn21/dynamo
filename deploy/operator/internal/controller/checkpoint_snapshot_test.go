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
	"testing"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newCheckpointJob(name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: types.UID("job-uid")},
	}
}

func newOwnedPod(podName string, job *batchv1.Job) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: testNamespace,
			Labels:    map[string]string{batchv1.JobNameLabel: job.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       job.Name,
				UID:        job.UID,
				Controller: ptr.To(true),
			}},
		},
	}
}

func newOwnedCheckpoint() *nvidiacomv1alpha1.DynamoCheckpoint {
	ckpt := makeTestCheckpoint(nvidiacomv1alpha1.DynamoCheckpointPhaseCreating)
	ckpt.UID = types.UID("ckpt-uid")
	return ckpt
}

func TestFindSourcePod_ReturnsJobOwnedPod(t *testing.T) {
	job := newCheckpointJob(defaultCheckpointJobName)
	pod := newOwnedPod("worker-xyz", job)
	r := makeCheckpointReconciler(checkpointTestScheme(), job, pod)

	got, err := r.findSourcePod(context.Background(), job)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "worker-xyz", got.Name)
}

func TestFindSourcePod_NotCreatedReturnsNotFound(t *testing.T) {
	job := newCheckpointJob(defaultCheckpointJobName)
	r := makeCheckpointReconciler(checkpointTestScheme(), job)

	got, err := r.findSourcePod(context.Background(), job)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
	assert.Nil(t, got)
	assert.NoError(t, client.IgnoreNotFound(err))
}

func TestFindSourcePod_IgnoresPodNotOwnedByJob(t *testing.T) {
	job := newCheckpointJob(defaultCheckpointJobName)
	other := newOwnedPod("stray", &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: job.Name, UID: types.UID("different-uid")}})
	r := makeCheckpointReconciler(checkpointTestScheme(), job, other)

	_, err := r.findSourcePod(context.Background(), job)
	assert.True(t, apierrors.IsNotFound(err))
}

func TestEnsureSnapshot_CreatesWhenAbsent(t *testing.T) {
	ckpt := newOwnedCheckpoint()
	r := makeCheckpointReconciler(checkpointTestScheme(), ckpt)

	require.NoError(t, r.ensureSnapshot(context.Background(), ckpt, testHash, "worker-xyz"))

	snap := &nvidiacomv1alpha1.Snapshot{}
	require.NoError(t, r.Get(context.Background(),
		client.ObjectKey{Namespace: testNamespace, Name: snapshotName(testHash)}, snap))
	assert.Equal(t, testHash, snap.Spec.CheckpointID)
	assert.Equal(t, "worker-xyz", snap.Spec.Source.PodRef.Name)
	assert.True(t, metav1.IsControlledBy(snap, ckpt), "snapshot must be controlled by the checkpoint")
}

func TestEnsureSnapshot_NoopWhenAlreadyOwned(t *testing.T) {
	ckpt := newOwnedCheckpoint()
	r := makeCheckpointReconciler(checkpointTestScheme(), ckpt)
	require.NoError(t, r.ensureSnapshot(context.Background(), ckpt, testHash, "worker-xyz"))

	// Second call is a no-op (already owned by us).
	require.NoError(t, r.ensureSnapshot(context.Background(), ckpt, testHash, "worker-xyz"))

	var snaps nvidiacomv1alpha1.SnapshotList
	require.NoError(t, r.List(context.Background(), &snaps, client.InNamespace(testNamespace)))
	assert.Len(t, snaps.Items, 1)
}

func TestEnsureSnapshot_ErrorsWhenNotOwned(t *testing.T) {
	ckpt := newOwnedCheckpoint()
	foreign := &nvidiacomv1alpha1.Snapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapshotName(testHash), Namespace: testNamespace},
		Spec: nvidiacomv1alpha1.SnapshotSpec{
			CheckpointID: testHash,
			Source:       nvidiacomv1alpha1.SnapshotSource{PodRef: nvidiacomv1alpha1.PodReference{Name: "someone-else"}},
		},
	}
	r := makeCheckpointReconciler(checkpointTestScheme(), ckpt, foreign)

	err := r.ensureSnapshot(context.Background(), ckpt, testHash, "worker-xyz")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not owned by checkpoint")
	// Must be terminal so failOrRequeueSnapshot fails the capture (no infinite requeue).
	assert.True(t, apierrors.IsForbidden(err))
}

func TestFailOrRequeueSnapshot_TerminalFailsCheckpoint(t *testing.T) {
	ckpt := newOwnedCheckpoint()
	r := makeCheckpointReconciler(checkpointTestScheme(), ckpt)

	terminal := apierrors.NewInvalid(
		schema.GroupKind{Group: "nvidia.com", Kind: "Snapshot"}, "snapshot-x", nil)
	res, err := r.failOrRequeueSnapshot(context.Background(), ckpt, terminal)
	require.NoError(t, err)
	assert.Zero(t, res.RequeueAfter)
	assert.Equal(t, nvidiacomv1alpha1.DynamoCheckpointPhaseFailed, ckpt.Status.Phase)
	assert.Contains(t, ckpt.Status.Message, "snapshot creation failed")
}

func TestFailOrRequeueSnapshot_TransientRequeues(t *testing.T) {
	ckpt := newOwnedCheckpoint()
	r := makeCheckpointReconciler(checkpointTestScheme(), ckpt)

	transient := apierrors.NewConflict(
		schema.GroupResource{Group: "nvidia.com", Resource: "snapshots"}, "snapshot-x", assert.AnError)
	_, err := r.failOrRequeueSnapshot(context.Background(), ckpt, transient)
	require.Error(t, err) // returned for requeue/backoff
	assert.NotEqual(t, nvidiacomv1alpha1.DynamoCheckpointPhaseFailed, ckpt.Status.Phase)
}
