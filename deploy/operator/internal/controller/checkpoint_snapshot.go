// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// checkpointSnapshotFieldManager is the Server-Side Apply field owner for Snapshots.
const checkpointSnapshotFieldManager = "dynamo-checkpoint-controller"

// snapshotName returns the deterministic Snapshot name for a checkpoint ID.
func snapshotName(checkpointID string) string {
	return "snapshot-" + checkpointID
}

// findSourcePod returns the checkpoint Job's pod, or a NotFound error if the Job has not
// created it yet (callers use client.IgnoreNotFound to requeue).
func (r *CheckpointReconciler) findSourcePod(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.JobNameLabel: job.Name},
	); err != nil {
		return nil, err
	}
	for i := range pods.Items {
		if metav1.IsControlledBy(&pods.Items[i], job) {
			return &pods.Items[i], nil
		}
	}
	return nil, apierrors.NewNotFound(corev1.Resource("pods"), job.Name)
}

// ensureSnapshot creates this checkpoint's Snapshot (owned by ckpt) via Server-Side Apply when
// absent, and is a no-op when it already exists and is ours.
func (r *CheckpointReconciler) ensureSnapshot(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, checkpointID, sourcePodName string) error {
	owned, err := r.findOwnedSnapshot(ctx, ckpt, snapshotName(checkpointID))
	if err != nil {
		return err
	}
	if owned {
		return nil
	}
	return r.applySnapshot(ctx, ckpt, buildSnapshot(ckpt, checkpointID, sourcePodName))
}

// findOwnedSnapshot reports whether this checkpoint's Snapshot already exists and is owned by
// ckpt. It returns a terminal Forbidden error (and emits an event) when a Snapshot with the same
// name exists but is owned by another controller; (false, nil) means none exists yet.
func (r *CheckpointReconciler) findOwnedSnapshot(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, name string) (bool, error) {
	existing := &nvidiacomv1alpha1.Snapshot{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ckpt.Namespace, Name: name}, existing); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if metav1.IsControlledBy(existing, ckpt) {
		return true, nil
	}
	// Forbidden is terminal (see controller_common.IgnoreIntermediateError): a foreign-owned
	// name collision will not resolve on retry.
	conflict := apierrors.NewForbidden(
		nvidiacomv1alpha1.GroupVersion.WithResource("snapshots").GroupResource(),
		name,
		fmt.Errorf("exists but is not owned by checkpoint %q", ckpt.Name),
	)
	r.Recorder.Event(ckpt, corev1.EventTypeWarning, "SnapshotCreateFailed", conflict.Error())
	return false, conflict
}

// buildSnapshot constructs the desired Snapshot for a checkpoint.
func buildSnapshot(ckpt *nvidiacomv1alpha1.DynamoCheckpoint, checkpointID, sourcePodName string) *nvidiacomv1alpha1.Snapshot {
	return &nvidiacomv1alpha1.Snapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nvidiacomv1alpha1.GroupVersion.String(),
			Kind:       "Snapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName(checkpointID),
			Namespace: ckpt.Namespace,
			Labels:    map[string]string{snapshotprotocol.CheckpointIDLabel: checkpointID},
		},
		Spec: nvidiacomv1alpha1.SnapshotSpec{
			CheckpointID: checkpointID,
			Source: nvidiacomv1alpha1.SnapshotSource{
				PodRef: nvidiacomv1alpha1.PodReference{Name: sourcePodName},
			},
		},
	}
}

// applySnapshot sets ckpt as controller owner and applies the Snapshot via Server-Side Apply,
// emitting an event on success or failure.
func (r *CheckpointReconciler) applySnapshot(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, snap *nvidiacomv1alpha1.Snapshot) error {
	if err := ctrl.SetControllerReference(ckpt, snap, r.Scheme()); err != nil {
		return err
	}
	if err := r.Patch(ctx, snap, client.Apply,
		client.FieldOwner(checkpointSnapshotFieldManager), client.ForceOwnership); err != nil {
		r.Recorder.Event(ckpt, corev1.EventTypeWarning, "SnapshotCreateFailed", err.Error())
		return err
	}
	r.Recorder.Eventf(ckpt, corev1.EventTypeNormal, "SnapshotCreated", "Created Snapshot %s", snap.Name)
	return nil
}

// updateFailedStatus marks the checkpoint Failed after a terminal Snapshot error. The failure
// event is emitted at the point of failure in ensureSnapshot; this records status only and does
// not stomp the JobCreated condition (the Job was created; only the Snapshot failed).
func (r *CheckpointReconciler) updateFailedStatus(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, err error) {
	ckpt.Status.Phase = nvidiacomv1alpha1.DynamoCheckpointPhaseFailed
	ckpt.Status.Message = fmt.Sprintf("snapshot creation failed: %v", err)
	if uerr := r.Status().Update(ctx, ckpt); uerr != nil {
		log.FromContext(ctx).Error(uerr, "failed to update DynamoCheckpoint status after snapshot failure")
	}
}
