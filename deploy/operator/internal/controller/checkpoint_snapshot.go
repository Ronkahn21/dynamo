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

// ensureSnapshot creates this checkpoint's Snapshot (owned by ckpt) via Server-Side Apply
// when absent, and is a no-op when it already exists and is ours. Errors propagate to the caller.
func (r *CheckpointReconciler) ensureSnapshot(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, checkpointID, sourcePodName string) error {
	if ckpt.UID == "" {
		// An empty owner UID would match any unowned Snapshot in the ownership check.
		return fmt.Errorf("checkpoint %q has no UID; refusing to create an unowned Snapshot", ckpt.Name)
	}
	name := snapshotName(checkpointID)

	existing := &nvidiacomv1alpha1.Snapshot{}
	err := r.Get(ctx, client.ObjectKey{Namespace: ckpt.Namespace, Name: name}, existing)
	if err == nil {
		if metav1.IsControlledBy(existing, ckpt) {
			return nil
		}
		// Forbidden is terminal in failOrRequeueSnapshot: a foreign-owned name collision
		// will not resolve on retry.
		return apierrors.NewForbidden(
			nvidiacomv1alpha1.GroupVersion.WithResource("snapshots").GroupResource(),
			name,
			fmt.Errorf("exists but is not owned by checkpoint %q", ckpt.Name),
		)
	}
	if client.IgnoreNotFound(err) != nil {
		return err
	}

	snap := &nvidiacomv1alpha1.Snapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nvidiacomv1alpha1.GroupVersion.String(),
			Kind:       "Snapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
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
	if err := ctrl.SetControllerReference(ckpt, snap, r.Scheme()); err != nil {
		return err
	}
	if err := r.Patch(ctx, snap, client.Apply,
		client.FieldOwner(checkpointSnapshotFieldManager), client.ForceOwnership); err != nil {
		return err
	}
	r.Recorder.Eventf(ckpt, corev1.EventTypeNormal, "SnapshotCreated", "Created Snapshot %s", name)
	return nil
}

// failOrRequeueSnapshot fails the capture on a terminal error, or returns the error to
// requeue on a transient one.
func (r *CheckpointReconciler) failOrRequeueSnapshot(ctx context.Context, ckpt *nvidiacomv1alpha1.DynamoCheckpoint, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsForbidden(err) {
		logger.Error(err, "Snapshot creation failed terminally; failing checkpoint")
		ckpt.Status.Phase = nvidiacomv1alpha1.DynamoCheckpointPhaseFailed
		ckpt.Status.Message = fmt.Sprintf("snapshot creation failed: %v", err)
		// The Job was created; only the Snapshot failed — leave the JobCreated condition.
		r.Recorder.Event(ckpt, corev1.EventTypeWarning, "SnapshotCreateFailed", err.Error())
		if uerr := r.Status().Update(ctx, ckpt); uerr != nil {
			return ctrl.Result{}, uerr
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{}, err
}
