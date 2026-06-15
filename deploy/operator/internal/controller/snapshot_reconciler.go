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
	"errors"
	"fmt"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

const (
	// snapshotFinalizer is set on the Snapshot so its bound SnapshotContent is
	// deleted before the Snapshot is removed.
	snapshotFinalizer = "nvidia.com/snapshot-content-cleanup"

	// snapshotContentFieldManager is the Server-Side Apply field owner for SnapshotContents.
	snapshotContentFieldManager = "dynamo-snapshot-controller"

	// snapshotPodResolveBackoffBase is the minimum requeue delay while waiting for the
	// source pod to be scheduled; jitter is added on top to avoid a synchronized hot loop.
	snapshotPodResolveBackoffBase = 2 * time.Second

	// snapshotContentDeleteRequeue is the delay between cascade-delete progress checks.
	snapshotContentDeleteRequeue = time.Second

	// maxResourceNameLength is the Kubernetes object name limit (RFC 1123 subdomain).
	maxResourceNameLength = 253
)

// errSnapshotPodUnscheduled signals that the source pod is not yet scheduled and the
// reconcile should retry with backoff rather than fail.
var errSnapshotPodUnscheduled = errors.New("source pod is not yet scheduled to a node")

// SnapshotReconciler reconciles a Snapshot: it creates the bound, cluster-scoped
// SnapshotContent work order for the node agent, mirrors the agent's terminal status
// back to the Snapshot, and cascades deletion to the SnapshotContent.
type SnapshotReconciler struct {
	client.Client
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=nvidia.com,resources=snapshots,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=nvidia.com,resources=snapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nvidia.com,resources=snapshots/finalizers,verbs=update
// +kubebuilder:rbac:groups=nvidia.com,resources=snapshotcontents,verbs=create;get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=nvidia.com,resources=snapshotcontents/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile drives a Snapshot through binding, status mirroring, and cascade deletion.
func (sr *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	snap := &nvidiacomv1alpha1.Snapshot{}
	if err := sr.Get(ctx, req.NamespacedName, snap); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !snap.GetDeletionTimestamp().IsZero() {
		return sr.handleDelete(ctx, snap)
	}

	if !controllerutil.ContainsFinalizer(snap, snapshotFinalizer) {
		controllerutil.AddFinalizer(snap, snapshotFinalizer)
		if err := sr.Update(ctx, snap); err != nil {
			return ctrl.Result{}, fmt.Errorf("add snapshot finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	pod, err := sr.getSourcePod(ctx, snap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Source pod not found, backing off", "snapshot", snap.Name)
			return ctrl.Result{RequeueAfter: jitteredBackoff(snapshotPodResolveBackoffBase)}, nil
		}
		return ctrl.Result{}, err
	}
	if err := validateSourcePod(pod); err != nil {
		logger.V(1).Info("Source pod not ready, backing off", "snapshot", snap.Name, "reason", err.Error())
		return ctrl.Result{RequeueAfter: jitteredBackoff(snapshotPodResolveBackoffBase)}, nil
	}

	// The checkpoint ID is carried as a label (set by the DynamoCheckpoint controller),
	// not a spec field; the agent independently reads it from the source pod.
	id := snap.Labels[snapshotprotocol.CheckpointIDLabel]
	if id == "" {
		return sr.failSnapshot(ctx, snap, "MissingCheckpointID",
			fmt.Errorf("snapshot %q missing %s label", snap.Name, snapshotprotocol.CheckpointIDLabel))
	}
	contentName := snapshotContentName(id)

	content, err := sr.ensureSnapshotContent(ctx, snap, contentName, pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A freshly-created content always matches; only a pre-existing content whose
	// source pod was rescheduled to another node mismatches (spec is immutable).
	if content.Spec.Source.NodeName != pod.Spec.NodeName {
		return sr.failSnapshot(ctx, snap, "PodRescheduled",
			fmt.Errorf("source pod moved from node %q to %q; CRIU checkpoint cannot survive migration",
				content.Spec.Source.NodeName, pod.Spec.NodeName))
	}

	return sr.propagateStatus(ctx, snap, content)
}

// getSourcePod loads the source pod referenced by the Snapshot.
func (sr *SnapshotReconciler) getSourcePod(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: snap.Namespace, Name: snap.Spec.Source.PodRef.Name}
	if err := sr.Get(ctx, key, pod); err != nil {
		return nil, err
	}
	return pod, nil
}

// validateSourcePod requires the pod to be scheduled to a node.
func validateSourcePod(pod *corev1.Pod) error {
	if pod.Spec.NodeName == "" {
		return errSnapshotPodUnscheduled
	}
	return nil
}

// ensureSnapshotContent returns the existing SnapshotContent or, when absent, creates the
// trigger via a single Server-Side Apply carrying the source ref and the node mirror label.
// The returned object is the source of truth for the reschedule guard.
func (sr *SnapshotReconciler) ensureSnapshotContent(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot, contentName string, pod *corev1.Pod) (*nvidiacomv1alpha1.SnapshotContent, error) {
	existing := &nvidiacomv1alpha1.SnapshotContent{}
	if err := sr.Get(ctx, client.ObjectKey{Name: contentName}, existing); err == nil {
		return existing, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	content := sr.buildSnapshotContent(snap, contentName, pod)
	if err := sr.Patch(ctx, content, client.Apply,
		client.FieldOwner(snapshotContentFieldManager), client.ForceOwnership); err != nil {
		sr.Recorder.Event(snap, corev1.EventTypeWarning, "SnapshotContentCreateFailed", err.Error())
		return nil, fmt.Errorf("apply SnapshotContent %q: %w", contentName, err)
	}
	return content, nil
}

// buildSnapshotContent constructs the desired cluster-scoped SnapshotContent for a Snapshot.
func (sr *SnapshotReconciler) buildSnapshotContent(snap *nvidiacomv1alpha1.Snapshot, contentName string, pod *corev1.Pod) *nvidiacomv1alpha1.SnapshotContent {
	return &nvidiacomv1alpha1.SnapshotContent{
		TypeMeta: metav1.TypeMeta{
			APIVersion: nvidiacomv1alpha1.GroupVersion.String(),
			Kind:       "SnapshotContent",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: contentName,
			Labels: map[string]string{
				snapshotprotocol.SnapshotNodeLabel: pod.Spec.NodeName,
			},
		},
		Spec: nvidiacomv1alpha1.SnapshotContentSpec{
			SnapshotRef: nvidiacomv1alpha1.SnapshotReference{
				Namespace: snap.Namespace,
				Name:      snap.Name,
				UID:       snap.UID,
			},
			Source: nvidiacomv1alpha1.SnapshotContentSource{
				PodRef:   nvidiacomv1alpha1.PodReference{Name: pod.Name, UID: pod.UID},
				NodeName: pod.Spec.NodeName,
			},
		},
	}
}

// propagateStatus records the binding and mirrors the SnapshotContent's terminal status to
// the Snapshot, defaulting to a Pending condition until the agent writes a result. It
// receives the content resolved earlier in the reconcile, so it never re-Gets it.
func (sr *SnapshotReconciler) propagateStatus(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot, content *nvidiacomv1alpha1.SnapshotContent) (ctrl.Result, error) {
	changed := false
	if snap.Status.BoundSnapshotContentName == nil || *snap.Status.BoundSnapshotContentName != content.Name {
		name := content.Name
		snap.Status.BoundSnapshotContentName = &name
		changed = true
	}

	switch {
	case nvidiacomv1alpha1.IsSnapshotContentSucceeded(content):
		cond := meta.FindStatusCondition(content.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionReady, metav1.ConditionTrue, cond.Reason, cond.Message) || changed
	case nvidiacomv1alpha1.IsSnapshotContentFailed(content):
		cond := meta.FindStatusCondition(content.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionFailed, metav1.ConditionTrue, cond.Reason, cond.Message) || changed
	default:
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionReady, metav1.ConditionFalse, "Pending", "Waiting for node agent to capture the checkpoint") || changed
	}

	if !changed {
		return ctrl.Result{}, nil
	}
	if err := sr.Status().Update(ctx, snap); err != nil {
		return ctrl.Result{}, fmt.Errorf("update snapshot status: %w", err)
	}
	return ctrl.Result{}, nil
}

// setCondition sets a status condition and reports whether it changed.
func (sr *SnapshotReconciler) setCondition(snap *nvidiacomv1alpha1.Snapshot, condType string, status metav1.ConditionStatus, reason, message string) bool {
	return meta.SetStatusCondition(&snap.Status.Conditions, metav1.Condition{
		Type:    condType,
		Status:  status,
		Reason:  reason,
		Message: message,
	})
}

// failSnapshot marks the Snapshot Failed terminally and records an event.
func (sr *SnapshotReconciler) failSnapshot(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot, reason string, cause error) (ctrl.Result, error) {
	sr.Recorder.Event(snap, corev1.EventTypeWarning, reason, cause.Error())
	sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionFailed, metav1.ConditionTrue, reason, cause.Error())
	if err := sr.Status().Update(ctx, snap); err != nil {
		return ctrl.Result{}, fmt.Errorf("mark snapshot failed: %w", err)
	}
	return ctrl.Result{}, nil
}

// handleDelete cascades deletion to the bound SnapshotContent and blocks (requeues) until
// it is gone before dropping the Snapshot finalizer. The SnapshotContent carries no
// finalizer of its own, so the Delete takes effect immediately.
func (sr *SnapshotReconciler) handleDelete(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(snap, snapshotFinalizer) {
		return ctrl.Result{}, nil
	}

	// Without a checkpoint-id label no SnapshotContent could have been bound; drop the
	// finalizer rather than misroute a delete to a wrongly-named object.
	id := snap.Labels[snapshotprotocol.CheckpointIDLabel]
	if id == "" {
		controllerutil.RemoveFinalizer(snap, snapshotFinalizer)
		if err := sr.Update(ctx, snap); err != nil {
			return ctrl.Result{}, fmt.Errorf("remove snapshot finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	contentName := snapshotContentName(id)
	content := &nvidiacomv1alpha1.SnapshotContent{ObjectMeta: metav1.ObjectMeta{Name: contentName}}
	if err := sr.Delete(ctx, content); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete SnapshotContent %q: %w", contentName, err)
	}

	// Block until the content is confirmed gone before releasing the Snapshot.
	if err := sr.Get(ctx, client.ObjectKey{Name: contentName}, &nvidiacomv1alpha1.SnapshotContent{}); err == nil {
		return ctrl.Result{RequeueAfter: snapshotContentDeleteRequeue}, nil
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("confirm SnapshotContent %q deleted: %w", contentName, err)
	}

	controllerutil.RemoveFinalizer(snap, snapshotFinalizer)
	if err := sr.Update(ctx, snap); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove snapshot finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager wires the controller: it owns Snapshots and watches SnapshotContents,
// mapping a SnapshotContent back to its bound Snapshot via spec.snapshotRef.
func (sr *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nvidiacomv1alpha1.Snapshot{}).
		Watches(
			&nvidiacomv1alpha1.SnapshotContent{},
			handler.EnqueueRequestsFromMapFunc(snapshotContentToSnapshot),
		).
		Complete(sr)
}

// snapshotContentToSnapshot maps a SnapshotContent (including a delete-event tombstone) back
// to its bound Snapshot. It MUST unwrap cache.DeletedFinalStateUnknown so that the final
// SnapshotContent delete still re-enqueues the Snapshot and the cascade can complete.
func snapshotContentToSnapshot(_ context.Context, obj client.Object) []reconcile.Request {
	ref, ok := snapshotRefFromContentObj(obj)
	if !ok || ref.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}}}
}

// snapshotRefFromContentObj extracts the bound Snapshot reference from a SnapshotContent,
// unwrapping a cache.DeletedFinalStateUnknown tombstone first so the final delete event
// still re-enqueues the Snapshot and the cascade can complete (F-2.2).
func snapshotRefFromContentObj(obj any) (nvidiacomv1alpha1.SnapshotReference, bool) {
	if tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
		obj = tombstone.Obj
	}
	content, ok := obj.(*nvidiacomv1alpha1.SnapshotContent)
	if !ok {
		return nvidiacomv1alpha1.SnapshotReference{}, false
	}
	return content.Spec.SnapshotRef, true
}

// snapshotContentName composes the deterministic cluster-scoped SnapshotContent name.
func snapshotContentName(checkpointID string) string {
	return "snapshotcontent-" + checkpointID
}

// jitteredBackoff adds up to 50% jitter to a base delay to avoid synchronized requeues.
func jitteredBackoff(base time.Duration) time.Duration {
	return base + time.Duration(rand.Int63n(int64(base/2)+1))
}
