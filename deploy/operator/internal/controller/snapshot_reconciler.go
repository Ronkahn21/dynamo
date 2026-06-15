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
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

const (
	// snapshotFinalizer guards SnapshotContent cleanup before a Snapshot is removed.
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
// +kubebuilder:rbac:groups=nvidia.com,resources=snapshotcontents/finalizers,verbs=update
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

	pod, err := sr.resolveSourcePod(ctx, snap)
	if err != nil {
		if errors.Is(err, errSnapshotPodUnscheduled) || apierrors.IsNotFound(err) {
			logger.V(1).Info("Source pod not ready, backing off", "snapshot", snap.Name, "reason", err.Error())
			return ctrl.Result{RequeueAfter: jitteredBackoff(snapshotPodResolveBackoffBase)}, nil
		}
		return ctrl.Result{}, err
	}

	contentName := snapshotContentName(snap.Spec.CheckpointID)
	if errs := validation.IsDNS1123Subdomain(contentName); len(errs) > 0 || len(contentName) > maxResourceNameLength {
		return sr.failSnapshot(ctx, snap, "InvalidContentName",
			fmt.Errorf("composed SnapshotContent name %q is invalid: too long or not a DNS subdomain", contentName))
	}

	bound, err := sr.findBoundContent(ctx, contentName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if bound != nil && bound.Spec.Source.NodeName != pod.Spec.NodeName {
		return sr.failSnapshot(ctx, snap, "PodRescheduled",
			fmt.Errorf("source pod moved from node %q to %q; CRIU checkpoint cannot survive migration",
				bound.Spec.Source.NodeName, pod.Spec.NodeName))
	}

	if err := sr.ensureSnapshotContent(ctx, snap, contentName, pod); err != nil {
		return ctrl.Result{}, err
	}

	if err := sr.bindAndMirror(ctx, snap, contentName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// resolveSourcePod loads the source pod and requires it be scheduled to a node.
func (sr *SnapshotReconciler) resolveSourcePod(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: snap.Namespace, Name: snap.Spec.Source.PodRef.Name}
	if err := sr.Get(ctx, key, pod); err != nil {
		return nil, err
	}
	if pod.Spec.NodeName == "" {
		return nil, errSnapshotPodUnscheduled
	}
	return pod, nil
}

// findBoundContent returns the bound SnapshotContent if it already exists, or nil.
func (sr *SnapshotReconciler) findBoundContent(ctx context.Context, contentName string) (*nvidiacomv1alpha1.SnapshotContent, error) {
	content := &nvidiacomv1alpha1.SnapshotContent{}
	if err := sr.Get(ctx, client.ObjectKey{Name: contentName}, content); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return content, nil
}

// ensureSnapshotContent applies the SnapshotContent work order via a single Server-Side
// Apply carrying source, the node mirror label, storage-coord metadata, and the finalizer.
func (sr *SnapshotReconciler) ensureSnapshotContent(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot, contentName string, pod *corev1.Pod) error {
	content := sr.buildSnapshotContent(snap, contentName, pod)
	if err := sr.Patch(ctx, content, client.Apply,
		client.FieldOwner(snapshotContentFieldManager), client.ForceOwnership); err != nil {
		sr.Recorder.Event(snap, corev1.EventTypeWarning, "SnapshotContentCreateFailed", err.Error())
		return fmt.Errorf("apply SnapshotContent %q: %w", contentName, err)
	}
	return nil
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
				snapshotprotocol.CheckpointIDLabel: snap.Spec.CheckpointID,
			},
			Annotations: map[string]string{
				snapshotprotocol.CheckpointArtifactVersionAnnotation: snapshotprotocol.ArtifactVersion(snap.Annotations[snapshotprotocol.CheckpointArtifactVersionAnnotation]),
			},
			Finalizers: []string{snapshotFinalizer},
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

// bindAndMirror records the binding and mirrors the SnapshotContent's terminal status to
// the Snapshot, defaulting to a Pending condition until the agent writes a result.
func (sr *SnapshotReconciler) bindAndMirror(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot, contentName string) error {
	content := &nvidiacomv1alpha1.SnapshotContent{}
	if err := sr.Get(ctx, client.ObjectKey{Name: contentName}, content); err != nil {
		return client.IgnoreNotFound(err)
	}

	changed := false
	if snap.Status.BoundSnapshotContentName == nil || *snap.Status.BoundSnapshotContentName != contentName {
		snap.Status.BoundSnapshotContentName = &contentName
		changed = true
	}

	ready := meta.FindStatusCondition(content.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionReady)
	failed := meta.FindStatusCondition(content.Status.Conditions, nvidiacomv1alpha1.SnapshotConditionFailed)
	switch {
	case ready != nil && ready.Status == metav1.ConditionTrue:
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionReady, metav1.ConditionTrue, ready.Reason, ready.Message) || changed
	case failed != nil && failed.Status == metav1.ConditionTrue:
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionFailed, metav1.ConditionTrue, failed.Reason, failed.Message) || changed
	default:
		changed = sr.setCondition(snap, nvidiacomv1alpha1.SnapshotConditionReady, metav1.ConditionFalse, "Pending", "Waiting for node agent to capture the checkpoint") || changed
	}

	if !changed {
		return nil
	}
	if err := sr.Status().Update(ctx, snap); err != nil {
		return fmt.Errorf("update snapshot status: %w", err)
	}
	return nil
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

// handleDelete cascades deletion to the bound SnapshotContent, waits for it to be gone,
// then drops the Snapshot finalizer.
func (sr *SnapshotReconciler) handleDelete(ctx context.Context, snap *nvidiacomv1alpha1.Snapshot) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(snap, snapshotFinalizer) {
		return ctrl.Result{}, nil
	}

	contentName := snapshotContentName(snap.Spec.CheckpointID)
	content := &nvidiacomv1alpha1.SnapshotContent{}
	err := sr.Get(ctx, client.ObjectKey{Name: contentName}, content)
	switch {
	case err == nil:
		// Clear the controller finalizer first so the subsequent Delete is not
		// blocked, then issue the Delete. The reconcile requeues until the
		// SnapshotContent is fully gone.
		if controllerutil.ContainsFinalizer(content, snapshotFinalizer) {
			controllerutil.RemoveFinalizer(content, snapshotFinalizer)
			if updErr := sr.Update(ctx, content); updErr != nil && !apierrors.IsNotFound(updErr) {
				return ctrl.Result{}, fmt.Errorf("clear SnapshotContent %q finalizer: %w", contentName, updErr)
			}
		}
		if content.GetDeletionTimestamp().IsZero() {
			if delErr := sr.Delete(ctx, content); delErr != nil && !apierrors.IsNotFound(delErr) {
				return ctrl.Result{}, fmt.Errorf("delete SnapshotContent %q: %w", contentName, delErr)
			}
		}
		return ctrl.Result{RequeueAfter: snapshotContentDeleteRequeue}, nil
	case apierrors.IsNotFound(err):
		// SnapshotContent gone; drop the Snapshot finalizer.
	default:
		return ctrl.Result{}, fmt.Errorf("get SnapshotContent %q: %w", contentName, err)
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
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false },
				UpdateFunc:  func(ue event.UpdateEvent) bool { return true },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				GenericFunc: func(event.GenericEvent) bool { return false },
			}),
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
