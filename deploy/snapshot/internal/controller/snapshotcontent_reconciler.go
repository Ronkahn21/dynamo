// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

// quiesceRequeueInterval is how often the reconciler re-checks a not-yet-Ready source pod.
const quiesceRequeueInterval = 2 * time.Second

// SnapshotContentReconciler is the per-node CSI-style driver. It picks up SnapshotContent
// work orders for its node, dumps the source container, and writes only
// SnapshotContent.status (snapshotHandle + Ready/Failed). It holds no finalizer.
type SnapshotContentReconciler struct {
	client.Client
	Clientset    kubernetes.Interface
	Config       *types.AgentConfig
	NodeName     string
	HolderID     string
	Checkpointer NodeCheckpointer

	inFlight   map[string]struct{}
	inFlightMu sync.Mutex
}

// Reconcile drives one SnapshotContent through provenance checks, quiesce, dump, and the
// terminal status write. It never mutates spec and writes status via Status().Patch only.
func (scr *SnapshotContentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	content := &nvidiacomv1alpha1.SnapshotContent{}
	if err := scr.Get(ctx, req.NamespacedName, content); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if content.Spec.Source.NodeName != scr.NodeName {
		return ctrl.Result{}, nil
	}

	// Idempotency: terminal status means the work is done.
	if isContentTerminal(content) {
		return ctrl.Result{}, nil
	}

	checkpointID, version, err := storageCoordsFromContent(content)
	if err != nil {
		return scr.writeFailed(ctx, content, "MissingStorageCoords", err)
	}

	destination, err := scr.resolveDestination(checkpointID, version)
	if err != nil {
		return scr.writeFailed(ctx, content, "InvalidDestination", err)
	}

	// Resume: a present artifact with unwritten status means a prior dump finished but the
	// status write did not. The artifact dir exists only after the executor's atomic rename,
	// so its presence means a completed dump; read via the agent's mounted volume, never
	// /host/proc/<pid>/root (there is no live PID on resume).
	if artifactPresent(destination) {
		return scr.writeReady(ctx, content)
	}

	key := req.NamespacedName.String()
	if !scr.tryAcquire(key) {
		return ctrl.Result{}, nil
	}
	releaseInFlight := true
	defer func() {
		if releaseInFlight {
			scr.release(key)
		}
	}()

	pod, result, err := scr.resolveSourcePod(ctx, content)
	if err != nil || pod == nil {
		return result, err
	}

	containerName, err := snapshotprotocol.TargetContainersFromAnnotations(pod.Annotations, 1, 1)
	if err != nil {
		return scr.writeFailed(ctx, content, "MissingTargetContainer", err)
	}
	if !isContainerReady(pod, containerName[0]) {
		logger.V(1).Info("Source container not ready, requeueing to quiesce", "pod", pod.Name, "container", containerName[0])
		return ctrl.Result{RequeueAfter: quiesceRequeueInterval}, nil
	}

	leaseKey := client.ObjectKey{Namespace: content.Spec.SnapshotRef.Namespace, Name: content.Name}
	acquired, err := scr.acquireLease(ctx, leaseKey)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !acquired {
		return ctrl.Result{RequeueAfter: quiesceRequeueInterval}, nil
	}

	releaseInFlight = false
	go scr.runCheckpoint(ctx, content, pod, containerName[0], checkpointID, destination, leaseKey, key)
	return ctrl.Result{}, nil
}

// runCheckpoint executes the dump under a renewed lease and writes the terminal status.
func (scr *SnapshotContentReconciler) runCheckpoint(
	ctx context.Context,
	content *nvidiacomv1alpha1.SnapshotContent,
	pod *corev1.Pod,
	containerName, checkpointID, destination string,
	leaseKey client.ObjectKey,
	inFlightKey string,
) {
	logger := log.FromContext(ctx)
	defer scr.release(inFlightKey)

	leaseCtx, stopLease := context.WithCancel(ctx)
	defer stopLease()
	go scr.renewLease(leaseCtx, leaseKey)
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := scr.releaseLease(releaseCtx, leaseKey); err != nil {
			logger.Error(err, "Failed to release checkpoint lease", "lease", leaseKey.String())
		}
	}()

	params := CheckpointParams{
		Pod:           pod,
		ContainerName: containerName,
		CheckpointID:  checkpointID,
		HostPath:      destination,
		ContainerPath: destination,
		StartedAt:     time.Now(),
	}
	if err := scr.Checkpointer.Checkpoint(leaseCtx, params); err != nil {
		logger.Error(err, "Checkpoint failed", "content", content.Name)
		if _, werr := scr.writeFailed(ctx, content, "CheckpointFailed", err); werr != nil {
			logger.Error(werr, "Failed to write SnapshotContent failed status", "content", content.Name)
		}
		return
	}

	if _, err := scr.writeReady(ctx, content); err != nil {
		logger.Error(err, "Failed to write SnapshotContent ready status", "content", content.Name)
	}
}

// resolveSourcePod loads the source pod and enforces UID provenance and pod liveness.
// It returns (nil, result, err) when the caller should return result/err instead of dumping.
func (scr *SnapshotContentReconciler) resolveSourcePod(ctx context.Context, content *nvidiacomv1alpha1.SnapshotContent) (*corev1.Pod, ctrl.Result, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: content.Spec.SnapshotRef.Namespace, Name: content.Spec.Source.PodRef.Name}
	if err := scr.Get(ctx, key, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ctrl.Result{RequeueAfter: quiesceRequeueInterval}, nil
		}
		return nil, ctrl.Result{}, err
	}
	if content.Spec.Source.PodRef.UID != "" && pod.UID != content.Spec.Source.PodRef.UID {
		result, err := scr.writeFailed(ctx, content, "StalePodReference",
			fmt.Errorf("source pod %q UID %q does not match work order UID %q", pod.Name, pod.UID, content.Spec.Source.PodRef.UID))
		return nil, result, err
	}
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		result, err := scr.writeFailed(ctx, content, "SourcePodGone",
			fmt.Errorf("source pod %q is no longer running (phase %s)", pod.Name, pod.Status.Phase))
		return nil, result, err
	}
	return pod, ctrl.Result{}, nil
}

// resolveDestination computes the artifact directory on the agent's mounted volume.
func (scr *SnapshotContentReconciler) resolveDestination(checkpointID, version string) (string, error) {
	resolved, err := snapshotprotocol.ResolveCheckpointStorage(checkpointID, version, snapshotprotocol.Storage{
		Type:     scr.Config.Storage.Type,
		BasePath: scr.Config.Storage.BasePath,
	})
	if err != nil {
		return "", err
	}
	location := resolved.Location
	if !filepath.IsAbs(location) || filepath.Clean(location) != location {
		return "", fmt.Errorf("checkpoint location must be an absolute, clean path: %q", location)
	}
	return location, nil
}

// writeReady patches status with the Ready condition.
func (scr *SnapshotContentReconciler) writeReady(ctx context.Context, content *nvidiacomv1alpha1.SnapshotContent) (ctrl.Result, error) {
	patch := client.MergeFrom(content.DeepCopy())
	meta.SetStatusCondition(&content.Status.Conditions, metav1.Condition{
		Type:    nvidiacomv1alpha1.SnapshotConditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Captured",
		Message: "Checkpoint captured and verified",
	})
	if err := scr.Status().Patch(ctx, content, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch SnapshotContent ready status: %w", err)
	}
	return ctrl.Result{}, nil
}

// writeFailed patches status with the Failed condition.
func (scr *SnapshotContentReconciler) writeFailed(ctx context.Context, content *nvidiacomv1alpha1.SnapshotContent, reason string, cause error) (ctrl.Result, error) {
	patch := client.MergeFrom(content.DeepCopy())
	meta.SetStatusCondition(&content.Status.Conditions, metav1.Condition{
		Type:    nvidiacomv1alpha1.SnapshotConditionFailed,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: cause.Error(),
	})
	if err := scr.Status().Patch(ctx, content, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("patch SnapshotContent failed status: %w", err)
	}
	return ctrl.Result{}, nil
}

// tryAcquire claims the in-flight slot for a work order, returning false if already held.
func (scr *SnapshotContentReconciler) tryAcquire(key string) bool {
	scr.inFlightMu.Lock()
	defer scr.inFlightMu.Unlock()
	if scr.inFlight == nil {
		scr.inFlight = make(map[string]struct{})
	}
	if _, held := scr.inFlight[key]; held {
		return false
	}
	scr.inFlight[key] = struct{}{}
	return true
}

// release frees the in-flight slot for a work order.
func (scr *SnapshotContentReconciler) release(key string) {
	scr.inFlightMu.Lock()
	defer scr.inFlightMu.Unlock()
	delete(scr.inFlight, key)
}

// SetupWithManager registers the reconciler. The manager cache is label-scoped to this
// node, so a defense-in-depth nodeName predicate is enough; no extra watches are added.
func (scr *SnapshotContentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nvidiacomv1alpha1.SnapshotContent{}).
		Complete(scr)
}

// isContentTerminal reports whether the work order already has a terminal condition.
func isContentTerminal(content *nvidiacomv1alpha1.SnapshotContent) bool {
	for _, t := range []string{nvidiacomv1alpha1.SnapshotConditionReady, nvidiacomv1alpha1.SnapshotConditionFailed} {
		if cond := meta.FindStatusCondition(content.Status.Conditions, t); cond != nil && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// storageCoordsFromContent reads the checkpoint ID (label) and artifact version
// (annotation) carried on the work order. A missing/blank checkpoint ID is fatal; the
// version falls back to the default only when the annotation is entirely absent.
func storageCoordsFromContent(content *nvidiacomv1alpha1.SnapshotContent) (string, string, error) {
	checkpointID := strings.TrimSpace(content.Labels[snapshotprotocol.CheckpointIDLabel])
	if checkpointID == "" {
		return "", "", fmt.Errorf("missing %s label", snapshotprotocol.CheckpointIDLabel)
	}
	version, ok := content.Annotations[snapshotprotocol.CheckpointArtifactVersionAnnotation]
	if !ok {
		version = snapshotprotocol.DefaultCheckpointArtifactVersion
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return "", "", fmt.Errorf("blank %s annotation", snapshotprotocol.CheckpointArtifactVersionAnnotation)
	}
	return checkpointID, version, nil
}

// artifactPresent reports whether a completed checkpoint directory already exists on disk.
func artifactPresent(destination string) bool {
	info, err := os.Stat(destination)
	return err == nil && info.IsDir()
}

