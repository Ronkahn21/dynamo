// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/executor"
	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

// CheckpointParams carries everything the node driver needs to dump one container.
type CheckpointParams struct {
	// Pod is the live source pod (already provenance-verified by the reconciler).
	Pod *corev1.Pod
	// ContainerName is the single target container to checkpoint.
	ContainerName string
	// CheckpointID is the stable artifact identity.
	CheckpointID string
	// HostPath is the agent-resolved destination directory for the dump.
	HostPath string
	// ContainerPath is the destination as seen inside the workload container's mount
	// namespace (equal to HostPath under agentMount storage).
	ContainerPath string
	// StartedAt marks when the reconciler observed the work order, for timing.
	StartedAt time.Time
}

// NodeCheckpointer performs the CRIU dump for a single SnapshotContent work order. The
// concrete implementation wraps executor.Checkpoint; unit tests substitute a fake.
type NodeCheckpointer interface {
	// Checkpoint runs the dump and verifies the produced artifact. It returns an error
	// on any failure; on success the artifact exists at params.HostPath.
	Checkpoint(ctx context.Context, params CheckpointParams) error
}

// executorCheckpointer is the production NodeCheckpointer backed by executor.Checkpoint.
type executorCheckpointer struct {
	clientset kubernetes.Interface
	runtime   snapshotruntime.Runtime
	config    *types.AgentConfig
	nodeName  string
}

// newExecutorCheckpointer builds the production node checkpointer.
func newExecutorCheckpointer(clientset kubernetes.Interface, rt snapshotruntime.Runtime, cfg *types.AgentConfig, nodeName string) *executorCheckpointer {
	return &executorCheckpointer{clientset: clientset, runtime: rt, config: cfg, nodeName: nodeName}
}

// Checkpoint resolves the target container, runs executor.Checkpoint to the destination,
// verifies the artifact directory, and writes the snapshot-complete sentinel. On dump or
// verification failure it SIGKILLs the CUDA-locked process before returning the error.
func (ec *executorCheckpointer) Checkpoint(ctx context.Context, params CheckpointParams) error {
	log := logr.FromContextOrDiscard(ctx)

	containerID := containerIDForName(params.Pod, params.ContainerName)
	if containerID == "" {
		return fmt.Errorf("could not resolve container %q ID", params.ContainerName)
	}

	containerPID, _, err := ec.runtime.ResolveContainer(ctx, containerID)
	if err != nil {
		return fmt.Errorf("resolve container %q: %w", params.ContainerName, err)
	}

	req := executor.CheckpointRequest{
		ContainerID:        containerID,
		ContainerName:      params.ContainerName,
		CheckpointID:       params.CheckpointID,
		CheckpointLocation: params.HostPath,
		StartedAt:          params.StartedAt,
		NodeName:           ec.nodeName,
		PodName:            params.Pod.Name,
		PodNamespace:       params.Pod.Namespace,
		Clientset:          ec.clientset,
	}
	if err := executor.Checkpoint(ctx, ec.runtime, log, req, ec.config); err != nil {
		ec.kill(log, containerPID, "checkpoint failed")
		return fmt.Errorf("checkpoint: %w", err)
	}

	info, statErr := os.Stat(params.HostPath)
	if statErr != nil || !info.IsDir() {
		ec.kill(log, containerPID, "checkpoint verification failed")
		if statErr != nil {
			return fmt.Errorf("verify checkpoint path %s: %w", params.HostPath, statErr)
		}
		return fmt.Errorf("verify checkpoint path %s: not a directory", params.HostPath)
	}

	if err := snapshotruntime.WriteControlSentinel(containerPID, snapshotprotocol.SnapshotCompleteFile); err != nil {
		ec.kill(log, containerPID, "checkpoint sentinel failed")
		return fmt.Errorf("write snapshot-complete sentinel: %w", err)
	}
	return nil
}

// kill signals the CUDA-locked process so it does not hang after a failed dump.
func (ec *executorCheckpointer) kill(log logr.Logger, pid int, reason string) {
	if err := snapshotruntime.SendSignalToPID(log, pid, syscall.SIGKILL, reason); err != nil {
		log.Error(err, "Failed to signal checkpoint process", "reason", reason)
	}
}

// containerIDForName returns the running container's CRI-stripped ID, or "" if absent.
func containerIDForName(pod *corev1.Pod, containerName string) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName {
			return snapshotruntime.StripCRIScheme(cs.ContainerID)
		}
	}
	return ""
}
