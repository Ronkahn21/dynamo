// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
)

// NewSnapshotContentManager builds the per-node controller-runtime Manager that drives
// checkpoint capture. Its cache is scoped to this node — SnapshotContent by the
// nvidia.com/snapshot-node mirror label, pods by their spec.nodeName field (so the agent
// does not open a second cluster-wide pod watch). Leader election is off, and the
// SnapshotContent reconciler is registered with the production executor-backed driver.
func NewSnapshotContentManager(cfg *types.AgentConfig, rt snapshotruntime.Runtime) (ctrl.Manager, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nvidiacomv1alpha1.AddToScheme(scheme))

	nodeContentSelector := labels.SelectorFromSet(labels.Set{snapshotprotocol.SnapshotNodeLabel: cfg.NodeName})
	nodePodSelector := fields.OneTermEqualSelector("spec.nodeName", cfg.NodeName)

	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
		Metrics:        server.Options{BindAddress: "0"},
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&nvidiacomv1alpha1.SnapshotContent{}: {Label: nodeContentSelector},
				&corev1.Pod{}:                        {Field: nodePodSelector},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create snapshot-content manager: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client for lease coordination: %w", err)
	}

	reconciler := &SnapshotContentReconciler{
		Client:       mgr.GetClient(),
		Clientset:    clientset,
		Config:       cfg,
		NodeName:     cfg.NodeName,
		HolderID:     "snapshot-agent/" + uuid.NewString(),
		Checkpointer: newExecutorCheckpointer(clientset, rt, cfg, cfg.NodeName),
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up SnapshotContent reconciler: %w", err)
	}
	return mgr, nil
}
