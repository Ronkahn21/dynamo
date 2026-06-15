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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Snapshot and SnapshotContent status condition types. Both objects share this
// vocabulary; the operator and node agent set them via meta.SetStatusCondition.
const (
	// SnapshotConditionReady is True when capture and binding completed and the
	// artifact is usable for restore.
	SnapshotConditionReady = "Ready"
	// SnapshotConditionFailed is True when capture or binding failed terminally.
	SnapshotConditionFailed = "Failed"
)

// SnapshotSpec defines the desired state of Snapshot.
//
// Minimal "trigger" shape: it names what to capture (an existing pod) and the
// artifact identity (CheckpointID). Capture parameters the node agent needs at
// dump time (target container, storage base path) are read from the referenced
// pod's existing annotations and mounts, not duplicated here. The spec is
// immutable after creation.
type SnapshotSpec struct {
	// CheckpointID is the stable artifact identity and the on-PVC artifact
	// subdirectory name (<basePath>/<checkpointID>/versions/<version>/). It is
	// the primary key of the storage contract shared with the restore path and
	// is immutable after creation.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CheckpointID string `json:"checkpointID"`

	// Source identifies the captured workload. It is a struct (rather than an
	// inlined reference) so future source variants can be added additively.
	// +kubebuilder:validation:Required
	Source SnapshotSource `json:"source"`
}

// SnapshotSource identifies the workload captured by a Snapshot.
type SnapshotSource struct {
	// PodRef references the pod, in the Snapshot's namespace, that is captured.
	// The operator prepares the pod (control volume, target-container annotation,
	// checkpoint storage mount) before creating the Snapshot.
	// +kubebuilder:validation:Required
	PodRef PodReference `json:"podRef"`
}

// PodReference names a pod in the same namespace as the referencing Snapshot.
type PodReference struct {
	// Name of the source pod.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// UID of the source pod, recorded so the node agent dumps that specific
	// pod and not a same-named recreation.
	// +optional
	UID types.UID `json:"uid,omitempty"`
}

// SnapshotStatus defines the observed state of Snapshot.
type SnapshotStatus struct {
	// BoundSnapshotContentName is the name of the cluster-scoped SnapshotContent
	// this Snapshot is bound to. It is nil until the agent has created the
	// content and recorded the binding.
	// +optional
	BoundSnapshotContentName *string `json:"boundSnapshotContentName,omitempty"`

	// Conditions reflect the latest observations of the Snapshot's state.
	// Standard types are Ready and Failed.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=snap
// +kubebuilder:printcolumn:name="CheckpointID",type="string",JSONPath=".spec.checkpointID",description="Artifact identity"
// +kubebuilder:printcolumn:name="Content",type="string",JSONPath=".status.boundSnapshotContentName",description="Bound SnapshotContent"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status",description="Ready condition"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec) || self.spec == oldSelf.spec",message="spec is immutable"

// Snapshot is the Schema for the snapshots API. It is the namespaced binding
// for a captured container checkpoint and is consumed by restore paths.
type Snapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotSpec   `json:"spec,omitempty"`
	Status SnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SnapshotList contains a list of Snapshot.
type SnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Snapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Snapshot{}, &SnapshotList{})
}
