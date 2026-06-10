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

// SnapshotContentSpec defines the desired state of SnapshotContent. It is
// populated by the node agent at creation time and is immutable thereafter.
type SnapshotContentSpec struct {
	// SnapshotRef is the back-pointer to the bound Snapshot. It may span
	// namespaces because SnapshotContent is cluster-scoped.
	// +kubebuilder:validation:Required
	SnapshotRef SnapshotReference `json:"snapshotRef"`

	// Source locates the physical artifact via a self-contained, opaque handle.
	// +kubebuilder:validation:Required
	Source SnapshotContentSource `json:"source"`
}

// SnapshotReference is a cross-namespace reference to a Snapshot.
type SnapshotReference struct {
	// Namespace of the referenced Snapshot.
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace"`

	// Name of the referenced Snapshot.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// UID of the referenced Snapshot, recorded at binding time to detect a
	// stale reference after a delete and recreate.
	// +optional
	UID types.UID `json:"uid,omitempty"`
}

// SnapshotContentSource locates the physical checkpoint artifact.
type SnapshotContentSource struct {
	// SnapshotHandle is a self-contained, opaque artifact locator. The v1alpha1
	// PVC format is:
	//   pvc://<namespace>/<claimName>/<basePath>/<checkpointID>/versions/<version>
	// It fully locates the artifact without correlating any other field.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	SnapshotHandle string `json:"snapshotHandle"`
}

// SnapshotContentStatus defines the observed state of SnapshotContent.
type SnapshotContentStatus struct {
	// SnapshotHandle mirrors spec.source.snapshotHandle once the node agent has
	// verified the artifact.
	// +optional
	SnapshotHandle *string `json:"snapshotHandle,omitempty"`

	// Conditions reflect the latest observations of the SnapshotContent's state.
	// Standard types are Ready and Failed.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=snapcontent
// +kubebuilder:printcolumn:name="Snapshot",type="string",JSONPath=".spec.snapshotRef.name",description="Bound Snapshot"
// +kubebuilder:printcolumn:name="Namespace",type="string",JSONPath=".spec.snapshotRef.namespace",description="Snapshot namespace"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status",description="Ready condition"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.spec) || self.spec == oldSelf.spec",message="spec is immutable"

// SnapshotContent is the Schema for the snapshotcontents API. It is the
// cluster-scoped artifact-of-record for a captured container checkpoint.
type SnapshotContent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SnapshotContentSpec   `json:"spec,omitempty"`
	Status SnapshotContentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SnapshotContentList contains a list of SnapshotContent.
type SnapshotContentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SnapshotContent `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SnapshotContent{}, &SnapshotContentList{})
}
