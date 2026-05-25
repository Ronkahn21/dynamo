# NVSnapshot v1alpha1 API

**Date:** 2026-05-25
**Status:** Draft
**Scope:** API surface only. Controller behavior, agent internals, RBAC, admission webhook configuration, and deployment concerns are out of scope.

---

## 1. Overview

NVSnapshot is a Kubernetes API for container checkpoint/restore (c/r). It is modeled on `VolumeSnapshot` but adapted to active-workload c/r — the snapshot subject is a running process tree, not a static volume.

### 1.1 The four APIs

| Kind | Scope | Role |
|---|---|---|
| `Snapshot` | namespaced | **Primary primitive.** Identifies a captured artifact; produces or binds a `SnapshotContent`; consumed by restore. Can be created directly by the user against an existing pod. |
| `SnapshotContent` | cluster-scoped | Artifact-of-record. Describes where the dump lives and what platform produced it. |
| `SnapshotJob` | namespaced | **Convenience wrapper over `Snapshot`.** Adds pod creation: runs a fresh pod from a PodTemplate, waits for quiesce, dumps. Produces a `Snapshot` + `SnapshotContent`. Ergonomic shortcut when the user doesn't want to manage the source pod themselves. |
| `RestoreSnapshot` | namespaced | Operator-driven restore. Targets an existing pod and drives CRIU replay against a referenced `Snapshot` or `SnapshotContent`. |

### 1.2 Relationships

```
                ┌────────────────────┐
                │    SnapshotJob     │ (namespaced)
                │     producer       │
                └──────────┬─────────┘
                           │ runs PodTemplate; on success
                           │ produces (no ownership)
              ┌────────────┴────────────┐
              ▼                         ▼
     ┌────────────────┐         ┌─────────────────┐
     │    Snapshot    │ ◀─────▶ │ SnapshotContent │
     │  (namespaced)  │  bound  │  (cluster-scoped)│
     │    binding     │         │     artifact    │
     └────────┬───────┘         └─────────────────┘
              │
              │ referenced by pod annotation
              ▼
     ┌──────────────────────────────┐
     │  Restore (Mode A) — pod      │
     │  carries nvsnapshot.io/      │
     │  restore-from annotation     │
     └──────────────────────────────┘
```

**Key facts:**

- **`SnapshotJob` does not own its outputs.** Deleting the SnapshotJob leaves the produced `Snapshot` and `SnapshotContent` in place. Lifecycle of the artifact is governed by `SnapshotContent.spec.deletionPolicy`.
- **`Snapshot` ↔ `SnapshotContent` is bidirectional binding** (mirrors `VolumeSnapshot` ↔ `VolumeSnapshotContent`):
  - `SnapshotContent.spec.snapshotRef` is the **back-pointer** (cluster-scoped → namespaced). Set by the controller at creation time.
  - `Snapshot.status.boundSnapshotContentName` is the **forward-pointer**. Set by the controller once binding is verified.
  - Consumers MUST verify the binding by checking that `Snapshot.status.boundSnapshotContentName` and `SnapshotContent.spec.snapshotRef.{namespace,name,uid}` agree before treating the artifact as usable. UID detects stale references.
- **`Snapshot` is the primary primitive; `SnapshotJob` is convenience.** Users can create a `Snapshot` directly against an existing pod (low-level path). Or they can create a `SnapshotJob` with a PodTemplate; the SnapshotJob controller creates the pod and then a Snapshot on top of it (ergonomic path). The result — a `Snapshot` + `SnapshotContent` — is identical either way.
- **Restore consumes via Snapshot or SnapshotContent reference**, never via SnapshotJob.

---

## 2. The four APIs

Each section below presents the API as Go types in package `apis/nvsnapshot/v1alpha1`. Field-level docstrings double as the canonical field semantics. Validation markers (`+kubebuilder:...`) are the source of truth for CRD generation.

### 2.1 Snapshot

**Role.** The primary primitive for container checkpoint. Identifies *what* was captured (always an existing pod). The unit of capture is one or more containers within a pod.

Two ways to create one:

1. **Directly** — user creates a Snapshot with `Spec.Source.PodRef` pointing at an existing pod and `Spec.QuiesceProbe` + `Spec.Storage` populated. The pod must already satisfy the snapshot contract (control volume, target-container labels, NRI-injected binaries). This is the canonical low-level path.
2. **Via SnapshotJob** (convenience) — user creates a `SnapshotJob` with a PodTemplate. The SnapshotJob controller creates the source pod, then creates a Snapshot on top of it (with `QuiesceProbe`/`Storage`/`DeletionPolicy` copied from the SnapshotJob and `PodRef` set to the new pod). The produced Snapshot is identical to one created directly.

**Reference to `SnapshotContent`:**

- `Status.BoundSnapshotContentName` — set by the controller once the dump completes and the binding is verified against `SnapshotContent.spec.snapshotRef`.

```go
// Snapshot is the user-facing binding for a container checkpoint.
// It identifies what was captured and is consumed by restore paths.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=snap
type Snapshot struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   SnapshotSpec   `json:"spec,omitempty"`
    Status SnapshotStatus `json:"status,omitempty"`
}
```

#### Spec

```go
// SnapshotSpec describes what this snapshot is OF and how it should be
// captured. Two creation flows:
//   1. SnapshotJob-produced: the SnapshotJob controller creates a
//      Snapshot, copying its quiesceProbe and storage into Snapshot.spec.
//   2. Direct user creation (live-pod): the user creates a Snapshot
//      with PodRef pointing at an existing pod that already satisfies
//      the snapshot contract (control volume, target-container labels,
//      etc.). The controller drives the dump.
// +kubebuilder:validation:XValidation:rule="self.source == oldSelf.source",message="source is immutable after binding"
type SnapshotSpec struct {
    // Source identifies the origin of this snapshot. Immutable after binding.
    // +kubebuilder:validation:Required
    Source SnapshotSource `json:"source"`

    // QuiesceProbe defines how the agent detects that the target
    // containers are safe to dump. When omitted, defaults to a
    // file-sentinel probe at /var/run/nvsnapshot/ready-for-checkpoint.
    // +optional
    QuiesceProbe *QuiesceProbe `json:"quiesceProbe,omitempty"`

    // Storage describes where the artifact will be written.
    // Required for direct creation; for SnapshotJob-produced Snapshots,
    // copied from SnapshotJob.spec.storage at creation time.
    // +kubebuilder:validation:Required
    Storage SnapshotStorage `json:"storage"`

    // DeletionPolicy propagates to the produced SnapshotContent.
    // +optional
    // +kubebuilder:default=Delete
    // +kubebuilder:validation:Enum=Delete;Retain
    DeletionPolicy SnapshotDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// SnapshotSource identifies the captured workload. Kept as a struct
// (rather than inlined PodRef) so future variants can be added additively.
type SnapshotSource struct {
    // PodRef references the pod whose containers were (or will be)
    // captured. For direct creation, the pod must exist at Snapshot
    // creation time and satisfy the snapshot contract.
    // +kubebuilder:validation:Required
    PodRef PodReference `json:"podRef"`
}

// PodReference identifies the pod that was (or will be) captured
// and optionally narrows which of its containers are in scope.
type PodReference struct {
    // Name of the source pod, in the same namespace as the Snapshot.
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // UID of the source pod, captured at binding time. Preserves
    // identity even after the pod is deleted.
    // +optional
    UID types.UID `json:"uid,omitempty"`

    // Containers narrows the snapshot scope to specific containers
    // within the pod. If empty, all containers in the pod are captured.
    // +optional
    Containers []string `json:"containers,omitempty"`
}
```

#### Status

```go
// SnapshotStatus is the observed state of a Snapshot. Shape mirrors
// VolumeSnapshotStatus: ReadyToUse + CreationTime + Error, no Phase enum.
type SnapshotStatus struct {
    // BoundSnapshotContentName is the name of the SnapshotContent object
    // this Snapshot is bound to. nil until the binding is established.
    //
    // Consumers MUST verify binding by checking that both Snapshot and
    // SnapshotContent point at each other before treating this object
    // as usable for restore.
    // +optional
    BoundSnapshotContentName *string `json:"boundSnapshotContentName,omitempty"`

    // CreationTime is the timestamp at which the artifact was created
    // (CRIU dump complete, manifest finalized). Mirrors the bound
    // SnapshotContent's CreationTime.
    // +optional
    CreationTime *metav1.Time `json:"creationTime,omitempty"`

    // ReadyToUse indicates whether the snapshot is ready to be used
    // for restore. True iff the bound SnapshotContent is Ready.
    // nil indicates that readiness is unknown (e.g., binding still
    // in progress).
    // +optional
    ReadyToUse *bool `json:"readyToUse,omitempty"`

    // Error is the last observed error during snapshot creation or
    // binding, if any. Cleared on the next successful reconcile.
    // +optional
    Error *SnapshotError `json:"error,omitempty"`
}

// SnapshotError reports a transient or terminal error observed by the
// controller. Shape mirrors VolumeSnapshotError.
type SnapshotError struct {
    // Time is the timestamp at which the error was observed.
    // +optional
    Time *metav1.Time `json:"time,omitempty"`

    // Message is a human-readable description of the error.
    // MUST NOT contain sensitive information (it may be logged).
    // +optional
    Message *string `json:"message,omitempty"`
}
```

---

### 2.2 SnapshotContent

**Role.** The artifact-of-record. Cluster-scoped so it survives Snapshot deletion (under `Retain` policy) and may be re-bound across namespaces in the future. Carries enough metadata for a consumer to (a) locate the artifact and (b) understand the platform that produced it.

**Reference to `Snapshot`:**

- `Spec.SnapshotRef` — the back-pointer to the bound Snapshot. Required and immutable after binding. Populated by the controller at SnapshotContent creation (via SnapshotJob), naming the produced Snapshot.

```go
// SnapshotContent is the cluster-scoped artifact-of-record for a
// captured container checkpoint. Equivalent to VolumeSnapshotContent.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=snapcontent
type SnapshotContent struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   SnapshotContentSpec   `json:"spec,omitempty"`
    Status SnapshotContentStatus `json:"status,omitempty"`
}
```

#### Spec

```go
// SnapshotContentSpec describes the artifact and its lifecycle policy.
// +kubebuilder:validation:XValidation:rule="self.snapshotRef == oldSelf.snapshotRef",message="snapshotRef is immutable after binding"
// +kubebuilder:validation:XValidation:rule="self.source == oldSelf.source",message="source is immutable after binding"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.platform) || (has(self.platform) && self.platform == oldSelf.platform)",message="platform is immutable after binding"
type SnapshotContentSpec struct {
    // DeletionPolicy controls what happens to the on-PVC artifact
    // when the bound Snapshot is deleted.
    //   Delete: the artifact directory is removed and the SnapshotContent
    //           is deleted.
    //   Retain: the artifact directory is left in place and the
    //           SnapshotContent persists with a stale snapshotRef. The
    //           SnapshotContent remains ready-to-use and may be re-bound
    //           by creating a new Snapshot with spec.source.snapshotContentName.
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Enum=Delete;Retain
    DeletionPolicy SnapshotDeletionPolicy `json:"deletionPolicy"`

    // SnapshotRef is the back-pointer to the bound Snapshot. May span
    // namespaces since SnapshotContent is cluster-scoped. Immutable
    // after binding.
    // +kubebuilder:validation:Required
    SnapshotRef SnapshotReference `json:"snapshotRef"`

    // Source describes where the artifact is stored. v1alpha1: PVC only.
    // Immutable after binding.
    // +kubebuilder:validation:Required
    Source SnapshotContentSource `json:"source"`

    // Platform describes the node and GPU that produced this artifact.
    // Descriptive only — the API does not enforce restore compatibility.
    // Populated by the producer for dynamic creation; supplied by the
    // user for pre-provisioned import. Immutable after binding.
    // +optional
    Platform *Platform `json:"platform,omitempty"`
}

// SnapshotReference is a cross-namespace reference to a Snapshot.
type SnapshotReference struct {
    // Namespace of the referent.
    // +kubebuilder:validation:Required
    Namespace string `json:"namespace"`

    // Name of the referent.
    // +kubebuilder:validation:Required
    Name string `json:"name"`

    // UID of the referent. Populated at binding time to detect stale
    // references (e.g., if the original Snapshot is deleted and a new
    // one with the same name is created).
    // +optional
    UID types.UID `json:"uid,omitempty"`
}

// SnapshotContentSource describes the artifact backend via an opaque,
// driver-specific handle. SnapshotContent is cluster-scoped and cannot
// directly reference namespaced resources (e.g., PVCs); the handle
// encodes any cross-namespace location information as a string.
//
// v1alpha1 supports the PVC backend; future backends (object storage,
// URI, etc.) use different handle formats but the same field shape.
type SnapshotContentSource struct {
    // SnapshotHandle is an opaque, driver-specific identifier for the
    // physical artifact.
    //
    // For the v1alpha1 PVC backend, the format is:
    //   pvc://<namespace>/<claimName>/<basePath>
    //
    // The PVC's namespace is encoded in the URI because SnapshotContent
    // (cluster-scoped) cannot carry a structured reference to a
    // namespaced PVC. The basePath component is producer-assigned
    // (derived from the producing SnapshotJob's UID) to guarantee global
    // uniqueness; consumers must not interpret its structure.
    //
    // For pre-provisioned import: user-supplied; the controller validates
    // and mirrors to status.snapshotHandle.
    // For dynamic creation: controller-assigned post-dump.
    //
    // Immutable after binding.
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:MinLength=1
    SnapshotHandle string `json:"snapshotHandle"`
}

// Platform describes the node and GPU that produced an artifact.
// Minimum v1alpha1 set; richer fields are reserved for future versions.
type Platform struct {
    // NodeArch is the CPU architecture of the source node, e.g. "amd64".
    // +optional
    NodeArch string `json:"nodeArch,omitempty"`

    // GPUModel is the GPU product label of the source node, e.g.
    // "NVIDIA H100". Restore consumers may use this for nodeSelector,
    // but the API does not enforce.
    // +optional
    GPUModel string `json:"gpuModel,omitempty"`

    // GPUDriverVersion is the NVIDIA driver version on the source node.
    // +optional
    GPUDriverVersion string `json:"gpuDriverVersion,omitempty"`

    // CRIUVersion is the CRIU version used to produce the dump.
    // +optional
    CRIUVersion string `json:"criuVersion,omitempty"`
}

// SnapshotDeletionPolicy is the cleanup policy for a bound artifact.
// +kubebuilder:validation:Enum=Delete;Retain
type SnapshotDeletionPolicy string

const (
    SnapshotDeletionPolicyDelete SnapshotDeletionPolicy = "Delete"
    SnapshotDeletionPolicyRetain SnapshotDeletionPolicy = "Retain"
)
```

#### Status

```go
// SnapshotContentStatus is the observed state of a SnapshotContent.
// Shape mirrors VolumeSnapshotContentStatus: SnapshotHandle +
// CreationTime + RestoreSize + ReadyToUse + Error, no Phase enum.
type SnapshotContentStatus struct {
    // SnapshotHandle is the canonical, validated handle for the artifact.
    // Mirrors spec.source.snapshotHandle once the controller has verified
    // the artifact (dynamic creation) or accepted the user-supplied
    // handle (pre-provisioned import).
    // +optional
    SnapshotHandle *string `json:"snapshotHandle,omitempty"`

    // CreationTime is the timestamp at which the artifact was created
    // (CRIU dump complete, manifest finalized). For pre-provisioned
    // imports, this is the time recorded in the artifact manifest.
    // +optional
    CreationTime *metav1.Time `json:"creationTime,omitempty"`

    // RestoreSize is the on-PVC artifact size in bytes. Consumers may
    // use this to size restore-target volumes / nodes.
    // +optional
    // +kubebuilder:validation:Minimum=0
    RestoreSize *int64 `json:"restoreSize,omitempty"`

    // ReadyToUse indicates whether the artifact is complete and usable
    // for restore. True iff the dump finished and the manifest is valid.
    // nil indicates that readiness is unknown (e.g., still provisioning).
    // +optional
    ReadyToUse *bool `json:"readyToUse,omitempty"`

    // Error is the last observed error during artifact provisioning or
    // validation, if any. Cleared on the next successful reconcile.
    // +optional
    Error *SnapshotError `json:"error,omitempty"`
}
```

`SnapshotError` is defined in §2.1 and shared between `Snapshot` and `SnapshotContent`.

---

### 2.3 SnapshotJob

**Role.** A convenience wrapper over `Snapshot` that adds pod creation. Analogous to `batch/v1.Job` in shape, but the lifecycle is: launch a pod from a PodTemplate, wait for quiesce, create a `Snapshot` against that pod, drive the dump, produce a `SnapshotContent`. The `Snapshot` produced by a SnapshotJob is indistinguishable from one created directly by the user (§2.1); SnapshotJob exists purely so users don't have to manage the source pod themselves.

**Does not own** the produced artifacts — deleting the SnapshotJob has no effect on them.

```go
// SnapshotJob is a one-shot producer of a Snapshot + SnapshotContent.
// Analogous to batch/v1.Job. Does not own the produced artifacts.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=snapjob
type SnapshotJob struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   SnapshotJobSpec   `json:"spec,omitempty"`
    Status SnapshotJobStatus `json:"status,omitempty"`
}
```

#### Spec

```go
// SnapshotJobSpec describes the desired snapshot production operation.
type SnapshotJobSpec struct {
    // PodTemplate is the pod to launch. The application running in the
    // target containers must implement the quiesce contract — see
    // QuiesceProbe.
    // +kubebuilder:validation:Required
    PodTemplate corev1.PodTemplateSpec `json:"podTemplate"`

    // QuiesceProbe defines how to determine when the pod is safe to dump.
    // When omitted, defaults to a file-sentinel probe at
    // /var/run/nvsnapshot/ready-for-checkpoint.
    // +optional
    QuiesceProbe *QuiesceProbe `json:"quiesceProbe,omitempty"`

    // TargetContainers narrows which containers in PodTemplate are
    // captured. If empty, all containers in the PodTemplate are captured.
    // +optional
    TargetContainers []string `json:"targetContainers,omitempty"`

    // Storage describes where the artifact will be written.
    // v1alpha1: PVC only.
    // +kubebuilder:validation:Required
    Storage SnapshotStorage `json:"storage"`

    // ActiveDeadlineSeconds bounds the total lifetime of the dump
    // operation (pod scheduling + quiesce wait + dump execution).
    // +optional
    // +kubebuilder:default=3600
    // +kubebuilder:validation:Minimum=1
    ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

    // DeletionPolicy is propagated to the produced SnapshotContent.
    // +optional
    // +kubebuilder:default=Delete
    // +kubebuilder:validation:Enum=Delete;Retain
    DeletionPolicy SnapshotDeletionPolicy `json:"deletionPolicy,omitempty"`
}

// SnapshotStorage describes the artifact destination. Shared between
// Snapshot.spec (direct creation) and SnapshotJob.spec (factory). v1alpha1
// supports PVC only; tagged-union shape preserved for additive future variants.
// +kubebuilder:validation:XValidation:rule="has(self.pvc)",message="exactly one storage variant must be set; v1alpha1 supports only pvc"
type SnapshotStorage struct {
    // PVC names the PersistentVolumeClaim that will store the artifact.
    // The artifact directory inside the PVC is producer-assigned (derived
    // from the producing Snapshot or SnapshotJob UID), not user-settable.
    // +optional
    PVC *PVCStorage `json:"pvc,omitempty"`
}

// PVCStorage references a PersistentVolumeClaim in the same namespace
// as the producing Snapshot or SnapshotJob.
type PVCStorage struct {
    // ClaimName of the PVC. Must support the access mode required by
    // the deployment (ReadWriteMany for production; ReadWriteOnce
    // permitted for single-node test/dev with appropriate nodeAffinity).
    // +kubebuilder:validation:Required
    ClaimName string `json:"claimName"`
}
```

#### Status

```go
// SnapshotJobStatus is the observed state of a SnapshotJob. Producer
// users read this object only — fields here are the minimum needed to
// drive restore consumption (one-pane-of-glass UX).
type SnapshotJobStatus struct {
    // Phase is the high-level lifecycle stage.
    // +optional
    Phase SnapshotJobPhase `json:"phase,omitempty"`

    // ContentName is the name of the produced SnapshotContent. Restore
    // paths reference the artifact by this name.
    // +optional
    ContentName string `json:"contentName,omitempty"`

    // Ready is true when ContentName references a Ready SnapshotContent.
    // +optional
    Ready bool `json:"ready,omitempty"`

    // StartedAt is the time at which the SnapshotJob entered the
    // Running phase.
    // +optional
    StartedAt *metav1.Time `json:"startedAt,omitempty"`

    // CompletedAt is the time at which the SnapshotJob reached a
    // terminal phase (Succeeded or Failed).
    // +optional
    CompletedAt *metav1.Time `json:"completedAt,omitempty"`

    // Message provides a human-readable explanation of the current phase.
    // +optional
    Message string `json:"message,omitempty"`
}

// SnapshotJobPhase is the lifecycle stage of a SnapshotJob.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
type SnapshotJobPhase string

const (
    // SnapshotJobPhasePending: SnapshotJob is created but pod has not started.
    SnapshotJobPhasePending SnapshotJobPhase = "Pending"

    // SnapshotJobPhaseRunning: pod is running; may be in quiesce or dump phase.
    SnapshotJobPhaseRunning SnapshotJobPhase = "Running"

    // SnapshotJobPhaseSucceeded: dump complete; SnapshotContent + Snapshot produced.
    SnapshotJobPhaseSucceeded SnapshotJobPhase = "Succeeded"

    // SnapshotJobPhaseFailed: dump failed (deadline, pod failure, agent error).
    SnapshotJobPhaseFailed SnapshotJobPhase = "Failed"
)
```

---

### 2.4 RestoreSnapshot

**Role.** The operator-driven restore object. Targets an existing pod and drives CRIU replay against a referenced `Snapshot` or `SnapshotContent`. Complements the annotation-driven restore path (§4) by giving operators a first-class CRD they can `kubectl get`, watch, and report status against.

```go
// RestoreSnapshot drives CRIU replay of a captured artifact into an
// existing pod. Complements Mode A annotation-driven restore (§4) by
// giving operators a discoverable, status-bearing CRD.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=restoresnap
type RestoreSnapshot struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`

    Spec   RestoreSnapshotSpec   `json:"spec,omitempty"`
    Status RestoreSnapshotStatus `json:"status,omitempty"`
}
```

#### Spec

```go
// RestoreSnapshotSpec describes a single restore operation.
// +kubebuilder:validation:XValidation:rule="self.source == oldSelf.source",message="source is immutable after creation"
// +kubebuilder:validation:XValidation:rule="self.podRef == oldSelf.podRef",message="podRef is immutable after creation"
type RestoreSnapshotSpec struct {
    // Source identifies the artifact to restore from. Exactly one of
    // Source.SnapshotName or Source.SnapshotContentName must be set.
    // +kubebuilder:validation:Required
    Source RestoreSource `json:"source"`

    // PodRef identifies the existing pod to restore into. The pod must
    // be in the same namespace as the RestoreSnapshot. The pod must
    // exist and be in a state compatible with restore shaping (see
    // §4 Restore consumption for the API contract).
    // +kubebuilder:validation:Required
    PodRef PodReference `json:"podRef"`

    // TargetContainers narrows which containers in the target pod are
    // restored. If empty, all containers covered by the referenced
    // artifact are restored.
    // +optional
    TargetContainers []string `json:"targetContainers,omitempty"`
}

// RestoreSource is a tagged union; exactly one variant must be set.
// +kubebuilder:validation:XValidation:rule="(has(self.snapshotName) && self.snapshotName != '' ? 1 : 0) + (has(self.snapshotContentName) && self.snapshotContentName != '' ? 1 : 0) == 1",message="exactly one of snapshotName or snapshotContentName must be set"
type RestoreSource struct {
    // SnapshotName references a Snapshot in the same namespace as the
    // RestoreSnapshot. The referenced Snapshot must have
    // status.readyToUse == true.
    // +optional
    SnapshotName string `json:"snapshotName,omitempty"`

    // SnapshotContentName references a SnapshotContent (cluster-scoped).
    // The referenced SnapshotContent must have status.readyToUse == true.
    // +optional
    SnapshotContentName string `json:"snapshotContentName,omitempty"`
}
```

#### Status

```go
// RestoreSnapshotStatus is the observed state of a restore operation.
type RestoreSnapshotStatus struct {
    // Phase is the high-level lifecycle stage.
    // +optional
    Phase RestoreSnapshotPhase `json:"phase,omitempty"`

    // StartedAt is the time at which restore shaping began on the target pod.
    // +optional
    StartedAt *metav1.Time `json:"startedAt,omitempty"`

    // CompletedAt is the time at which the restore reached a terminal phase.
    // +optional
    CompletedAt *metav1.Time `json:"completedAt,omitempty"`

    // Message provides a human-readable explanation of the current phase.
    // +optional
    Message string `json:"message,omitempty"`
}

// RestoreSnapshotPhase is the lifecycle stage of a RestoreSnapshot.
// +kubebuilder:validation:Enum=Pending;Restoring;Ready;Failed
type RestoreSnapshotPhase string

const (
    // RestoreSnapshotPhasePending: RestoreSnapshot exists; target pod
    // not yet shaped, or referenced artifact not yet Ready.
    RestoreSnapshotPhasePending RestoreSnapshotPhase = "Pending"

    // RestoreSnapshotPhaseRestoring: target pod has been shaped; CRIU
    // replay is in progress.
    RestoreSnapshotPhaseRestoring RestoreSnapshotPhase = "Restoring"

    // RestoreSnapshotPhaseReady: CRIU replay completed; the target
    // pod's restored containers are running.
    RestoreSnapshotPhaseReady RestoreSnapshotPhase = "Ready"

    // RestoreSnapshotPhaseFailed: restore failed and will not be retried automatically.
    RestoreSnapshotPhaseFailed RestoreSnapshotPhase = "Failed"
)
```

**Notes.**

- A single `RestoreSnapshot` targets one pod (`PodRef`). To restore many pods, create one RestoreSnapshot per pod (typically driven by an outer controller).
- The same artifact (`Source`) may be restored into many target pods via many RestoreSnapshots; the SnapshotContent is not consumed by restore.
- Mode A (annotation-on-pod) and Mode B (this CRD) coexist; they apply to disjoint pods (a pod is either restore-annotated or referenced from a RestoreSnapshot, not both). See §4.

---

## 3. Shared types

### 3.1 QuiesceProbe

**Role.** Defines how the snapshot agent determines that a target container is in a state safe to checkpoint. Modeled after `corev1.Probe` (familiar shape, reusing `corev1` action types where possible) but evaluated by the snapshot agent — not kubelet — so it can be applied without affecting the pod's serving readiness.

```go
// QuiesceProbe describes how the snapshot agent detects that the
// target containers are ready to be dumped. The same type is referenced
// from SnapshotJob.Spec and (in future versions) Snapshot.Spec.
type QuiesceProbe struct {
    // Action is the probe operation. Exactly one variant in Action must be set.
    // +kubebuilder:validation:Required
    Action QuiesceProbeAction `json:"action"`

    // PeriodSeconds is the interval between consecutive probe attempts.
    // +optional
    // +kubebuilder:default=1
    // +kubebuilder:validation:Minimum=1
    PeriodSeconds int32 `json:"periodSeconds,omitempty"`

    // TimeoutSeconds is the per-attempt timeout. A probe attempt that
    // does not complete within this many seconds counts as a failure.
    // +optional
    // +kubebuilder:default=1
    // +kubebuilder:validation:Minimum=1
    TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

    // SuccessThreshold is the number of consecutive successes required
    // before the probe is considered satisfied and the dump begins.
    // +optional
    // +kubebuilder:default=1
    // +kubebuilder:validation:Minimum=1
    SuccessThreshold int32 `json:"successThreshold,omitempty"`

    // FailureThreshold is the number of consecutive failures after which
    // the agent abandons the probe. Bounded; SnapshotJob.ActiveDeadlineSeconds
    // is the wall-clock hard deadline.
    // +optional
    // +kubebuilder:default=7200
    // +kubebuilder:validation:Minimum=1
    FailureThreshold int32 `json:"failureThreshold,omitempty"`
}

// QuiesceProbeAction is a tagged union of probe actions.
// Exactly one variant must be set.
type QuiesceProbeAction struct {
    // HTTPGet probes an HTTP endpoint on the target container.
    // +optional
    HTTPGet *corev1.HTTPGetAction `json:"httpGet,omitempty"`

    // Exec runs a command inside the target container.
    // +optional
    Exec *corev1.ExecAction `json:"exec,omitempty"`

    // TCPSocket probes a TCP port on the target container.
    // +optional
    TCPSocket *corev1.TCPSocketAction `json:"tcpSocket,omitempty"`

    // GRPC probes via the standard gRPC health protocol.
    // +optional
    GRPC *corev1.GRPCAction `json:"grpc,omitempty"`

    // File probes for the existence of a sentinel file in the target
    // container's filesystem. NVSnapshot-specific (no corev1 equivalent).
    // This is the default mechanism if QuiesceProbe is omitted entirely.
    // +optional
    File *FileAction `json:"file,omitempty"`
}

// FileAction probes for the existence of a sentinel file inside the
// target container's filesystem.
type FileAction struct {
    // Path is the absolute path to the sentinel file inside the container.
    // +kubebuilder:validation:Required
    Path string `json:"path"`
}
```

**Default when `QuiesceProbe` is unset on `SnapshotJob`:**

```go
QuiesceProbe{
    Action: QuiesceProbeAction{
        File: &FileAction{Path: "/var/run/nvsnapshot/ready-for-checkpoint"},
    },
    PeriodSeconds:    1,
    TimeoutSeconds:   1,
    SuccessThreshold: 1,
    FailureThreshold: 7200,
}
```

### 3.2 Pod conditions

NVSnapshot publishes state via custom pod conditions. These are **observability-only** — they do not participate in `pod.status.conditions[type=Ready]` (no readiness gate). The pod's own readinessProbe continues to govern traffic routing independently.

| Condition type | Meaning |
|---|---|
| `nvsnapshot.io/quiesce-ready` | `QuiesceProbe` succeeded; CRIU dump about to start |
| `nvsnapshot.io/snapshotted`   | CRIU dump complete; artifact written to PVC |
| `nvsnapshot.io/restored`      | CRIU replay complete on a restore pod |

Consumers observe these via `kubectl wait --for=condition=...` and standard pod-status APIs.

---

## 4. Restore consumption

Two modes, both shipped in v1alpha1. A given pod uses exactly one — modes apply to disjoint pods.

### 4.1 Mode A — annotation-on-pod

A pod opts into restore via an annotation on its PodTemplate:

```yaml
metadata:
  annotations:
    nvsnapshot.io/restore-from: my-snap                   # Snapshot in same namespace
    # — or —
    nvsnapshot.io/restore-from-content: my-content-<uid>  # SnapshotContent (cluster-scoped)
    nvsnapshot.io/restore-target-containers: main         # optional override
```

**API contract:**

- The restore annotation is **purely additive**. Pod creation is never blocked by NVSnapshot.
- If the referenced target exists and is in a ready state (`Snapshot.status.readyToUse == true` or `SnapshotContent.status.readyToUse == true`) at pod admission, the pod's target containers are shaped for CRIU replay; the pod publishes `nvsnapshot.io/restored` on completion.
- If the reference is missing or not Ready at admission, the pod admits **unchanged** — no restore shaping is applied. Callers requiring a guaranteed restore are responsible for gating pod creation on the referenced object's readiness.

### 4.2 Mode B — RestoreSnapshot CRD

The operator creates a `RestoreSnapshot` (see §2.4) referencing an existing pod via `Spec.PodRef`. The restore engine shapes that pod's target containers for CRIU replay and reports progress on `RestoreSnapshot.Status`.

**API contract:**

- The target pod must exist in the same namespace as the RestoreSnapshot. If the pod does not exist at RestoreSnapshot creation, the RestoreSnapshot remains in phase `Pending`; on pod appearance the controller transitions to `Restoring`.
- The referenced source must have `readyToUse == true` on its status (Snapshot or SnapshotContent). If not ready, the RestoreSnapshot stays in `Pending` until the source becomes ready or the RestoreSnapshot is deleted (`Spec.Source` is immutable).
- On successful CRIU replay, the target pod publishes `nvsnapshot.io/restored` and the RestoreSnapshot transitions to `Ready`.
- A given pod must not be referenced by both an `nvsnapshot.io/restore-from*` annotation and a `RestoreSnapshot.Spec.PodRef`. If both apply, the annotation path wins; the RestoreSnapshot transitions to `Failed` with an explanatory message.

**When to use which:**

- **Mode A** is natural when an outer controller (e.g., a Deployment, StatefulSet, or a higher-level operator like DynamoComponentDeployment) already manages the pod's PodTemplate and wants restore to be a property of that template.
- **Mode B** is natural when an operator wants restore to be a first-class status-bearing object — discoverable via `kubectl get restoresnapshot`, with failures localized to a dedicated status — and pod lifecycle is managed elsewhere.

---

## 5. Storage (PVC) requirements

v1alpha1 supports PVC-backed storage only.

| Requirement | Value |
|---|---|
| Access mode | `ReadWriteMany` recommended for production. Single-node test/dev MAY use `ReadWriteOnce` with appropriate nodeAffinity. |
| StorageClass | Must back onto storage that supports the chosen access mode; high sequential write throughput recommended. |
| PVC ownership | User-supplied, pre-existing. NVSnapshot does not provision PVCs in v1alpha1. |
| Sizing | `resident_memory + gpu_memory + overlay_delta`. For an 80GB H100 worker, plan ~80–120 GB per snapshot. |

### Path layout (protocol convention)

```
<basePath>/
  manifest.json      # version, file list, hashes, platform metadata
  criu/              # CRIU image files
  cuda/              # CUDA device-memory blobs
  oci/               # OCI bundle metadata, overlay deltas
```

Layout is fixed protocol. Tools reading the artifact rely on it.

---

## 6. Out of scope for v1alpha1

Explicitly deferred. Each is reachable from the current API surface without a breaking change unless noted.

- **Identity-based dedup** (`identityKey`/`identityHash`). Applications dedup at their own layer (e.g., DynamoCheckpoint).
- **Pre-provisioned import binding via Snapshot** (`Snapshot.spec.source.snapshotContentName`). Importing an externally-produced SnapshotContent and binding a Snapshot to it is deferred. (Restore can still consume an imported SnapshotContent directly via `RestoreSnapshot.spec.source.snapshotContentName`.)
- **Multi-driver support** / `SnapshotClass`. Single implementation (CRIU + cuda-checkpoint) hardcoded; `SnapshotContent.spec.driver` field is not present in v1alpha1 and will be added when a second driver appears.
- **Object-storage / URI sources** for SnapshotContent. PVC only in v1alpha1; the opaque `SnapshotHandle` string already accommodates future backends with new URI schemes (`s3://...`, `oci://...`) — additive, non-breaking.
- **Enforcement of `Platform`** — nodeAffinity injection, driver-version gates. Today the field is descriptive only.
- **Cross-cluster portability** automation.
- **Auto-GC of orphan PVC paths**.
- **Dynamic PVC provisioning** (per-snapshot PVCs from a StorageClass).
- **Rich platform metadata** (kernel version, container runtime, MIG profile, GPU count, etc.).
- **Restore-side `RestoreReadyProbe`** symmetric to `QuiesceProbe`.

---

## 7. Open questions

### Q1 — Application-facing `snapshotted` / `restored` signals

Workloads need an in-container notification that the snapshot completed, and on restore that the wake-up was a CRIU replay rather than a cold start.

- **Provisional v1alpha1**: reserved-name files at `<controlVolumeMount>/snapshotted` (on source pod post-dump) and `<controlVolumeMount>/restored` (on restore pod post-replay). Documented as alpha-stability — may be replaced or removed.
- **Resolve before v1beta1**: pick between (a) keep file convention, (b) expose the `nvsnapshot.io/snapshotted` and `nvsnapshot.io/restored` pod conditions via Downward API as files, (c) wait for upstream Kubernetes/CRIU standardization.
