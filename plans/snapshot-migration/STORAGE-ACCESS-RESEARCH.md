<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# PodSnapshot PVC storage access research

**Status:** research and option analysis; no architecture is selected by this document.

**Question:** how should Dynamo represent PodSnapshot storage when the snapshot agent reaches a
PVC either through the workload Pod or through storage mounted directly on the node agent?

**Current backend boundary:** PVC only. S3 and OCI are intentionally outside the implementation
scope of this analysis, but the API options are evaluated for whether they leave room for another
backend later.

## 1. Executive summary

Dynamo already implements two PVC data-access paths, but the choice is deployment-global and is not
represented in `PodSnapshot` or `PodSnapshotContent`:

- `agentMount`: the snapshot-agent DaemonSet mounts the checkpoint PVC and reads or writes through
  its own filesystem namespace.
- `podMount`: the workload Pod mounts the checkpoint PVC and the privileged node agent reaches that
  mount through `/host/proc/<container-pid>/root`.

The distinction describes how the agent reaches the files. It does **not** mean that only one Pod
mounts the PVC. Restore still needs the target container to see the checkpoint directory because
`nsrestore` receives both a host-visible checkpoint path and the path visible inside the target
container.

The current implementation has four separate sources of storage truth:

1. operator configuration can define and optionally create a namespace-local PVC;
2. the operator injects the PVC and storage annotations into capture and restore Pods;
3. the snapshot-agent ConfigMap defines `type`, `basePath`, and `accessMode`;
4. the agent combines Pod annotations with its own configuration at execution time.

`PodSnapshotContent` is an immutable node work order, but it currently carries no resolved storage
input. Consequently, a work order does not say which PVC contains the artifact or which access path
the selected node agent must use.

A reusable, administrator-owned storage object is a reasonable direction, but PVC storage makes its
shape less straightforward than GKE's object-storage configuration:

- PVCs are namespaced.
- A cluster-scoped object cannot directly mount a PVC.
- A running DaemonSet Pod cannot acquire a new PVC mount without recreation.
- A `ReadWriteOnce` PVC can be shared by multiple Pods only while they are on the same node.
- A restore Pod must be able to mount the artifact PVC on its destination node.

The main unresolved architectural choice is therefore not the CRD name. It is the storage data
plane:

- keep accessing a volume already mounted into the workload Pod;
- pre-mount one or more stores into every relevant node agent;
- access kubelet's host-side Pod volume mount;
- or create a privileged, same-node operation Pod that mounts the PVC for each save or restore.

## 2. Terminology

The following concepts must remain separate in the API and documentation:

| Term | Meaning |
|---|---|
| Backend | The durable storage technology. This document considers only `pvc`. |
| Access strategy | How snapshot code reaches the mounted filesystem: through the workload Pod, a node-agent mount, a host kubelet path, or an operation Pod. |
| PVC access mode | Kubernetes `ReadWriteOnce`, `ReadWriteMany`, or `ReadWriteOncePod`. This is not the snapshot access strategy. |
| Storage configuration | Administrator policy that selects a backend and an access strategy. |
| Resolved storage binding | Immutable, operation-specific identity of the claim, mount contract, and artifact key. |
| Snapshot handle | Agent-produced identifier for an artifact that can later be resolved for restore. |
| Base path | Mount point at which a PVC is visible to a Pod or agent, such as `/checkpoints`. |
| Artifact path | System-owned path below the storage root for one captured artifact. |

Names such as `accessMode` are ambiguous because Kubernetes already uses that term for PVC access
modes. A future CRD should prefer `accessStrategy`, `accessor`, or a structural union such as
`pod: { ... }` versus `nodeAgent: { ... }`.

## 3. Current Dynamo implementation

### 3.1 API objects

`PodSnapshot` is namespaced and contains only:

```yaml
spec:
  source:
    podRef:
      name: string
      uid: string
status:
  boundSnapshotContentName: string
  conditions: []
```

`PodSnapshotContent` is cluster-scoped and contains only the back-reference, source Pod identity,
and source node:

```yaml
spec:
  snapshotRef:
    namespace: string
    name: string
    uid: string
  source:
    podRef:
      name: string
      uid: string
    nodeName: string
status:
  conditions: []
```

The definitions are in:

- `deploy/operator/api/v1alpha1/podsnapshot_types.go`
- `deploy/operator/api/v1alpha1/podsnapshotcontent_types.go`

There is no storage configuration reference, resolved claim reference, access strategy, artifact
path, or snapshot handle in either object.

### 3.2 Operator storage configuration

`CheckpointConfiguration.Storage` in
`deploy/operator/api/config/v1alpha1/types.go` currently supports PVC settings:

- `pvcName`
- `basePath`
- `create`
- `size`
- `storageClassName`
- Kubernetes PVC `accessMode`

`deploy/operator/internal/checkpoint/storage.go`:

- normalizes and validates the PVC configuration;
- optionally creates a claim with `ReadWriteMany` as the default;
- resolves the checkpoint location as
  `<basePath>/<checkpointID>/versions/<artifactVersion>`;
- falls back to discovering a snapshot-agent DaemonSet when operator storage is omitted.

The operator injects the claim and base-path mount into capture and restore Pod specs. It also writes
the storage type and base path as Pod annotations. The claim name is represented by the Pod volume,
not by a storage annotation.

### 3.3 Agent configuration

`deploy/snapshot/internal/types/config.go` defines:

```yaml
storage:
  type: pvc
  basePath: /checkpoints
  accessMode: agentMount | podMount
```

The default is `agentMount`. Only PVC is accepted.

The Helm chart in `deploy/helm/charts/snapshot` renders the agent ConfigMap and:

- mounts the PVC into the DaemonSet only for `agentMount`;
- does not mount it into the DaemonSet for `podMount`;
- always gives the privileged agent host PID access, host `/proc`, the runtime socket, overlay
  storage, cgroups, and the kubelet Pod directory.

Because the access strategy is agent configuration, all snapshots handled by one agent deployment
use the same strategy.

### 3.4 Capture path

The operator creates a `PodSnapshotContent` pinned to the source Pod UID and node. The node agent:

1. watches the content;
2. retrieves and validates the source Pod;
3. reads checkpoint identity, target container, storage type, and base path from the Pod and its
   own configuration;
4. resolves the target container's host PID;
5. derives host-visible and container-visible storage paths;
6. runs CRIU/CUDA/rootfs capture;
7. atomically promotes the temporary directory to the final artifact directory;
8. marks `PodSnapshotContent` ready.

The relevant code is:

- `deploy/operator/internal/controller/podsnapshot_reconciler.go`
- `deploy/snapshot/internal/controller/podsnapshotcontent.go`
- `deploy/snapshot/internal/controller/controller.go`
- `deploy/snapshot/internal/executor/checkpoint.go`

For `agentMount`:

```text
HostPath      = /checkpoints/<checkpointID>/versions/<version>
ContainerPath = /checkpoints/<checkpointID>/versions/<version>
```

For `podMount`:

```text
ContainerPath = /checkpoints/<checkpointID>/versions/<version>
HostPath      = /host/proc/<pid>/root/checkpoints/<checkpointID>/versions/<version>
```

The `podMount` path is recomputed after resolving the live PID, and the agent validates that the PID
still represents the same container before accessing it.

### 3.5 Restore path

The operator or mutation webhook:

- chooses a ready checkpoint;
- injects the checkpoint PVC and mount into the target container;
- places the target process in restore standby;
- adds control-volume and readiness plumbing;
- stamps restore metadata on the Pod.

The node agent:

1. resolves the placeholder container and its host PID;
2. computes the checkpoint host path according to its configured access strategy;
3. verifies the artifact;
4. passes both the host-visible path and container-visible path to restore;
5. runs the external restore;
6. writes completion state.

This is implemented in:

- `deploy/operator/internal/checkpoint/podspec.go`
- `deploy/operator/internal/webhook/mutation/pod_checkpoint_restore_handler.go`
- `deploy/snapshot/protocol/restore.go`
- `deploy/snapshot/internal/controller/controller.go`
- `deploy/snapshot/internal/executor/restore.go`

Even in `agentMount`, the restore target needs a mount at the container checkpoint path. Direct agent
access therefore does not eliminate workload-Pod storage mutation for the current restore mechanism.

### 3.6 Current storage discovery

When explicit operator storage is absent, restore discovers a snapshot-agent DaemonSet in the
workload namespace and looks for:

- container name `agent`;
- volume name `checkpoints`;
- a PVC-backed DaemonSet volume;
- the corresponding agent mount path.

This legacy discovery assumes a namespace-local agent. It cannot discover a central DaemonSet in an
infrastructure namespace for a workload in another namespace. The operator's explicit storage
configuration is the current bridge for central `podMount` deployments.

### 3.7 Current correctness and security gaps

- A `PodSnapshotContent` cannot independently explain where its artifact will be written.
- Capture success records only conditions; no resolved artifact handle is persisted.
- Restore recomposes the path from checkpoint identity, annotations, and live configuration.
- A configuration change between capture and restore can redirect resolution.
- Storage base path can come from a Pod annotation. Although the implementation requires an
  absolute clean path and validates the live PID, the annotation still influences a privileged
  node-agent filesystem access. Future storage placement should come from an administrator-owned
  object or a controller-resolved immutable binding.
- `agentMount` has no explicit containment check proving the final path remains under a designated
  mounted storage root.
- `podMount` proves that the path exists through the container root, but it does not currently bind
  that path in the CRD to a particular Pod volume and PVC UID.
- Artifact identity remains `<checkpointID>/versions/<version>`, so delete/recreate and concurrent
  work orders require separate Lease and in-process guards.

## 4. Kubernetes and CSI constraints

### 4.1 PVC namespace

A Pod can reference only a PVC in the same namespace. Kubernetes resolves
`persistentVolumeClaim.claimName` in the Pod's namespace and mounts the backing PV into the Pod.

Implications:

- a cluster-scoped storage object should not treat an unqualified claim name as one unique cluster
  resource;
- a Pod-based strategy can resolve the real claim from the source Pod and snapshot namespace;
- an agent-mounted claim belongs to the agent Pod's namespace, which may differ from the
  `PodSnapshot` namespace;
- cross-namespace restore requires an explicit copy/replication mechanism or a different backend;
  a claim reference alone cannot cross the namespace boundary.

Source: [Kubernetes Persistent Volumes: Claims as
Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#claims-as-volumes).

### 4.2 PVC access modes

- `ReadWriteOnce` means a volume may be mounted read-write by one node. Multiple Pods on that same
  node may use it.
- `ReadWriteMany` permits read-write mounts from many nodes when the storage backend supports it.
- `ReadWriteOncePod` permits one Pod cluster-wide and is incompatible with a design that requires
  the workload plus a helper or agent Pod to mount the claim simultaneously.

Implications:

- `podMount` works naturally with RWO because only the workload Pod must mount the claim.
- a same-node operation Pod can share an RWO claim with the workload, but not an RWOP claim;
- one DaemonSet replica per node mounting the same claim normally requires RWX;
- restore to another node requires the storage backend to detach/reattach an RWO volume or provide
  multi-node access.

Sources:

- [Kubernetes Persistent Volume access
  modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes)
- [Kubernetes ReadWriteOncePod
  guidance](https://kubernetes.io/docs/tasks/administer-cluster/change-pv-access-mode-readwriteoncepod/)

### 4.3 Running Pod immutability

Kubernetes does not permit adding volumes or volume mounts to a running Pod. A Pod template change
causes a controller to create replacement Pods.

Implication: a storage CRD cannot dynamically add an arbitrary PVC to an existing snapshot-agent
DaemonSet Pod. A direct claim reference for node-agent access must be paired with:

- a pre-existing mount;
- a controlled DaemonSet rollout;
- or a separate operation Pod.

Source: [Kubernetes Pod update and
replacement](https://kubernetes.io/docs/concepts/workloads/pods/#pod-update-and-replacement).

### 4.4 CSI node publication

Kubelet directly calls CSI `NodeStageVolume` and `NodePublishVolume` over the registered CSI node
plugin socket. Normal Kubernetes components request a mount by creating/scheduling a Pod that
references a PVC; they do not invoke these CSI node methods themselves.

Implications:

- a Dynamo agent directly issuing CSI node calls would duplicate kubelet ownership, target-path
  lifecycle, credentials, idempotency, and unpublish behavior;
- the Kubernetes-native way to obtain a dynamic mount is to schedule a Pod that references the
  claim;
- an operation Pod should use scheduler-visible node affinity rather than bypassing scheduling
  constraints blindly, so PVC topology and attachment predicates are evaluated.

Sources:

- [Kubernetes CSI developer
  documentation](https://kubernetes-csi.github.io/docs/)
- [Assigning Pods to
  Nodes](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/)

### 4.5 Host-side Pod volume paths

A privileged DaemonSet can mount the kubelet Pod directory and access volumes already published for
Pods on that node. Velero File System Backup uses this design. It also documents important
limitations:

- the kubelet root path varies across platforms;
- access may require root and privileged mode;
- only volumes already mounted by a Pod are available;
- some environments, including some virtual-cluster layouts, do not expose the expected
  `<kubelet-root>/pods/<pod-uid>` layout.

Sources:

- [Velero File System
  Backup](https://velero.io/docs/main/file-system-backup/)
- [Velero installation and kubelet root
  customization](https://velero.io/docs/main/customize-installation/#customize-the-kubelet-root-path-of-the-node-agent)

## 5. Prior API and controller patterns

### 5.1 GKE Pod snapshots

GKE separates:

- cluster-scoped storage configuration;
- namespaced snapshot policy and snapshot resources;
- node-agent execution;
- controller lifecycle and cleanup;
- agent-produced state stored in Cloud Storage.

The workload selects no raw filesystem path. `PodSnapshotStorageConfig` contains the administrative
storage destination, while the policy references it by name.

This is strong precedent for a reusable administrator-owned storage configuration, but the backend
is Cloud Storage. It does not solve PVC namespacing or dynamic node attachment.

Sources:

- [GKE Pod snapshot
  architecture](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots)
- [GKE Pod snapshot CRD
  reference](https://docs.cloud.google.com/kubernetes-engine/docs/reference/crds/podsnapshot)

### 5.2 CSI VolumeSnapshot

CSI separates:

- `VolumeSnapshot`: namespaced user request;
- `VolumeSnapshotClass`: cluster-scoped administrator policy;
- `VolumeSnapshotContent`: cluster-scoped binding and backend identity;
- `snapshotHandle`: driver-produced backend identifier.

This supports the following Dynamo principles:

- a user object references an administrator-selected class/configuration;
- the content object records the resolved binding;
- the actor that creates the physical artifact publishes the handle;
- user-provided paths should not become privileged filesystem destinations.

Sources:

- [Kubernetes Volume
  Snapshots](https://kubernetes.io/docs/concepts/storage/volume-snapshots/)
- [Kubernetes VolumeSnapshotClass](https://kubernetes.io/docs/concepts/storage/volume-snapshot-classes/)

### 5.3 Velero

Velero separates reusable storage locations from individual backup requests. Its node agent
demonstrates two relevant data-plane patterns:

- privileged host traversal of volumes already mounted for Pods;
- short-lived data-mover Pods for some backup and restore operations.

Velero also makes the kubelet root configurable and documents environment-specific host-path
limitations. This is evidence that host traversal is workable, but not a portable abstraction that
should be hidden.

Sources:

- [Velero storage
  locations](https://velero.io/docs/main/locations/)
- [Velero File System
  Backup](https://velero.io/docs/main/file-system-backup/)
- [Velero node-agent
  configuration](https://velero.io/docs/main/supported-configmaps/node-agent-configmap/)

## 6. PVC access architecture options

### Option A: workload-Pod mount through `/proc/<pid>/root`

This is the current `podMount` implementation.

Capture:

```text
PodSnapshotContent
  -> node agent resolves source container PID
  -> agent validates PID and Pod UID
  -> /host/proc/<pid>/root/<mountPath>
  -> checkpoint files on the Pod's PVC
```

Restore:

```text
restore Pod mounts the artifact PVC
  -> node agent resolves placeholder PID
  -> /host/proc/<pid>/root/<mountPath>
  -> nsrestore receives host path and container path
```

Advantages:

- supports namespace-local PVCs without mounting them in the central agent namespace;
- works with RWO because the workload itself owns the node attachment;
- supports different claims in different namespaces;
- no additional privileged operation Pod;
- already implemented and tested;
- no dependency on kubelet's on-disk volume directory layout.

Disadvantages and risks:

- depends on a stable live container PID and mount namespace;
- the agent must remain privileged with host PID and `/proc` access;
- a path alone is insufficient provenance; it should be resolved to a specific Pod volume and PVC;
- a compromised or incorrectly prepared workload could present an unexpected filesystem at the
  configured path;
- restore cannot begin until the placeholder Pod and its PVC mount exist;
- RWOP remains possible only when no other helper Pod also mounts the claim.

Required hardening:

- storage configuration identifies a Pod `volumeName` or expected mount contract, not a raw
  user-controlled destination;
- controller resolves and pins claim namespace, name, and UID into the content work order;
- agent verifies that the target container mount uses that volume and expected mount path;
- reject `subPath`, mount propagation, read-only capture destinations, and non-PVC volume sources
  unless each is explicitly supported;
- derive the artifact path from system-owned identity and assert containment below the mounted root.

### Option B: PVC mounted directly into the node-agent DaemonSet

This is the current `agentMount` implementation.

Capture:

```text
PodSnapshotContent
  -> selected node agent
  -> agent's own /checkpoints mount
  -> shared PVC
```

Restore still requires:

```text
agent reads its own PVC mount
  + restore target sees the same artifact through its container mount
```

Advantages:

- simplest and most direct filesystem access;
- independent of source container PID for storage access;
- easy atomic rename and cleanup in the agent namespace;
- explicit mount path controlled by the agent deployment;
- no kubelet internal path discovery.

Disadvantages and risks:

- all DaemonSet replicas mounting one claim generally requires RWX;
- the PVC must exist in the DaemonSet namespace;
- a running agent cannot dynamically mount another claim;
- per-tenant or per-namespace claims require separate agent deployments or many static mounts;
- an RWO claim prevents simultaneous mounts from agent replicas on different nodes;
- restore target still needs a compatible view of the same storage;
- a global base-path annotation must not be allowed to redirect writes outside the mounted store.

Best fit:

- one trusted, shared RWX checkpoint store;
- a namespace-local agent installation with one claim;
- environments where predictable simplicity is more important than tenant isolation.

### Option C: named, pre-mounted node-agent bindings

The DaemonSet mounts one or more stores at installation time and gives each a logical name:

```yaml
storageBindings:
  primary:
    backend: pvc
    mountPath: /stores/primary
  archive:
    backend: pvc
    mountPath: /stores/archive
```

A cluster-scoped storage configuration selects `bindingName: primary`. The agent validates that the
binding exists locally.

Advantages:

- keeps PVC namespace and mount details under administrator control;
- a snapshot work order can state which configured store it requires;
- supports multiple pre-mounted stores without raw paths in user objects;
- agent validation is local and deterministic;
- gives a future object-store backend a similar logical-selection mechanism.

Disadvantages and risks:

- still static: adding or changing a binding rolls the DaemonSet;
- every applicable node must have the same logical binding;
- many bindings increase DaemonSet spec size and PVC attachment pressure;
- shared block PVCs still face RWO/RWX constraints;
- a config reference can become invalid if a binding is removed after capture.

Required lifecycle rule:

- content must pin the resolved binding and final handle at capture time;
- changing or deleting the reusable storage configuration must not change how an existing snapshot
  resolves;
- agent deployment validation should report binding readiness before accepting work.

### Option D: direct PVC reference with automated DaemonSet rollout

A storage configuration names a claim in the agent namespace. A controller patches the DaemonSet
template to mount it and waits for rollout.

Advantages:

- self-describing Kubernetes API;
- administrators can add a store by creating a CR instead of editing Helm values;
- normal kubelet/CSI attachment remains responsible for mounting.

Disadvantages and risks:

- storage policy now mutates infrastructure deployment state;
- a new storage config can restart every node agent;
- concurrent configuration changes require a deterministic merge and ownership model;
- removing a config may strand existing snapshot handles;
- RWO/RWX constraints remain;
- cross-controller ownership of a Helm-managed DaemonSet creates drift;
- rollout during active checkpoint/restore can interrupt operations.

Conclusion:

This option is feasible only as a larger storage-binding controller. A plain CRD field without that
controller is misleading and must not be presented as dynamic agent access.

### Option E: privileged, same-node operation Pod

The resident controller or agent creates a short-lived Pod for one checkpoint or restore. The Pod:

- mounts the selected namespace-local PVC normally;
- is constrained to the workload's node through scheduler-visible affinity;
- receives the runtime socket, host PID/proc, cgroup, and overlay access needed for CRIU;
- writes operation status back to `PodSnapshotContent`.

Advantages:

- Kubernetes and kubelet dynamically mount the PVC;
- supports different namespace-local claims without changing the DaemonSet;
- RWO can work when workload and operation Pod run on the same node;
- storage access is explicit in the operation Pod spec;
- avoids accessing checkpoint storage through the workload container root;
- operation resources, priority, timeout, and retries can be controlled per job.

Disadvantages and risks:

- the operation Pod is itself highly privileged;
- RWOP cannot be shared with the workload Pod;
- scheduling and PVC topology must converge on the already-selected workload node;
- startup latency is added to every save and restore;
- operation Pod creation, cancellation, garbage collection, retry, and status recovery become new
  controller state machines;
- runtime/container identity must be handed off safely;
- restore coordination becomes more complex because the placeholder Pod and operation Pod coexist;
- a node failure may leave operation CRs and temporary directories requiring cleanup.

Required design work:

- define an operation resource or durable status transaction marker;
- use node affinity that allows the scheduler to evaluate PVC constraints;
- establish owner references/finalizers and bounded retries;
- define whether the operation Pod or resident agent owns the Lease;
- prevent two worker Pods from writing the same artifact;
- prove that CRIU and CUDA operations work from the worker's namespace and host mounts.

### Option F: host traversal of kubelet-managed Pod volumes

The agent mounts the kubelet root, finds the source Pod UID and volume name, and accesses the
host-side published volume. Velero uses this general approach.

Advantages:

- the workload remains the Kubernetes consumer of the PVC;
- no `/proc/<pid>/root` dependency;
- no extra operation Pod;
- RWO works because the published volume is already on the workload node;
- volume identity can be based on Pod UID and volume name.

Disadvantages and risks:

- kubelet root differs by platform and must be configurable;
- CSI/in-tree volume directory shapes and mount propagation vary;
- vCluster and some managed environments may not expose expected paths;
- requires privileged hostPath access to all Pod volumes on the node;
- couples Dynamo to kubelet storage internals;
- only works after a Pod has caused the volume to be published;
- restore ordering must keep the placeholder and published mount alive.

This is credible precedent, not automatically preferable to current `/proc/<pid>/root` access.
A cluster compatibility matrix is required before selecting it.

### Option G: injected workload sidecar/helper

The operator injects a privileged or RPC-enabled helper into the workload Pod. The helper mounts the
checkpoint PVC and cooperates with the node agent.

Advantages:

- the PVC is naturally in the workload namespace;
- no kubelet path discovery;
- helper lifecycle follows the workload;
- communication can use a narrow protocol instead of arbitrary host filesystem traversal.

Disadvantages and risks:

- mutates every workload Pod;
- the helper may need privileges approaching those of the node agent;
- GPU/CRIU operations still require host runtime access or an agent RPC;
- sidecar resource overhead persists for the workload lifetime;
- restore bootstrap and ordering become more complex;
- users may object to an injected container and image supply-chain dependency.

This option has little advantage over `podMount` unless it can substantially reduce node-agent
privilege, which must be demonstrated.

### Option H: node agent calls CSI directly

The node agent calls `NodeStageVolume`/`NodePublishVolume` on CSI driver sockets itself.

Potential advantage:

- dynamic mount without an operation Pod.

Reasons to reject:

- duplicates kubelet's ownership of CSI publication;
- requires CSI-driver-specific volume IDs and secrets, not only PVC names;
- must reproduce target-path, staging, SELinux, fsGroup, mount-option, retry, and unpublish logic;
- can race kubelet and leave leaked mounts;
- creates a new compatibility surface with every CSI driver.

This should remain rejected unless Dynamo intentionally becomes a kubelet-integrated storage
component.

## 7. API representation options

### API option 1: cluster-scoped `PodSnapshotStorageConfig`

`PodSnapshot` references an administrator-owned configuration:

```yaml
spec:
  storageConfigName: shared-pod-pvc
```

The configuration selects exactly one backend and one access strategy. Separate objects represent
pod access and node-agent access.

Advantages:

- separates administrator infrastructure policy from namespaced snapshot requests;
- mirrors GKE, CSI class, and Velero storage-location patterns;
- avoids repeating storage policy in every snapshot;
- prevents ordinary snapshot users from supplying privileged host paths;
- permits status conditions for readiness and agent compatibility.

Challenges:

- a cluster-scoped object cannot directly own a namespaced PVC;
- pod access should describe a volume contract and resolve the real claim from the Pod;
- node-agent access must describe a pre-mounted binding or worker strategy, not promise an
  impossible live mount;
- RBAC must control who can create/update storage configs and which namespaces may reference them.

### API option 2: inline storage in `PodSnapshot.spec`

Example:

```yaml
spec:
  storage:
    pvc:
      pod:
        volumeName: checkpoint-storage
        mountPath: /checkpoints
```

Advantages:

- smallest object count;
- clear per-snapshot selection;
- natural access to the snapshot namespace.

Disadvantages:

- user-facing object exposes infrastructure and path details;
- storage configuration is repeated;
- node-agent mounts cannot be safely user-selected;
- future credentials/backends expand the namespaced API;
- immutable snapshots preserve stale infrastructure configuration;
- weaker separation between request and privileged placement.

### API option 3: resolved storage only in `PodSnapshotContent.spec`

The operator continues to take policy from Helm/operator configuration, but copies a resolved,
immutable binding into the content work order.

Advantages:

- smallest behavioral migration;
- agent no longer reads storage placement from Pod annotations;
- content becomes self-describing;
- no new CRD or user-facing choice;
- suitable if there will remain one storage policy per operator/agent installation.

Disadvantages:

- no declarative Kubernetes object for storage readiness;
- per-namespace/per-tenant routing remains configuration-driven;
- operator and agent configuration must still agree out of band;
- changing deployment configuration is the only way to add a storage target.

### API option 4: handle only in `PodSnapshotContent.status`

Keep all input configuration outside snapshot CRDs and add only an agent-produced
`status.snapshotHandle`.

Advantages:

- records the result needed by restore;
- follows CSI's driver-produced handle pattern;
- avoids exposing storage policy.

Disadvantages:

- the work order still does not tell the agent which store or access strategy to use;
- retries before the handle is written remain dependent on mutable external configuration;
- cannot validate an agent/config mismatch before starting capture.

This is useful but insufficient by itself.

### API option 5: no CRD storage representation

Keep the existing operator config, Pod annotations, and agent ConfigMap.

Advantages:

- no API migration;
- current behavior remains operational.

Disadvantages:

- preserves multiple mutable sources of truth;
- snapshot objects remain unable to identify their artifacts;
- storage changes can break restore;
- privileged placement remains influenced by Pod metadata;
- per-tenant routing and future backends remain difficult.

## 8. Non-final schema sketches

These sketches exist to make tradeoffs concrete. They are not an approved API.

### 8.1 Reusable storage configuration

Pod access:

```yaml
apiVersion: nvidia.com/v1alpha1
kind: PodSnapshotStorageConfig
metadata:
  name: workload-pvc
spec:
  pvc:
    pod:
      volumeName: checkpoint-storage
      mountPath: /checkpoints
status:
  conditions: []
```

Semantics:

- the object is cluster-scoped and administrator-owned;
- `volumeName` identifies a volume in the source and restore Pod specs;
- the operator resolves that volume to a PVC in the PodSnapshot namespace;
- `mountPath` is the expected path in the target container;
- the controller pins the actual claim identity into `PodSnapshotContent`;
- the storage config does not create the claim.

Named node-agent binding:

```yaml
apiVersion: nvidia.com/v1alpha1
kind: PodSnapshotStorageConfig
metadata:
  name: shared-agent-store
spec:
  pvc:
    nodeAgent:
      bindingName: primary
status:
  conditions: []
```

Semantics:

- `bindingName` is a logical binding installed in the agent DaemonSet configuration;
- each applicable agent must report or validate the same binding;
- the CR alone does not add a volume to the DaemonSet;
- existing contents pin the resolved binding and handle so later config changes do not redirect
  them.

Operation-Pod variant:

```yaml
apiVersion: nvidia.com/v1alpha1
kind: PodSnapshotStorageConfig
metadata:
  name: dynamic-operation-pvc
spec:
  pvc:
    operationPod:
      volumeName: checkpoint-storage
      mountPath: /checkpoints
status:
  conditions: []
```

This can use the same source-Pod volume-resolution contract while changing the execution strategy.

The CRD should enforce exactly one of `pod`, `nodeAgent`, or `operationPod` with structural fields
and CEL validation. It should not combine an enum with unrelated optional fields that permit invalid
shapes.

### 8.2 PodSnapshot reference

```yaml
spec:
  source:
    podRef:
      name: worker-0
      uid: ...
  storageConfigName: workload-pvc
```

Open questions:

- whether a default storage config is allowed;
- whether namespace policy restricts which configs a namespace may reference;
- whether the reference pins config UID/generation at creation;
- whether changing `storageConfigName` is forbidden by the existing immutable spec rule.

### 8.3 Resolved content binding

For pod access:

```yaml
spec:
  storage:
    configRef:
      name: workload-pvc
      uid: ...
    pvc:
      claimRef:
        namespace: inference
        name: snapshot-pvc
        uid: ...
      pod:
        volumeName: checkpoint-storage
        mountPath: /checkpoints
      artifactKey: podsnapshotcontent-<content-uid>/versions/1
```

For a pre-mounted agent binding:

```yaml
spec:
  storage:
    configRef:
      name: shared-agent-store
      uid: ...
    pvc:
      claimRef:
        namespace: snapshot-system
        name: snapshot-pvc
        uid: ...
      nodeAgent:
        bindingName: primary
        mountPath: /stores/primary
      artifactKey: podsnapshotcontent-<content-uid>/versions/1
```

Properties:

- `PodSnapshotContent.spec` remains immutable;
- paths are operator-derived and agent-validated;
- `artifactKey` is relative and system-owned;
- claim UID prevents a same-named replacement from silently becoming the restore source;
- content carries enough information to reject an incompatible agent before capture;
- the agent, not the user, writes the final handle.

### 8.4 Result handle

```yaml
status:
  snapshotHandle: pvc://inference/snapshot-pvc/podsnapshotcontent-<uid>/versions/1
  conditions:
    - type: Ready
      status: "True"
```

The final format must:

- include claim namespace and name or another stable claim identity;
- include a system-owned artifact key;
- avoid embedding a host `/proc`, kubelet, or agent-container path;
- be parseable without consulting Pod annotations;
- be paired with claim UID or equivalent provenance in structured status/spec;
- remain resolvable after storage configuration changes;
- define escaping and canonicalization rather than relying on string concatenation.

Whether the handle is opaque or formally versioned remains open.

## 9. Decision matrix

| Architecture | Dynamic per namespace | RWO | RWOP | RWX | Extra privileged Pod | Depends on live workload mount | Depends on kubelet layout | Agent rollout for new store |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Current `podMount` via `/proc` | Yes | Yes | Yes | Yes | No | Yes | No | No |
| Static `agentMount` | Limited | Poor across nodes | No | Yes | No | Restore target only | No | Yes |
| Named agent binding | Limited | Poor across nodes | No | Yes | No | Restore target only | No | Yes |
| Direct PVC + DS rollout | Yes, with controller | Poor across nodes | No | Yes | No | Restore target only | No | Yes, automated |
| Operation Pod | Yes | Yes, same node | No | Yes | Yes | Source/restore coordination | No | No |
| Kubelet host traversal | Yes | Yes | Yes | Yes | No | Yes | Yes | No |
| Injected sidecar | Yes | Yes | Usually no | Yes | Persistent helper | Yes | No | No |
| Direct CSI calls | Theoretical | Driver-specific | Driver-specific | Driver-specific | No | No | No | No |

Notes:

- “RWO: Yes” assumes all simultaneous consumers are scheduled on the same node.
- `ReadWriteOncePod` excludes any design with a second Pod mounting the claim.
- Static agent designs may use per-node claims, but an artifact then needs replication or
  node-pinned restore; that is a distinct backend architecture.

## 10. Failure-mode checklist

Any selected design must define behavior for:

- source Pod deleted before capture;
- source Pod recreated with the same name and different UID;
- container PID changes between resolution and filesystem access;
- referenced PVC missing, pending, terminating, or recreated;
- volume not mounted at the expected path;
- mount uses `subPath`, is read-only, or refers to a non-PVC volume;
- selected node lacks the required agent binding;
- RWO volume is attached to another node;
- operation Pod cannot schedule on the source/restore node;
- node fails during temporary-directory creation or atomic rename;
- checkpoint succeeds but status update fails;
- storage config changes or is deleted after capture;
- content is deleted while an operation is active;
- artifact directory exists from an earlier content lifetime;
- restore occurs on a different node or Kubernetes namespace;
- cleanup runs after the claim or storage binding disappears;
- agent version does not understand the stored handle version;
- two snapshots resolve to the same artifact key;
- a malicious or malformed path attempts traversal outside the storage root.

## 11. Security requirements

The implementation should preserve these invariants regardless of API choice:

1. Ordinary snapshot users never provide a host path.
2. Artifact placement is derived from system-owned UID or equivalent collision-resistant identity.
3. Every joined path is canonicalized and checked to remain below the resolved storage root.
4. Pod access is tied to a verified Pod UID, container, volume, mount, PVC name, and PVC UID.
5. Node-agent access selects only administrator-installed bindings.
6. A namespaced `PodSnapshot` cannot use a storage config to access an arbitrary infrastructure PVC.
7. `PodSnapshotContent.spec` is immutable after resolution.
8. The agent writes only status and the artifact location assigned by the resolved work order.
9. Restore uses the content's resolved handle, not mutable Pod annotations or current defaults.
10. RBAC for storage configuration and agent bindings is separate from permission to request a
    snapshot.
11. Cleanup verifies containment before recursive deletion.
12. Logs and status do not expose credentials or kubelet-internal paths.

## 12. Migration implications

A storage CRD cannot be introduced as an isolated field addition. A safe migration must address:

- dual-read precedence between CRD fields, Pod annotations, operator config, and agent config;
- whether old snapshots without handles remain restorable;
- cutover from checkpoint-ID paths to content-UID paths;
- how restore resolves old annotation-composed artifacts;
- whether storage configs are created automatically from current Helm values;
- how namespace-local legacy agent installations map to new configs;
- how central `podMount` installations map to new configs;
- status-handle population for captures completed before the upgrade;
- cleanup ownership for both path schemes;
- CRD conversion is unaffected because PodSnapshot types currently exist only in `v1alpha1`, but
  generated deepcopy, CRD bases, Helm CRD copies, RBAC, and API tests still require updates.

A likely compatibility strategy is:

1. introduce configuration and resolved-binding fields;
2. write the resolved content handle for new captures;
3. restore new captures from the handle;
4. retain a bounded legacy read path for old snapshots;
5. remove Pod storage annotations only after no supported snapshot depends on them.

This sequence is not approved; it is listed to expose the migration dependency.

## 13. Experiments required before a decision

### Experiment 1: harden and benchmark current `podMount`

- Verify capture and restore on containerd and CRI-O.
- Exercise PID replacement between resolution and access.
- Test a PVC mounted through `subPath`.
- Test RWO and RWOP claims.
- Validate behavior under SELinux/OpenShift.
- Measure save/restore overhead versus direct agent mount.
- Prove a controller can resolve and pin the actual PVC from `volumeName`.

### Experiment 2: kubelet host-volume traversal portability

- Locate the same PVC mount on GKE, EKS, AKS, OpenShift, and vCluster.
- Test CSI file and block-backed filesystem drivers.
- Compare paths with configurable kubelet roots.
- Verify mount propagation and SELinux access.
- Compare reliability with `/proc/<pid>/root`.

### Experiment 3: operation-Pod prototype

- Create a privileged Pod constrained to the source node with the same RWO PVC.
- Let the scheduler process both node affinity and PVC topology.
- Prove CRIU, CUDA checkpoint, overlay capture, and restore work from the worker.
- Measure startup overhead.
- Kill the worker at each phase and verify status/temporary-directory recovery.
- Test RWOP rejection explicitly.

### Experiment 4: static binding validation

- Configure two logical agent bindings.
- Verify every applicable agent reports the same binding.
- Remove or roll a binding during an active capture.
- Confirm an existing handle remains resolvable after config changes.
- Test one RWX store and per-node RWO stores.

### Experiment 5: handle and cleanup model

- Define canonical URI parsing and escaping.
- Prove claim recreation is detected by UID.
- Prove content recreation cannot overwrite an old artifact.
- Test orphan discovery and safe containment checks.
- Exercise capture-success/status-failure recovery.

## 14. Open decisions

The following decisions remain deliberately unresolved:

1. Which data-access architecture is the default?
2. Is host `/proc` traversal acceptable as the long-term pod strategy?
3. Is kubelet path traversal sufficiently portable for supported distributions?
4. Is an operation Pod's complexity justified by dynamic PVC access?
5. Does node-agent access need multiple named bindings or only one default store?
6. Should storage configuration be cluster-scoped, or should PVC policy be namespaced?
7. How are namespaces authorized to reference a cluster-scoped storage config?
8. Does the storage config merely describe an existing Pod volume, or can it provision claims?
9. Must restore remain in the capture namespace?
10. Are RWO and RWOP explicit support requirements?
11. Is `PodSnapshotContent.status.snapshotHandle` opaque or versioned and parseable?
12. Does the artifact path change from checkpoint ID to content UID?
13. Which controller owns deletion, retention, and orphan cleanup?
14. How long must legacy annotation-based snapshots remain restorable?
15. How does an agent advertise binding readiness and supported access strategies?

## 15. Evaluation guidance

The options should be evaluated in this order:

1. Define required storage topologies: namespace-local RWO, shared RWX, RWOP, multi-tenant, and
   cross-node restore.
2. Run the experiments for `podMount`, kubelet traversal, and operation Pods.
3. Decide the data plane before finalizing the CRD.
4. Select whether storage policy is reusable or deployment-global.
5. Define the immutable resolved binding and handle.
6. Design migration and cleanup together with restore.

Choosing a CRD schema before the data plane is proven risks encoding an access promise that the
DaemonSet or CSI layer cannot fulfill.
