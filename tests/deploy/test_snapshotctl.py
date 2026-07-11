# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live-cluster snapshotctl checkpoint and restore e2e tests.

Verifies `snapshotctl checkpoint` and `snapshotctl restore` end-to-end using
the PodSnapshot-based capture flow introduced in PR #10951. Does NOT use
DynamoGraphDeployment — this is the manual, standalone checkpoint path.

Pre-requisites on the cluster:
  - snapshot-agent DaemonSet running in the target namespace (agentMount mode)
  - snapshot-pvc (RWX, ≥50Gi) — or whatever --checkpoint-pvc names
  - ngc-secret for pulling the workload image
  - dynamo-operator (PodSnapshotReconciler) installed
  - snapshotctl-runner Role has pods: [get, list, watch, create]
    (apply once: kubectl patch role snapshotctl-runner -n default --type=json
     -p='[{"op":"replace","path":"/rules/1/verbs",
           "value":["get","list","watch","create"]}]')
"""

import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import pytest

logger = logging.getLogger(__name__)

TEST_TIMEOUT = 900   # 15 min — checkpoint-only test
E2E_TIMEOUT = 1800   # 30 min — combined checkpoint + restore

# Keys emitted by snapshotctl on stdout; parser is scoped to these to avoid
# picking up log lines that happen to contain '='.
_KNOWN_OUTPUT_KEYS = frozenset({
    "status", "namespace", "name",
    "checkpoint_job", "checkpoint_id", "checkpoint_location",
    "pod_snapshot", "bound_content",
    "restore_pod",
})

# ---------------------------------------------------------------------------
# Manifest template — substituted at test time
# ---------------------------------------------------------------------------
_WORKLOAD_MANIFEST = """\
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
  annotations:
    nvidia.com/snapshot-target-containers: "main"
spec:
  tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
  imagePullSecrets:
  - name: ngc-secret
  containers:
  - name: main
    image: {image}
    command: ["sh", "-c", "mkdir -p /snapshot-control && touch /snapshot-control/ready-for-snapshot && sleep 3600"]
    env:
    - name: PATH
      value: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    resources:
      requests:
        nvidia.com/gpu: "1"
      limits:
        nvidia.com/gpu: "1"
    volumeMounts:
    - name: checkpoints
      mountPath: /checkpoints
  volumes:
  - name: checkpoints
    persistentVolumeClaim:
      claimName: {pvc_name}
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.nightly
@pytest.mark.snapshotctl
@pytest.mark.deploy
@pytest.mark.k8s
@pytest.mark.e2e
@pytest.mark.gpu_1
@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.skipif(sys.platform != "linux", reason="snapshotctl is linux/amd64 only")
def test_snapshotctl_checkpoint(
    snapshotctl_binary: str,
    snapshot_agent_image: str,
    checkpoint_pvc: str,
    namespace: str,
) -> None:
    """snapshotctl checkpoint creates a PodSnapshot and the agent captures it."""
    checkpoint_id = f"snapshotctl-e2e-{int(time.time())}"
    pod_name = "snapshotctl-e2e-worker"
    binary = Path(snapshotctl_binary)
    if not binary.exists():
        pytest.fail(f"snapshotctl binary not found: {binary}")

    snap_name: Optional[str] = None
    job_name: Optional[str] = None
    manifest_path: Optional[str] = None

    try:
        manifest_path = _write_tempfile(
            _WORKLOAD_MANIFEST.format(
                pod_name=pod_name, namespace=namespace,
                image=snapshot_agent_image, pvc_name=checkpoint_pvc,
            )
        )
        _apply_manifest(manifest_path, namespace)
        _wait_pod_running(pod_name, namespace, timeout=120)
        _verify_cuda(pod_name, namespace)

        logger.info("Running snapshotctl checkpoint (checkpoint_id=%s)", checkpoint_id)
        proc = subprocess.run(
            [str(binary), "checkpoint", "--manifest", manifest_path,
             "--container", "main", "--namespace", namespace,
             "--checkpoint-id", checkpoint_id, "--timeout", "10m"],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            pytest.fail(
                f"snapshotctl exited {proc.returncode}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

        logger.info("snapshotctl stdout:\n%s", proc.stdout)
        output = _parse_output(proc.stdout)
        assert output.get("pod_snapshot"), f"pod_snapshot= missing from output:\n{proc.stdout}"
        assert output.get("checkpoint_id"), f"checkpoint_id= missing from output:\n{proc.stdout}"
        snap_name = output["pod_snapshot"]
        job_name = f"{pod_name}-checkpoint"
        logger.info("PodSnapshot: %s, checkpoint_id: %s", snap_name, output["checkpoint_id"])

        _assert_podsnapshot_ready(snap_name, namespace)
        _assert_artifact_present(checkpoint_id, checkpoint_pvc, namespace)

    finally:
        if manifest_path:
            try:
                os.unlink(manifest_path)
            except OSError:
                pass
        _cleanup(pod_name, snap_name, job_name, namespace)


@pytest.mark.nightly
@pytest.mark.snapshotctl
@pytest.mark.deploy
@pytest.mark.k8s
@pytest.mark.e2e
@pytest.mark.gpu_1
@pytest.mark.timeout(E2E_TIMEOUT)
@pytest.mark.skipif(sys.platform != "linux", reason="snapshotctl is linux/amd64 only")
def test_snapshotctl_e2e(
    snapshotctl_binary: str,
    snapshot_agent_image: str,
    checkpoint_pvc: str,
    namespace: str,
) -> None:
    """snapshotctl checkpoint then restore — full round-trip e2e."""
    ts = int(time.time())
    checkpoint_id = f"snapshotctl-e2e-{ts}"
    worker_pod = f"snapshotctl-e2e-worker-{ts}"
    restore_pod_name = f"snapshotctl-e2e-restore-{ts}"
    binary = Path(snapshotctl_binary)
    if not binary.exists():
        pytest.fail(f"snapshotctl binary not found: {binary}")

    snap_name: Optional[str] = None
    restore_pod: Optional[str] = None
    worker_manifest_path: Optional[str] = None
    restore_manifest_path: Optional[str] = None

    try:
        # -- checkpoint phase --
        worker_manifest_path = _write_tempfile(
            _WORKLOAD_MANIFEST.format(
                pod_name=worker_pod, namespace=namespace,
                image=snapshot_agent_image, pvc_name=checkpoint_pvc,
            )
        )
        _apply_manifest(worker_manifest_path, namespace)
        _wait_pod_running(worker_pod, namespace, timeout=120)
        _verify_cuda(worker_pod, namespace)

        logger.info("Running snapshotctl checkpoint (checkpoint_id=%s)", checkpoint_id)
        proc = subprocess.run(
            [str(binary), "checkpoint", "--manifest", worker_manifest_path,
             "--container", "main", "--namespace", namespace,
             "--checkpoint-id", checkpoint_id, "--timeout", "10m"],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            pytest.fail(
                f"snapshotctl checkpoint exited {proc.returncode}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

        logger.info("snapshotctl checkpoint stdout:\n%s", proc.stdout)
        output = _parse_output(proc.stdout)
        assert output.get("pod_snapshot"), f"pod_snapshot= missing:\n{proc.stdout}"
        assert output.get("checkpoint_id"), f"checkpoint_id= missing:\n{proc.stdout}"
        snap_name = output["pod_snapshot"]
        logger.info("PodSnapshot: %s, checkpoint_id: %s", snap_name, checkpoint_id)

        _assert_podsnapshot_ready(snap_name, namespace)
        _assert_artifact_present(checkpoint_id, checkpoint_pvc, namespace)

        # Free the GPU before restore — worker pod has served its purpose.
        _kubectl("delete", "pod", worker_pod, "--namespace", namespace,
                 "--ignore-not-found=true", "--wait=false", check=False)

        # -- restore phase --
        restore_manifest_path = _write_tempfile(
            _WORKLOAD_MANIFEST.format(
                pod_name=restore_pod_name, namespace=namespace,
                image=snapshot_agent_image, pvc_name=checkpoint_pvc,
            )
        )
        logger.info("Running snapshotctl restore (checkpoint_id=%s)", checkpoint_id)
        proc = subprocess.run(
            [str(binary), "restore", "--manifest", restore_manifest_path,
             "--containers", "main", "--namespace", namespace,
             "--checkpoint-id", checkpoint_id],
            capture_output=True, text=True,
            timeout=120,  # restore must return status=requested quickly
        )
        if proc.returncode != 0:
            pytest.fail(
                f"snapshotctl restore exited {proc.returncode}\n"
                f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

        logger.info("snapshotctl restore stdout:\n%s", proc.stdout)
        output = _parse_output(proc.stdout)
        assert output.get("restore_pod"), f"restore_pod= missing:\n{proc.stdout}"
        assert output.get("status") == "requested", (
            f"unexpected restore status: {output.get('status')}"
        )
        restore_pod = output["restore_pod"]
        logger.info("Restore pod: %s", restore_pod)

        _assert_restore_completed(restore_pod, namespace, timeout=300)
        _kubectl(
            "wait", f"pod/{restore_pod}", "--for=condition=Ready",
            "--timeout=60s", "--namespace", namespace, check=False,
        )

    finally:
        for path in [worker_manifest_path, restore_manifest_path]:
            if path:
                try:
                    os.unlink(path)
                except OSError:
                    pass
        _cleanup(worker_pod, snap_name, f"{worker_pod}-checkpoint", namespace)
        if restore_pod:
            _kubectl("delete", "pod", restore_pod, "--namespace", namespace,
                     "--ignore-not-found=true", "--wait=false", check=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl", *args],
        capture_output=True,
        text=True,
        check=check,
    )


def _write_tempfile(content: str) -> str:
    """Write content to a NamedTemporaryFile and return its path. Caller must os.unlink."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(content)
        return f.name


def _parse_output(stdout: str) -> dict:
    """Parse snapshotctl key=value output, scoped to known keys only."""
    result = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key in _KNOWN_OUTPUT_KEYS:
            result[key] = value
    return result


def _apply_manifest(path: str, namespace: str) -> None:
    proc = _kubectl("apply", "-f", path, "--namespace", namespace, check=False)
    if proc.returncode != 0:
        pytest.fail(f"kubectl apply failed:\n{proc.stderr}")


def _wait_pod_running(pod_name: str, namespace: str, timeout: int = 120) -> None:
    proc = _kubectl(
        "wait", f"pod/{pod_name}", "--for=condition=Ready",
        f"--timeout={timeout}s", "--namespace", namespace, check=False,
    )
    if proc.returncode != 0:
        desc = _kubectl("describe", "pod", pod_name, "--namespace", namespace, check=False)
        pytest.fail(
            f"Pod {pod_name} did not become Ready within {timeout}s:\n"
            f"{desc.stdout[-3000:]}"
        )


def _verify_cuda(pod_name: str, namespace: str) -> None:
    proc = _kubectl("exec", pod_name, "--namespace", namespace, "--", "nvidia-smi", check=False)
    if proc.returncode != 0:
        pytest.skip(
            f"CUDA not available in workload pod — skipping snapshotctl e2e "
            f"(nvidia-smi exited {proc.returncode}):\n{proc.stderr}"
        )


def _assert_podsnapshot_ready(snap_name: str, namespace: str) -> None:
    """Poll PodSnapshot for Ready; fail fast on Failed."""
    deadline = time.time() + 30
    while time.time() < deadline:
        proc = _kubectl(
            "get", f"podsnapshot/{snap_name}", "--namespace", namespace,
            "-o", "jsonpath={.status.conditions}", check=False,
        )
        if proc.returncode != 0:
            time.sleep(2)
            continue
        conditions_raw = proc.stdout
        if '"type":"Failed","status":"True"' in conditions_raw:
            pytest.fail(f"PodSnapshot {snap_name} is in Failed state:\n{conditions_raw}")
        if '"type":"Ready","status":"True"' in conditions_raw:
            logger.info("PodSnapshot %s is Ready", snap_name)
            bound = _kubectl(
                "get", f"podsnapshot/{snap_name}", "--namespace", namespace,
                "-o", "jsonpath={.status.boundPodSnapshotContentName}", check=False,
            )
            assert bound.stdout.strip(), (
                f"PodSnapshot {snap_name} Ready but boundPodSnapshotContentName is empty"
            )
            return
        time.sleep(2)
    pytest.fail(f"PodSnapshot {snap_name} did not reach Ready within 30s after snapshotctl returned")


def _assert_artifact_present(checkpoint_id: str, pvc_name: str, namespace: str) -> None:
    """Spin up a temporary pod to verify the checkpoint artifact exists in the PVC."""
    verifier_name = "snapshotctl-e2e-verifier"
    manifest = f"""\
apiVersion: v1
kind: Pod
metadata:
  name: {verifier_name}
  namespace: {namespace}
spec:
  tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
  restartPolicy: Never
  containers:
  - name: verify
    image: busybox:1.36
    command: ["ls", "/checkpoints/{checkpoint_id}/versions/1/"]
    volumeMounts:
    - name: checkpoints
      mountPath: /checkpoints
  volumes:
  - name: checkpoints
    persistentVolumeClaim:
      claimName: {pvc_name}
"""
    path = _write_tempfile(manifest)
    try:
        _apply_manifest(path, namespace)
        deadline = time.time() + 120
        while time.time() < deadline:
            proc = _kubectl(
                "get", f"pod/{verifier_name}", "--namespace", namespace,
                "-o", "jsonpath={.status.phase}", check=False,
            )
            phase = proc.stdout.strip()
            if phase == "Succeeded":
                logger.info("Artifact present at /checkpoints/%s/versions/1/", checkpoint_id)
                return
            if phase == "Failed":
                logs = _kubectl("logs", verifier_name, "--namespace", namespace, check=False)
                pytest.fail(
                    f"Checkpoint artifact missing at /checkpoints/{checkpoint_id}/versions/1/ "
                    f"in PVC {pvc_name}:\n{logs.stdout}\n{logs.stderr}"
                )
            time.sleep(3)
        pytest.fail(
            f"Artifact verification pod did not complete within 120s "
            f"(checkpoint_id={checkpoint_id})"
        )
    finally:
        os.unlink(path)
        _kubectl("delete", "pod", verifier_name, "--namespace", namespace,
                 "--ignore-not-found=true", check=False)


def _assert_restore_completed(pod_name: str, namespace: str, timeout: int = 300) -> None:
    """Poll the restore-status annotation until completed or failed."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        proc = _kubectl(
            "get", f"pod/{pod_name}", "--namespace", namespace,
            "-o", r"jsonpath={.metadata.annotations.nvidia\.com/snapshot-restore-status\.main}",
            check=False,
        )
        status = proc.stdout.strip()
        if status == "completed":
            logger.info("Restore completed for pod %s", pod_name)
            return
        if status == "failed":
            pytest.fail(f"Restore failed for pod {pod_name}")
        if proc.returncode != 0:
            time.sleep(5)
            continue
        logger.debug("Restore status for %s: %r — waiting", pod_name, status)
        time.sleep(5)
    pytest.fail(
        f"Restore did not complete within {timeout}s for pod {pod_name} "
        f"(last annotation value: {status!r})"
    )


def _cleanup(
    pod_name: str,
    snap_name: Optional[str],
    job_name: Optional[str],
    namespace: str,
) -> None:
    """Best-effort cleanup of all resources created by the test."""
    # Fetch and delete PodSnapshotContent before removing the PodSnapshot.
    if snap_name:
        content_proc = _kubectl(
            "get", f"podsnapshot/{snap_name}", "--namespace", namespace,
            "-o", "jsonpath={.status.boundPodSnapshotContentName}", check=False,
        )
        content_name = content_proc.stdout.strip()
        if content_name:
            _kubectl("delete", "podsnapshotcontent", content_name,
                     "--ignore-not-found=true", "--wait=false", check=False)

    for resource, name in [
        ("pod", pod_name),
        ("podsnapshot", snap_name),
        ("job", job_name),
    ]:
        if name:
            _kubectl("delete", resource, name, "--namespace", namespace,
                     "--ignore-not-found=true", "--wait=false", check=False)
