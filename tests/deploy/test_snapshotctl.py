# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live-cluster snapshotctl checkpoint e2e test.

Verifies that `snapshotctl checkpoint` works end-to-end using the PodSnapshot-based
capture flow introduced in PR #10951. Does NOT use DynamoGraphDeployment — this is the
manual, standalone checkpoint path.

Pre-requisites on the cluster:
  - snapshot-agent DaemonSet running in the target namespace (agentMount mode)
  - snapshot-pvc (RWX, ≥50Gi) — or whatever --checkpoint-pvc names
  - ngc-secret for pulling the workload image
  - dynamo-operator (PodSnapshotReconciler) installed
"""

import logging
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import pytest

logger = logging.getLogger(__name__)

TEST_TIMEOUT = 900  # 15 min outer guard

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
# Test
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
    pod_name = f"snapshotctl-e2e-worker"
    binary = Path(snapshotctl_binary)
    if not binary.exists():
        pytest.fail(f"snapshotctl binary not found: {binary}")

    manifest_content = _WORKLOAD_MANIFEST.format(
        pod_name=pod_name,
        namespace=namespace,
        image=snapshot_agent_image,
        pvc_name=checkpoint_pvc,
    )

    snap_name: Optional[str] = None
    job_name: Optional[str] = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as manifest_file:
            manifest_file.write(manifest_content)
            manifest_path = manifest_file.name

        # Pre-flight: verify CUDA is accessible on the node where the workload will run.
        # We do this by scheduling the pod first and exec-ing nvidia-smi.
        _apply_manifest(manifest_path, namespace)
        _wait_pod_running(pod_name, namespace, timeout=120)
        _verify_cuda(pod_name, namespace)

        # Run snapshotctl checkpoint.
        logger.info(
            "Running snapshotctl checkpoint (checkpoint_id=%s)", checkpoint_id
        )
        proc = subprocess.run(
            [
                str(binary),
                "checkpoint",
                "--manifest",
                manifest_path,
                "--container",
                "main",
                "--namespace",
                namespace,
                "--checkpoint-id",
                checkpoint_id,
                "--timeout",
                "10m",
            ],
            capture_output=True,
            text=True,
        )

        if proc.returncode != 0:
            pytest.fail(
                f"snapshotctl exited {proc.returncode}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )

        logger.info("snapshotctl stdout:\n%s", proc.stdout)

        # Parse output.
        output = {
            line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in proc.stdout.splitlines()
            if "=" in line
        }
        assert "pod_snapshot" in output, (
            f"pod_snapshot= missing from output:\n{proc.stdout}"
        )
        assert "checkpoint_id" in output, (
            f"checkpoint_id= missing from output:\n{proc.stdout}"
        )
        snap_name = output["pod_snapshot"]
        job_name = f"{pod_name}-checkpoint"
        logger.info("PodSnapshot: %s, checkpoint_id: %s", snap_name, output["checkpoint_id"])

        # Verify PodSnapshot status via kubectl.
        _assert_podsnapshot_ready(snap_name, namespace)

        # Verify artifact is present in the PVC.
        _assert_artifact_present(checkpoint_id, checkpoint_pvc, namespace)

    finally:
        _cleanup(pod_name, snap_name, job_name, namespace)


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


def _apply_manifest(path: str, namespace: str) -> None:
    proc = _kubectl("apply", "-f", path, "--namespace", namespace, check=False)
    if proc.returncode != 0:
        pytest.fail(f"kubectl apply failed:\n{proc.stderr}")


def _wait_pod_running(pod_name: str, namespace: str, timeout: int = 120) -> None:
    proc = _kubectl(
        "wait",
        f"pod/{pod_name}",
        "--for=condition=Ready",
        f"--timeout={timeout}s",
        "--namespace",
        namespace,
        check=False,
    )
    if proc.returncode != 0:
        # Grab describe for diagnostics.
        desc = _kubectl(
            "describe", "pod", pod_name, "--namespace", namespace, check=False
        )
        pytest.fail(
            f"Pod {pod_name} did not become Ready within {timeout}s:\n"
            f"{desc.stdout[-3000:]}"
        )


def _verify_cuda(pod_name: str, namespace: str) -> None:
    proc = _kubectl(
        "exec",
        pod_name,
        "--namespace",
        namespace,
        "--",
        "nvidia-smi",
        check=False,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"CUDA not available in workload pod on this node — skipping snapshotctl e2e "
            f"(nvidia-smi exited {proc.returncode}):\n{proc.stderr}"
        )


def _assert_podsnapshot_ready(snap_name: str, namespace: str) -> None:
    """Poll PodSnapshot for Ready; fail fast on Failed."""
    deadline = time.time() + 30
    while time.time() < deadline:
        proc = _kubectl(
            "get",
            f"podsnapshot/{snap_name}",
            "--namespace",
            namespace,
            "-o",
            "jsonpath={.status.conditions}",
            check=False,
        )
        if proc.returncode != 0:
            time.sleep(2)
            continue
        conditions_raw = proc.stdout
        if '"type":"Failed","status":"True"' in conditions_raw:
            pytest.fail(
                f"PodSnapshot {snap_name} is in Failed state:\n{conditions_raw}"
            )
        if '"type":"Ready","status":"True"' in conditions_raw:
            logger.info("PodSnapshot %s is Ready", snap_name)
            # Also check boundPodSnapshotContentName.
            bound = _kubectl(
                "get",
                f"podsnapshot/{snap_name}",
                "--namespace",
                namespace,
                "-o",
                "jsonpath={.status.boundPodSnapshotContentName}",
                check=False,
            )
            assert bound.stdout.strip(), (
                f"PodSnapshot {snap_name} Ready but boundPodSnapshotContentName is empty"
            )
            return
        time.sleep(2)
    pytest.fail(
        f"PodSnapshot {snap_name} did not reach Ready within 30s after snapshotctl returned"
    )


def _assert_artifact_present(
    checkpoint_id: str, pvc_name: str, namespace: str
) -> None:
    """Spin up a temporary pod to verify the checkpoint artifact exists in the PVC."""
    verifier_name = f"snapshotctl-e2e-verifier"
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
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(manifest)
        path = f.name

    try:
        _apply_manifest(path, namespace)
        # Wait for Completed or Failed.
        deadline = time.time() + 120
        while time.time() < deadline:
            proc = _kubectl(
                "get",
                f"pod/{verifier_name}",
                "--namespace",
                namespace,
                "-o",
                "jsonpath={.status.phase}",
                check=False,
            )
            phase = proc.stdout.strip()
            if phase == "Succeeded":
                logger.info("Artifact present at /checkpoints/%s/versions/1/", checkpoint_id)
                return
            if phase == "Failed":
                logs = _kubectl(
                    "logs",
                    verifier_name,
                    "--namespace",
                    namespace,
                    check=False,
                )
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
        _kubectl(
            "delete",
            "pod",
            verifier_name,
            "--namespace",
            namespace,
            "--ignore-not-found=true",
            check=False,
        )


@pytest.mark.nightly
@pytest.mark.snapshotctl
@pytest.mark.deploy
@pytest.mark.k8s
@pytest.mark.e2e
@pytest.mark.gpu_1
@pytest.mark.timeout(TEST_TIMEOUT)
@pytest.mark.skipif(sys.platform != "linux", reason="snapshotctl is linux/amd64 only")
def test_snapshotctl_restore(
    snapshotctl_binary: str,
    snapshot_agent_image: str,
    checkpoint_pvc: str,
    snapshotctl_checkpoint_id: str,
    namespace: str,
) -> None:
    """snapshotctl restore submits a restore request and the agent completes it."""
    pod_name = "snapshotctl-e2e-restore"
    binary = Path(snapshotctl_binary)
    if not binary.exists():
        pytest.fail(f"snapshotctl binary not found: {binary}")

    manifest_content = _WORKLOAD_MANIFEST.format(
        pod_name=pod_name,
        namespace=namespace,
        image=snapshot_agent_image,
        pvc_name=checkpoint_pvc,
    )

    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as manifest_file:
            manifest_file.write(manifest_content)
            manifest_path = manifest_file.name

        proc = subprocess.run(
            [
                str(binary),
                "restore",
                "--manifest",
                manifest_path,
                "--containers",
                "main",
                "--namespace",
                namespace,
                "--checkpoint-id",
                snapshotctl_checkpoint_id,
            ],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            pytest.fail(
                f"snapshotctl restore exited {proc.returncode}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )

        logger.info("snapshotctl restore stdout:\n%s", proc.stdout)
        output = {
            line.split("=", 1)[0]: line.split("=", 1)[1]
            for line in proc.stdout.splitlines()
            if "=" in line
        }
        assert "restore_pod" in output, f"restore_pod= missing from output:\n{proc.stdout}"
        assert output.get("status") == "requested", (
            f"unexpected status: {output.get('status')}"
        )
        restore_pod = output["restore_pod"]
        logger.info("Restore pod: %s, checkpoint_id: %s", restore_pod, snapshotctl_checkpoint_id)

        _assert_restore_completed(restore_pod, namespace)

    finally:
        _kubectl(
            "delete",
            "pod",
            pod_name,
            "--namespace",
            namespace,
            "--ignore-not-found=true",
            "--wait=false",
            check=False,
        )


def _assert_restore_completed(pod_name: str, namespace: str, timeout: int = 600) -> None:
    """Poll the restore-status annotation until completed or failed."""
    annotation = f"nvidia.com/snapshot-restore-status.main"
    deadline = time.time() + timeout
    while time.time() < deadline:
        proc = _kubectl(
            "get",
            f"pod/{pod_name}",
            "--namespace",
            namespace,
            "-o",
            f"jsonpath={{.metadata.annotations.nvidia\\.com/snapshot-restore-status\\.main}}",
            check=False,
        )
        status = proc.stdout.strip()
        if status == "completed":
            logger.info("Restore completed for pod %s", pod_name)
            return
        if status == "failed":
            pytest.fail(f"Restore failed for pod {pod_name} (annotation: {annotation})")
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
    for resource, name in [
        ("pod", pod_name),
        ("podsnapshot", snap_name),
        ("job", job_name),
    ]:
        if name:
            _kubectl(
                "delete",
                resource,
                name,
                "--namespace",
                namespace,
                "--ignore-not-found=true",
                "--wait=false",
                check=False,
            )
