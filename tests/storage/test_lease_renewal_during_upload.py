"""
Regression tests for Issue #4184 —
Large artifact uploads must not trigger duplicate task execution.
"""
import threading
import time
import pytest
from unittest.mock import MagicMock, call, patch

from src.storage.lease_manager import (
    Lease,
    LeaseManager,
    TaskState,
)
from src.storage.artifact_uploader import ArtifactUploader


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def make_lease(task_id="task-001", worker_id="worker-A") -> Lease:
    return Lease(
        task_id=task_id,
        worker_id=worker_id,
        expires_at=time.monotonic() + 30,
    )


@pytest.fixture()
def store():
    _db = {}
    mock = MagicMock()
    mock.save.side_effect  = lambda l: _db.update({l.task_id: l})
    mock.get.side_effect   = lambda tid: _db.get(tid)
    return mock


@pytest.fixture()
def metrics():
    return MagicMock()


@pytest.fixture()
def manager(store, metrics):
    return LeaseManager(store=store, metrics=metrics, ttl=30)


@pytest.fixture()
def storage_backend():
    mock = MagicMock()
    mock.put.return_value = "https://storage/artifact.bin"
    return mock


@pytest.fixture()
def uploader(storage_backend, manager, metrics):
    return ArtifactUploader(
        storage_backend=storage_backend,
        lease_manager=manager,
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# Core regression tests
# ---------------------------------------------------------------------------

class TestLeaseRenewalDuringUpload:

    def test_transition_sets_uploading_state(self, manager):
        lease = make_lease()
        updated = manager.transition_to_uploading(lease)
        assert updated.state == TaskState.UPLOADING

    def test_transition_extends_ttl(self, manager):
        lease = make_lease()
        before = lease.expires_at
        updated = manager.transition_to_uploading(lease)
        assert updated.expires_at > before + 60  # at least 60s more

    def test_worker_cannot_reclaim_uploading_task(self, manager, store):
        """Core regression: another worker must not re-claim an UPLOADING task."""
        lease = make_lease(task_id="task-001", worker_id="worker-A")
        manager.transition_to_uploading(lease)

        # Worker B tries to claim the same task
        result = manager.try_claim("task-001", "worker-B")
        assert result is None, (
            "Worker B must NOT claim a task in UPLOADING state — "
            "this is the root cause of #4184"
        )

    def test_renewal_loop_fires_during_upload(self, manager, metrics):
        """Lease renewal must happen at least once during a slow upload."""
        lease = make_lease()
        lease = manager.transition_to_uploading(lease)

        # Simulate a 0.3s upload with a very short renewal interval
        manager.UPLOAD_TTL_SECONDS = 0.1

        with manager.upload_lease_renewal(lease):
            time.sleep(0.25)   # long enough for 2 renewals

        assert lease.renewal_count >= 1, (
            "Lease must be renewed at least once during upload"
        )

    def test_renewal_emits_metrics(self, manager, metrics):
        """Every renewal must emit a lease.renewals metric."""
        lease = make_lease()
        lease = manager.transition_to_uploading(lease)
        manager.UPLOAD_TTL_SECONDS = 0.1

        with manager.upload_lease_renewal(lease):
            time.sleep(0.25)

        assert metrics.increment.called
        calls_str = str(metrics.increment.call_args_list)
        assert "lease.renewals" in calls_str

    def test_no_duplicate_execution_under_slow_storage(
        self, manager, store, metrics
    ):
        """
        Simulate slow upload (Worker A) + eager reclaim attempt (Worker B).
        Worker B must get None from try_claim for the entire upload window.
        """
        lease_a = make_lease(task_id="task-slow", worker_id="worker-A")
        lease_a = manager.transition_to_uploading(lease_a)

        duplicate_claims = []

        def worker_b_tries():
            for _ in range(10):
                result = manager.try_claim("task-slow", "worker-B")
                if result is not None:
                    duplicate_claims.append(result)
                time.sleep(0.01)

        t = threading.Thread(target=worker_b_tries)
        t.start()

        # Simulate Worker A uploading
        manager.UPLOAD_TTL_SECONDS = 0.5
        with manager.upload_lease_renewal(lease_a):
            time.sleep(0.15)

        t.join()

        assert duplicate_claims == [], (
            f"Duplicate task claims detected during upload: {duplicate_claims}"
        )

    def test_uploader_emits_completion_metric(self, uploader, metrics):
        """ArtifactUploader must emit artifact.upload.completed on success."""
        lease = make_lease()
        uploader.upload(lease, "/tmp/artifact.bin")

        calls_str = str(metrics.increment.call_args_list)
        assert "artifact.upload.completed" in calls_str

    def test_expired_upload_recovery_path(self, manager, store):
        """
        If the upload context exits with an exception, the lease stays
        in UPLOADING state so the reaper can recover it — NOT silently lost.
        """
        lease = make_lease()
        lease = manager.transition_to_uploading(lease)

        with pytest.raises(RuntimeError):
            with manager.upload_lease_renewal(lease):
                raise RuntimeError("storage failure")

        # Lease should still be UPLOADING (reaper will handle it)
        saved = store.get("task-001")
        assert saved.state == TaskState.UPLOADING, (
            "Failed upload must leave lease in UPLOADING state for recovery"
      )
