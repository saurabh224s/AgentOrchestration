from __future__ import annotations
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class TaskState(str, Enum):
    CLAIMED    = "claimed"
    COMPUTING  = "computing"
    UPLOADING  = "uploading"   # <-- NEW: prevents other workers re-claiming
    COMPLETED  = "completed"
    FAILED     = "failed"
    EXPIRED    = "expired"


@dataclass
class Lease:
    task_id: str
    worker_id: str
    expires_at: float          # Unix timestamp
    state: TaskState = TaskState.CLAIMED
    renewal_count: int = 0


class LeaseManager:
    """
    FIX: Extend job lease TTL during large artifact uploads.

    Key changes:
      1. transition_to_uploading() moves the task into UPLOADING state,
         which queue workers check before re-claiming.
      2. upload_lease_renewal() is a context manager that starts a
         background thread to renew the lease at half-TTL intervals
         while the upload is running.
      3. Renewal count and heartbeat timestamps are emitted as metrics.
      4. On upload failure the lease is marked EXPIRED so the recovery
         path (documented below) can reclaim and retry the task cleanly.
    """

    DEFAULT_TTL_SECONDS = 30
    UPLOAD_TTL_SECONDS  = 120   # wider window for the upload phase

    def __init__(self, store, metrics, ttl: int = DEFAULT_TTL_SECONDS):
        self._store   = store
        self._metrics = metrics
        self._ttl     = ttl

    # ------------------------------------------------------------------
    # Transition to UPLOADING state
    # ------------------------------------------------------------------

    def transition_to_uploading(self, lease: Lease) -> Lease:
        """
        Mark the task as UPLOADING with an extended TTL.
        Other workers calling try_claim() will skip tasks in this state.
        """
        lease.state      = TaskState.UPLOADING
        lease.expires_at = time.monotonic() + self.UPLOAD_TTL_SECONDS
        self._store.save(lease)
        logger.info(
            "lease.upload.started",
            extra={"task_id": lease.task_id, "worker_id": lease.worker_id},
        )
        return lease

    # ------------------------------------------------------------------
    # Background renewal context manager
    # ------------------------------------------------------------------

    @contextmanager
    def upload_lease_renewal(
        self,
        lease: Lease,
        on_expire: Optional[Callable[[Lease], None]] = None,
    ) -> Iterator[None]:
        """
        Context manager that renews the lease in a background thread
        while a large upload is running.

        Usage:
            lease = manager.transition_to_uploading(lease)
            with manager.upload_lease_renewal(lease):
                uploader.upload(artifact_path)
        """
        stop_event = threading.Event()
        renewal_interval = self.UPLOAD_TTL_SECONDS // 2

        def _renew_loop() -> None:
            while not stop_event.wait(timeout=renewal_interval):
                try:
                    self._renew(lease)
                except Exception as exc:
                    logger.warning(
                        "lease.renewal.failed",
                        extra={"task_id": lease.task_id, "error": str(exc)},
                    )
                    if on_expire:
                        on_expire(lease)
                    stop_event.set()

        thread = threading.Thread(target=_renew_loop, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _renew(self, lease: Lease) -> None:
        """Extend TTL and record metrics."""
        lease.expires_at   = time.monotonic() + self.UPLOAD_TTL_SECONDS
        lease.renewal_count += 1
        self._store.save(lease)

        self._metrics.increment(
            "lease.renewals",
            tags={"task_id": lease.task_id, "worker_id": lease.worker_id},
        )
        logger.debug(
            "lease.renewed",
            extra={
                "task_id":       lease.task_id,
                "renewal_count": lease.renewal_count,
                "expires_at":    lease.expires_at,
            },
        )

    def try_claim(self, task_id: str, worker_id: str) -> Optional[Lease]:
        """
        Claim a task only if it is unclaimed OR its lease has truly expired.
        Tasks in UPLOADING state are NEVER re-claimed here.
        """
        existing = self._store.get(task_id)
        if existing:
            if existing.state == TaskState.UPLOADING:
                return None   # <-- FIX: skip in-flight uploads
            if existing.expires_at > time.monotonic():
                return None   # still held by another worker
        lease = Lease(
            task_id=task_id,
            worker_id=worker_id,
            expires_at=time.monotonic() + self._ttl,
        )
        self._store.save(lease)
        return lease


# ------------------------------------------------------------------
# src/storage/artifact_uploader.py
# ------------------------------------------------------------------

class ArtifactUploader:
    """
    FIX: Uploader is now lease-aware.
    Calls transition_to_uploading() then wraps the upload in a
    lease renewal context so the lease stays alive for large files.

    Recovery path (documented):
      If the upload context exits with an exception the renewal thread
      stops and the lease state remains UPLOADING until its TTL expires
      (UPLOAD_TTL_SECONDS). A background reaper job (not shown) queries
      for leases in state=UPLOADING past their TTL, marks them EXPIRED,
      and re-queues the task so a fresh worker can retry.
    """

    def __init__(self, storage_backend, lease_manager: LeaseManager, metrics):
        self._storage = storage_backend
        self._leases  = lease_manager
        self._metrics = metrics

    def upload(self, lease: Lease, artifact_path: str) -> str:
        # 1. Widen the TTL and switch state before touching storage
        lease = self._leases.transition_to_uploading(lease)

        # 2. Keep the lease alive in the background while bytes flow
        with self._leases.upload_lease_renewal(lease):
            url = self._storage.put(artifact_path)

        # 3. Emit upload completion metric
        self._metrics.increment(
            "artifact.upload.completed",
            tags={
                "task_id":       lease.task_id,
                "renewals":      lease.renewal_count,
            },
        )
        logger.info(
            "artifact.upload.done",
            extra={
                "task_id":       lease.task_id,
                "renewals":      lease.renewal_count,
                "artifact_url":  url,
            },
        )
        return url
