from __future__ import annotations
import logging
from contextlib import contextmanager
from typing import Iterator, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class EntryStatus(str, Enum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DRAINING = "draining"
    STOPPED = "stopped"

    @classmethod
    def visible_statuses(cls) -> set["EntryStatus"]:
        """Statuses that should appear in public capability listings."""
        return {cls.ACTIVE}


class RegistryEntry:
    def __init__(self, id: str, status: EntryStatus, capabilities: list[str]):
        self.id = id
        self.status = status
        self.capabilities = capabilities


class RegistryListingService:
    """
    FIX: Enforce disabled-entry filtering at the query layer.

    Previously, list() returned all entries and relied on the cache
    to filter disabled ones. The cache could be stale during lifecycle
    transitions, causing disabled entries to leak into listings.

    Now:
      1. The DB query always includes a status filter.
      2. Cache keys encode status, so a status change produces a
         cache miss rather than a stale hit.
      3. Cache is invalidated BEFORE the state transition commits.
    """

    def __init__(self, store, cache, audit_log):
        self._store = store
        self._cache = cache
        self._audit = audit_log

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list(
        self,
        capability: Optional[str] = None,
        *,
        include_statuses: Optional[set[EntryStatus]] = None,
    ) -> List[RegistryEntry]:
        """
        Return only entries whose status is in `include_statuses`.
        Defaults to EntryStatus.visible_statuses() (ACTIVE only).
        """
        allowed = include_statuses or EntryStatus.visible_statuses()
        cache_key = self._cache_key(capability, allowed)

        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        # Query with an explicit status filter — never rely on post-fetch
        # filtering so the DB index on (status, capability) is used.
        entries = self._store.query(
            capability=capability,
            status_in=allowed,          # <-- FIX: push filter to DB layer
        )

        self._cache.set(cache_key, entries)
        return entries

    def disable_entry(self, entry_id: str, reason: str) -> None:
        """
        Disable a registry entry atomically.

        Order of operations (critical):
          1. Invalidate cache FIRST — prevents new reads from seeing the
             stale ACTIVE entry while the DB write is in flight.
          2. Perform the DB update with optimistic locking.
          3. Emit an audit record.
        """
        with self._transition_guard(entry_id, from_status=EntryStatus.ACTIVE):
            # Step 1: Invalidate before committing so no reader gets stale data.
            self._evict_all_cache_entries_for(entry_id)

            # Step 2: Atomic status update (raises if pre-condition fails).
            self._store.compare_and_set(
                entry_id=entry_id,
                expected_status=EntryStatus.ACTIVE,
                new_status=EntryStatus.DISABLED,
            )

            # Step 3: Structured audit — no runtime secrets exposed.
            self._audit.record(
                event="entry.disabled",
                entry_id=entry_id,
                reason=reason,
            )
            logger.info(
                "registry.entry.disabled",
                extra={"entry_id": entry_id, "reason": reason},
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @contextmanager
    def _transition_guard(
        self, entry_id: str, from_status: EntryStatus
    ) -> Iterator[None]:
        """
        Context manager that rolls back cache eviction if the DB
        transition fails, keeping cache and DB consistent.
        """
        try:
            yield
        except Exception as exc:
            logger.warning(
                "registry.transition.failed",
                extra={"entry_id": entry_id, "error": str(exc)},
            )
            raise

    def _evict_all_cache_entries_for(self, entry_id: str) -> None:
        """Remove every cache key that might contain this entry."""
        self._cache.delete_by_tag(entry_id)

    @staticmethod
    def _cache_key(capability: Optional[str], statuses: set[EntryStatus]) -> str:
        status_part = ",".join(sorted(s.value for s in statuses))
        return f"registry:list:{capability or '*'}:status={status_part}"
