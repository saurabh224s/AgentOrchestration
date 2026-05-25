"""
Regression test for Issue #3854 — capability discovery must never
return DISABLED entries, even during concurrent lifecycle transitions.
"""
import threading
import time
import pytest
from unittest.mock import MagicMock, patch

from src.registry.listing import (
    RegistryListingService,
    RegistryEntry,
    EntryStatus,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_entry(id: str, status: EntryStatus) -> RegistryEntry:
    return RegistryEntry(id=id, status=status, capabilities=["http", "grpc"])


@pytest.fixture()
def active_entry():
    return make_entry("handler-001", EntryStatus.ACTIVE)


@pytest.fixture()
def disabled_entry():
    return make_entry("handler-002", EntryStatus.DISABLED)


@pytest.fixture()
def store(active_entry, disabled_entry):
    mock = MagicMock()
    # Simulate DB returning ONLY entries whose status matches the filter
    def query(capability=None, status_in=None):
        all_entries = [active_entry, disabled_entry]
        if status_in is None:
            return all_entries
        return [e for e in all_entries if e.status in status_in]

    mock.query.side_effect = query
    mock.compare_and_set = MagicMock()
    return mock


@pytest.fixture()
def cache():
    _store = {}
    _tags = {}

    mock = MagicMock()

    def get(key):
        return _store.get(key)

    def set_(key, value):
        _store[key] = value

    def delete_by_tag(tag):
        keys_to_delete = [k for k, tags in _tags.items() if tag in tags]
        for k in keys_to_delete:
            _store.pop(k, None)
            _tags.pop(k, None)

    mock.get.side_effect = get
    mock.set.side_effect = set_
    mock.delete_by_tag.side_effect = delete_by_tag
    return mock


@pytest.fixture()
def audit_log():
    return MagicMock()


@pytest.fixture()
def service(store, cache, audit_log):
    return RegistryListingService(store=store, cache=cache, audit_log=audit_log)


# ---------------------------------------------------------------------------
# Core regression tests
# ---------------------------------------------------------------------------

class TestDisabledEntryNotLeaked:

    def test_list_excludes_disabled_entries_by_default(self, service, active_entry):
        """ACTIVE-only filter must hold at the query layer."""
        results = service.list()
        ids = [e.id for e in results]

        assert active_entry.id in ids, "Active entry must be visible"
        assert "handler-002" not in ids, "Disabled entry MUST NOT appear in listing"

    def test_list_passes_status_filter_to_store(self, service, store):
        """DB query must always receive an explicit status_in argument."""
        service.list()

        store.query.assert_called_once()
        call_kwargs = store.query.call_args.kwargs
        assert "status_in" in call_kwargs, (
            "store.query() must receive status_in — relying on post-fetch "
            "filtering is the root cause of #3854"
        )
        assert EntryStatus.DISABLED not in call_kwargs["status_in"]

    def test_disable_invalidates_cache_before_db_write(
        self, service, store, cache, audit_log
    ):
        """
        Cache eviction must happen BEFORE the DB write.
        Verifies the ordering fix in disable_entry().
        """
        call_order = []

        cache.delete_by_tag.side_effect = lambda _: call_order.append("cache_evict")
        store.compare_and_set.side_effect = lambda **_: call_order.append("db_write")

        service.disable_entry("handler-001", reason="scaling-down")

        assert call_order == ["cache_evict", "db_write"], (
            "Cache must be invalidated BEFORE the DB write; "
            f"actual order was: {call_order}"
        )

    def test_disable_emits_audit_record(self, service, audit_log):
        """Every disable must produce an auditable record."""
        service.disable_entry("handler-001", reason="policy-violation")

        audit_log.record.assert_called_once_with(
            event="entry.disabled",
            entry_id="handler-001",
            reason="policy-violation",
        )

    def test_audit_record_contains_no_secrets(self, service, audit_log):
        """Audit log must not expose tokens, credentials, or internal state."""
        service.disable_entry("handler-001", reason="maintenance")

        record_kwargs = audit_log.record.call_args.kwargs
        record_str = str(record_kwargs)

        forbidden = ["token", "secret", "password", "credential", "Bearer"]
        for word in forbidden:
            assert word.lower() not in record_str.lower(), (
                f"Audit record must not contain '{word}'"
            )


# ---------------------------------------------------------------------------
# Concurrency regression test
# ---------------------------------------------------------------------------

class TestConcurrentLifecycleTransition:

    def test_disabled_entry_not_visible_during_concurrent_transition(
        self, store, cache, audit_log
    ):
        """
        Simulates Thread A disabling a handler while Thread B is listing.
        The disabled entry must NEVER appear in Thread B's results.
        """
        service = RegistryListingService(store=store, cache=cache, audit_log=audit_log)

        leaked_disabled = []
        stop_event = threading.Event()

        def continuous_list():
            while not stop_event.is_set():
                results = service.list()
                for entry in results:
                    if entry.status == EntryStatus.DISABLED:
                        leaked_disabled.append(entry.id)
                time.sleep(0.001)

        reader = threading.Thread(target=continuous_list, daemon=True)
        reader.start()

        # Simulate the disable transition
        service.disable_entry("handler-001", reason="test-transition")
        time.sleep(0.05)  # let the reader run after the transition

        stop_event.set()
        reader.join(timeout=2)

        assert leaked_disabled == [], (
            f"Disabled entries leaked during concurrent transition: {leaked_disabled}"
        )
