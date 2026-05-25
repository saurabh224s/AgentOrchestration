# tests/runtime/test_result_runtime_json_validation.py
"""
Regression tests for Issue #4491 —
ResultRuntime must validate JSON serialization of tool results
BEFORE committing any durable state.
"""
import json
import math
import threading
import pytest
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, call

from src.runtime.result_runtime import (
    ResultRuntime,
    Run,
    RunState,
    validate_serializable,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_run(run_id: str = "run-001", state: RunState = RunState.RUNNING) -> Run:
    return Run(run_id=run_id, state=state, lock_id="lock-abc")


@pytest.fixture()
def store():
    _db = {}
    mock = MagicMock()
    mock.get.side_effect  = lambda rid: _db.get(rid)
    mock.save.side_effect = lambda r: _db.update({r.run_id: r})
    return mock


@pytest.fixture()
def event_bus():
    return MagicMock()


@pytest.fixture()
def lock_manager():
    return MagicMock()


@pytest.fixture()
def metrics():
    return MagicMock()


@pytest.fixture()
def runtime(store, event_bus, lock_manager, metrics):
    return ResultRuntime(
        store=store,
        event_bus=event_bus,
        lock_manager=lock_manager,
        metrics=metrics,
    )


# ---------------------------------------------------------------------------
# validate_serializable — unit tests
# ---------------------------------------------------------------------------

class TestValidateSerializable:

    def test_valid_dict_returns_encoded_string(self):
        result = validate_serializable({"status": "ok", "count": 42}, run_id="r1")
        assert json.loads(result) == {"status": "ok", "count": 42}

    def test_valid_list_passes(self):
        result = validate_serializable([1, "two", None, True], run_id="r1")
        assert json.loads(result) == [1, "two", None, True]

    def test_none_passes(self):
        result = validate_serializable(None, run_id="r1")
        assert json.loads(result) is None

    def test_datetime_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            validate_serializable({"ts": datetime.now()}, run_id="r1")

    def test_bytes_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            validate_serializable(b"raw bytes", run_id="r1")

    def test_decimal_raises(self):
        with pytest.raises(ValueError, match="JSON"):
            validate_serializable(Decimal("3.14"), run_id="r1")

    def test_nan_raises(self):
        with pytest.raises(ValueError, match="[Nn]on-finite"):
            validate_serializable(float("nan"), run_id="r1")

    def test_infinity_raises(self):
        with pytest.raises(ValueError, match="[Nn]on-finite"):
            validate_serializable(float("inf"), run_id="r1")

    def test_nested_nan_raises(self):
        with pytest.raises(ValueError, match="[Nn]on-finite"):
            validate_serializable({"score": float("nan")}, run_id="r1")

    def test_circular_reference_raises(self):
        circular: dict = {}
        circular["self"] = circular
        with pytest.raises(ValueError, match="JSON"):
            validate_serializable(circular, run_id="r1")

    def test_custom_class_raises(self):
        class Opaque:
            pass
        with pytest.raises(ValueError, match="JSON"):
            validate_serializable(Opaque(), run_id="r1")


# ---------------------------------------------------------------------------
# ResultRuntime.commit_result — state machine + validation order
# ---------------------------------------------------------------------------

class TestCommitResultHappyPath:

    def test_commit_valid_result_transitions_to_completed(
        self, runtime, store
    ):
        run = make_run()
        store.save(run)

        runtime.commit_result("run-001", {"output": "hello"})

        saved = store.get("run-001")
        assert saved.state == RunState.COMPLETED

    def test_commit_stores_json_encoded_result(self, runtime, store):
        run = make_run()
        store.save(run)

        runtime.commit_result("run-001", {"value": 42})

        saved = store.get("run-001")
        assert saved.result is not None
        assert json.loads(saved.result) == {"value": 42}

    def test_commit_emits_run_completed_event(
        self, runtime, store, event_bus
    ):
        run = make_run()
        store.save(run)

        runtime.commit_result("run-001", {"ok": True})

        event_bus.emit.assert_called_once()
        args = event_bus.emit.call_args
        assert args[0][0] == "run.completed"

    def test_lock_released_after_commit(
        self, runtime, store, lock_manager
    ):
        run = make_run()
        store.save(run)

        runtime.commit_result("run-001", {"x": 1})

        lock_manager.release.assert_called_once_with("lock-abc")


class TestCommitResultValidationGuard:

    def test_non_serializable_result_transitions_to_failed(
        self, runtime, store
    ):
        """
        Core regression: COMPLETED must NOT be written if the result
        fails JSON validation. Run must end in FAILED.
        """
        run = make_run()
        store.save(run)

        with pytest.raises(ValueError):
            runtime.commit_result("run-001", datetime.now())

        saved = store.get("run-001")
        assert saved.state == RunState.FAILED, (
            "Run must be FAILED, not COMPLETED, when result is not serializable"
        )

    def test_completed_not_written_before_validation(
        self, runtime, store
    ):
        """
        The premature SUCCESS bug: COMPLETED must never be stored
        before validation passes.
        """
        run = make_run()
        store.save(run)

        states_seen = []
        original_save = store.save.side_effect

        def tracking_save(r):
            states_seen.append(r.state)
            original_save(r)

        store.save.side_effect = tracking_save

        with pytest.raises(ValueError):
            runtime.commit_result("run-001", b"not serializable")

        assert RunState.COMPLETED not in states_seen, (
            "COMPLETED must never be written before validation passes — "
            "this is the root cause of #4491"
        )

    def test_event_not_emitted_on_validation_failure(
        self, runtime, store, event_bus
    ):
        run = make_run()
        store.save(run)

        with pytest.raises(ValueError):
            runtime.commit_result("run-001", {"bad": float("nan")})

        event_bus.emit.assert_not_called()

    def test_lock_released_even_on_validation_failure(
        self, runtime, store, lock_manager
    ):
        """No orphaned locks — finally block must always release."""
        run = make_run()
        store.save(run)

        with pytest.raises(ValueError):
            runtime.commit_result("run-001", datetime.now())

        lock_manager.release.assert_called_once_with("lock-abc")


class TestConcurrentCommitPrevented:

    def test_second_worker_cannot_double_commit(self, store, event_bus, lock_manager, metrics):
        """
        TOCTOU guard: two workers racing to commit must produce exactly
        one COMPLETED, not two.
        """
        run = make_run()
        store.save(run)

        completed_count = [0]
        errors = []

        def worker(payload):
            rt = ResultRuntime(
                store=store,
                event_bus=event_bus,
                lock_manager=lock_manager,
                metrics=metrics,
            )
            try:
                rt.commit_result("run-001", payload)
                completed_count[0] += 1
            except RuntimeError:
                pass   # expected for the second worker
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=worker, args=({"worker": "A"},))
        t2 = threading.Thread(target=worker, args=({"worker": "B"},))
        t1.start(); t2.start()
        t1.join();  t2.join()

        assert errors == [], f"Unexpected errors: {errors}"
        assert completed_count[0] == 1, (
            f"Expected exactly 1 COMPLETED commit, got {completed_count[0]}"
        )
