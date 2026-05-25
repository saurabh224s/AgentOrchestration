# src/runtime/result_runtime.py

from __future__ import annotations
import json
import logging
import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class RunState(str, Enum):
    PENDING    = "pending"
    RUNNING    = "running"
    VALIDATING = "validating"   # NEW: intermediate guard state
    COMPLETED  = "completed"
    FAILED     = "failed"

    # Valid transitions — enforced by the state machine
    TRANSITIONS: dict = {}   # populated below

RunState.TRANSITIONS = {
    RunState.PENDING:    {RunState.RUNNING},
    RunState.RUNNING:    {RunState.VALIDATING, RunState.FAILED},
    RunState.VALIDATING: {RunState.COMPLETED,  RunState.FAILED},
    RunState.COMPLETED:  set(),   # terminal
    RunState.FAILED:     set(),   # terminal
}


@dataclass
class Run:
    run_id:   str
    state:    RunState = RunState.PENDING
    result:   Optional[str] = None   # JSON-encoded result
    lock_id:  Optional[str] = None
    attempts: int = 0


# ---------------------------------------------------------------------------
# JSON serialization validator
# ---------------------------------------------------------------------------

class _SafeEncoder(json.JSONEncoder):
    """Raises immediately on any non-serializable type."""
    def default(self, obj: Any) -> Any:
        raise TypeError(
            f"Object of type {type(obj).__name__!r} is not JSON serializable. "
            "Tool results must consist of JSON-native types only "
            "(str, int, float, bool, list, dict, None). "
            "Convert before returning from the tool."
        )


def validate_serializable(result: Any, *, run_id: str) -> str:
    """
    Validate that `result` is JSON-serializable and return the
    encoded string. Raises ValueError for non-serializable values.

    Covers: non-native types, NaN, infinity, circular references.
    """
    # 1. Reject non-finite floats (NaN / infinity produce invalid JSON)
    _check_non_finite(result, path="result")

    # 2. Full encode — raises TypeError for non-serializable types,
    #    ValueError for circular references
    try:
        encoded = json.dumps(result, cls=_SafeEncoder, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Tool result for run {run_id!r} failed JSON validation: {exc}"
        ) from exc

    return encoded


def _check_non_finite(obj: Any, path: str) -> None:
    """Recursively scan for NaN / infinity before json.dumps."""
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise ValueError(
                f"Non-finite float {obj!r} at '{path}' is not JSON-safe. "
                "Use None or a sentinel string instead."
            )
    elif isinstance(obj, dict):
        for k, v in obj.items():
            _check_non_finite(v, path=f"{path}.{k}")
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            _check_non_finite(v, path=f"{path}[{i}]")


# ---------------------------------------------------------------------------
# Result runtime
# ---------------------------------------------------------------------------

class ResultRuntime:
    """
    FIX (#4491): Validate JSON serialization of tool results before
    committing any durable state.

    State machine:
      RUNNING → VALIDATING → COMPLETED  (happy path)
      RUNNING → FAILED                  (tool error)
      VALIDATING → FAILED               (serialization error)

    The VALIDATING state is the durable guard: once a run enters it,
    no other worker can claim the terminal transition (compare-and-set).
    The result is validated and encoded while in VALIDATING, so
    COMPLETED is only ever written when a valid JSON payload exists.
    """

    MAX_ATTEMPTS = 3

    def __init__(self, store, event_bus, lock_manager, metrics):
        self._store    = store
        self._events   = event_bus
        self._locks    = lock_manager
        self._metrics  = metrics

    def commit_result(self, run_id: str, tool_result: Any) -> None:
        """
        Validate, encode, and durably commit a tool result.

        Raises:
            ValueError:     if tool_result is not JSON-serializable
            RuntimeError:   if the run is in an unexpected state
        """
        run = self._store.get(run_id)
        if run is None:
            raise RuntimeError(f"Run {run_id!r} not found.")

        with self._lock_guard(run):
            # Step 1: Transition to VALIDATING — atomic, prevents TOCTOU
            self._transition(run, RunState.VALIDATING)

            # Step 2: Validate and encode BEFORE writing COMPLETED
            # If this raises, the run transitions to FAILED (see except below)
            try:
                encoded = validate_serializable(tool_result, run_id=run_id)
            except ValueError as exc:
                self._fail(run, reason=str(exc))
                raise

            # Step 3: Persist result + COMPLETED atomically
            run.result = encoded
            self._transition(run, RunState.COMPLETED)

            # Step 4: Emit side effects AFTER durable state is committed
            self._events.emit("run.completed", {
                "run_id": run_id,
                "result": encoded,
            })
            self._metrics.increment(
                "runtime.result.committed",
                tags={"run_id": run_id},
            )
            logger.info(
                "runtime.result.committed",
                extra={"run_id": run_id, "state": RunState.COMPLETED},
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _transition(self, run: Run, new_state: RunState) -> None:
        """
        Atomic compare-and-set state transition.
        Raises RuntimeError if the transition is not allowed.
        """
        allowed = RunState.TRANSITIONS.get(run.state, set())
        if new_state not in allowed:
            raise RuntimeError(
                f"Invalid state transition: {run.state} → {new_state} "
                f"for run {run.run_id!r}. Allowed: {allowed}"
            )
        run.state = new_state
        self._store.save(run)   # durable write on every transition

    def _fail(self, run: Run, reason: str) -> None:
        """Transition to FAILED and emit audit log."""
        try:
            self._transition(run, RunState.FAILED)
        except RuntimeError:
            pass   # already terminal — safe to ignore
        logger.warning(
            "runtime.result.failed",
            extra={"run_id": run.run_id, "reason": reason},
        )

    @contextmanager
    def _lock_guard(self, run: Run) -> Iterator[None]:
        """Release the run lock even if commit_result raises."""
        try:
            yield
        finally:
            if run.lock_id:
                self._locks.release(run.lock_id)
                run.lock_id = None
                self._store.save(run)
