"""
Regression tests for Issue #3873 —
MetricsCollector.gauge must reject non-finite values.
"""
import json
import math
import pytest

from codebase.src.common.metrics import MetricsCollector


@pytest.fixture()
def collector():
    return MetricsCollector()


# ---------------------------------------------------------------------------
# Core regression: non-finite values must be rejected
# ---------------------------------------------------------------------------

class TestGaugeRejectsNonFinite:

    def test_nan_raises_value_error(self, collector):
        """NaN must be rejected — it is the most common silent corruption."""
        with pytest.raises(ValueError, match="non-finite"):
            collector.gauge("cpu_usage", float("nan"))

    def test_positive_infinity_raises_value_error(self, collector):
        with pytest.raises(ValueError, match="non-finite"):
            collector.gauge("queue_depth", float("inf"))

    def test_negative_infinity_raises_value_error(self, collector):
        with pytest.raises(ValueError, match="non-finite"):
            collector.gauge("latency_ms", float("-inf"))

    def test_error_message_includes_metric_name(self, collector):
        """Error message must identify which gauge triggered the problem."""
        with pytest.raises(ValueError, match="my_metric"):
            collector.gauge("my_metric", float("nan"))

    def test_non_finite_value_not_stored_after_rejection(self, collector):
        """Rejected values must not appear in the snapshot."""
        with pytest.raises(ValueError):
            collector.gauge("bad_metric", float("nan"))

        snapshot = collector.snapshot()
        assert "bad_metric" not in snapshot


# ---------------------------------------------------------------------------
# Happy path: finite values must still work
# ---------------------------------------------------------------------------

class TestGaugeAcceptsFinite:

    def test_positive_integer(self, collector):
        collector.gauge("requests", 42)
        assert collector.snapshot()["requests"] == 42.0

    def test_zero(self, collector):
        collector.gauge("errors", 0)
        assert collector.snapshot()["errors"] == 0.0

    def test_negative_float(self, collector):
        collector.gauge("temperature", -3.14)
        assert math.isclose(collector.snapshot()["temperature"], -3.14)

    def test_very_large_finite_float(self, collector):
        collector.gauge("big", 1.7976931348623157e+308)
        assert math.isfinite(collector.snapshot()["big"])

    def test_snapshot_is_json_serialisable(self, collector):
        """The whole point — snapshots must survive json.dumps."""
        collector.gauge("cpu", 55.5)
        collector.gauge("mem", 1024.0)
        # Must not raise
        result = json.dumps(collector.snapshot())
        assert "cpu" in result
        assert "mem" in result


# ---------------------------------------------------------------------------
# Type safety
# ---------------------------------------------------------------------------

class TestGaugeTypeChecks:

    def test_string_raises_type_error(self, collector):
        with pytest.raises(TypeError):
            collector.gauge("latency", "fast")

    def test_none_raises_type_error(self, collector):
        with pytest.raises(TypeError):
            collector.gauge("latency", None)


# ---------------------------------------------------------------------------
# Snapshot isolation
# ---------------------------------------------------------------------------

class TestSnapshotIsolation:

    def test_snapshot_returns_copy(self, collector):
        """Mutating the snapshot must not affect internal state."""
        collector.gauge("x", 1.0)
        snap = collector.snapshot()
        snap["x"] = 999.0
        assert collector.snapshot()["x"] == 1.0
