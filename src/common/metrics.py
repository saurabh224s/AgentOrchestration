#
# Only the gauge() method needs to change.
# Everything else in MetricsCollector stays the same.

import math
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects named gauge values for export."""

    def __init__(self) -> None:
        self._gauges: Dict[str, float] = {}

    def gauge(self, name: str, value: float) -> None:
        """
        Record a gauge value.

        FIX (#3873): Rejects non-finite values (NaN, +inf, -inf) before
        storing them. Non-finite floats break JSON serialisation and can
        cause exporters to fail or silently drop entire metric batches.

        Args:
            name:  Metric name (non-empty string).
            value: Numeric value — must be a finite float or int.

        Raises:
            ValueError: If value is NaN, +infinity, or -infinity.
            TypeError:  If value is not numeric.
        """
        if not isinstance(value, (int, float)):
            raise TypeError(
                f"gauge '{name}': value must be numeric, got {type(value).__name__}"
            )

        # Core fix — math.isfinite returns False for NaN, +inf, and -inf.
        if not math.isfinite(value):
            raise ValueError(
                f"gauge '{name}': non-finite value {value!r} is not allowed. "
                "Gauge values must be finite for JSON compatibility. "
                "Check for division-by-zero or unconverged calculations "
                "before recording this metric."
            )

        self._gauges[name] = float(value)
        logger.debug("gauge.recorded", extra={"name": name, "value": value})

    def snapshot(self) -> Dict[str, Any]:
        """Return a copy of all current gauge values."""
        return dict(self._gauges)
# 2021-08-30T09:47:24 update

# 2021-10-19T13:43:46 update

# 2021-10-21T16:07:56 update

# 2021-12-27T08:18:40 update

# 2022-03-09T16:48:09 update

# 2022-03-29T10:51:15 update

# 2022-05-19T09:07:00 update

# 2022-06-08T15:24:11 update

# 2022-08-17T08:23:02 update

# 2022-08-20T16:37:39 update

# 2022-12-07T15:19:57 update

# 2022-12-26T11:59:00 update

# 2023-01-26T20:15:04 update

# 2023-02-01T10:10:52 update

# 2023-05-04T11:13:12 update

# 2023-07-06T08:27:57 update

# 2023-07-24T12:34:13 update

# 2023-08-31T15:00:03 update

# 2023-09-16T20:55:20 update

# 2023-12-08T16:55:55 update

# 2024-01-04T15:47:36 update

# 2024-01-05T14:46:16 update

# 2024-04-08T10:08:30 update

# 2024-04-08T20:31:02 update

# 2024-08-13T17:18:11 update

# 2024-09-13T08:11:06 update

# 2024-12-06T11:42:59 update

# 2025-02-03T11:41:46 update

# 2025-03-22T09:28:10 update

# 2025-04-06T11:12:26 update

# 2025-04-09T13:39:45 update

# 2025-08-07T15:56:14 update

# 2025-08-20T10:41:17 update

# 2025-10-16T17:51:05 update

# 2025-10-16T14:29:07 update

# 2025-12-05T13:05:17 update

# 2025-12-12T09:49:47 update

# 2025-12-19T13:59:03 update

# 2026-01-13T16:00:24 update

# 2026-02-05T14:23:35 update

# 2026-03-13T08:25:05 update

# 2026-04-23T12:24:44 update

# 2026-05-18T20:56:34 update
