from __future__ import annotations

import math
import time

_MAX_FUTURE_YEARS = 25.0
_UNIX_TS_UNIT_CUTOFF_MS = 10_000_000_000.0
_UNIX_TS_UNIT_CUTOFF_US = 10_000_000_000_000.0
_UNIX_TS_HARD_UPPER_SEC = 4_102_444_800.0  # 2100-01-01 UTC


def plausible_unix_timestamp_upper_sec(now_ts: float | None = None) -> float:
    reference = time.time() if now_ts is None else float(now_ts)
    future_window_sec = _MAX_FUTURE_YEARS * 366.0 * 24.0 * 3600.0
    return max(reference + future_window_sec, _UNIX_TS_HARD_UPPER_SEC)


def normalize_unix_timestamp_seconds(
    raw_ts: float | int | str | None,
    *,
    fallback_ts: float | None = None,
    now_ts: float | None = None,
) -> float:
    fallback = time.time() if fallback_ts is None else float(fallback_ts)
    try:
        ts = float(raw_ts)
    except (TypeError, ValueError):
        return fallback
    if not math.isfinite(ts) or ts <= 0:
        return fallback

    abs_ts = abs(ts)
    upper_sec = plausible_unix_timestamp_upper_sec(now_ts)
    if abs_ts <= upper_sec:
        return ts
    if _UNIX_TS_UNIT_CUTOFF_MS <= abs_ts <= (upper_sec * 1_000.0):
        return ts / 1_000.0
    if _UNIX_TS_UNIT_CUTOFF_US <= abs_ts <= (upper_sec * 1_000_000.0):
        return ts / 1_000_000.0
    return fallback
