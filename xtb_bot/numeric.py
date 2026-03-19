from __future__ import annotations

import math
from collections.abc import Sequence

try:
    import numpy as _np
except Exception:  # pragma: no cover
    _np = None


NUMPY_AVAILABLE = _np is not None


def _as_float_array(values: Sequence[float]):
    if _np is None:
        return None
    return _np.asarray(values, dtype=float)


def mean_std(values: Sequence[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    arr = _as_float_array(values)
    if arr is not None:
        mean = float(_np.mean(arr))
        std = float(_np.std(arr))
        return mean, std
    mean = sum(float(v) for v in values) / float(len(values))
    variance = sum((float(v) - mean) ** 2 for v in values) / float(len(values))
    return mean, math.sqrt(max(variance, 0.0))


def tail_mean(values: Sequence[float], window: int) -> float:
    if not values or window <= 0:
        return 0.0
    subset = values[-window:]
    arr = _as_float_array(subset)
    if arr is not None:
        return float(_np.mean(arr))
    return sum(float(v) for v in subset) / float(len(subset))


def zscore(values: Sequence[float], *, min_std: float = 1e-12) -> float:
    if not values:
        return 0.0
    mean, std = mean_std(values)
    if std <= min_std:
        return 0.0
    return (float(values[-1]) - mean) / std


def ema(values: Sequence[float], window: int) -> float:
    if not values:
        return 0.0
    if window <= 1:
        return float(values[-1])
    alpha = 2.0 / (window + 1.0)
    arr = _as_float_array(values)
    if arr is not None:
        out = float(arr[0])
        for value in arr[1:]:
            out = alpha * float(value) + (1.0 - alpha) * out
        return out
    out = float(values[0])
    for value in values[1:]:
        out = alpha * float(value) + (1.0 - alpha) * out
    return out


def ema_last_two(values: Sequence[float], window: int) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        single = float(values[0])
        return single, single
    if window <= 1:
        return float(values[-2]), float(values[-1])
    alpha = 2.0 / (window + 1.0)
    arr = _as_float_array(values)
    if arr is not None:
        current = float(arr[0])
        previous = current
        for value in arr[1:]:
            previous = current
            current = alpha * float(value) + (1.0 - alpha) * current
        return previous, current
    current = float(values[0])
    previous = current
    for value in values[1:]:
        previous = current
        current = alpha * float(value) + (1.0 - alpha) * current
    return previous, current


def atr_wilder(values: Sequence[float], window: int) -> float | None:
    if len(values) < window + 1 or window <= 0:
        return None
    arr = _as_float_array(values)
    if arr is not None:
        diffs = _np.abs(_np.diff(arr))
        if diffs.size < window:
            return None
        atr = float(_np.mean(diffs[:window]))
        for value in diffs[window:]:
            atr = ((atr * (window - 1)) + float(value)) / float(window)
        return atr
    ranges = [abs(float(values[idx]) - float(values[idx - 1])) for idx in range(1, len(values))]
    if len(ranges) < window:
        return None
    atr = sum(ranges[:window]) / float(window)
    for current_range in ranges[window:]:
        atr = ((atr * (window - 1)) + current_range) / float(window)
    return atr


def adx_from_close(values: Sequence[float], window: int) -> float | None:
    if len(values) < window + 2 or window <= 0:
        return None

    arr = _as_float_array(values)
    if arr is not None:
        diffs = _np.diff(arr)
        if diffs.size < window:
            return None
        up = _np.where(diffs > 0.0, diffs, 0.0)
        down = _np.where(diffs < 0.0, -diffs, 0.0)
        tr = _np.abs(diffs)
    else:
        diffs = [float(values[idx]) - float(values[idx - 1]) for idx in range(1, len(values))]
        if len(diffs) < window:
            return None
        up = [delta if delta > 0 else 0.0 for delta in diffs]
        down = [-delta if delta < 0 else 0.0 for delta in diffs]
        tr = [abs(delta) for delta in diffs]

    atr = float(_np.mean(tr[:window])) if arr is not None else sum(tr[:window]) / float(window)
    plus_smoothed = float(_np.mean(up[:window])) if arr is not None else sum(up[:window]) / float(window)
    minus_smoothed = float(_np.mean(down[:window])) if arr is not None else sum(down[:window]) / float(window)

    dx_values: list[float] = []
    for idx in range(window, len(tr)):
        tr_value = float(tr[idx])
        up_value = float(up[idx])
        down_value = float(down[idx])

        atr = ((atr * (window - 1)) + tr_value) / float(window)
        plus_smoothed = ((plus_smoothed * (window - 1)) + up_value) / float(window)
        minus_smoothed = ((minus_smoothed * (window - 1)) + down_value) / float(window)

        if atr <= 0:
            dx_values.append(0.0)
            continue

        plus_di = 100.0 * (plus_smoothed / atr)
        minus_di = 100.0 * (minus_smoothed / atr)
        denominator = plus_di + minus_di
        if denominator <= 0:
            dx_values.append(0.0)
        else:
            dx_values.append(100.0 * abs(plus_di - minus_di) / denominator)

    if not dx_values:
        return None
    if len(dx_values) < window:
        return sum(dx_values) / float(len(dx_values))

    adx = sum(dx_values[:window]) / float(window)
    for value in dx_values[window:]:
        adx = ((adx * (window - 1)) + value) / float(window)
    return adx


def rsi_sma(prices: Sequence[float]) -> float:
    if len(prices) < 2:
        return 50.0
    arr = _as_float_array(prices)
    if arr is not None:
        deltas = _np.diff(arr)
        gains = _np.where(deltas > 0.0, deltas, 0.0)
        losses = _np.where(deltas < 0.0, -deltas, 0.0)
        avg_gain = float(_np.mean(gains)) if gains.size > 0 else 0.0
        avg_loss = float(_np.mean(losses)) if losses.size > 0 else 0.0
    else:
        deltas = [float(prices[idx]) - float(prices[idx - 1]) for idx in range(1, len(prices))]
        gains = [max(delta, 0.0) for delta in deltas]
        losses = [abs(min(delta, 0.0)) for delta in deltas]
        avg_gain = sum(gains) / float(len(gains)) if gains else 0.0
        avg_loss = sum(losses) / float(len(losses)) if losses else 0.0

    if avg_loss <= 1e-12 and avg_gain <= 1e-12:
        return 50.0
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def rsi_wilder(prices: Sequence[float], period: int) -> float:
    if len(prices) < 2:
        return 50.0
    period = max(1, int(period))
    deltas = [float(prices[idx]) - float(prices[idx - 1]) for idx in range(1, len(prices))]
    if not deltas:
        return 50.0

    period = min(period, len(deltas))
    initial = deltas[:period]
    avg_gain = sum(max(delta, 0.0) for delta in initial) / float(period)
    avg_loss = sum(abs(min(delta, 0.0)) for delta in initial) / float(period)

    for delta in deltas[period:]:
        gain = max(delta, 0.0)
        loss = abs(min(delta, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / float(period)
        avg_loss = ((avg_loss * (period - 1)) + loss) / float(period)

    if avg_loss <= 1e-12 and avg_gain <= 1e-12:
        return 50.0
    if avg_loss <= 1e-12:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
