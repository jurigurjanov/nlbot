from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math
from collections.abc import Sequence
from typing import Any

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


def zscore(values: Sequence[float], *, min_std: float = FLOAT_ROUNDING_TOLERANCE) -> float:
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


def efficiency_ratio(values: Sequence[float], window: int) -> float | None:
    if window <= 0 or len(values) < (window + 1):
        return None
    end = len(values) - 1
    start = end - window
    try:
        change = abs(float(values[end]) - float(values[start]))
    except (TypeError, ValueError):
        return None
    noise = 0.0
    for idx in range(start + 1, end + 1):
        try:
            current = float(values[idx])
            previous = float(values[idx - 1])
        except (TypeError, ValueError):
            return None
        if not (math.isfinite(current) and math.isfinite(previous)):
            return None
        noise += abs(current - previous)
    if noise <= FLOAT_ROUNDING_TOLERANCE:
        return 0.0 if change <= FLOAT_ROUNDING_TOLERANCE else 1.0
    return max(0.0, min(1.0, change / noise))


def kama(
    values: Sequence[float],
    er_window: int,
    *,
    fast_window: int = 2,
    slow_window: int = 30,
) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        try:
            single = float(values[0])
        except (TypeError, ValueError):
            return None
        return single if math.isfinite(single) else None
    er_window = max(1, int(er_window))
    fast_window = max(1, int(fast_window))
    slow_window = max(fast_window + 1, int(slow_window))
    if len(values) < (er_window + 1):
        return None

    fast_sc = 2.0 / (fast_window + 1.0)
    slow_sc = 2.0 / (slow_window + 1.0)
    try:
        current = float(values[er_window])
    except (TypeError, ValueError):
        return None
    if not math.isfinite(current):
        return None

    for idx in range(er_window + 1, len(values)):
        er = efficiency_ratio(values[: idx + 1], er_window)
        if er is None:
            return None
        smoothing_constant = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        try:
            price = float(values[idx])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(price):
            return None
        current = current + smoothing_constant * (price - current)
    return current


def kama_last_two(
    values: Sequence[float],
    er_window: int,
    *,
    fast_window: int = 2,
    slow_window: int = 30,
) -> tuple[float | None, float | None]:
    if len(values) < max(2, er_window + 1):
        return None, None
    previous = kama(values[:-1], er_window, fast_window=fast_window, slow_window=slow_window)
    current = kama(values, er_window, fast_window=fast_window, slow_window=slow_window)
    return previous, current


def kama_slope_atr(
    values: Sequence[float],
    er_window: int,
    atr: float,
    *,
    fast_window: int = 2,
    slow_window: int = 30,
) -> float:
    if atr <= 0:
        return 0.0
    previous, current = kama_last_two(
        values,
        er_window,
        fast_window=fast_window,
        slow_window=slow_window,
    )
    if previous is None or current is None:
        return 0.0
    return (current - previous) / max(atr, FLOAT_COMPARISON_TOLERANCE)


def session_vwap_bands(
    prices: Sequence[float],
    volumes: Sequence[float],
    *,
    band_multipliers: Sequence[float] = (1.0, 2.0, 3.0),
) -> dict[str, float] | None:
    if not prices or not volumes or len(prices) != len(volumes):
        return None
    weighted_prices: list[tuple[float, float]] = []
    for raw_price, raw_volume in zip(prices, volumes):
        try:
            price = float(raw_price)
            volume = float(raw_volume)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(price) and math.isfinite(volume)):
            continue
        if volume <= 0.0:
            continue
        weighted_prices.append((price, volume))
    if not weighted_prices:
        return None

    total_volume = sum(volume for _, volume in weighted_prices)
    if total_volume <= FLOAT_ROUNDING_TOLERANCE:
        return None
    vwap = sum(price * volume for price, volume in weighted_prices) / total_volume
    variance = sum(volume * ((price - vwap) ** 2) for price, volume in weighted_prices) / total_volume
    sigma = math.sqrt(max(variance, 0.0))

    payload: dict[str, float] = {
        "vwap": vwap,
        "sigma": sigma,
        "valid_samples": float(len(weighted_prices)),
        "total_volume": total_volume,
    }
    avg_volume_per_sample = total_volume / max(float(len(weighted_prices)), 1.0)
    payload["avg_volume_per_sample"] = avg_volume_per_sample
    payload["volume_quality"] = max(0.0, min(1.0, avg_volume_per_sample / 1.0))
    for multiplier in band_multipliers:
        try:
            band = float(multiplier)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(band) or band <= 0.0:
            continue
        if abs(band - round(band)) <= FLOAT_COMPARISON_TOLERANCE:
            suffix = str(int(round(band)))
        else:
            suffix = str(band).replace(".", "_")
        payload[f"upper_{suffix}"] = vwap + sigma * band
        payload[f"lower_{suffix}"] = vwap - sigma * band
    return payload


def atr_wilder(values: Sequence[float | Sequence[float]], window: int) -> float | None:
    if len(values) < window + 1 or window <= 0:
        return None
    first_sample = values[0]
    ohlc_mode = isinstance(first_sample, Sequence) and not isinstance(first_sample, (str, bytes))
    if ohlc_mode:
        ohlc_samples: list[tuple[float, float, float]] = []
        for raw in values:
            sample = _parse_ohlc_sample(raw)
            if sample is None:
                return None
            ohlc_samples.append(sample)
        if len(ohlc_samples) < window + 1:
            return None
        true_ranges: list[float] = []
        prev_close = float(ohlc_samples[0][2])
        for high, low, close in ohlc_samples[1:]:
            current_range = max(
                max(0.0, high - low),
                abs(high - prev_close),
                abs(low - prev_close),
            )
            true_ranges.append(current_range)
            prev_close = close
        if len(true_ranges) < window:
            return None
        atr = sum(true_ranges[:window]) / float(window)
        for current_range in true_ranges[window:]:
            atr = ((atr * (window - 1)) + current_range) / float(window)
        return atr
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


def _parse_ohlc_sample(raw: Any) -> tuple[float, float, float] | None:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return None
    length = len(raw)
    if length < 3:
        return None
    try:
        if length >= 4:
            # Assume OHLC layout: [open, high, low, close, ...]
            high = float(raw[1])
            low = float(raw[2])
            close = float(raw[3])
        else:
            # Assume HLC layout: [high, low, close]
            high = float(raw[0])
            low = float(raw[1])
            close = float(raw[2])
    except (TypeError, ValueError):
        return None
    if not (
        math.isfinite(high)
        and math.isfinite(low)
        and math.isfinite(close)
    ):
        return None
    if low > high:
        high, low = low, high
    return high, low, close


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


def rsi_sma(prices: Sequence[float], period: int | None = None) -> float:
    if len(prices) < 2:
        return 50.0
    if period is not None:
        effective_period = max(1, int(period))
        if len(prices) > (effective_period + 1):
            prices = prices[-(effective_period + 1) :]
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

    if avg_loss <= FLOAT_ROUNDING_TOLERANCE and avg_gain <= FLOAT_ROUNDING_TOLERANCE:
        return 50.0
    if avg_loss <= FLOAT_ROUNDING_TOLERANCE:
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

    if avg_loss <= FLOAT_ROUNDING_TOLERANCE and avg_gain <= FLOAT_ROUNDING_TOLERANCE:
        return 50.0
    if avg_loss <= FLOAT_ROUNDING_TOLERANCE:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
