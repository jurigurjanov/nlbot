from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
import math
from typing import Any

from xtb_bot.models import Side, Signal
from xtb_bot.pip_size import (
    normalize_symbol,
    normalize_tick_size_to_pip_size,
    parse_symbol_pip_size_map,
    symbol_pip_size_fallback,
)


HOLD_STATE_NEUTRAL = "neutral"
HOLD_STATE_BLOCKED = "blocked"
HOLD_STATE_UNAVAILABLE = "unavailable"
HOLD_STATE_SIGNAL = "signal"
HOLD_STATE_METADATA_KEY = "hold_state"

_UNAVAILABLE_HOLD_REASONS: frozenset[str] = frozenset(
    {
        "component_error",
        "higher_tf_bias_unavailable",
        "invalid_timestamp",
        "missing_timestamp",
        "multi_components_unavailable",
        "rsi_unavailable",
        "volume_unavailable",
        "vwap_unavailable",
    }
)
_UNAVAILABLE_HOLD_REASON_PREFIXES: tuple[str, ...] = (
    "insufficient_",
    "missing_",
    "stale_",
    "unavailable",
    "unavailable_",
    "no_context_",
)
_BLOCKED_HOLD_REASONS: frozenset[str] = frozenset(
    {
        "adx_below_threshold",
        "higher_tf_bias_mismatch",
        "kama_chop_regime",
        "outside_trading_session",
        "session_filter_blocked",
        "spread_too_wide",
        "spread_too_wide_relative_to_atr",
        "spread_too_wide_relative_to_stop",
        "volume_below_threshold",
    }
)
_BLOCKED_HOLD_REASON_PREFIXES: tuple[str, ...] = (
    "session_",
    "spread_",
    "volume_",
    "vwap_",
)
_NEUTRAL_HOLD_REASONS: frozenset[str] = frozenset(
    {
        "band_touch_too_shallow",
        "band_walk_no_reentry",
        "breakout_invalidated",
        "low_volatility",
        "multi_all_hold",
        "multi_no_setup",
        "no_ema_cross",
        "no_ma_cross",
        "no_pullback_entry_atr",
        "no_pullback_reset",
        "no_signal",
        "no_signal_in_active_regime",
        "pullback_countermove_in_progress",
        "trend_slope_too_weak",
        "trend_too_weak",
        "waiting_for_pullback",
    }
)
_NEUTRAL_HOLD_REASON_PREFIXES: tuple[str, ...] = (
    "band_",
    "breakout_",
    "low_volatility",
    "no_",
    "pullback_",
    "trend_too_",
    "waiting_",
)


def classify_hold_reason(
    reason: object,
    metadata: dict[str, Any] | None = None,
) -> str:
    if isinstance(metadata, dict):
        explicit = str(metadata.get(HOLD_STATE_METADATA_KEY) or "").strip().lower()
        if explicit in {HOLD_STATE_NEUTRAL, HOLD_STATE_BLOCKED, HOLD_STATE_UNAVAILABLE}:
            return explicit

    normalized = str(reason or "").strip().lower()
    if not normalized:
        return HOLD_STATE_NEUTRAL
    if (
        normalized in _UNAVAILABLE_HOLD_REASONS
        or any(normalized.startswith(prefix) for prefix in _UNAVAILABLE_HOLD_REASON_PREFIXES)
    ):
        return HOLD_STATE_UNAVAILABLE
    if (
        normalized in _BLOCKED_HOLD_REASONS
        or any(normalized.startswith(prefix) for prefix in _BLOCKED_HOLD_REASON_PREFIXES)
    ):
        return HOLD_STATE_BLOCKED
    if (
        normalized in _NEUTRAL_HOLD_REASONS
        or any(normalized.startswith(prefix) for prefix in _NEUTRAL_HOLD_REASON_PREFIXES)
    ):
        return HOLD_STATE_NEUTRAL
    return HOLD_STATE_BLOCKED


def annotate_hold_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    payload = dict(metadata)
    payload[HOLD_STATE_METADATA_KEY] = classify_hold_reason(payload.get("reason"), payload)
    return payload


@dataclass(slots=True)
class StrategyContext:
    symbol: str
    prices: Sequence[float]
    timestamps: Sequence[float] = field(default_factory=tuple)
    volumes: Sequence[float] = field(default_factory=tuple)
    current_volume: float | None = None
    current_spread_pips: float | None = None
    tick_size: float | None = None
    pip_size: float | None = None


class Strategy(ABC):
    name: str = "base"
    min_history: int = 2
    _RUNTIME_SYMBOL_PIP_SIZES_KEY = "_runtime_symbol_pip_sizes"
    _ENTRY_QUALITY_WINDOW = 12
    _TREND_ENTRY_EFFICIENCY_SOFT = 0.32
    _TREND_ENTRY_ALIGNMENT_SOFT = 0.60
    _TREND_ENTRY_LAST_MOVE_SHARE_SOFT = 0.68
    _MEAN_REVERSION_REVERSAL_SHARE_SOFT = 0.18
    _MEAN_REVERSION_LAST_MOVE_SHARE_HARD = 0.78
    _MEAN_REVERSION_OPPOSITE_SHARE_HARD = 0.92

    def __init__(self, params: dict[str, Any]):
        self.params = params
        self._runtime_symbol_pip_sizes = parse_symbol_pip_size_map(
            params.get(self._RUNTIME_SYMBOL_PIP_SIZES_KEY),
        )

    def supports_symbol(self, symbol: str) -> bool:
        return True

    @abstractmethod
    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raise NotImplementedError

    def hold(self) -> Signal:
        stop_loss_pips = self._hold_stop_loss_pips()
        take_profit_pips = self._hold_take_profit_pips()
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            metadata=annotate_hold_metadata({"reason": "no_signal"}),
        )

    # ------------------------------------------------------------------
    # Param-reading helpers (typed access to self.params with clamping)
    # ------------------------------------------------------------------

    def _param_float(
        self,
        key: str,
        default: float = 0.0,
        *,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> float:
        """Read a float param with optional clamping."""
        try:
            val = float(self.params.get(key, default))
        except (TypeError, ValueError):
            val = float(default)
        if min_val is not None:
            val = max(val, min_val)
        if max_val is not None:
            val = min(val, max_val)
        return val

    def _param_int(
        self,
        key: str,
        default: int = 0,
        *,
        min_val: int | None = None,
        max_val: int | None = None,
    ) -> int:
        """Read an int param with optional clamping."""
        try:
            val = int(self.params.get(key, default))
        except (TypeError, ValueError):
            val = int(default)
        if min_val is not None:
            val = max(val, min_val)
        if max_val is not None:
            val = min(val, max_val)
        return val

    def _param_bool(self, key: str, default: bool = False) -> bool:
        """Read a bool param, handling string representations."""
        raw = self.params.get(key, default)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            return raw.strip().lower() not in ("0", "false", "no", "off", "")
        return bool(raw)

    @staticmethod
    def _as_bool(value: object, default: bool = False, *, strict_strings: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            if strict_strings:
                return default
            return lowered not in {"0", "false", "no", "off"}
        return bool(value)

    @staticmethod
    def _timestamp_to_seconds(raw_ts: float | int | str | None, *, default: float = 0.0) -> float:
        try:
            ts = float(raw_ts)
        except (TypeError, ValueError):
            return float(default)
        abs_ts = abs(ts)
        if abs_ts > 10_000_000_000_000:
            return ts / 1_000_000.0
        if abs_ts > 10_000_000_000:
            return ts / 1_000.0
        return ts

    def _hold_stop_loss_pips(self) -> float:
        return self._resolve_hold_distance(
            ("stop_loss_pips", "min_stop_loss_pips", "base_stop_loss_pips"),
            param_key="stop_loss_pips",
            default=25.0,
        )

    def _hold_take_profit_pips(self) -> float:
        return self._resolve_hold_distance(
            ("take_profit_pips", "min_take_profit_pips", "base_take_profit_pips"),
            param_key="take_profit_pips",
            default=50.0,
        )

    def _resolve_hold_distance(
        self,
        attr_names: Sequence[str],
        *,
        param_key: str,
        default: float,
    ) -> float:
        for attr_name in attr_names:
            candidate = getattr(self, attr_name, None)
            try:
                numeric = float(candidate)
            except (TypeError, ValueError):
                continue
            if math.isfinite(numeric) and numeric > 0.0:
                return numeric
        try:
            fallback = float(self.params.get(param_key, default))
        except (TypeError, ValueError):
            fallback = float(default)
        if math.isfinite(fallback) and fallback > 0.0:
            return fallback
        return float(default)

    @staticmethod
    def _extract_finite_prices(values: Sequence[object]) -> tuple[list[float], int]:
        cleaned: list[float] = []
        invalid = 0
        for raw in values:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                invalid += 1
                continue
            if not math.isfinite(value):
                invalid += 1
                continue
            if value <= 0.0:
                invalid += 1
                continue
            cleaned.append(value)
        return cleaned, invalid

    @staticmethod
    def _extract_finite_prices_and_timestamps(
        prices: Sequence[object],
        timestamps: Sequence[object],
        *,
        timestamp_normalizer: Callable[[object], float] | None = None,
        require_non_negative_timestamps: bool = True,
    ) -> tuple[list[float], list[float], int]:
        cleaned_prices: list[float] = []
        cleaned_timestamps: list[float] = []
        invalid_prices = 0
        timestamps_usable = len(timestamps) == len(prices)

        for idx, raw_price in enumerate(prices):
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                invalid_prices += 1
                continue
            if not math.isfinite(price):
                invalid_prices += 1
                continue
            if price <= 0.0:
                invalid_prices += 1
                continue

            cleaned_prices.append(price)
            if timestamps_usable:
                try:
                    raw_timestamp = timestamps[idx]
                    timestamp = (
                        timestamp_normalizer(raw_timestamp)
                        if timestamp_normalizer is not None
                        else float(raw_timestamp)
                    )
                except (TypeError, ValueError):
                    timestamps_usable = False
                    continue
                if not math.isfinite(timestamp):
                    timestamps_usable = False
                    continue
                if require_non_negative_timestamps and timestamp < 0:
                    timestamps_usable = False
                    continue
                cleaned_timestamps.append(float(timestamp))

        if not timestamps_usable:
            cleaned_timestamps = []
        return cleaned_prices, cleaned_timestamps, invalid_prices

    @staticmethod
    def _extract_finite_timestamps(
        values: Sequence[object],
        *,
        timestamp_normalizer: Callable[[object], float] | None = None,
        require_non_negative_timestamps: bool = True,
    ) -> list[float]:
        cleaned: list[float] = []
        for raw in values:
            try:
                timestamp = (
                    timestamp_normalizer(raw)
                    if timestamp_normalizer is not None
                    else float(raw)
                )
            except (TypeError, ValueError):
                continue
            if not math.isfinite(timestamp):
                continue
            if require_non_negative_timestamps and timestamp < 0:
                continue
            cleaned.append(float(timestamp))
        return cleaned

    @staticmethod
    def _extract_positive_finite_values(values: Sequence[object]) -> list[float]:
        cleaned: list[float] = []
        for raw in values:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value) or value <= 0.0:
                continue
            cleaned.append(value)
        return cleaned

    @staticmethod
    def _coerce_positive_finite(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed) or parsed <= 0.0:
            return None
        return parsed

    @staticmethod
    def _values_close(left: float, right: float) -> bool:
        return math.isclose(float(left), float(right), rel_tol=1e-6, abs_tol=FLOAT_COMPARISON_TOLERANCE)

    @staticmethod
    def _clamp01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    def _entry_quality_family(self, metadata: dict[str, Any]) -> str:
        strategy_name = str(getattr(self, "name", "") or "").strip().lower()
        if strategy_name == "index_hybrid":
            regime = str(metadata.get("regime") or "").strip().lower()
            if regime == "mean_reversion":
                return "mean_reversion"
            if regime == "trend_following":
                return "trend"
        if strategy_name == "mean_reversion_bb":
            return "mean_reversion"
        if strategy_name in {
            "momentum",
            "donchian_breakout",
            "g1",
            "trend_following",
            "crypto_trend_following",
            "mean_breakout_v2",
            "index_hybrid",
        }:
            return "trend"
        return "neutral"

    def _entry_quality_metrics(
        self,
        prices: Sequence[float],
        side: Side,
    ) -> dict[str, object] | None:
        sample_count = min(len(prices) - 1, self._ENTRY_QUALITY_WINDOW)
        if sample_count < 3:
            return None

        window_prices = [float(value) for value in prices[-(sample_count + 1) :]]
        diffs = [window_prices[idx] - window_prices[idx - 1] for idx in range(1, len(window_prices))]
        path = sum(abs(diff) for diff in diffs)
        if not math.isfinite(path) or path <= FLOAT_COMPARISON_TOLERANCE:
            return None

        sign = 1.0 if side == Side.BUY else -1.0
        last_move = diffs[-1]
        last_move_share = abs(last_move) / path
        last_move_in_signal_direction = (sign * last_move) > 0.0
        aligned_path_share = sum(max(sign * diff, 0.0) for diff in diffs) / path
        opposite_path_share = sum(max(-sign * diff, 0.0) for diff in diffs) / path
        displacement = abs(window_prices[-1] - window_prices[0])
        path_efficiency = displacement / path

        return {
            "entry_quality_window": sample_count,
            "entry_quality_path_efficiency": self._clamp01(path_efficiency),
            "entry_quality_aligned_path_share": self._clamp01(aligned_path_share),
            "entry_quality_opposite_path_share": self._clamp01(opposite_path_share),
            "entry_quality_last_move_share": self._clamp01(last_move_share),
            "entry_quality_last_move_in_signal_direction": last_move_in_signal_direction,
            "entry_quality_reversal_share": (
                self._clamp01(last_move_share) if last_move_in_signal_direction else 0.0
            ),
        }

    def _entry_quality_adjustment(
        self,
        family: str,
        metrics: dict[str, object],
    ) -> tuple[float, list[str], str]:
        penalty = 0.0
        reasons: list[str] = []
        path_efficiency = float(metrics.get("entry_quality_path_efficiency") or 0.0)
        aligned_path_share = float(metrics.get("entry_quality_aligned_path_share") or 0.0)
        opposite_path_share = float(metrics.get("entry_quality_opposite_path_share") or 0.0)
        last_move_share = float(metrics.get("entry_quality_last_move_share") or 0.0)
        reversal_share = float(metrics.get("entry_quality_reversal_share") or 0.0)
        last_move_in_signal_direction = bool(metrics.get("entry_quality_last_move_in_signal_direction"))

        if family == "trend":
            if path_efficiency < self._TREND_ENTRY_EFFICIENCY_SOFT:
                penalty += min(1.0, (self._TREND_ENTRY_EFFICIENCY_SOFT - path_efficiency) / 0.22) * 0.18
                reasons.append("choppy_path")
            if aligned_path_share < self._TREND_ENTRY_ALIGNMENT_SOFT:
                penalty += min(1.0, (self._TREND_ENTRY_ALIGNMENT_SOFT - aligned_path_share) / 0.25) * 0.12
                reasons.append("directional_alignment_weak")
            if last_move_share > self._TREND_ENTRY_LAST_MOVE_SHARE_SOFT:
                penalty += min(1.0, (last_move_share - self._TREND_ENTRY_LAST_MOVE_SHARE_SOFT) / 0.20) * 0.10
                reasons.append("late_impulse_dominance")
            penalty = min(0.35, penalty)
        elif family == "mean_reversion":
            if not last_move_in_signal_direction:
                penalty += 0.10
                reasons.append("latest_bar_not_reversing")
            if reversal_share < self._MEAN_REVERSION_REVERSAL_SHARE_SOFT:
                penalty += (
                    min(1.0, (self._MEAN_REVERSION_REVERSAL_SHARE_SOFT - reversal_share) / 0.18) * 0.10
                )
                reasons.append("reversal_share_too_small")
            if (
                opposite_path_share > self._MEAN_REVERSION_OPPOSITE_SHARE_HARD
                and last_move_share > self._MEAN_REVERSION_LAST_MOVE_SHARE_HARD
            ):
                penalty += 0.10
                reasons.append("runaway_extension")
            penalty = min(0.30, penalty)

        status = "penalized" if penalty > 0.0 else "ok"
        return penalty, reasons, status

    def _finalize_entry_signal(self, signal: Signal, *, prices: Sequence[float]) -> Signal:
        if signal.side == Side.HOLD:
            return signal

        payload = dict(signal.metadata or {})
        confidence_before = self._clamp01(float(signal.confidence))
        family = self._entry_quality_family(payload)
        payload["entry_quality_family"] = family

        metrics = self._entry_quality_metrics(prices, signal.side)
        if metrics is None or family == "neutral":
            payload["entry_quality_status"] = "unavailable" if metrics is None else "not_applicable"
            payload["entry_quality_penalty"] = 0.0
            payload["entry_quality_confidence_before"] = confidence_before
            payload["entry_quality_confidence_after"] = confidence_before
            if metrics is not None:
                payload.update(metrics)
            return Signal(
                side=signal.side,
                confidence=confidence_before,
                stop_loss_pips=float(signal.stop_loss_pips),
                take_profit_pips=float(signal.take_profit_pips),
                metadata=payload,
            )

        penalty, reasons, status = self._entry_quality_adjustment(family, metrics)
        confidence_after = self._clamp01(confidence_before - penalty)
        payload.update(metrics)
        payload["entry_quality_status"] = status
        payload["entry_quality_penalty"] = penalty
        payload["entry_quality_confidence_before"] = confidence_before
        payload["entry_quality_confidence_after"] = confidence_after
        if reasons:
            payload["entry_quality_reasons"] = reasons

        return Signal(
            side=signal.side,
            confidence=confidence_after,
            stop_loss_pips=float(signal.stop_loss_pips),
            take_profit_pips=float(signal.take_profit_pips),
            metadata=payload,
        )

    @staticmethod
    def _pip_size(symbol: str) -> float:
        return symbol_pip_size_fallback(
            symbol,
            index_pip_size=0.1,
            energy_pip_size=0.1,
        )

    def set_runtime_symbol_pip_size(self, symbol: str, pip_size: float) -> None:
        try:
            normalized = normalize_symbol(symbol)
            value = float(pip_size)
        except (TypeError, ValueError):
            return
        if not normalized or value <= 0:
            return
        self._runtime_symbol_pip_sizes[normalized] = value

    def _runtime_symbol_pip_size(self, symbol: str) -> float | None:
        return self._runtime_symbol_pip_sizes.get(normalize_symbol(symbol))

    @staticmethod
    def _context_pip_size(ctx: StrategyContext) -> tuple[float | None, str]:
        if ctx.pip_size not in (None, 0.0):
            return float(ctx.pip_size), "context_pip_size"
        if ctx.tick_size not in (None, 0.0):
            normalized = normalize_tick_size_to_pip_size(
                ctx.symbol,
                float(ctx.tick_size),
                index_pip_size=0.1,
                energy_pip_size=0.1,
            )
            if normalized is not None:
                return normalized, "context_tick_size"
        return None, ""

    def _has_context_pip_size(self, ctx: StrategyContext) -> bool:
        value, _ = self._context_pip_size(ctx)
        return value is not None

    def _resolve_context_pip_size(self, ctx: StrategyContext) -> tuple[float, str]:
        context_pip_size, context_source = self._context_pip_size(ctx)
        if context_pip_size is not None:
            return context_pip_size, context_source
        runtime_pip_size = self._runtime_symbol_pip_size(ctx.symbol)
        if runtime_pip_size is not None and runtime_pip_size > 0:
            return float(runtime_pip_size), "db_symbol_map"
        return self._pip_size(ctx.symbol), "fallback_symbol_map"
