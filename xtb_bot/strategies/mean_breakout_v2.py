from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections.abc import Sequence
from datetime import datetime, timezone
import math
import time

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema, mean_std
from xtb_bot.pip_size import is_fx_symbol as _shared_is_fx_symbol
from xtb_bot.symbols import is_index_symbol
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanBreakoutStrategyV2(Strategy):
    name = "mean_breakout_v2"
    _CONFIDENCE_QUANTIZATION_STEP = 0.05
    _SECONDARY_VOLUME_PENALTY = 0.10
    _SECONDARY_VOLUME_UNAVAILABLE_PENALTY = 0.12

    @staticmethod
    def _to_bool(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, str):
            normalized = value.strip().lower()
            if not normalized:
                return default
            return normalized not in {"0", "false", "no", "off"}
        return bool(value)

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.z_window = max(5, int(params.get("mb_zscore_window", 50)))
        self.br_window = max(2, int(params.get("mb_breakout_window", 20)))
        self.slope_window = max(2, int(params.get("mb_slope_window", 7)))

        self.z_threshold = float(params.get("mb_zscore_threshold", 2.0))
        self.min_slope_ratio = float(params.get("mb_min_slope_ratio", 0.0006))
        self.zscore_entry_mode = str(params.get("mb_zscore_entry_mode", "max_abs")).strip().lower()
        if self.zscore_entry_mode not in {"max_abs", "directional_extreme", "off"}:
            self.zscore_entry_mode = "max_abs"
        self.directional_extreme_threshold_ratio = max(
            1e-6,
            float(params.get("mb_directional_extreme_threshold_ratio", 0.85)),
        )
        self.max_spread_pips = max(0.0, float(params.get("mb_max_spread_pips", 0.0)))
        self.breakout_min_buffer_pips = max(0.0, float(params.get("mb_breakout_min_buffer_pips", 0.0)))
        self.breakout_min_buffer_atr_ratio = max(
            0.0,
            float(params.get("mb_breakout_min_buffer_atr_ratio", 0.10)),
        )
        self.breakout_min_buffer_spread_multiplier = max(
            0.0,
            float(params.get("mb_breakout_min_buffer_spread_multiplier", 2.0)),
        )
        self.exhaustion_sprint_lookback_bars = max(
            1,
            int(params.get("mb_exhaustion_sprint_lookback_bars", 3)),
        )
        self.exhaustion_sprint_move_channel_ratio = max(
            0.0,
            float(params.get("mb_exhaustion_sprint_move_channel_ratio", 0.8)),
        )
        self.breakout_invalidation_enabled = self._to_bool(
            params.get("mb_breakout_invalidation_enabled"),
            True,
        )
        self.breakout_invalidation_lookback_bars = max(
            1,
            int(params.get("mb_breakout_invalidation_lookback_bars", 1)),
        )
        self.exit_z_level = float(params.get("mb_exit_z_level", 0.5))

        self.base_sl = float(params.get("mb_stop_loss_pips", 40.0))
        self.base_tp = float(params.get("mb_take_profit_pips", 100.0))
        self.rr_ratio = max(1.0, float(params.get("mb_risk_reward_ratio", 2.5)))
        self.min_relative_stop_pct = max(0.0, float(params.get("mb_min_relative_stop_pct", 0.0008)))
        self.dynamic_tp_only = self._to_bool(params.get("mb_dynamic_tp_only"), True)
        self.dynamic_tp_respect_static_floor = self._to_bool(
            params.get("mb_dynamic_tp_respect_static_floor"),
            True,
        )
        self.adaptive_sl_max_atr_ratio = max(
            0.0,
            float(params.get("mb_adaptive_sl_max_atr_ratio", 0.0)),
        )

        default_atr_window = min(14, self.z_window)
        self.atr_window = max(2, int(params.get("mb_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("mb_atr_multiplier", 1.5)))
        self.min_stop_loss_pips = max(0.1, float(params.get("mb_min_stop_loss_pips", self.base_sl)))
        self.min_take_profit_pips = max(0.1, float(params.get("mb_min_take_profit_pips", self.base_tp)))

        self.default_timeframe_sec = float(params.get("mb_timeframe_sec", 60.0))
        if self.default_timeframe_sec <= 0:
            self.default_timeframe_sec = 60.0
        self.candle_timeframe_sec = max(0, int(float(params.get("mb_candle_timeframe_sec", self.default_timeframe_sec))))
        self.adaptive_timeframe_mode = str(params.get("mb_adaptive_timeframe_mode", "breakout_horizon")).strip().lower()
        if self.adaptive_timeframe_mode not in {"signal", "breakout_horizon"}:
            self.adaptive_timeframe_mode = "breakout_horizon"
        self.m5_max_sec = max(1.0, float(params.get("mb_m5_max_sec", 300.0)))
        self.m15_max_sec = max(self.m5_max_sec, float(params.get("mb_m15_max_sec", 900.0)))
        self.resample_mode = str(params.get("mb_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        self.ignore_sunday_candles = self._to_bool(params.get("mb_ignore_sunday_candles"), True)
        self.require_context_tick_size_for_cfd = self._to_bool(
            params.get("mb_require_context_tick_size_for_cfd"), True
        )

        self.sl_mult_m5 = max(0.1, float(params.get("mb_sl_mult_m5", 1.0)))
        self.tp_mult_m5 = max(0.1, float(params.get("mb_tp_mult_m5", 1.0)))
        self.sl_mult_m15 = max(0.1, float(params.get("mb_sl_mult_m15", 1.5)))
        self.tp_mult_m15 = max(0.1, float(params.get("mb_tp_mult_m15", 1.8)))
        self.sl_mult_h1 = max(0.1, float(params.get("mb_sl_mult_h1", 3.0)))
        self.tp_mult_h1 = max(0.1, float(params.get("mb_tp_mult_h1", 4.0)))
        self.trailing_enabled = self._to_bool(params.get("mb_trailing_enabled"), True)
        self.trailing_atr_multiplier = max(0.1, float(params.get("mb_trailing_atr_multiplier", 1.7)))
        self.trailing_activation_stop_ratio = max(
            0.0,
            float(params.get("mb_trailing_activation_stop_ratio", 1.0)),
        )
        self.trailing_activation_max_tp_ratio = min(
            1.0,
            max(0.0, float(params.get("mb_trailing_activation_max_tp_ratio", 0.35))),
        )
        self.trailing_distance_min_stop_ratio = max(
            0.0,
            float(params.get("mb_trailing_distance_min_stop_ratio", 0.45)),
        )
        self.trailing_min_activation_pips = max(
            0.0,
            float(params.get("mb_trailing_min_activation_pips", 0.0)),
        )
        self.volume_confirmation = self._to_bool(params.get("mb_volume_confirmation"), False)
        self.volume_window = max(2, int(params.get("mb_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("mb_min_volume_ratio", 1.5)))
        self.volume_min_ratio_for_entry = max(
            0.0,
            float(params.get("mb_volume_min_ratio_for_entry", 1.0)),
        )
        self.volume_min_samples = max(1, int(params.get("mb_volume_min_samples", 8)))
        self.volume_allow_missing = self._to_bool(params.get("mb_volume_allow_missing"), True)
        self.volume_require_spike = self._to_bool(params.get("mb_volume_require_spike"), False)
        self.volume_confidence_boost = max(0.0, float(params.get("mb_volume_confidence_boost", 0.1)))
        self.same_side_reentry_win_cooldown_sec = max(
            0.0,
            float(
                params.get(
                    "mean_breakout_v2_same_side_reentry_win_cooldown_sec",
                    params.get("same_side_reentry_win_cooldown_sec", 0.0),
                )
            ),
        )
        self.same_side_reentry_reset_on_opposite_signal = self._to_bool(
            params.get(
                "mean_breakout_v2_same_side_reentry_reset_on_opposite_signal",
                params.get("same_side_reentry_reset_on_opposite_signal"),
            ),
            True,
        )
        self.confidence_base = max(0.0, min(1.0, float(params.get("mb_confidence_base", 0.2))))
        self.confidence_gate_margin_weight = max(
            0.0,
            float(params.get("mb_confidence_gate_margin_weight", 0.25)),
        )
        self.confidence_cost_weight = max(
            0.0,
            float(params.get("mb_confidence_cost_weight", 0.2)),
        )
        self.confidence_extension_weight = max(
            0.0,
            float(params.get("mb_confidence_extension_weight", 0.2)),
        )
        self.confidence_gate_margin_norm_ratio = max(
            0.1,
            float(params.get("mb_confidence_gate_margin_norm_ratio", 0.6)),
        )
        self.confidence_cost_spread_to_sl_target = max(
            1e-6,
            float(params.get("mb_confidence_cost_spread_to_sl_target", 0.18)),
        )
        self.confidence_extension_soft_ratio = max(
            0.0,
            float(params.get("mb_confidence_extension_soft_ratio", 0.2)),
        )
        self.confidence_extension_hard_ratio = max(
            self.confidence_extension_soft_ratio + 1e-6,
            float(params.get("mb_confidence_extension_hard_ratio", 0.8)),
        )
        trailing_be_raw = params.get("mb_trailing_breakeven_offset_pips")
        if trailing_be_raw is None:
            self.trailing_breakeven_offset_pips: float | None = None
        else:
            self.trailing_breakeven_offset_pips = max(0.0, float(trailing_be_raw))

        self._local_same_side_reentry_block_side: dict[str, Side | None] = {}
        self._local_same_side_reentry_block_until_ts: dict[str, float] = {}

        self.stop_loss_pips = self.base_sl
        self.take_profit_pips = max(self.base_tp, self.base_sl * self.rr_ratio)
        self.min_history = max(self.z_window + self.slope_window + 1, self.br_window + 2, self.atr_window + 2)
        self._last_invalid_prices_dropped = 0

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _ema_series(values: Sequence[float], window: int) -> list[float]:
        if not values:
            return []
        if window <= 1:
            return [float(value) for value in values]
        alpha = 2.0 / (window + 1.0)
        current = float(values[0])
        series = [current]
        for value in values[1:]:
            current = (alpha * float(value)) + ((1.0 - alpha) * current)
            series.append(current)
        return series

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _is_fx_symbol(symbol: str) -> bool:
        return _shared_is_fx_symbol(symbol)

    @staticmethod
    def _infer_timeframe_sec(timestamps: Sequence[float]) -> float | None:
        if len(timestamps) < 2:
            return None
        deltas = [
            float(timestamps[idx]) - float(timestamps[idx - 1])
            for idx in range(1, len(timestamps))
        ]
        positive = sorted(delta for delta in deltas if delta > 0)
        if not positive:
            return None
        return positive[len(positive) // 2]

    def _resolve_timeframe_sec(self, ctx: StrategyContext) -> float:
        inferred = self._infer_timeframe_sec(ctx.timestamps)
        if inferred is not None and inferred > 0:
            return inferred
        return self.default_timeframe_sec

    def _is_candle_like_spacing(self, timestamps_sec: Sequence[float]) -> bool:
        if len(timestamps_sec) < 3:
            return False
        deltas = sorted(
            timestamps_sec[idx] - timestamps_sec[idx - 1]
            for idx in range(1, len(timestamps_sec))
            if (timestamps_sec[idx] - timestamps_sec[idx - 1]) > 0
        )
        if not deltas:
            return False
        timeframe = max(1.0, float(self.candle_timeframe_sec or self.default_timeframe_sec))
        median_delta = deltas[len(deltas) // 2]
        if median_delta < timeframe * 0.8:
            return False
        # Keep auto mode strict: treat input as candle-like only when cadence is
        # close to configured candle timeframe. This avoids 10-15s snapshots
        # being mistaken for candles and shrinking effective strategy horizons.
        return median_delta <= timeframe * 1.25

    def _resample_closed_candle_closes(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [float(value) for value in prices], False
        if not timestamps or len(timestamps) != len(prices):
            return [float(value) for value in prices], False

        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and not self._is_candle_like_spacing(normalized_timestamps):
            return [float(value) for value in prices], False

        closes: list[float] = []
        last_bucket: int | None = None
        timeframe = max(1, self.candle_timeframe_sec)
        for raw_price, ts in zip(prices, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            price = float(raw_price)
            if last_bucket is None or bucket != last_bucket:
                closes.append(price)
                last_bucket = bucket
            else:
                closes[-1] = price

        # Always exclude the current candle in resample mode. In auto mode,
        # candle-like feeds may still include a forming last bar.
        if len(closes) < 2:
            return [], True
        return closes[:-1], True

    def _get_adaptive_params(self, timeframe_sec: float) -> tuple[float, float, str]:
        if timeframe_sec <= self.m5_max_sec:
            return self.sl_mult_m5, self.tp_mult_m5, "M5_or_lower"
        if timeframe_sec <= self.m15_max_sec:
            return self.sl_mult_m15, self.tp_mult_m15, "M15"
        return self.sl_mult_h1, self.tp_mult_h1, "H1_plus"

    def _resolve_adaptive_timeframe_sec(self, signal_timeframe_sec: float) -> float:
        base_timeframe = max(1.0, float(signal_timeframe_sec))
        if self.adaptive_timeframe_mode == "signal":
            return base_timeframe
        # Use breakout channel horizon to select risk multipliers:
        # even on M1 bars, a 15-bar breakout works on ~M15 structure.
        return base_timeframe * float(max(1, self.br_window))

    def _calculate_trailing_params(
        self,
        side: Side,
        last_price: float,
        atr_pips: float,
        initial_stop_pips: float,
        take_profit_pips: float,
    ) -> dict[str, object] | None:
        _ = side
        _ = last_price
        if not self.trailing_enabled:
            return None
        trailing_distance_from_atr_pips = max(0.1, atr_pips * self.trailing_atr_multiplier)
        trailing_distance_from_stop_floor_pips = max(0.0, initial_stop_pips * self.trailing_distance_min_stop_ratio)
        trailing_distance_pips = max(
            0.1,
            trailing_distance_from_atr_pips,
            trailing_distance_from_stop_floor_pips,
        )
        trailing_activation_from_stop_pips = max(
            self.trailing_min_activation_pips,
            initial_stop_pips * self.trailing_activation_stop_ratio,
        )
        trailing_activation_cap_tp_pips: float | None = None
        trailing_activation_pips = trailing_activation_from_stop_pips
        if self.trailing_activation_max_tp_ratio > 0.0 and take_profit_pips > 0.0:
            trailing_activation_cap_tp_pips = take_profit_pips * self.trailing_activation_max_tp_ratio
            trailing_activation_pips = min(trailing_activation_pips, trailing_activation_cap_tp_pips)
        trailing_activation_ratio = (
            max(0.0, min(1.0, trailing_activation_pips / max(take_profit_pips, FLOAT_COMPARISON_TOLERANCE)))
            if take_profit_pips > 0.0
            else 0.0
        )
        payload: dict[str, object] = {
            "trailing_enabled": True,
            "trailing_distance_pips": trailing_distance_pips,
            "trailing_activation_pips": trailing_activation_pips,
            "trailing_activation_ratio": trailing_activation_ratio,
            "trailing_distance_from_atr_pips": trailing_distance_from_atr_pips,
            "trailing_distance_from_stop_floor_pips": trailing_distance_from_stop_floor_pips,
            "trailing_distance_min_stop_ratio": self.trailing_distance_min_stop_ratio,
            "trailing_activation_from_stop_pips": trailing_activation_from_stop_pips,
            "trailing_activation_cap_tp_pips": trailing_activation_cap_tp_pips,
            "trailing_activation_max_tp_ratio": self.trailing_activation_max_tp_ratio,
        }
        if self.trailing_breakeven_offset_pips is not None:
            payload["trailing_breakeven_offset_pips"] = self.trailing_breakeven_offset_pips
        return payload

    @staticmethod
    def _extract_finite_positive_volumes(raw_values: Sequence[float]) -> list[float]:
        values: list[float] = []
        for raw in raw_values:
            try:
                volume = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(volume) or volume <= 0.0:
                continue
            values.append(volume)
        return values

    @staticmethod
    def _volume_values_close(left: float, right: float) -> bool:
        tolerance = max(FLOAT_COMPARISON_TOLERANCE, max(abs(left), abs(right)) * 1e-6)
        return abs(left - right) <= tolerance

    @staticmethod
    def _normalize_volume(raw: float) -> float:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(value) or value <= 0.0:
            return 0.0
        return value

    def _resample_closed_candle_volumes(
        self,
        volumes: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [self._normalize_volume(value) for value in volumes], False
        if not timestamps or len(timestamps) != len(volumes):
            return [self._normalize_volume(value) for value in volumes], False

        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and not self._is_candle_like_spacing(normalized_timestamps):
            return [self._normalize_volume(value) for value in volumes], False

        candle_volumes: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        bucket_volume = 0.0
        for raw_volume, ts in zip(volumes, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            volume_value = self._normalize_volume(raw_volume)
            if last_bucket is None:
                last_bucket = bucket
                bucket_volume = volume_value
                continue
            if bucket != last_bucket:
                candle_volumes.append(bucket_volume)
                last_bucket = bucket
                bucket_volume = volume_value
            else:
                bucket_volume += volume_value

        if last_bucket is None:
            return [], True
        candle_volumes.append(bucket_volume)

        # Keep volume samples aligned with closed-candle price series.
        if len(candle_volumes) < 2:
            return [], True
        return candle_volumes[:-1], True

    def _evaluate_volume_confirmation(
        self,
        ctx: StrategyContext,
        *,
        samples: Sequence[float] | None = None,
        samples_aligned: bool = False,
        compare_closed_only: bool = False,
    ) -> tuple[dict[str, object], str | None, float]:
        index_missing_volume_relaxed = is_index_symbol(ctx.symbol)
        effective_volume_allow_missing = self.volume_allow_missing or index_missing_volume_relaxed
        metadata: dict[str, object] = {
            "volume_confirmation_enabled": self.volume_confirmation,
            "volume_window": self.volume_window,
            "min_volume_ratio": self.min_volume_ratio,
            "volume_min_ratio_for_entry": self.volume_min_ratio_for_entry,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_allow_missing_effective": effective_volume_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not self.volume_allow_missing,
            "volume_require_spike": self.volume_require_spike,
            "volume_confidence_boost_config": self.volume_confidence_boost,
            "volume_effective_required_ratio": None,
            "volume_confirmed": False,
            "volume_samples_aligned": samples_aligned,
            "volume_compare_closed_only": compare_closed_only,
            "volume_current_source": "unavailable",
            "volume_current_deduplicated": False,
            "volume_data_available": False,
            "volume_spike": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
            "volume_baseline_history_len": 0,
        }
        if not self.volume_confirmation:
            return metadata, None, 0.0

        if samples is None:
            raw_samples = self._extract_finite_positive_volumes(ctx.volumes[-(self.volume_window + 6) :])
        else:
            raw_samples = self._extract_finite_positive_volumes(samples[-(self.volume_window + 6) :])
        current_volume = self._coerce_positive_finite(ctx.current_volume)

        if compare_closed_only:
            if raw_samples:
                metadata["volume_current_source"] = "aligned_samples_last" if samples_aligned else "ctx_volumes_last"
        else:
            if current_volume is not None:
                if raw_samples and self._volume_values_close(raw_samples[-1], current_volume):
                    metadata["volume_current_source"] = "aligned_samples_last" if samples_aligned else "ctx_volumes_last"
                    metadata["volume_current_deduplicated"] = True
                else:
                    raw_samples.append(current_volume)
                    metadata["volume_current_source"] = "context_current_volume"
            elif raw_samples:
                metadata["volume_current_source"] = "aligned_samples_last" if samples_aligned else "ctx_volumes_last"

        if len(raw_samples) < 2:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = raw_samples[-1]
        history = raw_samples[:-1]
        if len(history) > self.volume_window:
            history = history[-self.volume_window :]
        metadata["volume_baseline_history_len"] = len(history)
        if len(history) < self.volume_min_samples:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        avg_volume = sum(history) / max(len(history), 1)
        if avg_volume <= 0.0:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        volume_ratio = current / avg_volume
        spike = volume_ratio >= self.min_volume_ratio
        required_ratio = self.min_volume_ratio if self.volume_require_spike else self.volume_min_ratio_for_entry
        confirmed = volume_ratio >= required_ratio
        metadata.update(
            {
                "volume_data_available": True,
                "volume_spike": spike,
                "volume_confirmed": confirmed,
                "volume_effective_required_ratio": required_ratio,
                "volume_current": current,
                "volume_avg": avg_volume,
                "volume_ratio": volume_ratio,
            }
        )
        if not confirmed:
            return metadata, "volume_not_confirmed", 0.0
        return metadata, None, self.volume_confidence_boost if spike else 0.0

    def _breakout_buffer_price(
        self,
        *,
        atr: float,
        pip_size: float,
        spread_pips: float | None,
    ) -> tuple[float, float, float, float]:
        atr_component = max(0.0, atr * self.breakout_min_buffer_atr_ratio)
        pip_component = max(0.0, self.breakout_min_buffer_pips * pip_size)
        spread_component = 0.0
        if spread_pips is not None and spread_pips > 0.0:
            spread_component = max(0.0, spread_pips * pip_size * self.breakout_min_buffer_spread_multiplier)
        effective = max(atr_component, pip_component, spread_component)
        return effective, atr_component, pip_component, spread_component

    def _zscore_indicator(self, prices: Sequence[float]) -> float:
        # Use the prior window as z-score baseline so breakout bar does not dilute itself
        # by inflating mean/std in the same calculation step.
        z_baseline = prices[-(self.z_window + 1) : -1]
        if len(z_baseline) < self.z_window:
            z_baseline = prices[-self.z_window :]
        current_ema = self._ema(prices[-self.z_window :], self.z_window)

        if len(z_baseline) >= 2:
            baseline_ema = self._ema(z_baseline, self.z_window)
            residuals = [float(price) - baseline_ema for price in z_baseline]
            _, std = mean_std(residuals)
        else:
            baseline_ema = current_ema
            std = 0.0
        return (float(prices[-1]) - baseline_ema) / std if std > 0 else 0.0

    def _slope_metrics(self, prices: Sequence[float]) -> tuple[float, dict[str, object]]:
        ema_series = self._ema_series(prices, self.z_window)
        if len(ema_series) < 2:
            return 0.0, {
                "slope_method": "ema_regression",
                "slope_effective_window": 0,
                "slope_endpoint_ratio": 0.0,
                "slope_directional_bar_share": 0.0,
            }

        base_lookback = max(2, int(self.slope_window))
        effective_lookback = min(len(ema_series) - 1, base_lookback + 2)
        segment = ema_series[-(effective_lookback + 1) :]
        endpoint_base = segment[0]
        endpoint_ratio = (
            (segment[-1] - endpoint_base) / endpoint_base if abs(endpoint_base) > FLOAT_ROUNDING_TOLERANCE else 0.0
        )

        directional_moves = 0
        total_moves = max(len(segment) - 1, 1)
        for prev_value, current_value in zip(segment, segment[1:]):
            if current_value > prev_value:
                directional_moves += 1
            elif current_value < prev_value:
                directional_moves -= 1
        directional_bar_share = abs(directional_moves) / total_moves

        points = len(segment)
        x_mean = (points - 1) / 2.0
        y_mean = sum(segment) / points
        denominator = sum((idx - x_mean) ** 2 for idx in range(points))
        numerator = sum((idx - x_mean) * (value - y_mean) for idx, value in enumerate(segment))
        regression_per_bar = numerator / denominator if denominator > 0.0 else 0.0
        normalization = sum(abs(value) for value in segment) / points
        slope = regression_per_bar / normalization if normalization > FLOAT_ROUNDING_TOLERANCE else 0.0

        return slope, {
            "slope_method": "ema_regression",
            "slope_effective_window": effective_lookback,
            "slope_endpoint_ratio": endpoint_ratio,
            "slope_directional_bar_share": directional_bar_share,
        }

    def _get_indicators(self, prices: Sequence[float]) -> tuple[float, float]:
        zscore = self._zscore_indicator(prices)
        slope, _metadata = self._slope_metrics(prices)
        return zscore, slope

    def _effective_zscore_threshold(self) -> float:
        if self.zscore_entry_mode == "directional_extreme":
            return self.z_threshold * self.directional_extreme_threshold_ratio
        return self.z_threshold

    def _cost_score(self, spread_to_sl_ratio: float | None) -> float:
        if spread_to_sl_ratio is None:
            return 0.6
        normalized_cost = max(0.0, spread_to_sl_ratio / self.confidence_cost_spread_to_sl_target)
        return self._clamp01(1.0 / (1.0 + (normalized_cost * normalized_cost)))

    def _quantize_confidence(self, value: float) -> float:
        clamped = self._clamp01(value)
        step = self._CONFIDENCE_QUANTIZATION_STEP
        if step <= 0.0:
            return clamped
        if clamped >= 1.0:
            return 1.0
        buckets = math.floor((clamped + FLOAT_ROUNDING_TOLERANCE) / step)
        return self._clamp01(buckets * step)

    def _breakout_exhaustion_guard(
        self,
        *,
        prices: Sequence[float],
        side: Side,
        zscore: float,
        effective_threshold: float,
        breakout_to_sl_ratio: float,
        channel_upper: float,
        channel_lower: float,
        entry_quality_metrics: dict[str, object] | None,
    ) -> tuple[bool, dict[str, object]]:
        zscore_gate = max(abs(effective_threshold), abs(self.z_threshold), FLOAT_COMPARISON_TOLERANCE)
        zscore_ratio = abs(zscore) / zscore_gate
        extension_threshold = max(0.35, self.confidence_extension_soft_ratio + 0.15)
        last_move_share = (
            float(entry_quality_metrics.get("entry_quality_last_move_share") or 0.0)
            if entry_quality_metrics is not None
            else 0.0
        )
        path_efficiency = (
            float(entry_quality_metrics.get("entry_quality_path_efficiency") or 0.0)
            if entry_quality_metrics is not None
            else 0.0
        )
        last_move_in_signal_direction = (
            bool(entry_quality_metrics.get("entry_quality_last_move_in_signal_direction"))
            if entry_quality_metrics is not None
            else False
        )
        sprint_meta = self._momentum_sprint_metrics(
            prices=prices,
            side=side,
            channel_upper=channel_upper,
            channel_lower=channel_lower,
        )
        sprint_detected = bool(sprint_meta["breakout_exhaustion_sprint_detected"])
        triggered = (
            side in {Side.BUY, Side.SELL}
            and self.zscore_entry_mode != "off"
            and zscore_ratio >= 1.35
            and breakout_to_sl_ratio >= extension_threshold
            and entry_quality_metrics is not None
            and last_move_in_signal_direction
            and last_move_share >= 0.55
            and path_efficiency >= 0.60
            and sprint_detected
        )
        metadata: dict[str, object] = {
            "breakout_exhaustion_guard_triggered": triggered,
            "breakout_exhaustion_zscore_ratio": zscore_ratio,
            "breakout_exhaustion_zscore_threshold_ratio": 1.35,
            "breakout_exhaustion_breakout_to_sl_ratio": breakout_to_sl_ratio,
            "breakout_exhaustion_breakout_to_sl_threshold": extension_threshold,
            "breakout_exhaustion_last_move_share": last_move_share,
            "breakout_exhaustion_last_move_share_threshold": 0.55,
            "breakout_exhaustion_path_efficiency": path_efficiency,
            "breakout_exhaustion_path_efficiency_threshold": 0.60,
            "breakout_exhaustion_last_move_in_signal_direction": last_move_in_signal_direction,
            **sprint_meta,
        }
        if entry_quality_metrics is not None:
            metadata.update(entry_quality_metrics)
        return triggered, metadata

    def _momentum_sprint_metrics(
        self,
        *,
        prices: Sequence[float],
        side: Side,
        channel_upper: float,
        channel_lower: float,
    ) -> dict[str, object]:
        lookback = max(1, min(self.exhaustion_sprint_lookback_bars, len(prices) - 1))
        channel_width = max(0.0, channel_upper - channel_lower)
        channel_width_safe = max(channel_width, FLOAT_COMPARISON_TOLERANCE)
        if side == Side.BUY:
            directional_move = max(0.0, float(prices[-1]) - float(prices[-(lookback + 1)]))
        elif side == Side.SELL:
            directional_move = max(0.0, float(prices[-(lookback + 1)]) - float(prices[-1]))
        else:
            directional_move = 0.0
        move_channel_ratio = directional_move / channel_width_safe
        sprint_detected = (
            side in {Side.BUY, Side.SELL}
            and channel_width > 0.0
            and move_channel_ratio >= self.exhaustion_sprint_move_channel_ratio
        )
        return {
            "breakout_exhaustion_sprint_detected": sprint_detected,
            "breakout_exhaustion_sprint_lookback_bars": lookback,
            "breakout_exhaustion_sprint_move_price": directional_move,
            "breakout_exhaustion_sprint_channel_width_price": channel_width,
            "breakout_exhaustion_sprint_move_channel_ratio": move_channel_ratio,
            "breakout_exhaustion_sprint_move_channel_threshold": self.exhaustion_sprint_move_channel_ratio,
        }

    def _signal_ts(self, raw_timestamps: Sequence[float]) -> float:
        if raw_timestamps:
            return self._timestamp_to_seconds(raw_timestamps[-1])
        return time.time()

    def _local_same_side_reentry_state(self, symbol: str, now_ts: float) -> dict[str, object]:
        block_side = self._local_same_side_reentry_block_side.get(symbol)
        remaining = max(0.0, self._local_same_side_reentry_block_until_ts.get(symbol, 0.0) - now_ts)
        if remaining <= 0.0:
            block_side = None
        return {
            "same_side_reentry_win_cooldown_sec": self.same_side_reentry_win_cooldown_sec,
            "same_side_reentry_reset_on_opposite_signal": self.same_side_reentry_reset_on_opposite_signal,
            "local_same_side_reentry_block_side": block_side.value if block_side is not None else None,
            "local_same_side_reentry_block_until_ts": (
                self._local_same_side_reentry_block_until_ts.get(symbol, 0.0) if block_side is not None else None
            ),
            "local_same_side_reentry_remaining_sec": remaining if block_side is not None else 0.0,
        }

    def _activate_local_same_side_reentry_cooldown(self, symbol: str, side: Side, now_ts: float) -> None:
        if side not in {Side.BUY, Side.SELL}:
            return
        if self.same_side_reentry_win_cooldown_sec <= 0.0:
            return
        self._local_same_side_reentry_block_side[symbol] = side
        self._local_same_side_reentry_block_until_ts[symbol] = max(
            self._local_same_side_reentry_block_until_ts.get(symbol, 0.0),
            now_ts + self.same_side_reentry_win_cooldown_sec,
        )

    def _local_same_side_reentry_allows_signal(self, symbol: str, signal_side: Side, now_ts: float) -> tuple[bool, dict[str, object]]:
        if self.same_side_reentry_win_cooldown_sec <= 0.0:
            return True, self._local_same_side_reentry_state(symbol, now_ts)
        blocked_side = self._local_same_side_reentry_block_side.get(symbol)
        if blocked_side is None:
            return True, self._local_same_side_reentry_state(symbol, now_ts)
        remaining = self._local_same_side_reentry_block_until_ts.get(symbol, 0.0) - now_ts
        if remaining <= 0.0:
            self._local_same_side_reentry_block_side.pop(symbol, None)
            self._local_same_side_reentry_block_until_ts.pop(symbol, None)
            return True, self._local_same_side_reentry_state(symbol, now_ts)
        if signal_side != blocked_side and self.same_side_reentry_reset_on_opposite_signal:
            self._local_same_side_reentry_block_side.pop(symbol, None)
            self._local_same_side_reentry_block_until_ts.pop(symbol, None)
            return True, self._local_same_side_reentry_state(symbol, now_ts)
        if signal_side == blocked_side:
            return False, self._local_same_side_reentry_state(symbol, now_ts)
        return True, self._local_same_side_reentry_state(symbol, now_ts)

    @staticmethod
    def _clamp01(value: float) -> float:
        return max(0.0, min(1.0, value))

    def _margin_score(self, ratio: float) -> float:
        margin = (ratio - 1.0) / max(self.confidence_gate_margin_norm_ratio, FLOAT_COMPARISON_TOLERANCE)
        return self._clamp01(margin)

    def _extension_score(self, breakout_to_sl_ratio: float) -> float:
        if breakout_to_sl_ratio <= self.confidence_extension_soft_ratio:
            return 1.0
        if breakout_to_sl_ratio >= self.confidence_extension_hard_ratio:
            return 0.0
        span = max(self.confidence_extension_hard_ratio - self.confidence_extension_soft_ratio, FLOAT_COMPARISON_TOLERANCE)
        return self._clamp01(1.0 - ((breakout_to_sl_ratio - self.confidence_extension_soft_ratio) / span))

    def _passes_zscore_filter(self, side: Side, zscore: float, effective_threshold: float) -> tuple[bool, str | None]:
        if self.zscore_entry_mode == "off":
            return True, None
        if self.zscore_entry_mode == "max_abs":
            if abs(zscore) >= effective_threshold:
                return True, None
            return False, "zscore_below_threshold"
        if side == Side.BUY:
            if zscore >= effective_threshold:
                return True, None
        elif side == Side.SELL:
            if zscore <= -effective_threshold:
                return True, None
        return False, "zscore_directional_not_confirmed"

    def _hold(
        self,
        reason: str,
        extra: dict[str, object] | None = None,
        *,
        stop_loss_pips: float | None = None,
        take_profit_pips: float | None = None,
    ) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "mean_breakout_v2"}
        if extra:
            payload.update(extra)
        if self._last_invalid_prices_dropped > 0 and "invalid_prices_dropped" not in payload:
            payload["invalid_prices_dropped"] = self._last_invalid_prices_dropped
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips if stop_loss_pips is None else float(stop_loss_pips),
            take_profit_pips=self.take_profit_pips if take_profit_pips is None else float(take_profit_pips),
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raw_prices, raw_timestamps, invalid_prices = self._extract_finite_prices_and_timestamps(
            ctx.prices,
            ctx.timestamps,
            timestamp_normalizer=self._timestamp_to_seconds,
        )
        self._last_invalid_prices_dropped = invalid_prices
        signal_ts = self._signal_ts(raw_timestamps)
        _symbol = str(ctx.symbol or "").strip().upper()
        local_reentry_state = self._local_same_side_reentry_state(_symbol, signal_ts)
        prices, using_closed_candles = self._resample_closed_candle_closes(raw_prices, raw_timestamps)
        raw_volumes = [self._normalize_volume(value) for value in ctx.volumes]
        volume_samples = list(raw_volumes)
        volume_samples_aligned = False
        volume_samples_using_closed_candles = False
        if raw_timestamps and len(raw_timestamps) == len(raw_volumes):
            volume_samples, volume_samples_using_closed_candles = self._resample_closed_candle_volumes(
                raw_volumes,
                raw_timestamps,
            )
            volume_samples_aligned = True
        volume_compare_closed_only = using_closed_candles and volume_samples_using_closed_candles
        if len(prices) < self.min_history:
            reason = "insufficient_candle_history" if using_closed_candles else "insufficient_price_history"
            return self._hold(
                reason,
                {
                    "history": len(prices),
                    "required_history": self.min_history,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "invalid_prices_dropped": invalid_prices,
                    "volume_samples_aligned": volume_samples_aligned,
                    "volume_samples_using_closed_candles": volume_samples_using_closed_candles,
                    "volume_compare_closed_only": volume_compare_closed_only,
                    **local_reentry_state,
                },
            )

        timeframe_sec = (
            float(self.candle_timeframe_sec)
            if using_closed_candles
            else (self._infer_timeframe_sec(raw_timestamps) or self.default_timeframe_sec)
        )
        adaptive_timeframe_sec = self._resolve_adaptive_timeframe_sec(timeframe_sec)
        sl_mult, tp_mult, timeframe_mode = self._get_adaptive_params(adaptive_timeframe_sec)
        static_sl = self.base_sl * sl_mult
        static_tp = self.base_tp * tp_mult
        adaptive_sl = max(self.min_stop_loss_pips, static_sl)
        adaptive_tp = max(self.min_take_profit_pips, static_tp, adaptive_sl * self.rr_ratio)

        atr = self._atr(prices, self.atr_window)
        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
        atr_for_buffer = float(atr) if atr is not None and atr > 0 else 0.0

        zscore = self._zscore_indicator(prices)
        slope, slope_meta = self._slope_metrics(prices)
        zscore_effective_threshold = self._effective_zscore_threshold()
        slope_payload = {
            "slope_method": slope_meta.get("slope_method"),
            "slope_effective_window": slope_meta.get("slope_effective_window"),
            "slope_endpoint_ratio": slope_meta.get("slope_endpoint_ratio"),
            "slope_directional_bar_share": slope_meta.get("slope_directional_bar_share"),
        }

        reference = prices[-(self.br_window + 1) : -1]
        upper = max(reference)
        lower = min(reference)
        last = prices[-1]
        prev = prices[-2]

        breakout_up_raw = last > upper and prev <= upper
        breakout_down_raw = last < lower and prev >= lower
        breakout_distance_up_price = max(0.0, float(last - upper))
        breakout_distance_down_price = max(0.0, float(lower - last))
        breakout_buffer_price, breakout_buffer_atr_price, breakout_buffer_pip_price, breakout_buffer_spread_price = (
            self._breakout_buffer_price(
                atr=atr_for_buffer,
                pip_size=pip_size,
                spread_pips=ctx.current_spread_pips,
            )
        )
        breakout_buffer_pips = breakout_buffer_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        breakout_up = breakout_up_raw and breakout_distance_up_price >= breakout_buffer_price
        breakout_down = breakout_down_raw and breakout_distance_down_price >= breakout_buffer_price
        recent_breakout_up_raw = False
        recent_breakout_down_raw = False
        recent_breakout_up_buffered = False
        recent_breakout_down_buffered = False
        recent_breakout_up_lag: int | None = None
        recent_breakout_down_lag: int | None = None
        max_lag = max(0, min(self.breakout_invalidation_lookback_bars, len(prices) - self.br_window - 1))
        for lag in range(1, max_lag + 1):
            breakout_idx = len(prices) - 1 - lag
            prev_idx = breakout_idx - 1
            ref_start = breakout_idx - self.br_window
            if prev_idx < 0 or ref_start < 0:
                continue
            breakout_ref = prices[ref_start:breakout_idx]
            if len(breakout_ref) < self.br_window:
                continue
            breakout_upper = max(breakout_ref)
            breakout_lower = min(breakout_ref)
            breakout_close = prices[breakout_idx]
            breakout_prev = prices[prev_idx]
            lag_up_raw = breakout_close > breakout_upper and breakout_prev <= breakout_upper
            lag_down_raw = breakout_close < breakout_lower and breakout_prev >= breakout_lower
            lag_up_buffered = lag_up_raw and (breakout_close - breakout_upper) >= breakout_buffer_price
            lag_down_buffered = lag_down_raw and (breakout_lower - breakout_close) >= breakout_buffer_price
            if lag_up_raw and not recent_breakout_up_raw:
                recent_breakout_up_raw = True
                recent_breakout_up_lag = lag
            if lag_down_raw and not recent_breakout_down_raw:
                recent_breakout_down_raw = True
                recent_breakout_down_lag = lag
            if lag_up_buffered and not recent_breakout_up_buffered:
                recent_breakout_up_buffered = True
                if recent_breakout_up_lag is None:
                    recent_breakout_up_lag = lag
            if lag_down_buffered and not recent_breakout_down_buffered:
                recent_breakout_down_buffered = True
                if recent_breakout_down_lag is None:
                    recent_breakout_down_lag = lag
        breakout_invalidation_up = (
            self.breakout_invalidation_enabled
            and recent_breakout_up_buffered
            and last <= upper
        )
        breakout_invalidation_down = (
            self.breakout_invalidation_enabled
            and recent_breakout_down_buffered
            and last >= lower
        )
        breakout_invalidated = breakout_invalidation_up or breakout_invalidation_down

        side = Side.HOLD
        hold_reason = "no_signal"
        if breakout_up:
            if slope > self.min_slope_ratio:
                z_ok, z_reason = self._passes_zscore_filter(Side.BUY, zscore, zscore_effective_threshold)
                if z_ok:
                    side = Side.BUY
                else:
                    hold_reason = z_reason or "zscore_filter_blocked"
            else:
                hold_reason = "slope_filter_blocked"
        elif breakout_down:
            if slope < -self.min_slope_ratio:
                z_ok, z_reason = self._passes_zscore_filter(Side.SELL, zscore, zscore_effective_threshold)
                if z_ok:
                    side = Side.SELL
                else:
                    hold_reason = z_reason or "zscore_filter_blocked"
            else:
                hold_reason = "slope_filter_blocked"
        if side == Side.HOLD and breakout_invalidated:
            hold_reason = "breakout_invalidated"
            if breakout_invalidation_up:
                self._activate_local_same_side_reentry_cooldown(_symbol, Side.BUY, signal_ts)
            if breakout_invalidation_down:
                self._activate_local_same_side_reentry_cooldown(_symbol, Side.SELL, signal_ts)
            local_reentry_state = self._local_same_side_reentry_state(_symbol, signal_ts)

        if side in {Side.BUY, Side.SELL}:
            allows_open, local_reentry_state = self._local_same_side_reentry_allows_signal(_symbol, side, signal_ts)
            if not allows_open:
                return self._hold(
                    "same_side_reentry_cooldown_active",
                    {
                        "hold_reason": "same_side_reentry_cooldown_active",
                        "zscore": zscore,
                        "slope": slope,
                        "zscore_threshold": self.z_threshold,
                        "zscore_effective_threshold": zscore_effective_threshold,
                        "directional_extreme_threshold_ratio": self.directional_extreme_threshold_ratio,
                        "zscore_entry_mode": self.zscore_entry_mode,
                        "zscore_baseline_excludes_current_bar": True,
                        **slope_payload,
                        "min_slope_ratio": self.min_slope_ratio,
                        "max_spread_pips": self.max_spread_pips,
                        "spread_pips": ctx.current_spread_pips,
                        "breakout_buffer_pips": breakout_buffer_pips,
                        "breakout_buffer_price": breakout_buffer_price,
                        "breakout_buffer_atr_price": breakout_buffer_atr_price,
                        "breakout_buffer_pip_price": breakout_buffer_pip_price,
                        "breakout_buffer_spread_price": breakout_buffer_spread_price,
                        "breakout_buffer_atr_ratio": self.breakout_min_buffer_atr_ratio,
                        "breakout_buffer_pips_config": self.breakout_min_buffer_pips,
                        "breakout_buffer_spread_multiplier": self.breakout_min_buffer_spread_multiplier,
                        "breakout_up_raw": breakout_up_raw,
                        "breakout_down_raw": breakout_down_raw,
                        "breakout_up_distance_price": breakout_distance_up_price,
                        "breakout_down_distance_price": breakout_distance_down_price,
                        "breakout_up_distance_pips": breakout_distance_up_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                        "breakout_down_distance_pips": breakout_distance_down_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                        "timeframe_sec": timeframe_sec,
                        "adaptive_timeframe_sec": adaptive_timeframe_sec,
                        "adaptive_timeframe_mode": self.adaptive_timeframe_mode,
                        "timeframe_mode": timeframe_mode,
                        "candle_timeframe_sec": self.candle_timeframe_sec,
                        "using_closed_candles": using_closed_candles,
                        "resample_mode": self.resample_mode,
                        "ignore_sunday_candles": self.ignore_sunday_candles,
                        **local_reentry_state,
                    },
                    stop_loss_pips=adaptive_sl,
                    take_profit_pips=adaptive_tp,
                )

        if side == Side.HOLD:
            exit_hint = (
                "close_on_breakout_invalidation"
                if breakout_invalidated
                else ("momentum_exhaustion" if abs(zscore) < self.exit_z_level else None)
            )
            return self._hold(
                hold_reason,
                {
                    "hold_reason": hold_reason,
                    "zscore": zscore,
                    "slope": slope,
                    "zscore_threshold": self.z_threshold,
                    "zscore_effective_threshold": zscore_effective_threshold,
                    "directional_extreme_threshold_ratio": self.directional_extreme_threshold_ratio,
                    "zscore_entry_mode": self.zscore_entry_mode,
                    "zscore_baseline_excludes_current_bar": True,
                    **slope_payload,
                    "min_slope_ratio": self.min_slope_ratio,
                    "max_spread_pips": self.max_spread_pips,
                    "spread_pips": ctx.current_spread_pips,
                    "breakout_buffer_pips": breakout_buffer_pips,
                    "breakout_buffer_price": breakout_buffer_price,
                    "breakout_buffer_atr_price": breakout_buffer_atr_price,
                    "breakout_buffer_pip_price": breakout_buffer_pip_price,
                    "breakout_buffer_spread_price": breakout_buffer_spread_price,
                    "breakout_buffer_atr_ratio": self.breakout_min_buffer_atr_ratio,
                    "breakout_buffer_pips_config": self.breakout_min_buffer_pips,
                    "breakout_buffer_spread_multiplier": self.breakout_min_buffer_spread_multiplier,
                    "breakout_up_raw": breakout_up_raw,
                    "breakout_down_raw": breakout_down_raw,
                    "breakout_up_distance_price": breakout_distance_up_price,
                    "breakout_down_distance_price": breakout_distance_down_price,
                    "breakout_up_distance_pips": breakout_distance_up_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                    "breakout_down_distance_pips": breakout_distance_down_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                    "exit_z_level": self.exit_z_level,
                    "exit_hint": exit_hint,
                    "timeframe_sec": timeframe_sec,
                    "adaptive_timeframe_sec": adaptive_timeframe_sec,
                    "adaptive_timeframe_mode": self.adaptive_timeframe_mode,
                    "timeframe_mode": timeframe_mode,
                    "sl_multiplier": sl_mult,
                    "tp_multiplier": tp_mult,
                    "sl_applied": adaptive_sl,
                    "tp_applied": adaptive_tp,
                    "atr_window": self.atr_window,
                    "atr_pips": None,
                    "atr_multiplier": self.atr_multiplier,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "breakout_up": breakout_up,
                    "breakout_down": breakout_down,
                    "breakout_invalidation_enabled": self.breakout_invalidation_enabled,
                    "breakout_invalidation_lookback_bars": self.breakout_invalidation_lookback_bars,
                    "recent_breakout_up_raw": recent_breakout_up_raw,
                    "recent_breakout_down_raw": recent_breakout_down_raw,
                    "recent_breakout_up_buffered": recent_breakout_up_buffered,
                    "recent_breakout_down_buffered": recent_breakout_down_buffered,
                    "recent_breakout_up_lag": recent_breakout_up_lag,
                    "recent_breakout_down_lag": recent_breakout_down_lag,
                    "breakout_invalidation_up": breakout_invalidation_up,
                    "breakout_invalidation_down": breakout_invalidation_down,
                    "breakout_invalidated": breakout_invalidated,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    **local_reentry_state,
                },
                stop_loss_pips=adaptive_sl,
                take_profit_pips=adaptive_tp,
            )

        if self.max_spread_pips > 0.0 and ctx.current_spread_pips is not None and ctx.current_spread_pips > self.max_spread_pips:
            return self._hold(
                "spread_too_wide",
                {
                    "spread_pips": ctx.current_spread_pips,
                    "max_spread_pips": self.max_spread_pips,
                    "timeframe_sec": timeframe_sec,
                    "adaptive_timeframe_sec": adaptive_timeframe_sec,
                    "adaptive_timeframe_mode": self.adaptive_timeframe_mode,
                    "timeframe_mode": timeframe_mode,
                    "breakout_buffer_pips": breakout_buffer_pips,
                    "breakout_buffer_price": breakout_buffer_price,
                },
                stop_loss_pips=adaptive_sl,
                take_profit_pips=adaptive_tp,
            )
        if (
            pip_size_source == "fallback_symbol_map"
            and self.require_context_tick_size_for_cfd
            and not self._is_fx_symbol(ctx.symbol)
        ):
            return self._hold(
                "tick_size_unavailable",
                {
                    "symbol": ctx.symbol,
                    "timeframe_sec": timeframe_sec,
                    "require_context_tick_size_for_cfd": self.require_context_tick_size_for_cfd,
                },
                stop_loss_pips=adaptive_sl,
                take_profit_pips=adaptive_tp,
            )
        if atr is None or atr <= 0:
            return self._hold(
                "atr_unavailable",
                {"atr_window": self.atr_window, "timeframe_sec": timeframe_sec},
                stop_loss_pips=adaptive_sl,
                take_profit_pips=adaptive_tp,
            )
        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        relative_sl_pips = (abs(last) * self.min_relative_stop_pct) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        atr_stop_pips = atr_pips * self.atr_multiplier
        adaptive_sl_cap_pips: float | None = None
        adaptive_sl_effective = adaptive_sl
        if self.adaptive_sl_max_atr_ratio > 0.0 and atr_pips > 0.0:
            adaptive_sl_cap_pips = atr_pips * self.adaptive_sl_max_atr_ratio
            adaptive_sl_effective = min(adaptive_sl_effective, adaptive_sl_cap_pips)
        final_sl = max(adaptive_sl_effective, atr_stop_pips, relative_sl_pips)
        tp_dynamic_from_sl = final_sl * self.rr_ratio
        if self.dynamic_tp_only:
            final_tp = max(self.min_take_profit_pips, tp_dynamic_from_sl)
            if self.dynamic_tp_respect_static_floor:
                final_tp = max(final_tp, static_tp)
        else:
            final_tp = max(self.min_take_profit_pips, static_tp, tp_dynamic_from_sl)
        trailing_params = self._calculate_trailing_params(side, last, atr_pips, final_sl, final_tp)

        breakout_distance_pips = (
            breakout_distance_up_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            if side == Side.BUY
            else breakout_distance_down_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        )
        z_ratio = 1.0
        if self.zscore_entry_mode != "off":
            z_ratio = abs(zscore) / max(zscore_effective_threshold, FLOAT_COMPARISON_TOLERANCE)
        slope_ratio = 1.0
        slope_threshold = abs(self.min_slope_ratio)
        if slope_threshold > 0.0:
            slope_ratio = abs(slope) / slope_threshold
        breakout_gate_pips = max(breakout_buffer_pips, 1e-6)
        breakout_ratio = breakout_distance_pips / breakout_gate_pips
        gate_margin_score = (
            self._margin_score(z_ratio)
            + self._margin_score(slope_ratio)
            + self._margin_score(breakout_ratio)
        ) / 3.0
        spread_to_sl_ratio = (
            (ctx.current_spread_pips / max(final_sl, FLOAT_COMPARISON_TOLERANCE))
            if ctx.current_spread_pips is not None and ctx.current_spread_pips > 0.0
            else None
        )
        breakout_to_sl_ratio = breakout_distance_pips / max(final_sl, FLOAT_COMPARISON_TOLERANCE)
        entry_quality_metrics = self._entry_quality_metrics(prices, side)
        breakout_exhausted, exhaustion_meta = self._breakout_exhaustion_guard(
            prices=prices,
            side=side,
            zscore=zscore,
            effective_threshold=zscore_effective_threshold,
            breakout_to_sl_ratio=breakout_to_sl_ratio,
            channel_upper=upper,
            channel_lower=lower,
            entry_quality_metrics=entry_quality_metrics,
        )
        if breakout_exhausted:
            return self._hold(
                "breakout_exhausted",
                {
                    "direction": "up" if side == Side.BUY else "down",
                    "zscore": zscore,
                    "slope": slope,
                    "zscore_threshold": self.z_threshold,
                    "zscore_effective_threshold": zscore_effective_threshold,
                    "directional_extreme_threshold_ratio": self.directional_extreme_threshold_ratio,
                    "zscore_entry_mode": self.zscore_entry_mode,
                    "zscore_baseline_excludes_current_bar": True,
                    **slope_payload,
                    "min_slope_ratio": self.min_slope_ratio,
                    "timeframe_sec": timeframe_sec,
                    "adaptive_timeframe_sec": adaptive_timeframe_sec,
                    "adaptive_timeframe_mode": self.adaptive_timeframe_mode,
                    "timeframe_mode": timeframe_mode,
                    "breakout_buffer_pips": breakout_buffer_pips,
                    "breakout_buffer_price": breakout_buffer_price,
                    "breakout_buffer_atr_price": breakout_buffer_atr_price,
                    "breakout_buffer_pip_price": breakout_buffer_pip_price,
                    "breakout_buffer_spread_price": breakout_buffer_spread_price,
                    "breakout_up_distance_pips": breakout_distance_up_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                    "breakout_down_distance_pips": breakout_distance_down_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                    "sl_applied": final_sl,
                    "tp_applied": final_tp,
                    "atr_pips": atr_pips,
                    **local_reentry_state,
                    **exhaustion_meta,
                },
                stop_loss_pips=final_sl,
                take_profit_pips=final_tp,
            )
        cost_score = self._cost_score(spread_to_sl_ratio)
        extension_score = self._extension_score(breakout_to_sl_ratio)
        confidence_raw_before_volume = self._clamp01(
            self.confidence_base
            + (self.confidence_gate_margin_weight * gate_margin_score)
            + (self.confidence_cost_weight * cost_score)
            + (self.confidence_extension_weight * extension_score)
        )
        volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(
            ctx,
            samples=volume_samples,
            samples_aligned=volume_samples_aligned,
            compare_closed_only=volume_compare_closed_only,
        )
        if volume_hold_reason is not None:
            volume_penalty = (
                self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                if volume_hold_reason == "volume_unavailable"
                else self._SECONDARY_VOLUME_PENALTY
            )
            volume_softened = True
            soft_filter_reasons = [volume_hold_reason]
        else:
            volume_penalty = 0.0
            volume_softened = False
            soft_filter_reasons = []
        confidence_raw_after_volume = min(1.0, confidence_raw_before_volume + volume_confidence_boost)
        soft_filter_penalty_total = volume_penalty
        confidence = self._quantize_confidence(max(0.0, confidence_raw_after_volume - soft_filter_penalty_total))

        signal = Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=final_sl,
            take_profit_pips=final_tp,
            metadata={
                "indicator": "mean_breakout_v2",
                "invalid_prices_dropped": invalid_prices,
                "strategy_mode": "momentum_breakout",
                "direction": "up" if side == Side.BUY else "down",
                "zscore": zscore,
                "slope": slope,
                "zscore_threshold": self.z_threshold,
                "zscore_effective_threshold": zscore_effective_threshold,
                "directional_extreme_threshold_ratio": self.directional_extreme_threshold_ratio,
                "zscore_entry_mode": self.zscore_entry_mode,
                "zscore_baseline_excludes_current_bar": True,
                **slope_payload,
                "confidence_base": self.confidence_base,
                "confidence_gate_margin_weight": self.confidence_gate_margin_weight,
                "confidence_cost_weight": self.confidence_cost_weight,
                "confidence_extension_weight": self.confidence_extension_weight,
                "confidence_gate_margin_norm_ratio": self.confidence_gate_margin_norm_ratio,
                "confidence_cost_spread_to_sl_target": self.confidence_cost_spread_to_sl_target,
                "confidence_cost_curve": "inverse_square",
                "confidence_extension_soft_ratio": self.confidence_extension_soft_ratio,
                "confidence_extension_hard_ratio": self.confidence_extension_hard_ratio,
                "confidence_gate_margin_score": gate_margin_score,
                "confidence_cost_score": cost_score,
                "confidence_extension_score": extension_score,
                "confidence_z_ratio": z_ratio,
                "confidence_slope_ratio": slope_ratio,
                "confidence_breakout_ratio": breakout_ratio,
                "confidence_breakout_to_sl_ratio": breakout_to_sl_ratio,
                "confidence_spread_to_sl_ratio": spread_to_sl_ratio,
                "confidence_quantization_step": self._CONFIDENCE_QUANTIZATION_STEP,
                "confidence_raw_before_volume_boost": confidence_raw_before_volume,
                "confidence_raw_after_volume_boost": confidence_raw_after_volume,
                "confidence_raw_after_soft_penalties": max(0.0, confidence_raw_after_volume - soft_filter_penalty_total),
                "min_slope_ratio": self.min_slope_ratio,
                "max_spread_pips": self.max_spread_pips,
                "spread_pips": ctx.current_spread_pips,
                "exit_z_level": self.exit_z_level,
                "breakout_window": self.br_window,
                "zscore_window": self.z_window,
                "slope_window": self.slope_window,
                "breakout_buffer_pips": breakout_buffer_pips,
                "breakout_buffer_price": breakout_buffer_price,
                "breakout_buffer_atr_price": breakout_buffer_atr_price,
                "breakout_buffer_pip_price": breakout_buffer_pip_price,
                "breakout_buffer_spread_price": breakout_buffer_spread_price,
                "breakout_buffer_atr_ratio": self.breakout_min_buffer_atr_ratio,
                "breakout_buffer_pips_config": self.breakout_min_buffer_pips,
                "breakout_buffer_spread_multiplier": self.breakout_min_buffer_spread_multiplier,
                "breakout_up_raw": breakout_up_raw,
                "breakout_down_raw": breakout_down_raw,
                "breakout_up_distance_price": breakout_distance_up_price,
                "breakout_down_distance_price": breakout_distance_down_price,
                "breakout_up_distance_pips": breakout_distance_up_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                "breakout_down_distance_pips": breakout_distance_down_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE),
                "breakout_invalidation_enabled": self.breakout_invalidation_enabled,
                "breakout_invalidation_lookback_bars": self.breakout_invalidation_lookback_bars,
                "recent_breakout_up_raw": recent_breakout_up_raw,
                "recent_breakout_down_raw": recent_breakout_down_raw,
                "recent_breakout_up_buffered": recent_breakout_up_buffered,
                "recent_breakout_down_buffered": recent_breakout_down_buffered,
                "recent_breakout_up_lag": recent_breakout_up_lag,
                "recent_breakout_down_lag": recent_breakout_down_lag,
                "breakout_invalidation_up": breakout_invalidation_up,
                "breakout_invalidation_down": breakout_invalidation_down,
                "breakout_invalidated": breakout_invalidated,
                "volume_softened": volume_softened,
                "volume_penalty": volume_penalty,
                "soft_filter_penalty_total": soft_filter_penalty_total,
                "soft_filter_reasons": soft_filter_reasons,
                "soft_filter_count": len(soft_filter_reasons),
                "timeframe_sec": timeframe_sec,
                "adaptive_timeframe_sec": adaptive_timeframe_sec,
                "adaptive_timeframe_mode": self.adaptive_timeframe_mode,
                "timeframe_mode": timeframe_mode,
                "sl_multiplier": sl_mult,
                "tp_multiplier": tp_mult,
                "adaptive_sl_before_cap": adaptive_sl,
                "adaptive_sl_cap_pips": adaptive_sl_cap_pips,
                "adaptive_sl_effective": adaptive_sl_effective,
                "adaptive_sl_max_atr_ratio": self.adaptive_sl_max_atr_ratio,
                "atr_stop_pips": atr_stop_pips,
                "sl_applied": final_sl,
                "tp_applied": final_tp,
                "tp_dynamic_from_sl": tp_dynamic_from_sl,
                "tp_static_floor": static_tp,
                "dynamic_tp_respect_static_floor": self.dynamic_tp_respect_static_floor,
                "atr_window": self.atr_window,
                "atr_pips": atr_pips,
                "atr_multiplier": self.atr_multiplier,
                "min_relative_stop_pct": self.min_relative_stop_pct,
                "relative_stop_pips": relative_sl_pips,
                "dynamic_tp_only": self.dynamic_tp_only,
                "pip_size": pip_size,
                "pip_size_source": pip_size_source,
                "channel_upper": upper,
                "channel_lower": lower,
                "trailing_stop": trailing_params,
                "candle_timeframe_sec": self.candle_timeframe_sec,
                "using_closed_candles": using_closed_candles,
                "resample_mode": self.resample_mode,
                "ignore_sunday_candles": self.ignore_sunday_candles,
                "volume_confidence_boost_applied": volume_confidence_boost,
                **exhaustion_meta,
                **local_reentry_state,
                **volume_meta,
            },
        )
        return self._finalize_entry_signal(signal, prices=prices)
