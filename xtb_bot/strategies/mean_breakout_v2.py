from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
import math
import threading

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema, mean_std
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanBreakoutStrategyV2(Strategy):
    name = "mean_breakout_v2"

    @staticmethod
    def _to_bool(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off"}
        return bool(value)

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.z_window = max(5, int(params.get("mb_zscore_window", 40)))
        self.br_window = max(2, int(params.get("mb_breakout_window", 20)))
        self.slope_window = max(2, int(params.get("mb_slope_window", 5)))

        self.z_threshold = float(params.get("mb_zscore_threshold", 2.0))
        self.min_slope_ratio = float(params.get("mb_min_slope_ratio", 0.0001))
        self.zscore_entry_mode = str(params.get("mb_zscore_entry_mode", "max_abs")).strip().lower()
        if self.zscore_entry_mode not in {"max_abs", "directional_extreme", "off"}:
            self.zscore_entry_mode = "max_abs"
        self.exit_z_level = float(params.get("mb_exit_z_level", 0.5))

        self.base_sl = float(params.get("mb_stop_loss_pips", 40.0))
        self.base_tp = float(params.get("mb_take_profit_pips", 100.0))
        self.rr_ratio = max(1.0, float(params.get("mb_risk_reward_ratio", 2.5)))
        self.min_relative_stop_pct = max(0.0, float(params.get("mb_min_relative_stop_pct", 0.0008)))
        self.dynamic_tp_only = self._to_bool(params.get("mb_dynamic_tp_only"), True)

        default_atr_window = min(14, self.z_window)
        self.atr_window = max(2, int(params.get("mb_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("mb_atr_multiplier", 1.5)))
        self.min_stop_loss_pips = max(0.1, float(params.get("mb_min_stop_loss_pips", self.base_sl)))
        self.min_take_profit_pips = max(0.1, float(params.get("mb_min_take_profit_pips", self.base_tp)))

        self.default_timeframe_sec = float(params.get("mb_timeframe_sec", 300.0))
        if self.default_timeframe_sec <= 0:
            self.default_timeframe_sec = 300.0
        self.candle_timeframe_sec = max(0, int(float(params.get("mb_candle_timeframe_sec", self.default_timeframe_sec))))
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
        self.trailing_atr_multiplier = max(0.1, float(params.get("mb_trailing_atr_multiplier", 1.5)))
        self.trailing_activation_stop_ratio = max(
            0.0,
            float(params.get("mb_trailing_activation_stop_ratio", 1.0)),
        )
        self.trailing_min_activation_pips = max(
            0.0,
            float(params.get("mb_trailing_min_activation_pips", 0.0)),
        )
        self.volume_confirmation = self._to_bool(params.get("mb_volume_confirmation"), False)
        self.volume_window = max(2, int(params.get("mb_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("mb_min_volume_ratio", 1.5)))
        self.volume_min_samples = max(1, int(params.get("mb_volume_min_samples", 8)))
        self.volume_allow_missing = self._to_bool(params.get("mb_volume_allow_missing"), True)
        self.volume_require_spike = self._to_bool(params.get("mb_volume_require_spike"), False)
        self.volume_confidence_boost = max(0.0, float(params.get("mb_volume_confidence_boost", 0.1)))
        trailing_be_raw = params.get("mb_trailing_breakeven_offset_pips")
        if trailing_be_raw is None:
            self.trailing_breakeven_offset_pips: float | None = None
        else:
            self.trailing_breakeven_offset_pips = max(0.0, float(trailing_be_raw))

        self.stop_loss_pips = self.base_sl
        self.take_profit_pips = max(self.base_tp, self.base_sl * self.rr_ratio)
        self.min_history = max(self.z_window + self.slope_window + 1, self.br_window + 2, self.atr_window + 2)
        self._signal_lock = threading.Lock()

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        upper = symbol.upper()
        if upper in {"AUS200", "AU200"}:
            return 1.0
        if upper in {"US100", "US500", "US30", "DE40", "UK100", "FRA40", "JP225", "EU50"}:
            return 0.1
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return 0.1
        if upper.endswith("JPY"):
            return 0.01
        if upper.startswith("XAU") or upper.startswith("XAG"):
            return 0.1
        if upper in {"WTI", "BRENT"}:
            return 0.1
        return 0.0001

    @staticmethod
    def _is_fx_symbol(symbol: str) -> bool:
        upper = symbol.upper()
        return len(upper) == 6 and upper.isalpha()

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

    @staticmethod
    def _timestamp_to_seconds(raw_ts: float) -> float:
        ts = float(raw_ts)
        abs_ts = abs(ts)
        if abs_ts > 10_000_000_000_000:
            return ts / 1_000_000.0
        if abs_ts > 10_000_000_000:
            return ts / 1_000.0
        return ts

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
        if min(deltas) < timeframe * 0.1:
            return False
        median_delta = deltas[len(deltas) // 2]
        return median_delta >= timeframe * 0.2

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
        if self.resample_mode == "auto" and self._is_candle_like_spacing(normalized_timestamps):
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

        if len(closes) < 2:
            return [], True
        return closes[:-1], True

    def _get_adaptive_params(self, timeframe_sec: float) -> tuple[float, float, str]:
        if timeframe_sec <= self.m5_max_sec:
            return self.sl_mult_m5, self.tp_mult_m5, "M5_or_lower"
        if timeframe_sec <= self.m15_max_sec:
            return self.sl_mult_m15, self.tp_mult_m15, "M15"
        return self.sl_mult_h1, self.tp_mult_h1, "H1_plus"

    def _calculate_trailing_params(
        self,
        side: Side,
        last_price: float,
        atr_pips: float,
        initial_stop_pips: float,
    ) -> dict[str, object] | None:
        _ = side
        _ = last_price
        if not self.trailing_enabled:
            return None
        trailing_distance_pips = max(0.1, atr_pips * self.trailing_atr_multiplier)
        trailing_activation_pips = max(
            self.trailing_min_activation_pips,
            initial_stop_pips * self.trailing_activation_stop_ratio,
        )
        payload: dict[str, object] = {
            "trailing_enabled": True,
            "trailing_distance_pips": trailing_distance_pips,
            "trailing_activation_pips": trailing_activation_pips,
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

    def _evaluate_volume_confirmation(self, ctx: StrategyContext) -> tuple[dict[str, object], str | None, float]:
        metadata: dict[str, object] = {
            "volume_confirmation_enabled": self.volume_confirmation,
            "volume_window": self.volume_window,
            "min_volume_ratio": self.min_volume_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_require_spike": self.volume_require_spike,
            "volume_confidence_boost_config": self.volume_confidence_boost,
            "volume_data_available": False,
            "volume_spike": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
        }
        if not self.volume_confirmation:
            return metadata, None, 0.0

        raw_samples = self._extract_finite_positive_volumes(ctx.volumes[-(self.volume_window + 5) :])
        current_volume = None
        if ctx.current_volume is not None:
            try:
                current_volume = float(ctx.current_volume)
            except (TypeError, ValueError):
                current_volume = None
            if current_volume is not None and (not math.isfinite(current_volume) or current_volume <= 0.0):
                current_volume = None
        if current_volume is not None:
            raw_samples.append(current_volume)

        if len(raw_samples) < 2:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = raw_samples[-1]
        history = raw_samples[:-1]
        if len(history) > self.volume_window:
            history = history[-self.volume_window :]
        if len(history) < self.volume_min_samples:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        avg_volume = sum(history) / max(len(history), 1)
        if avg_volume <= 0.0:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        volume_ratio = current / avg_volume
        spike = volume_ratio >= self.min_volume_ratio
        metadata.update(
            {
                "volume_data_available": True,
                "volume_spike": spike,
                "volume_current": current,
                "volume_avg": avg_volume,
                "volume_ratio": volume_ratio,
            }
        )
        if self.volume_require_spike and not spike:
            return metadata, "volume_not_confirmed", 0.0
        return metadata, None, self.volume_confidence_boost if spike else 0.0

    def _get_indicators(self, prices: Sequence[float]) -> tuple[float, float]:
        subset = prices[-self.z_window :]
        current_ema = self._ema(subset, self.z_window)

        mean, std = mean_std(subset)
        zscore = (float(prices[-1]) - mean) / std if std > 0 else 0.0

        prev_slice_start = -(self.z_window + self.slope_window)
        prev_slice_end = -self.slope_window
        prev_subset = prices[prev_slice_start:prev_slice_end]
        prev_ema = self._ema(prev_subset, self.z_window)
        slope = (current_ema - prev_ema) / prev_ema if prev_ema > 0 else 0.0

        return zscore, slope

    def _passes_zscore_filter(self, side: Side, zscore: float) -> tuple[bool, str | None]:
        if self.zscore_entry_mode == "off":
            return True, None
        if self.zscore_entry_mode == "max_abs":
            if abs(zscore) <= self.z_threshold:
                return True, None
            return False, "zscore_overextended"
        if side == Side.BUY:
            if zscore >= self.z_threshold:
                return True, None
        elif side == Side.SELL:
            if zscore <= -self.z_threshold:
                return True, None
        return False, "zscore_directional_not_confirmed"

    def _hold(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "mean_breakout_v2"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        with self._signal_lock:
            return self._generate_signal_locked(ctx)

    def _generate_signal_locked(self, ctx: StrategyContext) -> Signal:
        raw_prices = [float(v) for v in ctx.prices]
        raw_timestamps = [float(v) for v in ctx.timestamps]
        prices, using_closed_candles = self._resample_closed_candle_closes(raw_prices, raw_timestamps)
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
                },
            )

        timeframe_sec = float(self.candle_timeframe_sec) if using_closed_candles else self._resolve_timeframe_sec(ctx)
        sl_mult, tp_mult, timeframe_mode = self._get_adaptive_params(timeframe_sec)
        static_sl = self.base_sl * sl_mult
        static_tp = self.base_tp * tp_mult
        adaptive_sl = max(self.min_stop_loss_pips, static_sl)
        adaptive_tp = max(self.min_take_profit_pips, static_tp, adaptive_sl * self.rr_ratio)
        self.stop_loss_pips = adaptive_sl
        self.take_profit_pips = adaptive_tp

        zscore, slope = self._get_indicators(prices)

        reference = prices[-(self.br_window + 1) : -1]
        upper = max(reference)
        lower = min(reference)
        last = prices[-1]
        prev = prices[-2]

        breakout_up = last > upper and prev <= upper
        breakout_down = last < lower and prev >= lower

        side = Side.HOLD
        hold_reason = "no_signal"
        if breakout_up:
            if slope > self.min_slope_ratio:
                z_ok, z_reason = self._passes_zscore_filter(Side.BUY, zscore)
                if z_ok:
                    side = Side.BUY
                else:
                    hold_reason = z_reason or "zscore_filter_blocked"
            else:
                hold_reason = "slope_filter_blocked"
        elif breakout_down:
            if slope < -self.min_slope_ratio:
                z_ok, z_reason = self._passes_zscore_filter(Side.SELL, zscore)
                if z_ok:
                    side = Side.SELL
                else:
                    hold_reason = z_reason or "zscore_filter_blocked"
            else:
                hold_reason = "slope_filter_blocked"

        if side == Side.HOLD:
            exit_hint = "momentum_exhaustion" if abs(zscore) < self.exit_z_level else None
            return self._hold(
                hold_reason,
                {
                    "hold_reason": hold_reason,
                    "zscore": zscore,
                    "slope": slope,
                    "zscore_threshold": self.z_threshold,
                    "zscore_entry_mode": self.zscore_entry_mode,
                    "min_slope_ratio": self.min_slope_ratio,
                    "exit_z_level": self.exit_z_level,
                    "exit_hint": exit_hint,
                    "timeframe_sec": timeframe_sec,
                    "timeframe_mode": timeframe_mode,
                    "sl_multiplier": sl_mult,
                    "tp_multiplier": tp_mult,
                    "sl_applied": adaptive_sl,
                    "tp_applied": adaptive_tp,
                    "atr_window": self.atr_window,
                    "atr_pips": None,
                    "atr_multiplier": self.atr_multiplier,
                    "pip_size": None,
                    "pip_size_source": None,
                    "breakout_up": breakout_up,
                    "breakout_down": breakout_down,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                },
            )

        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return self._hold("atr_unavailable", {"atr_window": self.atr_window, "timeframe_sec": timeframe_sec})

        if ctx.tick_size not in (None, 0.0):
            pip_size = float(ctx.tick_size)
            pip_size_source = "context_tick_size"
        else:
            if self.require_context_tick_size_for_cfd and not self._is_fx_symbol(ctx.symbol):
                return self._hold(
                    "tick_size_unavailable",
                    {
                        "symbol": ctx.symbol,
                        "timeframe_sec": timeframe_sec,
                        "require_context_tick_size_for_cfd": self.require_context_tick_size_for_cfd,
                    },
                )
            pip_size = self._pip_size(ctx.symbol)
            pip_size_source = "fallback_symbol_map"
        atr_pips = atr / max(pip_size, 1e-9)
        relative_sl_pips = (abs(last) * self.min_relative_stop_pct) / max(pip_size, 1e-9)
        final_sl = max(adaptive_sl, atr_pips * self.atr_multiplier, relative_sl_pips)
        if self.dynamic_tp_only:
            final_tp = max(self.min_take_profit_pips, final_sl * self.rr_ratio)
        else:
            final_tp = max(self.min_take_profit_pips, static_tp, final_sl * self.rr_ratio)
        self.stop_loss_pips = final_sl
        self.take_profit_pips = final_tp
        trailing_params = self._calculate_trailing_params(side, last, atr_pips, final_sl)

        breakout_level = upper if side == Side.BUY else lower
        breakout_strength = abs(last - breakout_level) / max(abs(last), 1e-9)
        if self.zscore_entry_mode == "max_abs":
            z_norm = max(0.0, 1.0 - min(1.0, abs(zscore) / max(self.z_threshold, 1e-9)))
        elif self.zscore_entry_mode == "off":
            z_norm = 0.5
        else:
            z_norm = min(1.0, abs(zscore) / max(self.z_threshold * 2.0, 1e-9))
        slope_norm = min(1.0, abs(slope) * 1500.0)
        confidence = min(1.0, (z_norm * 0.4) + (min(1.0, breakout_strength * 600.0) * 0.35) + (slope_norm * 0.25))
        volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
        if volume_hold_reason is not None:
            return self._hold(
                volume_hold_reason,
                {
                    "direction": "up" if side == Side.BUY else "down",
                    "zscore": zscore,
                    "slope": slope,
                    "zscore_threshold": self.z_threshold,
                    "zscore_entry_mode": self.zscore_entry_mode,
                    "min_slope_ratio": self.min_slope_ratio,
                    "timeframe_sec": timeframe_sec,
                    "timeframe_mode": timeframe_mode,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    **volume_meta,
                },
            )
        confidence = min(1.0, confidence + volume_confidence_boost)

        return Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=final_sl,
            take_profit_pips=final_tp,
            metadata={
                "indicator": "mean_breakout_v2",
                "strategy_mode": "momentum_breakout",
                "direction": "up" if side == Side.BUY else "down",
                "zscore": zscore,
                "slope": slope,
                "zscore_threshold": self.z_threshold,
                "zscore_entry_mode": self.zscore_entry_mode,
                "min_slope_ratio": self.min_slope_ratio,
                "exit_z_level": self.exit_z_level,
                "breakout_window": self.br_window,
                "zscore_window": self.z_window,
                "slope_window": self.slope_window,
                "timeframe_sec": timeframe_sec,
                "timeframe_mode": timeframe_mode,
                "sl_multiplier": sl_mult,
                "tp_multiplier": tp_mult,
                "sl_applied": final_sl,
                "tp_applied": final_tp,
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
                **volume_meta,
            },
        )
