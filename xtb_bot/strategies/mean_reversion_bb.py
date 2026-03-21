from __future__ import annotations

from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, mean_std, rsi_sma, rsi_wilder, tail_mean
from xtb_bot.strategies.pip_size import pip_size as _pip_size_lookup
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanReversionBbStrategy(Strategy):
    name = "mean_reversion_bb"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.bb_window = max(5, int(params.get("mean_reversion_bb_window", 20)))
        self.bb_std_dev = max(0.1, float(params.get("mean_reversion_bb_std_dev", 2.2)))
        self.entry_mode = str(params.get("mean_reversion_bb_entry_mode", "reentry")).strip().lower()
        if self.entry_mode not in {"touch", "reentry"}:
            self.entry_mode = "reentry"
        self.reentry_tolerance_sigma = max(
            0.0,
            float(params.get("mean_reversion_bb_reentry_tolerance_sigma", 0.05)),
        )
        self.use_rsi_filter = self._as_bool(params.get("mean_reversion_bb_use_rsi_filter", True), True)
        self.rsi_period = max(2, int(params.get("mean_reversion_bb_rsi_period", 14)))
        self.rsi_history_multiplier = max(
            1.0,
            float(params.get("mean_reversion_bb_rsi_history_multiplier", 3.0)),
        )
        self.rsi_method = str(params.get("mean_reversion_bb_rsi_method", "wilder")).strip().lower()
        if self.rsi_method not in {"wilder", "sma"}:
            self.rsi_method = "wilder"
        self.rsi_overbought = min(100.0, max(0.0, float(params.get("mean_reversion_bb_rsi_overbought", 70.0))))
        self.rsi_oversold = min(100.0, max(0.0, float(params.get("mean_reversion_bb_rsi_oversold", 30.0))))

        self.min_std_ratio = max(0.0, float(params.get("mean_reversion_bb_min_std_ratio", 0.00005)))
        self.min_band_extension_ratio = max(
            0.0, float(params.get("mean_reversion_bb_min_band_extension_ratio", 0.0))
        )
        self.max_band_extension_ratio = max(
            0.0, float(params.get("mean_reversion_bb_max_band_extension_ratio", 2.0))
        )
        self.trend_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_trend_filter_enabled", False),
            False,
        )
        self.trend_ma_window = max(2, int(params.get("mean_reversion_bb_trend_ma_window", 100)))
        self.trend_filter_mode = str(
            params.get("mean_reversion_bb_trend_filter_mode", "strict")
        ).strip().lower()
        if self.trend_filter_mode not in {"strict", "extreme_override"}:
            self.trend_filter_mode = "strict"
        self.trend_filter_extreme_sigma = max(
            0.0, float(params.get("mean_reversion_bb_trend_filter_extreme_sigma", 2.5))
        )
        self.trend_slope_lookback_bars = max(
            1,
            int(params.get("mean_reversion_bb_trend_slope_lookback_bars", 5)),
        )
        self.trend_slope_strict_threshold = max(
            0.0,
            float(params.get("mean_reversion_bb_trend_slope_strict_threshold", 0.00015)),
        )
        self.exit_on_midline = self._as_bool(params.get("mean_reversion_bb_exit_on_midline", True), True)
        self.exit_midline_tolerance_sigma = max(
            0.0, float(params.get("mean_reversion_bb_exit_midline_tolerance_sigma", 0.15))
        )

        self.use_atr_sl_tp = self._as_bool(params.get("mean_reversion_bb_use_atr_sl_tp", False), False)
        self.atr_window = max(2, int(params.get("mean_reversion_bb_atr_window", 14)))
        self.atr_multiplier = max(0.1, float(params.get("mean_reversion_bb_atr_multiplier", 1.5)))
        self.risk_reward_ratio = max(1.0, float(params.get("mean_reversion_bb_risk_reward_ratio", 2.0)))
        self.take_profit_mode = str(
            params.get("mean_reversion_bb_take_profit_mode", "rr")
        ).strip().lower()
        if self.take_profit_mode not in {"rr", "midline", "midline_or_rr"}:
            self.take_profit_mode = "rr"

        self.min_stop_loss_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_bb_min_stop_loss_pips",
                    params.get("mean_reversion_bb_stop_loss_pips", params.get("stop_loss_pips", 25.0)),
                )
            ),
        )
        self.min_take_profit_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_bb_min_take_profit_pips",
                    params.get("mean_reversion_bb_take_profit_pips", params.get("take_profit_pips", 30.0)),
                )
            ),
        )
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = self.min_take_profit_pips
        self.reentry_base_confidence = min(
            1.0,
            max(0.0, float(params.get("mean_reversion_bb_reentry_base_confidence", 0.70))),
        )
        self.reentry_rsi_bonus = min(
            0.5,
            max(0.0, float(params.get("mean_reversion_bb_reentry_rsi_bonus", 0.10))),
        )
        self.reentry_extension_confidence_weight = min(
            0.5,
            max(0.0, float(params.get("mean_reversion_bb_reentry_extension_confidence_weight", 0.15))),
        )
        volume_confirmation_raw = params.get("mean_reversion_bb_volume_confirmation", True)
        self.volume_confirmation = self._as_bool(volume_confirmation_raw, True)
        self.volume_window = max(2, int(params.get("mean_reversion_bb_volume_window", 20)))
        self.volume_min_ratio = max(1.0, float(params.get("mean_reversion_bb_min_volume_ratio", 1.5)))
        self.volume_min_samples = max(1, int(params.get("mean_reversion_bb_volume_min_samples", 10)))
        self.volume_allow_missing = self._as_bool(params.get("mean_reversion_bb_volume_allow_missing", True), True)
        self.volume_require_spike = self._as_bool(params.get("mean_reversion_bb_volume_require_spike", False), False)
        self.volume_confidence_boost = min(
            0.4,
            max(0.0, float(params.get("mean_reversion_bb_volume_confidence_boost", 0.20))),
        )

        rsi_history = (
            max(self.rsi_period + 1, int(self.rsi_period * self.rsi_history_multiplier) + 1)
            if self.use_rsi_filter
            else 0
        )
        self.min_history = max(
            self.bb_window + 1,
            rsi_history,
            (self.atr_window + 1) if self.use_atr_sl_tp else 0,
            (self.trend_ma_window + self.trend_slope_lookback_bars) if self.trend_filter_enabled else 0,
        )

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _mean_std(values: Sequence[float]) -> tuple[float, float]:
        return mean_std(values)

    def _rsi_sma(self, prices: Sequence[float]) -> float:
        period_slice = prices[-(self.rsi_period + 1):] if len(prices) >= self.rsi_period + 1 else prices
        return rsi_sma(period_slice)

    def _rsi_wilder(self, prices: Sequence[float]) -> float:
        return rsi_wilder(prices, self.rsi_period)

    def _rsi(self, prices: Sequence[float]) -> float:
        if self.rsi_method == "sma":
            return self._rsi_sma(prices)
        return self._rsi_wilder(prices)

    @staticmethod
    def _atr(values: Sequence[float], window: int) -> float | None:
        return atr_wilder(values, window)

    @staticmethod
    def _positive_finite_volumes(values: Sequence[float]) -> list[float]:
        cleaned: list[float] = []
        for raw in values:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if value > 0:
                cleaned.append(value)
        return cleaned

    @staticmethod
    def _looks_like_cumulative_volume(samples: Sequence[float]) -> tuple[bool, int]:
        if len(samples) < 4:
            return False, 0
        non_decreasing = 0
        resets = 0
        comparisons = 0
        prev = float(samples[0])
        for raw in samples[1:]:
            current = float(raw)
            comparisons += 1
            if current >= prev:
                non_decreasing += 1
            elif current < (prev * 0.7):
                resets += 1
            prev = current
        if comparisons <= 0:
            return False, resets
        monotonic_ratio = non_decreasing / float(comparisons)
        return resets > 0 and monotonic_ratio >= 0.55, resets

    @staticmethod
    def _completed_candle_volumes_from_cumulative(samples: Sequence[float]) -> list[float]:
        if not samples:
            return []
        completed: list[float] = []
        segment_max = float(samples[0])
        prev = float(samples[0])
        for raw in samples[1:]:
            current = float(raw)
            if current < (prev * 0.7):
                completed.append(segment_max)
                segment_max = current
            else:
                segment_max = max(segment_max, current)
            prev = current
        return completed

    @staticmethod
    def _pip_size(symbol: str) -> float:
        return _pip_size_lookup(symbol)

    def _dynamic_sl_tp(
        self,
        ctx: StrategyContext,
        prices: Sequence[float],
    ) -> tuple[float, float, float | None, float, str]:
        stop_pips = self.min_stop_loss_pips
        take_pips = max(self.min_take_profit_pips, stop_pips * self.risk_reward_ratio)
        atr_pips: float | None = None
        if ctx.tick_size is not None and ctx.tick_size > 0:
            pip_size = float(ctx.tick_size)
            pip_size_source = "context_tick_size"
        else:
            pip_size = self._pip_size(ctx.symbol)
            pip_size_source = "fallback_symbol_map"
        if not self.use_atr_sl_tp:
            return stop_pips, take_pips, atr_pips, pip_size, pip_size_source
        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return stop_pips, take_pips, atr_pips, pip_size, pip_size_source
        atr_pips = atr / max(pip_size, 1e-9)
        stop_pips = max(stop_pips, atr_pips * self.atr_multiplier)
        take_pips = max(take_pips, stop_pips * self.risk_reward_ratio)
        return stop_pips, take_pips, atr_pips, pip_size, pip_size_source

    def _target_take_profit(
        self,
        last: float,
        mean: float,
        pip_size: float,
        rr_take_pips: float,
    ) -> tuple[float, float]:
        midline_target_pips = max(self.min_take_profit_pips, abs(last - mean) / max(pip_size, 1e-9))
        if self.take_profit_mode == "rr":
            return rr_take_pips, midline_target_pips
        if self.take_profit_mode == "midline":
            return midline_target_pips, midline_target_pips
        return max(self.min_take_profit_pips, min(rr_take_pips, midline_target_pips)), midline_target_pips

    def _trend_filter_allows(
        self,
        *,
        expected_side: Side,
        last: float,
        trend_ma: float | None,
        distance_sigma: float,
    ) -> tuple[bool, bool]:
        if not self.trend_filter_enabled or trend_ma is None:
            return True, False

        blocked = (expected_side == Side.SELL and last <= trend_ma) or (
            expected_side == Side.BUY and last >= trend_ma
        )
        if not blocked:
            return True, False

        if self.trend_filter_mode == "extreme_override" and distance_sigma >= self.trend_filter_extreme_sigma:
            return True, True
        return False, False

    def _trend_slope_ratio(self, prices: Sequence[float]) -> float | None:
        if not self.trend_filter_enabled:
            return None
        lookback = max(self.trend_slope_lookback_bars, 1)
        required = self.trend_ma_window + lookback
        if len(prices) < required:
            return None
        trend_now_slice = prices[-self.trend_ma_window :]
        trend_prev_slice = prices[-required:-lookback]
        trend_ma_now = sum(trend_now_slice) / max(len(trend_now_slice), 1)
        trend_ma_prev = sum(trend_prev_slice) / max(len(trend_prev_slice), 1)
        if abs(trend_ma_prev) <= 1e-12:
            return 0.0
        return (trend_ma_now - trend_ma_prev) / trend_ma_prev

    def _volume_spike_payload(self, ctx: StrategyContext) -> tuple[bool, dict[str, float | str | bool]]:
        payload: dict[str, float | str | bool] = {
            "volume_confirmation": self.volume_confirmation,
            "volume_window": self.volume_window,
            "volume_min_ratio": self.volume_min_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_require_spike": self.volume_require_spike,
        }
        if not self.volume_confirmation:
            payload["volume_status"] = "disabled"
            payload["vol_spike"] = False
            return False, payload

        history = self._positive_finite_volumes(ctx.volumes)
        current_volume = 0.0
        if ctx.current_volume is not None:
            try:
                current_volume = float(ctx.current_volume)
            except (TypeError, ValueError):
                current_volume = 0.0
        if current_volume <= 0.0 and history:
            current_volume = float(history[-1])
        payload["current_volume"] = current_volume if current_volume > 0 else None
        payload["volume_history_len"] = len(history)
        if current_volume <= 0.0:
            payload["volume_status"] = "missing_current_volume"
            payload["vol_spike"] = False
            return False, payload

        stream = list(history)
        if not stream or abs(stream[-1] - current_volume) > 1e-9:
            stream.append(current_volume)
        cumulative_mode, resets_detected = self._looks_like_cumulative_volume(stream)
        payload["volume_resets_detected"] = resets_detected
        if cumulative_mode:
            baseline = self._completed_candle_volumes_from_cumulative(stream)
            payload["volume_series_mode"] = "cumulative_candle_max"
        else:
            baseline = list(history)
            if baseline and abs(baseline[-1] - current_volume) <= 1e-9:
                baseline = baseline[:-1]
            payload["volume_series_mode"] = "raw_samples"
        payload["volume_baseline_len"] = len(baseline)
        lookback = min(self.volume_window, len(baseline))
        payload["volume_lookback"] = lookback
        if lookback < self.volume_min_samples:
            payload["volume_status"] = "insufficient_volume_history"
            payload["vol_spike"] = False
            return False, payload

        avg_volume = tail_mean(baseline, lookback)
        min_required = avg_volume * self.volume_min_ratio
        is_spike = current_volume >= min_required
        payload["avg_volume"] = avg_volume
        payload["min_required_volume"] = min_required
        payload["volume_status"] = "ok" if is_spike else "below_threshold"
        payload["vol_spike"] = is_spike
        return is_spike, payload

    def _hold(self, reason: str, **extra: object) -> Signal:
        payload: dict[str, object] = {"indicator": "bollinger_bands", "reason": reason}
        payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.min_stop_loss_pips,
            take_profit_pips=self.min_take_profit_pips,
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        prices = [float(value) for value in ctx.prices]
        if len(prices) < self.min_history:
            return self._hold("insufficient_price_history", prices_len=len(prices), min_history=self.min_history)

        bb_slice = prices[-self.bb_window :]
        mean, std = self._mean_std(bb_slice)
        if std <= 1e-12:
            return self._hold("flat_volatility", bb_midline=mean, bb_std=std)

        volatility_ratio = std / max(abs(mean), 1e-9)
        if volatility_ratio < self.min_std_ratio:
            return self._hold(
                "low_volatility",
                bb_midline=mean,
                bb_std=std,
                volatility_ratio=volatility_ratio,
                min_volatility_ratio=self.min_std_ratio,
            )

        upper = mean + self.bb_std_dev * std
        lower = mean - self.bb_std_dev * std
        last = float(prices[-1])
        prev_last = float(prices[-2]) if len(prices) >= 2 else last
        rsi_history_len = max(self.rsi_period + 1, int(self.rsi_period * self.rsi_history_multiplier) + 1)
        rsi = self._rsi(prices[-rsi_history_len:])
        stop_loss_pips, rr_take_profit_pips, atr_pips, pip_size, pip_size_source = self._dynamic_sl_tp(ctx, prices)
        take_profit_pips, midline_target_pips = self._target_take_profit(last, mean, pip_size, rr_take_profit_pips)
        volume_spike, volume_payload = self._volume_spike_payload(ctx)
        trend_ma = None
        if self.trend_filter_enabled:
            trend_slice = prices[-self.trend_ma_window :]
            trend_ma = sum(trend_slice) / max(len(trend_slice), 1)
        trend_slope_ratio = self._trend_slope_ratio(prices)
        distance_sigma = abs(last - mean) / max(std, 1e-9)

        rsi_allows_sell = (not self.use_rsi_filter) or (rsi >= self.rsi_overbought)
        rsi_allows_buy = (not self.use_rsi_filter) or (rsi <= self.rsi_oversold)

        reentry_sell = False
        reentry_buy = False
        prev_extension_ratio = 0.0
        if self.entry_mode == "reentry" and len(prices) >= self.bb_window + 1:
            prev_bb_slice = prices[-(self.bb_window + 1) : -1]
            prev_mean, prev_std = self._mean_std(prev_bb_slice)
            prev_upper = prev_mean + self.bb_std_dev * prev_std
            prev_lower = prev_mean - self.bb_std_dev * prev_std
            prev_tolerance = self.reentry_tolerance_sigma * max(prev_std, 0.0)
            # Require both re-entry into the band and first sign of reversal momentum.
            reentry_sell = prev_last >= (prev_upper - prev_tolerance) and last < upper and last <= prev_last
            reentry_buy = prev_last <= (prev_lower + prev_tolerance) and last > lower and last >= prev_last
            prev_band_width = max(prev_upper - prev_lower, 1e-9)
            if prev_last >= prev_upper:
                prev_extension_ratio = (prev_last - prev_upper) / prev_band_width
            elif prev_last <= prev_lower:
                prev_extension_ratio = (prev_lower - prev_last) / prev_band_width

        bb_distance = 0.0
        if last >= upper:
            bb_distance = (last - upper) / max(abs(last), 1e-9)
        elif last <= lower:
            bb_distance = (lower - last) / max(abs(last), 1e-9)

        band_width = max(upper - lower, 1e-9)
        band_extension_ratio = 0.0
        if last >= upper:
            band_extension_ratio = (last - upper) / band_width
        elif last <= lower:
            band_extension_ratio = (lower - last) / band_width

        if (
            self.max_band_extension_ratio > 0
            and ((last >= upper) or (last <= lower))
            and band_extension_ratio > self.max_band_extension_ratio
        ):
            return self._hold(
                "band_extension_too_extreme",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                rsi=rsi,
                rsi_method=self.rsi_method,
                band_extension_ratio=band_extension_ratio,
                max_band_extension_ratio=self.max_band_extension_ratio,
                trend_ma=trend_ma,
            )

        if (
            self.min_band_extension_ratio > 0
            and ((last >= upper) or (last <= lower))
            and band_extension_ratio < self.min_band_extension_ratio
        ):
            return self._hold(
                "band_touch_too_shallow",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                rsi=rsi,
                rsi_method=self.rsi_method,
                band_extension_ratio=band_extension_ratio,
                min_band_extension_ratio=self.min_band_extension_ratio,
                trend_ma=trend_ma,
            )

        sell_trigger = (last >= upper) if self.entry_mode == "touch" else reentry_sell
        buy_trigger = (last <= lower) if self.entry_mode == "touch" else reentry_buy

        if self.entry_mode == "reentry" and (last >= upper or last <= lower) and not (sell_trigger or buy_trigger):
            return self._hold(
                "band_walk_no_reentry",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                last=last,
                prev_last=prev_last,
                rsi=rsi,
                rsi_method=self.rsi_method,
                entry_mode=self.entry_mode,
                trend_ma=trend_ma,
                trend_slope_ratio=trend_slope_ratio,
            )

        if sell_trigger and rsi_allows_sell:
            if (
                self.trend_filter_enabled
                and self.trend_filter_mode == "strict"
                and trend_slope_ratio is not None
                and trend_slope_ratio > self.trend_slope_strict_threshold
            ):
                return self._hold(
                    "trend_too_strong_up",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="sell",
                    trend_ma=trend_ma,
                    trend_slope_ratio=trend_slope_ratio,
                    trend_slope_strict_threshold=self.trend_slope_strict_threshold,
                )
            trend_allowed, trend_override = self._trend_filter_allows(
                expected_side=Side.SELL,
                last=last,
                trend_ma=trend_ma,
                distance_sigma=distance_sigma,
            )
            if not trend_allowed:
                return self._hold(
                    "trend_filter_blocked",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="sell",
                    trend_ma=trend_ma,
                    trend_direction="up_or_flat",
                    trend_filter_mode=self.trend_filter_mode,
                    distance_sigma=distance_sigma,
                    trend_filter_extreme_sigma=self.trend_filter_extreme_sigma,
                    trend_slope_ratio=trend_slope_ratio,
                )
            if self.volume_confirmation and not volume_spike:
                status = str(volume_payload.get("volume_status") or "")
                if status in {"missing_current_volume", "insufficient_volume_history"} and not self.volume_allow_missing:
                    return self._hold("volume_data_unavailable", **volume_payload)
                if status == "below_threshold" and self.volume_require_spike:
                    return self._hold("volume_below_threshold", **volume_payload)
            extension_ratio = band_extension_ratio if self.entry_mode == "touch" else max(prev_extension_ratio, 0.0)
            band_score = min(1.0, max(0.0, extension_ratio) / 0.15)
            rsi_score = (
                min(1.0, max(0.0, (rsi - self.rsi_overbought) / max(100.0 - self.rsi_overbought, 1e-9)))
                if self.use_rsi_filter
                else min(1.0, abs(rsi - 50.0) / 50.0)
            )
            if self.entry_mode == "reentry":
                confidence = self.reentry_base_confidence
                confidence += rsi_score * self.reentry_rsi_bonus
                confidence += band_score * self.reentry_extension_confidence_weight
                if volume_spike:
                    confidence += self.volume_confidence_boost
                confidence = min(1.0, max(0.0, confidence))
            else:
                confidence = min(1.0, 0.2 + band_score * 0.55 + rsi_score * 0.25)
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "bollinger_bands",
                    "band": "upper",
                    "bb_window": self.bb_window,
                    "bb_std_dev": self.bb_std_dev,
                    "bb_midline": mean,
                    "upper": upper,
                    "lower": lower,
                    "rsi": rsi,
                    "rsi_method": self.rsi_method,
                    "use_rsi_filter": self.use_rsi_filter,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "take_profit_mode": self.take_profit_mode,
                    "midline_target_pips": midline_target_pips,
                    "rr_target_pips": rr_take_profit_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "distance_sigma": distance_sigma,
                    "entry_mode": self.entry_mode,
                    "trend_filter_mode": self.trend_filter_mode,
                    "trend_filter_override": trend_override,
                    "trend_ma": trend_ma,
                    "trend_slope_ratio": trend_slope_ratio,
                    "trend_slope_strict_threshold": self.trend_slope_strict_threshold,
                    "volume_spike": volume_spike,
                    **volume_payload,
                    "exit_hint": "close_on_bb_midline",
                },
            )

        if buy_trigger and rsi_allows_buy:
            if (
                self.trend_filter_enabled
                and self.trend_filter_mode == "strict"
                and trend_slope_ratio is not None
                and trend_slope_ratio < -self.trend_slope_strict_threshold
            ):
                return self._hold(
                    "trend_too_strong_down",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="buy",
                    trend_ma=trend_ma,
                    trend_slope_ratio=trend_slope_ratio,
                    trend_slope_strict_threshold=self.trend_slope_strict_threshold,
                )
            trend_allowed, trend_override = self._trend_filter_allows(
                expected_side=Side.BUY,
                last=last,
                trend_ma=trend_ma,
                distance_sigma=distance_sigma,
            )
            if not trend_allowed:
                return self._hold(
                    "trend_filter_blocked",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="buy",
                    trend_ma=trend_ma,
                    trend_direction="down_or_flat",
                    trend_filter_mode=self.trend_filter_mode,
                    distance_sigma=distance_sigma,
                    trend_filter_extreme_sigma=self.trend_filter_extreme_sigma,
                    trend_slope_ratio=trend_slope_ratio,
                )
            if self.volume_confirmation and not volume_spike:
                status = str(volume_payload.get("volume_status") or "")
                if status in {"missing_current_volume", "insufficient_volume_history"} and not self.volume_allow_missing:
                    return self._hold("volume_data_unavailable", **volume_payload)
                if status == "below_threshold" and self.volume_require_spike:
                    return self._hold("volume_below_threshold", **volume_payload)
            extension_ratio = band_extension_ratio if self.entry_mode == "touch" else max(prev_extension_ratio, 0.0)
            band_score = min(1.0, max(0.0, extension_ratio) / 0.15)
            rsi_score = (
                min(1.0, max(0.0, (self.rsi_oversold - rsi) / max(self.rsi_oversold, 1.0)))
                if self.use_rsi_filter
                else min(1.0, abs(rsi - 50.0) / 50.0)
            )
            if self.entry_mode == "reentry":
                confidence = self.reentry_base_confidence
                confidence += rsi_score * self.reentry_rsi_bonus
                confidence += band_score * self.reentry_extension_confidence_weight
                if volume_spike:
                    confidence += self.volume_confidence_boost
                confidence = min(1.0, max(0.0, confidence))
            else:
                confidence = min(1.0, 0.2 + band_score * 0.55 + rsi_score * 0.25)
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "bollinger_bands",
                    "band": "lower",
                    "bb_window": self.bb_window,
                    "bb_std_dev": self.bb_std_dev,
                    "bb_midline": mean,
                    "upper": upper,
                    "lower": lower,
                    "rsi": rsi,
                    "rsi_method": self.rsi_method,
                    "use_rsi_filter": self.use_rsi_filter,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "take_profit_mode": self.take_profit_mode,
                    "midline_target_pips": midline_target_pips,
                    "rr_target_pips": rr_take_profit_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "distance_sigma": distance_sigma,
                    "entry_mode": self.entry_mode,
                    "trend_filter_mode": self.trend_filter_mode,
                    "trend_filter_override": trend_override,
                    "trend_ma": trend_ma,
                    "trend_slope_ratio": trend_slope_ratio,
                    "trend_slope_strict_threshold": self.trend_slope_strict_threshold,
                    "volume_spike": volume_spike,
                    **volume_payload,
                    "exit_hint": "close_on_bb_midline",
                },
            )

        if self.exit_on_midline:
            if distance_sigma <= self.exit_midline_tolerance_sigma:
                return self._hold(
                    "mean_reversion_target_reached",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    bb_std=std,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    exit_hint="close_on_bb_midline",
                    distance_sigma=distance_sigma,
                    exit_tolerance_sigma=self.exit_midline_tolerance_sigma,
                    trend_ma=trend_ma,
                )

        if last >= upper and not rsi_allows_sell:
            return self._hold(
                "rsi_filter_not_confirmed",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                rsi=rsi,
                rsi_method=self.rsi_method,
                expected_side="sell",
                trend_ma=trend_ma,
            )
        if last <= lower and not rsi_allows_buy:
            return self._hold(
                "rsi_filter_not_confirmed",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                rsi=rsi,
                rsi_method=self.rsi_method,
                expected_side="buy",
                trend_ma=trend_ma,
            )
        return self._hold(
            "no_signal",
            bb_midline=mean,
            upper=upper,
            lower=lower,
            rsi=rsi,
            rsi_method=self.rsi_method,
            bb_distance=bb_distance,
            band_extension_ratio=band_extension_ratio,
            max_band_extension_ratio=self.max_band_extension_ratio,
            entry_mode=self.entry_mode,
            pip_size=pip_size,
            pip_size_source=pip_size_source,
            distance_sigma=distance_sigma,
            take_profit_mode=self.take_profit_mode,
            midline_target_pips=midline_target_pips,
            rr_target_pips=rr_take_profit_pips,
            trend_ma=trend_ma,
            trend_slope_ratio=trend_slope_ratio,
            volume_spike=volume_spike,
            **volume_payload,
        )
