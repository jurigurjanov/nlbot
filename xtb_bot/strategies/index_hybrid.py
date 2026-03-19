from __future__ import annotations

from datetime import datetime, timezone
import math
from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema, zscore
from xtb_bot.strategies.base import Strategy, StrategyContext


class IndexHybridStrategy(Strategy):
    name = "index_hybrid"

    _INDEX_SYMBOLS = {
        "US100",
        "US500",
        "US30",
        "DE40",
        "UK100",
        "FRA40",
        "JP225",
        "EU50",
        "AUS200",
        "AU200",
    }

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.fast_ema_window = max(2, int(params.get("index_fast_ema_window", 34)))
        self.slow_ema_window = max(self.fast_ema_window + 1, int(params.get("index_slow_ema_window", 144)))
        self.donchian_window = max(2, int(params.get("index_donchian_window", 20)))
        self.zscore_window_configured = max(5, int(params.get("index_zscore_window", 30)))
        self.zscore_window = self.zscore_window_configured
        self.window_sync_mode = str(params.get("index_window_sync_mode", "auto")).strip().lower()
        if self.window_sync_mode not in {"auto", "warn", "off"}:
            self.window_sync_mode = "auto"
        self.zscore_donchian_ratio_target = max(
            1.0,
            float(params.get("index_zscore_donchian_ratio_target", 1.5)),
        )
        self.zscore_donchian_ratio_tolerance = max(
            0.0,
            float(params.get("index_zscore_donchian_ratio_tolerance", 0.2)),
        )
        self.zscore_window_target = max(
            5,
            int(math.floor((self.donchian_window * self.zscore_donchian_ratio_target) + 0.5)),
        )
        self.window_sync_status = "off"
        self.zscore_window_relative_diff = 0.0
        if self.window_sync_mode != "off":
            relative_diff = abs(self.zscore_window_configured - self.zscore_window_target) / max(
                float(self.zscore_window_target), 1e-9
            )
            self.zscore_window_relative_diff = relative_diff
            if relative_diff > self.zscore_donchian_ratio_tolerance:
                if self.window_sync_mode == "auto":
                    self.zscore_window = self.zscore_window_target
                    self.window_sync_status = "auto_adjusted"
                else:
                    self.window_sync_status = "mismatch_warning"
            else:
                self.window_sync_status = "aligned"
        self.zscore_threshold = max(0.1, float(params.get("index_zscore_threshold", 2.2)))
        self.auto_correct_regime_thresholds = self._as_bool(
            params.get("index_auto_correct_regime_thresholds", True),
            True,
        )
        self.require_context_tick_size = self._as_bool(
            params.get("index_require_context_tick_size", False),
            False,
        )
        self.trend_gap_threshold = max(0.0, float(params.get("index_trend_gap_threshold", 0.0006)))
        self.mean_reversion_gap_threshold = max(
            0.0,
            float(params.get("index_mean_reversion_gap_threshold", self.trend_gap_threshold * 0.7)),
        )
        self.atr_window = max(2, int(params.get("index_atr_window", 14)))
        self.trend_atr_pct_threshold = max(0.0, float(params.get("index_trend_atr_pct_threshold", 0.08)))
        self.mean_reversion_atr_pct_threshold = max(
            0.0, float(params.get("index_mean_reversion_atr_pct_threshold", 0.05))
        )
        self.regime_thresholds_auto_corrected = False
        self.regime_threshold_corrections: dict[str, tuple[float, float]] = {}
        if self.auto_correct_regime_thresholds:
            if self.trend_gap_threshold < self.mean_reversion_gap_threshold:
                self.regime_thresholds_auto_corrected = True
                self.regime_threshold_corrections["gap"] = (
                    self.trend_gap_threshold,
                    self.mean_reversion_gap_threshold,
                )
                self.trend_gap_threshold, self.mean_reversion_gap_threshold = (
                    self.mean_reversion_gap_threshold,
                    self.trend_gap_threshold,
                )
            if self.trend_atr_pct_threshold < self.mean_reversion_atr_pct_threshold:
                self.regime_thresholds_auto_corrected = True
                self.regime_threshold_corrections["atr_pct"] = (
                    self.trend_atr_pct_threshold,
                    self.mean_reversion_atr_pct_threshold,
                )
                self.trend_atr_pct_threshold, self.mean_reversion_atr_pct_threshold = (
                    self.mean_reversion_atr_pct_threshold,
                    self.trend_atr_pct_threshold,
                )
        self.zscore_mode = str(params.get("index_zscore_mode", "detrended")).strip().lower()
        if self.zscore_mode not in {"classic", "detrended"}:
            self.zscore_mode = "detrended"
        self.zscore_ema_window = max(2, int(params.get("index_zscore_ema_window", self.slow_ema_window)))
        self.mean_reversion_allow_breakout = self._as_bool(
            params.get("index_mean_reversion_allow_breakout", True),
            True,
        )
        self.mean_reversion_breakout_extreme_multiplier = max(
            1.0,
            float(params.get("index_mean_reversion_breakout_extreme_multiplier", 1.0)),
        )
        self.min_breakout_distance_ratio = max(
            0.0,
            float(params.get("index_min_breakout_distance_ratio", 0.0)),
        )
        self.min_channel_width_atr = max(
            0.0,
            float(params.get("index_min_channel_width_atr", 0.0)),
        )
        self.volume_confirmation = self._as_bool(
            params.get("index_volume_confirmation", False),
            False,
        )
        self.volume_window = max(2, int(params.get("index_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("index_min_volume_ratio", 1.4)))
        self.volume_min_samples = max(1, int(params.get("index_volume_min_samples", 8)))
        self.volume_allow_missing = self._as_bool(
            params.get("index_volume_allow_missing", True),
            True,
        )
        self.volume_require_spike = self._as_bool(
            params.get("index_volume_require_spike", False),
            False,
        )
        self.volume_confidence_boost = max(
            0.0,
            float(params.get("index_volume_confidence_boost", 0.08)),
        )
        self.trend_confidence_base = max(0.0, min(1.0, float(params.get("index_trend_confidence_base", 0.2))))
        self.trend_confidence_gap_weight = max(
            0.0, min(1.0, float(params.get("index_trend_confidence_gap_weight", 0.5)))
        )
        self.trend_confidence_breakout_weight = max(
            0.0, min(1.0, float(params.get("index_trend_confidence_breakout_weight", 0.3)))
        )
        self.trend_confidence_gap_norm = max(
            1e-9,
            float(params.get("index_trend_confidence_gap_norm", max(self.trend_gap_threshold, 0.0005))),
        )
        self.trend_confidence_breakout_norm = max(
            1e-9,
            float(params.get("index_trend_confidence_breakout_norm", 0.0005)),
        )

        # Legacy absolute SL/TP are preserved as hard minimums for compatibility.
        self.stop_loss_pips = max(0.1, float(params.get("index_stop_loss_pips", params.get("stop_loss_pips", 30.0))))
        self.take_profit_pips = max(
            0.1,
            float(params.get("index_take_profit_pips", params.get("take_profit_pips", 80.0))),
        )
        self.stop_loss_pct = max(0.0, float(params.get("index_stop_loss_pct", 0.5)))
        self.take_profit_pct = max(0.0, float(params.get("index_take_profit_pct", 1.5)))
        self.stop_atr_multiplier = max(0.1, float(params.get("index_stop_atr_multiplier", 2.0)))
        self.risk_reward_ratio = max(1.0, float(params.get("index_risk_reward_ratio", 3.0)))

        self.session_filter_enabled = self._as_bool(params.get("index_session_filter_enabled", True), True)
        self.trend_session_start_hour_utc = int(float(params.get("index_trend_session_start_hour_utc", 6))) % 24
        self.trend_session_end_hour_utc = int(float(params.get("index_trend_session_end_hour_utc", 22))) % 24
        self.mean_reversion_outside_trend_session = self._as_bool(
            params.get("index_mean_reversion_outside_trend_session", True),
            True,
        )
        self.regime_selection_mode = str(params.get("index_regime_selection_mode", "hard")).strip().lower()
        if self.regime_selection_mode not in {"hard", "fuzzy"}:
            self.regime_selection_mode = "hard"
        self.regime_trend_index_threshold = max(
            0.0,
            min(5.0, float(params.get("index_regime_trend_index_threshold", 0.8))),
        )
        self.regime_fallback_mode = str(params.get("index_regime_fallback_mode", "nearest")).strip().lower()
        if self.regime_fallback_mode not in {"blocked", "nearest", "trend_following", "mean_reversion"}:
            self.regime_fallback_mode = "nearest"
        self.volume_as_bonus_only = self._as_bool(
            params.get("index_volume_as_bonus_only", False),
            False,
        )

        self.min_history = (
            max(
                self.fast_ema_window,
                self.slow_ema_window,
                self.donchian_window,
                self.zscore_window,
                self.atr_window,
                self.zscore_ema_window,
            )
            + 2
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
    def _is_index_symbol(symbol: str) -> bool:
        upper = symbol.upper()
        if upper in IndexHybridStrategy._INDEX_SYMBOLS:
            return True
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return True
        return False

    def supports_symbol(self, symbol: str) -> bool:
        return self._is_index_symbol(symbol)

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _zscore(values: Sequence[float]) -> float:
        return zscore(values, min_std=0.0)

    def _zscore_detrended(self, values: Sequence[float]) -> float:
        if len(values) < 2:
            return 0.0
        alpha = 2.0 / (self.zscore_ema_window + 1.0) if self.zscore_ema_window > 1 else 1.0
        ema = float(values[0])
        mean = 0.0
        m2 = 0.0
        count = 0
        last_residual = 0.0
        for idx, raw in enumerate(values):
            price = float(raw)
            if idx > 0:
                ema = alpha * price + (1.0 - alpha) * ema
            residual = price - ema
            last_residual = residual
            count += 1
            delta = residual - mean
            mean += delta / float(count)
            m2 += delta * (residual - mean)
        if count <= 1:
            return 0.0
        variance = m2 / float(count)
        std = math.sqrt(max(variance, 0.0))
        if std <= 0:
            return 0.0
        return (last_residual - mean) / std

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

    def _trend_confidence(self, ema_gap_ratio: float, breakout_distance_ratio: float) -> float:
        gap_score = min(1.0, ema_gap_ratio / self.trend_confidence_gap_norm)
        breakout_score = min(1.0, max(0.0, breakout_distance_ratio) / self.trend_confidence_breakout_norm)
        raw = (
            self.trend_confidence_base
            + gap_score * self.trend_confidence_gap_weight
            + breakout_score * self.trend_confidence_breakout_weight
        )
        return min(1.0, max(0.0, raw))

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

    def _donchian_breakout(self, prices: Sequence[float]) -> tuple[bool, bool, float, float]:
        if len(prices) < (self.donchian_window + 1):
            return False, False, float(prices[-1]), float(prices[-1])
        reference = prices[-(self.donchian_window + 1) : -1]
        upper = float(max(reference))
        lower = float(min(reference))
        last = float(prices[-1])
        return last > upper, last < lower, upper, lower

    @staticmethod
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    def _trend_session_state(self, timestamps: Sequence[float]) -> tuple[bool | None, int | None]:
        if not timestamps:
            return None, None
        try:
            now = datetime.fromtimestamp(float(timestamps[-1]), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None, None
        hour = int(now.hour)
        return self._hour_in_window(hour, self.trend_session_start_hour_utc, self.trend_session_end_hour_utc), hour

    def _resolve_regime_state(self, ema_gap_ratio: float, atr_pct: float) -> dict[str, object]:
        trend_regime_strict = (
            ema_gap_ratio >= self.trend_gap_threshold
            and atr_pct >= self.trend_atr_pct_threshold
        )
        mean_reversion_regime_strict = (
            ema_gap_ratio <= self.mean_reversion_gap_threshold
            and atr_pct <= self.mean_reversion_atr_pct_threshold
        )
        trend_regime = trend_regime_strict
        mean_reversion_regime = mean_reversion_regime_strict
        fallback_applied = False
        fallback_selected: str | None = None
        trend_distance: float | None = None
        mean_reversion_distance: float | None = None
        trend_power = ema_gap_ratio / max(self.trend_gap_threshold, 1e-9)
        volatility_power = atr_pct / max(self.trend_atr_pct_threshold, 1e-9)
        regime_index = (trend_power + volatility_power) / 2.0

        if self.regime_selection_mode == "fuzzy":
            trend_regime = regime_index >= self.regime_trend_index_threshold
            mean_reversion_regime = not trend_regime
            fallback_selected = "trend_following" if trend_regime else "mean_reversion"
            fallback_applied = not trend_regime_strict and not mean_reversion_regime_strict
        elif not trend_regime and not mean_reversion_regime:
            if self.regime_fallback_mode == "trend_following":
                trend_regime = True
                fallback_applied = True
                fallback_selected = "trend_following"
            elif self.regime_fallback_mode == "mean_reversion":
                mean_reversion_regime = True
                fallback_applied = True
                fallback_selected = "mean_reversion"
            elif self.regime_fallback_mode == "nearest":
                trend_gap_deficit = max(0.0, self.trend_gap_threshold - ema_gap_ratio) / max(
                    self.trend_gap_threshold, 1e-9
                )
                trend_atr_deficit = max(0.0, self.trend_atr_pct_threshold - atr_pct) / max(
                    self.trend_atr_pct_threshold, 1e-9
                )
                trend_distance = math.hypot(trend_gap_deficit, trend_atr_deficit)

                mean_gap_excess = max(0.0, ema_gap_ratio - self.mean_reversion_gap_threshold) / max(
                    self.mean_reversion_gap_threshold, 1e-9
                )
                mean_atr_excess = max(0.0, atr_pct - self.mean_reversion_atr_pct_threshold) / max(
                    self.mean_reversion_atr_pct_threshold, 1e-9
                )
                mean_reversion_distance = math.hypot(mean_gap_excess, mean_atr_excess)

                fallback_applied = True
                if trend_distance <= mean_reversion_distance:
                    trend_regime = True
                    fallback_selected = "trend_following"
                else:
                    mean_reversion_regime = True
                    fallback_selected = "mean_reversion"

        return {
            "trend_regime": trend_regime,
            "mean_reversion_regime": mean_reversion_regime,
            "trend_regime_strict": trend_regime_strict,
            "mean_reversion_regime_strict": mean_reversion_regime_strict,
            "regime_fallback_applied": fallback_applied,
            "regime_fallback_selected": fallback_selected,
            "regime_distance_to_trend": trend_distance,
            "regime_distance_to_mean_reversion": mean_reversion_distance,
            "regime_selection_mode": self.regime_selection_mode,
            "regime_index": regime_index,
            "regime_trend_index_threshold": self.regime_trend_index_threshold,
            "trend_power": trend_power,
            "volatility_power": volatility_power,
        }

    def _dynamic_sl_tp(
        self,
        symbol: str,
        last_price: float,
        atr: float,
        tick_size: float | None,
    ) -> tuple[float, float, dict[str, float]]:
        pip_size = float(tick_size) if tick_size not in (None, 0.0) else self._pip_size(symbol)
        atr_pips = atr / max(pip_size, 1e-9)
        stop_from_atr = atr_pips * self.stop_atr_multiplier
        stop_from_pct = ((last_price * self.stop_loss_pct) / 100.0) / max(pip_size, 1e-9)
        take_from_pct = ((last_price * self.take_profit_pct) / 100.0) / max(pip_size, 1e-9)
        stop_loss_pips = max(self.stop_loss_pips, stop_from_atr, stop_from_pct)
        take_profit_pips = max(self.take_profit_pips, stop_loss_pips * self.risk_reward_ratio, take_from_pct)
        return (
            stop_loss_pips,
            take_profit_pips,
            {
                "atr_pips": atr_pips,
                "stop_from_atr_pips": stop_from_atr,
                "stop_from_pct_pips": stop_from_pct,
                "take_from_pct_pips": take_from_pct,
                "pip_size": pip_size,
            },
        )

    def _hold(
        self,
        reason: str,
        stop_loss_pips: float,
        take_profit_pips: float,
        extra: dict[str, object] | None = None,
    ) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "index_hybrid"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        if not self._is_index_symbol(ctx.symbol):
            return self._hold("not_index_symbol", self.stop_loss_pips, self.take_profit_pips)
        if self.require_context_tick_size and ctx.tick_size in (None, 0.0):
            return self._hold(
                "tick_size_unavailable",
                self.stop_loss_pips,
                self.take_profit_pips,
                {"index_require_context_tick_size": True},
            )

        prices = ctx.prices
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                self.stop_loss_pips,
                self.take_profit_pips,
                {"history": len(prices), "required_history": self.min_history},
            )

        fast_now = self._ema(prices, self.fast_ema_window)
        slow_now = self._ema(prices, self.slow_ema_window)
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), 1e-9)
        breakout_up, breakout_down, channel_upper, channel_lower = self._donchian_breakout(prices)
        last = float(prices[-1])
        z_values = prices[-self.zscore_window :]
        if self.zscore_mode == "classic":
            z = self._zscore(z_values)
        else:
            z = self._zscore_detrended(z_values)

        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return self._hold(
                "atr_unavailable",
                self.stop_loss_pips,
                self.take_profit_pips,
                {"atr_window": self.atr_window},
            )
        atr_pct = (atr / max(abs(last), 1e-9)) * 100.0

        stop_loss_pips, take_profit_pips, risk_meta = self._dynamic_sl_tp(
            ctx.symbol, last, atr, ctx.tick_size
        )
        trend_session, session_hour_utc = self._trend_session_state(ctx.timestamps)

        trend_session_allowed = True
        mean_reversion_session_allowed = True
        if self.session_filter_enabled and trend_session is not None:
            trend_session_allowed = trend_session
            if self.mean_reversion_outside_trend_session:
                mean_reversion_session_allowed = not trend_session

        regime_state = self._resolve_regime_state(ema_gap_ratio, atr_pct)
        trend_regime = bool(regime_state["trend_regime"])
        mean_reversion_regime = bool(regime_state["mean_reversion_regime"])
        channel_mid = (channel_upper + channel_lower) / 2.0
        channel_width = max(0.0, channel_upper - channel_lower)
        channel_width_atr = channel_width / max(atr, 1e-9)
        base_meta: dict[str, object] = {
            "zscore": z,
            "zscore_mode": self.zscore_mode,
            "zscore_window_configured": self.zscore_window_configured,
            "zscore_window_effective": self.zscore_window,
            "zscore_window_target": self.zscore_window_target,
            "window_sync_mode": self.window_sync_mode,
            "window_sync_status": self.window_sync_status,
            "zscore_donchian_ratio_target": self.zscore_donchian_ratio_target,
            "zscore_donchian_ratio_tolerance": self.zscore_donchian_ratio_tolerance,
            "zscore_window_relative_diff": self.zscore_window_relative_diff,
            "auto_correct_regime_thresholds": self.auto_correct_regime_thresholds,
            "regime_thresholds_auto_corrected": self.regime_thresholds_auto_corrected,
            "regime_threshold_corrections": self.regime_threshold_corrections,
            "trend_gap_threshold": self.trend_gap_threshold,
            "mean_reversion_gap_threshold": self.mean_reversion_gap_threshold,
            "trend_atr_pct_threshold": self.trend_atr_pct_threshold,
            "mean_reversion_atr_pct_threshold": self.mean_reversion_atr_pct_threshold,
            "index_regime_selection_mode": self.regime_selection_mode,
            "index_regime_trend_index_threshold": self.regime_trend_index_threshold,
            "index_regime_fallback_mode": self.regime_fallback_mode,
            "index_volume_as_bonus_only": self.volume_as_bonus_only,
            "index_require_context_tick_size": self.require_context_tick_size,
            "ema_gap_ratio": ema_gap_ratio,
            "atr_pct": atr_pct,
            "trend_regime": trend_regime,
            "mean_reversion_regime": mean_reversion_regime,
            "trend_regime_strict": regime_state["trend_regime_strict"],
            "mean_reversion_regime_strict": regime_state["mean_reversion_regime_strict"],
            "regime_fallback_applied": regime_state["regime_fallback_applied"],
            "regime_fallback_selected": regime_state["regime_fallback_selected"],
            "regime_distance_to_trend": regime_state["regime_distance_to_trend"],
            "regime_distance_to_mean_reversion": regime_state["regime_distance_to_mean_reversion"],
            "regime_selection_mode": regime_state["regime_selection_mode"],
            "regime_index": regime_state["regime_index"],
            "regime_trend_index_threshold": regime_state["regime_trend_index_threshold"],
            "trend_power": regime_state["trend_power"],
            "volatility_power": regime_state["volatility_power"],
            "trend_session_allowed": trend_session_allowed,
            "mean_reversion_session_allowed": mean_reversion_session_allowed,
            "session_hour_utc": session_hour_utc,
            "channel_upper": channel_upper,
            "channel_mid": channel_mid,
            "channel_lower": channel_lower,
            "channel_width": channel_width,
            "channel_width_atr": channel_width_atr,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "mean_reversion_allow_breakout": self.mean_reversion_allow_breakout,
            "mean_reversion_breakout_extreme_multiplier": self.mean_reversion_breakout_extreme_multiplier,
            "index_min_breakout_distance_ratio": self.min_breakout_distance_ratio,
            "index_min_channel_width_atr": self.min_channel_width_atr,
            "index_volume_confirmation": self.volume_confirmation,
            "index_volume_window": self.volume_window,
            "index_min_volume_ratio": self.min_volume_ratio,
            "index_volume_min_samples": self.volume_min_samples,
            "index_volume_allow_missing": self.volume_allow_missing,
            "index_volume_require_spike": self.volume_require_spike,
            "index_volume_confidence_boost": self.volume_confidence_boost,
            "fast_ema": fast_now,
            "slow_ema": slow_now,
            **risk_meta,
        }
        volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
        base_meta.update(volume_meta)

        if self.min_channel_width_atr > 0 and channel_width_atr < self.min_channel_width_atr:
            return self._hold(
                "channel_too_narrow",
                stop_loss_pips,
                take_profit_pips,
                base_meta,
            )

        if trend_regime and trend_session_allowed and fast_now > slow_now and breakout_up:
            breakout_distance_ratio = max(0.0, last - channel_upper) / max(abs(last), 1e-9)
            if breakout_distance_ratio < self.min_breakout_distance_ratio:
                return self._hold(
                    "breakout_too_shallow",
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "up",
                        "breakout_distance_ratio": breakout_distance_ratio,
                    },
                )
            if volume_hold_reason is not None and not self.volume_as_bonus_only:
                return self._hold(
                    volume_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "up",
                        "breakout_distance_ratio": breakout_distance_ratio,
                    },
                )
            confidence = self._trend_confidence(ema_gap_ratio, breakout_distance_ratio)
            confidence = min(1.0, confidence + volume_confidence_boost)
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "index_hybrid",
                    "regime": "trend_following",
                    "trend": "up",
                    "donchian_breakout": True,
                    "breakout_distance_ratio": breakout_distance_ratio,
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    "exit_hint": "close_if_price_reenters_channel_mid",
                    **base_meta,
                },
            )

        if trend_regime and trend_session_allowed and fast_now < slow_now and breakout_down:
            breakout_distance_ratio = max(0.0, channel_lower - last) / max(abs(last), 1e-9)
            if breakout_distance_ratio < self.min_breakout_distance_ratio:
                return self._hold(
                    "breakout_too_shallow",
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "down",
                        "breakout_distance_ratio": breakout_distance_ratio,
                    },
                )
            if volume_hold_reason is not None and not self.volume_as_bonus_only:
                return self._hold(
                    volume_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "down",
                        "breakout_distance_ratio": breakout_distance_ratio,
                    },
                )
            confidence = self._trend_confidence(ema_gap_ratio, breakout_distance_ratio)
            confidence = min(1.0, confidence + volume_confidence_boost)
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "index_hybrid",
                    "regime": "trend_following",
                    "trend": "down",
                    "donchian_breakout": True,
                    "breakout_distance_ratio": breakout_distance_ratio,
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    "exit_hint": "close_if_price_reenters_channel_mid",
                    **base_meta,
                },
            )

        if trend_regime and not trend_session_allowed and (breakout_up or breakout_down):
            return self._hold(
                "trend_blocked_by_session",
                stop_loss_pips,
                take_profit_pips,
                base_meta,
            )

        breakout_active = breakout_up or breakout_down
        breakout_extreme_ok = (
            abs(z) >= (self.zscore_threshold * self.mean_reversion_breakout_extreme_multiplier)
        )
        mean_reversion_breakout_allowed = (
            not breakout_active
            or self.mean_reversion_allow_breakout
            or breakout_extreme_ok
        )

        if mean_reversion_regime and mean_reversion_session_allowed and mean_reversion_breakout_allowed:
            if z <= -self.zscore_threshold:
                if volume_hold_reason is not None and not self.volume_as_bonus_only:
                    return self._hold(
                        volume_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                        },
                    )
                confidence = min(1.0, abs(z) / (self.zscore_threshold * 2.0))
                confidence = min(1.0, confidence + volume_confidence_boost)
                return Signal(
                    side=Side.BUY,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        "indicator": "index_hybrid",
                        "regime": "mean_reversion",
                        "trend": "flat",
                        "breakout_extreme_ok": breakout_extreme_ok,
                        "volume_confidence_boost_applied": volume_confidence_boost,
                        "exit_hint": "close_if_price_reaches_channel_mid_or_opposite_band",
                        **base_meta,
                    },
                )
            if z >= self.zscore_threshold:
                if volume_hold_reason is not None and not self.volume_as_bonus_only:
                    return self._hold(
                        volume_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                        },
                    )
                confidence = min(1.0, abs(z) / (self.zscore_threshold * 2.0))
                confidence = min(1.0, confidence + volume_confidence_boost)
                return Signal(
                    side=Side.SELL,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        "indicator": "index_hybrid",
                        "regime": "mean_reversion",
                        "trend": "flat",
                        "breakout_extreme_ok": breakout_extreme_ok,
                        "volume_confidence_boost_applied": volume_confidence_boost,
                        "exit_hint": "close_if_price_reaches_channel_mid_or_opposite_band",
                        **base_meta,
                    },
                )

        if mean_reversion_regime and not mean_reversion_session_allowed and abs(z) >= self.zscore_threshold:
            return self._hold(
                "mean_reversion_blocked_by_session",
                stop_loss_pips,
                take_profit_pips,
                base_meta,
            )

        if mean_reversion_regime and breakout_active and not mean_reversion_breakout_allowed:
            return self._hold(
                "mean_reversion_blocked_by_breakout",
                stop_loss_pips,
                take_profit_pips,
                {
                    **base_meta,
                    "breakout_extreme_ok": breakout_extreme_ok,
                },
            )

        if trend_regime or mean_reversion_regime:
            return self._hold(
                "waiting_for_trigger" if self.regime_selection_mode == "fuzzy" else "no_signal_in_active_regime",
                stop_loss_pips,
                take_profit_pips,
                base_meta,
            )

        return self._hold("regime_filter_blocked", stop_loss_pips, take_profit_pips, base_meta)
