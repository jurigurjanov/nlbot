from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from datetime import datetime, timedelta, timezone
from collections.abc import Sequence
import json
import math
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from xtb_bot.models import Side, Signal
from xtb_bot.numeric import (
    adx_from_close,
    atr_wilder,
    efficiency_ratio,
    ema_last_two,
    kama_last_two,
    kama_slope_atr,
    session_vwap_bands,
    tail_mean,
)
from xtb_bot.symbols import is_commodity_symbol, is_index_symbol, resolve_index_market
from xtb_bot.strategies.base import Strategy, StrategyContext


class MomentumStrategy(Strategy):
    name = "momentum"
    _INDEX_LOCAL_SESSION_SPECS: dict[str, tuple[str, int, int]] = {
        "america": ("America/New_York", 9 * 60 + 30, 16 * 60),
        "europe": ("Europe/London", 8 * 60, 17 * 60),
        "japan": ("Asia/Tokyo", 9 * 60, 15 * 60),
        "australia": ("Australia/Sydney", 10 * 60, 16 * 60),
    }
    _KAMA_INDEX_SLOPE_GATE_MULTIPLIER_BY_MARKET: dict[str, float] = {
        "japan": 0.33,
        "australia": 0.33,
    }
    _QUALITY_MIN_SLOPE_ATR_RATIO = 0.02
    _QUALITY_MIN_TREND_GAP_ATR = 0.10
    _QUALITY_MAX_PRICE_SLOW_GAP_ATR = 3.0
    _QUALITY_MAX_PULLBACK_ENTRY_GAP_ATR = 3.0
    _FRESH_CROSS_HTF_PENALTY = 0.04
    _FRESH_CROSS_VOLUME_PENALTY = 0.03
    _FRESH_CROSS_REGIME_ADX_PENALTY = 0.04
    _FRESH_CROSS_GAP_PENALTY = 0.03
    _FRESH_CROSS_VWAP_OVERSTRETCH_PENALTY = 0.03
    _SECONDARY_HTF_MISMATCH_PENALTY = 0.12
    _SECONDARY_HTF_MISMATCH_CONTINUATION_PENALTY = 0.18
    _SECONDARY_VOLUME_PENALTY = 0.10
    _SECONDARY_VOLUME_UNAVAILABLE_PENALTY = 0.12
    _SECONDARY_KAMA_PENALTY = 0.12
    _SECONDARY_REGIME_ADX_PENALTY = 0.12
    _SECONDARY_KAMA_FRESH_CROSS_PENALTY = 0.06

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.fast_window = int(params.get("fast_window", 8))
        self.slow_window = int(params.get("slow_window", 21))
        self.ma_type = str(params.get("momentum_ma_type", "sma")).strip().lower()
        if self.ma_type not in {"sma", "ema"}:
            self.ma_type = "sma"
        self.entry_mode = str(params.get("momentum_entry_mode", "cross_or_trend")).strip().lower()
        if self.entry_mode not in {"cross_only", "cross_or_trend"}:
            self.entry_mode = "cross_or_trend"
        self.confirm_bars = max(
            1,
            int(params.get("momentum_confirm_bars", params.get("confirm_bars", 1))),
        )
        self.low_tf_min_confirm_bars = max(
            1,
            int(params.get("momentum_low_tf_min_confirm_bars", 1)),
        )
        self.low_tf_max_confirm_bars = max(
            self.low_tf_min_confirm_bars,
            int(params.get("momentum_low_tf_max_confirm_bars", 1)),
        )
        self.high_tf_max_confirm_bars = max(
            1,
            int(params.get("momentum_high_tf_max_confirm_bars", 1)),
        )
        auto_confirm_raw = params.get("momentum_auto_confirm_by_timeframe", False)
        if isinstance(auto_confirm_raw, str):
            self.auto_confirm_by_timeframe = auto_confirm_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.auto_confirm_by_timeframe = bool(auto_confirm_raw)
        timeframe_raw = params.get("momentum_timeframe_sec")
        self.timeframe_sec = float(timeframe_raw) if timeframe_raw is not None else None
        if self.timeframe_sec is not None and self.timeframe_sec <= 0:
            self.timeframe_sec = None
        max_spread_raw = params.get("momentum_max_spread_pips", params.get("max_spread_pips"))
        self.max_spread_pips = float(max_spread_raw) if max_spread_raw is not None else None
        self.max_spread_pips_by_symbol = self._parse_spread_limits(
            params.get("momentum_max_spread_pips_by_symbol")
        )
        max_spread_to_stop_raw = params.get("momentum_max_spread_to_stop_ratio", 0.20)
        try:
            max_spread_to_stop = float(max_spread_to_stop_raw) if max_spread_to_stop_raw is not None else 0.0
        except (TypeError, ValueError):
            max_spread_to_stop = 0.0
        self.max_spread_to_stop_ratio = (
            max_spread_to_stop if math.isfinite(max_spread_to_stop) and max_spread_to_stop > 0 else None
        )
        max_spread_to_atr_raw = params.get("momentum_max_spread_to_atr_ratio", 0.40)
        try:
            max_spread_to_atr = float(max_spread_to_atr_raw) if max_spread_to_atr_raw is not None else 0.0
        except (TypeError, ValueError):
            max_spread_to_atr = 0.0
        self.max_spread_to_atr_ratio = (
            max_spread_to_atr if math.isfinite(max_spread_to_atr) and max_spread_to_atr > 0 else None
        )
        tick_size_guard_raw = params.get("momentum_require_context_tick_size", False)
        if isinstance(tick_size_guard_raw, str):
            self.require_context_tick_size = tick_size_guard_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.require_context_tick_size = bool(tick_size_guard_raw)
        self.base_stop_loss_pips = max(0.1, float(params.get("stop_loss_pips", 25.0)))
        self.base_take_profit_pips = max(0.1, float(params.get("take_profit_pips", 50.0)))
        default_atr_window = 14
        self.atr_window = max(2, int(params.get("momentum_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("momentum_atr_multiplier", 1.7)))
        self.min_stop_loss_pips = max(
            0.1,
            float(params.get("momentum_min_stop_loss_pips", self.base_stop_loss_pips)),
        )
        default_rr = self.base_take_profit_pips / max(self.base_stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
        self.risk_reward_ratio = max(1.0, float(params.get("momentum_risk_reward_ratio", default_rr)))
        self.min_take_profit_pips = max(
            0.1,
            float(params.get("momentum_min_take_profit_pips", self.base_take_profit_pips)),
        )
        low_tf_risk_raw = params.get("momentum_low_tf_risk_profile_enabled", True)
        if isinstance(low_tf_risk_raw, str):
            self.low_tf_risk_profile_enabled = low_tf_risk_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.low_tf_risk_profile_enabled = bool(low_tf_risk_raw)
        self.low_tf_max_timeframe_sec = max(
            1.0,
            float(params.get("momentum_low_tf_max_timeframe_sec", 5.0 * 60.0)),
        )
        self.low_tf_atr_multiplier_cap = max(
            0.1,
            float(params.get("momentum_low_tf_atr_multiplier_cap", 1.7)),
        )
        self.low_tf_risk_reward_ratio_cap = max(
            1.0,
            float(params.get("momentum_low_tf_risk_reward_ratio_cap", 2.0)),
        )
        self.low_tf_min_stop_loss_pips = max(
            0.1,
            float(params.get("momentum_low_tf_min_stop_loss_pips", 10.0)),
        )
        self.low_tf_min_take_profit_pips = max(
            self.low_tf_min_stop_loss_pips,
            float(params.get("momentum_low_tf_min_take_profit_pips", 18.0)),
        )
        self.low_tf_max_stop_loss_atr = max(
            0.1,
            float(params.get("momentum_low_tf_max_stop_loss_atr", 1.7)),
        )
        self.low_tf_max_take_profit_atr = max(
            0.1,
            float(params.get("momentum_low_tf_max_take_profit_atr", 3.4)),
        )
        self.min_relative_stop_pct = max(0.0, float(params.get("momentum_min_relative_stop_pct", 0.0008)))
        raw_max_price_slow_gap_atr = float(params.get("momentum_max_price_slow_gap_atr", 1.65))
        self.max_price_slow_gap_atr = min(
            self._QUALITY_MAX_PRICE_SLOW_GAP_ATR,
            max(0.1, raw_max_price_slow_gap_atr),
        )
        self.price_gap_mode = str(params.get("momentum_price_gap_mode", "wait_pullback")).strip().lower()
        if self.price_gap_mode not in {"block", "wait_pullback"}:
            self.price_gap_mode = "wait_pullback"
        raw_pullback_entry_max_gap_atr = float(
            params.get(
                "momentum_pullback_entry_max_gap_atr",
                self.max_price_slow_gap_atr,
            )
        )
        self.pullback_entry_max_gap_atr = min(
            self.max_price_slow_gap_atr,
            self._QUALITY_MAX_PULLBACK_ENTRY_GAP_ATR,
            max(0.0, raw_pullback_entry_max_gap_atr),
        )
        self.continuation_fast_ma_retest_atr_tolerance = max(
            0.0,
            float(params.get("momentum_continuation_fast_ma_retest_atr_tolerance", 0.20)),
        )
        self.continuation_confidence_cap = min(
            1.0,
            max(0.05, float(params.get("momentum_continuation_confidence_cap", 0.80))),
        )
        self.continuation_price_alignment_bonus_multiplier = min(
            1.0,
            max(0.0, float(params.get("momentum_continuation_price_alignment_bonus_multiplier", 0.40))),
        )
        self.continuation_late_penalty_multiplier = max(
            0.0,
            float(params.get("momentum_continuation_late_penalty_multiplier", 0.90)),
        )
        self.kama_gate_enabled = self._as_bool(params.get("momentum_kama_gate_enabled", False), False)
        self.kama_er_window = max(2, int(float(params.get("momentum_kama_er_window", 10))))
        self.kama_fast_window = max(2, int(float(params.get("momentum_kama_fast_window", 2))))
        self.kama_slow_window = max(
            self.kama_fast_window + 1,
            int(float(params.get("momentum_kama_slow_window", 30))),
        )
        self.kama_min_efficiency_ratio = max(
            0.0,
            min(1.0, float(params.get("momentum_kama_min_efficiency_ratio", 0.12))),
        )
        self.kama_min_slope_atr_ratio = max(
            0.0,
            float(params.get("momentum_kama_min_slope_atr_ratio", 0.05)),
        )
        self.vwap_filter_enabled = self._as_bool(params.get("momentum_vwap_filter_enabled", False), False)
        self.vwap_reclaim_required = self._as_bool(params.get("momentum_vwap_reclaim_required", True), True)
        self.vwap_min_session_bars = max(
            2,
            int(float(params.get("momentum_vwap_min_session_bars", 8))),
        )
        self.vwap_min_volume_samples = max(
            1,
            int(float(params.get("momentum_vwap_min_volume_samples", 8))),
        )
        self.vwap_min_volume_quality = max(
            0.0,
            min(1.0, float(params.get("momentum_vwap_min_volume_quality", 0.5))),
        )
        self.vwap_overstretch_sigma = max(
            1.0,
            float(params.get("momentum_vwap_overstretch_sigma", 2.0)),
        )
        self.confirm_gap_relief_per_bar = max(
            0.0,
            float(params.get("momentum_confirm_gap_relief_per_bar", 0.25)),
        )
        self.min_slope_atr_ratio = max(
            self._QUALITY_MIN_SLOPE_ATR_RATIO,
            float(params.get("momentum_min_slope_atr_ratio", 0.07)),
        )
        self.min_trend_gap_atr = max(
            self._QUALITY_MIN_TREND_GAP_ATR,
            float(params.get("momentum_min_trend_gap_atr", 0.12)),
        )
        fresh_cross_relief_raw = params.get("momentum_fresh_cross_filter_relief_enabled", False)
        self.fresh_cross_filter_relief_enabled = self._as_bool(fresh_cross_relief_raw, False)
        session_filter_raw = params.get("momentum_session_filter_enabled", False)
        self.session_filter_enabled = self._as_bool(session_filter_raw, False)
        self.session_start_hour_utc = int(float(params.get("momentum_session_start_hour_utc", 6))) % 24
        self.session_end_hour_utc = int(float(params.get("momentum_session_end_hour_utc", 22))) % 24
        self.entry_filters_by_symbol = self._parse_entry_filters_by_symbol(
            params.get("momentum_entry_filters_by_symbol"),
        )
        volume_check_raw = params.get("momentum_volume_confirmation", False)
        self.volume_confirmation = self._as_bool(volume_check_raw, False)
        self.volume_window = max(2, int(params.get("momentum_volume_window", 20)))
        self.volume_min_ratio = max(1.0, float(params.get("momentum_min_volume_ratio", 1.3)))
        self.volume_min_samples = max(1, int(params.get("momentum_volume_min_samples", 8)))
        volume_allow_missing_raw = params.get("momentum_volume_allow_missing", True)
        self.volume_allow_missing = self._as_bool(volume_allow_missing_raw, True)
        higher_tf_bias_raw = params.get("momentum_higher_tf_bias_enabled", False)
        self.higher_tf_bias_enabled = self._as_bool(higher_tf_bias_raw, False)
        self.higher_tf_bias_timeframe_sec = max(
            60.0,
            float(params.get("momentum_higher_tf_bias_timeframe_sec", 5.0 * 60.0)),
        )
        self.higher_tf_bias_fast_window = max(
            2,
            int(params.get("momentum_higher_tf_bias_fast_window", self.fast_window)),
        )
        self.higher_tf_bias_slow_window = max(
            self.higher_tf_bias_fast_window + 1,
            int(params.get("momentum_higher_tf_bias_slow_window", self.slow_window)),
        )
        higher_tf_bias_allow_missing_raw = params.get("momentum_higher_tf_bias_allow_missing", False)
        self.higher_tf_bias_allow_missing = self._as_bool(higher_tf_bias_allow_missing_raw, False)
        regime_filter_raw = params.get("momentum_regime_adx_filter_enabled", False)
        self.regime_adx_filter_enabled = self._as_bool(regime_filter_raw, False)
        self.regime_adx_window = max(2, int(params.get("momentum_regime_adx_window", 8)))
        self.regime_min_adx = max(0.0, float(params.get("momentum_regime_min_adx", 24.0)))
        self.regime_adx_hysteresis = max(0.0, float(params.get("momentum_regime_adx_hysteresis", 1.0)))
        regime_hysteresis_state_raw = params.get("momentum_regime_use_hysteresis_state", True)
        self.regime_use_hysteresis_state = self._as_bool(regime_hysteresis_state_raw, True)
        self._adx_regime_active_by_scope: dict[str, bool] = {}
        self._adx_scope_last_timestamp: dict[str, float] = {}
        self._ADX_SCOPE_MAX_ENTRIES = 512
        self._adx_scope_ttl_sec = max(6.0 * 60.0 * 60.0, float(self.timeframe_sec or 60.0) * 256.0)
        fast_ma_trailing_enabled_raw = params.get("momentum_fast_ma_trailing_enabled", False)
        self.fast_ma_trailing_enabled = self._as_bool(fast_ma_trailing_enabled_raw, False)
        fast_ma_trailing_use_closed_raw = params.get(
            "momentum_fast_ma_trailing_use_closed_candle",
            True,
        )
        self.fast_ma_trailing_use_closed_candle = self._as_bool(
            fast_ma_trailing_use_closed_raw,
            True,
        )
        fast_ma_trailing_tf_raw = params.get("momentum_fast_ma_trailing_timeframe_sec")
        self.fast_ma_trailing_timeframe_sec: float | None
        if fast_ma_trailing_tf_raw in (None, ""):
            self.fast_ma_trailing_timeframe_sec = None
        else:
            try:
                parsed_fast_ma_tf = float(fast_ma_trailing_tf_raw)
            except (TypeError, ValueError):
                parsed_fast_ma_tf = 0.0
            self.fast_ma_trailing_timeframe_sec = (
                parsed_fast_ma_tf if math.isfinite(parsed_fast_ma_tf) and parsed_fast_ma_tf > 0 else None
            )
        self.fast_ma_trailing_activation_r_multiple = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_activation_r_multiple", 0.8)),
        )
        self.fast_ma_trailing_activation_min_profit_pips = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_activation_min_profit_pips", 0.0)),
        )
        self.fast_ma_trailing_buffer_atr = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_buffer_atr", 0.15)),
        )
        self.fast_ma_trailing_buffer_pips = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_buffer_pips", 0.0)),
        )
        self.fast_ma_trailing_min_step_pips = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_min_step_pips", 0.5)),
        )
        self.fast_ma_trailing_update_cooldown_sec = max(
            0.0,
            float(params.get("momentum_fast_ma_trailing_update_cooldown_sec", 5.0)),
        )

        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(
            self.min_take_profit_pips,
            self.stop_loss_pips * self.risk_reward_ratio,
        )
        self.min_history = max(
            self.fast_window,
            self.slow_window + self.confirm_bars,
            self.atr_window + 1,
            (self.regime_adx_window + 2) if self.regime_adx_filter_enabled else 0,
        )

    @staticmethod
    def _prefix_sums(values: Sequence[float]) -> list[float]:
        prefix = [0.0]
        running = 0.0
        for value in values:
            running += float(value)
            prefix.append(running)
        return prefix

    @staticmethod
    def _sma_from_prefix(prefix: Sequence[float], end: int, window: int) -> float:
        return (float(prefix[end]) - float(prefix[end - window])) / float(window)

    def _atr(self, prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _adx(prices: Sequence[float], window: int) -> float | None:
        return adx_from_close(prices, window)

    def _adx_gate_passes(
        self,
        scope_key: str,
        adx: float,
        entry_threshold: float,
        exit_threshold: float,
    ) -> tuple[bool, bool, bool]:
        active_before = self._adx_regime_active_by_scope.get(scope_key, False)
        if not self.regime_use_hysteresis_state:
            passes = adx >= entry_threshold
            self._adx_regime_active_by_scope[scope_key] = passes
            return passes, active_before, passes
        if active_before:
            active_after = adx >= exit_threshold
            self._adx_regime_active_by_scope[scope_key] = active_after
            return active_after, active_before, active_after
        active_after = adx >= entry_threshold
        self._adx_regime_active_by_scope[scope_key] = active_after
        return active_after, active_before, active_after

    @staticmethod
    def _finite_timeframe_key(timeframe_sec: float | None) -> int:
        if timeframe_sec is None or not math.isfinite(timeframe_sec) or timeframe_sec <= 0:
            return 0
        return max(1, int(round(timeframe_sec)))

    def _regime_scope_key(
        self,
        symbol: str,
        timeframe_sec: float | None,
        entry_mode: str | None = None,
    ) -> str:
        key = str(symbol).strip().upper()
        timeframe_key = self._finite_timeframe_key(timeframe_sec)
        configured_timeframe_key = self._finite_timeframe_key(self.timeframe_sec)
        effective_entry_mode = str(entry_mode or self.entry_mode).strip().lower()
        return (
            f"{key}"
            f"|tf={timeframe_key}"
            f"|configured_tf={configured_timeframe_key}"
            f"|entry_mode={effective_entry_mode}"
            f"|ma={self.ma_type}"
            f"|confirm={self.confirm_bars}"
        )

    def _latest_context_timestamp_sec(self, ctx: StrategyContext) -> float | None:
        if not ctx.timestamps:
            return None
        try:
            return self._normalize_timestamp_seconds(ctx.timestamps[-1])
        except (TypeError, ValueError):
            return None

    def _maybe_reset_regime_scope_for_timestamp(self, scope_key: str, latest_timestamp_sec: float | None) -> None:
        if latest_timestamp_sec is None or not math.isfinite(latest_timestamp_sec):
            return
        stale_before_ts = latest_timestamp_sec - self._adx_scope_ttl_sec
        stale_keys = [
            key
            for key, scope_ts in self._adx_scope_last_timestamp.items()
            if scope_ts < stale_before_ts
        ]
        for stale_key in stale_keys:
            self._adx_scope_last_timestamp.pop(stale_key, None)
            self._adx_regime_active_by_scope.pop(stale_key, None)
        previous_ts = self._adx_scope_last_timestamp.get(scope_key)
        if previous_ts is not None and latest_timestamp_sec + FLOAT_COMPARISON_TOLERANCE < previous_ts:
            self._adx_regime_active_by_scope.pop(scope_key, None)
        self._adx_scope_last_timestamp[scope_key] = latest_timestamp_sec
        if len(self._adx_scope_last_timestamp) > self._ADX_SCOPE_MAX_ENTRIES:
            oldest_key = min(self._adx_scope_last_timestamp, key=self._adx_scope_last_timestamp.get)  # type: ignore[arg-type]
            self._adx_scope_last_timestamp.pop(oldest_key, None)
            self._adx_regime_active_by_scope.pop(oldest_key, None)

    def _refresh_adx_regime_state(
        self,
        scope_key: str,
        latest_timestamp_sec: float | None,
        adx: float,
        entry_threshold: float,
        exit_threshold: float,
    ) -> None:
        if not scope_key:
            return
        self._maybe_reset_regime_scope_for_timestamp(scope_key, latest_timestamp_sec)
        active_before = self._adx_regime_active_by_scope.get(scope_key, False)
        if not self.regime_use_hysteresis_state:
            active_after = adx >= entry_threshold
        elif active_before:
            active_after = adx >= exit_threshold
        else:
            active_after = adx >= entry_threshold
        self._adx_regime_active_by_scope[scope_key] = active_after

    def _sma_last_two(self, prices: Sequence[float], window: int) -> tuple[float, float] | None:
        if len(prices) < (window + 1):
            return None
        prefix = self._prefix_sums(prices)
        prev = self._sma_from_prefix(prefix, len(prices) - 1, window)
        now = self._sma_from_prefix(prefix, len(prices), window)
        return prev, now

    def _ma_last_two(self, prices: Sequence[float], window: int) -> tuple[float, float] | None:
        if len(prices) < (window + 1):
            return None
        if self.ma_type == "ema":
            return ema_last_two(prices, window)
        return self._sma_last_two(prices, window)

    def _resample_closes_to_timeframe(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
        target_timeframe_sec: float,
    ) -> list[float]:
        if target_timeframe_sec <= 0:
            return []
        sample_count = min(len(prices), len(timestamps))
        if sample_count < 2:
            return []
        closes: list[float] = []
        current_bucket: int | None = None
        current_close: float | None = None
        for idx in range(sample_count):
            try:
                price = float(prices[idx])
                ts = self._normalize_timestamp_seconds(timestamps[idx])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(price) or not math.isfinite(ts):
                continue
            bucket = int(ts // target_timeframe_sec)
            if current_bucket is None:
                current_bucket = bucket
            elif bucket != current_bucket:
                if current_close is not None:
                    closes.append(current_close)
                current_bucket = bucket
            current_close = price
        if current_close is not None:
            closes.append(current_close)
        return closes

    def _resample_closes_with_timestamps(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
        target_timeframe_sec: float,
    ) -> tuple[list[float], list[float]]:
        if target_timeframe_sec <= 0:
            return [], []
        sample_count = min(len(prices), len(timestamps))
        if sample_count < 2:
            return [], []
        closes: list[float] = []
        close_timestamps: list[float] = []
        current_bucket: int | None = None
        current_close: float | None = None
        current_close_ts: float | None = None
        for idx in range(sample_count):
            try:
                price = float(prices[idx])
                ts = self._normalize_timestamp_seconds(timestamps[idx])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(price) or not math.isfinite(ts):
                continue
            bucket = int(ts // target_timeframe_sec)
            if current_bucket is None:
                current_bucket = bucket
            elif bucket != current_bucket:
                if current_close is not None and current_close_ts is not None:
                    closes.append(current_close)
                    close_timestamps.append(current_close_ts)
                current_bucket = bucket
            current_close = price
            current_close_ts = ts
        if current_close is not None and current_close_ts is not None:
            closes.append(current_close)
            close_timestamps.append(current_close_ts)
        return closes, close_timestamps

    def _resample_volume_sums_to_timeframe(
        self,
        volumes: Sequence[object],
        timestamps: Sequence[float],
        target_timeframe_sec: float,
    ) -> tuple[list[float], list[float]]:
        if target_timeframe_sec <= 0:
            return [], []
        sample_count = min(len(volumes), len(timestamps))
        if sample_count < 2:
            return [], []
        bucket_volumes: list[float] = []
        bucket_timestamps: list[float] = []
        current_bucket: int | None = None
        current_volume_sum = 0.0
        current_bucket_ts: float | None = None
        for idx in range(sample_count):
            try:
                ts = self._normalize_timestamp_seconds(timestamps[idx])
                raw_volume = float(volumes[idx])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts):
                continue
            bucket = int(ts // target_timeframe_sec)
            if current_bucket is None:
                current_bucket = bucket
            elif bucket != current_bucket:
                if current_bucket_ts is not None:
                    bucket_volumes.append(current_volume_sum)
                    bucket_timestamps.append(current_bucket_ts)
                current_bucket = bucket
                current_volume_sum = 0.0
            current_bucket_ts = ts
            if math.isfinite(raw_volume) and raw_volume > 0.0:
                current_volume_sum += raw_volume
        if current_bucket_ts is not None:
            bucket_volumes.append(current_volume_sum)
            bucket_timestamps.append(current_bucket_ts)
        return bucket_volumes, bucket_timestamps

    @staticmethod
    def _session_start_timestamp_utc(latest_timestamp: float | None, session_start_hour_utc: int) -> float | None:
        if latest_timestamp is None or not math.isfinite(latest_timestamp):
            return None
        latest_dt = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
        session_start = latest_dt.replace(
            hour=int(session_start_hour_utc) % 24,
            minute=0,
            second=0,
            microsecond=0,
        )
        if latest_dt.hour < int(session_start_hour_utc) % 24:
            session_start -= timedelta(days=1)
        return session_start.timestamp()

    def _session_vwap_payload(
        self,
        *,
        symbol: str,
        prices: Sequence[float],
        timestamps: Sequence[float],
        volumes: Sequence[object],
        timeframe_sec: float | None,
        session_start_hour_utc: int,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "momentum_vwap_filter_enabled": self.vwap_filter_enabled,
            "momentum_vwap_reclaim_required": self.vwap_reclaim_required,
            "momentum_vwap_min_session_bars": self.vwap_min_session_bars,
            "momentum_vwap_min_volume_samples": self.vwap_min_volume_samples,
            "momentum_vwap_min_volume_quality": self.vwap_min_volume_quality,
            "momentum_vwap_overstretch_sigma": self.vwap_overstretch_sigma,
        }
        if not self.vwap_filter_enabled:
            payload["vwap_status"] = "disabled"
            return payload
        if not (is_index_symbol(symbol) or is_commodity_symbol(symbol)):
            payload["vwap_status"] = "unsupported_symbol"
            return payload
        resolved_timeframe_sec = timeframe_sec or self._infer_timeframe_sec(timestamps) or 60.0
        resolved_timeframe_sec = max(60.0, float(resolved_timeframe_sec))
        payload["vwap_timeframe_sec"] = resolved_timeframe_sec
        closes, close_timestamps = self._resample_closes_with_timestamps(
            prices,
            timestamps,
            resolved_timeframe_sec,
        )
        volume_sums, volume_timestamps = self._resample_volume_sums_to_timeframe(
            volumes,
            timestamps,
            resolved_timeframe_sec,
        )
        aligned_count = min(
            len(closes),
            len(close_timestamps),
            len(volume_sums),
            len(volume_timestamps),
        )
        if aligned_count <= 1:
            payload["vwap_status"] = "insufficient_resampled_history"
            return payload
        closes = closes[:aligned_count]
        close_timestamps = close_timestamps[:aligned_count]
        volume_sums = volume_sums[:aligned_count]
        if len(closes) > 1:
            closes = closes[:-1]
            close_timestamps = close_timestamps[:-1]
            volume_sums = volume_sums[:-1]
        payload["vwap_resampled_bars"] = len(closes)
        if len(closes) < self.vwap_min_session_bars:
            payload["vwap_status"] = "insufficient_session_bars"
            return payload
        session_start_ts = self._session_start_timestamp_utc(close_timestamps[-1], session_start_hour_utc)
        if session_start_ts is None:
            payload["vwap_status"] = "missing_session_anchor"
            return payload
        payload["vwap_session_start_ts"] = session_start_ts
        session_prices: list[float] = []
        session_volumes: list[float] = []
        for price, ts, volume_sum in zip(closes, close_timestamps, volume_sums):
            if ts < session_start_ts:
                continue
            session_prices.append(float(price))
            session_volumes.append(float(volume_sum))
        payload["vwap_session_bars"] = len(session_prices)
        if len(session_prices) < self.vwap_min_session_bars:
            payload["vwap_status"] = "insufficient_session_bars"
            return payload
        vwap_payload = session_vwap_bands(
            session_prices,
            session_volumes,
            band_multipliers=(1.0, self.vwap_overstretch_sigma),
        )
        if vwap_payload is None:
            payload["vwap_status"] = "volume_unavailable"
            return payload
        valid_volume_samples = int(round(float(vwap_payload.get("valid_samples", 0.0))))
        payload["vwap_valid_volume_samples"] = valid_volume_samples
        payload.update(vwap_payload)
        if valid_volume_samples < self.vwap_min_volume_samples:
            payload["vwap_status"] = "insufficient_volume_samples"
            return payload
        volume_quality = float(vwap_payload.get("volume_quality", 0.0))
        if volume_quality < self.vwap_min_volume_quality:
            payload["vwap_status"] = "poor_volume_quality"
            return payload
        sigma = float(vwap_payload.get("sigma", 0.0))
        vwap = float(vwap_payload["vwap"])
        payload["vwap_status"] = "ok"
        payload["vwap_upper_overstretch"] = vwap + sigma * self.vwap_overstretch_sigma
        payload["vwap_lower_overstretch"] = vwap - sigma * self.vwap_overstretch_sigma
        return payload

    def _evaluate_higher_tf_bias(
        self,
        ctx: StrategyContext,
        *,
        candidate_side: Side,
        timeframe_sec: float | None,
    ) -> tuple[bool, str | None, dict[str, object]]:
        payload: dict[str, object] = {
            "higher_tf_bias_enabled": self.higher_tf_bias_enabled,
            "higher_tf_bias_timeframe_sec": self.higher_tf_bias_timeframe_sec,
            "higher_tf_bias_fast_window": self.higher_tf_bias_fast_window,
            "higher_tf_bias_slow_window": self.higher_tf_bias_slow_window,
            "higher_tf_bias_allow_missing": self.higher_tf_bias_allow_missing,
            "timeframe_sec": timeframe_sec,
        }
        if not self.higher_tf_bias_enabled:
            payload["higher_tf_bias_status"] = "disabled"
            return True, None, payload
        if candidate_side == Side.HOLD:
            payload["higher_tf_bias_status"] = "not_applicable"
            return True, None, payload
        if timeframe_sec is not None and self.higher_tf_bias_timeframe_sec <= (timeframe_sec + FLOAT_COMPARISON_TOLERANCE):
            payload["higher_tf_bias_status"] = "target_not_higher_than_source"
            return True, None, payload
        if len(ctx.timestamps) < 2:
            payload["higher_tf_bias_status"] = "missing_timestamps"
            if self.higher_tf_bias_allow_missing:
                return True, None, payload
            return False, "higher_tf_bias_unavailable", payload

        higher_tf_closes = self._resample_closes_to_timeframe(
            ctx.prices,
            ctx.timestamps,
            self.higher_tf_bias_timeframe_sec,
        )
        payload["higher_tf_closes_len"] = len(higher_tf_closes)
        required_history = self.higher_tf_bias_slow_window + 1
        payload["higher_tf_required_history"] = required_history
        if len(higher_tf_closes) < required_history:
            payload["higher_tf_bias_status"] = "insufficient_higher_tf_history"
            if self.higher_tf_bias_allow_missing:
                return True, None, payload
            return False, "higher_tf_bias_unavailable", payload

        fast_pair = self._ma_last_two(higher_tf_closes, self.higher_tf_bias_fast_window)
        slow_pair = self._ma_last_two(higher_tf_closes, self.higher_tf_bias_slow_window)
        if fast_pair is None or slow_pair is None:
            payload["higher_tf_bias_status"] = "ma_unavailable"
            if self.higher_tf_bias_allow_missing:
                return True, None, payload
            return False, "higher_tf_bias_unavailable", payload
        fast_prev, fast_now = fast_pair
        slow_prev, slow_now = slow_pair

        bullish_bias = fast_now > slow_now and fast_now >= fast_prev and slow_now >= slow_prev
        bearish_bias = fast_now < slow_now and fast_now <= fast_prev and slow_now <= slow_prev
        aligned = bullish_bias if candidate_side == Side.BUY else bearish_bias
        payload.update(
            {
                "higher_tf_fast_ma": fast_now,
                "higher_tf_slow_ma": slow_now,
                "higher_tf_fast_prev": fast_prev,
                "higher_tf_slow_prev": slow_prev,
                "higher_tf_bullish_bias": bullish_bias,
                "higher_tf_bearish_bias": bearish_bias,
                "higher_tf_bias_aligned": aligned,
            }
        )
        if not aligned:
            payload["higher_tf_bias_status"] = "mismatch"
            return False, "higher_tf_bias_mismatch", payload
        payload["higher_tf_bias_status"] = "aligned"
        return True, None, payload

    @staticmethod
    def _required_indices(total: int, points: int) -> list[int]:
        start_idx = max(0, total - points)
        required = {total - 1, total - 2}
        required.update(range(start_idx, total))
        return sorted(idx for idx in required if idx >= 0)

    def _ma_snapshot_sma(
        self,
        prices: Sequence[float],
        points: int,
    ) -> tuple[float, float, float, float, list[float]]:
        total = len(prices)
        required_indices = self._required_indices(total, points)
        prefix = self._prefix_sums(prices)

        fast_map: dict[int, float] = {}
        slow_map: dict[int, float] = {}
        for idx in required_indices:
            end = idx + 1
            fast_map[idx] = self._sma_from_prefix(prefix, end, self.fast_window)
            slow_map[idx] = self._sma_from_prefix(prefix, end, self.slow_window)

        start_idx = max(0, total - points)
        diffs = [fast_map[idx] - slow_map[idx] for idx in range(start_idx, total)]
        fast_now = fast_map[total - 1]
        slow_now = slow_map[total - 1]
        fast_prev = fast_map[total - 2]
        slow_prev = slow_map[total - 2]
        return fast_now, slow_now, fast_prev, slow_prev, diffs

    def _ma_snapshot_ema(
        self,
        prices: Sequence[float],
        points: int,
    ) -> tuple[float, float, float, float, list[float]]:
        total = len(prices)
        required_indices = set(self._required_indices(total, points))
        alpha_fast = 2.0 / (self.fast_window + 1.0)
        alpha_slow = 2.0 / (self.slow_window + 1.0)

        ema_fast = float(prices[0])
        ema_slow = float(prices[0])
        fast_map: dict[int, float] = {}
        slow_map: dict[int, float] = {}
        if 0 in required_indices:
            fast_map[0] = ema_fast
            slow_map[0] = ema_slow
        for idx in range(1, total):
            price = float(prices[idx])
            ema_fast = alpha_fast * price + (1.0 - alpha_fast) * ema_fast
            ema_slow = alpha_slow * price + (1.0 - alpha_slow) * ema_slow
            if idx in required_indices:
                fast_map[idx] = ema_fast
                slow_map[idx] = ema_slow

        start_idx = max(0, total - points)
        diffs = [fast_map[idx] - slow_map[idx] for idx in range(start_idx, total)]
        fast_now = fast_map[total - 1]
        slow_now = slow_map[total - 1]
        fast_prev = fast_map[total - 2]
        slow_prev = slow_map[total - 2]
        return fast_now, slow_now, fast_prev, slow_prev, diffs

    def _ma_last(self, prices: Sequence[float], window: int) -> float | None:
        if len(prices) < window:
            return None
        if self.ma_type == "ema":
            alpha = 2.0 / (window + 1.0)
            ema = float(prices[0])
            for idx in range(1, len(prices)):
                price = float(prices[idx])
                ema = (alpha * price) + ((1.0 - alpha) * ema)
            return ema
        window_slice = [float(value) for value in prices[-window:]]
        return sum(window_slice) / float(window)

    def _fast_ma_trailing_value(
        self,
        ctx: StrategyContext,
        *,
        fallback_fast_ma: float,
        timeframe_sec: float | None,
    ) -> tuple[float, str]:
        if not self.fast_ma_trailing_enabled:
            return fallback_fast_ma, "disabled"
        if not self.fast_ma_trailing_use_closed_candle:
            return fallback_fast_ma, "signal_fast_ma"
        if len(ctx.timestamps) < 2:
            return fallback_fast_ma, "signal_fast_ma"
        target_timeframe_sec = (
            self.fast_ma_trailing_timeframe_sec
            if self.fast_ma_trailing_timeframe_sec is not None
            else timeframe_sec
        )
        if target_timeframe_sec is None or target_timeframe_sec <= 0:
            return fallback_fast_ma, "signal_fast_ma"
        closes = self._resample_closes_to_timeframe(
            ctx.prices,
            ctx.timestamps,
            target_timeframe_sec,
        )
        # Drop the currently forming bucket to avoid intrabar trailing noise.
        if len(closes) > 1:
            closes = closes[:-1]
        if len(closes) < self.fast_window:
            return fallback_fast_ma, "signal_fast_ma"
        trailing_fast_ma = self._ma_last(closes, self.fast_window)
        if trailing_fast_ma is None or not math.isfinite(trailing_fast_ma):
            return fallback_fast_ma, "signal_fast_ma"
        return trailing_fast_ma, "closed_candle_fast_ma"

    @staticmethod
    def _parse_spread_limits(raw: object) -> dict[str, float]:
        if raw is None:
            return {}
        payload = raw
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return {}
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return {}
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, float] = {}
        for key, value in payload.items():
            symbol = str(key).strip().upper()
            if not symbol:
                continue
            try:
                limit = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(limit) or limit <= 0:
                continue
            parsed[symbol] = limit
        return parsed

    def _parse_entry_filters_by_symbol(
        self,
        raw: object,
    ) -> dict[str, dict[str, float | bool | int | str]]:
        if raw is None:
            return {}
        payload = raw
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return {}
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return {}
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, dict[str, float | bool | int | str]] = {}
        for key, value in payload.items():
            symbol = str(key).strip().upper()
            if not symbol or not isinstance(value, dict):
                continue
            item: dict[str, float | bool | int | str] = {}
            raw_entry_mode = value.get("momentum_entry_mode")
            if raw_entry_mode is not None:
                entry_mode = str(raw_entry_mode).strip().lower()
                if entry_mode in {"cross_only", "cross_or_trend"}:
                    item["entry_mode"] = entry_mode
            raw_min_slope = value.get("momentum_min_slope_atr_ratio")
            if raw_min_slope is not None:
                try:
                    min_slope = float(raw_min_slope)
                except (TypeError, ValueError):
                    min_slope = float("nan")
                if math.isfinite(min_slope):
                    item["min_slope_atr_ratio"] = max(self._QUALITY_MIN_SLOPE_ATR_RATIO, min_slope)
            raw_min_gap = value.get("momentum_min_trend_gap_atr")
            if raw_min_gap is not None:
                try:
                    min_gap = float(raw_min_gap)
                except (TypeError, ValueError):
                    min_gap = float("nan")
                if math.isfinite(min_gap):
                    item["min_trend_gap_atr"] = max(self._QUALITY_MIN_TREND_GAP_ATR, min_gap)
            raw_max_gap = value.get("momentum_max_price_slow_gap_atr")
            if raw_max_gap is not None:
                try:
                    max_gap = float(raw_max_gap)
                except (TypeError, ValueError):
                    max_gap = float("nan")
                if math.isfinite(max_gap):
                    item["max_price_slow_gap_atr"] = min(
                        self._QUALITY_MAX_PRICE_SLOW_GAP_ATR,
                        max(0.1, max_gap),
                    )
            raw_pullback_gap = value.get("momentum_pullback_entry_max_gap_atr")
            if raw_pullback_gap is not None:
                try:
                    pullback_gap = float(raw_pullback_gap)
                except (TypeError, ValueError):
                    pullback_gap = float("nan")
                if math.isfinite(pullback_gap):
                    item["pullback_entry_max_gap_atr"] = min(
                        self._QUALITY_MAX_PULLBACK_ENTRY_GAP_ATR,
                        max(0.0, pullback_gap),
                    )
            raw_continuation_retest = value.get("momentum_continuation_fast_ma_retest_atr_tolerance")
            if raw_continuation_retest is not None:
                try:
                    continuation_retest = float(raw_continuation_retest)
                except (TypeError, ValueError):
                    continuation_retest = float("nan")
                if math.isfinite(continuation_retest):
                    item["continuation_fast_ma_retest_atr_tolerance"] = max(0.0, continuation_retest)
            if "momentum_session_filter_enabled" in value:
                item["session_filter_enabled"] = self._as_bool(
                    value.get("momentum_session_filter_enabled"),
                    self.session_filter_enabled,
                )
            raw_session_start = value.get("momentum_session_start_hour_utc")
            if raw_session_start is not None:
                try:
                    item["session_start_hour_utc"] = int(float(raw_session_start)) % 24
                except (TypeError, ValueError):
                    pass
            raw_session_end = value.get("momentum_session_end_hour_utc")
            if raw_session_end is not None:
                try:
                    item["session_end_hour_utc"] = int(float(raw_session_end)) % 24
                except (TypeError, ValueError):
                    pass
            if item:
                parsed[symbol] = item
        return parsed

    def _entry_filters_for_symbol(self, symbol: str) -> dict[str, object]:
        symbol_key = str(symbol).strip().upper()
        overrides = self.entry_filters_by_symbol.get(symbol_key, {})
        max_price_slow_gap_atr = float(
            overrides.get("max_price_slow_gap_atr", self.max_price_slow_gap_atr),
        )
        pullback_entry_max_gap_atr = float(
            overrides.get("pullback_entry_max_gap_atr", self.pullback_entry_max_gap_atr),
        )
        return {
            "symbol": symbol_key,
            "entry_filter_overrides_applied": bool(overrides),
            "entry_filter_override_keys": sorted(overrides.keys()),
            "entry_mode": str(overrides.get("entry_mode", self.entry_mode)),
            "entry_mode_override_applied": "entry_mode" in overrides,
            "min_slope_atr_ratio": float(overrides.get("min_slope_atr_ratio", self.min_slope_atr_ratio)),
            "min_trend_gap_atr": float(overrides.get("min_trend_gap_atr", self.min_trend_gap_atr)),
            "max_price_slow_gap_atr": max_price_slow_gap_atr,
            "pullback_entry_max_gap_atr": min(
                max_price_slow_gap_atr,
                pullback_entry_max_gap_atr,
            ),
            "continuation_fast_ma_retest_atr_tolerance": float(
                overrides.get(
                    "continuation_fast_ma_retest_atr_tolerance",
                    self.continuation_fast_ma_retest_atr_tolerance,
                )
            ),
            "session_filter_enabled": bool(overrides.get("session_filter_enabled", self.session_filter_enabled)),
            "session_start_hour_utc": int(overrides.get("session_start_hour_utc", self.session_start_hour_utc)) % 24,
            "session_end_hour_utc": int(overrides.get("session_end_hour_utc", self.session_end_hour_utc)) % 24,
        }

    def _spread_limit_for_symbol(self, symbol: str) -> tuple[float | None, str]:
        upper_symbol = str(symbol).strip().upper()
        override = self.max_spread_pips_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return self.max_spread_pips, "global"

    @staticmethod
    def _linear_penalty(value: float, start: float, end: float, max_penalty: float) -> float:
        if max_penalty <= 0:
            return 0.0
        if value <= start:
            return 0.0
        if end <= start:
            return max_penalty
        return min(max_penalty, ((value - start) / (end - start)) * max_penalty)

    @classmethod
    def _confidence(
        cls,
        ema_gap_atr: float,
        slope_strength_atr: float,
        price_aligned_with_fast_ma: bool,
        *,
        price_slow_gap_atr: float = 0.0,
        is_trend_continuation_entry: bool = False,
        continuation_confidence_cap: float = 1.0,
        continuation_price_alignment_bonus_multiplier: float = 1.0,
        continuation_late_penalty_multiplier: float = 1.0,
    ) -> float:
        # Velocity-driven base score with explicit late-entry penalties.
        gap_component = min(1.0, max(0.0, ema_gap_atr) / 1.1) * 0.25
        slope_component = min(1.0, max(0.0, slope_strength_atr) / 0.1) * 0.45
        price_bonus = 0.15 if price_aligned_with_fast_ma else 0.0
        confidence_cap = 1.0
        if is_trend_continuation_entry:
            price_bonus *= continuation_price_alignment_bonus_multiplier
            confidence_cap = continuation_confidence_cap
        base = 0.16 + gap_component + slope_component + price_bonus

        # Penalize stretched entries (price too far from slow MA).
        extension_penalty = cls._linear_penalty(
            max(0.0, price_slow_gap_atr),
            1.35,
            3.0,
            0.28,
        )
        # Penalize very wide EMA separations that often appear late in the move.
        gap_overshoot_penalty = cls._linear_penalty(
            max(0.0, ema_gap_atr),
            1.35,
            2.6,
            0.10,
        )
        trend_late_penalty = 0.0
        if is_trend_continuation_entry:
            trend_late_penalty = cls._linear_penalty(
                max(0.0, price_slow_gap_atr),
                1.7,
                3.0,
                0.12 * continuation_late_penalty_multiplier,
            )

        return max(0.0, min(confidence_cap, base - extension_penalty - gap_overshoot_penalty - trend_late_penalty))

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

    def _resolve_timeframe_sec(self, ctx: StrategyContext) -> float | None:
        if self.timeframe_sec is not None:
            return self.timeframe_sec
        return self._infer_timeframe_sec(ctx.timestamps)

    def _is_low_timeframe_for_risk(self, timeframe_sec: float | None) -> bool:
        if not self.low_tf_risk_profile_enabled:
            return False
        if timeframe_sec is None:
            return False
        return timeframe_sec <= self.low_tf_max_timeframe_sec

    @staticmethod
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    @staticmethod
    def _minute_in_window(minute_of_day: int, start_minute: int, end_minute: int) -> bool:
        current = minute_of_day % (24 * 60)
        start = start_minute % (24 * 60)
        end = end_minute % (24 * 60)
        if start == end:
            return True
        if start < end:
            return start <= current < end
        return current >= start or current < end

    @staticmethod
    def _normalize_timestamp_seconds(raw_ts: object) -> float:
        return Strategy._timestamp_to_seconds(raw_ts)

    def _session_allows_entry(
        self,
        ctx: StrategyContext,
        *,
        session_filter_enabled: bool | None = None,
        session_start_hour_utc: int | None = None,
        session_end_hour_utc: int | None = None,
    ) -> tuple[bool, dict[str, object]]:
        effective_filter_enabled = self.session_filter_enabled if session_filter_enabled is None else bool(
            session_filter_enabled,
        )
        effective_start_hour_utc = (
            self.session_start_hour_utc
            if session_start_hour_utc is None
            else int(session_start_hour_utc) % 24
        )
        effective_end_hour_utc = (
            self.session_end_hour_utc
            if session_end_hour_utc is None
            else int(session_end_hour_utc) % 24
        )

        if not effective_filter_enabled:
            return True, {"session_filter_enabled": False}
        if not ctx.timestamps:
            return False, {
                "session_filter_enabled": True,
                "reason": "missing_timestamps",
                "session_start_hour_utc": effective_start_hour_utc,
                "session_end_hour_utc": effective_end_hour_utc,
            }
        try:
            timestamp_sec = self._normalize_timestamp_seconds(ctx.timestamps[-1])
            latest_dt_utc = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
            hour_utc = latest_dt_utc.hour
        except (ValueError, OSError, OverflowError):
            return False, {
                "session_filter_enabled": True,
                "reason": "invalid_timestamp",
                "session_start_hour_utc": effective_start_hour_utc,
                "session_end_hour_utc": effective_end_hour_utc,
            }
        index_market = resolve_index_market(ctx.symbol) if is_index_symbol(ctx.symbol) else None
        if index_market is not None:
            session_spec = self._INDEX_LOCAL_SESSION_SPECS.get(index_market)
            if session_spec is not None:
                timezone_name, start_minute, end_minute = session_spec
                payload: dict[str, object] = {
                    "session_filter_enabled": True,
                    "session_mode": "index_local_market_hours",
                    "session_profile": index_market,
                    "session_timezone": timezone_name,
                    "session_start_minute_local": start_minute,
                    "session_end_minute_local": end_minute,
                    "session_hour_utc": hour_utc,
                }
                try:
                    zone = ZoneInfo(timezone_name)
                except ZoneInfoNotFoundError:
                    zone = timezone.utc
                    payload["session_timezone_fallback_utc"] = True
                local_dt = latest_dt_utc.astimezone(zone)
                local_minute = local_dt.hour * 60 + local_dt.minute
                payload["session_local_hour"] = local_dt.hour
                payload["session_local_minute"] = local_dt.minute
                allowed = self._minute_in_window(local_minute, start_minute, end_minute)
                return allowed, payload
        allowed = self._hour_in_window(hour_utc, effective_start_hour_utc, effective_end_hour_utc)
        return allowed, {
            "session_filter_enabled": True,
            "session_mode": "utc_hour_window",
            "session_hour_utc": hour_utc,
            "session_start_hour_utc": effective_start_hour_utc,
            "session_end_hour_utc": effective_end_hour_utc,
        }

    def _effective_confirm_bars(self, timeframe_sec: float | None) -> int:
        bars = self.confirm_bars
        if not self.auto_confirm_by_timeframe or timeframe_sec is None:
            return bars
        if timeframe_sec <= 5 * 60:
            bars = max(bars, self.low_tf_min_confirm_bars)
            return min(bars, self.low_tf_max_confirm_bars)
        if timeframe_sec >= 60 * 60:
            return min(bars, self.high_tf_max_confirm_bars)
        return bars

    def _hold_with_reason(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "ma_cross"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=payload,
        )

    def _kama_gate_payload(
        self,
        symbol: str,
        prices: Sequence[float],
        atr: float,
        side: Side,
        *,
        min_slope_atr_ratio_hint: float | None = None,
    ) -> tuple[bool, dict[str, object]]:
        symbol_upper = str(symbol).strip().upper()
        index_market = resolve_index_market(symbol_upper) if is_index_symbol(symbol_upper) else None
        effective_min_slope_atr_ratio = self.kama_min_slope_atr_ratio
        effective_threshold_source = "configured"
        if index_market is not None and min_slope_atr_ratio_hint is not None:
            market_multiplier = self._KAMA_INDEX_SLOPE_GATE_MULTIPLIER_BY_MARKET.get(index_market)
            if market_multiplier is not None:
                adaptive_min_slope = max(
                    self._QUALITY_MIN_SLOPE_ATR_RATIO,
                    float(min_slope_atr_ratio_hint) * market_multiplier,
                )
                effective_min_slope_atr_ratio = min(self.kama_min_slope_atr_ratio, adaptive_min_slope)
                effective_threshold_source = f"{index_market}_index_entry_profile"
        payload: dict[str, object] = {
            "momentum_kama_gate_enabled": self.kama_gate_enabled,
            "momentum_kama_er_window": self.kama_er_window,
            "momentum_kama_fast_window": self.kama_fast_window,
            "momentum_kama_slow_window": self.kama_slow_window,
            "momentum_kama_min_efficiency_ratio": self.kama_min_efficiency_ratio,
            "momentum_kama_min_slope_atr_ratio": self.kama_min_slope_atr_ratio,
            "momentum_kama_min_slope_atr_ratio_effective": effective_min_slope_atr_ratio,
            "momentum_kama_min_slope_atr_ratio_source": effective_threshold_source,
        }
        if not self.kama_gate_enabled:
            payload["kama_gate_status"] = "disabled"
            return True, payload
        er = efficiency_ratio(prices, self.kama_er_window)
        kama_prev, kama_now = kama_last_two(
            prices,
            self.kama_er_window,
            fast_window=self.kama_fast_window,
            slow_window=self.kama_slow_window,
        )
        payload["kama_efficiency_ratio"] = er
        payload["kama_previous"] = kama_prev
        payload["kama_current"] = kama_now
        if er is None or kama_prev is None or kama_now is None or atr <= 0:
            payload["kama_gate_status"] = "unavailable"
            return True, payload
        raw_slope_atr_ratio = kama_slope_atr(
            prices,
            self.kama_er_window,
            atr,
            fast_window=self.kama_fast_window,
            slow_window=self.kama_slow_window,
        )
        directional_slope_atr_ratio = raw_slope_atr_ratio if side == Side.BUY else -raw_slope_atr_ratio
        payload["kama_slope_atr_ratio"] = raw_slope_atr_ratio
        payload["kama_directional_slope_atr_ratio"] = directional_slope_atr_ratio
        passes = (
            er >= self.kama_min_efficiency_ratio
            and directional_slope_atr_ratio >= effective_min_slope_atr_ratio
        )
        payload["kama_gate_status"] = "ok" if passes else "chop"
        return passes, payload

    def _volume_check(
        self,
        ctx: StrategyContext,
    ) -> tuple[bool, dict[str, object]]:
        index_missing_volume_relaxed = is_index_symbol(ctx.symbol)
        effective_volume_allow_missing = self.volume_allow_missing or index_missing_volume_relaxed
        payload: dict[str, object] = {
            "volume_confirmation": self.volume_confirmation,
            "volume_window": self.volume_window,
            "volume_min_ratio": self.volume_min_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_allow_missing_effective": effective_volume_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not self.volume_allow_missing,
        }
        if not self.volume_confirmation:
            payload["volume_check_status"] = "disabled"
            return True, payload

        current_volume = self._coerce_positive_finite(ctx.current_volume)
        history = self._extract_positive_finite_values(ctx.volumes)
        payload["volume_history_len"] = len(history)
        payload["current_volume"] = current_volume

        if current_volume is None or current_volume <= 0:
            payload["volume_check_status"] = "missing_current_volume"
            return effective_volume_allow_missing, payload

        volume_current_source = "context_current_volume"
        volume_current_deduplicated = False
        if history and self._values_close(history[-1], current_volume):
            current_volume = history[-1]
            history = history[:-1]
            volume_current_source = "ctx_volumes_last"
            volume_current_deduplicated = True
        payload["volume_current_source"] = volume_current_source
        payload["volume_current_deduplicated"] = volume_current_deduplicated
        payload["volume_baseline_history_len"] = len(history)

        lookback = min(self.volume_window, len(history))
        if lookback < self.volume_min_samples:
            payload["volume_check_status"] = "insufficient_volume_history"
            payload["volume_lookback"] = lookback
            return effective_volume_allow_missing, payload

        avg_volume = tail_mean(history, lookback)
        threshold = avg_volume * self.volume_min_ratio
        payload["volume_check_status"] = "ok" if current_volume >= threshold else "below_threshold"
        payload["avg_volume"] = avg_volume
        payload["min_required_volume"] = threshold
        payload["volume_lookback"] = lookback
        payload["volume_ratio"] = current_volume / max(avg_volume, FLOAT_COMPARISON_TOLERANCE)
        return current_volume >= threshold, payload

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        prices, invalid_prices = self._extract_finite_prices(ctx.prices)
        timestamps = self._extract_finite_timestamps(
            ctx.timestamps,
            timestamp_normalizer=self._normalize_timestamp_seconds,
        )
        ctx = StrategyContext(
            symbol=ctx.symbol,
            prices=prices,
            timestamps=timestamps,
            volumes=ctx.volumes,
            current_volume=ctx.current_volume,
            current_spread_pips=ctx.current_spread_pips,
            tick_size=ctx.tick_size,
            pip_size=ctx.pip_size,
        )
        prices = ctx.prices
        timeframe_sec = self._resolve_timeframe_sec(ctx)
        entry_filters = self._entry_filters_for_symbol(ctx.symbol)
        entry_mode_effective = str(entry_filters["entry_mode"]).strip().lower()
        regime_scope_key = self._regime_scope_key(ctx.symbol, timeframe_sec, entry_mode_effective)
        latest_timestamp_sec = self._latest_context_timestamp_sec(ctx)
        regime_adx_entry_threshold: float | None = None
        regime_adx_exit_threshold: float | None = None
        regime_adx_value_for_state: float | None = None
        effective_confirm_bars = self._effective_confirm_bars(timeframe_sec)
        required_history = max(
            self.fast_window,
            self.slow_window + effective_confirm_bars,
            self.atr_window + 1,
        )
        if len(prices) < required_history:
            return self._hold_with_reason(
                "insufficient_price_history",
                {
                    "history": len(prices),
                    "required_history": required_history,
                    "invalid_prices_dropped": invalid_prices,
                    "configured_confirm_bars": self.confirm_bars,
                    "effective_confirm_bars": effective_confirm_bars,
                    "timeframe_sec": timeframe_sec,
                },
            )
        if self.require_context_tick_size and not self._has_context_pip_size(ctx):
            return self._hold_with_reason(
                "tick_size_unavailable",
                {
                    "momentum_require_context_tick_size": True,
                },
            )
        if self.regime_adx_filter_enabled:
            regime_adx_entry_threshold = self.regime_min_adx + self.regime_adx_hysteresis
            regime_adx_exit_threshold = max(0.0, self.regime_min_adx - self.regime_adx_hysteresis)
            regime_adx_value_for_state = self._adx(prices, self.regime_adx_window)
            if regime_adx_value_for_state is not None:
                self._refresh_adx_regime_state(
                    regime_scope_key,
                    latest_timestamp_sec,
                    regime_adx_value_for_state,
                    regime_adx_entry_threshold,
                    regime_adx_exit_threshold,
                )

        spread_limit, spread_limit_source = self._spread_limit_for_symbol(ctx.symbol)
        if (
            spread_limit is not None
            and ctx.current_spread_pips is not None
            and ctx.current_spread_pips > spread_limit
        ):
            return self._hold_with_reason(
                "spread_too_wide",
                {
                    "spread_pips": ctx.current_spread_pips,
                    "max_spread_pips": spread_limit,
                    "max_spread_pips_source": spread_limit_source,
                },
            )

        min_slope_atr_ratio_effective = float(entry_filters["min_slope_atr_ratio"])
        min_trend_gap_atr_effective = float(entry_filters["min_trend_gap_atr"])
        max_price_slow_gap_atr_effective_base = float(entry_filters["max_price_slow_gap_atr"])
        pullback_entry_max_gap_atr_effective_base = float(entry_filters["pullback_entry_max_gap_atr"])

        session_allowed, session_payload = self._session_allows_entry(
            ctx,
            session_filter_enabled=bool(entry_filters["session_filter_enabled"]),
            session_start_hour_utc=int(entry_filters["session_start_hour_utc"]),
            session_end_hour_utc=int(entry_filters["session_end_hour_utc"]),
        )
        if not session_allowed:
            return self._hold_with_reason("outside_trading_session", session_payload)

        if self.ma_type == "ema":
            fast_now, slow_now, fast_prev, slow_prev, diffs = self._ma_snapshot_ema(
                prices, effective_confirm_bars + 1
            )
        else:
            fast_now, slow_now, fast_prev, slow_prev, diffs = self._ma_snapshot_sma(
                prices, effective_confirm_bars + 1
            )
        diff_now = diffs[-1]
        diff_prev = diffs[-2] if len(diffs) >= 2 else None
        current_price = float(prices[-1])
        fast_slope = fast_now - fast_prev
        slow_slope = slow_now - slow_prev
        bullish_gap_expanding = diff_prev is not None and diff_now > diff_prev
        bearish_gap_expanding = diff_prev is not None and diff_now < diff_prev

        bullish_cross_indices = [
            idx
            for idx in range(1, len(diffs))
            if diffs[idx - 1] <= 0 and diffs[idx] > 0
        ]
        bearish_cross_indices = [
            idx
            for idx in range(1, len(diffs))
            if diffs[idx - 1] >= 0 and diffs[idx] < 0
        ]
        latest_bullish_cross = bullish_cross_indices[-1] if bullish_cross_indices else None
        latest_bearish_cross = bearish_cross_indices[-1] if bearish_cross_indices else None

        bullish_cross_age = (
            len(diffs) - latest_bullish_cross
            if latest_bullish_cross is not None
            else None
        )
        bearish_cross_age = (
            len(diffs) - latest_bearish_cross
            if latest_bearish_cross is not None
            else None
        )
        bullish_cross_in_window = bullish_cross_age is not None
        bearish_cross_in_window = bearish_cross_age is not None
        required_cross_bars = 1 if effective_confirm_bars <= 1 else 2

        bullish_confirmed = (
            bullish_cross_age is not None
            and required_cross_bars <= bullish_cross_age <= effective_confirm_bars
            and all(diff > 0 for diff in diffs[latest_bullish_cross:])
        )
        bearish_confirmed = (
            bearish_cross_age is not None
            and required_cross_bars <= bearish_cross_age <= effective_confirm_bars
            and all(diff < 0 for diff in diffs[latest_bearish_cross:])
        )
        bullish_trend = (
            diff_prev is not None
            and diff_now > 0
            and diff_prev > 0
            and fast_now > fast_prev
            and slow_now >= slow_prev
        )
        bearish_trend = (
            diff_prev is not None
            and diff_now < 0
            and diff_prev < 0
            and fast_now < fast_prev
            and slow_now <= slow_prev
        )

        candidate_side = Side.HOLD
        trend_signal = "none"
        if bullish_confirmed:
            candidate_side = Side.BUY
            trend_signal = "ma_cross_up"
        elif bearish_confirmed:
            candidate_side = Side.SELL
            trend_signal = "ma_cross_down"
        elif entry_mode_effective == "cross_or_trend":
            if bullish_trend:
                candidate_side = Side.BUY
                trend_signal = "ma_trend_up"
            elif bearish_trend:
                candidate_side = Side.SELL
                trend_signal = "ma_trend_down"

        if candidate_side != Side.HOLD:
            is_fresh_cross_entry = trend_signal in {"ma_cross_up", "ma_cross_down"}
            is_trend_continuation_entry = trend_signal in {"ma_trend_up", "ma_trend_down"}
            fresh_cross_relief_active = self.fresh_cross_filter_relief_enabled and is_fresh_cross_entry
            higher_tf_penalty = 0.0
            volume_penalty = 0.0
            regime_adx_penalty = 0.0
            kama_penalty = 0.0
            gap_relief_penalty = 0.0
            vwap_penalty = 0.0
            soft_filter_reasons: list[str] = []
            higher_tf_bias_ok, higher_tf_bias_reason, higher_tf_bias_payload = self._evaluate_higher_tf_bias(
                ctx,
                candidate_side=candidate_side,
                timeframe_sec=timeframe_sec,
            )
            if not higher_tf_bias_ok and higher_tf_bias_reason is not None:
                if higher_tf_bias_reason == "higher_tf_bias_mismatch":
                    higher_tf_penalty = (
                        self._FRESH_CROSS_HTF_PENALTY
                        if fresh_cross_relief_active
                        else (
                            self._SECONDARY_HTF_MISMATCH_CONTINUATION_PENALTY
                            if is_trend_continuation_entry
                            else self._SECONDARY_HTF_MISMATCH_PENALTY
                        )
                    )
                    higher_tf_bias_payload["higher_tf_bias_relief_applied"] = True
                    higher_tf_bias_payload["higher_tf_bias_penalty"] = higher_tf_penalty
                    higher_tf_bias_payload["higher_tf_bias_softened"] = True
                    soft_filter_reasons.append(higher_tf_bias_reason)
                else:
                    higher_tf_bias_payload["higher_tf_bias_relief_applied"] = False
                    higher_tf_bias_payload["higher_tf_bias_penalty"] = 0.0
                    return self._hold_with_reason(higher_tf_bias_reason, higher_tf_bias_payload)
            else:
                higher_tf_bias_payload["higher_tf_bias_relief_applied"] = False
                higher_tf_bias_payload["higher_tf_bias_penalty"] = 0.0
                higher_tf_bias_payload["higher_tf_bias_softened"] = False

            volume_ok, volume_payload = self._volume_check(ctx)
            if not volume_ok:
                volume_penalty = (
                    self._FRESH_CROSS_VOLUME_PENALTY
                    if fresh_cross_relief_active
                    else (
                        self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                        if str(volume_payload.get("volume_check_status") or "").strip().lower()
                        in {"missing_current_volume", "insufficient_volume_history"}
                        else self._SECONDARY_VOLUME_PENALTY
                    )
                )
                volume_payload["volume_relief_applied"] = True
                volume_payload["volume_penalty"] = volume_penalty
                volume_payload["volume_softened"] = True
                soft_filter_reasons.append("volume_below_threshold")
            else:
                volume_payload["volume_relief_applied"] = False
                volume_payload["volume_penalty"] = 0.0
                volume_payload["volume_softened"] = False

            atr = self._atr(prices, self.atr_window)
            if atr is None or atr <= 0:
                return self._hold_with_reason("atr_unavailable", {"atr_window": self.atr_window})
            kama_gate_passes, kama_payload = self._kama_gate_payload(
                ctx.symbol,
                prices,
                atr,
                candidate_side,
                min_slope_atr_ratio_hint=min_slope_atr_ratio_effective,
            )
            if not kama_gate_passes:
                kama_penalty = (
                    self._SECONDARY_KAMA_FRESH_CROSS_PENALTY
                    if fresh_cross_relief_active
                    else self._SECONDARY_KAMA_PENALTY
                )
                kama_payload["kama_relief_applied"] = True
                kama_payload["kama_penalty"] = kama_penalty
                kama_payload["kama_softened"] = True
                soft_filter_reasons.append("kama_chop_regime")
            else:
                kama_payload["kama_relief_applied"] = False
                kama_payload["kama_penalty"] = 0.0
                kama_payload["kama_softened"] = False
            previous_price = float(prices[-2])
            vwap_payload = self._session_vwap_payload(
                symbol=ctx.symbol,
                prices=ctx.prices,
                timestamps=ctx.timestamps,
                volumes=ctx.volumes,
                timeframe_sec=timeframe_sec,
                session_start_hour_utc=int(entry_filters["session_start_hour_utc"]),
            )
            vwap_status = str(vwap_payload.get("vwap_status") or "")
            vwap_available = vwap_status == "ok"
            vwap_price = float(vwap_payload["vwap"]) if vwap_available else None
            vwap_upper_overstretch = (
                float(vwap_payload["vwap_upper_overstretch"])
                if vwap_available and vwap_payload.get("vwap_upper_overstretch") is not None
                else None
            )
            vwap_lower_overstretch = (
                float(vwap_payload["vwap_lower_overstretch"])
                if vwap_available and vwap_payload.get("vwap_lower_overstretch") is not None
                else None
            )
            buy_vwap_reclaim_confirmed = (
                vwap_price is not None
                and previous_price <= vwap_price
                and current_price >= vwap_price
            )
            sell_vwap_reclaim_confirmed = (
                vwap_price is not None
                and previous_price >= vwap_price
                and current_price <= vwap_price
            )
            if vwap_available:
                if candidate_side == Side.BUY:
                    if self.vwap_reclaim_required and current_price < vwap_price and not buy_vwap_reclaim_confirmed:
                        return self._hold_with_reason(
                            "below_vwap_no_reclaim",
                            {
                                "trend_signal": trend_signal,
                                "entry_mode": entry_mode_effective,
                                "configured_entry_mode": self.entry_mode,
                                "vwap_reclaim_confirmed": buy_vwap_reclaim_confirmed,
                                "vwap_bias_confirmed": current_price >= vwap_price,
                                **vwap_payload,
                            },
                        )
                    if (
                        vwap_upper_overstretch is not None
                        and current_price > vwap_upper_overstretch
                    ):
                        if is_trend_continuation_entry:
                            return self._hold_with_reason(
                                "vwap_overstretched_upper",
                                {
                                    "trend_signal": trend_signal,
                                    "entry_mode": entry_mode_effective,
                                    "configured_entry_mode": self.entry_mode,
                                    "vwap_reclaim_confirmed": buy_vwap_reclaim_confirmed,
                                    "vwap_bias_confirmed": current_price >= vwap_price,
                                    **vwap_payload,
                                },
                            )
                        if is_fresh_cross_entry:
                            vwap_penalty = self._FRESH_CROSS_VWAP_OVERSTRETCH_PENALTY
                else:
                    if self.vwap_reclaim_required and current_price > vwap_price and not sell_vwap_reclaim_confirmed:
                        return self._hold_with_reason(
                            "above_vwap_no_reclaim",
                            {
                                "trend_signal": trend_signal,
                                "entry_mode": entry_mode_effective,
                                "configured_entry_mode": self.entry_mode,
                                "vwap_reclaim_confirmed": sell_vwap_reclaim_confirmed,
                                "vwap_bias_confirmed": current_price <= vwap_price,
                                **vwap_payload,
                            },
                        )
                    if (
                        vwap_lower_overstretch is not None
                        and current_price < vwap_lower_overstretch
                    ):
                        if is_trend_continuation_entry:
                            return self._hold_with_reason(
                                "vwap_overstretched_lower",
                                {
                                    "trend_signal": trend_signal,
                                    "entry_mode": entry_mode_effective,
                                    "configured_entry_mode": self.entry_mode,
                                    "vwap_reclaim_confirmed": sell_vwap_reclaim_confirmed,
                                    "vwap_bias_confirmed": current_price <= vwap_price,
                                    **vwap_payload,
                                },
                            )
                        if is_fresh_cross_entry:
                            vwap_penalty = self._FRESH_CROSS_VWAP_OVERSTRETCH_PENALTY

            regime_payload: dict[str, object] = {}
            if self.regime_adx_filter_enabled:
                adx = (
                    regime_adx_value_for_state
                    if regime_adx_value_for_state is not None
                    else self._adx(prices, self.regime_adx_window)
                )
                if adx is None:
                    return self._hold_with_reason(
                        "regime_adx_unavailable",
                        {
                            "regime_adx_filter_enabled": True,
                            "regime_adx_window": self.regime_adx_window,
                            "history": len(prices),
                        },
                    )
                adx_entry_threshold = (
                    regime_adx_entry_threshold
                    if regime_adx_entry_threshold is not None
                    else self.regime_min_adx + self.regime_adx_hysteresis
                )
                adx_exit_threshold = (
                    regime_adx_exit_threshold
                    if regime_adx_exit_threshold is not None
                    else max(0.0, self.regime_min_adx - self.regime_adx_hysteresis)
                )
                adx_gate_passes, adx_regime_active_before, adx_regime_active_after = self._adx_gate_passes(
                    regime_scope_key,
                    adx,
                    adx_entry_threshold,
                    adx_exit_threshold,
                )
                regime_payload = {
                    "regime_adx_filter_enabled": True,
                    "regime_adx": adx,
                    "regime_min_adx": self.regime_min_adx,
                    "regime_adx_hysteresis": self.regime_adx_hysteresis,
                    "regime_adx_entry_threshold": adx_entry_threshold,
                    "regime_adx_exit_threshold": adx_exit_threshold,
                    "regime_use_hysteresis_state": self.regime_use_hysteresis_state,
                    "regime_adx_state_scope": regime_scope_key,
                    "regime_adx_state_refreshed_pre_signal": regime_adx_value_for_state is not None,
                    "regime_adx_active_before": adx_regime_active_before,
                    "regime_adx_active_after": adx_regime_active_after,
                }
                if not adx_gate_passes:
                    regime_adx_penalty = (
                        self._FRESH_CROSS_REGIME_ADX_PENALTY
                        if fresh_cross_relief_active
                        else self._SECONDARY_REGIME_ADX_PENALTY
                    )
                    regime_payload["regime_adx_relief_applied"] = True
                    regime_payload["regime_adx_penalty"] = regime_adx_penalty
                    regime_payload["regime_adx_softened"] = True
                    soft_filter_reasons.append("regime_adx_below_threshold")
                else:
                    regime_payload["regime_adx_relief_applied"] = False
                    regime_payload["regime_adx_penalty"] = 0.0
                    regime_payload["regime_adx_softened"] = False
            else:
                regime_payload["regime_adx_filter_enabled"] = False
                regime_payload["regime_adx_relief_applied"] = False
                regime_payload["regime_adx_penalty"] = 0.0
                regime_payload["regime_adx_softened"] = False

            ema_gap_atr = abs(fast_now - slow_now) / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if is_trend_continuation_entry and ema_gap_atr < min_trend_gap_atr_effective:
                return self._hold_with_reason(
                    "ma_gap_too_small",
                    {
                        "ema_gap_atr": ema_gap_atr,
                        "min_trend_gap_atr": min_trend_gap_atr_effective,
                    },
                )

            continuation_fast_ma_retest_gap_abs = abs(current_price - fast_now)
            continuation_fast_ma_retest_gap_atr_ratio = continuation_fast_ma_retest_gap_abs / max(atr, FLOAT_COMPARISON_TOLERANCE)
            continuation_fast_ma_retest_atr_tolerance = float(
                entry_filters["continuation_fast_ma_retest_atr_tolerance"]
            )
            continuation_trend_intact = current_price > slow_now if candidate_side == Side.BUY else current_price < slow_now
            if is_trend_continuation_entry and (
                continuation_fast_ma_retest_gap_atr_ratio > continuation_fast_ma_retest_atr_tolerance
                or not continuation_trend_intact
            ):
                return self._hold_with_reason(
                    "no_pullback_reset",
                    {
                        "entry_mode": entry_mode_effective,
                        "configured_entry_mode": self.entry_mode,
                        "entry_mode_override_applied": bool(entry_filters.get("entry_mode_override_applied")),
                        "trend_signal": trend_signal,
                        "fast_ma": fast_now,
                        "slow_ma": slow_now,
                        "current_price": current_price,
                        "atr": atr,
                        "continuation_fast_ma_retest_gap_abs": continuation_fast_ma_retest_gap_abs,
                        "continuation_fast_ma_retest_gap_atr_ratio": continuation_fast_ma_retest_gap_atr_ratio,
                        "continuation_fast_ma_retest_atr_tolerance": continuation_fast_ma_retest_atr_tolerance,
                        "continuation_trend_intact": continuation_trend_intact,
                    },
                )

            pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
            atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            low_tf_risk_compaction_applied = self._is_low_timeframe_for_risk(timeframe_sec)
            effective_atr_multiplier = self.atr_multiplier
            effective_risk_reward_ratio = self.risk_reward_ratio
            effective_min_stop_loss_pips = self.min_stop_loss_pips
            effective_min_take_profit_pips = self.min_take_profit_pips
            effective_stop_loss_cap_pips: float | None = None
            effective_take_profit_cap_pips: float | None = None

            if low_tf_risk_compaction_applied:
                effective_atr_multiplier = min(effective_atr_multiplier, self.low_tf_atr_multiplier_cap)
                effective_risk_reward_ratio = min(effective_risk_reward_ratio, self.low_tf_risk_reward_ratio_cap)
                effective_min_stop_loss_pips = min(effective_min_stop_loss_pips, self.low_tf_min_stop_loss_pips)
                effective_min_take_profit_pips = min(
                    effective_min_take_profit_pips,
                    self.low_tf_min_take_profit_pips,
                )
                effective_stop_loss_cap_pips = max(
                    effective_min_stop_loss_pips,
                    atr_pips * self.low_tf_max_stop_loss_atr,
                )
                effective_take_profit_cap_pips = max(
                    effective_min_take_profit_pips,
                    atr_pips * self.low_tf_max_take_profit_atr,
                )

            stop_loss_pips = max(effective_min_stop_loss_pips, atr_pips * effective_atr_multiplier)

            if self.min_relative_stop_pct > 0:
                min_relative_stop_pips = (current_price * self.min_relative_stop_pct) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
                stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)
            if effective_stop_loss_cap_pips is not None:
                stop_loss_pips = min(stop_loss_pips, effective_stop_loss_cap_pips)
            stop_loss_pips = max(effective_min_stop_loss_pips, stop_loss_pips)

            current_spread_pips = float(ctx.current_spread_pips) if ctx.current_spread_pips is not None else None
            spread_to_stop_ratio: float | None = None
            spread_to_atr_ratio: float | None = None
            if current_spread_pips is not None:
                spread_to_stop_ratio = current_spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
                spread_to_atr_ratio = current_spread_pips / max(atr_pips, FLOAT_COMPARISON_TOLERANCE)
                if (
                    self.max_spread_to_stop_ratio is not None
                    and spread_to_stop_ratio > self.max_spread_to_stop_ratio
                ):
                    return self._hold_with_reason(
                        "spread_too_wide_relative_to_stop",
                        {
                            "spread_pips": current_spread_pips,
                            "stop_loss_pips": stop_loss_pips,
                            "spread_to_stop_ratio": spread_to_stop_ratio,
                            "max_spread_to_stop_ratio": self.max_spread_to_stop_ratio,
                        },
                    )
                if (
                    self.max_spread_to_atr_ratio is not None
                    and spread_to_atr_ratio > self.max_spread_to_atr_ratio
                ):
                    return self._hold_with_reason(
                        "spread_too_wide_relative_to_atr",
                        {
                            "spread_pips": current_spread_pips,
                            "atr_pips": atr_pips,
                            "spread_to_atr_ratio": spread_to_atr_ratio,
                            "max_spread_to_atr_ratio": self.max_spread_to_atr_ratio,
                        },
                    )

            take_profit_pips = max(effective_min_take_profit_pips, stop_loss_pips * effective_risk_reward_ratio)
            if effective_take_profit_cap_pips is not None:
                take_profit_pips = min(take_profit_pips, effective_take_profit_cap_pips)
            take_profit_pips = max(effective_min_take_profit_pips, take_profit_pips)
            take_profit_pips = max(stop_loss_pips, take_profit_pips)

            price_slow_gap_atr = abs(current_price - slow_now) / max(atr, FLOAT_COMPARISON_TOLERANCE)
            gap_relief_atr = max(0.0, float(effective_confirm_bars - 1) * self.confirm_gap_relief_per_bar)
            max_price_slow_gap_atr_effective = max_price_slow_gap_atr_effective_base + gap_relief_atr
            pullback_entry_max_gap_atr_effective = pullback_entry_max_gap_atr_effective_base + gap_relief_atr
            extra_payload = {
                "price_slow_gap_atr": price_slow_gap_atr,
                "max_price_slow_gap_atr": max_price_slow_gap_atr_effective_base,
                "max_price_slow_gap_atr_effective": max_price_slow_gap_atr_effective,
                "pullback_entry_max_gap_atr": pullback_entry_max_gap_atr_effective_base,
                "pullback_entry_max_gap_atr_effective": pullback_entry_max_gap_atr_effective,
                "confirm_gap_relief_per_bar": self.confirm_gap_relief_per_bar,
                "confirm_gap_relief_atr": gap_relief_atr,
                "confirm_bars": effective_confirm_bars,
                "pullback_entry_price": fast_now,
                "pullback_entry_price_source": "fast_ma",
                "pullback_fast_ma_price": fast_now,
                "pullback_slow_ma_price": slow_now,
                "pullback_distance_price": abs(current_price - fast_now),
                "pullback_slow_ma_distance_price": abs(current_price - slow_now),
                "price_gap_mode": self.price_gap_mode,
                "continuation_fast_ma_retest_gap_abs": continuation_fast_ma_retest_gap_abs,
                "continuation_fast_ma_retest_gap_atr_ratio": continuation_fast_ma_retest_gap_atr_ratio,
                "continuation_fast_ma_retest_atr_tolerance": continuation_fast_ma_retest_atr_tolerance,
                "continuation_trend_intact": continuation_trend_intact,
                "suggested_order_type": "limit",
                "suggested_entry_price": fast_now,
                "suggested_entry_price_source": "fast_ma",
                "fresh_cross_filter_relief_enabled": self.fresh_cross_filter_relief_enabled,
                "fresh_cross_relief_active": fresh_cross_relief_active,
                "fresh_cross_gap_relief_applied": False,
                "fresh_cross_gap_penalty": 0.0,
                **entry_filters,
            }
            if self.price_gap_mode == "wait_pullback":
                if price_slow_gap_atr > max_price_slow_gap_atr_effective:
                    extra_payload["entry_action"] = "place_limit"
                    extra_payload["pullback_wait_state"] = "hard_cap"
                    return self._hold_with_reason("waiting_for_limit_pullback", extra_payload)
                if price_slow_gap_atr > pullback_entry_max_gap_atr_effective:
                    if fresh_cross_relief_active:
                        gap_relief_penalty = self._FRESH_CROSS_GAP_PENALTY
                        extra_payload["fresh_cross_gap_relief_applied"] = True
                        extra_payload["fresh_cross_gap_penalty"] = gap_relief_penalty
                    else:
                        return self._hold_with_reason("wait_pullback_entry", extra_payload)
            elif price_slow_gap_atr > max_price_slow_gap_atr_effective:
                return self._hold_with_reason(
                    "price_too_far_from_slow_ma",
                    extra_payload,
                )

            if candidate_side == Side.BUY:
                if fast_slope <= 0:
                    return self._hold_with_reason(
                        "ma_slope_direction_mismatch",
                        {
                            "direction": "up",
                            "fast_slope": fast_slope,
                            "slow_slope": slow_slope,
                            "slope_filter_mode": "fast_only",
                        },
                    )
            else:
                if fast_slope >= 0:
                    return self._hold_with_reason(
                        "ma_slope_direction_mismatch",
                        {
                            "direction": "down",
                            "fast_slope": fast_slope,
                            "slow_slope": slow_slope,
                            "slope_filter_mode": "fast_only",
                        },
                    )

            slope_strength_atr = abs(fast_slope) / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if slope_strength_atr < min_slope_atr_ratio_effective:
                return self._hold_with_reason(
                    "ma_slope_too_flat",
                    {
                        "slope_strength_atr": slope_strength_atr,
                        "min_slope_atr_ratio": min_slope_atr_ratio_effective,
                        "slope_filter_mode": "fast_only",
                    },
                )

            if candidate_side == Side.BUY:
                price_aligned_with_fast_ma = current_price > fast_now
            else:
                price_aligned_with_fast_ma = current_price < fast_now
            confidence = self._confidence(
                ema_gap_atr,
                slope_strength_atr,
                price_aligned_with_fast_ma,
                price_slow_gap_atr=price_slow_gap_atr,
                is_trend_continuation_entry=is_trend_continuation_entry,
                continuation_confidence_cap=self.continuation_confidence_cap,
                continuation_price_alignment_bonus_multiplier=self.continuation_price_alignment_bonus_multiplier,
                continuation_late_penalty_multiplier=self.continuation_late_penalty_multiplier,
            )
            auxiliary_penalty_total = (
                higher_tf_penalty
                + volume_penalty
                + regime_adx_penalty
                + kama_penalty
                + gap_relief_penalty
                + vwap_penalty
            )
            confidence = max(0.0, confidence - auxiliary_penalty_total)
            trailing_fast_ma, trailing_fast_ma_source = self._fast_ma_trailing_value(
                ctx,
                fallback_fast_ma=fast_now,
                timeframe_sec=timeframe_sec,
            )
            trailing_stop_payload: dict[str, object] | None = None
            if self.fast_ma_trailing_enabled:
                trailing_stop_payload = {
                    "trailing_enabled": True,
                    "trailing_mode": "fast_ma",
                    "trailing_activation_r_multiple": self.fast_ma_trailing_activation_r_multiple,
                    "trailing_activation_min_profit_pips": self.fast_ma_trailing_activation_min_profit_pips,
                    "fast_ma_value": trailing_fast_ma,
                    "fast_ma_source": trailing_fast_ma_source,
                    "fast_ma_use_closed_candle": self.fast_ma_trailing_use_closed_candle,
                    "fast_ma_buffer_atr": self.fast_ma_trailing_buffer_atr,
                    "fast_ma_buffer_pips": self.fast_ma_trailing_buffer_pips,
                    "fast_ma_min_step_pips": self.fast_ma_trailing_min_step_pips,
                    "fast_ma_update_cooldown_sec": self.fast_ma_trailing_update_cooldown_sec,
                }
                if self.fast_ma_trailing_timeframe_sec is not None:
                    trailing_stop_payload["fast_ma_timeframe_sec"] = self.fast_ma_trailing_timeframe_sec
            signal = Signal(
                side=candidate_side,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ma_cross",
                    "trend": "up" if candidate_side == Side.BUY else "down",
                    "trend_signal": trend_signal,
                    "entry_family": ("fresh_cross" if is_fresh_cross_entry else "trend_continuation"),
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": bool(entry_filters.get("entry_mode_override_applied")),
                    "continuation_fast_ma_retest_gap_abs": continuation_fast_ma_retest_gap_abs,
                    "continuation_fast_ma_retest_gap_atr_ratio": continuation_fast_ma_retest_gap_atr_ratio,
                    "continuation_fast_ma_retest_atr_tolerance": continuation_fast_ma_retest_atr_tolerance,
                    "continuation_trend_intact": continuation_trend_intact,
                    "ma_type": self.ma_type,
                    "confirm_bars": effective_confirm_bars,
                    "configured_confirm_bars": self.confirm_bars,
                    "timeframe_sec": timeframe_sec,
                    "atr_window": self.atr_window,
                    "atr_multiplier": self.atr_multiplier,
                    "atr_multiplier_effective": effective_atr_multiplier,
                    "atr_pips": atr_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "risk_reward_ratio_effective": effective_risk_reward_ratio,
                    "min_stop_loss_pips_effective": effective_min_stop_loss_pips,
                    "min_take_profit_pips_effective": effective_min_take_profit_pips,
                    "low_tf_risk_compaction_applied": low_tf_risk_compaction_applied,
                    "risk_profile": ("low_tf_compact" if low_tf_risk_compaction_applied else "default"),
                    "max_spread_to_stop_ratio": self.max_spread_to_stop_ratio,
                    "max_spread_to_atr_ratio": self.max_spread_to_atr_ratio,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "spread_to_atr_ratio": spread_to_atr_ratio,
                    "price_slow_gap_atr": price_slow_gap_atr,
                    "pullback_entry_max_gap_atr": pullback_entry_max_gap_atr_effective_base,
                    "pullback_entry_max_gap_atr_effective": pullback_entry_max_gap_atr_effective,
                    "max_price_slow_gap_atr_effective": max_price_slow_gap_atr_effective,
                    "ema_gap_atr": ema_gap_atr,
                    "slope_strength_atr": slope_strength_atr,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "momentum_continuation_confidence_cap": self.continuation_confidence_cap,
                    "momentum_continuation_price_alignment_bonus_multiplier": (
                        self.continuation_price_alignment_bonus_multiplier
                    ),
                    "momentum_continuation_late_penalty_multiplier": self.continuation_late_penalty_multiplier,
                    "momentum_confidence_cap_effective": (
                        self.continuation_confidence_cap if is_trend_continuation_entry else 1.0
                    ),
                    "fresh_cross_filter_relief_enabled": self.fresh_cross_filter_relief_enabled,
                    "fresh_cross_relief_active": fresh_cross_relief_active,
                    "fresh_cross_gap_relief_applied": extra_payload.get("fresh_cross_gap_relief_applied", False),
                    "higher_tf_bias_penalty": higher_tf_penalty,
                    "volume_penalty": volume_penalty,
                    "regime_adx_penalty": regime_adx_penalty,
                    "kama_penalty": kama_penalty,
                    "fresh_cross_gap_penalty": gap_relief_penalty,
                    "vwap_penalty": vwap_penalty,
                    "auxiliary_filter_penalty_total": auxiliary_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    "confidence_model": "velocity_weighted_v3",
                    "vwap_reclaim_confirmed": (
                        buy_vwap_reclaim_confirmed if candidate_side == Side.BUY else sell_vwap_reclaim_confirmed
                    ),
                    "vwap_bias_confirmed": (
                        (current_price >= vwap_price) if (candidate_side == Side.BUY and vwap_price is not None)
                        else (
                            (current_price <= vwap_price)
                            if (candidate_side == Side.SELL and vwap_price is not None)
                            else None
                        )
                    ),
                    **vwap_payload,
                    **kama_payload,
                    "fast_slope": fast_slope,
                    "slow_slope": slow_slope,
                    "diff_prev": diff_prev,
                    "diff_now": diff_now,
                    "bullish_gap_expanding": bullish_gap_expanding,
                    "bearish_gap_expanding": bearish_gap_expanding,
                    "slope_filter_mode": "fast_only",
                    "fast_ma": fast_now,
                    "slow_ma": slow_now,
                    "reverse_exit_supported": True,
                    "fast_ma_trailing_enabled": self.fast_ma_trailing_enabled,
                    "fast_ma_trailing_mode": ("fast_ma" if self.fast_ma_trailing_enabled else "disabled"),
                    "fast_ma_trailing_value": trailing_fast_ma,
                    "fast_ma_trailing_source": trailing_fast_ma_source,
                    "invalid_prices_dropped": invalid_prices,
                    **entry_filters,
                    **higher_tf_bias_payload,
                    **regime_payload,
                    **session_payload,
                    **volume_payload,
                    **({"trailing_stop": trailing_stop_payload} if trailing_stop_payload is not None else {}),
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if bullish_cross_in_window or bearish_cross_in_window:
            direction = "up"
            bars_since_cross = (bullish_cross_age - 1) if bullish_cross_age is not None else None
            if bullish_cross_in_window and bearish_cross_in_window:
                bullish_idx = latest_bullish_cross if latest_bullish_cross is not None else -1
                bearish_idx = latest_bearish_cross if latest_bearish_cross is not None else -1
                if bearish_idx > bullish_idx:
                    direction = "down"
                    bars_since_cross = (bearish_cross_age - 1) if bearish_cross_age is not None else None
            elif bearish_cross_in_window:
                direction = "down"
                bars_since_cross = (bearish_cross_age - 1) if bearish_cross_age is not None else None
            return self._hold_with_reason(
                "cross_not_confirmed",
                {
                    "confirm_bars": effective_confirm_bars,
                    "configured_confirm_bars": self.confirm_bars,
                    "timeframe_sec": timeframe_sec,
                    "direction": direction,
                    "bars_since_cross": bars_since_cross,
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": bool(entry_filters.get("entry_mode_override_applied")),
                },
            )

        return self._hold_with_reason(
            "no_ma_cross",
            {
                "entry_mode": entry_mode_effective,
                "configured_entry_mode": self.entry_mode,
                "entry_mode_override_applied": bool(entry_filters.get("entry_mode_override_applied")),
                "ma_type": self.ma_type,
            },
        )
