from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging
import math
from datetime import datetime, timedelta, timezone
from collections.abc import Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema, zscore
from xtb_bot.symbols import is_index_symbol, normalize_symbol, resolve_index_market
from xtb_bot.strategies.base import Strategy, StrategyContext


logger = logging.getLogger(__name__)


class IndexHybridStrategy(Strategy):
    name = "index_hybrid"
    _SOFT_FILTER_PENALTIES: dict[str, float] = {
        "volume_unavailable": 0.06,
        "volume_below_entry_ratio": 0.10,
        "volume_not_confirmed": 0.10,
        "trend_blocked_by_session": 0.12,
        "mean_reversion_blocked_by_session": 0.12,
        "trend_waiting_session_open_stabilization": 0.10,
        "waiting_for_volatility_explosion": 0.08,
    }

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.fast_ema_window = max(2, int(params.get("index_fast_ema_window", 34)))
        self.slow_ema_window = max(self.fast_ema_window + 1, int(params.get("index_slow_ema_window", 144)))
        self.donchian_window = max(2, int(params.get("index_donchian_window", 20)))
        self.zscore_window_configured = max(5, int(params.get("index_zscore_window", 30)))
        self.zscore_window = self.zscore_window_configured
        self.window_sync_mode = str(params.get("index_window_sync_mode", "off")).strip().lower()
        if self.window_sync_mode not in {"auto", "warn", "off"}:
            self.window_sync_mode = "off"
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
                float(self.zscore_window_target), FLOAT_COMPARISON_TOLERANCE
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
            params.get("index_auto_correct_regime_thresholds", False),
            False,
        )
        self.require_context_tick_size = self._as_bool(
            params.get("index_require_context_tick_size", False),
            False,
        )
        self.require_non_fallback_pip_size = self._as_bool(
            params.get("index_require_non_fallback_pip_size", False),
            False,
        )
        self.enforce_gap_hysteresis = self._as_bool(
            params.get("index_enforce_gap_hysteresis", False),
            False,
        )
        self.gap_hysteresis_min = max(
            0.0,
            float(params.get("index_gap_hysteresis_min", 0.00020)),
        )
        self.trend_gap_threshold_configured = max(0.0, float(params.get("index_trend_gap_threshold", 0.0006)))
        self.mean_reversion_gap_threshold_configured = max(
            0.0,
            float(params.get("index_mean_reversion_gap_threshold", self.trend_gap_threshold_configured * 0.7)),
        )
        self.atr_window = max(2, int(params.get("index_atr_window", 14)))
        self.trend_atr_pct_threshold_configured = max(0.0, float(params.get("index_trend_atr_pct_threshold", 0.08)))
        self.mean_reversion_atr_pct_threshold_configured = max(
            0.0, float(params.get("index_mean_reversion_atr_pct_threshold", 0.05))
        )
        self.trend_gap_threshold_effective = self.trend_gap_threshold_configured
        self.mean_reversion_gap_threshold_effective = self.mean_reversion_gap_threshold_configured
        self.trend_atr_pct_threshold_effective = self.trend_atr_pct_threshold_configured
        self.mean_reversion_atr_pct_threshold_effective = self.mean_reversion_atr_pct_threshold_configured
        self.regime_thresholds_auto_corrected = False
        self.gap_hysteresis_applied = False
        self.regime_threshold_corrections: dict[str, tuple[float, float]] = {}
        if self.auto_correct_regime_thresholds:
            if self.trend_gap_threshold_effective < self.mean_reversion_gap_threshold_effective:
                self.regime_thresholds_auto_corrected = True
                self.regime_threshold_corrections["gap"] = (
                    self.trend_gap_threshold_effective,
                    self.mean_reversion_gap_threshold_effective,
                )
                self.trend_gap_threshold_effective, self.mean_reversion_gap_threshold_effective = (
                    self.mean_reversion_gap_threshold_effective,
                    self.trend_gap_threshold_effective,
                )
            if self.trend_atr_pct_threshold_effective < self.mean_reversion_atr_pct_threshold_effective:
                self.regime_thresholds_auto_corrected = True
                self.regime_threshold_corrections["atr_pct"] = (
                    self.trend_atr_pct_threshold_effective,
                    self.mean_reversion_atr_pct_threshold_effective,
                )
                self.trend_atr_pct_threshold_effective, self.mean_reversion_atr_pct_threshold_effective = (
                    self.mean_reversion_atr_pct_threshold_effective,
                    self.trend_atr_pct_threshold_effective,
                )
        if self.enforce_gap_hysteresis and self.gap_hysteresis_min > 0:
            gap_span = self.trend_gap_threshold_effective - self.mean_reversion_gap_threshold_effective
            if gap_span < self.gap_hysteresis_min:
                self.regime_thresholds_auto_corrected = True
                self.gap_hysteresis_applied = True
                self.regime_threshold_corrections["gap_hysteresis"] = (
                    self.trend_gap_threshold_effective,
                    self.mean_reversion_gap_threshold_effective,
                )
                self.mean_reversion_gap_threshold_effective = max(
                    0.0,
                    self.trend_gap_threshold_effective - self.gap_hysteresis_min,
                )
        # Keep legacy names as effective thresholds for backwards compatibility
        # with existing metadata/tests that read these keys.
        self.trend_gap_threshold = self.trend_gap_threshold_effective
        self.mean_reversion_gap_threshold = self.mean_reversion_gap_threshold_effective
        self.trend_atr_pct_threshold = self.trend_atr_pct_threshold_effective
        self.mean_reversion_atr_pct_threshold = self.mean_reversion_atr_pct_threshold_effective
        self.zscore_mode_requested = str(params.get("index_zscore_mode", "detrended")).strip().lower()
        if self.zscore_mode_requested not in {"classic", "detrended"}:
            self.zscore_mode_requested = "detrended"
        self.zscore_mode = self.zscore_mode_requested
        self.zscore_mode_forced_detrended = False
        self.zscore_ema_window = max(2, int(params.get("index_zscore_ema_window", self.slow_ema_window)))
        self.mean_reversion_entry_mode = str(params.get("index_mean_reversion_entry_mode", "hook")).strip().lower()
        if self.mean_reversion_entry_mode not in {"touch", "hook"}:
            self.mean_reversion_entry_mode = "hook"
        self.mean_reversion_allow_breakout = self._as_bool(
            params.get("index_mean_reversion_allow_breakout", False),
            False,
        )
        self.mean_reversion_breakout_extreme_multiplier = max(
            1.0,
            float(params.get("index_mean_reversion_breakout_extreme_multiplier", 1.0)),
        )
        self.mean_reversion_require_band_proximity = self._as_bool(
            params.get("index_mean_reversion_require_band_proximity", True),
            True,
        )
        self.mean_reversion_band_proximity_max_ratio = max(
            0.0,
            min(1.0, float(params.get("index_mean_reversion_band_proximity_max_ratio", 0.25))),
        )
        self.mean_reversion_min_mid_deviation_ratio = max(
            0.0,
            min(1.0, float(params.get("index_mean_reversion_min_mid_deviation_ratio", 0.15))),
        )
        self.min_breakout_distance_ratio = max(
            0.0,
            float(params.get("index_min_breakout_distance_ratio", 0.0)),
        )
        self.max_spread_pips = max(
            0.0,
            float(params.get("index_max_spread_pips", 0.0)),
        )
        self.max_spread_pips_by_symbol = self._parse_float_mapping(
            params.get("index_max_spread_pips_by_symbol")
        )
        self.max_spread_to_breakout_ratio = max(
            0.0,
            float(params.get("index_max_spread_to_breakout_ratio", 0.6)),
        )
        self.max_spread_to_stop_ratio = max(
            0.0,
            float(params.get("index_max_spread_to_stop_ratio", 0.35)),
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
        self.volume_min_ratio_for_entry = max(
            0.0,
            float(params.get("index_volume_min_ratio_for_entry", 1.0)),
        )
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
        self.mean_reversion_confidence_base = max(
            0.0,
            min(1.0, float(params.get("index_mean_reversion_confidence_base", 0.10))),
        )
        self.trend_confidence_gap_weight = max(
            0.0, min(1.0, float(params.get("index_trend_confidence_gap_weight", 0.5)))
        )
        self.trend_confidence_breakout_weight = max(
            0.0, min(1.0, float(params.get("index_trend_confidence_breakout_weight", 0.3)))
        )
        self.trend_confidence_gap_norm = max(
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("index_trend_confidence_gap_norm", max(self.trend_gap_threshold_effective, 0.0005))),
        )
        self.trend_confidence_breakout_norm = max(
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("index_trend_confidence_breakout_norm", 0.0005)),
        )
        self.confidence_threshold_cap_enabled = self._as_bool(
            params.get("index_confidence_threshold_cap_enabled", True),
            True,
        )
        self.confidence_threshold_cap_trend = max(
            0.0,
            min(1.0, float(params.get("index_confidence_threshold_cap_trend", 0.58))),
        )
        self.confidence_threshold_cap_mean_reversion = max(
            0.0,
            min(1.0, float(params.get("index_confidence_threshold_cap_mean_reversion", 0.58))),
        )

        # Legacy base SL/TP are preserved as defaults; regime-specific profiles
        # can override them below.
        self.stop_loss_pips = max(0.1, float(params.get("index_stop_loss_pips", params.get("stop_loss_pips", 30.0))))
        self.take_profit_pips = max(
            0.1,
            float(params.get("index_take_profit_pips", params.get("take_profit_pips", 80.0))),
        )
        self.stop_loss_pct = max(0.0, float(params.get("index_stop_loss_pct", 0.5)))
        self.take_profit_pct = max(0.0, float(params.get("index_take_profit_pct", 1.5)))
        self.stop_atr_multiplier = max(0.1, float(params.get("index_stop_atr_multiplier", 2.0)))
        self.risk_reward_ratio = max(1.0, float(params.get("index_risk_reward_ratio", 3.0)))
        self.trend_stop_loss_pips = max(
            0.1,
            float(params.get("index_trend_stop_loss_pips", self.stop_loss_pips)),
        )
        self.trend_take_profit_pips = max(
            0.1,
            float(params.get("index_trend_take_profit_pips", self.take_profit_pips)),
        )
        self.trend_stop_loss_pct = max(
            0.0,
            float(params.get("index_trend_stop_loss_pct", self.stop_loss_pct)),
        )
        self.trend_take_profit_pct = max(
            0.0,
            float(params.get("index_trend_take_profit_pct", self.take_profit_pct)),
        )
        self.trend_stop_atr_multiplier = max(
            0.1,
            float(params.get("index_trend_stop_atr_multiplier", self.stop_atr_multiplier)),
        )
        self.trend_risk_reward_ratio = max(
            1.0,
            float(params.get("index_trend_risk_reward_ratio", self.risk_reward_ratio)),
        )
        self.trend_pct_floors_enabled = self._as_bool(
            params.get("index_trend_pct_floors_enabled", True),
            True,
        )
        self.mean_reversion_stop_loss_pips = max(
            0.1,
            float(params.get("index_mean_reversion_stop_loss_pips", self.trend_stop_loss_pips)),
        )
        self.mean_reversion_take_profit_pips = max(
            0.1,
            float(
                params.get(
                    "index_mean_reversion_take_profit_pips",
                    max(0.1, self.trend_take_profit_pips * 0.6),
                )
            ),
        )
        self.mean_reversion_stop_loss_pct = max(
            0.0,
            float(params.get("index_mean_reversion_stop_loss_pct", self.trend_stop_loss_pct)),
        )
        self.mean_reversion_take_profit_pct = max(
            0.0,
            float(
                params.get(
                    "index_mean_reversion_take_profit_pct",
                    max(0.0, self.trend_take_profit_pct * 0.45),
                )
            ),
        )
        self.mean_reversion_stop_atr_multiplier = max(
            0.1,
            float(
                params.get(
                    "index_mean_reversion_stop_atr_multiplier",
                    max(0.1, self.trend_stop_atr_multiplier * 0.85),
                )
            ),
        )
        self.mean_reversion_risk_reward_ratio = max(
            1.0,
            float(params.get("index_mean_reversion_risk_reward_ratio", 1.4)),
        )
        # Mean-reversion targets local reversion legs; percentage floors are
        # disabled by default to avoid forcing trend-horizon SL/TP distances.
        self.mean_reversion_pct_floors_enabled = self._as_bool(
            params.get("index_mean_reversion_pct_floors_enabled", False),
            False,
        )
        # Keep legacy fields aligned with trend profile for compatibility with
        # early HOLD paths that do not know active regime yet.
        self.stop_loss_pips = self.trend_stop_loss_pips
        self.take_profit_pips = max(
            self.trend_take_profit_pips,
            self.stop_loss_pips * self.trend_risk_reward_ratio,
        )
        self.stop_loss_pct = self.trend_stop_loss_pct
        self.take_profit_pct = self.trend_take_profit_pct
        self.stop_atr_multiplier = self.trend_stop_atr_multiplier
        self.risk_reward_ratio = self.trend_risk_reward_ratio
        if self.auto_correct_regime_thresholds and self.regime_threshold_corrections:
            logger.warning(
                "index_hybrid auto-corrected regime thresholds | corrections=%s",
                self.regime_threshold_corrections,
            )
        self.default_timeframe_sec = float(params.get("index_timeframe_sec", 60.0))
        if self.default_timeframe_sec <= 0:
            self.default_timeframe_sec = 60.0
        self.candle_timeframe_sec = max(
            0,
            int(float(params.get("index_candle_timeframe_sec", self.default_timeframe_sec))),
        )
        self.resample_mode = str(params.get("index_resample_mode", "always")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "always"
        self.ignore_sunday_candles = self._as_bool(
            params.get("index_ignore_sunday_candles", True),
            True,
        )

        self.session_filter_enabled = self._as_bool(params.get("index_session_filter_enabled", True), True)
        self.session_profile_mode_requested = str(
            params.get("index_session_profile_mode", "market_presets")
        ).strip().lower()
        if self.session_profile_mode_requested not in {"market_presets", "legacy_utc"}:
            self.session_profile_mode_requested = "market_presets"
        self.session_profile_mode = self.session_profile_mode_requested
        self.trend_session_start_hour_utc = int(float(params.get("index_trend_session_start_hour_utc", 6))) % 24
        self.trend_session_end_hour_utc = int(float(params.get("index_trend_session_end_hour_utc", 22))) % 24
        self.america_session_timezone = str(params.get("index_america_session_timezone", "America/New_York")).strip()
        if not self.america_session_timezone:
            self.america_session_timezone = "America/New_York"
        self.america_trend_session_start_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_america_trend_session_start_local",
                params.get("index_america_trend_session_start_hour_local", "09:30"),
            ),
            default_hour=9,
            default_minute=30,
        )
        self.america_trend_session_end_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_america_trend_session_end_local",
                params.get("index_america_trend_session_end_hour_local", 16),
            ),
            default_hour=16,
            default_minute=0,
        )
        self.america_trend_session_start_hour_local = self.america_trend_session_start_minute_local // 60
        self.america_trend_session_end_hour_local = self.america_trend_session_end_minute_local // 60
        self.europe_session_timezone = str(params.get("index_europe_session_timezone", "Europe/London")).strip()
        if not self.europe_session_timezone:
            self.europe_session_timezone = "Europe/London"
        self.europe_trend_session_start_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_europe_trend_session_start_local",
                params.get("index_europe_trend_session_start_hour_local", 8),
            ),
            default_hour=8,
            default_minute=0,
        )
        self.europe_trend_session_end_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_europe_trend_session_end_local",
                params.get("index_europe_trend_session_end_hour_local", 17),
            ),
            default_hour=17,
            default_minute=0,
        )
        self.europe_trend_session_start_hour_local = self.europe_trend_session_start_minute_local // 60
        self.europe_trend_session_end_hour_local = self.europe_trend_session_end_minute_local // 60
        self.japan_session_timezone = str(params.get("index_japan_session_timezone", "Asia/Tokyo")).strip()
        if not self.japan_session_timezone:
            self.japan_session_timezone = "Asia/Tokyo"
        self.japan_trend_session_start_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_japan_trend_session_start_local",
                params.get("index_japan_trend_session_start_hour_local", 9),
            ),
            default_hour=9,
            default_minute=0,
        )
        self.japan_trend_session_end_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_japan_trend_session_end_local",
                params.get("index_japan_trend_session_end_hour_local", 15),
            ),
            default_hour=15,
            default_minute=0,
        )
        self.japan_trend_session_start_hour_local = self.japan_trend_session_start_minute_local // 60
        self.japan_trend_session_end_hour_local = self.japan_trend_session_end_minute_local // 60
        self.australia_session_timezone = str(
            params.get("index_australia_session_timezone", "Australia/Sydney")
        ).strip()
        if not self.australia_session_timezone:
            self.australia_session_timezone = "Australia/Sydney"
        self.australia_trend_session_start_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_australia_trend_session_start_local",
                params.get("index_australia_trend_session_start_hour_local", 10),
            ),
            default_hour=10,
            default_minute=0,
        )
        self.australia_trend_session_end_minute_local = self._parse_session_time_minutes(
            params.get(
                "index_australia_trend_session_end_local",
                params.get("index_australia_trend_session_end_hour_local", 16),
            ),
            default_hour=16,
            default_minute=0,
        )
        self.australia_trend_session_start_hour_local = self.australia_trend_session_start_minute_local // 60
        self.australia_trend_session_end_hour_local = self.australia_trend_session_end_minute_local // 60
        self.trend_session_open_delay_minutes = max(
            0,
            int(float(params.get("index_trend_session_open_delay_minutes", 0))),
        )
        self.trend_volatility_explosion_filter_enabled = self._as_bool(
            params.get("index_trend_volatility_explosion_filter_enabled", False),
            False,
        )
        self.trend_volatility_explosion_lookback = max(
            1,
            int(params.get("index_trend_volatility_explosion_lookback", 20)),
        )
        self.trend_volatility_explosion_min_ratio = max(
            0.0,
            float(params.get("index_trend_volatility_explosion_min_ratio", 1.20)),
        )
        self.mean_reversion_outside_trend_session = self._as_bool(
            params.get("index_mean_reversion_outside_trend_session", True),
            True,
        )
        self.regime_selection_mode_requested = str(
            params.get("index_regime_selection_mode", "hard")
        ).strip().lower()
        # Fuzzy mode is intentionally removed to preserve strict dual-threshold
        # regime semantics (trend vs mean reversion).
        self.regime_selection_mode = "hard"
        self.regime_selection_mode_forced_hard = self.regime_selection_mode_requested != "hard"
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

        # Fast MA trailing stop params
        self._trailing_enabled = self._param_bool("index_hybrid_fast_ma_trailing_enabled", True)
        self._trailing_activation_r_multiple = self._param_float(
            "index_hybrid_fast_ma_trailing_activation_r_multiple", 0.8, min_val=0.0,
        )
        self._trailing_buffer_atr = self._param_float(
            "index_hybrid_fast_ma_trailing_buffer_atr", 0.15, min_val=0.0,
        )
        self._trailing_buffer_pips = self._param_float(
            "index_hybrid_fast_ma_trailing_buffer_pips", 0.0, min_val=0.0,
        )
        self._trailing_min_step_pips = self._param_float(
            "index_hybrid_fast_ma_trailing_min_step_pips", 0.5, min_val=0.0,
        )
        self._trailing_update_cooldown_sec = self._param_float(
            "index_hybrid_fast_ma_trailing_update_cooldown_sec", 5.0, min_val=0.0,
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

    def _trailing_payload(self, fast_ma_value: float) -> dict[str, object]:
        if not self._trailing_enabled:
            return {}
        return {
            "trailing_stop": {
                "trailing_enabled": True,
                "trailing_mode": "fast_ma",
                "trailing_activation_r_multiple": self._trailing_activation_r_multiple,
                "trailing_activation_min_profit_pips": 0.0,
                "fast_ma_value": fast_ma_value,
                "fast_ma_source": "fast_ema",
                "fast_ma_use_closed_candle": True,
                "fast_ma_buffer_atr": self._trailing_buffer_atr,
                "fast_ma_buffer_pips": self._trailing_buffer_pips,
                "fast_ma_min_step_pips": self._trailing_min_step_pips,
                "fast_ma_update_cooldown_sec": self._trailing_update_cooldown_sec,
            }
        }

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        return Strategy._as_bool(value, default, strict_strings=True)

    @staticmethod
    def _parse_float_mapping(raw: object) -> dict[str, float]:
        if raw in (None, ""):
            return {}
        if not isinstance(raw, dict):
            return {}
        parsed: dict[str, float] = {}
        for key, value in raw.items():
            symbol = normalize_symbol(str(key))
            if not symbol:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(number) or number <= 0.0:
                continue
            parsed[symbol] = number
        return parsed

    def _spread_pips_limit_for_symbol(self, symbol: str) -> tuple[float, str]:
        override = self.max_spread_pips_by_symbol.get(normalize_symbol(symbol))
        if override is not None and override > 0.0:
            return override, "symbol_override"
        return self.max_spread_pips, "global"

    @staticmethod
    def _parse_session_time_minutes(value: object, *, default_hour: int, default_minute: int = 0) -> int:
        default_total = ((default_hour % 24) * 60 + (default_minute % 60)) % (24 * 60)
        if value is None:
            return default_total
        if isinstance(value, bool):
            return default_total
        if isinstance(value, (int, float)):
            numeric = float(value)
            if not math.isfinite(numeric):
                return default_total
            return int(round(numeric * 60.0)) % (24 * 60)
        raw = str(value).strip()
        if not raw:
            return default_total
        if ":" in raw:
            parts = raw.split(":", 1)
            if len(parts) != 2:
                return default_total
            try:
                hour = int(parts[0])
                minute = int(parts[1])
            except ValueError:
                return default_total
            if minute < 0 or minute > 59:
                return default_total
            return ((hour % 24) * 60 + minute) % (24 * 60)
        try:
            numeric = float(raw)
        except ValueError:
            return default_total
        if not math.isfinite(numeric):
            return default_total
        return int(round(numeric * 60.0)) % (24 * 60)

    @staticmethod
    def _format_session_time(minutes: int) -> str:
        minute_of_day = minutes % (24 * 60)
        hour = minute_of_day // 60
        minute = minute_of_day % 60
        return f"{hour:02d}:{minute:02d}"

    def _session_window_utc_bounds(
        self,
        *,
        local_now: datetime,
        start_minute: int,
        end_minute: int,
    ) -> tuple[int, int, str, str, int]:
        offset = local_now.utcoffset()
        utc_offset_minutes = 0
        if offset is not None:
            utc_offset_minutes = int(round(offset.total_seconds() / 60.0))

        start_local = local_now.replace(
            hour=(start_minute // 60) % 24,
            minute=start_minute % 60,
            second=0,
            microsecond=0,
        )
        end_local = local_now.replace(
            hour=(end_minute // 60) % 24,
            minute=end_minute % 60,
            second=0,
            microsecond=0,
        )
        if start_minute != end_minute and start_minute > end_minute:
            local_minute_of_day = (local_now.hour * 60) + local_now.minute
            if local_minute_of_day < end_minute:
                start_local -= timedelta(days=1)
            else:
                end_local += timedelta(days=1)

        start_utc = start_local.astimezone(timezone.utc)
        end_utc = end_local.astimezone(timezone.utc)
        start_utc_minute = (start_utc.hour * 60) + start_utc.minute
        end_utc_minute = (end_utc.hour * 60) + end_utc.minute
        return (
            start_utc_minute,
            end_utc_minute,
            self._format_session_time(start_utc_minute),
            self._format_session_time(end_utc_minute),
            utc_offset_minutes,
        )

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        return is_index_symbol(symbol)

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

    def _trend_confidence(self, ema_gap_ratio: float, breakout_distance_ratio: float) -> float:
        gap_score = min(1.0, ema_gap_ratio / self.trend_confidence_gap_norm)
        breakout_score = min(1.0, max(0.0, breakout_distance_ratio) / self.trend_confidence_breakout_norm)
        raw = (
            self.trend_confidence_base
            + gap_score * self.trend_confidence_gap_weight
            + breakout_score * self.trend_confidence_breakout_weight
        )
        return min(1.0, max(0.0, raw))

    def _mean_reversion_confidence(self, extreme_abs_zscore: float) -> float:
        threshold = max(FLOAT_COMPARISON_TOLERANCE, self.zscore_threshold * 2.0)
        raw = self.mean_reversion_confidence_base + min(1.0, max(0.0, extreme_abs_zscore) / threshold)
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
        index_missing_volume_relaxed = self._is_index_symbol(ctx.symbol)
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
            "volume_data_available": False,
            "volume_spike": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
            "volume_current_source": None,
            "volume_current_deduplicated": False,
        }
        if not self.volume_confirmation:
            return metadata, None, 0.0

        raw_samples = self._extract_finite_positive_volumes(ctx.volumes[-(self.volume_window + 5) :])
        current_volume = self._coerce_positive_finite(ctx.current_volume)
        current_from_context = False
        current_deduplicated = False
        if current_volume is not None:
            if raw_samples and self._values_close(raw_samples[-1], current_volume):
                current_deduplicated = True
            else:
                raw_samples.append(current_volume)
                current_from_context = True

        if len(raw_samples) < 2:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = raw_samples[-1]
        history = raw_samples[:-1]
        if len(history) > self.volume_window:
            history = history[-self.volume_window :]
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
        metadata.update(
            {
                "volume_data_available": True,
                "volume_spike": spike,
                "volume_current": current,
                "volume_avg": avg_volume,
                "volume_ratio": volume_ratio,
                "volume_baseline_history_len": len(history),
                "volume_current_source": "current_volume" if current_from_context else "volumes",
                "volume_current_deduplicated": current_deduplicated,
            }
        )
        if self.volume_min_ratio_for_entry > 0.0 and volume_ratio < self.volume_min_ratio_for_entry:
            return metadata, "volume_below_entry_ratio", 0.0
        if self.volume_require_spike and not spike:
            return metadata, "volume_not_confirmed", 0.0
        return metadata, None, self.volume_confidence_boost if spike else 0.0

    def _volume_hold_blocks_entry(self, hold_reason: str | None) -> bool:
        if hold_reason is None:
            return False
        if hold_reason == "volume_below_entry_ratio":
            return True
        return not self.volume_as_bonus_only

    def _donchian_breakout(self, prices: Sequence[float]) -> tuple[bool, bool, float, float]:
        if len(prices) < (self.donchian_window + 1):
            return False, False, float(prices[-1]), float(prices[-1])
        reference = prices[-(self.donchian_window + 1) : -1]
        upper = float(max(reference))
        lower = float(min(reference))
        last = float(prices[-1])
        # Require first breakout cross, not repeated "still above band" highs.
        if len(prices) < (self.donchian_window + 2):
            return False, False, upper, lower
        prev = float(prices[-2])
        prev_reference = prices[-(self.donchian_window + 2) : -2]
        prev_upper = float(max(prev_reference))
        prev_lower = float(min(prev_reference))
        breakout_up = last > upper and prev <= prev_upper
        breakout_down = last < lower and prev >= prev_lower
        return breakout_up, breakout_down, upper, lower

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
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        return IndexHybridStrategy._minute_in_window(
            hour * 60,
            start_hour * 60,
            end_hour * 60,
        )

    @staticmethod
    def _minutes_since_session_start(current_minute: int, start_minute: int) -> int:
        return (current_minute - start_minute) % (24 * 60)

    def _rolling_channel_widths(self, prices: Sequence[float]) -> list[float]:
        if len(prices) < (self.donchian_window + 1):
            return []
        widths: list[float] = []
        span = self.donchian_window + 1
        for end in range(span, len(prices) + 1):
            reference = prices[end - span : end - 1]
            widths.append(max(reference) - min(reference))
        return widths

    def _median_sampling_interval_sec(self, timestamps: Sequence[float]) -> float | None:
        if len(timestamps) < 2:
            return None
        normalized: list[float] = []
        for raw in timestamps:
            try:
                ts = self._timestamp_to_seconds(float(raw))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts):
                continue
            normalized.append(ts)
        if len(normalized) < 2:
            return None
        deltas: list[float] = []
        previous = normalized[0]
        for current in normalized[1:]:
            gap = current - previous
            previous = current
            if not math.isfinite(gap) or gap <= 0:
                continue
            deltas.append(gap)
        if not deltas:
            return None
        deltas.sort()
        middle = len(deltas) // 2
        if len(deltas) % 2 == 1:
            return deltas[middle]
        return (deltas[middle - 1] + deltas[middle]) / 2.0

    def _resolve_timeframe_sec(
        self,
        timestamps: Sequence[float],
        *,
        using_closed_candles: bool,
    ) -> float:
        if using_closed_candles and self.candle_timeframe_sec > 1:
            return float(self.candle_timeframe_sec)
        inferred = self._median_sampling_interval_sec(timestamps)
        if inferred is not None and inferred > 0:
            return float(inferred)
        return float(self.default_timeframe_sec)

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
        return median_delta <= timeframe * 1.25

    def _resample_closed_candle_closes(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], list[float], bool, bool, str | None]:
        raw_prices = [float(value) for value in prices]
        raw_timestamps: list[float] = []
        for value in timestamps:
            try:
                ts = self._timestamp_to_seconds(float(value))
            except (TypeError, ValueError):
                return raw_prices, raw_timestamps, False, True, "timestamps_invalid"
            raw_timestamps.append(ts)
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return raw_prices, raw_timestamps, False, False, None
        if not timestamps or len(timestamps) != len(prices):
            return raw_prices, raw_timestamps, False, True, "timestamps_unavailable"
        if any(not math.isfinite(value) for value in raw_timestamps):
            return raw_prices, raw_timestamps, False, True, "timestamps_invalid"

        closes: list[float] = []
        close_timestamps: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_price, ts in zip(raw_prices, raw_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                closes.append(raw_price)
                close_timestamps.append(float(ts))
                last_bucket = bucket
            else:
                closes[-1] = raw_price
                close_timestamps[-1] = float(ts)

        # Always exclude the current candle in resample mode. In auto mode,
        # candle-like feeds may still include a forming last bar.
        if len(closes) < 2:
            return [], [], True, False, None
        return closes[:-1], close_timestamps[:-1], True, False, None

    def _session_profile_for_symbol(self, symbol: str) -> tuple[str, str, int, int]:
        market = resolve_index_market(symbol)
        if market == "america":
            return (
                "america",
                self.america_session_timezone,
                self.america_trend_session_start_minute_local,
                self.america_trend_session_end_minute_local,
            )
        if market == "japan":
            return (
                "japan_australia",
                self.japan_session_timezone,
                self.japan_trend_session_start_minute_local,
                self.japan_trend_session_end_minute_local,
            )
        if market == "australia":
            return (
                "japan_australia",
                self.australia_session_timezone,
                self.australia_trend_session_start_minute_local,
                self.australia_trend_session_end_minute_local,
            )
        if market == "europe":
            return (
                "europe",
                self.europe_session_timezone,
                self.europe_trend_session_start_minute_local,
                self.europe_trend_session_end_minute_local,
            )
        return (
            "europe",
            self.europe_session_timezone,
            self.europe_trend_session_start_minute_local,
            self.europe_trend_session_end_minute_local,
        )

    @staticmethod
    def _resolve_zoneinfo(timezone_name: str) -> tuple[timezone | ZoneInfo, bool]:
        try:
            return ZoneInfo(timezone_name), False
        except ZoneInfoNotFoundError:
            return timezone.utc, True

    def _trend_session_state(self, symbol: str, timestamps: Sequence[float]) -> dict[str, object]:
        state: dict[str, object] = {
            "trend_session": None,
            "session_hour_utc": None,
            "session_minute_utc": None,
            "session_hour_local": None,
            "session_minute_local": None,
            "trend_session_profile": None,
            "trend_session_timezone": None,
            "trend_session_timezone_fallback_utc": False,
            "trend_session_start_hour_active": None,
            "trend_session_end_hour_active": None,
            "trend_session_start_minute_active": None,
            "trend_session_end_minute_active": None,
            "trend_session_start_local_time_active": None,
            "trend_session_end_local_time_active": None,
            "trend_session_start_utc_minute_active": None,
            "trend_session_end_utc_minute_active": None,
            "trend_session_start_utc_time_active": None,
            "trend_session_end_utc_time_active": None,
            "trend_session_utc_offset_minutes": None,
            "session_profile_mode": self.session_profile_mode,
            "session_profile_mode_requested": self.session_profile_mode_requested,
            "trend_session_timestamp_source": "context",
            "trend_session_timestamp_fallback": False,
            "trend_session_timestamp_error": None,
        }
        if not timestamps:
            state["trend_session_timestamp_source"] = "context_missing"
            state["trend_session_timestamp_fallback"] = True
            state["trend_session_timestamp_error"] = "missing"
            return state
        else:
            try:
                now = datetime.fromtimestamp(self._timestamp_to_seconds(float(timestamps[-1])), tz=timezone.utc)
            except (ValueError, OSError, OverflowError, TypeError):
                state["trend_session_timestamp_source"] = "context_invalid"
                state["trend_session_timestamp_fallback"] = True
                state["trend_session_timestamp_error"] = "invalid"
                return state
        hour_utc = int(now.hour)
        minute_utc = int(now.minute)
        state["session_hour_utc"] = hour_utc
        state["session_minute_utc"] = (hour_utc * 60) + minute_utc

        if self.session_profile_mode == "legacy_utc":
            start_utc_minute = self.trend_session_start_hour_utc * 60
            end_utc_minute = self.trend_session_end_hour_utc * 60
            session_minute_utc = (hour_utc * 60) + minute_utc
            state.update(
                {
                    "trend_session": self._minute_in_window(
                        session_minute_utc,
                        start_utc_minute,
                        end_utc_minute,
                    ),
                    "session_hour_local": hour_utc,
                    "session_minute_local": session_minute_utc,
                    "trend_session_profile": "legacy_utc",
                    "trend_session_timezone": "UTC",
                    "trend_session_start_hour_active": self.trend_session_start_hour_utc,
                    "trend_session_end_hour_active": self.trend_session_end_hour_utc,
                    "trend_session_start_minute_active": start_utc_minute,
                    "trend_session_end_minute_active": end_utc_minute,
                    "trend_session_start_local_time_active": self._format_session_time(start_utc_minute),
                    "trend_session_end_local_time_active": self._format_session_time(end_utc_minute),
                    "trend_session_start_utc_minute_active": start_utc_minute,
                    "trend_session_end_utc_minute_active": end_utc_minute,
                    "trend_session_start_utc_time_active": self._format_session_time(start_utc_minute),
                    "trend_session_end_utc_time_active": self._format_session_time(end_utc_minute),
                    "trend_session_utc_offset_minutes": 0,
                }
            )
            return state

        profile, timezone_name, start_minute, end_minute = self._session_profile_for_symbol(symbol)
        tzinfo, tz_fallback = self._resolve_zoneinfo(timezone_name)
        local_now = now.astimezone(tzinfo)
        (
            start_utc_minute,
            end_utc_minute,
            start_utc_time,
            end_utc_time,
            utc_offset_minutes,
        ) = self._session_window_utc_bounds(
            local_now=local_now,
            start_minute=start_minute,
            end_minute=end_minute,
        )
        local_hour = int(local_now.hour)
        local_minute = int(local_now.minute)
        minute_of_day_local = (local_hour * 60) + local_minute
        state.update(
            {
                "trend_session": self._minute_in_window(minute_of_day_local, start_minute, end_minute),
                "session_hour_local": local_hour,
                "session_minute_local": minute_of_day_local,
                "trend_session_profile": profile,
                "trend_session_timezone": timezone_name if not tz_fallback else "UTC",
                "trend_session_timezone_fallback_utc": tz_fallback,
                "trend_session_start_hour_active": start_minute // 60,
                "trend_session_end_hour_active": end_minute // 60,
                "trend_session_start_minute_active": start_minute,
                "trend_session_end_minute_active": end_minute,
                "trend_session_start_local_time_active": self._format_session_time(start_minute),
                "trend_session_end_local_time_active": self._format_session_time(end_minute),
                "trend_session_start_utc_minute_active": start_utc_minute,
                "trend_session_end_utc_minute_active": end_utc_minute,
                "trend_session_start_utc_time_active": start_utc_time,
                "trend_session_end_utc_time_active": end_utc_time,
                "trend_session_utc_offset_minutes": utc_offset_minutes,
            }
        )
        return state

    def _resolve_regime_state(self, ema_gap_ratio: float, atr_pct: float) -> dict[str, object]:
        trend_regime_strict = (
            ema_gap_ratio >= self.trend_gap_threshold_effective
            and atr_pct >= self.trend_atr_pct_threshold_effective
        )
        mean_reversion_regime_strict = (
            ema_gap_ratio <= self.mean_reversion_gap_threshold_effective
            and atr_pct <= self.mean_reversion_atr_pct_threshold_effective
        )
        trend_regime = trend_regime_strict
        mean_reversion_regime = mean_reversion_regime_strict
        fallback_applied = False
        fallback_selected: str | None = None
        trend_distance: float | None = None
        mean_reversion_distance: float | None = None
        trend_power = ema_gap_ratio / max(self.trend_gap_threshold_effective, FLOAT_COMPARISON_TOLERANCE)
        volatility_power = atr_pct / max(self.trend_atr_pct_threshold_effective, FLOAT_COMPARISON_TOLERANCE)
        regime_index = (trend_power + volatility_power) / 2.0

        if not trend_regime and not mean_reversion_regime:
            if self.regime_fallback_mode == "trend_following":
                trend_regime = True
                fallback_applied = True
                fallback_selected = "trend_following"
            elif self.regime_fallback_mode == "mean_reversion":
                mean_reversion_regime = True
                fallback_applied = True
                fallback_selected = "mean_reversion"
            elif self.regime_fallback_mode == "nearest":
                trend_gap_deficit = max(0.0, self.trend_gap_threshold_effective - ema_gap_ratio) / max(
                    self.trend_gap_threshold_effective, FLOAT_COMPARISON_TOLERANCE
                )
                trend_atr_deficit = max(0.0, self.trend_atr_pct_threshold_effective - atr_pct) / max(
                    self.trend_atr_pct_threshold_effective, FLOAT_COMPARISON_TOLERANCE
                )
                trend_distance = math.hypot(trend_gap_deficit, trend_atr_deficit)

                mean_gap_excess = max(0.0, ema_gap_ratio - self.mean_reversion_gap_threshold_effective) / max(
                    self.mean_reversion_gap_threshold_effective, FLOAT_COMPARISON_TOLERANCE
                )
                mean_atr_excess = max(0.0, atr_pct - self.mean_reversion_atr_pct_threshold_effective) / max(
                    self.mean_reversion_atr_pct_threshold_effective, FLOAT_COMPARISON_TOLERANCE
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
            "regime_selection_mode_requested": self.regime_selection_mode_requested,
            "regime_selection_mode_forced_hard": self.regime_selection_mode_forced_hard,
            "regime_index": regime_index,
            "regime_trend_index_threshold": self.regime_trend_index_threshold,
            "trend_power": trend_power,
            "volatility_power": volatility_power,
        }

    def _dynamic_sl_tp(
        self,
        ctx: StrategyContext,
        last_price: float,
        atr: float,
        *,
        risk_profile: str,
    ) -> tuple[float, float, dict[str, float | str]]:
        if risk_profile == "mean_reversion":
            base_stop_pips = self.mean_reversion_stop_loss_pips
            base_take_pips = self.mean_reversion_take_profit_pips
            stop_loss_pct = self.mean_reversion_stop_loss_pct
            take_profit_pct = self.mean_reversion_take_profit_pct
            stop_atr_multiplier = self.mean_reversion_stop_atr_multiplier
            risk_reward_ratio = self.mean_reversion_risk_reward_ratio
            pct_floors_enabled = self.mean_reversion_pct_floors_enabled
        else:
            base_stop_pips = self.trend_stop_loss_pips
            base_take_pips = self.trend_take_profit_pips
            stop_loss_pct = self.trend_stop_loss_pct
            take_profit_pct = self.trend_take_profit_pct
            stop_atr_multiplier = self.trend_stop_atr_multiplier
            risk_reward_ratio = self.trend_risk_reward_ratio
            pct_floors_enabled = self.trend_pct_floors_enabled

        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        stop_from_atr = atr_pips * stop_atr_multiplier
        stop_from_pct_raw = ((last_price * stop_loss_pct) / 100.0) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        take_from_pct_raw = ((last_price * take_profit_pct) / 100.0) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        stop_from_pct = stop_from_pct_raw if pct_floors_enabled else 0.0
        take_from_pct = take_from_pct_raw if pct_floors_enabled else 0.0
        stop_loss_pips = max(base_stop_pips, stop_from_atr, stop_from_pct)
        take_profit_pips = max(base_take_pips, stop_loss_pips * risk_reward_ratio, take_from_pct)
        return (
            stop_loss_pips,
            take_profit_pips,
            {
                "risk_profile": risk_profile,
                "risk_profile_stop_loss_pips_base": base_stop_pips,
                "risk_profile_take_profit_pips_base": base_take_pips,
                "risk_profile_stop_loss_pct": stop_loss_pct,
                "risk_profile_take_profit_pct": take_profit_pct,
                "risk_profile_stop_atr_multiplier": stop_atr_multiplier,
                "risk_profile_risk_reward_ratio": risk_reward_ratio,
                "risk_profile_pct_floors_enabled": pct_floors_enabled,
                "atr_pips": atr_pips,
                "stop_from_atr_pips": stop_from_atr,
                "stop_from_pct_pips": stop_from_pct,
                "take_from_pct_pips": take_from_pct,
                "stop_from_pct_pips_raw": stop_from_pct_raw,
                "take_from_pct_pips_raw": take_from_pct_raw,
                "pip_size": pip_size,
                "pip_size_source": pip_size_source,
            },
        )

    @staticmethod
    def _select_risk_profile(
        *,
        trend_regime: bool,
        mean_reversion_regime: bool,
    ) -> str:
        if mean_reversion_regime and not trend_regime:
            return "mean_reversion"
        return "trend_following"

    def _confidence_threshold_cap_for_regime(self, regime: str) -> float | None:
        if not self.confidence_threshold_cap_enabled:
            return None
        if regime == "mean_reversion":
            cap = self.confidence_threshold_cap_mean_reversion
        else:
            cap = self.confidence_threshold_cap_trend
        if cap <= 0.0:
            return None
        return max(0.0, min(1.0, float(cap)))

    def _spread_filter(
        self,
        *,
        symbol: str,
        spread_pips: float | None,
        stop_loss_pips: float,
        breakout_distance_pips: float | None = None,
    ) -> tuple[str | None, dict[str, float | None]]:
        spread_to_stop_ratio: float | None = None
        spread_to_breakout_ratio: float | None = None
        reason: str | None = None
        max_spread_pips_limit = self.max_spread_pips
        max_spread_pips_source = "global"

        if spread_pips is not None and spread_pips > 0.0:
            max_spread_pips_limit, max_spread_pips_source = self._spread_pips_limit_for_symbol(symbol)
            if stop_loss_pips > 0.0:
                spread_to_stop_ratio = spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
            if breakout_distance_pips is not None and breakout_distance_pips > 0.0:
                spread_to_breakout_ratio = spread_pips / breakout_distance_pips

            if max_spread_pips_limit > 0.0 and spread_pips > max_spread_pips_limit:
                reason = "spread_too_wide"
            elif (
                self.max_spread_to_breakout_ratio > 0.0
                and spread_to_breakout_ratio is not None
                and spread_to_breakout_ratio > self.max_spread_to_breakout_ratio
            ):
                reason = "spread_too_wide_for_breakout"
            elif (
                self.max_spread_to_stop_ratio > 0.0
                and spread_to_stop_ratio is not None
                and spread_to_stop_ratio > self.max_spread_to_stop_ratio
            ):
                reason = "spread_too_wide_for_stop"

        return (
            reason,
            {
                "spread_pips": spread_pips,
                "index_max_spread_pips": max_spread_pips_limit,
                "index_max_spread_pips_source": max_spread_pips_source,
                "index_max_spread_to_breakout_ratio": self.max_spread_to_breakout_ratio,
                "index_max_spread_to_stop_ratio": self.max_spread_to_stop_ratio,
                "spread_to_stop_ratio": spread_to_stop_ratio,
                "spread_to_breakout_ratio": spread_to_breakout_ratio,
                "breakout_distance_pips": breakout_distance_pips,
            },
        )

    @staticmethod
    def _exit_signal_metadata(
        *,
        trend_regime: bool,
        mean_reversion_regime: bool,
        last_price: float,
        channel_mid: float,
        channel_upper: float,
        channel_lower: float,
    ) -> dict[str, object]:
        trend_exit_buy_reentry_mid = False
        trend_exit_sell_reentry_mid = False
        mean_exit_buy_mid_target = False
        mean_exit_sell_mid_target = False
        mean_exit_buy_opposite_band = False
        mean_exit_sell_opposite_band = False
        exit_hint: str | None = None

        if trend_regime and not mean_reversion_regime:
            trend_exit_buy_reentry_mid = last_price <= channel_mid
            trend_exit_sell_reentry_mid = last_price >= channel_mid
            if trend_exit_buy_reentry_mid or trend_exit_sell_reentry_mid:
                exit_hint = "close_if_price_reenters_channel_mid"

        if mean_reversion_regime and not trend_regime:
            mean_exit_buy_mid_target = last_price >= channel_mid
            mean_exit_sell_mid_target = last_price <= channel_mid
            mean_exit_buy_opposite_band = last_price >= channel_upper
            mean_exit_sell_opposite_band = last_price <= channel_lower
            if (
                mean_exit_buy_mid_target
                or mean_exit_sell_mid_target
                or mean_exit_buy_opposite_band
                or mean_exit_sell_opposite_band
            ):
                exit_hint = "close_if_price_reaches_channel_mid_or_opposite_band"

        payload: dict[str, object] = {
            "trend_exit_buy_reentry_mid": trend_exit_buy_reentry_mid,
            "trend_exit_sell_reentry_mid": trend_exit_sell_reentry_mid,
            "mean_exit_buy_mid_target": mean_exit_buy_mid_target,
            "mean_exit_sell_mid_target": mean_exit_sell_mid_target,
            "mean_exit_buy_opposite_band": mean_exit_buy_opposite_band,
            "mean_exit_sell_opposite_band": mean_exit_sell_opposite_band,
            "exit_signal_active": bool(exit_hint),
        }
        if exit_hint is not None:
            payload["exit_hint"] = exit_hint
        return payload

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

    def _soft_filter_penalty(self, reason: str | None) -> float:
        if reason is None:
            return 0.0
        return self._SOFT_FILTER_PENALTIES.get(str(reason).strip().lower(), 0.0)

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        if not self._is_index_symbol(ctx.symbol):
            return self._hold("not_index_symbol", self.stop_loss_pips, self.take_profit_pips)
        if self.require_context_tick_size and not self._has_context_pip_size(ctx):
            return self._hold(
                "tick_size_unavailable",
                self.stop_loss_pips,
                self.take_profit_pips,
                {"index_require_context_tick_size": True},
            )

        raw_prices: list[float] = []
        raw_timestamps: list[float] = []
        invalid_prices = 0
        timestamps_usable = bool(ctx.timestamps) and len(ctx.timestamps) == len(ctx.prices)
        for idx, raw_price in enumerate(ctx.prices):
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                invalid_prices += 1
                continue
            if not math.isfinite(price):
                invalid_prices += 1
                continue
            raw_prices.append(price)
            if timestamps_usable:
                try:
                    ts = self._timestamp_to_seconds(float(ctx.timestamps[idx]))
                except (TypeError, ValueError):
                    timestamps_usable = False
                    raw_timestamps = []
                else:
                    if not math.isfinite(ts):
                        timestamps_usable = False
                        raw_timestamps = []
                    else:
                        raw_timestamps.append(float(ts))
        if not timestamps_usable:
            raw_timestamps = []

        if len(raw_prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                self.stop_loss_pips,
                self.take_profit_pips,
                {
                    "history": len(raw_prices),
                    "required_history": self.min_history,
                    "invalid_prices_dropped": invalid_prices,
                },
            )

        prices, price_timestamps, using_closed_candles, timeframe_normalization_degraded, timeframe_normalization_reason = (
            self._resample_closed_candle_closes(raw_prices, raw_timestamps)
        )
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                self.stop_loss_pips,
                self.take_profit_pips,
                {
                    "history": len(prices),
                    "required_history": self.min_history,
                    "raw_history": len(raw_prices),
                    "using_closed_candles": using_closed_candles,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "timeframe_normalization_degraded": timeframe_normalization_degraded,
                    "timeframe_normalization_reason": timeframe_normalization_reason,
                    "invalid_prices_dropped": invalid_prices,
                },
            )

        timeframe_sec = self._resolve_timeframe_sec(
            price_timestamps if price_timestamps else raw_timestamps,
            using_closed_candles=using_closed_candles,
        )

        fast_now = self._ema(prices, self.fast_ema_window)
        slow_now = self._ema(prices, self.slow_ema_window)
        fast_prev = self._ema(prices[:-1], self.fast_ema_window)
        slow_prev = self._ema(prices[:-1], self.slow_ema_window)
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)
        fast_slope_ratio = (fast_now - fast_prev) / max(abs(fast_prev), FLOAT_COMPARISON_TOLERANCE)
        slow_slope_ratio = (slow_now - slow_prev) / max(abs(slow_prev), FLOAT_COMPARISON_TOLERANCE)
        trend_slope_up_confirmed = fast_now > fast_prev and slow_now > slow_prev
        trend_slope_down_confirmed = fast_now < fast_prev and slow_now < slow_prev
        breakout_up, breakout_down, channel_upper, channel_lower = self._donchian_breakout(prices)
        last = float(prices[-1])
        z_values = prices[-self.zscore_window :]
        if self.zscore_mode == "classic":
            z = self._zscore(z_values)
            z_prev = self._zscore(z_values[:-1])
        else:
            z = self._zscore_detrended(z_values)
            z_prev = self._zscore_detrended(z_values[:-1])
        mean_reversion_extreme_abs_zscore = max(abs(z), abs(z_prev))
        mean_reversion_buy_extreme = z <= -self.zscore_threshold
        mean_reversion_sell_extreme = z >= self.zscore_threshold
        mean_reversion_buy_hook_triggered = z_prev <= -self.zscore_threshold and z > -self.zscore_threshold
        mean_reversion_sell_hook_triggered = z_prev >= self.zscore_threshold and z < self.zscore_threshold
        if self.mean_reversion_entry_mode == "hook":
            mean_reversion_buy_signal_ready = mean_reversion_buy_hook_triggered
            mean_reversion_sell_signal_ready = mean_reversion_sell_hook_triggered
        else:
            mean_reversion_buy_signal_ready = mean_reversion_buy_extreme
            mean_reversion_sell_signal_ready = mean_reversion_sell_extreme

        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return self._hold(
                "atr_unavailable",
                self.stop_loss_pips,
                self.take_profit_pips,
                {"atr_window": self.atr_window},
            )
        atr_pct = (atr / max(abs(last), FLOAT_COMPARISON_TOLERANCE)) * 100.0

        session_state = self._trend_session_state(
            ctx.symbol,
            price_timestamps if price_timestamps else raw_timestamps,
        )
        trend_session = session_state.get("trend_session")
        session_hour_utc = session_state.get("session_hour_utc")
        session_minute_local_raw = session_state.get("session_minute_local")
        trend_session_start_minute_raw = session_state.get("trend_session_start_minute_active")

        minutes_since_trend_session_open: int | None = None
        trend_session_open_delay_active = False
        trend_session_open_delay_remaining_minutes = 0
        if (
            self.session_filter_enabled
            and trend_session is True
            and isinstance(session_minute_local_raw, int | float)
            and isinstance(trend_session_start_minute_raw, int | float)
        ):
            minutes_since_trend_session_open = self._minutes_since_session_start(
                int(session_minute_local_raw),
                int(trend_session_start_minute_raw),
            )
            if (
                self.trend_session_open_delay_minutes > 0
                and minutes_since_trend_session_open < self.trend_session_open_delay_minutes
            ):
                trend_session_open_delay_active = True
                trend_session_open_delay_remaining_minutes = (
                    self.trend_session_open_delay_minutes - minutes_since_trend_session_open
                )

        trend_session_allowed = True
        mean_reversion_session_allowed = True
        if self.session_filter_enabled and trend_session is not None:
            trend_session_allowed = trend_session
            if self.mean_reversion_outside_trend_session:
                mean_reversion_session_allowed = not trend_session

        regime_state = self._resolve_regime_state(ema_gap_ratio, atr_pct)
        trend_regime = bool(regime_state["trend_regime"])
        mean_reversion_regime = bool(regime_state["mean_reversion_regime"])
        active_risk_profile = self._select_risk_profile(
            trend_regime=trend_regime,
            mean_reversion_regime=mean_reversion_regime,
        )
        stop_loss_pips, take_profit_pips, risk_meta = self._dynamic_sl_tp(
            ctx,
            last,
            atr,
            risk_profile=active_risk_profile,
        )
        pip_size_source = str(risk_meta.get("pip_size_source") or "")
        if self.require_non_fallback_pip_size and pip_size_source == "fallback_symbol_map":
            return self._hold(
                "pip_size_source_fallback_blocked",
                stop_loss_pips,
                take_profit_pips,
                {
                    "index_require_non_fallback_pip_size": self.require_non_fallback_pip_size,
                    "index_require_context_tick_size": self.require_context_tick_size,
                    "pip_size_source": pip_size_source,
                },
            )
        spread_pips: float | None = None
        if ctx.current_spread_pips is not None:
            try:
                spread_pips = max(0.0, float(ctx.current_spread_pips))
            except (TypeError, ValueError):
                spread_pips = None
        pip_size = max(float(risk_meta.get("pip_size", self._pip_size(ctx.symbol))), FLOAT_COMPARISON_TOLERANCE)
        channel_mid = (channel_upper + channel_lower) / 2.0
        channel_width = max(0.0, channel_upper - channel_lower)
        channel_width_atr = channel_width / max(atr, FLOAT_COMPARISON_TOLERANCE)
        trend_channel_expansion_ratio: float | None = None
        trend_channel_expansion_baseline_width: float | None = None
        trend_channel_expansion_history_len = 0
        trend_volatility_explosion_ready = not self.trend_volatility_explosion_filter_enabled
        trend_volatility_explosion_passed: bool | None = None
        if self.trend_volatility_explosion_filter_enabled:
            width_history = self._rolling_channel_widths(prices)
            if len(width_history) >= (self.trend_volatility_explosion_lookback + 1):
                baseline_history = width_history[-(self.trend_volatility_explosion_lookback + 1) : -1]
                trend_channel_expansion_history_len = len(baseline_history)
                if baseline_history:
                    trend_channel_expansion_baseline_width = sum(baseline_history) / len(baseline_history)
                if trend_channel_expansion_baseline_width and trend_channel_expansion_baseline_width > 0.0:
                    trend_channel_expansion_ratio = channel_width / trend_channel_expansion_baseline_width
                    trend_volatility_explosion_ready = True
                    trend_volatility_explosion_passed = (
                        trend_channel_expansion_ratio >= self.trend_volatility_explosion_min_ratio
                    )
        channel_width_ratio_den = max(channel_width, FLOAT_COMPARISON_TOLERANCE)
        mr_distance_to_lower_ratio = max(0.0, last - channel_lower) / channel_width_ratio_den
        mr_distance_to_upper_ratio = max(0.0, channel_upper - last) / channel_width_ratio_den
        mr_buy_mid_deviation_ratio = max(0.0, channel_mid - last) / channel_width_ratio_den
        mr_sell_mid_deviation_ratio = max(0.0, last - channel_mid) / channel_width_ratio_den
        exit_meta = self._exit_signal_metadata(
            trend_regime=trend_regime,
            mean_reversion_regime=mean_reversion_regime,
            last_price=last,
            channel_mid=channel_mid,
            channel_upper=channel_upper,
            channel_lower=channel_lower,
        )
        regime_flip_exit_buy = bool(regime_state["trend_regime_strict"]) and not mean_reversion_regime and fast_now < slow_now
        regime_flip_exit_sell = bool(regime_state["trend_regime_strict"]) and not mean_reversion_regime and fast_now > slow_now
        max_spread_pips_limit, max_spread_pips_source = self._spread_pips_limit_for_symbol(ctx.symbol)
        base_meta: dict[str, object] = {
            "zscore": z,
            "zscore_prev": z_prev,
            "zscore_mode": self.zscore_mode,
            "zscore_mode_requested": self.zscore_mode_requested,
            "zscore_mode_forced_detrended": self.zscore_mode_forced_detrended,
            "zscore_ema_window": self.zscore_ema_window,
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
            "trend_gap_threshold_configured": self.trend_gap_threshold_configured,
            "mean_reversion_gap_threshold_configured": self.mean_reversion_gap_threshold_configured,
            "trend_atr_pct_threshold_configured": self.trend_atr_pct_threshold_configured,
            "mean_reversion_atr_pct_threshold_configured": self.mean_reversion_atr_pct_threshold_configured,
            "trend_gap_threshold": self.trend_gap_threshold,
            "mean_reversion_gap_threshold": self.mean_reversion_gap_threshold,
            "trend_atr_pct_threshold": self.trend_atr_pct_threshold,
            "mean_reversion_atr_pct_threshold": self.mean_reversion_atr_pct_threshold,
            "trend_gap_threshold_effective": self.trend_gap_threshold_effective,
            "mean_reversion_gap_threshold_effective": self.mean_reversion_gap_threshold_effective,
            "trend_atr_pct_threshold_effective": self.trend_atr_pct_threshold_effective,
            "mean_reversion_atr_pct_threshold_effective": self.mean_reversion_atr_pct_threshold_effective,
            "index_regime_selection_mode": self.regime_selection_mode,
            "index_regime_selection_mode_requested": self.regime_selection_mode_requested,
            "index_regime_selection_mode_forced_hard": self.regime_selection_mode_forced_hard,
            "index_regime_trend_index_threshold": self.regime_trend_index_threshold,
            "index_regime_trend_index_threshold_active": False,
            "index_regime_fallback_mode": self.regime_fallback_mode,
            "index_enforce_gap_hysteresis": self.enforce_gap_hysteresis,
            "index_gap_hysteresis_min": self.gap_hysteresis_min,
            "index_gap_hysteresis_applied": self.gap_hysteresis_applied,
            "index_parameter_surface_mode": self.params.get("index_parameter_surface_mode", "open"),
            "index_parameter_surface_tier": self.params.get("index_parameter_surface_tier"),
            "index_parameter_surface_locked": self.params.get("index_parameter_surface_locked", False),
            "index_parameter_surface_allowed_overrides": self.params.get(
                "index_parameter_surface_allowed_overrides",
                [],
            ),
            "index_parameter_surface_ignored_count": self.params.get("index_parameter_surface_ignored_count", 0),
            "index_volume_as_bonus_only": self.volume_as_bonus_only,
            "index_require_context_tick_size": self.require_context_tick_size,
            "index_require_non_fallback_pip_size": self.require_non_fallback_pip_size,
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
            "regime_selection_mode_requested": regime_state["regime_selection_mode_requested"],
            "regime_selection_mode_forced_hard": regime_state["regime_selection_mode_forced_hard"],
            "regime_index": regime_state["regime_index"],
            "regime_trend_index_threshold": regime_state["regime_trend_index_threshold"],
            "trend_power": regime_state["trend_power"],
            "volatility_power": regime_state["volatility_power"],
            "trend_session_allowed": trend_session_allowed,
            "mean_reversion_session_allowed": mean_reversion_session_allowed,
            "session_hour_utc": session_hour_utc,
            "session_minute_utc": session_state.get("session_minute_utc"),
            "session_hour_local": session_state.get("session_hour_local"),
            "session_minute_local": session_state.get("session_minute_local"),
            "session_profile_mode": session_state.get("session_profile_mode"),
            "session_profile_mode_requested": session_state.get("session_profile_mode_requested"),
            "trend_session_profile": session_state.get("trend_session_profile"),
            "trend_session_timezone": session_state.get("trend_session_timezone"),
            "trend_session_timezone_fallback_utc": session_state.get("trend_session_timezone_fallback_utc"),
            "trend_session_start_hour_active": session_state.get("trend_session_start_hour_active"),
            "trend_session_end_hour_active": session_state.get("trend_session_end_hour_active"),
            "trend_session_start_minute_active": session_state.get("trend_session_start_minute_active"),
            "trend_session_end_minute_active": session_state.get("trend_session_end_minute_active"),
            "trend_session_start_local_time_active": session_state.get("trend_session_start_local_time_active"),
            "trend_session_end_local_time_active": session_state.get("trend_session_end_local_time_active"),
            "trend_session_start_utc_minute_active": session_state.get("trend_session_start_utc_minute_active"),
            "trend_session_end_utc_minute_active": session_state.get("trend_session_end_utc_minute_active"),
            "trend_session_start_utc_time_active": session_state.get("trend_session_start_utc_time_active"),
            "trend_session_end_utc_time_active": session_state.get("trend_session_end_utc_time_active"),
            "trend_session_utc_offset_minutes": session_state.get("trend_session_utc_offset_minutes"),
            "trend_session_timestamp_source": session_state.get("trend_session_timestamp_source"),
            "trend_session_timestamp_fallback": session_state.get("trend_session_timestamp_fallback"),
            "trend_session_timestamp_error": session_state.get("trend_session_timestamp_error"),
            "index_trend_session_open_delay_minutes": self.trend_session_open_delay_minutes,
            "minutes_since_trend_session_open": minutes_since_trend_session_open,
            "trend_session_open_delay_active": trend_session_open_delay_active,
            "trend_session_open_delay_remaining_minutes": trend_session_open_delay_remaining_minutes,
            "session_filter_degraded": bool(self.session_filter_enabled and trend_session is None),
            "channel_upper": channel_upper,
            "channel_mid": channel_mid,
            "channel_lower": channel_lower,
            "channel_width": channel_width,
            "channel_width_atr": channel_width_atr,
            "index_trend_volatility_explosion_filter_enabled": self.trend_volatility_explosion_filter_enabled,
            "index_trend_volatility_explosion_lookback": self.trend_volatility_explosion_lookback,
            "index_trend_volatility_explosion_min_ratio": self.trend_volatility_explosion_min_ratio,
            "trend_channel_expansion_ratio": trend_channel_expansion_ratio,
            "trend_channel_expansion_baseline_width": trend_channel_expansion_baseline_width,
            "trend_channel_expansion_history_len": trend_channel_expansion_history_len,
            "trend_volatility_explosion_ready": trend_volatility_explosion_ready,
            "trend_volatility_explosion_passed": trend_volatility_explosion_passed,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "index_mean_reversion_entry_mode": self.mean_reversion_entry_mode,
            "mean_reversion_allow_breakout": self.mean_reversion_allow_breakout,
            "mean_reversion_breakout_extreme_multiplier": self.mean_reversion_breakout_extreme_multiplier,
            "index_mean_reversion_require_band_proximity": self.mean_reversion_require_band_proximity,
            "index_mean_reversion_band_proximity_max_ratio": self.mean_reversion_band_proximity_max_ratio,
            "index_mean_reversion_min_mid_deviation_ratio": self.mean_reversion_min_mid_deviation_ratio,
            "mean_reversion_distance_to_lower_ratio": mr_distance_to_lower_ratio,
            "mean_reversion_distance_to_upper_ratio": mr_distance_to_upper_ratio,
            "mean_reversion_buy_mid_deviation_ratio": mr_buy_mid_deviation_ratio,
            "mean_reversion_sell_mid_deviation_ratio": mr_sell_mid_deviation_ratio,
            "mean_reversion_extreme_abs_zscore": mean_reversion_extreme_abs_zscore,
            "mean_reversion_buy_extreme": mean_reversion_buy_extreme,
            "mean_reversion_sell_extreme": mean_reversion_sell_extreme,
            "mean_reversion_buy_hook_triggered": mean_reversion_buy_hook_triggered,
            "mean_reversion_sell_hook_triggered": mean_reversion_sell_hook_triggered,
            "mean_reversion_buy_signal_ready": mean_reversion_buy_signal_ready,
            "mean_reversion_sell_signal_ready": mean_reversion_sell_signal_ready,
            "index_min_breakout_distance_ratio": self.min_breakout_distance_ratio,
            "index_max_spread_pips": max_spread_pips_limit,
            "index_max_spread_pips_source": max_spread_pips_source,
            "index_max_spread_to_breakout_ratio": self.max_spread_to_breakout_ratio,
            "index_max_spread_to_stop_ratio": self.max_spread_to_stop_ratio,
            "spread_pips": spread_pips,
            "index_min_channel_width_atr": self.min_channel_width_atr,
            "index_volume_confirmation": self.volume_confirmation,
            "index_volume_window": self.volume_window,
            "index_min_volume_ratio": self.min_volume_ratio,
            "index_volume_min_ratio_for_entry": self.volume_min_ratio_for_entry,
            "index_volume_min_samples": self.volume_min_samples,
            "index_volume_allow_missing": self.volume_allow_missing,
            "index_volume_require_spike": self.volume_require_spike,
            "index_volume_confidence_boost": self.volume_confidence_boost,
            "index_confidence_threshold_cap_enabled": self.confidence_threshold_cap_enabled,
            "index_confidence_threshold_cap_trend": self.confidence_threshold_cap_trend,
            "index_confidence_threshold_cap_mean_reversion": self.confidence_threshold_cap_mean_reversion,
            "fast_ema": fast_now,
            "slow_ema": slow_now,
            "fast_ema_prev": fast_prev,
            "slow_ema_prev": slow_prev,
            "fast_slope_ratio": fast_slope_ratio,
            "slow_slope_ratio": slow_slope_ratio,
            "trend_slope_up_confirmed": trend_slope_up_confirmed,
            "trend_slope_down_confirmed": trend_slope_down_confirmed,
            "regime_flip_exit_buy": regime_flip_exit_buy,
            "regime_flip_exit_sell": regime_flip_exit_sell,
            "timeframe_sec": timeframe_sec,
            "default_timeframe_sec": self.default_timeframe_sec,
            "candle_timeframe_sec": self.candle_timeframe_sec,
            "resample_mode": self.resample_mode,
            "ignore_sunday_candles": self.ignore_sunday_candles,
            "using_closed_candles": using_closed_candles,
            "timeframe_normalization_degraded": timeframe_normalization_degraded,
            "timeframe_normalization_reason": timeframe_normalization_reason,
            "raw_history": len(raw_prices),
            "resampled_history": len(prices),
            "raw_timestamps_count": len(raw_timestamps),
            "resampled_timestamps_count": len(price_timestamps),
            "invalid_prices_dropped": invalid_prices,
            **exit_meta,
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

        if trend_regime and fast_now > slow_now and breakout_up and trend_slope_up_confirmed:
            soft_filter_reasons: list[str] = []
            soft_filter_penalty_total = 0.0
            breakout_distance_ratio = max(0.0, last - channel_upper) / max(abs(last), FLOAT_COMPARISON_TOLERANCE)
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
            breakout_distance_pips = max(0.0, last - channel_upper) / pip_size
            spread_hold_reason, spread_meta = self._spread_filter(
                symbol=ctx.symbol,
                spread_pips=spread_pips,
                stop_loss_pips=stop_loss_pips,
                breakout_distance_pips=breakout_distance_pips,
            )
            if spread_hold_reason is not None:
                return self._hold(
                    spread_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "up",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            if volume_hold_reason == "volume_below_entry_ratio":
                return self._hold(
                    volume_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "up",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            volume_softened = False
            if self._volume_hold_blocks_entry(volume_hold_reason):
                volume_softened = True
                soft_filter_reasons.append(volume_hold_reason)
                soft_filter_penalty_total += self._soft_filter_penalty(volume_hold_reason)
            if not trend_session_allowed:
                soft_filter_reasons.append("trend_blocked_by_session")
                soft_filter_penalty_total += self._soft_filter_penalty("trend_blocked_by_session")
            if trend_session_open_delay_active:
                soft_filter_reasons.append("trend_waiting_session_open_stabilization")
                soft_filter_penalty_total += self._soft_filter_penalty(
                    "trend_waiting_session_open_stabilization"
                )
            if self.trend_volatility_explosion_filter_enabled and trend_volatility_explosion_passed is not True:
                return self._hold(
                    "waiting_for_volatility_explosion",
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "up",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            confidence = self._trend_confidence(ema_gap_ratio, breakout_distance_ratio)
            confidence = min(1.0, confidence + volume_confidence_boost)
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            confidence_threshold_cap = self._confidence_threshold_cap_for_regime("trend_following")
            signal = Signal(
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
                    "volume_softened": volume_softened,
                    "exit_hint": "close_if_price_reenters_channel_mid",
                    "confidence_threshold_cap": confidence_threshold_cap,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    **spread_meta,
                    **base_meta,
                    **self._trailing_payload(fast_now),
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if trend_regime and fast_now < slow_now and breakout_down and trend_slope_down_confirmed:
            soft_filter_reasons: list[str] = []
            soft_filter_penalty_total = 0.0
            breakout_distance_ratio = max(0.0, channel_lower - last) / max(abs(last), FLOAT_COMPARISON_TOLERANCE)
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
            breakout_distance_pips = max(0.0, channel_lower - last) / pip_size
            spread_hold_reason, spread_meta = self._spread_filter(
                symbol=ctx.symbol,
                spread_pips=spread_pips,
                stop_loss_pips=stop_loss_pips,
                breakout_distance_pips=breakout_distance_pips,
            )
            if spread_hold_reason is not None:
                return self._hold(
                    spread_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "down",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            if volume_hold_reason == "volume_below_entry_ratio":
                return self._hold(
                    volume_hold_reason,
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "down",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            volume_softened = False
            if self._volume_hold_blocks_entry(volume_hold_reason):
                volume_softened = True
                soft_filter_reasons.append(volume_hold_reason)
                soft_filter_penalty_total += self._soft_filter_penalty(volume_hold_reason)
            if not trend_session_allowed:
                soft_filter_reasons.append("trend_blocked_by_session")
                soft_filter_penalty_total += self._soft_filter_penalty("trend_blocked_by_session")
            if trend_session_open_delay_active:
                soft_filter_reasons.append("trend_waiting_session_open_stabilization")
                soft_filter_penalty_total += self._soft_filter_penalty(
                    "trend_waiting_session_open_stabilization"
                )
            if self.trend_volatility_explosion_filter_enabled and trend_volatility_explosion_passed is not True:
                return self._hold(
                    "waiting_for_volatility_explosion",
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "trend_following",
                        "trend": "down",
                        "breakout_distance_ratio": breakout_distance_ratio,
                        **spread_meta,
                    },
                )
            confidence = self._trend_confidence(ema_gap_ratio, breakout_distance_ratio)
            confidence = min(1.0, confidence + volume_confidence_boost)
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            confidence_threshold_cap = self._confidence_threshold_cap_for_regime("trend_following")
            signal = Signal(
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
                    "volume_softened": volume_softened,
                    "exit_hint": "close_if_price_reenters_channel_mid",
                    "confidence_threshold_cap": confidence_threshold_cap,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    **spread_meta,
                    **base_meta,
                    **self._trailing_payload(fast_now),
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if (
            trend_regime
            and trend_session_allowed
            and ((fast_now > slow_now and breakout_up) or (fast_now < slow_now and breakout_down))
        ):
            return self._hold(
                "trend_slope_not_confirmed",
                stop_loss_pips,
                take_profit_pips,
                {
                    **base_meta,
                    "regime": "trend_following",
                    "trend": "up" if fast_now > slow_now else "down",
                },
            )

        breakout_active = breakout_up or breakout_down
        breakout_extreme_ok = (
            mean_reversion_extreme_abs_zscore >= (self.zscore_threshold * self.mean_reversion_breakout_extreme_multiplier)
        )
        mean_reversion_breakout_allowed = (
            not breakout_active
            or (self.mean_reversion_allow_breakout and breakout_extreme_ok)
        )
        mean_reversion_buy_structure_ok = (
            mr_distance_to_lower_ratio <= self.mean_reversion_band_proximity_max_ratio
            and mr_buy_mid_deviation_ratio >= self.mean_reversion_min_mid_deviation_ratio
        )
        mean_reversion_sell_structure_ok = (
            mr_distance_to_upper_ratio <= self.mean_reversion_band_proximity_max_ratio
            and mr_sell_mid_deviation_ratio >= self.mean_reversion_min_mid_deviation_ratio
        )
        base_meta.update(
            {
                "mean_reversion_buy_structure_ok": mean_reversion_buy_structure_ok,
                "mean_reversion_sell_structure_ok": mean_reversion_sell_structure_ok,
            }
        )

        if mean_reversion_regime and mean_reversion_breakout_allowed:
            if (
                self.mean_reversion_entry_mode == "hook"
                and not mean_reversion_buy_signal_ready
                and not mean_reversion_sell_signal_ready
                and (mean_reversion_buy_extreme or mean_reversion_sell_extreme)
            ):
                return self._hold(
                    "mean_reversion_waiting_hook_reentry",
                    stop_loss_pips,
                    take_profit_pips,
                    {
                        **base_meta,
                        "regime": "mean_reversion",
                        "trend": "flat",
                        "breakout_extreme_ok": breakout_extreme_ok,
                        "mean_reversion_waiting_buy_hook": mean_reversion_buy_extreme,
                        "mean_reversion_waiting_sell_hook": mean_reversion_sell_extreme,
                    },
                )
            if mean_reversion_buy_signal_ready:
                soft_filter_reasons: list[str] = []
                soft_filter_penalty_total = 0.0
                if self.mean_reversion_require_band_proximity and not mean_reversion_buy_structure_ok:
                    return self._hold(
                        "mean_reversion_not_near_lower_band",
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_buy_structure_ok,
                        },
                    )
                spread_hold_reason, spread_meta = self._spread_filter(
                    symbol=ctx.symbol,
                    spread_pips=spread_pips,
                    stop_loss_pips=stop_loss_pips,
                )
                if spread_hold_reason is not None:
                    return self._hold(
                        spread_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_buy_structure_ok,
                            **spread_meta,
                        },
                    )
                if volume_hold_reason == "volume_below_entry_ratio":
                    return self._hold(
                        volume_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_buy_structure_ok,
                            **spread_meta,
                        },
                    )
                volume_softened = False
                if self._volume_hold_blocks_entry(volume_hold_reason):
                    volume_softened = True
                    soft_filter_reasons.append(volume_hold_reason)
                    soft_filter_penalty_total += self._soft_filter_penalty(volume_hold_reason)
                if not mean_reversion_session_allowed:
                    soft_filter_reasons.append("mean_reversion_blocked_by_session")
                    soft_filter_penalty_total += self._soft_filter_penalty(
                        "mean_reversion_blocked_by_session"
                    )
                confidence = self._mean_reversion_confidence(mean_reversion_extreme_abs_zscore)
                confidence = min(1.0, confidence + volume_confidence_boost)
                confidence = max(0.0, confidence - soft_filter_penalty_total)
                confidence_threshold_cap = self._confidence_threshold_cap_for_regime("mean_reversion")
                signal = Signal(
                    side=Side.BUY,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        "indicator": "index_hybrid",
                        "regime": "mean_reversion",
                        "trend": "flat",
                        "breakout_extreme_ok": breakout_extreme_ok,
                        "mean_reversion_structure_ok": mean_reversion_buy_structure_ok,
                        "volume_confidence_boost_applied": volume_confidence_boost,
                        "volume_softened": volume_softened,
                        "exit_hint": "close_if_price_reaches_channel_mid_or_opposite_band",
                        "confidence_threshold_cap": confidence_threshold_cap,
                        "soft_filter_penalty_total": soft_filter_penalty_total,
                        "soft_filter_reasons": soft_filter_reasons,
                        "soft_filter_count": len(soft_filter_reasons),
                        **spread_meta,
                        **base_meta,
                        **self._trailing_payload(fast_now),
                    },
                )
                return self._finalize_entry_signal(signal, prices=prices)
            if mean_reversion_sell_signal_ready:
                soft_filter_reasons: list[str] = []
                soft_filter_penalty_total = 0.0
                if self.mean_reversion_require_band_proximity and not mean_reversion_sell_structure_ok:
                    return self._hold(
                        "mean_reversion_not_near_upper_band",
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_sell_structure_ok,
                        },
                    )
                spread_hold_reason, spread_meta = self._spread_filter(
                    symbol=ctx.symbol,
                    spread_pips=spread_pips,
                    stop_loss_pips=stop_loss_pips,
                )
                if spread_hold_reason is not None:
                    return self._hold(
                        spread_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_sell_structure_ok,
                            **spread_meta,
                        },
                    )
                if volume_hold_reason == "volume_below_entry_ratio":
                    return self._hold(
                        volume_hold_reason,
                        stop_loss_pips,
                        take_profit_pips,
                        {
                            **base_meta,
                            "regime": "mean_reversion",
                            "trend": "flat",
                            "breakout_extreme_ok": breakout_extreme_ok,
                            "mean_reversion_structure_ok": mean_reversion_sell_structure_ok,
                            **spread_meta,
                        },
                    )
                volume_softened = False
                if self._volume_hold_blocks_entry(volume_hold_reason):
                    volume_softened = True
                    soft_filter_reasons.append(volume_hold_reason)
                    soft_filter_penalty_total += self._soft_filter_penalty(volume_hold_reason)
                if not mean_reversion_session_allowed:
                    soft_filter_reasons.append("mean_reversion_blocked_by_session")
                    soft_filter_penalty_total += self._soft_filter_penalty(
                        "mean_reversion_blocked_by_session"
                    )
                confidence = self._mean_reversion_confidence(mean_reversion_extreme_abs_zscore)
                confidence = min(1.0, confidence + volume_confidence_boost)
                confidence = max(0.0, confidence - soft_filter_penalty_total)
                confidence_threshold_cap = self._confidence_threshold_cap_for_regime("mean_reversion")
                signal = Signal(
                    side=Side.SELL,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        "indicator": "index_hybrid",
                        "regime": "mean_reversion",
                        "trend": "flat",
                        "breakout_extreme_ok": breakout_extreme_ok,
                        "mean_reversion_structure_ok": mean_reversion_sell_structure_ok,
                        "volume_confidence_boost_applied": volume_confidence_boost,
                        "volume_softened": volume_softened,
                        "exit_hint": "close_if_price_reaches_channel_mid_or_opposite_band",
                        "confidence_threshold_cap": confidence_threshold_cap,
                        "soft_filter_penalty_total": soft_filter_penalty_total,
                        "soft_filter_reasons": soft_filter_reasons,
                        "soft_filter_count": len(soft_filter_reasons),
                        **spread_meta,
                        **base_meta,
                        **self._trailing_payload(fast_now),
                    },
                )
                return self._finalize_entry_signal(signal, prices=prices)

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
                "no_signal_in_active_regime",
                stop_loss_pips,
                take_profit_pips,
                base_meta,
            )

        return self._hold("no_regime_setup", stop_loss_pips, take_profit_pips, base_meta)
