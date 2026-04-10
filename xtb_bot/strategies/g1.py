from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from dataclasses import dataclass
from collections.abc import Sequence
from datetime import datetime, timezone
import json
import math
from threading import Lock

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import adx_from_close, atr_wilder, efficiency_ratio, ema, ema_last_two, kama_last_two
from xtb_bot.symbols import is_commodity_symbol, is_index_symbol
from xtb_bot.strategies.base import Strategy, StrategyContext


@dataclass(frozen=True, slots=True)
class _G1Profile:
    name: str
    fast_ema_window: int
    slow_ema_window: int
    adx_window: int
    adx_threshold: float
    adx_hysteresis: float
    atr_window: int
    atr_multiplier: float
    risk_reward_ratio: float
    max_spread_pips: float
    max_spread_pips_by_symbol: dict[str, float]
    min_stop_loss_pips: float
    max_price_ema_gap_ratio: float
    max_price_ema_gap_atr_multiple: float
    min_slow_slope_ratio: float


@dataclass(frozen=True, slots=True)
class _G1VolumeSettings:
    enabled: bool
    window: int
    min_volume_ratio: float
    min_ratio_for_entry: float
    min_samples: int
    allow_missing: bool
    require_spike: bool
    confidence_boost: float


@dataclass(frozen=True, slots=True)
class _Bar:
    ts: float
    open: float
    high: float
    low: float
    close: float


class G1Strategy(Strategy):
    name = "g1"
    _SECONDARY_ADX_PENALTY = 0.14
    _SECONDARY_KAMA_PENALTY = 0.12
    _SECONDARY_VOLUME_PENALTY = 0.10
    _SECONDARY_VOLUME_UNAVAILABLE_PENALTY = 0.12
    _SECONDARY_SLOW_SLOPE_PENALTY = 0.10

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.profile_override = str(params.get("g1_profile_override", "auto")).strip().lower()
        if self.profile_override not in {"auto", "fx", "index", "commodity"}:
            self.profile_override = "auto"
        self.candle_timeframe_sec = max(0, int(float(params.get("g1_candle_timeframe_sec", 60.0))))
        self.candle_confirm_bars = max(1, int(float(params.get("g1_candle_confirm_bars", 1))))
        use_incomplete_candle_raw = params.get("g1_use_incomplete_candle_for_entry", True)
        if isinstance(use_incomplete_candle_raw, str):
            self.use_incomplete_candle_for_entry = (
                use_incomplete_candle_raw.strip().lower() not in {"0", "false", "no", "off"}
            )
        else:
            self.use_incomplete_candle_for_entry = bool(use_incomplete_candle_raw)
        self.entry_mode = str(params.get("g1_entry_mode", "cross_only")).strip().lower()
        if self.entry_mode not in {"cross_only", "cross_or_trend"}:
            self.entry_mode = "cross_only"
        self.entry_mode_by_symbol = self._parse_entry_mode_by_symbol(
            params.get("g1_entry_mode_by_symbol"),
        )
        self.min_cross_gap_ratio = max(0.0, float(params.get("g1_min_cross_gap_ratio", 0.0)))
        self.min_trend_gap_ratio_configured = max(0.0, float(params.get("g1_min_trend_gap_ratio", 0.0)))
        # Continuation entries must not be weaker than fresh cross entries.
        self.min_trend_gap_ratio = max(self.min_trend_gap_ratio_configured, self.min_cross_gap_ratio)
        self.min_trend_gap_ratio_auto_aligned = (
            self.min_trend_gap_ratio > (self.min_trend_gap_ratio_configured + FLOAT_ROUNDING_TOLERANCE)
        )
        self.continuation_fast_ema_retest_atr_tolerance = max(
            0.0,
            float(params.get("g1_continuation_fast_ema_retest_atr_tolerance", 0.20)),
        )
        self.continuation_adx_multiplier = max(0.1, float(params.get("g1_continuation_adx_multiplier", 0.90)))
        self.continuation_min_adx = max(0.0, float(params.get("g1_continuation_min_adx", 22.0)))
        self.continuation_min_entry_threshold_ratio = min(
            1.0,
            max(0.0, float(params.get("g1_continuation_min_entry_threshold_ratio", 0.90))),
        )
        self.cross_adx_multiplier = max(0.1, float(params.get("g1_cross_adx_multiplier", 0.85)))
        self.cross_min_adx = max(0.0, float(params.get("g1_cross_min_adx", 22.0)))
        self.cross_min_entry_threshold_ratio = min(
            1.0,
            max(0.0, float(params.get("g1_cross_min_entry_threshold_ratio", 0.82))),
        )
        self.adx_warmup_multiplier = max(2.0, float(params.get("g1_adx_warmup_multiplier", 2.0)))
        self.adx_warmup_extra_bars = max(0, int(float(params.get("g1_adx_warmup_extra_bars", 2))))
        self.adx_warmup_cap_bars = max(0, int(float(params.get("g1_adx_warmup_cap_bars", 0))))
        hysteresis_raw = params.get("g1_use_adx_hysteresis_state", True)
        if isinstance(hysteresis_raw, str):
            self.use_adx_hysteresis_state = hysteresis_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.use_adx_hysteresis_state = bool(hysteresis_raw)
        self.resample_mode = str(params.get("g1_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        self.index_low_vol_atr_pct_threshold = max(
            0.0, float(params.get("g1_index_low_vol_atr_pct_threshold", 0.1))
        )
        self.index_low_vol_multiplier = max(1.0, float(params.get("g1_index_low_vol_multiplier", 1.5)))
        self.min_relative_stop_pct = max(0.0, float(params.get("g1_min_relative_stop_pct", 0.0008)))
        low_tf_risk_profile_raw = params.get("g1_low_tf_risk_profile_enabled", True)
        if isinstance(low_tf_risk_profile_raw, str):
            self.low_tf_risk_profile_enabled = low_tf_risk_profile_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.low_tf_risk_profile_enabled = bool(low_tf_risk_profile_raw)
        self.low_tf_max_timeframe_sec = max(1.0, float(params.get("g1_low_tf_max_timeframe_sec", 90.0)))
        self.low_tf_atr_multiplier_cap = max(0.1, float(params.get("g1_low_tf_atr_multiplier_cap", 1.8)))
        self.low_tf_risk_reward_ratio_cap = max(1.0, float(params.get("g1_low_tf_risk_reward_ratio_cap", 2.0)))
        self.low_tf_min_relative_stop_pct_cap = max(
            0.0,
            float(params.get("g1_low_tf_min_relative_stop_pct_cap", 0.00035)),
        )
        ignore_sunday_raw = params.get("g1_ignore_sunday_candles", True)
        if isinstance(ignore_sunday_raw, str):
            self.ignore_sunday_candles = ignore_sunday_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.ignore_sunday_candles = bool(ignore_sunday_raw)
        index_tick_size_guard_raw = params.get("g1_index_require_context_tick_size", False)
        if isinstance(index_tick_size_guard_raw, str):
            self.index_require_context_tick_size = index_tick_size_guard_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.index_require_context_tick_size = bool(index_tick_size_guard_raw)
        volume_confirmation_raw = params.get("g1_volume_confirmation", True)
        if isinstance(volume_confirmation_raw, str):
            self.volume_confirmation = volume_confirmation_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_confirmation = bool(volume_confirmation_raw)
        self.volume_window = max(2, int(params.get("g1_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("g1_min_volume_ratio", 1.5)))
        self.volume_min_ratio_for_entry = max(0.0, float(params.get("g1_volume_min_ratio_for_entry", 1.0)))
        self.volume_min_samples = max(1, int(params.get("g1_volume_min_samples", 8)))
        volume_allow_missing_raw = params.get("g1_volume_allow_missing", True)
        if isinstance(volume_allow_missing_raw, str):
            self.volume_allow_missing = volume_allow_missing_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_allow_missing = bool(volume_allow_missing_raw)
        volume_require_spike_raw = params.get("g1_volume_require_spike", False)
        if isinstance(volume_require_spike_raw, str):
            self.volume_require_spike = volume_require_spike_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_require_spike = bool(volume_require_spike_raw)
        self.volume_confidence_boost = max(0.0, float(params.get("g1_volume_confidence_boost", 0.08)))
        self.confidence_velocity_norm_ratio = max(
            1e-6, float(params.get("g1_confidence_velocity_norm_ratio", 0.0005))
        )
        self.confidence_base = max(0.0, float(params.get("g1_confidence_base", 0.10)))
        self.confidence_adx_weight = max(0.0, float(params.get("g1_confidence_adx_weight", 0.35)))
        self.confidence_gap_weight = max(0.0, float(params.get("g1_confidence_gap_weight", 0.15)))
        self.confidence_velocity_weight = max(0.0, float(params.get("g1_confidence_velocity_weight", 0.45)))
        self.cross_confidence_cap = min(1.0, max(0.05, float(params.get("g1_cross_confidence_cap", 1.0))))
        self.continuation_confidence_cap = min(
            self.cross_confidence_cap,
            max(0.05, float(params.get("g1_continuation_confidence_cap", 0.72))),
        )
        self.continuation_velocity_weight_multiplier = min(
            1.0,
            max(0.0, float(params.get("g1_continuation_velocity_weight_multiplier", 0.65))),
        )
        self.continuation_price_alignment_bonus_multiplier = min(
            1.0,
            max(0.0, float(params.get("g1_continuation_price_alignment_bonus_multiplier", 0.0))),
        )
        self.kama_gate_enabled = self._as_bool(
            params,
            "g1_kama_gate_enabled",
            "g1_kama_gate_enabled",
            False,
        )
        self.kama_er_window = max(2, int(float(params.get("g1_kama_er_window", 10))))
        self.kama_fast_window = max(2, int(float(params.get("g1_kama_fast_window", 2))))
        self.kama_slow_window = max(
            self.kama_fast_window + 1,
            int(float(params.get("g1_kama_slow_window", 30))),
        )
        self.kama_min_efficiency_ratio = max(
            0.0,
            min(1.0, float(params.get("g1_kama_min_efficiency_ratio", 0.12))),
        )
        self.kama_min_slope_atr_ratio = max(
            0.0,
            float(params.get("g1_kama_min_slope_atr_ratio", 0.06)),
        )
        self._min_cross_gap_ratio_by_profile = {
            "fx": self._as_float(
                params,
                "g1_fx_min_cross_gap_ratio",
                "g1_min_cross_gap_ratio",
                self.min_cross_gap_ratio,
                0.0,
            ),
            "index": self._as_float(
                params,
                "g1_index_min_cross_gap_ratio",
                "g1_min_cross_gap_ratio",
                self.min_cross_gap_ratio,
                0.0,
            ),
            "commodity": self._as_float(
                params,
                "g1_commodity_min_cross_gap_ratio",
                "g1_min_cross_gap_ratio",
                self.min_cross_gap_ratio,
                0.0,
            ),
        }
        self._min_trend_gap_ratio_configured_by_profile: dict[str, float] = {}
        self._min_trend_gap_ratio_by_profile: dict[str, float] = {}
        self._min_trend_gap_ratio_auto_aligned_by_profile: dict[str, bool] = {}
        for profile_name, prefix in (("fx", "g1_fx"), ("index", "g1_index"), ("commodity", "g1_commodity")):
            configured = self._as_float(
                params,
                f"{prefix}_min_trend_gap_ratio",
                "g1_min_trend_gap_ratio",
                self.min_trend_gap_ratio_configured,
                0.0,
            )
            effective = max(configured, self._min_cross_gap_ratio_by_profile[profile_name])
            auto_aligned = effective > (configured + FLOAT_ROUNDING_TOLERANCE)
            self._min_trend_gap_ratio_configured_by_profile[profile_name] = configured
            self._min_trend_gap_ratio_by_profile[profile_name] = effective
            self._min_trend_gap_ratio_auto_aligned_by_profile[profile_name] = auto_aligned
        self._confidence_velocity_norm_ratio_by_profile = {
            "fx": self._as_float(
                params,
                "g1_fx_confidence_velocity_norm_ratio",
                "g1_confidence_velocity_norm_ratio",
                self.confidence_velocity_norm_ratio,
                1e-6,
            ),
            "index": self._as_float(
                params,
                "g1_index_confidence_velocity_norm_ratio",
                "g1_confidence_velocity_norm_ratio",
                self.confidence_velocity_norm_ratio,
                1e-6,
            ),
            "commodity": self._as_float(
                params,
                "g1_commodity_confidence_velocity_norm_ratio",
                "g1_confidence_velocity_norm_ratio",
                self.confidence_velocity_norm_ratio,
                1e-6,
            ),
        }
        self._continuation_fast_ema_retest_atr_tolerance_by_profile = {
            "fx": self._as_float(
                params,
                "g1_fx_continuation_fast_ema_retest_atr_tolerance",
                "g1_continuation_fast_ema_retest_atr_tolerance",
                self.continuation_fast_ema_retest_atr_tolerance,
                0.0,
            ),
            "index": self._as_float(
                params,
                "g1_index_continuation_fast_ema_retest_atr_tolerance",
                "g1_continuation_fast_ema_retest_atr_tolerance",
                self.continuation_fast_ema_retest_atr_tolerance,
                0.0,
            ),
            "commodity": self._as_float(
                params,
                "g1_commodity_continuation_fast_ema_retest_atr_tolerance",
                "g1_continuation_fast_ema_retest_atr_tolerance",
                self.continuation_fast_ema_retest_atr_tolerance,
                0.0,
            ),
        }
        self._volume_settings_by_profile = {
            "fx": self._build_volume_settings(params, "g1_fx"),
            "index": self._build_volume_settings(params, "g1_index"),
            "commodity": self._build_volume_settings(params, "g1_commodity"),
        }
        self.confidence_adx_norm_multiplier = max(
            1.0,
            float(params.get("g1_confidence_adx_norm_multiplier", 1.5)),
        )
        self.confidence_price_alignment_bonus = min(
            0.2,
            max(0.0, float(params.get("g1_confidence_price_alignment_bonus", 0.05))),
        )
        self._adx_regime_active_by_scope: dict[str, bool] = {}
        self._adx_thresholds_by_scope: dict[str, tuple[float, float]] = {}
        self._adx_scope_last_timestamp: dict[str, float] = {}
        self._state_lock = Lock()

        self.fx_profile = self._build_profile(
            params=params,
            prefix="g1_fx",
            default_name="fx",
            defaults={
                "fast_ema_window": 20,
                "slow_ema_window": 50,
                "adx_window": 8,
                "adx_threshold": 25.0,
                "adx_hysteresis": 1.0,
                "atr_window": 14,
                "atr_multiplier": 2.0,
                "risk_reward_ratio": 3.0,
                "max_spread_pips": 1.0,
                "min_stop_loss_pips": 15.0,
                "max_price_ema_gap_ratio": 0.005,
                "max_price_ema_gap_atr_multiple": 0.8,
                "min_slow_slope_ratio": 0.00003,
            },
        )
        self.index_profile = self._build_profile(
            params=params,
            prefix="g1_index",
            default_name="index",
            defaults={
                "fast_ema_window": 20,
                "slow_ema_window": 50,
                "adx_window": 8,
                "adx_threshold": 22.0,
                "adx_hysteresis": 1.0,
                "atr_window": 14,
                "atr_multiplier": 3.0,
                "risk_reward_ratio": 3.0,
                "max_spread_pips": 8.0,
                "min_stop_loss_pips": 50.0,
                "max_price_ema_gap_ratio": 0.008,
                "max_price_ema_gap_atr_multiple": 0.9,
                "min_slow_slope_ratio": 0.00002,
            },
        )
        self.commodity_profile = self._build_profile(
            params=params,
            prefix="g1_commodity",
            default_name="commodity",
            defaults={
                "fast_ema_window": 20,
                "slow_ema_window": 50,
                "adx_window": 8,
                "adx_threshold": 24.0,
                "adx_hysteresis": 1.0,
                "atr_window": 14,
                "atr_multiplier": 2.6,
                "risk_reward_ratio": 3.0,
                "max_spread_pips": 5.0,
                "min_stop_loss_pips": 30.0,
                "max_price_ema_gap_ratio": 0.012,
                "max_price_ema_gap_atr_multiple": 0.9,
                "min_slow_slope_ratio": 0.00003,
            },
        )
        # Fast MA trailing stop params
        self._trailing_enabled = self._param_bool("g1_fast_ma_trailing_enabled", True)
        self._trailing_activation_r_multiple = self._param_float(
            "g1_fast_ma_trailing_activation_r_multiple", 0.8, min_val=0.0,
        )
        self._trailing_buffer_atr = self._param_float(
            "g1_fast_ma_trailing_buffer_atr", 0.15, min_val=0.0,
        )
        self._trailing_buffer_pips = self._param_float(
            "g1_fast_ma_trailing_buffer_pips", 0.0, min_val=0.0,
        )
        self._trailing_min_step_pips = self._param_float(
            "g1_fast_ma_trailing_min_step_pips", 0.5, min_val=0.0,
        )
        self._trailing_update_cooldown_sec = self._param_float(
            "g1_fast_ma_trailing_update_cooldown_sec", 5.0, min_val=0.0,
        )

        self.min_history = max(
            self._profile_min_history(self.fx_profile),
            self._profile_min_history(self.index_profile),
            self._profile_min_history(self.commodity_profile),
            self.candle_confirm_bars
            + max(
                self.fx_profile.slow_ema_window,
                self.index_profile.slow_ema_window,
                self.commodity_profile.slow_ema_window,
            )
            + 2,
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
    def _as_int(params: dict[str, object], key: str, fallback_key: str, default: int, min_value: int) -> int:
        raw = params.get(key, params.get(fallback_key, default))
        return max(min_value, int(raw))

    @staticmethod
    def _as_float(
        params: dict[str, object],
        key: str,
        fallback_key: str,
        default: float,
        min_value: float,
    ) -> float:
        raw = params.get(key, params.get(fallback_key, default))
        return max(min_value, float(raw))

    @staticmethod
    def _as_bool(
        params: dict[str, object],
        key: str,
        fallback_key: str,
        default: bool,
    ) -> bool:
        raw = params.get(key, params.get(fallback_key, default))
        return Strategy._as_bool(raw, default)

    @staticmethod
    def _profile_key(profile: _G1Profile) -> str:
        if profile.name in {"index", "commodity"}:
            return profile.name
        return "fx"

    def _min_cross_gap_ratio_for_profile(self, profile: _G1Profile) -> float:
        return self._min_cross_gap_ratio_by_profile.get(self._profile_key(profile), self.min_cross_gap_ratio)

    def _min_trend_gap_ratio_configured_for_profile(self, profile: _G1Profile) -> float:
        return self._min_trend_gap_ratio_configured_by_profile.get(
            self._profile_key(profile),
            self.min_trend_gap_ratio_configured,
        )

    def _min_trend_gap_ratio_for_profile(self, profile: _G1Profile) -> float:
        return self._min_trend_gap_ratio_by_profile.get(self._profile_key(profile), self.min_trend_gap_ratio)

    def _min_trend_gap_ratio_auto_aligned_for_profile(self, profile: _G1Profile) -> bool:
        return self._min_trend_gap_ratio_auto_aligned_by_profile.get(
            self._profile_key(profile),
            self.min_trend_gap_ratio_auto_aligned,
        )

    def _confidence_velocity_norm_ratio_for_profile(self, profile: _G1Profile) -> float:
        return self._confidence_velocity_norm_ratio_by_profile.get(
            self._profile_key(profile),
            self.confidence_velocity_norm_ratio,
        )

    def _continuation_fast_ema_retest_atr_tolerance_for_profile(self, profile: _G1Profile) -> float:
        return self._continuation_fast_ema_retest_atr_tolerance_by_profile.get(
            self._profile_key(profile),
            self.continuation_fast_ema_retest_atr_tolerance,
        )

    def _build_volume_settings(self, params: dict[str, object], prefix: str) -> _G1VolumeSettings:
        return _G1VolumeSettings(
            enabled=self._as_bool(
                params,
                f"{prefix}_volume_confirmation",
                "g1_volume_confirmation",
                self.volume_confirmation,
            ),
            window=self._as_int(
                params,
                f"{prefix}_volume_window",
                "g1_volume_window",
                self.volume_window,
                2,
            ),
            min_volume_ratio=self._as_float(
                params,
                f"{prefix}_min_volume_ratio",
                "g1_min_volume_ratio",
                self.min_volume_ratio,
                0.1,
            ),
            min_ratio_for_entry=self._as_float(
                params,
                f"{prefix}_volume_min_ratio_for_entry",
                "g1_volume_min_ratio_for_entry",
                self.volume_min_ratio_for_entry,
                0.0,
            ),
            min_samples=self._as_int(
                params,
                f"{prefix}_volume_min_samples",
                "g1_volume_min_samples",
                self.volume_min_samples,
                1,
            ),
            allow_missing=self._as_bool(
                params,
                f"{prefix}_volume_allow_missing",
                "g1_volume_allow_missing",
                self.volume_allow_missing,
            ),
            require_spike=self._as_bool(
                params,
                f"{prefix}_volume_require_spike",
                "g1_volume_require_spike",
                self.volume_require_spike,
            ),
            confidence_boost=self._as_float(
                params,
                f"{prefix}_volume_confidence_boost",
                "g1_volume_confidence_boost",
                self.volume_confidence_boost,
                0.0,
            ),
        )

    def _volume_settings_for_profile(self, profile: _G1Profile) -> _G1VolumeSettings:
        return self._volume_settings_by_profile.get(self._profile_key(profile), self._volume_settings_by_profile["fx"])

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

    @staticmethod
    def _parse_entry_mode_by_symbol(raw: object) -> dict[str, str]:
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
        parsed: dict[str, str] = {}
        for key, value in payload.items():
            symbol = str(key).strip().upper()
            if not symbol:
                continue
            entry_mode = str(value).strip().lower()
            if entry_mode not in {"cross_only", "cross_or_trend"}:
                continue
            parsed[symbol] = entry_mode
        return parsed

    def _entry_mode_for_symbol(self, symbol: str) -> tuple[str, bool]:
        symbol_key = str(symbol).strip().upper()
        override = self.entry_mode_by_symbol.get(symbol_key)
        if override in {"cross_only", "cross_or_trend"}:
            return override, True
        return self.entry_mode, False

    @staticmethod
    def _spread_limit_for_symbol(profile: _G1Profile, symbol: str) -> tuple[float, str]:
        upper_symbol = str(symbol).strip().upper()
        override = profile.max_spread_pips_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return profile.max_spread_pips, "global"

    def _build_profile(
        self,
        params: dict[str, object],
        prefix: str,
        default_name: str,
        defaults: dict[str, float | int],
    ) -> _G1Profile:
        fast = self._as_int(
            params,
            f"{prefix}_fast_ema_window",
            "g1_fast_ema_window",
            int(defaults["fast_ema_window"]),
            2,
        )
        slow = self._as_int(
            params,
            f"{prefix}_slow_ema_window",
            "g1_slow_ema_window",
            int(defaults["slow_ema_window"]),
            fast + 1,
        )
        adx_window = self._as_int(
            params,
            f"{prefix}_adx_window",
            "g1_adx_window",
            int(defaults["adx_window"]),
            2,
        )
        atr_window = self._as_int(
            params,
            f"{prefix}_atr_window",
            "g1_atr_window",
            int(defaults["atr_window"]),
            2,
        )
        min_stop_default = float(defaults["min_stop_loss_pips"])
        min_stop = self._as_float(
            params,
            f"{prefix}_min_stop_loss_pips",
            "g1_min_stop_loss_pips",
            min_stop_default,
            0.1,
        )
        if f"{prefix}_min_stop_loss_pips" not in params and "g1_min_stop_loss_pips" not in params:
            min_stop = max(min_stop, float(params.get("stop_loss_pips", min_stop)))

        return _G1Profile(
            name=default_name,
            fast_ema_window=fast,
            slow_ema_window=slow,
            adx_window=adx_window,
            adx_threshold=self._as_float(
                params,
                f"{prefix}_adx_threshold",
                "g1_adx_threshold",
                float(defaults["adx_threshold"]),
                0.0,
            ),
            adx_hysteresis=self._as_float(
                params,
                f"{prefix}_adx_hysteresis",
                "g1_adx_hysteresis",
                float(defaults["adx_hysteresis"]),
                0.0,
            ),
            atr_window=atr_window,
            atr_multiplier=self._as_float(
                params,
                f"{prefix}_atr_multiplier",
                "g1_atr_multiplier",
                float(defaults["atr_multiplier"]),
                0.1,
            ),
            risk_reward_ratio=self._as_float(
                params,
                f"{prefix}_risk_reward_ratio",
                "g1_risk_reward_ratio",
                float(defaults["risk_reward_ratio"]),
                1.0,
            ),
            max_spread_pips=self._as_float(
                params,
                f"{prefix}_max_spread_pips",
                "g1_max_spread_pips",
                float(defaults["max_spread_pips"]),
                0.0,
            ),
            max_spread_pips_by_symbol=self._parse_spread_limits(
                params.get(
                    f"{prefix}_max_spread_pips_by_symbol",
                    params.get("g1_max_spread_pips_by_symbol"),
                )
            ),
            min_stop_loss_pips=min_stop,
            max_price_ema_gap_ratio=self._as_float(
                params,
                f"{prefix}_max_price_ema_gap_ratio",
                "g1_max_price_ema_gap_ratio",
                float(defaults["max_price_ema_gap_ratio"]),
                0.0,
            ),
            max_price_ema_gap_atr_multiple=self._as_float(
                params,
                f"{prefix}_max_price_ema_gap_atr_multiple",
                "g1_max_price_ema_gap_atr_multiple",
                float(defaults["max_price_ema_gap_atr_multiple"]),
                0.0,
            ),
            min_slow_slope_ratio=self._as_float(
                params,
                f"{prefix}_min_slow_slope_ratio",
                "g1_min_slow_slope_ratio",
                float(defaults["min_slow_slope_ratio"]),
                0.0,
            ),
        )

    def _profile_min_history(self, profile: _G1Profile) -> int:
        adx_required = int(profile.adx_window * self.adx_warmup_multiplier) + self.adx_warmup_extra_bars
        adx_required = max(profile.adx_window + 2, adx_required)
        if self.adx_warmup_cap_bars > 0:
            adx_required = min(adx_required, self.adx_warmup_cap_bars)
        return max(
            profile.slow_ema_window + 2,
            profile.atr_window + 2,
            adx_required,
        )

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _ema_last_two(values: Sequence[float], window: int) -> tuple[float, float]:
        return ema_last_two(values, window)

    def _resample_closed_candle_bars(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[_Bar], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [
                _Bar(
                    ts=float(idx),
                    open=float(value),
                    high=float(value),
                    low=float(value),
                    close=float(value),
                )
                for idx, value in enumerate(prices)
            ], False
        if not timestamps or len(timestamps) != len(prices):
            return [
                _Bar(
                    ts=float(idx),
                    open=float(value),
                    high=float(value),
                    low=float(value),
                    close=float(value),
                )
                for idx, value in enumerate(prices)
            ], False
        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        input_is_candle_like = self.resample_mode == "auto" and self._is_candle_like_spacing(normalized_timestamps)

        bars: list[_Bar] = []
        last_bucket: int | None = None
        timeframe = max(1, self.candle_timeframe_sec)
        for raw_price, ts in zip(prices, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            price = float(raw_price)
            bar_ts = float((bucket + 1) * timeframe)
            if last_bucket is None or bucket != last_bucket:
                bars.append(
                    _Bar(
                        ts=bar_ts,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                    )
                )
                last_bucket = bucket
            else:
                previous = bars[-1]
                bars[-1] = _Bar(
                    ts=bar_ts,
                    open=previous.open,
                    high=max(previous.high, price),
                    low=min(previous.low, price),
                    close=price,
                )

        if input_is_candle_like:
            # Candle-like input from live providers can still include an
            # in-progress bar. Keep low-latency mode opt-in only.
            if self.use_incomplete_candle_for_entry:
                return bars, False
            if len(bars) < 2:
                return [], True
            return bars[:-1], True

        if self.use_incomplete_candle_for_entry:
            # Optional low-latency mode: include the in-progress candle for faster entries.
            # Kept opt-in because it can increase signal flicker on noisy symbols.
            return bars, True

        # Default behavior: exclude current in-progress candle to avoid tick-level flickering.
        if len(bars) < 2:
            return [], True
        return bars[:-1], True

    def _resample_closed_candle_closes(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], bool]:
        bars, using_closed = self._resample_closed_candle_bars(prices, timestamps)
        return [bar.close for bar in bars], using_closed

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
        timeframe = max(1.0, float(self.candle_timeframe_sec))
        if min(deltas) < timeframe * 0.5:
            return False
        median_delta = deltas[len(deltas) // 2]
        return median_delta >= timeframe * 0.8

    def _infer_timeframe_sec(self, raw_timestamps: Sequence[float]) -> float | None:
        if len(raw_timestamps) < 2:
            return None
        normalized = [self._timestamp_to_seconds(value) for value in raw_timestamps]
        deltas = sorted(
            normalized[idx] - normalized[idx - 1]
            for idx in range(1, len(normalized))
            if (normalized[idx] - normalized[idx - 1]) > 0.0
        )
        if not deltas:
            return None
        return float(deltas[len(deltas) // 2])

    def _resolve_timeframe_sec(self, raw_timestamps: Sequence[float], *, using_closed_candles: bool) -> float | None:
        if len(raw_timestamps) < 2:
            return None
        if using_closed_candles and self.candle_timeframe_sec > 1:
            return float(self.candle_timeframe_sec)
        return self._infer_timeframe_sec(raw_timestamps)

    def _is_low_timeframe_for_risk(self, timeframe_sec: float | None) -> bool:
        if not self.low_tf_risk_profile_enabled:
            return False
        if timeframe_sec is None:
            return False
        return timeframe_sec <= self.low_tf_max_timeframe_sec

    def _confirmed_by_closed_candles(
        self,
        prices: Sequence[float],
        profile: _G1Profile,
        side: Side,
        *,
        using_closed_candles: bool,
    ) -> bool:
        min_cross_gap_ratio = self._min_cross_gap_ratio_for_profile(profile)
        min_trend_gap_ratio = self._min_trend_gap_ratio_for_profile(profile)
        # Confirmation must be based on fully closed candles only.
        # Even when in-progress candle is included for low-latency signal
        # generation, we exclude it here to avoid intrabar phantom entries.
        confirm_prices = list(prices)
        if using_closed_candles and self.use_incomplete_candle_for_entry:
            confirm_prices = confirm_prices[:-1]
        bars = max(1, self.candle_confirm_bars)
        if len(confirm_prices) < profile.slow_ema_window + bars:
            return False
        for offset in range(bars):
            end = len(confirm_prices) - offset
            subset = confirm_prices[:end]
            fast_prev, fast_now = self._ema_last_two(subset, profile.fast_ema_window)
            slow_prev, slow_now = self._ema_last_two(subset, profile.slow_ema_window)
            close = float(subset[-1])
            ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)
            bullish_cross = (
                fast_prev <= slow_prev
                and fast_now > slow_now
                and ema_gap_ratio >= min_cross_gap_ratio
            )
            bearish_cross = (
                fast_prev >= slow_prev
                and fast_now < slow_now
                and ema_gap_ratio >= min_cross_gap_ratio
            )
            bullish_trend = (
                fast_now > slow_now
                and slow_now >= slow_prev
                and ema_gap_ratio >= min_trend_gap_ratio
            )
            bearish_trend = (
                fast_now < slow_now
                and slow_now <= slow_prev
                and ema_gap_ratio >= min_trend_gap_ratio
            )
            continuation_trend_intact = close > slow_now if side == Side.BUY else close < slow_now
            if side == Side.BUY and not (
                (
                    bullish_cross
                    and close > max(fast_now, slow_now)
                )
                or (
                    bullish_trend
                    and continuation_trend_intact
                )
            ):
                return False
            if side == Side.SELL and not (
                (
                    bearish_cross
                    and close < min(fast_now, slow_now)
                )
                or (
                    bearish_trend
                    and continuation_trend_intact
                )
            ):
                return False
        return True

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        return is_index_symbol(symbol)

    @staticmethod
    def _is_commodity_symbol(symbol: str) -> bool:
        return is_commodity_symbol(symbol)

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

    def _evaluate_volume_confirmation(
        self,
        ctx: StrategyContext,
        *,
        profile: _G1Profile | None = None,
        using_closed_candles: bool,
    ) -> tuple[dict[str, object], str | None, float]:
        effective_profile = profile if profile is not None else self._profile_for_symbol(ctx.symbol)
        volume_settings = self._volume_settings_for_profile(effective_profile)
        index_missing_volume_relaxed = self._is_index_symbol(ctx.symbol)
        effective_allow_missing = volume_settings.allow_missing or index_missing_volume_relaxed
        metadata: dict[str, object] = {
            "volume_confirmation_enabled": volume_settings.enabled,
            "volume_window": volume_settings.window,
            "min_volume_ratio": volume_settings.min_volume_ratio,
            "volume_min_ratio_for_entry": volume_settings.min_ratio_for_entry,
            "volume_min_samples": volume_settings.min_samples,
            "volume_allow_missing": volume_settings.allow_missing,
            "volume_allow_missing_effective": effective_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not volume_settings.allow_missing,
            "volume_require_spike": volume_settings.require_spike,
            "volume_confidence_boost_config": volume_settings.confidence_boost,
            "volume_profile": self._profile_key(effective_profile),
            "volume_effective_required_ratio": None,
            "volume_current_source": "unavailable",
            "volume_compare_closed_only": False,
            "volume_data_available": False,
            "volume_spike": False,
            "volume_confirmed": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
        }
        if not volume_settings.enabled:
            return metadata, None, 0.0

        samples = self._extract_finite_positive_volumes(ctx.volumes[-(volume_settings.window + 6) :])
        current_volume = None
        if ctx.current_volume is not None:
            try:
                current_volume = float(ctx.current_volume)
            except (TypeError, ValueError):
                current_volume = None
            if current_volume is not None and (not math.isfinite(current_volume) or current_volume <= 0.0):
                current_volume = None

        compare_closed_only = using_closed_candles and self.use_incomplete_candle_for_entry
        metadata["volume_compare_closed_only"] = compare_closed_only
        if compare_closed_only:
            # When entry is evaluated on an in-progress candle, compare only completed candle volumes.
            if current_volume is not None and samples:
                samples = samples[:-1]
                metadata["volume_current_source"] = "last_closed_ctx_volume"
            elif samples:
                metadata["volume_current_source"] = "last_ctx_volume"
        else:
            if current_volume is not None:
                if samples and self._volume_values_close(samples[-1], current_volume):
                    metadata["volume_current_source"] = "ctx_volumes_last"
                else:
                    samples.append(current_volume)
                    metadata["volume_current_source"] = "context_current_volume"
            elif samples:
                metadata["volume_current_source"] = "ctx_volumes_last"

        if len(samples) < 2:
            if effective_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = samples[-1]
        history = samples[:-1]
        if len(history) > volume_settings.window:
            history = history[-volume_settings.window :]
        if len(history) < volume_settings.min_samples:
            if effective_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        avg_volume = sum(history) / max(len(history), 1)
        if avg_volume <= 0.0:
            if effective_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        volume_ratio = current / avg_volume
        spike = volume_ratio >= volume_settings.min_volume_ratio
        required_ratio = (
            volume_settings.min_volume_ratio
            if volume_settings.require_spike
            else volume_settings.min_ratio_for_entry
        )
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
        return metadata, None, volume_settings.confidence_boost if spike else 0.0

    def _profile_for_symbol(self, symbol: str) -> _G1Profile:
        if self.profile_override == "fx":
            return self.fx_profile
        if self.profile_override == "index":
            return self.index_profile
        if self.profile_override == "commodity":
            return self.commodity_profile
        if self._is_index_symbol(symbol):
            return self.index_profile
        if self._is_commodity_symbol(symbol):
            return self.commodity_profile
        return self.fx_profile

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _atr_from_bars(bars: Sequence[_Bar], window: int) -> float | None:
        if len(bars) < window + 1:
            return None
        ohlc_values = [(bar.open, bar.high, bar.low, bar.close) for bar in bars]
        return atr_wilder(ohlc_values, window)

    @staticmethod
    def _adx(prices: Sequence[float], window: int) -> float | None:
        return adx_from_close(prices, window)

    @staticmethod
    def _adx_from_bars(bars: Sequence[_Bar], window: int) -> float | None:
        if len(bars) < (window * 2) + 1 or window <= 0:
            return None

        tr_values: list[float] = []
        plus_dm_values: list[float] = []
        minus_dm_values: list[float] = []

        for idx in range(1, len(bars)):
            current = bars[idx]
            previous = bars[idx - 1]

            up_move = current.high - previous.high
            down_move = previous.low - current.low

            plus_dm = up_move if (up_move > down_move and up_move > 0.0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0.0) else 0.0

            tr = max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )

            tr_values.append(max(tr, 0.0))
            plus_dm_values.append(plus_dm)
            minus_dm_values.append(minus_dm)

        if len(tr_values) < window * 2:
            return None

        tr_smooth = sum(tr_values[:window])
        plus_dm_smooth = sum(plus_dm_values[:window])
        minus_dm_smooth = sum(minus_dm_values[:window])

        dx_values: list[float] = []
        for idx in range(window, len(tr_values)):
            plus_di = 100.0 * (plus_dm_smooth / max(tr_smooth, FLOAT_COMPARISON_TOLERANCE))
            minus_di = 100.0 * (minus_dm_smooth / max(tr_smooth, FLOAT_COMPARISON_TOLERANCE))
            di_sum = plus_di + minus_di
            dx = 100.0 * abs(plus_di - minus_di) / max(di_sum, FLOAT_COMPARISON_TOLERANCE)
            dx_values.append(dx)

            tr_smooth = tr_smooth - (tr_smooth / window) + tr_values[idx]
            plus_dm_smooth = plus_dm_smooth - (plus_dm_smooth / window) + plus_dm_values[idx]
            minus_dm_smooth = minus_dm_smooth - (minus_dm_smooth / window) + minus_dm_values[idx]

        if not dx_values:
            return None
        if len(dx_values) < window:
            return sum(dx_values) / float(len(dx_values))

        adx = sum(dx_values[:window]) / float(window)
        for dx in dx_values[window:]:
            adx = ((adx * (window - 1)) + dx) / float(window)
        return adx if math.isfinite(adx) and adx >= 0.0 else None

    @staticmethod
    def _adx_series_from_close(values: Sequence[float], window: int) -> list[float]:
        if len(values) < window + 2 or window <= 0:
            return []

        diffs = [float(values[idx]) - float(values[idx - 1]) for idx in range(1, len(values))]
        if len(diffs) < window:
            return []

        up = [delta if delta > 0.0 else 0.0 for delta in diffs]
        down = [-delta if delta < 0.0 else 0.0 for delta in diffs]
        tr = [abs(delta) for delta in diffs]

        atr = sum(tr[:window]) / float(window)
        plus_smoothed = sum(up[:window]) / float(window)
        minus_smoothed = sum(down[:window]) / float(window)

        dx_values: list[float] = []
        for idx in range(window, len(tr)):
            atr = ((atr * (window - 1)) + tr[idx]) / float(window)
            plus_smoothed = ((plus_smoothed * (window - 1)) + up[idx]) / float(window)
            minus_smoothed = ((minus_smoothed * (window - 1)) + down[idx]) / float(window)

            if atr <= 0.0:
                dx_values.append(0.0)
                continue

            plus_di = 100.0 * (plus_smoothed / atr)
            minus_di = 100.0 * (minus_smoothed / atr)
            denominator = plus_di + minus_di
            if denominator <= 0.0:
                dx_values.append(0.0)
            else:
                dx_values.append(100.0 * abs(plus_di - minus_di) / denominator)

        if not dx_values:
            return []
        if len(dx_values) < window:
            return [sum(dx_values[: idx + 1]) / float(idx + 1) for idx in range(len(dx_values))]

        adx_values: list[float] = []
        adx = sum(dx_values[:window]) / float(window)
        adx_values.append(adx)
        for value in dx_values[window:]:
            adx = ((adx * (window - 1)) + value) / float(window)
            adx_values.append(adx)
        return [value for value in adx_values if math.isfinite(value) and value >= 0.0]

    @staticmethod
    def _adx_series_from_bars(bars: Sequence[_Bar], window: int) -> list[float]:
        if len(bars) < (window * 2) + 1 or window <= 0:
            return []

        tr_values: list[float] = []
        plus_dm_values: list[float] = []
        minus_dm_values: list[float] = []

        for idx in range(1, len(bars)):
            current = bars[idx]
            previous = bars[idx - 1]

            up_move = current.high - previous.high
            down_move = previous.low - current.low

            plus_dm = up_move if (up_move > down_move and up_move > 0.0) else 0.0
            minus_dm = down_move if (down_move > up_move and down_move > 0.0) else 0.0
            tr = max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )

            tr_values.append(max(tr, 0.0))
            plus_dm_values.append(plus_dm)
            minus_dm_values.append(minus_dm)

        if len(tr_values) < window * 2:
            return []

        tr_smooth = sum(tr_values[:window])
        plus_dm_smooth = sum(plus_dm_values[:window])
        minus_dm_smooth = sum(minus_dm_values[:window])

        dx_values: list[float] = []
        for idx in range(window, len(tr_values)):
            plus_di = 100.0 * (plus_dm_smooth / max(tr_smooth, FLOAT_COMPARISON_TOLERANCE))
            minus_di = 100.0 * (minus_dm_smooth / max(tr_smooth, FLOAT_COMPARISON_TOLERANCE))
            di_sum = plus_di + minus_di
            dx = 100.0 * abs(plus_di - minus_di) / max(di_sum, FLOAT_COMPARISON_TOLERANCE)
            dx_values.append(dx)

            tr_smooth = tr_smooth - (tr_smooth / window) + tr_values[idx]
            plus_dm_smooth = plus_dm_smooth - (plus_dm_smooth / window) + plus_dm_values[idx]
            minus_dm_smooth = minus_dm_smooth - (minus_dm_smooth / window) + minus_dm_values[idx]

        if not dx_values:
            return []
        if len(dx_values) < window:
            return [sum(dx_values[: idx + 1]) / float(idx + 1) for idx in range(len(dx_values))]

        adx_values: list[float] = []
        adx = sum(dx_values[:window]) / float(window)
        adx_values.append(adx)
        for value in dx_values[window:]:
            adx = ((adx * (window - 1)) + value) / float(window)
            adx_values.append(adx)
        return [value for value in adx_values if math.isfinite(value) and value >= 0.0]

    def _hold(
        self,
        reason: str,
        profile: _G1Profile,
        extra: dict[str, object] | None = None,
    ) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "g1", "profile": profile.name}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=profile.min_stop_loss_pips,
            take_profit_pips=profile.min_stop_loss_pips * profile.risk_reward_ratio,
            metadata=payload,
        )

    def _kama_gate_payload(
        self,
        prices: Sequence[float],
        atr: float,
        side: Side,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "g1_kama_gate_enabled": self.kama_gate_enabled,
            "g1_kama_er_window": self.kama_er_window,
            "g1_kama_fast_window": self.kama_fast_window,
            "g1_kama_slow_window": self.kama_slow_window,
            "g1_kama_min_efficiency_ratio": self.kama_min_efficiency_ratio,
            "g1_kama_min_slope_atr_ratio": self.kama_min_slope_atr_ratio,
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
        raw_slope_atr_ratio = (kama_now - kama_prev) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        directional_slope_atr_ratio = self._directional_ratio(raw_slope_atr_ratio, side)
        payload["kama_slope_atr_ratio"] = raw_slope_atr_ratio
        payload["kama_directional_slope_atr_ratio"] = directional_slope_atr_ratio
        passes = (
            er >= self.kama_min_efficiency_ratio
            and directional_slope_atr_ratio >= self.kama_min_slope_atr_ratio
        )
        payload["kama_gate_status"] = "ok" if passes else "chop"
        return passes, payload

    def _adx_gate_passes(
        self,
        scope_key: str,
        adx: float,
        entry_threshold: float,
        exit_threshold: float,
        *,
        seed_active: bool | None = None,
    ) -> tuple[bool, bool, bool]:
        with self._state_lock:
            previous_thresholds = self._adx_thresholds_by_scope.get(scope_key)
            thresholds_changed = (
                previous_thresholds is None
                or abs(previous_thresholds[0] - entry_threshold) > FLOAT_COMPARISON_TOLERANCE
                or abs(previous_thresholds[1] - exit_threshold) > FLOAT_COMPARISON_TOLERANCE
            )
            self._adx_thresholds_by_scope[scope_key] = (entry_threshold, exit_threshold)
            state_cached = scope_key in self._adx_regime_active_by_scope
            active_before = self._adx_regime_active_by_scope.get(scope_key, False)
            if seed_active is not None and ((not state_cached) or thresholds_changed):
                active_before = bool(seed_active)
            if not self.use_adx_hysteresis_state:
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

    def _adx_scope_key(
        self,
        symbol: str,
        side: Side,
        profile: _G1Profile,
        timeframe_sec: float | None,
        *,
        using_closed_candles: bool,
        entry_mode: str | None = None,
    ) -> str:
        normalized_symbol = str(symbol).strip().upper()
        direction = "buy" if side == Side.BUY else "sell"
        timeframe_key = self._finite_timeframe_key(timeframe_sec)
        effective_entry_mode = str(entry_mode or self.entry_mode).strip().lower()
        return (
            f"{normalized_symbol}:{direction}"
            f"|profile={profile.name}"
            f"|tf={timeframe_key}"
            f"|candletf={self.candle_timeframe_sec}"
            f"|entry_mode={effective_entry_mode}"
            f"|resample={self.resample_mode}"
            f"|closed={int(using_closed_candles)}"
            f"|incomplete={int(self.use_incomplete_candle_for_entry)}"
        )

    def _has_instance_override(self, method_name: str) -> bool:
        return method_name in self.__dict__

    def _maybe_reset_adx_scope_for_timestamp(self, scope_key: str, latest_timestamp_sec: float | None) -> None:
        if latest_timestamp_sec is None or not math.isfinite(latest_timestamp_sec):
            return
        with self._state_lock:
            previous_ts = self._adx_scope_last_timestamp.get(scope_key)
            if previous_ts is not None and latest_timestamp_sec + FLOAT_COMPARISON_TOLERANCE < previous_ts:
                self._adx_regime_active_by_scope.pop(scope_key, None)
                self._adx_thresholds_by_scope.pop(scope_key, None)
            self._adx_scope_last_timestamp[scope_key] = latest_timestamp_sec

    def _refresh_adx_regime_state(
        self,
        symbol: str,
        profile: _G1Profile,
        timeframe_sec: float | None,
        *,
        using_closed_candles: bool,
        entry_mode: str | None,
        latest_timestamp_sec: float | None,
        adx: float,
        entry_threshold: float,
        exit_threshold: float,
        seed_active: bool | None = None,
    ) -> None:
        symbol_key = str(symbol).strip().upper()
        if not symbol_key:
            return
        for side in (Side.BUY, Side.SELL):
            scope_key = self._adx_scope_key(
                symbol_key,
                side,
                profile,
                timeframe_sec,
                using_closed_candles=using_closed_candles,
                entry_mode=entry_mode,
            )
            self._maybe_reset_adx_scope_for_timestamp(scope_key, latest_timestamp_sec)
            with self._state_lock:
                previous_thresholds = self._adx_thresholds_by_scope.get(scope_key)
                scope_entry_threshold, scope_exit_threshold = previous_thresholds or (
                    entry_threshold,
                    exit_threshold,
                )
                thresholds_changed = (
                    previous_thresholds is None
                    or abs(scope_entry_threshold - entry_threshold) > FLOAT_COMPARISON_TOLERANCE
                    or abs(scope_exit_threshold - exit_threshold) > FLOAT_COMPARISON_TOLERANCE
                )
                if previous_thresholds is None:
                    self._adx_thresholds_by_scope[scope_key] = (entry_threshold, exit_threshold)
                state_cached = scope_key in self._adx_regime_active_by_scope
                active_before = self._adx_regime_active_by_scope.get(scope_key, False)
                if seed_active is not None and ((not state_cached) or thresholds_changed):
                    active_before = bool(seed_active)
                if not self.use_adx_hysteresis_state:
                    active_after = adx >= scope_entry_threshold
                elif active_before:
                    active_after = adx >= scope_exit_threshold
                else:
                    active_after = adx >= scope_entry_threshold
                self._adx_regime_active_by_scope[scope_key] = active_after

    @staticmethod
    def _infer_adx_regime_seed_active(
        adx_values: Sequence[float],
        entry_threshold: float,
        exit_threshold: float,
    ) -> bool | None:
        active = False
        seen = False
        for raw_value in adx_values:
            value = float(raw_value)
            if not math.isfinite(value):
                continue
            seen = True
            if active:
                active = value >= exit_threshold
            else:
                active = value >= entry_threshold
        if not seen:
            return None
        return active

    @staticmethod
    def _slope_ratio(current: float, previous: float) -> float:
        return (current - previous) / max(abs(previous), FLOAT_COMPARISON_TOLERANCE)

    @staticmethod
    def _directional_ratio(value: float, side: Side) -> float:
        if side == Side.BUY:
            return max(0.0, value)
        if side == Side.SELL:
            return max(0.0, -value)
        return 0.0

    @staticmethod
    def _gap_acceleration_ratio(
        ema_diff_before_prev: float,
        ema_diff_prev: float,
        ema_diff_now: float,
        scale: float,
    ) -> tuple[float, float, float]:
        gap_velocity_prev = ema_diff_prev - ema_diff_before_prev
        gap_velocity_now = ema_diff_now - ema_diff_prev
        gap_acceleration = gap_velocity_now - gap_velocity_prev
        gap_acceleration_ratio = gap_acceleration / max(abs(scale), FLOAT_COMPARISON_TOLERANCE)
        return gap_velocity_prev, gap_velocity_now, gap_acceleration_ratio

    def _confidence(
        self,
        adx: float,
        effective_adx_entry_threshold: float,
        ema_gap_ratio: float,
        ema_gap_norm: float,
        directional_velocity_ratio: float,
        price_aligned_with_fast_ma: bool,
        is_continuation_entry: bool = False,
        velocity_norm_ratio: float | None = None,
    ) -> float:
        adx_score = min(
            1.0,
            adx / max(effective_adx_entry_threshold * self.confidence_adx_norm_multiplier, FLOAT_COMPARISON_TOLERANCE),
        )
        gap_score = min(1.0, ema_gap_ratio / max(ema_gap_norm, FLOAT_COMPARISON_TOLERANCE))
        effective_velocity_norm_ratio = (
            self.confidence_velocity_norm_ratio if velocity_norm_ratio is None else velocity_norm_ratio
        )
        velocity_score = min(1.0, directional_velocity_ratio / max(effective_velocity_norm_ratio, FLOAT_COMPARISON_TOLERANCE))
        effective_velocity_weight = self.confidence_velocity_weight
        effective_price_alignment_bonus = self.confidence_price_alignment_bonus
        confidence_cap = self.cross_confidence_cap
        if is_continuation_entry:
            effective_velocity_weight *= self.continuation_velocity_weight_multiplier
            effective_price_alignment_bonus *= self.continuation_price_alignment_bonus_multiplier
            confidence_cap = self.continuation_confidence_cap
        price_alignment_bonus = effective_price_alignment_bonus if price_aligned_with_fast_ma else 0.0
        value = (
            self.confidence_base
            + adx_score * self.confidence_adx_weight
            + gap_score * self.confidence_gap_weight
            + velocity_score * effective_velocity_weight
            + price_alignment_bonus
        )
        return max(0.05, min(confidence_cap, value))

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raw_prices, raw_timestamps, invalid_prices = self._extract_finite_prices_and_timestamps(
            ctx.prices,
            ctx.timestamps,
            timestamp_normalizer=self._timestamp_to_seconds,
        )
        bars, using_closed_candles = self._resample_closed_candle_bars(raw_prices, raw_timestamps)
        prices = [bar.close for bar in bars]
        timeframe_sec = self._resolve_timeframe_sec(raw_timestamps, using_closed_candles=using_closed_candles)
        latest_timestamp_sec = (
            self._timestamp_to_seconds(raw_timestamps[-1])
            if raw_timestamps
            else None
        )
        entry_mode_effective, entry_mode_override_applied = self._entry_mode_for_symbol(ctx.symbol)
        profile = self._profile_for_symbol(ctx.symbol)
        min_cross_gap_ratio = self._min_cross_gap_ratio_for_profile(profile)
        min_trend_gap_ratio_configured = self._min_trend_gap_ratio_configured_for_profile(profile)
        min_trend_gap_ratio = self._min_trend_gap_ratio_for_profile(profile)
        min_trend_gap_ratio_auto_aligned = self._min_trend_gap_ratio_auto_aligned_for_profile(profile)
        confidence_velocity_norm_ratio = self._confidence_velocity_norm_ratio_for_profile(profile)
        min_history = self._profile_min_history(profile)
        if len(prices) < min_history:
            reason = "insufficient_candle_history" if using_closed_candles else "insufficient_price_history"
            return self._hold(
                reason,
                profile,
                {
                    "prices_len": len(prices),
                    "min_history": min_history,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                    "invalid_prices_dropped": invalid_prices,
                },
            )
        base_adx_entry_threshold = profile.adx_threshold + profile.adx_hysteresis
        base_adx_exit_threshold = max(0.0, profile.adx_threshold - profile.adx_hysteresis)
        adx_series_for_state: list[float] = []
        if self._has_instance_override("_adx"):
            adx_source = "close_override"
            adx_for_state = self._adx(prices, profile.adx_window)
        else:
            adx_source = "ohlc_resampled"
            adx_for_state = self._adx_from_bars(bars, profile.adx_window)
            adx_series_for_state = self._adx_series_from_bars(bars, profile.adx_window)
            if adx_for_state is None:
                adx_source = "close_fallback"
                adx_for_state = self._adx(prices, profile.adx_window)
                adx_series_for_state = self._adx_series_from_close(prices, profile.adx_window)
        if adx_for_state is not None:
            base_adx_regime_seed_active = self._infer_adx_regime_seed_active(
                adx_series_for_state,
                base_adx_entry_threshold,
                base_adx_exit_threshold,
            )
            self._refresh_adx_regime_state(
                ctx.symbol,
                profile,
                timeframe_sec,
                using_closed_candles=using_closed_candles,
                entry_mode=entry_mode_effective,
                latest_timestamp_sec=latest_timestamp_sec,
                adx=adx_for_state,
                entry_threshold=base_adx_entry_threshold,
                exit_threshold=base_adx_exit_threshold,
                seed_active=base_adx_regime_seed_active,
            )
        if self.index_require_context_tick_size and self._is_index_symbol(ctx.symbol) and not self._has_context_pip_size(ctx):
            return self._hold(
                "tick_size_unavailable",
                profile,
                {"index_require_context_tick_size": True},
            )

        fast_prev, fast_now = self._ema_last_two(prices, profile.fast_ema_window)
        slow_prev, slow_now = self._ema_last_two(prices, profile.slow_ema_window)
        fast_before_prev = self._ema(prices[:-2], profile.fast_ema_window)
        slow_before_prev = self._ema(prices[:-2], profile.slow_ema_window)
        fast_slope_ratio = self._slope_ratio(fast_now, fast_prev)
        slow_slope_ratio = self._slope_ratio(slow_now, slow_prev)
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)

        last_price = float(prices[-1])
        ema_diff_now = fast_now - slow_now
        ema_diff_prev = fast_prev - slow_prev
        ema_diff_before_prev = fast_before_prev - slow_before_prev
        gap_velocity_prev, gap_velocity_now, gap_acceleration_ratio = self._gap_acceleration_ratio(
            ema_diff_before_prev,
            ema_diff_prev,
            ema_diff_now,
            slow_now,
        )
        bullish_gap_expanding = ema_diff_now > ema_diff_prev
        bearish_gap_expanding = ema_diff_now < ema_diff_prev
        bullish_gap_accelerating = gap_acceleration_ratio > 0.0
        bearish_gap_accelerating = gap_acceleration_ratio < 0.0
        bullish_cross = fast_prev <= slow_prev and fast_now > slow_now
        bearish_cross = fast_prev >= slow_prev and fast_now < slow_now
        trend_up = (
            fast_now > slow_now
            and slow_now >= slow_prev
            and ema_gap_ratio >= min_trend_gap_ratio
        )
        trend_down = (
            fast_now < slow_now
            and slow_now <= slow_prev
            and ema_gap_ratio >= min_trend_gap_ratio
        )

        trend_signal = ""
        side = Side.HOLD
        if bullish_cross:
            side = Side.BUY
            trend_signal = "ema_cross_up"
        elif bearish_cross:
            side = Side.SELL
            trend_signal = "ema_cross_down"
        elif entry_mode_effective == "cross_or_trend":
            if trend_up:
                side = Side.BUY
                trend_signal = "ema_trend_up"
            elif trend_down:
                side = Side.SELL
                trend_signal = "ema_trend_down"

        if side == Side.HOLD:
            return self._hold(
                "no_ema_cross",
                profile,
                {
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": entry_mode_override_applied,
                    "ema_gap_ratio": ema_gap_ratio,
                    "min_trend_gap_ratio": min_trend_gap_ratio,
                    "g1_min_trend_gap_ratio_configured": min_trend_gap_ratio_configured,
                    "g1_min_trend_gap_ratio_effective": min_trend_gap_ratio,
                    "g1_min_trend_gap_ratio_auto_aligned": min_trend_gap_ratio_auto_aligned,
                    "g1_min_cross_gap_ratio": min_cross_gap_ratio,
                    "ema_diff_now": ema_diff_now,
                    "ema_diff_prev": ema_diff_prev,
                    "bullish_gap_expanding": bullish_gap_expanding,
                    "bearish_gap_expanding": bearish_gap_expanding,
                    "last_price": last_price,
                    "fast_ema": fast_now,
                    "slow_ema": slow_now,
                    "bullish_cross": bullish_cross,
                    "bearish_cross": bearish_cross,
                },
            )
        if trend_signal in {"ema_cross_up", "ema_cross_down"} and ema_gap_ratio < min_cross_gap_ratio:
            return self._hold(
                "ema_cross_gap_too_small",
                profile,
                {
                    "trend_signal": trend_signal,
                    "ema_gap_ratio": ema_gap_ratio,
                    "g1_min_cross_gap_ratio": min_cross_gap_ratio,
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": entry_mode_override_applied,
                },
            )

        directional_slow_slope_ratio = self._directional_ratio(slow_slope_ratio, side)
        if directional_slow_slope_ratio < profile.min_slow_slope_ratio:
            slow_slope_penalty = self._SECONDARY_SLOW_SLOPE_PENALTY
            slow_slope_softened = True
        else:
            slow_slope_penalty = 0.0
            slow_slope_softened = False

        if not self._confirmed_by_closed_candles(
            prices,
            profile,
            side,
            using_closed_candles=using_closed_candles,
        ):
            return self._hold(
                "candle_not_confirmed",
                profile,
                {
                    "candle_confirm_bars": self.candle_confirm_bars,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": entry_mode_override_applied,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                },
            )

        adx = adx_for_state if adx_for_state is not None else self._adx(prices, profile.adx_window)
        if adx is not None and adx_for_state is None and adx_source not in {"close_override"}:
            adx_source = "close_fallback"
        if adx is None:
            return self._hold("adx_unavailable", profile)
        adx_entry_threshold = base_adx_entry_threshold
        adx_exit_threshold = base_adx_exit_threshold
        is_cross_entry = trend_signal in {"ema_cross_up", "ema_cross_down"}
        is_continuation_entry = trend_signal in {"ema_trend_up", "ema_trend_down"}
        effective_adx_entry_threshold = adx_entry_threshold
        effective_adx_exit_threshold = adx_exit_threshold
        cross_adx_floor_applied = 0.0
        continuation_adx_floor_applied = 0.0
        adx_relief_mode = "none"
        directional_gap_acceleration_ratio = self._directional_ratio(gap_acceleration_ratio, side)
        cross_relief_min_gap_acceleration_ratio = max(
            min_cross_gap_ratio * 0.25,
            confidence_velocity_norm_ratio * 0.10,
            1e-6,
        )
        cross_relief_eligible = False
        if is_cross_entry:
            raw_effective_adx_entry_threshold = adx_entry_threshold * self.cross_adx_multiplier
            raw_effective_adx_exit_threshold = adx_exit_threshold * self.cross_adx_multiplier
            ratio_effective_adx_entry_threshold = adx_entry_threshold * self.cross_min_entry_threshold_ratio
            ratio_effective_adx_exit_threshold = adx_exit_threshold * self.cross_min_entry_threshold_ratio
            cross_relief_eligible = directional_gap_acceleration_ratio >= cross_relief_min_gap_acceleration_ratio
            if cross_relief_eligible:
                cross_adx_floor_applied = self.cross_min_adx
                effective_adx_entry_threshold = max(
                    raw_effective_adx_entry_threshold,
                    self.cross_min_adx,
                    ratio_effective_adx_entry_threshold,
                )
                effective_adx_exit_threshold = max(
                    raw_effective_adx_exit_threshold,
                    ratio_effective_adx_exit_threshold,
                )
                adx_relief_mode = "cross"
            else:
                adx_relief_mode = "cross_strict"
        elif is_continuation_entry:
            raw_effective_adx_entry_threshold = adx_entry_threshold * self.continuation_adx_multiplier
            raw_effective_adx_exit_threshold = adx_exit_threshold * self.continuation_adx_multiplier
            ratio_effective_adx_entry_threshold = (
                adx_entry_threshold * self.continuation_min_entry_threshold_ratio
            )
            ratio_effective_adx_exit_threshold = (
                adx_exit_threshold * self.continuation_min_entry_threshold_ratio
            )
            continuation_adx_floor_applied = self.continuation_min_adx
            effective_adx_entry_threshold = max(
                raw_effective_adx_entry_threshold,
                self.continuation_min_adx,
                ratio_effective_adx_entry_threshold,
            )
            effective_adx_exit_threshold = max(
                raw_effective_adx_exit_threshold,
                ratio_effective_adx_exit_threshold,
            )
            adx_relief_mode = "continuation"
        adx_delta = adx - profile.adx_threshold
        adx_delta_to_entry = adx - adx_entry_threshold
        adx_delta_to_exit = adx - adx_exit_threshold
        adx_delta_to_effective_entry = adx - effective_adx_entry_threshold
        adx_delta_to_effective_exit = adx - effective_adx_exit_threshold
        effective_adx_regime_seed_active = self._infer_adx_regime_seed_active(
            adx_series_for_state,
            effective_adx_entry_threshold,
            effective_adx_exit_threshold,
        )
        adx_scope_key = self._adx_scope_key(
            ctx.symbol,
            side,
            profile,
            timeframe_sec,
            using_closed_candles=using_closed_candles,
            entry_mode=entry_mode_effective,
        )
        self._maybe_reset_adx_scope_for_timestamp(adx_scope_key, latest_timestamp_sec)
        adx_gate_passes, adx_regime_active_before, adx_regime_active_after = self._adx_gate_passes(
            adx_scope_key,
            adx,
            effective_adx_entry_threshold,
            effective_adx_exit_threshold,
            seed_active=effective_adx_regime_seed_active,
        )
        if not adx_gate_passes:
            adx_penalty = self._SECONDARY_ADX_PENALTY
            adx_softened = True
        else:
            adx_penalty = 0.0
            adx_softened = False

        spread_limit, spread_limit_source = self._spread_limit_for_symbol(profile, ctx.symbol)
        if ctx.current_spread_pips is not None and ctx.current_spread_pips > spread_limit:
            return self._hold(
                "spread_too_wide",
                profile,
                {
                    "spread_pips": ctx.current_spread_pips,
                    "max_spread_pips": spread_limit,
                    "max_spread_pips_source": spread_limit_source,
                },
            )

        if self._has_instance_override("_atr"):
            atr_source = "close_override"
            atr = self._atr(prices, profile.atr_window)
        else:
            atr_source = "ohlc_resampled"
            atr = self._atr_from_bars(bars, profile.atr_window)
            if atr is None:
                atr_source = "close_fallback"
                atr = self._atr(prices, profile.atr_window)
        if atr is None or atr <= 0:
            return self._hold("atr_unavailable", profile)
        kama_gate_passes, kama_payload = self._kama_gate_payload(prices, atr, side)
        if not kama_gate_passes:
            kama_penalty = self._SECONDARY_KAMA_PENALTY
            kama_softened = True
        else:
            kama_penalty = 0.0
            kama_softened = False

        continuation_fast_ema_retest_atr_tolerance = self._continuation_fast_ema_retest_atr_tolerance_for_profile(
            profile
        )
        fast_ema_retest_gap_abs = abs(last_price - fast_now)
        fast_ema_retest_gap_atr_ratio = fast_ema_retest_gap_abs / max(atr, FLOAT_COMPARISON_TOLERANCE)
        continuation_trend_intact = last_price > slow_now if side == Side.BUY else last_price < slow_now
        if is_continuation_entry and (
            fast_ema_retest_gap_atr_ratio > continuation_fast_ema_retest_atr_tolerance
            or not continuation_trend_intact
        ):
            return self._hold(
                "no_pullback_reset",
                profile,
                {
                    "trend_signal": trend_signal,
                    "entry_mode": entry_mode_effective,
                    "configured_entry_mode": self.entry_mode,
                    "entry_mode_override_applied": entry_mode_override_applied,
                    "fast_ema": fast_now,
                    "slow_ema": slow_now,
                    "last_price": last_price,
                    "atr": atr,
                    "fast_ema_retest_gap_abs": fast_ema_retest_gap_abs,
                    "fast_ema_retest_gap_atr_ratio": fast_ema_retest_gap_atr_ratio,
                    "continuation_fast_ema_retest_atr_tolerance": continuation_fast_ema_retest_atr_tolerance,
                    "continuation_trend_intact": continuation_trend_intact,
                },
            )

        price_ema_gap_abs = abs(prices[-1] - slow_now)
        price_ema_gap_ratio = price_ema_gap_abs / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)
        price_ema_gap_atr_ratio = price_ema_gap_abs / max(atr, FLOAT_COMPARISON_TOLERANCE)
        adaptive_price_ema_gap_abs = atr * profile.max_price_ema_gap_atr_multiple
        adaptive_price_ema_gap_ratio = adaptive_price_ema_gap_abs / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)
        effective_max_price_ema_gap_ratio = max(
            profile.max_price_ema_gap_ratio,
            adaptive_price_ema_gap_ratio,
        )
        if price_ema_gap_ratio > effective_max_price_ema_gap_ratio:
            return self._hold(
                "price_too_far_from_ema",
                profile,
                {
                    "price_ema_gap_abs": price_ema_gap_abs,
                    "price_ema_gap_ratio": price_ema_gap_ratio,
                    "price_ema_gap_atr_ratio": price_ema_gap_atr_ratio,
                    "fast_ema_retest_gap_abs": fast_ema_retest_gap_abs,
                    "fast_ema_retest_gap_atr_ratio": fast_ema_retest_gap_atr_ratio,
                    "continuation_fast_ema_retest_atr_tolerance": continuation_fast_ema_retest_atr_tolerance,
                    "continuation_trend_intact": continuation_trend_intact,
                    "max_price_ema_gap_ratio": profile.max_price_ema_gap_ratio,
                    "max_price_ema_gap_atr_multiple": profile.max_price_ema_gap_atr_multiple,
                    "adaptive_price_ema_gap_abs": adaptive_price_ema_gap_abs,
                    "adaptive_price_ema_gap_ratio": adaptive_price_ema_gap_ratio,
                    "effective_max_price_ema_gap_ratio": effective_max_price_ema_gap_ratio,
                },
            )

        low_tf_risk_caps_applied = self._is_low_timeframe_for_risk(timeframe_sec)

        effective_atr_multiplier = profile.atr_multiplier
        effective_risk_reward_ratio = profile.risk_reward_ratio
        effective_min_relative_stop_pct = self.min_relative_stop_pct
        if low_tf_risk_caps_applied:
            effective_atr_multiplier = min(effective_atr_multiplier, self.low_tf_atr_multiplier_cap)
            effective_risk_reward_ratio = min(effective_risk_reward_ratio, self.low_tf_risk_reward_ratio_cap)
            effective_min_relative_stop_pct = min(
                effective_min_relative_stop_pct,
                self.low_tf_min_relative_stop_pct_cap,
            )

        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
        current_price = max(abs(prices[-1]), FLOAT_COMPARISON_TOLERANCE)
        atr_percent = (atr / current_price) * 100.0
        if self._is_index_symbol(ctx.symbol) and atr_percent < self.index_low_vol_atr_pct_threshold:
            effective_atr_multiplier *= self.index_low_vol_multiplier

        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        stop_loss_pips = max(profile.min_stop_loss_pips, atr_pips * effective_atr_multiplier)

        min_relative_stop_pips = 0.0
        if effective_min_relative_stop_pct > 0.0:
            min_relative_stop_pips = (current_price * effective_min_relative_stop_pct) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)

        take_profit_pips = stop_loss_pips * effective_risk_reward_ratio

        ema_gap_norm = max(min_trend_gap_ratio, profile.max_price_ema_gap_ratio * 0.5, 1e-4)
        base_directional_velocity_ratio = (
            max(0.0, fast_slope_ratio) + max(0.0, slow_slope_ratio) * 0.5
            if side == Side.BUY
            else max(0.0, -fast_slope_ratio) + max(0.0, -slow_slope_ratio) * 0.5
        )
        directional_velocity_ratio = base_directional_velocity_ratio + directional_gap_acceleration_ratio
        price_aligned_with_fast_ma = prices[-1] >= fast_now if side == Side.BUY else prices[-1] <= fast_now
        confidence = self._confidence(
            adx=adx,
            effective_adx_entry_threshold=effective_adx_entry_threshold,
            ema_gap_ratio=ema_gap_ratio,
            ema_gap_norm=ema_gap_norm,
            directional_velocity_ratio=directional_velocity_ratio,
            price_aligned_with_fast_ma=price_aligned_with_fast_ma,
            is_continuation_entry=is_continuation_entry,
            velocity_norm_ratio=confidence_velocity_norm_ratio,
        )
        volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(
            ctx,
            profile=profile,
            using_closed_candles=using_closed_candles,
        )
        if volume_hold_reason is not None:
            volume_penalty = (
                self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                if volume_hold_reason == "volume_unavailable"
                else self._SECONDARY_VOLUME_PENALTY
            )
            volume_softened = True
        else:
            volume_penalty = 0.0
            volume_softened = False
        confidence = min(1.0, confidence + volume_confidence_boost)
        soft_filter_reasons: list[str] = []
        if slow_slope_softened:
            soft_filter_reasons.append("slow_ema_too_flat")
        if adx_softened:
            soft_filter_reasons.append("adx_below_threshold")
        if kama_softened:
            soft_filter_reasons.append("kama_chop_regime")
        if volume_softened and volume_hold_reason is not None:
            soft_filter_reasons.append(volume_hold_reason)
        soft_filter_penalty_total = slow_slope_penalty + adx_penalty + kama_penalty + volume_penalty
        confidence = max(0.0, confidence - soft_filter_penalty_total)

        signal = Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            metadata={
                "indicator": "g1",
                "profile": profile.name,
                "entry_mode": entry_mode_effective,
                "configured_entry_mode": self.entry_mode,
                "entry_mode_override_applied": entry_mode_override_applied,
                "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                "trend_signal": trend_signal,
                "fast_ema_window": profile.fast_ema_window,
                "slow_ema_window": profile.slow_ema_window,
                "last_price": last_price,
                "fast_ema": fast_now,
                "slow_ema": slow_now,
                "ema_diff_now": ema_diff_now,
                "ema_diff_prev": ema_diff_prev,
                "ema_diff_before_prev": ema_diff_before_prev,
                "bullish_gap_expanding": bullish_gap_expanding,
                "bearish_gap_expanding": bearish_gap_expanding,
                "bullish_gap_accelerating": bullish_gap_accelerating,
                "bearish_gap_accelerating": bearish_gap_accelerating,
                "fast_slope_ratio": fast_slope_ratio,
                "slow_slope_ratio": slow_slope_ratio,
                "directional_slow_slope_ratio": directional_slow_slope_ratio,
                "min_slow_slope_ratio": profile.min_slow_slope_ratio,
                "slow_slope_softened": slow_slope_softened,
                "slow_slope_penalty": slow_slope_penalty,
                "gap_velocity_prev": gap_velocity_prev,
                "gap_velocity_now": gap_velocity_now,
                "gap_acceleration_ratio": gap_acceleration_ratio,
                "directional_gap_acceleration_ratio": directional_gap_acceleration_ratio,
                "base_directional_velocity_ratio": base_directional_velocity_ratio,
                "directional_velocity_ratio": directional_velocity_ratio,
                "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                "adx": adx,
                "adx_threshold": profile.adx_threshold,
                "adx_hysteresis": profile.adx_hysteresis,
                "adx_delta": adx_delta,
                "adx_entry_threshold": adx_entry_threshold,
                "adx_exit_threshold": adx_exit_threshold,
                "adx_delta_to_entry": adx_delta_to_entry,
                "adx_delta_to_exit": adx_delta_to_exit,
                "effective_adx_entry_threshold": effective_adx_entry_threshold,
                "effective_adx_exit_threshold": effective_adx_exit_threshold,
                "adx_delta_to_effective_entry": adx_delta_to_effective_entry,
                "adx_delta_to_effective_exit": adx_delta_to_effective_exit,
                "is_cross_entry": is_cross_entry,
                "is_continuation_entry": is_continuation_entry,
                "adx_relief_mode": adx_relief_mode,
                "cross_adx_multiplier": self.cross_adx_multiplier,
                "cross_min_adx": self.cross_min_adx,
                "cross_min_entry_threshold_ratio": self.cross_min_entry_threshold_ratio,
                "cross_adx_floor_applied": cross_adx_floor_applied,
                "cross_relief_eligible": cross_relief_eligible,
                "cross_relief_min_gap_acceleration_ratio": cross_relief_min_gap_acceleration_ratio,
                "continuation_adx_multiplier": self.continuation_adx_multiplier,
                "continuation_min_adx": self.continuation_min_adx,
                "continuation_min_entry_threshold_ratio": self.continuation_min_entry_threshold_ratio,
                "continuation_adx_floor_applied": continuation_adx_floor_applied,
                "adx_softened": adx_softened,
                "adx_penalty": adx_penalty,
                "use_adx_hysteresis_state": self.use_adx_hysteresis_state,
                "adx_state_refreshed_pre_signal": adx_for_state is not None,
                "adx_regime_active_before": adx_regime_active_before,
                "adx_regime_active_after": adx_regime_active_after,
                "adx_regime_seed_active": effective_adx_regime_seed_active,
                "adx_regime_seed_from_history": effective_adx_regime_seed_active is not None,
                "adx_state_scope": adx_scope_key,
                "adx_source": adx_source,
                "atr": atr,
                "atr_window": profile.atr_window,
                "atr_source": atr_source,
                "atr_multiplier": profile.atr_multiplier,
                "dynamic_atr_multiplier": effective_atr_multiplier,
                "atr_percent": atr_percent,
                "index_low_vol_atr_pct_threshold": self.index_low_vol_atr_pct_threshold,
                "index_low_vol_multiplier": self.index_low_vol_multiplier,
                **kama_payload,
                "kama_softened": kama_softened,
                "kama_penalty": kama_penalty,
                "atr_pips": atr_pips,
                "pip_size": pip_size,
                "pip_size_source": pip_size_source,
                "min_relative_stop_pct": self.min_relative_stop_pct,
                "effective_min_relative_stop_pct": effective_min_relative_stop_pct,
                "min_relative_stop_pips": min_relative_stop_pips,
                "risk_reward_ratio": profile.risk_reward_ratio,
                "effective_risk_reward_ratio": effective_risk_reward_ratio,
                "low_tf_risk_profile_enabled": self.low_tf_risk_profile_enabled,
                "low_tf_max_timeframe_sec": self.low_tf_max_timeframe_sec,
                "low_tf_atr_multiplier_cap": self.low_tf_atr_multiplier_cap,
                "low_tf_risk_reward_ratio_cap": self.low_tf_risk_reward_ratio_cap,
                "low_tf_min_relative_stop_pct_cap": self.low_tf_min_relative_stop_pct_cap,
                "timeframe_sec": timeframe_sec,
                "low_tf_risk_caps_applied": low_tf_risk_caps_applied,
                "spread_pips": ctx.current_spread_pips,
                "max_spread_pips": spread_limit,
                "max_spread_pips_source": spread_limit_source,
                "price_ema_gap_abs": price_ema_gap_abs,
                "price_ema_gap_ratio": price_ema_gap_ratio,
                "price_ema_gap_atr_ratio": price_ema_gap_atr_ratio,
                "fast_ema_retest_gap_abs": fast_ema_retest_gap_abs,
                "fast_ema_retest_gap_atr_ratio": fast_ema_retest_gap_atr_ratio,
                "continuation_fast_ema_retest_atr_tolerance": continuation_fast_ema_retest_atr_tolerance,
                "continuation_trend_intact": continuation_trend_intact,
                "max_price_ema_gap_ratio": profile.max_price_ema_gap_ratio,
                "max_price_ema_gap_atr_multiple": profile.max_price_ema_gap_atr_multiple,
                "adaptive_price_ema_gap_abs": adaptive_price_ema_gap_abs,
                "adaptive_price_ema_gap_ratio": adaptive_price_ema_gap_ratio,
                "effective_max_price_ema_gap_ratio": effective_max_price_ema_gap_ratio,
                "volume_softened": volume_softened,
                "volume_penalty": volume_penalty,
                "profile_override": self.profile_override,
                "ema_gap_norm": ema_gap_norm,
                "soft_filter_penalty_total": soft_filter_penalty_total,
                "soft_filter_reasons": soft_filter_reasons,
                "soft_filter_count": len(soft_filter_reasons),
                "confidence_model": "velocity_weighted_v4_parametric",
                "g1_confidence_base": self.confidence_base,
                "g1_confidence_adx_weight": self.confidence_adx_weight,
                "g1_confidence_gap_weight": self.confidence_gap_weight,
                "g1_confidence_velocity_weight": self.confidence_velocity_weight,
                "g1_cross_confidence_cap": self.cross_confidence_cap,
                "g1_continuation_confidence_cap": self.continuation_confidence_cap,
                "g1_continuation_velocity_weight_multiplier": self.continuation_velocity_weight_multiplier,
                "g1_continuation_price_alignment_bonus_multiplier": self.continuation_price_alignment_bonus_multiplier,
                "g1_confidence_weight_sum": (
                    self.confidence_base
                    + self.confidence_adx_weight
                    + self.confidence_gap_weight
                    + self.confidence_velocity_weight
                    + self.confidence_price_alignment_bonus
                ),
                "g1_confidence_velocity_norm_ratio": confidence_velocity_norm_ratio,
                "g1_confidence_adx_norm_multiplier": self.confidence_adx_norm_multiplier,
                "g1_confidence_price_alignment_bonus": self.confidence_price_alignment_bonus,
                "g1_confidence_cap_effective": (
                    self.continuation_confidence_cap if is_continuation_entry else self.cross_confidence_cap
                ),
                "g1_min_cross_gap_ratio": min_cross_gap_ratio,
                "g1_min_trend_gap_ratio_configured": min_trend_gap_ratio_configured,
                "g1_min_trend_gap_ratio_effective": min_trend_gap_ratio,
                "g1_min_trend_gap_ratio_auto_aligned": min_trend_gap_ratio_auto_aligned,
                "candle_timeframe_sec": self.candle_timeframe_sec,
                "candle_confirm_bars": self.candle_confirm_bars,
                "using_closed_candles": using_closed_candles,
                "ignore_sunday_candles": self.ignore_sunday_candles,
                "resample_mode": self.resample_mode,
                "adx_warmup_multiplier": self.adx_warmup_multiplier,
                "adx_warmup_extra_bars": self.adx_warmup_extra_bars,
                "volume_confidence_boost_applied": volume_confidence_boost,
                **volume_meta,
                **self._trailing_payload(fast_now),
            },
        )
        return self._finalize_entry_signal(signal, prices=prices)
