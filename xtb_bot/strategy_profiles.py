from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any


def _freeze_mapping(values: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(values))


def _collect_profile_keys(profiles: Mapping[str, Mapping[str, Any]]) -> frozenset[str]:
    return frozenset(key for payload in profiles.values() for key in payload)


def _apply_parameter_surface(
    original_params: Mapping[str, Any],
    *,
    default_profile: Mapping[str, Any],
    managed_keys: frozenset[str],
    allowed_keys: frozenset[str],
) -> tuple[dict[str, Any], list[str]]:
    merged = {key: value for key, value in original_params.items() if key not in managed_keys}
    merged.update(default_profile)

    ignored_keys: list[str] = []
    for key, value in original_params.items():
        if key not in managed_keys:
            continue
        if key in allowed_keys:
            merged[key] = value
            continue
        if key in default_profile and default_profile.get(key) != value:
            ignored_keys.append(key)
    return merged, sorted(ignored_keys)


_INDEX_HYBRID_SAFE: Mapping[str, Any] = _freeze_mapping({
    "index_mean_reversion_entry_mode": "hook",
    "index_fast_ema_window": 21,
    "index_slow_ema_window": 110,
    "index_donchian_window": 14,
    "index_zscore_window": 55,
    "index_window_sync_mode": "off",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "detrended",
    "index_zscore_ema_window": 120,
    "index_zscore_threshold": 2.1,
    "index_require_non_fallback_pip_size": True,
    "index_regime_selection_mode": "hard",
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.00045,
    "index_mean_reversion_gap_threshold": 0.00018,
    "index_atr_window": 14,
    "index_trend_atr_pct_threshold": 0.012,
    "index_mean_reversion_atr_pct_threshold": 0.028,
    "index_mean_reversion_allow_breakout": False,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_mean_reversion_require_band_proximity": True,
    "index_mean_reversion_band_proximity_max_ratio": 0.18,
    "index_mean_reversion_min_mid_deviation_ratio": 0.24,
    "index_min_breakout_distance_ratio": 0.00010,
    "index_max_spread_pips": 0.0,
    "index_max_spread_to_breakout_ratio": 0.55,
    "index_max_spread_to_stop_ratio": 0.30,
    "index_min_channel_width_atr": 0.90,
    "index_volume_confirmation": True,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.50,
    "index_volume_min_ratio_for_entry": 1.0,
    "index_volume_min_samples": 10,
    "index_volume_allow_missing": False,
    "index_volume_require_spike": True,
    "index_volume_confidence_boost": 0.08,
    "index_volume_as_bonus_only": False,
    "index_stop_loss_pips": 34.0,
    "index_take_profit_pips": 90.0,
    "index_stop_loss_pct": 0.15,
    "index_take_profit_pct": 0.45,
    "index_stop_atr_multiplier": 2.1,
    "index_risk_reward_ratio": 2.3,
    "index_trend_pct_floors_enabled": True,
    "index_mean_reversion_pct_floors_enabled": False,
    "index_confidence_threshold_cap_enabled": True,
    "index_confidence_threshold_cap_trend": 0.55,
    "index_confidence_threshold_cap_mean_reversion": 0.55,
    "index_mean_reversion_confidence_base": 0.10,
    "index_session_filter_enabled": True,
    "index_session_profile_mode": "market_presets",
    "index_trend_session_open_delay_minutes": 20,
    "index_trend_volatility_explosion_filter_enabled": True,
    "index_trend_volatility_explosion_lookback": 24,
    "index_trend_volatility_explosion_min_ratio": 1.20,
    "index_mean_reversion_outside_trend_session": True,
    "index_hybrid_trade_cooldown_sec": 420.0,
    "index_hybrid_signal_only_min_confidence_for_entry": 0.62,
    "index_hybrid_paper_min_confidence_for_entry": 0.67,
    "index_hybrid_execution_min_confidence_for_entry": 0.74,
    "index_hybrid_mean_reversion_signal_only_min_confidence_for_entry": 0.40,
    "index_hybrid_mean_reversion_paper_min_confidence_for_entry": 0.42,
    "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.58,
    "index_hybrid_mean_reversion_execution_confidence_hard_floor": 0.58,
})

_INDEX_HYBRID_CONSERVATIVE: Mapping[str, Any] = _freeze_mapping({
    "index_mean_reversion_entry_mode": "hook",
    "index_fast_ema_window": 21,
    "index_slow_ema_window": 89,
    "index_donchian_window": 14,
    "index_zscore_window": 48,
    "index_window_sync_mode": "off",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "detrended",
    "index_zscore_ema_window": 100,
    "index_zscore_threshold": 2.0,
    "index_require_non_fallback_pip_size": True,
    "index_regime_selection_mode": "hard",
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.00040,
    "index_mean_reversion_gap_threshold": 0.00016,
    "index_atr_window": 14,
    "index_trend_atr_pct_threshold": 0.01,
    "index_mean_reversion_atr_pct_threshold": 0.03,
    "index_mean_reversion_allow_breakout": False,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_mean_reversion_require_band_proximity": True,
    "index_mean_reversion_band_proximity_max_ratio": 0.20,
    "index_mean_reversion_min_mid_deviation_ratio": 0.20,
    "index_min_breakout_distance_ratio": 0.00008,
    "index_max_spread_pips": 0.0,
    "index_max_spread_to_breakout_ratio": 0.6,
    "index_max_spread_to_stop_ratio": 0.35,
    "index_min_channel_width_atr": 0.8,
    "index_volume_confirmation": True,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.45,
    "index_volume_min_ratio_for_entry": 1.0,
    "index_volume_min_samples": 10,
    "index_volume_allow_missing": False,
    "index_volume_require_spike": True,
    "index_volume_confidence_boost": 0.08,
    "index_volume_as_bonus_only": False,
    "index_stop_loss_pips": 30.0,
    "index_take_profit_pips": 80.0,
    "index_stop_loss_pct": 0.15,
    "index_take_profit_pct": 0.45,
    "index_stop_atr_multiplier": 2.0,
    "index_risk_reward_ratio": 2.2,
    "index_trend_pct_floors_enabled": True,
    "index_mean_reversion_pct_floors_enabled": False,
    "index_confidence_threshold_cap_enabled": True,
    "index_confidence_threshold_cap_trend": 0.58,
    "index_confidence_threshold_cap_mean_reversion": 0.58,
    "index_mean_reversion_confidence_base": 0.10,
    "index_session_filter_enabled": True,
    "index_session_profile_mode": "market_presets",
    "index_trend_session_open_delay_minutes": 15,
    "index_trend_volatility_explosion_filter_enabled": True,
    "index_trend_volatility_explosion_lookback": 20,
    "index_trend_volatility_explosion_min_ratio": 1.20,
    "index_mean_reversion_outside_trend_session": True,
    "index_hybrid_trade_cooldown_sec": 360.0,
    "index_hybrid_signal_only_min_confidence_for_entry": 0.60,
    "index_hybrid_paper_min_confidence_for_entry": 0.64,
    "index_hybrid_execution_min_confidence_for_entry": 0.70,
    "index_hybrid_mean_reversion_signal_only_min_confidence_for_entry": 0.38,
    "index_hybrid_mean_reversion_paper_min_confidence_for_entry": 0.40,
    "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.55,
    "index_hybrid_mean_reversion_execution_confidence_hard_floor": 0.55,
})

_INDEX_HYBRID_AGGRESSIVE: Mapping[str, Any] = _freeze_mapping({
    "index_mean_reversion_entry_mode": "hook",
    "index_fast_ema_window": 13,
    "index_slow_ema_window": 55,
    "index_donchian_window": 10,
    "index_zscore_window": 40,
    "index_window_sync_mode": "off",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "detrended",
    "index_zscore_ema_window": 89,
    "index_zscore_threshold": 1.9,
    "index_require_non_fallback_pip_size": True,
    "index_regime_selection_mode": "hard",
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.00032,
    "index_mean_reversion_gap_threshold": 0.00012,
    "index_atr_window": 14,
    "index_trend_atr_pct_threshold": 0.008,
    "index_mean_reversion_atr_pct_threshold": 0.035,
    "index_mean_reversion_allow_breakout": False,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_mean_reversion_require_band_proximity": True,
    "index_mean_reversion_band_proximity_max_ratio": 0.28,
    "index_mean_reversion_min_mid_deviation_ratio": 0.12,
    "index_min_breakout_distance_ratio": 0.00005,
    "index_max_spread_pips": 0.0,
    "index_max_spread_to_breakout_ratio": 0.8,
    "index_max_spread_to_stop_ratio": 0.45,
    "index_min_channel_width_atr": 0.65,
    "index_volume_confirmation": True,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.35,
    "index_volume_min_ratio_for_entry": 1.0,
    "index_volume_min_samples": 6,
    "index_volume_allow_missing": True,
    "index_volume_require_spike": False,
    "index_volume_confidence_boost": 0.06,
    "index_volume_as_bonus_only": True,
    "index_stop_loss_pips": 24.0,
    "index_take_profit_pips": 65.0,
    "index_stop_loss_pct": 0.12,
    "index_take_profit_pct": 0.36,
    "index_stop_atr_multiplier": 1.8,
    "index_risk_reward_ratio": 2.1,
    "index_trend_pct_floors_enabled": True,
    "index_mean_reversion_pct_floors_enabled": False,
    "index_confidence_threshold_cap_enabled": True,
    "index_confidence_threshold_cap_trend": 0.58,
    "index_confidence_threshold_cap_mean_reversion": 0.58,
    "index_mean_reversion_confidence_base": 0.10,
    "index_session_filter_enabled": True,
    "index_session_profile_mode": "market_presets",
    "index_trend_session_open_delay_minutes": 5,
    "index_trend_volatility_explosion_filter_enabled": True,
    "index_trend_volatility_explosion_lookback": 16,
    "index_trend_volatility_explosion_min_ratio": 1.20,
    "index_mean_reversion_outside_trend_session": True,
    "index_hybrid_trade_cooldown_sec": 240.0,
    "index_hybrid_signal_only_min_confidence_for_entry": 0.54,
    "index_hybrid_paper_min_confidence_for_entry": 0.58,
    "index_hybrid_execution_min_confidence_for_entry": 0.66,
    "index_hybrid_mean_reversion_signal_only_min_confidence_for_entry": 0.36,
    "index_hybrid_mean_reversion_paper_min_confidence_for_entry": 0.38,
    "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.55,
    "index_hybrid_mean_reversion_execution_confidence_hard_floor": 0.55,
})

_INDEX_HYBRID_TIERED_TUNABLE_KEYS: frozenset[str] = frozenset(
    {
        "index_fast_ema_window",
        "index_slow_ema_window",
        "index_donchian_window",
        "index_zscore_window",
        "index_window_sync_mode",
        "index_zscore_donchian_ratio_target",
        "index_zscore_donchian_ratio_tolerance",
        "index_zscore_mode",
        "index_zscore_ema_window",
        "index_zscore_threshold",
        "index_auto_correct_regime_thresholds",
        "index_enforce_gap_hysteresis",
        "index_gap_hysteresis_min",
        "index_trend_gap_threshold",
        "index_mean_reversion_gap_threshold",
        "index_mean_reversion_confidence_base",
        "index_trend_atr_pct_threshold",
        "index_mean_reversion_atr_pct_threshold",
    }
)
_INDEX_HYBRID_PROFILE_PRESERVE_KEYS: frozenset[str] = frozenset(
    {
        "index_window_sync_mode",
        "index_zscore_donchian_ratio_target",
        "index_zscore_donchian_ratio_tolerance",
        "index_zscore_mode",
        "index_zscore_ema_window",
        "index_auto_correct_regime_thresholds",
        "index_enforce_gap_hysteresis",
        "index_gap_hysteresis_min",
    }
)
_INDEX_HYBRID_SURFACE_CONTROL_KEYS: frozenset[str] = frozenset(
    {
        "index_parameter_surface_mode",
        "index_parameter_surface_tier",
    }
)

_DONCHIAN_BREAKOUT_LIVE_CORE_DEFAULTS: Mapping[str, Any] = _freeze_mapping({
    "donchian_breakout_window": 24,
    "donchian_breakout_exit_window": 8,
    "donchian_breakout_atr_window": 14,
    "donchian_breakout_atr_multiplier": 1.5,
    "donchian_breakout_risk_reward_ratio": 3.0,
    "donchian_breakout_min_stop_loss_pips": 30.0,
    "donchian_breakout_min_take_profit_pips": 90.0,
    "donchian_breakout_min_breakout_atr_ratio": 0.15,
    "donchian_breakout_max_breakout_atr_ratio": 1.8,
    "donchian_breakout_min_channel_width_atr": 1.0,
    "donchian_breakout_candle_timeframe_sec": 60.0,
    "donchian_breakout_resample_mode": "auto",
    "donchian_breakout_volume_confirmation": True,
})

_DONCHIAN_BREAKOUT_SURFACE_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "donchian_breakout_window",
        "donchian_breakout_exit_window",
        "donchian_breakout_atr_window",
        "donchian_breakout_atr_multiplier",
        "donchian_breakout_risk_reward_ratio",
        "donchian_breakout_min_stop_loss_pips",
        "donchian_breakout_min_take_profit_pips",
        "donchian_breakout_min_breakout_atr_ratio",
        "donchian_breakout_max_breakout_atr_ratio",
        "donchian_breakout_min_channel_width_atr",
        "donchian_breakout_candle_timeframe_sec",
        "donchian_breakout_resample_mode",
        "donchian_breakout_volume_confirmation",
        "donchian_breakout_max_spread_pips",
        "donchian_breakout_max_spread_pips_by_symbol",
    }
)

_INDEX_HYBRID_PROFILES: Mapping[str, Mapping[str, Any]] = _freeze_mapping({
    "safe": _INDEX_HYBRID_SAFE,
    "conservative": _INDEX_HYBRID_CONSERVATIVE,
    "aggressive": _INDEX_HYBRID_AGGRESSIVE,
})

_INDEX_HYBRID_PARAMETER_SURFACE_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "index_parameter_surface_locked",
        "index_parameter_surface_allowed_overrides",
        "index_parameter_surface_ignored_overrides",
        "index_parameter_surface_ignored_count",
    }
)
_INDEX_HYBRID_SURFACE_LOCKED_ONLY_KEYS: frozenset[str] = frozenset(
    {
        "index_require_context_tick_size",
        "index_max_spread_pips_by_symbol",
        "index_regime_trend_index_threshold",
        "index_trend_confidence_base",
        "index_trend_confidence_gap_weight",
        "index_trend_confidence_breakout_weight",
        "index_trend_confidence_gap_norm",
        "index_trend_confidence_breakout_norm",
        "index_hybrid_min_confidence_for_entry",
        "index_trend_session_start_hour_utc",
        "index_trend_session_end_hour_utc",
    }
)
_INDEX_HYBRID_SURFACE_MANAGED_KEYS: frozenset[str] = (
    _collect_profile_keys(_INDEX_HYBRID_PROFILES)
    | _INDEX_HYBRID_TIERED_TUNABLE_KEYS
    | _INDEX_HYBRID_SURFACE_CONTROL_KEYS
    | _INDEX_HYBRID_PARAMETER_SURFACE_METADATA_KEYS
    | _INDEX_HYBRID_SURFACE_LOCKED_ONLY_KEYS
)

_MEAN_BREAKOUT_V2_CONSERVATIVE: Mapping[str, Any] = _freeze_mapping({
    "mb_zscore_window": 50,
    "mb_breakout_window": 20,
    "mb_slope_window": 7,
    "mb_zscore_threshold": 1.85,
    "mb_zscore_entry_mode": "directional_extreme",
    "mb_directional_extreme_threshold_ratio": 0.9,
    "mb_min_slope_ratio": 0.0006,
    "mb_max_spread_pips": 3.0,
    "mb_breakout_min_buffer_pips": 0.0,
    "mb_breakout_min_buffer_atr_ratio": 0.10,
    "mb_breakout_min_buffer_spread_multiplier": 2.0,
    "mb_exhaustion_sprint_lookback_bars": 3,
    "mb_exhaustion_sprint_move_channel_ratio": 0.8,
    "mb_breakout_invalidation_enabled": True,
    "mb_breakout_invalidation_lookback_bars": 1,
    "mb_stop_loss_pips": 60.0,
    "mb_take_profit_pips": 120.0,
    "mb_risk_reward_ratio": 2.4,
    "mb_atr_window": 14,
    "mb_atr_multiplier": 1.5,
    "mb_min_stop_loss_pips": 80.0,
    "mb_min_take_profit_pips": 120.0,
    "mb_min_relative_stop_pct": 0.0016,
    "mb_dynamic_tp_only": True,
    "mb_dynamic_tp_respect_static_floor": True,
    "mb_adaptive_sl_max_atr_ratio": 5.0,
    "mb_require_context_tick_size_for_cfd": True,
    "mb_adaptive_timeframe_mode": "breakout_horizon",
    "mb_resample_mode": "always",
    "mb_volume_confirmation": True,
    "mb_volume_window": 20,
    "mb_min_volume_ratio": 1.4,
    "mb_volume_min_ratio_for_entry": 1.0,
    "mb_volume_min_samples": 10,
    "mb_volume_allow_missing": False,
    "mb_volume_require_spike": True,
    "mb_volume_confidence_boost": 0.12,
    "mb_confidence_base": 0.2,
    "mb_confidence_gate_margin_weight": 0.25,
    "mb_confidence_cost_weight": 0.24,
    "mb_confidence_extension_weight": 0.24,
    "mb_confidence_cost_spread_to_sl_target": 0.16,
    "mb_confidence_extension_soft_ratio": 0.18,
    "mb_confidence_extension_hard_ratio": 0.7,
    "mb_trailing_enabled": True,
    "mb_trailing_atr_multiplier": 1.7,
    "mb_trailing_activation_stop_ratio": 0.85,
    "mb_trailing_activation_max_tp_ratio": 0.33,
    "mb_trailing_distance_min_stop_ratio": 0.5,
    "mb_trailing_breakeven_offset_pips": 3.0,
})

_MEAN_BREAKOUT_V2_AGGRESSIVE: Mapping[str, Any] = _freeze_mapping({
    "mb_zscore_window": 45,
    "mb_breakout_window": 18,
    "mb_slope_window": 5,
    "mb_zscore_threshold": 1.75,
    "mb_zscore_entry_mode": "directional_extreme",
    "mb_directional_extreme_threshold_ratio": 0.9,
    "mb_min_slope_ratio": 0.00045,
    "mb_max_spread_pips": 5.0,
    "mb_breakout_min_buffer_pips": 0.0,
    "mb_breakout_min_buffer_atr_ratio": 0.08,
    "mb_breakout_min_buffer_spread_multiplier": 1.5,
    "mb_exhaustion_sprint_lookback_bars": 3,
    "mb_exhaustion_sprint_move_channel_ratio": 0.85,
    "mb_breakout_invalidation_enabled": True,
    "mb_breakout_invalidation_lookback_bars": 1,
    "mb_stop_loss_pips": 45.0,
    "mb_take_profit_pips": 105.0,
    "mb_risk_reward_ratio": 2.7,
    "mb_atr_window": 14,
    "mb_atr_multiplier": 1.45,
    "mb_min_stop_loss_pips": 50.0,
    "mb_min_take_profit_pips": 100.0,
    "mb_min_relative_stop_pct": 0.0010,
    "mb_dynamic_tp_only": True,
    "mb_dynamic_tp_respect_static_floor": True,
    "mb_adaptive_sl_max_atr_ratio": 4.0,
    "mb_require_context_tick_size_for_cfd": True,
    "mb_adaptive_timeframe_mode": "breakout_horizon",
    "mb_resample_mode": "always",
    "mb_volume_confirmation": True,
    "mb_volume_window": 20,
    "mb_min_volume_ratio": 1.35,
    "mb_volume_min_ratio_for_entry": 1.0,
    "mb_volume_min_samples": 8,
    "mb_volume_allow_missing": False,
    "mb_volume_require_spike": True,
    "mb_volume_confidence_boost": 0.10,
    "mb_confidence_base": 0.18,
    "mb_confidence_gate_margin_weight": 0.24,
    "mb_confidence_cost_weight": 0.18,
    "mb_confidence_extension_weight": 0.28,
    "mb_confidence_cost_spread_to_sl_target": 0.2,
    "mb_confidence_extension_soft_ratio": 0.15,
    "mb_confidence_extension_hard_ratio": 0.6,
    "mb_trailing_enabled": True,
    "mb_trailing_atr_multiplier": 1.6,
    "mb_trailing_activation_stop_ratio": 0.75,
    "mb_trailing_activation_max_tp_ratio": 0.38,
    "mb_trailing_distance_min_stop_ratio": 0.35,
    "mb_trailing_breakeven_offset_pips": 2.0,
})

_MEAN_BREAKOUT_V2_PROFILES: Mapping[str, Mapping[str, Any]] = _freeze_mapping({
    "safe": _MEAN_BREAKOUT_V2_CONSERVATIVE,
    "conservative": _MEAN_BREAKOUT_V2_CONSERVATIVE,
    "aggressive": _MEAN_BREAKOUT_V2_AGGRESSIVE,
})

_MEAN_REVERSION_BB_CONSERVATIVE: Mapping[str, Any] = _freeze_mapping({
    "mean_reversion_bb_entry_mode": "reentry",
    "mean_reversion_bb_reentry_tolerance_sigma": 0.18,
    "mean_reversion_bb_reentry_min_reversal_sigma": 0.22,
    "mean_reversion_bb_use_rsi_filter": True,
    "mean_reversion_bb_rsi_method": "wilder",
    "mean_reversion_bb_rsi_overbought": 85.0,
    "mean_reversion_bb_rsi_oversold": 15.0,
    "mean_reversion_bb_oscillator_mode": "connors",
    "mean_reversion_bb_oscillator_gate_mode": "soft",
    "mean_reversion_bb_connors_price_rsi_period": 3,
    "mean_reversion_bb_connors_streak_rsi_period": 2,
    "mean_reversion_bb_connors_rank_period": 20,
    "mean_reversion_bb_connors_price_rsi_weight": 0.5,
    "mean_reversion_bb_connors_streak_rsi_weight": 0.4,
    "mean_reversion_bb_connors_rank_weight": 0.1,
    "mean_reversion_bb_oscillator_soft_zone_width": 10.0,
    "mean_reversion_bb_oscillator_soft_confidence_penalty": 0.15,
    "mean_reversion_bb_candle_timeframe_sec": 60.0,
    "mean_reversion_bb_resample_mode": "auto",
    "mean_reversion_bb_ignore_sunday_candles": True,
    "mean_reversion_bb_std_dev": 2.4,
    "mean_reversion_bb_min_band_extension_ratio": 0.08,
    "mean_reversion_bb_max_band_extension_ratio": 1.5,
    "mean_reversion_bb_trend_filter_enabled": True,
    "mean_reversion_bb_trend_filter_mode": "strict",
    "mean_reversion_bb_trend_filter_extreme_sigma": 2.8,
    "mean_reversion_bb_trend_slope_lookback_bars": 5,
    "mean_reversion_bb_trend_slope_strict_threshold": 0.00015,
    "mean_reversion_bb_trend_countertrend_min_distance_sigma": 2.0,
    "mean_reversion_bb_trend_slope_block_max_distance_sigma": 1.8,
    "mean_reversion_bb_regime_adx_filter_enabled": True,
    "mean_reversion_bb_regime_adx_window": 14,
    "mean_reversion_bb_regime_min_adx": 16.0,
    "mean_reversion_bb_regime_max_adx": 20.0,
    "mean_reversion_bb_regime_adx_hysteresis": 2.0,
    "mean_reversion_bb_regime_use_hysteresis_state": True,
    "mean_reversion_bb_regime_volatility_expansion_filter_enabled": True,
    "mean_reversion_bb_regime_volatility_short_window": 14,
    "mean_reversion_bb_regime_volatility_long_window": 56,
    "mean_reversion_bb_regime_volatility_expansion_max_ratio": 1.35,
    "mean_reversion_bb_session_filter_enabled": True,
    "mean_reversion_bb_session_start_hour_utc": 6,
    "mean_reversion_bb_session_end_hour_utc": 22,
    "mean_reversion_bb_volume_confirmation": True,
    "mean_reversion_bb_volume_window": 20,
    "mean_reversion_bb_min_volume_ratio": 1.6,
    "mean_reversion_bb_volume_min_samples": 10,
    "mean_reversion_bb_volume_allow_missing": False,
    "mean_reversion_bb_volume_require_spike": True,
    "mean_reversion_bb_volume_spike_mode": "reversal_gate",
    "mean_reversion_bb_volume_confidence_boost": 0.15,
    "mean_reversion_bb_take_profit_mode": "rr",
    "mean_reversion_bb_exit_on_midline": False,
    "mean_reversion_bb_use_atr_sl_tp": True,
    "mean_reversion_bb_rejection_gate_enabled": True,
    "mean_reversion_bb_rejection_min_wick_to_body_ratio": 0.50,
    "mean_reversion_bb_rejection_min_wick_to_range_ratio": 0.18,
    "mean_reversion_bb_rejection_sell_max_close_location": 0.45,
    "mean_reversion_bb_rejection_buy_min_close_location": 0.55,
    "mean_reversion_bb_atr_window": 14,
    "mean_reversion_bb_atr_multiplier": 1.6,
    "mean_reversion_bb_risk_reward_ratio": 2.2,
    "mean_reversion_bb_min_stop_loss_pips": 25.0,
    "mean_reversion_bb_min_take_profit_pips": 40.0,
    "mean_reversion_bb_min_confidence_for_entry": 0.62,
    "mean_reversion_bb_signal_only_min_confidence_for_entry": 0.58,
    "mean_reversion_bb_paper_min_confidence_for_entry": 0.68,
    "mean_reversion_bb_execution_min_confidence_for_entry": 0.75,
    "mean_reversion_bb_trade_cooldown_sec": 360.0,
    "mean_reversion_bb_trade_cooldown_win_sec": 480.0,
    "mean_reversion_bb_trade_cooldown_loss_sec": 1800.0,
    "mean_reversion_bb_trade_cooldown_flat_sec": 360.0,
})

_MEAN_REVERSION_BB_AGGRESSIVE: Mapping[str, Any] = _freeze_mapping({
    "mean_reversion_bb_entry_mode": "reentry",
    "mean_reversion_bb_reentry_tolerance_sigma": 0.14,
    "mean_reversion_bb_reentry_min_reversal_sigma": 0.16,
    "mean_reversion_bb_use_rsi_filter": True,
    "mean_reversion_bb_rsi_method": "wilder",
    "mean_reversion_bb_rsi_overbought": 80.0,
    "mean_reversion_bb_rsi_oversold": 20.0,
    "mean_reversion_bb_oscillator_mode": "connors",
    "mean_reversion_bb_oscillator_gate_mode": "soft",
    "mean_reversion_bb_connors_price_rsi_period": 3,
    "mean_reversion_bb_connors_streak_rsi_period": 2,
    "mean_reversion_bb_connors_rank_period": 15,
    "mean_reversion_bb_connors_price_rsi_weight": 0.45,
    "mean_reversion_bb_connors_streak_rsi_weight": 0.4,
    "mean_reversion_bb_connors_rank_weight": 0.15,
    "mean_reversion_bb_oscillator_soft_zone_width": 12.0,
    "mean_reversion_bb_oscillator_soft_confidence_penalty": 0.10,
    "mean_reversion_bb_candle_timeframe_sec": 60.0,
    "mean_reversion_bb_resample_mode": "auto",
    "mean_reversion_bb_ignore_sunday_candles": True,
    "mean_reversion_bb_std_dev": 2.25,
    "mean_reversion_bb_min_band_extension_ratio": 0.06,
    "mean_reversion_bb_max_band_extension_ratio": 2.5,
    "mean_reversion_bb_trend_filter_enabled": True,
    "mean_reversion_bb_trend_filter_mode": "strict",
    "mean_reversion_bb_trend_filter_extreme_sigma": 2.2,
    "mean_reversion_bb_trend_slope_lookback_bars": 5,
    "mean_reversion_bb_trend_slope_strict_threshold": 0.00012,
    "mean_reversion_bb_trend_countertrend_min_distance_sigma": 1.8,
    "mean_reversion_bb_trend_slope_block_max_distance_sigma": 1.6,
    "mean_reversion_bb_regime_adx_filter_enabled": True,
    "mean_reversion_bb_regime_adx_window": 14,
    "mean_reversion_bb_regime_min_adx": 13.0,
    "mean_reversion_bb_regime_max_adx": 21.0,
    "mean_reversion_bb_regime_adx_hysteresis": 2.0,
    "mean_reversion_bb_regime_use_hysteresis_state": True,
    "mean_reversion_bb_regime_volatility_expansion_filter_enabled": True,
    "mean_reversion_bb_regime_volatility_short_window": 14,
    "mean_reversion_bb_regime_volatility_long_window": 56,
    "mean_reversion_bb_regime_volatility_expansion_max_ratio": 1.5,
    "mean_reversion_bb_session_filter_enabled": True,
    "mean_reversion_bb_session_start_hour_utc": 6,
    "mean_reversion_bb_session_end_hour_utc": 22,
    "mean_reversion_bb_volume_confirmation": True,
    "mean_reversion_bb_volume_window": 20,
    "mean_reversion_bb_min_volume_ratio": 1.5,
    "mean_reversion_bb_volume_min_samples": 8,
    "mean_reversion_bb_volume_allow_missing": False,
    "mean_reversion_bb_volume_require_spike": True,
    "mean_reversion_bb_volume_spike_mode": "reversal_gate",
    "mean_reversion_bb_volume_confidence_boost": 0.15,
    "mean_reversion_bb_take_profit_mode": "rr",
    "mean_reversion_bb_exit_on_midline": False,
    "mean_reversion_bb_use_atr_sl_tp": True,
    "mean_reversion_bb_rejection_gate_enabled": True,
    "mean_reversion_bb_rejection_min_wick_to_body_ratio": 0.35,
    "mean_reversion_bb_rejection_min_wick_to_range_ratio": 0.14,
    "mean_reversion_bb_rejection_sell_max_close_location": 0.48,
    "mean_reversion_bb_rejection_buy_min_close_location": 0.52,
    "mean_reversion_bb_atr_window": 14,
    "mean_reversion_bb_atr_multiplier": 1.35,
    "mean_reversion_bb_risk_reward_ratio": 2.0,
    "mean_reversion_bb_min_stop_loss_pips": 22.0,
    "mean_reversion_bb_min_take_profit_pips": 36.0,
    "mean_reversion_bb_min_confidence_for_entry": 0.45,
    "mean_reversion_bb_signal_only_min_confidence_for_entry": 0.45,
    "mean_reversion_bb_paper_min_confidence_for_entry": 0.52,
    "mean_reversion_bb_execution_min_confidence_for_entry": 0.70,
    "mean_reversion_bb_trade_cooldown_sec": 240.0,
    "mean_reversion_bb_trade_cooldown_win_sec": 300.0,
    "mean_reversion_bb_trade_cooldown_loss_sec": 1200.0,
    "mean_reversion_bb_trade_cooldown_flat_sec": 240.0,
})

_MEAN_REVERSION_BB_PROFILES: Mapping[str, Mapping[str, Any]] = _freeze_mapping({
    "safe": _MEAN_REVERSION_BB_CONSERVATIVE,
    "conservative": _MEAN_REVERSION_BB_CONSERVATIVE,
    "aggressive": _MEAN_REVERSION_BB_AGGRESSIVE,
})

_DONCHIAN_BREAKOUT_PARAMETER_SURFACE_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "donchian_breakout_parameter_surface_locked",
        "donchian_breakout_parameter_surface_allowed_overrides",
        "donchian_breakout_parameter_surface_ignored_overrides",
        "donchian_breakout_parameter_surface_ignored_count",
    }
)
_DONCHIAN_BREAKOUT_SURFACE_LOCKED_ONLY_KEYS: frozenset[str] = frozenset(
    {
        "donchian_breakout_min_relative_stop_pct",
        "donchian_breakout_volume_window",
        "donchian_breakout_min_volume_ratio",
        "donchian_breakout_volume_min_samples",
        "donchian_breakout_volume_allow_missing",
        "donchian_breakout_regime_filter_enabled",
        "donchian_breakout_regime_ema_window",
        "donchian_breakout_regime_adx_window",
        "donchian_breakout_regime_min_adx",
        "donchian_breakout_regime_adx_hysteresis",
        "donchian_breakout_ignore_sunday_candles",
        "donchian_breakout_session_filter_enabled",
        "donchian_breakout_session_filter_fx_only",
        "donchian_breakout_session_start_hour_utc",
        "donchian_breakout_session_end_hour_utc",
        "donchian_breakout_rsi_filter_enabled",
        "donchian_breakout_rsi_period",
        "donchian_breakout_rsi_buy_max",
        "donchian_breakout_rsi_sell_min",
        "donchian_breakout_breakout_quality_filter_enabled",
        "donchian_breakout_breakout_min_body_to_range_ratio",
        "donchian_breakout_breakout_buy_min_close_location",
        "donchian_breakout_breakout_sell_max_close_location",
        "donchian_breakout_confidence_base",
        "donchian_breakout_confidence_breakout_weight",
        "donchian_breakout_confidence_channel_weight",
        "donchian_breakout_confidence_extension_weight",
        "donchian_breakout_confidence_breakout_target_atr",
        "donchian_breakout_confidence_breakout_tolerance_atr",
        "donchian_breakout_confidence_channel_norm_atr",
    }
)
_DONCHIAN_BREAKOUT_SURFACE_MANAGED_KEYS: frozenset[str] = (
    frozenset(_DONCHIAN_BREAKOUT_LIVE_CORE_DEFAULTS)
    | _DONCHIAN_BREAKOUT_SURFACE_ALLOWED_KEYS
    | _DONCHIAN_BREAKOUT_PARAMETER_SURFACE_METADATA_KEYS
    | _DONCHIAN_BREAKOUT_SURFACE_LOCKED_ONLY_KEYS
)

_STRATEGY_PROFILES: Mapping[str, Mapping[str, Mapping[str, Any]]] = _freeze_mapping({
    "index_hybrid": _INDEX_HYBRID_PROFILES,
    "mean_breakout_v2": _MEAN_BREAKOUT_V2_PROFILES,
    "mean_reversion_bb": _MEAN_REVERSION_BB_PROFILES,
})


def _normalize_profile_name(profile_name: object, *, default: str) -> str:
    value = str(profile_name or "").strip().lower()
    if not value:
        return default
    return value


def _enforce_index_hybrid_parameter_surface(
    base_params: dict[str, Any],
    *,
    profile_name: str | None = None,
) -> tuple[dict[str, Any], bool]:
    original = dict(base_params)
    surface_mode = _normalize_profile_name(
        original.get("index_parameter_surface_mode", "tiered"),
        default="tiered",
    )
    if surface_mode not in {"tiered", "open"}:
        surface_mode = "tiered"

    requested_tier = _normalize_profile_name(
        profile_name if profile_name is not None else original.get("index_parameter_surface_tier", "conservative"),
        default="conservative",
    )
    if requested_tier not in _INDEX_HYBRID_PROFILES:
        requested_tier = "conservative"

    if surface_mode == "open":
        merged = dict(original)
        merged["index_parameter_surface_mode"] = "open"
        merged["index_parameter_surface_tier"] = requested_tier
        merged["index_parameter_surface_locked"] = False
        merged["index_parameter_surface_allowed_overrides"] = sorted(_INDEX_HYBRID_TIERED_TUNABLE_KEYS)
        merged["index_parameter_surface_ignored_overrides"] = []
        merged["index_parameter_surface_ignored_count"] = 0
        return merged, False

    tier_payload = _INDEX_HYBRID_PROFILES[requested_tier]
    merged, ignored_keys = _apply_parameter_surface(
        original,
        default_profile=tier_payload,
        managed_keys=_INDEX_HYBRID_SURFACE_MANAGED_KEYS,
        allowed_keys=_INDEX_HYBRID_TIERED_TUNABLE_KEYS | _INDEX_HYBRID_SURFACE_CONTROL_KEYS,
    )

    merged["index_zscore_ema_window"] = max(
        2,
        int(merged.get("index_zscore_ema_window", merged.get("index_slow_ema_window", 89))),
    )
    merged["index_parameter_surface_mode"] = "tiered"
    merged["index_parameter_surface_tier"] = requested_tier
    merged["index_parameter_surface_locked"] = True
    merged["index_parameter_surface_allowed_overrides"] = sorted(_INDEX_HYBRID_TIERED_TUNABLE_KEYS)
    merged["index_parameter_surface_ignored_overrides"] = ignored_keys
    merged["index_parameter_surface_ignored_count"] = len(ignored_keys)
    return merged, True


def _enforce_donchian_breakout_parameter_surface(
    base_params: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    original = dict(base_params)
    merged, ignored_keys = _apply_parameter_surface(
        original,
        default_profile=_DONCHIAN_BREAKOUT_LIVE_CORE_DEFAULTS,
        managed_keys=_DONCHIAN_BREAKOUT_SURFACE_MANAGED_KEYS,
        allowed_keys=_DONCHIAN_BREAKOUT_SURFACE_ALLOWED_KEYS,
    )
    merged["donchian_breakout_parameter_surface_locked"] = True
    merged["donchian_breakout_parameter_surface_allowed_overrides"] = sorted(
        _DONCHIAN_BREAKOUT_SURFACE_ALLOWED_KEYS
    )
    merged["donchian_breakout_parameter_surface_ignored_overrides"] = ignored_keys
    merged["donchian_breakout_parameter_surface_ignored_count"] = len(ignored_keys)
    return merged, True


def apply_strategy_profile(
    strategy_name: str,
    base_params: dict[str, Any],
    profile_name: str,
) -> tuple[dict[str, Any], bool]:
    strategy = str(strategy_name).strip().lower()
    profile = str(profile_name).strip().lower()
    merged = dict(base_params)

    profiles_for_strategy = _STRATEGY_PROFILES.get(strategy)
    if profiles_for_strategy is None:
        return merged, False

    profile_payload = profiles_for_strategy.get(profile)
    if profile_payload is None:
        raise ValueError(
            f"Unknown strategy profile '{profile_name}'. "
            f"Available for {strategy}: {', '.join(sorted(profiles_for_strategy))}"
        )
    merged.update(profile_payload)
    preserve_keys_raw = base_params.get("_strategy_profile_preserve_keys")
    preserve_keys: set[str] = set()
    if isinstance(preserve_keys_raw, (list, tuple, set)):
        preserve_keys = {str(item).strip() for item in preserve_keys_raw if str(item).strip()}
    if strategy == "index_hybrid":
        preserve_keys |= _INDEX_HYBRID_PROFILE_PRESERVE_KEYS
    if preserve_keys:
        for key in preserve_keys:
            if key in base_params:
                merged[key] = base_params[key]
    return merged, True


def enforce_strategy_parameter_surface(
    strategy_name: str,
    base_params: dict[str, Any],
    *,
    profile_name: str | None = None,
) -> tuple[dict[str, Any], bool]:
    strategy = str(strategy_name).strip().lower()
    if strategy == "index_hybrid":
        return _enforce_index_hybrid_parameter_surface(base_params, profile_name=profile_name)
    if strategy == "donchian_breakout":
        return _enforce_donchian_breakout_parameter_surface(base_params)
    return dict(base_params), False
