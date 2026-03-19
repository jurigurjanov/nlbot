from __future__ import annotations

from typing import Any


_INDEX_HYBRID_SAFE: dict[str, Any] = {
    "index_fast_ema_window": 21,
    "index_slow_ema_window": 89,
    "index_donchian_window": 14,
    "index_zscore_window": 21,
    "index_window_sync_mode": "auto",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "classic",
    "index_zscore_ema_window": 89,
    "index_zscore_threshold": 1.8,
    "index_regime_selection_mode": "fuzzy",
    "index_regime_trend_index_threshold": 0.8,
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.00035,
    "index_mean_reversion_gap_threshold": 0.00035,
    "index_atr_window": 14,
    "index_trend_atr_pct_threshold": 0.01,
    "index_mean_reversion_atr_pct_threshold": 0.03,
    "index_mean_reversion_allow_breakout": True,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_min_breakout_distance_ratio": 0.00008,
    "index_min_channel_width_atr": 0.45,
    "index_volume_confirmation": True,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.35,
    "index_volume_min_samples": 8,
    "index_volume_allow_missing": True,
    "index_volume_require_spike": False,
    "index_volume_confidence_boost": 0.08,
    "index_volume_as_bonus_only": True,
    "index_stop_loss_pips": 30.0,
    "index_take_profit_pips": 80.0,
    "index_stop_loss_pct": 0.15,
    "index_take_profit_pct": 0.45,
    "index_stop_atr_multiplier": 2.0,
    "index_risk_reward_ratio": 2.2,
    "index_session_filter_enabled": True,
    "index_trend_session_start_hour_utc": 6,
    "index_trend_session_end_hour_utc": 22,
    "index_mean_reversion_outside_trend_session": True,
}

_INDEX_HYBRID_AGGRESSIVE: dict[str, Any] = {
    "index_fast_ema_window": 13,
    "index_slow_ema_window": 55,
    "index_donchian_window": 10,
    "index_zscore_window": 15,
    "index_window_sync_mode": "auto",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "classic",
    "index_zscore_ema_window": 55,
    "index_zscore_threshold": 1.5,
    "index_regime_selection_mode": "fuzzy",
    "index_regime_trend_index_threshold": 0.75,
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.00025,
    "index_mean_reversion_gap_threshold": 0.00025,
    "index_atr_window": 14,
    "index_trend_atr_pct_threshold": 0.008,
    "index_mean_reversion_atr_pct_threshold": 0.035,
    "index_mean_reversion_allow_breakout": True,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_min_breakout_distance_ratio": 0.00005,
    "index_min_channel_width_atr": 0.3,
    "index_volume_confirmation": True,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.25,
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
    "index_session_filter_enabled": False,
    "index_trend_session_start_hour_utc": 6,
    "index_trend_session_end_hour_utc": 22,
    "index_mean_reversion_outside_trend_session": True,
}

_INDEX_HYBRID_PROFILES: dict[str, dict[str, Any]] = {
    "safe": _INDEX_HYBRID_SAFE,
    "conservative": _INDEX_HYBRID_SAFE,
    "aggressive": _INDEX_HYBRID_AGGRESSIVE,
}

_MEAN_BREAKOUT_V2_CONSERVATIVE: dict[str, Any] = {
    "mb_zscore_threshold": 1.4,
    "mb_zscore_entry_mode": "max_abs",
    "mb_min_slope_ratio": 0.00014,
    "mb_stop_loss_pips": 60.0,
    "mb_take_profit_pips": 120.0,
    "mb_risk_reward_ratio": 2.4,
    "mb_atr_window": 14,
    "mb_atr_multiplier": 1.8,
    "mb_min_stop_loss_pips": 80.0,
    "mb_min_take_profit_pips": 120.0,
    "mb_min_relative_stop_pct": 0.0016,
    "mb_dynamic_tp_only": True,
    "mb_require_context_tick_size_for_cfd": True,
    "mb_resample_mode": "auto",
    "mb_volume_confirmation": True,
    "mb_volume_window": 20,
    "mb_min_volume_ratio": 1.5,
    "mb_volume_min_samples": 8,
    "mb_volume_allow_missing": True,
    "mb_volume_require_spike": False,
    "mb_volume_confidence_boost": 0.12,
    "mb_trailing_enabled": True,
    "mb_trailing_atr_multiplier": 1.8,
    "mb_trailing_activation_stop_ratio": 1.1,
}

_MEAN_BREAKOUT_V2_AGGRESSIVE: dict[str, Any] = {
    "mb_zscore_threshold": 1.9,
    "mb_zscore_entry_mode": "max_abs",
    "mb_min_slope_ratio": 0.00008,
    "mb_stop_loss_pips": 45.0,
    "mb_take_profit_pips": 105.0,
    "mb_risk_reward_ratio": 2.7,
    "mb_atr_window": 14,
    "mb_atr_multiplier": 1.35,
    "mb_min_stop_loss_pips": 50.0,
    "mb_min_take_profit_pips": 100.0,
    "mb_min_relative_stop_pct": 0.0010,
    "mb_dynamic_tp_only": True,
    "mb_require_context_tick_size_for_cfd": True,
    "mb_resample_mode": "auto",
    "mb_volume_confirmation": True,
    "mb_volume_window": 20,
    "mb_min_volume_ratio": 1.35,
    "mb_volume_min_samples": 6,
    "mb_volume_allow_missing": True,
    "mb_volume_require_spike": False,
    "mb_volume_confidence_boost": 0.10,
    "mb_trailing_enabled": True,
    "mb_trailing_atr_multiplier": 1.45,
    "mb_trailing_activation_stop_ratio": 1.0,
}

_MEAN_BREAKOUT_V2_PROFILES: dict[str, dict[str, Any]] = {
    "safe": _MEAN_BREAKOUT_V2_CONSERVATIVE,
    "conservative": _MEAN_BREAKOUT_V2_CONSERVATIVE,
    "aggressive": _MEAN_BREAKOUT_V2_AGGRESSIVE,
}

_MEAN_REVERSION_BB_CONSERVATIVE: dict[str, Any] = {
    "mean_reversion_bb_entry_mode": "reentry",
    "mean_reversion_bb_reentry_tolerance_sigma": 0.05,
    "mean_reversion_bb_use_rsi_filter": True,
    "mean_reversion_bb_rsi_method": "wilder",
    "mean_reversion_bb_rsi_overbought": 72.0,
    "mean_reversion_bb_rsi_oversold": 28.0,
    "mean_reversion_bb_std_dev": 2.2,
    "mean_reversion_bb_min_band_extension_ratio": 0.05,
    "mean_reversion_bb_max_band_extension_ratio": 1.5,
    "mean_reversion_bb_trend_filter_enabled": True,
    "mean_reversion_bb_trend_filter_mode": "strict",
    "mean_reversion_bb_trend_filter_extreme_sigma": 2.8,
    "mean_reversion_bb_trend_slope_lookback_bars": 5,
    "mean_reversion_bb_trend_slope_strict_threshold": 0.00015,
    "mean_reversion_bb_volume_confirmation": True,
    "mean_reversion_bb_volume_window": 20,
    "mean_reversion_bb_min_volume_ratio": 1.5,
    "mean_reversion_bb_volume_min_samples": 10,
    "mean_reversion_bb_volume_allow_missing": True,
    "mean_reversion_bb_volume_require_spike": False,
    "mean_reversion_bb_volume_confidence_boost": 0.20,
    "mean_reversion_bb_take_profit_mode": "rr",
    "mean_reversion_bb_use_atr_sl_tp": True,
    "mean_reversion_bb_atr_window": 14,
    "mean_reversion_bb_atr_multiplier": 1.6,
    "mean_reversion_bb_risk_reward_ratio": 2.0,
    "mean_reversion_bb_min_stop_loss_pips": 25.0,
    "mean_reversion_bb_min_take_profit_pips": 22.0,
    "mean_reversion_bb_min_confidence_for_entry": 0.60,
    "mean_reversion_bb_signal_only_min_confidence_for_entry": 0.60,
    "mean_reversion_bb_paper_min_confidence_for_entry": 0.65,
    "mean_reversion_bb_execution_min_confidence_for_entry": 0.75,
    "mean_reversion_bb_trade_cooldown_sec": 360.0,
    "mean_reversion_bb_trade_cooldown_win_sec": 480.0,
    "mean_reversion_bb_trade_cooldown_loss_sec": 1800.0,
    "mean_reversion_bb_trade_cooldown_flat_sec": 360.0,
}

_MEAN_REVERSION_BB_AGGRESSIVE: dict[str, Any] = {
    "mean_reversion_bb_entry_mode": "reentry",
    "mean_reversion_bb_reentry_tolerance_sigma": 0.05,
    "mean_reversion_bb_use_rsi_filter": True,
    "mean_reversion_bb_rsi_method": "wilder",
    "mean_reversion_bb_rsi_overbought": 68.0,
    "mean_reversion_bb_rsi_oversold": 32.0,
    "mean_reversion_bb_std_dev": 2.1,
    "mean_reversion_bb_min_band_extension_ratio": 0.02,
    "mean_reversion_bb_max_band_extension_ratio": 2.5,
    "mean_reversion_bb_trend_filter_enabled": True,
    "mean_reversion_bb_trend_filter_mode": "strict",
    "mean_reversion_bb_trend_filter_extreme_sigma": 2.2,
    "mean_reversion_bb_trend_slope_lookback_bars": 5,
    "mean_reversion_bb_trend_slope_strict_threshold": 0.00012,
    "mean_reversion_bb_volume_confirmation": True,
    "mean_reversion_bb_volume_window": 20,
    "mean_reversion_bb_min_volume_ratio": 1.4,
    "mean_reversion_bb_volume_min_samples": 8,
    "mean_reversion_bb_volume_allow_missing": True,
    "mean_reversion_bb_volume_require_spike": False,
    "mean_reversion_bb_volume_confidence_boost": 0.18,
    "mean_reversion_bb_take_profit_mode": "rr",
    "mean_reversion_bb_use_atr_sl_tp": True,
    "mean_reversion_bb_atr_window": 14,
    "mean_reversion_bb_atr_multiplier": 1.35,
    "mean_reversion_bb_risk_reward_ratio": 1.8,
    "mean_reversion_bb_min_stop_loss_pips": 22.0,
    "mean_reversion_bb_min_take_profit_pips": 24.0,
    "mean_reversion_bb_min_confidence_for_entry": 0.45,
    "mean_reversion_bb_signal_only_min_confidence_for_entry": 0.45,
    "mean_reversion_bb_paper_min_confidence_for_entry": 0.52,
    "mean_reversion_bb_execution_min_confidence_for_entry": 0.70,
    "mean_reversion_bb_trade_cooldown_sec": 240.0,
    "mean_reversion_bb_trade_cooldown_win_sec": 300.0,
    "mean_reversion_bb_trade_cooldown_loss_sec": 1200.0,
    "mean_reversion_bb_trade_cooldown_flat_sec": 240.0,
}

_MEAN_REVERSION_BB_PROFILES: dict[str, dict[str, Any]] = {
    "safe": _MEAN_REVERSION_BB_CONSERVATIVE,
    "conservative": _MEAN_REVERSION_BB_CONSERVATIVE,
    "aggressive": _MEAN_REVERSION_BB_AGGRESSIVE,
}

_STRATEGY_PROFILES: dict[str, dict[str, dict[str, Any]]] = {
    "index_hybrid": _INDEX_HYBRID_PROFILES,
    "mean_breakout_v2": _MEAN_BREAKOUT_V2_PROFILES,
    "mean_reversion_bb": _MEAN_REVERSION_BB_PROFILES,
}


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
    return merged, True
