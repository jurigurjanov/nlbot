from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from xtb_bot.models import AccountType, RunMode


DEFAULT_SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY", "US100", "XAUUSD", "WTI"]
DEFAULT_INDEX_HYBRID_SYMBOLS = ["US500", "US100", "DE40", "UK100"]
DEFAULT_STRATEGY_PARAMS = {
    "fast_window": 8,
    "slow_window": 21,
    "momentum_ma_type": "ema",
    "momentum_entry_mode": "cross_only",
    "momentum_confirm_bars": 2,
    "momentum_low_tf_min_confirm_bars": 2,
    "momentum_low_tf_max_confirm_bars": 3,
    "momentum_high_tf_max_confirm_bars": 1,
    "momentum_auto_confirm_by_timeframe": True,
    "momentum_timeframe_sec": 60,
    "momentum_max_spread_pips": 12.0,
    "momentum_require_context_tick_size": False,
    "momentum_trade_cooldown_sec": 60.0,
    "momentum_min_confidence_for_entry": 0.55,
    "momentum_signal_only_min_confidence_for_entry": 0.55,
    "momentum_paper_min_confidence_for_entry": 0.55,
    "momentum_execution_min_confidence_for_entry": 0.65,
    "momentum_atr_window": 14,
    "momentum_atr_multiplier": 2.0,
    "momentum_risk_reward_ratio": 2.0,
    "momentum_min_stop_loss_pips": 15.0,
    "momentum_min_take_profit_pips": 30.0,
    "momentum_min_relative_stop_pct": 0.0008,
    "momentum_max_price_slow_gap_atr": 2.5,
    "momentum_pullback_entry_max_gap_atr": 2.5,
    "momentum_confirm_gap_relief_per_bar": 0.5,
    "momentum_price_gap_mode": "wait_pullback",
    "momentum_min_slope_atr_ratio": 0.05,
    "momentum_min_trend_gap_atr": 0.0,
    "momentum_session_filter_enabled": False,
    "momentum_session_start_hour_utc": 6,
    "momentum_session_end_hour_utc": 22,
    "momentum_volume_confirmation": False,
    "momentum_volume_window": 20,
    "momentum_min_volume_ratio": 1.2,
    "momentum_volume_min_samples": 5,
    "momentum_volume_allow_missing": True,
    "min_confidence_for_entry": 0.0,
    "debug_indicators": False,
    "debug_indicators_interval_sec": 0.0,
    "g1_fast_ema_window": 20,
    "g1_slow_ema_window": 50,
    "g1_adx_window": 14,
    "g1_adx_threshold": 25.0,
    "g1_adx_hysteresis": 2.0,
    "g1_atr_window": 14,
    "g1_atr_multiplier": 2.0,
    "g1_risk_reward_ratio": 3.0,
    "g1_max_spread_pips": 1.0,
    "g1_min_stop_loss_pips": 15.0,
    "g1_max_price_ema_gap_ratio": 0.005,
    "g1_candle_timeframe_sec": 60,
    "g1_candle_confirm_bars": 1,
    "g1_ignore_sunday_candles": True,
    "g1_entry_mode": "cross_or_trend",
    "g1_min_trend_gap_ratio": 0.0,
    "g1_min_cross_gap_ratio": 0.0,
    "g1_continuation_adx_multiplier": 0.55,
    "g1_continuation_min_adx": 0.0,
    "g1_adx_warmup_multiplier": 3.0,
    "g1_adx_warmup_extra_bars": 10,
    "g1_adx_warmup_cap_bars": 0,
    "g1_use_adx_hysteresis_state": True,
    "g1_index_require_context_tick_size": False,
    "g1_resample_mode": "auto",
    "g1_debug_indicators": False,
    "g1_debug_indicators_interval_sec": 0.0,
    "g1_index_low_vol_atr_pct_threshold": 0.1,
    "g1_index_low_vol_multiplier": 1.5,
    "g1_min_relative_stop_pct": 0.0008,
    "g1_volume_confirmation": False,
    "g1_volume_window": 20,
    "g1_min_volume_ratio": 1.4,
    "g1_volume_min_samples": 8,
    "g1_volume_allow_missing": True,
    "g1_volume_require_spike": False,
    "g1_volume_confidence_boost": 0.1,
    "g1_protective_exit_enabled": True,
    "g1_protective_exit_loss_ratio": 0.82,
    "g1_protective_exit_allow_adx_regime_loss": True,
    "g1_trade_cooldown_sec": 1800,
    "g1_min_confidence_for_entry": 0.60,
    "g1_signal_only_min_confidence_for_entry": 0.60,
    "g1_paper_min_confidence_for_entry": 0.60,
    "g1_execution_min_confidence_for_entry": 0.70,
    "g1_profile_override": "auto",
    "g1_fx_fast_ema_window": 20,
    "g1_fx_slow_ema_window": 50,
    "g1_fx_adx_window": 14,
    "g1_fx_adx_threshold": 25.0,
    "g1_fx_adx_hysteresis": 2.0,
    "g1_fx_atr_window": 14,
    "g1_fx_atr_multiplier": 2.0,
    "g1_fx_risk_reward_ratio": 3.0,
    "g1_fx_max_spread_pips": 1.0,
    "g1_fx_min_stop_loss_pips": 15.0,
    "g1_fx_max_price_ema_gap_ratio": 0.005,
    "g1_index_fast_ema_window": 20,
    "g1_index_slow_ema_window": 50,
    "g1_index_adx_window": 14,
    "g1_index_adx_threshold": 22.0,
    "g1_index_adx_hysteresis": 2.0,
    "g1_index_atr_window": 14,
    "g1_index_atr_multiplier": 3.0,
    "g1_index_risk_reward_ratio": 3.0,
    "g1_index_max_spread_pips": 8.0,
    "g1_index_min_stop_loss_pips": 50.0,
    "g1_index_max_price_ema_gap_ratio": 0.008,
    "fast_ema_window": 20,
    "slow_ema_window": 80,
    "donchian_window": 20,
    "use_donchian_filter": True,
    "trend_require_context_tick_size": False,
    "trend_breakout_lookback_bars": 6,
    "trend_atr_window": 14,
    "trend_pullback_max_distance_ratio": 0.003,
    "trend_pullback_max_distance_atr": 0.0,
    "trend_crypto_max_pullback_distance_atr": 1.8,
    "trend_pullback_ema_tolerance_ratio": 0.001,
    "trend_crypto_pullback_ema_tolerance_ratio": 0.0,
    "trend_crypto_min_ema_gap_ratio": 0.0012,
    "trend_crypto_min_fast_slope_ratio": 0.0002,
    "trend_crypto_min_slow_slope_ratio": 0.00005,
    "trend_crypto_min_atr_pct": 0.18,
    "trend_spread_buffer_factor": 0.0,
    "trend_max_spread_to_stop_ratio": 0.0,
    "trend_crypto_min_stop_pct": 0.0,
    "trend_max_stop_loss_atr": 0.0,
    "trend_crypto_max_stop_loss_atr": 0.0,
    "trend_max_timestamp_gap_sec": 0.0,
    "trend_pullback_bounce_required": True,
    "trend_slope_mode": "fast_with_slow_tolerance",
    "trend_slow_slope_tolerance_ratio": 0.0003,
    "trend_volume_confirmation": False,
    "trend_volume_window": 20,
    "trend_min_volume_ratio": 1.4,
    "trend_volume_min_samples": 8,
    "trend_volume_allow_missing": True,
    "trend_volume_require_spike": False,
    "trend_volume_confidence_boost": 0.1,
    "trend_risk_reward_ratio": 2.5,
    "trend_min_stop_loss_pips": 30.0,
    "trend_min_take_profit_pips": 75.0,
    "trend_index_min_stop_pct": 0.25,
    "trend_strength_norm_ratio": 0.003,
    "crypto_trend_following_min_confidence_for_entry": 0.65,
    "crypto_trend_following_signal_only_min_confidence_for_entry": 0.65,
    "crypto_trend_following_paper_min_confidence_for_entry": 0.68,
    "crypto_trend_following_execution_min_confidence_for_entry": 0.75,
    "donchian_breakout_window": 20,
    "donchian_breakout_atr_window": 14,
    "donchian_breakout_atr_multiplier": 2.0,
    "donchian_breakout_risk_reward_ratio": 2.5,
    "donchian_breakout_min_stop_loss_pips": 30.0,
    "donchian_breakout_min_take_profit_pips": 75.0,
    "donchian_breakout_min_relative_stop_pct": 0.0008,
    "donchian_breakout_min_breakout_atr_ratio": 0.15,
    "donchian_breakout_max_breakout_atr_ratio": 1.8,
    "donchian_breakout_min_channel_width_atr": 0.6,
    "donchian_breakout_volume_confirmation": False,
    "donchian_breakout_volume_window": 20,
    "donchian_breakout_min_volume_ratio": 1.2,
    "donchian_breakout_volume_min_samples": 5,
    "donchian_breakout_volume_allow_missing": True,
    "donchian_breakout_confidence_base": 0.2,
    "donchian_breakout_confidence_breakout_weight": 0.45,
    "donchian_breakout_confidence_channel_weight": 0.25,
    "donchian_breakout_confidence_extension_weight": 0.2,
    "donchian_breakout_confidence_breakout_target_atr": 0.6,
    "donchian_breakout_confidence_breakout_tolerance_atr": 0.6,
    "donchian_breakout_confidence_channel_norm_atr": 2.0,
    "index_fast_ema_window": 34,
    "index_slow_ema_window": 144,
    "index_donchian_window": 20,
    "index_atr_window": 14,
    "index_zscore_window": 30,
    "index_window_sync_mode": "auto",
    "index_zscore_donchian_ratio_target": 1.5,
    "index_zscore_donchian_ratio_tolerance": 0.2,
    "index_zscore_mode": "detrended",
    "index_zscore_ema_window": 144,
    "index_zscore_threshold": 2.2,
    "index_auto_correct_regime_thresholds": True,
    "index_require_context_tick_size": False,
    "index_mean_reversion_allow_breakout": True,
    "index_mean_reversion_breakout_extreme_multiplier": 1.0,
    "index_min_breakout_distance_ratio": 0.0,
    "index_min_channel_width_atr": 0.0,
    "index_regime_selection_mode": "hard",
    "index_regime_trend_index_threshold": 0.8,
    "index_regime_fallback_mode": "nearest",
    "index_trend_gap_threshold": 0.0006,
    "index_mean_reversion_gap_threshold": 0.0004,
    "index_trend_atr_pct_threshold": 0.08,
    "index_mean_reversion_atr_pct_threshold": 0.05,
    "index_volume_confirmation": False,
    "index_volume_window": 20,
    "index_min_volume_ratio": 1.4,
    "index_volume_min_samples": 8,
    "index_volume_allow_missing": True,
    "index_volume_require_spike": False,
    "index_volume_confidence_boost": 0.08,
    "index_volume_as_bonus_only": False,
    "index_trend_confidence_base": 0.2,
    "index_trend_confidence_gap_weight": 0.5,
    "index_trend_confidence_breakout_weight": 0.3,
    "index_trend_confidence_gap_norm": 0.0006,
    "index_trend_confidence_breakout_norm": 0.0005,
    "index_stop_loss_pct": 0.5,
    "index_take_profit_pct": 1.5,
    "index_stop_atr_multiplier": 2.0,
    "index_risk_reward_ratio": 3.0,
    "index_session_filter_enabled": True,
    "index_hybrid_min_confidence_for_entry": 0.65,
    "index_hybrid_signal_only_min_confidence_for_entry": 0.65,
    "index_hybrid_paper_min_confidence_for_entry": 0.65,
    "index_hybrid_execution_min_confidence_for_entry": 0.75,
    "index_trend_session_start_hour_utc": 6,
    "index_trend_session_end_hour_utc": 22,
    "index_mean_reversion_outside_trend_session": True,
    "mean_breakout_window": 30,
    "mean_breakout_breakout_window": 20,
    "mean_breakout_zscore_threshold": 2.0,
    "mb_zscore_window": 40,
    "mb_breakout_window": 20,
    "mb_slope_window": 5,
    "mb_zscore_threshold": 1.6,
    "mb_zscore_entry_mode": "max_abs",
    "mb_min_slope_ratio": 0.0001,
    "mb_exit_z_level": 0.5,
    "mb_stop_loss_pips": 50.0,
    "mb_take_profit_pips": 120.0,
    "mb_risk_reward_ratio": 2.5,
    "mb_atr_window": 14,
    "mb_atr_multiplier": 1.5,
    "mb_min_stop_loss_pips": 60.0,
    "mb_min_take_profit_pips": 120.0,
    "mb_min_relative_stop_pct": 0.0012,
    "mb_dynamic_tp_only": True,
    "mb_require_context_tick_size_for_cfd": True,
    "mb_timeframe_sec": 300.0,
    "mb_candle_timeframe_sec": 300.0,
    "mb_resample_mode": "auto",
    "mb_ignore_sunday_candles": True,
    "mb_m5_max_sec": 300.0,
    "mb_m15_max_sec": 900.0,
    "mb_sl_mult_m5": 1.0,
    "mb_tp_mult_m5": 1.0,
    "mb_sl_mult_m15": 1.5,
    "mb_tp_mult_m15": 1.8,
    "mb_sl_mult_h1": 3.0,
    "mb_tp_mult_h1": 4.0,
    "mb_trailing_enabled": True,
    "mb_trailing_atr_multiplier": 1.5,
    "mb_trailing_activation_stop_ratio": 1.0,
    "mb_trailing_min_activation_pips": 0.0,
    "mb_volume_confirmation": False,
    "mb_volume_window": 20,
    "mb_min_volume_ratio": 1.5,
    "mb_volume_min_samples": 8,
    "mb_volume_allow_missing": True,
    "mb_volume_require_spike": False,
    "mb_volume_confidence_boost": 0.1,
    "mean_breakout_v2_same_side_reentry_win_cooldown_sec": 0.0,
    "mean_breakout_v2_same_side_reentry_reset_on_opposite_signal": True,
    "mean_breakout_session_timezone": "Europe/Kyiv",
    "mean_breakout_session_start": "09:00",
    "mean_breakout_session_box_minutes": 30,
    "mean_breakout_session_trade_window_minutes": 360,
    "mean_breakout_session_one_trade_per_session": True,
    "mean_reversion_bb_window": 20,
    "mean_reversion_bb_std_dev": 2.2,
    "mean_reversion_bb_entry_mode": "reentry",
    "mean_reversion_bb_reentry_tolerance_sigma": 0.05,
    "mean_reversion_bb_use_rsi_filter": True,
    "mean_reversion_bb_rsi_period": 14,
    "mean_reversion_bb_rsi_history_multiplier": 3.0,
    "mean_reversion_bb_rsi_method": "wilder",
    "mean_reversion_bb_rsi_overbought": 70.0,
    "mean_reversion_bb_rsi_oversold": 30.0,
    "mean_reversion_bb_min_std_ratio": 0.00005,
    "mean_reversion_bb_min_band_extension_ratio": 0.03,
    "mean_reversion_bb_max_band_extension_ratio": 2.0,
    "mean_reversion_bb_trend_filter_enabled": True,
    "mean_reversion_bb_trend_ma_window": 100,
    "mean_reversion_bb_trend_filter_mode": "strict",
    "mean_reversion_bb_trend_filter_extreme_sigma": 2.5,
    "mean_reversion_bb_trend_slope_lookback_bars": 5,
    "mean_reversion_bb_trend_slope_strict_threshold": 0.00015,
    "mean_reversion_bb_volume_confirmation": True,
    "mean_reversion_bb_volume_window": 20,
    "mean_reversion_bb_min_volume_ratio": 1.5,
    "mean_reversion_bb_volume_min_samples": 10,
    "mean_reversion_bb_volume_allow_missing": True,
    "mean_reversion_bb_volume_require_spike": False,
    "mean_reversion_bb_volume_confidence_boost": 0.20,
    "mean_reversion_bb_reentry_base_confidence": 0.70,
    "mean_reversion_bb_reentry_rsi_bonus": 0.10,
    "mean_reversion_bb_reentry_extension_confidence_weight": 0.15,
    "mean_reversion_bb_exit_on_midline": True,
    "mean_reversion_bb_exit_midline_tolerance_sigma": 0.15,
    "mean_reversion_bb_use_atr_sl_tp": False,
    "mean_reversion_bb_atr_window": 14,
    "mean_reversion_bb_atr_multiplier": 1.5,
    "mean_reversion_bb_take_profit_mode": "rr",
    "mean_reversion_bb_min_confidence_for_entry": 0.60,
    "mean_reversion_bb_signal_only_min_confidence_for_entry": 0.60,
    "mean_reversion_bb_paper_min_confidence_for_entry": 0.65,
    "mean_reversion_bb_execution_min_confidence_for_entry": 0.75,
    "mean_reversion_bb_trade_cooldown_sec": 300.0,
    "mean_reversion_bb_trade_cooldown_win_sec": 420.0,
    "mean_reversion_bb_trade_cooldown_loss_sec": 1800.0,
    "mean_reversion_bb_trade_cooldown_flat_sec": 300.0,
    "mean_reversion_bb_risk_reward_ratio": 2.0,
    "mean_reversion_bb_min_stop_loss_pips": 25.0,
    "mean_reversion_bb_min_take_profit_pips": 30.0,
    "mean_reversion_zscore_window": 20,
    "mean_reversion_zscore_threshold": 1.8,
    "mean_reversion_exit_zscore": 0.15,
    "mean_reversion_trend_filter_enabled": True,
    "mean_reversion_trend_ma_window": 200,
    "mean_reversion_use_atr_sl_tp": True,
    "mean_reversion_atr_window": 14,
    "mean_reversion_atr_multiplier": 2.0,
    "mean_reversion_risk_reward_ratio": 1.8,
    "mean_reversion_min_stop_loss_pips": 20.0,
    "mean_reversion_min_take_profit_pips": 30.0,
    "zscore_window": 20,
    "zscore_threshold": 1.8,
    "stop_loss_pips": 25,
    "take_profit_pips": 50,
    "early_loss_exit_enabled": False,
    "early_loss_exit_grace_sec": 30.0,
    "early_loss_exit_loss_threshold": 0.3,
    "early_loss_exit_decay_sec": 300.0,
    "early_loss_exit_min_ratio": 0.5,
    "early_loss_exit_velocity_pips_sec": 0.0,
    "early_loss_exit_velocity_window_sec": 10.0,
}


def _normalize_mode_value(mode: RunMode | str | None) -> str | None:
    if mode is None:
        return None
    raw = getattr(mode, "value", mode)
    text = str(raw).strip().lower()
    return text or None


_STRATEGY_PARAM_ALIASES: dict[str, str] = {
    "momentum_index": "momentum",
    "momentum_fx": "momentum",
    "crypto_trend_following": "trend_following",
}


def resolve_strategy_param(
    params: dict[str, Any],
    strategy_name: str,
    suffix: str,
    default: Any,
    mode: RunMode | str | None = None,
) -> Any:
    strategy_key = str(strategy_name or "").strip().lower()
    mode_key = _normalize_mode_value(mode)
    strategy_candidates: list[str] = []
    if strategy_key:
        strategy_candidates.append(strategy_key)
        alias = _STRATEGY_PARAM_ALIASES.get(strategy_key)
        if alias and alias not in strategy_candidates:
            strategy_candidates.append(alias)

    candidates: list[str] = []
    if mode_key:
        for key in strategy_candidates:
            candidates.append(f"{key}_{mode_key}_{suffix}")
    if mode_key:
        candidates.append(f"{mode_key}_{suffix}")
    for key in strategy_candidates:
        candidates.append(f"{key}_{suffix}")
    candidates.append(suffix)

    for key in candidates:
        if key in params:
            return params[key]
    return default


@dataclass(slots=True)
class RiskConfig:
    start_balance: float = 10_000.0
    max_risk_per_trade_pct: float = 1.0
    max_daily_drawdown_pct: float = 5.0
    max_total_drawdown_pct: float = 12.0
    max_open_positions: int = 5
    open_slot_lease_sec: float = 30.0
    min_stop_loss_pips: float = 10.0
    min_tp_sl_ratio: float = 2.0
    trailing_activation_ratio: float = 0.0
    trailing_distance_pips: float = 10.0
    trailing_breakeven_offset_pips: float = 0.0
    trailing_breakeven_offset_pips_fx: float = 0.0
    trailing_breakeven_offset_pips_index: float = 0.0
    trailing_breakeven_offset_pips_commodity: float = 0.0
    session_close_buffer_min: int = 15
    news_event_buffer_min: int = 5
    news_filter_enabled: bool = True
    news_event_action: str = "breakeven"
    spread_filter_enabled: bool = True
    spread_anomaly_multiplier: float = 3.0
    spread_avg_window: int = 50
    spread_min_samples: int = 20
    spread_pct_filter_enabled: bool = True
    spread_max_pct: float = 0.0
    spread_max_pct_cfd: float = 0.20
    spread_max_pct_crypto: float = 0.50
    margin_check_enabled: bool = True
    margin_safety_buffer: float = 1.15
    margin_fallback_leverage: float = 20.0
    margin_overhead_pct: float = 0.0
    margin_weekend_multiplier: float = 1.0
    margin_weekend_start_hour_utc: int = 20
    margin_holiday_multiplier: float = 1.0
    margin_holiday_dates_utc: tuple[str, ...] = ()
    margin_commission_per_lot: float = 0.0
    margin_min_free_after_open: float = 0.0
    margin_min_free_after_open_pct: float = 0.0
    external_cashflow_rebase_enabled: bool = False
    external_cashflow_rebase_min_abs: float = 500.0
    external_cashflow_rebase_min_pct: float = 8.0
    connectivity_check_enabled: bool = True
    connectivity_max_latency_ms: float = 500.0
    connectivity_pong_timeout_sec: float = 2.0
    stream_health_check_enabled: bool = True
    stream_max_tick_age_sec: float = 15.0
    stream_event_cooldown_sec: float = 60.0
    hold_reason_log_interval_sec: float = 60.0
    worker_state_flush_interval_sec: float = 5.0
    account_snapshot_persist_interval_sec: float = 10.0
    symbol_auto_disable_on_epic_unavailable: bool = True
    symbol_auto_disable_epic_unavailable_threshold: int = 3
    emergency_cooldown_sec: int = 300


@dataclass(slots=True)
class BotConfig:
    user_id: str
    password: str
    app_name: str
    account_type: AccountType
    mode: RunMode
    symbols: list[str]
    strategy: str
    force_strategy: bool = False
    force_symbols: bool = False
    strategy_params: dict[str, Any] = field(default_factory=lambda: dict(DEFAULT_STRATEGY_PARAMS))
    poll_interval_sec: float = 5.0
    default_volume: float = 0.0
    storage_path: Path = Path("./state/xtb_bot.db")
    endpoint: str | None = None
    broker: str = "ig"
    api_key: str | None = None
    account_id: str | None = None
    symbol_epics: dict[str, str] = field(default_factory=dict)
    ig_stream_enabled: bool = True
    ig_stream_tick_max_age_sec: float = 15.0
    ig_rest_market_min_interval_sec: float = 2.0
    strict_broker_connect: bool = False
    worker_poll_jitter_sec: float = 1.0
    passive_history_poll_interval_sec: float = 10.0
    price_history_keep_rows_min: int = 1000
    bot_magic_prefix: str = "XTBBOT"
    bot_magic_instance: str | None = None
    strategy_params_map: dict[str, dict[str, Any]] = field(default_factory=dict)
    strategy_schedule: list["StrategyScheduleEntry"] = field(default_factory=list)
    strategy_schedule_timezone: str = "UTC"
    risk: RiskConfig = field(default_factory=RiskConfig)


@dataclass(frozen=True)
class StrategyScheduleEntry:
    strategy: str
    symbols: list[str]
    start_time: str
    end_time: str
    weekdays: tuple[int, ...] = (0, 1, 2, 3, 4)
    priority: int = 0
    strategy_params: dict[str, Any] = field(default_factory=dict)
    label: str | None = None
    start_minute: int = 0
    end_minute: int = 0


class ConfigError(RuntimeError):
    pass


def _parse_strategy_params_object(raw: Any, source_name: str) -> dict[str, Any]:
    if raw is None:
        return {}
    payload = raw
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{source_name} must be valid JSON object: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"{source_name} must be a JSON object")
    return dict(payload)


def _strategy_env_suffix(strategy: str) -> str:
    raw = "".join(ch if ch.isalnum() else "_" for ch in strategy.strip().upper())
    parts = [part for part in raw.split("_") if part]
    return "_".join(parts) or "DEFAULT"


def _select_strategy_preset(raw: Any, strategy: str, source_name: str) -> dict[str, Any]:
    presets = _parse_strategy_params_object(raw, source_name)
    if not presets:
        return {}

    candidates = [strategy, strategy.lower(), strategy.upper()]
    for key in candidates:
        if key not in presets:
            continue
        value = presets.get(key)
        if not isinstance(value, dict):
            raise ConfigError(f"{source_name}.{key} must be a JSON object")
        return dict(value)

    default_value = presets.get("default")
    if default_value is None:
        return {}
    if not isinstance(default_value, dict):
        raise ConfigError(f"{source_name}.default must be a JSON object")
    return dict(default_value)


def _merge_strategy_params_layers(
    *,
    strategy: str,
    global_raw: Any,
    presets_raw: Any,
    strategy_specific_raw: Any,
    global_source_name: str,
    presets_source_name: str,
    strategy_specific_source_name: str,
) -> dict[str, Any]:
    params = dict(DEFAULT_STRATEGY_PARAMS)
    params.update(_parse_strategy_params_object(global_raw, global_source_name))
    params.update(_select_strategy_preset(presets_raw, strategy, presets_source_name))
    params.update(_parse_strategy_params_object(strategy_specific_raw, strategy_specific_source_name))
    return params


def _load_strategy_params_for_name(raw_file: dict[str, Any], broker: str, strategy: str) -> dict[str, Any]:
    strategy_key = str(strategy).strip().lower()
    strategy_suffix = _strategy_env_suffix(strategy_key)
    if broker == "ig":
        return _merge_strategy_params_layers(
            strategy=strategy_key,
            global_raw=_resolve_dual_env(
                raw_file,
                "IG_STRATEGY_PARAMS",
                "XTB_STRATEGY_PARAMS",
                "strategy_params",
                None,
            ),
            presets_raw=_resolve_dual_env(
                raw_file,
                "IG_STRATEGY_PRESETS",
                "XTB_STRATEGY_PRESETS",
                "strategy_presets",
                None,
            ),
            strategy_specific_raw=_resolve_dual_env(
                raw_file,
                f"IG_STRATEGY_PARAMS_{strategy_suffix}",
                f"XTB_STRATEGY_PARAMS_{strategy_suffix}",
                f"strategy_params_{strategy_key}",
                None,
            ),
            global_source_name="IG_STRATEGY_PARAMS/XTB_STRATEGY_PARAMS",
            presets_source_name="IG_STRATEGY_PRESETS/XTB_STRATEGY_PRESETS",
            strategy_specific_source_name=(
                f"IG_STRATEGY_PARAMS_{strategy_suffix}/XTB_STRATEGY_PARAMS_{strategy_suffix}"
            ),
        )
    return _merge_strategy_params_layers(
        strategy=strategy_key,
        global_raw=_resolve(raw_file, "XTB_STRATEGY_PARAMS", "strategy_params", None),
        presets_raw=_resolve(raw_file, "XTB_STRATEGY_PRESETS", "strategy_presets", None),
        strategy_specific_raw=_resolve(
            raw_file,
            f"XTB_STRATEGY_PARAMS_{strategy_suffix}",
            f"strategy_params_{strategy_key}",
            None,
        ),
        global_source_name="XTB_STRATEGY_PARAMS",
        presets_source_name="XTB_STRATEGY_PRESETS",
        strategy_specific_source_name=f"XTB_STRATEGY_PARAMS_{strategy_suffix}",
    )


def _parse_time_hhmm(value: Any, field_name: str) -> tuple[str, int]:
    text = str(value or "").strip()
    parts = text.split(":", 1)
    if len(parts) != 2:
        raise ConfigError(f"{field_name} must be in HH:MM format")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as exc:
        raise ConfigError(f"{field_name} must be in HH:MM format") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ConfigError(f"{field_name} must be a valid time in HH:MM format")
    return f"{hour:02d}:{minute:02d}", hour * 60 + minute


def _parse_schedule_weekdays(value: Any) -> tuple[int, ...]:
    if value in (None, "", []):
        return (0, 1, 2, 3, 4)
    if isinstance(value, str):
        tokens = [item.strip().lower() for item in value.split(",") if item.strip()]
    elif isinstance(value, list):
        tokens = [str(item).strip().lower() for item in value if str(item).strip()]
    else:
        raise ConfigError("strategy_schedule weekdays must be a list or comma-separated string")

    mapping = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    result: list[int] = []
    seen: set[int] = set()

    def _add(day: int) -> None:
        if day not in seen:
            seen.add(day)
            result.append(day)

    for token in tokens:
        if token in {"weekday", "weekdays"}:
            for day in range(5):
                _add(day)
            continue
        if token in {"weekend", "weekends"}:
            for day in (5, 6):
                _add(day)
            continue
        if token in {"all", "daily", "everyday"}:
            for day in range(7):
                _add(day)
            continue
        if "-" in token:
            left, right = token.split("-", 1)
            if left not in mapping or right not in mapping:
                raise ConfigError(f"Invalid strategy_schedule weekday token: {token}")
            start = mapping[left]
            end = mapping[right]
            day = start
            while True:
                _add(day)
                if day == end:
                    break
                day = (day + 1) % 7
            continue
        if token not in mapping:
            raise ConfigError(f"Invalid strategy_schedule weekday token: {token}")
        _add(mapping[token])

    if not result:
        raise ConfigError("strategy_schedule weekdays cannot be empty")
    return tuple(result)


def _parse_strategy_schedule(
    raw_file: dict[str, Any],
    broker: str,
) -> tuple[list[StrategyScheduleEntry], str]:
    if broker == "ig":
        raw = _resolve_dual_env(
            raw_file,
            "IG_STRATEGY_SCHEDULE",
            "XTB_STRATEGY_SCHEDULE",
            "strategy_schedule",
            None,
        )
        timezone_raw = _resolve_dual_env(
            raw_file,
            "IG_STRATEGY_SCHEDULE_TIMEZONE",
            "XTB_STRATEGY_SCHEDULE_TIMEZONE",
            "strategy_schedule_timezone",
            None,
        )
    else:
        raw = _resolve(raw_file, "XTB_STRATEGY_SCHEDULE", "strategy_schedule", None)
        timezone_raw = _resolve(
            raw_file,
            "XTB_STRATEGY_SCHEDULE_TIMEZONE",
            "strategy_schedule_timezone",
            None,
        )

    if raw in (None, ""):
        timezone_name = str(timezone_raw or "UTC").strip() or "UTC"
        try:
            ZoneInfo(timezone_name)
        except Exception as exc:
            raise ConfigError(f"Invalid strategy_schedule timezone: {timezone_name}") from exc
        return [], timezone_name

    payload = raw
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"strategy_schedule must be valid JSON: {exc.msg}") from exc

    if isinstance(payload, dict):
        slots_raw = payload.get("slots")
        timezone_name = str(payload.get("timezone") or timezone_raw or "UTC").strip() or "UTC"
    elif isinstance(payload, list):
        slots_raw = payload
        timezone_name = str(timezone_raw or "UTC").strip() or "UTC"
    else:
        raise ConfigError("strategy_schedule must be a JSON array or object with slots")

    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise ConfigError(f"Invalid strategy_schedule timezone: {timezone_name}") from exc

    if not isinstance(slots_raw, list):
        raise ConfigError("strategy_schedule.slots must be a JSON array")

    from xtb_bot.strategies import available_strategies

    allowed_strategies = set(available_strategies())
    entries: list[StrategyScheduleEntry] = []
    for index, item in enumerate(slots_raw):
        if not isinstance(item, dict):
            raise ConfigError(f"strategy_schedule[{index}] must be a JSON object")
        strategy = str(item.get("strategy") or "").strip().lower()
        if strategy not in allowed_strategies:
            allowed = ", ".join(sorted(allowed_strategies))
            raise ConfigError(f"strategy_schedule[{index}].strategy must be one of: {allowed}")
        start_time, start_minute = _parse_time_hhmm(
            item.get("start") if item.get("start") is not None else item.get("start_time"),
            f"strategy_schedule[{index}].start",
        )
        end_time, end_minute = _parse_time_hhmm(
            item.get("end") if item.get("end") is not None else item.get("end_time"),
            f"strategy_schedule[{index}].end",
        )
        if start_minute == end_minute:
            raise ConfigError(f"strategy_schedule[{index}] start and end cannot be equal")
        symbols_raw = item.get("symbols")
        symbols = _resolve_symbols_for_strategy(raw_file, strategy) if symbols_raw in (None, "", []) else _as_symbols(symbols_raw)
        if not symbols:
            raise ConfigError(f"strategy_schedule[{index}].symbols cannot be empty")
        weekdays = _parse_schedule_weekdays(item.get("weekdays", item.get("days")))
        priority = int(item.get("priority") or 0)
        params = _parse_strategy_params_object(
            item.get("strategy_params", item.get("params")),
            f"strategy_schedule[{index}].strategy_params",
        )
        label_raw = item.get("label")
        label = str(label_raw).strip() if label_raw not in (None, "") else None
        entries.append(
            StrategyScheduleEntry(
                strategy=strategy,
                symbols=symbols,
                start_time=start_time,
                end_time=end_time,
                weekdays=weekdays,
                priority=priority,
                strategy_params=params,
                label=label,
                start_minute=start_minute,
                end_minute=end_minute,
            )
        )

    return entries, timezone_name



def _as_float(value: Any, default: float) -> float:
    if value is None:
        return default
    return float(value)



def _as_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigError(f"Invalid boolean value: {value}")



def _as_symbols(value: Any) -> list[str]:
    if value is None:
        return list(DEFAULT_SYMBOLS)
    if isinstance(value, list):
        return [str(item).strip().upper() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [item.strip().upper() for item in value.split(",") if item.strip()]
    raise ConfigError("symbols must be a list or comma-separated string")



def _resolve(raw_file: dict[str, Any], env_key: str, file_key: str, default: Any = None) -> Any:
    env_val = os.getenv(env_key)
    if env_val not in (None, ""):
        return env_val
    file_val = raw_file.get(file_key)
    if file_val is not None:
        return file_val
    return default


def _resolve_dual_env(
    raw_file: dict[str, Any],
    primary_env_key: str,
    secondary_env_key: str,
    file_key: str,
    default: Any = None,
) -> Any:
    primary = os.getenv(primary_env_key)
    if primary not in (None, ""):
        return primary
    secondary = os.getenv(secondary_env_key)
    if secondary not in (None, ""):
        return secondary
    file_val = raw_file.get(file_key)
    if file_val is not None:
        return file_val
    return default


def _resolve_symbols_for_strategy(raw_file: dict[str, Any], strategy: str) -> list[str]:
    strategy_name = str(strategy).strip().lower()
    strategy_suffix = _strategy_env_suffix(strategy_name)

    # Preferred strategy-scoped keys for any strategy:
    # IG_SYMBOLS_<STRATEGY> / XTB_SYMBOLS_<STRATEGY>
    scoped = _resolve_dual_env(
        raw_file,
        f"IG_SYMBOLS_{strategy_suffix}",
        f"XTB_SYMBOLS_{strategy_suffix}",
        f"{strategy_name}_symbols",
        None,
    )
    if scoped not in (None, ""):
        return _as_symbols(scoped)

    if strategy_name == "index_hybrid":
        # Backward compatibility: legacy index_hybrid-only keys.
        scoped = _resolve_dual_env(
            raw_file,
            "IG_INDEX_HYBRID_SYMBOLS",
            "XTB_INDEX_HYBRID_SYMBOLS",
            "index_hybrid_symbols",
            None,
        )
        if scoped not in (None, ""):
            return _as_symbols(scoped)

        generic = _resolve_dual_env(raw_file, "IG_SYMBOLS", "XTB_SYMBOLS", "symbols", None)
        if generic not in (None, ""):
            return _as_symbols(generic)
        return list(DEFAULT_INDEX_HYBRID_SYMBOLS)

    return _as_symbols(_resolve_dual_env(raw_file, "IG_SYMBOLS", "XTB_SYMBOLS", "symbols", None))


def _as_mapping(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    payload = value
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return {}
        payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ConfigError("symbol_epics must be a JSON object")
    result: dict[str, str] = {}
    for key, val in payload.items():
        key_text = str(key).strip().upper()
        val_text = str(val).strip()
        if key_text and val_text:
            result[key_text] = val_text
    return result


def _as_csv_list(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value).replace(";", ",").split(",")
    result: list[str] = []
    for item in raw_items:
        token = str(item).strip()
        if not token:
            continue
        if token not in result:
            result.append(token)
    return tuple(result)


def _valid_magic_part(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", "-"} for ch in value)



def load_config(
    config_path: str | None = None,
    mode_override: str | None = None,
    strategy_override: str | None = None,
    symbols_override: list[str] | None = None,
    force_strategy_override: bool | None = None,
    force_symbols_override: bool | None = None,
    ig_stream_enabled_override: bool | None = None,
    strict_broker_connect_override: bool | None = None,
) -> BotConfig:
    if config_path not in (None, ""):
        raise ConfigError(
            "JSON config files are no longer supported. "
            "Use .env / environment variables as the single configuration source."
        )
    raw_file: dict[str, Any] = {}

    broker_raw = (
        os.getenv("BROKER")
        or os.getenv("IG_BROKER")
        or os.getenv("XTB_BROKER")
        or raw_file.get("broker")
        or "ig"
    )
    broker = str(broker_raw).strip().lower()
    if broker not in {"xtb", "ig"}:
        raise ConfigError("broker must be one of: ig, xtb")

    if broker == "ig":
        user_id = _resolve_dual_env(raw_file, "IG_IDENTIFIER", "XTB_USER_ID", "user_id", "")
        password = _resolve_dual_env(raw_file, "IG_PASSWORD", "XTB_PASSWORD", "password", "")
        app_name = _resolve_dual_env(raw_file, "IG_APP_NAME", "XTB_APP_NAME", "app_name", "ig-bot")
        api_key = _resolve(raw_file, "IG_API_KEY", "api_key", "")
        account_id_raw = _resolve(raw_file, "IG_ACCOUNT_ID", "account_id", None)
        account_id = str(account_id_raw).strip() if account_id_raw not in (None, "") else None
        endpoint = _resolve_dual_env(raw_file, "IG_ENDPOINT", "XTB_ENDPOINT", "endpoint", None)
        symbol_epics = _as_mapping(
            _resolve(raw_file, "IG_SYMBOL_EPICS", "symbol_epics", raw_file.get("ig_symbol_epics"))
        )
        if ig_stream_enabled_override is None:
            ig_stream_enabled = _as_bool(
                _resolve(raw_file, "IG_STREAM_ENABLED", "ig_stream_enabled", True),
                True,
            )
        else:
            ig_stream_enabled = bool(ig_stream_enabled_override)
        if not user_id or not password or not api_key:
            raise ConfigError(
                "IG credentials are required (IG_IDENTIFIER, IG_PASSWORD, IG_API_KEY)"
            )
        account_type_value = _resolve_dual_env(raw_file, "IG_ACCOUNT_TYPE", "XTB_ACCOUNT_TYPE", "account_type", "demo")
        mode_value = mode_override or _resolve_dual_env(raw_file, "IG_MODE", "XTB_MODE", "mode", "paper")
        strategy_value = strategy_override or _resolve_dual_env(
            raw_file,
            "IG_STRATEGY",
            "XTB_STRATEGY",
            "strategy",
            "momentum",
        )
        strategy_key = str(strategy_value).strip().lower()
    else:
        user_id = _resolve(raw_file, "XTB_USER_ID", "user_id", "")
        password = _resolve(raw_file, "XTB_PASSWORD", "password", "")
        app_name = _resolve(raw_file, "XTB_APP_NAME", "app_name", "xtb-bot")
        api_key = None
        account_id = None
        endpoint = _resolve(raw_file, "XTB_ENDPOINT", "endpoint", None)
        symbol_epics: dict[str, str] = {}
        ig_stream_enabled = True
        if not user_id or not password:
            raise ConfigError("XTB credentials are required (XTB_USER_ID, XTB_PASSWORD)")

        account_type_value = _resolve(raw_file, "XTB_ACCOUNT_TYPE", "account_type", "demo")
        mode_value = mode_override or _resolve(raw_file, "XTB_MODE", "mode", "paper")
        strategy_value = strategy_override or _resolve(raw_file, "XTB_STRATEGY", "strategy", "momentum")
        strategy_key = str(strategy_value).strip().lower()

    schedule_entries_raw, strategy_schedule_timezone = _parse_strategy_schedule(raw_file, broker)
    force_strategy = (
        bool(force_strategy_override)
        if force_strategy_override is not None
        else _as_bool(
            os.getenv("BOT_FORCE_STRATEGY")
            if os.getenv("BOT_FORCE_STRATEGY") is not None
            else _resolve_dual_env(
                raw_file,
                "IG_FORCE_STRATEGY",
                "XTB_FORCE_STRATEGY",
                "force_strategy",
                False,
            ),
            False,
        )
    )
    force_symbols = (
        bool(force_symbols_override)
        if force_symbols_override is not None
        else _as_bool(
            os.getenv("BOT_FORCE_SYMBOLS")
            if os.getenv("BOT_FORCE_SYMBOLS") is not None
            else _resolve_dual_env(
                raw_file,
                "IG_FORCE_SYMBOLS",
                "XTB_FORCE_SYMBOLS",
                "force_symbols",
                False,
            ),
            False,
        )
    )
    strategy_names = {strategy_key, *(entry.strategy for entry in schedule_entries_raw)}
    strategy_params_map = {
        name: _load_strategy_params_for_name(raw_file, broker, name)
        for name in sorted(strategy_names)
    }
    strategy_params = dict(strategy_params_map.get(strategy_key) or dict(DEFAULT_STRATEGY_PARAMS))
    strategy_schedule = [
        StrategyScheduleEntry(
            strategy=entry.strategy,
            symbols=list(entry.symbols),
            start_time=entry.start_time,
            end_time=entry.end_time,
            weekdays=tuple(entry.weekdays),
            priority=entry.priority,
            strategy_params={
                **dict(strategy_params_map.get(entry.strategy) or dict(DEFAULT_STRATEGY_PARAMS)),
                **dict(entry.strategy_params),
            },
            label=entry.label,
            start_minute=entry.start_minute,
            end_minute=entry.end_minute,
        )
        for entry in schedule_entries_raw
    ]

    risk_raw = raw_file.get("risk", {}) if isinstance(raw_file.get("risk"), dict) else {}

    if strict_broker_connect_override is None:
        strict_raw = (
            os.getenv("BOT_STRICT_BROKER_CONNECT")
            or os.getenv("IG_STRICT_BROKER_CONNECT")
            or os.getenv("XTB_STRICT_BROKER_CONNECT")
        )
        strict_broker_connect = _as_bool(strict_raw, False)
    else:
        strict_broker_connect = bool(strict_broker_connect_override)

    trailing_breakeven_offset_pips = _as_float(
        _resolve(
            risk_raw,
            "XTB_TRAILING_BREAKEVEN_OFFSET_PIPS",
            "trailing_breakeven_offset_pips",
            0.0,
        ),
        0.0,
    )
    trailing_breakeven_offset_pips_fx = _as_float(
        _resolve(
            risk_raw,
            "XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_FX",
            "trailing_breakeven_offset_pips_fx",
            trailing_breakeven_offset_pips,
        ),
        trailing_breakeven_offset_pips,
    )
    trailing_breakeven_offset_pips_index = _as_float(
        _resolve(
            risk_raw,
            "XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_INDEX",
            "trailing_breakeven_offset_pips_index",
            trailing_breakeven_offset_pips,
        ),
        trailing_breakeven_offset_pips,
    )
    trailing_breakeven_offset_pips_commodity = _as_float(
        _resolve(
            risk_raw,
            "XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_COMMODITY",
            "trailing_breakeven_offset_pips_commodity",
            trailing_breakeven_offset_pips,
        ),
        trailing_breakeven_offset_pips,
    )

    risk = RiskConfig(
        start_balance=_as_float(
            _resolve(risk_raw, "XTB_START_BALANCE", "start_balance", 10_000.0),
            10_000.0,
        ),
        max_risk_per_trade_pct=_as_float(
            _resolve(risk_raw, "XTB_MAX_RISK_PER_TRADE_PCT", "max_risk_per_trade_pct", 1.0),
            1.0,
        ),
        max_daily_drawdown_pct=_as_float(
            _resolve(risk_raw, "XTB_MAX_DAILY_DRAWDOWN_PCT", "max_daily_drawdown_pct", 5.0),
            5.0,
        ),
        max_total_drawdown_pct=_as_float(
            _resolve(risk_raw, "XTB_MAX_TOTAL_DRAWDOWN_PCT", "max_total_drawdown_pct", 12.0),
            12.0,
        ),
        max_open_positions=_as_int(
            _resolve(risk_raw, "XTB_MAX_OPEN_POSITIONS", "max_open_positions", 5),
            5,
        ),
        open_slot_lease_sec=_as_float(
            _resolve(risk_raw, "XTB_OPEN_SLOT_LEASE_SEC", "open_slot_lease_sec", 30.0),
            30.0,
        ),
        min_stop_loss_pips=_as_float(
            _resolve(risk_raw, "XTB_MIN_STOP_LOSS_PIPS", "min_stop_loss_pips", 10.0),
            10.0,
        ),
        min_tp_sl_ratio=_as_float(
            _resolve(risk_raw, "XTB_MIN_TP_SL_RATIO", "min_tp_sl_ratio", 2.0),
            2.0,
        ),
        trailing_activation_ratio=_as_float(
            _resolve(
                risk_raw,
                "XTB_TRAILING_ACTIVATION_RATIO",
                "trailing_activation_ratio",
                0.0,
            ),
            0.0,
        ),
        trailing_distance_pips=_as_float(
            _resolve(
                risk_raw,
                "XTB_TRAILING_DISTANCE_PIPS",
                "trailing_distance_pips",
                10.0,
            ),
            10.0,
        ),
        trailing_breakeven_offset_pips=trailing_breakeven_offset_pips,
        trailing_breakeven_offset_pips_fx=trailing_breakeven_offset_pips_fx,
        trailing_breakeven_offset_pips_index=trailing_breakeven_offset_pips_index,
        trailing_breakeven_offset_pips_commodity=trailing_breakeven_offset_pips_commodity,
        session_close_buffer_min=_as_int(
            _resolve(
                risk_raw,
                "XTB_SESSION_CLOSE_BUFFER_MIN",
                "session_close_buffer_min",
                15,
            ),
            15,
        ),
        news_event_buffer_min=_as_int(
            _resolve(
                risk_raw,
                "XTB_NEWS_EVENT_BUFFER_MIN",
                "news_event_buffer_min",
                5,
            ),
            5,
        ),
        news_filter_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_NEWS_FILTER_ENABLED",
                "news_filter_enabled",
                True,
            ),
            True,
        ),
        news_event_action=str(
            _resolve(
                risk_raw,
                "XTB_NEWS_EVENT_ACTION",
                "news_event_action",
                "breakeven",
            )
        ).strip().lower(),
        spread_filter_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_SPREAD_FILTER_ENABLED",
                "spread_filter_enabled",
                True,
            ),
            True,
        ),
        spread_anomaly_multiplier=_as_float(
            _resolve(
                risk_raw,
                "XTB_SPREAD_ANOMALY_MULTIPLIER",
                "spread_anomaly_multiplier",
                3.0,
            ),
            3.0,
        ),
        spread_avg_window=_as_int(
            _resolve(
                risk_raw,
                "XTB_SPREAD_AVG_WINDOW",
                "spread_avg_window",
                50,
            ),
            50,
        ),
        spread_min_samples=_as_int(
            _resolve(
                risk_raw,
                "XTB_SPREAD_MIN_SAMPLES",
                "spread_min_samples",
                20,
            ),
            20,
        ),
        spread_pct_filter_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_SPREAD_PCT_FILTER_ENABLED",
                "spread_pct_filter_enabled",
                True,
            ),
            True,
        ),
        spread_max_pct=_as_float(
            _resolve(
                risk_raw,
                "XTB_SPREAD_MAX_PCT",
                "spread_max_pct",
                0.0,
            ),
            0.0,
        ),
        spread_max_pct_cfd=_as_float(
            _resolve(
                risk_raw,
                "XTB_SPREAD_MAX_PCT_CFD",
                "spread_max_pct_cfd",
                0.20,
            ),
            0.20,
        ),
        spread_max_pct_crypto=_as_float(
            _resolve(
                risk_raw,
                "XTB_SPREAD_MAX_PCT_CRYPTO",
                "spread_max_pct_crypto",
                0.50,
            ),
            0.50,
        ),
        margin_check_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_MARGIN_CHECK_ENABLED",
                "margin_check_enabled",
                True,
            ),
            True,
        ),
        margin_safety_buffer=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_SAFETY_BUFFER",
                "margin_safety_buffer",
                1.15,
            ),
            1.15,
        ),
        margin_fallback_leverage=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_FALLBACK_LEVERAGE",
                "margin_fallback_leverage",
                20.0,
            ),
            20.0,
        ),
        margin_overhead_pct=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_OVERHEAD_PCT",
                "margin_overhead_pct",
                0.0,
            ),
            0.0,
        ),
        margin_weekend_multiplier=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_WEEKEND_MULTIPLIER",
                "margin_weekend_multiplier",
                1.0,
            ),
            1.0,
        ),
        margin_weekend_start_hour_utc=_as_int(
            _resolve(
                risk_raw,
                "XTB_MARGIN_WEEKEND_START_HOUR_UTC",
                "margin_weekend_start_hour_utc",
                20,
            ),
            20,
        ),
        margin_holiday_multiplier=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_HOLIDAY_MULTIPLIER",
                "margin_holiday_multiplier",
                1.0,
            ),
            1.0,
        ),
        margin_holiday_dates_utc=_as_csv_list(
            _resolve(
                risk_raw,
                "XTB_MARGIN_HOLIDAY_DATES_UTC",
                "margin_holiday_dates_utc",
                "",
            )
        ),
        margin_commission_per_lot=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_COMMISSION_PER_LOT",
                "margin_commission_per_lot",
                0.0,
            ),
            0.0,
        ),
        margin_min_free_after_open=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_MIN_FREE_AFTER_OPEN",
                "margin_min_free_after_open",
                0.0,
            ),
            0.0,
        ),
        margin_min_free_after_open_pct=_as_float(
            _resolve(
                risk_raw,
                "XTB_MARGIN_MIN_FREE_AFTER_OPEN_PCT",
                "margin_min_free_after_open_pct",
                0.0,
            ),
            0.0,
        ),
        external_cashflow_rebase_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_EXTERNAL_CASHFLOW_REBASE_ENABLED",
                "external_cashflow_rebase_enabled",
                False,
            ),
            False,
        ),
        external_cashflow_rebase_min_abs=_as_float(
            _resolve(
                risk_raw,
                "XTB_EXTERNAL_CASHFLOW_REBASE_MIN_ABS",
                "external_cashflow_rebase_min_abs",
                500.0,
            ),
            500.0,
        ),
        external_cashflow_rebase_min_pct=_as_float(
            _resolve(
                risk_raw,
                "XTB_EXTERNAL_CASHFLOW_REBASE_MIN_PCT",
                "external_cashflow_rebase_min_pct",
                8.0,
            ),
            8.0,
        ),
        connectivity_check_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_CONNECTIVITY_CHECK_ENABLED",
                "connectivity_check_enabled",
                True,
            ),
            True,
        ),
        connectivity_max_latency_ms=_as_float(
            _resolve(
                risk_raw,
                "XTB_CONNECTIVITY_MAX_LATENCY_MS",
                "connectivity_max_latency_ms",
                500.0,
            ),
            500.0,
        ),
        connectivity_pong_timeout_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_CONNECTIVITY_PONG_TIMEOUT_SEC",
                "connectivity_pong_timeout_sec",
                2.0,
            ),
            2.0,
        ),
        stream_health_check_enabled=_as_bool(
            _resolve(
                risk_raw,
                "XTB_STREAM_HEALTH_CHECK_ENABLED",
                "stream_health_check_enabled",
                True,
            ),
            True,
        ),
        stream_max_tick_age_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_STREAM_MAX_TICK_AGE_SEC",
                "stream_max_tick_age_sec",
                15.0,
            ),
            15.0,
        ),
        stream_event_cooldown_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_STREAM_EVENT_COOLDOWN_SEC",
                "stream_event_cooldown_sec",
                60.0,
            ),
            60.0,
        ),
        hold_reason_log_interval_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_HOLD_REASON_LOG_INTERVAL_SEC",
                "hold_reason_log_interval_sec",
                60.0,
            ),
            60.0,
        ),
        worker_state_flush_interval_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_WORKER_STATE_FLUSH_INTERVAL_SEC",
                "worker_state_flush_interval_sec",
                5.0,
            ),
            5.0,
        ),
        account_snapshot_persist_interval_sec=_as_float(
            _resolve(
                risk_raw,
                "XTB_ACCOUNT_SNAPSHOT_PERSIST_INTERVAL_SEC",
                "account_snapshot_persist_interval_sec",
                10.0,
            ),
            10.0,
        ),
        symbol_auto_disable_on_epic_unavailable=_as_bool(
            _resolve(
                risk_raw,
                "XTB_SYMBOL_AUTO_DISABLE_ON_EPIC_UNAVAILABLE",
                "symbol_auto_disable_on_epic_unavailable",
                True,
            ),
            True,
        ),
        symbol_auto_disable_epic_unavailable_threshold=_as_int(
            _resolve(
                risk_raw,
                "XTB_SYMBOL_AUTO_DISABLE_EPIC_UNAVAILABLE_THRESHOLD",
                "symbol_auto_disable_epic_unavailable_threshold",
                3,
            ),
            3,
        ),
        emergency_cooldown_sec=_as_int(
            _resolve(risk_raw, "XTB_EMERGENCY_COOLDOWN_SEC", "emergency_cooldown_sec", 300),
            300,
        ),
    )

    if not (0.0 < risk.max_risk_per_trade_pct <= 2.0):
        raise ConfigError("max_risk_per_trade_pct must be in range (0, 2]")
    if not (1 <= risk.max_open_positions <= 15):
        raise ConfigError("max_open_positions must be in range [1, 15]")
    if risk.open_slot_lease_sec <= 0:
        raise ConfigError("open_slot_lease_sec must be > 0")
    if risk.min_stop_loss_pips <= 0:
        raise ConfigError("min_stop_loss_pips must be > 0")
    if not (2.0 <= risk.min_tp_sl_ratio <= 3.0):
        raise ConfigError("min_tp_sl_ratio must be in range [2.0, 3.0]")
    if not (0.0 <= risk.trailing_activation_ratio <= 1.0):
        raise ConfigError("trailing_activation_ratio must be in range [0.0, 1.0]")
    if risk.trailing_distance_pips <= 0:
        raise ConfigError("trailing_distance_pips must be > 0")
    if risk.trailing_breakeven_offset_pips < 0:
        raise ConfigError("trailing_breakeven_offset_pips must be >= 0")
    if risk.trailing_breakeven_offset_pips_fx < 0:
        raise ConfigError("trailing_breakeven_offset_pips_fx must be >= 0")
    if risk.trailing_breakeven_offset_pips_index < 0:
        raise ConfigError("trailing_breakeven_offset_pips_index must be >= 0")
    if risk.trailing_breakeven_offset_pips_commodity < 0:
        raise ConfigError("trailing_breakeven_offset_pips_commodity must be >= 0")
    if risk.session_close_buffer_min < 0:
        raise ConfigError("session_close_buffer_min must be >= 0")
    if risk.news_event_buffer_min < 0:
        raise ConfigError("news_event_buffer_min must be >= 0")
    if risk.news_event_action not in {"close", "breakeven"}:
        raise ConfigError("news_event_action must be one of: close, breakeven")
    if risk.spread_anomaly_multiplier <= 1.0:
        raise ConfigError("spread_anomaly_multiplier must be > 1.0")
    if risk.spread_avg_window < 2:
        raise ConfigError("spread_avg_window must be >= 2")
    if not (1 <= risk.spread_min_samples <= risk.spread_avg_window):
        raise ConfigError("spread_min_samples must be in range [1, spread_avg_window]")
    if risk.spread_max_pct < 0:
        raise ConfigError("spread_max_pct must be >= 0")
    if risk.spread_max_pct_cfd < 0:
        raise ConfigError("spread_max_pct_cfd must be >= 0")
    if risk.spread_max_pct_crypto < 0:
        raise ConfigError("spread_max_pct_crypto must be >= 0")
    if risk.margin_safety_buffer < 1.0:
        raise ConfigError("margin_safety_buffer must be >= 1.0")
    if risk.margin_fallback_leverage <= 0:
        raise ConfigError("margin_fallback_leverage must be > 0")
    if risk.margin_overhead_pct < 0:
        raise ConfigError("margin_overhead_pct must be >= 0")
    if risk.margin_weekend_multiplier < 1.0:
        raise ConfigError("margin_weekend_multiplier must be >= 1.0")
    if not (0 <= risk.margin_weekend_start_hour_utc <= 23):
        raise ConfigError("margin_weekend_start_hour_utc must be in range [0, 23]")
    if risk.margin_holiday_multiplier < 1.0:
        raise ConfigError("margin_holiday_multiplier must be >= 1.0")
    for holiday_date in risk.margin_holiday_dates_utc:
        try:
            datetime.strptime(holiday_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ConfigError(
                "margin_holiday_dates_utc must be a comma-separated list of YYYY-MM-DD dates"
            ) from exc
    if risk.margin_commission_per_lot < 0:
        raise ConfigError("margin_commission_per_lot must be >= 0")
    if risk.margin_min_free_after_open < 0:
        raise ConfigError("margin_min_free_after_open must be >= 0")
    if risk.margin_min_free_after_open_pct < 0:
        raise ConfigError("margin_min_free_after_open_pct must be >= 0")
    if risk.external_cashflow_rebase_min_abs < 0:
        raise ConfigError("external_cashflow_rebase_min_abs must be >= 0")
    if risk.external_cashflow_rebase_min_pct < 0:
        raise ConfigError("external_cashflow_rebase_min_pct must be >= 0")
    if risk.connectivity_max_latency_ms <= 0:
        raise ConfigError("connectivity_max_latency_ms must be > 0")
    if risk.connectivity_pong_timeout_sec <= 0:
        raise ConfigError("connectivity_pong_timeout_sec must be > 0")
    if risk.stream_max_tick_age_sec <= 0:
        raise ConfigError("stream_max_tick_age_sec must be > 0")
    if risk.stream_event_cooldown_sec <= 0:
        raise ConfigError("stream_event_cooldown_sec must be > 0")
    if risk.hold_reason_log_interval_sec <= 0:
        raise ConfigError("hold_reason_log_interval_sec must be > 0")
    if risk.worker_state_flush_interval_sec <= 0:
        raise ConfigError("worker_state_flush_interval_sec must be > 0")
    if risk.account_snapshot_persist_interval_sec <= 0:
        raise ConfigError("account_snapshot_persist_interval_sec must be > 0")
    if risk.symbol_auto_disable_epic_unavailable_threshold < 1:
        raise ConfigError("symbol_auto_disable_epic_unavailable_threshold must be >= 1")

    ig_stream_tick_max_age_sec = _as_float(
        _resolve_dual_env(
            raw_file,
            "IG_STREAM_TICK_MAX_AGE_SEC",
            "XTB_STREAM_TICK_MAX_AGE_SEC",
            "ig_stream_tick_max_age_sec",
            15.0,
        ),
        15.0,
    )
    if ig_stream_tick_max_age_sec <= 0:
        raise ConfigError("ig_stream_tick_max_age_sec must be > 0")

    ig_rest_market_min_interval_sec = _as_float(
        _resolve_dual_env(
            raw_file,
            "IG_REST_MARKET_MIN_INTERVAL_SEC",
            "XTB_REST_MARKET_MIN_INTERVAL_SEC",
            "ig_rest_market_min_interval_sec",
            2.0,
        ),
        2.0,
    )
    if ig_rest_market_min_interval_sec < 0:
        raise ConfigError("ig_rest_market_min_interval_sec must be >= 0")

    worker_poll_jitter_sec = _as_float(
        _resolve_dual_env(
            raw_file,
            "IG_WORKER_POLL_JITTER_SEC",
            "XTB_WORKER_POLL_JITTER_SEC",
            "worker_poll_jitter_sec",
            1.0,
        ),
        1.0,
    )
    if worker_poll_jitter_sec < 0:
        raise ConfigError("worker_poll_jitter_sec must be >= 0")

    poll_interval_sec = _as_float(
        _resolve_dual_env(raw_file, "IG_POLL_INTERVAL_SEC", "XTB_POLL_INTERVAL_SEC", "poll_interval_sec", 5.0),
        5.0,
    )
    if poll_interval_sec <= 0:
        raise ConfigError("poll_interval_sec must be > 0")

    passive_history_poll_interval_default = max(1.0, min(15.0, poll_interval_sec))
    passive_history_poll_interval_sec = _as_float(
        _resolve_dual_env(
            raw_file,
            "IG_PASSIVE_HISTORY_POLL_INTERVAL_SEC",
            "XTB_PASSIVE_HISTORY_POLL_INTERVAL_SEC",
            "passive_history_poll_interval_sec",
            passive_history_poll_interval_default,
        ),
        passive_history_poll_interval_default,
    )
    if passive_history_poll_interval_sec <= 0:
        raise ConfigError("passive_history_poll_interval_sec must be > 0")

    price_history_keep_rows_min = _as_int(
        _resolve_dual_env(
            raw_file,
            "IG_PRICE_HISTORY_KEEP_ROWS_MIN",
            "XTB_PRICE_HISTORY_KEEP_ROWS_MIN",
            "price_history_keep_rows_min",
            1000,
        ),
        1000,
    )
    if price_history_keep_rows_min < 100:
        raise ConfigError("price_history_keep_rows_min must be >= 100")

    storage_path_value = (
        os.getenv("BOT_STORAGE_PATH")
        or os.getenv("IG_STORAGE_PATH")
        or os.getenv("XTB_STORAGE_PATH")
        or raw_file.get("storage_path")
        or "./state/xtb_bot.db"
    )
    storage_path = Path(str(storage_path_value)).expanduser()

    bot_magic_prefix = str(
        _resolve_dual_env(raw_file, "IG_BOT_MAGIC_PREFIX", "XTB_BOT_MAGIC_PREFIX", "bot_magic_prefix", "XTBBOT")
    ).strip().upper()
    if not (3 <= len(bot_magic_prefix) <= 16) or not _valid_magic_part(bot_magic_prefix):
        raise ConfigError(
            "bot_magic_prefix must be 3..16 chars and contain only [A-Z0-9_-]"
        )

    bot_magic_instance_raw = _resolve_dual_env(
        raw_file,
        "IG_BOT_MAGIC_INSTANCE",
        "XTB_BOT_MAGIC_INSTANCE",
        "bot_magic_instance",
        None,
    )
    bot_magic_instance = (
        str(bot_magic_instance_raw).strip() if bot_magic_instance_raw not in (None, "") else None
    )
    if bot_magic_instance is not None:
        if not (4 <= len(bot_magic_instance) <= 20) or not _valid_magic_part(bot_magic_instance):
            raise ConfigError(
                "bot_magic_instance must be 4..20 chars and contain only [A-Z0-9a-z_-]"
            )

    default_volume = _as_float(
        _resolve_dual_env(raw_file, "IG_DEFAULT_VOLUME", "XTB_DEFAULT_VOLUME", "default_volume", 0.0),
        0.0,
    )
    if default_volume < 0:
        raise ConfigError("default_volume must be >= 0")

    resolved_symbols = (
        list(symbols_override)
        if symbols_override is not None
        else _resolve_symbols_for_strategy(raw_file, str(strategy_value).lower())
    )

    return BotConfig(
        user_id=str(user_id),
        password=str(password),
        app_name=str(app_name),
        account_type=AccountType(str(account_type_value).lower()),
        mode=RunMode(str(mode_value).lower()),
        symbols=resolved_symbols,
        strategy=str(strategy_value).lower(),
        force_strategy=force_strategy,
        force_symbols=force_symbols,
        strategy_params=strategy_params,
        poll_interval_sec=poll_interval_sec,
        default_volume=default_volume,
        storage_path=storage_path,
        endpoint=endpoint,
        broker=broker,
        api_key=str(api_key) if api_key not in (None, "") else None,
        account_id=account_id,
        symbol_epics=symbol_epics,
        ig_stream_enabled=ig_stream_enabled,
        ig_stream_tick_max_age_sec=ig_stream_tick_max_age_sec,
        ig_rest_market_min_interval_sec=ig_rest_market_min_interval_sec,
        strict_broker_connect=strict_broker_connect,
        worker_poll_jitter_sec=worker_poll_jitter_sec,
        passive_history_poll_interval_sec=passive_history_poll_interval_sec,
        price_history_keep_rows_min=price_history_keep_rows_min,
        bot_magic_prefix=bot_magic_prefix,
        bot_magic_instance=bot_magic_instance,
        strategy_params_map=strategy_params_map,
        strategy_schedule=strategy_schedule,
        strategy_schedule_timezone=strategy_schedule_timezone,
        risk=risk,
    )
