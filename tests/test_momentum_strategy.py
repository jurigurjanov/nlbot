from __future__ import annotations

from datetime import datetime, timezone

import pytest

from xtb_bot.models import Side
from xtb_bot.strategies.base import StrategyContext
from xtb_bot.strategies.momentum import MomentumStrategy


def _make_strategy() -> MomentumStrategy:
    return MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_only",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )


def test_momentum_defaults_to_cross_or_trend_runtime_mode():
    strategy = MomentumStrategy({})
    assert strategy.entry_mode == "cross_or_trend"
    assert strategy.auto_confirm_by_timeframe is False


def test_momentum_reports_insufficient_history_reason():
    strategy = _make_strategy()
    ctx = StrategyContext(symbol="EURUSD", prices=[1.0, 1.01, 1.02])
    signal = strategy.generate_signal(ctx)
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "insufficient_price_history"
    assert signal.metadata.get("required_history") == strategy.min_history


def test_momentum_reports_no_cross_reason():
    strategy = _make_strategy()
    prices = [1.0000, 1.0005, 1.0009, 1.0012, 1.0014, 1.0017]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ma_cross"


def test_momentum_emits_buy_on_bullish_cross():
    strategy = _make_strategy()
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips > 0.0
    assert signal.take_profit_pips >= signal.stop_loss_pips * 2.0
    assert signal.metadata.get("price_aligned_with_fast_ma") is True
    assert signal.metadata.get("confidence_model") == "velocity_weighted_v3"


def test_momentum_holds_in_kama_chop_regime():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_continuation_fast_ma_retest_atr_tolerance": 10.0,
            "momentum_kama_gate_enabled": True,
            "momentum_kama_er_window": 4,
            "momentum_kama_min_efficiency_ratio": 0.8,
            "momentum_kama_min_slope_atr_ratio": 0.5,
        }
    )
    prices = [1.0, 0.99, 1.0, 0.99, 1.0, 1.01]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001))
    assert signal.side == Side.BUY
    assert "kama_chop_regime" in (signal.metadata.get("soft_filter_reasons") or [])
    assert float(signal.metadata.get("kama_penalty") or 0.0) > 0.0


def test_momentum_asia_index_kama_gate_uses_adaptive_slope_floor():
    strategy = MomentumStrategy(
        {
            "momentum_kama_gate_enabled": True,
            "momentum_kama_er_window": 4,
            "momentum_kama_fast_window": 2,
            "momentum_kama_slow_window": 30,
            "momentum_kama_min_efficiency_ratio": 0.16,
            "momentum_kama_min_slope_atr_ratio": 0.05,
        }
    )
    prices = [100.0, 100.2, 100.25, 100.3, 100.35, 100.4, 100.45]
    passes, payload = strategy._kama_gate_payload(
        "AUS200",
        prices,
        1.0,
        Side.BUY,
        min_slope_atr_ratio_hint=0.06,
    )
    assert passes is True
    assert payload.get("momentum_kama_min_slope_atr_ratio_source") == "australia_index_entry_profile"
    assert payload.get("momentum_kama_min_slope_atr_ratio_effective") == pytest.approx(0.02)


def test_momentum_session_vwap_payload_flags_poor_volume_quality():
    strategy = MomentumStrategy(
        {
            "momentum_vwap_filter_enabled": True,
            "momentum_vwap_min_session_bars": 3,
            "momentum_vwap_min_volume_samples": 3,
            "momentum_vwap_min_volume_quality": 0.5,
        }
    )
    payload = strategy._session_vwap_payload(
        symbol="US100",
        prices=[100.0, 101.0, 102.0, 103.0, 104.0],
        timestamps=[0.0, 60.0, 120.0, 180.0, 240.0],
        volumes=[1e-9, 1e-9, 1e-9, 1e-9, 1e-9],
        timeframe_sec=60.0,
        session_start_hour_utc=0,
    )
    assert payload.get("vwap_status") == "poor_volume_quality"
    assert float(payload.get("volume_quality", 1.0)) < 0.5


def test_momentum_blocks_index_short_above_vwap_without_reclaim():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "momentum_kama_gate_enabled": False,
            "momentum_vwap_filter_enabled": True,
            "momentum_vwap_reclaim_required": True,
            "momentum_vwap_min_session_bars": 5,
            "momentum_vwap_min_volume_samples": 5,
            "momentum_vwap_min_volume_quality": 0.1,
            "momentum_vwap_overstretch_sigma": 2.0,
        }
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 106.0, 105.0, 106.0]
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="US100",
            prices=prices,
            timestamps=[idx * 60.0 for idx in range(len(prices))],
            volumes=[100.0] * len(prices),
            tick_size=1.0,
            pip_size=1.0,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "above_vwap_no_reclaim"
    assert signal.metadata.get("vwap_status") == "ok"


def test_momentum_emits_fast_ma_trailing_payload_when_enabled():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_fast_ma_trailing_enabled": True,
            "momentum_fast_ma_trailing_use_closed_candle": False,
            "momentum_fast_ma_trailing_activation_r_multiple": 0.7,
            "momentum_fast_ma_trailing_buffer_atr": 0.2,
            "momentum_fast_ma_trailing_buffer_pips": 0.4,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001))
    assert signal.side == Side.BUY
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("trailing_enabled") is True
    assert trailing.get("trailing_mode") == "fast_ma"
    assert trailing.get("trailing_activation_r_multiple") == pytest.approx(0.7)
    assert trailing.get("fast_ma_buffer_atr") == pytest.approx(0.2)
    assert trailing.get("fast_ma_buffer_pips") == pytest.approx(0.4)
    assert trailing.get("fast_ma_value") == pytest.approx(float(signal.metadata.get("fast_ma", 0.0)))
    assert signal.metadata.get("fast_ma_trailing_mode") == "fast_ma"


def test_momentum_confidence_rewards_velocity_and_price_alignment():
    base = MomentumStrategy._confidence(ema_gap_atr=0.2, slope_strength_atr=0.05, price_aligned_with_fast_ma=False)
    with_alignment = MomentumStrategy._confidence(
        ema_gap_atr=0.2, slope_strength_atr=0.05, price_aligned_with_fast_ma=True
    )
    high_gap_low_slope = MomentumStrategy._confidence(
        ema_gap_atr=1.2, slope_strength_atr=0.02, price_aligned_with_fast_ma=False
    )
    low_gap_high_slope = MomentumStrategy._confidence(
        ema_gap_atr=0.2, slope_strength_atr=0.10, price_aligned_with_fast_ma=False
    )
    assert with_alignment > base
    assert low_gap_high_slope > high_gap_low_slope


def test_momentum_confidence_penalizes_late_stretched_trend_entries():
    early = MomentumStrategy._confidence(
        ema_gap_atr=0.6,
        slope_strength_atr=0.08,
        price_aligned_with_fast_ma=True,
        price_slow_gap_atr=0.8,
        is_trend_continuation_entry=True,
    )
    late = MomentumStrategy._confidence(
        ema_gap_atr=1.8,
        slope_strength_atr=0.12,
        price_aligned_with_fast_ma=True,
        price_slow_gap_atr=2.8,
        is_trend_continuation_entry=True,
    )
    assert early > late


def test_momentum_entry_quality_penalizes_choppy_cross_setup():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 0.25,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )

    smooth_trend = [1.00, 1.01, 1.02, 1.03, 1.04, 1.05, 1.045]
    choppy_cross = [1.02, 1.00, 1.01, 0.99, 1.00, 0.98, 1.05]

    smooth_signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=smooth_trend, tick_size=0.0001))
    choppy_signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=choppy_cross, tick_size=0.0001))

    assert smooth_signal.side == Side.BUY
    assert choppy_signal.side == Side.BUY
    assert smooth_signal.metadata.get("entry_quality_family") == "trend"
    assert smooth_signal.metadata.get("entry_quality_status") == "ok"
    assert choppy_signal.metadata.get("entry_quality_status") == "penalized"
    assert "choppy_path" in (choppy_signal.metadata.get("entry_quality_reasons") or [])
    assert smooth_signal.confidence > choppy_signal.confidence


def test_momentum_holds_when_spread_is_too_wide():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_spread_pips": 2.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, current_spread_pips=2.5)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"
    assert signal.metadata.get("max_spread_pips") == 2.0
    assert signal.metadata.get("max_spread_pips_source") == "global"


def test_momentum_spread_limit_can_be_overridden_per_symbol():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_spread_pips": 2.0,
            "momentum_max_spread_pips_by_symbol": {"WTI": 5.0},
            "momentum_max_spread_to_stop_ratio": 10.0,
            "momentum_max_spread_to_atr_ratio": 100.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="WTI", prices=prices, current_spread_pips=4.8)
    )
    assert signal.side == Side.BUY


def test_momentum_blocks_when_spread_is_too_large_relative_to_stop():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_max_spread_pips": 10.0,
            "momentum_max_spread_to_stop_ratio": 0.10,
            "momentum_max_spread_to_atr_ratio": 10.0,
            "momentum_min_stop_loss_pips": 20.0,
            "momentum_min_take_profit_pips": 40.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "stop_loss_pips": 20.0,
            "take_profit_pips": 40.0,
        }
    )
    prices = [1.0, 0.9996, 0.9992, 0.9988, 0.9984, 1.0012]
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, current_spread_pips=2.5, tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide_relative_to_stop"
    assert signal.metadata.get("spread_to_stop_ratio", 0.0) > 0.10


def test_momentum_blocks_when_spread_is_too_large_relative_to_atr():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_max_spread_pips": 10.0,
            "momentum_max_spread_to_stop_ratio": 1.0,
            "momentum_max_spread_to_atr_ratio": 0.20,
            "momentum_min_stop_loss_pips": 20.0,
            "momentum_min_take_profit_pips": 40.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "stop_loss_pips": 20.0,
            "take_profit_pips": 40.0,
        }
    )
    prices = [1.0, 0.9996, 0.9992, 0.9988, 0.9984, 1.0012]
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, current_spread_pips=2.5, tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide_relative_to_atr"
    assert signal.metadata.get("spread_to_atr_ratio", 0.0) > 0.20


def test_momentum_symbol_spread_override_takes_precedence_over_global_limit():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_spread_pips": 3.0,
            "momentum_max_spread_pips_by_symbol": {"EURUSD": 1.5},
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, current_spread_pips=2.0)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"
    assert signal.metadata.get("max_spread_pips") == 1.5
    assert signal.metadata.get("max_spread_pips_source") == "symbol_override"


def test_momentum_requires_cross_confirmation_bars():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_confirm_bars": 2,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )

    weak_cross = [1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 0.9950, 1.0500]
    weak_signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=weak_cross))
    assert weak_signal.side == Side.HOLD
    assert weak_signal.metadata.get("reason") == "cross_not_confirmed"

    confirmed_cross = [1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 1.0030, 1.0032]
    confirmed_signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=confirmed_cross)
    )
    assert confirmed_signal.side == Side.BUY
    assert confirmed_signal.metadata.get("confirm_bars") == 2


def test_momentum_uses_atr_based_risk_levels():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 1.0,
            "take_profit_pips": 2.0,
            "momentum_atr_window": 5,
            "momentum_atr_multiplier": 2.0,
            "momentum_risk_reward_ratio": 3.0,
            "momentum_min_stop_loss_pips": 1.0,
            "momentum_min_take_profit_pips": 1.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001)
    )
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips > 100.0
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 3.0)
    assert signal.metadata.get("atr_pips", 0.0) > 0.0


def test_momentum_low_tf_risk_profile_compresses_sl_tp_targets():
    params = {
        "fast_window": 3,
        "slow_window": 5,
        "momentum_entry_mode": "cross_only",
        "momentum_atr_window": 5,
        "momentum_timeframe_sec": 60,
        "momentum_max_price_slow_gap_atr": 10.0,
        "momentum_pullback_entry_max_gap_atr": 10.0,
        "momentum_min_relative_stop_pct": 0.0,
        "momentum_atr_multiplier": 2.4,
        "momentum_risk_reward_ratio": 2.0,
        "momentum_min_stop_loss_pips": 20.0,
        "momentum_min_take_profit_pips": 40.0,
        "stop_loss_pips": 20.0,
        "take_profit_pips": 40.0,
    }
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    ctx = StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001)

    compact = MomentumStrategy({**params, "momentum_low_tf_risk_profile_enabled": True}).generate_signal(ctx)
    unbounded = MomentumStrategy({**params, "momentum_low_tf_risk_profile_enabled": False}).generate_signal(ctx)

    assert compact.side == Side.BUY
    assert unbounded.side == Side.BUY
    assert compact.stop_loss_pips < unbounded.stop_loss_pips
    assert compact.take_profit_pips < unbounded.take_profit_pips
    assert compact.metadata.get("risk_profile") == "low_tf_compact"
    assert compact.metadata.get("atr_multiplier_effective") == pytest.approx(1.7)
    assert compact.metadata.get("risk_reward_ratio_effective") == pytest.approx(2.0)


def test_momentum_blocks_flat_ma_slope():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_slope_atr_ratio": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
            "momentum_min_relative_stop_pct": 0.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "ma_slope_too_flat"


def test_momentum_auto_confirm_bounds_to_one_for_m1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_only",
            "momentum_confirm_bars": 5,
            "momentum_auto_confirm_by_timeframe": True,
            "momentum_timeframe_sec": 60,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    assert strategy._effective_confirm_bars(60.0) == 1


def test_momentum_auto_confirm_raises_to_min_for_m1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_only",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": True,
            "momentum_timeframe_sec": 60,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("confirm_bars") == 1


def test_momentum_auto_confirm_uses_one_for_h1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_confirm_bars": 4,
            "momentum_auto_confirm_by_timeframe": True,
            "momentum_timeframe_sec": 3600,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("confirm_bars") == 1
    assert signal.metadata.get("configured_confirm_bars") == 4


def test_momentum_can_disable_auto_confirm_by_timeframe():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_confirm_bars": 5,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_timeframe_sec": 60,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0030, 1.0020, 1.0010, 1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 1.0100, 1.0200]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("confirm_bars") == 5


def test_momentum_default_atr_window_is_14_even_with_small_slow_window():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    assert strategy.atr_window == 14


def test_momentum_index_pip_size_fallback_uses_tenth_point():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 105.0]
    signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.1)
    assert signal.metadata.get("pip_size_source") == "fallback_symbol_map"


def test_momentum_can_require_tick_size_from_context():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_require_context_tick_size": True,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 105.0]
    signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices, tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_momentum_accepts_context_pip_size_when_tick_size_missing():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_require_context_tick_size": True,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 105.0]
    signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices, tick_size=None, pip_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.1)
    assert signal.metadata.get("pip_size_source") == "context_pip_size"


def test_momentum_allows_entry_when_fast_slope_positive_but_slow_slope_still_negative():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_ma_type": "sma",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [
        1.00638,
        0.99779,
        1.00566,
        0.99982,
        0.99391,
        0.99739,
        1.00615,
        0.99862,
        0.98876,
        0.98615,
        0.97664,
        0.97874,
        0.98592,
        0.97966,
    ]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("fast_slope", 0.0) > 0
    assert signal.metadata.get("slow_slope", 0.0) < 0
    assert signal.metadata.get("slope_filter_mode") == "fast_only"


def test_momentum_trend_continuation_requires_pullback_reset():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [0.96, 0.968, 0.976, 0.984, 0.993, 1.003, 1.014, 1.026]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_pullback_reset"
    assert signal.metadata.get("trend_signal") == "ma_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_momentum_can_use_trend_continuation_entry_after_pullback_reset():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 0.25,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [0.96, 0.968, 0.976, 0.984, 0.993, 1.003, 1.014, 1.011]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ma_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_momentum_continuation_confidence_is_capped_separately_from_cross():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 0.25,
            "momentum_continuation_confidence_cap": 0.45,
            "momentum_continuation_price_alignment_bonus_multiplier": 0.0,
            "momentum_continuation_late_penalty_multiplier": 1.35,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [0.96, 0.968, 0.976, 0.984, 0.993, 1.003, 1.014, 1.011]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ma_trend_up"
    assert signal.metadata.get("momentum_confidence_cap_effective") == pytest.approx(0.45)
    assert signal.confidence <= 0.4500001


def test_momentum_entry_mode_can_be_overridden_per_symbol():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_entry_filters_by_symbol": {
                "WTI": {"momentum_entry_mode": "cross_only"},
            },
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 0.25,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [0.96, 0.968, 0.976, 0.984, 0.993, 1.003, 1.014, 1.011]
    eurusd_signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    wti_signal = strategy.generate_signal(StrategyContext(symbol="WTI", prices=prices))

    assert eurusd_signal.side == Side.BUY
    assert eurusd_signal.metadata.get("trend_signal") == "ma_trend_up"
    assert eurusd_signal.metadata.get("entry_mode") == "cross_or_trend"

    assert wti_signal.side == Side.HOLD
    assert wti_signal.metadata.get("reason") == "no_ma_cross"
    assert wti_signal.metadata.get("entry_mode") == "cross_only"
    assert wti_signal.metadata.get("configured_entry_mode") == "cross_or_trend"
    assert wti_signal.metadata.get("entry_mode_override_applied") is True


def test_momentum_trend_mode_requires_slow_ma_slope_alignment():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_ma_type": "sma",
            "momentum_entry_mode": "cross_or_trend",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_atr_window": 5,
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [
        1.0,
        0.99155,
        0.98301,
        0.99356,
        1.00614,
        1.00146,
        0.98875,
        0.96947,
        0.96325,
        0.97458,
        0.95633,
        0.96664,
        0.98652,
        0.9734,
        0.97353,
    ]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ma_cross"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_momentum_trend_mode_requires_expanding_ma_gap():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 5,
            "momentum_ma_type": "sma",
            "momentum_entry_mode": "cross_or_trend",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_atr_window": 5,
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 1.5,
            "momentum_pullback_entry_max_gap_atr": 1.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 1.02, 1.04, 1.06, 1.08, 1.09, 1.095, 1.097, 1.102]
    signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices, tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_pullback_reset"


def test_momentum_entry_filters_by_symbol_override_wait_pullback_thresholds():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 5,
            "momentum_ma_type": "sma",
            "momentum_entry_mode": "cross_or_trend",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_atr_window": 5,
            "momentum_min_slope_atr_ratio": 0.02,
            "momentum_min_trend_gap_atr": 0.10,
            "momentum_max_price_slow_gap_atr": 1.3,
            "momentum_pullback_entry_max_gap_atr": 1.0,
            "momentum_entry_filters_by_symbol": {
                "US100": {
                    "momentum_max_price_slow_gap_atr": 0.9,
                    "momentum_pullback_entry_max_gap_atr": 0.7,
                    "momentum_continuation_fast_ma_retest_atr_tolerance": 0.15,
                }
            },
            "momentum_continuation_fast_ma_retest_atr_tolerance": 0.20,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_session_filter_enabled": False,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 1.006402, 1.011701, 1.016209, 1.018504, 1.019004, 1.019504, 1.020004, 1.0207]
    us500_signal = strategy.generate_signal(StrategyContext(symbol="US500", prices=prices, tick_size=0.1))
    us100_signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices, tick_size=0.1))
    assert us500_signal.side == Side.BUY
    assert us500_signal.metadata.get("entry_filter_overrides_applied") is False
    assert us100_signal.side == Side.HOLD
    assert us100_signal.metadata.get("reason") == "no_pullback_reset"
    assert us100_signal.metadata.get("continuation_fast_ma_retest_atr_tolerance") == pytest.approx(0.15)


def test_momentum_clamps_overly_permissive_quality_filters():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_min_slope_atr_ratio": 0.00001,
            "momentum_min_trend_gap_atr": 0.0,
            "momentum_max_price_slow_gap_atr": 5.0,
            "momentum_pullback_entry_max_gap_atr": 5.0,
        }
    )
    assert strategy.min_slope_atr_ratio == MomentumStrategy._QUALITY_MIN_SLOPE_ATR_RATIO
    assert strategy.min_trend_gap_atr == MomentumStrategy._QUALITY_MIN_TREND_GAP_ATR
    assert strategy.max_price_slow_gap_atr == MomentumStrategy._QUALITY_MAX_PRICE_SLOW_GAP_ATR
    assert strategy.pullback_entry_max_gap_atr == MomentumStrategy._QUALITY_MAX_PULLBACK_ENTRY_GAP_ATR


def test_momentum_supports_ema_ma_type():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_ma_type": "ema",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.metadata.get("ma_type") == "ema"
    assert signal.side in {Side.HOLD, Side.BUY, Side.SELL}


def test_momentum_volume_confirmation_blocks_low_volume():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_window": 5,
            "momentum_volume_min_samples": 3,
            "momentum_min_volume_ratio": 1.2,
            "momentum_volume_allow_missing": False,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_entry_mode": "cross_only",
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="EURUSD",
            prices=prices,
            volumes=[100.0, 110.0, 120.0, 130.0, 140.0],
            current_volume=90.0,
        )
    )
    assert signal.side == Side.BUY
    assert "volume_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True
    assert signal.metadata.get("volume_check_status") == "below_threshold"


def test_momentum_volume_confirmation_allows_missing_volume_with_fallback():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_allow_missing": True,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, current_volume=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_check_status") == "missing_current_volume"


def test_momentum_volume_confirmation_blocks_missing_volume_if_required():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_allow_missing": False,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_entry_mode": "cross_only",
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, current_volume=None))
    assert signal.side == Side.BUY
    assert "volume_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True
    assert signal.metadata.get("volume_check_status") == "missing_current_volume"


def test_momentum_index_missing_volume_is_soft_allowed_even_when_required():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_allow_missing": False,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="IT40", prices=prices, current_volume=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_check_status") == "missing_current_volume"
    assert signal.metadata.get("volume_missing_relaxed_for_index") is True
    assert signal.metadata.get("volume_allow_missing_effective") is True


def test_momentum_volume_confirmation_ignores_non_numeric_history_values():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_window": 5,
            "momentum_volume_min_samples": 3,
            "momentum_min_volume_ratio": 1.1,
            "momentum_volume_allow_missing": False,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="EURUSD",
            prices=prices,
            volumes=[100.0, None, "oops", 120.0, 130.0],
            current_volume=150.0,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_check_status") == "ok"


def test_momentum_volume_confirmation_deduplicates_current_volume_tail_sample():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_window": 5,
            "momentum_volume_min_samples": 1,
            "momentum_min_volume_ratio": 1.2,
            "momentum_volume_allow_missing": False,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="EURUSD",
            prices=prices,
            volumes=[100.0, 100.0, 200.0],
            current_volume=200.0,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_current_deduplicated") is True
    assert signal.metadata.get("volume_current_source") == "ctx_volumes_last"
    assert signal.metadata.get("avg_volume") == pytest.approx(100.0)
    assert signal.metadata.get("volume_ratio") == pytest.approx(2.0)


def test_momentum_drops_non_finite_prices_before_signal_generation():
    strategy = _make_strategy()
    prices = [1.0, 0.99, 0.98, float("nan"), 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("invalid_prices_dropped") == 1


def test_momentum_session_filter_blocks_outside_hours():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_session_filter_enabled": True,
            "momentum_session_start_hour_utc": 6,
            "momentum_session_end_hour_utc": 22,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    ts = datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc).timestamp()
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, timestamps=[ts]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "outside_trading_session"


def test_momentum_session_filter_accepts_millisecond_timestamps():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_session_filter_enabled": True,
            "momentum_session_start_hour_utc": 6,
            "momentum_session_end_hour_utc": 22,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    ts_ms = int(datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, timestamps=[ts_ms]))
    assert signal.side == Side.BUY
    assert signal.metadata.get("session_hour_utc") == 12


def test_momentum_index_session_filter_uses_local_market_hours_for_topix():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_session_filter_enabled": True,
            "momentum_session_start_hour_utc": 6,
            "momentum_session_end_hour_utc": 22,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_volume_confirmation": False,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": False,
            "momentum_kama_gate_enabled": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    ts = datetime(2026, 4, 6, 4, 0, tzinfo=timezone.utc).timestamp()
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="TOPIX", prices=prices, timestamps=[ts], current_spread_pips=0.01)
    )
    assert signal.metadata.get("reason") != "outside_trading_session"
    assert signal.side == Side.BUY
    assert signal.metadata.get("session_mode") == "index_local_market_hours"
    assert signal.metadata.get("session_profile") == "japan"
    assert signal.metadata.get("session_local_hour") == 13


def test_momentum_wait_pullback_mode_blocks_far_entries_with_target():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 0.2,
            "momentum_price_gap_mode": "wait_pullback",
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "waiting_for_limit_pullback"
    assert signal.metadata.get("entry_action") == "place_limit"
    assert signal.metadata.get("pullback_wait_state") == "hard_cap"
    assert signal.metadata.get("pullback_entry_price") is not None
    assert signal.metadata.get("pullback_entry_price_source") == "fast_ma"
    assert signal.metadata.get("suggested_order_type") == "limit"
    assert signal.metadata.get("suggested_entry_price_source") == "fast_ma"


def test_momentum_wait_pullback_targets_fast_ma_for_limit_reentry():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 0.2,
            "momentum_price_gap_mode": "wait_pullback",
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "waiting_for_limit_pullback"
    assert signal.metadata.get("pullback_entry_price") == pytest.approx(signal.metadata.get("pullback_fast_ma_price"))
    assert signal.metadata.get("suggested_entry_price") == pytest.approx(signal.metadata.get("pullback_fast_ma_price"))
    assert signal.metadata.get("pullback_distance_price", 0.0) < signal.metadata.get(
        "pullback_slow_ma_distance_price", 0.0
    )


def test_momentum_wait_pullback_uses_soft_entry_zone_to_block_far_entry():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 5,
            "momentum_ma_type": "ema",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 0.3,
            "momentum_price_gap_mode": "wait_pullback",
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.999, 0.998, 0.997, 0.996, 0.997, 0.998]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "wait_pullback_entry"
    assert signal.metadata.get("pullback_entry_max_gap_atr") == pytest.approx(0.3)


def test_momentum_wait_pullback_allows_entry_after_returning_to_soft_zone():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 5,
            "momentum_ma_type": "ema",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 0.3,
            "momentum_price_gap_mode": "wait_pullback",
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.999, 0.998, 0.997, 0.996, 0.9975, 0.998]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pullback_entry_max_gap_atr") == pytest.approx(0.3)


def test_momentum_fresh_cross_relief_can_bypass_soft_pullback_zone():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 5,
            "momentum_ma_type": "ema",
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 0.3,
            "momentum_price_gap_mode": "wait_pullback",
            "momentum_fresh_cross_filter_relief_enabled": True,
            "momentum_min_relative_stop_pct": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.999, 0.998, 0.997, 0.996, 0.997, 0.998]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("entry_family") == "fresh_cross"
    assert signal.metadata.get("fresh_cross_gap_relief_applied") is True
    assert signal.metadata.get("fresh_cross_gap_penalty", 0.0) > 0.0


def test_momentum_higher_tf_bias_blocks_counter_trend_entry():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 3,
            "momentum_ma_type": "sma",
            "momentum_atr_window": 3,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_higher_tf_bias_enabled": True,
            "momentum_higher_tf_bias_timeframe_sec": 300,
            "momentum_higher_tf_bias_fast_window": 2,
            "momentum_higher_tf_bias_slow_window": 3,
            "momentum_higher_tf_bias_allow_missing": False,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [
        10.0, 9.9, 9.8, 9.7, 9.6,
        9.5, 9.4, 9.3, 9.2, 9.1,
        9.0, 8.9, 8.8, 8.7, 8.6,
        8.5, 8.4, 8.3, 8.9, 9.0,
    ]
    t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc).timestamp()
    timestamps = [t0 + (idx * 60.0) for idx in range(len(prices))]
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=prices, timestamps=timestamps, tick_size=0.1)
    )
    assert signal.side == Side.BUY
    assert "higher_tf_bias_mismatch" in (signal.metadata.get("soft_filter_reasons") or [])
    assert float(signal.metadata.get("higher_tf_bias_penalty") or 0.0) > 0.0
    assert signal.metadata.get("higher_tf_bias_status") == "mismatch"


def test_momentum_higher_tf_bias_remains_strict_even_when_relief_enabled():
    strategy = MomentumStrategy(
        {
            "fast_window": 2,
            "slow_window": 3,
            "momentum_ma_type": "sma",
            "momentum_atr_window": 3,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_higher_tf_bias_enabled": True,
            "momentum_higher_tf_bias_timeframe_sec": 300,
            "momentum_higher_tf_bias_fast_window": 2,
            "momentum_higher_tf_bias_slow_window": 3,
            "momentum_higher_tf_bias_allow_missing": False,
            "momentum_fresh_cross_filter_relief_enabled": True,
            "momentum_continuation_fast_ma_retest_atr_tolerance": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [
        10.0, 9.9, 9.8, 9.7, 9.6,
        9.5, 9.4, 9.3, 9.2, 9.1,
        9.0, 8.9, 8.8, 8.7, 8.6,
        8.5, 8.4, 8.3, 8.9, 9.0,
    ]
    t0 = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc).timestamp()
    timestamps = [t0 + (idx * 60.0) for idx in range(len(prices))]
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=prices, timestamps=timestamps, tick_size=0.1)
    )
    assert signal.side == Side.BUY
    assert "higher_tf_bias_mismatch" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("higher_tf_bias_status") == "mismatch"
    assert signal.metadata.get("higher_tf_bias_relief_applied") is True
    assert signal.metadata.get("higher_tf_bias_penalty", 0.0) > 0.0


def test_momentum_higher_tf_bias_can_hold_without_timestamps_when_strict():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_higher_tf_bias_enabled": True,
            "momentum_higher_tf_bias_allow_missing": False,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "higher_tf_bias_unavailable"
    assert signal.metadata.get("higher_tf_bias_status") == "missing_timestamps"


def test_momentum_regime_adx_filter_blocks_low_trend_strength():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_regime_adx_window": 3,
            "momentum_regime_min_adx": 20.0,
            "momentum_regime_adx_hysteresis": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    strategy._adx = lambda _prices, _window: 12.0  # type: ignore[method-assign]
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001))
    assert signal.side == Side.BUY
    assert "regime_adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("regime_adx_softened") is True
    assert signal.metadata.get("regime_adx") == pytest.approx(12.0)


def test_momentum_fresh_cross_relief_softens_low_volume_and_adx_filters():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_volume_confirmation": True,
            "momentum_volume_window": 5,
            "momentum_volume_min_samples": 3,
            "momentum_min_volume_ratio": 1.2,
            "momentum_volume_allow_missing": False,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_regime_adx_window": 3,
            "momentum_regime_min_adx": 20.0,
            "momentum_regime_adx_hysteresis": 0.0,
            "momentum_fresh_cross_filter_relief_enabled": True,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    strategy._adx = lambda _prices, _window: 12.0  # type: ignore[method-assign]
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="EURUSD",
            prices=prices,
            volumes=[100.0, 110.0, 120.0, 130.0, 140.0],
            current_volume=90.0,
            tick_size=0.0001,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_check_status") == "below_threshold"
    assert signal.metadata.get("volume_relief_applied") is True
    assert signal.metadata.get("volume_penalty", 0.0) > 0.0
    assert signal.metadata.get("regime_adx_relief_applied") is True
    assert signal.metadata.get("regime_adx_penalty", 0.0) > 0.0
    assert signal.metadata.get("auxiliary_filter_penalty_total", 0.0) > 0.0


def test_momentum_regime_adx_filter_allows_entry_when_trend_strength_sufficient():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_regime_adx_window": 3,
            "momentum_regime_min_adx": 20.0,
            "momentum_regime_adx_hysteresis": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    strategy._adx = lambda _prices, _window: 35.0  # type: ignore[method-assign]
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, tick_size=0.0001))
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime_adx_filter_enabled") is True
    assert signal.metadata.get("regime_adx") == pytest.approx(35.0)


def test_momentum_regime_adx_hysteresis_state_refreshes_without_candidate_signal():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_only",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_atr_window": 5,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_regime_adx_window": 3,
            "momentum_regime_min_adx": 25.0,
            "momentum_regime_adx_hysteresis": 2.0,
            "momentum_regime_use_hysteresis_state": True,
            "momentum_max_spread_pips": 5.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    adx_values = iter([30.0, 20.0, 24.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]

    cross_prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    no_signal_prices = [1.0, 1.01, 1.02, 1.03, 1.04, 1.05]

    first = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=cross_prices, tick_size=0.0001))
    second = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=no_signal_prices, tick_size=0.0001))
    third = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=cross_prices, tick_size=0.0001))

    assert first.side == Side.BUY
    assert second.side == Side.HOLD
    assert second.metadata.get("reason") == "no_ma_cross"
    assert third.side == Side.BUY
    assert "regime_adx_below_threshold" in (third.metadata.get("soft_filter_reasons") or [])
    assert third.metadata.get("regime_adx_active_before") is False
    assert third.metadata.get("regime_adx_state_refreshed_pre_signal") is True


def test_momentum_regime_adx_hysteresis_state_isolated_by_timeframe_scope():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_entry_mode": "cross_only",
            "momentum_confirm_bars": 1,
            "momentum_auto_confirm_by_timeframe": False,
            "momentum_atr_window": 5,
            "momentum_min_relative_stop_pct": 0.0,
            "momentum_max_price_slow_gap_atr": 10.0,
            "momentum_pullback_entry_max_gap_atr": 10.0,
            "momentum_higher_tf_bias_enabled": False,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_regime_adx_window": 3,
            "momentum_regime_min_adx": 25.0,
            "momentum_regime_adx_hysteresis": 2.0,
            "momentum_regime_use_hysteresis_state": True,
            "momentum_max_spread_pips": 5.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    adx_values = iter([30.0, 24.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]

    t0 = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc).timestamp()
    m1_timestamps = [t0 + (idx * 60.0) for idx in range(len(prices))]
    m5_timestamps = [t0 + (idx * 300.0) for idx in range(len(prices))]

    first = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, timestamps=m1_timestamps, tick_size=0.0001)
    )
    second = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=prices, timestamps=m5_timestamps, tick_size=0.0001)
    )

    assert first.side == Side.BUY
    assert second.side == Side.BUY
    assert "regime_adx_below_threshold" in (second.metadata.get("soft_filter_reasons") or [])
    assert second.metadata.get("regime_adx_active_before") is False


def test_momentum_regime_scope_ttl_evicts_stale_entries():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_regime_adx_filter_enabled": True,
            "momentum_timeframe_sec": 60,
        }
    )
    strategy._adx_regime_active_by_scope["stale"] = True
    strategy._adx_scope_last_timestamp["stale"] = 1_700_000_000.0

    strategy._maybe_reset_regime_scope_for_timestamp("fresh", 1_700_000_000.0 + strategy._adx_scope_ttl_sec + 1.0)

    assert "stale" not in strategy._adx_scope_last_timestamp
    assert "stale" not in strategy._adx_regime_active_by_scope
    assert "fresh" in strategy._adx_scope_last_timestamp
