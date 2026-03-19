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
            "momentum_atr_window": 5,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )


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
    assert signal.metadata.get("confidence_model") == "velocity_weighted_v2"


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
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(
        StrategyContext(symbol="WTI", prices=prices, current_spread_pips=4.8)
    )
    assert signal.side == Side.BUY


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
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )

    weak_cross = [1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 0.9950, 1.0500]
    weak_signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=weak_cross))
    assert weak_signal.side == Side.HOLD
    assert weak_signal.metadata.get("reason") == "cross_not_confirmed"

    confirmed_cross = [1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 1.0100, 1.0200]
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


def test_momentum_blocks_flat_ma_slope():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
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


def test_momentum_auto_confirm_bounds_to_three_for_m1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_confirm_bars": 5,
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
    assert signal.metadata.get("confirm_bars") == 3
    assert signal.metadata.get("configured_confirm_bars") == 5
    assert signal.metadata.get("timeframe_sec") == 60.0


def test_momentum_auto_confirm_raises_to_min_for_m1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_confirm_bars": 1,
            "momentum_timeframe_sec": 60,
            "momentum_atr_window": 5,
            "momentum_max_price_slow_gap_atr": 10.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0030, 1.0020, 1.0010, 1.0000, 0.9990, 0.9980, 0.9970, 0.9960, 1.0100, 1.0200]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.metadata.get("confirm_bars") == 2


def test_momentum_auto_confirm_uses_one_for_h1():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_confirm_bars": 4,
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
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 105.0]
    signal = strategy.generate_signal(StrategyContext(symbol="US100", prices=prices, tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


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


def test_momentum_can_use_trend_continuation_entry_mode():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_entry_mode": "cross_or_trend",
            "momentum_min_trend_gap_atr": 0.0,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [0.96, 0.97, 0.98, 0.99, 1.00, 1.01, 1.02, 1.03]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ma_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_momentum_supports_ema_ma_type():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_ma_type": "ema",
            "momentum_atr_window": 5,
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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_threshold"
    assert signal.metadata.get("volume_check_status") == "below_threshold"


def test_momentum_volume_confirmation_allows_missing_volume_with_fallback():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_volume_confirmation": True,
            "momentum_volume_allow_missing": True,
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
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, current_volume=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_threshold"
    assert signal.metadata.get("volume_check_status") == "missing_current_volume"


def test_momentum_session_filter_blocks_outside_hours():
    strategy = MomentumStrategy(
        {
            "fast_window": 3,
            "slow_window": 5,
            "momentum_atr_window": 5,
            "momentum_session_filter_enabled": True,
            "momentum_session_start_hour_utc": 6,
            "momentum_session_end_hour_utc": 22,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 20.0,
        }
    )
    ts = datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc).timestamp()
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05]
    signal = strategy.generate_signal(StrategyContext(symbol="EURUSD", prices=prices, timestamps=[ts]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "outside_trading_session"


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
    assert signal.metadata.get("reason") == "wait_pullback_entry"
    assert signal.metadata.get("pullback_entry_price") is not None


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
