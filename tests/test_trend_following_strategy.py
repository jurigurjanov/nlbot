from __future__ import annotations

from datetime import datetime, timezone
import logging

import pytest

from xtb_bot.models import Side
from xtb_bot.numeric import ema, mean_std
import xtb_bot.strategies.mean_reversion_bb as mean_reversion_bb_module
from xtb_bot.strategies import available_strategies, create_strategy
from xtb_bot.strategies.base import StrategyContext


def _ctx(
    prices: list[float],
    symbol: str = "US100",
    timestamps: list[float] | None = None,
    volumes: list[float] | None = None,
    current_volume: float | None = None,
    current_spread_pips: float | None = None,
    tick_size: float | None = None,
    pip_size: float | None = None,
) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        prices=prices,
        timestamps=timestamps or [],
        volumes=volumes or [],
        current_volume=current_volume,
        current_spread_pips=current_spread_pips,
        tick_size=tick_size,
        pip_size=pip_size,
    )


def _ts(hhmm: str) -> float:
    hour_text, minute_text = hhmm.split(":", 1)
    dt = datetime(2026, 1, 15, int(hour_text), int(minute_text), tzinfo=timezone.utc)
    return dt.timestamp()


def test_available_strategies_contains_trend_following():
    assert "g1" in available_strategies()
    assert "trend_following" in available_strategies()
    assert "crypto_trend_following" in available_strategies()
    assert "donchian_breakout" in available_strategies()
    assert "index_hybrid" in available_strategies()
    assert "mean_breakout_v2" in available_strategies()
    assert "mean_reversion_bb" in available_strategies()
    assert "momentum_index" in available_strategies()
    assert "momentum_fx" in available_strategies()


def test_g1_holds_in_kama_chop_regime():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
                "g1_atr_window": 3,
                "g1_max_price_ema_gap_ratio": 10.0,
                "g1_volume_confirmation": False,
                "g1_entry_mode": "cross_or_trend",
                "g1_kama_gate_enabled": True,
                "g1_kama_er_window": 4,
                "g1_kama_min_efficiency_ratio": 0.8,
            "g1_kama_min_slope_atr_ratio": 0.5,
        },
    )
    prices = [1.0, 0.99, 0.98, 0.97, 0.96, 1.05, 1.04, 1.03]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    assert signal.side != Side.HOLD
    assert "kama_chop_regime" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("kama_softened") is True
    assert float(signal.metadata.get("kama_penalty") or 0.0) > 0.0


def test_trend_following_holds_in_kama_chop_regime():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_index_min_stop_pct": 0.0,
            "trend_kama_gate_enabled": True,
            "trend_kama_er_window": 4,
            "trend_kama_min_efficiency_ratio": 0.8,
            "trend_kama_min_slope_atr_ratio": 0.5,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert "kama_chop_regime" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("kama_softened") is True
    assert float(signal.metadata.get("kama_penalty") or 0.0) > 0.0


def test_mean_reversion_bb_blocks_buy_without_vwap_reclaim():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_session_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": False,
            "mean_reversion_bb_vwap_filter_enabled": True,
            "mean_reversion_bb_vwap_reclaim_required": True,
            "mean_reversion_bb_vwap_min_session_bars": 5,
            "mean_reversion_bb_vwap_min_volume_samples": 5,
            "mean_reversion_bb_resample_mode": "off",
        },
    )
    prices = [100.0, 100.2, 100.1, 99.9, 99.8, 96.0]
    timestamps = [float(1_775_000_000 + idx * 60) for idx in range(len(prices))]
    volumes = [100.0] * len(prices)
    signal = strategy.generate_signal(
        _ctx(
            prices,
            symbol="US100",
            timestamps=timestamps,
            volumes=volumes,
            current_spread_pips=0.5,
            tick_size=1.0,
            pip_size=1.0,
        )
    )
    assert signal.side == Side.BUY
    assert "vwap_reclaim_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("vwap_softened") is True
    assert signal.metadata.get("vwap_status") == "ok"


def test_crypto_trend_following_supports_only_crypto_symbols():
    strategy = create_strategy("crypto_trend_following", {})
    assert strategy.supports_symbol("BTC")
    assert strategy.supports_symbol("ETH")
    assert strategy.supports_symbol("SOL")
    assert strategy.supports_symbol("ADAUSD")
    assert not strategy.supports_symbol("US100")
    assert not strategy.supports_symbol("EURUSD")


def test_crypto_trend_following_uses_crypto_tuned_defaults():
    strategy = create_strategy("crypto_trend_following", {})
    assert strategy.fast_ema_window == 21  # type: ignore[attr-defined]
    assert strategy.slow_ema_window == 89  # type: ignore[attr-defined]
    assert strategy.use_donchian_filter is False  # type: ignore[attr-defined]
    assert strategy.risk_reward_ratio == pytest.approx(2.0)  # type: ignore[attr-defined]
    assert strategy.min_stop_loss_pips == pytest.approx(150.0)  # type: ignore[attr-defined]
    assert strategy.max_spread_to_stop_ratio == pytest.approx(0.6)  # type: ignore[attr-defined]
    assert strategy.pullback_bounce_required is False  # type: ignore[attr-defined]
    assert strategy.bounce_rejection_min_wick_to_body_ratio == pytest.approx(1.0)  # type: ignore[attr-defined]
    assert strategy.crypto_pullback_ema_tolerance_ratio == pytest.approx(0.02)  # type: ignore[attr-defined]


def test_crypto_trend_following_uses_ig_point_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)
    assert signal.metadata.get("pip_size_source") == "crypto_default_ig_point"


def test_crypto_trend_following_blocks_pair_symbol_without_explicit_pip_size():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "trend_crypto_allow_pair_default_pip_size": False,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTCUSD", tick_size=None, pip_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pip_size_unavailable"
    assert signal.metadata.get("pip_size_source") == "crypto_pair_fallback_blocked"


def test_crypto_trend_following_prefers_context_tick_size_when_available():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=0.5))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.5)
    assert signal.metadata.get("pip_size_source") == "context_tick_size"


def test_crypto_trend_following_prefers_context_pip_size_over_tick_size():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=0.5, pip_size=2.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(2.0)
    assert signal.metadata.get("pip_size_source") == "context_pip_size"


def test_crypto_trend_following_drops_non_finite_prices_before_atr():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
        },
    )
    prices = [
        100000.0,
        99000.0,
        98000.0,
        99000.0,
        100000.0,
        101000.0,
        103000.0,
        float("nan"),
        102200.0,
        102400.0,
    ]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=1.0))
    assert signal.metadata.get("reason") != "crypto_atr_unavailable"


def test_crypto_trend_following_blocks_when_spread_is_too_large_relative_to_stop():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "trend_crypto_max_stop_loss_atr": 0.0,
            "trend_crypto_min_stop_pct": 0.0,
            "trend_min_stop_loss_pips": 20.0,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", current_spread_pips=30.0, tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_high_relative_to_stop"


def test_crypto_trend_following_spread_ratio_override_by_symbol():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "trend_crypto_max_stop_loss_atr": 0.0,
            "trend_crypto_min_stop_pct": 0.0,
            "trend_min_stop_loss_pips": 20.0,
            "trend_max_spread_to_stop_ratio": 0.5,
            "trend_max_spread_to_stop_ratio_by_symbol": {"BTC": 2.0},
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", current_spread_pips=30.0, tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_max_spread_to_stop_ratio") == pytest.approx(2.0)
    assert signal.metadata.get("trend_max_spread_to_stop_ratio_source") == "symbol_override"


def test_trend_following_applies_spread_buffer_to_stop_loss():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_index_min_stop_pct": 0.0,
            "trend_spread_buffer_factor": 1.0,
            "trend_max_spread_to_stop_ratio": 0.0,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    base = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, current_spread_pips=0.0))
    with_spread = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, current_spread_pips=5.0))
    assert base.side == Side.BUY
    assert with_spread.side == Side.BUY
    assert with_spread.stop_loss_pips == pytest.approx(base.stop_loss_pips + 5.0)


def test_trend_following_blocks_when_absolute_spread_limit_is_exceeded():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_index_min_stop_pct": 0.0,
            "trend_max_spread_pips": 4.0,
            "trend_max_spread_to_stop_ratio": 0.0,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, current_spread_pips=5.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"
    assert signal.metadata.get("trend_max_spread_pips") == pytest.approx(4.0)
    assert signal.metadata.get("trend_max_spread_pips_source") == "global"


def test_trend_following_absolute_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_index_min_stop_pct": 0.0,
            "trend_max_spread_pips": 2.0,
            "trend_max_spread_pips_by_symbol": {"US100": 5.0},
            "trend_max_spread_to_stop_ratio": 0.0,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    blocked = strategy.generate_signal(_ctx(prices, symbol="DE40", tick_size=0.1, current_spread_pips=3.0))
    assert blocked.side == Side.HOLD
    assert blocked.metadata.get("reason") == "spread_too_wide"
    assert blocked.metadata.get("trend_max_spread_pips_source") == "global"

    allowed = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, current_spread_pips=3.0))
    assert allowed.side == Side.BUY
    assert allowed.metadata.get("trend_max_spread_pips") == pytest.approx(5.0)
    assert allowed.metadata.get("trend_max_spread_pips_source") == "symbol_override"


def test_trend_following_caps_stop_loss_by_atr_when_configured():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.01,
            "trend_atr_window": 3,
            "trend_min_stop_loss_pips": 1.0,
            "trend_index_min_stop_pct": 0.0,
            "trend_max_stop_loss_atr": 0.1,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    atr = float(signal.metadata.get("atr") or 0.0)
    pip_size = float(signal.metadata.get("pip_size") or 1.0)
    atr_cap_pips = (atr * 0.1) / max(pip_size, 1e-9)
    assert signal.stop_loss_pips <= (atr_cap_pips + 1e-9)


def test_trend_following_blocks_signal_on_large_timestamp_gap():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "trend_max_timestamp_gap_sec": 3600.0,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    timestamps = [float(idx * 60) for idx in range(len(prices) - 1)] + [8000.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps, tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "timestamp_gap_too_wide"


def test_trend_following_normalizes_millisecond_timestamps_for_gap_check():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "trend_max_timestamp_gap_sec": 3600.0,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    base_ts_ms = 1_700_000_000_000.0
    timestamps_ms = [base_ts_ms + (idx * 60_000.0) for idx in range(len(prices))]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps_ms, tick_size=1.0))
    assert signal.metadata.get("reason") != "insufficient_price_history"
    assert signal.metadata.get("reason") != "timestamp_gap_too_wide"


def test_crypto_trend_following_scales_min_atr_threshold_by_sampling_interval():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": False,
            "trend_crypto_min_atr_pct": 0.20,
            "trend_crypto_min_atr_pct_baseline_sec": 60.0,
            "trend_crypto_min_atr_pct_min_ratio": 0.25,
            "trend_crypto_min_atr_pct_scale_mode": "linear",
            "trend_crypto_min_ema_gap_ratio": 0.0,
            "trend_crypto_min_fast_slope_ratio": 0.0,
            "trend_crypto_min_slow_slope_ratio": 0.0,
            "trend_pullback_bounce_required": False,
            "trend_pullback_max_distance_ratio": 0.05,
            "trend_pullback_max_distance_atr": 0.0,
            "trend_crypto_max_pullback_distance_atr": 0.0,
            "trend_resample_mode": "off",
            "trend_crypto_min_stop_pct": 0.0,
            "trend_crypto_max_stop_loss_atr": 0.0,
            "trend_min_stop_loss_pips": 1.0,
            "trend_min_take_profit_pips": 1.0,
            "trend_risk_reward_ratio": 1.0,
        },
    )
    prices = [
        100000.0,
        100150.0,
        100080.0,
        100220.0,
        100140.0,
        100300.0,
        100210.0,
        100360.0,
        100280.0,
        100420.0,
        100350.0,
        100500.0,
    ]
    timestamps_60s = [1_700_000_000.0 + (idx * 60.0) for idx in range(len(prices))]
    timestamps_15s = [1_700_000_000.0 + (idx * 15.0) for idx in range(len(prices))]

    signal_60s = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps_60s, tick_size=1.0))
    signal_15s = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps_15s, tick_size=1.0))

    assert signal_60s.side == Side.HOLD
    assert signal_60s.metadata.get("reason") == "crypto_atr_below_threshold"
    assert signal_60s.metadata.get("effective_crypto_min_atr_pct") == pytest.approx(0.20)

    assert signal_15s.metadata.get("effective_crypto_min_atr_pct") == pytest.approx(0.05)
    assert signal_15s.metadata.get("reason") != "crypto_atr_below_threshold"


def test_crypto_trend_following_deduplicates_equal_prices_for_atr():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": False,
            "trend_crypto_min_atr_pct": 0.20,
            "trend_crypto_min_atr_pct_baseline_sec": 60.0,
            "trend_crypto_min_atr_pct_min_ratio": 0.25,
            "trend_crypto_min_atr_pct_scale_mode": "linear",
            "trend_crypto_min_ema_gap_ratio": 0.0,
            "trend_crypto_min_fast_slope_ratio": 0.0,
            "trend_crypto_min_slow_slope_ratio": 0.0,
            "trend_pullback_bounce_required": False,
            "trend_pullback_max_distance_ratio": 0.05,
            "trend_pullback_max_distance_atr": 0.0,
            "trend_crypto_max_pullback_distance_atr": 0.0,
            "trend_resample_mode": "off",
            "trend_crypto_min_stop_pct": 0.0,
            "trend_crypto_max_stop_loss_atr": 0.0,
            "trend_min_stop_loss_pips": 1.0,
            "trend_min_take_profit_pips": 1.0,
            "trend_risk_reward_ratio": 1.0,
        },
    )
    base_prices = [100000.0, 100150.0, 100080.0, 100220.0, 100140.0, 100300.0, 100210.0, 100360.0, 100280.0, 100420.0]
    prices: list[float] = []
    for value in base_prices:
        prices.extend([value, value])
    timestamps = [1_700_000_000.0 + (idx * 15.0) for idx in range(len(prices))]

    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps, tick_size=1.0))
    assert signal.metadata.get("reason") != "crypto_atr_unavailable"
    assert signal.metadata.get("atr_price_source") == "dedup_consecutive_equal"


def test_crypto_trend_following_zero_atr_is_treated_as_below_threshold():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": False,
            "trend_crypto_min_atr_pct": 0.20,
            "trend_pullback_bounce_required": False,
        },
    )
    prices = [100000.0] * 24
    timestamps = [1_700_000_000.0 + (idx * 60.0) for idx in range(len(prices))]

    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps, tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "crypto_atr_below_threshold"
    assert signal.metadata.get("atr_pct") == pytest.approx(0.0)


def test_crypto_trend_following_min_history_includes_atr_window_requirement():
    strategy = create_strategy(
        "crypto_trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 14,
        },
    )
    prices = [100000.0 + float(idx) for idx in range(10)]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "insufficient_price_history"
    assert signal.metadata.get("required_history") == strategy.min_history


def test_trend_following_generates_buy_on_uptrend_pullback():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "stop_loss_pips": 30,
            "take_profit_pips": 75,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips >= 30.0
    assert signal.take_profit_pips >= 75.0
    assert signal.metadata.get("entry_type") == "pullback"
    assert signal.metadata.get("indicator") == "ema_trend_following"


def test_trend_following_generates_sell_on_downtrend_pullback():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
        },
    )
    signal = strategy.generate_signal(_ctx([104.0, 105.0, 106.0, 105.0, 104.0, 103.0, 101.0, 102.0, 102.5, 102.2]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("entry_type") == "pullback"
    assert signal.metadata.get("indicator") == "ema_trend_following"


def test_trend_following_blocks_micro_bounce_pullback_noise():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.01,
            "trend_pullback_bounce_required": True,
            "trend_pullback_bounce_min_retrace_atr_ratio": 0.08,
            "trend_index_min_stop_pct": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.6, 102.2, 102.22], symbol="US100", tick_size=0.1)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_not_confirmed"
    assert signal.metadata.get("bounce_directional_close_confirmed") is True
    assert signal.metadata.get("bounce_local_extreme_confirmed") is True
    assert signal.metadata.get("bounce_retrace_confirmed") is False


def test_trend_following_holds_in_flat_market():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "use_donchian_filter": True,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]))
    assert signal.side == Side.HOLD


def test_trend_following_disabling_donchian_filter_allows_trend_pullback_entry():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 4,
            "trend_breakout_lookback_bars": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": False,
            "trend_pullback_max_distance_ratio": 0.05,
            "trend_pullback_ema_tolerance_ratio": 0.01,
            "trend_pullback_bounce_required": False,
            "trend_min_stop_loss_pips": 10,
            "trend_min_take_profit_pips": 20,
            "trend_risk_reward_ratio": 2.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx(
            [100.0, 101.0, 102.0, 103.0, 104.0, 103.6, 103.4, 103.5, 103.45, 103.5],
            tick_size=0.1,
            pip_size=0.1,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("entry_type") == "pullback"
    assert signal.metadata.get("recent_breakout") is False
    assert signal.metadata.get("use_donchian_filter_configured") is False
    assert signal.metadata.get("donchian_filter_applied") is False


def test_trend_following_resamples_snapshots_to_time_normalized_candles():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_breakout_lookback_bars": 3,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_candle_timeframe_sec": 60,
            "trend_resample_mode": "always",
            "trend_ignore_sunday_candles": True,
            "stop_loss_pips": 30,
            "take_profit_pips": 75,
        },
    )
    candle_closes = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    base_ts_seed = 1_700_000_000.0
    base_ts = base_ts_seed - (base_ts_seed % 60.0)
    candle_timestamps = [base_ts + (idx * 60.0) + 59.0 for idx in range(len(candle_closes))]
    candle_resampled, candle_resampled_ts, candle_using_closed = strategy._resample_closed_candle_closes(  # type: ignore[attr-defined]
        candle_closes,
        candle_timestamps,
    )

    snapshot_prices: list[float] = []
    snapshot_timestamps: list[float] = []
    previous_close = candle_closes[0]
    for idx, close in enumerate(candle_closes):
        bucket_start = base_ts + (idx * 60.0)
        for step in range(1, 13):
            snapshot_prices.append(previous_close + ((close - previous_close) * (step / 12.0)))
            snapshot_timestamps.append(bucket_start + (step * 5.0) - 1.0)
        previous_close = close
    snapshot_resampled, snapshot_resampled_ts, snapshot_using_closed = strategy._resample_closed_candle_closes(  # type: ignore[attr-defined]
        snapshot_prices,
        snapshot_timestamps,
    )

    assert candle_using_closed is True
    assert snapshot_using_closed is True
    assert snapshot_resampled == pytest.approx(candle_resampled)
    assert snapshot_resampled_ts == pytest.approx(candle_resampled_ts)


def test_trend_following_live_candle_preview_can_cut_one_bar_of_entry_lag():
    base_params = {
        "fast_ema_window": 3,
        "slow_ema_window": 5,
        "donchian_window": 3,
        "trend_atr_window": 3,
        "use_donchian_filter": False,
        "trend_breakout_lookback_bars": 3,
        "trend_pullback_max_distance_ratio": 0.03,
        "trend_pullback_ema_tolerance_ratio": 0.002,
        "trend_pullback_bounce_required": True,
        "trend_candle_timeframe_sec": 60,
        "trend_resample_mode": "always",
        "trend_ignore_sunday_candles": True,
        "stop_loss_pips": 30,
        "take_profit_pips": 75,
    }
    closed_only_strategy = create_strategy(
        "trend_following",
        {
            **base_params,
            "trend_live_candle_entry_enabled": False,
        },
    )
    live_strategy = create_strategy(
        "trend_following",
        {
            **base_params,
            "trend_live_candle_entry_enabled": True,
        },
    )
    candle_closes = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    prices: list[float] = []
    timestamps: list[float] = []
    base_ts = _ts("10:00")
    previous_close = candle_closes[0]
    for idx, close in enumerate(candle_closes[:-1]):
        bucket_start = base_ts + (idx * 60.0)
        for step in range(1, 13):
            prices.append(previous_close + ((close - previous_close) * (step / 12.0)))
            timestamps.append(bucket_start + (step * 5.0) - 1.0)
        previous_close = close
    final_bucket_start = base_ts + ((len(candle_closes) - 1) * 60.0)
    final_close = candle_closes[-1]
    for step in range(1, 7):
        prices.append(previous_close + ((final_close - previous_close) * (step / 6.0)))
        timestamps.append(final_bucket_start + (step * 5.0) - 1.0)

    closed_only_signal = closed_only_strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1)
    )
    live_signal = live_strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1)
    )

    assert closed_only_signal.side == Side.HOLD
    assert closed_only_signal.metadata.get("reason") == "pullback_countermove_in_progress"

    assert live_signal.side == Side.BUY
    assert live_signal.metadata.get("recent_breakout") is False
    assert live_signal.metadata.get("using_closed_candles") is True
    assert live_signal.metadata.get("using_live_candle_entry") is True


def test_trend_following_auto_resample_excludes_last_candle_for_candle_like_input():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_candle_timeframe_sec": 60,
            "trend_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0]
    timestamps = [0.0, 60.0, 120.0, 180.0]
    closes, close_timestamps, using_closed = strategy._resample_closed_candle_closes(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is True
    assert closes == pytest.approx(prices[:-1])
    assert close_timestamps == pytest.approx(timestamps[:-1])


def test_trend_following_auto_resample_uses_raw_prices_for_snapshot_flow():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_candle_timeframe_sec": 60,
            "trend_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    timestamps = [0.0, 15.0, 30.0, 60.0, 75.0, 90.0]
    closes, close_timestamps, using_closed = strategy._resample_closed_candle_closes(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is False
    assert closes == pytest.approx(prices)
    assert close_timestamps == pytest.approx(timestamps)


def test_trend_following_pullback_zone_requires_bounded_distance_from_fast_ema(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_ema_tolerance_ratio": 0.001,
            "trend_pullback_max_distance_ratio": 1.0,
            "trend_pullback_max_distance_atr": 0.0,
            "trend_pullback_bounce_required": False,
            "trend_resample_mode": "off",
            "trend_index_min_ema_gap_ratio": 0.0,
            "trend_index_min_fast_slope_ratio": 0.0,
            "trend_index_min_slow_slope_ratio": 0.0,
            "trend_min_stop_loss_pips": 1.0,
            "trend_min_take_profit_pips": 1.0,
            "trend_risk_reward_ratio": 1.0,
        },
    )

    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args: (True, False))
    monkeypatch.setattr(strategy, "_slope_ratio", lambda *_args: 0.001)
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 108.0, 100.0))

    signal = strategy.generate_signal(
        _ctx(
            [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 106.6],
            symbol="US100",
            tick_size=0.1,
            pip_size=0.1,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_not_in_zone"
    assert signal.metadata.get("price_above_slow_ema") is True
    assert float(signal.metadata.get("pullback_ema_distance_ratio") or 0.0) > float(
        signal.metadata.get("pullback_ema_tolerance_ratio") or 0.0
    )


def test_trend_following_can_expand_pullback_zone_by_atr(monkeypatch: pytest.MonkeyPatch):
    base_params = {
        "fast_ema_window": 3,
        "slow_ema_window": 5,
        "donchian_window": 3,
        "trend_atr_window": 3,
        "use_donchian_filter": True,
        "trend_pullback_ema_tolerance_ratio": 0.001,
        "trend_pullback_max_distance_ratio": 1.0,
        "trend_pullback_max_distance_atr": 0.0,
        "trend_pullback_bounce_required": False,
        "trend_pullback_max_countermove_ratio": 0.01,
        "trend_resample_mode": "off",
        "trend_index_min_ema_gap_ratio": 0.0,
        "trend_index_min_fast_slope_ratio": 0.0,
        "trend_index_min_slow_slope_ratio": 0.0,
        "trend_min_stop_loss_pips": 1.0,
        "trend_min_take_profit_pips": 1.0,
        "trend_risk_reward_ratio": 1.0,
    }
    strict_strategy = create_strategy(
        "trend_following",
        {
            **base_params,
            "trend_pullback_ema_tolerance_atr_multiplier": 0.0,
        },
    )
    adaptive_strategy = create_strategy(
        "trend_following",
        {
            **base_params,
            "trend_pullback_ema_tolerance_atr_multiplier": 0.5,
        },
    )
    for strategy in (strict_strategy, adaptive_strategy):
        monkeypatch.setattr(strategy, "_trend_direction", lambda *_args, **_kwargs: (True, False))
        monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
        monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 108.5, 100.0))

    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 107.4]

    strict_signal = strict_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, pip_size=0.1))
    adaptive_signal = adaptive_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, pip_size=0.1))

    assert strict_signal.side == Side.HOLD
    assert strict_signal.metadata.get("reason") == "pullback_not_in_zone"

    assert adaptive_signal.side == Side.BUY
    assert float(adaptive_signal.metadata.get("pullback_ema_tolerance_ratio_effective") or 0.0) > float(
        adaptive_signal.metadata.get("pullback_ema_tolerance_ratio") or 0.0
    )
    assert float(adaptive_signal.metadata.get("pullback_ema_distance_ratio") or 0.0) > float(
        adaptive_signal.metadata.get("pullback_ema_tolerance_ratio") or 0.0
    )
    assert float(adaptive_signal.metadata.get("pullback_ema_distance_ratio") or 0.0) <= float(
        adaptive_signal.metadata.get("pullback_ema_tolerance_ratio_effective") or 0.0
    )


def test_trend_following_pullback_distance_gate_does_not_use_channel_mid_min(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_ema_tolerance_ratio": 0.2,
            "trend_pullback_max_distance_ratio": 0.002,
            "trend_pullback_max_distance_atr": 0.0,
            "trend_pullback_bounce_required": False,
            "trend_resample_mode": "off",
            "trend_index_min_ema_gap_ratio": 0.0,
            "trend_index_min_fast_slope_ratio": 0.0,
            "trend_index_min_slow_slope_ratio": 0.0,
            "trend_min_stop_loss_pips": 1.0,
            "trend_min_take_profit_pips": 1.0,
            "trend_risk_reward_ratio": 1.0,
        },
    )

    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args: (True, False))
    monkeypatch.setattr(strategy, "_slope_ratio", lambda *_args: 0.001)
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(
        strategy,
        "_donchian_breakout",
        # Channel mid is intentionally aligned with last price to prove it does not relax gate.
        lambda prices: (True, False, float(prices[-1]) + 0.4, float(prices[-1]) - 0.4),
    )

    signal = strategy.generate_signal(
        _ctx(
            [100.5284, 102.2691, 103.8675, 104.6011, 105.7433, 106.8075, 108.2152, 109.856, 110.3156, 110.6638, 112.3846, 113.4203, 111.4908],
            symbol="US100",
            tick_size=0.1,
            pip_size=0.1,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_pullback_entry"
    assert float(signal.metadata.get("pullback_distance_ratio") or 0.0) == pytest.approx(
        float(signal.metadata.get("pullback_ema_distance_ratio") or 0.0)
    )
    assert signal.metadata.get("pullback_channel_mid_distance_ratio") == pytest.approx(0.0)


def test_trend_following_blocks_countermove_even_when_bounce_filter_is_disabled(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_ema_tolerance_ratio": 0.2,
            "trend_pullback_max_distance_ratio": 1.0,
            "trend_pullback_max_distance_atr": 0.0,
            "trend_pullback_bounce_required": False,
            "trend_pullback_max_countermove_ratio": 0.0,
            "trend_resample_mode": "off",
            "trend_min_stop_loss_pips": 1.0,
            "trend_min_take_profit_pips": 1.0,
            "trend_risk_reward_ratio": 1.0,
        },
    )

    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args: (True, False))
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 108.0, 100.0))

    signal = strategy.generate_signal(
        _ctx(
            [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 107.8],
            symbol="US100",
            tick_size=0.1,
            pip_size=0.1,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_countermove_in_progress"
    assert signal.metadata.get("trend_pullback_bounce_required") is False
    assert float(signal.metadata.get("pullback_countermove_ratio") or 0.0) > 0.0
    assert signal.metadata.get("trend_pullback_max_countermove_ratio") == pytest.approx(0.0)


def test_trend_following_uses_channel_based_stop_when_wider_than_minimum():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.01,
            "trend_min_stop_loss_pips": 30.0,
            "trend_index_min_stop_pct": 0.0,
            "trend_entry_stop_invalidation_enabled": False,
            "trend_risk_reward_ratio": 2.5,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(200.0)
    assert signal.take_profit_pips == pytest.approx(500.0)


def test_trend_following_caps_channel_stop_with_entry_invalidation():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.01,
            "trend_min_stop_loss_pips": 30.0,
            "trend_index_min_stop_pct": 0.0,
            "trend_entry_stop_invalidation_enabled": True,
            "trend_entry_stop_invalidation_atr_buffer": 0.0,
            "trend_risk_reward_ratio": 2.5,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("entry_stop_invalidation_applied") is True
    structure_stop_pips = float(signal.metadata.get("structure_stop_pips") or 0.0)
    entry_stop_pips = float(signal.metadata.get("entry_stop_invalidation_pips") or 0.0)
    assert structure_stop_pips == pytest.approx(200.0)
    assert entry_stop_pips < structure_stop_pips
    assert signal.stop_loss_pips == pytest.approx(max(30.0, entry_stop_pips))
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 2.5)


def test_crypto_trend_following_can_disable_entry_stop_invalidation():
    base_params = {
        "fast_ema_window": 3,
        "slow_ema_window": 5,
        "donchian_window": 4,
        "trend_atr_window": 3,
        "trend_entry_stop_invalidation_enabled": True,
        "trend_entry_stop_invalidation_atr_buffer": 0.0,
        "trend_crypto_min_stop_pct": 0.0,
        "trend_crypto_max_stop_loss_atr": 0.0,
        "trend_min_stop_loss_pips": 20.0,
    }
    disabled_strategy = create_strategy(
        "crypto_trend_following",
        {
            **base_params,
            "trend_crypto_entry_stop_invalidation_enabled": False,
        },
    )
    enabled_strategy = create_strategy(
        "crypto_trend_following",
        {
            **base_params,
            "trend_crypto_entry_stop_invalidation_enabled": True,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]

    disabled_signal = disabled_strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=1.0))
    enabled_signal = enabled_strategy.generate_signal(_ctx(prices, symbol="BTC", tick_size=1.0))

    assert disabled_signal.side == Side.BUY
    assert enabled_signal.side == Side.BUY
    assert disabled_signal.metadata.get("trend_entry_stop_invalidation_enabled") is True
    assert disabled_signal.metadata.get("trend_crypto_entry_stop_invalidation_enabled") is False
    assert disabled_signal.metadata.get("trend_entry_stop_invalidation_enabled_effective") is False
    assert disabled_signal.metadata.get("entry_stop_invalidation_applied") is False
    assert enabled_signal.metadata.get("trend_entry_stop_invalidation_enabled_effective") is True
    assert enabled_signal.metadata.get("entry_stop_invalidation_applied") is True
    assert disabled_signal.stop_loss_pips > enabled_signal.stop_loss_pips


def test_trend_following_clips_index_price_floor_to_entry_invalidation_ratio():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.01,
            "trend_min_stop_loss_pips": 20.0,
            "trend_index_min_stop_pct": 0.05,
            "trend_index_min_stop_pct_max_entry_ratio": 1.0,
            "trend_entry_stop_invalidation_enabled": True,
            "trend_entry_stop_invalidation_atr_buffer": 0.0,
            "trend_risk_reward_ratio": 2.3,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    raw_floor = float(signal.metadata.get("index_stop_pct_raw_pips") or 0.0)
    effective_floor = float(signal.metadata.get("index_stop_pct_effective_pips") or 0.0)
    entry_stop = float(signal.metadata.get("entry_stop_invalidation_pips") or 0.0)
    assert raw_floor > entry_stop
    assert effective_floor == pytest.approx(entry_stop)
    assert signal.metadata.get("index_stop_pct_clipped_by_entry_ratio") is True
    assert signal.stop_loss_pips == pytest.approx(max(20.0, effective_floor))
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 2.3)


def test_trend_following_requires_pullback_zone_for_entry():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.001,
        },
    )
    # Up-trend + recent breakout are present, but price is outside pullback zone.
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_not_in_zone"


def test_trend_following_allows_runaway_continuation_entry_without_ema_touch():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 4,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.0005,
            "trend_runaway_entry_enabled": True,
            "trend_runaway_max_distance_atr": 1.2,
            "trend_index_min_stop_pct": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 99.0, 98.0, 99.0, 100.0, 101.5, 103.0, 104.0, 104.6, 104.9], symbol="US100", tick_size=0.1)
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("entry_type") == "runaway_continuation"
    assert signal.metadata.get("runaway_entry_allowed") is True
    assert signal.metadata.get("pullback_ema_distance_ratio", 0.0) > signal.metadata.get(
        "pullback_ema_tolerance_ratio", 0.0
    )


def test_trend_following_runaway_entry_still_blocks_when_too_extended():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 4,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.0005,
            "trend_runaway_entry_enabled": True,
            "trend_runaway_max_distance_atr": 0.1,
            "trend_index_min_stop_pct": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 99.0, 98.0, 99.0, 100.0, 101.5, 103.0, 104.0, 104.6, 104.9], symbol="US100", tick_size=0.1)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_not_in_zone"
    assert signal.metadata.get("runaway_entry_allowed") is False
    assert signal.metadata.get("runaway_distance_ok") is False


def test_trend_following_strong_runaway_trend_extends_distance_and_removes_penalty():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_runaway_entry_enabled": True,
            "trend_runaway_max_distance_atr": 0.9,
            "trend_runaway_adx_window": 14,
            "trend_runaway_strong_trend_min_adx": 30.0,
            "trend_runaway_strong_trend_distance_atr": 1.2,
            "trend_runaway_confidence_penalty": 0.08,
            "trend_runaway_strong_trend_confidence_penalty": 0.0,
            "trend_strength_norm_ratio": 0.0025,
            "trend_confidence_velocity_norm_ratio": 0.0005,
            "trend_confidence_gap_velocity_positive_threshold": 0.20,
        },
    )
    allowed, payload = strategy._runaway_entry_allows(  # type: ignore[attr-defined]
        trend="up",
        prices=[100.0, 101.0, 102.0],
        last_price=102.0,
        prev_price=101.0,
        fast_now=101.0,
        fast_prev=100.5,
        ema_gap_ratio=0.004,
        directional_velocity_ratio=0.001,
        effective_min_ema_gap_ratio=0.0002,
        effective_min_fast_slope_ratio=0.00003,
        effective_min_slow_slope_ratio=0.00001,
        pullback_distance_atr=1.0,
        max_pullback_distance_atr=1.5,
        atr=1.0,
        channel_upper=101.6,
        channel_lower=99.0,
        adx=35.0,
        ema_gap_velocity_ratio=0.25,
    )
    assert allowed is True
    assert payload.get("runaway_strong_trend") is True
    assert payload.get("runaway_distance_limit_atr") == pytest.approx(1.2)
    assert payload.get("runaway_confidence_penalty") == pytest.approx(0.0)


def test_trend_following_index_mature_trend_bonus_lifts_index_confidence():
    without_bonus = create_strategy(
        "trend_following",
        {
            "trend_index_mature_trend_confidence_bonus": 0.0,
        },
    )
    with_bonus = create_strategy(
        "trend_following",
        {
            "trend_index_mature_trend_confidence_bonus": 0.05,
        },
    )

    confidence_kwargs = {
        "ema_gap_ratio": 0.004,
        "pullback_distance_ratio": 0.01,
        "overextension_ratio": 0.001,
        "bounce_confirmed": False,
        "directional_velocity_ratio": 0.0005,
        "price_aligned_with_fast_ma": True,
        "entry_type": "runaway_continuation",
        "trend_profile": "index",
        "ema_gap_velocity_ratio": 0.25,
        "runaway_confidence_penalty": 0.08,
    }

    assert with_bonus._index_mature_trend_confidence_bonus(  # type: ignore[attr-defined]
        trend_profile="index",
        ema_gap_ratio=confidence_kwargs["ema_gap_ratio"],
        directional_velocity_ratio=confidence_kwargs["directional_velocity_ratio"],
        ema_gap_velocity_ratio=confidence_kwargs["ema_gap_velocity_ratio"],
        overextension_ratio=confidence_kwargs["overextension_ratio"],
    ) == pytest.approx(0.05)
    assert with_bonus._confidence(**confidence_kwargs) > without_bonus._confidence(**confidence_kwargs)  # type: ignore[attr-defined]


def test_trend_following_can_require_tick_size_from_context():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_require_context_tick_size": True,
        },
    )
    prices = [100.0 + float(idx) for idx in range(120)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_trend_following_slope_mode_allows_small_slow_ema_lag():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_slope_mode": "fast_with_slow_tolerance",
            "trend_slow_slope_tolerance_ratio": 0.0003,
        },
    )
    trend_up, trend_down = strategy._trend_direction(105.0, 100.0, 104.0, 100.03)  # type: ignore[attr-defined]
    assert trend_up is True
    assert trend_down is False


def test_trend_following_slope_mode_fast_and_slow_blocks_slow_ema_lag():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_slope_mode": "fast_and_slow",
        },
    )
    trend_up, trend_down = strategy._trend_direction(105.0, 100.0, 104.0, 100.03)  # type: ignore[attr-defined]
    assert trend_up is False
    assert trend_down is False


def test_trend_following_treats_aus200_as_index_profile(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_index_min_ema_gap_ratio": 0.5,
            "trend_index_min_fast_slope_ratio": 0.0,
            "trend_index_min_slow_slope_ratio": 0.0,
            "trend_fx_min_ema_gap_ratio": 0.0,
            "trend_resample_mode": "off",
        },
    )
    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args: (True, False))
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 101.0, 99.0))

    signal = strategy.generate_signal(
        _ctx(
            [9000.0, 9005.0, 9008.0, 9010.0, 9012.0, 9015.0, 9017.0, 9018.0, 9019.0, 9020.0],
            symbol="AUS200",
            tick_size=1.0,
            pip_size=1.0,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pullback_not_confirmed"
    profile, *_ = strategy._non_crypto_strength_thresholds("AUS200")  # type: ignore[attr-defined]
    assert profile == "index"


def test_trend_following_caps_slow_slope_tolerance_for_non_crypto():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_slope_mode": "fast_with_slow_tolerance",
            "trend_slow_slope_tolerance_ratio": 0.0008,
            "trend_slow_slope_tolerance_ratio_cap_non_crypto": 0.0001,
        },
    )
    uncapped_up, _ = strategy._trend_direction(105.0, 100.0, 104.0, 100.03)  # type: ignore[attr-defined]
    capped_up, _ = strategy._trend_direction(  # type: ignore[attr-defined]
        105.0,
        100.0,
        104.0,
        100.03,
        slow_slope_tolerance_ratio=0.0001,
    )
    assert uncapped_up is True
    assert capped_up is False


def test_trend_following_blocks_non_crypto_trend_with_weak_gap(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_ema_tolerance_ratio": 0.2,
            "trend_pullback_max_distance_ratio": 1.0,
            "trend_pullback_bounce_required": False,
            "trend_index_min_ema_gap_ratio": 0.5,
            "trend_index_min_fast_slope_ratio": 0.0,
            "trend_index_min_slow_slope_ratio": 0.0,
            "trend_resample_mode": "off",
        },
    )
    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args, **_kwargs: (True, False))
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 101.0, 99.0))

    signal = strategy.generate_signal(
        _ctx(
            [100.0, 100.05, 100.08, 100.1, 100.12, 100.15, 100.17, 100.18, 100.19, 100.2],
            symbol="US100",
            tick_size=0.1,
            pip_size=0.1,
        )
    )
    assert signal.side == Side.BUY
    assert "trend_too_weak" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("trend_gap_softened") is True
    assert signal.metadata.get("trend_profile") == "index"


def test_trend_following_blocks_non_crypto_trend_with_weak_slopes(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_ema_tolerance_ratio": 0.2,
            "trend_pullback_max_distance_ratio": 1.0,
            "trend_pullback_bounce_required": False,
            "trend_fx_min_ema_gap_ratio": 0.0,
            "trend_fx_min_fast_slope_ratio": 0.001,
            "trend_fx_min_slow_slope_ratio": 0.001,
            "trend_resample_mode": "off",
        },
    )
    monkeypatch.setattr(strategy, "_trend_direction", lambda *_args, **_kwargs: (True, False))
    monkeypatch.setattr(strategy, "_recent_donchian_breakout", lambda _prices: (True, False))
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 1.102, 1.098))

    signal = strategy.generate_signal(
        _ctx(
            [1.10000, 1.10003, 1.10005, 1.10007, 1.10009, 1.10010, 1.10012, 1.10013, 1.10014, 1.10015],
            symbol="EURUSD",
            tick_size=0.0001,
            pip_size=0.0001,
        )
    )
    assert signal.side == Side.BUY
    assert "trend_slope_too_weak" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("trend_slope_softened") is True
    assert signal.metadata.get("trend_profile") == "fx"


def test_trend_following_index_pip_size_fallback_matches_index_step():
    strategy = create_strategy("trend_following", {})
    assert strategy._pip_size("DE40") == pytest.approx(0.1)  # type: ignore[attr-defined]
    assert strategy._pip_size("US100") == pytest.approx(0.1)  # type: ignore[attr-defined]
    assert strategy._pip_size("AUS200") == pytest.approx(1.0)  # type: ignore[attr-defined]
    assert strategy._pip_size("GOLD") == pytest.approx(0.1)  # type: ignore[attr-defined]


def test_trend_following_blocks_when_volume_spike_is_required():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_volume_confirmation": True,
            "trend_volume_window": 5,
            "trend_volume_min_samples": 3,
            "trend_min_volume_ratio": 1.4,
            "trend_volume_require_spike": True,
            "trend_volume_allow_missing": False,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    volumes = [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 101.0, 115.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes))
    assert signal.side == Side.BUY
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_trend_following_volume_spike_boosts_confidence():
    base_params = {
        "fast_ema_window": 3,
        "slow_ema_window": 5,
        "donchian_window": 3,
        "trend_atr_window": 3,
        "use_donchian_filter": True,
        "trend_pullback_max_distance_ratio": 0.03,
        "trend_pullback_ema_tolerance_ratio": 0.002,
        "trend_volume_window": 5,
        "trend_volume_min_samples": 3,
        "trend_min_volume_ratio": 1.4,
        "trend_volume_allow_missing": True,
        "trend_volume_confidence_boost": 0.2,
    }
    base_strategy = create_strategy("trend_following", base_params)
    boosted_strategy = create_strategy(
        "trend_following",
        {
            **base_params,
            "trend_volume_confirmation": True,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4]
    volumes = [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 103.0, 97.0, 101.0, 180.0]
    base_signal = base_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes))
    boosted_signal = boosted_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes))
    assert base_signal.side == Side.BUY
    assert boosted_signal.side == Side.BUY
    assert boosted_signal.confidence > base_signal.confidence
    assert boosted_signal.metadata.get("volume_spike") is True


def test_trend_following_volume_confirmation_deduplicates_current_volume_tail_sample():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_volume_confirmation": True,
            "trend_volume_allow_missing": False,
            "trend_volume_require_spike": False,
            "trend_volume_window": 5,
            "trend_volume_min_samples": 1,
        },
    )
    metadata, reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 10, volumes=[100.0, 100.0, 200.0], current_volume=200.0),
    )
    assert reason is None
    assert boost > 0.0
    assert metadata.get("volume_current_source") == "volumes"
    assert metadata.get("volume_current_deduplicated") is True
    assert metadata.get("volume_ratio") == pytest.approx(2.0)


def test_trend_following_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_volume_confirmation": True,
            "trend_volume_allow_missing": False,
            "trend_volume_require_spike": False,
            "trend_volume_window": 5,
            "trend_volume_min_samples": 3,
        },
    )
    metadata, reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 10, symbol="IT40", volumes=[], current_volume=None),
    )
    assert reason is None
    assert boost == pytest.approx(0.0)
    assert metadata.get("volume_allow_missing_effective") is True
    assert metadata.get("volume_missing_relaxed_for_index") is True


def test_trend_following_smoothed_slope_ratio_ignores_single_bar_wobble():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_slope_window": 4,
        },
    )
    ema_series = [100.0, 100.2, 100.4, 100.6, 100.55]
    raw = strategy._slope_ratio(ema_series[-1], ema_series[-2])  # type: ignore[attr-defined]
    smoothed = strategy._smoothed_slope_ratio(ema_series)  # type: ignore[attr-defined]
    assert raw < 0.0
    assert smoothed is not None
    assert smoothed > 0.0


def test_trend_following_bounce_rejection_confirms_bullish_pinbar_below_prev_close():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_bounce_rejection_enabled": True,
            "trend_bounce_rejection_min_wick_to_range_ratio": 0.30,
            "trend_bounce_rejection_min_wick_to_body_ratio": 1.0,
            "trend_bounce_rejection_buy_min_close_location": 0.50,
        },
    )
    confirmed, payload = strategy._pullback_bounce_confirmation(  # type: ignore[attr-defined]
        [100.2, 99.8, 99.75],
        trend="up",
        atr=1.0,
        open_price=99.95,
        high_price=100.0,
        low_price=99.2,
        close_price=99.75,
    )
    assert confirmed is True
    assert payload.get("bounce_price_action_confirmed") is False
    assert payload.get("bounce_rejection_confirmed") is True
    assert payload.get("bounce_rejection_status") == "ok"


def test_trend_following_bounce_rejection_blocks_bearish_continuation_candle():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_bounce_rejection_enabled": True,
            "trend_pullback_bounce_min_retrace_atr_ratio": 0.05,
            "trend_bounce_rejection_min_wick_to_range_ratio": 0.30,
            "trend_bounce_rejection_min_wick_to_body_ratio": 1.0,
            "trend_bounce_rejection_buy_min_close_location": 0.50,
        },
    )
    confirmed, payload = strategy._pullback_bounce_confirmation(  # type: ignore[attr-defined]
        [100.5, 99.0, 99.1],
        trend="up",
        atr=1.0,
        open_price=100.2,
        high_price=100.25,
        low_price=99.05,
        close_price=99.1,
    )
    assert confirmed is False
    assert payload.get("bounce_price_action_confirmed") is True
    assert payload.get("bounce_rejection_confirmed") is False
    assert payload.get("bounce_continuation_candle_blocked") is True


def test_trend_following_confidence_rewards_velocity_and_price_alignment():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_strength_norm_ratio": 0.003,
            "trend_confidence_velocity_norm_ratio": 0.0005,
        },
    )
    base_confidence = strategy._confidence(  # type: ignore[attr-defined]
        ema_gap_ratio=0.002,
        pullback_distance_ratio=0.002,
        overextension_ratio=0.0002,
        bounce_confirmed=False,
        directional_velocity_ratio=0.0001,
        price_aligned_with_fast_ma=False,
    )
    boosted_confidence = strategy._confidence(  # type: ignore[attr-defined]
        ema_gap_ratio=0.002,
        pullback_distance_ratio=0.002,
        overextension_ratio=0.0002,
        bounce_confirmed=True,
        directional_velocity_ratio=0.0012,
        price_aligned_with_fast_ma=True,
    )
    assert boosted_confidence > base_confidence


def test_trend_following_confidence_rewards_gap_acceleration_and_penalizes_exhaustion():
    strategy = create_strategy(
        "trend_following",
        {
            "trend_strength_norm_ratio": 0.003,
            "trend_confidence_velocity_norm_ratio": 0.0005,
            "trend_confidence_gap_velocity_positive_threshold": 0.20,
            "trend_confidence_gap_velocity_negative_threshold": -0.10,
            "trend_confidence_gap_velocity_positive_bonus": 0.12,
            "trend_confidence_gap_velocity_negative_penalty": 0.15,
        },
    )
    accelerating = strategy._confidence(  # type: ignore[attr-defined]
        ema_gap_ratio=0.002,
        pullback_distance_ratio=0.002,
        overextension_ratio=0.0002,
        bounce_confirmed=True,
        directional_velocity_ratio=0.0008,
        price_aligned_with_fast_ma=True,
        ema_gap_velocity_ratio=0.25,
    )
    decelerating = strategy._confidence(  # type: ignore[attr-defined]
        ema_gap_ratio=0.002,
        pullback_distance_ratio=0.002,
        overextension_ratio=0.0002,
        bounce_confirmed=True,
        directional_velocity_ratio=0.0008,
        price_aligned_with_fast_ma=True,
        ema_gap_velocity_ratio=-0.20,
    )
    assert accelerating > decelerating


def test_trend_following_uses_looser_bounce_rejection_for_non_crypto():
    strategy = create_strategy("trend_following", {})
    crypto_strategy = create_strategy("crypto_trend_following", {})
    assert strategy.bounce_rejection_min_wick_to_body_ratio == pytest.approx(0.65)  # type: ignore[attr-defined]
    assert crypto_strategy.bounce_rejection_min_wick_to_body_ratio == pytest.approx(1.0)  # type: ignore[attr-defined]


def test_trend_following_internal_confidence_gate_blocks_low_confidence_signal():
    strategy = create_strategy(
        "trend_following",
        {
            "_worker_mode": "execution",
            "trend_execution_min_confidence_for_entry": 0.95,
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "trend_atr_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_index_min_stop_pct": 0.0,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "confidence_below_internal_threshold"
    assert signal.metadata.get("trend_internal_min_confidence_source") == "trend_execution_min_confidence_for_entry"
    assert signal.metadata.get("trend_internal_confidence_mode") == "execution"


def test_trend_following_blocks_overextended_pullback_by_atr():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 1.0,
            "trend_pullback_max_distance_atr": 1.0,
            "trend_pullback_bounce_required": False,
            "trend_pullback_ema_tolerance_ratio": 0.05,
            "trend_atr_window": 14,
        },
    )
    prices = [
        100.0,
        100.75107496244007,
        101.36380200714456,
        101.43671653647391,
        101.25098333694265,
        101.46902289113243,
        101.5169175110531,
        102.17099525350874,
        102.05629561523502,
        102.21885074187878,
        102.55226200500684,
        103.40524262131937,
        103.6127415906272,
        103.46368214166672,
        104.07296886831828,
        104.46235926299882,
        104.26316940917872,
        105.1187634187279,
        106.09122018038813,
        106.78756775798267,
        107.631033278686,
        107.52726938959694,
        108.09500018681314,
        108.93314144756192,
        109.42751573862662,
        109.58294408335097,
        109.14406601626035,
        109.2387409529864,
        109.61616011049648,
        110.47697779567712,
        111.42354798411033,
    ]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_pullback_entry_atr"


def test_trend_following_crypto_blocks_weak_trend_gap():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "use_donchian_filter": True,
            "trend_atr_window": 3,
            "trend_pullback_max_distance_ratio": 0.03,
            "trend_pullback_ema_tolerance_ratio": 0.002,
            "trend_crypto_min_ema_gap_ratio": 0.5,
            "trend_crypto_min_atr_pct": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx(
            [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 103.0, 102.5, 102.2, 102.4],
            symbol="ETH",
            tick_size=0.1,
        )
    )
    assert signal.side == Side.BUY
    assert "crypto_trend_too_weak" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("trend_gap_softened") is True


def test_donchian_breakout_buy_signal():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_stop_loss_pips": 20,
            "donchian_breakout_take_profit_pips": 60,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 103.0]))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips >= 20.0
    assert signal.take_profit_pips >= 60.0
    assert signal.metadata.get("indicator") == "donchian_breakout"


def test_donchian_breakout_sell_signal():
    strategy = create_strategy("donchian_breakout", {"donchian_breakout_window": 3})
    signal = strategy.generate_signal(_ctx([103.0, 102.0, 101.0, 102.0, 100.0]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "donchian_breakout"


def test_donchian_breakout_holds_without_breakout():
    strategy = create_strategy("donchian_breakout", {"donchian_breakout_window": 3})
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 101.5]))
    assert signal.side == Side.HOLD


def test_donchian_breakout_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_max_spread_pips": 2.0,
            "donchian_breakout_max_spread_pips_by_symbol": {"WTI": 5.0},
        },
    )
    blocked = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="EURUSD", current_spread_pips=3.0)
    )
    assert blocked.side == Side.HOLD
    assert blocked.metadata.get("reason") == "spread_too_wide"
    assert blocked.metadata.get("max_spread_pips") == pytest.approx(2.0)
    assert blocked.metadata.get("max_spread_pips_source") == "global"

    allowed = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="WTI", current_spread_pips=3.0)
    )
    assert allowed.side == Side.BUY
    assert allowed.metadata.get("max_spread_pips") == pytest.approx(5.0)
    assert allowed.metadata.get("max_spread_pips_source") == "symbol_override"


def test_donchian_breakout_uses_dynamic_atr_stop_and_rr():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_atr_multiplier": 4.0,
            "donchian_breakout_risk_reward_ratio": 3.0,
            "donchian_breakout_min_stop_loss_pips": 1.0,
            "donchian_breakout_min_take_profit_pips": 1.0,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(5.333333333333333)
    assert signal.take_profit_pips == pytest.approx(16.0)


def test_donchian_breakout_volume_confirmation_deduplicates_current_volume_tail_sample():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_volume_confirmation": True,
            "donchian_breakout_volume_window": 5,
            "donchian_breakout_volume_min_samples": 1,
            "donchian_breakout_min_volume_ratio": 1.2,
            "donchian_breakout_volume_allow_missing": False,
        },
    )
    signal = strategy.generate_signal(
        _ctx(
            [100.0, 101.0, 102.0, 101.0, 103.0],
            symbol="US100",
            tick_size=1.0,
            volumes=[100.0, 100.0, 200.0],
            current_volume=200.0,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_current_deduplicated") is True
    assert signal.metadata.get("volume_current_source") == "ctx_volumes_last"
    assert signal.metadata.get("avg_volume") == pytest.approx(100.0)
    assert signal.metadata.get("volume_ratio") == pytest.approx(2.0)


def test_donchian_breakout_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_volume_confirmation": True,
            "donchian_breakout_volume_min_samples": 3,
            "donchian_breakout_volume_allow_missing": False,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="ES35", tick_size=1.0, volumes=[], current_volume=None)
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_check_status") == "missing_current_volume"
    assert signal.metadata.get("volume_allow_missing_effective") is True
    assert signal.metadata.get("volume_missing_relaxed_for_index") is True


def test_donchian_breakout_blocks_shallow_breakout():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_breakout_atr_ratio": 0.3,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 102.1], symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_too_shallow"


def test_donchian_breakout_uses_index_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="US100"))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.1)


def test_donchian_breakout_uses_aus200_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="AUS200"))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)


def test_donchian_breakout_blocks_low_volume_when_enabled():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_volume_confirmation": True,
            "donchian_breakout_volume_window": 5,
            "donchian_breakout_min_volume_ratio": 1.2,
            "donchian_breakout_volume_min_samples": 3,
            "donchian_breakout_volume_allow_missing": False,
        },
    )
    signal = strategy.generate_signal(
        _ctx(
            [100.0, 101.0, 102.0, 101.0, 103.0],
            symbol="US100",
            tick_size=1.0,
            volumes=[100.0, 100.0, 100.0, 100.0, 100.0],
            current_volume=80.0,
        )
    )
    assert signal.side == Side.BUY
    assert "volume_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_donchian_breakout_regime_filter_blocks_countertrend_breakout():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_regime_filter_enabled": True,
            "donchian_breakout_regime_ema_window": 8,
            "donchian_breakout_regime_adx_window": 3,
            "donchian_breakout_regime_min_adx": 10.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([120.0, 118.0, 116.0, 114.0, 112.0, 110.0, 108.0, 106.0, 104.0, 102.0, 100.0, 98.0, 99.0, 101.0])
    )
    assert signal.side != Side.HOLD
    assert "regime_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("regime_softened") is True
    assert signal.metadata.get("regime_filter_status") == "direction_blocked"
    assert signal.metadata.get("regime_trend_up") is False
    assert signal.metadata.get("regime_adx_ok") is True


def test_donchian_breakout_regime_filter_allows_aligned_breakout():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_regime_filter_enabled": True,
            "donchian_breakout_regime_ema_window": 8,
            "donchian_breakout_regime_adx_window": 3,
            "donchian_breakout_regime_min_adx": 10.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([90.0, 92.0, 94.0, 96.0, 98.0, 100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 111.0, 113.0])
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime_filter_status") == "ok"
    assert signal.metadata.get("regime_trend_up") is True
    assert signal.metadata.get("regime_adx_ok") is True


def test_donchian_breakout_regime_adx_hysteresis_avoids_threshold_chop(monkeypatch):
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_regime_filter_enabled": True,
            "donchian_breakout_regime_ema_window": 8,
            "donchian_breakout_regime_adx_window": 3,
            "donchian_breakout_regime_min_adx": 18.0,
            "donchian_breakout_regime_adx_hysteresis": 2.0,
        },
    )
    prices = [90.0, 92.0, 94.0, 96.0, 98.0, 100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 111.0, 113.0]

    monkeypatch.setattr(strategy, "_adx", lambda _prices, _window: 19.0)
    blocked = strategy.generate_signal(_ctx(prices))
    assert blocked.side == Side.BUY
    assert "regime_filter_blocked" in (blocked.metadata.get("soft_filter_reasons") or [])
    assert blocked.metadata.get("regime_filter_status") == "adx_below_threshold"
    assert blocked.metadata.get("regime_adx_entry_threshold") == pytest.approx(20.0)
    assert blocked.metadata.get("regime_adx_active_after") is False

    monkeypatch.setattr(strategy, "_adx", lambda _prices, _window: 20.5)
    activated = strategy.generate_signal(_ctx(prices))
    assert activated.side == Side.BUY
    assert activated.metadata.get("regime_adx_active_before") is False
    assert activated.metadata.get("regime_adx_active_after") is True

    monkeypatch.setattr(strategy, "_adx", lambda _prices, _window: 17.5)
    held_active = strategy.generate_signal(_ctx(prices))
    assert held_active.side == Side.BUY
    assert held_active.metadata.get("regime_adx_active_before") is True
    assert held_active.metadata.get("regime_adx_exit_threshold") == pytest.approx(16.0)
    assert held_active.metadata.get("regime_adx_active_after") is True

    monkeypatch.setattr(strategy, "_adx", lambda _prices, _window: 15.5)
    deactivated = strategy.generate_signal(_ctx(prices))
    assert deactivated.side == Side.BUY
    assert "regime_filter_blocked" in (deactivated.metadata.get("soft_filter_reasons") or [])
    assert deactivated.metadata.get("regime_filter_status") == "adx_below_threshold"
    assert deactivated.metadata.get("regime_adx_active_before") is True
    assert deactivated.metadata.get("regime_adx_active_after") is False


def test_donchian_breakout_no_breakout_keeps_directional_exit_channel_trailing_metadata():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 101.0, 101.5], symbol="US100", tick_size=1.0))
    trailing = signal.metadata.get("trailing_stop")

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_breakout"
    assert signal.metadata.get("exit_window") == 2
    assert signal.metadata.get("exit_channel_lower") == pytest.approx(101.0)
    assert signal.metadata.get("exit_channel_upper") == pytest.approx(102.0)
    assert isinstance(trailing, dict)
    assert trailing.get("long_anchor_value") == pytest.approx(101.0)
    assert trailing.get("short_anchor_value") == pytest.approx(102.0)
    assert trailing.get("anchor_source") == "donchian_exit_channel"


def test_donchian_breakout_uses_exit_window_override_in_trailing_metadata():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 6,
            "donchian_breakout_exit_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 103.0, 104.0, 103.0, 103.5, 103.4], symbol="US100", tick_size=1.0)
    )
    assert signal.metadata.get("exit_window") == 3
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("exit_window") == 3


def test_donchian_breakout_blocks_weak_breakout_candle_on_closed_candle_confirmation():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_resample_mode": "always",
            "donchian_breakout_candle_timeframe_sec": 60.0,
            "donchian_breakout_breakout_quality_filter_enabled": True,
            "donchian_breakout_breakout_min_body_to_range_ratio": 0.4,
            "donchian_breakout_volume_confirmation": False,
        },
    )
    start_ts = _ts("14:00")
    timestamps = [start_ts + (idx * 15.0) for idx in range(24)]
    prices = [
        99.0, 99.2, 99.1, 99.0,
        100.0, 100.2, 100.1, 100.0,
        101.0, 101.2, 101.1, 101.0,
        102.0, 102.2, 102.1, 102.0,
        102.18, 103.2, 101.8, 102.25,
        102.3, 102.35, 102.32, 102.31,
    ]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", timestamps=timestamps, tick_size=0.0001))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_candle_weak"
    assert signal.metadata.get("using_closed_candles") is True
    assert signal.metadata.get("breakout_quality_status") == "body_too_small"


def test_donchian_breakout_session_filter_blocks_fx_breakout_outside_window():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_session_filter_enabled": True,
            "donchian_breakout_session_filter_fx_only": True,
            "donchian_breakout_session_start_hour_utc": 6,
            "donchian_breakout_session_end_hour_utc": 22,
            "donchian_breakout_resample_mode": "off",
            "donchian_breakout_breakout_quality_filter_enabled": False,
            "donchian_breakout_volume_confirmation": False,
        },
    )
    timestamps = [_ts("02:00") + (idx * 60.0) for idx in range(5)]
    signal = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 101.0, 103.0], symbol="EURUSD", timestamps=timestamps, tick_size=0.0001)
    )
    assert signal.side == Side.BUY
    assert "session_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("session_softened") is True
    assert signal.metadata.get("session_status") == "outside_window"
    assert signal.metadata.get("session_hour_utc") == 2


def test_donchian_breakout_rsi_filter_blocks_overbought_breakout():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_window": 3,
            "donchian_breakout_atr_window": 3,
            "donchian_breakout_min_channel_width_atr": 0.0,
            "donchian_breakout_rsi_filter_enabled": True,
            "donchian_breakout_rsi_period": 5,
            "donchian_breakout_rsi_buy_max": 80.0,
            "donchian_breakout_breakout_quality_filter_enabled": False,
            "donchian_breakout_volume_confirmation": False,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 108.5, 110.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert "rsi_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("rsi_softened") is True
    assert signal.metadata.get("rsi_filter_status") == "overbought"
    assert float(signal.metadata.get("rsi", 0.0)) > 80.0


def test_index_hybrid_buy_signal_for_index():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0])
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "index_hybrid"
    assert signal.metadata.get("confidence_threshold_cap") == pytest.approx(0.58)


def test_index_hybrid_auto_corrects_inverted_regime_thresholds():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_trend_gap_threshold": 0.0002,
            "index_mean_reversion_gap_threshold": 0.00045,
            "index_trend_atr_pct_threshold": 0.008,
            "index_mean_reversion_atr_pct_threshold": 0.03,
            "index_auto_correct_regime_thresholds": True,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0], tick_size=0.1)
    )
    assert signal.metadata.get("regime_thresholds_auto_corrected") is True
    assert signal.metadata.get("trend_gap_threshold") == pytest.approx(0.00045)
    assert signal.metadata.get("mean_reversion_gap_threshold") == pytest.approx(0.0002)
    assert signal.metadata.get("trend_atr_pct_threshold") == pytest.approx(0.03)
    assert signal.metadata.get("mean_reversion_atr_pct_threshold") == pytest.approx(0.008)
    assert signal.metadata.get("trend_gap_threshold_configured") == pytest.approx(0.0002)
    assert signal.metadata.get("mean_reversion_gap_threshold_configured") == pytest.approx(0.00045)
    assert signal.metadata.get("trend_atr_pct_threshold_configured") == pytest.approx(0.008)
    assert signal.metadata.get("mean_reversion_atr_pct_threshold_configured") == pytest.approx(0.03)
    assert signal.metadata.get("trend_gap_threshold_effective") == pytest.approx(0.00045)
    assert signal.metadata.get("mean_reversion_gap_threshold_effective") == pytest.approx(0.0002)
    assert signal.metadata.get("trend_atr_pct_threshold_effective") == pytest.approx(0.03)
    assert signal.metadata.get("mean_reversion_atr_pct_threshold_effective") == pytest.approx(0.008)


def test_index_hybrid_enforces_gap_hysteresis_when_thresholds_match():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_trend_gap_threshold": 0.00025,
            "index_mean_reversion_gap_threshold": 0.00025,
            "index_gap_hysteresis_min": 0.00005,
            "index_enforce_gap_hysteresis": True,
            "index_auto_correct_regime_thresholds": True,
            "index_regime_fallback_mode": "blocked",
        },
    )
    assert strategy.trend_gap_threshold_effective == pytest.approx(0.00025)  # type: ignore[attr-defined]
    assert strategy.mean_reversion_gap_threshold_effective == pytest.approx(0.0002)  # type: ignore[attr-defined]
    assert strategy.gap_hysteresis_applied is True  # type: ignore[attr-defined]
    state = strategy._resolve_regime_state(0.00022, 0.03)  # type: ignore[attr-defined]
    assert state.get("trend_regime_strict") is False
    assert state.get("mean_reversion_regime_strict") is False
    assert state.get("trend_regime") is False
    assert state.get("mean_reversion_regime") is False


def test_index_hybrid_logs_warning_when_auto_correct_swaps_thresholds(caplog):
    with caplog.at_level(logging.WARNING, logger="xtb_bot.strategies.index_hybrid"):
        create_strategy(
            "index_hybrid",
            {
                "index_fast_ema_window": 3,
                "index_slow_ema_window": 5,
                "index_donchian_window": 3,
                "index_zscore_window": 3,
                "index_atr_window": 3,
                "index_zscore_ema_window": 5,
                "index_trend_gap_threshold": 0.0002,
                "index_mean_reversion_gap_threshold": 0.00045,
                "index_auto_correct_regime_thresholds": True,
            },
        )

    assert any("auto-corrected regime thresholds" in record.message for record in caplog.records)


def test_index_hybrid_can_require_tick_size_from_context():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_require_context_tick_size": True,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0] * 200, tick_size=None)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_index_hybrid_auto_syncs_zscore_window_to_donchian_ratio():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 4,
            "index_zscore_window": 20,
            "index_window_sync_mode": "auto",
            "index_zscore_donchian_ratio_target": 1.5,
            "index_zscore_donchian_ratio_tolerance": 0.2,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0, 105.0, 106.0])
    )
    assert strategy.zscore_window == 6
    assert signal.metadata.get("window_sync_status") == "auto_adjusted"
    assert signal.metadata.get("zscore_window_effective") == 6
    assert signal.metadata.get("zscore_window_target") == 6


def test_index_hybrid_warn_mode_keeps_configured_zscore_window():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 4,
            "index_zscore_window": 20,
            "index_window_sync_mode": "warn",
            "index_zscore_donchian_ratio_target": 1.5,
            "index_zscore_donchian_ratio_tolerance": 0.2,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0 + float(idx) for idx in range(30)])
    )
    assert strategy.zscore_window == 20
    assert signal.metadata.get("window_sync_status") == "mismatch_warning"
    assert signal.metadata.get("zscore_window_effective") == 20
    assert signal.metadata.get("zscore_window_target") == 6


def test_index_hybrid_off_mode_disables_window_sync():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 4,
            "index_zscore_window": 20,
            "index_window_sync_mode": "off",
            "index_zscore_donchian_ratio_target": 1.5,
            "index_zscore_donchian_ratio_tolerance": 0.2,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0 + float(idx) for idx in range(30)])
    )
    assert strategy.zscore_window == 20
    assert signal.metadata.get("window_sync_status") == "off"
    assert signal.metadata.get("zscore_window_effective") == 20
    assert signal.metadata.get("zscore_window_target") == 6


def test_index_hybrid_nearest_regime_fallback_avoids_dead_zone_hold():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 1.0,
            "index_trend_gap_threshold": 0.0005,
            "index_mean_reversion_gap_threshold": 0.0002,
            "index_trend_atr_pct_threshold": 0.04,
            "index_mean_reversion_atr_pct_threshold": 0.02,
            "index_regime_fallback_mode": "nearest",
        },
    )
    prices = [100.0 + (0.03 * idx) for idx in range(30)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.metadata.get("trend_regime_strict") is False
    assert signal.metadata.get("mean_reversion_regime_strict") is False
    assert signal.metadata.get("regime_fallback_applied") is True
    assert signal.metadata.get("regime_fallback_selected") == "trend_following"
    assert signal.metadata.get("trend_regime") is True
    assert signal.metadata.get("reason") != "regime_filter_blocked"


def test_index_hybrid_can_disable_regime_fallback():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 1.0,
            "index_trend_gap_threshold": 0.0005,
            "index_mean_reversion_gap_threshold": 0.0002,
            "index_trend_atr_pct_threshold": 0.04,
            "index_mean_reversion_atr_pct_threshold": 0.02,
            "index_regime_fallback_mode": "blocked",
        },
    )
    prices = [100.0 + (0.03 * idx) for idx in range(30)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_regime_setup"
    assert signal.metadata.get("regime_fallback_applied") is False
    assert signal.metadata.get("regime_fallback_selected") is None


def test_index_hybrid_forces_hard_mode_when_fuzzy_requested():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 1.0,
            "index_trend_gap_threshold": 0.0005,
            "index_mean_reversion_gap_threshold": 0.0002,
            "index_trend_atr_pct_threshold": 0.04,
            "index_mean_reversion_atr_pct_threshold": 0.02,
            "index_regime_selection_mode": "fuzzy",
            "index_regime_trend_index_threshold": 0.8,
        },
    )
    prices = [100.0 + (0.03 * idx) for idx in range(30)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.metadata.get("regime_selection_mode") == "hard"
    assert signal.metadata.get("regime_selection_mode_requested") == "fuzzy"
    assert signal.metadata.get("regime_selection_mode_forced_hard") is True
    assert signal.metadata.get("regime_index", 0.0) > 0.0
    assert signal.metadata.get("reason") != "regime_filter_blocked"


def test_index_hybrid_respects_classic_zscore_mode_when_requested(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 1.0,
            "index_zscore_mode": "classic",
            "index_zscore_ema_window": 55,
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(strategy, "_zscore", lambda _values: -3.0)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: -0.2)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0 + (0.01 * idx) for idx in range(70)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("zscore") == pytest.approx(-3.0)
    assert signal.metadata.get("zscore_mode") == "classic"
    assert signal.metadata.get("zscore_mode_requested") == "classic"
    assert signal.metadata.get("zscore_mode_forced_detrended") is False
    assert signal.metadata.get("zscore_ema_window") == 55


def test_index_hybrid_volume_can_work_as_bonus_only():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_volume_confirmation": True,
            "index_volume_window": 5,
            "index_volume_min_samples": 3,
            "index_min_volume_ratio": 1.4,
            "index_volume_require_spike": True,
            "index_volume_allow_missing": False,
            "index_volume_as_bonus_only": True,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    volumes = [100.0, 101.0, 99.0, 100.0, 100.0, 101.0, 99.0, 115.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_spike") is False
    assert signal.metadata.get("volume_confidence_boost_applied") == pytest.approx(0.0)


def test_index_hybrid_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_volume_confirmation": True,
            "index_volume_allow_missing": False,
            "index_volume_min_samples": 10,
        },
    )
    metadata, hold_reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 80, symbol="ES35", volumes=[], current_volume=None)
    )
    assert hold_reason is None
    assert boost == pytest.approx(0.0)
    assert metadata.get("volume_allow_missing_effective") is True
    assert metadata.get("volume_missing_relaxed_for_index") is True


def test_index_hybrid_volume_deduplicates_current_volume_sample():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_volume_confirmation": True,
            "index_volume_window": 5,
            "index_volume_min_samples": 3,
            "index_min_volume_ratio": 1.3,
            "index_volume_min_ratio_for_entry": 1.0,
            "index_volume_require_spike": True,
            "index_volume_allow_missing": False,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    volumes = [100.0, 100.0, 100.0, 130.0]
    signal = strategy.generate_signal(
        _ctx(
            prices,
            symbol="US100",
            tick_size=1.0,
            volumes=volumes,
            current_volume=130.0,
        )
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_spike") is True
    assert signal.metadata.get("volume_current_deduplicated") is True
    assert signal.metadata.get("volume_current_source") == "volumes"


def test_index_hybrid_blocks_low_volume_ratio_even_in_bonus_only_mode():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_volume_confirmation": True,
            "index_volume_window": 5,
            "index_volume_min_samples": 3,
            "index_min_volume_ratio": 1.4,
            "index_volume_min_ratio_for_entry": 1.0,
            "index_volume_require_spike": False,
            "index_volume_allow_missing": True,
            "index_volume_as_bonus_only": True,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    volumes = [100.0, 100.0, 100.0, 100.0, 100.0, 95.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_entry_ratio"
    assert signal.metadata.get("volume_ratio", 1.0) < 1.0


def test_index_hybrid_holds_for_non_index_symbol():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0])
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "not_index_symbol"


def test_index_hybrid_prefers_trend_signal_in_strong_down_move():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.8,
        },
    )
    prices = [26000.0, 25980.0, 25950.0, 25920.0, 25880.0, 25840.0, 25800.0, 25790.0, 25770.0, 25780.0, 25600.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.SELL
    assert signal.metadata.get("regime") == "trend_following"
    assert signal.metadata.get("breakout_down") is True


def test_index_hybrid_blocks_trend_entry_when_slope_not_confirmed(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
        },
    )

    def fake_ema(values: Sequence[float], window: int) -> float:
        if window == 3:
            return 105.0 if len(values) >= 8 else 104.0
        return 100.0 if len(values) >= 8 else 101.0

    monkeypatch.setattr(strategy, "_ema", fake_ema)
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 104.0, 99.0))
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )

    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "trend_slope_not_confirmed"
    assert signal.metadata.get("trend_slope_up_confirmed") is False
    assert signal.metadata.get("fast_slope_ratio", 0.0) > 0.0
    assert signal.metadata.get("slow_slope_ratio", 0.0) < 0.0


def test_index_hybrid_scales_stop_loss_by_index_price_percentage():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_stop_loss_pct": 0.5,
            "index_take_profit_pct": 1.5,
        },
    )
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25010.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    expected_min_stop_pips = (prices[-1] * 0.5 / 100.0) / 0.1
    expected_min_take_pips = (prices[-1] * 1.5 / 100.0) / 0.1
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips >= expected_min_stop_pips
    assert signal.take_profit_pips >= expected_min_take_pips


def test_index_hybrid_risk_profiles_have_different_tp_shapes():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_trend_stop_loss_pips": 20.0,
            "index_trend_take_profit_pips": 80.0,
            "index_trend_risk_reward_ratio": 3.0,
            "index_trend_take_profit_pct": 1.5,
            "index_mean_reversion_stop_loss_pips": 20.0,
            "index_mean_reversion_take_profit_pips": 30.0,
            "index_mean_reversion_risk_reward_ratio": 1.2,
            "index_mean_reversion_take_profit_pct": 0.45,
        },
    )
    ctx = _ctx([100.0 + (0.01 * idx) for idx in range(200)], symbol="US100", tick_size=1.0)
    trend_sl, trend_tp, trend_meta = strategy._dynamic_sl_tp(  # type: ignore[attr-defined]
        ctx,
        100.0,
        2.0,
        risk_profile="trend_following",
    )
    mr_sl, mr_tp, mr_meta = strategy._dynamic_sl_tp(  # type: ignore[attr-defined]
        ctx,
        100.0,
        2.0,
        risk_profile="mean_reversion",
    )
    assert trend_meta.get("risk_profile") == "trend_following"
    assert mr_meta.get("risk_profile") == "mean_reversion"
    assert mr_tp < trend_tp
    assert mr_sl <= trend_sl


def test_index_hybrid_mean_reversion_pct_floors_disabled_by_default():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_stop_loss_pct": 0.15,
            "index_take_profit_pct": 0.45,
            "index_mean_reversion_stop_loss_pips": 30.0,
            "index_mean_reversion_take_profit_pips": 80.0,
            "index_mean_reversion_stop_loss_pct": 0.15,
            "index_mean_reversion_take_profit_pct": 0.45,
            "index_mean_reversion_stop_atr_multiplier": 1.0,
            "index_mean_reversion_risk_reward_ratio": 1.2,
        },
    )
    ctx = _ctx([100.0 + (0.01 * idx) for idx in range(200)], symbol="US100", tick_size=0.1)
    stop_pips, take_pips, meta = strategy._dynamic_sl_tp(  # type: ignore[attr-defined]
        ctx,
        18000.0,
        6.0,
        risk_profile="mean_reversion",
    )

    assert meta.get("risk_profile_pct_floors_enabled") is False
    assert meta.get("stop_from_pct_pips_raw") == pytest.approx(270.0)
    assert meta.get("take_from_pct_pips_raw") == pytest.approx(810.0)
    assert meta.get("stop_from_pct_pips") == pytest.approx(0.0)
    assert meta.get("take_from_pct_pips") == pytest.approx(0.0)
    assert stop_pips < 270.0
    assert take_pips < 810.0


def test_index_hybrid_can_enable_mean_reversion_pct_floors():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_stop_loss_pct": 0.15,
            "index_take_profit_pct": 0.45,
            "index_mean_reversion_stop_loss_pips": 30.0,
            "index_mean_reversion_take_profit_pips": 80.0,
            "index_mean_reversion_stop_loss_pct": 0.15,
            "index_mean_reversion_take_profit_pct": 0.45,
            "index_mean_reversion_stop_atr_multiplier": 1.0,
            "index_mean_reversion_risk_reward_ratio": 1.2,
            "index_mean_reversion_pct_floors_enabled": True,
        },
    )
    ctx = _ctx([100.0 + (0.01 * idx) for idx in range(200)], symbol="US100", tick_size=0.1)
    stop_pips, take_pips, meta = strategy._dynamic_sl_tp(  # type: ignore[attr-defined]
        ctx,
        18000.0,
        6.0,
        risk_profile="mean_reversion",
    )

    assert meta.get("risk_profile_pct_floors_enabled") is True
    assert meta.get("stop_from_pct_pips") == pytest.approx(270.0)
    assert meta.get("take_from_pct_pips") == pytest.approx(810.0)
    assert stop_pips >= 270.0
    assert take_pips >= 810.0


def test_index_hybrid_applies_mean_reversion_risk_profile_to_mr_entry(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_session_filter_enabled": False,
            "index_trend_stop_loss_pips": 20.0,
            "index_trend_take_profit_pips": 80.0,
            "index_trend_risk_reward_ratio": 3.0,
            "index_mean_reversion_stop_loss_pips": 20.0,
            "index_mean_reversion_take_profit_pips": 30.0,
            "index_mean_reversion_risk_reward_ratio": 1.2,
            "index_mean_reversion_take_profit_pct": 0.45,
            "index_mean_reversion_entry_mode": "touch",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: -2.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("risk_profile") == "mean_reversion"
    assert signal.metadata.get("risk_profile_risk_reward_ratio") == pytest.approx(1.2)
    assert signal.take_profit_pips <= max(signal.stop_loss_pips * 1.5, 60.0)


def test_index_hybrid_uses_index_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_stop_loss_pct": 0.5,
            "index_take_profit_pct": 1.5,
        },
    )
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25010.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)


def test_index_hybrid_uses_aus200_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
        },
    )
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25010.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="AUS200", tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)
    assert signal.metadata.get("indicator") == "index_hybrid"


def test_index_hybrid_blocks_when_fallback_pip_size_is_disallowed():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_require_non_fallback_pip_size": True,
        },
    )
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25010.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=None, pip_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "pip_size_source_fallback_blocked"
    assert signal.metadata.get("pip_size_source") == "fallback_symbol_map"


def test_index_hybrid_allows_runtime_db_pip_size_when_fallback_is_disallowed():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_require_non_fallback_pip_size": True,
            "_runtime_symbol_pip_sizes": {"US100": 0.1},
        },
    )
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25010.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=None, pip_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size_source") == "db_symbol_map"


def test_index_hybrid_blocks_shallow_breakout_when_min_distance_is_set():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_min_breakout_distance_ratio": 0.03,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0])
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_too_shallow"


def test_index_hybrid_blocks_breakout_when_spread_too_wide_for_breakout():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_min_breakout_distance_ratio": 0.0,
            "index_max_spread_pips": 0.0,
            "index_max_spread_to_breakout_ratio": 0.5,
            "index_max_spread_to_stop_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx(
            [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0],
            symbol="US100",
            tick_size=0.1,
            current_spread_pips=20.0,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide_for_breakout"
    assert signal.metadata.get("spread_to_breakout_ratio", 0.0) > 0.5


def test_index_hybrid_absolute_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_min_breakout_distance_ratio": 0.0,
            "index_max_spread_pips": 1.0,
            "index_max_spread_pips_by_symbol": {"NK20": 3.0},
            "index_max_spread_to_breakout_ratio": 0.0,
            "index_max_spread_to_stop_ratio": 0.0,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]

    blocked = strategy.generate_signal(
        _ctx(prices, symbol="DE40", tick_size=0.1, current_spread_pips=2.0)
    )
    assert blocked.side == Side.HOLD
    assert blocked.metadata.get("reason") == "spread_too_wide"
    assert blocked.metadata.get("index_max_spread_pips_source") == "global"

    allowed = strategy.generate_signal(
        _ctx(prices, symbol="NK20", tick_size=1.0, current_spread_pips=2.0)
    )
    assert allowed.side == Side.BUY
    assert allowed.metadata.get("index_max_spread_pips") == pytest.approx(3.0)
    assert allowed.metadata.get("index_max_spread_pips_source") == "symbol_override"


def test_index_hybrid_blocks_mean_reversion_when_spread_too_wide_for_stop(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_session_filter_enabled": False,
            "index_mean_reversion_require_band_proximity": False,
            "index_max_spread_pips": 0.0,
            "index_max_spread_to_breakout_ratio": 0.0,
            "index_max_spread_to_stop_ratio": 0.1,
            "index_mean_reversion_stop_loss_pips": 20.0,
            "index_mean_reversion_stop_atr_multiplier": 1.0,
            "index_mean_reversion_pct_floors_enabled": False,
            "index_mean_reversion_entry_mode": "touch",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: -2.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=0.1, current_spread_pips=8.0)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide_for_stop"
    assert signal.metadata.get("spread_to_stop_ratio", 0.0) > 0.1


def test_index_hybrid_blocks_when_channel_is_too_narrow():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_min_channel_width_atr": 1000.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0])
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "channel_too_narrow"


def test_index_hybrid_waits_for_volatility_explosion_before_trend_entry(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
            "index_volume_confirmation": False,
            "index_trend_volatility_explosion_filter_enabled": True,
            "index_trend_volatility_explosion_lookback": 3,
            "index_trend_volatility_explosion_min_ratio": 1.25,
        },
    )
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 0.1)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )
    prices = [100.0, 100.2, 100.1, 100.3, 100.2, 100.4, 100.3, 100.5, 100.4, 100.6]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "waiting_for_volatility_explosion"
    assert signal.metadata.get("regime") == "trend_following"
    assert signal.metadata.get("trend_volatility_explosion_ready") is True
    assert signal.metadata.get("trend_channel_expansion_ratio", 0.0) < 1.25


def test_index_hybrid_allows_trend_entry_after_channel_expands(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
            "index_volume_confirmation": False,
            "index_trend_volatility_explosion_filter_enabled": True,
            "index_trend_volatility_explosion_lookback": 3,
            "index_trend_volatility_explosion_min_ratio": 1.25,
        },
    )
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 0.1)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )
    prices = [100.0, 100.05, 100.0, 100.05, 100.0, 100.1, 100.0, 100.8, 100.6, 101.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_channel_expansion_ratio", 0.0) >= 1.25


def test_index_hybrid_blocks_when_volume_spike_is_required():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_volume_confirmation": True,
            "index_volume_window": 5,
            "index_volume_min_samples": 3,
            "index_min_volume_ratio": 1.4,
            "index_volume_require_spike": True,
            "index_volume_allow_missing": False,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    volumes = [100.0, 101.0, 99.0, 100.0, 100.0, 101.0, 99.0, 115.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.BUY
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_index_hybrid_volume_spike_boosts_confidence():
    base_params = {
        "index_fast_ema_window": 3,
        "index_slow_ema_window": 5,
        "index_donchian_window": 3,
        "index_zscore_window": 3,
        "index_atr_window": 3,
        "index_zscore_ema_window": 5,
        "index_zscore_threshold": 1.0,
        "index_volume_window": 5,
        "index_volume_min_samples": 3,
        "index_min_volume_ratio": 1.4,
        "index_volume_allow_missing": True,
        "index_volume_confidence_boost": 0.2,
        "index_trend_confidence_base": 0.05,
        "index_trend_confidence_gap_weight": 0.1,
        "index_trend_confidence_breakout_weight": 0.1,
        "index_trend_confidence_gap_norm": 1.0,
        "index_trend_confidence_breakout_norm": 1.0,
    }
    base_strategy = create_strategy("index_hybrid", base_params)
    boosted_strategy = create_strategy("index_hybrid", {**base_params, "index_volume_confirmation": True})
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    volumes = [100.0, 101.0, 99.0, 100.0, 100.0, 101.0, 99.0, 180.0]
    base_signal = base_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    boosted_signal = boosted_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert base_signal.side == Side.BUY
    assert boosted_signal.side == Side.BUY
    assert boosted_signal.confidence > base_signal.confidence
    assert boosted_signal.metadata.get("volume_spike") is True


def test_index_hybrid_mean_reversion_can_trade_on_breakout_when_enabled():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_trend_gap_threshold": 1.0,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_mean_reversion_allow_breakout": True,
            "index_mean_reversion_entry_mode": "touch",
        },
    )
    prices = [100.0, 100.0, 100.0, 99.0, 100.0, 100.0, 99.5, 98.9]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime") == "mean_reversion"
    assert signal.metadata.get("breakout_down") is True


def test_index_hybrid_mean_reversion_requires_band_proximity(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_trend_gap_threshold": 1.0,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_mean_reversion_require_band_proximity": True,
            "index_mean_reversion_band_proximity_max_ratio": 0.2,
            "index_mean_reversion_min_mid_deviation_ratio": 0.1,
            "index_mean_reversion_entry_mode": "touch",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: -2.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)

    prices = [100.0, 100.1, 100.2, 100.1, 100.0, 100.1, 100.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_not_near_lower_band"


def test_index_hybrid_mean_reversion_allows_signal_near_lower_band(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_trend_gap_threshold": 1.0,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_mean_reversion_require_band_proximity": True,
            "index_mean_reversion_band_proximity_max_ratio": 0.2,
            "index_mean_reversion_min_mid_deviation_ratio": 0.1,
            "index_mean_reversion_entry_mode": "touch",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: -2.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime") == "mean_reversion"
    assert signal.metadata.get("mean_reversion_structure_ok") is True


def test_index_hybrid_mean_reversion_can_still_block_on_breakout_when_configured():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_trend_gap_threshold": 1.0,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_mean_reversion_allow_breakout": False,
            "index_mean_reversion_breakout_extreme_multiplier": 3.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 99.0, 100.0, 100.0, 99.5, 98.9]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_blocked_by_breakout"
    assert signal.metadata.get("breakout_extreme_ok") is False


def test_index_hybrid_mean_reversion_hook_waits_for_reentry_from_extreme(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_session_filter_enabled": False,
            "index_mean_reversion_require_band_proximity": False,
            "index_mean_reversion_entry_mode": "hook",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    z_values = iter([-2.0, -2.4])
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: next(z_values))
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_waiting_hook_reentry"
    assert signal.metadata.get("mean_reversion_waiting_buy_hook") is True
    assert signal.metadata.get("mean_reversion_buy_signal_ready") is False


def test_index_hybrid_mean_reversion_hook_enters_on_reentry_from_extreme(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_session_filter_enabled": False,
            "index_mean_reversion_require_band_proximity": False,
            "index_mean_reversion_entry_mode": "hook",
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    z_values = iter([-0.1, -2.0])
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: next(z_values))
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime") == "mean_reversion"
    assert signal.metadata.get("mean_reversion_buy_hook_triggered") is True
    assert signal.metadata.get("zscore_prev") == pytest.approx(-2.0)
    assert signal.metadata.get("mean_reversion_extreme_abs_zscore") == pytest.approx(2.0)


def test_index_hybrid_mean_reversion_confidence_base_lifts_threshold_edge_signal(
    monkeypatch: pytest.MonkeyPatch,
):
    base_params = {
        "index_fast_ema_window": 3,
        "index_slow_ema_window": 5,
        "index_donchian_window": 3,
        "index_zscore_window": 5,
        "index_zscore_ema_window": 5,
        "index_atr_window": 3,
        "index_zscore_threshold": 2.0,
        "index_session_filter_enabled": False,
        "index_volume_confirmation": False,
        "index_mean_reversion_require_band_proximity": False,
        "index_mean_reversion_entry_mode": "hook",
    }
    strategy = create_strategy(
        "index_hybrid",
        {**base_params, "index_mean_reversion_confidence_base": 0.10},
    )
    zero_base_strategy = create_strategy("index_hybrid", {**base_params, "index_mean_reversion_confidence_base": 0.0})
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )
    monkeypatch.setattr(zero_base_strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(zero_base_strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        zero_base_strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )

    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 91.0]
    z_values = iter([-0.1, -2.0])
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: next(z_values))
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    zero_z_values = iter([-0.1, -2.0])
    monkeypatch.setattr(zero_base_strategy, "_zscore_detrended", lambda _values: next(zero_z_values))
    zero_base_signal = zero_base_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert zero_base_signal.side == Side.BUY
    assert signal.confidence > zero_base_signal.confidence
    assert signal.metadata.get("confidence_threshold_cap") == pytest.approx(0.58)


def test_index_hybrid_mean_reversion_breakout_requires_extreme_when_enabled():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.5,
            "index_trend_gap_threshold": 1.0,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_mean_reversion_allow_breakout": True,
            "index_mean_reversion_breakout_extreme_multiplier": 3.0,
            "index_mean_reversion_require_band_proximity": False,
        },
    )
    prices = [100.0, 100.0, 100.0, 99.0, 100.0, 100.0, 99.5, 98.9]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_blocked_by_breakout"
    assert signal.metadata.get("breakout_extreme_ok") is False


def test_index_hybrid_trend_confidence_respects_configured_norms():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 3,
            "index_atr_window": 3,
            "index_zscore_ema_window": 5,
            "index_zscore_threshold": 1.0,
            "index_trend_confidence_base": 0.2,
            "index_trend_confidence_gap_weight": 0.3,
            "index_trend_confidence_breakout_weight": 0.2,
            "index_trend_confidence_gap_norm": 1.0,
            "index_trend_confidence_breakout_norm": 1.0,
        },
    )
    signal = strategy.generate_signal(
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0])
    )
    assert signal.side == Side.BUY
    assert signal.confidence < 0.6
    assert signal.metadata.get("breakout_distance_ratio", 0.0) >= 0.0


def test_index_hybrid_emits_trend_exit_hint_flags_on_mid_reentry(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )
    prices = [100.0, 99.8, 99.5, 99.2, 99.0, 98.5, 97.0, 95.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal_in_active_regime"
    assert signal.metadata.get("exit_hint") == "close_if_price_reenters_channel_mid"
    assert signal.metadata.get("trend_exit_buy_reentry_mid") is True
    assert signal.metadata.get("trend_exit_sell_reentry_mid") is False


def test_index_hybrid_emits_mean_exit_hint_flags_on_opposite_band(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
        },
    )
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (False, False, 110.0, 90.0))
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": False,
            "mean_reversion_regime": True,
            "trend_regime_strict": False,
            "mean_reversion_regime_strict": True,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 0.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 0.0,
            "volatility_power": 0.0,
        },
    )
    prices = [100.0, 100.1, 100.2, 100.3, 100.4, 100.7, 100.9, 111.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal_in_active_regime"
    assert signal.metadata.get("exit_hint") == "close_if_price_reaches_channel_mid_or_opposite_band"
    assert signal.metadata.get("mean_exit_buy_mid_target") is True
    assert signal.metadata.get("mean_exit_buy_opposite_band") is True
    assert signal.metadata.get("mean_exit_sell_mid_target") is False


def test_index_hybrid_blocks_mean_reversion_inside_trend_session():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 5,
            "index_zscore_window": 5,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_zscore_threshold": 0.01,
            "index_trend_gap_threshold": 0.5,
            "index_mean_reversion_gap_threshold": 1.0,
            "index_trend_atr_pct_threshold": 100.0,
            "index_mean_reversion_atr_pct_threshold": 100.0,
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "legacy_utc",
            "index_trend_session_start_hour_utc": 6,
            "index_trend_session_end_hour_utc": 22,
            "index_mean_reversion_outside_trend_session": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 102.0, 101.8, 101.6, 101.4, 101.2, 101.0]
    timestamps = [_ts("14:00") + idx * 60 for idx in range(len(prices))]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") != "mean_reversion_blocked_by_session"
    assert signal.metadata.get("reason") == "mean_reversion_waiting_hook_reentry"


def test_index_hybrid_waits_for_session_open_stabilization(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
            "index_trend_session_open_delay_minutes": 15,
            "index_trend_volatility_explosion_filter_enabled": False,
            "index_volume_confirmation": False,
            "index_resample_mode": "off",
        },
    )
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 103.0, 98.0))
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    timestamps = [_ts("14:25") + idx * 60.0 for idx in range(len(prices))]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=1.0))
    assert signal.side == Side.BUY
    assert "trend_waiting_session_open_stabilization" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("minutes_since_trend_session_open") == 2
    assert signal.metadata.get("trend_session_open_delay_remaining_minutes") == 13


def test_index_hybrid_allows_trend_entry_after_session_open_delay(monkeypatch: pytest.MonkeyPatch):
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 3,
            "index_zscore_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
            "index_trend_session_open_delay_minutes": 15,
            "index_trend_volatility_explosion_filter_enabled": False,
            "index_volume_confirmation": False,
            "index_resample_mode": "off",
        },
    )
    monkeypatch.setattr(strategy, "_atr", lambda _prices, _window: 1.0)
    monkeypatch.setattr(strategy, "_zscore_detrended", lambda _values: 0.0)
    monkeypatch.setattr(strategy, "_donchian_breakout", lambda _prices: (True, False, 103.0, 98.0))
    monkeypatch.setattr(
        strategy,
        "_resolve_regime_state",
        lambda _gap, _atr_pct: {
            "trend_regime": True,
            "mean_reversion_regime": False,
            "trend_regime_strict": True,
            "mean_reversion_regime_strict": False,
            "regime_fallback_applied": False,
            "regime_fallback_selected": None,
            "regime_distance_to_trend": 0.0,
            "regime_distance_to_mean_reversion": 0.0,
            "regime_selection_mode": "hard",
            "regime_selection_mode_requested": "hard",
            "regime_selection_mode_forced_hard": False,
            "regime_index": 1.0,
            "regime_trend_index_threshold": 0.8,
            "trend_power": 1.0,
            "volatility_power": 1.0,
        },
    )
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 100.5, 104.0]
    timestamps = [_ts("14:43") + idx * 60.0 for idx in range(len(prices))]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("minutes_since_trend_session_open") == 20
    assert signal.metadata.get("trend_session_open_delay_active") is False


def test_index_hybrid_trend_session_state_normalizes_millisecond_timestamps():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "legacy_utc",
            "index_trend_session_start_hour_utc": 6,
            "index_trend_session_end_hour_utc": 22,
        },
    )
    ts_ms = int(_ts("14:00") * 1000)
    state = strategy._trend_session_state("US100", [float(ts_ms)])  # type: ignore[attr-defined]
    assert state.get("trend_session") is True
    assert state.get("session_hour_utc") == 14
    assert state.get("session_hour_local") == 14
    assert state.get("trend_session_profile") == "legacy_utc"
    assert state.get("trend_session_timezone") == "UTC"


def test_index_hybrid_market_presets_split_us_and_japan_sessions():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
        },
    )
    ts = _ts("15:00")
    us_state = strategy._trend_session_state("US100", [ts])  # type: ignore[attr-defined]
    jp_state = strategy._trend_session_state("JP225", [ts])  # type: ignore[attr-defined]
    assert us_state.get("trend_session") is True
    assert jp_state.get("trend_session") is False
    assert us_state.get("trend_session_profile") == "america"
    assert jp_state.get("trend_session_profile") == "japan_australia"


def test_index_hybrid_market_presets_route_active_vs_inactive_by_symbol():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
            "index_america_trend_session_start_local": "09:30",
            "index_america_trend_session_end_local": "16:00",
            "index_europe_trend_session_start_local": "08:00",
            "index_europe_trend_session_end_local": "17:00",
            "index_japan_trend_session_start_local": "09:00",
            "index_japan_trend_session_end_local": "15:00",
        },
    )
    ts = _ts("13:00")
    us_state = strategy._trend_session_state("US30", [ts])  # type: ignore[attr-defined]
    eu_state = strategy._trend_session_state("DE40", [ts])  # type: ignore[attr-defined]
    jp_state = strategy._trend_session_state("JP225", [ts])  # type: ignore[attr-defined]
    assert us_state.get("trend_session") is False
    assert eu_state.get("trend_session") is True
    assert jp_state.get("trend_session") is False
    assert us_state.get("trend_session_start_local_time_active") == "09:30"
    assert eu_state.get("trend_session_start_local_time_active") == "08:00"
    assert jp_state.get("trend_session_start_local_time_active") == "09:00"


def test_index_hybrid_market_presets_expose_dst_adjusted_utc_window():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
            "index_europe_trend_session_start_local": "08:00",
            "index_europe_trend_session_end_local": "17:00",
        },
    )
    pre_dst = datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc).timestamp()
    post_dst = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc).timestamp()

    pre_state = strategy._trend_session_state("DE40", [pre_dst])  # type: ignore[attr-defined]
    post_state = strategy._trend_session_state("DE40", [post_dst])  # type: ignore[attr-defined]

    assert pre_state.get("trend_session_start_utc_minute_active") == 8 * 60
    assert post_state.get("trend_session_start_utc_minute_active") == 7 * 60
    assert pre_state.get("trend_session_utc_offset_minutes") == 0
    assert post_state.get("trend_session_utc_offset_minutes") == 60
    assert post_state.get("trend_session_start_utc_time_active") == "07:00"


def test_index_hybrid_market_presets_distinguish_japan_vs_australia_timezones():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
        },
    )
    ts = _ts("23:30")
    jp_state = strategy._trend_session_state("JP225", [ts])  # type: ignore[attr-defined]
    au_state = strategy._trend_session_state("AUS200", [ts])  # type: ignore[attr-defined]
    assert jp_state.get("trend_session") is False
    assert au_state.get("trend_session") is True
    assert jp_state.get("trend_session_timezone") == "Asia/Tokyo"
    assert au_state.get("trend_session_timezone") == "Australia/Sydney"


def test_index_hybrid_session_state_marks_degraded_when_timestamps_missing():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
        },
    )
    state = strategy._trend_session_state("US100", [])  # type: ignore[attr-defined]
    assert state.get("trend_session") is None
    assert state.get("session_hour_utc") is None
    assert state.get("trend_session_timestamp_source") == "context_missing"
    assert state.get("trend_session_timestamp_fallback") is True
    assert state.get("trend_session_timestamp_error") == "missing"


def test_index_hybrid_session_state_marks_degraded_when_timestamp_invalid():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_session_filter_enabled": True,
            "index_session_profile_mode": "market_presets",
        },
    )
    state = strategy._trend_session_state("US100", [float("nan")])  # type: ignore[attr-defined]
    assert state.get("trend_session") is None
    assert state.get("session_hour_utc") is None
    assert state.get("trend_session_timestamp_source") == "context_invalid"
    assert state.get("trend_session_timestamp_fallback") is True
    assert state.get("trend_session_timestamp_error") == "invalid"


def test_index_hybrid_sets_session_filter_degraded_metadata_on_missing_timestamps():
    strategy = create_strategy("index_hybrid", {"index_session_filter_enabled": True})
    prices = [100.0 + (0.01 * idx) for idx in range(200)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.metadata.get("session_filter_degraded") is True
    assert signal.metadata.get("trend_session_timestamp_source") == "context_missing"


def test_index_hybrid_resamples_to_closed_candles_in_always_mode():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 4,
            "index_zscore_window": 6,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
            "index_resample_mode": "always",
            "index_timeframe_sec": 60.0,
            "index_candle_timeframe_sec": 60.0,
        },
    )
    start_ts = _ts("14:00")
    timestamps = [start_ts + (idx * 15.0) for idx in range(80)]
    prices = [100.0 + (idx * 0.05) for idx in range(80)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=1.0))
    assert signal.metadata.get("using_closed_candles") is True
    assert signal.metadata.get("timeframe_sec") == pytest.approx(60.0)
    assert signal.metadata.get("raw_history") == len(prices)
    assert signal.metadata.get("resampled_history", 0) < len(prices)
    assert signal.metadata.get("timeframe_normalization_degraded") is False


def test_index_hybrid_marks_timeframe_normalization_degraded_without_timestamps():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_fast_ema_window": 3,
            "index_slow_ema_window": 5,
            "index_donchian_window": 4,
            "index_zscore_window": 6,
            "index_zscore_ema_window": 5,
            "index_atr_window": 3,
            "index_session_filter_enabled": False,
            "index_resample_mode": "always",
            "index_timeframe_sec": 60.0,
            "index_candle_timeframe_sec": 60.0,
        },
    )
    prices = [100.0 + (idx * 0.02) for idx in range(200)]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.metadata.get("using_closed_candles") is False
    assert signal.metadata.get("timeframe_normalization_degraded") is True
    assert signal.metadata.get("timeframe_normalization_reason") == "timestamps_unavailable"


def test_index_hybrid_auto_resample_excludes_last_candle_for_candle_like_input():
    strategy = create_strategy(
        "index_hybrid",
        {
            "index_resample_mode": "auto",
            "index_candle_timeframe_sec": 60.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0]
    timestamps = [0.0, 60.0, 120.0, 180.0]
    closes, close_timestamps, using_closed, degraded, degraded_reason = strategy._resample_closed_candle_closes(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is True
    assert degraded is False
    assert degraded_reason is None
    assert closes == pytest.approx(prices[:-1])
    assert close_timestamps == pytest.approx(timestamps[:-1])


def test_mean_breakout_v2_buy_signal():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "mean_breakout_v2"
    assert signal.metadata.get("direction") == "up"
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("trailing_enabled") is True
    assert float(trailing.get("trailing_distance_pips", 0.0)) > 0.0
    assert float(trailing.get("trailing_activation_pips", 0.0)) > 0.0


def test_mean_breakout_v2_drops_non_finite_prices_before_signal_generation():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
        },
    )
    prices = [100.0, 100.0, 100.0, float("nan"), 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("invalid_prices_dropped") == 1


def test_mean_breakout_v2_defaults_to_auto_resample_mode():
    strategy = create_strategy("mean_breakout_v2", {})
    assert strategy.resample_mode == "auto"  # type: ignore[attr-defined]


def test_mean_breakout_v2_sell_signal():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
        },
    )
    prices = [106.0, 106.0, 106.0, 106.0, 106.0, 106.0, 106.0, 105.0, 104.0, 103.0, 102.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "mean_breakout_v2"
    assert signal.metadata.get("direction") == "down"


def test_mean_breakout_v2_hold_with_exit_hint_on_momentum_exhaustion():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 1.5,
            "mb_min_slope_ratio": 0.0001,
            "mb_exit_z_level": 0.5,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100"))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("indicator") == "mean_breakout_v2"
    assert signal.metadata.get("reason") == "no_signal"
    assert signal.metadata.get("exit_hint") == "momentum_exhaustion"


def test_mean_breakout_v2_adapts_sl_tp_by_timeframe_from_timestamps():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_resample_mode": "off",
            "mb_adaptive_timeframe_mode": "signal",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    t0 = _ts("10:00")

    m5_signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=[t0 + (idx * 60) for idx in range(len(prices))], tick_size=1.0)
    )
    assert m5_signal.side == Side.BUY
    assert m5_signal.metadata.get("timeframe_mode") == "M5_or_lower"
    assert m5_signal.stop_loss_pips == pytest.approx(40.0)
    assert m5_signal.take_profit_pips == pytest.approx(100.0)

    m15_signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=[t0 + (idx * 600) for idx in range(len(prices))], tick_size=1.0)
    )
    assert m15_signal.side == Side.BUY
    assert m15_signal.metadata.get("timeframe_mode") == "M15"
    assert m15_signal.stop_loss_pips == pytest.approx(60.0)
    assert m15_signal.take_profit_pips == pytest.approx(180.0)

    h1_signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=[t0 + (idx * 3600) for idx in range(len(prices))], tick_size=1.0)
    )
    assert h1_signal.side == Side.BUY
    assert h1_signal.metadata.get("timeframe_mode") == "H1_plus"
    assert h1_signal.stop_loss_pips == pytest.approx(120.0)
    assert h1_signal.take_profit_pips == pytest.approx(400.0)


def test_mean_breakout_v2_enforces_rr_floor_after_timeframe_multiplier():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 20.0,
            "mb_risk_reward_ratio": 3.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_sl_mult_m5": 1.0,
            "mb_tp_mult_m5": 1.0,
            "mb_adaptive_timeframe_mode": "signal",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(40.0)
    assert signal.take_profit_pips == pytest.approx(120.0)


def test_mean_breakout_v2_uses_atr_floor_for_stop_loss():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 5.0,
            "mb_take_profit_pips": 10.0,
            "mb_risk_reward_ratio": 2.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 2.0,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_sl_mult_m5": 1.0,
            "mb_tp_mult_m5": 1.0,
        },
    )
    prices = [100.0, 110.0, 90.0, 110.0, 90.0, 110.0, 95.0, 110.0, 90.0, 110.0, 100.0, 130.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("atr_pips", 0.0) > 0.0
    assert signal.stop_loss_pips == pytest.approx(max(5.0, signal.metadata.get("atr_pips", 0.0) * 2.0))


def test_mean_breakout_v2_dynamic_tp_only_respects_static_tp_floor_by_default():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 20.0,
            "mb_take_profit_pips": 120.0,
            "mb_risk_reward_ratio": 2.0,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": True,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(20.0)
    assert signal.take_profit_pips == pytest.approx(120.0)
    assert signal.metadata.get("tp_dynamic_from_sl") == pytest.approx(40.0)
    assert signal.metadata.get("tp_static_floor") == pytest.approx(120.0)
    assert signal.metadata.get("dynamic_tp_respect_static_floor") is True


def test_mean_breakout_v2_dynamic_tp_only_can_ignore_static_tp_floor_when_disabled():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 20.0,
            "mb_take_profit_pips": 120.0,
            "mb_risk_reward_ratio": 2.0,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": False,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(20.0)
    assert signal.take_profit_pips == pytest.approx(40.0)
    assert signal.metadata.get("tp_dynamic_from_sl") == pytest.approx(40.0)
    assert signal.metadata.get("tp_static_floor") == pytest.approx(120.0)
    assert signal.metadata.get("dynamic_tp_respect_static_floor") is False


def test_mean_breakout_v2_can_cap_static_sl_floor_by_atr_ratio():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 50.0,
            "mb_min_stop_loss_pips": 50.0,
            "mb_take_profit_pips": 120.0,
            "mb_risk_reward_ratio": 2.0,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": False,
            "mb_adaptive_sl_max_atr_ratio": 2.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.5,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.2, 100.4, 100.6, 100.8, 101.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("adaptive_sl_before_cap") == pytest.approx(50.0)
    assert signal.metadata.get("adaptive_sl_cap_pips") is not None
    assert signal.metadata.get("adaptive_sl_effective", 50.0) < 50.0
    assert signal.stop_loss_pips == pytest.approx(signal.metadata.get("adaptive_sl_effective"))


def test_mean_breakout_v2_uses_configured_timeframe_fallback_without_timestamps():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_timeframe_sec": 3600.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("timeframe_mode") == "H1_plus"
    assert signal.stop_loss_pips == pytest.approx(120.0)
    assert signal.take_profit_pips == pytest.approx(400.0)


def test_mean_breakout_v2_breakout_horizon_mode_enables_m15_multipliers_on_m1():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 15,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_timeframe_sec": 60.0,
            "mb_candle_timeframe_sec": 60.0,
            "mb_adaptive_timeframe_mode": "breakout_horizon",
            "mb_sl_mult_m5": 1.0,
            "mb_tp_mult_m5": 1.0,
            "mb_sl_mult_m15": 1.3,
            "mb_tp_mult_m15": 1.6,
            "mb_sl_mult_h1": 1.8,
            "mb_tp_mult_h1": 2.4,
        },
    )
    prices = [100.0] * 16 + [101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    assert signal.metadata.get("timeframe_sec") == pytest.approx(60.0)
    assert signal.metadata.get("adaptive_timeframe_mode") == "breakout_horizon"
    assert signal.metadata.get("adaptive_timeframe_sec") == pytest.approx(900.0)
    assert signal.metadata.get("timeframe_mode") == "M15"
    assert signal.stop_loss_pips == pytest.approx(52.0)
    assert signal.take_profit_pips == pytest.approx(160.0)


def test_mean_breakout_v2_breakout_horizon_mode_can_enable_h1_multipliers_on_m1():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 20,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_timeframe_sec": 60.0,
            "mb_candle_timeframe_sec": 60.0,
            "mb_adaptive_timeframe_mode": "breakout_horizon",
            "mb_sl_mult_m5": 1.0,
            "mb_tp_mult_m5": 1.0,
            "mb_sl_mult_m15": 1.3,
            "mb_tp_mult_m15": 1.6,
            "mb_sl_mult_h1": 1.8,
            "mb_tp_mult_h1": 2.4,
        },
    )
    prices = [100.0] * 18 + [101.0, 102.0, 103.0, 104.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    assert signal.metadata.get("timeframe_sec") == pytest.approx(60.0)
    assert signal.metadata.get("adaptive_timeframe_mode") == "breakout_horizon"
    assert signal.metadata.get("adaptive_timeframe_sec") == pytest.approx(1200.0)
    assert signal.metadata.get("timeframe_mode") == "H1_plus"
    assert signal.stop_loss_pips == pytest.approx(72.0)
    assert signal.take_profit_pips == pytest.approx(240.0)


def test_mean_breakout_v2_holds_on_tick_noise_when_closed_candle_mode_is_enabled():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_candle_timeframe_sec": 300.0,
            "mb_resample_mode": "always",
        },
    )
    prices = [5000.0 + (0.1 * idx) for idx in range(120)]
    t0 = _ts("10:00")
    timestamps = [t0 + (idx * 5) for idx in range(len(prices))]

    signal = strategy.generate_signal(_ctx(prices, symbol="GOLD", timestamps=timestamps, tick_size=0.1))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "insufficient_candle_history"
    assert signal.metadata.get("using_closed_candles") is True
    assert signal.metadata.get("candle_timeframe_sec") == 300


def test_mean_breakout_v2_auto_resample_does_not_treat_15s_snapshots_as_candles():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_candle_timeframe_sec": 60.0,
            "mb_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    timestamps = [0.0, 15.0, 30.0, 60.0, 75.0, 90.0]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]

    assert using_closed is False
    assert closes == pytest.approx(prices)


def test_mean_breakout_v2_auto_resample_still_normalizes_candle_like_sunday_filter():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_candle_timeframe_sec": 60.0,
            "mb_resample_mode": "auto",
            "mb_ignore_sunday_candles": True,
        },
    )
    sat_2358 = datetime(2026, 3, 7, 23, 58, tzinfo=timezone.utc).timestamp()
    sat_2359 = datetime(2026, 3, 7, 23, 59, tzinfo=timezone.utc).timestamp()
    sun_0000 = datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc).timestamp()
    sun_0001 = datetime(2026, 3, 8, 0, 1, tzinfo=timezone.utc).timestamp()
    prices = [100.0, 101.0, 102.0, 103.0]
    timestamps = [sat_2358, sat_2359, sun_0000, sun_0001]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]

    assert using_closed is True
    # Sunday candles are filtered first, then last potentially incomplete candle is excluded.
    assert closes == pytest.approx([100.0])


def test_donchian_breakout_auto_resample_uses_raw_prices_for_snapshot_flow():
    strategy = create_strategy(
        "donchian_breakout",
        {
            "donchian_breakout_candle_timeframe_sec": 60.0,
            "donchian_breakout_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    timestamps = [0.0, 15.0, 30.0, 60.0, 75.0, 90.0]
    opens, closes, highs, lows, close_timestamps, using_closed = strategy._resample_closed_candle_ohlc(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is False
    assert opens == pytest.approx(prices)
    assert closes == pytest.approx(prices)
    assert highs == pytest.approx(prices)
    assert lows == pytest.approx(prices)
    assert close_timestamps == pytest.approx(timestamps)


def test_mean_breakout_v2_resamples_ticks_into_closed_candles_for_signal_generation():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 5,
            "mb_breakout_window": 3,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_candle_timeframe_sec": 60.0,
            "mb_resample_mode": "always",
        },
    )
    candle_closes = [100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 107.0]
    prices: list[float] = []
    timestamps: list[float] = []
    t0 = _ts("10:00")
    for candle_idx, close in enumerate(candle_closes):
        candle_start = t0 + (candle_idx * 60)
        for tick_idx in range(12):
            prices.append(close)
            timestamps.append(candle_start + (tick_idx * 5))

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=1.0))

    assert signal.side == Side.BUY
    assert signal.metadata.get("using_closed_candles") is True
    assert signal.metadata.get("timeframe_sec") == pytest.approx(60.0)
    assert signal.metadata.get("timeframe_mode") == "M5_or_lower"


def test_mean_breakout_v2_requires_minimum_zscore_strength_in_max_abs_mode():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 3.0,
            "mb_min_slope_ratio": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "zscore_below_threshold"


def test_mean_breakout_v2_accepts_strong_breakout_in_max_abs_mode():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_min_slope_ratio": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY


def test_mean_breakout_v2_zscore_uses_previous_window_baseline():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.5, 101.0, 101.2, 101.4, 101.7]

    zscore, _slope = strategy._get_indicators(prices)  # type: ignore[attr-defined]

    baseline = prices[-9:-1]
    baseline_ema = ema(baseline, 8)
    _, baseline_std = mean_std([value - baseline_ema for value in baseline])
    expected = (prices[-1] - baseline_ema) / baseline_std

    inclusive = prices[-8:]
    inclusive_ema = ema(inclusive, 8)
    _, inclusive_std = mean_std([value - inclusive_ema for value in inclusive])
    old_style = (prices[-1] - inclusive_ema) / inclusive_std

    assert zscore == pytest.approx(expected)
    assert zscore > old_style


def test_mean_breakout_v2_slope_uses_continuous_ema_series():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 3,
            "mb_zscore_entry_mode": "off",
        },
    )
    prices = [
        88.513, 86.334, 83.551, 83.807, 81.239, 81.326, 84.152, 87.041,
        89.699, 88.143, 85.342, 83.359, 83.638, 84.289, 85.901, 83.659,
    ]

    _zscore, slope = strategy._get_indicators(prices)  # type: ignore[attr-defined]

    ema_series = strategy._ema_series(prices, 8)  # type: ignore[attr-defined]
    segment = ema_series[-6:]
    points = len(segment)
    x_mean = (points - 1) / 2.0
    y_mean = sum(segment) / points
    denominator = sum((idx - x_mean) ** 2 for idx in range(points))
    numerator = sum((idx - x_mean) * (value - y_mean) for idx, value in enumerate(segment))
    expected = (numerator / denominator) / (sum(abs(value) for value in segment) / points)

    old_current = ema(prices[-8:], 8)
    old_prev = ema(prices[-11:-3], 8)
    old_style = (old_current - old_prev) / old_prev

    assert slope == pytest.approx(expected)
    assert slope < 0.0
    assert old_style > 0.0
    assert slope > ((ema_series[-1] - ema_series[-4]) / ema_series[-4])


def test_mean_breakout_v2_directional_extreme_uses_effective_threshold_ratio():
    prices = [100.0, 100.2206, 100.4258, 100.5142, 100.3441, 100.4922, 100.7238, 100.6484, 100.4559, 100.3564, 100.5837, 100.6947]

    strict = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "directional_extreme",
            "mb_zscore_threshold": 1.5,
            "mb_directional_extreme_threshold_ratio": 1.0,
            "mb_min_slope_ratio": 0.0,
        },
    )
    relaxed = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "directional_extreme",
            "mb_zscore_threshold": 1.5,
            "mb_directional_extreme_threshold_ratio": 0.85,
            "mb_min_slope_ratio": 0.0,
        },
    )

    strict_signal = strict.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    relaxed_signal = relaxed.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert strict_signal.side == Side.HOLD
    assert strict_signal.metadata.get("reason") == "zscore_directional_not_confirmed"
    assert strict_signal.metadata.get("zscore_effective_threshold") == pytest.approx(1.5)

    assert relaxed_signal.side == Side.BUY
    assert relaxed_signal.metadata.get("zscore_effective_threshold") == pytest.approx(1.275)


def test_mean_breakout_v2_requires_min_breakout_buffer_in_pips():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.5,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.2]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"
    assert signal.metadata.get("breakout_up_raw") is True
    assert signal.metadata.get("breakout_up") is False
    assert signal.metadata.get("breakout_buffer_pips") == pytest.approx(0.5)


def test_mean_breakout_v2_can_require_breakout_to_cover_spread_buffer():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 1.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.2]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"
    assert signal.metadata.get("breakout_up_raw") is True
    assert signal.metadata.get("breakout_up") is False
    assert signal.metadata.get("breakout_buffer_spread_price") == pytest.approx(1.0)


def test_mean_breakout_v2_holds_on_signal_when_spread_is_too_wide():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_max_spread_pips": 0.5,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"
    assert signal.metadata.get("spread_pips") == pytest.approx(1.0)
    assert signal.metadata.get("max_spread_pips") == pytest.approx(0.5)


def test_mean_breakout_v2_marks_breakout_invalidation_on_reentry_into_channel():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
            "mb_breakout_invalidation_enabled": True,
            "mb_breakout_invalidation_lookback_bars": 1,
        },
    )
    # Previous bar is an upside breakout, current bar re-enters the channel.
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 103.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_invalidated"
    assert signal.metadata.get("exit_hint") == "close_on_breakout_invalidation"
    assert signal.metadata.get("breakout_invalidation_up") is True
    assert signal.metadata.get("breakout_invalidation_down") is False


def test_mean_breakout_v2_blocks_same_side_reentry_after_breakout_invalidation():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
            "mb_breakout_invalidation_enabled": True,
            "mb_breakout_invalidation_lookback_bars": 1,
            "mb_resample_mode": "off",
            "mean_breakout_v2_same_side_reentry_win_cooldown_sec": 300.0,
            "mean_breakout_v2_same_side_reentry_reset_on_opposite_signal": True,
        },
    )
    # First call: recent upside breakout gets invalidated and arms local cooldown.
    invalidation_prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 103.0]
    t0 = _ts("10:00")
    invalidation_ts = [t0 + (idx * 60) for idx in range(len(invalidation_prices))]
    invalidation_signal = strategy.generate_signal(
        _ctx(invalidation_prices, symbol="US100", tick_size=1.0, timestamps=invalidation_ts)
    )
    assert invalidation_signal.side == Side.HOLD
    assert invalidation_signal.metadata.get("reason") == "breakout_invalidated"
    assert invalidation_signal.metadata.get("local_same_side_reentry_block_side") == "buy"

    # Second call shortly after: same-side BUY breakout is blocked by local cooldown.
    reentry_prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    reentry_ts = [t0 + 180.0 + (idx * 60) for idx in range(len(reentry_prices))]
    reentry_signal = strategy.generate_signal(_ctx(reentry_prices, symbol="US100", tick_size=1.0, timestamps=reentry_ts))

    assert reentry_signal.side == Side.HOLD
    assert reentry_signal.metadata.get("reason") == "same_side_reentry_cooldown_active"
    assert reentry_signal.metadata.get("local_same_side_reentry_block_side") == "buy"
    assert reentry_signal.metadata.get("local_same_side_reentry_remaining_sec", 0.0) > 0.0


def test_mean_breakout_v2_holds_when_cfd_tick_size_is_missing():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_require_context_tick_size_for_cfd": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_mean_breakout_v2_treats_xauusd_as_cfd_for_tick_size_requirement():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_require_context_tick_size_for_cfd": True,
            "mb_resample_mode": "off",
        },
    )
    prices = [2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2000.0, 2001.0, 2002.0, 2003.0, 2004.0, 2006.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="XAUUSD", tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_mean_breakout_v2_normalizes_fx_tick_size_to_pip_size():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_resample_mode": "off",
        },
    )
    prices = [1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1010, 1.1020, 1.1030, 1.1040, 1.1060]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.00001))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size_source") == "context_tick_size"
    assert signal.metadata.get("pip_size") == pytest.approx(0.0001)


def test_mean_breakout_v2_index_pip_fallback_handles_aus200():
    strategy = create_strategy("mean_breakout_v2", {})
    assert strategy._pip_size("AUS200") == pytest.approx(1.0)  # type: ignore[attr-defined]


def test_mean_breakout_v2_blocks_when_volume_spike_is_required():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_volume_confirmation": True,
            "mb_volume_window": 5,
            "mb_volume_min_samples": 3,
            "mb_min_volume_ratio": 1.4,
            "mb_volume_require_spike": True,
            "mb_volume_allow_missing": False,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    volumes = [100.0, 103.0, 98.0, 101.0, 102.0, 100.0, 99.0, 104.0, 100.0, 101.0, 99.0, 120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.BUY
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_mean_breakout_v2_empty_string_bool_uses_default_false():
    strategy = create_strategy("mean_breakout_v2", {"mb_volume_confirmation": ""})
    assert strategy.volume_confirmation is False


def test_mean_breakout_v2_volume_spike_boosts_confidence():
    base_params = {
        "mb_zscore_window": 8,
        "mb_breakout_window": 4,
        "mb_slope_window": 2,
        "mb_zscore_threshold": 0.8,
        "mb_zscore_entry_mode": "off",
        "mb_min_slope_ratio": 0.0,
        "mb_volume_window": 5,
        "mb_volume_min_samples": 3,
        "mb_min_volume_ratio": 1.4,
        "mb_volume_allow_missing": True,
        "mb_volume_confidence_boost": 0.2,
    }
    base_strategy = create_strategy("mean_breakout_v2", base_params)
    boosted_strategy = create_strategy(
        "mean_breakout_v2",
        {
            **base_params,
            "mb_volume_confirmation": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    volumes = [100.0, 103.0, 98.0, 101.0, 102.0, 100.0, 99.0, 104.0, 100.0, 101.0, 99.0, 180.0]
    base_signal = base_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    boosted_signal = boosted_strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert base_signal.side == Side.BUY
    assert boosted_signal.side == Side.BUY
    assert boosted_signal.confidence > base_signal.confidence
    assert boosted_signal.metadata.get("volume_spike") is True


def test_mean_breakout_v2_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_volume_confirmation": True,
            "mb_volume_allow_missing": False,
            "mb_volume_min_samples": 10,
        },
    )
    metadata, hold_reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 80, symbol="SK20", volumes=[], current_volume=None)
    )
    assert hold_reason is None
    assert boost == pytest.approx(0.0)
    assert metadata.get("volume_allow_missing_effective") is True
    assert metadata.get("volume_missing_relaxed_for_index") is True


def test_mean_breakout_v2_volume_confirmation_requires_entry_ratio_even_without_spike_mode():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.8,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_volume_confirmation": True,
            "mb_volume_window": 5,
            "mb_volume_min_samples": 3,
            "mb_min_volume_ratio": 1.3,
            "mb_volume_min_ratio_for_entry": 1.05,
            "mb_volume_require_spike": False,
            "mb_volume_allow_missing": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    volumes = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))

    assert signal.side == Side.BUY
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_confirmed") is False
    assert signal.metadata.get("volume_spike") is False
    assert signal.metadata.get("volume_effective_required_ratio") == pytest.approx(1.05)
    assert signal.metadata.get("volume_ratio") == pytest.approx(1.0)


def test_mean_breakout_v2_volume_confirmation_uses_aligned_closed_candle_samples():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 5,
            "mb_breakout_window": 3,
            "mb_slope_window": 2,
            "mb_zscore_threshold": 0.5,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_candle_timeframe_sec": 60.0,
            "mb_resample_mode": "always",
            "mb_volume_confirmation": True,
            "mb_volume_window": 5,
            "mb_volume_min_samples": 3,
            "mb_volume_min_ratio_for_entry": 1.05,
            "mb_min_volume_ratio": 1.3,
            "mb_volume_allow_missing": False,
            "mb_volume_require_spike": False,
        },
    )
    candle_closes = [100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0, 107.0]
    prices: list[float] = []
    volumes: list[float] = []
    timestamps: list[float] = []
    t0 = _ts("10:00")
    for candle_idx, close in enumerate(candle_closes):
        candle_start = t0 + (candle_idx * 60)
        for tick_idx in range(12):
            prices.append(close)
            volumes.append(10.0 if candle_idx < (len(candle_closes) - 1) else 500.0)
            timestamps.append(candle_start + (tick_idx * 5))

    signal = strategy.generate_signal(
        _ctx(
            prices,
            symbol="US100",
            timestamps=timestamps,
            volumes=volumes,
            current_volume=9000.0,
            tick_size=1.0,
        )
    )

    assert signal.side == Side.BUY
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("using_closed_candles") is True
    assert signal.metadata.get("volume_samples_aligned") is True
    assert signal.metadata.get("volume_compare_closed_only") is True
    assert signal.metadata.get("volume_current_source") == "aligned_samples_last"
    assert signal.metadata.get("volume_ratio") == pytest.approx(1.0)
    assert signal.metadata.get("volume_spike") is False


def test_mean_breakout_v2_trailing_caps_activation_by_tp_ratio():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 50.0,
            "mb_take_profit_pips": 110.0,
            "mb_risk_reward_ratio": 2.2,
            "mb_dynamic_tp_only": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_trailing_atr_multiplier": 1.2,
            "mb_trailing_activation_stop_ratio": 0.8,
            "mb_trailing_activation_max_tp_ratio": 0.3,
            "mb_trailing_distance_min_stop_ratio": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("trailing_enabled") is True
    assert trailing.get("trailing_activation_from_stop_pips") == pytest.approx(40.0)
    assert trailing.get("trailing_activation_cap_tp_pips") == pytest.approx(33.0)
    assert trailing.get("trailing_activation_pips") == pytest.approx(33.0)
    assert trailing.get("trailing_activation_ratio") == pytest.approx(0.3)


def test_mean_breakout_v2_trailing_distance_has_stop_floor():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 50.0,
            "mb_take_profit_pips": 110.0,
            "mb_risk_reward_ratio": 2.2,
            "mb_dynamic_tp_only": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_trailing_atr_multiplier": 1.2,
            "mb_trailing_activation_stop_ratio": 0.8,
            "mb_trailing_activation_max_tp_ratio": 0.8,
            "mb_trailing_distance_min_stop_ratio": 0.5,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("trailing_enabled") is True
    assert float(trailing.get("trailing_distance_from_atr_pips", 0.0)) < 25.0
    assert trailing.get("trailing_distance_from_stop_floor_pips") == pytest.approx(25.0)
    assert trailing.get("trailing_distance_pips") == pytest.approx(25.0)


def test_mean_breakout_v2_trailing_can_publish_breakeven_offset():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 50.0,
            "mb_take_profit_pips": 110.0,
            "mb_risk_reward_ratio": 2.2,
            "mb_dynamic_tp_only": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_trailing_atr_multiplier": 1.2,
            "mb_trailing_activation_stop_ratio": 0.8,
            "mb_trailing_breakeven_offset_pips": 2.5,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    trailing = signal.metadata.get("trailing_stop")
    assert isinstance(trailing, dict)
    assert trailing.get("trailing_breakeven_offset_pips") == pytest.approx(2.5)


def test_mean_breakout_v2_confidence_penalizes_overextended_breakout():
    params = {
        "mb_zscore_window": 8,
        "mb_breakout_window": 4,
        "mb_slope_window": 2,
        "mb_zscore_entry_mode": "off",
        "mb_min_slope_ratio": 0.0,
        "mb_adaptive_timeframe_mode": "signal",
        "mb_stop_loss_pips": 40.0,
        "mb_take_profit_pips": 100.0,
        "mb_risk_reward_ratio": 2.5,
        "mb_dynamic_tp_only": True,
        "mb_dynamic_tp_respect_static_floor": False,
        "mb_min_stop_loss_pips": 1.0,
        "mb_min_take_profit_pips": 1.0,
        "mb_min_relative_stop_pct": 0.0,
        "mb_atr_window": 3,
        "mb_atr_multiplier": 0.1,
        "mb_breakout_min_buffer_atr_ratio": 0.0,
        "mb_breakout_min_buffer_pips": 0.0,
        "mb_breakout_min_buffer_spread_multiplier": 0.0,
        "mb_confidence_base": 0.2,
        "mb_confidence_gate_margin_weight": 0.25,
        "mb_confidence_cost_weight": 0.2,
        "mb_confidence_extension_weight": 0.2,
        "mb_confidence_extension_soft_ratio": 0.2,
        "mb_confidence_extension_hard_ratio": 0.8,
    }
    strategy = create_strategy("mean_breakout_v2", params)

    moderate_prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]
    extended_prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 118.0]

    moderate = strategy.generate_signal(_ctx(moderate_prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))
    extended = strategy.generate_signal(_ctx(extended_prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))

    assert moderate.side == Side.BUY
    assert extended.side == Side.BUY
    assert moderate.confidence > extended.confidence
    assert moderate.metadata.get("confidence_extension_score", 0.0) > extended.metadata.get(
        "confidence_extension_score",
        0.0,
    )
    assert moderate.metadata.get("confidence_breakout_to_sl_ratio", 0.0) < extended.metadata.get(
        "confidence_breakout_to_sl_ratio",
        0.0,
    )


def test_mean_breakout_v2_blocks_exhaustion_breakout():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "directional_extreme",
            "mb_zscore_threshold": 0.8,
            "mb_directional_extreme_threshold_ratio": 1.0,
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 130.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_exhausted"
    assert signal.metadata.get("breakout_exhaustion_guard_triggered") is True
    assert signal.metadata.get("breakout_exhaustion_sprint_detected") is True
    assert signal.metadata.get("breakout_exhaustion_zscore_ratio", 0.0) > 1.35
    assert signal.metadata.get("breakout_exhaustion_breakout_to_sl_ratio", 0.0) > signal.metadata.get(
        "breakout_exhaustion_breakout_to_sl_threshold",
        1.0,
    )


def test_mean_breakout_v2_allows_accumulated_breakout_without_sprint_exhaustion():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 7,
            "mb_slope_window": 3,
            "mb_zscore_entry_mode": "directional_extreme",
            "mb_zscore_threshold": 0.8,
            "mb_directional_extreme_threshold_ratio": 1.0,
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
            "mb_exhaustion_sprint_lookback_bars": 3,
            "mb_exhaustion_sprint_move_channel_ratio": 0.8,
        },
    )
    prices = [101.0, 100.0, 98.0, 90.0, 92.0, 94.0, 96.0, 98.0, 100.0, 102.0, 104.0, 108.0]

    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))

    assert signal.side == Side.BUY
    assert signal.metadata.get("breakout_exhaustion_guard_triggered") is False
    assert signal.metadata.get("breakout_exhaustion_sprint_detected") is False
    assert signal.metadata.get("breakout_exhaustion_sprint_move_channel_ratio", 1.0) < 0.8


def test_mean_breakout_v2_confidence_quantizes_minor_spread_noise():
    strategy = create_strategy(
        "mean_breakout_v2",
        {
            "mb_zscore_window": 8,
            "mb_breakout_window": 4,
            "mb_slope_window": 2,
            "mb_zscore_entry_mode": "off",
            "mb_min_slope_ratio": 0.0,
            "mb_adaptive_timeframe_mode": "signal",
            "mb_stop_loss_pips": 40.0,
            "mb_take_profit_pips": 100.0,
            "mb_risk_reward_ratio": 2.5,
            "mb_dynamic_tp_only": True,
            "mb_dynamic_tp_respect_static_floor": False,
            "mb_min_stop_loss_pips": 1.0,
            "mb_min_take_profit_pips": 1.0,
            "mb_min_relative_stop_pct": 0.0,
            "mb_atr_window": 3,
            "mb_atr_multiplier": 0.1,
            "mb_breakout_min_buffer_atr_ratio": 0.0,
            "mb_breakout_min_buffer_pips": 0.0,
            "mb_breakout_min_buffer_spread_multiplier": 0.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0]

    low_spread = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.0))
    slightly_wider_spread = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=1.0, current_spread_pips=1.2)
    )

    assert low_spread.side == Side.BUY
    assert slightly_wider_spread.side == Side.BUY
    assert low_spread.confidence == pytest.approx(slightly_wider_spread.confidence)
    assert low_spread.metadata.get("confidence_raw_after_volume_boost", 0.0) > slightly_wider_spread.metadata.get(
        "confidence_raw_after_volume_boost",
        0.0,
    )


def test_g1_buy_signal_with_dynamic_atr_stop_and_tp():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "g1"
    assert signal.metadata.get("trend_signal") == "ema_cross_up"
    assert signal.metadata.get("adx_delta") == pytest.approx(
        float(signal.metadata.get("adx", 0.0)) - float(signal.metadata.get("adx_threshold", 0.0))
    )
    assert signal.metadata.get("adx_delta_to_effective_entry") == pytest.approx(
        float(signal.metadata.get("adx", 0.0)) - float(signal.metadata.get("effective_adx_entry_threshold", 0.0))
    )
    assert signal.stop_loss_pips > 0.0
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 3.0)


def test_g1_drops_non_finite_prices_before_signal_generation():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, float("nan"), 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY


def test_g1_sell_signal_for_reverse_cross():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 101.31, 100.11, 99.87, 98.87, 100.26, 100.76, 99.38]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "g1"
    assert signal.metadata.get("trend_signal") == "ema_cross_down"
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 3.0)


def test_g1_cross_only_mode_holds_without_fresh_cross():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_only",
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ema_cross"
    assert signal.metadata.get("entry_mode") == "cross_only"


def test_g1_defaults_to_cross_only_mode():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ema_cross"
    assert signal.metadata.get("entry_mode") == "cross_only"
    assert signal.metadata.get("configured_entry_mode") == "cross_only"


def test_g1_cross_or_trend_mode_requires_pullback_reset():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_pullback_reset"
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_g1_cross_or_trend_mode_allows_trend_continuation_after_pullback_reset():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_continuation_fast_ema_retest_atr_tolerance": 0.25,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


def test_g1_continuation_confidence_is_capped_separately_from_cross():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_continuation_fast_ema_retest_atr_tolerance": 0.25,
            "g1_continuation_confidence_cap": 0.55,
            "g1_continuation_velocity_weight_multiplier": 1.0,
            "g1_continuation_price_alignment_bonus_multiplier": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("g1_confidence_cap_effective") == pytest.approx(0.55)
    assert signal.confidence <= 0.5500001


def test_g1_entry_mode_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_entry_mode_by_symbol": {"WTI": "cross_only"},
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]

    eurusd_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    wti_signal = strategy.generate_signal(_ctx(prices, symbol="WTI", current_spread_pips=0.8))

    assert eurusd_signal.side == Side.BUY
    assert eurusd_signal.metadata.get("trend_signal") == "ema_trend_up"
    assert eurusd_signal.metadata.get("entry_mode") == "cross_or_trend"

    assert wti_signal.side == Side.HOLD
    assert wti_signal.metadata.get("reason") == "no_ema_cross"
    assert wti_signal.metadata.get("entry_mode") == "cross_only"
    assert wti_signal.metadata.get("configured_entry_mode") == "cross_or_trend"
    assert wti_signal.metadata.get("entry_mode_override_applied") is True


def test_g1_invalid_entry_mode_falls_back_to_cross_only():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "not_a_mode",
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ema_cross"
    assert signal.metadata.get("entry_mode") == "cross_only"


def test_g1_continuation_entry_uses_relaxed_adx_threshold():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.80,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_min_slow_slope_ratio": 0.0,
            "g1_continuation_fast_ema_retest_atr_tolerance": 10.0,
        },
    )
    strategy._adx = lambda _prices, _window: 22.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("is_continuation_entry") is True
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(21.6)


def test_g1_continuation_entry_still_blocks_when_adx_too_low():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.80,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_min_slow_slope_ratio": 0.0,
            "g1_continuation_fast_ema_retest_atr_tolerance": 10.0,
        },
    )
    strategy._adx = lambda _prices, _window: 20.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side != Side.HOLD
    assert "adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("adx_softened") is True
    assert signal.metadata.get("is_continuation_entry") is True
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(21.6)


def test_g1_cross_entry_uses_relaxed_adx_threshold():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_cross_adx_multiplier": 0.70,
            "g1_cross_min_adx": 18.0,
            "g1_cross_min_entry_threshold_ratio": 0.70,
            "g1_min_slow_slope_ratio": 0.00001,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_volume_confirmation": False,
        },
    )
    strategy._adx = lambda _prices, _window: 20.0  # type: ignore[method-assign]
    strategy._atr = lambda _prices, _window: 1.0  # type: ignore[method-assign]
    strategy._confirmed_by_closed_candles = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    strategy._ema_last_two = (  # type: ignore[method-assign]
        lambda _values, window: (99.90, 100.10) if window == 3 else (100.00, 100.03)
    )
    strategy._ema = lambda _values, window: 99.95 if window == 3 else 100.00  # type: ignore[method-assign]
    prices = [100.0, 100.0, 100.0, 100.0, 100.02, 100.04, 100.07, 100.10]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))

    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_cross_up"
    assert signal.metadata.get("is_cross_entry") is True
    assert signal.metadata.get("adx_relief_mode") == "cross"
    assert signal.metadata.get("cross_relief_eligible") is True
    assert float(signal.metadata.get("directional_gap_acceleration_ratio", 0.0)) > 0.0
    assert float(signal.metadata.get("directional_velocity_ratio", 0.0)) > float(
        signal.metadata.get("base_directional_velocity_ratio", 0.0)
    )
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(18.9)


def test_g1_cross_relief_requires_gap_acceleration():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_adx_window": 2,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_cross_adx_multiplier": 0.70,
            "g1_cross_min_adx": 18.0,
            "g1_cross_min_entry_threshold_ratio": 0.70,
            "g1_min_slow_slope_ratio": 0.00001,
            "g1_atr_window": 2,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_volume_confirmation": False,
        },
    )
    strategy._adx = lambda _prices, _window: 20.0  # type: ignore[method-assign]
    strategy._atr = lambda _prices, _window: 1.0  # type: ignore[method-assign]
    strategy._confirmed_by_closed_candles = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    strategy._ema_last_two = (  # type: ignore[method-assign]
        lambda _values, window: (99.90, 100.10) if window == 2 else (100.00, 100.03)
    )
    strategy._ema = lambda _values, window: 99.73 if window == 2 else 100.00  # type: ignore[method-assign]

    signal = strategy.generate_signal(
        _ctx(
            [100.0, 100.0, 100.0, 100.0, 100.02, 100.04, 100.07, 100.10],
            symbol="EURUSD",
            current_spread_pips=0.8,
        )
    )

    assert signal.side != Side.HOLD
    assert "adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("adx_softened") is True
    assert signal.metadata.get("adx_relief_mode") == "cross_strict"
    assert signal.metadata.get("cross_relief_eligible") is False
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(25.0)


def test_g1_blocks_cross_when_slow_ema_is_too_flat():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_adx_window": 2,
            "g1_adx_threshold": 20.0,
            "g1_adx_hysteresis": 0.0,
            "g1_min_slow_slope_ratio": 0.0002,
            "g1_atr_window": 2,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_volume_confirmation": False,
        },
    )
    strategy._adx = lambda _prices, _window: 28.0  # type: ignore[method-assign]
    strategy._atr = lambda _prices, _window: 1.0  # type: ignore[method-assign]
    strategy._confirmed_by_closed_candles = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    strategy._ema_last_two = (  # type: ignore[method-assign]
        lambda _values, window: (99.90, 100.10) if window == 2 else (100.00, 100.01)
    )
    strategy._ema = lambda _values, window: 99.70 if window == 2 else 100.00  # type: ignore[method-assign]

    signal = strategy.generate_signal(
        _ctx(
            [100.0, 100.0, 100.0, 100.0, 100.02, 100.04, 100.07, 100.10],
            symbol="EURUSD",
            current_spread_pips=0.8,
        )
    )

    assert signal.side != Side.HOLD
    assert "slow_ema_too_flat" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("slow_slope_softened") is True
    assert signal.metadata.get("min_slow_slope_ratio") == pytest.approx(0.0002)


def test_g1_continuation_gap_cannot_be_weaker_than_cross_gap():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_cross_gap_ratio": 0.01,
            "g1_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert strategy.min_trend_gap_ratio == pytest.approx(0.01)
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_ema_cross"
    assert signal.metadata.get("g1_min_trend_gap_ratio_configured") == pytest.approx(0.0)
    assert signal.metadata.get("g1_min_trend_gap_ratio_effective") == pytest.approx(0.01)
    assert signal.metadata.get("g1_min_trend_gap_ratio_auto_aligned") is True


def test_g1_holds_when_adx_below_threshold():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 70.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side != Side.HOLD
    assert "adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("adx_softened") is True
    assert signal.metadata.get("adx_delta") == pytest.approx(
        float(signal.metadata.get("adx", 0.0)) - float(signal.metadata.get("adx_threshold", 0.0))
    )
    assert signal.metadata.get("adx_delta_to_effective_entry") == pytest.approx(
        float(signal.metadata.get("adx", 0.0)) - float(signal.metadata.get("effective_adx_entry_threshold", 0.0))
    )


def test_g1_holds_when_spread_is_too_wide():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=1.2))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"


def test_g1_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_max_spread_pips_by_symbol": {"EURUSD": 2.0},
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=1.2))
    assert signal.side == Side.BUY
    assert signal.metadata.get("max_spread_pips") == pytest.approx(2.0)
    assert signal.metadata.get("max_spread_pips_source") == "symbol_override"


def test_g1_profile_specific_adx_threshold_for_fx_and_index():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_fx_adx_threshold": 25.0,
            "g1_index_adx_threshold": 70.0,
            "g1_fx_adx_hysteresis": 0.0,
            "g1_index_adx_hysteresis": 0.0,
            "g1_fx_max_spread_pips": 1.0,
            "g1_index_max_spread_pips": 3.0,
            "g1_fx_max_price_ema_gap_ratio": 1.0,
            "g1_index_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    fx_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert fx_signal.side == Side.BUY
    assert fx_signal.metadata.get("profile") == "fx"

    index_signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=0.8))
    assert index_signal.side != Side.HOLD
    assert index_signal.metadata.get("profile") == "index"
    assert "adx_below_threshold" in (index_signal.metadata.get("soft_filter_reasons") or [])
    assert index_signal.metadata.get("adx_softened") is True


def test_g1_profile_specific_spread_limit_for_fx_and_index():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_fx_max_spread_pips": 1.0,
            "g1_index_max_spread_pips": 3.0,
            "g1_fx_max_price_ema_gap_ratio": 1.0,
            "g1_index_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    fx_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=2.0))
    assert fx_signal.side == Side.HOLD
    assert fx_signal.metadata.get("profile") == "fx"
    assert fx_signal.metadata.get("reason") == "spread_too_wide"

    index_signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=2.0))
    assert index_signal.side == Side.BUY
    assert index_signal.metadata.get("profile") == "index"


def test_g1_profile_specific_spread_limit_for_commodity():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_fx_max_spread_pips": 1.0,
            "g1_index_max_spread_pips": 1.0,
            "g1_commodity_max_spread_pips": 3.0,
            "g1_fx_max_price_ema_gap_ratio": 1.0,
            "g1_index_max_price_ema_gap_ratio": 1.0,
            "g1_commodity_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    commodity_signal = strategy.generate_signal(_ctx(prices, symbol="GOLD", current_spread_pips=2.0))
    assert commodity_signal.side == Side.BUY
    assert commodity_signal.metadata.get("profile") == "commodity"
    assert commodity_signal.metadata.get("max_spread_pips") == pytest.approx(3.0)


def test_g1_index_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_index_max_spread_pips": 1.0,
            "g1_index_max_spread_pips_by_symbol": {"NK20": 3.0},
            "g1_index_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    blocked = strategy.generate_signal(_ctx(prices, symbol="DE40", current_spread_pips=1.4))
    assert blocked.side == Side.HOLD
    assert blocked.metadata.get("reason") == "spread_too_wide"
    assert blocked.metadata.get("max_spread_pips_source") == "global"

    allowed = strategy.generate_signal(_ctx(prices, symbol="NK20", current_spread_pips=1.4))
    assert allowed.side == Side.BUY
    assert allowed.metadata.get("profile") == "index"
    assert allowed.metadata.get("max_spread_pips") == pytest.approx(3.0)
    assert allowed.metadata.get("max_spread_pips_source") == "symbol_override"


def test_g1_profile_override_can_force_commodity_profile():
    strategy = create_strategy(
        "g1",
        {
            "g1_profile_override": "commodity",
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_fx_adx_threshold": 0.0,
            "g1_index_adx_threshold": 0.0,
            "g1_commodity_adx_threshold": 70.0,
            "g1_fx_adx_hysteresis": 0.0,
            "g1_index_adx_hysteresis": 0.0,
            "g1_commodity_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side != Side.HOLD
    assert signal.metadata.get("profile") == "commodity"
    assert "adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("adx_softened") is True


def test_g1_profile_specific_gap_threshold_for_fx_and_index():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_fx_min_cross_gap_ratio": 0.0,
            "g1_index_min_cross_gap_ratio": 1.0,
            "g1_fx_min_trend_gap_ratio": 0.0,
            "g1_index_min_trend_gap_ratio": 0.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    fx_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    index_signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=0.8))

    assert fx_signal.side == Side.BUY
    assert fx_signal.metadata.get("profile") == "fx"
    assert fx_signal.metadata.get("g1_min_cross_gap_ratio") == pytest.approx(0.0)

    assert index_signal.side == Side.HOLD
    assert index_signal.metadata.get("profile") == "index"
    assert index_signal.metadata.get("reason") == "ema_cross_gap_too_small"
    assert index_signal.metadata.get("g1_min_cross_gap_ratio") == pytest.approx(1.0)


def test_g1_profile_specific_velocity_norm_ratio_changes_confidence():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_min_cross_gap_ratio": 0.0,
            "g1_min_trend_gap_ratio": 0.0,
            "g1_fx_confidence_velocity_norm_ratio": 0.0001,
            "g1_index_confidence_velocity_norm_ratio": 0.01,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]

    fx_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    index_signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=0.8))

    assert fx_signal.side == Side.BUY
    assert index_signal.side == Side.BUY
    assert fx_signal.confidence > index_signal.confidence
    assert fx_signal.metadata.get("g1_confidence_velocity_norm_ratio") == pytest.approx(0.0001)
    assert index_signal.metadata.get("g1_confidence_velocity_norm_ratio") == pytest.approx(0.01)


def test_g1_profile_specific_volume_confirmation_for_index_only():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_volume_confirmation": False,
            "g1_index_volume_confirmation": True,
            "g1_index_volume_window": 5,
            "g1_index_volume_min_samples": 3,
            "g1_index_min_volume_ratio": 1.4,
            "g1_index_volume_min_ratio_for_entry": 1.0,
            "g1_index_volume_require_spike": False,
            "g1_index_volume_allow_missing": False,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    volumes = [100.0, 102.0, 99.0, 101.0, 100.0, 99.0, 101.0, 90.0]

    fx_signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8, volumes=volumes))
    index_signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=0.8, volumes=volumes))

    assert fx_signal.side == Side.BUY
    assert fx_signal.metadata.get("volume_confirmation_enabled") is False
    assert fx_signal.metadata.get("volume_profile") == "fx"

    assert index_signal.side != Side.HOLD
    assert "volume_not_confirmed" in (index_signal.metadata.get("soft_filter_reasons") or [])
    assert index_signal.metadata.get("volume_softened") is True
    assert index_signal.metadata.get("volume_confirmation_enabled") is True
    assert index_signal.metadata.get("volume_profile") == "index"


def test_g1_holds_when_price_is_too_far_from_slow_ema():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 2.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 0.005,
            "g1_max_price_ema_gap_atr_multiple": 0.0,
            "g1_fx_max_price_ema_gap_atr_multiple": 0.0,
        },
    )
    prices = [100.0, 101.31, 100.11, 99.87, 98.87, 100.26, 100.76, 99.38]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "price_too_far_from_ema"


def test_g1_price_gap_guard_uses_atr_adaptive_allowance():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 2.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 0.005,
            "g1_max_price_ema_gap_atr_multiple": 1.5,
        },
    )
    strategy._atr = lambda _prices, _window: 2.0  # type: ignore[method-assign]
    prices = [100.0, 101.31, 100.11, 99.87, 98.87, 100.26, 100.76, 99.38]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))

    assert signal.side != Side.HOLD
    assert signal.metadata.get("effective_max_price_ema_gap_ratio") > signal.metadata.get("max_price_ema_gap_ratio")


def test_g1_uses_tick_size_from_context_for_atr_pips():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 20.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 2.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal_small_tick = strategy.generate_signal(
        _ctx(prices, symbol="US100", current_spread_pips=0.8, tick_size=0.1)
    )
    signal_big_tick = strategy.generate_signal(
        _ctx(prices, symbol="US100", current_spread_pips=0.8, tick_size=1.0)
    )

    assert signal_small_tick.side == Side.BUY
    assert signal_big_tick.side == Side.BUY
    assert signal_small_tick.metadata.get("pip_size") == pytest.approx(0.1)
    assert signal_big_tick.metadata.get("pip_size") == pytest.approx(1.0)
    assert signal_small_tick.stop_loss_pips > signal_big_tick.stop_loss_pips


def test_g1_uses_index_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 20.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 2.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=0.8, tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.1)
    assert signal.metadata.get("pip_size_source") == "fallback_symbol_map"


def test_g1_normalizes_fx_tick_size_to_standard_pip_size():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 2.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
        },
    )
    prices = [1.0800, 1.0805, 1.0810, 1.0814, 1.0818, 1.0822, 1.0827, 1.0824]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", current_spread_pips=0.8, tick_size=0.00001, pip_size=None)
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(0.0001)
    assert signal.metadata.get("pip_size_source") == "context_tick_size"


def test_g1_uses_aus200_pip_fallback_when_tick_size_missing():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 20.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 2.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="AUS200", current_spread_pips=0.8, tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)
    assert signal.metadata.get("pip_size_source") == "fallback_symbol_map"


def test_g1_blocks_weak_cross_when_min_cross_gap_ratio_is_set():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_min_cross_gap_ratio": 1.0,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "ema_cross_gap_too_small"


def test_g1_blocks_when_volume_spike_is_required():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_volume_confirmation": True,
            "g1_volume_window": 5,
            "g1_volume_min_samples": 3,
            "g1_min_volume_ratio": 1.4,
            "g1_volume_require_spike": True,
            "g1_volume_allow_missing": False,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    volumes = [100.0, 102.0, 99.0, 101.0, 100.0, 99.0, 101.0, 118.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8, volumes=volumes))
    assert signal.side != Side.HOLD
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_g1_volume_spike_boosts_confidence():
    base_params = {
        "g1_fast_ema_window": 3,
        "g1_slow_ema_window": 5,
        "g1_adx_window": 3,
        "g1_adx_threshold": 25.0,
        "g1_adx_hysteresis": 0.0,
        "g1_atr_window": 3,
        "g1_atr_multiplier": 1.5,
        "g1_risk_reward_ratio": 3.0,
        "g1_max_spread_pips": 1.0,
        "g1_min_stop_loss_pips": 5.0,
        "g1_max_price_ema_gap_ratio": 1.0,
        "g1_volume_confirmation": False,
        "g1_volume_window": 5,
        "g1_volume_min_samples": 3,
        "g1_min_volume_ratio": 1.4,
        "g1_volume_allow_missing": True,
        "g1_volume_confidence_boost": 0.2,
    }
    base_strategy = create_strategy("g1", base_params)
    boosted_strategy = create_strategy("g1", {**base_params, "g1_volume_confirmation": True})
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    volumes = [100.0, 102.0, 99.0, 101.0, 100.0, 99.0, 101.0, 180.0]
    base_signal = base_strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8, volumes=volumes))
    boosted_signal = boosted_strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", current_spread_pips=0.8, volumes=volumes)
    )
    assert base_signal.side == Side.BUY
    assert boosted_signal.side == Side.BUY
    assert boosted_signal.confidence > base_signal.confidence
    assert boosted_signal.metadata.get("volume_spike") is True


def test_g1_volume_confirmation_blocks_below_entry_ratio_without_spike_requirement():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 1.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_volume_confirmation": True,
            "g1_volume_window": 5,
            "g1_volume_min_samples": 3,
            "g1_min_volume_ratio": 1.4,
            "g1_volume_min_ratio_for_entry": 1.0,
            "g1_volume_require_spike": False,
            "g1_volume_allow_missing": False,
        },
    )
    prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    volumes = [100.0, 102.0, 99.0, 101.0, 100.0, 99.0, 101.0, 90.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8, volumes=volumes))

    assert signal.side != Side.HOLD
    assert "volume_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True
    assert signal.metadata.get("volume_spike") is False
    assert signal.metadata.get("volume_effective_required_ratio") == pytest.approx(1.0)
    assert float(signal.metadata.get("volume_ratio") or 0.0) < 1.0


def test_g1_volume_confirmation_deduplicates_current_volume_tail_sample():
    strategy = create_strategy(
        "g1",
        {
            "g1_volume_confirmation": True,
            "g1_volume_allow_missing": False,
            "g1_volume_require_spike": False,
            "g1_volume_min_ratio_for_entry": 0.0,
            "g1_volume_window": 5,
            "g1_volume_min_samples": 1,
        },
    )
    metadata, reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 16, volumes=[100.0, 100.0, 200.0], current_volume=200.0),
        using_closed_candles=False,
    )

    assert reason is None
    assert boost >= 0.0
    assert metadata.get("volume_current_source") == "ctx_volumes_last"
    assert metadata.get("volume_ratio") == pytest.approx(2.0)


def test_g1_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "g1",
        {
            "g1_volume_confirmation": True,
            "g1_volume_allow_missing": False,
            "g1_volume_require_spike": False,
            "g1_volume_window": 5,
            "g1_volume_min_samples": 8,
            "g1_index_volume_confirmation": True,
            "g1_index_volume_allow_missing": False,
            "g1_index_volume_require_spike": False,
            "g1_index_volume_window": 5,
            "g1_index_volume_min_samples": 8,
        },
    )
    metadata, reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 16, symbol="DEMID50", volumes=[], current_volume=None),
        using_closed_candles=False,
    )
    assert reason is None
    assert boost == pytest.approx(0.0)
    assert metadata.get("volume_allow_missing_effective") is True
    assert metadata.get("volume_missing_relaxed_for_index") is True


def test_g1_volume_confirmation_uses_closed_volume_when_entry_uses_incomplete_candle():
    strategy = create_strategy(
        "g1",
        {
            "g1_volume_confirmation": True,
            "g1_volume_allow_missing": False,
            "g1_volume_require_spike": False,
            "g1_volume_min_ratio_for_entry": 0.0,
            "g1_volume_window": 5,
            "g1_volume_min_samples": 1,
            "g1_use_incomplete_candle_for_entry": True,
        },
    )
    metadata, reason, boost = strategy._evaluate_volume_confirmation(  # type: ignore[attr-defined]
        _ctx([100.0] * 16, volumes=[80.0, 100.0, 40.0], current_volume=40.0),
        using_closed_candles=True,
    )

    assert reason is None
    assert boost >= 0.0
    assert metadata.get("volume_compare_closed_only") is True
    assert metadata.get("volume_current_source") == "last_closed_ctx_volume"
    assert metadata.get("volume_current") == pytest.approx(100.0)
    assert metadata.get("volume_ratio") == pytest.approx(100.0 / 80.0)


def test_g1_confidence_rewards_velocity_and_price_alignment():
    strategy = create_strategy("g1", {"g1_confidence_velocity_norm_ratio": 0.0005})
    base_confidence = strategy._confidence(  # type: ignore[attr-defined]
        adx=30.0,
        effective_adx_entry_threshold=25.0,
        ema_gap_ratio=0.0006,
        ema_gap_norm=0.001,
        directional_velocity_ratio=0.0001,
        price_aligned_with_fast_ma=False,
    )
    boosted_confidence = strategy._confidence(  # type: ignore[attr-defined]
        adx=30.0,
        effective_adx_entry_threshold=25.0,
        ema_gap_ratio=0.0006,
        ema_gap_norm=0.001,
        directional_velocity_ratio=0.001,
        price_aligned_with_fast_ma=True,
    )
    assert boosted_confidence > base_confidence


def test_g1_confidence_does_not_auto_pass_on_adx_gate_threshold():
    strategy = create_strategy("g1", {})
    confidence = strategy._confidence(  # type: ignore[attr-defined]
        adx=25.0,
        effective_adx_entry_threshold=25.0,
        ema_gap_ratio=0.0,
        ema_gap_norm=0.001,
        directional_velocity_ratio=0.0,
        price_aligned_with_fast_ma=True,
    )
    assert confidence < 0.55


def test_g1_confidence_weights_are_parametric():
    base_strategy = create_strategy(
        "g1",
        {
            "g1_confidence_base": 0.10,
            "g1_confidence_adx_weight": 0.40,
            "g1_confidence_gap_weight": 0.20,
            "g1_confidence_velocity_weight": 0.0,
            "g1_confidence_price_alignment_bonus": 0.0,
        },
    )
    velocity_strategy = create_strategy(
        "g1",
        {
            "g1_confidence_base": 0.10,
            "g1_confidence_adx_weight": 0.40,
            "g1_confidence_gap_weight": 0.0,
            "g1_confidence_velocity_weight": 0.50,
            "g1_confidence_price_alignment_bonus": 0.0,
        },
    )

    base_confidence = base_strategy._confidence(  # type: ignore[attr-defined]
        adx=30.0,
        effective_adx_entry_threshold=25.0,
        ema_gap_ratio=0.0006,
        ema_gap_norm=0.001,
        directional_velocity_ratio=0.001,
        price_aligned_with_fast_ma=False,
    )
    velocity_confidence = velocity_strategy._confidence(  # type: ignore[attr-defined]
        adx=30.0,
        effective_adx_entry_threshold=25.0,
        ema_gap_ratio=0.0006,
        ema_gap_norm=0.001,
        directional_velocity_ratio=0.001,
        price_aligned_with_fast_ma=False,
    )

    assert velocity_confidence > base_confidence


def test_g1_can_require_tick_size_for_index():
    strategy = create_strategy(
        "g1",
        {
            "g1_index_require_context_tick_size": True,
            "g1_candle_timeframe_sec": 0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0] * 120, symbol="US100", tick_size=None))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "tick_size_unavailable"


def test_g1_continuation_min_adx_floor_overrides_multiplier_threshold():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 18.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_min_slow_slope_ratio": 0.0,
            "g1_continuation_fast_ema_retest_atr_tolerance": 10.0,
        },
    )
    strategy._adx = lambda _prices, _window: 15.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side != Side.HOLD
    assert "adx_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("adx_softened") is True
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(18.0)


def test_g1_continuation_floor_preserves_exit_hysteresis():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 22.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.70,
            "g1_continuation_min_adx": 18.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_use_adx_hysteresis_state": True,
            "g1_min_slow_slope_ratio": 0.0,
        },
    )
    adx_values = iter([19.1, 17.0, 13.9])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]
    seq1 = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    seq2 = seq1 + [105.4]
    seq3 = seq2 + [105.45]

    first = strategy.generate_signal(_ctx(seq1, symbol="US100", current_spread_pips=0.8, tick_size=1.0))
    second = strategy.generate_signal(_ctx(seq2, symbol="US100", current_spread_pips=0.8, tick_size=1.0))
    third = strategy.generate_signal(_ctx(seq3, symbol="US100", current_spread_pips=0.8, tick_size=1.0))

    assert first.side == Side.BUY
    assert first.metadata.get("effective_adx_entry_threshold") == pytest.approx(18.0)
    assert first.metadata.get("effective_adx_exit_threshold") == pytest.approx(14.0)
    assert second.side == Side.BUY
    assert second.metadata.get("adx_regime_active_before") is True
    assert third.side != Side.HOLD
    assert "adx_below_threshold" in (third.metadata.get("soft_filter_reasons") or [])
    assert third.metadata.get("adx_softened") is True


def test_g1_warmup_cap_reduces_required_history():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_atr_window": 3,
            "g1_adx_warmup_multiplier": 3.0,
            "g1_adx_warmup_extra_bars": 10,
            "g1_adx_warmup_cap_bars": 8,
            "g1_candle_timeframe_sec": 0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0] * 8, symbol="EURUSD", tick_size=0.0001))
    assert signal.metadata.get("reason") != "insufficient_price_history"


def test_g1_applies_dynamic_index_multiplier_on_low_volatility():
    prices = [
        10000.0,
        10000.68884370305,
        10001.204752508931,
        10001.045895670593,
        10000.56372917118,
        10000.586278613917,
        10000.396146888817,
        10000.963744066887,
    ]
    common_params = {
        "g1_fast_ema_window": 3,
        "g1_slow_ema_window": 5,
        "g1_adx_window": 3,
        "g1_adx_threshold": 0.0,
        "g1_adx_hysteresis": 0.0,
        "g1_atr_window": 3,
        "g1_atr_multiplier": 0.2,
        "g1_risk_reward_ratio": 3.0,
        "g1_max_spread_pips": 10.0,
        "g1_min_stop_loss_pips": 0.1,
        "g1_max_price_ema_gap_ratio": 1.0,
        "g1_candle_timeframe_sec": 0,
        "g1_index_low_vol_atr_pct_threshold": 0.1,
        "g1_min_relative_stop_pct": 0.0,
        "g1_min_slow_slope_ratio": 0.0,
        "g1_volume_confirmation": False,
    }
    strategy_low_mult = create_strategy(
        "g1",
        {**common_params, "g1_index_low_vol_multiplier": 1.0},
    )
    strategy_high_mult = create_strategy(
        "g1",
        {**common_params, "g1_index_low_vol_multiplier": 1.5},
    )

    signal_low = strategy_low_mult.generate_signal(
        _ctx(prices, symbol="US100", current_spread_pips=1.0, tick_size=1.0)
    )
    signal_high = strategy_high_mult.generate_signal(
        _ctx(prices, symbol="US100", current_spread_pips=1.0, tick_size=1.0)
    )

    assert signal_low.side == Side.BUY
    assert signal_high.side == Side.BUY
    assert signal_low.metadata.get("atr_percent", 0.0) < 0.1
    assert signal_low.metadata.get("dynamic_atr_multiplier") == pytest.approx(0.2)
    assert signal_high.metadata.get("dynamic_atr_multiplier") == pytest.approx(0.3)
    assert signal_high.stop_loss_pips > signal_low.stop_loss_pips


def test_g1_enforces_min_relative_stop_pct_floor():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 0.2,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 0.1,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_index_low_vol_atr_pct_threshold": 0.1,
            "g1_index_low_vol_multiplier": 1.5,
            "g1_min_relative_stop_pct": 0.001,
            "g1_min_slow_slope_ratio": 0.0,
            "g1_volume_confirmation": False,
        },
    )
    prices = [
        10000.0,
        10000.68884370305,
        10001.204752508931,
        10001.045895670593,
        10000.56372917118,
        10000.586278613917,
        10000.396146888817,
        10000.963744066887,
    ]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", current_spread_pips=1.0, tick_size=1.0))

    assert signal.side == Side.BUY
    min_relative = float(signal.metadata.get("min_relative_stop_pips") or 0.0)
    assert min_relative > 0.0
    assert signal.stop_loss_pips == pytest.approx(min_relative)
    assert signal.metadata.get("min_relative_stop_pct") == pytest.approx(0.001)


def test_g1_applies_low_tf_risk_caps_for_m1_timeframe():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 2.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 10.0,
            "g1_min_stop_loss_pips": 0.1,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 60,
            "g1_min_relative_stop_pct": 0.001,
            "g1_low_tf_risk_profile_enabled": True,
            "g1_low_tf_max_timeframe_sec": 90.0,
            "g1_low_tf_atr_multiplier_cap": 1.8,
            "g1_low_tf_risk_reward_ratio_cap": 2.0,
            "g1_low_tf_min_relative_stop_pct_cap": 0.0002,
            "g1_index_low_vol_atr_pct_threshold": 0.0,
            "g1_use_incomplete_candle_for_entry": True,
            "g1_min_slow_slope_ratio": 0.0,
            "g1_volume_confirmation": False,
        },
    )
    prices = [
        10000.0,
        10000.68884370305,
        10001.204752508931,
        10001.045895670593,
        10000.56372917118,
        10000.586278613917,
        10000.396146888817,
        10000.963744066887,
    ]
    timestamps = [0.0, 60.0, 120.0, 180.0, 240.0, 300.0, 360.0, 420.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, current_spread_pips=1.0, tick_size=1.0)
    )

    assert signal.side == Side.BUY
    assert signal.metadata.get("low_tf_risk_caps_applied") is True
    assert signal.metadata.get("timeframe_sec") == pytest.approx(60.0)
    assert signal.metadata.get("effective_risk_reward_ratio") == pytest.approx(2.0)
    assert signal.metadata.get("effective_min_relative_stop_pct") == pytest.approx(0.0002)
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 2.0)


def test_g1_ignores_in_progress_candle_for_cross_signal():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_adx_window": 2,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 2,
            "g1_atr_multiplier": 1.0,
            "g1_risk_reward_ratio": 2.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 1.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 60,
            "g1_candle_confirm_bars": 1,
        },
    )
    prices = [101.0, 100.0, 99.0, 98.0, 99.0, 100.0, 110.0]
    timestamps = [0.0, 60.0, 120.0, 180.0, 240.0, 300.0, 305.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", timestamps=timestamps, current_spread_pips=0.8, tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") in {"insufficient_candle_history", "no_ema_cross", "candle_not_confirmed"}


def test_g1_in_progress_candle_is_not_used_for_confirmation_when_enabled():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_adx_window": 2,
            "g1_adx_threshold": 0.0,
            "g1_adx_hysteresis": 0.0,
            "g1_atr_window": 2,
            "g1_atr_multiplier": 1.0,
            "g1_risk_reward_ratio": 2.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 1.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 60,
            "g1_candle_confirm_bars": 1,
            "g1_use_incomplete_candle_for_entry": True,
        },
    )
    prices = [101.0, 100.0, 99.0, 98.0, 97.0, 96.0, 110.0]
    timestamps = [0.0, 60.0, 120.0, 180.0, 240.0, 300.0, 305.0]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]
    assert using_closed is True
    assert closes == pytest.approx([101.0, 100.0, 99.0, 98.0, 97.0, 110.0])

    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", timestamps=timestamps, current_spread_pips=0.8, tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "candle_not_confirmed"


def test_g1_resample_normalizes_millisecond_timestamps():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_candle_timeframe_sec": 60,
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    base_ms = 1_700_000_000_000.0
    timestamps_ms = [
        base_ms,
        base_ms + 15_000.0,
        base_ms + 30_000.0,
        base_ms + 60_000.0,
        base_ms + 75_000.0,
        base_ms + 90_000.0,
    ]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps_ms)  # type: ignore[attr-defined]

    assert using_closed is True
    # Default low-latency mode includes the most recent resampled candle.
    assert closes == pytest.approx([102.0, 105.0])


def test_g1_resample_can_drop_sunday_candles():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_candle_timeframe_sec": 60,
            "g1_ignore_sunday_candles": True,
            "g1_resample_mode": "always",
        },
    )
    saturday = datetime(2026, 3, 7, 23, 59, tzinfo=timezone.utc).timestamp()
    sunday = datetime(2026, 3, 8, 23, 59, tzinfo=timezone.utc).timestamp()
    monday = datetime(2026, 3, 9, 0, 1, tzinfo=timezone.utc).timestamp()
    prices = [100.0, 101.0, 102.0]
    timestamps = [saturday, sunday, monday]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]

    assert using_closed is True
    # Sunday value (101.0) is ignored; low-latency mode keeps the latest remaining candle.
    assert closes == pytest.approx([100.0, 102.0])


def test_g1_auto_resample_uses_live_candle_like_input_when_low_latency_enabled():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_candle_timeframe_sec": 60,
            "g1_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0]
    timestamps = [0.0, 60.0, 120.0, 180.0]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]

    assert using_closed is False
    assert closes == pytest.approx(prices)


def test_g1_auto_resample_applies_sunday_filter_before_returning_live_candle_like_input():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 2,
            "g1_slow_ema_window": 3,
            "g1_candle_timeframe_sec": 60,
            "g1_resample_mode": "auto",
            "g1_ignore_sunday_candles": True,
        },
    )
    saturday = datetime(2026, 3, 7, 23, 59, tzinfo=timezone.utc).timestamp()
    sunday = datetime(2026, 3, 8, 23, 59, tzinfo=timezone.utc).timestamp()
    monday = datetime(2026, 3, 9, 0, 1, tzinfo=timezone.utc).timestamp()
    prices = [100.0, 101.0, 102.0]
    timestamps = [saturday, sunday, monday]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]

    assert using_closed is False
    assert closes == pytest.approx([100.0, 102.0])


def test_g1_respects_configured_adx_warmup_history():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_warmup_multiplier": 3.0,
            "g1_adx_warmup_extra_bars": 10,
            "g1_candle_timeframe_sec": 0,
        },
    )
    prices = [100.0 + idx for idx in range(18)]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "insufficient_price_history"
    assert signal.metadata.get("min_history") == 19


def test_g1_adx_hysteresis_state_allows_between_entry_and_exit_thresholds():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_use_adx_hysteresis_state": True,
            "g1_min_slow_slope_ratio": 0.0,
        },
    )
    adx_values = iter([16.0, 13.0, 12.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]
    seq1 = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    seq2 = seq1 + [105.4]
    seq3 = seq2 + [105.45]

    first = strategy.generate_signal(_ctx(seq1, symbol="EURUSD", current_spread_pips=0.8))
    second = strategy.generate_signal(_ctx(seq2, symbol="EURUSD", current_spread_pips=0.8))
    third = strategy.generate_signal(_ctx(seq3, symbol="EURUSD", current_spread_pips=0.8))

    assert first.side == Side.BUY
    assert second.side == Side.BUY
    assert third.side != Side.HOLD
    assert "adx_below_threshold" in (third.metadata.get("soft_filter_reasons") or [])
    assert third.metadata.get("adx_softened") is True
    assert third.metadata.get("adx_regime_active_before") is False
    assert third.metadata.get("adx_regime_active_after") is False


def test_g1_adx_hysteresis_state_is_directional_by_side():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_use_adx_hysteresis_state": True,
            "g1_min_slow_slope_ratio": 0.0,
        },
    )
    adx_values = iter([16.0, 13.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]

    buy_prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    sell_prices = [107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0, 101.7]

    first = strategy.generate_signal(_ctx(buy_prices, symbol="EURUSD", current_spread_pips=0.8))
    second = strategy.generate_signal(_ctx(sell_prices, symbol="EURUSD", current_spread_pips=0.8))

    assert first.side == Side.BUY
    first_scope = str(first.metadata.get("adx_state_scope") or "")
    assert first_scope.startswith("EURUSD:buy|")
    assert second.side == Side.SELL
    assert "adx_below_threshold" in (second.metadata.get("soft_filter_reasons") or [])
    assert second.metadata.get("adx_softened") is True
    assert second.metadata.get("trend_signal") == "ema_trend_down"
    assert second.metadata.get("adx_regime_active_before") is False
    second_scope = str(second.metadata.get("adx_state_scope") or "")
    assert second_scope.startswith("EURUSD:sell|")


def test_g1_adx_hysteresis_state_refreshes_when_no_candidate_signal():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_entry_mode": "cross_only",
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_use_adx_hysteresis_state": True,
        },
    )
    adx_values = iter([30.0, 15.0, 17.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]

    cross_prices = [100.0, 100.57, 99.42, 99.63, 98.16, 98.47, 99.37, 99.66]
    no_signal_prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]

    first = strategy.generate_signal(_ctx(cross_prices, symbol="EURUSD", current_spread_pips=0.8))
    second = strategy.generate_signal(_ctx(no_signal_prices, symbol="EURUSD", current_spread_pips=0.8))
    third = strategy.generate_signal(_ctx(cross_prices, symbol="EURUSD", current_spread_pips=0.8))

    assert first.side == Side.BUY
    assert second.side == Side.HOLD
    assert second.metadata.get("reason") == "no_ema_cross"
    assert third.side != Side.HOLD
    assert "adx_below_threshold" in (third.metadata.get("soft_filter_reasons") or [])
    assert third.metadata.get("adx_softened") is True
    assert third.metadata.get("adx_regime_active_before") is False
    assert third.metadata.get("adx_state_refreshed_pre_signal") is True


def test_g1_adx_hysteresis_seed_recovers_between_thresholds_after_restart():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_use_adx_hysteresis_state": True,
            "g1_min_slow_slope_ratio": 0.0,
        },
    )
    strategy._adx_from_bars = lambda _bars, _window: 13.0  # type: ignore[method-assign]
    strategy._adx_series_from_bars = lambda _bars, _window: [16.0, 13.0]  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]

    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))

    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("adx_regime_seed_active") is True
    assert signal.metadata.get("adx_regime_seed_from_history") is True
    assert signal.metadata.get("adx_regime_active_before") is True


def test_g1_adx_hysteresis_state_isolated_by_timeframe_scope():
    strategy = create_strategy(
        "g1",
        {
            "g1_fast_ema_window": 3,
            "g1_slow_ema_window": 5,
            "g1_adx_window": 3,
            "g1_adx_threshold": 25.0,
            "g1_adx_hysteresis": 2.0,
            "g1_continuation_adx_multiplier": 0.55,
            "g1_continuation_min_adx": 0.0,
            "g1_continuation_min_entry_threshold_ratio": 0.0,
            "g1_entry_mode": "cross_or_trend",
            "g1_min_trend_gap_ratio": 0.0,
            "g1_atr_window": 3,
            "g1_atr_multiplier": 1.5,
            "g1_risk_reward_ratio": 3.0,
            "g1_max_spread_pips": 5.0,
            "g1_min_stop_loss_pips": 5.0,
            "g1_max_price_ema_gap_ratio": 1.0,
            "g1_candle_timeframe_sec": 0,
            "g1_use_adx_hysteresis_state": True,
            "g1_min_slow_slope_ratio": 0.0,
        },
    )
    adx_values = iter([16.0, 13.5])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 105.35]
    t0 = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc).timestamp()
    m1_timestamps = [t0 + (idx * 60.0) for idx in range(len(prices))]
    m5_timestamps = [t0 + (idx * 300.0) for idx in range(len(prices))]

    first = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", timestamps=m1_timestamps, current_spread_pips=0.8)
    )
    second = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", timestamps=m5_timestamps, current_spread_pips=0.8)
    )

    assert first.side == Side.BUY
    assert second.side != Side.HOLD
    assert "adx_below_threshold" in (second.metadata.get("soft_filter_reasons") or [])
    assert second.metadata.get("adx_softened") is True
    assert second.metadata.get("adx_regime_active_before") is False


def test_mean_reversion_bb_sell_on_upper_band_and_rsi():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 1.0,
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_rsi_oversold": 30.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "bollinger_bands"
    assert signal.metadata.get("band") == "upper"
    assert signal.metadata.get("exit_hint") is None


def test_mean_reversion_bb_buy_on_lower_band_and_rsi():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 1.0,
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_rsi_oversold": 30.0,
        },
    )
    signal = strategy.generate_signal(_ctx([110.0, 110.0, 110.0, 110.0, 110.0, 110.0, 100.0]))
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "bollinger_bands"
    assert signal.metadata.get("band") == "lower"


def test_mean_reversion_bb_entry_quality_penalizes_touch_without_reversal_bar():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 1.0,
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_rsi_oversold": 30.0,
        },
    )

    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0], tick_size=0.1))

    assert signal.side == Side.SELL
    assert signal.metadata.get("entry_quality_family") == "mean_reversion"
    assert signal.metadata.get("entry_quality_status") == "penalized"
    assert "latest_bar_not_reversing" in (signal.metadata.get("entry_quality_reasons") or [])
    assert float(signal.metadata.get("entry_quality_penalty", 0.0)) > 0.0


def test_mean_reversion_bb_reentry_sell_on_return_inside_upper_band():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("entry_mode") == "reentry"


def test_mean_reversion_bb_reentry_blocks_noisy_return_inside_upper_band():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_reentry_tolerance_sigma": 0.18,
            "mean_reversion_bb_reentry_min_reversal_sigma": 0.22,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 109.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"


def test_mean_reversion_bb_entry_quality_keeps_clean_reentry_reversal_unpenalized():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )

    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0], tick_size=0.1))

    assert signal.side == Side.SELL
    assert signal.metadata.get("entry_quality_family") == "mean_reversion"
    assert signal.metadata.get("entry_quality_status") == "ok"
    assert signal.metadata.get("entry_quality_penalty") == pytest.approx(0.0)


def test_mean_reversion_bb_drops_non_finite_prices_before_signal_generation():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 100.0, 100.0, float("nan"), 100.0, 100.0, 110.0, 102.0], symbol="US100", tick_size=0.1)
    )
    assert signal.side == Side.SELL
    assert signal.metadata.get("invalid_prices_dropped") == 1


def test_mean_reversion_bb_reentry_buy_on_return_inside_lower_band():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )
    signal = strategy.generate_signal(_ctx([110.0, 110.0, 110.0, 110.0, 110.0, 100.0, 108.0]))
    assert signal.side == Side.BUY
    assert signal.metadata.get("entry_mode") == "reentry"


def test_mean_reversion_bb_reentry_blocks_noisy_return_inside_lower_band():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_reentry_tolerance_sigma": 0.18,
            "mean_reversion_bb_reentry_min_reversal_sigma": 0.22,
        },
    )
    signal = strategy.generate_signal(_ctx([110.0, 110.0, 110.0, 110.0, 110.0, 100.0, 101.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"


def test_mean_reversion_bb_reentry_blocks_shallow_previous_extension():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_min_band_extension_ratio": 0.20,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_touch_too_shallow"
    assert float(signal.metadata.get("effective_extension_ratio", 0.0)) < 0.20
    assert float(signal.metadata.get("prev_extension_ratio", 0.0)) > 0.0


def test_mean_reversion_bb_reentry_blocks_too_extreme_previous_extension():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_max_band_extension_ratio": 0.10,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_extension_too_extreme"
    assert float(signal.metadata.get("effective_extension_ratio", 0.0)) > 0.10
    assert float(signal.metadata.get("prev_extension_ratio", 0.0)) > 0.0


def test_mean_reversion_bb_reentry_holds_on_band_walk_without_reentry():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 130.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_walk_no_reentry"


def test_mean_reversion_bb_holds_when_rsi_filter_not_confirmed():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 1.0,
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_rsi_oversold": 30.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 100.0, 101.0, 100.0, 101.0, 101.5]))
    assert signal.side == Side.HOLD


def test_mean_reversion_bb_emits_exit_hint_on_midline_reversion():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_exit_on_midline": True,
            "mean_reversion_bb_exit_midline_tolerance_sigma": 0.2,
            "mean_reversion_bb_min_take_profit_pips": 1.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 102.0, 98.0, 100.0, 100.0, 100.2]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_target_reached"
    assert signal.metadata.get("exit_hint") == "close_on_bb_midline"
    assert signal.metadata.get("exit_action") == "close_position"


def test_mean_reversion_bb_holds_on_low_volatility_guard():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.005,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.01, 99.99, 100.0, 100.01, 99.99]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "low_volatility"


def test_mean_reversion_bb_uses_wilder_rsi_by_default():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 1.0,
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_rsi_oversold": 30.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("rsi_method") == "wilder"


def test_mean_reversion_bb_does_not_compute_rsi_when_filter_disabled(monkeypatch):
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_rsi_period": 14,
            "mean_reversion_bb_rsi_history_multiplier": 3.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )

    def _fail_rsi(_prices):
        raise AssertionError("RSI should not be computed when filter is disabled")

    monkeypatch.setattr(strategy, "_rsi", _fail_rsi)
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("rsi") == pytest.approx(50.0)


def test_mean_reversion_bb_uses_hlc_for_atr_when_closed_candles_enabled(monkeypatch):
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_use_atr_sl_tp": True,
            "mean_reversion_bb_atr_window": 2,
            "mean_reversion_bb_atr_multiplier": 1.0,
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "auto",
        },
    )

    captured: dict[str, object] = {}

    def _fake_atr(values, _window):
        captured["first"] = values[0] if values else None
        return 10.0

    monkeypatch.setattr(strategy, "_atr", _fake_atr)
    base_ts = _ts("09:00")
    prices: list[float] = []
    timestamps: list[float] = []
    for minute in range(7):
        close = 100.0 + (minute * 0.5)
        prices.extend([110.0, 90.0, close])
        timestamps.extend(
            [
                base_ts + minute * 60 + 5,
                base_ts + minute * 60 + 20,
                base_ts + minute * 60 + 50,
            ]
        )
    strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=1.0))
    first = captured.get("first")
    assert isinstance(first, tuple)
    assert len(first) == 3
    assert first[0] >= first[1]


def test_mean_reversion_bb_auto_resample_excludes_last_candle_for_candle_like_input():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0]
    timestamps = [0.0, 60.0, 120.0, 180.0]
    closes, close_timestamps, using_closed = strategy._resample_closed_candle_series(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is True
    assert closes == pytest.approx(prices[:-1])
    assert close_timestamps == pytest.approx(timestamps[:-1])


def test_mean_reversion_bb_auto_resample_uses_raw_prices_for_snapshot_flow():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "auto",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    timestamps = [0.0, 15.0, 30.0, 60.0, 75.0, 90.0]
    closes, close_timestamps, using_closed = strategy._resample_closed_candle_series(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert using_closed is False
    assert closes == pytest.approx(prices)
    assert close_timestamps == pytest.approx(timestamps)


def test_mean_reversion_bb_ignores_intrabar_spike_when_using_closed_candles():
    strategy_closed = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "auto",
        },
    )
    strategy_raw = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "off",
        },
    )
    base_ts = _ts("09:00")
    prices: list[float] = []
    timestamps: list[float] = []
    for minute in range(7):
        timestamps.extend([base_ts + minute * 60 + 5, base_ts + minute * 60 + 35])
        prices.extend([100.0, 115.0 if minute == 6 else 100.0])

    closed_signal = strategy_closed.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1)
    )
    raw_signal = strategy_raw.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1)
    )
    assert closed_signal.side == Side.SELL
    assert raw_signal.side == Side.SELL
    assert closed_signal.metadata.get("using_closed_candles") is False


def test_mean_reversion_bb_touch_mode_holds_on_band_walk_without_fresh_cross():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_min_band_extension_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 150.0, 220.0, 210.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_walk_touch_no_fresh_cross"


def test_mean_reversion_bb_trend_filter_blocks_countertrend_sell():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_trend_filter_enabled": True,
            "mean_reversion_bb_trend_ma_window": 6,
            "mean_reversion_bb_trend_slope_lookback_bars": 1,
            "mean_reversion_bb_trend_countertrend_min_distance_sigma": 10.0,
            "mean_reversion_bb_trend_slope_block_max_distance_sigma": 10.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 110.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "trend_too_strong_up"
    assert signal.metadata.get("expected_side") == "sell"


def test_mean_reversion_bb_strict_trend_slope_blocks_reentry_countertrend_sell():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": True,
            "mean_reversion_bb_trend_filter_mode": "strict",
            "mean_reversion_bb_trend_ma_window": 5,
            "mean_reversion_bb_trend_slope_lookback_bars": 2,
            "mean_reversion_bb_trend_slope_strict_threshold": 0.0001,
        },
    )
    signal = strategy.generate_signal(
        _ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 110.0, 107.0], tick_size=0.1)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "trend_too_strong_up"


def test_mean_reversion_bb_strict_trend_filter_blocks_extreme_overextension():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.2,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_trend_filter_enabled": True,
            "mean_reversion_bb_trend_filter_mode": "strict",
            "mean_reversion_bb_trend_ma_window": 6,
            "mean_reversion_bb_trend_slope_lookback_bars": 1,
            "mean_reversion_bb_trend_countertrend_min_distance_sigma": 1.5,
            "mean_reversion_bb_trend_slope_block_max_distance_sigma": 1.4,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 125.0], tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "trend_too_strong_up"


def test_mean_reversion_bb_session_filter_blocks_entry_outside_window():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_session_filter_enabled": True,
            "mean_reversion_bb_session_start_hour_utc": 6,
            "mean_reversion_bb_session_end_hour_utc": 22,
            "mean_reversion_bb_resample_mode": "off",
        },
    )
    base_ts = _ts("03:00")
    timestamps = [base_ts + idx * 60 for idx in range(7)]
    signal = strategy.generate_signal(
        _ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0], timestamps=timestamps)
    )
    assert signal.side == Side.SELL
    assert "session_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("expected_side") == "sell"
    assert signal.metadata.get("session_status") == "outside_window"


def test_mean_reversion_bb_regime_adx_filter_blocks_strong_trend_entry():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_regime_adx_filter_enabled": True,
            "mean_reversion_bb_regime_adx_window": 3,
            "mean_reversion_bb_regime_max_adx": 20.0,
            "mean_reversion_bb_regime_adx_hysteresis": 0.0,
            "mean_reversion_bb_regime_use_hysteresis_state": False,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 120.0], tick_size=0.1))
    assert signal.side == Side.SELL
    assert "regime_adx_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert float(signal.metadata.get("regime_adx", 0.0)) > 20.0


def test_mean_reversion_bb_regime_volatility_filter_blocks_expansion_entry():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_regime_volatility_expansion_filter_enabled": True,
            "mean_reversion_bb_regime_volatility_short_window": 2,
            "mean_reversion_bb_regime_volatility_long_window": 5,
            "mean_reversion_bb_regime_volatility_expansion_max_ratio": 1.1,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0], tick_size=0.1))
    assert signal.side == Side.SELL
    assert "regime_volatility_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert float(signal.metadata.get("regime_volatility_expansion_ratio", 0.0)) > 1.1


def test_mean_reversion_bb_connors_soft_gate_allows_entry_with_penalty():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_oscillator_mode": "connors",
            "mean_reversion_bb_oscillator_gate_mode": "soft",
            "mean_reversion_bb_rsi_overbought": 100.0,
            "mean_reversion_bb_rsi_oversold": 0.0,
            "mean_reversion_bb_oscillator_soft_zone_width": 10.0,
            "mean_reversion_bb_oscillator_soft_confidence_penalty": 0.25,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
        },
    )
    prices = [100.0] * 29 + [110.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.SELL
    assert signal.metadata.get("oscillator_mode") == "connors"
    assert signal.metadata.get("oscillator_gate_mode") == "soft"
    assert float(signal.metadata.get("oscillator_soft_penalty_applied", 0.0)) > 0.0


def test_mean_reversion_bb_rejection_gate_blocks_reentry_without_wick():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_rejection_gate_enabled": True,
            "mean_reversion_bb_rejection_min_wick_to_body_ratio": 0.2,
            "mean_reversion_bb_rejection_min_wick_to_range_ratio": 0.1,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0], tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "candle_rejection_not_confirmed"
    assert signal.metadata.get("rejection_status") == "wick_to_body_too_small"


def test_mean_reversion_bb_rejection_gate_allows_reentry_with_wick_reversal_candle():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_candle_timeframe_sec": 60.0,
            "mean_reversion_bb_resample_mode": "auto",
            "mean_reversion_bb_rejection_gate_enabled": True,
            "mean_reversion_bb_rejection_min_wick_to_body_ratio": 0.1,
            "mean_reversion_bb_rejection_min_wick_to_range_ratio": 0.1,
            "mean_reversion_bb_rejection_sell_max_close_location": 0.5,
        },
    )
    base_ts = _ts("09:00")
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 112.0, 114.0, 102.0, 101.0]
    timestamps = [
        base_ts + 0,
        base_ts + 60,
        base_ts + 120,
        base_ts + 180,
        base_ts + 240,
        base_ts + 300,
        base_ts + 360,
        base_ts + 365,
        base_ts + 370,
        base_ts + 420,
    ]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1)
    )
    assert signal.side == Side.SELL
    assert signal.metadata.get("rejection_status") == "ok"


def test_mean_reversion_bb_blocks_shallow_band_touch():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_min_band_extension_ratio": 0.6,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_touch_too_shallow"


def test_mean_reversion_bb_volume_spike_boosts_reentry_confidence():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": True,
            "mean_reversion_bb_volume_confidence_boost": 0.2,
            "mean_reversion_bb_reentry_base_confidence": 0.70,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]
    volumes = [100.0] * 25 + [120.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes, current_volume=220.0)
    )
    assert signal.side == Side.SELL
    assert signal.metadata.get("volume_spike") is True
    assert signal.metadata.get("volume_reversal_confirmed") is True
    assert signal.metadata.get("volume_spike_boost_applied") is True
    assert signal.confidence >= 0.85


def test_mean_reversion_bb_volume_require_spike_blocks_when_volume_below_threshold():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": True,
            "mean_reversion_bb_volume_require_spike": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]
    volumes = [100.0] * 25 + [100.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes, current_volume=90.0)
    )
    assert signal.side == Side.SELL
    assert "volume_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_softened") is True


def test_mean_reversion_bb_volume_require_spike_is_soft_disabled_for_fx_by_default():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": True,
            "mean_reversion_bb_volume_require_spike": True,
        },
    )
    prices = [1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1100, 1.1020]
    volumes = [100.0] * 25 + [100.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", tick_size=0.0001, volumes=volumes, current_volume=90.0)
    )
    assert signal.side == Side.SELL
    assert signal.metadata.get("volume_status") == "below_threshold"
    assert signal.metadata.get("volume_require_spike") is False
    assert signal.metadata.get("volume_require_spike_configured") is True


def test_mean_reversion_bb_volume_require_spike_fx_override_true_blocks_when_below_threshold():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": True,
            "mean_reversion_bb_volume_require_spike": True,
            "mean_reversion_bb_volume_require_spike_fx": True,
        },
    )
    prices = [1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1100, 1.1020]
    volumes = [100.0] * 25 + [100.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", tick_size=0.0001, volumes=volumes, current_volume=90.0)
    )
    assert signal.side == Side.SELL
    assert "volume_session_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_require_spike") is True
    assert signal.metadata.get("volume_require_spike_fx_override") is True
    assert signal.metadata.get("volume_fx_liquidity_status") == "missing_timestamp"


def test_mean_reversion_bb_fx_volume_requires_liquid_session_when_spike_is_mandatory():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_session_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.2,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": False,
            "mean_reversion_bb_volume_require_spike": True,
            "mean_reversion_bb_volume_require_spike_fx": True,
            "mean_reversion_bb_resample_mode": "off",
        },
    )
    prices = [1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1100, 1.1020]
    volumes = [100.0] * 25
    base_ts = _ts("03:00")
    timestamps = [base_ts + idx * 60 for idx in range(len(prices))]
    signal = strategy.generate_signal(
        _ctx(
            prices,
            symbol="EURUSD",
            tick_size=0.0001,
            volumes=volumes,
            current_volume=300.0,
            timestamps=timestamps,
        )
    )
    assert signal.side == Side.SELL
    assert "volume_session_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_fx_liquidity_status") == "outside_window"
    assert signal.metadata.get("volume_spike_raw") is True
    assert signal.metadata.get("volume_spike") is False


def test_mean_reversion_bb_missing_current_volume_does_not_use_history_for_spike_on_non_index():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_min_band_extension_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 3,
            "mean_reversion_bb_min_volume_ratio": 1.2,
            "mean_reversion_bb_volume_min_samples": 1,
            "mean_reversion_bb_volume_allow_missing": False,
            "mean_reversion_bb_volume_require_spike": True,
            "mean_reversion_bb_volume_require_spike_fx": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]
    volumes = [50.0, 60.0, 180.0]
    base_ts = _ts("10:00")
    timestamps = [base_ts + idx * 60 for idx in range(len(prices))]
    signal = strategy.generate_signal(
        _ctx(
            prices,
            symbol="EURUSD",
            tick_size=0.0001,
            volumes=volumes,
            current_volume=None,
            timestamps=timestamps,
        )
    )
    assert signal.side == Side.BUY
    assert "volume_data_unavailable" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_status") == "missing_current_volume"
    assert signal.metadata.get("current_volume") is None


def test_mean_reversion_bb_index_missing_volume_is_soft_allowed():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 20,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 10,
            "mean_reversion_bb_volume_allow_missing": False,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="IT40", tick_size=0.1, volumes=[], current_volume=None)
    )
    assert signal.side == Side.SELL
    assert signal.metadata.get("volume_status") == "missing_current_volume"
    assert signal.metadata.get("volume_allow_missing_effective") is True
    assert signal.metadata.get("volume_missing_relaxed_for_index") is True


def test_mean_reversion_bb_volume_uses_cumulative_ig_series_per_candle_baseline():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 10,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 1,
            "mean_reversion_bb_volume_allow_missing": False,
            "mean_reversion_bb_volume_require_spike": True,
        },
    )
    # IG-like cumulative stream: growing within candle, then reset on new candle.
    volumes = [
        10.0, 20.0, 40.0, 80.0, 100.0,
        5.0, 15.0, 30.0, 60.0, 90.0, 100.0,
        4.0, 20.0, 40.0, 60.0, 80.0,
    ]
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes, current_volume=80.0)
    )
    assert signal.side == Side.SELL
    assert "volume_below_threshold" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_series_mode") == "cumulative_candle_max"
    assert int(signal.metadata.get("volume_resets_detected") or 0) >= 1


def test_mean_reversion_bb_volume_payload_ignores_non_finite_history_values():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 5,
            "mean_reversion_bb_min_volume_ratio": 1.5,
            "mean_reversion_bb_volume_min_samples": 1,
            "mean_reversion_bb_volume_allow_missing": False,
            "mean_reversion_bb_volume_require_spike": True,
        },
    )
    spike, payload = strategy._volume_spike_payload(  # type: ignore[attr-defined]
        _ctx(
            [100.0] * 8,
            symbol="US100",
            volumes=[100.0, float("inf"), 120.0],
            current_volume=180.0,
        )
    )
    assert spike is True
    assert payload.get("avg_volume") == pytest.approx(110.0)
    assert payload.get("volume_ratio") == pytest.approx(180.0 / 110.0)


def test_mean_reversion_bb_volume_spike_reversal_gate_blocks_continuation_touch_entry():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_min_band_extension_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": True,
            "mean_reversion_bb_volume_window": 3,
            "mean_reversion_bb_min_volume_ratio": 1.2,
            "mean_reversion_bb_volume_min_samples": 1,
            "mean_reversion_bb_volume_allow_missing": False,
            "mean_reversion_bb_volume_require_spike": False,
            "mean_reversion_bb_volume_spike_mode": "reversal_gate",
        },
    )
    prices = [100.0, 100.0, 100.0, 110.0, 113.0, 114.0]
    volumes = [100.0, 110.0, 90.0]
    signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", tick_size=0.1, volumes=volumes, current_volume=260.0)
    )
    assert signal.side == Side.SELL
    assert "volume_spike_without_reversal" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("volume_spike") is True
    assert signal.metadata.get("volume_reversal_confirmed") is False


def test_mean_reversion_bb_index_pip_size_fallback_handles_aus200():
    strategy = create_strategy("mean_reversion_bb", {})
    assert strategy._pip_size("AUS200") == pytest.approx(1.0)  # type: ignore[attr-defined]


def test_mean_reversion_bb_supports_only_fx_and_index_by_default():
    strategy = create_strategy("mean_reversion_bb", {})
    assert strategy.supports_symbol("EURUSD")
    assert strategy.supports_symbol("US100")
    assert not strategy.supports_symbol("GOLD")
    assert not strategy.supports_symbol("BTC")


def test_mean_reversion_bb_supports_symbol_with_whitelist_and_blacklist():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_allowed_asset_classes": "all",
            "mean_reversion_bb_symbol_whitelist": "EURUSD,US100",
            "mean_reversion_bb_symbol_blacklist": "US100",
        },
    )
    assert strategy.supports_symbol("EURUSD")
    assert not strategy.supports_symbol("US100")
    assert not strategy.supports_symbol("GBPUSD")


def test_mean_reversion_bb_switches_runtime_params_by_asset_class():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_allowed_asset_classes": "fx,index",
            "mean_reversion_bb_std_dev": 2.2,
            "mean_reversion_bb_params_by_asset_class": {
                "fx": {"mean_reversion_bb_std_dev": 1.9},
                "index": {"mean_reversion_bb_std_dev": 2.7},
            },
        },
    )
    strategy._sync_params_for_symbol("EURUSD")  # type: ignore[attr-defined]
    assert strategy.bb_std_dev == pytest.approx(1.9)
    strategy._sync_params_for_symbol("US100")  # type: ignore[attr-defined]
    assert strategy.bb_std_dev == pytest.approx(2.7)


def test_mean_reversion_bb_uses_extended_rsi_history_window(monkeypatch):
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.0,
            "mean_reversion_bb_use_rsi_filter": True,
            "mean_reversion_bb_rsi_period": 5,
            "mean_reversion_bb_rsi_history_multiplier": 3.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    captured: dict[str, int] = {}

    def _fake_rsi(values):
        captured["len"] = len(values)
        return 80.0

    monkeypatch.setattr(strategy, "_rsi", _fake_rsi)
    prices = [100.0] * 15 + [102.0, 103.0, 104.0]
    strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    assert captured.get("len", 0) >= 16


def test_mean_reversion_bb_connors_rsi_uses_configured_component_weights(monkeypatch):
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_connors_price_rsi_weight": 0.5,
            "mean_reversion_bb_connors_streak_rsi_weight": 0.4,
            "mean_reversion_bb_connors_rank_weight": 0.1,
        },
    )
    call_counter = {"count": 0}

    def _fake_rsi(values, period):
        call_counter["count"] += 1
        return 80.0 if call_counter["count"] == 1 else 20.0

    monkeypatch.setattr(mean_reversion_bb_module, "rsi_wilder", _fake_rsi)
    monkeypatch.setattr(strategy, "_percent_rank", lambda value, samples: 10.0)
    connors = strategy._connors_rsi([float(value) for value in range(1, 40)])  # type: ignore[attr-defined]
    assert connors is not None
    value, payload = connors
    assert value == pytest.approx((80.0 * 0.5) + (20.0 * 0.4) + (10.0 * 0.1))
    assert payload["connors_price_rsi_weight"] == pytest.approx(0.5)
    assert payload["connors_streak_rsi_weight"] == pytest.approx(0.4)
    assert payload["connors_rank_weight"] == pytest.approx(0.1)


def test_mean_reversion_bb_take_profit_mode_midline_or_rr_prefers_midline_target():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_use_atr_sl_tp": False,
            "mean_reversion_bb_risk_reward_ratio": 3.0,
            "mean_reversion_bb_min_stop_loss_pips": 20.0,
            "mean_reversion_bb_min_take_profit_pips": 1.0,
            "mean_reversion_bb_take_profit_mode": "midline_or_rr",
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1020], symbol="EURUSD", tick_size=0.0001)
    )
    assert signal.side == Side.SELL
    rr_target = float(signal.metadata.get("rr_target_pips", 0.0))
    midline_target = float(signal.metadata.get("midline_target_pips", 0.0))
    assert signal.take_profit_pips == pytest.approx(midline_target)
    assert signal.take_profit_pips <= rr_target


def test_mean_reversion_bb_take_profit_mode_midline_keeps_strategy_target_on_midline():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_use_atr_sl_tp": False,
            "mean_reversion_bb_risk_reward_ratio": 3.0,
            "mean_reversion_bb_min_stop_loss_pips": 20.0,
            "mean_reversion_bb_min_take_profit_pips": 20.0,
            "mean_reversion_bb_take_profit_mode": "midline",
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1000, 1.1010], symbol="EURUSD", tick_size=0.0001)
    )
    assert signal.side == Side.SELL
    strategy_target = float(signal.metadata.get("strategy_take_profit_pips", 0.0))
    midline_target = float(signal.metadata.get("midline_target_pips", 0.0))
    execution_target = float(signal.metadata.get("execution_take_profit_pips", 0.0))
    assert signal.take_profit_pips == pytest.approx(strategy_target)
    assert strategy_target == pytest.approx(midline_target)
    assert strategy_target < 20.0
    assert execution_target == pytest.approx(20.0)
    assert signal.metadata.get("execution_tp_floor_applied") is True


def test_mean_reversion_bb_midline_exit_is_suppressed_when_execution_tp_floor_applies():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_use_atr_sl_tp": False,
            "mean_reversion_bb_risk_reward_ratio": 3.0,
            "mean_reversion_bb_min_stop_loss_pips": 20.0,
            "mean_reversion_bb_min_take_profit_pips": 20.0,
            "mean_reversion_bb_take_profit_mode": "midline",
            "mean_reversion_bb_exit_on_midline": True,
            "mean_reversion_bb_exit_midline_tolerance_sigma": 1.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([1.1000, 1.1002, 1.1000, 1.1002, 1.1000, 1.1001], symbol="EURUSD", tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"
    assert signal.metadata.get("midline_exit_armed") is False
    assert signal.metadata.get("midline_exit_suppressed_by_tp_floor") is True


def test_mean_reversion_bb_midline_exit_requires_target_to_clear_min_profit_floor():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_use_atr_sl_tp": False,
            "mean_reversion_bb_risk_reward_ratio": 3.0,
            "mean_reversion_bb_min_stop_loss_pips": 5.0,
            "mean_reversion_bb_min_take_profit_pips": 20.0,
            "mean_reversion_bb_take_profit_mode": "rr",
            "mean_reversion_bb_exit_on_midline": True,
            "mean_reversion_bb_exit_midline_tolerance_sigma": 1.0,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(
        _ctx([1.1000, 1.1002, 1.1000, 1.1002, 1.1000, 1.1001], symbol="EURUSD", tick_size=0.0001)
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_signal"
    assert signal.metadata.get("midline_exit_armed") is False
    assert signal.metadata.get("midline_exit_target_respects_floor") is False
    assert signal.metadata.get("midline_exit_suppressed_by_target_floor") is True


def test_mean_reversion_bb_regime_adx_blocks_dead_range_below_minimum(monkeypatch):
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_trend_filter_enabled": False,
            "mean_reversion_bb_session_filter_enabled": False,
            "mean_reversion_bb_volume_confirmation": False,
            "mean_reversion_bb_regime_adx_filter_enabled": True,
            "mean_reversion_bb_regime_adx_window": 2,
            "mean_reversion_bb_regime_min_adx": 15.0,
            "mean_reversion_bb_regime_max_adx": 23.0,
        },
    )
    monkeypatch.setattr(strategy, "_adx", lambda values, window: 10.0)
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 110.0, 102.0], tick_size=0.1))
    assert signal.side == Side.SELL
    assert "regime_adx_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("regime_adx_status") == "below_minimum"
    assert signal.metadata.get("regime_min_adx") == pytest.approx(15.0)


def test_mean_reversion_bb_trend_filter_extreme_override_blocks_touch_countertrend_entry():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_trend_filter_enabled": True,
            "mean_reversion_bb_trend_ma_window": 6,
            "mean_reversion_bb_trend_slope_lookback_bars": 1,
            "mean_reversion_bb_trend_filter_mode": "extreme_override",
            "mean_reversion_bb_trend_filter_extreme_sigma": 0.3,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 110.0], tick_size=0.1))
    assert signal.side == Side.SELL
    assert "trend_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("trend_filter_status") == "override_requires_reentry"


def test_mean_reversion_bb_trend_filter_extreme_override_allows_reentry_after_extreme_move():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "reentry",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_trend_filter_enabled": True,
            "mean_reversion_bb_trend_ma_window": 6,
            "mean_reversion_bb_trend_slope_lookback_bars": 1,
            "mean_reversion_bb_trend_slope_strict_threshold": 0.05,
            "mean_reversion_bb_trend_filter_mode": "extreme_override",
            "mean_reversion_bb_trend_filter_extreme_sigma": 0.5,
            "mean_reversion_bb_trend_countertrend_min_distance_sigma": 0.8,
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 111.0, 108.0], tick_size=0.1))
    assert signal.side == Side.SELL
    assert signal.metadata.get("trend_filter_override") is True
    assert signal.metadata.get("trend_filter_status") == "override_ok"


def test_mean_reversion_bb_blocks_too_extreme_band_extension():
    strategy = create_strategy(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_window": 5,
            "mean_reversion_bb_std_dev": 1.5,
            "mean_reversion_bb_entry_mode": "touch",
            "mean_reversion_bb_use_rsi_filter": False,
            "mean_reversion_bb_min_std_ratio": 0.0,
            "mean_reversion_bb_max_band_extension_ratio": 0.05,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 110.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "band_extension_too_extreme"
