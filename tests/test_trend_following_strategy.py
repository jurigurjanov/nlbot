from __future__ import annotations

from datetime import datetime, timezone

import pytest

from xtb_bot.models import Side
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
) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        prices=prices,
        timestamps=timestamps or [],
        volumes=volumes or [],
        current_volume=current_volume,
        current_spread_pips=current_spread_pips,
        tick_size=tick_size,
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
    assert "mean_breakout" in available_strategies()
    assert "mean_breakout_v2" in available_strategies()
    assert "mean_breakout_session" in available_strategies()
    assert "mean_reversion_bb" in available_strategies()
    assert "momentum_index" in available_strategies()
    assert "momentum_fx" in available_strategies()


def test_crypto_trend_following_supports_only_crypto_symbols():
    strategy = create_strategy("crypto_trend_following", {})
    assert strategy.supports_symbol("BTC")
    assert strategy.supports_symbol("ETH")
    assert strategy.supports_symbol("SOL")
    assert not strategy.supports_symbol("US100")
    assert not strategy.supports_symbol("EURUSD")


def test_crypto_trend_following_uses_crypto_tuned_defaults():
    strategy = create_strategy("crypto_trend_following", {})
    assert strategy.fast_ema_window == 21  # type: ignore[attr-defined]
    assert strategy.slow_ema_window == 89  # type: ignore[attr-defined]
    assert strategy.risk_reward_ratio == pytest.approx(2.0)  # type: ignore[attr-defined]
    assert strategy.min_stop_loss_pips == pytest.approx(150.0)  # type: ignore[attr-defined]
    assert strategy.max_spread_to_stop_ratio == pytest.approx(0.5)  # type: ignore[attr-defined]
    assert strategy.pullback_bounce_required is False  # type: ignore[attr-defined]
    assert strategy.crypto_pullback_ema_tolerance_ratio == pytest.approx(0.015)  # type: ignore[attr-defined]


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
            "trend_max_timestamp_gap_sec": 3600.0,
        },
    )
    prices = [100000.0, 99000.0, 98000.0, 99000.0, 100000.0, 101000.0, 103000.0, 102500.0, 102200.0, 102400.0]
    timestamps = [float(idx * 60) for idx in range(len(prices) - 1)] + [8000.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="BTC", timestamps=timestamps, tick_size=1.0))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "timestamp_gap_too_wide"


def test_trend_following_generates_buy_on_uptrend_pullback():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
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
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.03,
        },
    )
    signal = strategy.generate_signal(_ctx([104.0, 105.0, 106.0, 105.0, 104.0, 103.0, 101.0, 102.0, 102.5, 102.2]))
    assert signal.side == Side.SELL
    assert signal.metadata.get("entry_type") == "pullback"
    assert signal.metadata.get("indicator") == "ema_trend_following"


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


def test_trend_following_uses_channel_based_stop_when_wider_than_minimum():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
            "use_donchian_filter": True,
            "trend_pullback_max_distance_ratio": 0.01,
            "trend_min_stop_loss_pips": 30.0,
            "trend_index_min_stop_pct": 0.0,
            "trend_risk_reward_ratio": 2.5,
        },
    )
    prices = [25000.0, 24920.0, 24850.0, 24900.0, 24980.0, 25060.0, 25140.0, 25090.0, 25050.0, 25070.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(200.0)
    assert signal.take_profit_pips == pytest.approx(500.0)


def test_trend_following_requires_pullback_zone_for_entry():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
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


def test_trend_following_index_pip_size_fallback_matches_index_step():
    strategy = create_strategy("trend_following", {})
    assert strategy._pip_size("DE40") == pytest.approx(0.1)  # type: ignore[attr-defined]
    assert strategy._pip_size("US100") == pytest.approx(0.1)  # type: ignore[attr-defined]
    assert strategy._pip_size("AUS200") == pytest.approx(1.0)  # type: ignore[attr-defined]


def test_trend_following_blocks_when_volume_spike_is_required():
    strategy = create_strategy(
        "trend_following",
        {
            "fast_ema_window": 3,
            "slow_ema_window": 5,
            "donchian_window": 3,
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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_not_confirmed"


def test_trend_following_volume_spike_boosts_confidence():
    base_params = {
        "fast_ema_window": 3,
        "slow_ema_window": 5,
        "donchian_window": 3,
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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "crypto_trend_too_weak"


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
    assert signal.stop_loss_pips == 20.0
    assert signal.take_profit_pips == 60.0
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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_threshold"


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
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0])
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "index_hybrid"


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
    assert signal.metadata.get("reason") == "regime_filter_blocked"
    assert signal.metadata.get("regime_fallback_applied") is False
    assert signal.metadata.get("regime_fallback_selected") is None


def test_index_hybrid_fuzzy_mode_selects_regime_without_dead_zone():
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
    assert signal.metadata.get("regime_selection_mode") == "fuzzy"
    assert signal.metadata.get("regime_index", 0.0) > 0.0
    assert signal.metadata.get("reason") != "regime_filter_blocked"
    assert signal.metadata.get("reason") != "no_signal_in_active_regime"


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
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0]
    volumes = [100.0, 101.0, 99.0, 100.0, 100.0, 101.0, 99.0, 115.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.BUY
    assert signal.metadata.get("volume_spike") is False
    assert signal.metadata.get("volume_confidence_boost_applied") == pytest.approx(0.0)


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
    prices = [26000.0, 25980.0, 25950.0, 25920.0, 25880.0, 25840.0, 25800.0, 25750.0, 25710.0, 25660.0, 25600.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    assert signal.side == Side.SELL
    assert signal.metadata.get("regime") == "trend_following"
    assert signal.metadata.get("breakout_down") is True


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
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25040.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=0.1))
    expected_min_stop_pips = (prices[-1] * 0.5 / 100.0) / 0.1
    expected_min_take_pips = (prices[-1] * 1.5 / 100.0) / 0.1
    assert signal.side == Side.BUY
    assert signal.stop_loss_pips >= expected_min_stop_pips
    assert signal.take_profit_pips >= expected_min_take_pips


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
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25040.0, 25120.0]
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
    prices = [25000.0, 24980.0, 24960.0, 24970.0, 24990.0, 25020.0, 25040.0, 25120.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="AUS200", tick_size=None))
    assert signal.side == Side.BUY
    assert signal.metadata.get("pip_size") == pytest.approx(1.0)
    assert signal.metadata.get("indicator") == "index_hybrid"


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
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0])
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "breakout_too_shallow"


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
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0]
    volumes = [100.0, 101.0, 99.0, 100.0, 100.0, 101.0, 99.0, 115.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0, volumes=volumes))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_not_confirmed"


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
    prices = [100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0]
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
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 99.0, 98.0, 97.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))
    assert signal.side == Side.BUY
    assert signal.metadata.get("regime") == "mean_reversion"
    assert signal.metadata.get("breakout_down") is True


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
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 99.0, 98.0, 97.0]
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
        StrategyContext(symbol="US100", prices=[100.0, 99.0, 98.0, 99.0, 100.0, 101.0, 102.0, 104.0])
    )
    assert signal.side == Side.BUY
    assert signal.confidence < 0.6
    assert signal.metadata.get("breakout_distance_ratio", 0.0) >= 0.0


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
            "index_trend_session_start_hour_utc": 6,
            "index_trend_session_end_hour_utc": 22,
            "index_mean_reversion_outside_trend_session": True,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 102.0, 101.8, 101.6, 101.4, 101.2]
    timestamps = [_ts("14:00") + idx * 60 for idx in range(len(prices))]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", timestamps=timestamps, tick_size=0.1))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_blocked_by_session"


def test_mean_breakout_buy_signal():
    strategy = create_strategy(
        "mean_breakout",
        {
            "mean_breakout_window": 5,
            "mean_breakout_breakout_window": 3,
            "mean_breakout_zscore_threshold": 1.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 102.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="DE40"))
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "mean_breakout"
    assert signal.metadata.get("direction") == "up"


def test_mean_breakout_sell_signal():
    strategy = create_strategy(
        "mean_breakout",
        {
            "mean_breakout_window": 5,
            "mean_breakout_breakout_window": 3,
            "mean_breakout_zscore_threshold": 1.0,
        },
    )
    prices = [102.0, 102.0, 102.0, 102.0, 102.0, 101.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="WTI"))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "mean_breakout"
    assert signal.metadata.get("direction") == "down"


def test_mean_breakout_holds_without_breakout():
    strategy = create_strategy(
        "mean_breakout",
        {
            "mean_breakout_window": 5,
            "mean_breakout_breakout_window": 3,
            "mean_breakout_zscore_threshold": 1.0,
        },
    )
    prices = [100.0, 100.0, 100.0, 100.0, 101.0, 100.5, 100.7]
    signal = strategy.generate_signal(_ctx(prices, symbol="DE40"))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "no_breakout"


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
    assert m15_signal.take_profit_pips == pytest.approx(150.0)

    h1_signal = strategy.generate_signal(
        _ctx(prices, symbol="US100", timestamps=[t0 + (idx * 3600) for idx in range(len(prices))], tick_size=1.0)
    )
    assert h1_signal.side == Side.BUY
    assert h1_signal.metadata.get("timeframe_mode") == "H1_plus"
    assert h1_signal.stop_loss_pips == pytest.approx(120.0)
    assert h1_signal.take_profit_pips == pytest.approx(300.0)


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
    assert signal.take_profit_pips == pytest.approx(300.0)


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


def test_mean_breakout_v2_blocks_overextended_breakout_by_default():
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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "zscore_overextended"


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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_not_confirmed"


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


def test_mean_breakout_session_buy_signal():
    strategy = create_strategy(
        "mean_breakout_session",
        {
            "mean_breakout_session_timezone": "UTC",
            "mean_breakout_session_start": "09:00",
            "mean_breakout_session_box_minutes": 15,
            "mean_breakout_session_trade_window_minutes": 120,
            "mean_breakout_session_one_trade_per_session": True,
        },
    )
    prices = [99.5, 100.0, 101.0, 100.8, 101.4, 101.2, 102.0]
    timestamps = [_ts("08:59"), _ts("09:00"), _ts("09:05"), _ts("09:10"), _ts("09:14"), _ts("09:15"), _ts("09:16")]
    signal = strategy.generate_signal(_ctx(prices, symbol="DE40", timestamps=timestamps))
    assert signal.side == Side.BUY
    assert signal.metadata.get("indicator") == "mean_breakout_session"
    assert signal.metadata.get("direction") == "up"


def test_mean_breakout_session_sell_signal():
    strategy = create_strategy(
        "mean_breakout_session",
        {
            "mean_breakout_session_timezone": "UTC",
            "mean_breakout_session_start": "09:00",
            "mean_breakout_session_box_minutes": 15,
            "mean_breakout_session_trade_window_minutes": 120,
            "mean_breakout_session_one_trade_per_session": True,
        },
    )
    prices = [102.0, 101.5, 101.0, 101.2, 100.8, 101.0, 100.4]
    timestamps = [_ts("08:59"), _ts("09:00"), _ts("09:05"), _ts("09:10"), _ts("09:14"), _ts("09:15"), _ts("09:16")]
    signal = strategy.generate_signal(_ctx(prices, symbol="WTI", timestamps=timestamps))
    assert signal.side == Side.SELL
    assert signal.metadata.get("indicator") == "mean_breakout_session"
    assert signal.metadata.get("direction") == "down"


def test_mean_breakout_session_holds_while_box_is_building():
    strategy = create_strategy(
        "mean_breakout_session",
        {
            "mean_breakout_session_timezone": "UTC",
            "mean_breakout_session_start": "09:00",
            "mean_breakout_session_box_minutes": 15,
        },
    )
    prices = [99.5, 100.0, 101.0, 100.8]
    timestamps = [_ts("08:59"), _ts("09:00"), _ts("09:05"), _ts("09:10")]
    signal = strategy.generate_signal(_ctx(prices, symbol="DE40", timestamps=timestamps))
    assert signal.side == Side.HOLD


def test_mean_breakout_session_allows_only_one_trade_per_session():
    strategy = create_strategy(
        "mean_breakout_session",
        {
            "mean_breakout_session_timezone": "UTC",
            "mean_breakout_session_start": "09:00",
            "mean_breakout_session_box_minutes": 15,
            "mean_breakout_session_one_trade_per_session": True,
        },
    )

    prices_first = [99.5, 100.0, 101.0, 100.8, 101.4, 101.2, 102.0]
    timestamps_first = [_ts("08:59"), _ts("09:00"), _ts("09:05"), _ts("09:10"), _ts("09:14"), _ts("09:15"), _ts("09:16")]
    first_signal = strategy.generate_signal(_ctx(prices_first, symbol="DE40", timestamps=timestamps_first))
    assert first_signal.side == Side.BUY

    prices_second = [99.5, 100.0, 101.0, 100.8, 101.4, 101.2, 102.0, 101.3, 102.2]
    timestamps_second = [
        _ts("08:59"),
        _ts("09:00"),
        _ts("09:05"),
        _ts("09:10"),
        _ts("09:14"),
        _ts("09:15"),
        _ts("09:16"),
        _ts("09:20"),
        _ts("09:21"),
    ]
    second_signal = strategy.generate_signal(_ctx(prices_second, symbol="DE40", timestamps=timestamps_second))
    assert second_signal.side == Side.HOLD
    assert second_signal.metadata.get("reason") == "session_trade_already_taken"


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
    assert signal.stop_loss_pips > 0.0
    assert signal.take_profit_pips == pytest.approx(signal.stop_loss_pips * 3.0)


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


def test_g1_cross_or_trend_mode_allows_trend_continuation_entry():
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
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("entry_mode") == "cross_or_trend"


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
    strategy._adx = lambda _prices, _window: 15.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.BUY
    assert signal.metadata.get("trend_signal") == "ema_trend_up"
    assert signal.metadata.get("is_continuation_entry") is True
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(14.85)


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
    strategy._adx = lambda _prices, _window: 10.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "adx_below_threshold"
    assert signal.metadata.get("is_continuation_entry") is True
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(14.85)


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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "adx_below_threshold"


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
    assert index_signal.side == Side.HOLD
    assert index_signal.metadata.get("profile") == "index"
    assert index_signal.metadata.get("reason") == "adx_below_threshold"


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
        },
    )
    prices = [100.0, 101.31, 100.11, 99.87, 98.87, 100.26, 100.76, 99.38]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "price_too_far_from_ema"


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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_not_confirmed"


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
    strategy._adx = lambda _prices, _window: 15.0  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "adx_below_threshold"
    assert signal.metadata.get("effective_adx_entry_threshold") == pytest.approx(18.0)


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


def test_g1_can_include_in_progress_candle_for_entry_when_enabled():
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
    prices = [101.0, 100.0, 99.0, 98.0, 99.0, 100.0, 110.0]
    timestamps = [0.0, 60.0, 120.0, 180.0, 240.0, 300.0, 305.0]

    closes, using_closed = strategy._resample_closed_candle_closes(prices, timestamps)  # type: ignore[attr-defined]
    assert using_closed is True
    assert closes == pytest.approx([101.0, 100.0, 99.0, 98.0, 99.0, 110.0])

    signal = strategy.generate_signal(
        _ctx(prices, symbol="EURUSD", timestamps=timestamps, current_spread_pips=0.8, tick_size=0.0001)
    )
    assert signal.side == Side.BUY
    assert signal.metadata.get("use_incomplete_candle_for_entry") is True


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
    # 2 completed candles: [0..59] close=102, [60..119] close=105; last in-progress is excluded.
    assert closes == pytest.approx([102.0])


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
    # Sunday value (101.0) is ignored, then current in-progress candle (Monday) is excluded.
    assert closes == pytest.approx([100.0])


def test_g1_auto_resample_skips_when_input_is_already_candle_like():
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
        },
    )
    adx_values = iter([16.0, 13.0, 12.0])
    strategy._adx = lambda _prices, _window: next(adx_values)  # type: ignore[method-assign]
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]

    first = strategy.generate_signal(_ctx(prices, symbol="EURUSD", current_spread_pips=0.8))
    second = strategy.generate_signal(_ctx(prices + [108.0], symbol="EURUSD", current_spread_pips=0.8))
    third = strategy.generate_signal(_ctx(prices + [109.0], symbol="EURUSD", current_spread_pips=0.8))

    assert first.side == Side.BUY
    assert second.side == Side.BUY
    assert third.side == Side.HOLD
    assert third.metadata.get("reason") == "adx_below_threshold"
    assert third.metadata.get("adx_regime_active_before") is True
    assert third.metadata.get("adx_regime_active_after") is False


def test_mean_reversion_buy_signal_with_trend_filter_and_atr_sl_tp():
    strategy = create_strategy(
        "mean_reversion",
        {
            "mean_reversion_zscore_window": 5,
            "mean_reversion_zscore_threshold": 1.0,
            "mean_reversion_trend_filter_enabled": True,
            "mean_reversion_trend_ma_window": 10,
            "mean_reversion_use_atr_sl_tp": True,
            "mean_reversion_atr_window": 3,
            "mean_reversion_atr_multiplier": 2.0,
            "mean_reversion_risk_reward_ratio": 1.8,
            "mean_reversion_min_stop_loss_pips": 1.0,
            "mean_reversion_min_take_profit_pips": 1.0,
        },
    )
    prices = [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 106.0, 104.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="US100", tick_size=1.0))

    assert signal.side == Side.BUY
    assert signal.stop_loss_pips == pytest.approx(2.6666666666666665)
    assert signal.take_profit_pips == pytest.approx(4.8)
    assert signal.metadata.get("indicator") == "mean_reversion"
    assert signal.metadata.get("zscore") is not None


def test_mean_reversion_blocks_counter_trend_entries():
    strategy = create_strategy(
        "mean_reversion",
        {
            "mean_reversion_zscore_window": 5,
            "mean_reversion_zscore_threshold": 1.0,
            "mean_reversion_trend_filter_enabled": True,
            "mean_reversion_trend_ma_window": 10,
            "mean_reversion_use_atr_sl_tp": False,
        },
    )
    prices = [111.0, 110.0, 109.0, 108.0, 107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "blocked_by_trend_filter"
    assert signal.metadata.get("indicator") == "mean_reversion"


def test_mean_reversion_emits_exit_hint_when_price_reverts_to_mean():
    strategy = create_strategy(
        "mean_reversion",
        {
            "mean_reversion_zscore_window": 5,
            "mean_reversion_zscore_threshold": 1.0,
            "mean_reversion_exit_zscore": 0.2,
            "mean_reversion_trend_filter_enabled": False,
            "mean_reversion_use_atr_sl_tp": False,
        },
    )
    prices = [99.0, 100.0, 101.0, 99.0, 100.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))

    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_target_reached"
    assert signal.metadata.get("exit_hint") == "close_on_mean_reversion"
    assert abs(float(signal.metadata.get("zscore", 1.0))) <= 0.2


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
            "mean_reversion_bb_min_std_ratio": 0.0,
        },
    )
    signal = strategy.generate_signal(_ctx([100.0, 101.0, 99.0, 100.0, 100.0, 100.0]))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "mean_reversion_target_reached"
    assert signal.metadata.get("exit_hint") == "close_on_bb_midline"


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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_threshold"


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
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "volume_below_threshold"
    assert signal.metadata.get("volume_series_mode") == "cumulative_candle_max"
    assert int(signal.metadata.get("volume_resets_detected") or 0) >= 1


def test_mean_reversion_bb_index_pip_size_fallback_handles_aus200():
    strategy = create_strategy("mean_reversion_bb", {})
    assert strategy._pip_size("AUS200") == pytest.approx(1.0)  # type: ignore[attr-defined]


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


def test_mean_reversion_bb_trend_filter_extreme_override_allows_countertrend_entry():
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
    assert signal.metadata.get("trend_filter_override") is True


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
