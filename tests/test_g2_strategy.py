from __future__ import annotations

from datetime import datetime, timezone

import pytest

from xtb_bot.models import Side
from xtb_bot.strategies import create_strategy
from xtb_bot.strategies.base import StrategyContext


def _expand_candles(
    candles: list[tuple[float, float, float, float]],
    *,
    timeframe_sec: int = 60,
    start_ts: float = 1_775_000_000.0,
) -> tuple[list[float], list[float]]:
    aligned_start_ts = float((int(start_ts) // timeframe_sec) * timeframe_sec)
    prices: list[float] = []
    timestamps: list[float] = []
    for idx, (open_price, high_price, low_price, close_price) in enumerate(candles):
        bucket_start = aligned_start_ts + idx * timeframe_sec
        for offset, price in (
            (0.0, open_price),
            (timeframe_sec * 0.20, high_price),
            (timeframe_sec * 0.50, low_price),
            (timeframe_sec * 0.80, close_price),
        ):
            prices.append(float(price))
            timestamps.append(bucket_start + offset)
    return prices, timestamps


def _trend_then_pullback_context(*, spread_pips: float = 0.8) -> StrategyContext:
    candles: list[tuple[float, float, float, float]] = []
    close = 100.0
    for _ in range(28):
        open_price = close
        close += 0.55
        candles.append((open_price, close + 0.18, open_price - 0.08, close))

    prev_close = close
    close = prev_close - 2.4
    candles.append((prev_close, prev_close + 0.10, close - 0.40, close))

    open_price = close
    recovery_close = prev_close - 0.35
    candles.append((open_price, recovery_close + 0.20, open_price - 0.85, recovery_close))

    candles.append((recovery_close, recovery_close + 0.12, recovery_close - 0.10, recovery_close + 0.05))
    prices, timestamps = _expand_candles(candles)
    return StrategyContext(
        symbol="US100",
        prices=prices,
        timestamps=timestamps,
        current_spread_pips=spread_pips,
        pip_size=1.0,
        tick_size=1.0,
    )


def test_g2_defaults_use_faster_intraday_recovery_profile():
    strategy = create_strategy("g2", {})
    assert strategy.candle_timeframe_sec == 60
    assert strategy.risk_reward_ratio == 2.2
    assert strategy.min_stop_loss_pips == 35.0
    assert strategy.min_take_profit_pips == 75.0
    assert strategy.max_pullback_depth_atr == 0.65
    assert strategy.reclaim_buffer_atr == 0.18
    assert strategy.session_filter_enabled is False
    assert strategy.session_open_delay_minutes == 0


def test_g2_supports_only_indices():
    strategy = create_strategy("g2", {})
    assert strategy.supports_symbol("US100")
    assert strategy.supports_symbol("TOPIX")
    assert not strategy.supports_symbol("WTI")
    assert not strategy.supports_symbol("EURUSD")


def test_g2_returns_hold_for_non_index_symbol():
    strategy = create_strategy("g2", {})
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="EURUSD",
            prices=[1.0] * 300,
            timestamps=[1_775_000_000.0 + i for i in range(300)],
            pip_size=0.0001,
            tick_size=0.0001,
        )
    )
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "symbol_not_index"


def test_g2_generates_buy_signal_for_pullback_recovery():
    strategy = create_strategy(
        "g2",
        {
            "g2_ema_fast": 4,
            "g2_ema_trend": 10,
            "g2_ema_macro": 24,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_min_trend_gap_ratio": 0.0,
            "g2_min_trend_slope_ratio": 0.0,
            "g2_min_macro_slope_ratio": 0.0,
            "g2_min_atr_pct": 0.0,
            "g2_max_pullback_depth_atr": 1.6,
            "g2_max_trend_ema_breach_atr": 0.8,
            "g2_max_close_to_fast_ema_distance_atr": 0.5,
            "g2_session_filter_enabled": False,
            "g2_max_spread_to_stop_ratio": 0.6,
        },
    )
    signal = strategy.generate_signal(_trend_then_pullback_context(spread_pips=0.6))
    assert signal.side == Side.BUY
    assert signal.confidence >= 0.6
    assert signal.metadata.get("signal_reason") == "buy_the_dip"
    assert signal.metadata.get("using_closed_candles") is True


def test_g2_off_resample_uses_raw_bars_for_snapshot_flow():
    strategy = create_strategy(
        "g2",
        {
            "g2_candle_timeframe_sec": 60.0,
            "g2_resample_mode": "off",
        },
    )
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    base_ts = 1_775_000_000.0
    timestamps = [base_ts + offset for offset in (0.0, 15.0, 30.0, 60.0, 75.0, 90.0)]
    bars, using_closed, resample_error = strategy._resample_closed_candle_ohlc(  # type: ignore[attr-defined]
        prices,
        timestamps,
    )
    assert resample_error is None
    assert using_closed is False
    assert [bar.close for bar in bars] == pytest.approx(prices)


def test_g2_blocks_entry_when_spread_too_wide():
    strategy = create_strategy(
        "g2",
        {
            "g2_ema_fast": 4,
            "g2_ema_trend": 10,
            "g2_ema_macro": 24,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_min_trend_gap_ratio": 0.0,
            "g2_min_trend_slope_ratio": 0.0,
            "g2_min_macro_slope_ratio": 0.0,
            "g2_min_atr_pct": 0.0,
            "g2_max_pullback_depth_atr": 1.6,
            "g2_max_trend_ema_breach_atr": 0.8,
            "g2_max_close_to_fast_ema_distance_atr": 0.5,
            "g2_session_filter_enabled": False,
            "g2_max_spread_pips": 1.0,
        },
    )
    signal = strategy.generate_signal(_trend_then_pullback_context(spread_pips=1.4))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "spread_too_wide"


def test_g2_spread_limit_can_be_overridden_per_symbol():
    strategy = create_strategy(
        "g2",
        {
            "g2_ema_fast": 4,
            "g2_ema_trend": 10,
            "g2_ema_macro": 24,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_min_trend_gap_ratio": 0.0,
            "g2_min_trend_slope_ratio": 0.0,
            "g2_min_macro_slope_ratio": 0.0,
            "g2_min_atr_pct": 0.0,
            "g2_max_pullback_depth_atr": 1.6,
            "g2_max_trend_ema_breach_atr": 0.8,
            "g2_max_close_to_fast_ema_distance_atr": 0.5,
            "g2_session_filter_enabled": False,
            "g2_max_spread_pips": 1.0,
            "g2_max_spread_pips_by_symbol": {"NK20": 3.0},
            "g2_max_spread_to_stop_ratio": 0.6,
        },
    )
    base_ctx = _trend_then_pullback_context(spread_pips=1.4)
    blocked = strategy.generate_signal(
        StrategyContext(
            symbol="DE40",
            prices=base_ctx.prices,
            timestamps=base_ctx.timestamps,
            volumes=base_ctx.volumes,
            current_volume=base_ctx.current_volume,
            current_spread_pips=base_ctx.current_spread_pips,
            pip_size=base_ctx.pip_size,
            tick_size=base_ctx.tick_size,
        )
    )
    assert blocked.side == Side.HOLD
    assert blocked.metadata.get("reason") == "spread_too_wide"

    allowed = strategy.generate_signal(
        StrategyContext(
            symbol="NK20",
            prices=base_ctx.prices,
            timestamps=base_ctx.timestamps,
            volumes=base_ctx.volumes,
            current_volume=base_ctx.current_volume,
            current_spread_pips=base_ctx.current_spread_pips,
            pip_size=base_ctx.pip_size,
            tick_size=base_ctx.tick_size,
        )
    )
    assert allowed.side == Side.BUY


def test_g2_blocks_outside_market_session():
    strategy = create_strategy(
        "g2",
        {
            "g2_ema_fast": 4,
            "g2_ema_trend": 10,
            "g2_ema_macro": 24,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_min_trend_gap_ratio": 0.0,
            "g2_min_trend_slope_ratio": 0.0,
            "g2_min_macro_slope_ratio": 0.0,
            "g2_min_atr_pct": 0.0,
            "g2_max_pullback_depth_atr": 1.6,
            "g2_max_trend_ema_breach_atr": 0.8,
            "g2_max_close_to_fast_ema_distance_atr": 0.5,
            "g2_session_filter_enabled": True,
            "g2_session_open_delay_minutes": 0,
        },
    )
    start_ts = datetime(2026, 4, 4, 2, 0, tzinfo=timezone.utc).timestamp()
    candles: list[tuple[float, float, float, float]] = []
    close = 100.0
    for _ in range(28):
        open_price = close
        close += 0.55
        candles.append((open_price, close + 0.18, open_price - 0.08, close))
    prev_close = close
    close = prev_close - 2.4
    candles.append((prev_close, prev_close + 0.10, close - 0.40, close))
    open_price = close
    recovery_close = prev_close - 0.35
    candles.append((open_price, recovery_close + 0.20, open_price - 0.85, recovery_close))
    candles.append((recovery_close, recovery_close + 0.12, recovery_close - 0.10, recovery_close + 0.05))
    prices, timestamps = _expand_candles(candles, start_ts=start_ts)
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="US100",
            prices=prices,
            timestamps=timestamps,
            current_spread_pips=0.8,
            pip_size=1.0,
            tick_size=1.0,
        )
    )
    assert signal.side == Side.BUY
    assert "session_filter_blocked" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("session_softened") is True


def test_g2_accepts_one_minute_candle_timeframe():
    strategy = create_strategy("g2", {"g2_candle_timeframe_sec": 60})
    assert strategy.candle_timeframe_sec == 60


def test_g2_blocks_long_when_vwap_reclaim_is_not_confirmed():
    strategy = create_strategy(
        "g2",
        {
            "g2_ema_fast": 4,
            "g2_ema_trend": 10,
            "g2_ema_macro": 24,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_min_trend_gap_ratio": 0.0,
            "g2_min_trend_slope_ratio": 0.0,
            "g2_min_macro_slope_ratio": 0.0,
            "g2_min_atr_pct": 0.0,
            "g2_min_pullback_depth_atr": 0.0,
            "g2_max_pullback_depth_atr": 3.0,
            "g2_max_trend_ema_breach_atr": 2.0,
            "g2_max_close_to_fast_ema_distance_atr": 1.0,
            "g2_session_filter_enabled": False,
            "g2_vwap_filter_enabled": True,
            "g2_vwap_reclaim_required": True,
            "g2_vwap_min_session_bars": 6,
            "g2_vwap_min_volume_samples": 6,
        },
    )
    candles = []
    close = 100.0
    for _ in range(28):
        open_price = close
        close += 0.7
        candles.append((open_price, close + 0.15, open_price - 0.05, close))
    candles.append((close, close + 0.1, close - 5.0, close - 4.7))
    candles.append((close - 4.7, close - 2.2, close - 5.0, close - 2.4))
    candles.append((close - 2.4, close - 2.35, close - 2.6, close - 2.4))
    prices, timestamps = _expand_candles(candles)
    volumes = [100.0] * len(prices)
    signal = strategy.generate_signal(
        StrategyContext(
            symbol="US100",
            prices=prices,
            timestamps=timestamps,
            volumes=volumes,
            current_spread_pips=0.6,
            pip_size=1.0,
            tick_size=1.0,
        )
    )
    assert signal.side == Side.BUY
    assert "vwap_reclaim_not_confirmed" in (signal.metadata.get("soft_filter_reasons") or [])
    assert signal.metadata.get("vwap_softened") is True
    assert signal.metadata.get("vwap_status") == "ok"
