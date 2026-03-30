from __future__ import annotations

from xtb_bot.models import Side
from xtb_bot.strategies import available_strategies, create_strategy
from xtb_bot.strategies.base import StrategyContext


def _ctx(
    prices: list[float],
    symbol: str = "US100",
    tick_size: float | None = None,
) -> StrategyContext:
    return StrategyContext(
        symbol=symbol,
        prices=prices,
        timestamps=[],
        volumes=[],
        tick_size=tick_size,
    )


def test_multi_strategy_is_registered():
    assert "multi" in available_strategies()


def test_multi_strategy_runs_sub_strategies():
    strategy = create_strategy("multi", {})
    prices = [100.0] * 300
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    assert signal.side in (Side.BUY, Side.SELL, Side.HOLD)
    assert "indicator" in signal.metadata or "reason" in signal.metadata


def test_multi_strategy_hold_when_no_signals():
    strategy = create_strategy("multi", {})
    # Very few prices — most strategies won't have enough history
    prices = [100.0, 100.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD"))
    assert signal.side == Side.HOLD
    assert signal.metadata.get("reason") == "multi_no_signal"


def test_multi_strategy_returns_source_strategy_in_metadata():
    strategy = create_strategy("multi", {})
    # Generate enough price action for some strategies to fire
    prices = [100.0 + i * 0.5 for i in range(200)]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    if signal.side != Side.HOLD:
        assert "source_strategy" in signal.metadata
        assert "multi_votes_buy" in signal.metadata
        assert "multi_votes_sell" in signal.metadata
        assert "multi_voters" in signal.metadata


def test_multi_strategy_filter_by_multi_strategies_param():
    strategy = create_strategy("multi", {"multi_strategies": "momentum,g1"})
    assert len(strategy._sub_strategies) == 2
    names = {s.name for s in strategy._sub_strategies}
    assert names == {"momentum", "g1"}


def test_multi_strategy_min_history_is_max_of_sub_strategies():
    strategy = create_strategy("multi", {})
    individual_mins = [s.min_history for s in strategy._sub_strategies]
    assert strategy.min_history == max(individual_mins)


def test_multi_strategy_supports_symbol_delegates():
    strategy = create_strategy("multi", {})
    # Generic FX symbol — at least some strategies support it
    assert strategy.supports_symbol("EURUSD") is True


def test_multi_strategy_excludes_itself():
    strategy = create_strategy("multi", {})
    sub_names = {s.name for s in strategy._sub_strategies}
    assert "multi" not in sub_names


def test_multi_strategy_buy_signal_with_strong_trend():
    strategy = create_strategy(
        "multi",
        {"multi_strategies": "momentum", "fast_window": 5, "slow_window": 10},
    )
    # Strong uptrend — momentum should fire BUY
    prices = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0, 101.0, 102.0, 103.0, 104.0]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    if signal.side == Side.BUY:
        assert signal.metadata.get("source_strategy") == "momentum"
        assert signal.confidence > 0


def test_multi_strategy_votes_metadata_counts():
    strategy = create_strategy("multi", {"multi_strategies": "momentum,trend_following"})
    prices = [100.0 + i * 0.5 for i in range(200)]
    signal = strategy.generate_signal(_ctx(prices, symbol="EURUSD", tick_size=0.0001))
    if signal.side != Side.HOLD:
        total = signal.metadata.get("multi_votes_total", 0)
        buy = signal.metadata.get("multi_votes_buy", 0)
        sell = signal.metadata.get("multi_votes_sell", 0)
        assert total == buy + sell
        assert total > 0
