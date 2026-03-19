from __future__ import annotations

from xtb_bot.strategies.base import Strategy
from xtb_bot.strategies.crypto_trend_following import CryptoTrendFollowingStrategy
from xtb_bot.strategies.donchian_breakout import DonchianBreakoutStrategy
from xtb_bot.strategies.g1 import G1Strategy
from xtb_bot.strategies.index_hybrid import IndexHybridStrategy
from xtb_bot.strategies.mean_breakout import MeanBreakoutStrategy
from xtb_bot.strategies.mean_breakout_v2 import MeanBreakoutStrategyV2
from xtb_bot.strategies.mean_breakout_session import MeanBreakoutSessionStrategy
from xtb_bot.strategies.mean_reversion_bb import MeanReversionBbStrategy
from xtb_bot.strategies.mean_reversion import MeanReversionStrategy
from xtb_bot.strategies.momentum import MomentumStrategy
from xtb_bot.strategies.trend_following import TrendFollowingStrategy

_STRATEGIES: dict[str, type[Strategy]] = {
    CryptoTrendFollowingStrategy.name: CryptoTrendFollowingStrategy,
    DonchianBreakoutStrategy.name: DonchianBreakoutStrategy,
    G1Strategy.name: G1Strategy,
    IndexHybridStrategy.name: IndexHybridStrategy,
    MeanBreakoutStrategy.name: MeanBreakoutStrategy,
    MeanBreakoutStrategyV2.name: MeanBreakoutStrategyV2,
    MeanBreakoutSessionStrategy.name: MeanBreakoutSessionStrategy,
    MeanReversionBbStrategy.name: MeanReversionBbStrategy,
    MomentumStrategy.name: MomentumStrategy,
    MeanReversionStrategy.name: MeanReversionStrategy,
    TrendFollowingStrategy.name: TrendFollowingStrategy,
    # Momentum aliases for separate presets/schedules by market regime.
    "momentum_index": MomentumStrategy,
    "momentum_fx": MomentumStrategy,
}


def create_strategy(name: str, params: dict[str, object]) -> Strategy:
    key = name.lower().strip()
    if key not in _STRATEGIES:
        allowed = ", ".join(sorted(_STRATEGIES))
        raise ValueError(f"Unknown strategy: {name}. Allowed: {allowed}")
    return _STRATEGIES[key](params)


def available_strategies() -> list[str]:
    return sorted(_STRATEGIES)
