from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from xtb_bot.models import Side, Signal


@dataclass(slots=True)
class StrategyContext:
    symbol: str
    prices: Sequence[float]
    timestamps: Sequence[float] = field(default_factory=tuple)
    volumes: Sequence[float] = field(default_factory=tuple)
    current_volume: float | None = None
    current_spread_pips: float | None = None
    tick_size: float | None = None


class Strategy(ABC):
    name: str = "base"
    min_history: int = 2

    def __init__(self, params: dict[str, Any]):
        self.params = params

    def supports_symbol(self, symbol: str) -> bool:
        return True

    @abstractmethod
    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raise NotImplementedError

    def hold(self) -> Signal:
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=float(self.params.get("stop_loss_pips", 25)),
            take_profit_pips=float(self.params.get("take_profit_pips", 50)),
            metadata={"reason": "no_signal"},
        )
