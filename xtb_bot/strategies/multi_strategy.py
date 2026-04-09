from __future__ import annotations

from typing import Any

from xtb_bot.models import Signal
from xtb_bot.strategies.base import Strategy, StrategyContext


_MULTI_STRATEGY_BASE_COMPONENT_PARAM = "_multi_strategy_base_component"


class MultiStrategyCarrier(Strategy):
    name = "multi_strategy"
    min_history = 0

    def __init__(self, params: dict[str, Any]):
        super().__init__(params)
        raw_names = params.get("multi_strategy_names") or params.get("multi_strategy_default_names") or ""
        self._component_names = tuple(
            str(item).strip().lower()
            for item in str(raw_names).split(",")
            if str(item).strip()
        )
        self._base_component = str(params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or "").strip().lower()

    def supports_symbol(self, symbol: str) -> bool:
        return bool(str(symbol or "").strip())

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        signal = self.hold()
        metadata = dict(signal.metadata)
        metadata.update(
            {
                "reason": "carrier_runtime_only",
                "carrier_only": True,
                "carrier_strategy": self.name,
                "component_count": len(self._component_names),
                "components": list(self._component_names),
                "base_component": self._base_component or None,
                "symbol": str(ctx.symbol or "").strip().upper(),
            }
        )
        signal.metadata = metadata
        return signal
