from __future__ import annotations

import logging
from typing import Any

from xtb_bot.models import Side, Signal
from xtb_bot.strategies.base import Strategy, StrategyContext

logger = logging.getLogger(__name__)


class MultiStrategy(Strategy):
    """Runs all registered strategies in parallel, picks the best signal.

    Selection logic:
    - Collects non-HOLD signals from all sub-strategies.
    - Groups by direction (BUY / SELL).
    - Picks the side with the most votes; on tie picks higher max confidence.
    - Within the winning side, selects the signal with the highest confidence.
    - Merges metadata from all agreeing strategies for transparency.
    """

    name = "multi"

    # Strategies that are meta/wrapper or need special symbol sets are excluded.
    _EXCLUDE: frozenset[str] = frozenset({
        "multi",
        "momentum_index",
        "momentum_fx",
    })

    def __init__(self, params: dict[str, Any]) -> None:
        super().__init__(params)

        from xtb_bot.strategies import _STRATEGIES

        include_raw = params.get("multi_strategies")
        if include_raw:
            include = {s.strip().lower() for s in str(include_raw).split(",")}
        else:
            include = None

        self._sub_strategies: list[Strategy] = []
        for key, cls in _STRATEGIES.items():
            if key in self._EXCLUDE:
                continue
            if include is not None and key not in include:
                continue
            try:
                self._sub_strategies.append(cls(params))
            except Exception:
                logger.warning("multi: failed to instantiate strategy %s", key, exc_info=True)

        self.min_history = max((s.min_history for s in self._sub_strategies), default=2)

    def supports_symbol(self, symbol: str) -> bool:
        return any(s.supports_symbol(symbol) for s in self._sub_strategies)

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        candidates: list[tuple[Signal, Strategy]] = []

        for strategy in self._sub_strategies:
            if not strategy.supports_symbol(ctx.symbol):
                continue
            if len(ctx.prices) < strategy.min_history:
                continue
            try:
                signal = strategy.generate_signal(ctx)
            except Exception:
                logger.warning("multi: %s raised on %s", strategy.name, ctx.symbol, exc_info=True)
                continue
            if signal.side in (Side.BUY, Side.SELL):
                candidates.append((signal, strategy))

        if not candidates:
            return self._hold_with_votes([])

        buy_candidates = [(s, st) for s, st in candidates if s.side == Side.BUY]
        sell_candidates = [(s, st) for s, st in candidates if s.side == Side.SELL]

        if len(buy_candidates) > len(sell_candidates):
            winner_group = buy_candidates
        elif len(sell_candidates) > len(buy_candidates):
            winner_group = sell_candidates
        else:
            buy_best = max(s.confidence for s, _ in buy_candidates)
            sell_best = max(s.confidence for s, _ in sell_candidates)
            winner_group = buy_candidates if buy_best >= sell_best else sell_candidates

        best_signal, best_strategy = max(winner_group, key=lambda x: x[0].confidence)

        meta = dict(best_signal.metadata)
        meta["source_strategy"] = best_strategy.name
        meta["multi_votes_buy"] = len(buy_candidates)
        meta["multi_votes_sell"] = len(sell_candidates)
        meta["multi_votes_total"] = len(candidates)
        meta["multi_voters"] = [st.name for _, st in winner_group]
        meta["multi_all_signals"] = {
            st.name: {"side": s.side.value, "confidence": round(s.confidence, 4)}
            for s, st in candidates
        }

        return Signal(
            side=best_signal.side,
            confidence=best_signal.confidence,
            stop_loss_pips=best_signal.stop_loss_pips,
            take_profit_pips=best_signal.take_profit_pips,
            metadata=meta,
        )

    def _hold_with_votes(self, candidates: list) -> Signal:
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=float(self.params.get("stop_loss_pips", 25)),
            take_profit_pips=float(self.params.get("take_profit_pips", 50)),
            metadata={
                "reason": "multi_no_signal",
                "indicator": "multi",
                "multi_votes_total": len(candidates),
            },
        )
