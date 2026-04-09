from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math
from typing import Any

from xtb_bot.models import Position, RunMode, Side, SymbolSpec


class WorkerPriceGuardRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    def broker_min_stop_distance_price(self) -> float:
        return self.broker_min_stop_distance_price_for_spec(self.worker.symbol_spec)

    @staticmethod
    def broker_min_stop_distance_price_for_spec(symbol_spec: SymbolSpec | None) -> float:
        if symbol_spec is None:
            return 0.0
        metadata = symbol_spec.metadata if isinstance(symbol_spec.metadata, dict) else {}
        raw = metadata.get("min_stop_distance_price")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(value) or value <= 0:
            return 0.0
        return value

    def apply_broker_min_stop_distance_guard(
        self,
        position: Position,
        desired_stop_loss: float,
        bid: float,
        ask: float,
    ) -> float | None:
        worker = self.worker
        min_stop_distance = self.broker_min_stop_distance_price()
        if min_stop_distance <= 0:
            return desired_stop_loss

        if position.side == Side.BUY:
            max_allowed_stop = worker._normalize_price_floor(bid - min_stop_distance)
            if max_allowed_stop <= position.stop_loss:
                return None
            adjusted = min(desired_stop_loss, max_allowed_stop)
            adjusted = worker._normalize_price_floor(adjusted)
            if adjusted <= position.stop_loss:
                return None
            return adjusted

        min_allowed_stop = worker._normalize_price_ceil(ask + min_stop_distance)
        if min_allowed_stop >= position.stop_loss:
            return None
        adjusted = max(desired_stop_loss, min_allowed_stop)
        adjusted = worker._normalize_price_ceil(adjusted)
        if adjusted >= position.stop_loss:
            return None
        return adjusted

    def apply_broker_open_level_guard(
        self,
        side: Side,
        stop_loss: float,
        take_profit: float,
        bid: float,
        ask: float,
    ) -> tuple[float, float, dict[str, float] | None]:
        return self.apply_broker_open_level_guard_for_spec(
            self.worker.symbol_spec,
            side,
            stop_loss,
            take_profit,
            bid,
            ask,
        )

    def apply_broker_open_level_guard_for_spec(
        self,
        symbol_spec: SymbolSpec | None,
        side: Side,
        stop_loss: float,
        take_profit: float,
        bid: float,
        ask: float,
    ) -> tuple[float, float, dict[str, float] | None]:
        worker = self.worker
        if worker.mode != RunMode.EXECUTION:
            return stop_loss, take_profit, None

        min_stop_distance = self.broker_min_stop_distance_price_for_spec(symbol_spec)
        if min_stop_distance <= 0 or bid <= 0 or ask <= 0:
            return stop_loss, take_profit, None

        original_stop_loss = stop_loss
        original_take_profit = take_profit

        if side == Side.BUY:
            max_allowed_stop = worker._normalize_price_floor_for_spec(symbol_spec, bid - min_stop_distance)
            min_allowed_take_profit = worker._normalize_price_ceil_for_spec(symbol_spec, ask + min_stop_distance)
            stop_loss = min(stop_loss, max_allowed_stop)
            take_profit = max(take_profit, min_allowed_take_profit)
            stop_loss = worker._normalize_price_floor_for_spec(symbol_spec, stop_loss)
            take_profit = worker._normalize_price_ceil_for_spec(symbol_spec, take_profit)
        else:
            min_allowed_stop = worker._normalize_price_ceil_for_spec(symbol_spec, ask + min_stop_distance)
            max_allowed_take_profit = worker._normalize_price_floor_for_spec(symbol_spec, bid - min_stop_distance)
            stop_loss = max(stop_loss, min_allowed_stop)
            take_profit = min(take_profit, max_allowed_take_profit)
            stop_loss = worker._normalize_price_ceil_for_spec(symbol_spec, stop_loss)
            take_profit = worker._normalize_price_floor_for_spec(symbol_spec, take_profit)

        changed = (
            abs(stop_loss - original_stop_loss) > FLOAT_ROUNDING_TOLERANCE
            or abs(take_profit - original_take_profit) > FLOAT_ROUNDING_TOLERANCE
        )
        if not changed:
            return stop_loss, take_profit, None

        payload = {
            "original_stop_loss": original_stop_loss,
            "adjusted_stop_loss": stop_loss,
            "original_take_profit": original_take_profit,
            "adjusted_take_profit": take_profit,
            "min_stop_distance_price": min_stop_distance,
            "bid": bid,
            "ask": ask,
        }
        return stop_loss, take_profit, payload
