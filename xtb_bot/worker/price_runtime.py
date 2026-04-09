from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

from xtb_bot.models import SymbolSpec


class WorkerPriceRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    @staticmethod
    def normalize_deal_reference(value: str) -> str:
        return "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_"})

    def normalize_price(self, price: float) -> float:
        return self.normalize_price_for_spec(self.worker.symbol_spec, price)

    @staticmethod
    def normalize_price_for_spec(symbol_spec: SymbolSpec | None, price: float) -> float:
        if symbol_spec is None:
            return price
        tick_size = max(symbol_spec.tick_size, FLOAT_COMPARISON_TOLERANCE)
        normalized = round(price / tick_size) * tick_size
        return round(normalized, symbol_spec.price_precision)

    def normalize_price_floor(self, price: float) -> float:
        return self.normalize_price_floor_for_spec(self.worker.symbol_spec, price)

    @staticmethod
    def normalize_price_floor_for_spec(symbol_spec: SymbolSpec | None, price: float) -> float:
        if symbol_spec is None:
            return price
        tick_size = max(symbol_spec.tick_size, FLOAT_COMPARISON_TOLERANCE)
        tick = Decimal(str(tick_size))
        value = Decimal(str(price))
        steps = (value / tick).to_integral_value(rounding=ROUND_FLOOR)
        normalized = steps * tick
        return round(float(normalized), symbol_spec.price_precision)

    def normalize_price_ceil(self, price: float) -> float:
        return self.normalize_price_ceil_for_spec(self.worker.symbol_spec, price)

    @staticmethod
    def normalize_price_ceil_for_spec(symbol_spec: SymbolSpec | None, price: float) -> float:
        if symbol_spec is None:
            return price
        tick_size = max(symbol_spec.tick_size, FLOAT_COMPARISON_TOLERANCE)
        tick = Decimal(str(tick_size))
        value = Decimal(str(price))
        steps = (value / tick).to_integral_value(rounding=ROUND_CEILING)
        normalized = steps * tick
        return round(float(normalized), symbol_spec.price_precision)
