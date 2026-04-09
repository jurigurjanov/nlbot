from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math

from xtb_bot.models import Position, PriceTick, Side, SymbolSpec


class WorkerValueRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    @staticmethod
    def normalize_currency_code(value: object) -> str | None:
        text = str(value or "").strip().upper().replace(".", "")
        if not text:
            return None
        aliases = {
            "€": "EUR",
            "$": "USD",
            "£": "GBP",
            "#": "GBP",
            "E": "EUR",
        }
        mapped = aliases.get(text, text)
        if len(mapped) == 3 and mapped.isalpha():
            return mapped
        return None

    def broker_account_currency_code(self) -> str | None:
        getter = getattr(self.worker.broker, "get_account_currency_code", None)
        if not callable(getter):
            return None
        try:
            return self.normalize_currency_code(getter())
        except Exception:
            return None

    def currency_conversion_rate(
        self,
        from_currency: str | None,
        to_currency: str | None,
    ) -> tuple[float | None, str | None]:
        worker = self.worker
        source_currency = self.normalize_currency_code(from_currency)
        target_currency = self.normalize_currency_code(to_currency)
        if not source_currency or not target_currency:
            return None, None
        if source_currency == target_currency:
            return 1.0, "identity"

        stream_only_getter = getattr(worker.broker, "get_price_stream_only", None)
        pair_candidates = (
            (f"{source_currency}{target_currency}", False),
            (f"{target_currency}{source_currency}", True),
        )
        for pair_symbol, invert in pair_candidates:
            tick: PriceTick | None = None
            if callable(stream_only_getter):
                try:
                    tick = stream_only_getter(pair_symbol, wait_timeout_sec=0.0)
                except Exception:
                    tick = None
            if tick is None:
                try:
                    tick = worker.broker.get_price(pair_symbol)
                except Exception:
                    tick = None
            if tick is None:
                continue
            mid = (float(tick.bid) + float(tick.ask)) / 2.0
            if mid <= 0.0:
                continue
            if invert:
                return 1.0 / mid, f"fx:{pair_symbol}:inverse_mid"
            return mid, f"fx:{pair_symbol}:mid"
        return None, None

    def normalize_pnl_to_account_currency(
        self,
        pnl_amount: float | None,
        pnl_currency: object | None,
    ) -> tuple[float | None, dict[str, object]]:
        diagnostics: dict[str, object] = {}
        if pnl_amount is None:
            return None, diagnostics
        amount = float(pnl_amount)
        native_currency = self.normalize_currency_code(pnl_currency)
        if native_currency:
            diagnostics["pnl_currency"] = native_currency
        account_currency = self.broker_account_currency_code()
        if account_currency:
            diagnostics["account_currency"] = account_currency
        if not native_currency or not account_currency or native_currency == account_currency:
            diagnostics["pnl_conversion_applied"] = False
            return amount, diagnostics

        conversion_rate, conversion_source = self.currency_conversion_rate(
            native_currency,
            account_currency,
        )
        if conversion_rate is None or conversion_rate <= 0.0:
            diagnostics["pnl_conversion_applied"] = False
            diagnostics["pnl_conversion_missing"] = True
            diagnostics["pnl_native_amount"] = amount
            return None, diagnostics

        converted = amount * conversion_rate
        diagnostics.update(
            {
                "pnl_conversion_applied": True,
                "pnl_native_amount": amount,
                "pnl_conversion_rate": conversion_rate,
                "pnl_conversion_source": conversion_source,
            }
        )
        return converted, diagnostics

    def calculate_pnl(self, position: Position, price: float) -> float:
        worker = self.worker
        symbol_spec: SymbolSpec | None = worker.symbol_spec
        if symbol_spec is not None:
            tick_size = max(symbol_spec.tick_size, FLOAT_COMPARISON_TOLERANCE)
            ticks = (price - position.open_price) / tick_size
            signed_ticks = ticks if position.side == Side.BUY else -ticks
            return signed_ticks * symbol_spec.tick_value * position.volume

        multiplier = worker._contract_multiplier_fallback()
        if position.side == Side.BUY:
            return (price - position.open_price) * position.volume * multiplier
        return (position.open_price - price) * position.volume * multiplier

    @staticmethod
    def safe_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed):
            return None
        return parsed

    @staticmethod
    def coerce_positive_finite(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed) or parsed <= 0.0:
            return None
        return parsed
