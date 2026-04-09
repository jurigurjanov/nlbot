from __future__ import annotations

import math
from typing import TYPE_CHECKING

from xtb_bot.models import PriceTick, Position
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot


class BotBrokerStateRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot

    @staticmethod
    def finite_float_or_none(raw: object) -> float | None:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value):
            return None
        return value

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

    def broker_public_api_backoff_remaining_sec(self) -> float:
        getter = getattr(self._bot.broker, "get_public_api_backoff_remaining_sec", None)
        if not callable(getter):
            return 0.0
        try:
            remaining = float(getter())
        except Exception:
            return 0.0
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def broker_market_data_wait_remaining_sec(self) -> float:
        getter = getattr(self._bot.broker, "get_market_data_wait_remaining_sec", None)
        if not callable(getter):
            return 0.0
        try:
            remaining = float(getter())
        except Exception:
            return 0.0
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def broker_account_currency_code(self) -> str | None:
        getter = getattr(self._bot.broker, "get_account_currency_code", None)
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
        source_currency = self.normalize_currency_code(from_currency)
        target_currency = self.normalize_currency_code(to_currency)
        if not source_currency or not target_currency:
            return None, None
        if source_currency == target_currency:
            return 1.0, "identity"

        stream_only_getter = getattr(self._bot.broker, "get_price_stream_only", None)
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
                    tick = self._bot.broker.get_price(pair_symbol)
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
            return amount, diagnostics

        diagnostics["pnl_conversion_applied"] = True
        diagnostics["pnl_conversion_rate"] = conversion_rate
        diagnostics["pnl_conversion_source"] = conversion_source
        diagnostics["pnl_native_amount"] = amount
        return amount * conversion_rate, diagnostics

    def estimate_position_pnl_from_close_price(
        self,
        position: Position,
        close_price: float | None,
        pnl_currency: object | None = None,
    ) -> tuple[float | None, dict[str, object]]:
        normalized_close_price = self.finite_float_or_none(close_price)
        if normalized_close_price is None or normalized_close_price <= 0.0:
            return None, {}
        spec = self._bot.store.load_broker_symbol_spec(
            position.symbol,
            max_age_sec=0.0,
            epic=str(position.epic or "").strip().upper() or None,
            epic_variant=str(position.epic_variant or "").strip().lower() or None,
        )
        if spec is None:
            return None, {}
        tick_size = max(float(spec.tick_size), FLOAT_COMPARISON_TOLERANCE)
        tick_value = float(spec.tick_value)
        calibration = self._bot.store.load_broker_tick_value_calibration(position.symbol)
        if isinstance(calibration, dict):
            calibrated_tick_value = self.finite_float_or_none(calibration.get("tick_value"))
            calibrated_tick_size = self.finite_float_or_none(calibration.get("tick_size"))
            if (
                calibrated_tick_value is not None
                and calibrated_tick_value > 0.0
                and calibrated_tick_size is not None
                and math.isclose(calibrated_tick_size, tick_size, rel_tol=0.0, abs_tol=max(FLOAT_COMPARISON_TOLERANCE, tick_size * FLOAT_COMPARISON_TOLERANCE))
            ):
                tick_value = calibrated_tick_value
        if tick_value <= 0.0:
            return None, {}
        ticks = (normalized_close_price - float(position.open_price)) / tick_size
        signed_ticks = ticks if position.side.value == "buy" else -ticks
        pnl_native = signed_ticks * tick_value * float(position.volume)
        return self.normalize_pnl_to_account_currency(pnl_native, pnl_currency)
