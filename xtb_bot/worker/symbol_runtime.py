from __future__ import annotations

from typing import Any

from xtb_bot.models import Position, SymbolSpec
from xtb_bot.symbols import is_index_symbol as _shared_is_index_symbol


class WorkerSymbolRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    def is_index_symbol(self) -> bool:
        return _shared_is_index_symbol(self.worker.symbol)

    def epic_text(self) -> str:
        symbol_spec = self.worker.symbol_spec
        if symbol_spec is None or not isinstance(symbol_spec.metadata, dict):
            return ""
        return str(symbol_spec.metadata.get("epic") or "").strip().upper()

    @staticmethod
    def spec_epic(spec: SymbolSpec | None) -> str:
        if spec is None or not isinstance(spec.metadata, dict):
            return ""
        return str(spec.metadata.get("epic") or "").strip().upper()

    @staticmethod
    def epic_variant_text(epic: str) -> str:
        upper_epic = str(epic or "").strip().upper()
        if not upper_epic:
            return ""
        if ".CASH." in upper_epic:
            return "cash"
        if ".DAILY." in upper_epic:
            return "daily"
        if ".IFS." in upper_epic:
            return "ifs"
        if ".IFD." in upper_epic:
            return "ifd"
        if ".CFD." in upper_epic:
            return "cfd"
        return "other"

    @classmethod
    def spec_epic_variant(cls, spec: SymbolSpec | None) -> str:
        return cls.epic_variant_text(cls.spec_epic(spec))

    def position_epic(self, position: Position | None) -> str:
        if position is None:
            return ""
        return str(position.epic or "").strip().upper()

    def position_epic_variant(self, position: Position | None) -> str:
        if position is None:
            return ""
        variant = str(position.epic_variant or "").strip().lower()
        if variant:
            return variant
        return self.epic_variant_text(self.position_epic(position))

    def is_crypto_symbol(self) -> bool:
        upper = self.worker.symbol.upper()
        if upper in {"BTC", "ETH", "LTC", "BCH", "DOGE", "XRP", "SOL"}:
            return True
        epic = self.epic_text()
        return any(
            token in epic
            for token in ("BITCOIN", "ETHER", "ETHEREUM", "CRYPTO", "DOGE", "LITECOIN", "RIPPLE", "SOLANA")
        )

    def is_commodity_symbol(self) -> bool:
        upper = self.worker.symbol.upper()
        if upper.startswith(("XAU", "XAG")):
            return True
        if upper in {
            "WTI",
            "BRENT",
            "XAUUSD",
            "XAGUSD",
            "GOLD",
            "SILVER",
            "NGAS",
            "NATGAS",
            "COPPER",
        }:
            return True
        return False

    def is_fx_symbol(self) -> bool:
        upper = self.worker.symbol.upper()
        if self.is_index_symbol() or self.is_commodity_symbol() or self.is_crypto_symbol():
            return False
        return len(upper) == 6 and upper.isalpha()

    def is_non_fx_cfd_symbol(self) -> bool:
        if self.is_fx_symbol():
            return False
        epic = self.epic_text()
        if epic.startswith(("IX.", "CC.", "CS.", "UA.", "SA.")):
            return True
        upper = self.worker.symbol.upper()
        if self.is_index_symbol() or self.is_commodity_symbol() or self.is_crypto_symbol():
            return True
        if upper.startswith(("AAPL", "MSFT")):
            return True
        return False

    def multi_strategy_symbol_asset_class(self) -> str:
        if self.is_index_symbol():
            return "index"
        if self.is_commodity_symbol():
            return "commodity"
        if self.is_crypto_symbol():
            return "crypto"
        if self.is_fx_symbol():
            return "fx"
        return "other"

    def trailing_breakeven_offset_for_symbol_pips(self) -> float:
        worker = self.worker
        if self.is_index_symbol():
            return worker.trailing_breakeven_offset_pips_index
        if self.is_commodity_symbol():
            return worker.trailing_breakeven_offset_pips_commodity
        return worker.trailing_breakeven_offset_pips_fx

    def session_exit_reason(self, now_ts: float) -> str | None:
        worker = self.worker
        if not self.is_index_symbol():
            return None
        close_ts = worker.broker.get_session_close_utc(worker.symbol, now_ts)
        if close_ts is None:
            return None
        seconds_left = int(close_ts - now_ts)
        if seconds_left <= worker.session_close_buffer_sec:
            return f"session_end_buffer:{seconds_left}s"
        return None
