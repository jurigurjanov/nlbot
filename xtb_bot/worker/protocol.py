from __future__ import annotations

import collections
import threading
from typing import Protocol, runtime_checkable

from xtb_bot.client import BaseBrokerClient
from xtb_bot.models import AccountSnapshot, Position, RunMode, SymbolSpec
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore


@runtime_checkable
class WorkerProtocol(Protocol):
    """Structural protocol describing the interface worker/* modules expect."""

    # ------------------------------------------------------------------
    # Core dependencies
    # ------------------------------------------------------------------

    @property
    def symbol(self) -> str: ...

    @property
    def strategy_name(self) -> str: ...

    @property
    def mode(self) -> RunMode: ...

    @property
    def broker(self) -> BaseBrokerClient: ...

    @property
    def store(self) -> StateStore: ...

    @property
    def risk(self) -> RiskManager: ...

    @property
    def position_book(self) -> PositionBook: ...

    @property
    def symbol_spec(self) -> SymbolSpec | None: ...

    @property
    def stop_event(self) -> threading.Event: ...

    @property
    def iteration(self) -> int: ...

    # ------------------------------------------------------------------
    # Config scalars
    # ------------------------------------------------------------------

    @property
    def poll_interval_sec(self) -> float: ...

    @property
    def default_volume(self) -> float: ...

    @property
    def min_confidence_for_entry(self) -> float: ...

    @property
    def trade_cooldown_sec(self) -> float: ...

    @property
    def bot_magic_prefix(self) -> str: ...

    @property
    def bot_magic_instance(self) -> str | None: ...

    # ------------------------------------------------------------------
    # Price data
    # ------------------------------------------------------------------

    @property
    def prices(self) -> collections.deque: ...

    @property
    def price_timestamps(self) -> collections.deque: ...

    @property
    def volumes(self) -> collections.deque: ...

    # ------------------------------------------------------------------
    # Key methods
    # ------------------------------------------------------------------

    def _monotonic_now(self) -> float: ...

    def _wall_time_now(self) -> float: ...

    def _runtime_remaining_sec(
        self,
        deadline_ts: float,
        *,
        now_monotonic: float | None = None,
    ) -> float: ...

    def _runtime_age_sec(
        self,
        ts: float,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float: ...

    def _latest_price(self) -> float: ...

    def _safe_float(self, value: object) -> float | None: ...

    def _normalize_price(self, price: float) -> float: ...

    def _calculate_pnl(self, position: Position, mark: float) -> float: ...

    def _save_state(
        self,
        *,
        last_price: float | None = None,
        last_error: str | None = None,
        force: bool = False,
    ) -> None: ...

    def _get_account_snapshot_cached(self) -> AccountSnapshot: ...

    def _pip_size_fallback(self) -> float: ...

    def _strategy_float_param(
        self, params: dict, key: str, default: float
    ) -> float: ...

    def _strategy_bool_param(
        self, params: dict, key: str, default: bool
    ) -> bool: ...

    def _is_index_symbol(self) -> bool: ...

    def _is_commodity_symbol(self) -> bool: ...

    def _is_crypto_symbol(self) -> bool: ...

    def _is_non_fx_cfd_symbol(self) -> bool: ...
