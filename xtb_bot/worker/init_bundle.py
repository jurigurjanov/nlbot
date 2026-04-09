from __future__ import annotations

from dataclasses import dataclass
import os
import threading
from typing import Callable

from xtb_bot.client import BaseBrokerClient
from xtb_bot.models import PriceTick, RunMode
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore


@dataclass(frozen=True)
class WorkerInitBundle:
    symbol: str
    mode: RunMode
    strategy_name: str
    strategy_params: dict[str, object]
    broker: BaseBrokerClient
    store: StateStore
    risk: RiskManager
    position_book: PositionBook
    stop_event: threading.Event
    poll_interval_sec: float
    poll_jitter_sec: float
    default_volume: float
    bot_magic_prefix: str
    bot_magic_instance: str
    db_first_reads_enabled: bool
    db_first_tick_max_age_sec: float
    db_first_symbol_spec_max_age_sec: float
    db_first_account_snapshot_max_age_sec: float
    strategy_symbols_map: dict[str, list[str]] | None
    strategy_params_map: dict[str, dict[str, object]] | None
    latest_tick_getter: Callable[[str, float], PriceTick | None] | None
    latest_tick_updater: Callable[[PriceTick], None] | None
    worker_lease_key: str | None
    worker_lease_id: str | None


def build_worker_init_bundle(
    *,
    symbol: str,
    mode: RunMode,
    strategy_name: str,
    strategy_params: dict[str, object],
    broker: BaseBrokerClient,
    store: StateStore,
    risk: RiskManager,
    position_book: PositionBook,
    stop_event: threading.Event,
    poll_interval_sec: float,
    poll_jitter_sec: float,
    default_volume: float,
    bot_magic_prefix: str,
    bot_magic_instance: str,
    db_first_reads_enabled: bool = False,
    db_first_tick_max_age_sec: float | None = None,
    strategy_symbols_map: dict[str, list[str]] | None = None,
    strategy_params_map: dict[str, dict[str, object]] | None = None,
    latest_tick_getter: Callable[[str, float], PriceTick | None] | None = None,
    latest_tick_updater: Callable[[PriceTick], None] | None = None,
    worker_lease_key: str | None = None,
    worker_lease_id: str | None = None,
) -> WorkerInitBundle:
    if db_first_tick_max_age_sec is None:
        effective_db_first_tick_max_age_sec = max(
            0.5,
            float(os.getenv('XTB_DB_FIRST_TICK_MAX_AGE_SEC', '15.0')),
        )
    else:
        effective_db_first_tick_max_age_sec = max(0.5, float(db_first_tick_max_age_sec))

    return WorkerInitBundle(
        symbol=symbol,
        mode=mode,
        strategy_name=strategy_name,
        strategy_params=dict(strategy_params),
        broker=broker,
        store=store,
        risk=risk,
        position_book=position_book,
        stop_event=stop_event,
        poll_interval_sec=float(poll_interval_sec),
        poll_jitter_sec=max(0.0, float(poll_jitter_sec)),
        default_volume=float(default_volume),
        bot_magic_prefix=bot_magic_prefix,
        bot_magic_instance=bot_magic_instance,
        db_first_reads_enabled=bool(db_first_reads_enabled),
        db_first_tick_max_age_sec=effective_db_first_tick_max_age_sec,
        db_first_symbol_spec_max_age_sec=max(
            5.0,
            float(os.getenv('XTB_DB_FIRST_SYMBOL_SPEC_MAX_AGE_SEC', '900.0')),
        ),
        db_first_account_snapshot_max_age_sec=max(
            1.0,
            float(os.getenv('XTB_DB_FIRST_ACCOUNT_SNAPSHOT_MAX_AGE_SEC', '30.0')),
        ),
        strategy_symbols_map=strategy_symbols_map,
        strategy_params_map=strategy_params_map,
        latest_tick_getter=latest_tick_getter if callable(latest_tick_getter) else None,
        latest_tick_updater=latest_tick_updater if callable(latest_tick_updater) else None,
        worker_lease_key=(str(worker_lease_key or '').strip() or None),
        worker_lease_id=(str(worker_lease_id or '').strip() or None),
    )
