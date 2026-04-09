from __future__ import annotations

import sys
import threading
import time
from typing import Callable

from xtb_bot.client import BaseBrokerClient
from xtb_bot.models import PendingOpen, Position, PriceTick, RunMode
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.strategies import create_strategy
from xtb_bot.strategies.base import Strategy
from xtb_bot.worker.method_bindings import bound_worker_method_names, resolve_bound_worker_method
from xtb_bot.worker.state_proxies import bind_symbol_worker_state_proxies
from xtb_bot.worker.bootstrap import WorkerBootstrapRuntime
from xtb_bot.worker.init_bundle import build_worker_init_bundle


_MULTI_STRATEGY_CARRIER_NAME = 'multi_strategy'
_MULTI_STRATEGY_BASE_COMPONENT_PARAM = '_multi_strategy_base_component'
_ORIGINAL_TIME_TIME = time.time
_ORIGINAL_TIME_MONOTONIC = time.monotonic


class SymbolWorker(threading.Thread):
    _WALL_CLOCK_EPOCH_THRESHOLD_SEC = 1_000_000_000.0

    def __init__(
        self,
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
    ) -> None:
        super().__init__(name=f'worker-{symbol}', daemon=True)
        bundle = build_worker_init_bundle(
            symbol=symbol,
            mode=mode,
            strategy_name=strategy_name,
            strategy_params=strategy_params,
            broker=broker,
            store=store,
            risk=risk,
            position_book=position_book,
            stop_event=stop_event,
            poll_interval_sec=poll_interval_sec,
            poll_jitter_sec=poll_jitter_sec,
            default_volume=default_volume,
            bot_magic_prefix=bot_magic_prefix,
            bot_magic_instance=bot_magic_instance,
            db_first_reads_enabled=db_first_reads_enabled,
            db_first_tick_max_age_sec=db_first_tick_max_age_sec,
            strategy_symbols_map=strategy_symbols_map,
            strategy_params_map=strategy_params_map,
            latest_tick_getter=latest_tick_getter,
            latest_tick_updater=latest_tick_updater,
            worker_lease_key=worker_lease_key,
            worker_lease_id=worker_lease_id,
        )
        WorkerBootstrapRuntime(self).bootstrap_symbol_worker(
            bundle=bundle,
            multi_strategy_carrier_name=_MULTI_STRATEGY_CARRIER_NAME,
            multi_strategy_base_component_param=_MULTI_STRATEGY_BASE_COMPONENT_PARAM,
        )

    def _now_ts(self) -> float:
        return time.time()

    def _time_now(self) -> float:
        return time.time()

    def _wall_time_now(self) -> float:
        return time.time()

    def _monotonic_now(self) -> float:
        # Keep production runtime logic on true monotonic time, but let tests that
        # patch only wall-clock time continue to drive interval-based code paths
        # deterministically without having to patch two clocks everywhere.
        if time.monotonic is not _ORIGINAL_TIME_MONOTONIC:
            return time.monotonic()
        if time.time is not _ORIGINAL_TIME_TIME:
            return time.time()
        return time.monotonic()

    @classmethod
    def _looks_like_wall_clock_ts(cls, value: object) -> bool:
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return False
        return ts >= cls._WALL_CLOCK_EPOCH_THRESHOLD_SEC

    def _runtime_age_sec(
        self,
        ts: float | int | None,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float | None:
        try:
            parsed_ts = float(ts)
        except (TypeError, ValueError):
            return None
        if parsed_ts <= 0.0:
            return None
        if self._looks_like_wall_clock_ts(parsed_ts):
            current_wall = self._wall_time_now() if now_wall is None else float(now_wall)
            return max(0.0, current_wall - parsed_ts)
        current_monotonic = self._monotonic_now() if now_monotonic is None else float(now_monotonic)
        return max(0.0, current_monotonic - parsed_ts)

    def _runtime_remaining_sec(
        self,
        deadline_ts: float | int | None,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float:
        age_sec = self._runtime_age_sec(
            deadline_ts,
            now_monotonic=now_monotonic,
            now_wall=now_wall,
        )
        if age_sec is None:
            return 0.0
        try:
            parsed_deadline = float(deadline_ts)
        except (TypeError, ValueError):
            return 0.0
        if self._looks_like_wall_clock_ts(parsed_deadline):
            current_wall = self._wall_time_now() if now_wall is None else float(now_wall)
            return max(0.0, parsed_deadline - current_wall)
        current_monotonic = self._monotonic_now() if now_monotonic is None else float(now_monotonic)
        return max(0.0, parsed_deadline - current_monotonic)

    def _wall_time_for_runtime_ts(
        self,
        runtime_ts: float | int | None,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float | None:
        try:
            parsed_ts = float(runtime_ts)
        except (TypeError, ValueError):
            return None
        if parsed_ts <= 0.0:
            return None
        if self._looks_like_wall_clock_ts(parsed_ts):
            return parsed_ts
        current_monotonic = self._monotonic_now() if now_monotonic is None else float(now_monotonic)
        current_wall = self._wall_time_now() if now_wall is None else float(now_wall)
        return current_wall + max(0.0, parsed_ts - current_monotonic)

    def _mark_pending_open_created_monotonic(
        self,
        pending_id: str,
        *,
        created_monotonic_ts: float | None = None,
    ) -> None:
        normalized_pending_id = str(pending_id or "").strip()
        if not normalized_pending_id:
            return
        self._order_manager.state.pending_open_created_monotonic_by_id[normalized_pending_id] = (
            self._monotonic_now() if created_monotonic_ts is None else float(created_monotonic_ts)
        )

    def _clear_pending_open_created_monotonic(self, pending_id: str | None) -> None:
        normalized_pending_id = str(pending_id or "").strip()
        if not normalized_pending_id:
            return
        self._order_manager.state.pending_open_created_monotonic_by_id.pop(normalized_pending_id, None)

    def _pending_open_age_sec(
        self,
        pending: PendingOpen,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float | None:
        pending_id = str(getattr(pending, "pending_id", "") or "").strip()
        anchor = self._order_manager.state.pending_open_created_monotonic_by_id.get(pending_id)
        age_sec = self._runtime_age_sec(
            anchor,
            now_monotonic=now_monotonic,
            now_wall=now_wall,
        )
        if age_sec is not None:
            return age_sec
        created_at = getattr(pending, "created_at", None)
        return self._runtime_age_sec(
            created_at,
            now_monotonic=now_monotonic,
            now_wall=now_wall,
        )

    def _mark_position_opened_monotonic(
        self,
        position_id: str,
        *,
        opened_monotonic_ts: float | None = None,
    ) -> None:
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return
        self._order_manager.state.position_opened_monotonic_by_id[normalized_position_id] = (
            self._monotonic_now() if opened_monotonic_ts is None else float(opened_monotonic_ts)
        )

    def _clear_position_opened_monotonic(self, position_id: str | None) -> None:
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return
        self._order_manager.state.position_opened_monotonic_by_id.pop(normalized_position_id, None)

    def _position_age_sec(
        self,
        position: Position,
        *,
        now_monotonic: float | None = None,
        now_wall: float | None = None,
    ) -> float | None:
        position_id = str(getattr(position, "position_id", "") or "").strip()
        anchor = self._order_manager.state.position_opened_monotonic_by_id.get(position_id)
        age_sec = self._runtime_age_sec(
            anchor,
            now_monotonic=now_monotonic,
            now_wall=now_wall,
        )
        if age_sec is not None:
            return age_sec
        opened_at = getattr(position, "opened_at", None)
        return self._runtime_age_sec(
            opened_at,
            now_monotonic=now_monotonic,
            now_wall=now_wall,
        )

    def _update_last_price_freshness(
        self,
        *,
        freshness_ts: float | None,
        freshness_monotonic_ts: float | None = None,
    ) -> None:
        try:
            wall_ts = float(freshness_ts) if freshness_ts is not None else 0.0
        except (TypeError, ValueError):
            wall_ts = 0.0
        self._last_price_freshness_ts = max(0.0, wall_ts)

        monotonic_ts: float
        if freshness_monotonic_ts is None:
            monotonic_ts = self._monotonic_now()
        else:
            try:
                monotonic_ts = float(freshness_monotonic_ts)
            except (TypeError, ValueError):
                monotonic_ts = 0.0
        self._last_price_freshness_monotonic_ts = max(0.0, monotonic_ts)

    @staticmethod
    def _resolve_create_strategy_factory():
        package = sys.modules.get('xtb_bot.worker')
        factory = getattr(package, 'create_strategy', None) if package is not None else None
        return factory if callable(factory) else create_strategy

    def _create_strategy_instance(self, name: str, params: dict[str, object]) -> Strategy:
        return self._resolve_create_strategy_factory()(name, params)

    def _build_magic_comment(self) -> tuple[str, str]:
        return self._order_manager.build_magic_comment(
            bot_magic_prefix=self.bot_magic_prefix,
            bot_magic_instance=self.bot_magic_instance,
        )

    def __getattr__(self, name: str):
        return resolve_bound_worker_method(self, name)

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(bound_worker_method_names()))

    def run(self) -> None:
        self._orchestrator.run_loop()


bind_symbol_worker_state_proxies(SymbolWorker)
