from __future__ import annotations

import logging
import threading
import uuid
from typing import TYPE_CHECKING

from xtb_bot.bot._assignment import WorkerAssignment
from xtb_bot.bot.strategy_assignment import BotStrategyAssignmentRuntime
from xtb_bot.models import RunMode
from xtb_bot.worker import SymbolWorker

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotWorkerLifecycleRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot

    @staticmethod
    def _worker_lease_key(symbol: str) -> str:
        return f"worker.lease.{str(symbol).strip().upper()}"

    def _acquire_worker_lease(self, symbol: str, lease_id: str) -> None:
        self._bot.store.set_kv(self._worker_lease_key(symbol), str(lease_id))

    def _revoke_worker_lease(self, symbol: str) -> None:
        try:
            self._bot.store.delete_kv(self._worker_lease_key(symbol))
        except Exception:
            logger.debug("Failed to revoke worker lease for %s", symbol, exc_info=True)

    def _make_worker(self, assignment: WorkerAssignment, stop_event: threading.Event) -> SymbolWorker:
        db_first_tick_max_age_sec: float | None = None
        if self._bot._db_first_enabled():
            db_first_tick_max_age_sec = self._bot._db_first_tick._db_first_tick_max_age_for_workers()
        worker_mode = assignment.mode_override or self._bot.config.mode
        symbol = str(assignment.symbol).strip().upper()
        with self._bot._workers_lock:
            lease_id = self._bot._worker_lease_id_by_symbol.get(symbol)
        strategy_params_map_for_worker = {
            str(name).strip().lower(): dict(params)
            for name, params in self._bot.config.strategy_params_map.items()
            if isinstance(params, dict)
        }
        strategy_params_map_for_worker[str(assignment.strategy_name).strip().lower()] = dict(
            assignment.strategy_params
        )
        return SymbolWorker(
            symbol=symbol,
            mode=worker_mode,
            strategy_name=assignment.strategy_name,
            strategy_params=assignment.strategy_params,
            strategy_symbols_map=self._bot.config.strategy_symbols_map,
            strategy_params_map=strategy_params_map_for_worker,
            broker=self._bot.broker,
            store=self._bot.store,
            risk=self._bot.risk,
            position_book=self._bot.position_book,
            stop_event=stop_event,
            poll_interval_sec=self._bot.config.poll_interval_sec,
            poll_jitter_sec=self._bot.config.worker_poll_jitter_sec,
            default_volume=self._bot.config.default_volume,
            bot_magic_prefix=self._bot.bot_magic_prefix,
            bot_magic_instance=self._bot.bot_magic_instance,
            db_first_reads_enabled=self._bot._db_first_reads_enabled and self._bot.config.broker == "ig",
            db_first_tick_max_age_sec=db_first_tick_max_age_sec,
            latest_tick_getter=self._bot._load_latest_tick_from_memory_cache,
            latest_tick_updater=self._bot._stream_ticks._update_latest_tick_from_broker,
            worker_lease_key=self._worker_lease_key(symbol),
            worker_lease_id=lease_id,
        )

    def _start_worker_for_assignment(self, assignment: WorkerAssignment) -> None:
        with self._bot._worker_lifecycle_lock:
            normalized_symbol = self._bot._worker_key(assignment.symbol)
            with self._bot._workers_lock:
                existing_worker = self._bot.workers.get(normalized_symbol)
                existing_assignment = self._bot._worker_assignments.get(normalized_symbol)
                if (
                    existing_worker is not None
                    and existing_assignment is not None
                    and existing_assignment.runtime_signature() == assignment.runtime_signature()
                    and existing_assignment.signature() != assignment.signature()
                ):
                    self._bot._worker_assignments[normalized_symbol] = assignment
            if existing_worker is not None:
                logger.debug(
                    "Skipping duplicate worker start for %s | strategy=%s source=%s",
                    assignment.symbol,
                    assignment.strategy_name,
                    assignment.source,
                )
                return

            has_restored_position = self._bot.position_book.get(assignment.symbol) is not None
            support_cache_key = (
                *BotStrategyAssignmentRuntime._strategy_cache_identity(
                    assignment.strategy_name,
                    assignment.strategy_params,
                ),
                str(assignment.symbol).strip().upper(),
            )
            supported = self._bot._strategy_support_cache.get(support_cache_key)
            if supported is None:
                supported = self._bot._strategy_supports_symbol(
                    assignment.strategy_name,
                    assignment.strategy_params,
                    assignment.symbol,
                )
            if (not has_restored_position) and (not supported):
                strategy_payload = self._bot._strategy_assignment._assignment_strategy_labels(assignment)
                logger.warning(
                    "Skipping symbol=%s for strategy=%s%s: unsupported symbol",
                    assignment.symbol,
                    strategy_payload[0] or assignment.strategy_name,
                    (f" strategy_base={strategy_payload[1]}" if strategy_payload[1] else ""),
                )
                self._bot.store.record_event(
                    "WARN",
                    assignment.symbol,
                    "Symbol skipped by strategy filter",
                    {
                        **self._bot._strategy_assignment._strategy_event_payload(
                            assignment.strategy_name,
                            assignment.strategy_params,
                        ),
                        "reason": "unsupported_symbol",
                    },
                )
                return

            history_keep_rows = self._bot._register_symbol_history_requirement(
                assignment.symbol,
                assignment.strategy_name,
                assignment.strategy_params,
            )
            lease_id = uuid.uuid4().hex
            self._acquire_worker_lease(normalized_symbol, lease_id)
            stop_event = threading.Event()
            with self._bot._workers_lock:
                self._bot._worker_lease_id_by_symbol[normalized_symbol] = lease_id
            worker = self._bot._make_worker(assignment, stop_event)
            setattr(worker, "price_history_keep_rows", max(100, int(history_keep_rows)))
            with self._bot._workers_lock:
                self._bot._worker_stop_events[normalized_symbol] = stop_event
                self._bot._worker_assignments[normalized_symbol] = assignment
                self._bot.workers[normalized_symbol] = worker
            try:
                worker.start()
            except Exception:
                with self._bot._workers_lock:
                    self._bot.workers.pop(normalized_symbol, None)
                    self._bot._worker_stop_events.pop(normalized_symbol, None)
                    self._bot._worker_assignments.pop(normalized_symbol, None)
                    self._bot._worker_lease_id_by_symbol.pop(normalized_symbol, None)
                self._revoke_worker_lease(normalized_symbol)
                raise
            strategy_name, strategy_base = self._bot._strategy_assignment._assignment_strategy_labels(assignment)
            logger.info(
                "Started worker for %s | strategy=%s%s mode=%s source=%s",
                assignment.symbol,
                strategy_name or assignment.strategy_name,
                (f" strategy_base={strategy_base}" if strategy_base else ""),
                (assignment.mode_override.value if isinstance(assignment.mode_override, RunMode) else self._bot.config.mode.value),
                assignment.source,
            )

    def _stop_worker_for_symbol(self, symbol: str, reason: str) -> None:
        with self._bot._worker_lifecycle_lock:
            normalized_symbol = self._bot._worker_key(symbol)
            with self._bot._workers_lock:
                worker = self._bot.workers.pop(normalized_symbol, None)
                stop_event = self._bot._worker_stop_events.pop(normalized_symbol, None)
                assignment = self._bot._worker_assignments.pop(normalized_symbol, None)
                self._bot._worker_lease_id_by_symbol.pop(normalized_symbol, None)
                self._bot._deferred_switch_signature_by_symbol.pop(normalized_symbol, None)
            self._revoke_worker_lease(normalized_symbol)
            if stop_event is not None:
                stop_event.set()
            if worker is not None:
                worker.join(timeout=5.0)
            logger.info("Stopped worker for %s | reason=%s", normalized_symbol, reason)
            if assignment is not None:
                self._bot.store.record_event(
                    "INFO",
                    normalized_symbol,
                    "Worker stopped",
                    {
                        **self._bot._strategy_assignment._strategy_event_payload(
                            assignment.strategy_name,
                            assignment.strategy_params,
                        ),
                        "reason": reason,
                        "source": assignment.source,
                    },
                )

    def _record_deferred_switch(self, symbol: str, current: WorkerAssignment, desired: WorkerAssignment) -> None:
        normalized_symbol = self._bot._worker_key(symbol)
        current_mode = (
            current.mode_override.value if isinstance(current.mode_override, RunMode) else self._bot.config.mode.value
        )
        desired_mode = (
            desired.mode_override.value if isinstance(desired.mode_override, RunMode) else self._bot.config.mode.value
        )
        marker = (f"{current.strategy_name}:{current_mode}", f"{desired.strategy_name}:{desired_mode}")
        with self._bot._workers_lock:
            if self._bot._deferred_switch_signature_by_symbol.get(normalized_symbol) == marker:
                return
            self._bot._deferred_switch_signature_by_symbol[normalized_symbol] = marker
        logger.info(
            "Deferred strategy switch for %s until open position closes | current=%s(%s) next=%s(%s)",
            normalized_symbol,
            current.strategy_name,
            current_mode,
            desired.strategy_name,
            desired_mode,
        )
        self._bot.store.record_event(
            "INFO",
            normalized_symbol,
            "Strategy switch deferred until position closes",
            {
                "current_strategy": self._bot._strategy_assignment._assignment_strategy_labels(current)[0],
                "current_strategy_base": self._bot._strategy_assignment._assignment_strategy_labels(current)[1],
                "next_strategy": self._bot._strategy_assignment._assignment_strategy_labels(desired)[0],
                "next_strategy_base": self._bot._strategy_assignment._assignment_strategy_labels(desired)[1],
                "current_mode": current_mode,
                "next_mode": desired_mode,
            },
        )
