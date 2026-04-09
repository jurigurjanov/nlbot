from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from xtb_bot.bot._assignment import WorkerAssignment
from xtb_bot.bot.strategy_assignment import BotStrategyAssignmentRuntime
from xtb_bot.models import Position, RunMode

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)

_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"


class BotWorkerReconcileRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot

    def _is_schedule_entry_active(self, now_utc: datetime, entry) -> bool:
        local_dt = now_utc.astimezone(self._bot._schedule_timezone)
        minute_of_day = local_dt.hour * 60 + local_dt.minute
        weekday = local_dt.weekday()
        if entry.start_minute < entry.end_minute:
            return weekday in entry.weekdays and entry.start_minute <= minute_of_day < entry.end_minute
        if weekday in entry.weekdays and minute_of_day >= entry.start_minute:
            return True
        previous_weekday = (weekday - 1) % 7
        if previous_weekday in entry.weekdays and minute_of_day < entry.end_minute:
            return True
        return False

    def _schedule_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        current_now = now_utc or self._bot._now_utc()
        if (
            self._bot.config.force_symbols
            or self._bot.config.force_strategy
            or not self._bot.config.strategy_schedule
            or self._bot._schedule_disabled_by_multi_strategy()
        ):
            return self._bot._static_assignments()

        ranked: dict[str, tuple[int, int, WorkerAssignment]] = {}
        for index, entry in enumerate(self._bot.config.strategy_schedule):
            if not self._is_schedule_entry_active(current_now, entry):
                continue
            assignment = WorkerAssignment(
                symbol="",
                strategy_name=entry.strategy,
                strategy_params=dict(entry.strategy_params),
                mode_override=None,
                source="schedule",
                label=entry.label or f"{entry.strategy}@{entry.start_time}-{entry.end_time}",
            )
            for symbol in entry.symbols:
                current = ranked.get(symbol)
                candidate = (
                    int(entry.priority),
                    index,
                    WorkerAssignment(
                        symbol=symbol,
                        strategy_name=assignment.strategy_name,
                        strategy_params=dict(assignment.strategy_params),
                        mode_override=self._bot._strategy_assignment._mode_override_for_symbol(symbol),
                        source=assignment.source,
                        label=assignment.label,
                    ),
                )
                if current is None or candidate[0] > current[0] or (candidate[0] == current[0] and candidate[1] >= current[1]):
                    ranked[symbol] = candidate
        return {symbol: item[2] for symbol, item in ranked.items()}

    def _assignment_for_open_position(
        self,
        position: Position,
        fallback: WorkerAssignment | None = None,
    ) -> WorkerAssignment:
        row = self._bot.store.get_trade_record(position.position_id) or {}
        strategy_name = str(
            row.get("strategy")
            or position.strategy
            or (fallback.strategy_name if fallback else self._bot.config.strategy)
        ).strip().lower()
        if fallback is not None and strategy_name == str(fallback.strategy_name).strip().lower():
            strategy_params = dict(fallback.strategy_params)
        elif strategy_name == _MULTI_STRATEGY_CARRIER_NAME:
            carrier_base_strategy = (
                BotStrategyAssignmentRuntime._normalize_strategy_label(row.get("strategy_entry"))
                or BotStrategyAssignmentRuntime._normalize_strategy_label(position.strategy_entry)
                or self._bot._strategy_assignment._default_strategy_entry_for_assignment(fallback)
                or BotStrategyAssignmentRuntime._normalize_strategy_label(self._bot.config.strategy)
                or self._bot.config.strategy
            )
            carrier_name, carrier_params = self._bot._worker_assignment_payload(
                carrier_base_strategy,
                self._bot._strategy_params_for(carrier_base_strategy),
            )
            strategy_name = carrier_name
            strategy_params = carrier_params
        else:
            strategy_params = self._bot._strategy_params_for(strategy_name)
        mode_override: RunMode | None = None
        row_mode = str(row.get("mode") or "").strip().lower()
        if row_mode in {mode.value for mode in RunMode}:
            resolved_mode = RunMode(row_mode)
            if resolved_mode != self._bot.config.mode:
                mode_override = resolved_mode
        elif fallback is not None and fallback.mode_override is not None:
            mode_override = fallback.mode_override
        else:
            mode_override = self._bot._strategy_assignment._mode_override_for_symbol(position.symbol)
        return WorkerAssignment(
            symbol=position.symbol,
            strategy_name=strategy_name,
            strategy_params=strategy_params,
            mode_override=mode_override,
            source="open_position",
            label=(fallback.label if fallback is not None else None),
        )

    def _static_assignments(self) -> dict[str, WorkerAssignment]:
        source = (
            "forced_symbols"
            if self._bot.config.force_symbols
            else ("forced" if self._bot.config.force_strategy else "static")
        )
        base_strategy_name = str(self._bot.config.strategy).strip().lower()
        base_strategy_params = self._bot._strategy_params_for(self._bot.config.strategy)
        if (
            (self._bot.config.force_symbols or self._bot.config.force_strategy)
            and base_strategy_name != _MULTI_STRATEGY_CARRIER_NAME
            and "multi_strategy_enabled" not in base_strategy_params
        ):
            assignment_strategy_name = base_strategy_name
            assignment_strategy_params = dict(base_strategy_params)
        else:
            assignment_strategy_name, assignment_strategy_params = self._bot._worker_assignment_payload(
                self._bot.config.strategy,
                base_strategy_params,
            )
        if (
            source == "static"
            and self._bot.config.strategy_schedule
            and self._bot._schedule_disabled_by_multi_strategy()
        ):
            source = "multi_static"
        return {
            symbol: WorkerAssignment(
                symbol=symbol,
                strategy_name=assignment_strategy_name,
                strategy_params=dict(assignment_strategy_params),
                mode_override=self._bot._strategy_assignment._mode_override_for_symbol(symbol),
                source=source,
            )
            for symbol in self._bot.config.symbols
        }

    def _target_worker_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        desired = {
            self._bot._worker_key(symbol): assignment
            for symbol, assignment in self._bot._schedule_assignments(now_utc).items()
        }
        for position in self._bot.position_book.all_open():
            symbol_key = self._bot._worker_key(position.symbol)
            desired[symbol_key] = self._bot._assignment_for_open_position(position, desired.get(symbol_key))
        return desired

    def _reconcile_workers(self, now_utc: datetime | None = None) -> None:
        with self._bot._worker_lifecycle_lock:
            desired = self._bot._target_worker_assignments(now_utc)

            with self._bot._workers_lock:
                worker_items = list(self._bot.workers.items())
            for symbol, worker in worker_items:
                if worker is None:
                    continue
                if worker.is_alive():
                    continue
                with self._bot._workers_lock:
                    self._bot.workers.pop(symbol, None)
                    self._bot._worker_stop_events.pop(symbol, None)
                    current_assignment = self._bot._worker_assignments.pop(symbol, None)
                    self._bot._worker_lease_id_by_symbol.pop(self._bot._worker_key(symbol), None)
                    self._bot._deferred_switch_signature_by_symbol.pop(symbol, None)
                active_position = self._bot.position_book.get(symbol)
                restart_assignment = desired.get(symbol)
                if active_position is not None:
                    restart_assignment = self._bot._assignment_for_open_position(active_position, restart_assignment)
                if restart_assignment is not None:
                    logger.warning(
                        "Worker for %s stopped unexpectedly, restarting with strategy=%s",
                        symbol,
                        restart_assignment.strategy_name,
                    )
                    self._bot.store.record_event(
                        "WARN",
                        symbol,
                        "Worker restarted",
                        {
                            **self._bot._strategy_assignment._strategy_event_payload(
                                restart_assignment.strategy_name,
                                restart_assignment.strategy_params,
                            ),
                            "previous_strategy": self._bot._strategy_assignment._assignment_strategy_labels(current_assignment)[0],
                            "previous_strategy_base": self._bot._strategy_assignment._assignment_strategy_labels(current_assignment)[1],
                        },
                    )
                    self._bot._start_worker_for_assignment(restart_assignment)

            with self._bot._workers_lock:
                assignment_items = list(self._bot._worker_assignments.items())
            for symbol, assignment in assignment_items:
                desired_assignment = desired.get(symbol)
                active_position = self._bot.position_book.get(symbol)
                if desired_assignment is None:
                    if active_position is None:
                        self._bot._stop_worker_for_symbol(symbol, "schedule_inactive")
                    continue

                if assignment.runtime_signature() == desired_assignment.runtime_signature():
                    # Do not restart workers when only metadata/source changed.
                    if assignment.signature() != desired_assignment.signature():
                        with self._bot._workers_lock:
                            self._bot._worker_assignments[symbol] = desired_assignment
                    with self._bot._workers_lock:
                        self._bot._deferred_switch_signature_by_symbol.pop(symbol, None)
                    continue

                if active_position is not None:
                    self._bot._worker_lifecycle._record_deferred_switch(symbol, assignment, desired_assignment)
                    continue

                current_mode = (
                    assignment.mode_override.value if isinstance(assignment.mode_override, RunMode) else self._bot.config.mode.value
                )
                desired_mode = (
                    desired_assignment.mode_override.value
                    if isinstance(desired_assignment.mode_override, RunMode)
                    else self._bot.config.mode.value
                )
                self._bot._stop_worker_for_symbol(
                    symbol,
                    (
                        "assignment_change:"
                        f"{assignment.strategy_name}@{current_mode}"
                        "->"
                        f"{desired_assignment.strategy_name}@{desired_mode}"
                    ),
                )
                self._bot._start_worker_for_assignment(desired_assignment)

            for symbol, assignment in desired.items():
                with self._bot._workers_lock:
                    exists = symbol in self._bot.workers
                if exists:
                    continue
                self._bot._start_worker_for_assignment(assignment)

    def _start_workers(self) -> None:
        self._bot._reconcile_workers()
