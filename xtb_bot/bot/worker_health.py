from __future__ import annotations

import _thread as _thread
import faulthandler
import logging
import sys
import threading
import time
from typing import TYPE_CHECKING, Callable

from xtb_bot.worker import SymbolWorker

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotWorkerHealthRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot

    def _runtime_monitor_interval_sec(self) -> float:
        return max(
            1.0,
            min(
                float(self._bot.config.poll_interval_sec),
                float(self._bot.config.passive_history_poll_interval_sec),
            ),
        )

    def _runtime_monitor_stale_after_sec(self) -> float:
        return max(180.0, self._runtime_monitor_interval_sec() * 12.0)

    def _worker_stale_heartbeat_after_sec(self, worker: SymbolWorker) -> float:
        poll_interval_sec = max(1.0, float(getattr(worker, "poll_interval_sec", self._bot.config.poll_interval_sec)))
        flush_interval_sec = max(
            0.5,
            float(getattr(worker, "worker_state_flush_interval_sec", self._bot.config.risk.worker_state_flush_interval_sec)),
        )
        return max(30.0, max(poll_interval_sec, flush_interval_sec, self._bot._worker_health_interval_sec) * 6.0)

    @staticmethod
    def _worker_watchdog_blocking_operation_payload(
        worker: SymbolWorker,
        *,
        now_wall: float,
    ) -> dict[str, object] | None:
        getter = getattr(worker, "watchdog_blocking_operation_status", None)
        raw_status = getter() if callable(getter) else None
        if not isinstance(raw_status, dict):
            return None
        operation = str(raw_status.get("operation") or "").strip()
        started_at = float(raw_status.get("started_at") or 0.0)
        started_at_monotonic = float(raw_status.get("started_at_monotonic") or 0.0)
        grace_sec = max(0.0, float(raw_status.get("grace_sec") or 0.0))
        if not operation or grace_sec <= 0.0:
            return None
        if started_at_monotonic > 0.0:
            age_sec = max(0.0, time.monotonic() - started_at_monotonic)
        else:
            if started_at <= 0.0:
                return None
            age_sec = max(0.0, now_wall - started_at)
        remaining_sec = max(0.0, grace_sec - age_sec)
        return {
            "blocking_operation": operation,
            "blocking_operation_age_sec": round(age_sec, 3),
            "blocking_operation_grace_sec": round(grace_sec, 3),
            "blocking_operation_remaining_sec": round(remaining_sec, 3),
        }

    def _record_runtime_monitor_failure(self, task_name: str, exc: Exception) -> None:
        now_monotonic = time.monotonic()
        signature = (str(task_name), exc.__class__.__name__, str(exc))
        should_log = (
            signature != self._bot._runtime_monitor_last_failure_signature
            or (now_monotonic - self._bot._runtime_monitor_last_failure_log_monotonic) >= 30.0
        )
        if should_log:
            logger.exception("Runtime monitor task failed | task=%s error=%s", task_name, exc)
            try:
                self._bot.store.record_event(
                    "ERROR",
                    None,
                    "Runtime monitor task failed",
                    {
                        "task": str(task_name),
                        "error": str(exc),
                        "error_type": exc.__class__.__name__,
                    },
                )
            except Exception:
                logger.debug("Failed to persist runtime monitor failure event", exc_info=True)
            self._bot._runtime_monitor_last_failure_log_monotonic = now_monotonic
            self._bot._runtime_monitor_last_failure_signature = signature

    def _run_monitor_task(self, task_name: str, operation: Callable[[], None]) -> None:
        self._bot._runtime_monitor_active_task_name = str(task_name)
        self._bot._runtime_monitor_last_progress_monotonic = time.monotonic()
        try:
            operation()
        except Exception as exc:
            self._record_runtime_monitor_failure(task_name, exc)
        finally:
            self._bot._runtime_monitor_last_progress_monotonic = time.monotonic()
            self._bot._runtime_monitor_active_task_name = None

    def _pulse_runtime_monitor_progress(self) -> None:
        self._bot._runtime_monitor_last_progress_monotonic = time.monotonic()

    def _restart_worker_for_symbol(self, symbol: str, *, reason: str) -> bool:
        with self._bot._worker_lifecycle_lock:
            normalized_symbol = str(symbol).strip().upper()
            now_monotonic = time.monotonic()
            last_restart = self._bot._worker_last_restart_monotonic_by_symbol.get(normalized_symbol, 0.0)
            if (
                last_restart > 0.0
                and (now_monotonic - last_restart) < self._bot._worker_stale_heartbeat_restart_cooldown_sec
            ):
                return False

            desired = self._bot._target_worker_assignments()
            restart_assignment = desired.get(normalized_symbol)
            active_position = self._bot.position_book.get(normalized_symbol)
            if active_position is not None:
                restart_assignment = self._bot._assignment_for_open_position(active_position, restart_assignment)

            self._bot._worker_last_restart_monotonic_by_symbol[normalized_symbol] = now_monotonic
            self._bot._stop_worker_for_symbol(normalized_symbol, reason)
            if restart_assignment is None:
                return False
            self._bot._start_worker_for_assignment(restart_assignment)
            return True

    def _worker_health_check_once(self) -> list[str]:
        with self._bot._workers_lock:
            dead_symbols = sorted(
                {
                    str(symbol).strip().upper()
                    for symbol, worker in self._bot.workers.items()
                    if worker is not None and (not worker.is_alive())
                }
            )
        if not dead_symbols:
            return []
        logger.warning(
            "Worker health watchdog detected stopped workers | count=%d symbols=%s",
            len(dead_symbols),
            ",".join(dead_symbols),
        )
        self._bot.store.record_event(
            "WARN",
            None,
            "Worker health watchdog detected stopped workers",
            {
                "count": len(dead_symbols),
                "symbols": dead_symbols,
            },
        )
        self._bot._reconcile_workers()
        return dead_symbols

    def _worker_stale_heartbeat_check_once(self) -> list[str]:
        now_wall = time.time()
        now_monotonic = time.monotonic()
        stale_symbols: list[str] = []
        with self._bot._workers_lock:
            worker_items = list(self._bot.workers.items())
        for symbol, worker in worker_items:
            if worker is None or (not worker.is_alive()):
                continue
            heartbeat_age_sec = self._bot._worker_last_saved_state_age_sec(
                worker,
                now_wall=now_wall,
                now_monotonic=now_monotonic,
            )
            if heartbeat_age_sec is None:
                continue
            stale_after_sec = self._bot._worker_stale_heartbeat_after_sec(worker)
            if heartbeat_age_sec < stale_after_sec:
                continue
            blocking_operation = self._bot._worker_watchdog_blocking_operation_payload(
                worker,
                now_wall=now_wall,
            )
            if (
                blocking_operation is not None
                and float(blocking_operation.get("blocking_operation_remaining_sec") or 0.0) > 0.0
            ):
                continue
            normalized_symbol = str(symbol).strip().upper()
            stale_symbols.append(normalized_symbol)
            last_signature = getattr(worker, "_last_saved_worker_state_signature", None)
            last_error = None
            if isinstance(last_signature, tuple) and len(last_signature) >= 2:
                raw_last_error = last_signature[1]
                if raw_last_error not in (None, ""):
                    last_error = str(raw_last_error)
            with self._bot._workers_lock:
                assignment = self._bot._worker_assignments.get(normalized_symbol)
            payload = {
                "symbol": normalized_symbol,
                "heartbeat_age_sec": round(heartbeat_age_sec, 3),
                "stale_after_sec": round(stale_after_sec, 3),
                "thread_name": getattr(worker, "name", None),
                "last_error": last_error,
            }
            if blocking_operation is not None:
                payload.update(blocking_operation)
            payload.update(
                self._bot._strategy_event_payload(
                    getattr(worker, "strategy_name", None),
                    (
                        getattr(assignment, "strategy_params", None)
                        if assignment is not None
                        else None
                    ),
                    strategy_entry_hint=getattr(worker, "_multi_strategy_base_component_name", None),
                )
            )
            logger.error(
                "Worker health watchdog detected stale heartbeat | symbol=%s age=%.1fs stale_after=%.1fs",
                normalized_symbol,
                heartbeat_age_sec,
                stale_after_sec,
            )
            self._bot.store.record_event(
                "ERROR",
                normalized_symbol,
                "Worker health watchdog detected stale heartbeat",
                payload,
            )
            self._bot._restart_worker_for_symbol(normalized_symbol, reason="watchdog_stale_heartbeat")
        return stale_symbols

    def _dump_runtime_thread_traces(self, *, reason: str) -> None:
        try:
            logger.error("Dumping Python thread traces | reason=%s", reason)
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        except Exception:
            logger.exception("Failed to dump Python thread traces | reason=%s", reason)

    def _abort_process_for_watchdog(self, *, reason: str) -> None:
        try:
            self._bot.store.record_event(
                "ERROR",
                None,
                "Runtime watchdog aborting process",
                {"reason": str(reason)},
            )
        except Exception:
            logger.debug("Failed to persist watchdog abort event", exc_info=True)
        self._bot.request_graceful_stop(
            reason=f"watchdog_abort:{str(reason).strip() or 'unknown'}",
            source="runtime_watchdog",
        )
        self._bot._worker_health_stop_event.set()
        try:
            self._bot._drain_runtime_for_watchdog_abort()
        except Exception:
            logger.debug("Failed to drain runtime before watchdog abort", exc_info=True)
        logger.critical("Runtime watchdog aborting process | reason=%s", reason)
        try:
            _thread.interrupt_main()
        except Exception:
            logger.debug("Failed to interrupt main thread during watchdog abort", exc_info=True)
        raise SystemExit(1)

    def _monitor_loop_health_check_once(self) -> bool:
        if not self._bot._runtime_monitor_watchdog_enabled:
            return False
        now_monotonic = time.monotonic()
        stale_after_sec = self._bot._runtime_monitor_stale_after_sec()
        last_progress_monotonic = max(
            self._bot._runtime_monitor_last_started_monotonic,
            self._bot._runtime_monitor_last_completed_monotonic,
            self._bot._runtime_monitor_last_progress_monotonic,
        )
        age_sec = max(0.0, now_monotonic - last_progress_monotonic)
        if age_sec < stale_after_sec:
            self._bot._runtime_monitor_stall_first_detected_monotonic = 0.0
            return False

        if self._bot._runtime_monitor_stall_first_detected_monotonic <= 0.0:
            self._bot._runtime_monitor_stall_first_detected_monotonic = now_monotonic
            payload = {
                "stale_after_sec": round(stale_after_sec, 3),
                "age_sec": round(age_sec, 3),
                "last_started_age_sec": round(
                    max(0.0, now_monotonic - self._bot._runtime_monitor_last_started_monotonic),
                    3,
                ),
                "last_completed_age_sec": round(
                    max(0.0, now_monotonic - self._bot._runtime_monitor_last_completed_monotonic),
                    3,
                ),
                "last_progress_age_sec": round(
                    max(0.0, now_monotonic - self._bot._runtime_monitor_last_progress_monotonic),
                    3,
                ),
                "active_task": self._bot._runtime_monitor_active_task_name,
            }
            logger.error(
                "Runtime monitor watchdog detected stale main loop | age=%.1fs stale_after=%.1fs",
                age_sec,
                stale_after_sec,
            )
            self._bot.store.record_event(
                "ERROR",
                None,
                "Runtime monitor watchdog detected stale main loop",
                payload,
            )
            self._bot._dump_runtime_thread_traces(reason="runtime_monitor_stale")
            return True

        abort_after_sec = max(30.0, self._bot._worker_health_interval_sec * 3.0)
        if (now_monotonic - self._bot._runtime_monitor_stall_first_detected_monotonic) >= abort_after_sec:
            self._bot._abort_process_for_watchdog(reason="runtime_monitor_stale")
        return True

    def _worker_health_watchdog_loop(self) -> None:
        interval_sec = max(1.0, float(self._bot._worker_health_interval_sec))
        while not self._bot.stop_event.is_set() and not self._bot._worker_health_stop_event.is_set():
            try:
                self._bot._worker_health_check_once()
                self._bot._worker_stale_heartbeat_check_once()
                self._bot._monitor_loop_health_check_once()
            except Exception as exc:
                logger.warning("Worker health watchdog cycle failed: %s", exc)
            self._bot._worker_health_stop_event.wait(timeout=interval_sec)

    def _start_worker_health_thread(self) -> None:
        thread = self._bot._worker_health_thread
        if thread is not None and thread.is_alive():
            return
        self._bot._worker_health_stop_event.clear()
        thread = threading.Thread(
            target=self._bot._worker_health_watchdog_loop,
            name="worker-health-watchdog",
            daemon=True,
        )
        self._bot._worker_health_thread = thread
        thread.start()

    def _stop_worker_health_thread(self) -> None:
        self._bot._worker_health_stop_event.set()
        thread = self._bot._worker_health_thread
        self._bot._worker_health_thread = None
        if thread is None:
            return
        if thread.is_alive():
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning("Worker health watchdog did not stop in time")
