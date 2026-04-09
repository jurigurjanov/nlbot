from __future__ import annotations

from dataclasses import dataclass
import logging

from xtb_bot.client import BrokerError
from xtb_bot.models import PriceTick, StreamHealthStatus


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkerIterationState:
    last_error: str | None = None
    tick: PriceTick | None = None
    stream_health: StreamHealthStatus | None = None
    sleep_override_sec: float | None = None


class WorkerOrchestrator:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def run_loop(self) -> None:
        worker = self.worker
        if worker.poll_jitter_sec > 0:
            startup_delay = worker._rng.uniform(0.0, worker.poll_jitter_sec)
            worker.stop_event.wait(startup_delay)

        while not worker.stop_event.is_set():
            worker.iteration += 1
            state = WorkerIterationState()

            precheck = self._pre_iteration_checks(state)
            if precheck == "break":
                break
            if precheck == "continue":
                continue

            try:
                if self._process_iteration(state):
                    continue
            except BrokerError as exc:
                state.last_error, state.sleep_override_sec = worker._handle_broker_error_exception(
                    exc,
                    tick=state.tick,
                )
            except Exception as exc:
                self._handle_fatal_error(exc, tick=state.tick)

            self._finalize_iteration(state)

    def _pre_iteration_checks(self, state: WorkerIterationState) -> str:
        worker = self.worker
        if not worker._worker_lease_allows_trading():
            state.last_error = "worker_lease_revoked"
            worker._save_state(
                last_price=worker._latest_price(),
                last_error=state.last_error,
            )
            return "break"

        if worker._symbol_disabled:
            state.last_error = worker._symbol_disabled_reason or "symbol_disabled_by_safety_guard"
            worker._save_state(
                last_price=worker._latest_price(),
                last_error=state.last_error,
            )
            worker.stop_event.wait(max(worker._next_wait_duration(), 5.0))
            return "continue"

        return "proceed"

    def _process_iteration(self, state: WorkerIterationState) -> bool:
        worker = self.worker
        worker._load_symbol_spec()
        state.tick = self._load_iteration_tick(state)
        if state.tick is None:
            return True

        raw_tick_timestamp_sec = worker._normalize_timestamp_seconds(state.tick.timestamp)
        tick_freshness_timestamp_sec = worker._tick_freshness_timestamp_sec(state.tick)
        if tick_freshness_timestamp_sec is None:
            tick_freshness_timestamp_sec = raw_tick_timestamp_sec
        tick_freshness_monotonic_sec = worker._monotonic_now()
        current_volume, volume_sample = worker._current_volume_from_tick(state.tick)
        tick_timestamp = worker._history_runtime.append_tick_sample(
            state.tick,
            current_volume=current_volume,
            volume_sample=volume_sample,
        )
        worker._update_last_price_freshness(
            freshness_ts=tick_freshness_timestamp_sec,
            freshness_monotonic_ts=tick_freshness_monotonic_sec,
        )
        worker._reset_broker_error_trackers()
        current_spread_pips = worker._spread_in_pips(state.tick.bid, state.tick.ask)
        current_spread_pct = worker._spread_in_pct(state.tick.bid, state.tick.ask)
        spread_blocked, spread_avg, spread_threshold = worker._spread_filter_metrics(
            current_spread_pips
        )
        worker._append_spread_sample(current_spread_pips)
        state.stream_health = worker._refresh_stream_health()

        active = worker.position_book.get(worker.symbol)
        if active and worker._cycle_runtime.process_active_position(
            tick=state.tick,
            tick_timestamp=tick_timestamp,
            last_error=state.last_error,
        ):
            return True

        active = worker.position_book.get(worker.symbol)
        if not active and worker._cycle_runtime.process_flat_entry(
            tick=state.tick,
            raw_tick_timestamp_sec=raw_tick_timestamp_sec,
            tick_freshness_timestamp_sec=tick_freshness_timestamp_sec,
            tick_freshness_monotonic_sec=tick_freshness_monotonic_sec,
            tick_timestamp=tick_timestamp,
            current_spread_pips=current_spread_pips,
            current_spread_pct=current_spread_pct,
            current_volume=current_volume,
            spread_blocked=spread_blocked,
            spread_avg=spread_avg,
            spread_threshold=spread_threshold,
            stream_health=state.stream_health,
            last_error=state.last_error,
        ):
            return True

        return False

    def _load_iteration_tick(self, state: WorkerIterationState) -> PriceTick | None:
        worker = self.worker
        if worker.db_first_reads_enabled:
            tick = worker._load_db_first_tick()
            if tick is None:
                active_for_tick_fallback = worker.position_book.get(worker.symbol)
                if active_for_tick_fallback is not None:
                    tick = worker._load_direct_tick_for_active_position_fallback(active_for_tick_fallback)
                else:
                    tick = worker._load_direct_tick_for_entry_fallback(
                        allow_rest_fallback=False,
                    )
            if tick is None:
                state.last_error = f"DB-first tick cache is empty or stale for {worker.symbol}"
                state.sleep_override_sec = worker._handle_db_first_tick_cache_miss()
                active_for_reconcile = worker.position_book.get(worker.symbol)
                if active_for_reconcile is not None:
                    worker._handle_active_position_without_fresh_tick(
                        active_for_reconcile,
                        state.last_error,
                    )
                worker._save_state(
                    last_price=worker._latest_price(),
                    last_error=state.last_error,
                )
                wait_sec = worker._next_wait_duration(
                    has_active_position=worker.position_book.get(worker.symbol) is not None
                )
                wait_sec = max(wait_sec, state.sleep_override_sec)
                worker._wait_with_state_heartbeat(
                    wait_sec,
                    last_price=worker._latest_price(),
                    last_error=state.last_error,
                )
                return None
            worker._db_first_tick_cache_miss_streak = 0
            worker._db_first_tick_cache_retry_after_ts = 0.0
            worker._db_first_tick_cache_retry_backoff_sec = 0.0
            return tick

        tick = worker.broker.get_price(worker.symbol)
        worker._publish_latest_tick(tick)
        return tick

    def _handle_fatal_error(self, exc: Exception, *, tick: PriceTick | None) -> None:
        worker = self.worker
        error_text = str(exc)
        last_error = f"fatal:{type(exc).__name__}:{error_text}"
        logger.exception("Fatal worker error for %s", worker.symbol)
        worker.store.record_event(
            "CRITICAL",
            worker.symbol,
            "Fatal worker error",
            {
                "error": error_text,
                "error_type": type(exc).__name__,
            },
        )
        worker._save_state(
            last_price=tick.mid if tick else None,
            last_error=last_error,
        )
        worker.stop_event.set()
        raise

    def _finalize_iteration(self, state: WorkerIterationState) -> None:
        worker = self.worker
        worker._save_state(
            last_price=state.tick.mid if state.tick else None,
            last_error=state.last_error,
        )
        wait_sec = worker._next_wait_duration(
            has_active_position=worker.position_book.get(worker.symbol) is not None
        )
        if state.sleep_override_sec is not None:
            wait_sec = max(wait_sec, state.sleep_override_sec)
        worker.stop_event.wait(wait_sec)
