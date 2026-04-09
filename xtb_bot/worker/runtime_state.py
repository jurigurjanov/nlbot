from __future__ import annotations

from dataclasses import dataclass
import math

from xtb_bot.client import BrokerError
from xtb_bot.models import AccountSnapshot, WorkerState


@dataclass(slots=True)
class WorkerRuntimeState:
    cached_account_snapshot: AccountSnapshot | None = None
    cached_account_snapshot_ts: float = 0.0
    last_saved_worker_state_ts: float = 0.0
    last_saved_worker_state_monotonic: float = 0.0
    last_saved_worker_state_signature: tuple[object, ...] | None = None
    last_account_snapshot_persist_ts: float = 0.0
    last_account_snapshot_persist_signature: tuple[object, ...] | None = None


class WorkerRuntimeStateManager:
    def __init__(self, worker: object) -> None:
        self.worker = worker
        self.state = WorkerRuntimeState()

    def latest_price(self) -> float | None:
        worker = self.worker
        history_lock = getattr(worker, "_price_history_lock", None)
        if history_lock is None:
            prices = worker.prices
        else:
            with history_lock:
                prices = list(worker.prices)
        if not prices:
            return None
        try:
            latest = float(prices[-1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(latest):
            return None
        return latest

    def get_account_snapshot_cached(self) -> AccountSnapshot:
        worker = self.worker
        now_monotonic = worker._monotonic_now()
        if worker.db_first_reads_enabled:
            cached_db_snapshot = worker.store.load_latest_broker_account_snapshot(
                max_age_sec=worker.db_first_account_snapshot_max_age_sec
            )
            if cached_db_snapshot is not None:
                self.state.cached_account_snapshot = cached_db_snapshot
                self.state.cached_account_snapshot_ts = now_monotonic
                return cached_db_snapshot
            stale_db_snapshot = worker.store.load_latest_broker_account_snapshot(max_age_sec=0.0)
            active_position_present = worker.position_book.get(worker.symbol) is not None
            direct_fallback = worker._load_direct_account_snapshot_fallback(
                stale_snapshot=stale_db_snapshot,
                allow_stale_for_active_position=active_position_present,
            )
            if direct_fallback is not None:
                return direct_fallback
            cached = self.state.cached_account_snapshot
            if cached is not None and active_position_present:
                return cached
            raise BrokerError(
                f"DB-first account snapshot cache is empty or stale for {worker.symbol}"
            )

        cached = self.state.cached_account_snapshot
        if cached is not None and worker._local_allowance_backoff_remaining_sec() > 0:
            return cached
        if (
            cached is not None
            and (now_monotonic - self.state.cached_account_snapshot_ts) <= worker.account_snapshot_cache_ttl_sec
        ):
            return cached
        snapshot = worker.broker.get_account_snapshot()
        self.state.cached_account_snapshot = snapshot
        self.state.cached_account_snapshot_ts = now_monotonic
        return snapshot

    def save_state(
        self,
        last_price: float | None,
        last_error: str | None = None,
        *,
        force: bool = False,
    ) -> None:
        worker = self.worker
        active = worker.position_book.get(worker.symbol)
        worker._prune_position_runtime_state(active.position_id if active is not None else None)
        emergency_error = worker._current_multi_strategy_emergency_worker_error()
        effective_last_error = str(last_error or "").strip()
        if emergency_error:
            if effective_last_error:
                if emergency_error not in effective_last_error:
                    effective_last_error = f"{effective_last_error} | {emergency_error}"
            else:
                effective_last_error = emergency_error
        if not effective_last_error:
            effective_last_error = None
        position_signature: tuple[object, ...] | None = None
        if active is not None:
            position_signature = (
                active.position_id,
                active.status,
                round(float(active.open_price), 8),
                round(float(active.stop_loss), 8),
                round(float(active.take_profit), 8),
                round(float(active.close_price), 8) if active.close_price is not None else None,
            )
        signature = (position_signature, str(effective_last_error or ""))
        now_monotonic = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        last_saved_monotonic = float(self.state.last_saved_worker_state_monotonic or 0.0)
        if last_saved_monotonic <= 0.0:
            legacy_last_saved_ts = float(self.state.last_saved_worker_state_ts or 0.0)
            if legacy_last_saved_ts > 0.0 and not worker._looks_like_wall_clock_ts(legacy_last_saved_ts):
                last_saved_monotonic = legacy_last_saved_ts
        if (
            (not force)
            and self.state.last_saved_worker_state_signature == signature
            and (now_monotonic - last_saved_monotonic) < worker.worker_state_flush_interval_sec
        ):
            return
        base_strategy_label = worker._base_strategy_label()
        normalized_strategy_name = worker._normalize_strategy_label(worker.strategy_name)
        position_strategy_entry = worker._position_entry_strategy(active) if active is not None else None
        position_strategy_component = worker._position_entry_component(active) if active is not None else None
        position_strategy_signal = worker._position_entry_signal(active) if active is not None else None
        state = WorkerState(
            symbol=worker.symbol,
            thread_name=worker.name,
            mode=worker.mode,
            strategy=worker.strategy_name,
            last_price=last_price,
            last_heartbeat=worker._wall_time_now(),
            iteration=worker.iteration,
            strategy_base=(
                base_strategy_label
                if base_strategy_label != normalized_strategy_name
                else None
            ),
            position_strategy_entry=position_strategy_entry,
            position_strategy_component=position_strategy_component,
            position_strategy_signal=position_strategy_signal,
            position=active.to_dict() if active else None,
            last_error=effective_last_error,
        )
        worker.store.save_worker_state(state)
        self.state.last_saved_worker_state_ts = now_wall
        self.state.last_saved_worker_state_monotonic = now_monotonic
        self.state.last_saved_worker_state_signature = signature

    def persist_account_snapshot_if_due(
        self,
        snapshot: AccountSnapshot,
        *,
        open_positions: int,
        daily_pnl: float,
        drawdown_pct: float,
    ) -> None:
        worker = self.worker
        signature = (
            round(float(snapshot.balance), 6),
            round(float(snapshot.equity), 6),
            round(float(snapshot.margin_free), 6),
            int(open_positions),
            round(float(daily_pnl), 6),
            round(float(drawdown_pct), 6),
        )
        now_monotonic = worker._monotonic_now()
        if (
            self.state.last_account_snapshot_persist_signature == signature
            and (now_monotonic - self.state.last_account_snapshot_persist_ts) < worker.account_snapshot_persist_interval_sec
        ):
            return
        if (
            self.state.last_account_snapshot_persist_signature is not None
            and (now_monotonic - self.state.last_account_snapshot_persist_ts) < worker.account_snapshot_persist_interval_sec
            and int(open_positions) == int(self.state.last_account_snapshot_persist_signature[3])
        ):
            return
        worker.store.record_account_snapshot(
            snapshot=snapshot,
            open_positions=open_positions,
            daily_pnl=daily_pnl,
            drawdown_pct=drawdown_pct,
        )
        self.state.last_account_snapshot_persist_ts = now_monotonic
        self.state.last_account_snapshot_persist_signature = signature

    def next_wait_duration(self, has_active_position: bool = False) -> float:
        worker = self.worker
        base_interval = worker.poll_interval_sec
        if has_active_position:
            base_interval = min(base_interval, worker.active_position_poll_interval_sec)
        if base_interval <= 0:
            return 0.0
        if worker.poll_jitter_sec <= 0:
            return base_interval
        jitter = worker._rng.uniform(-worker.poll_jitter_sec, worker.poll_jitter_sec)
        return max(0.1, base_interval + jitter)
