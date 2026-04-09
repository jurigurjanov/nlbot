from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from contextlib import contextmanager
from dataclasses import dataclass, field
import threading

from xtb_bot.models import Position


@dataclass(slots=True)
class WorkerLifecycleState:
    worker_lease_next_check_ts: float = 0.0
    worker_lease_last_revoke_event_ts: float = 0.0
    watchdog_blocking_operation_lock: threading.Lock = field(default_factory=threading.Lock)
    watchdog_blocking_operation_stack: list[tuple[str, float, float]] = field(default_factory=list)
    watchdog_blocking_operation_name: str | None = None
    watchdog_blocking_operation_started_at: float = 0.0
    watchdog_blocking_operation_grace_sec: float = 0.0


class WorkerLifecycleManager:
    def __init__(self, worker: object) -> None:
        self.worker = worker
        self.state = WorkerLifecycleState()

    def worker_lease_allows_trading(self, *, force_check: bool = False) -> bool:
        worker = self.worker
        if worker._worker_lease_key is None or worker._worker_lease_id is None:
            return True
        now = worker._monotonic_now()
        if (
            (not force_check)
            and worker._runtime_remaining_sec(
                self.state.worker_lease_next_check_ts,
                now_monotonic=now,
            ) > FLOAT_COMPARISON_TOLERANCE
        ):
            return True
        self.state.worker_lease_next_check_ts = now + worker._worker_lease_check_interval_sec
        current_lease_id = str(worker.store.get_kv(worker._worker_lease_key) or "").strip()
        if current_lease_id == worker._worker_lease_id:
            return True

        if (now - self.state.worker_lease_last_revoke_event_ts) >= max(
            5.0,
            worker.stream_event_cooldown_sec,
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Worker lease revoked, stopping stale worker",
                {
                    "worker": worker.name,
                    "lease_key": worker._worker_lease_key,
                    "lease_id": worker._worker_lease_id,
                    "observed_lease_id": current_lease_id or None,
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                },
            )
            self.state.worker_lease_last_revoke_event_ts = now
        worker.stop_event.set()
        return False

    def allow_risk_reducing_action_after_lease_revocation(
        self,
        *,
        action: str,
        position: Position,
        reason: str,
        close_volume: float | None = None,
    ) -> None:
        worker = self.worker
        if self.worker_lease_allows_trading(force_check=True):
            return
        payload: dict[str, object] = {
            "position_id": position.position_id,
            "reason": reason,
            "mode": worker.mode.value,
            "strategy": worker.strategy_name,
        }
        if close_volume is not None and close_volume > 0:
            payload["close_volume"] = close_volume
        worker.store.record_event(
            "WARN",
            worker.symbol,
            f"Worker lease revoked during {action}, proceeding with risk-reducing broker close",
            payload,
        )

    def set_watchdog_blocking_operation_state(
        self,
        operation: str | None,
        *,
        started_at: float = 0.0,
        grace_sec: float = 0.0,
    ) -> None:
        normalized_operation = str(operation or "").strip() or None
        if normalized_operation is None:
            self.state.watchdog_blocking_operation_name = None
            self.state.watchdog_blocking_operation_started_at = 0.0
            self.state.watchdog_blocking_operation_grace_sec = 0.0
            return
        self.state.watchdog_blocking_operation_name = normalized_operation
        self.state.watchdog_blocking_operation_started_at = max(0.0, float(started_at or 0.0))
        self.state.watchdog_blocking_operation_grace_sec = max(0.0, float(grace_sec or 0.0))

    @contextmanager
    def watchdog_blocking_operation(
        self,
        operation: str,
        *,
        last_price: float | None = None,
        last_error: str | None = None,
        grace_sec: float = 180.0,
    ):
        worker = self.worker
        normalized_operation = str(operation or "").strip() or "broker_operation"
        started_at = worker._monotonic_now()
        effective_grace_sec = max(30.0, float(grace_sec or 0.0))
        worker._save_state(last_price=last_price, last_error=last_error, force=True)
        with self.state.watchdog_blocking_operation_lock:
            self.state.watchdog_blocking_operation_stack.append(
                (normalized_operation, started_at, effective_grace_sec)
            )
            self.set_watchdog_blocking_operation_state(
                normalized_operation,
                started_at=started_at,
                grace_sec=effective_grace_sec,
            )
        try:
            yield
        finally:
            with self.state.watchdog_blocking_operation_lock:
                if self.state.watchdog_blocking_operation_stack:
                    self.state.watchdog_blocking_operation_stack.pop()
                if self.state.watchdog_blocking_operation_stack:
                    previous_operation, previous_started_at, previous_grace_sec = (
                        self.state.watchdog_blocking_operation_stack[-1]
                    )
                    self.set_watchdog_blocking_operation_state(
                        previous_operation,
                        started_at=previous_started_at,
                        grace_sec=previous_grace_sec,
                    )
                else:
                    self.set_watchdog_blocking_operation_state(None)

    def watchdog_blocking_operation_status(self) -> dict[str, object] | None:
        worker = self.worker
        with self.state.watchdog_blocking_operation_lock:
            if not self.state.watchdog_blocking_operation_stack:
                return None
            operation, started_at, grace_sec = self.state.watchdog_blocking_operation_stack[-1]
        return {
            "operation": operation,
            "started_at": float(
                worker._wall_time_for_runtime_ts(
                    started_at,
                    now_monotonic=worker._monotonic_now(),
                    now_wall=worker._wall_time_now(),
                )
                or 0.0
            ),
            "started_at_monotonic": float(started_at),
            "grace_sec": float(grace_sec),
        }
