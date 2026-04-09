from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from dataclasses import dataclass
import logging
import math
import re
import time

from xtb_bot.models import PriceTick, SymbolSpec


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkerRecoveryState:
    epic_unavailable_consecutive_errors: int = 0
    quote_unavailable_consecutive_errors: int = 0
    symbol_disabled: bool = False
    symbol_disabled_reason: str | None = None
    epic_unavailable_first_error_ts: float = 0.0
    quote_unavailable_first_error_ts: float = 0.0
    allowance_backoff_until_ts: float = 0.0
    last_allowance_backoff_kind: str | None = None
    last_allowance_backoff_event_ts: float = 0.0
    allowance_backoff_streak: int = 0
    db_first_tick_cache_miss_streak: int = 0
    db_first_tick_cache_last_warn_ts: float = 0.0
    db_first_tick_cache_retry_after_ts: float = 0.0
    db_first_tick_cache_retry_backoff_sec: float = 0.0
    db_first_active_tick_fallback_last_warn_ts: float = 0.0
    db_first_entry_tick_fallback_last_warn_ts: float = 0.0
    db_first_account_snapshot_fallback_last_warn_ts: float = 0.0
    db_first_account_snapshot_fallback_next_attempt_ts: float = 0.0
    db_first_symbol_spec_cache_miss_streak: int = 0
    db_first_symbol_spec_cache_last_warn_ts: float = 0.0
    symbol_spec_retry_after_ts: float = 0.0
    symbol_spec_retry_backoff_sec: float = 0.0


class WorkerRecoveryManager:
    def __init__(self, worker: object) -> None:
        self.worker = worker
        self.state = WorkerRecoveryState()

    @staticmethod
    def is_epic_unavailable_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return "epic.unavailable" in lowered or "instrument.epic.unavailable" in lowered

    @staticmethod
    def is_quote_unavailable_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return "does not contain bid/offer" in lowered

    def disable_symbol(self, reason: str) -> None:
        worker = self.worker
        if self.state.symbol_disabled:
            return
        self.state.symbol_disabled = True
        self.state.symbol_disabled_reason = reason
        worker.store.record_event(
            "ERROR",
            worker.symbol,
            "Symbol disabled by safety guard",
            {
                "reason": reason,
                "epic_unavailable_consecutive_errors": self.state.epic_unavailable_consecutive_errors,
                "quote_unavailable_consecutive_errors": self.state.quote_unavailable_consecutive_errors,
                "disable_threshold": worker.symbol_auto_disable_epic_unavailable_threshold,
                "auto_disable_enabled": worker.symbol_auto_disable_on_epic_unavailable,
                "min_error_window_sec": worker.symbol_auto_disable_min_error_window_sec,
            },
        )
        logger.error(
            "Symbol disabled by safety guard | symbol=%s reason=%s consecutive=%s threshold=%s",
            worker.symbol,
            reason,
            self.state.epic_unavailable_consecutive_errors,
            worker.symbol_auto_disable_epic_unavailable_threshold,
        )

    def register_broker_error(self, error_text: str) -> None:
        worker = self.worker
        if self.state.symbol_disabled:
            return
        now = worker._monotonic_now()
        if self.is_epic_unavailable_error(error_text):
            if self.state.epic_unavailable_consecutive_errors <= 0:
                self.state.epic_unavailable_first_error_ts = now
            self.state.epic_unavailable_consecutive_errors += 1
            self.state.quote_unavailable_consecutive_errors = 0
            self.state.quote_unavailable_first_error_ts = 0.0
            elapsed_sec = (
                now - self.state.epic_unavailable_first_error_ts
                if self.state.epic_unavailable_first_error_ts > 0
                else 0.0
            )
            if (
                worker.symbol_auto_disable_on_epic_unavailable
                and self.state.epic_unavailable_consecutive_errors
                >= worker.symbol_auto_disable_epic_unavailable_threshold
                and elapsed_sec >= worker.symbol_auto_disable_min_error_window_sec
            ):
                self.disable_symbol("repeated_epic_unavailable_errors")
            return
        if self.is_quote_unavailable_error(error_text):
            if self.state.quote_unavailable_consecutive_errors <= 0:
                self.state.quote_unavailable_first_error_ts = now
            self.state.quote_unavailable_consecutive_errors += 1
            self.state.epic_unavailable_consecutive_errors = 0
            self.state.epic_unavailable_first_error_ts = 0.0
            elapsed_sec = (
                now - self.state.quote_unavailable_first_error_ts
                if self.state.quote_unavailable_first_error_ts > 0
                else 0.0
            )
            if (
                worker.symbol_auto_disable_on_epic_unavailable
                and self.state.quote_unavailable_consecutive_errors
                >= worker.symbol_auto_disable_epic_unavailable_threshold
                and elapsed_sec >= worker.symbol_auto_disable_min_error_window_sec
            ):
                self.disable_symbol("repeated_quote_unavailable_errors")
            return
        self.state.epic_unavailable_consecutive_errors = 0
        self.state.quote_unavailable_consecutive_errors = 0
        self.state.epic_unavailable_first_error_ts = 0.0
        self.state.quote_unavailable_first_error_ts = 0.0

    def reset_broker_error_trackers(self) -> None:
        self.state.epic_unavailable_consecutive_errors = 0
        self.state.quote_unavailable_consecutive_errors = 0
        self.state.epic_unavailable_first_error_ts = 0.0
        self.state.quote_unavailable_first_error_ts = 0.0

    @staticmethod
    def is_allowance_backoff_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return (
            "allowance cooldown is active" in lowered
            or "critical_trade_operation_active" in lowered
            or "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )

    @staticmethod
    def allowance_backoff_kind(error_text: str) -> str:
        lowered = str(error_text).lower()
        if "allowance cooldown is active" in lowered:
            return "cooldown_active"
        if "critical_trade_operation_active" in lowered:
            return "critical_trade_deferred"
        if "exceeded-api-key-allowance" in lowered:
            return "api_key_allowance_exceeded"
        if "exceeded-account-allowance" in lowered:
            return "account_allowance_exceeded"
        if "exceeded-account-trading-allowance" in lowered:
            return "account_trading_allowance_exceeded"
        return "allowance_backoff"

    @staticmethod
    def extract_broker_minimum_lot_from_error(error_text: str) -> tuple[float | None, str | None, str | None]:
        text = str(error_text)
        min_match = re.search(r"\bmin=([0-9]*\.?[0-9]+)\b", text)
        if min_match is None:
            return None, None, None
        try:
            minimum = float(min_match.group(1))
        except (TypeError, ValueError):
            return None, None, None
        if not math.isfinite(minimum) or minimum <= 0:
            return None, None, None

        epic_match = re.search(r"\bepic=([^,\s)]+)", text)
        source_match = re.search(r"\blot_min_source=([^,\s)]+)", text)
        epic = str(epic_match.group(1)).strip() if epic_match else None
        source = str(source_match.group(1)).strip() if source_match else None
        return minimum, epic, source

    def sync_local_symbol_lot_min_from_broker_error(self, error_text: str) -> bool:
        worker = self.worker
        if worker.symbol_spec is None:
            return False
        minimum, epic, source = self.extract_broker_minimum_lot_from_error(error_text)
        if minimum is None:
            return False
        previous = float(worker.symbol_spec.lot_min)
        if minimum <= previous + FLOAT_ROUNDING_TOLERANCE:
            return False

        worker.symbol_spec.lot_min = minimum
        worker.symbol_spec.lot_max = max(float(worker.symbol_spec.lot_max), minimum)
        metadata = worker.symbol_spec.metadata if isinstance(worker.symbol_spec.metadata, dict) else {}
        metadata["lot_min_source"] = source or "broker_minimum_error"
        metadata["lot_min_update_reason"] = "BROKER_MINIMUM_SIZE_ERROR"
        metadata["lot_min_update_ts"] = worker._wall_time_now()
        if epic:
            metadata["lot_min_update_epic"] = epic
        worker.symbol_spec.metadata = metadata
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Local symbol lot minimum updated",
            {
                "previous_lot_min": previous,
                "updated_lot_min": minimum,
                "source": source or "broker_minimum_error",
                "epic": epic,
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Local symbol lot_min updated | symbol=%s %.6f->%.6f source=%s epic=%s",
            worker.symbol,
            previous,
            minimum,
            source or "broker_minimum_error",
            epic or "",
        )
        return True

    @staticmethod
    def is_open_confirm_timeout_pending_recovery_error(error_text: str) -> bool:
        lowered = str(error_text or "").strip().lower()
        return (
            "confirm timed out before dealid was available" in lowered
            and "pending recovery required" in lowered
        )

    def refresh_symbol_spec_after_min_order_reject(self, error_text: str) -> bool:
        worker = self.worker
        lowered = str(error_text).lower()
        if (
            "minimum_order_size_error" not in lowered
            and "order_size_increment_error" not in lowered
            and "size_increment" not in lowered
            and "set increments" not in lowered
            and "invalid_size" not in lowered
            and "invalid.size" not in lowered
            and "invalid size" not in lowered
        ):
            return False
        getter = getattr(worker.broker, "get_symbol_spec", None)
        if not callable(getter):
            return False
        try:
            current_epic = worker._epic_text()
            with worker._watchdog_blocking_operation(
                "refresh_symbol_spec_after_min_order_reject",
                last_price=worker._latest_price(),
            ):
                if current_epic:
                    latest_spec = worker._broker_get_symbol_spec_for_epic(
                        worker.symbol,
                        current_epic,
                        force_refresh=True,
                    )
                else:
                    latest_spec = worker._broker_get_symbol_spec(worker.symbol, force_refresh=True)
        except Exception:
            return False
        if not isinstance(latest_spec, SymbolSpec):
            return False

        previous_min = float(worker.symbol_spec.lot_min) if worker.symbol_spec is not None else 0.0
        previous_step = float(worker.symbol_spec.lot_step) if worker.symbol_spec is not None else 0.0
        latest_min = float(latest_spec.lot_min)
        latest_step = float(latest_spec.lot_step)
        min_promoted = latest_min > previous_min + FLOAT_ROUNDING_TOLERANCE
        step_promoted = latest_step > previous_step + FLOAT_ROUNDING_TOLERANCE
        if not min_promoted and not step_promoted:
            return False
        worker.symbol_spec = latest_spec
        worker._cache_symbol_pip_size(latest_spec.tick_size, source="worker_symbol_spec_refresh")
        try:
            worker.store.upsert_broker_symbol_spec(
                symbol=worker.symbol,
                spec=latest_spec,
                ts=worker._wall_time_now(),
                source="worker_symbol_spec_refresh",
            )
        except Exception:
            logger.debug("Failed to persist refreshed symbol specification", exc_info=True)
        refresh_reason = "minimum_order_size_reject" if min_promoted else "order_size_increment_reject"
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Local symbol specification refreshed",
            {
                "previous_lot_min": previous_min,
                "updated_lot_min": latest_min,
                "previous_lot_step": previous_step,
                "updated_lot_step": latest_step,
                "reason": refresh_reason,
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Local symbol spec refreshed after size reject | symbol=%s min %.6f->%.6f step %.6f->%.6f",
            worker.symbol,
            previous_min,
            latest_min,
            previous_step,
            latest_step,
        )
        return True

    @staticmethod
    def extract_allowance_backoff_remaining_sec(error_text: str) -> float | None:
        match = re.search(r"\(([\d.]+)s remaining\)", str(error_text))
        if match is None:
            return None
        try:
            remaining = float(match.group(1))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(remaining) or remaining <= 0:
            return None
        return remaining

    def handle_allowance_backoff_error(self, error_text: str) -> float:
        worker = self.worker
        now = worker._monotonic_now()
        lowered = str(error_text or "").lower()
        kind = self.allowance_backoff_kind(error_text)
        is_market_data_allowance = (
            kind in {"api_key_allowance_exceeded", "account_allowance_exceeded"}
            and "/markets" in lowered
        )
        remaining = self.extract_allowance_backoff_remaining_sec(error_text)
        if remaining is None:
            broker_remaining = self.broker_public_api_backoff_remaining_sec()
            if is_market_data_allowance:
                remaining = min(5.0, max(1.0, broker_remaining if broker_remaining > 0 else 2.0))
            else:
                remaining = max(2.0, min(30.0, worker.stream_event_cooldown_sec / 2.0))
        elif is_market_data_allowance:
            remaining = min(5.0, max(1.0, remaining))
        self.state.allowance_backoff_until_ts = max(self.state.allowance_backoff_until_ts, now + remaining)
        reason_changed = kind != self.state.last_allowance_backoff_kind
        cooldown_passed = (now - self.state.last_allowance_backoff_event_ts) >= worker.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            payload = {
                "kind": kind,
                "remaining_sec": round(remaining, 2),
                "error": error_text,
            }
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Broker allowance backoff active",
                payload,
            )
            self.state.last_allowance_backoff_event_ts = now
            self.state.last_allowance_backoff_kind = kind
        self.state.allowance_backoff_streak += 1
        if (
            worker.operational_guard_enabled
            and self.state.allowance_backoff_streak >= worker.operational_guard_allowance_backoff_streak_threshold
        ):
            worker._activate_operational_guard(
                reason=f"allowance_backoff_streak:{self.state.allowance_backoff_streak}",
                now_ts=now,
                payload={
                    "kind": kind,
                    "remaining_sec": round(remaining, 2),
                    "threshold": worker.operational_guard_allowance_backoff_streak_threshold,
                },
            )
        logger.warning(
            "Broker allowance backoff active | symbol=%s kind=%s remaining_sec=%.2f",
            worker.symbol,
            kind,
            remaining,
        )
        return remaining

    def sync_allowance_backoff_from_connectivity_probe(
        self,
        error_text: str,
        *,
        now_ts: float | None = None,
    ) -> float:
        worker = self.worker
        now = worker._monotonic_now() if now_ts is None else float(now_ts)
        remaining = self.extract_allowance_backoff_remaining_sec(error_text)
        if remaining is None:
            broker_remaining = self.broker_public_api_backoff_remaining_sec()
            remaining = max(
                2.0,
                broker_remaining if broker_remaining > 0 else max(30.0, worker.connectivity_check_interval_sec),
            )
        self.state.allowance_backoff_until_ts = max(self.state.allowance_backoff_until_ts, now + remaining)
        return remaining

    def broker_public_api_backoff_remaining_sec(self) -> float:
        worker = self.worker
        getter = getattr(worker.broker, "get_public_api_backoff_remaining_sec", None)
        if not callable(getter):
            return 0.0
        try:
            remaining = float(getter())
        except Exception:
            return 0.0
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def local_allowance_backoff_remaining_sec(self) -> float:
        worker = self.worker
        remaining = worker._runtime_remaining_sec(
            self.state.allowance_backoff_until_ts,
            now_monotonic=worker._monotonic_now(),
        )
        if not math.isfinite(remaining) or remaining <= 0:
            self.state.allowance_backoff_streak = 0
            return 0.0
        return remaining

    @staticmethod
    def is_db_first_tick_cache_miss_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return "db-first tick cache is empty or stale" in lowered

    @staticmethod
    def is_db_first_symbol_spec_cache_miss_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return "db-first symbol spec cache is empty or stale" in lowered

    def handle_db_first_tick_cache_miss(self) -> float:
        worker = self.worker
        self.state.db_first_tick_cache_miss_streak += 1
        now = worker._monotonic_now()
        base_backoff_sec = max(5.0, worker.poll_interval_sec * 2.0)
        next_backoff_sec = min(
            120.0,
            max(
                base_backoff_sec,
                self.state.db_first_tick_cache_retry_backoff_sec * 2.0
                if self.state.db_first_tick_cache_retry_backoff_sec > 0.0
                else base_backoff_sec,
            ),
        )
        self.state.db_first_tick_cache_retry_backoff_sec = next_backoff_sec
        self.state.db_first_tick_cache_retry_after_ts = now + next_backoff_sec
        warn_interval_sec = max(
            60.0,
            worker.stream_event_cooldown_sec * 3.0,
            min(300.0, next_backoff_sec),
        )
        if (
            self.state.db_first_tick_cache_last_warn_ts <= 0.0
            or (now - self.state.db_first_tick_cache_last_warn_ts) >= warn_interval_sec
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "DB-first tick cache warming up",
                {
                    "streak": self.state.db_first_tick_cache_miss_streak,
                    "max_age_sec": worker.db_first_tick_max_age_sec,
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                    "retry_after_sec": round(next_backoff_sec, 3),
                },
            )
            self.state.db_first_tick_cache_last_warn_ts = now
        return max(base_backoff_sec, next_backoff_sec)

    def wait_with_state_heartbeat(
        self,
        wait_sec: float,
        *,
        last_price: float | None,
        last_error: str | None = None,
    ) -> None:
        worker = self.worker
        remaining = max(0.0, float(wait_sec))
        if remaining <= 0.0:
            return
        heartbeat_interval_sec = max(
            1.0,
            min(float(worker.worker_state_flush_interval_sec), 15.0),
        )
        deadline = time.monotonic() + remaining
        while not worker.stop_event.is_set():
            now_monotonic = time.monotonic()
            remaining_sec = deadline - now_monotonic
            if remaining_sec <= FLOAT_COMPARISON_TOLERANCE:
                return
            slice_sec = min(heartbeat_interval_sec, remaining_sec)
            worker.stop_event.wait(slice_sec)
            if worker.stop_event.is_set():
                return
            if (deadline - time.monotonic()) > FLOAT_COMPARISON_TOLERANCE:
                worker._save_state(last_price=last_price, last_error=last_error)

    def handle_db_first_symbol_spec_cache_miss(self) -> float:
        worker = self.worker
        self.state.db_first_symbol_spec_cache_miss_streak += 1
        now = worker._monotonic_now()
        next_backoff_sec = max(5.0, float(self.state.symbol_spec_retry_backoff_sec or 0.0))
        warn_interval_sec = max(
            60.0,
            worker.stream_event_cooldown_sec * 3.0,
            min(300.0, next_backoff_sec),
        )
        if (
            self.state.db_first_symbol_spec_cache_last_warn_ts <= 0.0
            or (now - self.state.db_first_symbol_spec_cache_last_warn_ts) >= warn_interval_sec
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "DB-first symbol spec cache warming up",
                {
                    "streak": self.state.db_first_symbol_spec_cache_miss_streak,
                    "max_age_sec": worker.db_first_symbol_spec_max_age_sec,
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                    "retry_after_sec": round(next_backoff_sec, 3),
                },
            )
            self.state.db_first_symbol_spec_cache_last_warn_ts = now
        return max(5.0, next_backoff_sec)

    def handle_broker_exception(
        self,
        exc: Exception,
        *,
        tick: PriceTick | None,
    ) -> tuple[str | None, float | None]:
        worker = self.worker
        error_text = str(exc)
        last_error: str | None = None
        sleep_override_sec: float | None = None
        if self.is_db_first_symbol_spec_cache_miss_error(error_text):
            last_error = error_text
            sleep_override_sec = self.handle_db_first_symbol_spec_cache_miss()
        elif self.is_db_first_tick_cache_miss_error(error_text):
            last_error = error_text
            sleep_override_sec = self.handle_db_first_tick_cache_miss()
        elif self.is_allowance_backoff_error(error_text):
            last_error = f"allowance_backoff:{error_text}"
            sleep_override_sec = self.handle_allowance_backoff_error(error_text)
        elif self.refresh_symbol_spec_after_min_order_reject(error_text):
            last_error = f"symbol_spec_refreshed:{error_text}"
        elif self.sync_local_symbol_lot_min_from_broker_error(error_text):
            last_error = f"symbol_lot_min_updated:{error_text}"
        elif worker._activate_reject_entry_cooldown(error_text):
            last_error = f"broker_reject_cooldown:{error_text}"
            self.register_broker_error(error_text)
            worker.store.record_event("ERROR", worker.symbol, "Broker error", {"error": error_text})
        elif worker._handle_missing_broker_position_error(error_text, tick):
            if worker._pending_missing_position_id is not None:
                last_error = f"broker_position_missing_pending:{error_text}"
            else:
                last_error = f"broker_position_missing_reconciled:{error_text}"
        else:
            last_error = error_text
            self.register_broker_error(last_error)
            worker.store.record_event("ERROR", worker.symbol, "Broker error", {"error": error_text})
        return last_error, sleep_override_sec
