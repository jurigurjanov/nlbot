from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging
from typing import Any

from xtb_bot.broker_method_support import call_broker_method_with_supported_kwargs
from xtb_bot.models import Position, PriceTick, RunMode, Side


logger = logging.getLogger("xtb_bot.worker")


class WorkerReconcileManager:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    @staticmethod
    def is_broker_position_missing_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        if "position.details.null.error" in lowered:
            return True
        if "position.notional.details.null.error" in lowered:
            return True
        if "positions/otc" in lowered and "404" in lowered and "error.service.marketdata.position" in lowered:
            return True
        if "positions/otc/" in lowered and "404" in lowered:
            return True
        return False

    def record_close_deferred_by_allowance(
        self,
        position: Position,
        reason: str,
        wait_sec: float,
        error_text: str,
    ) -> None:
        worker = self._worker
        now = worker._monotonic_now()
        reason_changed = worker._last_close_deferred_position_id != position.position_id
        cooldown_passed = (now - worker._last_close_deferred_event_ts) >= worker.stream_event_cooldown_sec
        if not reason_changed and not cooldown_passed:
            return
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Position close deferred by broker allowance backoff",
            {
                "position_id": position.position_id,
                "reason": reason,
                "wait_sec": round(max(0.0, wait_sec), 3),
                "error": error_text,
            },
        )
        worker._last_close_deferred_event_ts = now
        worker._last_close_deferred_position_id = position.position_id

    def record_close_deferred_for_retry(
        self,
        position: Position,
        reason: str,
        wait_sec: float,
        error_text: str,
    ) -> None:
        worker = self._worker
        now = worker._monotonic_now()
        reason_changed = worker._last_close_deferred_position_id != position.position_id
        cooldown_passed = (now - worker._last_close_deferred_event_ts) >= worker.stream_event_cooldown_sec
        if not reason_changed and not cooldown_passed:
            return
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Position close deferred for retry",
            {
                "position_id": position.position_id,
                "reason": reason,
                "wait_sec": round(max(0.0, wait_sec), 3),
                "error": error_text,
            },
        )
        worker._last_close_deferred_event_ts = now
        worker._last_close_deferred_position_id = position.position_id

    def record_close_pending_verification(
        self,
        position: Position,
        reason: str,
        wait_sec: float,
        broker_sync: dict[str, object] | None,
    ) -> None:
        worker = self._worker
        now = worker._monotonic_now()
        reason_changed = worker._last_close_deferred_position_id != position.position_id
        cooldown_passed = (now - worker._last_close_deferred_event_ts) >= worker.stream_event_cooldown_sec
        if not reason_changed and not cooldown_passed:
            return
        payload: dict[str, object] = {
            "position_id": position.position_id,
            "reason": reason,
            "wait_sec": round(max(0.0, wait_sec), 3),
        }
        if isinstance(broker_sync, dict) and broker_sync:
            payload["broker_close_sync"] = broker_sync
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Broker close awaiting factual verification",
            payload,
        )
        worker._last_close_deferred_event_ts = now
        worker._last_close_deferred_position_id = position.position_id

    def close_verification_retry_interval_sec(self) -> float:
        worker = self._worker
        return max(
            worker.manual_close_sync_interval_sec,
            min(60.0, max(worker.poll_interval_sec * 10.0, 20.0)),
        )

    def manual_close_sync_interval_for_position(self, position: Position, now: float) -> float:
        worker = self._worker
        base_interval = max(1.0, float(worker.manual_close_sync_interval_sec))
        if worker._pending_close_verification_position_id == position.position_id:
            return base_interval
        if worker._pending_close_retry_position_id == position.position_id:
            return base_interval
        age_sec = worker._position_age_sec(
            position,
            now_monotonic=now,
            now_wall=worker._wall_time_now(),
        )
        if age_sec is None:
            return base_interval
        if age_sec >= 8.0 * 60.0 * 60.0:
            return 120.0
        if age_sec >= 2.0 * 60.0 * 60.0:
            return 10.0
        if age_sec >= 30.0 * 60.0:
            return 3.0
        return base_interval

    def arm_pending_close_verification(
        self,
        position: Position,
        reason: str,
        broker_sync: dict[str, object] | None = None,
    ) -> None:
        worker = self._worker
        now = worker._monotonic_now()
        if worker._pending_close_verification_position_id != position.position_id:
            worker._pending_close_verification_started_ts = now
        worker._pending_close_verification_position_id = position.position_id
        worker._pending_close_verification_reason = str(reason)
        retry_sec = self.close_verification_retry_interval_sec()
        worker._pending_close_retry_position_id = position.position_id
        worker._pending_close_retry_until_ts = max(
            worker._pending_close_retry_until_ts,
            now + retry_sec,
        )
        worker._pending_close_retry_reason = str(reason)
        worker._manual_close_sync_position_id = position.position_id
        worker._manual_close_sync_next_check_ts = 0.0
        self.record_close_pending_verification(
            position=position,
            reason=reason,
            wait_sec=worker._runtime_remaining_sec(
                worker._pending_close_retry_until_ts,
                now_monotonic=now,
            ),
            broker_sync=broker_sync,
        )

    def clear_pending_close_verification(self, position_id: str | None = None) -> None:
        worker = self._worker
        normalized_position_id = str(position_id or worker._pending_close_verification_position_id or "").strip()
        if not normalized_position_id:
            return
        if worker._pending_close_verification_position_id != normalized_position_id:
            return
        worker._pending_close_verification_position_id = None
        worker._pending_close_verification_reason = None
        worker._pending_close_verification_started_ts = 0.0

    def schedule_missing_position_reconciliation(
        self,
        position: Position,
        wait_sec: float,
        broker_error: str,
    ) -> None:
        worker = self._worker
        now = worker._monotonic_now()
        if (
            worker._pending_missing_position_id != position.position_id
            or worker._runtime_remaining_sec(
                worker._pending_missing_position_deadline_ts,
                now_monotonic=now,
            ) <= FLOAT_COMPARISON_TOLERANCE
        ):
            worker._pending_missing_position_deadline_ts = (
                now + worker.missing_position_reconcile_timeout_sec
            )
        worker._pending_missing_position_id = position.position_id
        worker._pending_missing_position_until_ts = now + max(1.0, wait_sec)
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Broker position missing reconciliation delayed",
            {
                "position_id": position.position_id,
                "wait_sec": round(max(1.0, wait_sec), 2),
                "deadline_sec": round(
                    worker._runtime_remaining_sec(
                        worker._pending_missing_position_deadline_ts,
                        now_monotonic=now,
                    ),
                    2,
                ),
                "broker_error": broker_error,
            },
        )

    def clear_pending_missing_position_reconciliation(self) -> None:
        worker = self._worker
        worker._pending_missing_position_id = None
        worker._pending_missing_position_until_ts = 0.0
        worker._pending_missing_position_deadline_ts = 0.0

    def attempt_pending_missing_position_reconciliation(
        self,
        position: Position,
        tick: PriceTick | None,
    ) -> bool:
        worker = self._worker
        if worker._pending_missing_position_id != position.position_id:
            return False

        now = worker._monotonic_now()
        if worker._runtime_remaining_sec(
            worker._pending_missing_position_until_ts,
            now_monotonic=now,
        ) > FLOAT_COMPARISON_TOLERANCE:
            return True

        broker_sync = self.get_broker_close_sync(position)
        if self.broker_sync_has_factual_close_details(broker_sync):
            inferred_price, inferred_reason = self.infer_missing_position_close(
                position,
                tick.bid if tick is not None else None,
                tick.ask if tick is not None else None,
            )
            sync_source = str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync"
            broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
            use_local_close_price = broker_close_price is not None and broker_close_price > 0
            self.clear_pending_missing_position_reconciliation()
            worker._finalize_position_close(
                position,
                inferred_price,
                f"broker_position_missing:delayed:{inferred_reason}:{sync_source}",
                broker_sync=broker_sync,
                use_local_close_price=use_local_close_price,
                estimate_close_price=inferred_price,
                pnl_estimate_source="missing_position_inferred_close_price",
            )
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Broker position close sync reconciled",
                {
                    "position_id": position.position_id,
                    "broker_close_sync": broker_sync,
                },
            )
            return True

        deadline_ts = worker._pending_missing_position_deadline_ts
        deadline_remaining_sec = worker._runtime_remaining_sec(
            deadline_ts,
            now_monotonic=now,
        )
        if deadline_remaining_sec > FLOAT_COMPARISON_TOLERANCE:
            backoff_remaining = worker._broker_public_api_backoff_remaining_sec()
            retry_sec = (
                backoff_remaining
                if backoff_remaining > 0
                else min(5.0, max(1.0, deadline_remaining_sec))
            )
            worker._pending_missing_position_until_ts = now + max(1.0, retry_sec)
            if backoff_remaining > 0:
                logger.warning(
                    "Broker position missing reconciliation retry delayed by allowance backoff"
                    " | symbol=%s position_id=%s wait_sec=%.2f deadline_sec=%.2f",
                    worker.symbol,
                    position.position_id,
                    retry_sec,
                    deadline_remaining_sec,
                )
            return True

        self.clear_pending_missing_position_reconciliation()
        worker._finalize_position_close(
            position,
            position.open_price,
            "broker_position_missing:timeout:pending_broker_sync",
            broker_sync={
                "position_id": position.position_id,
                "source": "broker_position_missing_timeout",
                "position_found": False,
            },
            use_local_close_price=False,
        )
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Broker position missing reconciled after timeout (close details pending broker sync)",
            {
                "position_id": position.position_id,
            },
        )
        return True

    @staticmethod
    def broker_sync_has_close_evidence(payload: dict[str, object] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        close_price = payload.get("close_price")
        realized_pnl = payload.get("realized_pnl")
        close_deal_id = str(payload.get("close_deal_id") or "").strip()
        deal_reference = str(payload.get("deal_reference") or "").strip()
        history_reference = str(payload.get("history_reference") or "").strip()
        position_found_raw = payload.get("position_found")
        position_missing = isinstance(position_found_raw, bool) and (not position_found_raw)
        return (
            close_price is not None
            or realized_pnl is not None
            or bool(close_deal_id)
            or bool(deal_reference)
            or bool(history_reference)
            or position_missing
        )

    @staticmethod
    def broker_sync_is_partial_close(payload: dict[str, object] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        close_complete = payload.get("close_complete")
        return isinstance(close_complete, bool) and (not close_complete)

    @staticmethod
    def broker_sync_has_factual_close_details(payload: dict[str, object] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        if WorkerReconcileManager.broker_sync_is_partial_close(payload):
            return False
        close_price = payload.get("close_price")
        realized_pnl = payload.get("realized_pnl")
        closed_at = payload.get("closed_at")
        return close_price is not None or realized_pnl is not None or closed_at is not None

    def maybe_reconcile_execution_manual_close(
        self,
        position: Position,
        tick: PriceTick | None,
    ) -> bool:
        worker = self._worker
        if worker.mode != RunMode.EXECUTION:
            return False
        if worker._pending_missing_position_id == position.position_id:
            return False

        now = worker._monotonic_now()
        if worker._manual_close_sync_position_id != position.position_id:
            worker._manual_close_sync_position_id = position.position_id
            worker._manual_close_sync_next_check_ts = 0.0
        if worker._runtime_remaining_sec(
            worker._manual_close_sync_next_check_ts,
            now_monotonic=now,
        ) > FLOAT_COMPARISON_TOLERANCE:
            return False

        allowance_backoff_remaining = worker._broker_public_api_backoff_remaining_sec()
        if allowance_backoff_remaining > 0:
            worker._manual_close_sync_next_check_ts = now + max(1.0, allowance_backoff_remaining)
            return False

        next_poll_interval_sec = self.manual_close_sync_interval_for_position(position, now)
        worker._manual_close_sync_next_check_ts = now + next_poll_interval_sec
        broker_sync = self.get_broker_close_sync(position, include_history=False)
        if not self.broker_sync_has_close_evidence(broker_sync):
            return False
        if self.broker_sync_is_partial_close(broker_sync):
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Ignoring partial broker close sync for active position",
                {
                    "position_id": position.position_id,
                    "closed_volume": worker._safe_float((broker_sync or {}).get("closed_volume")),
                    "broker_close_sync": broker_sync,
                },
            )
            return False
        if not self.broker_sync_has_factual_close_details(broker_sync):
            if not self.broker_sync_indicates_position_missing(broker_sync):
                return False
            history_broker_sync = self.get_broker_close_sync(position, include_history=True)
            if isinstance(history_broker_sync, dict):
                if self.broker_sync_is_partial_close(history_broker_sync):
                    worker.store.record_event(
                        "INFO",
                        worker.symbol,
                        "Ignoring partial broker close sync for active position",
                        {
                            "position_id": position.position_id,
                            "closed_volume": worker._safe_float((history_broker_sync or {}).get("closed_volume")),
                            "broker_close_sync": history_broker_sync,
                        },
                    )
                    return False
                if self.broker_sync_has_factual_close_details(history_broker_sync):
                    broker_sync = history_broker_sync
                elif not self.broker_sync_indicates_position_missing(history_broker_sync):
                    return False
        else:
            history_broker_sync = self.get_broker_close_sync(position, include_history=True)
            if isinstance(history_broker_sync, dict) and self.broker_sync_has_factual_close_details(history_broker_sync):
                broker_sync = history_broker_sync

        # If a pending close verification reason exists (from the original
        # _close_position call), preserve it so the real trigger is visible in
        # trade history instead of a generic "broker_manual_close" label.
        pending_reason = (
            worker._pending_close_verification_reason
            if worker._pending_close_verification_position_id == position.position_id
            else None
        )

        if not self.broker_sync_has_factual_close_details(broker_sync):
            bid = tick.bid if tick is not None else None
            ask = tick.ask if tick is not None else None
            inferred_price, inferred_reason = self.infer_missing_position_close(position, bid, ask)
            broker_source = str((broker_sync or {}).get("source") or "").strip().lower() or "unknown"
            close_reason = (
                f"close_verified:{pending_reason}:pending_broker_sync:{broker_source}"
                if pending_reason
                else f"broker_manual_close:{inferred_reason}:pending_broker_sync:{broker_source}"
            )
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Broker close detected without factual details; using inferred close price",
                {
                    "position_id": position.position_id,
                    "inferred_reason": inferred_reason,
                    "inferred_close_price": inferred_price,
                    "original_close_reason": pending_reason,
                    "broker_close_sync": broker_sync,
                },
            )
            self.clear_pending_missing_position_reconciliation()
            worker._finalize_position_close(
                position,
                inferred_price,
                close_reason,
                broker_sync=broker_sync,
                use_local_close_price=False,
                estimate_close_price=inferred_price,
                pnl_estimate_source="manual_reconcile_inferred_close_price",
            )
            self.clear_pending_close_verification(position.position_id)
            return True

        broker_source = str((broker_sync or {}).get("source") or "").strip().lower() or "unknown"
        bid = tick.bid if tick is not None else None
        ask = tick.ask if tick is not None else None
        inferred_price, inferred_reason = self.infer_missing_position_close(position, bid, ask)
        broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
        use_local_close_price = broker_close_price is not None and broker_close_price > 0
        close_reason = (
            f"close_verified:{pending_reason}:{broker_source}"
            if pending_reason
            else f"broker_manual_close:{inferred_reason}:{broker_source}"
        )
        self.clear_pending_missing_position_reconciliation()
        worker._finalize_position_close(
            position,
            inferred_price,
            close_reason,
            broker_sync=broker_sync,
            use_local_close_price=use_local_close_price,
            estimate_close_price=inferred_price,
            pnl_estimate_source=(
                "manual_reconcile_inferred_close_price"
                if not use_local_close_price
                else "broker_close_price"
            ),
        )
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Closed trade details backfilled from broker sync",
            {
                "position_id": position.position_id,
                "close_price": broker_sync.get("close_price"),
                "realized_pnl": broker_sync.get("realized_pnl"),
                "source": broker_source,
            },
        )
        self.clear_pending_close_verification(position.position_id)
        return True

    def handle_missing_broker_position_error(
        self,
        error_text: str,
        tick: PriceTick | None,
    ) -> bool:
        worker = self._worker
        if worker.mode != RunMode.EXECUTION:
            return False
        if not self.is_broker_position_missing_error(error_text):
            return False

        active = worker.position_book.get(worker.symbol)
        if active is None:
            return False

        broker_sync = self.get_broker_close_sync(active)
        bid = tick.bid if tick is not None else None
        ask = tick.ask if tick is not None else None
        inferred_price, inferred_reason = self.infer_missing_position_close(active, bid, ask)
        if not self.broker_sync_has_factual_close_details(broker_sync):
            broker_backoff_remaining = worker._broker_public_api_backoff_remaining_sec()
            wait_sec = (
                broker_backoff_remaining
                if broker_backoff_remaining > 0
                else min(5.0, max(1.0, worker.manual_close_sync_interval_sec / 4.0))
            )
            self.schedule_missing_position_reconciliation(
                active,
                wait_sec,
                error_text,
            )
            logger.warning(
                "Broker position missing reconciliation delayed (awaiting factual close sync)"
                " | symbol=%s position_id=%s wait_sec=%.2f",
                worker.symbol,
                active.position_id,
                wait_sec,
            )
            return True
        sync_source = str((broker_sync or {}).get("source") or "").strip().lower()
        if sync_source:
            final_reason = f"broker_position_missing:{inferred_reason}:{sync_source}"
        else:
            final_reason = f"broker_position_missing:{inferred_reason}"
        broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
        use_local_close_price = broker_close_price is not None and broker_close_price > 0
        worker._finalize_position_close(
            active,
            inferred_price,
            final_reason,
            broker_sync=broker_sync,
            use_local_close_price=use_local_close_price,
            estimate_close_price=inferred_price,
            pnl_estimate_source=(
                "missing_position_inferred_close_price"
                if not use_local_close_price
                else "broker_close_price"
            ),
        )
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Broker position missing reconciled locally",
            {
                "position_id": active.position_id,
                "inferred_reason": inferred_reason,
                "inferred_close_price": inferred_price,
                "close_price_pending_broker_sync": not use_local_close_price,
                "broker_close_sync": broker_sync,
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Broker position missing reconciled locally | symbol=%s position_id=%s reason=%s",
            worker.symbol,
            active.position_id,
            inferred_reason,
        )
        return True

    def infer_missing_position_close(
        self,
        position: Position,
        bid: float | None,
        ask: float | None,
    ) -> tuple[float, str]:
        worker = self._worker
        tick_size = worker.symbol_spec.tick_size if worker.symbol_spec is not None else worker._pip_size_fallback()
        tolerance = max(tick_size * 2.0, FLOAT_COMPARISON_TOLERANCE)

        if bid is not None and ask is not None:
            mark = bid if position.side == Side.BUY else ask
        else:
            mark = position.open_price

        if position.side == Side.BUY:
            if mark <= (position.stop_loss + tolerance):
                return position.stop_loss, "stop_loss"
            if mark >= (position.take_profit - tolerance):
                return position.take_profit, "take_profit"
            return mark, "mark"

        if mark >= (position.stop_loss - tolerance):
            return position.stop_loss, "stop_loss"
        if mark <= (position.take_profit + tolerance):
            return position.take_profit, "take_profit"
        return mark, "mark"

    def validate_broker_close_sync(
        self,
        position: Position,
        payload: dict[str, object],
        *,
        expected_deal_reference: str | None = None,
    ) -> dict[str, object] | None:
        worker = self._worker
        closed_at = worker._safe_float(payload.get("closed_at"))
        source = str(payload.get("source") or "").strip().lower()
        match_mode = str(payload.get("history_match_mode") or "").strip().lower()
        expected_reference = worker._normalize_deal_reference(str(expected_deal_reference or "").strip()) if expected_deal_reference else ""
        payload_reference_raw = str(
            payload.get("deal_reference") or payload.get("history_reference") or ""
        ).strip()
        payload_reference = worker._normalize_deal_reference(payload_reference_raw) if payload_reference_raw else ""

        if (
            closed_at is not None
            and closed_at > 0
            and position.opened_at > 0
            and (closed_at + 2.0) < float(position.opened_at)
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Broker close sync rejected as stale",
                {
                    "position_id": position.position_id,
                    "source": source,
                    "history_match_mode": match_mode or None,
                    "broker_closed_at": closed_at,
                    "position_opened_at": float(position.opened_at),
                },
            )
            return None

        if source == "ig_history_transactions" and expected_reference:
            strict_reference_required = match_mode != "position_id"
            if strict_reference_required and not payload_reference:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Broker close sync rejected due to missing deal reference",
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "history_match_mode": match_mode,
                        "expected_deal_reference": expected_reference,
                    },
                )
                return None
            if strict_reference_required and payload_reference != expected_reference:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Broker close sync rejected by deal reference mismatch",
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "history_match_mode": match_mode,
                        "expected_deal_reference": expected_reference,
                        "payload_deal_reference": payload_reference,
                    },
                )
                return None

        close_price = worker._safe_float(payload.get("close_price"))
        realized_pnl = worker._safe_float(payload.get("realized_pnl"))
        enforce_pnl_sign_check = source.startswith("ig_history_")
        if (
            enforce_pnl_sign_check
            and close_price is not None
            and close_price > 0
            and realized_pnl is not None
            and abs(realized_pnl) >= 1.0
        ):
            normalized_close = worker._normalize_price(close_price)
            local_pnl_from_close = worker._calculate_pnl(position, normalized_close)
            if abs(local_pnl_from_close) >= 1.0 and (local_pnl_from_close * realized_pnl) < 0:
                sanitized_payload = dict(payload)
                sanitized_payload.pop("close_price", None)
                sanitized_payload["close_price_rejected_reason"] = "pnl_sign_mismatch"
                sanitized_payload["local_pnl_from_close_price"] = local_pnl_from_close
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Broker close sync close_price rejected by pnl sign mismatch",
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "history_match_mode": match_mode or None,
                        "broker_close_price": close_price,
                        "normalized_close_price": normalized_close,
                        "broker_realized_pnl": realized_pnl,
                        "local_pnl_from_close_price": local_pnl_from_close,
                    },
                )
                return sanitized_payload

        return payload

    def get_broker_close_sync(
        self,
        position: Position,
        *,
        include_history: bool = True,
    ) -> dict[str, object] | None:
        worker = self._worker
        getter = getattr(worker.broker, "get_position_close_sync", None)
        if not callable(getter):
            return None
        expected_deal_reference = worker.store.get_trade_deal_reference(position.position_id)
        if expected_deal_reference:
            expected_deal_reference = worker._normalize_deal_reference(expected_deal_reference)
        try:
            payload = call_broker_method_with_supported_kwargs(
                worker.broker,
                "get_position_close_sync",
                getter,
                position.position_id,
                deal_reference=expected_deal_reference,
                symbol=position.symbol,
                opened_at=position.opened_at,
                open_price=position.open_price,
                volume=position.volume,
                side=position.side.value,
                tick_size=worker.symbol_spec.tick_size if worker.symbol_spec is not None else None,
                include_history=bool(include_history),
            )
        except Exception as exc:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Broker close sync fetch failed",
                {"position_id": position.position_id, "error": str(exc)},
            )
            return None
        if not isinstance(payload, dict):
            return None
        return self.validate_broker_close_sync(
            position,
            payload,
            expected_deal_reference=expected_deal_reference,
        )

    @staticmethod
    def broker_sync_indicates_position_missing(payload: dict[str, object] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        position_found = payload.get("position_found")
        return isinstance(position_found, bool) and (position_found is False)

    def get_broker_open_sync(self, position_id: str) -> dict[str, object] | None:
        worker = self._worker
        getter = getattr(worker.broker, "get_position_open_sync", None)
        if not callable(getter):
            return None
        try:
            payload = getter(position_id)
        except Exception as exc:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Broker open sync fetch failed",
                {"position_id": position_id, "error": str(exc)},
            )
            return None
        if not isinstance(payload, dict):
            return None
        return payload
