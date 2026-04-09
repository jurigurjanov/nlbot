from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from xtb_bot.bot.broker_state import BotBrokerStateRuntime
from xtb_bot.broker_method_support import call_broker_method_with_supported_kwargs
from xtb_bot.models import Position, RunMode
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotCloseDetailsRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        self._closed_trade_details_retry_after_monotonic: dict[str, float] = {}
        self._closed_trade_details_retry_backoff_sec: dict[str, float] = {}

    @staticmethod
    def _trade_event_matches_position(payload: dict[str, object] | None, position_id: str) -> bool:
        if not isinstance(payload, dict):
            return False
        return str(payload.get("position_id") or "").strip() == str(position_id or "").strip()


    def _best_close_details_from_events(self, position: Position) -> dict[str, object] | None:
        event_rows = self._bot.store.load_trade_close_events(
            symbol=position.symbol,
            position_id=position.position_id,
            opened_at=position.opened_at,
            closed_at=position.closed_at,
        )
        candidates: list[dict[str, object]] = []
        for event in event_rows:
            message = str(event.get("message") or "").strip()
            is_close_event = (
                message in {
                    "Position closed",
                    "Closed trade details backfilled from broker sync",
                    "Closed trade details repaired from stored close events",
                    "Manual broker close reconciled",
                    "Manual broker close reconciled (details pending broker sync)",
                }
                or message.startswith("Local open position reconciled as closed during ")
            )
            if not is_close_event:
                continue
            payload = event.get("payload")
            if not self._trade_event_matches_position(payload if isinstance(payload, dict) else None, position.position_id):
                continue
            payload_dict = payload if isinstance(payload, dict) else {}
            broker_sync = payload_dict.get("broker_close_sync")
            broker_sync_dict = broker_sync if isinstance(broker_sync, dict) else {}
            close_price = (
                BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("close_price"))
                or BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("close_price"))
                or BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("inferred_close_price"))
            )
            pnl_currency = (
                BotBrokerStateRuntime.normalize_currency_code(broker_sync_dict.get("pnl_currency"))
                or BotBrokerStateRuntime.normalize_currency_code(payload_dict.get("pnl_currency"))
            )
            realized_pnl = (
                BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("realized_pnl"))
                or BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("realized_pnl"))
            )
            pnl_value: float | None = realized_pnl
            pnl_is_estimated = False
            pnl_estimate_source: str | None = None
            payload_pnl = BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("pnl"))
            inferred_close_price = BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("inferred_close_price"))
            if pnl_value is None and payload_pnl is not None and abs(payload_pnl) > FLOAT_COMPARISON_TOLERANCE:
                pnl_value = payload_pnl
                pnl_is_estimated = bool(payload_dict.get("pnl_is_estimated"))
                pnl_estimate_source = (
                    str(payload_dict.get("pnl_estimate_source") or "").strip() or None
                )
            local_pnl_estimate = BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("local_pnl_estimate"))
            if pnl_value is None and local_pnl_estimate is not None and abs(local_pnl_estimate) > FLOAT_COMPARISON_TOLERANCE:
                pnl_value = local_pnl_estimate
                pnl_is_estimated = True
                pnl_estimate_source = (
                    str(payload_dict.get("pnl_estimate_source") or "").strip()
                    or "event_local_pnl_estimate"
                )
            if pnl_value is None and close_price is not None:
                estimated_pnl, _ = self._bot._broker_state.estimate_position_pnl_from_close_price(
                    position,
                    close_price,
                    pnl_currency,
                )
                if estimated_pnl is not None:
                    pnl_value = estimated_pnl
                    pnl_is_estimated = True
                    pnl_estimate_source = (
                        str(payload_dict.get("pnl_estimate_source") or "").strip()
                        or (
                            "event_inferred_close_price"
                            if inferred_close_price is not None
                            else "event_close_price"
                        )
                    )
            event_closed_at = BotBrokerStateRuntime.finite_float_or_none(event.get("ts"))
            event_has_close_details = (
                close_price is not None
                or realized_pnl is not None
                or payload_pnl is not None
                or local_pnl_estimate is not None
                or inferred_close_price is not None
            )
            closed_at = (
                BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("closed_at"))
                or BotBrokerStateRuntime.finite_float_or_none(payload_dict.get("broker_closed_at"))
                or (event_closed_at if event_has_close_details else None)
            )
            if close_price is None and closed_at is None and pnl_value is None:
                continue
            candidates.append(
                {
                    "close_price": close_price,
                    "closed_at": closed_at,
                    "pnl": pnl_value,
                    "pnl_is_estimated": pnl_is_estimated,
                    "pnl_estimate_source": pnl_estimate_source,
                    "pnl_currency": pnl_currency,
                    "event_ts": BotBrokerStateRuntime.finite_float_or_none(event.get("ts")) or 0.0,
                    "has_realized_pnl": realized_pnl is not None,
                }
            )
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                1 if bool(item.get("has_realized_pnl")) else 0,
                1 if BotBrokerStateRuntime.finite_float_or_none(item.get("pnl")) is not None else 0,
                1 if BotBrokerStateRuntime.finite_float_or_none(item.get("close_price")) is not None else 0,
                1 if BotBrokerStateRuntime.finite_float_or_none(item.get("closed_at")) is not None else 0,
                float(item.get("event_ts") or 0.0),
            ),
        )


    def _resolved_close_details(
        self,
        position: Position,
        broker_sync: dict[str, object] | None,
    ) -> dict[str, object]:
        broker_sync_dict = broker_sync if isinstance(broker_sync, dict) else {}
        event_details = self._best_close_details_from_events(position)
        close_price = BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("close_price"))
        if close_price is None and isinstance(event_details, dict):
            close_price = BotBrokerStateRuntime.finite_float_or_none(event_details.get("close_price"))
        closed_at = BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("closed_at"))
        if closed_at is None and isinstance(event_details, dict):
            closed_at = BotBrokerStateRuntime.finite_float_or_none(event_details.get("closed_at"))
        realized_pnl = BotBrokerStateRuntime.finite_float_or_none(broker_sync_dict.get("realized_pnl"))
        pnl_value = realized_pnl
        pnl_is_estimated = False
        pnl_estimate_source: str | None = None
        pnl_currency = BotBrokerStateRuntime.normalize_currency_code(broker_sync_dict.get("pnl_currency"))
        if pnl_currency is None and isinstance(event_details, dict):
            pnl_currency = BotBrokerStateRuntime.normalize_currency_code(event_details.get("pnl_currency"))
        if pnl_value is None and isinstance(event_details, dict):
            event_pnl = BotBrokerStateRuntime.finite_float_or_none(event_details.get("pnl"))
            if event_pnl is not None:
                pnl_value = event_pnl
                pnl_is_estimated = bool(event_details.get("pnl_is_estimated"))
                pnl_estimate_source = str(event_details.get("pnl_estimate_source") or "").strip() or None
        if pnl_value is None and close_price is not None:
            estimated_pnl, _ = self._bot._broker_state.estimate_position_pnl_from_close_price(position, close_price, pnl_currency)
            if estimated_pnl is not None:
                pnl_value = estimated_pnl
                pnl_is_estimated = True
                pnl_estimate_source = "broker_close_price"
        return {
            "close_price": close_price,
            "closed_at": closed_at,
            "pnl": pnl_value,
            "pnl_is_estimated": pnl_is_estimated,
            "pnl_estimate_source": pnl_estimate_source,
            "pnl_currency": pnl_currency,
            "has_realized_pnl": realized_pnl is not None,
        }


    @staticmethod
    def _broker_close_sync_has_evidence(payload: dict[str, object] | None) -> bool:
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


    def _get_broker_close_sync(
        self,
        position: Position,
        *,
        context: str,
        wait_timeout_sec: float = 0.2,
    ) -> dict[str, object] | None:
        self._bot._pulse_runtime_monitor_progress()
        getter = getattr(self._bot.broker, "get_position_close_sync", None)
        if not callable(getter):
            return None
        if not self._bot._reserve_ig_non_trading_budget(
            scope="position_close_sync",
            wait_timeout_sec=max(0.0, float(wait_timeout_sec)),
        ):
            return None
        expected_deal_reference = self._bot.store.get_trade_deal_reference(position.position_id)
        try:
            payload = call_broker_method_with_supported_kwargs(
                self._bot.broker,
                "get_position_close_sync",
                getter,
                position.position_id,
                deal_reference=expected_deal_reference,
                symbol=position.symbol,
                opened_at=position.opened_at,
                open_price=position.open_price,
                volume=position.volume,
                side=position.side.value,
            )
        except Exception as exc:
            self._bot._pulse_runtime_monitor_progress()
            self._bot.store.record_event(
                "WARN",
                position.symbol,
                "Broker close sync fetch failed",
                {
                    "position_id": position.position_id,
                    "context": context,
                    "error": str(exc),
                },
            )
            return None
        self._bot._pulse_runtime_monitor_progress()
        if not isinstance(payload, dict):
            return None
        return payload


    def _reconcile_missing_local_position_from_broker_sync(
        self,
        position: Position,
        *,
        context: str,
        now_ts: float,
    ) -> bool:
        broker_sync = self._bot._get_broker_close_sync(
            position,
            context=context,
            wait_timeout_sec=0.75,
        )
        if not self._broker_close_sync_has_evidence(broker_sync):
            close_details = self._resolved_close_details(position, None)
            if (
                BotBrokerStateRuntime.finite_float_or_none(close_details.get("close_price")) is None
                and BotBrokerStateRuntime.finite_float_or_none(close_details.get("closed_at")) is None
                and BotBrokerStateRuntime.finite_float_or_none(close_details.get("pnl")) is None
            ):
                return False
        else:
            close_details = self._resolved_close_details(position, broker_sync)

        close_price = BotBrokerStateRuntime.finite_float_or_none(close_details.get("close_price"))
        if close_price is not None and close_price <= 0:
            close_price = None
        closed_at_raw = BotBrokerStateRuntime.finite_float_or_none(close_details.get("closed_at"))
        closed_at = min(now_ts, closed_at_raw) if closed_at_raw is not None and closed_at_raw > 0 else None
        pnl_value = BotBrokerStateRuntime.finite_float_or_none(close_details.get("pnl"))
        pnl_is_estimated = bool(close_details.get("pnl_is_estimated")) if pnl_value is not None else None
        pnl_estimate_source = close_details.get("pnl_estimate_source")
        has_close_details = (close_price is not None) or (closed_at is not None) or (pnl_value is not None)

        # If the worker already persisted a close_reason (from the original
        # _close_position call), prefer it over the generic "broker_reconcile"
        # label so the real exit trigger is preserved in trade history.
        existing_perf = self._bot.store.load_trade_performance(position.position_id)
        existing_reason = str((existing_perf or {}).get("close_reason") or "").strip() or None

        if has_close_details:
            self._bot.store.update_trade_status(
                position.position_id,
                status="closed",
                close_price=close_price,
                closed_at=closed_at,
                pnl=pnl_value,
                pnl_is_estimated=pnl_is_estimated,
                pnl_estimate_source=pnl_estimate_source,
            )
            reconcile_reason = (
                f"close_verified:{existing_reason}:{context}"
                if existing_reason
                else f"broker_reconcile:{context}:closed"
            )
            self._bot.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=closed_at,
                close_reason=reconcile_reason,
            )
        else:
            # Position absence on broker confirms closure, but we keep existing close fields untouched
            # when broker history does not provide execution details yet.
            preserved_closed_at = BotBrokerStateRuntime.finite_float_or_none(position.closed_at)
            if preserved_closed_at is not None and preserved_closed_at > 0:
                preserved_closed_at = min(now_ts, preserved_closed_at)
            else:
                preserved_closed_at = None
            self._bot.store.update_trade_status(
                position.position_id,
                status="closed",
            )
            reconcile_reason = (
                f"close_verified:{existing_reason}:{context}"
                if existing_reason
                else f"broker_reconcile:{context}:closed_pending_details"
            )
            self._bot.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=preserved_closed_at,
                close_reason=reconcile_reason,
            )
        source = str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync"
        self._bot.store.record_event(
            "INFO",
            position.symbol,
            f"Local open position reconciled as closed during {context}",
            {
                "position_id": position.position_id,
                "source": source,
                "broker_close_sync": broker_sync,
            },
        )
        logger.info(
            "Local open position reconciled as closed during %s | symbol=%s position_id=%s source=%s",
            context,
            position.symbol,
            position.position_id,
            source,
        )
        return True


    def _backfill_closed_trade_details(self) -> int:
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return 0
        if self._bot._broker_public_api_backoff_remaining_sec() > 0.0:
            return 0
        now_ts = time.time()
        now_monotonic = time.monotonic()
        max_passes = self._bot._close_reconcile_max_passes if self._bot._close_reconcile_enabled else 1
        reconciled_count_total = 0
        mismatch_alert_count = 0
        for pass_idx in range(max_passes):
            self._bot._pulse_runtime_monitor_progress()
            closed_positions = self._bot.store.load_positions_by_status(
                "closed",
                mode=self._bot.config.mode.value,
            )
            if not closed_positions:
                break
            pass_reconciled = 0
            pending_count = 0
            for position in closed_positions.values():
                self._bot._pulse_runtime_monitor_progress()
                close_price_existing = BotBrokerStateRuntime.finite_float_or_none(position.close_price)
                closed_at_existing = BotBrokerStateRuntime.finite_float_or_none(position.closed_at)
                pnl_existing = BotBrokerStateRuntime.finite_float_or_none(position.pnl)
                has_good_close_details = (
                    close_price_existing is not None
                    and close_price_existing > 0
                    and closed_at_existing is not None
                    and closed_at_existing > 0
                )
                looks_like_inferred_open_snapshot = (
                    has_good_close_details
                    and abs(float(close_price_existing) - float(position.open_price)) <= max(
                        FLOAT_COMPARISON_TOLERANCE,
                        abs(float(position.open_price)) * FLOAT_COMPARISON_TOLERANCE,
                    )
                    and abs(float(closed_at_existing) - float(position.opened_at)) <= 2.0
                    and (pnl_existing is not None and abs(float(pnl_existing)) > FLOAT_COMPARISON_TOLERANCE)
                )
                has_non_zero_pnl = pnl_existing is not None and abs(float(pnl_existing)) > FLOAT_COMPARISON_TOLERANCE
                should_probe = (
                    (not has_good_close_details)
                    or (not has_non_zero_pnl)
                    or looks_like_inferred_open_snapshot
                )
                if not should_probe:
                    self._closed_trade_details_retry_after_monotonic.pop(position.position_id, None)
                    self._closed_trade_details_retry_backoff_sec.pop(position.position_id, None)
                    continue
                if pass_idx > 0:
                    pending_count += 1
                    continue
                if not self._closed_trade_details_retry_due(position.position_id, now_monotonic):
                    pending_count += 1
                    continue
                broker_sync = self._bot._get_broker_close_sync(
                    position,
                    context="closed trade details backfill",
                    wait_timeout_sec=2.0,
                )
                if not isinstance(broker_sync, dict):
                    broker_sync = None
                close_details = self._resolved_close_details(position, broker_sync)
                close_price = BotBrokerStateRuntime.finite_float_or_none(close_details.get("close_price"))
                if close_price is not None and close_price <= 0:
                    close_price = None
                closed_at = BotBrokerStateRuntime.finite_float_or_none(close_details.get("closed_at"))
                if closed_at is not None and closed_at > 0:
                    closed_at = min(now_ts, closed_at)
                else:
                    closed_at = None
                pnl_value = BotBrokerStateRuntime.finite_float_or_none(close_details.get("pnl"))
                pnl_is_estimated = bool(close_details.get("pnl_is_estimated")) if pnl_value is not None else None
                pnl_estimate_source = close_details.get("pnl_estimate_source")
                has_realized_pnl = bool(close_details.get("has_realized_pnl"))
                if close_price is None and closed_at is None and pnl_value is None:
                    pending_count += 1
                    self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                    continue
                if (
                    has_realized_pnl
                    and pnl_value is not None
                    and pnl_existing is not None
                    and abs(float(pnl_value) - float(pnl_existing)) > self._bot._close_reconcile_pnl_alert_threshold
                ):
                    mismatch_alert_count += 1
                    self._bot.store.record_event(
                        "WARN",
                        position.symbol,
                        "Trade pnl mismatch vs broker sync",
                        {
                            "position_id": position.position_id,
                            "local_pnl": float(pnl_existing),
                            "broker_pnl": float(pnl_value),
                            "pnl_abs_diff": round(abs(float(pnl_value) - float(pnl_existing)), 6),
                            "threshold": self._bot._close_reconcile_pnl_alert_threshold,
                            "source": str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync",
                        },
                    )
                changed = False
                if close_price is not None and (
                    close_price_existing is None
                    or abs(float(close_price_existing) - float(close_price)) > max(FLOAT_COMPARISON_TOLERANCE, abs(float(close_price)) * FLOAT_COMPARISON_TOLERANCE)
                ):
                    changed = True
                if closed_at is not None and (
                    closed_at_existing is None or abs(float(closed_at_existing) - float(closed_at)) > 1.0
                ):
                    changed = True
                if pnl_value is not None and (
                    pnl_existing is None or abs(float(pnl_existing) - float(pnl_value)) > FLOAT_COMPARISON_TOLERANCE
                ):
                    changed = True
                existing_pnl_estimated = bool(getattr(position, "pnl_is_estimated", False))
                existing_pnl_estimate_source = str(getattr(position, "pnl_estimate_source", "") or "").strip() or None
                if pnl_is_estimated is not None and existing_pnl_estimated != pnl_is_estimated:
                    changed = True
                if pnl_is_estimated and existing_pnl_estimate_source != (str(pnl_estimate_source or "").strip() or None):
                    changed = True
                incomplete_monetary_close = not has_realized_pnl
                if not changed:
                    if incomplete_monetary_close:
                        pending_count += 1
                        self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                    continue
                self._bot.store.update_trade_status(
                    position.position_id,
                    status="closed",
                    close_price=close_price,
                    closed_at=closed_at,
                    pnl=pnl_value,
                    pnl_is_estimated=pnl_is_estimated,
                    pnl_estimate_source=pnl_estimate_source,
                )
                self._bot.store.finalize_trade_performance(
                    position_id=position.position_id,
                    symbol=position.symbol,
                    closed_at=closed_at,
                    close_reason="closed_trade_backfill",
                )
                source = str((broker_sync or {}).get("source") or "").strip().lower()
                if not source:
                    source = str(pnl_estimate_source or "").strip().lower() or "event_repair"
                self._bot.store.record_event(
                    "INFO",
                    position.symbol,
                    (
                        "Closed trade details backfilled from broker sync"
                        if isinstance(broker_sync, dict)
                        else "Closed trade details repaired from stored close events"
                    ),
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "broker_close_sync": broker_sync,
                        "pnl": pnl_value,
                        "pnl_is_estimated": pnl_is_estimated,
                        "pnl_estimate_source": pnl_estimate_source,
                    },
                )
                if incomplete_monetary_close:
                    pending_count += 1
                    self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                else:
                    self._closed_trade_details_retry_after_monotonic.pop(position.position_id, None)
                    self._closed_trade_details_retry_backoff_sec.pop(position.position_id, None)
                pass_reconciled += 1

            reconciled_count_total += pass_reconciled
            if not self._bot._close_reconcile_enabled or pending_count <= 0 or pass_reconciled <= 0:
                break

        if reconciled_count_total > 0:
            self._bot.store.record_event(
                "INFO",
                None,
                "Closed trade details backfilled",
                {"count": reconciled_count_total, "mode": self._bot.config.mode.value},
            )
        if reconciled_count_total > 0 or mismatch_alert_count > 0:
            self._bot.store.record_event(
                "INFO" if mismatch_alert_count == 0 else "WARN",
                None,
                "Closed trade reconcile summary",
                {
                    "count": reconciled_count_total,
                    "pnl_mismatch_alerts": mismatch_alert_count,
                    "mode": self._bot.config.mode.value,
                },
            )
        return reconciled_count_total


    def _closed_trade_details_retry_due(self, position_id: str, now_monotonic: float) -> bool:
        retry_after = float(self._closed_trade_details_retry_after_monotonic.get(str(position_id), 0.0))
        return retry_after <= 0.0 or now_monotonic + FLOAT_COMPARISON_TOLERANCE >= retry_after


    def _schedule_closed_trade_details_retry(self, position_id: str, now_monotonic: float) -> None:
        key = str(position_id)
        base_backoff = max(
            self._bot._position_sync._runtime_closed_details_backfill_interval_sec,
            self._bot._position_sync._runtime_broker_sync_active_interval_sec,
            60.0,
        )
        previous = float(self._closed_trade_details_retry_backoff_sec.get(key, 0.0))
        next_backoff = base_backoff if previous <= 0.0 else min(900.0, previous * 2.0)
        self._closed_trade_details_retry_backoff_sec[key] = next_backoff
        self._closed_trade_details_retry_after_monotonic[key] = now_monotonic + next_backoff


    def _backfill_missing_on_broker_trades(self) -> int:
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return 0
        if self._bot._broker_public_api_backoff_remaining_sec() > 0.0:
            return 0
        missing_positions = self._bot.store.load_positions_by_status(
            "missing_on_broker",
            mode=self._bot.config.mode.value,
        )
        if not missing_positions:
            return 0
        now_ts = time.time()
        reconciled_count = 0
        for position in missing_positions.values():
            self._bot._pulse_runtime_monitor_progress()
            if self._bot._reconcile_missing_local_position_from_broker_sync(
                position,
                context="missing_on_broker backfill",
                now_ts=now_ts,
            ):
                reconciled_count += 1
        if reconciled_count > 0:
            self._bot.store.record_event(
                "INFO",
                None,
                "Missing-on-broker trades reconciled as closed",
                {"count": reconciled_count, "mode": self._bot.config.mode.value},
            )
        return reconciled_count



