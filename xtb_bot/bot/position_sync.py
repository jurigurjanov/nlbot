from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING

from xtb_bot.models import PendingOpen, Position, RunMode

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotPositionSyncRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        runtime_broker_sync_interval_sec = max(
            1.0,
            bot._env_float("XTB_RUNTIME_BROKER_SYNC_INTERVAL_SEC", 30.0),
        )
        self._runtime_broker_sync_idle_interval_sec = max(
            300.0,
            runtime_broker_sync_interval_sec,
        )
        self._runtime_broker_sync_active_interval_sec = max(
            1.0,
            bot._env_float("XTB_RUNTIME_BROKER_SYNC_ACTIVE_INTERVAL_SEC", 8.0),
        )
        self._runtime_missing_backfill_interval_sec = max(
            1.0,
            bot._env_float(
                "XTB_RUNTIME_MISSING_BACKFILL_INTERVAL_SEC",
                self._runtime_broker_sync_active_interval_sec,
            ),
        )
        self._runtime_closed_details_backfill_interval_sec = max(
            5.0,
            bot._env_float("XTB_RUNTIME_CLOSED_DETAILS_BACKFILL_INTERVAL_SEC", 60.0),
        )
        self._last_runtime_broker_sync_monotonic = 0.0
        self._last_runtime_broker_sync_error_monotonic = 0.0
        self._last_runtime_missing_backfill_monotonic = 0.0
        self._last_runtime_closed_details_backfill_monotonic = 0.0

    def _has_local_open_execution_positions(self) -> bool:
        if self._bot.position_book.count() > 0:
            return True
        if self._bot.config.mode != RunMode.EXECUTION:
            return False
        if self._bot.config.broker != "ig":
            return False
        try:
            return bool(self._bot.store.load_open_positions(mode=self._bot.config.mode.value))
        except Exception:
            return False

    def _runtime_sync_interval_sec(
        self,
        *,
        has_local_open_positions: bool | None = None,
        has_pending_opens: bool = False,
        oldest_open_age_sec: float | None = None,
    ) -> float:
        if has_local_open_positions is None:
            has_local_open_positions = self._has_local_open_execution_positions()
        if bool(has_local_open_positions) or bool(has_pending_opens):
            if has_pending_opens:
                return self._runtime_broker_sync_active_interval_sec
            age_sec = float(oldest_open_age_sec or 0.0)
            if age_sec >= 8.0 * 60.0 * 60.0:
                return 120.0
            if age_sec >= 2.0 * 60.0 * 60.0:
                return 10.0
            if age_sec >= 30.0 * 60.0:
                return 3.0
            return self._runtime_broker_sync_active_interval_sec
        return self._runtime_broker_sync_idle_interval_sec

    def _runtime_backfill_missing_on_broker_trades(self, force: bool = False) -> int:
        if self._bot.stop_event.is_set():
            return 0
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return 0
        now_monotonic = time.monotonic()
        if (
            not force
            and (now_monotonic - self._last_runtime_missing_backfill_monotonic)
            < self._runtime_missing_backfill_interval_sec
        ):
            return 0
        self._last_runtime_missing_backfill_monotonic = now_monotonic
        return self._bot._backfill_missing_on_broker_trades()

    def _runtime_backfill_closed_trade_details(self, force: bool = False) -> int:
        if self._bot.stop_event.is_set():
            return 0
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return 0
        now_monotonic = time.monotonic()
        if (
            not force
            and (now_monotonic - self._last_runtime_closed_details_backfill_monotonic)
            < self._runtime_closed_details_backfill_interval_sec
        ):
            return 0
        self._last_runtime_closed_details_backfill_monotonic = now_monotonic
        return self._bot._backfill_closed_trade_details()

    def _runtime_sync_open_positions(self, force: bool = False) -> None:
        self._bot._pulse_runtime_monitor_progress()
        if self._bot.stop_event.is_set():
            return
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return

        now_monotonic = time.monotonic()
        preferred_symbols = self._bot._symbols_to_run()
        pending_opens = self._bot.store.load_pending_opens(mode=self._bot.config.mode.value)
        local_store_positions = self._bot.store.load_open_positions(mode=self._bot.config.mode.value)
        local_by_id: dict[str, Position] = {}
        for position in local_store_positions.values():
            local_by_id[str(position.position_id)] = position
        for position in self._bot.position_book.all_open():
            local_by_id.setdefault(str(position.position_id), position)
        oldest_open_age_sec: float | None = None
        if local_by_id:
            now_ts = time.time()
            oldest_opened_at: float | None = None
            for position in local_by_id.values():
                opened_at = self._bot._finite_float_or_none(getattr(position, "opened_at", None))
                if opened_at is None or opened_at <= 0:
                    continue
                if oldest_opened_at is None or opened_at < oldest_opened_at:
                    oldest_opened_at = opened_at
            if oldest_opened_at is not None:
                oldest_open_age_sec = max(0.0, now_ts - oldest_opened_at)
        runtime_sync_interval_sec = self._runtime_sync_interval_sec(
            has_local_open_positions=bool(local_by_id),
            has_pending_opens=bool(pending_opens),
            oldest_open_age_sec=oldest_open_age_sec,
        )
        if not force and (now_monotonic - self._last_runtime_broker_sync_monotonic) < runtime_sync_interval_sec:
            return

        if not force and self._bot._broker_public_api_backoff_remaining_sec() > 0:
            return

        self._last_runtime_broker_sync_monotonic = now_monotonic

        known_position_ids = list(local_by_id.keys())
        known_position_ids.extend(
            str(pending.position_id)
            for pending in pending_opens
            if pending.position_id and str(pending.position_id).strip()
        )

        known_deal_references = self._bot.store.load_open_trade_deal_references(mode=self._bot.config.mode.value)
        known_deal_references.extend(
            str(pending.pending_id)
            for pending in pending_opens
            if str(pending.pending_id).strip()
        )
        known_deal_references = list(dict.fromkeys(known_deal_references))
        known_position_ids = list(dict.fromkeys([value for value in known_position_ids if str(value).strip()]))

        if not self._bot._reserve_ig_non_trading_budget(
            scope="runtime_managed_open_positions",
            wait_timeout_sec=0.2,
        ):
            return
        self._bot._pulse_runtime_monitor_progress()
        try:
            broker_restored = self._bot.broker.get_managed_open_positions(
                self._bot.bot_magic_prefix,
                self._bot.bot_magic_instance,
                preferred_symbols=preferred_symbols,
                known_deal_references=known_deal_references,
                known_position_ids=known_position_ids,
                pending_opens=pending_opens,
                include_unmatched_preferred=True,
            )
        except Exception as exc:
            self._bot._pulse_runtime_monitor_progress()
            error_text = str(exc)
            if self._bot._is_allowance_related_error(error_text):
                if force or (
                    now_monotonic - self._last_runtime_broker_sync_error_monotonic
                ) >= max(60.0, runtime_sync_interval_sec):
                    logger.info("Runtime broker open-position sync deferred by allowance backoff: %s", exc)
                    self._bot.store.record_event(
                        "WARN",
                        None,
                        "Runtime broker open-position sync deferred by allowance backoff",
                        {"error": error_text, "mode": self._bot.config.mode.value},
                    )
                self._last_runtime_broker_sync_error_monotonic = now_monotonic
                return
            if force or (
                now_monotonic - self._last_runtime_broker_sync_error_monotonic
            ) >= max(60.0, runtime_sync_interval_sec):
                logger.warning("Runtime broker open-position sync failed: %s", exc)
                self._bot.store.record_event(
                    "WARN",
                    None,
                    "Runtime broker open-position sync failed",
                    {"error": error_text, "mode": self._bot.config.mode.value},
                )
            self._last_runtime_broker_sync_error_monotonic = now_monotonic
            return

        self._bot._pulse_runtime_monitor_progress()
        self._last_runtime_broker_sync_error_monotonic = 0.0
        broker_restored = self._filter_restored_positions_for_mode(broker_restored)
        broker_ids = {
            str(position.position_id).strip()
            for position in broker_restored.values()
            if str(position.position_id).strip()
        }
        stale_local_count = 0
        if local_by_id:
            now_ts = time.time()
            for position_id, local_position in list(local_by_id.items()):
                self._bot._pulse_runtime_monitor_progress()
                normalized_position_id = str(position_id).strip()
                if not normalized_position_id or normalized_position_id in broker_ids:
                    continue
                symbol = str(local_position.symbol).strip().upper() or None
                if self._bot._reconcile_missing_local_position_from_broker_sync(
                    local_position,
                    context="runtime sync",
                    now_ts=now_ts,
                ):
                    self._bot.position_book.remove_by_id(normalized_position_id)
                    local_by_id.pop(position_id, None)
                    stale_local_count += 1
                    continue
                self._bot.store.update_trade_status(
                    normalized_position_id,
                    status="missing_on_broker",
                    closed_at=now_ts,
                )
                missing_existing_perf = self._bot.store.load_trade_performance(normalized_position_id)
                missing_existing_reason = str((missing_existing_perf or {}).get("close_reason") or "").strip() or None
                missing_close_reason = (
                    f"close_verified:{missing_existing_reason}:runtime_sync"
                    if missing_existing_reason
                    else "runtime_sync:missing_on_broker"
                )
                self._bot.store.finalize_trade_performance(
                    position_id=normalized_position_id,
                    symbol=local_position.symbol,
                    closed_at=now_ts,
                    close_reason=missing_close_reason,
                )
                self._bot.position_book.remove_by_id(normalized_position_id)
                local_by_id.pop(position_id, None)
                stale_local_count += 1
                logger.warning(
                    "Local open position missing on broker during runtime sync | symbol=%s position_id=%s",
                    symbol or "-",
                    normalized_position_id,
                )
                self._bot.store.record_event(
                    "WARN",
                    symbol,
                    "Local open position missing on broker during runtime sync",
                    {
                        "position_id": normalized_position_id,
                        "mode": self._bot.config.mode.value,
                    },
                )
        if stale_local_count > 0:
            self._bot.store.record_event(
                "WARN",
                None,
                "Runtime broker open-position sync marked stale local positions",
                {"count": stale_local_count, "mode": self._bot.config.mode.value},
            )
            # Try to enrich newly missing positions with factual close details immediately.
            self._bot._runtime_backfill_missing_on_broker_trades(force=True)
        if not broker_restored:
            return

        desired_assignments = self._bot._schedule_assignments(self._bot._now_utc())
        recovered_count = 0
        for position in broker_restored.values():
            self._bot._pulse_runtime_monitor_progress()
            symbol = str(position.symbol).strip().upper()
            position_id = str(position.position_id).strip()
            if not position_id:
                continue
            if position_id in local_by_id:
                # Keep in-memory book aligned with persisted state when possible.
                active = self._bot.position_book.get_by_id(position_id)
                if active is None:
                    self._bot.position_book.upsert(position)
                continue

            existing_row = self._bot.store.get_trade_record(position_id)
            matched_pending = self._match_pending_open(position, pending_opens)

            default_assignment = desired_assignments.get(symbol)
            (
                thread_name,
                strategy,
                strategy_entry,
                strategy_entry_component,
                strategy_entry_signal,
                mode,
            ) = self._bot._resolved_recovery_trade_identity(
                symbol=symbol,
                existing_row=existing_row,
                matched_pending=matched_pending,
                default_assignment=default_assignment,
            )
            self._bot._apply_position_trade_identity(
                position,
                strategy=strategy,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )

            self._bot.store.upsert_trade(
                position,
                thread_name,
                strategy,
                mode,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            if matched_pending is not None:
                conflicting_position_id = self._bot.store.bind_trade_deal_reference(
                    position.position_id,
                    matched_pending.pending_id,
                )
                if conflicting_position_id:
                    self._bot.store.record_event(
                        "ERROR",
                        symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position.position_id,
                            "deal_reference": matched_pending.pending_id,
                            "conflicting_position_id": conflicting_position_id,
                            "context": "runtime_reconcile",
                        },
                    )
                self._restore_pending_trailing_override(matched_pending, position.position_id)
                self._bot.store.delete_pending_open(matched_pending.pending_id)

            self._bot.position_book.upsert(position)
            local_by_id[position_id] = position
            recovered_count += 1
            self._bot.store.record_event(
                "WARN",
                symbol,
                "Recovered broker-managed open position during runtime sync",
                {
                    **self._bot._strategy_event_payload(
                        strategy,
                        (
                            default_assignment.strategy_params
                            if default_assignment is not None and str(default_assignment.strategy_name).strip().lower() == str(strategy).strip().lower()
                            else None
                        ),
                        strategy_entry_hint=strategy_entry,
                    ),
                    "position_id": position.position_id,
                    "strategy_entry": strategy_entry,
                    "strategy_entry_component": strategy_entry_component,
                    "strategy_entry_signal": strategy_entry_signal,
                    "mode": self._bot.config.mode.value,
                    "recovered_from_pending_open": matched_pending is not None,
                },
            )

        if recovered_count > 0:
            logger.warning(
                "Runtime broker open-position sync recovered %d positions",
                recovered_count,
            )
            self._bot.store.record_event(
                "WARN",
                None,
                "Runtime broker open-position sync recovered positions",
                {"count": recovered_count, "mode": self._bot.config.mode.value},
            )

    def _filter_restored_positions_for_mode(self, restored: dict[str, Position]) -> dict[str, Position]:
        if self._bot.config.mode != RunMode.EXECUTION:
            return restored

        filtered: dict[str, Position] = {}
        for position in restored.values():
            symbol = str(position.symbol).strip().upper()
            position_id = str(position.position_id or "")
            lowered = position_id.lower()
            if lowered.startswith("paper-") or lowered.startswith("mock-"):
                logger.warning(
                    "Skipping incompatible restored position in execution mode | symbol=%s position_id=%s",
                    symbol,
                    position_id,
                )
                self._bot.store.record_event(
                    "WARN",
                    symbol,
                    "Skipped incompatible restored position in execution mode",
                    {"position_id": position_id, "mode": self._bot.config.mode.value},
                )
                continue
            filtered[position_id] = position
        return filtered

    def _match_pending_open(self, position: Position, pending_opens: list[PendingOpen]) -> PendingOpen | None:
        exact_matches = [
            pending
            for pending in pending_opens
            if pending.position_id and str(pending.position_id) == str(position.position_id)
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        return None

    def _restore_pending_trailing_override(self, pending: PendingOpen, position_id: str) -> None:
        if not pending.trailing_override:
            return
        payload = {
            "position_id": position_id,
            "override": pending.trailing_override,
        }
        self._bot.store.set_kv(f"worker.trailing_override.{pending.symbol}", json.dumps(payload))

    def _sync_execution_positions_from_broker(
        self,
        local_restored: dict[str, Position],
    ) -> tuple[dict[str, Position] | None, dict[str, object]]:
        summary: dict[str, object] = {
            "broker_sync_used": False,
            "broker_sync_status": "skipped",
            "source": "state_store",
            "broker_open_count": 0,
            "recovered_count": 0,
            "stale_local_count": 0,
            "pending_open_count": 0,
            "resolved_pending_count": 0,
        }
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return None, summary

        preferred_symbols = self._bot._symbols_to_run()
        desired_assignments = self._bot._schedule_assignments(self._bot._now_utc())
        pending_opens = self._bot.store.load_pending_opens(mode=self._bot.config.mode.value)
        if self._bot._broker_public_api_backoff_remaining_sec() > 0:
            summary["broker_sync_status"] = "deferred_allowance_backoff"
            summary["source"] = "state_store"
            summary["pending_open_count"] = len(pending_opens)
            return None, summary
        try:
            known_position_ids = [position.position_id for position in local_restored.values() if str(position.position_id).strip()]
            known_position_ids.extend(
                str(pending.position_id)
                for pending in pending_opens
                if pending.position_id and str(pending.position_id).strip()
            )
            known_deal_references = self._bot.store.load_open_trade_deal_references(mode=self._bot.config.mode.value)
            known_deal_references.extend(
                str(pending.pending_id)
                for pending in pending_opens
                if str(pending.pending_id).strip()
            )
            known_deal_references = list(dict.fromkeys(known_deal_references))
            if not self._bot._reserve_ig_non_trading_budget(
                scope="startup_managed_open_positions",
                wait_timeout_sec=0.3,
            ):
                summary["broker_sync_status"] = "deferred_local_non_trading_budget"
                summary["source"] = "state_store"
                summary["pending_open_count"] = len(pending_opens)
                return None, summary
            broker_restored = self._bot.broker.get_managed_open_positions(
                self._bot.bot_magic_prefix,
                self._bot.bot_magic_instance,
                preferred_symbols=preferred_symbols,
                known_deal_references=known_deal_references,
                known_position_ids=known_position_ids,
                pending_opens=pending_opens,
                include_unmatched_preferred=True,
            )
        except Exception as exc:
            error_text = str(exc)
            if self._bot._is_allowance_related_error(error_text):
                logger.info(
                    "Managed broker position sync deferred on startup due to allowance backoff, using local state fallback: %s",
                    exc,
                )
                self._bot.store.record_event(
                    "WARN",
                    None,
                    "Managed broker position sync deferred on startup due to allowance backoff",
                    {"mode": self._bot.config.mode.value, "error": error_text},
                )
                summary["broker_sync_status"] = "deferred_allowance_backoff"
                summary["source"] = "state_store"
                summary["error"] = error_text
                summary["pending_open_count"] = len(pending_opens)
                return None, summary
            logger.warning("Managed broker position sync failed on startup, using local state fallback: %s", exc)
            self._bot.store.record_event(
                "WARN",
                None,
                "Managed broker position sync failed on startup",
                {"mode": self._bot.config.mode.value, "error": error_text},
            )
            summary["broker_sync_status"] = "failed_fallback_local"
            summary["source"] = "state_store"
            summary["error"] = error_text
            summary["pending_open_count"] = len(pending_opens)
            return None, summary

        broker_restored = self._filter_restored_positions_for_mode(broker_restored)
        local_by_position_id = {position.position_id: position for position in local_restored.values()}
        broker_ids = {position.position_id for position in broker_restored.values()}
        matched_pending_ids: set[str] = set()
        now_ts = time.time()
        recovered_count = 0
        stale_local_count = 0

        for position in broker_restored.values():
            symbol = str(position.symbol).strip().upper()
            existing_row = self._bot.store.get_trade_record(position.position_id)
            existing_position = local_by_position_id.get(position.position_id)
            matched_pending = self._match_pending_open(position, pending_opens)
            if existing_position is not None and position.entry_confidence <= 0 and existing_position.entry_confidence > 0:
                position.entry_confidence = existing_position.entry_confidence
            if matched_pending is not None and position.entry_confidence <= 0 and matched_pending.entry_confidence > 0:
                position.entry_confidence = matched_pending.entry_confidence

            default_assignment = desired_assignments.get(symbol)
            (
                thread_name,
                strategy,
                strategy_entry,
                strategy_entry_component,
                strategy_entry_signal,
                mode,
            ) = self._bot._resolved_recovery_trade_identity(
                symbol=symbol,
                existing_row=existing_row,
                matched_pending=matched_pending,
                default_assignment=default_assignment,
            )
            self._bot._apply_position_trade_identity(
                position,
                strategy=strategy,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            self._bot.store.upsert_trade(
                position,
                thread_name,
                strategy,
                mode,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            if matched_pending is not None:
                matched_pending_ids.add(matched_pending.pending_id)
                conflicting_position_id = self._bot.store.bind_trade_deal_reference(
                    position.position_id,
                    matched_pending.pending_id,
                )
                if conflicting_position_id:
                    self._bot.store.record_event(
                        "ERROR",
                        symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position.position_id,
                            "deal_reference": matched_pending.pending_id,
                            "conflicting_position_id": conflicting_position_id,
                            "context": "startup_reconcile",
                        },
                    )
                self._restore_pending_trailing_override(matched_pending, position.position_id)
                self._bot.store.delete_pending_open(matched_pending.pending_id)

            if existing_position is None:
                logger.info(
                    "Recovered broker-managed open position on startup | symbol=%s position_id=%s",
                    symbol,
                    position.position_id,
                )
                self._bot.store.record_event(
                    "INFO",
                    symbol,
                    "Recovered broker-managed open position on startup",
                    {
                        **self._bot._strategy_event_payload(
                            strategy,
                            (
                                default_assignment.strategy_params
                                if default_assignment is not None and str(default_assignment.strategy_name).strip().lower() == str(strategy).strip().lower()
                                else None
                            ),
                            strategy_entry_hint=strategy_entry,
                        ),
                        "position_id": position.position_id,
                        "strategy_entry": strategy_entry,
                        "strategy_entry_component": strategy_entry_component,
                        "strategy_entry_signal": strategy_entry_signal,
                        "mode": self._bot.config.mode.value,
                        "recovered_from_pending_open": matched_pending is not None,
                    },
                )
                recovered_count += 1

        for position in local_restored.values():
            symbol = str(position.symbol).strip().upper()
            if position.position_id in broker_ids:
                continue
            if self._bot._reconcile_missing_local_position_from_broker_sync(
                position,
                context="startup sync",
                now_ts=now_ts,
            ):
                stale_local_count += 1
                continue
            self._bot.store.update_trade_status(
                position.position_id,
                status="missing_on_broker",
                closed_at=now_ts,
            )
            self._bot.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=now_ts,
                close_reason="startup_sync:missing_on_broker",
            )
            logger.warning(
                "Local open position missing on broker during startup sync | symbol=%s position_id=%s",
                symbol,
                position.position_id,
            )
            self._bot.store.record_event(
                "WARN",
                symbol,
                "Local open position missing on broker during startup sync",
                {"position_id": position.position_id, "mode": self._bot.config.mode.value},
            )
            stale_local_count += 1

        logger.info(
            "Managed broker startup sync complete | restored=%d local_stale=%d mode=%s",
            len(broker_restored),
            stale_local_count,
            self._bot.config.mode.value,
        )
        summary.update(
            {
                "broker_sync_used": True,
                "broker_sync_status": "ok",
                "source": "broker_sync",
                "broker_open_count": len(broker_restored),
                "recovered_count": recovered_count,
                "stale_local_count": stale_local_count,
                "pending_open_count": len(pending_opens),
                "resolved_pending_count": len(matched_pending_ids),
            }
        )
        return broker_restored, summary

    def _ensure_execution_startup_broker_sync_succeeded(self, summary: dict[str, object]) -> None:
        if self._bot.config.mode != RunMode.EXECUTION or self._bot.config.broker != "ig":
            return
        status = str(summary.get("broker_sync_status") or "").strip().lower()
        if status == "ok":
            return
        source = str(summary.get("source") or "unknown").strip() or "unknown"
        error_text = str(summary.get("error") or "").strip()
        payload = {
            "mode": self._bot.config.mode.value,
            "broker": self._bot.config.broker,
            "status": status or "unknown",
            "source": source,
            "error": error_text or None,
            "pending_open_count": int(summary.get("pending_open_count") or 0),
            "local_open_count": int(summary.get("local_open_count") or 0),
        }
        logger.error(
            "Execution startup requires live broker position sync; aborting startup | status=%s source=%s error=%s",
            status or "unknown",
            source,
            error_text or "-",
        )
        self._bot.store.record_event(
            "ERROR",
            None,
            "Execution startup aborted: live broker position sync unavailable",
            payload,
        )
        raise RuntimeError(
            "Execution startup requires live broker position sync "
            f"(status={status or 'unknown'} source={source})"
        )

    def _restore_open_positions(self) -> tuple[dict[str, Position], dict[str, object]]:
        restored_raw = self._bot.store.load_open_positions(mode=self._bot.config.mode.value)
        restored = self._filter_restored_positions_for_mode(restored_raw)
        broker_synced, summary = self._sync_execution_positions_from_broker(restored)
        final_restored = restored if broker_synced is None else broker_synced
        summary.update(
            {
                "mode": self._bot.config.mode.value,
                "broker": self._bot.config.broker,
                "local_open_count": len(restored),
                "final_open_count": len(final_restored),
                "restored_open_count": len(final_restored),
            }
        )
        self._ensure_execution_startup_broker_sync_succeeded(summary)
        return final_restored, summary

    def _log_open_position_restore_summary(self, summary: dict[str, object]) -> None:
        logger.info(
            "Open position restore summary | source=%s local=%s broker=%s recovered=%s stale=%s final=%s mode=%s",
            summary.get("source", "-"),
            summary.get("local_open_count", 0),
            summary.get("broker_open_count", 0),
            summary.get("recovered_count", 0),
            summary.get("stale_local_count", 0),
            summary.get("final_open_count", 0),
            summary.get("mode", self._bot.config.mode.value),
        )
        self._bot.store.record_event(
            "INFO",
            None,
            "Open position restore summary",
            dict(summary),
        )

    def sync_open_positions(self) -> dict[str, object]:
        self._bot._connect_broker()
        final_restored, summary = self._restore_open_positions()
        self._bot.position_book.bootstrap(final_restored)
        reconciled_missing_count = self._bot._backfill_missing_on_broker_trades()
        if reconciled_missing_count > 0:
            summary["missing_on_broker_reconciled_count"] = reconciled_missing_count
        reconciled_closed_details_count = self._bot._backfill_closed_trade_details()
        if reconciled_closed_details_count > 0:
            summary["closed_details_reconciled_count"] = reconciled_closed_details_count
        self._log_open_position_restore_summary(summary)
        return summary
