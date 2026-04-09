from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from dataclasses import dataclass, field
import json
import logging
import uuid
from typing import Any

from xtb_bot.client import BrokerError
from xtb_bot.models import PendingOpen, Position, PriceTick, RunMode, Signal, Side


logger = logging.getLogger("xtb_bot.worker")

_POSITION_RUNTIME_FULL_SWEEP_INTERVAL_SEC = 300.0
_POSITION_RUNTIME_PRUNE_EVENT_MAX_IDS = 10


@dataclass(slots=True)
class WorkerOrderRuntimeState:
    pending_close_retry_position_id: str | None = None
    pending_close_retry_until_ts: float = 0.0
    pending_close_retry_reason: str | None = None
    pending_close_verification_position_id: str | None = None
    pending_close_verification_reason: str | None = None
    pending_close_verification_started_ts: float = 0.0
    last_close_deferred_event_ts: float = 0.0
    last_close_deferred_position_id: str | None = None
    pending_missing_position_id: str | None = None
    pending_missing_position_until_ts: float = 0.0
    pending_missing_position_deadline_ts: float = 0.0
    manual_close_sync_next_check_ts: float = 0.0
    manual_close_sync_position_id: str | None = None
    strategy_trailing_overrides_by_position: dict[str, dict[str, float | str]] = field(default_factory=dict)
    pending_open_created_monotonic_by_id: dict[str, float] = field(default_factory=dict)
    position_opened_monotonic_by_id: dict[str, float] = field(default_factory=dict)
    position_peak_favorable_pips: dict[str, float] = field(default_factory=dict)
    position_peak_favorable_ts: dict[str, float] = field(default_factory=dict)
    position_peak_adverse_pips: dict[str, float] = field(default_factory=dict)
    position_trailing_last_update_ts: dict[str, float] = field(default_factory=dict)
    position_partial_take_profit_done: dict[str, bool] = field(default_factory=dict)
    position_entry_strategy_by_id: dict[str, str] = field(default_factory=dict)
    position_entry_component_by_id: dict[str, str] = field(default_factory=dict)
    position_entry_signal_by_id: dict[str, str] = field(default_factory=dict)
    last_position_runtime_full_sweep_ts: float = 0.0


class WorkerOrderManager:
    def __init__(self, worker: Any) -> None:
        self._worker = worker
        self._state = WorkerOrderRuntimeState()

    @property
    def state(self) -> WorkerOrderRuntimeState:
        return self._state

    @staticmethod
    def build_magic_comment(
        *,
        bot_magic_prefix: str,
        bot_magic_instance: str,
    ) -> tuple[str, str]:
        trade_uid = uuid.uuid4().hex[:10]
        # Keep broker reference/comment compact to avoid silent truncation on broker side.
        prefix = str(bot_magic_prefix).strip()[:10] or "BOT"
        instance = str(bot_magic_instance).strip()[:8] or "RUN"
        comment = f"{prefix}:{instance}:{trade_uid}"
        if len(comment) > 32:
            comment = comment[:32]
        return comment, trade_uid

    @staticmethod
    def is_retryable_close_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return (
            "validation.null-not-allowed.request" in lowered
            or "client-token-invalid" in lowered
            or "ig api client is not connected" in lowered
            or "503 service unavailable" in lowered
            or "temporarily unavailable" in lowered
            or "timeout" in lowered
            or "timed out" in lowered
        )

    @staticmethod
    def is_market_closed_close_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return (
            "market_closed_with_edits" in lowered
            or "market_offline" in lowered
            or "market closed" in lowered
            or "market not available online" in lowered
        )

    def persist_trailing_override(self, position_id: str, override: dict[str, float | str]) -> None:
        worker = self._worker
        payload = {
            "position_id": position_id,
            "override": override,
        }
        worker.store.set_kv(worker._strategy_trailing_store_key, json.dumps(payload))

    def load_persisted_trailing_override_for_position(
        self,
        position_id: str,
    ) -> dict[str, float | str] | None:
        worker = self._worker
        raw = worker.store.get_kv(worker._strategy_trailing_store_key)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        if str(payload.get("position_id")) != position_id:
            return None
        override = payload.get("override")
        if not isinstance(override, dict):
            return None
        trailing_mode = str(override.get("trailing_mode") or "distance").strip().lower()
        normalized: dict[str, float | str] = {
            "trailing_mode": "fast_ma" if trailing_mode == "fast_ma" else "distance",
        }
        adaptive_family = str(override.get("adaptive_trailing_family") or "").strip().lower()
        if adaptive_family in {"trend", "mean_reversion", "neutral"}:
            normalized["adaptive_trailing_family"] = adaptive_family
        if trailing_mode == "fast_ma":
            numeric_keys = (
                "trailing_activation_r_multiple",
                "trailing_activation_min_profit_pips",
                "fast_ma_buffer_atr",
                "fast_ma_buffer_pips",
                "fast_ma_min_step_pips",
                "fast_ma_update_cooldown_sec",
                "trailing_breakeven_offset_pips",
            )
        else:
            numeric_keys = (
                "trailing_activation_ratio",
                "trailing_distance_pips",
                "trailing_breakeven_offset_pips",
            )
        numeric_keys = numeric_keys + (
            "adaptive_trailing_entry_extreme_abs",
            "adaptive_trailing_initial_stop_pips",
        )
        for key in numeric_keys:
            if key not in override:
                continue
            try:
                normalized[key] = float(override[key])
            except (TypeError, ValueError):
                continue
        if len(normalized) <= 1:
            return None
        return normalized

    def clear_persisted_trailing_override(self) -> None:
        worker = self._worker
        worker.store.delete_kv(worker._strategy_trailing_store_key)

    def set_position_trailing_override(
        self,
        position_id: str,
        override: dict[str, float | str] | None,
    ) -> None:
        state = self._state
        state.strategy_trailing_overrides_by_position.clear()
        if override is None:
            self.clear_persisted_trailing_override()
            return
        state.strategy_trailing_overrides_by_position[position_id] = override
        self.persist_trailing_override(position_id, override)

    def get_position_trailing_override(self, position_id: str) -> dict[str, float | str] | None:
        state = self._state
        override = state.strategy_trailing_overrides_by_position.get(position_id)
        if override is not None:
            return override
        loaded = self.load_persisted_trailing_override_for_position(position_id)
        if loaded is None:
            return None
        state.strategy_trailing_overrides_by_position[position_id] = loaded
        return loaded

    def clear_position_runtime_state(self, position_id: str | None) -> None:
        state = self._state
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return
        state.strategy_trailing_overrides_by_position.pop(normalized_position_id, None)
        state.position_opened_monotonic_by_id.pop(normalized_position_id, None)
        state.position_peak_favorable_pips.pop(normalized_position_id, None)
        state.position_peak_favorable_ts.pop(normalized_position_id, None)
        state.position_peak_adverse_pips.pop(normalized_position_id, None)
        state.position_trailing_last_update_ts.pop(normalized_position_id, None)
        state.position_partial_take_profit_done.pop(normalized_position_id, None)
        state.position_entry_strategy_by_id.pop(normalized_position_id, None)
        state.position_entry_component_by_id.pop(normalized_position_id, None)
        state.position_entry_signal_by_id.pop(normalized_position_id, None)
        persisted_position_id = self._persisted_trailing_override_position_id()
        if persisted_position_id == normalized_position_id:
            self.clear_persisted_trailing_override()

    def _tracked_position_runtime_ids(self) -> set[str]:
        state = self._state
        tracked_ids: set[str] = set()
        tracked_ids.update(state.strategy_trailing_overrides_by_position)
        tracked_ids.update(state.position_opened_monotonic_by_id)
        tracked_ids.update(state.position_peak_favorable_pips)
        tracked_ids.update(state.position_peak_favorable_ts)
        tracked_ids.update(state.position_peak_adverse_pips)
        tracked_ids.update(state.position_trailing_last_update_ts)
        tracked_ids.update(state.position_partial_take_profit_done)
        tracked_ids.update(state.position_entry_strategy_by_id)
        tracked_ids.update(state.position_entry_component_by_id)
        tracked_ids.update(state.position_entry_signal_by_id)
        return tracked_ids

    def _tracked_pending_open_runtime_ids(self) -> set[str]:
        return set(self._state.pending_open_created_monotonic_by_id)

    def _retained_runtime_position_ids(self, active_position_id: str | None = None) -> set[str]:
        worker = self._worker
        state = self._state
        retained_ids: set[str] = set()
        normalized_active_id = str(active_position_id or "").strip()
        if normalized_active_id:
            retained_ids.add(normalized_active_id)
        for position in worker.position_book.get_all(worker.symbol):
            normalized_position_id = str(position.position_id or "").strip()
            if normalized_position_id:
                retained_ids.add(normalized_position_id)
        for candidate in (
            state.pending_close_retry_position_id,
            state.pending_close_verification_position_id,
            state.pending_missing_position_id,
            state.manual_close_sync_position_id,
        ):
            normalized_candidate = str(candidate or "").strip()
            if normalized_candidate:
                retained_ids.add(normalized_candidate)
        return retained_ids

    def _retained_store_position_ids(self) -> set[str]:
        worker = self._worker
        retained_ids: set[str] = set()
        for position in worker.store.load_open_positions(mode=worker.mode.value).values():
            if str(position.symbol or "").strip().upper() != str(worker.symbol or "").strip().upper():
                continue
            normalized_position_id = str(position.position_id or "").strip()
            if normalized_position_id:
                retained_ids.add(normalized_position_id)
        return retained_ids

    def _retained_pending_open_ids(self) -> set[str]:
        worker = self._worker
        retained_ids: set[str] = set()
        for pending in worker.store.load_pending_opens(mode=worker.mode.value):
            if str(pending.symbol or "").strip().upper() != str(worker.symbol or "").strip().upper():
                continue
            normalized_pending_id = str(pending.pending_id or "").strip()
            if normalized_pending_id:
                retained_ids.add(normalized_pending_id)
        return retained_ids

    def _persisted_trailing_override_position_id(self) -> str | None:
        worker = self._worker
        raw = worker.store.get_kv(worker._strategy_trailing_store_key)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        normalized_position_id = str(payload.get("position_id") or "").strip()
        return normalized_position_id or None

    def _record_runtime_state_prune(
        self,
        *,
        stale_position_ids: list[str],
        stale_pending_open_ids: list[str],
        cleared_persisted_trailing_override_for: str | None,
    ) -> None:
        worker = self._worker
        if (
            not stale_position_ids
            and not stale_pending_open_ids
            and not cleared_persisted_trailing_override_for
        ):
            return
        payload: dict[str, object] = {
            "stale_position_count": len(stale_position_ids),
            "stale_pending_open_count": len(stale_pending_open_ids),
        }
        if stale_position_ids:
            payload["stale_position_ids"] = stale_position_ids[:_POSITION_RUNTIME_PRUNE_EVENT_MAX_IDS]
        if stale_pending_open_ids:
            payload["stale_pending_open_ids"] = stale_pending_open_ids[:_POSITION_RUNTIME_PRUNE_EVENT_MAX_IDS]
        if cleared_persisted_trailing_override_for:
            payload["cleared_persisted_trailing_override_for"] = cleared_persisted_trailing_override_for
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Worker runtime state pruned",
            payload,
        )
        logger.info(
            "Worker runtime state pruned | symbol=%s stale_positions=%s stale_pending_opens=%s cleared_persisted_override=%s",
            worker.symbol,
            len(stale_position_ids),
            len(stale_pending_open_ids),
            bool(cleared_persisted_trailing_override_for),
        )

    def prune_position_runtime_state(
        self,
        active_position_id: str | None = None,
        *,
        force_full_sweep: bool = False,
    ) -> None:
        worker = self._worker
        state = self._state
        now = worker._monotonic_now()
        full_sweep_due = force_full_sweep or (
            (now - state.last_position_runtime_full_sweep_ts) >= _POSITION_RUNTIME_FULL_SWEEP_INTERVAL_SEC
        )
        if not full_sweep_due:
            return

        retained_position_ids = self._retained_runtime_position_ids(active_position_id)
        retained_position_ids.update(self._retained_store_position_ids())
        stale_position_ids = sorted(self._tracked_position_runtime_ids() - retained_position_ids)
        for position_id in stale_position_ids:
            self.clear_position_runtime_state(position_id)

        retained_pending_open_ids = self._retained_pending_open_ids()
        stale_pending_open_ids = sorted(
            self._tracked_pending_open_runtime_ids() - retained_pending_open_ids
        )
        for pending_id in stale_pending_open_ids:
            state.pending_open_created_monotonic_by_id.pop(pending_id, None)

        persisted_trailing_override_position_id = self._persisted_trailing_override_position_id()
        cleared_persisted_trailing_override_for: str | None = None
        if (
            persisted_trailing_override_position_id
            and persisted_trailing_override_position_id not in retained_position_ids
        ):
            self.clear_persisted_trailing_override()
            cleared_persisted_trailing_override_for = persisted_trailing_override_position_id

        state.last_position_runtime_full_sweep_ts = now
        self._record_runtime_state_prune(
            stale_position_ids=stale_position_ids,
            stale_pending_open_ids=stale_pending_open_ids,
            cleared_persisted_trailing_override_for=cleared_persisted_trailing_override_for,
        )

    def apply_trailing_stop(self, position: Position, new_stop_loss: float, progress: float) -> None:
        worker = self._worker
        old_stop = position.stop_loss
        if worker.mode == RunMode.EXECUTION:
            with worker._watchdog_blocking_operation(
                "modify_position_trailing_stop",
                last_price=worker._latest_price(),
            ):
                worker.broker.modify_position(position, new_stop_loss, position.take_profit)

        position.stop_loss = new_stop_loss
        self._state.position_trailing_last_update_ts[position.position_id] = worker._monotonic_now()
        worker._persist_position_trade_state(position)
        trailing_settings = worker._effective_trailing_settings(position)
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Trailing stop adjusted",
            {
                "position_id": position.position_id,
                "old_stop_loss": old_stop,
                "new_stop_loss": new_stop_loss,
                "take_profit": position.take_profit,
                "activation_ratio": trailing_settings["trailing_activation_ratio"],
                "progress_to_tp": round(progress, 4),
                "trailing_mode": str(trailing_settings.get("trailing_mode") or "distance"),
                "trailing_distance_pips": trailing_settings["trailing_distance_pips"],
                "trailing_breakeven_offset_pips": trailing_settings["trailing_breakeven_offset_pips"],
            },
        )
        logger.warning(
            "Trailing stop adjusted | symbol=%s position_id=%s old_sl=%s new_sl=%s progress=%s",
            worker.symbol,
            position.position_id,
            old_stop,
            new_stop_loss,
            round(progress, 4),
        )

    def apply_breakeven_protection(
        self,
        position: Position,
        trigger: str,
        event_id: str | None = None,
        bid: float | None = None,
        ask: float | None = None,
    ) -> bool:
        worker = self._worker
        if position.side == Side.BUY:
            new_stop_loss = max(position.stop_loss, position.open_price)
            if new_stop_loss <= position.stop_loss:
                return False
        else:
            new_stop_loss = min(position.stop_loss, position.open_price)
            if new_stop_loss >= position.stop_loss:
                return False

        new_stop_loss = worker._normalize_price(new_stop_loss)
        if bid is not None and ask is not None:
            guarded = worker._apply_broker_min_stop_distance_guard(position, new_stop_loss, bid, ask)
            if guarded is None:
                return False
            new_stop_loss = guarded
        if worker.mode == RunMode.EXECUTION:
            with worker._watchdog_blocking_operation(
                "modify_position_breakeven",
                last_price=worker._latest_price(),
            ):
                worker.broker.modify_position(position, new_stop_loss, position.take_profit)
        position.stop_loss = new_stop_loss
        worker._persist_position_trade_state(position)
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Breakeven protection applied",
            {
                "position_id": position.position_id,
                "trigger": trigger,
                "event_id": event_id,
                "new_stop_loss": new_stop_loss,
                "open_price": position.open_price,
                "take_profit": position.take_profit,
            },
        )
        logger.warning(
            "Breakeven protection applied | symbol=%s position_id=%s trigger=%s new_sl=%s",
            worker.symbol,
            position.position_id,
            trigger,
            new_stop_loss,
        )
        return True

    def maybe_apply_partial_take_profit(
        self,
        position: Position,
        *,
        bid: float,
        ask: float,
    ) -> bool:
        worker = self._worker
        state = self._state
        if not worker.partial_take_profit_enabled:
            return False
        if worker.partial_take_profit_fraction <= 0:
            return False
        if state.position_partial_take_profit_done.get(position.position_id):
            return False
        mark = bid if position.side == Side.BUY else ask
        runner_state = worker._runner_preservation_state(position, mark)
        if worker.partial_take_profit_skip_trend_positions and str(runner_state["family"]) == "trend":
            state.position_partial_take_profit_done[position.position_id] = True
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Partial take-profit skipped for trend preservation",
                {
                    "position_id": position.position_id,
                    "family": runner_state["family"],
                },
            )
            return False
        if worker.partial_take_profit_skip_runner_positions and bool(runner_state["matured"]):
            state.position_partial_take_profit_done[position.position_id] = True
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Partial take-profit skipped for runner preservation",
                {
                    "position_id": position.position_id,
                    "family": runner_state["family"],
                    "peak_favorable_pips": round(float(runner_state["peak_favorable_pips"]), 4),
                    "required_peak_pips": round(float(runner_state["required_peak_pips"]), 4),
                },
            )
            return False
        stop_distance = abs(float(position.open_price) - float(position.stop_loss))
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return False
        favorable_move = (
            (mark - float(position.open_price))
            if position.side == Side.BUY
            else (float(position.open_price) - mark)
        )
        if favorable_move + FLOAT_COMPARISON_TOLERANCE < (worker.partial_take_profit_r_multiple * stop_distance):
            return False
        close_volume = worker._partial_take_profit_close_volume(position)
        if close_volume <= 0:
            state.position_partial_take_profit_done[position.position_id] = True
            return False

        if worker.mode == RunMode.EXECUTION:
            worker._allow_risk_reducing_action_after_lease_revocation(
                action="partial close",
                position=position,
                reason="partial_take_profit",
                close_volume=close_volume,
            )
            try:
                with worker._watchdog_blocking_operation(
                    "partial_close_position",
                    last_price=worker._latest_price(),
                    grace_sec=180.0,
                ):
                    worker.broker.close_position(position, close_volume)
            except BrokerError as exc:
                error_text = str(exc)
                if worker._is_allowance_backoff_error(error_text):
                    worker._handle_allowance_backoff_error(error_text)
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Partial take-profit close failed",
                    {
                        "position_id": position.position_id,
                        "close_volume": close_volume,
                        "error": error_text,
                    },
                )
                return False

        original_volume = max(FLOAT_COMPARISON_TOLERANCE, float(position.volume))
        raw_remaining_volume = max(0.0, original_volume - close_volume)
        remaining_volume = worker._normalize_volume(raw_remaining_volume)
        if remaining_volume <= 0 and raw_remaining_volume > FLOAT_COMPARISON_TOLERANCE:
            remaining_volume = raw_remaining_volume
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Partial take-profit left non-normalized remaining volume",
                {
                    "position_id": position.position_id,
                    "close_volume": close_volume,
                    "raw_remaining_volume": raw_remaining_volume,
                    "lot_min": float(worker.symbol_spec.lot_min) if worker.symbol_spec is not None else None,
                    "lot_step": float(worker.symbol_spec.lot_step) if worker.symbol_spec is not None else None,
                    "action": "keep_position_open_pending_broker_sync",
                },
            )
        if remaining_volume <= 0:
            state.position_partial_take_profit_done[position.position_id] = True
            worker._apply_multi_strategy_scale_out_allocation(position, closed_volume=close_volume)
            self.finalize_position_close(
                position,
                close_price=mark,
                reason="partial_take_profit:full_close",
                estimate_close_price=mark if worker.mode == RunMode.EXECUTION else None,
                pnl_estimate_source="partial_take_profit_mark" if worker.mode == RunMode.EXECUTION else None,
            )
            return True

        position.volume = remaining_volume
        position.pnl = worker._calculate_pnl(position, mark)
        state.position_partial_take_profit_done[position.position_id] = True
        worker._apply_multi_strategy_scale_out_allocation(position, closed_volume=close_volume)
        worker._persist_position_trade_state(position)
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Partial take-profit executed",
            {
                "position_id": position.position_id,
                "trigger_r_multiple": worker.partial_take_profit_r_multiple,
                "close_volume": close_volume,
                "remaining_volume": remaining_volume,
                "mark_price": mark,
            },
        )
        return True

    def open_position(
        self,
        side: Side,
        entry: float,
        stop_loss: float,
        take_profit: float,
        volume: float,
        confidence: float = 0.0,
        trailing_override: dict[str, float | str] | None = None,
        entry_strategy: str | None = None,
        entry_strategy_component: str | None = None,
        signal: Signal | None = None,
    ) -> None:
        worker = self._worker
        state = self._state
        if not worker._worker_lease_allows_trading(force_check=True):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Open skipped: worker lease is no longer active",
                {
                    "side": side.value,
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                },
            )
            return
        effective_entry_strategy = worker._normalize_strategy_label(
            entry_strategy,
            fallback=worker.strategy_name,
        )
        effective_entry_strategy_component = worker._normalize_strategy_label(
            entry_strategy_component,
            fallback=effective_entry_strategy,
        )
        effective_entry_signal = worker._effective_entry_signal_from_signal(signal) if signal is not None else None
        comment, trade_uid = self.build_magic_comment(
            bot_magic_prefix=worker.bot_magic_prefix,
            bot_magic_instance=worker.bot_magic_instance,
        )
        pending_id = worker._normalize_deal_reference(comment)
        deal_reference: str | None = pending_id if worker.mode == RunMode.EXECUTION else None
        pending_cleanup_required = False

        if worker.mode == RunMode.EXECUTION:
            worker.store.upsert_pending_open(
                PendingOpen(
                    pending_id=pending_id,
                    symbol=worker.symbol,
                    side=side,
                    volume=volume,
                    entry=entry,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    created_at=worker._wall_time_now(),
                    thread_name=worker.name,
                    strategy=worker.strategy_name,
                    strategy_entry=effective_entry_strategy,
                    strategy_entry_component=effective_entry_strategy_component,
                    strategy_entry_signal=effective_entry_signal,
                    mode=worker.mode.value,
                    entry_confidence=confidence,
                    trailing_override=trailing_override,
                )
            )
            worker._mark_pending_open_created_monotonic(pending_id)
            pending_cleanup_required = True

        try:
            requested_entry = entry
            if worker.mode == RunMode.SIGNAL_ONLY:
                worker.store.record_event(
                    "INFO",
                    worker.symbol,
                    "Signal generated (signal_only)",
                    {
                        "side": side.value,
                        "entry": entry,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "volume": volume,
                        "confidence": confidence,
                        "trade_uid": trade_uid,
                        "magic_comment": comment,
                        "strategy_trailing_override": trailing_override,
                    },
                )
                return

            if worker.mode == RunMode.PAPER:
                position_id = f"paper-{trade_uid}"
            else:
                try:
                    with worker._watchdog_blocking_operation(
                        "open_position",
                        last_price=entry,
                        grace_sec=180.0,
                    ):
                        position_id = worker.broker.open_position(
                            symbol=worker.symbol,
                            side=side,
                            volume=volume,
                            stop_loss=stop_loss,
                            take_profit=take_profit,
                            comment=comment,
                            entry_price=entry,
                        )
                except BrokerError as exc:
                    error_text = str(exc)
                    if worker._is_open_confirm_timeout_pending_recovery_error(error_text):
                        pending_cleanup_required = False
                        worker.store.record_event(
                            "WARN",
                            worker.symbol,
                            "Pending open retained for broker recovery",
                            {
                                "pending_id": pending_id,
                                "deal_reference": deal_reference,
                                "side": side.value,
                                "volume": volume,
                                "error": error_text,
                            },
                        )
                    raise
                worker.store.update_pending_open_position_id(pending_id, position_id)
                with worker._watchdog_blocking_operation(
                    "open_position_sync",
                    last_price=entry,
                    grace_sec=90.0,
                ):
                    broker_open_sync = worker._get_broker_open_sync(position_id)
                if isinstance(broker_open_sync, dict):
                    synced_entry = worker._safe_float(broker_open_sync.get("open_price"))
                    synced_stop = worker._safe_float(broker_open_sync.get("stop_loss"))
                    synced_take = worker._safe_float(broker_open_sync.get("take_profit"))
                    synced_reference = str(broker_open_sync.get("deal_reference") or "").strip()
                    if synced_entry and synced_entry > 0:
                        entry = synced_entry
                    if synced_stop and synced_stop > 0:
                        stop_loss = synced_stop
                    if synced_take and synced_take > 0:
                        take_profit = synced_take
                    if synced_reference:
                        deal_reference = synced_reference
                    worker.store.record_event(
                        "INFO",
                        worker.symbol,
                        "Broker open sync applied",
                        {
                            "position_id": position_id,
                            "open_price": entry,
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                            "source": broker_open_sync.get("source"),
                        },
                    )

            self.set_position_trailing_override(position_id, trailing_override)
            state.position_peak_favorable_pips[position_id] = 0.0
            state.position_peak_favorable_ts[position_id] = worker._monotonic_now()
            state.position_peak_adverse_pips[position_id] = 0.0
            state.position_trailing_last_update_ts[position_id] = 0.0
            state.position_partial_take_profit_done[position_id] = False
            self.prune_position_runtime_state(position_id)
            if worker._multi_strategy_scale_out_offset_by_name:
                worker._multi_strategy_scale_out_offset_by_name = {
                    key: 0.0 for key in worker._multi_strategy_scale_out_offset_by_name
                }
            position = Position(
                position_id=position_id,
                symbol=worker.symbol,
                side=side,
                volume=volume,
                open_price=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                opened_at=worker._wall_time_now(),
                entry_confidence=confidence,
                status="open",
                epic=worker._epic_text() or None,
                epic_variant=worker._epic_variant_text(worker._epic_text()) or None,
            )
            worker._mark_position_opened_monotonic(position_id)
            worker._apply_position_strategy_metadata(
                position,
                strategy=worker.strategy_name,
                strategy_entry=effective_entry_strategy,
                strategy_entry_component=effective_entry_strategy_component,
                strategy_entry_signal=effective_entry_signal,
            )
            worker._clear_entry_signal_persistence_state()
            worker.position_book.upsert(position)
            worker._persist_position_trade_state(position)
            worker.store.update_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                opened_at=position.opened_at,
                max_favorable_pips=0.0,
                max_adverse_pips=0.0,
                max_favorable_pnl=0.0,
                max_adverse_pnl=0.0,
            )
            entry_fill_quality = worker._persist_entry_execution_quality(
                position_id=position.position_id,
                symbol=position.symbol,
                side=side,
                reference_price=requested_entry,
                fill_price=entry,
            )
            worker._maybe_adapt_mean_breakout_buffer_after_entry_slippage(
                position_id=position.position_id,
                entry_fill_quality=entry_fill_quality,
                signal=signal,
                entry_strategy=effective_entry_strategy,
                entry_strategy_component=effective_entry_strategy_component,
                reference_price=requested_entry,
                fill_price=entry,
            )
            if worker.mode == RunMode.EXECUTION and deal_reference:
                conflicting_position_id = worker.store.bind_trade_deal_reference(position_id, deal_reference)
                if conflicting_position_id:
                    worker.store.record_event(
                        "ERROR",
                        worker.symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position_id,
                            "deal_reference": deal_reference,
                            "conflicting_position_id": conflicting_position_id,
                        },
                    )
                else:
                    worker.store.record_event(
                        "INFO",
                        worker.symbol,
                        "Trade deal reference bound",
                        {
                            "position_id": position_id,
                            "deal_reference": deal_reference,
                        },
                    )
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Position opened",
                {
                    "position_id": position_id,
                    "side": side.value,
                    "entry": entry,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "volume": volume,
                    "confidence": confidence,
                    "strategy_entry": effective_entry_strategy,
                    "strategy_entry_component": effective_entry_strategy_component,
                    "strategy_entry_signal": effective_entry_signal,
                    "strategy_base": worker._base_strategy_label(),
                    "mode": worker.mode.value,
                    "trade_uid": trade_uid,
                    "magic_comment": comment,
                    "strategy_trailing_override": trailing_override,
                },
            )
            if entry_fill_quality:
                worker.store.record_event(
                    "INFO",
                    worker.symbol,
                    "Entry fill quality recorded",
                    {
                        "position_id": position_id,
                        "strategy_entry": effective_entry_strategy,
                        "strategy_entry_component": effective_entry_strategy_component,
                        "strategy_entry_signal": effective_entry_signal,
                        **entry_fill_quality,
                    },
                )
            if worker.mode == RunMode.EXECUTION:
                worker.store.delete_pending_open(pending_id)
                worker._clear_pending_open_created_monotonic(pending_id)
                pending_cleanup_required = False
        finally:
            if worker.mode == RunMode.EXECUTION and pending_cleanup_required:
                try:
                    worker.store.delete_pending_open(pending_id)
                    worker._clear_pending_open_created_monotonic(pending_id)
                except Exception as cleanup_exc:
                    logger.warning(
                        "Failed to cleanup pending open after open-position failure | symbol=%s pending_id=%s error=%s",
                        worker.symbol,
                        pending_id,
                        cleanup_exc,
                    )

    def finalize_position_close(
        self,
        position: Position,
        close_price: float,
        reason: str,
        broker_sync: dict[str, object] | None = None,
        *,
        use_local_close_price: bool = True,
        estimate_close_price: float | None = None,
        pnl_estimate_source: str | None = None,
    ) -> None:
        worker = self._worker
        state = self._state
        now_mono = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        if worker.mode == RunMode.EXECUTION:
            use_local_close_price = False
        broker_closed_at = worker._safe_float((broker_sync or {}).get("closed_at"))
        broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
        final_close_price: float | None = None
        estimated_close_price: float | None = None
        if broker_close_price is not None and broker_close_price > 0:
            final_close_price = worker._normalize_price(broker_close_price)
        elif use_local_close_price and close_price > 0:
            final_close_price = worker._normalize_price(close_price)
        if estimate_close_price is not None and estimate_close_price > 0:
            estimated_close_price = worker._normalize_price(estimate_close_price)
        position.close_price = final_close_price
        final_closed_at = now_wall
        if broker_closed_at is not None and broker_closed_at > 0:
            final_closed_at = min(now_wall, broker_closed_at)
        position.closed_at = final_closed_at
        position.status = "closed"
        pnl_reference_price = final_close_price if final_close_price is not None else estimated_close_price
        local_pnl = worker._calculate_pnl(position, pnl_reference_price) if pnl_reference_price is not None else 0.0
        broker_realized_pnl = worker._safe_float((broker_sync or {}).get("realized_pnl"))
        broker_pnl_currency = worker._normalize_currency_code((broker_sync or {}).get("pnl_currency"))
        local_pnl_account, local_pnl_conversion = worker._normalize_pnl_to_account_currency(
            local_pnl,
            broker_pnl_currency,
        )
        broker_pnl_account, broker_pnl_conversion = worker._normalize_pnl_to_account_currency(
            broker_realized_pnl,
            broker_pnl_currency,
        )
        worker._maybe_record_broker_tick_value_calibration(
            position=position,
            final_close_price=final_close_price,
            broker_pnl_account=broker_pnl_account,
            broker_sync=broker_sync,
        )
        monetary_pnl_pending_broker_sync = (
            worker.mode == RunMode.EXECUTION
            and broker_sync is not None
            and broker_realized_pnl is None
        )
        broker_pnl_conversion_missing = bool(broker_pnl_conversion.get("pnl_conversion_missing"))
        local_pnl_conversion_missing = bool(local_pnl_conversion.get("pnl_conversion_missing"))
        pnl_is_estimated = False
        resolved_pnl_estimate_source: str | None = None
        if broker_pnl_account is not None:
            position.pnl = broker_pnl_account
        elif pnl_reference_price is not None and not local_pnl_conversion_missing:
            position.pnl = float(local_pnl_account if local_pnl_account is not None else local_pnl)
            if monetary_pnl_pending_broker_sync:
                pnl_is_estimated = True
                if estimated_close_price is not None and final_close_price is None:
                    resolved_pnl_estimate_source = pnl_estimate_source or "inferred_close_price"
                elif final_close_price is not None:
                    resolved_pnl_estimate_source = pnl_estimate_source or "broker_close_price"
                else:
                    resolved_pnl_estimate_source = pnl_estimate_source or "local_close_price"
        else:
            position.pnl = 0.0
            if broker_pnl_conversion_missing or local_pnl_conversion_missing:
                pnl_is_estimated = True
                resolved_pnl_estimate_source = pnl_estimate_source or "currency_conversion_missing"
        position.pnl_is_estimated = pnl_is_estimated
        position.pnl_estimate_source = resolved_pnl_estimate_source

        worker._maybe_mark_symbol_guaranteed_stop_required_after_slippage(
            position=position,
            final_close_price=final_close_price,
            reason=reason,
        )

        max_favorable_pips = state.position_peak_favorable_pips.get(position.position_id, 0.0)
        max_adverse_pips = state.position_peak_adverse_pips.get(position.position_id, 0.0)
        worker.store.update_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            opened_at=position.opened_at,
            closed_at=final_closed_at,
            close_reason=reason,
            max_favorable_pips=max_favorable_pips,
            max_adverse_pips=max_adverse_pips,
            max_favorable_pnl=max(0.0, float(position.pnl)),
            max_adverse_pnl=min(0.0, float(position.pnl)),
        )
        worker.store.finalize_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            closed_at=final_closed_at,
            close_reason=reason,
        )
        entry_strategy_label = worker._position_entry_strategy(position)
        exit_fill_quality = worker._persist_exit_execution_quality(
            position_id=position.position_id,
            symbol=position.symbol,
            side=position.side,
            reference_price=close_price,
            fill_price=final_close_price,
        )
        worker._fill_quality_summary_cache.pop((worker.symbol.upper(), entry_strategy_label), None)
        trade_performance = worker.store.load_trade_performance(position.position_id)

        self.set_position_trailing_override(position.position_id, None)
        worker._clear_position_opened_monotonic(position.position_id)
        state.position_peak_favorable_pips.pop(position.position_id, None)
        state.position_peak_favorable_ts.pop(position.position_id, None)
        state.position_peak_adverse_pips.pop(position.position_id, None)
        state.position_trailing_last_update_ts.pop(position.position_id, None)
        state.position_partial_take_profit_done.pop(position.position_id, None)
        if worker._multi_strategy_scale_out_offset_by_name:
            worker._multi_strategy_scale_out_offset_by_name = {
                key: 0.0 for key in worker._multi_strategy_scale_out_offset_by_name
            }
        if state.manual_close_sync_position_id == position.position_id:
            state.manual_close_sync_position_id = None
            state.manual_close_sync_next_check_ts = 0.0
        worker._clear_pending_close_verification(position.position_id)
        if state.pending_close_retry_position_id == position.position_id:
            state.pending_close_retry_position_id = None
            state.pending_close_retry_until_ts = 0.0
            state.pending_close_retry_reason = None
        worker.position_book.remove_by_id(position.position_id)
        entry_strategy_for_close = worker._position_entry_strategy(position)
        entry_component_for_close = worker._position_entry_component(position)
        entry_signal_for_close = worker._position_entry_signal(position)
        exit_strategy_label, exit_strategy_component, exit_strategy_signal = (
            worker._effective_exit_metadata_from_reason(position, reason)
        )
        worker._apply_position_strategy_metadata(
            position,
            strategy=worker.strategy_name,
            strategy_entry=entry_strategy_for_close,
            strategy_entry_component=entry_component_for_close,
            strategy_entry_signal=entry_signal_for_close,
            strategy_exit=exit_strategy_label,
            strategy_exit_component=exit_strategy_component,
            strategy_exit_signal=exit_strategy_signal,
        )
        worker._persist_position_trade_state(
            position,
            strategy_exit=exit_strategy_label,
            strategy_exit_component=exit_strategy_component,
            strategy_exit_signal=exit_strategy_signal,
        )
        state.position_entry_strategy_by_id.pop(position.position_id, None)
        state.position_entry_component_by_id.pop(position.position_id, None)
        state.position_entry_signal_by_id.pop(position.position_id, None)
        payload: dict[str, object] = {
            "position_id": position.position_id,
            "reason": reason,
            "close_price": final_close_price,
            "pnl": position.pnl,
            "strategy_entry": entry_strategy_for_close,
            "strategy_entry_component": entry_component_for_close,
            "strategy_base": worker._base_strategy_label(),
        }
        if exit_strategy_label:
            payload["strategy_exit"] = exit_strategy_label
        if exit_strategy_component:
            payload["strategy_exit_component"] = exit_strategy_component
        if exit_strategy_signal:
            payload["strategy_exit_signal"] = exit_strategy_signal
        if final_close_price is None:
            payload["close_price_pending_broker_sync"] = True
        if monetary_pnl_pending_broker_sync or (broker_realized_pnl is None and final_close_price is None):
            payload["pnl_pending_broker_sync"] = True
        if broker_pnl_conversion_missing or local_pnl_conversion_missing:
            payload["pnl_pending_currency_conversion"] = True
        if broker_closed_at is not None and broker_closed_at > 0:
            payload["broker_closed_at"] = broker_closed_at
        if broker_sync:
            payload["broker_close_sync"] = broker_sync
            if local_pnl_account is not None:
                payload["local_pnl_estimate"] = float(local_pnl_account)
            elif not local_pnl_conversion_missing:
                payload["local_pnl_estimate"] = float(local_pnl)
            if pnl_is_estimated:
                payload["pnl_is_estimated"] = True
                if resolved_pnl_estimate_source:
                    payload["pnl_estimate_source"] = resolved_pnl_estimate_source
            if (
                local_pnl_conversion.get("pnl_conversion_applied")
                and float(local_pnl_conversion.get("pnl_native_amount") or 0.0) != payload["local_pnl_estimate"]
            ):
                payload["local_pnl_estimate_native"] = float(local_pnl_conversion["pnl_native_amount"])
            pnl_details = broker_pnl_conversion if broker_realized_pnl is not None else local_pnl_conversion
            if broker_pnl_currency:
                payload["pnl_currency"] = broker_pnl_currency
            if pnl_details:
                payload["pnl_normalization"] = pnl_details
        if isinstance(trade_performance, dict):
            payload["trade_performance"] = {
                "max_favorable_pips": float(trade_performance.get("max_favorable_pips") or 0.0),
                "max_adverse_pips": float(trade_performance.get("max_adverse_pips") or 0.0),
                "max_favorable_pnl": float(trade_performance.get("max_favorable_pnl") or 0.0),
                "max_adverse_pnl": float(trade_performance.get("max_adverse_pnl") or 0.0),
                "entry_slippage_pips": trade_performance.get("entry_slippage_pips"),
                "entry_adverse_slippage_pips": trade_performance.get("entry_adverse_slippage_pips"),
                "exit_slippage_pips": trade_performance.get("exit_slippage_pips"),
                "exit_adverse_slippage_pips": trade_performance.get("exit_adverse_slippage_pips"),
            }
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Position closed",
            payload,
        )
        if exit_fill_quality:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Exit fill quality recorded",
                {
                    "position_id": position.position_id,
                    "strategy_entry": entry_strategy_label,
                    "strategy_entry_component": entry_component_for_close,
                    "strategy_exit_component": exit_strategy_component,
                    "strategy_exit_signal": exit_strategy_signal,
                    **exit_fill_quality,
                },
            )
        outcome = worker._cooldown_outcome_from_pnl(position.pnl)
        cooldown_sec = worker._cooldown_duration_for_outcome(outcome)
        if cooldown_sec > 0:
            worker._next_entry_allowed_ts = max(worker._next_entry_allowed_ts, now_mono + cooldown_sec)
            worker._active_entry_cooldown_sec = cooldown_sec
            worker._active_entry_cooldown_outcome = outcome
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Entry cooldown activated",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "cooldown_sec": cooldown_sec,
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        worker._next_entry_allowed_ts,
                        now_monotonic=now_mono,
                        now_wall=now_wall,
                    ),
                    "cooldown_outcome": outcome,
                    "trade_pnl": position.pnl,
                    "strategy": worker.strategy_name,
                },
            )
        micro_chop_payload = worker._micro_chop_cooldown_payload(position, trade_performance)
        if micro_chop_payload is not None:
            worker._next_entry_allowed_ts = max(
                worker._next_entry_allowed_ts,
                now_mono + worker.micro_chop_cooldown_sec,
            )
            worker._active_entry_cooldown_sec = max(
                worker._active_entry_cooldown_sec,
                worker.micro_chop_cooldown_sec,
            )
            worker._active_entry_cooldown_outcome = "micro_chop"
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Micro-chop cooldown activated",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "cooldown_sec": worker.micro_chop_cooldown_sec,
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        worker._next_entry_allowed_ts,
                        now_monotonic=now_mono,
                        now_wall=now_wall,
                    ),
                    "trade_pnl": position.pnl,
                    **micro_chop_payload,
                    "strategy": worker.strategy_name,
                },
            )
        if outcome == "win":
            worker._register_continuation_reentry_on_profitable_close(position, reason=reason)
        if outcome == "win" and worker.same_side_reentry_win_cooldown_sec > 0:
            worker._same_side_reentry_block_side = position.side
            worker._same_side_reentry_block_until_ts = max(
                worker._same_side_reentry_block_until_ts,
                now_mono + worker.same_side_reentry_win_cooldown_sec,
            )
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Same-side reentry cooldown activated",
                {
                    "position_id": position.position_id,
                    "blocked_side": position.side.value,
                    "cooldown_sec": worker.same_side_reentry_win_cooldown_sec,
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        worker._same_side_reentry_block_until_ts,
                        now_monotonic=now_mono,
                        now_wall=now_wall,
                    ),
                    "trade_pnl": position.pnl,
                    "strategy": worker.strategy_name,
                },
            )

    def close_position(self, position: Position, close_price: float, reason: str) -> None:
        worker = self._worker
        state = self._state
        worker._allow_risk_reducing_action_after_lease_revocation(
            action="close",
            position=position,
            reason=reason,
        )
        broker_sync: dict[str, object] | None = None
        use_local_close_price = True
        if worker.mode == RunMode.EXECUTION:
            now = worker._monotonic_now()
            if state.pending_close_verification_position_id == position.position_id:
                retry_due = (
                    state.pending_close_retry_position_id == position.position_id
                    and worker._runtime_remaining_sec(
                        state.pending_close_retry_until_ts,
                        now_monotonic=now,
                    ) <= FLOAT_COMPARISON_TOLERANCE
                )
                if not retry_due:
                    return
            if (
                state.pending_close_retry_position_id == position.position_id
                and worker._runtime_remaining_sec(
                    state.pending_close_retry_until_ts,
                    now_monotonic=now,
                ) > FLOAT_COMPARISON_TOLERANCE
            ):
                wait_sec = worker._runtime_remaining_sec(
                    state.pending_close_retry_until_ts,
                    now_monotonic=now,
                )
                worker._record_close_deferred_by_allowance(
                    position=position,
                    reason=reason,
                    wait_sec=wait_sec,
                    error_text="allowance_backoff_cooldown_active",
                )
                return
            try:
                with worker._watchdog_blocking_operation(
                    "close_position",
                    last_price=worker._latest_price(),
                    grace_sec=180.0,
                ):
                    worker.broker.close_position(position)
                    state.pending_close_retry_position_id = None
                    state.pending_close_retry_until_ts = 0.0
                    state.pending_close_retry_reason = None
                    broker_sync = worker._get_broker_close_sync(position)
                broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
                use_local_close_price = broker_close_price is not None and broker_close_price > 0
                if not worker._broker_sync_has_factual_close_details(broker_sync):
                    use_local_close_price = False
                    # Persist the original close reason now so reconciliation
                    # paths (worker manual-close sync, bot runtime sync) can
                    # preserve it instead of overwriting with a generic
                    # "broker_reconcile" / "broker_manual_close" label.
                    worker.store.update_trade_performance(
                        position_id=position.position_id,
                        symbol=position.symbol,
                        close_reason=reason,
                        max_favorable_pips=0.0,
                        max_adverse_pips=0.0,
                        max_favorable_pnl=0.0,
                        max_adverse_pnl=0.0,
                    )
                    worker._arm_pending_close_verification(
                        position=position,
                        reason=reason,
                        broker_sync=broker_sync,
                    )
                    return
            except BrokerError as exc:
                error_text = str(exc)
                if worker._is_allowance_backoff_error(error_text):
                    backoff_sec = worker._handle_allowance_backoff_error(error_text)
                    now = worker._monotonic_now()
                    state.pending_close_retry_position_id = position.position_id
                    state.pending_close_retry_until_ts = max(
                        state.pending_close_retry_until_ts,
                        now + max(0.5, backoff_sec),
                    )
                    state.pending_close_retry_reason = str(reason)
                    worker._record_close_deferred_by_allowance(
                        position=position,
                        reason=reason,
                        wait_sec=worker._runtime_remaining_sec(
                            state.pending_close_retry_until_ts,
                            now_monotonic=now,
                        ),
                        error_text=error_text,
                    )
                    return
                if self.is_retryable_close_error(error_text):
                    now = worker._monotonic_now()
                    retry_sec = max(2.0, min(15.0, max(worker.poll_interval_sec * 2.0, 5.0)))
                    state.pending_close_retry_position_id = position.position_id
                    state.pending_close_retry_until_ts = max(
                        state.pending_close_retry_until_ts,
                        now + retry_sec,
                    )
                    state.pending_close_retry_reason = str(reason)
                    worker._record_close_deferred_for_retry(
                        position=position,
                        reason=reason,
                        wait_sec=worker._runtime_remaining_sec(
                            state.pending_close_retry_until_ts,
                            now_monotonic=now,
                        ),
                        error_text=error_text,
                    )
                    return
                if self.is_market_closed_close_error(error_text):
                    now = worker._monotonic_now()
                    retry_sec = max(60.0, min(900.0, max(worker.poll_interval_sec * 20.0, 180.0)))
                    state.pending_close_retry_position_id = position.position_id
                    state.pending_close_retry_until_ts = max(
                        state.pending_close_retry_until_ts,
                        now + retry_sec,
                    )
                    state.pending_close_retry_reason = str(reason)
                    worker._record_close_deferred_for_retry(
                        position=position,
                        reason=reason,
                        wait_sec=worker._runtime_remaining_sec(
                            state.pending_close_retry_until_ts,
                            now_monotonic=now,
                        ),
                        error_text=error_text,
                    )
                    return
                if not worker._is_broker_position_missing_error(error_text):
                    raise
                broker_sync = worker._get_broker_close_sync(position)
                broker_close_price = worker._safe_float((broker_sync or {}).get("close_price"))
                use_local_close_price = broker_close_price is not None and broker_close_price > 0
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Broker position missing on close",
                    {
                        "position_id": position.position_id,
                        "requested_reason": reason,
                        "close_details_pending_broker_sync": not use_local_close_price,
                        "broker_close_sync": broker_sync,
                        "broker_error": error_text,
                    },
                )
                if not use_local_close_price and not isinstance(broker_sync, dict):
                    broker_sync = {
                        "position_id": position.position_id,
                        "source": "broker_position_missing_on_close",
                        "position_found": False,
                    }
                reason = "broker_position_missing_on_close"
                state.pending_close_retry_position_id = None
                state.pending_close_retry_until_ts = 0.0
                state.pending_close_retry_reason = None
                worker._clear_pending_close_verification(position.position_id)

        self.finalize_position_close(
            position,
            close_price,
            reason,
            broker_sync=broker_sync,
            use_local_close_price=use_local_close_price,
            estimate_close_price=(close_price if not use_local_close_price and close_price > 0 else None),
            pnl_estimate_source=(
                "requested_close_price"
                if not use_local_close_price and close_price > 0
                else None
            ),
        )

    def maybe_execute_pending_close_retry(
        self,
        position: Position,
        tick: PriceTick | None,
    ) -> bool:
        worker = self._worker
        state = self._state
        if worker.mode != RunMode.EXECUTION:
            return False
        if state.pending_close_retry_position_id != position.position_id:
            return False
        now = worker._monotonic_now()
        if worker._runtime_remaining_sec(
            state.pending_close_retry_until_ts,
            now_monotonic=now,
        ) > FLOAT_COMPARISON_TOLERANCE:
            return False

        retry_reason = str(state.pending_close_retry_reason or "").strip() or "deferred_close_retry"
        close_price = (
            (tick.bid if position.side == Side.BUY else tick.ask)
            if tick is not None
            else position.open_price
        )
        try:
            worker._close_position(position, close_price, retry_reason)
        except BrokerError as exc:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Deferred close retry failed",
                {
                    "position_id": position.position_id,
                    "reason": retry_reason,
                    "error": str(exc),
                },
            )
        return True

    def handle_active_position_without_fresh_tick(self, active: Position, last_error: str) -> None:
        worker = self._worker
        if worker._attempt_pending_missing_position_reconciliation(active, None):
            return

        latest_active = worker.position_book.get(worker.symbol)
        if latest_active is None:
            return
        if worker._maybe_reconcile_execution_manual_close(latest_active, None):
            return

        latest_active = worker.position_book.get(worker.symbol)
        if latest_active is None:
            return
        if self.maybe_execute_pending_close_retry(latest_active, None):
            return
