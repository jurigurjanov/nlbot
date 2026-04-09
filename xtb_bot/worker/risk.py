from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from contextlib import nullcontext
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
import logging
import math
from typing import Any

from xtb_bot.config import resolve_strategy_param
from xtb_bot.models import Position, Signal, Side


logger = logging.getLogger("xtb_bot.worker")

_INDEX_HYBRID_MEAN_REVERSION_CONFIDENCE_FLOOR_CAP_BUFFER = 0.03
_INDEX_TREND_SESSION_OPEN_PERSISTENCE_RELIEF_WINDOW_MINUTES = 30.0
_INDEX_TREND_SESSION_OPEN_PERSISTENCE_SEC_RATIO = 0.5
_OIL_CONTRACT_CONFIDENCE_THRESHOLD_CAP = 0.45
_OIL_CONTRACT_SYMBOLS = frozenset({"WTI", "BRENT", "USOIL", "UKOIL"})


@dataclass(slots=True)
class WorkerRiskState:
    next_entry_allowed_ts: float = 0.0
    active_entry_cooldown_sec: float = 0.0
    active_entry_cooldown_outcome: str = "none"
    last_confidence_block_log_ts: float = 0.0
    last_cooldown_block_log_ts: float = 0.0
    entry_signal_persistence_signature: tuple[str, ...] | None = None
    entry_signal_persistence_first_ts: float = 0.0
    entry_signal_persistence_seen_count: int = 0
    last_entry_signal_persistence_log_ts: float = 0.0
    last_pending_open_block_log_ts: float = 0.0
    same_side_reentry_block_side: Side | None = None
    same_side_reentry_block_until_ts: float = 0.0
    last_same_side_reentry_block_log_ts: float = 0.0
    continuation_reentry_block_side: Side | None = None
    continuation_reentry_block_signal: str | None = None
    continuation_reentry_block_armed_at_ts: float = 0.0
    continuation_reentry_post_win_exit_price: float | None = None
    continuation_reentry_post_win_stop_distance: float | None = None
    last_continuation_reentry_block_log_ts: float = 0.0
    stale_entry_tick_streak: int = 0
    last_entry_tick_stale_log_ts: float = 0.0
    operational_guard_until_ts: float = 0.0
    operational_guard_reason: str | None = None
    last_operational_guard_event_ts: float = 0.0


class WorkerRiskEvaluator:
    def __init__(self, worker: Any) -> None:
        self._worker = worker
        self._state = WorkerRiskState()

    @property
    def state(self) -> WorkerRiskState:
        return self._state

    def _spread_lock(self):
        lock = getattr(self._worker, "_spread_buffer_lock", None)
        return lock if lock is not None else nullcontext()

    def spread_buffer_snapshot(self) -> list[float]:
        worker = self._worker
        with self._spread_lock():
            return list(worker.spreads_pips)

    def append_spread_sample(self, spread_pips: float) -> None:
        worker = self._worker
        with self._spread_lock():
            worker.spreads_pips.append(float(spread_pips))

    def replace_latest_spread_sample(self, spread_pips: float) -> None:
        worker = self._worker
        with self._spread_lock():
            if worker.spreads_pips:
                worker.spreads_pips[-1] = float(spread_pips)
            else:
                worker.spreads_pips.append(float(spread_pips))

    def latest_spread_pips(self) -> float | None:
        spreads = self.spread_buffer_snapshot()
        if not spreads:
            return None
        try:
            latest = float(spreads[-1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(latest):
            return None
        return latest

    @staticmethod
    def is_open_reject_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        if "ig deal rejected:" in lowered:
            return True
        if "requested size below broker minimum" in lowered:
            return True
        return "ig api post /positions/otc failed" in lowered

    def resolve_mode_scoped_strategy_param(
        self,
        strategy_params: dict[str, object] | None,
        strategy_key: str,
        suffix: str,
    ) -> object | None:
        worker = self._worker
        if not isinstance(strategy_params, dict) or not strategy_params:
            return None
        normalized_strategy_key = str(strategy_key or "").strip().lower()
        if not normalized_strategy_key:
            return None
        mode_value = worker.mode.value if hasattr(worker.mode, "value") else str(worker.mode or "")
        mode_key = mode_value.strip().lower()
        candidates: list[str] = []
        if mode_key:
            candidates.append(f"{normalized_strategy_key}_{mode_key}_{suffix}")
        candidates.append(f"{normalized_strategy_key}_{suffix}")
        for key in candidates:
            if key in strategy_params:
                return strategy_params[key]
        return None

    def effective_index_hybrid_regime_from_signal(self, signal: Signal) -> str | None:
        worker = self._worker
        entry_signal = str(worker._effective_entry_signal_from_signal(signal) or "").strip().lower()
        if entry_signal.startswith("index_hybrid:"):
            regime = entry_signal.split(":", 1)[1].strip().lower()
            if regime in {"mean_reversion", "trend_following"}:
                return regime
        metadata = worker._signal_metadata(signal)
        regime = str(metadata.get("regime") or "").strip().lower()
        if regime in {"mean_reversion", "trend_following"}:
            return regime
        return None

    def effective_confidence_hard_floor_for_signal(self, signal: Signal) -> float | None:
        worker = self._worker
        entry_strategy = worker._effective_entry_component_from_signal(signal)
        entry_params = worker._strategy_params_map.get(entry_strategy)
        if not isinstance(entry_params, dict) or not entry_params:
            return None

        resolved_floor: object = None
        if entry_strategy == "index_hybrid":
            regime = self.effective_index_hybrid_regime_from_signal(signal)
            if regime in {"mean_reversion", "trend_following"}:
                resolved_floor = self.resolve_mode_scoped_strategy_param(
                    entry_params,
                    f"{entry_strategy}_{regime}",
                    "confidence_hard_floor",
                )
        if resolved_floor is None:
            resolved_floor = resolve_strategy_param(
                entry_params,
                entry_strategy,
                "confidence_hard_floor",
                0.0,
                mode=worker.mode,
            )
        try:
            floor = float(resolved_floor)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(floor) or floor <= 0.0:
            return None
        return max(0.0, min(1.0, floor))

    def derived_confidence_threshold_cap_for_signal(self, signal: Signal) -> float | None:
        worker = self._worker
        if str(worker.symbol or "").strip().upper() not in _OIL_CONTRACT_SYMBOLS:
            return None
        if self.signal_management_family(signal) != "trend":
            return None
        if worker._effective_entry_component_from_signal(signal) != "oil":
            return None
        cap = float(_OIL_CONTRACT_CONFIDENCE_THRESHOLD_CAP)
        if not math.isfinite(cap) or cap <= 0.0:
            return None
        return max(0.0, min(1.0, cap))

    def activate_operational_guard(
        self,
        *,
        reason: str,
        now_ts: float | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        worker = self._worker
        state = self._state
        if not worker.operational_guard_enabled:
            return
        now = worker._monotonic_now() if now_ts is None else float(now_ts)
        now_wall = worker._wall_time_now()
        state.operational_guard_until_ts = max(
            state.operational_guard_until_ts,
            now + worker.operational_guard_cooldown_sec,
        )
        reason_changed = reason != state.operational_guard_reason
        cooldown_passed = (now - state.last_operational_guard_event_ts) >= worker.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            active_for_sec = worker._runtime_remaining_sec(
                state.operational_guard_until_ts,
                now_monotonic=now,
            )
            event_payload: dict[str, object] = {
                "reason": reason,
                "active_until_ts": worker._wall_time_for_runtime_ts(
                    state.operational_guard_until_ts,
                    now_monotonic=now,
                    now_wall=now_wall,
                ),
                "active_for_sec": round(active_for_sec, 2),
                "stale_entry_tick_streak": state.stale_entry_tick_streak,
                "allowance_backoff_streak": worker._allowance_backoff_streak,
            }
            if payload:
                event_payload.update(payload)
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Operational guard activated",
                event_payload,
            )
            state.last_operational_guard_event_ts = now
            state.operational_guard_reason = reason

    def operational_guard_active(self, *, now_ts: float | None = None) -> bool:
        worker = self._worker
        state = self._state
        if not worker.operational_guard_enabled:
            return False
        now = worker._monotonic_now() if now_ts is None else float(now_ts)
        return worker._runtime_remaining_sec(
            state.operational_guard_until_ts,
            now_monotonic=now,
        ) > FLOAT_COMPARISON_TOLERANCE

    def operational_guard_allows_open(self) -> bool:
        worker = self._worker
        state = self._state
        if not self.operational_guard_active():
            return True
        now = worker._monotonic_now()
        if (now - worker._last_hold_reason_log_ts) >= worker.hold_reason_log_interval_sec:
            active_for_sec = worker._runtime_remaining_sec(
                state.operational_guard_until_ts,
                now_monotonic=now,
            )
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by operational guard",
                {
                    "active_for_sec": round(active_for_sec, 2),
                    "reason": state.operational_guard_reason,
                    "stale_entry_tick_streak": state.stale_entry_tick_streak,
                    "allowance_backoff_streak": worker._allowance_backoff_streak,
                },
            )
            worker._last_hold_reason_log_ts = now
        return False

    def apply_operational_guard_risk_reduction(
        self,
        position: Position,
        *,
        bid: float,
        ask: float,
    ) -> None:
        worker = self._worker
        if not self.operational_guard_active():
            return
        if not worker.operational_guard_reduce_risk_on_profit:
            return
        mark = bid if position.side == Side.BUY else ask
        pnl = worker._calculate_pnl(position, mark)
        if pnl + FLOAT_COMPARISON_TOLERANCE < worker.operational_guard_reduce_risk_min_pnl:
            return
        worker._apply_breakeven_protection(
            position,
            trigger="operational_guard",
            bid=bid,
            ask=ask,
        )

    def spread_in_pips(self, bid: float, ask: float) -> float:
        worker = self._worker
        pip_size = worker._execution_pip_size()
        return max(0.0, (ask - bid) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))

    @staticmethod
    def spread_in_pct(bid: float, ask: float) -> float:
        if bid <= 0 and ask <= 0:
            return 0.0
        if bid <= 0:
            mid = ask
        elif ask <= 0:
            mid = bid
        else:
            mid = (bid + ask) / 2.0
        if mid <= 0:
            return 0.0
        return max(0.0, ((ask - bid) / mid) * 100.0)

    def derived_confidence_hard_floor_for_signal(
        self,
        signal: Signal,
        *,
        confidence_threshold_cap: float | None = None,
    ) -> float | None:
        worker = self._worker
        if worker.mode.name != "EXECUTION":
            return None
        entry_strategy = worker._effective_entry_component_from_signal(signal)
        if entry_strategy != "index_hybrid":
            return None
        regime = self.effective_index_hybrid_regime_from_signal(signal)
        if regime != "mean_reversion":
            return None

        cap = confidence_threshold_cap
        if cap is None:
            metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
            cap_raw = metadata.get("confidence_threshold_cap")
            try:
                cap = float(cap_raw) if cap_raw is not None else None
            except (TypeError, ValueError):
                cap = None
        if cap is None or not math.isfinite(cap) or cap <= 0.0:
            return None

        floor = float(cap) - _INDEX_HYBRID_MEAN_REVERSION_CONFIDENCE_FLOOR_CAP_BUFFER
        if not math.isfinite(floor) or floor <= 0.0:
            return None
        return max(0.0, min(1.0, floor))

    def effective_min_confidence_for_signal(self, signal: Signal) -> tuple[float, float | None]:
        worker = self._worker
        state = self._state
        _ = state
        threshold = max(0.0, min(1.0, float(worker.min_confidence_for_entry)))
        entry_strategy = worker._effective_entry_component_from_signal(signal)
        entry_params = worker._strategy_params_map.get(entry_strategy)
        if isinstance(entry_params, dict) and entry_params:
            resolved_threshold: object = None
            if entry_strategy == "index_hybrid":
                regime = self.effective_index_hybrid_regime_from_signal(signal)
                if regime in {"mean_reversion", "trend_following"}:
                    resolved_threshold = self.resolve_mode_scoped_strategy_param(
                        entry_params,
                        f"{entry_strategy}_{regime}",
                        "min_confidence_for_entry",
                    )
            if resolved_threshold is None:
                resolved_threshold = resolve_strategy_param(
                    entry_params,
                    entry_strategy,
                    "min_confidence_for_entry",
                    threshold,
                    mode=worker.mode,
                )
            try:
                threshold = max(0.0, min(1.0, float(resolved_threshold)))
            except (TypeError, ValueError):
                pass
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        cap_raw = metadata.get("confidence_threshold_cap")
        cap: float | None = None
        if cap_raw is not None:
            try:
                parsed = float(cap_raw)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None and math.isfinite(parsed) and parsed > 0.0:
                cap = max(0.0, min(1.0, parsed))
        derived_cap = self.derived_confidence_threshold_cap_for_signal(signal)
        if derived_cap is not None:
            cap = derived_cap if cap is None else min(cap, derived_cap)
        if cap is not None:
            threshold = min(threshold, cap)
        hard_floor = self.effective_confidence_hard_floor_for_signal(signal)
        if hard_floor is None:
            hard_floor = self.derived_confidence_hard_floor_for_signal(
                signal,
                confidence_threshold_cap=cap,
            )
        if hard_floor is not None:
            threshold = max(threshold, hard_floor)
        return threshold, cap

    def confidence_allows_open(self, signal: Signal) -> bool:
        worker = self._worker
        state = self._state
        min_confidence_for_entry, confidence_threshold_cap = self.effective_min_confidence_for_signal(signal)
        if min_confidence_for_entry <= 0:
            return True

        confidence = max(0.0, min(1.0, float(signal.confidence)))
        if confidence + FLOAT_ROUNDING_TOLERANCE >= min_confidence_for_entry:
            return True

        confidence_hard_floor = self.effective_confidence_hard_floor_for_signal(signal)
        if confidence_hard_floor is None:
            confidence_hard_floor = self.derived_confidence_hard_floor_for_signal(
                signal,
                confidence_threshold_cap=confidence_threshold_cap,
            )
        now = worker._monotonic_now()
        if (now - state.last_confidence_block_log_ts) >= worker.hold_reason_log_interval_sec:
            entry_strategy = worker._effective_entry_strategy_from_signal(signal)
            entry_component = worker._effective_entry_component_from_signal(signal)
            entry_signal = worker._effective_entry_signal_from_signal(signal)
            payload: dict[str, object] = {
                "signal": signal.side.value,
                "confidence": confidence,
                "min_confidence_for_entry": min_confidence_for_entry,
                "min_confidence_for_entry_configured": worker.min_confidence_for_entry,
                "strategy": worker.strategy_name,
                "entry_strategy": entry_strategy,
                "entry_component": entry_component,
            }
            if entry_signal is not None:
                payload["entry_signal"] = entry_signal
            if confidence_threshold_cap is not None:
                payload["confidence_threshold_cap"] = confidence_threshold_cap
            if confidence_hard_floor is not None:
                payload["confidence_hard_floor"] = confidence_hard_floor
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by confidence threshold",
                payload,
            )
            logger.warning(
                "Trade blocked by confidence threshold | symbol=%s confidence=%.4f threshold=%.4f configured=%.4f strategy=%s cap=%s",
                worker.symbol,
                confidence,
                min_confidence_for_entry,
                worker.min_confidence_for_entry,
                worker.strategy_name,
                confidence_threshold_cap,
            )
            state.last_confidence_block_log_ts = now
        return False

    @staticmethod
    def trend_signal_from_signal(signal: Signal | None) -> str | None:
        if signal is None:
            return None
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        raw = metadata.get("trend_signal")
        text = str(raw or "").strip().lower()
        if not text or text == "none":
            return None
        return text

    @staticmethod
    def trend_signal_direction(trend_signal: str | None) -> Side | None:
        if not trend_signal:
            return None
        if trend_signal.endswith("_up"):
            return Side.BUY
        if trend_signal.endswith("_down"):
            return Side.SELL
        return None

    @staticmethod
    def is_cross_trend_signal(trend_signal: str | None) -> bool:
        if not trend_signal:
            return False
        return "_cross_" in trend_signal or trend_signal.startswith("cross_")

    @staticmethod
    def is_continuation_trend_signal(trend_signal: str | None) -> bool:
        if not trend_signal:
            return False
        return "_trend_" in trend_signal or trend_signal.startswith("trend_")

    def clear_continuation_reentry_block(self) -> None:
        state = self._state
        state.continuation_reentry_block_side = None
        state.continuation_reentry_block_signal = None
        state.continuation_reentry_block_armed_at_ts = 0.0
        state.continuation_reentry_post_win_exit_price = None
        state.continuation_reentry_post_win_stop_distance = None

    def post_win_same_side_cross_allows_reentry(
        self,
        direction: Side,
        *,
        now_ts: float,
    ) -> tuple[bool, dict[str, object]]:
        worker = self._worker
        state = self._state
        if not worker.continuation_reentry_post_win_reset_guard_enabled:
            return True, {}
        if state.continuation_reentry_block_signal != "post_win_exit":
            return True, {}

        exit_price = state.continuation_reentry_post_win_exit_price
        stop_distance = state.continuation_reentry_post_win_stop_distance
        armed_at_ts = state.continuation_reentry_block_armed_at_ts
        if (
            exit_price is None
            or stop_distance is None
            or stop_distance <= FLOAT_COMPARISON_TOLERANCE
            or armed_at_ts <= 0.0
        ):
            return True, {}

        block_age_sec = max(
            0.0,
            worker._runtime_age_sec(
                armed_at_ts,
                now_monotonic=now_ts,
                now_wall=worker._wall_time_now(),
            )
            or 0.0,
        )
        if (
            worker.continuation_reentry_post_win_max_age_sec > 0.0
            and block_age_sec + FLOAT_COMPARISON_TOLERANCE >= worker.continuation_reentry_post_win_max_age_sec
        ):
            return True, {
                "post_win_reset_guard_expired": True,
                "block_age_sec": round(block_age_sec, 3),
                "max_age_sec": worker.continuation_reentry_post_win_max_age_sec,
            }

        required_reset_distance = max(
            0.0,
            stop_distance * worker.continuation_reentry_post_win_min_reset_stop_ratio,
        )
        if required_reset_distance <= FLOAT_COMPARISON_TOLERANCE:
            return True, {}

        history_prices, history_timestamps, _history_volumes = worker._history_runtime.history_snapshot()
        armed_at_wall_ts = worker._wall_time_for_runtime_ts(
            armed_at_ts,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        prices_since_block = [
            float(price)
            for price, ts in zip(history_prices, history_timestamps)
            if armed_at_wall_ts is not None and ts + FLOAT_COMPARISON_TOLERANCE >= armed_at_wall_ts
        ]
        latest_price = worker._latest_price()
        if not prices_since_block and latest_price is not None:
            prices_since_block = [latest_price]
        if not prices_since_block:
            return False, {
                "post_win_reset_guard": True,
                "post_win_exit_price": exit_price,
                "required_reset_distance_price": required_reset_distance,
                "observed_reset_distance_price": 0.0,
                "block_age_sec": round(block_age_sec, 3),
                "samples_since_block": 0,
            }

        if direction == Side.BUY:
            reset_anchor_price = min(prices_since_block)
            observed_reset_distance = max(0.0, exit_price - reset_anchor_price)
        else:
            reset_anchor_price = max(prices_since_block)
            observed_reset_distance = max(0.0, reset_anchor_price - exit_price)

        return observed_reset_distance + FLOAT_COMPARISON_TOLERANCE >= required_reset_distance, {
            "post_win_reset_guard": True,
            "post_win_exit_price": exit_price,
            "post_win_stop_distance_price": stop_distance,
            "required_reset_distance_price": required_reset_distance,
            "observed_reset_distance_price": observed_reset_distance,
            "reset_anchor_price": reset_anchor_price,
            "block_age_sec": round(block_age_sec, 3),
            "max_age_sec": worker.continuation_reentry_post_win_max_age_sec,
            "samples_since_block": len(prices_since_block),
        }

    def continuation_reentry_allows_open(self, signal_side: Side, signal: Signal | None) -> bool:
        worker = self._worker
        state = self._state
        if not worker.continuation_reentry_guard_enabled:
            return True
        trend_signal = self.trend_signal_from_signal(signal)
        if not trend_signal:
            return True

        direction = self.trend_signal_direction(trend_signal) or signal_side
        blocked_side = state.continuation_reentry_block_side

        if self.is_cross_trend_signal(trend_signal):
            now = worker._monotonic_now()
            if blocked_side == direction:
                allowed, reset_payload = self.post_win_same_side_cross_allows_reentry(
                    direction,
                    now_ts=now,
                )
                if not allowed:
                    if (now - state.last_continuation_reentry_block_log_ts) >= worker.hold_reason_log_interval_sec:
                        worker.store.record_event(
                            "INFO",
                            worker.symbol,
                            "Trade blocked by post-win reentry reset guard",
                            {
                                "signal": signal_side.value,
                                "trend_signal": trend_signal,
                                "blocked_side": blocked_side.value,
                                "blocked_by_trend_signal": state.continuation_reentry_block_signal,
                                "strategy": worker.strategy_name,
                                **reset_payload,
                            },
                        )
                        state.last_continuation_reentry_block_log_ts = now
                    return False
                self.clear_continuation_reentry_block()
            elif (
                blocked_side is not None
                and blocked_side != direction
                and worker.continuation_reentry_reset_on_opposite_signal
            ):
                self.clear_continuation_reentry_block()
            return True

        if not self.is_continuation_trend_signal(trend_signal):
            return True

        if blocked_side is None:
            return True
        if blocked_side != direction:
            if worker.continuation_reentry_reset_on_opposite_signal:
                self.clear_continuation_reentry_block()
            return True

        now = worker._monotonic_now()
        if (now - state.last_continuation_reentry_block_log_ts) >= worker.hold_reason_log_interval_sec:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Trade blocked by continuation reentry guard",
                {
                    "signal": signal_side.value,
                    "trend_signal": trend_signal,
                    "blocked_side": blocked_side.value,
                    "blocked_by_trend_signal": state.continuation_reentry_block_signal,
                    "strategy": worker.strategy_name,
                },
            )
            state.last_continuation_reentry_block_log_ts = now
        return False

    def register_continuation_reentry_on_open(self, signal: Signal | None) -> None:
        worker = self._worker
        state = self._state
        if not worker.continuation_reentry_guard_enabled:
            return
        trend_signal = self.trend_signal_from_signal(signal)
        if not self.is_continuation_trend_signal(trend_signal):
            return
        direction = self.trend_signal_direction(trend_signal)
        if direction is None:
            return

        state.continuation_reentry_block_side = direction
        state.continuation_reentry_block_signal = trend_signal
        state.continuation_reentry_block_armed_at_ts = worker._monotonic_now()
        state.continuation_reentry_post_win_exit_price = None
        state.continuation_reentry_post_win_stop_distance = None
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Continuation reentry guard armed",
            {
                "blocked_side": direction.value,
                "trend_signal": trend_signal,
                "strategy": worker.strategy_name,
            },
        )

    def register_continuation_reentry_on_profitable_close(
        self,
        position: Position,
        *,
        reason: str,
    ) -> None:
        worker = self._worker
        state = self._state
        if not worker.continuation_reentry_guard_enabled:
            return
        if worker._position_management_family(position) != "trend":
            return

        state.continuation_reentry_block_side = position.side
        state.continuation_reentry_block_signal = "post_win_exit"
        state.continuation_reentry_block_armed_at_ts = worker._monotonic_now()
        state.continuation_reentry_post_win_exit_price = float(
            position.close_price if position.close_price is not None else position.open_price
        )
        state.continuation_reentry_post_win_stop_distance = abs(position.open_price - position.stop_loss)
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Continuation reentry guard armed after profitable exit",
            {
                "position_id": position.position_id,
                "blocked_side": position.side.value,
                "reason": reason,
                "strategy": worker.strategy_name,
                "exit_price": state.continuation_reentry_post_win_exit_price,
                "stop_distance_price": state.continuation_reentry_post_win_stop_distance,
            },
        )

    def same_side_reentry_allows_open(self, signal_side: Side) -> bool:
        worker = self._worker
        state = self._state
        if worker.same_side_reentry_win_cooldown_sec <= 0:
            return True

        blocked_side = state.same_side_reentry_block_side
        if blocked_side is None:
            return True

        now = worker._monotonic_now()
        remaining = worker._runtime_remaining_sec(
            state.same_side_reentry_block_until_ts,
            now_monotonic=now,
        )
        if remaining <= 0:
            state.same_side_reentry_block_side = None
            state.same_side_reentry_block_until_ts = 0.0
            return True

        if signal_side != blocked_side:
            if worker.same_side_reentry_reset_on_opposite_signal:
                state.same_side_reentry_block_side = None
                state.same_side_reentry_block_until_ts = 0.0
            return True

        if (now - state.last_same_side_reentry_block_log_ts) >= worker.hold_reason_log_interval_sec:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Trade blocked by same-side reentry cooldown",
                {
                    "signal": signal_side.value,
                    "blocked_side": blocked_side.value,
                    "cooldown_sec": worker.same_side_reentry_win_cooldown_sec,
                    "remaining_sec": round(remaining, 3),
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        state.same_side_reentry_block_until_ts,
                        now_monotonic=now,
                        now_wall=worker._wall_time_now(),
                    ),
                    "strategy": worker.strategy_name,
                },
            )
            state.last_same_side_reentry_block_log_ts = now
        return False

    def cooldown_allows_open(self, signal_side: Side, signal: Signal | None = None) -> bool:
        worker = self._worker
        state = self._state
        if not self.continuation_reentry_allows_open(signal_side, signal):
            return False
        if not self.same_side_reentry_allows_open(signal_side):
            return False

        now = worker._monotonic_now()
        remaining = worker._runtime_remaining_sec(
            state.next_entry_allowed_ts,
            now_monotonic=now,
        )
        if remaining <= 0:
            state.active_entry_cooldown_sec = 0.0
            state.active_entry_cooldown_outcome = "none"
            return True

        if (now - state.last_cooldown_block_log_ts) >= worker.hold_reason_log_interval_sec:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Trade blocked by entry cooldown",
                {
                    "signal": signal_side.value,
                    "cooldown_sec": state.active_entry_cooldown_sec,
                    "remaining_sec": round(remaining, 3),
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        state.next_entry_allowed_ts,
                        now_monotonic=now,
                        now_wall=worker._wall_time_now(),
                    ),
                    "cooldown_outcome": state.active_entry_cooldown_outcome,
                    "strategy": worker.strategy_name,
                },
            )
            state.last_cooldown_block_log_ts = now
        return False

    @staticmethod
    def cooldown_outcome_from_pnl(pnl: float) -> str:
        if pnl > FLOAT_COMPARISON_TOLERANCE:
            return "win"
        if pnl < -FLOAT_COMPARISON_TOLERANCE:
            return "loss"
        return "flat"

    def cooldown_duration_for_outcome(self, outcome: str) -> float:
        worker = self._worker
        if outcome == "win":
            return worker.trade_cooldown_win_sec
        if outcome == "loss":
            return worker.trade_cooldown_loss_sec
        return worker.trade_cooldown_flat_sec

    def clear_entry_signal_persistence_state(self) -> None:
        state = self._state
        state.entry_signal_persistence_signature = None
        state.entry_signal_persistence_first_ts = 0.0
        state.entry_signal_persistence_seen_count = 0

    def signal_management_family(self, signal: Signal | None) -> str:
        worker = self._worker
        if signal is None:
            return "neutral"
        strategy_name = worker._effective_entry_strategy_from_signal(signal)
        metadata = worker._signal_metadata(signal)
        if strategy_name == "mean_reversion_bb":
            return "mean_reversion"
        if strategy_name == "index_hybrid":
            regime = str(metadata.get("regime") or "").strip().lower()
            if regime == "mean_reversion":
                return "mean_reversion"
            return "trend"
        if strategy_name in {
            "momentum",
            "g1",
            "oil",
            "trend_following",
            "crypto_trend_following",
            "donchian_breakout",
            "mean_breakout_v2",
        }:
            return "trend"
        return "neutral"

    def entry_signal_persistence_signature_for_signal(self, signal: Signal) -> tuple[str, ...]:
        worker = self._worker
        metadata = worker._signal_metadata(signal)
        return (
            signal.side.value,
            worker._effective_entry_strategy_from_signal(signal),
            worker._effective_entry_component_from_signal(signal),
            str(worker._effective_entry_signal_from_signal(signal) or "").strip().lower(),
            str(metadata.get("indicator") or "").strip().lower(),
            str(metadata.get("regime") or "").strip().lower(),
            str(metadata.get("trend") or "").strip().lower(),
        )

    def is_index_hybrid_trend_entry_signal(self, signal: Signal) -> bool:
        worker = self._worker
        if not (worker._is_index_symbol() or worker._is_commodity_symbol()):
            return False
        if worker._effective_entry_component_from_signal(signal) != "index_hybrid":
            return False
        metadata = worker._signal_metadata(signal)
        regime = str(metadata.get("regime") or "").strip().lower()
        if regime != "trend_following":
            return False
        return self.signal_management_family(signal) == "trend"

    def index_trend_session_open_persistence_relief_active(self, signal: Signal) -> bool:
        worker = self._worker
        if not self.is_index_hybrid_trend_entry_signal(signal):
            return False
        metadata = worker._signal_metadata(signal)
        minutes_raw = metadata.get("minutes_since_trend_session_open")
        try:
            minutes_since_open = float(minutes_raw)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(minutes_since_open) or minutes_since_open < 0.0:
            return False
        delay_raw = metadata.get("index_trend_session_open_delay_minutes")
        try:
            delay_minutes = max(0.0, float(delay_raw))
        except (TypeError, ValueError):
            delay_minutes = 0.0
        relief_window_minutes = max(
            _INDEX_TREND_SESSION_OPEN_PERSISTENCE_RELIEF_WINDOW_MINUTES,
            delay_minutes * 2.0,
        )
        return minutes_since_open <= (relief_window_minutes + FLOAT_COMPARISON_TOLERANCE)

    def entry_signal_persistence_requirements(self, signal: Signal) -> tuple[float, int]:
        worker = self._worker
        min_persistence_sec = worker.entry_signal_min_persistence_sec
        min_consecutive_evals = worker.entry_signal_min_consecutive_evals
        if self.is_index_hybrid_trend_entry_signal(signal):
            min_persistence_sec = max(
                min_persistence_sec,
                worker.entry_signal_index_trend_min_persistence_sec,
            )
            min_consecutive_evals = max(
                min_consecutive_evals,
                worker.entry_signal_index_trend_min_consecutive_evals,
            )
            if self.index_trend_session_open_persistence_relief_active(signal):
                relieved_persistence_sec = max(
                    worker.entry_signal_min_persistence_sec,
                    min_persistence_sec * _INDEX_TREND_SESSION_OPEN_PERSISTENCE_SEC_RATIO,
                )
                relieved_consecutive_evals = max(
                    worker.entry_signal_min_consecutive_evals,
                    min_consecutive_evals - 1,
                )
                min_persistence_sec = min(min_persistence_sec, relieved_persistence_sec)
                min_consecutive_evals = min(min_consecutive_evals, relieved_consecutive_evals)
        return min_persistence_sec, min_consecutive_evals

    def entry_signal_persistence_allows_open(self, signal: Signal) -> bool:
        worker = self._worker
        state = self._state
        if signal.side == Side.HOLD:
            self.clear_entry_signal_persistence_state()
            return True
        if not worker.entry_signal_persistence_enabled:
            return True
        if not (worker._is_index_symbol() or worker._is_commodity_symbol()):
            return True
        if (
            worker.entry_signal_persistence_trend_only
            and self.signal_management_family(signal) != "trend"
        ):
            return True
        min_persistence_sec, min_consecutive_evals = self.entry_signal_persistence_requirements(signal)
        metadata = worker._signal_metadata(signal)
        session_open_relief_active = self.index_trend_session_open_persistence_relief_active(signal)
        if min_persistence_sec <= 0.0 and min_consecutive_evals <= 1:
            return True

        signature = self.entry_signal_persistence_signature_for_signal(signal)
        now = worker._monotonic_now()
        if signature != state.entry_signal_persistence_signature:
            state.entry_signal_persistence_signature = signature
            state.entry_signal_persistence_first_ts = now
            state.entry_signal_persistence_seen_count = 1
        else:
            state.entry_signal_persistence_seen_count += 1

        elapsed_sec = max(0.0, now - state.entry_signal_persistence_first_ts)
        if (
            state.entry_signal_persistence_seen_count >= min_consecutive_evals
            and elapsed_sec + FLOAT_COMPARISON_TOLERANCE >= min_persistence_sec
        ):
            return True

        if (now - state.last_entry_signal_persistence_log_ts) >= worker.hold_reason_log_interval_sec:
            payload: dict[str, object] = {
                "signal": signal.side.value,
                "entry_strategy": worker._effective_entry_strategy_from_signal(signal),
                "entry_component": worker._effective_entry_component_from_signal(signal),
                "entry_signal": worker._effective_entry_signal_from_signal(signal),
                "seen_count": state.entry_signal_persistence_seen_count,
                "required_count": min_consecutive_evals,
                "elapsed_sec": round(elapsed_sec, 3),
                "required_persistence_sec": min_persistence_sec,
                "trend_only": worker.entry_signal_persistence_trend_only,
            }
            if session_open_relief_active:
                payload["session_open_persistence_relief_active"] = True
                minutes_since_open = metadata.get("minutes_since_trend_session_open")
                try:
                    parsed_minutes_since_open = float(minutes_since_open)
                except (TypeError, ValueError):
                    parsed_minutes_since_open = None
                if parsed_minutes_since_open is not None and math.isfinite(parsed_minutes_since_open):
                    payload["minutes_since_trend_session_open"] = parsed_minutes_since_open
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Entry waiting for signal persistence",
                payload,
            )
            state.last_entry_signal_persistence_log_ts = now
        return False

    def pending_open_conflicts_with_new_entry(self) -> bool:
        worker = self._worker
        state = self._state
        if worker.mode.name != "EXECUTION":
            return False
        if worker.entry_pending_open_block_window_sec <= 0.0:
            return False
        now = worker._monotonic_now()
        for pending in worker.store.load_pending_opens(mode=worker.mode.value):
            if str(pending.symbol or "").strip().upper() != worker.symbol.upper():
                continue
            created_at = float(pending.created_at or 0.0)
            pending_age_sec = worker._pending_open_age_sec(
                pending,
                now_monotonic=now,
                now_wall=worker._wall_time_now(),
            )
            if (
                pending_age_sec is not None
                and pending_age_sec > worker.entry_pending_open_block_window_sec
            ):
                continue
            if (now - state.last_pending_open_block_log_ts) >= worker.hold_reason_log_interval_sec:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Trade blocked by pending open guard",
                    {
                        "pending_id": pending.pending_id,
                        "pending_position_id": pending.position_id,
                        "created_at": created_at,
                        "age_sec": round(max(0.0, pending_age_sec or 0.0), 3),
                        "guard_window_sec": worker.entry_pending_open_block_window_sec,
                        "strategy": worker.strategy_name,
                    },
                )
                state.last_pending_open_block_log_ts = now
            return True
        return False

    def index_trend_structure_allows_open(
        self,
        signal: Signal,
        *,
        current_spread_pips: float,
    ) -> bool:
        worker = self._worker
        if not self.is_index_hybrid_trend_entry_signal(signal):
            return True
        metadata = worker._signal_metadata(signal)
        breakout_distance_pips = worker._safe_float(metadata.get("breakout_distance_pips"))
        breakout_distance_ratio = worker._safe_float(metadata.get("breakout_distance_ratio"))
        entry_quality_penalty = worker._safe_float(metadata.get("entry_quality_penalty"))

        if (
            worker.entry_index_trend_min_breakout_to_spread_ratio > 0.0
            and breakout_distance_pips is not None
            and current_spread_pips > 0.0
        ):
            required_breakout_pips = current_spread_pips * worker.entry_index_trend_min_breakout_to_spread_ratio
            if breakout_distance_pips + FLOAT_COMPARISON_TOLERANCE < required_breakout_pips:
                worker.store.record_event(
                    "INFO",
                    worker.symbol,
                    "Trade blocked by index trend anti-chop guard",
                    {
                        "reason": "breakout_inside_spread_noise",
                        "signal": signal.side.value,
                        "entry_strategy": worker._effective_entry_strategy_from_signal(signal),
                        "entry_component": worker._effective_entry_component_from_signal(signal),
                        "breakout_distance_pips": breakout_distance_pips,
                        "required_breakout_pips": required_breakout_pips,
                        "breakout_distance_ratio": breakout_distance_ratio,
                        "current_spread_pips": current_spread_pips,
                    },
                )
                return False

        if (
            worker.entry_index_trend_max_entry_quality_penalty > 0.0
            and entry_quality_penalty is not None
            and entry_quality_penalty - FLOAT_COMPARISON_TOLERANCE > worker.entry_index_trend_max_entry_quality_penalty
        ):
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Trade blocked by index trend anti-chop guard",
                {
                    "reason": "entry_quality_too_choppy",
                    "signal": signal.side.value,
                    "entry_strategy": worker._effective_entry_strategy_from_signal(signal),
                    "entry_component": worker._effective_entry_component_from_signal(signal),
                    "entry_quality_penalty": entry_quality_penalty,
                    "max_entry_quality_penalty": worker.entry_index_trend_max_entry_quality_penalty,
                    "entry_quality_status": metadata.get("entry_quality_status"),
                    "entry_quality_reasons": metadata.get("entry_quality_reasons"),
                    "breakout_distance_ratio": breakout_distance_ratio,
                },
            )
            return False

        return True

    def entry_spike_guard_block_payload(
        self,
        signal: Signal,
        *,
        current_spread_pips: float,
    ) -> dict[str, object] | None:
        worker = self._worker
        if not worker.entry_spike_guard_enabled:
            return None
        if signal.side not in {Side.BUY, Side.SELL}:
            return None
        signal_family = self.signal_management_family(signal)
        if worker.entry_spike_guard_trend_only and signal_family != "trend":
            return None
        lookback_samples = max(1, int(worker.entry_spike_guard_lookback_samples))
        required_samples = lookback_samples + 1
        history_prices, history_timestamps, _history_volumes = worker._history_runtime.history_snapshot()
        if len(history_prices) < required_samples or len(history_timestamps) < required_samples:
            return None
        pip_size = worker._execution_pip_size()
        if pip_size <= 0.0:
            return None

        current_price = worker._latest_price()
        if current_price is None:
            return None
        baseline_price = float(history_prices[-1 - lookback_samples])
        current_ts = float(history_timestamps[-1])
        baseline_ts = float(history_timestamps[-1 - lookback_samples])
        move_window_sec = max(0.0, current_ts - baseline_ts)
        if (
            worker.entry_spike_guard_max_window_sec > 0.0
            and move_window_sec - FLOAT_COMPARISON_TOLERANCE > worker.entry_spike_guard_max_window_sec
        ):
            return None

        if signal.side == Side.BUY:
            directional_move_price = current_price - baseline_price
        else:
            directional_move_price = baseline_price - current_price
        directional_move_pips = directional_move_price / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if not math.isfinite(directional_move_pips) or directional_move_pips <= 0.0:
            return None

        atr_pips = worker._signal_atr_pips_for_entry(signal)
        threshold_candidates: list[float] = []
        if atr_pips is not None and worker.entry_spike_guard_atr_multiplier > 0.0:
            threshold_candidates.append(float(atr_pips) * worker.entry_spike_guard_atr_multiplier)
        if current_spread_pips > 0.0 and worker.entry_spike_guard_spread_multiplier > 0.0:
            threshold_candidates.append(float(current_spread_pips) * worker.entry_spike_guard_spread_multiplier)
        if not threshold_candidates:
            return None

        threshold_pips = max(threshold_candidates)
        if directional_move_pips + FLOAT_COMPARISON_TOLERANCE < threshold_pips:
            return None

        return {
            "reason": "entry_spike_detected",
            "directional_move_pips": round(directional_move_pips, 6),
            "spike_guard_threshold_pips": round(threshold_pips, 6),
            "spike_guard_window_sec": round(move_window_sec, 3),
            "spike_guard_lookback_samples": lookback_samples,
            "spike_guard_signal_family": signal_family,
            "spike_guard_atr_pips": (round(float(atr_pips), 6) if atr_pips is not None else None),
            "spike_guard_current_spread_pips": round(float(current_spread_pips), 6),
        }

    def activate_reject_entry_cooldown(self, error_text: str) -> bool:
        worker = self._worker
        state = self._state
        if worker.open_reject_cooldown_sec <= 0:
            return False
        if not self.is_open_reject_error(error_text):
            return False

        now = worker._monotonic_now()
        previous_next = state.next_entry_allowed_ts
        next_allowed = max(previous_next, now + worker.open_reject_cooldown_sec)
        state.next_entry_allowed_ts = next_allowed
        state.active_entry_cooldown_sec = worker.open_reject_cooldown_sec
        state.active_entry_cooldown_outcome = "broker_reject"

        if next_allowed > previous_next + 1e-6:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Entry cooldown activated after broker reject",
                {
                    "cooldown_sec": worker.open_reject_cooldown_sec,
                    "next_entry_at": worker._wall_time_for_runtime_ts(
                        state.next_entry_allowed_ts,
                        now_monotonic=now,
                        now_wall=worker._wall_time_now(),
                    ),
                    "error": error_text,
                    "strategy": worker.strategy_name,
                },
            )
        return True

    def spread_pct_limit(self) -> tuple[float | None, str]:
        worker = self._worker
        if not worker.spread_pct_filter_enabled:
            return None, "disabled"
        if worker._is_crypto_symbol() and worker.spread_max_pct_crypto > 0:
            return worker.spread_max_pct_crypto, "crypto"
        if worker._is_non_fx_cfd_symbol() and worker.spread_max_pct_cfd > 0:
            return worker.spread_max_pct_cfd, "cfd"
        if worker.spread_max_pct > 0:
            return worker.spread_max_pct, "global"
        return None, "off"

    def spread_pct_allows_open(self, current_spread_pct: float, current_spread_pips: float) -> bool:
        worker = self._worker
        limit_pct, scope = self.spread_pct_limit()
        if limit_pct is None or current_spread_pct <= limit_pct:
            return True
        payload = {
            "current_spread_pct": round(current_spread_pct, 6),
            "limit_pct": round(limit_pct, 6),
            "scope": scope,
            "current_spread_pips": round(current_spread_pips, 6),
        }
        if worker.symbol_spec is not None and isinstance(worker.symbol_spec.metadata, dict):
            epic = str(worker.symbol_spec.metadata.get("epic") or "").strip()
            if epic:
                payload["epic"] = epic
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Trade blocked by spread pct filter",
            payload,
        )
        logger.warning(
            "Trade blocked by spread pct filter | symbol=%s spread_pct=%.4f limit_pct=%.4f scope=%s spread_pips=%.4f",
            worker.symbol,
            current_spread_pct,
            limit_pct,
            scope,
            current_spread_pips,
        )
        return False

    def spread_filter_metrics(self, current_spread_pips: float) -> tuple[bool, float, float]:
        worker = self._worker
        if not worker.spread_filter_enabled:
            return False, 0.0, 0.0
        spreads = self.spread_buffer_snapshot()
        if len(spreads) < worker.spread_min_samples:
            return False, 0.0, 0.0

        avg_spread = sum(spreads) / len(spreads)
        threshold = avg_spread * worker.spread_anomaly_multiplier
        return current_spread_pips > threshold, avg_spread, threshold

    def micro_chop_cooldown_payload(
        self,
        position: Position,
        trade_performance: dict[str, object] | None,
    ) -> dict[str, float] | None:
        worker = self._worker
        if worker.micro_chop_cooldown_sec <= 0.0:
            return None
        if not (worker._is_index_symbol() or worker._is_commodity_symbol()):
            return None
        if worker._position_management_family(position) != "trend":
            return None
        if float(position.pnl or 0.0) > FLOAT_COMPARISON_TOLERANCE:
            return None
        closed_at = float(position.closed_at or 0.0)
        opened_at = float(position.opened_at or 0.0)
        if closed_at <= 0.0 or opened_at <= 0.0:
            return None
        age_sec = max(0.0, closed_at - opened_at)
        if worker.micro_chop_trade_max_age_sec > 0.0 and age_sec - FLOAT_COMPARISON_TOLERANCE > worker.micro_chop_trade_max_age_sec:
            return None

        max_favorable_pips = float((trade_performance or {}).get("max_favorable_pips") or 0.0)
        tp_progress = 0.0
        pip_size = worker._execution_pip_size()
        if pip_size > 0.0:
            tp_distance_pips = abs(position.take_profit - position.open_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            if tp_distance_pips > FLOAT_COMPARISON_TOLERANCE:
                tp_progress = max_favorable_pips / max(tp_distance_pips, FLOAT_COMPARISON_TOLERANCE)
        if (
            max_favorable_pips - FLOAT_COMPARISON_TOLERANCE > worker.micro_chop_max_favorable_pips
            and tp_progress - FLOAT_COMPARISON_TOLERANCE > worker.micro_chop_max_tp_progress
        ):
            return None
        return {
            "age_sec": age_sec,
            "max_favorable_pips": max_favorable_pips,
            "max_tp_progress": tp_progress,
        }

    def entry_tick_age_allows_open(
        self,
        raw_tick_timestamp_sec: float,
        *,
        freshness_timestamp_sec: float | None = None,
        freshness_monotonic_sec: float | None = None,
    ) -> bool:
        worker = self._worker
        state = self._state
        if worker.entry_tick_max_age_sec <= 0:
            state.stale_entry_tick_streak = 0
            return True
        now = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        effective_timestamp_sec = (
            float(freshness_timestamp_sec)
            if freshness_timestamp_sec is not None and math.isfinite(float(freshness_timestamp_sec))
            else float(raw_tick_timestamp_sec)
        )
        if freshness_monotonic_sec is not None and math.isfinite(float(freshness_monotonic_sec)):
            tick_age_sec = max(0.0, now - float(freshness_monotonic_sec))
        else:
            tick_age_sec = max(0.0, now_wall - effective_timestamp_sec)
        quote_age_sec = max(0.0, now_wall - float(raw_tick_timestamp_sec))
        if tick_age_sec <= worker.entry_tick_max_age_sec:
            state.stale_entry_tick_streak = 0
            return True
        state.stale_entry_tick_streak += 1
        if (
            worker.operational_guard_enabled
            and state.stale_entry_tick_streak >= worker.operational_guard_stale_tick_streak_threshold
        ):
            self.activate_operational_guard(
                reason=f"stale_entry_tick_streak:{state.stale_entry_tick_streak}",
                now_ts=now,
                payload={
                    "tick_age_sec": round(tick_age_sec, 3),
                    "max_entry_tick_age_sec": round(worker.entry_tick_max_age_sec, 3),
                    "threshold": worker.operational_guard_stale_tick_streak_threshold,
                },
            )
        if (now - state.last_entry_tick_stale_log_ts) >= worker.hold_reason_log_interval_sec:
            payload = {
                "tick_age_sec": round(tick_age_sec, 3),
                "quote_age_sec": round(quote_age_sec, 3),
                "max_entry_tick_age_sec": round(worker.entry_tick_max_age_sec, 3),
                "db_first_reads_enabled": worker.db_first_reads_enabled,
                "db_first_tick_max_age_sec": worker.db_first_tick_max_age_sec,
            }
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by stale tick age",
                payload,
            )
            logger.warning(
                "Trade blocked by stale tick age | symbol=%s tick_age_sec=%.3f max_entry_tick_age_sec=%.3f",
                worker.symbol,
                tick_age_sec,
                worker.entry_tick_max_age_sec,
            )
            state.last_entry_tick_stale_log_ts = now
        return False

    def normalize_volume(self, raw_volume: float) -> float:
        worker = self._worker
        if raw_volume <= 0:
            return 0.0

        if worker.symbol_spec is None:
            return round(raw_volume, 2)

        step = worker.symbol_spec.lot_step if worker.symbol_spec.lot_step > 0 else 0.01
        step_dec = Decimal(str(step))
        raw_dec = Decimal(str(raw_volume)) + (step_dec * Decimal(str(FLOAT_COMPARISON_TOLERANCE)))
        steps = (raw_dec / step_dec).to_integral_value(rounding=ROUND_FLOOR)
        normalized = float(steps * step_dec)
        normalized = min(normalized, worker.symbol_spec.effective_lot_max())

        if normalized < worker.symbol_spec.lot_min:
            return 0.0

        return round(normalized, worker.symbol_spec.lot_precision)

    def partial_take_profit_close_volume(self, position: Position) -> float:
        worker = self._worker
        if position.volume <= 0:
            return 0.0
        requested = max(0.0, float(position.volume) * worker.partial_take_profit_fraction)
        close_volume = self.normalize_volume(requested)
        if close_volume <= 0:
            return 0.0
        min_lot = (
            float(worker.symbol_spec.lot_min)
            if worker.symbol_spec is not None and worker.symbol_spec.lot_min > 0
            else 0.01
        )
        min_remaining = max(min_lot, worker.partial_take_profit_min_remaining_lots)
        remaining = float(position.volume) - close_volume
        if remaining + FLOAT_COMPARISON_TOLERANCE < min_remaining:
            adjusted = self.normalize_volume(float(position.volume) - min_remaining)
            if adjusted <= 0:
                return 0.0
            close_volume = adjusted
            remaining = float(position.volume) - close_volume
        if remaining + FLOAT_COMPARISON_TOLERANCE < min_remaining:
            return 0.0
        return close_volume
