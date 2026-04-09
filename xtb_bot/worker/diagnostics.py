from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import logging
import threading
import time

from xtb_bot.models import Position, Side, Signal


logger = logging.getLogger(__name__)

_HOLD_REASON_METADATA_BASIC_KEYS: tuple[str, ...] = (
    "indicator",
    "reason",
    "hold_state",
    "entry_mode",
    "ma_type",
    "trend",
    "trend_signal",
    "regime",
    "regime_selection_mode",
    "trend_regime",
    "mean_reversion_regime",
    "zscore",
    "atr_pct",
    "atr_pips",
    "ema_gap_ratio",
    "channel_width_atr",
    "breakout_up",
    "breakout_down",
    "volume_data_available",
    "volume_ratio",
    "volume_spike",
    "directional_move_pips",
    "spike_guard_threshold_pips",
    "spike_guard_window_sec",
    "session_hour_utc",
    "session_key",
    "session_timezone",
    "session_start",
    "box_minutes",
    "trade_window_minutes",
    "session_trade_source",
    "session_trade_count",
    "pip_size",
    "pip_size_source",
    "exit_hint",
    "multi_net_side",
    "requested_net_qty_lots",
    "target_net_qty_lots",
    "buy_power",
    "sell_power",
    "conflict_detected",
    "intent_count",
    "multi_component_count",
    "multi_buy_component_count",
    "multi_sell_component_count",
    "multi_hold_component_count",
    "multi_neutral_component_count",
    "multi_blocked_component_count",
    "multi_unavailable_component_count",
    "multi_active_component_count",
    "multi_directional_component_count",
    "multi_dominant_power",
    "multi_opposing_power",
    "multi_power_ratio",
    "multi_balance_ratio",
    "multi_representative_strategy",
    "multi_dominant_component_strategy",
    "multi_confidence_net_qty",
    "multi_confidence_representative",
    "multi_confidence_support_ratio",
    "multi_confidence_dominant_power",
    "multi_hold_reason_count",
    "multi_top_neutral_reasons",
    "multi_top_blocked_reasons",
    "multi_top_unavailable_reasons",
    "multi_top_hold_reasons",
    "multi_top_soft_reasons",
    "multi_hold_reason_by_strategy",
    "multi_neutral_reason_by_strategy",
    "multi_blocked_reason_by_strategy",
    "multi_unavailable_reason_by_strategy",
    "multi_soft_reason_by_strategy",
    "soft_filter_penalty_total",
    "soft_filter_reasons",
    "soft_filter_count",
)


@dataclass(slots=True)
class WorkerDiagnosticsState:
    handled_news_events: set[str] = field(default_factory=set)
    handled_news_event_order: deque[str] = field(default_factory=deque)
    handled_news_events_max: int = 2048
    last_hold_reason: str | None = None
    last_hold_reason_log_ts: float = 0.0
    last_news_entry_block_log_ts: float = 0.0
    last_news_entry_block_signature: str | None = None
    trend_hold_reasons: deque[str] = field(default_factory=lambda: deque(maxlen=20))
    last_trend_hold_summary_ts: float = 0.0
    last_indicator_debug_ts: float = 0.0


class WorkerDiagnosticsRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker
        self._state_lock = threading.RLock()
        trend_hold_window = max(20, int(getattr(worker, "_trend_hold_summary_window", 240) or 240))
        self.state = WorkerDiagnosticsState(
            handled_news_events_max=2048,
            trend_hold_reasons=deque(maxlen=trend_hold_window),
        )

    @staticmethod
    def is_json_scalar(value: object) -> bool:
        return value is None or isinstance(value, (str, int, float, bool))

    @classmethod
    def is_compact_json_value(cls, value: object) -> bool:
        if cls.is_json_scalar(value):
            return True
        if isinstance(value, (list, tuple)) and len(value) <= 8:
            return all(cls.is_json_scalar(item) for item in value)
        return False

    def hold_reason_metadata_payload(self, metadata: dict[str, object]) -> dict[str, object]:
        worker = self.worker
        mode = str(worker.hold_reason_metadata_verbosity or "basic").strip().lower()
        if mode == "full":
            return dict(metadata)
        if mode == "minimal":
            compact: dict[str, object] = {}
            for key in ("reason", "indicator"):
                if key in metadata and self.is_json_scalar(metadata.get(key)):
                    compact[key] = metadata.get(key)
            return compact

        compact: dict[str, object] = {}
        for key in _HOLD_REASON_METADATA_BASIC_KEYS:
            if key not in metadata:
                continue
            value = metadata.get(key)
            if self.is_compact_json_value(value):
                compact[key] = value
        return compact

    def effective_signal_min_history(self) -> int:
        worker = self.worker
        current = int(getattr(worker.strategy, "min_history", 1) or 1)
        if worker._multi_strategy_enabled:
            return max(1, max(current, int(worker._signal_min_history)))
        return max(1, current)

    def maybe_record_hold_reason(
        self,
        signal: Signal,
        current_spread_pips: float,
        current_spread_pct: float | None = None,
    ) -> None:
        worker = self.worker
        signal_metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        reason = str(signal_metadata.get("reason") or "unknown")
        now = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        self.maybe_record_trend_hold_summary(reason, now)
        with self._state_lock:
            last_hold_reason = self.state.last_hold_reason
            last_hold_reason_log_ts = self.state.last_hold_reason_log_ts
        reason_changed = reason != last_hold_reason
        interval_passed = (now - last_hold_reason_log_ts) >= worker.hold_reason_log_interval_sec
        if not reason_changed and not interval_passed:
            return

        spread_avg_pips = 0.0
        spread_samples = worker._spread_buffer_snapshot()
        if spread_samples:
            spread_avg_pips = sum(spread_samples) / len(spread_samples)
        payload = {
            "reason": reason,
            "strategy": worker.strategy_name,
            "symbol": worker.symbol,
            "confidence": signal.confidence,
            "current_spread_pips": current_spread_pips,
            "current_spread_pct": current_spread_pct,
            "average_spread_pips": spread_avg_pips,
            "spread_samples": len(spread_samples),
            "utc_hour": time.gmtime(now_wall).tm_hour,
            "allowance_backoff_remaining_sec": round(worker._local_allowance_backoff_remaining_sec(), 3),
            "min_history": self.effective_signal_min_history(),
            "metadata": self.hold_reason_metadata_payload(signal_metadata),
            "metadata_verbosity": worker.hold_reason_metadata_verbosity,
        }
        payload.update(self.price_buffer_debug_fields(now))
        worker.store.record_event("INFO", worker.symbol, "Signal hold reason", payload)
        with self._state_lock:
            self.state.last_hold_reason = reason
            self.state.last_hold_reason_log_ts = now

    def maybe_record_trend_hold_summary(self, reason: str, now: float) -> None:
        worker = self.worker
        if not worker._trend_hold_summary_enabled:
            return
        normalized_reason = str(reason or "unknown").strip() or "unknown"
        with self._state_lock:
            self.state.trend_hold_reasons.append(normalized_reason)
            sample_count = len(self.state.trend_hold_reasons)
            if sample_count < worker._trend_hold_summary_min_samples:
                return
            if (now - self.state.last_trend_hold_summary_ts) < worker._trend_hold_summary_interval_sec:
                return

            reason_counts: dict[str, int] = {}
            for item in list(self.state.trend_hold_reasons):
                reason_counts[item] = reason_counts.get(item, 0) + 1
            self.state.last_trend_hold_summary_ts = now
        ranked = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
        top_reasons: list[dict[str, object]] = []
        top_text_parts: list[str] = []
        for item_reason, count in ranked:
            share = count / sample_count if sample_count > 0 else 0.0
            top_reasons.append({"reason": item_reason, "count": count, "share": round(share, 4)})
            top_text_parts.append(f"{item_reason}:{count}")

        payload = {
            "strategy": worker.strategy_name,
            "symbol": worker.symbol,
            "window_size": sample_count,
            "configured_window_size": worker._trend_hold_summary_window,
            "min_samples": worker._trend_hold_summary_min_samples,
            "interval_sec": worker._trend_hold_summary_interval_sec,
            "top_reasons": top_reasons,
            "allowance_backoff_remaining_sec": round(worker._local_allowance_backoff_remaining_sec(), 3),
        }
        worker.store.record_event("INFO", worker.symbol, "Trend hold summary", payload)
        logger.info(
            "Trend hold summary | symbol=%s window=%s top=%s",
            worker.symbol,
            sample_count,
            ", ".join(top_text_parts),
        )

    def maybe_record_indicator_debug(
        self,
        signal: Signal,
        current_spread_pips: float,
    ) -> None:
        worker = self.worker
        if not worker.debug_indicators_enabled:
            return

        now = worker._monotonic_now()
        if (
            worker.debug_indicators_interval_sec > 0
            and (now - self.state.last_indicator_debug_ts) < worker.debug_indicators_interval_sec
        ):
            return

        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        payload = {
            "strategy": worker.strategy_name,
            "symbol": worker.symbol,
            "signal_side": signal.side.value,
            "confidence": signal.confidence,
            "reason": metadata.get("reason"),
            "current_spread_pips": current_spread_pips,
            "min_history": self.effective_signal_min_history(),
            "fast_ema": metadata.get("fast_ema"),
            "slow_ema": metadata.get("slow_ema"),
            "adx": metadata.get("adx"),
            "atr": metadata.get("atr"),
            "trend_signal": metadata.get("trend_signal"),
            "indicator": metadata.get("indicator"),
            "metadata": metadata,
        }
        payload.update(self.price_buffer_debug_fields(now))
        worker.store.record_event("INFO", worker.symbol, "Strategy indicator snapshot", payload)
        self.state.last_indicator_debug_ts = now

    def price_buffer_debug_fields(self, now_ts: float | None = None) -> dict[str, object]:
        worker = self.worker
        now_wall = (
            float(worker._wall_time_for_runtime_ts(now_ts) or worker._wall_time_now())
            if now_ts is not None
            else worker._wall_time_now()
        )
        prices, _timestamps, _volumes = worker._history_runtime.history_snapshot()
        latest_ts = worker._history_runtime.latest_history_timestamp()
        latest_age_sec: float | None = None
        if latest_ts is not None:
            latest_age_sec = max(0.0, now_wall - latest_ts)
        return {
            "prices_buffer_len": len(prices),
            "prices_buffer_capacity": int(worker.prices.maxlen or 0),
            "prices_seen_total": int(worker._price_samples_seen),
            "latest_price_ts": latest_ts,
            "latest_price_age_sec": (round(latest_age_sec, 3) if latest_age_sec is not None else None),
        }

    def mark_news_event_handled(self, event_id: str) -> bool:
        normalized_event_id = str(event_id or "").strip()
        if not normalized_event_id:
            return False
        with self._state_lock:
            if normalized_event_id in self.state.handled_news_events:
                return False
            self.state.handled_news_events.add(normalized_event_id)
            self.state.handled_news_event_order.append(normalized_event_id)
            while len(self.state.handled_news_event_order) > self.state.handled_news_events_max:
                stale_event_id = self.state.handled_news_event_order.popleft()
                self.state.handled_news_events.discard(stale_event_id)
            return True

    def apply_news_filter(self, position: Position, now_ts: float, bid: float, ask: float) -> str | None:
        worker = self.worker
        if not worker.news_filter_enabled or worker.news_event_buffer_sec <= 0:
            return None

        events = worker.broker.get_upcoming_high_impact_events(now_ts, worker.news_event_buffer_sec)
        if not events:
            return None

        close_reason: str | None = None
        for event in events:
            if not self.mark_news_event_handled(event.event_id):
                continue

            if worker.news_event_action == "close":
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "News filter close trigger",
                    {
                        "position_id": position.position_id,
                        "event_id": event.event_id,
                        "event_name": event.name,
                        "event_time": event.timestamp,
                    },
                )
                close_reason = f"news_event_close:{event.name}"
                break

            worker._apply_breakeven_protection(
                position,
                trigger=f"news_event:{event.name}",
                event_id=event.event_id,
                bid=bid,
                ask=ask,
            )
        return close_reason

    def entry_news_filter_allows_open(self, now_ts: float, signal_side: Side) -> bool:
        worker = self.worker
        if not worker.news_filter_enabled or worker.news_event_buffer_sec <= 0:
            return True
        events = worker.broker.get_upcoming_high_impact_events(now_ts, worker.news_event_buffer_sec)
        if not events:
            return True

        def _event_timestamp_or_now(event: object) -> float:
            raw_timestamp = getattr(event, "timestamp", now_ts)
            try:
                return float(raw_timestamp)
            except (TypeError, ValueError):
                return now_ts

        next_event = min(events, key=_event_timestamp_or_now)
        event_ts = _event_timestamp_or_now(next_event)
        time_to_event_sec = max(0.0, event_ts - now_ts)
        signature = f"{next_event.event_id}:{int(event_ts)}"
        should_log = (
            signature != self.state.last_news_entry_block_signature
            or (now_ts - self.state.last_news_entry_block_log_ts) >= worker.hold_reason_log_interval_sec
        )
        if should_log:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by news filter",
                {
                    "signal": signal_side.value,
                    "event_id": next_event.event_id,
                    "event_name": next_event.name,
                    "event_time": event_ts,
                    "time_to_event_sec": round(time_to_event_sec, 3),
                    "news_event_buffer_sec": worker.news_event_buffer_sec,
                },
            )
            logger.warning(
                "Trade blocked by news filter | symbol=%s signal=%s event=%s time_to_event_sec=%.1f",
                worker.symbol,
                signal_side.value,
                next_event.name,
                time_to_event_sec,
            )
            self.state.last_news_entry_block_signature = signature
            self.state.last_news_entry_block_log_ts = now_ts
        return False
