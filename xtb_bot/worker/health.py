from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from typing import Any

from xtb_bot.models import ConnectivityStatus, StreamHealthStatus


logger = logging.getLogger("xtb_bot.worker")

_STREAM_ENTRY_BLOCK_REASON_PREFIXES: tuple[str, ...] = (
    "stream_disconnected",
    "stream_tick_stale",
    "stream_subscription_failed",
)
_STREAM_ENTRY_BLOCK_MIN_REQUESTS = 20
_STREAM_ENTRY_BLOCK_MIN_REST_FALLBACK_HITS = 8
_STREAM_ENTRY_BLOCK_MIN_HIT_RATE_PCT = 70.0


@dataclass(slots=True)
class WorkerHealthState:
    last_stream_health: StreamHealthStatus | None = None
    last_stream_health_event_ts: float = 0.0
    last_stream_block_reason: str | None = None
    last_stream_block_event_ts: float = 0.0
    last_stream_metrics_event_ts: float = 0.0
    last_stream_metrics_key: tuple[int, int, int, int, int] | None = None
    cached_connectivity_status: ConnectivityStatus | None = None
    next_connectivity_probe_ts: float = 0.0
    last_connectivity_block_reason: str | None = None
    last_connectivity_block_event_ts: float = 0.0
    last_connectivity_degraded_reason: str | None = None
    last_connectivity_degraded_event_ts: float = 0.0


class WorkerHealthMonitor:
    def __init__(self, worker: Any) -> None:
        self._worker = worker
        self._state = WorkerHealthState()

    @property
    def state(self) -> WorkerHealthState:
        return self._state

    def connectivity_allows_open(self) -> bool:
        worker = self._worker
        state = self._state
        if not worker.connectivity_check_enabled:
            return True

        now = worker._monotonic_now()
        status = self.refresh_connectivity_status(now_ts=now)

        if status is None:
            return False
        if status.healthy:
            state.last_connectivity_block_reason = None
            state.last_connectivity_degraded_reason = None
            return True

        reason_text = str(status.reason or "").lower()
        is_allowance_related = worker._is_allowance_backoff_error(reason_text)
        if is_allowance_related:
            reason_changed = status.reason != state.last_connectivity_degraded_reason
            cooldown_passed = (
                now - state.last_connectivity_degraded_event_ts
            ) >= worker.stream_event_cooldown_sec
            if reason_changed or cooldown_passed:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Connectivity check deferred by allowance backoff (trade allowed)",
                    {
                        "reason": status.reason,
                        "latency_ms": status.latency_ms,
                        "pong_ok": status.pong_ok,
                        "latency_limit_ms": worker.connectivity_max_latency_ms,
                        "pong_timeout_sec": worker.connectivity_pong_timeout_sec,
                        "probe_interval_sec": worker.connectivity_check_interval_sec,
                    },
                )
                logger.warning(
                    "Connectivity check deferred by allowance backoff, trade allowed | symbol=%s reason=%s",
                    worker.symbol,
                    status.reason,
                )
                state.last_connectivity_degraded_reason = status.reason
                state.last_connectivity_degraded_event_ts = now
            state.last_connectivity_block_reason = None
            return True

        is_high_latency_only = self.is_connectivity_high_latency_non_blocking(status)
        if is_high_latency_only:
            reason_changed = status.reason != state.last_connectivity_degraded_reason
            cooldown_passed = (
                now - state.last_connectivity_degraded_event_ts
            ) >= worker.stream_event_cooldown_sec
            if reason_changed or cooldown_passed:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Connectivity degraded (high latency, trade allowed)",
                    {
                        "reason": status.reason,
                        "latency_ms": status.latency_ms,
                        "pong_ok": status.pong_ok,
                        "latency_limit_ms": worker.connectivity_max_latency_ms,
                        "pong_timeout_sec": worker.connectivity_pong_timeout_sec,
                        "probe_interval_sec": worker.connectivity_check_interval_sec,
                    },
                )
                logger.warning(
                    "Connectivity degraded but trade allowed | symbol=%s reason=%s latency_ms=%s pong_ok=%s",
                    worker.symbol,
                    status.reason,
                    status.latency_ms,
                    status.pong_ok,
                )
                state.last_connectivity_degraded_reason = status.reason
                state.last_connectivity_degraded_event_ts = now
            state.last_connectivity_block_reason = None
            return True

        is_dispatch_timeout_only = self.is_connectivity_dispatch_timeout_non_blocking(status)
        if is_dispatch_timeout_only:
            reason_changed = status.reason != state.last_connectivity_degraded_reason
            cooldown_passed = (
                now - state.last_connectivity_degraded_event_ts
            ) >= worker.stream_event_cooldown_sec
            if reason_changed or cooldown_passed:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Connectivity check deferred by request dispatch timeout (trade allowed)",
                    {
                        "reason": status.reason,
                        "latency_ms": status.latency_ms,
                        "pong_ok": status.pong_ok,
                        "latency_limit_ms": worker.connectivity_max_latency_ms,
                        "pong_timeout_sec": worker.connectivity_pong_timeout_sec,
                        "probe_interval_sec": worker.connectivity_check_interval_sec,
                    },
                )
                logger.warning(
                    "Connectivity check deferred by request dispatch timeout, trade allowed | symbol=%s reason=%s",
                    worker.symbol,
                    status.reason,
                )
                state.last_connectivity_degraded_reason = status.reason
                state.last_connectivity_degraded_event_ts = now
            state.last_connectivity_block_reason = None
            return True

        reason_changed = status.reason != state.last_connectivity_block_reason
        cooldown_passed = (now - state.last_connectivity_block_event_ts) >= worker.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by connectivity check",
                {
                    "reason": status.reason,
                    "latency_ms": status.latency_ms,
                    "pong_ok": status.pong_ok,
                    "latency_limit_ms": worker.connectivity_max_latency_ms,
                    "pong_timeout_sec": worker.connectivity_pong_timeout_sec,
                    "probe_interval_sec": worker.connectivity_check_interval_sec,
                },
            )
            logger.warning(
                "Trade blocked by connectivity check | symbol=%s reason=%s latency_ms=%s pong_ok=%s",
                worker.symbol,
                status.reason,
                status.latency_ms,
                status.pong_ok,
            )
            state.last_connectivity_block_reason = status.reason
            state.last_connectivity_block_event_ts = now
        return False

    @staticmethod
    def is_connectivity_high_latency_non_blocking(status: ConnectivityStatus) -> bool:
        reason_text = str(status.reason or "").lower()
        return reason_text.startswith("latency_too_high") and bool(status.pong_ok)

    @staticmethod
    def is_connectivity_dispatch_timeout_non_blocking(status: ConnectivityStatus) -> bool:
        reason_text = str(status.reason or "").lower()
        return "request dispatch timeout" in reason_text

    def is_connectivity_non_blocking(self, status: ConnectivityStatus) -> bool:
        worker = self._worker
        if worker._is_allowance_backoff_error(status.reason):
            return True
        if self.is_connectivity_high_latency_non_blocking(status):
            return True
        return self.is_connectivity_dispatch_timeout_non_blocking(status)

    def connectivity_status_allows_trading(self, status: ConnectivityStatus | None) -> bool:
        if status is None:
            return True
        if status.healthy:
            return True
        return self.is_connectivity_non_blocking(status)

    def refresh_connectivity_status(self, *, now_ts: float | None = None) -> ConnectivityStatus | None:
        worker = self._worker
        state = self._state
        if not worker.connectivity_check_enabled:
            return None
        now = worker._monotonic_now() if now_ts is None else float(now_ts)
        status = state.cached_connectivity_status
        should_probe = status is None or now >= state.next_connectivity_probe_ts
        if should_probe:
            status = worker.broker.get_connectivity_status(
                max_latency_ms=worker.connectivity_max_latency_ms,
                pong_timeout_sec=worker.connectivity_pong_timeout_sec,
            )
            state.cached_connectivity_status = status
            state.next_connectivity_probe_ts = now + worker.connectivity_check_interval_sec
            if not status.healthy and worker._is_allowance_backoff_error(status.reason):
                backoff_sec = worker._sync_allowance_backoff_from_connectivity_probe(
                    status.reason,
                    now_ts=now,
                )
                state.next_connectivity_probe_ts = max(state.next_connectivity_probe_ts, now + backoff_sec)
            elif not status.healthy and self.is_connectivity_dispatch_timeout_non_blocking(status):
                state.next_connectivity_probe_ts = max(state.next_connectivity_probe_ts, now + 15.0)
        return status

    def stream_health_payload(self, status: StreamHealthStatus) -> dict[str, object]:
        worker = self._worker
        payload = asdict(status)
        payload["max_tick_age_sec"] = worker.stream_max_tick_age_sec
        payload["event_cooldown_sec"] = worker.stream_event_cooldown_sec
        return payload

    @staticmethod
    def stream_reason_matches_entry_block(reason_text: str) -> bool:
        reason = str(reason_text or "").strip().lower()
        if not reason:
            return False
        return any(reason.startswith(prefix) for prefix in _STREAM_ENTRY_BLOCK_REASON_PREFIXES)

    def stream_health_entry_block_reason(self, status: StreamHealthStatus) -> str | None:
        reason = str(status.reason or "").strip().lower()
        if self.stream_reason_matches_entry_block(reason):
            return reason or "stream_degraded_rest_fallback"
        if reason in {"stream_subscription_pending_rest_fallback", "stream_warmup_rest_fallback"}:
            return None

        total_requests = int(getattr(status, "price_requests_total", 0) or 0)
        rest_hits = int(getattr(status, "rest_fallback_hits_total", 0) or 0)
        stream_hit_rate_pct_raw = getattr(status, "stream_hit_rate_pct", None)
        try:
            stream_hit_rate_pct = (
                float(stream_hit_rate_pct_raw)
                if stream_hit_rate_pct_raw is not None
                else None
            )
        except (TypeError, ValueError):
            stream_hit_rate_pct = None

        if (
            total_requests >= _STREAM_ENTRY_BLOCK_MIN_REQUESTS
            and rest_hits >= _STREAM_ENTRY_BLOCK_MIN_REST_FALLBACK_HITS
            and stream_hit_rate_pct is not None
            and stream_hit_rate_pct < _STREAM_ENTRY_BLOCK_MIN_HIT_RATE_PCT
        ):
            return (
                "stream_low_hit_rate_rest_fallback:"
                f"{stream_hit_rate_pct:.2f}%<{_STREAM_ENTRY_BLOCK_MIN_HIT_RATE_PCT:.2f}%"
            )
        return None

    def maybe_record_stream_usage_metrics(self, status: StreamHealthStatus) -> None:
        worker = self._worker
        state = self._state
        total = int(getattr(status, "price_requests_total", 0) or 0)
        stream_hits = int(getattr(status, "stream_hits_total", 0) or 0)
        rest_hits = int(getattr(status, "rest_fallback_hits_total", 0) or 0)
        desired_subscriptions = int(getattr(status, "desired_subscriptions", 0) or 0)
        active_subscriptions = int(getattr(status, "active_subscriptions", 0) or 0)
        if total <= 0:
            return

        metrics_key = (
            total,
            stream_hits,
            rest_hits,
            desired_subscriptions,
            active_subscriptions,
        )
        now = worker._monotonic_now()
        if metrics_key == state.last_stream_metrics_key:
            return
        if (now - state.last_stream_metrics_event_ts) < worker.stream_event_cooldown_sec:
            return

        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Stream usage metrics",
            {
                "price_requests_total": total,
                "stream_hits_total": stream_hits,
                "rest_fallback_hits_total": rest_hits,
                "stream_hit_rate_pct": status.stream_hit_rate_pct,
                "stream_connected": status.connected,
                "stream_reason": status.reason,
                "desired_subscriptions": desired_subscriptions,
                "active_subscriptions": active_subscriptions,
            },
        )
        state.last_stream_metrics_event_ts = now
        state.last_stream_metrics_key = metrics_key

    def refresh_stream_health(self) -> StreamHealthStatus | None:
        worker = self._worker
        state = self._state
        if not worker.stream_health_check_enabled:
            state.last_stream_health = None
            return None

        status = worker.broker.get_stream_health_status(
            symbol=worker.symbol,
            max_tick_age_sec=worker.stream_max_tick_age_sec,
        )
        self.maybe_record_stream_usage_metrics(status)
        previous = state.last_stream_health
        state.last_stream_health = status
        now = worker._monotonic_now()

        if previous is None:
            if not status.healthy:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Stream health degraded",
                    self.stream_health_payload(status),
                )
                state.last_stream_health_event_ts = now
            return status

        if status.healthy:
            if not previous.healthy:
                worker.store.record_event(
                    "INFO",
                    worker.symbol,
                    "Stream health recovered",
                    self.stream_health_payload(status),
                )
                state.last_stream_health_event_ts = now
            return status

        reason_changed = status.reason != previous.reason
        cooldown_passed = (now - state.last_stream_health_event_ts) >= worker.stream_event_cooldown_sec
        if previous.healthy or reason_changed or cooldown_passed:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Stream health degraded",
                self.stream_health_payload(status),
            )
            state.last_stream_health_event_ts = now
        return status

    def stream_health_allows_open(self, status: StreamHealthStatus | None) -> bool:
        worker = self._worker
        state = self._state
        if not worker.stream_health_check_enabled:
            return True

        if status is None:
            status = self.refresh_stream_health()
        if status is None:
            return True
        effective_reason: str | None = None
        if status.healthy:
            effective_reason = self.stream_health_entry_block_reason(status)
            if effective_reason is None:
                return True
        else:
            effective_reason = str(status.reason or "").strip().lower() or "stream_unhealthy"

        now = worker._monotonic_now()
        reason_changed = effective_reason != state.last_stream_block_reason
        cooldown_passed = (now - state.last_stream_block_event_ts) >= worker.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            payload = self.stream_health_payload(status)
            payload["effective_reason"] = effective_reason
            if status.healthy:
                payload["entry_block_override"] = True
                payload["entry_block_reason_source"] = "rest_fallback_degradation"
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by stream health check",
                payload,
            )
            state.last_stream_block_event_ts = now
            state.last_stream_block_reason = effective_reason

        logger.warning(
            "Trade blocked by stream health check | symbol=%s reason=%s stream_reason=%s connected=%s age_sec=%s",
            worker.symbol,
            effective_reason,
            status.reason,
            status.connected,
            status.last_tick_age_sec,
        )
        return False
