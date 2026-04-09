from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from urllib import parse

if TYPE_CHECKING:
    from xtb_bot.ig_client import IgApiClient

from xtb_bot.client import BrokerError
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE

logger = logging.getLogger(__name__)


class IgRateLimitMixin:
    """Rate limiting, allowance cooldown, and critical-trade deferral methods for IgApiClient."""

    __slots__ = ()

    # ------------------------------------------------------------------
    # Static request-classification helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_trade_critical_request(method: str, path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if normalized_path.startswith("/confirms/") and upper_method == "GET":
            return True
        if normalized_path == "/positions/otc" and upper_method in {"POST", "DELETE"}:
            return True
        if normalized_path.startswith("/positions/otc/") and upper_method == "PUT":
            return True
        return False

    @staticmethod
    def _is_trading_allowance_request(method: str, path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if normalized_path == "/positions/otc" and upper_method in {"POST", "DELETE", "PUT"}:
            return True
        if normalized_path.startswith("/positions/otc/") and upper_method in {"PUT", "DELETE"}:
            return True
        if normalized_path == "/workingorders/otc" and upper_method in {"POST", "DELETE", "PUT"}:
            return True
        if normalized_path.startswith("/workingorders/otc/") and upper_method in {"PUT", "DELETE"}:
            return True
        return False

    @staticmethod
    def _is_historical_price_request(path: str, method: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        return upper_method == "GET" and normalized_path.startswith("/prices/")

    @staticmethod
    def _parse_positive_int(value: Any) -> int:
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)

    # ------------------------------------------------------------------
    # Requested-history-points extraction
    # ------------------------------------------------------------------

    def _extract_requested_history_points(self, path: str, query: dict[str, Any] | None) -> int:
        if not self._is_historical_price_request(path, "GET"):
            return 0
        candidates: list[Any] = []
        if isinstance(query, dict):
            for key in ("max", "numPoints", "pageSize", "count"):
                if key in query and query.get(key) is not None:
                    candidates.append(query.get(key))
        parsed_url = parse.urlsplit(str(path))
        if parsed_url.query:
            query_params = parse.parse_qs(parsed_url.query, keep_blank_values=False)
            for key in ("max", "numPoints", "pageSize", "count"):
                values = query_params.get(key)
                if values:
                    candidates.append(values[-1])
        for candidate in candidates:
            points = self._parse_positive_int(candidate)
            if points > 0:
                return points
        return 1

    # ------------------------------------------------------------------
    # Rate-limit window helpers (called under self._lock)
    # ------------------------------------------------------------------

    @staticmethod
    def _request_window_wait_locked(
        timestamps: deque[float],
        limit_per_window: int,
        now_ts: float,
        window_sec: float,
    ) -> float:
        if limit_per_window <= 0:
            return float("inf")
        while timestamps and (now_ts - timestamps[0]) >= window_sec:
            timestamps.popleft()
        if len(timestamps) < limit_per_window:
            return 0.0
        wait_sec = window_sec - (now_ts - timestamps[0])
        if not math.isfinite(wait_sec):
            return 0.0
        return max(0.0, wait_sec)

    def _prune_historical_points_window_locked(self, now_ts: float) -> None:
        window_sec = float(self._rate_limit_week_window_sec)
        while self._rate_limit_historical_points_events:
            event_ts, points = self._rate_limit_historical_points_events[0]
            if (now_ts - event_ts) < window_sec:
                break
            self._rate_limit_historical_points_events.popleft()
            self._rate_limit_historical_points_week_total = max(
                0,
                int(self._rate_limit_historical_points_week_total) - int(points),
            )

    def _record_rate_limit_worker_block_locked(self, worker_key: str, now_ts: float) -> None:
        normalized = str(worker_key).strip().lower()
        if normalized not in self._rate_limit_worker_blocked_total:
            return
        self._rate_limit_worker_blocked_total[normalized] = (
            int(self._rate_limit_worker_blocked_total.get(normalized, 0)) + 1
        )
        self._rate_limit_worker_last_block_ts[normalized] = float(now_ts)

    def _record_rate_limit_worker_request_locked(self, worker_key: str, now_ts: float) -> None:
        normalized = str(worker_key).strip().lower()
        if normalized not in self._rate_limit_worker_last_request_ts:
            return
        self._rate_limit_worker_last_request_ts[normalized] = float(now_ts)

    # ------------------------------------------------------------------
    # Snapshot / public API
    # ------------------------------------------------------------------

    def _rate_limit_worker_usage_snapshot_locked(self, now_ts: float) -> list[dict[str, Any]]:
        self._request_window_wait_locked(
            self._rate_limit_app_non_trading_requests,
            int(self._rate_limit_app_non_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._request_window_wait_locked(
            self._rate_limit_account_non_trading_requests,
            int(self._rate_limit_account_non_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._request_window_wait_locked(
            self._rate_limit_account_trading_requests,
            int(self._rate_limit_account_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._prune_historical_points_window_locked(now_ts)

        app_used = int(len(self._rate_limit_app_non_trading_requests))
        app_limit = int(self._rate_limit_app_non_trading_per_min)
        account_non_trading_used = int(len(self._rate_limit_account_non_trading_requests))
        account_non_trading_limit = int(self._rate_limit_account_non_trading_per_min)
        trading_used = int(len(self._rate_limit_account_trading_requests))
        trading_limit = int(self._rate_limit_account_trading_per_min)
        historical_used = int(self._rate_limit_historical_points_week_total)
        historical_limit = int(self._rate_limit_historical_points_per_week)

        return [
            {
                "worker_key": "app_non_trading",
                "limit_scope": "per_app",
                "limit_unit": "requests_per_minute",
                "limit_value": app_limit,
                "used_value": app_used,
                "remaining_value": max(0, app_limit - app_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("app_non_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("app_non_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("app_non_trading", 0.0) or 0.0),
                "notes": "IG per-app non-trading requests/minute",
            },
            {
                "worker_key": "account_trading",
                "limit_scope": "per_account",
                "limit_unit": "requests_per_minute",
                "limit_value": trading_limit,
                "used_value": trading_used,
                "remaining_value": max(0, trading_limit - trading_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("account_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("account_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("account_trading", 0.0) or 0.0),
                "notes": "IG per-account trading requests/minute",
            },
            {
                "worker_key": "account_non_trading",
                "limit_scope": "per_account",
                "limit_unit": "requests_per_minute",
                "limit_value": account_non_trading_limit,
                "used_value": account_non_trading_used,
                "remaining_value": max(0, account_non_trading_limit - account_non_trading_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("account_non_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("account_non_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("account_non_trading", 0.0) or 0.0),
                "notes": "IG per-account non-trading requests/minute",
            },
            {
                "worker_key": "historical_points",
                "limit_scope": "per_app",
                "limit_unit": "points_per_week",
                "limit_value": historical_limit,
                "used_value": historical_used,
                "remaining_value": max(0, historical_limit - historical_used),
                "window_sec": float(self._rate_limit_week_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("historical_points", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("historical_points", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("historical_points", 0.0) or 0.0),
                "notes": "IG historical price data points/week",
            },
        ]

    def get_rate_limit_workers_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            now_ts = time.time()
            return self._rate_limit_worker_usage_snapshot_locked(now_ts)

    # ------------------------------------------------------------------
    # Slot reservation (blocking)
    # ------------------------------------------------------------------

    def _reserve_request_rate_limit_slot(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None,
    ) -> None:
        if not self._rate_limit_enabled:
            return
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        is_trading = self._is_trading_allowance_request(upper_method, normalized_path)
        requested_history_points = (
            self._extract_requested_history_points(path, query)
            if self._is_historical_price_request(normalized_path, upper_method)
            else 0
        )
        is_historical = requested_history_points > 0
        sleep_cap_sec = 0.25
        while True:
            sleep_sec = 0.0
            with self._lock:
                now_ts = time.time()
                if is_trading:
                    trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_account_trading_requests,
                        int(self._rate_limit_account_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    sleep_sec = trading_wait_sec
                    if trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("account_trading", now_ts)
                else:
                    account_non_trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_account_non_trading_requests,
                        int(self._rate_limit_account_non_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    app_non_trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_app_non_trading_requests,
                        int(self._rate_limit_app_non_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    sleep_sec = max(account_non_trading_wait_sec, app_non_trading_wait_sec)
                    if account_non_trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("account_non_trading", now_ts)
                    if app_non_trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("app_non_trading", now_ts)

                if not math.isfinite(sleep_sec):
                    worker_key = "account_trading" if is_trading else "account_non_trading"
                    raise BrokerError(
                        "IG local rate limiter blocks all requests for this allowance window "
                        f"(worker={worker_key}, limit=0)"
                    )
                if sleep_sec <= 0:
                    if requested_history_points > 0 and self._rate_limit_historical_points_per_week > 0:
                        requested = int(requested_history_points)
                        weekly_limit = int(self._rate_limit_historical_points_per_week)
                        if requested > weekly_limit:
                            self._record_rate_limit_worker_block_locked("historical_points", now_ts)
                            raise BrokerError(
                                "IG historical price request exceeds weekly allowance "
                                f"(requested={requested}, weekly_limit={weekly_limit})"
                            )
                        self._prune_historical_points_window_locked(now_ts)
                        weekly_used = int(self._rate_limit_historical_points_week_total)
                        if (weekly_used + requested) > weekly_limit:
                            self._record_rate_limit_worker_block_locked("historical_points", now_ts)
                            raise BrokerError(
                                "IG historical price weekly allowance exceeded by local limiter "
                                f"(used={weekly_used}, requested={requested}, weekly_limit={weekly_limit})"
                            )
                    if is_trading:
                        self._rate_limit_account_trading_requests.append(now_ts)
                        self._record_rate_limit_worker_request_locked("account_trading", now_ts)
                    else:
                        self._rate_limit_account_non_trading_requests.append(now_ts)
                        self._rate_limit_app_non_trading_requests.append(now_ts)
                        self._record_rate_limit_worker_request_locked("account_non_trading", now_ts)
                        self._record_rate_limit_worker_request_locked("app_non_trading", now_ts)
                    if requested_history_points > 0 and self._rate_limit_historical_points_per_week > 0:
                        points = int(requested_history_points)
                        self._rate_limit_historical_points_events.append((now_ts, points))
                        self._rate_limit_historical_points_week_total += points
                    if is_historical:
                        self._record_rate_limit_worker_request_locked("historical_points", now_ts)
                    return
            time.sleep(min(sleep_cap_sec, sleep_sec))

    # ------------------------------------------------------------------
    # Critical-trade deferral
    # ------------------------------------------------------------------

    @staticmethod
    def _is_non_critical_rest_request(method: str, path: str) -> bool:
        return not IgRateLimitMixin._is_trade_critical_request(method, path)

    def _critical_trade_active_for_other_threads(self) -> bool:
        thread_id = threading.get_ident()
        with self._lock:
            if self._critical_trade_active_total <= 0:
                return False
            return self._critical_trade_owner_threads.get(thread_id, 0) <= 0

    def _current_thread_has_critical_trade_context(self) -> bool:
        thread_id = threading.get_ident()
        with self._lock:
            return self._critical_trade_owner_threads.get(thread_id, 0) > 0

    def _current_thread_request_critical_bypass(self) -> bool:
        return bool(getattr(self._request_context, "critical_bypass", False))

    def _should_defer_non_critical_request(self, method: str, path: str) -> bool:
        if not self._critical_trade_active_for_other_threads():
            return False
        return self._is_non_critical_rest_request(method, path)

    def _critical_trade_defer_remaining_sec(self) -> float:
        now = time.time()
        with self._lock:
            remaining = max(0.0, self._trade_submit_next_allowed_ts - now)
            if self._critical_trade_active_total > 0:
                remaining = max(remaining, float(self._trade_submit_min_interval_sec))
        return max(0.1, remaining)

    @contextmanager
    def _critical_trade_operation(self, operation: str):
        _ = operation
        thread_id = threading.get_ident()
        # LOCK ORDER: _trade_submit_lock → _lock
        with self._trade_submit_lock:
            while True:
                with self._lock:
                    wait_sec = self._trade_submit_next_allowed_ts - time.time()
                if wait_sec <= 0:
                    break
                time.sleep(min(wait_sec, 0.25))
            with self._lock:
                self._critical_trade_active_total += 1
                self._critical_trade_owner_threads[thread_id] = self._critical_trade_owner_threads.get(thread_id, 0) + 1
        try:
            yield
        finally:
            with self._lock:
                current = self._critical_trade_owner_threads.get(thread_id, 0)
                if current <= 1:
                    self._critical_trade_owner_threads.pop(thread_id, None)
                else:
                    self._critical_trade_owner_threads[thread_id] = current - 1
                self._critical_trade_active_total = max(0, self._critical_trade_active_total - 1)
                min_interval = max(0.0, float(self._trade_submit_min_interval_sec))
                if min_interval > 0:
                    self._trade_submit_next_allowed_ts = max(
                        self._trade_submit_next_allowed_ts,
                        time.time() + min_interval,
                    )

    # ------------------------------------------------------------------
    # Request worker key
    # ------------------------------------------------------------------

    def _request_worker_key(self, method: str, path: str) -> str:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if self._is_historical_price_request(normalized_path, upper_method):
            return "historical_points"
        if self._is_trading_allowance_request(upper_method, normalized_path):
            return "account_trading"
        if normalized_path.startswith("/accounts"):
            return "account_non_trading"
        if normalized_path.startswith("/positions"):
            return "account_non_trading"
        if normalized_path.startswith("/workingorders"):
            return "account_non_trading"
        if normalized_path.startswith("/confirms/"):
            return "account_non_trading"
        if normalized_path.startswith("/history/"):
            return "account_non_trading"
        return "app_non_trading"

    # ------------------------------------------------------------------
    # Allowance cooldown
    # ------------------------------------------------------------------

    @staticmethod
    def _is_allowance_error_text(text: str) -> bool:
        lowered = text.lower()
        return (
            "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )

    @staticmethod
    def _is_trade_allowance_error_text(text: str) -> bool:
        lowered = str(text).lower()
        return "exceeded-account-trading-allowance" in lowered

    def _maybe_start_allowance_cooldown(self, error_text: str, scope: str) -> float | None:
        if not self._is_allowance_error_text(error_text):
            return None
        minimum_cooldown_sec = 30.0 if self._is_trade_allowance_error_text(error_text) else 0.0
        cooldown_sec = self._start_allowance_cooldown(error_text, minimum_sec=minimum_cooldown_sec)
        with self._lock:
            self._allowance_last_scope = str(scope)
        self._adapt_market_rest_interval_after_allowance(scope)
        logger.warning(
            "IG allowance exceeded for %s, pausing REST calls for %.1fs",
            scope,
            cooldown_sec,
        )
        return cooldown_sec

    def _allowance_cooldown_remaining(self) -> float:
        with self._lock:
            return max(0.0, self._allowance_cooldown_until_ts - time.time())

    def _allowance_cooldown_remaining_for_market_data(self) -> float:
        remaining = self._allowance_cooldown_remaining()
        if remaining <= 0:
            return 0.0
        with self._lock:
            last_error = str(self._allowance_last_error or "")
            last_scope = str(self._allowance_last_scope or "").strip().lower()
        if (
            self._is_trade_allowance_error_text(last_error)
            and last_scope in {"trade open", "trade open confirm", "managed positions"}
        ):
            # Trading allowance should not block quote acquisition if quotes are still available.
            return 0.0
        return remaining

    def _adapt_market_rest_interval_after_allowance(self, scope: str) -> None:
        normalized_scope = str(scope or "").strip().lower()
        if normalized_scope not in {"market data", "symbol spec", "managed positions", "accounts"}:
            return
        with self._lock:
            previous = float(self.rest_market_min_interval_sec)
            target = min(
                float(self._rest_market_min_interval_max_sec),
                max(previous + 0.2, previous * 1.4),
            )
            if target <= (previous + FLOAT_COMPARISON_TOLERANCE):
                return
            self.rest_market_min_interval_sec = target
        logger.warning(
            "IG REST market interval auto-raised after allowance exceed: %.2fs -> %.2fs",
            previous,
            target,
        )

    def _start_allowance_cooldown(self, error_text: str, *, minimum_sec: float = 0.0) -> float:
        now = time.time()
        with self._lock:
            remaining = self._allowance_cooldown_until_ts - now
            if remaining > 0:
                self._allowance_last_error = error_text
                return remaining
            previous = self._allowance_cooldown_sec
            next_delay = min(
                self._allowance_cooldown_max_sec,
                max(2.0, float(minimum_sec), previous * 2.0),
            )
            self._allowance_cooldown_sec = next_delay
            self._allowance_cooldown_until_ts = now + next_delay
            self._allowance_last_error = error_text
            return next_delay

    def _reset_allowance_cooldown(self) -> None:
        with self._lock:
            self._allowance_cooldown_until_ts = 0.0
            self._allowance_cooldown_sec = 1.0
            self._allowance_last_error = None
            self._allowance_last_scope = None
