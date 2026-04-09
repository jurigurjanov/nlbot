from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from xtb_bot.client import BaseBrokerClient, BrokerError
from xtb_bot.models import (
    AccountSnapshot,
    ConnectivityStatus,
    NewsEvent,
    PendingOpen,
    Position,
    PriceTick,
    Side,
    StreamHealthStatus,
    SymbolSpec,
)


logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float, *, minimum: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return max(minimum, float(default))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return max(minimum, float(default))
    return max(minimum, value)


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return max(minimum, int(default))
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return max(minimum, int(default))
    return max(minimum, value)


class TokenBucketRateLimiter:
    """Thread-safe token-bucket limiter."""

    def __init__(self, capacity: int, refill_per_sec: float) -> None:
        self._capacity = max(1, int(capacity))
        self._refill_per_sec = max(FLOAT_COMPARISON_TOLERANCE, float(refill_per_sec))
        self._tokens = float(self._capacity)
        self._last_refill_monotonic = time.monotonic()
        self._cond = threading.Condition(threading.Lock())

    def _refill_locked(self) -> None:
        now_mono = time.monotonic()
        elapsed = now_mono - self._last_refill_monotonic
        if elapsed <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + (elapsed * self._refill_per_sec))
        self._last_refill_monotonic = now_mono

    def acquire(self, timeout_sec: float = 30.0) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        with self._cond:
            while True:
                self._refill_locked()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BrokerError("IG rate limit exceeded (local token bucket timeout)")
                wait_for_token = (1.0 - self._tokens) / self._refill_per_sec
                self._cond.wait(timeout=max(0.001, min(remaining, wait_for_token + 0.01)))

    def try_acquire(self) -> bool:
        with self._cond:
            self._refill_locked()
            if self._tokens < 1.0:
                return False
            self._tokens -= 1.0
            return True

    @property
    def tokens(self) -> float:
        with self._cond:
            self._refill_locked()
            return self._tokens


@dataclass(slots=True)
class CacheEntry:
    value: Any = None
    fetched_at: float = 0.0
    ttl_sec: float = 10.0

    def is_fresh(self, now_ts: float | None = None) -> bool:
        if self.value is None:
            return False
        now = time.time() if now_ts is None else float(now_ts)
        return (now - self.fetched_at) < self.ttl_sec

    def is_usable(self, max_stale_sec: float, now_ts: float | None = None) -> bool:
        if self.value is None:
            return False
        now = time.time() if now_ts is None else float(now_ts)
        return (now - self.fetched_at) < (self.ttl_sec + max(0.0, float(max_stale_sec)))


class RateLimitedBrokerProxy(BaseBrokerClient):
    """Rate-limited proxy in front of an IG broker client."""

    def __init__(
        self,
        real_broker: BaseBrokerClient,
        symbols: list[str] | None = None,
        stop_event: threading.Event | None = None,
        *,
        pollers_enabled: bool | None = None,
    ) -> None:
        self._broker = real_broker
        self._symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
        self._external_stop_event = stop_event
        self._internal_stop_event = threading.Event()

        account_non_trading_per_min = _env_float("XTB_IG_PROXY_ACCOUNT_NON_TRADING_PER_MIN", 25.0, minimum=1.0)
        account_non_trading_burst = _env_int("XTB_IG_PROXY_ACCOUNT_NON_TRADING_BURST", 5, minimum=1)
        account_trading_per_min = _env_float("XTB_IG_PROXY_ACCOUNT_TRADING_PER_MIN", 100.0, minimum=1.0)
        account_trading_burst = _env_int("XTB_IG_PROXY_ACCOUNT_TRADING_BURST", 10, minimum=1)
        historical_per_week = _env_float("XTB_IG_PROXY_HISTORICAL_POINTS_PER_WEEK", 10_000.0, minimum=1.0)

        self._account_non_trading = TokenBucketRateLimiter(
            capacity=account_non_trading_burst,
            refill_per_sec=account_non_trading_per_min / 60.0,
        )
        self._account_trading = TokenBucketRateLimiter(
            capacity=account_trading_burst,
            refill_per_sec=account_trading_per_min / 60.0,
        )
        self._historical = TokenBucketRateLimiter(
            capacity=max(1, min(100, int(historical_per_week))),
            refill_per_sec=historical_per_week / (7.0 * 24.0 * 60.0 * 60.0),
        )

        self._cache_max_stale_sec = _env_float("XTB_IG_PROXY_CACHE_MAX_STALE_SEC", 120.0, minimum=0.0)
        self._account_snapshot_ttl_sec = _env_float("XTB_IG_PROXY_ACCOUNT_SNAPSHOT_TTL_SEC", 10.0, minimum=0.5)
        self._symbol_spec_ttl_sec = _env_float("XTB_IG_PROXY_SYMBOL_SPEC_TTL_SEC", 600.0, minimum=1.0)
        self._session_close_ttl_sec = _env_float("XTB_IG_PROXY_SESSION_CLOSE_TTL_SEC", 3600.0, minimum=1.0)
        self._news_ttl_sec = _env_float("XTB_IG_PROXY_NEWS_TTL_SEC", 300.0, minimum=1.0)

        self._cache_lock = threading.Lock()
        self._account_snapshot_cache = CacheEntry(ttl_sec=self._account_snapshot_ttl_sec)
        self._symbol_spec_cache: dict[str, CacheEntry] = {}
        self._session_close_cache: dict[str, CacheEntry] = {}
        self._news_events_cache_by_window: dict[int, CacheEntry] = {}

        self._pollers_enabled = (
            _env_bool("XTB_IG_PROXY_POLLERS_ENABLED", False)
            if pollers_enabled is None
            else bool(pollers_enabled)
        )
        self._pollers: list[threading.Thread] = []
        self._pollers_lock = threading.Lock()
        self._account_poller_interval_sec = _env_float("XTB_IG_PROXY_ACCOUNT_POLL_INTERVAL_SEC", 10.0, minimum=1.0)
        self._historical_poller_interval_sec = _env_float(
            "XTB_IG_PROXY_HISTORICAL_POLL_INTERVAL_SEC",
            300.0,
            minimum=5.0,
        )
        self._historical_poller_news_within_sec = _env_int(
            "XTB_IG_PROXY_HISTORICAL_NEWS_WINDOW_SEC",
            3600,
            minimum=300,
        )

    @property
    def underlying(self) -> BaseBrokerClient:
        return self._broker

    def _is_stop_requested(self) -> bool:
        if self._internal_stop_event.is_set():
            return True
        return bool(self._external_stop_event is not None and self._external_stop_event.is_set())

    def _wait_for_stop(self, timeout_sec: float) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        while not self._is_stop_requested():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            self._internal_stop_event.wait(timeout=min(0.5, remaining))
        return True

    def connect(self) -> None:
        if self._internal_stop_event.is_set():
            self._internal_stop_event = threading.Event()
        self._broker.connect()
        if self._pollers_enabled:
            self._start_pollers()

    def close(self) -> None:
        self._internal_stop_event.set()
        self._stop_pollers()
        self._broker.close()

    def _start_pollers(self) -> None:
        with self._pollers_lock:
            alive = [thread for thread in self._pollers if thread.is_alive()]
            if alive:
                self._pollers = alive
                return
            self._pollers.clear()
            for name, target in (
                ("account", self._account_poller_loop),
                ("historical", self._historical_poller_loop),
            ):
                thread = threading.Thread(target=target, name=f"ig-proxy-{name}", daemon=True)
                thread.start()
                self._pollers.append(thread)
                logger.info("IG proxy poller started: %s", name)

    def _stop_pollers(self) -> None:
        with self._pollers_lock:
            threads = list(self._pollers)
            self._pollers.clear()
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=3.0)

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        return self._broker.get_connectivity_status(max_latency_ms, pong_timeout_sec)

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        return self._broker.get_stream_health_status(symbol, max_tick_age_sec)

    def get_account_snapshot(self) -> AccountSnapshot:
        now = time.time()
        with self._cache_lock:
            cache = self._account_snapshot_cache
            if cache.is_fresh(now):
                return cache.value
            stale_snapshot = cache.value if cache.is_usable(self._cache_max_stale_sec, now) else None
        try:
            self._account_non_trading.acquire(timeout_sec=15.0)
            snapshot = self._broker.get_account_snapshot()
        except Exception:
            if stale_snapshot is not None:
                return stale_snapshot
            raise
        with self._cache_lock:
            self._account_snapshot_cache = CacheEntry(
                value=snapshot,
                fetched_at=time.time(),
                ttl_sec=self._account_snapshot_ttl_sec,
            )
        return snapshot

    def get_account_currency_code(self) -> str | None:
        getter = getattr(self._broker, "get_account_currency_code", None)
        if not callable(getter):
            return None
        try:
            self._account_non_trading.acquire(timeout_sec=10.0)
            return getter()
        except Exception:
            return None

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        upper_symbol = str(symbol).strip().upper()
        now = time.time()
        with self._cache_lock:
            cache = self._symbol_spec_cache.get(upper_symbol)
            if cache is not None and cache.is_fresh(now):
                return cache.value
            stale_spec = (
                cache.value
                if cache is not None and cache.is_usable(self._cache_max_stale_sec, now)
                else None
            )
        try:
            self._account_non_trading.acquire(timeout_sec=15.0)
            spec = self._broker.get_symbol_spec(upper_symbol)
        except Exception:
            if stale_spec is not None:
                return stale_spec
            raise
        with self._cache_lock:
            self._symbol_spec_cache[upper_symbol] = CacheEntry(
                value=spec,
                fetched_at=time.time(),
                ttl_sec=self._symbol_spec_ttl_sec,
            )
        return spec

    def get_managed_open_positions(
        self,
        magic_prefix: str,
        magic_instance: str,
        preferred_symbols: list[str] | None = None,
        known_deal_references: list[str] | None = None,
        known_position_ids: list[str] | None = None,
        pending_opens: list[PendingOpen] | None = None,
        include_unmatched_preferred: bool = False,
    ) -> dict[str, Position]:
        self._account_non_trading.acquire(timeout_sec=20.0)
        return self._broker.get_managed_open_positions(
            magic_prefix,
            magic_instance,
            preferred_symbols=preferred_symbols,
            known_deal_references=known_deal_references,
            known_position_ids=known_position_ids,
            pending_opens=pending_opens,
            include_unmatched_preferred=include_unmatched_preferred,
        )

    def get_position_open_sync(self, position_id: str) -> dict[str, Any] | None:
        self._account_non_trading.acquire(timeout_sec=15.0)
        return self._broker.get_position_open_sync(position_id)

    def get_position_close_sync(
        self,
        position_id: str,
        *,
        deal_reference: str | None = None,
        opened_at: float | None = None,
        symbol: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        self._account_non_trading.acquire(timeout_sec=15.0)
        if hasattr(self._broker, "get_position_close_sync"):
            return self._broker.get_position_close_sync(  # type: ignore[attr-defined]
                position_id,
                deal_reference=deal_reference,
                opened_at=opened_at,
                symbol=symbol,
                **kwargs,
            )
        return None

    def get_public_api_backoff_remaining_sec(self) -> float:
        return self._broker.get_public_api_backoff_remaining_sec()

    def get_market_data_wait_remaining_sec(self) -> float:
        return self._broker.get_market_data_wait_remaining_sec()

    def open_position(
        self,
        symbol: str,
        side: Side,
        volume: float,
        stop_loss: float,
        take_profit: float,
        comment: str,
        entry_price: float | None = None,
    ) -> str:
        self._account_trading.acquire(timeout_sec=30.0)
        return self._broker.open_position(
            symbol=symbol,
            side=side,
            volume=volume,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=comment,
            entry_price=entry_price,
        )

    def close_position(self, position: Position, volume: float | None = None) -> None:
        self._account_trading.acquire(timeout_sec=30.0)
        self._broker.close_position(position, volume)

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        self._account_trading.acquire(timeout_sec=30.0)
        self._broker.modify_position(position, stop_loss, take_profit)

    def get_price(self, symbol: str) -> PriceTick:
        return self._broker.get_price(symbol)

    def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
        getter = getattr(self._broker, "get_price_stream_only", None)
        if not callable(getter):
            return None
        return getter(symbol, wait_timeout_sec=wait_timeout_sec)

    def set_stream_tick_handler(self, handler: Callable[[PriceTick], None] | None) -> None:
        setter = getattr(self._broker, "set_stream_tick_handler", None)
        if callable(setter):
            setter(handler)

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        upper_symbol = str(symbol).strip().upper()
        now = time.time()
        with self._cache_lock:
            cache = self._session_close_cache.get(upper_symbol)
            if cache is not None and cache.is_fresh(now):
                return cache.value
            stale_value = (
                cache.value
                if cache is not None and cache.is_usable(self._cache_max_stale_sec, now)
                else None
            )
        try:
            self._historical.acquire(timeout_sec=10.0)
            value = self._broker.get_session_close_utc(upper_symbol, now_ts)
        except Exception:
            if stale_value is not None:
                return stale_value
            raise
        with self._cache_lock:
            self._session_close_cache[upper_symbol] = CacheEntry(
                value=value,
                fetched_at=time.time(),
                ttl_sec=self._session_close_ttl_sec,
            )
        return value

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        window_sec = max(0, int(within_sec))
        now = time.time()
        with self._cache_lock:
            cache = self._news_events_cache_by_window.get(window_sec)
            if cache is not None and cache.is_fresh(now):
                return cache.value
            stale_value = (
                cache.value
                if cache is not None and cache.is_usable(self._cache_max_stale_sec, now)
                else None
            )
        try:
            self._historical.acquire(timeout_sec=10.0)
            events = self._broker.get_upcoming_high_impact_events(now_ts, window_sec)
        except Exception:
            if stale_value is not None:
                return stale_value
            raise
        with self._cache_lock:
            self._news_events_cache_by_window[window_sec] = CacheEntry(
                value=events,
                fetched_at=time.time(),
                ttl_sec=self._news_ttl_sec,
            )
        return events

    def _account_poller_loop(self) -> None:
        while not self._is_stop_requested():
            try:
                self._account_non_trading.acquire(timeout_sec=5.0)
                snapshot = self._broker.get_account_snapshot()
                with self._cache_lock:
                    self._account_snapshot_cache = CacheEntry(
                        value=snapshot,
                        fetched_at=time.time(),
                        ttl_sec=self._account_snapshot_ttl_sec,
                    )
            except Exception:
                logger.warning("IG proxy account poller failed", exc_info=True)
            if self._wait_for_stop(self._account_poller_interval_sec):
                return

    def _historical_poller_loop(self) -> None:
        while not self._is_stop_requested():
            try:
                self._historical.acquire(timeout_sec=5.0)
                now = time.time()
                window_sec = self._historical_poller_news_within_sec
                events = self._broker.get_upcoming_high_impact_events(now, window_sec)
                with self._cache_lock:
                    self._news_events_cache_by_window[window_sec] = CacheEntry(
                        value=events,
                        fetched_at=now,
                        ttl_sec=self._news_ttl_sec,
                    )
            except Exception:
                logger.warning("IG proxy historical poller failed", exc_info=True)
            if self._wait_for_stop(self._historical_poller_interval_sec):
                return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._broker, name)
