"""Rate-limited proxy for IG API broker client.

Sits between SymbolWorkers and IgApiClient, enforcing IG's rate limits:
- App non-trading: 60 requests/min
- Account non-trading: 30 requests/min
- Account trading: 100 requests/min
- Historical price data: 10,000 points/week

Cacheable reads are served from in-memory cache populated by background
poller threads.  Trading writes pass through synchronously with a
token-bucket rate limiter.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

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


# ---------------------------------------------------------------------------
# Token bucket rate limiter
# ---------------------------------------------------------------------------

class TokenBucketRateLimiter:
    """Thread-safe token-bucket rate limiter.

    ``capacity`` tokens, refilling at ``refill_per_sec`` tokens/second.
    ``acquire()`` blocks until a token is available or *timeout_sec* elapses.
    """

    def __init__(self, capacity: int, refill_per_sec: float) -> None:
        self._capacity = max(1, capacity)
        self._refill_per_sec = max(1e-9, refill_per_sec)
        self._tokens = float(self._capacity)
        self._last_refill = time.monotonic()
        self._cond = threading.Condition(threading.Lock())

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_sec)
            self._last_refill = now

    def acquire(self, timeout_sec: float = 30.0) -> None:
        """Block until a token is available.  Raises ``BrokerError`` on timeout."""
        deadline = time.monotonic() + timeout_sec
        with self._cond:
            while True:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait_for_token = (1.0 - self._tokens) / self._refill_per_sec
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BrokerError("IG rate limit exceeded (token bucket timeout)")
                self._cond.wait(timeout=min(wait_for_token + 0.01, remaining))

    @property
    def tokens(self) -> float:
        with self._cond:
            self._refill()
            return self._tokens


# ---------------------------------------------------------------------------
# Cache entry
# ---------------------------------------------------------------------------

@dataclass
class CacheEntry:
    """In-memory cache entry with TTL."""
    value: Any = None
    fetched_at: float = 0.0
    ttl_sec: float = 10.0
    error: Exception | None = None

    def is_fresh(self) -> bool:
        return self.value is not None and (time.time() - self.fetched_at) < self.ttl_sec

    def is_usable(self, max_stale_sec: float = 60.0) -> bool:
        """Return True if the value exists and is not too stale."""
        return self.value is not None and (time.time() - self.fetched_at) < (self.ttl_sec + max_stale_sec)


# ---------------------------------------------------------------------------
# Rate-limited broker proxy
# ---------------------------------------------------------------------------

class RateLimitedBrokerProxy(BaseBrokerClient):
    """Proxy that enforces IG rate limits and caches read-only data.

    Parameters
    ----------
    real_broker:
        The actual ``IgApiClient`` (or any ``BaseBrokerClient``).
    symbols:
        List of symbols the bot trades — used by pollers to know what to fetch.
    stop_event:
        Shared stop event; when set, pollers shut down.
    """

    def __init__(
        self,
        real_broker: BaseBrokerClient,
        symbols: list[str] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._broker = real_broker
        self._symbols = list(symbols or [])
        self._stop = stop_event or threading.Event()

        # ---- Rate limiters (one per IG category) ----
        self._app_non_trading = TokenBucketRateLimiter(capacity=60, refill_per_sec=60.0 / 60.0)
        self._account_non_trading = TokenBucketRateLimiter(capacity=30, refill_per_sec=30.0 / 60.0)
        self._account_trading = TokenBucketRateLimiter(capacity=100, refill_per_sec=100.0 / 60.0)
        self._historical = TokenBucketRateLimiter(capacity=100, refill_per_sec=10_000.0 / (7 * 24 * 3600))

        # ---- Caches ----
        self._cache_lock = threading.Lock()

        # App non-trading
        self._connectivity_cache: dict[str, CacheEntry] = {}  # key = f"{max_latency_ms}:{pong_timeout_sec}"
        self._stream_health_cache: dict[str, CacheEntry] = {}  # key = f"{symbol}:{max_tick_age_sec}"

        # Account non-trading
        self._account_snapshot_cache = CacheEntry(ttl_sec=10.0)
        self._symbol_spec_cache: dict[str, CacheEntry] = {}  # key = symbol
        self._managed_positions_cache = CacheEntry(ttl_sec=15.0)
        self._managed_positions_args: tuple[Any, ...] | None = None

        # Historical
        self._session_close_cache: dict[str, CacheEntry] = {}  # key = symbol
        self._news_events_cache = CacheEntry(ttl_sec=300.0)

        # ---- Poller threads ----
        self._pollers: list[threading.Thread] = []

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    def connect(self) -> None:
        self._broker.connect()
        self._start_pollers()

    def close(self) -> None:
        self._stop.set()
        for t in self._pollers:
            t.join(timeout=5.0)
        self._pollers.clear()
        self._broker.close()

    def _start_pollers(self) -> None:
        poller_specs = [
            ("app_poller", self._app_poller_loop),
            ("account_poller", self._account_poller_loop),
            ("historical_poller", self._historical_poller_loop),
        ]
        for name, target in poller_specs:
            t = threading.Thread(target=target, name=f"rl-{name}", daemon=True)
            t.start()
            self._pollers.append(t)
            logger.info("Started rate-limit poller: %s", name)

    # -----------------------------------------------------------------------
    # App non-trading (60/min) — cached
    # -----------------------------------------------------------------------

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        key = f"{max_latency_ms}:{pong_timeout_sec}"
        with self._cache_lock:
            entry = self._connectivity_cache.get(key)
            if entry and entry.is_fresh():
                return entry.value
        # Cache miss or stale — fetch inline with rate limiter
        self._app_non_trading.acquire(timeout_sec=10.0)
        result = self._broker.get_connectivity_status(max_latency_ms, pong_timeout_sec)
        with self._cache_lock:
            self._connectivity_cache[key] = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=5.0)
        return result

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        key = f"{symbol}:{max_tick_age_sec}"
        with self._cache_lock:
            entry = self._stream_health_cache.get(key)
            if entry and entry.is_fresh():
                return entry.value
        self._app_non_trading.acquire(timeout_sec=10.0)
        result = self._broker.get_stream_health_status(symbol, max_tick_age_sec)
        with self._cache_lock:
            self._stream_health_cache[key] = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=5.0)
        return result

    # -----------------------------------------------------------------------
    # Account non-trading (30/min) — cached or pass-through
    # -----------------------------------------------------------------------

    def get_account_snapshot(self) -> AccountSnapshot:
        with self._cache_lock:
            if self._account_snapshot_cache.is_fresh():
                return self._account_snapshot_cache.value
        self._account_non_trading.acquire(timeout_sec=15.0)
        result = self._broker.get_account_snapshot()
        with self._cache_lock:
            self._account_snapshot_cache = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=10.0)
        return result

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        with self._cache_lock:
            entry = self._symbol_spec_cache.get(symbol)
            if entry and entry.is_fresh():
                return entry.value
        self._account_non_trading.acquire(timeout_sec=15.0)
        result = self._broker.get_symbol_spec(symbol)
        with self._cache_lock:
            self._symbol_spec_cache[symbol] = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=600.0)
        return result

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
        # This is called infrequently (every 30s from bot.py) — pass through with rate limiter
        self._account_non_trading.acquire(timeout_sec=15.0)
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
        return self._broker.get_position_close_sync(
            position_id,
            deal_reference=deal_reference,
            opened_at=opened_at,
            symbol=symbol,
            **kwargs,
        )

    def get_public_api_backoff_remaining_sec(self) -> float:
        return self._broker.get_public_api_backoff_remaining_sec()

    def get_market_data_wait_remaining_sec(self) -> float:
        return self._broker.get_market_data_wait_remaining_sec()

    # -----------------------------------------------------------------------
    # Account trading (100/min) — pass-through with rate limiter
    # -----------------------------------------------------------------------

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
        return self._broker.open_position(symbol, side, volume, stop_loss, take_profit, comment, entry_price)

    def close_position(self, position: Position, volume: float | None = None) -> None:
        self._account_trading.acquire(timeout_sec=30.0)
        self._broker.close_position(position, volume)

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        self._account_trading.acquire(timeout_sec=30.0)
        self._broker.modify_position(position, stop_loss, take_profit)

    # -----------------------------------------------------------------------
    # Historical / market data (10,000 points/week) — cached or pass-through
    # -----------------------------------------------------------------------

    def get_price(self, symbol: str) -> PriceTick:
        # get_price primarily uses Lightstreamer streaming (no REST rate limit).
        # The IG client internally falls back to REST only when stream is stale.
        # We rate-limit at the proxy level to protect the historical budget.
        self._historical.acquire(timeout_sec=10.0)
        return self._broker.get_price(symbol)

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        key = symbol
        with self._cache_lock:
            entry = self._session_close_cache.get(key)
            if entry and entry.is_fresh():
                return entry.value
        self._historical.acquire(timeout_sec=10.0)
        result = self._broker.get_session_close_utc(symbol, now_ts)
        with self._cache_lock:
            self._session_close_cache[key] = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=3600.0)
        return result

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        with self._cache_lock:
            if self._news_events_cache.is_fresh():
                return self._news_events_cache.value
        self._historical.acquire(timeout_sec=10.0)
        result = self._broker.get_upcoming_high_impact_events(now_ts, within_sec)
        with self._cache_lock:
            self._news_events_cache = CacheEntry(value=result, fetched_at=time.time(), ttl_sec=300.0)
        return result

    # -----------------------------------------------------------------------
    # Attribute proxy — forward any attribute access to the real broker
    # so that code accessing IG-specific attrs (e.g. ig_client internals
    # used by worker/bot) still works.
    # -----------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        return getattr(self._broker, name)

    # -----------------------------------------------------------------------
    # Background pollers
    # -----------------------------------------------------------------------

    def _app_poller_loop(self) -> None:
        """Periodically refreshes app non-trading caches."""
        while not self._stop.is_set():
            try:
                # Refresh connectivity status for common args
                try:
                    self._app_non_trading.acquire(timeout_sec=5.0)
                    result = self._broker.get_connectivity_status(500.0, 5.0)
                    with self._cache_lock:
                        self._connectivity_cache["500.0:5.0"] = CacheEntry(
                            value=result, fetched_at=time.time(), ttl_sec=5.0,
                        )
                except Exception:
                    logger.debug("app_poller: connectivity_status fetch failed", exc_info=True)

                # Refresh stream health per symbol
                for symbol in self._symbols:
                    if self._stop.is_set():
                        return
                    try:
                        self._app_non_trading.acquire(timeout_sec=5.0)
                        result = self._broker.get_stream_health_status(symbol, 30.0)
                        key = f"{symbol}:30.0"
                        with self._cache_lock:
                            self._stream_health_cache[key] = CacheEntry(
                                value=result, fetched_at=time.time(), ttl_sec=5.0,
                            )
                    except Exception:
                        logger.debug("app_poller: stream_health fetch failed for %s", symbol, exc_info=True)

            except Exception:
                logger.warning("app_poller: unexpected error", exc_info=True)

            self._stop.wait(timeout=5.0)

    def _account_poller_loop(self) -> None:
        """Periodically refreshes account non-trading caches."""
        # Pre-fetch symbol specs on startup
        for symbol in self._symbols:
            if self._stop.is_set():
                return
            try:
                self._account_non_trading.acquire(timeout_sec=15.0)
                result = self._broker.get_symbol_spec(symbol)
                with self._cache_lock:
                    self._symbol_spec_cache[symbol] = CacheEntry(
                        value=result, fetched_at=time.time(), ttl_sec=600.0,
                    )
            except Exception:
                logger.warning("account_poller: symbol_spec fetch failed for %s", symbol, exc_info=True)

        while not self._stop.is_set():
            try:
                # Account snapshot
                try:
                    self._account_non_trading.acquire(timeout_sec=15.0)
                    result = self._broker.get_account_snapshot()
                    with self._cache_lock:
                        self._account_snapshot_cache = CacheEntry(
                            value=result, fetched_at=time.time(), ttl_sec=10.0,
                        )
                except Exception:
                    logger.debug("account_poller: account_snapshot fetch failed", exc_info=True)

            except Exception:
                logger.warning("account_poller: unexpected error", exc_info=True)

            self._stop.wait(timeout=10.0)

    def _historical_poller_loop(self) -> None:
        """Periodically refreshes historical/market data caches."""
        while not self._stop.is_set():
            try:
                # News events
                try:
                    self._historical.acquire(timeout_sec=10.0)
                    now = time.time()
                    result = self._broker.get_upcoming_high_impact_events(now, 3600)
                    with self._cache_lock:
                        self._news_events_cache = CacheEntry(
                            value=result, fetched_at=time.time(), ttl_sec=300.0,
                        )
                except Exception:
                    logger.debug("historical_poller: news_events fetch failed", exc_info=True)

                # Session close times per symbol (infrequent)
                for symbol in self._symbols:
                    if self._stop.is_set():
                        return
                    with self._cache_lock:
                        entry = self._session_close_cache.get(symbol)
                        if entry and entry.is_fresh():
                            continue
                    try:
                        self._historical.acquire(timeout_sec=10.0)
                        now = time.time()
                        result = self._broker.get_session_close_utc(symbol, now)
                        with self._cache_lock:
                            self._session_close_cache[symbol] = CacheEntry(
                                value=result, fetched_at=time.time(), ttl_sec=3600.0,
                            )
                    except Exception:
                        logger.debug("historical_poller: session_close fetch failed for %s", symbol, exc_info=True)

            except Exception:
                logger.warning("historical_poller: unexpected error", exc_info=True)

            self._stop.wait(timeout=60.0)
