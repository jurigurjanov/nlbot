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
from dataclasses import dataclass
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
    ``try_acquire()`` is non-blocking: returns True if a token was consumed.
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

    def try_acquire(self) -> bool:
        """Try to consume a token without blocking.  Returns True on success."""
        with self._cond:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

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

    def is_fresh(self) -> bool:
        return self.value is not None and (time.time() - self.fetched_at) < self.ttl_sec

    def is_usable(self, max_stale_sec: float = 120.0) -> bool:
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

        # ---- Rate limiters ----
        # We use a SINGLE bucket for all account non-trading requests
        # (get_price REST, get_account_snapshot, get_symbol_spec, etc.)
        # because IG counts them all against the same 30/min limit.
        #
        # Capacity (burst) is kept LOW to prevent startup burst from
        # tripping IG's sliding-window rate limit.  Refill gives the
        # sustained rate (well below 30/min to leave headroom).
        self._account_non_trading = TokenBucketRateLimiter(capacity=5, refill_per_sec=25.0 / 60.0)
        self._account_trading = TokenBucketRateLimiter(capacity=10, refill_per_sec=100.0 / 60.0)
        self._historical = TokenBucketRateLimiter(capacity=10, refill_per_sec=10_000.0 / (7 * 24 * 3600))

        # ---- Caches ----
        self._cache_lock = threading.Lock()

        # Account non-trading
        self._account_snapshot_cache = CacheEntry(ttl_sec=10.0)
        self._symbol_spec_cache: dict[str, CacheEntry] = {}
        self._managed_positions_cache = CacheEntry(ttl_sec=15.0)

        # Historical
        self._session_close_cache: dict[str, CacheEntry] = {}
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
        # No rate limiting — this is a Lightstreamer ping/pong, NOT a REST call.
        return self._broker.get_connectivity_status(max_latency_ms, pong_timeout_sec)

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        # No rate limiting — this checks internal stream state, NOT a REST call.
        return self._broker.get_stream_health_status(symbol, max_tick_age_sec)

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
    # Price data — stream-first with REST fallback + per-symbol cache
    # -----------------------------------------------------------------------

    def get_price(self, symbol: str) -> PriceTick:
        # Let the IG client handle stream-vs-REST logic internally.
        # It checks Lightstreamer first, waits for fresh tick, falls back
        # to REST only when needed.  The IG client already has its own
        # rest_market_min_interval_sec throttle for REST calls.
        #
        # The proxy does NOT intercept the stream cache — the IG client's
        # get_price() is the single source of truth for tick freshness.
        return self._broker.get_price(symbol)

    # -----------------------------------------------------------------------
    # Historical / market data (10,000 points/week) — cached
    # -----------------------------------------------------------------------

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

    def _account_poller_loop(self) -> None:
        """Periodically refreshes account non-trading caches.

        Symbol specs are NOT pre-fetched here — workers fetch them on
        demand via ``get_symbol_spec()`` which caches for 600 s.
        This avoids burning through the IG rate limit at startup when
        many symbols are configured.
        """
        while not self._stop.is_set():
            try:
                # Account snapshot — main payload of this poller
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
        """Periodically refreshes news events cache.

        Session close times and symbol specs are fetched lazily (on demand)
        and cached with long TTLs, so they don't need a poller.
        """
        while not self._stop.is_set():
            try:
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

            except Exception:
                logger.warning("historical_poller: unexpected error", exc_info=True)

            self._stop.wait(timeout=300.0)
