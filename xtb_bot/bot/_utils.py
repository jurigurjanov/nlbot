from __future__ import annotations

import threading
import time

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE


class _TokenBucket:
    def __init__(self, capacity: int, refill_per_sec: float) -> None:
        self._capacity = max(1, int(capacity))
        self._refill_per_sec = max(FLOAT_COMPARISON_TOLERANCE, float(refill_per_sec))
        self._tokens = float(self._capacity)
        self._last_refill_mono = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now_mono = time.monotonic()
        elapsed = now_mono - self._last_refill_mono
        if elapsed <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + (elapsed * self._refill_per_sec))
        self._last_refill_mono = now_mono

    def try_consume(self, tokens: float = 1.0) -> bool:
        requested = max(FLOAT_COMPARISON_TOLERANCE, float(tokens))
        with self._lock:
            self._refill_locked()
            if self._tokens < requested:
                return False
            self._tokens -= requested
            return True

    def available_tokens(self) -> float:
        with self._lock:
            self._refill_locked()
            return self._tokens


class _BoundedTtlCache:
    def __init__(self, *, max_entries: int, ttl_sec: float) -> None:
        self._max_entries = max(1, int(max_entries))
        self._ttl_sec = max(1.0, float(ttl_sec))
        self._entries: dict[object, tuple[object, float]] = {}
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def get(self, key: object, default: object | None = None) -> object | None:
        with self._lock:
            now = time.monotonic()
            entry = self._entries.get(key)
            if entry is None:
                return default
            value, expires_at = entry
            if expires_at <= now:
                self._entries.pop(key, None)
                return default
            self._entries.pop(key, None)
            self._entries[key] = (value, now + self._ttl_sec)
            self._evict_locked(now)
            return value

    def __setitem__(self, key: object, value: object) -> None:
        with self._lock:
            now = time.monotonic()
            self._entries.pop(key, None)
            self._entries[key] = (value, now + self._ttl_sec)
            self._evict_locked(now)

    def _evict_locked(self, now: float) -> None:
        expired_keys = [
            key
            for key, (_, expires_at) in list(self._entries.items())
            if expires_at <= now
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
        while len(self._entries) > self._max_entries:
            oldest_key = next(iter(self._entries))
            self._entries.pop(oldest_key, None)
