from __future__ import annotations

import logging
import math
import threading
import time
from typing import TYPE_CHECKING

from xtb_bot.models import PriceTick
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotStreamTickRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        self._stream_tick_persist_lock = threading.Lock()
        self._stream_tick_last_persisted_by_symbol: dict[str, tuple[float, float, float, float | None]] = {}
        self._stream_tick_persist_error_last_ts = 0.0
        self._stream_tick_persist_error_log_interval_sec = 30.0
        self._latest_tick_cache_lock = threading.Lock()
        self._latest_tick_cache_by_symbol: dict[str, PriceTick] = {}

    def _register_stream_tick_persistence_hook(self) -> None:
        setter = getattr(self._bot.broker, "set_stream_tick_handler", None)
        if not callable(setter):
            return
        try:
            setter(self._persist_stream_tick_from_broker)
        except Exception:
            logger.debug("Failed to register broker stream tick persistence hook", exc_info=True)

    def _update_latest_tick_from_broker(self, tick: PriceTick) -> None:
        symbol = str(getattr(tick, "symbol", "")).strip().upper()
        if not symbol:
            return
        try:
            bid = float(tick.bid)
            ask = float(tick.ask)
        except (TypeError, ValueError):
            return
        if not math.isfinite(bid) or not math.isfinite(ask):
            return
        ts = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", time.time()))
        if not math.isfinite(ts) or ts <= 0:
            ts = time.time()
        received_at = self._bot._normalize_timestamp_seconds(getattr(tick, "received_at", time.time()))
        if not math.isfinite(received_at) or received_at <= 0:
            received_at = time.time()
        volume: float | None = None
        try:
            if tick.volume is not None:
                parsed_volume = float(tick.volume)
                if math.isfinite(parsed_volume):
                    volume = parsed_volume
        except (TypeError, ValueError):
            volume = None

        normalized_tick = PriceTick(
            symbol=symbol,
            bid=bid,
            ask=ask,
            timestamp=float(ts),
            volume=volume,
            received_at=float(received_at),
        )
        with self._latest_tick_cache_lock:
            previous = self._latest_tick_cache_by_symbol.get(symbol)
            if previous is not None:
                prev_quote_ts = float(getattr(previous, "timestamp", 0.0) or 0.0)
                prev_received_at = float(getattr(previous, "received_at", prev_quote_ts) or prev_quote_ts)
                if ts < (prev_quote_ts - FLOAT_COMPARISON_TOLERANCE):
                    return
                if (
                    abs(received_at - prev_received_at) <= FLOAT_COMPARISON_TOLERANCE
                    and abs(bid - float(previous.bid)) <= FLOAT_ROUNDING_TOLERANCE
                    and abs(ask - float(previous.ask)) <= FLOAT_ROUNDING_TOLERANCE
                    and (
                        (volume is None and previous.volume is None)
                        or (
                            volume is not None
                            and previous.volume is not None
                            and abs(volume - float(previous.volume)) <= FLOAT_ROUNDING_TOLERANCE
                        )
                    )
                ):
                    return
            self._latest_tick_cache_by_symbol[symbol] = normalized_tick

    def _load_latest_tick_from_memory_cache(self, symbol: str, max_age_sec: float) -> PriceTick | None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return None
        with self._latest_tick_cache_lock:
            tick = self._latest_tick_cache_by_symbol.get(normalized_symbol)
        if tick is None:
            return None
        ts = float(getattr(tick, "timestamp", 0.0) or 0.0)
        received_at = float(getattr(tick, "received_at", ts) or ts)
        freshness_ts = received_at if received_at > 0 else ts
        if max_age_sec > 0 and (time.time() - freshness_ts) > float(max_age_sec):
            return None
        return PriceTick(
            symbol=normalized_symbol,
            bid=float(tick.bid),
            ask=float(tick.ask),
            timestamp=ts,
            volume=(float(tick.volume) if tick.volume is not None else None),
            received_at=freshness_ts,
        )

    def _persist_stream_tick_from_broker(self, tick: PriceTick) -> None:
        symbol = str(getattr(tick, "symbol", "")).strip().upper()
        if not symbol:
            return
        try:
            bid = float(tick.bid)
            ask = float(tick.ask)
        except (TypeError, ValueError):
            return
        if not math.isfinite(bid) or not math.isfinite(ask):
            return
        ts = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", time.time()))
        if not math.isfinite(ts) or ts <= 0:
            ts = time.time()
        volume: float | None = None
        try:
            if tick.volume is not None:
                parsed_volume = float(tick.volume)
                if math.isfinite(parsed_volume):
                    volume = parsed_volume
        except (TypeError, ValueError):
            volume = None
        self._update_latest_tick_from_broker(
            PriceTick(
                symbol=symbol,
                bid=bid,
                ask=ask,
                timestamp=ts,
                volume=volume,
                received_at=time.time(),
            )
        )

        with self._stream_tick_persist_lock:
            previous = self._stream_tick_last_persisted_by_symbol.get(symbol)
            if previous is not None:
                prev_ts, prev_bid, prev_ask, prev_volume = previous
                if (
                    abs(ts - prev_ts) <= FLOAT_COMPARISON_TOLERANCE
                    and abs(bid - prev_bid) <= FLOAT_ROUNDING_TOLERANCE
                    and abs(ask - prev_ask) <= FLOAT_ROUNDING_TOLERANCE
                    and (
                        (volume is None and prev_volume is None)
                        or (
                            volume is not None
                            and prev_volume is not None
                            and abs(volume - prev_volume) <= FLOAT_ROUNDING_TOLERANCE
                        )
                    )
                ):
                    return
            self._stream_tick_last_persisted_by_symbol[symbol] = (ts, bid, ask, volume)

        try:
            self._bot.store.upsert_broker_tick(
                symbol=symbol,
                bid=bid,
                ask=ask,
                ts=ts,
                volume=volume,
                source="ig_stream_direct",
            )
        except Exception as exc:
            now = time.time()
            if (now - self._stream_tick_persist_error_last_ts) >= self._stream_tick_persist_error_log_interval_sec:
                self._stream_tick_persist_error_last_ts = now
                logger.warning("Failed to persist direct broker stream tick for %s: %s", symbol, exc)
