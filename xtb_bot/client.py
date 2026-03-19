from __future__ import annotations

from collections import deque
import json
import logging
import os
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import websocket  # type: ignore
except ImportError:  # pragma: no cover
    websocket = None

from xtb_bot.models import (
    AccountType,
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


class BrokerError(RuntimeError):
    pass


class BaseBrokerClient(ABC):
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_price(self, symbol: str) -> PriceTick:
        raise NotImplementedError

    @abstractmethod
    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        raise NotImplementedError

    @abstractmethod
    def get_account_snapshot(self) -> AccountSnapshot:
        raise NotImplementedError

    @abstractmethod
    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        raise NotImplementedError

    @abstractmethod
    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        raise NotImplementedError

    @abstractmethod
    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        raise NotImplementedError

    @abstractmethod
    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def close_position(self, position: Position, volume: float | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        raise NotImplementedError

    def get_position_open_sync(self, position_id: str) -> dict[str, Any] | None:
        _ = position_id
        return None

    def get_public_api_backoff_remaining_sec(self) -> float:
        return 0.0

    def get_market_data_wait_remaining_sec(self) -> float:
        return 0.0

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
        _ = (
            magic_prefix,
            magic_instance,
            preferred_symbols,
            known_deal_references,
            known_position_ids,
            pending_opens,
            include_unmatched_preferred,
        )
        return {}


@dataclass(slots=True)
class MockBrokerClient(BaseBrokerClient):
    start_balance: float = 10_000.0

    def __post_init__(self) -> None:
        self._connected = False
        self._lock = threading.Lock()
        self._prices: dict[str, float] = {}
        self._balance = self.start_balance
        self._news_events: list[NewsEvent] = []
        self._session_close_utc_map: dict[str, str] = {
            "US100": "21:00",
            "US500": "21:00",
            "US30": "21:00",
            "DE40": "21:00",
            "FRA40": "21:00",
            "UK100": "21:00",
            "JP225": "20:00",
        }
        self._specs: dict[str, SymbolSpec] = {
            "EURUSD": SymbolSpec(
                symbol="EURUSD",
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                price_precision=5,
                lot_precision=2,
            ),
            "GBPUSD": SymbolSpec(
                symbol="GBPUSD",
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                price_precision=5,
                lot_precision=2,
            ),
            "USDJPY": SymbolSpec(
                symbol="USDJPY",
                tick_size=0.01,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                price_precision=3,
                lot_precision=2,
            ),
            "XAUUSD": SymbolSpec(
                symbol="XAUUSD",
                tick_size=0.1,
                tick_value=1.0,
                contract_size=100.0,
                lot_min=0.01,
                lot_max=50.0,
                lot_step=0.01,
                price_precision=2,
                lot_precision=2,
            ),
            "WTI": SymbolSpec(
                symbol="WTI",
                tick_size=0.1,
                tick_value=1.0,
                contract_size=100.0,
                lot_min=0.01,
                lot_max=50.0,
                lot_step=0.01,
                price_precision=2,
                lot_precision=2,
            ),
            "US100": SymbolSpec(
                symbol="US100",
                tick_size=1.0,
                tick_value=1.0,
                contract_size=10.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                price_precision=1,
                lot_precision=2,
            ),
        }

    def connect(self) -> None:
        with self._lock:
            self._connected = True

    def close(self) -> None:
        with self._lock:
            self._connected = False

    def _seed_price(self, symbol: str) -> float:
        seeds = {
            "EURUSD": 1.0850,
            "GBPUSD": 1.2700,
            "USDJPY": 149.00,
            "XAUUSD": 2030.0,
            "WTI": 75.0,
            "US100": 17750.0,
        }
        return float(seeds.get(symbol, 100.0))

    def get_price(self, symbol: str) -> PriceTick:
        with self._lock:
            if not self._connected:
                raise BrokerError("Mock broker is not connected")
            current = self._prices.setdefault(symbol, self._seed_price(symbol))
            delta = current * random.uniform(-0.0006, 0.0006)
            next_price = max(0.0001, current + delta)
            self._prices[symbol] = next_price
            spread = max(next_price * 0.00008, 0.00001)
            bid = next_price - spread / 2.0
            ask = next_price + spread / 2.0
            volume = random.uniform(10.0, 250.0)
            return PriceTick(symbol=symbol, bid=bid, ask=ask, timestamp=time.time(), volume=volume)

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        with self._lock:
            if not self._connected:
                raise BrokerError("Mock broker is not connected")
            spec = self._specs.get(symbol)
            if spec is not None:
                return spec

            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                price_precision=5,
                lot_precision=2,
            )

    def get_account_snapshot(self) -> AccountSnapshot:
        with self._lock:
            if not self._connected:
                raise BrokerError("Mock broker is not connected")
            return AccountSnapshot(
                balance=self._balance,
                equity=self._balance,
                margin_free=self._balance,
                timestamp=time.time(),
            )

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        with self._lock:
            if not self._connected:
                raise BrokerError("Mock broker is not connected")
            close_hhmm = self._session_close_utc_map.get(symbol.upper())
            if close_hhmm is None:
                return None

        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        hh, mm = (int(part) for part in close_hhmm.split(":", 1))
        close_dt = now_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return close_dt.timestamp()

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        with self._lock:
            if not self._connected:
                raise BrokerError("Mock broker is not connected")
            end_ts = now_ts + max(0, within_sec)
            result = [
                event
                for event in self._news_events
                if now_ts <= event.timestamp <= end_ts and str(event.impact).lower() == "high"
            ]
            result.sort(key=lambda event: event.timestamp)
            return result

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        _ = pong_timeout_sec
        with self._lock:
            if not self._connected:
                return ConnectivityStatus(
                    healthy=False,
                    reason="broker_not_connected",
                    latency_ms=None,
                    pong_ok=False,
                )
        simulated_latency_ms = min(5.0, max_latency_ms / 10.0)
        return ConnectivityStatus(
            healthy=True,
            reason="ok",
            latency_ms=simulated_latency_ms,
            pong_ok=True,
        )

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        _ = (symbol, max_tick_age_sec)
        with self._lock:
            healthy = self._connected
        if healthy:
            return StreamHealthStatus(
                healthy=True,
                connected=True,
                reason="ok",
                symbol=symbol.upper() if symbol else None,
            )
        return StreamHealthStatus(
            healthy=False,
            connected=False,
            reason="broker_not_connected",
            symbol=symbol.upper() if symbol else None,
        )

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
        _ = (symbol, side, volume, stop_loss, take_profit, comment, entry_price)
        if not self._connected:
            raise BrokerError("Mock broker is not connected")
        return f"mock-{uuid.uuid4().hex[:12]}"

    def close_position(self, position: Position, volume: float | None = None) -> None:
        if not self._connected:
            raise BrokerError("Mock broker is not connected")
        _ = (position, volume)

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        if not self._connected:
            raise BrokerError("Mock broker is not connected")
        _ = (position, stop_loss, take_profit)


@dataclass(slots=True)
class XtbApiClient(BaseBrokerClient):
    user_id: str
    password: str
    app_name: str
    account_type: AccountType
    endpoint: str | None = None
    timeout_sec: float = 10.0
    trade_status_timeout_sec: float = 15.0
    trade_status_poll_interval_sec: float = 0.4
    account_snapshot_cache_ttl_sec: float = 5.0

    def __post_init__(self) -> None:
        self._lock = threading.RLock()
        self._ws: websocket.WebSocket | None = None
        self._stream_ws: websocket.WebSocket | None = None
        self._stream_reader_thread: threading.Thread | None = None
        self._stream_stop_event = threading.Event()
        self._stream_monitor_thread: threading.Thread | None = None
        self._stream_monitor_stop_event = threading.Event()
        self._control_keepalive_thread: threading.Thread | None = None
        self._control_keepalive_stop_event = threading.Event()
        self._stream_session_id: str | None = None
        self._trading_hours_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._calendar_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._pending_raw_messages: deque[str] = deque()
        self._command_seq = 0
        self._last_latency_ms: float | None = None
        self._last_pong_ok: bool | None = None
        raw_keepalive_interval = os.getenv("XTB_CONTROL_KEEPALIVE_INTERVAL_SEC", "25")
        try:
            self._control_keepalive_interval_sec = max(0.0, float(raw_keepalive_interval))
        except (TypeError, ValueError):
            self._control_keepalive_interval_sec = 25.0
        raw_keepalive_ping_timeout = os.getenv("XTB_CONTROL_KEEPALIVE_PING_TIMEOUT_SEC", "2.0")
        try:
            self._control_keepalive_ping_timeout_sec = max(0.1, float(raw_keepalive_ping_timeout))
        except (TypeError, ValueError):
            self._control_keepalive_ping_timeout_sec = 2.0
        self._last_control_keepalive_error_log_monotonic = 0.0
        self._tick_cache: dict[str, PriceTick] = {}
        self._stream_desired_subscriptions: set[str] = set()
        self._tick_subscriptions: set[str] = set()
        self._stream_last_error: str | None = None
        self._stream_reconnect_attempts = 0
        self._stream_total_reconnects = 0
        self._stream_last_disconnect_ts: float | None = None
        self._stream_last_reconnect_ts: float | None = None
        self._stream_next_retry_at: float | None = None
        self._stream_backoff_base_sec = 0.5
        self._stream_backoff_max_sec = 30.0
        self._account_snapshot_cache: AccountSnapshot | None = None
        self._account_snapshot_cached_at = 0.0
        self._account_snapshot_lock = threading.Lock()
        self._endpoint = self.endpoint or (
            "wss://xapi.xtb.com/demo"
            if self.account_type == AccountType.DEMO
            else "wss://xapi.xtb.com/real"
        )
        self._stream_endpoint = self._derive_stream_endpoint(self._endpoint)

    @staticmethod
    def _lot_precision(step: float) -> int:
        raw = f"{step:.8f}".rstrip("0")
        if "." not in raw:
            return 0
        return min(8, len(raw.split(".", 1)[1]))

    @staticmethod
    def _derive_stream_endpoint(endpoint: str) -> str:
        if endpoint.endswith("Stream"):
            return endpoint
        if endpoint.endswith("/demo"):
            return f"{endpoint}Stream"
        if endpoint.endswith("/real"):
            return f"{endpoint}Stream"
        return f"{endpoint.rstrip('/')}/stream"

    def connect(self) -> None:
        if websocket is None:
            raise BrokerError(
                "websocket-client is not installed. Install dependency: pip install websocket-client"
            )
        with self._lock:
            if self._ws is not None:
                return
            ws = websocket.create_connection(self._endpoint, timeout=self.timeout_sec)
            self._ws = ws
            login = self._send_command(
                "login",
                {
                    "userId": self.user_id,
                    "password": self.password,
                    "appName": self.app_name,
                },
            )
            self._stream_session_id = login.get("streamSessionId")
            self._ensure_stream_connected_locked()
            self._start_stream_monitor_locked()
            self._start_control_keepalive_locked()

    def close(self) -> None:
        monitor_to_join: threading.Thread | None = None
        reader_to_join: threading.Thread | None = None
        keepalive_to_join: threading.Thread | None = None
        with self._lock:
            monitor_to_join = self._stop_stream_monitor_locked()
            keepalive_to_join = self._stop_control_keepalive_locked()
            reader_to_join = self._close_stream_locked()
            if self._ws is not None:
                try:
                    self._send_command("logout")
                except Exception:
                    pass
                self._ws.close()
                self._ws = None
            self._stream_session_id = None
            self._pending_raw_messages.clear()
            self._tick_cache.clear()
            self._stream_desired_subscriptions.clear()
            self._tick_subscriptions.clear()
            self._stream_last_error = None
            self._stream_next_retry_at = None
            self._stream_reconnect_attempts = 0
            self._account_snapshot_cache = None
            self._account_snapshot_cached_at = 0.0
        if reader_to_join is not None and reader_to_join.is_alive():
            reader_to_join.join(timeout=1.0)
        if monitor_to_join is not None and monitor_to_join.is_alive():
            monitor_to_join.join(timeout=1.0)
        if keepalive_to_join is not None and keepalive_to_join.is_alive():
            keepalive_to_join.join(timeout=1.0)

    def _ensure_connected(self) -> websocket.WebSocket:
        if self._ws is None:
            raise BrokerError("XTB API client is not connected")
        return self._ws

    def _stream_backoff_delay(self, attempt: int) -> float:
        if attempt <= 0:
            return self._stream_backoff_base_sec
        return min(self._stream_backoff_max_sec, self._stream_backoff_base_sec * (2 ** (attempt - 1)))

    def _start_stream_monitor_locked(self) -> None:
        monitor = self._stream_monitor_thread
        if monitor is not None and monitor.is_alive():
            return

        self._stream_monitor_stop_event.clear()
        self._stream_monitor_thread = threading.Thread(
            target=self._stream_monitor_loop,
            name="xtb-stream-monitor",
            daemon=True,
        )
        self._stream_monitor_thread.start()

    def _stop_stream_monitor_locked(self) -> threading.Thread | None:
        self._stream_monitor_stop_event.set()
        monitor = self._stream_monitor_thread
        self._stream_monitor_thread = None
        if monitor is threading.current_thread():
            return None
        return monitor

    def _start_control_keepalive_locked(self) -> None:
        if self._control_keepalive_interval_sec <= 0:
            return
        thread = self._control_keepalive_thread
        if thread is not None and thread.is_alive():
            return
        self._control_keepalive_stop_event.clear()
        self._control_keepalive_thread = threading.Thread(
            target=self._control_keepalive_loop,
            name="xtb-control-keepalive",
            daemon=True,
        )
        self._control_keepalive_thread.start()

    def _stop_control_keepalive_locked(self) -> threading.Thread | None:
        self._control_keepalive_stop_event.set()
        thread = self._control_keepalive_thread
        self._control_keepalive_thread = None
        if thread is threading.current_thread():
            return None
        return thread

    def _control_keepalive_loop(self) -> None:
        interval = self._control_keepalive_interval_sec
        while not self._control_keepalive_stop_event.wait(interval):
            with self._lock:
                if self._ws is None:
                    return
                try:
                    self._send_command("ping")
                    # Raw websocket ping helps detect half-open TCP state.
                    self._perform_control_ping(self._control_keepalive_ping_timeout_sec)
                except Exception as exc:
                    now = time.monotonic()
                    if (
                        now - self._last_control_keepalive_error_log_monotonic
                    ) >= max(10.0, interval):
                        logger.warning("Control keepalive ping failed: %s", exc)
                        self._last_control_keepalive_error_log_monotonic = now

    def _mark_stream_disconnected_locked(self, error: str | None) -> None:
        stream_ws = self._stream_ws
        self._stream_ws = None
        self._tick_subscriptions.clear()
        self._stream_last_disconnect_ts = time.time()
        if error:
            self._stream_last_error = error
        if self._stream_next_retry_at is None:
            self._stream_next_retry_at = time.time()
        if stream_ws is not None:
            try:
                stream_ws.close()
            except Exception:
                pass

    def _ensure_stream_connected_locked(self) -> bool:
        if websocket is None:
            return False
        if self._stream_ws is not None:
            return True
        if not self._stream_session_id:
            return False
        now = time.time()
        if self._stream_next_retry_at is not None and now < self._stream_next_retry_at:
            return False
        try:
            stream_ws = websocket.create_connection(self._stream_endpoint, timeout=self.timeout_sec)
        except Exception as exc:
            self._stream_last_error = str(exc)
            self._stream_reconnect_attempts += 1
            self._stream_last_disconnect_ts = now
            backoff_sec = self._stream_backoff_delay(self._stream_reconnect_attempts)
            self._stream_next_retry_at = now + backoff_sec
            logger.warning(
                "Failed to connect stream endpoint %s (attempt=%s, retry_in=%.2fs): %s",
                self._stream_endpoint,
                self._stream_reconnect_attempts,
                backoff_sec,
                exc,
            )
            return False

        was_reconnect = self._stream_last_disconnect_ts is not None
        self._stream_ws = stream_ws
        self._stream_stop_event.clear()
        self._stream_reader_thread = threading.Thread(
            target=self._stream_reader_loop,
            name="xtb-stream-reader",
            daemon=True,
        )
        self._stream_reader_thread.start()
        self._stream_last_error = None
        self._stream_next_retry_at = None
        self._stream_reconnect_attempts = 0
        self._stream_last_reconnect_ts = time.time()
        if was_reconnect:
            self._stream_total_reconnects += 1
            logger.warning(
                "Stream reconnected | endpoint=%s total_reconnects=%s",
                self._stream_endpoint,
                self._stream_total_reconnects,
            )
        return True

    def _close_stream_locked(self) -> threading.Thread | None:
        self._stream_stop_event.set()
        stream_ws = self._stream_ws
        self._stream_ws = None
        self._tick_subscriptions.clear()
        if stream_ws is not None:
            try:
                stream_ws.close()
            except Exception:
                pass
        reader = self._stream_reader_thread
        self._stream_reader_thread = None
        self._stream_next_retry_at = None
        self._stream_reconnect_attempts = 0
        if reader is threading.current_thread():
            return None
        return reader

    def _stream_monitor_loop(self) -> None:
        while not self._stream_monitor_stop_event.is_set():
            wait_for = 0.5
            with self._lock:
                if self._ws is None or not self._stream_session_id:
                    return

                reader = self._stream_reader_thread
                if self._stream_ws is not None and reader is not None and not reader.is_alive():
                    self._mark_stream_disconnected_locked("stream_reader_stopped")

                if self._stream_ws is None:
                    now = time.time()
                    if self._stream_next_retry_at is None or now >= self._stream_next_retry_at:
                        self._ensure_stream_connected_locked()
                    retry_at = self._stream_next_retry_at
                    if retry_at is not None:
                        wait_for = max(0.2, min(2.0, retry_at - time.time()))

            self._stream_monitor_stop_event.wait(wait_for)

    @staticmethod
    def _parse_stream_tick(raw_message: str) -> PriceTick | None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            return None

        for key in ("data", "returnData"):
            candidate = payload.get(key)
            if isinstance(candidate, dict) and "symbol" in candidate:
                payload = candidate
                break

        if not isinstance(payload, dict):
            return None
        if "symbol" not in payload or "bid" not in payload or "ask" not in payload:
            return None

        timestamp_raw = payload.get("timestamp", payload.get("time", time.time() * 1000))
        try:
            ts = float(timestamp_raw)
        except (TypeError, ValueError):
            ts = time.time() * 1000
        if ts > 10_000_000_000:
            ts /= 1000.0

        volume_raw = payload.get("vol", payload.get("volume", payload.get("quoteVolume")))
        try:
            volume = float(volume_raw) if volume_raw is not None else None
        except (TypeError, ValueError):
            volume = None
        if volume is not None and volume <= 0:
            volume = None

        return PriceTick(
            symbol=str(payload["symbol"]).upper(),
            bid=float(payload["bid"]),
            ask=float(payload["ask"]),
            timestamp=ts,
            volume=volume,
        )

    def _stream_reader_loop(self) -> None:
        while not self._stream_stop_event.is_set():
            stream_ws = self._stream_ws
            if stream_ws is None:
                return
            try:
                raw = stream_ws.recv()
                if raw is None:
                    continue
                tick = self._parse_stream_tick(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if tick is not None:
                    with self._lock:
                        self._tick_cache[tick.symbol] = tick
            except Exception as exc:
                with self._lock:
                    # Ignore stale reader errors after a reconnect replaced socket instance.
                    if self._stream_ws is stream_ws:
                        self._mark_stream_disconnected_locked(str(exc))
                if not self._stream_stop_event.is_set():
                    logger.warning("Stream reader stopped: %s", exc)
                return

    def _send_stream_subscribe_locked(self, symbol: str) -> None:
        if self._stream_ws is None:
            raise BrokerError("XTB stream socket is not connected")
        if not self._stream_session_id:
            raise BrokerError("XTB stream session id is missing")

        payload = {
            "command": "getTickPrices",
            "symbol": symbol,
            "streamSessionId": self._stream_session_id,
            "minArrivalTime": 1,
            "maxLevel": 0,
        }
        self._stream_ws.send(json.dumps(payload))

    def _ensure_tick_subscription_locked(self, symbol: str) -> bool:
        symbol = symbol.upper()
        self._stream_desired_subscriptions.add(symbol)
        if symbol in self._tick_subscriptions:
            return True
        if not self._ensure_stream_connected_locked():
            return False
        try:
            self._send_stream_subscribe_locked(symbol)
        except Exception as exc:
            self._mark_stream_disconnected_locked(str(exc))
            logger.warning("Failed to subscribe tick stream for %s: %s", symbol, exc)
            return False
        self._tick_subscriptions.add(symbol)
        return True

    @staticmethod
    def _raw_message_to_text(raw_message: Any) -> str:
        if isinstance(raw_message, bytes):
            return raw_message.decode("utf-8", errors="replace")
        return str(raw_message)

    def _recv_command_response_payload(
        self,
        ws: websocket.WebSocket,
        expected_custom_tag: str | None,
    ) -> dict[str, Any]:
        # Control ping handler may queue text/binary frames while waiting for PONG.
        # Consume queued/raw frames until a valid command response payload is found.
        deadline = time.monotonic() + max(0.5, float(self.timeout_sec))
        while True:
            if self._pending_raw_messages:
                raw_response = self._pending_raw_messages.popleft()
            else:
                raw_response = ws.recv()
            response_text = self._raw_message_to_text(raw_response).strip()
            if not response_text:
                if time.monotonic() > deadline:
                    raise BrokerError("XTB command response timeout (empty payload)")
                continue
            try:
                parsed = json.loads(response_text)
            except json.JSONDecodeError:
                if time.monotonic() > deadline:
                    raise BrokerError(
                        f"XTB command response timeout (non-JSON payload received: {response_text[:200]!r})"
                    )
                logger.debug("Ignoring non-JSON control payload: %s", response_text[:200])
                continue
            if isinstance(parsed, dict) and "status" in parsed:
                if expected_custom_tag:
                    response_tag = str(parsed.get("customTag") or "").strip()
                    if response_tag and response_tag != expected_custom_tag:
                        logger.debug(
                            "Ignoring command response with mismatched customTag: expected=%s got=%s",
                            expected_custom_tag,
                            response_tag,
                        )
                        if time.monotonic() > deadline:
                            raise BrokerError(
                                "XTB command response timeout (mismatched customTag payloads received)"
                            )
                        continue
                return parsed
            if time.monotonic() > deadline:
                raise BrokerError(
                    "XTB command response timeout (JSON payload without status field received)"
                )
            logger.debug("Ignoring unexpected control JSON payload: %s", response_text[:200])

    def _send_command(self, command: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        ws = self._ensure_connected()
        payload: dict[str, Any] = {"command": command}
        if arguments:
            payload["arguments"] = arguments
        self._command_seq += 1
        custom_tag = f"cmd-{self._command_seq}"
        payload["customTag"] = custom_tag

        started = time.perf_counter()
        ws.send(json.dumps(payload))
        response = self._recv_command_response_payload(ws, expected_custom_tag=custom_tag)
        self._last_latency_ms = (time.perf_counter() - started) * 1000.0

        if not response.get("status", False):
            code = response.get("errorCode", "UNKNOWN")
            descr = response.get("errorDescr", "No description")
            raise BrokerError(f"XTB command {command} failed: {code} {descr}")

        return response.get("returnData", {})

    def _perform_control_ping(self, timeout_sec: float) -> bool:
        ws = self._ensure_connected()
        payload = f"hb-{int(time.time() * 1000)}".encode("utf-8")

        previous_timeout = getattr(ws, "timeout", None)
        ws.settimeout(timeout_sec)
        try:
            ws.ping(payload)
            deadline = time.time() + timeout_sec
            while time.time() < deadline:
                frame = ws.recv_frame()
                if frame is None:
                    continue
                opcode = frame.opcode

                if opcode == websocket.ABNF.OPCODE_PONG:
                    return True
                if opcode == websocket.ABNF.OPCODE_PING:
                    ws.pong(frame.data)
                    continue
                if opcode in {websocket.ABNF.OPCODE_TEXT, websocket.ABNF.OPCODE_BINARY}:
                    data = frame.data
                    self._pending_raw_messages.append(self._raw_message_to_text(data))
                    continue
                if opcode == websocket.ABNF.OPCODE_CLOSE:
                    return False
            return False
        finally:
            ws.settimeout(previous_timeout)

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        with self._lock:
            try:
                self._send_command("ping")
                latency_ms = self._last_latency_ms
            except Exception as exc:
                self._last_pong_ok = False
                return ConnectivityStatus(
                    healthy=False,
                    reason=f"ping_command_failed:{exc}",
                    latency_ms=None,
                    pong_ok=False,
                )

            if latency_ms is not None and latency_ms > max_latency_ms:
                self._last_pong_ok = False
                return ConnectivityStatus(
                    healthy=False,
                    reason=f"latency_too_high:{latency_ms:.2f}ms>{max_latency_ms:.2f}ms",
                    latency_ms=latency_ms,
                    pong_ok=False,
                )

            try:
                pong_ok = self._perform_control_ping(max(pong_timeout_sec, 0.1))
            except Exception as exc:
                self._last_pong_ok = False
                return ConnectivityStatus(
                    healthy=False,
                    reason=f"pong_check_failed:{exc}",
                    latency_ms=latency_ms,
                    pong_ok=False,
                )

            self._last_pong_ok = pong_ok
            if not pong_ok:
                return ConnectivityStatus(
                    healthy=False,
                    reason="pong_timeout_or_missing",
                    latency_ms=latency_ms,
                    pong_ok=False,
                )

            return ConnectivityStatus(
                healthy=True,
                reason="ok",
                latency_ms=latency_ms,
                pong_ok=True,
            )

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        with self._lock:
            normalized_symbol = symbol.upper() if symbol else None
            now = time.time()

            reader = self._stream_reader_thread
            connected = self._stream_ws is not None and (reader is None or reader.is_alive())

            last_tick_age_sec: float | None = None
            if normalized_symbol:
                tick = self._tick_cache.get(normalized_symbol)
                if tick is not None:
                    last_tick_age_sec = max(0.0, now - tick.timestamp)
            elif self._tick_cache:
                freshest_ts = max(item.timestamp for item in self._tick_cache.values())
                last_tick_age_sec = max(0.0, now - freshest_ts)

            next_retry_in_sec = None
            if self._stream_next_retry_at is not None:
                next_retry_in_sec = max(0.0, self._stream_next_retry_at - now)

            desired_subscriptions = len(self._stream_desired_subscriptions)
            active_subscriptions = len(self._tick_subscriptions)

            healthy = True
            reason = "ok"
            if self._ws is None or not self._stream_session_id:
                healthy = False
                reason = "command_socket_not_connected"
            elif not connected:
                healthy = False
                reason = "stream_disconnected"
                if self._stream_last_error:
                    reason = f"{reason}:{self._stream_last_error}"
            elif (
                normalized_symbol
                and normalized_symbol in self._stream_desired_subscriptions
                and last_tick_age_sec is None
            ):
                healthy = False
                reason = "stream_tick_missing"
            elif (
                normalized_symbol
                and normalized_symbol in self._stream_desired_subscriptions
                and last_tick_age_sec is not None
                and last_tick_age_sec > max(0.1, max_tick_age_sec)
            ):
                healthy = False
                reason = f"stream_tick_stale:{last_tick_age_sec:.2f}s>{max_tick_age_sec:.2f}s"

            return StreamHealthStatus(
                healthy=healthy,
                connected=connected,
                reason=reason,
                symbol=normalized_symbol,
                last_tick_age_sec=last_tick_age_sec,
                reconnect_attempts=self._stream_reconnect_attempts,
                total_reconnects=self._stream_total_reconnects,
                last_disconnect_ts=self._stream_last_disconnect_ts,
                last_reconnect_ts=self._stream_last_reconnect_ts,
                next_retry_in_sec=next_retry_in_sec,
                desired_subscriptions=desired_subscriptions,
                active_subscriptions=active_subscriptions,
                last_error=self._stream_last_error,
            )

    def _wait_trade_transaction(self, order_id: int) -> dict[str, Any]:
        started = time.time()
        last_response: dict[str, Any] = {}

        while (time.time() - started) < self.trade_status_timeout_sec:
            data = self._send_command("tradeTransactionStatus", {"order": int(order_id)})
            last_response = data
            request_status_raw = data.get("requestStatus")

            if request_status_raw is None:
                # Do not treat missing status as accepted; keep polling until timeout.
                time.sleep(self.trade_status_poll_interval_sec)
                continue

            try:
                request_status = int(request_status_raw)
            except (TypeError, ValueError):
                time.sleep(self.trade_status_poll_interval_sec)
                continue

            if request_status == 3:
                return data

            if request_status == 1:
                time.sleep(self.trade_status_poll_interval_sec)
                continue

            message = str(data.get("message", "Unknown trade transaction status"))
            raise BrokerError(
                f"tradeTransaction rejected for order={order_id} status={request_status}: {message}"
            )

        raise BrokerError(
            "Timeout waiting tradeTransactionStatus for order="
            f"{order_id}; last={last_response}; expected requestStatus=3"
        )

    def _parse_symbol_spec(self, symbol: str, data: dict[str, Any]) -> SymbolSpec:
        price_precision = int(data.get("precision", 5) or 5)
        tick_size = float(data.get("tickSize") or 10 ** (-price_precision))
        tick_value = float(data.get("tickValue") or 1.0)
        contract_size = float(data.get("contractSize") or 1.0)
        lot_min = float(data.get("lotMin") or 0.01)
        lot_max = float(data.get("lotMax") or 100.0)
        lot_step = float(data.get("lotStep") or 0.01)

        if tick_size <= 0:
            tick_size = 10 ** (-price_precision)
        if lot_step <= 0:
            lot_step = 0.01
        stops_level_raw = data.get("stopsLevel", data.get("stops_level"))
        try:
            stops_level_points = float(stops_level_raw) if stops_level_raw is not None else 0.0
        except (TypeError, ValueError):
            stops_level_points = 0.0
        if stops_level_points < 0:
            stops_level_points = 0.0
        min_stop_distance_price = stops_level_points * tick_size
        leverage_raw = data.get("leverage")
        try:
            leverage = float(leverage_raw) if leverage_raw is not None else 0.0
        except (TypeError, ValueError):
            leverage = 0.0

        return SymbolSpec(
            symbol=symbol,
            tick_size=tick_size,
            tick_value=max(tick_value, 1e-9),
            contract_size=max(contract_size, 1e-9),
            lot_min=max(lot_min, lot_step),
            lot_max=max(lot_max, lot_min),
            lot_step=lot_step,
            price_precision=max(price_precision, 0),
            lot_precision=self._lot_precision(lot_step),
            metadata={
                "broker": "xtb",
                "stops_level_points": stops_level_points,
                "min_stop_distance_price": min_stop_distance_price,
                "leverage": leverage,
            },
        )

    def _get_cached_tick_locked(self, symbol: str, max_age_sec: float = 5.0) -> PriceTick | None:
        tick = self._tick_cache.get(symbol.upper())
        if tick is None:
            return None
        if (time.time() - tick.timestamp) > max_age_sec:
            return None
        return PriceTick(
            symbol=tick.symbol,
            bid=float(tick.bid),
            ask=float(tick.ask),
            timestamp=float(tick.timestamp),
            volume=(float(tick.volume) if tick.volume is not None else None),
        )

    def get_price(self, symbol: str) -> PriceTick:
        normalized_symbol = symbol.upper()
        with self._lock:
            self._ensure_tick_subscription_locked(normalized_symbol)

        deadline = time.time() + min(1.0, self.timeout_sec)
        while time.time() < deadline:
            with self._lock:
                cached = self._get_cached_tick_locked(normalized_symbol, max_age_sec=10.0)
                if cached is not None:
                    return cached
            time.sleep(0.05)

        with self._lock:
            data = self._send_command("getSymbol", {"symbol": normalized_symbol})
            ts_ms = data.get("time") or int(time.time() * 1000)
            volume_raw = data.get("vol", data.get("volume", data.get("quoteVolume")))
            try:
                volume = float(volume_raw) if volume_raw is not None else None
            except (TypeError, ValueError):
                volume = None
            if volume is not None and volume <= 0:
                volume = None
            return PriceTick(
                symbol=normalized_symbol,
                bid=float(data["bid"]),
                ask=float(data["ask"]),
                timestamp=float(ts_ms) / 1000.0,
                volume=volume,
            )

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        with self._lock:
            normalized_symbol = symbol.upper()
            self._ensure_tick_subscription_locked(normalized_symbol)
            data = self._send_command("getSymbol", {"symbol": normalized_symbol})
            return self._parse_symbol_spec(normalized_symbol, data)

    def get_account_snapshot(self) -> AccountSnapshot:
        now = time.time()
        with self._lock:
            cached = self._account_snapshot_cache
            cached_age = now - self._account_snapshot_cached_at
        if cached is not None and cached_age <= self.account_snapshot_cache_ttl_sec:
            return AccountSnapshot(
                balance=cached.balance,
                equity=cached.equity,
                margin_free=cached.margin_free,
                timestamp=now,
            )

        with self._account_snapshot_lock:
            now = time.time()
            with self._lock:
                cached = self._account_snapshot_cache
                cached_age = now - self._account_snapshot_cached_at
                if cached is not None and cached_age <= self.account_snapshot_cache_ttl_sec:
                    return AccountSnapshot(
                        balance=cached.balance,
                        equity=cached.equity,
                        margin_free=cached.margin_free,
                        timestamp=now,
                    )

                data = self._send_command("getMarginLevel")
                balance = float(data.get("balance", 0.0))
                equity = float(data.get("equity", balance))
                margin_free = float(data.get("margin_free", data.get("marginFree", equity)))
                snapshot = AccountSnapshot(
                    balance=balance,
                    equity=equity,
                    margin_free=margin_free,
                    timestamp=now,
                )
                self._account_snapshot_cache = snapshot
                self._account_snapshot_cached_at = now
                return snapshot

    @staticmethod
    def _map_xtb_day_to_weekday(day_value: int) -> int:
        # Supports both 1..7 (Mon..Sun) and 0..6 (Sun..Sat) conventions.
        return (int(day_value) - 1) % 7

    @staticmethod
    def _event_timestamp_seconds(raw_value: Any) -> float | None:
        if raw_value is None:
            return None
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return None
        if value > 10_000_000_000:
            value /= 1000.0
        return value

    @staticmethod
    def _is_high_impact(raw_impact: Any) -> bool:
        if raw_impact is None:
            return False
        if isinstance(raw_impact, (int, float)):
            return float(raw_impact) >= 3.0
        text = str(raw_impact).strip().lower()
        if text in {"3", "high"}:
            return True
        return "high" in text

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        with self._lock:
            now = time.time()
            cache_key = symbol.upper()
            cached = self._trading_hours_cache.get(cache_key)
            if cached is None or (now - cached[0]) > 1800:
                data = self._send_command("getTradingHours", {"symbols": [symbol]})
                symbol_data: dict[str, Any] | None = None
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and str(item.get("symbol", "")).upper() == cache_key:
                            symbol_data = item
                            break
                elif isinstance(data, dict):
                    symbol_data = data

                trading_entries = []
                if isinstance(symbol_data, dict):
                    raw_entries = symbol_data.get("trading") or symbol_data.get("quotes") or []
                    if isinstance(raw_entries, list):
                        trading_entries = [entry for entry in raw_entries if isinstance(entry, dict)]
                self._trading_hours_cache[cache_key] = (now, trading_entries)
            else:
                trading_entries = cached[1]

        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        weekday = now_dt.weekday()
        day_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        closes: list[float] = []
        for entry in trading_entries:
            day_raw = entry.get("day")
            to_t_raw = entry.get("toT")
            if day_raw is None or to_t_raw is None:
                continue
            try:
                mapped_weekday = self._map_xtb_day_to_weekday(int(day_raw))
                close_ms = int(to_t_raw)
            except (TypeError, ValueError):
                continue
            if mapped_weekday != weekday or close_ms <= 0:
                continue

            close_dt = day_start + timedelta(milliseconds=close_ms)
            closes.append(close_dt.timestamp())

        if not closes:
            return None
        closes.sort()
        for close_ts in closes:
            if close_ts >= now_ts:
                return close_ts
        return closes[-1]

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        with self._lock:
            now = time.time()
            if self._calendar_cache is None or (now - self._calendar_cache[0]) > 60:
                data = self._send_command("getCalendar")
                raw_events = data if isinstance(data, list) else []
                self._calendar_cache = (now, raw_events)
            else:
                raw_events = self._calendar_cache[1]

        end_ts = now_ts + max(0, within_sec)
        result: list[NewsEvent] = []
        for event in raw_events:
            if not isinstance(event, dict):
                continue
            ts = self._event_timestamp_seconds(event.get("time", event.get("timestamp")))
            if ts is None or ts < now_ts or ts > end_ts:
                continue
            if not self._is_high_impact(event.get("impact")):
                continue

            title = str(event.get("title") or event.get("name") or "High impact event")
            country_raw = event.get("country")
            country = str(country_raw) if country_raw not in (None, "") else None
            event_id = str(event.get("id") or f"{int(ts)}:{title}")
            result.append(
                NewsEvent(
                    event_id=event_id,
                    name=title,
                    timestamp=ts,
                    impact="high",
                    country=country,
                )
            )

        result.sort(key=lambda item: item.timestamp)
        return result

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
        _ = entry_price
        cmd = 0 if side == Side.BUY else 1
        with self._lock:
            normalized_symbol = symbol.upper()
            self._ensure_tick_subscription_locked(normalized_symbol)
            cached = self._get_cached_tick_locked(normalized_symbol)
            if cached is not None:
                price = float(cached.ask if side == Side.BUY else cached.bid)
            else:
                tick = self._send_command("getSymbol", {"symbol": normalized_symbol})
                price = float(tick["ask"] if side == Side.BUY else tick["bid"])
            payload = {
                "tradeTransInfo": {
                    "cmd": cmd,
                    "symbol": normalized_symbol,
                    "type": 0,
                    "volume": float(volume),
                    "price": price,
                    "sl": float(stop_loss),
                    "tp": float(take_profit),
                    "customComment": comment,
                }
            }
            response = self._send_command("tradeTransaction", payload)
            order_id_raw = response.get("order")
            if order_id_raw is None:
                raise BrokerError("tradeTransaction succeeded but order id is missing")

            order_id = int(order_id_raw)
            self._wait_trade_transaction(order_id)
            return str(order_id)

    def close_position(self, position: Position, volume: float | None = None) -> None:
        cmd = 0 if position.side == Side.BUY else 1
        close_volume = float(position.volume if volume is None else volume)
        if close_volume <= 0:
            raise BrokerError("close volume must be positive")
        if close_volume - float(position.volume) > 1e-9:
            raise BrokerError(
                f"close volume exceeds open position volume (requested={close_volume:g}, open={float(position.volume):g})"
            )
        with self._lock:
            symbol = position.symbol.upper()
            self._ensure_tick_subscription_locked(symbol)
            cached = self._get_cached_tick_locked(symbol)
            if cached is not None:
                price = float(cached.bid if position.side == Side.BUY else cached.ask)
            else:
                tick = self._send_command("getSymbol", {"symbol": symbol})
                price = float(tick["bid"] if position.side == Side.BUY else tick["ask"])
            payload = {
                "tradeTransInfo": {
                    "cmd": cmd,
                    "symbol": symbol,
                    "type": 2,
                    "order": int(position.position_id),
                    "volume": close_volume,
                    "price": price,
                }
            }
            response = self._send_command("tradeTransaction", payload)
            order_id_raw = response.get("order")
            if order_id_raw is None:
                raise BrokerError("tradeTransaction close succeeded but order id is missing")

            self._wait_trade_transaction(int(order_id_raw))

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        cmd = 0 if position.side == Side.BUY else 1
        with self._lock:
            symbol = position.symbol.upper()
            self._ensure_tick_subscription_locked(symbol)
            cached = self._get_cached_tick_locked(symbol)
            if cached is not None:
                price = float(cached.bid if position.side == Side.BUY else cached.ask)
            else:
                tick = self._send_command("getSymbol", {"symbol": symbol})
                price = float(tick["bid"] if position.side == Side.BUY else tick["ask"])
            payload = {
                "tradeTransInfo": {
                    "cmd": cmd,
                    "symbol": symbol,
                    "type": 3,
                    "order": int(position.position_id),
                    "volume": float(position.volume),
                    "price": price,
                    "sl": float(stop_loss),
                    "tp": float(take_profit),
                }
            }
            response = self._send_command("tradeTransaction", payload)
            order_id_raw = response.get("order")
            if order_id_raw is None:
                raise BrokerError("tradeTransaction modify succeeded but order id is missing")

            self._wait_trade_transaction(int(order_id_raw))
