from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class RunMode(str, Enum):
    SIGNAL_ONLY = "signal_only"
    PAPER = "paper"
    EXECUTION = "execution"


class AccountType(str, Enum):
    DEMO = "demo"
    LIVE = "live"


@dataclass(slots=True)
class PriceTick:
    symbol: str
    bid: float
    ask: float
    timestamp: float
    volume: float | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass(slots=True)
class AccountSnapshot:
    balance: float
    equity: float
    margin_free: float
    timestamp: float


@dataclass(slots=True)
class SymbolSpec:
    symbol: str
    tick_size: float
    tick_value: float
    contract_size: float
    lot_min: float
    lot_max: float
    lot_step: float
    price_precision: int = 5
    lot_precision: int = 2
    metadata: dict[str, Any] = field(default_factory=dict)

    def round_volume(self, raw: float) -> float:
        if raw <= 0:
            return 0.0
        step = self.lot_step if self.lot_step > 0 else 0.01
        normalized = round(raw / step) * step
        clamped = min(max(normalized, self.lot_min), self.lot_max)
        return round(clamped, self.lot_precision)


@dataclass(slots=True)
class NewsEvent:
    event_id: str
    name: str
    timestamp: float
    impact: str = "high"
    country: str | None = None


@dataclass(slots=True)
class ConnectivityStatus:
    healthy: bool
    reason: str
    latency_ms: float | None = None
    pong_ok: bool | None = None


@dataclass(slots=True)
class StreamHealthStatus:
    healthy: bool
    connected: bool
    reason: str
    symbol: str | None = None
    last_tick_age_sec: float | None = None
    reconnect_attempts: int = 0
    total_reconnects: int = 0
    last_disconnect_ts: float | None = None
    last_reconnect_ts: float | None = None
    next_retry_in_sec: float | None = None
    desired_subscriptions: int = 0
    active_subscriptions: int = 0
    last_error: str | None = None
    price_requests_total: int = 0
    stream_hits_total: int = 0
    rest_fallback_hits_total: int = 0
    stream_hit_rate_pct: float | None = None


@dataclass(slots=True)
class Signal:
    side: Side
    confidence: float
    stop_loss_pips: float
    take_profit_pips: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Position:
    position_id: str
    symbol: str
    side: Side
    volume: float
    open_price: float
    stop_loss: float
    take_profit: float
    opened_at: float
    entry_confidence: float = 0.0
    status: str = "open"
    close_price: float | None = None
    closed_at: float | None = None
    pnl: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        values = dict(data)
        values["side"] = Side(values["side"])
        return cls(**values)


@dataclass(slots=True)
class PendingOpen:
    pending_id: str
    symbol: str
    side: Side
    volume: float
    entry: float
    stop_loss: float
    take_profit: float
    created_at: float
    thread_name: str
    strategy: str
    mode: str
    entry_confidence: float = 0.0
    position_id: str | None = None
    trailing_override: dict[str, float] | None = None


@dataclass(slots=True)
class WorkerState:
    symbol: str
    thread_name: str
    mode: RunMode
    strategy: str
    last_price: float | None
    last_heartbeat: float
    iteration: int
    position: dict[str, Any] | None = None
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["mode"] = self.mode.value
        return payload
