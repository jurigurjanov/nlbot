from __future__ import annotations

import time

import pytest

from xtb_bot.client import BaseBrokerClient, BrokerError
from xtb_bot.ig_proxy import CacheEntry, RateLimitedBrokerProxy
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


class _FakeBroker(BaseBrokerClient):
    def __init__(self) -> None:
        self.connected = False
        self.account_snapshot_calls = 0
        self.symbol_spec_calls = 0
        self.managed_positions_calls = 0
        self.custom_attr = "proxy-target"

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def get_price(self, symbol: str) -> PriceTick:
        return PriceTick(symbol=str(symbol).upper(), bid=1.0, ask=1.1, timestamp=time.time())

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        self.symbol_spec_calls += 1
        upper = str(symbol).upper()
        return SymbolSpec(
            symbol=upper,
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
        self.account_snapshot_calls += 1
        return AccountSnapshot(balance=10_000.0, equity=10_100.0, margin_free=9_900.0, timestamp=time.time())

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        _ = symbol
        return float(now_ts) + 3600.0

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        _ = (now_ts, within_sec)
        return []

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        _ = (max_latency_ms, pong_timeout_sec)
        return ConnectivityStatus(healthy=True, reason="ok", latency_ms=5.0, pong_ok=True)

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        _ = (symbol, max_tick_age_sec)
        return StreamHealthStatus(healthy=True, connected=True, reason="ok")

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
        return "POS-1"

    def close_position(self, position: Position, volume: float | None = None) -> None:
        _ = (position, volume)

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        _ = (position, stop_loss, take_profit)

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
        self.managed_positions_calls += 1
        return {}


def test_proxy_caches_account_snapshot_and_symbol_spec() -> None:
    broker = _FakeBroker()
    proxy = RateLimitedBrokerProxy(broker, symbols=["EURUSD"], pollers_enabled=False)

    first_snapshot = proxy.get_account_snapshot()
    second_snapshot = proxy.get_account_snapshot()
    assert first_snapshot.balance == second_snapshot.balance
    assert broker.account_snapshot_calls == 1

    first_spec = proxy.get_symbol_spec("eurusd")
    second_spec = proxy.get_symbol_spec("EURUSD")
    assert first_spec.symbol == "EURUSD"
    assert second_spec.symbol == "EURUSD"
    assert broker.symbol_spec_calls == 1


def test_proxy_returns_stale_snapshot_when_limiter_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    broker = _FakeBroker()
    proxy = RateLimitedBrokerProxy(broker, symbols=["EURUSD"], pollers_enabled=False)
    stale = AccountSnapshot(balance=9_999.0, equity=9_999.0, margin_free=9_999.0, timestamp=1.0)
    proxy._account_snapshot_cache = CacheEntry(  # type: ignore[assignment]
        value=stale,
        fetched_at=time.time() - 5.0,
        ttl_sec=1.0,
    )

    def _raise_timeout(timeout_sec: float = 30.0) -> None:
        _ = timeout_sec
        raise BrokerError("timeout")

    monkeypatch.setattr(proxy._account_non_trading, "acquire", _raise_timeout)

    snapshot = proxy.get_account_snapshot()
    assert snapshot.balance == pytest.approx(9_999.0)
    assert broker.account_snapshot_calls == 0


def test_proxy_rate_limits_managed_positions_read(monkeypatch: pytest.MonkeyPatch) -> None:
    broker = _FakeBroker()
    proxy = RateLimitedBrokerProxy(broker, symbols=["EURUSD"], pollers_enabled=False)
    acquire_calls: list[float] = []

    def _track_acquire(timeout_sec: float = 30.0) -> None:
        acquire_calls.append(float(timeout_sec))

    monkeypatch.setattr(proxy._account_non_trading, "acquire", _track_acquire)

    result = proxy.get_managed_open_positions("MAGIC", "INSTANCE")
    assert result == {}
    assert broker.managed_positions_calls == 1
    assert acquire_calls == [20.0]


def test_proxy_forwards_unknown_attributes() -> None:
    broker = _FakeBroker()
    proxy = RateLimitedBrokerProxy(broker, symbols=["EURUSD"], pollers_enabled=False)
    assert proxy.custom_attr == "proxy-target"
