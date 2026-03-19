from __future__ import annotations

import time

import pytest

from xtb_bot.client import BrokerError
from xtb_bot.client import XtbApiClient
from xtb_bot.models import AccountType, Position, PriceTick, Side


def test_open_position_waits_for_trade_transaction_status(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        trade_status_timeout_sec=2.0,
        trade_status_poll_interval_sec=0.01,
    )

    calls: list[tuple[str, dict]] = []
    responses = [
        {"ask": 1.1000, "bid": 1.0998},
        {"order": 42},
        {"requestStatus": 1, "message": "pending"},
        {"requestStatus": 3, "message": "accepted"},
    ]

    def fake_send_command(command: str, arguments: dict | None = None):
        calls.append((command, arguments or {}))
        return responses.pop(0)

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    monkeypatch.setattr("xtb_bot.client.time.sleep", lambda _: None)

    order_id = client.open_position(
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        stop_loss=1.0950,
        take_profit=1.1050,
        comment="test",
    )

    assert order_id == "42"
    assert [item[0] for item in calls] == [
        "getSymbol",
        "tradeTransaction",
        "tradeTransactionStatus",
        "tradeTransactionStatus",
    ]


def test_modify_position_waits_for_trade_transaction_status(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        trade_status_timeout_sec=2.0,
        trade_status_poll_interval_sec=0.01,
    )
    position = Position(
        position_id="42",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1000,
        stop_loss=1.0990,
        take_profit=1.1020,
        opened_at=1.0,
    )

    calls: list[tuple[str, dict]] = []
    responses = [
        {"ask": 1.1001, "bid": 1.0999},
        {"order": 555},
        {"requestStatus": 1, "message": "pending"},
        {"requestStatus": 3, "message": "accepted"},
    ]

    def fake_send_command(command: str, arguments: dict | None = None):
        calls.append((command, arguments or {}))
        return responses.pop(0)

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    monkeypatch.setattr("xtb_bot.client.time.sleep", lambda _: None)

    client.modify_position(position, stop_loss=1.1002, take_profit=1.1020)
    assert [item[0] for item in calls] == [
        "getSymbol",
        "tradeTransaction",
        "tradeTransactionStatus",
        "tradeTransactionStatus",
    ]


def test_xtb_account_snapshot_cache_reduces_duplicate_requests(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        account_snapshot_cache_ttl_sec=10.0,
    )
    calls = {"count": 0}

    def fake_send_command(command: str, arguments: dict | None = None):
        _ = arguments
        if command == "getMarginLevel":
            calls["count"] += 1
            return {
                "balance": 10_000.0,
                "equity": 10_050.0,
                "marginFree": 9_800.0,
            }
        raise AssertionError(f"Unexpected command: {command}")

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    snap1 = client.get_account_snapshot()
    snap2 = client.get_account_snapshot()

    assert calls["count"] == 1
    assert snap1.balance == 10_000.0
    assert snap2.equity == 10_050.0


def test_get_session_close_utc_parses_trading_hours(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )
    # 2026-02-27 20:50:00 UTC (Friday)
    now_ts = 1772225400.0
    calls: list[str] = []

    def fake_send_command(command: str, arguments: dict | None = None):
        calls.append(command)
        assert command == "getTradingHours"
        _ = arguments
        return [
            {
                "symbol": "US100",
                "trading": [
                    {"day": 5, "fromT": 0, "toT": 21 * 60 * 60 * 1000},  # Friday 21:00
                ],
            }
        ]

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    close_ts = client.get_session_close_utc("US100", now_ts)
    assert close_ts is not None
    assert int(close_ts - now_ts) == 600
    assert calls == ["getTradingHours"]


def test_get_upcoming_high_impact_events_filters_window(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )
    now_ts = 1_700_000_000.0

    def fake_send_command(command: str, arguments: dict | None = None):
        assert command == "getCalendar"
        _ = arguments
        return [
            {"id": "1", "title": "NFP", "impact": 3, "time": int((now_ts + 60) * 1000), "country": "US"},
            {"id": "2", "title": "Low impact", "impact": 1, "time": int((now_ts + 60) * 1000), "country": "US"},
        ]

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    events = client.get_upcoming_high_impact_events(now_ts, within_sec=300)
    assert len(events) == 1
    assert events[0].event_id == "1"
    assert events[0].name == "NFP"


def test_connectivity_status_unhealthy_when_latency_exceeds_limit(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )

    def fake_send_command(command: str, arguments: dict | None = None):
        _ = (command, arguments)
        client._last_latency_ms = 900.0
        return {}

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    status = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    assert status.healthy is False
    assert "latency_too_high" in status.reason


def test_connectivity_status_unhealthy_when_pong_missing(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )

    def fake_send_command(command: str, arguments: dict | None = None):
        _ = (command, arguments)
        client._last_latency_ms = 100.0
        return {}

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    monkeypatch.setattr(client, "_perform_control_ping", lambda timeout_sec: False)
    status = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    assert status.healthy is False
    assert status.reason == "pong_timeout_or_missing"


def test_derive_stream_endpoint_for_xtb_demo_and_live():
    assert XtbApiClient._derive_stream_endpoint("wss://xapi.xtb.com/demo") == "wss://xapi.xtb.com/demoStream"
    assert XtbApiClient._derive_stream_endpoint("wss://xapi.xtb.com/real") == "wss://xapi.xtb.com/realStream"


def test_get_price_prefers_cached_stream_tick(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )
    expected = PriceTick(symbol="EURUSD", bid=1.1010, ask=1.1012, timestamp=time.time())
    client._tick_cache["EURUSD"] = expected

    monkeypatch.setattr(client, "_ensure_tick_subscription_locked", lambda symbol: True)
    monkeypatch.setattr(client, "_send_command", lambda command, arguments=None: (_ for _ in ()).throw(
        AssertionError(f"unexpected command call: {command} {arguments}")
    ))

    tick = client.get_price("EURUSD")
    assert tick == expected


def test_get_price_falls_back_to_get_symbol_when_stream_cache_missing(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        timeout_sec=0.1,
    )
    calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(client, "_ensure_tick_subscription_locked", lambda symbol: False)

    def fake_send_command(command: str, arguments: dict | None = None):
        calls.append((command, arguments or {}))
        assert command == "getSymbol"
        assert arguments == {"symbol": "EURUSD"}
        return {"bid": 1.2000, "ask": 1.2002, "time": 1700000000000}

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    monkeypatch.setattr("xtb_bot.client.time.sleep", lambda _: None)

    tick = client.get_price("EURUSD")
    assert tick.symbol == "EURUSD"
    assert tick.bid == 1.2000
    assert tick.ask == 1.2002
    assert tick.timestamp == 1700000000.0
    assert [item[0] for item in calls] == ["getSymbol"]


def test_open_position_uses_stream_tick_cache_for_price(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )
    client._tick_cache["EURUSD"] = PriceTick(symbol="EURUSD", bid=1.2200, ask=1.2203, timestamp=time.time())

    calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(client, "_ensure_tick_subscription_locked", lambda symbol: True)
    monkeypatch.setattr(client, "_wait_trade_transaction", lambda order_id: {"requestStatus": 3})

    def fake_send_command(command: str, arguments: dict | None = None):
        calls.append((command, arguments or {}))
        assert command == "tradeTransaction"
        info = (arguments or {}).get("tradeTransInfo", {})
        assert info.get("price") == 1.2203
        return {"order": 777}

    monkeypatch.setattr(client, "_send_command", fake_send_command)

    order_id = client.open_position(
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        stop_loss=1.2150,
        take_profit=1.2300,
        comment="stream-cache-test",
    )
    assert order_id == "777"
    assert [item[0] for item in calls] == ["tradeTransaction"]


def test_stream_health_status_reports_command_socket_not_connected():
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )

    status = client.get_stream_health_status(symbol="EURUSD", max_tick_age_sec=10.0)
    assert status.healthy is False
    assert status.connected is False
    assert status.reason == "command_socket_not_connected"


def test_stream_health_status_detects_stale_tick_for_symbol():
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )
    client._ws = object()  # type: ignore[assignment]
    client._stream_session_id = "session-1"
    client._stream_ws = object()  # type: ignore[assignment]
    client._stream_reader_thread = None
    client._stream_desired_subscriptions.add("EURUSD")
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1000,
        ask=1.1002,
        timestamp=time.time() - 30.0,
    )

    status = client.get_stream_health_status(symbol="EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is False
    assert "stream_tick_stale" in status.reason


def test_send_command_skips_unexpected_pending_messages_before_valid_response():
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )

    class _FakeWs:
        def __init__(self):
            self.sent: list[str] = []
            self.recv_calls = 0

        def send(self, payload: str) -> None:
            self.sent.append(payload)

        def recv(self) -> str:
            self.recv_calls += 1
            return '{"status": true, "returnData": {"ok": 1}}'

    ws = _FakeWs()
    client._ws = ws  # type: ignore[assignment]
    client._pending_raw_messages.append('{"foo": "bar"}')
    client._pending_raw_messages.append("NOT_A_JSON")

    payload = client._send_command("ping")

    assert payload == {"ok": 1}
    assert ws.recv_calls == 1
    assert ws.sent


def test_wait_trade_transaction_does_not_accept_missing_request_status(monkeypatch):
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        trade_status_timeout_sec=0.03,
        trade_status_poll_interval_sec=0.0,
    )

    def fake_send_command(command: str, arguments: dict | None = None):
        _ = (command, arguments)
        return {"message": "still_processing"}

    monkeypatch.setattr(client, "_send_command", fake_send_command)
    monkeypatch.setattr("xtb_bot.client.time.sleep", lambda _: None)

    with pytest.raises(BrokerError, match="expected requestStatus=3"):
        client._wait_trade_transaction(42)


def test_send_command_ignores_mismatched_custom_tag_response():
    client = XtbApiClient(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
    )

    class _FakeWs:
        def __init__(self):
            self.sent: list[str] = []
            self._responses = [
                '{"status": true, "customTag": "cmd-999", "returnData": {"stale": 1}}',
                '{"status": true, "customTag": "cmd-1", "returnData": {"ok": 2}}',
            ]

        def send(self, payload: str) -> None:
            self.sent.append(payload)

        def recv(self) -> str:
            return self._responses.pop(0)

    ws = _FakeWs()
    client._ws = ws  # type: ignore[assignment]

    payload = client._send_command("ping")

    assert payload == {"ok": 2}
    assert ws.sent
