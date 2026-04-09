from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import io
import logging
import threading
import time
from urllib import error as urlerror

import pytest

from xtb_bot.client import BrokerError
import xtb_bot.ig_client as ig_client_module
from xtb_bot.ig_client import (
    LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS,
    IgApiClient,
    _as_float,
    _normalize_volume_floor_for_spec,
    _is_fx_pair_symbol,
)
from xtb_bot.models import AccountType, PendingOpen, Position, PriceTick, Side, SymbolSpec


class _AliveThread:
    def is_alive(self) -> bool:
        return True


def _install_fake_lightstreamer_sdk(monkeypatch):
    created_clients: list[object] = []
    created_subscriptions: list[object] = []

    class _FakeConnectionDetails:
        def __init__(self) -> None:
            self.user: str | None = None
            self.password: str | None = None

        def setUser(self, user: str) -> None:  # noqa: N802
            self.user = user

        def setPassword(self, password: str) -> None:  # noqa: N802
            self.password = password

    class _FakeLightstreamerClient:
        def __init__(self, server_address: str, adapter_set: str) -> None:
            self.server_address = server_address
            self.adapter_set = adapter_set
            self.connectionDetails = _FakeConnectionDetails()
            self.listeners: list[object] = []
            self.subscriptions: list[object] = []
            created_clients.append(self)

        def addListener(self, listener: object) -> None:  # noqa: N802
            self.listeners.append(listener)

        def removeListener(self, listener: object) -> None:  # noqa: N802
            if listener in self.listeners:
                self.listeners.remove(listener)

        def connect(self) -> None:
            for listener in list(self.listeners):
                callback = getattr(listener, "onStatusChange", None)
                if callable(callback):
                    callback("CONNECTED:WS-STREAMING")

        def disconnect(self) -> None:
            for listener in list(self.listeners):
                callback = getattr(listener, "onStatusChange", None)
                if callable(callback):
                    callback("DISCONNECTED")

        def subscribe(self, subscription: object) -> None:
            self.subscriptions.append(subscription)

        def unsubscribe(self, subscription: object) -> None:
            if subscription in self.subscriptions:
                self.subscriptions.remove(subscription)

    class _FakeLightstreamerSubscription:
        def __init__(self, mode: str, items: list[str], fields: list[str]) -> None:
            self.mode = mode
            self.items = items
            self.fields = fields
            self.data_adapter: str | None = None
            self.requested_snapshot: str | None = None
            self.listeners: list[object] = []
            created_subscriptions.append(self)

        def setDataAdapter(self, adapter: str) -> None:  # noqa: N802
            self.data_adapter = adapter

        def setRequestedSnapshot(self, requested_snapshot: str) -> None:  # noqa: N802
            self.requested_snapshot = requested_snapshot

        def addListener(self, listener: object) -> None:  # noqa: N802
            self.listeners.append(listener)

    monkeypatch.setattr(ig_client_module, "_LightstreamerClient", _FakeLightstreamerClient)
    monkeypatch.setattr(ig_client_module, "_LightstreamerSubscription", _FakeLightstreamerSubscription)
    return created_clients, created_subscriptions


def _make_client() -> IgApiClient:
    return IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
        account_id="ABC123",
        rest_market_min_interval_sec=0.0,
    )


def test_as_float_rejects_arbitrary_error_text():
    assert _as_float("Error-500.2x", 0.0) == pytest.approx(0.0)


def test_as_float_accepts_common_localized_numeric_formats():
    assert _as_float("1,234.56", 0.0) == pytest.approx(1234.56)
    assert _as_float("1.234,56", 0.0) == pytest.approx(1234.56)
    assert _as_float("1 234,56", 0.0) == pytest.approx(1234.56)


def test_parse_position_opened_at_accepts_iso_timezone_offsets():
    now_ts = 1_700_000_000.0
    expected = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc).timestamp()

    parsed = IgApiClient._parse_position_opened_at(
        {"createdDateUTC": "2024-01-15T10:30:00+00:00"},
        now_ts,
    )
    assert parsed == pytest.approx(expected)

    parsed_z = IgApiClient._parse_position_opened_at(
        {"createdDateUTC": "2024-01-15T10:30:00Z"},
        now_ts,
    )
    assert parsed_z == pytest.approx(expected)


def test_get_account_currency_code_coalesces_concurrent_refreshes(monkeypatch):
    client = _make_client()
    calls = {"n": 0}
    ready = threading.Event()

    def fake_request(*args, **kwargs):
        _ = (args, kwargs)
        calls["n"] += 1
        ready.set()
        time.sleep(0.05)
        return {"accounts": [{"accountId": "ABC123", "currency": "USD"}]}, {}

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    results: list[str | None] = []
    threads = [
        threading.Thread(target=lambda: results.append(client.get_account_currency_code()))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    ready.wait(timeout=1.0)
    for thread in threads:
        thread.join(timeout=1.0)

    assert sorted(results) == ["USD", "USD"]
    assert calls["n"] == 1


def test_request_deferred_message_reports_actual_remaining(monkeypatch):
    client = _make_client()
    client._connected = True  # type: ignore[attr-defined]

    now = {"ts": 100.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["ts"])
    client._trade_submit_min_interval_sec = 0.75  # type: ignore[attr-defined]
    client._trade_submit_next_allowed_ts = 104.2  # type: ignore[attr-defined]
    client._critical_trade_active_total = 1  # type: ignore[attr-defined]
    client._critical_trade_owner_threads = {999999: 1}  # type: ignore[attr-defined]

    with pytest.raises(BrokerError, match=r"critical_trade_operation_active.*\(4\.2s remaining\)"):
        client._request("GET", "/session", version="1", auth=True)  # type: ignore[attr-defined]


def test_ig_request_retries_transient_http_after_auth_refresh(monkeypatch):
    client = _make_client()
    client._connected = True
    attempts = {"count": 0}

    class _FakeResponse:
        def __init__(self, body: str) -> None:
            encoded = body.encode("utf-8")
            self._raw = encoded
            self.length = len(encoded)
            self.headers: dict[str, str] = {}
            self.status = 200

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise urlerror.HTTPError(
                req.full_url,
                401,
                "Unauthorized",
                hdrs={},
                fp=io.BytesIO(b'{"errorCode":"error.security.client-token-invalid"}'),
            )
        if attempts["count"] == 2:
            raise urlerror.HTTPError(
                req.full_url,
                500,
                "Server Error",
                hdrs={},
                fp=io.BytesIO(b"{}"),
            )
        return _FakeResponse("{}")

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)
    monkeypatch.setattr(
        client,
        "_maybe_refresh_auth_session_after_failure",
        lambda **kwargs: True,
    )

    body, _ = client._request_direct("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]
    assert body == {}
    assert attempts["count"] == 3


def test_ig_request_retries_network_error_after_auth_refresh(monkeypatch):
    client = _make_client()
    client._connected = True
    attempts = {"count": 0}

    class _FakeResponse:
        def __init__(self, body: str) -> None:
            encoded = body.encode("utf-8")
            self._raw = encoded
            self.length = len(encoded)
            self.headers: dict[str, str] = {}
            self.status = 200

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise urlerror.HTTPError(
                req.full_url,
                401,
                "Unauthorized",
                hdrs={},
                fp=io.BytesIO(b'{"errorCode":"error.security.client-token-invalid"}'),
            )
        if attempts["count"] == 2:
            raise urlerror.URLError("connection reset")
        return _FakeResponse("{}")

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)
    monkeypatch.setattr(
        client,
        "_maybe_refresh_auth_session_after_failure",
        lambda **kwargs: True,
    )

    body, _ = client._request_direct("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]
    assert body == {}
    assert attempts["count"] == 3


def test_ig_request_window_wait_blocks_when_limit_is_zero():
    wait_sec = IgApiClient._request_window_wait_locked(deque(), 0, 100.0, 60.0)
    assert wait_sec == float("inf")


def test_ig_local_rate_limiter_blocks_requests_when_limit_is_zero():
    client = _make_client()
    client._rate_limit_enabled = True  # type: ignore[attr-defined]
    client._rate_limit_account_non_trading_per_min = 0  # type: ignore[attr-defined]
    client._rate_limit_app_non_trading_per_min = 0  # type: ignore[attr-defined]

    with pytest.raises(BrokerError, match="limit=0"):
        client._reserve_request_rate_limit_slot("GET", "/positions", None)  # type: ignore[attr-defined]


def test_ig_stream_line_updates_tick_cache_with_delta_fields():
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None, None, None]

    assert client._handle_stream_line("1,1|1.1000|1.1002|1710000000000")
    first = client._tick_cache["EURUSD"]
    assert first.bid == pytest.approx(1.1)
    assert first.ask == pytest.approx(1.1002)

    assert client._handle_stream_line("1,1||1.1003|")
    second = client._tick_cache["EURUSD"]
    assert second.bid == pytest.approx(1.1)
    assert second.ask == pytest.approx(1.1003)
    assert second.timestamp == pytest.approx(first.timestamp)


def test_ig_stream_tick_handler_receives_stream_updates():
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None, None, None]
    received: list[PriceTick] = []
    client.set_stream_tick_handler(lambda tick: received.append(tick))

    assert client._handle_stream_line("1,1|1.1000|1.1002|1710000000000")
    assert len(received) == 1
    delivered = received[0]
    internal = client._tick_cache["EURUSD"]
    assert delivered.symbol == "EURUSD"
    assert delivered.bid == pytest.approx(1.1)
    assert delivered.ask == pytest.approx(1.1002)
    assert delivered.timestamp == pytest.approx(1_710_000_000.0)
    assert delivered.received_at is not None
    assert delivered is not internal


def test_ig_cached_tick_freshness_uses_received_at(monkeypatch):
    client = _make_client()
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: 200.0)
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1,
        ask=1.1002,
        timestamp=100.0,
        received_at=198.0,
    )

    cached = client._get_cached_tick_locked("EURUSD", max_age_sec=5.0)
    assert cached is not None
    assert cached.received_at == pytest.approx(198.0)


def test_ig_stream_line_accepts_table_only_prefix():
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None, None, None]

    assert client._handle_stream_line("1|1.1000|1.1002|1710000000000")
    tick = client._tick_cache["EURUSD"]
    assert tick.bid == pytest.approx(1.1)
    assert tick.ask == pytest.approx(1.1002)
    assert tick.timestamp == pytest.approx(1_710_000_000.0)


def test_ig_stream_line_parses_hhmmss_timestamp(monkeypatch):
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None, None, None]
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now_dt.timestamp())

    assert client._handle_stream_line("1,1|1.1000|1.1002|165458")
    tick = client._tick_cache["EURUSD"]
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert tick.timestamp == pytest.approx(expected)


def test_ig_stream_line_parses_hms_text_timestamp(monkeypatch):
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None, None, None]
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now_dt.timestamp())

    assert client._handle_stream_line("1,1|1.1000|1.1002|16:54:58")
    tick = client._tick_cache["EURUSD"]
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert tick.timestamp == pytest.approx(expected)


def test_ig_stream_line_accepts_bid_offer_update_time_fields():
    client = _make_client()
    client._stream_table_to_symbol[1] = "EURUSD"
    client._stream_table_field_values[1] = [None] * 6

    # Legacy BIDPRICE1/ASKPRICE1 are empty; BID/OFFER/UPDATE_TIME carry the quote.
    assert client._handle_stream_line("1,1||||1.2000|1.2003|1710000000123")
    tick = client._tick_cache["EURUSD"]
    assert tick.bid == pytest.approx(1.2)
    assert tick.ask == pytest.approx(1.2003)
    assert tick.timestamp == pytest.approx(1_710_000_000.123)


def test_ig_invalid_epic_warning_is_throttled(monkeypatch, caplog: pytest.LogCaptureFixture):
    client = _make_client()
    now = {"value": 1_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["value"])

    with caplog.at_level(logging.WARNING):
        client._mark_epic_temporarily_invalid("EU50", "IX.D.STX50.CASH.IP", "stream_subscription:error:21")
        now["value"] = 1_010.0
        client._mark_epic_temporarily_invalid("EU50", "IX.D.STX50.CASH.IP", "stream_subscription:error:21")

    messages = [record.getMessage() for record in caplog.records if "IG epic marked temporarily invalid" in record.getMessage()]
    assert len(messages) == 1


def test_ig_epic_remap_warning_is_throttled(monkeypatch, caplog: pytest.LogCaptureFixture):
    client = _make_client()
    now = {"value": 2_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["value"])
    client._epics["EU50"] = "IX.D.STX50.CASH.IP"

    with caplog.at_level(logging.WARNING):
        client._activate_epic("EU50", "IX.D.EUSTX50.CASH.IP")
        client._epics["EU50"] = "IX.D.STX50.CASH.IP"
        now["value"] = 2_010.0
        client._activate_epic("EU50", "IX.D.EUSTX50.CASH.IP")

    messages = [record.getMessage() for record in caplog.records if "IG epic remapped for EU50" in record.getMessage()]
    assert len(messages) == 1


def test_ig_stream_sdk_start_and_subscribe_path(monkeypatch):
    created_clients, created_subscriptions = _install_fake_lightstreamer_sdk(monkeypatch)
    client = _make_client()
    client._stream_use_sdk = True  # type: ignore[attr-defined]
    client._stream_sdk_available = True  # type: ignore[attr-defined]
    client._stream_sdk_enabled = True  # type: ignore[attr-defined]
    client._stream_endpoint = "https://demo-apd.marketdatasystems.com/lightstreamer"
    client._cst = "token-cst"  # type: ignore[attr-defined]
    client._security_token = "token-sec"  # type: ignore[attr-defined]
    client.account_id = "ABC123"

    client._start_stream_thread_locked()  # type: ignore[attr-defined]

    assert len(created_clients) == 1
    sdk_client = created_clients[0]
    assert getattr(sdk_client, "server_address") == "https://demo-apd.marketdatasystems.com/lightstreamer"
    assert getattr(sdk_client, "adapter_set") == "DEFAULT"
    connection_details = getattr(sdk_client, "connectionDetails")
    assert getattr(connection_details, "user") == "ABC123"
    assert getattr(connection_details, "password") == "CST-token-cst|XST-token-sec"

    assert client._ensure_stream_subscription("EURUSD")  # type: ignore[attr-defined]
    assert len(created_subscriptions) == 1
    subscription = created_subscriptions[0]
    assert getattr(subscription, "mode") == "MERGE"
    assert getattr(subscription, "items") == ["PRICE:ABC123:CS.D.EURUSD.CFD.IP"]
    assert getattr(subscription, "fields") == list(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS)
    assert getattr(subscription, "data_adapter") == "Pricing"
    assert getattr(subscription, "requested_snapshot") == "yes"

    listener = getattr(subscription, "listeners")[0]
    on_subscription = getattr(listener, "onSubscription")
    on_subscription()

    class _FakeUpdate:
        def getValue(self, field_name: str):  # noqa: N802
            values = {
                "BIDPRICE1": "1.2050",
                "ASKPRICE1": "1.2053",
                "TIMESTAMP": "1710000000123",
            }
            return values.get(field_name)

    on_item_update = getattr(listener, "onItemUpdate")
    on_item_update(_FakeUpdate())
    tick = client._tick_cache["EURUSD"]  # type: ignore[attr-defined]
    assert tick.bid == pytest.approx(1.2050)
    assert tick.ask == pytest.approx(1.2053)
    assert tick.timestamp == pytest.approx(1_710_000_000.123)


def test_ig_subscribe_symbol_uses_space_separated_schema(monkeypatch):
    client = _make_client()
    client._stream_session_id = "session-1"
    client._stream_control_url = "https://example/lightstreamer/control.txt"
    client.account_id = "ABC123"

    sent_params: dict[str, object] = {}

    monkeypatch.setattr(client, "_epic_for_symbol", lambda symbol: "CS.D.EURUSD.CFD.IP")

    def fake_send_stream_control(url: str, params: dict[str, object]) -> tuple[bool, str]:
        _ = url
        sent_params.update(params)
        return True, "ok"

    monkeypatch.setattr(client, "_send_stream_control", fake_send_stream_control)

    assert client._subscribe_symbol("EURUSD") is True
    assert sent_params["LS_schema"] == " ".join(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS)
    assert "|" not in str(sent_params["LS_schema"])


def test_ig_subscribe_symbol_fails_over_epic_on_invalid_group(monkeypatch):
    client = _make_client()
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_use_sdk = False  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._stream_control_url = "https://example/lightstreamer/control.txt"
    client.account_id = "ABC123"
    now = {"value": 1_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["value"])

    sent_items: list[str] = []

    def fake_send_stream_control(url: str, params: dict[str, object]) -> tuple[bool, str]:
        _ = url
        item = str(params.get("LS_id") or "")
        sent_items.append(item)
        if item.endswith(":IX.D.FTSE.CASH.IP"):
            return False, "error:21:Invalid group"
        if item.endswith(":IX.D.FTSE.DAILY.IP"):
            return True, "ok"
        raise AssertionError(f"Unexpected stream item: {item}")

    monkeypatch.setattr(client, "_send_stream_control", fake_send_stream_control)

    assert client._subscribe_symbol("UK100") is False
    assert sent_items == ["PRICE:ABC123:IX.D.FTSE.CASH.IP"]
    assert client._epic_for_symbol("UK100") == "IX.D.FTSE.DAILY.IP"
    assert client._stream_symbol_to_table.get("UK100") is None  # type: ignore[attr-defined]
    assert client._is_epic_temporarily_invalid("UK100", "IX.D.FTSE.CASH.IP")
    assert client._stream_subscription_retry_remaining("UK100") == pytest.approx(1.0)
    assert client._stream_rest_fallback_block_remaining("UK100") == pytest.approx(0.0)

    now["value"] = 1_001.1
    assert client._ensure_stream_subscription("UK100") is True
    assert sent_items == [
        "PRICE:ABC123:IX.D.FTSE.CASH.IP",
        "PRICE:ABC123:IX.D.FTSE.DAILY.IP",
    ]
    assert client._stream_symbol_to_table.get("UK100") is not None  # type: ignore[attr-defined]


def test_ig_subscribe_symbol_invalid_group_without_fallback_reports_failure(monkeypatch):
    client = _make_client()
    client._stream_session_id = "session-1"
    client._stream_control_url = "https://example/lightstreamer/control.txt"
    client.account_id = "ABC123"
    sent_items: list[str] = []

    def fake_send_stream_control(url: str, params: dict[str, object]) -> tuple[bool, str]:
        _ = url
        sent_items.append(str(params.get("LS_id") or ""))
        return False, "error:21:Invalid group"

    monkeypatch.setattr(client, "_send_stream_control", fake_send_stream_control)

    assert client._subscribe_symbol("EURUSD") is False
    assert sent_items == ["PRICE:ABC123:CS.D.EURUSD.CFD.IP"]
    assert client._stream_last_error == "subscription_failed:EURUSD:error:21:Invalid group"  # type: ignore[attr-defined]
    assert client._stream_rest_fallback_block_remaining("EURUSD") > 0.0


def test_ig_stream_invalid_group_exhaustion_blocks_rest_fallback(monkeypatch):
    client = _make_client()
    client._epics["GOLD"] = "CS.D.XAUUSD.CASH.IP"  # type: ignore[attr-defined]
    client._epic_candidates["GOLD"] = [  # type: ignore[attr-defined]
        "CS.D.XAUUSD.CASH.IP",
        "CS.D.XAUUSD.TODAY.IP",
    ]
    monkeypatch.setattr(
        client,
        "_epic_attempt_order",
        lambda symbol: ["CS.D.XAUUSD.CASH.IP", "CS.D.XAUUSD.TODAY.IP"],
    )
    client._mark_epic_temporarily_invalid("GOLD", "CS.D.XAUUSD.CASH.IP", reason="test")
    client._mark_epic_temporarily_invalid("GOLD", "CS.D.XAUUSD.TODAY.IP", reason="test")

    next_epic = client._promote_stream_subscription_epic_after_invalid_group(
        "GOLD",
        "CS.D.XAUUSD.CASH.IP",
        reason="stream_subscription:error:21:Invalid group",
    )

    assert next_epic is None
    assert client._stream_rest_fallback_block_remaining("GOLD") > 0.0


def test_ig_stream_sdk_subscription_error_invalid_group_retries_with_fallback(monkeypatch):
    client = _make_client()
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_use_sdk = True  # type: ignore[attr-defined]
    client._stream_sdk_client = object()  # type: ignore[attr-defined]
    client._stream_sdk_pending_subscriptions.add("UK100")  # type: ignore[attr-defined]
    client._stream_sdk_subscriptions["UK100"] = object()  # type: ignore[attr-defined]
    client._stream_sdk_subscription_listeners["UK100"] = object()  # type: ignore[attr-defined]
    now = {"value": 2_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["value"])

    retried_epics: list[str] = []

    def fake_subscribe_symbol_sdk(symbol: str) -> bool:
        retried_epics.append(client._epic_for_symbol(symbol))
        return True

    monkeypatch.setattr(client, "_subscribe_symbol_sdk", fake_subscribe_symbol_sdk)

    client._on_stream_sdk_subscription_error("UK100", 21, "Invalid group")

    assert retried_epics == []
    assert client._epic_for_symbol("UK100") == "IX.D.FTSE.DAILY.IP"
    assert client._is_epic_temporarily_invalid("UK100", "IX.D.FTSE.CASH.IP")
    assert client._stream_subscription_retry_remaining("UK100") == pytest.approx(1.0)

    now["value"] = 2_001.1
    assert client._ensure_stream_subscription("UK100") is True
    assert retried_epics == ["IX.D.FTSE.DAILY.IP"]


def test_ig_stream_health_reports_sdk_missing_when_enabled():
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_sdk_enabled = True  # type: ignore[attr-defined]
    client._stream_sdk_available = False  # type: ignore[attr-defined]
    client._stream_use_sdk = False  # type: ignore[attr-defined]
    client._stream_endpoint = "https://demo-apd.marketdatasystems.com/lightstreamer"

    status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is True
    assert status.connected is False
    assert status.reason == "lightstreamer_sdk_missing_rest_fallback"


def test_ig_get_price_uses_rest_fallback_when_stream_is_not_ready(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return {"snapshot": {"bid": 1.2345, "offer": 1.2347, "updateTimeUTC": "12:00:00"}}, {}

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.2345)
    assert tick.ask == pytest.approx(1.2347)
    assert "EURUSD" in client._tick_cache


def test_ig_get_price_disables_rest_fallback_when_stream_subscription_is_unavailable(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._mark_stream_rest_fallback_block("EURUSD", reason="stream_subscription:error:21:Invalid group")
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: 1_700_000_000.0)

    def fail_rest_call(symbol: str):
        _ = symbol
        raise AssertionError("REST price call must be disabled during stream-subscription cooldown")

    monkeypatch.setattr(client, "_get_price_rest", fail_rest_call)

    with pytest.raises(BrokerError, match="REST fallback disabled"):
        client.get_price("EURUSD")


def test_ig_get_price_updates_stream_hit_counters(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1010,
        ask=1.1012,
        timestamp=time.time(),
    )
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: True)

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.1010)
    assert tick.ask == pytest.approx(1.1012)
    assert client._price_requests_total == 1  # type: ignore[attr-defined]
    assert client._stream_price_hits_total == 1  # type: ignore[attr-defined]
    assert client._rest_fallback_hits_total == 0  # type: ignore[attr-defined]


def test_ig_get_price_prefers_rest_when_stream_quote_is_stagnant(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._stream_stagnant_quote_max_age_sec = 30.0  # type: ignore[attr-defined]

    now = {"ts": 1_700_000_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["ts"])

    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1010,
        ask=1.1012,
        timestamp=now["ts"],
    )
    client._stream_last_quote_change_ts_by_symbol["EURUSD"] = now["ts"] - 120.0  # type: ignore[attr-defined]

    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: True)

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return {"snapshot": {"bid": 1.1020, "offer": 1.1022, "updateTimeUTC": "12:00:00"}}, {}
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.1020)
    assert tick.ask == pytest.approx(1.1022)
    assert client._stream_price_hits_total == 0  # type: ignore[attr-defined]
    assert client._rest_fallback_hits_total == 1  # type: ignore[attr-defined]


def test_ig_get_cached_tick_returns_detached_copy(monkeypatch):
    client = _make_client()
    now = 1_700_000_000.0
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now)

    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1000,
        ask=1.1002,
        timestamp=now - 5.0,
    )

    cached = client._get_cached_tick_locked("EURUSD", max_age_sec=60.0)  # type: ignore[attr-defined]
    assert cached is not None
    cached.timestamp = now + 10_000.0
    cached.bid = 9.9999

    internal = client._tick_cache["EURUSD"]  # type: ignore[attr-defined]
    assert internal.timestamp == pytest.approx(now - 5.0)
    assert internal.bid == pytest.approx(1.1000)


def test_ig_get_price_waits_for_rest_slot_when_stream_quote_is_stagnant(monkeypatch):
    client = IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
        account_id="ABC123",
        rest_market_min_interval_sec=0.6,
    )
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._stream_stagnant_quote_max_age_sec = 30.0  # type: ignore[attr-defined]
    client._stream_rest_fallback_min_interval_sec = 0.0  # type: ignore[attr-defined]

    clock = {"wall": 1_700_000_000.0, "mono": 10_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["wall"])
    monkeypatch.setattr("xtb_bot.ig_client.time.monotonic", lambda: clock["mono"])

    def fake_sleep(sec: float) -> None:
        step = max(0.0, float(sec))
        clock["wall"] += step
        clock["mono"] += step

    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", fake_sleep)

    client._tick_cache["EURUSD"] = PriceTick(  # type: ignore[attr-defined]
        symbol="EURUSD",
        bid=1.1010,
        ask=1.1012,
        timestamp=clock["wall"],
    )
    client._stream_last_quote_change_ts_by_symbol["EURUSD"] = clock["wall"] - 120.0  # type: ignore[attr-defined]
    client._last_market_rest_request_monotonic = clock["mono"]  # type: ignore[attr-defined]
    client._last_market_rest_request_ts = clock["wall"]  # type: ignore[attr-defined]
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: True)

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        requests.append((str(method), str(path)))
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return {"snapshot": {"bid": 1.1020, "offer": 1.1022, "updateTimeUTC": "12:00:00"}}, {}
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.1020)
    assert tick.ask == pytest.approx(1.1022)
    assert requests == [("GET", "/markets/CS.D.EURUSD.CFD.IP")]
    assert client._rest_fallback_hits_total == 1  # type: ignore[attr-defined]


def test_ig_get_price_respects_stream_disabled_and_counts_rest_hits(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = False  # type: ignore[attr-defined]

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return {"snapshot": {"bid": 1.2100, "offer": 1.2102, "updateTimeUTC": "12:00:00"}}, {}

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: (_ for _ in ()).throw(AssertionError))

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.2100)
    assert tick.ask == pytest.approx(1.2102)
    assert client._price_requests_total == 1  # type: ignore[attr-defined]
    assert client._stream_price_hits_total == 0  # type: ignore[attr-defined]
    assert client._rest_fallback_hits_total == 1  # type: ignore[attr-defined]

    status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is True
    assert status.reason == "stream_disabled_rest_only"
    assert status.stream_hit_rate_pct == pytest.approx(0.0)


def test_ig_get_price_falls_back_to_last_traded_when_bid_offer_missing(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return {
            "snapshot": {
                "bid": "-",
                "offer": None,
                "lastTraded": 214.37,
                "updateTimeUTC": "12:00:00",
            }
        }, {}

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)

    tick = client.get_price("AAPL")
    assert tick.bid == pytest.approx(214.37)
    assert tick.ask == pytest.approx(214.37)


def test_ig_get_price_parses_string_bid_offer_formats(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return {
            "snapshot": {
                "bid": "214,37",
                "offer": "214.52",
                "updateTimeUTC": "12:00:00",
            }
        }, {}

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)

    tick = client.get_price("AAPL")
    assert tick.bid == pytest.approx(214.37)
    assert tick.ask == pytest.approx(214.52)


def test_ig_get_price_error_includes_snapshot_diagnostics_when_price_missing(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        return {
            "instrument": {"marketStatus": "TRADEABLE"},
            "snapshot": {"delayTime": 15, "marketStatus": "TRADEABLE"},
        }, {}

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)
    monkeypatch.setattr(
        client,
        "_request_market_details_with_epic_failover",
        lambda symbol: ("UA.D.AAPL.CASH.IP", {"instrument": {"marketStatus": "TRADEABLE"}, "snapshot": {"delayTime": 15, "marketStatus": "TRADEABLE"}}),
    )

    with pytest.raises(BrokerError, match="raw_bid=None raw_offer=None .*snapshot_keys=delayTime,marketStatus"):
        client.get_price("AAPL")


def test_ig_get_price_tries_next_epic_when_first_snapshot_has_no_quote(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = False  # type: ignore[attr-defined]
    requests: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        requests.append(path)
        if method != "GET" or not path.startswith("/markets/"):
            raise AssertionError(f"Unexpected request: {method} {path}")
        if path == "/markets/UA.D.AAPL.CASH.IP":
            return (
                {
                    "snapshot": {
                        "bid": None,
                        "offer": None,
                        "marketStatus": "TRADEABLE",
                    }
                },
                {},
            )
        if path == "/markets/UA.D.AAPL.DAILY.IP":
            return (
                {
                    "snapshot": {
                        "bid": 214.30,
                        "offer": 214.36,
                        "updateTimeUTC": "12:00:00",
                    }
                },
                {},
            )
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)

    tick = client.get_price("AAPL")

    assert tick.bid == pytest.approx(214.30)
    assert tick.ask == pytest.approx(214.36)
    assert client._epic_for_symbol("AAPL") == "UA.D.AAPL.DAILY.IP"
    assert requests[:2] == ["/markets/UA.D.AAPL.CASH.IP", "/markets/UA.D.AAPL.DAILY.IP"]


def test_ig_stream_health_keeps_bot_operational_on_stream_degradation():
    client = _make_client()
    client._connected = True
    client._stream_endpoint = "https://demo-apd.marketdatasystems.com/lightstreamer"
    client._stream_last_error = "stream_eof"

    status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is True
    assert status.connected is False
    assert "rest_fallback" in status.reason


def test_ig_stream_health_ok_when_connected_and_fresh_tick():
    client = _make_client()
    client._connected = True
    client._stream_endpoint = "https://demo-apd.marketdatasystems.com/lightstreamer"
    client._stream_session_id = "session-1"
    client._stream_thread = _AliveThread()  # type: ignore[assignment]
    client._stream_desired_subscriptions.add("EURUSD")
    client._stream_symbol_to_table["EURUSD"] = 1
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.2,
        ask=1.2002,
        timestamp=time.time(),
    )

    status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is True
    assert status.connected is True
    assert status.reason == "ok"


def test_ig_stream_health_marks_quote_stagnation_when_timestamp_is_fresh():
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._stream_thread = _AliveThread()  # type: ignore[assignment]
    client._stream_desired_subscriptions.add("EURUSD")
    client._stream_symbol_to_table["EURUSD"] = 1
    client._stream_stagnant_quote_max_age_sec = 60.0  # type: ignore[attr-defined]

    now = 1_700_000_000.0
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1000,
        ask=1.1002,
        timestamp=now - 1.0,
    )
    client._stream_last_quote_change_ts_by_symbol["EURUSD"] = now - 120.0  # type: ignore[attr-defined]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("xtb_bot.ig_client.time.time", lambda: now)
        status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)

    assert status.healthy is True
    assert status.connected is True
    assert status.reason.startswith("stream_quote_stagnant_rest_fallback:")


def test_ig_stream_health_reports_subscription_pending_when_symbol_is_not_active():
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_session_id = "session-1"
    client._stream_thread = _AliveThread()  # type: ignore[assignment]
    client._stream_desired_subscriptions.add("EURUSD")
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.1000,
        ask=1.1002,
        timestamp=time.time(),
    )

    status = client.get_stream_health_status("EURUSD", max_tick_age_sec=5.0)
    assert status.healthy is True
    assert status.connected is True
    assert status.reason == "stream_subscription_pending_rest_fallback"


def test_request_dispatch_uses_configured_timeout_without_timeout_multiplier(monkeypatch):
    client = _make_client()
    client._request_dispatch_enabled = True  # type: ignore[attr-defined]
    client._request_dispatch_timeout_sec = 12.0  # type: ignore[attr-defined]
    client.timeout_sec = 90.0

    captured: dict[str, float] = {}

    class _FakeDone:
        def wait(self, timeout=None):
            captured["timeout"] = float(timeout or 0.0)
            return False

    class _FakeJob:
        def __init__(self, **kwargs):
            _ = kwargs
            self.done = _FakeDone()
            self.error = None
            self.result = None

    class _FakeQueue:
        def put(self, job, timeout=None):
            captured["job_seen"] = 1.0
            captured["job"] = job
            captured["put_timeout"] = float(timeout or 0.0)

    monkeypatch.setattr(ig_client_module, "_QueuedRequestJob", _FakeJob)
    monkeypatch.setattr(client, "_ensure_request_workers_started", lambda: None)
    client._request_worker_queues["account_non_trading"] = _FakeQueue()  # type: ignore[index]

    with pytest.raises(BrokerError, match="IG request dispatch timeout"):
        client._request("GET", "/positions", version="2", auth=True)

    assert captured["job_seen"] == 1.0
    assert captured["put_timeout"] == pytest.approx(0.5)
    assert captured["timeout"] == pytest.approx(12.0)


def test_request_dispatch_raises_when_worker_queue_is_full(monkeypatch):
    client = _make_client()
    client._request_dispatch_enabled = True  # type: ignore[attr-defined]
    client._request_dispatch_timeout_sec = 12.0  # type: ignore[attr-defined]

    class _FakeQueue:
        def put(self, job, timeout=None):
            _ = (job, timeout)
            raise ig_client_module.Full

    monkeypatch.setattr(client, "_ensure_request_workers_started", lambda: None)
    client._request_worker_queues["account_non_trading"] = _FakeQueue()  # type: ignore[index]

    with pytest.raises(BrokerError, match="IG request dispatch queue full"):
        client._request("GET", "/positions", version="2", auth=True)


def test_ig_connect_accepts_case_insensitive_session_token_headers(monkeypatch):
    client = _make_client()
    client.account_id = None

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "POST" and path == "/session":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "Cst": "token-cst",
                    "X-Security-Token": "token-sec",
                },
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)

    client.connect()

    assert client._connected is True  # type: ignore[attr-defined]
    assert client._cst == "token-cst"  # type: ignore[attr-defined]
    assert client._security_token == "token-sec"  # type: ignore[attr-defined]
    assert client._stream_endpoint == "https://demo-apd.marketdatasystems.com/lightstreamer"  # type: ignore[attr-defined]


def test_ig_auth_headers_can_skip_session_tokens():
    client = _make_client()
    client._cst = "old-cst"  # type: ignore[attr-defined]
    client._security_token = "old-sec"  # type: ignore[attr-defined]

    headers_auth = client._auth_headers(include_session_tokens=True)
    headers_login = client._auth_headers(include_session_tokens=False)

    assert headers_auth.get("CST") == "old-cst"
    assert headers_auth.get("X-SECURITY-TOKEN") == "old-sec"
    assert "CST" not in headers_login
    assert "X-SECURITY-TOKEN" not in headers_login


def test_ig_connect_fallbacks_to_login_v2_when_v3_has_no_session_tokens(monkeypatch):
    client = _make_client()
    requests: list[tuple[str, str, str]] = []

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        requests.append((method, path, version))
        if method == "POST" and path == "/session" and version == "3":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {},
            )
        if method == "POST" and path == "/session" and version == "2":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst-v2",
                    "X-SECURITY-TOKEN": "token-sec-v2",
                },
            )
        raise AssertionError(f"Unexpected request: {method} {path} v={version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)

    client.connect()

    assert ("POST", "/session", "3") in requests
    assert ("POST", "/session", "2") in requests
    assert client._connected is True  # type: ignore[attr-defined]
    assert client._cst == "token-cst-v2"  # type: ignore[attr-defined]
    assert client._security_token == "token-sec-v2"  # type: ignore[attr-defined]


def test_ig_connect_retries_v2_on_invalid_client_security_token(monkeypatch):
    client = _make_client()
    requests: list[tuple[str, str, str]] = []

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        requests.append((method, path, version))
        if method == "POST" and path == "/session" and version == "3":
            raise BrokerError(
                'IG API POST /session failed: 401 Unauthorized {"errorCode":"service.security.authentication.failure-invalid-client-security-token"}'
            )
        if method == "POST" and path == "/session" and version == "2":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst-v2",
                    "X-SECURITY-TOKEN": "token-sec-v2",
                },
            )
        raise AssertionError(f"Unexpected request: {method} {path} v={version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)

    client.connect()

    assert ("POST", "/session", "3") in requests
    assert ("POST", "/session", "2") in requests
    assert client._connected is True  # type: ignore[attr-defined]
    assert client._cst == "token-cst-v2"  # type: ignore[attr-defined]
    assert client._security_token == "token-sec-v2"  # type: ignore[attr-defined]


def test_ig_connect_retries_when_v2_fallback_returns_invalid_client_security_token(monkeypatch):
    client = _make_client()
    client.connect_retry_attempts = 3
    sleeps: list[float] = []
    calls = {"v3": 0, "v2": 0}

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        if method != "POST" or path != "/session":
            raise AssertionError(f"Unexpected request: {method} {path} v={version}")
        assert kwargs.get("auth") is False
        # Login attempts must never carry stale session state.
        assert client._cst is None  # type: ignore[attr-defined]
        assert client._security_token is None  # type: ignore[attr-defined]

        if version == "3":
            calls["v3"] += 1
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {},
            )

        if version == "2":
            calls["v2"] += 1
            if calls["v2"] == 1:
                raise BrokerError(
                    'IG API POST /session failed: 401 Unauthorized {"errorCode":"service.security.authentication.failure-invalid-client-security-token"}'
                )
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst-v2",
                    "X-SECURITY-TOKEN": "token-sec-v2",
                },
            )

        raise AssertionError(f"Unexpected login version: {version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: sleeps.append(float(sec)))

    client.connect()

    assert calls["v3"] == 2
    assert calls["v2"] == 2
    assert len(sleeps) == 1
    assert client._connected is True  # type: ignore[attr-defined]
    assert client._cst == "token-cst-v2"  # type: ignore[attr-defined]
    assert client._security_token == "token-sec-v2"  # type: ignore[attr-defined]


def test_ig_connect_retries_on_allowance_exceeded_and_succeeds(monkeypatch):
    client = _make_client()
    client.connect_retry_attempts = 4
    calls = {"count": 0}
    sleeps: list[float] = []

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        if method == "POST" and path == "/session":
            calls["count"] += 1
            if calls["count"] <= 2:
                raise BrokerError(
                    'IG API POST /session failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
                )
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst",
                    "X-SECURITY-TOKEN": "token-sec",
                },
            )
        raise AssertionError(f"Unexpected request: {method} {path} v={version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: sleeps.append(float(sec)))

    client.connect()

    assert calls["count"] == 3
    assert len(sleeps) == 2
    assert sleeps[0] == pytest.approx(2.0, abs=0.1)
    assert sleeps[1] == pytest.approx(2.0, abs=0.1)
    assert client._connected is True  # type: ignore[attr-defined]
    assert client._cst == "token-cst"  # type: ignore[attr-defined]
    assert client._security_token == "token-sec"  # type: ignore[attr-defined]
    assert client._allowance_last_error is None  # type: ignore[attr-defined]


def test_ig_connect_raises_after_allowance_retries_exhausted(monkeypatch):
    client = _make_client()
    client.connect_retry_attempts = 2
    sleeps: list[float] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "POST" and path == "/session":
            raise BrokerError(
                'IG API POST /session failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: sleeps.append(float(sec)))

    with pytest.raises(BrokerError, match="exceeded-api-key-allowance"):
        client.connect()

    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(2.0, abs=0.1)


def test_ig_connect_handles_null_v3_body_when_v2_login_succeeds(monkeypatch):
    client = _make_client()
    requests: list[tuple[str, str, str]] = []

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        requests.append((method, path, version))
        if method == "POST" and path == "/session" and version == "3":
            return (None, {})
        if method == "POST" and path == "/session" and version == "2":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst-v2",
                    "X-SECURITY-TOKEN": "token-sec-v2",
                },
            )
        raise AssertionError(f"Unexpected request: {method} {path} v={version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)

    client.connect()

    assert ("POST", "/session", "3") in requests
    assert ("POST", "/session", "2") in requests
    assert client._connected is True  # type: ignore[attr-defined]
    assert client.account_id == "ABC123"


def test_ig_connect_does_not_switch_account_when_current_account_is_same(monkeypatch):
    client = _make_client()
    client.account_id = None
    requests: list[tuple[str, str, str]] = []

    def fake_request(method, path, **kwargs):
        version = str(kwargs.get("version", ""))
        requests.append((method, path, version))
        if method == "POST" and path == "/session":
            return (
                {
                    "currentAccountId": "ABC123",
                    "lightstreamerEndpoint": "https://demo-apd.marketdatasystems.com",
                },
                {
                    "CST": "token-cst",
                    "X-SECURITY-TOKEN": "token-sec",
                },
            )
        if method == "PUT" and path == "/session":
            raise AssertionError("PUT /session must not be called when IG_ACCOUNT_ID is not explicitly set")
        raise AssertionError(f"Unexpected request: {method} {path} v={version}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_start_stream_thread_locked", lambda: None)

    client.connect()
    assert client._connected is True  # type: ignore[attr-defined]
    assert ("PUT", "/session", "1") not in requests


def test_ig_get_symbol_spec_handles_nullable_dealing_rules(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {"contractSize": 1.0},
                "dealingRules": {
                    "minDealSize": None,
                    "maxDealSize": None,
                    "minStepDistance": None,
                },
                "snapshot": {"decimalPlacesFactor": 5},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(0.0001)
    assert spec.lot_min == pytest.approx(0.5)
    assert spec.lot_max == pytest.approx(100.0)
    assert spec.lot_step == pytest.approx(0.01)


def test_ig_get_symbol_spec_caps_lot_step_by_lot_min(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {"contractSize": 10.0, "valueOfOnePip": 10.0},
                "dealingRules": {
                    "minDealSize": {"value": 0.001},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 6.0},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("WTI")
    assert spec.lot_min == pytest.approx(0.001)
    assert spec.lot_step == pytest.approx(0.001)
    assert spec.lot_precision >= 3
    assert spec.round_volume(0.313956) > 0.0


def test_normalize_volume_floor_for_spec_ignores_uninitialized_lot_max():
    spec = SymbolSpec(
        symbol="WTI",
        tick_size=0.1,
        tick_value=1.0,
        contract_size=100.0,
        lot_min=0.1,
        lot_max=0.0,
        lot_step=0.1,
        price_precision=1,
        lot_precision=1,
    )

    normalized = _normalize_volume_floor_for_spec(spec, 0.26)
    assert normalized == pytest.approx(0.2)


def test_ig_get_symbol_spec_promotes_non_fx_lot_step_to_lot_min(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {"contractSize": 10.0, "valueOfOnePip": 10.0},
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("WTI")
    assert spec.lot_min == pytest.approx(0.1)
    assert spec.lot_step == pytest.approx(0.1)
    assert spec.metadata.get("lot_step_source") == "promoted_to_lot_min_non_fx"


def test_ig_index_pip_fallback_one_point_can_be_disabled(monkeypatch):
    monkeypatch.setenv("IG_INDEX_PIP_FALLBACK_ONE_POINT", "false")
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {"contractSize": 1.0},
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.1},
                },
                "snapshot": {"decimalPlacesFactor": 2},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("US500")
    assert spec.tick_size == pytest.approx(0.1)
    metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
    assert metadata.get("pip_size_source") != "index_fallback_1_point"


def test_ig_get_symbol_spec_fails_over_epic_on_unavailable(monkeypatch):
    client = _make_client()
    client._connected = True
    paths: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        paths.append(path)
        if method != "GET" or not path.startswith("/markets/"):
            raise AssertionError(f"Unexpected request: {method} {path}")
        if path == "/markets/IX.D.SPTRD.CASH.IP":
            raise BrokerError(
                'IG API GET /markets/IX.D.SPTRD.CASH.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        if path == "/markets/IX.D.SPTRD.DAILY.IP":
            return (
                {
                    "instrument": {"contractSize": 1.0},
                    "dealingRules": {
                        "minDealSize": {"value": 1.0},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 1.0},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("US500")

    assert spec.metadata.get("epic") == "IX.D.SPTRD.DAILY.IP"
    assert client._epic_for_symbol("US500") == "IX.D.SPTRD.DAILY.IP"
    assert paths[:2] == ["/markets/IX.D.SPTRD.CASH.IP", "/markets/IX.D.SPTRD.DAILY.IP"]


def test_ig_get_symbol_spec_supports_gold_alias(monkeypatch):
    client = _make_client()
    client._connected = True
    paths: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        paths.append(path)
        if method != "GET" or path != "/markets/CS.D.GOLD.CFD.IP":
            raise AssertionError(f"Unexpected request: {method} {path}")
        return (
            {
                "instrument": {"contractSize": 100.0},
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 1.0},
                },
                "snapshot": {"decimalPlacesFactor": 2},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("GOLD")
    assert spec.symbol == "GOLD"
    assert spec.metadata.get("epic") == "CS.D.GOLD.CFD.IP"
    assert client._epic_for_symbol("GOLD") == "CS.D.GOLD.CFD.IP"
    assert paths == ["/markets/CS.D.GOLD.CFD.IP"]


def test_ig_get_symbol_spec_auto_discovers_epic_via_search_when_default_unavailable(monkeypatch):
    monkeypatch.setenv("IG_EPIC_SEARCH_ENABLED", "true")
    client = _make_client()
    client._connected = True
    requests: list[tuple[str, str, dict[str, object]]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path, dict(kwargs)))
        if method != "GET":
            raise AssertionError(f"Unexpected request: {method} {path}")
        if path == "/markets/CS.D.GOLD.CFD.IP":
            raise BrokerError(
                'IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        if path == "/markets":
            return (
                {
                    "markets": [
                        {"instrument": {"epic": "CS.D.XAUUSD.CFD.IP"}},
                        {"instrument": {"epic": "CS.D.GOLD.CFD.IP"}},
                    ]
                },
                {},
            )
        if path == "/markets/CS.D.XAUUSD.CFD.IP":
            return (
                {
                    "instrument": {"contractSize": 100.0},
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 1.0},
                    },
                    "snapshot": {"decimalPlacesFactor": 2},
                },
                {},
            )
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CS.D.GOLD.CFD.IP"])

    spec = client.get_symbol_spec("GOLD")
    assert spec.metadata.get("epic") == "CS.D.XAUUSD.CFD.IP"
    assert client._epic_for_symbol("GOLD") == "CS.D.XAUUSD.CFD.IP"
    assert any(path == "/markets" for _, path, _ in requests)


def test_ig_get_symbol_spec_failovers_to_static_gold_alias_when_search_disabled(monkeypatch):
    monkeypatch.setenv("IG_EPIC_SEARCH_ENABLED", "false")
    client = _make_client()
    client._connected = True
    requests: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        requests.append(path)
        if method != "GET":
            raise AssertionError(f"Unexpected request: {method} {path}")
        if path == "/markets/CS.D.GOLD.CFD.IP":
            raise BrokerError(
                'IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        if path == "/markets/CS.D.XAUUSD.CFD.IP":
            return (
                {
                    "instrument": {"contractSize": 100.0},
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 1.0},
                    },
                    "snapshot": {"decimalPlacesFactor": 2},
                },
                {},
            )
        if path == "/markets":
            raise AssertionError("Search endpoint should not be used when static alias failover exists")
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("GOLD")
    assert spec.metadata.get("epic") == "CS.D.XAUUSD.CFD.IP"
    assert client._epic_for_symbol("GOLD") == "CS.D.XAUUSD.CFD.IP"
    assert "/markets" not in requests


def test_ig_activate_epic_keeps_symbol_spec_cache_when_epic_is_unchanged():
    client = _make_client()
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100000.0,
        lot_min=1.0,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
        metadata={"epic": "CS.D.EURUSD.CFD.IP"},
    )
    client._symbol_spec_cache["EURUSD"] = spec  # type: ignore[attr-defined]
    client._epics["EURUSD"] = "CS.D.EURUSD.CFD.IP"  # type: ignore[attr-defined]
    client._stream_symbol_to_table["EURUSD"] = 7  # type: ignore[attr-defined]
    client._stream_table_to_symbol[7] = "EURUSD"  # type: ignore[attr-defined]
    client._stream_table_field_values[7] = [None, None, None]  # type: ignore[attr-defined]

    client._activate_epic("EURUSD", "CS.D.EURUSD.CFD.IP")

    assert client._symbol_spec_cache["EURUSD"] is spec  # type: ignore[attr-defined]
    assert client._stream_symbol_to_table.get("EURUSD") == 7  # type: ignore[attr-defined]
    assert client._stream_table_to_symbol.get(7) == "EURUSD"  # type: ignore[attr-defined]


def test_ig_market_details_short_circuits_repeated_epic_unavailable_with_retry(monkeypatch):
    client = _make_client()
    client._connected = True
    client._epic_search_enabled = False  # type: ignore[attr-defined]
    client.rest_market_min_interval_sec = 0.0

    requests: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        requests.append(path)
        if method == "GET" and path.startswith("/markets/"):
            raise BrokerError(
                f'IG API GET {path} failed: 404 Not Found '
                '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CS.D.GOLD.CFD.IP", "CS.D.XAUUSD.CFD.IP"])

    with pytest.raises(BrokerError, match="instrument.epic.unavailable"):
        client.get_symbol_spec("GOLD")
    assert requests == ["/markets/CS.D.GOLD.CFD.IP", "/markets/CS.D.XAUUSD.CFD.IP"]

    with pytest.raises(BrokerError, match="retry cooldown is active"):
        client.get_symbol_spec("GOLD")
    assert requests == ["/markets/CS.D.GOLD.CFD.IP", "/markets/CS.D.XAUUSD.CFD.IP"]


def test_ig_epic_attempt_order_has_candidates_for_extended_symbols():
    client = _make_client()
    symbols = [
        "GOLD",
        "USDCAD",
        "FR40",
        "EU50",
        "AUS200",
        "BTC",
        "ETH",
        "LTC",
        "SOL",
        "XRP",
        "DOGE",
        "AAPL",
        "MSFT",
    ]
    for symbol in symbols:
        candidates = client._epic_attempt_order(symbol)
        assert candidates, f"Expected epic candidates for {symbol}"


def test_ig_epic_attempt_order_prefers_dogusd_for_doge():
    client = _make_client()
    candidates = client._epic_attempt_order("DOGE")
    assert candidates
    assert candidates[0] == "CS.D.DOGUSD.CFD.IP"
    assert "CS.D.DOGUSD.CFD.IP" in candidates
    assert "CS.D.DOGE.CFD.IP" not in candidates


def test_ig_epic_attempt_order_skips_temporarily_invalid_epic_until_expired(monkeypatch):
    client = _make_client()
    clock = {"now": 1_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: float(clock["now"]))

    client._mark_epic_temporarily_invalid("DE40", "IX.D.DAX.CASH.IP", reason="instrument.invalid")

    candidates_while_blocked = client._epic_attempt_order("DE40")
    assert candidates_while_blocked
    assert "IX.D.DAX.CASH.IP" not in candidates_while_blocked

    clock["now"] = 4_000.0
    candidates_after_expiry = client._epic_attempt_order("DE40")
    assert "IX.D.DAX.CASH.IP" in candidates_after_expiry


def test_ig_epic_attempt_order_can_be_pinned_without_failover(monkeypatch):
    monkeypatch.setenv("IG_EPIC_FAILOVER_ENABLED", "false")
    client = _make_client()
    candidates = client._epic_attempt_order("US500")
    assert candidates == ["IX.D.SPTRD.CASH.IP"]


def test_ig_epic_attempt_order_keeps_active_index_epic_first():
    client = _make_client()
    client._epics["UK100"] = "IX.D.FTSE.DAILY.IP"  # type: ignore[attr-defined]
    client._epic_candidates["UK100"] = [  # type: ignore[attr-defined]
        "IX.D.FTSE.DAILY.IP",
        "IX.D.FTSE.CASH.IP",
        "IX.D.FTSE.IFS.IP",
    ]

    candidates = client._epic_attempt_order("UK100")
    assert candidates
    assert candidates[0] == "IX.D.FTSE.DAILY.IP"


def test_ig_get_symbol_spec_for_epic_activates_requested_variant(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method != "GET" or path != "/markets/IX.D.FTSE.DAILY.IP":
            raise AssertionError(f"Unexpected request: {method} {path}")
        return (
            {
                "instrument": {
                    "epic": "IX.D.FTSE.DAILY.IP",
                    "expiry": "DFB",
                    "currencies": [{"code": "GBP", "isDefault": True, "baseExchangeRate": 1.0}],
                    "lotSize": 1.0,
                    "marginFactor": 5.0,
                    "marginFactorUnit": "PERCENTAGE",
                    "valueOfOnePip": 10.0,
                },
                "snapshot": {
                    "bid": 8000.0,
                    "offer": 8001.0,
                    "decimalPlacesFactor": 1,
                    "scalingFactor": 1,
                },
                "dealingRules": {
                    "minDealSize": {"value": 1.0, "unit": "POINTS"},
                    "maxDealSize": {"value": 100.0, "unit": "POINTS"},
                    "minStepDistance": {"value": 1.0, "unit": "POINTS"},
                    "minNormalStopOrLimitDistance": {"value": 4.0, "unit": "POINTS"},
                },
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec_for_epic("UK100", "IX.D.FTSE.DAILY.IP")

    assert spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
    assert spec.metadata.get("epic_variant") == "daily"
    assert client._epic_for_symbol("UK100") == "IX.D.FTSE.DAILY.IP"


def test_ig_get_symbol_spec_for_epic_force_refresh_bypasses_cached_variant(monkeypatch):
    client = _make_client()
    client._connected = True
    client._symbol_spec_cache["UK100"] = SymbolSpec(
        symbol="UK100",
        tick_size=1.0,
        tick_value=10.0,
        contract_size=10.0,
        lot_min=1.0,
        lot_max=100.0,
        lot_step=1.0,
        price_precision=1,
        lot_precision=1,
        metadata={"broker": "ig", "epic": "IX.D.FTSE.DAILY.IP", "epic_variant": "daily", "lot_min_source": "stale_cached"},
    )

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method != "GET" or path != "/markets/IX.D.FTSE.DAILY.IP":
            raise AssertionError(f"Unexpected request: {method} {path}")
        return (
            {
                "instrument": {
                    "epic": "IX.D.FTSE.DAILY.IP",
                    "expiry": "DFB",
                    "currencies": [{"code": "GBP", "isDefault": True, "baseExchangeRate": 1.0}],
                    "contractSize": 10.0,
                    "marginFactor": 5.0,
                    "marginFactorUnit": "PERCENTAGE",
                    "valueOfOnePip": 10.0,
                },
                "snapshot": {
                    "bid": 8000.0,
                    "offer": 8001.0,
                    "decimalPlacesFactor": 1,
                    "scalingFactor": 1,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.5, "unit": "POINTS"},
                    "maxDealSize": {"value": 100.0, "unit": "POINTS"},
                    "minStepDistance": {"value": 0.5, "unit": "POINTS"},
                    "minNormalStopOrLimitDistance": {"value": 4.0, "unit": "POINTS"},
                },
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec_for_epic("UK100", "IX.D.FTSE.DAILY.IP", force_refresh=True)

    assert spec.lot_min == pytest.approx(0.5)
    assert spec.lot_step == pytest.approx(0.5)
    assert spec.metadata.get("lot_min_source") == "minDealSize"


def test_ig_get_symbol_spec_for_epic_allowance_cooldown_does_not_clone_wrong_variant(monkeypatch):
    client = _make_client()
    client._connected = True
    client._symbol_spec_cache["UK100"] = SymbolSpec(
        symbol="UK100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=1.0,
        lot_max=100.0,
        lot_step=1.0,
        price_precision=1,
        lot_precision=1,
        metadata={"broker": "ig", "epic": "IX.D.FTSE.CASH.IP", "epic_variant": "cash"},
    )
    client._allowance_cooldown_until_ts = time.time() + 30.0

    spec = client.get_symbol_spec_for_epic("UK100", "IX.D.FTSE.DAILY.IP")

    assert spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
    assert spec.metadata.get("epic_variant") == "daily"
    assert spec.lot_min != pytest.approx(1.0)
    assert spec.metadata.get("spec_origin") == "critical_fallback_allowance_epic_override"


def test_ig_get_symbol_spec_candidates_for_entry_returns_all_index_variants(monkeypatch):
    client = _make_client()
    client._connected = True
    monkeypatch.setattr(
        client,
        "_epic_attempt_order",
        lambda symbol: [
            "IX.D.SPTRD.CASH.IP",
            "IX.D.SPTRD.DAILY.IP",
            "IX.D.SPTRD.IFS.IP",
        ],
    )

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method != "GET" or not path.startswith("/markets/"):
            raise AssertionError(f"Unexpected request: {method} {path}")
        epic = path.split("/markets/", 1)[1]
        if epic == "IX.D.SPTRD.CASH.IP":
            point_value, min_deal = 1.0, 1.0
        elif epic == "IX.D.SPTRD.DAILY.IP":
            point_value, min_deal = 50.0, 0.04
        elif epic == "IX.D.SPTRD.IFS.IP":
            point_value, min_deal = 250.0, 0.2
        else:  # pragma: no cover
            raise AssertionError(f"Unexpected epic: {epic}")
        return (
            {
                "instrument": {
                    "epic": epic,
                    "expiry": "DFB",
                    "currencies": [{"code": "USD", "isDefault": True, "baseExchangeRate": 1.0}],
                    "lotSize": min_deal,
                    "contractSize": point_value,
                    "marginFactor": 5.0,
                    "marginFactorUnit": "PERCENTAGE",
                    "valueOfOnePoint": point_value,
                },
                "snapshot": {
                    "bid": 8000.0,
                    "offer": 8001.0,
                    "decimalPlacesFactor": 1,
                    "scalingFactor": 1,
                },
                "dealingRules": {
                    "minDealSize": {"value": min_deal, "unit": "POINTS"},
                    "maxDealSize": {"value": 100.0, "unit": "POINTS"},
                    "minStepDistance": {"value": min_deal, "unit": "POINTS"},
                    "minNormalStopOrLimitDistance": {"value": 4.0, "unit": "POINTS"},
                },
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    candidates = client.get_symbol_spec_candidates_for_entry("US500")

    assert [candidate.metadata.get("epic") for candidate in candidates] == [
        "IX.D.SPTRD.CASH.IP",
        "IX.D.SPTRD.DAILY.IP",
        "IX.D.SPTRD.IFS.IP",
    ]


def test_ig_get_symbol_spec_candidates_for_entry_force_refresh_bypasses_cached_specs(monkeypatch):
    client = _make_client()
    client._connected = True
    client._symbol_spec_cache["US500"] = SymbolSpec(
        symbol="US500",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=1.0,
        lot_max=100.0,
        lot_step=1.0,
        price_precision=1,
        lot_precision=1,
        metadata={"broker": "ig", "epic": "IX.D.SPTRD.CASH.IP", "epic_variant": "cash", "lot_min_source": "stale_cached"},
    )
    monkeypatch.setattr(
        client,
        "_epic_attempt_order",
        lambda symbol: [
            "IX.D.SPTRD.CASH.IP",
            "IX.D.SPTRD.DAILY.IP",
        ],
    )

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method != "GET" or not path.startswith("/markets/"):
            raise AssertionError(f"Unexpected request: {method} {path}")
        epic = path.split("/markets/", 1)[1]
        if epic == "IX.D.SPTRD.CASH.IP":
            point_value, min_deal = 1.0, 0.1
        elif epic == "IX.D.SPTRD.DAILY.IP":
            point_value, min_deal = 50.0, 0.04
        else:  # pragma: no cover
            raise AssertionError(f"Unexpected epic: {epic}")
        return (
            {
                "instrument": {
                    "epic": epic,
                    "expiry": "DFB",
                    "currencies": [{"code": "USD", "isDefault": True, "baseExchangeRate": 1.0}],
                    "lotSize": min_deal,
                    "contractSize": point_value,
                    "marginFactor": 5.0,
                    "marginFactorUnit": "PERCENTAGE",
                    "valueOfOnePoint": point_value,
                },
                "snapshot": {
                    "bid": 8000.0,
                    "offer": 8001.0,
                    "decimalPlacesFactor": 1,
                    "scalingFactor": 1,
                },
                "dealingRules": {
                    "minDealSize": {"value": min_deal, "unit": "POINTS"},
                    "maxDealSize": {"value": 100.0, "unit": "POINTS"},
                    "minStepDistance": {"value": min_deal, "unit": "POINTS"},
                    "minNormalStopOrLimitDistance": {"value": 4.0, "unit": "POINTS"},
                },
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    candidates = client.get_symbol_spec_candidates_for_entry("US500", force_refresh=True)

    assert [candidate.lot_min for candidate in candidates] == pytest.approx([0.1, 0.04])
    assert [candidate.metadata.get("epic") for candidate in candidates] == [
        "IX.D.SPTRD.CASH.IP",
        "IX.D.SPTRD.DAILY.IP",
    ]


def test_ig_get_price_fails_over_epic_on_unavailable(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = False  # type: ignore[attr-defined]
    paths: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        paths.append(path)
        if method != "GET" or not path.startswith("/markets/"):
            raise AssertionError(f"Unexpected request: {method} {path}")
        if path == "/markets/IX.D.FTSE.CASH.IP":
            raise BrokerError(
                'IG API GET /markets/IX.D.FTSE.CASH.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        if path == "/markets/IX.D.FTSE.DAILY.IP":
            return (
                {
                    "snapshot": {
                        "bid": 8150.0,
                        "offer": 8151.2,
                        "updateTimeUTC": "12:00:00",
                    }
                },
                {},
            )
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_ensure_stream_subscription", lambda symbol: False)

    tick = client.get_price("UK100")

    assert tick.bid == pytest.approx(8150.0)
    assert tick.ask == pytest.approx(8151.2)
    assert client._epic_for_symbol("UK100") == "IX.D.FTSE.DAILY.IP"
    assert paths[:2] == ["/markets/IX.D.FTSE.CASH.IP", "/markets/IX.D.FTSE.DAILY.IP"]


def test_ig_market_details_strict_mode_does_not_switch_epic(monkeypatch):
    monkeypatch.setenv("IG_EPIC_FAILOVER_ENABLED", "false")
    client = _make_client()
    client._connected = True
    paths: list[str] = []

    def fake_request(method, path, **kwargs):
        _ = kwargs
        paths.append(path)
        if method == "GET" and path == "/markets/IX.D.FTSE.CASH.IP":
            raise BrokerError(
                'IG API GET /markets/IX.D.FTSE.CASH.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(BrokerError, match="instrument.epic.unavailable"):
        client.get_symbol_spec("UK100")
    assert paths == ["/markets/IX.D.FTSE.CASH.IP"]


def test_ig_get_symbol_spec_uses_one_pip_and_scaling_factor(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "onePipMeans": 0.0001,
                    "valueOfOnePip": 10.0,
                    "scalingFactor": 10000,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.01},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.one_pip_means == pytest.approx(0.0001)
    assert spec.tick_value == pytest.approx(10.0)
    assert spec.contract_size == pytest.approx(100000.0)


def test_ig_get_symbol_spec_uses_scaling_factor_for_scaled_fx_quote(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "valueOfOnePip": 10.0,
                    "scalingFactor": 10000,
                    # onePipMeans is missing for this market variant, so scalingFactor must drive the quote scale.
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.5},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.one_pip_means is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(10_000.0)


def test_ig_get_symbol_spec_uses_pip_position_for_scaled_fx_quote_when_scaling_factor_missing(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "valueOfOnePip": 10.0,
                    "pipPosition": 4,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.5},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.one_pip_means is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(10_000.0)


def test_ig_get_symbol_spec_does_not_guess_scaled_fx_quote_without_broker_scale_fields(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "valueOfOnePip": 10.0,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.5},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.one_pip_means is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(1.0)


def test_ig_get_symbol_spec_infers_margin_price_scale_for_scaled_wti_quote(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100.0,
                    "onePipMeans": 0.01,
                    "valueOfOnePip": 100.0,
                    "scalingFactor": 10.0,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.1},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("WTI")
    assert spec.tick_size == pytest.approx(0.1)
    assert spec.one_pip_means == pytest.approx(0.01)
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(10.0)


def test_ig_get_symbol_spec_supports_value_per_pip_alias(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "onePipMeans": 0.0001,
                    "valuePerPip": 9.5,
                    "scalingFactor": 10000,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.01},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("EURUSD")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.tick_value == pytest.approx(9.5)
    assert spec.metadata.get("tick_value_source") == "valuePerPip"
    assert spec.metadata.get("pip_size_source") == "onePipMeans_scaled_by_scalingFactor"


def test_ig_get_symbol_spec_uses_one_point_pip_fallback_for_us500(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 250.0,
                    "valueOfOnePip": 250.0,
                    # onePipMeans intentionally absent -> fallback path.
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.01},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 2},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("US500")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.metadata.get("pip_size_source") == "index_fallback_1_point"
    assert spec.tick_value == pytest.approx(250.0)
    assert str(spec.metadata.get("tick_value_source") or "") == "valueOfOnePip"
    assert spec.metadata.get("tick_value_rescale_ratio") is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(1.0)


def test_ig_get_symbol_spec_keeps_broker_non_fx_contract_value_when_one_pip_means_is_missing(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100.0,
                    "valueOfOnePip": 100.0,
                    # onePipMeans intentionally absent: fallback pip path.
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 1.0},
                },
                "snapshot": {"decimalPlacesFactor": 2},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("GOLD")
    assert spec.tick_size == pytest.approx(0.1)
    assert spec.tick_value == pytest.approx(100.0)
    assert str(spec.metadata.get("tick_value_source") or "") == "valueOfOnePip"
    assert spec.metadata.get("tick_value_rescale_ratio") is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(1.0)


def test_ig_get_symbol_spec_keeps_broker_index_pip_value_when_one_pip_means_is_missing(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 10.0,
                    "valueOfOnePip": 10.0,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.2},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 1.0},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("US30")
    assert spec.tick_size == pytest.approx(1.0)
    assert spec.tick_value == pytest.approx(10.0)
    assert str(spec.metadata.get("tick_value_source") or "") == "valueOfOnePip"
    assert spec.metadata.get("tick_value_rescale_ratio") is None
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(1.0)


def test_ig_get_symbol_spec_keeps_point_value_for_non_fx_without_contract_division(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100.0,
                    "onePipMeans": 0.01,
                    "valueOfOnePoint": 1.0,
                    "scalingFactor": 10.0,
                },
                "dealingRules": {
                    "minDealSize": {"value": 0.1},
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.1},
                },
                "snapshot": {"decimalPlacesFactor": 1},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("WTI")
    assert spec.tick_size == pytest.approx(0.1)
    assert spec.tick_value == pytest.approx(1.0)
    assert spec.metadata.get("tick_value_kind") == "point"
    assert spec.metadata.get("tick_value_rescale_ratio") is None


def test_ig_parse_ts_supports_epoch_milliseconds():
    now_ts = 1_700_000_000.0
    ts_ms = 1_700_000_123_456
    parsed = IgApiClient._parse_ts({"updateTimeUTC": ts_ms}, now_ts)
    assert parsed == pytest.approx(1_700_000_123.456)


def test_ig_parse_ts_supports_numeric_hhmmss():
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    now_ts = now_dt.timestamp()
    parsed = IgApiClient._parse_ts({"updateTimeUTC": 165458}, now_ts)
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert parsed == pytest.approx(expected)


def test_ig_parse_ts_supports_numeric_hhmm():
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    now_ts = now_dt.timestamp()
    parsed = IgApiClient._parse_ts({"updateTimeUTC": 1338}, now_ts)
    expected = datetime(2026, 3, 16, 13, 38, 0, tzinfo=timezone.utc).timestamp()
    assert parsed == pytest.approx(expected)


def test_ig_parse_ts_supports_hhmm_text():
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    now_ts = now_dt.timestamp()
    parsed = IgApiClient._parse_ts({"updateTimeUTC": "13:38"}, now_ts)
    expected = datetime(2026, 3, 16, 13, 38, 0, tzinfo=timezone.utc).timestamp()
    assert parsed == pytest.approx(expected)


def test_ig_parse_ts_rejects_far_future_time_of_day():
    now_dt = datetime(2026, 3, 19, 8, 30, 0, tzinfo=timezone.utc)
    now_ts = now_dt.timestamp()
    parsed = IgApiClient._parse_ts({"updateTimeUTC": "13:38:00"}, now_ts)
    expected = datetime(2026, 3, 18, 13, 38, 0, tzinfo=timezone.utc).timestamp()
    assert parsed == pytest.approx(expected)


def test_ig_parse_ts_supports_milliseconds_of_day():
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    now_ts = now_dt.timestamp()
    day_ms = ((16 * 60 * 60) + (54 * 60) + 58) * 1000
    parsed = IgApiClient._parse_ts({"updateTimeUTC": day_ms}, now_ts)
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert parsed == pytest.approx(expected)


def test_ig_confirm_rejection_reason_uses_extended_fields():
    assert (
        IgApiClient._confirm_rejection_reason({"statusReason": "attached_order_level_error"})
        == "ATTACHED_ORDER_LEVEL_ERROR"
    )
    assert (
        IgApiClient._confirm_rejection_reason({"rejectionReason": "instrument_not_tradeable_in_this_currency"})
        == "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY"
    )
    assert (
        IgApiClient._confirm_rejection_reason({"errorCode": "error.service.otc.invalid_size"})
        == "ORDER_SIZE_INCREMENT_ERROR"
    )
    assert (
        IgApiClient._confirm_rejection_reason({"errorCode": "error.service.otc.invalid.size"})
        == "ORDER_SIZE_INCREMENT_ERROR"
    )
    assert (
        IgApiClient._confirm_rejection_reason({"message": "Rejected: Instrument not Valid"})
        == "INSTRUMENT_NOT_VALID"
    )
    assert (
        IgApiClient._confirm_rejection_reason(
            {
                "reason": "UNKNOWN",
                "statusReason": "Failed to retrieve price information for this currency",
            }
        )
        == "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY"
    )
    assert (
        IgApiClient._confirm_rejection_reason(
            {
                "reason": "UNKNOWN",
                "statusReason": "Order size must be traded in set increments.",
            }
        )
        == "ORDER_SIZE_INCREMENT_ERROR"
    )
    assert (
        IgApiClient._confirm_rejection_reason({"reason": "SIZE_INCREMENT"})
        == "ORDER_SIZE_INCREMENT_ERROR"
    )


def test_is_fx_pair_symbol_rejects_same_base_quote():
    assert _is_fx_pair_symbol("EURUSD") is True
    assert _is_fx_pair_symbol("USDUSD") is False


def test_is_fx_pair_symbol_rejects_crypto_pairs():
    assert _is_fx_pair_symbol("BTCUSD") is False
    assert _is_fx_pair_symbol("ETHUSD") is False


def test_plausible_epic_accepts_crypto_pair_aliases():
    assert ig_client_module._is_plausible_epic_for_symbol("BTCUSD", "CS.D.BITCOIN.CFD.IP") is True
    assert ig_client_module._is_non_fx_cfd_symbol("BTCUSD", "CS.D.BITCOIN.CFD.IP") is True


def test_epic_attempt_order_skips_blank_cached_candidates_when_failover_disabled():
    client = _make_client()
    client._epic_failover_enabled = False  # type: ignore[attr-defined]
    client._epic_candidates["BRENT"] = ["", "   "]  # type: ignore[attr-defined]
    client._epics.pop("BRENT", None)  # type: ignore[attr-defined]

    attempts = client._epic_attempt_order("BRENT")

    assert attempts
    assert attempts[0]


def test_ig_form_body_uses_form_urlencoding_plus_for_spaces():
    encoded = IgApiClient._form_body({"LS_cid": "a b", "LS_user": "ABC+123"}).decode("utf-8")
    assert "LS_cid=a+b" in encoded
    assert "LS_user=ABC%2B123" in encoded


def test_ig_market_data_wait_uses_monotonic_clock(monkeypatch):
    client = _make_client()
    client.rest_market_min_interval_sec = 2.0
    client._last_market_rest_request_monotonic = 100.0  # type: ignore[attr-defined]
    monkeypatch.setattr("xtb_bot.ig_client.time.monotonic", lambda: 101.25)

    remaining = client.get_market_data_wait_remaining_sec()
    assert remaining == pytest.approx(0.75)


def test_ig_brent_has_static_epic_fallback_candidates():
    client = _make_client()
    attempts = client._epic_attempt_order("BRENT")
    assert "CC.D.LCO.USS.IP" in attempts
    assert "CC.D.LCO.CFD.IP" in attempts
    assert "CC.D.LCO.UNC.IP" in attempts


def test_ig_get_price_uses_fresh_cache_during_allowance_cooldown(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = False  # type: ignore[attr-defined]

    calls = {"count": 0}

    def fake_request(method, path, **kwargs):
        _ = (kwargs,)
        if method == "GET" and path.startswith("/markets/"):
            calls["count"] += 1
            raise BrokerError(
                'IG API GET /markets/CS.D.EURUSD.CFD.IP failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-allowance"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.2,
        ask=1.2002,
        timestamp=time.time(),
    )
    monkeypatch.setattr(client, "_request", fake_request)

    first = client.get_price("EURUSD")
    second = client.get_price("EURUSD")

    assert first.bid == pytest.approx(1.2)
    assert second.ask == pytest.approx(1.2002)
    assert calls["count"] == 1


def test_ig_get_price_uses_cache_when_rest_rate_limit_slot_is_busy(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = False  # type: ignore[attr-defined]
    client.rest_market_min_interval_sec = 5.0

    now = {"ts": 1_700_000_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["ts"])

    client._last_market_rest_request_ts = now["ts"] - 1.0  # type: ignore[attr-defined]
    client._tick_cache["EURUSD"] = PriceTick(
        symbol="EURUSD",
        bid=1.2050,
        ask=1.2052,
        timestamp=now["ts"],
    )

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        raise AssertionError("REST request must be skipped while cached price is fresh")

    monkeypatch.setattr(client, "_request", fake_request)

    tick = client.get_price("EURUSD")
    assert tick.bid == pytest.approx(1.2050)
    assert tick.ask == pytest.approx(1.2052)


def test_ig_account_snapshot_cache_reduces_duplicate_requests(monkeypatch):
    client = _make_client()
    client._connected = True
    calls = {"count": 0}

    def fake_request(method, path, **kwargs):
        _ = (kwargs,)
        if method == "GET" and path == "/accounts":
            calls["count"] += 1
            return (
                {
                    "accounts": [
                        {
                            "accountId": "ABC123",
                            "preferred": True,
                            "balance": {
                                "balance": 10000.0,
                                "profitLoss": 50.0,
                                "available": 9800.0,
                            },
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    snap1 = client.get_account_snapshot()
    snap2 = client.get_account_snapshot()

    assert calls["count"] == 1
    assert snap1.balance == pytest.approx(10000.0)
    assert snap2.equity == pytest.approx(10050.0)


def test_ig_allowance_error_detector_supports_api_key_variant():
    client = _make_client()
    assert client._is_allowance_error_text("error.public-api.exceeded-account-allowance")
    assert client._is_allowance_error_text("error.public-api.exceeded-api-key-allowance")
    assert client._is_allowance_error_text("error.public-api.exceeded-account-trading-allowance")


def test_ig_epic_search_fallback_can_be_disabled(monkeypatch):
    monkeypatch.setenv("IG_EPIC_SEARCH_ENABLED", "false")
    client = _make_client()
    assert client._extend_epic_candidates_from_search("EURUSD") == []


def test_ig_market_details_search_fallback_works_without_default_epic_candidates(monkeypatch):
    client = _make_client()

    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: [])
    monkeypatch.setattr(client, "_wait_for_market_rest_slot", lambda: None)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["IX.D.MIB.CASH.IP"])

    def fake_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/markets/IX.D.MIB.CASH.IP"
        return ({"instrument": {"marketId": "IT40"}}, {})

    monkeypatch.setattr(client, "_request", fake_request)

    epic, body = client._request_market_details_with_epic_failover("IT40")
    assert epic == "IX.D.MIB.CASH.IP"
    assert body["instrument"]["marketId"] == "IT40"
    assert client._epic_for_symbol("IT40") == "IX.D.MIB.CASH.IP"


def test_ig_market_details_search_fallback_works_for_us2000_without_default_epic_candidates(monkeypatch):
    client = _make_client()

    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: [])
    monkeypatch.setattr(client, "_wait_for_market_rest_slot", lambda: None)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["IX.D.RUSSELL2K.CASH.IP"])

    def fake_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/markets/IX.D.RUSSELL2K.CASH.IP"
        return ({"instrument": {"marketId": "US2000"}}, {})

    monkeypatch.setattr(client, "_request", fake_request)

    epic, body = client._request_market_details_with_epic_failover("US2000")
    assert epic == "IX.D.RUSSELL2K.CASH.IP"
    assert body["instrument"]["marketId"] == "US2000"
    assert client._epic_for_symbol("US2000") == "IX.D.RUSSELL2K.CASH.IP"


def test_ig_market_details_search_fallback_works_for_topix_without_default_epic_candidates(monkeypatch):
    client = _make_client()

    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: [])
    monkeypatch.setattr(client, "_wait_for_market_rest_slot", lambda: None)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["IX.D.TOPIX.CASH.IP"])

    def fake_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/markets/IX.D.TOPIX.CASH.IP"
        return ({"instrument": {"marketId": "TOPIX"}}, {})

    monkeypatch.setattr(client, "_request", fake_request)

    epic, body = client._request_market_details_with_epic_failover("TOPIX")
    assert epic == "IX.D.TOPIX.CASH.IP"
    assert body["instrument"]["marketId"] == "TOPIX"
    assert client._epic_for_symbol("TOPIX") == "IX.D.TOPIX.CASH.IP"


def test_ig_market_details_search_fallback_works_for_carbon_without_default_epic_candidates(monkeypatch):
    client = _make_client()

    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: [])
    monkeypatch.setattr(client, "_wait_for_market_rest_slot", lambda: None)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["CC.D.EUA.USS.IP"])

    def fake_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/markets/CC.D.EUA.USS.IP"
        return ({"instrument": {"marketId": "CARBON"}}, {})

    monkeypatch.setattr(client, "_request", fake_request)

    epic, body = client._request_market_details_with_epic_failover("CARBON")
    assert epic == "CC.D.EUA.USS.IP"
    assert body["instrument"]["marketId"] == "CARBON"
    assert client._epic_for_symbol("CARBON") == "CC.D.EUA.USS.IP"


def test_ig_open_position_sets_allowance_cooldown_on_trading_allowance_error(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    calls = {"post": 0, "get_markets": 0}

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            calls["get_markets"] += 1
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.5},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 5},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            calls["post"] += 1
            raise BrokerError(
                'IG API POST /positions/otc failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-trading-allowance"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(BrokerError, match="exceeded-account-trading-allowance"):
        client.open_position(
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.8,
            stop_loss=1.1490,
            take_profit=1.1520,
            comment="XTBBOT:TEST:ABC:EURUSD",
        )

    assert calls["post"] == 1
    assert client._allowance_cooldown_remaining() >= 29.0  # type: ignore[attr-defined]

    get_markets_before_retry = calls["get_markets"]
    with pytest.raises(BrokerError, match="allowance cooldown is active"):
        client.open_position(
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.8,
            stop_loss=1.1490,
            take_profit=1.1520,
            comment="XTBBOT:TEST:ABC:EURUSD",
        )

    # Retry should be blocked by cooldown before an extra trade POST.
    assert calls["post"] == 1
    assert calls["get_markets"] == get_markets_before_retry


def test_ig_request_defers_non_critical_rest_during_critical_trade(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]

    release_guard = threading.Event()
    guard_ready = threading.Event()

    def _hold_critical_operation():
        with client._critical_trade_operation("open_position:EURUSD"):  # type: ignore[attr-defined]
            guard_ready.set()
            release_guard.wait(timeout=2.0)

    holder = threading.Thread(target=_hold_critical_operation)
    holder.start()
    try:
        assert guard_ready.wait(timeout=1.0)
        with pytest.raises(BrokerError, match="critical_trade_operation_active"):
            client._request("GET", "/markets/CS.D.EURUSD.CFD.IP", version="3", auth=True)  # type: ignore[attr-defined]
    finally:
        release_guard.set()
        holder.join(timeout=2.0)


def test_ig_request_allows_non_critical_rest_from_owner_inside_critical_trade_with_dispatch(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    client._request_dispatch_enabled = True  # type: ignore[attr-defined]
    calls: list[tuple[int, str, str]] = []

    class _FakeResponse:
        def __init__(self, raw: str) -> None:
            self._raw = raw.encode("utf-8")
            self.length = len(self._raw)
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        calls.append((threading.get_ident(), req.get_method(), req.full_url))
        return _FakeResponse('{"ok": true}')

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    with client._critical_trade_operation("open_position:EURUSD"):  # type: ignore[attr-defined]
        body, _ = client._request("GET", "/markets/CS.D.EURUSD.CFD.IP", version="3", auth=True)  # type: ignore[attr-defined]
        assert body.get("ok") is True

    assert calls


def test_ig_request_reauths_and_retries_on_client_token_invalid(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-old"  # type: ignore[attr-defined]
    client._security_token = "sec-old"  # type: ignore[attr-defined]
    counters = {"urlopen": 0, "connect": 0}

    class _FakeResponse:
        def __init__(self, raw: str) -> None:
            self._raw = raw.encode("utf-8")
            self.length = len(self._raw)
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        counters["urlopen"] += 1
        if counters["urlopen"] == 1:
            raise urlerror.HTTPError(
                req.full_url,
                401,
                "Unauthorized",
                hdrs={},
                fp=io.BytesIO(b'{"errorCode":"error.security.client-token-invalid"}'),
            )
        return _FakeResponse('{"positions": []}')

    def fake_connect():
        counters["connect"] += 1
        client._connected = True  # type: ignore[attr-defined]
        client._cst = "cst-new"  # type: ignore[attr-defined]
        client._security_token = "sec-new"  # type: ignore[attr-defined]

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)
    monkeypatch.setattr(client, "connect", fake_connect)

    body, _ = client._request("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]
    assert isinstance(body, dict)
    assert body.get("positions") == []
    assert counters["connect"] == 1
    assert counters["urlopen"] == 2


def test_ig_request_runtime_reconnects_when_disconnected(monkeypatch):
    client = _make_client()
    client._connected = False
    client._cst = None  # type: ignore[attr-defined]
    client._security_token = None  # type: ignore[attr-defined]
    client._runtime_reconnect_min_interval_sec = 0.0  # type: ignore[attr-defined]
    counters = {"connect": 0, "urlopen": 0}

    class _FakeResponse:
        def __init__(self, raw: str) -> None:
            self._raw = raw.encode("utf-8")
            self.length = len(self._raw)
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_connect():
        counters["connect"] += 1
        client._connected = True  # type: ignore[attr-defined]
        client._cst = "cst-new"  # type: ignore[attr-defined]
        client._security_token = "sec-new"  # type: ignore[attr-defined]

    def fake_urlopen(req, timeout):
        _ = timeout
        counters["urlopen"] += 1
        assert req.get_method() == "GET"
        return _FakeResponse('{"positions": []}')

    monkeypatch.setattr(client, "connect", fake_connect)
    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    body, _ = client._request("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]
    assert body.get("positions") == []
    assert counters["connect"] == 1
    assert counters["urlopen"] == 1


def test_ig_request_does_not_reauth_for_session_endpoint(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-old"  # type: ignore[attr-defined]
    client._security_token = "sec-old"  # type: ignore[attr-defined]
    counters = {"urlopen": 0, "connect": 0}

    def fake_urlopen(req, timeout):
        _ = timeout
        counters["urlopen"] += 1
        raise urlerror.HTTPError(
            req.full_url,
            401,
            "Unauthorized",
            hdrs={},
            fp=io.BytesIO(b'{"errorCode":"error.security.client-token-invalid"}'),
        )

    def fake_connect():
        counters["connect"] += 1

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)
    monkeypatch.setattr(client, "connect", fake_connect)

    with pytest.raises(BrokerError, match="client-token-invalid"):
        client._request("GET", "/session", version="1", auth=True)  # type: ignore[attr-defined]
    assert counters["connect"] == 0
    assert counters["urlopen"] == 1


def test_ig_request_does_not_retry_on_http_429_allowance(monkeypatch):
    client = _make_client()
    client._connected = True
    counters = {"urlopen": 0}

    def fake_urlopen(req, timeout):
        _ = timeout
        counters["urlopen"] += 1
        raise urlerror.HTTPError(
            req.full_url,
            429,
            "Too Many Requests",
            hdrs={},
            fp=io.BytesIO(b'{"errorCode":"error.public-api.exceeded-api-key-allowance"}'),
        )

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    with pytest.raises(BrokerError, match="exceeded-api-key-allowance"):
        client._request("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]

    assert counters["urlopen"] == 1
    assert client._allowance_cooldown_remaining() > 0.0  # type: ignore[attr-defined]


def test_ig_request_auth_refresh_attempts_single_connect_cycle(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-old"  # type: ignore[attr-defined]
    client._security_token = "sec-old"  # type: ignore[attr-defined]
    counters = {"urlopen": 0, "connect": 0}

    def fake_urlopen(req, timeout):
        _ = timeout
        counters["urlopen"] += 1
        raise urlerror.HTTPError(
            req.full_url,
            401,
            "Unauthorized",
            hdrs={},
            fp=io.BytesIO(b'{"errorCode":"error.security.client-token-invalid"}'),
        )

    def fake_connect():
        counters["connect"] += 1
        raise BrokerError("refresh failed")

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)
    monkeypatch.setattr(client, "connect", fake_connect)

    with pytest.raises(BrokerError, match="client-token-invalid"):
        client._request("GET", "/positions", version="2", auth=True)  # type: ignore[attr-defined]

    assert counters["connect"] == 1
    assert counters["urlopen"] == 1


def test_ig_stream_loop_does_not_preemptively_refresh_auth_on_reconnect(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_stop_event.clear()  # type: ignore[attr-defined]
    client._stream_reconnect_attempts = 1  # type: ignore[attr-defined]
    client._stream_next_retry_at = None  # type: ignore[attr-defined]

    def fail_if_called(**kwargs):
        _ = kwargs
        raise AssertionError("pre-reconnect auth refresh must not run")

    def fake_open_stream_session():
        client._stream_stop_event.set()  # type: ignore[attr-defined]
        raise BrokerError("stream open failed")

    monkeypatch.setattr(client, "_maybe_refresh_auth_session_after_failure", fail_if_called)
    monkeypatch.setattr(client, "_open_stream_session", fake_open_stream_session)

    client._stream_loop()


def test_ig_connectivity_status_treats_critical_trade_deferred_as_healthy(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]

    def fake_request(*args, **kwargs):
        _ = (args, kwargs)
        raise BrokerError(
            "IG non-critical REST request deferred: critical_trade_operation_active method=GET path=/session (1.0s remaining)"
        )

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    status = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    assert status.healthy is True
    assert status.reason == "critical_trade_operation_active"
    assert status.pong_ok is True


def test_ig_connectivity_status_uses_shared_cache_within_ttl(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    client._connectivity_status_cache_ttl_sec = 15.0  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    calls = {"n": 0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_request(method, path, **kwargs):
        _ = (kwargs,)
        calls["n"] += 1
        assert method == "GET"
        assert path == "/session"
        return ({}, {})

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    first = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    second = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    clock["ts"] += 16.0
    third = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)

    assert first.healthy is True
    assert second.healthy is True
    assert third.healthy is True
    assert calls["n"] == 2


def test_ig_connectivity_status_caches_unhealthy_state_briefly(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    client._connectivity_status_cache_ttl_sec = 15.0  # type: ignore[attr-defined]
    client._connectivity_status_unhealthy_cache_ttl_sec = 3.0  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    calls = {"n": 0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        calls["n"] += 1
        raise BrokerError("IG API GET /session failed: 500 Internal Server Error")

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    first = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    second = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    clock["ts"] += 4.0
    third = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)

    assert first.healthy is False
    assert second.healthy is False
    assert third.healthy is False
    assert calls["n"] == 2


def test_ig_connectivity_status_caches_dispatch_timeout_longer_to_reduce_probe_storm(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    client._connectivity_status_cache_ttl_sec = 15.0  # type: ignore[attr-defined]
    client._connectivity_status_unhealthy_cache_ttl_sec = 2.0  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    calls = {"n": 0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        calls["n"] += 1
        raise BrokerError(
            "IG request dispatch timeout (worker=account_non_trading, method=GET, path=/session)"
        )

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    first = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    clock["ts"] += 5.0
    second = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    clock["ts"] += 11.0
    third = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)

    assert first.healthy is False
    assert second.healthy is False
    assert third.healthy is False
    assert calls["n"] == 2


def test_ig_connectivity_status_starts_allowance_cooldown_and_reuses_it(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    calls = {"n": 0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        calls["n"] += 1
        raise BrokerError(
            'IG API GET /session failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
        )

    monkeypatch.setattr(client, "_request", fake_request)  # type: ignore[assignment]

    first = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)
    clock["ts"] += 5.0
    second = client.get_connectivity_status(max_latency_ms=500.0, pong_timeout_sec=2.0)

    assert first.healthy is False
    assert second.healthy is False
    assert "allowance cooldown is active" in str(first.reason)
    assert "allowance cooldown is active" in str(second.reason)
    assert client.get_public_api_backoff_remaining_sec() == pytest.approx(25.0)
    assert calls["n"] == 1


def test_ig_request_allows_trade_critical_post_during_parallel_critical_trade(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    calls: list[tuple[str, str]] = []

    class _FakeResponse:
        def __init__(self) -> None:
            self.length = 0
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return b""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        calls.append((req.get_method(), req.full_url))
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    release_guard = threading.Event()
    guard_ready = threading.Event()

    def _hold_critical_operation():
        with client._critical_trade_operation("close_position:EURUSD"):  # type: ignore[attr-defined]
            guard_ready.set()
            release_guard.wait(timeout=2.0)

    holder = threading.Thread(target=_hold_critical_operation)
    holder.start()
    try:
        assert guard_ready.wait(timeout=1.0)
        body, _ = client._request(  # type: ignore[attr-defined]
            "POST",
            "/positions/otc",
            payload={"dealId": "DIAAAATEST"},
            version="2",
            auth=True,
        )
        assert body == {}
        assert calls
        assert calls[0][0] == "POST"
    finally:
        release_guard.set()
        holder.join(timeout=2.0)


def test_ig_request_allows_trade_critical_delete_during_parallel_critical_trade(monkeypatch):
    client = _make_client()
    client._connected = True
    client._cst = "cst-token"  # type: ignore[attr-defined]
    client._security_token = "sec-token"  # type: ignore[attr-defined]
    calls: list[tuple[str, str]] = []

    class _FakeResponse:
        def __init__(self) -> None:
            self.length = 0
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return b""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        calls.append((req.get_method(), req.full_url))
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    release_guard = threading.Event()
    guard_ready = threading.Event()

    def _hold_critical_operation():
        with client._critical_trade_operation("close_position:EURUSD"):  # type: ignore[attr-defined]
            guard_ready.set()
            release_guard.wait(timeout=2.0)

    holder = threading.Thread(target=_hold_critical_operation)
    holder.start()
    try:
        assert guard_ready.wait(timeout=1.0)
        body, _ = client._request(  # type: ignore[attr-defined]
            "DELETE",
            "/positions/otc",
            payload={"dealId": "DIAAAATEST", "direction": "SELL", "size": 0.5},
            version="1",
            auth=True,
        )
        assert body == {}
        assert calls
        assert calls[0][0] == "DELETE"
    finally:
        release_guard.set()
        holder.join(timeout=2.0)


def test_ig_allowance_cooldown_does_not_escalate_while_active(monkeypatch):
    client = _make_client()
    current = {"ts": 1_700_000_000.0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: current["ts"])

    first = client._start_allowance_cooldown("allowance-1")
    second = client._start_allowance_cooldown("allowance-2")
    assert first == pytest.approx(2.0)
    assert second == pytest.approx(2.0)

    current["ts"] += 2.1
    third = client._start_allowance_cooldown("allowance-3")
    assert third == pytest.approx(4.0)


def test_ig_close_position_stores_close_sync_from_confirm(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        headers = dict(kwargs.get("extra_headers") or {})
        query = dict(kwargs.get("query") or {})
        if method in {"POST", "DELETE"} and path == "/positions/otc":
            if method == "POST":
                assert headers.get("_method") == "DELETE"
                assert query.get("_method") == "DELETE"
            return {"dealReference": "ref-close-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSE001",
                    "level": 1.34116,
                    "profit": -71.41,
                    "profitCurrency": "EUR",
                    "reason": "STOP_LOSS",
                    "date": "2026-03-17T08:56:31",
                },
                {},
            )
        if method == "GET" and path == "/positions/DIAAAAWUKUH89AQ":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWUKUH89AQ failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAAWUKUH89AQ",
        symbol="GBPUSD",
        side=Side.BUY,
        volume=0.55,
        open_price=1.34266,
        stop_loss=1.34116,
        take_profit=1.34566,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position)

    sync = client.get_position_close_sync(position.position_id)
    assert isinstance(sync, dict)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.34116)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(-71.41)
    assert str(sync.get("pnl_currency") or "") == "EUR"
    assert str(sync.get("close_deal_id") or "") == "DIAAAACLOSE001"
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 17, 8, 56, 31, tzinfo=timezone.utc).timestamp()
    )
    sync_again = client.get_position_close_sync(position.position_id)
    assert isinstance(sync_again, dict)
    assert str(sync_again.get("source") or "") == "ig_positions_deal_id"
    assert sync_again.get("position_found") is False


def test_ig_close_position_uses_delete_method(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str, dict[str, str], dict[str, object]]] = []

    def fake_request(method, path, **kwargs):
        headers = dict(kwargs.get("extra_headers") or {})
        query = dict(kwargs.get("query") or {})
        calls.append((method, path, headers, query))
        if method in {"POST", "DELETE"} and path == "/positions/otc":
            if method == "POST":
                assert headers.get("_method") == "DELETE"
                assert query.get("_method") == "DELETE"
            return {"dealReference": "ref-close-delete-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-delete-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSEDEL1",
                    "level": 1.11111,
                    "date": "2026-03-17T10:00:00",
                },
                {},
            )
        if method == "GET" and path == "/positions/DIAAAATESTDEL01":
            raise BrokerError(
                "IG API GET /positions/DIAAAATESTDEL01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAATESTDEL01",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.5,
        open_price=1.112,
        stop_loss=1.11,
        take_profit=1.115,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position)

    assert any(row[0] == "POST" and row[1] == "/positions/otc" for row in calls)


def test_ig_close_position_supports_partial_volume(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        payload = kwargs.get("payload") or {}
        headers = dict(kwargs.get("extra_headers") or {})
        query = dict(kwargs.get("query") or {})
        if method in {"POST", "DELETE"} and path == "/positions/otc":
            if method == "POST":
                assert headers.get("_method") == "DELETE"
                assert query.get("_method") == "DELETE"
            assert float(payload.get("size") or 0.0) == pytest.approx(0.2)
            return {"dealReference": "ref-close-partial-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-partial-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSEPART1",
                    "level": 1.11111,
                    "date": "2026-03-17T10:00:00",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAATESTDEL02",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.5,
        open_price=1.112,
        stop_loss=1.11,
        take_profit=1.115,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position, volume=0.2)

    sync = client.get_position_close_sync(position.position_id)
    assert isinstance(sync, dict)
    assert float(sync.get("closed_volume") or 0.0) == pytest.approx(0.2)
    assert bool(sync.get("close_complete")) is False


def test_ig_close_position_retries_without_optional_fields_on_validation_null(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str, dict[str, object]]] = []

    def fake_request(method, path, **kwargs):
        payload = dict(kwargs.get("payload") or {})
        calls.append((method, path, payload))
        if method == "POST" and path == "/positions/otc":
            if "orderType" in payload:
                raise BrokerError('IG API POST /positions/otc failed: 400 Bad Request {"errorCode":"validation.null-not-allowed.request"}')
            assert payload.get("dealId") == "DIAAAATESTDEL03"
            assert payload.get("direction") == "SELL"
            assert float(payload.get("size") or 0.0) == pytest.approx(0.5)
            return {"dealReference": "ref-close-retry-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-retry-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSERTRY1",
                    "level": 1.11111,
                    "date": "2026-03-17T10:00:00",
                },
                {},
            )
        if method == "GET" and path == "/positions/DIAAAATESTDEL03":
            raise BrokerError(
                "IG API GET /positions/DIAAAATESTDEL03 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAATESTDEL03",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.5,
        open_price=1.112,
        stop_loss=1.11,
        take_profit=1.115,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position)

    post_calls = [row for row in calls if row[0] == "POST" and row[1] == "/positions/otc"]
    assert len(post_calls) == 3
    assert "timeInForce" in post_calls[0][2]
    assert "orderType" in post_calls[1][2]
    assert "orderType" not in post_calls[2][2]

    sync = client.get_position_close_sync(position.position_id)
    assert isinstance(sync, dict)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.11111)


def test_ig_close_position_falls_back_to_post_method_override_on_validation_null(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str, dict[str, object], dict[str, str]]] = []

    def fake_request(method, path, **kwargs):
        payload = dict(kwargs.get("payload") or {})
        headers = dict(kwargs.get("extra_headers") or {})
        calls.append((method, path, payload, headers))
        if method == "DELETE" and path == "/positions/otc":
            raise BrokerError(
                'IG API DELETE /positions/otc failed: 400 Bad Request {"errorCode":"validation.null-not-allowed.request"}'
            )
        if method == "POST" and path == "/positions/otc":
            assert headers.get("_method") == "DELETE"
            assert payload.get("dealId") == "DIAAAATESTDEL04"
            assert payload.get("direction") == "BUY"
            assert float(payload.get("size") or 0.0) == pytest.approx(0.5)
            return {"dealReference": "ref-close-post-override-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-post-override-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSEPOST1",
                    "level": 1.11111,
                    "date": "2026-03-17T10:00:00",
                },
                {},
            )
        if method == "GET" and path == "/positions/DIAAAATESTDEL04":
            raise BrokerError(
                "IG API GET /positions/DIAAAATESTDEL04 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAATESTDEL04",
        symbol="EURUSD",
        side=Side.SELL,
        volume=0.5,
        open_price=1.112,
        stop_loss=1.114,
        take_profit=1.109,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position)

    delete_calls = [row for row in calls if row[0] == "DELETE" and row[1] == "/positions/otc"]
    post_calls = [row for row in calls if row[0] == "POST" and row[1] == "/positions/otc"]
    assert post_calls
    assert not delete_calls
    assert post_calls[0][3].get("_method") == "DELETE"

    sync = client.get_position_close_sync(position.position_id)
    assert isinstance(sync, dict)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.11111)


def test_ig_close_position_falls_back_to_delete_when_post_override_is_not_honored(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str, dict[str, object], dict[str, str], dict[str, object]]] = []

    def fake_request(method, path, **kwargs):
        payload = dict(kwargs.get("payload") or {})
        headers = dict(kwargs.get("extra_headers") or {})
        query = dict(kwargs.get("query") or {})
        calls.append((method, path, payload, headers, query))
        if method == "POST" and path == "/positions/otc":
            raise BrokerError(
                'IG API POST /positions/otc failed: 404 Not Found {"errorCode":"error.service.marketdata.position.notional.details.null.error"}'
            )
        if method == "DELETE" and path == "/positions/otc":
            assert payload.get("dealId") == "DIAAAATESTDEL05"
            return {"dealReference": "ref-close-delete-fallback-1"}, {}
        if method == "GET" and path == "/confirms/ref-close-delete-fallback-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAACLOSEDELFB1",
                    "level": 1.11111,
                    "date": "2026-03-17T10:00:00",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAATESTDEL05",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.5,
        open_price=1.112,
        stop_loss=1.11,
        take_profit=1.115,
        opened_at=1.0,
        status="open",
    )
    client.close_position(position)

    post_calls = [row for row in calls if row[0] == "POST" and row[1] == "/positions/otc"]
    delete_calls = [row for row in calls if row[0] == "DELETE" and row[1] == "/positions/otc"]
    assert post_calls
    assert delete_calls
    assert post_calls[0][3].get("_method") == "DELETE"
    assert post_calls[0][4].get("_method") == "DELETE"


def test_ig_close_position_raises_when_confirm_rejected(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        headers = dict(kwargs.get("extra_headers") or {})
        query = dict(kwargs.get("query") or {})
        if method in {"POST", "DELETE"} and path == "/positions/otc":
            if method == "POST":
                assert headers.get("_method") == "DELETE"
                assert query.get("_method") == "DELETE"
            return {"dealReference": "ref-close-2"}, {}
        if method == "GET" and path == "/confirms/ref-close-2":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAAREJECT001",
        symbol="GBPUSD",
        side=Side.SELL,
        volume=0.56,
        open_price=1.34155,
        stop_loss=1.34212,
        take_profit=1.33855,
        opened_at=1.0,
        status="open",
    )
    with pytest.raises(BrokerError, match="IG close deal rejected"):
        client.close_position(position)
    assert client.get_position_close_sync(position.position_id) is None


def test_ig_cleanup_internal_caches_prunes_stale_entries(monkeypatch):
    client = _make_client()
    now = {"ts": 1_700_000_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now["ts"])

    with client._lock:
        client._sync_cache_max_age_sec = 60.0  # type: ignore[attr-defined]
        client._cache_cleanup_interval_sec = 0.0  # type: ignore[attr-defined]
        client._position_open_sync["stale-open"] = {"_cache_ts": now["ts"] - 120.0}  # type: ignore[attr-defined]
        client._position_open_sync["fresh-open"] = {"_cache_ts": now["ts"] - 10.0}  # type: ignore[attr-defined]
        client._position_close_sync["stale-close"] = {"_cache_ts": now["ts"] - 120.0}  # type: ignore[attr-defined]
        client._position_close_sync["fresh-close"] = {"_cache_ts": now["ts"] - 10.0}  # type: ignore[attr-defined]
        client._pending_confirm_last_probe_ts["stale-probe"] = now["ts"] - 120.0  # type: ignore[attr-defined]
        client._pending_confirm_last_probe_ts["fresh-probe"] = now["ts"] - 10.0  # type: ignore[attr-defined]

    client._maybe_cleanup_internal_caches(force=True)

    with client._lock:
        assert "stale-open" not in client._position_open_sync  # type: ignore[attr-defined]
        assert "fresh-open" in client._position_open_sync  # type: ignore[attr-defined]
        assert "stale-close" not in client._position_close_sync  # type: ignore[attr-defined]
        assert "fresh-close" in client._position_close_sync  # type: ignore[attr-defined]
        assert "stale-probe" not in client._pending_confirm_last_probe_ts  # type: ignore[attr-defined]
        assert "fresh-probe" in client._pending_confirm_last_probe_ts  # type: ignore[attr-defined]


def test_ig_stream_line_logs_control_events(caplog):
    client = _make_client()
    with caplog.at_level(logging.WARNING, logger="xtb_bot.ig_client"):
        handled = client._handle_stream_line("LOOP")
    assert handled is False
    assert any("control event" in rec.message.lower() and "LOOP" in rec.message for rec in caplog.records)


def test_ig_modify_position_uses_put_method(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        calls.append((method, path))
        return {}, {}

    monkeypatch.setattr(client, "_request", fake_request)
    position = Position(
        position_id="DIAAAAMODIFY001",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.5,
        open_price=1.1000,
        stop_loss=1.0980,
        take_profit=1.1040,
        opened_at=1.0,
        status="open",
    )

    client.modify_position(position, stop_loss=1.0990, take_profit=1.1030)
    assert ("PUT", f"/positions/otc/{position.position_id}") in calls


def test_ig_open_position_rejected_error_contains_confirm_summary(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/markets/IX.D.NASDAQ.CASH.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            return {"dealReference": "ref-open-1"}, {}
        if method == "GET" and path == "/confirms/ref-open-1":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "MINIMUM_ORDER_SIZE_ERROR",
                    "epic": "IX.D.NASDAQ.CASH.IP",
                    "direction": "BUY",
                    "size": 0.2,
                    "status": "ERROR",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(BrokerError, match=r"confirm=\{.*\"dealStatus\":\"REJECTED\".*\}") as exc_info:
        client.open_position(
            symbol="US100",
            side=Side.BUY,
            volume=0.2,
            stop_loss=20980.0,
            take_profit=21060.0,
            comment="XTBBOT:TEST:ABC:US100",
        )

    message = str(exc_info.value)
    assert "IG deal rejected: MINIMUM_ORDER_SIZE_ERROR" in message
    assert "confirm={\"dealStatus\":\"REJECTED\",\"direction\":\"BUY\",\"epic\":\"IX.D.NASDAQ.CASH.IP\"" in message


def test_ig_open_position_recovers_when_confirm_rejected_but_position_is_open(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "GBP"  # type: ignore[attr-defined]
    client._activate_epic("AUS200", "IX.D.ASX.IFS.IP")

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path == "/markets/IX.D.ASX.IFS.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "GBP", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            return {"dealReference": "IGBOT81361a628de5b3d53a"}, {}
        if method == "GET" and path == "/confirms/IGBOT81361a628de5b3d53a":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "dealId": "DIAAAAWWCW9QBAQ",
                    "direction": "SELL",
                    "epic": "IX.D.ASX.IFS.IP",
                },
                {},
            )
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAWWCW23YA4",
                                "dealReference": "IGBOT81361a628de5b3d53a",
                                "direction": "SELL",
                                "size": 3.4,
                                "level": 8655.8,
                                "stopLevel": 8676.0,
                                "limitLevel": 8615.0,
                            },
                            "market": {"epic": "IX.D.ASX.IFS.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_should_try_next_epic_on_open_reject", lambda *args, **kwargs: False)

    position_id = client.open_position(
        symbol="AUS200",
        side=Side.SELL,
        volume=3.44,
        stop_loss=8676.0,
        take_profit=8615.0,
        comment="IGBOT:81361a62:8de5b3d53a",
        entry_price=8655.8,
    )

    assert position_id == "DIAAAAWWCW23YA4"
    assert ("GET", "/positions") in requests
    sync = client.get_position_open_sync(position_id)
    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_positions_after_rejected_confirm"
    assert str(sync.get("deal_reference") or "") == "IGBOT81361a628de5b3d53a"
    assert float(sync.get("open_price") or 0.0) == pytest.approx(8655.8)
    assert float(sync.get("stop_loss") or 0.0) == pytest.approx(8676.0)
    assert float(sync.get("take_profit") or 0.0) == pytest.approx(8615.0)


def test_ig_get_position_open_sync_falls_back_to_positions_lookup(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAFALLBACK001",
                                "dealReference": "IGBOTREFSYNC001",
                                "level": 8716.2,
                                "stopLevel": 8718.0,
                                "limitLevel": 8675.0,
                            },
                            "market": {"epic": "CC.D.CL.UNC.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    sync = client.get_position_open_sync("DIAAAAFALLBACK001")
    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_positions_deal_id"
    assert str(sync.get("deal_reference") or "") == "IGBOTREFSYNC001"
    assert float(sync.get("open_price") or 0.0) == pytest.approx(8716.2)
    assert float(sync.get("stop_loss") or 0.0) == pytest.approx(8718.0)
    assert float(sync.get("take_profit") or 0.0) == pytest.approx(8675.0)


def test_ig_open_position_recovers_real_deal_id_after_confirm_timeout(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    clock = {"ts": 1_700_000_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: clock.__setitem__("ts", clock["ts"] + float(sec)))

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        _ = kwargs
        if method == "GET" and path == "/markets/CC.D.CL.UNC.IP":
            return (
                {
                    "instrument": {
                        "epic": "CC.D.CL.UNC.IP",
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "marginFactor": 10.0,
                        "marginFactorUnit": "PERCENTAGE",
                        "valueOfOnePip": 10.0,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1, "bid": 491.5, "offer": 492.0},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            return {"dealReference": "IGBOTTIMEOUT001"}, {}
        if method == "GET" and path == "/confirms/IGBOTTIMEOUT001":
            return ({"dealStatus": "PENDING"}, {})
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAARECOV001",
                                "dealReference": "IGBOTTIMEOUT001",
                                "direction": "BUY",
                                "size": 0.05,
                                "level": 491.7,
                                "stopLevel": 439.2,
                                "limitLevel": 544.2,
                            },
                            "market": {"epic": "CC.D.CL.UNC.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.UNC.IP"])

    position_id = client.open_position(
        symbol="WTI",
        side=Side.BUY,
        volume=0.05,
        stop_loss=439.2,
        take_profit=544.2,
        comment="XTBBOT:TEST:TIMEOUT:WTI",
        entry_price=491.7,
    )

    assert position_id == "DIAAAARECOV001"
    assert position_id != "IGBOTTIMEOUT001"
    assert ("GET", "/positions") in requests
    sync = client.get_position_open_sync(position_id)
    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_positions_after_rejected_confirm"
    assert str(sync.get("deal_reference") or "") == "IGBOTTIMEOUT001"
    assert float(sync.get("open_price") or 0.0) == pytest.approx(491.7)
    assert float(sync.get("stop_loss") or 0.0) == pytest.approx(439.2)
    assert float(sync.get("take_profit") or 0.0) == pytest.approx(544.2)


def test_ig_open_position_confirm_timeout_raises_instead_of_returning_deal_reference(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    clock = {"ts": 1_700_000_000.0}
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: clock.__setitem__("ts", clock["ts"] + float(sec)))

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/markets/CC.D.CL.UNC.IP":
            return (
                {
                    "instrument": {
                        "epic": "CC.D.CL.UNC.IP",
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "marginFactor": 10.0,
                        "marginFactorUnit": "PERCENTAGE",
                        "valueOfOnePip": 10.0,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1, "bid": 491.5, "offer": 492.0},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            return {"dealReference": "IGBOTTIMEOUT002"}, {}
        if method == "GET" and path == "/confirms/IGBOTTIMEOUT002":
            return ({"dealStatus": "PENDING"}, {})
        if method == "GET" and path == "/positions":
            return ({"positions": []}, {})
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.UNC.IP"])

    with pytest.raises(BrokerError, match="confirm timed out before dealId was available"):
        client.open_position(
            symbol="WTI",
            side=Side.BUY,
            volume=0.05,
            stop_loss=439.2,
            take_profit=544.2,
            comment="XTBBOT:TEST:TIMEOUT:WTI",
            entry_price=491.7,
        )

    with client._lock:
        assert "IGBOTTIMEOUT002" not in client._position_open_sync  # type: ignore[attr-defined]


def test_ig_open_position_fails_over_to_next_epic_when_cash_epic_rejected(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        _ = kwargs
        if method == "GET" and path in {"/markets/IX.D.SPTRD.CASH.IP", "/markets/IX.D.SPTRD.DAILY.IP"}:
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = kwargs.get("payload") or {}
            epic = str(payload.get("epic") or "")
            if epic == "IX.D.SPTRD.CASH.IP":
                return {"dealReference": "ref-open-cash"}, {}
            if epic == "IX.D.SPTRD.DAILY.IP":
                return {"dealReference": "ref-open-daily"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-cash":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.SPTRD.CASH.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-daily":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAASP500OK",
                    "epic": "IX.D.SPTRD.DAILY.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="US500",
        side=Side.SELL,
        volume=0.2,
        stop_loss=6711.7,
        take_profit=6666.7,
        comment="XTBBOT:TEST:ABC:US500",
    )

    assert deal_id == "DIAAAASP500OK"
    assert ("GET", "/confirms/ref-open-cash") in requests
    assert ("GET", "/confirms/ref-open-daily") in requests


def test_ig_open_position_uses_unique_deal_reference_per_attempt(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    post_references: list[str] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path in {"/markets/IX.D.SPTRD.CASH.IP", "/markets/IX.D.SPTRD.DAILY.IP"}:
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = kwargs.get("payload") or {}
            post_references.append(str(payload.get("dealReference") or ""))
            epic = str(payload.get("epic") or "")
            if epic == "IX.D.SPTRD.CASH.IP":
                return {"dealReference": "ref-open-cash-unique"}, {}
            if epic == "IX.D.SPTRD.DAILY.IP":
                return {"dealReference": "ref-open-daily-unique"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-cash-unique":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.SPTRD.CASH.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-daily-unique":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAASP500OK2",
                    "epic": "IX.D.SPTRD.DAILY.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="US500",
        side=Side.SELL,
        volume=0.2,
        stop_loss=6711.7,
        take_profit=6666.7,
        comment="XTBBOT:TEST:ABC:US500",
    )

    assert deal_id == "DIAAAASP500OK2"
    assert len(post_references) == 2
    assert post_references[0]
    assert post_references[1]
    assert post_references[0] != post_references[1]


def test_ig_open_position_retries_next_epic_when_post_returns_instrument_invalid(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    client._invalid_epic_retry_sec = 3600.0  # type: ignore[attr-defined]

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path in {"/markets/IX.D.NASDAQ.CASH.IP", "/markets/IX.D.NASDAQ.DAILY.IP"}:
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            epic = str((kwargs.get("payload") or {}).get("epic") or "")
            if epic == "IX.D.NASDAQ.CASH.IP":
                raise BrokerError(
                    'IG API POST /positions/otc failed: 400 Bad Request {"errorCode":"error.service.create.otc.position.instrument.invalid"}'
                )
            if epic == "IX.D.NASDAQ.DAILY.IP":
                return {"dealReference": "ref-open-daily-post-retry"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-daily-post-retry":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAANASDAQOK",
                    "epic": "IX.D.NASDAQ.DAILY.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="US100",
        side=Side.BUY,
        volume=0.2,
        stop_loss=20980.0,
        take_profit=21060.0,
        comment="XTBBOT:TEST:ABC:US100",
    )

    assert deal_id == "DIAAAANASDAQOK"
    assert ("POST", "/positions/otc") in requests
    assert ("GET", "/confirms/ref-open-daily-post-retry") in requests
    blocked = client._invalid_epic_until_ts_by_symbol.get("US100", {})  # type: ignore[attr-defined]
    assert float(blocked.get("IX.D.NASDAQ.CASH.IP", 0.0)) > 0.0
    assert "IX.D.NASDAQ.CASH.IP" not in client._epic_attempt_order("US100")


def test_ig_open_position_retries_from_ifs_unknown_to_cfd(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    client._activate_epic("US100", "IX.D.NASDAQ.IFS.IP")

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path in {
            "/markets/IX.D.NASDAQ.IFS.IP",
            "/markets/IX.D.NASDAQ.CASH.IP",
            "/markets/IX.D.NASDAQ.DAILY.IP",
            "/markets/IX.D.NASDAQ.CFD.IP",
        }:
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            epic = str((kwargs.get("payload") or {}).get("epic") or "")
            if epic == "IX.D.NASDAQ.IFS.IP":
                return {"dealReference": "ref-open-ifs"}, {}
            if epic == "IX.D.NASDAQ.CASH.IP":
                return {"dealReference": "ref-open-cash-after-ifs"}, {}
            if epic == "IX.D.NASDAQ.DAILY.IP":
                return {"dealReference": "ref-open-daily-after-ifs"}, {}
            if epic == "IX.D.NASDAQ.CFD.IP":
                return {"dealReference": "ref-open-cfd"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-ifs":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.NASDAQ.IFS.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-cash-after-ifs":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "INSTRUMENT_NOT_VALID",
                    "status": "ERROR",
                    "epic": "IX.D.NASDAQ.CASH.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-daily-after-ifs":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.NASDAQ.DAILY.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-cfd":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAANASDAQCFD",
                    "epic": "IX.D.NASDAQ.CFD.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="US100",
        side=Side.SELL,
        volume=0.01,
        stop_loss=24464.0,
        take_profit=24250.5,
        comment="XTBBOT:TEST:ABC:US100",
    )

    assert deal_id == "DIAAAANASDAQCFD"
    assert ("GET", "/confirms/ref-open-ifs") in requests
    assert ("GET", "/confirms/ref-open-cash-after-ifs") in requests
    assert ("GET", "/confirms/ref-open-daily-after-ifs") in requests
    assert ("GET", "/confirms/ref-open-cfd") in requests


def test_ig_open_position_uses_instrument_currency_when_account_currency_is_unavailable(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "EUR"  # type: ignore[attr-defined]
    client._activate_epic("UK100", "IX.D.FTSE.DAILY.IP")

    seen_payloads: list[dict[str, object]] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/IX.D.FTSE.DAILY.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [
                            {"code": "GBP", "isDefault": True},
                            {"code": "USD", "isDefault": False},
                        ],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = dict(kwargs.get("payload") or {})
            seen_payloads.append(payload)
            return {"dealReference": "ref-ftse-1"}, {}
        if method == "GET" and path == "/confirms/ref-ftse-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAFTSEOK",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="UK100",
        side=Side.SELL,
        volume=0.35,
        stop_loss=10341.9,
        take_profit=10281.9,
        comment="XTBBOT:TEST:ABC:UK100",
    )

    assert deal_id == "DIAAAAFTSEOK"
    assert seen_payloads
    assert seen_payloads[0].get("currencyCode") == "GBP"


def test_ig_open_position_retries_next_epic_on_currency_reject(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "EUR"  # type: ignore[attr-defined]
    client._activate_epic("UK100", "IX.D.FTSE.DAILY.IP")

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path in {
            "/markets/IX.D.FTSE.DAILY.IP",
            "/markets/IX.D.FTSE.CASH.IP",
            "/markets/IX.D.FTSE.IFS.IP",
        }:
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "GBP", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = kwargs.get("payload") or {}
            epic = str(payload.get("epic") or "")
            currency = str(payload.get("currencyCode") or "")
            if epic == "IX.D.FTSE.DAILY.IP":
                if currency == "GBP":
                    return {"dealReference": "ref-ftse-daily-gbp"}, {}
                raise AssertionError(f"Unexpected currency for daily epic: {currency}")
            if epic == "IX.D.FTSE.CASH.IP":
                return {"dealReference": "ref-ftse-cash"}, {}
            if epic == "IX.D.FTSE.IFS.IP":
                return {"dealReference": "ref-ftse-ifs"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-ftse-daily-gbp":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-ftse-cash":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "INSTRUMENT_NOT_VALID",
                    "status": "ERROR",
                    "epic": "IX.D.FTSE.CASH.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-ftse-ifs":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAFTSEIFS",
                    "epic": "IX.D.FTSE.IFS.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="UK100",
        side=Side.BUY,
        volume=0.35,
        stop_loss=10313.1,
        take_profit=10373.1,
        comment="XTBBOT:TEST:ABC:UK100",
    )

    assert deal_id == "DIAAAAFTSEIFS"
    assert ("GET", "/confirms/ref-ftse-daily-gbp") in requests
    assert ("GET", "/confirms/ref-ftse-cash") in requests
    assert ("GET", "/confirms/ref-ftse-ifs") in requests


def test_ig_open_position_prefers_next_currency_before_next_epic_for_currency_status_reason(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "EUR"  # type: ignore[attr-defined]
    client._activate_epic("UK100", "IX.D.FTSE.DAILY.IP")

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path == "/markets/IX.D.FTSE.DAILY.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [
                            {"code": "GBP", "isDefault": True},
                            {"code": "EUR", "isDefault": False},
                        ],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = kwargs.get("payload") or {}
            epic = str(payload.get("epic") or "")
            currency = str(payload.get("currencyCode") or "")
            assert epic == "IX.D.FTSE.DAILY.IP"
            if currency == "EUR":
                return {"dealReference": "ref-ftse-daily-eur"}, {}
            if currency == "GBP":
                return {"dealReference": "ref-ftse-daily-gbp"}, {}
            raise AssertionError(f"Unexpected currency: {currency}")
        if method == "GET" and path == "/confirms/ref-ftse-daily-eur":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "statusReason": "Failed to retrieve price information for this currency",
                    "status": "ERROR",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-ftse-daily-gbp":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAFTSEGBP",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="UK100",
        side=Side.BUY,
        volume=0.35,
        stop_loss=10313.1,
        take_profit=10373.1,
        comment="XTBBOT:TEST:ABC:UK100",
    )

    assert deal_id == "DIAAAAFTSEGBP"
    # Stayed on the same epic and retried currency instead of failing over to next epic.
    assert ("GET", "/markets/IX.D.FTSE.CASH.IP") not in requests
    assert ("GET", "/markets/IX.D.FTSE.IFS.IP") not in requests
    assert ("GET", "/confirms/ref-ftse-daily-eur") in requests
    assert ("GET", "/confirms/ref-ftse-daily-gbp") in requests


def test_ig_get_symbol_spec_uses_conservative_fx_min_deal_fallback(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {"contractSize": 100000.0},
                "dealingRules": {
                    "minDealSize": None,
                    "maxDealSize": {"value": 100.0},
                    "minStepDistance": {"value": 0.01},
                },
                "snapshot": {"decimalPlacesFactor": 5},
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("AUDUSD")
    assert spec.lot_min == pytest.approx(0.5)
    assert spec.metadata.get("lot_min_source") == "fallback_fx_0.5"


def test_ig_open_position_blocks_volume_below_broker_minimum(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CS.D.AUDUSD.CFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": None,
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 5},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            raise AssertionError("Order must be blocked locally before POST")
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(BrokerError, match=r"Requested size below broker minimum.*requested=0.4.*min=0.5"):
        client.open_position(
            symbol="AUDUSD",
            side=Side.SELL,
            volume=0.4,
            stop_loss=0.70837,
            take_profit=0.70237,
            comment="XTBBOT:TEST:ABC:AUDUSD",
        )


def test_ig_open_position_promotes_min_lot_after_minimum_order_reject(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    post_calls = 0

    def fake_request(method, path, **kwargs):
        nonlocal post_calls
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": None,
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 5},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            post_calls += 1
            return {"dealReference": f"ref-eurusd-{post_calls}"}, {}
        if method == "GET" and path == "/confirms/ref-eurusd-1":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "MINIMUM_ORDER_SIZE_ERROR",
                    "status": "ERROR",
                    "epic": "CS.D.EURUSD.CFD.IP",
                },
                {},
            )
        if method == "GET" and path == "/markets":
            return {"markets": []}, {}
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    with pytest.raises(BrokerError, match="MINIMUM_ORDER_SIZE_ERROR"):
        client.open_position(
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.75,
            stop_loss=1.1490,
            take_profit=1.1520,
            comment="XTBBOT:TEST:ABC:EURUSD",
        )

    spec = client.get_symbol_spec("EURUSD")
    assert spec.lot_min == pytest.approx(1.0)
    assert str(spec.metadata.get("lot_min_source") or "").startswith("adaptive_reject_")
    assert post_calls == 1

    with pytest.raises(BrokerError, match=r"Requested size below broker minimum.*requested=0.75.*min=1"):
        client.open_position(
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.75,
            stop_loss=1.1490,
            take_profit=1.1520,
            comment="XTBBOT:TEST:ABC:EURUSD",
        )

    assert post_calls == 1


def test_ig_open_position_increment_reject_promotes_lot_step_only(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    post_calls = 0

    def fake_request(method, path, **kwargs):
        nonlocal post_calls
        if method == "GET" and path == "/markets/CC.D.CL.UNC.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            post_calls += 1
            return {"dealReference": f"ref-wti-{post_calls}"}, {}
        if method == "GET" and path == "/confirms/ref-wti-1":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "SIZE_INCREMENT",
                    "status": "ERROR",
                    "epic": "CC.D.CL.UNC.IP",
                },
                {},
            )
        if method == "GET" and path == "/markets":
            return {"markets": []}, {}
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.UNC.IP"])

    with pytest.raises(BrokerError, match="ORDER_SIZE_INCREMENT_ERROR"):
        client.open_position(
            symbol="WTI",
            side=Side.BUY,
            volume=0.31,
            stop_loss=9700.0,
            take_profit=9800.0,
            comment="XTBBOT:TEST:ABC:WTI",
        )

    spec = client.get_symbol_spec("WTI")
    assert spec.lot_min == pytest.approx(0.1)
    assert spec.lot_step == pytest.approx(0.1)
    assert post_calls == 1


def test_ig_open_position_normalizes_size_to_candidate_step(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    posted_sizes: list[float] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CC.D.CL.UNC.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = dict(kwargs.get("payload") or {})
            posted_sizes.append(float(payload.get("size") or 0.0))
            return {"dealReference": "ref-wti-normalized"}, {}
        if method == "GET" and path == "/confirms/ref-wti-normalized":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAWTINORM",
                    "epic": "CC.D.CL.UNC.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.UNC.IP"])

    deal_id = client.open_position(
        symbol="WTI",
        side=Side.BUY,
        volume=0.32,
        stop_loss=9700.0,
        take_profit=9800.0,
        comment="XTBBOT:TEST:ABC:WTI",
    )

    assert deal_id == "DIAAAAWTINORM"
    assert posted_sizes == pytest.approx([0.3])


def test_ig_get_symbol_spec_uses_index_fallback_min_deal_size(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/IX.D.NASDAQ.CASH.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": None,
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    spec = client.get_symbol_spec("US100")
    assert spec.lot_min == pytest.approx(0.1)
    assert spec.metadata.get("lot_min_source") == "fallback_index_0.1"


def test_ig_open_position_skips_candidate_epic_when_volume_below_that_epic_minimum(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    client._activate_epic("US100", "IX.D.NASDAQ.IFS.IP")

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/IX.D.NASDAQ.IFS.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "GET" and path in {
            "/markets/IX.D.NASDAQ.CASH.IP",
            "/markets/IX.D.NASDAQ.DAILY.IP",
            "/markets/IX.D.NASDAQ.CFD.IP",
        }:
            raise BrokerError(
                f'IG API GET {path} failed: 404 Not Found {{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}}'
            )
        if method == "GET" and path == "/markets/IX.D.NASDAQ.IFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.5},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            epic = str((kwargs.get("payload") or {}).get("epic") or "")
            if epic == "IX.D.NASDAQ.IFS.IP":
                return {"dealReference": "ref-open-ifs-small"}, {}
            raise AssertionError(f"Order must not be posted to {epic}")
        if method == "GET" and path == "/confirms/ref-open-ifs-small":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "IX.D.NASDAQ.IFS.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["IX.D.NASDAQ.IFD.IP"])

    with pytest.raises(BrokerError, match=r"Requested size below broker minimum.*requested=0.01.*min=0.5.*epic=IX\.D\.NASDAQ\.IFD\.IP"):
        client.open_position(
            symbol="US100",
            side=Side.SELL,
            volume=0.01,
            stop_loss=24484.9,
            take_profit=24271.1,
            comment="XTBBOT:TEST:ABC:US100",
        )


def test_ig_open_position_ignores_stale_reference_min_and_uses_candidate_epic(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]
    client._symbol_spec_cache["US100"] = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=1.0,
        lot_max=100.0,
        lot_step=0.1,
        price_precision=1,
        lot_precision=1,
        metadata={"epic": "IX.D.NASDAQ.CASH.IP", "lot_min_source": "stale_cached"},
    )

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/IX.D.NASDAQ.IFS.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            return {"dealReference": "ref-open-us100-ok"}, {}
        if method == "GET" and path == "/confirms/ref-open-us100-ok":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAUS100OK",
                    "epic": "IX.D.NASDAQ.IFS.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["IX.D.NASDAQ.IFS.IP"])

    deal_id = client.open_position(
        symbol="US100",
        side=Side.BUY,
        volume=0.2,
        stop_loss=24300.0,
        take_profit=24500.0,
        comment="XTBBOT:TEST:ABC:US100",
    )

    assert deal_id == "DIAAAAUS100OK"


def test_ig_open_position_retries_next_epic_on_brent_unknown_reject(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path in {"/markets/CC.D.LCO.USS.IP", "/markets/CC.D.LCO.CFD.IP"}:
            return (
                {
                    "instrument": {
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.1},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            epic = str((kwargs.get("payload") or {}).get("epic") or "")
            if epic == "CC.D.LCO.USS.IP":
                return {"dealReference": "ref-open-brent-uss"}, {}
            if epic == "CC.D.LCO.CFD.IP":
                return {"dealReference": "ref-open-brent-cfd"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-brent-uss":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "CC.D.LCO.USS.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-brent-cfd":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAABRENTOK",
                    "epic": "CC.D.LCO.CFD.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["CC.D.LCO.CFD.IP"])

    deal_id = client.open_position(
        symbol="BRENT",
        side=Side.BUY,
        volume=0.18,
        stop_loss=9907.6,
        take_profit=10051.1,
        comment="XTBBOT:TEST:ABC:BRENT",
    )

    assert deal_id == "DIAAABRENTOK"
    assert ("GET", "/confirms/ref-open-brent-uss") in requests
    assert ("GET", "/confirms/ref-open-brent-cfd") in requests


def test_ig_open_position_keeps_previous_epic_when_all_candidates_rejected(monkeypatch):
    client = _make_client()
    client._connected = True
    baseline_epic = client._epic_for_symbol("WTI")

    requests: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        requests.append((method, path))
        if method == "GET" and path in {"/markets/CC.D.CL.UNC.IP", "/markets/CC.D.CL.IFS.IP"}:
            return (
                {
                    "instrument": {"contractSize": 1000.0},
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            epic = str((kwargs.get("payload") or {}).get("epic") or "")
            if epic == "CC.D.CL.UNC.IP":
                return {"dealReference": "ref-open-wti-unc"}, {}
            if epic == "CC.D.CL.IFS.IP":
                return {"dealReference": "ref-open-wti-ifs"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-wti-unc":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "UNKNOWN",
                    "status": "ERROR",
                    "epic": "CC.D.CL.UNC.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-wti-ifs":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "MARKET_CLOSED_WITH_EDITS",
                    "status": "ERROR",
                    "epic": "CC.D.CL.IFS.IP",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_order_currency_attempts", lambda symbol, spec, **kwargs: ["USD"])
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.UNC.IP"])
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["CC.D.CL.IFS.IP"])

    with pytest.raises(BrokerError, match="MARKET_CLOSED_WITH_EDITS"):
        client.open_position(
            symbol="WTI",
            side=Side.SELL,
            volume=0.05,
            stop_loss=489.3,
            take_profit=384.3,
            comment="XTBBOT:TEST:ABC:WTI",
            entry_price=459.3,
        )

    assert client._epic_for_symbol("WTI") == baseline_epic
    assert ("GET", "/confirms/ref-open-wti-unc") in requests
    assert ("GET", "/confirms/ref-open-wti-ifs") in requests


def test_ig_open_position_recalculates_attached_levels_for_candidate_epic(monkeypatch):
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    posted_payloads: list[dict[str, object]] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CC.D.CL.OLD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "onePipMeans": 0.1,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {"decimalPlacesFactor": 1, "bid": 491.5, "offer": 492.0},
                },
                {},
            )
        if method == "GET" and path == "/markets/CC.D.CL.UNC.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 1000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "onePipMeans": 1.0,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.01},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.1},
                    },
                    "snapshot": {"decimalPlacesFactor": 1, "bid": 10055.0, "offer": 10059.0},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            payload = dict(kwargs.get("payload") or {})
            posted_payloads.append(payload)
            epic = str(payload.get("epic") or "")
            if epic == "CC.D.CL.OLD.IP":
                return {"dealReference": "ref-open-wti-old"}, {}
            if epic == "CC.D.CL.UNC.IP":
                return {"dealReference": "ref-open-wti-unc"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-open-wti-old":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "ATTACHED_ORDER_LEVEL_ERROR",
                    "status": "ERROR",
                    "epic": "CC.D.CL.OLD.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-open-wti-unc":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAWTIUNC",
                    "epic": "CC.D.CL.UNC.IP",
                    "level": 10059.0,
                    "stopLevel": 9534.0,
                    "limitLevel": 10584.0,
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["CC.D.CL.OLD.IP"])
    monkeypatch.setattr(client, "_extend_epic_candidates_from_search", lambda symbol: ["CC.D.CL.UNC.IP"])

    deal_id = client.open_position(
        symbol="WTI",
        side=Side.BUY,
        volume=0.05,
        stop_loss=439.2,
        take_profit=544.2,
        comment="XTBBOT:TEST:ABC:WTI",
        entry_price=491.7,
    )

    assert deal_id == "DIAAAAWTIUNC"
    assert len(posted_payloads) == 2
    assert posted_payloads[0]["epic"] == "CC.D.CL.OLD.IP"
    assert posted_payloads[0]["stopLevel"] == pytest.approx(439.5)
    assert posted_payloads[0]["limitLevel"] == pytest.approx(544.5)
    assert posted_payloads[1]["epic"] == "CC.D.CL.UNC.IP"
    assert posted_payloads[1]["stopLevel"] == pytest.approx(9534.0)
    assert posted_payloads[1]["limitLevel"] == pytest.approx(10584.0)

    open_sync = client.get_position_open_sync("DIAAAAWTIUNC")
    assert open_sync is not None
    assert float(open_sync["open_price"]) == pytest.approx(10059.0)
    assert float(open_sync["stop_loss"]) == pytest.approx(9534.0)
    assert float(open_sync["take_profit"]) == pytest.approx(10584.0)


def test_ig_open_position_sets_guaranteed_stop_when_market_requires_it(monkeypatch):
    monkeypatch.setenv("IG_GUARANTEED_STOP_MODE", "auto")
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    posted_payloads: list[dict[str, object]] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "controlledRiskAllowed": True,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.5},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                        "minNormalStopOrLimitDistance": {"value": 0.0, "unit": "PIPS"},
                        "minControlledRiskStopDistance": {"value": 4.0, "unit": "PIPS"},
                    },
                    "snapshot": {"decimalPlacesFactor": 5, "bid": 1.10100, "offer": 1.10120},
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            posted_payloads.append(dict(kwargs.get("payload") or {}))
            return {"dealReference": "ref-guaranteed-1"}, {}
        if method == "GET" and path == "/confirms/ref-guaranteed-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAGUARANTEED1",
                    "epic": "CS.D.EURUSD.CFD.IP",
                    "level": 1.10120,
                    "stopLevel": 1.10070,
                    "limitLevel": 1.10320,
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.8,
        stop_loss=1.10070,
        take_profit=1.10320,
        comment="XTBBOT:TEST:ABC:EURUSD",
        entry_price=1.10120,
    )

    assert deal_id == "DIAAAAGUARANTEED1"
    assert posted_payloads
    assert posted_payloads[0].get("guaranteedStop") is True


def test_ig_open_position_can_attach_quote_id_and_level_tolerance(monkeypatch):
    monkeypatch.setenv("IG_OPEN_USE_QUOTE_ID", "true")
    monkeypatch.setenv("IG_OPEN_LEVEL_TOLERANCE_PIPS", "2.5")
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    posted_payloads: list[dict[str, object]] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "onePipMeans": 0.0001,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.5},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {
                        "decimalPlacesFactor": 5,
                        "bid": 1.10100,
                        "offer": 1.10120,
                        "quoteId": "QUOTE-ABC-123",
                    },
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            posted_payloads.append(dict(kwargs.get("payload") or {}))
            return {"dealReference": "ref-quote-1"}, {}
        if method == "GET" and path == "/confirms/ref-quote-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAQUOTE001",
                    "epic": "CS.D.EURUSD.CFD.IP",
                    "level": 1.10120,
                    "stopLevel": 1.10000,
                    "limitLevel": 1.10320,
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.8,
        stop_loss=1.10000,
        take_profit=1.10320,
        comment="XTBBOT:TEST:ABC:EURUSD",
        entry_price=1.10120,
    )

    assert deal_id == "DIAAAAQUOTE001"
    assert posted_payloads
    assert posted_payloads[0].get("orderType") == "QUOTE"
    assert posted_payloads[0].get("quoteId") == "QUOTE-ABC-123"
    assert float(posted_payloads[0].get("level") or 0.0) == pytest.approx(1.1015)


def test_ig_open_position_market_order_does_not_send_level_or_quote_id(monkeypatch):
    monkeypatch.setenv("IG_OPEN_USE_QUOTE_ID", "false")
    monkeypatch.setenv("IG_OPEN_LEVEL_TOLERANCE_PIPS", "2.5")
    client = _make_client()
    client._connected = True
    client._account_currency_code = "USD"  # type: ignore[attr-defined]

    posted_payloads: list[dict[str, object]] = []

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/markets/CS.D.EURUSD.CFD.IP":
            return (
                {
                    "instrument": {
                        "contractSize": 100000.0,
                        "currencies": [{"code": "USD", "isDefault": True}],
                        "onePipMeans": 0.0001,
                    },
                    "dealingRules": {
                        "minDealSize": {"value": 0.5},
                        "maxDealSize": {"value": 100.0},
                        "minStepDistance": {"value": 0.01},
                    },
                    "snapshot": {
                        "decimalPlacesFactor": 5,
                        "bid": 1.10100,
                        "offer": 1.10120,
                        "quoteId": "QUOTE-ABC-123",
                    },
                },
                {},
            )
        if method == "POST" and path == "/positions/otc":
            posted_payloads.append(dict(kwargs.get("payload") or {}))
            return {"dealReference": "ref-market-1"}, {}
        if method == "GET" and path == "/confirms/ref-market-1":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "DIAAAAMARKET001",
                    "epic": "CS.D.EURUSD.CFD.IP",
                    "level": 1.10120,
                    "stopLevel": 1.10000,
                    "limitLevel": 1.10320,
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    deal_id = client.open_position(
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.8,
        stop_loss=1.10000,
        take_profit=1.10320,
        comment="XTBBOT:TEST:ABC:EURUSD",
        entry_price=1.10120,
    )

    assert deal_id == "DIAAAAMARKET001"
    assert posted_payloads
    assert posted_payloads[0].get("orderType") == "MARKET"
    assert "quoteId" not in posted_payloads[0]
    assert "level" not in posted_payloads[0]


def test_ig_get_position_close_sync_falls_back_to_history_transactions(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWUKUH89AQ":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWUKUH89AQ failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            query = kwargs.get("query") or {}
            assert int(query.get("pageSize") or 0) == 200
            return (
                {
                    "transactions": [
                        {
                            "dealId": "OTHER-DEAL",
                            "closeLevel": 1.3400,
                            "profitAndLoss": -10.0,
                            "currency": "EUR",
                        },
                        {
                            "dealId": "DIAAAAWUKUH89AQ",
                            "closeLevel": 1.34116,
                            "profitAndLoss": -71.41,
                            "currency": "EUR",
                            "reference": "UKUM6MA9",
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync("DIAAAAWUKUH89AQ")
    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_transactions"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.34116)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(-71.41)
    assert str(sync.get("pnl_currency") or "") == "EUR"
    assert str(sync.get("history_reference") or "") == "UKUM6MA9"


def test_ig_get_position_close_sync_matches_history_by_deal_reference(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWVZQJVAAF":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWVZQJVAAF failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return (
                {
                    "transactions": [
                        {
                            "closeLevel": 1.3321,
                            "profitAndLoss": 36.38,
                            "currency": "EUR",
                            "reference": "VZQB3LA9",
                            "dateUtc": "2026-03-17T08:56:31",
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWVZQJVAAF",
        deal_reference="VZQB3LA9",
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_transactions"
    assert str(sync.get("history_match_mode") or "") == "deal_reference"
    assert str(sync.get("history_reference") or "") == "VZQB3LA9"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.3321)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(36.38)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 17, 8, 56, 31, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_matches_system_close_by_short_position_token(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWWZFAF8AS":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWWZFAF8AS failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return (
                {
                    "transactions": [
                        {
                            "dealId": "DIAAAAWWZFBFFA9",
                            "activityType": "Order",
                            "result": "Position/s closed: WZFAF8AS",
                            "marketName": "Germany 40 Cash (GBP1)",
                            "date": "19/03/26",
                            "time": "12:25",
                            "level": 22864.0,
                            "currency": "GBP",
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWWZFAF8AS",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_transactions"
    assert str(sync.get("history_match_mode") or "") == "position_id"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(22864.0)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 25, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_returns_none_without_exact_position_or_reference_match(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWVZLATE01":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWVZLATE01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return (
                {
                    "transactions": [
                        {
                            "marketName": "GBP/USD converted at 0.86442814",
                            "openLevel": 1.33149,
                            "closeLevel": 1.3321,
                            "profitAndLoss": 36.38,
                            "currency": "EUR",
                            "reference": "VZQB3LA9",
                            "dateUtc": "2026-03-17T08:56:31",
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWVZLATE01",
        deal_reference="NO-MATCH-REF",
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_positions_deal_id"
    assert sync.get("position_found") is False


def test_ig_get_position_close_sync_returns_none_when_position_is_still_open(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAAOPEN123":
            return (
                {
                    "position": {
                        "dealId": "DIAAAAOPEN123",
                        "dealReference": "OPENREF123",
                        "direction": "BUY",
                        "size": 0.5,
                        "level": 1.2345,
                    },
                    "market": {"epic": "CS.D.EURUSD.CFD.IP"},
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync("DIAAAAOPEN123")
    assert sync is None


def test_ig_get_position_close_sync_does_not_trust_cached_confirm_while_position_is_still_open(monkeypatch):
    client = _make_client()
    client._connected = True
    state = {"open": True}

    with client._lock:
        client._position_close_sync["DIAAAAOPEN124"] = {
            "position_id": "DIAAAAOPEN124",
            "source": "ig_confirm",
            "close_price": 1.2345,
            "realized_pnl": -12.3,
            "closed_at": datetime(2026, 3, 17, 10, 0, 0, tzinfo=timezone.utc).timestamp(),
            "_cache_ts": time.time(),
        }

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAAOPEN124":
            if state["open"]:
                return (
                    {
                        "position": {
                            "dealId": "DIAAAAOPEN124",
                            "dealReference": "OPENREF124",
                            "direction": "BUY",
                            "size": 0.5,
                            "level": 1.2345,
                        },
                        "market": {"epic": "CS.D.EURUSD.CFD.IP"},
                    },
                    {},
                )
            raise BrokerError(
                "IG API GET /positions/DIAAAAOPEN124 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    assert client.get_position_close_sync("DIAAAAOPEN124") is None
    with client._lock:
        assert "DIAAAAOPEN124" in client._position_close_sync  # type: ignore[attr-defined]

    state["open"] = False
    sync = client.get_position_close_sync("DIAAAAOPEN124")
    assert isinstance(sync, dict)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.2345)
    assert sync.get("position_found") is False


def test_ig_get_position_close_sync_matches_history_with_csv_style_keys(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAAWWZFAF8AS":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWWZFAF8AS failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return (
                {
                    "Transactions": [
                        {
                            "DealId": "DIAAAAWWZFBFFA9",
                            "ActivityType": "Order",
                            "Result": "Position/s closed: WZFAF8AS",
                            "MarketName": "Germany 40 Cash (GBP1)",
                            "Date": "19/03/26",
                            "Time": "12:25",
                            "Level": "22864",
                            "Currency": "GBP",
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWWZFAF8AS",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_transactions"
    assert str(sync.get("history_match_mode") or "") == "position_id"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(22864.0)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 25, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_falls_back_to_history_activity(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWWZFAF8AS":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWWZFAF8AS failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            return (
                {
                    "activities": [
                        {
                            "date": "2026-03-19T12:25:00",
                            "marketName": "Germany 40 Cash (GBP1)",
                            "currency": "£",
                            "details": {
                                "profitAndLoss": {"amount": "-71.41"},
                                "dealReference": "REF-CLOSE-1",
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAAWWZFAF8AS",
                                        "level": 22864.0,
                                    }
                                ],
                            },
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWWZFAF8AS",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_activity"
    assert str(sync.get("history_match_mode") or "") == "position_id"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(22864.0)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(-71.41)
    assert str(sync.get("pnl_currency") or "") == "GBP"
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 25, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_activity_aggregates_partial_closes(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAPARTIAL01":
            raise BrokerError(
                "IG API GET /positions/DIAAAAPARTIAL01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            return (
                {
                    "activities": [
                        {
                            "date": "2026-04-03T12:01:02",
                            "marketName": "US Tech 100 Cash (GBP1)",
                            "currency": "GBP",
                            "details": {
                                "profitAndLoss": {"amount": "2.91"},
                                "dealReference": "REF-PARTIAL-1",
                                "actions": [
                                    {
                                        "actionType": "POSITION_PARTIALLY_CLOSED",
                                        "dealId": "DIAAAAPARTIALCLOSE1",
                                        "affectedDealId": "DIAAAAPARTIAL01",
                                        "level": 23961.4,
                                    }
                                ],
                            },
                        },
                        {
                            "date": "2026-04-03T12:02:37",
                            "marketName": "US Tech 100 Cash (GBP1)",
                            "currency": "GBP",
                            "details": {
                                "profitAndLoss": {"amount": "0.57"},
                                "dealReference": "REF-PARTIAL-2",
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "dealId": "DIAAAAPARTIALCLOSE2",
                                        "affectedDealId": "DIAAAAPARTIAL01",
                                        "level": 23966.0,
                                    }
                                ],
                            },
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAPARTIAL01",
        symbol="US100",
        opened_at=datetime(2026, 4, 3, 10, 54, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_activity"
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(3.48)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(23966.0)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 4, 3, 12, 2, 37, tzinfo=timezone.utc).timestamp()
    )
    assert int(sync.get("partial_close_count") or 0) == 1


def test_ig_get_position_close_sync_caches_factual_history_payload(monkeypatch):
    client = _make_client()
    client._connected = True
    calls = {"positions": 0, "transactions": 0, "activity": 0}

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAACACHECLOSE1":
            calls["positions"] += 1
            raise BrokerError(
                "IG API GET /positions/DIAAAACACHECLOSE1 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            calls["transactions"] += 1
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            calls["activity"] += 1
            return (
                {
                    "activities": [
                        {
                            "date": "2026-03-19T12:25:00",
                            "marketName": "Germany 40 Cash (GBP1)",
                            "currency": "GBP",
                            "details": {
                                "dealReference": "REF-CLOSE-CACHED-1",
                                "profitAndLoss": "-36.38",
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAACACHECLOSE1",
                                        "level": 22864.0,
                                    }
                                ],
                            },
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    sync = client.get_position_close_sync(
        "DIAAAACACHECLOSE1",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )
    sync_again = client.get_position_close_sync(
        "DIAAAACACHECLOSE1",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert isinstance(sync_again, dict)
    assert float(sync_again.get("realized_pnl") or 0.0) == pytest.approx(-36.38)
    assert calls == {"positions": 1, "transactions": 2, "activity": 1}


def test_ig_get_position_close_sync_does_not_cache_incomplete_history_payload(monkeypatch):
    client = _make_client()
    client._connected = True
    calls = {"positions": 0, "transactions": 0, "activity": 0}

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAAINCOMPLETE01":
            calls["positions"] += 1
            raise BrokerError(
                "IG API GET /positions/DIAAAAINCOMPLETE01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            calls["transactions"] += 1
            if calls["transactions"] <= 2:
                return ({"transactions": []}, {})
            return (
                {
                    "transactions": [
                        {
                            "dealId": "DIAAAAINCOMPLETE01",
                            "closeLevel": "10324.0",
                            "profitAndLoss": "-463.50",
                            "currency": "GBP",
                            "dateUtc": "2026-04-02T12:48:47",
                        }
                    ]
                },
                {},
            )
        if method == "GET" and path == "/history/activity":
            calls["activity"] += 1
            return (
                {
                    "activities": [
                        {
                            "date": "2026-04-02T12:48:47",
                            "marketName": "FTSE 100 Cash (£10)",
                            "currency": "GBP",
                            "details": {
                                "dealReference": "REF-INCOMPLETE-1",
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAAINCOMPLETE01",
                                        "level": 10324.0,
                                    }
                                ],
                            },
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    sync = client.get_position_close_sync(
        "DIAAAAINCOMPLETE01",
        symbol="UK100",
        opened_at=datetime(2026, 4, 2, 12, 9, 27, tzinfo=timezone.utc).timestamp(),
    )
    sync_again = client.get_position_close_sync(
        "DIAAAAINCOMPLETE01",
        symbol="UK100",
        opened_at=datetime(2026, 4, 2, 12, 9, 27, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(-463.50)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(10324.0)
    assert isinstance(sync_again, dict)
    assert float(sync_again.get("realized_pnl") or 0.0) == pytest.approx(-463.50)
    assert str(sync_again.get("pnl_currency") or "") == "GBP"
    assert calls == {"positions": 1, "transactions": 3, "activity": 1}


def test_ig_get_position_close_sync_uses_activity_close_hints_to_match_transactions(monkeypatch):
    client = _make_client()
    client._connected = True
    calls = {"positions": 0, "transactions": 0, "activity": 0}

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAHINTMATCH01":
            calls["positions"] += 1
            raise BrokerError(
                "IG API GET /positions/DIAAAAHINTMATCH01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            calls["transactions"] += 1
            if calls["transactions"] == 1:
                return ({"transactions": []}, {})
            return (
                {
                    "transactions": [
                        {
                            "dealId": "DIAAAAHINTCLOSE1",
                            "closeLevel": "10324.0",
                            "profitAndLoss": "-463.50",
                            "currency": "GBP",
                            "dateUtc": "2026-04-02T12:48:47",
                            "marketName": "FTSE 100 Cash (£10)",
                        }
                    ]
                },
                {},
            )
        if method == "GET" and path == "/history/activity":
            calls["activity"] += 1
            return (
                {
                    "activities": [
                        {
                            "date": "2026-04-02T12:48:47",
                            "marketName": "FTSE 100 Cash (£10)",
                            "currency": "GBP",
                            "details": {
                                "dealReference": "REF-HINT-1",
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAAHINTMATCH01",
                                        "dealId": "DIAAAAHINTCLOSE1",
                                        "level": 10324.0,
                                    }
                                ],
                            },
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    sync = client.get_position_close_sync(
        "DIAAAAHINTMATCH01",
        symbol="UK100",
        tick_size=1.0,
        opened_at=datetime(2026, 4, 2, 12, 9, 27, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert float(sync.get("close_price") or 0.0) == pytest.approx(10324.0)
    assert float(sync.get("realized_pnl") or 0.0) == pytest.approx(-463.50)
    assert str(sync.get("pnl_currency") or "") == "GBP"
    assert str(sync.get("history_match_mode") or "") == "close_deal_id"
    assert calls == {"positions": 1, "transactions": 3, "activity": 1}


def test_ig_get_position_close_sync_history_transactions_falls_back_to_max_span(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions/DIAAAATRANSHIST01":
            raise BrokerError(
                "IG API GET /positions/DIAAAATRANSHIST01 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            query = kwargs.get("query") or {}
            if "from" in query:
                raise BrokerError("IG API GET /history/transactions failed: 400 error.request.invalid.date-range")
            assert int(query.get("maxSpanSeconds") or 0) == 172800
            return (
                {
                    "transactions": [
                        {
                            "dealId": "DIAAAATRANSHIST01",
                            "closeLevel": "1.3277",
                            "profitAndLoss": "-105.00",
                            "currency": "USD",
                            "dateUtc": "2026-03-19T12:30:00",
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAATRANSHIST01",
        symbol="GBPUSD",
        opened_at=datetime(2026, 3, 19, 12, 1, 0, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_transactions"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(1.3277)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 30, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_history_activity_v2_without_detailed_query_param(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAACTIVITY02":
            raise BrokerError(
                "IG API GET /positions/DIAAAAACTIVITY02 failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            version = str(kwargs.get("version") or "")
            query = kwargs.get("query") or {}
            if version == "3":
                raise BrokerError("IG API GET /history/activity failed: 403 endpoint.unavailable.for.api-key")
            if version == "2":
                assert "detailed" not in query
                if "from" in query:
                    raise BrokerError("IG API GET /history/activity failed: 400 error.request.invalid.date-range")
                assert int(query.get("maxSpanSeconds") or 0) == 172800
                return (
                    {
                        "activities": [
                            {
                                "dealId": "DIAAAAACTIVITY02",
                                "activityType": "Order",
                                "result": "Position/s closed: ACTIVITY02",
                                "date": "19/03/26",
                                "time": "12:30",
                                "level": "22864",
                                "currency": "GBP",
                                "marketName": "Germany 40 Cash (GBP1)",
                            }
                        ]
                    },
                    {},
                )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAACTIVITY02",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 0, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_activity"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(22864.0)
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 30, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_history_activity_prefers_close_over_open_activity(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWWZFAF8AS":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWWZFAF8AS failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            return (
                {
                    "activities": [
                        {
                            "date": "2026-03-19T12:01:01",
                            "dealId": "DIAAAAWWZFAF8AS",
                            "description": "Position opened",
                            "marketName": "Germany 40 Cash (GBP1)",
                            "details": {
                                "actions": [
                                    {
                                        "actionType": "POSITION_OPENED",
                                        "affectedDealId": "DIAAAAWWZFAF8AS",
                                        "level": 22928.2,
                                    }
                                ]
                            },
                        },
                        {
                            "date": "2026-03-19T12:25:00",
                            "dealId": "DIAAAAWWZFBFFA9",
                            "description": "Position closed",
                            "marketName": "Germany 40 Cash (GBP1)",
                            "details": {
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAAWWZFAF8AS",
                                        "level": 22864.0,
                                    }
                                ]
                            },
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWWZFAF8AS",
        symbol="DE40",
        opened_at=datetime(2026, 3, 19, 12, 1, 1, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_activity"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(22864.0)
    assert str(sync.get("close_deal_id") or "") == "DIAAAAWWZFBFFA9"
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 25, 0, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_position_close_sync_history_activity_ignores_rejected_sl_rows(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        if method == "GET" and path == "/positions/DIAAAAWXFK58GAS":
            raise BrokerError(
                "IG API GET /positions/DIAAAAWXFK58GAS failed: 404 Not Found "
                '{"errorCode":"error.position.notfound"}'
            )
        if method == "GET" and path == "/history/transactions":
            return ({"transactions": []}, {})
        if method == "GET" and path == "/history/activity":
            return (
                {
                    "activities": [
                        {
                            "Date": "23/03/26",
                            "Time": "17:44",
                            "ActivityType": "S&L",
                            "Result": "Rejected: Stop too close",
                            "ActionStatus": "REJECT",
                            "DealId": "DIAAAAWXFKKV9AV",
                            "TextEpic": "CC.D.CL.UNC.IP",
                            "Level": 8896.8,
                            "Stop": 8900.0,
                            "details": {
                                "actions": [
                                    {
                                        "actionType": "LIMIT_STOP_AMENDED",
                                        "affectedDealId": "DIAAAAWXFK58GAS",
                                        "level": 8900.0,
                                    }
                                ]
                            },
                        },
                        {
                            "Date": "23/03/26",
                            "Time": "17:44",
                            "ActivityType": "Order",
                            "Result": "Position/s closed: XFK58GAS",
                            "ActionStatus": "ACCEPT",
                            "DealId": "DIAAAAWXFKGA5AZ",
                            "TextEpic": "CC.D.CL.UNC.IP",
                            "Level": 8876.0,
                            "details": {
                                "actions": [
                                    {
                                        "actionType": "POSITION_CLOSED",
                                        "affectedDealId": "DIAAAAWXFK58GAS",
                                        "level": 8876.0,
                                    }
                                ]
                            },
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)
    sync = client.get_position_close_sync(
        "DIAAAAWXFK58GAS",
        symbol="WTI",
        opened_at=datetime(2026, 3, 23, 17, 44, 0, tzinfo=timezone.utc).timestamp(),
    )

    assert isinstance(sync, dict)
    assert str(sync.get("source") or "") == "ig_history_activity"
    assert float(sync.get("close_price") or 0.0) == pytest.approx(8876.0)
    assert str(sync.get("close_deal_id") or "") == "DIAAAAWXFKGA5AZ"


def test_ig_get_managed_open_positions_filters_by_known_deal_reference(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAMANAGED1",
                                "dealReference": "XTBBOTTEST01ABC1234567",
                                "direction": "BUY",
                                "size": 0.5,
                                "level": 1.2345,
                                "stopLevel": 1.2300,
                                "limitLevel": 1.2400,
                                "createdDateUTC": "2026/03/12 17:29:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        },
                        {
                            "position": {
                                "dealId": "OTHERPOS1",
                                "dealReference": "MANUALTRADE123",
                                "direction": "SELL",
                                "size": 1.0,
                                "level": 1.1111,
                            },
                            "market": {"epic": "CS.D.EURUSD.CFD.IP"},
                        },
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD", "EURUSD"],
        known_deal_references=["XTBBOTTEST01ABC1234567"],
    )

    assert set(restored.keys()) == {"DIAAAAMANAGED1"}
    position = restored["DIAAAAMANAGED1"]
    assert position.position_id == "DIAAAAMANAGED1"
    assert position.side == Side.BUY
    assert position.volume == pytest.approx(0.5)
    assert position.open_price == pytest.approx(1.2345)
    assert position.stop_loss == pytest.approx(1.2300)
    assert position.take_profit == pytest.approx(1.2400)
    assert position.opened_at == pytest.approx(datetime(2026, 3, 12, 17, 29, tzinfo=timezone.utc).timestamp())


def test_ig_get_managed_open_positions_parses_iso_timezone_opened_at(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAMANAGEDTZ1",
                                "dealReference": "XTBBOTTEST01TZ123456789",
                                "direction": "BUY",
                                "size": 0.5,
                                "level": 1.2345,
                                "stopLevel": 1.2300,
                                "limitLevel": 1.2400,
                                "createdDateUTC": "2026-03-12T17:29:00+00:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        known_deal_references=["XTBBOTTEST01TZ123456789"],
    )

    assert set(restored.keys()) == {"DIAAAAMANAGEDTZ1"}
    assert restored["DIAAAAMANAGEDTZ1"].opened_at == pytest.approx(
        datetime(2026, 3, 12, 17, 29, tzinfo=timezone.utc).timestamp()
    )


def test_ig_get_managed_open_positions_matches_retry_suffix_deal_reference(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAMANAGED2",
                                "dealReference": "XTBBOTTEST01ABC1234567_01",
                                "direction": "BUY",
                                "size": 0.5,
                                "level": 1.2345,
                                "stopLevel": 1.2300,
                                "limitLevel": 1.2400,
                                "createdDateUTC": "2026/03/12 17:29:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        known_deal_references=["XTBBOTTEST01ABC1234567"],
    )

    assert set(restored.keys()) == {"DIAAAAMANAGED2"}


def test_ig_get_managed_open_positions_prefers_requested_symbol_alias(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "DIAAAAGOLD001",
                                "dealReference": "XTBBOTTEST01GOLD12345",
                                "direction": "SELL",
                                "size": 0.2,
                                "level": 2925.5,
                            },
                            "market": {"epic": "CS.D.GOLD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GOLD"],
        known_deal_references=["XTBBOTTEST01GOLD12345"],
    )

    assert set(restored.keys()) == {"DIAAAAGOLD001"}
    assert restored["DIAAAAGOLD001"].symbol == "GOLD"


def test_ig_get_managed_open_positions_recovers_known_local_position_without_reference(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "VH39DRAH",
                                "direction": "BUY",
                                "size": 0.4,
                                "level": 1.3342,
                                "stopLevel": 1.3300,
                                "limitLevel": 1.3400,
                                "createdDateUTC": "2026/03/12 17:29:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        known_position_ids=["VH39DRAH"],
    )

    assert set(restored.keys()) == {"VH39DRAH"}
    position = restored["VH39DRAH"]
    assert position.position_id == "VH39DRAH"
    assert position.side == Side.BUY
    assert position.volume == pytest.approx(0.4)
    assert position.open_price == pytest.approx(1.3342)


def test_ig_get_managed_open_positions_recovers_pending_open_without_known_position_id(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "VH39DRAH",
                                "direction": "BUY",
                                "size": 0.4,
                                "level": 1.3342,
                                "stopLevel": 1.3300,
                                "limitLevel": 1.3400,
                                "createdDateUTC": "2026/03/12 17:29:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        if method == "GET" and path == "/confirms/XTBBOTTEST01ABC1234567":
            return (
                {
                    "dealStatus": "ACCEPTED",
                    "dealId": "VH39DRAH",
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        pending_opens=[
            PendingOpen(
                pending_id="XTBBOTTEST01ABC1234567",
                symbol="GBPUSD",
                side=Side.BUY,
                volume=0.4,
                entry=1.3342,
                stop_loss=1.3300,
                take_profit=1.3400,
                created_at=time.time(),
                thread_name="worker-GBPUSD",
                strategy="momentum",
                mode="execution",
            )
        ],
    )

    assert set(restored.keys()) == {"VH39DRAH"}
    assert restored["VH39DRAH"].position_id == "VH39DRAH"


def test_ig_get_managed_open_positions_can_include_unmatched_preferred_symbol(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "GET" and path == "/positions":
            return (
                {
                    "positions": [
                        {
                            "position": {
                                "dealId": "ORPHAN-GBP-1",
                                "dealReference": "UNTRACKEDREF123",
                                "direction": "BUY",
                                "size": 0.5,
                                "level": 1.2500,
                                "stopLevel": 1.2450,
                                "limitLevel": 1.2600,
                                "createdDateUTC": "2026/03/17 13:12:00",
                            },
                            "market": {"epic": "CS.D.GBPUSD.CFD.IP"},
                        }
                    ]
                },
                {},
            )
        raise AssertionError(f"Unexpected request: {method} {path}")

    monkeypatch.setattr(client, "_request", fake_request)

    strict_only = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        include_unmatched_preferred=False,
    )
    assert strict_only == {}

    restored = client.get_managed_open_positions(
        "XTBBOT",
        "TEST01",
        preferred_symbols=["GBPUSD"],
        include_unmatched_preferred=True,
    )
    assert set(restored.keys()) == {"ORPHAN-GBP-1"}
    assert restored["ORPHAN-GBP-1"].position_id == "ORPHAN-GBP-1"


def test_ig_rate_limit_worker_snapshot_tracks_request_and_block_counters(monkeypatch):
    client = _make_client()
    client._connected = True
    client._rate_limit_enabled = True  # type: ignore[attr-defined]
    client._rate_limit_account_non_trading_per_min = 1  # type: ignore[attr-defined]
    client._rate_limit_app_non_trading_per_min = 1  # type: ignore[attr-defined]
    client._rate_limit_account_trading_per_min = 100  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    request_ts: list[float] = []

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_sleep(sec: float) -> None:
        clock["ts"] += max(0.0, float(sec))

    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", fake_sleep)

    class _FakeResponse:
        length = 0

        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return b""

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = (req, timeout)
        request_ts.append(clock["ts"])
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    client._request("GET", "/accounts", version="1", auth=True)  # type: ignore[attr-defined]
    client._request("GET", "/accounts", version="1", auth=True)  # type: ignore[attr-defined]

    assert len(request_ts) == 2
    assert request_ts[1] - request_ts[0] >= 59.5

    snapshot = client.get_rate_limit_workers_snapshot()
    by_key = {str(item["worker_key"]): item for item in snapshot}

    app_non_trading = by_key["app_non_trading"]
    account_non_trading = by_key["account_non_trading"]
    account_trading = by_key["account_trading"]

    assert int(app_non_trading["blocked_total"]) >= 1
    assert int(account_non_trading["blocked_total"]) >= 1
    assert float(app_non_trading["last_request_ts"]) > 0
    assert float(account_non_trading["last_request_ts"]) > 0
    assert int(account_trading["blocked_total"]) == 0


def test_ig_rate_limit_worker_snapshot_tracks_historical_points(monkeypatch):
    client = _make_client()
    client._connected = True
    client._rate_limit_enabled = True  # type: ignore[attr-defined]
    client._historical_http_cache_enabled = False  # type: ignore[attr-defined]
    client._rate_limit_account_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_app_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_account_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_historical_points_per_week = 5  # type: ignore[attr-defined]

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: 1_700_000_000.0)
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: None)

    class _FakeResponse:
        length = 0

        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return b""

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", lambda req, timeout: _FakeResponse())

    client._request(  # type: ignore[attr-defined]
        "GET",
        "/prices/CS.D.EURUSD.CFD.IP",
        version="3",
        auth=False,
        query={"numPoints": 4},
    )
    with pytest.raises(BrokerError, match="historical price weekly allowance exceeded by local limiter"):
        client._request(
            "GET",
            "/prices/CS.D.EURUSD.CFD.IP",
            version="3",
            auth=False,
            query={"numPoints": 4},
        )

    snapshot = client.get_rate_limit_workers_snapshot()
    by_key = {str(item["worker_key"]): item for item in snapshot}
    historical = by_key["historical_points"]
    assert int(historical["used_value"]) == 4
    assert int(historical["remaining_value"]) == 1
    assert int(historical["blocked_total"]) >= 1


def test_ig_historical_http_cache_skips_duplicate_prices_calls(monkeypatch):
    client = _make_client()
    client._connected = True
    client._rate_limit_enabled = False  # type: ignore[attr-defined]
    client._historical_http_cache_enabled = True  # type: ignore[attr-defined]
    client._historical_http_cache_ttl_sec = 600.0  # type: ignore[attr-defined]

    call_count = {"urlopen": 0}

    class _FakeResponse:
        def __init__(self, body: str = "") -> None:
            encoded = body.encode("utf-8")
            self._raw = encoded
            self.length = len(encoded)
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = (req, timeout)
        call_count["urlopen"] += 1
        return _FakeResponse('{"prices":[{"snapshotTimeUTC":"2026-03-20T12:00:00","closePrice":{"bid":1.1,"ask":1.2}}]}')

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    first, _ = client._request(  # type: ignore[attr-defined]
        "GET",
        "/prices/CS.D.EURUSD.CFD.IP",
        version="3",
        auth=False,
        query={"resolution": "MINUTE", "max": 10},
    )
    second, _ = client._request(  # type: ignore[attr-defined]
        "GET",
        "/prices/CS.D.EURUSD.CFD.IP",
        version="3",
        auth=False,
        query={"resolution": "MINUTE", "max": 10},
    )

    assert call_count["urlopen"] == 1
    assert first == second


def test_ig_fetch_recent_price_history_parses_prices(monkeypatch):
    client = _make_client()
    client._connected = True
    client._rate_limit_enabled = False  # type: ignore[attr-defined]
    monkeypatch.setattr(client, "_epic_attempt_order", lambda symbol: ["IX.D.NASDAQ.CASH.IP"])
    activated: list[tuple[str, str]] = []
    monkeypatch.setattr(client, "_activate_epic", lambda symbol, epic: activated.append((symbol, epic)))

    def fake_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/prices/IX.D.NASDAQ.CASH.IP"
        assert kwargs.get("query", {}).get("resolution") == "MINUTE"
        return (
            {
                "prices": [
                    {
                        "snapshotTimeUTC": "2026-03-20T12:00:00",
                        "openPrice": {"bid": 19998.0, "ask": 19999.0},
                        "highPrice": {"bid": 20004.0, "ask": 20005.0},
                        "lowPrice": {"bid": 19995.0, "ask": 19996.0},
                        "closePrice": {"bid": 20000.0, "ask": 20001.0},
                        "lastTradedVolume": 12,
                    },
                    {
                        "snapshotTimeUTC": "2026-03-20T12:01:00",
                        "openPrice": {"bid": 20001.0, "ask": 20002.0},
                        "highPrice": {"bid": 20006.0, "ask": 20007.0},
                        "lowPrice": {"bid": 19999.0, "ask": 20000.0},
                        "closePrice": {"bid": 20002.0, "ask": 20003.0},
                        "lastTradedVolume": 14,
                    },
                ]
            },
            {},
        )

    monkeypatch.setattr(client, "_request", fake_request)

    rows = client.fetch_recent_price_history("US100", resolution="MINUTE", points=2)
    assert len(rows) == 2
    assert rows[0][1] == pytest.approx(20000.5)
    assert rows[1][1] == pytest.approx(20002.5)
    assert rows[0][2] == pytest.approx(12.0)
    assert rows[1][2] == pytest.approx(14.0)
    assert rows[0][3] == pytest.approx(19998.5)
    assert rows[0][4] == pytest.approx(20004.5)
    assert rows[0][5] == pytest.approx(19995.5)
    assert rows[1][3] == pytest.approx(20001.5)
    assert rows[1][4] == pytest.approx(20006.5)
    assert rows[1][5] == pytest.approx(19999.5)
    assert activated == [("US100", "IX.D.NASDAQ.CASH.IP")]


def test_ig_request_dispatch_routes_calls_to_four_worker_threads(monkeypatch):
    client = _make_client()
    client._connected = True
    client._request_dispatch_enabled = True  # type: ignore[attr-defined]
    client._rate_limit_enabled = False  # type: ignore[attr-defined]

    seen_threads_by_path: dict[str, str] = {}

    class _FakeResponse:
        def __init__(self, body: str = "") -> None:
            encoded = body.encode("utf-8")
            self._raw = encoded
            self.length = len(encoded)
            self.headers: dict[str, str] = {}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    def fake_urlopen(req, timeout):
        _ = timeout
        path = str(req.full_url).split("/gateway/deal", 1)[-1]
        seen_threads_by_path[path] = threading.current_thread().name
        return _FakeResponse("{}")

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    client._request("GET", "/markets/CS.D.EURUSD.CFD.IP", version="3", auth=True)  # type: ignore[attr-defined]
    client._request("GET", "/accounts", version="1", auth=True)  # type: ignore[attr-defined]
    client._request("POST", "/positions/otc", payload={"epic": "IX.D.NASDAQ.CASH.IP"}, version="2", auth=False)  # type: ignore[attr-defined]
    client._request(  # type: ignore[attr-defined]
        "GET",
        "/prices/CS.D.EURUSD.CFD.IP",
        version="3",
        auth=False,
        query={"numPoints": 1},
    )

    try:
        assert seen_threads_by_path["/markets/CS.D.EURUSD.CFD.IP"].startswith("ig-req-app_non_trading")
        assert seen_threads_by_path["/accounts"].startswith("ig-req-account_non_trading")
        assert seen_threads_by_path["/positions/otc"].startswith("ig-req-account_trading")
        # Query string can be appended by urllib on GET /prices.
        matched_prices_paths = [path for path in seen_threads_by_path if path.startswith("/prices/CS.D.EURUSD.CFD.IP")]
        assert matched_prices_paths
        assert seen_threads_by_path[matched_prices_paths[0]].startswith("ig-req-historical_points")
    finally:
        client.close()
