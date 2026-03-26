from __future__ import annotations

from datetime import datetime, timezone
import io
import logging
import threading
import time
from urllib import error as urlerror

import pytest

from xtb_bot.client import BrokerError
from xtb_bot.ig_client import IgApiClient, _is_fx_pair_symbol
from xtb_bot.models import AccountType, PendingOpen, Position, PriceTick, Side, SymbolSpec


class _AliveThread:
    def is_alive(self) -> bool:
        return True


def _make_client() -> IgApiClient:
    return IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
        account_id="ABC123",
        rest_market_min_interval_sec=0.0,
    )


def test_ig_stream_line_updates_tick_cache_with_delta_fields():
    client = _make_client()
    # Use the new _update_tick_from_stream_update method directly
    client._update_tick_from_stream_update("EURUSD", "1.1000", "1.1002", "1710000000000")
    first = client._tick_cache["EURUSD"]
    assert first.bid == pytest.approx(1.1)
    assert first.ask == pytest.approx(1.1002)

    # Second update with only ask changed
    client._update_tick_from_stream_update("EURUSD", None, "1.1003", None)
    second = client._tick_cache["EURUSD"]
    # bid stays 0 (None -> 0) but ask provides fallback
    assert second.ask == pytest.approx(1.1003)
    assert second.bid == pytest.approx(1.1003)  # bid=0 falls back to ask


def test_ig_stream_line_parses_hhmmss_timestamp(monkeypatch):
    client = _make_client()
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now_dt.timestamp())

    client._update_tick_from_stream_update("EURUSD", "1.1000", "1.1002", "165458")
    tick = client._tick_cache["EURUSD"]
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert tick.timestamp == pytest.approx(expected)


def test_ig_stream_line_parses_hms_text_timestamp(monkeypatch):
    client = _make_client()
    now_dt = datetime(2026, 3, 16, 17, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: now_dt.timestamp())

    client._update_tick_from_stream_update("EURUSD", "1.1000", "1.1002", "16:54:58")
    tick = client._tick_cache["EURUSD"]
    expected = datetime(2026, 3, 16, 16, 54, 58, tzinfo=timezone.utc).timestamp()
    assert tick.timestamp == pytest.approx(expected)


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


def test_ig_get_price_updates_stream_hit_counters(monkeypatch):
    client = _make_client()
    client._connected = True
    client._stream_enabled = True  # type: ignore[attr-defined]
    client._stream_connected = True
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
    client._stream_connected = True
    client._stream_desired_subscriptions.add("EURUSD")
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
    assert client._stream_endpoint == "https://demo-apd.marketdatasystems.com"  # type: ignore[attr-defined]


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
    # Simulate an active subscription for EURUSD
    client._stream_subscriptions["EURUSD"] = object()  # type: ignore[attr-defined]

    client._activate_epic("EURUSD", "CS.D.EURUSD.CFD.IP")

    assert client._symbol_spec_cache["EURUSD"] is spec  # type: ignore[attr-defined]
    # Epic unchanged → subscription should still be present
    assert "EURUSD" in client._stream_subscriptions  # type: ignore[attr-defined]


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
    assert spec.tick_value == pytest.approx(10.0)
    assert spec.contract_size == pytest.approx(100000.0)


def test_ig_get_symbol_spec_infers_margin_price_scale_for_scaled_fx_quote(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = (method, path, kwargs)
        return (
            {
                "instrument": {
                    "contractSize": 100000.0,
                    "valueOfOnePip": 10.0,
                    # onePipMeans is missing for this market variant.
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
    assert spec.metadata.get("margin_price_scale_divisor") == pytest.approx(10_000.0)


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
        == "ERROR.SERVICE.OTC.INVALID_SIZE"
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


def test_is_fx_pair_symbol_rejects_same_base_quote():
    assert _is_fx_pair_symbol("EURUSD") is True
    assert _is_fx_pair_symbol("USDUSD") is False


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
        assert calls[0][0] == "POST"  # DELETE with body → POST + _method:DELETE header
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
        _ = kwargs
        if method == "DELETE" and path == "/positions/otc":
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
    assert client.get_position_close_sync(position.position_id) is None


def test_ig_close_position_uses_delete_method(monkeypatch):
    client = _make_client()
    client._connected = True
    calls: list[tuple[str, str]] = []

    def fake_request(method, path, **kwargs):
        calls.append((method, path))
        if method == "DELETE" and path == "/positions/otc":
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

    assert ("DELETE", "/positions/otc") in calls


def test_ig_close_position_supports_partial_volume(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        payload = kwargs.get("payload") or {}
        if method == "DELETE" and path == "/positions/otc":
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


def test_ig_close_position_raises_when_confirm_rejected(monkeypatch):
    client = _make_client()
    client._connected = True

    def fake_request(method, path, **kwargs):
        _ = kwargs
        if method == "DELETE" and path == "/positions/otc":
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


def test_ig_stream_update_ignores_zero_prices():
    """When both bid and ask are zero/None, tick cache should not be updated."""
    client = _make_client()
    client._update_tick_from_stream_update("EURUSD", None, None, None)
    assert "EURUSD" not in client._tick_cache

    client._update_tick_from_stream_update("EURUSD", "0", "0", None)
    assert "EURUSD" not in client._tick_cache

    # Valid update should work
    client._update_tick_from_stream_update("EURUSD", "1.1", "1.1002", None)
    assert "EURUSD" in client._tick_cache
    assert client._tick_cache["EURUSD"].bid == pytest.approx(1.1)
    assert client._tick_cache["EURUSD"].ask == pytest.approx(1.1002)


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
                                "size": 3.44,
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


def test_ig_open_position_prefers_account_currency_before_instrument_default(monkeypatch):
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
    assert seen_payloads[0].get("currencyCode") == "EUR"


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
                if currency == "EUR":
                    return {"dealReference": "ref-ftse-daily-eur"}, {}
                if currency == "USD":
                    return {"dealReference": "ref-ftse-daily-usd"}, {}
                if currency == "GBP":
                    return {"dealReference": "ref-ftse-daily-gbp"}, {}
                raise AssertionError(f"Unexpected currency for daily epic: {currency}")
            if epic == "IX.D.FTSE.CASH.IP":
                return {"dealReference": "ref-ftse-cash"}, {}
            if epic == "IX.D.FTSE.IFS.IP":
                return {"dealReference": "ref-ftse-ifs"}, {}
            raise AssertionError(f"Unexpected epic payload: {epic}")
        if method == "GET" and path == "/confirms/ref-ftse-daily-eur":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY",
                    "status": "ERROR",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
        if method == "GET" and path == "/confirms/ref-ftse-daily-usd":
            return (
                {
                    "dealStatus": "REJECTED",
                    "reason": "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY",
                    "status": "ERROR",
                    "epic": "IX.D.FTSE.DAILY.IP",
                },
                {},
            )
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
    assert ("GET", "/confirms/ref-ftse-daily-eur") in requests
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
    assert posted_payloads[0].get("quoteId") == "QUOTE-ABC-123"
    assert float(posted_payloads[0].get("level") or 0.0) == pytest.approx(1.1015)


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
                            "currency": "GBP",
                            "details": {
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
    assert float(sync.get("closed_at") or 0.0) == pytest.approx(
        datetime(2026, 3, 19, 12, 25, 0, tzinfo=timezone.utc).timestamp()
    )


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
