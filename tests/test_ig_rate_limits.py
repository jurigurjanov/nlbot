from __future__ import annotations

from urllib import request as urllib_request

import pytest

from xtb_bot.client import BrokerError
from xtb_bot.ig_client import IgApiClient
from xtb_bot.models import AccountType


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


def _make_client() -> IgApiClient:
    client = IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
        account_id="ABC123",
        rest_market_min_interval_sec=0.0,
    )
    client._connected = True  # type: ignore[attr-defined]
    return client


def test_ig_request_rate_limit_enforces_global_non_trading_per_min(monkeypatch):
    client = _make_client()
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

    def fake_urlopen(req: urllib_request.Request, timeout: float):
        _ = (req, timeout)
        request_ts.append(clock["ts"])
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    client._request("GET", "/accounts", version="1", auth=True)  # type: ignore[attr-defined]
    client._request("GET", "/accounts", version="1", auth=True)  # type: ignore[attr-defined]

    assert len(request_ts) == 2
    assert request_ts[1] - request_ts[0] >= 59.5


def test_ig_request_rate_limit_enforces_trading_per_min(monkeypatch):
    client = _make_client()
    client._rate_limit_enabled = True  # type: ignore[attr-defined]
    client._rate_limit_account_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_app_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_account_trading_per_min = 1  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    request_ts: list[float] = []

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])

    def fake_sleep(sec: float) -> None:
        clock["ts"] += max(0.0, float(sec))

    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", fake_sleep)

    def fake_urlopen(req: urllib_request.Request, timeout: float):
        _ = (req, timeout)
        request_ts.append(clock["ts"])
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

    client._request("POST", "/positions/otc", payload={"epic": "IX.D.NASDAQ.CASH.IP"}, version="2", auth=False)  # type: ignore[attr-defined]
    client._request("POST", "/positions/otc", payload={"epic": "IX.D.NASDAQ.CASH.IP"}, version="2", auth=False)  # type: ignore[attr-defined]

    assert len(request_ts) == 2
    assert request_ts[1] - request_ts[0] >= 59.5


def test_ig_request_rate_limit_blocks_historical_points_over_week_limit(monkeypatch):
    client = _make_client()
    client._rate_limit_enabled = True  # type: ignore[attr-defined]
    client._rate_limit_account_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_app_non_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_account_trading_per_min = 1000  # type: ignore[attr-defined]
    client._rate_limit_historical_points_per_week = 5  # type: ignore[attr-defined]

    clock = {"ts": 1_700_000_000.0}
    request_count = {"n": 0}

    monkeypatch.setattr("xtb_bot.ig_client.time.time", lambda: clock["ts"])
    monkeypatch.setattr("xtb_bot.ig_client.time.sleep", lambda sec: None)

    def fake_urlopen(req: urllib_request.Request, timeout: float):
        _ = (req, timeout)
        request_count["n"] += 1
        return _FakeResponse()

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", fake_urlopen)

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

    assert request_count["n"] == 1
