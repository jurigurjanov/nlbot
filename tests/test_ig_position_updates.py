from __future__ import annotations

import io
from urllib import error as urlerror

import pytest

from xtb_bot.client import BrokerError
from xtb_bot.ig_client import IgApiClient
from xtb_bot.models import AccountType


def _make_client(callback):
    client = IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
        account_id="ABC123",
        rest_market_min_interval_sec=0.0,
        position_update_callback=callback,
    )
    client._connected = True  # type: ignore[attr-defined]
    return client


def test_request_direct_records_successful_position_update(monkeypatch):
    updates: list[dict[str, object]] = []
    client = _make_client(updates.append)

    class _FakeResponse:
        status = 200

        def __init__(self, raw: str) -> None:
            self._raw = raw.encode("utf-8")
            self.length = len(self._raw)
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    monkeypatch.setattr(
        "xtb_bot.ig_client.request.urlopen",
        lambda req, timeout: _FakeResponse('{"dealReference":"REF-1","dealId":"DIAAAA1","epic":"CS.D.GBPUSD.CFD.IP"}'),
    )

    body, _ = client._request_direct(
        "POST",
        "/positions/otc",
        payload={"epic": "CS.D.GBPUSD.CFD.IP", "size": 0.3},
        version="2",
        auth=False,
    )

    assert body.get("dealId") == "DIAAAA1"
    assert len(updates) == 1
    update = updates[0]
    assert str(update.get("operation")) == "open_position"
    assert str(update.get("method")) == "POST"
    assert str(update.get("path")) == "/positions/otc"
    assert str(update.get("symbol")) == "GBPUSD"
    assert str(update.get("position_id")) == "DIAAAA1"
    assert str(update.get("deal_reference")) == "REF-1"
    assert bool(update.get("success")) is True
    assert int(update.get("http_status") or 0) == 200


def test_request_direct_records_failed_position_update(monkeypatch):
    updates: list[dict[str, object]] = []
    client = _make_client(updates.append)

    def _raise_http_error(req, timeout):
        _ = (req, timeout)
        raise urlerror.HTTPError(
            url="https://demo-api.ig.com/gateway/deal/positions/otc",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"errorCode":"validation.null-not-allowed.request"}'),
        )

    monkeypatch.setattr("xtb_bot.ig_client.request.urlopen", _raise_http_error)

    with pytest.raises(BrokerError, match="validation.null-not-allowed.request"):
        client._request_direct(
            "DELETE",
            "/positions/otc",
            payload={"dealId": "DIAAAA2"},
            version="1",
            auth=False,
        )

    assert len(updates) == 1
    update = updates[0]
    assert str(update.get("operation")) == "close_position"
    assert str(update.get("position_id")) == "DIAAAA2"
    assert bool(update.get("success")) is False
    assert int(update.get("http_status") or 0) == 400
    assert "validation.null-not-allowed.request" in str(update.get("error_text"))
    response = update.get("response")
    assert isinstance(response, dict)
    assert str(response.get("errorCode")) == "validation.null-not-allowed.request"


def test_request_direct_classifies_post_with_dealid_as_close_position(monkeypatch):
    updates: list[dict[str, object]] = []
    client = _make_client(updates.append)

    class _FakeResponse:
        status = 200

        def __init__(self, raw: str) -> None:
            self._raw = raw.encode("utf-8")
            self.length = len(self._raw)
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return self._raw

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            _ = (exc_type, exc, tb)
            return False

    monkeypatch.setattr(
        "xtb_bot.ig_client.request.urlopen",
        lambda req, timeout: _FakeResponse('{"dealReference":"REF-CLOSE-1"}'),
    )

    client._request_direct(
        "POST",
        "/positions/otc",
        payload={"dealId": "DIAAAA-CLOSE-1", "direction": "SELL", "size": 0.2},
        version="1",
        auth=False,
        extra_headers={"_method": "DELETE", "X-HTTP-Method-Override": "DELETE"},
    )

    assert len(updates) == 1
    assert str(updates[0].get("operation")) == "close_position"
