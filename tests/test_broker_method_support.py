from __future__ import annotations

import pytest

from xtb_bot.broker_method_support import call_broker_method_with_supported_kwargs


def test_call_broker_method_with_supported_kwargs_filters_unsupported_keywords():
    class _Broker:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None, bool]] = []

        def get_position_close_sync(
            self,
            position_id: str,
            *,
            deal_reference: str | None = None,
            include_history: bool = True,
        ) -> dict[str, object]:
            self.calls.append((position_id, deal_reference, include_history))
            return {"ok": True}

    broker = _Broker()

    payload = call_broker_method_with_supported_kwargs(
        broker,
        "get_position_close_sync",
        broker.get_position_close_sync,
        "DIAAAATEST001",
        deal_reference="REF-123",
        include_history=False,
        symbol="EURUSD",
        opened_at=10.0,
    )

    assert payload == {"ok": True}
    assert broker.calls == [("DIAAAATEST001", "REF-123", False)]


def test_call_broker_method_with_supported_kwargs_rebuilds_cache_when_callable_changes():
    class _Broker:
        pass

    broker = _Broker()
    first_calls: list[str] = []
    second_calls: list[tuple[str, str | None]] = []

    def _first(position_id: str) -> dict[str, object]:
        first_calls.append(position_id)
        return {"version": 1}

    def _second(position_id: str, *, deal_reference: str | None = None) -> dict[str, object]:
        second_calls.append((position_id, deal_reference))
        return {"version": 2}

    broker.get_position_close_sync = _first  # type: ignore[attr-defined]
    first_payload = call_broker_method_with_supported_kwargs(
        broker,
        "get_position_close_sync",
        broker.get_position_close_sync,  # type: ignore[attr-defined]
        "DIAAAATEST002",
        deal_reference="REF-FIRST",
    )

    broker.get_position_close_sync = _second  # type: ignore[attr-defined]
    second_payload = call_broker_method_with_supported_kwargs(
        broker,
        "get_position_close_sync",
        broker.get_position_close_sync,  # type: ignore[attr-defined]
        "DIAAAATEST003",
        deal_reference="REF-SECOND",
    )

    assert first_payload == {"version": 1}
    assert second_payload == {"version": 2}
    assert first_calls == ["DIAAAATEST002"]
    assert second_calls == [("DIAAAATEST003", "REF-SECOND")]


def test_call_broker_method_with_supported_kwargs_does_not_mask_internal_type_error():
    class _Broker:
        def get_position_close_sync(
            self,
            position_id: str,
            *,
            deal_reference: str | None = None,
        ) -> dict[str, object]:
            raise TypeError(f"internal close sync failure for {position_id} / {deal_reference}")

    broker = _Broker()

    with pytest.raises(TypeError, match="internal close sync failure"):
        call_broker_method_with_supported_kwargs(
            broker,
            "get_position_close_sync",
            broker.get_position_close_sync,
            "DIAAAATEST004",
            deal_reference="REF-ERR",
            include_history=True,
            symbol="EURUSD",
        )
