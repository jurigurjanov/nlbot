from __future__ import annotations

import threading

import pytest

from xtb_bot.config import RiskConfig
from xtb_bot.models import AccountSnapshot, SymbolSpec
from xtb_bot.risk_manager import RiskManager, SlotDecision
from xtb_bot.state_store import StateStore


@pytest.fixture
def store(tmp_path):
    db = StateStore(tmp_path / "risk.db")
    try:
        yield db
    finally:
        db.close()


def test_volume_calculated_from_symbol_spec(store):
    cfg = RiskConfig(max_risk_per_trade_pct=1.0, min_stop_loss_pips=10)
    manager = RiskManager(cfg, store)

    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=9_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
    )

    # 1% risk = 100 USD, SL = 50 ticks, risk per lot = 500 USD => 0.20 lot.
    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(0.20)


def test_trade_blocked_when_volume_below_instrument_min(store):
    cfg = RiskConfig(
        start_balance=5_000,
        max_risk_per_trade_pct=0.1,
        min_stop_loss_pips=10,
        max_total_drawdown_pct=50.0,
    )
    manager = RiskManager(cfg, store)

    snapshot = AccountSnapshot(balance=5_000, equity=5_000, margin_free=5_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="XAUUSD",
        tick_size=0.1,
        tick_value=5.0,
        contract_size=100.0,
        lot_min=1.0,
        lot_max=50.0,
        lot_step=1.0,
        price_precision=2,
        lot_precision=0,
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="XAUUSD",
        open_positions_count=0,
        entry=2030.0,
        stop_loss=2028.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert decision.suggested_volume == 0.0
    assert "below instrument minimum" in decision.reason


def test_volume_uses_balance_as_risk_base(store):
    cfg = RiskConfig(start_balance=10_000, max_risk_per_trade_pct=1.0, min_stop_loss_pips=10)
    manager = RiskManager(cfg, store)

    snapshot = AccountSnapshot(
        balance=10_000,  # realized base for risk
        equity=12_000,   # higher equity should not increase per-trade risk
        margin_free=12_000,
        timestamp=1.0,
    )
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
    )

    # Risk base = 10_000 -> risk 100 USD -> 50 ticks * 10 USD = 500 USD/lot -> 0.20 lot.
    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(0.20)


def test_volume_sizing_accounts_for_current_spread(store):
    cfg = RiskConfig(max_risk_per_trade_pct=1.0, min_stop_loss_pips=1)
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US500",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
    )

    no_spread = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US500",
        open_positions_count=0,
        entry=100.0,
        stop_loss=99.0,
        symbol_spec=spec,
        current_spread_pips=0.0,
    )
    with_spread = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US500",
        open_positions_count=0,
        entry=100.0,
        stop_loss=99.0,
        symbol_spec=spec,
        current_spread_pips=4.0,
    )

    assert no_spread.allowed is True
    assert with_spread.allowed is True
    assert no_spread.suggested_volume > with_spread.suggested_volume
    assert no_spread.suggested_volume == pytest.approx(100.0)
    assert with_spread.suggested_volume == pytest.approx(20.0)


def test_margin_precheck_blocks_when_free_margin_is_insufficient(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=100.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=100.0,
        stop_loss=99.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert decision.suggested_volume == 0.0
    assert "Insufficient free margin" in decision.reason


def test_margin_precheck_applies_configured_overhead_pct(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.0,
        margin_fallback_leverage=20.0,
        margin_overhead_pct=10.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=52.0, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={"leverage": 20.0},
    )

    # Base required margin without overhead: 50
    # With overhead 10%: 55 -> should block when free margin is 52.
    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=10.0,
        stop_loss=9.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert "Insufficient free margin" in decision.reason
    assert "overhead_pct:10.00" in decision.reason


def test_margin_precheck_applies_holiday_multiplier(store, monkeypatch):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.0,
        margin_fallback_leverage=20.0,
        margin_holiday_multiplier=1.2,
        margin_holiday_dates_utc=("2026-12-25",),
    )
    manager = RiskManager(cfg, store)
    monkeypatch.setattr(manager, "_is_holiday_margin_window", lambda: True)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=55.0, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={"leverage": 20.0},
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=10.0,
        stop_loss=9.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert "Insufficient free margin" in decision.reason
    assert "holiday_multiplier:1.20" in decision.reason


def test_margin_precheck_blocks_when_post_open_reserve_is_breached(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.0,
        margin_fallback_leverage=20.0,
        margin_min_free_after_open=25.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=70.0, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={"leverage": 20.0},
    )

    # Required margin: 50 -> free_after_open: 20, reserve: 25 -> must block.
    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=10.0,
        stop_loss=9.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert "Free margin reserve would be breached" in decision.reason


def test_margin_precheck_can_be_disabled(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=False,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=100.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=100.0,
        stop_loss=99.0,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(100.0)


def test_margin_precheck_uses_fallback_for_non_positive_symbol_leverage(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=20_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={"leverage": 0.0},
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=100.0,
        stop_loss=99.0,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(100.0)


def test_margin_precheck_normalizes_scaled_ig_fx_quote_for_margin(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=1.0,  # scaled quote mode (pip = 1.0 point in price stream)
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.5,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={"broker": "ig", "leverage": 0.0},
    )

    # Entry is scaled (11520.0 means ~1.15200). Without normalization,
    # margin pre-check would require tens of millions and always block.
    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=0,
        entry=11_520.0,
        stop_loss=11_510.0,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(1.0)


def test_margin_precheck_applies_account_currency_conversion_rate(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.0,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=55.0, timestamp=1.0)
    spec = SymbolSpec(
        symbol="US100",
        tick_size=1.0,
        tick_value=1.0,
        contract_size=1.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={
            "leverage": 20.0,
            "account_currency_conversion_rate": 1.2,
        },
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="US100",
        open_positions_count=0,
        entry=10.0,
        stop_loss=9.0,
        symbol_spec=spec,
    )

    assert decision.allowed is False
    assert "Insufficient free margin" in decision.reason
    assert "conversion:account_currency_conversion_rate:1.2" in decision.reason


def test_margin_precheck_normalizes_scaled_fx_quote_without_broker_metadata(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=1.0,  # scaled quote mode, but metadata is incomplete
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.5,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=1,
        lot_precision=2,
        metadata={},  # no broker/margin scale metadata
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=0,
        entry=11_520.0,
        stop_loss=11_510.0,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume == pytest.approx(1.0)


def test_margin_precheck_normalizes_scaled_fx_quote_by_magnitude_heuristic(store):
    cfg = RiskConfig(
        max_risk_per_trade_pct=1.0,
        min_stop_loss_pips=1,
        margin_check_enabled=True,
        margin_safety_buffer=1.15,
        margin_fallback_leverage=20.0,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,  # looks unscaled, while entry is clearly scaled
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
        metadata={},
    )

    decision = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=0,
        entry=13_169.0,
        stop_loss=13_168.997,
        symbol_spec=spec,
    )

    assert decision.allowed is True
    assert decision.suggested_volume > 0.0


def test_daily_drawdown_lock_blocks_until_next_day(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
        min_stop_loss_pips=10,
    )
    manager = RiskManager(cfg, store)

    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
    )

    monkeypatch.setattr(manager, "_current_day", lambda: "2026-02-26")

    # Initialize day anchors.
    decision_ok = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert decision_ok.allowed is True

    # Hit 5% daily drawdown threshold: 10_000 -> 9_500.
    decision_lock = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=9_500, margin_free=9_500, timestamp=2.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert decision_lock.allowed is False
    assert "locked until 2026-02-27 00:00:00" in decision_lock.reason

    lock_events = [event for event in store.load_events() if event["message"] == "Daily drawdown lock activated"]
    assert lock_events
    lock_payload = lock_events[0]["payload"]
    assert lock_payload["unlock_at"] == "2026-02-27 00:00:00"
    assert "daily drawdown" in lock_payload["reason"].lower()

    # Still blocked same day even if drawdown recovers below threshold.
    decision_same_day = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=9_800, margin_free=9_800, timestamp=3.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert decision_same_day.allowed is False
    assert "lock active until 2026-02-27 00:00:00" in decision_same_day.reason

    # Next day the lock resets automatically.
    monkeypatch.setattr(manager, "_current_day", lambda: "2026-02-27")
    decision_next_day = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=9_800, equity=9_800, margin_free=9_800, timestamp=4.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert decision_next_day.allowed is True

    release_events = [event for event in store.load_events() if event["message"] == "Daily drawdown lock released"]
    assert release_events


def test_max_open_positions_emits_alert_once_while_limit_active(store):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_total_drawdown_pct=50.0,
        max_open_positions=5,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)

    blocked_one = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=5,
        entry=1.1000,
        stop_loss=1.0950,
    )
    assert blocked_one.allowed is False
    assert blocked_one.reason == "Max open positions reached"

    blocked_two = manager.can_open_trade(
        snapshot=snapshot,
        symbol="EURUSD",
        open_positions_count=6,
        entry=1.1000,
        stop_loss=1.0950,
    )
    assert blocked_two.allowed is False

    alerts = [
        event
        for event in store.load_events()
        if event["message"] == "Trade blocked: max open positions limit reached"
    ]
    assert len(alerts) == 1
    assert alerts[0]["payload"]["max_open_positions"] == 5


def test_daily_drawdown_uses_equity_hwm(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
        min_stop_loss_pips=10,
    )
    manager = RiskManager(cfg, store)

    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
    )

    monkeypatch.setattr(manager, "_current_day", lambda: "2026-02-26")

    first = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert first.allowed is True

    # New intraday equity high watermark.
    second = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=11_000, margin_free=11_000, timestamp=2.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert second.allowed is True

    # Drawdown from HWM: 11_000 -> 10_450 = 5%.
    third = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=10_450, margin_free=10_450, timestamp=3.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=spec,
    )
    assert third.allowed is False
    assert "locked until 2026-02-27 00:00:00" in third.reason


def test_can_open_trade_blocks_when_symbol_spec_missing(store):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_total_drawdown_pct=50.0,
        max_open_positions=5,
    )
    manager = RiskManager(cfg, store)

    decision = manager.can_open_trade(
        snapshot=AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0),
        symbol="EURUSD",
        open_positions_count=0,
        entry=1.1000,
        stop_loss=1.0950,
        symbol_spec=None,
    )
    assert decision.allowed is False
    assert decision.reason == "Symbol specification unavailable"


def test_open_slot_acquire_release_blocks_until_released(store):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_open_positions=1,
    )
    manager = RiskManager(cfg, store)

    first = manager.try_acquire_open_slot(symbol="EURUSD", open_positions_count=0)
    assert first.acquired is True
    assert first.reservation_id is not None
    assert manager.pending_open_slots() == 1

    second = manager.try_acquire_open_slot(symbol="GBPUSD", open_positions_count=0)
    assert second.acquired is False
    assert second.reason == "Max open positions reached"
    assert manager.pending_open_slots() == 1

    manager.release_open_slot(first.reservation_id)
    assert manager.pending_open_slots() == 0

    third = manager.try_acquire_open_slot(symbol="USDJPY", open_positions_count=0)
    assert third.acquired is True
    manager.release_open_slot(third.reservation_id)


def test_can_open_trade_blocks_when_pending_slots_reach_limit(store):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_open_positions=1,
    )
    manager = RiskManager(cfg, store)
    snapshot = AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    spec = SymbolSpec(
        symbol="EURUSD",
        tick_size=0.0001,
        tick_value=10.0,
        contract_size=100_000.0,
        lot_min=0.01,
        lot_max=100.0,
        lot_step=0.01,
        price_precision=5,
        lot_precision=2,
    )

    slot = manager.try_acquire_open_slot(symbol="EURUSD", open_positions_count=0)
    assert slot.acquired is True
    assert slot.reservation_id is not None

    try:
        decision = manager.can_open_trade(
            snapshot=snapshot,
            symbol="GBPUSD",
            open_positions_count=0,
            entry=1.1000,
            stop_loss=1.0950,
            symbol_spec=spec,
        )
        assert decision.allowed is False
        assert decision.reason == "Max open positions reached"
    finally:
        manager.release_open_slot(slot.reservation_id)


def test_open_slot_acquire_is_atomic_under_threads(store):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_open_positions=1,
    )
    manager = RiskManager(cfg, store)
    start_barrier = threading.Barrier(2)
    outcomes: list[SlotDecision] = []
    outcomes_lock = threading.Lock()

    def _attempt(symbol: str) -> None:
        start_barrier.wait()
        result: SlotDecision = manager.try_acquire_open_slot(symbol=symbol, open_positions_count=0)
        with outcomes_lock:
            outcomes.append(result)

    t1 = threading.Thread(target=_attempt, args=("EURUSD",))
    t2 = threading.Thread(target=_attempt, args=("GBPUSD",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert sum(1 for item in outcomes if item.acquired) == 1


def test_compute_stats_rebases_external_cashflow_when_no_open_positions(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
        external_cashflow_rebase_enabled=True,
        external_cashflow_rebase_min_abs=100.0,
        external_cashflow_rebase_min_pct=1.0,
    )
    manager = RiskManager(cfg, store)
    monkeypatch.setattr(manager, "_current_day", lambda: "2026-03-18")

    initial = manager.compute_stats(
        AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0),
        open_positions_count=0,
    )
    assert initial.daily_pnl == pytest.approx(0.0)

    rebased = manager.compute_stats(
        AccountSnapshot(balance=12_000, equity=12_000, margin_free=12_000, timestamp=2.0),
        open_positions_count=0,
    )
    assert rebased.daily_pnl == pytest.approx(0.0)
    assert rebased.daily_drawdown_pct == pytest.approx(0.0)

    events = [event for event in store.load_events() if event["message"] == "Risk anchors rebased for external cashflow"]
    assert events
    assert float(events[0]["payload"]["external_cashflow_rebased"]) == pytest.approx(2_000.0)


def test_compute_stats_does_not_rebase_external_cashflow_with_open_positions(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
        external_cashflow_rebase_enabled=True,
        external_cashflow_rebase_min_abs=100.0,
        external_cashflow_rebase_min_pct=1.0,
    )
    manager = RiskManager(cfg, store)
    monkeypatch.setattr(manager, "_current_day", lambda: "2026-03-18")

    manager.compute_stats(
        AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0),
        open_positions_count=1,
    )
    unchanged = manager.compute_stats(
        AccountSnapshot(balance=12_000, equity=12_000, margin_free=12_000, timestamp=2.0),
        open_positions_count=1,
    )
    assert unchanged.daily_pnl == pytest.approx(2_000.0)

def test_daily_hwm_anchor_keeps_max_equity_under_threads(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
        min_stop_loss_pips=10,
    )
    manager = RiskManager(cfg, store)
    monkeypatch.setattr(manager, "_current_day", lambda: "2026-03-13")

    manager.compute_stats(
        AccountSnapshot(balance=10_000, equity=10_000, margin_free=10_000, timestamp=1.0)
    )

    barrier = threading.Barrier(2)

    def _update(snapshot: AccountSnapshot) -> None:
        barrier.wait()
        manager.compute_stats(snapshot)

    high_thread = threading.Thread(
        target=_update,
        args=(AccountSnapshot(balance=10_000, equity=11_000, margin_free=11_000, timestamp=2.0),),
    )
    low_thread = threading.Thread(
        target=_update,
        args=(AccountSnapshot(balance=10_000, equity=10_500, margin_free=10_500, timestamp=3.0),),
    )
    high_thread.start()
    low_thread.start()
    high_thread.join()
    low_thread.join()

    assert float(store.get_kv("risk.daily_hwm_equity") or 0.0) == pytest.approx(11_000.0)


def test_should_force_flatten_ignores_when_no_open_positions(store, monkeypatch):
    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_daily_drawdown_pct=5.0,
    )
    manager = RiskManager(cfg, store)
    day = "2026-02-26"
    monkeypatch.setattr(manager, "_current_day", lambda: day)

    store.set_kv("risk.day", day)
    store.set_kv("risk.daily_locked_day", day)
    store.set_kv("risk.daily_lock_until", "2026-02-27 00:00:00")
    store.set_kv("risk.daily_lock_reason", "test lock")

    snapshot = AccountSnapshot(balance=10_000, equity=9_000, margin_free=8_000, timestamp=1.0)

    should_flatten, reason = manager.should_force_flatten(snapshot, open_positions_count=0)
    assert should_flatten is False
    assert reason == ""

    should_flatten_with_positions, _ = manager.should_force_flatten(snapshot, open_positions_count=1)
    assert should_flatten_with_positions is True


def test_open_slot_acquire_is_atomic_across_store_instances(tmp_path):
    db_path = tmp_path / "shared-risk.db"
    store_a = StateStore(db_path)
    store_b = StateStore(db_path)

    cfg = RiskConfig(
        start_balance=10_000,
        max_risk_per_trade_pct=1.0,
        max_open_positions=1,
    )
    manager_a = RiskManager(cfg, store_a)
    manager_b = RiskManager(cfg, store_b)
    manager_a._open_slot_lease_sec = 60.0
    manager_b._open_slot_lease_sec = 60.0

    try:
        barrier = threading.Barrier(2)
        outcomes: list[SlotDecision] = []
        outcomes_lock = threading.Lock()

        def _attempt(manager: RiskManager, symbol: str) -> None:
            barrier.wait()
            result = manager.try_acquire_open_slot(symbol=symbol, open_positions_count=0)
            with outcomes_lock:
                outcomes.append(result)

        t1 = threading.Thread(target=_attempt, args=(manager_a, "EURUSD"))
        t2 = threading.Thread(target=_attempt, args=(manager_b, "GBPUSD"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert sum(1 for item in outcomes if item.acquired) == 1
        assert sum(1 for item in outcomes if not item.acquired) == 1

        for item in outcomes:
            if item.reservation_id:
                manager_a.release_open_slot(item.reservation_id)
    finally:
        store_a.close()
        store_b.close()
