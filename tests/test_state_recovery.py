from __future__ import annotations

import json

import pytest

from xtb_bot.bot import TradingBot
from xtb_bot.config import BotConfig, RiskConfig
from xtb_bot.models import AccountType, PendingOpen, Position, RunMode, Side
from xtb_bot.state_store import StateStore


def test_state_store_restores_only_open_positions(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        open_position = Position(
            position_id="paper-open",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1,
            stop_loss=1.095,
            take_profit=1.11,
            opened_at=10.0,
            status="open",
        )
        closed_position = Position(
            position_id="paper-closed",
            symbol="GBPUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.26,
            stop_loss=1.265,
            take_profit=1.25,
            opened_at=11.0,
            status="closed",
            close_price=1.255,
            closed_at=12.0,
            pnl=50.0,
        )

        store.upsert_trade(open_position, "worker-EURUSD", "momentum", "paper")
        store.upsert_trade(closed_position, "worker-GBPUSD", "momentum", "paper")

        restored = store.load_open_positions()
        assert set(restored.keys()) == {"paper-open"}
        assert restored["paper-open"].position_id == "paper-open"
        assert restored["paper-open"].entry_confidence == 0.0
    finally:
        store.close()


def test_state_store_persists_entry_confidence(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="paper-open",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1,
            stop_loss=1.095,
            take_profit=1.11,
            opened_at=10.0,
            entry_confidence=0.73,
            status="open",
        )

        store.upsert_trade(position, "worker-EURUSD", "momentum", "paper")

        restored = store.load_open_positions()
        assert restored["paper-open"].entry_confidence == 0.73
    finally:
        store.close()


def test_state_store_filters_open_positions_by_mode(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        paper_position = Position(
            position_id="paper-open",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1,
            stop_loss=1.095,
            take_profit=1.11,
            opened_at=10.0,
            status="open",
        )
        execution_position = Position(
            position_id="deal-exec-1",
            symbol="GBPUSD",
            side=Side.SELL,
            volume=0.2,
            open_price=1.26,
            stop_loss=1.265,
            take_profit=1.25,
            opened_at=11.0,
            status="open",
        )

        store.upsert_trade(paper_position, "worker-EURUSD", "momentum", "paper")
        store.upsert_trade(execution_position, "worker-GBPUSD", "momentum", "execution")

        restored_paper = store.load_open_positions(mode="paper")
        restored_execution = store.load_open_positions(mode="execution")

        assert set(restored_paper.keys()) == {"paper-open"}
        assert restored_paper["paper-open"].position_id == "paper-open"
        assert set(restored_execution.keys()) == {"deal-exec-1"}
        assert restored_execution["deal-exec-1"].position_id == "deal-exec-1"
    finally:
        store.close()


def test_bot_restores_positions_on_start(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    persisted = Position(
        position_id="paper-open-1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1,
        stop_loss=1.095,
        take_profit=1.11,
        opened_at=10.0,
        status="open",
    )
    bot.store.upsert_trade(persisted, "worker-EURUSD", "momentum", "paper")

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()
    restored = bot.position_book.get("EURUSD")
    assert restored is not None
    assert restored.position_id == "paper-open-1"

    bot.stop()


def test_bot_restores_positions_only_for_current_mode(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        symbols=["EURUSD", "GBPUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-exec.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="paper-open-eurusd",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1,
            stop_loss=1.095,
            take_profit=1.11,
            opened_at=10.0,
            status="open",
        ),
        "worker-EURUSD",
        "momentum",
        "execution",
    )
    bot.store.upsert_trade(
        Position(
            position_id="deal-exec-gbpusd",
            symbol="GBPUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.26,
            stop_loss=1.265,
            take_profit=1.25,
            opened_at=11.0,
            status="open",
        ),
        "worker-GBPUSD",
        "momentum",
        "execution",
    )

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()
    assert bot.position_book.get("EURUSD") is None
    restored_exec = bot.position_book.get("GBPUSD")
    assert restored_exec is not None
    assert restored_exec.position_id == "deal-exec-gbpusd"
    bot.stop()


def test_bot_recovers_managed_ig_positions_on_start(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-ig.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    recovered_position = Position(
        position_id="DIAAAARECOVER1",
        symbol="GBPUSD",
        side=Side.SELL,
        volume=0.2,
        open_price=1.2650,
        stop_loss=1.2700,
        take_profit=1.2550,
        opened_at=11.0,
        status="open",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert magic_prefix == bot.bot_magic_prefix
            assert magic_instance == bot.bot_magic_instance
            assert preferred_symbols is not None
            assert known_deal_references == []
            assert known_position_ids == []
            assert pending_opens == []
            assert include_unmatched_preferred is True
            return {"GBPUSD": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    restored = bot.position_book.get("GBPUSD")
    assert restored is not None
    assert restored.position_id == "DIAAAARECOVER1"
    assert bot._symbols_to_run() == ["EURUSD", "GBPUSD"]

    stored = bot.store.load_open_positions(mode="execution")
    assert "DIAAAARECOVER1" in stored
    assert stored["DIAAAARECOVER1"].position_id == "DIAAAARECOVER1"

    summary_events = [event for event in bot.store.load_events(limit=20) if event.get("message") == "Open position restore summary"]
    assert summary_events
    payload = summary_events[0].get("payload") or {}
    assert str(payload.get("source") or "") == "broker_sync"
    assert int(payload.get("broker_open_count") or 0) == 1
    assert int(payload.get("recovered_count") or 0) == 1
    assert int(payload.get("stale_local_count") or 0) == 0

    bot.stop()


def test_bot_marks_local_execution_position_missing_when_absent_on_broker(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-missing.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAASTALE01",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=10.0,
            status="open",
        ),
        "worker-EURUSD",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert known_deal_references == []
            assert known_position_ids == ["DIAAAASTALE01"]
            assert pending_opens == []
            assert include_unmatched_preferred is True
            return {}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    assert bot.position_book.get("EURUSD") is None
    assert bot.store.load_open_positions(mode="execution") == {}

    record = bot.store.get_trade_record("DIAAAASTALE01")
    assert record is not None
    assert str(record.get("status") or "") == "missing_on_broker"

    events = bot.store.load_events(limit=10)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position missing on broker during startup sync" in messages
    assert "Open position restore summary" in messages

    bot.stop()


def test_bot_keeps_local_execution_position_when_broker_matches_by_known_position_id(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["GBPUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-known-id.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="VH39DRAH",
            symbol="GBPUSD",
            side=Side.BUY,
            volume=0.2,
            open_price=1.3342,
            stop_loss=1.3300,
            take_profit=1.3400,
            opened_at=10.0,
            status="open",
        ),
        "worker-GBPUSD",
        "momentum",
        "execution",
    )

    recovered_position = Position(
        position_id="VH39DRAH",
        symbol="GBPUSD",
        side=Side.BUY,
        volume=0.2,
        open_price=1.3342,
        stop_loss=1.3300,
        take_profit=1.3400,
        opened_at=11.0,
        status="open",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert known_deal_references == []
            assert known_position_ids == ["VH39DRAH"]
            assert pending_opens == []
            assert include_unmatched_preferred is True
            return {"GBPUSD": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    restored = bot.position_book.get("GBPUSD")
    assert restored is not None
    assert restored.position_id == "VH39DRAH"

    record = bot.store.get_trade_record("VH39DRAH")
    assert record is not None
    assert str(record.get("status") or "") == "open"

    events = bot.store.load_events(limit=10)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position missing on broker during startup sync" not in messages

    bot.stop()


def test_bot_recovers_pending_execution_open_written_before_trade_insert(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["GBPUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-pending.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_pending_open(
        PendingOpen(
            pending_id="XTBBOTTEST01ABC1234567",
            symbol="GBPUSD",
            side=Side.BUY,
            volume=0.2,
            entry=1.3342,
            stop_loss=1.3300,
            take_profit=1.3400,
            created_at=10.0,
            thread_name="worker-GBPUSD",
            strategy="momentum",
            mode="execution",
            entry_confidence=0.77,
            trailing_override={
                "trailing_activation_ratio": 0.5,
                "trailing_distance_pips": 12.0,
            },
        )
    )

    recovered_position = Position(
        position_id="VH39DRAH",
        symbol="GBPUSD",
        side=Side.BUY,
        volume=0.2,
        open_price=1.3342,
        stop_loss=1.3300,
        take_profit=1.3400,
        opened_at=11.0,
        status="open",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert known_position_ids == []
            assert known_deal_references == ["XTBBOTTEST01ABC1234567"]
            assert pending_opens is not None
            assert len(pending_opens) == 1
            assert pending_opens[0].pending_id == "XTBBOTTEST01ABC1234567"
            assert include_unmatched_preferred is True
            pending_opens[0].position_id = "VH39DRAH"
            return {"GBPUSD": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    restored = bot.position_book.get("GBPUSD")
    assert restored is not None
    assert restored.position_id == "VH39DRAH"
    assert restored.entry_confidence == 0.77

    record = bot.store.get_trade_record("VH39DRAH")
    assert record is not None
    assert str(record.get("strategy") or "") == "momentum"
    assert float(record.get("entry_confidence") or 0.0) == 0.77

    assert bot.store.load_pending_opens(mode="execution") == []
    trailing_raw = bot.store.get_kv("worker.trailing_override.GBPUSD")
    assert trailing_raw is not None
    trailing_payload = json.loads(trailing_raw)
    assert str(trailing_payload.get("position_id") or "") == "VH39DRAH"

    bot.stop()


def test_sync_open_positions_returns_summary(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-sync.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    recovered_position = Position(
        position_id="DIAAAASYNC001",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1010,
        stop_loss=1.0960,
        take_profit=1.1110,
        opened_at=10.0,
        status="open",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            _ = (magic_prefix, magic_instance, preferred_symbols)
            assert known_deal_references == []
            assert known_position_ids == []
            assert pending_opens == []
            assert include_unmatched_preferred is True
            return {"EURUSD": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    summary = bot.sync_open_positions()

    assert str(summary.get("source") or "") == "broker_sync"
    assert int(summary.get("final_open_count") or 0) == 1
    assert int(summary.get("broker_open_count") or 0) == 1
    assert bot.position_book.get("EURUSD") is not None

    bot.stop()


def test_runtime_sync_recovers_broker_open_position_missing_in_local_store(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="mean_reversion_bb",
        strategy_params={"stop_loss_pips": 20, "take_profit_pips": 40},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-sync.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    recovered_position = Position(
        position_id="DIAAAAWWCW23YA4",
        symbol="AUS200",
        side=Side.SELL,
        volume=3.44,
        open_price=8655.8,
        stop_loss=8676.0,
        take_profit=8615.0,
        opened_at=20.0,
        status="open",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert magic_prefix == bot.bot_magic_prefix
            assert magic_instance == bot.bot_magic_instance
            assert preferred_symbols is not None
            assert include_unmatched_preferred is True
            return {"AUS200": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=True)

    restored = bot.position_book.get("AUS200")
    assert restored is not None
    assert restored.position_id == "DIAAAAWWCW23YA4"

    record = bot.store.get_trade_record("DIAAAAWWCW23YA4")
    assert record is not None
    assert str(record.get("status") or "") == "open"

    events = bot.store.load_events(limit=20)
    messages = [str(event.get("message") or "") for event in events]
    assert "Recovered broker-managed open position during runtime sync" in messages

    bot.stop()


def test_runtime_sync_marks_local_open_position_missing_when_absent_on_broker(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="mean_reversion_bb",
        strategy_params={"stop_loss_pips": 20, "take_profit_pips": 40},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-sync-stale.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    stale_position = Position(
        position_id="DIAAAAWWSTALE001",
        symbol="AUS200",
        side=Side.BUY,
        volume=1.0,
        open_price=8600.0,
        stop_loss=8550.0,
        take_profit=8650.0,
        opened_at=10.0,
        status="open",
    )
    bot.position_book.upsert(stale_position)
    bot.store.upsert_trade(stale_position, "worker-AUS200", bot.config.strategy, bot.config.mode.value)

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
            assert magic_prefix == bot.bot_magic_prefix
            assert magic_instance == bot.bot_magic_instance
            assert preferred_symbols is not None
            assert include_unmatched_preferred is True
            return {}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=True)

    assert bot.position_book.get("AUS200") is None
    record = bot.store.get_trade_record("DIAAAAWWSTALE001")
    assert record is not None
    assert str(record.get("status") or "") == "missing_on_broker"
    assert float(record.get("closed_at") or 0.0) > 0.0

    events = bot.store.load_events(limit=30)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position missing on broker during runtime sync" in messages
    assert "Runtime broker open-position sync marked stale local positions" in messages

    bot.stop()


def test_startup_sync_reconciles_local_missing_position_as_closed_from_broker_sync(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-startup-close-sync.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAASTALECLOSE01",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=10.0,
            status="open",
        ),
        "worker-EURUSD",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_history_transactions",
                "close_price": 1.0975,
                "realized_pnl": -25.0,
                "closed_at": 50.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    assert bot.position_book.get("EURUSD") is None
    record = bot.store.get_trade_record("DIAAAASTALECLOSE01")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(1.0975)
    assert float(record.get("pnl") or 0.0) == pytest.approx(-25.0)

    events = bot.store.load_events(limit=20)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position reconciled as closed during startup sync" in messages

    bot.stop()


def test_runtime_sync_reconciles_local_missing_position_as_closed_from_broker_sync(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="mean_reversion_bb",
        strategy_params={"stop_loss_pips": 20, "take_profit_pips": 40},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-close-sync.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    stale_position = Position(
        position_id="DIAAAAWWSTALE002",
        symbol="AUS200",
        side=Side.BUY,
        volume=1.0,
        open_price=8600.0,
        stop_loss=8550.0,
        take_profit=8650.0,
        opened_at=10.0,
        status="open",
    )
    bot.position_book.upsert(stale_position)
    bot.store.upsert_trade(stale_position, "worker-AUS200", bot.config.strategy, bot.config.mode.value)

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_history_transactions",
                "close_price": 8585.0,
                "closed_at": 25.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=True)

    assert bot.position_book.get("AUS200") is None
    record = bot.store.get_trade_record("DIAAAAWWSTALE002")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(8585.0)
    assert float(record.get("closed_at") or 0.0) > 0.0

    events = bot.store.load_events(limit=30)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position reconciled as closed during runtime sync" in messages

    bot.stop()


def test_sync_open_positions_backfills_missing_on_broker_as_closed(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["DE40"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-backfill-missing.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAAMISSINGB001",
            symbol="DE40",
            side=Side.SELL,
            volume=1.0,
            open_price=22900.0,
            stop_loss=22980.0,
            take_profit=22840.0,
            opened_at=10.0,
            status="missing_on_broker",
            closed_at=20.0,
            pnl=0.0,
        ),
        "worker-DE40",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_positions_deal_id",
                "position_found": False,
                "close_price": 22870.0,
                "realized_pnl": 30.0,
                "closed_at": 30.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    summary = bot.sync_open_positions()

    assert int(summary.get("missing_on_broker_reconciled_count") or 0) == 1
    record = bot.store.get_trade_record("DIAAAAMISSINGB001")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(22870.0)
    assert float(record.get("pnl") or 0.0) == pytest.approx(30.0)

    events = bot.store.load_events(limit=20)
    messages = [str(event.get("message") or "") for event in events]
    assert "Local open position reconciled as closed during missing_on_broker backfill" in messages
    assert "Missing-on-broker trades reconciled as closed" in messages

    bot.stop()


def test_sync_open_positions_backfills_closed_trade_details(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["GBPUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-backfill-closed-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAACLOSEDDET01",
            symbol="GBPUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.3300,
            stop_loss=1.3270,
            take_profit=1.3330,
            opened_at=10.0,
            status="closed",
            close_price=None,
            closed_at=None,
            pnl=0.0,
        ),
        "worker-GBPUSD",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_history_transactions",
                "close_price": 1.3277,
                "realized_pnl": -105.0,
                "closed_at": 123.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    summary = bot.sync_open_positions()

    assert int(summary.get("closed_details_reconciled_count") or 0) == 1
    record = bot.store.get_trade_record("DIAAAACLOSEDDET01")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(1.3277)
    assert float(record.get("pnl") or 0.0) == pytest.approx(-105.0)
    assert float(record.get("closed_at") or 0.0) == pytest.approx(123.0)

    events = bot.store.load_events(limit=20)
    messages = [str(event.get("message") or "") for event in events]
    assert "Closed trade details backfilled from broker sync" in messages
    assert "Closed trade details backfilled" in messages

    bot.stop()


def test_sync_open_positions_closes_missing_on_broker_without_synthetic_close_fields(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["DE40"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-missing-no-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAAMISSINGB002",
            symbol="DE40",
            side=Side.SELL,
            volume=1.0,
            open_price=22900.0,
            stop_loss=22980.0,
            take_profit=22840.0,
            opened_at=10.0,
            status="missing_on_broker",
            closed_at=20.0,
            pnl=0.0,
        ),
        "worker-DE40",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_positions_deal_id",
                "position_found": False,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    summary = bot.sync_open_positions()

    assert int(summary.get("missing_on_broker_reconciled_count") or 0) == 1
    record = bot.store.get_trade_record("DIAAAAMISSINGB002")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert record.get("close_price") is None
    assert float(record.get("closed_at") or 0.0) == pytest.approx(20.0)

    bot.stop()


def test_sync_open_positions_backfills_suspect_inferred_closed_details(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["US500"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-backfill-suspect-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAASUSPECT01",
            symbol="US500",
            side=Side.SELL,
            volume=4.0,
            open_price=6577.43,
            stop_loss=6598.0,
            take_profit=6537.0,
            opened_at=1000.0,
            status="closed",
            close_price=6577.43,
            closed_at=1000.0,
            pnl=-3.40,
        ),
        "worker-US500",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_managed_open_positions(
            self,
            magic_prefix,
            magic_instance,
            preferred_symbols=None,
            known_deal_references=None,
            known_position_ids=None,
            pending_opens=None,
            include_unmatched_preferred=False,
        ):
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

        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_history_activity",
                "close_price": 6598.0,
                "closed_at": 1300.0,
                "realized_pnl": -3.40,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    summary = bot.sync_open_positions()
    assert int(summary.get("closed_details_reconciled_count") or 0) == 1

    record = bot.store.get_trade_record("DIAAAASUSPECT01")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(6598.0)
    assert float(record.get("closed_at") or 0.0) == pytest.approx(1300.0)
    assert float(record.get("pnl") or 0.0) == pytest.approx(-3.40)

    bot.stop()
