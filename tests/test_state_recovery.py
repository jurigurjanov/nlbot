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


def test_fast_start_skips_heavy_startup_warmups_for_execution_db_first(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        broker="ig",
        api_key="ig-key",
        mode=RunMode.EXECUTION,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-fast-start.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    calls: list[str] = []

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: calls.append("connect"))
    monkeypatch.setattr(
        TradingBot,
        "_restore_open_positions",
        lambda self: ({}, {"source": "state_store", "mode": self.config.mode.value}),
    )
    monkeypatch.setattr(TradingBot, "_log_open_position_restore_summary", lambda self, summary: calls.append("log"))
    monkeypatch.setattr(TradingBot, "_seed_startup_history_for_fast_start", lambda self: calls.append("seed_history"))
    monkeypatch.setattr(TradingBot, "_start_db_first_cache_workers", lambda self: calls.append("db_first_workers"))
    monkeypatch.setattr(
        TradingBot,
        "_prime_db_first_account_snapshot_before_workers",
        lambda self: calls.append("prime_account"),
    )
    monkeypatch.setattr(TradingBot, "_reconcile_workers", lambda self, now_utc=None: calls.append("reconcile"))
    monkeypatch.setattr(TradingBot, "_start_worker_health_thread", lambda self: calls.append("health"))
    monkeypatch.setattr(
        TradingBot,
        "_preload_symbol_specs_on_startup",
        lambda self: (_ for _ in ()).throw(AssertionError("startup symbol preload must be deferred")),
    )
    monkeypatch.setattr(
        TradingBot,
        "_prime_db_first_caches",
        lambda self: (_ for _ in ()).throw(AssertionError("db-first prime must be deferred")),
    )
    monkeypatch.setattr(
        TradingBot,
        "_pre_warm_history",
        lambda self: (_ for _ in ()).throw(AssertionError("history prewarm must be deferred")),
    )
    monkeypatch.setattr(
        TradingBot,
        "_backfill_missing_on_broker_trades",
        lambda self: (_ for _ in ()).throw(AssertionError("missing-on-broker backfill must be deferred")),
    )
    monkeypatch.setattr(
        TradingBot,
        "_backfill_closed_trade_details",
        lambda self: (_ for _ in ()).throw(AssertionError("closed trade backfill must be deferred")),
    )

    bot.start()

    assert bot._startup_fast_path_enabled() is True
    assert calls == ["connect", "log", "seed_history", "db_first_workers", "prime_account", "reconcile", "health"]

    events = bot.store.load_events(limit=10)
    assert any(event.get("message") == "Startup fast-path enabled" for event in events)

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

    recovered_position = Position(
        position_id="deal-exec-gbpusd",
        symbol="GBPUSD",
        side=Side.SELL,
        volume=0.1,
        open_price=1.26,
        stop_loss=1.265,
        take_profit=1.25,
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
            assert known_position_ids == ["deal-exec-gbpusd"]
            return {"GBPUSD": recovered_position}

        def close(self):
            return None

    bot = TradingBot(config)
    bot.broker = _FakeBroker()  # type: ignore[assignment]
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


def test_bot_start_raises_when_execution_startup_broker_sync_fails(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "bot-sync-fail.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    class _FailingBroker:
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
            raise RuntimeError("managed sync failed")

        def close(self):
            return None

    bot.broker = _FailingBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    try:
        with pytest.raises(RuntimeError, match="Execution startup requires live broker position sync"):
            bot.start()
        events = bot.store.load_events(limit=10)
        assert any(
            str(event.get("message") or "") == "Execution startup aborted: live broker position sync unavailable"
            for event in events
        )
    finally:
        bot.store.close()


def test_bot_start_raises_when_execution_startup_broker_sync_is_deferred(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "bot-sync-deferred.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 30.0)

    try:
        with pytest.raises(RuntimeError, match="Execution startup requires live broker position sync"):
            bot.start()
        events = bot.store.load_events(limit=10)
        aborted = [
            event for event in events
            if str(event.get("message") or "") == "Execution startup aborted: live broker position sync unavailable"
        ]
        assert aborted
        payload = aborted[0].get("payload") or {}
        assert str(payload.get("status") or "") == "deferred_allowance_backoff"
    finally:
        bot.store.close()


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


def test_bot_recovers_pending_execution_open_with_multi_strategy_entry_metadata(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="momentum",
        strategy_params={
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 10,
            "take_profit_pips": 20,
            "multi_strategy_names": "momentum,g1",
        },
        strategy_params_map={
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "stop_loss_pips": 10,
                "take_profit_pips": 20,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-pending-multi.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_pending_open(
        PendingOpen(
            pending_id="XTBBOTTEST01CARRIERPEND1",
            symbol="AUS200",
            side=Side.BUY,
            volume=1.0,
            entry=8600.0,
            stop_loss=8550.0,
            take_profit=8650.0,
            created_at=10.0,
            thread_name="worker-AUS200",
            strategy="multi_strategy",
            strategy_entry="momentum",
            strategy_entry_component="g1",
            strategy_entry_signal="ema_cross_up",
            mode="execution",
            entry_confidence=0.81,
        )
    )

    recovered_position = Position(
        position_id="DIAAAAPENDINGMULTI01",
        symbol="AUS200",
        side=Side.BUY,
        volume=1.0,
        open_price=8600.0,
        stop_loss=8550.0,
        take_profit=8650.0,
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
            _ = (
                magic_prefix,
                magic_instance,
                preferred_symbols,
                known_position_ids,
            )
            assert known_deal_references == ["XTBBOTTEST01CARRIERPEND1"]
            assert include_unmatched_preferred is True
            assert pending_opens is not None
            assert len(pending_opens) == 1
            assert pending_opens[0].strategy == "multi_strategy"
            assert pending_opens[0].strategy_entry == "momentum"
            assert pending_opens[0].strategy_entry_component == "g1"
            pending_opens[0].position_id = "DIAAAAPENDINGMULTI01"
            return {"AUS200": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    record = bot.store.get_trade_record("DIAAAAPENDINGMULTI01")
    assert record is not None
    assert str(record.get("strategy") or "") == "multi_strategy"
    assert str(record.get("strategy_entry") or "") == "momentum"
    assert str(record.get("strategy_entry_component") or "") == "g1"
    assert str(record.get("strategy_entry_signal") or "") == "ema_cross_up"
    assert float(record.get("entry_confidence") or 0.0) == 0.81

    events = bot.store.load_events(limit=20)
    recovered_events = [
        event
        for event in events
        if event.get("message") == "Recovered broker-managed open position on startup"
    ]
    assert recovered_events
    payload = recovered_events[0].get("payload") or {}
    assert payload.get("strategy") == "multi_strategy"
    assert payload.get("strategy_base") == "momentum"
    assert payload.get("strategy_entry") == "momentum"
    assert payload.get("strategy_entry_component") == "g1"
    assert payload.get("strategy_entry_signal") == "ema_cross_up"

    assert bot.store.load_pending_opens(mode="execution") == []

    bot.stop()


def test_bot_startup_recovery_uses_base_strategy_entry_for_multi_strategy_carrier(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="momentum",
        strategy_params={
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 10,
            "take_profit_pips": 20,
            "multi_strategy_names": "momentum,g1",
        },
        strategy_params_map={
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "stop_loss_pips": 10,
                "take_profit_pips": 20,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-startup-multi.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    recovered_position = Position(
        position_id="DIAAAASTARTMULTI01",
        symbol="AUS200",
        side=Side.BUY,
        volume=1.0,
        open_price=8600.0,
        stop_loss=8550.0,
        take_profit=8650.0,
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
            _ = (
                magic_prefix,
                magic_instance,
                preferred_symbols,
                known_deal_references,
                known_position_ids,
                pending_opens,
            )
            assert include_unmatched_preferred is True
            return {"AUS200": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_start_workers", lambda self: None)

    bot.start()

    record = bot.store.get_trade_record("DIAAAASTARTMULTI01")
    assert record is not None
    assert str(record.get("strategy") or "") == "multi_strategy"
    assert str(record.get("strategy_entry") or "") == "momentum"

    events = bot.store.load_events(limit=20)
    recovered_events = [
        event
        for event in events
        if event.get("message") == "Recovered broker-managed open position on startup"
    ]
    assert recovered_events
    payload = recovered_events[0].get("payload") or {}
    assert payload.get("strategy") == "multi_strategy"
    assert payload.get("strategy_base") == "momentum"
    assert payload.get("strategy_entry") == "momentum"

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


def test_runtime_sync_recovery_uses_base_strategy_entry_for_multi_strategy_carrier(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["AUS200"],
        strategy="momentum",
        strategy_params={
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 10,
            "take_profit_pips": 20,
            "multi_strategy_names": "momentum,g1",
        },
        strategy_params_map={
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "stop_loss_pips": 10,
                "take_profit_pips": 20,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-multi.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)

    recovered_position = Position(
        position_id="DIAAAARUNTIMEMULT01",
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
            _ = (
                magic_prefix,
                magic_instance,
                preferred_symbols,
                known_deal_references,
                known_position_ids,
                pending_opens,
            )
            assert include_unmatched_preferred is True
            return {"AUS200": recovered_position}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=True)

    record = bot.store.get_trade_record("DIAAAARUNTIMEMULT01")
    assert record is not None
    assert str(record.get("strategy") or "") == "multi_strategy"
    assert str(record.get("strategy_entry") or "") == "momentum"

    events = bot.store.load_events(limit=20)
    recovered_events = [
        event
        for event in events
        if event.get("message") == "Recovered broker-managed open position during runtime sync"
    ]
    assert recovered_events
    payload = recovered_events[0].get("payload") or {}
    assert payload.get("strategy") == "multi_strategy"
    assert payload.get("strategy_base") == "momentum"
    assert payload.get("strategy_entry") == "momentum"

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


def test_runtime_sync_uses_idle_interval_when_no_local_or_pending_positions(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "bot-runtime-sync-idle.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    monotonic_now = {"value": 1_000.0}
    calls = {"count": 0}

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
            calls["count"] += 1
            return {}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr("xtb_bot.bot.core.time.monotonic", lambda: monotonic_now["value"])
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 0.0)
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=False)
    assert calls["count"] == 1

    monotonic_now["value"] += 30.0
    bot._runtime_sync_open_positions(force=False)
    assert calls["count"] == 1

    monotonic_now["value"] += 271.0
    bot._runtime_sync_open_positions(force=False)
    assert calls["count"] == 2

    bot.stop()


def test_runtime_sync_keeps_fast_interval_when_pending_open_exists(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "bot-runtime-sync-pending.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_pending_open(
        PendingOpen(
            pending_id="XTBBOTTESTPENDING01",
            symbol="AUS200",
            side=Side.BUY,
            volume=0.1,
            entry=8600.0,
            stop_loss=8550.0,
            take_profit=8650.0,
            created_at=10.0,
            thread_name="worker-AUS200",
            strategy="mean_reversion_bb",
            mode="execution",
        )
    )
    monotonic_now = {"value": 1_000.0}
    calls = {"count": 0}

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
            assert pending_opens is not None
            assert len(pending_opens) == 1
            calls["count"] += 1
            return {}

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr("xtb_bot.bot.core.time.monotonic", lambda: monotonic_now["value"])
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 0.0)
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["AUS200"])

    bot._runtime_sync_open_positions(force=False)
    assert calls["count"] == 1

    monotonic_now["value"] += 30.0
    bot._runtime_sync_open_positions(force=False)
    assert calls["count"] == 2

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


def test_bot_get_broker_close_sync_filters_kwargs_by_supported_signature(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-close-sync-filter.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    try:
        position = Position(
            position_id="DIAAAABOTSYNC001",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=10.0,
            status="open",
        )
        bot.store.upsert_trade(position, "worker-EURUSD", bot.config.strategy, bot.config.mode.value)
        bot.store.bind_trade_deal_reference(position.position_id, "BOTREF123")
        captured: list[tuple[str, str | None]] = []

        class _FakeBroker:
            def get_position_close_sync(self, position_id, *, deal_reference=None):
                captured.append((position_id, deal_reference))
                return {"source": "ig_history_transactions", "close_price": 1.0975}

            def close(self):
                return None

        bot.broker = _FakeBroker()  # type: ignore[assignment]
        bot._reserve_ig_non_trading_budget = lambda **kwargs: True  # type: ignore[method-assign]

        payload = bot._get_broker_close_sync(position, context="test")

        assert payload == {"source": "ig_history_transactions", "close_price": 1.0975}
        assert captured == [(position.position_id, "BOTREF123")]
    finally:
        bot.store.close()


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


def test_runtime_backfill_closed_trade_details_reconciles_pending_trade(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["WTI"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-closed-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAARUNTIMEDET01",
            symbol="WTI",
            side=Side.BUY,
            volume=1.0,
            open_price=10250.0,
            stop_loss=10210.0,
            take_profit=10330.0,
            opened_at=10.0,
            status="closed",
            close_price=None,
            closed_at=20.0,
            pnl=0.0,
        ),
        "worker-WTI",
        "momentum",
        "execution",
    )

    class _FakeBroker:
        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return {
                "source": "ig_history_transactions",
                "close_price": 10219.0,
                "realized_pnl": -31.0,
                "closed_at": 45.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    reconciled = bot._runtime_backfill_closed_trade_details(force=True)

    assert reconciled == 1
    record = bot.store.get_trade_record("DIAAARUNTIMEDET01")
    assert record is not None
    assert str(record.get("status") or "") == "closed"
    assert float(record.get("close_price") or 0.0) == pytest.approx(10219.0)
    assert float(record.get("pnl") or 0.0) == pytest.approx(-31.0)
    assert float(record.get("closed_at") or 0.0) == pytest.approx(45.0)

    bot.stop()


def test_runtime_backfill_closed_trade_details_skips_recent_closed_trade_with_good_details(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["UK100"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-skip-good-closed-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAASKIPGOOD01",
            symbol="UK100",
            side=Side.SELL,
            volume=1.0,
            open_price=10300.0,
            stop_loss=10340.0,
            take_profit=10220.0,
            opened_at=10.0,
            status="closed",
            close_price=10270.0,
            closed_at=1000.0,
            pnl=34.5,
        ),
        "worker-UK100",
        "momentum",
        "execution",
    )

    calls = {"count": 0}

    class _FakeBroker:
        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            calls["count"] += 1
            return {
                "source": "ig_history_activity",
                "close_price": 10270.0,
                "closed_at": 1000.0,
                "realized_pnl": 34.5,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    reconciled = bot._runtime_backfill_closed_trade_details(force=True)

    assert reconciled == 0
    assert calls["count"] == 0

    bot.stop()


def test_runtime_backfill_closed_trade_details_throttles_pending_trade_retries(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["UK100"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-pending-closed-details.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAPENDING01",
            symbol="UK100",
            side=Side.SELL,
            volume=1.0,
            open_price=10300.0,
            stop_loss=10340.0,
            take_profit=10220.0,
            opened_at=10.0,
            status="closed",
            close_price=10324.0,
            closed_at=1000.0,
            pnl=0.0,
        ),
        "worker-UK100",
        "momentum",
        "execution",
    )

    calls = {"count": 0}
    now = {"mono": 100.0, "ts": 1000.0}

    class _FakeBroker:
        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            calls["count"] += 1
            return {
                "source": "ig_history_activity",
                "close_price": 10324.0,
                "closed_at": 1000.0,
            }

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr("xtb_bot.bot.core.time.monotonic", lambda: now["mono"])
    monkeypatch.setattr("xtb_bot.bot.core.time.time", lambda: now["ts"])

    reconciled_first = bot._backfill_closed_trade_details()
    reconciled_second = bot._backfill_closed_trade_details()
    now["mono"] += 61.0
    now["ts"] += 61.0
    reconciled_third = bot._backfill_closed_trade_details()

    assert reconciled_first == 0
    assert reconciled_second == 0
    assert reconciled_third == 0
    assert calls["count"] == 2

    bot.stop()


def test_runtime_backfill_closed_trade_details_repairs_zero_pnl_from_close_event_estimate(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "bot-runtime-event-estimate.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    bot.store.upsert_trade(
        Position(
            position_id="DIAAAAEVENTEST01",
            symbol="DE40",
            side=Side.SELL,
            volume=0.5,
            open_price=23184.0,
            stop_loss=23219.0,
            take_profit=23120.0,
            opened_at=10.0,
            status="closed",
            close_price=23219.0,
            closed_at=20.0,
            pnl=0.0,
        ),
        "worker-DE40",
        "momentum",
        "execution",
    )
    bot.store.record_event(
        "INFO",
        "DE40",
        "Position closed",
        {
            "position_id": "DIAAAAEVENTEST01",
            "close_price": 23219.0,
            "pnl": 0.0,
            "local_pnl_estimate": -17.72,
            "pnl_is_estimated": True,
            "pnl_estimate_source": "broker_close_price",
        },
    )

    class _FakeBroker:
        def get_position_close_sync(self, position_id, **kwargs):
            _ = (position_id, kwargs)
            return None

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)

    reconciled = bot._runtime_backfill_closed_trade_details(force=True)

    assert reconciled == 1
    record = bot.store.get_trade_record("DIAAAAEVENTEST01")
    assert record is not None
    assert float(record.get("pnl") or 0.0) == pytest.approx(-17.72)
    assert bool(record.get("pnl_is_estimated")) is True
    assert str(record.get("pnl_estimate_source") or "") == "broker_close_price"

    bot.stop()


def test_runtime_sync_open_positions_pulses_monitor_progress_during_stale_reconcile_loop(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        symbols=["DE40", "FR40"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-runtime-sync-progress.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    stale_positions = [
        Position(
            position_id="DIAAAAPROGRESS001",
            symbol="DE40",
            side=Side.SELL,
            volume=1.0,
            open_price=22900.0,
            stop_loss=22980.0,
            take_profit=22840.0,
            opened_at=10.0,
            status="open",
        ),
        Position(
            position_id="DIAAAAPROGRESS002",
            symbol="FR40",
            side=Side.BUY,
            volume=1.0,
            open_price=8200.0,
            stop_loss=8150.0,
            take_profit=8290.0,
            opened_at=11.0,
            status="open",
        ),
    ]
    for position in stale_positions:
        bot.position_book.upsert(position)
        bot.store.upsert_trade(position, f"worker-{position.symbol}", bot.config.strategy, bot.config.mode.value)

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

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(TradingBot, "_symbols_to_run", lambda self: ["DE40", "FR40"])
    monkeypatch.setattr(
        bot,
        "_reserve_ig_non_trading_budget",
        lambda scope, wait_timeout_sec=0.0: True,
    )
    monkeypatch.setattr(bot, "_runtime_backfill_missing_on_broker_trades", lambda force=False: 0)

    reconcile_calls: list[str] = []

    def _fake_reconcile(position, *, context, now_ts):
        _ = (context, now_ts)
        reconcile_calls.append(str(position.position_id))
        return False

    monkeypatch.setattr(bot, "_reconcile_missing_local_position_from_broker_sync", _fake_reconcile)

    pulses: list[float] = []
    original_pulse = bot._pulse_runtime_monitor_progress

    def _spy_pulse() -> None:
        original_pulse()
        pulses.append(float(bot._runtime_monitor_last_progress_monotonic))

    monkeypatch.setattr(bot, "_pulse_runtime_monitor_progress", _spy_pulse)

    bot._runtime_sync_open_positions(force=True)

    assert len(reconcile_calls) == 2
    assert len(pulses) >= len(reconcile_calls) + 2
    assert pulses[-1] >= pulses[0]

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
