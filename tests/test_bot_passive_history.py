from __future__ import annotations

import pytest

from xtb_bot.bot import TradingBot
from xtb_bot.config import BotConfig
from xtb_bot.models import AccountType, PriceTick, RunMode
from xtb_bot.risk_manager import RiskConfig


def _make_config(tmp_path) -> BotConfig:
    return BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-passive.db",
        risk=RiskConfig(),
    )


def test_force_passive_history_refresh_skips_when_allowance_backoff_active(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 2.5

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.price_calls += 1
            raise AssertionError("get_price must not be called during active allowance backoff")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        assert fake.price_calls == 0
    finally:
        bot.stop()


def test_force_passive_history_refresh_does_not_block_on_rest_market_wait(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.4

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.price_calls += 1
            raise AssertionError("get_price must not be called while market wait is active in force mode")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        assert fake.price_calls == 0
    finally:
        bot.stop()


def test_passive_history_refresh_stops_cycle_on_allowance_error(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.requested_symbols: list[str] = []

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.requested_symbols.append(symbol)
            raise RuntimeError(
                'IG API GET /markets failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=False)
        assert fake.requested_symbols == ["EURUSD"]
    finally:
        bot.stop()


def test_monitor_workers_skips_rest_sync_and_passive_refresh_during_allowance_backoff(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"sync": 0, "reconcile": 0, "refresh": 0, "housekeeping": 0}

    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 5.0)
    monkeypatch.setattr(bot, "_runtime_sync_open_positions", lambda force=False: calls.__setitem__("sync", calls["sync"] + 1))
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1))
    monkeypatch.setattr(bot, "_refresh_passive_price_history", lambda force=False: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: calls.__setitem__("housekeeping", calls["housekeeping"] + 1))

    try:
        bot._monitor_workers()
        assert calls["housekeeping"] == 1
        assert calls["reconcile"] == 1
        assert calls["sync"] == 0
        assert calls["refresh"] == 0
    finally:
        bot.stop()


def test_monitor_workers_tolerates_housekeeping_failure(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"sync": 0, "reconcile": 0, "refresh": 0}

    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 0.0)
    monkeypatch.setattr(bot, "_runtime_sync_open_positions", lambda force=False: calls.__setitem__("sync", calls["sync"] + 1))
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1))
    monkeypatch.setattr(bot, "_refresh_passive_price_history", lambda force=False: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: (_ for _ in ()).throw(RuntimeError("db busy")))

    try:
        bot._monitor_workers()
        assert calls["sync"] == 1
        assert calls["reconcile"] == 1
        assert calls["refresh"] == 1
    finally:
        bot.stop()


def test_passive_history_refresh_makes_timestamps_monotonic_when_broker_ts_stalls(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.price_calls += 1
            mid = 1.1000 + self.price_calls * 0.0001
            return PriceTick(
                symbol=symbol,
                bid=mid - 0.00005,
                ask=mid + 0.00005,
                timestamp=1_700_000_000.0,
                volume=10.0 + self.price_calls,
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        bot._refresh_passive_price_history(force=True)

        samples = bot.store.load_recent_price_history("EURUSD", limit=10)
        assert len(samples) >= 2
        assert float(samples[-1]["ts"]) > float(samples[-2]["ts"])
    finally:
        bot.stop()


def test_passive_history_refresh_clamps_future_broker_timestamp(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.price_calls += 1
            mid = 1.1000 + self.price_calls * 0.0001
            return PriceTick(
                symbol=symbol,
                bid=mid - 0.00005,
                ask=mid + 0.00005,
                timestamp=1_900_000_000.0,
                volume=10.0 + self.price_calls,
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr("xtb_bot.bot.time.time", lambda: 1_800_000_000.0)
    try:
        bot._refresh_passive_price_history(force=True)
        bot._refresh_passive_price_history(force=True)

        samples = bot.store.load_recent_price_history("EURUSD", limit=10)
        assert len(samples) >= 2
        assert float(samples[-2]["ts"]) == pytest.approx(1_800_000_000.0)
        assert float(samples[-1]["ts"]) > float(samples[-2]["ts"])
        assert float(samples[-1]["ts"]) <= 1_800_000_001.0
    finally:
        bot.stop()


def test_monitor_workers_records_trade_reason_summary_snapshot(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    bot._trade_reason_summary_enabled = True
    bot._trade_reason_summary_interval_sec = 60.0
    bot._trade_reason_summary_window_sec = 3_600.0
    bot._trade_reason_summary_scan_limit = 1_000
    bot._trade_reason_summary_top = 10
    bot._last_trade_reason_summary_monotonic = 0.0

    bot.store.record_event("WARN", "EURUSD", "Trade blocked by spread filter", {"spread": 4.2})
    bot.store.record_event(
        "ERROR",
        "DE40",
        "Broker error",
        {
            "error": (
                "IG API POST /positions/otc failed: 400 Bad Request "
                '{"errorCode":"error.service.create.otc.position.instrument.invalid"}'
            )
        },
    )

    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: {"ran": False})
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 1.0)
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: None)

    try:
        bot._monitor_workers()
        snapshots = [
            event
            for event in bot.store.load_events(limit=50)
            if str(event.get("message") or "") == "Trade reason summary snapshot"
        ]
        assert snapshots
        payload = snapshots[0].get("payload") or {}
        by_kind = payload.get("by_kind") or {}
        assert int(by_kind.get("block") or 0) >= 1
        assert int(by_kind.get("reject") or 0) >= 1
    finally:
        bot.stop()
