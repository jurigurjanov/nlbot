from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import threading

import pytest

from xtb_bot.bot import TradingBot, WorkerAssignment
from xtb_bot.config import BotConfig, RiskConfig, StrategyScheduleEntry, load_config
from xtb_bot.models import AccountType, Position, PriceTick, RunMode, Side


class _DummyWorker:
    def __init__(self, symbol: str, strategy_name: str, stop_event):
        self.symbol = symbol
        self.strategy_name = strategy_name
        self._stop_event = stop_event
        self._started = False

    def start(self):
        self._started = True

    def is_alive(self) -> bool:
        return self._started and not self._stop_event.is_set()

    def join(self, timeout=None):
        _ = timeout
        self._started = False


class _StuckWorker:
    def __init__(self, symbol: str, strategy_name: str, stop_event):
        self.symbol = symbol
        self.strategy_name = strategy_name
        self._stop_event = stop_event
        self._alive = True

    def start(self):
        return None

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout=None):
        _ = timeout
        return None


def _set_required_env(monkeypatch):
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "1")
    monkeypatch.setenv("XTB_PASSWORD", "1")
    monkeypatch.delenv("XTB_STRATEGY_SCHEDULE", raising=False)
    monkeypatch.delenv("XTB_STRATEGY_SCHEDULE_TIMEZONE", raising=False)
    monkeypatch.delenv("XTB_SYMBOLS_MEAN_REVERSION_BB", raising=False)
    monkeypatch.delenv("IG_SYMBOLS_MEAN_REVERSION_BB", raising=False)


def test_load_config_parses_strategy_schedule(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv(
        "XTB_STRATEGY_SCHEDULE",
        (
            '{"timezone":"UTC","slots":['
            '{"strategy":"momentum","symbols":["DE40","GBPUSD"],"start":"07:00","end":"10:00","weekdays":"mon-fri","priority":10},'
            '{"strategy":"mean_reversion_bb","start":"09:00","end":"12:00","weekdays":["mon","tue","wed","thu","fri"]}'
            "]} "
        ),
    )
    monkeypatch.setenv("XTB_SYMBOLS_MEAN_REVERSION_BB", "EURGBP,EURCHF")

    config = load_config()

    assert config.strategy_schedule_timezone == "UTC"
    assert len(config.strategy_schedule) == 2
    first = config.strategy_schedule[0]
    assert first.strategy == "momentum"
    assert first.symbols == ["DE40", "GBPUSD"]
    assert first.start_minute == 7 * 60
    assert first.end_minute == 10 * 60
    assert first.weekdays == (0, 1, 2, 3, 4)
    second = config.strategy_schedule[1]
    assert second.strategy == "mean_reversion_bb"
    assert second.symbols == ["EURGBP", "EURCHF"]
    assert "momentum" in config.strategy_params_map
    assert "mean_reversion_bb" in config.strategy_params_map


def test_bot_switches_strategy_by_schedule_without_process_restart(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5},
            "mean_reversion": {"mean_reversion_zscore_window": 20, "mean_reversion_zscore_threshold": 1.8},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="momentum",
                symbols=["EURUSD"],
                start_time="07:00",
                end_time="10:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=7 * 60,
                end_minute=10 * 60,
            ),
            StrategyScheduleEntry(
                strategy="mean_reversion",
                symbols=["EURUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            ),
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    def fake_make_worker(self, assignment, stop_event):
        return _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event)

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_make_worker", fake_make_worker)
    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 8, 0, tzinfo=timezone.utc),
    )

    bot.start()
    assert bot._worker_assignments["EURUSD"].strategy_name == "momentum"

    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc),
    )
    bot._reconcile_workers()
    assert bot._worker_assignments["EURUSD"].strategy_name == "mean_reversion"

    bot.stop()


def test_bot_force_strategy_ignores_schedule(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        force_strategy=True,
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5},
            "mean_reversion": {"mean_reversion_zscore_window": 20, "mean_reversion_zscore_threshold": 1.8},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion",
                symbols=["EURUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            )
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-force.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc),
    )

    assignments = bot._schedule_assignments(bot._now_utc())
    assert assignments["EURUSD"].strategy_name == "momentum"
    assert assignments["EURUSD"].source == "forced"

    bot.store.close()


def test_bot_force_symbols_ignores_schedule_symbol_sets(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["AUS200", "BRENT"],
        strategy="trend_following",
        force_symbols=True,
        strategy_params={"fast_ema_window": 20, "slow_ema_window": 80},
        strategy_params_map={
            "trend_following": {"fast_ema_window": 20, "slow_ema_window": 80},
            "mean_reversion": {"mean_reversion_zscore_window": 20, "mean_reversion_zscore_threshold": 1.8},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion",
                symbols=["EURUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            )
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-force-symbols.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc),
    )

    assignments = bot._schedule_assignments(bot._now_utc())
    assert sorted(assignments) == ["AUS200", "BRENT"]
    assert assignments["AUS200"].strategy_name == "trend_following"
    assert assignments["BRENT"].strategy_name == "trend_following"
    assert assignments["AUS200"].source == "forced_symbols"
    assert assignments["BRENT"].source == "forced_symbols"

    bot.store.close()


def test_bot_defers_schedule_switch_while_position_is_open(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5},
            "mean_reversion": {"mean_reversion_zscore_window": 20, "mean_reversion_zscore_threshold": 1.8},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion",
                symbols=["EURUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            )
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-open.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    bot.position_book.upsert(
        Position(
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
    )
    bot.store.upsert_trade(
        Position(
            position_id="paper-open-1",
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
        "paper",
    )

    def fake_make_worker(self, assignment, stop_event):
        return _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event)

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_make_worker", fake_make_worker)
    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc),
    )

    bot._start_worker_for_assignment(
        bot._assignment_for_open_position(bot.position_book.get("EURUSD"))  # type: ignore[arg-type]
    )
    assert bot._worker_assignments["EURUSD"].strategy_name == "momentum"

    bot._reconcile_workers()
    assert bot._worker_assignments["EURUSD"].strategy_name == "momentum"

    bot.position_book.remove("EURUSD")
    bot.store.update_trade_status("paper-open-1", status="closed", close_price=1.105, closed_at=20.0, pnl=50.0)
    bot._reconcile_workers()
    assert bot._worker_assignments["EURUSD"].strategy_name == "mean_reversion"

    bot.stop()


def test_bot_stop_can_defer_resource_close_until_workers_finish(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "stop-safe.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    class _FakeBroker:
        def __init__(self):
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    fake_broker = _FakeBroker()
    bot.broker = fake_broker  # type: ignore[assignment]
    worker = _StuckWorker("EURUSD", "momentum", threading.Event())

    monkeypatch.setattr(
        TradingBot,
        "_make_worker",
        lambda self, assignment, stop_event: worker,
    )

    assignment = WorkerAssignment(
        symbol="EURUSD",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="static",
    )
    bot._start_worker_for_assignment(assignment)

    bot.stop(force_close=False)

    assert fake_broker.close_calls == 0
    assert bot._resources_closed is False
    bot.store.set_kv("shutdown.safe", "1")
    assert bot.store.get_kv("shutdown.safe") == "1"

    worker._alive = False
    bot.stop()

    assert fake_broker.close_calls == 1
    assert bot._resources_closed is True


def test_bot_stop_force_close_releases_resources_with_stuck_workers(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "stop-force.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    class _FakeBroker:
        def __init__(self):
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    fake_broker = _FakeBroker()
    bot.broker = fake_broker  # type: ignore[assignment]
    worker = _StuckWorker("EURUSD", "momentum", threading.Event())

    monkeypatch.setattr(
        TradingBot,
        "_make_worker",
        lambda self, assignment, stop_event: worker,
    )

    assignment = WorkerAssignment(
        symbol="EURUSD",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="static",
    )
    bot._start_worker_for_assignment(assignment)

    bot.stop()

    assert fake_broker.close_calls == 1
    assert bot._resources_closed is True


def test_bot_magic_instance_is_deterministic_in_execution_when_unset(tmp_path):
    config = BotConfig(
        user_id="user-1",
        password="1",
        app_name="app-1",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        broker="ig",
        api_key="ig-key",
        account_id="ABC123",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "magic-instance.db",
        risk=RiskConfig(),
    )

    bot = TradingBot(config)
    expected = hashlib.sha1("ig|demo|abc123|user-1|app-1".encode("utf-8")).hexdigest()[:8]
    assert bot.bot_magic_instance == expected
    bot.stop()


def test_run_forever_calls_stop_when_start_fails(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "run-forever-stop.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    called = {"stop": 0}

    def _fail_start():
        raise RuntimeError("startup failed")

    def _mark_stop(*, force_close: bool = True):
        _ = force_close
        called["stop"] += 1

    monkeypatch.setattr(bot, "start", _fail_start)
    monkeypatch.setattr(bot, "stop", _mark_stop)

    with pytest.raises(RuntimeError, match="startup failed"):
        bot.run_forever()

    assert called["stop"] == 1


def test_history_keep_rows_estimate_respects_caps(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_HISTORY_KEEP_ROWS_CAP", "1500")
    monkeypatch.setenv("XTB_HISTORY_TICKS_PER_CANDLE_CAP", "40")

    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=0.01,
        storage_path=tmp_path / "history-cap.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    class _FakeStrategy:
        min_history = 10_000
        candle_timeframe_sec = 86_400

        @staticmethod
        def supports_symbol(symbol: str) -> bool:
            return bool(symbol)

    monkeypatch.setattr("xtb_bot.bot.create_strategy", lambda name, params: _FakeStrategy())

    try:
        estimated = bot._estimate_history_keep_rows("momentum", {"a": 1})
        assert estimated == 1500
    finally:
        bot.stop()


def test_bot_passively_warms_history_for_inactive_scheduled_symbols(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5},
            "mean_reversion": {"mean_reversion_zscore_window": 20, "mean_reversion_zscore_threshold": 1.8},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="momentum",
                symbols=["EURUSD"],
                start_time="07:00",
                end_time="10:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=7 * 60,
                end_minute=10 * 60,
            ),
            StrategyScheduleEntry(
                strategy="mean_reversion",
                symbols=["GBPUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            ),
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "passive-history.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    class _FakeBroker:
        def __init__(self):
            self.calls: list[str] = []

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            upper = str(symbol).upper()
            self.calls.append(upper)
            mid = 1.2500 if upper == "GBPUSD" else 1.1000
            return PriceTick(
                symbol=upper,
                bid=mid - 0.0001,
                ask=mid + 0.0001,
                timestamp=1_700_000_000.0,
                volume=25.0,
            )

        def close(self):
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]

    def fake_make_worker(self, assignment, stop_event):
        return _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event)

    monkeypatch.setattr(TradingBot, "_make_worker", fake_make_worker)
    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 8, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr("xtb_bot.bot.time.time", lambda: 1_700_000_000.0)

    bot._monitor_workers()

    assert "EURUSD" in bot.workers
    assert bot.broker.calls == ["GBPUSD"]  # type: ignore[attr-defined]

    gbp_history = bot.store.load_recent_price_history("GBPUSD", limit=10)
    eur_history = bot.store.load_recent_price_history("EURUSD", limit=10)
    assert len(gbp_history) == 1
    assert gbp_history[0]["close"] == pytest.approx(1.25)
    assert eur_history == []

    bot.stop()
