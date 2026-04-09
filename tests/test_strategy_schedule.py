from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import threading

import pytest

from xtb_bot.bot import TradingBot, WorkerAssignment
from xtb_bot.config import BotConfig, ConfigError, RiskConfig, StrategyScheduleEntry, load_config
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


def test_load_config_schedule_drops_unsupported_symbols(monkeypatch, caplog):
    _set_required_env(monkeypatch)
    monkeypatch.setenv(
        "XTB_STRATEGY_SCHEDULE",
        (
            '{"timezone":"UTC","slots":['
            '{"strategy":"index_hybrid","symbols":["EURUSD","US100"],"start":"07:00","end":"10:00","weekdays":"mon-fri","priority":10}'
            "]} "
        ),
    )

    config = load_config()

    assert len(config.strategy_schedule) == 1
    assert config.strategy_schedule[0].strategy == "index_hybrid"
    assert config.strategy_schedule[0].symbols == ["US100"]
    assert "Dropping unsupported symbols from strategy_schedule[0]" in caplog.text


def test_load_config_schedule_raises_when_all_symbols_unsupported(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv(
        "XTB_STRATEGY_SCHEDULE",
        (
            '{"timezone":"UTC","slots":['
            '{"strategy":"index_hybrid","symbols":["EURUSD","AUDUSD","USDCHF"],"start":"07:00","end":"10:00","weekdays":"mon-fri","priority":10}'
            "]} "
        ),
    )

    with pytest.raises(ConfigError, match="has no symbols supported by strategy=index_hybrid"):
        load_config()


def test_bot_switches_strategy_by_schedule_without_process_restart(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
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
                strategy="mean_reversion_bb",
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
    assert bot._worker_assignments["EURUSD"].strategy_name == "mean_reversion_bb"

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
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion_bb",
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
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion_bb",
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


def test_bot_multi_strategy_default_disables_schedule(monkeypatch, tmp_path):
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
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion_bb",
                symbols=["GBPUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
            )
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-multi-disabled.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    monkeypatch.setattr(
        TradingBot,
        "_now_utc",
        lambda self: datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc),
    )

    assignments = bot._schedule_assignments(bot._now_utc())
    assert sorted(assignments) == ["EURUSD"]
    assert assignments["EURUSD"].strategy_name == "multi_strategy"
    assert assignments["EURUSD"].strategy_params.get("_multi_strategy_base_component") == "momentum"
    assert assignments["EURUSD"].source == "multi_static"
    assert bot._symbols_to_run() == ["EURUSD"]

    bot.store.close()


def test_assignment_for_open_position_restores_multi_strategy_carrier(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={
            "fast_window": 3,
            "slow_window": 5,
            "multi_strategy_names": "momentum,g1",
        },
        strategy_params_map={
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-open-multi.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    position = Position(
        position_id="paper-open-multi",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1,
        stop_loss=1.095,
        take_profit=1.11,
        opened_at=10.0,
        status="open",
    )
    bot.position_book.upsert(position)
    bot.store.upsert_trade(
        position,
        "worker-EURUSD",
        "multi_strategy",
        "paper",
        strategy_entry="momentum",
        strategy_entry_component="g1",
    )

    assignment = bot._assignment_for_open_position(position)
    assert assignment.strategy_name == "multi_strategy"
    assert assignment.strategy_params.get("_multi_strategy_base_component") == "momentum"
    assert assignment.strategy_params.get("multi_strategy_names") == "momentum,g1"

    bot.store.close()


def test_assignment_for_open_position_restores_multi_strategy_carrier_from_trade_entry_base(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="g2",
        strategy_params={"min_history": 1, "multi_strategy_enabled": False},
        strategy_params_map={
            "g2": {"min_history": 1, "multi_strategy_enabled": False},
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-open-multi-entry-base.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    position = Position(
        position_id="paper-open-multi-entry-base",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1,
        stop_loss=1.095,
        take_profit=1.11,
        opened_at=10.0,
        status="open",
    )
    bot.position_book.upsert(position)
    bot.store.upsert_trade(
        position,
        "worker-EURUSD",
        "multi_strategy",
        "paper",
        strategy_entry="momentum",
        strategy_entry_component="g1",
    )

    assignment = bot._assignment_for_open_position(position)
    assert assignment.strategy_name == "multi_strategy"
    assert assignment.strategy_params.get("_multi_strategy_base_component") == "momentum"
    assert assignment.strategy_params.get("multi_strategy_names") == "momentum,g1"
    assert assignment.strategy_params.get("min_history") is None

    bot.store.close()


def test_multi_strategy_carrier_support_check_uses_component_support(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["BRENT"],
        strategy="momentum",
        strategy_params={
            "fast_window": 3,
            "slow_window": 5,
            "multi_strategy_names": "momentum,g1",
        },
        strategy_params_map={
            "momentum": {
                "fast_window": 3,
                "slow_window": 5,
                "multi_strategy_names": "momentum,g1",
            },
            "g1": {},
        },
        poll_interval_sec=1.0,
        storage_path=tmp_path / "schedule-carrier-support.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    started: list[tuple[str, str]] = []

    support_map = {
        "momentum": {"EURUSD"},
        "g1": {"US100"},
    }

    class _FakeStrategy:
        def __init__(self, strategy_name: str) -> None:
            self.strategy_name = strategy_name

        def supports_symbol(self, symbol: str) -> bool:
            return str(symbol).strip().upper() in support_map.get(self.strategy_name, set())

    def fake_create_strategy(name: str, params: dict[str, object]):
        _ = params
        return _FakeStrategy(str(name).strip().lower())

    def fake_make_worker(self, assignment, stop_event):
        started.append((assignment.symbol, assignment.strategy_name))
        return _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event)

    monkeypatch.setattr("xtb_bot.bot.create_strategy", fake_create_strategy)
    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    monkeypatch.setattr(TradingBot, "_make_worker", fake_make_worker)

    try:
        bot._strategy_support_cache.clear()
        assignment = bot._static_assignments()["BRENT"]
        assert assignment.strategy_name == "multi_strategy"
        assert bot._strategy_supports_symbol(assignment.strategy_name, assignment.strategy_params, "US100") is True
        assert bot._strategy_supports_symbol(assignment.strategy_name, assignment.strategy_params, "BRENT") is False

        bot._start_worker_for_assignment(assignment)
        assert started == []
        assert "BRENT" not in bot._worker_assignments
    finally:
        bot.stop()


def test_bot_defers_schedule_switch_while_position_is_open(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
        },
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="mean_reversion_bb",
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
    assert bot._worker_assignments["EURUSD"].strategy_name == "mean_reversion_bb"

    bot.stop()


def test_reconcile_workers_does_not_restart_when_only_assignment_source_changes(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["WTI"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "reconcile-source-only.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    stop_event = threading.Event()
    worker = _DummyWorker("WTI", "momentum", stop_event)
    worker.start()

    current_assignment = WorkerAssignment(
        symbol="WTI",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="open_position",
    )
    desired_assignment = WorkerAssignment(
        symbol="WTI",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="schedule",
    )

    bot.workers["WTI"] = worker  # type: ignore[assignment]
    bot._worker_stop_events["WTI"] = stop_event
    bot._worker_assignments["WTI"] = current_assignment

    monkeypatch.setattr(
        TradingBot,
        "_target_worker_assignments",
        lambda self, now_utc=None: {"WTI": desired_assignment},
    )
    monkeypatch.setattr(
        TradingBot,
        "_stop_worker_for_symbol",
        lambda self, symbol, reason: (_ for _ in ()).throw(AssertionError(f"unexpected stop: {symbol} {reason}")),
    )
    monkeypatch.setattr(
        TradingBot,
        "_start_worker_for_assignment",
        lambda self, assignment: (_ for _ in ()).throw(
            AssertionError(f"unexpected restart: {assignment.symbol} {assignment.strategy_name}")
        ),
    )

    bot._reconcile_workers()

    assert bot._worker_assignments["WTI"].source == "schedule"
    bot.stop()


def test_reconcile_workers_serializes_concurrent_start_for_same_symbol(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "reconcile-concurrent-start.db",
        risk=RiskConfig(),
    )

    monkeypatch.setattr(TradingBot, "_connect_broker", lambda self: None)
    bot = TradingBot(config)

    assignment = WorkerAssignment(
        symbol="EURUSD",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="static",
    )
    support_calls = {"count": 0}
    make_worker_calls = {"count": 0}
    support_lock = threading.Lock()
    first_support_entered = threading.Event()
    release_first_support = threading.Event()
    start_barrier = threading.Barrier(3)
    failures: list[Exception] = []

    def fake_supports_symbol(self, strategy_name, strategy_params, symbol):
        _ = (strategy_name, strategy_params, symbol)
        with support_lock:
            support_calls["count"] += 1
            call_number = support_calls["count"]
        if call_number == 1:
            first_support_entered.set()
            release_first_support.wait(timeout=0.2)
        else:
            release_first_support.set()
        return True

    def fake_make_worker(self, worker_assignment, stop_event):
        make_worker_calls["count"] += 1
        return _DummyWorker(worker_assignment.symbol, worker_assignment.strategy_name, stop_event)

    monkeypatch.setattr(
        TradingBot,
        "_target_worker_assignments",
        lambda self, now_utc=None: {"EURUSD": assignment},
    )
    monkeypatch.setattr(TradingBot, "_strategy_supports_symbol", fake_supports_symbol)
    monkeypatch.setattr(TradingBot, "_make_worker", fake_make_worker)

    def _run_reconcile() -> None:
        try:
            start_barrier.wait()
            bot._reconcile_workers()
        except Exception as exc:  # pragma: no cover
            failures.append(exc)

    thread_a = threading.Thread(target=_run_reconcile)
    thread_b = threading.Thread(target=_run_reconcile)
    thread_a.start()
    thread_b.start()
    start_barrier.wait(timeout=1.0)
    thread_a.join(timeout=1.0)
    thread_b.join(timeout=1.0)

    try:
        assert failures == []
        assert first_support_entered.is_set()
        assert support_calls["count"] == 1
        assert make_worker_calls["count"] == 1
        with bot._workers_lock:
            assert list(bot.workers) == ["EURUSD"]
            assert list(bot._worker_assignments) == ["EURUSD"]
    finally:
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
    lease_key = "worker.lease.EURUSD"
    assert bot.store.get_kv(lease_key) is not None

    bot.stop(force_close=False)

    assert fake_broker.close_calls == 0
    assert bot._resources_closed is False
    assert bot.store.get_kv(lease_key) is None
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


def test_worker_lease_is_created_on_start_and_revoked_on_stop(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "worker-lease.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    monkeypatch.setattr(
        TradingBot,
        "_make_worker",
        lambda self, assignment, stop_event: _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event),
    )

    assignment = WorkerAssignment(
        symbol="EURUSD",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="static",
    )

    bot._start_worker_for_assignment(assignment)
    lease_key = "worker.lease.EURUSD"
    lease_id = bot.store.get_kv(lease_key)
    assert isinstance(lease_id, str)
    assert len(lease_id) >= 8

    bot._stop_worker_for_symbol("EURUSD", "unit_test")
    assert bot.store.get_kv(lease_key) is None

    bot.stop()


def test_worker_lifecycle_uses_normalized_symbol_keys_for_cleanup(monkeypatch, tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["eurusd"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        poll_interval_sec=1.0,
        storage_path=tmp_path / "worker-normalized-keys.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)

    monkeypatch.setattr(
        TradingBot,
        "_make_worker",
        lambda self, assignment, stop_event: _DummyWorker(assignment.symbol, assignment.strategy_name, stop_event),
    )

    assignment = WorkerAssignment(
        symbol="eurusd",
        strategy_name="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        source="static",
    )

    bot._start_worker_for_assignment(assignment)
    with bot._workers_lock:
        assert list(bot.workers.keys()) == ["EURUSD"]
        assert list(bot._worker_stop_events.keys()) == ["EURUSD"]
        assert list(bot._worker_assignments.keys()) == ["EURUSD"]
        bot._deferred_switch_signature_by_symbol["EURUSD"] = ("momentum:paper", "g1:paper")

    bot._stop_worker_for_symbol("eurusd", "unit_test")

    with bot._workers_lock:
        assert "EURUSD" not in bot.workers
        assert "EURUSD" not in bot._worker_stop_events
        assert "EURUSD" not in bot._worker_assignments
        assert "EURUSD" not in bot._worker_lease_id_by_symbol
        assert "EURUSD" not in bot._deferred_switch_signature_by_symbol

    bot.stop()


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


def test_run_forever_tolerates_monitor_loop_exception(monkeypatch, tmp_path):
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
        storage_path=tmp_path / "run-forever-monitor-failure.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    called = {"monitor": 0, "stop": 0}

    monkeypatch.setattr(bot, "start", lambda: None)

    def _monitor_once_then_stop():
        called["monitor"] += 1
        if called["monitor"] == 1:
            raise RuntimeError("monitor failed")
        bot.stop_event.set()

    def _mark_stop(*, force_close: bool = True):
        _ = force_close
        called["stop"] += 1

    monkeypatch.setattr(bot, "_monitor_workers", _monitor_once_then_stop)
    monkeypatch.setattr(bot, "stop", _mark_stop)

    bot.run_forever()

    assert called["monitor"] == 2
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
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "mean_reversion_bb": {"mean_reversion_bb_window": 20, "mean_reversion_bb_std_dev": 2.0},
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
                strategy="mean_reversion_bb",
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


def test_db_first_symbol_scope_includes_all_configured_symbols_for_warmup(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD", "SOL"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "trend_following": {"fast_ema_window": 20, "slow_ema_window": 80},
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
                strategy="trend_following",
                symbols=["SOL"],
                start_time="20:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=20 * 60,
                end_minute=23 * 60,
            ),
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "db-first-scope-active.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    try:
        now_utc = datetime(2026, 3, 12, 8, 0, tzinfo=timezone.utc)
        assert bot._symbols_for_db_first_cache(now_utc) == ["EURUSD", "SOL"]
    finally:
        bot.stop()


def test_db_first_symbol_scope_keeps_open_positions_when_schedule_is_inactive(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={"momentum": {"fast_window": 3, "slow_window": 5}},
        strategy_schedule=[
            StrategyScheduleEntry(
                strategy="momentum",
                symbols=["EURUSD"],
                start_time="07:00",
                end_time="10:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=7 * 60,
                end_minute=10 * 60,
            )
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "db-first-scope-open-position.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    bot.position_book.upsert(
        Position(
            position_id="paper-open-42",
            symbol="GBPUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.25,
            stop_loss=1.24,
            take_profit=1.26,
            opened_at=10.0,
            status="open",
        )
    )
    try:
        now_utc = datetime(2026, 3, 12, 13, 0, tzinfo=timezone.utc)
        assert bot._symbols_for_db_first_cache(now_utc) == ["EURUSD", "GBPUSD"]
    finally:
        bot.stop()


def test_db_first_runtime_symbols_follow_active_schedule_slots(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD", "SOL"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "trend_following": {"fast_ema_window": 20, "slow_ema_window": 80},
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
                strategy="trend_following",
                symbols=["SOL"],
                start_time="20:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=20 * 60,
                end_minute=23 * 60,
            ),
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "db-first-runtime-scope-active.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    try:
        morning = datetime(2026, 3, 12, 8, 0, tzinfo=timezone.utc)
        midday = datetime(2026, 3, 12, 13, 0, tzinfo=timezone.utc)
        assert bot._runtime_symbols_for_db_first_requests(morning) == ["EURUSD"]
        assert bot._runtime_symbols_for_db_first_requests(midday) == []
    finally:
        bot.stop()


def test_db_first_symbol_spec_refresh_scope_follows_runtime_symbols(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD", "SOL"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
        strategy_params_map={
            "momentum": {"fast_window": 3, "slow_window": 5, "multi_strategy_enabled": False},
            "trend_following": {"fast_ema_window": 20, "slow_ema_window": 80},
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
                strategy="trend_following",
                symbols=["SOL"],
                start_time="20:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=20 * 60,
                end_minute=23 * 60,
            ),
        ],
        strategy_schedule_timezone="UTC",
        poll_interval_sec=1.0,
        storage_path=tmp_path / "db-first-symbol-spec-refresh-scope.db",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    try:
        morning = datetime(2026, 3, 12, 8, 0, tzinfo=timezone.utc)
        midday = datetime(2026, 3, 12, 13, 0, tzinfo=timezone.utc)
        assert bot._symbols_for_db_first_symbol_spec_refresh(morning) == ["EURUSD"]
        assert bot._symbols_for_db_first_symbol_spec_refresh(midday) == []
    finally:
        bot.stop()


def test_strategy_params_for_unknown_strategy_returns_empty_dict(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5},
        strategy_params_map={"momentum": {"fast_window": 3, "slow_window": 5}},
        storage_path=tmp_path / "state.sqlite3",
        risk=RiskConfig(),
    )
    bot = TradingBot(config)
    try:
        assert bot._strategy_params_for("momentum") == {"fast_window": 3, "slow_window": 5}
        assert bot._strategy_params_for("unknown_strategy") == {}
    finally:
        bot.stop()
