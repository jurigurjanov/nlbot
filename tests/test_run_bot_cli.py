from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
import types
from pathlib import Path

import pytest

import run_bot
import xtb_bot.cli as cli_module
from xtb_bot.models import Position, Side
from xtb_bot.state_store import StateStore


class _DummyBot:
    def __init__(self, config):
        self.config = config

    def run_forever(self):
        return None


def _run_main_with_args(monkeypatch, argv):
    captured: dict[str, object] = {}

    def fake_load_config(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(strategy="momentum", strategy_params={})

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", fake_load_config)
    monkeypatch.setattr(run_bot, "TradingBot", _DummyBot)
    monkeypatch.setattr(run_bot, "available_strategies", lambda: ["momentum"])
    monkeypatch.setattr("sys.argv", argv)

    run_bot.main()
    return captured


def _run_main_with_config(monkeypatch, argv, cfg):
    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", lambda **kwargs: cfg)
    monkeypatch.setattr(
        run_bot,
        "available_strategies",
        lambda: ["momentum", "index_hybrid", "mean_breakout_v2", "mean_reversion_bb"],
    )
    monkeypatch.setattr("sys.argv", argv)

    captured: dict[str, object] = {}

    class _CaptureBot:
        def __init__(self, config):
            captured["config"] = config

        def run_forever(self):
            captured["ran"] = True
            return None

    monkeypatch.setattr(run_bot, "TradingBot", _CaptureBot)
    run_bot.main()
    return captured


def test_main_passes_ig_stream_enabled_override_true(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--ig-stream-enabled"])
    assert captured.get("ig_stream_enabled_override") is True


def test_main_passes_ig_stream_enabled_override_false(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--no-ig-stream-enabled"])
    assert captured.get("ig_stream_enabled_override") is False


def test_main_passes_strict_broker_connect_override_true(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--strict-broker-connect"])
    assert captured.get("strict_broker_connect_override") is True


def test_main_passes_force_strategy_override_true(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--force"])
    assert captured.get("force_strategy_override") is True


def test_main_passes_symbols_override(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--symbols", "us100, us500 , brent"])
    assert captured.get("symbols_override") == ["US100", "US500", "BRENT"]


def test_main_passes_force_symbols_override_true(monkeypatch):
    captured = _run_main_with_args(monkeypatch, ["run_bot.py", "--force-symbols"])
    assert captured.get("force_symbols_override") is True


def test_main_installs_sigterm_and_sigint_handlers(monkeypatch):
    registered: dict[object, object] = {}

    def fake_signal(sig, handler):
        registered[sig] = handler
        return None

    monkeypatch.setattr(run_bot.signal, "signal", fake_signal)
    monkeypatch.setattr(run_bot.signal, "getsignal", lambda sig: None)

    _run_main_with_args(monkeypatch, ["run_bot.py"])

    assert run_bot.signal.SIGINT in registered
    assert run_bot.signal.SIGTERM in registered


def test_termination_handler_requests_graceful_stop(monkeypatch):
    registered: dict[object, object] = {}

    def fake_signal(sig, handler):
        registered[sig] = handler
        return None

    monkeypatch.setattr(run_bot.signal, "signal", fake_signal)
    monkeypatch.setattr(run_bot.signal, "getsignal", lambda sig: None)

    class _SignalAwareBot:
        def __init__(self, config):
            self.config = config
            self.stop_requests: list[tuple[str | None, str | None]] = []

        def request_graceful_stop(self, *, reason=None, source=None):
            self.stop_requests.append((reason, source))

        def run_forever(self):
            handler = registered[run_bot.signal.SIGTERM]
            handler(run_bot.signal.SIGTERM, None)
            assert self.stop_requests == [("signal:SIGTERM", "cli_signal_handler")]
            return None

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", lambda **kwargs: types.SimpleNamespace(strategy="momentum", strategy_params={}))
    monkeypatch.setattr(run_bot, "TradingBot", _SignalAwareBot)
    monkeypatch.setattr(run_bot, "available_strategies", lambda: ["momentum"])
    monkeypatch.setattr("sys.argv", ["run_bot.py"])

    run_bot.main()


def test_load_dotenv_preserves_nested_quotes(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text('GREETING=\'say "hello"\'\n', encoding="utf-8")
    monkeypatch.delenv("GREETING", raising=False)

    run_bot._load_dotenv(str(dotenv))

    assert os.environ["GREETING"] == 'say "hello"'


def test_cli_table_columns_rejects_invalid_identifier_without_sql_execution():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    try:
        con.execute("CREATE TABLE trades(id INTEGER PRIMARY KEY, symbol TEXT)")
        columns = cli_module._table_columns(con, 'trades); DROP TABLE trades; --')
        assert columns == set()
        remaining = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'").fetchone()
        assert remaining is not None
    finally:
        con.close()


def test_main_show_trades_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_trades(storage_path, limit, open_only, strategy):
        captured["storage_path"] = storage_path
        captured["limit"] = limit
        captured["open_only"] = open_only
        captured["strategy"] = strategy
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_trades", fake_show_trades)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-trades", "--trades-limit", "7", "--trades-open-only", "--strategy", "momentum"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["limit"] == 7
    assert captured["open_only"] is True
    assert captured["strategy"] == "momentum"


def test_main_watch_open_trades_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_watch_open_trades(storage_path, interval_sec, strategy, iterations):
        captured["storage_path"] = storage_path
        captured["interval_sec"] = interval_sec
        captured["strategy"] = strategy
        captured["iterations"] = iterations
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_watch_open_trades", fake_watch_open_trades)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_bot.py",
            "--watch-open-trades",
            "--watch-interval-sec",
            "5",
            "--watch-iterations",
            "2",
            "--strategy",
            "g1",
        ],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["interval_sec"] == 5.0
    assert captured["strategy"] == "g1"
    assert captured["iterations"] == 2


def test_main_show_trade_confidence_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_trade_confidence(storage_path, strategy):
        captured["storage_path"] = storage_path
        captured["strategy"] = strategy
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_trade_confidence", fake_show_trade_confidence)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr("sys.argv", ["run_bot.py", "--show-trade-confidence", "--strategy", "momentum"])

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["strategy"] == "momentum"


def test_main_show_fill_quality_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_fill_quality(storage_path, strategy, limit):
        captured["storage_path"] = storage_path
        captured["strategy"] = strategy
        captured["limit"] = limit
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_fill_quality", fake_show_fill_quality)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-fill-quality", "--strategy", "momentum", "--fill-quality-limit", "7"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["strategy"] == "momentum"
    assert captured["limit"] == 7


def test_main_show_strategy_scorecard_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_strategy_scorecard(storage_path, strategy, limit):
        captured["storage_path"] = storage_path
        captured["strategy"] = strategy
        captured["limit"] = limit
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_strategy_scorecard", fake_show_strategy_scorecard)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_bot.py",
            "--show-strategy-scorecard",
            "--strategy",
            "momentum",
            "--strategy-scorecard-limit",
            "6",
        ],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["strategy"] == "momentum"
    assert captured["limit"] == 6


def test_main_show_balance_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_balance(storage_path):
        captured["storage_path"] = storage_path
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_balance", fake_show_balance)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr("sys.argv", ["run_bot.py", "--show-balance"])

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")


def test_main_show_status_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_status(storage_path):
        captured["storage_path"] = storage_path
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_status", fake_show_status)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr("sys.argv", ["run_bot.py", "--show-status"])

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")


def test_main_show_active_schedule_calls_helper(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="momentum",
        strategy_params={},
        storage_path=Path("/tmp/original.db"),
        strict_broker_connect=False,
    )
    captured: dict[str, object] = {}

    def fake_show_active_schedule(config):
        captured["config"] = config
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", lambda **kwargs: cfg)
    monkeypatch.setattr(run_bot, "_show_active_schedule", fake_show_active_schedule)
    monkeypatch.setattr(run_bot, "available_strategies", lambda: ["momentum"])
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-active-schedule", "--storage-path", "/tmp/custom.db"],
    )

    run_bot.main()

    used_cfg = captured["config"]
    assert used_cfg is cfg
    assert cfg.storage_path == Path("/tmp/custom.db")


def test_main_show_active_schedule_passes_symbols_and_force_symbols(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="momentum",
        strategy_params={},
        storage_path=Path("/tmp/original.db"),
        strict_broker_connect=False,
    )
    captured: dict[str, object] = {}

    def fake_load_config(**kwargs):
        captured.update(kwargs)
        return cfg

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", fake_load_config)
    monkeypatch.setattr(run_bot, "_show_active_schedule", lambda config: 1)
    monkeypatch.setattr(run_bot, "available_strategies", lambda: ["momentum"])
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-active-schedule", "--strategy", "momentum", "--symbols", "AUS200,BRENT", "--force-symbols"],
    )

    run_bot.main()

    assert captured["strategy_override"] == "momentum"
    assert captured["symbols_override"] == ["AUS200", "BRENT"]
    assert captured["force_symbols_override"] is True


def test_show_active_schedule_uses_multi_strategy_carrier_assignments(monkeypatch, tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    cfg = types.SimpleNamespace(
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
        strategy_schedule=[
            types.SimpleNamespace(
                strategy="mean_reversion_bb",
                strategy_params={"mean_reversion_bb_window": 20},
                symbols=["GBPUSD"],
                start_time="19:00",
                end_time="23:00",
                weekdays=(0, 1, 2, 3, 4),
                start_minute=19 * 60,
                end_minute=23 * 60,
                priority=10,
                label="mr-slot",
            )
        ],
        strategy_schedule_timezone="UTC",
        force_symbols=False,
        force_strategy=False,
        symbols=["EURUSD"],
        mode=types.SimpleNamespace(value="paper"),
        storage_path=storage_path,
    )

    class _FixedDateTime:
        @staticmethod
        def now(tz=None):
            current = datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc)
            if tz is not None:
                return current.astimezone(tz)
            return current

    monkeypatch.setattr(run_bot, "datetime", _FixedDateTime)

    printed = run_bot._show_active_schedule(cfg)
    output = capsys.readouterr().out

    assert printed == 1
    assert "configured_slots=1 active_slots=0 schedule_disabled_by_multi_strategy=yes" in output
    assert "Active slots:" not in output
    assert "EURUSD | strategy=multi_strategy strategy_base=momentum source=multi_static" in output
    assert "GBPUSD" not in output


def test_show_active_schedule_restores_multi_strategy_open_position(monkeypatch, tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="open-ms-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.8,
                status="open",
            ),
            "worker-EURUSD",
            "multi_strategy",
            "paper",
            strategy_entry="momentum",
            strategy_entry_component="g1",
        )
    finally:
        store.close()

    cfg = types.SimpleNamespace(
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
        strategy_schedule=[],
        strategy_schedule_timezone="UTC",
        force_symbols=False,
        force_strategy=False,
        symbols=["EURUSD"],
        mode=types.SimpleNamespace(value="paper"),
        storage_path=storage_path,
    )

    class _FixedDateTime:
        @staticmethod
        def now(tz=None):
            current = datetime(2026, 3, 12, 19, 30, tzinfo=timezone.utc)
            if tz is not None:
                return current.astimezone(tz)
            return current

    monkeypatch.setattr(run_bot, "datetime", _FixedDateTime)

    printed = run_bot._show_active_schedule(cfg)
    output = capsys.readouterr().out

    assert printed == 1
    assert "mode=static" in output
    assert "EURUSD | strategy=multi_strategy strategy_base=momentum source=open_position" in output
    assert "open_position_id=open-ms-1" in output
    assert "entry_component=g1" in output


def test_main_show_trade_reasons_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_trade_reasons(storage_path, limit, window, kinds=None):
        captured["storage_path"] = storage_path
        captured["limit"] = limit
        captured["window"] = window
        captured["kinds"] = kinds
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_trade_reasons", fake_show_trade_reasons)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-trade-reasons", "--trade-reasons-limit", "5", "--trade-reasons-window", "250"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["limit"] == 5
    assert captured["window"] == 250
    assert captured["kinds"] is None


def test_main_show_entry_attempts_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_entry_attempts(storage_path, limit, window):
        captured["storage_path"] = storage_path
        captured["limit"] = limit
        captured["window"] = window
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_entry_attempts", fake_show_entry_attempts)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-entry-attempts", "--entry-attempts-limit", "9", "--entry-attempts-window", "400"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["limit"] == 9
    assert captured["window"] == 400


@pytest.mark.parametrize(
    ("argv", "required_flag"),
    [
        (["run_bot.py", "--entry-attempts-limit", "5"], "--show-entry-attempts"),
        (["run_bot.py", "--trade-reasons-only-rejects"], "--show-trade-reasons"),
        (["run_bot.py", "--trade-timeline-limit", "5"], "--show-trade-timeline"),
        (["run_bot.py", "--show-trades-live-sync"], "--show-trades"),
    ],
)
def test_main_rejects_readonly_modifier_without_primary_command(monkeypatch, capsys, argv, required_flag):
    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        run_bot,
        "TradingBot",
        lambda config: (_ for _ in ()).throw(AssertionError("TradingBot should not be instantiated")),
    )
    monkeypatch.setattr("sys.argv", argv)

    with pytest.raises(SystemExit) as exc:
        run_bot.main()

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert required_flag in err


def test_main_export_position_updates_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_export(storage_path, limit):
        captured["storage_path"] = storage_path
        captured["limit"] = limit
        return 2

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_export_position_updates", fake_export)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--export-position-updates", "--position-updates-limit", "500"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["limit"] == 500


def test_main_show_ig_allowance_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_ig_allowance(storage_path, window, symbols_limit):
        captured["storage_path"] = storage_path
        captured["window"] = window
        captured["symbols_limit"] = symbols_limit
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_ig_allowance", fake_show_ig_allowance)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-ig-allowance", "--ig-allowance-window", "350", "--ig-allowance-symbols", "7"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["window"] == 350
    assert captured["symbols_limit"] == 7


def test_main_show_db_first_health_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_db_first_health(storage_path, window, symbols_limit):
        captured["storage_path"] = storage_path
        captured["window"] = window
        captured["symbols_limit"] = symbols_limit
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_show_db_first_health", fake_show_db_first_health)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--show-db-first-health", "--db-first-health-window", "777", "--db-first-health-symbols", "9"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["window"] == 777
    assert captured["symbols_limit"] == 9


def test_main_cleanup_incompatible_open_trades_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_cleanup(storage_path, dry_run):
        captured["storage_path"] = storage_path
        captured["dry_run"] = dry_run
        return 2

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "_resolve_storage_path", lambda _: Path("/tmp/state.db"))
    monkeypatch.setattr(run_bot, "_cleanup_incompatible_open_trades", fake_cleanup)
    monkeypatch.setattr(
        run_bot,
        "load_config",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("load_config should not be called")),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--cleanup-incompatible-open-trades", "--cleanup-dry-run"],
    )

    run_bot.main()

    assert captured["storage_path"] == Path("/tmp/state.db")
    assert captured["dry_run"] is True


def test_main_sync_open_positions_calls_helper(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="momentum",
        strategy_params={},
        storage_path=Path("/tmp/original.db"),
        strict_broker_connect=False,
    )
    captured: dict[str, object] = {}

    def fake_sync_open_positions(config):
        captured["config"] = config
        return 1

    monkeypatch.setattr(run_bot, "_load_dotenv", lambda path: None)
    monkeypatch.setattr(run_bot, "load_config", lambda **kwargs: cfg)
    monkeypatch.setattr(run_bot, "_sync_open_positions", fake_sync_open_positions)
    monkeypatch.setattr(run_bot, "available_strategies", lambda: ["momentum"])
    monkeypatch.setattr(
        "sys.argv",
        ["run_bot.py", "--sync-open-positions", "--storage-path", "/tmp/custom.db"],
    )

    run_bot.main()

    used_cfg = captured["config"]
    assert used_cfg is cfg
    assert cfg.strict_broker_connect is True
    assert cfg.storage_path == Path("/tmp/custom.db")


def test_main_applies_aggressive_strategy_profile_for_index_hybrid(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="index_hybrid",
        strategy_params={"index_fast_ema_window": 34, "index_session_filter_enabled": True},
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "aggressive"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["index_fast_ema_window"] == 13
    assert cfg.strategy_params["index_session_filter_enabled"] is True
    assert cfg.strategy_params["index_parameter_surface_mode"] == "tiered"
    assert cfg.strategy_params["index_parameter_surface_tier"] == "aggressive"
    assert cfg.strategy_params["index_parameter_surface_locked"] is True
    assert cfg.strategy_params["index_zscore_mode"] == "detrended"


def test_main_keeps_index_hybrid_tunable_overrides_when_profile_is_applied(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="index_hybrid",
        strategy_params={
            "index_zscore_mode": "classic",
            "index_auto_correct_regime_thresholds": False,
            "index_enforce_gap_hysteresis": False,
            "index_gap_hysteresis_min": 0.00025,
        },
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "aggressive"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["index_parameter_surface_mode"] == "tiered"
    assert cfg.strategy_params["index_parameter_surface_tier"] == "aggressive"
    assert cfg.strategy_params["index_zscore_mode"] == "classic"
    assert cfg.strategy_params["index_auto_correct_regime_thresholds"] is False
    assert cfg.strategy_params["index_enforce_gap_hysteresis"] is False
    assert cfg.strategy_params["index_gap_hysteresis_min"] == 0.00025


def test_main_applies_safe_strategy_profile_for_momentum(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="momentum",
        strategy_params={"momentum_confirm_bars": 2},
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "safe"],
        cfg,
    )
    assert captured.get("ran") is True
    # "safe" maps to conservative profile which overrides params
    assert cfg.strategy_params["momentum_confirm_bars"] == 2
    assert cfg.strategy_params["momentum_atr_multiplier"] == 2.0
    assert cfg.strategy_params["momentum_risk_reward_ratio"] == 2.5
    assert cfg.strategy_params["momentum_execution_min_confidence_for_entry"] == 0.74


def test_main_applies_conservative_strategy_profile_for_mean_breakout_v2(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_breakout_v2",
        strategy_params={
            "mb_zscore_threshold": 1.6,
            "mb_atr_multiplier": 1.5,
            "mb_min_stop_loss_pips": 60.0,
        },
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "conservative"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.85
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.5
    assert cfg.strategy_params["mb_min_stop_loss_pips"] == 80.0
    assert cfg.strategy_params["mb_adaptive_sl_max_atr_ratio"] == 5.0
    assert cfg.strategy_params["mb_trailing_breakeven_offset_pips"] == 3.0
    assert cfg.strategy_params["mb_slope_window"] == 7
    assert cfg.strategy_params["mb_breakout_min_buffer_spread_multiplier"] == 2.0


def test_main_applies_aggressive_strategy_profile_for_mean_breakout_v2(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_breakout_v2",
        strategy_params={
            "mb_zscore_threshold": 1.6,
            "mb_atr_multiplier": 1.5,
            "mb_min_stop_loss_pips": 60.0,
        },
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "aggressive"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.75
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.45
    assert cfg.strategy_params["mb_min_stop_loss_pips"] == 50.0
    assert cfg.strategy_params["mb_adaptive_sl_max_atr_ratio"] == 4.0
    assert cfg.strategy_params["mb_trailing_breakeven_offset_pips"] == 2.0
    assert cfg.strategy_params["mb_slope_window"] == 5
    assert cfg.strategy_params["mb_breakout_min_buffer_spread_multiplier"] == 1.5


def test_main_applies_strategy_profile_from_env_for_mean_breakout_v2(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_breakout_v2",
        strategy_params={
            "mb_zscore_threshold": 1.6,
            "mb_atr_multiplier": 1.5,
        },
    )
    monkeypatch.setenv("XTB_STRATEGY_PROFILE", "conservative")
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.85
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.5


def test_main_applies_conservative_strategy_profile_for_mean_reversion_bb(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_reversion_bb",
        strategy_params={
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_min_confidence_for_entry": 0.5,
            "mean_reversion_bb_trade_cooldown_sec": 300.0,
        },
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "conservative"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 85.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.62
    assert cfg.strategy_params["mean_reversion_bb_regime_min_adx"] == 16.0
    assert cfg.strategy_params["mean_reversion_bb_regime_max_adx"] == 20.0
    assert cfg.strategy_params["mean_reversion_bb_reentry_tolerance_sigma"] == 0.18
    assert cfg.strategy_params["mean_reversion_bb_reentry_min_reversal_sigma"] == 0.22
    assert cfg.strategy_params["mean_reversion_bb_exit_on_midline"] is False
    assert cfg.strategy_params["mean_reversion_bb_trade_cooldown_sec"] == 360.0


def test_main_applies_aggressive_strategy_profile_for_mean_reversion_bb(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_reversion_bb",
        strategy_params={
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_min_confidence_for_entry": 0.5,
            "mean_reversion_bb_trade_cooldown_sec": 300.0,
        },
    )
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py", "--strategy-profile", "aggressive"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 80.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.45
    assert cfg.strategy_params["mean_reversion_bb_regime_min_adx"] == 13.0
    assert cfg.strategy_params["mean_reversion_bb_regime_max_adx"] == 21.0
    assert cfg.strategy_params["mean_reversion_bb_reentry_tolerance_sigma"] == 0.14
    assert cfg.strategy_params["mean_reversion_bb_reentry_min_reversal_sigma"] == 0.16
    assert cfg.strategy_params["mean_reversion_bb_exit_on_midline"] is False
    assert cfg.strategy_params["mean_reversion_bb_trade_cooldown_sec"] == 240.0


def test_main_applies_strategy_profile_from_env_for_mean_reversion_bb(monkeypatch):
    cfg = types.SimpleNamespace(
        strategy="mean_reversion_bb",
        strategy_params={
            "mean_reversion_bb_rsi_overbought": 70.0,
            "mean_reversion_bb_min_confidence_for_entry": 0.5,
        },
    )
    monkeypatch.setenv("XTB_STRATEGY_PROFILE", "conservative")
    captured = _run_main_with_config(
        monkeypatch,
        ["run_bot.py"],
        cfg,
    )
    assert captured.get("ran") is True
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 85.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.62
    assert cfg.strategy_params["mean_reversion_bb_reentry_tolerance_sigma"] == 0.18
    assert cfg.strategy_params["mean_reversion_bb_exit_on_midline"] is False


def test_show_trades_prints_entry_confidence(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="paper-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.731,
                status="open",
            ),
            "worker-EURUSD",
            "momentum",
            "paper",
        )
    finally:
        store.close()

    printed = run_bot._show_trades(storage_path, limit=10, open_only=False)
    output = capsys.readouterr().out

    assert printed == 1
    assert "conf=0.731" in output
    assert "conf_gate=pass@0.550" in output


def test_show_trades_filters_by_strategy(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="paper-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.731,
                status="open",
            ),
            "worker-EURUSD",
            "momentum",
            "paper",
        )
        store.upsert_trade(
            Position(
                position_id="paper-2",
                symbol="DE40",
                side=Side.SELL,
                volume=0.2,
                open_price=20000.0,
                stop_loss=20050.0,
                take_profit=19900.0,
                opened_at=20.0,
                entry_confidence=0.88,
                status="open",
            ),
            "worker-DE40",
            "index_hybrid",
            "paper",
        )
    finally:
        store.close()

    printed = run_bot._show_trades(storage_path, limit=10, open_only=False, strategy="momentum")
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy=momentum" in output
    assert "strategy=index_hybrid" not in output


def test_show_trades_prefers_strategy_entry_component_for_display_and_filter(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="exec-1",
                symbol="WTI",
                side=Side.BUY,
                volume=0.2,
                open_price=10192.6,
                stop_loss=10162.0,
                take_profit=10254.0,
                opened_at=10.0,
                entry_confidence=0.9,
                status="closed",
                close_price=10162.0,
                closed_at=20.0,
                pnl=-70.38,
            ),
            "worker-WTI",
            "momentum",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="g1",
        )
    finally:
        store.close()

    printed = run_bot._show_trades(storage_path, limit=10, open_only=False, strategy="g1")
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy=g1" in output
    assert "strategy_entry=g1" in output
    assert "strategy_base=momentum" in output


def test_trade_strategy_labels_use_entry_base_for_multi_strategy_carrier():
    strategy, strategy_entry, strategy_base, strategy_exit = run_bot._trade_strategy_labels(
        {
            "strategy": "multi_strategy",
            "strategy_entry_base": "momentum",
            "strategy_entry_component": "g1",
            "strategy_exit": "momentum",
        }
    )

    assert strategy == "g1"
    assert strategy_entry == "g1"
    assert strategy_base == "momentum"
    assert strategy_exit == "momentum"


def test_trade_strategy_labels_map_generic_multi_strategy_exit_to_entry_component():
    strategy, strategy_entry, strategy_base, strategy_exit = run_bot._trade_strategy_labels(
        {
            "strategy": "multi_strategy",
            "strategy_entry_base": "momentum",
            "strategy_entry_component": "g1",
            "strategy_exit": "momentum",
            "close_reason": "broker_manual_close:mark:ig_history_activity",
        }
    )

    assert strategy == "g1"
    assert strategy_entry == "g1"
    assert strategy_base == "momentum"
    assert strategy_exit == "g1"


def test_trade_strategy_labels_prefers_explicit_exit_component_metadata():
    strategy, strategy_entry, strategy_base, strategy_exit = run_bot._trade_strategy_labels(
        {
            "strategy": "multi_strategy",
            "strategy_entry_base": "momentum",
            "strategy_entry_component": "g1",
            "strategy_exit": "momentum",
            "strategy_exit_component": "index_hybrid",
            "strategy_exit_signal": "index_hybrid:trend_mid_reentry",
        }
    )

    assert strategy == "g1"
    assert strategy_entry == "g1"
    assert strategy_base == "momentum"
    assert strategy_exit == "index_hybrid"


def test_show_trades_multi_strategy_carrier_keeps_carrier_filter_and_base_component_label(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="exec-ms-1",
                symbol="WTI",
                side=Side.BUY,
                volume=0.2,
                open_price=10192.6,
                stop_loss=10162.0,
                take_profit=10254.0,
                opened_at=10.0,
                entry_confidence=0.9,
                status="closed",
                close_price=10162.0,
                closed_at=20.0,
                pnl=-70.38,
            ),
            "worker-WTI",
            "multi_strategy",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="g1",
        )
    finally:
        store.close()

    printed = run_bot._show_trades(storage_path, limit=10, open_only=False, strategy="multi_strategy")
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy=g1" in output
    assert "strategy_entry=g1" in output
    assert "strategy_base=momentum" in output
    assert "strategy_base=multi_strategy" not in output


def test_show_trade_timeline_prefers_strategy_component_and_summarizes_updates(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        position_id = "exec-2"
        store.upsert_trade(
            Position(
                position_id=position_id,
                symbol="WTI",
                side=Side.BUY,
                volume=0.23,
                open_price=10192.6,
                stop_loss=10162.0,
                take_profit=10254.0,
                opened_at=10.0,
                entry_confidence=0.9,
                status="closed",
                close_price=10162.0,
                closed_at=20.0,
                pnl=-70.38,
            ),
            "worker-WTI",
            "momentum",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="g1",
            strategy_exit="momentum",
        )
        store.update_trade_performance(
            position_id=position_id,
            symbol="WTI",
            opened_at=10.0,
            closed_at=20.0,
            close_reason="broker_manual_close:mark:ig_history_activity",
            max_favorable_pips=1.3,
            max_adverse_pips=17.7,
            max_favorable_pnl=2.99,
            max_adverse_pnl=-70.38,
        )
        store.record_position_update(
            {
                "ts": 10.1,
                "symbol": "WTI",
                "position_id": position_id,
                "operation": "confirm",
                "method": "GET",
                "path": "/confirms/XTB1",
                "http_status": 200,
                "success": True,
                "source": "ig_rest",
            }
        )
        for ts in (10.2, 11.2):
            store.record_position_update(
                {
                    "ts": ts,
                    "symbol": "WTI",
                    "position_id": position_id,
                    "operation": "position_details",
                    "method": "GET",
                    "path": f"/positions/{position_id}",
                    "http_status": 200,
                    "success": True,
                    "source": "ig_rest",
                }
            )
        for ts in (20.2, 21.2):
            store.record_position_update(
                {
                    "ts": ts,
                    "symbol": "WTI",
                    "position_id": position_id,
                    "operation": "position_details",
                    "method": "GET",
                    "path": f"/positions/{position_id}",
                    "http_status": 404,
                    "success": False,
                    "source": "ig_rest",
                    "error_text": "error.position.notfound",
                }
            )
    finally:
        store.close()

    con = sqlite3.connect(str(storage_path))
    try:
        event_rows = [
            (10.0, "INFO", "WTI", "Trade deal reference bound", {"position_id": position_id}),
            (
                10.1,
                "INFO",
                "WTI",
                "Position opened",
                {
                    "position_id": position_id,
                    "strategy_entry": "momentum",
                    "strategy_entry_component": "g1",
                    "strategy_base": "momentum",
                    "confidence": 0.9,
                },
            ),
            (10.2, "INFO", "WTI", "Stream usage metrics", {"position_id": position_id}),
            (
                20.0,
                "INFO",
                "WTI",
                "Position closed",
                {
                    "position_id": position_id,
                    "reason": "stop_loss",
                    "strategy_entry": "momentum",
                    "strategy_entry_component": "g1",
                    "strategy_base": "momentum",
                    "strategy_exit": "g1",
                    "pnl": -70.38,
                    "close_price": 10162.0,
                },
            ),
        ]
        con.executemany(
            "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
            [
                (ts, level, symbol, message, json.dumps(payload))
                for ts, level, symbol, message, payload in event_rows
            ],
        )
        con.commit()
    finally:
        con.close()

    printed = run_bot._show_trade_timeline(storage_path, limit=1, events_limit=20)
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy=g1 strategy_base=momentum strategy_exit=g1" in output
    assert "close_reason=stop_loss broker_close_reason=broker_manual_close:mark:ig_history_activity" in output
    assert "Stream usage metrics" in output
    assert "suppressed 2 technical events" not in output
    assert "Position opened strategy=g1 strategy_base=momentum conf=0.900" in output
    assert "Position closed reason=stop_loss strategy=g1 strategy_base=momentum strategy_exit=g1 pnl=-70.38 close_price=10162.00000" in output
    assert f"position_details GET /positions/{position_id} http=200 success=1 source=ig_rest x2" in output
    assert f"position_details GET /positions/{position_id} http=404 success=0 source=ig_rest x2 error=error.position.notfound" in output


def test_show_trades_handles_legacy_db_without_entry_confidence(tmp_path, capsys):
    storage_path = tmp_path / "legacy.db"
    con = sqlite3.connect(str(storage_path))
    try:
        con.execute(
            """
            CREATE TABLE trades (
                position_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                volume REAL NOT NULL,
                open_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit REAL NOT NULL,
                opened_at REAL NOT NULL,
                status TEXT NOT NULL,
                close_price REAL,
                closed_at REAL,
                pnl REAL NOT NULL,
                thread_name TEXT NOT NULL,
                strategy TEXT NOT NULL,
                mode TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        con.execute(
            """
            INSERT INTO trades (
                position_id, symbol, side, volume, open_price, stop_loss, take_profit,
                opened_at, status, close_price, closed_at, pnl, thread_name, strategy, mode, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-1",
                "EURUSD",
                "buy",
                0.1,
                1.1,
                1.095,
                1.11,
                10.0,
                "open",
                None,
                None,
                0.0,
                "worker-EURUSD",
                "momentum",
                "paper",
                11.0,
            ),
        )
        con.commit()
    finally:
        con.close()

    printed = run_bot._show_trades(storage_path, limit=10, open_only=False)
    output = capsys.readouterr().out

    assert printed == 1
    assert "conf=0.000" in output
    assert "conf_gate=below@0.550" in output


def test_show_trade_confidence_prints_bucket_analysis(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="t1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.18,
                status="closed",
                close_price=1.1010,
                closed_at=11.0,
                pnl=-10.0,
            ),
            "worker-EURUSD",
            "momentum",
            "paper",
        )
        store.upsert_trade(
            Position(
                position_id="t2",
                symbol="US100",
                side=Side.SELL,
                volume=0.1,
                open_price=100.0,
                stop_loss=101.0,
                take_profit=98.0,
                opened_at=12.0,
                entry_confidence=0.62,
                status="closed",
                close_price=98.5,
                closed_at=13.0,
                pnl=25.0,
            ),
            "worker-US100",
            "momentum",
            "execution",
        )
        store.upsert_trade(
            Position(
                position_id="t3",
                symbol="DE40",
                side=Side.BUY,
                volume=0.1,
                open_price=200.0,
                stop_loss=198.0,
                take_profit=205.0,
                opened_at=14.0,
                entry_confidence=0.88,
                status="closed",
                close_price=204.0,
                closed_at=15.0,
                pnl=40.0,
            ),
            "worker-DE40",
            "index_hybrid",
            "paper",
        )
    finally:
        store.close()

    printed = run_bot._show_trade_confidence(storage_path)
    output = capsys.readouterr().out

    assert printed == 3
    assert "Trade confidence analysis:" in output
    assert "closed_trades=3" in output
    assert "[0.00,0.25) | trades=1" in output
    assert "[0.50,0.75) | trades=1" in output
    assert "[0.75,1.00] | trades=1" in output
    assert "momentum | trades=2" in output
    assert "ref_signal_only=0.550 ref_paper=0.550 ref_execution=0.650" in output
    assert "index_hybrid | trades=1" in output
    assert "ref_signal_only=0.650 ref_paper=0.650 ref_execution=0.750" in output
    assert "momentum/paper | trades=1 avg_conf=0.180 threshold=0.550" in output
    assert "momentum/execution | trades=1 avg_conf=0.620 threshold=0.650" in output


def test_show_trade_confidence_filters_by_strategy(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="t1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.18,
                status="closed",
                close_price=1.1010,
                closed_at=11.0,
                pnl=-10.0,
            ),
            "worker-EURUSD",
            "momentum",
            "paper",
        )
        store.upsert_trade(
            Position(
                position_id="t2",
                symbol="DE40",
                side=Side.BUY,
                volume=0.1,
                open_price=200.0,
                stop_loss=198.0,
                take_profit=205.0,
                opened_at=14.0,
                entry_confidence=0.88,
                status="closed",
                close_price=204.0,
                closed_at=15.0,
                pnl=40.0,
            ),
            "worker-DE40",
            "index_hybrid",
            "paper",
        )
    finally:
        store.close()

    printed = run_bot._show_trade_confidence(storage_path, strategy="momentum")
    output = capsys.readouterr().out

    assert printed == 1
    assert "closed_trades=1" in output
    assert "momentum | trades=1" in output
    assert "index_hybrid | trades=1" not in output


def test_show_fill_quality_prints_strategy_and_symbol_rankings(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        store.upsert_trade(
            Position(
                position_id="fill-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=10.0,
                entry_confidence=0.60,
                status="closed",
                close_price=1.1020,
                closed_at=11.0,
                pnl=20.0,
            ),
            "worker-EURUSD",
            "momentum",
            "execution",
            strategy_entry="momentum",
        )
        store.upsert_trade(
            Position(
                position_id="fill-2",
                symbol="US100",
                side=Side.SELL,
                volume=0.1,
                open_price=100.0,
                stop_loss=101.0,
                take_profit=98.0,
                opened_at=12.0,
                entry_confidence=0.70,
                status="closed",
                close_price=99.0,
                closed_at=13.0,
                pnl=15.0,
            ),
            "worker-US100",
            "g1",
            "execution",
            strategy_entry="g1",
        )
        store.upsert_trade(
            Position(
                position_id="fill-3",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1010,
                stop_loss=1.0960,
                take_profit=1.1110,
                opened_at=14.0,
                entry_confidence=0.72,
                status="closed",
                close_price=1.1030,
                closed_at=15.0,
                pnl=20.0,
            ),
            "worker-EURUSD",
            "momentum",
            "execution",
            strategy_entry="momentum",
        )
        store.update_trade_execution_quality(
            position_id="fill-1",
            symbol="EURUSD",
            entry_slippage_pips=3.0,
            exit_slippage_pips=2.0,
            entry_adverse_slippage_pips=3.0,
            exit_adverse_slippage_pips=2.0,
        )
        store.update_trade_execution_quality(
            position_id="fill-2",
            symbol="US100",
            entry_slippage_pips=1.0,
            exit_slippage_pips=1.0,
            entry_adverse_slippage_pips=1.0,
            exit_adverse_slippage_pips=1.0,
        )
        store.update_trade_execution_quality(
            position_id="fill-3",
            symbol="EURUSD",
            entry_slippage_pips=2.0,
            exit_slippage_pips=1.0,
            entry_adverse_slippage_pips=2.0,
            exit_adverse_slippage_pips=1.0,
        )
    finally:
        store.close()

    printed = run_bot._show_fill_quality(storage_path, limit=5)
    output = capsys.readouterr().out

    assert printed == 3
    assert "Fill quality analysis:" in output
    assert "closed_trades_with_fill_quality=3" in output
    assert "strategies (worst first):" in output
    assert "momentum | samples=2" in output
    assert "g1 | samples=1" in output
    assert "symbol_strategy (worst first):" in output
    assert "EURUSD momentum | samples=2" in output
    assert "US100 g1 | samples=1" in output
    assert "avg_total_adv=4.00p" in output
    assert "adverse_share=100.0%" in output


def test_show_fill_quality_filters_by_strategy_entry(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        for position_id, symbol, strategy_entry in (
            ("fill-a", "EURUSD", "momentum"),
            ("fill-b", "DE40", "index_hybrid"),
        ):
            store.upsert_trade(
                Position(
                    position_id=position_id,
                    symbol=symbol,
                    side=Side.BUY,
                    volume=0.1,
                    open_price=1.1000,
                    stop_loss=1.0950,
                    take_profit=1.1100,
                    opened_at=10.0,
                    entry_confidence=0.60,
                    status="closed",
                    close_price=1.1020,
                    closed_at=11.0,
                    pnl=20.0,
                ),
                f"worker-{symbol}",
                strategy_entry,
                "execution",
                strategy_entry=strategy_entry,
            )
            store.update_trade_execution_quality(
                position_id=position_id,
                symbol=symbol,
                entry_slippage_pips=1.0,
                exit_slippage_pips=1.0,
                entry_adverse_slippage_pips=1.0,
                exit_adverse_slippage_pips=1.0,
            )
    finally:
        store.close()

    printed = run_bot._show_fill_quality(storage_path, strategy="momentum", limit=5)
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy_filter=momentum" in output
    assert "momentum | samples=1" in output
    assert "index_hybrid | samples=1" not in output


def test_show_strategy_scorecard_prints_ranked_management_view(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        trade_specs = [
            ("score-1", "EURUSD", "momentum", 35.0, 0.78, 40.0, -10.0, 0.2, 0.1),
            ("score-2", "US100", "momentum", 25.0, 0.74, 30.0, -8.0, 0.3, 0.2),
            ("score-3", "EURUSD", "g1", -20.0, 0.62, 10.0, -15.0, 2.5, 1.5),
            ("score-4", "DE40", "g1", -25.0, 0.58, 12.0, -18.0, 3.0, 2.0),
        ]
        for idx, (position_id, symbol, strategy_entry, pnl, confidence, max_favorable_pnl, max_adverse_pnl, entry_adv, exit_adv) in enumerate(trade_specs, start=1):
            opened_at = float(idx * 10)
            closed_at = opened_at + 1.0
            store.upsert_trade(
                Position(
                    position_id=position_id,
                    symbol=symbol,
                    side=Side.BUY if pnl >= 0.0 else Side.SELL,
                    volume=0.1,
                    open_price=1.1000 if symbol == "EURUSD" else 100.0 + idx,
                    stop_loss=1.0950 if symbol == "EURUSD" else 99.0 + idx,
                    take_profit=1.1100 if symbol == "EURUSD" else 103.0 + idx,
                    opened_at=opened_at,
                    entry_confidence=confidence,
                    status="closed",
                    close_price=1.1020 if symbol == "EURUSD" else 101.0 + idx,
                    closed_at=closed_at,
                    pnl=pnl,
                ),
                f"worker-{symbol}",
                strategy_entry,
                "execution",
                strategy_entry=strategy_entry,
            )
            store.update_trade_performance(
                position_id=position_id,
                symbol=symbol,
                max_favorable_pips=max_favorable_pnl / 10.0,
                max_adverse_pips=abs(max_adverse_pnl) / 10.0,
                max_favorable_pnl=max_favorable_pnl,
                max_adverse_pnl=max_adverse_pnl,
                closed_at=closed_at,
            )
            store.update_trade_execution_quality(
                position_id=position_id,
                symbol=symbol,
                entry_slippage_pips=entry_adv,
                exit_slippage_pips=exit_adv,
                entry_adverse_slippage_pips=entry_adv,
                exit_adverse_slippage_pips=exit_adv,
            )
    finally:
        store.close()

    printed = run_bot._show_strategy_scorecard(storage_path, limit=5)
    output = capsys.readouterr().out

    assert printed == 4
    assert "Strategy scorecard:" in output
    assert "closed_trades=4" in output
    assert "ranked_strategies:" in output
    assert "momentum | score=" in output
    assert "verdict=promote" in output
    assert "g1 | score=" in output
    assert "verdict=demote" in output
    assert "hotspots:" in output
    assert "EURUSD g1 | score=" in output


def test_show_strategy_scorecard_filters_by_strategy_entry(tmp_path, capsys):
    storage_path = tmp_path / "state.db"
    store = StateStore(storage_path)
    try:
        for position_id, symbol, strategy_entry, pnl in (
            ("score-a", "EURUSD", "momentum", 18.0),
            ("score-b", "DE40", "index_hybrid", 22.0),
        ):
            store.upsert_trade(
                Position(
                    position_id=position_id,
                    symbol=symbol,
                    side=Side.BUY,
                    volume=0.1,
                    open_price=1.1000,
                    stop_loss=1.0950,
                    take_profit=1.1100,
                    opened_at=10.0,
                    entry_confidence=0.70,
                    status="closed",
                    close_price=1.1020,
                    closed_at=11.0,
                    pnl=pnl,
                ),
                f"worker-{symbol}",
                strategy_entry,
                "execution",
                strategy_entry=strategy_entry,
            )
            store.update_trade_performance(
                position_id=position_id,
                symbol=symbol,
                max_favorable_pips=4.0,
                max_adverse_pips=1.5,
                max_favorable_pnl=24.0,
                max_adverse_pnl=-8.0,
                closed_at=11.0,
            )
            store.update_trade_execution_quality(
                position_id=position_id,
                symbol=symbol,
                entry_slippage_pips=0.5,
                exit_slippage_pips=0.3,
                entry_adverse_slippage_pips=0.5,
                exit_adverse_slippage_pips=0.3,
            )
    finally:
        store.close()

    printed = run_bot._show_strategy_scorecard(storage_path, strategy="momentum", limit=5)
    output = capsys.readouterr().out

    assert printed == 1
    assert "strategy_filter=momentum" in output
    assert "momentum | score=" in output
    assert "index_hybrid | score=" not in output
