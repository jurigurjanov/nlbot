from __future__ import annotations

import sqlite3
import types
from pathlib import Path

import run_bot
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


def test_main_show_trade_reasons_calls_helper(monkeypatch):
    captured: dict[str, object] = {}

    def fake_show_trade_reasons(storage_path, limit, window, **kwargs):
        captured["storage_path"] = storage_path
        captured["limit"] = limit
        captured["window"] = window
        captured.update(kwargs)
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
    assert cfg.strategy_params["index_session_filter_enabled"] is False


def test_main_ignores_strategy_profile_for_non_index_hybrid(monkeypatch):
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
    assert cfg.strategy_params == {"momentum_confirm_bars": 2}


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
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.4
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.8
    assert cfg.strategy_params["mb_min_stop_loss_pips"] == 80.0


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
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.9
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.35
    assert cfg.strategy_params["mb_min_stop_loss_pips"] == 50.0


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
    assert cfg.strategy_params["mb_zscore_threshold"] == 1.4
    assert cfg.strategy_params["mb_atr_multiplier"] == 1.8


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
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 72.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.60
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
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 68.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.45
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
    assert cfg.strategy_params["mean_reversion_bb_rsi_overbought"] == 72.0
    assert cfg.strategy_params["mean_reversion_bb_min_confidence_for_entry"] == 0.60


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
