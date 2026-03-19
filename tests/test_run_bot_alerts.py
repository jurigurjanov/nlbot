from __future__ import annotations

import sqlite3
from pathlib import Path

from run_bot import (
    _cleanup_incompatible_open_trades,
    _reset_risk_anchor,
    _resolve_storage_path,
    _show_alerts,
    _show_balance,
    _show_ig_allowance,
    _show_status,
    _show_trade_reasons,
    _show_trades,
)
from xtb_bot.models import AccountSnapshot
from xtb_bot.models import Position, Side
from xtb_bot.state_store import StateStore


def test_resolve_storage_path_priority(monkeypatch):
    monkeypatch.delenv("BOT_STORAGE_PATH", raising=False)
    monkeypatch.delenv("IG_STORAGE_PATH", raising=False)
    monkeypatch.delenv("XTB_STORAGE_PATH", raising=False)
    resolved_default = _resolve_storage_path(None)
    assert resolved_default == Path("./state/xtb_bot.db")

    monkeypatch.setenv("XTB_STORAGE_PATH", "./from-env.db")
    resolved_env = _resolve_storage_path(None)
    assert resolved_env == Path("./from-env.db")

    resolved_override = _resolve_storage_path("./from-override.db")
    assert resolved_override == Path("./from-override.db")


def test_show_alerts_prints_filtered_events(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Heartbeat", {"ok": True})
        store.record_event("WARN", None, "Daily drawdown lock activated", {"unlock_at": "2026-02-27 00:00:00"})
        store.record_event("INFO", None, "Daily drawdown lock released", {"released_day": "2026-02-27"})
        store.record_event("ERROR", "EURUSD", "Broker error", {"error": "network"})
    finally:
        store.close()

    count = _show_alerts(db_path, limit=10)
    output = capsys.readouterr().out

    assert count == 3
    assert "Daily drawdown lock activated" in output
    assert "Daily drawdown lock released" in output
    assert "Broker error" in output
    assert "Heartbeat" not in output


def test_show_alerts_includes_hold_reason_info_event(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event(
            "INFO",
            "EURUSD",
            "Signal hold reason",
            {"reason": "no_ema_cross", "strategy": "g1"},
        )
    finally:
        store.close()

    count = _show_alerts(db_path, limit=10)
    output = capsys.readouterr().out

    assert count == 1
    assert "Signal hold reason" in output
    assert "no_ema_cross" in output


def test_show_balance_prints_latest_account_snapshot(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_account_snapshot(
            snapshot=AccountSnapshot(
                timestamp=1000.0,
                balance=9999.5,
                equity=10010.2,
                margin_free=9950.1,
            ),
            open_positions=2,
            daily_pnl=10.2,
            drawdown_pct=0.4,
        )
    finally:
        store.close()

    count = _show_balance(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "balance=9999.50" in output
    assert "equity=10010.20" in output
    assert "margin_free=9950.10" in output
    assert "open_positions=2" in output
    assert "daily_pnl=10.20" in output
    assert "drawdown_pct=0.40" in output


def test_show_status_prints_housekeeping_summary(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(
        db_path,
        housekeeping_interval_sec=0.0,
        housekeeping_events_keep_rows=10,
        housekeeping_incremental_vacuum_pages=0,
    )
    try:
        for idx in range(15):
            store.record_event("INFO", "EURUSD", f"evt-{idx}", {"i": idx})
        store.record_account_snapshot(
            snapshot=AccountSnapshot(
                timestamp=1000.0,
                balance=9999.5,
                equity=10010.2,
                margin_free=9950.1,
            ),
            open_positions=2,
            daily_pnl=10.2,
            drawdown_pct=0.4,
        )
        store.run_housekeeping(force=True)
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "State status | path=" in output
    assert "trades_open=" in output
    assert "events_total=" in output
    assert "latest_snapshot=" in output
    assert "db_housekeeping=" in output
    assert "db_checkpoint=" in output


def test_show_status_handles_missing_housekeeping_summary(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Heartbeat", {"ok": True})
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "db_housekeeping=not_available" in output


def test_show_trades_prints_open_and_closed(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="p-open",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.11111,
                stop_loss=1.10111,
                take_profit=1.13111,
                opened_at=1000.0,
                status="open",
                pnl=12.34,
            ),
            thread_name="t-eurusd",
            strategy="momentum",
            mode="paper",
        )
        store.upsert_trade(
            Position(
                position_id="p-closed",
                symbol="USDJPY",
                side=Side.SELL,
                volume=0.2,
                open_price=157.12,
                stop_loss=157.62,
                take_profit=156.12,
                opened_at=2000.0,
                status="closed",
                close_price=156.88,
                closed_at=2100.0,
                pnl=45.67,
            ),
            thread_name="t-usdjpy",
            strategy="g1",
            mode="paper",
        )
    finally:
        store.close()

    count = _show_trades(db_path, limit=10, open_only=False)
    output = capsys.readouterr().out

    assert count == 2
    assert "OPEN EURUSD BUY" in output
    assert "CLOSED USDJPY SELL" in output
    assert "closed_at:" in output
    assert "id=p-open" in output
    assert "id=p-closed" in output


def test_show_trades_open_only_filter(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="p-open",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.11111,
                stop_loss=1.10111,
                take_profit=1.13111,
                opened_at=1000.0,
                status="open",
                pnl=0.0,
            ),
            thread_name="t-eurusd",
            strategy="momentum",
            mode="paper",
        )
        store.upsert_trade(
            Position(
                position_id="p-closed",
                symbol="USDJPY",
                side=Side.SELL,
                volume=0.2,
                open_price=157.12,
                stop_loss=157.62,
                take_profit=156.12,
                opened_at=2000.0,
                status="closed",
                close_price=156.88,
                closed_at=2100.0,
                pnl=45.67,
            ),
            thread_name="t-usdjpy",
            strategy="g1",
            mode="paper",
        )
    finally:
        store.close()

    count = _show_trades(db_path, limit=10, open_only=True)
    output = capsys.readouterr().out

    assert count == 1
    assert "OPEN EURUSD BUY" in output
    assert "CLOSED USDJPY SELL" not in output


def test_show_trade_reasons_aggregates_close_block_and_hold(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Position closed", {"reason": "stop_loss", "pnl": -10.0})
        store.record_event("INFO", "US100", "Position closed", {"reason": "stop_loss", "pnl": -20.0})
        store.record_event("INFO", "USDJPY", "Position closed", {"reason": "take_profit", "pnl": 15.0})
        store.record_event("WARN", "EURUSD", "Trade blocked by spread filter", {"spread": 5.0})
        store.record_event("INFO", "EURUSD", "Trade blocked by entry cooldown", {"remaining_sec": 12.0})
        store.record_event("INFO", "EURUSD", "Signal hold reason", {"reason": "no_ma_cross"})
    finally:
        store.close()

    count = _show_trade_reasons(db_path, limit=10, window=100)
    output = capsys.readouterr().out

    assert count >= 5
    assert "Trade reason summary" in output
    assert "close | stop_loss | count=2" in output
    assert "pnl_sum=-30.00" in output
    assert "close | take_profit | count=1" in output
    assert "block | spread_too_wide | count=1" in output
    assert "block | entry_cooldown | count=1" in output
    assert "hold | no_ma_cross | count=1" in output


def test_show_trade_reasons_includes_broker_rejections_and_allowance_backoff(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event(
            "ERROR",
            "DE40",
            "Broker error",
            {
                "error": (
                    "IG API GET /markets/IX.D.DAX.DAILY.IP failed: 404 Not Found "
                    '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
                )
            },
        )
        store.record_event(
            "ERROR",
            "DE40",
            "Broker error",
            {
                "error": (
                    "IG deal rejected: MINIMUM_ORDER_SIZE_ERROR | epic=IX.D.DAX.IFS.IP "
                    "direction=BUY size=0.1"
                )
            },
        )
        store.record_event(
            "WARN",
            "US100",
            "Broker allowance backoff active",
            {"kind": "api_key_allowance_exceeded", "remaining_sec": 30.0},
        )
    finally:
        store.close()

    count = _show_trade_reasons(db_path, limit=20, window=200)
    output = capsys.readouterr().out

    assert count >= 3
    assert "reject | ig_api_get_markets:error_service_marketdata_instrument_epic_unavailable | count=1" in output
    assert "reject | deal_rejected:minimum_order_size_error | count=1" in output
    assert "block | allowance:api_key_allowance_exceeded | count=1" in output


def test_show_trade_reasons_returns_empty_when_no_matching_events(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Heartbeat", {"ok": True})
    finally:
        store.close()

    count = _show_trade_reasons(db_path, limit=10, window=100)
    output = capsys.readouterr().out

    assert count == 0
    assert "No trade reasons found." in output


def test_show_trade_reasons_can_filter_only_rejects(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Signal hold reason", {"reason": "no_signal"})
        store.record_event("WARN", "US100", "Trade blocked by spread filter", {"spread": 3.0})
        store.record_event(
            "ERROR",
            "DE40",
            "Broker error",
            {"error": "IG deal rejected: MINIMUM_ORDER_SIZE_ERROR | epic=IX.D.DAX.IFS.IP"},
        )
    finally:
        store.close()

    count = _show_trade_reasons(db_path, limit=10, window=100, kinds={"reject"})
    output = capsys.readouterr().out

    assert count == 1
    assert "kinds=reject" in output
    assert "reject | deal_rejected:minimum_order_size_error | count=1" in output
    assert "hold | no_signal" not in output
    assert "block | spread_too_wide" not in output


def test_show_ig_allowance_aggregates_errors_and_stream_metrics(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event(
            "ERROR",
            "EURUSD",
            "Broker error",
            {"error": 'IG API GET /accounts failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'},
        )
        store.record_event(
            "ERROR",
            "EURUSD",
            "Broker error",
            {"error": 'IG API GET /markets failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-allowance"}'},
        )
        store.record_event(
            "ERROR",
            "US100",
            "Broker error",
            {"error": "IG public API allowance cooldown is active (9.9s remaining)"},
        )
        store.record_event(
            "INFO",
            "EURUSD",
            "Stream usage metrics",
            {
                "price_requests_total": 10,
                "stream_hits_total": 8,
                "rest_fallback_hits_total": 2,
                "stream_hit_rate_pct": 80.0,
                "stream_reason": "tick_fresh",
            },
        )
        store.record_event(
            "INFO",
            "US100",
            "Stream usage metrics",
            {
                "price_requests_total": 5,
                "stream_hits_total": 1,
                "rest_fallback_hits_total": 4,
                "stream_hit_rate_pct": 20.0,
                "stream_reason": "rest_cooldown",
            },
        )
    finally:
        store.close()

    count = _show_ig_allowance(db_path, window=100, symbols_limit=10)
    output = capsys.readouterr().out

    assert count == 5
    assert "IG allowance diagnostics (last 100 events):" in output
    assert "allowance_exceeded_errors=2" in output
    assert "cooldown_active_errors=1" in output
    assert "allowance_exceeded_by_symbol:" in output
    assert "  EURUSD: 2" in output
    assert "cooldown_active_by_symbol:" in output
    assert "  US100: 1" in output
    assert "latest_stream_usage_metrics:" in output
    assert "US100 | requests=5 stream_hits=1 rest_fallback_hits=4" in output
    assert "EURUSD | requests=10 stream_hits=8 rest_fallback_hits=2" in output


def test_show_ig_allowance_returns_empty_when_no_matching_events(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Heartbeat", {"ok": True})
    finally:
        store.close()

    count = _show_ig_allowance(db_path, window=100, symbols_limit=5)
    output = capsys.readouterr().out

    assert count == 0
    assert "No IG allowance data found." in output


def test_reset_risk_anchor_uses_latest_equity(tmp_path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_account_snapshot(
            snapshot=AccountSnapshot(
                timestamp=1000.0,
                balance=8000.0,
                equity=7900.0,
                margin_free=7800.0,
            ),
            open_positions=0,
            daily_pnl=0.0,
            drawdown_pct=21.0,
        )
    finally:
        store.close()

    value = _reset_risk_anchor(db_path, None)
    assert value == 7900.0

    store2 = StateStore(db_path)
    try:
        assert store2.get_kv("risk.start_equity") == "7900.0"
    finally:
        store2.close()


def test_reset_risk_anchor_accepts_explicit_value(tmp_path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.close()

    value = _reset_risk_anchor(db_path, 8430.0)
    assert value == 8430.0

    store2 = StateStore(db_path)
    try:
        assert store2.get_kv("risk.start_equity") == "8430.0"
    finally:
        store2.close()


def test_cleanup_incompatible_open_trades_dry_run_does_not_mutate(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="paper-open-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.1,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=1000.0,
                status="open",
            ),
            thread_name="t1",
            strategy="momentum",
            mode="execution",
        )
        store.upsert_trade(
            Position(
                position_id="deal-exec-1",
                symbol="GBPUSD",
                side=Side.SELL,
                volume=0.1,
                open_price=1.3000,
                stop_loss=1.3050,
                take_profit=1.2900,
                opened_at=1001.0,
                status="open",
            ),
            thread_name="t2",
            strategy="g1",
            mode="execution",
        )
    finally:
        store.close()

    count = _cleanup_incompatible_open_trades(db_path, dry_run=True)
    output = capsys.readouterr().out

    assert count == 1
    assert "paper-open-1" in output
    assert "Dry-run mode: no changes were applied." in output

    store_check = StateStore(db_path)
    try:
        open_positions = store_check.load_open_positions(mode="execution")
        assert "paper-open-1" in open_positions
        assert open_positions["paper-open-1"].position_id == "paper-open-1"
        assert "deal-exec-1" in open_positions
    finally:
        store_check.close()


def test_cleanup_incompatible_open_trades_archives_and_logs_event(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="mock-open-1",
                symbol="US100",
                side=Side.BUY,
                volume=0.1,
                open_price=19000.0,
                stop_loss=18900.0,
                take_profit=19200.0,
                opened_at=1000.0,
                status="open",
            ),
            thread_name="t3",
            strategy="index_hybrid",
            mode="execution",
        )
    finally:
        store.close()

    count = _cleanup_incompatible_open_trades(db_path, dry_run=False)
    output = capsys.readouterr().out
    assert count == 1
    assert "Archived 1 incompatible trade(s)." in output

    con = sqlite3.connect(str(db_path))
    try:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT status, close_price, closed_at FROM trades WHERE position_id = ?",
            ("mock-open-1",),
        ).fetchone()
        assert row is not None
        assert str(row["status"]) == "closed"
        assert float(row["close_price"]) == 19000.0
        assert row["closed_at"] is not None

        event = con.execute(
            "SELECT message, payload_json FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert event is not None
        assert str(event["message"]) == "Incompatible open execution trades archived"
        assert "mock-open-1" in str(event["payload_json"] or "")
    finally:
        con.close()
