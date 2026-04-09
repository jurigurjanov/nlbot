from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from run_bot import (
    _cleanup_incompatible_open_trades,
    _reset_risk_anchor,
    _resolve_storage_path,
    _show_alerts,
    _show_db_first_health,
    _show_entry_attempts,
    _show_balance,
    _show_ig_allowance,
    _show_open_trades_snapshot,
    _show_status,
    _show_trade_reasons,
    _show_trades,
)
from xtb_bot.models import AccountSnapshot
from xtb_bot.models import Position, RunMode, Side, WorkerState
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


def test_show_status_marks_stale_snapshot(tmp_path, capsys):
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
            open_positions=1,
            daily_pnl=10.2,
            drawdown_pct=0.4,
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "latest_snapshot=" in output
    assert "state=stale" in output
    assert "age=" in output


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


def test_show_status_prints_stream_runtime_status(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("IG_STREAM_ENABLED", "true")
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event(
            "INFO",
            "US100",
            "Stream usage metrics",
            {
                "price_requests_total": 10,
                "stream_hits_total": 7,
                "rest_fallback_hits_total": 3,
                "stream_hit_rate_pct": 70.0,
                "stream_connected": True,
                "stream_reason": "stream_ok",
                "desired_subscriptions": 25,
                "active_subscriptions": 21,
            },
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "ig_stream=configured=enabled runtime=active" in output
    assert "desired=25 active=21" in output
    assert "ig_stream_usage=requests=10 stream_hits=7 rest_fallback_hits=3" in output
    assert "ig_stream_reasons=stream_ok:1" in output


def test_show_status_prints_multi_strategy_emergency_summary(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        now = time.time()
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="worker-WTI",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=80.0,
                last_heartbeat=now,
                iteration=12,
                position=None,
                last_error="multi_strategy_emergency_mode:connectivity=latency_too_high",
            )
        )
        store.save_worker_state(
            WorkerState(
                symbol="GOLD",
                thread_name="worker-GOLD",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=2300.0,
                last_heartbeat=now,
                iteration=7,
                position=None,
                last_error="multi_strategy_emergency_mode:connectivity=latency_too_high",
            )
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "multi_strategy_emergency=active" in output
    assert "workers=2" in output
    assert "symbols=GOLD,WTI" in output
    assert "reasons=connectivity=latency_too_high:2" in output


def test_show_status_prints_multi_strategy_emergency_inactive_when_absent(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.save_worker_state(
            WorkerState(
                symbol="EURUSD",
                thread_name="worker-EURUSD",
                mode=RunMode.PAPER,
                strategy="momentum",
                last_price=1.1,
                last_heartbeat=time.time(),
                iteration=1,
                position=None,
                last_error=None,
            )
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "multi_strategy_emergency=inactive" in output


def test_show_status_prints_recent_trade_flow_latest_closed_and_strategy_performance(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.upsert_trade(
            Position(
                position_id="p-closed-1",
                symbol="WTI",
                side=Side.BUY,
                volume=0.5,
                open_price=80.0,
                stop_loss=79.0,
                take_profit=82.0,
                opened_at=now - 900.0,
                status="closed",
                close_price=81.2,
                closed_at=now - 300.0,
                pnl=12.5,
            ),
            thread_name="t-wti",
            strategy="momentum",
            mode="execution",
            strategy_entry="g1",
            strategy_entry_component="g1",
            strategy_entry_signal="ema_cross_up",
        )
        store.update_trade_performance(
            position_id="p-closed-1",
            symbol="WTI",
            max_favorable_pips=8.0,
            max_adverse_pips=3.0,
            max_favorable_pnl=20.0,
            max_adverse_pnl=-5.0,
            opened_at=now - 900.0,
            closed_at=now - 300.0,
            close_reason="take_profit",
        )
        store.upsert_trade(
            Position(
                position_id="p-closed-2",
                symbol="US100",
                side=Side.SELL,
                volume=0.2,
                open_price=18000.0,
                stop_loss=18100.0,
                take_profit=17800.0,
                opened_at=now - 1800.0,
                status="closed",
                close_price=18060.0,
                closed_at=now - 120.0,
                pnl=-30.0,
            ),
            thread_name="t-us100",
            strategy="momentum",
            mode="execution",
            strategy_entry="index_hybrid",
            strategy_entry_component="index_hybrid",
            strategy_entry_signal="trend_breakout_up",
        )
        store.update_trade_performance(
            position_id="p-closed-2",
            symbol="US100",
            max_favorable_pips=10.0,
            max_adverse_pips=18.0,
            max_favorable_pnl=40.0,
            max_adverse_pnl=-35.0,
            opened_at=now - 1800.0,
            closed_at=now - 120.0,
            close_reason="strategy_exit:protective:fast_fail",
        )
        store.record_event("INFO", "WTI", "Position opened", {"position_id": "p-closed-1"})
        store.record_event("INFO", "WTI", "Position closed", {"position_id": "p-closed-1", "reason": "take_profit", "pnl": 12.5})
        store.record_event(
            "INFO",
            "DE40",
            "Signal hold reason",
            {
                "reason": "multi_all_hold",
                "metadata": {
                    "multi_hold_reason_by_strategy": "momentum=spread_too_wide; g1=adx_below_threshold",
                    "multi_blocked_reason_by_strategy": "g1=adx_below_threshold",
                    "multi_soft_reason_by_strategy": "momentum=higher_tf_bias_mismatch",
                },
            },
        )
        store.record_event(
            "INFO",
            "US100",
            "Signal hold reason",
            {
                "reason": "entry_spike_detected",
                "metadata": {
                    "indicator": "index_hybrid",
                    "directional_move_pips": 4.0,
                    "spike_guard_threshold_pips": 2.0,
                },
            },
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "recent_trade_flow=" in output
    assert "recent_trade_reasons:" in output
    assert "hold | multi_all_hold | count=1" in output
    assert "recent_multi_hold_components:" in output
    assert "g1=adx_below_threshold count=1" in output
    assert "recent_hard_block_components:" in output
    assert "recent_soft_filters:" in output
    assert "momentum=higher_tf_bias_mismatch count=1" in output
    assert "recent_entry_spike_blocks=1" in output
    assert "recent_entry_spike_symbols:" in output
    assert "US100 count=1" in output
    assert "recent_entry_spike_components:" in output
    assert "index_hybrid count=1" in output
    assert "latest_closed_performance=2 latest trades" in output
    assert "subtype=g1:ema_cross_up" in output
    assert "strategy_performance:" in output
    assert "strategy_subtype_performance:" in output
    assert "g1 | trades=1" in output
    assert "index_hybrid | trades=1" in output
    assert "g1:ema_cross_up | trades=1" in output
    assert "index_hybrid:trend_breakout_up | trades=1" in output


def test_show_status_falls_back_to_closed_trade_event_pnl(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.upsert_trade(
            Position(
                position_id="p-closed-backfill-status",
                symbol="GOLD",
                side=Side.SELL,
                volume=0.2,
                open_price=4552.34,
                stop_loss=4556.30,
                take_profit=4544.60,
                opened_at=now - 1200.0,
                status="closed",
                close_price=4554.26,
                closed_at=now - 900.0,
                pnl=0.0,
            ),
            thread_name="worker-GOLD",
            strategy="momentum",
            mode="execution",
            strategy_entry="g1",
            strategy_entry_component="g1",
        )
        store.update_trade_performance(
            position_id="p-closed-backfill-status",
            symbol="GOLD",
            max_favorable_pips=11.9,
            max_adverse_pips=14.2,
            max_favorable_pnl=23.8,
            max_adverse_pnl=-80.4,
            opened_at=now - 1200.0,
            closed_at=now - 900.0,
            close_reason="broker_manual_close:mark:ig_history_activity",
        )
        store.record_event(
            "INFO",
            "GOLD",
            "Closed trade details backfilled from broker sync",
            {
                "position_id": "p-closed-backfill-status",
                "source": "ig_history_activity",
                "broker_close_sync": {"realized_pnl": -80.40},
            },
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "latest_closed_performance=1 latest trades" in output
    assert "symbol=GOLD" in output
    assert "pnl=-80.40" in output


def test_show_status_falls_back_to_local_pnl_estimate_when_realized_missing(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.upsert_trade(
            Position(
                position_id="p-closed-estimate-status",
                symbol="DE40",
                side=Side.SELL,
                volume=0.5,
                open_price=23219.0,
                stop_loss=23254.0,
                take_profit=23140.0,
                opened_at=now - 1200.0,
                status="closed",
                close_price=23219.0,
                closed_at=now - 900.0,
                pnl=0.0,
            ),
            thread_name="worker-DE40",
            strategy="momentum",
            mode="execution",
            strategy_entry="g1",
            strategy_entry_component="g1",
        )
        store.update_trade_performance(
            position_id="p-closed-estimate-status",
            symbol="DE40",
            max_favorable_pips=12.0,
            max_adverse_pips=35.0,
            max_favorable_pnl=14.0,
            max_adverse_pnl=-17.7,
            opened_at=now - 1200.0,
            closed_at=now - 900.0,
            close_reason="broker_manual_close:stop_loss:ig_history_activity",
        )
        store.record_event(
            "INFO",
            "DE40",
            "Position closed",
            {
                "position_id": "p-closed-estimate-status",
                "pnl": 0.0,
                "local_pnl_estimate": -17.72,
            },
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "symbol=DE40" in output
    assert "pnl=-17.72" in output


def test_show_status_prints_request_limits_unavailable_symbols_and_current_trades(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.upsert_trade(
            Position(
                position_id="p-open-1",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.3,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=now - 600.0,
                entry_confidence=0.72,
                status="open",
                pnl=15.0,
            ),
            thread_name="t-eurusd",
            strategy="momentum",
            mode="execution",
        )
        store.save_worker_state(
            WorkerState(
                symbol="EURUSD",
                thread_name="t-eurusd",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=1.1030,
                last_heartbeat=now,
                iteration=42,
                position={"position_id": "p-open-1"},
                last_error=None,
            )
        )
        store.save_worker_state(
            WorkerState(
                symbol="FR40",
                thread_name="t-fr40",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=None,
                last_heartbeat=now,
                iteration=7,
                position=None,
                last_error="db_first_tick_cache_warming_up",
            )
        )
    finally:
        store.close()

    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO ig_rate_limit_workers(
                worker_key, limit_scope, limit_unit, limit_value, used_value, remaining_value,
                window_sec, blocked_total, last_request_ts, last_block_ts, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "account_non_trading",
                "per_account",
                "rpm",
                30,
                28,
                2,
                60.0,
                99,
                now - 5.0,
                now - 10.0,
                "test",
                now,
            ),
        )
        con.commit()
    finally:
        con.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "ig_request_limits=workers=1 hot=account_non_trading used=28/30 remaining=2" in output
    assert "symbols_unavailable=none" in output
    assert "symbols_degraded:" in output
    assert "FR40 | status=degraded" in output
    assert "current_trades=1" in output
    assert "EURUSD BUY | age=" in output


def test_show_status_hides_recovered_transient_degraded_symbols(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.save_worker_state(
            WorkerState(
                symbol="US30",
                thread_name="t-us30",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=42000.0,
                last_heartbeat=now - 400.0,
                iteration=1,
                position=None,
                last_error=None,
            )
        )
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="t-wti",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=80.0,
                last_heartbeat=now,
                iteration=2,
                position=None,
                last_error=None,
            )
        )
        store.upsert_broker_tick(
            symbol="WTI",
            bid=80.0,
            ask=80.1,
            ts=now,
            source="test",
        )
        store.record_event(
            "INFO",
            "WTI",
            "Signal hold reason",
            {"reason": "multi_degraded_hold"},
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "symbols_unavailable:" in output
    assert "US30 | status=unavailable reason=worker_stale" in output
    assert "symbols_degraded=none" in output
    assert "WTI | status=degraded reason=multi_degraded_hold" not in output


def test_show_status_keeps_transient_degraded_symbols_until_tick_or_heartbeat_recovers(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="t-wti",
                mode=RunMode.EXECUTION,
                strategy="momentum",
                last_price=80.0,
                last_heartbeat=now,
                iteration=2,
                position=None,
                last_error=None,
            )
        )
        store.record_event(
            "INFO",
            "WTI",
            "Signal hold reason",
            {"reason": "multi_degraded_hold"},
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "symbols_unavailable=none" in output
    assert "symbols_degraded:" in output
    assert "WTI | status=degraded reason=multi_degraded_hold" in output


def test_show_status_displays_strategy_base_for_multi_strategy_worker_states(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="worker-WTI",
                mode=RunMode.EXECUTION,
                strategy="multi_strategy",
                strategy_base="momentum",
                last_price=80.0,
                last_heartbeat=now,
                iteration=2,
                position=None,
                last_error=None,
            )
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "symbols_degraded:" in output
    assert "WTI | status=degraded reason=no_tick_cache worker=execution/multi_strategy strategy_base=momentum" in output


def test_show_status_displays_active_position_component_for_multi_strategy_worker_state(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    now = time.time()
    try:
        position = Position(
            position_id="paper-open",
            symbol="WTI",
            side=Side.BUY,
            volume=0.2,
            open_price=80.0,
            stop_loss=79.0,
            take_profit=82.0,
            opened_at=now - 60.0,
            status="open",
        )
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="worker-WTI",
                mode=RunMode.EXECUTION,
                strategy="multi_strategy",
                strategy_base="momentum",
                position_strategy_entry="momentum",
                position_strategy_component="g1",
                position_strategy_signal="ema_cross_up",
                last_price=80.0,
                last_heartbeat=now,
                iteration=2,
                position=position.to_dict(),
                last_error=None,
            )
        )
    finally:
        store.close()

    count = _show_status(db_path)
    output = capsys.readouterr().out

    assert count == 1
    assert "symbols_degraded:" in output
    assert (
        "WTI | status=degraded reason=no_tick_cache worker=execution/multi_strategy "
        "strategy_base=momentum position=BUY/g1"
    ) in output


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


def test_show_trades_open_includes_live_mark_and_pnl(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="p-open-live",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.5,
                open_price=1.10000,
                stop_loss=1.09500,
                take_profit=1.11000,
                opened_at=1000.0,
                status="open",
                pnl=0.0,
            ),
            thread_name="t-eurusd",
            strategy="momentum",
            mode="paper",
        )
        now = time.time()
        store.upsert_broker_tick(
            symbol="EURUSD",
            bid=1.10100,
            ask=1.10120,
            ts=now,
            source="test",
        )
    finally:
        store.close()

    count = _show_trades(db_path, limit=10, open_only=True)
    output = capsys.readouterr().out

    assert count == 1
    assert "OPEN EURUSD BUY" in output
    assert "live: mark=" in output
    assert "pnl_live=" in output
    assert "mark_source=tick_cache" in output


def test_show_trades_falls_back_to_closed_trade_event_pnl(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="p-closed-backfill",
                symbol="WTI",
                side=Side.BUY,
                volume=0.2,
                open_price=10192.6,
                stop_loss=10162.0,
                take_profit=10254.0,
                opened_at=2000.0,
                status="closed",
                close_price=10162.0,
                closed_at=2100.0,
                pnl=0.0,
            ),
            thread_name="worker-WTI",
            strategy="momentum",
            mode="execution",
        )
        store.record_event(
            "INFO",
            "WTI",
            "Closed trade details backfilled from broker sync",
            {
                "position_id": "p-closed-backfill",
                "source": "ig_history_activity",
                "broker_close_sync": {"realized_pnl": -70.38},
            },
        )
        for idx in range(2105):
            store.record_event(
                "INFO",
                "WTI",
                "Signal hold reason",
                {
                    "reason": "multi_net_flat",
                    "position_id": f"noise-{idx}",
                },
            )
    finally:
        store.close()

    count = _show_trades(db_path, limit=10, open_only=False)
    output = capsys.readouterr().out

    assert count == 1
    assert "CLOSED WTI BUY" in output
    assert "pnl=-70.38" in output


def test_show_trades_falls_back_to_local_pnl_estimate(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="p-closed-estimate",
                symbol="AUS200",
                side=Side.SELL,
                volume=1.0,
                open_price=8650.0,
                stop_loss=8640.0,
                take_profit=8700.0,
                opened_at=2000.0,
                status="closed",
                close_price=8640.6,
                closed_at=2100.0,
                pnl=0.0,
            ),
            thread_name="worker-AUS200",
            strategy="momentum",
            mode="execution",
        )
        store.record_event(
            "INFO",
            "AUS200",
            "Position closed",
            {
                "position_id": "p-closed-estimate",
                "pnl": 0.0,
                "local_pnl_estimate": -8.26,
            },
        )
    finally:
        store.close()

    count = _show_trades(db_path, limit=10, open_only=False)
    output = capsys.readouterr().out

    assert count == 1
    assert "CLOSED AUS200 SELL" in output
    assert "pnl=-8.26" in output


def test_show_open_trades_snapshot_prints_risk_and_ids(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_trade(
            Position(
                position_id="DIAAAA123",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.52,
                open_price=1.33800,
                stop_loss=1.33700,
                take_profit=1.34100,
                opened_at=1_700_000_000.0,
                entry_confidence=0.66,
                status="open",
                pnl=21.5,
            ),
            thread_name="w-eurusd",
            strategy="g1",
            mode="execution",
        )
        # Keep deal_reference populated to expose local/IG ids.
        con = sqlite3.connect(str(db_path))
        try:
            con.execute(
                "UPDATE trades SET deal_reference = ? WHERE position_id = ?",
                ("IGBOTREF001", "DIAAAA123"),
            )
            con.commit()
        finally:
            con.close()
        store.save_worker_state(
            WorkerState(
                symbol="EURUSD",
                thread_name="w-eurusd",
                mode=RunMode.EXECUTION,
                strategy="g1",
                last_price=1.33855,
                last_heartbeat=time.time(),
                iteration=123,
                position={"position_id": "DIAAAA123"},
                last_error=None,
            )
        )
    finally:
        store.close()

    count = _show_open_trades_snapshot(db_path, strategy=None)
    output = capsys.readouterr().out

    assert count == 1
    assert "Open trades live snapshot" in output
    assert "id_local=DIAAAA123 id_ig=IGBOTREF001" in output
    assert "risk_estimate=" in output
    assert "risk_to_sl=" in output
    assert "progress_to_tp=" in output
    assert "worker_age=" in output


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
        store.record_event(
            "INFO",
            "US100",
            "Signal hold reason",
            {
                "reason": "entry_spike_detected",
                "metadata": {"indicator": "index_hybrid"},
            },
        )
    finally:
        store.close()

    count = _show_trade_reasons(db_path, limit=10, window=100)
    output = capsys.readouterr().out

    assert count >= 6
    assert "Trade reason summary" in output
    assert "close | stop_loss | count=2" in output
    assert "pnl_sum=-30.00" in output
    assert "close | take_profit | count=1" in output
    assert "block | spread_too_wide | count=1" in output
    assert "block | entry_cooldown | count=1" in output
    assert "hold | no_ma_cross | count=1" in output
    assert "hold | entry_spike_detected | count=1" in output
    assert "entry_spike_detected summary: count=1" in output
    assert "entry_spike_detected by symbol:" in output
    assert "US100 count=1" in output
    assert "entry_spike_detected by component:" in output
    assert "index_hybrid count=1" in output


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


def test_show_entry_attempts_prints_accepted_and_rejected(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event(
            "INFO",
            "EURUSD",
            "Position opened",
            {
                "position_id": "pos-1",
                "side": "buy",
                "volume": 0.2,
                "entry": 1.1012,
                "stop_loss": 1.0992,
                "take_profit": 1.1062,
                "confidence": 0.73,
                "strategy_entry": "momentum",
                "mode": "execution",
            },
        )
        store.record_event(
            "WARN",
            "EURUSD",
            "Trade blocked by confidence threshold",
            {
                "signal": "buy",
                "confidence": 0.51,
                "min_confidence_for_entry": 0.68,
                "strategy": "momentum",
            },
        )
        store.record_event(
            "ERROR",
            "DE40",
            "Broker error",
            {"error": "IG deal rejected: MINIMUM_ORDER_SIZE_ERROR | epic=IX.D.DAX.IFS.IP"},
        )
        store.record_event(
            "ERROR",
            "US100",
            "Broker error",
            {"error": 'IG API GET /accounts failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'},
        )
    finally:
        store.close()

    count = _show_entry_attempts(db_path, limit=10, window=100)
    output = capsys.readouterr().out

    assert count == 3
    assert "Entry attempts (last 100 events, newest first):" in output
    assert "accepted | Position opened | reason=position_opened" in output
    assert "rejected | Trade blocked by confidence threshold | reason=confidence_below_threshold" in output
    assert "rejected | Broker error | reason=deal_rejected:minimum_order_size_error" in output
    assert "summary: accepted=1 rejected=2" in output
    assert "ig_api_get_accounts:error_public_api_exceeded_api_key_allowance" not in output


def test_show_entry_attempts_returns_empty_when_no_attempts(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "Heartbeat", {"ok": True})
    finally:
        store.close()

    count = _show_entry_attempts(db_path, limit=10, window=100)
    output = capsys.readouterr().out

    assert count == 0
    assert "No entry attempts found." in output


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


def test_show_ig_allowance_prints_worker_bottleneck_diagnostics(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.upsert_ig_rate_limit_worker(
            worker_key="app_non_trading",
            limit_scope="ig_app_non_trading",
            limit_unit="requests_per_minute",
            limit_value=60,
            used_value=58,
            remaining_value=2,
            window_sec=60.0,
            blocked_total=12,
            last_request_ts=time.time(),
            last_block_ts=time.time(),
            notes=None,
        )
        store.upsert_ig_rate_limit_worker(
            worker_key="account_non_trading",
            limit_scope="ig_account_non_trading",
            limit_unit="requests_per_minute",
            limit_value=30,
            used_value=8,
            remaining_value=22,
            window_sec=60.0,
            blocked_total=0,
            last_request_ts=time.time(),
            last_block_ts=None,
            notes=None,
        )
    finally:
        store.close()

    count = _show_ig_allowance(db_path, window=100, symbols_limit=5)
    output = capsys.readouterr().out

    assert count == 2
    assert "rate_limit_worker_pressure:" in output
    assert "app_non_trading | used=58/60" in output
    assert "account_non_trading | used=8/30" in output
    assert "detected_bottleneck_worker=app_non_trading" in output


def test_show_db_first_health_reports_stale_and_no_tick(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        now = time.time()
        store._conn.execute(  # noqa: SLF001
            """
            INSERT INTO broker_ticks(symbol, bid, ask, mid, ts, volume, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("BTC", 100.0, 101.0, 100.5, now - 200.0, 1.0, "db_first_app_non_trading_cache", now),
        )
        store.save_worker_state(
            WorkerState(
                symbol="BTC",
                thread_name="worker-BTC",
                mode=RunMode.EXECUTION,
                strategy="crypto_trend_following",
                last_price=100.5,
                last_heartbeat=now - 1.0,
                iteration=1,
                last_error=None,
            )
        )
        store.record_event(
            "WARN",
            "BTC",
            "DB-first tick cache warming up",
            {"max_age_sec": 85.0, "streak": 12, "reason": "allowance_cooldown_active"},
        )
        store.save_worker_state(
            WorkerState(
                symbol="DOGE",
                thread_name="worker-DOGE",
                mode=RunMode.EXECUTION,
                strategy="crypto_trend_following",
                last_price=None,
                last_heartbeat=now - 2.0,
                iteration=1,
                last_error="DB-first tick cache is empty or stale for DOGE",
            )
        )
    finally:
        store.close()

    count = _show_db_first_health(db_path, window=100, symbols_limit=10)
    output = capsys.readouterr().out

    assert count == 2
    assert "DB-first health diagnostics" in output
    assert "summary: no_tick=1 stale=1" in output
    assert "BTC | status=STALE" in output
    assert "DOGE | status=NO_TICK" in output


def test_show_db_first_health_displays_multi_strategy_base_and_position(tmp_path, capsys):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        now = time.time()
        position = Position(
            position_id="paper-open-db-first",
            symbol="WTI",
            side=Side.BUY,
            volume=0.2,
            open_price=80.0,
            stop_loss=79.0,
            take_profit=82.0,
            opened_at=now - 60.0,
            status="open",
        )
        store.save_worker_state(
            WorkerState(
                symbol="WTI",
                thread_name="worker-WTI",
                mode=RunMode.EXECUTION,
                strategy="multi_strategy",
                strategy_base="momentum",
                position_strategy_entry="momentum",
                position_strategy_component="g1",
                position_strategy_signal="ema_cross_up",
                last_price=80.0,
                last_heartbeat=now - 2.0,
                iteration=1,
                position=position.to_dict(),
                last_error="DB-first tick cache is empty or stale for WTI",
            )
        )
    finally:
        store.close()

    count = _show_db_first_health(db_path, window=100, symbols_limit=10)
    output = capsys.readouterr().out

    assert count == 1
    assert "WTI | status=NO_TICK" in output
    assert "worker=execution/multi_strategy strategy_base=momentum position=BUY/g1" in output


def test_reset_risk_anchor_uses_latest_equity(tmp_path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        now = time.time()
        store.record_account_snapshot(
            snapshot=AccountSnapshot(
                timestamp=now,
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


def test_reset_risk_anchor_falls_back_to_broker_when_snapshots_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.close()

    monkeypatch.setattr("run_bot._fetch_current_account_anchor_from_broker", lambda: 9123.45)
    value = _reset_risk_anchor(db_path, None)
    assert value == 9123.45

    store2 = StateStore(db_path)
    try:
        assert store2.get_kv("risk.start_equity") == "9123.45"
    finally:
        store2.close()


def test_reset_risk_anchor_uses_broker_when_latest_snapshot_is_stale(tmp_path, monkeypatch):
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
            open_positions=1,
            daily_pnl=0.0,
            drawdown_pct=21.0,
        )
    finally:
        store.close()

    monkeypatch.setattr("run_bot._fetch_current_account_anchor_from_broker", lambda: 9123.45)
    value = _reset_risk_anchor(db_path, None)
    assert value == 9123.45


def test_reset_risk_anchor_raises_when_latest_snapshot_is_stale_and_broker_fetch_fails(tmp_path, monkeypatch):
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
            open_positions=1,
            daily_pnl=0.0,
            drawdown_pct=21.0,
        )
    finally:
        store.close()

    monkeypatch.setattr(
        "run_bot._fetch_current_account_anchor_from_broker",
        lambda: (_ for _ in ()).throw(ValueError("broker unavailable")),
    )
    with pytest.raises(ValueError, match="Latest account snapshot is stale"):
        _reset_risk_anchor(db_path, None)


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
