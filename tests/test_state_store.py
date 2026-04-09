from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from xtb_bot.models import AccountSnapshot, PendingOpen, Position, Side, SymbolSpec
from xtb_bot.state_store import StateStore


def test_state_store_initializes_pragmas_and_indexes(tmp_path):
    store = StateStore(tmp_path / "state.db", sqlite_timeout_sec=42.0)
    try:
        with store._lock:
            journal_mode_row = store._conn.execute("PRAGMA journal_mode").fetchone()
            busy_timeout_row = store._conn.execute("PRAGMA busy_timeout").fetchone()
            user_version_row = store._conn.execute("PRAGMA user_version").fetchone()
            schema_versions = [
                int(row["version"])
                for row in store._conn.execute(
                    "SELECT version FROM schema_migrations ORDER BY version ASC"
                ).fetchall()
            ]
            trades_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(trades)").fetchall()
            }
            pending_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(pending_opens)").fetchall()
            }
            events_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(events)").fetchall()
            }
            ig_rate_limit_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(ig_rate_limit_workers)").fetchall()
            }
            position_updates_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(position_updates)").fetchall()
            }
            trade_performance_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(trade_performance)").fetchall()
            }
            virtual_positions_current_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(virtual_positions_current)").fetchall()
            }
            virtual_positions_ledger_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(virtual_positions_ledger)").fetchall()
            }
            real_positions_cache_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(real_positions_cache)").fetchall()
            }
            system_errors_indexes = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA index_list(system_errors)").fetchall()
            }

        assert journal_mode_row is not None
        assert str(journal_mode_row[0]).lower() == "wal"
        assert busy_timeout_row is not None
        assert int(busy_timeout_row[0]) >= 42_000
        assert user_version_row is not None
        assert int(user_version_row[0]) == 5
        assert schema_versions == [1, 2, 3, 4, 5]
        assert "idx_trades_status_mode_opened_at" in trades_indexes
        assert "idx_pending_opens_mode_created_at" in pending_indexes
        assert "idx_events_ts" in events_indexes
        assert "idx_events_level_ts" in events_indexes
        assert "idx_ig_rate_limit_workers_updated_at" in ig_rate_limit_indexes
        assert "idx_position_updates_ts" in position_updates_indexes
        assert "idx_position_updates_position_ts" in position_updates_indexes
        assert "idx_position_updates_symbol_ts" in position_updates_indexes
        assert "idx_position_updates_deal_reference_ts" in position_updates_indexes
        assert "idx_trade_performance_symbol_closed" in trade_performance_indexes
        assert "idx_trade_performance_updated_at" in trade_performance_indexes
        assert "idx_virtual_positions_current_symbol" in virtual_positions_current_indexes
        assert "idx_virtual_positions_ledger_symbol_ts" in virtual_positions_ledger_indexes
        assert "idx_virtual_positions_ledger_strategy_ts" in virtual_positions_ledger_indexes
        assert "idx_real_positions_cache_updated_at" in real_positions_cache_indexes
        assert "idx_system_errors_updated_at" in system_errors_indexes
    finally:
        store.close()


def test_append_price_sample_uses_batched_cleanup(tmp_path):
    store = StateStore(
        tmp_path / "state.db",
        price_history_cleanup_every=50,
        price_history_cleanup_min_interval_sec=0.0,
    )
    try:
        for idx in range(1, 206):
            store.append_price_sample(
                symbol="EURUSD",
                ts=float(1_700_000_000 + idx),
                close=1.1000 + idx * 0.00001,
                volume=10.0 + idx,
                max_rows_per_symbol=100,
            )

        rows = store.load_recent_price_history("EURUSD", limit=1_000)

        # Cleanup runs every 50 writes, so after 205 writes with keep=100:
        # after write 200 -> trimmed to 100 rows, plus 5 new rows => 105.
        assert len(rows) == 105
        assert float(rows[0]["ts"]) == 1_700_000_101.0
        assert float(rows[-1]["ts"]) == 1_700_000_205.0
    finally:
        store.close()


def test_pending_open_roundtrip_persists_entry_metadata(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_pending_open(
            PendingOpen(
                pending_id="XTBBOTTEST01PENDINGMETA1",
                position_id="PENDINGPOS1",
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
                entry_confidence=0.71,
                trailing_override={"trailing_distance_pips": 12.0},
            )
        )

        pending = store.load_pending_opens(mode="execution")

        assert len(pending) == 1
        assert pending[0].strategy == "multi_strategy"
        assert pending[0].strategy_entry == "momentum"
        assert pending[0].strategy_entry_component == "g1"
        assert pending[0].strategy_entry_signal == "ema_cross_up"
    finally:
        store.close()


def test_append_price_sample_prunes_future_timestamps_for_symbol(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        with patch("xtb_bot.state_store.time.time", return_value=1_800_000_000.0):
            store.append_price_sample(
                symbol="EURUSD",
                ts=1_900_000_000.0,
                close=1.2000,
                volume=10.0,
                max_rows_per_symbol=100,
            )
            store.append_price_sample(
                symbol="EURUSD",
                ts=1_800_000_010.0,
                close=1.2010,
                volume=11.0,
                max_rows_per_symbol=100,
            )

        rows = store.load_recent_price_history("EURUSD", limit=100)
        assert len(rows) == 1
        assert float(rows[0]["ts"]) == pytest.approx(1_800_000_010.0)
        assert float(rows[0]["close"]) == pytest.approx(1.2010)
    finally:
        store.close()


def test_price_and_candle_history_normalize_symbol_case(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.append_price_sample(
            symbol="eurusd",
            ts=1_800_000_010.0,
            close=1.2010,
            volume=11.0,
            max_rows_per_symbol=100,
            candle_resolutions_sec=(300,),
        )
        store.append_candle_sample(
            "eurusd",
            resolution_sec=900,
            ts=1_800_000_900.0,
            close=1.2250,
            open_price=1.2200,
            high_price=1.2300,
            low_price=1.2180,
            volume=77.0,
            max_rows_per_symbol=100,
        )

        rows_upper = store.load_recent_price_history("EURUSD", limit=10)
        rows_lower = store.load_recent_price_history("eurusd", limit=10)
        candles_upper = store.load_recent_candle_history("EURUSD", resolution_sec=900, limit=10)
        candles_lower = store.load_recent_candle_history("eurusd", resolution_sec=900, limit=10)

        assert len(rows_upper) == 1
        assert len(rows_lower) == 1
        assert rows_upper[0]["symbol"] == "EURUSD"
        assert rows_lower[0]["symbol"] == "EURUSD"
        assert len(candles_upper) == 1
        assert len(candles_lower) == 1
        assert candles_upper[0]["symbol"] == "EURUSD"
        assert candles_lower[0]["symbol"] == "EURUSD"
    finally:
        store.close()


def test_candle_history_roundtrip_uses_separate_storage(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        base_ts = 1_700_000_100.0
        for idx in range(6):
            store.append_price_sample(
                symbol="EURUSD",
                ts=base_ts + idx * 60.0,
                close=1.1000 + idx * 0.0001,
                volume=10.0 + idx,
                max_rows_per_symbol=500,
                candle_resolutions_sec=(300,),
            )

        candle_rows = store.load_recent_candle_history("EURUSD", resolution_sec=300, limit=10)
        raw_rows = store.load_recent_price_history("EURUSD", limit=10)

        assert len(raw_rows) == 6
        assert len(candle_rows) == 2
        assert float(candle_rows[0]["open"]) == pytest.approx(1.1000)
        assert float(candle_rows[0]["high"]) == pytest.approx(1.1004)
        assert float(candle_rows[0]["low"]) == pytest.approx(1.1000)
        assert float(candle_rows[0]["close"]) == pytest.approx(1.1004)
        assert float(candle_rows[1]["open"]) == pytest.approx(1.1005)
        assert float(candle_rows[1]["high"]) == pytest.approx(1.1005)
        assert float(candle_rows[1]["low"]) == pytest.approx(1.1005)
        assert float(candle_rows[1]["close"]) == pytest.approx(1.1005)
        assert float(candle_rows[0]["ts"]) == pytest.approx(base_ts + 299.999, abs=1e-3)

        store.append_candle_sample(
            "EURUSD",
            resolution_sec=900,
            ts=base_ts + 900.0,
            close=1.1250,
            open_price=1.1200,
            high_price=1.1300,
            low_price=1.1180,
            volume=77.0,
            max_rows_per_symbol=100,
        )
        m15_rows = store.load_recent_candle_history("EURUSD", resolution_sec=900, limit=10)
        assert len(m15_rows) == 1
        assert float(m15_rows[0]["open"]) == pytest.approx(1.1200)
        assert float(m15_rows[0]["high"]) == pytest.approx(1.1300)
        assert float(m15_rows[0]["low"]) == pytest.approx(1.1180)
        assert float(m15_rows[0]["close"]) == pytest.approx(1.1250)
        assert float(m15_rows[0]["volume"]) == pytest.approx(77.0)
    finally:
        store.close()


def test_candle_history_roundtrip_supports_minute_resolution(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        base_ts = 1_700_000_100.0
        for idx in range(4):
            store.append_candle_sample(
                "EURUSD",
                resolution_sec=60,
                ts=base_ts + idx * 60.0,
                close=1.2000 + idx * 0.0001,
                open_price=1.1995 + idx * 0.0001,
                high_price=1.2005 + idx * 0.0001,
                low_price=1.1990 + idx * 0.0001,
                volume=10.0 + idx,
                max_rows_per_symbol=100,
            )

        minute_rows = store.load_recent_candle_history("EURUSD", resolution_sec=60, limit=10)

        assert len(minute_rows) == 4
        assert float(minute_rows[0]["open"]) == pytest.approx(1.1995)
        assert float(minute_rows[0]["high"]) == pytest.approx(1.2005)
        assert float(minute_rows[0]["low"]) == pytest.approx(1.1990)
        assert float(minute_rows[0]["close"]) == pytest.approx(1.2000)
        assert float(minute_rows[0]["ts"]) == pytest.approx(base_ts + 59.999, abs=1e-3)
    finally:
        store.close()


def test_run_housekeeping_prunes_old_events_and_reports_checkpoint(tmp_path):
    store = StateStore(
        tmp_path / "state.db",
        housekeeping_interval_sec=0.0,
        housekeeping_events_keep_rows=120,
        housekeeping_incremental_vacuum_pages=64,
    )
    try:
        for idx in range(300):
            store.record_event("INFO", "EURUSD", f"evt-{idx}", {"i": idx})

        summary = store.run_housekeeping(force=True)
        assert summary["ran"] is True
        assert int(summary["events_kept"]) == 120
        assert int(summary["events_deleted"]) >= 180
        stored_summary_raw = store.get_kv("db.housekeeping.last_summary")
        assert stored_summary_raw not in (None, "")
        checkpoint = summary.get("checkpoint")
        if checkpoint is not None:
            assert isinstance(checkpoint, dict)
            assert {"busy", "wal_frames", "checkpointed_frames"} <= set(checkpoint.keys())

        events = store.load_events(limit=500)
        assert len(events) == 120
    finally:
        store.close()


def test_run_housekeeping_respects_interval_without_force(tmp_path):
    store = StateStore(
        tmp_path / "state.db",
        housekeeping_interval_sec=300.0,
        housekeeping_events_keep_rows=200,
    )
    try:
        for idx in range(20):
            store.record_event("INFO", "EURUSD", f"evt-{idx}", {"i": idx})

        with patch(
            "xtb_bot.state_store.time.time",
            side_effect=[1000.0, 1000.0, 1000.0, 1000.0, 1001.0],
        ):
            first = store.run_housekeeping(force=False)
            second = store.run_housekeeping(force=False)

        assert first["ran"] is True
        assert second["ran"] is False
        assert int(second["events_deleted"]) == 0
    finally:
        store.close()


def test_load_events_since_filters_by_timestamp_and_limit(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        with patch(
            "xtb_bot.state_store.time.time",
            side_effect=[1000.0, 1010.0, 1020.0],
        ):
            store.record_event("INFO", "EURUSD", "evt-1", {"n": 1})
            store.record_event("WARN", "EURUSD", "evt-2", {"n": 2})
            store.record_event("ERROR", "EURUSD", "evt-3", {"n": 3})

        events_all = store.load_events_since(1005.0, limit=10)
        assert [str(item["message"]) for item in events_all] == ["evt-3", "evt-2"]
        assert int((events_all[0].get("payload") or {}).get("n") or 0) == 3

        events_limited = store.load_events_since(0.0, limit=2)
        assert len(events_limited) == 2
        assert [str(item["message"]) for item in events_limited] == ["evt-3", "evt-2"]
    finally:
        store.close()


def test_has_trade_opened_between_filters_symbol_strategy_mode_and_window(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        eurusd_open = Position(
            position_id="pos-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=1_700_000_100.0,
            status="closed",
            close_price=1.1020,
            closed_at=1_700_000_200.0,
            pnl=20.0,
        )
        us100_open = Position(
            position_id="pos-2",
            symbol="US100",
            side=Side.SELL,
            volume=1.0,
            open_price=10000.0,
            stop_loss=10050.0,
            take_profit=9950.0,
            opened_at=1_700_000_150.0,
            status="closed",
            close_price=9980.0,
            closed_at=1_700_000_300.0,
            pnl=20.0,
        )
        store.upsert_trade(eurusd_open, "worker-EURUSD", "momentum", "paper")
        store.upsert_trade(us100_open, "worker-US100", "momentum", "execution")

        assert store.has_trade_opened_between(
            symbol="EURUSD",
            strategy="momentum",
            mode="paper",
            start_ts=1_700_000_000.0,
            end_ts=1_700_000_180.0,
        )
        assert not store.has_trade_opened_between(
            symbol="EURUSD",
            strategy="momentum",
            mode="execution",
            start_ts=1_700_000_000.0,
            end_ts=1_700_000_180.0,
        )
        assert not store.has_trade_opened_between(
            symbol="EURUSD",
            strategy="g1",
            mode="paper",
            start_ts=1_700_000_000.0,
            end_ts=1_700_000_180.0,
        )
        assert not store.has_trade_opened_between(
            symbol="EURUSD",
            strategy="momentum",
            mode="paper",
            start_ts=1_700_000_180.0,
            end_ts=1_700_000_400.0,
        )
    finally:
        store.close()


def test_state_store_round_trips_trade_epic_metadata(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="deal-1",
            symbol="DE40",
            side=Side.BUY,
            volume=1.0,
            open_price=22500.0,
            stop_loss=22450.0,
            take_profit=22650.0,
            opened_at=1_700_000_000.0,
            status="open",
            epic="IX.D.DAX.DAILY.IP",
            epic_variant="daily",
        )
        store.upsert_trade(position, "thread-1", "momentum", "execution")

        loaded = store.load_open_positions(mode="execution")

        assert "deal-1" in loaded
        assert loaded["deal-1"].epic == "IX.D.DAX.DAILY.IP"
        assert loaded["deal-1"].epic_variant == "daily"
    finally:
        store.close()


def test_load_open_positions_includes_closing_positions(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="deal-closing-1",
            symbol="US100",
            side=Side.BUY,
            volume=1.0,
            open_price=20_000.0,
            stop_loss=19_900.0,
            take_profit=20_150.0,
            opened_at=1_700_000_000.0,
            status="closing",
        )
        store.upsert_trade(position, "thread-1", "momentum", "execution")

        loaded = store.load_open_positions(mode="execution")

        assert "deal-closing-1" in loaded
        assert loaded["deal-closing-1"].status == "closing"
    finally:
        store.close()


def test_has_trade_opened_between_matches_entry_component_for_multi_strategy(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="ms-open-1",
            symbol="WTI",
            side=Side.BUY,
            volume=0.2,
            open_price=80.0,
            stop_loss=79.0,
            take_profit=82.0,
            opened_at=1_700_100_000.0,
            status="open",
        )
        store.upsert_trade(
            position,
            "worker-WTI",
            "multi_strategy",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="g1",
        )

        assert store.has_trade_opened_between(
            symbol="WTI",
            strategy="g1",
            mode="execution",
            start_ts=1_700_099_900.0,
            end_ts=1_700_100_100.0,
        )
        assert not store.has_trade_opened_between(
            symbol="WTI",
            strategy="multi_strategy",
            mode="execution",
            start_ts=1_700_099_900.0,
            end_ts=1_700_100_100.0,
        )
    finally:
        store.close()


def test_upsert_and_load_ig_rate_limit_workers(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_ig_rate_limit_worker(
            worker_key="app_non_trading",
            limit_scope="per_app",
            limit_unit="requests_per_minute",
            limit_value=60,
            used_value=12,
            remaining_value=48,
            window_sec=60.0,
            blocked_total=3,
            last_request_ts=1_700_000_010.0,
            last_block_ts=1_700_000_008.0,
            notes="IG per-app non-trading requests/minute",
        )
        store.upsert_ig_rate_limit_worker(
            worker_key="historical_points",
            limit_scope="per_app",
            limit_unit="points_per_week",
            limit_value=10_000,
            used_value=250,
            remaining_value=9_750,
            window_sec=604_800.0,
            blocked_total=1,
            last_request_ts=1_700_000_020.0,
            last_block_ts=1_700_000_018.0,
            notes="IG historical price points/week",
        )

        rows = store.load_ig_rate_limit_workers()
        assert len(rows) == 2

        by_key = {str(item["worker_key"]): item for item in rows}
        app = by_key["app_non_trading"]
        hist = by_key["historical_points"]

        assert int(app["limit_value"]) == 60
        assert int(app["used_value"]) == 12
        assert int(app["remaining_value"]) == 48
        assert int(app["blocked_total"]) == 3
        assert str(app["limit_unit"]) == "requests_per_minute"

        assert int(hist["limit_value"]) == 10_000
        assert int(hist["used_value"]) == 250
        assert int(hist["remaining_value"]) == 9_750
        assert int(hist["blocked_total"]) == 1
        assert str(hist["limit_unit"]) == "points_per_week"
    finally:
        store.close()


def test_state_store_broker_cache_tick_spec_and_account_snapshot(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        now_ts = 1_800_000_000.0
        store.upsert_broker_tick(
            symbol="EURUSD",
            bid=1.1000,
            ask=1.1002,
            ts=now_ts,
            volume=123.0,
            source="test",
        )
        with patch("xtb_bot.state_store.time.time", return_value=now_ts + 1.0):
            tick = store.load_latest_broker_tick("EURUSD", max_age_sec=30.0)
        assert tick is not None
        assert tick.bid == pytest.approx(1.1000)
        assert tick.ask == pytest.approx(1.1002)
        assert tick.timestamp == pytest.approx(now_ts)

        spec = SymbolSpec(
            symbol="EURUSD",
            tick_size=0.0001,
            tick_value=1.0,
            contract_size=100000.0,
            lot_min=0.01,
            lot_max=100.0,
            lot_step=0.01,
            min_stop_distance_price=0.0003,
            one_pip_means=0.0001,
            metadata={"source": "test-spec"},
        )
        store.upsert_broker_symbol_spec(symbol="EURUSD", spec=spec, ts=now_ts + 5.0, source="test")
        with patch("xtb_bot.state_store.time.time", return_value=now_ts + 10.0):
            loaded_spec = store.load_broker_symbol_spec("EURUSD", max_age_sec=30.0)
        assert loaded_spec is not None
        assert loaded_spec.tick_size == pytest.approx(0.0001)
        assert loaded_spec.lot_step == pytest.approx(0.01)
        assert str(loaded_spec.metadata.get("source")) == "test-spec"
        with patch("xtb_bot.state_store.time.time", return_value=now_ts + 10.0):
            loaded_pip_size = store.load_broker_symbol_pip_size("EURUSD", max_age_sec=30.0)
        assert loaded_pip_size == pytest.approx(0.0001)

        snapshot = AccountSnapshot(
            balance=10_000.0,
            equity=10_050.0,
            margin_free=8_000.0,
            timestamp=now_ts + 10.0,
        )
        store.upsert_broker_account_snapshot(snapshot, source="test")
        with patch("xtb_bot.state_store.time.time", return_value=now_ts + 12.0):
            loaded_snapshot = store.load_latest_broker_account_snapshot(max_age_sec=30.0)
        assert loaded_snapshot is not None
        assert loaded_snapshot.balance == pytest.approx(10_000.0)
        assert loaded_snapshot.equity == pytest.approx(10_050.0)
        assert loaded_snapshot.margin_free == pytest.approx(8_000.0)
    finally:
        store.close()


def test_state_store_keeps_multiple_symbol_specs_by_epic_variant(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_broker_symbol_spec(
            symbol="UK100",
            spec=SymbolSpec(
                symbol="UK100",
                tick_size=1.0,
                tick_value=1.0,
                contract_size=1.0,
                lot_min=1.0,
                lot_max=100.0,
                lot_step=1.0,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.CASH.IP", "epic_variant": "cash"},
            ),
            ts=1_800_000_000.0,
            source="cash_seed",
        )
        store.upsert_broker_symbol_spec(
            symbol="UK100",
            spec=SymbolSpec(
                symbol="UK100",
                tick_size=1.0,
                tick_value=10.0,
                contract_size=10.0,
                lot_min=0.5,
                lot_max=100.0,
                lot_step=0.5,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.DAILY.IP", "epic_variant": "daily"},
            ),
            ts=1_800_000_100.0,
            source="daily_seed",
        )

        cash = store.load_broker_symbol_spec(
            "UK100",
            max_age_sec=0.0,
            epic="IX.D.FTSE.CASH.IP",
        )
        daily = store.load_broker_symbol_spec(
            "UK100",
            max_age_sec=0.0,
            epic="IX.D.FTSE.DAILY.IP",
        )
        latest = store.load_broker_symbol_spec("UK100", max_age_sec=0.0)

        assert cash is not None
        assert daily is not None
        assert latest is not None
        assert cash.metadata.get("epic") == "IX.D.FTSE.CASH.IP"
        assert cash.lot_min == pytest.approx(1.0)
        assert daily.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        assert daily.lot_min == pytest.approx(0.5)
        assert latest.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
    finally:
        store.close()


def test_state_store_migrates_legacy_broker_symbol_spec_table_to_variant_schema(tmp_path):
    db_path = tmp_path / "legacy_state.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE broker_symbol_specs (
                symbol TEXT PRIMARY KEY,
                tick_size REAL NOT NULL,
                tick_value REAL NOT NULL,
                contract_size REAL NOT NULL,
                lot_min REAL NOT NULL,
                lot_max REAL NOT NULL,
                lot_step REAL NOT NULL,
                min_stop_distance_price REAL,
                one_pip_means REAL,
                metadata_json TEXT,
                source TEXT,
                ts REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO broker_symbol_specs(
                symbol, tick_size, tick_value, contract_size, lot_min, lot_max, lot_step,
                min_stop_distance_price, one_pip_means, metadata_json, source, ts, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "BRENT",
                1.0,
                10.0,
                10.0,
                1.0,
                100.0,
                1.0,
                None,
                1.0,
                '{"broker":"ig","epic":"CC.D.LCO.CFD.IP","epic_variant":"cfd"}',
                "legacy_seed",
                1_800_000_000.0,
                1_800_000_000.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    store = StateStore(db_path)
    try:
        with store._lock:
            columns = {
                str(row["name"])
                for row in store._conn.execute("PRAGMA table_info(broker_symbol_specs)").fetchall()
            }
        loaded = store.load_broker_symbol_spec(
            "BRENT",
            max_age_sec=0.0,
            epic="CC.D.LCO.CFD.IP",
        )

        assert "cache_key" in columns
        assert "epic" in columns
        assert "epic_variant" in columns
        assert loaded is not None
        assert loaded.metadata.get("epic") == "CC.D.LCO.CFD.IP"
        assert loaded.metadata.get("epic_variant") == "cfd"
        assert loaded.lot_min == pytest.approx(1.0)
    finally:
        store.close()


def test_state_store_broker_tick_cache_ignores_older_timestamp_updates(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_broker_tick(
            symbol="DE40",
            bid=18_500.0,
            ask=18_501.0,
            ts=1_800_000_100.0,
            volume=8.0,
            source="newer_tick",
        )
        store.upsert_broker_tick(
            symbol="DE40",
            bid=18_000.0,
            ask=18_001.0,
            ts=1_800_000_010.0,
            volume=1.0,
            source="older_tick",
        )

        tick = store.load_latest_broker_tick("DE40", max_age_sec=10_000_000_000.0)
        assert tick is not None
        assert tick.bid == pytest.approx(18_500.0)
        assert tick.ask == pytest.approx(18_501.0)
        assert tick.timestamp == pytest.approx(1_800_000_100.0)

        with store._lock:
            row = store._conn.execute(
                """
                SELECT source, volume, ts
                FROM broker_ticks
                WHERE symbol = ?
                """,
                ("DE40",),
            ).fetchone()
        assert row is not None
        assert str(row["source"]) == "newer_tick"
        assert float(row["volume"]) == pytest.approx(8.0)
        assert float(row["ts"]) == pytest.approx(1_800_000_100.0)
    finally:
        store.close()


def test_state_store_broker_tick_history_keeps_same_timestamp_updates(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        tick_ts = 1_800_000_100.0
        store.upsert_broker_tick(
            symbol="DE40",
            bid=18_500.0,
            ask=18_501.0,
            ts=tick_ts,
            volume=8.0,
            source="tick_one",
        )
        store.upsert_broker_tick(
            symbol="DE40",
            bid=18_502.0,
            ask=18_503.0,
            ts=tick_ts,
            volume=9.0,
            source="tick_two",
        )

        latest = store.load_latest_broker_tick("DE40", max_age_sec=10_000_000_000.0)
        assert latest is not None
        assert latest.bid == pytest.approx(18_502.0)
        assert latest.ask == pytest.approx(18_503.0)
        assert latest.timestamp == pytest.approx(tick_ts)

        with store._lock:
            rows = store._conn.execute(
                """
                SELECT bid, ask, ts, source
                FROM broker_tick_history
                WHERE symbol = ?
                ORDER BY id ASC
                """,
                ("DE40",),
            ).fetchall()

        assert len(rows) == 2
        assert float(rows[0]["bid"]) == pytest.approx(18_500.0)
        assert float(rows[1]["bid"]) == pytest.approx(18_502.0)
        assert str(rows[0]["source"]) == "tick_one"
        assert str(rows[1]["source"]) == "tick_two"
    finally:
        store.close()


def test_state_store_broker_symbol_pip_size_cache_round_trip(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        now_ts = 1_800_000_000.0
        store.upsert_broker_symbol_pip_size(
            symbol="US100",
            pip_size=1.0,
            ts=now_ts,
            source="test",
        )
        with patch("xtb_bot.state_store.time.time", return_value=now_ts + 5.0):
            loaded = store.load_broker_symbol_pip_size("US100", max_age_sec=30.0)
        assert loaded == pytest.approx(1.0)

        mapping = store.load_broker_symbol_pip_sizes(max_age_sec=0.0)
        assert mapping.get("US100") == pytest.approx(1.0)
    finally:
        store.close()


def test_state_store_infers_broker_tick_value_calibration_from_closed_positions(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="deal-us100-cal",
            symbol="US100",
            side=Side.BUY,
            volume=1.5,
            open_price=24044.7,
            stop_loss=24020.0,
            take_profit=24090.0,
            opened_at=time.time() - 60.0,
            status="closed",
            close_price=24058.0,
            closed_at=time.time(),
            pnl=22.62,
        )
        store.upsert_trade(position, "worker-us100", "momentum", "execution")
        store.record_event(
            "INFO",
            "US100",
            "Position closed",
            {
                "position_id": position.position_id,
                "close_price": 24058.0,
                "pnl": 22.62,
                "broker_close_sync": {
                    "source": "ig_confirm",
                    "realized_pnl": 22.62,
                    "pnl_currency": "EUR",
                },
            },
        )

        calibration = store.infer_broker_tick_value_calibration_from_recent_closes(
            symbol="US100",
            tick_size=1.0,
        )

        assert isinstance(calibration, dict)
        assert float(calibration.get("tick_value") or 0.0) == pytest.approx(1.1338, rel=1e-3)
        cached = store.load_broker_tick_value_calibration("US100")
        assert isinstance(cached, dict)
        assert float(cached.get("tick_value") or 0.0) == pytest.approx(1.1338, rel=1e-3)
    finally:
        store.close()


def test_record_and_load_position_updates(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.record_position_update(
            {
                "ts": 1_800_000_100.0,
                "symbol": "WTI",
                "position_id": "DIAAAA1",
                "deal_reference": "IGBOT-1",
                "operation": "open_position",
                "method": "POST",
                "path": "/positions/otc",
                "http_status": 200,
                "success": True,
                "request": {"epic": "CC.D.CL.UNC.IP", "size": 0.3},
                "response": {"dealId": "DIAAAA1", "dealReference": "IGBOT-1"},
            }
        )
        store.record_position_update(
            {
                "ts": 1_800_000_130.0,
                "symbol": "WTI",
                "position_id": "DIAAAA1",
                "deal_reference": "IGBOT-1",
                "operation": "close_position",
                "method": "DELETE",
                "path": "/positions/otc",
                "http_status": 400,
                "success": False,
                "error_text": "validation.null-not-allowed.request",
                "request": {"dealId": "DIAAAA1"},
                "response": {"errorCode": "validation.null-not-allowed.request"},
            }
        )

        rows = store.load_position_updates(position_id="DIAAAA1")
        assert len(rows) == 2
        assert str(rows[0]["operation"]) == "open_position"
        assert str(rows[1]["operation"]) == "close_position"
        assert rows[0]["request"] == {"epic": "CC.D.CL.UNC.IP", "size": 0.3}
        assert rows[1]["response"] == {"errorCode": "validation.null-not-allowed.request"}
        assert int(rows[1]["success"]) == 0
    finally:
        store.close()


def test_housekeeping_prunes_position_updates_older_than_week(tmp_path):
    week_sec = 7.0 * 24.0 * 60.0 * 60.0
    store = StateStore(
        tmp_path / "state.db",
        housekeeping_interval_sec=0.0,
        housekeeping_position_updates_retention_sec=week_sec,
    )
    try:
        now_ts = 1_900_000_000.0
        old_ts = now_ts - week_sec - 10.0
        fresh_ts = now_ts - 60.0
        store.record_position_update(
            {
                "ts": old_ts,
                "symbol": "GOLD",
                "position_id": "OLD-1",
                "operation": "open_position",
                "method": "POST",
                "path": "/positions/otc",
                "success": True,
            }
        )
        store.record_position_update(
            {
                "ts": fresh_ts,
                "symbol": "GOLD",
                "position_id": "FRESH-1",
                "operation": "open_position",
                "method": "POST",
                "path": "/positions/otc",
                "success": True,
            }
        )

        with patch("xtb_bot.state_store.time.time", return_value=now_ts):
            summary = store.run_housekeeping(force=True)

        assert int(summary.get("position_updates_deleted") or 0) >= 1
        rows = store.load_position_updates()
        ids = {str(item.get("position_id")) for item in rows}
        assert "OLD-1" not in ids
        assert "FRESH-1" in ids
    finally:
        store.close()


def test_record_position_updates_enriches_strategy_identity_from_pending_open(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_pending_open(
            PendingOpen(
                pending_id="IGBOT-42",
                symbol="WTI",
                side=Side.BUY,
                volume=0.3,
                entry=80.0,
                stop_loss=79.0,
                take_profit=82.0,
                created_at=1_800_000_000.0,
                thread_name="worker-WTI",
                strategy="multi_strategy",
                strategy_entry="momentum",
                strategy_entry_component="g1",
                strategy_entry_signal="ema_cross_up",
                mode="execution",
            )
        )

        store.record_position_update(
            {
                "ts": 1_800_000_010.0,
                "deal_reference": "IGBOT-42",
                "operation": "open_position",
                "method": "POST",
                "path": "/positions/otc",
                "http_status": 200,
                "success": True,
            }
        )

        rows = store.load_position_updates(deal_reference="IGBOT-42")
        assert len(rows) == 1
        assert rows[0]["strategy"] == "multi_strategy"
        assert rows[0]["strategy_base"] == "momentum"
        assert rows[0]["strategy_entry"] == "momentum"
        assert rows[0]["strategy_entry_component"] == "g1"
        assert rows[0]["strategy_entry_signal"] == "ema_cross_up"
        assert rows[0]["mode"] == "execution"
    finally:
        store.close()


def test_prune_stale_broker_ticks_removes_rows_older_than_threshold(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        now_ts = 1_800_000_000.0
        store.upsert_broker_tick(
            symbol="US100",
            bid=20000.0,
            ask=20001.0,
            ts=now_ts - 25.0,
            source="test",
        )
        store.upsert_broker_tick(
            symbol="US500",
            bid=5000.0,
            ask=5001.0,
            ts=now_ts - 3.0,
            source="test",
        )

        deleted = store.prune_stale_broker_ticks(max_age_sec=10.0, now_ts=now_ts)
        assert deleted >= 1

        stale = store.load_latest_broker_tick("US100", max_age_sec=10_000.0)
        fresh = store.load_latest_broker_tick("US500", max_age_sec=10_000.0)
        assert stale is None
        assert fresh is not None
    finally:
        store.close()


def test_record_event_async_writer_flushes_for_reads_and_close(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("XTB_DB_EVENTS_ASYNC_WRITER_ENABLED", "true")
    monkeypatch.setenv("XTB_DB_EVENTS_ASYNC_FLUSH_MS", "1000")
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        store.record_event("INFO", "EURUSD", "async-event-visible", {"ok": True})
        events = store.load_events(limit=10)
        assert any(str(item.get("message")) == "async-event-visible" for item in events)
    finally:
        store.close()

    reopened = StateStore(db_path)
    try:
        events = reopened.load_events(limit=10)
        assert any(str(item.get("message")) == "async-event-visible" for item in events)
    finally:
        reopened.close()


def test_record_event_falls_back_to_sync_when_event_async_writer_is_unavailable(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("XTB_DB_EVENTS_ASYNC_WRITER_ENABLED", "true")
    store = StateStore(tmp_path / "state.db")
    try:
        assert store._event_async_writer is not None
        monkeypatch.setattr(store._event_async_writer, "enqueue", lambda item: False)

        store.record_event("INFO", "EURUSD", "event-sync-fallback", {"ok": True})

        events = store.load_events(limit=10)
        assert any(str(item.get("message")) == "event-sync-fallback" for item in events)
        assert store._event_async_writer_enabled is False
    finally:
        store.close()


def test_state_store_defaults_async_writers_off(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("XTB_DB_EVENTS_ASYNC_WRITER_ENABLED", raising=False)
    monkeypatch.delenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", raising=False)
    store = StateStore(tmp_path / "state.db")
    try:
        assert store._event_async_writer_enabled is False
        assert store._multi_async_writer_enabled is False
    finally:
        store.close()


def test_state_store_quarantines_corrupt_db_and_reinitializes(tmp_path, monkeypatch: pytest.MonkeyPatch):
    check_results = iter(["database disk image is malformed", "ok"])
    monkeypatch.setattr(StateStore, "_run_quick_check", lambda self: next(check_results))
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    try:
        assert store._db_recovered_from_corruption is True
        assert store._db_quarantine_path not in (None, "")
        quarantine_path = Path(str(store._db_quarantine_path))
        assert quarantine_path.exists()
        assert db_path.exists()
        events = store.load_events(limit=10)
        assert any(
            str(item.get("message")) == "State DB quarantined and reinitialized"
            for item in events
        )
    finally:
        store.close()


def test_state_store_quarantine_reinitialize_blocks_concurrent_reads_until_reopen(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    store = StateStore(tmp_path / "state.db")
    try:
        real_open_connection = store._open_connection
        open_entered = threading.Event()
        release_open = threading.Event()
        read_started = threading.Event()
        read_finished = threading.Event()
        reader_result: dict[str, object] = {}

        def delayed_open_connection():
            open_entered.set()
            assert release_open.wait(timeout=1.0) is True
            return real_open_connection()

        monkeypatch.setattr(store, "_open_connection", delayed_open_connection)
        store._db_integrity_error = "simulated corruption"

        def reader() -> None:
            read_started.set()
            try:
                reader_result["events"] = store.load_events(limit=5)
            except Exception as exc:  # pragma: no cover - regression guard
                reader_result["error"] = exc
            finally:
                read_finished.set()

        quarantine_thread = threading.Thread(target=store._quarantine_corrupt_db_and_reinitialize)
        quarantine_thread.start()
        assert open_entered.wait(timeout=1.0) is True

        reader_thread = threading.Thread(target=reader)
        reader_thread.start()
        assert read_started.wait(timeout=1.0) is True
        assert read_finished.wait(timeout=0.05) is False

        release_open.set()
        quarantine_thread.join(timeout=1.0)
        reader_thread.join(timeout=1.0)

        assert quarantine_thread.is_alive() is False
        assert reader_thread.is_alive() is False
        assert "error" not in reader_result
        assert isinstance(reader_result.get("events"), list)
    finally:
        store.close()


def test_state_store_wal_recovery_does_not_delete_sidecars_when_checkpoint_blocked(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = tmp_path / "state.db"
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    db_path.write_bytes(b"")
    wal_path.write_bytes(b"wal")
    shm_path.write_bytes(b"shm")

    class _FakeCursor:
        def fetchone(self):
            return (1, 10, 0)

    class _FakeConn:
        def execute(self, sql):
            _ = sql
            return _FakeCursor()

        def close(self):
            return None

    store = StateStore.__new__(StateStore)
    store._db_path = db_path
    store._sqlite_timeout_sec = 1.0
    store._db_recovered_from_corruption = False

    monkeypatch.setattr("xtb_bot.state_store.sqlite3.connect", lambda *args, **kwargs: _FakeConn())

    StateStore._recover_stale_wal_if_needed(store)

    assert wal_path.exists()
    assert shm_path.exists()
    assert store._db_recovered_from_corruption is False


def test_state_store_stale_wal_recovery_success_does_not_mark_corruption(tmp_path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "state.db"
    wal_path = Path(f"{db_path}-wal")
    db_path.write_bytes(b"")
    wal_path.write_bytes(b"wal")

    class _FakeCursor:
        def fetchone(self):
            return (0, 10, 10)

    class _FakeConn:
        def execute(self, sql):
            _ = sql
            return _FakeCursor()

        def close(self):
            return None

    store = StateStore.__new__(StateStore)
    store._db_path = db_path
    store._sqlite_timeout_sec = 1.0
    store._db_recovered_from_corruption = False

    monkeypatch.setattr("xtb_bot.state_store.sqlite3.connect", lambda *args, **kwargs: _FakeConn())

    StateStore._recover_stale_wal_if_needed(store)

    assert store._db_recovered_from_corruption is False


def test_state_store_open_connection_quarantines_db_when_initial_connect_fails(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    db_path = tmp_path / "state.db"
    db_path.write_bytes(b"not-a-real-sqlite-db")
    real_connect = sqlite3.connect
    attempts = {"count": 0}

    def fake_connect(path, *args, **kwargs):
        if str(path) == str(db_path) and attempts["count"] == 0:
            attempts["count"] += 1
            raise sqlite3.DatabaseError("database disk image is malformed")
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(StateStore, "_recover_stale_wal_if_needed", lambda self: None)
    monkeypatch.setattr("xtb_bot.state_store.sqlite3.connect", fake_connect)

    store = StateStore(db_path)
    try:
        assert store._db_quarantine_path not in (None, "")
        quarantine_path = Path(str(store._db_quarantine_path))
        assert quarantine_path.exists()
        assert db_path.exists()
    finally:
        store.close()


def test_multi_position_state_round_trip_and_reconciliation(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "true")
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_FLUSH_MS", "10")
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_BATCH_SIZE", "16")
    store = StateStore(tmp_path / "state.db")
    try:
        event_id = store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="trend_following",
            qty_lots=1.0,
            reason="trend_up",
        )
        assert len(event_id) == 32
        assert uuid.UUID(hex=event_id).version == 7
        store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="mean_breakout_v2",
            qty_lots=-0.4,
            reason="zscore_fade",
        )
        store.upsert_real_position_cache(
            symbol="US100",
            real_qty_lots=0.8,
            broker_ts=1_800_000_010.0,
            source="position_book",
        )
        store.upsert_system_error_qty(symbol="US100", err_qty_lots=0.0, reason=None)

        assert store.flush_multi_async_writes(timeout_sec=1.0) is True
        hot = store.load_hot_multi_positions_snapshot()
        assert ("US100", "trend_following") in hot["virtual_positions"]
        assert "US100" in hot["real_positions"]

        virtual_rows = store.load_virtual_positions_state(symbol="US100")
        by_strategy = {str(item["strategy_id"]): float(item["qty_lots"]) for item in virtual_rows}
        assert by_strategy == {
            "mean_breakout_v2": pytest.approx(-0.4),
            "trend_following": pytest.approx(1.0),
        }

        ledger_rows = store.load_virtual_positions_ledger(limit=10)
        assert len(ledger_rows) >= 2
        assert str(ledger_rows[0]["symbol"]) == "US100"

        reconciliation_rows = store.load_multi_position_reconciliation(epsilon_lots=1e-6)
        assert len(reconciliation_rows) == 1
        row = reconciliation_rows[0]
        assert str(row["symbol"]) == "US100"
        assert float(row["sum_virtual"]) == pytest.approx(0.6)
        assert float(row["real_qty"]) == pytest.approx(0.8)
        assert float(row["err_qty"]) == pytest.approx(0.0)
        assert float(row["diff"]) == pytest.approx(0.2)

        store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="mean_breakout_v2",
            qty_lots=0.0,
        )
        rows_after_zero = store.load_virtual_positions_state(symbol="US100")
        assert len(rows_after_zero) == 1
        assert str(rows_after_zero[0]["strategy_id"]) == "trend_following"
    finally:
        store.close()


def test_multi_async_writer_updates_hot_cache_before_async_flush(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "true")
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_FLUSH_MS", "1000")
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="trend_following",
            qty_lots=1.0,
            reason="trend_up",
        )
        store.upsert_real_position_cache(
            symbol="US100",
            real_qty_lots=0.8,
            broker_ts=1_800_000_010.0,
            source="position_book",
        )
        store.upsert_system_error_qty(symbol="US100", err_qty_lots=0.2, reason="sync_gap")

        hot = store.load_hot_multi_positions_snapshot()
        assert hot["virtual_positions"][("US100", "trend_following")]["qty_lots"] == pytest.approx(1.0)
        assert hot["real_positions"]["US100"]["real_qty_lots"] == pytest.approx(0.8)
        assert hot["system_errors"]["US100"]["err_qty_lots"] == pytest.approx(0.2)
    finally:
        store.close()


def test_multi_async_writer_keeps_hot_cache_live_even_when_batch_persist_fails(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "true")
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_FLUSH_MS", "10")
    store = StateStore(tmp_path / "state.db")
    try:
        def _fail_batch(batch):
            _ = batch
            raise sqlite3.OperationalError("simulated async batch failure")

        monkeypatch.setattr(store, "_apply_multi_async_batch", _fail_batch)

        store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="trend_following",
            qty_lots=1.0,
            reason="trend_up",
        )
        store.upsert_real_position_cache(
            symbol="US100",
            real_qty_lots=0.8,
            broker_ts=1_800_000_010.0,
            source="position_book",
        )
        store.upsert_system_error_qty(symbol="US100", err_qty_lots=0.2, reason="sync_gap")

        assert store.flush_multi_async_writes(timeout_sec=1.0) is False
        hot = store.load_hot_multi_positions_snapshot()
        assert hot["virtual_positions"][("US100", "trend_following")]["qty_lots"] == pytest.approx(1.0)
        assert hot["real_positions"]["US100"]["real_qty_lots"] == pytest.approx(0.8)
        assert hot["system_errors"]["US100"]["err_qty_lots"] == pytest.approx(0.2)

        with store._lock:
            virtual_rows = store._conn.execute("SELECT * FROM virtual_positions_current").fetchall()
            real_rows = store._conn.execute("SELECT * FROM real_positions_cache").fetchall()
            error_rows = store._conn.execute("SELECT * FROM system_errors").fetchall()
        assert virtual_rows == []
        assert real_rows == []
        assert error_rows == []
    finally:
        store.close()


def test_multi_async_writer_falls_back_to_sync_when_writer_is_unavailable(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "true")
    store = StateStore(tmp_path / "state.db")
    try:
        assert store._multi_async_writer is not None
        monkeypatch.setattr(store._multi_async_writer, "enqueue", lambda item: False)

        event_id = store.upsert_virtual_position_state(
            symbol="US100",
            strategy_id="trend_following",
            qty_lots=1.0,
            reason="trend_up",
        )

        rows = store.load_virtual_positions_state(symbol="US100")
        assert len(rows) == 1
        assert len(event_id) == 32
        assert str(rows[0]["strategy_id"]) == "trend_following"
        assert store._multi_async_writer_enabled is False
    finally:
        store.close()


def test_multi_async_writer_flushes_pending_rows_on_close(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "true")
    monkeypatch.setenv("XTB_MULTI_DB_ASYNC_FLUSH_MS", "1000")
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    store.upsert_virtual_position_state(
        symbol="DE40",
        strategy_id="g1",
        qty_lots=0.7,
        reason="trend_up",
    )
    store.close()

    reopened = StateStore(db_path)
    try:
        rows = reopened.load_virtual_positions_state(symbol="DE40")
        assert len(rows) == 1
        assert str(rows[0]["strategy_id"]) == "g1"
        assert float(rows[0]["qty_lots"]) == pytest.approx(0.7)
    finally:
        reopened.close()


def test_trade_performance_upserts_extrema_and_finalization(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="p-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=1_800_000_000.0,
            status="open",
            close_price=None,
            closed_at=None,
            pnl=0.0,
        )
        store.upsert_trade(position, "worker-EURUSD", "momentum", "paper")

        store.update_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            opened_at=position.opened_at,
            max_favorable_pips=12.0,
            max_adverse_pips=4.0,
            max_favorable_pnl=18.0,
            max_adverse_pnl=-6.0,
        )
        store.update_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            opened_at=position.opened_at,
            max_favorable_pips=9.0,
            max_adverse_pips=7.0,
            max_favorable_pnl=15.0,
            max_adverse_pnl=-4.0,
        )
        store.finalize_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            closed_at=1_800_000_300.0,
            close_reason="test_close_reason",
        )
        row = store.load_trade_performance(position.position_id)
        assert row is not None
        assert float(row["max_favorable_pips"]) == pytest.approx(12.0)
        assert float(row["max_adverse_pips"]) == pytest.approx(7.0)
        assert float(row["max_favorable_pnl"]) == pytest.approx(18.0)
        assert float(row["max_adverse_pnl"]) == pytest.approx(-6.0)
        assert float(row["closed_at"]) == pytest.approx(1_800_000_300.0)
        assert str(row["close_reason"]) == "test_close_reason"

        store.update_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            opened_at=position.opened_at,
            closed_at=1_800_000_999.0,
            max_favorable_pips=5.0,
            max_adverse_pips=2.0,
            max_favorable_pnl=4.0,
            max_adverse_pnl=-2.0,
        )
        row_after_reupdate = store.load_trade_performance(position.position_id)
        assert row_after_reupdate is not None
        assert float(row_after_reupdate["closed_at"]) == pytest.approx(1_800_000_300.0)
    finally:
        store.close()


def test_upsert_trade_allows_clearing_nullable_metadata_fields(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="p-clear-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=1_800_000_000.0,
            status="open",
            pnl=0.0,
            epic="CS.D.EURUSD.CFD.IP",
            epic_variant="primary",
            strategy_entry="momentum",
            strategy_entry_component="index_hybrid",
            strategy_entry_signal="breakout",
            strategy_exit="momentum",
            strategy_exit_component="index_hybrid",
            strategy_exit_signal="reverse",
        )
        store.upsert_trade(position, "worker-EURUSD", "momentum", "execution")

        position.epic = None
        position.epic_variant = None
        position.strategy_entry_component = None
        position.strategy_exit_component = None
        position.strategy_exit_signal = None
        store.upsert_trade(position, "worker-EURUSD", "momentum", "execution")

        with store._lock:
            row = store._conn.execute(
                """
                SELECT epic, epic_variant, strategy_entry_component, strategy_exit_component, strategy_exit_signal
                FROM trades
                WHERE position_id = ?
                """,
                (position.position_id,),
            ).fetchone()
        assert row is not None
        assert row["epic"] is None
        assert row["epic_variant"] is None
        assert row["strategy_entry_component"] is None
        assert row["strategy_exit_component"] is None
        assert row["strategy_exit_signal"] is None
    finally:
        store.close()


def test_trade_execution_quality_persists_and_summarizes_by_strategy_entry(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        first = Position(
            position_id="p-fill-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=1_800_000_000.0,
            status="closed",
            close_price=1.1040,
            closed_at=1_800_000_300.0,
            pnl=40.0,
        )
        second = Position(
            position_id="p-fill-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1010,
            stop_loss=1.0960,
            take_profit=1.1110,
            opened_at=1_800_000_400.0,
            status="closed",
            close_price=1.1050,
            closed_at=1_800_000_700.0,
            pnl=40.0,
        )
        store.upsert_trade(first, "worker-EURUSD", "momentum", "execution", strategy_entry="momentum")
        store.upsert_trade(second, "worker-EURUSD", "momentum", "execution", strategy_entry="momentum")
        store.update_trade_execution_quality(
            position_id=first.position_id,
            symbol=first.symbol,
            entry_reference_price=1.1000,
            entry_fill_price=1.1003,
            entry_slippage_pips=3.0,
            entry_adverse_slippage_pips=3.0,
            exit_reference_price=1.1040,
            exit_fill_price=1.1038,
            exit_slippage_pips=2.0,
            exit_adverse_slippage_pips=2.0,
        )
        store.update_trade_execution_quality(
            position_id=second.position_id,
            symbol=second.symbol,
            entry_reference_price=1.1010,
            entry_fill_price=1.1012,
            entry_slippage_pips=2.0,
            entry_adverse_slippage_pips=2.0,
            exit_reference_price=1.1050,
            exit_fill_price=1.1049,
            exit_slippage_pips=1.0,
            exit_adverse_slippage_pips=1.0,
        )

        first_row = store.load_trade_performance(first.position_id)
        assert first_row is not None
        assert float(first_row["entry_reference_price"]) == pytest.approx(1.1000)
        assert float(first_row["entry_fill_price"]) == pytest.approx(1.1003)
        assert float(first_row["entry_slippage_pips"]) == pytest.approx(3.0)
        assert float(first_row["exit_adverse_slippage_pips"]) == pytest.approx(2.0)

        summary = store.summarize_recent_trade_execution_quality(
            symbol="EURUSD",
            strategy_entry="momentum",
            limit=10,
        )
        assert summary is not None
        assert int(summary["sample_count"]) == 2
        assert float(summary["avg_entry_adverse_slippage_pips"]) == pytest.approx(2.5)
        assert float(summary["avg_exit_adverse_slippage_pips"]) == pytest.approx(1.5)
        assert float(summary["avg_total_adverse_slippage_pips"]) == pytest.approx(4.0)
        assert float(summary["max_total_adverse_slippage_pips"]) == pytest.approx(5.0)
        assert float(summary["adverse_trade_share"]) == pytest.approx(1.0)
    finally:
        store.close()


def test_trade_execution_quality_summary_prefers_strategy_entry_component(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        position = Position(
            position_id="p-fill-component-1",
            symbol="AUS200",
            side=Side.BUY,
            volume=1.0,
            open_price=8429.4,
            stop_loss=8399.0,
            take_profit=8505.0,
            opened_at=1_800_000_000.0,
            status="closed",
            close_price=8399.0,
            closed_at=1_800_000_120.0,
            pnl=-91.2,
        )
        store.upsert_trade(
            position,
            "worker-AUS200",
            "momentum",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="donchian_breakout",
        )
        store.update_trade_execution_quality(
            position_id=position.position_id,
            symbol=position.symbol,
            entry_reference_price=8429.4,
            entry_fill_price=8429.8,
            entry_slippage_pips=0.4,
            entry_adverse_slippage_pips=0.4,
            exit_reference_price=8399.0,
            exit_fill_price=8398.6,
            exit_slippage_pips=0.4,
            exit_adverse_slippage_pips=0.4,
        )

        component_summary = store.summarize_recent_trade_execution_quality(
            symbol="AUS200",
            strategy_entry="donchian_breakout",
            limit=10,
        )
        assert component_summary is not None
        assert int(component_summary["sample_count"]) == 1
        assert float(component_summary["avg_total_adverse_slippage_pips"]) == pytest.approx(0.8)

        base_summary = store.summarize_recent_trade_execution_quality(
            symbol="AUS200",
            strategy_entry="momentum",
            limit=10,
        )
        assert base_summary is None
    finally:
        store.close()


def test_load_recent_strategy_entry_confidence_samples_prefers_strategy_entry_component(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        first = Position(
            position_id="p-confidence-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=1_800_000_000.0,
            status="closed",
            close_price=1.1010,
            closed_at=1_800_000_100.0,
            pnl=10.0,
            entry_confidence=0.82,
        )
        second = Position(
            position_id="p-confidence-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1010,
            stop_loss=1.0960,
            take_profit=1.1110,
            opened_at=1_800_000_200.0,
            status="closed",
            close_price=1.1020,
            closed_at=1_800_000_300.0,
            pnl=10.0,
            entry_confidence=0.34,
        )
        third = Position(
            position_id="p-confidence-3",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1020,
            stop_loss=1.0970,
            take_profit=1.1120,
            opened_at=1_800_000_400.0,
            status="closed",
            close_price=1.1030,
            closed_at=1_800_000_500.0,
            pnl=10.0,
            entry_confidence=0.55,
        )
        store.upsert_trade(
            first,
            "worker-EURUSD",
            "momentum",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="donchian_breakout",
        )
        store.upsert_trade(
            second,
            "worker-EURUSD",
            "momentum",
            "execution",
            strategy_entry="momentum",
            strategy_entry_component="donchian_breakout",
        )
        store.upsert_trade(
            third,
            "worker-EURUSD",
            "momentum",
            "paper",
            strategy_entry="momentum",
            strategy_entry_component="donchian_breakout",
        )

        component_samples = store.load_recent_strategy_entry_confidence_samples(
            symbol="EURUSD",
            strategy_entry="donchian_breakout",
            limit=5,
        )
        assert component_samples == pytest.approx([0.55, 0.34, 0.82])

        exec_samples = store.load_recent_strategy_entry_confidence_samples(
            symbol="EURUSD",
            strategy_entry="donchian_breakout",
            mode="execution",
            limit=5,
        )
        assert exec_samples == pytest.approx([0.34, 0.82])

        base_samples = store.load_recent_strategy_entry_confidence_samples(
            symbol="EURUSD",
            strategy_entry="momentum",
            limit=5,
        )
        assert base_samples == []
    finally:
        store.close()
