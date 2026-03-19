from __future__ import annotations

from unittest.mock import patch

import pytest

from xtb_bot.state_store import StateStore


def test_state_store_initializes_pragmas_and_indexes(tmp_path):
    store = StateStore(tmp_path / "state.db", sqlite_timeout_sec=42.0)
    try:
        with store._lock:
            journal_mode_row = store._conn.execute("PRAGMA journal_mode").fetchone()
            busy_timeout_row = store._conn.execute("PRAGMA busy_timeout").fetchone()
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

        assert journal_mode_row is not None
        assert str(journal_mode_row[0]).lower() == "wal"
        assert busy_timeout_row is not None
        assert int(busy_timeout_row[0]) >= 42_000
        assert "idx_trades_status_mode_opened_at" in trades_indexes
        assert "idx_pending_opens_mode_created_at" in pending_indexes
        assert "idx_events_ts" in events_indexes
        assert "idx_events_level_ts" in events_indexes
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
