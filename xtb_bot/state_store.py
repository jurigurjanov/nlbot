from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import json
import logging
import math
import os
import queue
import re
import sqlite3
import statistics
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Callable, Generic, TypeVar
import uuid
import secrets

from xtb_bot.models import AccountSnapshot, PendingOpen, Position, PriceTick, Side, SymbolSpec, WorkerState

_T = TypeVar("_T")
_BatchT = TypeVar("_BatchT")
_UNSET = object()
logger = logging.getLogger(__name__)
_SQLITE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LATEST_STATE_STORE_SCHEMA_VERSION = 5


def _quote_sqlite_identifier(name: str) -> str:
    normalized = str(name or "").strip()
    if not _SQLITE_IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"invalid sqlite identifier: {name!r}")
    return f'"{normalized}"'


def _uuid7_hex(ts_ms: int | None = None) -> str:
    """Generate a UUIDv7-compatible hex identifier.

    Python 3.12 stdlib does not expose uuid7 yet, so we build RFC9562 layout manually:
    - 48 bits unix timestamp in ms
    - 4 bits version (0b0111)
    - 12 bits random A
    - 2 bits variant (0b10)
    - 62 bits random B
    """
    ms = int(time.time() * 1000) if ts_ms is None else int(ts_ms)
    ms = max(0, min(ms, (1 << 48) - 1))
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = 0
    value |= ms << 80
    value |= 0x7 << 76
    value |= rand_a << 64
    value |= 0b10 << 62
    value |= rand_b
    return uuid.UUID(int=value).hex


def _normalize_broker_symbol_spec_epic(epic: object | None) -> str:
    return str(epic or "").strip().upper()


def _normalize_broker_symbol_spec_epic_variant(
    epic_variant: object | None,
    *,
    epic: object | None = None,
) -> str:
    normalized = str(epic_variant or "").strip().lower()
    if normalized:
        return normalized
    upper_epic = _normalize_broker_symbol_spec_epic(epic)
    if ".CASH." in upper_epic:
        return "cash"
    if ".DAILY." in upper_epic:
        return "daily"
    if ".IFS." in upper_epic:
        return "ifs"
    if ".IFD." in upper_epic:
        return "ifd"
    if ".CFD." in upper_epic:
        return "cfd"
    if upper_epic:
        return "other"
    return ""


def _broker_symbol_spec_cache_key(symbol: str, epic: str) -> str:
    upper_symbol = str(symbol or "").strip().upper()
    upper_epic = _normalize_broker_symbol_spec_epic(epic)
    if not upper_epic:
        return upper_symbol
    return f"{upper_symbol}|{upper_epic}"


class _AsyncBatchWriter(Generic[_BatchT]):
    def __init__(
        self,
        *,
        name: str,
        flush_interval_sec: float,
        batch_size: int,
        apply_batch: Callable[[list[_BatchT]], None],
        on_failure: Callable[[Exception], None] | None = None,
    ) -> None:
        self._name = str(name)
        self._flush_interval_sec = max(0.01, float(flush_interval_sec))
        self._batch_size = max(1, int(batch_size))
        self._apply_batch = apply_batch
        self._on_failure = on_failure
        self._queue: queue.Queue[tuple[str, _BatchT | None, threading.Event | None]] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure_lock = threading.Lock()
        self._failure_count = 0
        self._last_error: str | None = None

    @property
    def failure_count(self) -> int:
        with self._failure_lock:
            return self._failure_count

    @property
    def last_error(self) -> str | None:
        with self._failure_lock:
            return self._last_error

    @property
    def thread(self) -> threading.Thread | None:
        return self._thread

    @property
    def queue_size(self) -> int:
        return int(self._queue.qsize())

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name=self._name, daemon=True)
        self._thread.start()

    def enqueue(self, item: _BatchT) -> bool:
        thread = self._thread
        if thread is None or not thread.is_alive():
            return False
        self._queue.put(("item", item, None))
        return True

    def flush(self, timeout_sec: float = 2.0) -> bool:
        thread = self._thread
        if thread is None:
            return True
        if not thread.is_alive():
            return False
        waiter = threading.Event()
        self._queue.put(("flush", None, waiter))
        return waiter.wait(timeout=max(0.05, float(timeout_sec)))

    def close(self, *, flush_timeout_sec: float, join_timeout_sec: float) -> tuple[bool, bool]:
        thread = self._thread
        if thread is None:
            return True, True
        flushed = self.flush(timeout_sec=flush_timeout_sec)
        self._stop.set()
        if thread.is_alive():
            wake_event = threading.Event()
            self._queue.put(("wake", None, wake_event))
            wake_event.wait(timeout=1.0)
            thread.join(timeout=max(0.05, float(join_timeout_sec)))
        alive = thread.is_alive()
        self._thread = None
        return flushed, not alive

    def _loop(self) -> None:
        pending: list[_BatchT] = []
        try:
            while True:
                if self._stop.is_set() and self._queue.empty():
                    break
                try:
                    op, payload, waiter = self._queue.get(timeout=self._flush_interval_sec)
                except queue.Empty:
                    self._flush_pending(pending)
                    continue

                if op == "flush":
                    self._flush_pending(pending)
                    if waiter is not None:
                        waiter.set()
                    continue
                if op == "wake":
                    if waiter is not None:
                        waiter.set()
                    continue

                if payload is not None:
                    pending.append(payload)
                if len(pending) >= self._batch_size:
                    self._flush_pending(pending)
            self._flush_pending(pending)
        except Exception as exc:  # pragma: no cover - defensive guard
            self._record_failure(exc)
            logger.exception("%s: writer loop crashed", self._name)
            self._stop.set()

    _MAX_BATCH_RETRIES = 2

    def _flush_pending(self, pending: list[_BatchT]) -> None:
        if not pending:
            return
        batch = list(pending)
        pending.clear()
        for attempt in range(1, self._MAX_BATCH_RETRIES + 1):
            try:
                self._apply_batch(batch)
                return
            except Exception as exc:
                self._record_failure(exc)
                if attempt < self._MAX_BATCH_RETRIES:
                    logger.warning(
                        "%s: batch apply failed (attempt %d/%d, %d ops), retrying",
                        self._name, attempt, self._MAX_BATCH_RETRIES, len(batch),
                    )
                else:
                    logger.exception(
                        "%s: batch apply failed after %d attempts (%d ops dropped)",
                        self._name, self._MAX_BATCH_RETRIES, len(batch),
                    )

    def _record_failure(self, exc: Exception) -> None:
        with self._failure_lock:
            self._failure_count += 1
            self._last_error = f"{type(exc).__name__}: {exc}"
        if self._on_failure is not None:
            self._on_failure(exc)


class StateStore:
    def __init__(
        self,
        db_path: Path,
        *,
        sqlite_timeout_sec: float = 60.0,
        price_history_cleanup_every: int = 100,
        price_history_cleanup_min_interval_sec: float = 30.0,
        housekeeping_interval_sec: float = 300.0,
        housekeeping_events_keep_rows: int = 50_000,
        housekeeping_incremental_vacuum_pages: int = 256,
        housekeeping_position_updates_retention_sec: float = 7.0 * 24.0 * 60.0 * 60.0,
        position_updates_cleanup_min_interval_sec: float = 30.0,
    ):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._sqlite_timeout_sec = max(1.0, float(sqlite_timeout_sec))
        self._price_history_cleanup_every = max(1, int(price_history_cleanup_every))
        self._price_history_cleanup_min_interval_sec = max(
            0.0,
            float(price_history_cleanup_min_interval_sec),
        )
        self._price_history_cleanup_counts: dict[str, int] = {}
        self._price_history_last_cleanup_ts: dict[str, float] = {}
        self._candle_history_cleanup_counts: dict[tuple[str, int], int] = {}
        self._candle_history_last_cleanup_ts: dict[tuple[str, int], float] = {}
        self._housekeeping_interval_sec = max(0.0, float(housekeeping_interval_sec))
        self._housekeeping_events_keep_rows = max(100, int(housekeeping_events_keep_rows))
        self._housekeeping_incremental_vacuum_pages = max(0, int(housekeeping_incremental_vacuum_pages))
        self._housekeeping_position_updates_retention_sec = max(
            60.0,
            float(housekeeping_position_updates_retention_sec),
        )
        self._position_updates_cleanup_min_interval_sec = max(
            1.0,
            float(position_updates_cleanup_min_interval_sec),
        )
        self._last_position_updates_cleanup_ts = 0.0
        self._last_housekeeping_ts = 0.0
        self._db_integrity_ok = True
        self._db_integrity_error: str | None = None
        self._db_recovered_from_corruption = False
        self._db_quarantine_path: str | None = None
        self._conn = self._open_connection()
        self._closed = False
        raw_async_multi_writer = os.getenv("XTB_MULTI_DB_ASYNC_WRITER_ENABLED", "false")
        self._multi_async_writer_enabled = str(raw_async_multi_writer).strip().lower() in {"1", "true", "yes", "on"}
        raw_async_flush_ms = os.getenv("XTB_MULTI_DB_ASYNC_FLUSH_MS", "200")
        try:
            async_flush_ms = float(raw_async_flush_ms)
        except (TypeError, ValueError):
            async_flush_ms = 200.0
        self._multi_async_flush_interval_sec = max(0.05, async_flush_ms / 1000.0)
        raw_async_batch_size = os.getenv("XTB_MULTI_DB_ASYNC_BATCH_SIZE", "1024")
        try:
            async_batch_size = int(float(raw_async_batch_size))
        except (TypeError, ValueError):
            async_batch_size = 1024
        self._multi_async_batch_size = max(1, async_batch_size)
        self._multi_async_writer: _AsyncBatchWriter[tuple[str, tuple[Any, ...]]] | None = None
        self._multi_async_failure_count = 0
        self._multi_async_last_error: str | None = None
        raw_event_async_writer = os.getenv("XTB_DB_EVENTS_ASYNC_WRITER_ENABLED", "false")
        self._event_async_writer_enabled = str(raw_event_async_writer).strip().lower() in {"1", "true", "yes", "on"}
        raw_event_async_flush_ms = os.getenv("XTB_DB_EVENTS_ASYNC_FLUSH_MS", "300")
        try:
            event_async_flush_ms = float(raw_event_async_flush_ms)
        except (TypeError, ValueError):
            event_async_flush_ms = 300.0
        self._event_async_flush_interval_sec = max(0.05, event_async_flush_ms / 1000.0)
        raw_event_async_batch_size = os.getenv("XTB_DB_EVENTS_ASYNC_BATCH_SIZE", "512")
        try:
            event_async_batch_size = int(float(raw_event_async_batch_size))
        except (TypeError, ValueError):
            event_async_batch_size = 512
        self._event_async_batch_size = max(1, event_async_batch_size)
        self._event_async_writer: _AsyncBatchWriter[tuple[Any, ...]] | None = None
        self._hot_lock = threading.Lock()
        self._hot_virtual_positions: dict[tuple[str, str], dict[str, Any]] = {}
        self._hot_real_positions: dict[str, dict[str, Any]] = {}
        self._hot_system_errors: dict[str, dict[str, Any]] = {}
        self._init_schema()
        self._check_integrity_and_disable_async_writers_if_needed()
        self._load_hot_multi_state()
        if self._event_async_writer_enabled:
            self._start_event_async_writer()
        if self._multi_async_writer_enabled:
            self._start_multi_async_writer()

    def _open_connection(self) -> sqlite3.Connection:
        self._recover_stale_wal_if_needed()
        try:
            conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                timeout=self._sqlite_timeout_sec,
            )
        except sqlite3.DatabaseError as exc:
            logger.error(
                "state_store: failed to open DB cleanly, quarantining database instead of deleting WAL: %s",
                exc,
            )
            self._quarantine_db_files(str(exc))
            conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                timeout=self._sqlite_timeout_sec,
            )
        conn.row_factory = sqlite3.Row
        return conn

    def _quarantine_db_files(self, reason: str) -> Path:
        quarantine_suffix = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        quarantine_path = self._db_path.with_name(
            f"{self._db_path.stem}.corrupt-{quarantine_suffix}{self._db_path.suffix}"
        )
        for source, target in (
            (self._db_path, quarantine_path),
            (Path(f"{self._db_path}-wal"), Path(f"{quarantine_path}-wal")),
            (Path(f"{self._db_path}-shm"), Path(f"{quarantine_path}-shm")),
        ):
            if not source.exists():
                continue
            try:
                source.replace(target)
            except OSError:
                logger.exception("Failed to quarantine SQLite sidecar %s", source)
        self._db_quarantine_path = str(quarantine_path)
        self._db_recovered_from_corruption = True
        logger.warning(
            "state_store: quarantined DB files to %s (reason=%s)",
            quarantine_path,
            reason,
        )
        return quarantine_path

    def _recover_stale_wal_if_needed(self) -> None:
        """Recover from a previous unclean shutdown by checkpointing stale WAL.

        If .db-wal exists when we open a fresh connection, it means the previous
        session did not cleanly checkpoint. We open a temporary connection, force
        a TRUNCATE checkpoint to fold the WAL back into the main DB, then close it.
        This prevents the new session from inheriting a corrupted/locked WAL state.
        """
        wal_path = Path(str(self._db_path) + "-wal")
        shm_path = Path(str(self._db_path) + "-shm")
        if not wal_path.exists():
            return
        wal_size = wal_path.stat().st_size
        logger.warning(
            "state_store: stale WAL file detected (%d bytes), attempting recovery checkpoint",
            wal_size,
        )
        try:
            recovery_conn = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                timeout=max(10.0, self._sqlite_timeout_sec),
            )
            recovery_busy_timeout_ms = max(10_000, int(self._sqlite_timeout_sec * 1000))
            recovery_conn.execute(f"PRAGMA busy_timeout={recovery_busy_timeout_ms}")
            recovery_conn.execute("PRAGMA journal_mode=WAL")
            result = recovery_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            logger.info(
                "state_store: WAL recovery checkpoint result: blocked=%s, wal_pages=%s, checkpointed=%s",
                result[0] if result else "?",
                result[1] if result else "?",
                result[2] if result else "?",
            )
            recovery_conn.close()
            # If WAL is still there after checkpoint, it's zero-length (truncated) — that's fine.
            # If checkpoint is blocked, leave files untouched and let the later
            # open/quarantine path preserve data instead of deleting it.
            if result and result[0] != 0:
                logger.warning(
                    "state_store: WAL checkpoint was blocked; leaving WAL/SHM untouched for quarantine handling"
                )
            else:
                logger.info("state_store: WAL recovery checkpoint succeeded")
        except Exception as exc:
            logger.error("state_store: WAL recovery checkpoint failed: %s", exc)
            logger.warning(
                "state_store: leaving WAL/SHM untouched after failed recovery; open/quarantine path will decide next step"
            )

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            busy_timeout_ms = max(1_000, int(self._sqlite_timeout_sec * 1000))
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
            cur.execute("PRAGMA auto_vacuum=INCREMENTAL")
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS worker_states (
                    symbol TEXT PRIMARY KEY,
                    thread_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    strategy_base TEXT,
                    position_strategy_entry TEXT,
                    position_strategy_component TEXT,
                    position_strategy_signal TEXT,
                    last_price REAL,
                    last_heartbeat REAL NOT NULL,
                    iteration INTEGER NOT NULL,
                    position_json TEXT,
                    last_error TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trades (
                    position_id TEXT PRIMARY KEY,
                    deal_reference TEXT,
                    symbol TEXT NOT NULL,
                    epic TEXT,
                    epic_variant TEXT,
                    side TEXT NOT NULL,
                    volume REAL NOT NULL,
                    open_price REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    take_profit REAL NOT NULL,
                    opened_at REAL NOT NULL,
                    entry_confidence REAL NOT NULL DEFAULT 0.0,
                    status TEXT NOT NULL,
                    close_price REAL,
                    closed_at REAL,
                    pnl REAL NOT NULL,
                    pnl_is_estimated INTEGER NOT NULL DEFAULT 0,
                    pnl_estimate_source TEXT,
                    thread_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    strategy_entry TEXT,
                    strategy_entry_component TEXT,
                    strategy_entry_signal TEXT,
                    strategy_exit TEXT,
                    strategy_exit_component TEXT,
                    strategy_exit_signal TEXT,
                    mode TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    level TEXT NOT NULL,
                    symbol TEXT,
                    message TEXT NOT NULL,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS account_snapshots (
                    ts REAL PRIMARY KEY,
                    balance REAL NOT NULL,
                    equity REAL NOT NULL,
                    margin_free REAL NOT NULL,
                    open_positions INTEGER NOT NULL,
                    daily_pnl REAL NOT NULL,
                    drawdown_pct REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ig_rate_limit_workers (
                    worker_key TEXT PRIMARY KEY,
                    limit_scope TEXT NOT NULL,
                    limit_unit TEXT NOT NULL,
                    limit_value INTEGER NOT NULL,
                    used_value INTEGER NOT NULL,
                    remaining_value INTEGER NOT NULL,
                    window_sec REAL NOT NULL,
                    blocked_total INTEGER NOT NULL,
                    last_request_ts REAL,
                    last_block_ts REAL,
                    notes TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS broker_ticks (
                    symbol TEXT PRIMARY KEY,
                    bid REAL NOT NULL,
                    ask REAL NOT NULL,
                    mid REAL NOT NULL,
                    ts REAL NOT NULL,
                    volume REAL,
                    source TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS broker_tick_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    bid REAL NOT NULL,
                    ask REAL NOT NULL,
                    mid REAL NOT NULL,
                    ts REAL NOT NULL,
                    volume REAL,
                    source TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS broker_symbol_specs (
                    cache_key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    epic TEXT,
                    epic_variant TEXT,
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
                );

                CREATE TABLE IF NOT EXISTS broker_symbol_pip_sizes (
                    symbol TEXT PRIMARY KEY,
                    pip_size REAL NOT NULL,
                    source TEXT,
                    ts REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS broker_account_cache (
                    cache_key TEXT PRIMARY KEY,
                    balance REAL NOT NULL,
                    equity REAL NOT NULL,
                    margin_free REAL NOT NULL,
                    ts REAL NOT NULL,
                    source TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS open_slot_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pending_opens (
                    pending_id TEXT PRIMARY KEY,
                    position_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    volume REAL NOT NULL,
                    entry REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    take_profit REAL NOT NULL,
                    created_at REAL NOT NULL,
                    thread_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    strategy_entry TEXT,
                    strategy_entry_component TEXT,
                    strategy_entry_signal TEXT,
                    mode TEXT NOT NULL,
                    entry_confidence REAL NOT NULL DEFAULT 0.0,
                    trailing_override_json TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS price_history (
                    symbol TEXT NOT NULL,
                    ts REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    PRIMARY KEY(symbol, ts)
                );

                CREATE TABLE IF NOT EXISTS candle_history (
                    symbol TEXT NOT NULL,
                    resolution_sec INTEGER NOT NULL,
                    bucket_start_ts REAL NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL NOT NULL,
                    volume REAL,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(symbol, resolution_sec, bucket_start_ts)
                );

                CREATE TABLE IF NOT EXISTS position_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    symbol TEXT,
                    position_id TEXT,
                    deal_reference TEXT,
                    operation TEXT NOT NULL,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    http_status INTEGER,
                    success INTEGER NOT NULL,
                    error_text TEXT,
                    request_json TEXT,
                    response_json TEXT,
                    strategy TEXT,
                    strategy_base TEXT,
                    strategy_entry TEXT,
                    strategy_entry_component TEXT,
                    strategy_entry_signal TEXT,
                    mode TEXT,
                    source TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trade_performance (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    opened_at REAL,
                    closed_at REAL,
                    close_reason TEXT,
                    max_favorable_pips REAL NOT NULL DEFAULT 0.0,
                    max_adverse_pips REAL NOT NULL DEFAULT 0.0,
                    max_favorable_pnl REAL NOT NULL DEFAULT 0.0,
                    max_adverse_pnl REAL NOT NULL DEFAULT 0.0,
                    entry_reference_price REAL,
                    entry_fill_price REAL,
                    entry_slippage_pips REAL,
                    entry_adverse_slippage_pips REAL,
                    exit_reference_price REAL,
                    exit_fill_price REAL,
                    exit_slippage_pips REAL,
                    exit_adverse_slippage_pips REAL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS virtual_positions_current (
                    symbol TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    qty_lots REAL NOT NULL,
                    source TEXT,
                    reason TEXT,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY(symbol, strategy_id)
                );

                CREATE TABLE IF NOT EXISTS virtual_positions_ledger (
                    event_id TEXT PRIMARY KEY,
                    ts REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    qty_lots REAL NOT NULL,
                    source TEXT,
                    reason TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS real_positions_cache (
                    symbol TEXT PRIMARY KEY,
                    real_qty_lots REAL NOT NULL,
                    broker_ts REAL,
                    source TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS system_errors (
                    symbol TEXT PRIMARY KEY,
                    err_qty_lots REAL NOT NULL,
                    reason TEXT,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    description TEXT NOT NULL,
                    applied_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_trades_status_mode_opened_at
                    ON trades(status, mode, opened_at);
                CREATE INDEX IF NOT EXISTS idx_pending_opens_mode_created_at
                    ON pending_opens(mode, created_at);
                CREATE INDEX IF NOT EXISTS idx_events_ts
                    ON events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_events_level_ts
                    ON events(level, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_ig_rate_limit_workers_updated_at
                    ON ig_rate_limit_workers(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_ticks_ts
                    ON broker_ticks(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_tick_history_symbol_ts
                    ON broker_tick_history(symbol, ts DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_tick_history_ts
                    ON broker_tick_history(ts DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_ts
                    ON broker_symbol_specs(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_symbol_ts
                    ON broker_symbol_specs(symbol, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_symbol_pip_sizes_ts
                    ON broker_symbol_pip_sizes(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_broker_account_cache_ts
                    ON broker_account_cache(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_position_updates_ts
                    ON position_updates(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_candle_history_symbol_resolution_ts
                    ON candle_history(symbol, resolution_sec, bucket_start_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_position_updates_position_ts
                    ON position_updates(position_id, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_position_updates_symbol_ts
                    ON position_updates(symbol, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_position_updates_deal_reference_ts
                    ON position_updates(deal_reference, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_performance_symbol_closed
                    ON trade_performance(symbol, closed_at DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_performance_updated_at
                    ON trade_performance(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_virtual_positions_current_symbol
                    ON virtual_positions_current(symbol);
                CREATE INDEX IF NOT EXISTS idx_virtual_positions_ledger_symbol_ts
                    ON virtual_positions_ledger(symbol, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_virtual_positions_ledger_strategy_ts
                    ON virtual_positions_ledger(strategy_id, ts DESC);
                CREATE INDEX IF NOT EXISTS idx_real_positions_cache_updated_at
                    ON real_positions_cache(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_system_errors_updated_at
                    ON system_errors(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_pending_opens_symbol
                    ON pending_opens(symbol, mode);
                CREATE INDEX IF NOT EXISTS idx_trades_symbol
                    ON trades(symbol, status, opened_at DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_performance_symbol_closed_at
                    ON trade_performance(symbol, closed_at DESC, close_reason);
                """
            )
            self._apply_schema_migrations(cur)
            cur.execute("UPDATE candle_history SET open = close WHERE open IS NULL")
            cur.execute("UPDATE candle_history SET high = close WHERE high IS NULL")
            cur.execute("UPDATE candle_history SET low = close WHERE low IS NULL")
            cur.execute(
                """
                UPDATE trades
                SET strategy_entry = strategy
                WHERE strategy_entry IS NULL OR TRIM(strategy_entry) = ''
                """
            )
            cur.execute(
                """
                UPDATE pending_opens
                SET strategy_entry = strategy
                WHERE strategy_entry IS NULL OR TRIM(strategy_entry) = ''
                """
            )
            self._conn.commit()

    def _apply_schema_migrations(self, cursor: sqlite3.Cursor) -> None:
        applied_versions = {
            int(row["version"])
            for row in cursor.execute("SELECT version FROM schema_migrations").fetchall()
        }
        migrations: tuple[tuple[int, str, Callable[[sqlite3.Cursor], None]], ...] = (
            (
                1,
                "broker symbol spec variant identity",
                self._migrate_schema_v1_broker_symbol_spec_variants,
            ),
            (
                2,
                "trade and worker strategy metadata",
                self._migrate_schema_v2_strategy_metadata,
            ),
            (
                3,
                "trade pnl estimation and candle ohl columns",
                self._migrate_schema_v3_trade_and_candle_columns,
            ),
            (
                4,
                "trade performance slippage columns",
                self._migrate_schema_v4_trade_performance_columns,
            ),
            (
                5,
                "broker tick history retention",
                self._migrate_schema_v5_broker_tick_history,
            ),
        )
        for version, description, migration in migrations:
            if version in applied_versions:
                continue
            try:
                migration(cursor)
            except Exception:
                logger.exception(
                    "schema migration v%d (%s) failed — database may be in an inconsistent state",
                    version, description,
                )
                raise
            cursor.execute(
                """
                INSERT OR REPLACE INTO schema_migrations(version, description, applied_at)
                VALUES (?, ?, ?)
                """,
                (int(version), str(description), time.time()),
            )
            logger.info("schema migration v%d applied: %s", version, description)
        cursor.execute(f"PRAGMA user_version={int(_LATEST_STATE_STORE_SCHEMA_VERSION)}")

    def _migrate_schema_v1_broker_symbol_spec_variants(self, cursor: sqlite3.Cursor) -> None:
        self._ensure_broker_symbol_specs_variant_schema(cursor)
        self._ensure_column_exists(cursor, "broker_symbol_specs", "epic", "TEXT")
        self._ensure_column_exists(cursor, "broker_symbol_specs", "epic_variant", "TEXT")
        self._ensure_broker_symbol_spec_indexes(cursor)

    def _migrate_schema_v2_strategy_metadata(self, cursor: sqlite3.Cursor) -> None:
        self._ensure_column_exists(cursor, "trades", "deal_reference", "TEXT")
        self._ensure_column_exists(cursor, "trades", "epic", "TEXT")
        self._ensure_column_exists(cursor, "trades", "epic_variant", "TEXT")
        self._ensure_column_exists(cursor, "trades", "entry_confidence", "REAL NOT NULL DEFAULT 0.0")
        self._ensure_column_exists(cursor, "trades", "strategy_entry", "TEXT")
        self._ensure_column_exists(cursor, "trades", "strategy_entry_component", "TEXT")
        self._ensure_column_exists(cursor, "trades", "strategy_entry_signal", "TEXT")
        self._ensure_column_exists(cursor, "trades", "strategy_exit", "TEXT")
        self._ensure_column_exists(cursor, "trades", "strategy_exit_component", "TEXT")
        self._ensure_column_exists(cursor, "trades", "strategy_exit_signal", "TEXT")
        self._ensure_column_exists(cursor, "worker_states", "strategy_base", "TEXT")
        self._ensure_column_exists(cursor, "worker_states", "position_strategy_entry", "TEXT")
        self._ensure_column_exists(cursor, "worker_states", "position_strategy_component", "TEXT")
        self._ensure_column_exists(cursor, "worker_states", "position_strategy_signal", "TEXT")
        self._ensure_column_exists(cursor, "pending_opens", "strategy_entry", "TEXT")
        self._ensure_column_exists(cursor, "pending_opens", "strategy_entry_component", "TEXT")
        self._ensure_column_exists(cursor, "pending_opens", "strategy_entry_signal", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "strategy", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "strategy_base", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "strategy_entry", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "strategy_entry_component", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "strategy_entry_signal", "TEXT")
        self._ensure_column_exists(cursor, "position_updates", "mode", "TEXT")

    def _migrate_schema_v3_trade_and_candle_columns(self, cursor: sqlite3.Cursor) -> None:
        self._ensure_column_exists(cursor, "trades", "pnl_is_estimated", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column_exists(cursor, "trades", "pnl_estimate_source", "TEXT")
        self._ensure_column_exists(cursor, "candle_history", "open", "REAL")
        self._ensure_column_exists(cursor, "candle_history", "high", "REAL")
        self._ensure_column_exists(cursor, "candle_history", "low", "REAL")

    def _migrate_schema_v4_trade_performance_columns(self, cursor: sqlite3.Cursor) -> None:
        self._ensure_column_exists(cursor, "trade_performance", "entry_reference_price", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "entry_fill_price", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "entry_slippage_pips", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "entry_adverse_slippage_pips", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "exit_reference_price", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "exit_fill_price", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "exit_slippage_pips", "REAL")
        self._ensure_column_exists(cursor, "trade_performance", "exit_adverse_slippage_pips", "REAL")

    def _migrate_schema_v5_broker_tick_history(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_tick_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                mid REAL NOT NULL,
                ts REAL NOT NULL,
                volume REAL,
                source TEXT,
                updated_at REAL NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_tick_history_symbol_ts
                ON broker_tick_history(symbol, ts DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_tick_history_ts
                ON broker_tick_history(ts DESC, id DESC)
            """
        )

    def _check_integrity_and_disable_async_writers_if_needed(self) -> None:
        result = self._run_quick_check()
        if result.lower() == "ok":
            self._db_integrity_ok = True
            self._db_integrity_error = None
            return
        self._db_integrity_ok = False
        self._db_integrity_error = result
        logger.error(
            "State DB integrity check failed; quarantining DB and disabling async writers: %s",
            self._db_integrity_error,
        )
        self._quarantine_corrupt_db_and_reinitialize()

    def _run_quick_check(self) -> str:
        with self._lock:
            try:
                row = self._conn.execute("PRAGMA quick_check(1)").fetchone()
            except sqlite3.DatabaseError as exc:
                return str(exc)
        return str(row[0]).strip() if row and row[0] is not None else "unknown"

    def _quarantine_corrupt_db_and_reinitialize(self) -> None:
        integrity_error = self._db_integrity_error or "unknown"
        event_writer = self._event_async_writer
        multi_writer = self._multi_async_writer
        pending_event_writes = event_writer.queue_size if event_writer is not None else 0
        pending_multi_writes = multi_writer.queue_size if multi_writer is not None else 0
        self._event_async_writer = None
        self._multi_async_writer = None
        self._event_async_writer_enabled = False
        self._multi_async_writer_enabled = False
        if pending_event_writes > 0 or pending_multi_writes > 0:
            logger.error(
                "State DB quarantine is discarding pending async writes | events=%d multi=%d error=%s",
                pending_event_writes,
                pending_multi_writes,
                integrity_error,
            )
        for writer_name, writer in (
            ("event", event_writer),
            ("multi", multi_writer),
        ):
            if writer is None:
                continue
            try:
                writer.close(flush_timeout_sec=1.0, join_timeout_sec=1.0)
            except Exception:
                logger.debug("Failed to close %s async writer during DB quarantine", writer_name, exc_info=True)
        with self._lock:
            self._conn.close()
            quarantine_path = self._quarantine_db_files(integrity_error)
            self._conn = self._open_connection()
            self._init_schema()
            self._db_recovered_from_corruption = True
            self._db_integrity_ok = True
            self._db_integrity_error = None
        self.record_event(
            "ERROR",
            None,
            "State DB quarantined and reinitialized",
            {
                "quarantine_path": str(quarantine_path),
                "integrity_error": integrity_error,
            },
        )

    @staticmethod
    def _ensure_column_exists(
        cursor: sqlite3.Cursor,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        quoted_table = _quote_sqlite_identifier(table_name)
        quoted_column = _quote_sqlite_identifier(column_name)
        rows = cursor.execute(f"PRAGMA table_info({quoted_table})").fetchall()
        existing = {str(row["name"]) for row in rows}
        if column_name in existing:
            return
        cursor.execute(f"ALTER TABLE {quoted_table} ADD COLUMN {quoted_column} {column_sql}")

    @staticmethod
    def _parse_broker_symbol_spec_metadata(
        metadata_raw: object | None,
    ) -> dict[str, Any]:
        if not metadata_raw:
            return {}
        try:
            parsed = json.loads(str(metadata_raw))
        except json.JSONDecodeError:
            return {}
        if not isinstance(parsed, dict):
            return {}
        return dict(parsed)

    @classmethod
    def _extract_broker_symbol_spec_identity(
        cls,
        *,
        symbol: object | None,
        metadata_raw: object | None,
        epic: object | None = None,
        epic_variant: object | None = None,
    ) -> tuple[str, str]:
        metadata = cls._parse_broker_symbol_spec_metadata(metadata_raw)
        normalized_epic = _normalize_broker_symbol_spec_epic(
            epic if epic is not None else metadata.get("epic"),
        )
        normalized_variant = _normalize_broker_symbol_spec_epic_variant(
            epic_variant if epic_variant is not None else metadata.get("epic_variant"),
            epic=normalized_epic,
        )
        if normalized_epic:
            metadata["epic"] = normalized_epic
        if normalized_variant:
            metadata["epic_variant"] = normalized_variant
        return normalized_epic, normalized_variant

    def _ensure_broker_symbol_specs_variant_schema(self, cursor: sqlite3.Cursor) -> None:
        rows = cursor.execute("PRAGMA table_info(broker_symbol_specs)").fetchall()
        if not rows:
            return
        columns = {str(row["name"]): row for row in rows}
        if "cache_key" in columns and "symbol" in columns:
            return

        legacy_rows = cursor.execute("SELECT * FROM broker_symbol_specs").fetchall()
        cursor.execute("DROP TABLE IF EXISTS broker_symbol_specs_v2")
        cursor.execute(
            """
            CREATE TABLE broker_symbol_specs_v2 (
                cache_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                epic TEXT,
                epic_variant TEXT,
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
        for row in legacy_rows:
            normalized_symbol = str(row["symbol"] or "").strip().upper()
            if not normalized_symbol:
                continue
            normalized_epic, normalized_variant = self._extract_broker_symbol_spec_identity(
                symbol=normalized_symbol,
                metadata_raw=row["metadata_json"],
            )
            cache_key = _broker_symbol_spec_cache_key(normalized_symbol, normalized_epic)
            cursor.execute(
                """
                INSERT OR REPLACE INTO broker_symbol_specs_v2(
                    cache_key, symbol, epic, epic_variant, tick_size, tick_value, contract_size,
                    lot_min, lot_max, lot_step, min_stop_distance_price, one_pip_means,
                    metadata_json, source, ts, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cache_key,
                    normalized_symbol,
                    normalized_epic or None,
                    normalized_variant or None,
                    row["tick_size"],
                    row["tick_value"],
                    row["contract_size"],
                    row["lot_min"],
                    row["lot_max"],
                    row["lot_step"],
                    row["min_stop_distance_price"],
                    row["one_pip_means"],
                    row["metadata_json"],
                    row["source"],
                    row["ts"],
                    row["updated_at"],
                ),
            )
        cursor.execute("DROP TABLE broker_symbol_specs")
        cursor.execute("ALTER TABLE broker_symbol_specs_v2 RENAME TO broker_symbol_specs")

    @staticmethod
    def _ensure_broker_symbol_spec_indexes(cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_ts
                ON broker_symbol_specs(ts DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_symbol_ts
                ON broker_symbol_specs(symbol, ts DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_symbol_epic_ts
                ON broker_symbol_specs(symbol, epic, ts DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_broker_symbol_specs_symbol_variant_ts
                ON broker_symbol_specs(symbol, epic_variant, ts DESC)
            """
        )

    def _start_multi_async_writer(self) -> None:
        if self._multi_async_writer is not None:
            return
        self._multi_async_writer = _AsyncBatchWriter(
            name="state-store-multi-writer",
            flush_interval_sec=self._multi_async_flush_interval_sec,
            batch_size=self._multi_async_batch_size,
            apply_batch=lambda batch: self._apply_multi_async_batch(batch),
            on_failure=self._mark_multi_async_failure,
        )
        self._multi_async_writer.start()

    def _apply_multi_async_batch(self, batch: list[tuple[str, tuple[Any, ...]]]) -> None:
        if not batch:
            return
        with self._lock:
            with self._conn:
                for op, payload in batch:
                    if op == "virtual_position":
                        self._apply_virtual_position_write_in_txn(*payload)
                        continue
                    if op == "real_position":
                        self._apply_real_position_write_in_txn(*payload)
                        continue
                    if op == "system_error":
                        self._apply_system_error_write_in_txn(*payload)
                        continue
        self._apply_hot_multi_batch(batch)

    def _mark_multi_async_failure(self, exc: Exception) -> None:
        with self._lock:
            self._multi_async_failure_count += 1
            self._multi_async_last_error = f"{type(exc).__name__}: {exc}"

    def _apply_hot_multi_batch(self, batch: list[tuple[str, tuple[Any, ...]]]) -> None:
        with self._hot_lock:
            for op, payload in batch:
                if op == "virtual_position":
                    self._apply_hot_virtual_position_state(payload)
                    continue
                if op == "real_position":
                    self._apply_hot_real_position_cache(payload)
                    continue
                if op == "system_error":
                    self._apply_hot_system_error_qty(payload)

    def _apply_hot_virtual_position_state(self, payload: tuple[Any, ...]) -> None:
        _, _, symbol, strategy_id, qty_lots, source, reason, updated_at = payload
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_strategy = str(strategy_id or "").strip().lower()
        if abs(float(qty_lots)) <= FLOAT_ROUNDING_TOLERANCE:
            self._hot_virtual_positions.pop((normalized_symbol, normalized_strategy), None)
            return
        self._hot_virtual_positions[(normalized_symbol, normalized_strategy)] = {
            "symbol": normalized_symbol,
            "strategy_id": normalized_strategy,
            "qty_lots": float(qty_lots),
            "source": source,
            "reason": reason,
            "updated_at": float(updated_at),
        }

    def _apply_hot_real_position_cache(self, payload: tuple[Any, ...]) -> None:
        symbol, real_qty_lots, broker_ts, source, updated_at = payload
        normalized_symbol = str(symbol or "").strip().upper()
        self._hot_real_positions[normalized_symbol] = {
            "symbol": normalized_symbol,
            "real_qty_lots": float(real_qty_lots),
            "broker_ts": (float(broker_ts) if broker_ts is not None else None),
            "source": source,
            "updated_at": float(updated_at),
        }

    def _apply_hot_system_error_qty(self, payload: tuple[Any, ...]) -> None:
        symbol, err_qty_lots, reason, updated_at = payload
        normalized_symbol = str(symbol or "").strip().upper()
        if abs(float(err_qty_lots)) <= FLOAT_ROUNDING_TOLERANCE:
            self._hot_system_errors.pop(normalized_symbol, None)
            return
        self._hot_system_errors[normalized_symbol] = {
            "symbol": normalized_symbol,
            "err_qty_lots": float(err_qty_lots),
            "reason": reason,
            "updated_at": float(updated_at),
        }

    def _load_hot_multi_state(self) -> None:
        with self._lock:
            vp_rows = self._conn.execute(
                "SELECT symbol, strategy_id, qty_lots, source, reason, updated_at FROM virtual_positions_current"
            ).fetchall()
            rp_rows = self._conn.execute(
                "SELECT symbol, real_qty_lots, broker_ts, source, updated_at FROM real_positions_cache"
            ).fetchall()
            se_rows = self._conn.execute(
                "SELECT symbol, err_qty_lots, reason, updated_at FROM system_errors"
            ).fetchall()

        self._hot_virtual_positions = {
            (str(row["symbol"]), str(row["strategy_id"])): {
                "symbol": str(row["symbol"]),
                "strategy_id": str(row["strategy_id"]),
                "qty_lots": float(row["qty_lots"]),
                "source": row["source"],
                "reason": row["reason"],
                "updated_at": float(row["updated_at"]),
            }
            for row in vp_rows
        }
        self._hot_real_positions = {
            str(row["symbol"]): {
                "symbol": str(row["symbol"]),
                "real_qty_lots": float(row["real_qty_lots"]),
                "broker_ts": self._parse_float(row["broker_ts"], 0.0) if row["broker_ts"] is not None else None,
                "source": row["source"],
                "updated_at": float(row["updated_at"]),
            }
            for row in rp_rows
        }
        self._hot_system_errors = {
            str(row["symbol"]): {
                "symbol": str(row["symbol"]),
                "err_qty_lots": float(row["err_qty_lots"]),
                "reason": row["reason"],
                "updated_at": float(row["updated_at"]),
            }
            for row in se_rows
        }

    def flush_multi_async_writes(self, timeout_sec: float = 2.0) -> bool:
        if (not self._multi_async_writer_enabled) or self._closed:
            return self._multi_async_failure_count == 0
        writer = self._multi_async_writer
        if writer is None:
            return self._multi_async_failure_count == 0
        flushed = writer.flush(timeout_sec=timeout_sec)
        if not flushed:
            return False
        return self._multi_async_failure_count == 0

    def _start_event_async_writer(self) -> None:
        if self._event_async_writer is not None:
            return
        self._event_async_writer = _AsyncBatchWriter(
            name="state-store-event-writer",
            flush_interval_sec=self._event_async_flush_interval_sec,
            batch_size=self._event_async_batch_size,
            apply_batch=lambda batch: self._apply_event_async_batch(batch),
        )
        self._event_async_writer.start()

    def _apply_event_async_batch(self, batch: list[tuple[Any, ...]]) -> None:
        if not batch:
            return
        with self._lock:
            with self._conn:
                self._conn.executemany(
                    "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
                    batch,
                )

    def flush_event_async_writes(self, timeout_sec: float = 2.0) -> bool:
        if (not self._event_async_writer_enabled) or self._closed:
            return True
        writer = self._event_async_writer
        if writer is None:
            return True
        return writer.flush(timeout_sec=timeout_sec)

    def _enqueue_multi_async_write(self, op: str, payload: tuple[Any, ...]) -> bool:
        with self._lock:
            if (not self._multi_async_writer_enabled) or self._closed:
                return False
            writer = self._multi_async_writer
        if writer is not None and writer.enqueue((op, payload)):
            return True
        with self._lock:
            self._multi_async_writer_enabled = False
        self._mark_multi_async_failure(RuntimeError("multi async writer unavailable"))
        logger.error(
            "state_store: multi async writer unavailable, falling back to synchronous writes%s",
            f" (last_error={writer.last_error})" if writer is not None and writer.last_error else "",
        )
        return False

    def _enqueue_event_async_write(self, payload: tuple[Any, ...]) -> bool:
        with self._lock:
            if (not self._event_async_writer_enabled) or self._closed:
                return False
            writer = self._event_async_writer
        if writer is not None and writer.enqueue(payload):
            return True
        with self._lock:
            self._event_async_writer_enabled = False
        logger.error(
            "state_store: event async writer unavailable, falling back to synchronous writes%s",
            f" (last_error={writer.last_error})" if writer is not None and writer.last_error else "",
        )
        return False

    @staticmethod
    def _parse_float(raw: Any, fallback: float) -> float:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return fallback
        return value

    def _run_write_tx(self, operation: Callable[[], _T]) -> _T:
        with self._lock:
            with self._conn:
                return operation()

    def _run_write_tx_immediate(self, operation: Callable[[], _T]) -> _T:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = operation()
                self._conn.execute("COMMIT")
                return result
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def _apply_virtual_position_write_in_txn(
        self,
        event_id: str,
        ts: float,
        symbol: str,
        strategy_id: str,
        qty_lots: float,
        source: str | None,
        reason: str | None,
        updated_at: float,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO virtual_positions_ledger(
                event_id, ts, symbol, strategy_id, qty_lots, source, reason, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event_id),
                float(ts),
                str(symbol),
                str(strategy_id),
                float(qty_lots),
                source,
                reason,
                float(updated_at),
            ),
        )
        if abs(float(qty_lots)) <= FLOAT_ROUNDING_TOLERANCE:
            self._conn.execute(
                "DELETE FROM virtual_positions_current WHERE symbol = ? AND strategy_id = ?",
                (str(symbol), str(strategy_id)),
            )
            return
        self._conn.execute(
            """
            INSERT INTO virtual_positions_current(
                symbol, strategy_id, qty_lots, source, reason, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, strategy_id) DO UPDATE SET
                qty_lots=excluded.qty_lots,
                source=excluded.source,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (
                str(symbol),
                str(strategy_id),
                float(qty_lots),
                source,
                reason,
                float(updated_at),
            ),
        )

    def _apply_real_position_write_in_txn(
        self,
        symbol: str,
        real_qty_lots: float,
        broker_ts: float | None,
        source: str | None,
        updated_at: float,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO real_positions_cache(symbol, real_qty_lots, broker_ts, source, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                real_qty_lots=excluded.real_qty_lots,
                broker_ts=excluded.broker_ts,
                source=excluded.source,
                updated_at=excluded.updated_at
            """,
            (
                str(symbol),
                float(real_qty_lots),
                (float(broker_ts) if broker_ts is not None else None),
                source,
                float(updated_at),
            ),
        )

    def _apply_system_error_write_in_txn(
        self,
        symbol: str,
        err_qty_lots: float,
        reason: str | None,
        updated_at: float,
    ) -> None:
        if abs(float(err_qty_lots)) <= FLOAT_ROUNDING_TOLERANCE:
            self._conn.execute(
                "DELETE FROM system_errors WHERE symbol = ?",
                (str(symbol),),
            )
            return
        self._conn.execute(
            """
            INSERT INTO system_errors(symbol, err_qty_lots, reason, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                err_qty_lots=excluded.err_qty_lots,
                reason=excluded.reason,
                updated_at=excluded.updated_at
            """,
            (
                str(symbol),
                float(err_qty_lots),
                reason,
                float(updated_at),
            ),
        )

    def upsert_virtual_position_state(
        self,
        *,
        symbol: str,
        strategy_id: str,
        qty_lots: float,
        source: str | None = "aggregator",
        reason: str | None = None,
        ts: float | None = None,
    ) -> str:
        event_ts = time.time() if ts is None else float(ts)
        updated_at = time.time()
        event_id = _uuid7_hex(int(event_ts * 1000.0))
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_strategy = str(strategy_id or "").strip().lower()
        payload = (
            event_id,
            event_ts,
            normalized_symbol,
            normalized_strategy,
            float(qty_lots),
            source,
            reason,
            updated_at,
        )
        if self._enqueue_multi_async_write("virtual_position", payload):
            with self._hot_lock:
                self._apply_hot_virtual_position_state(payload)
            return event_id

        def _write() -> None:
            self._apply_virtual_position_write_in_txn(*payload)
        self._run_write_tx(_write)
        with self._hot_lock:
            self._apply_hot_virtual_position_state(payload)
        return event_id

    def upsert_real_position_cache(
        self,
        *,
        symbol: str,
        real_qty_lots: float,
        broker_ts: float | None = None,
        source: str | None = "executor",
    ) -> None:
        updated_at = time.time()
        normalized_symbol = str(symbol or "").strip().upper()
        payload = (
            normalized_symbol,
            float(real_qty_lots),
            (float(broker_ts) if broker_ts is not None else None),
            source,
            updated_at,
        )
        if self._enqueue_multi_async_write("real_position", payload):
            with self._hot_lock:
                self._apply_hot_real_position_cache(payload)
            return

        def _write() -> None:
            self._apply_real_position_write_in_txn(*payload)
        self._run_write_tx(_write)
        with self._hot_lock:
            self._apply_hot_real_position_cache(payload)

    def upsert_system_error_qty(
        self,
        *,
        symbol: str,
        err_qty_lots: float,
        reason: str | None = None,
    ) -> None:
        updated_at = time.time()
        normalized_symbol = str(symbol or "").strip().upper()
        payload = (normalized_symbol, float(err_qty_lots), reason, updated_at)
        if self._enqueue_multi_async_write("system_error", payload):
            with self._hot_lock:
                self._apply_hot_system_error_qty(payload)
            return

        def _write() -> None:
            self._apply_system_error_write_in_txn(*payload)
        self._run_write_tx(_write)
        with self._hot_lock:
            self._apply_hot_system_error_qty(payload)

    def load_virtual_positions_state(self, symbol: str | None = None) -> list[dict[str, Any]]:
        self.flush_multi_async_writes()
        normalized_symbol = str(symbol or "").strip().upper() if symbol is not None else None
        with self._lock:
            if normalized_symbol:
                rows = self._conn.execute(
                    """
                    SELECT symbol, strategy_id, qty_lots, source, reason, updated_at
                    FROM virtual_positions_current
                    WHERE symbol = ?
                    ORDER BY strategy_id ASC
                    """,
                    (normalized_symbol,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT symbol, strategy_id, qty_lots, source, reason, updated_at
                    FROM virtual_positions_current
                    ORDER BY symbol ASC, strategy_id ASC
                    """
                ).fetchall()
        return [dict(row) for row in rows]

    def load_virtual_positions_ledger(self, limit: int = 1000) -> list[dict[str, Any]]:
        self.flush_multi_async_writes()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT event_id, ts, symbol, strategy_id, qty_lots, source, reason, updated_at
                FROM virtual_positions_ledger
                ORDER BY ts DESC, event_id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [dict(row) for row in rows]

    def load_real_positions_cache(self) -> list[dict[str, Any]]:
        self.flush_multi_async_writes()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, real_qty_lots, broker_ts, source, updated_at
                FROM real_positions_cache
                ORDER BY symbol ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def load_system_errors(self) -> list[dict[str, Any]]:
        self.flush_multi_async_writes()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, err_qty_lots, reason, updated_at
                FROM system_errors
                ORDER BY symbol ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def load_multi_position_reconciliation(self, epsilon_lots: float = 1e-6) -> list[dict[str, Any]]:
        self.flush_multi_async_writes()
        epsilon = max(0.0, float(epsilon_lots))
        with self._lock:
            rows = self._conn.execute(
                """
                WITH virtual_sum AS (
                    SELECT symbol, SUM(qty_lots) AS sum_virtual
                    FROM virtual_positions_current
                    GROUP BY symbol
                ),
                symbols AS (
                    SELECT symbol FROM virtual_sum
                    UNION
                    SELECT symbol FROM real_positions_cache
                    UNION
                    SELECT symbol FROM system_errors
                )
                SELECT
                    symbols.symbol AS symbol,
                    COALESCE(virtual_sum.sum_virtual, 0.0) AS sum_virtual,
                    COALESCE(real_positions_cache.real_qty_lots, 0.0) AS real_qty,
                    COALESCE(system_errors.err_qty_lots, 0.0) AS err_qty,
                    (
                        COALESCE(real_positions_cache.real_qty_lots, 0.0)
                        - COALESCE(virtual_sum.sum_virtual, 0.0)
                        - COALESCE(system_errors.err_qty_lots, 0.0)
                    ) AS diff
                FROM symbols
                LEFT JOIN virtual_sum ON virtual_sum.symbol = symbols.symbol
                LEFT JOIN real_positions_cache ON real_positions_cache.symbol = symbols.symbol
                LEFT JOIN system_errors ON system_errors.symbol = symbols.symbol
                WHERE ABS(
                    COALESCE(real_positions_cache.real_qty_lots, 0.0)
                    - COALESCE(virtual_sum.sum_virtual, 0.0)
                    - COALESCE(system_errors.err_qty_lots, 0.0)
                ) > ?
                ORDER BY ABS(diff) DESC, symbols.symbol ASC
                """,
                (epsilon,),
            ).fetchall()
        return [dict(row) for row in rows]

    def load_hot_multi_positions_snapshot(self) -> dict[str, Any]:
        self.flush_multi_async_writes()
        with self._hot_lock:
            return {
                "virtual_positions": dict(self._hot_virtual_positions),
                "real_positions": dict(self._hot_real_positions),
                "system_errors": dict(self._hot_system_errors),
            }

    def _upsert_kv_in_txn(self, key: str, value: str) -> None:
        self._conn.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, time.time()),
        )

    def save_worker_state(self, state: WorkerState) -> None:
        payload = state.to_dict()
        now = time.time()
        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO worker_states(
                    symbol, thread_name, mode, strategy, strategy_base,
                    position_strategy_entry, position_strategy_component, position_strategy_signal,
                    last_price, last_heartbeat, iteration,
                    position_json, last_error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    thread_name=excluded.thread_name,
                    mode=excluded.mode,
                    strategy=excluded.strategy,
                    strategy_base=excluded.strategy_base,
                    position_strategy_entry=excluded.position_strategy_entry,
                    position_strategy_component=excluded.position_strategy_component,
                    position_strategy_signal=excluded.position_strategy_signal,
                    last_price=excluded.last_price,
                    last_heartbeat=excluded.last_heartbeat,
                    iteration=excluded.iteration,
                    position_json=excluded.position_json,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at
                """,
                (
                    payload["symbol"],
                    payload["thread_name"],
                    payload["mode"],
                    payload["strategy"],
                    payload.get("strategy_base"),
                    payload.get("position_strategy_entry"),
                    payload.get("position_strategy_component"),
                    payload.get("position_strategy_signal"),
                    payload["last_price"],
                    payload["last_heartbeat"],
                    payload["iteration"],
                    json.dumps(payload.get("position")) if payload.get("position") else None,
                    payload.get("last_error"),
                    now,
                ),
            )

        self._run_write_tx(_write)

    def load_worker_state(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM worker_states WHERE symbol = ?", (symbol,)
            ).fetchone()

        if row is None:
            return None

        result = dict(row)
        if result.get("position_json"):
            result["position"] = json.loads(result["position_json"])
        else:
            result["position"] = None
        return result

    @staticmethod
    def _normalize_strategy_label(value: object) -> str | None:
        text = str(value or "").strip().lower()
        return text or None

    @classmethod
    def _strategy_base_from_trade_row(cls, row_dict: dict[str, Any]) -> str | None:
        carrier = cls._normalize_strategy_label(row_dict.get("strategy"))
        entry = cls._normalize_strategy_label(row_dict.get("strategy_entry"))
        component = cls._normalize_strategy_label(row_dict.get("strategy_entry_component"))
        if carrier == "multi_strategy":
            return entry or carrier
        return carrier or entry or component

    @classmethod
    def _entry_component_from_trade_row(cls, row_dict: dict[str, Any]) -> str | None:
        component = cls._normalize_strategy_label(row_dict.get("strategy_entry_component"))
        if component:
            return component
        entry = cls._normalize_strategy_label(row_dict.get("strategy_entry"))
        if entry:
            return entry
        return cls._normalize_strategy_label(row_dict.get("strategy"))

    def upsert_trade(
        self,
        position: Position,
        thread_name: str,
        strategy: str,
        mode: str,
        *,
        strategy_entry: str | None = None,
        strategy_entry_component: str | None = None,
        strategy_entry_signal: str | None = None,
        strategy_exit: str | None = None,
        strategy_exit_component: str | None = None,
        strategy_exit_signal: str | None = None,
    ) -> None:
        base_strategy = self._normalize_strategy_label(strategy) or ""
        normalized_entry = (
            self._normalize_strategy_label(
                position.strategy_entry if strategy_entry is None else strategy_entry
            )
            if strategy_entry is not None
            else self._normalize_strategy_label(position.strategy_entry)
        )
        normalized_entry_component = (
            self._normalize_strategy_label(
                position.strategy_entry_component if strategy_entry_component is None else strategy_entry_component
            )
            if strategy_entry_component is not None
            else self._normalize_strategy_label(position.strategy_entry_component)
        )
        normalized_entry_signal = (
            self._normalize_strategy_label(
                position.strategy_entry_signal if strategy_entry_signal is None else strategy_entry_signal
            )
            if strategy_entry_signal is not None
            else self._normalize_strategy_label(position.strategy_entry_signal)
        )
        normalized_exit = (
            self._normalize_strategy_label(
                position.strategy_exit if strategy_exit is None else strategy_exit
            )
            if strategy_exit is not None
            else self._normalize_strategy_label(position.strategy_exit)
        )
        normalized_exit_component = (
            self._normalize_strategy_label(
                position.strategy_exit_component
                if strategy_exit_component is None
                else strategy_exit_component
            )
            if strategy_exit_component is not None
            else self._normalize_strategy_label(position.strategy_exit_component)
        )
        normalized_exit_signal = (
            self._normalize_strategy_label(
                position.strategy_exit_signal if strategy_exit_signal is None else strategy_exit_signal
            )
            if strategy_exit_signal is not None
            else self._normalize_strategy_label(position.strategy_exit_signal)
        )
        if normalized_entry == "":
            normalized_entry = None
        if normalized_entry_component == "":
            normalized_entry_component = None
        if normalized_entry_signal == "":
            normalized_entry_signal = None
        if normalized_exit == "":
            normalized_exit = None
        if normalized_exit_component == "":
            normalized_exit_component = None
        if normalized_exit_signal == "":
            normalized_exit_signal = None
        now = time.time()
        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO trades(
                    position_id, symbol, epic, epic_variant, side, volume, open_price, stop_loss,
                    take_profit, opened_at, entry_confidence, status, close_price, closed_at,
                    pnl, pnl_is_estimated, pnl_estimate_source, thread_name, strategy, strategy_entry,
                    strategy_entry_component, strategy_entry_signal, strategy_exit,
                    strategy_exit_component, strategy_exit_signal, mode, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    symbol=excluded.symbol,
                    epic=excluded.epic,
                    epic_variant=excluded.epic_variant,
                    side=excluded.side,
                    volume=excluded.volume,
                    open_price=excluded.open_price,
                    stop_loss=excluded.stop_loss,
                    take_profit=excluded.take_profit,
                    opened_at=excluded.opened_at,
                    entry_confidence=excluded.entry_confidence,
                    status=excluded.status,
                    close_price=excluded.close_price,
                    closed_at=excluded.closed_at,
                    pnl=excluded.pnl,
                    pnl_is_estimated=excluded.pnl_is_estimated,
                    pnl_estimate_source=excluded.pnl_estimate_source,
                    thread_name=excluded.thread_name,
                    strategy=excluded.strategy,
                    strategy_entry=excluded.strategy_entry,
                    strategy_entry_component=excluded.strategy_entry_component,
                    strategy_entry_signal=excluded.strategy_entry_signal,
                    strategy_exit=excluded.strategy_exit,
                    strategy_exit_component=excluded.strategy_exit_component,
                    strategy_exit_signal=excluded.strategy_exit_signal,
                    mode=excluded.mode,
                    updated_at=excluded.updated_at
                """,
                (
                    position.position_id,
                    position.symbol,
                    str(position.epic).strip().upper() if position.epic else None,
                    str(position.epic_variant).strip().lower() if position.epic_variant else None,
                    position.side.value,
                    position.volume,
                    position.open_price,
                    position.stop_loss,
                    position.take_profit,
                    position.opened_at,
                    position.entry_confidence,
                    position.status,
                    position.close_price,
                    position.closed_at,
                    position.pnl,
                    1 if position.pnl_is_estimated else 0,
                    str(position.pnl_estimate_source).strip() if position.pnl_estimate_source else None,
                    thread_name,
                    base_strategy,
                    normalized_entry,
                    normalized_entry_component,
                    normalized_entry_signal,
                    normalized_exit,
                    normalized_exit_component,
                    normalized_exit_signal,
                    mode,
                    now,
                ),
            )

        self._run_write_tx(_write)

    @staticmethod
    def _row_to_position(row_dict: dict[str, Any]) -> Position:
        strategy = StateStore._normalize_strategy_label(row_dict.get("strategy"))
        strategy_entry = StateStore._normalize_strategy_label(row_dict.get("strategy_entry"))
        strategy_entry_component = StateStore._entry_component_from_trade_row(row_dict)
        strategy_entry_signal = StateStore._normalize_strategy_label(row_dict.get("strategy_entry_signal"))
        strategy_exit = StateStore._normalize_strategy_label(row_dict.get("strategy_exit"))
        strategy_exit_component = StateStore._normalize_strategy_label(
            row_dict.get("strategy_exit_component")
        ) or strategy_exit
        strategy_exit_signal = StateStore._normalize_strategy_label(row_dict.get("strategy_exit_signal"))
        return Position(
            position_id=row_dict["position_id"],
            symbol=row_dict["symbol"],
            side=Side(row_dict["side"]),
            volume=row_dict["volume"],
            open_price=row_dict["open_price"],
            stop_loss=row_dict["stop_loss"],
            take_profit=row_dict["take_profit"],
            opened_at=row_dict["opened_at"],
            entry_confidence=float(row_dict.get("entry_confidence") or 0.0),
            status=row_dict["status"],
            close_price=row_dict["close_price"],
            closed_at=row_dict["closed_at"],
            pnl=row_dict["pnl"],
            pnl_is_estimated=bool(row_dict.get("pnl_is_estimated") or 0),
            pnl_estimate_source=str(row_dict.get("pnl_estimate_source") or "").strip() or None,
            epic=str(row_dict.get("epic") or "").strip().upper() or None,
            epic_variant=str(row_dict.get("epic_variant") or "").strip().lower() or None,
            strategy=strategy,
            strategy_base=StateStore._strategy_base_from_trade_row(row_dict),
            strategy_entry=strategy_entry or strategy,
            strategy_entry_component=strategy_entry_component,
            strategy_entry_signal=strategy_entry_signal,
            strategy_exit=strategy_exit,
            strategy_exit_component=strategy_exit_component,
            strategy_exit_signal=strategy_exit_signal,
        )

    def load_open_positions(self, mode: str | None = None) -> dict[str, Position]:
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status IN ('open', 'closing') ORDER BY opened_at ASC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT *
                    FROM trades
                    WHERE status IN ('open', 'closing') AND mode = ?
                    ORDER BY opened_at ASC
                    """,
                    (str(mode),),
                ).fetchall()

        positions: dict[str, Position] = {}
        for row in rows:
            row_dict = dict(row)
            position = self._row_to_position(row_dict)
            positions[str(position.position_id)] = position
        return positions

    def load_positions_by_status(self, status: str, mode: str | None = None) -> dict[str, Position]:
        normalized_status = str(status).strip()
        if not normalized_status:
            return {}
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status = ? ORDER BY opened_at ASC",
                    (normalized_status,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status = ? AND mode = ? ORDER BY opened_at ASC",
                    (normalized_status, str(mode)),
                ).fetchall()
        positions: dict[str, Position] = {}
        for row in rows:
            row_dict = dict(row)
            position = self._row_to_position(row_dict)
            positions[str(position.position_id)] = position
        return positions

    def has_trade_opened_between(
        self,
        *,
        symbol: str,
        strategy: str,
        start_ts: float,
        end_ts: float,
        mode: str | None = None,
    ) -> bool:
        normalized_symbol = str(symbol).strip().upper()
        normalized_strategy = str(strategy).strip().lower()
        if not normalized_symbol or not normalized_strategy:
            return False
        if not math.isfinite(float(start_ts)) or not math.isfinite(float(end_ts)):
            return False
        if float(end_ts) <= float(start_ts):
            return False

        with self._lock:
            if mode is None:
                row = self._conn.execute(
                    """
                    SELECT 1
                    FROM trades
                    WHERE symbol = ?
                      AND LOWER(
                        COALESCE(
                            NULLIF(strategy_entry_component, ''),
                            NULLIF(strategy_entry, ''),
                            strategy
                        )
                      ) = ?
                      AND opened_at >= ?
                      AND opened_at < ?
                    LIMIT 1
                    """,
                    (normalized_symbol, normalized_strategy, float(start_ts), float(end_ts)),
                ).fetchone()
            else:
                row = self._conn.execute(
                    """
                    SELECT 1
                    FROM trades
                    WHERE symbol = ?
                      AND LOWER(
                        COALESCE(
                            NULLIF(strategy_entry_component, ''),
                            NULLIF(strategy_entry, ''),
                            strategy
                        )
                      ) = ?
                      AND mode = ?
                      AND opened_at >= ?
                      AND opened_at < ?
                    LIMIT 1
                    """,
                    (
                        normalized_symbol,
                        normalized_strategy,
                        str(mode),
                        float(start_ts),
                        float(end_ts),
                    ),
                ).fetchone()
        return row is not None

    def get_trade_record(self, position_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM trades WHERE position_id = ?",
                (str(position_id),),
            ).fetchone()
        if row is None:
            return None
        return dict(row)

    def bind_trade_deal_reference(self, position_id: str, deal_reference: str) -> str | None:
        normalized_position_id = str(position_id).strip()
        normalized_reference = str(deal_reference).strip()
        if not normalized_position_id or not normalized_reference:
            return None

        def _write() -> str | None:
            conflicting = self._conn.execute(
                """
                SELECT position_id
                FROM trades
                WHERE deal_reference = ?
                  AND position_id <> ?
                LIMIT 1
                """,
                (normalized_reference, normalized_position_id),
            ).fetchone()
            if conflicting is not None:
                return str(conflicting["position_id"])

            self._conn.execute(
                """
                UPDATE trades
                SET deal_reference = ?, updated_at = ?
                WHERE position_id = ?
                """,
                (normalized_reference, time.time(), normalized_position_id),
            )
            return None

        return self._run_write_tx(_write)

    def get_trade_deal_reference(self, position_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT deal_reference FROM trades WHERE position_id = ?",
                (str(position_id),),
            ).fetchone()
        if row is None:
            return None
        raw = row["deal_reference"]
        if raw in (None, ""):
            return None
        value = str(raw).strip()
        return value or None

    def load_open_trade_deal_references(self, mode: str | None = None) -> list[str]:
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    """
                    SELECT deal_reference
                    FROM trades
                    WHERE status = 'open' AND deal_reference IS NOT NULL AND deal_reference <> ''
                    ORDER BY opened_at ASC
                    """
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT deal_reference
                    FROM trades
                    WHERE status = 'open' AND mode = ? AND deal_reference IS NOT NULL AND deal_reference <> ''
                    ORDER BY opened_at ASC
                    """,
                    (str(mode),),
                ).fetchall()
        result: list[str] = []
        for row in rows:
            raw = row["deal_reference"]
            if raw in (None, ""):
                continue
            value = str(raw).strip()
            if value:
                result.append(value)
        return result

    def upsert_pending_open(self, pending: PendingOpen) -> None:
        now = time.time()
        trailing_override_json = (
            json.dumps(pending.trailing_override) if pending.trailing_override is not None else None
        )
        strategy_entry = str(pending.strategy_entry or "").strip().lower() or None
        strategy_entry_component = str(pending.strategy_entry_component or "").strip().lower() or None
        strategy_entry_signal = str(pending.strategy_entry_signal or "").strip().lower() or None
        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO pending_opens(
                    pending_id, position_id, symbol, side, volume, entry,
                    stop_loss, take_profit, created_at, thread_name, strategy,
                    strategy_entry, strategy_entry_component, strategy_entry_signal,
                    mode, entry_confidence, trailing_override_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pending_id) DO UPDATE SET
                    position_id=excluded.position_id,
                    symbol=excluded.symbol,
                    side=excluded.side,
                    volume=excluded.volume,
                    entry=excluded.entry,
                    stop_loss=excluded.stop_loss,
                    take_profit=excluded.take_profit,
                    created_at=excluded.created_at,
                    thread_name=excluded.thread_name,
                    strategy=excluded.strategy,
                    strategy_entry=COALESCE(excluded.strategy_entry, pending_opens.strategy_entry),
                    strategy_entry_component=COALESCE(
                        excluded.strategy_entry_component,
                        pending_opens.strategy_entry_component
                    ),
                    strategy_entry_signal=COALESCE(
                        excluded.strategy_entry_signal,
                        pending_opens.strategy_entry_signal
                    ),
                    mode=excluded.mode,
                    entry_confidence=excluded.entry_confidence,
                    trailing_override_json=excluded.trailing_override_json,
                    updated_at=excluded.updated_at
                """,
                (
                    pending.pending_id,
                    pending.position_id,
                    pending.symbol,
                    pending.side.value,
                    pending.volume,
                    pending.entry,
                    pending.stop_loss,
                    pending.take_profit,
                    pending.created_at,
                    pending.thread_name,
                    pending.strategy,
                    strategy_entry,
                    strategy_entry_component,
                    strategy_entry_signal,
                    pending.mode,
                    pending.entry_confidence,
                    trailing_override_json,
                    now,
                ),
            )

        self._run_write_tx(_write)

    def update_pending_open_position_id(self, pending_id: str, position_id: str) -> None:
        def _write() -> None:
            self._conn.execute(
                """
                UPDATE pending_opens
                SET position_id = ?, updated_at = ?
                WHERE pending_id = ?
                """,
                (str(position_id), time.time(), str(pending_id)),
            )

        self._run_write_tx(_write)

    def load_pending_opens(self, mode: str | None = None) -> list[PendingOpen]:
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    "SELECT * FROM pending_opens ORDER BY created_at ASC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM pending_opens WHERE mode = ? ORDER BY created_at ASC",
                    (str(mode),),
                ).fetchall()

        result: list[PendingOpen] = []
        for row in rows:
            row_dict = dict(row)
            trailing_override: dict[str, float | str] | None = None
            raw_override = row_dict.get("trailing_override_json")
            if raw_override:
                try:
                    payload = json.loads(raw_override)
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    normalized: dict[str, float | str] = {}
                    for key, value in payload.items():
                        normalized_key = str(key)
                        try:
                            normalized[normalized_key] = float(value)
                            continue
                        except (TypeError, ValueError):
                            pass
                        if isinstance(value, str) and value.strip():
                            normalized[normalized_key] = value.strip()
                    trailing_override = normalized or None
            result.append(
                PendingOpen(
                    pending_id=str(row_dict["pending_id"]),
                    position_id=str(row_dict["position_id"]) if row_dict.get("position_id") else None,
                    symbol=str(row_dict["symbol"]),
                    side=Side(str(row_dict["side"])),
                    volume=float(row_dict["volume"]),
                    entry=float(row_dict["entry"]),
                    stop_loss=float(row_dict["stop_loss"]),
                    take_profit=float(row_dict["take_profit"]),
                    created_at=float(row_dict["created_at"]),
                    thread_name=str(row_dict["thread_name"]),
                    strategy=str(row_dict["strategy"]),
                    strategy_entry=str(row_dict["strategy_entry"]) if row_dict.get("strategy_entry") else None,
                    strategy_entry_component=(
                        str(row_dict["strategy_entry_component"])
                        if row_dict.get("strategy_entry_component")
                        else None
                    ),
                    strategy_entry_signal=(
                        str(row_dict["strategy_entry_signal"])
                        if row_dict.get("strategy_entry_signal")
                        else None
                    ),
                    mode=str(row_dict["mode"]),
                    entry_confidence=float(row_dict.get("entry_confidence") or 0.0),
                    trailing_override=trailing_override,
                )
            )
        return result

    def delete_pending_open(self, pending_id: str) -> None:
        def _write() -> None:
            self._conn.execute("DELETE FROM pending_opens WHERE pending_id = ?", (str(pending_id),))

        self._run_write_tx(_write)

    def update_trade_status(
        self,
        position_id: str,
        *,
        status: str,
        close_price: float | None = None,
        closed_at: float | None = None,
        pnl: float | None = None,
        pnl_is_estimated: bool | None = None,
        pnl_estimate_source: object = _UNSET,
    ) -> None:
        now = time.time()
        fields = ["status = ?", "updated_at = ?"]
        params: list[Any] = [str(status), now]

        if close_price is not None:
            fields.append("close_price = ?")
            params.append(float(close_price))
        if closed_at is not None:
            fields.append("closed_at = ?")
            params.append(float(closed_at))
        if pnl is not None:
            fields.append("pnl = ?")
            params.append(float(pnl))
        if pnl_is_estimated is not None:
            fields.append("pnl_is_estimated = ?")
            params.append(1 if pnl_is_estimated else 0)
        if pnl_estimate_source is not _UNSET:
            normalized_estimate_source = (
                str(pnl_estimate_source).strip()
                if pnl_estimate_source not in (None, "")
                else None
            )
            fields.append("pnl_estimate_source = ?")
            params.append(normalized_estimate_source)

        params.append(str(position_id))
        query = f"UPDATE trades SET {', '.join(fields)} WHERE position_id = ?"

        def _write() -> None:
            self._conn.execute(query, tuple(params))

        self._run_write_tx(_write)

    def update_trade_performance(
        self,
        *,
        position_id: str,
        symbol: str,
        max_favorable_pips: float,
        max_adverse_pips: float,
        max_favorable_pnl: float,
        max_adverse_pnl: float,
        opened_at: float | None = None,
        closed_at: float | None = None,
        close_reason: str | None = None,
    ) -> None:
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return
        now = time.time()
        normalized_opened_at = None
        if opened_at is not None:
            value = self._parse_float(opened_at, 0.0)
            if math.isfinite(value) and value > 0:
                normalized_opened_at = value
        normalized_closed_at = None
        if closed_at is not None:
            value = self._parse_float(closed_at, 0.0)
            if math.isfinite(value) and value > 0:
                normalized_closed_at = value
        normalized_reason = str(close_reason or "").strip() or None

        favorable_pips = max(0.0, self._parse_float(max_favorable_pips, 0.0))
        adverse_pips = max(0.0, self._parse_float(max_adverse_pips, 0.0))
        favorable_pnl = max(0.0, self._parse_float(max_favorable_pnl, 0.0))
        adverse_pnl = min(0.0, self._parse_float(max_adverse_pnl, 0.0))

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO trade_performance(
                    position_id, symbol, opened_at, closed_at, close_reason,
                    max_favorable_pips, max_adverse_pips,
                    max_favorable_pnl, max_adverse_pnl,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    symbol=excluded.symbol,
                    opened_at=COALESCE(trade_performance.opened_at, excluded.opened_at),
                    closed_at=COALESCE(trade_performance.closed_at, excluded.closed_at),
                    close_reason=COALESCE(excluded.close_reason, trade_performance.close_reason),
                    max_favorable_pips=MAX(trade_performance.max_favorable_pips, excluded.max_favorable_pips),
                    max_adverse_pips=MAX(trade_performance.max_adverse_pips, excluded.max_adverse_pips),
                    max_favorable_pnl=MAX(trade_performance.max_favorable_pnl, excluded.max_favorable_pnl),
                    max_adverse_pnl=MIN(trade_performance.max_adverse_pnl, excluded.max_adverse_pnl),
                    updated_at=excluded.updated_at
                """,
                (
                    normalized_position_id,
                    normalized_symbol,
                    normalized_opened_at,
                    normalized_closed_at,
                    normalized_reason,
                    favorable_pips,
                    adverse_pips,
                    favorable_pnl,
                    adverse_pnl,
                    now,
                ),
            )

        self._run_write_tx(_write)

    def finalize_trade_performance(
        self,
        *,
        position_id: str,
        symbol: str,
        closed_at: float | None,
        close_reason: str | None = None,
    ) -> None:
        self.update_trade_performance(
            position_id=position_id,
            symbol=symbol,
            max_favorable_pips=0.0,
            max_adverse_pips=0.0,
            max_favorable_pnl=0.0,
            max_adverse_pnl=0.0,
            closed_at=closed_at,
            close_reason=close_reason,
        )

    def update_trade_execution_quality(
        self,
        *,
        position_id: str,
        symbol: str,
        entry_reference_price: float | None = None,
        entry_fill_price: float | None = None,
        entry_slippage_pips: float | None = None,
        entry_adverse_slippage_pips: float | None = None,
        exit_reference_price: float | None = None,
        exit_fill_price: float | None = None,
        exit_slippage_pips: float | None = None,
        exit_adverse_slippage_pips: float | None = None,
    ) -> None:
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return

        def _price_or_none(value: float | None) -> float | None:
            if value is None:
                return None
            parsed = self._parse_float(value, 0.0)
            if not math.isfinite(parsed) or parsed <= 0.0:
                return None
            return parsed

        def _metric_or_none(value: float | None) -> float | None:
            if value is None:
                return None
            parsed = self._parse_float(value, 0.0)
            if not math.isfinite(parsed):
                return None
            return parsed

        payload = {
            "entry_reference_price": _price_or_none(entry_reference_price),
            "entry_fill_price": _price_or_none(entry_fill_price),
            "entry_slippage_pips": _metric_or_none(entry_slippage_pips),
            "entry_adverse_slippage_pips": _metric_or_none(entry_adverse_slippage_pips),
            "exit_reference_price": _price_or_none(exit_reference_price),
            "exit_fill_price": _price_or_none(exit_fill_price),
            "exit_slippage_pips": _metric_or_none(exit_slippage_pips),
            "exit_adverse_slippage_pips": _metric_or_none(exit_adverse_slippage_pips),
        }
        now = time.time()

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO trade_performance(
                    position_id,
                    symbol,
                    max_favorable_pips,
                    max_adverse_pips,
                    max_favorable_pnl,
                    max_adverse_pnl,
                    entry_reference_price,
                    entry_fill_price,
                    entry_slippage_pips,
                    entry_adverse_slippage_pips,
                    exit_reference_price,
                    exit_fill_price,
                    exit_slippage_pips,
                    exit_adverse_slippage_pips,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    symbol=excluded.symbol,
                    entry_reference_price=COALESCE(excluded.entry_reference_price, trade_performance.entry_reference_price),
                    entry_fill_price=COALESCE(excluded.entry_fill_price, trade_performance.entry_fill_price),
                    entry_slippage_pips=COALESCE(excluded.entry_slippage_pips, trade_performance.entry_slippage_pips),
                    entry_adverse_slippage_pips=COALESCE(excluded.entry_adverse_slippage_pips, trade_performance.entry_adverse_slippage_pips),
                    exit_reference_price=COALESCE(excluded.exit_reference_price, trade_performance.exit_reference_price),
                    exit_fill_price=COALESCE(excluded.exit_fill_price, trade_performance.exit_fill_price),
                    exit_slippage_pips=COALESCE(excluded.exit_slippage_pips, trade_performance.exit_slippage_pips),
                    exit_adverse_slippage_pips=COALESCE(excluded.exit_adverse_slippage_pips, trade_performance.exit_adverse_slippage_pips),
                    updated_at=excluded.updated_at
                """,
                (
                    normalized_position_id,
                    normalized_symbol,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    payload["entry_reference_price"],
                    payload["entry_fill_price"],
                    payload["entry_slippage_pips"],
                    payload["entry_adverse_slippage_pips"],
                    payload["exit_reference_price"],
                    payload["exit_fill_price"],
                    payload["exit_slippage_pips"],
                    payload["exit_adverse_slippage_pips"],
                    now,
                ),
            )

        self._run_write_tx(_write)

    def load_trade_performance(self, position_id: str) -> dict[str, Any] | None:
        normalized_position_id = str(position_id or "").strip()
        if not normalized_position_id:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    position_id,
                    symbol,
                    opened_at,
                    closed_at,
                    close_reason,
                    max_favorable_pips,
                    max_adverse_pips,
                    max_favorable_pnl,
                    max_adverse_pnl,
                    entry_reference_price,
                    entry_fill_price,
                    entry_slippage_pips,
                    entry_adverse_slippage_pips,
                    exit_reference_price,
                    exit_fill_price,
                    exit_slippage_pips,
                    exit_adverse_slippage_pips,
                    updated_at
                FROM trade_performance
                WHERE position_id = ?
                LIMIT 1
                """,
                (normalized_position_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(row)

    def load_trade_close_events(
        self,
        *,
        symbol: str,
        position_id: str,
        opened_at: float | None = None,
        closed_at: float | None = None,
        updated_at: float | None = None,
        limit: int = 2000,
        lookback_sec: float = 900.0,
        lookahead_sec: float = 900.0,
    ) -> list[dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_position_id = str(position_id or "").strip()
        normalized_limit = max(1, int(limit))
        if not normalized_symbol or not normalized_position_id:
            return []

        lower_bound: float | None = None
        upper_bound: float | None = None
        normalized_opened_at = self._parse_float(opened_at, 0.0) if opened_at is not None else 0.0
        normalized_closed_at = self._parse_float(closed_at, 0.0) if closed_at is not None else 0.0
        normalized_updated_at = self._parse_float(updated_at, 0.0) if updated_at is not None else 0.0
        if normalized_opened_at > 0.0:
            lower_bound = max(0.0, normalized_opened_at - max(0.0, float(lookback_sec)))
        elif normalized_closed_at > 0.0:
            lower_bound = max(0.0, normalized_closed_at - max(0.0, float(lookback_sec)))
        end_ts = normalized_closed_at if normalized_closed_at > 0.0 else normalized_updated_at
        if end_ts > 0.0:
            upper_bound = end_ts + max(0.0, float(lookahead_sec))

        with self._lock:
            rows: list[sqlite3.Row] = []
            if lower_bound is not None and upper_bound is not None and upper_bound >= lower_bound:
                rows = self._conn.execute(
                    """
                    SELECT ts, id, message, payload_json
                    FROM events
                    WHERE symbol = ? AND ts >= ? AND ts <= ?
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                    """,
                    (normalized_symbol, lower_bound, upper_bound, normalized_limit),
                ).fetchall()
            if not rows:
                escaped_position_id = (
                    normalized_position_id
                    .replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_")
                )
                rows = self._conn.execute(
                    """
                    SELECT ts, id, message, payload_json
                    FROM events
                    WHERE symbol = ? AND payload_json LIKE ? ESCAPE '\\'
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                    """,
                    (normalized_symbol, f"%{escaped_position_id}%", normalized_limit),
                ).fetchall()
            if not rows:
                rows = self._conn.execute(
                    """
                    SELECT ts, id, message, payload_json
                    FROM events
                    WHERE symbol = ?
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                    """,
                    (normalized_symbol, normalized_limit),
                ).fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload_raw = item.get("payload_json")
            item["payload"] = json.loads(payload_raw) if payload_raw else None
            result.append(item)
        return result

    def summarize_recent_trade_execution_quality(
        self,
        *,
        symbol: str,
        strategy_entry: str,
        limit: int = 12,
    ) -> dict[str, Any] | None:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_strategy = str(strategy_entry or "").strip().lower()
        if not normalized_symbol or not normalized_strategy:
            return None
        sample_limit = max(1, int(limit))
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS sample_count,
                    AVG(COALESCE(entry_adverse_slippage_pips, 0.0)) AS avg_entry_adverse_slippage_pips,
                    AVG(COALESCE(exit_adverse_slippage_pips, 0.0)) AS avg_exit_adverse_slippage_pips,
                    AVG(COALESCE(entry_adverse_slippage_pips, 0.0) + COALESCE(exit_adverse_slippage_pips, 0.0))
                        AS avg_total_adverse_slippage_pips,
                    MAX(COALESCE(entry_adverse_slippage_pips, 0.0) + COALESCE(exit_adverse_slippage_pips, 0.0))
                        AS max_total_adverse_slippage_pips,
                    AVG(
                        CASE
                            WHEN (COALESCE(entry_adverse_slippage_pips, 0.0) + COALESCE(exit_adverse_slippage_pips, 0.0)) > 0.0
                            THEN 1.0
                            ELSE 0.0
                        END
                    ) AS adverse_trade_share
                FROM (
                    SELECT
                        tp.entry_adverse_slippage_pips,
                        tp.exit_adverse_slippage_pips
                    FROM trades t
                    JOIN trade_performance tp ON tp.position_id = t.position_id
                    WHERE t.status = 'closed'
                      AND t.symbol = ?
                      AND LOWER(
                        COALESCE(
                            NULLIF(t.strategy_entry_component, ''),
                            NULLIF(t.strategy_entry, ''),
                            t.strategy
                        )
                      ) = ?
                      AND (
                        tp.entry_adverse_slippage_pips IS NOT NULL
                        OR tp.exit_adverse_slippage_pips IS NOT NULL
                    )
                    ORDER BY COALESCE(t.closed_at, tp.closed_at, t.updated_at, t.opened_at) DESC
                    LIMIT ?
                ) recent_fills
                """,
                (
                    normalized_symbol,
                    normalized_strategy,
                    sample_limit,
                ),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        sample_count = int(result.get("sample_count") or 0)
        if sample_count <= 0:
            return None
        result["sample_count"] = sample_count
        return result

    def load_recent_strategy_entry_confidence_samples(
        self,
        *,
        symbol: str,
        strategy_entry: str,
        limit: int = 256,
        mode: str | None = None,
    ) -> list[float]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_strategy = str(strategy_entry or "").strip().lower()
        if not normalized_symbol or not normalized_strategy:
            return []
        sample_limit = max(1, int(limit))
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    """
                    SELECT
                        entry_confidence
                    FROM trades t
                    WHERE t.symbol = ?
                      AND LOWER(
                        COALESCE(
                            NULLIF(t.strategy_entry_component, ''),
                            NULLIF(t.strategy_entry, ''),
                            t.strategy
                        )
                      ) = ?
                      AND t.entry_confidence IS NOT NULL
                    ORDER BY COALESCE(t.opened_at, t.updated_at) DESC
                    LIMIT ?
                    """,
                    (
                        normalized_symbol,
                        normalized_strategy,
                        sample_limit,
                    ),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT
                        entry_confidence
                    FROM trades t
                    WHERE t.symbol = ?
                      AND t.mode = ?
                      AND LOWER(
                        COALESCE(
                            NULLIF(t.strategy_entry_component, ''),
                            NULLIF(t.strategy_entry, ''),
                            t.strategy
                        )
                      ) = ?
                      AND t.entry_confidence IS NOT NULL
                    ORDER BY COALESCE(t.opened_at, t.updated_at) DESC
                    LIMIT ?
                    """,
                    (
                        normalized_symbol,
                        str(mode),
                        normalized_strategy,
                        sample_limit,
                    ),
                ).fetchall()
        result: list[float] = []
        for row in rows:
            try:
                parsed = float(row["entry_confidence"])
            except (TypeError, ValueError, KeyError):
                continue
            if not math.isfinite(parsed):
                continue
            if parsed < 0.0:
                parsed = 0.0
            elif parsed > 1.0:
                parsed = 1.0
            result.append(parsed)
        return result

    def record_event(
        self,
        level: str,
        symbol: str | None,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        now = time.time()
        row = (
            float(now),
            str(level),
            (str(symbol) if symbol is not None else None),
            str(message),
            (json.dumps(payload) if payload else None),
        )
        if self._enqueue_event_async_write(row):
            return

        def _write() -> None:
            self._conn.execute(
                "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
                row,
            )

        self._run_write_tx(_write)

    def load_events(self, limit: int = 100) -> list[dict[str, Any]]:
        self.flush_event_async_writes()
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()

        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload_raw = item.get("payload_json")
            item["payload"] = json.loads(payload_raw) if payload_raw else None
            events.append(item)
        return events

    def load_events_since(self, since_ts: float, limit: int = 10_000) -> list[dict[str, Any]]:
        self.flush_event_async_writes()
        normalized_since = self._parse_float(since_ts, 0.0)
        normalized_limit = max(1, int(limit))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM events
                WHERE ts >= ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (normalized_since, normalized_limit),
            ).fetchall()

        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload_raw = item.get("payload_json")
            item["payload"] = json.loads(payload_raw) if payload_raw else None
            events.append(item)
        return events

    def _cleanup_position_updates_if_due_in_txn(self, now_ts: float) -> None:
        if (now_ts - self._last_position_updates_cleanup_ts) < self._position_updates_cleanup_min_interval_sec:
            return
        cutoff_ts = now_ts - self._housekeeping_position_updates_retention_sec
        self._conn.execute(
            "DELETE FROM position_updates WHERE ts < ?",
            (float(cutoff_ts),),
        )
        self._last_position_updates_cleanup_ts = now_ts

    def record_position_update(self, update: dict[str, Any]) -> None:
        if not isinstance(update, dict):
            return
        now = time.time()
        ts = self._parse_float(update.get("ts"), now)
        if not math.isfinite(ts) or ts <= 0:
            ts = now
        request_payload: Any = update.get("request")
        query_payload: Any = update.get("query")
        if query_payload is not None:
            request_payload = {
                "payload": request_payload,
                "query": query_payload,
            }
        normalized_update = {
            "ts": ts,
            "symbol": str(update.get("symbol") or "").strip().upper() or None,
            "position_id": str(update.get("position_id") or "").strip() or None,
            "deal_reference": str(update.get("deal_reference") or "").strip() or None,
            "operation": str(update.get("operation") or "position_request").strip() or "position_request",
            "method": str(update.get("method") or "").strip().upper() or "GET",
            "path": str(update.get("path") or "").strip() or "/",
            "http_status": (
                int(float(update["http_status"]))
                if update.get("http_status") not in (None, "")
                else None
            ),
            "success": 1 if bool(update.get("success")) else 0,
            "error_text": str(update.get("error_text") or "").strip() or None,
            "strategy": None,
            "strategy_base": None,
            "strategy_entry": None,
            "strategy_entry_component": None,
            "strategy_entry_signal": None,
            "mode": None,
            "source": str(update.get("source") or "ig_rest").strip() or "ig_rest",
            "request_json": request_payload,
            "response_json": update.get("response"),
            "updated_at": now,
        }

        def _write() -> None:
            identity_row = None
            if normalized_update["position_id"]:
                identity_row = self._conn.execute(
                    "SELECT * FROM trades WHERE position_id = ? LIMIT 1",
                    (normalized_update["position_id"],),
                ).fetchone()
            if identity_row is None and normalized_update["deal_reference"]:
                identity_row = self._conn.execute(
                    "SELECT * FROM trades WHERE deal_reference = ? ORDER BY updated_at DESC LIMIT 1",
                    (normalized_update["deal_reference"],),
                ).fetchone()
            if identity_row is None and normalized_update["deal_reference"]:
                identity_row = self._conn.execute(
                    """
                    SELECT
                        position_id,
                        symbol,
                        strategy,
                        strategy_entry,
                        strategy_entry_component,
                        strategy_entry_signal,
                        mode
                    FROM pending_opens
                    WHERE pending_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (normalized_update["deal_reference"],),
                ).fetchone()
            if identity_row is None and normalized_update["position_id"]:
                identity_row = self._conn.execute(
                    """
                    SELECT
                        position_id,
                        symbol,
                        strategy,
                        strategy_entry,
                        strategy_entry_component,
                        strategy_entry_signal,
                        mode
                    FROM pending_opens
                    WHERE position_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (normalized_update["position_id"],),
                ).fetchone()
            if identity_row is not None:
                identity = dict(identity_row)
                normalized_update["symbol"] = (
                    str(identity.get("symbol") or "").strip().upper()
                    or normalized_update["symbol"]
                )
                normalized_update["strategy"] = self._normalize_strategy_label(identity.get("strategy"))
                normalized_update["strategy_base"] = self._strategy_base_from_trade_row(identity)
                normalized_update["strategy_entry"] = (
                    self._normalize_strategy_label(identity.get("strategy_entry"))
                    or normalized_update["strategy_base"]
                    or normalized_update["strategy"]
                )
                normalized_update["strategy_entry_component"] = self._entry_component_from_trade_row(identity)
                normalized_update["strategy_entry_signal"] = self._normalize_strategy_label(
                    identity.get("strategy_entry_signal")
                )
                normalized_update["mode"] = str(identity.get("mode") or "").strip().lower() or None
            self._conn.execute(
                """
                INSERT INTO position_updates(
                    ts, symbol, position_id, deal_reference, operation,
                    method, path, http_status, success, error_text,
                    request_json, response_json, strategy, strategy_base,
                    strategy_entry, strategy_entry_component, strategy_entry_signal,
                    mode, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_update["ts"],
                    normalized_update["symbol"],
                    normalized_update["position_id"],
                    normalized_update["deal_reference"],
                    normalized_update["operation"],
                    normalized_update["method"],
                    normalized_update["path"],
                    normalized_update["http_status"],
                    normalized_update["success"],
                    normalized_update["error_text"],
                    (
                        json.dumps(normalized_update["request_json"], ensure_ascii=False, default=str)
                        if normalized_update["request_json"] is not None
                        else None
                    ),
                    (
                        json.dumps(normalized_update["response_json"], ensure_ascii=False, default=str)
                        if normalized_update["response_json"] is not None
                        else None
                    ),
                    normalized_update["strategy"],
                    normalized_update["strategy_base"],
                    normalized_update["strategy_entry"],
                    normalized_update["strategy_entry_component"],
                    normalized_update["strategy_entry_signal"],
                    normalized_update["mode"],
                    normalized_update["source"],
                    normalized_update["updated_at"],
                ),
            )
            self._cleanup_position_updates_if_due_in_txn(now)

        self._run_write_tx(_write)

    def load_position_updates(
        self,
        *,
        limit: int = 0,
        position_id: str | None = None,
        symbol: str | None = None,
        deal_reference: str | None = None,
        since_ts: float | None = None,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(0, int(limit))
        where_clauses: list[str] = []
        params: list[Any] = []

        normalized_position_id = str(position_id or "").strip()
        if normalized_position_id:
            where_clauses.append("position_id = ?")
            params.append(normalized_position_id)

        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol:
            where_clauses.append("symbol = ?")
            params.append(normalized_symbol)

        normalized_reference = str(deal_reference or "").strip()
        if normalized_reference:
            where_clauses.append("deal_reference = ?")
            params.append(normalized_reference)

        if since_ts is not None:
            normalized_since_ts = self._parse_float(since_ts, 0.0)
            if math.isfinite(normalized_since_ts) and normalized_since_ts > 0:
                where_clauses.append("ts >= ?")
                params.append(normalized_since_ts)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        if normalized_limit > 0:
            query = (
                "SELECT * FROM ("
                "SELECT * FROM position_updates "
                f"{where_sql} "
                "ORDER BY ts DESC, id DESC LIMIT ?"
                ") ORDER BY ts ASC, id ASC"
            )
            params.append(normalized_limit)
        else:
            query = f"SELECT * FROM position_updates {where_sql} ORDER BY ts ASC, id ASC"

        with self._lock:
            rows = self._conn.execute(query, tuple(params)).fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            request_raw = item.pop("request_json", None)
            response_raw = item.pop("response_json", None)
            item["request"] = json.loads(request_raw) if request_raw else None
            item["response"] = json.loads(response_raw) if response_raw else None
            result.append(item)
        return result

    def upsert_ig_rate_limit_worker(
        self,
        *,
        worker_key: str,
        limit_scope: str,
        limit_unit: str,
        limit_value: int,
        used_value: int,
        remaining_value: int,
        window_sec: float,
        blocked_total: int,
        last_request_ts: float | None = None,
        last_block_ts: float | None = None,
        notes: str | None = None,
    ) -> None:
        normalized_worker_key = str(worker_key).strip().lower()
        if not normalized_worker_key:
            return

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO ig_rate_limit_workers(
                    worker_key, limit_scope, limit_unit, limit_value,
                    used_value, remaining_value, window_sec, blocked_total,
                    last_request_ts, last_block_ts, notes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_key) DO UPDATE SET
                    limit_scope=excluded.limit_scope,
                    limit_unit=excluded.limit_unit,
                    limit_value=excluded.limit_value,
                    used_value=excluded.used_value,
                    remaining_value=excluded.remaining_value,
                    window_sec=excluded.window_sec,
                    blocked_total=excluded.blocked_total,
                    last_request_ts=excluded.last_request_ts,
                    last_block_ts=excluded.last_block_ts,
                    notes=excluded.notes,
                    updated_at=excluded.updated_at
                """,
                (
                    normalized_worker_key,
                    str(limit_scope),
                    str(limit_unit),
                    int(limit_value),
                    int(used_value),
                    int(remaining_value),
                    float(window_sec),
                    int(blocked_total),
                    float(last_request_ts) if last_request_ts is not None else None,
                    float(last_block_ts) if last_block_ts is not None else None,
                    (str(notes) if notes is not None else None),
                    time.time(),
                ),
            )

        self._run_write_tx(_write)

    def load_ig_rate_limit_workers(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM ig_rate_limit_workers
                ORDER BY worker_key ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_broker_tick(
        self,
        *,
        symbol: str,
        bid: float,
        ask: float,
        ts: float,
        volume: float | None = None,
        source: str | None = None,
    ) -> None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return
        now = time.time()
        mid = (float(bid) + float(ask)) / 2.0

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO broker_tick_history(symbol, bid, ask, mid, ts, volume, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_symbol,
                    float(bid),
                    float(ask),
                    float(mid),
                    float(ts),
                    (float(volume) if volume is not None else None),
                    (str(source) if source is not None else None),
                    now,
                ),
            )
            self._conn.execute(
                """
                INSERT INTO broker_ticks(symbol, bid, ask, mid, ts, volume, source, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    bid=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.bid ELSE broker_ticks.bid END,
                    ask=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.ask ELSE broker_ticks.ask END,
                    mid=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.mid ELSE broker_ticks.mid END,
                    ts=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.ts ELSE broker_ticks.ts END,
                    volume=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.volume ELSE broker_ticks.volume END,
                    source=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.source ELSE broker_ticks.source END,
                    updated_at=CASE WHEN excluded.ts >= broker_ticks.ts THEN excluded.updated_at ELSE broker_ticks.updated_at END
                """,
                (
                    normalized_symbol,
                    float(bid),
                    float(ask),
                    float(mid),
                    float(ts),
                    (float(volume) if volume is not None else None),
                    (str(source) if source is not None else None),
                    now,
                ),
            )

        self._run_write_tx(_write)

    def prune_stale_broker_ticks(
        self,
        *,
        max_age_sec: float,
        symbol: str | None = None,
        now_ts: float | None = None,
    ) -> int:
        normalized_max_age = max(0.5, float(max_age_sec))
        normalized_now = time.time() if now_ts is None else float(now_ts)
        cutoff_ts = normalized_now - normalized_max_age
        normalized_symbol = str(symbol or "").strip().upper()

        deleted_rows = 0

        def _write() -> None:
            nonlocal deleted_rows
            total_deleted = 0
            if normalized_symbol:
                latest_cursor = self._conn.execute(
                    "DELETE FROM broker_ticks WHERE symbol = ? AND ts < ?",
                    (normalized_symbol, float(cutoff_ts)),
                )
                history_cursor = self._conn.execute(
                    "DELETE FROM broker_tick_history WHERE symbol = ? AND ts < ?",
                    (normalized_symbol, float(cutoff_ts)),
                )
            else:
                latest_cursor = self._conn.execute(
                    "DELETE FROM broker_ticks WHERE ts < ?",
                    (float(cutoff_ts),),
                )
                history_cursor = self._conn.execute(
                    "DELETE FROM broker_tick_history WHERE ts < ?",
                    (float(cutoff_ts),),
                )
            for cursor in (latest_cursor, history_cursor):
                if cursor.rowcount in (None, -1):
                    continue
                total_deleted += max(0, int(cursor.rowcount))
            deleted_rows = total_deleted

        self._run_write_tx(_write)
        return deleted_rows

    def load_latest_broker_tick(self, symbol: str, max_age_sec: float) -> PriceTick | None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT symbol, bid, ask, ts, volume, updated_at
                FROM broker_ticks
                WHERE symbol = ?
                LIMIT 1
                """,
                (normalized_symbol,),
            ).fetchone()
        if row is None:
            return None
        ts = self._parse_float(row["ts"], 0.0)
        received_at = self._parse_float(row["updated_at"], ts)
        freshness_ts = max(ts, received_at)
        if max_age_sec > 0 and (time.time() - freshness_ts) > float(max_age_sec):
            self.prune_stale_broker_ticks(
                max_age_sec=max_age_sec,
                symbol=normalized_symbol,
            )
            return None
        volume = row["volume"]
        parsed_volume = None if volume is None else self._parse_float(volume, 0.0)
        return PriceTick(
            symbol=normalized_symbol,
            bid=self._parse_float(row["bid"], 0.0),
            ask=self._parse_float(row["ask"], 0.0),
            timestamp=ts,
            volume=parsed_volume,
            received_at=received_at if received_at > 0 else ts,
        )

    def _upsert_broker_symbol_pip_size_in_txn(
        self,
        *,
        symbol: str,
        pip_size: float,
        ts: float,
        updated_at: float,
        source: str | None,
    ) -> None:
        if pip_size <= 0 or not math.isfinite(pip_size):
            return
        self._conn.execute(
            """
            INSERT INTO broker_symbol_pip_sizes(
                symbol, pip_size, source, ts, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                pip_size=excluded.pip_size,
                source=excluded.source,
                ts=excluded.ts,
                updated_at=excluded.updated_at
            """,
            (
                symbol,
                float(pip_size),
                (str(source) if source is not None else None),
                float(ts),
                float(updated_at),
            ),
        )

    def upsert_broker_symbol_pip_size(
        self,
        *,
        symbol: str,
        pip_size: float,
        ts: float | None = None,
        source: str | None = None,
    ) -> None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return
        now = time.time()
        effective_ts = float(ts if ts is not None else now)

        def _write() -> None:
            self._upsert_broker_symbol_pip_size_in_txn(
                symbol=normalized_symbol,
                pip_size=float(pip_size),
                ts=effective_ts,
                updated_at=now,
                source=source,
            )

        self._run_write_tx(_write)

    def load_broker_symbol_pip_size(self, symbol: str, max_age_sec: float = 0.0) -> float | None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT pip_size, ts
                FROM broker_symbol_pip_sizes
                WHERE symbol = ?
                LIMIT 1
                """,
                (normalized_symbol,),
            ).fetchone()
        if row is None:
            return None
        ts = self._parse_float(row["ts"], 0.0)
        if max_age_sec > 0 and (time.time() - ts) > float(max_age_sec):
            return None
        pip_size = self._parse_float(row["pip_size"], 0.0)
        if not math.isfinite(pip_size) or pip_size <= 0:
            return None
        return pip_size

    def load_broker_symbol_pip_sizes(self, max_age_sec: float = 0.0) -> dict[str, float]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, pip_size, ts
                FROM broker_symbol_pip_sizes
                """
            ).fetchall()
        now = time.time()
        result: dict[str, float] = {}
        for row in rows:
            symbol = str(row["symbol"] or "").strip().upper()
            if not symbol:
                continue
            ts = self._parse_float(row["ts"], 0.0)
            if max_age_sec > 0 and (now - ts) > float(max_age_sec):
                continue
            pip_size = self._parse_float(row["pip_size"], 0.0)
            if not math.isfinite(pip_size) or pip_size <= 0:
                continue
            result[symbol] = pip_size
        return result

    def upsert_broker_symbol_spec(
        self,
        *,
        symbol: str,
        spec: SymbolSpec,
        ts: float | None = None,
        source: str | None = None,
    ) -> None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return
        now = time.time()
        metadata = dict(spec.metadata) if isinstance(spec.metadata, dict) else {}
        normalized_epic = _normalize_broker_symbol_spec_epic(metadata.get("epic"))
        normalized_variant = _normalize_broker_symbol_spec_epic_variant(
            metadata.get("epic_variant"),
            epic=normalized_epic,
        )
        if normalized_epic:
            metadata["epic"] = normalized_epic
        if normalized_variant:
            metadata["epic_variant"] = normalized_variant
        metadata_json = json.dumps(metadata) if metadata else None
        effective_ts = float(ts if ts is not None else now)
        cache_key = _broker_symbol_spec_cache_key(normalized_symbol, normalized_epic)

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO broker_symbol_specs(
                    cache_key, symbol, epic, epic_variant, tick_size, tick_value, contract_size, lot_min, lot_max,
                    lot_step, min_stop_distance_price, one_pip_means, metadata_json,
                    source, ts, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    symbol=excluded.symbol,
                    epic=excluded.epic,
                    epic_variant=excluded.epic_variant,
                    tick_size=excluded.tick_size,
                    tick_value=excluded.tick_value,
                    contract_size=excluded.contract_size,
                    lot_min=excluded.lot_min,
                    lot_max=excluded.lot_max,
                    lot_step=excluded.lot_step,
                    min_stop_distance_price=excluded.min_stop_distance_price,
                    one_pip_means=excluded.one_pip_means,
                    metadata_json=excluded.metadata_json,
                    source=excluded.source,
                    ts=excluded.ts,
                    updated_at=excluded.updated_at
                """,
                (
                    cache_key,
                    normalized_symbol,
                    normalized_epic or None,
                    normalized_variant or None,
                    float(spec.tick_size),
                    float(spec.tick_value),
                    float(spec.contract_size),
                    float(spec.lot_min),
                    float(spec.lot_max),
                    float(spec.lot_step),
                    (
                        float(spec.min_stop_distance_price)
                        if spec.min_stop_distance_price is not None
                        else None
                    ),
                    (float(spec.one_pip_means) if spec.one_pip_means is not None else None),
                    metadata_json,
                    (str(source) if source is not None else None),
                    effective_ts,
                    now,
                ),
            )
            self._upsert_broker_symbol_pip_size_in_txn(
                symbol=normalized_symbol,
                pip_size=float(spec.tick_size),
                ts=effective_ts,
                updated_at=now,
                source=source or "broker_symbol_spec",
            )

        self._run_write_tx(_write)

    def load_broker_symbol_spec(
        self,
        symbol: str,
        max_age_sec: float,
        *,
        epic: str | None = None,
        epic_variant: str | None = None,
    ) -> SymbolSpec | None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return None
        normalized_epic = _normalize_broker_symbol_spec_epic(epic)
        normalized_variant = _normalize_broker_symbol_spec_epic_variant(
            epic_variant,
            epic=normalized_epic,
        )
        query_candidates: list[tuple[str, tuple[object, ...]]] = []
        if normalized_epic:
            query_candidates.append(
                (
                    """
                    SELECT *
                    FROM broker_symbol_specs
                    WHERE symbol = ? AND epic = ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    (normalized_symbol, normalized_epic),
                )
            )
        if normalized_variant:
            query_candidates.append(
                (
                    """
                    SELECT *
                    FROM broker_symbol_specs
                    WHERE symbol = ? AND epic_variant = ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    (normalized_symbol, normalized_variant),
                )
            )
        query_candidates.append(
            (
                """
                SELECT *
                FROM broker_symbol_specs
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT 1
                """,
                (normalized_symbol,),
            )
        )
        row: sqlite3.Row | None = None
        with self._lock:
            for query, params in query_candidates:
                row = self._conn.execute(query, params).fetchone()
                if row is None:
                    continue
                ts = self._parse_float(row["ts"], 0.0)
                if max_age_sec > 0 and (time.time() - ts) > float(max_age_sec):
                    row = None
                    continue
                break
        if row is None:
            return None
        metadata = self._parse_broker_symbol_spec_metadata(row["metadata_json"])
        row_epic = _normalize_broker_symbol_spec_epic(row["epic"])
        row_variant = _normalize_broker_symbol_spec_epic_variant(
            row["epic_variant"],
            epic=row_epic,
        )
        if row_epic:
            metadata["epic"] = row_epic
        if row_variant:
            metadata["epic_variant"] = row_variant
        return SymbolSpec(
            symbol=normalized_symbol,
            tick_size=self._parse_float(row["tick_size"], 0.0),
            tick_value=self._parse_float(row["tick_value"], 0.0),
            contract_size=self._parse_float(row["contract_size"], 0.0),
            lot_min=self._parse_float(row["lot_min"], 0.0),
            lot_max=self._parse_float(row["lot_max"], 0.0),
            lot_step=self._parse_float(row["lot_step"], 0.0),
            min_stop_distance_price=(
                None
                if row["min_stop_distance_price"] is None
                else self._parse_float(row["min_stop_distance_price"], 0.0)
            ),
            one_pip_means=(
                None
                if row["one_pip_means"] is None
                else self._parse_float(row["one_pip_means"], 0.0)
            ),
            metadata=metadata,
        )

    def save_broker_tick_value_calibration(
        self,
        *,
        symbol: str,
        tick_size: float,
        tick_value: float,
        source: str,
        samples: int = 1,
    ) -> None:
        normalized_symbol = str(symbol).strip().upper()
        value = float(tick_value)
        size = float(tick_size)
        if not normalized_symbol or not math.isfinite(value) or value <= 0.0:
            return
        if not math.isfinite(size) or size <= 0.0:
            return
        payload = {
            "symbol": normalized_symbol,
            "tick_size": size,
            "tick_value": value,
            "source": str(source or "broker_close_calibration"),
            "samples": max(1, int(samples)),
            "updated_at": time.time(),
        }
        self.set_kv(
            f"broker_tick_value_calibration.{normalized_symbol}",
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
        )

    def load_broker_tick_value_calibration(
        self,
        symbol: str,
        *,
        max_age_sec: float = 30.0 * 24.0 * 60.0 * 60.0,
    ) -> dict[str, Any] | None:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return None
        raw = self.get_kv(f"broker_tick_value_calibration.{normalized_symbol}")
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        updated_at = self._parse_float(payload.get("updated_at"), 0.0)
        if max_age_sec > 0 and updated_at > 0.0 and (time.time() - updated_at) > float(max_age_sec):
            return None
        tick_value = self._parse_float(payload.get("tick_value"), 0.0)
        tick_size = self._parse_float(payload.get("tick_size"), 0.0)
        if tick_value <= 0.0 or tick_size <= 0.0:
            return None
        payload["symbol"] = normalized_symbol
        payload["tick_value"] = tick_value
        payload["tick_size"] = tick_size
        payload["samples"] = max(1, int(self._parse_float(payload.get("samples"), 1.0)))
        payload["updated_at"] = updated_at
        return payload

    def infer_broker_tick_value_calibration_from_recent_closes(
        self,
        *,
        symbol: str,
        tick_size: float,
        max_age_sec: float = 30.0 * 24.0 * 60.0 * 60.0,
        limit: int = 24,
    ) -> dict[str, Any] | None:
        normalized_symbol = str(symbol).strip().upper()
        normalized_tick_size = float(tick_size)
        if not normalized_symbol:
            return None
        if not math.isfinite(normalized_tick_size) or normalized_tick_size <= 0.0:
            return None
        cutoff_ts = time.time() - max(0.0, float(max_age_sec))
        with self._lock:
            event_rows = self._conn.execute(
                """
                SELECT payload_json
                FROM events
                WHERE symbol = ?
                  AND message = 'Position closed'
                  AND ts >= ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (normalized_symbol, cutoff_ts, max(1, int(limit))),
            ).fetchall()

        observed_values: list[float] = []
        for row in event_rows:
            payload_raw = row["payload_json"]
            if not payload_raw:
                continue
            try:
                payload = json.loads(str(payload_raw))
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            broker_sync = payload.get("broker_close_sync")
            if not isinstance(broker_sync, dict):
                continue
            if broker_sync.get("realized_pnl") is None:
                continue
            if bool(payload.get("pnl_pending_broker_sync")):
                continue
            position_id = str(payload.get("position_id") or "").strip()
            if not position_id:
                continue
            close_price = self._parse_float(payload.get("close_price"), 0.0)
            pnl_account = self._parse_float(payload.get("pnl"), 0.0)
            if close_price <= 0.0 or abs(pnl_account) <= FLOAT_COMPARISON_TOLERANCE:
                continue
            trade = self.get_trade_record(position_id)
            if not isinstance(trade, dict):
                continue
            open_price = self._parse_float(trade.get("open_price"), 0.0)
            volume = self._parse_float(trade.get("volume"), 0.0)
            if open_price <= 0.0 or volume <= 0.0:
                continue
            price_delta = abs(close_price - open_price)
            if price_delta <= 0.0:
                continue
            ticks = price_delta / normalized_tick_size
            if not math.isfinite(ticks) or ticks <= 0.0:
                continue
            observed_tick_value = abs(pnl_account) / (ticks * volume)
            if not math.isfinite(observed_tick_value) or observed_tick_value <= 0.0:
                continue
            observed_values.append(observed_tick_value)

        if not observed_values:
            return None
        median_tick_value = float(statistics.median(observed_values))
        calibration = {
            "symbol": normalized_symbol,
            "tick_size": normalized_tick_size,
            "tick_value": median_tick_value,
            "samples": len(observed_values),
            "source": "recent_broker_closed_positions",
            "updated_at": time.time(),
        }
        self.save_broker_tick_value_calibration(
            symbol=normalized_symbol,
            tick_size=normalized_tick_size,
            tick_value=median_tick_value,
            source="recent_broker_closed_positions",
            samples=len(observed_values),
        )
        return calibration

    def upsert_broker_account_snapshot(
        self,
        snapshot: AccountSnapshot,
        *,
        source: str | None = None,
        cache_key: str = "primary",
    ) -> None:
        normalized_key = str(cache_key).strip().lower() or "primary"
        now = time.time()

        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO broker_account_cache(
                    cache_key, balance, equity, margin_free, ts, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    balance=excluded.balance,
                    equity=excluded.equity,
                    margin_free=excluded.margin_free,
                    ts=excluded.ts,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                (
                    normalized_key,
                    float(snapshot.balance),
                    float(snapshot.equity),
                    float(snapshot.margin_free),
                    float(snapshot.timestamp),
                    (str(source) if source is not None else None),
                    now,
                ),
            )

        self._run_write_tx(_write)

    def load_latest_broker_account_snapshot(
        self,
        *,
        max_age_sec: float,
        cache_key: str = "primary",
    ) -> AccountSnapshot | None:
        normalized_key = str(cache_key).strip().lower() or "primary"
        with self._lock:
            row = self._conn.execute(
                """
                SELECT balance, equity, margin_free, ts
                FROM broker_account_cache
                WHERE cache_key = ?
                LIMIT 1
                """,
                (normalized_key,),
            ).fetchone()
        if row is None:
            return None
        ts = self._parse_float(row["ts"], 0.0)
        if max_age_sec > 0 and (time.time() - ts) > float(max_age_sec):
            return None
        return AccountSnapshot(
            balance=self._parse_float(row["balance"], 0.0),
            equity=self._parse_float(row["equity"], 0.0),
            margin_free=self._parse_float(row["margin_free"], 0.0),
            timestamp=ts,
        )

    def record_account_snapshot(
        self,
        snapshot: AccountSnapshot,
        open_positions: int,
        daily_pnl: float,
        drawdown_pct: float,
    ) -> None:
        def _write() -> None:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO account_snapshots(
                    ts, balance, equity, margin_free, open_positions, daily_pnl, drawdown_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.timestamp,
                    snapshot.balance,
                    snapshot.equity,
                    snapshot.margin_free,
                    open_positions,
                    daily_pnl,
                    drawdown_pct,
                ),
            )

        self._run_write_tx(_write)

    def set_kv(self, key: str, value: str) -> None:
        def _write() -> None:
            self._upsert_kv_in_txn(key, value)

        self._run_write_tx(_write)

    def get_kv(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def delete_kv(self, key: str) -> None:
        def _write() -> None:
            self._conn.execute("DELETE FROM kv WHERE key = ?", (key,))

        self._run_write_tx(_write)

    def sync_risk_anchors(
        self,
        *,
        day: str,
        snapshot_equity: float,
        snapshot_balance: float,
        start_balance: float,
        allow_external_cashflow_rebase: bool = False,
        external_cashflow_rebase_min_abs: float = 0.0,
        external_cashflow_rebase_min_pct: float = 0.0,
    ) -> dict[str, Any]:
        base_start_equity = max(float(snapshot_equity), float(start_balance))

        def _write() -> dict[str, Any]:
            rows = self._conn.execute(
                """
                SELECT key, value
                FROM kv
                WHERE key IN (
                    'risk.start_equity',
                    'risk.day',
                    'risk.daily_start_equity',
                    'risk.daily_hwm_equity',
                    'risk.daily_start_balance',
                    'risk.daily_locked_day',
                    'risk.daily_lock_reason',
                    'risk.daily_lock_until',
                    'risk.last_snapshot_day',
                    'risk.last_snapshot_balance',
                    'risk.last_snapshot_equity'
                )
                """
            ).fetchall()
            kv = {str(row["key"]): str(row["value"]) for row in rows}

            start_equity = self._parse_float(kv.get("risk.start_equity"), base_start_equity)
            if "risk.start_equity" not in kv:
                start_equity = base_start_equity
                self._upsert_kv_in_txn("risk.start_equity", f"{start_equity}")

            stored_day = kv.get("risk.day")
            previous_locked_day = kv.get("risk.daily_locked_day", "")
            previous_unlock_at = kv.get("risk.daily_lock_until", "")
            released_daily_lock = False
            external_cashflow_rebased = 0.0

            if stored_day != day:
                day_start_equity = float(snapshot_equity)
                day_hwm_equity = float(snapshot_equity)
                day_start_balance = float(snapshot_balance)
                self._upsert_kv_in_txn("risk.day", day)
                self._upsert_kv_in_txn("risk.daily_start_equity", f"{day_start_equity}")
                self._upsert_kv_in_txn("risk.daily_hwm_equity", f"{day_hwm_equity}")
                self._upsert_kv_in_txn("risk.daily_start_balance", f"{day_start_balance}")
                self._upsert_kv_in_txn("risk.daily_locked_day", "")
                self._upsert_kv_in_txn("risk.daily_lock_reason", "")
                self._upsert_kv_in_txn("risk.daily_lock_until", "")
                released_daily_lock = bool(previous_locked_day)
            else:
                day_start_equity = self._parse_float(
                    kv.get("risk.daily_start_equity"),
                    float(snapshot_equity),
                )
                day_start_balance = self._parse_float(
                    kv.get("risk.daily_start_balance"),
                    float(snapshot_balance),
                )
                day_hwm_equity = self._parse_float(
                    kv.get("risk.daily_hwm_equity"),
                    max(day_start_equity, float(snapshot_equity)),
                )

                if allow_external_cashflow_rebase:
                    last_snapshot_day = str(kv.get("risk.last_snapshot_day") or "")
                    last_balance = self._parse_float(
                        kv.get("risk.last_snapshot_balance"),
                        float(snapshot_balance),
                    )
                    last_equity = self._parse_float(
                        kv.get("risk.last_snapshot_equity"),
                        float(snapshot_equity),
                    )
                    if last_snapshot_day == day:
                        delta_balance = float(snapshot_balance) - last_balance
                        delta_equity = float(snapshot_equity) - last_equity
                        move_abs = abs(delta_balance)
                        baseline = max(abs(last_balance), 1.0)
                        move_pct = (move_abs / baseline) * 100.0
                        similarity_gap = abs(delta_balance - delta_equity)
                        similarity_tolerance = max(1.0, move_abs * 0.05)
                        if (
                            move_abs >= max(float(external_cashflow_rebase_min_abs), 0.0)
                            and move_pct >= max(float(external_cashflow_rebase_min_pct), 0.0)
                            and similarity_gap <= similarity_tolerance
                        ):
                            external_cashflow_rebased = delta_balance
                            start_equity += external_cashflow_rebased
                            day_start_equity += external_cashflow_rebased
                            day_hwm_equity += external_cashflow_rebased
                            day_start_balance += external_cashflow_rebased
                            self._upsert_kv_in_txn("risk.start_equity", f"{start_equity}")
                            self._upsert_kv_in_txn("risk.daily_start_equity", f"{day_start_equity}")
                            self._upsert_kv_in_txn("risk.daily_hwm_equity", f"{day_hwm_equity}")
                            self._upsert_kv_in_txn("risk.daily_start_balance", f"{day_start_balance}")

                if float(snapshot_equity) > day_hwm_equity:
                    day_hwm_equity = float(snapshot_equity)
                    self._upsert_kv_in_txn("risk.daily_hwm_equity", f"{day_hwm_equity}")

            self._upsert_kv_in_txn("risk.last_snapshot_day", day)
            self._upsert_kv_in_txn("risk.last_snapshot_balance", f"{float(snapshot_balance)}")
            self._upsert_kv_in_txn("risk.last_snapshot_equity", f"{float(snapshot_equity)}")

            return {
                "start_equity": start_equity,
                "day_start_equity": day_start_equity,
                "day_hwm_equity": day_hwm_equity,
                "day_start_balance": day_start_balance,
                "external_cashflow_rebased": external_cashflow_rebased,
                "released_daily_lock": released_daily_lock,
                "previous_locked_day": previous_locked_day,
                "previous_unlock_at": previous_unlock_at,
            }

        return self._run_write_tx_immediate(_write)

    def activate_daily_drawdown_lock(self, *, day: str, reason: str, unlock_at: str) -> None:
        def _write() -> None:
            self._upsert_kv_in_txn("risk.daily_locked_day", str(day))
            self._upsert_kv_in_txn("risk.daily_lock_reason", str(reason))
            self._upsert_kv_in_txn("risk.daily_lock_until", str(unlock_at))

        self._run_write_tx_immediate(_write)

    def acquire_open_slot(
        self,
        open_positions_count: int,
        max_open_positions: int,
        lease_sec: float,
    ) -> tuple[str | None, int, int]:
        lease = max(1.0, float(lease_sec))
        now = time.time()
        cutoff = now - lease
        normalized_open_positions = max(0, int(open_positions_count))

        def _write() -> tuple[str | None, int, int]:
            self._conn.execute(
                "DELETE FROM open_slot_reservations WHERE created_at < ?",
                (cutoff,),
            )
            row = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM open_slot_reservations"
            ).fetchone()
            pending = int(row["cnt"]) if row is not None else 0
            effective_open_positions = normalized_open_positions + pending
            if effective_open_positions >= int(max_open_positions):
                return None, pending, effective_open_positions

            reservation_id = _uuid7_hex(int(now * 1000.0))
            self._conn.execute(
                "INSERT INTO open_slot_reservations(reservation_id, created_at) VALUES (?, ?)",
                (reservation_id, now),
            )
            return reservation_id, pending + 1, effective_open_positions + 1

        return self._run_write_tx_immediate(_write)

    def release_open_slot(self, reservation_id: str) -> None:
        if not reservation_id:
            return

        def _write() -> None:
            self._conn.execute(
                "DELETE FROM open_slot_reservations WHERE reservation_id = ?",
                (reservation_id,),
            )

        self._run_write_tx(_write)

    def count_open_slot_reservations(self, lease_sec: float) -> int:
        lease = max(1.0, float(lease_sec))
        now = time.time()
        cutoff = now - lease

        def _write() -> int:
            self._conn.execute(
                "DELETE FROM open_slot_reservations WHERE created_at < ?",
                (cutoff,),
            )
            row = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM open_slot_reservations"
            ).fetchone()
            if row is None:
                return 0
            return int(row["cnt"])

        return self._run_write_tx(_write)

    def append_price_sample(
        self,
        symbol: str,
        ts: float,
        close: float,
        volume: float | None = None,
        max_rows_per_symbol: int = 5000,
        candle_resolutions_sec: Sequence[int] | None = None,
    ) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        now = time.time()
        max_future_skew_sec = 120.0
        normalized_resolutions = self._normalize_candle_resolutions(candle_resolutions_sec)

        def _write() -> None:
            self._conn.execute(
                """
                DELETE FROM price_history
                WHERE symbol = ? AND ts > ?
                """,
                (normalized_symbol, now + max_future_skew_sec),
            )
            self._conn.execute(
                """
                INSERT INTO price_history(symbol, ts, close, volume)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol, ts) DO UPDATE SET
                    close=excluded.close,
                    volume=excluded.volume
                """,
                (normalized_symbol, ts, close, volume),
            )
            cleanup_calls = self._price_history_cleanup_counts.get(normalized_symbol, 0) + 1
            self._price_history_cleanup_counts[normalized_symbol] = cleanup_calls
            last_cleanup_ts = self._price_history_last_cleanup_ts.get(normalized_symbol, 0.0)
            cleanup_due_by_count = cleanup_calls >= self._price_history_cleanup_every
            cleanup_due_by_time = (
                self._price_history_cleanup_min_interval_sec <= 0
                or (now - last_cleanup_ts) >= self._price_history_cleanup_min_interval_sec
            )
            if cleanup_due_by_count and cleanup_due_by_time:
                keep = max(100, int(max_rows_per_symbol))
                self._conn.execute(
                    """
                    DELETE FROM price_history
                    WHERE symbol = ?
                      AND ts < COALESCE(
                            (SELECT ts
                             FROM price_history
                             WHERE symbol = ?
                             ORDER BY ts DESC
                             LIMIT 1 OFFSET ?),
                            -1
                      )
                    """,
                    (normalized_symbol, normalized_symbol, keep - 1),
                )
                self._price_history_cleanup_counts[normalized_symbol] = 0
                self._price_history_last_cleanup_ts[normalized_symbol] = now

            for resolution_sec in normalized_resolutions:
                self._upsert_candle_history_row(
                    symbol=normalized_symbol,
                    resolution_sec=resolution_sec,
                    ts=ts,
                    open_price=close,
                    high_price=close,
                    low_price=close,
                    close=close,
                    volume=volume,
                    now_ts=now,
                    max_rows_per_symbol=max_rows_per_symbol,
                    overwrite_ohlc=False,
                )

        self._run_write_tx(_write)

    @staticmethod
    def _normalize_candle_resolutions(raw: Sequence[int] | None) -> tuple[int, ...]:
        if not raw:
            return ()
        normalized: list[int] = []
        for item in raw:
            try:
                resolution_sec = int(float(item))
            except (TypeError, ValueError):
                continue
            if resolution_sec <= 60:
                continue
            if resolution_sec not in normalized:
                normalized.append(resolution_sec)
        normalized.sort()
        return tuple(normalized)

    @staticmethod
    def _candle_bucket_start(ts: float, resolution_sec: int) -> float:
        if resolution_sec <= 0:
            return float(ts)
        return float(int(float(ts) // float(resolution_sec)) * int(resolution_sec))

    def _upsert_candle_history_row(
        self,
        *,
        symbol: str,
        resolution_sec: int,
        ts: float,
        open_price: float | None,
        high_price: float | None,
        low_price: float | None,
        close: float,
        volume: float | None,
        now_ts: float,
        max_rows_per_symbol: int,
        overwrite_ohlc: bool,
    ) -> None:
        bucket_start_ts = self._candle_bucket_start(ts, resolution_sec)
        open_value = float(open_price) if open_price is not None else float(close)
        high_value = float(high_price) if high_price is not None else float(close)
        low_value = float(low_price) if low_price is not None else float(close)
        self._conn.execute(
            """
            DELETE FROM candle_history
            WHERE symbol = ? AND resolution_sec = ? AND bucket_start_ts > ?
            """,
            (symbol, int(resolution_sec), now_ts + 120.0),
        )
        if overwrite_ohlc:
            self._conn.execute(
                """
                INSERT INTO candle_history(
                    symbol, resolution_sec, bucket_start_ts, open, high, low, close, volume, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, resolution_sec, bucket_start_ts) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=COALESCE(excluded.volume, candle_history.volume),
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    int(resolution_sec),
                    bucket_start_ts,
                    open_value,
                    high_value,
                    low_value,
                    close,
                    volume,
                    now_ts,
                ),
            )
        else:
            self._conn.execute(
                """
                INSERT INTO candle_history(
                    symbol, resolution_sec, bucket_start_ts, open, high, low, close, volume, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, resolution_sec, bucket_start_ts) DO UPDATE SET
                    open=COALESCE(candle_history.open, excluded.open),
                    high=CASE
                        WHEN candle_history.high IS NULL THEN excluded.high
                        WHEN excluded.high IS NULL THEN candle_history.high
                        ELSE MAX(candle_history.high, excluded.high)
                    END,
                    low=CASE
                        WHEN candle_history.low IS NULL THEN excluded.low
                        WHEN excluded.low IS NULL THEN candle_history.low
                        ELSE MIN(candle_history.low, excluded.low)
                    END,
                    close=excluded.close,
                    volume=COALESCE(excluded.volume, candle_history.volume),
                    updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    int(resolution_sec),
                    bucket_start_ts,
                    open_value,
                    high_value,
                    low_value,
                    close,
                    volume,
                    now_ts,
                ),
            )
        cleanup_key = (symbol, int(resolution_sec))
        cleanup_calls = self._candle_history_cleanup_counts.get(cleanup_key, 0) + 1
        self._candle_history_cleanup_counts[cleanup_key] = cleanup_calls
        last_cleanup_ts = self._candle_history_last_cleanup_ts.get(cleanup_key, 0.0)
        cleanup_due_by_count = cleanup_calls >= self._price_history_cleanup_every
        cleanup_due_by_time = (
            self._price_history_cleanup_min_interval_sec <= 0
            or (now_ts - last_cleanup_ts) >= self._price_history_cleanup_min_interval_sec
        )
        if cleanup_due_by_count and cleanup_due_by_time:
            keep = max(100, int(max_rows_per_symbol))
            self._conn.execute(
                """
                DELETE FROM candle_history
                WHERE symbol = ?
                  AND resolution_sec = ?
                  AND bucket_start_ts < COALESCE(
                        (SELECT bucket_start_ts
                         FROM candle_history
                         WHERE symbol = ? AND resolution_sec = ?
                         ORDER BY bucket_start_ts DESC
                         LIMIT 1 OFFSET ?),
                        -1
                  )
                """,
                (symbol, int(resolution_sec), symbol, int(resolution_sec), keep - 1),
            )
            self._candle_history_cleanup_counts[cleanup_key] = 0
            self._candle_history_last_cleanup_ts[cleanup_key] = now_ts

    def append_candle_sample(
        self,
        symbol: str,
        *,
        resolution_sec: int,
        ts: float,
        close: float,
        open_price: float | None = None,
        high_price: float | None = None,
        low_price: float | None = None,
        volume: float | None = None,
        max_rows_per_symbol: int = 5000,
    ) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        resolution = max(60, int(resolution_sec))
        now = time.time()

        def _write() -> None:
            self._upsert_candle_history_row(
                symbol=normalized_symbol,
                resolution_sec=resolution,
                ts=ts,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close=close,
                volume=volume,
                now_ts=now,
                max_rows_per_symbol=max_rows_per_symbol,
                overwrite_ohlc=True,
            )

        self._run_write_tx(_write)

    def load_recent_price_history(self, symbol: str, limit: int = 1000) -> list[dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, ts, close, volume
                FROM price_history
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (normalized_symbol, max(1, int(limit))),
            ).fetchall()
        items = [dict(row) for row in rows]
        items.reverse()
        return items

    def load_recent_candle_history(
        self,
        symbol: str,
        *,
        resolution_sec: int,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_resolution = max(60, int(resolution_sec))
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, resolution_sec, bucket_start_ts, open, high, low, close, volume, updated_at
                FROM candle_history
                WHERE symbol = ? AND resolution_sec = ?
                ORDER BY bucket_start_ts DESC
                LIMIT ?
                """,
                (normalized_symbol, normalized_resolution, max(1, int(limit))),
            ).fetchall()
        items: list[dict[str, Any]] = []
        resolution_value = normalized_resolution
        for row in reversed(rows):
            item = dict(row)
            bucket_start_ts = float(item.get("bucket_start_ts") or 0.0)
            close_value = float(item.get("close") or 0.0)
            for field_name in ("open", "high", "low"):
                value = item.get(field_name)
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    parsed = close_value
                if not math.isfinite(parsed) or parsed <= 0.0:
                    parsed = close_value
                item[field_name] = parsed
            item["ts"] = bucket_start_ts + float(resolution_value) - 1e-3
            items.append(item)
        return items

    def run_housekeeping(self, *, force: bool = False) -> dict[str, Any]:
        self.flush_event_async_writes()
        now = time.time()
        with self._lock:
            if (
                not force
                and self._housekeeping_interval_sec > 0
                and (now - self._last_housekeeping_ts) < self._housekeeping_interval_sec
            ):
                return {
                    "ran": False,
                    "events_deleted": 0,
                    "position_updates_deleted": 0,
                    "events_kept": self._housekeeping_events_keep_rows,
                    "position_updates_retention_sec": self._housekeeping_position_updates_retention_sec,
                    "checkpoint": None,
                }

            keep_events = max(100, int(self._housekeeping_events_keep_rows))
            events_deleted = 0
            position_updates_deleted = 0
            deleted_before = self._conn.total_changes
            with self._conn:
                self._conn.execute(
                    """
                    DELETE FROM events
                    WHERE id < COALESCE(
                        (SELECT id FROM events ORDER BY id DESC LIMIT 1 OFFSET ?),
                        -1
                    )
                    """,
                    (keep_events - 1,),
                )
                deleted_after_events = self._conn.total_changes
                events_deleted = max(0, deleted_after_events - deleted_before)
                cutoff_ts = now - self._housekeeping_position_updates_retention_sec
                self._conn.execute(
                    "DELETE FROM position_updates WHERE ts < ?",
                    (float(cutoff_ts),),
                )
                deleted_after_updates = self._conn.total_changes
                position_updates_deleted = max(0, deleted_after_updates - deleted_after_events)

            checkpoint_payload: dict[str, int] | None = None
            try:
                checkpoint_row = self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            except sqlite3.OperationalError as exc:
                logger.warning("housekeeping: WAL checkpoint failed: %s", exc)
                checkpoint_row = None
            if checkpoint_row is not None and len(checkpoint_row) >= 3:
                checkpoint_payload = {
                    "busy": int(checkpoint_row[0]),
                    "wal_frames": int(checkpoint_row[1]),
                    "checkpointed_frames": int(checkpoint_row[2]),
                }

            if self._housekeeping_incremental_vacuum_pages > 0:
                try:
                    self._conn.execute(
                        f"PRAGMA incremental_vacuum({int(self._housekeeping_incremental_vacuum_pages)})"
                    )
                except sqlite3.OperationalError as exc:
                    logger.warning("housekeeping: incremental vacuum failed: %s", exc)

            summary = {
                "ran": True,
                "ts": now,
                "events_deleted": events_deleted,
                "position_updates_deleted": position_updates_deleted,
                "events_kept": keep_events,
                "position_updates_retention_sec": self._housekeeping_position_updates_retention_sec,
                "checkpoint": checkpoint_payload,
            }
            with self._conn:
                self._upsert_kv_in_txn(
                    "db.housekeeping.last_summary",
                    json.dumps(summary, separators=(",", ":"), ensure_ascii=False),
                )
                self._upsert_kv_in_txn("db.housekeeping.last_ts", f"{now}")
            self._last_housekeeping_ts = now
            return summary

    def close(self) -> None:
        raw_flush = os.getenv("XTB_DB_CLOSE_FLUSH_TIMEOUT_SEC", "10")
        raw_join = os.getenv("XTB_DB_CLOSE_JOIN_TIMEOUT_SEC", "10")
        try:
            flush_timeout = max(1.0, float(raw_flush))
        except (TypeError, ValueError):
            flush_timeout = 10.0
        try:
            join_timeout = max(1.0, float(raw_join))
        except (TypeError, ValueError):
            join_timeout = 10.0

        if self._event_async_writer is not None and not self._closed:
            flushed = self.flush_event_async_writes(timeout_sec=flush_timeout)
            if not flushed:
                logger.error("state_store close: event async flush timed out, pending writes may be lost")
            writer = self._event_async_writer
            if writer is not None:
                _closed_flushed, joined = writer.close(
                    flush_timeout_sec=flush_timeout,
                    join_timeout_sec=join_timeout,
                )
                if not joined:
                    logger.error("state_store close: event async writer thread still alive after join timeout")
            self._event_async_writer = None
        if self._multi_async_writer is not None and not self._closed:
            flushed = self.flush_multi_async_writes(timeout_sec=flush_timeout)
            if not flushed:
                logger.error("state_store close: multi async flush timed out, pending writes may be lost")
            writer = self._multi_async_writer
            if writer is not None:
                _closed_flushed, joined = writer.close(
                    flush_timeout_sec=flush_timeout,
                    join_timeout_sec=join_timeout,
                )
                if not joined:
                    logger.error("state_store close: multi async writer thread still alive after join timeout")
            self._multi_async_writer = None
        with self._lock:
            if self._closed:
                return
            # Checkpoint WAL to fold all pending writes into main DB before closing.
            # This prevents stale WAL files from blocking the next startup.
            try:
                result = self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                logger.info(
                    "state_store close: WAL checkpoint result: blocked=%s, wal_pages=%s, checkpointed=%s",
                    result[0] if result else "?",
                    result[1] if result else "?",
                    result[2] if result else "?",
                )
            except Exception as exc:
                logger.warning("state_store close: WAL checkpoint failed: %s", exc)
            self._conn.close()
            self._closed = True
