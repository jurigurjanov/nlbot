from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, TypeVar
import uuid

from xtb_bot.models import AccountSnapshot, PendingOpen, Position, Side, WorkerState

_T = TypeVar("_T")


class StateStore:
    def __init__(
        self,
        db_path: Path,
        *,
        sqlite_timeout_sec: float = 30.0,
        price_history_cleanup_every: int = 100,
        price_history_cleanup_min_interval_sec: float = 30.0,
        housekeeping_interval_sec: float = 300.0,
        housekeeping_events_keep_rows: int = 50_000,
        housekeeping_incremental_vacuum_pages: int = 256,
    ):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sqlite_timeout_sec = max(1.0, float(sqlite_timeout_sec))
        self._price_history_cleanup_every = max(1, int(price_history_cleanup_every))
        self._price_history_cleanup_min_interval_sec = max(
            0.0,
            float(price_history_cleanup_min_interval_sec),
        )
        self._price_history_cleanup_counts: dict[str, int] = {}
        self._price_history_last_cleanup_ts: dict[str, float] = {}
        self._housekeeping_interval_sec = max(0.0, float(housekeeping_interval_sec))
        self._housekeeping_events_keep_rows = max(100, int(housekeeping_events_keep_rows))
        self._housekeeping_incremental_vacuum_pages = max(0, int(housekeeping_incremental_vacuum_pages))
        self._last_housekeeping_ts = 0.0
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            timeout=self._sqlite_timeout_sec,
        )
        self._conn.row_factory = sqlite3.Row
        self._closed = False
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            busy_timeout_ms = max(1_000, int(self._sqlite_timeout_sec * 1000))
            cur.executescript(
                f"""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                PRAGMA busy_timeout={busy_timeout_ms};
                PRAGMA auto_vacuum=INCREMENTAL;

                CREATE TABLE IF NOT EXISTS worker_states (
                    symbol TEXT PRIMARY KEY,
                    thread_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    strategy TEXT NOT NULL,
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
                    thread_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
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

                CREATE INDEX IF NOT EXISTS idx_trades_status_mode_opened_at
                    ON trades(status, mode, opened_at);
                CREATE INDEX IF NOT EXISTS idx_pending_opens_mode_created_at
                    ON pending_opens(mode, created_at);
                CREATE INDEX IF NOT EXISTS idx_events_ts
                    ON events(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_events_level_ts
                    ON events(level, ts DESC);

                CREATE TABLE IF NOT EXISTS position_audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    position_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    side TEXT,
                    volume REAL,
                    price REAL,
                    stop_loss REAL,
                    take_profit REAL,
                    pnl REAL,
                    deal_reference TEXT,
                    detail_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_position_audit_ts
                    ON position_audit_log(ts DESC);
                CREATE INDEX IF NOT EXISTS idx_position_audit_position_id
                    ON position_audit_log(position_id, ts DESC);
                """
            )
            self._ensure_column_exists(cur, "trades", "deal_reference", "TEXT")
            self._ensure_column_exists(cur, "trades", "entry_confidence", "REAL NOT NULL DEFAULT 0.0")
            self._conn.commit()

    @staticmethod
    def _ensure_column_exists(
        cursor: sqlite3.Cursor,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in rows}
        if column_name in existing:
            return
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

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
                    symbol, thread_name, mode, strategy,
                    last_price, last_heartbeat, iteration,
                    position_json, last_error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    thread_name=excluded.thread_name,
                    mode=excluded.mode,
                    strategy=excluded.strategy,
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

    def upsert_trade(self, position: Position, thread_name: str, strategy: str, mode: str) -> None:
        now = time.time()
        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO trades(
                    position_id, symbol, side, volume, open_price, stop_loss,
                    take_profit, opened_at, entry_confidence, status, close_price, closed_at,
                    pnl, thread_name, strategy, mode, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    symbol=excluded.symbol,
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
                    thread_name=excluded.thread_name,
                    strategy=excluded.strategy,
                    mode=excluded.mode,
                    updated_at=excluded.updated_at
                """,
                (
                    position.position_id,
                    position.symbol,
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
                    thread_name,
                    strategy,
                    mode,
                    now,
                ),
            )

        self._run_write_tx(_write)

    @staticmethod
    def _row_to_position(row_dict: dict[str, Any]) -> Position:
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
        )

    def load_open_positions(self, mode: str | None = None) -> dict[str, Position]:
        with self._lock:
            if mode is None:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status = 'open' ORDER BY opened_at ASC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM trades WHERE status = 'open' AND mode = ? ORDER BY opened_at ASC",
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
        def _write() -> None:
            self._conn.execute(
                """
                INSERT INTO pending_opens(
                    pending_id, position_id, symbol, side, volume, entry,
                    stop_loss, take_profit, created_at, thread_name, strategy,
                    mode, entry_confidence, trailing_override_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            trailing_override: dict[str, float] | None = None
            raw_override = row_dict.get("trailing_override_json")
            if raw_override:
                try:
                    payload = json.loads(raw_override)
                except json.JSONDecodeError:
                    payload = None
                if isinstance(payload, dict):
                    normalized: dict[str, float] = {}
                    for key, value in payload.items():
                        try:
                            normalized[str(key)] = float(value)
                        except (TypeError, ValueError):
                            continue
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

        params.append(str(position_id))
        query = f"UPDATE trades SET {', '.join(fields)} WHERE position_id = ?"

        def _write() -> None:
            self._conn.execute(query, tuple(params))

        self._run_write_tx(_write)

    def record_event(
        self,
        level: str,
        symbol: str | None,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        def _write() -> None:
            self._conn.execute(
                "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
                (
                    time.time(),
                    level,
                    symbol,
                    message,
                    json.dumps(payload) if payload else None,
                ),
            )

        self._run_write_tx(_write)

    def load_events(self, limit: int = 100) -> list[dict[str, Any]]:
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

            reservation_id = uuid.uuid4().hex
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
    ) -> None:
        normalized_symbol = str(symbol)
        now = time.time()
        max_future_skew_sec = 120.0

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

        self._run_write_tx(_write)

    def load_recent_price_history(self, symbol: str, limit: int = 1000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT symbol, ts, close, volume
                FROM price_history
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (symbol, max(1, int(limit))),
            ).fetchall()
        items = [dict(row) for row in rows]
        items.reverse()
        return items

    def run_housekeeping(self, *, force: bool = False) -> dict[str, Any]:
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
                    "events_kept": self._housekeeping_events_keep_rows,
                    "checkpoint": None,
                }

            keep_events = max(100, int(self._housekeeping_events_keep_rows))
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
            events_deleted = max(0, self._conn.total_changes - deleted_before)

            audit_deleted_before = self._conn.total_changes
            with self._conn:
                self._conn.execute(
                    "DELETE FROM position_audit_log WHERE ts < ?",
                    (now - 7 * 86400,),
                )
            audit_deleted = max(0, self._conn.total_changes - audit_deleted_before)

            checkpoint_payload: dict[str, int] | None = None
            try:
                checkpoint_row = self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            except sqlite3.OperationalError:
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
                except sqlite3.OperationalError:
                    pass

            summary = {
                "ran": True,
                "ts": now,
                "events_deleted": events_deleted,
                "events_kept": keep_events,
                "audit_log_purged": audit_deleted,
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

    def record_position_audit(
        self,
        position_id: str,
        symbol: str,
        operation: str,
        *,
        side: str | None = None,
        volume: float | None = None,
        price: float | None = None,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        pnl: float | None = None,
        deal_reference: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        now = time.time()
        detail_json = (
            json.dumps(detail, separators=(",", ":"), ensure_ascii=False, default=str)
            if detail
            else None
        )

        def _insert() -> None:
            self._conn.execute(
                """
                INSERT INTO position_audit_log
                    (ts, position_id, symbol, operation, side, volume,
                     price, stop_loss, take_profit, pnl, deal_reference, detail_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    str(position_id),
                    str(symbol),
                    str(operation),
                    side,
                    volume,
                    price,
                    stop_loss,
                    take_profit,
                    pnl,
                    deal_reference,
                    detail_json,
                ),
            )

        self._run_write_tx(_insert)

    def get_position_audit(
        self,
        limit: int = 100,
        symbol: str | None = None,
        position_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return recent position audit rows, newest first."""
        with self._lock:
            clauses: list[str] = []
            params: list[object] = []
            if symbol:
                clauses.append("symbol = ?")
                params.append(symbol.upper())
            if position_id:
                clauses.append("position_id = ?")
                params.append(position_id)
            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            params.append(limit)
            cur = self._conn.execute(
                f"SELECT * FROM position_audit_log{where} ORDER BY ts DESC LIMIT ?",
                params,
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def purge_position_audit(self, max_age_sec: float = 7 * 86400) -> int:
        cutoff = time.time() - max_age_sec

        def _purge() -> int:
            cur = self._conn.execute(
                "DELETE FROM position_audit_log WHERE ts < ?", (cutoff,)
            )
            return cur.rowcount

        return self._run_write_tx(_purge)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._conn.close()
            self._closed = True
