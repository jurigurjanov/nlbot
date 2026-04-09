from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import argparse
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from xtb_bot.bot import TradingBot
from xtb_bot.client import XtbApiClient
from xtb_bot.config import ConfigError, DEFAULT_STRATEGY_PARAMS, load_config, resolve_strategy_param
from xtb_bot.ig_client import IgApiClient
from xtb_bot.pip_size import symbol_pip_size_fallback
from xtb_bot.strategy_profiles import apply_strategy_profile, enforce_strategy_parameter_surface
from xtb_bot.strategies import available_strategies


_ACCOUNT_SNAPSHOT_STALE_MAX_AGE_SEC = 120.0
_STATUS_EVENTS_WINDOW = 2000
_STATUS_ACTIVITY_WINDOW_SEC = 2.0 * 3600.0
_STATUS_REASON_LIMIT = 5
_STATUS_LATEST_CLOSED_LIMIT = 5
_STATUS_STRATEGY_LIMIT = 6
_STATUS_STRATEGY_SUBTYPE_LIMIT = 8
_STATUS_UNAVAILABLE_LIMIT = 8
_STATUS_OPEN_TRADES_LIMIT = 8
_STATUS_WORKER_STALE_MAX_AGE_SEC = 180.0
_STATUS_TICK_STALE_MAX_AGE_SEC = 90.0
_SQLITE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _strip_matching_quotes(value: str) -> str:
    text = str(value)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def _quote_sqlite_identifier(name: str) -> str:
    normalized = str(name or "").strip()
    if not _SQLITE_IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"invalid sqlite identifier: {name!r}")
    return f'"{normalized}"'


def _load_dotenv(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    import os

    prefer_existing_env = str(os.getenv("BOT_DOTENV_PREFER_ENV", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = _strip_matching_quotes(value.strip())
        if not key:
            continue
        if prefer_existing_env and key in os.environ:
            continue
        if key:
            os.environ[key] = value


def _install_termination_handlers(bot: TradingBot):
    handled_signals: list[signal.Signals] = []
    original_handlers: dict[signal.Signals, object] = {}

    def _request_graceful_stop(signum, _frame) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except Exception:
            signal_name = str(signum)
        logging.getLogger(__name__).warning(
            "Termination signal received (%s); requesting graceful bot stop",
            signal_name,
        )
        request_stop = getattr(bot, "request_graceful_stop", None)
        if callable(request_stop):
            request_stop(reason=f"signal:{signal_name}", source="cli_signal_handler")
            return
        stop_event = getattr(bot, "stop_event", None)
        if stop_event is not None and hasattr(stop_event, "set"):
            stop_event.set()

    for candidate in ("SIGINT", "SIGTERM"):
        raw_signal = getattr(signal, candidate, None)
        if raw_signal is None:
            continue
        original_handlers[raw_signal] = signal.getsignal(raw_signal)
        signal.signal(raw_signal, _request_graceful_stop)
        handled_signals.append(raw_signal)

    def _restore_handlers() -> None:
        for handled_signal in handled_signals:
            original = original_handlers.get(handled_signal)
            if original is None:
                continue
            signal.signal(handled_signal, original)

    return _restore_handlers


def _parse_symbols_override(raw: str | None) -> list[str] | None:
    if raw in (None, ""):
        return None
    items = [part.strip().upper() for part in str(raw).split(",") if part.strip()]
    return items or None


def _argv_has_option(argv: list[str], option: str) -> bool:
    normalized = str(option or "").strip()
    if not normalized:
        return False
    option_eq = f"{normalized}="
    return any(item == normalized or item.startswith(option_eq) for item in argv)


def _validate_readonly_cli_dependencies(
    parser: argparse.ArgumentParser,
    argv: list[str],
) -> None:
    requirements = [
        ("--show-alerts", ["--alerts-limit"]),
        ("--show-trades", ["--trades-limit", "--trades-open-only", "--show-trades-live-sync", "--no-show-trades-live-sync"]),
        ("--show-trade-timeline", ["--trade-timeline-limit", "--trade-timeline-events-limit"]),
        ("--watch-open-trades", ["--watch-interval-sec", "--watch-iterations"]),
        ("--show-fill-quality", ["--fill-quality-limit"]),
        ("--show-strategy-scorecard", ["--strategy-scorecard-limit"]),
        ("--show-ig-allowance", ["--ig-allowance-window", "--ig-allowance-symbols"]),
        ("--show-db-first-health", ["--db-first-health-window", "--db-first-health-symbols"]),
        ("--show-trade-reasons", ["--trade-reasons-limit", "--trade-reasons-window", "--trade-reasons-only-rejects"]),
        ("--show-entry-attempts", ["--entry-attempts-limit", "--entry-attempts-window"]),
        ("--export-position-updates", ["--position-updates-limit"]),
        ("--reset-risk-anchor", ["--risk-anchor-value"]),
        ("--cleanup-incompatible-open-trades", ["--cleanup-dry-run"]),
    ]
    for primary, modifiers in requirements:
        if _argv_has_option(argv, primary):
            continue
        used = [option for option in modifiers if _argv_has_option(argv, option)]
        if used:
            parser.error(f"{', '.join(used)} requires {primary}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Multi-broker trading bot")
    parser.add_argument("--mode", help="Override mode: signal_only|paper|execution", default=None)
    parser.add_argument("--strategy", help="Override strategy name", default=None)
    parser.add_argument("--symbols", help="Override symbols list: comma-separated, e.g. US100,US500,BRENT", default=None)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force the selected strategy above schedule assignments for this run",
    )
    parser.add_argument(
        "--force-symbols",
        action="store_true",
        help="Force the selected symbols above schedule and strategy-scoped symbol sets for this run",
    )
    parser.add_argument(
        "--strategy-profile",
        choices=["safe", "conservative", "aggressive"],
        default=None,
        help=(
            "Apply built-in strategy profile "
            "(all strategies: safe|conservative|aggressive; "
            "safe aliases conservative)"
        ),
    )
    parser.add_argument(
        "--strict-broker-connect",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Fail startup on broker connect errors (disable mock fallback in paper/signal modes)",
    )
    parser.add_argument(
        "--ig-stream-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override IG Lightstreamer streaming: --ig-stream-enabled / --no-ig-stream-enabled",
    )
    parser.add_argument("--log-level", help="DEBUG|INFO|WARNING|ERROR", default="INFO")
    parser.add_argument("--list-strategies", action="store_true", help="List available strategies")
    parser.add_argument("--show-alerts", action="store_true", help="Show risk/broker alerts from SQLite state")
    parser.add_argument("--alerts-limit", type=int, default=20, help="Max alerts to print (default: 20)")
    parser.add_argument("--show-balance", action="store_true", help="Show latest account snapshot from SQLite state")
    parser.add_argument(
        "--show-status",
        action="store_true",
        help="Show state DB status (trades/events/snapshots + latest DB housekeeping summary)",
    )
    parser.add_argument(
        "--show-active-schedule",
        action="store_true",
        help="Show currently active strategy schedule assignments without starting the bot",
    )
    parser.add_argument("--show-trades", action="store_true", help="Show trades from SQLite state")
    parser.add_argument("--trades-limit", type=int, default=20, help="Max trades to print (default: 20)")
    parser.add_argument(
        "--show-trade-timeline",
        action="store_true",
        help="Show detailed timeline (events, position updates, MFE/MAE) for latest closed trades",
    )
    parser.add_argument(
        "--trade-timeline-limit",
        type=int,
        default=5,
        help="How many latest closed trades to analyze in --show-trade-timeline (default: 5)",
    )
    parser.add_argument(
        "--trade-timeline-events-limit",
        type=int,
        default=120,
        help="Max events/position-updates per trade in --show-trade-timeline (default: 120)",
    )
    parser.add_argument(
        "--watch-open-trades",
        action="store_true",
        help="Watch open trades live (refresh every --watch-interval-sec, default: 5s)",
    )
    parser.add_argument(
        "--watch-interval-sec",
        type=float,
        default=5.0,
        help="Refresh interval for --watch-open-trades (default: 5.0)",
    )
    parser.add_argument(
        "--watch-iterations",
        type=int,
        default=0,
        help="How many refresh cycles to run for --watch-open-trades (0 = infinite)",
    )
    parser.add_argument(
        "--show-trades-live-sync",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Before --show-trades, try broker live sync of open/closed execution trades "
            "(default: disabled; enable explicitly with --show-trades-live-sync)"
        ),
    )
    parser.add_argument(
        "--show-trade-confidence",
        action="store_true",
        help="Show closed-trade performance grouped by entry confidence buckets",
    )
    parser.add_argument(
        "--show-fill-quality",
        action="store_true",
        help="Show realized fill quality ranked by strategy entry and symbol",
    )
    parser.add_argument(
        "--fill-quality-limit",
        type=int,
        default=15,
        help="How many ranked rows to print for --show-fill-quality (default: 15)",
    )
    parser.add_argument(
        "--show-strategy-scorecard",
        action="store_true",
        help="Show management-grade alpha/execution scorecard by strategy entry",
    )
    parser.add_argument(
        "--strategy-scorecard-limit",
        type=int,
        default=12,
        help="How many ranked rows to print for --show-strategy-scorecard (default: 12)",
    )
    parser.add_argument(
        "--show-ig-allowance",
        action="store_true",
        help="Show IG API allowance diagnostics from SQLite events",
    )
    parser.add_argument(
        "--ig-allowance-window",
        type=int,
        default=1000,
        help="How many latest events to analyze for --show-ig-allowance (default: 1000)",
    )
    parser.add_argument(
        "--ig-allowance-symbols",
        type=int,
        default=10,
        help="How many symbols to print in --show-ig-allowance output (default: 10)",
    )
    parser.add_argument(
        "--show-db-first-health",
        action="store_true",
        help="Show DB-first cache health by symbol (tick age, worker state, latest refresh issue)",
    )
    parser.add_argument(
        "--db-first-health-window",
        type=int,
        default=2000,
        help="How many latest events to analyze for --show-db-first-health (default: 2000)",
    )
    parser.add_argument(
        "--db-first-health-symbols",
        type=int,
        default=20,
        help="How many symbols to print in --show-db-first-health output (default: 20)",
    )
    parser.add_argument(
        "--show-trade-reasons",
        action="store_true",
        help="Show aggregated trade reasons (close/block/hold) from SQLite events",
    )
    parser.add_argument(
        "--show-entry-attempts",
        action="store_true",
        help="Show latest trade entry attempts with accept/reject diagnostics",
    )
    parser.add_argument(
        "--export-position-updates",
        action="store_true",
        help="Export IG position-operation updates from SQLite as JSONL",
    )
    parser.add_argument(
        "--position-updates-limit",
        type=int,
        default=0,
        help="Max rows for --export-position-updates (0 = all)",
    )
    parser.add_argument(
        "--trade-reasons-limit",
        type=int,
        default=20,
        help="Max trade-reason groups to print (default: 20)",
    )
    parser.add_argument(
        "--trade-reasons-window",
        type=int,
        default=1000,
        help="How many latest events to analyze for --show-trade-reasons (default: 1000)",
    )
    parser.add_argument(
        "--trade-reasons-only-rejects",
        action="store_true",
        help="Show only broker rejection reasons (kind=reject) in --show-trade-reasons output",
    )
    parser.add_argument(
        "--entry-attempts-limit",
        type=int,
        default=20,
        help="Max entry attempts to print (default: 20)",
    )
    parser.add_argument(
        "--entry-attempts-window",
        type=int,
        default=2000,
        help="How many latest events to analyze for --show-entry-attempts (default: 2000)",
    )
    parser.add_argument(
        "--trades-open-only",
        action="store_true",
        help="Show only open trades (works with --show-trades)",
    )
    parser.add_argument(
        "--reset-risk-anchor",
        action="store_true",
        help="Reset risk.start_equity in state DB using latest account equity (or provided value)",
    )
    parser.add_argument(
        "--risk-anchor-value",
        type=float,
        default=None,
        help="Explicit value for --reset-risk-anchor (overrides latest account equity)",
    )
    parser.add_argument(
        "--cleanup-incompatible-open-trades",
        action="store_true",
        help=(
            "Archive open execution trades with incompatible IDs "
            "(position_id starts with paper- or mock-)"
        ),
    )
    parser.add_argument(
        "--sync-open-positions",
        action="store_true",
        help="Connect to broker, reconcile managed open positions, update SQLite, and exit",
    )
    parser.add_argument(
        "--cleanup-dry-run",
        action="store_true",
        help="Preview rows for --cleanup-incompatible-open-trades without writing to DB",
    )
    parser.add_argument(
        "--storage-path",
        default=None,
        help=(
            "SQLite state path override for "
            "--show-alerts/--show-balance/--show-status/--show-active-schedule/--show-trades/--show-ig-allowance/"
            "--show-db-first-health/--watch-open-trades/--show-trade-confidence/--show-trade-reasons/--show-entry-attempts/"
            "--show-fill-quality/--show-strategy-scorecard/--export-position-updates/--reset-risk-anchor/--cleanup-incompatible-open-trades/"
            "--sync-open-positions/--show-trade-timeline"
        ),
    )
    parser.add_argument("--dotenv", default=".env", help="Path to .env file")
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(raw_argv)
    _validate_readonly_cli_dependencies(parser, raw_argv)
    return args


def _resolve_storage_path(storage_path_override: str | None) -> Path:
    if storage_path_override:
        return Path(storage_path_override).expanduser()

    env_storage = os.getenv("BOT_STORAGE_PATH") or os.getenv("IG_STORAGE_PATH") or os.getenv("XTB_STORAGE_PATH")
    if env_storage:
        return Path(env_storage).expanduser()

    return Path("./state/xtb_bot.db")


def _sqlite_readonly_timeout_sec() -> float:
    raw = str(os.getenv("XTB_CLI_DB_TIMEOUT_SEC", "1.0"))
    try:
        return max(0.1, float(raw))
    except (TypeError, ValueError):
        return 1.0


def _connect_readonly_db(storage_path: Path) -> sqlite3.Connection:
    timeout_sec = _sqlite_readonly_timeout_sec()
    db_uri = f"file:{storage_path.expanduser()}?mode=ro"
    con = sqlite3.connect(db_uri, uri=True, timeout=timeout_sec)
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={max(100, int(timeout_sec * 1000))};")
    return con


def _load_events_readonly(storage_path: Path, limit: int) -> list[dict[str, object]]:
    max_items = max(1, int(limit))
    timeout_sec = _sqlite_readonly_timeout_sec()
    db_uri = f"file:{storage_path.expanduser()}?mode=ro"

    con = sqlite3.connect(db_uri, uri=True, timeout=timeout_sec)
    try:
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={max(100, int(timeout_sec * 1000))};")
        rows = con.execute(
            """
            SELECT id, ts, level, symbol, message, payload_json
            FROM events
            ORDER BY id DESC
            LIMIT ?
            """,
            (max_items,),
        ).fetchall()
    finally:
        con.close()

    events: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        payload_raw = item.get("payload_json")
        payload: dict[str, object] | None = None
        if isinstance(payload_raw, str) and payload_raw:
            try:
                parsed = json.loads(payload_raw)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                payload = parsed
        item["payload"] = payload
        events.append(item)
    return events


def _load_ig_rate_limit_workers_readonly(storage_path: Path) -> list[dict[str, object]]:
    timeout_sec = _sqlite_readonly_timeout_sec()
    db_uri = f"file:{storage_path.expanduser()}?mode=ro"

    con = sqlite3.connect(db_uri, uri=True, timeout=timeout_sec)
    try:
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={max(100, int(timeout_sec * 1000))};")
        rows = con.execute(
            """
            SELECT
                worker_key,
                limit_scope,
                limit_unit,
                limit_value,
                used_value,
                remaining_value,
                window_sec,
                blocked_total,
                last_request_ts,
                last_block_ts,
                notes,
                updated_at
            FROM ig_rate_limit_workers
            ORDER BY worker_key ASC
            """
        ).fetchall()
    finally:
        con.close()

    return [dict(row) for row in rows]


def _load_broker_ticks_readonly(storage_path: Path) -> list[dict[str, object]]:
    timeout_sec = _sqlite_readonly_timeout_sec()
    db_uri = f"file:{storage_path.expanduser()}?mode=ro"
    con = sqlite3.connect(db_uri, uri=True, timeout=timeout_sec)
    try:
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={max(100, int(timeout_sec * 1000))};")
        rows = con.execute(
            """
            SELECT symbol, bid, ask, mid, ts, source, updated_at
            FROM broker_ticks
            ORDER BY symbol ASC
            """
        ).fetchall()
    finally:
        con.close()
    return [dict(row) for row in rows]


def _load_worker_states_readonly(storage_path: Path) -> list[dict[str, object]]:
    timeout_sec = _sqlite_readonly_timeout_sec()
    db_uri = f"file:{storage_path.expanduser()}?mode=ro"
    con = sqlite3.connect(db_uri, uri=True, timeout=timeout_sec)
    try:
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={max(100, int(timeout_sec * 1000))};")
        rows = con.execute(
            """
            SELECT
                symbol,
                mode,
                strategy,
                strategy_base,
                position_strategy_entry,
                position_strategy_component,
                position_strategy_signal,
                position_json,
                last_heartbeat,
                last_error,
                updated_at
            FROM worker_states
            ORDER BY symbol ASC
            """
        ).fetchall()
    finally:
        con.close()
    return [dict(row) for row in rows]


def _show_alerts(storage_path: Path, limit: int) -> int:
    max_items = max(1, int(limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    try:
        # Load a wider window and then filter alerts.
        events = _load_events_readonly(storage_path, limit=max(max_items * 10, 100))
    except sqlite3.OperationalError as exc:
        print(f"State DB is busy, retry in a moment: {exc}")
        return 0

    alerts: list[dict[str, object]] = []
    for event in events:
        level = str(event.get("level", "")).upper()
        message = str(event.get("message", ""))
        message_lc = message.lower()
        if (
            level in {"WARN", "ERROR"}
            or "lock" in message_lc
            or "hold reason" in message_lc
        ):
            alerts.append(event)
        if len(alerts) >= max_items:
            break

    if not alerts:
        print("No alerts found.")
        return 0

    for event in alerts:
        ts = float(event.get("ts") or 0.0)
        ts_text = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        level = str(event.get("level") or "INFO")
        symbol = str(event.get("symbol") or "-")
        message = str(event.get("message") or "")
        print(f"[{ts_text}] {level} {symbol} | {message}")
        payload = event.get("payload")
        if payload:
            print(f"  payload: {json.dumps(payload, ensure_ascii=False, sort_keys=True)}")

    return len(alerts)


def _worker_state_strategy_labels(item: dict[str, object]) -> tuple[str, str | None]:
    strategy = str(item.get("strategy") or "").strip().lower() or "-"
    strategy_base = str(item.get("strategy_base") or "").strip().lower()
    if not strategy_base or strategy_base == strategy:
        return strategy, None
    return strategy, strategy_base


def _worker_state_position_label(item: dict[str, object]) -> str | None:
    raw_position = item.get("position_json")
    if not raw_position:
        return None
    try:
        position_payload = json.loads(str(raw_position))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(position_payload, dict):
        return None
    side = str(position_payload.get("side") or "").strip().upper()
    if not side:
        return None
    component = (
        str(item.get("position_strategy_component") or "").strip().lower()
        or str(item.get("position_strategy_entry") or "").strip().lower()
        or None
    )
    if component:
        return f"{side}/{component}"
    return side


def _show_balance(storage_path: Path) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        row = con.execute(
            """
            SELECT ts, balance, equity, margin_free, open_positions, daily_pnl, drawdown_pct
            FROM account_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    if row is None:
        print("No account snapshots found.")
        return 0

    item = dict(row)
    ts_text = datetime.fromtimestamp(float(item.get("ts") or 0.0)).strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{ts_text}] balance={float(item.get('balance') or 0.0):.2f} "
        f"equity={float(item.get('equity') or 0.0):.2f} "
        f"margin_free={float(item.get('margin_free') or 0.0):.2f} "
        f"open_positions={int(item.get('open_positions') or 0)} "
        f"daily_pnl={float(item.get('daily_pnl') or 0.0):.2f} "
        f"drawdown_pct={float(item.get('drawdown_pct') or 0.0):.2f}"
    )
    return 1


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    lowered = str(value or "").strip().lower()
    return lowered in {"1", "true", "yes", "on"}


def _stream_status_summary_from_db(con: sqlite3.Connection) -> dict[str, object]:
    configured_enabled = _as_bool(os.getenv("IG_STREAM_ENABLED", "true"))
    try:
        max_age_sec = max(30.0, float(os.getenv("XTB_CLI_STREAM_STATUS_MAX_AGE_SEC", "300")))
    except (TypeError, ValueError):
        max_age_sec = 300.0
    try:
        event_window = max(100, int(float(os.getenv("XTB_CLI_STREAM_STATUS_EVENT_WINDOW", "5000"))))
    except (TypeError, ValueError):
        event_window = 5000
    try:
        reasons_limit = max(1, int(float(os.getenv("XTB_CLI_STREAM_STATUS_REASONS_LIMIT", "5"))))
    except (TypeError, ValueError):
        reasons_limit = 5

    try:
        rows = con.execute(
            """
            SELECT id, ts, symbol, payload_json
            FROM events
            WHERE message = 'Stream usage metrics'
            ORDER BY id DESC
            LIMIT ?
            """,
            (event_window,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "configured_enabled": configured_enabled,
            "runtime_state": "unknown_events_unavailable",
            "symbols_total": 0,
            "symbols_connected": 0,
            "symbols_healthy": 0,
            "desired_subscriptions": None,
            "active_subscriptions": None,
            "latest_age_sec": None,
            "requests": None,
            "stream_hits": None,
            "rest_fallback_hits": None,
            "stream_hit_rate_pct": None,
            "reasons": [],
        }

    latest_payload: dict[str, object] | None = None
    latest_ts = 0.0
    latest_by_symbol: dict[str, dict[str, object]] = {}
    for row in rows:
        payload_raw = row["payload_json"]
        if not payload_raw:
            continue
        try:
            parsed = json.loads(str(payload_raw))
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        row_ts = float(row["ts"] or 0.0)
        if latest_payload is None:
            latest_payload = dict(parsed)
            latest_ts = row_ts
        symbol = str(row["symbol"] or "").strip().upper()
        if symbol and symbol not in latest_by_symbol:
            latest_by_symbol[symbol] = dict(parsed)

    def _as_int(value: object, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _as_float_or_none(value: object) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    latest_age_sec: float | None = None
    if latest_ts > 0:
        latest_age_sec = max(0.0, time.time() - latest_ts)

    connected_count = 0
    healthy_count = 0
    reason_counts: dict[str, int] = {}
    for payload in latest_by_symbol.values():
        connected = _as_bool(payload.get("stream_connected"))
        reason = str(payload.get("stream_reason") or "-").strip() or "-"
        if connected:
            connected_count += 1
            if reason == "ok" or reason.startswith("stream_ok"):
                healthy_count += 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    requests: int | None = None
    stream_hits: int | None = None
    rest_hits: int | None = None
    stream_hit_rate_pct: float | None = None
    desired_subscriptions: int | None = None
    active_subscriptions: int | None = None
    if latest_payload is not None:
        requests = _as_int(latest_payload.get("price_requests_total"), 0)
        stream_hits = _as_int(latest_payload.get("stream_hits_total"), 0)
        rest_hits = _as_int(latest_payload.get("rest_fallback_hits_total"), 0)
        stream_hit_rate_pct = _as_float_or_none(latest_payload.get("stream_hit_rate_pct"))
        if "desired_subscriptions" in latest_payload:
            desired_subscriptions = _as_int(latest_payload.get("desired_subscriptions"), 0)
        if "active_subscriptions" in latest_payload:
            active_subscriptions = _as_int(latest_payload.get("active_subscriptions"), 0)

    runtime_state = "unknown_no_metrics"
    if not configured_enabled:
        runtime_state = "disabled_by_config"
    elif latest_payload is None:
        runtime_state = "unknown_no_metrics"
    elif latest_age_sec is not None and latest_age_sec > max_age_sec:
        runtime_state = "unknown_stale_metrics"
    elif connected_count <= 0:
        runtime_state = "inactive_rest_only"
    elif healthy_count > 0:
        runtime_state = "active"
    else:
        runtime_state = "connected_fallback"

    reasons = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:reasons_limit]
    return {
        "configured_enabled": configured_enabled,
        "runtime_state": runtime_state,
        "symbols_total": len(latest_by_symbol),
        "symbols_connected": connected_count,
        "symbols_healthy": healthy_count,
        "desired_subscriptions": desired_subscriptions,
        "active_subscriptions": active_subscriptions,
        "latest_age_sec": latest_age_sec,
        "requests": requests,
        "stream_hits": stream_hits,
        "rest_fallback_hits": rest_hits,
        "stream_hit_rate_pct": stream_hit_rate_pct,
        "reasons": reasons,
    }


def _table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({_quote_sqlite_identifier(table_name)})").fetchall()
    except (sqlite3.OperationalError, ValueError):
        return set()
    return {str(row["name"]) for row in rows}


def _trade_strategy_display_expr(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    candidates: list[str] = []
    if "strategy_entry_component" in trade_columns:
        candidates.append(f"NULLIF({prefix}strategy_entry_component, '')")
    if "strategy_entry" in trade_columns:
        candidates.append(f"NULLIF({prefix}strategy_entry, '')")
    candidates.append(f"{prefix}strategy")
    if len(candidates) == 1:
        return candidates[0]
    return f"COALESCE({', '.join(candidates)})"


def _trade_strategy_signal_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_entry_signal" in trade_columns:
        return f"{prefix}strategy_entry_signal"
    return "NULL"


def _trade_strategy_subtype_expr(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    display_expr = _trade_strategy_display_expr(trade_columns, table_alias)
    if "strategy_entry_signal" not in trade_columns:
        return display_expr
    component_expr = (
        f"COALESCE(NULLIF({prefix}strategy_entry_component, ''), NULLIF({prefix}strategy_entry, ''), {prefix}strategy)"
    )
    signal_expr = f"NULLIF({prefix}strategy_entry_signal, '')"
    return (
        f"CASE WHEN {signal_expr} IS NOT NULL "
        f"THEN {component_expr} || ':' || {signal_expr} "
        f"ELSE {display_expr} END"
    )


def _trade_strategy_exit_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_exit" in trade_columns:
        return f"{prefix}strategy_exit"
    return "NULL"


def _trade_strategy_exit_component_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_exit_component" in trade_columns:
        return f"{prefix}strategy_exit_component"
    return "NULL"


def _trade_strategy_exit_signal_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_exit_signal" in trade_columns:
        return f"{prefix}strategy_exit_signal"
    return "NULL"


def _trade_strategy_component_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_entry_component" in trade_columns:
        return f"{prefix}strategy_entry_component"
    return "NULL"


def _trade_strategy_entry_base_select(trade_columns: set[str], table_alias: str = "t") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if "strategy_entry" in trade_columns:
        return f"{prefix}strategy_entry"
    return "NULL"


def _append_trade_strategy_filter(
    *,
    where_parts: list[str],
    query_params: list[object],
    trade_columns: set[str],
    strategy: str | None,
    table_alias: str = "t",
) -> None:
    if not strategy:
        return

    prefix = f"{table_alias}." if table_alias else ""
    predicates = [f"LOWER({prefix}strategy) = LOWER(?)"]
    params: list[object] = [str(strategy)]
    if "strategy_entry" in trade_columns:
        predicates.append(f"LOWER(COALESCE({prefix}strategy_entry, '')) = LOWER(?)")
        params.append(str(strategy))
    if "strategy_entry_component" in trade_columns:
        predicates.append(
            f"LOWER(COALESCE({prefix}strategy_entry_component, '')) = LOWER(?)"
        )
        params.append(str(strategy))
    if "strategy_exit" in trade_columns:
        predicates.append(f"LOWER(COALESCE({prefix}strategy_exit, '')) = LOWER(?)")
        params.append(str(strategy))
    if "strategy_exit_component" in trade_columns:
        predicates.append(f"LOWER(COALESCE({prefix}strategy_exit_component, '')) = LOWER(?)")
        params.append(str(strategy))

    where_parts.append(f"({' OR '.join(predicates)})")
    query_params.extend(params)


def _trade_strategy_labels(item: dict[str, object]) -> tuple[str, str, str | None, str]:
    carrier_strategy = str(item.get("strategy") or "").strip() or "-"
    entry_base_strategy = str(item.get("strategy_entry_base") or "").strip()
    entry_component_strategy = str(item.get("strategy_entry_component") or "").strip()
    explicit_exit_component = str(item.get("strategy_exit_component") or "").strip()
    explicit_exit_signal = str(item.get("strategy_exit_signal") or "").strip()
    display_strategy = (
        entry_component_strategy
        or entry_base_strategy
        or str(item.get("strategy_entry") or "").strip()
        or carrier_strategy
        or "-"
    )
    if display_strategy in {"", "-"}:
        display_strategy = carrier_strategy if carrier_strategy not in {"", "-"} else "-"
    raw_exit_strategy = str(item.get("strategy_exit") or "-").strip() or "-"
    close_reason = str(item.get("close_reason") or "").strip().lower()
    exit_strategy = explicit_exit_component or raw_exit_strategy
    if explicit_exit_component:
        exit_strategy = explicit_exit_component
    elif close_reason.startswith("strategy_exit:"):
        tail = close_reason.split("strategy_exit:", 1)[1]
        scope = str(tail.split(":", 1)[0] or "").strip().lower()
        if scope and scope not in {"protective"}:
            exit_strategy = scope
        elif entry_component_strategy:
            exit_strategy = entry_component_strategy
        elif entry_base_strategy:
            exit_strategy = entry_base_strategy
    elif close_reason.startswith("multi_strategy_emergency_mode") or close_reason.startswith("multi_"):
        exit_strategy = "multi_strategy_aggregator"
    elif (
        close_reason.startswith("profit_lock")
        or close_reason.startswith("early_loss_cut")
        or "reverse_signal:" in close_reason
        or "signal_invalidation:" in close_reason
        or close_reason.startswith("broker_manual_close:")
        or close_reason.startswith("broker_position_missing")
        or close_reason.endswith(":pending_broker_sync")
    ):
        if entry_component_strategy:
            exit_strategy = entry_component_strategy
        elif entry_base_strategy:
            exit_strategy = entry_base_strategy
    if not explicit_exit_component and explicit_exit_signal and ":" in explicit_exit_signal:
        explicit_scope = str(explicit_exit_signal.split(":", 1)[0] or "").strip()
        if explicit_scope and explicit_scope not in {"protective"}:
            exit_strategy = explicit_scope
    if carrier_strategy == "multi_strategy":
        base_strategy_display = (
            entry_base_strategy
            if entry_base_strategy not in {"", "-", display_strategy}
            else None
        )
    else:
        base_strategy_display = (
            carrier_strategy
            if carrier_strategy not in {"", "-", display_strategy}
            else None
        )
    return display_strategy, display_strategy, base_strategy_display, exit_strategy


def _decode_payload_json(payload_raw: object) -> dict[str, object] | None:
    if payload_raw in (None, ""):
        return None
    try:
        payload = json.loads(str(payload_raw))
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


_TRADE_TIMELINE_NOISE_MESSAGES = {
    "Stream usage metrics",
    "Trade deal reference bound",
    "Entry fill quality recorded",
    "Exit fill quality recorded",
}

_SHOW_TRADES_PNL_EVENT_SCAN_LIMIT = 2000
_SHOW_TRADES_PNL_EVENT_MESSAGES = {
    "Position closed",
    "Closed trade details backfilled from broker sync",
}
_TRADE_CLOSE_EVENT_LOOKBACK_SEC = 900.0
_TRADE_CLOSE_EVENT_LOOKAHEAD_SEC = 900.0


def _trade_timeline_event_matches_position(payload: dict[str, object] | None, position_id: str) -> bool:
    if not position_id:
        return True
    if not isinstance(payload, dict):
        return False
    payload_position_id = str(payload.get("position_id") or "").strip()
    if not payload_position_id:
        return False
    return payload_position_id == position_id


def _trade_event_realized_pnl(payload: dict[str, object] | None) -> float | None:
    if not isinstance(payload, dict):
        return None
    containers: list[dict[str, object]] = [payload]
    broker_sync = payload.get("broker_close_sync")
    if isinstance(broker_sync, dict):
        containers.append(broker_sync)
    for container in containers:
        for key in ("realized_pnl", "pnl"):
            parsed = _safe_float(container.get(key))
            if parsed is not None and (key != "pnl" or abs(parsed) > FLOAT_COMPARISON_TOLERANCE):
                return parsed
    parsed_local_estimate = _safe_float(payload.get("local_pnl_estimate"))
    if parsed_local_estimate is not None and abs(parsed_local_estimate) > FLOAT_COMPARISON_TOLERANCE:
        return parsed_local_estimate
    return None


def _show_trades_closed_pnl_from_event_rows(
    event_rows: list[sqlite3.Row],
    *,
    position_id: str,
) -> float | None:
    for event in event_rows:
        message = str(event["message"] or "").strip()
        if (
            message not in _SHOW_TRADES_PNL_EVENT_MESSAGES
            and not message.startswith("Local open position reconciled as closed during ")
        ):
            continue
        payload = _decode_payload_json(event["payload_json"])
        if not _trade_timeline_event_matches_position(payload, position_id):
            continue
        pnl = _trade_event_realized_pnl(payload)
        if pnl is not None:
            return pnl
    return None


def _load_trade_close_event_rows(
    con: sqlite3.Connection,
    *,
    symbol: str,
    position_id: str,
    opened_at: float | None,
    closed_at: float | None,
    updated_at: float | None,
) -> list[sqlite3.Row]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    lower_bound: float | None = None
    upper_bound: float | None = None
    if opened_at is not None and opened_at > 0.0:
        lower_bound = max(0.0, float(opened_at) - _TRADE_CLOSE_EVENT_LOOKBACK_SEC)
    elif closed_at is not None and closed_at > 0.0:
        lower_bound = max(0.0, float(closed_at) - _TRADE_CLOSE_EVENT_LOOKBACK_SEC)
    end_ts = None
    if closed_at is not None and closed_at > 0.0:
        end_ts = float(closed_at)
    elif updated_at is not None and updated_at > 0.0:
        end_ts = float(updated_at)
    if end_ts is not None:
        upper_bound = end_ts + _TRADE_CLOSE_EVENT_LOOKAHEAD_SEC
    if lower_bound is not None and upper_bound is not None and upper_bound >= lower_bound:
        rows = con.execute(
            """
            SELECT ts, id, message, payload_json
            FROM events
            WHERE symbol = ? AND ts >= ? AND ts <= ?
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (normalized_symbol, lower_bound, upper_bound, _SHOW_TRADES_PNL_EVENT_SCAN_LIMIT),
        ).fetchall()
        if rows:
            return rows
    normalized_position_id = str(position_id or "").strip()
    if normalized_position_id:
        escaped_position_id = (
            normalized_position_id
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        rows = con.execute(
            """
            SELECT ts, id, message, payload_json
            FROM events
            WHERE symbol = ? AND payload_json LIKE ? ESCAPE '\\'
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (normalized_symbol, f"%{escaped_position_id}%", _SHOW_TRADES_PNL_EVENT_SCAN_LIMIT),
        ).fetchall()
        if rows:
            return rows
    return con.execute(
        """
        SELECT ts, id, message, payload_json
        FROM events
        WHERE symbol = ?
        ORDER BY ts DESC, id DESC
        LIMIT ?
        """,
        (normalized_symbol, _SHOW_TRADES_PNL_EVENT_SCAN_LIMIT),
    ).fetchall()


def _hydrate_closed_trade_rows_with_event_pnl(
    con: sqlite3.Connection,
    items: list[dict[str, object]],
    *,
    table_names: set[str],
) -> None:
    if "events" not in table_names:
        return
    for item in items:
        status_value = str(item.get("status") or "").strip().lower()
        if status_value and status_value != "closed":
            continue
        current_pnl = _safe_float(item.get("pnl"))
        if current_pnl is not None and abs(current_pnl) > FLOAT_COMPARISON_TOLERANCE:
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        position_id = str(item.get("position_id") or "").strip()
        if not symbol or not position_id:
            continue
        event_rows = _load_trade_close_event_rows(
            con,
            symbol=symbol,
            position_id=position_id,
            opened_at=_safe_float(item.get("opened_at")),
            closed_at=_safe_float(item.get("closed_at")),
            updated_at=_safe_float(item.get("updated_at")),
        )
        fallback_pnl = _show_trades_closed_pnl_from_event_rows(
            event_rows,
            position_id=position_id,
        )
        if fallback_pnl is not None:
            item["pnl"] = fallback_pnl


def _trade_timeline_event_extra(message: str, payload: dict[str, object] | None) -> str:
    if not isinstance(payload, dict):
        return ""

    fragments: list[str] = []
    reason = str(payload.get("reason") or "").strip()
    if reason:
        fragments.append(f"reason={reason}")

    if message == "Position opened":
        strategy = (
            str(payload.get("strategy_entry_component") or payload.get("strategy_entry") or "").strip()
        )
        strategy_base = str(payload.get("strategy_base") or "").strip()
        confidence = payload.get("confidence")
        if strategy:
            fragments.append(f"strategy={strategy}")
        if strategy_base and strategy_base not in {"", strategy}:
            fragments.append(f"strategy_base={strategy_base}")
        try:
            if confidence not in (None, ""):
                fragments.append(f"conf={float(confidence):.3f}")
        except (TypeError, ValueError):
            pass
    elif message == "Position closed":
        strategy = (
            str(payload.get("strategy_entry_component") or payload.get("strategy_entry") or "").strip()
        )
        strategy_base = str(payload.get("strategy_base") or "").strip()
        strategy_exit = str(payload.get("strategy_exit") or "").strip()
        strategy_exit_signal = str(payload.get("strategy_exit_signal") or "").strip()
        if strategy:
            fragments.append(f"strategy={strategy}")
        if strategy_base and strategy_base not in {"", strategy}:
            fragments.append(f"strategy_base={strategy_base}")
        if strategy_exit and strategy_exit not in {"", "-"}:
            fragments.append(f"strategy_exit={strategy_exit}")
        if strategy_exit_signal and strategy_exit_signal not in {"", "-", strategy_exit}:
            fragments.append(f"strategy_exit_signal={strategy_exit_signal}")
        try:
            pnl = payload.get("pnl")
            if pnl not in (None, ""):
                fragments.append(f"pnl={float(pnl):.2f}")
        except (TypeError, ValueError):
            pass
        try:
            close_price = payload.get("close_price")
            if close_price not in (None, ""):
                fragments.append(f"close_price={float(close_price):.5f}")
        except (TypeError, ValueError):
            pass
    elif message == "Partial take-profit executed":
        for key in ("close_volume", "remaining_volume"):
            value = payload.get(key)
            try:
                if value not in (None, ""):
                    fragments.append(f"{key}={float(value):.4f}")
            except (TypeError, ValueError):
                continue

    if not fragments:
        return ""
    return " " + " ".join(fragments)


def _trade_timeline_events(
    event_rows: list[sqlite3.Row],
    *,
    position_id: str,
    limit: int,
) -> tuple[list[dict[str, object]], int, str | None]:
    selected: list[dict[str, object]] = []
    close_reason: str | None = None

    for event in event_rows:
        payload = _decode_payload_json(event["payload_json"])
        if not _trade_timeline_event_matches_position(payload, position_id):
            continue

        message = str(event["message"] or "-")
        if message == "Position closed" and isinstance(payload, dict):
            reason = str(payload.get("reason") or "").strip()
            if reason:
                close_reason = reason

        selected.append(
            {
                "ts": float(event["ts"] or 0.0),
                "level": str(event["level"] or "-"),
                "message": message,
                "payload": payload,
            }
        )

    return selected[: max(1, int(limit))], 0, close_reason


def _summarize_position_updates(update_rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    buckets: list[dict[str, object]] = []
    index_by_signature: dict[tuple[object, ...], int] = {}

    for update in update_rows:
        ts = float(update["ts"] or 0.0)
        signature = (
            str(update["operation"] or ""),
            str(update["method"] or ""),
            str(update["path"] or ""),
            update["http_status"],
            int(update["success"] or 0),
            str(update["source"] or ""),
            str(update["error_text"] or ""),
            str(update["strategy"] or ""),
            str(update["strategy_base"] or ""),
            str(update["strategy_entry_component"] or update["strategy_entry"] or ""),
            str(update["mode"] or ""),
        )
        bucket_index = index_by_signature.get(signature)
        if bucket_index is None:
            index_by_signature[signature] = len(buckets)
            buckets.append(
                {
                    "first_ts": ts,
                    "last_ts": ts,
                    "count": 1,
                    "operation": signature[0],
                    "method": signature[1],
                    "path": signature[2],
                    "http_status": signature[3],
                    "success": signature[4],
                    "source": signature[5],
                    "error_text": signature[6],
                    "strategy": signature[7],
                    "strategy_base": signature[8],
                    "strategy_entry_component": signature[9],
                    "mode": signature[10],
                }
            )
            continue

        bucket = buckets[bucket_index]
        bucket["count"] = int(bucket["count"]) + 1
        bucket["last_ts"] = ts

    return buckets


def _trade_performance_summary_from_db(con: sqlite3.Connection) -> dict[str, object] | None:
    table_names = {
        str(row["name"])
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if "trade_performance" not in table_names:
        return None

    aggregates = con.execute(
        """
        SELECT
            COUNT(*) AS closed_count,
            AVG(tp.max_favorable_pnl) AS avg_mfe_pnl,
            AVG(tp.max_adverse_pnl) AS avg_mae_pnl,
            AVG(
                CASE
                    WHEN tp.max_favorable_pnl > 0.0 THEN t.pnl / tp.max_favorable_pnl
                    ELSE NULL
                END
            ) AS avg_capture_ratio
        FROM trades t
        JOIN trade_performance tp ON tp.position_id = t.position_id
        WHERE t.status = 'closed'
        """
    ).fetchone()
    latest = con.execute(
        """
        SELECT
            t.position_id,
            t.symbol,
            t.pnl,
            t.closed_at,
            tp.close_reason,
            tp.max_favorable_pips,
            tp.max_adverse_pips,
            tp.max_favorable_pnl,
            tp.max_adverse_pnl
        FROM trades t
        JOIN trade_performance tp ON tp.position_id = t.position_id
        WHERE t.status = 'closed'
        ORDER BY COALESCE(t.closed_at, t.updated_at, t.opened_at) DESC, t.position_id DESC
        LIMIT 1
        """
    ).fetchone()
    if aggregates is None:
        return None
    return {
        "closed_count": int(aggregates["closed_count"] or 0),
        "avg_mfe_pnl": (
            float(aggregates["avg_mfe_pnl"])
            if aggregates["avg_mfe_pnl"] not in (None, "")
            else None
        ),
        "avg_mae_pnl": (
            float(aggregates["avg_mae_pnl"])
            if aggregates["avg_mae_pnl"] not in (None, "")
            else None
        ),
        "avg_capture_ratio": (
            float(aggregates["avg_capture_ratio"])
            if aggregates["avg_capture_ratio"] not in (None, "")
            else None
        ),
        "latest": (dict(latest) if latest is not None else None),
    }


def _extract_multi_strategy_emergency_marker(last_error: object) -> str | None:
    text = str(last_error or "").strip()
    if not text:
        return None
    for part in text.split("|"):
        token = part.strip()
        if token.startswith("multi_strategy_emergency_mode"):
            return token
    return None


def _multi_strategy_emergency_summary_from_db(con: sqlite3.Connection) -> dict[str, object]:
    try:
        rows = con.execute(
            """
            SELECT symbol, last_error, updated_at
            FROM worker_states
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "active": False,
            "count": 0,
            "symbols": [],
            "reasons": [],
            "latest_age_sec": None,
        }

    active_entries: list[tuple[str, str, float]] = []
    for row in rows:
        marker = _extract_multi_strategy_emergency_marker(row["last_error"])
        if marker is None:
            continue
        symbol = str(row["symbol"] or "").strip().upper() or "UNKNOWN"
        reason = "unknown"
        if ":" in marker:
            _prefix, suffix = marker.split(":", 1)
            parsed_reason = str(suffix or "").strip()
            if parsed_reason:
                reason = parsed_reason
        updated_at = float(row["updated_at"] or 0.0)
        active_entries.append((symbol, reason, updated_at))

    if not active_entries:
        return {
            "active": False,
            "count": 0,
            "symbols": [],
            "reasons": [],
            "latest_age_sec": None,
        }

    reason_counts: dict[str, int] = {}
    for _symbol, reason, _updated_at in active_entries:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    reasons = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
    symbols = sorted({symbol for symbol, _reason, _updated_at in active_entries})
    latest_updated_at = max((updated_at for _symbol, _reason, updated_at in active_entries), default=0.0)
    latest_age_sec: float | None = None
    if latest_updated_at > 0:
        latest_age_sec = max(0.0, time.time() - latest_updated_at)
    return {
        "active": True,
        "count": len(active_entries),
        "symbols": symbols,
        "reasons": reasons,
        "latest_age_sec": latest_age_sec,
    }


def _status_parse_multi_hold_reasons(payload: dict[str, object] | None) -> list[tuple[str, str]]:
    if not isinstance(payload, dict):
        return []
    metadata = payload.get("metadata")
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    raw = metadata_dict.get("multi_hold_reason_by_strategy")
    if raw in (None, ""):
        raw = payload.get("multi_hold_reason_by_strategy")
    text = str(raw or "").strip()
    if not text:
        return []
    rows: list[tuple[str, str]] = []
    for chunk in text.split(";"):
        token = str(chunk or "").strip()
        if not token or "=" not in token:
            continue
        strategy_name, reason = token.split("=", 1)
        strategy_text = str(strategy_name or "").strip()
        reason_text = str(reason or "").strip()
        if strategy_text and reason_text:
            rows.append((strategy_text, reason_text))
    return rows


def _status_payload_metadata(payload: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}
    metadata = payload.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _status_parse_reason_count_text(text: object) -> list[tuple[str, int]]:
    raw = str(text or "").strip()
    if not raw:
        return []
    rows: list[tuple[str, int]] = []
    for chunk in raw.split(","):
        token = str(chunk or "").strip()
        if not token:
            continue
        count = 1
        reason = token
        if token.endswith(")") and "(" in token:
            prefix, suffix = token.rsplit("(", 1)
            suffix_text = suffix[:-1].strip()
            if suffix_text.isdigit():
                reason = prefix.strip()
                count = max(1, int(suffix_text))
        if reason:
            rows.append((reason, count))
    return rows


def _status_parse_strategy_reason_rows(
    payload: dict[str, object] | None,
    field_name: str,
) -> list[tuple[str, str, int]]:
    metadata_dict = _status_payload_metadata(payload)
    raw = metadata_dict.get(field_name)
    if raw in (None, "") and isinstance(payload, dict):
        raw = payload.get(field_name)
    text = str(raw or "").strip()
    if not text:
        return []
    rows: list[tuple[str, str, int]] = []
    for chunk in text.split(";"):
        token = str(chunk or "").strip()
        if not token or "=" not in token:
            continue
        strategy_name, reason_text = token.split("=", 1)
        strategy = str(strategy_name or "").strip().lower()
        reason_rows = _status_parse_reason_count_text(reason_text)
        if not reason_rows:
            reason = str(reason_text or "").strip().lower()
            if strategy and reason:
                rows.append((strategy, reason, 1))
            continue
        for reason, count in reason_rows:
            if strategy and reason:
                rows.append((strategy, str(reason).strip().lower(), int(count)))
    return rows


def _status_entry_component_from_payload(payload: dict[str, object] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    metadata = payload.get("metadata")
    metadata_dict = metadata if isinstance(metadata, dict) else {}
    for source in (metadata_dict, payload):
        for key in (
            "multi_entry_strategy_component",
            "multi_dominant_component_strategy",
            "multi_representative_strategy",
            "strategy",
            "indicator",
        ):
            value = str(source.get(key) or "").strip()
            if value:
                return value
    return None


def _status_recent_trade_flow_summary_from_db(con: sqlite3.Connection) -> dict[str, object]:
    try:
        rows = con.execute(
            """
            SELECT id, ts, level, symbol, message, payload_json
            FROM events
            ORDER BY id DESC
            LIMIT ?
            """,
            (_STATUS_EVENTS_WINDOW,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {
            "events_scanned": 0,
            "opened_recent": 0,
            "closed_recent": 0,
            "hold_count": 0,
            "block_count": 0,
            "reject_count": 0,
            "top_reasons": [],
            "top_component_holds": [],
            "top_component_blocked_holds": [],
            "top_component_unavailable_holds": [],
            "top_component_soft_filters": [],
            "spike_hold_count": 0,
            "top_spike_symbols": [],
            "top_spike_components": [],
        }

    cutoff_ts = time.time() - _STATUS_ACTIVITY_WINDOW_SEC
    aggregates: dict[tuple[str, str], dict[str, object]] = {}
    component_holds: dict[tuple[str, str], int] = {}
    component_blocked_holds: dict[tuple[str, str], int] = {}
    component_unavailable_holds: dict[tuple[str, str], int] = {}
    component_soft_filters: dict[tuple[str, str], int] = {}
    spike_symbols: dict[str, int] = {}
    spike_components: dict[str, int] = {}
    opened_recent = 0
    closed_recent = 0
    hold_count = 0
    block_count = 0
    reject_count = 0
    spike_hold_count = 0

    for row in rows:
        ts = _as_float(row["ts"], 0.0)
        if ts <= 0.0 or ts < cutoff_ts:
            continue
        payload = _decode_payload_json(row["payload_json"])
        event = {
            "message": row["message"],
            "payload": payload,
            "symbol": row["symbol"],
            "ts": ts,
        }
        message = str(row["message"] or "")
        if message == "Position opened":
            opened_recent += 1
        elif message == "Position closed":
            closed_recent += 1
        reason_key = _event_reason(event)
        if reason_key is None:
            continue
        kind, reason = reason_key
        if kind == "hold":
            hold_count += 1
            metadata_dict = _status_payload_metadata(payload)
            hold_state = str(
                metadata_dict.get("hold_state")
                or payload.get("hold_state")
                or ""
            ).strip().lower()
            for strategy_name, hold_reason in _status_parse_multi_hold_reasons(payload):
                key = (strategy_name, hold_reason)
                component_holds[key] = component_holds.get(key, 0) + 1
            for strategy_name, hold_reason, count in _status_parse_strategy_reason_rows(
                payload,
                "multi_blocked_reason_by_strategy",
            ):
                key = (strategy_name, hold_reason)
                component_blocked_holds[key] = component_blocked_holds.get(key, 0) + count
            for strategy_name, hold_reason, count in _status_parse_strategy_reason_rows(
                payload,
                "multi_unavailable_reason_by_strategy",
            ):
                key = (strategy_name, hold_reason)
                component_unavailable_holds[key] = component_unavailable_holds.get(key, 0) + count
            for strategy_name, soft_reason, count in _status_parse_strategy_reason_rows(
                payload,
                "multi_soft_reason_by_strategy",
            ):
                key = (strategy_name, soft_reason)
                component_soft_filters[key] = component_soft_filters.get(key, 0) + count
            strategy_label = _status_entry_component_from_payload(payload)
            if strategy_label and strategy_label not in {"multi_strategy", ""}:
                normalized_strategy = str(strategy_label).strip().lower()
                if hold_state == "blocked":
                    key = (normalized_strategy, reason)
                    component_blocked_holds[key] = component_blocked_holds.get(key, 0) + 1
                elif hold_state == "unavailable":
                    key = (normalized_strategy, reason)
                    component_unavailable_holds[key] = component_unavailable_holds.get(key, 0) + 1
                soft_reasons = metadata_dict.get("soft_filter_reasons")
                if isinstance(soft_reasons, (list, tuple)):
                    for raw_soft_reason in soft_reasons:
                        soft_reason = str(raw_soft_reason or "").strip().lower()
                        if not soft_reason:
                            continue
                        key = (normalized_strategy, soft_reason)
                        component_soft_filters[key] = component_soft_filters.get(key, 0) + 1
            if reason == "entry_spike_detected":
                spike_hold_count += 1
                symbol = str(row["symbol"] or "").strip().upper()
                if symbol:
                    spike_symbols[symbol] = spike_symbols.get(symbol, 0) + 1
                component = _status_entry_component_from_payload(payload)
                if component:
                    spike_components[component] = spike_components.get(component, 0) + 1
        elif kind == "block":
            block_count += 1
        elif kind == "reject":
            reject_count += 1
        key = (kind, reason)
        if key not in aggregates:
            aggregates[key] = {
                "count": 0,
                "last_ts": 0.0,
                "symbols": set(),
            }
        bucket = aggregates[key]
        bucket["count"] = int(bucket["count"]) + 1
        bucket["last_ts"] = max(float(bucket["last_ts"] or 0.0), ts)
        symbol = str(row["symbol"] or "").strip().upper()
        if symbol:
            symbols = bucket["symbols"]
            if isinstance(symbols, set):
                symbols.add(symbol)

    top_reasons: list[dict[str, object]] = []
    for (kind, reason), bucket in sorted(
        aggregates.items(),
        key=lambda item: (-int(item[1]["count"]), -float(item[1]["last_ts"]), item[0][0], item[0][1]),
    )[:_STATUS_REASON_LIMIT]:
        symbols_value = bucket.get("symbols")
        symbols = sorted(str(item) for item in symbols_value) if isinstance(symbols_value, set) else []
        top_reasons.append(
            {
                "kind": kind,
                "reason": reason,
                "count": int(bucket["count"]),
                "symbols": symbols,
            }
        )

    top_component_holds = [
        {
            "strategy": strategy_name,
            "reason": hold_reason,
            "count": count,
        }
        for (strategy_name, hold_reason), count in sorted(
            component_holds.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )[:_STATUS_REASON_LIMIT]
    ]
    top_component_blocked_holds = [
        {
            "strategy": strategy_name,
            "reason": hold_reason,
            "count": count,
        }
        for (strategy_name, hold_reason), count in sorted(
            component_blocked_holds.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )[:_STATUS_REASON_LIMIT]
    ]
    top_component_unavailable_holds = [
        {
            "strategy": strategy_name,
            "reason": hold_reason,
            "count": count,
        }
        for (strategy_name, hold_reason), count in sorted(
            component_unavailable_holds.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )[:_STATUS_REASON_LIMIT]
    ]
    top_component_soft_filters = [
        {
            "strategy": strategy_name,
            "reason": soft_reason,
            "count": count,
        }
        for (strategy_name, soft_reason), count in sorted(
            component_soft_filters.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )[:_STATUS_REASON_LIMIT]
    ]

    top_spike_symbols = [
        {
            "symbol": symbol,
            "count": count,
        }
        for symbol, count in sorted(
            spike_symbols.items(),
            key=lambda item: (-item[1], item[0]),
        )[:_STATUS_REASON_LIMIT]
    ]
    top_spike_components = [
        {
            "component": component,
            "count": count,
        }
        for component, count in sorted(
            spike_components.items(),
            key=lambda item: (-item[1], item[0]),
        )[:_STATUS_REASON_LIMIT]
    ]

    return {
        "events_scanned": len(rows),
        "opened_recent": opened_recent,
        "closed_recent": closed_recent,
        "hold_count": hold_count,
        "block_count": block_count,
        "reject_count": reject_count,
        "top_reasons": top_reasons,
        "top_component_holds": top_component_holds,
        "top_component_blocked_holds": top_component_blocked_holds,
        "top_component_unavailable_holds": top_component_unavailable_holds,
        "top_component_soft_filters": top_component_soft_filters,
        "spike_hold_count": spike_hold_count,
        "top_spike_symbols": top_spike_symbols,
        "top_spike_components": top_spike_components,
    }


def _request_limit_status_from_db(con: sqlite3.Connection) -> dict[str, object]:
    try:
        rows = con.execute(
            """
            SELECT
                worker_key,
                limit_scope,
                limit_unit,
                limit_value,
                used_value,
                remaining_value,
                window_sec,
                blocked_total,
                last_request_ts,
                last_block_ts,
                notes,
                updated_at
            FROM ig_rate_limit_workers
            ORDER BY worker_key ASC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {"rows": [], "hot": None}

    now_ts = time.time()
    items: list[dict[str, object]] = []
    for row in rows:
        limit_value = max(0, int(_as_float(row["limit_value"], 0.0)))
        used_value = max(0, int(_as_float(row["used_value"], 0.0)))
        remaining_value = max(0, int(_as_float(row["remaining_value"], 0.0)))
        blocked_total = max(0, int(_as_float(row["blocked_total"], 0.0)))
        window_sec = max(1.0, _as_float(row["window_sec"], 60.0))
        updated_at = _as_float(row["updated_at"], 0.0)
        last_block_ts = _as_float(row["last_block_ts"], 0.0)
        utilization_pct = (used_value / limit_value * 100.0) if limit_value > 0 else 0.0
        blocked_recent = last_block_ts > 0 and (now_ts - last_block_ts) <= max(120.0, window_sec * 2.0)
        stale = updated_at > 0 and (now_ts - updated_at) > max(120.0, window_sec * 2.0)
        score = utilization_pct + min(50.0, blocked_total * 0.1)
        if blocked_recent:
            score += 35.0
        if stale:
            score += 20.0
        status_parts: list[str] = []
        if blocked_recent:
            status_parts.append("recent_blocks")
        if utilization_pct >= 90.0:
            status_parts.append("high_utilization")
        elif utilization_pct >= 75.0:
            status_parts.append("elevated_utilization")
        if stale:
            status_parts.append("stale_metrics")
        items.append(
            {
                "worker_key": str(row["worker_key"] or "").strip() or "unknown",
                "limit_scope": str(row["limit_scope"] or "").strip() or "-",
                "limit_unit": str(row["limit_unit"] or "").strip() or "-",
                "limit_value": limit_value,
                "used_value": used_value,
                "remaining_value": remaining_value,
                "blocked_total": blocked_total,
                "window_sec": window_sec,
                "updated_at": updated_at,
                "utilization_pct": utilization_pct,
                "score": score,
                "status": "+".join(status_parts) if status_parts else "normal",
            }
        )

    items.sort(key=lambda item: (-float(item["score"]), str(item["worker_key"])))
    return {
        "rows": items,
        "hot": items[0] if items else None,
    }


def _strategy_status_summary_from_db(
    con: sqlite3.Connection,
    *,
    use_subtype: bool = False,
) -> list[dict[str, object]]:
    trade_columns = _table_columns(con, "trades")
    if not trade_columns:
        return []
    table_names = {
        str(row["name"])
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    label_key = "strategy_subtype" if use_subtype else "strategy_entry"
    display_expr = (
        _trade_strategy_subtype_expr(trade_columns, "t")
        if use_subtype
        else _trade_strategy_display_expr(trade_columns, "t")
    )
    has_trade_performance = "trade_performance" in table_names
    join_clause = "LEFT JOIN trade_performance tp ON tp.position_id = t.position_id" if has_trade_performance else ""
    capture_expr = (
        """
        AVG(
            CASE
                WHEN tp.max_favorable_pnl > 0.0 THEN t.pnl / tp.max_favorable_pnl
                ELSE NULL
            END
        ) AS avg_capture_ratio
        """
        if has_trade_performance
        else "NULL AS avg_capture_ratio"
    )
    total_closed_row = con.execute(
        "SELECT COUNT(*) AS cnt FROM trades WHERE status = 'closed'"
    ).fetchone()
    total_closed = int((total_closed_row["cnt"] if total_closed_row is not None else 0) or 0)
    rows = con.execute(
        f"""
        SELECT
            {display_expr} AS {label_key},
            COUNT(*) AS sample_count,
            SUM(CASE WHEN COALESCE(t.pnl, 0.0) > 0.0 THEN 1 ELSE 0 END) AS win_count,
            SUM(COALESCE(t.pnl, 0.0)) AS pnl_sum,
            AVG(COALESCE(t.pnl, 0.0)) AS avg_pnl,
            {capture_expr}
        FROM trades t
        {join_clause}
        WHERE t.status = 'closed'
        GROUP BY {label_key}
        ORDER BY sample_count DESC, pnl_sum DESC, {label_key} ASC
        LIMIT ?
        """,
        ((_STATUS_STRATEGY_SUBTYPE_LIMIT if use_subtype else _STATUS_STRATEGY_LIMIT),),
    ).fetchall()
    items: list[dict[str, object]] = []
    for row in rows:
        sample_count = int(row["sample_count"] or 0)
        if sample_count <= 0:
            continue
        win_count = int(row["win_count"] or 0)
        items.append(
            {
                label_key: str(row[label_key] or "-"),
                "sample_count": sample_count,
                "share_pct": (sample_count / total_closed * 100.0) if total_closed > 0 else 0.0,
                "win_rate_pct": (win_count / sample_count * 100.0) if sample_count > 0 else 0.0,
                "pnl_sum": float(row["pnl_sum"] or 0.0),
                "avg_pnl": float(row["avg_pnl"] or 0.0),
                "avg_capture_ratio": (
                    float(row["avg_capture_ratio"])
                    if row["avg_capture_ratio"] not in (None, "")
                    else None
                ),
            }
        )
    return items


def _latest_closed_performance_rows_from_db(con: sqlite3.Connection) -> list[dict[str, object]]:
    table_names = {
        str(row["name"])
        for row in con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if "trade_performance" not in table_names:
        return []
    trade_columns = _table_columns(con, "trades")
    strategy_display_expr = _trade_strategy_display_expr(trade_columns, "t")
    strategy_subtype_expr = _trade_strategy_subtype_expr(trade_columns, "t")
    strategy_component_expr = _trade_strategy_component_select(trade_columns, "t")
    strategy_signal_expr = _trade_strategy_signal_select(trade_columns, "t")
    rows = con.execute(
        f"""
        SELECT
            t.position_id,
            t.symbol,
            t.pnl,
            t.closed_at,
            {strategy_display_expr} AS strategy_entry,
            {strategy_subtype_expr} AS strategy_subtype,
            {strategy_component_expr} AS strategy_entry_component,
            {strategy_signal_expr} AS strategy_entry_signal,
            tp.close_reason,
            tp.max_favorable_pips,
            tp.max_adverse_pips,
            tp.max_favorable_pnl,
            tp.max_adverse_pnl
        FROM trades t
        JOIN trade_performance tp ON tp.position_id = t.position_id
        WHERE t.status = 'closed'
        ORDER BY COALESCE(t.closed_at, t.updated_at, t.opened_at) DESC, t.position_id DESC
        LIMIT ?
        """,
        (_STATUS_LATEST_CLOSED_LIMIT,),
    ).fetchall()
    items = [dict(row) for row in rows]
    _hydrate_closed_trade_rows_with_event_pnl(con, items, table_names=table_names)
    return items


def _status_unavailable_symbols_from_db(con: sqlite3.Connection) -> list[dict[str, object]]:
    try:
        event_rows = con.execute(
            """
            SELECT id, ts, symbol, message, payload_json
            FROM events
            ORDER BY id DESC
            LIMIT ?
            """,
            (_STATUS_EVENTS_WINDOW,),
        ).fetchall()
    except sqlite3.OperationalError:
        event_rows = []
    try:
        worker_rows = con.execute(
            """
            SELECT
                symbol,
                mode,
                strategy,
                strategy_base,
                position_strategy_entry,
                position_strategy_component,
                position_strategy_signal,
                position_json,
                last_heartbeat,
                last_error,
                updated_at
            FROM worker_states
            ORDER BY symbol ASC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        worker_rows = []
    try:
        tick_rows = con.execute(
            """
            SELECT symbol, ts
            FROM broker_ticks
            ORDER BY symbol ASC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        tick_rows = []

    now_ts = time.time()
    tick_by_symbol = {
        str(row["symbol"] or "").strip().upper(): float(row["ts"] or 0.0)
        for row in tick_rows
        if str(row["symbol"] or "").strip()
    }
    transient_recovered_reasons = {
        "multi_degraded_hold",
        "db_first_tick_cache_warming_up",
    }

    def _event_issue_reason(message: str, payload: dict[str, object] | None) -> str | None:
        if message in {
            "DB-first tick cache refresh failed",
            "DB-first tick cache refresh deferred",
            "DB-first tick cache warming up",
            "Trade blocked by connectivity check",
            "Trade blocked by stream health check",
            "Trade blocked by stale tick age",
            "Trade blocked by stale entry history",
        }:
            event = {"message": message, "payload": payload}
            reason_key = _event_reason(event)
            if reason_key is not None:
                return str(reason_key[1])
            return _slug_reason(message)
        if message == "Trade blocked by risk manager":
            reason = str((payload or {}).get("reason") or "").strip()
            normalized = _slug_reason(reason)
            if "db_first" in normalized or "account_snapshot_cache_is_empty_or_stale" in normalized:
                return normalized
            return None
        if message == "Broker allowance backoff active":
            event = {"message": message, "payload": payload}
            reason_key = _event_reason(event)
            if reason_key is not None:
                return str(reason_key[1])
            return "allowance_backoff_active"
        if message == "Broker error":
            error_text = str((payload or {}).get("error") or "")
            normalized = _normalize_broker_error_reason(error_text)
            lowered = normalized.lower()
            if "epic_unavailable" in lowered or "allowance" in lowered or "marketdata" in lowered:
                return normalized
        if message == "Signal hold reason":
            reason = str((payload or {}).get("reason") or "").strip()
            if reason == "multi_degraded_hold":
                return reason
        return None

    latest_issue_by_symbol: dict[str, dict[str, object]] = {}
    for row in event_rows:
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol or symbol in latest_issue_by_symbol:
            continue
        payload = _decode_payload_json(row["payload_json"])
        reason = _event_issue_reason(str(row["message"] or ""), payload)
        if not reason:
            continue
        latest_issue_by_symbol[symbol] = {
            "ts": float(row["ts"] or 0.0),
            "reason": reason,
        }

    items: list[dict[str, object]] = []
    for row in worker_rows:
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol:
            continue
        heartbeat_ts = float(row["last_heartbeat"] or 0.0)
        heartbeat_age = max(0.0, now_ts - heartbeat_ts) if heartbeat_ts > 0 else None
        tick_ts = tick_by_symbol.get(symbol)
        tick_age = max(0.0, now_ts - tick_ts) if tick_ts and tick_ts > 0 else None
        heartbeat_fresh = heartbeat_age is not None and heartbeat_age <= _STATUS_WORKER_STALE_MAX_AGE_SEC
        tick_fresh = tick_age is not None and tick_age <= _STATUS_TICK_STALE_MAX_AGE_SEC
        status = None
        reason = None
        issue = latest_issue_by_symbol.get(symbol)
        if issue is not None:
            candidate_reason = str(issue["reason"])
            if candidate_reason not in transient_recovered_reasons or not (heartbeat_fresh and tick_fresh):
                status = "degraded"
                reason = candidate_reason
        elif heartbeat_age is not None and heartbeat_age > _STATUS_WORKER_STALE_MAX_AGE_SEC:
            status = "unavailable"
            reason = "worker_stale"
        elif tick_age is None:
            status = "degraded"
            reason = "no_tick_cache"
        elif tick_age > _STATUS_TICK_STALE_MAX_AGE_SEC:
            status = "degraded"
            reason = "tick_stale"
        last_error = str(row["last_error"] or "").strip()
        if not reason and last_error:
            candidate_reason = _slug_reason(last_error)
            if candidate_reason not in transient_recovered_reasons or not (heartbeat_fresh and tick_fresh):
                status = "degraded"
                reason = candidate_reason
        if not reason:
            continue
        items.append(
            {
                "symbol": symbol,
                "status": status or "degraded",
                "reason": reason,
                "heartbeat_age_sec": heartbeat_age,
                "tick_age_sec": tick_age,
                "strategy": str(row["strategy"] or "-"),
                "strategy_base": str(row["strategy_base"] or "") or None,
                "position_strategy_entry": str(row["position_strategy_entry"] or "") or None,
                "position_strategy_component": str(row["position_strategy_component"] or "") or None,
                "position_strategy_signal": str(row["position_strategy_signal"] or "") or None,
                "position_json": str(row["position_json"] or "") or None,
                "mode": str(row["mode"] or "-"),
            }
        )

    items.sort(
        key=lambda item: (
            0 if str(item["status"]) == "unavailable" else 1,
            -_as_float(item.get("tick_age_sec"), 0.0),
            str(item["symbol"]),
        )
    )
    return items[:_STATUS_UNAVAILABLE_LIMIT]


def _open_trades_status_rows_from_db(con: sqlite3.Connection) -> list[dict[str, object]]:
    trade_columns = _table_columns(con, "trades")
    if not trade_columns:
        return []
    strategy_entry_select = f"{_trade_strategy_display_expr(trade_columns, 't')} AS strategy_entry"
    strategy_subtype_select = f"{_trade_strategy_subtype_expr(trade_columns, 't')} AS strategy_subtype"
    strategy_exit_select = f"{_trade_strategy_exit_select(trade_columns, 't')} AS strategy_exit"
    rows = con.execute(
        f"""
        SELECT
            t.position_id,
            t.deal_reference,
            t.symbol,
            t.side,
            t.volume,
            t.open_price,
            t.stop_loss,
            t.take_profit,
            t.opened_at,
            t.entry_confidence,
            t.pnl,
            t.strategy,
            {strategy_entry_select},
            {strategy_subtype_select},
            {strategy_exit_select},
            t.mode,
            t.updated_at,
            ws.last_price,
            ws.last_heartbeat,
            ws.last_error,
            ws.iteration
        FROM trades t
        LEFT JOIN worker_states ws ON ws.symbol = t.symbol
        WHERE t.status = 'open'
        ORDER BY t.opened_at ASC
        LIMIT ?
        """,
        (_STATUS_OPEN_TRADES_LIMIT,),
    ).fetchall()

    now_ts = time.time()
    items: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        symbol = str(item.get("symbol") or "-")
        side = str(item.get("side") or "").strip().lower()
        position_id = str(item.get("position_id") or "-")
        deal_reference = str(item.get("deal_reference") or "-")
        volume = float(item.get("volume") or 0.0)
        open_price = float(item.get("open_price") or 0.0)
        stop_loss = float(item.get("stop_loss") or 0.0)
        take_profit = float(item.get("take_profit") or 0.0)
        opened_at_ts = float(item.get("opened_at") or 0.0)
        mark_price = float(item.get("last_price") or open_price)
        pnl = float(item.get("pnl") or 0.0)
        heartbeat_ts = float(item.get("last_heartbeat") or 0.0)
        heartbeat_age = max(0.0, now_ts - heartbeat_ts) if heartbeat_ts > 0 else None
        pip_size = max(_pip_size_for_symbol(symbol), FLOAT_COMPARISON_TOLERANCE)
        stop_distance = abs(open_price - stop_loss)
        tp_distance = abs(take_profit - open_price)
        if side == "buy":
            favorable_move = max(0.0, mark_price - open_price)
            adverse_move = max(0.0, open_price - mark_price)
        else:
            favorable_move = max(0.0, open_price - mark_price)
            adverse_move = max(0.0, mark_price - open_price)
        items.append(
            {
                "symbol": symbol,
                "side": side.upper() if side else "-",
                "position_id": position_id,
                "deal_reference": deal_reference,
                "volume": volume,
                "opened_at": opened_at_ts,
                "age_text": _format_duration(now_ts - opened_at_ts) if opened_at_ts > 0 else "-",
                "pnl": pnl,
                "mark_price": mark_price,
                "open_price": open_price,
                "entry_confidence": float(item.get("entry_confidence") or 0.0),
                "strategy_entry": str(item.get("strategy_entry") or item.get("strategy") or "-"),
                "strategy_exit": str(item.get("strategy_exit") or "-"),
                "mode": str(item.get("mode") or "-"),
                "heartbeat_age_sec": heartbeat_age,
                "progress_to_tp_pct": (favorable_move / tp_distance * 100.0) if tp_distance > FLOAT_COMPARISON_TOLERANCE else 0.0,
                "risk_to_sl_pct": (adverse_move / stop_distance * 100.0) if stop_distance > FLOAT_COMPARISON_TOLERANCE else 0.0,
                "favorable_pips": favorable_move / pip_size,
                "adverse_pips": adverse_move / pip_size,
                "last_error": str(item.get("last_error") or "").strip(),
            }
        )
    return items


def _show_status(storage_path: Path) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    stream_summary: dict[str, object] | None = None
    trade_performance_summary: dict[str, object] | None = None
    emergency_summary: dict[str, object] | None = None
    recent_trade_flow_summary: dict[str, object] | None = None
    request_limit_summary: dict[str, object] | None = None
    strategy_status_rows: list[dict[str, object]] = []
    strategy_subtype_rows: list[dict[str, object]] = []
    latest_closed_rows: list[dict[str, object]] = []
    unavailable_symbols: list[dict[str, object]] = []
    open_trade_rows: list[dict[str, object]] = []
    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        open_row = con.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE status = 'open'"
        ).fetchone()
        closed_row = con.execute(
            "SELECT COUNT(*) AS cnt FROM trades WHERE status = 'closed'"
        ).fetchone()
        events_row = con.execute("SELECT COUNT(*) AS cnt FROM events").fetchone()
        latest_event = con.execute(
            "SELECT ts, level, symbol, message FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        latest_snapshot = con.execute(
            """
            SELECT ts, balance, equity, margin_free, open_positions, daily_pnl, drawdown_pct
            FROM account_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
        housekeeping_row = con.execute(
            "SELECT value FROM kv WHERE key = 'db.housekeeping.last_summary'"
        ).fetchone()
        stream_summary = _stream_status_summary_from_db(con)
        trade_performance_summary = _trade_performance_summary_from_db(con)
        emergency_summary = _multi_strategy_emergency_summary_from_db(con)
        recent_trade_flow_summary = _status_recent_trade_flow_summary_from_db(con)
        request_limit_summary = _request_limit_status_from_db(con)
        strategy_status_rows = _strategy_status_summary_from_db(con)
        strategy_subtype_rows = _strategy_status_summary_from_db(con, use_subtype=True)
        latest_closed_rows = _latest_closed_performance_rows_from_db(con)
        unavailable_symbols = _status_unavailable_symbols_from_db(con)
        open_trade_rows = _open_trades_status_rows_from_db(con)
        # Count broker-side positions for reconciliation
        pending_open_row = None
        broker_real_row = None
        try:
            pending_open_row = con.execute(
                "SELECT COUNT(*) AS cnt FROM pending_opens"
            ).fetchone()
        except Exception:
            pass
        try:
            broker_real_row = con.execute(
                "SELECT COUNT(*) AS cnt FROM real_positions_cache WHERE abs(real_qty_lots) > 1e-9"
            ).fetchone()
        except Exception:
            pass
    finally:
        con.close()

    open_count = int((open_row["cnt"] if open_row is not None else 0) or 0)
    closed_count = int((closed_row["cnt"] if closed_row is not None else 0) or 0)
    events_count = int((events_row["cnt"] if events_row is not None else 0) or 0)
    pending_open_count = int((pending_open_row["cnt"] if pending_open_row is not None else 0) or 0)
    broker_real_count = int((broker_real_row["cnt"] if broker_real_row is not None else 0) or 0)
    broker_snapshot_open = (
        int(latest_snapshot["open_positions"] or 0) if latest_snapshot is not None else None
    )

    print(f"State status | path={storage_path}")
    open_parts = [
        f"trades_open={open_count}",
        f"trades_closed={closed_count}",
        f"events_total={events_count}",
    ]
    if pending_open_count > 0:
        open_parts.append(f"pending_opens={pending_open_count}")
    if broker_real_count > 0 and broker_real_count != open_count:
        open_parts.append(f"broker_positions={broker_real_count}")
    print(" ".join(open_parts))
    if broker_snapshot_open is not None and broker_snapshot_open != open_count:
        print(
            f"  WARNING: trades_open={open_count} but broker reports "
            f"open_positions={broker_snapshot_open} — possible sync lag or external position"
        )

    if latest_event is not None:
        latest_event_ts = float(latest_event["ts"] or 0.0)
        latest_event_text = datetime.fromtimestamp(latest_event_ts).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"latest_event=[{latest_event_text}] level={latest_event['level']} "
            f"symbol={latest_event['symbol'] or '-'} message={latest_event['message']}"
        )
    else:
        print("latest_event=none")

    if latest_snapshot is not None:
        snap_ts = float(latest_snapshot["ts"] or 0.0)
        snap_text = datetime.fromtimestamp(snap_ts).strftime("%Y-%m-%d %H:%M:%S")
        snap_age_sec = max(0.0, time.time() - snap_ts) if snap_ts > 0 else None
        snap_age_text = "-" if snap_age_sec is None else f"{snap_age_sec:.1f}s"
        snap_state = (
            "stale"
            if snap_age_sec is not None and snap_age_sec > _ACCOUNT_SNAPSHOT_STALE_MAX_AGE_SEC
            else "fresh"
        )
        print(
            f"latest_snapshot=[{snap_text}] age={snap_age_text} state={snap_state} "
            f"balance={float(latest_snapshot['balance'] or 0.0):.2f} "
            f"equity={float(latest_snapshot['equity'] or 0.0):.2f} "
            f"margin_free={float(latest_snapshot['margin_free'] or 0.0):.2f} "
            f"open_positions={int(latest_snapshot['open_positions'] or 0)} "
            f"daily_pnl={float(latest_snapshot['daily_pnl'] or 0.0):.2f} "
            f"drawdown_pct={float(latest_snapshot['drawdown_pct'] or 0.0):.2f}"
        )
    else:
        print("latest_snapshot=none")

    if stream_summary is not None:
        configured = "enabled" if bool(stream_summary.get("configured_enabled")) else "disabled"
        latest_age_sec = stream_summary.get("latest_age_sec")
        latest_age_text = "-" if latest_age_sec is None else f"{float(latest_age_sec):.1f}s"
        print(
            f"ig_stream=configured={configured} runtime={stream_summary.get('runtime_state') or 'unknown'} "
            f"symbols={int(stream_summary.get('symbols_total') or 0)} "
            f"connected={int(stream_summary.get('symbols_connected') or 0)} "
            f"healthy={int(stream_summary.get('symbols_healthy') or 0)} "
            f"desired={int(stream_summary.get('desired_subscriptions') or 0)} "
            f"active={int(stream_summary.get('active_subscriptions') or 0)} "
            f"last_metrics_age={latest_age_text}"
        )
        requests = stream_summary.get("requests")
        stream_hits = stream_summary.get("stream_hits")
        rest_hits = stream_summary.get("rest_fallback_hits")
        if requests is not None and stream_hits is not None and rest_hits is not None:
            hit_rate = stream_summary.get("stream_hit_rate_pct")
            hit_rate_text = "-" if hit_rate is None else f"{float(hit_rate):.2f}%"
            print(
                f"ig_stream_usage=requests={int(requests)} stream_hits={int(stream_hits)} "
                f"rest_fallback_hits={int(rest_hits)} stream_hit_rate={hit_rate_text}"
            )
        reasons = stream_summary.get("reasons")
        if isinstance(reasons, list) and reasons:
            reason_text = ",".join(f"{str(reason)}:{int(count)}" for reason, count in reasons)
            print(f"ig_stream_reasons={reason_text}")

    if request_limit_summary is not None:
        hot_row = request_limit_summary.get("hot")
        worker_rows = request_limit_summary.get("rows")
        if isinstance(hot_row, dict):
            print(
                "ig_request_limits="
                f"workers={len(worker_rows) if isinstance(worker_rows, list) else 0} "
                f"hot={hot_row.get('worker_key') or '-'} "
                f"used={int(hot_row.get('used_value') or 0)}/{int(hot_row.get('limit_value') or 0)} "
                f"remaining={int(hot_row.get('remaining_value') or 0)} "
                f"blocked_total={int(hot_row.get('blocked_total') or 0)} "
                f"status={hot_row.get('status') or 'normal'}"
            )
            if isinstance(worker_rows, list):
                for row in worker_rows[: min(3, len(worker_rows))]:
                    print(
                        "  ig_limit_worker="
                        f"{row.get('worker_key') or '-'} "
                        f"used={int(row.get('used_value') or 0)}/{int(row.get('limit_value') or 0)} "
                        f"remaining={int(row.get('remaining_value') or 0)} "
                        f"blocked_total={int(row.get('blocked_total') or 0)} "
                        f"status={row.get('status') or 'normal'}"
                    )
        else:
            print("ig_request_limits=not_available")

    if trade_performance_summary is not None:
        closed_count = int(trade_performance_summary.get("closed_count") or 0)
        avg_mfe = trade_performance_summary.get("avg_mfe_pnl")
        avg_mae = trade_performance_summary.get("avg_mae_pnl")
        avg_capture = trade_performance_summary.get("avg_capture_ratio")
        avg_mfe_text = "-" if avg_mfe is None else f"{float(avg_mfe):.2f}"
        avg_mae_text = "-" if avg_mae is None else f"{float(avg_mae):.2f}"
        avg_capture_text = "-" if avg_capture is None else f"{float(avg_capture) * 100.0:.1f}%"
        print(
            "trade_performance="
            f"closed={closed_count} avg_mfe_pnl={avg_mfe_text} "
            f"avg_mae_pnl={avg_mae_text} avg_capture={avg_capture_text}"
        )
        if latest_closed_rows:
            print(f"latest_closed_performance={len(latest_closed_rows)} latest trades")
            for row in latest_closed_rows:
                closed_at = float(row.get("closed_at") or 0.0)
                closed_text = datetime.fromtimestamp(closed_at).strftime("%Y-%m-%d %H:%M:%S") if closed_at > 0 else "-"
                max_favorable_pnl = float(row.get("max_favorable_pnl") or 0.0)
                final_pnl = float(row.get("pnl") or 0.0)
                capture_pct = (final_pnl / max_favorable_pnl * 100.0) if max_favorable_pnl > 0 else None
                capture_text = "-" if capture_pct is None else f"{capture_pct:.1f}%"
                close_reason = str(row.get("close_reason") or "-")
                print(
                    "  closed="
                    f"[{closed_text}] symbol={row.get('symbol') or '-'} "
                    f"strategy={row.get('strategy_entry') or '-'} "
                    f"subtype={row.get('strategy_subtype') or row.get('strategy_entry') or '-'} "
                    f"pnl={final_pnl:.2f} mfe_pnl={max_favorable_pnl:.2f} "
                    f"mae_pnl={float(row.get('max_adverse_pnl') or 0.0):.2f} "
                    f"mfe_pips={float(row.get('max_favorable_pips') or 0.0):.2f} "
                    f"mae_pips={float(row.get('max_adverse_pips') or 0.0):.2f} "
                    f"capture={capture_text} reason={close_reason}"
                )

    if emergency_summary is not None:
        if bool(emergency_summary.get("active")):
            symbols = list(emergency_summary.get("symbols") or [])
            reasons = list(emergency_summary.get("reasons") or [])
            symbol_preview = ",".join(str(item) for item in symbols[:8])
            if len(symbols) > 8:
                symbol_preview = f"{symbol_preview},+{len(symbols) - 8}"
            if not symbol_preview:
                symbol_preview = "-"
            reason_text = ",".join(f"{str(reason)}:{int(count)}" for reason, count in reasons) if reasons else "unknown:1"
            latest_age_sec = emergency_summary.get("latest_age_sec")
            latest_age_text = "-" if latest_age_sec is None else f"{float(latest_age_sec):.1f}s"
            print(
                "multi_strategy_emergency=active "
                f"workers={int(emergency_summary.get('count') or 0)} "
                f"symbols={symbol_preview} reasons={reason_text} "
                f"last_update_age={latest_age_text}"
            )
        else:
            print("multi_strategy_emergency=inactive")

    if recent_trade_flow_summary is not None:
        print(
            "recent_trade_flow="
            f"window_events={int(recent_trade_flow_summary.get('events_scanned') or 0)} "
            f"window_hours={_STATUS_ACTIVITY_WINDOW_SEC / 3600.0:.1f} "
            f"opened={int(recent_trade_flow_summary.get('opened_recent') or 0)} "
            f"closed={int(recent_trade_flow_summary.get('closed_recent') or 0)} "
            f"holds={int(recent_trade_flow_summary.get('hold_count') or 0)} "
            f"blocks={int(recent_trade_flow_summary.get('block_count') or 0)} "
            f"rejects={int(recent_trade_flow_summary.get('reject_count') or 0)}"
        )
        top_reasons = recent_trade_flow_summary.get("top_reasons")
        if isinstance(top_reasons, list) and top_reasons:
            print("recent_trade_reasons:")
            for item in top_reasons:
                symbols = list(item.get("symbols") or [])
                symbol_preview = ",".join(str(symbol) for symbol in symbols[:6]) if symbols else "-"
                if len(symbols) > 6:
                    symbol_preview = f"{symbol_preview},+{len(symbols) - 6}"
                print(
                    f"  {item.get('kind') or '-'} | {item.get('reason') or '-'} | "
                    f"count={int(item.get('count') or 0)} symbols={symbol_preview}"
                )
        component_holds = recent_trade_flow_summary.get("top_component_holds")
        if isinstance(component_holds, list) and component_holds:
            print("recent_multi_hold_components:")
            for item in component_holds:
                print(
                    f"  {item.get('strategy') or '-'}={item.get('reason') or '-'} "
                    f"count={int(item.get('count') or 0)}"
                )
        component_blocked_holds = recent_trade_flow_summary.get("top_component_blocked_holds")
        if isinstance(component_blocked_holds, list) and component_blocked_holds:
            print("recent_hard_block_components:")
            for item in component_blocked_holds:
                print(
                    f"  {item.get('strategy') or '-'}={item.get('reason') or '-'} "
                    f"count={int(item.get('count') or 0)}"
                )
        component_unavailable_holds = recent_trade_flow_summary.get("top_component_unavailable_holds")
        if isinstance(component_unavailable_holds, list) and component_unavailable_holds:
            print("recent_unavailable_components:")
            for item in component_unavailable_holds:
                print(
                    f"  {item.get('strategy') or '-'}={item.get('reason') or '-'} "
                    f"count={int(item.get('count') or 0)}"
                )
        component_soft_filters = recent_trade_flow_summary.get("top_component_soft_filters")
        if isinstance(component_soft_filters, list) and component_soft_filters:
            print("recent_soft_filters:")
            for item in component_soft_filters:
                print(
                    f"  {item.get('strategy') or '-'}={item.get('reason') or '-'} "
                    f"count={int(item.get('count') or 0)}"
                )
        spike_hold_count = int(recent_trade_flow_summary.get("spike_hold_count") or 0)
        if spike_hold_count > 0:
            print(f"recent_entry_spike_blocks={spike_hold_count}")
            spike_symbols = recent_trade_flow_summary.get("top_spike_symbols")
            if isinstance(spike_symbols, list) and spike_symbols:
                print("recent_entry_spike_symbols:")
                for item in spike_symbols:
                    print(
                        f"  {item.get('symbol') or '-'} "
                        f"count={int(item.get('count') or 0)}"
                    )
            spike_components = recent_trade_flow_summary.get("top_spike_components")
            if isinstance(spike_components, list) and spike_components:
                print("recent_entry_spike_components:")
                for item in spike_components:
                    print(
                        f"  {item.get('component') or '-'} "
                        f"count={int(item.get('count') or 0)}"
                    )

    if strategy_status_rows:
        print("strategy_performance:")
        for item in strategy_status_rows:
            capture = item.get("avg_capture_ratio")
            capture_text = "-" if capture is None else f"{float(capture) * 100.0:.1f}%"
            print(
                f"  {item.get('strategy_entry') or '-'} | trades={int(item.get('sample_count') or 0)} "
                f"share={float(item.get('share_pct') or 0.0):.1f}% "
                f"win_rate={float(item.get('win_rate_pct') or 0.0):.1f}% "
                f"pnl_sum={float(item.get('pnl_sum') or 0.0):.2f} "
                f"avg_pnl={float(item.get('avg_pnl') or 0.0):.2f} capture={capture_text}"
            )
    else:
        print("strategy_performance=none")

    if strategy_subtype_rows:
        print("strategy_subtype_performance:")
        for item in strategy_subtype_rows:
            capture = item.get("avg_capture_ratio")
            capture_text = "-" if capture is None else f"{float(capture) * 100.0:.1f}%"
            print(
                f"  {item.get('strategy_subtype') or '-'} | trades={int(item.get('sample_count') or 0)} "
                f"share={float(item.get('share_pct') or 0.0):.1f}% "
                f"win_rate={float(item.get('win_rate_pct') or 0.0):.1f}% "
                f"pnl_sum={float(item.get('pnl_sum') or 0.0):.2f} "
                f"avg_pnl={float(item.get('avg_pnl') or 0.0):.2f} capture={capture_text}"
            )
    else:
        print("strategy_subtype_performance=none")

    unavailable_only = [
        item for item in unavailable_symbols if str(item.get("status") or "").strip().lower() == "unavailable"
    ]
    degraded_only = [
        item for item in unavailable_symbols if str(item.get("status") or "").strip().lower() != "unavailable"
    ]

    if unavailable_only:
        print("symbols_unavailable:")
        for item in unavailable_only:
            heartbeat_age = item.get("heartbeat_age_sec")
            tick_age = item.get("tick_age_sec")
            heartbeat_text = "-" if heartbeat_age is None else f"{float(heartbeat_age):.1f}s"
            tick_text = "-" if tick_age is None else f"{float(tick_age):.1f}s"
            worker_strategy, worker_strategy_base = _worker_state_strategy_labels(item)
            worker_position = _worker_state_position_label(item)
            print(
                f"  {item.get('symbol') or '-'} | status={item.get('status') or '-'} "
                f"reason={item.get('reason') or '-'} worker={item.get('mode') or '-'}/{worker_strategy}"
                + (f" strategy_base={worker_strategy_base}" if worker_strategy_base else "")
                + (f" position={worker_position}" if worker_position else "")
                + " "
                f"heartbeat_age={heartbeat_text} tick_age={tick_text}"
            )
    else:
        print("symbols_unavailable=none")

    if degraded_only:
        print("symbols_degraded:")
        for item in degraded_only:
            heartbeat_age = item.get("heartbeat_age_sec")
            tick_age = item.get("tick_age_sec")
            heartbeat_text = "-" if heartbeat_age is None else f"{float(heartbeat_age):.1f}s"
            tick_text = "-" if tick_age is None else f"{float(tick_age):.1f}s"
            worker_strategy, worker_strategy_base = _worker_state_strategy_labels(item)
            worker_position = _worker_state_position_label(item)
            print(
                f"  {item.get('symbol') or '-'} | status={item.get('status') or '-'} "
                f"reason={item.get('reason') or '-'} worker={item.get('mode') or '-'}/{worker_strategy}"
                + (f" strategy_base={worker_strategy_base}" if worker_strategy_base else "")
                + (f" position={worker_position}" if worker_position else "")
                + " "
                f"heartbeat_age={heartbeat_text} tick_age={tick_text}"
            )
    else:
        print("symbols_degraded=none")

    if open_trade_rows:
        print(f"current_trades={len(open_trade_rows)}")
        for item in open_trade_rows:
            heartbeat_age = item.get("heartbeat_age_sec")
            heartbeat_text = "-" if heartbeat_age is None else f"{float(heartbeat_age):.1f}s"
            print(
                f"  {item.get('symbol') or '-'} {item.get('side') or '-'} | age={item.get('age_text') or '-'} "
                f"pnl={float(item.get('pnl') or 0.0):.2f} mark={float(item.get('mark_price') or 0.0):.5f} "
                f"progress_to_tp={float(item.get('progress_to_tp_pct') or 0.0):.1f}% "
                f"risk_to_sl={float(item.get('risk_to_sl_pct') or 0.0):.1f}% "
                f"favorable={float(item.get('favorable_pips') or 0.0):.1f}p "
                f"adverse={float(item.get('adverse_pips') or 0.0):.1f}p "
                f"strategy={item.get('strategy_entry') or '-'} "
                f"subtype={item.get('strategy_subtype') or item.get('strategy_entry') or '-'} "
                f"id_local={item.get('position_id') or '-'} id_ig={item.get('deal_reference') or '-'} "
                f"worker_age={heartbeat_text}"
                + (f" last_error={item.get('last_error')}" if str(item.get("last_error") or "").strip() else "")
            )
    else:
        print("current_trades=none")

    housekeeping_summary: dict[str, object] | None = None
    if housekeeping_row is not None:
        raw = housekeeping_row["value"]
        if raw not in (None, ""):
            try:
                parsed = json.loads(str(raw))
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                housekeeping_summary = parsed

    if housekeeping_summary is None:
        print("db_housekeeping=not_available")
        return 1

    hk_ts = float(housekeeping_summary.get("ts") or 0.0)
    hk_ts_text = (
        datetime.fromtimestamp(hk_ts).strftime("%Y-%m-%d %H:%M:%S")
        if hk_ts > 0
        else "-"
    )
    print(
        f"db_housekeeping=[{hk_ts_text}] events_deleted={int(housekeeping_summary.get('events_deleted') or 0)} "
        f"events_kept={int(housekeeping_summary.get('events_kept') or 0)}"
    )
    checkpoint_raw = housekeeping_summary.get("checkpoint")
    if isinstance(checkpoint_raw, dict):
        print(
            "db_checkpoint="
            f"busy={int(checkpoint_raw.get('busy') or 0)} "
            f"wal_frames={int(checkpoint_raw.get('wal_frames') or 0)} "
            f"checkpointed_frames={int(checkpoint_raw.get('checkpointed_frames') or 0)}"
        )
    else:
        print("db_checkpoint=not_available")
    return 1


def _show_trade_timeline(storage_path: Path, limit: int, events_limit: int) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    max_trades = max(1, int(limit))
    per_trade_limit = max(10, int(events_limit))
    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        table_names = {
            str(row["name"])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        trade_columns = _table_columns(con, "trades")
        has_trade_performance = "trade_performance" in table_names
        strategy_display_expr = _trade_strategy_display_expr(trade_columns, "t")
        strategy_entry_base_select = _trade_strategy_entry_base_select(trade_columns, "t")
        strategy_component_select = _trade_strategy_component_select(trade_columns, "t")
        strategy_exit_select = f"{_trade_strategy_exit_select(trade_columns, 't')} AS strategy_exit"
        strategy_exit_component_select = (
            f"{_trade_strategy_exit_component_select(trade_columns, 't')} AS strategy_exit_component"
        )
        strategy_exit_signal_select = f"{_trade_strategy_exit_signal_select(trade_columns, 't')} AS strategy_exit_signal"
        if has_trade_performance:
            trade_rows = con.execute(
                """
                SELECT
                    t.position_id, t.symbol, t.side, t.open_price, t.close_price, t.volume,
                    t.opened_at, t.closed_at, t.pnl, t.strategy,
                    """
                + f"""
                    {strategy_display_expr} AS strategy_entry,
                    {strategy_entry_base_select} AS strategy_entry_base,
                    {strategy_component_select} AS strategy_entry_component,
                    {strategy_exit_select},
                    {strategy_exit_component_select},
                    {strategy_exit_signal_select},
                    t.mode,
                    tp.close_reason, tp.max_favorable_pips, tp.max_adverse_pips,
                    tp.max_favorable_pnl, tp.max_adverse_pnl
                FROM trades t
                LEFT JOIN trade_performance tp ON tp.position_id = t.position_id
                WHERE t.status = 'closed'
                ORDER BY COALESCE(t.closed_at, t.updated_at, t.opened_at) DESC, t.position_id DESC
                LIMIT ?
                """,
                (max_trades,),
            ).fetchall()
        else:
            trade_rows = con.execute(
                """
                SELECT
                    t.position_id, t.symbol, t.side, t.open_price, t.close_price, t.volume,
                    t.opened_at, t.closed_at, t.pnl, t.strategy,
                    """
                + f"""
                    {strategy_display_expr} AS strategy_entry,
                    {strategy_entry_base_select} AS strategy_entry_base,
                    {strategy_component_select} AS strategy_entry_component,
                    {strategy_exit_select},
                    {strategy_exit_component_select},
                    {strategy_exit_signal_select},
                    t.mode
                FROM trades t
                WHERE t.status = 'closed'
                ORDER BY COALESCE(t.closed_at, t.updated_at, t.opened_at) DESC, t.position_id DESC
                LIMIT ?
                """,
                (max_trades,),
            ).fetchall()
        if not trade_rows:
            print("No closed trades found.")
            return 0

        print(f"Trade timeline | path={storage_path} trades={len(trade_rows)}")
        for idx, row in enumerate(trade_rows, start=1):
            item = dict(row)
            position_id = str(item.get("position_id") or "")
            symbol = str(item.get("symbol") or "-")
            opened_at = float(item.get("opened_at") or 0.0)
            closed_at = float(item.get("closed_at") or 0.0)
            window_end_ts = closed_at if closed_at > opened_at > 0 else (opened_at + 3600.0)
            opened_text = datetime.fromtimestamp(opened_at).strftime("%Y-%m-%d %H:%M:%S") if opened_at > 0 else "-"
            closed_text = datetime.fromtimestamp(closed_at).strftime("%Y-%m-%d %H:%M:%S") if closed_at > 0 else "-"
            max_favorable_pnl = float(item.get("max_favorable_pnl") or 0.0)
            final_pnl = float(item.get("pnl") or 0.0)
            capture_pct = (final_pnl / max_favorable_pnl * 100.0) if max_favorable_pnl > 0 else None
            capture_text = "-" if capture_pct is None else f"{capture_pct:.1f}%"
            raw_close_reason = str(item.get("close_reason") or "-")
            strategy_name, strategy_entry, strategy_base, strategy_exit = _trade_strategy_labels(item)
            display_close_reason = raw_close_reason
            filtered_events: list[dict[str, object]] = []
            suppressed_event_count = 0
            print(
                f"\n[{idx}] {symbol} position_id={position_id} "
                f"side={str(item.get('side') or '').upper()} strategy={strategy_name}"
                + (f" strategy_base={strategy_base}" if strategy_base else "")
                + f" strategy_exit={strategy_exit} mode={item.get('mode') or '-'}"
            )
            print(
                f"  open=[{opened_text}] close=[{closed_text}] volume={float(item.get('volume') or 0.0):.4f} "
                f"open_price={float(item.get('open_price') or 0.0):.6f} close_price={float(item.get('close_price') or 0.0):.6f} "
                f"pnl={final_pnl:.2f}"
            )

            if opened_at > 0 and window_end_ts > opened_at:
                price_row = con.execute(
                    """
                    SELECT
                        COUNT(*) AS samples,
                        MIN(close) AS min_close,
                        MAX(close) AS max_close
                    FROM price_history
                    WHERE symbol = ? AND ts >= ? AND ts <= ?
                    """,
                    (symbol, opened_at, window_end_ts),
                ).fetchone()
                if price_row is not None and int(price_row["samples"] or 0) > 0:
                    print(
                        "  price_window="
                        f"samples={int(price_row['samples'] or 0)} "
                        f"min_close={float(price_row['min_close'] or 0.0):.6f} "
                        f"max_close={float(price_row['max_close'] or 0.0):.6f}"
                    )

                event_rows = con.execute(
                    """
                    SELECT ts, level, message, payload_json
                    FROM events
                    WHERE symbol = ? AND ts >= ? AND ts <= ?
                    ORDER BY ts ASC, id ASC
                    LIMIT ?
                    """,
                    (symbol, opened_at, window_end_ts, max(per_trade_limit * 4, per_trade_limit)),
                ).fetchall()
                if event_rows:
                    filtered_events, suppressed_event_count, close_reason_from_events = _trade_timeline_events(
                        event_rows,
                        position_id=position_id,
                        limit=per_trade_limit,
                    )
                    if close_reason_from_events:
                        display_close_reason = close_reason_from_events

                if has_trade_performance:
                    close_reason_suffix = ""
                    if raw_close_reason not in {"", "-", display_close_reason}:
                        close_reason_suffix = f" broker_close_reason={raw_close_reason}"
                    print(
                        "  performance="
                        f"mfe_pnl={max_favorable_pnl:.2f} mae_pnl={float(item.get('max_adverse_pnl') or 0.0):.2f} "
                        f"mfe_pips={float(item.get('max_favorable_pips') or 0.0):.2f} "
                        f"mae_pips={float(item.get('max_adverse_pips') or 0.0):.2f} "
                        f"capture={capture_text} close_reason={display_close_reason}{close_reason_suffix}"
                    )

                if filtered_events:
                    print("  events:")
                    for event in filtered_events:
                        event_ts = float(event["ts"] or 0.0)
                        event_text = datetime.fromtimestamp(event_ts).strftime("%H:%M:%S")
                        level = str(event["level"] or "-")
                        message = str(event["message"] or "-")
                        extra = _trade_timeline_event_extra(message, event.get("payload") if isinstance(event, dict) else None)
                        print(f"    [{event_text}] {level} {message}{extra}")
            elif has_trade_performance:
                print(
                    "  performance="
                    f"mfe_pnl={max_favorable_pnl:.2f} mae_pnl={float(item.get('max_adverse_pnl') or 0.0):.2f} "
                    f"mfe_pips={float(item.get('max_favorable_pips') or 0.0):.2f} "
                    f"mae_pips={float(item.get('max_adverse_pips') or 0.0):.2f} "
                    f"capture={capture_text} close_reason={display_close_reason}"
                )

            if "position_updates" in table_names and position_id:
                update_rows = con.execute(
                    """
                    SELECT
                        ts,
                        operation,
                        method,
                        path,
                        http_status,
                        success,
                        error_text,
                        source,
                        strategy,
                        strategy_base,
                        strategy_entry,
                        strategy_entry_component,
                        mode
                    FROM position_updates
                    WHERE position_id = ?
                    ORDER BY ts ASC, id ASC
                    LIMIT ?
                    """,
                    (position_id, per_trade_limit),
                ).fetchall()
                if update_rows:
                    print("  position_updates:")
                    for update in _summarize_position_updates(update_rows):
                        first_ts = float(update["first_ts"] or 0.0)
                        last_ts = float(update["last_ts"] or 0.0)
                        if abs(last_ts - first_ts) >= FLOAT_COMPARISON_TOLERANCE:
                            update_text = (
                                f"{datetime.fromtimestamp(first_ts).strftime('%H:%M:%S')}.."
                                f"{datetime.fromtimestamp(last_ts).strftime('%H:%M:%S')}"
                            )
                        else:
                            update_text = datetime.fromtimestamp(first_ts).strftime("%H:%M:%S")
                        status = str(update["http_status"]) if update["http_status"] is not None else "-"
                        success = int(update["success"] or 0)
                        error_text = str(update["error_text"] or "").strip()
                        count_suffix = f" x{int(update['count'])}" if int(update["count"] or 0) > 1 else ""
                        strategy_name = str(
                            update.get("strategy_entry_component")
                            or update.get("strategy_entry")
                            or update.get("strategy")
                            or ""
                        ).strip()
                        strategy_base = str(update.get("strategy_base") or "").strip()
                        mode_name = str(update.get("mode") or "").strip()
                        strategy_suffix = ""
                        if strategy_name:
                            strategy_suffix += f" strategy={strategy_name}"
                        if strategy_base and strategy_base not in {"", strategy_name}:
                            strategy_suffix += f" strategy_base={strategy_base}"
                        if mode_name:
                            strategy_suffix += f" mode={mode_name}"
                        suffix = f" error={error_text}" if error_text else ""
                        print(
                            f"    [{update_text}] {update['operation']} {update['method']} {update['path']} "
                            f"http={status} success={success} source={update['source']}{count_suffix}{suffix}{strategy_suffix}"
                        )

        return len(trade_rows)
    finally:
        con.close()


def _strategy_params_for_schedule(config, strategy_name: str) -> dict[str, object]:
    normalized = str(strategy_name).strip().lower()
    params_map = getattr(config, "strategy_params_map", {})
    if isinstance(params_map, dict):
        params = params_map.get(normalized)
        if isinstance(params, dict):
            return dict(params)
    if normalized == str(getattr(config, "strategy", "")).strip().lower():
        current = getattr(config, "strategy_params", {})
        if isinstance(current, dict):
            return dict(current)
    return {}


_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"
_MULTI_STRATEGY_BASE_COMPONENT_PARAM = "_multi_strategy_base_component"


def _multi_strategy_enabled_for_schedule_params(strategy_params: dict[str, object]) -> bool:
    raw = strategy_params.get("multi_strategy_enabled", True)
    if raw is None:
        raw = True
    return _as_bool(raw)


def _schedule_disabled_by_multi_strategy_for_cli(config) -> bool:
    if not getattr(config, "strategy_schedule", None):
        return False
    base_strategy = str(getattr(config, "strategy", "") or "").strip().lower()
    strategy_params = _strategy_params_for_schedule(config, base_strategy)
    return _multi_strategy_enabled_for_schedule_params(strategy_params)


def _assignment_payload_for_cli(
    config,
    strategy_name: str,
    strategy_params: dict[str, object],
) -> tuple[str, dict[str, object]]:
    normalized = str(strategy_name or "").strip().lower()
    params = dict(strategy_params)
    if normalized == _MULTI_STRATEGY_CARRIER_NAME:
        base_name = str(
            params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or getattr(config, "strategy", "")
        ).strip().lower()
        if base_name and base_name != _MULTI_STRATEGY_CARRIER_NAME:
            params[_MULTI_STRATEGY_BASE_COMPONENT_PARAM] = base_name
        return _MULTI_STRATEGY_CARRIER_NAME, params
    if _multi_strategy_enabled_for_schedule_params(params):
        if normalized and normalized != _MULTI_STRATEGY_CARRIER_NAME:
            params[_MULTI_STRATEGY_BASE_COMPONENT_PARAM] = normalized
        return _MULTI_STRATEGY_CARRIER_NAME, params
    return normalized, params


def _assignment_strategy_labels_for_cli(strategy_name: str, strategy_params: dict[str, object]) -> tuple[str, str | None]:
    normalized = str(strategy_name or "").strip().lower() or "-"
    params = dict(strategy_params or {})
    if normalized != _MULTI_STRATEGY_CARRIER_NAME:
        return normalized, None
    strategy_base = str(params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or "").strip().lower()
    if not strategy_base or strategy_base == normalized:
        return normalized, None
    return normalized, strategy_base


def _is_schedule_entry_active(config, now_utc: datetime, entry) -> bool:
    schedule_timezone = ZoneInfo(str(getattr(config, "strategy_schedule_timezone", "UTC") or "UTC"))
    local_dt = now_utc.astimezone(schedule_timezone)
    minute_of_day = local_dt.hour * 60 + local_dt.minute
    weekday = local_dt.weekday()
    if entry.start_minute < entry.end_minute:
        return weekday in entry.weekdays and entry.start_minute <= minute_of_day < entry.end_minute
    if weekday in entry.weekdays and minute_of_day >= entry.start_minute:
        return True
    previous_weekday = (weekday - 1) % 7
    if previous_weekday in entry.weekdays and minute_of_day < entry.end_minute:
        return True
    return False


def _schedule_assignments_for_cli(config, now_utc: datetime) -> dict[str, dict[str, object]]:
    multi_schedule_disabled = _schedule_disabled_by_multi_strategy_for_cli(config)
    if (
        getattr(config, "force_symbols", False)
        or getattr(config, "force_strategy", False)
        or not config.strategy_schedule
        or multi_schedule_disabled
    ):
        source = "forced_symbols" if getattr(config, "force_symbols", False) else (
            "forced" if getattr(config, "force_strategy", False) else "static"
        )
        strategy_name, strategy_params = _assignment_payload_for_cli(
            config,
            str(getattr(config, "strategy", "") or ""),
            _strategy_params_for_schedule(config, str(getattr(config, "strategy", "") or "")),
        )
        if source == "static" and getattr(config, "strategy_schedule", None) and multi_schedule_disabled:
            source = "multi_static"
        return {
            str(symbol).strip().upper(): {
                "symbol": str(symbol).strip().upper(),
                "strategy_name": strategy_name,
                "strategy_params": dict(strategy_params),
                "source": source,
                "label": None,
                "open_position_id": None,
            }
            for symbol in getattr(config, "symbols", [])
            if str(symbol).strip()
        }

    ranked: dict[str, tuple[int, int, dict[str, object]]] = {}
    for index, entry in enumerate(config.strategy_schedule):
        if not _is_schedule_entry_active(config, now_utc, entry):
            continue
        for symbol in entry.symbols:
            normalized_symbol = str(symbol).strip().upper()
            if not normalized_symbol:
                continue
            candidate = (
                int(entry.priority),
                index,
                {
                    "symbol": normalized_symbol,
                    "strategy_name": str(entry.strategy),
                    "strategy_params": dict(entry.strategy_params),
                    "source": "schedule",
                    "label": entry.label or f"{entry.strategy}@{entry.start_time}-{entry.end_time}",
                    "open_position_id": None,
                },
            )
            current = ranked.get(normalized_symbol)
            if current is None or candidate[0] > current[0] or (candidate[0] == current[0] and candidate[1] >= current[1]):
                ranked[normalized_symbol] = candidate
    return {symbol: item[2] for symbol, item in ranked.items()}


def _load_open_positions_for_schedule(storage_path: Path, mode_value: str) -> dict[str, dict[str, str | None]]:
    if not storage_path.exists():
        return {}
    con = _connect_readonly_db(storage_path)
    try:
        rows = con.execute(
            """
            SELECT
                symbol,
                position_id,
                strategy,
                strategy_entry,
                strategy_entry_component,
                strategy_entry_signal,
                updated_at
            FROM trades
            WHERE status = 'open' AND mode = ?
            ORDER BY updated_at DESC
            """,
            (str(mode_value),),
        ).fetchall()
    finally:
        con.close()
    by_symbol: dict[str, dict[str, str | None]] = {}
    for row in rows:
        item = dict(row)
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol or symbol in by_symbol:
            continue
        by_symbol[symbol] = {
            "position_id": str(item.get("position_id") or "").strip() or None,
            "strategy": str(item.get("strategy") or "").strip().lower() or None,
            "strategy_entry": str(item.get("strategy_entry") or "").strip().lower() or None,
            "strategy_entry_component": str(item.get("strategy_entry_component") or "").strip().lower() or None,
            "strategy_entry_signal": str(item.get("strategy_entry_signal") or "").strip().lower() or None,
        }
    return by_symbol


def _show_active_schedule(config) -> int:
    now_utc = datetime.now(timezone.utc)
    schedule_timezone = ZoneInfo(str(getattr(config, "strategy_schedule_timezone", "UTC") or "UTC"))
    active_slots = [
        entry
        for entry in config.strategy_schedule
        if _is_schedule_entry_active(config, now_utc, entry)
    ]
    multi_schedule_disabled = _schedule_disabled_by_multi_strategy_for_cli(config)
    assignments = _schedule_assignments_for_cli(config, now_utc)
    open_positions = _load_open_positions_for_schedule(Path(config.storage_path), config.mode.value)
    for symbol, open_meta in open_positions.items():
        fallback = assignments.get(symbol)
        fallback_label = str(fallback.get("label") or "-") if isinstance(fallback, dict) else "-"
        fallback_strategy = str(fallback.get("strategy_name") or config.strategy) if isinstance(fallback, dict) else str(config.strategy)
        fallback_params = dict(fallback.get("strategy_params") or {}) if isinstance(fallback, dict) else {}
        raw_strategy_name = str(open_meta.get("strategy") or fallback_strategy).strip().lower()
        if fallback is not None and raw_strategy_name == str(fallback.get("strategy_name") or "").strip().lower():
            strategy_name = raw_strategy_name
            strategy_params = dict(fallback_params)
        elif raw_strategy_name == _MULTI_STRATEGY_CARRIER_NAME:
            base_strategy_name = str(
                open_meta.get("strategy_entry")
                or fallback_params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM)
                or getattr(config, "strategy", "")
            ).strip().lower()
            strategy_name, strategy_params = _assignment_payload_for_cli(
                config,
                base_strategy_name,
                _strategy_params_for_schedule(config, base_strategy_name),
            )
        else:
            strategy_name = raw_strategy_name
            strategy_params = _strategy_params_for_schedule(config, strategy_name)
        assignments[symbol] = {
            "symbol": symbol,
            "strategy_name": strategy_name,
            "strategy_params": strategy_params,
            "source": "open_position",
            "label": fallback_label if fallback_label != "-" else None,
            "open_position_id": str(open_meta.get("position_id") or "").strip() or None,
            "strategy_entry_component": str(open_meta.get("strategy_entry_component") or "").strip() or None,
            "strategy_entry_signal": str(open_meta.get("strategy_entry_signal") or "").strip() or None,
        }

    schedule_tz = str(config.strategy_schedule_timezone or "UTC")
    local_now = now_utc.astimezone(schedule_timezone)
    print(
        f"Active schedule | timezone={schedule_tz} "
        f"now_utc={now_utc.strftime('%Y-%m-%d %H:%M:%S')} "
        f"now_local={local_now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    )

    configured_strategy_name, configured_strategy_params = _assignment_payload_for_cli(
        config,
        str(getattr(config, "strategy", "") or ""),
        _strategy_params_for_schedule(config, str(getattr(config, "strategy", "") or "")),
    )
    configured_strategy_label, configured_strategy_base = _assignment_strategy_labels_for_cli(
        configured_strategy_name,
        configured_strategy_params,
    )
    if getattr(config, "force_symbols", False):
        print(
            f"  mode=forced_symbols strategy={configured_strategy_label}"
            + (f" strategy_base={configured_strategy_base}" if configured_strategy_base else "")
            + f" symbols={','.join(config.symbols)}"
        )
    elif getattr(config, "force_strategy", False):
        print(
            f"  mode=forced strategy={configured_strategy_label}"
            + (f" strategy_base={configured_strategy_base}" if configured_strategy_base else "")
        )
    elif not config.strategy_schedule:
        print("  mode=static")
    else:
        active_slot_count = 0 if multi_schedule_disabled else len(active_slots)
        print(
            f"  configured_slots={len(config.strategy_schedule)} active_slots={active_slot_count}"
            + (
                " schedule_disabled_by_multi_strategy=yes"
                if multi_schedule_disabled
                else ""
            )
        )
        if active_slots and not multi_schedule_disabled:
            print("Active slots:")
            for entry in active_slots:
                weekdays = ",".join(str(day) for day in entry.weekdays)
                label = entry.label or f"{entry.strategy}@{entry.start_time}-{entry.end_time}"
                symbols = ",".join(entry.symbols)
                entry_strategy_label, entry_strategy_base = _assignment_strategy_labels_for_cli(
                    str(entry.strategy),
                    dict(getattr(entry, "strategy_params", {}) or {}),
                )
                print(
                    f"  {label} | strategy={entry_strategy_label}"
                    + (f" strategy_base={entry_strategy_base}" if entry_strategy_base else "")
                    + f" symbols={symbols} "
                    f"time={entry.start_time}-{entry.end_time} weekdays={weekdays} priority={entry.priority}"
                )

    if not assignments:
        print("No active symbol assignments.")
        return 0

    print("Effective assignments:")
    for symbol in sorted(assignments):
        assignment = assignments[symbol]
        assignment_strategy_label, assignment_strategy_base = _assignment_strategy_labels_for_cli(
            str(assignment.get("strategy_name") or ""),
            dict(assignment.get("strategy_params") or {}),
        )
        print(
            f"  {symbol} | strategy={assignment_strategy_label}"
            + (f" strategy_base={assignment_strategy_base}" if assignment_strategy_base else "")
            + f" source={assignment['source']} "
            f"label={assignment.get('label') or '-'} open_position_id={assignment.get('open_position_id') or '-'}"
            + (
                f" entry_component={assignment.get('strategy_entry_component')}"
                if assignment.get("source") == "open_position" and assignment.get("strategy_entry_component")
                else ""
            )
            + (
                f" entry_signal={assignment.get('strategy_entry_signal')}"
                if assignment.get("source") == "open_position" and assignment.get("strategy_entry_signal")
                else ""
            )
        )
    return len(assignments)


def _show_trades(storage_path: Path, limit: int, open_only: bool = False, strategy: str | None = None) -> int:
    max_items = max(1, int(limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    items: list[dict[str, object]] = []
    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        trade_columns = _table_columns(con, "trades")
        table_names = {
            str(row["name"])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        has_live_join_tables = {
            "worker_states",
            "broker_ticks",
            "broker_symbol_specs",
        }.issubset(table_names)
        has_trade_performance = "trade_performance" in table_names
        confidence_select = (
            ", t.entry_confidence"
            if "entry_confidence" in trade_columns
            else ", 0.0 AS entry_confidence"
        )
        strategy_display_select = f", {_trade_strategy_display_expr(trade_columns, 't')} AS strategy_entry"
        strategy_entry_base_select = (
            f", {_trade_strategy_entry_base_select(trade_columns, 't')} AS strategy_entry_base"
        )
        strategy_component_select = f", {_trade_strategy_component_select(trade_columns, 't')} AS strategy_entry_component"
        strategy_exit_select = f", {_trade_strategy_exit_select(trade_columns, 't')} AS strategy_exit"
        strategy_exit_component_select = (
            f", {_trade_strategy_exit_component_select(trade_columns, 't')} AS strategy_exit_component"
        )
        strategy_exit_signal_select = (
            f", {_trade_strategy_exit_signal_select(trade_columns, 't')} AS strategy_exit_signal"
        )
        close_reason_select = ", tp.close_reason" if has_trade_performance else ", NULL AS close_reason"
        trade_performance_join = (
            "LEFT JOIN trade_performance tp ON tp.position_id = t.position_id"
            if has_trade_performance
            else ""
        )
        where_parts: list[str] = []
        query_params: list[object] = []
        if open_only:
            where_parts.append("t.status = 'open'")
        _append_trade_strategy_filter(
            where_parts=where_parts,
            query_params=query_params,
            trade_columns=trade_columns,
            strategy=strategy,
            table_alias="t",
        )
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        query_params.append(max_items)
        if has_live_join_tables:
            live_select = """
                ws.last_price AS ws_last_price,
                ws.last_heartbeat AS ws_last_heartbeat,
                bt.mid AS tick_mid,
                bt.ts AS tick_ts,
                bss.tick_size AS spec_tick_size,
                bss.tick_value AS spec_tick_value
            """
            from_clause = f"""
                FROM trades t
                LEFT JOIN worker_states ws ON ws.symbol = t.symbol
                LEFT JOIN broker_ticks bt ON bt.symbol = t.symbol
                LEFT JOIN (
                    SELECT symbol, tick_size, tick_value
                    FROM broker_symbol_specs
                    GROUP BY symbol
                ) bss ON bss.symbol = t.symbol
                {trade_performance_join}
            """
        else:
            live_select = """
                NULL AS ws_last_price,
                NULL AS ws_last_heartbeat,
                NULL AS tick_mid,
                NULL AS tick_ts,
                NULL AS spec_tick_size,
                NULL AS spec_tick_value
            """
            from_clause = f"FROM trades t {trade_performance_join}"
        rows = con.execute(
            f"""
            SELECT
                t.position_id, t.symbol, t.side, t.volume, t.open_price, t.stop_loss, t.take_profit,
                t.opened_at{confidence_select}, t.status, t.close_price, t.closed_at, t.pnl, t.thread_name, t.strategy,
                t.mode{strategy_display_select}{strategy_entry_base_select}{strategy_component_select}{strategy_exit_select}{strategy_exit_component_select}{strategy_exit_signal_select}{close_reason_select},
                t.updated_at,
                {live_select}
            {from_clause}
            {where_clause}
            ORDER BY t.updated_at DESC
            LIMIT ?
            """,
            tuple(query_params),
        ).fetchall()
        items = [dict(row) for row in rows]
        _hydrate_closed_trade_rows_with_event_pnl(con, items, table_names=table_names)
    finally:
        con.close()

    if not items:
        print("No trades found.")
        return 0

    for item in items:
        opened_ts = float(item.get("opened_at") or 0.0)
        opened_at = datetime.fromtimestamp(opened_ts).strftime("%Y-%m-%d %H:%M:%S")
        status = str(item.get("status") or "-").upper()
        symbol = str(item.get("symbol") or "-")
        side = str(item.get("side") or "-").upper()
        volume = float(item.get("volume") or 0.0)
        open_price = float(item.get("open_price") or 0.0)
        stop_loss = float(item.get("stop_loss") or 0.0)
        take_profit = float(item.get("take_profit") or 0.0)
        entry_confidence = float(item.get("entry_confidence") or 0.0)
        pnl = float(item.get("pnl") or 0.0)
        strategy, strategy_entry, strategy_base, strategy_exit = _trade_strategy_labels(item)
        confidence_strategy = strategy_entry if strategy_entry not in {"", "-"} else strategy
        mode = str(item.get("mode") or "-")
        position_id = str(item.get("position_id") or "-")
        now_ts = time.time()
        updated_at = _safe_float(item.get("updated_at"))
        updated_age_sec = max(0.0, now_ts - updated_at) if updated_at is not None else None
        confidence_gate, confidence_threshold = _confidence_gate_status(confidence_strategy, mode, entry_confidence)
        strategy_base_text = f" strategy_base={strategy_base}" if strategy_base else ""
        print(
            f"[{opened_at}] {status} {symbol} {side} | "
            f"volume={volume:.4f} open={open_price:.5f} sl={stop_loss:.5f} tp={take_profit:.5f} "
            f"conf={entry_confidence:.3f} conf_gate={confidence_gate}@{confidence_threshold:.3f} "
            f"pnl={pnl:.2f} strategy={strategy} strategy_entry={strategy_entry}{strategy_base_text} strategy_exit={strategy_exit} mode={mode} id={position_id}"
        )
        if status == "OPEN":
            live_mark, live_mark_source, live_mark_age_sec = _resolve_open_trade_mark(
                item,
                now_ts=now_ts,
            )
            live_pnl = _estimate_open_trade_pnl(
                symbol=symbol,
                side=side,
                volume=volume,
                open_price=open_price,
                mark_price=live_mark,
                tick_size=_safe_float(item.get("spec_tick_size")),
                tick_value=_safe_float(item.get("spec_tick_value")),
            )
            live_bits: list[str] = []
            if live_mark is not None:
                live_bits.append(f"mark={live_mark:.5f}")
            if live_pnl is not None:
                live_bits.append(f"pnl_live={live_pnl:.2f}")
            if live_mark_source:
                live_bits.append(f"mark_source={live_mark_source}")
            if live_mark_age_sec is not None:
                live_bits.append(f"mark_age={live_mark_age_sec:.1f}s")
            if updated_age_sec is not None:
                live_bits.append(f"trade_update_age={updated_age_sec:.1f}s")
            if live_bits:
                print("  live: " + " ".join(live_bits))
        closed_at_ts = item.get("closed_at")
        if closed_at_ts is not None:
            closed_at = datetime.fromtimestamp(float(closed_at_ts)).strftime("%Y-%m-%d %H:%M:%S")
            close_price_raw = item.get("close_price")
            if close_price_raw is None:
                print("  closed_at: %s close_price=pending_broker_sync" % closed_at)
            else:
                close_price = float(close_price_raw)
                print(f"  closed_at: {closed_at} close_price={close_price:.5f}")

    return len(rows)


def _pip_size_for_symbol(symbol: str) -> float:
    return symbol_pip_size_fallback(
        symbol,
        index_pip_size=1.0,
        energy_pip_size=1.0,
    )


def _contract_multiplier_for_symbol(symbol: str) -> float:
    upper = str(symbol or "").upper()
    if upper.endswith("USD") or upper.startswith("USD"):
        return 100000.0
    if upper.startswith("XAU"):
        return 100.0
    if upper in {"WTI", "BRENT"}:
        return 100.0
    if upper.startswith("US"):
        return 10.0
    return 1000.0


def _safe_float(raw: object) -> float | None:
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value == value else None


def _show_trades_mark_max_age_sec() -> float:
    raw = str(os.getenv("XTB_CLI_SHOW_TRADES_MARK_MAX_AGE_SEC", "60.0"))
    try:
        return max(1.0, float(raw))
    except (TypeError, ValueError):
        return 60.0


def _resolve_open_trade_mark(item: dict[str, object], *, now_ts: float) -> tuple[float | None, str | None, float | None]:
    max_age_sec = _show_trades_mark_max_age_sec()
    tick_mid = _safe_float(item.get("tick_mid"))
    tick_ts = _safe_float(item.get("tick_ts"))
    if tick_mid is not None and tick_ts is not None:
        tick_age = max(0.0, now_ts - tick_ts)
        if tick_age <= max_age_sec:
            return tick_mid, "tick_cache", tick_age

    ws_last_price = _safe_float(item.get("ws_last_price"))
    ws_last_heartbeat = _safe_float(item.get("ws_last_heartbeat"))
    if ws_last_price is None:
        return None, None, None
    ws_age = max(0.0, now_ts - ws_last_heartbeat) if ws_last_heartbeat is not None else None
    return ws_last_price, "worker_state", ws_age


def _estimate_open_trade_pnl(
    *,
    symbol: str,
    side: str,
    volume: float,
    open_price: float,
    mark_price: float | None,
    tick_size: float | None,
    tick_value: float | None,
) -> float | None:
    if mark_price is None:
        return None
    direction = 1.0 if side.upper() == "BUY" else -1.0
    if tick_size is not None and tick_value is not None and tick_size > 0 and tick_value > 0:
        ticks = (mark_price - open_price) / tick_size
        return direction * ticks * tick_value * volume
    multiplier = _contract_multiplier_for_symbol(symbol)
    return direction * (mark_price - open_price) * volume * multiplier


def _format_duration(seconds: float) -> str:
    total = max(0, int(seconds))
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _risk_estimate_label(risk_to_sl_pct: float) -> str:
    if risk_to_sl_pct >= 100.0:
        return "critical"
    if risk_to_sl_pct >= 75.0:
        return "high"
    if risk_to_sl_pct >= 45.0:
        return "medium"
    return "low"


def _show_open_trades_snapshot(storage_path: Path, strategy: str | None = None) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        trade_columns = _table_columns(con, "trades")
        strategy_entry_select = f"{_trade_strategy_display_expr(trade_columns, 't')} AS strategy_entry"
        strategy_entry_base_select = (
            f"{_trade_strategy_entry_base_select(trade_columns, 't')} AS strategy_entry_base"
        )
        strategy_exit_select = f"{_trade_strategy_exit_select(trade_columns, 't')} AS strategy_exit"
        strategy_exit_component_select = (
            f"{_trade_strategy_exit_component_select(trade_columns, 't')} AS strategy_exit_component"
        )
        strategy_exit_signal_select = f"{_trade_strategy_exit_signal_select(trade_columns, 't')} AS strategy_exit_signal"
        where_parts = ["t.status = 'open'"]
        query_params: list[object] = []
        _append_trade_strategy_filter(
            where_parts=where_parts,
            query_params=query_params,
            trade_columns=trade_columns,
            strategy=strategy,
            table_alias="t",
        )
        where_clause = " AND ".join(where_parts)
        rows = con.execute(
            f"""
            SELECT
                t.position_id,
                t.deal_reference,
                t.symbol,
                t.side,
                t.volume,
                t.open_price,
                t.stop_loss,
                t.take_profit,
                t.opened_at,
                t.entry_confidence,
                t.pnl,
                t.thread_name,
                t.strategy,
                {strategy_entry_select},
                {strategy_entry_base_select},
                {strategy_exit_select},
                {strategy_exit_component_select},
                {strategy_exit_signal_select},
                t.mode,
                t.updated_at,
                ws.last_price,
                ws.last_heartbeat,
                ws.last_error,
                ws.iteration
            FROM trades t
            LEFT JOIN worker_states ws ON ws.symbol = t.symbol
            WHERE {where_clause}
            ORDER BY t.opened_at ASC
            """,
            tuple(query_params),
        ).fetchall()
    finally:
        con.close()

    now_ts = time.time()
    header_ts = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{header_ts}] Open trades live snapshot | count={len(rows)} "
        f"strategy_filter={strategy or '-'}"
    )
    if not rows:
        print("No open trades found.")
        return 0

    for row in rows:
        item = dict(row)
        symbol = str(item.get("symbol") or "-")
        side = str(item.get("side") or "").strip().lower()
        side_text = side.upper() if side else "-"
        position_id = str(item.get("position_id") or "-")
        deal_reference = str(item.get("deal_reference") or "-")
        volume = float(item.get("volume") or 0.0)
        open_price = float(item.get("open_price") or 0.0)
        stop_loss = float(item.get("stop_loss") or 0.0)
        take_profit = float(item.get("take_profit") or 0.0)
        opened_at_ts = float(item.get("opened_at") or 0.0)
        opened_at = datetime.fromtimestamp(opened_at_ts).strftime("%Y-%m-%d %H:%M:%S")
        age_text = _format_duration(now_ts - opened_at_ts)
        entry_confidence = float(item.get("entry_confidence") or 0.0)
        strategy_name, strategy_entry, strategy_base, strategy_exit = _trade_strategy_labels(item)
        confidence_strategy = strategy_entry if strategy_entry not in {"", "-"} else strategy_name
        mode_name = str(item.get("mode") or "-")
        confidence_gate, confidence_threshold = _confidence_gate_status(
            confidence_strategy,
            mode_name,
            entry_confidence,
        )
        trade_updated_at = float(item.get("updated_at") or 0.0)
        trade_update_age = max(0.0, now_ts - trade_updated_at) if trade_updated_at > 0 else 0.0
        heartbeat_ts = float(item.get("last_heartbeat") or 0.0)
        heartbeat_age = max(0.0, now_ts - heartbeat_ts) if heartbeat_ts > 0 else 0.0
        iteration = int(item.get("iteration") or 0)
        worker_last_error = str(item.get("last_error") or "").strip()

        last_price_raw = item.get("last_price")
        try:
            mark_price = float(last_price_raw) if last_price_raw is not None else float(open_price)
        except (TypeError, ValueError):
            mark_price = float(open_price)
        pnl = float(item.get("pnl") or 0.0)
        pip_size = max(_pip_size_for_symbol(symbol), FLOAT_COMPARISON_TOLERANCE)
        stop_distance = abs(open_price - stop_loss)
        tp_distance = abs(take_profit - open_price)

        if side == "buy":
            favorable_move = max(0.0, mark_price - open_price)
            adverse_move = max(0.0, open_price - mark_price)
            dist_to_sl = max(0.0, mark_price - stop_loss)
            dist_to_tp = max(0.0, take_profit - mark_price)
        else:
            favorable_move = max(0.0, open_price - mark_price)
            adverse_move = max(0.0, mark_price - open_price)
            dist_to_sl = max(0.0, stop_loss - mark_price)
            dist_to_tp = max(0.0, mark_price - take_profit)

        risk_to_sl_pct = (adverse_move / stop_distance * 100.0) if stop_distance > FLOAT_COMPARISON_TOLERANCE else 0.0
        progress_to_tp_pct = (favorable_move / tp_distance * 100.0) if tp_distance > FLOAT_COMPARISON_TOLERANCE else 0.0
        rr_planned = (tp_distance / stop_distance) if stop_distance > FLOAT_COMPARISON_TOLERANCE else 0.0
        risk_label = _risk_estimate_label(risk_to_sl_pct)

        print(
            f"{symbol} {side_text} | opened={opened_at} age={age_text} "
            f"id_local={position_id} id_ig={deal_reference} mode={mode_name} "
            f"strategy={strategy_name} strategy_entry={strategy_entry}"
            + (f" strategy_base={strategy_base}" if strategy_base else "")
            + f" strategy_exit={strategy_exit}"
        )
        print(
            f"  volume={volume:.4f} mark={mark_price:.5f} open={open_price:.5f} sl={stop_loss:.5f} tp={take_profit:.5f} "
            f"pnl={pnl:.2f} conf={entry_confidence:.3f} conf_gate={confidence_gate}@{confidence_threshold:.3f}"
        )
        print(
            f"  risk_estimate={risk_label} risk_to_sl={risk_to_sl_pct:.1f}% progress_to_tp={progress_to_tp_pct:.1f}% "
            f"rr_planned={rr_planned:.2f} dist_to_sl={dist_to_sl / pip_size:.1f}p dist_to_tp={dist_to_tp / pip_size:.1f}p "
            f"adverse={adverse_move / pip_size:.1f}p favorable={favorable_move / pip_size:.1f}p"
        )
        print(
            f"  worker_age={heartbeat_age:.1f}s trade_update_age={trade_update_age:.1f}s iteration={iteration}"
            + (f" last_error={worker_last_error}" if worker_last_error else "")
        )

    return len(rows)


def _watch_open_trades(
    storage_path: Path,
    interval_sec: float,
    strategy: str | None = None,
    iterations: int = 0,
) -> int:
    refresh = max(0.5, float(interval_sec))
    max_iterations = max(0, int(iterations))
    cycle = 0
    last_count = 0
    try:
        while True:
            started_at = time.time()
            last_count = _show_open_trades_snapshot(storage_path, strategy=strategy)
            cycle += 1
            if max_iterations > 0 and cycle >= max_iterations:
                break
            elapsed = max(0.0, time.time() - started_at)
            wait_sec = max(0.0, refresh - elapsed)
            if wait_sec > 0:
                time.sleep(wait_sec)
            print("")
    except KeyboardInterrupt:
        print("Stopped open-trades watch.")
    return last_count


def _confidence_bucket_label(value: float) -> str:
    if value < 0.25:
        return "[0.00,0.25)"
    if value < 0.50:
        return "[0.25,0.50)"
    if value < 0.75:
        return "[0.50,0.75)"
    return "[0.75,1.00]"


def _confidence_threshold_reference(strategy: str, mode: str) -> float:
    raw = resolve_strategy_param(
        DEFAULT_STRATEGY_PARAMS,
        strategy,
        "min_confidence_for_entry",
        0.0,
        mode=mode,
    )
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = 0.0
    return max(0.0, min(1.0, value))


def _confidence_gate_status(strategy: str, mode: str, entry_confidence: float) -> tuple[str, float]:
    threshold = _confidence_threshold_reference(strategy, mode)
    if threshold <= 0:
        return "off", threshold
    if max(0.0, min(1.0, entry_confidence)) + FLOAT_ROUNDING_TOLERANCE >= threshold:
        return "pass", threshold
    return "below", threshold


def _show_trade_confidence(storage_path: Path, strategy: str | None = None) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        trade_columns = {
            str(row["name"])
            for row in con.execute("PRAGMA table_info(trades)").fetchall()
        }
        if "entry_confidence" not in trade_columns:
            print("No trade confidence data found in trades table.")
            return 0

        strategy_expr = _trade_strategy_display_expr(trade_columns, "")
        query = f"""
            SELECT position_id, {strategy_expr} AS strategy, mode, status, pnl, entry_confidence
            FROM trades
            """
        query_params: list[object] = []
        if strategy:
            query += f"WHERE LOWER({strategy_expr}) = LOWER(?)\n"
            query_params.append(str(strategy))
        query += """
            ORDER BY updated_at DESC
            """
        rows = con.execute(query, tuple(query_params)).fetchall()
    finally:
        con.close()

    if not rows:
        print("No trades found.")
        return 0

    closed_rows: list[dict[str, object]] = []
    open_count = 0
    for row in rows:
        item = dict(row)
        if str(item.get("status") or "").lower() == "closed":
            closed_rows.append(item)
        else:
            open_count += 1

    if not closed_rows:
        print("No closed trades found for confidence analysis.")
        return 0

    bucket_order = ("[0.00,0.25)", "[0.25,0.50)", "[0.50,0.75)", "[0.75,1.00]")
    bucket_stats: dict[str, dict[str, float]] = {
        label: {"count": 0.0, "wins": 0.0, "pnl_sum": 0.0, "conf_sum": 0.0}
        for label in bucket_order
    }
    strategy_stats: dict[str, dict[str, float]] = {}
    strategy_mode_stats: dict[tuple[str, str], dict[str, float]] = {}
    zero_conf_closed = 0
    pnl_sum = 0.0
    conf_sum = 0.0
    wins = 0

    for item in closed_rows:
        pnl = float(item.get("pnl") or 0.0)
        conf = max(0.0, min(1.0, float(item.get("entry_confidence") or 0.0)))
        strategy = str(item.get("strategy") or "-")
        mode = str(item.get("mode") or "-").lower()
        if abs(conf) <= FLOAT_ROUNDING_TOLERANCE:
            zero_conf_closed += 1
        if pnl > 0:
            wins += 1
        pnl_sum += pnl
        conf_sum += conf

        bucket = _confidence_bucket_label(conf)
        bucket_item = bucket_stats[bucket]
        bucket_item["count"] += 1.0
        bucket_item["pnl_sum"] += pnl
        bucket_item["conf_sum"] += conf
        if pnl > 0:
            bucket_item["wins"] += 1.0

        strategy_item = strategy_stats.setdefault(
            strategy,
            {"count": 0.0, "wins": 0.0, "pnl_sum": 0.0, "conf_sum": 0.0},
        )
        strategy_item["count"] += 1.0
        strategy_item["pnl_sum"] += pnl
        strategy_item["conf_sum"] += conf
        if pnl > 0:
            strategy_item["wins"] += 1.0

        strategy_mode_item = strategy_mode_stats.setdefault(
            (strategy, mode),
            {"count": 0.0, "wins": 0.0, "pnl_sum": 0.0, "conf_sum": 0.0},
        )
        strategy_mode_item["count"] += 1.0
        strategy_mode_item["pnl_sum"] += pnl
        strategy_mode_item["conf_sum"] += conf
        if pnl > 0:
            strategy_mode_item["wins"] += 1.0

    closed_count = len(closed_rows)
    avg_conf = conf_sum / closed_count if closed_count else 0.0
    win_rate = (wins / closed_count * 100.0) if closed_count else 0.0
    nonzero_conf_closed = closed_count - zero_conf_closed

    print("Trade confidence analysis:")
    print(
        f"closed_trades={closed_count} open_trades={open_count} "
        f"avg_conf={avg_conf:.3f} win_rate={win_rate:.1f}% pnl_sum={pnl_sum:.2f}"
    )
    print(
        f"closed_with_nonzero_conf={nonzero_conf_closed} "
        f"closed_with_zero_conf={zero_conf_closed}"
    )
    print("buckets:")
    for label in bucket_order:
        item = bucket_stats[label]
        count = int(item["count"])
        if count <= 0:
            continue
        bucket_win_rate = item["wins"] / count * 100.0
        avg_bucket_pnl = item["pnl_sum"] / count
        avg_bucket_conf = item["conf_sum"] / count
        print(
            f"  {label} | trades={count} avg_conf={avg_bucket_conf:.3f} "
            f"win_rate={bucket_win_rate:.1f}% pnl_sum={item['pnl_sum']:.2f} avg_pnl={avg_bucket_pnl:.2f}"
        )

    print("by_strategy:")
    for strategy, item in sorted(
        strategy_stats.items(),
        key=lambda pair: (-int(pair[1]["count"]), pair[0]),
    ):
        count = int(item["count"])
        strategy_win_rate = item["wins"] / count * 100.0 if count else 0.0
        avg_strategy_conf = item["conf_sum"] / count if count else 0.0
        avg_strategy_pnl = item["pnl_sum"] / count if count else 0.0
        ref_signal_only = _confidence_threshold_reference(strategy, "signal_only")
        ref_paper = _confidence_threshold_reference(strategy, "paper")
        ref_execution = _confidence_threshold_reference(strategy, "execution")
        print(
            f"  {strategy} | trades={count} avg_conf={avg_strategy_conf:.3f} "
            f"win_rate={strategy_win_rate:.1f}% pnl_sum={item['pnl_sum']:.2f} avg_pnl={avg_strategy_pnl:.2f} "
            f"ref_signal_only={ref_signal_only:.3f} ref_paper={ref_paper:.3f} ref_execution={ref_execution:.3f}"
        )

    print("by_strategy_mode:")
    for (strategy, mode), item in sorted(
        strategy_mode_stats.items(),
        key=lambda pair: (-int(pair[1]["count"]), pair[0][0], pair[0][1]),
    ):
        count = int(item["count"])
        mode_win_rate = item["wins"] / count * 100.0 if count else 0.0
        avg_mode_conf = item["conf_sum"] / count if count else 0.0
        avg_mode_pnl = item["pnl_sum"] / count if count else 0.0
        ref_threshold = _confidence_threshold_reference(strategy, mode)
        print(
            f"  {strategy}/{mode} | trades={count} avg_conf={avg_mode_conf:.3f} "
            f"threshold={ref_threshold:.3f} win_rate={mode_win_rate:.1f}% "
            f"pnl_sum={item['pnl_sum']:.2f} avg_pnl={avg_mode_pnl:.2f}"
        )

    print("note: older trades created before confidence persistence may appear with conf=0.000")
    return closed_count


def _show_fill_quality(storage_path: Path, strategy: str | None = None, limit: int = 15) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        trade_columns = _table_columns(con, "trades")
        performance_columns = _table_columns(con, "trade_performance")
        required_trade_columns = {"symbol", "strategy", "status"}
        required_performance_columns = {
            "entry_slippage_pips",
            "exit_slippage_pips",
            "entry_adverse_slippage_pips",
            "exit_adverse_slippage_pips",
        }
        if not required_trade_columns <= trade_columns or not required_performance_columns <= performance_columns:
            print("No fill quality data found in trade_performance table.")
            return 0

        display_strategy_expr = _trade_strategy_display_expr(trade_columns, "t")
        filter_strategy_expr = f"LOWER({display_strategy_expr})"
        where_parts = [
            "t.status = 'closed'",
            (
                "("
                "tp.entry_slippage_pips IS NOT NULL "
                "OR tp.exit_slippage_pips IS NOT NULL "
                "OR tp.entry_adverse_slippage_pips IS NOT NULL "
                "OR tp.exit_adverse_slippage_pips IS NOT NULL"
                ")"
            ),
        ]
        query_params: list[object] = []
        if strategy:
            where_parts.append(f"{filter_strategy_expr} = LOWER(?)")
            query_params.append(str(strategy))
        where_clause = " AND ".join(where_parts)
        row_limit = max(1, int(limit))

        aggregate_sql = """
            COUNT(*) AS sample_count,
            AVG(COALESCE(tp.entry_slippage_pips, 0.0)) AS avg_entry_slippage_pips,
            AVG(COALESCE(tp.exit_slippage_pips, 0.0)) AS avg_exit_slippage_pips,
            AVG(COALESCE(tp.entry_adverse_slippage_pips, 0.0)) AS avg_entry_adverse_slippage_pips,
            AVG(COALESCE(tp.exit_adverse_slippage_pips, 0.0)) AS avg_exit_adverse_slippage_pips,
            AVG(COALESCE(tp.entry_adverse_slippage_pips, 0.0) + COALESCE(tp.exit_adverse_slippage_pips, 0.0))
                AS avg_total_adverse_slippage_pips,
            AVG(COALESCE(tp.entry_slippage_pips, 0.0) + COALESCE(tp.exit_slippage_pips, 0.0))
                AS avg_total_slippage_pips,
            AVG(
                CASE
                    WHEN (COALESCE(tp.entry_adverse_slippage_pips, 0.0) + COALESCE(tp.exit_adverse_slippage_pips, 0.0)) > 0.0
                    THEN 1.0
                    ELSE 0.0
                END
            ) AS adverse_trade_share
        """
        overall = con.execute(
            f"""
            SELECT {aggregate_sql}
            FROM trades t
            JOIN trade_performance tp ON tp.position_id = t.position_id
            WHERE {where_clause}
            """,
            tuple(query_params),
        ).fetchone()
        strategy_rows = con.execute(
            f"""
            SELECT
                {display_strategy_expr} AS strategy_entry,
                {aggregate_sql}
            FROM trades t
            JOIN trade_performance tp ON tp.position_id = t.position_id
            WHERE {where_clause}
            GROUP BY strategy_entry
            ORDER BY
                avg_total_adverse_slippage_pips DESC,
                adverse_trade_share DESC,
                sample_count DESC,
                strategy_entry ASC
            LIMIT ?
            """,
            (*query_params, row_limit),
        ).fetchall()
        pair_rows = con.execute(
            f"""
            SELECT
                t.symbol,
                {display_strategy_expr} AS strategy_entry,
                {aggregate_sql}
            FROM trades t
            JOIN trade_performance tp ON tp.position_id = t.position_id
            WHERE {where_clause}
            GROUP BY t.symbol, strategy_entry
            ORDER BY
                avg_total_adverse_slippage_pips DESC,
                adverse_trade_share DESC,
                sample_count DESC,
                t.symbol ASC,
                strategy_entry ASC
            LIMIT ?
            """,
            (*query_params, row_limit),
        ).fetchall()
    finally:
        con.close()

    total_samples = int((overall["sample_count"] if overall is not None else 0) or 0)
    if total_samples <= 0:
        print("No closed trades found for fill-quality analysis.")
        return 0

    def _pips_text(value: object) -> str:
        return f"{float(value or 0.0):.2f}p"

    def _pct_text(value: object) -> str:
        return f"{float(value or 0.0) * 100.0:.1f}%"

    print("Fill quality analysis:")
    print(
        f"closed_trades_with_fill_quality={total_samples} strategy_filter={strategy or '-'} "
        f"avg_entry_slip={_pips_text(overall['avg_entry_slippage_pips'])} "
        f"avg_exit_slip={_pips_text(overall['avg_exit_slippage_pips'])} "
        f"avg_total_adverse={_pips_text(overall['avg_total_adverse_slippage_pips'])} "
        f"adverse_fill_share={_pct_text(overall['adverse_trade_share'])}"
    )
    print("strategies (worst first):")
    for row in strategy_rows:
        item = dict(row)
        print(
            f"  {item['strategy_entry']} | samples={int(item['sample_count'] or 0)} "
            f"avg_entry={_pips_text(item['avg_entry_slippage_pips'])} "
            f"avg_exit={_pips_text(item['avg_exit_slippage_pips'])} "
            f"avg_entry_adv={_pips_text(item['avg_entry_adverse_slippage_pips'])} "
            f"avg_exit_adv={_pips_text(item['avg_exit_adverse_slippage_pips'])} "
            f"avg_total_adv={_pips_text(item['avg_total_adverse_slippage_pips'])} "
            f"adverse_share={_pct_text(item['adverse_trade_share'])}"
        )
    print("symbol_strategy (worst first):")
    for row in pair_rows:
        item = dict(row)
        print(
            f"  {item['symbol']} {item['strategy_entry']} | samples={int(item['sample_count'] or 0)} "
            f"avg_entry={_pips_text(item['avg_entry_slippage_pips'])} "
            f"avg_exit={_pips_text(item['avg_exit_slippage_pips'])} "
            f"avg_entry_adv={_pips_text(item['avg_entry_adverse_slippage_pips'])} "
            f"avg_exit_adv={_pips_text(item['avg_exit_adverse_slippage_pips'])} "
            f"avg_total_adv={_pips_text(item['avg_total_adverse_slippage_pips'])} "
            f"adverse_share={_pct_text(item['adverse_trade_share'])}"
        )
    return total_samples


def _show_strategy_scorecard(storage_path: Path, strategy: str | None = None, limit: int = 12) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    def _float_or_none(value: object) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))

    def _profit_factor(gross_profit: float, gross_loss: float) -> float | None:
        if gross_profit <= 0.0 and gross_loss >= 0.0:
            return 0.0
        if gross_loss < 0.0:
            return gross_profit / abs(gross_loss)
        if gross_profit > 0.0:
            return float("inf")
        return None

    def _alpha_score(item: dict[str, object]) -> tuple[float, str]:
        sample_count = max(1, int(item.get("sample_count") or 0))
        win_rate_pct = _as_float(item.get("win_rate_pct"), 0.0)
        avg_confidence = _float_or_none(item.get("avg_confidence"))
        avg_capture_ratio = _float_or_none(item.get("avg_capture_ratio"))
        avg_pnl = _as_float(item.get("avg_pnl"), 0.0)
        avg_mae_pnl = _float_or_none(item.get("avg_mae_pnl"))
        profit_factor = _float_or_none(item.get("profit_factor"))

        if profit_factor is None:
            profit_factor_score = 45.0
        elif profit_factor == float("inf"):
            profit_factor_score = 100.0
        else:
            profit_factor_score = _clamp((profit_factor - 0.8) / 1.2 * 100.0, 0.0, 100.0)

        capture_score = 50.0
        if avg_capture_ratio is not None:
            capture_score = _clamp(
                50.0 + 40.0 * _clamp(avg_capture_ratio, -1.0, 1.5),
                0.0,
                100.0,
            )

        if avg_mae_pnl is None or abs(avg_mae_pnl) <= FLOAT_COMPARISON_TOLERANCE:
            efficiency_score = 55.0 if avg_pnl >= 0.0 else 20.0
        else:
            efficiency = avg_pnl / max(abs(avg_mae_pnl), FLOAT_COMPARISON_TOLERANCE)
            efficiency_score = _clamp(
                50.0 + 35.0 * _clamp(efficiency, -1.0, 1.5),
                0.0,
                100.0,
            )

        confidence_score = 50.0 if avg_confidence is None else _clamp(avg_confidence * 100.0, 0.0, 100.0)
        alpha_base = (
            0.30 * profit_factor_score
            + 0.30 * _clamp(win_rate_pct, 0.0, 100.0)
            + 0.20 * capture_score
            + 0.10 * efficiency_score
            + 0.10 * confidence_score
        )
        sample_factor = min(1.0, sample_count / 4.0)
        alpha_score = alpha_base * (0.65 + 0.35 * sample_factor)
        alpha_label = "strong" if alpha_score >= 72.0 else ("okay" if alpha_score >= 55.0 else "weak")
        return alpha_score, alpha_label

    def _execution_score(item: dict[str, object]) -> tuple[float | None, str]:
        fill_sample_count = int(item.get("fill_sample_count") or 0)
        avg_total_adverse = _float_or_none(item.get("avg_total_adverse_slippage_pips"))
        adverse_fill_share = _float_or_none(item.get("adverse_fill_share"))
        if fill_sample_count <= 0 or avg_total_adverse is None or adverse_fill_share is None:
            return None, "unknown"

        adverse_share_pct = _clamp(adverse_fill_share * 100.0, 0.0, 100.0)
        cost_score = _clamp(100.0 - max(0.0, avg_total_adverse) * 18.0, 0.0, 100.0)
        share_score = _clamp(100.0 - adverse_share_pct * 0.30, 0.0, 100.0)
        execution_base = 0.70 * cost_score + 0.30 * share_score
        fill_factor = min(1.0, fill_sample_count / 4.0)
        execution_score = execution_base * (0.75 + 0.25 * fill_factor)
        execution_label = "clean" if execution_score >= 72.0 else ("watch" if execution_score >= 55.0 else "poor")
        return execution_score, execution_label

    def _decorate_scorecard_row(row: sqlite3.Row) -> dict[str, object]:
        item = dict(row)
        sample_count = int(item.get("sample_count") or 0)
        win_count = int(item.get("win_count") or 0)
        pnl_sum = _as_float(item.get("pnl_sum"), 0.0)
        avg_pnl = _as_float(item.get("avg_pnl"), 0.0)
        gross_profit = _as_float(item.get("gross_profit"), 0.0)
        gross_loss = _as_float(item.get("gross_loss"), 0.0)
        profit_factor = _profit_factor(gross_profit, gross_loss)
        item["sample_count"] = sample_count
        item["win_rate_pct"] = (win_count / sample_count * 100.0) if sample_count else 0.0
        item["pnl_sum"] = pnl_sum
        item["avg_pnl"] = avg_pnl
        item["profit_factor"] = profit_factor
        alpha_score, alpha_label = _alpha_score(item)
        execution_score, execution_label = _execution_score(item)
        management_score = alpha_score * 0.85 if execution_score is None else (alpha_score * 0.65 + execution_score * 0.35)
        if alpha_score >= 72.0 and (execution_score is None or execution_score >= 72.0):
            verdict = "promote"
        elif alpha_score >= 65.0 and execution_score is not None and execution_score < 55.0:
            verdict = "tighten_execution"
        elif alpha_score < 45.0 and execution_score is not None and execution_score >= 60.0:
            verdict = "rework_alpha"
        elif alpha_score < 45.0:
            verdict = "demote"
        elif management_score >= 62.0:
            verdict = "keep"
        else:
            verdict = "watch"
        item["alpha_score"] = alpha_score
        item["alpha_label"] = alpha_label
        item["execution_score"] = execution_score
        item["execution_label"] = execution_label
        item["management_score"] = management_score
        item["verdict"] = verdict
        return item

    def _score_text(value: float | None) -> str:
        return "na" if value is None else f"{value:.0f}"

    def _money_text(value: object) -> str:
        return f"{_as_float(value, 0.0):.2f}"

    def _ratio_text(value: object) -> str:
        ratio = _float_or_none(value)
        return "na" if ratio is None else f"{ratio:.2f}"

    def _pips_text(value: object) -> str:
        pips = _float_or_none(value)
        return "na" if pips is None else f"{pips:.2f}p"

    def _pct_text(value: object) -> str:
        pct = _float_or_none(value)
        return "na" if pct is None else f"{pct * 100.0:.1f}%"

    con = _connect_readonly_db(storage_path)
    try:
        con.row_factory = sqlite3.Row
        trade_columns = _table_columns(con, "trades")
        performance_columns = _table_columns(con, "trade_performance")
        required_trade_columns = {"symbol", "strategy", "status", "pnl"}
        if not required_trade_columns <= trade_columns:
            print("No strategy scorecard data found in trades table.")
            return 0

        display_strategy_expr = _trade_strategy_display_expr(trade_columns, "t")
        filter_strategy_expr = f"LOWER({display_strategy_expr})"
        where_parts = ["t.status = 'closed'"]
        query_params: list[object] = []
        if strategy:
            where_parts.append(f"{filter_strategy_expr} = LOWER(?)")
            query_params.append(str(strategy))
        where_clause = " AND ".join(where_parts)
        row_limit = max(1, int(limit))

        join_clause = (
            "LEFT JOIN trade_performance tp ON tp.position_id = t.position_id"
            if performance_columns
            else ""
        )
        avg_confidence_expr = (
            "AVG(COALESCE(t.entry_confidence, 0.0)) AS avg_confidence"
            if "entry_confidence" in trade_columns
            else "NULL AS avg_confidence"
        )
        capture_expr = "NULL AS avg_capture_ratio"
        avg_mfe_expr = "NULL AS avg_mfe_pnl"
        avg_mae_expr = "NULL AS avg_mae_pnl"
        if {"max_favorable_pnl", "max_adverse_pnl"} <= performance_columns:
            avg_mfe_expr = "AVG(tp.max_favorable_pnl) AS avg_mfe_pnl"
            avg_mae_expr = "AVG(tp.max_adverse_pnl) AS avg_mae_pnl"
            capture_expr = """
                AVG(
                    CASE
                        WHEN tp.max_favorable_pnl > 0.0 THEN t.pnl / tp.max_favorable_pnl
                        ELSE NULL
                    END
                ) AS avg_capture_ratio
            """

        fill_sample_expr = "0 AS fill_sample_count"
        avg_total_adverse_expr = "NULL AS avg_total_adverse_slippage_pips"
        adverse_share_expr = "NULL AS adverse_fill_share"
        fill_metric_columns = {
            "entry_slippage_pips",
            "exit_slippage_pips",
            "entry_adverse_slippage_pips",
            "exit_adverse_slippage_pips",
        }
        if fill_metric_columns <= performance_columns:
            has_fill_expr = (
                "("
                "tp.entry_slippage_pips IS NOT NULL "
                "OR tp.exit_slippage_pips IS NOT NULL "
                "OR tp.entry_adverse_slippage_pips IS NOT NULL "
                "OR tp.exit_adverse_slippage_pips IS NOT NULL"
                ")"
            )
            fill_sample_expr = f"""
                SUM(
                    CASE
                        WHEN {has_fill_expr} THEN 1
                        ELSE 0
                    END
                ) AS fill_sample_count
            """
            avg_total_adverse_expr = f"""
                AVG(
                    CASE
                        WHEN {has_fill_expr}
                        THEN COALESCE(tp.entry_adverse_slippage_pips, 0.0) + COALESCE(tp.exit_adverse_slippage_pips, 0.0)
                        ELSE NULL
                    END
                ) AS avg_total_adverse_slippage_pips
            """
            adverse_share_expr = f"""
                AVG(
                    CASE
                        WHEN {has_fill_expr}
                        THEN CASE
                            WHEN (COALESCE(tp.entry_adverse_slippage_pips, 0.0) + COALESCE(tp.exit_adverse_slippage_pips, 0.0)) > 0.0
                            THEN 1.0
                            ELSE 0.0
                        END
                        ELSE NULL
                    END
                ) AS adverse_fill_share
            """

        aggregate_sql = f"""
            COUNT(*) AS sample_count,
            SUM(CASE WHEN COALESCE(t.pnl, 0.0) > 0.0 THEN 1 ELSE 0 END) AS win_count,
            SUM(COALESCE(t.pnl, 0.0)) AS pnl_sum,
            AVG(COALESCE(t.pnl, 0.0)) AS avg_pnl,
            SUM(CASE WHEN COALESCE(t.pnl, 0.0) > 0.0 THEN COALESCE(t.pnl, 0.0) ELSE 0.0 END) AS gross_profit,
            SUM(CASE WHEN COALESCE(t.pnl, 0.0) < 0.0 THEN COALESCE(t.pnl, 0.0) ELSE 0.0 END) AS gross_loss,
            {avg_confidence_expr},
            {avg_mfe_expr},
            {avg_mae_expr},
            {capture_expr},
            {fill_sample_expr},
            {avg_total_adverse_expr},
            {adverse_share_expr}
        """
        overall = con.execute(
            f"""
            SELECT {aggregate_sql}
            FROM trades t
            {join_clause}
            WHERE {where_clause}
            """,
            tuple(query_params),
        ).fetchone()
        strategy_rows = con.execute(
            f"""
            SELECT
                {display_strategy_expr} AS strategy_entry,
                {aggregate_sql}
            FROM trades t
            {join_clause}
            WHERE {where_clause}
            GROUP BY strategy_entry
            """,
            tuple(query_params),
        ).fetchall()
        pair_rows = con.execute(
            f"""
            SELECT
                t.symbol,
                {display_strategy_expr} AS strategy_entry,
                {aggregate_sql}
            FROM trades t
            {join_clause}
            WHERE {where_clause}
            GROUP BY t.symbol, strategy_entry
            """,
            tuple(query_params),
        ).fetchall()
    finally:
        con.close()

    total_closed = int((overall["sample_count"] if overall is not None else 0) or 0)
    if total_closed <= 0:
        print("No closed trades found for strategy scorecard.")
        return 0

    ranked_strategies = [_decorate_scorecard_row(row) for row in strategy_rows]
    ranked_strategies.sort(
        key=lambda item: (
            -_as_float(item.get("management_score"), 0.0),
            -item["sample_count"],
            str(item.get("strategy_entry") or ""),
        )
    )
    ranked_pairs = [_decorate_scorecard_row(row) for row in pair_rows]
    ranked_pairs.sort(
        key=lambda item: (
            _as_float(item.get("management_score"), 0.0),
            -_as_float(item.get("avg_total_adverse_slippage_pips"), 0.0),
            -item["sample_count"],
            str(item.get("symbol") or ""),
            str(item.get("strategy_entry") or ""),
        )
    )
    strategy_view = ranked_strategies[:row_limit]
    hotspot_view = ranked_pairs[:row_limit]
    avg_alpha_score = (
        sum(_as_float(item.get("alpha_score"), 0.0) for item in ranked_strategies) / len(ranked_strategies)
        if ranked_strategies
        else 0.0
    )
    execution_scores = [
        _as_float(item.get("execution_score"), 0.0)
        for item in ranked_strategies
        if item.get("execution_score") is not None
    ]
    avg_execution_score = (
        sum(execution_scores) / len(execution_scores)
        if execution_scores
        else None
    )

    print("Strategy scorecard:")
    print(
        f"closed_trades={total_closed} strategy_filter={strategy or '-'} "
        f"strategies={len(ranked_strategies)} fill_quality_samples={int(_as_float(overall['fill_sample_count'], 0.0))} "
        f"avg_alpha={avg_alpha_score:.0f} avg_execution={_score_text(avg_execution_score)}"
    )
    print("ranked_strategies:")
    for item in strategy_view:
        print(
            f"  {item['strategy_entry']} | score={_score_text(_float_or_none(item.get('management_score')))} "
            f"alpha={_score_text(_float_or_none(item.get('alpha_score')))}/{item['alpha_label']} "
            f"exec={_score_text(_float_or_none(item.get('execution_score')))}/{item['execution_label']} "
            f"verdict={item['verdict']} trades={item['sample_count']} "
            f"win_rate={item['win_rate_pct']:.1f}% pnl_sum={_money_text(item['pnl_sum'])} avg_pnl={_money_text(item['avg_pnl'])} "
            f"profit_factor={_ratio_text(item['profit_factor'])} capture={_ratio_text(item['avg_capture_ratio'])} "
            f"avg_total_adv={_pips_text(item['avg_total_adverse_slippage_pips'])} "
            f"adverse_fill={_pct_text(item['adverse_fill_share'])}"
        )
    print("hotspots:")
    for item in hotspot_view:
        print(
            f"  {item['symbol']} {item['strategy_entry']} | score={_score_text(_float_or_none(item.get('management_score')))} "
            f"alpha={_score_text(_float_or_none(item.get('alpha_score')))}/{item['alpha_label']} "
            f"exec={_score_text(_float_or_none(item.get('execution_score')))}/{item['execution_label']} "
            f"verdict={item['verdict']} trades={item['sample_count']} pnl_sum={_money_text(item['pnl_sum'])} "
            f"avg_total_adv={_pips_text(item['avg_total_adverse_slippage_pips'])} "
            f"adverse_fill={_pct_text(item['adverse_fill_share'])}"
        )
    return total_closed


def _as_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _show_ig_allowance(storage_path: Path, window: int, symbols_limit: int) -> int:
    scan_items = max(1, int(window))
    max_symbols = max(1, int(symbols_limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    try:
        events = _load_events_readonly(storage_path, limit=scan_items)
    except sqlite3.OperationalError as exc:
        print(f"State DB is busy, retry in a moment: {exc}")
        return 0
    try:
        rate_limit_workers = _load_ig_rate_limit_workers_readonly(storage_path)
    except sqlite3.OperationalError:
        rate_limit_workers = []

    allowance_exceeded = 0
    cooldown_active = 0
    last_allowance_ts = 0.0
    allowance_by_symbol: dict[str, int] = {}
    cooldown_by_symbol: dict[str, int] = {}
    latest_stream_metrics: dict[str, dict[str, object]] = {}

    for event in events:
        message = str(event.get("message") or "")
        symbol = str(event.get("symbol") or "-")
        payload_raw = event.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        ts = _as_float(event.get("ts"), 0.0)

        if message == "Stream usage metrics" and symbol not in latest_stream_metrics:
            latest_stream_metrics[symbol] = payload

        if message != "Broker error":
            continue

        error_text = str(payload.get("error") or "")
        lowered = error_text.lower()
        if "exceeded-account-allowance" in lowered or "exceeded-api-key-allowance" in lowered:
            allowance_exceeded += 1
            allowance_by_symbol[symbol] = allowance_by_symbol.get(symbol, 0) + 1
            last_allowance_ts = max(last_allowance_ts, ts)

        if "allowance cooldown is active" in lowered:
            cooldown_active += 1
            cooldown_by_symbol[symbol] = cooldown_by_symbol.get(symbol, 0) + 1
            last_allowance_ts = max(last_allowance_ts, ts)

    if allowance_exceeded == 0 and cooldown_active == 0 and not latest_stream_metrics and not rate_limit_workers:
        print("No IG allowance data found.")
        return 0

    print(f"IG allowance diagnostics (last {scan_items} events):")
    print(f"allowance_exceeded_errors={allowance_exceeded}")
    print(f"cooldown_active_errors={cooldown_active}")
    if last_allowance_ts > 0:
        ts_text = datetime.fromtimestamp(last_allowance_ts).strftime("%Y-%m-%d %H:%M:%S")
        print(f"last_allowance_error_at={ts_text}")

    if allowance_by_symbol:
        print("allowance_exceeded_by_symbol:")
        for symbol, count in sorted(allowance_by_symbol.items(), key=lambda item: (-item[1], item[0]))[:max_symbols]:
            print(f"  {symbol}: {count}")

    if cooldown_by_symbol:
        print("cooldown_active_by_symbol:")
        for symbol, count in sorted(cooldown_by_symbol.items(), key=lambda item: (-item[1], item[0]))[:max_symbols]:
            print(f"  {symbol}: {count}")

    if latest_stream_metrics:
        print("latest_stream_usage_metrics:")
        ranked_symbols = sorted(
            latest_stream_metrics.items(),
            key=lambda item: -int(_as_float(item[1].get("rest_fallback_hits_total"), 0.0)),
        )[:max_symbols]
        for symbol, payload in ranked_symbols:
            requests = int(_as_float(payload.get("price_requests_total"), 0.0))
            stream_hits = int(_as_float(payload.get("stream_hits_total"), 0.0))
            rest_hits = int(_as_float(payload.get("rest_fallback_hits_total"), 0.0))
            hit_rate = payload.get("stream_hit_rate_pct")
            stream_reason = str(payload.get("stream_reason") or "-")
            if hit_rate is None:
                hit_rate_text = "-"
            else:
                hit_rate_text = f"{_as_float(hit_rate, 0.0):.2f}%"
            print(
                f"  {symbol} | requests={requests} stream_hits={stream_hits} "
                f"rest_fallback_hits={rest_hits} stream_hit_rate={hit_rate_text} reason={stream_reason}"
            )

    if rate_limit_workers:
        print("rate_limit_worker_pressure:")
        now_ts = time.time()
        scored_workers: list[tuple[float, str, dict[str, object], str]] = []
        for row in rate_limit_workers:
            worker_key = str(row.get("worker_key") or "").strip().lower() or "unknown"
            limit_value = max(0, int(_as_float(row.get("limit_value"), 0.0)))
            used_value = max(0, int(_as_float(row.get("used_value"), 0.0)))
            remaining_value = max(0, int(_as_float(row.get("remaining_value"), 0.0)))
            blocked_total = max(0, int(_as_float(row.get("blocked_total"), 0.0)))
            window_sec = max(1.0, _as_float(row.get("window_sec"), 60.0))
            updated_at = _as_float(row.get("updated_at"), 0.0)
            last_block_ts = _as_float(row.get("last_block_ts"), 0.0)
            utilization_pct = (used_value / limit_value * 100.0) if limit_value > 0 else 0.0

            freshness_age_sec = max(0.0, now_ts - updated_at) if updated_at > 0 else float("inf")
            stale = freshness_age_sec > max(120.0, window_sec * 2.0)
            blocked_recent = (
                last_block_ts > 0
                and (now_ts - last_block_ts) <= max(120.0, window_sec * 2.0)
            )

            score = utilization_pct
            if blocked_recent:
                score += 40.0
            score += min(30.0, blocked_total * 0.2)
            if stale:
                score += 20.0

            reason_parts: list[str] = []
            if blocked_recent:
                reason_parts.append("recent_blocks")
            if utilization_pct >= 90.0:
                reason_parts.append("high_utilization")
            elif utilization_pct >= 75.0:
                reason_parts.append("elevated_utilization")
            if stale:
                reason_parts.append("stale_metrics")
            reason = "+".join(reason_parts) if reason_parts else "normal"

            scored_workers.append((score, worker_key, row, reason))

        scored_workers.sort(key=lambda item: (-item[0], item[1]))
        for score, worker_key, row, reason in scored_workers:
            limit_value = max(0, int(_as_float(row.get("limit_value"), 0.0)))
            used_value = max(0, int(_as_float(row.get("used_value"), 0.0)))
            remaining_value = max(0, int(_as_float(row.get("remaining_value"), 0.0)))
            blocked_total = max(0, int(_as_float(row.get("blocked_total"), 0.0)))
            window_sec = max(1.0, _as_float(row.get("window_sec"), 60.0))
            utilization_pct = (used_value / limit_value * 100.0) if limit_value > 0 else 0.0
            updated_at = _as_float(row.get("updated_at"), 0.0)
            updated_text = (
                datetime.fromtimestamp(updated_at).strftime("%Y-%m-%d %H:%M:%S")
                if updated_at > 0
                else "-"
            )
            print(
                f"  {worker_key} | used={used_value}/{limit_value} ({utilization_pct:.1f}%) "
                f"remaining={remaining_value} blocked_total={blocked_total} "
                f"window_sec={window_sec:.0f} score={score:.1f} status={reason} updated_at={updated_text}"
            )

        top_score, top_worker, _, top_reason = scored_workers[0]
        print(
            f"detected_bottleneck_worker={top_worker} score={top_score:.1f} status={top_reason}"
        )

    return allowance_exceeded + cooldown_active + len(latest_stream_metrics) + len(rate_limit_workers)


def _show_db_first_health(storage_path: Path, window: int, symbols_limit: int) -> int:
    scan_items = max(1, int(window))
    max_symbols = max(1, int(symbols_limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    try:
        events = _load_events_readonly(storage_path, limit=scan_items)
        tick_rows = _load_broker_ticks_readonly(storage_path)
        worker_rows = _load_worker_states_readonly(storage_path)
    except sqlite3.OperationalError as exc:
        print(f"State DB is busy, retry in a moment: {exc}")
        return 0

    tick_by_symbol = {str(row.get("symbol") or "").upper(): row for row in tick_rows if str(row.get("symbol") or "").strip()}
    worker_by_symbol = {str(row.get("symbol") or "").upper(): row for row in worker_rows if str(row.get("symbol") or "").strip()}

    latest_issue_by_symbol: dict[str, dict[str, object]] = {}
    max_age_by_symbol: dict[str, float] = {}
    for event in events:
        message = str(event.get("message") or "")
        if message not in {
            "DB-first tick cache refresh failed",
            "DB-first tick cache refresh deferred",
            "DB-first tick cache warming up",
        }:
            continue
        symbol = str(event.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        payload_raw = event.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        if symbol not in latest_issue_by_symbol:
            latest_issue_by_symbol[symbol] = {
                "ts": _as_float(event.get("ts"), 0.0),
                "message": message,
                "payload": payload,
            }
        max_age = _as_float(payload.get("max_age_sec"), 0.0)
        if max_age > 0:
            max_age_by_symbol[symbol] = max_age

    symbols = sorted(set(tick_by_symbol.keys()) | set(worker_by_symbol.keys()) | set(latest_issue_by_symbol.keys()))
    if not symbols:
        print("No DB-first health data found.")
        return 0

    env_default_max_age = _as_float(os.getenv("XTB_DB_FIRST_TICK_MAX_AGE_SEC"), 15.0)
    now_ts = time.time()
    rows: list[tuple[int, float, str, str]] = []
    summary_counts = {"OK": 0, "STALE": 0, "NO_TICK": 0, "DEGRADED": 0}

    for symbol in symbols:
        tick_row = tick_by_symbol.get(symbol)
        worker_row = worker_by_symbol.get(symbol)
        issue_row = latest_issue_by_symbol.get(symbol)
        max_age = max(1.0, _as_float(max_age_by_symbol.get(symbol), env_default_max_age))

        tick_age: float | None = None
        if tick_row is not None:
            tick_age = max(0.0, now_ts - _as_float(tick_row.get("ts"), 0.0))

        issue_age: float | None = None
        issue_streak = 0
        issue_reason = "-"
        if issue_row is not None:
            issue_ts = _as_float(issue_row.get("ts"), 0.0)
            if issue_ts > 0:
                issue_age = max(0.0, now_ts - issue_ts)
            payload = issue_row.get("payload")
            payload_dict = payload if isinstance(payload, dict) else {}
            issue_streak = int(_as_float(payload_dict.get("streak"), 0.0))
            issue_reason = str(payload_dict.get("reason") or "").strip() or _normalize_broker_error_reason(
                str(payload_dict.get("error") or "")
            )
            if not issue_reason:
                issue_reason = str(issue_row.get("message") or "-")

        if tick_age is None:
            status = "NO_TICK"
            status_rank = 3
        elif tick_age > max_age:
            status = "STALE"
            status_rank = 2
        elif issue_age is not None and issue_age <= max_age and issue_streak >= 3:
            status = "DEGRADED"
            status_rank = 1
        else:
            status = "OK"
            status_rank = 0
        summary_counts[status] = summary_counts.get(status, 0) + 1

        worker_mode = str(worker_row.get("mode") or "-") if worker_row else "-"
        worker_strategy = "-"
        worker_strategy_base = None
        worker_position = None
        if worker_row:
            worker_strategy, worker_strategy_base = _worker_state_strategy_labels(worker_row)
            worker_position = _worker_state_position_label(worker_row)
        heartbeat_age = (
            max(0.0, now_ts - _as_float(worker_row.get("last_heartbeat"), 0.0))
            if worker_row and _as_float(worker_row.get("last_heartbeat"), 0.0) > 0
            else None
        )
        tick_age_text = "-" if tick_age is None else f"{tick_age:.1f}s"
        issue_age_text = "-" if issue_age is None else f"{issue_age:.1f}s"
        heartbeat_age_text = "-" if heartbeat_age is None else f"{heartbeat_age:.1f}s"
        worker_text = f"{worker_mode}/{worker_strategy}"
        if worker_strategy_base:
            worker_text = f"{worker_text} strategy_base={worker_strategy_base}"
        if worker_position:
            worker_text = f"{worker_text} position={worker_position}"
        row_text = (
            f"  {symbol} | status={status} tick_age={tick_age_text} max_age={max_age:.1f}s "
            f"issue={issue_reason} issue_age={issue_age_text} streak={issue_streak} "
            f"worker={worker_text} heartbeat_age={heartbeat_age_text}"
        )
        rows.append((status_rank, tick_age if tick_age is not None else float("inf"), symbol, row_text))

    rows.sort(key=lambda item: (-item[0], -item[1], item[2]))
    print(f"DB-first health diagnostics (symbols={len(symbols)}, events={scan_items}):")
    print(
        "summary: "
        f"no_tick={summary_counts.get('NO_TICK', 0)} "
        f"stale={summary_counts.get('STALE', 0)} "
        f"degraded={summary_counts.get('DEGRADED', 0)} "
        f"ok={summary_counts.get('OK', 0)}"
    )
    for _, _, _, row_text in rows[:max_symbols]:
        print(row_text)
    return len(symbols)


def _event_reason(event: dict[str, object]) -> tuple[str, str] | None:
    message = str(event.get("message") or "")
    payload_raw = event.get("payload")
    payload = payload_raw if isinstance(payload_raw, dict) else {}

    if message == "Position closed":
        return "close", str(payload.get("reason") or "unknown")
    if message == "Trade blocked by risk manager":
        return "block", str(payload.get("reason") or "risk_manager")
    if message == "Trade blocked by spread filter":
        return "block", "spread_too_wide"
    if message == "Trade blocked by confidence threshold":
        return "block", "confidence_below_threshold"
    if message == "Trade blocked by entry cooldown":
        return "block", "entry_cooldown"
    if message == "Trade blocked by connectivity check":
        return "block", str(payload.get("reason") or "connectivity_check_failed")
    if message == "Trade blocked by stream health check":
        return "block", str(payload.get("reason") or "stream_health_degraded")
    if message == "Broker allowance backoff active":
        kind = str(payload.get("kind") or "").strip()
        if kind:
            return "block", f"allowance:{_slug_reason(kind, fallback='unknown')}"
        error_text = str(payload.get("error") or "")
        return "block", f"allowance:{_normalize_broker_error_reason(error_text)}"
    if message == "Broker error":
        error_text = str(payload.get("error") or "")
        return "reject", _normalize_broker_error_reason(error_text)
    if message == "Signal hold reason":
        return "hold", str(payload.get("reason") or "unknown")
    return None


def _slug_reason(value: str, fallback: str = "unknown") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback

    parts: list[str] = []
    pending_sep = False
    for char in text:
        if char.isalnum():
            if pending_sep and parts:
                parts.append("_")
            parts.append(char)
            pending_sep = False
        else:
            pending_sep = True

    normalized = "".join(parts).strip("_")
    if not normalized:
        return fallback
    if len(normalized) > 120:
        return normalized[:120].rstrip("_")
    return normalized


def _extract_ig_error_code(error_text: str) -> str | None:
    marker = '"errorCode":"'
    start = error_text.find(marker)
    if start < 0:
        return None
    start += len(marker)
    end = error_text.find('"', start)
    if end <= start:
        return None
    raw_code = error_text[start:end].strip()
    if not raw_code:
        return None
    return _slug_reason(raw_code, fallback="unknown")


def _extract_ig_api_endpoint(error_text: str) -> tuple[str, str] | None:
    marker = "IG API "
    start = error_text.find(marker)
    if start < 0:
        return None
    request_text = error_text[start + len(marker) :].strip()
    if not request_text:
        return None

    parts = request_text.split()
    if len(parts) < 2:
        return None

    method = _slug_reason(parts[0], fallback="request")
    raw_path = parts[1].strip()
    segments = [segment for segment in raw_path.split("/") if segment]
    if not segments:
        return method, "root"

    resource = _slug_reason(segments[0], fallback="unknown")
    if resource in {"positions", "workingorders"} and len(segments) > 1 and segments[1].lower() == "otc":
        resource = f"{resource}_otc"
    return method, resource


def _normalize_broker_error_reason(error_text: str) -> str:
    text = str(error_text or "").strip()
    if not text:
        return "broker_error_unknown"

    lowered = text.lower()
    if lowered.startswith("ig deal rejected:"):
        tail = text.split(":", 1)[1].strip()
        reject_reason = tail.split("|", 1)[0].strip()
        return f"deal_rejected:{_slug_reason(reject_reason, fallback='unknown')}"

    if "requested size below broker minimum" in lowered:
        return "requested_size_below_broker_minimum"

    if "allowance cooldown is active" in lowered:
        return "allowance_cooldown_active"

    endpoint = _extract_ig_api_endpoint(text)
    if endpoint is not None:
        method, resource = endpoint
        error_code = _extract_ig_error_code(text)
        if error_code:
            return f"ig_api_{method}_{resource}:{error_code}"
        return f"ig_api_{method}_{resource}:failed"

    return _slug_reason(text, fallback="broker_error_unknown")


def _show_trade_reasons(
    storage_path: Path,
    limit: int,
    window: int,
    kinds: set[str] | None = None,
) -> int:
    max_items = max(1, int(limit))
    scan_items = max(1, int(window))
    kind_filter = {str(item).strip().lower() for item in (kinds or set()) if str(item).strip()}
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    try:
        events = _load_events_readonly(storage_path, limit=scan_items)
    except sqlite3.OperationalError as exc:
        print(f"State DB is busy, retry in a moment: {exc}")
        return 0

    aggregates: dict[tuple[str, str], dict[str, object]] = {}
    spike_symbols: dict[str, int] = {}
    spike_components: dict[str, int] = {}
    spike_count = 0
    for event in events:
        reason_key = _event_reason(event)
        if reason_key is None:
            continue

        kind, reason = reason_key
        if kind_filter and kind not in kind_filter:
            continue
        payload_raw = event.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        if kind == "hold" and reason == "entry_spike_detected":
            spike_count += 1
            symbol = str(event.get("symbol") or "").strip().upper()
            if symbol:
                spike_symbols[symbol] = spike_symbols.get(symbol, 0) + 1
            component = _status_entry_component_from_payload(payload)
            if component:
                spike_components[component] = spike_components.get(component, 0) + 1
        key = (kind, reason)
        if key not in aggregates:
            aggregates[key] = {
                "count": 0,
                "pnl_sum": 0.0,
                "last_ts": 0.0,
                "symbols": set(),
            }
        bucket = aggregates[key]
        bucket["count"] = int(bucket["count"]) + 1
        bucket["last_ts"] = max(float(bucket["last_ts"]), _as_float(event.get("ts"), 0.0))

        symbol = str(event.get("symbol") or "").strip()
        if symbol:
            symbols = bucket["symbols"]
            if isinstance(symbols, set):
                symbols.add(symbol)

        if kind == "close":
            bucket["pnl_sum"] = float(bucket["pnl_sum"]) + _as_float(payload.get("pnl"), 0.0)

    if not aggregates:
        print("No trade reasons found.")
        return 0

    rows: list[dict[str, object]] = []
    for (kind, reason), bucket in aggregates.items():
        symbols = bucket.get("symbols")
        if isinstance(symbols, set):
            symbols_text = ",".join(sorted(str(item) for item in symbols)) or "-"
        else:
            symbols_text = "-"
        rows.append(
            {
                "kind": kind,
                "reason": reason,
                "count": int(bucket["count"]),
                "pnl_sum": float(bucket["pnl_sum"]),
                "last_ts": float(bucket["last_ts"]),
                "symbols": symbols_text,
            }
        )

    rows.sort(key=lambda item: (-int(item["count"]), -float(item["last_ts"]), str(item["kind"]), str(item["reason"])))
    selected = rows[:max_items]
    if kind_filter:
        selected_kinds = ",".join(sorted(kind_filter))
        print(f"Trade reason summary (last {scan_items} events, kinds={selected_kinds}):")
    else:
        print(f"Trade reason summary (last {scan_items} events):")
    for row in selected:
        kind = str(row["kind"])
        reason = str(row["reason"])
        count = int(row["count"])
        symbols = str(row["symbols"])
        ts_text = datetime.fromtimestamp(float(row["last_ts"])).strftime("%Y-%m-%d %H:%M:%S")
        if kind == "close":
            pnl_sum = float(row["pnl_sum"])
            avg_pnl = pnl_sum / count if count > 0 else 0.0
            print(
                f"{kind} | {reason} | count={count} pnl_sum={pnl_sum:.2f} avg_pnl={avg_pnl:.2f} "
                f"symbols={symbols} last_seen={ts_text}"
            )
        else:
            print(f"{kind} | {reason} | count={count} symbols={symbols} last_seen={ts_text}")

    if spike_count > 0:
        print(f"entry_spike_detected summary: count={spike_count}")
        if spike_symbols:
            print("entry_spike_detected by symbol:")
            for symbol, count in sorted(
                spike_symbols.items(),
                key=lambda item: (-item[1], item[0]),
            )[:_STATUS_REASON_LIMIT]:
                print(f"  {symbol} count={count}")
        if spike_components:
            print("entry_spike_detected by component:")
            for component, count in sorted(
                spike_components.items(),
                key=lambda item: (-item[1], item[0]),
            )[:_STATUS_REASON_LIMIT]:
                print(f"  {component} count={count}")

    return len(selected)


_ENTRY_ATTEMPT_REJECT_REASON_BY_MESSAGE = {
    "Trade blocked by risk manager": "risk_manager",
    "Trade blocked by spread filter": "spread_too_wide",
    "Trade blocked by spread pct filter": "spread_pct_too_wide",
    "Trade blocked by confidence threshold": "confidence_below_threshold",
    "Trade blocked by entry cooldown": "entry_cooldown",
    "Trade blocked by stale tick age": "stale_tick_age",
    "Trade blocked by stale entry history": "stale_entry_history",
    "Trade blocked by connectivity check": "connectivity_check_failed",
    "Trade blocked by stream health check": "stream_health_degraded",
    "Trade blocked by news filter": "news_filter_active",
    "Trade blocked by operational guard": "operational_guard_active",
    "Trade blocked by continuation reentry guard": "continuation_reentry_guard",
    "Trade blocked by same-side reentry cooldown": "same_side_reentry_cooldown",
    "Trade blocked by volume normalization": "volume_normalization",
    "Open skipped: worker lease is no longer active": "worker_lease_inactive",
    "Broker allowance backoff active": "allowance_backoff_active",
}

_ENTRY_ATTEMPT_DETAIL_FIELDS_BY_MESSAGE: dict[str, tuple[str, ...]] = {
    "Position opened": (
        "position_id",
        "side",
        "volume",
        "entry",
        "stop_loss",
        "take_profit",
        "confidence",
        "strategy_entry",
        "strategy_entry_component",
        "mode",
    ),
    "Trade blocked by risk manager": ("reason", "signal"),
    "Trade blocked by spread filter": ("current_spread_pips", "average_spread_pips", "anomaly_threshold_pips"),
    "Trade blocked by spread pct filter": ("current_spread_pct", "limit_pct", "scope", "current_spread_pips"),
    "Trade blocked by confidence threshold": (
        "signal",
        "confidence",
        "min_confidence_for_entry",
        "min_confidence_for_entry_configured",
        "confidence_threshold_cap",
        "strategy",
    ),
    "Trade blocked by entry cooldown": ("signal", "remaining_sec", "cooldown_sec", "cooldown_outcome", "strategy"),
    "Trade blocked by stale tick age": ("tick_age_sec", "quote_age_sec", "max_entry_tick_age_sec"),
    "Trade blocked by stale entry history": ("recent_samples", "min_recent_samples", "last_gap_sec", "max_gap_sec"),
    "Trade blocked by connectivity check": ("reason", "latency_ms", "pong_ok", "latency_limit_ms"),
    "Trade blocked by stream health check": ("effective_reason", "reason", "connected", "last_tick_age_sec"),
    "Trade blocked by news filter": ("signal", "event_name", "time_to_event_sec", "news_event_buffer_sec"),
    "Trade blocked by operational guard": ("reason", "active_for_sec", "stale_entry_tick_streak", "allowance_backoff_streak"),
    "Trade blocked by continuation reentry guard": ("signal", "trend_signal", "blocked_side", "strategy"),
    "Trade blocked by same-side reentry cooldown": ("signal", "blocked_side", "remaining_sec", "strategy"),
    "Trade blocked by volume normalization": ("suggested", "default_volume"),
    "Open skipped: worker lease is no longer active": ("side", "mode", "strategy"),
    "Broker allowance backoff active": ("kind", "remaining_sec"),
}


def _is_entry_broker_reject_error(error_text: str) -> bool:
    lowered = str(error_text or "").lower()
    if not lowered:
        return False
    if lowered.startswith("ig deal rejected:"):
        return True
    if "requested size below broker minimum" in lowered:
        return True
    return "ig api post /positions/otc failed" in lowered


def _compact_text(value: object, *, max_len: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 3].rstrip()}..."


def _format_detail_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    if isinstance(value, bool):
        return "true" if value else "false"
    return _compact_text(value, max_len=160)


def _entry_attempt_row(event: dict[str, object]) -> dict[str, object] | None:
    message = str(event.get("message") or "")
    symbol = str(event.get("symbol") or "").strip() or "-"
    ts = _as_float(event.get("ts"), 0.0)
    payload_raw = event.get("payload")
    payload = payload_raw if isinstance(payload_raw, dict) else {}

    if message == "Position opened":
        reason = "position_opened"
        outcome = "accepted"
    elif message.startswith("Trade blocked by "):
        reason = str(payload.get("reason") or _ENTRY_ATTEMPT_REJECT_REASON_BY_MESSAGE.get(message, "entry_blocked"))
        outcome = "rejected"
    elif message == "Open skipped: worker lease is no longer active":
        reason = _ENTRY_ATTEMPT_REJECT_REASON_BY_MESSAGE[message]
        outcome = "rejected"
    elif message == "Broker allowance backoff active":
        kind = str(payload.get("kind") or "").strip()
        if kind:
            reason = f"allowance:{_slug_reason(kind, fallback='unknown')}"
        else:
            reason = f"allowance:{_normalize_broker_error_reason(str(payload.get('error') or ''))}"
        outcome = "rejected"
    elif message == "Broker error":
        error_text = str(payload.get("error") or "")
        if not _is_entry_broker_reject_error(error_text):
            return None
        reason = _normalize_broker_error_reason(error_text)
        outcome = "rejected"
    else:
        return None

    details: dict[str, object] = {}
    for field in _ENTRY_ATTEMPT_DETAIL_FIELDS_BY_MESSAGE.get(message, ()):
        value = payload.get(field)
        if value not in (None, ""):
            details[field] = value

    if message == "Broker error":
        error_text = str(payload.get("error") or "")
        details["error_reason"] = reason
        if error_text:
            details["error"] = _compact_text(error_text, max_len=220)
    elif message == "Broker allowance backoff active":
        error_text = str(payload.get("error") or "")
        if error_text:
            details["error_reason"] = _normalize_broker_error_reason(error_text)

    reason_text = str(reason or "").strip() or "unknown"
    return {
        "ts": ts,
        "symbol": symbol,
        "message": message,
        "outcome": outcome,
        "reason": reason_text,
        "details": details,
    }


def _show_entry_attempts(storage_path: Path, limit: int, window: int) -> int:
    max_items = max(1, int(limit))
    scan_items = max(1, int(window))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    try:
        events = _load_events_readonly(storage_path, limit=scan_items)
    except sqlite3.OperationalError as exc:
        print(f"State DB is busy, retry in a moment: {exc}")
        return 0

    attempts: list[dict[str, object]] = []
    for event in events:
        row = _entry_attempt_row(event)
        if row is None:
            continue
        attempts.append(row)
        if len(attempts) >= max_items:
            break

    if not attempts:
        print("No entry attempts found.")
        return 0

    accepted = 0
    rejected = 0
    print(f"Entry attempts (last {scan_items} events, newest first):")
    for item in attempts:
        ts = _as_float(item.get("ts"), 0.0)
        ts_text = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        symbol = str(item.get("symbol") or "-")
        outcome = str(item.get("outcome") or "unknown")
        message = str(item.get("message") or "-")
        reason = str(item.get("reason") or "unknown")
        details_raw = item.get("details")
        details = details_raw if isinstance(details_raw, dict) else {}
        detail_parts = [f"{key}={_format_detail_value(value)}" for key, value in details.items()]
        detail_text = f" | {' '.join(detail_parts)}" if detail_parts else ""
        print(f"[{ts_text}] {symbol} {outcome} | {message} | reason={reason}{detail_text}")
        if outcome == "accepted":
            accepted += 1
        elif outcome == "rejected":
            rejected += 1

    print(f"summary: accepted={accepted} rejected={rejected}")
    return len(attempts)


def _export_position_updates(storage_path: Path, limit: int = 0) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    normalized_limit = max(0, int(limit))
    con = _connect_readonly_db(storage_path)
    try:
        if normalized_limit > 0:
            rows = con.execute(
                """
                SELECT *
                FROM (
                    SELECT *
                    FROM position_updates
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                )
                ORDER BY ts ASC, id ASC
                """,
                (normalized_limit,),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT *
                FROM position_updates
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
    except sqlite3.OperationalError as exc:
        print(f"Failed to read position_updates: {exc}")
        return 0
    finally:
        con.close()

    if not rows:
        print("No position updates found.")
        return 0

    for row in rows:
        item = dict(row)
        request_raw = item.pop("request_json", None)
        response_raw = item.pop("response_json", None)
        item["request"] = json.loads(request_raw) if request_raw else None
        item["response"] = json.loads(response_raw) if response_raw else None
        ts = float(item.get("ts") or 0.0)
        item["ts_iso"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        print(json.dumps(item, ensure_ascii=False))
    return len(rows)


def _reset_risk_anchor(storage_path: Path, value: float | None = None) -> float:
    if not storage_path.exists():
        raise ValueError(f"No state DB found: {storage_path}")

    con = sqlite3.connect(str(storage_path))
    try:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        if value is not None:
            anchor = float(value)
        else:
            row = cur.execute(
                "SELECT ts, equity, balance FROM account_snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if row is None:
                anchor = _fetch_current_account_anchor_from_broker()
            else:
                snapshot_ts = float(row["ts"] or 0.0)
                snapshot_age_sec = max(0.0, time.time() - snapshot_ts) if snapshot_ts > 0 else float("inf")
                if snapshot_age_sec > _ACCOUNT_SNAPSHOT_STALE_MAX_AGE_SEC:
                    try:
                        anchor = _fetch_current_account_anchor_from_broker()
                    except ValueError as exc:
                        raise ValueError(
                            "Latest account snapshot is stale "
                            f"({snapshot_age_sec:.1f}s old), and failed to fetch current broker balance."
                        ) from exc
                else:
                    equity = float(row["equity"] or 0.0)
                    balance = float(row["balance"] or 0.0)
                    anchor = equity if equity > 0 else balance

        if anchor <= 0:
            raise ValueError(f"Risk anchor value must be > 0, got: {anchor}")

        now = time.time()
        cur.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            ("risk.start_equity", f"{anchor}", now),
        )
        cur.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, '', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            ("risk.daily_locked_day", now),
        )
        cur.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, '', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            ("risk.daily_lock_reason", now),
        )
        cur.execute(
            """
            INSERT INTO kv(key, value, updated_at)
            VALUES (?, '', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            ("risk.daily_lock_until", now),
        )
        cur.execute(
            "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
            (
                now,
                "INFO",
                None,
                "Risk anchor reset",
                json.dumps({"risk_start_equity": anchor}, ensure_ascii=False),
            ),
        )
        con.commit()
        return anchor
    finally:
        con.close()


def _fetch_current_account_anchor_from_broker() -> float:
    try:
        config = load_config()
    except (ConfigError, ValueError) as exc:
        raise ValueError(
            "No account snapshots found in DB, and broker config could not be loaded for live balance fallback."
        ) from exc

    if config.broker == "ig":
        broker = IgApiClient(
            identifier=config.user_id,
            password=config.password,
            api_key=str(config.api_key or ""),
            account_type=config.account_type,
            account_id=config.account_id,
            endpoint=config.endpoint,
            symbol_epics=config.symbol_epics,
            stream_enabled=config.ig_stream_enabled,
            stream_tick_max_age_sec=config.ig_stream_tick_max_age_sec,
            rest_market_min_interval_sec=config.ig_rest_market_min_interval_sec,
        )
    else:
        broker = XtbApiClient(
            user_id=config.user_id,
            password=config.password,
            app_name=config.app_name,
            account_type=config.account_type,
            endpoint=config.endpoint,
        )

    try:
        broker.connect()
        snapshot = broker.get_account_snapshot()
    except Exception as exc:
        raise ValueError(
            "No account snapshots found in DB, and failed to fetch current account balance from broker."
        ) from exc
    finally:
        try:
            broker.close()
        except Exception:
            pass

    balance = float(snapshot.balance or 0.0)
    equity = float(snapshot.equity or 0.0)
    anchor = balance if balance > 0 else equity
    if anchor <= 0:
        raise ValueError(
            f"No account snapshots found in DB, and broker returned non-positive balance/equity (balance={balance:.2f}, equity={equity:.2f})."
        )
    return anchor


def _cleanup_incompatible_open_trades(storage_path: Path, dry_run: bool = False) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = sqlite3.connect(str(storage_path))
    try:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT position_id, symbol, side, opened_at, open_price
            FROM trades
            WHERE status = 'open'
              AND mode = 'execution'
              AND (
                    LOWER(position_id) LIKE 'paper-%'
                 OR LOWER(position_id) LIKE 'mock-%'
              )
            ORDER BY updated_at DESC
            """
        ).fetchall()

        if not rows:
            print("No incompatible open execution trades found.")
            return 0

        print(f"Found {len(rows)} incompatible open execution trade(s):")
        for row in rows:
            item = dict(row)
            opened_at = datetime.fromtimestamp(float(item.get("opened_at") or 0.0)).strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"  {item.get('position_id')} | symbol={item.get('symbol')} "
                f"side={item.get('side')} opened_at={opened_at}"
            )

        if dry_run:
            print("Dry-run mode: no changes were applied.")
            return len(rows)

        now = time.time()
        archived_ids: list[str] = []
        for row in rows:
            item = dict(row)
            position_id = str(item.get("position_id") or "")
            open_price = float(item.get("open_price") or 0.0)
            cur.execute(
                """
                UPDATE trades
                SET
                    status = 'closed',
                    close_price = ?,
                    closed_at = ?,
                    updated_at = ?
                WHERE position_id = ?
                """,
                (open_price, now, now, position_id),
            )
            archived_ids.append(position_id)

        cur.execute(
            "INSERT INTO events(ts, level, symbol, message, payload_json) VALUES (?, ?, ?, ?, ?)",
            (
                now,
                "WARN",
                None,
                "Incompatible open execution trades archived",
                json.dumps(
                    {"count": len(archived_ids), "position_ids": archived_ids},
                    ensure_ascii=False,
                ),
            ),
        )
        con.commit()
        print(f"Archived {len(archived_ids)} incompatible trade(s).")
        return len(archived_ids)
    finally:
        con.close()


def _sync_open_positions(config) -> int:
    bot = TradingBot(config)
    try:
        summary = bot.sync_open_positions()
    finally:
        bot.stop()

    print("Open position sync summary:")
    print(
        "  "
        f"source={summary.get('source', '-')} "
        f"status={summary.get('broker_sync_status', '-')} "
        f"mode={summary.get('mode', '-')} "
        f"broker={summary.get('broker', '-')} "
        f"local={int(summary.get('local_open_count') or 0)} "
        f"broker_open={int(summary.get('broker_open_count') or 0)} "
        f"recovered={int(summary.get('recovered_count') or 0)} "
        f"stale={int(summary.get('stale_local_count') or 0)} "
        f"final={int(summary.get('final_open_count') or 0)}"
    )
    error_text = str(summary.get("error") or "").strip()
    if error_text:
        print(f"  error={error_text}")
    return int(summary.get("final_open_count") or 0)


def _best_effort_sync_for_show_trades(config) -> None:
    if str(getattr(config.mode, "value", config.mode)).lower() != "execution":
        return

    bot = TradingBot(config)
    summary: dict[str, object] = {}
    try:
        summary = bot.sync_open_positions()
    except Exception as exc:
        logging.warning("show-trades live sync skipped due to sync error: %s", exc)
        return
    finally:
        try:
            bot.stop()
        except Exception:
            pass

    recovered_count = int(summary.get("recovered_count") or 0)
    stale_local_count = int(summary.get("stale_local_count") or 0)
    reconciled_missing_count = int(summary.get("missing_on_broker_reconciled_count") or 0)
    reconciled_closed_details_count = int(summary.get("closed_details_reconciled_count") or 0)
    if (
        recovered_count > 0
        or stale_local_count > 0
        or reconciled_missing_count > 0
        or reconciled_closed_details_count > 0
    ):
        print(
            "show-trades live sync | "
            f"recovered={recovered_count} stale={stale_local_count} "
            f"missing_reconciled={reconciled_missing_count} "
            f"closed_details_reconciled={reconciled_closed_details_count}"
        )


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    _load_dotenv(args.dotenv)

    if args.list_strategies:
        for name in available_strategies():
            print(name)
        return

    if args.show_alerts:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_alerts(storage_path, args.alerts_limit)
        return

    if args.show_balance:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_balance(storage_path)
        return

    if args.show_status:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_status(storage_path)
        return

    if args.show_active_schedule:
        try:
            config = load_config(
                mode_override=args.mode,
                strategy_override=args.strategy,
                symbols_override=_parse_symbols_override(args.symbols),
                force_strategy_override=args.force,
                force_symbols_override=args.force_symbols,
                ig_stream_enabled_override=args.ig_stream_enabled,
                strict_broker_connect_override=args.strict_broker_connect,
            )
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        if args.storage_path:
            config.storage_path = _resolve_storage_path(args.storage_path)
        _show_active_schedule(config)
        return

    if args.show_trades:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        if args.show_trades_live_sync:
            try:
                config = load_config(
                    mode_override=args.mode,
                    strategy_override=args.strategy,
                    symbols_override=_parse_symbols_override(args.symbols),
                    force_strategy_override=args.force,
                    force_symbols_override=args.force_symbols,
                    ig_stream_enabled_override=args.ig_stream_enabled,
                    strict_broker_connect_override=args.strict_broker_connect,
                )
                if args.storage_path:
                    config.storage_path = storage_path
                _best_effort_sync_for_show_trades(config)
            except (ConfigError, ValueError) as exc:
                logging.warning("show-trades live sync skipped due to configuration error: %s", exc)
        _show_trades(storage_path, args.trades_limit, args.trades_open_only, args.strategy)
        return

    if args.show_trade_timeline:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_trade_timeline(
            storage_path,
            args.trade_timeline_limit,
            args.trade_timeline_events_limit,
        )
        return

    if args.watch_open_trades:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _watch_open_trades(
            storage_path=storage_path,
            interval_sec=args.watch_interval_sec,
            strategy=args.strategy,
            iterations=args.watch_iterations,
        )
        return

    if args.show_trade_confidence:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_trade_confidence(storage_path, args.strategy)
        return

    if args.show_fill_quality:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_fill_quality(storage_path, args.strategy, args.fill_quality_limit)
        return

    if args.show_strategy_scorecard:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_strategy_scorecard(storage_path, args.strategy, args.strategy_scorecard_limit)
        return

    if args.show_ig_allowance:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_ig_allowance(storage_path, args.ig_allowance_window, args.ig_allowance_symbols)
        return

    if args.show_db_first_health:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_db_first_health(storage_path, args.db_first_health_window, args.db_first_health_symbols)
        return

    if args.show_trade_reasons:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        kinds = {"reject"} if args.trade_reasons_only_rejects else None
        _show_trade_reasons(
            storage_path,
            args.trade_reasons_limit,
            args.trade_reasons_window,
            kinds=kinds,
        )
        return

    if args.show_entry_attempts:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_entry_attempts(
            storage_path,
            args.entry_attempts_limit,
            args.entry_attempts_window,
        )
        return

    if args.export_position_updates:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _export_position_updates(storage_path, args.position_updates_limit)
        return

    if args.reset_risk_anchor:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
            anchor = _reset_risk_anchor(storage_path, args.risk_anchor_value)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        print(f"risk.start_equity has been reset to {anchor:.2f} in {storage_path}")
        return

    if args.cleanup_incompatible_open_trades:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
            cleaned = _cleanup_incompatible_open_trades(storage_path, dry_run=args.cleanup_dry_run)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        if args.cleanup_dry_run:
            print(f"dry-run rows matched: {cleaned}")
        else:
            print(f"archived rows: {cleaned}")
        return

    try:
        config = load_config(
            mode_override=args.mode,
            strategy_override=args.strategy,
            symbols_override=_parse_symbols_override(args.symbols),
            force_strategy_override=args.force,
            force_symbols_override=args.force_symbols,
            ig_stream_enabled_override=args.ig_stream_enabled,
            strict_broker_connect_override=args.strict_broker_connect,
        )
    except (ConfigError, ValueError) as exc:
        raise SystemExit(f"Configuration error: {exc}")

    effective_strategy_profile = (
        args.strategy_profile
        or os.getenv("XTB_STRATEGY_PROFILE")
        or os.getenv("IG_STRATEGY_PROFILE")
        or os.getenv("BOT_STRATEGY_PROFILE")
    )

    if effective_strategy_profile:
        strategy_name = str(getattr(config, "strategy", "") or "")
        strategy_params = getattr(config, "strategy_params", {})
        if isinstance(strategy_params, dict):
            try:
                merged_params, applied = apply_strategy_profile(
                    strategy_name,
                    strategy_params,
                    effective_strategy_profile,
                )
                merged_params, _surface_applied = enforce_strategy_parameter_surface(
                    strategy_name,
                    merged_params,
                    profile_name=effective_strategy_profile,
                )
            except ValueError as exc:
                raise SystemExit(f"Configuration error: {exc}")
            config.strategy_params = merged_params
            if applied:
                logging.getLogger(__name__).info(
                    "Applied strategy profile=%s for strategy=%s",
                    effective_strategy_profile,
                    strategy_name or "-",
                )
            else:
                logging.getLogger(__name__).warning(
                    "Strategy profile=%s is ignored for strategy=%s",
                    effective_strategy_profile,
                    strategy_name or "-",
                )
        else:
            logging.getLogger(__name__).warning(
                "Cannot apply strategy profile=%s: strategy_params is not a mapping",
                effective_strategy_profile,
            )

    if args.storage_path:
        config.storage_path = _resolve_storage_path(args.storage_path)

    if args.sync_open_positions:
        config.strict_broker_connect = True
        _sync_open_positions(config)
        return

    bot = TradingBot(config)
    restore_handlers = _install_termination_handlers(bot)
    try:
        bot.run_forever()
    finally:
        restore_handlers()


if __name__ == "__main__":
    main()
