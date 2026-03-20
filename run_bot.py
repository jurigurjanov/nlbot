from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from xtb_bot.bot import TradingBot
from xtb_bot.config import ConfigError, DEFAULT_STRATEGY_PARAMS, load_config, resolve_strategy_param
from xtb_bot.state_store import StateStore
from xtb_bot.strategy_profiles import apply_strategy_profile
from xtb_bot.strategies import available_strategies


def _load_dotenv(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    import os

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _parse_symbols_override(raw: str | None) -> list[str] | None:
    if raw in (None, ""):
        return None
    items = [part.strip().upper() for part in str(raw).split(",") if part.strip()]
    return items or None


def parse_args() -> argparse.Namespace:
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
            "(index_hybrid, mean_breakout_v2, mean_reversion_bb): conservative|aggressive "
            "(safe is an alias of conservative)"
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
        "--show-trade-confidence",
        action="store_true",
        help="Show closed-trade performance grouped by entry confidence buckets",
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
        "--show-trade-reasons",
        action="store_true",
        help="Show aggregated trade reasons (close/block/hold) from SQLite events",
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
            "--show-trade-confidence/--show-trade-reasons/--reset-risk-anchor/"
            "--cleanup-incompatible-open-trades/--sync-open-positions"
        ),
    )
    parser.add_argument("--dotenv", default=".env", help="Path to .env file")
    return parser.parse_args()


def _resolve_storage_path(storage_path_override: str | None) -> Path:
    if storage_path_override:
        return Path(storage_path_override).expanduser()

    env_storage = os.getenv("BOT_STORAGE_PATH") or os.getenv("IG_STORAGE_PATH") or os.getenv("XTB_STORAGE_PATH")
    if env_storage:
        return Path(env_storage).expanduser()

    return Path("./state/xtb_bot.db")


def _show_alerts(storage_path: Path, limit: int) -> int:
    max_items = max(1, int(limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    store = StateStore(storage_path)
    try:
        # Load a wider window and then filter alerts.
        events = store.load_events(limit=max(max_items * 10, 100))
    finally:
        store.close()

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


def _show_balance(storage_path: Path) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = sqlite3.connect(str(storage_path))
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


def _show_status(storage_path: Path) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = sqlite3.connect(str(storage_path))
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
    finally:
        con.close()

    open_count = int((open_row["cnt"] if open_row is not None else 0) or 0)
    closed_count = int((closed_row["cnt"] if closed_row is not None else 0) or 0)
    events_count = int((events_row["cnt"] if events_row is not None else 0) or 0)

    print(f"State status | path={storage_path}")
    print(
        f"trades_open={open_count} trades_closed={closed_count} events_total={events_count}"
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
        print(
            f"latest_snapshot=[{snap_text}] balance={float(latest_snapshot['balance'] or 0.0):.2f} "
            f"equity={float(latest_snapshot['equity'] or 0.0):.2f} "
            f"margin_free={float(latest_snapshot['margin_free'] or 0.0):.2f} "
            f"open_positions={int(latest_snapshot['open_positions'] or 0)} "
            f"daily_pnl={float(latest_snapshot['daily_pnl'] or 0.0):.2f} "
            f"drawdown_pct={float(latest_snapshot['drawdown_pct'] or 0.0):.2f}"
        )
    else:
        print("latest_snapshot=none")

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


def _show_active_schedule(config) -> int:
    bot = TradingBot(config)
    try:
        restored = bot.store.load_open_positions(mode=config.mode.value)
        filtered = bot._filter_restored_positions_for_mode(restored)
        if filtered:
            bot.position_book.bootstrap(filtered)

        now_utc = datetime.now(timezone.utc)
        active_slots = [
            entry
            for entry in config.strategy_schedule
            if bot._is_schedule_entry_active(now_utc, entry)
        ]
        assignments = bot._target_worker_assignments(now_utc)

        schedule_tz = str(config.strategy_schedule_timezone or "UTC")
        local_now = now_utc.astimezone(bot._schedule_timezone)
        print(
            f"Active schedule | timezone={schedule_tz} "
            f"now_utc={now_utc.strftime('%Y-%m-%d %H:%M:%S')} "
            f"now_local={local_now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )

        if getattr(config, "force_symbols", False):
            print(f"  mode=forced_symbols strategy={config.strategy} symbols={','.join(config.symbols)}")
        elif getattr(config, "force_strategy", False):
            print(f"  mode=forced strategy={config.strategy}")
        elif not config.strategy_schedule:
            print("  mode=static")
        else:
            print(f"  configured_slots={len(config.strategy_schedule)} active_slots={len(active_slots)}")
            if active_slots:
                print("Active slots:")
                for entry in active_slots:
                    weekdays = ",".join(str(day) for day in entry.weekdays)
                    label = entry.label or f"{entry.strategy}@{entry.start_time}-{entry.end_time}"
                    symbols = ",".join(entry.symbols)
                    print(
                        f"  {label} | strategy={entry.strategy} symbols={symbols} "
                        f"time={entry.start_time}-{entry.end_time} weekdays={weekdays} priority={entry.priority}"
                    )

        if not assignments:
            print("No active symbol assignments.")
            return 0

        print("Effective assignments:")
        for symbol in sorted(assignments):
            assignment = assignments[symbol]
            position = bot.position_book.get(symbol)
            position_id = position.position_id if position is not None else None
            print(
                f"  {symbol} | strategy={assignment.strategy_name} source={assignment.source} "
                f"label={assignment.label or '-'} open_position_id={position_id or '-'}"
            )
        return len(assignments)
    finally:
        bot.store.close()


def _show_trades(storage_path: Path, limit: int, open_only: bool = False, strategy: str | None = None) -> int:
    max_items = max(1, int(limit))
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = sqlite3.connect(str(storage_path))
    try:
        con.row_factory = sqlite3.Row
        trade_columns = {
            str(row["name"])
            for row in con.execute("PRAGMA table_info(trades)").fetchall()
        }
        confidence_select = ", entry_confidence" if "entry_confidence" in trade_columns else ", 0.0 AS entry_confidence"
        where_parts: list[str] = []
        query_params: list[object] = []
        if open_only:
            where_parts.append("status = 'open'")
        if strategy:
            where_parts.append("LOWER(strategy) = LOWER(?)")
            query_params.append(str(strategy))
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        query_params.append(max_items)
        rows = con.execute(
            f"""
            SELECT
                position_id, symbol, side, volume, open_price, stop_loss, take_profit,
                opened_at{confidence_select}, status, close_price, closed_at, pnl, thread_name, strategy, mode
            FROM trades
            {where_clause}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            tuple(query_params),
        ).fetchall()
    finally:
        con.close()

    if not rows:
        print("No trades found.")
        return 0

    for row in rows:
        item = dict(row)
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
        strategy = str(item.get("strategy") or "-")
        mode = str(item.get("mode") or "-")
        position_id = str(item.get("position_id") or "-")
        confidence_gate, confidence_threshold = _confidence_gate_status(strategy, mode, entry_confidence)
        print(
            f"[{opened_at}] {status} {symbol} {side} | "
            f"volume={volume:.4f} open={open_price:.5f} sl={stop_loss:.5f} tp={take_profit:.5f} "
            f"conf={entry_confidence:.3f} conf_gate={confidence_gate}@{confidence_threshold:.3f} "
            f"pnl={pnl:.2f} strategy={strategy} mode={mode} id={position_id}"
        )
        closed_at_ts = item.get("closed_at")
        if closed_at_ts is not None:
            closed_at = datetime.fromtimestamp(float(closed_at_ts)).strftime("%Y-%m-%d %H:%M:%S")
            close_price = float(item.get("close_price") or 0.0)
            print(f"  closed_at: {closed_at} close_price={close_price:.5f}")

    return len(rows)


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
    if max(0.0, min(1.0, entry_confidence)) + 1e-12 >= threshold:
        return "pass", threshold
    return "below", threshold


def _show_trade_confidence(storage_path: Path, strategy: str | None = None) -> int:
    if not storage_path.exists():
        print(f"No state DB found: {storage_path}")
        return 0

    con = sqlite3.connect(str(storage_path))
    try:
        con.row_factory = sqlite3.Row
        trade_columns = {
            str(row["name"])
            for row in con.execute("PRAGMA table_info(trades)").fetchall()
        }
        if "entry_confidence" not in trade_columns:
            print("No trade confidence data found in trades table.")
            return 0

        query = """
            SELECT position_id, strategy, mode, status, pnl, entry_confidence
            FROM trades
            """
        query_params: list[object] = []
        if strategy:
            query += "WHERE LOWER(strategy) = LOWER(?)\n"
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
        if abs(conf) <= 1e-12:
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

    store = StateStore(storage_path)
    try:
        events = store.load_events(limit=scan_items)
    finally:
        store.close()

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

    if allowance_exceeded == 0 and cooldown_active == 0 and not latest_stream_metrics:
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

    return allowance_exceeded + cooldown_active + len(latest_stream_metrics)


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

    store = StateStore(storage_path)
    try:
        events = store.load_events(limit=scan_items)
    finally:
        store.close()

    aggregates: dict[tuple[str, str], dict[str, object]] = {}
    for event in events:
        reason_key = _event_reason(event)
        if reason_key is None:
            continue

        kind, reason = reason_key
        if kind_filter and kind not in kind_filter:
            continue
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
            payload_raw = event.get("payload")
            payload = payload_raw if isinstance(payload_raw, dict) else {}
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

    return len(selected)


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
                "SELECT equity, balance FROM account_snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise ValueError(
                    "No account snapshots found. Provide --risk-anchor-value explicitly."
                )
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
        _show_trades(storage_path, args.trades_limit, args.trades_open_only, args.strategy)
        return

    if args.show_trade_confidence:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_trade_confidence(storage_path, args.strategy)
        return

    if args.show_ig_allowance:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
        except (ConfigError, ValueError) as exc:
            raise SystemExit(f"Configuration error: {exc}")
        _show_ig_allowance(storage_path, args.ig_allowance_window, args.ig_allowance_symbols)
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

    if args.reset_risk_anchor:
        try:
            storage_path = _resolve_storage_path(args.storage_path)
            anchor = _reset_risk_anchor(storage_path, args.risk_anchor_value)
        except (ConfigError, ValueError) as exc:
            if args.risk_anchor_value is not None:
                raise SystemExit(f"Configuration error: {exc}")
            # No snapshots and no explicit value – try fetching live balance from broker.
            try:
                config = load_config(
                    mode_override=args.mode,
                    strategy_override=args.strategy,
                    symbols_override=_parse_symbols_override(args.symbols),
                    force_strategy_override=args.force,
                    force_symbols_override=args.force_symbols,
                    ig_stream_enabled_override=args.ig_stream_enabled,
                    strict_broker_connect_override=True,
                )
                if args.storage_path:
                    config.storage_path = _resolve_storage_path(args.storage_path)
                bot = TradingBot(config)
                try:
                    bot.broker.connect()
                    snapshot = bot.broker.get_account_snapshot()
                    equity = snapshot.equity
                    balance = snapshot.balance
                    live_anchor = equity if equity > 0 else balance
                    if live_anchor <= 0:
                        raise SystemExit(
                            "Configuration error: broker returned non-positive balance "
                            f"(equity={equity}, balance={balance})"
                        )
                    storage_path = config.storage_path
                    anchor = _reset_risk_anchor(storage_path, live_anchor)
                    print(f"(fetched live account equity from broker)")
                finally:
                    bot.stop()
            except SystemExit:
                raise
            except Exception as inner_exc:
                raise SystemExit(
                    f"Configuration error: no snapshots in DB and broker fetch failed: {inner_exc}"
                )
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
        or os.getenv("BOT_STRATEGY_PROFILE")
        or os.getenv("IG_STRATEGY_PROFILE")
        or os.getenv("XTB_STRATEGY_PROFILE")
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
    bot.run_forever()


if __name__ == "__main__":
    main()
