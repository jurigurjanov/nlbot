#!/usr/bin/env python3
"""
Download historical price data from Dukascopy for backtesting.

Downloads 1-minute candle data day-by-day, saves CSVs to downloads/<SYMBOL>/,
and populates a SQLite database at downloads/backtest.db.

Usage:
    python download_data.py
    python download_data.py --days 14
    python download_data.py --symbols EURUSD,US100
    python download_data.py --from-date 2025-02-01 --to-date 2025-03-01
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Symbol mapping: our name -> dukascopy instrument id
# ---------------------------------------------------------------------------
SYMBOL_MAP: dict[str, str] = {
    # Forex
    "EURUSD": "eurusd",
    "GBPUSD": "gbpusd",
    "USDJPY": "usdjpy",
    "AUDUSD": "audusd",
    "USDCHF": "usdchf",
    "USDCAD": "usdcad",
    "EURGBP": "eurgbp",
    "EURCHF": "eurchf",
    "GBPJPY": "gbpjpy",
    # Indices
    "US100": "usatechidxusd",
    "US500": "usa500idxusd",
    "US30": "usa30idxusd",
    "US2000": "ussc2000idxusd",
    "DE40": "deuidxeur",
    "UK100": "gbridxgbp",
    "FR40": "fraidxeur",
    "EU50": "eusidxeur",
    "JPN225": "jpnidxjpy",
    "AUS200": "ausidxaud",
    "IT40": "itaidxeur",
    "ES35": "espidxeur",
    # Commodities
    "GOLD": "xauusd",
    "WTI": "lightcmdusd",
    "BRENT": "brentcmdusd",
}

# Symbols not available in Dukascopy
UNSUPPORTED = {"SK20", "NK20", "TOPIX"}

# ---------------------------------------------------------------------------
# Default symbol specs for backtesting (tick_size, tick_value, contract_size,
# lot_min, lot_step, one_pip_means)
# ---------------------------------------------------------------------------
SYMBOL_SPECS: dict[str, dict] = {
    # Forex (standard lot = 100k units)
    "EURUSD": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "GBPUSD": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "USDJPY": {"tick_size": 0.001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.01},
    "AUDUSD": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "USDCHF": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "USDCAD": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "EURGBP": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "EURCHF": {"tick_size": 0.00001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.0001},
    "GBPJPY": {"tick_size": 0.001, "tick_value": 1.0, "contract_size": 100000, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.01},
    # Indices
    "US100":  {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "US500":  {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "US30":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "US2000": {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "DE40":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "UK100":  {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "FR40":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "EU50":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "JPN225": {"tick_size": 1.0, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "AUS200": {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "IT40":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    "ES35":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 1.0},
    # Commodities
    "GOLD":   {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.01, "lot_step": 0.01, "one_pip_means": 0.1},
    "WTI":    {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 0.01},
    "BRENT":  {"tick_size": 0.01, "tick_value": 1.0, "contract_size": 1, "lot_min": 0.1, "lot_step": 0.1, "one_pip_means": 0.01},
}


def load_symbols_from_env() -> list[str]:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return list(SYMBOL_MAP.keys())
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("XTB_SYMBOLS="):
            raw = stripped.split("=", 1)[1]
            return [s.strip().upper() for s in raw.split(",") if s.strip()]
    return list(SYMBOL_MAP.keys())


def download_day(
    symbol: str,
    instrument: str,
    date: datetime,
    output_dir: Path,
) -> Path | None:
    date_str = date.strftime("%Y-%m-%d")
    next_day = (date + timedelta(days=1)).strftime("%Y-%m-%d")
    csv_dir = output_dir / symbol
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / f"{date_str}.csv"

    if csv_path.exists() and csv_path.stat().st_size > 0:
        return csv_path

    cmd = [
        "npx", "dukascopy-node",
        "-i", instrument,
        "-from", date_str,
        "-to", next_day,
        "-t", "m1",
        "-f", "csv",
        "--volumes", "true",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(output_dir.parent),
        )
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT: {symbol} {date_str}")
        return None
    except Exception as exc:
        print(f"    ERROR: {symbol} {date_str}: {exc}")
        return None

    # dukascopy-node writes to download/ subdir with its own naming
    # Find the downloaded file
    duka_dir = output_dir.parent / "download"
    if duka_dir.exists():
        for f in sorted(duka_dir.iterdir()):
            if f.suffix == ".csv" and instrument in f.name and f.stat().st_size > 0:
                f.rename(csv_path)
                return csv_path
            elif f.suffix == ".csv":
                f.unlink()  # empty file
        # clean up dukascopy download dir if empty
        try:
            duka_dir.rmdir()
        except OSError:
            pass

    return None


def parse_dukascopy_csv(csv_path: Path, symbol: str) -> list[tuple[float, float, float | None]]:
    rows = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_ms = int(row["timestamp"])
            ts_sec = ts_ms / 1000.0
            close = float(row["close"])
            volume = float(row.get("volume") or 0) or None
            rows.append((ts_sec, close, volume))
    return rows


def init_db(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute(
        "CREATE TABLE IF NOT EXISTS price_history ("
        "  symbol TEXT NOT NULL,"
        "  ts REAL NOT NULL,"
        "  close REAL NOT NULL,"
        "  volume REAL,"
        "  PRIMARY KEY(symbol, ts)"
        ")"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS broker_symbol_specs ("
        "  cache_key TEXT PRIMARY KEY,"
        "  symbol TEXT NOT NULL,"
        "  epic TEXT,"
        "  epic_variant TEXT,"
        "  tick_size REAL NOT NULL,"
        "  tick_value REAL NOT NULL,"
        "  contract_size REAL NOT NULL,"
        "  lot_min REAL NOT NULL,"
        "  lot_max REAL NOT NULL DEFAULT 100.0,"
        "  lot_step REAL NOT NULL,"
        "  min_stop_distance_price REAL,"
        "  one_pip_means REAL,"
        "  metadata_json TEXT,"
        "  source TEXT DEFAULT 'dukascopy',"
        "  ts REAL NOT NULL,"
        "  updated_at REAL NOT NULL"
        ")"
    )
    con.commit()
    return con


def seed_symbol_specs(con: sqlite3.Connection, symbols: list[str]) -> None:
    now = time.time()
    for symbol in symbols:
        spec = SYMBOL_SPECS.get(symbol)
        if not spec:
            continue
        con.execute(
            "INSERT OR REPLACE INTO broker_symbol_specs "
            "(cache_key, symbol, tick_size, tick_value, contract_size, "
            " lot_min, lot_max, lot_step, one_pip_means, source, ts, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 100.0, ?, ?, 'dukascopy', ?, ?)",
            (
                symbol,
                symbol,
                spec["tick_size"],
                spec["tick_value"],
                spec["contract_size"],
                spec["lot_min"],
                spec["lot_step"],
                spec["one_pip_means"],
                now,
                now,
            ),
        )
    con.commit()


def insert_price_data(con: sqlite3.Connection, symbol: str, rows: list[tuple[float, float, float | None]]) -> int:
    if not rows:
        return 0
    con.executemany(
        "INSERT OR IGNORE INTO price_history (symbol, ts, close, volume) VALUES (?, ?, ?, ?)",
        [(symbol, ts, close, vol) for ts, close, vol in rows],
    )
    con.commit()
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Dukascopy data for backtesting")
    parser.add_argument("--days", type=int, default=30, help="Number of days to download (default: 30)")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols (default: from .env)")
    parser.add_argument("--from-date", default=None, help="Start date YYYY-MM-DD (default: --days ago)")
    parser.add_argument("--to-date", default=None, help="End date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--output", default="downloads", help="Output directory (default: downloads)")
    args = parser.parse_args()

    # Determine date range
    if args.to_date:
        end_date = datetime.strptime(args.to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    if args.from_date:
        start_date = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_date = end_date - timedelta(days=args.days - 1)

    # Determine symbols
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        symbols = load_symbols_from_env()

    # Filter to supported symbols
    supported = []
    for s in symbols:
        if s in UNSUPPORTED:
            print(f"  SKIP: {s} (not available in Dukascopy)")
            continue
        if s not in SYMBOL_MAP:
            print(f"  SKIP: {s} (no Dukascopy mapping)")
            continue
        supported.append(s)

    if not supported:
        print("No supported symbols to download!")
        return 1

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "backtest.db"

    print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Symbols: {len(supported)} ({', '.join(supported)})")
    print(f"Output: {output_dir}/")
    print(f"Database: {db_path}")
    print()

    # Init DB
    con = init_db(db_path)
    seed_symbol_specs(con, supported)

    # Generate list of dates
    dates: list[datetime] = []
    current = start_date
    while current <= end_date:
        # Skip weekends (Sat=5, Sun=6) for non-crypto
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)

    total_days = len(dates) * len(supported)
    done = 0
    total_rows = 0

    for symbol in supported:
        instrument = SYMBOL_MAP[symbol]
        symbol_rows = 0
        print(f"[{symbol}] instrument={instrument}")

        for date in dates:
            done += 1
            date_str = date.strftime("%Y-%m-%d")
            pct = done * 100 // total_days

            # Check if already in DB for this day
            existing = con.execute(
                "SELECT COUNT(*) FROM price_history WHERE symbol = ? AND ts >= ? AND ts < ?",
                (symbol, date.timestamp(), (date + timedelta(days=1)).timestamp()),
            ).fetchone()[0]
            if existing > 100:
                symbol_rows += existing
                print(f"  [{pct:3d}%] {date_str} — cached ({existing} rows)")
                continue

            csv_path = download_day(symbol, instrument, date, output_dir)
            if csv_path is None:
                print(f"  [{pct:3d}%] {date_str} — no data")
                continue

            rows = parse_dukascopy_csv(csv_path, symbol)
            inserted = insert_price_data(con, symbol, rows)
            symbol_rows += inserted
            print(f"  [{pct:3d}%] {date_str} — {inserted} rows")

        total_rows += symbol_rows
        print(f"  Total: {symbol_rows} rows\n")

    con.close()
    print(f"Done! {total_rows} total rows in {db_path}")
    print(f"\nRun backtest:")
    print(f"  python backtest.py --db {db_path}")
    print(f"  python backtest.py --db {db_path} --strategy momentum --symbols EURUSD,US100")
    return 0


if __name__ == "__main__":
    sys.exit(main())
