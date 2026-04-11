#!/usr/bin/env python3
"""Strategy × Symbol PnL matrix. Identifies incompatible pairs."""
from __future__ import annotations
import sys, sqlite3, concurrent.futures
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backtest import resample_to_candles, simulate_symbol, get_spread, load_symbol_specs, load_strategy_params_from_env, load_strategy_specific_params
from xtb_bot.strategies import create_strategy


def _run_one(args):
    sn, sym, ticks, spec, spread, params, start_ts, end_ts = args
    strategy = create_strategy(sn, params)
    candles = resample_to_candles(ticks)
    if len(candles) < 510:
        return sn, sym, 0, 0.0
    r = simulate_symbol(sym, candles, strategy, spec, warmup_bars=500, spread_pips=spread, entry_delay_bars=2, cooldown_bars=360,
        trailing_enabled=True, trailing_distance_pips=999.0, trailing_activation_ratio=0.99,
        trailing_breakeven_offset_pips=2.0, trailing_breakeven_min_peak_pips=8.0, monday_cooldown_hours=2.0)
    trades = [t for t in r.trades if start_ts <= t.entry_ts < end_ts]
    return sn, sym, len(trades), sum(t.pnl_pips for t in trades)


def main():
    db = "downloads/backtest_all.db"
    specs = load_symbol_specs(Path(db))
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    symbols = [r['symbol'] for r in con.execute('SELECT DISTINCT symbol FROM price_history ORDER BY symbol').fetchall()]

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", default="2026-03-01")
    ap.add_argument("--to-date", default="2026-04-11")
    cli = ap.parse_args()
    start = datetime.strptime(cli.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(cli.to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    warmup_start = start.timestamp() - (600 * 60)
    strats = ['trend_following', 'index_hybrid', 'mean_reversion_bb', 'donchian_breakout']

    strat_params = {}
    for sn in strats:
        p = load_strategy_params_from_env()
        p.update(load_strategy_specific_params(sn))
        p.setdefault('stop_loss_pips', 25); p.setdefault('take_profit_pips', 50)
        strat_params[sn] = p

    tasks = []
    for sn in strats:
        for sym in symbols:
            rows = con.execute('SELECT ts, close, volume FROM price_history WHERE symbol=? AND ts>=? AND ts<? ORDER BY ts',
                (sym, warmup_start, end.timestamp())).fetchall()
            if len(rows) < 550: continue
            ticks = [(float(r['ts']), float(r['close']), float(r['volume']) if r['volume'] else None) for r in rows]
            tasks.append((sn, sym, ticks, specs.get(sym), get_spread(sym) * 2.0, strat_params[sn], start.timestamp(), end.timestamp()))
    con.close()

    print(f"Running {len(tasks)} tasks on 8 workers...")
    matrix = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as pool:
        for sn, sym, n, pnl in pool.map(_run_one, tasks, chunksize=4):
            matrix[(sn, sym)] = (n, pnl)

    # Print
    abbr = {'trend_following': 'TF', 'index_hybrid': 'IH', 'mean_reversion_bb': 'MR', 'donchian_breakout': 'DC'}
    print(f"\n{'Symbol':<10} {'TF':>10} {'IH':>10} {'MR':>10} {'DC':>10} {'TOTAL':>10}")
    print('-' * 65)
    for sym in sorted(symbols):
        vals = [matrix.get((sn, sym), (0, 0.0))[1] for sn in strats]
        total = sum(vals)
        flags = [' X' if v < -100 else '  ' for v in vals]
        print(f"{sym:<10} {vals[0]:>+9.0f}{flags[0]} {vals[1]:>+9.0f}{flags[1]} {vals[2]:>+9.0f}{flags[2]} {vals[3]:>+9.0f}{flags[3]} {total:>+10.0f}")

    # Strat totals
    print('-' * 65)
    totals = [sum(matrix.get((sn, sym), (0, 0.0))[1] for sym in symbols) for sn in strats]
    grand = sum(totals)
    print(f"{'TOTAL':<10} {totals[0]:>+9.0f}   {totals[1]:>+9.0f}   {totals[2]:>+9.0f}   {totals[3]:>+9.0f}   {grand:>+10.0f}")

    print(f"\nINCOMPATIBLE (PnL < -100):")
    losers = [(sn, sym, n, pnl) for (sn, sym), (n, pnl) in matrix.items() if pnl < -100]
    losers.sort(key=lambda x: x[3])
    for sn, sym, n, pnl in losers:
        print(f"  {sn:<20} {sym:<10} {n:>3} trades  {pnl:>+8.0f} pips")


if __name__ == "__main__":
    main()
