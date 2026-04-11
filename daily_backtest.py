#!/usr/bin/env python3
"""Daily P&L backtest report. Runs all strategies in parallel."""
from __future__ import annotations
import sys, sqlite3, concurrent.futures, os
from datetime import datetime, timezone, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backtest import (
    resample_to_candles, simulate_symbol, get_spread,
    load_symbol_specs, load_strategy_params_from_env, load_strategy_specific_params,
)
from xtb_bot.strategies import create_strategy


def _run_day_symbol(args):
    sn, sym, ticks, spec, spread, params, warmup, cooldown = args
    strategy = create_strategy(sn, params)
    candles = resample_to_candles(ticks)
    if len(candles) < warmup + 10:
        return []
    r = simulate_symbol(
        sym, candles, strategy, spec,
        warmup_bars=warmup, spread_pips=spread, entry_delay_bars=2, cooldown_bars=cooldown,
        trailing_enabled=True, trailing_distance_pips=999.0, trailing_activation_ratio=0.99,
        trailing_breakeven_offset_pips=2.0, trailing_breakeven_min_peak_pips=8.0,
    )
    return [(sn, t.pnl_pips, t.entry_ts) for t in r.trades]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="downloads/backtest_all.db")
    parser.add_argument("--from-date", default="2026-03-01")
    parser.add_argument("--to-date", default="2026-04-11")
    parser.add_argument("--cooldown", type=int, default=360)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    specs = load_symbol_specs(Path(args.db))
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    symbols = [r['symbol'] for r in con.execute('SELECT DISTINCT symbol FROM price_history ORDER BY symbol').fetchall()]

    strategies_list = ['trend_following', 'index_hybrid', 'mean_reversion_bb', 'donchian_breakout']

    # Preload params
    strat_params = {}
    for sn in strategies_list:
        p = load_strategy_params_from_env()
        p.update(load_strategy_specific_params(sn))
        p.setdefault('stop_loss_pips', 25)
        p.setdefault('take_profit_pips', 50)
        strat_params[sn] = p

    start = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = []
    d = start
    while d < end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)

    header = f"{'Date':<12} {'Tr':>4} {'WR%':>5} {'PnL':>10} {'TF':>8} {'IH':>8} {'MR':>8} {'DC':>8} {'Equity':>10}"
    print(header)
    print('-' * len(header))

    equity = 0.0
    total_trades = 0
    total_wins = 0
    profitable_days = 0

    for day in days:
        day_start = day.timestamp()
        day_end = (day + timedelta(days=1)).timestamp()
        warmup_start = day_start - (args.warmup * 60 + 100 * 60)

        # Build tasks: one per (strategy, symbol)
        tasks = []
        for sn in strategies_list:
            for sym in symbols:
                rows = con.execute(
                    'SELECT ts, close, volume FROM price_history WHERE symbol=? AND ts>=? AND ts<? ORDER BY ts',
                    (sym, warmup_start, day_end),
                ).fetchall()
                if len(rows) < args.warmup + 50:
                    continue
                ticks = [(float(r['ts']), float(r['close']), float(r['volume']) if r['volume'] else None) for r in rows]
                tasks.append((sn, sym, ticks, specs.get(sym), get_spread(sym) * 2.0, strat_params[sn], args.warmup, args.cooldown))

        # Run in parallel
        by_strat = {sn: 0.0 for sn in strategies_list}
        day_trades_pnl = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
            for result_list in pool.map(_run_day_symbol, tasks, chunksize=4):
                for sn, pnl, entry_ts in result_list:
                    if day_start <= entry_ts < day_end:
                        by_strat[sn] += pnl
                        day_trades_pnl.append(pnl)

        day_pnl = sum(by_strat.values())
        equity += day_pnl
        n = len(day_trades_pnl)
        total_trades += n
        w = sum(1 for p in day_trades_pnl if p > 0)
        total_wins += w
        if day_pnl > 0:
            profitable_days += 1
        wr = w / n * 100 if n else 0
        ds = day.strftime('%Y-%m-%d')
        marker = ' <<' if day_pnl < -500 else (' **' if day_pnl > 500 else '')
        print(f'{ds:<12} {n:>4} {wr:>5.1f} {day_pnl:>+10.1f} {by_strat["trend_following"]:>+8.1f} {by_strat["index_hybrid"]:>+8.1f} {by_strat["mean_reversion_bb"]:>+8.1f} {by_strat["donchian_breakout"]:>+8.1f} {equity:>+10.1f}{marker}', flush=True)

    con.close()
    print()
    total_wr = total_wins / max(1, total_trades) * 100
    print(f'TOTAL: {total_trades} trades, WR={total_wr:.1f}%, PnL={equity:+,.1f} pips')
    print(f'Days: {len(days)}, Profitable: {profitable_days} ({profitable_days/len(days)*100:.0f}%)')


if __name__ == "__main__":
    main()
