#!/usr/bin/env python3
"""Donchian breakout monthly analysis — parallel by (month, symbol)."""
from __future__ import annotations
import sys, sqlite3, concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backtest import resample_to_candles, simulate_symbol, get_spread, load_symbol_specs, load_strategy_params_from_env, load_strategy_specific_params
from xtb_bot.strategies import create_strategy


def _run_one(args):
    label, sym, ticks, spec, spread, params, start_ts, end_ts = args
    strategy = create_strategy('donchian_breakout', params)
    candles = resample_to_candles(ticks)
    if len(candles) < 510:
        return label, sym, 0, 0.0
    r = simulate_symbol(sym, candles, strategy, spec, warmup_bars=500, spread_pips=spread, entry_delay_bars=2, cooldown_bars=360,
        trailing_enabled=True, trailing_distance_pips=999.0, trailing_activation_ratio=0.99,
        trailing_breakeven_offset_pips=2.0, trailing_breakeven_min_peak_pips=8.0, monday_cooldown_hours=2.0)
    trades = [t for t in r.trades if start_ts <= t.entry_ts < end_ts]
    return label, sym, len(trades), sum(t.pnl_pips for t in trades)


def main():
    db = "downloads/backtest_all.db"
    specs = load_symbol_specs(Path(db))
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    symbols = [r['symbol'] for r in con.execute('SELECT DISTINCT symbol FROM price_history ORDER BY symbol').fetchall()]

    params = load_strategy_params_from_env()
    sp = load_strategy_specific_params('donchian_breakout')
    params.update(sp)
    params.setdefault('stop_loss_pips', 25); params.setdefault('take_profit_pips', 50)

    # Generate months
    months = []
    y, m = 2024, 1
    while (y, m) <= (2026, 4):
        start = datetime(y, m, 1, tzinfo=timezone.utc)
        end = datetime(y + (1 if m == 12 else 0), (m % 12) + 1, 1, tzinfo=timezone.utc)
        months.append((f'{y}-{m:02d}', start, end))
        m += 1
        if m > 12: m = 1; y += 1

    # Build all tasks: (month, symbol)
    tasks = []
    for label, start, end in months:
        warmup_start = start.timestamp() - (600 * 60)
        for sym in symbols:
            rows = con.execute('SELECT ts, close, volume FROM price_history WHERE symbol=? AND ts>=? AND ts<? ORDER BY ts',
                (sym, warmup_start, end.timestamp())).fetchall()
            if len(rows) < 550: continue
            ticks = [(float(r['ts']), float(r['close']), float(r['volume']) if r['volume'] else None) for r in rows]
            tasks.append((label, sym, ticks, specs.get(sym), get_spread(sym) * 2.0, params, start.timestamp(), end.timestamp()))
    con.close()

    print(f"Running {len(tasks)} tasks on 8 workers ({len(months)} months x {len(symbols)} symbols)...")

    # Parallel
    results = defaultdict(lambda: defaultdict(float))
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as pool:
        for label, sym, n, pnl in pool.map(_run_one, tasks, chunksize=4):
            results[label][sym] = pnl

    # Monthly summary
    print(f"\n{'Month':<10} {'PnL':>8}  Worst                          Best")
    print('-' * 80)
    for label, _, _ in months:
        month_data = results.get(label, {})
        total = sum(month_data.values())
        top = sorted(month_data.items(), key=lambda x: x[1])
        worst = ', '.join(f'{s}({p:+.0f})' for s, p in top[:3] if p < -50)
        best = ', '.join(f'{s}({p:+.0f})' for s, p in reversed(top[-3:]) if p > 50)
        print(f'{label:<10} {total:>+8.0f}  {worst:<30} {best}')

    # Symbol summary
    sym_total = defaultdict(float)
    sym_months_profit = defaultdict(int)
    sym_months_loss = defaultdict(int)
    for label in results:
        for sym, pnl in results[label].items():
            sym_total[sym] += pnl
            if pnl > 0: sym_months_profit[sym] += 1
            elif pnl < 0: sym_months_loss[sym] += 1

    print(f"\n{'='*60}")
    print(f"DONCHIAN: Symbol profitability (Jan 2024 — Apr 2026)")
    print(f"{'='*60}")
    print(f"{'Symbol':<10} {'Total':>8} {'Win months':>11} {'Loss months':>12}")
    print('-' * 45)
    for sym in sorted(symbols, key=lambda s: sym_total.get(s, 0)):
        t = sym_total.get(sym, 0)
        flag = ' <<<' if t < -300 else (' ***' if t > 300 else '')
        print(f'{sym:<10} {t:>+8.0f} {sym_months_profit[sym]:>11} {sym_months_loss[sym]:>12}{flag}')

    grand = sum(sym_total.values())
    print(f"\nGRAND TOTAL: {grand:>+,.0f} pips")


if __name__ == "__main__":
    main()
