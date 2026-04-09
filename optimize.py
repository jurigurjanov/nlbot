#!/usr/bin/env python3
"""
Run all strategies on all instruments, then optimize parameters.

Usage:
    python optimize.py                          # baseline + optimization
    python optimize.py --baseline-only          # just baseline scan
    python optimize.py --strategy momentum      # optimize single strategy
"""
from __future__ import annotations

import argparse
import concurrent.futures
import itertools
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backtest import (
    load_price_history,
    load_symbol_specs,
    resample_to_candles,
    simulate_symbol,
    get_spread,
    resolve_pip_size,
    Candle,
    SymbolSpec,
    SymbolResult,
)
from xtb_bot.strategies import create_strategy, available_strategies

# Strategies to test (exclude aliases and multi_strategy wrapper)
STRATEGIES = [
    "momentum", "g1", "g2", "trend_following", "donchian_breakout",
    "mean_breakout_v2", "mean_reversion_bb", "index_hybrid",
]

# Parameter grids for optimization
PARAM_GRIDS: dict[str, dict[str, list]] = {
    "momentum": {
        "stop_loss_pips": [15, 25, 50],
        "take_profit_pips": [50, 75, 100],
        "momentum_atr_multiplier": [1.5, 2.0, 3.0],
    },
    "g1": {
        "stop_loss_pips": [15, 25, 40],
        "take_profit_pips": [50, 75, 100],
    },
    "g2": {
        "stop_loss_pips": [15, 25, 40],
        "take_profit_pips": [50, 75, 100],
    },
    "trend_following": {
        "stop_loss_pips": [20, 35, 50],
        "take_profit_pips": [50, 75, 100],
    },
    "donchian_breakout": {
        "stop_loss_pips": [15, 25, 40],
        "take_profit_pips": [50, 75, 100],
    },
    "mean_breakout_v2": {
        "stop_loss_pips": [15, 25, 40],
        "take_profit_pips": [50, 75, 100],
    },
    "mean_reversion_bb": {
        "stop_loss_pips": [10, 20, 30],
        "take_profit_pips": [30, 50, 75],
    },
    "index_hybrid": {
        "stop_loss_pips": [20, 35, 50],
        "take_profit_pips": [50, 75, 100],
    },
}


@dataclass
class RunResult:
    strategy: str
    params: dict[str, Any]
    total_trades: int
    total_pnl: float
    win_rate: float
    profit_factor: float
    sharpe: float
    max_dd: float
    per_symbol: dict[str, tuple[int, float]]  # symbol -> (trades, pnl)


def _run_one_symbol(args: tuple) -> tuple[str, SymbolResult]:
    symbol, candles_data, strategy_name, params, spec, spread = args
    # Reconstruct candles from serialized data
    candles = [Candle(*c) for c in candles_data]
    strategy = create_strategy(strategy_name, params)
    result = simulate_symbol(
        symbol=symbol,
        candles=candles,
        strategy=strategy,
        spec=spec,
        warmup_bars=250,
        spread_pips=spread,
    )
    return symbol, result


def _run_strategy_combo(args: tuple) -> RunResult:
    strategy_name, params, symbol_tasks = args
    strategy = create_strategy(strategy_name, params)

    total_trades = 0
    total_pnl = 0.0
    total_wins = 0
    total_gross_win = 0.0
    total_gross_loss = 0.0
    total_max_dd = 0.0
    all_returns: list[float] = []
    per_symbol: dict[str, tuple[int, float]] = {}

    for symbol, candles, spec, spread in symbol_tasks:
        result = simulate_symbol(
            symbol=symbol,
            candles=candles,
            strategy=strategy,
            spec=spec,
            warmup_bars=250,
            spread_pips=spread,
        )
        trades = result.total_trades
        pnl = result.total_pnl_pips
        total_trades += trades
        total_pnl += pnl
        total_wins += result.wins
        total_gross_win += sum(t.pnl_pips for t in result.trades if t.pnl_pips > 0)
        total_gross_loss += abs(sum(t.pnl_pips for t in result.trades if t.pnl_pips < 0))
        total_max_dd = max(total_max_dd, result.max_drawdown_pips)
        all_returns.extend(t.pnl_pips for t in result.trades)
        per_symbol[symbol] = (trades, pnl)

    wr = total_wins / total_trades if total_trades else 0.0
    pf = total_gross_win / total_gross_loss if total_gross_loss > 0 else float("inf")

    sharpe = 0.0
    if len(all_returns) >= 2:
        mean = sum(all_returns) / len(all_returns)
        var = sum((r - mean) ** 2 for r in all_returns) / (len(all_returns) - 1)
        std = math.sqrt(var) if var > 0 else 0.0
        sharpe = mean / std if std > 0 else 0.0

    return RunResult(
        strategy=strategy_name,
        params=params,
        total_trades=total_trades,
        total_pnl=total_pnl,
        win_rate=wr,
        profit_factor=pf,
        sharpe=sharpe,
        max_dd=total_max_dd,
        per_symbol=per_symbol,
    )


def _run_one_flat(args: tuple) -> tuple[str, SymbolResult]:
    combo_idx, sym, strategy_name, params, candles, spec, spread = args
    strategy = create_strategy(strategy_name, params)
    result = simulate_symbol(
        symbol=sym, candles=candles, strategy=strategy, spec=spec,
        warmup_bars=250, spread_pips=spread,
    )
    return sym, result


def generate_param_combos(grid: dict[str, list]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid.keys())
    values = list(grid.values())
    combos = []
    for combo in itertools.product(*values):
        params = dict(zip(keys, combo))
        # Skip combos where TP <= SL
        sl = params.get("stop_loss_pips", 25)
        tp = params.get("take_profit_pips", 50)
        if tp <= sl:
            continue
        combos.append(params)
    return combos


def main() -> int:
    parser = argparse.ArgumentParser(description="Optimize strategy parameters")
    parser.add_argument("--db", default="downloads/backtest.db", help="Database path")
    parser.add_argument("--strategy", default=None, help="Optimize single strategy")
    parser.add_argument("--baseline-only", action="store_true", help="Only run baseline")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--top", type=int, default=5, help="Show top N results per strategy")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return 1

    print("Loading data ...")
    history = load_price_history(db_path)
    specs = load_symbol_specs(db_path)
    print(f"  {len(history)} symbols, {sum(len(v) for v in history.values())} ticks")

    # Pre-build candles for all symbols
    print("Building candles ...")
    symbol_candles: dict[str, list[Candle]] = {}
    for sym, ticks in history.items():
        symbol_candles[sym] = resample_to_candles(ticks, resolution_sec=60)
    print(f"  {sum(len(c) for c in symbol_candles.values())} total candles")

    strategies = [args.strategy] if args.strategy else STRATEGIES
    workers = args.workers

    # =========================================================================
    # BASELINE: run each strategy with default params on all symbols
    # =========================================================================
    print(f"\n{'='*90}")
    print("  BASELINE: all strategies x all symbols (default params)")
    print(f"{'='*90}\n")

    baseline_results: dict[str, RunResult] = {}
    t0 = time.monotonic()

    symbol_tasks_all = []
    for sym, candles in sorted(symbol_candles.items()):
        spec = specs.get(sym)
        spread = get_spread(sym)
        symbol_tasks_all.append((sym, candles, spec, spread))

    default_params = {"stop_loss_pips": 25, "take_profit_pips": 50}
    baseline_tasks = [(s, default_params, symbol_tasks_all) for s in strategies]

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(_run_strategy_combo, baseline_tasks):
            baseline_results[result.strategy] = result
            pf_str = f"{result.profit_factor:.2f}" if result.profit_factor < 999 else "INF"
            print(
                f"  {result.strategy:<20} {result.total_trades:>5} trades  "
                f"WR {result.win_rate*100:>5.1f}%  PnL {result.total_pnl:>+10.1f}  "
                f"PF {pf_str:>6}  Sharpe {result.sharpe:>+6.2f}  MaxDD {result.max_dd:>8.1f}",
                flush=True,
            )

    print(f"\nBaseline elapsed: {time.monotonic()-t0:.1f}s")

    if args.baseline_only:
        return 0

    # =========================================================================
    # OPTIMIZATION: grid search for each strategy
    # =========================================================================
    print(f"\n{'='*90}")
    print("  OPTIMIZATION: grid search across parameter space")
    print(f"{'='*90}")

    best_overall: list[RunResult] = []

    for strat_name in strategies:
        grid = PARAM_GRIDS.get(strat_name, {})
        combos = generate_param_combos(grid)
        if len(combos) <= 1:
            print(f"\n[{strat_name}] no parameter grid defined, skipping")
            continue

        print(f"\n[{strat_name}] {len(combos)} parameter combinations x {len(symbol_candles)} symbols = {len(combos)*len(symbol_candles)} tasks")

        # Build flat list of (combo_idx, symbol, candles, spec, spread) tasks
        symbol_list = sorted(symbol_candles.keys())
        flat_tasks = []
        for combo_idx, combo in enumerate(combos):
            for sym in symbol_list:
                flat_tasks.append((combo_idx, sym, strat_name, combo, symbol_candles[sym], specs.get(sym), get_spread(sym)))

        t0 = time.monotonic()
        # Run all tasks in parallel, then aggregate by combo_idx
        combo_results: dict[int, list[tuple[str, SymbolResult]]] = {i: [] for i in range(len(combos))}
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_one_flat, t): t[0] for t in flat_tasks}
            for future in concurrent.futures.as_completed(futures):
                combo_idx = futures[future]
                sym, sr = future.result()
                combo_results[combo_idx].append((sym, sr))

        # Aggregate results per combo
        results: list[RunResult] = []
        for combo_idx, combo in enumerate(combos):
            sym_results = combo_results[combo_idx]
            total_trades = 0
            total_pnl = 0.0
            total_wins = 0
            total_gross_win = 0.0
            total_gross_loss = 0.0
            total_max_dd = 0.0
            all_returns: list[float] = []
            per_symbol: dict[str, tuple[int, float]] = {}
            for sym, sr in sym_results:
                total_trades += sr.total_trades
                total_pnl += sr.total_pnl_pips
                total_wins += sr.wins
                total_gross_win += sum(t.pnl_pips for t in sr.trades if t.pnl_pips > 0)
                total_gross_loss += abs(sum(t.pnl_pips for t in sr.trades if t.pnl_pips < 0))
                total_max_dd = max(total_max_dd, sr.max_drawdown_pips)
                all_returns.extend(t.pnl_pips for t in sr.trades)
                per_symbol[sym] = (sr.total_trades, sr.total_pnl_pips)
            wr = total_wins / total_trades if total_trades else 0.0
            pf = total_gross_win / total_gross_loss if total_gross_loss > 0 else float("inf")
            sharpe = 0.0
            if len(all_returns) >= 2:
                mean = sum(all_returns) / len(all_returns)
                var = sum((r - mean) ** 2 for r in all_returns) / (len(all_returns) - 1)
                std = math.sqrt(var) if var > 0 else 0.0
                sharpe = mean / std if std > 0 else 0.0
            results.append(RunResult(
                strategy=strat_name, params=combo,
                total_trades=total_trades, total_pnl=total_pnl,
                win_rate=wr, profit_factor=pf, sharpe=sharpe,
                max_dd=total_max_dd, per_symbol=per_symbol,
            ))

        elapsed = time.monotonic() - t0

        # Sort by PnL descending
        results.sort(key=lambda r: r.total_pnl, reverse=True)

        print(f"  Completed in {elapsed:.1f}s")
        print(f"\n  {'Rank':<5} {'Trades':>6} {'WR%':>6} {'PnL':>10} {'PF':>6} {'Sharpe':>7} {'MaxDD':>8}   Parameters")
        print(f"  {'-'*85}")

        for i, r in enumerate(results[:args.top]):
            pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 999 else "INF"
            # Show only non-default params
            param_str = ", ".join(f"{k}={v}" for k, v in sorted(r.params.items()))
            print(
                f"  #{i+1:<4} {r.total_trades:>6} {r.win_rate*100:>5.1f}% {r.total_pnl:>+10.1f} "
                f"{pf_str:>6} {r.sharpe:>+7.2f} {r.max_dd:>8.1f}   {param_str}"
            )

        if results:
            best_overall.append(results[0])

        # Show worst too for contrast
        if len(results) > 2:
            worst = results[-1]
            pf_str = f"{worst.profit_factor:.2f}" if worst.profit_factor < 999 else "INF"
            param_str = ", ".join(f"{k}={v}" for k, v in sorted(worst.params.items()))
            print(f"  ...worst: {worst.total_trades:>5} trades  PnL {worst.total_pnl:>+10.1f}  {param_str}")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    if best_overall:
        print(f"\n{'='*90}")
        print("  BEST PARAMETERS PER STRATEGY")
        print(f"{'='*90}\n")

        best_overall.sort(key=lambda r: r.total_pnl, reverse=True)
        for r in best_overall:
            pf_str = f"{r.profit_factor:.2f}" if r.profit_factor < 999 else "INF"
            print(f"  {r.strategy:<20} PnL {r.total_pnl:>+10.1f}  WR {r.win_rate*100:.1f}%  "
                  f"PF {pf_str}  Sharpe {r.sharpe:>+.2f}  MaxDD {r.max_dd:.1f}")
            param_str = json.dumps(r.params, sort_keys=True)
            print(f"    params: {param_str}")

            # Top symbols
            sorted_syms = sorted(r.per_symbol.items(), key=lambda x: x[1][1], reverse=True)
            top_profitable = [(s, t, p) for s, (t, p) in sorted_syms if p > 0][:5]
            if top_profitable:
                sym_str = ", ".join(f"{s}({p:+.0f})" for s, t, p in top_profitable)
                print(f"    best symbols: {sym_str}")
            print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
