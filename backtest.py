#!/usr/bin/env python3
"""
Backtester for NLBot strategies.

Replays price_history from a SQLite state database through strategy signal
generation and simulates trade execution with SL/TP.

Usage:
    python backtest.py --db state.bck/xtb_bot.db
    python backtest.py --db downloads/backtest.db --strategy momentum --symbols US100,DE40
    python backtest.py --db downloads/backtest.db --strategy index_hybrid --profile conservative
    python backtest.py --db downloads/backtest.db --compare
    python backtest.py --db downloads/backtest.db --equity-csv equity.csv
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

# ---------------------------------------------------------------------------
# Bootstrap project imports
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from xtb_bot.models import Side, Signal
from xtb_bot.pip_size import symbol_pip_size_fallback
from xtb_bot.strategies import create_strategy, available_strategies
from xtb_bot.strategies.base import Strategy, StrategyContext
from xtb_bot.strategy_profiles import apply_strategy_profile


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class SymbolSpec:
    symbol: str
    tick_size: float
    tick_value: float
    contract_size: float
    lot_min: float
    lot_step: float
    one_pip_means: float | None


@dataclass(slots=True)
class SimTrade:
    symbol: str
    side: Side
    entry_price: float
    entry_ts: float
    stop_loss: float
    take_profit: float
    sl_pips: float
    tp_pips: float
    confidence: float
    exit_price: float = 0.0
    exit_ts: float = 0.0
    exit_reason: str = ""
    pnl_pips: float = 0.0
    peak_favorable_pips: float = 0.0
    trailing_activated: bool = False


@dataclass
class SymbolResult:
    symbol: str
    trades: list[SimTrade] = field(default_factory=list)
    signals_buy: int = 0
    signals_sell: int = 0
    signals_hold: int = 0
    equity_curve: list[tuple[float, float]] = field(default_factory=list)

    @property
    def total_trades(self) -> int:
        return len(self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.pnl_pips > 0)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.trades if t.pnl_pips <= 0)

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades else 0.0

    @property
    def total_pnl_pips(self) -> float:
        return sum(t.pnl_pips for t in self.trades)

    @property
    def avg_win_pips(self) -> float:
        wins = [t.pnl_pips for t in self.trades if t.pnl_pips > 0]
        return sum(wins) / len(wins) if wins else 0.0

    @property
    def avg_loss_pips(self) -> float:
        losses = [t.pnl_pips for t in self.trades if t.pnl_pips <= 0]
        return sum(losses) / len(losses) if losses else 0.0

    @property
    def profit_factor(self) -> float:
        gross_win = sum(t.pnl_pips for t in self.trades if t.pnl_pips > 0)
        gross_loss = abs(sum(t.pnl_pips for t in self.trades if t.pnl_pips < 0))
        return gross_win / gross_loss if gross_loss > 0 else float("inf")

    @property
    def max_drawdown_pips(self) -> float:
        peak = 0.0
        equity = 0.0
        max_dd = 0.0
        for t in self.trades:
            equity += t.pnl_pips
            peak = max(peak, equity)
            dd = peak - equity
            max_dd = max(max_dd, dd)
        return max_dd

    @property
    def sharpe_ratio(self) -> float:
        if len(self.trades) < 2:
            return 0.0
        returns = [t.pnl_pips for t in self.trades]
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        return mean / std if std > 0 else 0.0

    @property
    def sortino_ratio(self) -> float:
        if len(self.trades) < 2:
            return 0.0
        returns = [t.pnl_pips for t in self.trades]
        mean = sum(returns) / len(returns)
        downside = [min(r, 0.0) ** 2 for r in returns]
        downside_dev = math.sqrt(sum(downside) / (len(downside) - 1)) if len(downside) > 1 else 0.0
        return mean / downside_dev if downside_dev > 0 else 0.0


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def load_price_history(
    db_path: Path,
    symbols: list[str] | None = None,
) -> dict[str, list[tuple[float, float, float | None]]]:
    """Load price history as {symbol: [(ts, close, volume), ...]} sorted by ts."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            rows = con.execute(
                f"SELECT symbol, ts, close, volume FROM price_history "
                f"WHERE symbol IN ({placeholders}) ORDER BY symbol, ts",
                [s.upper() for s in symbols],
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT symbol, ts, close, volume FROM price_history ORDER BY symbol, ts"
            ).fetchall()
    finally:
        con.close()

    history: dict[str, list[tuple[float, float, float | None]]] = defaultdict(list)
    for row in rows:
        history[row["symbol"]].append((
            float(row["ts"]),
            float(row["close"]),
            float(row["volume"]) if row["volume"] is not None else None,
        ))
    return dict(history)


def load_symbol_specs(db_path: Path) -> dict[str, SymbolSpec]:
    """Load broker symbol specs from DB."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT symbol, tick_size, tick_value, contract_size, "
            "lot_min, lot_step, one_pip_means FROM broker_symbol_specs GROUP BY symbol"
        ).fetchall()
    finally:
        con.close()

    specs: dict[str, SymbolSpec] = {}
    for row in rows:
        specs[row["symbol"]] = SymbolSpec(
            symbol=row["symbol"],
            tick_size=float(row["tick_size"]),
            tick_value=float(row["tick_value"]),
            contract_size=float(row["contract_size"]),
            lot_min=float(row["lot_min"]),
            lot_step=float(row["lot_step"]),
            one_pip_means=float(row["one_pip_means"]) if row["one_pip_means"] else None,
        )
    return specs


# ---------------------------------------------------------------------------
# Candle resampling
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class Candle:
    ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float


def resample_to_candles(
    ticks: list[tuple[float, float, float | None]],
    resolution_sec: int = 60,
) -> list[Candle]:
    """Resample tick-level (ts, close, volume) to OHLCV candles."""
    if not ticks:
        return []

    candles: list[Candle] = []
    bucket_start = math.floor(ticks[0][0] / resolution_sec) * resolution_sec
    bucket_open = ticks[0][1]
    bucket_high = ticks[0][1]
    bucket_low = ticks[0][1]
    bucket_close = ticks[0][1]
    bucket_volume = 0.0

    for ts, price, vol in ticks:
        candle_start = math.floor(ts / resolution_sec) * resolution_sec
        if candle_start > bucket_start:
            candles.append(Candle(
                ts=bucket_start,
                open=bucket_open,
                high=bucket_high,
                low=bucket_low,
                close=bucket_close,
                volume=bucket_volume,
            ))
            gap_start = bucket_start + resolution_sec
            while gap_start < candle_start:
                candles.append(Candle(
                    ts=gap_start,
                    open=bucket_close,
                    high=bucket_close,
                    low=bucket_close,
                    close=bucket_close,
                    volume=0.0,
                ))
                gap_start += resolution_sec
            bucket_start = candle_start
            bucket_open = price
            bucket_high = price
            bucket_low = price
            bucket_close = price
            bucket_volume = 0.0

        bucket_high = max(bucket_high, price)
        bucket_low = min(bucket_low, price)
        bucket_close = price
        bucket_volume += vol if vol else 0.0

    candles.append(Candle(
        ts=bucket_start,
        open=bucket_open,
        high=bucket_high,
        low=bucket_low,
        close=bucket_close,
        volume=bucket_volume,
    ))
    return candles


# ---------------------------------------------------------------------------
# Pip size — prefer spec.one_pip_means, then symbol_pip_size_fallback()
# ---------------------------------------------------------------------------
def resolve_pip_size(symbol: str, spec: SymbolSpec | None) -> float:
    if spec and spec.one_pip_means and spec.one_pip_means > 0:
        return spec.one_pip_means
    return symbol_pip_size_fallback(symbol, index_pip_size=1.0, energy_pip_size=0.01)


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------
def _compute_pnl(side: Side, entry: float, exit_price: float, pip_size: float) -> float:
    if side == Side.BUY:
        return (exit_price - entry) / pip_size
    return (entry - exit_price) / pip_size


# ---------------------------------------------------------------------------
# Trailing stop engine
# ---------------------------------------------------------------------------
def _update_trailing_stop(
    trade: SimTrade,
    mark: float,
    pip_size: float,
    *,
    trailing_distance_pips: float,
    breakeven_offset_pips: float,
    activation_ratio: float,
    breakeven_min_peak_pips: float,
) -> None:
    """Update trade's stop_loss in-place based on trailing logic."""
    tp_distance = abs(trade.take_profit - trade.entry_price)
    if tp_distance <= 0:
        return

    if trade.side == Side.BUY:
        favorable = mark - trade.entry_price
        progress = favorable / tp_distance
        peak_pips = max(trade.peak_favorable_pips, favorable / pip_size)
        trade.peak_favorable_pips = peak_pips
    else:
        favorable = trade.entry_price - mark
        progress = favorable / tp_distance
        peak_pips = max(trade.peak_favorable_pips, favorable / pip_size)
        trade.peak_favorable_pips = peak_pips

    if favorable <= 0:
        return

    be_offset = breakeven_offset_pips * pip_size
    trail_dist = trailing_distance_pips * pip_size

    # Breakeven lock: once peak reaches threshold, lock SL at breakeven
    if peak_pips >= breakeven_min_peak_pips:
        if trade.side == Side.BUY:
            be_level = trade.entry_price + be_offset
            if be_level > trade.stop_loss:
                trade.stop_loss = be_level
        else:
            be_level = trade.entry_price - be_offset
            if be_level < trade.stop_loss:
                trade.stop_loss = be_level

    # Distance trailing: after activation, trail SL at fixed distance from mark
    if progress >= activation_ratio:
        trade.trailing_activated = True
        if trade.side == Side.BUY:
            candidate = mark - trail_dist
            # Only move SL up, never down
            if candidate > trade.stop_loss:
                trade.stop_loss = candidate
        else:
            candidate = mark + trail_dist
            # Only move SL down (for sell), never up
            if candidate < trade.stop_loss:
                trade.stop_loss = candidate


def simulate_symbol(
    symbol: str,
    candles: list[Candle],
    strategy: Strategy,
    spec: SymbolSpec | None,
    *,
    warmup_bars: int = 250,
    eval_interval_bars: int = 1,
    cooldown_bars: int = 5,
    spread_pips: float = 1.0,
    commission_pips: float = 0.0,
    trailing_enabled: bool = True,
    trailing_distance_pips: float = 10.0,
    trailing_activation_ratio: float = 0.3,
    trailing_breakeven_offset_pips: float = 2.0,
    trailing_breakeven_min_peak_pips: float = 4.0,
) -> SymbolResult:
    """
    Run strategy signals over candle history for one symbol.

    Walk-forward: at each bar after warmup, build StrategyContext from all
    bars up to that point, call generate_signal(), simulate trade entry/exit.
    """
    result = SymbolResult(symbol=symbol)

    if len(candles) < warmup_bars + 10:
        return result

    pip_size = resolve_pip_size(symbol, spec)
    tick_size = spec.tick_size if spec else pip_size

    # Pre-build arrays once (avoid O(n^2) list comprehensions per bar)
    all_prices = [c.close for c in candles]
    all_timestamps = [c.ts for c in candles]
    all_volumes = [c.volume for c in candles]

    open_trade: SimTrade | None = None
    cooldown_until_bar: int = 0
    equity = 0.0

    for bar_idx in range(warmup_bars, len(candles)):
        candle = candles[bar_idx]

        # --- Trailing stop update (use previous bar's close as mark) ---
        if open_trade is not None and trailing_enabled and bar_idx > warmup_bars:
            prev_close = candles[bar_idx - 1].close
            # Also track peak from candle extremes
            if open_trade.side == Side.BUY:
                mark = max(prev_close, candle.open)
            else:
                mark = min(prev_close, candle.open)
            _update_trailing_stop(
                open_trade, mark, pip_size,
                trailing_distance_pips=trailing_distance_pips,
                breakeven_offset_pips=trailing_breakeven_offset_pips,
                activation_ratio=trailing_activation_ratio,
                breakeven_min_peak_pips=trailing_breakeven_min_peak_pips,
            )

        # --- Check open trade SL/TP against candle range ---
        if open_trade is not None:
            hit_sl = False
            hit_tp = False

            if open_trade.side == Side.BUY:
                hit_sl = candle.low <= open_trade.stop_loss
                hit_tp = candle.high >= open_trade.take_profit
            else:
                hit_sl = candle.high >= open_trade.stop_loss
                hit_tp = candle.low <= open_trade.take_profit

            if hit_sl and hit_tp:
                # Disambiguate: which level is closer to candle open?
                dist_sl = abs(candle.open - open_trade.stop_loss)
                dist_tp = abs(candle.open - open_trade.take_profit)
                if dist_sl <= dist_tp:
                    hit_tp = False
                else:
                    hit_sl = False

            if hit_sl:
                open_trade.exit_price = open_trade.stop_loss
                open_trade.exit_ts = candle.ts
                open_trade.exit_reason = "stop_loss"
                open_trade.pnl_pips = _compute_pnl(open_trade.side, open_trade.entry_price, open_trade.exit_price, pip_size) - commission_pips
                equity += open_trade.pnl_pips
                result.equity_curve.append((candle.ts, equity))
                result.trades.append(open_trade)
                open_trade = None
                cooldown_until_bar = bar_idx + cooldown_bars
            elif hit_tp:
                open_trade.exit_price = open_trade.take_profit
                open_trade.exit_ts = candle.ts
                open_trade.exit_reason = "take_profit"
                open_trade.pnl_pips = _compute_pnl(open_trade.side, open_trade.entry_price, open_trade.exit_price, pip_size) - commission_pips
                equity += open_trade.pnl_pips
                result.equity_curve.append((candle.ts, equity))
                result.trades.append(open_trade)
                open_trade = None
                cooldown_until_bar = bar_idx + cooldown_bars

        # --- Generate signal ---
        if bar_idx % eval_interval_bars != 0:
            continue

        # Pass only the last `warmup_bars` prices — strategies don't need
        # more history and this keeps generate_signal() at O(warmup) per bar
        # instead of O(bar_idx).
        window_start = max(0, bar_idx + 1 - warmup_bars)
        end = bar_idx + 1
        ctx = StrategyContext(
            symbol=symbol,
            prices=all_prices[window_start:end],
            timestamps=all_timestamps[window_start:end],
            volumes=all_volumes[window_start:end],
            current_volume=candle.volume if candle.volume else None,
            current_spread_pips=spread_pips,
            tick_size=tick_size,
            pip_size=pip_size,
        )

        try:
            signal = strategy.generate_signal(ctx)
        except Exception:
            continue

        if signal.side == Side.BUY:
            result.signals_buy += 1
        elif signal.side == Side.SELL:
            result.signals_sell += 1
        else:
            result.signals_hold += 1
            continue

        # --- Entry logic ---
        if open_trade is not None:
            continue
        if bar_idx < cooldown_until_bar:
            continue

        sl_pips = max(signal.stop_loss_pips, 5.0)
        tp_pips = max(signal.take_profit_pips, sl_pips * 1.5)

        half_spread = spread_pips * pip_size * 0.5

        if signal.side == Side.BUY:
            entry = candle.close + half_spread
            sl = entry - sl_pips * pip_size
            tp = entry + tp_pips * pip_size
        else:
            entry = candle.close - half_spread
            sl = entry + sl_pips * pip_size
            tp = entry - tp_pips * pip_size

        open_trade = SimTrade(
            symbol=symbol,
            side=signal.side,
            entry_price=entry,
            entry_ts=candle.ts,
            stop_loss=sl,
            take_profit=tp,
            sl_pips=sl_pips,
            tp_pips=tp_pips,
            confidence=signal.confidence,
        )

    # Close any remaining open trade at last price
    if open_trade is not None:
        last = candles[-1]
        open_trade.exit_price = last.close
        open_trade.exit_ts = last.ts
        open_trade.exit_reason = "end_of_data"
        open_trade.pnl_pips = _compute_pnl(open_trade.side, open_trade.entry_price, open_trade.exit_price, pip_size) - commission_pips
        equity += open_trade.pnl_pips
        result.equity_curve.append((last.ts, equity))
        result.trades.append(open_trade)

    return result


# ---------------------------------------------------------------------------
# Default spread estimates per symbol class
# ---------------------------------------------------------------------------
_DEFAULT_SPREADS: dict[str, float] = {
    "EURUSD": 0.8, "GBPUSD": 1.2, "USDJPY": 1.0, "AUDUSD": 0.9,
    "USDCHF": 1.0, "USDCAD": 1.2, "EURGBP": 1.2, "EURCHF": 1.5,
    "GBPJPY": 2.0,
    "US100": 1.5, "US500": 0.8, "US30": 2.0, "US2000": 2.0,
    "DE40": 2.0, "UK100": 1.5, "FR40": 2.5, "EU50": 2.0,
    "IT40": 3.0, "ES35": 3.0, "SK20": 3.0, "NK20": 3.0,
    "JPN225": 7.0, "TOPIX": 1.0, "AUS200": 3.0,
    "GOLD": 3.0, "WTI": 4.0, "BRENT": 4.0,
}


def get_spread(symbol: str) -> float:
    return _DEFAULT_SPREADS.get(symbol.upper(), 2.0)


# ---------------------------------------------------------------------------
# Default strategy params (minimal viable set for backtesting)
# ---------------------------------------------------------------------------
def load_strategy_params_from_env() -> dict[str, Any]:
    """Try to load XTB_STRATEGY_PARAMS from .env if available."""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return {}
    try:
        content = env_path.read_text(encoding="utf-8")
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("XTB_STRATEGY_PARAMS="):
                json_str = stripped[len("XTB_STRATEGY_PARAMS="):]
                return json.loads(json_str)
    except Exception:
        pass
    return {}


def load_strategy_specific_params(strategy_name: str) -> dict[str, Any]:
    """Try to load XTB_STRATEGY_PARAMS_<STRATEGY> from .env."""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return {}
    key = f"XTB_STRATEGY_PARAMS_{strategy_name.upper()}="
    try:
        content = env_path.read_text(encoding="utf-8")
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith(key):
                json_str = stripped[len(key):]
                return json.loads(json_str)
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def format_results_table(
    results: dict[str, SymbolResult],
    label: str = "",
) -> str:
    """Format results as an aligned text table."""
    lines: list[str] = []
    if label:
        lines.append(f"\n{'=' * 100}")
        lines.append(f"  {label}")
        lines.append(f"{'=' * 100}")

    header = (
        f"{'Symbol':<10} {'Trades':>6} {'Wins':>5} {'Loss':>5} "
        f"{'WR%':>6} {'PnL':>10} {'AvgWin':>8} {'AvgLoss':>8} "
        f"{'PF':>6} {'MaxDD':>8} {'Sharpe':>7} {'Sortino':>8} {'Signals':>12}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    sorted_symbols = sorted(results.keys())
    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_pnl = 0.0

    for sym in sorted_symbols:
        r = results[sym]
        if r.total_trades == 0:
            lines.append(f"{sym:<10} {'--':>6}")
            continue
        total_trades += r.total_trades
        total_wins += r.wins
        total_losses += r.losses
        total_pnl += r.total_pnl_pips
        pf = r.profit_factor
        pf_str = f"{pf:.2f}" if pf < 999 else "INF"
        signals = f"B{r.signals_buy}/S{r.signals_sell}/H{r.signals_hold}"
        sharpe_str = f"{r.sharpe_ratio:.2f}" if r.total_trades >= 2 else "--"
        sortino_str = f"{r.sortino_ratio:.2f}" if r.total_trades >= 2 else "--"
        lines.append(
            f"{sym:<10} {r.total_trades:>6} {r.wins:>5} {r.losses:>5} "
            f"{r.win_rate * 100:>5.1f}% {r.total_pnl_pips:>+10.1f} "
            f"{r.avg_win_pips:>+8.1f} {r.avg_loss_pips:>+8.1f} "
            f"{pf_str:>6} {r.max_drawdown_pips:>8.1f} {sharpe_str:>7} {sortino_str:>8} {signals:>12}"
        )

    lines.append("-" * len(header))
    total_wr = total_wins / total_trades * 100 if total_trades else 0
    lines.append(
        f"{'TOTAL':<10} {total_trades:>6} {total_wins:>5} {total_losses:>5} "
        f"{total_wr:>5.1f}% {total_pnl:>+10.1f}"
    )
    return "\n".join(lines)


def format_trade_list(results: dict[str, SymbolResult], limit: int = 50) -> str:
    """Format detailed trade list."""
    from datetime import datetime, timezone

    all_trades: list[SimTrade] = []
    for r in results.values():
        all_trades.extend(r.trades)
    all_trades.sort(key=lambda t: t.entry_ts)

    lines = [
        f"\n{'Symbol':<8} {'Side':<5} {'Entry':>10} {'SL':>10} {'TP':>10} "
        f"{'Exit':>10} {'PnL':>8} {'Reason':<12} {'Conf':>5}"
    ]
    lines.append("-" * 90)
    for t in all_trades[:limit]:
        ts_str = datetime.fromtimestamp(t.entry_ts, tz=timezone.utc).strftime("%m/%d %H:%M")
        lines.append(
            f"{t.symbol:<8} {t.side.value:<5} {t.entry_price:>10.2f} "
            f"{t.stop_loss:>10.2f} {t.take_profit:>10.2f} "
            f"{t.exit_price:>10.2f} {t.pnl_pips:>+8.1f} "
            f"{t.exit_reason:<12} {t.confidence:>5.2f}"
        )
    if len(all_trades) > limit:
        lines.append(f"  ... and {len(all_trades) - limit} more trades")
    return "\n".join(lines)


def write_equity_csv(results: dict[str, SymbolResult], path: Path) -> None:
    """Write combined equity curve to CSV."""
    from datetime import datetime, timezone

    all_points: list[tuple[float, str, float]] = []
    for r in results.values():
        for ts, eq in r.equity_curve:
            all_points.append((ts, r.symbol, eq))
    all_points.sort(key=lambda x: x[0])

    with open(path, "w") as f:
        f.write("timestamp,datetime,symbol,equity_pips\n")
        for ts, sym, eq in all_points:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{ts:.0f},{dt},{sym},{eq:.2f}\n")


# ---------------------------------------------------------------------------
# Parallel simulation helper
# ---------------------------------------------------------------------------
def _simulate_one(args: tuple) -> tuple[str, SymbolResult]:
    (symbol, ticks, strategy_name, strategy_params, spec,
     warmup_bars, candle_sec, spread_pips, commission_pips,
     trailing_enabled, trailing_distance_pips, trailing_activation_ratio,
     trailing_breakeven_offset_pips, trailing_breakeven_min_peak_pips) = args
    candles = resample_to_candles(ticks, resolution_sec=candle_sec)
    strategy = create_strategy(strategy_name, strategy_params)
    result = simulate_symbol(
        symbol=symbol,
        candles=candles,
        strategy=strategy,
        spec=spec,
        warmup_bars=warmup_bars,
        spread_pips=spread_pips,
        commission_pips=commission_pips,
        trailing_enabled=trailing_enabled,
        trailing_distance_pips=trailing_distance_pips,
        trailing_activation_ratio=trailing_activation_ratio,
        trailing_breakeven_offset_pips=trailing_breakeven_offset_pips,
        trailing_breakeven_min_peak_pips=trailing_breakeven_min_peak_pips,
    )
    return symbol, result


# ---------------------------------------------------------------------------
# Main backtest runner
# ---------------------------------------------------------------------------
def run_backtest(
    db_path: Path,
    strategy_name: str,
    symbols: list[str] | None = None,
    params_override: dict[str, Any] | None = None,
    profile: str | None = None,
    warmup_bars: int = 250,
    candle_sec: int = 60,
    commission_pips: float = 0.0,
    parallel: bool = False,
    verbose: bool = False,
    trailing_enabled: bool = True,
    trailing_distance_pips: float = 10.0,
    trailing_activation_ratio: float = 0.3,
    trailing_breakeven_offset_pips: float = 2.0,
    trailing_breakeven_min_peak_pips: float = 4.0,
) -> dict[str, SymbolResult]:
    """Run backtest for a strategy across symbols."""

    base_params = load_strategy_params_from_env()
    strategy_specific = load_strategy_specific_params(strategy_name)
    params = {**base_params, **strategy_specific}

    if profile:
        params, applied = apply_strategy_profile(strategy_name, params, profile)
        if verbose:
            print(f"Profile '{profile}' applied: {applied}")

    if params_override:
        params.update(params_override)

    params.setdefault("stop_loss_pips", 25)
    params.setdefault("take_profit_pips", 50)

    print(f"Loading price history from {db_path} ...")
    history = load_price_history(db_path, symbols)
    specs = load_symbol_specs(db_path)

    if not history:
        print("No price history found!")
        return {}

    print(f"Loaded {sum(len(v) for v in history.values())} ticks across {len(history)} symbols")

    if parallel and len(history) > 1:
        workers = min(len(history), os.cpu_count() or 4)
        print(f"Running {len(history)} symbols in parallel ({workers} workers) ...")
        task_args = [
            (sym, ticks, strategy_name, params, specs.get(sym),
             warmup_bars, candle_sec, get_spread(sym), commission_pips,
             trailing_enabled, trailing_distance_pips, trailing_activation_ratio,
             trailing_breakeven_offset_pips, trailing_breakeven_min_peak_pips)
            for sym, ticks in sorted(history.items())
        ]
        results: dict[str, SymbolResult] = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            for sym, result in pool.map(_simulate_one, task_args):
                results[sym] = result
                if verbose:
                    print(f"  {sym}: {result.total_trades} trades")
        return results

    # Sequential
    strategy = create_strategy(strategy_name, params)
    results = {}
    for sym, ticks in sorted(history.items()):
        candles = resample_to_candles(ticks, resolution_sec=candle_sec)
        if verbose:
            print(f"  {sym}: {len(ticks)} ticks -> {len(candles)} candles ({candle_sec}s)")

        spec = specs.get(sym)
        spread = get_spread(sym)

        result = simulate_symbol(
            symbol=sym,
            candles=candles,
            strategy=strategy,
            spec=spec,
            warmup_bars=warmup_bars,
            spread_pips=spread,
            commission_pips=commission_pips,
            trailing_enabled=trailing_enabled,
            trailing_distance_pips=trailing_distance_pips,
            trailing_activation_ratio=trailing_activation_ratio,
            trailing_breakeven_offset_pips=trailing_breakeven_offset_pips,
            trailing_breakeven_min_peak_pips=trailing_breakeven_min_peak_pips,
        )
        results[sym] = result

    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest NLBot strategies against historical price data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python backtest.py --db downloads/backtest.db
  python backtest.py --db downloads/backtest.db --strategy g2 --symbols US100,DE40
  python backtest.py --db downloads/backtest.db --profile aggressive
  python backtest.py --db downloads/backtest.db --compare
  python backtest.py --db downloads/backtest.db --parallel --equity-csv equity.csv
        """,
    )
    parser.add_argument("--db", default=None, help="Path to SQLite state database")
    parser.add_argument("--download-dir", default=None, help="Use downloads dir (default: downloads/backtest.db)")
    parser.add_argument(
        "--strategy", default="momentum",
        help=f"Strategy name (available: {', '.join(sorted(available_strategies()))})",
    )
    parser.add_argument("--symbols", default=None, help="Comma-separated symbol filter")
    parser.add_argument("--params", default=None, help="JSON params override")
    parser.add_argument("--profile", default=None, help="Strategy profile: safe|conservative|aggressive")
    parser.add_argument("--warmup", type=int, default=250, help="Warmup bars before trading (default: 250)")
    parser.add_argument("--candle-sec", type=int, default=60, help="Candle resolution in seconds (default: 60)")
    parser.add_argument("--commission-pips", type=float, default=0.0, help="Commission per trade in pips (default: 0)")
    parser.add_argument("--no-trailing", action="store_true", help="Disable trailing stop")
    parser.add_argument("--trailing-distance", type=float, default=10.0, help="Trailing distance in pips (default: 10)")
    parser.add_argument("--trailing-activation", type=float, default=0.3, help="Trailing activation ratio 0-1 (default: 0.3)")
    parser.add_argument("--trailing-breakeven-offset", type=float, default=2.0, help="Breakeven offset pips (default: 2.0)")
    parser.add_argument("--trailing-breakeven-peak", type=float, default=4.0, help="Min peak pips for breakeven (default: 4.0)")
    parser.add_argument("--trades", action="store_true", help="Show detailed trade list")
    parser.add_argument("--compare", action="store_true", help="Compare conservative vs aggressive profiles")
    parser.add_argument("--parallel", action="store_true", help="Run symbols in parallel (multi-process)")
    parser.add_argument("--equity-csv", default=None, help="Write equity curve to CSV file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()
    if args.download_dir:
        db_path = Path(args.download_dir) / "backtest.db"
    elif args.db:
        db_path = Path(args.db)
    else:
        db_path = Path("downloads/backtest.db")
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        if not args.db and not args.download_dir:
            print("Run 'python download_data.py' first to download data.")
        return 1

    symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    params_override = json.loads(args.params) if args.params else None
    trailing_kw = dict(
        trailing_enabled=not args.no_trailing,
        trailing_distance_pips=args.trailing_distance,
        trailing_activation_ratio=args.trailing_activation,
        trailing_breakeven_offset_pips=args.trailing_breakeven_offset,
        trailing_breakeven_min_peak_pips=args.trailing_breakeven_peak,
    )

    if args.compare:
        t0 = time.monotonic()
        print(f"Strategy: {args.strategy}")
        print(f"Comparing conservative vs aggressive profiles ...\n")

        results_cons = run_backtest(
            db_path, args.strategy, symbols,
            params_override=params_override,
            profile="conservative",
            warmup_bars=args.warmup,
            candle_sec=args.candle_sec,
            commission_pips=args.commission_pips,
            parallel=args.parallel,
            verbose=args.verbose,
            **trailing_kw,
        )
        print(format_results_table(results_cons, label=f"{args.strategy} / CONSERVATIVE"))

        results_aggr = run_backtest(
            db_path, args.strategy, symbols,
            params_override=params_override,
            profile="aggressive",
            warmup_bars=args.warmup,
            candle_sec=args.candle_sec,
            commission_pips=args.commission_pips,
            parallel=args.parallel,
            verbose=args.verbose,
            **trailing_kw,
        )
        print(format_results_table(results_aggr, label=f"{args.strategy} / AGGRESSIVE"))

        def _totals(results: dict[str, SymbolResult]) -> tuple[int, int, float]:
            trades = sum(r.total_trades for r in results.values())
            wins = sum(r.wins for r in results.values())
            pnl = sum(r.total_pnl_pips for r in results.values())
            return trades, wins, pnl

        ct, cw, cp = _totals(results_cons)
        at, aw, ap = _totals(results_aggr)
        print(f"\n{'COMPARISON':=^60}")
        print(f"  Conservative: {ct} trades, WR {cw/ct*100:.1f}%, PnL {cp:+.1f} pips" if ct else "  Conservative: 0 trades")
        print(f"  Aggressive:   {at} trades, WR {aw/at*100:.1f}%, PnL {ap:+.1f} pips" if at else "  Aggressive:   0 trades")
        winner = "CONSERVATIVE" if cp >= ap else "AGGRESSIVE"
        print(f"  Winner: {winner}")
        print(f"\nElapsed: {time.monotonic() - t0:.1f}s")
        return 0

    # Single run
    t0 = time.monotonic()
    label_parts = [args.strategy]
    if args.profile:
        label_parts.append(args.profile)

    results = run_backtest(
        db_path, args.strategy, symbols,
        params_override=params_override,
        profile=args.profile,
        warmup_bars=args.warmup,
        candle_sec=args.candle_sec,
        commission_pips=args.commission_pips,
        parallel=args.parallel,
        verbose=args.verbose,
        **trailing_kw,
    )
    print(format_results_table(results, label=" / ".join(label_parts)))

    if args.trades:
        print(format_trade_list(results))

    if args.equity_csv:
        eq_path = Path(args.equity_csv)
        write_equity_csv(results, eq_path)
        print(f"\nEquity curve written to {eq_path}")

    elapsed = time.monotonic() - t0
    print(f"\nElapsed: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
