# CLAUDE.md — Agent Instructions for nlbot

## Configuration Change Protocol

**CRITICAL**: When modifying strategy parameters, protective exit thresholds, or any config values:

1. **Always update `.env.example`** with the new parameter and its default value
2. **Always update `README.md`** if the parameter is user-facing or changes trading behavior
3. **After editing `.env.example`**: synchronize `.env` by copying everything **after** the line `##### SYNC AFTER THIS LINE ######` from `.env.example` into `.env`
4. **Never rely solely on Python code defaults** in `config.py` — the `.env` file has higher priority and will override code defaults. If a parameter exists in `.env`, changing its default in Python has **no effect**.

### Why this matters

The bot reads parameters in this priority order (highest first):
1. `XTB_STRATEGY_PARAMS_<STRATEGY>` in `.env` (strategy-specific JSON)
2. `XTB_STRATEGY_PARAMS` in `.env` (global JSON)
3. Config file (if any)
4. Python defaults in `config.py` (`_GLOBAL_DEFAULTS`, `_INDEX_HYBRID_DEFAULTS`, etc.)

If a parameter exists in `.env`, the Python default is **never used**. Always check `.env` first.

## Project Overview

- Python 3.13+ algorithmic trading bot for IG.com broker
- Multi-threaded: one thread per symbol (27 symbols), fully synchronous
- SQLite with WAL mode, per-thread connections
- Lightstreamer SDK v2 for streaming prices
- Multi-strategy netting with family-based aggregation (trend vs mean-reversion)

## Key Architecture

- `xtb_bot/bot.py` — Main bot orchestrator
- `xtb_bot/worker/` — Per-symbol worker with mixin-based decomposition
- `xtb_bot/strategies/` — Strategy implementations (momentum, g1, g2, index_hybrid, etc.)
- `xtb_bot/multi_strategy.py` — Core aggregation engine (VirtualPositionAggregator)
- `xtb_bot/worker/multi_strategy.py` — Worker-side multi-strategy orchestration
- `xtb_bot/ig_client.py` + mixins — IG broker API client
- `xtb_bot/risk_manager.py` — Position sizing and risk checks
- `xtb_bot/state_store.py` — SQLite persistence layer

## Parallelization

Always run backtests, optimizations, and strategy matrix scans with **8 parallel workers** (`--workers 8` or `ProcessPoolExecutor(max_workers=8)`). Never run sequential when parallel is available.

## Commit Convention

Always commit after code changes (standing instruction from user: "всегда делай коммиты после изменений в коде").
