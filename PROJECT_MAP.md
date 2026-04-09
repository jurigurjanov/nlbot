# Project Map

Эта карта нужна для быстрого онбординга в новый сеанс. Если сессия свежая, начинай отсюда, а не с хаотичного чтения файлов.

## Read Order

1. `AGENTS.md`
2. `PROJECT_MAP.md`
3. `README.md`
4. Дальше уже идти в нужный домен:
   - orchestration: `xtb_bot/bot.py`
   - worker runtime: `xtb_bot/worker/`
   - broker/data: `xtb_bot/ig_client.py`, `xtb_bot/ig_proxy.py`, `xtb_bot/client.py`
   - persistence: `xtb_bot/state_store.py`
   - sizing/risk: `xtb_bot/risk_manager.py`
   - strategies: `xtb_bot/strategies/`
   - CLI/debug: `xtb_bot/cli.py`

## High-Level Runtime Flow

`run_bot.py`
-> exec-loads `xtb_bot/cli.py`
-> builds `BotConfig`
-> creates `TradingBot`
-> `TradingBot.start()/run_forever()`
-> starts broker, restores state, launches one `SymbolWorker` per active symbol
-> each worker runs its own signal/risk/execution loop
-> state is persisted to SQLite
-> CLI read-only commands inspect the same SQLite database

## Main Entry Points

- `run_bot.py`
  - tiny wrapper that loads the canonical CLI implementation from `xtb_bot/cli.py`
- `xtb_bot/cli.py`
  - all CLI commands
  - run/start modes
  - read-only diagnostics (`--show-status`, `--show-trades`, `--show-trade-reasons`, etc.)
- `xtb_bot/bot.py`
  - top-level orchestrator
  - worker lifecycle
  - startup restore/sync
  - periodic runtime monitor/watchdog
  - shared latest tick cache
  - broker-wide runtime tasks

## Broker Stack

- `xtb_bot/client.py`
  - `BaseBrokerClient`
  - mock/XTB support
  - common broker abstractions
- `xtb_bot/ig_client.py`
  - real IG REST + streaming implementation
  - close/open sync
  - history lookup
  - symbol specs
  - exact per-`epic` symbol-spec cache alongside latest-per-symbol cache
  - account snapshot
  - connectivity / stream status
- `xtb_bot/ig_proxy.py`
  - rate-limited wrapper around IG broker client
  - account-trading/account-non-trading buckets
- `xtb_bot/broker_method_support.py`
  - introspection-based method calling for unstable broker method signatures
  - currently used for safe `get_position_close_sync(...)` invocation

## Persistence / State

- `xtb_bot/state_store.py`
  - SQLite schema + read/write API
  - primary runtime state boundary
  - important tables:
    - `schema_migrations`
      - applied SQLite migration ledger + `PRAGMA user_version` source of truth
    - `trades`
    - `pending_opens`
    - `events`
    - `worker_states`
    - `account_snapshots`
    - `price_history`
    - `candle_history`
    - `broker_ticks`
    - `broker_tick_history`
      - append-only recent tick history; preserves same-timestamp updates that the latest-cache table would otherwise overwrite
    - `broker_symbol_specs`
      - variant-aware broker metadata cache keyed by symbol + `epic`
      - used by DB-first sizing/min-lot/tick-value paths
    - `position_updates`
    - `kv_store`
- `xtb_bot/position_book.py`
  - in-memory active-position registry
  - keeps both `open` and transitional `closing` positions during runtime/bootstrap
  - authoritative runtime view inside a running process

Important rule:
- if something looks wrong in CLI, always decide whether the truth should come from:
  - `position_book` / live runtime
  - SQLite store
  - broker sync / IG history

## Worker Architecture

`xtb_bot/worker/` is a package now. The old god-object file was decomposed into runtime slices.

### Worker entry files

- `xtb_bot/worker/__init__.py`
  - public import surface
  - `from xtb_bot.worker import SymbolWorker`
- `xtb_bot/worker/core.py`
  - `SymbolWorker` class
  - thin facade + clock helpers
- `xtb_bot/worker/bootstrap.py`
  - worker initialization, runtime wiring, buffer setup
- `xtb_bot/worker/init_bundle.py`
  - init bundle/dependency preparation
- `xtb_bot/worker/method_bindings.py`
  - declarative binding of thin facade methods to runtime components
- `xtb_bot/worker/state_proxies.py`
  - compatibility proxy properties that expose runtime state via `SymbolWorker`

### Worker runtime slices

- `config_runtime.py`
  - env/config parsing helpers inside worker domain
- `parameters.py`
  - parameter initialization
- `strategy_params.py`
  - strategy-scoped param resolution
- `orchestrator.py`
  - main loop orchestration
- `cycle.py`
  - active-position vs entry-cycle execution flow
- `signal_runtime.py`
  - build contexts and produce signals
- `multi_strategy.py`
  - multi-strategy aggregation/netting
- `risk.py`
  - entry gates, cooldowns, confidence, spread guards, micro-chop logic
- `orders.py`
  - open/close/modify/trailing/partial TP
  - runtime position-scoped state
  - runtime state sweeping/pruning
- `reconcile.py`
  - broker close sync, manual close reconcile, missing-position reconcile
- `protective_runtime.py`
  - MFE/MAE, fast-fail, breakeven, protective exits
- `dynamic_exit_runtime.py`
  - signal-driven and synthetic exit reasoning
- `trailing_runtime.py`
  - adaptive trailing candidate logic
- `execution_quality.py`
  - slippage/fill-quality logic
- `value_runtime.py`
  - PnL/currency/value normalization
- `price_runtime.py`
  - price normalization helpers
- `price_guard_runtime.py`
  - broker min-stop/open-level guards
- `symbol_spec.py`
  - symbol spec loading and pip-size/spec fallbacks
  - rebinds DB-first symbol specs to active-position `epic` when needed
- `symbol_runtime.py`
  - symbol classification and epic helpers
- `history.py`
  - price/candle history restore and snapshots
- `data_access.py`
  - DB-first/direct broker fallback reads
- `health.py`
  - connectivity + stream health
- `recovery.py`
  - allowance/backoff/broker error recovery
- `runtime_state.py`
  - worker state flush + account snapshot cache
- `diagnostics.py`
  - hold reasons, trend summaries, indicator debug
- `lifecycle.py`
  - worker lease and blocking-operation watchdog integration
- `trade_metadata.py`
  - entry/exit strategy metadata on positions/trades

### Worker state that tends to matter in bugs

- position-scoped runtime maps live primarily in `worker/orders.py`
- signal/risk gating state lives in `worker/risk.py`
- stream/connectivity health cache lives in `worker/health.py`
- runtime worker-state flush and cached account snapshot live in `worker/runtime_state.py`

## Strategy Layer

Directory: `xtb_bot/strategies/`

Key strategies:
- `momentum.py`
- `g1.py`
- `g2.py`
- `trend_following.py`
- `index_hybrid.py`
- `mean_breakout_v2.py`
- `mean_reversion_bb.py`
- `donchian_breakout.py`
- `oil.py`
- `crypto_trend_following.py`

Common helpers:
- `base.py`
- `__init__.py` factory export

Related non-strategy runtime:
- `xtb_bot/multi_strategy.py`
  - cross-strategy aggregation primitives and shared decision objects
- `xtb_bot/strategy_profiles.py`
  - safe/aggressive/conservative profile defaults

## Config / Environment

- `xtb_bot/config.py`
  - `BotConfig`, `RiskConfig`, env parsing, global defaults
- `.env`
  - local runtime values
- `.env.example`
  - template; must stay synced after the marked sync line

Rule:
- if adding a config parameter, update `.env`, `.env.example`, and `README.md` in one change

## Debugging Entry Points

If the problem is...

### No trades / too few trades

Start here:
- `xtb_bot/cli.py`
- `xtb_bot/bot.py`
- `xtb_bot/worker/risk.py`
- `xtb_bot/worker/signal_runtime.py`
- `xtb_bot/worker/multi_strategy.py`

Useful CLI:
- `xtb-bot --show-status`
- `xtb-bot --show-trade-reasons`
- `xtb-bot --show-db-first-health`
- `xtb-bot --show-ig-allowance`
- `xtb-bot --show-active-schedule`

### Bot stopped writing to DB / hung / watchdog fired

Start here:
- `xtb_bot/bot.py`
- `xtb_bot/worker/orchestrator.py`
- `xtb_bot/worker/lifecycle.py`
- `xtb_bot/worker/health.py`
- `xtb_bot/ig_proxy.py`
- `xtb_bot/ig_client.py`

Look for events:
- `Runtime monitor task failed`
- `Runtime monitor watchdog detected stale main loop`
- `Runtime watchdog aborting process`
- `Bot shutdown requested`
- `Bot shutdown completed`

### Wrong close price / pnl / reconcile issues

Start here:
- `xtb_bot/worker/reconcile.py`
- `xtb_bot/worker/orders.py`
- `xtb_bot/bot.py`
- `xtb_bot/ig_client.py`
- `xtb_bot/broker_method_support.py`

### Stale data / DB-first / history warmup

Start here:
- `xtb_bot/worker/history.py`
- `xtb_bot/worker/data_access.py`
- `xtb_bot/bot.py`
- `xtb_bot/state_store.py`

### Risk sizing / blocked trades / min lot

Start here:
- `xtb_bot/risk_manager.py`
- `xtb_bot/worker/risk.py`
- `xtb_bot/worker/entry_planner.py`
- `xtb_bot/worker/symbol_spec.py`

## SQLite Reality Map

When debugging, remember the runtime has three layers:

1. live in-memory worker state
2. SQLite persisted state
3. broker truth from IG/XTB

Common mismatch patterns:
- open in broker, missing locally
- closed in broker, still open in SQLite
- stale worker runtime map despite position already gone
- pending open survived broker timeout but not yet matched to real position

Never assume one layer is authoritative without checking which code path owns the issue.

## Useful File Groups

### Runtime / execution

- `xtb_bot/bot.py`
- `xtb_bot/worker/`
- `xtb_bot/risk_manager.py`
- `xtb_bot/state_store.py`

### Broker / market data

- `xtb_bot/client.py`
- `xtb_bot/ig_client.py`
- `xtb_bot/ig_proxy.py`
- `xtb_bot/symbols.py`
- `xtb_bot/pip_size.py`

### Diagnostics / CLI

- `xtb_bot/cli.py`
- `docs/BOT_FIX_GUARDRAILS.md`
- `docs/BOT_RUNTIME_HARDENING_CHECKLIST.md`

### Numeric / safety helpers

- `xtb_bot/numeric.py`
- `xtb_bot/tolerances.py`
- `xtb_bot/broker_method_support.py`

## Test Map

Most useful targeted suites:
- `tests/test_worker_exit_logic.py`
  - the biggest worker runtime regression suite
- `tests/test_state_recovery.py`
  - restart/sync/reconcile/runtime recovery
- `tests/test_ig_streaming.py`
  - IG-specific stream/history/sync logic
- `tests/test_run_bot_cli.py`
  - CLI behavior
- `tests/test_run_bot_alerts.py`
  - status/trades/timeline diagnostics
- strategy-specific suites:
  - `tests/test_momentum_strategy.py`
  - `tests/test_g2_strategy.py`
  - `tests/test_trend_following_strategy.py`
  - etc.

## Current Architectural Reality

What is already true:
- `SymbolWorker` is no longer the old business-logic god object
- the worker is now a facade over runtime components in `xtb_bot/worker/`
- worker state sweeping is periodic and no longer relies only on graceful close
- runtime interval logic uses monotonic time
- magic float epsilons were centralized in `xtb_bot/tolerances.py`
- broker close sync method calling is now signature-aware via introspection

What to remember before changing code:
- trading/risk/execution changes default to global scope unless explicitly strategy-specific
- every code/config change in this repo must be committed
- if config keys change, sync `.env`, `.env.example`, and `README.md`

## Session Checklist For A Fresh Restart

1. Read `AGENTS.md`
2. Read `PROJECT_MAP.md`
3. Run `git status --short`
4. If debugging runtime:
   - inspect latest DB-backed status first
   - then inspect the relevant runtime module
5. Before changing logic:
   - identify whether truth should come from worker memory, SQLite, or broker sync
6. After every change:
   - run targeted tests
   - commit
