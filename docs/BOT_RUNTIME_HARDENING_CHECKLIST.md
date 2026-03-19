# Bot Runtime Hardening Checklist

This file is the operational checklist to avoid repeated production regressions.
Use it before deploy, after incidents, and before changing `ig_client.py`, `worker.py`, `bot.py`, `risk_manager.py`.

## 1. Non-Negotiable Runtime Invariants

1. `position_id` is the primary identity for open/close state.
2. `dealReference` binding is exact full-string match only.
3. Trade-critical calls (`POST/DELETE /positions/otc`, `PUT /positions/otc/{id}`, `GET /confirms/*`) are never downgraded to non-critical behavior.
4. Startup/runtime sync failures must not mutate open-position state to closed unless broker evidence exists.
5. Bot shutdown must close broker/store even if worker threads are stuck (forced close path).

## 2. IG Allowance Protection Rules

1. If allowance backoff is active, skip non-critical REST-heavy loops.
2. Runtime `/positions` sync must back off, not retry-storm.
3. Passive history refresh must exit early on allowance errors.
4. Startup warmup must not flood `/markets` for all symbols at once.

## 3. Position Integrity Rules

1. Every execution-mode open must persist:
- local trade row
- pending-open lifecycle
- dealReference binding
2. Every close must require IG confirm status `ACCEPTED`.
3. Manual/external closes must reconcile via strict broker evidence.
4. Partial close must update sync with `closed_volume` and `close_complete`.

## 4. Symbol/Market Validity Rules

1. Never keep retrying an epic that returns `instrument.epic.unavailable` without cooldown.
2. For symbols with frequent alias issues (GOLD/XAU, DOGE, WTI/BRENT), keep candidate fallback order deterministic.
3. `UNKNOWN`/`instrument invalid` on non-FX CFDs should trigger next-epic path, not tight loop on same epic.

## 5. Risk and Sizing Rules

1. Margin pre-check must use broker metadata when available (`margin_factor` > fallback leverage).
2. Keep conservative local overlays enabled in execution:
- overhead percent
- weekend multiplier
- free-margin reserve after open
3. If volume is below broker min lot, block once and promote adaptive min lot from reject context.

## 6. Strategy/Worker Safety Rules

1. Access to `workers` maps must be under lock or via snapshot copies.
2. Worker restart logic must be idempotent and schedule-aware.
3. Strategy creation in hot paths must be cached (history estimate + symbol support decisions).
4. History buffer estimation must be capped to prevent memory/db blow-ups.

## 7. Mandatory Regression Tests Before Deploy

Run all commands:

```bash
PYTHONPATH=. pytest -q tests/test_ig_streaming.py tests/test_worker_exit_logic.py
PYTHONPATH=. pytest -q tests/test_state_recovery.py tests/test_strategy_schedule.py
PYTHONPATH=. pytest -q tests/test_bot_passive_history.py tests/test_ig_config_and_broker.py
```

If any command fails: do not deploy.

## 8. Incident Triage Template (Fill Every Time)

1. Incident time (UTC):
2. First broker error code:
3. Impacted symbols:
4. Was position lost from local DB, broker, or both:
5. Matching `position_id`:
6. Matching `dealReference`:
7. Allowance backoff active at incident time (`yes/no`):
8. Fix commit:
9. Added/updated tests:
10. Rollback plan:

## 9. Change Control (Required for Critical Files)

For every PR touching critical runtime files:

1. List exactly which invariants are affected.
2. Add at least one failing test first, then fix.
3. Include production-safe fallback behavior for broker errors.
4. Record config/env impact in `.env.example`.
