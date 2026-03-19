# Bot Fix Guardrails

This file is a persistent checklist to prevent repeated regressions in IG execution mode.
For runtime/deploy flow use:
- `docs/BOT_RUNTIME_HARDENING_CHECKLIST.md`

## Critical Invariants

1. Position identity must always be `position_id`-first.
2. Deal reference matching must be exact normalized full-string match, never prefix match.
3. Runtime recovery (`/positions`) is non-critical and must back off on IG allowance limits.
4. Open/close order operations are critical and must not be silently treated as success without IG confirmation.
5. Symbol auto-disable must not trigger from short transient bursts.

## Repeated Incidents And Permanent Fixes

1. Lost positions from symbol-key overwrite:
- Fixed by storing and restoring open positions keyed by `position_id`.

2. Wrong close reconciliation from history:
- Fixed by paginated history scan with strict match priority and close-evidence filtering.

3. Pending-open leakage:
- Fixed by guaranteed `pending_open` cleanup in worker open flow `finally`.

4. Non-critical `/positions` calls causing allowance storms:
- Fixed by allowance-aware backoff in `get_managed_open_positions`.

5. Symbol disabled too fast on temporary epic errors:
- Fixed by minimum error-window gate before disable.

6. GOLD/XAU unavailable epic loops:
- Fixed by wider default epic candidate set and existing search-based remap.

## Mandatory Test Gate Before Deploy

Run:

```bash
PYTHONPATH=. pytest -q tests/test_ig_streaming.py tests/test_worker_exit_logic.py tests/test_state_recovery.py tests/test_run_bot_alerts.py
PYTHONPATH=. pytest -q -k 'not config_limits'
```

Do not deploy if either command fails.
