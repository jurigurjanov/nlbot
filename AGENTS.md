# Agent Workflow

- On a fresh session, read `PROJECT_MAP.md` right after this file before diving into the codebase.
- Treat `PROJECT_MAP.md` as the fast orientation map for runtime flow, module ownership, persistence layers, and debugging entry points.
- Update `PROJECT_MAP.md` whenever the project structure or runtime architecture changes in a meaningful way: moved modules, new entry points, new persistence/runtime layers, major refactors, or changes in debugging flow.
- After every code or configuration change, create a git commit in this repository.
- Default scope for trading logic, risk management, and execution behavior fixes is global across all strategies.
- All changes to trading logic, risk management, and execution behavior must account for their impact on other strategies and avoid silent regressions outside the immediate target case.
- In `EXECUTION` mode, never allow silent fallback from live broker operations to mock brokers, stale local state, paper-like behavior, or degraded startup paths; fail fast instead of continuing with unverified live-trading state.
- Apply strategy-specific fixes only when the user explicitly requests a single-strategy change.
- After every change, review the surrounding and adjacent code paths for possible breakage in other modules, strategies, runtime flows, persistence paths, and watchdog/diagnostic behavior before considering the task done.
- Finish each requested task end-to-end in the current pass whenever feasible: implement the full change, clean up adjacent config/docs/tests, verify it, and avoid leaving obvious follow-up work for later.
- Do not create or grow God Objects. If a class or module starts mixing multiple responsibilities (for example orchestration, broker I/O, persistence, risk, execution management, diagnostics), extract a dedicated component instead of adding more logic to the existing object.
- Treat large facade/orchestrator classes such as `TradingBot`, `SymbolWorker`, and CLI entry modules as composition roots: new domain logic should go into focused modules, not back into the coordinator.
- Keep `.env` synchronized with `.env.example` for all entries after `##### SYNC AFTER THIS LINE ######`.
- When adding any new config parameter, update `.env`, `.env.example`, and `README.md` in the same change.
