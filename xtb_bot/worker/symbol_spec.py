from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging

from xtb_bot.client import BrokerError
from xtb_bot.models import SymbolSpec
from xtb_bot.pip_size import normalize_tick_size_to_pip_size, symbol_pip_size_fallback
from xtb_bot.symbols import is_index_symbol as _shared_is_index_symbol


logger = logging.getLogger(__name__)


class WorkerSymbolSpecRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    @staticmethod
    def broker_supports_force_refresh_fallback(exc: TypeError) -> bool:
        lowered = str(exc or "").lower()
        return "force_refresh" in lowered and (
            "unexpected keyword" in lowered
            or "got an unexpected keyword" in lowered
            or "positional argument" in lowered
        )

    def broker_get_symbol_spec(
        self,
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> SymbolSpec | None:
        worker = self.worker
        getter = getattr(worker.broker, "get_symbol_spec", None)
        if not callable(getter):
            return None
        if force_refresh:
            try:
                return getter(symbol, force_refresh=True)
            except TypeError as exc:
                if not self.broker_supports_force_refresh_fallback(exc):
                    raise
        return getter(symbol)

    def broker_get_symbol_spec_for_epic(
        self,
        symbol: str,
        epic: str,
        *,
        force_refresh: bool = False,
    ) -> SymbolSpec | None:
        worker = self.worker
        getter = getattr(worker.broker, "get_symbol_spec_for_epic", None)
        if not callable(getter):
            return None
        if force_refresh:
            try:
                return getter(symbol, epic, force_refresh=True)
            except TypeError as exc:
                if not self.broker_supports_force_refresh_fallback(exc):
                    raise
        return getter(symbol, epic)

    def broker_get_symbol_spec_candidates_for_entry(
        self,
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> list[SymbolSpec]:
        worker = self.worker
        getter = getattr(worker.broker, "get_symbol_spec_candidates_for_entry", None)
        if not callable(getter):
            return []
        if force_refresh:
            try:
                return list(getter(symbol, force_refresh=True) or [])
            except TypeError as exc:
                if not self.broker_supports_force_refresh_fallback(exc):
                    raise
        return list(getter(symbol) or [])

    def adopt_symbol_spec_for_entry(self, spec: SymbolSpec, *, source: str) -> None:
        worker = self.worker
        worker.symbol_spec = worker._apply_broker_tick_value_calibration(spec)
        self.cache_symbol_pip_size(worker.symbol_spec.tick_size, source=source)
        try:
                worker.store.upsert_broker_symbol_spec(
                    symbol=worker.symbol,
                    spec=worker.symbol_spec,
                    ts=worker._wall_time_now(),
                    source=source,
                )
        except Exception:
            logger.debug("Failed to persist adopted entry symbol spec", exc_info=True)

    def sync_strategy_runtime_pip_size(self, pip_size: float) -> None:
        worker = self.worker
        setter = getattr(worker.strategy, "set_runtime_symbol_pip_size", None)
        if not callable(setter):
            return
        try:
            setter(worker.symbol, float(pip_size))
        except Exception:
            logger.debug("Failed to sync runtime strategy pip size", exc_info=True)

    def cache_symbol_pip_size(self, pip_size: float | None, *, source: str) -> None:
        worker = self.worker
        normalized = normalize_tick_size_to_pip_size(
            worker.symbol,
            pip_size,
            index_pip_size=1.0,
            energy_pip_size=1.0,
        )
        if normalized is None:
            return
        worker._symbol_pip_size = normalized
        self.sync_strategy_runtime_pip_size(normalized)
        try:
                worker.store.upsert_broker_symbol_pip_size(
                    symbol=worker.symbol,
                    pip_size=normalized,
                    ts=worker._wall_time_now(),
                    source=source,
                )
        except Exception:
            logger.debug("Failed to persist broker symbol pip size", exc_info=True)

    def refresh_symbol_pip_size_from_store(self) -> float | None:
        worker = self.worker
        pip_size = worker.store.load_broker_symbol_pip_size(
            worker.symbol,
            max_age_sec=0.0,
        )
        normalized = normalize_tick_size_to_pip_size(
            worker.symbol,
            pip_size,
            index_pip_size=1.0,
            energy_pip_size=1.0,
        )
        if normalized is None:
            return None
        worker._symbol_pip_size = float(normalized)
        self.sync_strategy_runtime_pip_size(float(normalized))
        if pip_size is not None and abs(float(pip_size) - float(normalized)) > FLOAT_ROUNDING_TOLERANCE:
            try:
                worker.store.upsert_broker_symbol_pip_size(
                    symbol=worker.symbol,
                    pip_size=float(normalized),
                    ts=worker._wall_time_now(),
                    source="worker_store_fx_tick_normalized",
                )
            except Exception:
                logger.debug("Failed to persist normalized broker symbol pip size", exc_info=True)
        return worker._symbol_pip_size

    def pip_size_fallback(self) -> float:
        worker = self.worker
        if worker._symbol_pip_size is not None and worker._symbol_pip_size > 0:
            return float(worker._symbol_pip_size)
        cached_pip_size = self.refresh_symbol_pip_size_from_store()
        if cached_pip_size is not None and cached_pip_size > 0:
            return float(cached_pip_size)
        return symbol_pip_size_fallback(
            worker.symbol,
            index_pip_size=1.0,
            energy_pip_size=1.0,
        )

    def context_pip_size(self) -> float | None:
        worker = self.worker
        if worker._symbol_pip_size is not None and worker._symbol_pip_size > 0:
            return float(worker._symbol_pip_size)
        if worker.symbol_spec is not None and worker.symbol_spec.tick_size > 0:
            return normalize_tick_size_to_pip_size(
                worker.symbol,
                worker.symbol_spec.tick_size,
                index_pip_size=1.0,
                energy_pip_size=1.0,
            )
        return None

    def execution_pip_size(self) -> float:
        context_pip_size = self.context_pip_size()
        if context_pip_size is not None and context_pip_size > 0:
            return context_pip_size
        return self.pip_size_fallback()

    def contract_multiplier_fallback(self) -> float:
        worker = self.worker
        upper = worker.symbol.upper()
        if upper.endswith("USD") or upper.startswith("USD"):
            return 100000.0
        if upper.startswith("XAU"):
            return 100.0
        if upper in {"WTI", "BRENT"}:
            return 100.0
        if upper.startswith("US"):
            return 10.0
        return 1000.0

    def execution_pip_size_for_spec(self, symbol_spec: SymbolSpec | None) -> float:
        worker = self.worker
        if symbol_spec is not None and symbol_spec.tick_size > 0:
            return normalize_tick_size_to_pip_size(
                worker.symbol,
                symbol_spec.tick_size,
                index_pip_size=1.0,
                energy_pip_size=1.0,
            )
        return self.execution_pip_size()

    def load_symbol_spec(self) -> None:
        worker = self.worker
        active_position = worker.position_book.get(worker.symbol)
        required_position_epic = worker._position_epic(active_position)
        current_loaded_epic = worker._epic_text()
        if worker.symbol_spec is not None:
            if (
                required_position_epic
                and current_loaded_epic
                and current_loaded_epic != required_position_epic
            ):
                worker.store.record_event(
                    "INFO",
                    worker.symbol,
                    "Rebinding symbol specification to active position epic",
                    {
                        "current_epic": current_loaded_epic,
                        "required_epic": required_position_epic,
                        "required_epic_variant": worker._position_epic_variant(active_position),
                        "source": "active_position_rebind",
                    },
                )
                worker.symbol_spec = None
            else:
                if (
                    (worker._symbol_pip_size is None or worker._symbol_pip_size <= 0)
                    and worker.symbol_spec.tick_size > 0
                ):
                    self.cache_symbol_pip_size(
                        worker.symbol_spec.tick_size,
                        source="worker_symbol_spec_existing",
                    )
                return

        now = worker._monotonic_now()
        if worker._runtime_remaining_sec(
            worker._symbol_spec_retry_after_ts,
            now_monotonic=now,
        ) > FLOAT_COMPARISON_TOLERANCE:
            return

        try:
            refreshed_from_broker = False
            rebind_reason: str | None = None
            cached_spec: SymbolSpec | None = None
            stale_spec: SymbolSpec | None = None
            if worker.db_first_reads_enabled:
                cached_spec = worker.store.load_broker_symbol_spec(
                    worker.symbol,
                    max_age_sec=worker.db_first_symbol_spec_max_age_sec,
                    epic=required_position_epic or None,
                )
                if cached_spec is None:
                    stale_spec = worker.store.load_broker_symbol_spec(
                        worker.symbol,
                        max_age_sec=0.0,
                        epic=required_position_epic or None,
                    )
                    if stale_spec is None:
                        raise BrokerError(
                            f"DB-first symbol spec cache is empty or stale for {worker.symbol}"
                        )
                else:
                    worker.symbol_spec = cached_spec

            if required_position_epic:
                candidate_spec = worker.symbol_spec if worker.symbol_spec is not None else stale_spec
                candidate_epic = ""
                if candidate_spec is not None and isinstance(candidate_spec.metadata, dict):
                    candidate_epic = str(candidate_spec.metadata.get("epic") or "").strip().upper()
                if candidate_spec is not None and candidate_epic == required_position_epic:
                    worker.symbol_spec = candidate_spec
                    if candidate_spec is stale_spec:
                        worker.store.record_event(
                            "WARN",
                            worker.symbol,
                            "Loaded stale DB-first symbol spec fallback",
                            {
                                "max_age_sec": worker.db_first_symbol_spec_max_age_sec,
                                "required_epic": required_position_epic,
                            },
                        )
                else:
                    worker.store.record_event(
                        "INFO",
                        worker.symbol,
                        "Rebinding symbol specification to active position epic",
                        {
                            "required_epic": required_position_epic,
                            "required_epic_variant": worker._position_epic_variant(active_position),
                            "cached_epic": candidate_epic or None,
                            "source": "active_position_rebind",
                        },
                    )
                    getter = getattr(worker.broker, "get_symbol_spec_for_epic", None)
                    with worker._watchdog_blocking_operation(
                        "load_symbol_spec_for_active_position_epic",
                        last_price=worker._latest_price(),
                        grace_sec=120.0,
                    ):
                        if callable(getter):
                            worker.symbol_spec = getter(worker.symbol, required_position_epic)
                        else:
                            worker.symbol_spec = self.broker_get_symbol_spec(worker.symbol)
                    refreshed_from_broker = True
                    rebind_reason = "active_position_epic"
            elif worker.db_first_reads_enabled:
                db_candidate_spec = worker.symbol_spec if worker.symbol_spec is not None else stale_spec
                if (
                    db_candidate_spec is not None
                    and isinstance(db_candidate_spec.metadata, dict)
                    and str(db_candidate_spec.metadata.get("broker") or "").strip().lower() == "ig"
                    and _shared_is_index_symbol(worker.symbol)
                ):
                    worker.symbol_spec = db_candidate_spec
                    if db_candidate_spec is stale_spec:
                        worker.store.record_event(
                            "WARN",
                            worker.symbol,
                            "Loaded stale DB-first symbol spec fallback",
                            {"max_age_sec": worker.db_first_symbol_spec_max_age_sec},
                        )
                elif worker.symbol_spec is None:
                    worker.symbol_spec = stale_spec
                    worker.store.record_event(
                        "WARN",
                        worker.symbol,
                        "Loaded stale DB-first symbol spec fallback",
                        {"max_age_sec": worker.db_first_symbol_spec_max_age_sec},
                    )
            else:
                with worker._watchdog_blocking_operation(
                    "load_symbol_spec",
                    last_price=worker._latest_price(),
                    grace_sec=120.0,
                ):
                    worker.symbol_spec = self.broker_get_symbol_spec(worker.symbol)
                    refreshed_from_broker = True
        except Exception:
            next_backoff = min(
                120.0,
                max(
                    5.0,
                    worker._symbol_spec_retry_backoff_sec * 2.0
                    if worker._symbol_spec_retry_backoff_sec > 0
                    else 5.0,
                ),
            )
            worker._symbol_spec_retry_backoff_sec = next_backoff
            worker._symbol_spec_retry_after_ts = now + next_backoff
            raise
        raw_symbol_spec = worker.symbol_spec
        worker._db_first_symbol_spec_cache_miss_streak = 0
        worker._symbol_spec_retry_backoff_sec = 0.0
        worker._symbol_spec_retry_after_ts = 0.0
        worker.symbol_spec = worker._apply_broker_tick_value_calibration(worker.symbol_spec)
        self.cache_symbol_pip_size(worker.symbol_spec.tick_size, source="worker_symbol_spec_load")
        try:
            worker.store.upsert_broker_symbol_spec(
                symbol=worker.symbol,
                spec=raw_symbol_spec if raw_symbol_spec is not None else worker.symbol_spec,
                ts=worker._wall_time_now(),
                source="worker_symbol_spec_load",
            )
        except Exception:
            logger.debug("Failed to persist loaded symbol specification", exc_info=True)
        payload = {
            "tick_size": worker.symbol_spec.tick_size,
            "tick_value": worker.symbol_spec.tick_value,
            "contract_size": worker.symbol_spec.contract_size,
            "lot_min": worker.symbol_spec.lot_min,
            "lot_max": worker.symbol_spec.lot_max,
            "lot_step": worker.symbol_spec.lot_step,
            "refreshed_from_broker": refreshed_from_broker,
        }
        if rebind_reason:
            payload["rebind_reason"] = rebind_reason
        if worker.symbol_spec.metadata:
            payload["metadata"] = worker.symbol_spec.metadata
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Loaded symbol specification",
            payload,
        )
