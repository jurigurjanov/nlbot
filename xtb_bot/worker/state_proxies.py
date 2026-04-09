from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class WorkerStateProxySpec:
    property_name: str
    manager_attr: str
    state_attr: str | None = None
    cast: Callable[[object], object] | None = None

    @property
    def resolved_state_attr(self) -> str:
        return self.state_attr or self.property_name.lstrip("_")


def _spec(
    property_name: str,
    manager_attr: str,
    cast: Callable[[object], object] | None = None,
    *,
    state_attr: str | None = None,
) -> WorkerStateProxySpec:
    return WorkerStateProxySpec(
        property_name=property_name,
        manager_attr=manager_attr,
        state_attr=state_attr,
        cast=cast,
    )


_PROXY_SPECS: tuple[WorkerStateProxySpec, ...] = (
    # Health monitor state that is still referenced directly by tests/runtime helpers.
    _spec("_last_stream_health", "_health_monitor"),
    _spec("_cached_connectivity_status", "_health_monitor"),
    _spec("_next_connectivity_probe_ts", "_health_monitor", float),
    # Multi-strategy runtime compatibility surface.
    _spec("_multi_strategy_enabled", "_multi_strategy_runtime", bool, state_attr="enabled"),
    _spec("_multi_strategy_components", "_multi_strategy_runtime", state_attr="components"),
    _spec("_multi_strategy_intent_ttl_sec", "_multi_strategy_runtime", float, state_attr="intent_ttl_sec"),
    _spec("_multi_strategy_aggregator", "_multi_strategy_runtime", state_attr="aggregator"),
    _spec("_last_multi_strategy_decision", "_multi_strategy_runtime", state_attr="last_decision"),
    _spec("_multi_strategy_scale_out_offset_by_name", "_multi_strategy_runtime", state_attr="scale_out_offset_by_name"),
    # Order runtime state referenced by tests and adjacent runtimes.
    _spec("_pending_close_retry_position_id", "_order_manager"),
    _spec("_pending_close_retry_until_ts", "_order_manager", float),
    _spec("_pending_close_retry_reason", "_order_manager"),
    _spec("_pending_close_verification_position_id", "_order_manager"),
    _spec("_pending_close_verification_reason", "_order_manager"),
    _spec("_pending_close_verification_started_ts", "_order_manager", float),
    _spec("_last_close_deferred_event_ts", "_order_manager", float),
    _spec("_last_close_deferred_position_id", "_order_manager"),
    _spec("_pending_missing_position_id", "_order_manager"),
    _spec("_pending_missing_position_until_ts", "_order_manager", float),
    _spec("_pending_missing_position_deadline_ts", "_order_manager", float),
    _spec("_manual_close_sync_next_check_ts", "_order_manager", float),
    _spec("_manual_close_sync_position_id", "_order_manager"),
    _spec("_strategy_trailing_overrides_by_position", "_order_manager"),
    _spec("_position_peak_favorable_pips", "_order_manager"),
    _spec("_position_peak_favorable_ts", "_order_manager"),
    _spec("_position_peak_adverse_pips", "_order_manager"),
    _spec("_position_trailing_last_update_ts", "_order_manager"),
    _spec("_position_partial_take_profit_done", "_order_manager"),
    _spec("_position_entry_strategy_by_id", "_order_manager"),
    _spec("_position_entry_component_by_id", "_order_manager"),
    _spec("_position_entry_signal_by_id", "_order_manager"),
    # Risk runtime compatibility surface.
    _spec("_next_entry_allowed_ts", "_risk_evaluator", float),
    _spec("_active_entry_cooldown_sec", "_risk_evaluator", float),
    _spec("_active_entry_cooldown_outcome", "_risk_evaluator", str),
    _spec("_entry_signal_persistence_signature", "_risk_evaluator"),
    _spec("_same_side_reentry_block_side", "_risk_evaluator"),
    _spec("_same_side_reentry_block_until_ts", "_risk_evaluator", float),
    _spec("_continuation_reentry_block_side", "_risk_evaluator"),
    _spec("_last_entry_tick_stale_log_ts", "_risk_evaluator", float),
    # Diagnostics compatibility surface.
    _spec("_handled_news_events", "_diagnostics_runtime"),
    _spec("_last_hold_reason_log_ts", "_diagnostics_runtime", float),
    # Recovery/runtime safety state still referenced directly by tests/runtime loops.
    _spec("_epic_unavailable_consecutive_errors", "_recovery_manager", int),
    _spec("_symbol_disabled", "_recovery_manager", bool),
    _spec("_symbol_disabled_reason", "_recovery_manager"),
    _spec("_allowance_backoff_until_ts", "_recovery_manager", float),
    _spec("_allowance_backoff_streak", "_recovery_manager", int),
    _spec("_db_first_tick_cache_miss_streak", "_recovery_manager", int),
    _spec("_db_first_tick_cache_retry_after_ts", "_recovery_manager", float),
    _spec("_db_first_tick_cache_retry_backoff_sec", "_recovery_manager", float),
    _spec("_db_first_active_tick_fallback_last_warn_ts", "_recovery_manager", float),
    _spec("_db_first_entry_tick_fallback_last_warn_ts", "_recovery_manager", float),
    _spec("_db_first_account_snapshot_fallback_last_warn_ts", "_recovery_manager", float),
    _spec("_db_first_account_snapshot_fallback_next_attempt_ts", "_recovery_manager", float),
    _spec("_db_first_symbol_spec_cache_miss_streak", "_recovery_manager", int),
    _spec("_symbol_spec_retry_after_ts", "_recovery_manager", float),
    _spec("_symbol_spec_retry_backoff_sec", "_recovery_manager", float),
    # Runtime-state cache compatibility surface.
    _spec("_cached_account_snapshot", "_runtime_state_manager"),
    _spec("_cached_account_snapshot_ts", "_runtime_state_manager", float),
    _spec("_last_saved_worker_state_ts", "_runtime_state_manager", float),
    _spec("_last_saved_worker_state_monotonic", "_runtime_state_manager", float),
    _spec("_last_saved_worker_state_signature", "_runtime_state_manager"),
)


def _build_proxy(spec: WorkerStateProxySpec) -> property:
    def getter(self):
        manager = getattr(self, spec.manager_attr)
        return getattr(manager.state, spec.resolved_state_attr)

    def setter(self, value):
        manager = getattr(self, spec.manager_attr)
        if spec.cast is not None:
            value = spec.cast(value)
        setattr(manager.state, spec.resolved_state_attr, value)

    return property(getter, setter)


def bind_symbol_worker_state_proxies(cls: type) -> type:
    for spec in _PROXY_SPECS:
        setattr(cls, spec.property_name, _build_proxy(spec))
    return cls
