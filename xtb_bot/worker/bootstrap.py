from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections import deque
import logging
import math
import os
import threading

from xtb_bot.pip_size import normalize_tick_size_to_pip_size
from xtb_bot.strategies.base import Strategy
from xtb_bot.worker.config_runtime import WorkerConfigRuntime
from xtb_bot.worker.cycle import WorkerCycleRuntime
from xtb_bot.worker.data_access import WorkerDataAccessRuntime
from xtb_bot.worker.diagnostics import WorkerDiagnosticsRuntime
from xtb_bot.worker.dynamic_exit_runtime import WorkerDynamicExitRuntime
from xtb_bot.worker.entry_planner import WorkerEntryPlanner
from xtb_bot.worker.execution_quality import WorkerExecutionQualityRuntime
from xtb_bot.worker.health import WorkerHealthMonitor
from xtb_bot.worker.history import WorkerHistoryRuntime
from xtb_bot.worker.lifecycle import WorkerLifecycleManager
from xtb_bot.worker.multi_strategy import WorkerMultiStrategyRuntime
from xtb_bot.worker.orchestrator import WorkerOrchestrator
from xtb_bot.worker.orders import WorkerOrderManager
from xtb_bot.worker.parameters import WorkerParameterRuntime
from xtb_bot.worker.price_guard_runtime import WorkerPriceGuardRuntime
from xtb_bot.worker.price_runtime import WorkerPriceRuntime
from xtb_bot.worker.protective_runtime import WorkerProtectiveRuntime
from xtb_bot.worker.reconcile import WorkerReconcileManager
from xtb_bot.worker.recovery import WorkerRecoveryManager
from xtb_bot.worker.risk import WorkerRiskEvaluator
from xtb_bot.worker.runtime_state import WorkerRuntimeStateManager
from xtb_bot.worker.signal_runtime import WorkerSignalRuntime
from xtb_bot.worker.symbol_runtime import WorkerSymbolRuntime
from xtb_bot.worker.symbol_spec import WorkerSymbolSpecRuntime
from xtb_bot.worker.strategy_params import WorkerStrategyParamRuntime
from xtb_bot.worker.trade_metadata import WorkerTradeMetadataRuntime
from xtb_bot.worker.trailing_runtime import WorkerTrailingRuntime
from xtb_bot.worker.init_bundle import WorkerInitBundle
from xtb_bot.worker.value_runtime import WorkerValueRuntime


logger = logging.getLogger(__name__)


class WorkerBootstrapRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def bootstrap_symbol_worker(
        self,
        *,
        bundle: WorkerInitBundle,
        multi_strategy_carrier_name: str,
        multi_strategy_base_component_param: str,
    ) -> None:
        worker = self.worker
        worker.symbol = bundle.symbol
        worker.mode = bundle.mode
        worker.strategy_name = bundle.strategy_name
        worker.store = bundle.store
        worker.broker = bundle.broker
        worker.risk = bundle.risk
        worker.position_book = bundle.position_book
        worker.stop_event = bundle.stop_event
        worker.poll_interval_sec = bundle.poll_interval_sec
        worker.poll_jitter_sec = bundle.poll_jitter_sec
        worker.default_volume = bundle.default_volume
        worker.bot_magic_prefix = bundle.bot_magic_prefix
        worker.bot_magic_instance = bundle.bot_magic_instance
        worker.db_first_reads_enabled = bundle.db_first_reads_enabled
        worker.db_first_tick_max_age_sec = bundle.db_first_tick_max_age_sec
        worker.db_first_symbol_spec_max_age_sec = bundle.db_first_symbol_spec_max_age_sec
        worker.db_first_account_snapshot_max_age_sec = bundle.db_first_account_snapshot_max_age_sec
        worker._latest_tick_getter = bundle.latest_tick_getter
        worker._latest_tick_updater = bundle.latest_tick_updater
        worker._worker_lease_key = bundle.worker_lease_key
        worker._worker_lease_id = bundle.worker_lease_id
        worker._worker_lease_check_interval_sec = max(
            0.1,
            float(os.getenv("XTB_WORKER_LEASE_CHECK_INTERVAL_SEC", "0.5")),
        )
        worker._spread_buffer_lock = threading.RLock()
        worker.symbol_spec = None
        worker._config_runtime = WorkerConfigRuntime(worker)
        worker._strategy_param_runtime = WorkerStrategyParamRuntime(worker)
        self.initialize_strategy_runtime(
            strategy_name=bundle.strategy_name,
            strategy_params=bundle.strategy_params,
            strategy_params_map=bundle.strategy_params_map,
            strategy_symbols_map=bundle.strategy_symbols_map,
            multi_strategy_carrier_name=multi_strategy_carrier_name,
            multi_strategy_base_component_param=multi_strategy_base_component_param,
        )
        self.initialize_history_buffers(strategy_params=bundle.strategy_params)
        self.initialize_runtime_components(strategy_params=bundle.strategy_params)
        self.finalize_runtime_startup(strategy_params=bundle.strategy_params)

    def initialize_strategy_runtime(
        self,
        *,
        strategy_name: str,
        strategy_params: dict[str, object],
        strategy_params_map: dict[str, dict[str, object]] | None,
        strategy_symbols_map: dict[str, list[str]] | None,
        multi_strategy_carrier_name: str,
        multi_strategy_base_component_param: str,
    ) -> None:
        worker = self.worker
        normalized_strategy_name = worker._normalize_strategy_label(strategy_name)
        worker._multi_strategy_carrier_enabled = normalized_strategy_name == multi_strategy_carrier_name
        raw_base_component = (
            strategy_params.get(multi_strategy_base_component_param)
            if isinstance(strategy_params, dict)
            else None
        )
        resolved_base_component = worker._normalize_strategy_label(raw_base_component)
        if worker._multi_strategy_carrier_enabled and not resolved_base_component:
            configured_names = worker._parse_strategy_names(
                strategy_params.get("multi_strategy_names") if isinstance(strategy_params, dict) else None
            )
            if not configured_names:
                configured_names = worker._parse_strategy_names(
                    strategy_params.get("multi_strategy_default_names") if isinstance(strategy_params, dict) else None
                )
            for configured_name in configured_names:
                if configured_name != multi_strategy_carrier_name:
                    resolved_base_component = configured_name
                    break
        if not resolved_base_component:
            resolved_base_component = normalized_strategy_name
        worker._multi_strategy_base_component_name = resolved_base_component

        cached_pip_size = worker.store.load_broker_symbol_pip_size(worker.symbol, max_age_sec=0.0)
        worker._symbol_pip_size = normalize_tick_size_to_pip_size(
            worker.symbol,
            cached_pip_size,
            index_pip_size=1.0,
            energy_pip_size=1.0,
        )
        worker._runtime_strategy_overlay = {
            "_worker_mode": (
                worker.mode.value if hasattr(worker.mode, "value") else str(worker.mode or "")
            ),
        }
        runtime_strategy_params = dict(strategy_params)
        runtime_strategy_params.update(worker._runtime_strategy_overlay)
        if worker._symbol_pip_size is not None and worker._symbol_pip_size > 0:
            pip_size_payload = {
                worker.symbol.upper(): float(worker._symbol_pip_size),
            }
            worker._runtime_strategy_overlay[Strategy._RUNTIME_SYMBOL_PIP_SIZES_KEY] = pip_size_payload
            runtime_strategy_params[Strategy._RUNTIME_SYMBOL_PIP_SIZES_KEY] = pip_size_payload

        worker._strategy_params_map = {}
        if isinstance(strategy_params_map, dict):
            for raw_name, raw_params in strategy_params_map.items():
                name = worker._normalize_strategy_label(raw_name)
                if not name or not isinstance(raw_params, dict):
                    continue
                worker._strategy_params_map[name] = dict(raw_params)
        worker._strategy_params_map[normalized_strategy_name] = dict(strategy_params)
        if (
            worker._multi_strategy_carrier_enabled
            and worker._multi_strategy_base_component_name
            and worker._multi_strategy_base_component_name not in worker._strategy_params_map
        ):
            worker._strategy_params_map[worker._multi_strategy_base_component_name] = dict(strategy_params)

        worker.strategy = worker._create_strategy_instance(strategy_name, runtime_strategy_params)
        worker._strategy_symbols_scope = worker._normalize_strategy_symbols_scope(strategy_symbols_map)
        worker._multi_strategy_runtime = WorkerMultiStrategyRuntime(worker)
        worker._fill_quality_summary_cache = {}
        worker.multi_strategy_emergency_event_cooldown_sec = max(
            5.0,
            worker._strategy_float_param(
                strategy_params,
                "multi_strategy_emergency_event_cooldown_sec",
                30.0,
            ),
        )
        worker._initialize_multi_strategy_runtime(
            strategy_params=strategy_params,
            runtime_strategy_params=runtime_strategy_params,
        )
        worker._signal_min_history = max(
            [int(getattr(worker.strategy, "min_history", 1) or 1)]
            + [
                int(getattr(item_strategy, "min_history", 1) or 1)
                for _name, item_strategy in worker._multi_strategy_components
            ]
        )
        worker._signal_candle_timeframe_sec = max(
            [int(float(getattr(worker.strategy, "candle_timeframe_sec", 0) or 0))]
            + [
                int(float(getattr(item_strategy, "candle_timeframe_sec", 0) or 0))
                for _name, item_strategy in worker._multi_strategy_components
            ]
        )
        strategy_candle_timeframes: dict[str, int] = {
            worker._base_strategy_label(): int(
                float(getattr(worker.strategy, "candle_timeframe_sec", 0) or 0)
            ),
        }
        for item_name, item_strategy in worker._multi_strategy_components:
            strategy_candle_timeframes[worker._normalize_strategy_label(item_name)] = int(
                float(getattr(item_strategy, "candle_timeframe_sec", 0) or 0)
            )
        worker._strategy_candle_timeframe_sec_by_name = strategy_candle_timeframes
        history_candle_resolutions: list[int] = []
        for resolution_sec in strategy_candle_timeframes.values():
            if resolution_sec <= 60:
                continue
            if resolution_sec not in history_candle_resolutions:
                history_candle_resolutions.append(int(resolution_sec))
        history_candle_resolutions.sort()
        worker._history_candle_resolutions_sec = tuple(history_candle_resolutions)

    def initialize_history_buffers(
        self,
        *,
        strategy_params: dict[str, object],
    ) -> None:
        worker = self.worker
        history_buffer = max(worker._signal_min_history * 3, 64)
        candle_timeframe_sec = int(worker._signal_candle_timeframe_sec)
        if candle_timeframe_sec > 1 and worker.poll_interval_sec > 0:
            ticks_per_candle = max(1, math.ceil(candle_timeframe_sec / max(worker.poll_interval_sec, FLOAT_COMPARISON_TOLERANCE)))
            min_candle_samples = (worker._signal_min_history + 2) * ticks_per_candle
            history_buffer = max(history_buffer, min_candle_samples * 2)
        history_buffer_cap_raw = worker._strategy_float_param(
            strategy_params,
            "history_buffer_cap",
            20_000.0,
        )
        if not math.isfinite(history_buffer_cap_raw):
            history_buffer_cap_raw = 20_000.0
        history_buffer_cap = max(256, int(history_buffer_cap_raw))
        if history_buffer > history_buffer_cap:
            logger.warning(
                "History buffer capped for %s | requested=%s cap=%s strategy=%s",
                worker.symbol,
                history_buffer,
                history_buffer_cap,
                worker.strategy_name,
            )
            history_buffer = history_buffer_cap
        worker.price_history_keep_rows = max(1000, history_buffer)
        history_restore_rows_raw = worker._strategy_float_param(
            strategy_params,
            "history_restore_rows",
            history_buffer,
        )
        if not math.isfinite(history_restore_rows_raw):
            history_restore_rows_raw = history_buffer
        worker.price_history_restore_rows = max(100, int(history_restore_rows_raw))
        worker.price_history_restore_rows = min(worker.price_history_restore_rows, history_buffer)
        worker._price_history_lock = threading.RLock()
        worker.prices = deque(maxlen=history_buffer)
        worker.price_timestamps = deque(maxlen=history_buffer)
        worker.volumes = deque(maxlen=history_buffer)
        worker._price_samples_seen = 0

    def initialize_runtime_components(
        self,
        *,
        strategy_params: dict[str, object],
    ) -> None:
        worker = self.worker
        worker._parameter_runtime = WorkerParameterRuntime(worker)
        worker._parameter_runtime.initialize_parameter_config(strategy_params=strategy_params)
        worker._recovery_manager = WorkerRecoveryManager(worker)
        worker._health_monitor = WorkerHealthMonitor(worker)
        worker._entry_planner = WorkerEntryPlanner(worker)
        worker._lifecycle_manager = WorkerLifecycleManager(worker)
        worker._data_access_runtime = WorkerDataAccessRuntime(worker)
        worker._symbol_runtime = WorkerSymbolRuntime(worker)
        worker._symbol_spec_runtime = WorkerSymbolSpecRuntime(worker)
        worker._price_runtime = WorkerPriceRuntime(worker)
        worker._price_guard_runtime = WorkerPriceGuardRuntime(worker)
        worker._value_runtime = WorkerValueRuntime(worker)
        worker._execution_quality_runtime = WorkerExecutionQualityRuntime(worker)
        worker._protective_runtime = WorkerProtectiveRuntime(worker)
        worker._dynamic_exit_runtime = WorkerDynamicExitRuntime(worker)
        worker._order_manager = WorkerOrderManager(worker)
        worker._reconcile_manager = WorkerReconcileManager(worker)
        worker._risk_evaluator = WorkerRiskEvaluator(worker)
        worker._last_entry_tick_stale_log_ts = 0.0
        worker._diagnostics_runtime = WorkerDiagnosticsRuntime(worker)
        worker._history_runtime = WorkerHistoryRuntime(worker)
        worker._cycle_runtime = WorkerCycleRuntime(worker)
        worker._orchestrator = WorkerOrchestrator(worker)
        worker._signal_runtime = WorkerSignalRuntime(worker)
        worker._trade_metadata_runtime = WorkerTradeMetadataRuntime(worker)
        worker._trailing_runtime = WorkerTrailingRuntime(worker)
        worker._runtime_state_manager = WorkerRuntimeStateManager(worker)

    def finalize_runtime_startup(
        self,
        *,
        strategy_params: dict[str, object],
    ) -> None:
        worker = self.worker
        worker._restore_price_history()
        worker._log_effective_entry_gates(strategy_params)
        worker._log_strategy_param_hints(strategy_params)
