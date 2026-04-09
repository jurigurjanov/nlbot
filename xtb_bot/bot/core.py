from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import _thread as _thread  # noqa: force stdlib resolution
import faulthandler
import hashlib
import json
import logging
import math
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from xtb_bot.client import BaseBrokerClient, MockBrokerClient, XtbApiClient
from xtb_bot.broker_method_support import call_broker_method_with_supported_kwargs
from xtb_bot.config import (
    DEFAULT_MULTI_STRATEGY_COMPONENT_NAMES,
    BotConfig,
    compact_strategy_params,
)
from xtb_bot.ig_client import IgApiClient
from xtb_bot.ig_proxy import RateLimitedBrokerProxy
from xtb_bot.models import PendingOpen, Position, PriceTick, RunMode, SymbolSpec
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.strategies import create_strategy
from xtb_bot.time_utils import normalize_unix_timestamp_seconds
from xtb_bot.worker import SymbolWorker

from xtb_bot.bot._utils import _TokenBucket, _BoundedTtlCache
from xtb_bot.bot._assignment import (
    WorkerAssignment,
    _freeze_cache_value,
    _strategy_params_signature,
)
from xtb_bot.bot.ig_budget import BotIgBudgetRuntime
from xtb_bot.bot.broker_state import BotBrokerStateRuntime
from xtb_bot.bot.strategy_assignment import BotStrategyAssignmentRuntime
from xtb_bot.bot.stream_ticks import BotStreamTickRuntime
from xtb_bot.bot.db_first_tick import BotDbFirstTickRuntime
from xtb_bot.bot.db_first_spec import BotDbFirstSpecRuntime
from xtb_bot.bot.price_history import BotPriceHistoryRuntime
from xtb_bot.bot.close_details import BotCloseDetailsRuntime
from xtb_bot.bot.trade_metadata import BotTradeMetadataRuntime
from xtb_bot.bot.position_sync import BotPositionSyncRuntime
from xtb_bot.bot.worker_lifecycle import BotWorkerLifecycleRuntime
from xtb_bot.bot.worker_reconcile import BotWorkerReconcileRuntime


logger = logging.getLogger(__name__)

_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"
_MULTI_STRATEGY_BASE_COMPONENT_PARAM = "_multi_strategy_base_component"


class TradingBot:
    _WORKER_WALL_CLOCK_EPOCH_THRESHOLD_SEC = 1_000_000_000.0

    # -- Position sync compat proxies (accessed by tests / close_details runtime) --

    @property
    def _runtime_broker_sync_active_interval_sec(self) -> float:
        return self._position_sync._runtime_broker_sync_active_interval_sec

    @property
    def _runtime_broker_sync_idle_interval_sec(self) -> float:
        return self._position_sync._runtime_broker_sync_idle_interval_sec

    @property
    def _runtime_missing_backfill_interval_sec(self) -> float:
        return self._position_sync._runtime_missing_backfill_interval_sec

    @property
    def _runtime_closed_details_backfill_interval_sec(self) -> float:
        return self._position_sync._runtime_closed_details_backfill_interval_sec

    @property
    def _last_runtime_broker_sync_monotonic(self) -> float:
        return self._position_sync._last_runtime_broker_sync_monotonic

    @_last_runtime_broker_sync_monotonic.setter
    def _last_runtime_broker_sync_monotonic(self, value: float) -> None:
        self._position_sync._last_runtime_broker_sync_monotonic = value

    @property
    def _last_runtime_broker_sync_error_monotonic(self) -> float:
        return self._position_sync._last_runtime_broker_sync_error_monotonic

    @_last_runtime_broker_sync_error_monotonic.setter
    def _last_runtime_broker_sync_error_monotonic(self, value: float) -> None:
        self._position_sync._last_runtime_broker_sync_error_monotonic = value

    @property
    def _last_runtime_missing_backfill_monotonic(self) -> float:
        return self._position_sync._last_runtime_missing_backfill_monotonic

    @_last_runtime_missing_backfill_monotonic.setter
    def _last_runtime_missing_backfill_monotonic(self, value: float) -> None:
        self._position_sync._last_runtime_missing_backfill_monotonic = value

    @property
    def _last_runtime_closed_details_backfill_monotonic(self) -> float:
        return self._position_sync._last_runtime_closed_details_backfill_monotonic

    @_last_runtime_closed_details_backfill_monotonic.setter
    def _last_runtime_closed_details_backfill_monotonic(self, value: float) -> None:
        self._position_sync._last_runtime_closed_details_backfill_monotonic = value

    # -- IG budget compat proxies (accessed by tests) --

    @property
    def _ig_non_trading_budget_reserve_rpm(self) -> float:
        return self._ig_budget.reserve_rpm

    @property
    def _ig_non_trading_budget(self) -> _TokenBucket:
        return self._ig_budget._bucket

    @property
    def _ig_non_trading_budget_blocked_total(self) -> int:
        return self._ig_budget._blocked_total

    @_ig_non_trading_budget_blocked_total.setter
    def _ig_non_trading_budget_blocked_total(self, value: int) -> None:
        self._ig_budget._blocked_total = value

    @property
    def _ig_non_trading_budget_warn_interval_sec(self) -> float:
        return self._ig_budget._warn_interval_sec

    @_ig_non_trading_budget_warn_interval_sec.setter
    def _ig_non_trading_budget_warn_interval_sec(self, value: float) -> None:
        self._ig_budget._warn_interval_sec = value

    @property
    def _last_ig_account_non_trading_snapshot(self) -> dict[str, float | int] | None:
        return self._ig_budget._last_account_non_trading_snapshot

    @_last_ig_account_non_trading_snapshot.setter
    def _last_ig_account_non_trading_snapshot(self, value: dict[str, float | int] | None) -> None:
        self._ig_budget._last_account_non_trading_snapshot = value

    @staticmethod
    def _worker_key(symbol: str | None) -> str:
        return str(symbol or "").strip().upper()

    @classmethod
    def _looks_like_wall_clock_ts(cls, value: object) -> bool:
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return False
        return ts >= cls._WORKER_WALL_CLOCK_EPOCH_THRESHOLD_SEC

    @classmethod
    def _worker_last_saved_state_age_sec(
        cls,
        worker: object,
        *,
        now_wall: float,
        now_monotonic: float,
    ) -> float | None:
        last_saved_monotonic = float(getattr(worker, "_last_saved_worker_state_monotonic", 0.0) or 0.0)
        if last_saved_monotonic > 0.0:
            return max(0.0, now_monotonic - last_saved_monotonic)

        last_saved_ts = float(getattr(worker, "_last_saved_worker_state_ts", 0.0) or 0.0)
        if last_saved_ts <= 0.0:
            return None
        if cls._looks_like_wall_clock_ts(last_saved_ts):
            return max(0.0, now_wall - last_saved_ts)
        return max(0.0, now_monotonic - last_saved_ts)

    def __init__(self, config: BotConfig):
        self.config = config
        db_timeout_sec = self._env_float("XTB_DB_SQLITE_TIMEOUT_SEC", 60.0)
        db_cleanup_every = self._env_int("XTB_DB_PRICE_HISTORY_CLEANUP_EVERY", 100)
        db_cleanup_min_interval_sec = self._env_float("XTB_DB_PRICE_HISTORY_CLEANUP_MIN_INTERVAL_SEC", 30.0)
        db_housekeeping_interval_sec = self._env_float("XTB_DB_HOUSEKEEPING_INTERVAL_SEC", 300.0)
        db_housekeeping_events_keep_rows = self._env_int("XTB_DB_HOUSEKEEPING_EVENTS_KEEP_ROWS", 50_000)
        db_housekeeping_vacuum_pages = self._env_int("XTB_DB_HOUSEKEEPING_VACUUM_PAGES", 256)
        db_position_updates_retention_sec = self._env_float(
            "XTB_DB_POSITION_UPDATES_RETENTION_SEC",
            7.0 * 24.0 * 60.0 * 60.0,
        )

        self.store = StateStore(
            config.storage_path,
            sqlite_timeout_sec=db_timeout_sec,
            price_history_cleanup_every=db_cleanup_every,
            price_history_cleanup_min_interval_sec=db_cleanup_min_interval_sec,
            housekeeping_interval_sec=db_housekeeping_interval_sec,
            housekeeping_events_keep_rows=db_housekeeping_events_keep_rows,
            housekeeping_incremental_vacuum_pages=db_housekeeping_vacuum_pages,
            housekeeping_position_updates_retention_sec=db_position_updates_retention_sec,
        )
        self.position_book = PositionBook()
        self.stop_event = threading.Event()
        self.risk = RiskManager(config.risk, self.store)
        self._workers_lock = threading.RLock()
        self._worker_lifecycle_lock = threading.RLock()
        self.workers: dict[str, SymbolWorker] = {}
        self._worker_stop_events: dict[str, threading.Event] = {}
        self._worker_assignments: dict[str, WorkerAssignment] = {}
        self._worker_lease_id_by_symbol: dict[str, str] = {}
        self._worker_health_stop_event = threading.Event()
        self._worker_health_thread: threading.Thread | None = None
        self._worker_health_interval_sec = 10.0
        self._worker_stale_heartbeat_restart_cooldown_sec = 60.0
        self._worker_last_restart_monotonic_by_symbol: dict[str, float] = {}
        now_monotonic = time.monotonic()
        self._runtime_monitor_last_started_monotonic = now_monotonic
        self._runtime_monitor_last_completed_monotonic = now_monotonic
        self._runtime_monitor_last_progress_monotonic = now_monotonic
        self._runtime_monitor_watchdog_enabled = False
        self._runtime_monitor_stall_first_detected_monotonic = 0.0
        self._runtime_monitor_last_failure_log_monotonic = 0.0
        self._runtime_monitor_last_failure_signature: tuple[str, str, str] | None = None
        self._runtime_monitor_active_task_name: str | None = None
        self._deferred_switch_signature_by_symbol: dict[str, tuple[str, str] | None] = {}
        self._resources_closed = False
        self._shutdown_state_lock = threading.Lock()
        self._shutdown_requested_reason: str | None = None
        self._shutdown_requested_source: str | None = None
        self._shutdown_requested_ts = 0.0
        self._shutdown_request_event_recorded = False
        self._shutdown_completed_event_recorded = False
        self._stream_ticks = BotStreamTickRuntime(self)
        self.bot_magic_prefix = config.bot_magic_prefix
        self.bot_magic_instance = self._resolve_bot_magic_instance(config.bot_magic_instance)
        self._strategy_assignment = BotStrategyAssignmentRuntime(self)
        reconcile_params = self._strategy_params_for(config.strategy)
        self._close_reconcile_enabled = self._as_bool_param(
            reconcile_params.get("close_reconcile_enabled"),
            True,
        )
        self._close_reconcile_pnl_alert_threshold = max(
            0.0,
            self._as_float_param(
                reconcile_params.get("close_reconcile_pnl_alert_threshold"),
                0.5,
            ),
        )
        self._close_reconcile_recent_window_sec = max(
            60.0,
            self._as_float_param(
                reconcile_params.get("close_reconcile_recent_window_sec"),
                4.0 * 60.0 * 60.0,
            ),
        )
        self._close_reconcile_max_passes = max(
            1,
            int(
                self._as_float_param(
                    reconcile_params.get("close_reconcile_max_passes"),
                    3.0,
                )
            ),
        )
        self._schedule_timezone = ZoneInfo(config.strategy_schedule_timezone)
        if self.config.strategy_schedule and self._schedule_disabled_by_multi_strategy():
            logger.info(
                "Strategy schedule is disabled because multi-strategy mode is enabled | strategy=%s slots=%d",
                self.config.strategy,
                len(self.config.strategy_schedule),
            )
        self._price_history = BotPriceHistoryRuntime(self)
        self._position_sync = BotPositionSyncRuntime(self)
        self._worker_lifecycle = BotWorkerLifecycleRuntime(self)
        self._worker_reconcile = BotWorkerReconcileRuntime(self)
        self._close_details = BotCloseDetailsRuntime(self)
        self._ig_budget = BotIgBudgetRuntime(self)
        self._broker_state = BotBrokerStateRuntime(self)
        self._last_runtime_monitor_noncritical_deferral_monotonic = 0.0
        self._last_db_housekeeping_error_monotonic = 0.0
        self._trade_metadata = BotTradeMetadataRuntime(self)
        # passive_history state is now on self._price_history
        self._last_runtime_deferred_startup_tasks_monotonic = 0.0
        self._last_runtime_deferred_startup_error_monotonic = 0.0
        self._db_first_reads_enabled = self._env_bool(
            "XTB_DB_FIRST_READS_ENABLED",
            config.broker == "ig",
        )
        self._db_first_cache_stop_event = threading.Event()
        self._db_first_cache_threads: dict[str, threading.Thread] = {}
        self._db_first_tick = BotDbFirstTickRuntime(self)
        self._db_first_spec = BotDbFirstSpecRuntime(self)
        default_db_first_account_poll_interval_sec = 3.0
        if self._ig_budget.enabled and self.config.broker == "ig":
            reserve_rpm = max(1.0, self._ig_budget.reserve_rpm)
            default_db_first_account_poll_interval_sec = max(
                3.0,
                60.0 / reserve_rpm,
            )
        self._db_first_account_snapshot_poll_interval_sec = max(
            1.0,
            self._env_float(
                "XTB_DB_FIRST_ACCOUNT_POLL_INTERVAL_SEC",
                default_db_first_account_poll_interval_sec,
            ),
        )
        self._db_first_account_snapshot_last_success_ts = 0.0
        self._db_first_account_snapshot_stale_warn_last_ts = 0.0
        self._db_first_history_poll_interval_sec = max(
            1.0,
            self._env_float("XTB_DB_FIRST_HISTORY_POLL_INTERVAL_SEC", self._passive_history_poll_interval_sec),
        )
        # db_first_tick error/reconnect state is now on self._db_first_tick
        self._db_first_loop_backoff_sec_by_name: dict[str, float] = {}
        self._db_first_loop_retry_after_monotonic_by_name: dict[str, float] = {}
        # _db_first_symbol_spec retry state is now on self._db_first_spec
        self._db_first_prime_enabled = self._env_bool("XTB_DB_FIRST_PRIME_ENABLED", False)
        self._db_first_prime_max_symbols = max(0, self._env_int("XTB_DB_FIRST_PRIME_MAX_SYMBOLS", 0))
        self._db_first_prime_account_enabled = self._env_bool("XTB_DB_FIRST_PRIME_ACCOUNT_ENABLED", False)
        # history prefetch state is now on self._price_history
        fast_startup_enabled = self.config.mode == RunMode.EXECUTION and self._db_first_reads_enabled and self.config.broker == "ig"
        self._runtime_deferred_symbol_spec_preload_completed = not fast_startup_enabled
        self._runtime_deferred_history_prewarm_completed = not (fast_startup_enabled and self._history_prefetch_enabled)
        self._runtime_deferred_startup_completion_recorded = not fast_startup_enabled
        self._ig_proxy_enabled = self._env_bool("XTB_IG_PROXY_ENABLED", False)

        if config.broker == "ig":
            broker: BaseBrokerClient = IgApiClient(
                identifier=config.user_id,
                password=config.password,
                api_key=str(config.api_key or ""),
                account_type=config.account_type,
                account_id=config.account_id,
                endpoint=config.endpoint,
                symbol_epics=config.symbol_epics,
                stream_enabled=config.ig_stream_enabled,
                stream_tick_max_age_sec=config.ig_stream_tick_max_age_sec,
                rest_market_min_interval_sec=config.ig_rest_market_min_interval_sec,
                position_update_callback=self.store.record_position_update,
            )
            if self._ig_proxy_enabled:
                symbols = [str(symbol).strip().upper() for symbol in config.symbols if str(symbol).strip()]
                broker = RateLimitedBrokerProxy(
                    broker,
                    symbols=symbols,
                    stop_event=self.stop_event,
                )
                logger.info("IG proxy broker enabled | symbols=%s", len(symbols))
            self.broker = broker
        else:
            self.broker = XtbApiClient(
                user_id=config.user_id,
                password=config.password,
                app_name=config.app_name,
                account_type=config.account_type,
                endpoint=config.endpoint,
            )
        self._register_stream_tick_persistence_hook()
        self._bootstrap_symbol_history_requirements()

    def _resolve_bot_magic_instance(self, configured: str | None) -> str:
        if configured:
            return configured
        stored = self.store.get_kv("bot.magic_instance")
        if stored:
            return stored

        if self.config.mode == RunMode.EXECUTION:
            seed_parts = [
                str(self.config.broker).strip().lower(),
                str(self.config.account_type.value).strip().lower(),
                str(self.config.account_id or "").strip().lower(),
                str(self.config.user_id or "").strip().lower(),
                str(self.config.app_name or "").strip().lower(),
            ]
            seed = "|".join(seed_parts)
            digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()
            generated = digest[:8]
            logger.warning(
                "bot_magic_instance is not configured, using deterministic fallback=%s; "
                "set XTB_BOT_MAGIC_INSTANCE/IG_BOT_MAGIC_INSTANCE explicitly for multi-instance safety",
                generated,
            )
        else:
            generated = uuid.uuid4().hex[:8]
        self.store.set_kv("bot.magic_instance", generated)
        return generated

    def _connect_broker(self) -> None:
        try:
            self.broker.connect()
            logger.info("Connected to broker=%s", self.config.broker)
        except Exception as exc:
            if self.config.mode == RunMode.EXECUTION:
                logger.error(
                    "Broker=%s connect failed in execution mode; refusing mock broker fallback: %s",
                    self.config.broker,
                    exc,
                )
                raise
            if self.config.strict_broker_connect:
                logger.error(
                    "Broker=%s connect failed in strict mode for %s: %s",
                    self.config.broker,
                    self.config.mode.value,
                    exc,
                )
                raise
            logger.warning(
                "Broker=%s unavailable for %s mode, using mock broker fallback: %s",
                self.config.broker,
                self.config.mode.value,
                exc,
            )
            self.broker = MockBrokerClient(start_balance=self.config.risk.start_balance)
            self.broker.connect()

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        return TradingBot._as_bool_param(os.getenv(name), default)

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        return TradingBot._as_float_param(os.getenv(name, str(default)), default)

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name, str(default))
        try:
            parsed = int(float(raw))
        except (TypeError, ValueError):
            return int(default)
        return int(parsed)

    @staticmethod
    def _as_bool_param(raw: object, default: bool) -> bool:
        if raw is None:
            return bool(default)
        if isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
        return bool(raw)

    @staticmethod
    def _as_float_param(raw: object, default: float) -> float:
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            return float(default)
        if not math.isfinite(parsed):
            return float(default)
        return parsed

    @staticmethod
    def _as_symbol_list_param(raw: object) -> list[str]:
        if raw in (None, ""):
            return []
        payload = raw
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return []
            if text.startswith("[") and text.endswith("]"):
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    payload = [item.strip() for item in text.split(",") if item.strip()]
            else:
                payload = [item.strip() for item in text.split(",") if item.strip()]
        if isinstance(payload, (list, tuple, set)):
            items = payload
        else:
            return []
        result: list[str] = []
        seen: set[str] = set()
        for item in items:
            symbol = str(item or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            result.append(symbol)
        return result

    # -- Strategy & Assignment compat proxies (accessed by tests) --

    @property
    def _strategy_symbol_filter(self):
        return self._strategy_assignment._strategy_symbol_filter

    @_strategy_symbol_filter.setter
    def _strategy_symbol_filter(self, value):
        self._strategy_assignment._strategy_symbol_filter = value

    @property
    def _multi_strategy_rollout_mode_override_by_symbol(self):
        return self._strategy_assignment._multi_strategy_rollout_mode_override_by_symbol

    @_multi_strategy_rollout_mode_override_by_symbol.setter
    def _multi_strategy_rollout_mode_override_by_symbol(self, value):
        self._strategy_assignment._multi_strategy_rollout_mode_override_by_symbol = value

    @property
    def _strategy_support_cache(self):
        return self._strategy_assignment._strategy_support_cache

    @staticmethod
    def _strategy_cache_identity(
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[str, tuple[tuple[str, object], ...]]:
        return BotStrategyAssignmentRuntime._strategy_cache_identity(strategy_name, strategy_params)

    def _strategy_params_for(self, strategy_name: str) -> dict[str, object]:
        return self._strategy_assignment._strategy_params_for(strategy_name)

    def _multi_strategy_carrier_params(
        self,
        *,
        base_strategy_name: str,
        strategy_params: dict[str, object],
    ) -> dict[str, object]:
        return self._strategy_assignment._multi_strategy_carrier_params(
            base_strategy_name=base_strategy_name,
            strategy_params=strategy_params,
        )

    def _worker_assignment_payload(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[str, dict[str, object]]:
        return self._strategy_assignment._worker_assignment_payload(strategy_name, strategy_params)

    @staticmethod
    def _normalize_strategy_label(value: object) -> str | None:
        return BotStrategyAssignmentRuntime._normalize_strategy_label(value)

    def _strategy_base_label(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
    ) -> str | None:
        return self._strategy_assignment._strategy_base_label(
            strategy_name, strategy_params, strategy_entry_hint=strategy_entry_hint,
        )

    def _strategy_labels(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
    ) -> tuple[str | None, str | None]:
        return self._strategy_assignment._strategy_labels(
            strategy_name, strategy_params, strategy_entry_hint=strategy_entry_hint,
        )

    def _assignment_strategy_labels(self, assignment: WorkerAssignment | None) -> tuple[str | None, str | None]:
        return self._strategy_assignment._assignment_strategy_labels(assignment)

    def _strategy_event_payload(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
        strategy_key: str = "strategy",
        base_key: str = "strategy_base",
    ) -> dict[str, object]:
        return self._strategy_assignment._strategy_event_payload(
            strategy_name, strategy_params,
            strategy_entry_hint=strategy_entry_hint,
            strategy_key=strategy_key,
            base_key=base_key,
        )

    def _default_strategy_entry_for_assignment(self, assignment: WorkerAssignment | None) -> str | None:
        return self._strategy_assignment._default_strategy_entry_for_assignment(assignment)

    def _apply_position_trade_identity(
        self,
        position: Position,
        *,
        strategy: object,
        strategy_entry: object | None = None,
        strategy_entry_component: object | None = None,
        strategy_entry_signal: object | None = None,
    ) -> None:
        self._strategy_assignment._apply_position_trade_identity(
            position,
            strategy=strategy,
            strategy_entry=strategy_entry,
            strategy_entry_component=strategy_entry_component,
            strategy_entry_signal=strategy_entry_signal,
        )

    def _resolved_recovery_trade_identity(
        self,
        *,
        symbol: str,
        existing_row: dict[str, object] | None,
        matched_pending: PendingOpen | None,
        default_assignment: WorkerAssignment | None,
    ) -> tuple[str, str, str, str | None, str | None, str]:
        return self._strategy_assignment._resolved_recovery_trade_identity(
            symbol=symbol,
            existing_row=existing_row,
            matched_pending=matched_pending,
            default_assignment=default_assignment,
        )

    @staticmethod
    def _multi_strategy_enabled_for_params(strategy_params: dict[str, object]) -> bool:
        return BotStrategyAssignmentRuntime._multi_strategy_enabled_for_params(strategy_params)

    @staticmethod
    def _parse_strategy_names(raw: object) -> list[str]:
        return BotStrategyAssignmentRuntime._parse_strategy_names(raw)

    def _resolve_multi_strategy_component_names(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> list[str]:
        return self._strategy_assignment._resolve_multi_strategy_component_names(strategy_name, strategy_params)

    def _strategy_supports_symbol(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
        symbol: str,
    ) -> bool:
        return self._strategy_assignment._strategy_supports_symbol(strategy_name, strategy_params, symbol)

    def _schedule_disabled_by_multi_strategy(self) -> bool:
        return self._strategy_assignment._schedule_disabled_by_multi_strategy()

    def _resolve_multi_strategy_rollout_mode_overrides(self) -> dict[str, RunMode]:
        return self._strategy_assignment._resolve_multi_strategy_rollout_mode_overrides()

    def _mode_override_for_symbol(self, symbol: str) -> RunMode | None:
        return self._strategy_assignment._mode_override_for_symbol(symbol)

    def _db_first_loop_backoff_remaining_sec(
        self,
        loop_name: str,
        *,
        now_monotonic: float | None = None,
    ) -> float:
        current = time.monotonic() if now_monotonic is None else float(now_monotonic)
        retry_after = float(self._db_first_loop_retry_after_monotonic_by_name.get(loop_name, 0.0))
        return max(0.0, retry_after - current)

    def _record_db_first_loop_failure(
        self,
        loop_name: str,
        *,
        base_interval_sec: float,
        max_backoff_sec: float = 60.0,
        now_monotonic: float | None = None,
    ) -> float:
        current = time.monotonic() if now_monotonic is None else float(now_monotonic)
        prior = float(self._db_first_loop_backoff_sec_by_name.get(loop_name, 0.0))
        base = max(float(base_interval_sec), 0.5)
        next_backoff = min(
            max(float(max_backoff_sec), base),
            max(base, (prior * 2.0) if prior > 0.0 else base),
        )
        self._db_first_loop_backoff_sec_by_name[loop_name] = next_backoff
        self._db_first_loop_retry_after_monotonic_by_name[loop_name] = current + next_backoff
        return next_backoff

    def _clear_db_first_loop_backoff(self, loop_name: str) -> None:
        self._db_first_loop_backoff_sec_by_name.pop(loop_name, None)
        self._db_first_loop_retry_after_monotonic_by_name.pop(loop_name, None)

    def _static_assignments(self) -> dict[str, WorkerAssignment]:
        return self._worker_reconcile._static_assignments()

    @staticmethod
    def _worker_lease_key(symbol: str) -> str:
        return BotWorkerLifecycleRuntime._worker_lease_key(symbol)

    def _acquire_worker_lease(self, symbol: str, lease_id: str) -> None:
        self._worker_lifecycle._acquire_worker_lease(symbol, lease_id)

    def _revoke_worker_lease(self, symbol: str) -> None:
        self._worker_lifecycle._revoke_worker_lease(symbol)

    def _make_worker(self, assignment: WorkerAssignment, stop_event: threading.Event) -> SymbolWorker:
        return self._worker_lifecycle._make_worker(assignment, stop_event)

    # -- DB-First Tick Caching compat proxies --

    @property
    def _db_first_tick_target_rpm(self):
        return self._db_first_tick._db_first_tick_target_rpm

    @_db_first_tick_target_rpm.setter
    def _db_first_tick_target_rpm(self, value):
        self._db_first_tick._db_first_tick_target_rpm = value

    @property
    def _db_first_tick_priority_symbols(self):
        return self._db_first_tick._db_first_tick_priority_symbols

    @_db_first_tick_priority_symbols.setter
    def _db_first_tick_priority_symbols(self, value):
        self._db_first_tick._db_first_tick_priority_symbols = value

    @property
    def _db_first_tick_passive_every_n_active(self):
        return self._db_first_tick._db_first_tick_passive_every_n_active

    @_db_first_tick_passive_every_n_active.setter
    def _db_first_tick_passive_every_n_active(self, value):
        self._db_first_tick._db_first_tick_passive_every_n_active = value

    @property
    def _db_first_disconnect_error_log_interval_sec(self):
        return self._db_first_tick._db_first_disconnect_error_log_interval_sec

    @_db_first_disconnect_error_log_interval_sec.setter
    def _db_first_disconnect_error_log_interval_sec(self, value):
        self._db_first_tick._db_first_disconnect_error_log_interval_sec = value

    def _db_first_tick_request_interval_sec(self) -> float:
        return self._db_first_tick._db_first_tick_request_interval_sec()

    def _db_first_tick_effective_hard_max_age_sec(self, active_symbol_count: int | None = None) -> float:
        return self._db_first_tick._db_first_tick_effective_hard_max_age_sec(active_symbol_count)

    def _db_first_tick_max_age_for_workers(self) -> float:
        return self._db_first_tick._db_first_tick_max_age_for_workers()

    def _estimated_active_db_first_symbol_count(self) -> int:
        return self._db_first_tick._estimated_active_db_first_symbol_count()

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _register_stream_tick_persistence_hook(self) -> None:
        self._stream_ticks._register_stream_tick_persistence_hook()

    def _update_latest_tick_from_broker(self, tick: PriceTick) -> None:
        self._stream_ticks._update_latest_tick_from_broker(tick)

    def _load_latest_tick_from_memory_cache(self, symbol: str, max_age_sec: float) -> PriceTick | None:
        return self._stream_ticks._load_latest_tick_from_memory_cache(symbol, max_age_sec)

    def _persist_stream_tick_from_broker(self, tick: PriceTick) -> None:
        self._stream_ticks._persist_stream_tick_from_broker(tick)

    @staticmethod
    def _normalize_timestamp_seconds(raw_ts: float | int | str | None) -> float:
        return normalize_unix_timestamp_seconds(raw_ts)

    def _drain_runtime_for_watchdog_abort(self, *, worker_join_budget_sec: float = 3.0) -> None:
        with self._workers_lock:
            stop_events = list(self._worker_stop_events.values())
            worker_items = list(self.workers.items())
        for stop_event in stop_events:
            stop_event.set()
        self._stop_db_first_cache_workers()

        deadline = time.monotonic() + max(0.1, float(worker_join_budget_sec))
        alive_symbols: list[str] = []
        for symbol, worker in worker_items:
            remaining = deadline - time.monotonic()
            if remaining > 0.0 and worker.is_alive():
                worker.join(timeout=min(0.5, remaining))
            if worker.is_alive():
                alive_symbols.append(str(symbol).strip().upper())
        if alive_symbols:
            logger.warning(
                "Watchdog abort proceeding with workers still alive: %s",
                ",".join(sorted(alive_symbols)),
            )

        for flush_name in ("flush_multi_async_writes", "flush_event_async_writes"):
            flush = getattr(self.store, flush_name, None)
            if not callable(flush):
                continue
            try:
                flushed = bool(flush(timeout_sec=2.0))
                if not flushed:
                    logger.warning("Watchdog abort timed out while draining %s", flush_name)
            except Exception:
                logger.debug("Failed to drain %s during watchdog abort", flush_name, exc_info=True)

    # -- Price History compat proxies --

    @property
    def _history_keep_rows_min(self):
        return self._price_history._history_keep_rows_min

    @property
    def _history_keep_rows_cap(self):
        return self._price_history._history_keep_rows_cap

    @property
    def _history_prefetch_enabled(self):
        return self._price_history._history_prefetch_enabled

    @property
    def _history_prefetch_points(self):
        return self._price_history._history_prefetch_points

    @property
    def _history_prefetch_resolution(self):
        return self._price_history._history_prefetch_resolution

    @property
    def _history_prefetch_skip_ratio(self):
        return self._price_history._history_prefetch_skip_ratio

    @property
    def _passive_history_poll_interval_sec(self):
        return self._price_history._passive_history_poll_interval_sec

    @property
    def _passive_history_max_symbols_per_cycle(self):
        return self._price_history._passive_history_max_symbols_per_cycle

    @property
    def _passive_history_refresh_lock(self):
        return self._price_history._passive_history_refresh_lock

    def _estimate_history_keep_rows(self, strategy_name: str, strategy_params: dict[str, object]) -> int:
        return self._price_history._estimate_history_keep_rows(strategy_name, strategy_params)

    @staticmethod
    def _history_resolution_seconds(resolution: str) -> int | None:
        return BotPriceHistoryRuntime._history_resolution_seconds(resolution)

    def _estimate_history_prefetch_specs(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[tuple[str, int], ...]:
        return self._price_history._estimate_history_prefetch_specs(strategy_name, strategy_params)

    def _register_symbol_history_requirement(
        self,
        symbol: str,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> int:
        return self._price_history._register_symbol_history_requirement(symbol, strategy_name, strategy_params)

    def _bootstrap_symbol_history_requirements(self) -> None:
        self._price_history._bootstrap_symbol_history_requirements()

    def _price_history_keep_rows_for_symbol(self, symbol: str) -> int:
        return self._price_history._price_history_keep_rows_for_symbol(symbol)

    def _history_prefetch_plan_for_symbol(self, symbol: str) -> list[tuple[str, int]]:
        return self._price_history._history_prefetch_plan_for_symbol(symbol)

    def _candle_history_resolutions_for_symbol(self, symbol: str) -> tuple[int, ...]:
        return self._price_history._candle_history_resolutions_for_symbol(symbol)

    def _history_prefetch_has_time_coverage(
        self,
        symbol: str,
        *,
        resolution: str,
        target_points: int,
    ) -> bool:
        return self._price_history._history_prefetch_has_time_coverage(symbol, resolution=resolution, target_points=target_points)

    def _is_schedule_entry_active(self, now_utc: datetime, entry) -> bool:
        return self._worker_reconcile._is_schedule_entry_active(now_utc, entry)

    def _schedule_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        return self._worker_reconcile._schedule_assignments(now_utc)

    def _assignment_for_open_position(
        self,
        position: Position,
        fallback: WorkerAssignment | None = None,
    ) -> WorkerAssignment:
        return self._worker_reconcile._assignment_for_open_position(position, fallback)

    def _target_worker_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        return self._worker_reconcile._target_worker_assignments(now_utc)

    def _start_worker_for_assignment(self, assignment: WorkerAssignment) -> None:
        self._worker_lifecycle._start_worker_for_assignment(assignment)

    def _stop_worker_for_symbol(self, symbol: str, reason: str) -> None:
        self._worker_lifecycle._stop_worker_for_symbol(symbol, reason)

    def _record_deferred_switch(self, symbol: str, current: WorkerAssignment, desired: WorkerAssignment) -> None:
        self._worker_lifecycle._record_deferred_switch(symbol, current, desired)

    def _reconcile_workers(self, now_utc: datetime | None = None) -> None:
        self._worker_reconcile._reconcile_workers(now_utc)

    def _start_workers(self) -> None:
        self._worker_reconcile._start_workers()

    def _runtime_monitor_interval_sec(self) -> float:
        return max(
            1.0,
            min(
                float(self.config.poll_interval_sec),
                float(self.config.passive_history_poll_interval_sec),
            ),
        )

    def _runtime_monitor_stale_after_sec(self) -> float:
        return max(180.0, self._runtime_monitor_interval_sec() * 12.0)

    def _worker_stale_heartbeat_after_sec(self, worker: SymbolWorker) -> float:
        poll_interval_sec = max(1.0, float(getattr(worker, "poll_interval_sec", self.config.poll_interval_sec)))
        flush_interval_sec = max(
            0.5,
            float(getattr(worker, "worker_state_flush_interval_sec", self.config.risk.worker_state_flush_interval_sec)),
        )
        return max(30.0, max(poll_interval_sec, flush_interval_sec, self._worker_health_interval_sec) * 6.0)

    @staticmethod
    def _worker_watchdog_blocking_operation_payload(
        worker: SymbolWorker,
        *,
        now_wall: float,
    ) -> dict[str, object] | None:
        getter = getattr(worker, "watchdog_blocking_operation_status", None)
        raw_status = getter() if callable(getter) else None
        if not isinstance(raw_status, dict):
            return None
        operation = str(raw_status.get("operation") or "").strip()
        started_at = float(raw_status.get("started_at") or 0.0)
        started_at_monotonic = float(raw_status.get("started_at_monotonic") or 0.0)
        grace_sec = max(0.0, float(raw_status.get("grace_sec") or 0.0))
        if not operation or grace_sec <= 0.0:
            return None
        if started_at_monotonic > 0.0:
            age_sec = max(0.0, time.monotonic() - started_at_monotonic)
        else:
            if started_at <= 0.0:
                return None
            age_sec = max(0.0, now_wall - started_at)
        remaining_sec = max(0.0, grace_sec - age_sec)
        return {
            "blocking_operation": operation,
            "blocking_operation_age_sec": round(age_sec, 3),
            "blocking_operation_grace_sec": round(grace_sec, 3),
            "blocking_operation_remaining_sec": round(remaining_sec, 3),
        }

    def _record_runtime_monitor_failure(self, task_name: str, exc: Exception) -> None:
        now_monotonic = time.monotonic()
        signature = (str(task_name), exc.__class__.__name__, str(exc))
        should_log = (
            signature != self._runtime_monitor_last_failure_signature
            or (now_monotonic - self._runtime_monitor_last_failure_log_monotonic) >= 30.0
        )
        if should_log:
            logger.exception("Runtime monitor task failed | task=%s error=%s", task_name, exc)
            try:
                self.store.record_event(
                    "ERROR",
                    None,
                    "Runtime monitor task failed",
                    {
                        "task": str(task_name),
                        "error": str(exc),
                        "error_type": exc.__class__.__name__,
                    },
                )
            except Exception:
                logger.debug("Failed to persist runtime monitor failure event", exc_info=True)
            self._runtime_monitor_last_failure_log_monotonic = now_monotonic
            self._runtime_monitor_last_failure_signature = signature

    def _run_monitor_task(self, task_name: str, operation: Callable[[], None]) -> None:
        self._runtime_monitor_active_task_name = str(task_name)
        self._runtime_monitor_last_progress_monotonic = time.monotonic()
        try:
            operation()
        except Exception as exc:
            self._record_runtime_monitor_failure(task_name, exc)
        finally:
            self._runtime_monitor_last_progress_monotonic = time.monotonic()
            self._runtime_monitor_active_task_name = None

    def _pulse_runtime_monitor_progress(self) -> None:
        self._runtime_monitor_last_progress_monotonic = time.monotonic()

    def _restart_worker_for_symbol(self, symbol: str, *, reason: str) -> bool:
        with self._worker_lifecycle_lock:
            normalized_symbol = str(symbol).strip().upper()
            now_monotonic = time.monotonic()
            last_restart = self._worker_last_restart_monotonic_by_symbol.get(normalized_symbol, 0.0)
            if (
                last_restart > 0.0
                and (now_monotonic - last_restart) < self._worker_stale_heartbeat_restart_cooldown_sec
            ):
                return False

            desired = self._target_worker_assignments()
            restart_assignment = desired.get(normalized_symbol)
            active_position = self.position_book.get(normalized_symbol)
            if active_position is not None:
                restart_assignment = self._assignment_for_open_position(active_position, restart_assignment)

            self._worker_last_restart_monotonic_by_symbol[normalized_symbol] = now_monotonic
            self._stop_worker_for_symbol(normalized_symbol, reason)
            if restart_assignment is None:
                return False
            self._start_worker_for_assignment(restart_assignment)
            return True

    def _worker_health_check_once(self) -> list[str]:
        with self._workers_lock:
            dead_symbols = sorted(
                {
                    str(symbol).strip().upper()
                    for symbol, worker in self.workers.items()
                    if worker is not None and (not worker.is_alive())
                }
            )
        if not dead_symbols:
            return []
        logger.warning(
            "Worker health watchdog detected stopped workers | count=%d symbols=%s",
            len(dead_symbols),
            ",".join(dead_symbols),
        )
        self.store.record_event(
            "WARN",
            None,
            "Worker health watchdog detected stopped workers",
            {
                "count": len(dead_symbols),
                "symbols": dead_symbols,
            },
        )
        self._reconcile_workers()
        return dead_symbols

    def _worker_stale_heartbeat_check_once(self) -> list[str]:
        now_wall = time.time()
        now_monotonic = time.monotonic()
        stale_symbols: list[str] = []
        with self._workers_lock:
            worker_items = list(self.workers.items())
        for symbol, worker in worker_items:
            if worker is None or (not worker.is_alive()):
                continue
            heartbeat_age_sec = self._worker_last_saved_state_age_sec(
                worker,
                now_wall=now_wall,
                now_monotonic=now_monotonic,
            )
            if heartbeat_age_sec is None:
                continue
            stale_after_sec = self._worker_stale_heartbeat_after_sec(worker)
            if heartbeat_age_sec < stale_after_sec:
                continue
            blocking_operation = self._worker_watchdog_blocking_operation_payload(
                worker,
                now_wall=now_wall,
            )
            if (
                blocking_operation is not None
                and float(blocking_operation.get("blocking_operation_remaining_sec") or 0.0) > 0.0
            ):
                continue
            normalized_symbol = str(symbol).strip().upper()
            stale_symbols.append(normalized_symbol)
            last_signature = getattr(worker, "_last_saved_worker_state_signature", None)
            last_error = None
            if isinstance(last_signature, tuple) and len(last_signature) >= 2:
                raw_last_error = last_signature[1]
                if raw_last_error not in (None, ""):
                    last_error = str(raw_last_error)
            with self._workers_lock:
                assignment = self._worker_assignments.get(normalized_symbol)
            payload = {
                "symbol": normalized_symbol,
                "heartbeat_age_sec": round(heartbeat_age_sec, 3),
                "stale_after_sec": round(stale_after_sec, 3),
                "thread_name": getattr(worker, "name", None),
                "last_error": last_error,
            }
            if blocking_operation is not None:
                payload.update(blocking_operation)
            payload.update(
                self._strategy_event_payload(
                    getattr(worker, "strategy_name", None),
                    (
                        getattr(assignment, "strategy_params", None)
                        if assignment is not None
                        else None
                    ),
                    strategy_entry_hint=getattr(worker, "_multi_strategy_base_component_name", None),
                )
            )
            logger.error(
                "Worker health watchdog detected stale heartbeat | symbol=%s age=%.1fs stale_after=%.1fs",
                normalized_symbol,
                heartbeat_age_sec,
                stale_after_sec,
            )
            self.store.record_event(
                "ERROR",
                normalized_symbol,
                "Worker health watchdog detected stale heartbeat",
                payload,
            )
            self._restart_worker_for_symbol(normalized_symbol, reason="watchdog_stale_heartbeat")
        return stale_symbols

    def _dump_runtime_thread_traces(self, *, reason: str) -> None:
        try:
            logger.error("Dumping Python thread traces | reason=%s", reason)
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        except Exception:
            logger.exception("Failed to dump Python thread traces | reason=%s", reason)

    def _abort_process_for_watchdog(self, *, reason: str) -> None:
        try:
            self.store.record_event(
                "ERROR",
                None,
                "Runtime watchdog aborting process",
                {"reason": str(reason)},
            )
        except Exception:
            logger.debug("Failed to persist watchdog abort event", exc_info=True)
        self.request_graceful_stop(
            reason=f"watchdog_abort:{str(reason).strip() or 'unknown'}",
            source="runtime_watchdog",
        )
        self._worker_health_stop_event.set()
        try:
            self._drain_runtime_for_watchdog_abort()
        except Exception:
            logger.debug("Failed to drain runtime before watchdog abort", exc_info=True)
        logger.critical("Runtime watchdog aborting process | reason=%s", reason)
        try:
            _thread.interrupt_main()
        except Exception:
            logger.debug("Failed to interrupt main thread during watchdog abort", exc_info=True)
        raise SystemExit(1)

    def _monitor_loop_health_check_once(self) -> bool:
        if not self._runtime_monitor_watchdog_enabled:
            return False
        now_monotonic = time.monotonic()
        stale_after_sec = self._runtime_monitor_stale_after_sec()
        last_progress_monotonic = max(
            self._runtime_monitor_last_started_monotonic,
            self._runtime_monitor_last_completed_monotonic,
            self._runtime_monitor_last_progress_monotonic,
        )
        age_sec = max(0.0, now_monotonic - last_progress_monotonic)
        if age_sec < stale_after_sec:
            self._runtime_monitor_stall_first_detected_monotonic = 0.0
            return False

        if self._runtime_monitor_stall_first_detected_monotonic <= 0.0:
            self._runtime_monitor_stall_first_detected_monotonic = now_monotonic
            payload = {
                "stale_after_sec": round(stale_after_sec, 3),
                "age_sec": round(age_sec, 3),
                "last_started_age_sec": round(
                    max(0.0, now_monotonic - self._runtime_monitor_last_started_monotonic),
                    3,
                ),
                "last_completed_age_sec": round(
                    max(0.0, now_monotonic - self._runtime_monitor_last_completed_monotonic),
                    3,
                ),
                "last_progress_age_sec": round(
                    max(0.0, now_monotonic - self._runtime_monitor_last_progress_monotonic),
                    3,
                ),
                "active_task": self._runtime_monitor_active_task_name,
            }
            logger.error(
                "Runtime monitor watchdog detected stale main loop | age=%.1fs stale_after=%.1fs",
                age_sec,
                stale_after_sec,
            )
            self.store.record_event(
                "ERROR",
                None,
                "Runtime monitor watchdog detected stale main loop",
                payload,
            )
            self._dump_runtime_thread_traces(reason="runtime_monitor_stale")
            return True

        abort_after_sec = max(30.0, self._worker_health_interval_sec * 3.0)
        if (now_monotonic - self._runtime_monitor_stall_first_detected_monotonic) >= abort_after_sec:
            self._abort_process_for_watchdog(reason="runtime_monitor_stale")
        return True

    def _worker_health_watchdog_loop(self) -> None:
        interval_sec = max(1.0, float(self._worker_health_interval_sec))
        while not self.stop_event.is_set() and not self._worker_health_stop_event.is_set():
            try:
                self._worker_health_check_once()
                self._worker_stale_heartbeat_check_once()
                self._monitor_loop_health_check_once()
            except Exception as exc:
                logger.warning("Worker health watchdog cycle failed: %s", exc)
            self._worker_health_stop_event.wait(timeout=interval_sec)

    def _start_worker_health_thread(self) -> None:
        thread = self._worker_health_thread
        if thread is not None and thread.is_alive():
            return
        self._worker_health_stop_event.clear()
        thread = threading.Thread(
            target=self._worker_health_watchdog_loop,
            name="worker-health-watchdog",
            daemon=True,
        )
        self._worker_health_thread = thread
        thread.start()

    def _stop_worker_health_thread(self) -> None:
        self._worker_health_stop_event.set()
        thread = self._worker_health_thread
        self._worker_health_thread = None
        if thread is None:
            return
        if thread.is_alive():
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning("Worker health watchdog did not stop in time")

    def _symbols_to_run(self) -> list[str]:
        symbols: list[str] = []
        seen: set[str] = set()
        scheduled_symbols: list[str] = []
        if not self._schedule_disabled_by_multi_strategy():
            scheduled_symbols = [symbol for entry in self.config.strategy_schedule for symbol in entry.symbols]
        for symbol in list(self.config.symbols) + scheduled_symbols + [pos.symbol for pos in self.position_book.all_open()]:
            text = str(symbol).strip().upper()
            if not text or text in seen:
                continue
            seen.add(text)
            symbols.append(text)
        return symbols

    def _symbols_for_db_first_cache(self, now_utc: datetime | None = None) -> list[str]:
        # DB-first cache warming should keep data ready for all configured/scheduled symbols,
        # not only currently active strategy assignments.
        _ = now_utc
        return self._symbols_to_run()

    def _db_first_enabled(self) -> bool:
        return self._db_first_reads_enabled and self.config.broker == "ig"

    def _runtime_symbols_for_db_first_requests(self, now_utc: datetime | None = None) -> list[str]:
        now_value = self._now_utc() if now_utc is None else now_utc
        symbols: list[str] = []
        seen: set[str] = set()

        def _add(raw_symbol: object) -> None:
            text = str(raw_symbol).strip().upper()
            if not text or text in seen:
                return
            seen.add(text)
            symbols.append(text)

        for symbol in self._schedule_assignments(now_value).keys():
            _add(symbol)
        with self._workers_lock:
            worker_items = list(self.workers.items())
        for symbol, worker in worker_items:
            if worker.is_alive():
                _add(symbol)
        for position in self.position_book.all_open():
            _add(position.symbol)
        if not symbols and (not self.config.strategy_schedule or self._schedule_disabled_by_multi_strategy()):
            for symbol in self.config.symbols:
                _add(symbol)
        return symbols

    # -- DB-First Symbol Spec compat proxies --

    @property
    def _db_first_symbol_spec_poll_interval_sec(self):
        return self._db_first_spec._db_first_symbol_spec_poll_interval_sec

    @property
    def _db_first_symbol_spec_refresh_age_sec(self):
        return self._db_first_spec._db_first_symbol_spec_refresh_age_sec

    @property
    def _db_first_symbol_spec_retry_after_ts_by_symbol(self):
        return self._db_first_spec._db_first_symbol_spec_retry_after_ts_by_symbol

    @_db_first_symbol_spec_retry_after_ts_by_symbol.setter
    def _db_first_symbol_spec_retry_after_ts_by_symbol(self, value):
        self._db_first_spec._db_first_symbol_spec_retry_after_ts_by_symbol = value

    def _symbols_for_db_first_symbol_spec_refresh(self, now_utc: datetime | None = None) -> list[str]:
        return self._db_first_spec._symbols_for_db_first_symbol_spec_refresh(now_utc)

    def _reserve_ig_non_trading_budget(
        self,
        *,
        scope: str,
        wait_timeout_sec: float = 0.0,
    ) -> bool:
        return self._ig_budget.reserve(scope=scope, wait_timeout_sec=wait_timeout_sec)

    def _active_symbols_for_db_first_tick_cache(self) -> list[str]:
        return self._db_first_tick._active_symbols_for_db_first_tick_cache()

    @staticmethod
    def _coerce_finite_positive_float(value: object) -> float | None:
        return BotDbFirstTickRuntime._coerce_finite_positive_float(value)

    def _symbols_near_breakout_levels(self, active_symbols: list[str]) -> set[str]:
        return self._db_first_tick._symbols_near_breakout_levels(active_symbols)

    def _db_first_tick_priority_symbols_for_active(self, active_symbols: list[str]) -> list[str]:
        return self._db_first_tick._db_first_tick_priority_symbols_for_active(active_symbols)

    def _db_first_tick_symbol_buckets(self) -> tuple[list[str], list[str]]:
        return self._db_first_tick._db_first_tick_symbol_buckets()

    def _select_weighted_db_first_active_symbol(
        self,
        *,
        active_symbols: list[str],
        priority_symbols: list[str],
    ) -> str:
        return self._db_first_tick._select_weighted_db_first_active_symbol(
            active_symbols=active_symbols, priority_symbols=priority_symbols,
        )

    def _select_db_first_tick_symbol(self, active_symbols: list[str], passive_symbols: list[str]) -> str | None:
        return self._db_first_tick._select_db_first_tick_symbol(active_symbols, passive_symbols)

    def _maybe_warn_db_first_active_cycle_too_long(
        self,
        *,
        active_symbols: list[str],
        request_interval_sec: float,
    ) -> None:
        self._db_first_tick._maybe_warn_db_first_active_cycle_too_long(
            active_symbols=active_symbols, request_interval_sec=request_interval_sec,
        )

    def _start_db_first_cache_workers(self) -> None:
        if not self._db_first_enabled():
            return
        self._db_first_cache_stop_event.clear()
        workers: dict[str, callable] = {
            "app_non_trading_cache": self._db_first_tick_cache_loop,
            "account_non_trading_cache": self._db_first_account_cache_loop,
            "historical_cache": self._db_first_history_cache_loop,
            "symbol_spec_cache": self._db_first_symbol_spec_cache_loop,
        }
        for name, target in workers.items():
            thread = self._db_first_cache_threads.get(name)
            if thread is not None and thread.is_alive():
                continue
            thread = threading.Thread(
                target=target,
                name=f"db-first-{name}",
                daemon=True,
            )
            self._db_first_cache_threads[name] = thread
            thread.start()

    def _stop_db_first_cache_workers(self) -> None:
        if not self._db_first_cache_threads:
            return
        self._db_first_cache_stop_event.set()
        threads = list(self._db_first_cache_threads.items())
        self._db_first_cache_threads = {}
        for name, thread in threads:
            if thread.is_alive():
                thread.join(timeout=2.0)
                if thread.is_alive():
                    logger.warning("DB-first cache worker did not stop in time: %s", name)

    def _fetch_db_first_tick(self, symbol: str, *, request_interval_sec: float) -> PriceTick | None:
        return self._db_first_tick._fetch_db_first_tick(symbol, request_interval_sec=request_interval_sec)

    def _db_first_tick_cache_loop(self) -> None:
        self._db_first_tick._db_first_tick_cache_loop()

    def _maybe_prune_db_first_stale_ticks(self, *, active_symbol_count: int | None = None) -> None:
        self._db_first_tick._maybe_prune_db_first_stale_ticks(active_symbol_count=active_symbol_count)

    def _maybe_record_db_first_tick_cache_refresh_error(self, symbol: str, error: Exception) -> None:
        self._db_first_tick._maybe_record_db_first_tick_cache_refresh_error(symbol, error)

    def _maybe_reconnect_broker_for_db_first(self, now_ts: float | None = None) -> bool:
        return self._db_first_tick._maybe_reconnect_broker_for_db_first(now_ts)

    def _db_first_symbol_spec_cache_loop(self) -> None:
        self._db_first_spec._db_first_symbol_spec_cache_loop()

    def _filter_db_first_symbol_spec_refresh_candidates(
        self,
        symbols: list[str],
        *,
        now_ts: float | None = None,
    ) -> tuple[list[str], float | None]:
        return self._db_first_spec._filter_db_first_symbol_spec_refresh_candidates(symbols, now_ts=now_ts)

    def _record_db_first_symbol_spec_refresh_failure(
        self,
        symbol: str,
        error_text: str,
        *,
        request_interval_sec: float,
        now_ts: float | None = None,
    ) -> float:
        return self._db_first_spec._record_db_first_symbol_spec_refresh_failure(
            symbol, error_text, request_interval_sec=request_interval_sec, now_ts=now_ts,
        )

    def _clear_db_first_symbol_spec_refresh_retry(self, symbol: str) -> None:
        self._db_first_spec._clear_db_first_symbol_spec_refresh_retry(symbol)

    @staticmethod
    def _symbol_spec_change_payload(
        previous: SymbolSpec | None,
        current: SymbolSpec,
    ) -> dict[str, dict[str, object]] | None:
        return BotDbFirstSpecRuntime._symbol_spec_change_payload(previous, current)

    def _symbol_spec_preload_is_strict(self) -> bool:
        return self._db_first_spec._symbol_spec_preload_is_strict()

    def _broker_candidate_chain(self, max_depth: int = 4) -> list[object]:
        return self._db_first_spec._broker_candidate_chain(max_depth)

    @staticmethod
    def _is_startup_symbol_spec_fallback_error(error_text: str) -> bool:
        return BotDbFirstSpecRuntime._is_startup_symbol_spec_fallback_error(error_text)

    def _build_startup_symbol_spec_fallback(
        self,
        symbol: str,
        *,
        error_text: str = "",
    ) -> SymbolSpec | None:
        return self._db_first_spec._build_startup_symbol_spec_fallback(symbol, error_text=error_text)

    def _preload_symbol_specs_on_startup(self) -> None:
        self._db_first_spec._preload_symbol_specs_on_startup()

    def _db_first_account_cache_loop(self) -> None:
        loop_name = "account"
        while not self.stop_event.is_set() and not self._db_first_cache_stop_event.is_set():
            loop_backoff_remaining = self._db_first_loop_backoff_remaining_sec(loop_name)
            if loop_backoff_remaining > 0.0:
                self._db_first_cache_stop_event.wait(timeout=max(0.5, min(loop_backoff_remaining, 60.0)))
                continue
            if not self._reserve_ig_non_trading_budget(
                scope="db_first_account_snapshot",
                wait_timeout_sec=min(0.5, self._db_first_account_snapshot_poll_interval_sec),
            ):
                self._maybe_warn_stale_db_first_account_snapshot(reason="local_non_trading_budget")
                self._db_first_cache_stop_event.wait(timeout=self._db_first_account_snapshot_poll_interval_sec)
                continue
            try:
                snapshot = self.broker.get_account_snapshot()
                self.store.upsert_broker_account_snapshot(
                    snapshot,
                    source="db_first_account_non_trading_cache",
                )
                self._db_first_account_snapshot_last_success_ts = time.time()
                self._clear_db_first_loop_backoff(loop_name)
            except Exception as exc:
                self._maybe_warn_stale_db_first_account_snapshot(reason=str(exc))
                backoff_sec = self._record_db_first_loop_failure(
                    loop_name,
                    base_interval_sec=self._db_first_account_snapshot_poll_interval_sec,
                )
                if not self._is_allowance_related_error(str(exc)):
                    logger.debug("DB-first account snapshot refresh failed: %s", exc)
                self._db_first_cache_stop_event.wait(
                    timeout=max(self._db_first_account_snapshot_poll_interval_sec, backoff_sec)
                )
                continue
            self._db_first_cache_stop_event.wait(timeout=self._db_first_account_snapshot_poll_interval_sec)

    def _maybe_warn_stale_db_first_account_snapshot(self, *, reason: str) -> None:
        stale_after_sec = max(30.0, self._db_first_account_snapshot_poll_interval_sec * 2.0)
        latest_snapshot = self.store.load_latest_broker_account_snapshot(max_age_sec=0.0)
        if latest_snapshot is not None:
            latest_ts = float(latest_snapshot.timestamp)
        else:
            latest_ts = float(self._db_first_account_snapshot_last_success_ts or 0.0)
        if latest_ts <= 0.0:
            cache_age_sec = float("inf")
        else:
            cache_age_sec = max(0.0, time.time() - latest_ts)
        if cache_age_sec < stale_after_sec:
            return

        now = time.time()
        warn_interval_sec = max(30.0, self._db_first_account_snapshot_poll_interval_sec * 2.0)
        if (
            self._db_first_account_snapshot_stale_warn_last_ts > 0.0
            and (now - self._db_first_account_snapshot_stale_warn_last_ts) < warn_interval_sec
        ):
            return

        payload = {
            "cache_age_sec": round(cache_age_sec, 3) if math.isfinite(cache_age_sec) else "inf",
            "stale_after_sec": round(stale_after_sec, 3),
            "poll_interval_sec": round(self._db_first_account_snapshot_poll_interval_sec, 3),
            "reason": str(reason or "").strip() or "unknown",
        }
        self.store.record_event(
            "WARN",
            None,
            "DB-first account snapshot cache stale",
            payload,
        )
        logger.warning(
            "DB-first account snapshot cache stale | age=%.1fs stale_after=%.1fs poll_interval=%.1fs reason=%s",
            cache_age_sec if math.isfinite(cache_age_sec) else -1.0,
            stale_after_sec,
            self._db_first_account_snapshot_poll_interval_sec,
            payload["reason"],
        )
        self._db_first_account_snapshot_stale_warn_last_ts = now

    def _db_first_history_cache_loop(self) -> None:
        loop_name = "history"
        while not self.stop_event.is_set() and not self._db_first_cache_stop_event.is_set():
            loop_backoff_remaining = self._db_first_loop_backoff_remaining_sec(loop_name)
            if loop_backoff_remaining > 0.0:
                self._db_first_cache_stop_event.wait(timeout=max(0.5, min(loop_backoff_remaining, 60.0)))
                continue
            try:
                self._refresh_passive_price_history_from_cached_ticks()
                self._clear_db_first_loop_backoff(loop_name)
            except Exception as exc:
                backoff_sec = self._record_db_first_loop_failure(
                    loop_name,
                    base_interval_sec=self._db_first_history_poll_interval_sec,
                )
                if not self._is_allowance_related_error(str(exc)):
                    logger.debug("DB-first history cache refresh failed: %s", exc)
                self._db_first_cache_stop_event.wait(timeout=max(self._db_first_history_poll_interval_sec, backoff_sec))
                continue
            self._db_first_cache_stop_event.wait(timeout=self._db_first_history_poll_interval_sec)

    def _refresh_passive_price_history_from_cached_ticks(self) -> None:
        self._price_history._refresh_passive_price_history_from_cached_ticks()

    def _prime_db_first_caches(self) -> None:
        if not self._db_first_enabled():
            return
        if not self._db_first_prime_enabled:
            return
        symbols = self._runtime_symbols_for_db_first_requests()
        if self._db_first_prime_max_symbols > 0:
            symbols = symbols[: self._db_first_prime_max_symbols]
        for symbol in symbols:
            if not self._reserve_ig_non_trading_budget(
                scope="db_first_prime_symbol_spec",
                wait_timeout_sec=0.2,
            ):
                break
            try:
                spec = self.broker.get_symbol_spec(symbol)
                self.store.upsert_broker_symbol_spec(
                    symbol=symbol,
                    spec=spec,
                    ts=time.time(),
                    source="db_first_prime",
                )
            except Exception as exc:
                logger.debug("DB-first prime symbol spec failed for %s: %s", symbol, exc)
            if not self._reserve_ig_non_trading_budget(
                scope="db_first_prime_tick",
                wait_timeout_sec=0.2,
            ):
                break
            try:
                tick = self.broker.get_price(symbol)
                self._update_latest_tick_from_broker(tick)
                timestamp = self._normalize_timestamp_seconds(getattr(tick, "timestamp", time.time()))
                self.store.upsert_broker_tick(
                    symbol=symbol,
                    bid=float(tick.bid),
                    ask=float(tick.ask),
                    ts=float(timestamp),
                    volume=(float(tick.volume) if tick.volume is not None else None),
                    source="db_first_prime",
                )
            except Exception as exc:
                logger.debug("DB-first prime tick failed for %s: %s", symbol, exc)
        if self._db_first_prime_account_enabled:
            if not self._reserve_ig_non_trading_budget(
                scope="db_first_prime_account_snapshot",
                wait_timeout_sec=0.3,
            ):
                return
            try:
                snapshot = self.broker.get_account_snapshot()
                self.store.upsert_broker_account_snapshot(snapshot, source="db_first_prime")
            except Exception as exc:
                logger.debug("DB-first prime account snapshot failed: %s", exc)

    def _prime_db_first_account_snapshot_before_workers(self) -> None:
        """Ensure at least one account snapshot is cached before workers start.

        Without this, workers start and immediately fail with
        'DB-first account snapshot cache is empty or stale' because the async
        cache loop hasn't completed its first iteration yet.
        """
        if not self._db_first_enabled():
            return
        # Check if a fresh snapshot already exists (e.g. from _prime_db_first_caches)
        existing = self.store.load_latest_broker_account_snapshot(max_age_sec=30.0)
        if existing is not None:
            logger.debug("DB-first account snapshot already primed, skipping")
            return
        logger.info("Priming DB-first account snapshot before starting workers ...")
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                snapshot = self.broker.get_account_snapshot()
                self.store.upsert_broker_account_snapshot(
                    snapshot,
                    source="db_first_startup_prime",
                )
                logger.info("DB-first account snapshot primed successfully")
                return
            except Exception as exc:
                logger.warning(
                    "DB-first account snapshot prime attempt %d/%d failed: %s",
                    attempt,
                    max_attempts,
                    exc,
                )
                if attempt < max_attempts:
                    time.sleep(1.0)
        logger.error(
            "Failed to prime DB-first account snapshot after %d attempts — "
            "workers may see 'cache empty or stale' errors on first iterations",
            max_attempts,
        )

    def _prefill_price_history_from_broker(
        self,
        *,
        force_all_symbols: bool = False,
        max_symbols: int | None = None,
    ) -> dict[str, object]:
        return self._price_history._prefill_price_history_from_broker(force_all_symbols=force_all_symbols, max_symbols=max_symbols)

    def _history_prefetch_complete_for_all_symbols(self) -> bool:
        return self._price_history._history_prefetch_complete_for_all_symbols()

    def _pre_warm_history(self) -> None:
        self._price_history._pre_warm_history()

    def _seed_startup_history_for_fast_start(self) -> None:
        self._price_history._seed_startup_history_for_fast_start()

    def _runtime_complete_deferred_startup_tasks(self, force: bool = False) -> None:
        if self.stop_event.is_set():
            return
        if not self._startup_fast_path_enabled():
            return
        if (
            self._runtime_deferred_symbol_spec_preload_completed
            and self._runtime_deferred_history_prewarm_completed
        ):
            if not self._runtime_deferred_startup_completion_recorded:
                self.store.record_event(
                    "INFO",
                    None,
                    "Startup fast-path deferred tasks complete",
                    {
                        "mode": self.config.mode.value,
                        "broker": self.config.broker,
                    },
                )
                self._runtime_deferred_startup_completion_recorded = True
            return

        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_runtime_deferred_startup_tasks_monotonic) < 30.0:
            return
        self._last_runtime_deferred_startup_tasks_monotonic = now_monotonic

        if not self._runtime_deferred_symbol_spec_preload_completed:
            try:
                self._preload_symbol_specs_on_startup()
            except Exception as exc:
                if (now_monotonic - self._last_runtime_deferred_startup_error_monotonic) >= 60.0:
                    logger.warning("Deferred startup symbol specification preload failed: %s", exc)
                    self._last_runtime_deferred_startup_error_monotonic = now_monotonic
            else:
                self._runtime_deferred_symbol_spec_preload_completed = True

        if not self._runtime_deferred_history_prewarm_completed:
            if self._history_prefetch_complete_for_all_symbols():
                self._runtime_deferred_history_prewarm_completed = True
            else:
                max_symbols = max(1, min(4, self._passive_history_max_symbols_per_cycle))
                summary = self._prefill_price_history_from_broker(
                    force_all_symbols=False,
                    max_symbols=max_symbols,
                )
                activity_total = sum(
                    int(summary.get(key) or 0)
                    for key in ("symbols_fetched", "symbols_skipped", "symbols_failed")
                )
                if activity_total > 0 and (
                    int(summary.get("symbols_fetched") or 0) > 0
                    or int(summary.get("symbols_failed") or 0) > 0
                ):
                    payload = {
                        **summary,
                        "resolution": self._history_prefetch_resolution,
                        "target_points": self._history_prefetch_points,
                        "mode": self.config.mode.value,
                        "fast_startup": True,
                        "max_symbols": max_symbols,
                    }
                    level = "WARN" if int(summary.get("symbols_failed") or 0) > 0 else "INFO"
                    self.store.record_event(level, None, "Runtime startup history catch-up", payload)
                    logger.info(
                        "Runtime startup history catch-up | total=%s fetched=%s skipped=%s failed=%s deferred=%s appended=%s max_symbols=%s failed_symbols=%s deferred_symbols=%s",
                        summary.get("symbols_total", 0),
                        summary.get("symbols_fetched", 0),
                        summary.get("symbols_skipped", 0),
                        summary.get("symbols_failed", 0),
                        summary.get("symbols_deferred", 0),
                        summary.get("appended_samples", 0),
                        max_symbols,
                        ",".join(summary.get("failed_symbols", []) or []),
                        ",".join(summary.get("deferred_symbols", []) or []),
                    )
                if self._history_prefetch_complete_for_all_symbols():
                    self._runtime_deferred_history_prewarm_completed = True

        if (
            self._runtime_deferred_symbol_spec_preload_completed
            and self._runtime_deferred_history_prewarm_completed
            and not self._runtime_deferred_startup_completion_recorded
        ):
            self.store.record_event(
                "INFO",
                None,
                "Startup fast-path deferred tasks complete",
                {
                    "mode": self.config.mode.value,
                    "broker": self.config.broker,
                },
            )
            self._runtime_deferred_startup_completion_recorded = True

    def _broker_public_api_backoff_remaining_sec(self) -> float:
        return self._broker_state.broker_public_api_backoff_remaining_sec()

    def _broker_market_data_wait_remaining_sec(self) -> float:
        return self._broker_state.broker_market_data_wait_remaining_sec()

    @staticmethod
    def _finite_float_or_none(raw: object) -> float | None:
        return BotBrokerStateRuntime.finite_float_or_none(raw)

    @staticmethod
    def _normalize_currency_code(value: object) -> str | None:
        return BotBrokerStateRuntime.normalize_currency_code(value)

    def _broker_account_currency_code(self) -> str | None:
        return self._broker_state.broker_account_currency_code()

    def _currency_conversion_rate(
        self,
        from_currency: str | None,
        to_currency: str | None,
    ) -> tuple[float | None, str | None]:
        return self._broker_state.currency_conversion_rate(from_currency, to_currency)

    def _normalize_pnl_to_account_currency(
        self,
        pnl_amount: float | None,
        pnl_currency: object | None,
    ) -> tuple[float | None, dict[str, object]]:
        return self._broker_state.normalize_pnl_to_account_currency(pnl_amount, pnl_currency)

    def _estimate_position_pnl_from_close_price(
        self,
        position: Position,
        close_price: float | None,
        pnl_currency: object | None = None,
    ) -> tuple[float | None, dict[str, object]]:
        return self._broker_state.estimate_position_pnl_from_close_price(position, close_price, pnl_currency)

    @staticmethod
    def _trade_event_matches_position(payload: dict[str, object] | None, position_id: str) -> bool:
        return BotCloseDetailsRuntime._trade_event_matches_position(payload, position_id)

    def _best_close_details_from_events(self, position: Position) -> dict[str, object] | None:
        return self._close_details._best_close_details_from_events(position)

    def _resolved_close_details(
        self,
        position: Position,
        broker_sync: dict[str, object] | None,
    ) -> dict[str, object] | None:
        return self._close_details._resolved_close_details(position, broker_sync)

    @staticmethod
    def _broker_close_sync_has_evidence(payload: dict[str, object] | None) -> bool:
        return BotCloseDetailsRuntime._broker_close_sync_has_evidence(payload)

    def _get_broker_close_sync(
        self,
        position: Position,
        *,
        context: str,
        wait_timeout_sec: float = 0.2,
    ) -> dict[str, object] | None:
        return self._close_details._get_broker_close_sync(
            position, context=context, wait_timeout_sec=wait_timeout_sec,
        )

    def _reconcile_missing_local_position_from_broker_sync(
        self,
        position: Position,
        *,
        context: str,
        now_ts: float,
    ) -> bool:
        return self._close_details._reconcile_missing_local_position_from_broker_sync(
            position, context=context, now_ts=now_ts,
        )

    def _backfill_closed_trade_details(self) -> int:
        return self._close_details._backfill_closed_trade_details()

    def _closed_trade_details_retry_due(self, position_id: str, now_monotonic: float) -> bool:
        return self._close_details._closed_trade_details_retry_due(position_id, now_monotonic)

    def _schedule_closed_trade_details_retry(self, position_id: str, now_monotonic: float) -> None:
        self._close_details._schedule_closed_trade_details_retry(position_id, now_monotonic)

    @staticmethod
    def _is_allowance_related_error(error_text: str) -> bool:
        return BotTradeMetadataRuntime._is_allowance_related_error(error_text)

    @staticmethod
    def _slug_reason(value: str, fallback: str = "unknown") -> str:
        return BotTradeMetadataRuntime._slug_reason(value, fallback)

    def _extract_ig_error_code(self, error_text: str) -> str | None:
        return self._trade_metadata._extract_ig_error_code(error_text)

    def _extract_ig_api_endpoint(self, error_text: str) -> tuple[str, str] | None:
        return self._trade_metadata._extract_ig_api_endpoint(error_text)

    def _normalize_broker_error_reason(self, error_text: str) -> str:
        return self._trade_metadata._normalize_broker_error_reason(error_text)

    def _event_reason(self, event: dict[str, object]) -> tuple[str, str] | None:
        return self._trade_metadata._event_reason(event)

    def _maybe_record_trade_reason_summary(self, now_monotonic: float, *, force: bool = False) -> None:
        self._trade_metadata._maybe_record_trade_reason_summary(now_monotonic, force=force)

    def _refresh_passive_price_history(self, force: bool = False) -> None:
        self._price_history._refresh_passive_price_history(force)

    def _monitor_workers(self) -> None:
        if self.stop_event.is_set():
            return
        self._runtime_monitor_last_started_monotonic = time.monotonic()
        self._runtime_monitor_last_progress_monotonic = self._runtime_monitor_last_started_monotonic
        now_monotonic = time.monotonic()
        self._run_monitor_task(
            "flush_ig_rate_limit_worker_metrics",
            lambda: self._flush_ig_rate_limit_worker_metrics(now_monotonic),
        )
        self._runtime_monitor_active_task_name = "housekeeping"
        try:
            self.store.run_housekeeping()
        except Exception as exc:
            if (now_monotonic - self._last_db_housekeeping_error_monotonic) >= 60.0:
                logger.warning("State DB housekeeping failed: %s", exc)
                try:
                    self.store.record_event(
                        "WARN",
                        None,
                        "State DB housekeeping failed",
                        {"error": str(exc)},
                    )
                except Exception:
                    pass
                self._last_db_housekeeping_error_monotonic = now_monotonic
        finally:
            self._runtime_monitor_last_progress_monotonic = time.monotonic()
            self._runtime_monitor_active_task_name = None
        self._run_monitor_task(
            "record_trade_reason_summary",
            lambda: self._maybe_record_trade_reason_summary(now_monotonic),
        )
        self._run_monitor_task("runtime_sync_open_positions", self._runtime_sync_open_positions)
        deferred_noncritical_tasks: list[str] = []
        if self._ig_account_non_trading_under_pressure():
            deferred_noncritical_tasks.extend(
                [
                    "runtime_backfill_missing_on_broker_trades",
                    "runtime_backfill_closed_trade_details",
                    "runtime_complete_deferred_startup_tasks",
                ]
            )
        else:
            self._run_monitor_task(
                "runtime_backfill_missing_on_broker_trades",
                self._runtime_backfill_missing_on_broker_trades,
            )
            self._run_monitor_task(
                "runtime_backfill_closed_trade_details",
                self._runtime_backfill_closed_trade_details,
            )
            self._run_monitor_task(
                "runtime_complete_deferred_startup_tasks",
                self._runtime_complete_deferred_startup_tasks,
            )
        if deferred_noncritical_tasks:
            self._record_runtime_monitor_noncritical_deferral(
                deferred_noncritical_tasks,
                now_monotonic=time.monotonic(),
            )
        self._run_monitor_task("reconcile_workers", self._reconcile_workers)
        if (not self._db_first_enabled()) or (not self._db_first_cache_threads):
            self._run_monitor_task("refresh_passive_price_history", self._refresh_passive_price_history)
        self._runtime_monitor_last_completed_monotonic = time.monotonic()
        self._runtime_monitor_last_progress_monotonic = self._runtime_monitor_last_completed_monotonic

    def _ig_account_non_trading_under_pressure(self) -> bool:
        return self._ig_budget.account_non_trading_under_pressure()

    def _record_runtime_monitor_noncritical_deferral(
        self,
        deferred_tasks: list[str],
        *,
        now_monotonic: float,
    ) -> None:
        if not deferred_tasks:
            return
        if (now_monotonic - self._last_runtime_monitor_noncritical_deferral_monotonic) < 60.0:
            return
        self._last_runtime_monitor_noncritical_deferral_monotonic = now_monotonic
        payload: dict[str, object] = {
            "deferred_tasks": [str(task) for task in deferred_tasks],
            "mode": self.config.mode.value,
        }
        snapshot = self._ig_budget.last_account_non_trading_snapshot
        if isinstance(snapshot, dict):
            payload.update(snapshot)
        self.store.record_event(
            "WARN",
            None,
            "Runtime monitor deferred non-critical IG tasks",
            payload,
        )
        logger.warning(
            "Runtime monitor deferred non-critical IG tasks | tasks=%s used=%s limit=%s remaining=%s",
            ",".join(deferred_tasks),
            payload.get("used_value"),
            payload.get("limit_value"),
            payload.get("remaining_value"),
        )

    def _has_local_open_execution_positions(self) -> bool:
        return self._position_sync._has_local_open_execution_positions()

    def _runtime_sync_interval_sec(
        self,
        *,
        has_local_open_positions: bool | None = None,
        has_pending_opens: bool = False,
        oldest_open_age_sec: float | None = None,
    ) -> float:
        return self._position_sync._runtime_sync_interval_sec(
            has_local_open_positions=has_local_open_positions,
            has_pending_opens=has_pending_opens,
            oldest_open_age_sec=oldest_open_age_sec,
        )

    def _runtime_backfill_missing_on_broker_trades(self, force: bool = False) -> int:
        return self._position_sync._runtime_backfill_missing_on_broker_trades(force=force)

    def _runtime_backfill_closed_trade_details(self, force: bool = False) -> int:
        return self._position_sync._runtime_backfill_closed_trade_details(force=force)

    def _flush_ig_rate_limit_worker_metrics(self, now_monotonic: float) -> None:
        self._ig_budget.flush_worker_metrics(now_monotonic)

    def _runtime_sync_open_positions(self, force: bool = False) -> None:
        return self._position_sync._runtime_sync_open_positions(force=force)

    def _filter_restored_positions_for_mode(self, restored: dict[str, Position]) -> dict[str, Position]:
        return self._position_sync._filter_restored_positions_for_mode(restored)

    def _match_pending_open(self, position: Position, pending_opens: list[PendingOpen]) -> PendingOpen | None:
        return self._position_sync._match_pending_open(position, pending_opens)

    def _restore_pending_trailing_override(self, pending: PendingOpen, position_id: str) -> None:
        self._position_sync._restore_pending_trailing_override(pending, position_id)

    def _sync_execution_positions_from_broker(
        self,
        local_restored: dict[str, Position],
    ) -> tuple[dict[str, Position] | None, dict[str, object]]:
        return self._position_sync._sync_execution_positions_from_broker(local_restored)

    def _ensure_execution_startup_broker_sync_succeeded(self, summary: dict[str, object]) -> None:
        self._position_sync._ensure_execution_startup_broker_sync_succeeded(summary)

    def _restore_open_positions(self) -> tuple[dict[str, Position], dict[str, object]]:
        return self._position_sync._restore_open_positions()

    def _startup_fast_path_enabled(self) -> bool:
        return self.config.mode == RunMode.EXECUTION and self._db_first_enabled()

    def _log_open_position_restore_summary(self, summary: dict[str, object]) -> None:
        self._position_sync._log_open_position_restore_summary(summary)

    def _backfill_missing_on_broker_trades(self) -> int:
        return self._close_details._backfill_missing_on_broker_trades()

    def sync_open_positions(self) -> dict[str, object]:
        return self._position_sync.sync_open_positions()

    def start(self) -> None:
        configured_strategy_name, configured_strategy_params = self._worker_assignment_payload(
            self.config.strategy,
            self._strategy_params_for(self.config.strategy),
        )
        configured_strategy_label, configured_strategy_base = self._strategy_labels(
            configured_strategy_name,
            configured_strategy_params,
        )
        logging.info(
            "Starting bot | broker=%s account=%s mode=%s strategy=%s%s symbols=%s",
            self.config.broker,
            self.config.account_type.value,
            self.config.mode.value,
            configured_strategy_label or self.config.strategy,
            (f" strategy_base={configured_strategy_base}" if configured_strategy_base else ""),
            ",".join(self.config.symbols),
        )
        startup_core_params = compact_strategy_params(
            configured_strategy_name,
            configured_strategy_params,
        )
        if startup_core_params:
            logger.info(
                "Strategy core params | strategy=%s%s params=%s",
                configured_strategy_label or self.config.strategy,
                (f" strategy_base={configured_strategy_base}" if configured_strategy_base else ""),
                json.dumps(startup_core_params, sort_keys=True, separators=(",", ":"), default=str),
            )
        if configured_strategy_name == _MULTI_STRATEGY_CARRIER_NAME:
            base_name = str(
                configured_strategy_params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or self.config.strategy
            ).strip().lower()
            component_names = self._resolve_multi_strategy_component_names(base_name, configured_strategy_params)
            for component_name in component_names:
                component_params = self._strategy_params_for(component_name)
                component_core_params = compact_strategy_params(component_name, component_params)
                if not component_core_params:
                    continue
                logger.info(
                    "Multi component core params | component=%s params=%s",
                    component_name,
                    json.dumps(component_core_params, sort_keys=True, separators=(",", ":"), default=str),
                )

        self._connect_broker()
        final_restored, summary = self._restore_open_positions()
        self.position_book.bootstrap(final_restored)
        if final_restored:
            logger.info(
                "Restored %d open positions from %s for mode=%s",
                len(final_restored),
                str(summary.get("source") or "state_store"),
                self.config.mode.value,
            )
        fast_startup = self._startup_fast_path_enabled()
        if not fast_startup:
            reconciled_missing_count = self._backfill_missing_on_broker_trades()
            if reconciled_missing_count > 0:
                summary["missing_on_broker_reconciled_count"] = reconciled_missing_count
            reconciled_closed_details_count = self._backfill_closed_trade_details()
            if reconciled_closed_details_count > 0:
                summary["closed_details_reconciled_count"] = reconciled_closed_details_count
        self._log_open_position_restore_summary(summary)

        if not fast_startup:
            self._preload_symbol_specs_on_startup()
            self._prime_db_first_caches()
            self._pre_warm_history()
        else:
            self.store.record_event(
                "INFO",
                None,
                "Startup fast-path enabled",
                {
                    "mode": self.config.mode.value,
                    "broker": self.config.broker,
                    "db_first_enabled": self._db_first_enabled(),
                    "deferred_tasks": [
                        "startup_symbol_spec_preload",
                        "startup_history_prewarm",
                        "missing_on_broker_backfill",
                        "closed_trade_details_backfill",
                    ],
                },
            )
            logger.info(
                "Startup fast-path enabled | deferring heavy warmup/backfill tasks until runtime monitor"
            )
            self._seed_startup_history_for_fast_start()
        self._start_db_first_cache_workers()
        self._prime_db_first_account_snapshot_before_workers()
        self._reconcile_workers()
        self._start_worker_health_thread()
        if not self._db_first_enabled():
            self._refresh_passive_price_history(force=True)

    def _close_resources(self) -> None:
        if self._resources_closed:
            return
        broker_error: Exception | None = None
        store_error: Exception | None = None
        setter = getattr(self.broker, "set_stream_tick_handler", None)
        if callable(setter):
            try:
                setter(None)
            except Exception:
                logger.debug("Failed to clear broker stream tick handler during shutdown", exc_info=True)
        try:
            self.broker.close()
        except Exception as exc:
            broker_error = exc
            logger.warning("Broker close failed during shutdown: %s", exc)
        try:
            self.store.close()
        except Exception as exc:
            store_error = exc
            logger.warning("State store close failed during shutdown: %s", exc)
        else:
            logger.info("State store closed cleanly; database snapshot is safe to copy now")
        self._resources_closed = True
        if broker_error is not None or store_error is not None:
            logger.warning(
                "Shutdown completed with close errors | broker_error=%s store_error=%s",
                broker_error,
                store_error,
            )

    def request_graceful_stop(
        self,
        *,
        reason: str | None = None,
        source: str | None = None,
    ) -> None:
        normalized_reason = str(reason or "").strip() or "stop_requested"
        normalized_source = str(source or "").strip() or None
        now_ts = time.time()
        with self._shutdown_state_lock:
            if not self._shutdown_requested_reason:
                self._shutdown_requested_reason = normalized_reason
                self._shutdown_requested_source = normalized_source
                self._shutdown_requested_ts = now_ts
            else:
                if (
                    normalized_source
                    and (not self._shutdown_requested_source)
                    and self._shutdown_requested_reason == "stop_requested"
                ):
                    self._shutdown_requested_source = normalized_source
                if (
                    normalized_reason != "stop_requested"
                    and self._shutdown_requested_reason == "stop_requested"
                ):
                    self._shutdown_requested_reason = normalized_reason
                    if normalized_source:
                        self._shutdown_requested_source = normalized_source
                    if self._shutdown_requested_ts <= 0.0:
                        self._shutdown_requested_ts = now_ts
        self.stop_event.set()

    def _resolve_shutdown_state(
        self,
        *,
        force_close: bool,
        alive_workers: list[str] | None = None,
    ) -> dict[str, object]:
        with self._shutdown_state_lock:
            if not self._shutdown_requested_reason:
                self._shutdown_requested_reason = "stop_called"
                self._shutdown_requested_source = "TradingBot.stop"
                self._shutdown_requested_ts = time.time()
            reason = self._shutdown_requested_reason
            source = self._shutdown_requested_source
            requested_ts = self._shutdown_requested_ts
        payload: dict[str, object] = {
            "reason": reason,
            "force_close": bool(force_close),
            "mode": self.config.mode.value,
            "broker": self.config.broker,
            "source": source,
        }
        if requested_ts > 0.0:
            payload["requested_at_ts"] = requested_ts
        if alive_workers is not None:
            normalized_alive = sorted({str(symbol).strip().upper() for symbol in alive_workers if str(symbol).strip()})
            payload["alive_workers"] = normalized_alive
            payload["alive_worker_count"] = len(normalized_alive)
        return payload

    def _record_shutdown_requested_event(self, *, force_close: bool) -> None:
        with self._shutdown_state_lock:
            if self._shutdown_request_event_recorded:
                return
            self._shutdown_request_event_recorded = True
        payload = self._resolve_shutdown_state(force_close=force_close)
        try:
            self.store.record_event(
                "WARN" if str(payload.get("reason") or "").startswith("signal:") else "INFO",
                None,
                "Bot shutdown requested",
                payload,
            )
        except Exception:
            logger.debug("Failed to persist shutdown-requested event", exc_info=True)

    def _record_shutdown_completed_event(
        self,
        *,
        force_close: bool,
        alive_workers: list[str],
    ) -> None:
        with self._shutdown_state_lock:
            if self._shutdown_completed_event_recorded:
                return
            self._shutdown_completed_event_recorded = True
        payload = self._resolve_shutdown_state(force_close=force_close, alive_workers=alive_workers)
        try:
            self.store.record_event(
                "WARN" if alive_workers else "INFO",
                None,
                "Bot shutdown completed",
                payload,
            )
        except Exception:
            logger.debug("Failed to persist shutdown-completed event", exc_info=True)

    def stop(self, *, force_close: bool = True) -> None:
        self.request_graceful_stop(reason=None, source="TradingBot.stop")
        self._record_shutdown_requested_event(force_close=force_close)
        self._stop_worker_health_thread()
        with self._workers_lock:
            stop_events = list(self._worker_stop_events.values())
            worker_symbols = list(self.workers.keys())
            worker_symbols.extend(self._worker_lease_id_by_symbol.keys())
        worker_symbols = sorted({str(symbol).strip().upper() for symbol in worker_symbols if str(symbol).strip()})
        for stop_event in stop_events:
            stop_event.set()
        for symbol in worker_symbols:
            self._revoke_worker_lease(symbol)

        alive_symbols: list[str] = []
        with self._workers_lock:
            worker_items = list(self.workers.items())
        for symbol, worker in worker_items:
            worker.join(timeout=10.0)
            if worker.is_alive():
                alive_symbols.append(symbol)
                logger.warning("Worker for %s did not stop within timeout", symbol)
                continue
            with self._workers_lock:
                self.workers.pop(symbol, None)
                self._worker_stop_events.pop(symbol, None)
                self._worker_assignments.pop(symbol, None)
                self._worker_lease_id_by_symbol.pop(symbol, None)
                self._deferred_switch_signature_by_symbol.pop(symbol, None)
            logger.info("Stopped worker for %s", symbol)

        with self._workers_lock:
            self._worker_lease_id_by_symbol.clear()

        self._stop_db_first_cache_workers()

        if alive_symbols:
            self.store.record_event(
                "WARN",
                None,
                "Bot stop detected workers still alive",
                {"alive_workers": alive_symbols, "force_close": bool(force_close)},
            )
            if not force_close:
                return
            logger.warning(
                "Forcing broker/store close with %d workers still alive: %s",
                len(alive_symbols),
                ",".join(sorted(alive_symbols)),
            )
            # Give lingering workers a final grace period to finish DB writes
            # before we close the store connection underneath them.
            for symbol, worker in worker_items:
                if worker.is_alive():
                    worker.join(timeout=5.0)

        self._record_shutdown_completed_event(force_close=force_close, alive_workers=alive_symbols)
        self._close_resources()

    def run_forever(self) -> None:
        try:
            self.start()
            monitor_interval_sec = self._runtime_monitor_interval_sec()
            self._runtime_monitor_watchdog_enabled = True
            while not self.stop_event.is_set():
                try:
                    self._monitor_workers()
                except Exception as exc:
                    self._record_runtime_monitor_failure("monitor_loop", exc)
                self.stop_event.wait(timeout=monitor_interval_sec)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.request_graceful_stop(reason="keyboard_interrupt", source="run_forever")
        finally:
            self._runtime_monitor_watchdog_enabled = False
            self.stop()
