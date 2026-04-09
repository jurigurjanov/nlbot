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


logger = logging.getLogger(__name__)

_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"
_MULTI_STRATEGY_BASE_COMPONENT_PARAM = "_multi_strategy_base_component"


class TradingBot:
    _WORKER_WALL_CLOCK_EPOCH_THRESHOLD_SEC = 1_000_000_000.0

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
        self._history_keep_rows_min = max(100, int(config.price_history_keep_rows_min))
        history_keep_rows_cap = self._env_int("XTB_HISTORY_KEEP_ROWS_CAP", 20_000)
        self._history_keep_rows_cap = max(self._history_keep_rows_min, history_keep_rows_cap)
        history_ticks_per_candle_cap = self._env_int("XTB_HISTORY_TICKS_PER_CANDLE_CAP", 120)
        self._history_ticks_per_candle_cap = max(1, history_ticks_per_candle_cap)
        self._history_keep_rows_by_symbol: dict[str, int] = {}
        self._strategy_history_estimate_cache = _BoundedTtlCache(max_entries=512, ttl_sec=1800.0)
        self._history_prefetch_specs_by_symbol: dict[str, dict[str, int]] = {}
        self._strategy_prefetch_estimate_cache = _BoundedTtlCache(max_entries=512, ttl_sec=1800.0)
        # _strategy_support_cache is now on self._strategy_assignment
        self._passive_history_poll_interval_sec = max(0.5, float(config.passive_history_poll_interval_sec))
        self._passive_history_max_symbols_per_cycle = max(
            1,
            self._env_int("XTB_PASSIVE_HISTORY_MAX_SYMBOLS_PER_CYCLE", 2),
        )
        self._runtime_broker_sync_interval_sec = max(
            1.0,
            self._env_float("XTB_RUNTIME_BROKER_SYNC_INTERVAL_SEC", 30.0),
        )
        self._runtime_broker_sync_idle_interval_sec = max(
            300.0,
            self._runtime_broker_sync_interval_sec,
        )
        self._runtime_broker_sync_active_interval_sec = max(
            1.0,
            self._env_float("XTB_RUNTIME_BROKER_SYNC_ACTIVE_INTERVAL_SEC", 8.0),
        )
        self._runtime_missing_backfill_interval_sec = max(
            1.0,
            self._env_float(
                "XTB_RUNTIME_MISSING_BACKFILL_INTERVAL_SEC",
                self._runtime_broker_sync_active_interval_sec,
            ),
        )
        self._runtime_closed_details_backfill_interval_sec = max(
            5.0,
            self._env_float("XTB_RUNTIME_CLOSED_DETAILS_BACKFILL_INTERVAL_SEC", 60.0),
        )
        self._last_runtime_broker_sync_monotonic = 0.0
        self._last_runtime_broker_sync_error_monotonic = 0.0
        self._last_runtime_missing_backfill_monotonic = 0.0
        self._last_runtime_closed_details_backfill_monotonic = 0.0
        self._closed_trade_details_retry_after_monotonic: dict[str, float] = {}
        self._closed_trade_details_retry_backoff_sec: dict[str, float] = {}
        self._ig_budget = BotIgBudgetRuntime(self)
        self._broker_state = BotBrokerStateRuntime(self)
        self._last_runtime_monitor_noncritical_deferral_monotonic = 0.0
        self._last_db_housekeeping_error_monotonic = 0.0
        self._trade_reason_summary_enabled = self._env_bool("XTB_TRADE_REASON_SUMMARY_ENABLED", True)
        trade_reason_summary_interval_sec = self._env_float("XTB_TRADE_REASON_SUMMARY_INTERVAL_SEC", 3600.0)
        trade_reason_summary_window_sec = self._env_float("XTB_TRADE_REASON_SUMMARY_WINDOW_SEC", 3600.0)
        trade_reason_summary_scan_limit = self._env_int("XTB_TRADE_REASON_SUMMARY_SCAN_LIMIT", 20_000)
        trade_reason_summary_top = self._env_int("XTB_TRADE_REASON_SUMMARY_TOP", 20)
        self._trade_reason_summary_interval_sec = max(60.0, trade_reason_summary_interval_sec)
        self._trade_reason_summary_window_sec = max(60.0, trade_reason_summary_window_sec)
        self._trade_reason_summary_scan_limit = max(100, trade_reason_summary_scan_limit)
        self._trade_reason_summary_top = max(1, trade_reason_summary_top)
        self._last_trade_reason_summary_monotonic = 0.0
        self._last_trade_reason_summary_error_monotonic = 0.0
        self._passive_history_cursor = 0
        self._passive_history_refresh_lock = threading.Lock()
        self._last_passive_history_sample_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_error_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_ts_by_symbol: dict[str, float] = {}
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
        self._history_prefetch_enabled = self._env_bool(
            "XTB_IG_HISTORY_PREFETCH_ENABLED",
            config.broker == "ig",
        )
        self._history_prefetch_points = max(20, self._env_int("XTB_IG_HISTORY_PREFETCH_POINTS", 300))
        self._history_prefetch_resolution = str(
            os.getenv("XTB_IG_HISTORY_PREFETCH_RESOLUTION", "MINUTE")
        ).strip().upper() or "MINUTE"
        self._history_prefetch_skip_ratio = min(
            1.0,
            max(0.0, self._env_float("XTB_IG_HISTORY_PREFETCH_SKIP_RATIO", 0.85)),
        )
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
        source = (
            "forced_symbols"
            if self.config.force_symbols
            else ("forced" if self.config.force_strategy else "static")
        )
        base_strategy_name = str(self.config.strategy).strip().lower()
        base_strategy_params = self._strategy_params_for(self.config.strategy)
        if (
            (self.config.force_symbols or self.config.force_strategy)
            and base_strategy_name != _MULTI_STRATEGY_CARRIER_NAME
            and "multi_strategy_enabled" not in base_strategy_params
        ):
            assignment_strategy_name = base_strategy_name
            assignment_strategy_params = dict(base_strategy_params)
        else:
            assignment_strategy_name, assignment_strategy_params = self._worker_assignment_payload(
                self.config.strategy,
                base_strategy_params,
            )
        if (
            source == "static"
            and self.config.strategy_schedule
            and self._schedule_disabled_by_multi_strategy()
        ):
            source = "multi_static"
        return {
            symbol: WorkerAssignment(
                symbol=symbol,
                strategy_name=assignment_strategy_name,
                strategy_params=dict(assignment_strategy_params),
                mode_override=self._mode_override_for_symbol(symbol),
                source=source,
            )
            for symbol in self.config.symbols
        }

    @staticmethod
    def _worker_lease_key(symbol: str) -> str:
        return f"worker.lease.{str(symbol).strip().upper()}"

    def _acquire_worker_lease(self, symbol: str, lease_id: str) -> None:
        self.store.set_kv(self._worker_lease_key(symbol), str(lease_id))

    def _revoke_worker_lease(self, symbol: str) -> None:
        try:
            self.store.delete_kv(self._worker_lease_key(symbol))
        except Exception:
            logger.debug("Failed to revoke worker lease for %s", symbol, exc_info=True)

    def _make_worker(self, assignment: WorkerAssignment, stop_event: threading.Event) -> SymbolWorker:
        db_first_tick_max_age_sec: float | None = None
        if self._db_first_enabled():
            db_first_tick_max_age_sec = self._db_first_tick_max_age_for_workers()
        worker_mode = assignment.mode_override or self.config.mode
        symbol = str(assignment.symbol).strip().upper()
        with self._workers_lock:
            lease_id = self._worker_lease_id_by_symbol.get(symbol)
        strategy_params_map_for_worker = {
            str(name).strip().lower(): dict(params)
            for name, params in self.config.strategy_params_map.items()
            if isinstance(params, dict)
        }
        strategy_params_map_for_worker[str(assignment.strategy_name).strip().lower()] = dict(
            assignment.strategy_params
        )
        return SymbolWorker(
            symbol=symbol,
            mode=worker_mode,
            strategy_name=assignment.strategy_name,
            strategy_params=assignment.strategy_params,
            strategy_symbols_map=self.config.strategy_symbols_map,
            strategy_params_map=strategy_params_map_for_worker,
            broker=self.broker,
            store=self.store,
            risk=self.risk,
            position_book=self.position_book,
            stop_event=stop_event,
            poll_interval_sec=self.config.poll_interval_sec,
            poll_jitter_sec=self.config.worker_poll_jitter_sec,
            default_volume=self.config.default_volume,
            bot_magic_prefix=self.bot_magic_prefix,
            bot_magic_instance=self.bot_magic_instance,
            db_first_reads_enabled=self._db_first_reads_enabled and self.config.broker == "ig",
            db_first_tick_max_age_sec=db_first_tick_max_age_sec,
            latest_tick_getter=self._load_latest_tick_from_memory_cache,
            latest_tick_updater=self._update_latest_tick_from_broker,
            worker_lease_key=self._worker_lease_key(symbol),
            worker_lease_id=lease_id,
        )

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

    def _estimate_history_keep_rows(self, strategy_name: str, strategy_params: dict[str, object]) -> int:
        cache_key = self._strategy_cache_identity(strategy_name, strategy_params)
        cached = self._strategy_history_estimate_cache.get(cache_key)
        if cached is not None:
            return cached

        strategy = create_strategy(strategy_name, strategy_params)
        min_history = max(2, int(getattr(strategy, "min_history", 2)))
        history_buffer = max(min_history * 3, 64)
        candle_timeframe_sec = int(float(getattr(strategy, "candle_timeframe_sec", 0) or 0))
        if candle_timeframe_sec > 1 and self.config.poll_interval_sec > 0:
            ticks_per_candle = max(
                1,
                min(
                    self._history_ticks_per_candle_cap,
                    math.ceil(candle_timeframe_sec / max(self.config.poll_interval_sec, FLOAT_COMPARISON_TOLERANCE)),
                ),
            )
            min_candle_samples = (min_history + 2) * ticks_per_candle
            history_buffer = max(history_buffer, min_candle_samples * 2)
        estimated = min(
            self._history_keep_rows_cap,
            max(self._history_keep_rows_min, history_buffer),
        )
        self._strategy_history_estimate_cache[cache_key] = estimated
        return estimated

    @staticmethod
    def _history_resolution_seconds(resolution: str) -> int | None:
        mapping = {
            "MINUTE": 60,
            "MINUTE_2": 120,
            "MINUTE_3": 180,
            "MINUTE_5": 300,
            "MINUTE_10": 600,
            "MINUTE_15": 900,
            "MINUTE_30": 1800,
            "HOUR": 3600,
            "HOUR_2": 7200,
            "HOUR_3": 10800,
            "HOUR_4": 14400,
            "DAY": 86400,
        }
        key = str(resolution or "").strip().upper()
        return mapping.get(key)

    def _estimate_history_prefetch_specs(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[tuple[str, int], ...]:
        cache_key = self._strategy_cache_identity(strategy_name, strategy_params)
        cached = self._strategy_prefetch_estimate_cache.get(cache_key)
        if cached is not None:
            return cached
        specs: list[tuple[str, int]] = []
        strategy = create_strategy(strategy_name, strategy_params)
        min_history = max(2, int(getattr(strategy, "min_history", 2)))
        candle_timeframe_sec = int(float(getattr(strategy, "candle_timeframe_sec", 0) or 0))
        if candle_timeframe_sec > 60:
            target_candles = min_history + 4
            supported_resolutions = (
                ("MINUTE", 60),
                ("MINUTE_2", 120),
                ("MINUTE_3", 180),
                ("MINUTE_5", 300),
                ("MINUTE_10", 600),
                ("MINUTE_15", 900),
                ("MINUTE_30", 1800),
                ("HOUR", 3600),
            )
            exact_resolution = next(
                (resolution for resolution, resolution_sec in supported_resolutions if resolution_sec == candle_timeframe_sec),
                None,
            )
            if exact_resolution is not None:
                chosen_resolution = exact_resolution
                chosen_points = target_candles
            else:
                chosen_resolution = "MINUTE"
                chosen_points = min(2000, max(self._history_prefetch_points, target_candles))
                for resolution, resolution_sec in supported_resolutions:
                    if resolution_sec > candle_timeframe_sec:
                        continue
                    required_points = math.ceil(target_candles * candle_timeframe_sec / max(resolution_sec, 1))
                    if required_points <= 2000:
                        chosen_resolution = resolution
                        chosen_points = max(target_candles, required_points)
                        break
            base_resolution = str(self._history_prefetch_resolution or "").strip().upper()
            if (
                chosen_points > 0
                and (
                    chosen_resolution != base_resolution
                    or chosen_points > int(self._history_prefetch_points)
                )
            ):
                specs.append((chosen_resolution, int(chosen_points)))
        result = tuple(specs)
        self._strategy_prefetch_estimate_cache[cache_key] = result
        return result

    def _register_symbol_history_requirement(
        self,
        symbol: str,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> int:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return self._history_keep_rows_min
        keep_rows = int(self._history_keep_rows_by_symbol.get(normalized_symbol, self._history_keep_rows_min))
        base_name = str(strategy_name).strip().lower()
        component_names = self._resolve_multi_strategy_component_names(strategy_name, strategy_params)
        if not component_names:
            component_names = [base_name]
        seen_components: set[str] = set()

        for component_name in component_names:
            normalized_component = str(component_name).strip().lower()
            if not normalized_component or normalized_component in seen_components:
                continue
            seen_components.add(normalized_component)
            component_params = (
                dict(strategy_params)
                if normalized_component == base_name
                else self._strategy_params_for(normalized_component)
            )
            if not self._strategy_supports_symbol(
                normalized_component,
                component_params,
                normalized_symbol,
            ):
                continue
            required_rows = self._history_keep_rows_min
            try:
                required_rows = self._estimate_history_keep_rows(normalized_component, component_params)
            except Exception as exc:
                logger.warning(
                    "Failed to estimate history keep rows for %s/%s, using default=%d: %s",
                    normalized_symbol,
                    normalized_component,
                    self._history_keep_rows_min,
                    exc,
                )
            keep_rows = max(keep_rows, required_rows, self._history_keep_rows_min)
            try:
                prefetch_specs = self._estimate_history_prefetch_specs(normalized_component, component_params)
            except Exception as exc:
                logger.debug(
                    "Failed to estimate history prefetch specs for %s/%s: %s",
                    normalized_symbol,
                    normalized_component,
                    exc,
                )
                prefetch_specs = ()
            if prefetch_specs:
                symbol_specs = self._history_prefetch_specs_by_symbol.setdefault(normalized_symbol, {})
                for resolution, points in prefetch_specs:
                    current_points = int(symbol_specs.get(resolution, 0) or 0)
                    symbol_specs[resolution] = max(current_points, int(points))
        self._history_keep_rows_by_symbol[normalized_symbol] = keep_rows
        return keep_rows

    def _bootstrap_symbol_history_requirements(self) -> None:
        default_strategy_params = self._strategy_params_for(self.config.strategy)
        for symbol in self.config.symbols:
            self._register_symbol_history_requirement(symbol, self.config.strategy, default_strategy_params)
        if self._schedule_disabled_by_multi_strategy():
            return
        for entry in self.config.strategy_schedule:
            for symbol in entry.symbols:
                self._register_symbol_history_requirement(symbol, entry.strategy, dict(entry.strategy_params))

    def _price_history_keep_rows_for_symbol(self, symbol: str) -> int:
        normalized_symbol = str(symbol).strip().upper()
        return int(self._history_keep_rows_by_symbol.get(normalized_symbol, self._history_keep_rows_min))

    def _history_prefetch_plan_for_symbol(self, symbol: str) -> list[tuple[str, int]]:
        normalized_symbol = str(symbol).strip().upper()
        keep_rows = self._price_history_keep_rows_for_symbol(normalized_symbol)
        base_resolution = str(self._history_prefetch_resolution or "MINUTE").strip().upper() or "MINUTE"
        base_points = max(2, min(int(self._history_prefetch_points), int(keep_rows)))
        plan: list[tuple[str, int]] = [(base_resolution, base_points)]
        extra_specs = self._history_prefetch_specs_by_symbol.get(normalized_symbol, {})
        if not extra_specs:
            return plan
        for resolution, points in sorted(
            extra_specs.items(),
            key=lambda item: self._history_resolution_seconds(item[0]) or 10**9,
        ):
            effective_points = max(2, min(int(points), int(keep_rows), 2000))
            if resolution == base_resolution and effective_points <= base_points:
                continue
            plan.append((str(resolution).strip().upper(), effective_points))
        return plan

    def _candle_history_resolutions_for_symbol(self, symbol: str) -> tuple[int, ...]:
        normalized_symbol = str(symbol).strip().upper()
        extra_specs = self._history_prefetch_specs_by_symbol.get(normalized_symbol, {})
        if not extra_specs:
            return ()
        resolutions_sec: list[int] = []
        for resolution in extra_specs:
            resolution_sec = self._history_resolution_seconds(str(resolution))
            if resolution_sec is None or resolution_sec <= 60:
                continue
            if resolution_sec not in resolutions_sec:
                resolutions_sec.append(int(resolution_sec))
        resolutions_sec.sort()
        return tuple(resolutions_sec)

    def _history_prefetch_has_time_coverage(
        self,
        symbol: str,
        *,
        resolution: str,
        target_points: int,
    ) -> bool:
        resolution_sec = self._history_resolution_seconds(resolution)
        if resolution_sec is None or resolution_sec <= 0:
            return False
        if resolution_sec >= 60:
            rows = self.store.load_recent_candle_history(
                symbol,
                resolution_sec=int(resolution_sec),
                limit=max(2, int(target_points)),
            )
        else:
            rows = self.store.load_recent_price_history(symbol, limit=max(2, int(target_points)))
        if len(rows) < 2:
            return False
        timestamps: list[float] = []
        for row in rows:
            ts = self._normalize_timestamp_seconds(row.get("ts"))
            if math.isfinite(ts) and ts > 0:
                timestamps.append(ts)
        if len(timestamps) < 2:
            return False
        coverage_sec = max(timestamps) - min(timestamps)
        required_coverage_sec = max(0.0, (int(target_points) - 1) * resolution_sec * self._history_prefetch_skip_ratio)
        return coverage_sec >= required_coverage_sec

    def _is_schedule_entry_active(self, now_utc: datetime, entry) -> bool:
        local_dt = now_utc.astimezone(self._schedule_timezone)
        minute_of_day = local_dt.hour * 60 + local_dt.minute
        weekday = local_dt.weekday()
        if entry.start_minute < entry.end_minute:
            return weekday in entry.weekdays and entry.start_minute <= minute_of_day < entry.end_minute
        if weekday in entry.weekdays and minute_of_day >= entry.start_minute:
            return True
        previous_weekday = (weekday - 1) % 7
        if previous_weekday in entry.weekdays and minute_of_day < entry.end_minute:
            return True
        return False

    def _schedule_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        current_now = now_utc or self._now_utc()
        if (
            self.config.force_symbols
            or self.config.force_strategy
            or not self.config.strategy_schedule
            or self._schedule_disabled_by_multi_strategy()
        ):
            return self._static_assignments()

        ranked: dict[str, tuple[int, int, WorkerAssignment]] = {}
        for index, entry in enumerate(self.config.strategy_schedule):
            if not self._is_schedule_entry_active(current_now, entry):
                continue
            assignment = WorkerAssignment(
                symbol="",
                strategy_name=entry.strategy,
                strategy_params=dict(entry.strategy_params),
                mode_override=None,
                source="schedule",
                label=entry.label or f"{entry.strategy}@{entry.start_time}-{entry.end_time}",
            )
            for symbol in entry.symbols:
                current = ranked.get(symbol)
                candidate = (
                    int(entry.priority),
                    index,
                    WorkerAssignment(
                        symbol=symbol,
                        strategy_name=assignment.strategy_name,
                        strategy_params=dict(assignment.strategy_params),
                        mode_override=self._mode_override_for_symbol(symbol),
                        source=assignment.source,
                        label=assignment.label,
                    ),
                )
                if current is None or candidate[0] > current[0] or (candidate[0] == current[0] and candidate[1] >= current[1]):
                    ranked[symbol] = candidate
        return {symbol: item[2] for symbol, item in ranked.items()}

    def _assignment_for_open_position(
        self,
        position: Position,
        fallback: WorkerAssignment | None = None,
    ) -> WorkerAssignment:
        row = self.store.get_trade_record(position.position_id) or {}
        strategy_name = str(
            row.get("strategy")
            or position.strategy
            or (fallback.strategy_name if fallback else self.config.strategy)
        ).strip().lower()
        if fallback is not None and strategy_name == str(fallback.strategy_name).strip().lower():
            strategy_params = dict(fallback.strategy_params)
        elif strategy_name == _MULTI_STRATEGY_CARRIER_NAME:
            carrier_base_strategy = (
                self._normalize_strategy_label(row.get("strategy_entry"))
                or self._normalize_strategy_label(position.strategy_entry)
                or self._default_strategy_entry_for_assignment(fallback)
                or self._normalize_strategy_label(self.config.strategy)
                or self.config.strategy
            )
            carrier_name, carrier_params = self._worker_assignment_payload(
                carrier_base_strategy,
                self._strategy_params_for(carrier_base_strategy),
            )
            strategy_name = carrier_name
            strategy_params = carrier_params
        else:
            strategy_params = self._strategy_params_for(strategy_name)
        mode_override: RunMode | None = None
        row_mode = str(row.get("mode") or "").strip().lower()
        if row_mode in {mode.value for mode in RunMode}:
            resolved_mode = RunMode(row_mode)
            if resolved_mode != self.config.mode:
                mode_override = resolved_mode
        elif fallback is not None and fallback.mode_override is not None:
            mode_override = fallback.mode_override
        else:
            mode_override = self._mode_override_for_symbol(position.symbol)
        return WorkerAssignment(
            symbol=position.symbol,
            strategy_name=strategy_name,
            strategy_params=strategy_params,
            mode_override=mode_override,
            source="open_position",
            label=(fallback.label if fallback is not None else None),
        )

    def _target_worker_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        desired = {
            self._worker_key(symbol): assignment
            for symbol, assignment in self._schedule_assignments(now_utc).items()
        }
        for position in self.position_book.all_open():
            symbol_key = self._worker_key(position.symbol)
            desired[symbol_key] = self._assignment_for_open_position(position, desired.get(symbol_key))
        return desired

    def _start_worker_for_assignment(self, assignment: WorkerAssignment) -> None:
        with self._worker_lifecycle_lock:
            normalized_symbol = self._worker_key(assignment.symbol)
            with self._workers_lock:
                existing_worker = self.workers.get(normalized_symbol)
                existing_assignment = self._worker_assignments.get(normalized_symbol)
                if (
                    existing_worker is not None
                    and existing_assignment is not None
                    and existing_assignment.runtime_signature() == assignment.runtime_signature()
                    and existing_assignment.signature() != assignment.signature()
                ):
                    self._worker_assignments[normalized_symbol] = assignment
            if existing_worker is not None:
                logger.debug(
                    "Skipping duplicate worker start for %s | strategy=%s source=%s",
                    assignment.symbol,
                    assignment.strategy_name,
                    assignment.source,
                )
                return

            has_restored_position = self.position_book.get(assignment.symbol) is not None
            support_cache_key = (
                *self._strategy_cache_identity(
                    assignment.strategy_name,
                    assignment.strategy_params,
                ),
                str(assignment.symbol).strip().upper(),
            )
            supported = self._strategy_support_cache.get(support_cache_key)
            if supported is None:
                supported = self._strategy_supports_symbol(
                    assignment.strategy_name,
                    assignment.strategy_params,
                    assignment.symbol,
                )
            if (not has_restored_position) and (not supported):
                strategy_payload = self._assignment_strategy_labels(assignment)
                logger.warning(
                    "Skipping symbol=%s for strategy=%s%s: unsupported symbol",
                    assignment.symbol,
                    strategy_payload[0] or assignment.strategy_name,
                    (f" strategy_base={strategy_payload[1]}" if strategy_payload[1] else ""),
                )
                self.store.record_event(
                    "WARN",
                    assignment.symbol,
                    "Symbol skipped by strategy filter",
                    {
                        **self._strategy_event_payload(
                            assignment.strategy_name,
                            assignment.strategy_params,
                        ),
                        "reason": "unsupported_symbol",
                    },
                )
                return

            history_keep_rows = self._register_symbol_history_requirement(
                assignment.symbol,
                assignment.strategy_name,
                assignment.strategy_params,
            )
            lease_id = uuid.uuid4().hex
            self._acquire_worker_lease(normalized_symbol, lease_id)
            stop_event = threading.Event()
            with self._workers_lock:
                self._worker_lease_id_by_symbol[normalized_symbol] = lease_id
            worker = self._make_worker(assignment, stop_event)
            setattr(worker, "price_history_keep_rows", max(100, int(history_keep_rows)))
            with self._workers_lock:
                self._worker_stop_events[normalized_symbol] = stop_event
                self._worker_assignments[normalized_symbol] = assignment
                self.workers[normalized_symbol] = worker
            try:
                worker.start()
            except Exception:
                with self._workers_lock:
                    self.workers.pop(normalized_symbol, None)
                    self._worker_stop_events.pop(normalized_symbol, None)
                    self._worker_assignments.pop(normalized_symbol, None)
                    self._worker_lease_id_by_symbol.pop(normalized_symbol, None)
                self._revoke_worker_lease(normalized_symbol)
                raise
            strategy_name, strategy_base = self._assignment_strategy_labels(assignment)
            logger.info(
                "Started worker for %s | strategy=%s%s mode=%s source=%s",
                assignment.symbol,
                strategy_name or assignment.strategy_name,
                (f" strategy_base={strategy_base}" if strategy_base else ""),
                (assignment.mode_override.value if isinstance(assignment.mode_override, RunMode) else self.config.mode.value),
                assignment.source,
            )

    def _stop_worker_for_symbol(self, symbol: str, reason: str) -> None:
        with self._worker_lifecycle_lock:
            normalized_symbol = self._worker_key(symbol)
            with self._workers_lock:
                worker = self.workers.pop(normalized_symbol, None)
                stop_event = self._worker_stop_events.pop(normalized_symbol, None)
                assignment = self._worker_assignments.pop(normalized_symbol, None)
                self._worker_lease_id_by_symbol.pop(normalized_symbol, None)
                self._deferred_switch_signature_by_symbol.pop(normalized_symbol, None)
            self._revoke_worker_lease(normalized_symbol)
            if stop_event is not None:
                stop_event.set()
            if worker is not None:
                worker.join(timeout=5.0)
            logger.info("Stopped worker for %s | reason=%s", normalized_symbol, reason)
            if assignment is not None:
                self.store.record_event(
                    "INFO",
                    normalized_symbol,
                    "Worker stopped",
                    {
                        **self._strategy_event_payload(
                            assignment.strategy_name,
                            assignment.strategy_params,
                        ),
                        "reason": reason,
                        "source": assignment.source,
                    },
                )

    def _record_deferred_switch(self, symbol: str, current: WorkerAssignment, desired: WorkerAssignment) -> None:
        normalized_symbol = self._worker_key(symbol)
        current_mode = (
            current.mode_override.value if isinstance(current.mode_override, RunMode) else self.config.mode.value
        )
        desired_mode = (
            desired.mode_override.value if isinstance(desired.mode_override, RunMode) else self.config.mode.value
        )
        marker = (f"{current.strategy_name}:{current_mode}", f"{desired.strategy_name}:{desired_mode}")
        with self._workers_lock:
            if self._deferred_switch_signature_by_symbol.get(normalized_symbol) == marker:
                return
            self._deferred_switch_signature_by_symbol[normalized_symbol] = marker
        logger.info(
            "Deferred strategy switch for %s until open position closes | current=%s(%s) next=%s(%s)",
            normalized_symbol,
            current.strategy_name,
            current_mode,
            desired.strategy_name,
            desired_mode,
        )
        self.store.record_event(
            "INFO",
            normalized_symbol,
            "Strategy switch deferred until position closes",
            {
                "current_strategy": self._assignment_strategy_labels(current)[0],
                "current_strategy_base": self._assignment_strategy_labels(current)[1],
                "next_strategy": self._assignment_strategy_labels(desired)[0],
                "next_strategy_base": self._assignment_strategy_labels(desired)[1],
                "current_mode": current_mode,
                "next_mode": desired_mode,
            },
        )

    def _reconcile_workers(self, now_utc: datetime | None = None) -> None:
        with self._worker_lifecycle_lock:
            desired = self._target_worker_assignments(now_utc)

            with self._workers_lock:
                worker_items = list(self.workers.items())
            for symbol, worker in worker_items:
                if worker is None:
                    continue
                if worker.is_alive():
                    continue
                with self._workers_lock:
                    self.workers.pop(symbol, None)
                    self._worker_stop_events.pop(symbol, None)
                    current_assignment = self._worker_assignments.pop(symbol, None)
                    self._worker_lease_id_by_symbol.pop(self._worker_key(symbol), None)
                    self._deferred_switch_signature_by_symbol.pop(symbol, None)
                active_position = self.position_book.get(symbol)
                restart_assignment = desired.get(symbol)
                if active_position is not None:
                    restart_assignment = self._assignment_for_open_position(active_position, restart_assignment)
                if restart_assignment is not None:
                    logger.warning(
                        "Worker for %s stopped unexpectedly, restarting with strategy=%s",
                        symbol,
                        restart_assignment.strategy_name,
                    )
                    self.store.record_event(
                        "WARN",
                        symbol,
                        "Worker restarted",
                        {
                            **self._strategy_event_payload(
                                restart_assignment.strategy_name,
                                restart_assignment.strategy_params,
                            ),
                            "previous_strategy": self._assignment_strategy_labels(current_assignment)[0],
                            "previous_strategy_base": self._assignment_strategy_labels(current_assignment)[1],
                        },
                    )
                    self._start_worker_for_assignment(restart_assignment)

            with self._workers_lock:
                assignment_items = list(self._worker_assignments.items())
            for symbol, assignment in assignment_items:
                desired_assignment = desired.get(symbol)
                active_position = self.position_book.get(symbol)
                if desired_assignment is None:
                    if active_position is None:
                        self._stop_worker_for_symbol(symbol, "schedule_inactive")
                    continue

                if assignment.runtime_signature() == desired_assignment.runtime_signature():
                    # Do not restart workers when only metadata/source changed.
                    if assignment.signature() != desired_assignment.signature():
                        with self._workers_lock:
                            self._worker_assignments[symbol] = desired_assignment
                    with self._workers_lock:
                        self._deferred_switch_signature_by_symbol.pop(symbol, None)
                    continue

                if active_position is not None:
                    self._record_deferred_switch(symbol, assignment, desired_assignment)
                    continue

                current_mode = (
                    assignment.mode_override.value if isinstance(assignment.mode_override, RunMode) else self.config.mode.value
                )
                desired_mode = (
                    desired_assignment.mode_override.value
                    if isinstance(desired_assignment.mode_override, RunMode)
                    else self.config.mode.value
                )
                self._stop_worker_for_symbol(
                    symbol,
                    (
                        "assignment_change:"
                        f"{assignment.strategy_name}@{current_mode}"
                        "->"
                        f"{desired_assignment.strategy_name}@{desired_mode}"
                    ),
                )
                self._start_worker_for_assignment(desired_assignment)

            for symbol, assignment in desired.items():
                with self._workers_lock:
                    exists = symbol in self.workers
                if exists:
                    continue
                self._start_worker_for_assignment(assignment)

    def _start_workers(self) -> None:
        self._reconcile_workers()

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
        if self.stop_event.is_set():
            return
        if not self._passive_history_refresh_lock.acquire(blocking=False):
            return
        loop_start_monotonic = time.monotonic()
        try:
            with self._workers_lock:
                worker_items = list(self.workers.items())
            active_symbols = {symbol for symbol, worker in worker_items if worker.is_alive()}
            symbols = self._symbols_for_db_first_cache()
            if not symbols:
                return
            total_symbols = len(symbols)
            start_idx = self._passive_history_cursor % total_symbols
            ordered_symbols = symbols[start_idx:] + symbols[:start_idx]
            max_symbols_per_cycle = min(total_symbols, self._passive_history_max_symbols_per_cycle)
            processed = 0
            max_tick_age_sec = max(
                5.0,
                min(300.0, self._db_first_tick_max_age_for_workers() * 4.0),
            )

            for symbol in ordered_symbols:
                now_monotonic = time.monotonic()
                if (now_monotonic - loop_start_monotonic) >= max(1.0, self._db_first_history_poll_interval_sec):
                    break
                if symbol in active_symbols:
                    continue
                last_sample_monotonic = self._last_passive_history_sample_monotonic_by_symbol.get(symbol, 0.0)
                if (now_monotonic - last_sample_monotonic) < self._passive_history_poll_interval_sec:
                    continue
                tick = self.store.load_latest_broker_tick(symbol, max_age_sec=max_tick_age_sec)
                if tick is None:
                    continue
                keep_rows = self._price_history_keep_rows_for_symbol(symbol)
                now_ts = time.time()
                timestamp = self._normalize_timestamp_seconds(getattr(tick, "timestamp", now_ts))
                max_future_skew_sec = 120.0
                if timestamp > (now_ts + max_future_skew_sec):
                    timestamp = now_ts
                last_ts = self._last_passive_history_ts_by_symbol.get(symbol)
                if last_ts is not None:
                    if last_ts > (now_ts + max_future_skew_sec):
                        last_ts = now_ts
                    if timestamp <= (last_ts + FLOAT_COMPARISON_TOLERANCE):
                        continue
                self._last_passive_history_ts_by_symbol[symbol] = timestamp
                current_volume: float | None = None
                if tick.volume is not None:
                    try:
                        parsed_volume = float(tick.volume)
                    except (TypeError, ValueError):
                        parsed_volume = 0.0
                    if parsed_volume > 0:
                        current_volume = parsed_volume

                self.store.append_price_sample(
                    symbol=symbol,
                    ts=timestamp,
                    close=float(tick.mid),
                    volume=current_volume,
                    max_rows_per_symbol=keep_rows,
                    candle_resolutions_sec=self._candle_history_resolutions_for_symbol(symbol),
                )
                self._last_passive_history_sample_monotonic_by_symbol[symbol] = now_monotonic
                processed += 1
                if processed >= max_symbols_per_cycle:
                    break

            self._passive_history_cursor = (start_idx + max(1, processed)) % total_symbols
        finally:
            self._passive_history_refresh_lock.release()

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
        summary: dict[str, object] = {
            "symbols_total": 0,
            "symbols_fetched": 0,
            "symbols_skipped": 0,
            "symbols_failed": 0,
            "symbols_deferred": 0,
            "appended_samples": 0,
            "fetched_symbols": [],
            "failed_symbols": [],
            "deferred_symbols": [],
        }
        if not self._history_prefetch_enabled:
            return summary
        fetcher = getattr(self.broker, "fetch_recent_price_history", None)
        if not callable(fetcher):
            return summary

        def _append_summary_symbol(key: str, symbol_name: str) -> None:
            items = summary.get(key)
            if not isinstance(items, list):
                items = []
                summary[key] = items
            text = str(symbol_name).strip().upper()
            if text and text not in items:
                items.append(text)

        symbols = self._symbols_to_run()
        summary["symbols_total"] = len(symbols)
        target_symbol_limit = max(1, int(max_symbols)) if max_symbols is not None else None
        targeted_symbols = 0
        for symbol in symbols:
            keep_rows = self._price_history_keep_rows_for_symbol(symbol)
            fetched_for_symbol = False
            attempted_for_symbol = False
            skipped_all_specs = True
            for resolution, target_points in self._history_prefetch_plan_for_symbol(symbol):
                if not force_all_symbols and self._history_prefetch_has_time_coverage(
                    symbol,
                    resolution=resolution,
                    target_points=target_points,
                ):
                    continue
                skipped_all_specs = False
                attempted_for_symbol = True
                if not self._reserve_ig_non_trading_budget(
                    scope="history_prefetch",
                    wait_timeout_sec=0.3,
                ):
                    summary["symbols_deferred"] = int(summary.get("symbols_deferred") or 0) + 1
                    _append_summary_symbol("deferred_symbols", symbol)
                    break
                try:
                    rows = fetcher(
                        symbol,
                        resolution=resolution,
                        points=target_points,
                    )
                except Exception as exc:
                    text = str(exc)
                    if self._is_allowance_related_error(text):
                        logger.info(
                            "Skipping history prefetch due allowance/backoff | symbol=%s resolution=%s error=%s",
                            symbol,
                            resolution,
                            text,
                        )
                        summary["symbols_deferred"] = int(summary.get("symbols_deferred") or 0) + 1
                        _append_summary_symbol("deferred_symbols", symbol)
                        break
                    logger.debug(
                        "History prefetch failed for %s resolution=%s: %s",
                        symbol,
                        resolution,
                        exc,
                    )
                    summary["symbols_failed"] = int(summary.get("symbols_failed") or 0) + 1
                    _append_summary_symbol("failed_symbols", symbol)
                    continue
                summary["symbols_fetched"] = int(summary.get("symbols_fetched") or 0) + 1
                _append_summary_symbol("fetched_symbols", symbol)
                fetched_for_symbol = True
                if not rows:
                    continue
                appended = 0
                resolution_sec = self._history_resolution_seconds(resolution)
                for row in rows:
                    if not isinstance(row, tuple) or len(row) < 2:
                        continue
                    ts = self._normalize_timestamp_seconds(row[0])
                    close = float(row[1])
                    open_price: float | None = None
                    high_price: float | None = None
                    low_price: float | None = None
                    volume: float | None = None
                    if len(row) >= 3 and row[2] is not None:
                        try:
                            parsed_volume = float(row[2])
                        except (TypeError, ValueError):
                            parsed_volume = 0.0
                        if parsed_volume > 0:
                            volume = parsed_volume
                    if len(row) >= 6:
                        for value, target in ((row[3], "open"), (row[4], "high"), (row[5], "low")):
                            try:
                                parsed = float(value) if value is not None else 0.0
                            except (TypeError, ValueError):
                                parsed = 0.0
                            if parsed <= 0.0 or not math.isfinite(parsed):
                                parsed = 0.0
                            if target == "open" and parsed > 0.0:
                                open_price = parsed
                            elif target == "high" and parsed > 0.0:
                                high_price = parsed
                            elif target == "low" and parsed > 0.0:
                                low_price = parsed
                    if resolution_sec is not None and resolution_sec > 60:
                        self.store.append_candle_sample(
                            symbol=symbol,
                            resolution_sec=resolution_sec,
                            ts=ts,
                            close=close,
                            open_price=open_price,
                            high_price=high_price,
                            low_price=low_price,
                            volume=volume,
                            max_rows_per_symbol=max(keep_rows, target_points + 16),
                        )
                    else:
                        self.store.append_price_sample(
                            symbol=symbol,
                            ts=ts,
                            close=close,
                            volume=volume,
                            max_rows_per_symbol=keep_rows,
                            candle_resolutions_sec=self._candle_history_resolutions_for_symbol(symbol),
                        )
                        if resolution_sec is not None and resolution_sec >= 60:
                            self.store.append_candle_sample(
                                symbol=symbol,
                                resolution_sec=resolution_sec,
                                ts=ts,
                                close=close,
                                open_price=open_price,
                                high_price=high_price,
                                low_price=low_price,
                                volume=volume,
                                max_rows_per_symbol=max(keep_rows, target_points + 16),
                            )
                    appended += 1
                if appended > 0:
                    summary["appended_samples"] = int(summary.get("appended_samples") or 0) + appended
                    logger.info(
                        "History prefetch complete | symbol=%s appended=%d resolution=%s target_points=%d",
                        symbol,
                        appended,
                        resolution,
                        target_points,
                    )
            if not skipped_all_specs:
                targeted_symbols += 1
            if not attempted_for_symbol and skipped_all_specs:
                summary["symbols_skipped"] += 1
            elif not fetched_for_symbol and skipped_all_specs:
                summary["symbols_skipped"] += 1
            if target_symbol_limit is not None and targeted_symbols >= target_symbol_limit:
                break
        return summary

    def _history_prefetch_complete_for_all_symbols(self) -> bool:
        if not self._history_prefetch_enabled:
            return True
        fetcher = getattr(self.broker, "fetch_recent_price_history", None)
        if not callable(fetcher):
            return True
        symbols = self._symbols_to_run()
        if not symbols:
            return True
        for symbol in symbols:
            for resolution, target_points in self._history_prefetch_plan_for_symbol(symbol):
                if not self._history_prefetch_has_time_coverage(
                    symbol,
                    resolution=resolution,
                    target_points=target_points,
                ):
                    return False
        return True

    def _pre_warm_history(self) -> None:
        summary = self._prefill_price_history_from_broker(force_all_symbols=True)
        if int(summary.get("symbols_total") or 0) <= 0:
            return
        self.store.record_event(
            "INFO",
            None,
            "Startup history pre-warm complete",
            {
                **summary,
                "resolution": self._history_prefetch_resolution,
                "target_points": self._history_prefetch_points,
                "mode": self.config.mode.value,
            },
        )
        logger.info(
            "Startup history pre-warm complete | total=%s fetched=%s skipped=%s failed=%s appended=%s resolution=%s target_points=%s",
            summary.get("symbols_total", 0),
            summary.get("symbols_fetched", 0),
            summary.get("symbols_skipped", 0),
            summary.get("symbols_failed", 0),
            summary.get("appended_samples", 0),
            self._history_prefetch_resolution,
            self._history_prefetch_points,
        )

    def _seed_startup_history_for_fast_start(self) -> None:
        summary = self._prefill_price_history_from_broker(force_all_symbols=False)
        if int(summary.get("symbols_total") or 0) <= 0:
            return
        activity_total = sum(
            int(summary.get(key) or 0)
            for key in ("symbols_fetched", "symbols_skipped", "symbols_failed")
        )
        if activity_total <= 0:
            return
        self.store.record_event(
            "INFO",
            None,
            "Startup history seed complete",
            {
                **summary,
                "resolution": self._history_prefetch_resolution,
                "target_points": self._history_prefetch_points,
                "mode": self.config.mode.value,
                "fast_startup": True,
            },
        )
        logger.info(
            "Startup history seed complete | total=%s fetched=%s skipped=%s failed=%s appended=%s resolution=%s target_points=%s",
            summary.get("symbols_total", 0),
            summary.get("symbols_fetched", 0),
            summary.get("symbols_skipped", 0),
            summary.get("symbols_failed", 0),
            summary.get("appended_samples", 0),
            self._history_prefetch_resolution,
            self._history_prefetch_points,
        )

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
        if not isinstance(payload, dict):
            return False
        return str(payload.get("position_id") or "").strip() == str(position_id or "").strip()

    def _best_close_details_from_events(self, position: Position) -> dict[str, object] | None:
        event_rows = self.store.load_trade_close_events(
            symbol=position.symbol,
            position_id=position.position_id,
            opened_at=position.opened_at,
            closed_at=position.closed_at,
        )
        candidates: list[dict[str, object]] = []
        for event in event_rows:
            message = str(event.get("message") or "").strip()
            is_close_event = (
                message in {
                    "Position closed",
                    "Closed trade details backfilled from broker sync",
                    "Closed trade details repaired from stored close events",
                    "Manual broker close reconciled",
                    "Manual broker close reconciled (details pending broker sync)",
                }
                or message.startswith("Local open position reconciled as closed during ")
            )
            if not is_close_event:
                continue
            payload = event.get("payload")
            if not self._trade_event_matches_position(payload if isinstance(payload, dict) else None, position.position_id):
                continue
            payload_dict = payload if isinstance(payload, dict) else {}
            broker_sync = payload_dict.get("broker_close_sync")
            broker_sync_dict = broker_sync if isinstance(broker_sync, dict) else {}
            close_price = (
                self._finite_float_or_none(payload_dict.get("close_price"))
                or self._finite_float_or_none(broker_sync_dict.get("close_price"))
                or self._finite_float_or_none(payload_dict.get("inferred_close_price"))
            )
            pnl_currency = (
                self._normalize_currency_code(broker_sync_dict.get("pnl_currency"))
                or self._normalize_currency_code(payload_dict.get("pnl_currency"))
            )
            realized_pnl = (
                self._finite_float_or_none(broker_sync_dict.get("realized_pnl"))
                or self._finite_float_or_none(payload_dict.get("realized_pnl"))
            )
            pnl_value: float | None = realized_pnl
            pnl_is_estimated = False
            pnl_estimate_source: str | None = None
            payload_pnl = self._finite_float_or_none(payload_dict.get("pnl"))
            inferred_close_price = self._finite_float_or_none(payload_dict.get("inferred_close_price"))
            if pnl_value is None and payload_pnl is not None and abs(payload_pnl) > FLOAT_COMPARISON_TOLERANCE:
                pnl_value = payload_pnl
                pnl_is_estimated = bool(payload_dict.get("pnl_is_estimated"))
                pnl_estimate_source = (
                    str(payload_dict.get("pnl_estimate_source") or "").strip() or None
                )
            local_pnl_estimate = self._finite_float_or_none(payload_dict.get("local_pnl_estimate"))
            if pnl_value is None and local_pnl_estimate is not None and abs(local_pnl_estimate) > FLOAT_COMPARISON_TOLERANCE:
                pnl_value = local_pnl_estimate
                pnl_is_estimated = True
                pnl_estimate_source = (
                    str(payload_dict.get("pnl_estimate_source") or "").strip()
                    or "event_local_pnl_estimate"
                )
            if pnl_value is None and close_price is not None:
                estimated_pnl, _ = self._estimate_position_pnl_from_close_price(
                    position,
                    close_price,
                    pnl_currency,
                )
                if estimated_pnl is not None:
                    pnl_value = estimated_pnl
                    pnl_is_estimated = True
                    pnl_estimate_source = (
                        str(payload_dict.get("pnl_estimate_source") or "").strip()
                        or (
                            "event_inferred_close_price"
                            if inferred_close_price is not None
                            else "event_close_price"
                        )
                    )
            event_closed_at = self._finite_float_or_none(event.get("ts"))
            event_has_close_details = (
                close_price is not None
                or realized_pnl is not None
                or payload_pnl is not None
                or local_pnl_estimate is not None
                or inferred_close_price is not None
            )
            closed_at = (
                self._finite_float_or_none(broker_sync_dict.get("closed_at"))
                or self._finite_float_or_none(payload_dict.get("broker_closed_at"))
                or (event_closed_at if event_has_close_details else None)
            )
            if close_price is None and closed_at is None and pnl_value is None:
                continue
            candidates.append(
                {
                    "close_price": close_price,
                    "closed_at": closed_at,
                    "pnl": pnl_value,
                    "pnl_is_estimated": pnl_is_estimated,
                    "pnl_estimate_source": pnl_estimate_source,
                    "pnl_currency": pnl_currency,
                    "event_ts": self._finite_float_or_none(event.get("ts")) or 0.0,
                    "has_realized_pnl": realized_pnl is not None,
                }
            )
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda item: (
                1 if bool(item.get("has_realized_pnl")) else 0,
                1 if self._finite_float_or_none(item.get("pnl")) is not None else 0,
                1 if self._finite_float_or_none(item.get("close_price")) is not None else 0,
                1 if self._finite_float_or_none(item.get("closed_at")) is not None else 0,
                float(item.get("event_ts") or 0.0),
            ),
        )

    def _resolved_close_details(
        self,
        position: Position,
        broker_sync: dict[str, object] | None,
    ) -> dict[str, object]:
        broker_sync_dict = broker_sync if isinstance(broker_sync, dict) else {}
        event_details = self._best_close_details_from_events(position)
        close_price = self._finite_float_or_none(broker_sync_dict.get("close_price"))
        if close_price is None and isinstance(event_details, dict):
            close_price = self._finite_float_or_none(event_details.get("close_price"))
        closed_at = self._finite_float_or_none(broker_sync_dict.get("closed_at"))
        if closed_at is None and isinstance(event_details, dict):
            closed_at = self._finite_float_or_none(event_details.get("closed_at"))
        realized_pnl = self._finite_float_or_none(broker_sync_dict.get("realized_pnl"))
        pnl_value = realized_pnl
        pnl_is_estimated = False
        pnl_estimate_source: str | None = None
        pnl_currency = self._normalize_currency_code(broker_sync_dict.get("pnl_currency"))
        if pnl_currency is None and isinstance(event_details, dict):
            pnl_currency = self._normalize_currency_code(event_details.get("pnl_currency"))
        if pnl_value is None and isinstance(event_details, dict):
            event_pnl = self._finite_float_or_none(event_details.get("pnl"))
            if event_pnl is not None:
                pnl_value = event_pnl
                pnl_is_estimated = bool(event_details.get("pnl_is_estimated"))
                pnl_estimate_source = str(event_details.get("pnl_estimate_source") or "").strip() or None
        if pnl_value is None and close_price is not None:
            estimated_pnl, _ = self._estimate_position_pnl_from_close_price(position, close_price, pnl_currency)
            if estimated_pnl is not None:
                pnl_value = estimated_pnl
                pnl_is_estimated = True
                pnl_estimate_source = "broker_close_price"
        return {
            "close_price": close_price,
            "closed_at": closed_at,
            "pnl": pnl_value,
            "pnl_is_estimated": pnl_is_estimated,
            "pnl_estimate_source": pnl_estimate_source,
            "pnl_currency": pnl_currency,
            "has_realized_pnl": realized_pnl is not None,
        }

    @staticmethod
    def _broker_close_sync_has_evidence(payload: dict[str, object] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        close_price = payload.get("close_price")
        realized_pnl = payload.get("realized_pnl")
        close_deal_id = str(payload.get("close_deal_id") or "").strip()
        deal_reference = str(payload.get("deal_reference") or "").strip()
        history_reference = str(payload.get("history_reference") or "").strip()
        position_found_raw = payload.get("position_found")
        position_missing = isinstance(position_found_raw, bool) and (not position_found_raw)
        return (
            close_price is not None
            or realized_pnl is not None
            or bool(close_deal_id)
            or bool(deal_reference)
            or bool(history_reference)
            or position_missing
        )

    def _get_broker_close_sync(
        self,
        position: Position,
        *,
        context: str,
        wait_timeout_sec: float = 0.2,
    ) -> dict[str, object] | None:
        self._pulse_runtime_monitor_progress()
        getter = getattr(self.broker, "get_position_close_sync", None)
        if not callable(getter):
            return None
        if not self._reserve_ig_non_trading_budget(
            scope="position_close_sync",
            wait_timeout_sec=max(0.0, float(wait_timeout_sec)),
        ):
            return None
        expected_deal_reference = self.store.get_trade_deal_reference(position.position_id)
        try:
            payload = call_broker_method_with_supported_kwargs(
                self.broker,
                "get_position_close_sync",
                getter,
                position.position_id,
                deal_reference=expected_deal_reference,
                symbol=position.symbol,
                opened_at=position.opened_at,
                open_price=position.open_price,
                volume=position.volume,
                side=position.side.value,
            )
        except Exception as exc:
            self._pulse_runtime_monitor_progress()
            self.store.record_event(
                "WARN",
                position.symbol,
                "Broker close sync fetch failed",
                {
                    "position_id": position.position_id,
                    "context": context,
                    "error": str(exc),
                },
            )
            return None
        self._pulse_runtime_monitor_progress()
        if not isinstance(payload, dict):
            return None
        return payload

    def _reconcile_missing_local_position_from_broker_sync(
        self,
        position: Position,
        *,
        context: str,
        now_ts: float,
    ) -> bool:
        broker_sync = self._get_broker_close_sync(
            position,
            context=context,
            wait_timeout_sec=0.75,
        )
        if not self._broker_close_sync_has_evidence(broker_sync):
            close_details = self._resolved_close_details(position, None)
            if (
                self._finite_float_or_none(close_details.get("close_price")) is None
                and self._finite_float_or_none(close_details.get("closed_at")) is None
                and self._finite_float_or_none(close_details.get("pnl")) is None
            ):
                return False
        else:
            close_details = self._resolved_close_details(position, broker_sync)

        close_price = self._finite_float_or_none(close_details.get("close_price"))
        if close_price is not None and close_price <= 0:
            close_price = None
        closed_at_raw = self._finite_float_or_none(close_details.get("closed_at"))
        closed_at = min(now_ts, closed_at_raw) if closed_at_raw is not None and closed_at_raw > 0 else None
        pnl_value = self._finite_float_or_none(close_details.get("pnl"))
        pnl_is_estimated = bool(close_details.get("pnl_is_estimated")) if pnl_value is not None else None
        pnl_estimate_source = close_details.get("pnl_estimate_source")
        has_close_details = (close_price is not None) or (closed_at is not None) or (pnl_value is not None)

        # If the worker already persisted a close_reason (from the original
        # _close_position call), prefer it over the generic "broker_reconcile"
        # label so the real exit trigger is preserved in trade history.
        existing_perf = self.store.load_trade_performance(position.position_id)
        existing_reason = str((existing_perf or {}).get("close_reason") or "").strip() or None

        if has_close_details:
            self.store.update_trade_status(
                position.position_id,
                status="closed",
                close_price=close_price,
                closed_at=closed_at,
                pnl=pnl_value,
                pnl_is_estimated=pnl_is_estimated,
                pnl_estimate_source=pnl_estimate_source,
            )
            reconcile_reason = (
                f"close_verified:{existing_reason}:{context}"
                if existing_reason
                else f"broker_reconcile:{context}:closed"
            )
            self.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=closed_at,
                close_reason=reconcile_reason,
            )
        else:
            # Position absence on broker confirms closure, but we keep existing close fields untouched
            # when broker history does not provide execution details yet.
            preserved_closed_at = self._finite_float_or_none(position.closed_at)
            if preserved_closed_at is not None and preserved_closed_at > 0:
                preserved_closed_at = min(now_ts, preserved_closed_at)
            else:
                preserved_closed_at = None
            self.store.update_trade_status(
                position.position_id,
                status="closed",
            )
            reconcile_reason = (
                f"close_verified:{existing_reason}:{context}"
                if existing_reason
                else f"broker_reconcile:{context}:closed_pending_details"
            )
            self.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=preserved_closed_at,
                close_reason=reconcile_reason,
            )
        source = str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync"
        self.store.record_event(
            "INFO",
            position.symbol,
            f"Local open position reconciled as closed during {context}",
            {
                "position_id": position.position_id,
                "source": source,
                "broker_close_sync": broker_sync,
            },
        )
        logger.info(
            "Local open position reconciled as closed during %s | symbol=%s position_id=%s source=%s",
            context,
            position.symbol,
            position.position_id,
            source,
        )
        return True

    def _backfill_closed_trade_details(self) -> int:
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return 0
        if self._broker_public_api_backoff_remaining_sec() > 0.0:
            return 0
        now_ts = time.time()
        now_monotonic = time.monotonic()
        max_passes = self._close_reconcile_max_passes if self._close_reconcile_enabled else 1
        reconciled_count_total = 0
        mismatch_alert_count = 0
        for pass_idx in range(max_passes):
            self._pulse_runtime_monitor_progress()
            closed_positions = self.store.load_positions_by_status(
                "closed",
                mode=self.config.mode.value,
            )
            if not closed_positions:
                break
            pass_reconciled = 0
            pending_count = 0
            for position in closed_positions.values():
                self._pulse_runtime_monitor_progress()
                close_price_existing = self._finite_float_or_none(position.close_price)
                closed_at_existing = self._finite_float_or_none(position.closed_at)
                pnl_existing = self._finite_float_or_none(position.pnl)
                has_good_close_details = (
                    close_price_existing is not None
                    and close_price_existing > 0
                    and closed_at_existing is not None
                    and closed_at_existing > 0
                )
                looks_like_inferred_open_snapshot = (
                    has_good_close_details
                    and abs(float(close_price_existing) - float(position.open_price)) <= max(
                        FLOAT_COMPARISON_TOLERANCE,
                        abs(float(position.open_price)) * FLOAT_COMPARISON_TOLERANCE,
                    )
                    and abs(float(closed_at_existing) - float(position.opened_at)) <= 2.0
                    and (pnl_existing is not None and abs(float(pnl_existing)) > FLOAT_COMPARISON_TOLERANCE)
                )
                has_non_zero_pnl = pnl_existing is not None and abs(float(pnl_existing)) > FLOAT_COMPARISON_TOLERANCE
                should_probe = (
                    (not has_good_close_details)
                    or (not has_non_zero_pnl)
                    or looks_like_inferred_open_snapshot
                )
                if not should_probe:
                    self._closed_trade_details_retry_after_monotonic.pop(position.position_id, None)
                    self._closed_trade_details_retry_backoff_sec.pop(position.position_id, None)
                    continue
                if pass_idx > 0:
                    pending_count += 1
                    continue
                if not self._closed_trade_details_retry_due(position.position_id, now_monotonic):
                    pending_count += 1
                    continue
                broker_sync = self._get_broker_close_sync(
                    position,
                    context="closed trade details backfill",
                    wait_timeout_sec=2.0,
                )
                if not isinstance(broker_sync, dict):
                    broker_sync = None
                close_details = self._resolved_close_details(position, broker_sync)
                close_price = self._finite_float_or_none(close_details.get("close_price"))
                if close_price is not None and close_price <= 0:
                    close_price = None
                closed_at = self._finite_float_or_none(close_details.get("closed_at"))
                if closed_at is not None and closed_at > 0:
                    closed_at = min(now_ts, closed_at)
                else:
                    closed_at = None
                pnl_value = self._finite_float_or_none(close_details.get("pnl"))
                pnl_is_estimated = bool(close_details.get("pnl_is_estimated")) if pnl_value is not None else None
                pnl_estimate_source = close_details.get("pnl_estimate_source")
                has_realized_pnl = bool(close_details.get("has_realized_pnl"))
                if close_price is None and closed_at is None and pnl_value is None:
                    pending_count += 1
                    self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                    continue
                if (
                    has_realized_pnl
                    and pnl_value is not None
                    and pnl_existing is not None
                    and abs(float(pnl_value) - float(pnl_existing)) > self._close_reconcile_pnl_alert_threshold
                ):
                    mismatch_alert_count += 1
                    self.store.record_event(
                        "WARN",
                        position.symbol,
                        "Trade pnl mismatch vs broker sync",
                        {
                            "position_id": position.position_id,
                            "local_pnl": float(pnl_existing),
                            "broker_pnl": float(pnl_value),
                            "pnl_abs_diff": round(abs(float(pnl_value) - float(pnl_existing)), 6),
                            "threshold": self._close_reconcile_pnl_alert_threshold,
                            "source": str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync",
                        },
                    )
                changed = False
                if close_price is not None and (
                    close_price_existing is None
                    or abs(float(close_price_existing) - float(close_price)) > max(FLOAT_COMPARISON_TOLERANCE, abs(float(close_price)) * FLOAT_COMPARISON_TOLERANCE)
                ):
                    changed = True
                if closed_at is not None and (
                    closed_at_existing is None or abs(float(closed_at_existing) - float(closed_at)) > 1.0
                ):
                    changed = True
                if pnl_value is not None and (
                    pnl_existing is None or abs(float(pnl_existing) - float(pnl_value)) > FLOAT_COMPARISON_TOLERANCE
                ):
                    changed = True
                existing_pnl_estimated = bool(getattr(position, "pnl_is_estimated", False))
                existing_pnl_estimate_source = str(getattr(position, "pnl_estimate_source", "") or "").strip() or None
                if pnl_is_estimated is not None and existing_pnl_estimated != pnl_is_estimated:
                    changed = True
                if pnl_is_estimated and existing_pnl_estimate_source != (str(pnl_estimate_source or "").strip() or None):
                    changed = True
                incomplete_monetary_close = not has_realized_pnl
                if not changed:
                    if incomplete_monetary_close:
                        pending_count += 1
                        self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                    continue
                self.store.update_trade_status(
                    position.position_id,
                    status="closed",
                    close_price=close_price,
                    closed_at=closed_at,
                    pnl=pnl_value,
                    pnl_is_estimated=pnl_is_estimated,
                    pnl_estimate_source=pnl_estimate_source,
                )
                self.store.finalize_trade_performance(
                    position_id=position.position_id,
                    symbol=position.symbol,
                    closed_at=closed_at,
                    close_reason="closed_trade_backfill",
                )
                source = str((broker_sync or {}).get("source") or "").strip().lower()
                if not source:
                    source = str(pnl_estimate_source or "").strip().lower() or "event_repair"
                self.store.record_event(
                    "INFO",
                    position.symbol,
                    (
                        "Closed trade details backfilled from broker sync"
                        if isinstance(broker_sync, dict)
                        else "Closed trade details repaired from stored close events"
                    ),
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "broker_close_sync": broker_sync,
                        "pnl": pnl_value,
                        "pnl_is_estimated": pnl_is_estimated,
                        "pnl_estimate_source": pnl_estimate_source,
                    },
                )
                if incomplete_monetary_close:
                    pending_count += 1
                    self._schedule_closed_trade_details_retry(position.position_id, now_monotonic)
                else:
                    self._closed_trade_details_retry_after_monotonic.pop(position.position_id, None)
                    self._closed_trade_details_retry_backoff_sec.pop(position.position_id, None)
                pass_reconciled += 1

            reconciled_count_total += pass_reconciled
            if not self._close_reconcile_enabled or pending_count <= 0 or pass_reconciled <= 0:
                break

        if reconciled_count_total > 0:
            self.store.record_event(
                "INFO",
                None,
                "Closed trade details backfilled",
                {"count": reconciled_count_total, "mode": self.config.mode.value},
            )
        if reconciled_count_total > 0 or mismatch_alert_count > 0:
            self.store.record_event(
                "INFO" if mismatch_alert_count == 0 else "WARN",
                None,
                "Closed trade reconcile summary",
                {
                    "count": reconciled_count_total,
                    "pnl_mismatch_alerts": mismatch_alert_count,
                    "mode": self.config.mode.value,
                },
            )
        return reconciled_count_total

    def _closed_trade_details_retry_due(self, position_id: str, now_monotonic: float) -> bool:
        retry_after = float(self._closed_trade_details_retry_after_monotonic.get(str(position_id), 0.0))
        return retry_after <= 0.0 or now_monotonic + FLOAT_COMPARISON_TOLERANCE >= retry_after

    def _schedule_closed_trade_details_retry(self, position_id: str, now_monotonic: float) -> None:
        key = str(position_id)
        base_backoff = max(
            self._runtime_closed_details_backfill_interval_sec,
            self._runtime_broker_sync_active_interval_sec,
            60.0,
        )
        previous = float(self._closed_trade_details_retry_backoff_sec.get(key, 0.0))
        next_backoff = base_backoff if previous <= 0.0 else min(900.0, previous * 2.0)
        self._closed_trade_details_retry_backoff_sec[key] = next_backoff
        self._closed_trade_details_retry_after_monotonic[key] = now_monotonic + next_backoff

    @staticmethod
    def _is_allowance_related_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return (
            "allowance cooldown is active" in lowered
            or "critical_trade_operation_active" in lowered
            or "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )

    @staticmethod
    def _slug_reason(value: str, fallback: str = "unknown") -> str:
        text = str(value or "").strip().lower()
        if not text:
            return fallback

        parts: list[str] = []
        pending_sep = False
        for char in text:
            if char.isalnum():
                if pending_sep and parts:
                    parts.append("_")
                parts.append(char)
                pending_sep = False
            else:
                pending_sep = True

        normalized = "".join(parts).strip("_")
        if not normalized:
            return fallback
        return normalized[:120].rstrip("_")

    def _extract_ig_error_code(self, error_text: str) -> str | None:
        marker = '"errorCode":"'
        start = error_text.find(marker)
        if start < 0:
            return None
        start += len(marker)
        end = error_text.find('"', start)
        if end <= start:
            return None
        raw_code = error_text[start:end].strip()
        if not raw_code:
            return None
        return self._slug_reason(raw_code, fallback="unknown")

    def _extract_ig_api_endpoint(self, error_text: str) -> tuple[str, str] | None:
        marker = "IG API "
        start = error_text.find(marker)
        if start < 0:
            return None
        request_text = error_text[start + len(marker) :].strip()
        if not request_text:
            return None
        parts = request_text.split()
        if len(parts) < 2:
            return None
        method = self._slug_reason(parts[0], fallback="request")
        raw_path = parts[1].strip()
        segments = [segment for segment in raw_path.split("/") if segment]
        if not segments:
            return method, "root"
        resource = self._slug_reason(segments[0], fallback="unknown")
        if resource in {"positions", "workingorders"} and len(segments) > 1 and segments[1].lower() == "otc":
            resource = f"{resource}_otc"
        return method, resource

    def _normalize_broker_error_reason(self, error_text: str) -> str:
        text = str(error_text or "").strip()
        if not text:
            return "broker_error_unknown"
        lowered = text.lower()
        if lowered.startswith("ig deal rejected:"):
            tail = text.split(":", 1)[1].strip()
            reject_reason = tail.split("|", 1)[0].strip()
            return f"deal_rejected:{self._slug_reason(reject_reason, fallback='unknown')}"
        if "requested size below broker minimum" in lowered:
            return "requested_size_below_broker_minimum"
        if "allowance cooldown is active" in lowered:
            return "allowance_cooldown_active"
        endpoint = self._extract_ig_api_endpoint(text)
        if endpoint is not None:
            method, resource = endpoint
            error_code = self._extract_ig_error_code(text)
            if error_code:
                return f"ig_api_{method}_{resource}:{error_code}"
            return f"ig_api_{method}_{resource}:failed"
        return self._slug_reason(text, fallback="broker_error_unknown")

    def _event_reason(self, event: dict[str, object]) -> tuple[str, str] | None:
        message = str(event.get("message") or "")
        payload_raw = event.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        if message == "Trade blocked by risk manager":
            return "block", str(payload.get("reason") or "risk_manager")
        if message == "Trade blocked by spread filter":
            return "block", "spread_too_wide"
        if message == "Trade blocked by confidence threshold":
            return "block", "confidence_below_threshold"
        if message == "Trade blocked by entry cooldown":
            return "block", "entry_cooldown"
        if message == "Trade blocked by connectivity check":
            return "block", str(payload.get("reason") or "connectivity_check_failed")
        if message == "Trade blocked by stream health check":
            return "block", str(payload.get("reason") or "stream_health_degraded")
        if message == "Broker allowance backoff active":
            kind = str(payload.get("kind") or "").strip()
            if kind:
                return "block", f"allowance:{self._slug_reason(kind, fallback='unknown')}"
            error_text = str(payload.get("error") or "")
            return "block", f"allowance:{self._normalize_broker_error_reason(error_text)}"
        if message == "Broker error":
            error_text = str(payload.get("error") or "")
            return "reject", self._normalize_broker_error_reason(error_text)
        if message == "Signal hold reason":
            return "hold", str(payload.get("reason") or "unknown")
        return None

    def _maybe_record_trade_reason_summary(self, now_monotonic: float, *, force: bool = False) -> None:
        if not self._trade_reason_summary_enabled:
            return
        if (
            not force
            and (now_monotonic - self._last_trade_reason_summary_monotonic) < self._trade_reason_summary_interval_sec
        ):
            return
        self._last_trade_reason_summary_monotonic = now_monotonic
        since_ts = time.time() - self._trade_reason_summary_window_sec
        try:
            events = self.store.load_events_since(since_ts, limit=self._trade_reason_summary_scan_limit)
            aggregates: dict[tuple[str, str], dict[str, object]] = {}
            matched = 0
            for event in events:
                reason_key = self._event_reason(event)
                if reason_key is None:
                    continue
                matched += 1
                kind, reason = reason_key
                key = (kind, reason)
                bucket = aggregates.get(key)
                if bucket is None:
                    bucket = {
                        "count": 0,
                        "last_ts": 0.0,
                        "symbols": set(),
                    }
                    aggregates[key] = bucket
                bucket["count"] = int(bucket["count"]) + 1
                bucket["last_ts"] = max(float(bucket["last_ts"]), float(event.get("ts") or 0.0))
                symbol = str(event.get("symbol") or "").strip().upper()
                if symbol:
                    symbols = bucket["symbols"]
                    if isinstance(symbols, set):
                        symbols.add(symbol)

            if matched == 0:
                return

            rows: list[dict[str, object]] = []
            by_kind: dict[str, int] = {}
            for (kind, reason), bucket in aggregates.items():
                count = int(bucket["count"])
                by_kind[kind] = by_kind.get(kind, 0) + count
                symbols_raw = bucket.get("symbols")
                symbols = sorted(str(item) for item in symbols_raw) if isinstance(symbols_raw, set) else []
                rows.append(
                    {
                        "kind": kind,
                        "reason": reason,
                        "count": count,
                        "last_ts": float(bucket["last_ts"]),
                        "symbols": symbols[:10],
                    }
                )
            rows.sort(key=lambda item: (-int(item["count"]), -float(item["last_ts"]), str(item["kind"]), str(item["reason"])))
            top_rows = rows[: self._trade_reason_summary_top]
            top_text = ", ".join(
                f"{row['kind']}:{row['reason']}={int(row['count'])}"
                for row in top_rows
            )
            logger.info(
                "Trade reason summary snapshot | window_sec=%d scanned=%d matched=%d unique=%d by_kind=%s top=%s",
                int(self._trade_reason_summary_window_sec),
                len(events),
                matched,
                len(rows),
                by_kind,
                top_text or "-",
            )
            self.store.record_event(
                "INFO",
                None,
                "Trade reason summary snapshot",
                {
                    "window_sec": int(self._trade_reason_summary_window_sec),
                    "scanned_events": len(events),
                    "matched_events": matched,
                    "unique_reasons": len(rows),
                    "by_kind": by_kind,
                    "top_reasons": top_rows,
                },
            )
        except Exception as exc:
            if force or (now_monotonic - self._last_trade_reason_summary_error_monotonic) >= 60.0:
                logger.warning("Trade reason summary snapshot failed: %s", exc)
                self._last_trade_reason_summary_error_monotonic = now_monotonic

    def _refresh_passive_price_history(self, force: bool = False) -> None:
        if self.stop_event.is_set():
            return
        if not self._passive_history_refresh_lock.acquire(blocking=False):
            return
        loop_start_monotonic = time.monotonic()
        try:
            poll_interval_sec = 0.0 if force else self._passive_history_poll_interval_sec
            with self._workers_lock:
                worker_items = list(self.workers.items())
            active_symbols = {symbol for symbol, worker in worker_items if worker.is_alive()}
            symbols = self._symbols_to_run()
            if not symbols:
                return
            total_symbols = len(symbols)
            start_idx = self._passive_history_cursor % total_symbols
            ordered_symbols = symbols[start_idx:] + symbols[:start_idx]
            force_deadline_monotonic = (
                loop_start_monotonic + max(0.5, min(3.0, self._passive_history_poll_interval_sec))
                if force
                else None
            )
            max_fetch_attempts = total_symbols if force else min(total_symbols, self._passive_history_max_symbols_per_cycle)
            fetch_attempts = 0

            for symbol in ordered_symbols:
                now_monotonic = time.monotonic()
                if force_deadline_monotonic is not None and now_monotonic >= force_deadline_monotonic:
                    break
                backoff_remaining = self._broker_public_api_backoff_remaining_sec()
                if backoff_remaining > 0.0:
                    if force:
                        break
                    return
                # Startup warmup must not block on broker-side REST pacing.
                if force and self._broker_market_data_wait_remaining_sec() > 0.0:
                    break

                if symbol in active_symbols:
                    continue
                last_sample_monotonic = self._last_passive_history_sample_monotonic_by_symbol.get(symbol, 0.0)
                if (now_monotonic - last_sample_monotonic) < poll_interval_sec:
                    continue
                keep_rows = self._price_history_keep_rows_for_symbol(symbol)
                if not self._reserve_ig_non_trading_budget(
                    scope="passive_history_refresh",
                    wait_timeout_sec=0.2 if force else 0.0,
                ):
                    if force:
                        break
                    return
                try:
                    tick = self.broker.get_price(symbol)
                    fetch_attempts += 1
                except Exception as exc:
                    fetch_attempts += 1
                    error_text = str(exc)
                    if self._is_allowance_related_error(error_text):
                        self._last_passive_history_error_monotonic_by_symbol[symbol] = now_monotonic
                        if force:
                            break
                        return
                    last_error_monotonic = self._last_passive_history_error_monotonic_by_symbol.get(symbol, 0.0)
                    if force or (now_monotonic - last_error_monotonic) >= max(60.0, self._passive_history_poll_interval_sec * 10.0):
                        logger.warning(
                            "Passive history refresh failed for %s: %s",
                            symbol,
                            exc,
                        )
                    self._last_passive_history_error_monotonic_by_symbol[symbol] = now_monotonic
                    continue

                now_ts = time.time()
                timestamp = self._normalize_timestamp_seconds(getattr(tick, "timestamp", now_ts))
                max_future_skew_sec = 120.0
                if timestamp > (now_ts + max_future_skew_sec):
                    timestamp = now_ts
                last_ts = self._last_passive_history_ts_by_symbol.get(symbol)
                if last_ts is not None:
                    if last_ts > (now_ts + max_future_skew_sec):
                        last_ts = now_ts
                    if timestamp <= (last_ts + FLOAT_COMPARISON_TOLERANCE):
                        # Broker timestamp can stall for inactive symbols; keep history monotonic.
                        timestamp = max(now_ts, last_ts + 1e-3)
                self._last_passive_history_ts_by_symbol[symbol] = timestamp
                current_volume: float | None = None
                volume_raw = getattr(tick, "volume", None)
                if volume_raw is not None:
                    try:
                        parsed_volume = float(volume_raw)
                    except (TypeError, ValueError):
                        parsed_volume = 0.0
                    if parsed_volume > 0:
                        current_volume = parsed_volume

                self.store.append_price_sample(
                    symbol=symbol,
                    ts=timestamp,
                    close=float(tick.mid),
                    volume=current_volume,
                    max_rows_per_symbol=keep_rows,
                    candle_resolutions_sec=self._candle_history_resolutions_for_symbol(symbol),
                )
                self._last_passive_history_sample_monotonic_by_symbol[symbol] = now_monotonic
                self._last_passive_history_error_monotonic_by_symbol.pop(symbol, None)
                if not force and fetch_attempts >= max_fetch_attempts:
                    break

            cursor_advance = max(1, fetch_attempts)
            self._passive_history_cursor = (start_idx + cursor_advance) % total_symbols
        finally:
            self._passive_history_refresh_lock.release()

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
        if self.position_book.count() > 0:
            return True
        if self.config.mode != RunMode.EXECUTION:
            return False
        if self.config.broker != "ig":
            return False
        try:
            return bool(self.store.load_open_positions(mode=self.config.mode.value))
        except Exception:
            return False

    def _runtime_sync_interval_sec(
        self,
        *,
        has_local_open_positions: bool | None = None,
        has_pending_opens: bool = False,
        oldest_open_age_sec: float | None = None,
    ) -> float:
        if has_local_open_positions is None:
            has_local_open_positions = self._has_local_open_execution_positions()
        if bool(has_local_open_positions) or bool(has_pending_opens):
            if has_pending_opens:
                return self._runtime_broker_sync_active_interval_sec
            age_sec = float(oldest_open_age_sec or 0.0)
            if age_sec >= 8.0 * 60.0 * 60.0:
                return 120.0
            if age_sec >= 2.0 * 60.0 * 60.0:
                return 10.0
            if age_sec >= 30.0 * 60.0:
                return 3.0
            return self._runtime_broker_sync_active_interval_sec
        return self._runtime_broker_sync_idle_interval_sec

    def _runtime_backfill_missing_on_broker_trades(self, force: bool = False) -> int:
        if self.stop_event.is_set():
            return 0
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return 0
        now_monotonic = time.monotonic()
        if (
            not force
            and (now_monotonic - self._last_runtime_missing_backfill_monotonic)
            < self._runtime_missing_backfill_interval_sec
        ):
            return 0
        self._last_runtime_missing_backfill_monotonic = now_monotonic
        return self._backfill_missing_on_broker_trades()

    def _runtime_backfill_closed_trade_details(self, force: bool = False) -> int:
        if self.stop_event.is_set():
            return 0
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return 0
        now_monotonic = time.monotonic()
        if (
            not force
            and (now_monotonic - self._last_runtime_closed_details_backfill_monotonic)
            < self._runtime_closed_details_backfill_interval_sec
        ):
            return 0
        self._last_runtime_closed_details_backfill_monotonic = now_monotonic
        return self._backfill_closed_trade_details()

    def _flush_ig_rate_limit_worker_metrics(self, now_monotonic: float) -> None:
        self._ig_budget.flush_worker_metrics(now_monotonic)

    def _runtime_sync_open_positions(self, force: bool = False) -> None:
        self._pulse_runtime_monitor_progress()
        if self.stop_event.is_set():
            return
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return

        now_monotonic = time.monotonic()
        preferred_symbols = self._symbols_to_run()
        pending_opens = self.store.load_pending_opens(mode=self.config.mode.value)
        local_store_positions = self.store.load_open_positions(mode=self.config.mode.value)
        local_by_id: dict[str, Position] = {}
        for position in local_store_positions.values():
            local_by_id[str(position.position_id)] = position
        for position in self.position_book.all_open():
            local_by_id.setdefault(str(position.position_id), position)
        oldest_open_age_sec: float | None = None
        if local_by_id:
            now_ts = time.time()
            oldest_opened_at: float | None = None
            for position in local_by_id.values():
                opened_at = self._finite_float_or_none(getattr(position, "opened_at", None))
                if opened_at is None or opened_at <= 0:
                    continue
                if oldest_opened_at is None or opened_at < oldest_opened_at:
                    oldest_opened_at = opened_at
            if oldest_opened_at is not None:
                oldest_open_age_sec = max(0.0, now_ts - oldest_opened_at)
        runtime_sync_interval_sec = self._runtime_sync_interval_sec(
            has_local_open_positions=bool(local_by_id),
            has_pending_opens=bool(pending_opens),
            oldest_open_age_sec=oldest_open_age_sec,
        )
        if not force and (now_monotonic - self._last_runtime_broker_sync_monotonic) < runtime_sync_interval_sec:
            return

        if not force and self._broker_public_api_backoff_remaining_sec() > 0:
            return

        self._last_runtime_broker_sync_monotonic = now_monotonic

        known_position_ids = list(local_by_id.keys())
        known_position_ids.extend(
            str(pending.position_id)
            for pending in pending_opens
            if pending.position_id and str(pending.position_id).strip()
        )

        known_deal_references = self.store.load_open_trade_deal_references(mode=self.config.mode.value)
        known_deal_references.extend(
            str(pending.pending_id)
            for pending in pending_opens
            if str(pending.pending_id).strip()
        )
        known_deal_references = list(dict.fromkeys(known_deal_references))
        known_position_ids = list(dict.fromkeys([value for value in known_position_ids if str(value).strip()]))

        if not self._reserve_ig_non_trading_budget(
            scope="runtime_managed_open_positions",
            wait_timeout_sec=0.2,
        ):
            return
        self._pulse_runtime_monitor_progress()
        try:
            broker_restored = self.broker.get_managed_open_positions(
                self.bot_magic_prefix,
                self.bot_magic_instance,
                preferred_symbols=preferred_symbols,
                known_deal_references=known_deal_references,
                known_position_ids=known_position_ids,
                pending_opens=pending_opens,
                include_unmatched_preferred=True,
            )
        except Exception as exc:
            self._pulse_runtime_monitor_progress()
            error_text = str(exc)
            if self._is_allowance_related_error(error_text):
                if force or (
                    now_monotonic - self._last_runtime_broker_sync_error_monotonic
                ) >= max(60.0, runtime_sync_interval_sec):
                    logger.info("Runtime broker open-position sync deferred by allowance backoff: %s", exc)
                    self.store.record_event(
                        "WARN",
                        None,
                        "Runtime broker open-position sync deferred by allowance backoff",
                        {"error": error_text, "mode": self.config.mode.value},
                    )
                self._last_runtime_broker_sync_error_monotonic = now_monotonic
                return
            if force or (
                now_monotonic - self._last_runtime_broker_sync_error_monotonic
            ) >= max(60.0, runtime_sync_interval_sec):
                logger.warning("Runtime broker open-position sync failed: %s", exc)
                self.store.record_event(
                    "WARN",
                    None,
                    "Runtime broker open-position sync failed",
                    {"error": error_text, "mode": self.config.mode.value},
                )
            self._last_runtime_broker_sync_error_monotonic = now_monotonic
            return

        self._pulse_runtime_monitor_progress()
        self._last_runtime_broker_sync_error_monotonic = 0.0
        broker_restored = self._filter_restored_positions_for_mode(broker_restored)
        broker_ids = {
            str(position.position_id).strip()
            for position in broker_restored.values()
            if str(position.position_id).strip()
        }
        stale_local_count = 0
        if local_by_id:
            now_ts = time.time()
            for position_id, local_position in list(local_by_id.items()):
                self._pulse_runtime_monitor_progress()
                normalized_position_id = str(position_id).strip()
                if not normalized_position_id or normalized_position_id in broker_ids:
                    continue
                symbol = str(local_position.symbol).strip().upper() or None
                if self._reconcile_missing_local_position_from_broker_sync(
                    local_position,
                    context="runtime sync",
                    now_ts=now_ts,
                ):
                    self.position_book.remove_by_id(normalized_position_id)
                    local_by_id.pop(position_id, None)
                    stale_local_count += 1
                    continue
                self.store.update_trade_status(
                    normalized_position_id,
                    status="missing_on_broker",
                    closed_at=now_ts,
                )
                missing_existing_perf = self.store.load_trade_performance(normalized_position_id)
                missing_existing_reason = str((missing_existing_perf or {}).get("close_reason") or "").strip() or None
                missing_close_reason = (
                    f"close_verified:{missing_existing_reason}:runtime_sync"
                    if missing_existing_reason
                    else "runtime_sync:missing_on_broker"
                )
                self.store.finalize_trade_performance(
                    position_id=normalized_position_id,
                    symbol=local_position.symbol,
                    closed_at=now_ts,
                    close_reason=missing_close_reason,
                )
                self.position_book.remove_by_id(normalized_position_id)
                local_by_id.pop(position_id, None)
                stale_local_count += 1
                logger.warning(
                    "Local open position missing on broker during runtime sync | symbol=%s position_id=%s",
                    symbol or "-",
                    normalized_position_id,
                )
                self.store.record_event(
                    "WARN",
                    symbol,
                    "Local open position missing on broker during runtime sync",
                    {
                        "position_id": normalized_position_id,
                        "mode": self.config.mode.value,
                    },
                )
        if stale_local_count > 0:
            self.store.record_event(
                "WARN",
                None,
                "Runtime broker open-position sync marked stale local positions",
                {"count": stale_local_count, "mode": self.config.mode.value},
            )
            # Try to enrich newly missing positions with factual close details immediately.
            self._runtime_backfill_missing_on_broker_trades(force=True)
        if not broker_restored:
            return

        desired_assignments = self._schedule_assignments(self._now_utc())
        recovered_count = 0
        for position in broker_restored.values():
            self._pulse_runtime_monitor_progress()
            symbol = str(position.symbol).strip().upper()
            position_id = str(position.position_id).strip()
            if not position_id:
                continue
            if position_id in local_by_id:
                # Keep in-memory book aligned with persisted state when possible.
                active = self.position_book.get_by_id(position_id)
                if active is None:
                    self.position_book.upsert(position)
                continue

            existing_row = self.store.get_trade_record(position_id)
            matched_pending = self._match_pending_open(position, pending_opens)

            default_assignment = desired_assignments.get(symbol)
            (
                thread_name,
                strategy,
                strategy_entry,
                strategy_entry_component,
                strategy_entry_signal,
                mode,
            ) = self._resolved_recovery_trade_identity(
                symbol=symbol,
                existing_row=existing_row,
                matched_pending=matched_pending,
                default_assignment=default_assignment,
            )
            self._apply_position_trade_identity(
                position,
                strategy=strategy,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )

            self.store.upsert_trade(
                position,
                thread_name,
                strategy,
                mode,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            if matched_pending is not None:
                conflicting_position_id = self.store.bind_trade_deal_reference(
                    position.position_id,
                    matched_pending.pending_id,
                )
                if conflicting_position_id:
                    self.store.record_event(
                        "ERROR",
                        symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position.position_id,
                            "deal_reference": matched_pending.pending_id,
                            "conflicting_position_id": conflicting_position_id,
                            "context": "runtime_reconcile",
                        },
                    )
                self._restore_pending_trailing_override(matched_pending, position.position_id)
                self.store.delete_pending_open(matched_pending.pending_id)

            self.position_book.upsert(position)
            local_by_id[position_id] = position
            recovered_count += 1
            self.store.record_event(
                "WARN",
                symbol,
                "Recovered broker-managed open position during runtime sync",
                {
                    **self._strategy_event_payload(
                        strategy,
                        (
                            default_assignment.strategy_params
                            if default_assignment is not None and str(default_assignment.strategy_name).strip().lower() == str(strategy).strip().lower()
                            else None
                        ),
                        strategy_entry_hint=strategy_entry,
                    ),
                    "position_id": position.position_id,
                    "strategy_entry": strategy_entry,
                    "strategy_entry_component": strategy_entry_component,
                    "strategy_entry_signal": strategy_entry_signal,
                    "mode": self.config.mode.value,
                    "recovered_from_pending_open": matched_pending is not None,
                },
            )

        if recovered_count > 0:
            logger.warning(
                "Runtime broker open-position sync recovered %d positions",
                recovered_count,
            )
            self.store.record_event(
                "WARN",
                None,
                "Runtime broker open-position sync recovered positions",
                {"count": recovered_count, "mode": self.config.mode.value},
            )

    def _filter_restored_positions_for_mode(self, restored: dict[str, Position]) -> dict[str, Position]:
        if self.config.mode != RunMode.EXECUTION:
            return restored

        filtered: dict[str, Position] = {}
        for position in restored.values():
            symbol = str(position.symbol).strip().upper()
            position_id = str(position.position_id or "")
            lowered = position_id.lower()
            if lowered.startswith("paper-") or lowered.startswith("mock-"):
                logger.warning(
                    "Skipping incompatible restored position in execution mode | symbol=%s position_id=%s",
                    symbol,
                    position_id,
                )
                self.store.record_event(
                    "WARN",
                    symbol,
                    "Skipped incompatible restored position in execution mode",
                    {"position_id": position_id, "mode": self.config.mode.value},
                )
                continue
            filtered[position_id] = position
        return filtered

    def _match_pending_open(self, position: Position, pending_opens: list[PendingOpen]) -> PendingOpen | None:
        exact_matches = [
            pending
            for pending in pending_opens
            if pending.position_id and str(pending.position_id) == str(position.position_id)
        ]
        if len(exact_matches) == 1:
            return exact_matches[0]
        return None

    def _restore_pending_trailing_override(self, pending: PendingOpen, position_id: str) -> None:
        if not pending.trailing_override:
            return
        payload = {
            "position_id": position_id,
            "override": pending.trailing_override,
        }
        self.store.set_kv(f"worker.trailing_override.{pending.symbol}", json.dumps(payload))

    def _sync_execution_positions_from_broker(
        self,
        local_restored: dict[str, Position],
    ) -> tuple[dict[str, Position] | None, dict[str, object]]:
        summary: dict[str, object] = {
            "broker_sync_used": False,
            "broker_sync_status": "skipped",
            "source": "state_store",
            "broker_open_count": 0,
            "recovered_count": 0,
            "stale_local_count": 0,
            "pending_open_count": 0,
            "resolved_pending_count": 0,
        }
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return None, summary

        preferred_symbols = self._symbols_to_run()
        desired_assignments = self._schedule_assignments(self._now_utc())
        pending_opens = self.store.load_pending_opens(mode=self.config.mode.value)
        if self._broker_public_api_backoff_remaining_sec() > 0:
            summary["broker_sync_status"] = "deferred_allowance_backoff"
            summary["source"] = "state_store"
            summary["pending_open_count"] = len(pending_opens)
            return None, summary
        try:
            known_position_ids = [position.position_id for position in local_restored.values() if str(position.position_id).strip()]
            known_position_ids.extend(
                str(pending.position_id)
                for pending in pending_opens
                if pending.position_id and str(pending.position_id).strip()
            )
            known_deal_references = self.store.load_open_trade_deal_references(mode=self.config.mode.value)
            known_deal_references.extend(
                str(pending.pending_id)
                for pending in pending_opens
                if str(pending.pending_id).strip()
            )
            known_deal_references = list(dict.fromkeys(known_deal_references))
            if not self._reserve_ig_non_trading_budget(
                scope="startup_managed_open_positions",
                wait_timeout_sec=0.3,
            ):
                summary["broker_sync_status"] = "deferred_local_non_trading_budget"
                summary["source"] = "state_store"
                summary["pending_open_count"] = len(pending_opens)
                return None, summary
            broker_restored = self.broker.get_managed_open_positions(
                self.bot_magic_prefix,
                self.bot_magic_instance,
                preferred_symbols=preferred_symbols,
                known_deal_references=known_deal_references,
                known_position_ids=known_position_ids,
                pending_opens=pending_opens,
                include_unmatched_preferred=True,
            )
        except Exception as exc:
            error_text = str(exc)
            if self._is_allowance_related_error(error_text):
                logger.info(
                    "Managed broker position sync deferred on startup due to allowance backoff, using local state fallback: %s",
                    exc,
                )
                self.store.record_event(
                    "WARN",
                    None,
                    "Managed broker position sync deferred on startup due to allowance backoff",
                    {"mode": self.config.mode.value, "error": error_text},
                )
                summary["broker_sync_status"] = "deferred_allowance_backoff"
                summary["source"] = "state_store"
                summary["error"] = error_text
                summary["pending_open_count"] = len(pending_opens)
                return None, summary
            logger.warning("Managed broker position sync failed on startup, using local state fallback: %s", exc)
            self.store.record_event(
                "WARN",
                None,
                "Managed broker position sync failed on startup",
                {"mode": self.config.mode.value, "error": error_text},
            )
            summary["broker_sync_status"] = "failed_fallback_local"
            summary["source"] = "state_store"
            summary["error"] = error_text
            summary["pending_open_count"] = len(pending_opens)
            return None, summary

        broker_restored = self._filter_restored_positions_for_mode(broker_restored)
        local_by_position_id = {position.position_id: position for position in local_restored.values()}
        broker_ids = {position.position_id for position in broker_restored.values()}
        matched_pending_ids: set[str] = set()
        now_ts = time.time()
        recovered_count = 0
        stale_local_count = 0

        for position in broker_restored.values():
            symbol = str(position.symbol).strip().upper()
            existing_row = self.store.get_trade_record(position.position_id)
            existing_position = local_by_position_id.get(position.position_id)
            matched_pending = self._match_pending_open(position, pending_opens)
            if existing_position is not None and position.entry_confidence <= 0 and existing_position.entry_confidence > 0:
                position.entry_confidence = existing_position.entry_confidence
            if matched_pending is not None and position.entry_confidence <= 0 and matched_pending.entry_confidence > 0:
                position.entry_confidence = matched_pending.entry_confidence

            default_assignment = desired_assignments.get(symbol)
            (
                thread_name,
                strategy,
                strategy_entry,
                strategy_entry_component,
                strategy_entry_signal,
                mode,
            ) = self._resolved_recovery_trade_identity(
                symbol=symbol,
                existing_row=existing_row,
                matched_pending=matched_pending,
                default_assignment=default_assignment,
            )
            self._apply_position_trade_identity(
                position,
                strategy=strategy,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            self.store.upsert_trade(
                position,
                thread_name,
                strategy,
                mode,
                strategy_entry=strategy_entry,
                strategy_entry_component=strategy_entry_component,
                strategy_entry_signal=strategy_entry_signal,
            )
            if matched_pending is not None:
                matched_pending_ids.add(matched_pending.pending_id)
                conflicting_position_id = self.store.bind_trade_deal_reference(
                    position.position_id,
                    matched_pending.pending_id,
                )
                if conflicting_position_id:
                    self.store.record_event(
                        "ERROR",
                        symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position.position_id,
                            "deal_reference": matched_pending.pending_id,
                            "conflicting_position_id": conflicting_position_id,
                            "context": "startup_reconcile",
                        },
                    )
                self._restore_pending_trailing_override(matched_pending, position.position_id)
                self.store.delete_pending_open(matched_pending.pending_id)

            if existing_position is None:
                logger.info(
                    "Recovered broker-managed open position on startup | symbol=%s position_id=%s",
                    symbol,
                    position.position_id,
                )
                self.store.record_event(
                    "INFO",
                    symbol,
                    "Recovered broker-managed open position on startup",
                    {
                        **self._strategy_event_payload(
                            strategy,
                            (
                                default_assignment.strategy_params
                                if default_assignment is not None and str(default_assignment.strategy_name).strip().lower() == str(strategy).strip().lower()
                                else None
                            ),
                            strategy_entry_hint=strategy_entry,
                        ),
                        "position_id": position.position_id,
                        "strategy_entry": strategy_entry,
                        "strategy_entry_component": strategy_entry_component,
                        "strategy_entry_signal": strategy_entry_signal,
                        "mode": self.config.mode.value,
                        "recovered_from_pending_open": matched_pending is not None,
                    },
                )
                recovered_count += 1

        for position in local_restored.values():
            symbol = str(position.symbol).strip().upper()
            if position.position_id in broker_ids:
                continue
            if self._reconcile_missing_local_position_from_broker_sync(
                position,
                context="startup sync",
                now_ts=now_ts,
            ):
                stale_local_count += 1
                continue
            self.store.update_trade_status(
                position.position_id,
                status="missing_on_broker",
                closed_at=now_ts,
            )
            self.store.finalize_trade_performance(
                position_id=position.position_id,
                symbol=position.symbol,
                closed_at=now_ts,
                close_reason="startup_sync:missing_on_broker",
            )
            logger.warning(
                "Local open position missing on broker during startup sync | symbol=%s position_id=%s",
                symbol,
                position.position_id,
            )
            self.store.record_event(
                "WARN",
                symbol,
                "Local open position missing on broker during startup sync",
                {"position_id": position.position_id, "mode": self.config.mode.value},
            )
            stale_local_count += 1

        logger.info(
            "Managed broker startup sync complete | restored=%d local_stale=%d mode=%s",
            len(broker_restored),
            stale_local_count,
            self.config.mode.value,
        )
        summary.update(
            {
                "broker_sync_used": True,
                "broker_sync_status": "ok",
                "source": "broker_sync",
                "broker_open_count": len(broker_restored),
                "recovered_count": recovered_count,
                "stale_local_count": stale_local_count,
                "pending_open_count": len(pending_opens),
                "resolved_pending_count": len(matched_pending_ids),
            }
        )
        return broker_restored, summary

    def _ensure_execution_startup_broker_sync_succeeded(self, summary: dict[str, object]) -> None:
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return
        status = str(summary.get("broker_sync_status") or "").strip().lower()
        if status == "ok":
            return
        source = str(summary.get("source") or "unknown").strip() or "unknown"
        error_text = str(summary.get("error") or "").strip()
        payload = {
            "mode": self.config.mode.value,
            "broker": self.config.broker,
            "status": status or "unknown",
            "source": source,
            "error": error_text or None,
            "pending_open_count": int(summary.get("pending_open_count") or 0),
            "local_open_count": int(summary.get("local_open_count") or 0),
        }
        logger.error(
            "Execution startup requires live broker position sync; aborting startup | status=%s source=%s error=%s",
            status or "unknown",
            source,
            error_text or "-",
        )
        self.store.record_event(
            "ERROR",
            None,
            "Execution startup aborted: live broker position sync unavailable",
            payload,
        )
        raise RuntimeError(
            "Execution startup requires live broker position sync "
            f"(status={status or 'unknown'} source={source})"
        )

    def _restore_open_positions(self) -> tuple[dict[str, Position], dict[str, object]]:
        restored_raw = self.store.load_open_positions(mode=self.config.mode.value)
        restored = self._filter_restored_positions_for_mode(restored_raw)
        broker_synced, summary = self._sync_execution_positions_from_broker(restored)
        final_restored = restored if broker_synced is None else broker_synced
        summary.update(
            {
                "mode": self.config.mode.value,
                "broker": self.config.broker,
                "local_open_count": len(restored),
                "final_open_count": len(final_restored),
                "restored_open_count": len(final_restored),
            }
        )
        self._ensure_execution_startup_broker_sync_succeeded(summary)
        return final_restored, summary

    def _startup_fast_path_enabled(self) -> bool:
        return self.config.mode == RunMode.EXECUTION and self._db_first_enabled()

    def _log_open_position_restore_summary(self, summary: dict[str, object]) -> None:
        logger.info(
            "Open position restore summary | source=%s local=%s broker=%s recovered=%s stale=%s final=%s mode=%s",
            summary.get("source", "-"),
            summary.get("local_open_count", 0),
            summary.get("broker_open_count", 0),
            summary.get("recovered_count", 0),
            summary.get("stale_local_count", 0),
            summary.get("final_open_count", 0),
            summary.get("mode", self.config.mode.value),
        )
        self.store.record_event(
            "INFO",
            None,
            "Open position restore summary",
            dict(summary),
        )

    def _backfill_missing_on_broker_trades(self) -> int:
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return 0
        if self._broker_public_api_backoff_remaining_sec() > 0.0:
            return 0
        missing_positions = self.store.load_positions_by_status(
            "missing_on_broker",
            mode=self.config.mode.value,
        )
        if not missing_positions:
            return 0
        now_ts = time.time()
        reconciled_count = 0
        for position in missing_positions.values():
            self._pulse_runtime_monitor_progress()
            if self._reconcile_missing_local_position_from_broker_sync(
                position,
                context="missing_on_broker backfill",
                now_ts=now_ts,
            ):
                reconciled_count += 1
        if reconciled_count > 0:
            self.store.record_event(
                "INFO",
                None,
                "Missing-on-broker trades reconciled as closed",
                {"count": reconciled_count, "mode": self.config.mode.value},
            )
        return reconciled_count

    def sync_open_positions(self) -> dict[str, object]:
        self._connect_broker()
        final_restored, summary = self._restore_open_positions()
        self.position_book.bootstrap(final_restored)
        reconciled_missing_count = self._backfill_missing_on_broker_trades()
        if reconciled_missing_count > 0:
            summary["missing_on_broker_reconciled_count"] = reconciled_missing_count
        reconciled_closed_details_count = self._backfill_closed_trade_details()
        if reconciled_closed_details_count > 0:
            summary["closed_details_reconciled_count"] = reconciled_closed_details_count
        self._log_open_position_restore_summary(summary)
        return summary

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
