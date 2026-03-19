from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from xtb_bot.client import MockBrokerClient, XtbApiClient
from xtb_bot.config import BotConfig
from xtb_bot.ig_client import IgApiClient
from xtb_bot.models import PendingOpen, Position, RunMode
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.strategies import create_strategy
from xtb_bot.worker import SymbolWorker


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerAssignment:
    symbol: str
    strategy_name: str
    strategy_params: dict[str, object]
    source: str = "static"
    label: str | None = None

    def signature(self) -> tuple[str, str, str, str]:
        return (
            self.symbol,
            self.strategy_name,
            self.source,
            json.dumps(self.strategy_params, sort_keys=True, separators=(",", ":"), default=str),
        )


class TradingBot:
    def __init__(self, config: BotConfig):
        self.config = config
        raw_db_timeout_sec = os.getenv("XTB_DB_SQLITE_TIMEOUT_SEC", "30")
        raw_db_cleanup_every = os.getenv("XTB_DB_PRICE_HISTORY_CLEANUP_EVERY", "100")
        raw_db_cleanup_min_interval_sec = os.getenv("XTB_DB_PRICE_HISTORY_CLEANUP_MIN_INTERVAL_SEC", "30")
        raw_db_housekeeping_interval_sec = os.getenv("XTB_DB_HOUSEKEEPING_INTERVAL_SEC", "300")
        raw_db_housekeeping_events_keep_rows = os.getenv("XTB_DB_HOUSEKEEPING_EVENTS_KEEP_ROWS", "50000")
        raw_db_housekeeping_vacuum_pages = os.getenv("XTB_DB_HOUSEKEEPING_VACUUM_PAGES", "256")

        try:
            db_timeout_sec = float(raw_db_timeout_sec)
        except (TypeError, ValueError):
            db_timeout_sec = 30.0
        try:
            db_cleanup_every = int(float(raw_db_cleanup_every))
        except (TypeError, ValueError):
            db_cleanup_every = 100
        try:
            db_cleanup_min_interval_sec = float(raw_db_cleanup_min_interval_sec)
        except (TypeError, ValueError):
            db_cleanup_min_interval_sec = 30.0
        try:
            db_housekeeping_interval_sec = float(raw_db_housekeeping_interval_sec)
        except (TypeError, ValueError):
            db_housekeeping_interval_sec = 300.0
        try:
            db_housekeeping_events_keep_rows = int(float(raw_db_housekeeping_events_keep_rows))
        except (TypeError, ValueError):
            db_housekeeping_events_keep_rows = 50_000
        try:
            db_housekeeping_vacuum_pages = int(float(raw_db_housekeeping_vacuum_pages))
        except (TypeError, ValueError):
            db_housekeeping_vacuum_pages = 256

        self.store = StateStore(
            config.storage_path,
            sqlite_timeout_sec=db_timeout_sec,
            price_history_cleanup_every=db_cleanup_every,
            price_history_cleanup_min_interval_sec=db_cleanup_min_interval_sec,
            housekeeping_interval_sec=db_housekeeping_interval_sec,
            housekeeping_events_keep_rows=db_housekeeping_events_keep_rows,
            housekeeping_incremental_vacuum_pages=db_housekeeping_vacuum_pages,
        )
        self.position_book = PositionBook()
        self.stop_event = threading.Event()
        self.risk = RiskManager(config.risk, self.store)
        self._workers_lock = threading.RLock()
        self.workers: dict[str, SymbolWorker] = {}
        self._worker_stop_events: dict[str, threading.Event] = {}
        self._worker_assignments: dict[str, WorkerAssignment] = {}
        self._deferred_switch_signature_by_symbol: dict[str, tuple[str, str] | None] = {}
        self._resources_closed = False
        self.bot_magic_prefix = config.bot_magic_prefix
        self.bot_magic_instance = self._resolve_bot_magic_instance(config.bot_magic_instance)
        self._strategy_symbol_filter = create_strategy(config.strategy, config.strategy_params)
        self._schedule_timezone = ZoneInfo(config.strategy_schedule_timezone)
        self._history_keep_rows_min = max(100, int(config.price_history_keep_rows_min))
        raw_history_keep_rows_cap = os.getenv("XTB_HISTORY_KEEP_ROWS_CAP", "20000")
        try:
            history_keep_rows_cap = int(float(raw_history_keep_rows_cap))
        except (TypeError, ValueError):
            history_keep_rows_cap = 20000
        self._history_keep_rows_cap = max(self._history_keep_rows_min, history_keep_rows_cap)
        raw_history_ticks_per_candle_cap = os.getenv("XTB_HISTORY_TICKS_PER_CANDLE_CAP", "120")
        try:
            history_ticks_per_candle_cap = int(float(raw_history_ticks_per_candle_cap))
        except (TypeError, ValueError):
            history_ticks_per_candle_cap = 120
        self._history_ticks_per_candle_cap = max(1, history_ticks_per_candle_cap)
        self._history_keep_rows_by_symbol: dict[str, int] = {}
        self._strategy_history_estimate_cache: dict[tuple[str, str], int] = {}
        self._strategy_support_cache: dict[tuple[str, str, str], bool] = {}
        self._passive_history_poll_interval_sec = max(0.5, float(config.passive_history_poll_interval_sec))
        self._passive_history_max_symbols_per_cycle = max(
            1,
            int(float(os.getenv("XTB_PASSIVE_HISTORY_MAX_SYMBOLS_PER_CYCLE", "2"))),
        )
        self._runtime_broker_sync_interval_sec = max(
            15.0,
            float(os.getenv("XTB_RUNTIME_BROKER_SYNC_INTERVAL_SEC", "30")),
        )
        self._last_runtime_broker_sync_monotonic = 0.0
        self._last_runtime_broker_sync_error_monotonic = 0.0
        self._last_db_housekeeping_error_monotonic = 0.0
        raw_trade_reason_summary_enabled = str(os.getenv("XTB_TRADE_REASON_SUMMARY_ENABLED", "1")).strip().lower()
        self._trade_reason_summary_enabled = raw_trade_reason_summary_enabled in {"1", "true", "yes", "on"}
        try:
            trade_reason_summary_interval_sec = float(os.getenv("XTB_TRADE_REASON_SUMMARY_INTERVAL_SEC", "3600"))
        except (TypeError, ValueError):
            trade_reason_summary_interval_sec = 3600.0
        try:
            trade_reason_summary_window_sec = float(os.getenv("XTB_TRADE_REASON_SUMMARY_WINDOW_SEC", "3600"))
        except (TypeError, ValueError):
            trade_reason_summary_window_sec = 3600.0
        try:
            trade_reason_summary_scan_limit = int(float(os.getenv("XTB_TRADE_REASON_SUMMARY_SCAN_LIMIT", "20000")))
        except (TypeError, ValueError):
            trade_reason_summary_scan_limit = 20_000
        try:
            trade_reason_summary_top = int(float(os.getenv("XTB_TRADE_REASON_SUMMARY_TOP", "20")))
        except (TypeError, ValueError):
            trade_reason_summary_top = 20
        self._trade_reason_summary_interval_sec = max(60.0, trade_reason_summary_interval_sec)
        self._trade_reason_summary_window_sec = max(60.0, trade_reason_summary_window_sec)
        self._trade_reason_summary_scan_limit = max(100, trade_reason_summary_scan_limit)
        self._trade_reason_summary_top = max(1, trade_reason_summary_top)
        self._last_trade_reason_summary_monotonic = 0.0
        self._last_trade_reason_summary_error_monotonic = 0.0
        self._passive_history_cursor = 0
        self._last_passive_history_sample_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_error_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_ts_by_symbol: dict[str, float] = {}

        if config.broker == "ig":
            self.broker = IgApiClient(
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
            )
        else:
            self.broker = XtbApiClient(
                user_id=config.user_id,
                password=config.password,
                app_name=config.app_name,
                account_type=config.account_type,
                endpoint=config.endpoint,
            )
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
            if self.config.mode == RunMode.EXECUTION or self.config.strict_broker_connect:
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

    def _strategy_params_for(self, strategy_name: str) -> dict[str, object]:
        params = self.config.strategy_params_map.get(str(strategy_name).strip().lower())
        if params is None:
            if str(strategy_name).strip().lower() == str(self.config.strategy).strip().lower():
                return dict(self.config.strategy_params)
            return dict(self.config.strategy_params)
        return dict(params)

    def _make_worker(self, assignment: WorkerAssignment, stop_event: threading.Event) -> SymbolWorker:
        return SymbolWorker(
            symbol=assignment.symbol,
            mode=self.config.mode,
            strategy_name=assignment.strategy_name,
            strategy_params=assignment.strategy_params,
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
        )

    def _now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _normalize_timestamp_seconds(raw_ts: float | int | str | None) -> float:
        try:
            ts = float(raw_ts)
        except (TypeError, ValueError):
            return time.time()
        if not math.isfinite(ts) or ts <= 0:
            return time.time()
        abs_ts = abs(ts)
        if abs_ts > 10_000_000_000_000:  # microseconds epoch
            return ts / 1_000_000.0
        if abs_ts > 10_000_000_000:  # milliseconds epoch
            return ts / 1_000.0
        return ts

    def _estimate_history_keep_rows(self, strategy_name: str, strategy_params: dict[str, object]) -> int:
        cache_key = (
            str(strategy_name).strip().lower(),
            json.dumps(strategy_params, sort_keys=True, separators=(",", ":"), default=str),
        )
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
                    math.ceil(candle_timeframe_sec / max(self.config.poll_interval_sec, 1e-9)),
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

    def _register_symbol_history_requirement(
        self,
        symbol: str,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> int:
        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            return self._history_keep_rows_min
        required_rows = self._history_keep_rows_min
        try:
            required_rows = self._estimate_history_keep_rows(strategy_name, strategy_params)
        except Exception as exc:
            logger.warning(
                "Failed to estimate history keep rows for %s/%s, using default=%d: %s",
                normalized_symbol,
                strategy_name,
                self._history_keep_rows_min,
                exc,
            )
        current_rows = int(self._history_keep_rows_by_symbol.get(normalized_symbol, self._history_keep_rows_min))
        keep_rows = max(current_rows, required_rows, self._history_keep_rows_min)
        self._history_keep_rows_by_symbol[normalized_symbol] = keep_rows
        return keep_rows

    def _bootstrap_symbol_history_requirements(self) -> None:
        default_strategy_params = self._strategy_params_for(self.config.strategy)
        for symbol in self.config.symbols:
            self._register_symbol_history_requirement(symbol, self.config.strategy, default_strategy_params)
        for entry in self.config.strategy_schedule:
            for symbol in entry.symbols:
                self._register_symbol_history_requirement(symbol, entry.strategy, dict(entry.strategy_params))

    def _price_history_keep_rows_for_symbol(self, symbol: str) -> int:
        normalized_symbol = str(symbol).strip().upper()
        return int(self._history_keep_rows_by_symbol.get(normalized_symbol, self._history_keep_rows_min))

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
        if self.config.force_symbols or self.config.force_strategy or not self.config.strategy_schedule:
            return {
                symbol: WorkerAssignment(
                    symbol=symbol,
                    strategy_name=self.config.strategy,
                    strategy_params=self._strategy_params_for(self.config.strategy),
                    source=(
                        "forced_symbols"
                        if self.config.force_symbols
                        else ("forced" if self.config.force_strategy else "static")
                    ),
                )
                for symbol in self.config.symbols
            }

        ranked: dict[str, tuple[int, int, WorkerAssignment]] = {}
        for index, entry in enumerate(self.config.strategy_schedule):
            if not self._is_schedule_entry_active(current_now, entry):
                continue
            assignment = WorkerAssignment(
                symbol="",
                strategy_name=entry.strategy,
                strategy_params=dict(entry.strategy_params),
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
        strategy_name = str(row.get("strategy") or (fallback.strategy_name if fallback else self.config.strategy)).strip().lower()
        return WorkerAssignment(
            symbol=position.symbol,
            strategy_name=strategy_name,
            strategy_params=self._strategy_params_for(strategy_name),
            source="open_position",
            label=(fallback.label if fallback is not None else None),
        )

    def _target_worker_assignments(self, now_utc: datetime | None = None) -> dict[str, WorkerAssignment]:
        desired = self._schedule_assignments(now_utc)
        for position in self.position_book.all_open():
            desired[position.symbol] = self._assignment_for_open_position(position, desired.get(position.symbol))
        return desired

    def _start_worker_for_assignment(self, assignment: WorkerAssignment) -> None:
        has_restored_position = self.position_book.get(assignment.symbol) is not None
        support_cache_key = (
            str(assignment.strategy_name).strip().lower(),
            json.dumps(assignment.strategy_params, sort_keys=True, separators=(",", ":"), default=str),
            str(assignment.symbol).strip().upper(),
        )
        supported = self._strategy_support_cache.get(support_cache_key)
        if supported is None:
            strategy_filter = create_strategy(assignment.strategy_name, assignment.strategy_params)
            supported = bool(strategy_filter.supports_symbol(assignment.symbol))
            self._strategy_support_cache[support_cache_key] = supported
        if (not has_restored_position) and (not supported):
            logger.warning(
                "Skipping symbol=%s for strategy=%s: unsupported symbol",
                assignment.symbol,
                assignment.strategy_name,
            )
            self.store.record_event(
                "WARN",
                assignment.symbol,
                "Symbol skipped by strategy filter",
                {
                    "strategy": assignment.strategy_name,
                    "reason": "unsupported_symbol",
                },
            )
            return

        history_keep_rows = self._register_symbol_history_requirement(
            assignment.symbol,
            assignment.strategy_name,
            assignment.strategy_params,
        )
        stop_event = threading.Event()
        worker = self._make_worker(assignment, stop_event)
        setattr(worker, "price_history_keep_rows", max(100, int(history_keep_rows)))
        with self._workers_lock:
            self._worker_stop_events[assignment.symbol] = stop_event
            self._worker_assignments[assignment.symbol] = assignment
            self.workers[assignment.symbol] = worker
        try:
            worker.start()
        except Exception:
            with self._workers_lock:
                self.workers.pop(assignment.symbol, None)
                self._worker_stop_events.pop(assignment.symbol, None)
                self._worker_assignments.pop(assignment.symbol, None)
            raise
        logger.info(
            "Started worker for %s | strategy=%s source=%s",
            assignment.symbol,
            assignment.strategy_name,
            assignment.source,
        )

    def _stop_worker_for_symbol(self, symbol: str, reason: str) -> None:
        with self._workers_lock:
            worker = self.workers.pop(symbol, None)
            stop_event = self._worker_stop_events.pop(symbol, None)
            assignment = self._worker_assignments.pop(symbol, None)
            self._deferred_switch_signature_by_symbol.pop(symbol, None)
        if stop_event is not None:
            stop_event.set()
        if worker is not None:
            worker.join(timeout=5.0)
        logger.info("Stopped worker for %s | reason=%s", symbol, reason)
        if assignment is not None:
            self.store.record_event(
                "INFO",
                symbol,
                "Worker stopped",
                {"reason": reason, "strategy": assignment.strategy_name, "source": assignment.source},
            )

    def _record_deferred_switch(self, symbol: str, current: WorkerAssignment, desired: WorkerAssignment) -> None:
        marker = (current.strategy_name, desired.strategy_name)
        with self._workers_lock:
            if self._deferred_switch_signature_by_symbol.get(symbol) == marker:
                return
            self._deferred_switch_signature_by_symbol[symbol] = marker
        logger.info(
            "Deferred strategy switch for %s until open position closes | current=%s next=%s",
            symbol,
            current.strategy_name,
            desired.strategy_name,
        )
        self.store.record_event(
            "INFO",
            symbol,
            "Strategy switch deferred until position closes",
            {"current_strategy": current.strategy_name, "next_strategy": desired.strategy_name},
        )

    def _reconcile_workers(self, now_utc: datetime | None = None) -> None:
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
                    {"strategy": restart_assignment.strategy_name, "previous_strategy": getattr(current_assignment, "strategy_name", None)},
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

            if assignment.signature() == desired_assignment.signature():
                with self._workers_lock:
                    self._deferred_switch_signature_by_symbol.pop(symbol, None)
                continue

            if active_position is not None:
                self._record_deferred_switch(symbol, assignment, desired_assignment)
                continue

            self._stop_worker_for_symbol(symbol, f"strategy_change:{assignment.strategy_name}->{desired_assignment.strategy_name}")
            self._start_worker_for_assignment(desired_assignment)

        for symbol, assignment in desired.items():
            with self._workers_lock:
                exists = symbol in self.workers
            if exists:
                continue
            self._start_worker_for_assignment(assignment)

    def _start_workers(self) -> None:
        self._reconcile_workers()

    def _symbols_to_run(self) -> list[str]:
        symbols: list[str] = []
        seen: set[str] = set()
        scheduled_symbols = [symbol for entry in self.config.strategy_schedule for symbol in entry.symbols]
        for symbol in list(self.config.symbols) + scheduled_symbols + [pos.symbol for pos in self.position_book.all_open()]:
            text = str(symbol).strip().upper()
            if not text or text in seen:
                continue
            seen.add(text)
            symbols.append(text)
        return symbols

    def _broker_public_api_backoff_remaining_sec(self) -> float:
        getter = getattr(self.broker, "get_public_api_backoff_remaining_sec", None)
        if not callable(getter):
            return 0.0
        try:
            remaining = float(getter())
        except Exception:
            return 0.0
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def _broker_market_data_wait_remaining_sec(self) -> float:
        getter = getattr(self.broker, "get_market_data_wait_remaining_sec", None)
        if not callable(getter):
            return 0.0
        try:
            remaining = float(getter())
        except Exception:
            return 0.0
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    @staticmethod
    def _finite_float_or_none(raw: object) -> float | None:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value):
            return None
        return value

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

    def _get_broker_close_sync(self, position: Position, *, context: str) -> dict[str, object] | None:
        getter = getattr(self.broker, "get_position_close_sync", None)
        if not callable(getter):
            return None
        expected_deal_reference = self.store.get_trade_deal_reference(position.position_id)
        try:
            payload = getter(
                position.position_id,
                deal_reference=expected_deal_reference,
                symbol=position.symbol,
                opened_at=position.opened_at,
                open_price=position.open_price,
                volume=position.volume,
                side=position.side.value,
            )
        except TypeError:
            try:
                payload = getter(
                    position.position_id,
                    deal_reference=expected_deal_reference,
                )
            except TypeError:
                try:
                    payload = getter(position.position_id)
                except Exception as exc:
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
            except Exception as exc:
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
        except Exception as exc:
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
        broker_sync = self._get_broker_close_sync(position, context=context)
        if not self._broker_close_sync_has_evidence(broker_sync):
            return False

        close_price = self._finite_float_or_none((broker_sync or {}).get("close_price"))
        if close_price is not None and close_price <= 0:
            close_price = None
        closed_at_raw = self._finite_float_or_none((broker_sync or {}).get("closed_at"))
        closed_at = None
        if closed_at_raw is not None and closed_at_raw > 0:
            closed_at = min(now_ts, closed_at_raw)
        realized_pnl = self._finite_float_or_none((broker_sync or {}).get("realized_pnl"))
        has_close_details = (close_price is not None) or (closed_at is not None) or (realized_pnl is not None)

        if has_close_details:
            self.store.update_trade_status(
                position.position_id,
                status="closed",
                close_price=close_price,
                closed_at=closed_at,
                pnl=realized_pnl,
            )
        else:
            # Position absence on broker confirms closure, but we keep existing close fields untouched
            # when broker history does not provide execution details yet.
            self.store.update_trade_status(
                position.position_id,
                status="closed",
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
        closed_positions = self.store.load_positions_by_status(
            "closed",
            mode=self.config.mode.value,
        )
        if not closed_positions:
            return 0

        now_ts = time.time()
        reconciled_count = 0
        for position in closed_positions.values():
            close_price_existing = self._finite_float_or_none(position.close_price)
            closed_at_existing = self._finite_float_or_none(position.closed_at)
            pnl_existing = self._finite_float_or_none(position.pnl)
            has_good_close_details = (
                close_price_existing is not None
                and close_price_existing > 0
                and closed_at_existing is not None
                and closed_at_existing > 0
            )
            # Historical bug pattern: inferred close was stored as open price/open time
            # while realized PnL came from broker history. Treat this as stale and try
            # to rehydrate real close details.
            looks_like_inferred_open_snapshot = (
                has_good_close_details
                and abs(float(close_price_existing) - float(position.open_price)) <= max(
                    1e-9,
                    abs(float(position.open_price)) * 1e-9,
                )
                and abs(float(closed_at_existing) - float(position.opened_at)) <= 2.0
                and (pnl_existing is not None and abs(float(pnl_existing)) > 1e-9)
            )
            if has_good_close_details and not looks_like_inferred_open_snapshot:
                continue
            broker_sync = self._get_broker_close_sync(position, context="closed trade details backfill")
            if not isinstance(broker_sync, dict):
                continue
            close_price = self._finite_float_or_none(broker_sync.get("close_price"))
            if close_price is not None and close_price <= 0:
                close_price = None
            closed_at = self._finite_float_or_none(broker_sync.get("closed_at"))
            if closed_at is not None and closed_at > 0:
                closed_at = min(now_ts, closed_at)
            else:
                closed_at = None
            realized_pnl = self._finite_float_or_none(broker_sync.get("realized_pnl"))
            if close_price is None and closed_at is None and realized_pnl is None:
                continue
            self.store.update_trade_status(
                position.position_id,
                status="closed",
                close_price=close_price,
                closed_at=closed_at,
                pnl=realized_pnl,
            )
            source = str(broker_sync.get("source") or "").strip().lower() or "broker_sync"
            self.store.record_event(
                "INFO",
                position.symbol,
                "Closed trade details backfilled from broker sync",
                {
                    "position_id": position.position_id,
                    "source": source,
                    "broker_close_sync": broker_sync,
                },
            )
            reconciled_count += 1
        if reconciled_count > 0:
            self.store.record_event(
                "INFO",
                None,
                "Closed trade details backfilled",
                {"count": reconciled_count, "mode": self.config.mode.value},
            )
        return reconciled_count

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
        loop_start_monotonic = time.monotonic()
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
                if timestamp <= (last_ts + 1e-9):
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
            )
            self._last_passive_history_sample_monotonic_by_symbol[symbol] = now_monotonic
            self._last_passive_history_error_monotonic_by_symbol.pop(symbol, None)
            if not force and fetch_attempts >= max_fetch_attempts:
                break

        cursor_advance = max(1, fetch_attempts)
        self._passive_history_cursor = (start_idx + cursor_advance) % total_symbols

    def _monitor_workers(self) -> None:
        if self.stop_event.is_set():
            return
        now_monotonic = time.monotonic()
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
        self._maybe_record_trade_reason_summary(now_monotonic)
        if self._broker_public_api_backoff_remaining_sec() > 0.0:
            self._reconcile_workers()
            return
        self._runtime_sync_open_positions()
        self._reconcile_workers()
        self._refresh_passive_price_history()

    def _runtime_sync_open_positions(self, force: bool = False) -> None:
        if self.stop_event.is_set():
            return
        if self.config.mode != RunMode.EXECUTION or self.config.broker != "ig":
            return

        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_runtime_broker_sync_monotonic) < self._runtime_broker_sync_interval_sec:
            return

        if not force and self._broker_public_api_backoff_remaining_sec() > 0:
            return

        self._last_runtime_broker_sync_monotonic = now_monotonic
        preferred_symbols = self._symbols_to_run()
        pending_opens = self.store.load_pending_opens(mode=self.config.mode.value)
        local_store_positions = self.store.load_open_positions(mode=self.config.mode.value)
        local_by_id: dict[str, Position] = {}
        for position in local_store_positions.values():
            local_by_id[str(position.position_id)] = position
        for position in self.position_book.all_open():
            local_by_id.setdefault(str(position.position_id), position)

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
            error_text = str(exc)
            if self._is_allowance_related_error(error_text):
                if force or (
                    now_monotonic - self._last_runtime_broker_sync_error_monotonic
                ) >= max(60.0, self._runtime_broker_sync_interval_sec):
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
            ) >= max(60.0, self._runtime_broker_sync_interval_sec):
                logger.warning("Runtime broker open-position sync failed: %s", exc)
                self.store.record_event(
                    "WARN",
                    None,
                    "Runtime broker open-position sync failed",
                    {"error": error_text, "mode": self.config.mode.value},
                )
            self._last_runtime_broker_sync_error_monotonic = now_monotonic
            return

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
        if not broker_restored:
            return

        desired_assignments = self._schedule_assignments(self._now_utc())
        recovered_count = 0
        for position in broker_restored.values():
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
            default_strategy = default_assignment.strategy_name if default_assignment is not None else self.config.strategy
            thread_name = (
                str(existing_row.get("thread_name") or f"worker-{symbol}")
                if existing_row
                else (matched_pending.thread_name if matched_pending is not None else f"worker-{symbol}")
            )
            strategy = (
                str(existing_row.get("strategy") or default_strategy)
                if existing_row
                else (matched_pending.strategy if matched_pending is not None else default_strategy)
            )
            mode = (
                str(existing_row.get("mode") or self.config.mode.value)
                if existing_row
                else (matched_pending.mode if matched_pending is not None else self.config.mode.value)
            )

            self.store.upsert_trade(position, thread_name, strategy, mode)
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
                    "position_id": position.position_id,
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

            thread_name = (
                str(existing_row.get("thread_name") or f"worker-{symbol}")
                if existing_row
                else (matched_pending.thread_name if matched_pending is not None else f"worker-{symbol}")
            )
            default_assignment = desired_assignments.get(symbol)
            default_strategy = default_assignment.strategy_name if default_assignment is not None else self.config.strategy
            strategy = (
                str(existing_row.get("strategy") or default_strategy)
                if existing_row
                else (matched_pending.strategy if matched_pending is not None else default_strategy)
            )
            mode = (
                str(existing_row.get("mode") or self.config.mode.value)
                if existing_row
                else (matched_pending.mode if matched_pending is not None else self.config.mode.value)
            )
            self.store.upsert_trade(position, thread_name, strategy, mode)
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
                        "position_id": position.position_id,
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
            }
        )
        if broker_synced is not None:
            summary["restored_open_count"] = len(final_restored)
        else:
            summary["restored_open_count"] = len(final_restored)
        return final_restored, summary

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
        logging.info(
            "Starting bot | broker=%s account=%s mode=%s strategy=%s symbols=%s",
            self.config.broker,
            self.config.account_type.value,
            self.config.mode.value,
            self.config.strategy,
            ",".join(self.config.symbols),
        )

        self._connect_broker()
        final_restored, summary = self._restore_open_positions()
        if final_restored:
            self.position_book.bootstrap(final_restored)
            logger.info(
                "Restored %d open positions from %s for mode=%s",
                len(final_restored),
                str(summary.get("source") or "state_store"),
                self.config.mode.value,
            )
        reconciled_missing_count = self._backfill_missing_on_broker_trades()
        if reconciled_missing_count > 0:
            summary["missing_on_broker_reconciled_count"] = reconciled_missing_count
        reconciled_closed_details_count = self._backfill_closed_trade_details()
        if reconciled_closed_details_count > 0:
            summary["closed_details_reconciled_count"] = reconciled_closed_details_count
        self._log_open_position_restore_summary(summary)

        self._reconcile_workers()
        self._refresh_passive_price_history(force=True)

    def _close_resources(self) -> None:
        if self._resources_closed:
            return
        broker_error: Exception | None = None
        store_error: Exception | None = None
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
        self._resources_closed = True
        if broker_error is not None or store_error is not None:
            logger.warning(
                "Shutdown completed with close errors | broker_error=%s store_error=%s",
                broker_error,
                store_error,
            )

    def stop(self, *, force_close: bool = True) -> None:
        self.stop_event.set()
        with self._workers_lock:
            stop_events = list(self._worker_stop_events.values())
        for stop_event in stop_events:
            stop_event.set()

        alive_symbols: list[str] = []
        with self._workers_lock:
            worker_items = list(self.workers.items())
        for symbol, worker in worker_items:
            worker.join(timeout=5.0)
            if worker.is_alive():
                alive_symbols.append(symbol)
                logger.warning("Worker for %s did not stop within timeout", symbol)
                continue
            with self._workers_lock:
                self.workers.pop(symbol, None)
                self._worker_stop_events.pop(symbol, None)
                self._worker_assignments.pop(symbol, None)
            logger.info("Stopped worker for %s", symbol)

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

        self._close_resources()

    def run_forever(self) -> None:
        try:
            self.start()
            monitor_interval_sec = max(
                1.0,
                min(
                    float(self.config.poll_interval_sec),
                    float(self.config.passive_history_poll_interval_sec),
                ),
            )
            while not self.stop_event.is_set():
                self._monitor_workers()
                self.stop_event.wait(timeout=monitor_interval_sec)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        finally:
            self.stop()
