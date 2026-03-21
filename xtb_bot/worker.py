from __future__ import annotations

from collections import deque
from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import json
import logging
import math
import random
import re
import threading
import time
import uuid

from xtb_bot.client import BaseBrokerClient, BrokerError
from xtb_bot.config import resolve_strategy_param
from xtb_bot.models import (
    AccountSnapshot,
    PendingOpen,
    Position,
    PriceTick,
    RunMode,
    Side,
    StreamHealthStatus,
    SymbolSpec,
    WorkerState,
)
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.strategies import create_strategy
from xtb_bot.strategies.base import StrategyContext


logger = logging.getLogger(__name__)


class SymbolWorker(threading.Thread):
    def __init__(
        self,
        symbol: str,
        mode: RunMode,
        strategy_name: str,
        strategy_params: dict[str, object],
        broker: BaseBrokerClient,
        store: StateStore,
        risk: RiskManager,
        position_book: PositionBook,
        stop_event: threading.Event,
        poll_interval_sec: float,
        poll_jitter_sec: float,
        default_volume: float,
        bot_magic_prefix: str,
        bot_magic_instance: str,
    ) -> None:
        super().__init__(name=f"worker-{symbol}", daemon=True)
        self.symbol = symbol
        self.mode = mode
        self.strategy_name = strategy_name
        self.strategy = create_strategy(strategy_name, strategy_params)
        self.broker = broker
        self.store = store
        self.risk = risk
        self.position_book = position_book
        self.stop_event = stop_event
        self.poll_interval_sec = poll_interval_sec
        self.poll_jitter_sec = max(0.0, float(poll_jitter_sec))
        self.default_volume = default_volume
        self.bot_magic_prefix = bot_magic_prefix
        self.bot_magic_instance = bot_magic_instance
        seed = f"{self.bot_magic_instance}:{self.symbol}"
        self._rng = random.Random(seed)
        history_buffer = max(self.strategy.min_history * 3, 64)
        candle_timeframe_sec = int(float(getattr(self.strategy, "candle_timeframe_sec", 0) or 0))
        if candle_timeframe_sec > 1 and self.poll_interval_sec > 0:
            ticks_per_candle = max(1, math.ceil(candle_timeframe_sec / max(self.poll_interval_sec, 1e-9)))
            # Closed-candle strategies need enough sampled ticks to build candle history.
            min_candle_samples = (self.strategy.min_history + 2) * ticks_per_candle
            history_buffer = max(history_buffer, min_candle_samples * 2)
        history_buffer_cap_raw = self._strategy_float_param(
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
                self.symbol,
                history_buffer,
                history_buffer_cap,
                self.strategy_name,
            )
            history_buffer = history_buffer_cap
        self.price_history_keep_rows = max(1000, history_buffer)
        history_restore_rows_raw = self._strategy_float_param(
            strategy_params,
            "history_restore_rows",
            min(history_buffer, 5000),
        )
        if not math.isfinite(history_restore_rows_raw):
            history_restore_rows_raw = min(history_buffer, 5000)
        self.price_history_restore_rows = max(100, int(history_restore_rows_raw))
        self.price_history_restore_rows = min(self.price_history_restore_rows, history_buffer)
        self.prices: deque[float] = deque(maxlen=history_buffer)
        self.price_timestamps: deque[float] = deque(maxlen=history_buffer)
        self.volumes: deque[float] = deque(maxlen=history_buffer)
        self.iteration = 0
        self.symbol_spec: SymbolSpec | None = None
        self.min_stop_loss_pips = max(1.0, float(self.risk.cfg.min_stop_loss_pips))
        self.min_tp_sl_ratio = max(1.0, float(self.risk.cfg.min_tp_sl_ratio))
        self.trailing_activation_ratio = max(0.0, min(1.0, float(self.risk.cfg.trailing_activation_ratio)))
        self.trailing_distance_pips = max(0.1, float(self.risk.cfg.trailing_distance_pips))
        self.trailing_breakeven_offset_pips = max(
            0.0, float(self.risk.cfg.trailing_breakeven_offset_pips)
        )
        self.trailing_breakeven_offset_pips_fx = max(
            0.0, float(self.risk.cfg.trailing_breakeven_offset_pips_fx)
        )
        self.trailing_breakeven_offset_pips_index = max(
            0.0, float(self.risk.cfg.trailing_breakeven_offset_pips_index)
        )
        self.trailing_breakeven_offset_pips_commodity = max(
            0.0, float(self.risk.cfg.trailing_breakeven_offset_pips_commodity)
        )
        base_trade_cooldown_sec = max(0.0, float(strategy_params.get("trade_cooldown_sec", 0.0)))
        strategy_trade_cooldown_sec = max(
            0.0,
            float(
                strategy_params.get(
                    f"{strategy_name.lower()}_trade_cooldown_sec",
                    base_trade_cooldown_sec,
                )
            ),
        )
        self.trade_cooldown_sec = strategy_trade_cooldown_sec
        self.trade_cooldown_win_sec = self._strategy_float_param(
            strategy_params,
            "trade_cooldown_win_sec",
            self.trade_cooldown_sec,
        )
        self.trade_cooldown_loss_sec = self._strategy_float_param(
            strategy_params,
            "trade_cooldown_loss_sec",
            self.trade_cooldown_sec,
        )
        self.trade_cooldown_flat_sec = self._strategy_float_param(
            strategy_params,
            "trade_cooldown_flat_sec",
            self.trade_cooldown_sec,
        )
        self.trade_cooldown_win_sec = max(0.0, self.trade_cooldown_win_sec)
        self.trade_cooldown_loss_sec = max(0.0, self.trade_cooldown_loss_sec)
        self.trade_cooldown_flat_sec = max(0.0, self.trade_cooldown_flat_sec)
        base_reject_cooldown_default = (
            self.trade_cooldown_loss_sec
            if self.trade_cooldown_loss_sec > 0
            else (
                self.trade_cooldown_sec
                if self.trade_cooldown_sec > 0
                else 60.0
            )
        )
        self.open_reject_cooldown_sec = max(
            0.0,
            self._strategy_float_param(
                strategy_params,
                "open_reject_cooldown_sec",
                max(20.0, min(180.0, base_reject_cooldown_default)),
            ),
        )
        self.same_side_reentry_win_cooldown_sec = max(
            0.0,
            self._strategy_float_param(
                strategy_params,
                "same_side_reentry_win_cooldown_sec",
                0.0,
            ),
        )
        self.same_side_reentry_reset_on_opposite_signal = self._strategy_bool_param(
            strategy_params,
            "same_side_reentry_reset_on_opposite_signal",
            True,
        )
        self.session_close_buffer_sec = max(0, int(self.risk.cfg.session_close_buffer_min)) * 60
        self.news_event_buffer_sec = max(0, int(self.risk.cfg.news_event_buffer_min)) * 60
        self.news_filter_enabled = bool(self.risk.cfg.news_filter_enabled)
        self.news_event_action = str(self.risk.cfg.news_event_action).lower()
        self._handled_news_events: set[str] = set()
        self.spread_filter_enabled = bool(self.risk.cfg.spread_filter_enabled)
        self.spread_anomaly_multiplier = max(1.0, float(self.risk.cfg.spread_anomaly_multiplier))
        self.spread_min_samples = max(1, int(self.risk.cfg.spread_min_samples))
        self.spread_pct_filter_enabled = bool(self.risk.cfg.spread_pct_filter_enabled)
        self.spread_max_pct = max(0.0, float(self.risk.cfg.spread_max_pct))
        self.spread_max_pct_cfd = max(0.0, float(self.risk.cfg.spread_max_pct_cfd))
        self.spread_max_pct_crypto = max(0.0, float(self.risk.cfg.spread_max_pct_crypto))
        spread_window = max(2, int(self.risk.cfg.spread_avg_window))
        self.spreads_pips: deque[float] = deque(maxlen=spread_window)
        self.connectivity_check_enabled = bool(self.risk.cfg.connectivity_check_enabled)
        self.connectivity_max_latency_ms = max(1.0, float(self.risk.cfg.connectivity_max_latency_ms))
        self.connectivity_pong_timeout_sec = max(0.1, float(self.risk.cfg.connectivity_pong_timeout_sec))
        self.connectivity_check_interval_sec = max(
            1.0,
            self._strategy_float_param(
                strategy_params,
                "worker_connectivity_check_interval_sec",
                max(10.0, self.poll_interval_sec),
            ),
        )
        self.stream_health_check_enabled = bool(self.risk.cfg.stream_health_check_enabled)
        self.stream_max_tick_age_sec = max(0.5, float(self.risk.cfg.stream_max_tick_age_sec))
        self.stream_event_cooldown_sec = max(5.0, float(self.risk.cfg.stream_event_cooldown_sec))
        self.hold_reason_log_interval_sec = max(1.0, float(self.risk.cfg.hold_reason_log_interval_sec))
        self.worker_state_flush_interval_sec = max(0.5, float(self.risk.cfg.worker_state_flush_interval_sec))
        self.account_snapshot_persist_interval_sec = max(
            1.0,
            float(self.risk.cfg.account_snapshot_persist_interval_sec),
        )
        self.symbol_auto_disable_on_epic_unavailable = bool(
            self.risk.cfg.symbol_auto_disable_on_epic_unavailable
        )
        self.symbol_auto_disable_epic_unavailable_threshold = max(
            1,
            int(self.risk.cfg.symbol_auto_disable_epic_unavailable_threshold),
        )
        self._epic_unavailable_consecutive_errors = 0
        self._quote_unavailable_consecutive_errors = 0
        self._symbol_disabled = False
        self._symbol_disabled_reason: str | None = None
        self._last_stream_health: StreamHealthStatus | None = None
        self._last_stream_health_event_ts = 0.0
        self._last_stream_block_reason: str | None = None
        self._last_stream_block_event_ts = 0.0
        self._last_stream_metrics_event_ts = 0.0
        self._last_stream_metrics_key: tuple[int, int, int] | None = None
        self._cached_connectivity_status: ConnectivityStatus | None = None
        self._next_connectivity_probe_ts = 0.0
        self._last_connectivity_block_reason: str | None = None
        self._last_connectivity_block_event_ts = 0.0
        self._last_hold_reason: str | None = None
        self._last_hold_reason_log_ts = 0.0
        strategy_name_lower = str(self.strategy_name or "").strip().lower()
        self._trend_hold_summary_enabled = (
            strategy_name_lower in {"trend_following", "crypto_trend_following"}
            and self._strategy_bool_param(strategy_params, "hold_summary_enabled", True)
        )
        self._trend_hold_summary_interval_sec = max(
            10.0,
            self._strategy_float_param(
                strategy_params,
                "hold_summary_interval_sec",
                180.0,
            ),
        )
        trend_hold_summary_window_raw = self._strategy_float_param(
            strategy_params,
            "hold_summary_window",
            240.0,
        )
        if not math.isfinite(trend_hold_summary_window_raw):
            trend_hold_summary_window_raw = 240.0
        self._trend_hold_summary_window = max(20, int(trend_hold_summary_window_raw))
        trend_hold_summary_min_samples_raw = self._strategy_float_param(
            strategy_params,
            "hold_summary_min_samples",
            30.0,
        )
        if not math.isfinite(trend_hold_summary_min_samples_raw):
            trend_hold_summary_min_samples_raw = 30.0
        self._trend_hold_summary_min_samples = min(
            self._trend_hold_summary_window,
            max(5, int(trend_hold_summary_min_samples_raw)),
        )
        self._trend_hold_reasons: deque[str] = deque(maxlen=self._trend_hold_summary_window)
        self._last_trend_hold_summary_ts = 0.0
        self.debug_indicators_enabled = self._strategy_bool_param(
            strategy_params,
            "debug_indicators",
            False,
        )
        self.min_confidence_for_entry = max(
            0.0,
            min(
                1.0,
                self._strategy_float_param(
                    strategy_params,
                    "min_confidence_for_entry",
                    0.0,
                ),
            ),
        )
        self.g1_protective_exit_enabled = self._strategy_bool_param(
            strategy_params,
            "protective_exit_enabled",
            True,
        )
        self.g1_protective_exit_loss_ratio = max(
            0.0,
            min(
                1.0,
                self._strategy_float_param(
                    strategy_params,
                    "protective_exit_loss_ratio",
                    0.82,
                ),
            ),
        )
        self.g1_protective_exit_allow_adx_regime_loss = self._strategy_bool_param(
            strategy_params,
            "protective_exit_allow_adx_regime_loss",
            True,
        )
        self.debug_indicators_interval_sec = max(
            0.0,
            self._strategy_float_param(
                strategy_params,
                "debug_indicators_interval_sec",
                0.0,
            ),
        )
        self._last_indicator_debug_ts = 0.0
        self._next_entry_allowed_ts = 0.0
        self._active_entry_cooldown_sec = 0.0
        self._active_entry_cooldown_outcome = "none"
        self._last_confidence_block_log_ts = 0.0
        self._last_cooldown_block_log_ts = 0.0
        self._same_side_reentry_block_side: Side | None = None
        self._same_side_reentry_block_until_ts = 0.0
        self._last_same_side_reentry_block_log_ts = 0.0
        self._allowance_backoff_until_ts = 0.0
        self._last_allowance_backoff_kind: str | None = None
        self._last_allowance_backoff_event_ts = 0.0
        self._pending_close_retry_position_id: str | None = None
        self._pending_close_retry_until_ts = 0.0
        self._last_close_deferred_event_ts = 0.0
        self._last_close_deferred_position_id: str | None = None
        self._symbol_spec_retry_after_ts = 0.0
        self._symbol_spec_retry_backoff_sec = 0.0
        self.symbol_auto_disable_min_error_window_sec = max(
            0.0,
            self._strategy_float_param(
                strategy_params,
                "symbol_auto_disable_min_error_window_sec",
                180.0,
            ),
        )
        self._epic_unavailable_first_error_ts = 0.0
        self._quote_unavailable_first_error_ts = 0.0
        self._pending_missing_position_id: str | None = None
        self._pending_missing_position_until_ts = 0.0
        self._pending_missing_position_deadline_ts = 0.0
        self.manual_close_sync_interval_sec = max(
            5.0,
            self._strategy_float_param(
                strategy_params,
                "worker_manual_close_sync_interval_sec",
                60.0,
            ),
        )
        self._manual_close_sync_next_check_ts = 0.0
        self._manual_close_sync_position_id: str | None = None
        self.missing_position_reconcile_timeout_sec = max(
            10.0,
            self._strategy_float_param(
                strategy_params,
                "worker_missing_position_reconcile_timeout_sec",
                max(30.0, self.stream_event_cooldown_sec * 3.0),
            ),
        )
        self._strategy_trailing_overrides_by_position: dict[str, dict[str, float]] = {}
        self._strategy_trailing_store_key = f"worker.trailing_override.{self.symbol}"
        self.active_position_poll_interval_sec = max(
            0.1,
            self._strategy_float_param(
                strategy_params,
                "active_position_poll_interval_sec",
                min(1.0, max(self.poll_interval_sec, 0.1)),
            ),
        )
        self.account_snapshot_cache_ttl_sec = max(
            0.2,
            self._strategy_float_param(
                strategy_params,
                "worker_account_snapshot_cache_ttl_sec",
                min(5.0, max(self.poll_interval_sec, 0.2)),
            ),
        )
        self._cached_account_snapshot: AccountSnapshot | None = None
        self._cached_account_snapshot_ts = 0.0
        self._last_saved_worker_state_ts = 0.0
        self._last_saved_worker_state_signature: tuple[object, ...] | None = None
        self._last_account_snapshot_persist_ts = 0.0
        self._last_account_snapshot_persist_signature: tuple[object, ...] | None = None
        self._restore_price_history()

    def _strategy_param_key(self, suffix: str) -> str:
        return f"{self.strategy_name.lower()}_{suffix}"

    def _strategy_bool_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
        default: bool,
    ) -> bool:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_name,
            suffix,
            default,
            mode=self.mode,
        )
        if isinstance(raw, str):
            return raw.strip().lower() not in {"0", "false", "no", "off"}
        return bool(raw)

    def _strategy_float_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
        default: float,
    ) -> float:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_name,
            suffix,
            default,
            mode=self.mode,
        )
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _is_epic_unavailable_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return "epic.unavailable" in lowered or "instrument.epic.unavailable" in lowered

    @staticmethod
    def _is_quote_unavailable_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return "does not contain bid/offer" in lowered

    def _disable_symbol(self, reason: str) -> None:
        if self._symbol_disabled:
            return
        self._symbol_disabled = True
        self._symbol_disabled_reason = reason
        self.store.record_event(
            "ERROR",
            self.symbol,
            "Symbol disabled by safety guard",
            {
                "reason": reason,
                "epic_unavailable_consecutive_errors": self._epic_unavailable_consecutive_errors,
                "quote_unavailable_consecutive_errors": self._quote_unavailable_consecutive_errors,
                "disable_threshold": self.symbol_auto_disable_epic_unavailable_threshold,
                "auto_disable_enabled": self.symbol_auto_disable_on_epic_unavailable,
                "min_error_window_sec": self.symbol_auto_disable_min_error_window_sec,
            },
        )
        logger.error(
            "Symbol disabled by safety guard | symbol=%s reason=%s consecutive=%s threshold=%s",
            self.symbol,
            reason,
            self._epic_unavailable_consecutive_errors,
            self.symbol_auto_disable_epic_unavailable_threshold,
        )

    def _register_broker_error(self, error_text: str) -> None:
        if self._symbol_disabled:
            return
        now = time.time()
        if self._is_epic_unavailable_error(error_text):
            if self._epic_unavailable_consecutive_errors <= 0:
                self._epic_unavailable_first_error_ts = now
            self._epic_unavailable_consecutive_errors += 1
            self._quote_unavailable_consecutive_errors = 0
            self._quote_unavailable_first_error_ts = 0.0
            elapsed_sec = (
                now - self._epic_unavailable_first_error_ts
                if self._epic_unavailable_first_error_ts > 0
                else 0.0
            )
            if (
                self.symbol_auto_disable_on_epic_unavailable
                and self._epic_unavailable_consecutive_errors
                >= self.symbol_auto_disable_epic_unavailable_threshold
                and elapsed_sec >= self.symbol_auto_disable_min_error_window_sec
            ):
                self._disable_symbol(
                    "repeated_epic_unavailable_errors"
                )
            return
        if self._is_quote_unavailable_error(error_text):
            if self._quote_unavailable_consecutive_errors <= 0:
                self._quote_unavailable_first_error_ts = now
            self._quote_unavailable_consecutive_errors += 1
            self._epic_unavailable_consecutive_errors = 0
            self._epic_unavailable_first_error_ts = 0.0
            elapsed_sec = (
                now - self._quote_unavailable_first_error_ts
                if self._quote_unavailable_first_error_ts > 0
                else 0.0
            )
            if (
                self.symbol_auto_disable_on_epic_unavailable
                and self._quote_unavailable_consecutive_errors
                >= self.symbol_auto_disable_epic_unavailable_threshold
                and elapsed_sec >= self.symbol_auto_disable_min_error_window_sec
            ):
                self._disable_symbol(
                    "repeated_quote_unavailable_errors"
                )
            return
        self._epic_unavailable_consecutive_errors = 0
        self._quote_unavailable_consecutive_errors = 0
        self._epic_unavailable_first_error_ts = 0.0
        self._quote_unavailable_first_error_ts = 0.0

    def _reset_broker_error_trackers(self) -> None:
        self._epic_unavailable_consecutive_errors = 0
        self._quote_unavailable_consecutive_errors = 0
        self._epic_unavailable_first_error_ts = 0.0
        self._quote_unavailable_first_error_ts = 0.0

    @staticmethod
    def _is_allowance_backoff_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return (
            "allowance cooldown is active" in lowered
            or "critical_trade_operation_active" in lowered
            or "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )

    @staticmethod
    def _allowance_backoff_kind(error_text: str) -> str:
        lowered = str(error_text).lower()
        if "allowance cooldown is active" in lowered:
            return "cooldown_active"
        if "critical_trade_operation_active" in lowered:
            return "critical_trade_deferred"
        if "exceeded-api-key-allowance" in lowered:
            return "api_key_allowance_exceeded"
        if "exceeded-account-allowance" in lowered:
            return "account_allowance_exceeded"
        if "exceeded-account-trading-allowance" in lowered:
            return "account_trading_allowance_exceeded"
        return "allowance_backoff"

    @staticmethod
    def _extract_broker_minimum_lot_from_error(error_text: str) -> tuple[float | None, str | None, str | None]:
        text = str(error_text)
        min_match = re.search(r"\bmin=([0-9]*\.?[0-9]+)\b", text)
        if min_match is None:
            return None, None, None
        try:
            minimum = float(min_match.group(1))
        except (TypeError, ValueError):
            return None, None, None
        if not math.isfinite(minimum) or minimum <= 0:
            return None, None, None

        epic_match = re.search(r"\bepic=([^,\s)]+)", text)
        source_match = re.search(r"\blot_min_source=([^,\s)]+)", text)
        epic = str(epic_match.group(1)).strip() if epic_match else None
        source = str(source_match.group(1)).strip() if source_match else None
        return minimum, epic, source

    def _sync_local_symbol_lot_min_from_broker_error(self, error_text: str) -> bool:
        if self.symbol_spec is None:
            return False
        minimum, epic, source = self._extract_broker_minimum_lot_from_error(error_text)
        if minimum is None:
            return False
        previous = float(self.symbol_spec.lot_min)
        if minimum <= previous + 1e-12:
            return False

        self.symbol_spec.lot_min = minimum
        self.symbol_spec.lot_max = max(float(self.symbol_spec.lot_max), minimum)
        metadata = self.symbol_spec.metadata if isinstance(self.symbol_spec.metadata, dict) else {}
        metadata["lot_min_source"] = source or "broker_minimum_error"
        metadata["lot_min_update_reason"] = "BROKER_MINIMUM_SIZE_ERROR"
        metadata["lot_min_update_ts"] = time.time()
        if epic:
            metadata["lot_min_update_epic"] = epic
        self.symbol_spec.metadata = metadata
        self.store.record_event(
            "WARN",
            self.symbol,
            "Local symbol lot minimum updated",
            {
                "previous_lot_min": previous,
                "updated_lot_min": minimum,
                "source": source or "broker_minimum_error",
                "epic": epic,
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Local symbol lot_min updated | symbol=%s %.6f->%.6f source=%s epic=%s",
            self.symbol,
            previous,
            minimum,
            source or "broker_minimum_error",
            epic or "",
        )
        return True

    def _refresh_symbol_spec_after_min_order_reject(self, error_text: str) -> bool:
        lowered = str(error_text).lower()
        if "minimum_order_size_error" not in lowered:
            return False
        getter = getattr(self.broker, "get_symbol_spec", None)
        if not callable(getter):
            return False
        try:
            latest_spec = getter(self.symbol)
        except Exception:
            return False
        if not isinstance(latest_spec, SymbolSpec):
            return False

        previous_min = float(self.symbol_spec.lot_min) if self.symbol_spec is not None else 0.0
        latest_min = float(latest_spec.lot_min)
        if latest_min <= previous_min + 1e-12:
            return False
        self.symbol_spec = latest_spec
        self.store.record_event(
            "WARN",
            self.symbol,
            "Local symbol specification refreshed",
            {
                "previous_lot_min": previous_min,
                "updated_lot_min": latest_min,
                "reason": "minimum_order_size_reject",
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Local symbol spec refreshed after min-size reject | symbol=%s %.6f->%.6f",
            self.symbol,
            previous_min,
            latest_min,
        )
        return True

    @staticmethod
    def _extract_allowance_backoff_remaining_sec(error_text: str) -> float | None:
        match = re.search(r"\(([\d.]+)s remaining\)", str(error_text))
        if match is None:
            return None
        try:
            remaining = float(match.group(1))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(remaining) or remaining <= 0:
            return None
        return remaining

    def _handle_allowance_backoff_error(self, error_text: str) -> float:
        now = time.time()
        lowered = str(error_text or "").lower()
        kind = self._allowance_backoff_kind(error_text)
        is_market_data_allowance = (
            kind in {"api_key_allowance_exceeded", "account_allowance_exceeded"}
            and "/markets" in lowered
        )
        remaining = self._extract_allowance_backoff_remaining_sec(error_text)
        if remaining is None:
            broker_remaining = self._broker_public_api_backoff_remaining_sec()
            if is_market_data_allowance:
                remaining = min(5.0, max(1.0, broker_remaining if broker_remaining > 0 else 2.0))
            else:
                remaining = max(2.0, min(30.0, self.stream_event_cooldown_sec / 2.0))
        elif is_market_data_allowance:
            remaining = min(5.0, max(1.0, remaining))
        self._allowance_backoff_until_ts = max(self._allowance_backoff_until_ts, now + remaining)
        reason_changed = kind != self._last_allowance_backoff_kind
        cooldown_passed = (now - self._last_allowance_backoff_event_ts) >= self.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            payload = {
                "kind": kind,
                "remaining_sec": round(remaining, 2),
                "error": error_text,
            }
            self.store.record_event(
                "WARN",
                self.symbol,
                "Broker allowance backoff active",
                payload,
            )
            self._last_allowance_backoff_event_ts = now
            self._last_allowance_backoff_kind = kind
        logger.warning(
            "Broker allowance backoff active | symbol=%s kind=%s remaining_sec=%.2f",
            self.symbol,
            kind,
            remaining,
        )
        return remaining

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

    def _local_allowance_backoff_remaining_sec(self) -> float:
        remaining = self._allowance_backoff_until_ts - time.time()
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def _record_close_deferred_by_allowance(
        self,
        position: Position,
        reason: str,
        wait_sec: float,
        error_text: str,
    ) -> None:
        now = time.time()
        reason_changed = self._last_close_deferred_position_id != position.position_id
        cooldown_passed = (now - self._last_close_deferred_event_ts) >= self.stream_event_cooldown_sec
        if not reason_changed and not cooldown_passed:
            return
        self.store.record_event(
            "WARN",
            self.symbol,
            "Position close deferred by broker allowance backoff",
            {
                "position_id": position.position_id,
                "reason": reason,
                "wait_sec": round(max(0.0, wait_sec), 3),
                "error": error_text,
            },
        )
        self._last_close_deferred_event_ts = now
        self._last_close_deferred_position_id = position.position_id

    def _schedule_missing_position_reconciliation(
        self,
        position: Position,
        wait_sec: float,
        broker_error: str,
    ) -> None:
        now = time.time()
        if (
            self._pending_missing_position_id != position.position_id
            or self._pending_missing_position_deadline_ts <= now
        ):
            self._pending_missing_position_deadline_ts = (
                now + self.missing_position_reconcile_timeout_sec
            )
        self._pending_missing_position_id = position.position_id
        self._pending_missing_position_until_ts = now + max(1.0, wait_sec)
        self.store.record_event(
            "WARN",
            self.symbol,
            "Broker position missing reconciliation delayed",
            {
                "position_id": position.position_id,
                "wait_sec": round(max(1.0, wait_sec), 2),
                "deadline_sec": round(
                    max(0.0, self._pending_missing_position_deadline_ts - now),
                    2,
                ),
                "broker_error": broker_error,
            },
        )

    def _clear_pending_missing_position_reconciliation(self) -> None:
        self._pending_missing_position_id = None
        self._pending_missing_position_until_ts = 0.0
        self._pending_missing_position_deadline_ts = 0.0

    def _attempt_pending_missing_position_reconciliation(
        self,
        position: Position,
        tick: PriceTick | None,
    ) -> bool:
        if self._pending_missing_position_id != position.position_id:
            return False

        now = time.time()
        if now < self._pending_missing_position_until_ts:
            return True

        broker_sync = self._get_broker_close_sync(position)
        if broker_sync is not None:
            inferred_price, inferred_reason = self._infer_missing_position_close(
                position,
                tick.bid if tick is not None else None,
                tick.ask if tick is not None else None,
            )
            sync_source = str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync"
            self._clear_pending_missing_position_reconciliation()
            self._finalize_position_close(
                position,
                inferred_price,
                f"broker_position_missing:delayed:{inferred_reason}:{sync_source}",
                broker_sync=broker_sync,
            )
            self.store.record_event(
                "INFO",
                self.symbol,
                "Broker position close sync reconciled",
                {
                    "position_id": position.position_id,
                    "broker_close_sync": broker_sync,
                },
            )
            return True

        deadline_ts = self._pending_missing_position_deadline_ts
        if deadline_ts > now:
            backoff_remaining = self._broker_public_api_backoff_remaining_sec()
            retry_sec = (
                backoff_remaining
                if backoff_remaining > 0
                else min(5.0, max(1.0, deadline_ts - now))
            )
            self._pending_missing_position_until_ts = now + max(1.0, retry_sec)
            if backoff_remaining > 0:
                logger.warning(
                    "Broker position missing reconciliation retry delayed by allowance backoff"
                    " | symbol=%s position_id=%s wait_sec=%.2f deadline_sec=%.2f",
                    self.symbol,
                    position.position_id,
                    retry_sec,
                    max(0.0, deadline_ts - now),
                )
            return True

        inferred_price, inferred_reason = self._infer_missing_position_close(
            position,
            tick.bid if tick is not None else None,
            tick.ask if tick is not None else None,
        )
        self._clear_pending_missing_position_reconciliation()
        self._finalize_position_close(
            position,
            inferred_price,
            f"broker_position_missing:timeout:{inferred_reason}",
            broker_sync=None,
        )
        self.store.record_event(
            "WARN",
            self.symbol,
            "Broker position missing reconciled after timeout",
            {
                "position_id": position.position_id,
                "inferred_reason": inferred_reason,
                "inferred_close_price": inferred_price,
            },
        )
        return True

    @staticmethod
    def _broker_sync_has_close_evidence(payload: dict[str, object] | None) -> bool:
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

    def _maybe_reconcile_execution_manual_close(
        self,
        position: Position,
        tick: PriceTick | None,
    ) -> bool:
        if self.mode != RunMode.EXECUTION:
            return False
        if self._pending_missing_position_id == position.position_id:
            return False

        now = time.time()
        if self._manual_close_sync_position_id != position.position_id:
            self._manual_close_sync_position_id = position.position_id
            self._manual_close_sync_next_check_ts = 0.0
        if now < self._manual_close_sync_next_check_ts:
            return False

        allowance_backoff_remaining = self._broker_public_api_backoff_remaining_sec()
        if allowance_backoff_remaining > 0:
            self._manual_close_sync_next_check_ts = now + max(1.0, allowance_backoff_remaining)
            return False

        self._manual_close_sync_next_check_ts = now + self.manual_close_sync_interval_sec
        broker_sync = self._get_broker_close_sync(position)
        if not self._broker_sync_has_close_evidence(broker_sync):
            return False

        bid = tick.bid if tick is not None else None
        ask = tick.ask if tick is not None else None
        inferred_price, inferred_reason = self._infer_missing_position_close(position, bid, ask)
        sync_source = str((broker_sync or {}).get("source") or "").strip().lower() or "broker_sync"
        final_reason = f"broker_manual_close:{inferred_reason}:{sync_source}"
        self._clear_pending_missing_position_reconciliation()
        self._finalize_position_close(position, inferred_price, final_reason, broker_sync=broker_sync)
        self.store.record_event(
            "INFO",
            self.symbol,
            "Manual broker close reconciled",
            {
                "position_id": position.position_id,
                "inferred_reason": inferred_reason,
                "inferred_close_price": inferred_price,
                "broker_close_sync": broker_sync,
            },
        )
        logger.info(
            "Manual broker close reconciled | symbol=%s position_id=%s source=%s",
            self.symbol,
            position.position_id,
            sync_source,
        )
        return True

    def _maybe_record_hold_reason(
        self,
        signal,
        current_spread_pips: float,
        current_spread_pct: float | None = None,
    ) -> None:
        reason = str(signal.metadata.get("reason") or "unknown")
        now = time.time()
        self._maybe_record_trend_hold_summary(reason, now)
        reason_changed = reason != self._last_hold_reason
        interval_passed = (now - self._last_hold_reason_log_ts) >= self.hold_reason_log_interval_sec
        if not reason_changed and not interval_passed:
            return

        spread_avg_pips = 0.0
        if self.spreads_pips:
            spread_avg_pips = sum(self.spreads_pips) / len(self.spreads_pips)
        payload = {
            "reason": reason,
            "strategy": self.strategy_name,
            "symbol": self.symbol,
            "confidence": signal.confidence,
            "current_spread_pips": current_spread_pips,
            "current_spread_pct": current_spread_pct,
            "average_spread_pips": spread_avg_pips,
            "spread_samples": len(self.spreads_pips),
            "utc_hour": time.gmtime(now).tm_hour,
            "allowance_backoff_remaining_sec": round(self._local_allowance_backoff_remaining_sec(), 3),
            "prices_buffer_len": len(self.prices),
            "min_history": self.strategy.min_history,
            "metadata": signal.metadata,
        }
        self.store.record_event("INFO", self.symbol, "Signal hold reason", payload)
        self._last_hold_reason = reason
        self._last_hold_reason_log_ts = now

    def _maybe_record_trend_hold_summary(self, reason: str, now: float) -> None:
        if not self._trend_hold_summary_enabled:
            return
        normalized_reason = str(reason or "unknown").strip() or "unknown"
        self._trend_hold_reasons.append(normalized_reason)
        sample_count = len(self._trend_hold_reasons)
        if sample_count < self._trend_hold_summary_min_samples:
            return
        if (now - self._last_trend_hold_summary_ts) < self._trend_hold_summary_interval_sec:
            return

        reason_counts: dict[str, int] = {}
        for item in self._trend_hold_reasons:
            reason_counts[item] = reason_counts.get(item, 0) + 1
        ranked = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:3]
        top_reasons: list[dict[str, object]] = []
        top_text_parts: list[str] = []
        for item_reason, count in ranked:
            share = count / sample_count if sample_count > 0 else 0.0
            top_reasons.append(
                {
                    "reason": item_reason,
                    "count": count,
                    "share": round(share, 4),
                }
            )
            top_text_parts.append(f"{item_reason}:{count}")

        payload = {
            "strategy": self.strategy_name,
            "symbol": self.symbol,
            "window_size": sample_count,
            "configured_window_size": self._trend_hold_summary_window,
            "min_samples": self._trend_hold_summary_min_samples,
            "interval_sec": self._trend_hold_summary_interval_sec,
            "top_reasons": top_reasons,
            "allowance_backoff_remaining_sec": round(self._local_allowance_backoff_remaining_sec(), 3),
        }
        self.store.record_event("INFO", self.symbol, "Trend hold summary", payload)
        logger.info(
            "Trend hold summary | symbol=%s window=%s top=%s",
            self.symbol,
            sample_count,
            ", ".join(top_text_parts),
        )
        self._last_trend_hold_summary_ts = now

    def _maybe_record_indicator_debug(
        self,
        signal,
        current_spread_pips: float,
    ) -> None:
        if not self.debug_indicators_enabled:
            return

        now = time.time()
        if (
            self.debug_indicators_interval_sec > 0
            and (now - self._last_indicator_debug_ts) < self.debug_indicators_interval_sec
        ):
            return

        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        payload = {
            "strategy": self.strategy_name,
            "symbol": self.symbol,
            "signal_side": signal.side.value,
            "confidence": signal.confidence,
            "reason": metadata.get("reason"),
            "current_spread_pips": current_spread_pips,
            "prices_buffer_len": len(self.prices),
            "min_history": self.strategy.min_history,
            "fast_ema": metadata.get("fast_ema"),
            "slow_ema": metadata.get("slow_ema"),
            "adx": metadata.get("adx"),
            "atr": metadata.get("atr"),
            "trend_signal": metadata.get("trend_signal"),
            "indicator": metadata.get("indicator"),
            "metadata": metadata,
        }
        self.store.record_event("INFO", self.symbol, "Strategy indicator snapshot", payload)
        self._last_indicator_debug_ts = now

    def _confidence_allows_open(self, signal) -> bool:
        if self.min_confidence_for_entry <= 0:
            return True

        confidence = max(0.0, min(1.0, float(signal.confidence)))
        if confidence + 1e-12 >= self.min_confidence_for_entry:
            return True

        now = time.time()
        if (now - self._last_confidence_block_log_ts) >= self.hold_reason_log_interval_sec:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Trade blocked by confidence threshold",
                {
                    "signal": signal.side.value,
                    "confidence": confidence,
                    "min_confidence_for_entry": self.min_confidence_for_entry,
                    "strategy": self.strategy_name,
                },
            )
            logger.warning(
                "Trade blocked by confidence threshold | symbol=%s confidence=%.4f threshold=%.4f strategy=%s",
                self.symbol,
                confidence,
                self.min_confidence_for_entry,
                self.strategy_name,
            )
            self._last_confidence_block_log_ts = now
        return False

    def _cooldown_allows_open(self, signal_side: Side) -> bool:
        if not self._same_side_reentry_allows_open(signal_side):
            return False

        now = time.time()
        remaining = self._next_entry_allowed_ts - now
        if remaining <= 0:
            self._active_entry_cooldown_sec = 0.0
            self._active_entry_cooldown_outcome = "none"
            return True

        if (now - self._last_cooldown_block_log_ts) >= self.hold_reason_log_interval_sec:
            self.store.record_event(
                "INFO",
                self.symbol,
                "Trade blocked by entry cooldown",
                {
                    "signal": signal_side.value,
                    "cooldown_sec": self._active_entry_cooldown_sec,
                    "remaining_sec": round(remaining, 3),
                    "next_entry_at": self._next_entry_allowed_ts,
                    "cooldown_outcome": self._active_entry_cooldown_outcome,
                    "strategy": self.strategy_name,
                },
            )
            self._last_cooldown_block_log_ts = now
        return False

    def _same_side_reentry_allows_open(self, signal_side: Side) -> bool:
        if self.same_side_reentry_win_cooldown_sec <= 0:
            return True

        blocked_side = self._same_side_reentry_block_side
        if blocked_side is None:
            return True

        now = time.time()
        remaining = self._same_side_reentry_block_until_ts - now
        if remaining <= 0:
            self._same_side_reentry_block_side = None
            self._same_side_reentry_block_until_ts = 0.0
            return True

        if signal_side != blocked_side:
            if self.same_side_reentry_reset_on_opposite_signal:
                self._same_side_reentry_block_side = None
                self._same_side_reentry_block_until_ts = 0.0
            return True

        if (now - self._last_same_side_reentry_block_log_ts) >= self.hold_reason_log_interval_sec:
            self.store.record_event(
                "INFO",
                self.symbol,
                "Trade blocked by same-side reentry cooldown",
                {
                    "signal": signal_side.value,
                    "blocked_side": blocked_side.value,
                    "cooldown_sec": self.same_side_reentry_win_cooldown_sec,
                    "remaining_sec": round(remaining, 3),
                    "next_entry_at": self._same_side_reentry_block_until_ts,
                    "strategy": self.strategy_name,
                },
            )
            self._last_same_side_reentry_block_log_ts = now
        return False

    def _cooldown_outcome_from_pnl(self, pnl: float) -> str:
        if pnl > 1e-9:
            return "win"
        if pnl < -1e-9:
            return "loss"
        return "flat"

    def _cooldown_duration_for_outcome(self, outcome: str) -> float:
        if outcome == "win":
            return self.trade_cooldown_win_sec
        if outcome == "loss":
            return self.trade_cooldown_loss_sec
        return self.trade_cooldown_flat_sec

    @staticmethod
    def _is_open_reject_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        if "ig deal rejected:" in lowered:
            return True
        if "requested size below broker minimum" in lowered:
            return True
        return "ig api post /positions/otc failed" in lowered

    def _activate_reject_entry_cooldown(self, error_text: str) -> bool:
        if self.open_reject_cooldown_sec <= 0:
            return False
        if not self._is_open_reject_error(error_text):
            return False

        now = time.time()
        previous_next = self._next_entry_allowed_ts
        next_allowed = max(previous_next, now + self.open_reject_cooldown_sec)
        self._next_entry_allowed_ts = next_allowed
        self._active_entry_cooldown_sec = self.open_reject_cooldown_sec
        self._active_entry_cooldown_outcome = "broker_reject"

        if next_allowed > previous_next + 1e-6:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Entry cooldown activated after broker reject",
                {
                    "cooldown_sec": self.open_reject_cooldown_sec,
                    "next_entry_at": self._next_entry_allowed_ts,
                    "error": error_text,
                    "strategy": self.strategy_name,
                },
            )
        return True

    def _build_magic_comment(self) -> tuple[str, str]:
        trade_uid = uuid.uuid4().hex[:10]
        # Keep broker reference/comment compact to avoid silent truncation on broker side.
        prefix = str(self.bot_magic_prefix).strip()[:10] or "BOT"
        instance = str(self.bot_magic_instance).strip()[:8] or "RUN"
        comment = f"{prefix}:{instance}:{trade_uid}"
        if len(comment) > 32:
            comment = comment[:32]
        return comment, trade_uid

    def _restore_price_history(self) -> None:
        capacity = int(self.prices.maxlen or 0)
        if capacity <= 0:
            return
        restore_limit = max(
            1,
            min(
                capacity,
                int(getattr(self, "price_history_restore_rows", capacity) or capacity),
            ),
        )
        restored = self.store.load_recent_price_history(self.symbol, limit=restore_limit)
        if not restored:
            return
        parsed_samples: list[tuple[float, float, float | None]] = []
        for item in restored:
            try:
                ts = float(item["ts"])
                close = float(item["close"])
            except (KeyError, TypeError, ValueError):
                continue
            if not math.isfinite(ts) or not math.isfinite(close) or ts <= 0:
                continue
            volume: float | None
            volume_raw = item.get("volume")
            try:
                volume = float(volume_raw) if volume_raw is not None else None
            except (TypeError, ValueError):
                volume = None
            if volume is not None and volume <= 0:
                volume = None
            parsed_samples.append((ts, close, volume))
        if not parsed_samples:
            return

        timestamp_repair_applied = False
        if self._restored_timestamps_need_repair([sample[0] for sample in parsed_samples]):
            parsed_samples = self._repair_restored_timestamps(parsed_samples)
            timestamp_repair_applied = True

        restored_count = 0
        restore_step_sec = max(1e-3, float(self.poll_interval_sec or 1.0))
        for ts, close, volume in parsed_samples:
            if self.price_timestamps and ts <= (float(self.price_timestamps[-1]) + 1e-9):
                ts = float(self.price_timestamps[-1]) + restore_step_sec
            self.price_timestamps.append(ts)
            self.prices.append(close)
            if volume is not None:
                self.volumes.append(volume)
            restored_count += 1

        if restored_count > 0:
            self.store.record_event(
                "INFO",
                self.symbol,
                "Restored price history",
                {
                    "restored_samples": restored_count,
                    "buffer_capacity": capacity,
                    "restore_limit": restore_limit,
                    "timestamp_repair_applied": timestamp_repair_applied,
                },
            )

    def _restored_timestamps_need_repair(self, timestamps: list[float]) -> bool:
        total = len(timestamps)
        if total < 3:
            return False
        rounded_unique = len({round(float(ts), 6) for ts in timestamps})
        positive_deltas = 0
        prev = float(timestamps[0])
        for ts in timestamps[1:]:
            current = float(ts)
            if current > (prev + 1e-9):
                positive_deltas += 1
            prev = current
        min_unique = max(3, int(total * 0.10))
        min_positive = max(2, int((total - 1) * 0.10))
        if rounded_unique < min_unique or positive_deltas < min_positive:
            return True
        candle_timeframe = float(getattr(self.strategy, "candle_timeframe_sec", 0) or 0)
        if candle_timeframe > 1.0 and total >= max(16, int(getattr(self.strategy, "min_history", 1) or 1)):
            span = max(0.0, float(timestamps[-1]) - float(timestamps[0]))
            min_span = candle_timeframe * max(4.0, min(float(getattr(self.strategy, "min_history", 1) or 1), 24.0) * 0.5)
            if span < min_span:
                return True
        return False

    def _repair_restored_timestamps(self, samples: list[tuple[float, float, float | None]]) -> list[tuple[float, float, float | None]]:
        if not samples:
            return samples
        step_sec = max(1.0, float(self.poll_interval_sec or 1.0))
        now_ts = time.time()
        start_ts = now_ts - step_sec * max(0, len(samples) - 1)
        repaired: list[tuple[float, float, float | None]] = []
        for idx, (_, close, volume) in enumerate(samples):
            repaired.append((start_ts + step_sec * idx, close, volume))
        return repaired

    def _cache_price_sample(self, timestamp: float, close: float, volume: float | None) -> None:
        max_rows = max(
            100,
            int(getattr(self, "price_history_keep_rows", 0) or 0),
            int(self.prices.maxlen or 0),
        )
        self.store.append_price_sample(
            symbol=self.symbol,
            ts=timestamp,
            close=close,
            volume=volume,
            max_rows_per_symbol=max_rows,
        )

    def _get_account_snapshot_cached(self) -> AccountSnapshot:
        now = time.time()
        cached = self._cached_account_snapshot
        if cached is not None and self._local_allowance_backoff_remaining_sec() > 0:
            # Keep using cached account state while broker allowance backoff is active,
            # so active positions do not amplify REST pressure during cooldown windows.
            return AccountSnapshot(
                balance=cached.balance,
                equity=cached.equity,
                margin_free=cached.margin_free,
                timestamp=now,
            )
        if cached is not None and (now - self._cached_account_snapshot_ts) <= self.account_snapshot_cache_ttl_sec:
            return AccountSnapshot(
                balance=cached.balance,
                equity=cached.equity,
                margin_free=cached.margin_free,
                timestamp=now,
            )
        snapshot = self.broker.get_account_snapshot()
        self._cached_account_snapshot = snapshot
        self._cached_account_snapshot_ts = now
        return snapshot

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

    def _normalize_tick_timestamp_for_history(self, raw_ts: float | int | str | None) -> float:
        now = time.time()
        normalized = self._normalize_timestamp_seconds(raw_ts)
        max_future_skew_sec = 120.0
        if normalized > (now + max_future_skew_sec):
            normalized = now
        if self.price_timestamps:
            last_ts = float(self.price_timestamps[-1])
            if last_ts > (now + max_future_skew_sec):
                # Re-anchor corrupted future history timestamps to current wall clock.
                shift = last_ts - now
                self.price_timestamps = deque(
                    (float(ts) - shift for ts in self.price_timestamps),
                    maxlen=self.price_timestamps.maxlen,
                )
                last_ts = float(self.price_timestamps[-1])
            if normalized <= (last_ts + 1e-9):
                # Broker timestamp can stall while quotes are still polled.
                # Use local clock fallback so candle buckets continue to progress.
                normalized = max(now, last_ts + 1e-3)
        return normalized

    def _pip_size_fallback(self) -> float:
        upper = self.symbol.upper()
        if upper in {"AUS200", "AU200"}:
            return 1.0
        if upper in {"US100", "US500", "US30", "DE40", "UK100", "FRA40", "JP225", "EU50"}:
            return 1.0
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return 1.0
        if upper.endswith("JPY"):
            return 0.01
        if upper.startswith("XAU") or upper.startswith("XAG"):
            return 0.1
        if upper in {"WTI", "BRENT"}:
            return 1.0
        return 0.0001

    def _contract_multiplier_fallback(self) -> float:
        upper = self.symbol.upper()
        if upper.endswith("USD") or upper.startswith("USD"):
            return 100000.0
        if upper.startswith("XAU"):
            return 100.0
        if upper in {"WTI", "BRENT"}:
            return 100.0
        if upper.startswith("US"):
            return 10.0
        return 1000.0

    def _build_sl_tp(self, side: Side, entry: float, stop_pips: float, take_pips: float) -> tuple[float, float]:
        pip_size = self.symbol_spec.tick_size if self.symbol_spec is not None else self._pip_size_fallback()
        effective_stop_pips = max(stop_pips, self.min_stop_loss_pips)
        effective_take_pips = max(take_pips, effective_stop_pips * self.min_tp_sl_ratio)
        stop_dist = effective_stop_pips * pip_size
        take_dist = effective_take_pips * pip_size

        if side == Side.BUY:
            return entry - stop_dist, entry + take_dist
        return entry + stop_dist, entry - take_dist

    def _calculate_pnl(self, position: Position, price: float) -> float:
        if self.symbol_spec is not None:
            tick_size = max(self.symbol_spec.tick_size, 1e-9)
            ticks = (price - position.open_price) / tick_size
            signed_ticks = ticks if position.side == Side.BUY else -ticks
            return signed_ticks * self.symbol_spec.tick_value * position.volume

        multiplier = self._contract_multiplier_fallback()
        if position.side == Side.BUY:
            return (price - position.open_price) * position.volume * multiplier
        return (position.open_price - price) * position.volume * multiplier

    def _should_close_position(self, position: Position, bid: float, ask: float) -> tuple[bool, float, str]:
        mark = bid if position.side == Side.BUY else ask

        if position.side == Side.BUY:
            if mark <= position.stop_loss:
                return True, mark, "stop_loss"
            if mark >= position.take_profit:
                return True, mark, "take_profit"
        else:
            if mark >= position.stop_loss:
                return True, mark, "stop_loss"
            if mark <= position.take_profit:
                return True, mark, "take_profit"

        return False, mark, ""

    @staticmethod
    def _is_broker_position_missing_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        if "position.details.null.error" in lowered:
            return True
        if "position.notional.details.null.error" in lowered:
            return True
        if "positions/otc" in lowered and "404" in lowered and "error.service.marketdata.position" in lowered:
            return True
        if "positions/otc/" in lowered and "404" in lowered:
            return True
        return False

    def _infer_missing_position_close(
        self,
        position: Position,
        bid: float | None,
        ask: float | None,
    ) -> tuple[float, str]:
        tick_size = self.symbol_spec.tick_size if self.symbol_spec is not None else self._pip_size_fallback()
        tolerance = max(tick_size * 2.0, 1e-9)

        if bid is not None and ask is not None:
            mark = bid if position.side == Side.BUY else ask
        else:
            mark = position.open_price

        if position.side == Side.BUY:
            if mark <= (position.stop_loss + tolerance):
                return position.stop_loss, "stop_loss"
            if mark >= (position.take_profit - tolerance):
                return position.take_profit, "take_profit"
            return mark, "mark"

        if mark >= (position.stop_loss - tolerance):
            return position.stop_loss, "stop_loss"
        if mark <= (position.take_profit + tolerance):
            return position.take_profit, "take_profit"
        return mark, "mark"

    @staticmethod
    def _safe_float(value: object) -> float | None:
        try:
            parsed = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed):
            return None
        return parsed

    @staticmethod
    def _normalize_deal_reference(value: str) -> str:
        return "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_"})

    def _validate_broker_close_sync(
        self,
        position: Position,
        payload: dict[str, object],
        *,
        expected_deal_reference: str | None = None,
    ) -> dict[str, object] | None:
        closed_at = self._safe_float(payload.get("closed_at"))
        source = str(payload.get("source") or "").strip().lower()
        match_mode = str(payload.get("history_match_mode") or "").strip().lower()
        expected_reference = self._normalize_deal_reference(str(expected_deal_reference or "").strip()) if expected_deal_reference else ""
        payload_reference_raw = str(
            payload.get("deal_reference") or payload.get("history_reference") or ""
        ).strip()
        payload_reference = self._normalize_deal_reference(payload_reference_raw) if payload_reference_raw else ""

        # Reject impossible chronology: broker close timestamp earlier than local open timestamp.
        if (
            closed_at is not None
            and closed_at > 0
            and position.opened_at > 0
            and (closed_at + 2.0) < float(position.opened_at)
        ):
            self.store.record_event(
                "WARN",
                self.symbol,
                "Broker close sync rejected as stale",
                {
                    "position_id": position.position_id,
                    "source": source,
                    "history_match_mode": match_mode or None,
                    "broker_closed_at": closed_at,
                    "position_opened_at": float(position.opened_at),
                },
            )
            return None

        # For history-based reconciliation we enforce deal-reference equality only when the
        # match itself was not position-id based. IG system SL/TP close rows can omit open
        # deal reference while still containing the position/deal id in textual payloads.
        if source == "ig_history_transactions" and expected_reference:
            strict_reference_required = match_mode != "position_id"
            if strict_reference_required and not payload_reference:
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "Broker close sync rejected due to missing deal reference",
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "history_match_mode": match_mode,
                        "expected_deal_reference": expected_reference,
                    },
                )
                return None
            if strict_reference_required and payload_reference != expected_reference:
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "Broker close sync rejected by deal reference mismatch",
                    {
                        "position_id": position.position_id,
                        "source": source,
                        "history_match_mode": match_mode,
                        "expected_deal_reference": expected_reference,
                        "payload_deal_reference": payload_reference,
                    },
                )
                return None

        return payload

    def _get_broker_close_sync(self, position: Position) -> dict[str, object] | None:
        getter = getattr(self.broker, "get_position_close_sync", None)
        if not callable(getter):
            return None
        expected_deal_reference = self.store.get_trade_deal_reference(position.position_id)
        if expected_deal_reference:
            expected_deal_reference = self._normalize_deal_reference(expected_deal_reference)
        try:
            payload = getter(
                position.position_id,
                deal_reference=expected_deal_reference,
                symbol=position.symbol,
                opened_at=position.opened_at,
                open_price=position.open_price,
                volume=position.volume,
                side=position.side.value,
                tick_size=self.symbol_spec.tick_size if self.symbol_spec is not None else None,
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
                        self.symbol,
                        "Broker close sync fetch failed",
                        {"position_id": position.position_id, "error": str(exc)},
                    )
                    return None
            except Exception as exc:
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "Broker close sync fetch failed",
                    {"position_id": position.position_id, "error": str(exc)},
                )
                return None
        except Exception as exc:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Broker close sync fetch failed",
                {"position_id": position.position_id, "error": str(exc)},
            )
            return None
        if not isinstance(payload, dict):
            return None
        return self._validate_broker_close_sync(
            position,
            payload,
            expected_deal_reference=expected_deal_reference,
        )

    def _get_broker_open_sync(self, position_id: str) -> dict[str, object] | None:
        getter = getattr(self.broker, "get_position_open_sync", None)
        if not callable(getter):
            return None
        try:
            payload = getter(position_id)
        except Exception as exc:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Broker open sync fetch failed",
                {"position_id": position_id, "error": str(exc)},
            )
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _normalize_price(self, price: float) -> float:
        if self.symbol_spec is None:
            return price
        tick_size = max(self.symbol_spec.tick_size, 1e-9)
        normalized = round(price / tick_size) * tick_size
        return round(normalized, self.symbol_spec.price_precision)

    def _normalize_price_floor(self, price: float) -> float:
        if self.symbol_spec is None:
            return price
        tick_size = max(self.symbol_spec.tick_size, 1e-9)
        tick = Decimal(str(tick_size))
        value = Decimal(str(price))
        steps = (value / tick).to_integral_value(rounding=ROUND_FLOOR)
        normalized = steps * tick
        return round(float(normalized), self.symbol_spec.price_precision)

    def _normalize_price_ceil(self, price: float) -> float:
        if self.symbol_spec is None:
            return price
        tick_size = max(self.symbol_spec.tick_size, 1e-9)
        tick = Decimal(str(tick_size))
        value = Decimal(str(price))
        steps = (value / tick).to_integral_value(rounding=ROUND_CEILING)
        normalized = steps * tick
        return round(float(normalized), self.symbol_spec.price_precision)

    def _broker_min_stop_distance_price(self) -> float:
        if self.symbol_spec is None:
            return 0.0
        metadata = self.symbol_spec.metadata if isinstance(self.symbol_spec.metadata, dict) else {}
        raw = metadata.get("min_stop_distance_price")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(value) or value <= 0:
            return 0.0
        return value

    def _apply_broker_min_stop_distance_guard(
        self,
        position: Position,
        desired_stop_loss: float,
        bid: float,
        ask: float,
    ) -> float | None:
        min_stop_distance = self._broker_min_stop_distance_price()
        if min_stop_distance <= 0:
            return desired_stop_loss

        if position.side == Side.BUY:
            max_allowed_stop = self._normalize_price_floor(bid - min_stop_distance)
            if max_allowed_stop <= position.stop_loss:
                return None
            adjusted = min(desired_stop_loss, max_allowed_stop)
            adjusted = self._normalize_price_floor(adjusted)
            if adjusted <= position.stop_loss:
                return None
            return adjusted

        min_allowed_stop = self._normalize_price_ceil(ask + min_stop_distance)
        if min_allowed_stop >= position.stop_loss:
            return None
        adjusted = max(desired_stop_loss, min_allowed_stop)
        adjusted = self._normalize_price_ceil(adjusted)
        if adjusted >= position.stop_loss:
            return None
        return adjusted

    def _apply_broker_open_level_guard(
        self,
        side: Side,
        stop_loss: float,
        take_profit: float,
        bid: float,
        ask: float,
    ) -> tuple[float, float, dict[str, float] | None]:
        if self.mode != RunMode.EXECUTION:
            return stop_loss, take_profit, None

        min_stop_distance = self._broker_min_stop_distance_price()
        if min_stop_distance <= 0 or bid <= 0 or ask <= 0:
            return stop_loss, take_profit, None

        original_stop_loss = stop_loss
        original_take_profit = take_profit

        if side == Side.BUY:
            max_allowed_stop = self._normalize_price_floor(bid - min_stop_distance)
            min_allowed_take_profit = self._normalize_price_ceil(ask + min_stop_distance)
            stop_loss = min(stop_loss, max_allowed_stop)
            take_profit = max(take_profit, min_allowed_take_profit)
            stop_loss = self._normalize_price_floor(stop_loss)
            take_profit = self._normalize_price_ceil(take_profit)
        else:
            min_allowed_stop = self._normalize_price_ceil(ask + min_stop_distance)
            max_allowed_take_profit = self._normalize_price_floor(bid - min_stop_distance)
            stop_loss = max(stop_loss, min_allowed_stop)
            take_profit = min(take_profit, max_allowed_take_profit)
            stop_loss = self._normalize_price_ceil(stop_loss)
            take_profit = self._normalize_price_floor(take_profit)

        changed = (
            abs(stop_loss - original_stop_loss) > 1e-12
            or abs(take_profit - original_take_profit) > 1e-12
        )
        if not changed:
            return stop_loss, take_profit, None

        payload = {
            "original_stop_loss": original_stop_loss,
            "adjusted_stop_loss": stop_loss,
            "original_take_profit": original_take_profit,
            "adjusted_take_profit": take_profit,
            "min_stop_distance_price": min_stop_distance,
            "bid": bid,
            "ask": ask,
        }
        return stop_loss, take_profit, payload

    def _persist_trailing_override(self, position_id: str, override: dict[str, float]) -> None:
        payload = {
            "position_id": position_id,
            "override": override,
        }
        self.store.set_kv(self._strategy_trailing_store_key, json.dumps(payload))

    def _load_persisted_trailing_override_for_position(self, position_id: str) -> dict[str, float] | None:
        raw = self.store.get_kv(self._strategy_trailing_store_key)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        if str(payload.get("position_id")) != position_id:
            return None
        override = payload.get("override")
        if not isinstance(override, dict):
            return None
        normalized: dict[str, float] = {}
        for key in (
            "trailing_activation_ratio",
            "trailing_distance_pips",
            "trailing_breakeven_offset_pips",
        ):
            if key not in override:
                continue
            try:
                normalized[key] = float(override[key])
            except (TypeError, ValueError):
                continue
        if not normalized:
            return None
        return normalized

    def _clear_persisted_trailing_override(self) -> None:
        self.store.delete_kv(self._strategy_trailing_store_key)

    def _set_position_trailing_override(self, position_id: str, override: dict[str, float] | None) -> None:
        self._strategy_trailing_overrides_by_position.clear()
        if override is None:
            self._clear_persisted_trailing_override()
            return
        self._strategy_trailing_overrides_by_position[position_id] = override
        self._persist_trailing_override(position_id, override)

    def _get_position_trailing_override(self, position_id: str) -> dict[str, float] | None:
        override = self._strategy_trailing_overrides_by_position.get(position_id)
        if override is not None:
            return override
        loaded = self._load_persisted_trailing_override_for_position(position_id)
        if loaded is None:
            return None
        self._strategy_trailing_overrides_by_position[position_id] = loaded
        return loaded

    def _parse_trailing_override_from_signal(
        self,
        signal: Signal,
        entry_price: float,
        take_profit_price: float,
    ) -> dict[str, float] | None:
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        trailing_raw = metadata.get("trailing_stop")
        if not isinstance(trailing_raw, dict):
            return None
        if not bool(trailing_raw.get("trailing_enabled", False)):
            return None

        distance_raw = trailing_raw.get("trailing_distance_pips")
        try:
            distance_pips = float(distance_raw)
        except (TypeError, ValueError):
            return None
        if distance_pips <= 0:
            return None

        pip_size = self.symbol_spec.tick_size if self.symbol_spec is not None else self._pip_size_fallback()
        tp_distance_pips = abs(take_profit_price - entry_price) / max(pip_size, 1e-9)
        if tp_distance_pips <= 0:
            return None

        activation_ratio: float | None = None
        activation_ratio_raw = trailing_raw.get("trailing_activation_ratio")
        if activation_ratio_raw is not None:
            try:
                activation_ratio = float(activation_ratio_raw)
            except (TypeError, ValueError):
                activation_ratio = None

        if activation_ratio is None:
            activation_pips_raw = trailing_raw.get("trailing_activation_pips")
            if activation_pips_raw is not None:
                try:
                    activation_pips = float(activation_pips_raw)
                except (TypeError, ValueError):
                    activation_pips = None
                if activation_pips is not None:
                    activation_ratio = activation_pips / max(tp_distance_pips, 1e-9)

        if activation_ratio is None:
            activation_ratio = self.trailing_activation_ratio

        breakeven_offset_pips = self._trailing_breakeven_offset_for_symbol_pips()
        breakeven_offset_raw = trailing_raw.get("trailing_breakeven_offset_pips")
        if breakeven_offset_raw is not None:
            try:
                breakeven_offset_pips = float(breakeven_offset_raw)
            except (TypeError, ValueError):
                pass

        return {
            "trailing_activation_ratio": max(0.0, min(1.0, activation_ratio)),
            "trailing_distance_pips": max(0.1, distance_pips),
            "trailing_breakeven_offset_pips": max(0.0, breakeven_offset_pips),
        }

    def _effective_trailing_settings(self, position: Position) -> dict[str, float]:
        override = self._get_position_trailing_override(position.position_id)
        if override is None:
            return {
                "trailing_activation_ratio": self.trailing_activation_ratio,
                "trailing_distance_pips": self.trailing_distance_pips,
                "trailing_breakeven_offset_pips": self._trailing_breakeven_offset_for_symbol_pips(),
            }
        return {
            "trailing_activation_ratio": max(
                0.0,
                min(1.0, float(override.get("trailing_activation_ratio", self.trailing_activation_ratio))),
            ),
            "trailing_distance_pips": max(
                0.1,
                float(override.get("trailing_distance_pips", self.trailing_distance_pips)),
            ),
            "trailing_breakeven_offset_pips": max(
                0.0,
                float(
                    override.get(
                        "trailing_breakeven_offset_pips",
                        self._trailing_breakeven_offset_for_symbol_pips(),
                    )
                ),
            ),
        }

    def _trailing_candidate_stop(
        self, position: Position, bid: float, ask: float
    ) -> tuple[float | None, float]:
        mark = bid if position.side == Side.BUY else ask
        tp_distance = (
            position.take_profit - position.open_price
            if position.side == Side.BUY
            else position.open_price - position.take_profit
        )
        if tp_distance <= 0:
            return None, 0.0

        trailing_settings = self._effective_trailing_settings(position)
        trailing_activation_ratio = trailing_settings["trailing_activation_ratio"]
        trailing_distance_pips = trailing_settings["trailing_distance_pips"]
        breakeven_offset_pips = trailing_settings["trailing_breakeven_offset_pips"]

        moved = (
            mark - position.open_price
            if position.side == Side.BUY
            else position.open_price - mark
        )
        progress = moved / tp_distance
        # Trailing starts only after position becomes profitable.
        if moved <= 0:
            return None, progress
        if progress + 1e-9 < trailing_activation_ratio:
            return None, progress

        pip_size = self.symbol_spec.tick_size if self.symbol_spec is not None else self._pip_size_fallback()
        trail_distance = trailing_distance_pips * pip_size
        breakeven_offset = breakeven_offset_pips * pip_size

        if position.side == Side.BUY:
            breakeven_level = position.open_price + breakeven_offset
            if mark <= breakeven_level:
                return None, progress
            desired = max(position.stop_loss, breakeven_level, mark - trail_distance)
            desired = self._normalize_price(desired)
            guarded = self._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
            if guarded is not None and guarded > position.stop_loss:
                return guarded, progress
            return None, progress

        breakeven_level = position.open_price - breakeven_offset
        if mark >= breakeven_level:
            return None, progress
        desired = min(position.stop_loss, breakeven_level, mark + trail_distance)
        desired = self._normalize_price(desired)
        guarded = self._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
        if guarded is not None and guarded < position.stop_loss:
            return guarded, progress
        return None, progress

    def _apply_trailing_stop(self, position: Position, new_stop_loss: float, progress: float) -> None:
        old_stop = position.stop_loss
        position.stop_loss = new_stop_loss
        try:
            if self.mode == RunMode.EXECUTION:
                self.broker.modify_position(position, new_stop_loss, position.take_profit)
        except Exception:
            position.stop_loss = old_stop
            raise
        self.store.upsert_trade(position, self.name, self.strategy_name, self.mode.value)
        trailing_settings = self._effective_trailing_settings(position)
        self.store.record_event(
            "WARN",
            self.symbol,
            "Trailing stop adjusted",
            {
                "position_id": position.position_id,
                "old_stop_loss": old_stop,
                "new_stop_loss": new_stop_loss,
                "take_profit": position.take_profit,
                "activation_ratio": trailing_settings["trailing_activation_ratio"],
                "progress_to_tp": round(progress, 4),
                "trailing_distance_pips": trailing_settings["trailing_distance_pips"],
                "trailing_breakeven_offset_pips": trailing_settings["trailing_breakeven_offset_pips"],
            },
        )
        logger.warning(
            "Trailing stop adjusted | symbol=%s position_id=%s old_sl=%s new_sl=%s progress=%s",
            self.symbol,
            position.position_id,
            old_stop,
            new_stop_loss,
            round(progress, 4),
        )

    def _is_index_symbol(self) -> bool:
        upper = self.symbol.upper()
        if upper in {"US100", "US500", "US30", "DE40", "UK100", "FRA40", "JP225", "EU50"}:
            return True
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return True
        return False

    def _epic_text(self) -> str:
        if self.symbol_spec is None or not isinstance(self.symbol_spec.metadata, dict):
            return ""
        return str(self.symbol_spec.metadata.get("epic") or "").strip().upper()

    def _is_crypto_symbol(self) -> bool:
        upper = self.symbol.upper()
        if upper in {"BTC", "ETH", "LTC", "BCH", "DOGE", "XRP", "SOL"}:
            return True
        epic = self._epic_text()
        return any(token in epic for token in ("BITCOIN", "ETHER", "ETHEREUM", "CRYPTO", "DOGE", "LITECOIN", "RIPPLE", "SOLANA"))

    def _is_commodity_symbol(self) -> bool:
        upper = self.symbol.upper()
        if upper.startswith(("XAU", "XAG")):
            return True
        if upper in {
            "WTI",
            "BRENT",
            "XAUUSD",
            "XAGUSD",
            "GOLD",
            "SILVER",
            "NGAS",
            "NATGAS",
            "COPPER",
        }:
            return True
        return False

    def _is_fx_symbol(self) -> bool:
        upper = self.symbol.upper()
        if self._is_index_symbol() or self._is_commodity_symbol() or self._is_crypto_symbol():
            return False
        return len(upper) == 6 and upper.isalpha()

    def _is_non_fx_cfd_symbol(self) -> bool:
        if self._is_fx_symbol():
            return False
        epic = self._epic_text()
        if epic.startswith(("IX.", "CC.", "CS.", "UA.", "SA.")):
            return True
        upper = self.symbol.upper()
        if self._is_index_symbol() or self._is_commodity_symbol() or self._is_crypto_symbol():
            return True
        if upper.startswith(("AAPL", "MSFT")):
            return True
        return False

    def _trailing_breakeven_offset_for_symbol_pips(self) -> float:
        if self._is_index_symbol():
            return self.trailing_breakeven_offset_pips_index
        if self._is_commodity_symbol():
            return self.trailing_breakeven_offset_pips_commodity
        return self.trailing_breakeven_offset_pips_fx

    def _session_exit_reason(self, now_ts: float) -> str | None:
        if not self._is_index_symbol():
            return None
        close_ts = self.broker.get_session_close_utc(self.symbol, now_ts)
        if close_ts is None:
            return None
        seconds_left = int(close_ts - now_ts)
        if seconds_left <= self.session_close_buffer_sec:
            return f"session_end_buffer:{seconds_left}s"
        return None

    def _spread_in_pips(self, bid: float, ask: float) -> float:
        pip_size = self.symbol_spec.tick_size if self.symbol_spec is not None else self._pip_size_fallback()
        return max(0.0, (ask - bid) / max(pip_size, 1e-9))

    @staticmethod
    def _spread_in_pct(bid: float, ask: float) -> float:
        if bid <= 0 and ask <= 0:
            return 0.0
        if bid <= 0:
            mid = ask
        elif ask <= 0:
            mid = bid
        else:
            mid = (bid + ask) / 2.0
        if mid <= 0:
            return 0.0
        return max(0.0, ((ask - bid) / mid) * 100.0)

    def _spread_pct_limit(self) -> tuple[float | None, str]:
        if not self.spread_pct_filter_enabled:
            return None, "disabled"
        if self._is_crypto_symbol() and self.spread_max_pct_crypto > 0:
            return self.spread_max_pct_crypto, "crypto"
        if self._is_non_fx_cfd_symbol() and self.spread_max_pct_cfd > 0:
            return self.spread_max_pct_cfd, "cfd"
        if self.spread_max_pct > 0:
            return self.spread_max_pct, "global"
        return None, "off"

    def _spread_pct_allows_open(self, current_spread_pct: float, current_spread_pips: float) -> bool:
        limit_pct, scope = self._spread_pct_limit()
        if limit_pct is None or current_spread_pct <= limit_pct:
            return True
        payload = {
            "current_spread_pct": round(current_spread_pct, 6),
            "limit_pct": round(limit_pct, 6),
            "scope": scope,
            "current_spread_pips": round(current_spread_pips, 6),
        }
        if self.symbol_spec is not None and isinstance(self.symbol_spec.metadata, dict):
            epic = str(self.symbol_spec.metadata.get("epic") or "").strip()
            if epic:
                payload["epic"] = epic
        self.store.record_event(
            "WARN",
            self.symbol,
            "Trade blocked by spread pct filter",
            payload,
        )
        logger.warning(
            "Trade blocked by spread pct filter | symbol=%s spread_pct=%.4f limit_pct=%.4f scope=%s spread_pips=%.4f",
            self.symbol,
            current_spread_pct,
            limit_pct,
            scope,
            current_spread_pips,
        )
        return False

    def _spread_filter_metrics(self, current_spread_pips: float) -> tuple[bool, float, float]:
        if not self.spread_filter_enabled:
            return False, 0.0, 0.0
        if len(self.spreads_pips) < self.spread_min_samples:
            return False, 0.0, 0.0

        avg_spread = sum(self.spreads_pips) / len(self.spreads_pips)
        threshold = avg_spread * self.spread_anomaly_multiplier
        return current_spread_pips > threshold, avg_spread, threshold

    def _connectivity_allows_open(self) -> bool:
        if not self.connectivity_check_enabled:
            return True

        now = time.time()
        status = self._cached_connectivity_status
        should_probe = status is None or now >= self._next_connectivity_probe_ts
        if should_probe:
            status = self.broker.get_connectivity_status(
                max_latency_ms=self.connectivity_max_latency_ms,
                pong_timeout_sec=self.connectivity_pong_timeout_sec,
            )
            self._cached_connectivity_status = status
            self._next_connectivity_probe_ts = now + self.connectivity_check_interval_sec
            if not status.healthy and self._is_allowance_backoff_error(status.reason):
                backoff_sec = self._handle_allowance_backoff_error(status.reason)
                self._next_connectivity_probe_ts = max(self._next_connectivity_probe_ts, now + backoff_sec)

        if status is None:
            return False
        if status.healthy:
            self._last_connectivity_block_reason = None
            return True

        reason_changed = status.reason != self._last_connectivity_block_reason
        cooldown_passed = (now - self._last_connectivity_block_event_ts) >= self.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Trade blocked by connectivity check",
                {
                    "reason": status.reason,
                    "latency_ms": status.latency_ms,
                    "pong_ok": status.pong_ok,
                    "latency_limit_ms": self.connectivity_max_latency_ms,
                    "pong_timeout_sec": self.connectivity_pong_timeout_sec,
                    "probe_interval_sec": self.connectivity_check_interval_sec,
                },
            )
            logger.warning(
                "Trade blocked by connectivity check | symbol=%s reason=%s latency_ms=%s pong_ok=%s",
                self.symbol,
                status.reason,
                status.latency_ms,
                status.pong_ok,
            )
            self._last_connectivity_block_reason = status.reason
            self._last_connectivity_block_event_ts = now
        return False

    def _stream_health_payload(self, status: StreamHealthStatus) -> dict[str, object]:
        payload = asdict(status)
        payload["max_tick_age_sec"] = self.stream_max_tick_age_sec
        payload["event_cooldown_sec"] = self.stream_event_cooldown_sec
        return payload

    def _maybe_record_stream_usage_metrics(self, status: StreamHealthStatus) -> None:
        total = int(getattr(status, "price_requests_total", 0) or 0)
        stream_hits = int(getattr(status, "stream_hits_total", 0) or 0)
        rest_hits = int(getattr(status, "rest_fallback_hits_total", 0) or 0)
        if total <= 0:
            return

        metrics_key = (total, stream_hits, rest_hits)
        now = time.time()
        if metrics_key == self._last_stream_metrics_key:
            return
        if (now - self._last_stream_metrics_event_ts) < self.stream_event_cooldown_sec:
            return

        self.store.record_event(
            "INFO",
            self.symbol,
            "Stream usage metrics",
            {
                "price_requests_total": total,
                "stream_hits_total": stream_hits,
                "rest_fallback_hits_total": rest_hits,
                "stream_hit_rate_pct": status.stream_hit_rate_pct,
                "stream_connected": status.connected,
                "stream_reason": status.reason,
            },
        )
        self._last_stream_metrics_event_ts = now
        self._last_stream_metrics_key = metrics_key

    def _refresh_stream_health(self) -> StreamHealthStatus | None:
        if not self.stream_health_check_enabled:
            self._last_stream_health = None
            return None

        status = self.broker.get_stream_health_status(
            symbol=self.symbol,
            max_tick_age_sec=self.stream_max_tick_age_sec,
        )
        self._maybe_record_stream_usage_metrics(status)
        previous = self._last_stream_health
        self._last_stream_health = status
        now = time.time()

        if previous is None:
            if not status.healthy:
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "Stream health degraded",
                    self._stream_health_payload(status),
                )
                self._last_stream_health_event_ts = now
            return status

        if status.healthy:
            if not previous.healthy:
                self.store.record_event(
                    "INFO",
                    self.symbol,
                    "Stream health recovered",
                    self._stream_health_payload(status),
                )
                self._last_stream_health_event_ts = now
            return status

        reason_changed = status.reason != previous.reason
        cooldown_passed = (now - self._last_stream_health_event_ts) >= self.stream_event_cooldown_sec
        if previous.healthy or reason_changed or cooldown_passed:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Stream health degraded",
                self._stream_health_payload(status),
            )
            self._last_stream_health_event_ts = now
        return status

    def _stream_health_allows_open(self, status: StreamHealthStatus | None) -> bool:
        if not self.stream_health_check_enabled:
            return True

        if status is None:
            status = self._refresh_stream_health()
        if status is None or status.healthy:
            return True

        now = time.time()
        reason_changed = status.reason != self._last_stream_block_reason
        cooldown_passed = (now - self._last_stream_block_event_ts) >= self.stream_event_cooldown_sec
        if reason_changed or cooldown_passed:
            self.store.record_event(
                "WARN",
                self.symbol,
                "Trade blocked by stream health check",
                self._stream_health_payload(status),
            )
            self._last_stream_block_event_ts = now
            self._last_stream_block_reason = status.reason

        logger.warning(
            "Trade blocked by stream health check | symbol=%s reason=%s connected=%s age_sec=%s",
            self.symbol,
            status.reason,
            status.connected,
            status.last_tick_age_sec,
        )
        return False

    def _apply_breakeven_protection(
        self,
        position: Position,
        trigger: str,
        event_id: str | None = None,
        bid: float | None = None,
        ask: float | None = None,
    ) -> bool:
        if position.side == Side.BUY:
            new_stop_loss = max(position.stop_loss, position.open_price)
            if new_stop_loss <= position.stop_loss:
                return False
        else:
            new_stop_loss = min(position.stop_loss, position.open_price)
            if new_stop_loss >= position.stop_loss:
                return False

        new_stop_loss = self._normalize_price(new_stop_loss)
        if bid is not None and ask is not None:
            guarded = self._apply_broker_min_stop_distance_guard(position, new_stop_loss, bid, ask)
            if guarded is None:
                return False
            new_stop_loss = guarded
        old_stop = position.stop_loss
        position.stop_loss = new_stop_loss
        try:
            if self.mode == RunMode.EXECUTION:
                self.broker.modify_position(position, new_stop_loss, position.take_profit)
        except Exception:
            position.stop_loss = old_stop
            raise
        self.store.upsert_trade(position, self.name, self.strategy_name, self.mode.value)
        self.store.record_event(
            "WARN",
            self.symbol,
            "Breakeven protection applied",
            {
                "position_id": position.position_id,
                "trigger": trigger,
                "event_id": event_id,
                "new_stop_loss": new_stop_loss,
                "open_price": position.open_price,
                "take_profit": position.take_profit,
            },
        )
        logger.warning(
            "Breakeven protection applied | symbol=%s position_id=%s trigger=%s new_sl=%s",
            self.symbol,
            position.position_id,
            trigger,
            new_stop_loss,
        )
        return True

    def _apply_news_filter(self, position: Position, now_ts: float, bid: float, ask: float) -> str | None:
        if not self.news_filter_enabled or self.news_event_buffer_sec <= 0:
            return None

        events = self.broker.get_upcoming_high_impact_events(now_ts, self.news_event_buffer_sec)
        if not events:
            return None

        if len(self._handled_news_events) > 500:
            current_event_ids = {e.event_id for e in events}
            self._handled_news_events = self._handled_news_events & current_event_ids

        for event in events:
            if event.event_id in self._handled_news_events:
                continue
            self._handled_news_events.add(event.event_id)

            if self.news_event_action == "close":
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "News filter close trigger",
                    {
                        "position_id": position.position_id,
                        "event_id": event.event_id,
                        "event_name": event.name,
                        "event_time": event.timestamp,
                    },
                )
                return f"news_event_close:{event.name}"

            self._apply_breakeven_protection(
                position,
                trigger=f"news_event:{event.name}",
                event_id=event.event_id,
                bid=bid,
                ask=ask,
            )
        return None

    def _normalize_volume(self, raw_volume: float) -> float:
        if raw_volume <= 0:
            return 0.0

        if self.symbol_spec is None:
            return round(raw_volume, 2)

        step = self.symbol_spec.lot_step if self.symbol_spec.lot_step > 0 else 0.01
        step_dec = Decimal(str(step))
        raw_dec = Decimal(str(raw_volume)) + (step_dec * Decimal("1e-9"))
        steps = (raw_dec / step_dec).to_integral_value(rounding=ROUND_FLOOR)
        normalized = float(steps * step_dec)
        normalized = min(normalized, self.symbol_spec.lot_max)

        if normalized < self.symbol_spec.lot_min:
            return 0.0

        return round(normalized, self.symbol_spec.lot_precision)

    def _open_position(
        self,
        side: Side,
        entry: float,
        stop_loss: float,
        take_profit: float,
        volume: float,
        confidence: float = 0.0,
        trailing_override: dict[str, float] | None = None,
    ) -> None:
        comment, trade_uid = self._build_magic_comment()
        pending_id = self._normalize_deal_reference(comment)
        deal_reference: str | None = pending_id if self.mode == RunMode.EXECUTION else None
        pending_cleanup_required = False

        if self.mode == RunMode.EXECUTION:
            self.store.upsert_pending_open(
                PendingOpen(
                    pending_id=pending_id,
                    symbol=self.symbol,
                    side=side,
                    volume=volume,
                    entry=entry,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    created_at=time.time(),
                    thread_name=self.name,
                    strategy=self.strategy_name,
                    mode=self.mode.value,
                    entry_confidence=confidence,
                    trailing_override=trailing_override,
                )
            )
            pending_cleanup_required = True

        try:
            if self.mode == RunMode.SIGNAL_ONLY:
                self.store.record_event(
                    "INFO",
                    self.symbol,
                    "Signal generated (signal_only)",
                    {
                        "side": side.value,
                        "entry": entry,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "volume": volume,
                        "confidence": confidence,
                        "trade_uid": trade_uid,
                        "magic_comment": comment,
                        "strategy_trailing_override": trailing_override,
                    },
                )
                return

            if self.mode == RunMode.PAPER:
                position_id = f"paper-{trade_uid}"
            else:
                position_id = self.broker.open_position(
                    symbol=self.symbol,
                    side=side,
                    volume=volume,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    comment=comment,
                    entry_price=entry,
                )
                self.store.update_pending_open_position_id(pending_id, position_id)
                broker_open_sync = self._get_broker_open_sync(position_id)
                if isinstance(broker_open_sync, dict):
                    synced_entry = self._safe_float(broker_open_sync.get("open_price"))
                    synced_stop = self._safe_float(broker_open_sync.get("stop_loss"))
                    synced_take = self._safe_float(broker_open_sync.get("take_profit"))
                    synced_reference = str(broker_open_sync.get("deal_reference") or "").strip()
                    if synced_entry and synced_entry > 0:
                        entry = synced_entry
                    if synced_stop and synced_stop > 0:
                        stop_loss = synced_stop
                    if synced_take and synced_take > 0:
                        take_profit = synced_take
                    if synced_reference:
                        deal_reference = synced_reference
                    self.store.record_event(
                        "INFO",
                        self.symbol,
                        "Broker open sync applied",
                        {
                            "position_id": position_id,
                            "open_price": entry,
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                            "source": broker_open_sync.get("source"),
                        },
                    )

            self._set_position_trailing_override(position_id, trailing_override)
            position = Position(
                position_id=position_id,
                symbol=self.symbol,
                side=side,
                volume=volume,
                open_price=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                opened_at=time.time(),
                entry_confidence=confidence,
                status="open",
            )
            self.position_book.upsert(position)
            self.store.upsert_trade(position, self.name, self.strategy_name, self.mode.value)
            if self.mode == RunMode.EXECUTION and deal_reference:
                conflicting_position_id = self.store.bind_trade_deal_reference(position_id, deal_reference)
                if conflicting_position_id:
                    self.store.record_event(
                        "ERROR",
                        self.symbol,
                        "Duplicate deal reference binding detected",
                        {
                            "position_id": position_id,
                            "deal_reference": deal_reference,
                            "conflicting_position_id": conflicting_position_id,
                        },
                    )
                else:
                    self.store.record_event(
                        "INFO",
                        self.symbol,
                        "Trade deal reference bound",
                        {
                            "position_id": position_id,
                            "deal_reference": deal_reference,
                        },
                    )
            self.store.record_event(
                "INFO",
                self.symbol,
                "Position opened",
                {
                    "position_id": position_id,
                    "side": side.value,
                    "entry": entry,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "volume": volume,
                    "confidence": confidence,
                    "mode": self.mode.value,
                    "trade_uid": trade_uid,
                    "magic_comment": comment,
                    "strategy_trailing_override": trailing_override,
                },
            )
            if self.mode == RunMode.EXECUTION:
                self.store.delete_pending_open(pending_id)
                pending_cleanup_required = False
        finally:
            if self.mode == RunMode.EXECUTION and pending_cleanup_required:
                try:
                    self.store.delete_pending_open(pending_id)
                except Exception as cleanup_exc:
                    logger.warning(
                        "Failed to cleanup pending open after open-position failure | symbol=%s pending_id=%s error=%s",
                        self.symbol,
                        pending_id,
                        cleanup_exc,
                    )

    def _finalize_position_close(
        self,
        position: Position,
        close_price: float,
        reason: str,
        broker_sync: dict[str, object] | None = None,
    ) -> None:
        now = time.time()
        broker_closed_at = self._safe_float((broker_sync or {}).get("closed_at"))
        broker_close_price = self._safe_float((broker_sync or {}).get("close_price"))
        target_close_price = broker_close_price if broker_close_price is not None and broker_close_price > 0 else close_price
        final_close_price = self._normalize_price(target_close_price)
        position.close_price = final_close_price
        final_closed_at = now
        if broker_closed_at is not None and broker_closed_at > 0:
            # Guard against invalid future timestamps while preserving broker execution time.
            final_closed_at = min(now, broker_closed_at)
        position.closed_at = final_closed_at
        position.status = "closed"
        local_pnl = self._calculate_pnl(position, final_close_price)
        broker_realized_pnl = self._safe_float((broker_sync or {}).get("realized_pnl"))
        position.pnl = broker_realized_pnl if broker_realized_pnl is not None else local_pnl

        self._set_position_trailing_override(position.position_id, None)
        if self._manual_close_sync_position_id == position.position_id:
            self._manual_close_sync_position_id = None
            self._manual_close_sync_next_check_ts = 0.0
        if self._pending_close_retry_position_id == position.position_id:
            self._pending_close_retry_position_id = None
            self._pending_close_retry_until_ts = 0.0
        self.position_book.remove_by_id(position.position_id)
        self.store.upsert_trade(position, self.name, self.strategy_name, self.mode.value)
        payload: dict[str, object] = {
            "position_id": position.position_id,
            "reason": reason,
            "close_price": final_close_price,
            "pnl": position.pnl,
        }
        if broker_closed_at is not None and broker_closed_at > 0:
            payload["broker_closed_at"] = broker_closed_at
        if broker_sync:
            payload["broker_close_sync"] = broker_sync
            payload["local_pnl_estimate"] = local_pnl
        self.store.record_event(
            "INFO",
            self.symbol,
            "Position closed",
            payload,
        )
        outcome = self._cooldown_outcome_from_pnl(position.pnl)
        cooldown_sec = self._cooldown_duration_for_outcome(outcome)
        if cooldown_sec > 0:
            self._next_entry_allowed_ts = max(self._next_entry_allowed_ts, now + cooldown_sec)
            self._active_entry_cooldown_sec = cooldown_sec
            self._active_entry_cooldown_outcome = outcome
            self.store.record_event(
                "INFO",
                self.symbol,
                "Entry cooldown activated",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "cooldown_sec": cooldown_sec,
                    "next_entry_at": self._next_entry_allowed_ts,
                    "cooldown_outcome": outcome,
                    "trade_pnl": position.pnl,
                    "strategy": self.strategy_name,
                },
            )
        if outcome == "win" and self.same_side_reentry_win_cooldown_sec > 0:
            self._same_side_reentry_block_side = position.side
            self._same_side_reentry_block_until_ts = max(
                self._same_side_reentry_block_until_ts,
                now + self.same_side_reentry_win_cooldown_sec,
            )
            self.store.record_event(
                "INFO",
                self.symbol,
                "Same-side reentry cooldown activated",
                {
                    "position_id": position.position_id,
                    "blocked_side": position.side.value,
                    "cooldown_sec": self.same_side_reentry_win_cooldown_sec,
                    "next_entry_at": self._same_side_reentry_block_until_ts,
                    "trade_pnl": position.pnl,
                    "strategy": self.strategy_name,
                },
            )

    def _close_position(self, position: Position, close_price: float, reason: str) -> None:
        broker_sync: dict[str, object] | None = None
        if self.mode == RunMode.EXECUTION:
            now = time.time()
            if (
                self._pending_close_retry_position_id == position.position_id
                and now < self._pending_close_retry_until_ts
            ):
                wait_sec = max(0.0, self._pending_close_retry_until_ts - now)
                self._record_close_deferred_by_allowance(
                    position=position,
                    reason=reason,
                    wait_sec=wait_sec,
                    error_text="allowance_backoff_cooldown_active",
                )
                return
            try:
                self.broker.close_position(position)
                self._pending_close_retry_position_id = None
                self._pending_close_retry_until_ts = 0.0
                broker_sync = self._get_broker_close_sync(position)
            except BrokerError as exc:
                error_text = str(exc)
                if self._is_allowance_backoff_error(error_text):
                    backoff_sec = self._handle_allowance_backoff_error(error_text)
                    now = time.time()
                    self._pending_close_retry_position_id = position.position_id
                    self._pending_close_retry_until_ts = max(
                        self._pending_close_retry_until_ts,
                        now + max(0.5, backoff_sec),
                    )
                    self._record_close_deferred_by_allowance(
                        position=position,
                        reason=reason,
                        wait_sec=max(0.0, self._pending_close_retry_until_ts - now),
                        error_text=error_text,
                    )
                    return
                if not self._is_broker_position_missing_error(error_text):
                    raise
                # Position was already closed by broker (e.g. SL/TP hit).
                # Try to fetch close sync so we get the real close_price and
                # realized_pnl from the broker instead of inferring locally.
                broker_sync = self._get_broker_close_sync(position)
                inferred_price, inferred_reason = self._infer_missing_position_close(
                    position,
                    close_price,
                    close_price,
                )
                self.store.record_event(
                    "WARN",
                    self.symbol,
                    "Broker position missing on close, finalized locally",
                    {
                        "position_id": position.position_id,
                        "requested_reason": reason,
                        "inferred_reason": inferred_reason,
                        "inferred_close_price": inferred_price,
                        "broker_error": error_text,
                    },
                )
                reason = f"broker_position_missing_on_close:{inferred_reason}"
                close_price = inferred_price
                self._pending_close_retry_position_id = None
                self._pending_close_retry_until_ts = 0.0

        self._finalize_position_close(position, close_price, reason, broker_sync=broker_sync)

    def _handle_missing_broker_position_error(self, error_text: str, tick: PriceTick | None) -> bool:
        if self.mode != RunMode.EXECUTION:
            return False
        if not self._is_broker_position_missing_error(error_text):
            return False

        active = self.position_book.get(self.symbol)
        if active is None:
            return False

        broker_sync = self._get_broker_close_sync(active)
        bid = tick.bid if tick is not None else None
        ask = tick.ask if tick is not None else None
        inferred_price, inferred_reason = self._infer_missing_position_close(active, bid, ask)
        broker_backoff_remaining = self._broker_public_api_backoff_remaining_sec()
        if broker_sync is None and inferred_reason == "mark":
            wait_sec = (
                broker_backoff_remaining
                if broker_backoff_remaining > 0
                else min(5.0, max(1.0, self.manual_close_sync_interval_sec / 4.0))
            )
            self._schedule_missing_position_reconciliation(
                active,
                wait_sec,
                error_text,
            )
            logger.warning(
                "Broker position missing reconciliation delayed | symbol=%s position_id=%s wait_sec=%.2f",
                self.symbol,
                active.position_id,
                wait_sec,
            )
            return True
        sync_source = str((broker_sync or {}).get("source") or "").strip().lower()
        if sync_source:
            final_reason = f"broker_position_missing:{inferred_reason}:{sync_source}"
        else:
            final_reason = f"broker_position_missing:{inferred_reason}"
        self._finalize_position_close(active, inferred_price, final_reason, broker_sync=broker_sync)
        self.store.record_event(
            "WARN",
            self.symbol,
            "Broker position missing reconciled locally",
            {
                "position_id": active.position_id,
                "inferred_reason": inferred_reason,
                "inferred_close_price": inferred_price,
                "broker_close_sync": broker_sync,
                "broker_error": error_text,
            },
        )
        logger.warning(
            "Broker position missing reconciled locally | symbol=%s position_id=%s reason=%s",
            self.symbol,
            active.position_id,
            inferred_reason,
        )
        return True

    def _save_state(self, last_price: float | None, last_error: str | None = None) -> None:
        active = self.position_book.get(self.symbol)
        position_signature: tuple[object, ...] | None = None
        if active is not None:
            position_signature = (
                active.position_id,
                active.status,
                round(float(active.open_price), 8),
                round(float(active.stop_loss), 8),
                round(float(active.take_profit), 8),
                round(float(active.close_price), 8) if active.close_price is not None else None,
            )
        signature = (
            position_signature,
            str(last_error or ""),
        )
        now = time.time()
        if (
            self._last_saved_worker_state_signature == signature
            and (now - self._last_saved_worker_state_ts) < self.worker_state_flush_interval_sec
        ):
            return
        state = WorkerState(
            symbol=self.symbol,
            thread_name=self.name,
            mode=self.mode,
            strategy=self.strategy_name,
            last_price=last_price,
            last_heartbeat=time.time(),
            iteration=self.iteration,
            position=active.to_dict() if active else None,
            last_error=last_error,
        )
        self.store.save_worker_state(state)
        self._last_saved_worker_state_ts = now
        self._last_saved_worker_state_signature = signature

    def _persist_account_snapshot_if_due(
        self,
        snapshot: AccountSnapshot,
        *,
        open_positions: int,
        daily_pnl: float,
        drawdown_pct: float,
    ) -> None:
        signature = (
            round(float(snapshot.balance), 6),
            round(float(snapshot.equity), 6),
            round(float(snapshot.margin_free), 6),
            int(open_positions),
            round(float(daily_pnl), 6),
            round(float(drawdown_pct), 6),
        )
        now = time.time()
        if (
            self._last_account_snapshot_persist_signature == signature
            and (now - self._last_account_snapshot_persist_ts) < self.account_snapshot_persist_interval_sec
        ):
            return
        if (
            self._last_account_snapshot_persist_signature is not None
            and (now - self._last_account_snapshot_persist_ts) < self.account_snapshot_persist_interval_sec
            and int(open_positions) == int(self._last_account_snapshot_persist_signature[3])
        ):
            return
        self.store.record_account_snapshot(
            snapshot=snapshot,
            open_positions=open_positions,
            daily_pnl=daily_pnl,
            drawdown_pct=drawdown_pct,
        )
        self._last_account_snapshot_persist_ts = now
        self._last_account_snapshot_persist_signature = signature

    def _next_wait_duration(self, has_active_position: bool = False) -> float:
        base_interval = self.poll_interval_sec
        if has_active_position:
            base_interval = min(base_interval, self.active_position_poll_interval_sec)
        if base_interval <= 0:
            return 0.0
        if self.poll_jitter_sec <= 0:
            return base_interval
        jitter = self._rng.uniform(-self.poll_jitter_sec, self.poll_jitter_sec)
        return max(0.1, base_interval + jitter)

    def _reverse_signal_exit_reason(self, position: Position) -> str | None:
        signal = self._strategy_exit_signal()
        return self._reverse_signal_exit_reason_from_signal(position, signal)

    def _strategy_exit_signal(self) -> Signal | None:
        if len(self.prices) < self.strategy.min_history:
            return None

        return self.strategy.generate_signal(
            StrategyContext(
                symbol=self.symbol,
                prices=list(self.prices),
                timestamps=list(self.price_timestamps),
                volumes=list(self.volumes),
                current_volume=self.volumes[-1] if self.volumes else None,
                tick_size=self.symbol_spec.tick_size if self.symbol_spec is not None else None,
            )
        )

    @staticmethod
    def _metadata_number(metadata: dict[str, object], key: str) -> float | None:
        raw = metadata.get(key)
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _metadata_bool(metadata: dict[str, object], key: str) -> bool | None:
        raw = metadata.get(key)
        if raw is None:
            return None
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            return None
        if isinstance(raw, (int, float)):
            return bool(raw)
        return None

    def _reverse_signal_exit_reason_from_signal(
        self,
        position: Position,
        signal: Signal | None,
    ) -> str | None:
        if signal is None:
            return None
        if signal.side in {Side.BUY, Side.SELL} and signal.side != position.side:
            return f"reverse_signal:{signal.side.value}"
        return None

    def _strategy_dynamic_exit_reason(
        self,
        position: Position,
        bid: float,
        ask: float,
        signal: Signal | None,
    ) -> str | None:
        if signal is None:
            return None
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}

        # ------------------------------------------------------------------
        # Global trend invalidation: if the strategy exposes fast/slow MA
        # values in metadata, close when the MA relationship has inverted
        # relative to the position side.  Works for ALL strategies.
        # ------------------------------------------------------------------
        fast_ma = self._metadata_number(metadata, "fast_ema") or self._metadata_number(metadata, "fast_ma")
        slow_ma = self._metadata_number(metadata, "slow_ema") or self._metadata_number(metadata, "slow_ma")
        if fast_ma is not None and slow_ma is not None:
            if position.side == Side.BUY and fast_ma <= slow_ma:
                return f"strategy_exit:{self.strategy_name}:trend_invalidated"
            if position.side == Side.SELL and fast_ma >= slow_ma:
                return f"strategy_exit:{self.strategy_name}:trend_invalidated"

        if self.strategy_name == "mean_reversion":
            if metadata.get("indicator") == "mean_reversion" and metadata.get("exit_hint") == "close_on_mean_reversion":
                return "strategy_exit:mean_reversion:mean_reverted"
            return None

        if self.strategy_name == "mean_reversion_bb":
            if metadata.get("indicator") == "bollinger_bands" and metadata.get("exit_hint") == "close_on_bb_midline":
                return "strategy_exit:mean_reversion_bb:midline_reverted"
            return None

        if self.strategy_name == "g1":
            if not self.g1_protective_exit_enabled:
                return None
            if metadata.get("indicator") != "g1":
                return None

            mark = bid if position.side == Side.BUY else ask
            stop_distance = abs(position.open_price - position.stop_loss)
            if stop_distance <= 1e-9:
                return None

            adverse_move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
            if adverse_move <= 0:
                return None

            loss_ratio = adverse_move / stop_distance
            if loss_ratio + 1e-9 < self.g1_protective_exit_loss_ratio:
                return None

            reason = str(metadata.get("reason") or "").strip().lower()
            if self.g1_protective_exit_allow_adx_regime_loss and reason == "adx_below_threshold":
                adx_regime_active_after = self._metadata_bool(metadata, "adx_regime_active_after")
                if adx_regime_active_after is False:
                    return "strategy_exit:g1:loss_guard_adx_regime_lost"
            return None

        if self.strategy_name != "index_hybrid":
            return None

        if metadata.get("indicator") != "index_hybrid":
            return None

        trend_regime = bool(metadata.get("trend_regime"))
        mean_reversion_regime = bool(metadata.get("mean_reversion_regime"))
        channel_mid = self._metadata_number(metadata, "channel_mid")
        channel_upper = self._metadata_number(metadata, "channel_upper")
        channel_lower = self._metadata_number(metadata, "channel_lower")
        mark = bid if position.side == Side.BUY else ask

        if trend_regime and not mean_reversion_regime and channel_mid is not None:
            if position.side == Side.BUY and mark <= channel_mid:
                return "strategy_exit:index_hybrid:trend_mid_reentry"
            if position.side == Side.SELL and mark >= channel_mid:
                return "strategy_exit:index_hybrid:trend_mid_reentry"

        if mean_reversion_regime and not trend_regime:
            if position.side == Side.BUY:
                if channel_mid is not None and mark >= channel_mid:
                    return "strategy_exit:index_hybrid:mean_mid_target"
                if channel_upper is not None and mark >= channel_upper:
                    return "strategy_exit:index_hybrid:mean_opposite_band"
            else:
                if channel_mid is not None and mark <= channel_mid:
                    return "strategy_exit:index_hybrid:mean_mid_target"
                if channel_lower is not None and mark <= channel_lower:
                    return "strategy_exit:index_hybrid:mean_opposite_band"

        return None

    def _load_symbol_spec(self) -> None:
        if self.symbol_spec is not None:
            return

        now = time.time()
        if self._symbol_spec_retry_after_ts > now:
            return

        try:
            self.symbol_spec = self.broker.get_symbol_spec(self.symbol)
        except Exception:
            next_backoff = min(
                120.0,
                max(
                    5.0,
                    self._symbol_spec_retry_backoff_sec * 2.0
                    if self._symbol_spec_retry_backoff_sec > 0
                    else 5.0,
                ),
            )
            self._symbol_spec_retry_backoff_sec = next_backoff
            self._symbol_spec_retry_after_ts = now + next_backoff
            raise
        self._symbol_spec_retry_backoff_sec = 0.0
        self._symbol_spec_retry_after_ts = 0.0
        payload = {
            "tick_size": self.symbol_spec.tick_size,
            "tick_value": self.symbol_spec.tick_value,
            "contract_size": self.symbol_spec.contract_size,
            "lot_min": self.symbol_spec.lot_min,
            "lot_max": self.symbol_spec.lot_max,
            "lot_step": self.symbol_spec.lot_step,
        }
        if self.symbol_spec.metadata:
            payload["metadata"] = self.symbol_spec.metadata
        self.store.record_event(
            "INFO",
            self.symbol,
            "Loaded symbol specification",
            payload,
        )

    def run(self) -> None:
        if self.poll_jitter_sec > 0:
            startup_delay = self._rng.uniform(0.0, self.poll_jitter_sec)
            self.stop_event.wait(startup_delay)

        while not self.stop_event.is_set():
            self.iteration += 1
            last_error: str | None = None
            tick = None
            stream_health: StreamHealthStatus | None = None
            sleep_override_sec: float | None = None

            if self._symbol_disabled:
                last_error = self._symbol_disabled_reason or "symbol_disabled_by_safety_guard"
                self._save_state(
                    last_price=self.prices[-1] if self.prices else None,
                    last_error=last_error,
                )
                self.stop_event.wait(max(self._next_wait_duration(), 5.0))
                continue

            try:
                self._load_symbol_spec()
                tick = self.broker.get_price(self.symbol)
                tick.timestamp = self._normalize_tick_timestamp_for_history(tick.timestamp)
                self.prices.append(tick.mid)
                self.price_timestamps.append(tick.timestamp)
                current_volume: float | None = None
                parsed_volume = 0.0
                if tick.volume is not None:
                    try:
                        parsed_volume = float(tick.volume)
                    except (TypeError, ValueError):
                        parsed_volume = 0.0
                if parsed_volume > 0:
                    current_volume = parsed_volume
                self.volumes.append(parsed_volume)
                self._cache_price_sample(tick.timestamp, tick.mid, current_volume)
                self._reset_broker_error_trackers()
                current_spread_pips = self._spread_in_pips(tick.bid, tick.ask)
                current_spread_pct = self._spread_in_pct(tick.bid, tick.ask)
                spread_blocked, spread_avg, spread_threshold = self._spread_filter_metrics(
                    current_spread_pips
                )
                self.spreads_pips.append(current_spread_pips)
                stream_health = self._refresh_stream_health()

                active = self.position_book.get(self.symbol)
                force_flatten = False
                reason = ""

                if active:
                    if self._attempt_pending_missing_position_reconciliation(active, tick):
                        self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                        self.stop_event.wait(
                            self._next_wait_duration(
                                has_active_position=self.position_book.get(self.symbol) is not None
                            )
                        )
                        continue
                    if self._maybe_reconcile_execution_manual_close(active, tick):
                        self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                        self.stop_event.wait(
                            self._next_wait_duration(
                                has_active_position=self.position_book.get(self.symbol) is not None
                            )
                        )
                        continue
                    snapshot = self._get_account_snapshot_cached()
                    stats = self.risk.compute_stats(snapshot)
                    open_positions_count = self.position_book.count()
                    self._persist_account_snapshot_if_due(
                        snapshot,
                        open_positions=open_positions_count,
                        daily_pnl=stats.daily_pnl,
                        drawdown_pct=stats.total_drawdown_pct,
                    )
                    force_flatten, reason = self.risk.should_force_flatten(
                        snapshot,
                        open_positions_count=open_positions_count,
                    )

                    should_close, mark_price, close_reason = self._should_close_position(
                        active, tick.bid, tick.ask
                    )
                    active.pnl = self._calculate_pnl(active, mark_price)
                    self.store.upsert_trade(active, self.name, self.strategy_name, self.mode.value)

                    if should_close:
                        self._close_position(active, mark_price, close_reason)
                    else:
                        session_reason = self._session_exit_reason(tick.timestamp)
                        if session_reason:
                            self._close_position(active, mark_price, session_reason)
                        else:
                            news_close_reason = self._apply_news_filter(active, tick.timestamp, tick.bid, tick.ask)
                            if news_close_reason:
                                self._close_position(active, mark_price, news_close_reason)
                            else:
                                new_stop, progress = self._trailing_candidate_stop(active, tick.bid, tick.ask)
                                if new_stop is not None:
                                    self._apply_trailing_stop(active, new_stop, progress)

                                exit_signal = self._strategy_exit_signal()
                                reverse_reason = self._reverse_signal_exit_reason_from_signal(active, exit_signal)
                                if reverse_reason:
                                    self._close_position(active, mark_price, reverse_reason)
                                else:
                                    dynamic_exit_reason = self._strategy_dynamic_exit_reason(
                                        active,
                                        tick.bid,
                                        tick.ask,
                                        exit_signal,
                                    )
                                    if dynamic_exit_reason:
                                        self._close_position(active, mark_price, dynamic_exit_reason)
                                    elif self.position_book.get(self.symbol) is not None and force_flatten:
                                        self._close_position(active, mark_price, f"emergency:{reason}")

                active = self.position_book.get(self.symbol)
                if not active:
                    signal = self.strategy.generate_signal(
                        StrategyContext(
                            symbol=self.symbol,
                            prices=list(self.prices),
                            timestamps=list(self.price_timestamps),
                            volumes=list(self.volumes),
                            current_volume=current_volume,
                            current_spread_pips=current_spread_pips,
                            tick_size=self.symbol_spec.tick_size if self.symbol_spec is not None else None,
                        )
                    )
                    self._maybe_record_indicator_debug(signal, current_spread_pips)

                    if signal.side != Side.HOLD:
                        if not self._confidence_allows_open(signal):
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue
                        if not self._cooldown_allows_open(signal.side):
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue
                        if spread_blocked:
                            self.store.record_event(
                                "WARN",
                                self.symbol,
                                "Trade blocked by spread filter",
                                {
                                    "current_spread_pips": round(current_spread_pips, 6),
                                    "average_spread_pips": round(spread_avg, 6),
                                    "anomaly_threshold_pips": round(spread_threshold, 6),
                                    "spread_multiplier": self.spread_anomaly_multiplier,
                                    "spread_samples": len(self.spreads_pips),
                                },
                            )
                            logger.warning(
                                "Trade blocked by spread filter | symbol=%s spread=%.4f avg=%.4f thr=%.4f",
                                self.symbol,
                                current_spread_pips,
                                spread_avg,
                                spread_threshold,
                            )
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue
                        if not self._spread_pct_allows_open(current_spread_pct, current_spread_pips):
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue
                        if not self._connectivity_allows_open():
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue
                        if not self._stream_health_allows_open(stream_health):
                            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
                            self.stop_event.wait(
                                self._next_wait_duration(
                                    has_active_position=self.position_book.get(self.symbol) is not None
                                )
                            )
                            continue

                        snapshot = self._get_account_snapshot_cached()
                        stats = self.risk.compute_stats(snapshot)
                        self._persist_account_snapshot_if_due(
                            snapshot,
                            open_positions=self.position_book.count(),
                            daily_pnl=stats.daily_pnl,
                            drawdown_pct=stats.total_drawdown_pct,
                        )

                        entry = tick.ask if signal.side == Side.BUY else tick.bid
                        stop_loss, take_profit = self._build_sl_tp(
                            signal.side,
                            entry,
                            signal.stop_loss_pips,
                            signal.take_profit_pips,
                        )
                        stop_loss, take_profit, open_level_guard_payload = self._apply_broker_open_level_guard(
                            signal.side,
                            stop_loss,
                            take_profit,
                            tick.bid,
                            tick.ask,
                        )
                        if open_level_guard_payload is not None:
                            self.store.record_event(
                                "INFO",
                                self.symbol,
                                "Broker open levels adjusted",
                                {
                                    "side": signal.side.value,
                                    "entry": entry,
                                    **open_level_guard_payload,
                                },
                            )
                        trailing_override = self._parse_trailing_override_from_signal(
                            signal=signal,
                            entry_price=entry,
                            take_profit_price=take_profit,
                        )

                        decision = self.risk.can_open_trade(
                            snapshot=snapshot,
                            symbol=self.symbol,
                            open_positions_count=self.position_book.count(),
                            entry=entry,
                            stop_loss=stop_loss,
                            symbol_spec=self.symbol_spec,
                            current_spread_pips=current_spread_pips,
                        )

                        if decision.allowed:
                            self.store.record_event(
                                "INFO",
                                self.symbol,
                                "Risk sizing approved",
                                {
                                    "signal": signal.side.value,
                                    "suggested_volume": decision.suggested_volume,
                                    "entry": entry,
                                    "stop_loss": stop_loss,
                                    "take_profit": take_profit,
                                    "current_spread_pips": current_spread_pips,
                                    "strategy": self.strategy_name,
                                },
                            )
                            slot_decision = self.risk.try_acquire_open_slot(
                                symbol=self.symbol,
                                open_positions_count=self.position_book.count(),
                            )
                            if not slot_decision.acquired:
                                self.store.record_event(
                                    "WARN",
                                    self.symbol,
                                    "Trade blocked by risk manager",
                                    {
                                        "reason": slot_decision.reason,
                                        "signal": signal.side.value,
                                    },
                                )
                            else:
                                reservation_id = slot_decision.reservation_id
                                try:
                                    volume = decision.suggested_volume
                                    if self.default_volume > 0:
                                        volume = min(volume, self.default_volume)
                                    volume = self._normalize_volume(volume)

                                    if volume <= 0:
                                        self.store.record_event(
                                            "WARN",
                                            self.symbol,
                                            "Trade blocked by volume normalization",
                                            {
                                                "suggested": decision.suggested_volume,
                                                "default_volume": self.default_volume,
                                            },
                                        )
                                    else:
                                        self._open_position(
                                            side=signal.side,
                                            entry=entry,
                                            stop_loss=stop_loss,
                                            take_profit=take_profit,
                                            volume=volume,
                                            confidence=signal.confidence,
                                            trailing_override=trailing_override,
                                        )
                                finally:
                                    self.risk.release_open_slot(reservation_id)
                        else:
                            self.store.record_event(
                                "WARN",
                                self.symbol,
                                "Trade blocked by risk manager",
                                {
                                    "reason": decision.reason,
                                    "signal": signal.side.value,
                                },
                            )
                    else:
                        self._maybe_record_hold_reason(
                            signal,
                            current_spread_pips,
                            current_spread_pct=current_spread_pct,
                        )

            except BrokerError as exc:
                error_text = str(exc)
                if self._is_allowance_backoff_error(error_text):
                    last_error = f"allowance_backoff:{error_text}"
                    sleep_override_sec = self._handle_allowance_backoff_error(error_text)
                elif self._refresh_symbol_spec_after_min_order_reject(error_text):
                    last_error = f"symbol_spec_refreshed:{error_text}"
                elif self._sync_local_symbol_lot_min_from_broker_error(error_text):
                    last_error = f"symbol_lot_min_updated:{error_text}"
                elif self._activate_reject_entry_cooldown(error_text):
                    last_error = f"broker_reject_cooldown:{error_text}"
                    self._register_broker_error(error_text)
                    self.store.record_event("ERROR", self.symbol, "Broker error", {"error": error_text})
                elif self._handle_missing_broker_position_error(error_text, tick):
                    if self._pending_missing_position_id is not None:
                        last_error = f"broker_position_missing_pending:{error_text}"
                    else:
                        last_error = f"broker_position_missing_reconciled:{error_text}"
                else:
                    last_error = error_text
                    self._register_broker_error(last_error)
                    self.store.record_event("ERROR", self.symbol, "Broker error", {"error": error_text})
            except Exception as exc:
                last_error = str(exc)
                self.store.record_event("ERROR", self.symbol, "Worker error", {"error": str(exc)})

            self._save_state(last_price=tick.mid if tick else None, last_error=last_error)
            wait_sec = self._next_wait_duration(
                has_active_position=self.position_book.get(self.symbol) is not None
            )
            if sleep_override_sec is not None:
                wait_sec = max(wait_sec, sleep_override_sec)
            self.stop_event.wait(wait_sec)
