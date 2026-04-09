from __future__ import annotations

import logging
import math
import os
import threading
import time
from typing import TYPE_CHECKING

from xtb_bot.models import PriceTick
from xtb_bot.tolerances import FLOAT_ROUNDING_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotDbFirstTickRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        cfg = bot.config

        self._db_first_tick_poll_interval_sec = max(
            0.25,
            bot._env_float("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", 1.0),
        )
        db_first_tick_target_rpm = bot._env_float("XTB_DB_FIRST_TICK_TARGET_RPM", 24.0)
        self._db_first_tick_target_rpm = max(1.0, min(58.0, db_first_tick_target_rpm))
        if bot._ig_budget.enabled and cfg.broker == "ig":
            max_tick_rpm_by_local_budget = max(
                1.0,
                bot._ig_budget.rpm - bot._ig_budget.reserve_rpm,
            )
            self._db_first_tick_target_rpm = min(
                self._db_first_tick_target_rpm,
                max_tick_rpm_by_local_budget,
            )
        self._db_first_tick_hard_max_age_sec = max(
            1.0,
            bot._env_float("XTB_DB_FIRST_TICK_HARD_MAX_AGE_SEC", 10.0),
        )
        self._db_first_tick_prune_interval_sec = max(
            0.25,
            bot._env_float("XTB_DB_FIRST_TICK_PRUNE_INTERVAL_SEC", 1.0),
        )
        self._db_first_tick_last_prune_ts = 0.0
        self._db_first_tick_active_cursor = 0
        self._db_first_tick_passive_cursor = 0
        self._db_first_tick_regular_cursor = 0
        self._db_first_tick_priority_cursor = 0
        self._db_first_tick_active_streak = 0
        self._db_first_tick_priority_streak = 0
        self._db_first_tick_active_cycle_warn_last_ts = 0.0
        self._db_first_tick_passive_every_n_active = 12
        self._db_first_tick_active_priority_ratio = 5
        self._db_first_tick_breakout_lookback = 24
        self._db_first_tick_breakout_min_samples = 8
        self._db_first_tick_breakout_edge_ratio = 0.12
        self._db_first_tick_priority_symbols: list[str] = []
        self._db_first_tick_error_log_interval_sec = max(
            1.0,
            bot._env_float("XTB_DB_FIRST_TICK_ERROR_LOG_INTERVAL_SEC", 20.0),
        )
        self._db_first_disconnect_error_log_interval_sec = max(
            5.0,
            bot._env_float("XTB_DB_FIRST_DISCONNECT_ERROR_LOG_INTERVAL_SEC", 60.0),
        )
        self._db_first_reconnect_min_interval_sec = max(
            1.0,
            bot._env_float("XTB_DB_FIRST_RECONNECT_MIN_INTERVAL_SEC", 5.0),
        )
        self._db_first_tick_last_error_event_ts_by_symbol: dict[str, float] = {}
        self._db_first_tick_last_error_reason_by_symbol: dict[str, str] = {}
        self._db_first_tick_error_streak_by_symbol: dict[str, int] = {}
        self._db_first_disconnect_last_error_event_ts = 0.0
        self._db_first_disconnect_error_streak = 0
        self._db_first_reconnect_last_attempt_ts = 0.0
        self._db_first_reconnect_last_error_ts = 0.0
        self._db_first_reconnect_lock = threading.Lock()

    def _db_first_tick_request_interval_sec(self) -> float:
        target_interval = 60.0 / max(1.0, self._db_first_tick_target_rpm)
        return max(
            0.25,
            float(self._db_first_tick_poll_interval_sec),
            float(self._bot.config.ig_rest_market_min_interval_sec),
            target_interval,
        )

    def _db_first_tick_effective_hard_max_age_sec(self, active_symbol_count: int | None = None) -> float:
        configured_hard_max_age = float(self._db_first_tick_hard_max_age_sec)
        if active_symbol_count is None:
            active_symbol_count = self._estimated_active_db_first_symbol_count()
        count = max(1, int(active_symbol_count))
        request_interval_sec = self._db_first_tick_request_interval_sec()
        cycle_sec = request_interval_sec * float(count)
        cycle_based_floor = cycle_sec * 1.2
        return max(
            configured_hard_max_age,
            min(300.0, max(5.0, cycle_based_floor)),
        )

    def _db_first_tick_max_age_for_workers(self) -> float:
        raw_override = os.getenv("XTB_DB_FIRST_TICK_MAX_AGE_SEC")
        if raw_override is not None and str(raw_override).strip() != "":
            try:
                return max(0.5, float(raw_override))
            except (TypeError, ValueError):
                pass

        symbol_count = self._estimated_active_db_first_symbol_count()
        producer_interval_sec = self._db_first_tick_request_interval_sec()
        full_refresh_cycle_sec = producer_interval_sec * float(symbol_count)
        return max(15.0, min(300.0, full_refresh_cycle_sec * 2.5))

    def _estimated_active_db_first_symbol_count(self) -> int:
        now_utc = self._bot._now_utc()
        symbols: set[str] = set()
        for symbol in self._bot._schedule_assignments(now_utc).keys():
            text = str(symbol).strip().upper()
            if text:
                symbols.add(text)
        for position in self._bot.position_book.all_open():
            text = str(position.symbol).strip().upper()
            if text:
                symbols.add(text)
        if not symbols:
            for symbol in self._bot.config.symbols:
                text = str(symbol).strip().upper()
                if text:
                    symbols.add(text)
        return max(1, len(symbols))

    def _active_symbols_for_db_first_tick_cache(self) -> list[str]:
        active: list[str] = []
        seen: set[str] = set()

        with self._bot._workers_lock:
            worker_items = list(self._bot.workers.items())
        for symbol, worker in worker_items:
            if not worker.is_alive():
                continue
            text = str(symbol).strip().upper()
            if not text or text in seen:
                continue
            seen.add(text)
            active.append(text)

        for position in self._bot.position_book.all_open():
            text = str(position.symbol).strip().upper()
            if not text or text in seen:
                continue
            seen.add(text)
            active.append(text)
        return active

    @staticmethod
    def _coerce_finite_positive_float(value: object) -> float | None:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(parsed) or parsed <= 0:
            return None
        return parsed

    def _symbols_near_breakout_levels(self, active_symbols: list[str]) -> set[str]:
        if not active_symbols:
            return set()
        with self._bot._workers_lock:
            worker_items = list(self._bot.workers.items())
        worker_by_symbol = {
            str(symbol).strip().upper(): worker
            for symbol, worker in worker_items
            if str(symbol).strip()
        }
        near_breakout: set[str] = set()
        min_samples = max(4, int(self._db_first_tick_breakout_min_samples))
        lookback = max(min_samples, int(self._db_first_tick_breakout_lookback))
        edge_ratio = max(0.01, min(0.49, float(self._db_first_tick_breakout_edge_ratio)))

        for symbol in active_symbols:
            worker = worker_by_symbol.get(symbol)
            if worker is None or not worker.is_alive():
                continue
            raw_prices = getattr(worker, "prices", None)
            if raw_prices is None:
                continue
            try:
                samples = list(raw_prices)
            except Exception:
                continue
            if len(samples) < min_samples:
                continue
            recent = [
                price
                for price in (
                    self._coerce_finite_positive_float(item)
                    for item in samples[-lookback:]
                )
                if price is not None
            ]
            if len(recent) < min_samples:
                continue
            current = recent[-1]
            high = max(recent)
            low = min(recent)
            span = high - low
            if span <= FLOAT_ROUNDING_TOLERANCE:
                continue
            distance_to_high = (high - current) / span
            distance_to_low = (current - low) / span
            if min(distance_to_high, distance_to_low) <= edge_ratio:
                near_breakout.add(symbol)
        return near_breakout

    def _db_first_tick_priority_symbols_for_active(self, active_symbols: list[str]) -> list[str]:
        if not active_symbols:
            return []
        active_set = set(active_symbols)
        open_position_symbols: set[str] = set()
        for position in self._bot.position_book.all_open():
            text = str(position.symbol).strip().upper()
            if text and text in active_set:
                open_position_symbols.add(text)
        breakout_symbols = self._symbols_near_breakout_levels(active_symbols)
        priority_symbols: list[str] = []
        for symbol in active_symbols:
            if symbol in open_position_symbols or symbol in breakout_symbols:
                priority_symbols.append(symbol)
        return priority_symbols

    def _db_first_tick_symbol_buckets(self) -> tuple[list[str], list[str]]:
        all_symbols = self._bot._runtime_symbols_for_db_first_requests()
        if not all_symbols:
            self._db_first_tick_priority_symbols = []
            return [], []

        active_set = {symbol for symbol in self._active_symbols_for_db_first_tick_cache() if symbol}
        if not active_set:
            self._db_first_tick_priority_symbols = []
            return list(all_symbols), []

        active_symbols = [symbol for symbol in all_symbols if symbol in active_set]
        if not active_symbols:
            self._db_first_tick_priority_symbols = []
            return list(all_symbols), []
        passive_symbols = [symbol for symbol in all_symbols if symbol not in active_set]
        self._db_first_tick_priority_symbols = self._db_first_tick_priority_symbols_for_active(active_symbols)
        if not self._db_first_tick_priority_symbols:
            self._db_first_tick_priority_streak = 0
        return active_symbols, passive_symbols

    def _select_weighted_db_first_active_symbol(
        self,
        *,
        active_symbols: list[str],
        priority_symbols: list[str],
    ) -> str:
        if not priority_symbols:
            self._db_first_tick_priority_streak = 0
            symbol = active_symbols[self._db_first_tick_active_cursor % len(active_symbols)]
            self._db_first_tick_active_cursor += 1
            return symbol
        priority_set = set(priority_symbols)
        regular_symbols = [symbol for symbol in active_symbols if symbol not in priority_set]
        if not regular_symbols:
            symbol = priority_symbols[self._db_first_tick_priority_cursor % len(priority_symbols)]
            self._db_first_tick_priority_cursor += 1
            self._db_first_tick_priority_streak = min(
                self._db_first_tick_active_priority_ratio,
                self._db_first_tick_priority_streak + 1,
            )
            return symbol

        if self._db_first_tick_priority_streak < self._db_first_tick_active_priority_ratio:
            symbol = priority_symbols[self._db_first_tick_priority_cursor % len(priority_symbols)]
            self._db_first_tick_priority_cursor += 1
            self._db_first_tick_priority_streak += 1
            return symbol

        symbol = regular_symbols[self._db_first_tick_regular_cursor % len(regular_symbols)]
        self._db_first_tick_regular_cursor += 1
        self._db_first_tick_priority_streak = 0
        return symbol

    def _select_db_first_tick_symbol(self, active_symbols: list[str], passive_symbols: list[str]) -> str | None:
        if not active_symbols and not passive_symbols:
            return None
        if not passive_symbols:
            return self._select_weighted_db_first_active_symbol(
                active_symbols=active_symbols,
                priority_symbols=[symbol for symbol in self._db_first_tick_priority_symbols if symbol in active_symbols],
            )
        if not active_symbols:
            symbol = passive_symbols[self._db_first_tick_passive_cursor % len(passive_symbols)]
            self._db_first_tick_passive_cursor += 1
            self._db_first_tick_active_streak = 0
            return symbol

        if self._db_first_tick_active_streak >= self._db_first_tick_passive_every_n_active:
            symbol = passive_symbols[self._db_first_tick_passive_cursor % len(passive_symbols)]
            self._db_first_tick_passive_cursor += 1
            self._db_first_tick_active_streak = 0
            return symbol

        symbol = self._select_weighted_db_first_active_symbol(
            active_symbols=active_symbols,
            priority_symbols=[candidate for candidate in self._db_first_tick_priority_symbols if candidate in active_symbols],
        )
        self._db_first_tick_active_streak += 1
        return symbol

    def _maybe_warn_db_first_active_cycle_too_long(
        self,
        *,
        active_symbols: list[str],
        request_interval_sec: float,
    ) -> None:
        if not active_symbols:
            return
        full_active_cycle_sec = request_interval_sec * float(len(active_symbols))
        effective_hard_max_age_sec = self._db_first_tick_effective_hard_max_age_sec(len(active_symbols))
        if full_active_cycle_sec <= effective_hard_max_age_sec:
            return
        now = time.time()
        if (now - self._db_first_tick_active_cycle_warn_last_ts) < 60.0:
            return
        self._db_first_tick_active_cycle_warn_last_ts = now
        self._bot.store.record_event(
            "WARN",
            None,
            "DB-first active tick cycle exceeds hard max age",
            {
                "active_symbols": len(active_symbols),
                "request_interval_sec": round(request_interval_sec, 3),
                "active_cycle_sec": round(full_active_cycle_sec, 3),
                "hard_max_age_sec": round(effective_hard_max_age_sec, 3),
                "configured_hard_max_age_sec": round(float(self._db_first_tick_hard_max_age_sec), 3),
                "mode": self._bot.config.mode.value,
            },
        )
        logger.warning(
            "DB-first active tick cycle exceeds hard max age | active_symbols=%s interval=%.3fs cycle=%.3fs hard_max_age=%.3fs",
            len(active_symbols),
            request_interval_sec,
            full_active_cycle_sec,
            effective_hard_max_age_sec,
        )

    def _fetch_db_first_tick(self, symbol: str, *, request_interval_sec: float) -> PriceTick | None:
        stream_only_getter = getattr(self._bot.broker, "get_price_stream_only", None)
        if callable(stream_only_getter):
            try:
                stream_tick = stream_only_getter(
                    symbol,
                    wait_timeout_sec=min(0.5, max(0.0, request_interval_sec)),
                )
            except Exception:
                stream_tick = None
            if stream_tick is not None:
                return stream_tick

        rest_fallback_block_remaining_getter = getattr(
            self._bot.broker,
            "get_stream_rest_fallback_block_remaining_sec",
            None,
        )
        if callable(rest_fallback_block_remaining_getter):
            try:
                if float(rest_fallback_block_remaining_getter(symbol)) > 0.0:
                    return None
            except Exception:
                pass

        if not self._bot._reserve_ig_non_trading_budget(
            scope="db_first_tick",
            wait_timeout_sec=min(0.5, request_interval_sec),
        ):
            return None
        return self._bot.broker.get_price(symbol)

    def _db_first_tick_cache_loop(self) -> None:
        loop_name = "tick"
        request_interval_sec = self._db_first_tick_request_interval_sec()
        while not self._bot.stop_event.is_set() and not self._bot._db_first_cache_stop_event.is_set():
            loop_backoff_remaining = self._bot._db_first_loop_backoff_remaining_sec(loop_name)
            if loop_backoff_remaining > 0.0:
                self._bot._db_first_cache_stop_event.wait(timeout=max(0.5, min(loop_backoff_remaining, 60.0)))
                continue
            active_symbols, passive_symbols = self._db_first_tick_symbol_buckets()
            if not active_symbols and not passive_symbols:
                self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)
                continue
            self._maybe_warn_db_first_active_cycle_too_long(
                active_symbols=active_symbols,
                request_interval_sec=request_interval_sec,
            )
            backoff_remaining = self._bot._broker_public_api_backoff_remaining_sec()
            if backoff_remaining > 0.0:
                self._bot._db_first_cache_stop_event.wait(timeout=max(0.5, min(backoff_remaining, request_interval_sec)))
                continue
            symbol = self._select_db_first_tick_symbol(active_symbols, passive_symbols)
            if not symbol:
                self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)
                continue
            try:
                tick = self._fetch_db_first_tick(
                    symbol,
                    request_interval_sec=request_interval_sec,
                )
                if tick is None:
                    self._bot._db_first_cache_stop_event.wait(timeout=min(0.5, request_interval_sec))
                    continue
                self._bot._update_latest_tick_from_broker(tick)
                timestamp = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", time.time()))
                self._bot.store.upsert_broker_tick(
                    symbol=symbol,
                    bid=float(tick.bid),
                    ask=float(tick.ask),
                    ts=float(timestamp),
                    volume=(float(tick.volume) if tick.volume is not None else None),
                    source="db_first_app_non_trading_cache",
                )
                upper_symbol = str(symbol).strip().upper()
                if upper_symbol:
                    self._db_first_tick_error_streak_by_symbol[upper_symbol] = 0
                    self._db_first_tick_last_error_reason_by_symbol.pop(upper_symbol, None)
                self._bot._clear_db_first_loop_backoff(loop_name)
            except Exception as exc:
                self._maybe_record_db_first_tick_cache_refresh_error(symbol, exc)
                backoff_sec = self._bot._record_db_first_loop_failure(
                    loop_name,
                    base_interval_sec=request_interval_sec,
                )
                self._maybe_prune_db_first_stale_ticks(active_symbol_count=len(active_symbols))
                self._bot._db_first_cache_stop_event.wait(timeout=max(request_interval_sec, backoff_sec))
                continue
            self._maybe_prune_db_first_stale_ticks(active_symbol_count=len(active_symbols))
            self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)

    def _maybe_prune_db_first_stale_ticks(self, *, active_symbol_count: int | None = None) -> None:
        now = time.time()
        if (now - self._db_first_tick_last_prune_ts) < self._db_first_tick_prune_interval_sec:
            return
        effective_hard_max_age_sec = self._db_first_tick_effective_hard_max_age_sec(active_symbol_count)
        try:
            self._bot.store.prune_stale_broker_ticks(
                max_age_sec=effective_hard_max_age_sec,
                now_ts=now,
            )
        except Exception:
            return
        self._db_first_tick_last_prune_ts = now

    def _maybe_record_db_first_tick_cache_refresh_error(self, symbol: str, error: Exception) -> None:
        upper_symbol = str(symbol).strip().upper()
        if not upper_symbol:
            return
        now = time.time()
        error_text = str(error)
        allowance_related = self._bot._is_allowance_related_error(error_text)
        reason = self._bot._normalize_broker_error_reason(error_text)
        disconnected_reason = reason == "ig_api_client_is:failed"
        if disconnected_reason:
            self._bot._maybe_reconnect_broker_for_db_first(now)

            self._db_first_disconnect_error_streak += 1
            if (
                now - self._db_first_disconnect_last_error_event_ts
            ) < self._db_first_disconnect_error_log_interval_sec:
                return
            self._db_first_disconnect_last_error_event_ts = now
            message = "DB-first tick cache paused: broker disconnected"
            payload = {
                "error": error_text,
                "reason": reason,
                "streak": self._db_first_disconnect_error_streak,
                "allowance_related": allowance_related,
                "mode": self._bot.config.mode.value,
                "poll_interval_sec": self._db_first_tick_poll_interval_sec,
            }
            self._bot.store.record_event("WARN", None, message, payload)
            logger.warning(
                "DB-first tick cache paused (broker disconnected) | streak=%s error=%s",
                self._db_first_disconnect_error_streak,
                error_text,
            )
            return
        self._db_first_disconnect_error_streak = 0

        prior_reason = self._db_first_tick_last_error_reason_by_symbol.get(upper_symbol)
        last_event_ts = self._db_first_tick_last_error_event_ts_by_symbol.get(upper_symbol, 0.0)
        streak = int(self._db_first_tick_error_streak_by_symbol.get(upper_symbol, 0)) + 1
        self._db_first_tick_error_streak_by_symbol[upper_symbol] = streak
        self._db_first_tick_last_error_reason_by_symbol[upper_symbol] = reason

        reason_changed = prior_reason != reason
        if (not reason_changed) and ((now - last_event_ts) < self._db_first_tick_error_log_interval_sec):
            return
        self._db_first_tick_last_error_event_ts_by_symbol[upper_symbol] = now

        with self._bot._workers_lock:
            assignment = self._bot._worker_assignments.get(upper_symbol)
        payload: dict[str, object] = {
            "error": error_text,
            "reason": reason,
            "streak": streak,
            "allowance_related": allowance_related,
            "mode": self._bot.config.mode.value,
            "poll_interval_sec": self._db_first_tick_poll_interval_sec,
        }
        if assignment is not None:
            payload.update(
                self._bot._strategy_event_payload(
                    assignment.strategy_name,
                    assignment.strategy_params,
                )
            )
        backoff_remaining_sec = self._bot._broker_public_api_backoff_remaining_sec()
        if backoff_remaining_sec > 0:
            payload["broker_public_api_backoff_remaining_sec"] = round(backoff_remaining_sec, 2)

        message = "DB-first tick cache refresh deferred" if allowance_related else "DB-first tick cache refresh failed"
        level = "WARN" if allowance_related else "ERROR"
        self._bot.store.record_event(level, upper_symbol, message, payload)
        log_fn = logger.warning if allowance_related else logger.error
        log_fn(
            "DB-first tick cache refresh %s for %s | reason=%s streak=%s error=%s",
            "deferred" if allowance_related else "failed",
            upper_symbol,
            reason,
            streak,
            error_text,
        )

    def _maybe_reconnect_broker_for_db_first(self, now_ts: float | None = None) -> bool:
        if self._bot.config.broker != "ig":
            return False
        connect = getattr(self._bot.broker, "connect", None)
        if not callable(connect):
            return False

        now = time.time() if now_ts is None else float(now_ts)
        if (now - self._db_first_reconnect_last_attempt_ts) < self._db_first_reconnect_min_interval_sec:
            return False
        if not self._db_first_reconnect_lock.acquire(blocking=False):
            return False
        try:
            now = time.time()
            if (now - self._db_first_reconnect_last_attempt_ts) < self._db_first_reconnect_min_interval_sec:
                return False
            self._db_first_reconnect_last_attempt_ts = now
            try:
                connect()
            except Exception as exc:
                if (
                    now - self._db_first_reconnect_last_error_ts
                ) >= self._db_first_disconnect_error_log_interval_sec:
                    self._bot.store.record_event(
                        "WARN",
                        None,
                        "DB-first broker reconnect failed",
                        {"error": str(exc), "mode": self._bot.config.mode.value},
                    )
                    logger.warning("DB-first broker reconnect failed: %s", exc)
                    self._db_first_reconnect_last_error_ts = now
                return False
            self._bot.store.record_event(
                "INFO",
                None,
                "DB-first broker reconnect succeeded",
                {"mode": self._bot.config.mode.value},
            )
            logger.info("DB-first broker reconnect succeeded")
            return True
        finally:
            self._db_first_reconnect_lock.release()
