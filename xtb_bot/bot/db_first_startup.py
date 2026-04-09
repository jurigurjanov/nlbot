from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import TYPE_CHECKING

from xtb_bot.models import RunMode

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotDbFirstStartupRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot

    def _db_first_enabled(self) -> bool:
        return self._bot._db_first_reads_enabled and self._bot.config.broker == "ig"

    def _symbols_to_run(self) -> list[str]:
        symbols: list[str] = []
        seen: set[str] = set()
        scheduled_symbols: list[str] = []
        if not self._bot._schedule_disabled_by_multi_strategy():
            scheduled_symbols = [symbol for entry in self._bot.config.strategy_schedule for symbol in entry.symbols]
        for symbol in list(self._bot.config.symbols) + scheduled_symbols + [pos.symbol for pos in self._bot.position_book.all_open()]:
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
        return self._bot._symbols_to_run()

    def _runtime_symbols_for_db_first_requests(self, now_utc: datetime | None = None) -> list[str]:
        now_value = self._bot._now_utc() if now_utc is None else now_utc
        symbols: list[str] = []
        seen: set[str] = set()

        def _add(raw_symbol: object) -> None:
            text = str(raw_symbol).strip().upper()
            if not text or text in seen:
                return
            seen.add(text)
            symbols.append(text)

        for symbol in self._bot._schedule_assignments(now_value).keys():
            _add(symbol)
        with self._bot._workers_lock:
            worker_items = list(self._bot.workers.items())
        for symbol, worker in worker_items:
            if worker.is_alive():
                _add(symbol)
        for position in self._bot.position_book.all_open():
            _add(position.symbol)
        if not symbols and (not self._bot.config.strategy_schedule or self._bot._schedule_disabled_by_multi_strategy()):
            for symbol in self._bot.config.symbols:
                _add(symbol)
        return symbols

    def _prime_db_first_caches(self) -> None:
        if not self._bot._db_first_enabled():
            return
        if not self._bot._db_first_prime_enabled:
            return
        symbols = self._bot._runtime_symbols_for_db_first_requests()
        if self._bot._db_first_prime_max_symbols > 0:
            symbols = symbols[: self._bot._db_first_prime_max_symbols]
        for symbol in symbols:
            if not self._bot._reserve_ig_non_trading_budget(
                scope="db_first_prime_symbol_spec",
                wait_timeout_sec=0.2,
            ):
                break
            try:
                spec = self._bot.broker.get_symbol_spec(symbol)
                self._bot.store.upsert_broker_symbol_spec(
                    symbol=symbol,
                    spec=spec,
                    ts=time.time(),
                    source="db_first_prime",
                )
            except Exception as exc:
                logger.debug("DB-first prime symbol spec failed for %s: %s", symbol, exc)
            if not self._bot._reserve_ig_non_trading_budget(
                scope="db_first_prime_tick",
                wait_timeout_sec=0.2,
            ):
                break
            try:
                tick = self._bot.broker.get_price(symbol)
                self._bot._update_latest_tick_from_broker(tick)
                timestamp = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", time.time()))
                self._bot.store.upsert_broker_tick(
                    symbol=symbol,
                    bid=float(tick.bid),
                    ask=float(tick.ask),
                    ts=float(timestamp),
                    volume=(float(tick.volume) if tick.volume is not None else None),
                    source="db_first_prime",
                )
            except Exception as exc:
                logger.debug("DB-first prime tick failed for %s: %s", symbol, exc)
        if self._bot._db_first_prime_account_enabled:
            if not self._bot._reserve_ig_non_trading_budget(
                scope="db_first_prime_account_snapshot",
                wait_timeout_sec=0.3,
            ):
                return
            try:
                snapshot = self._bot.broker.get_account_snapshot()
                self._bot.store.upsert_broker_account_snapshot(snapshot, source="db_first_prime")
            except Exception as exc:
                logger.debug("DB-first prime account snapshot failed: %s", exc)

    def _prime_db_first_account_snapshot_before_workers(self) -> None:
        if not self._bot._db_first_enabled():
            return
        existing = self._bot.store.load_latest_broker_account_snapshot(max_age_sec=30.0)
        if existing is not None:
            logger.debug("DB-first account snapshot already primed, skipping")
            return
        logger.info("Priming DB-first account snapshot before starting workers ...")
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                snapshot = self._bot.broker.get_account_snapshot()
                self._bot.store.upsert_broker_account_snapshot(
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

    def _db_first_account_cache_loop(self) -> None:
        loop_name = "account"
        while not self._bot.stop_event.is_set() and not self._bot._db_first_cache_stop_event.is_set():
            loop_backoff_remaining = self._bot._db_first_loop_backoff_remaining_sec(loop_name)
            if loop_backoff_remaining > 0.0:
                self._bot._db_first_cache_stop_event.wait(timeout=max(0.5, min(loop_backoff_remaining, 60.0)))
                continue
            if not self._bot._reserve_ig_non_trading_budget(
                scope="db_first_account_snapshot",
                wait_timeout_sec=min(0.5, self._bot._db_first_account_snapshot_poll_interval_sec),
            ):
                self._maybe_warn_stale_db_first_account_snapshot(reason="local_non_trading_budget")
                self._bot._db_first_cache_stop_event.wait(timeout=self._bot._db_first_account_snapshot_poll_interval_sec)
                continue
            try:
                snapshot = self._bot.broker.get_account_snapshot()
                self._bot.store.upsert_broker_account_snapshot(
                    snapshot,
                    source="db_first_account_non_trading_cache",
                )
                self._bot._db_first_account_snapshot_last_success_ts = time.time()
                self._bot._clear_db_first_loop_backoff(loop_name)
            except Exception as exc:
                self._maybe_warn_stale_db_first_account_snapshot(reason=str(exc))
                backoff_sec = self._bot._record_db_first_loop_failure(
                    loop_name,
                    base_interval_sec=self._bot._db_first_account_snapshot_poll_interval_sec,
                )
                if not self._bot._is_allowance_related_error(str(exc)):
                    logger.debug("DB-first account snapshot refresh failed: %s", exc)
                self._bot._db_first_cache_stop_event.wait(
                    timeout=max(self._bot._db_first_account_snapshot_poll_interval_sec, backoff_sec)
                )
                continue
            self._bot._db_first_cache_stop_event.wait(timeout=self._bot._db_first_account_snapshot_poll_interval_sec)

    def _maybe_warn_stale_db_first_account_snapshot(self, *, reason: str) -> None:
        stale_after_sec = max(30.0, self._bot._db_first_account_snapshot_poll_interval_sec * 2.0)
        latest_snapshot = self._bot.store.load_latest_broker_account_snapshot(max_age_sec=0.0)
        if latest_snapshot is not None:
            latest_ts = float(latest_snapshot.timestamp)
        else:
            latest_ts = float(self._bot._db_first_account_snapshot_last_success_ts or 0.0)
        if latest_ts <= 0.0:
            cache_age_sec = float("inf")
        else:
            cache_age_sec = max(0.0, time.time() - latest_ts)
        if cache_age_sec < stale_after_sec:
            return

        now = time.time()
        warn_interval_sec = max(30.0, self._bot._db_first_account_snapshot_poll_interval_sec * 2.0)
        if (
            self._bot._db_first_account_snapshot_stale_warn_last_ts > 0.0
            and (now - self._bot._db_first_account_snapshot_stale_warn_last_ts) < warn_interval_sec
        ):
            return

        payload = {
            "cache_age_sec": round(cache_age_sec, 3) if math.isfinite(cache_age_sec) else "inf",
            "stale_after_sec": round(stale_after_sec, 3),
            "poll_interval_sec": round(self._bot._db_first_account_snapshot_poll_interval_sec, 3),
            "reason": str(reason or "").strip() or "unknown",
        }
        self._bot.store.record_event(
            "WARN",
            None,
            "DB-first account snapshot cache stale",
            payload,
        )
        logger.warning(
            "DB-first account snapshot cache stale | age=%.1fs stale_after=%.1fs poll_interval=%.1fs reason=%s",
            cache_age_sec if math.isfinite(cache_age_sec) else -1.0,
            stale_after_sec,
            self._bot._db_first_account_snapshot_poll_interval_sec,
            payload["reason"],
        )
        self._bot._db_first_account_snapshot_stale_warn_last_ts = now

    def _db_first_history_cache_loop(self) -> None:
        loop_name = "history"
        while not self._bot.stop_event.is_set() and not self._bot._db_first_cache_stop_event.is_set():
            loop_backoff_remaining = self._bot._db_first_loop_backoff_remaining_sec(loop_name)
            if loop_backoff_remaining > 0.0:
                self._bot._db_first_cache_stop_event.wait(timeout=max(0.5, min(loop_backoff_remaining, 60.0)))
                continue
            try:
                self._bot._refresh_passive_price_history_from_cached_ticks()
                self._bot._clear_db_first_loop_backoff(loop_name)
            except Exception as exc:
                backoff_sec = self._bot._record_db_first_loop_failure(
                    loop_name,
                    base_interval_sec=self._bot._db_first_history_poll_interval_sec,
                )
                if not self._bot._is_allowance_related_error(str(exc)):
                    logger.debug("DB-first history cache refresh failed: %s", exc)
                self._bot._db_first_cache_stop_event.wait(timeout=max(self._bot._db_first_history_poll_interval_sec, backoff_sec))
                continue
            self._bot._db_first_cache_stop_event.wait(timeout=self._bot._db_first_history_poll_interval_sec)

    def _runtime_complete_deferred_startup_tasks(self, force: bool = False) -> None:
        if self._bot.stop_event.is_set():
            return
        if not self._bot._startup_fast_path_enabled():
            return
        if (
            self._bot._runtime_deferred_symbol_spec_preload_completed
            and self._bot._runtime_deferred_history_prewarm_completed
        ):
            if not self._bot._runtime_deferred_startup_completion_recorded:
                self._bot.store.record_event(
                    "INFO",
                    None,
                    "Startup fast-path deferred tasks complete",
                    {
                        "mode": self._bot.config.mode.value,
                        "broker": self._bot.config.broker,
                    },
                )
                self._bot._runtime_deferred_startup_completion_recorded = True
            return

        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._bot._last_runtime_deferred_startup_tasks_monotonic) < 30.0:
            return
        self._bot._last_runtime_deferred_startup_tasks_monotonic = now_monotonic

        if not self._bot._runtime_deferred_symbol_spec_preload_completed:
            try:
                self._bot._preload_symbol_specs_on_startup()
            except Exception as exc:
                if (now_monotonic - self._bot._last_runtime_deferred_startup_error_monotonic) >= 60.0:
                    logger.warning("Deferred startup symbol specification preload failed: %s", exc)
                    self._bot._last_runtime_deferred_startup_error_monotonic = now_monotonic
            else:
                self._bot._runtime_deferred_symbol_spec_preload_completed = True

        if not self._bot._runtime_deferred_history_prewarm_completed:
            if self._bot._history_prefetch_complete_for_all_symbols():
                self._bot._runtime_deferred_history_prewarm_completed = True
            else:
                max_symbols = max(1, min(4, self._bot._passive_history_max_symbols_per_cycle))
                summary = self._bot._prefill_price_history_from_broker(
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
                        "resolution": self._bot._history_prefetch_resolution,
                        "target_points": self._bot._history_prefetch_points,
                        "mode": self._bot.config.mode.value,
                        "fast_startup": True,
                        "max_symbols": max_symbols,
                    }
                    level = "WARN" if int(summary.get("symbols_failed") or 0) > 0 else "INFO"
                    self._bot.store.record_event(level, None, "Runtime startup history catch-up", payload)
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
                if self._bot._history_prefetch_complete_for_all_symbols():
                    self._bot._runtime_deferred_history_prewarm_completed = True

        if (
            self._bot._runtime_deferred_symbol_spec_preload_completed
            and self._bot._runtime_deferred_history_prewarm_completed
            and not self._bot._runtime_deferred_startup_completion_recorded
        ):
            self._bot.store.record_event(
                "INFO",
                None,
                "Startup fast-path deferred tasks complete",
                {
                    "mode": self._bot.config.mode.value,
                    "broker": self._bot.config.broker,
                },
            )
            self._bot._runtime_deferred_startup_completion_recorded = True

    def _startup_fast_path_enabled(self) -> bool:
        return self._bot.config.mode == RunMode.EXECUTION and self._bot._db_first_enabled()

    def _db_first_loop_backoff_remaining_sec(
        self,
        loop_name: str,
        *,
        now_monotonic: float | None = None,
    ) -> float:
        current = time.monotonic() if now_monotonic is None else float(now_monotonic)
        retry_after = float(self._bot._db_first_loop_retry_after_monotonic_by_name.get(loop_name, 0.0))
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
        prior = float(self._bot._db_first_loop_backoff_sec_by_name.get(loop_name, 0.0))
        base = max(float(base_interval_sec), 0.5)
        next_backoff = min(
            max(float(max_backoff_sec), base),
            max(base, (prior * 2.0) if prior > 0.0 else base),
        )
        self._bot._db_first_loop_backoff_sec_by_name[loop_name] = next_backoff
        self._bot._db_first_loop_retry_after_monotonic_by_name[loop_name] = current + next_backoff
        return next_backoff

    def _clear_db_first_loop_backoff(self, loop_name: str) -> None:
        self._bot._db_first_loop_backoff_sec_by_name.pop(loop_name, None)
        self._bot._db_first_loop_retry_after_monotonic_by_name.pop(loop_name, None)
