from __future__ import annotations

import logging
import math
import os
import threading
import time
from typing import TYPE_CHECKING

from xtb_bot.bot._utils import _BoundedTtlCache
from xtb_bot.models import PriceTick
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotPriceHistoryRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        cfg = bot.config

        self._history_keep_rows_min = max(100, int(cfg.price_history_keep_rows_min))
        history_keep_rows_cap = bot._env_int("XTB_HISTORY_KEEP_ROWS_CAP", 20_000)
        self._history_keep_rows_cap = max(self._history_keep_rows_min, history_keep_rows_cap)
        history_ticks_per_candle_cap = bot._env_int("XTB_HISTORY_TICKS_PER_CANDLE_CAP", 120)
        self._history_ticks_per_candle_cap = max(1, history_ticks_per_candle_cap)
        self._history_keep_rows_by_symbol: dict[str, int] = {}
        self._strategy_history_estimate_cache = _BoundedTtlCache(max_entries=512, ttl_sec=1800.0)
        self._history_prefetch_specs_by_symbol: dict[str, dict[str, int]] = {}
        self._strategy_prefetch_estimate_cache = _BoundedTtlCache(max_entries=512, ttl_sec=1800.0)
        self._passive_history_poll_interval_sec = max(0.5, float(cfg.passive_history_poll_interval_sec))
        self._passive_history_max_symbols_per_cycle = max(
            1,
            bot._env_int("XTB_PASSIVE_HISTORY_MAX_SYMBOLS_PER_CYCLE", 2),
        )
        self._history_prefetch_enabled = bot._env_bool(
            "XTB_IG_HISTORY_PREFETCH_ENABLED",
            cfg.broker == "ig",
        )
        self._history_prefetch_points = max(20, bot._env_int("XTB_IG_HISTORY_PREFETCH_POINTS", 300))
        self._history_prefetch_resolution = str(
            os.getenv("XTB_IG_HISTORY_PREFETCH_RESOLUTION", "MINUTE")
        ).strip().upper() or "MINUTE"
        self._history_prefetch_skip_ratio = min(
            1.0,
            max(0.0, bot._env_float("XTB_IG_HISTORY_PREFETCH_SKIP_RATIO", 0.85)),
        )
        self._passive_history_cursor = 0
        self._passive_history_refresh_lock = threading.Lock()
        self._last_passive_history_sample_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_error_monotonic_by_symbol: dict[str, float] = {}
        self._last_passive_history_ts_by_symbol: dict[str, float] = {}

    def _estimate_history_keep_rows(self, strategy_name: str, strategy_params: dict[str, object]) -> int:
        cache_key = self._bot._strategy_cache_identity(strategy_name, strategy_params)
        cached = self._strategy_history_estimate_cache.get(cache_key)
        if cached is not None:
            return cached

        import xtb_bot.bot.core as _core_mod
        strategy = _core_mod.create_strategy(strategy_name, strategy_params)
        min_history = max(2, int(getattr(strategy, "min_history", 2)))
        history_buffer = max(min_history * 3, 64)
        candle_timeframe_sec = int(float(getattr(strategy, "candle_timeframe_sec", 0) or 0))
        if candle_timeframe_sec > 1 and self._bot.config.poll_interval_sec > 0:
            ticks_per_candle = max(
                1,
                min(
                    self._history_ticks_per_candle_cap,
                    math.ceil(candle_timeframe_sec / max(self._bot.config.poll_interval_sec, FLOAT_COMPARISON_TOLERANCE)),
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
        cache_key = self._bot._strategy_cache_identity(strategy_name, strategy_params)
        cached = self._strategy_prefetch_estimate_cache.get(cache_key)
        if cached is not None:
            return cached
        specs: list[tuple[str, int]] = []
        import xtb_bot.bot.core as _core_mod
        strategy = _core_mod.create_strategy(strategy_name, strategy_params)
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
        component_names = self._bot._resolve_multi_strategy_component_names(strategy_name, strategy_params)
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
                else self._bot._strategy_params_for(normalized_component)
            )
            if not self._bot._strategy_supports_symbol(
                normalized_component,
                component_params,
                normalized_symbol,
            ):
                continue
            required_rows = self._history_keep_rows_min
            try:
                required_rows = self._bot._estimate_history_keep_rows(normalized_component, component_params)
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
                prefetch_specs = self._bot._estimate_history_prefetch_specs(normalized_component, component_params)
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
        default_strategy_params = self._bot._strategy_params_for(self._bot.config.strategy)
        for symbol in self._bot.config.symbols:
            self._bot._register_symbol_history_requirement(symbol, self._bot.config.strategy, default_strategy_params)
        if self._bot._schedule_disabled_by_multi_strategy():
            return
        for entry in self._bot.config.strategy_schedule:
            for symbol in entry.symbols:
                self._bot._register_symbol_history_requirement(symbol, entry.strategy, dict(entry.strategy_params))

    def _price_history_keep_rows_for_symbol(self, symbol: str) -> int:
        normalized_symbol = str(symbol).strip().upper()
        return int(self._history_keep_rows_by_symbol.get(normalized_symbol, self._history_keep_rows_min))

    def _history_prefetch_plan_for_symbol(self, symbol: str) -> list[tuple[str, int]]:
        normalized_symbol = str(symbol).strip().upper()
        keep_rows = self._bot._price_history_keep_rows_for_symbol(normalized_symbol)
        base_resolution = str(self._history_prefetch_resolution or "MINUTE").strip().upper() or "MINUTE"
        base_points = max(2, min(int(self._history_prefetch_points), int(keep_rows)))
        plan: list[tuple[str, int]] = [(base_resolution, base_points)]
        extra_specs = self._history_prefetch_specs_by_symbol.get(normalized_symbol, {})
        if not extra_specs:
            return plan
        for resolution, points in sorted(
            extra_specs.items(),
            key=lambda item: self._bot._history_resolution_seconds(item[0]) or 10**9,
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
            resolution_sec = self._bot._history_resolution_seconds(str(resolution))
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
        resolution_sec = self._bot._history_resolution_seconds(resolution)
        if resolution_sec is None or resolution_sec <= 0:
            return False
        if resolution_sec >= 60:
            rows = self._bot.store.load_recent_candle_history(
                symbol,
                resolution_sec=int(resolution_sec),
                limit=max(2, int(target_points)),
            )
        else:
            rows = self._bot.store.load_recent_price_history(symbol, limit=max(2, int(target_points)))
        if len(rows) < 2:
            return False
        timestamps: list[float] = []
        for row in rows:
            ts = self._bot._normalize_timestamp_seconds(row.get("ts"))
            if math.isfinite(ts) and ts > 0:
                timestamps.append(ts)
        if len(timestamps) < 2:
            return False
        coverage_sec = max(timestamps) - min(timestamps)
        required_coverage_sec = max(0.0, (int(target_points) - 1) * resolution_sec * self._history_prefetch_skip_ratio)
        return coverage_sec >= required_coverage_sec

    def _refresh_passive_price_history_from_cached_ticks(self) -> None:
        if self._bot.stop_event.is_set():
            return
        if not self._passive_history_refresh_lock.acquire(blocking=False):
            return
        loop_start_monotonic = time.monotonic()
        try:
            with self._bot._workers_lock:
                worker_items = list(self._bot.workers.items())
            active_symbols = {symbol for symbol, worker in worker_items if worker.is_alive()}
            symbols = self._bot._symbols_for_db_first_cache()
            if not symbols:
                return
            total_symbols = len(symbols)
            start_idx = self._passive_history_cursor % total_symbols
            ordered_symbols = symbols[start_idx:] + symbols[:start_idx]
            max_symbols_per_cycle = min(total_symbols, self._passive_history_max_symbols_per_cycle)
            processed = 0
            max_tick_age_sec = max(
                5.0,
                min(300.0, self._bot._db_first_tick_max_age_for_workers() * 4.0),
            )

            for symbol in ordered_symbols:
                now_monotonic = time.monotonic()
                if (now_monotonic - loop_start_monotonic) >= max(1.0, self._bot._db_first_history_poll_interval_sec):
                    break
                if symbol in active_symbols:
                    continue
                last_sample_monotonic = self._last_passive_history_sample_monotonic_by_symbol.get(symbol, 0.0)
                if (now_monotonic - last_sample_monotonic) < self._passive_history_poll_interval_sec:
                    continue
                tick = self._bot.store.load_latest_broker_tick(symbol, max_age_sec=max_tick_age_sec)
                if tick is None:
                    continue
                keep_rows = self._bot._price_history_keep_rows_for_symbol(symbol)
                now_ts = time.time()
                timestamp = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", now_ts))
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

                self._bot.store.append_price_sample(
                    symbol=symbol,
                    ts=timestamp,
                    close=float(tick.mid),
                    volume=current_volume,
                    max_rows_per_symbol=keep_rows,
                    candle_resolutions_sec=self._bot._candle_history_resolutions_for_symbol(symbol),
                )
                self._last_passive_history_sample_monotonic_by_symbol[symbol] = now_monotonic
                processed += 1
                if processed >= max_symbols_per_cycle:
                    break

            self._passive_history_cursor = (start_idx + max(1, processed)) % total_symbols
        finally:
            self._passive_history_refresh_lock.release()

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
        fetcher = getattr(self._bot.broker, "fetch_recent_price_history", None)
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

        symbols = self._bot._symbols_to_run()
        summary["symbols_total"] = len(symbols)
        target_symbol_limit = max(1, int(max_symbols)) if max_symbols is not None else None
        targeted_symbols = 0
        for symbol in symbols:
            keep_rows = self._bot._price_history_keep_rows_for_symbol(symbol)
            fetched_for_symbol = False
            attempted_for_symbol = False
            skipped_all_specs = True
            for resolution, target_points in self._bot._history_prefetch_plan_for_symbol(symbol):
                if not force_all_symbols and self._bot._history_prefetch_has_time_coverage(
                    symbol,
                    resolution=resolution,
                    target_points=target_points,
                ):
                    continue
                skipped_all_specs = False
                attempted_for_symbol = True
                if not self._bot._reserve_ig_non_trading_budget(
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
                    if self._bot._is_allowance_related_error(text):
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
                resolution_sec = self._bot._history_resolution_seconds(resolution)
                for row in rows:
                    if not isinstance(row, tuple) or len(row) < 2:
                        continue
                    ts = self._bot._normalize_timestamp_seconds(row[0])
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
                        self._bot.store.append_candle_sample(
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
                        self._bot.store.append_price_sample(
                            symbol=symbol,
                            ts=ts,
                            close=close,
                            volume=volume,
                            max_rows_per_symbol=keep_rows,
                            candle_resolutions_sec=self._bot._candle_history_resolutions_for_symbol(symbol),
                        )
                        if resolution_sec is not None and resolution_sec >= 60:
                            self._bot.store.append_candle_sample(
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
        fetcher = getattr(self._bot.broker, "fetch_recent_price_history", None)
        if not callable(fetcher):
            return True
        symbols = self._bot._symbols_to_run()
        if not symbols:
            return True
        for symbol in symbols:
            for resolution, target_points in self._bot._history_prefetch_plan_for_symbol(symbol):
                if not self._bot._history_prefetch_has_time_coverage(
                    symbol,
                    resolution=resolution,
                    target_points=target_points,
                ):
                    return False
        return True

    def _pre_warm_history(self) -> None:
        summary = self._bot._prefill_price_history_from_broker(force_all_symbols=True)
        if int(summary.get("symbols_total") or 0) <= 0:
            return
        self._bot.store.record_event(
            "INFO",
            None,
            "Startup history pre-warm complete",
            {
                **summary,
                "resolution": self._history_prefetch_resolution,
                "target_points": self._history_prefetch_points,
                "mode": self._bot.config.mode.value,
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
        summary = self._bot._prefill_price_history_from_broker(force_all_symbols=False)
        if int(summary.get("symbols_total") or 0) <= 0:
            return
        activity_total = sum(
            int(summary.get(key) or 0)
            for key in ("symbols_fetched", "symbols_skipped", "symbols_failed")
        )
        if activity_total <= 0:
            return
        self._bot.store.record_event(
            "INFO",
            None,
            "Startup history seed complete",
            {
                **summary,
                "resolution": self._history_prefetch_resolution,
                "target_points": self._history_prefetch_points,
                "mode": self._bot.config.mode.value,
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

    def _refresh_passive_price_history(self, force: bool = False) -> None:
        if self._bot.stop_event.is_set():
            return
        if not self._passive_history_refresh_lock.acquire(blocking=False):
            return
        loop_start_monotonic = time.monotonic()
        try:
            poll_interval_sec = 0.0 if force else self._passive_history_poll_interval_sec
            with self._bot._workers_lock:
                worker_items = list(self._bot.workers.items())
            active_symbols = {symbol for symbol, worker in worker_items if worker.is_alive()}
            symbols = self._bot._symbols_to_run()
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
                backoff_remaining = self._bot._broker_public_api_backoff_remaining_sec()
                if backoff_remaining > 0.0:
                    if force:
                        break
                    return
                # Startup warmup must not block on broker-side REST pacing.
                if force and self._bot._broker_market_data_wait_remaining_sec() > 0.0:
                    break

                if symbol in active_symbols:
                    continue
                last_sample_monotonic = self._last_passive_history_sample_monotonic_by_symbol.get(symbol, 0.0)
                if (now_monotonic - last_sample_monotonic) < poll_interval_sec:
                    continue
                keep_rows = self._bot._price_history_keep_rows_for_symbol(symbol)
                if not self._bot._reserve_ig_non_trading_budget(
                    scope="passive_history_refresh",
                    wait_timeout_sec=0.2 if force else 0.0,
                ):
                    if force:
                        break
                    return
                try:
                    tick = self._bot.broker.get_price(symbol)
                    fetch_attempts += 1
                except Exception as exc:
                    fetch_attempts += 1
                    error_text = str(exc)
                    if self._bot._is_allowance_related_error(error_text):
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
                timestamp = self._bot._normalize_timestamp_seconds(getattr(tick, "timestamp", now_ts))
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

                self._bot.store.append_price_sample(
                    symbol=symbol,
                    ts=timestamp,
                    close=float(tick.mid),
                    volume=current_volume,
                    max_rows_per_symbol=keep_rows,
                    candle_resolutions_sec=self._bot._candle_history_resolutions_for_symbol(symbol),
                )
                self._last_passive_history_sample_monotonic_by_symbol[symbol] = now_monotonic
                self._last_passive_history_error_monotonic_by_symbol.pop(symbol, None)
                if not force and fetch_attempts >= max_fetch_attempts:
                    break

            cursor_advance = max(1, fetch_attempts)
            self._passive_history_cursor = (start_idx + cursor_advance) % total_symbols
        finally:
            self._passive_history_refresh_lock.release()


