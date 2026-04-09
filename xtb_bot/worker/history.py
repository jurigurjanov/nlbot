from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections import deque
from contextlib import nullcontext
import math

from xtb_bot.models import PriceTick, Signal
from xtb_bot.strategies.base import Strategy, StrategyContext
from xtb_bot.time_utils import normalize_unix_timestamp_seconds


class WorkerHistoryRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def _history_lock(self):
        lock = getattr(self.worker, "_price_history_lock", None)
        return lock if lock is not None else nullcontext()

    def history_snapshot(self) -> tuple[list[float], list[float], list[float]]:
        worker = self.worker
        with self._history_lock():
            prices = list(worker.prices)
            timestamps = list(worker.price_timestamps)
            volumes = list(worker.volumes)
        return prices, timestamps, volumes

    def timestamp_snapshot(self) -> list[float]:
        worker = self.worker
        with self._history_lock():
            return list(worker.price_timestamps)

    def latest_history_timestamp(self) -> float | None:
        timestamps = self.timestamp_snapshot()
        if not timestamps:
            return None
        try:
            latest_ts = float(timestamps[-1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(latest_ts):
            return None
        return latest_ts

    def latest_history_volume(self) -> float | None:
        _, _, volumes = self.history_snapshot()
        if not volumes:
            return None
        try:
            latest_volume = float(volumes[-1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(latest_volume):
            return None
        return latest_volume

    def _normalize_tick_timestamp_for_history_locked(
        self,
        raw_ts: float | int | str | None,
    ) -> float:
        worker = self.worker
        now = worker._wall_time_now()
        normalized = self.normalize_timestamp_seconds(raw_ts)
        max_future_skew_sec = 120.0
        if normalized > (now + max_future_skew_sec):
            normalized = now
        if worker.price_timestamps:
            last_ts = float(worker.price_timestamps[-1])
            if last_ts > (now + max_future_skew_sec):
                shift = last_ts - now
                worker.price_timestamps = deque(
                    (float(ts) - shift for ts in worker.price_timestamps),
                    maxlen=worker.price_timestamps.maxlen,
                )
                last_ts = float(worker.price_timestamps[-1])
            if normalized <= (last_ts + FLOAT_COMPARISON_TOLERANCE):
                normalized = max(now, last_ts + 1e-3)
        return normalized

    def restore_price_history(self) -> None:
        worker = self.worker
        capacity = int(worker.prices.maxlen or 0)
        if capacity <= 0:
            return
        restore_limit = max(
            1,
            min(
                capacity,
                int(getattr(worker, "price_history_restore_rows", capacity) or capacity),
            ),
        )
        restored = worker.store.load_recent_price_history(worker.symbol, limit=restore_limit)
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
        if self.restored_timestamps_need_repair([sample[0] for sample in parsed_samples]):
            parsed_samples = self.repair_restored_timestamps(parsed_samples)
            timestamp_repair_applied = True

        restored_count = 0
        restore_step_sec = max(1e-3, float(worker.poll_interval_sec or 1.0))
        with self._history_lock():
            for ts, close, volume in parsed_samples:
                if worker.price_timestamps and ts <= (float(worker.price_timestamps[-1]) + FLOAT_COMPARISON_TOLERANCE):
                    ts = float(worker.price_timestamps[-1]) + restore_step_sec
                worker.price_timestamps.append(ts)
                worker.prices.append(close)
                worker.volumes.append(float(volume) if volume is not None else 0.0)
                restored_count += 1
                worker._price_samples_seen += 1

        if restored_count > 0:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Restored price history",
                {
                    "restored_samples": restored_count,
                    "buffer_capacity": capacity,
                    "restore_limit": restore_limit,
                    "timestamp_repair_applied": timestamp_repair_applied,
                },
            )

    def restored_timestamps_need_repair(self, timestamps: list[float]) -> bool:
        worker = self.worker
        total = len(timestamps)
        if total < 3:
            return False
        rounded_unique = len({round(float(ts), 6) for ts in timestamps})
        positive_deltas = 0
        prev = float(timestamps[0])
        for ts in timestamps[1:]:
            current = float(ts)
            if current > (prev + FLOAT_COMPARISON_TOLERANCE):
                positive_deltas += 1
            prev = current
        min_unique = max(3, int(total * 0.10))
        min_positive = max(2, int((total - 1) * 0.10))
        if rounded_unique < min_unique or positive_deltas < min_positive:
            return True
        candle_timeframe = float(getattr(worker.strategy, "candle_timeframe_sec", 0) or 0)
        if candle_timeframe > 1.0 and total >= max(16, int(getattr(worker.strategy, "min_history", 1) or 1)):
            span = max(0.0, float(timestamps[-1]) - float(timestamps[0]))
            min_span = candle_timeframe * max(
                4.0,
                min(float(getattr(worker.strategy, "min_history", 1) or 1), 24.0) * 0.5,
            )
            if span < min_span:
                return True
        return False

    def repair_restored_timestamps(
        self,
        samples: list[tuple[float, float, float | None]],
    ) -> list[tuple[float, float, float | None]]:
        worker = self.worker
        if not samples:
            return samples
        step_sec = max(1.0, float(worker.poll_interval_sec or 1.0))
        now_ts = worker._wall_time_now()
        start_ts = now_ts - step_sec * max(0, len(samples) - 1)
        repaired: list[tuple[float, float, float | None]] = []
        for idx, (_, close, volume) in enumerate(samples):
            repaired.append((start_ts + step_sec * idx, close, volume))
        return repaired

    def cache_price_sample(self, timestamp: float, close: float, volume: float | None) -> None:
        worker = self.worker
        max_rows = max(
            100,
            int(getattr(worker, "price_history_keep_rows", 0) or 0),
            int(worker.prices.maxlen or 0),
        )
        worker.store.append_price_sample(
            symbol=worker.symbol,
            ts=timestamp,
            close=close,
            volume=volume,
            max_rows_per_symbol=max_rows,
            candle_resolutions_sec=worker._history_candle_resolutions_sec,
        )

    def normalize_timestamp_seconds(self, raw_ts: float | int | str | None) -> float:
        worker = self.worker
        return normalize_unix_timestamp_seconds(raw_ts, fallback_ts=worker._wall_time_now())

    def normalize_tick_timestamp_for_history(self, raw_ts: float | int | str | None) -> float:
        with self._history_lock():
            return self._normalize_tick_timestamp_for_history_locked(raw_ts)

    @staticmethod
    def tick_freshness_timestamp_sec(tick: PriceTick | None) -> float | None:
        if tick is None:
            return None
        raw_freshness_ts = getattr(tick, "received_at", None)
        if raw_freshness_ts in (None, "", 0):
            raw_freshness_ts = getattr(tick, "timestamp", None)
        try:
            freshness_ts = float(raw_freshness_ts)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(freshness_ts) or freshness_ts <= 0:
            return None
        return freshness_ts

    @staticmethod
    def current_volume_from_tick(tick: PriceTick) -> tuple[float | None, float]:
        current_volume: float | None = None
        volume_sample = 0.0
        if tick.volume is not None:
            try:
                parsed_volume = float(tick.volume)
            except (TypeError, ValueError):
                parsed_volume = 0.0
            if parsed_volume > 0:
                current_volume = parsed_volume
                volume_sample = parsed_volume
        return current_volume, volume_sample

    @staticmethod
    def signal_timeframe_hint_sec(signal: Signal | None) -> float | None:
        if signal is None or not isinstance(signal.metadata, dict):
            return None
        metadata = signal.metadata
        for key in ("candle_timeframe_sec", "timeframe_sec", "fast_ma_timeframe_sec"):
            raw_value = metadata.get(key)
            try:
                parsed = float(raw_value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(parsed) and parsed > 0.0:
                return parsed
        return None

    def stale_entry_history_allows_open(self, signal: Signal) -> bool:
        worker = self.worker
        timeframe_hint_sec = self.signal_timeframe_hint_sec(signal)
        poll_baseline_sec = max(1.0, float(worker.poll_interval_sec))
        recent_window_sec = max(
            20.0,
            min(
                300.0,
                max(
                    (timeframe_hint_sec or 0.0) * 1.5,
                    poll_baseline_sec * 6.0,
                ),
            ),
        )
        max_gap_sec = max(
            poll_baseline_sec * 2.0,
            min(
                recent_window_sec * 0.75,
                max((timeframe_hint_sec or 0.0) * 1.25, 15.0),
            ),
        )
        now = worker._wall_time_now()
        now_monotonic = worker._monotonic_now()
        recent_samples = 0
        timestamps: list[float] = []
        for raw_ts in self.timestamp_snapshot():
            try:
                ts = float(raw_ts)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts):
                continue
            timestamps.append(ts)
            if (now - ts) <= recent_window_sec:
                recent_samples += 1
        min_recent_samples = 3
        last_gap_sec: float | None = None
        if len(timestamps) >= 2:
            last_gap_sec = max(0.0, timestamps[-1] - timestamps[-2])

        history_ready = recent_samples >= min_recent_samples
        if last_gap_sec is not None:
            history_ready = history_ready and (last_gap_sec <= max_gap_sec + FLOAT_COMPARISON_TOLERANCE)
        if history_ready:
            return True

        if (now_monotonic - worker._last_stale_entry_history_block_log_ts) >= worker.hold_reason_log_interval_sec:
            indicator = "-"
            if isinstance(signal.metadata, dict):
                indicator = str(signal.metadata.get("indicator") or "-")
            payload: dict[str, object] = {
                "strategy": worker.strategy_name,
                "indicator": indicator,
                "recent_window_sec": round(recent_window_sec, 3),
                "recent_samples": recent_samples,
                "min_recent_samples": min_recent_samples,
                "max_gap_sec": round(max_gap_sec, 3),
            }
            if timeframe_hint_sec is not None:
                payload["timeframe_hint_sec"] = round(timeframe_hint_sec, 3)
            if last_gap_sec is not None:
                payload["last_gap_sec"] = round(last_gap_sec, 3)
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Trade blocked by stale entry history",
                payload,
            )
            worker._last_stale_entry_history_block_log_ts = now_monotonic
        return False

    def refresh_stale_entry_candidate(
        self,
        tick: PriceTick,
        *,
        raw_tick_timestamp_sec: float,
        current_spread_pips: float,
        current_spread_pct: float,
        current_volume: float | None,
    ) -> tuple[PriceTick, float, float, float, float, float, float | None] | None:
        worker = self.worker
        if not worker.db_first_reads_enabled or worker.entry_tick_max_age_sec <= 0:
            return None
        freshness_ts = self.tick_freshness_timestamp_sec(tick)
        if freshness_ts is None:
            return None
        if (
            worker._runtime_age_sec(
                freshness_ts,
                now_monotonic=worker._monotonic_now(),
                now_wall=worker._wall_time_now(),
            )
            or 0.0
        ) <= worker.entry_tick_max_age_sec:
            return None

        refreshed_tick = worker._load_direct_tick_for_entry_fallback()
        if refreshed_tick is None:
            return None

        refreshed_raw_tick_timestamp_sec = worker._normalize_timestamp_seconds(refreshed_tick.timestamp)
        refreshed_freshness_ts = self.tick_freshness_timestamp_sec(refreshed_tick)
        if refreshed_freshness_ts is None:
            refreshed_freshness_ts = refreshed_raw_tick_timestamp_sec
        refreshed_freshness_monotonic_ts = worker._monotonic_now()
        refreshed_current_volume, refreshed_volume_sample = self.current_volume_from_tick(refreshed_tick)
        refreshed_spread_pips = worker._spread_in_pips(refreshed_tick.bid, refreshed_tick.ask)
        refreshed_spread_pct = worker._spread_in_pct(refreshed_tick.bid, refreshed_tick.ask)
        worker._replace_latest_spread_sample(refreshed_spread_pips)
        worker._replace_latest_tick_sample(
            refreshed_tick,
            current_volume=refreshed_current_volume,
            volume_sample=refreshed_volume_sample,
        )
        return (
            refreshed_tick,
            refreshed_raw_tick_timestamp_sec,
            refreshed_freshness_ts,
            refreshed_freshness_monotonic_ts,
            refreshed_spread_pips,
            refreshed_spread_pct,
            refreshed_current_volume,
        )

    def replace_latest_tick_sample(
        self,
        tick: PriceTick,
        *,
        current_volume: float | None,
        volume_sample: float,
    ) -> float:
        worker = self.worker
        with self._history_lock():
            tick_timestamp = self._normalize_tick_timestamp_for_history_locked(tick.timestamp)
            if worker.prices:
                worker.prices[-1] = tick.mid
            else:
                worker.prices.append(tick.mid)
            if worker.price_timestamps:
                worker.price_timestamps[-1] = tick_timestamp
            else:
                worker.price_timestamps.append(tick_timestamp)
            if worker.volumes:
                worker.volumes[-1] = volume_sample
            else:
                worker.volumes.append(volume_sample)
        self.cache_price_sample(tick_timestamp, tick.mid, current_volume)
        return tick_timestamp

    def append_tick_sample(
        self,
        tick: PriceTick,
        *,
        current_volume: float | None,
        volume_sample: float,
    ) -> float:
        worker = self.worker
        with self._history_lock():
            tick_timestamp = self._normalize_tick_timestamp_for_history_locked(tick.timestamp)
            worker.prices.append(tick.mid)
            worker.price_timestamps.append(tick_timestamp)
            worker.volumes.append(volume_sample)
            worker._price_samples_seen += 1
        self.cache_price_sample(tick_timestamp, tick.mid, current_volume)
        return tick_timestamp

    @staticmethod
    def strategy_candle_timeframe(strategy: Strategy | None) -> int:
        if strategy is None:
            return 0
        try:
            timeframe_sec = int(float(getattr(strategy, "candle_timeframe_sec", 0) or 0))
        except (TypeError, ValueError):
            return 0
        return max(0, timeframe_sec)

    @staticmethod
    def bucket_start_ts(ts: float, resolution_sec: int) -> float:
        if resolution_sec <= 0:
            return float(ts)
        return float(int(float(ts) // float(resolution_sec)) * int(resolution_sec))

    @staticmethod
    def coerce_positive_volume(value: object) -> float:
        try:
            volume = float(value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(volume) or volume <= 0.0:
            return 0.0
        return volume

    def strategy_history_context_limit(self, strategy: Strategy) -> int:
        min_history = max(2, int(getattr(strategy, "min_history", 2) or 2))
        return max(min_history + 8, min(min_history * 3, 2000))

    def build_candle_history_context(
        self,
        *,
        strategy: Strategy,
        base_context: StrategyContext,
    ) -> StrategyContext | None:
        worker = self.worker
        resolution_sec = self.strategy_candle_timeframe(strategy)
        if resolution_sec <= 60:
            return None
        limit = self.strategy_history_context_limit(strategy)
        rows = worker.store.load_recent_candle_history(
            worker.symbol,
            resolution_sec=resolution_sec,
            limit=limit,
        )
        if not rows:
            return None

        candle_by_bucket: dict[float, tuple[float, float, float, float, float]] = {}
        for row in rows:
            try:
                bucket_start_ts = float(row.get("bucket_start_ts") or 0.0)
                open_price = float(row.get("open") if row.get("open") is not None else row["close"])
                high_price = float(row.get("high") if row.get("high") is not None else row["close"])
                low_price = float(row.get("low") if row.get("low") is not None else row["close"])
                close = float(row["close"])
            except (KeyError, TypeError, ValueError):
                continue
            if not math.isfinite(bucket_start_ts) or bucket_start_ts <= 0.0:
                continue
            if (
                not math.isfinite(open_price)
                or not math.isfinite(high_price)
                or not math.isfinite(low_price)
                or not math.isfinite(close)
            ):
                continue
            volume = self.coerce_positive_volume(row.get("volume"))
            candle_by_bucket[bucket_start_ts] = (
                float(open_price),
                float(high_price),
                float(low_price),
                float(close),
                float(volume),
            )

        if not candle_by_bucket:
            return None

        last_bucket_start = max(candle_by_bucket)
        raw_bucket_rows: dict[float, list[tuple[float, float, float]]] = {}
        history_prices, history_timestamps, history_volumes = self.history_snapshot()
        common_len = min(len(history_prices), len(history_timestamps), len(history_volumes))
        history_prices = history_prices[:common_len]
        history_timestamps = history_timestamps[:common_len]
        history_volumes = history_volumes[:common_len]
        for price, timestamp, volume in zip(history_prices, history_timestamps, history_volumes):
            try:
                ts = float(timestamp)
                close = float(price)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts) or ts <= 0.0 or not math.isfinite(close):
                continue
            bucket_start = self.bucket_start_ts(ts, resolution_sec)
            if bucket_start < (last_bucket_start - FLOAT_COMPARISON_TOLERANCE):
                continue
            raw_bucket_rows.setdefault(float(bucket_start), []).append(
                (float(ts), float(close), self.coerce_positive_volume(volume))
            )

        ordered_buckets = sorted(candle_by_bucket.keys())
        if not ordered_buckets:
            return None
        if len(ordered_buckets) > limit:
            ordered_buckets = ordered_buckets[-limit:]
        raw_only_buckets = sorted(
            bucket_start for bucket_start in raw_bucket_rows.keys() if bucket_start not in candle_by_bucket
        )
        effective_bucket_count = len(ordered_buckets) + len(raw_only_buckets)

        timestamps: list[float] = []
        prices: list[float] = []
        volumes: list[float] = []

        def _append_synthetic_candle(
            *,
            bucket_start: float,
            open_price: float,
            high_price: float,
            low_price: float,
            close_price: float,
            volume_value: float,
        ) -> None:
            quarter_step = max(1e-3, float(resolution_sec) / 4.0)
            mid_step = max(quarter_step + 1e-3, float(resolution_sec) / 2.0)
            timestamps.extend(
                [
                    float(bucket_start),
                    float(bucket_start + quarter_step),
                    float(bucket_start + mid_step),
                    float(bucket_start + resolution_sec - 1e-3),
                ]
            )
            prices.extend(
                [
                    float(open_price),
                    float(high_price),
                    float(low_price),
                    float(close_price),
                ]
            )
            volumes.extend([0.0, 0.0, 0.0, float(volume_value)])

        for bucket_start in ordered_buckets:
            raw_rows = raw_bucket_rows.get(bucket_start)
            if raw_rows:
                for ts, close, volume in sorted(raw_rows, key=lambda item: item[0]):
                    timestamps.append(float(ts))
                    prices.append(float(close))
                    volumes.append(float(volume))
                continue
            open_price, high_price, low_price, close_price, volume_value = candle_by_bucket[bucket_start]
            _append_synthetic_candle(
                bucket_start=bucket_start,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
                volume_value=volume_value,
            )
        for bucket_start in raw_only_buckets:
            for ts, close, volume in sorted(raw_bucket_rows[bucket_start], key=lambda item: item[0]):
                timestamps.append(float(ts))
                prices.append(float(close))
                volumes.append(float(volume))

        min_history = max(2, int(getattr(strategy, "min_history", 2) or 2))
        if effective_bucket_count < min_history:
            return None

        return StrategyContext(
            symbol=worker.symbol,
            prices=prices,
            timestamps=timestamps,
            volumes=volumes,
            current_volume=base_context.current_volume,
            current_spread_pips=base_context.current_spread_pips,
            tick_size=base_context.tick_size,
            pip_size=base_context.pip_size,
        )
