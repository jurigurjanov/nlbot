from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging
import math

from xtb_bot.models import AccountSnapshot, Position, PriceTick


logger = logging.getLogger(__name__)


class WorkerDataAccessRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def publish_latest_tick(self, tick: PriceTick) -> None:
        worker = self.worker
        if worker._latest_tick_updater is None:
            return
        try:
            worker._latest_tick_updater(tick)
        except Exception:
            logger.debug("Failed to update shared latest tick cache", exc_info=True)

    def load_db_first_tick(self) -> PriceTick | None:
        worker = self.worker
        if worker._latest_tick_getter is not None:
            try:
                shared_tick = worker._latest_tick_getter(worker.symbol, worker.db_first_tick_max_age_sec)
            except Exception:
                logger.debug("Failed to load latest tick from shared cache", exc_info=True)
                shared_tick = None
            if shared_tick is not None:
                return shared_tick
        tick = worker.store.load_latest_broker_tick(
            worker.symbol,
            max_age_sec=worker.db_first_tick_max_age_sec,
        )
        if tick is not None:
            self.publish_latest_tick(tick)
        return tick

    def load_direct_tick_for_active_position_fallback(self, active: Position) -> PriceTick | None:
        worker = self.worker
        if not worker.db_first_active_position_direct_tick_fallback_enabled:
            return None

        try:
            tick = worker.broker.get_price(worker.symbol)
        except Exception:
            return None
        if tick is None:
            return None

        try:
            bid = float(tick.bid)
            ask = float(tick.ask)
        except (AttributeError, TypeError, ValueError):
            return None
        if not math.isfinite(bid) or not math.isfinite(ask):
            return None
        timestamp = worker._normalize_timestamp_seconds(getattr(tick, "timestamp", worker._wall_time_now()))
        volume: float | None = None
        try:
            if tick.volume is not None:
                parsed_volume = float(tick.volume)
                if math.isfinite(parsed_volume):
                    volume = parsed_volume
        except (TypeError, ValueError):
            volume = None
        now_monotonic = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        fallback_tick = PriceTick(
            symbol=worker.symbol,
            bid=bid,
            ask=ask,
            timestamp=float(timestamp),
            volume=volume,
            received_at=now_wall,
        )
        self.publish_latest_tick(fallback_tick)
        try:
            worker.store.upsert_broker_tick(
                symbol=worker.symbol,
                bid=float(fallback_tick.bid),
                ask=float(fallback_tick.ask),
                ts=float(fallback_tick.timestamp),
                volume=(float(fallback_tick.volume) if fallback_tick.volume is not None else None),
                source="db_first_active_position_direct_fallback",
            )
        except Exception:
            pass

        warn_interval_sec = max(15.0, worker.stream_event_cooldown_sec)
        if (
            worker._db_first_active_tick_fallback_last_warn_ts <= 0.0
            or (now_monotonic - worker._db_first_active_tick_fallback_last_warn_ts) >= warn_interval_sec
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "DB-first tick cache stale, direct broker fallback used for active position",
                {
                    "position_id": active.position_id,
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                    "max_age_sec": worker.db_first_tick_max_age_sec,
                },
            )
            worker._db_first_active_tick_fallback_last_warn_ts = now_monotonic

        return fallback_tick

    def load_direct_tick_for_entry_fallback(self, *, allow_rest_fallback: bool = True) -> PriceTick | None:
        worker = self.worker
        if not worker.db_first_reads_enabled:
            return None

        try:
            stream_only_getter = getattr(worker.broker, "get_price_stream_only", None)
            tick = None
            if callable(stream_only_getter):
                tick = stream_only_getter(
                    worker.symbol,
                    wait_timeout_sec=min(0.2, max(0.0, worker.poll_interval_sec)),
                )
            if tick is None and allow_rest_fallback:
                tick = worker.broker.get_price(worker.symbol)
        except Exception:
            return None
        if tick is None:
            return None

        try:
            bid = float(tick.bid)
            ask = float(tick.ask)
        except (AttributeError, TypeError, ValueError):
            return None
        if not math.isfinite(bid) or not math.isfinite(ask):
            return None
        timestamp = worker._normalize_timestamp_seconds(getattr(tick, "timestamp", worker._wall_time_now()))
        volume: float | None = None
        try:
            if tick.volume is not None:
                parsed_volume = float(tick.volume)
                if math.isfinite(parsed_volume):
                    volume = parsed_volume
        except (TypeError, ValueError):
            volume = None
        now_monotonic = worker._monotonic_now()
        now_wall = worker._wall_time_now()
        fallback_tick = PriceTick(
            symbol=worker.symbol,
            bid=bid,
            ask=ask,
            timestamp=float(timestamp),
            volume=volume,
            received_at=now_wall,
        )
        self.publish_latest_tick(fallback_tick)
        try:
            worker.store.upsert_broker_tick(
                symbol=worker.symbol,
                bid=float(fallback_tick.bid),
                ask=float(fallback_tick.ask),
                ts=float(fallback_tick.timestamp),
                volume=(float(fallback_tick.volume) if fallback_tick.volume is not None else None),
                source="db_first_entry_direct_fallback",
            )
        except Exception:
            pass

        warn_interval_sec = max(15.0, worker.stream_event_cooldown_sec)
        if (
            worker._db_first_entry_tick_fallback_last_warn_ts <= 0.0
            or (now_monotonic - worker._db_first_entry_tick_fallback_last_warn_ts) >= warn_interval_sec
        ):
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "DB-first tick cache stale, direct broker fallback used for entry",
                {
                    "mode": worker.mode.value,
                    "strategy": worker.strategy_name,
                    "max_age_sec": worker.db_first_tick_max_age_sec,
                },
            )
            worker._db_first_entry_tick_fallback_last_warn_ts = now_monotonic

        return fallback_tick

    def load_direct_account_snapshot_fallback(
        self,
        *,
        stale_snapshot: AccountSnapshot | None,
        allow_stale_for_active_position: bool,
    ) -> AccountSnapshot | None:
        worker = self.worker
        if not worker.db_first_reads_enabled:
            return None

        now_monotonic = worker._monotonic_now()
        warn_interval_sec = max(15.0, worker.stream_event_cooldown_sec)
        if worker._local_allowance_backoff_remaining_sec() > 0.0:
            if allow_stale_for_active_position and stale_snapshot is not None:
                return stale_snapshot
            return None

        if worker._runtime_remaining_sec(
            worker._db_first_account_snapshot_fallback_next_attempt_ts,
            now_monotonic=now_monotonic,
        ) > FLOAT_COMPARISON_TOLERANCE:
            if allow_stale_for_active_position and stale_snapshot is not None:
                return stale_snapshot
            return None

        worker._db_first_account_snapshot_fallback_next_attempt_ts = now_monotonic + warn_interval_sec
        try:
            snapshot = worker.broker.get_account_snapshot()
        except Exception:
            if allow_stale_for_active_position and stale_snapshot is not None:
                return stale_snapshot
            return None

        worker._cached_account_snapshot = snapshot
        worker._cached_account_snapshot_ts = now_monotonic
        try:
            worker.store.upsert_broker_account_snapshot(
                snapshot,
                source="db_first_account_direct_fallback",
            )
        except Exception:
            pass

        stale_age_sec: float | None = None
        if stale_snapshot is not None:
            stale_age_sec = max(0.0, worker._wall_time_now() - float(stale_snapshot.timestamp))
        if (
            worker._db_first_account_snapshot_fallback_last_warn_ts <= 0.0
            or (now_monotonic - worker._db_first_account_snapshot_fallback_last_warn_ts) >= warn_interval_sec
        ):
            payload: dict[str, object] = {
                "mode": worker.mode.value,
                "strategy": worker.strategy_name,
                "max_age_sec": worker.db_first_account_snapshot_max_age_sec,
            }
            if stale_age_sec is not None:
                payload["stale_age_sec"] = round(stale_age_sec, 3)
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "DB-first account snapshot stale, direct broker fallback used",
                payload,
            )
            worker._db_first_account_snapshot_fallback_last_warn_ts = now_monotonic

        return snapshot
