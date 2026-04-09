from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging

from xtb_bot.models import PriceTick, Side, StreamHealthStatus


logger = logging.getLogger(__name__)


class WorkerCycleRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def wait_after_short_circuit(
        self,
        *,
        tick: PriceTick | None,
        last_error: str | None,
    ) -> None:
        worker = self.worker
        worker._save_state(last_price=tick.mid if tick else None, last_error=last_error)
        worker.stop_event.wait(
            worker._next_wait_duration(
                has_active_position=worker.position_book.get(worker.symbol) is not None
            )
        )

    def process_active_position(
        self,
        *,
        tick: PriceTick,
        tick_timestamp: float,
        last_error: str | None,
    ) -> bool:
        worker = self.worker
        active = worker.position_book.get(worker.symbol)
        if active is None:
            return False

        worker._clear_entry_signal_persistence_state()
        if worker._attempt_pending_missing_position_reconciliation(active, tick):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if worker._maybe_reconcile_execution_manual_close(active, tick):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if worker._maybe_execute_pending_close_retry(active, tick):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True

        snapshot = worker._get_account_snapshot_cached()
        stats = worker.risk.compute_stats(snapshot)
        open_positions_count = worker.position_book.count()
        worker._persist_account_snapshot_if_due(
            snapshot,
            open_positions=open_positions_count,
            daily_pnl=stats.daily_pnl,
            drawdown_pct=stats.total_drawdown_pct,
        )
        force_flatten, reason = worker.risk.should_force_flatten(
            snapshot,
            open_positions_count=open_positions_count,
        )

        mark_price = tick.bid if active.side == Side.BUY else tick.ask
        active.pnl = worker._calculate_pnl(active, mark_price)
        worker._persist_trade_performance_snapshot(active, mark_price, active.pnl)
        worker._persist_position_trade_state(active)
        if worker._maybe_apply_partial_take_profit(active, bid=tick.bid, ask=tick.ask):
            active = worker.position_book.get(worker.symbol)
            if active is None:
                self.wait_after_short_circuit(tick=tick, last_error=last_error)
                return True
            mark_price = tick.bid if active.side == Side.BUY else tick.ask
            active.pnl = worker._calculate_pnl(active, mark_price)
            worker._persist_trade_performance_snapshot(active, mark_price, active.pnl)
            worker._persist_position_trade_state(active)
        should_close, mark_price, close_reason = worker._should_close_position(
            active,
            tick.bid,
            tick.ask,
            now_ts=worker._monotonic_now(),
        )

        if should_close:
            worker._close_position(active, mark_price, close_reason)
            return False

        worker._apply_operational_guard_risk_reduction(
            active,
            bid=tick.bid,
            ask=tick.ask,
        )
        session_reason = worker._session_exit_reason(tick_timestamp)
        if session_reason:
            worker._close_position(active, mark_price, session_reason)
        else:
            news_close_reason = worker._apply_news_filter(active, tick_timestamp, tick.bid, tick.ask)
            if news_close_reason:
                worker._close_position(active, mark_price, news_close_reason)
            else:
                exit_signal = worker._strategy_exit_signal()
                new_stop, progress = worker._trailing_candidate_stop(
                    active,
                    tick.bid,
                    tick.ask,
                    signal=exit_signal,
                )
                if new_stop is not None:
                    worker._apply_trailing_stop(active, new_stop, progress)

                multi_emergency_reason = worker._multi_strategy_emergency_exit_reason(exit_signal)
                if multi_emergency_reason:
                    worker._close_position(active, mark_price, multi_emergency_reason)
                else:
                    reverse_reason = worker._reverse_signal_exit_reason_from_signal(
                        active,
                        exit_signal,
                        mark_price=mark_price,
                    )
                    if reverse_reason:
                        worker._close_position(active, mark_price, reverse_reason)
                    else:
                        dynamic_exit_reason = worker._strategy_dynamic_exit_reason(
                            active,
                            tick.bid,
                            tick.ask,
                            exit_signal,
                        )
                        if dynamic_exit_reason:
                            worker._close_position(active, mark_price, dynamic_exit_reason)
        if force_flatten:
            latest_active = worker.position_book.get(worker.symbol)
            if latest_active is not None:
                close_retry_pending = (
                    worker._pending_close_retry_position_id == latest_active.position_id
                    and worker._runtime_remaining_sec(worker._pending_close_retry_until_ts) > FLOAT_COMPARISON_TOLERANCE
                )
                if not close_retry_pending:
                    worker._close_position(
                        latest_active,
                        mark_price,
                        f"emergency:{reason}",
                    )
        return False

    def process_flat_entry(
        self,
        *,
        tick: PriceTick,
        raw_tick_timestamp_sec: float,
        tick_freshness_timestamp_sec: float | None,
        tick_freshness_monotonic_sec: float | None,
        tick_timestamp: float,
        current_spread_pips: float,
        current_spread_pct: float | None,
        current_volume: float,
        spread_blocked: bool,
        spread_avg: float,
        spread_threshold: float,
        stream_health: StreamHealthStatus | None,
        last_error: str | None,
    ) -> bool:
        worker = self.worker
        if worker.position_book.get(worker.symbol) is not None:
            return False

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=current_volume,
                current_spread_pips=current_spread_pips,
            ),
            real_position_lots=0.0,
        )
        signal = worker._execution_calibrated_entry_signal(
            signal,
            current_spread_pips=current_spread_pips,
            current_spread_pct=current_spread_pct,
        )
        worker._maybe_record_indicator_debug(signal, current_spread_pips)
        used_stale_entry_fallback = False

        if signal.side == Side.HOLD:
            worker._clear_entry_signal_persistence_state()
            worker._maybe_record_hold_reason(
                signal,
                current_spread_pips,
                current_spread_pct=current_spread_pct,
            )
            return False

        refreshed_candidate = worker._refresh_stale_entry_candidate(
            tick,
            raw_tick_timestamp_sec=raw_tick_timestamp_sec,
            current_spread_pips=current_spread_pips,
            current_spread_pct=current_spread_pct,
            current_volume=current_volume,
        )
        if refreshed_candidate is not None:
            used_stale_entry_fallback = True
            (
                tick,
                raw_tick_timestamp_sec,
                tick_freshness_timestamp_sec,
                tick_freshness_monotonic_sec,
                current_spread_pips,
                current_spread_pct,
                current_volume,
            ) = refreshed_candidate
            worker._update_last_price_freshness(
                freshness_ts=tick_freshness_timestamp_sec,
                freshness_monotonic_ts=tick_freshness_monotonic_sec,
            )
            signal = worker._generate_signal(
                context=worker._build_strategy_context(
                    current_volume=current_volume,
                    current_spread_pips=current_spread_pips,
                ),
                real_position_lots=0.0,
            )
            signal = worker._execution_calibrated_entry_signal(
                signal,
                current_spread_pips=current_spread_pips,
                current_spread_pct=current_spread_pct,
            )
            worker._maybe_record_indicator_debug(signal, current_spread_pips)
            if signal.side == Side.HOLD:
                self.wait_after_short_circuit(tick=tick, last_error=last_error)
                return True
        if used_stale_entry_fallback and not worker._stale_entry_history_allows_open(signal):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._entry_tick_age_allows_open(
            raw_tick_timestamp_sec,
            freshness_timestamp_sec=tick_freshness_timestamp_sec,
            freshness_monotonic_sec=tick_freshness_monotonic_sec,
        ):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._operational_guard_allows_open():
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._confidence_allows_open(signal):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._cooldown_allows_open(signal.side, signal=signal):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if spread_blocked:
            spread_samples = worker._spread_buffer_snapshot()
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by spread filter",
                {
                    "current_spread_pips": round(current_spread_pips, 6),
                    "average_spread_pips": round(spread_avg, 6),
                    "anomaly_threshold_pips": round(spread_threshold, 6),
                    "spread_multiplier": worker.spread_anomaly_multiplier,
                    "spread_samples": len(spread_samples),
                },
            )
            logger.warning(
                "Trade blocked by spread filter | symbol=%s spread=%.4f avg=%.4f thr=%.4f",
                worker.symbol,
                current_spread_pips,
                spread_avg,
                spread_threshold,
            )
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._spread_pct_allows_open(current_spread_pct, current_spread_pips):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._connectivity_allows_open():
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._stream_health_allows_open(stream_health):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._entry_news_filter_allows_open(tick_timestamp, signal.side):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._entry_signal_persistence_allows_open(signal):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if worker._pending_open_conflicts_with_new_entry():
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True
        if not worker._index_trend_structure_allows_open(
            signal,
            current_spread_pips=current_spread_pips,
        ):
            self.wait_after_short_circuit(tick=tick, last_error=last_error)
            return True

        snapshot = worker._get_account_snapshot_cached()
        stats = worker.risk.compute_stats(snapshot)
        worker._persist_account_snapshot_if_due(
            snapshot,
            open_positions=worker.position_book.count(),
            daily_pnl=stats.daily_pnl,
            drawdown_pct=stats.total_drawdown_pct,
        )

        entry = tick.ask if signal.side == Side.BUY else tick.bid
        stop_loss, take_profit = worker._build_sl_tp(
            signal.side,
            entry,
            signal.stop_loss_pips,
            signal.take_profit_pips,
        )
        stop_loss, take_profit, open_level_guard_payload = worker._apply_broker_open_level_guard(
            signal.side,
            stop_loss,
            take_profit,
            tick.bid,
            tick.ask,
        )
        if open_level_guard_payload is not None:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Broker open levels adjusted",
                {
                    "side": signal.side.value,
                    "entry": entry,
                    **open_level_guard_payload,
                },
            )
        decision = worker.risk.can_open_trade(
            snapshot=snapshot,
            symbol=worker.symbol,
            open_positions_count=worker.position_book.count(),
            entry=entry,
            stop_loss=stop_loss,
            symbol_spec=worker.symbol_spec,
            current_spread_pips=current_spread_pips,
        )
        if not decision.allowed:
            (
                decision,
                stop_loss,
                take_profit,
                open_level_guard_payload,
            ) = worker._maybe_retry_with_more_affordable_entry_variant(
                snapshot=snapshot,
                signal=signal,
                entry=entry,
                bid=tick.bid,
                ask=tick.ask,
                current_spread_pips=current_spread_pips,
                decision=decision,
            )

        trailing_override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=entry,
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
        )

        if not decision.allowed:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by risk manager",
                {
                    "reason": decision.reason,
                    "signal": signal.side.value,
                },
            )
            return False

        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Risk sizing approved",
            {
                "signal": signal.side.value,
                "suggested_volume": decision.suggested_volume,
                "entry": entry,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "current_spread_pips": current_spread_pips,
                "strategy": worker.strategy_name,
            },
        )
        slot_decision = worker.risk.try_acquire_open_slot(
            symbol=worker.symbol,
            open_positions_count=worker.position_book.count(),
        )
        if not slot_decision.acquired:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Trade blocked by risk manager",
                {
                    "reason": slot_decision.reason,
                    "signal": signal.side.value,
                },
            )
            return False

        reservation_id = slot_decision.reservation_id
        try:
            volume = decision.suggested_volume
            if worker.default_volume > 0:
                volume = min(volume, worker.default_volume)
            volume = worker._normalize_volume(volume)

            if volume <= 0:
                worker.store.record_event(
                    "WARN",
                    worker.symbol,
                    "Trade blocked by volume normalization",
                    {
                        "suggested": decision.suggested_volume,
                        "default_volume": worker.default_volume,
                    },
                )
                return False

            worker._open_position(
                side=signal.side,
                entry=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                volume=volume,
                confidence=signal.confidence,
                trailing_override=trailing_override,
                entry_strategy=worker._effective_entry_strategy_from_signal(signal),
                entry_strategy_component=worker._effective_entry_component_from_signal(signal),
                signal=signal,
            )
            worker._register_continuation_reentry_on_open(signal)
            return False
        finally:
            worker.risk.release_open_slot(reservation_id)
