from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from typing import Any

from xtb_bot.models import Position, Signal, Side


_HOLD_INVALIDATION_REASONS: set[str] = {
    "no_signal",
    "no_ma_cross",
    "no_ema_cross",
    "cross_not_confirmed",
    "no_breakout",
    "breakout_invalidated",
    "zscore_not_confirmed",
    "trend_filter_not_confirmed",
    "trend_filter_blocked",
    "pullback_not_in_zone",
    "pullback_not_confirmed",
    "invalid_structure_stop",
    "regime_filter_blocked",
    "adx_below_threshold",
    "ma_slope_direction_mismatch",
    "price_too_far_from_ema",
}


class WorkerDynamicExitRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    def reverse_signal_exit_reason(self, position: Position) -> str | None:
        worker = self.worker
        signal = worker._strategy_exit_signal()
        return self.reverse_signal_exit_reason_from_signal(position, signal, mark_price=None)

    def reverse_signal_exit_reason_from_signal(
        self,
        position: Position,
        signal: Signal | None,
        mark_price: float | None = None,
    ) -> str | None:
        worker = self.worker
        if signal is None:
            return None
        reverse_reason: str | None = None
        if signal.side in {Side.BUY, Side.SELL} and signal.side != position.side:
            reverse_reason = f"reverse_signal:{signal.side.value}"
        if worker.protective_exit_on_signal_invalidation and signal.side == Side.HOLD:
            metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
            if metadata:
                indicator = str(metadata.get("indicator") or "").strip().lower()
                if indicator != "multi_strategy_netting":
                    implied_side = self.signal_implied_trend_side(metadata)
                    if implied_side is not None and implied_side != position.side:
                        reverse_reason = f"signal_invalidation:{implied_side.value}"
        if reverse_reason is None:
            hold_reason = self.hold_invalidation_reason(signal)
            if (
                hold_reason is not None
                and mark_price is not None
                and worker._fresh_reversal_grace_blocks_exit(
                    position,
                    mark=mark_price,
                    now_ts=worker._monotonic_now(),
                )
            ):
                return None
            if (
                mark_price is not None
                and worker.protective_profit_lock_on_hold_invalidation
                and hold_reason is not None
                and worker._profit_lock_allowed(position, mark_price)
            ):
                return f"profit_lock:hold_invalidation:{hold_reason}"
            if (
                mark_price is not None
                and hold_reason is not None
                and worker._peak_drawdown_exit_allowed(position, mark_price)
            ):
                return f"profit_lock_peak_drawdown:hold_invalidation:{hold_reason}"
            if (
                mark_price is not None
                and worker.protective_exit_on_signal_invalidation
                and worker.protective_early_loss_cut_on_hold_invalidation
                and hold_reason is not None
                and worker._early_loss_cut_allowed(position, mark_price)
            ):
                return f"early_loss_cut:hold_invalidation:{hold_reason}"
            return None
        if mark_price is None:
            return reverse_reason
        if worker._fresh_reversal_grace_blocks_exit(
            position,
            mark=mark_price,
            now_ts=worker._monotonic_now(),
        ):
            return None
        if worker._peak_drawdown_exit_allowed(position, mark_price):
            return f"profit_lock_peak_drawdown:{reverse_reason}"
        if worker._reversal_exit_armed(position, mark_price):
            return reverse_reason
        if worker.protective_profit_lock_on_reversal and worker._profit_lock_allowed(position, mark_price):
            return f"profit_lock:{reverse_reason}"
        if worker._early_loss_cut_allowed(position, mark_price):
            return f"early_loss_cut:{reverse_reason}"
        return None

    @staticmethod
    def hold_invalidation_reason(signal: Signal | None) -> str | None:
        if signal is None or signal.side != Side.HOLD:
            return None
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        reason = str(metadata.get("reason") or "").strip().lower()
        if not reason:
            return None
        if reason in _HOLD_INVALIDATION_REASONS:
            return reason
        return None

    def signal_implied_trend_side(self, metadata: dict[str, object]) -> Side | None:
        worker = self.worker
        explicit_side = str(metadata.get("signal_side_implied") or "").strip().lower()
        if explicit_side == Side.BUY.value:
            return Side.BUY
        if explicit_side == Side.SELL.value:
            return Side.SELL

        fast_ema = worker._metadata_number(metadata, "fast_ema")
        slow_ema = worker._metadata_number(metadata, "slow_ema")
        if fast_ema is not None and slow_ema is not None:
            if fast_ema > slow_ema:
                return Side.BUY
            if fast_ema < slow_ema:
                return Side.SELL

        fast_ma = worker._metadata_number(metadata, "fast_ma")
        slow_ma = worker._metadata_number(metadata, "slow_ma")
        if fast_ma is not None and slow_ma is not None:
            if fast_ma > slow_ma:
                return Side.BUY
            if fast_ma < slow_ma:
                return Side.SELL

        for key in ("trend", "direction"):
            raw_value = str(metadata.get(key) or "").strip().lower()
            if raw_value in {"up", "buy", "bull", "bullish", "long"}:
                return Side.BUY
            if raw_value in {"down", "sell", "bear", "bearish", "short"}:
                return Side.SELL

        trend_up = worker._metadata_bool(metadata, "trend_up")
        trend_down = worker._metadata_bool(metadata, "trend_down")
        if trend_up is True and trend_down is not True:
            return Side.BUY
        if trend_down is True and trend_up is not True:
            return Side.SELL

        breakout_up = worker._metadata_bool(metadata, "breakout_up")
        breakout_down = worker._metadata_bool(metadata, "breakout_down")
        if breakout_up is True and breakout_down is not True:
            return Side.BUY
        if breakout_down is True and breakout_up is not True:
            return Side.SELL

        recent_breakout_up = worker._metadata_bool(metadata, "recent_breakout_up")
        recent_breakout_down = worker._metadata_bool(metadata, "recent_breakout_down")
        if recent_breakout_up is True and recent_breakout_down is not True:
            return Side.BUY
        if recent_breakout_down is True and recent_breakout_up is not True:
            return Side.SELL

        return None

    @staticmethod
    def dynamic_exit_scope(metadata: dict[str, object]) -> str:
        indicator = str(metadata.get("indicator") or "").strip().lower()
        if indicator == "g1":
            return "g1"
        if indicator == "oil":
            return "oil"
        return "protective"

    @staticmethod
    def dynamic_exit_reason(scope: str, suffix: str) -> str:
        return f"strategy_exit:{scope}:{suffix}"

    def metadata_trend_invalidation(
        self,
        position: Position,
        metadata: dict[str, object],
    ) -> tuple[bool, bool]:
        worker = self.worker
        fast_ema = worker._metadata_number(metadata, "fast_ema")
        slow_ema = worker._metadata_number(metadata, "slow_ema")
        if fast_ema is not None and slow_ema is not None:
            if position.side == Side.BUY and fast_ema <= slow_ema:
                return True, True
            if position.side == Side.SELL and fast_ema >= slow_ema:
                return True, True

        fast_ma = worker._metadata_number(metadata, "fast_ma")
        slow_ma = worker._metadata_number(metadata, "slow_ma")
        if fast_ma is not None and slow_ma is not None:
            if position.side == Side.BUY and fast_ma <= slow_ma:
                return True, False
            if position.side == Side.SELL and fast_ma >= slow_ma:
                return True, False

        implied_side = self.signal_implied_trend_side(metadata)
        if implied_side is not None and implied_side != position.side:
            return True, False
        return False, False

    def generic_protective_dynamic_exit_reason(
        self,
        position: Position,
        bid: float,
        ask: float,
        metadata: dict[str, object],
    ) -> str | None:
        worker = self.worker
        if not worker.protective_exit_enabled:
            return None

        mark = bid if position.side == Side.BUY else ask
        stop_distance = abs(position.open_price - position.stop_loss)
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return None

        scope = self.dynamic_exit_scope(metadata)
        reason = str(metadata.get("reason") or "").strip().lower()
        trend_invalidated, ema_invalidated = self.metadata_trend_invalidation(position, metadata)
        now_value = worker._monotonic_now()
        pip_size = worker._execution_pip_size()
        current_spread_pips = 0.0
        if pip_size > 0.0:
            current_spread_pips = abs(float(ask) - float(bid)) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)

        if trend_invalidated and (
            worker._fresh_reversal_grace_blocks_exit(
                position,
                mark=mark,
                now_ts=now_value,
                spread_pips=current_spread_pips,
            )
            or worker._index_trend_reversal_grace_blocks_exit(
                position,
                bid=bid,
                ask=ask,
                now_ts=now_value,
            )
        ):
            trend_invalidated = False

        if worker.protective_exit_on_trend_reversal and trend_invalidated:
            if worker._peak_drawdown_exit_allowed(position, mark):
                return self.dynamic_exit_reason(scope, "peak_drawdown_trend_reversal")
            if worker._reversal_exit_armed(position, mark):
                return self.dynamic_exit_reason(scope, "trend_reversal")
            if worker.protective_profit_lock_on_reversal and worker._profit_lock_allowed(position, mark):
                return f"profit_lock:{scope}:trend_reversal"
            if worker._early_loss_cut_allowed(position, mark):
                return f"early_loss_cut:{scope}:trend_reversal"

        adverse_move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
        if adverse_move <= 0:
            return None

        if worker._fast_fail_exit_allowed(position, mark, now_ts=now_value):
            return self.dynamic_exit_reason(scope, "fast_fail")
        if worker._stale_loser_exit_allowed(position, mark, now_ts=now_value):
            return self.dynamic_exit_reason(scope, "stale_loser_timeout")

        loss_ratio = adverse_move / stop_distance
        if loss_ratio + FLOAT_COMPARISON_TOLERANCE < worker.protective_exit_loss_ratio:
            return None

        if trend_invalidated:
            if scope == "g1" and ema_invalidated:
                return self.dynamic_exit_reason(scope, "loss_guard_ema_invalidated")
            return self.dynamic_exit_reason(scope, "loss_guard_trend_invalidated")

        if worker.protective_exit_allow_adx_regime_loss and reason == "adx_below_threshold":
            adx_regime_active_after = worker._metadata_bool(metadata, "adx_regime_active_after")
            if adx_regime_active_after is False:
                return self.dynamic_exit_reason(scope, "loss_guard_adx_regime_lost")
        return None

    def strategy_dynamic_exit_reason(
        self,
        position: Position,
        bid: float,
        ask: float,
        signal: Signal | None,
    ) -> str | None:
        worker = self.worker
        if signal is None:
            return None
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}

        if worker.strategy_name == "mean_breakout_v2":
            if (
                metadata.get("indicator") == "mean_breakout_v2"
                and metadata.get("exit_hint") == "close_on_breakout_invalidation"
                and signal.side == Side.HOLD
            ):
                invalid_up = worker._metadata_bool(metadata, "breakout_invalidation_up")
                invalid_down = worker._metadata_bool(metadata, "breakout_invalidation_down")
                if position.side == Side.BUY and invalid_up is True:
                    return "strategy_exit:mean_breakout_v2:breakout_invalidation"
                if position.side == Side.SELL and invalid_down is True:
                    return "strategy_exit:mean_breakout_v2:breakout_invalidation"
            return None

        if worker.strategy_name == "mean_reversion_bb":
            if (
                signal.side == Side.HOLD
                and metadata.get("indicator") == "bollinger_bands"
                and metadata.get("reason") == "mean_reversion_target_reached"
                and metadata.get("exit_hint") == "close_on_bb_midline"
            ):
                if worker._metadata_bool(metadata, "execution_tp_floor_applied") is True:
                    return None
                return "strategy_exit:mean_reversion_bb:midline_reverted"
            return None

        if worker.strategy_name == "index_hybrid" and metadata.get("indicator") == "index_hybrid":
            exit_hint = str(metadata.get("exit_hint") or "").strip().lower()
            if exit_hint == "close_if_price_reenters_channel_mid":
                trend_exit_buy_reentry_mid = worker._metadata_bool(metadata, "trend_exit_buy_reentry_mid")
                trend_exit_sell_reentry_mid = worker._metadata_bool(metadata, "trend_exit_sell_reentry_mid")
                if position.side == Side.BUY and trend_exit_buy_reentry_mid is True:
                    return "strategy_exit:index_hybrid:trend_mid_reentry"
                if position.side == Side.SELL and trend_exit_sell_reentry_mid is True:
                    return "strategy_exit:index_hybrid:trend_mid_reentry"
            elif exit_hint == "close_if_price_reaches_channel_mid_or_opposite_band":
                mean_exit_buy_mid_target = worker._metadata_bool(metadata, "mean_exit_buy_mid_target")
                mean_exit_sell_mid_target = worker._metadata_bool(metadata, "mean_exit_sell_mid_target")
                mean_exit_buy_opposite_band = worker._metadata_bool(metadata, "mean_exit_buy_opposite_band")
                mean_exit_sell_opposite_band = worker._metadata_bool(metadata, "mean_exit_sell_opposite_band")
                if position.side == Side.BUY:
                    if mean_exit_buy_opposite_band is True:
                        return "strategy_exit:index_hybrid:mean_opposite_band"
                    if mean_exit_buy_mid_target is True:
                        return "strategy_exit:index_hybrid:mean_mid_target"
                else:
                    if mean_exit_sell_opposite_band is True:
                        return "strategy_exit:index_hybrid:mean_opposite_band"
                    if mean_exit_sell_mid_target is True:
                        return "strategy_exit:index_hybrid:mean_mid_target"

            regime_flip_exit_buy = worker._metadata_bool(metadata, "regime_flip_exit_buy")
            regime_flip_exit_sell = worker._metadata_bool(metadata, "regime_flip_exit_sell")
            if position.side == Side.BUY and regime_flip_exit_buy is True:
                return "strategy_exit:index_hybrid:regime_flip_against_position"
            if position.side == Side.SELL and regime_flip_exit_sell is True:
                return "strategy_exit:index_hybrid:regime_flip_against_position"

            trend_regime = bool(metadata.get("trend_regime"))
            mean_reversion_regime = bool(metadata.get("mean_reversion_regime"))
            channel_mid = worker._metadata_number(metadata, "channel_mid")
            channel_upper = worker._metadata_number(metadata, "channel_upper")
            channel_lower = worker._metadata_number(metadata, "channel_lower")
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

        generic_reason = self.generic_protective_dynamic_exit_reason(position, bid, ask, metadata)
        if generic_reason:
            return generic_reason
        return None
