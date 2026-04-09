from __future__ import annotations

from xtb_bot.tolerances import (
    FLOAT_COMPARISON_TOLERANCE,
    float_gt,
    float_gte,
    float_lt,
    float_lte,
)

import math
from typing import Any

from xtb_bot.models import Position, Side


_TREND_ENTRY_COMPONENTS: set[str] = {
    "momentum",
    "g1",
    "g2",
    "oil",
    "trend_following",
    "crypto_trend_following",
    "donchian_breakout",
    "mean_breakout_v2",
}
_INDEX_TREND_PEAK_PROTECTION_STOP_RATIO = 0.35
_INDEX_TREND_BREAKEVEN_STOP_RATIO = 0.50


class WorkerProtectiveRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    def is_index_trend_following_position(self, position: Position) -> bool:
        worker = self.worker
        if worker._position_entry_component(position) != "index_hybrid":
            return False
        entry_signal = str(worker._position_entry_signal(position) or "").strip().lower()
        return entry_signal == "index_hybrid:trend_following"

    def capped_required_peak_pips_by_initial_stop(
        self,
        position: Position,
        required_peak_pips: float,
        *,
        configured_min_peak_pips: float,
        stop_ratio: float,
    ) -> float:
        if not self.is_index_trend_following_position(position):
            return required_peak_pips
        initial_stop_pips = self.position_initial_stop_pips(position)
        if initial_stop_pips <= 0.0:
            return required_peak_pips
        capped_required_peak_pips = max(
            max(0.0, float(configured_min_peak_pips)),
            initial_stop_pips * max(0.0, float(stop_ratio)),
        )
        return min(required_peak_pips, capped_required_peak_pips)

    def position_favorable_move_pips(self, position: Position, mark: float) -> float:
        worker = self.worker
        pip_size = worker._execution_pip_size()
        if pip_size <= 0:
            return 0.0
        move = (mark - position.open_price) if position.side == Side.BUY else (position.open_price - mark)
        return move / pip_size

    def position_adverse_move_pips(self, position: Position, mark: float) -> float:
        worker = self.worker
        pip_size = worker._execution_pip_size()
        if pip_size <= 0:
            return 0.0
        move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
        return move / pip_size

    def update_position_peak_favorable_pips(self, position: Position, mark: float) -> float:
        worker = self.worker
        current = self.position_favorable_move_pips(position, mark)
        previous = worker._position_peak_favorable_pips.get(position.position_id, 0.0)
        peak = max(previous, current, 0.0)
        worker._position_peak_favorable_pips[position.position_id] = peak
        now = worker._monotonic_now()
        peak_ts = worker._position_peak_favorable_ts.get(position.position_id, 0.0)
        if float_gt(peak, previous) or peak_ts <= 0.0:
            worker._position_peak_favorable_ts[position.position_id] = now
        return peak

    def update_position_peak_adverse_pips(self, position: Position, mark: float) -> float:
        worker = self.worker
        current = self.position_adverse_move_pips(position, mark)
        previous = worker._position_peak_adverse_pips.get(position.position_id, 0.0)
        peak = max(previous, current, 0.0)
        worker._position_peak_adverse_pips[position.position_id] = peak
        return peak

    def protective_required_min_peak_pips(
        self,
        position: Position,
        configured_min_peak_pips: float,
    ) -> float:
        worker = self.worker
        required = max(0.0, float(configured_min_peak_pips))
        pip_size = worker._execution_pip_size()
        if pip_size <= 0:
            return required
        tp_distance_pips = abs(position.take_profit - position.open_price) / pip_size
        if tp_distance_pips <= 0:
            return required
        arm_progress = max(0.0, min(1.0, float(worker.protective_exit_arm_tp_progress)))
        if arm_progress <= 0:
            return required
        required = max(required, tp_distance_pips * arm_progress)
        return self.capped_required_peak_pips_by_initial_stop(
            position,
            required,
            configured_min_peak_pips=configured_min_peak_pips,
            stop_ratio=_INDEX_TREND_PEAK_PROTECTION_STOP_RATIO,
        )

    def position_management_family(
        self,
        position: Position,
        *,
        trailing_settings: dict[str, float | str] | None = None,
    ) -> str:
        worker = self.worker
        settings = trailing_settings
        if settings is None:
            settings = worker._effective_trailing_settings(position)
        family = str(settings.get("adaptive_trailing_family") or "").strip().lower()
        if family in {"trend", "mean_reversion"}:
            return family
        return self.position_entry_management_family(position)

    def position_entry_management_family(self, position: Position) -> str:
        worker = self.worker
        entry_component = worker._position_entry_component(position)
        entry_signal = str(worker._position_entry_signal(position) or "").strip().lower()
        if entry_component == "index_hybrid":
            if entry_signal == "index_hybrid:mean_reversion":
                return "mean_reversion"
            if entry_signal == "index_hybrid:trend_following":
                return "trend"
            entry_strategy = worker._position_entry_strategy(position)
            if entry_strategy == "mean_reversion_bb":
                return "mean_reversion"
            if entry_strategy in _TREND_ENTRY_COMPONENTS:
                return "trend"
            return "neutral"
        if entry_component == "mean_reversion_bb":
            return "mean_reversion"
        if entry_component in _TREND_ENTRY_COMPONENTS:
            return "trend"

        entry_strategy = worker._position_entry_strategy(position)
        if entry_strategy == "mean_reversion_bb":
            return "mean_reversion"
        if entry_strategy in _TREND_ENTRY_COMPONENTS:
            return "trend"
        return "neutral"

    def position_initial_stop_pips(self, position: Position) -> float:
        worker = self.worker
        pip_size = worker._execution_pip_size()
        if pip_size <= 0.0:
            return 0.0
        return max(0.0, abs(position.open_price - position.stop_loss) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))

    def runner_preservation_state(
        self,
        position: Position,
        mark: float,
        *,
        trailing_settings: dict[str, float | str] | None = None,
    ) -> dict[str, float | str | bool]:
        worker = self.worker
        settings = trailing_settings
        if settings is None:
            settings = worker._effective_trailing_settings(position)
        family = self.position_management_family(position, trailing_settings=settings)
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        current_favorable_pips = max(0.0, self.position_favorable_move_pips(position, mark))
        pip_size = worker._execution_pip_size()
        tp_distance_pips = 0.0
        if pip_size > 0.0:
            tp_distance_pips = abs(position.take_profit - position.open_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        runner_threshold_enabled = worker.protective_runner_preservation_enabled and family == "trend"
        required_peak_pips = (
            max(
                worker.protective_runner_min_peak_pips,
                tp_distance_pips * worker.protective_runner_min_tp_progress if tp_distance_pips > 0.0 else 0.0,
            )
            if runner_threshold_enabled
            else 0.0
        )
        retain_ratio = (
            current_favorable_pips / max(peak_favorable_pips, FLOAT_COMPARISON_TOLERANCE)
            if peak_favorable_pips > 0.0
            else 0.0
        )
        matured = runner_threshold_enabled and float_gte(peak_favorable_pips, required_peak_pips)
        active = matured and current_favorable_pips > 0.0 and (
            float_gte(retain_ratio, worker.protective_runner_min_retain_ratio)
        )
        return {
            "family": family,
            "peak_favorable_pips": peak_favorable_pips,
            "current_favorable_pips": current_favorable_pips,
            "tp_distance_pips": tp_distance_pips,
            "required_peak_pips": required_peak_pips,
            "retain_ratio": retain_ratio,
            "matured": matured,
            "active": active,
        }

    def peak_drawdown_exit_allowed(self, position: Position, mark: float) -> bool:
        worker = self.worker
        if not worker.protective_peak_drawdown_exit_enabled:
            return False
        if worker.protective_peak_drawdown_ratio <= 0.0:
            return False
        runner_state = self.runner_preservation_state(position, mark)
        peak_favorable_pips = float(runner_state["peak_favorable_pips"])
        required_peak_pips = self.protective_required_min_peak_pips(
            position,
            worker.protective_peak_drawdown_min_peak_pips,
        )
        if (
            worker.protective_runner_preservation_enabled
            and str(runner_state["family"]) == "trend"
            and bool(runner_state["matured"])
        ):
            required_peak_pips = max(required_peak_pips, float(runner_state["required_peak_pips"]))
        if float_lt(peak_favorable_pips, required_peak_pips):
            return False
        current_favorable_pips = float(runner_state["current_favorable_pips"])
        drawdown_pips = peak_favorable_pips - current_favorable_pips
        if drawdown_pips <= 0.0:
            return False
        drawdown_ratio = drawdown_pips / max(peak_favorable_pips, FLOAT_COMPARISON_TOLERANCE)
        required_drawdown_ratio = worker.protective_peak_drawdown_ratio
        if bool(runner_state["active"]):
            required_drawdown_ratio = min(
                1.0,
                worker.protective_peak_drawdown_ratio
                * worker.protective_runner_peak_drawdown_ratio_multiplier,
            )
        return float_gte(drawdown_ratio, required_drawdown_ratio)

    def breakeven_lock_candidate_stop(self, position: Position, mark: float) -> float | None:
        worker = self.worker
        if not worker.protective_breakeven_lock_enabled:
            return None
        runner_state = self.runner_preservation_state(position, mark)
        peak_favorable_pips = float(runner_state["peak_favorable_pips"])
        required_peak_pips = worker.protective_breakeven_min_peak_pips
        if str(runner_state["family"]) == "trend":
            tp_distance_pips = max(0.0, float(runner_state["tp_distance_pips"]))
            required_peak_pips = max(
                required_peak_pips,
                tp_distance_pips * worker.protective_breakeven_min_tp_progress if tp_distance_pips > 0.0 else 0.0,
            )
            if worker.protective_runner_preservation_enabled and bool(runner_state["matured"]):
                required_peak_pips = max(required_peak_pips, float(runner_state["required_peak_pips"]))
            required_peak_pips = self.capped_required_peak_pips_by_initial_stop(
                position,
                required_peak_pips,
                configured_min_peak_pips=worker.protective_breakeven_min_peak_pips,
                stop_ratio=_INDEX_TREND_BREAKEVEN_STOP_RATIO,
            )
        if float_lt(peak_favorable_pips, required_peak_pips):
            return None
        pip_size = worker._execution_pip_size()
        if pip_size <= 0:
            return None
        offset_price = max(0.0, worker.protective_breakeven_offset_pips) * pip_size
        if position.side == Side.BUY:
            breakeven_level = worker._normalize_price(position.open_price + offset_price)
            if float_lte(breakeven_level, position.stop_loss):
                return None
            guarded = worker._apply_broker_min_stop_distance_guard(
                position,
                breakeven_level,
                mark,
                mark,
            )
            if guarded is not None and float_gt(guarded, position.stop_loss):
                return guarded
            return None
        breakeven_level = worker._normalize_price(position.open_price - offset_price)
        if float_gte(breakeven_level, position.stop_loss):
            return None
        guarded = worker._apply_broker_min_stop_distance_guard(
            position,
            breakeven_level,
            mark,
            mark,
        )
        if guarded is not None and float_lt(guarded, position.stop_loss):
            return guarded
        return None

    def peak_stagnation_exit_allowed(
        self,
        position: Position,
        mark: float,
        *,
        now_ts: float,
    ) -> bool:
        worker = self.worker
        if not worker.protective_peak_stagnation_exit_enabled:
            return False
        runner_state = self.runner_preservation_state(position, mark)
        timeout_sec = max(0.0, worker.protective_peak_stagnation_timeout_sec)
        if worker.protective_runner_preservation_enabled and bool(runner_state["active"]):
            timeout_sec *= worker.protective_runner_peak_stagnation_timeout_multiplier
        if timeout_sec <= 0:
            return False
        peak_favorable_pips = float(runner_state["peak_favorable_pips"])
        required_peak_pips = self.protective_required_min_peak_pips(
            position,
            worker.protective_peak_stagnation_min_peak_pips,
        )
        if (
            worker.protective_runner_preservation_enabled
            and str(runner_state["family"]) == "trend"
            and bool(runner_state["matured"])
        ):
            required_peak_pips = max(required_peak_pips, float(runner_state["required_peak_pips"]))
        if float_lt(peak_favorable_pips, required_peak_pips):
            return False
        peak_ts = worker._position_peak_favorable_ts.get(position.position_id, 0.0)
        if peak_ts <= 0:
            return False
        peak_age_sec = worker._runtime_age_sec(
            peak_ts,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        if peak_age_sec is None or float_lt(peak_age_sec, timeout_sec):
            return False
        current_favorable_pips = float(runner_state["current_favorable_pips"])
        if current_favorable_pips <= 0:
            return False
        retain_ratio = current_favorable_pips / max(peak_favorable_pips, FLOAT_COMPARISON_TOLERANCE)
        return float_lte(retain_ratio, worker.protective_peak_stagnation_min_retain_ratio)

    def stale_loser_exit_allowed(
        self,
        position: Position,
        mark: float,
        *,
        now_ts: float,
    ) -> bool:
        worker = self.worker
        if not worker.protective_stale_loser_exit_enabled:
            return False
        timeout_sec = max(0.0, worker.protective_stale_loser_timeout_sec)
        if timeout_sec <= 0:
            return False
        age_sec = worker._position_age_sec(
            position,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        if age_sec is None:
            return False
        if float_lt(age_sec, timeout_sec):
            return False
        stop_distance = abs(position.open_price - position.stop_loss)
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return False
        adverse_move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
        if adverse_move <= 0.0:
            return False
        loss_ratio = adverse_move / stop_distance
        if float_lt(loss_ratio, worker.protective_stale_loser_loss_ratio):
            return False
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        if float_gt(peak_favorable_pips, worker.protective_stale_loser_profit_tolerance_pips):
            return False
        return True

    def fast_fail_exit_allowed(
        self,
        position: Position,
        mark: float,
        *,
        now_ts: float,
    ) -> bool:
        worker = self.worker
        if not worker.protective_fast_fail_exit_enabled:
            return False
        if self.position_management_family(position) != "trend":
            return False
        timeout_sec = max(0.0, worker.protective_fast_fail_timeout_sec)
        if timeout_sec <= 0.0:
            return False
        age_sec = worker._position_age_sec(
            position,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        if age_sec is None:
            return False
        if age_sec <= 0.0 or float_gt(age_sec, timeout_sec):
            return False
        stop_distance = abs(position.open_price - position.stop_loss)
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return False
        adverse_move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
        if adverse_move <= 0.0:
            return False
        loss_ratio = adverse_move / stop_distance
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        effective_loss_ratio = worker.protective_fast_fail_loss_ratio
        pip_size = worker._execution_pip_size()
        tp_progress_tolerance = max(0.0, worker.protective_fast_fail_peak_tp_progress_tolerance)
        peak_tp_progress = 0.0
        has_meaningful_tp_progress = False
        if pip_size > 0.0 and tp_progress_tolerance > 0.0:
            tp_distance_pips = abs(position.take_profit - position.open_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            if tp_distance_pips > FLOAT_COMPARISON_TOLERANCE:
                peak_tp_progress = peak_favorable_pips / max(tp_distance_pips, FLOAT_COMPARISON_TOLERANCE)
                has_meaningful_tp_progress = float_gt(peak_tp_progress, tp_progress_tolerance)
            elif float_gt(peak_favorable_pips, worker.protective_fast_fail_profit_tolerance_pips):
                has_meaningful_tp_progress = True
        elif float_gt(peak_favorable_pips, worker.protective_fast_fail_profit_tolerance_pips):
            has_meaningful_tp_progress = True

        zero_followthrough = float_lte(peak_favorable_pips, worker.protective_fast_fail_profit_tolerance_pips)
        if tp_progress_tolerance > 0.0:
            zero_followthrough = zero_followthrough or float_lte(peak_tp_progress, tp_progress_tolerance)

        if zero_followthrough:
            zero_followthrough_timeout_sec = max(
                0.0,
                worker.protective_fast_fail_zero_followthrough_timeout_sec,
            )
            entry_signal = worker._position_entry_signal(position)
            if zero_followthrough_timeout_sec > 0.0 and worker._is_continuation_trend_signal(entry_signal):
                zero_followthrough_timeout_sec = min(
                    zero_followthrough_timeout_sec,
                    max(45.0, zero_followthrough_timeout_sec * 0.5),
                )
            if zero_followthrough_timeout_sec > 0.0 and float_gte(age_sec, zero_followthrough_timeout_sec):
                effective_loss_ratio = min(
                    effective_loss_ratio,
                    worker.protective_fast_fail_zero_followthrough_loss_ratio,
                )
        if float_lt(loss_ratio, effective_loss_ratio):
            return False
        if has_meaningful_tp_progress:
            return False
        return True

    def persist_trade_performance_snapshot(self, position: Position, mark: float, pnl: float) -> None:
        worker = self.worker
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        peak_adverse_pips = self.update_position_peak_adverse_pips(position, mark)
        worker.store.update_trade_performance(
            position_id=position.position_id,
            symbol=position.symbol,
            opened_at=position.opened_at,
            max_favorable_pips=peak_favorable_pips,
            max_adverse_pips=peak_adverse_pips,
            max_favorable_pnl=max(0.0, float(pnl)),
            max_adverse_pnl=min(0.0, float(pnl)),
        )

    def reversal_exit_armed(self, position: Position, mark: float) -> bool:
        worker = self.worker
        if not worker.protective_exit_require_armed_profit:
            return True
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        if float_lt(peak_favorable_pips, worker.protective_exit_arm_min_profit_pips):
            return False
        pip_size = worker._execution_pip_size()
        if pip_size <= 0:
            return peak_favorable_pips > 0
        tp_distance_pips = abs(position.take_profit - position.open_price) / pip_size
        if tp_distance_pips <= FLOAT_COMPARISON_TOLERANCE:
            return peak_favorable_pips > 0
        progress = peak_favorable_pips / tp_distance_pips
        return float_gte(progress, worker.protective_exit_arm_tp_progress)

    def early_loss_cut_allowed(self, position: Position, mark: float) -> bool:
        worker = self.worker
        if not worker.protective_early_loss_cut_enabled:
            return False
        if worker.protective_early_loss_cut_loss_ratio <= 0:
            return False
        stop_distance = abs(position.open_price - position.stop_loss)
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return False
        adverse_move = (position.open_price - mark) if position.side == Side.BUY else (mark - position.open_price)
        if adverse_move <= 0:
            return False
        loss_ratio = adverse_move / stop_distance
        if float_lt(loss_ratio, worker.protective_early_loss_cut_loss_ratio):
            return False
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        if worker.protective_early_loss_cut_never_profitable_only:
            tolerance = worker.protective_early_loss_cut_profit_tolerance_pips
            if float_gt(peak_favorable_pips, tolerance):
                return False
        return True

    def index_trend_reversal_grace_blocks_exit(
        self,
        position: Position,
        *,
        bid: float,
        ask: float,
        now_ts: float,
    ) -> bool:
        worker = self.worker
        if worker.protective_index_trend_reversal_grace_sec <= 0.0:
            return False
        if not (worker._is_index_symbol() or worker._is_commodity_symbol()):
            return False
        if self.position_management_family(position) != "trend":
            return False
        if worker._position_entry_component(position) != "index_hybrid":
            return False
        age_sec = worker._position_age_sec(
            position,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        if age_sec is None:
            return False
        if float_gte(age_sec, worker.protective_index_trend_reversal_grace_sec):
            return False
        mark = bid if position.side == Side.BUY else ask
        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        if float_gt(peak_favorable_pips, worker.protective_fast_fail_profit_tolerance_pips):
            return False
        pip_size = worker._execution_pip_size()
        if pip_size <= 0.0:
            return False
        spread_pips = abs(float(ask) - float(bid)) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if spread_pips <= 0.0:
            return False
        adverse_move_pips = max(0.0, self.position_adverse_move_pips(position, mark))
        max_adverse_pips = spread_pips * worker.protective_index_trend_reversal_grace_max_adverse_spread_ratio
        return float_lte(adverse_move_pips, max_adverse_pips)

    def fresh_reversal_grace_blocks_exit(
        self,
        position: Position,
        *,
        mark: float,
        now_ts: float,
        spread_pips: float | None = None,
    ) -> bool:
        worker = self.worker
        if worker.protective_fresh_reversal_grace_sec <= 0.0:
            return False
        age_sec = worker._position_age_sec(
            position,
            now_monotonic=now_ts,
            now_wall=worker._wall_time_now(),
        )
        if age_sec is None:
            return False
        if age_sec <= 0.0 or float_gte(age_sec, worker.protective_fresh_reversal_grace_sec):
            return False

        pip_size = worker._execution_pip_size()
        if pip_size <= 0.0:
            return False
        stop_distance_pips = abs(position.open_price - position.stop_loss) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if stop_distance_pips <= 0.0:
            return False

        current_spread_pips = 0.0
        if spread_pips is not None and math.isfinite(float(spread_pips)):
            current_spread_pips = max(0.0, float(spread_pips))
        else:
            latest_spread_pips = worker._latest_spread_pips()
            if latest_spread_pips is not None:
                current_spread_pips = max(0.0, latest_spread_pips)

        adverse_noise_cap_pips = max(
            current_spread_pips * worker.protective_fresh_reversal_grace_max_adverse_spread_ratio,
            stop_distance_pips * worker.protective_fresh_reversal_grace_max_adverse_stop_ratio,
        )
        if adverse_noise_cap_pips <= 0.0:
            return False

        peak_favorable_pips = self.update_position_peak_favorable_pips(position, mark)
        if float_gt(peak_favorable_pips, adverse_noise_cap_pips):
            return False
        adverse_move_pips = max(0.0, self.position_adverse_move_pips(position, mark))
        return float_lte(adverse_move_pips, adverse_noise_cap_pips)

    def profit_lock_allowed(self, position: Position, mark: float) -> bool:
        worker = self.worker
        favorable_pips = self.position_favorable_move_pips(position, mark)
        return float_gt(favorable_pips, worker.protective_profit_lock_min_profit_pips)

    def should_close_position(
        self,
        position: Position,
        bid: float,
        ask: float,
        *,
        now_ts: float | None = None,
    ) -> tuple[bool, float, str]:
        worker = self.worker
        mark = bid if position.side == Side.BUY else ask
        now_value = float(now_ts) if now_ts is not None else worker._monotonic_now()

        if (
            worker.protective_peak_drawdown_hard_exit_enabled
            and self.peak_drawdown_exit_allowed(position, mark)
        ):
            return True, mark, "profit_lock_peak_drawdown:price_retrace"
        if self.peak_stagnation_exit_allowed(position, mark, now_ts=now_value):
            return True, mark, "profit_lock_peak_stagnation"

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
