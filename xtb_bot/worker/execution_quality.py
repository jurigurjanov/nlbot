from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math
from typing import Any

from xtb_bot.multi_strategy import clip01
from xtb_bot.models import Position, RunMode, Signal, Side
from xtb_bot.strategies.base import Strategy


_FILL_QUALITY_CACHE_TTL_SEC = 30.0
_FILL_QUALITY_MIN_SAMPLE_COUNT = 3
_FILL_QUALITY_SOFT_PRESSURE_RATIO = 0.35
_FILL_QUALITY_HARD_PRESSURE_RATIO = 1.25
_FILL_QUALITY_SOFT_ABSOLUTE_PIPS = 0.30
_FILL_QUALITY_HARD_ABSOLUTE_PIPS = 1.20
_EXECUTION_SOFT_SPREAD_TO_STOP_RATIO = 0.18
_EXECUTION_HARD_SPREAD_TO_STOP_RATIO = 0.42
_EXECUTION_SOFT_SPREAD_TO_TAKE_PROFIT_RATIO = 0.08
_EXECUTION_HARD_SPREAD_TO_TAKE_PROFIT_RATIO = 0.22
_EXECUTION_SOFT_REWARD_TO_SPREAD_RATIO = 8.0
_EXECUTION_HARD_REWARD_TO_SPREAD_RATIO = 3.5
_EXECUTION_LIMIT_HINT_PENALTY = 0.08
_EXECUTION_ENTRY_QUALITY_PENALTY_WEIGHT = 0.20
_MULTI_STRATEGY_SOFT_OVERLAP_PENALTY_RATIO = 0.35
_MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO = 0.58


class WorkerExecutionQualityRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    @staticmethod
    def metadata_number(metadata: dict[str, object], key: str) -> float | None:
        raw = metadata.get(key)
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def metadata_bool(metadata: dict[str, object], key: str) -> bool | None:
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

    @staticmethod
    def metadata_text(metadata: dict[str, object], key: str) -> str | None:
        raw = str(metadata.get(key) or "").strip().lower()
        if not raw:
            return None
        return raw

    def signal_execution_metrics(
        self,
        signal: Signal,
        *,
        current_spread_pips: float | None,
    ) -> dict[str, object]:
        worker = self.worker
        metadata = worker._signal_metadata(signal)
        spread_pips = worker._coerce_positive_finite(current_spread_pips)
        stop_loss_pips = worker._coerce_positive_finite(signal.stop_loss_pips)
        take_profit_pips = worker._coerce_positive_finite(signal.take_profit_pips)
        spread_to_stop_ratio: float | None = None
        spread_to_take_profit_ratio: float | None = None
        reward_to_spread_ratio: float | None = None
        if spread_pips is not None:
            if stop_loss_pips is not None:
                spread_to_stop_ratio = spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
            if take_profit_pips is not None:
                spread_to_take_profit_ratio = spread_pips / max(take_profit_pips, FLOAT_COMPARISON_TOLERANCE)
                reward_to_spread_ratio = take_profit_pips / max(spread_pips, FLOAT_COMPARISON_TOLERANCE)
        return {
            "spread_pips": spread_pips,
            "stop_loss_pips": stop_loss_pips,
            "take_profit_pips": take_profit_pips,
            "spread_to_stop_ratio": spread_to_stop_ratio,
            "spread_to_take_profit_ratio": spread_to_take_profit_ratio,
            "reward_to_spread_ratio": reward_to_spread_ratio,
            "order_type_hint": self.metadata_text(metadata, "suggested_order_type"),
            "entry_quality_penalty": max(
                0.0,
                float(self.metadata_number(metadata, "entry_quality_penalty") or 0.0),
            ),
        }

    @staticmethod
    def execution_quality_penalty_from_metrics(
        metrics: dict[str, object],
        *,
        overlap_ratio: float | None = None,
    ) -> tuple[float, list[str]]:
        penalty = 0.0
        reasons: list[str] = []

        spread_to_stop_ratio = metrics.get("spread_to_stop_ratio")
        if isinstance(spread_to_stop_ratio, (int, float)) and spread_to_stop_ratio > _EXECUTION_SOFT_SPREAD_TO_STOP_RATIO:
            stop_penalty = min(
                0.34,
                ((float(spread_to_stop_ratio) - _EXECUTION_SOFT_SPREAD_TO_STOP_RATIO) / max(
                    _EXECUTION_HARD_SPREAD_TO_STOP_RATIO - _EXECUTION_SOFT_SPREAD_TO_STOP_RATIO,
                    FLOAT_COMPARISON_TOLERANCE,
                )) * 0.34,
            )
            penalty += max(0.0, stop_penalty)
            reasons.append("spread_drag_vs_stop")

        spread_to_take_profit_ratio = metrics.get("spread_to_take_profit_ratio")
        if (
            isinstance(spread_to_take_profit_ratio, (int, float))
            and spread_to_take_profit_ratio > _EXECUTION_SOFT_SPREAD_TO_TAKE_PROFIT_RATIO
        ):
            take_penalty = min(
                0.24,
                ((float(spread_to_take_profit_ratio) - _EXECUTION_SOFT_SPREAD_TO_TAKE_PROFIT_RATIO) / max(
                    _EXECUTION_HARD_SPREAD_TO_TAKE_PROFIT_RATIO - _EXECUTION_SOFT_SPREAD_TO_TAKE_PROFIT_RATIO,
                    FLOAT_COMPARISON_TOLERANCE,
                )) * 0.24,
            )
            penalty += max(0.0, take_penalty)
            reasons.append("spread_drag_vs_take_profit")

        reward_to_spread_ratio = metrics.get("reward_to_spread_ratio")
        if isinstance(reward_to_spread_ratio, (int, float)) and reward_to_spread_ratio < _EXECUTION_SOFT_REWARD_TO_SPREAD_RATIO:
            reward_penalty = min(
                0.22,
                ((_EXECUTION_SOFT_REWARD_TO_SPREAD_RATIO - float(reward_to_spread_ratio)) / max(
                    _EXECUTION_SOFT_REWARD_TO_SPREAD_RATIO - _EXECUTION_HARD_REWARD_TO_SPREAD_RATIO,
                    FLOAT_COMPARISON_TOLERANCE,
                )) * 0.22,
            )
            penalty += max(0.0, reward_penalty)
            reasons.append("reward_to_spread_compressed")

        if metrics.get("order_type_hint") == "limit":
            penalty += _EXECUTION_LIMIT_HINT_PENALTY
            reasons.append("market_execution_overrides_limit_hint")

        entry_quality_penalty = metrics.get("entry_quality_penalty")
        if isinstance(entry_quality_penalty, (int, float)) and entry_quality_penalty > 0.0:
            penalty += min(0.14, float(entry_quality_penalty) * _EXECUTION_ENTRY_QUALITY_PENALTY_WEIGHT)
            reasons.append("entry_quality_drag")

        if overlap_ratio is not None and overlap_ratio > _MULTI_STRATEGY_SOFT_OVERLAP_PENALTY_RATIO:
            overlap_penalty = min(
                0.26,
                ((float(overlap_ratio) - _MULTI_STRATEGY_SOFT_OVERLAP_PENALTY_RATIO) / max(
                    _MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO - _MULTI_STRATEGY_SOFT_OVERLAP_PENALTY_RATIO,
                    FLOAT_COMPARISON_TOLERANCE,
                )) * 0.26,
            )
            penalty += max(0.0, overlap_penalty)
            reasons.append("soft_multi_strategy_overlap")

        return min(0.85, max(0.0, penalty)), reasons

    @staticmethod
    def merge_reason_lists(*reason_lists: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for reason_list in reason_lists:
            for reason in reason_list:
                text = str(reason or "").strip().lower()
                if not text or text in seen:
                    continue
                seen.add(text)
                merged.append(text)
        return merged

    def signal_fill_quality_strategy(self, signal: Signal) -> str:
        worker = self.worker
        metadata = worker._signal_metadata(signal)
        base_strategy = worker._base_strategy_label()
        if metadata.get("indicator") == "multi_strategy_netting":
            for key in (
                "multi_entry_strategy_component",
                "multi_dominant_component_strategy",
                "multi_representative_strategy",
                "multi_entry_strategy",
                "strategy",
            ):
                normalized = worker._normalize_strategy_label(metadata.get(key))
                if normalized:
                    return normalized
            return base_strategy
        for key in (
            "strategy",
            "indicator",
            "multi_entry_strategy_component",
            "multi_dominant_component_strategy",
            "multi_entry_strategy",
            "multi_representative_strategy",
        ):
            normalized = worker._normalize_strategy_label(metadata.get(key))
            if normalized:
                return normalized
        return base_strategy

    def recent_fill_quality_summary(self, strategy_name: str) -> dict[str, object]:
        worker = self.worker
        normalized_strategy = worker._normalize_strategy_label(strategy_name)
        if not normalized_strategy:
            return {"sample_count": 0}
        cache_key = (worker.symbol.upper(), normalized_strategy)
        now = worker._monotonic_now()
        cached = worker._fill_quality_summary_cache.get(cache_key)
        if cached is not None and (now - cached[0]) <= _FILL_QUALITY_CACHE_TTL_SEC:
            return dict(cached[1])

        summary = worker.store.summarize_recent_trade_execution_quality(
            symbol=worker.symbol,
            strategy_entry=normalized_strategy,
            limit=12,
        )
        normalized_summary: dict[str, object] = {
            "sample_count": int((summary or {}).get("sample_count") or 0),
            "avg_entry_adverse_slippage_pips": float((summary or {}).get("avg_entry_adverse_slippage_pips") or 0.0),
            "avg_exit_adverse_slippage_pips": float((summary or {}).get("avg_exit_adverse_slippage_pips") or 0.0),
            "avg_total_adverse_slippage_pips": float((summary or {}).get("avg_total_adverse_slippage_pips") or 0.0),
            "max_total_adverse_slippage_pips": float((summary or {}).get("max_total_adverse_slippage_pips") or 0.0),
            "adverse_trade_share": float((summary or {}).get("adverse_trade_share") or 0.0),
        }
        worker._fill_quality_summary_cache[cache_key] = (now, dict(normalized_summary))
        return normalized_summary

    def fill_quality_weight_multiplier(
        self,
        strategy_name: str,
        *,
        current_spread_pips: float | None,
    ) -> tuple[float, dict[str, object], float | None]:
        worker = self.worker
        summary = self.recent_fill_quality_summary(strategy_name)
        sample_count = int(summary.get("sample_count") or 0)
        if sample_count < _FILL_QUALITY_MIN_SAMPLE_COUNT:
            return 1.0, summary, None

        avg_total_adverse = max(0.0, float(summary.get("avg_total_adverse_slippage_pips") or 0.0))
        if avg_total_adverse <= FLOAT_COMPARISON_TOLERANCE:
            return 1.0, summary, None

        spread_pips = worker._coerce_positive_finite(current_spread_pips)
        pressure_ratio: float | None = None
        penalty = 0.0
        if spread_pips is not None:
            pressure_ratio = avg_total_adverse / max(spread_pips, FLOAT_COMPARISON_TOLERANCE)
            if pressure_ratio > _FILL_QUALITY_SOFT_PRESSURE_RATIO:
                penalty = min(
                    0.45,
                    ((pressure_ratio - _FILL_QUALITY_SOFT_PRESSURE_RATIO) / max(
                        _FILL_QUALITY_HARD_PRESSURE_RATIO - _FILL_QUALITY_SOFT_PRESSURE_RATIO,
                        FLOAT_COMPARISON_TOLERANCE,
                    )) * 0.45,
                )
        elif avg_total_adverse > _FILL_QUALITY_SOFT_ABSOLUTE_PIPS:
            penalty = min(
                0.45,
                ((avg_total_adverse - _FILL_QUALITY_SOFT_ABSOLUTE_PIPS) / max(
                    _FILL_QUALITY_HARD_ABSOLUTE_PIPS - _FILL_QUALITY_SOFT_ABSOLUTE_PIPS,
                    FLOAT_COMPARISON_TOLERANCE,
                )) * 0.45,
            )

        multiplier = max(0.55, 1.0 - max(0.0, penalty))
        return multiplier, summary, pressure_ratio

    def resolve_mean_breakout_runtime_strategy(
        self,
        *,
        entry_strategy: str,
        entry_strategy_component: str,
    ) -> Strategy | None:
        worker = self.worker
        normalized_entry = worker._normalize_strategy_label(entry_strategy)
        normalized_component = worker._normalize_strategy_label(entry_strategy_component)
        if normalized_component and normalized_component != "mean_breakout_v2":
            return None
        if normalized_entry and normalized_entry != "mean_breakout_v2":
            return None
        candidates: list[tuple[str, Strategy]] = [
            (worker._normalize_strategy_label(worker.strategy_name), worker.strategy),
        ]
        candidates.extend(
            (worker._normalize_strategy_label(name), strategy)
            for name, strategy in worker._multi_strategy_components
        )
        for name, strategy in candidates:
            if name == "mean_breakout_v2":
                return strategy
        return None

    def signal_atr_pips_for_entry(self, signal: Signal | None) -> float | None:
        worker = self.worker
        if signal is None:
            return None
        metadata = worker._signal_metadata(signal)
        atr_pips = worker._metadata_number(metadata, "atr_pips")
        if atr_pips is not None and atr_pips > 0.0:
            return atr_pips
        atr_stop_pips = worker._metadata_number(metadata, "atr_stop_pips")
        atr_multiplier = worker._metadata_number(metadata, "atr_multiplier")
        if (
            atr_stop_pips is not None
            and atr_stop_pips > 0.0
            and atr_multiplier is not None
            and atr_multiplier > 0.0
        ):
            derived = atr_stop_pips / max(atr_multiplier, FLOAT_COMPARISON_TOLERANCE)
            if math.isfinite(derived) and derived > 0.0:
                return derived
        return None

    def slippage_pips(
        self,
        *,
        side: Side,
        reference_price: float | None,
        fill_price: float | None,
        context: str,
    ) -> tuple[float | None, float | None]:
        worker = self.worker
        reference = worker._coerce_positive_finite(reference_price)
        fill = worker._coerce_positive_finite(fill_price)
        pip_size = worker._execution_pip_size()
        if side not in {Side.BUY, Side.SELL}:
            return None, None
        if reference is None or fill is None or pip_size <= 0.0 or not math.isfinite(pip_size):
            return None, None

        if context == "entry":
            signed = ((fill - reference) / pip_size) * (1.0 if side == Side.BUY else -1.0)
        else:
            signed = ((reference - fill) / pip_size) * (1.0 if side == Side.BUY else -1.0)
        if not math.isfinite(signed):
            return None, None
        return signed, max(0.0, signed)

    def maybe_adapt_mean_breakout_buffer_after_entry_slippage(
        self,
        *,
        position_id: str,
        entry_fill_quality: dict[str, float] | None,
        signal: Signal | None,
        entry_strategy: str,
        entry_strategy_component: str,
        reference_price: float | None,
        fill_price: float | None,
    ) -> None:
        worker = self.worker
        if not entry_fill_quality:
            return
        strategy = self.resolve_mean_breakout_runtime_strategy(
            entry_strategy=entry_strategy,
            entry_strategy_component=entry_strategy_component,
        )
        if strategy is None:
            return
        adverse_slippage_pips = worker._coerce_positive_finite(
            entry_fill_quality.get("entry_adverse_slippage_pips")
        )
        if adverse_slippage_pips is None:
            return
        atr_pips = self.signal_atr_pips_for_entry(signal)
        if atr_pips is None:
            return
        threshold_pips = 0.5 * atr_pips
        if adverse_slippage_pips <= (threshold_pips + FLOAT_COMPARISON_TOLERANCE):
            return

        try:
            current_buffer_pips = max(0.0, float(getattr(strategy, "breakout_min_buffer_pips", 0.0)))
        except (TypeError, ValueError):
            current_buffer_pips = 0.0
        if not math.isfinite(current_buffer_pips):
            current_buffer_pips = 0.0
        target_buffer_pips = max(current_buffer_pips, adverse_slippage_pips)
        if target_buffer_pips <= (current_buffer_pips + FLOAT_COMPARISON_TOLERANCE):
            return

        setattr(strategy, "breakout_min_buffer_pips", float(target_buffer_pips))
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Mean breakout buffer auto-increased after slippage",
            {
                "position_id": position_id,
                "entry_strategy": entry_strategy,
                "entry_strategy_component": entry_strategy_component,
                "entry_reference_price": reference_price,
                "entry_fill_price": fill_price,
                "entry_adverse_slippage_pips": adverse_slippage_pips,
                "atr_pips": atr_pips,
                "slippage_threshold_pips": threshold_pips,
                "breakout_min_buffer_pips_before": current_buffer_pips,
                "breakout_min_buffer_pips_after": target_buffer_pips,
            },
        )

    def execution_calibrated_entry_signal(
        self,
        signal: Signal,
        *,
        current_spread_pips: float,
        current_spread_pct: float,
    ) -> Signal:
        worker = self.worker
        if signal.side not in {Side.BUY, Side.SELL}:
            return signal

        metadata = dict(worker._signal_metadata(signal))
        metrics = self.signal_execution_metrics(
            signal,
            current_spread_pips=current_spread_pips,
        )
        overlap_ratio: float | None = None
        if metadata.get("indicator") == "multi_strategy_netting":
            buy_power = worker._metadata_number(metadata, "buy_power")
            sell_power = worker._metadata_number(metadata, "sell_power")
            if buy_power is not None and sell_power is not None:
                dominant = max(abs(float(buy_power)), abs(float(sell_power)))
                opposing = min(abs(float(buy_power)), abs(float(sell_power)))
                if dominant > FLOAT_COMPARISON_TOLERANCE and opposing > FLOAT_COMPARISON_TOLERANCE:
                    overlap_ratio = opposing / dominant

        hard_reasons: list[str] = []
        spread_to_stop_ratio = metrics.get("spread_to_stop_ratio")
        if isinstance(spread_to_stop_ratio, (int, float)) and spread_to_stop_ratio >= _EXECUTION_HARD_SPREAD_TO_STOP_RATIO:
            hard_reasons.append("spread_cost_exceeds_stop_budget")

        spread_to_take_profit_ratio = metrics.get("spread_to_take_profit_ratio")
        if (
            isinstance(spread_to_take_profit_ratio, (int, float))
            and spread_to_take_profit_ratio >= _EXECUTION_HARD_SPREAD_TO_TAKE_PROFIT_RATIO
        ):
            hard_reasons.append("spread_cost_exceeds_take_profit_budget")

        reward_to_spread_ratio = metrics.get("reward_to_spread_ratio")
        if isinstance(reward_to_spread_ratio, (int, float)) and reward_to_spread_ratio <= _EXECUTION_HARD_REWARD_TO_SPREAD_RATIO:
            hard_reasons.append("reward_to_spread_too_low")

        if overlap_ratio is not None and overlap_ratio >= _MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO:
            hard_reasons.append("multi_strategy_overlap")

        penalty, penalty_reasons = self.execution_quality_penalty_from_metrics(
            metrics,
            overlap_ratio=overlap_ratio,
        )
        execution_reasons = self.merge_reason_lists(hard_reasons, penalty_reasons)
        confidence_before = worker._confidence_value(signal)
        confidence_after = clip01(confidence_before * max(0.0, 1.0 - penalty))

        metadata["execution_quality_status"] = "pass"
        metadata["execution_quality_penalty"] = penalty
        metadata["execution_confidence_before"] = confidence_before
        metadata["execution_confidence_after"] = confidence_after
        metadata["execution_current_spread_pips"] = current_spread_pips
        metadata["execution_current_spread_pct"] = current_spread_pct
        metadata["execution_order_type_hint"] = metrics.get("order_type_hint")
        metadata["execution_entry_quality_penalty"] = metrics.get("entry_quality_penalty")
        metadata["execution_spread_to_stop_ratio"] = spread_to_stop_ratio
        metadata["execution_spread_to_take_profit_ratio"] = spread_to_take_profit_ratio
        metadata["execution_reward_to_spread_ratio"] = reward_to_spread_ratio
        if overlap_ratio is not None:
            metadata["execution_multi_overlap_ratio"] = overlap_ratio
        if execution_reasons:
            metadata["execution_reasons"] = execution_reasons

        spike_guard_payload = worker._entry_spike_guard_block_payload(
            signal,
            current_spread_pips=current_spread_pips,
        )
        if spike_guard_payload is not None:
            metadata.update(spike_guard_payload)
            metadata["execution_quality_status"] = "blocked"
            metadata["blocked_signal_side"] = signal.side.value
            metadata["execution_reasons"] = self.merge_reason_lists(execution_reasons, ["entry_spike_detected"])
            return Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=float(signal.stop_loss_pips),
                take_profit_pips=float(signal.take_profit_pips),
                metadata=metadata,
            )

        if metadata.get("indicator") != "multi_strategy_netting":
            fill_quality_strategy = self.signal_fill_quality_strategy(signal)
            fill_quality_multiplier, fill_quality_summary, fill_quality_pressure_ratio = self.fill_quality_weight_multiplier(
                fill_quality_strategy,
                current_spread_pips=current_spread_pips,
            )
            metadata["execution_fill_quality_strategy"] = fill_quality_strategy
            metadata["execution_fill_quality_weight_multiplier"] = fill_quality_multiplier
            metadata["execution_fill_quality_sample_count"] = int(fill_quality_summary.get("sample_count") or 0)
            metadata["execution_fill_quality_avg_total_adverse_slippage_pips"] = float(
                fill_quality_summary.get("avg_total_adverse_slippage_pips") or 0.0
            )
            metadata["execution_fill_quality_adverse_trade_share"] = float(
                fill_quality_summary.get("adverse_trade_share") or 0.0
            )
            if fill_quality_pressure_ratio is not None:
                metadata["execution_fill_quality_pressure_ratio"] = fill_quality_pressure_ratio
            if fill_quality_multiplier < 1.0 - FLOAT_COMPARISON_TOLERANCE:
                confidence_after = clip01(confidence_after * fill_quality_multiplier)
                metadata["execution_confidence_after"] = confidence_after
                execution_reasons = self.merge_reason_lists(execution_reasons, ["post_fill_deweight"])
                metadata["execution_reasons"] = execution_reasons

        if hard_reasons:
            metadata["execution_quality_status"] = "blocked"
            metadata["blocked_signal_side"] = signal.side.value
            metadata["reason"] = (
                "multi_overlap_hold"
                if "multi_strategy_overlap" in hard_reasons
                else "execution_cost_too_high"
            )
            return Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=float(signal.stop_loss_pips),
                take_profit_pips=float(signal.take_profit_pips),
                metadata=metadata,
            )

        if penalty > FLOAT_COMPARISON_TOLERANCE:
            metadata["execution_quality_status"] = "adjusted"

        return Signal(
            side=signal.side,
            confidence=confidence_after,
            stop_loss_pips=float(signal.stop_loss_pips),
            take_profit_pips=float(signal.take_profit_pips),
            metadata=metadata,
        )

    def persist_entry_execution_quality(
        self,
        *,
        position_id: str,
        symbol: str,
        side: Side,
        reference_price: float | None,
        fill_price: float | None,
    ) -> dict[str, float] | None:
        worker = self.worker
        signed_pips, adverse_pips = self.slippage_pips(
            side=side,
            reference_price=reference_price,
            fill_price=fill_price,
            context="entry",
        )
        if signed_pips is None and adverse_pips is None:
            return None
        worker.store.update_trade_execution_quality(
            position_id=position_id,
            symbol=symbol,
            entry_reference_price=reference_price,
            entry_fill_price=fill_price,
            entry_slippage_pips=signed_pips,
            entry_adverse_slippage_pips=adverse_pips,
        )
        return {
            "entry_slippage_pips": float(signed_pips or 0.0),
            "entry_adverse_slippage_pips": float(adverse_pips or 0.0),
        }

    def persist_exit_execution_quality(
        self,
        *,
        position_id: str,
        symbol: str,
        side: Side,
        reference_price: float | None,
        fill_price: float | None,
    ) -> dict[str, float] | None:
        worker = self.worker
        signed_pips, adverse_pips = self.slippage_pips(
            side=side,
            reference_price=reference_price,
            fill_price=fill_price,
            context="exit",
        )
        if signed_pips is None and adverse_pips is None:
            return None
        worker.store.update_trade_execution_quality(
            position_id=position_id,
            symbol=symbol,
            exit_reference_price=reference_price,
            exit_fill_price=fill_price,
            exit_slippage_pips=signed_pips,
            exit_adverse_slippage_pips=adverse_pips,
        )
        return {
            "exit_slippage_pips": float(signed_pips or 0.0),
            "exit_adverse_slippage_pips": float(adverse_pips or 0.0),
        }

    def maybe_mark_symbol_guaranteed_stop_required_after_slippage(
        self,
        *,
        position: Position,
        final_close_price: float | None,
        reason: str,
    ) -> None:
        worker = self.worker
        if worker.mode != RunMode.EXECUTION:
            return
        if not worker.stop_slippage_auto_guaranteed_stop_enabled:
            return
        if str(reason).strip().lower() != "stop_loss":
            return
        if final_close_price is None or final_close_price <= 0:
            return
        stop_distance = abs(float(position.open_price) - float(position.stop_loss))
        if stop_distance <= FLOAT_COMPARISON_TOLERANCE:
            return
        adverse_move = (
            float(final_close_price) - float(position.open_price)
            if position.side == Side.SELL
            else float(position.open_price) - float(final_close_price)
        )
        if adverse_move <= 0:
            return
        slippage_ratio = adverse_move / stop_distance
        if slippage_ratio + FLOAT_COMPARISON_TOLERANCE < worker.stop_slippage_auto_guaranteed_stop_ratio:
            return

        marker = getattr(worker.broker, "mark_symbol_guaranteed_stop_required", None)
        if not callable(marker):
            return

        source = f"runtime_stop_slippage_ratio_{slippage_ratio:.2f}"
        try:
            marker(worker.symbol, source=source)
        except Exception as exc:
            worker.store.record_event(
                "WARN",
                worker.symbol,
                "Failed to auto-enable guaranteed stop after stop slippage",
                {
                    "position_id": position.position_id,
                    "reason": reason,
                    "error": str(exc),
                    "slippage_ratio": round(slippage_ratio, 4),
                },
            )
            return
        worker.store.record_event(
            "WARN",
            worker.symbol,
            "Auto-enabled guaranteed stop after severe stop slippage",
            {
                "position_id": position.position_id,
                "reason": reason,
                "open_price": position.open_price,
                "stop_loss": position.stop_loss,
                "close_price": final_close_price,
                "slippage_ratio": round(slippage_ratio, 4),
                "threshold_ratio": worker.stop_slippage_auto_guaranteed_stop_ratio,
                "source": source,
            },
        )
