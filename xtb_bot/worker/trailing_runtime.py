from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math
from typing import Any

from xtb_bot.models import Position, Signal, Side
from xtb_bot.multi_strategy import clip01


class WorkerTrailingRuntime:
    def __init__(self, worker: Any) -> None:
        self.worker = worker

    def effective_trailing_settings(self, position: Position) -> dict[str, float | str]:
        worker = self.worker
        override = worker._get_position_trailing_override(position.position_id)
        default_family = worker._position_entry_management_family(position)
        default_initial_stop_pips = worker._position_initial_stop_pips(position)
        if override is None:
            return {
                "trailing_mode": "distance",
                "trailing_activation_ratio": worker.trailing_activation_ratio,
                "trailing_distance_pips": worker.trailing_distance_pips,
                "trailing_breakeven_offset_pips": worker._trailing_breakeven_offset_for_symbol_pips(),
                "trailing_activation_r_multiple": 0.0,
                "trailing_activation_min_profit_pips": 0.0,
                "fast_ma_buffer_atr": 0.0,
                "fast_ma_buffer_pips": 0.0,
                "fast_ma_min_step_pips": 0.0,
                "fast_ma_update_cooldown_sec": 0.0,
                "adaptive_trailing_family": default_family,
                "adaptive_trailing_entry_extreme_abs": 0.0,
                "adaptive_trailing_initial_stop_pips": default_initial_stop_pips,
                "trailing_distance_override_explicit": "false",
            }
        trailing_mode = str(override.get("trailing_mode") or "distance").strip().lower()
        explicit_distance_override = trailing_mode != "fast_ma" and (
            "trailing_activation_ratio" in override
            or "trailing_distance_pips" in override
            or "trailing_activation_pips" in override
        )
        adaptive_family = str(override.get("adaptive_trailing_family") or "").strip().lower()
        if adaptive_family not in {"trend", "mean_reversion", "neutral"}:
            adaptive_family = default_family
        adaptive_initial_stop_pips = max(
            0.0,
            float(override.get("adaptive_trailing_initial_stop_pips", default_initial_stop_pips)),
        )
        return {
            "trailing_mode": ("fast_ma" if trailing_mode == "fast_ma" else "distance"),
            "trailing_activation_ratio": max(
                0.0,
                min(1.0, float(override.get("trailing_activation_ratio", worker.trailing_activation_ratio))),
            ),
            "trailing_distance_pips": max(
                0.1,
                float(override.get("trailing_distance_pips", worker.trailing_distance_pips)),
            ),
            "trailing_breakeven_offset_pips": max(
                0.0,
                float(
                    override.get(
                        "trailing_breakeven_offset_pips",
                        worker._trailing_breakeven_offset_for_symbol_pips(),
                    )
                ),
            ),
            "trailing_activation_r_multiple": max(
                0.0,
                float(override.get("trailing_activation_r_multiple", 0.0)),
            ),
            "trailing_activation_min_profit_pips": max(
                0.0,
                float(override.get("trailing_activation_min_profit_pips", 0.0)),
            ),
            "fast_ma_buffer_atr": max(
                0.0,
                float(override.get("fast_ma_buffer_atr", 0.0)),
            ),
            "fast_ma_buffer_pips": max(
                0.0,
                float(override.get("fast_ma_buffer_pips", 0.0)),
            ),
            "fast_ma_min_step_pips": max(
                0.0,
                float(override.get("fast_ma_min_step_pips", 0.0)),
            ),
            "fast_ma_update_cooldown_sec": max(
                0.0,
                float(override.get("fast_ma_update_cooldown_sec", 0.0)),
            ),
            "adaptive_trailing_family": adaptive_family,
            "adaptive_trailing_entry_extreme_abs": max(
                0.0,
                float(override.get("adaptive_trailing_entry_extreme_abs", 0.0)),
            ),
            "adaptive_trailing_initial_stop_pips": adaptive_initial_stop_pips,
            "trailing_distance_override_explicit": "true" if explicit_distance_override else "false",
        }

    def parse_trailing_override_from_signal(
        self,
        signal: Signal,
        entry_price: float,
        take_profit_price: float,
        stop_loss_price: float | None = None,
    ) -> dict[str, float | str] | None:
        worker = self.worker
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        trailing_raw = metadata.get("trailing_stop")
        trailing_override: dict[str, float | str] = {}
        entry_component = worker._effective_entry_component_from_signal(signal)
        entry_strategy = worker._effective_entry_strategy_from_signal(signal)
        default_activation_ratio = worker._strategy_float_param_for_label(
            entry_component,
            "trailing_activation_ratio",
            worker._strategy_float_param_for_label(
                entry_strategy,
                "trailing_activation_ratio",
                worker.trailing_activation_ratio,
            ),
        )
        default_breakeven_offset_pips = worker._strategy_float_param_for_label(
            entry_component,
            "trailing_breakeven_offset_pips",
            worker._strategy_float_param_for_label(
                entry_strategy,
                "trailing_breakeven_offset_pips",
                worker._trailing_breakeven_offset_for_symbol_pips(),
            ),
        )
        if isinstance(trailing_raw, dict) and bool(trailing_raw.get("trailing_enabled", False)):
            trailing_mode = str(trailing_raw.get("trailing_mode") or "distance").strip().lower()
            if trailing_mode == "fast_ma":
                activation_r_raw = trailing_raw.get("trailing_activation_r_multiple", 0.0)
                activation_min_profit_raw = trailing_raw.get("trailing_activation_min_profit_pips", 0.0)
                fast_ma_buffer_atr_raw = trailing_raw.get("fast_ma_buffer_atr", 0.0)
                fast_ma_buffer_pips_raw = trailing_raw.get("fast_ma_buffer_pips", 0.0)
                fast_ma_min_step_pips_raw = trailing_raw.get("fast_ma_min_step_pips", 0.0)
                fast_ma_update_cooldown_raw = trailing_raw.get("fast_ma_update_cooldown_sec", 0.0)
                try:
                    activation_r_multiple = max(0.0, float(activation_r_raw))
                except (TypeError, ValueError):
                    activation_r_multiple = 0.0
                try:
                    activation_min_profit_pips = max(0.0, float(activation_min_profit_raw))
                except (TypeError, ValueError):
                    activation_min_profit_pips = 0.0
                try:
                    fast_ma_buffer_atr = max(0.0, float(fast_ma_buffer_atr_raw))
                except (TypeError, ValueError):
                    fast_ma_buffer_atr = 0.0
                try:
                    fast_ma_buffer_pips = max(0.0, float(fast_ma_buffer_pips_raw))
                except (TypeError, ValueError):
                    fast_ma_buffer_pips = 0.0
                try:
                    fast_ma_min_step_pips = max(0.0, float(fast_ma_min_step_pips_raw))
                except (TypeError, ValueError):
                    fast_ma_min_step_pips = 0.0
                try:
                    fast_ma_update_cooldown_sec = max(0.0, float(fast_ma_update_cooldown_raw))
                except (TypeError, ValueError):
                    fast_ma_update_cooldown_sec = 0.0
                breakeven_offset_pips = default_breakeven_offset_pips
                breakeven_offset_raw = trailing_raw.get("trailing_breakeven_offset_pips")
                if breakeven_offset_raw is not None:
                    try:
                        breakeven_offset_pips = float(breakeven_offset_raw)
                    except (TypeError, ValueError):
                        pass
                trailing_override.update(
                    {
                        "trailing_mode": "fast_ma",
                        "trailing_activation_r_multiple": activation_r_multiple,
                        "trailing_activation_min_profit_pips": activation_min_profit_pips,
                        "fast_ma_buffer_atr": fast_ma_buffer_atr,
                        "fast_ma_buffer_pips": fast_ma_buffer_pips,
                        "fast_ma_min_step_pips": fast_ma_min_step_pips,
                        "fast_ma_update_cooldown_sec": fast_ma_update_cooldown_sec,
                        "trailing_breakeven_offset_pips": max(0.0, breakeven_offset_pips),
                    }
                )
            else:
                distance_raw = trailing_raw.get("trailing_distance_pips")
                try:
                    distance_pips = float(distance_raw)
                except (TypeError, ValueError):
                    distance_pips = None
                if distance_pips is not None and distance_pips > 0:
                    pip_size = worker._execution_pip_size()
                    tp_distance_pips = abs(take_profit_price - entry_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
                    if tp_distance_pips > 0:
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
                                    activation_ratio = activation_pips / max(tp_distance_pips, FLOAT_COMPARISON_TOLERANCE)

                        if activation_ratio is None:
                            activation_ratio = default_activation_ratio

                        breakeven_offset_pips = default_breakeven_offset_pips
                        breakeven_offset_raw = trailing_raw.get("trailing_breakeven_offset_pips")
                        if breakeven_offset_raw is not None:
                            try:
                                breakeven_offset_pips = float(breakeven_offset_raw)
                            except (TypeError, ValueError):
                                pass

                        trailing_override.update(
                            {
                                "trailing_mode": "distance",
                                "trailing_activation_ratio": max(0.0, min(1.0, activation_ratio)),
                                "trailing_distance_pips": max(0.1, distance_pips),
                                "trailing_breakeven_offset_pips": max(0.0, breakeven_offset_pips),
                            }
                        )

        adaptive_context = self.adaptive_trailing_override_context(
            signal=signal,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
        )
        if adaptive_context:
            trailing_override.update(adaptive_context)

        return trailing_override or None

    @staticmethod
    def smoothstep01(value: float) -> float:
        clipped = clip01(value)
        return clipped * clipped * (3.0 - (2.0 * clipped))

    def adaptive_trailing_family_from_metadata(self, metadata: dict[str, object]) -> str:
        worker = self.worker
        regime = worker._metadata_text(metadata, "regime")
        indicator = worker._metadata_text(metadata, "indicator")
        representative = worker._metadata_text(metadata, "multi_representative_strategy")
        if regime == "mean_reversion":
            return "mean_reversion"
        if regime == "trend_following":
            return "trend"
        if indicator in {"bollinger_bands", "mean_reversion_bb"}:
            return "mean_reversion"
        if representative in {"mean_reversion_bb"}:
            return "mean_reversion"
        if any(key in metadata for key in ("distance_sigma", "mean_reversion_extreme_abs_zscore")):
            return "mean_reversion"
        if indicator in {
            "ma_cross",
            "momentum",
            "g1",
            "g2",
            "g2_index_pullback",
            "oil",
            "donchian_breakout",
            "mean_breakout_v2",
            "trend_following",
            "crypto_trend_following",
            "index_hybrid",
        }:
            return "trend"
        if any(
            key in metadata
            for key in (
                "price_slow_gap_atr",
                "price_ema_gap_ratio",
                "ema_gap_ratio",
                "breakout_distance_ratio",
                "confidence_breakout_to_sl_ratio",
            )
        ):
            return "trend"
        return "neutral"

    def adaptive_trailing_entry_extreme_abs_from_metadata(
        self,
        metadata: dict[str, object],
        family: str,
    ) -> float | None:
        worker = self.worker
        if family != "mean_reversion":
            return None
        for key in ("distance_sigma", "zscore", "mean_reversion_extreme_abs_zscore"):
            value = worker._metadata_number(metadata, key)
            if value is None:
                continue
            value = abs(value)
            if value > 0.0:
                return value
        return None

    def adaptive_trailing_override_context(
        self,
        *,
        signal: Signal,
        entry_price: float,
        stop_loss_price: float | None,
    ) -> dict[str, float | str]:
        worker = self.worker
        if not worker.adaptive_trailing_enabled:
            return {}
        metadata = worker._signal_metadata(signal)
        family = self.adaptive_trailing_family_from_metadata(metadata)
        context: dict[str, float | str] = {}
        if family in {"trend", "mean_reversion"}:
            context["adaptive_trailing_family"] = family

        pip_size = worker._execution_pip_size()
        initial_stop_pips: float | None = None
        if (
            stop_loss_price is not None
            and math.isfinite(stop_loss_price)
            and stop_loss_price > 0.0
            and math.isfinite(entry_price)
            and entry_price > 0.0
            and pip_size > 0.0
        ):
            initial_stop_pips = abs(entry_price - stop_loss_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        elif signal.stop_loss_pips > 0.0:
            initial_stop_pips = float(signal.stop_loss_pips)
        if initial_stop_pips is not None and initial_stop_pips > 0.0:
            context["adaptive_trailing_initial_stop_pips"] = initial_stop_pips

        entry_extreme_abs = self.adaptive_trailing_entry_extreme_abs_from_metadata(metadata, family)
        if entry_extreme_abs is not None and entry_extreme_abs > 0.0:
            context["adaptive_trailing_entry_extreme_abs"] = entry_extreme_abs
        return context

    def adaptive_trailing_trend_heat(
        self,
        *,
        position: Position,
        metadata: dict[str, object],
        progress: float,
    ) -> float:
        worker = self.worker
        heat_scores: list[float] = []
        zscore = worker._metadata_number(metadata, "zscore")
        zscore_threshold = worker._metadata_number(metadata, "zscore_effective_threshold")
        if zscore_threshold is None or zscore_threshold <= 0.0:
            zscore_threshold = worker._metadata_number(metadata, "zscore_threshold")
        if zscore is not None and zscore_threshold is not None and zscore_threshold > 0.0:
            directional_z = zscore if position.side == Side.BUY else -zscore
            if directional_z > 0.0:
                z_ratio = directional_z / max(zscore_threshold, FLOAT_COMPARISON_TOLERANCE)
                heat_scores.append(clip01((z_ratio - 1.0) / 1.0))

        for value_key, limit_keys in (
            ("price_slow_gap_atr", ("max_price_slow_gap_atr_effective", "max_price_slow_gap_atr")),
            ("price_ema_gap_ratio", ("max_price_ema_gap_ratio",)),
            ("ema_gap_ratio", ("trend_gap_threshold_effective", "trend_gap_threshold")),
            ("breakout_distance_ratio", ("index_min_breakout_distance_ratio",)),
        ):
            value = worker._metadata_number(metadata, value_key)
            if value is None:
                continue
            limit: float | None = None
            for limit_key in limit_keys:
                limit = worker._metadata_number(metadata, limit_key)
                if limit is not None and limit > 0.0:
                    break
            if limit is None or limit <= 0.0:
                continue
            ratio = max(0.0, value) / max(limit, FLOAT_COMPARISON_TOLERANCE)
            heat_scores.append(clip01((ratio - 0.85) / 0.65))

        breakout_to_sl_ratio = worker._metadata_number(metadata, "confidence_breakout_to_sl_ratio")
        extension_soft_ratio = worker._metadata_number(metadata, "confidence_extension_soft_ratio")
        extension_hard_ratio = worker._metadata_number(metadata, "confidence_extension_hard_ratio")
        if (
            breakout_to_sl_ratio is not None
            and extension_soft_ratio is not None
            and extension_hard_ratio is not None
            and extension_hard_ratio > extension_soft_ratio
        ):
            span = max(extension_hard_ratio - extension_soft_ratio, FLOAT_COMPARISON_TOLERANCE)
            heat_scores.append(clip01((breakout_to_sl_ratio - extension_soft_ratio) / span))

        heat_scores.append(clip01((progress - 0.35) / 0.45))
        return max(heat_scores) if heat_scores else 0.0

    def adaptive_trailing_mean_reversion_heat(
        self,
        *,
        metadata: dict[str, object],
        progress: float,
        entry_extreme_abs: float,
    ) -> float:
        worker = self.worker
        heat_scores: list[float] = []
        current_abs: float | None = None
        for key in ("distance_sigma", "zscore"):
            value = worker._metadata_number(metadata, key)
            if value is None:
                continue
            current_abs = abs(value)
            break
        if entry_extreme_abs > 0.0 and current_abs is not None:
            captured = 1.0 - min(entry_extreme_abs, current_abs) / max(entry_extreme_abs, FLOAT_COMPARISON_TOLERANCE)
            heat_scores.append(clip01(captured / 0.80))
        heat_scores.append(clip01((progress - 0.20) / 0.55))
        return max(heat_scores) if heat_scores else 0.0

    def adaptive_trailing_heat(
        self,
        *,
        position: Position,
        metadata: dict[str, object],
        progress: float,
        trailing_settings: dict[str, float | str],
    ) -> float:
        family = str(trailing_settings.get("adaptive_trailing_family") or "").strip().lower()
        if family not in {"trend", "mean_reversion"}:
            family = self.adaptive_trailing_family_from_metadata(metadata)
        if family == "mean_reversion":
            entry_extreme_abs = max(
                0.0,
                float(trailing_settings.get("adaptive_trailing_entry_extreme_abs", 0.0)),
            )
            return self.adaptive_trailing_mean_reversion_heat(
                metadata=metadata,
                progress=progress,
                entry_extreme_abs=entry_extreme_abs,
            )
        if family == "trend":
            return self.adaptive_trailing_trend_heat(
                position=position,
                metadata=metadata,
                progress=progress,
            )
        return clip01((progress - 0.40) / 0.50)

    @staticmethod
    def prefer_tighter_trailing_candidate(
        position: Position,
        *candidates: float | None,
    ) -> float | None:
        valid = [candidate for candidate in candidates if candidate is not None]
        if not valid:
            return None
        if position.side == Side.BUY:
            return max(valid)
        return min(valid)

    def adaptive_trailing_candidate_stop(
        self,
        position: Position,
        bid: float,
        ask: float,
        *,
        mark: float,
        progress: float,
        signal: Signal | None,
        trailing_settings: dict[str, float | str],
    ) -> float | None:
        worker = self.worker
        if not worker.adaptive_trailing_enabled:
            return None
        if progress + FLOAT_COMPARISON_TOLERANCE < worker.adaptive_trailing_profit_lock_min_progress and progress <= 0.0:
            return None
        metadata = worker._signal_metadata(signal) if signal is not None else {}
        atr_pips = worker._metadata_number(metadata, "atr_pips")
        pip_size = worker._execution_pip_size()
        if atr_pips is None or atr_pips <= 0.0 or pip_size <= 0.0:
            return None
        heat = self.adaptive_trailing_heat(
            position=position,
            metadata=metadata,
            progress=progress,
            trailing_settings=trailing_settings,
        )
        trailing_mode = str(trailing_settings.get("trailing_mode") or "distance").strip().lower()
        runner_progress_gate = worker.protective_runner_min_tp_progress
        explicit_distance_override = str(
            trailing_settings.get("trailing_distance_override_explicit") or "false"
        ).strip().lower() == "true"
        if trailing_mode == "distance" and not explicit_distance_override:
            runner_progress_gate = worker.protective_runner_distance_min_tp_progress
        if (
            worker.protective_runner_preservation_enabled
            and str(trailing_settings.get("adaptive_trailing_family") or "").strip().lower() == "trend"
            and progress + FLOAT_COMPARISON_TOLERANCE < runner_progress_gate
        ):
            return None
        if heat + FLOAT_COMPARISON_TOLERANCE < worker.adaptive_trailing_heat_trigger:
            return None
        span = max(worker.adaptive_trailing_heat_full - worker.adaptive_trailing_heat_trigger, FLOAT_COMPARISON_TOLERANCE)
        heat_progress = clip01((heat - worker.adaptive_trailing_heat_trigger) / span)
        compression = self.smoothstep01(heat_progress)
        initial_stop_pips = max(
            0.0,
            float(trailing_settings.get("adaptive_trailing_initial_stop_pips", 0.0)),
        )
        if initial_stop_pips <= 0.0:
            initial_stop_pips = abs(position.open_price - position.stop_loss) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if initial_stop_pips <= 0.0:
            return None

        spread_pips = abs(ask - bid) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        min_distance_pips = max(
            worker.adaptive_trailing_min_distance_stop_ratio * initial_stop_pips,
            worker.adaptive_trailing_min_distance_spread_multiplier * spread_pips,
        )
        distance_factor = 1.0 - (
            (1.0 - worker.adaptive_trailing_distance_factor_at_full) * compression
        )
        compressed_distance_pips = max(
            min_distance_pips,
            atr_pips * worker.adaptive_trailing_atr_base_multiplier * distance_factor,
        )
        compressed_distance_price = compressed_distance_pips * pip_size
        profit_lock_price: float | None = None
        if (
            worker.adaptive_trailing_profit_lock_r_at_full > 0.0
            and progress + FLOAT_COMPARISON_TOLERANCE >= worker.adaptive_trailing_profit_lock_min_progress
        ):
            lock_r = worker.adaptive_trailing_profit_lock_r_at_full * compression
            lock_distance_price = initial_stop_pips * lock_r * pip_size
            if lock_distance_price > 0.0:
                if position.side == Side.BUY:
                    profit_lock_price = position.open_price + lock_distance_price
                else:
                    profit_lock_price = position.open_price - lock_distance_price

        if position.side == Side.BUY:
            desired = max(position.stop_loss, mark - compressed_distance_price)
            if profit_lock_price is not None:
                desired = max(desired, profit_lock_price)
            desired = worker._normalize_price(desired)
            guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
            if guarded is not None and guarded > position.stop_loss + FLOAT_COMPARISON_TOLERANCE:
                return guarded
            return None

        desired = min(position.stop_loss, mark + compressed_distance_price)
        if profit_lock_price is not None:
            desired = min(desired, profit_lock_price)
        desired = worker._normalize_price(desired)
        guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
        if guarded is not None and guarded < position.stop_loss - FLOAT_COMPARISON_TOLERANCE:
            return guarded
        return None

    def trailing_candidate_stop(
        self,
        position: Position,
        bid: float,
        ask: float,
        *,
        signal: Signal | None = None,
    ) -> tuple[float | None, float]:
        worker = self.worker
        mark = bid if position.side == Side.BUY else ask
        tp_distance = (
            position.take_profit - position.open_price
            if position.side == Side.BUY
            else position.open_price - position.take_profit
        )
        if tp_distance <= 0:
            return None, 0.0

        trailing_settings = self.effective_trailing_settings(position)
        trailing_activation_ratio = trailing_settings["trailing_activation_ratio"]
        trailing_distance_pips = trailing_settings["trailing_distance_pips"]
        breakeven_offset_pips = trailing_settings["trailing_breakeven_offset_pips"]
        trailing_mode = str(trailing_settings.get("trailing_mode") or "distance").strip().lower()

        moved = (
            mark - position.open_price
            if position.side == Side.BUY
            else position.open_price - mark
        )
        progress = moved / tp_distance
        runner_state = worker._runner_preservation_state(
            position,
            mark,
            trailing_settings=trailing_settings,
        )
        effective_activation_ratio = trailing_activation_ratio
        runner_progress_gate = 0.0
        if worker.protective_runner_preservation_enabled and str(runner_state["family"]) == "trend":
            runner_progress_gate = worker.protective_runner_min_tp_progress
            explicit_distance_override = str(
                trailing_settings.get("trailing_distance_override_explicit") or "false"
            ).strip().lower() == "true"
            if trailing_mode == "distance" and not explicit_distance_override:
                runner_progress_gate = worker.protective_runner_distance_min_tp_progress
            effective_activation_ratio = max(float(trailing_activation_ratio), runner_progress_gate)
        breakeven_stop = worker._breakeven_lock_candidate_stop(position, mark)
        if trailing_mode == "fast_ma":
            metadata = signal.metadata if (signal is not None and isinstance(signal.metadata, dict)) else {}
            signal_side = signal.side if signal is not None else Side.HOLD
            if signal_side in {Side.BUY, Side.SELL} and signal_side != position.side:
                adaptive_candidate = self.adaptive_trailing_candidate_stop(
                    position,
                    bid,
                    ask,
                    mark=mark,
                    progress=progress,
                    signal=signal,
                    trailing_settings=trailing_settings,
                )
                return self.prefer_tighter_trailing_candidate(
                    position,
                    breakeven_stop,
                    adaptive_candidate,
                ), progress
            fast_ma = worker._safe_float(metadata.get("fast_ma"))
            trailing_meta = metadata.get("trailing_stop")
            if fast_ma is None and isinstance(trailing_meta, dict):
                directional_keys = (
                    ("long_anchor_value", "buy_anchor_value")
                    if position.side == Side.BUY
                    else ("short_anchor_value", "sell_anchor_value")
                )
                for key in directional_keys:
                    fast_ma = worker._safe_float(trailing_meta.get(key))
                    if fast_ma is not None:
                        break
                if fast_ma is None:
                    fast_ma = worker._safe_float(trailing_meta.get("fast_ma_value"))
            if fast_ma is None:
                fast_ma = worker._safe_float(metadata.get("fast_ema"))

            candidate: float | None = None
            if fast_ma is not None:
                pip_size = worker._execution_pip_size()
                stop_distance = abs(position.open_price - position.stop_loss)
                activation_r_multiple = float(trailing_settings.get("trailing_activation_r_multiple", 0.0))
                activation_min_profit_pips = float(
                    trailing_settings.get("trailing_activation_min_profit_pips", 0.0)
                )
                activation_move = max(
                    max(0.0, activation_r_multiple) * max(0.0, stop_distance),
                    max(0.0, activation_min_profit_pips) * pip_size,
                )
                if moved + FLOAT_COMPARISON_TOLERANCE >= activation_move:
                    if progress + FLOAT_COMPARISON_TOLERANCE < runner_progress_gate:
                        candidate = None
                    else:
                        atr_pips = worker._metadata_number(metadata, "atr_pips")
                        atr_price = (max(0.0, atr_pips) * pip_size) if atr_pips is not None else 0.0
                        fast_ma_buffer_atr = max(
                            0.0, float(trailing_settings.get("fast_ma_buffer_atr", 0.0))
                        )
                        fast_ma_buffer_pips = max(
                            0.0, float(trailing_settings.get("fast_ma_buffer_pips", 0.0))
                        )
                        min_step_pips = max(
                            0.0, float(trailing_settings.get("fast_ma_min_step_pips", 0.0))
                        )
                        update_cooldown_sec = max(
                            0.0, float(trailing_settings.get("fast_ma_update_cooldown_sec", 0.0))
                        )
                        buffer_price = (fast_ma_buffer_pips * pip_size) + (fast_ma_buffer_atr * atr_price)
                        min_step_price = min_step_pips * pip_size

                        if position.side == Side.BUY:
                            desired = worker._normalize_price(max(position.stop_loss, fast_ma - buffer_price))
                            guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
                            candidate = guarded if (guarded is not None and guarded > position.stop_loss) else None
                            if (
                                candidate is not None
                                and min_step_price > 0
                                and (candidate - position.stop_loss) < (min_step_price - FLOAT_COMPARISON_TOLERANCE)
                            ):
                                candidate = None
                        else:
                            desired = worker._normalize_price(min(position.stop_loss, fast_ma + buffer_price))
                            guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
                            candidate = guarded if (guarded is not None and guarded < position.stop_loss) else None
                            if (
                                candidate is not None
                                and min_step_price > 0
                                and (position.stop_loss - candidate) < (min_step_price - FLOAT_COMPARISON_TOLERANCE)
                            ):
                                candidate = None

                        if candidate is not None and update_cooldown_sec > 0:
                            last_update = worker._position_trailing_last_update_ts.get(position.position_id, 0.0)
                            if (
                                worker._runtime_age_sec(
                                    last_update,
                                    now_monotonic=worker._monotonic_now(),
                                    now_wall=worker._wall_time_now(),
                                )
                                or 0.0
                            ) < update_cooldown_sec:
                                candidate = None

            adaptive_candidate = self.adaptive_trailing_candidate_stop(
                position,
                bid,
                ask,
                mark=mark,
                progress=progress,
                signal=signal,
                trailing_settings=trailing_settings,
            )
            return self.prefer_tighter_trailing_candidate(
                position,
                candidate,
                breakeven_stop,
                adaptive_candidate,
            ), progress

        candidate = None
        if moved > 0 and progress + FLOAT_COMPARISON_TOLERANCE >= effective_activation_ratio:
            pip_size = worker._execution_pip_size()
            trail_distance = trailing_distance_pips * pip_size
            breakeven_offset = breakeven_offset_pips * pip_size

            if position.side == Side.BUY:
                breakeven_level = position.open_price + breakeven_offset
                if mark > breakeven_level:
                    desired = max(position.stop_loss, breakeven_level, mark - trail_distance)
                    desired = worker._normalize_price(desired)
                    guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
                    candidate = guarded if (guarded is not None and guarded > position.stop_loss) else None
            else:
                breakeven_level = position.open_price - breakeven_offset
                if mark < breakeven_level:
                    desired = min(position.stop_loss, breakeven_level, mark + trail_distance)
                    desired = worker._normalize_price(desired)
                    guarded = worker._apply_broker_min_stop_distance_guard(position, desired, bid, ask)
                    candidate = guarded if (guarded is not None and guarded < position.stop_loss) else None

        adaptive_candidate = self.adaptive_trailing_candidate_stop(
            position,
            bid,
            ask,
            mark=mark,
            progress=progress,
            signal=signal,
            trailing_settings=trailing_settings,
        )
        adaptive_primary = (
            str(trailing_settings.get("trailing_distance_override_explicit") or "false").strip().lower() != "true"
            and str(trailing_settings.get("adaptive_trailing_family") or "").strip().lower()
            in {"trend", "mean_reversion"}
        )
        if adaptive_primary and adaptive_candidate is not None:
            return self.prefer_tighter_trailing_candidate(
                position,
                breakeven_stop,
                adaptive_candidate,
            ), progress
        return self.prefer_tighter_trailing_candidate(
            position,
            candidate,
            breakeven_stop,
            adaptive_candidate,
        ), progress
