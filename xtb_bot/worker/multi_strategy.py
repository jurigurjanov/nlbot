from __future__ import annotations

from xtb_bot.tolerances import (
    FLOAT_COMPARISON_TOLERANCE,
    FLOAT_ROUNDING_TOLERANCE,
    float_gt,
    float_is_close,
    float_lt,
    float_lte,
)

from dataclasses import dataclass, field
import logging
import math
import time
from typing import Any

from xtb_bot.config import DEFAULT_MULTI_STRATEGY_COMPONENT_NAMES, resolve_strategy_param
from xtb_bot.models import Position, Signal, Side
from xtb_bot.multi_strategy import (
    AggregatorConfig,
    NetIntentDecision,
    RebalanceMode,
    RollingPercentileNormalizer,
    StrategyIntent,
    VirtualPositionAggregator,
    allocate_partial_fill_seniority_first,
    clip01,
    debug_decision_payload,
)
from xtb_bot.strategies.base import (
    HOLD_STATE_BLOCKED,
    HOLD_STATE_METADATA_KEY,
    HOLD_STATE_NEUTRAL,
    HOLD_STATE_SIGNAL,
    HOLD_STATE_UNAVAILABLE,
    Strategy,
    StrategyContext,
    classify_hold_reason,
)


logger = logging.getLogger("xtb_bot.worker")

_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"
_MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO = 0.58
_MULTI_STRATEGY_SECONDARY_BREAKOUT_CONFIRM_RATIO = 0.35
_MULTI_STRATEGY_REGIME_BOOST_MULTIPLIER = 1.5
_MULTI_STRATEGY_REGIME_PENALTY_MULTIPLIER = 0.2
_MULTI_STRATEGY_REGIME_HARD_MIN_POWER = 1.25
_MULTI_STRATEGY_REGIME_HARD_DOMINANCE_RATIO = 1.35
_MULTI_STRATEGY_REPRESENTATIVE_METADATA_KEYS: tuple[str, ...] = (
    "fast_ma",
    "slow_ma",
    "fast_ema",
    "slow_ema",
    "atr_pips",
    "atr_window",
    "timeframe_sec",
    "regime",
    "zscore",
    "zscore_prev",
    "zscore_threshold",
    "zscore_effective_threshold",
    "distance_sigma",
    "mean_reversion_extreme_abs_zscore",
    "price_slow_gap_atr",
    "max_price_slow_gap_atr",
    "max_price_slow_gap_atr_effective",
    "price_ema_gap_ratio",
    "max_price_ema_gap_ratio",
    "ema_gap_ratio",
    "trend_gap_threshold",
    "trend_gap_threshold_effective",
    "breakout_distance_ratio",
    "breakout_distance_pips",
    "index_min_breakout_distance_ratio",
    "confidence_breakout_to_sl_ratio",
    "confidence_extension_soft_ratio",
    "confidence_extension_hard_ratio",
    "entry_quality_family",
    "entry_quality_status",
    "entry_quality_penalty",
    "entry_quality_confidence_before",
    "entry_quality_confidence_after",
    "entry_quality_reasons",
    "entry_quality_path_efficiency",
    "entry_quality_aligned_path_share",
    "entry_quality_opposite_path_share",
    "entry_quality_last_move_share",
    "entry_quality_reversal_share",
    "entry_quality_last_move_in_signal_direction",
    "index_trend_session_open_delay_minutes",
    "minutes_since_trend_session_open",
    "trend_session_open_delay_active",
    "trend_session_open_delay_remaining_minutes",
)
_MULTI_STRATEGY_TREND_COMPONENTS: tuple[str, ...] = (
    "momentum",
    "momentum_fx",
    "momentum_index",
    "g1",
    "trend_following",
    "crypto_trend_following",
    "mean_breakout_v2",
    "donchian_breakout",
)
_MULTI_STRATEGY_MEAN_REVERSION_COMPONENTS: tuple[str, ...] = (
    "mean_reversion_bb",
)


@dataclass(slots=True)
class MultiStrategyRuntimeState:
    enabled: bool = False
    components: list[tuple[str, Strategy]] = field(default_factory=list)
    weight_by_name: dict[str, float] = field(default_factory=dict)
    family_weight_by_asset_class: dict[str, dict[str, float]] = field(default_factory=dict)
    secondary_weight_by_name: dict[str, float] = field(default_factory=dict)
    seniority_by_name: dict[str, float] = field(default_factory=dict)
    intent_seq: dict[str, int] = field(default_factory=dict)
    intent_ttl_sec: float = 5.0
    aggregator: VirtualPositionAggregator | None = None
    last_decision: NetIntentDecision | None = None
    last_signal_by_name: dict[str, Signal] = field(default_factory=dict)
    last_runtime_health_payload: dict[str, object] = field(default_factory=dict)
    last_emergency_event_ts: float = 0.0
    last_emergency_signature: str | None = None
    scale_out_offset_by_name: dict[str, float] = field(default_factory=dict)
    # Per-asset-class soft overlap threshold (overrides _MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO).
    soft_overlap_hold_ratio: float = _MULTI_STRATEGY_SOFT_OVERLAP_HOLD_RATIO
    family_netting_enabled: bool = False
    family_map_config: dict[str, str] = field(default_factory=dict)


class WorkerMultiStrategyRuntime:
    def __init__(self, worker: Any) -> None:
        self._worker = worker
        self._state = MultiStrategyRuntimeState()

    @property
    def state(self) -> MultiStrategyRuntimeState:
        return self._state

    def resolve_multi_strategy_names(self, strategy_params: dict[str, object]) -> list[str]:
        worker = self._worker
        raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_names",
            "",
            mode=worker.mode,
        )
        names = worker._parse_strategy_names(raw)
        if not names:
            default_raw = resolve_strategy_param(
                strategy_params,
                worker.strategy_name,
                "multi_strategy_default_names",
                ",".join(DEFAULT_MULTI_STRATEGY_COMPONENT_NAMES),
                mode=worker.mode,
            )
            names = worker._parse_strategy_names(default_raw)
            if names:
                logger.info(
                    "Multi-strategy names not set, using defaults | symbol=%s base_strategy=%s names=%s",
                    worker.symbol,
                    worker.strategy_name,
                    ",".join(names),
                )
        if not names:
            return []
        base_component = worker._base_strategy_label()
        names = [name for name in names if name != _MULTI_STRATEGY_CARRIER_NAME]
        if base_component and base_component not in names:
            names.insert(0, base_component)
        return names

    def initialize_runtime(
        self,
        *,
        strategy_params: dict[str, object],
        runtime_strategy_params: dict[str, object],
    ) -> None:
        worker = self._worker
        state = self._state
        enabled = worker._strategy_bool_param(strategy_params, "multi_strategy_enabled", True)
        if not enabled:
            return

        names = self.resolve_multi_strategy_names(strategy_params)
        if not names:
            logger.warning(
                "Multi-strategy enabled but no strategy names configured | symbol=%s base_strategy=%s",
                worker.symbol,
                worker.strategy_name,
            )
            return

        weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_weights",
            {},
            mode=worker.mode,
        )
        seniority_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_seniority",
            {},
            mode=worker.mode,
        )
        secondary_weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_secondary_weights",
            {},
            mode=worker.mode,
        )
        index_weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_index_weights",
            {},
            mode=worker.mode,
        )
        fx_weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_fx_weights",
            {},
            mode=worker.mode,
        )
        commodity_weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_commodity_weights",
            {},
            mode=worker.mode,
        )
        crypto_weight_raw = resolve_strategy_param(
            strategy_params,
            worker.strategy_name,
            "multi_strategy_crypto_weights",
            {},
            mode=worker.mode,
        )
        state.weight_by_name = worker._parse_weight_map(weight_raw)
        state.seniority_by_name = worker._parse_weight_map(seniority_raw)
        state.secondary_weight_by_name = worker._parse_weight_map(secondary_weight_raw)
        state.family_weight_by_asset_class = {
            "index": worker._parse_weight_map(index_weight_raw),
            "fx": worker._parse_weight_map(fx_weight_raw),
            "commodity": worker._parse_weight_map(commodity_weight_raw),
            "crypto": worker._parse_weight_map(crypto_weight_raw),
        }

        components: list[tuple[str, Strategy]] = []
        seen: set[str] = set()
        for name in names:
            normalized = str(name or "").strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            runtime_name = worker._multi_strategy_component_runtime_name(normalized)
            if normalized != worker._base_strategy_label():
                allowed_symbols = worker._strategy_symbols_scope.get(normalized)
                if allowed_symbols is not None and worker.symbol.upper() not in allowed_symbols:
                    logger.info(
                        "Skipping multi-strategy component by configured symbol scope | symbol=%s strategy=%s",
                        worker.symbol,
                        normalized,
                    )
                    continue
            if (
                (not worker._multi_strategy_carrier_enabled)
                and normalized == worker.strategy_name.lower()
                and runtime_name == normalized
            ):
                strategy = worker.strategy
            else:
                component_params = dict(
                    worker._strategy_params_map.get(runtime_name)
                    or worker._strategy_params_map.get(normalized)
                    or {}
                )
                component_runtime_params = dict(component_params)
                component_runtime_params.update(worker._runtime_strategy_overlay)
                try:
                    strategy = worker._create_strategy_instance(runtime_name, component_runtime_params)
                except Exception:
                    if runtime_name != normalized:
                        logger.warning(
                            "Multi-strategy runtime alias unavailable, falling back to canonical strategy | symbol=%s strategy=%s runtime_strategy=%s",
                            worker.symbol,
                            normalized,
                            runtime_name,
                            exc_info=True,
                        )
                        try:
                            strategy = worker._create_strategy_instance(normalized, component_runtime_params)
                        except Exception:
                            logger.exception(
                                "Failed to initialize multi-strategy component | symbol=%s strategy=%s runtime_strategy=%s",
                                worker.symbol,
                                normalized,
                                runtime_name,
                            )
                            continue
                    else:
                        logger.exception(
                            "Failed to initialize multi-strategy component | symbol=%s strategy=%s runtime_strategy=%s",
                            worker.symbol,
                            normalized,
                            runtime_name,
                        )
                        continue
            try:
                supported = bool(strategy.supports_symbol(worker.symbol))
            except Exception:
                logger.exception(
                    "Failed to check multi-strategy symbol support | symbol=%s strategy=%s",
                    worker.symbol,
                    normalized,
                )
                continue
            if not supported:
                logger.warning(
                    "Skipping multi-strategy component that does not support symbol | symbol=%s strategy=%s",
                    worker.symbol,
                    normalized,
                )
                continue
            components.append((normalized, strategy))

        if len(components) < 2:
            logger.warning(
                "Multi-strategy disabled because fewer than two components are active | symbol=%s configured=%s active=%s",
                worker.symbol,
                ",".join(names),
                ",".join(name for name, _strategy in components) or "-",
            )
            return

        intent_ttl_sec = worker._strategy_float_param(
            strategy_params,
            "multi_strategy_intent_ttl_sec",
            max(5.0, worker.poll_interval_sec * 5.0),
        )
        state.intent_ttl_sec = max(1.0, float(intent_ttl_sec))
        lot_step_lots = worker._strategy_float_param(strategy_params, "multi_strategy_lot_step_lots", 0.1)
        min_open_lot = worker._strategy_float_param(strategy_params, "multi_strategy_min_open_lot", abs(lot_step_lots))
        deadband_lots = worker._strategy_float_param(strategy_params, "multi_strategy_deadband_lots", abs(lot_step_lots))
        min_conflict_power = worker._strategy_float_param(strategy_params, "multi_strategy_min_conflict_power", 0.05)
        conflict_ratio_low = worker._strategy_float_param(strategy_params, "multi_strategy_conflict_ratio_low", 0.75)
        conflict_ratio_high = worker._strategy_float_param(strategy_params, "multi_strategy_conflict_ratio_high", 1.18)
        intent_ttl_grace_sec = worker._strategy_float_param(strategy_params, "multi_strategy_intent_ttl_grace_sec", 0.0)
        reconciliation_epsilon_lots = worker._strategy_float_param(
            strategy_params,
            "multi_strategy_reconciliation_epsilon_lots",
            1e-6,
        )

        # Per-asset-class overrides for conflict zone and soft overlap.
        asset_class = worker._multi_strategy_symbol_asset_class()
        conflict_low_by_asset = strategy_params.get("multi_strategy_conflict_ratio_low_by_asset")
        conflict_high_by_asset = strategy_params.get("multi_strategy_conflict_ratio_high_by_asset")
        soft_overlap_by_asset = strategy_params.get("multi_strategy_soft_overlap_ratio_by_asset")
        if isinstance(conflict_low_by_asset, dict) and asset_class in conflict_low_by_asset:
            try:
                conflict_ratio_low = float(conflict_low_by_asset[asset_class])
            except (TypeError, ValueError):
                pass
        if isinstance(conflict_high_by_asset, dict) and asset_class in conflict_high_by_asset:
            try:
                conflict_ratio_high = float(conflict_high_by_asset[asset_class])
            except (TypeError, ValueError):
                pass
        if isinstance(soft_overlap_by_asset, dict) and asset_class in soft_overlap_by_asset:
            try:
                state.soft_overlap_hold_ratio = max(0.0, min(1.0, float(soft_overlap_by_asset[asset_class])))
            except (TypeError, ValueError):
                pass

        normalizer_window = max(
            8,
            int(worker._strategy_float_param(strategy_params, "multi_strategy_normalizer_window", 100)),
        )
        normalizer_min_samples = max(
            1,
            int(worker._strategy_float_param(strategy_params, "multi_strategy_normalizer_min_samples", 32)),
        )
        normalizer_default = worker._strategy_float_param(strategy_params, "multi_strategy_normalizer_default", 0.5)

        state.components = components
        state.intent_seq = {name: 0 for name, _strategy in components}
        state.scale_out_offset_by_name = {name: 0.0 for name, _strategy in components}
        state.aggregator = VirtualPositionAggregator(
            config=AggregatorConfig(
                lot_step_lots=max(1e-6, abs(float(lot_step_lots))),
                min_open_lot=max(0.0, abs(float(min_open_lot))),
                deadband_lots=max(0.0, abs(float(deadband_lots))),
                intent_ttl_grace_sec=max(0.0, float(intent_ttl_grace_sec)),
                min_conflict_power=max(0.0, float(min_conflict_power)),
                conflict_ratio_low=float(conflict_ratio_low),
                conflict_ratio_high=float(conflict_ratio_high),
                reconciliation_epsilon_lots=max(0.0, float(reconciliation_epsilon_lots)),
            ),
            normalizer=RollingPercentileNormalizer(
                window=normalizer_window,
                min_samples=normalizer_min_samples,
                default_value=clip01(float(normalizer_default)),
            ),
        )
        self.seed_multi_strategy_normalizer_from_store(
            strategy_names=[name for name, _strategy in components],
            limit=normalizer_window,
        )
        state.enabled = True
        state.family_netting_enabled = worker._strategy_bool_param(
            strategy_params, "multi_strategy_family_netting_enabled", True
        )
        family_map_raw = strategy_params.get("multi_strategy_family_map")
        if isinstance(family_map_raw, dict):
            state.family_map_config = {
                str(k).strip().lower(): str(v).strip().lower()
                for k, v in family_map_raw.items()
                if str(k).strip() and str(v).strip()
            }
        if state.family_netting_enabled:
            logger.info(
                "Multi-strategy family netting enabled | symbol=%s families=%s",
                worker.symbol,
                ",".join(sorted(set(state.family_map_config.values()) or {"trend"})),
            )
        if asset_class == "fx":
            logger.info(
                "Multi-strategy FX tuning applied | symbol=%s conflict_zone=[%.3f,%.3f] soft_overlap=%.3f components=%s",
                worker.symbol,
                float(conflict_ratio_low),
                float(conflict_ratio_high),
                state.soft_overlap_hold_ratio,
                ",".join(name for name, _ in components),
            )
        logger.info(
            (
                "Multi-strategy netting enabled | symbol=%s base=%s components=%s "
                "lot_step=%.4f min_open=%.4f deadband=%.4f epsilon=%.8f"
            ),
            worker.symbol,
            worker.strategy_name,
            ",".join(name for name, _strategy in components),
            state.aggregator.config.lot_step_lots,
            state.aggregator.config.min_open_lot,
            state.aggregator.config.deadband_lots,
            state.aggregator.config.reconciliation_epsilon_lots,
        )

    def seed_multi_strategy_normalizer_from_store(
        self,
        *,
        strategy_names: list[str],
        limit: int,
    ) -> None:
        worker = self._worker
        aggregator = self._state.aggregator
        if aggregator is None:
            return
        loader = getattr(worker.store, "load_recent_strategy_entry_confidence_samples", None)
        if not callable(loader):
            return
        max_rows = max(8, int(limit))
        seeded_total = 0
        seeded_by_strategy: dict[str, int] = {}
        for strategy_name in strategy_names:
            normalized = worker._normalize_strategy_label(strategy_name)
            if not normalized:
                continue
            try:
                samples = loader(
                    symbol=worker.symbol,
                    strategy_entry=normalized,
                    limit=max_rows,
                    mode=worker.mode.value,
                )
            except Exception:
                continue
            if not isinstance(samples, list) or not samples:
                continue
            seeded_count = 0
            for value in reversed(samples):
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(parsed):
                    continue
                aggregator.normalizer.observe(normalized, clip01(parsed))
                seeded_count += 1
            if seeded_count <= 0:
                continue
            seeded_total += seeded_count
            seeded_by_strategy[normalized] = seeded_count
        if seeded_total <= 0:
            return
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Multi-strategy normalizer warm-started from trade history",
            {
                "strategy": worker.strategy_name,
                "symbol": worker.symbol,
                "seeded_total": seeded_total,
                "seeded_by_strategy": seeded_by_strategy,
                "mode": worker.mode.value,
            },
        )

    def runtime_health(self, now_monotonic: float) -> tuple[bool, bool, dict[str, object]]:
        worker = self._worker
        latest_price_ts = (
            float(worker._last_price_freshness_ts)
            if worker._last_price_freshness_ts > 0
            else worker._history_runtime.latest_history_timestamp()
        )
        latest_price_age_sec = None
        if worker._last_price_freshness_monotonic_ts > 0:
            latest_price_age_sec = max(
                0.0,
                float(now_monotonic) - float(worker._last_price_freshness_monotonic_ts),
            )
        elif latest_price_ts is not None:
            latest_price_age_sec = max(0.0, worker._wall_time_now() - latest_price_ts)

        fresh_max_age_sec = max(1.0, float(worker.stream_max_tick_age_sec))
        if worker.db_first_reads_enabled:
            fresh_max_age_sec = min(fresh_max_age_sec, max(0.5, float(worker.db_first_tick_max_age_sec)))
        market_data_fresh = latest_price_age_sec is not None and latest_price_age_sec <= fresh_max_age_sec

        stream_status = worker._last_stream_health
        stream_healthy: bool | None = None
        stream_connected: bool | None = None
        stream_reason: str | None = None
        if worker.stream_health_check_enabled and stream_status is not None:
            stream_healthy = bool(stream_status.healthy)
            stream_connected = bool(stream_status.connected)
            stream_reason = str(stream_status.reason or "")

        connectivity_status = worker._cached_connectivity_status
        connectivity_healthy: bool | None = None
        connectivity_effective_healthy: bool | None = None
        connectivity_reason: str | None = None
        if worker.connectivity_check_enabled:
            connectivity_status = worker._refresh_connectivity_status(now_ts=now_monotonic)
            if connectivity_status is not None:
                connectivity_healthy = bool(connectivity_status.healthy)
                connectivity_effective_healthy = worker._connectivity_status_allows_trading(connectivity_status)
                connectivity_reason = str(connectivity_status.reason or "")

        heartbeat_ok = True
        if stream_healthy is not None:
            heartbeat_ok = heartbeat_ok and stream_healthy and bool(stream_connected)
        if connectivity_effective_healthy is not None:
            heartbeat_ok = heartbeat_ok and connectivity_effective_healthy

        payload: dict[str, object] = {
            "market_data_fresh": market_data_fresh,
            "heartbeat_ok": heartbeat_ok,
            "latest_price_ts": latest_price_ts,
            "latest_price_age_sec": (round(latest_price_age_sec, 3) if latest_price_age_sec is not None else None),
            "fresh_max_age_sec": round(fresh_max_age_sec, 3),
            "stream_check_enabled": worker.stream_health_check_enabled,
            "stream_healthy": stream_healthy,
            "stream_connected": stream_connected,
            "stream_reason": stream_reason,
            "connectivity_check_enabled": worker.connectivity_check_enabled,
            "connectivity_healthy": connectivity_healthy,
            "connectivity_effective_healthy": connectivity_effective_healthy,
            "connectivity_reason": connectivity_reason,
        }
        return market_data_fresh, heartbeat_ok, payload

    def maybe_record_emergency(
        self,
        decision: NetIntentDecision,
        *,
        real_position_lots: float,
        health_payload: dict[str, object],
    ) -> None:
        worker = self._worker
        state = self._state
        now = self._worker._monotonic_now()
        signature = "|".join(
            [
                str(decision.mode.value),
                str(decision.state.value),
                str(health_payload.get("stream_reason") or ""),
                str(health_payload.get("connectivity_reason") or ""),
            ]
        )
        cooldown_passed = (now - state.last_emergency_event_ts) >= worker.multi_strategy_emergency_event_cooldown_sec
        if (not cooldown_passed) and signature == state.last_emergency_signature:
            return

        manual_intervention_required = (
            abs(float(real_position_lots)) > FLOAT_COMPARISON_TOLERANCE and abs(float(decision.execution_delta_lots)) > FLOAT_COMPARISON_TOLERANCE
        )
        payload = {
            "strategy": worker.strategy_name,
            "symbol": worker.symbol,
            "real_position_lots": float(real_position_lots),
            "manual_intervention_required": manual_intervention_required,
            "health": health_payload,
            **debug_decision_payload(decision),
        }
        worker.store.record_event(
            "ERROR",
            worker.symbol,
            "Multi-strategy emergency mode active",
            payload,
        )
        logger.error(
            "Multi-strategy emergency mode active | symbol=%s strategy=%s real=%.4f target=%.4f delta=%.4f manual_intervention_required=%s",
            worker.symbol,
            worker.strategy_name,
            float(real_position_lots),
            float(decision.target_net_qty_lots),
            float(decision.execution_delta_lots),
            manual_intervention_required,
        )
        state.last_emergency_event_ts = now
        state.last_emergency_signature = signature

    def emergency_exit_reason(self, signal: Signal | None) -> str | None:
        state = self._state
        if not state.enabled:
            return None
        if state.last_decision is not None and state.last_decision.mode == RebalanceMode.EMERGENCY:
            return "multi_strategy_emergency_mode"
        if signal is None:
            return None
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        if str(metadata.get("reason") or "").strip().lower() == "multi_emergency_hold":
            return "multi_strategy_emergency_mode"
        return None

    def current_emergency_worker_error(self) -> str | None:
        state = self._state
        if not state.enabled:
            return None
        decision = state.last_decision
        if decision is None or decision.mode != RebalanceMode.EMERGENCY:
            return None
        health = state.last_runtime_health_payload if isinstance(state.last_runtime_health_payload, dict) else {}
        details: list[str] = []
        connectivity_reason = str(health.get("connectivity_reason") or "").strip()
        stream_reason = str(health.get("stream_reason") or "").strip()
        if connectivity_reason and connectivity_reason.lower() != "ok":
            details.append(f"connectivity={connectivity_reason}")
        if stream_reason and stream_reason.lower() != "ok":
            details.append(f"stream={stream_reason}")
        if not details and health.get("heartbeat_ok") is False:
            details.append("heartbeat_not_ok")
        detail_text = ",".join(details)
        if detail_text:
            return f"multi_strategy_emergency_mode:{detail_text}"
        return "multi_strategy_emergency_mode"

    def multi_component_strategy_from_metadata(
        self,
        metadata: dict[str, object],
        *,
        side: Side,
    ) -> str | None:
        worker = self._worker
        if side not in {Side.BUY, Side.SELL}:
            return None
        components_raw = metadata.get("multi_components")
        if not isinstance(components_raw, dict):
            return None
        best_name: str | None = None
        best_weight = -1.0
        best_confidence = -1.0
        for raw_name, raw_payload in components_raw.items():
            name = worker._normalize_strategy_label(raw_name)
            if not name:
                continue
            payload = raw_payload if isinstance(raw_payload, dict) else {}
            weighted_qty = worker._safe_float(payload.get("weighted_qty"))
            if weighted_qty is None:
                weighted_qty = 0.0
            payload_side = str(payload.get("side") or "").strip().lower()
            side_matches = False
            if side == Side.BUY:
                side_matches = float_gt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE) or payload_side in {"buy", "long"}
            elif side == Side.SELL:
                side_matches = float_lt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE) or payload_side in {"sell", "short"}
            if not side_matches:
                continue
            confidence = worker._safe_float(payload.get("confidence"))
            if confidence is None:
                confidence = 0.0
            weight = abs(float(weighted_qty))
            if float_lt(weight, best_weight, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                continue
            if float_is_close(weight, best_weight, rel_tol=0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE) and float_lte(
                confidence,
                best_confidence,
                abs_tol=FLOAT_ROUNDING_TOLERANCE,
            ):
                continue
            best_name = name
            best_weight = weight
            best_confidence = confidence
        return best_name

    def multi_component_support_metrics(
        self,
        metadata: dict[str, object],
        *,
        side: Side,
        dominant_component: str | None = None,
    ) -> dict[str, float | str | None]:
        worker = self._worker
        if side not in {Side.BUY, Side.SELL}:
            return {}
        components_raw = metadata.get("multi_components")
        if not isinstance(components_raw, dict):
            return {}
        dominant_name = worker._normalize_strategy_label(dominant_component)
        dominant_weight = 0.0
        same_side_weight = 0.0
        non_dominant_weight = 0.0
        secondary_breakout_support_weight = 0.0
        for raw_name, raw_payload in components_raw.items():
            name = worker._normalize_strategy_label(raw_name)
            if not name:
                continue
            payload = raw_payload if isinstance(raw_payload, dict) else {}
            weighted_qty = worker._safe_float(payload.get("weighted_qty"))
            if weighted_qty is None:
                weighted_qty = 0.0
            payload_side = str(payload.get("side") or "").strip().lower()
            if side == Side.BUY:
                side_matches = float_gt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE) or payload_side in {"buy", "long"}
            else:
                side_matches = float_lt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE) or payload_side in {"sell", "short"}
            if not side_matches:
                continue
            weight = abs(float(weighted_qty))
            same_side_weight += weight
            if dominant_name and name == dominant_name:
                dominant_weight += weight
                continue
            non_dominant_weight += weight
            if name != "donchian_breakout":
                secondary_breakout_support_weight += weight
        support_ratio = (
            secondary_breakout_support_weight / max(dominant_weight, FLOAT_COMPARISON_TOLERANCE)
            if dominant_weight > FLOAT_COMPARISON_TOLERANCE
            else None
        )
        return {
            "dominant_component": dominant_name or None,
            "dominant_weight": dominant_weight,
            "same_side_weight": same_side_weight,
            "non_dominant_weight": non_dominant_weight,
            "secondary_breakout_support_weight": secondary_breakout_support_weight,
            "secondary_breakout_support_ratio": support_ratio,
        }

    def select_risk_signal(
        self,
        side: Side,
        decision: NetIntentDecision,
        signal_by_name: dict[str, Signal],
        *,
        current_spread_pips: float | None = None,
    ) -> tuple[str | None, Signal | None, dict[str, object]]:
        worker = self._worker
        if side not in {Side.BUY, Side.SELL}:
            return None, None, {}
        best_name: str | None = None
        best_signal: Signal | None = None
        best_payload: dict[str, object] = {}
        best_score = -1.0
        best_reward_to_spread = -1.0
        best_weighted_qty = -1.0
        for item in decision.intents:
            signal = signal_by_name.get(item.intent.strategy_id)
            if signal is None or signal.side != side:
                continue
            weighted_qty = abs(float(item.weighted_qty))
            metrics = worker._signal_execution_metrics(
                signal,
                current_spread_pips=current_spread_pips,
            )
            penalty, reasons = worker._execution_quality_penalty_from_metrics(metrics)
            score = weighted_qty * max(0.05, 1.0 - penalty)
            reward_to_spread_ratio = float(metrics.get("reward_to_spread_ratio") or 0.0)
            if float_lt(score, best_score, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                continue
            if float_is_close(score, best_score, rel_tol=0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                if float_lt(reward_to_spread_ratio, best_reward_to_spread, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                    continue
                if (
                    float_is_close(
                        reward_to_spread_ratio,
                        best_reward_to_spread,
                        rel_tol=0.0,
                        abs_tol=FLOAT_ROUNDING_TOLERANCE,
                    )
                    and float_lte(weighted_qty, best_weighted_qty, abs_tol=FLOAT_ROUNDING_TOLERANCE)
                ):
                    continue
            best_score = score
            best_reward_to_spread = reward_to_spread_ratio
            best_weighted_qty = weighted_qty
            best_name = item.intent.strategy_id
            best_signal = signal
            best_payload = {
                "strategy": item.intent.strategy_id,
                "raw_weighted_qty": weighted_qty,
                "score": score,
                "penalty": penalty,
                "order_type_hint": metrics.get("order_type_hint"),
                "spread_to_stop_ratio": metrics.get("spread_to_stop_ratio"),
                "spread_to_take_profit_ratio": metrics.get("spread_to_take_profit_ratio"),
                "reward_to_spread_ratio": metrics.get("reward_to_spread_ratio"),
                "reasons": reasons,
            }
        return best_name, best_signal, best_payload

    def component_family(
        self,
        strategy_name: str,
        signal: Signal | None,
    ) -> str:
        worker = self._worker
        metadata = worker._signal_metadata(signal) if signal is not None else {}
        inferred = worker._adaptive_trailing_family_from_metadata(metadata)
        if inferred in {"trend", "mean_reversion"}:
            return inferred

        normalized = worker._normalize_strategy_label(strategy_name)
        configured = self._state.family_map_config.get(normalized)
        if configured:
            return configured
        if normalized in _MULTI_STRATEGY_MEAN_REVERSION_COMPONENTS:
            return "mean_reversion"
        if normalized in _MULTI_STRATEGY_TREND_COMPONENTS:
            return "trend"
        return "neutral"

    @staticmethod
    def directional_summary(decision: NetIntentDecision) -> dict[str, float | int]:
        buy_count = 0
        sell_count = 0
        hold_count = 0
        for item in decision.intents:
            weighted_qty = float(item.weighted_qty)
            if float_gt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                buy_count += 1
            elif float_lt(weighted_qty, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE):
                sell_count += 1
            else:
                hold_count += 1
        dominant_power = max(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
        opposing_power = min(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
        if float_lte(dominant_power, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE):
            power_ratio = 0.0
        elif float_lte(opposing_power, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE):
            power_ratio = float("inf")
        else:
            power_ratio = dominant_power / max(opposing_power, FLOAT_ROUNDING_TOLERANCE)
        total_power = dominant_power + opposing_power
        balance_ratio = (
            0.0
            if float_lte(total_power, 0.0, abs_tol=FLOAT_ROUNDING_TOLERANCE)
            else (float(decision.buy_power) - float(decision.sell_power)) / max(total_power, FLOAT_ROUNDING_TOLERANCE)
        )
        return {
            "buy_component_count": buy_count,
            "sell_component_count": sell_count,
            "hold_component_count": hold_count,
            "directional_component_count": buy_count + sell_count,
            "dominant_power": dominant_power,
            "opposing_power": opposing_power,
            "power_ratio": power_ratio,
            "balance_ratio": balance_ratio,
        }

    def component_hold_reason_summary(
        self,
        signal_by_name: dict[str, Signal],
    ) -> dict[str, object]:
        worker = self._worker
        reason_counts: dict[str, int] = {}
        state_counts: dict[str, int] = {
            HOLD_STATE_NEUTRAL: 0,
            HOLD_STATE_BLOCKED: 0,
            HOLD_STATE_UNAVAILABLE: 0,
        }
        reason_counts_by_state: dict[str, dict[str, int]] = {
            HOLD_STATE_NEUTRAL: {},
            HOLD_STATE_BLOCKED: {},
            HOLD_STATE_UNAVAILABLE: {},
        }
        per_strategy_parts: list[str] = []
        per_strategy_parts_by_state: dict[str, list[str]] = {
            HOLD_STATE_NEUTRAL: [],
            HOLD_STATE_BLOCKED: [],
            HOLD_STATE_UNAVAILABLE: [],
        }
        soft_reason_counts: dict[str, int] = {}
        soft_reason_counts_by_strategy: dict[str, dict[str, int]] = {}
        for strategy_name, signal in signal_by_name.items():
            normalized_strategy_name = worker._normalize_strategy_label(strategy_name)
            if signal.side != Side.HOLD:
                metadata = worker._signal_metadata(signal)
                soft_reasons_raw = metadata.get("soft_filter_reasons")
                if isinstance(soft_reasons_raw, (list, tuple)):
                    strategy_soft_counts = soft_reason_counts_by_strategy.setdefault(normalized_strategy_name, {})
                    for raw_reason in soft_reasons_raw:
                        soft_reason = str(raw_reason or "").strip().lower()
                        if not soft_reason:
                            continue
                        soft_reason_counts[soft_reason] = soft_reason_counts.get(soft_reason, 0) + 1
                        strategy_soft_counts[soft_reason] = strategy_soft_counts.get(soft_reason, 0) + 1
                continue
            metadata = worker._signal_metadata(signal)
            reason = str(metadata.get("reason") or "hold").strip().lower() or "hold"
            hold_state = worker._signal_hold_state(signal)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            state_counts[hold_state] = state_counts.get(hold_state, 0) + 1
            state_reason_counts = reason_counts_by_state.setdefault(hold_state, {})
            state_reason_counts[reason] = state_reason_counts.get(reason, 0) + 1
            per_strategy_parts.append(f"{normalized_strategy_name}={reason}")
            per_strategy_parts_by_state.setdefault(hold_state, []).append(f"{normalized_strategy_name}={reason}")

            soft_reasons_raw = metadata.get("soft_filter_reasons")
            if isinstance(soft_reasons_raw, (list, tuple)):
                strategy_soft_counts = soft_reason_counts_by_strategy.setdefault(normalized_strategy_name, {})
                for raw_reason in soft_reasons_raw:
                    soft_reason = str(raw_reason or "").strip().lower()
                    if not soft_reason:
                        continue
                    soft_reason_counts[soft_reason] = soft_reason_counts.get(soft_reason, 0) + 1
                    strategy_soft_counts[soft_reason] = strategy_soft_counts.get(soft_reason, 0) + 1

        ranked = sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
        top_reasons_by_state = {
            state: ", ".join(
                f"{reason}({count})"
                for reason, count in sorted(
                    reason_counts_by_state.get(state, {}).items(),
                    key=lambda item: (-item[1], item[0]),
                )[:3]
            )
            for state in (HOLD_STATE_NEUTRAL, HOLD_STATE_BLOCKED, HOLD_STATE_UNAVAILABLE)
        }
        soft_ranked = sorted(soft_reason_counts.items(), key=lambda item: (-item[1], item[0]))
        soft_by_strategy_parts: list[str] = []
        for strategy_name, strategy_soft_counts in sorted(
            soft_reason_counts_by_strategy.items(),
            key=lambda item: item[0],
        ):
            for soft_reason, _count in sorted(
                strategy_soft_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[:3]:
                soft_by_strategy_parts.append(f"{strategy_name}={soft_reason}")

        def _state_parts_text(state_name: str) -> str:
            return "; ".join(per_strategy_parts_by_state.get(state_name, [])[:6])

        return {
            "count": len(reason_counts),
            "neutral_count": int(state_counts.get(HOLD_STATE_NEUTRAL, 0)),
            "blocked_count": int(state_counts.get(HOLD_STATE_BLOCKED, 0)),
            "unavailable_count": int(state_counts.get(HOLD_STATE_UNAVAILABLE, 0)),
            "top_reasons_text": ", ".join(f"{reason}({count})" for reason, count in ranked[:3]),
            "top_neutral_reasons_text": top_reasons_by_state[HOLD_STATE_NEUTRAL],
            "top_blocked_reasons_text": top_reasons_by_state[HOLD_STATE_BLOCKED],
            "top_unavailable_reasons_text": top_reasons_by_state[HOLD_STATE_UNAVAILABLE],
            "top_soft_reasons_text": ", ".join(f"{reason}({count})" for reason, count in soft_ranked[:3]),
            "by_strategy_text": "; ".join(per_strategy_parts[:6]),
            "by_strategy_neutral_text": _state_parts_text(HOLD_STATE_NEUTRAL),
            "by_strategy_blocked_text": _state_parts_text(HOLD_STATE_BLOCKED),
            "by_strategy_unavailable_text": _state_parts_text(HOLD_STATE_UNAVAILABLE),
            "soft_count": len(soft_reason_counts),
            "soft_by_strategy_text": "; ".join(soft_by_strategy_parts[:6]),
        }

    def hold_reason(
        self,
        *,
        decision: NetIntentDecision,
        soft_overlap_detected: bool,
        real_position_lots: float,
        hold_reason_summary: dict[str, object] | None = None,
    ) -> str:
        state = self._state
        if decision.mode.value == "emergency":
            return "multi_emergency_hold"
        if decision.mode.value == "risk_blocked":
            return "multi_risk_blocked_hold"
        if decision.mode.value == "degraded":
            return "multi_degraded_hold"
        if soft_overlap_detected:
            return "multi_overlap_hold"
        if decision.conflict_detected:
            return "multi_conflict_hold"

        summary = self.directional_summary(decision)
        directional_count = int(summary["directional_component_count"])
        dominant_power = float(summary["dominant_power"])
        requested_target = abs(float(decision.requested_net_qty_lots))
        real_position_abs = abs(float(real_position_lots))
        neutral_count = int((hold_reason_summary or {}).get("neutral_count") or 0)
        blocked_count = int((hold_reason_summary or {}).get("blocked_count") or 0)
        unavailable_count = int((hold_reason_summary or {}).get("unavailable_count") or 0)
        min_directional_power = max(
            0.05,
            min(
                0.25,
                float(
                    state.aggregator.config.min_conflict_power
                    if state.aggregator
                    else 0.0
                ),
            ),
        )

        if directional_count <= 0:
            if unavailable_count > 0 and neutral_count <= 0 and blocked_count <= 0:
                return "multi_components_unavailable"
            if neutral_count > 0 and blocked_count <= 0:
                return "multi_no_setup"
            if blocked_count > 0 and neutral_count <= 0 and unavailable_count <= 0:
                return "multi_blocked_flat"
            return "multi_all_hold"
        if dominant_power < (min_directional_power - FLOAT_COMPARISON_TOLERANCE):
            return "multi_underpowered_flat"
        if requested_target <= FLOAT_COMPARISON_TOLERANCE and real_position_abs <= FLOAT_COMPARISON_TOLERANCE:
            return "multi_balanced_flat"
        return "multi_net_flat"

    @staticmethod
    def confidence(
        *,
        side: Side,
        target_qty_lots: float,
        decision: NetIntentDecision,
        representative: Signal | None,
    ) -> tuple[float, dict[str, float]]:
        if side not in {Side.BUY, Side.SELL}:
            return 0.0, {
                "net_qty": 0.0,
                "representative": 0.0,
                "support_ratio": 0.0,
                "dominant_power": 0.0,
            }
        dominant_power = max(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
        opposing_power = min(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
        support_ratio = 1.0
        if dominant_power > FLOAT_ROUNDING_TOLERANCE:
            support_ratio = clip01(1.0 - ((opposing_power / max(dominant_power, FLOAT_ROUNDING_TOLERANCE)) * 0.5))
        net_confidence = clip01(abs(float(target_qty_lots)))
        representative_confidence = clip01(float(representative.confidence)) if representative is not None else 0.0
        dominant_power_confidence = clip01(dominant_power)
        confidence = representative_confidence
        return confidence, {
            "net_qty": net_confidence,
            "representative": representative_confidence,
            "support_ratio": support_ratio,
            "dominant_power": dominant_power_confidence,
        }

    def regime_multipliers(
        self,
        *,
        intents: list[StrategyIntent],
        signal_by_name: dict[str, Signal],
    ) -> tuple[dict[tuple[str, str], float], dict[str, object]]:
        worker = self._worker
        trend_power = 0.0
        mean_reversion_power = 0.0
        family_by_strategy: dict[str, str] = {}

        for raw_strategy_name, signal in signal_by_name.items():
            strategy_name = worker._normalize_strategy_label(raw_strategy_name)
            if not strategy_name:
                continue
            hold_state = worker._signal_hold_state(signal)
            if signal.side == Side.HOLD and hold_state in {HOLD_STATE_NEUTRAL, HOLD_STATE_UNAVAILABLE}:
                continue
            metadata = worker._signal_metadata(signal)
            family = self.component_family(strategy_name, signal)
            family_by_strategy[strategy_name] = family

            confidence = worker._confidence_value(signal)
            directional_power = max(0.25, confidence)
            if family == "trend":
                trend_power += directional_power
            elif family == "mean_reversion":
                mean_reversion_power += directional_power

            regime_hint = worker._metadata_text(metadata, "regime")
            if regime_hint in {"trend", "trend_following"}:
                trend_power += 0.75
            elif regime_hint in {"mean_reversion", "mean"}:
                mean_reversion_power += 0.75

            trend_regime = worker._metadata_bool(metadata, "trend_regime")
            mean_regime = worker._metadata_bool(metadata, "mean_reversion_regime")
            if trend_regime is True and mean_regime is False:
                trend_power += 1.0
            elif mean_regime is True and trend_regime is False:
                mean_reversion_power += 1.0

            trend_regime_strict = worker._metadata_bool(metadata, "trend_regime_strict")
            mean_regime_strict = worker._metadata_bool(metadata, "mean_reversion_regime_strict")
            if trend_regime_strict is True and mean_regime_strict is not True:
                trend_power += 0.85
            elif mean_regime_strict is True and trend_regime_strict is not True:
                mean_reversion_power += 0.85

            if any(
                worker._metadata_bool(metadata, key) is True
                for key in ("breakout_up", "breakout_down", "recent_breakout_up", "recent_breakout_down")
            ):
                trend_power += 0.35

            mean_extreme = worker._metadata_number(metadata, "mean_reversion_extreme_abs_zscore")
            if mean_extreme is not None and abs(mean_extreme) >= 1.0:
                mean_reversion_power += min(1.0, abs(mean_extreme) / 4.0) * 0.45

            sigma = worker._metadata_number(metadata, "distance_sigma")
            if sigma is not None and abs(sigma) >= 1.0:
                mean_reversion_power += min(1.0, abs(sigma) / 4.0) * 0.30

        mode = "neutral"
        if (
            trend_power >= _MULTI_STRATEGY_REGIME_HARD_MIN_POWER
            and trend_power >= mean_reversion_power * _MULTI_STRATEGY_REGIME_HARD_DOMINANCE_RATIO
        ):
            mode = "hard_trend"
        elif (
            mean_reversion_power >= _MULTI_STRATEGY_REGIME_HARD_MIN_POWER
            and mean_reversion_power >= trend_power * _MULTI_STRATEGY_REGIME_HARD_DOMINANCE_RATIO
        ):
            mode = "hard_mean_reversion"

        multiplier_by_strategy: dict[str, float] = {}
        multipliers: dict[tuple[str, str], float] = {}
        for intent in intents:
            strategy_name = worker._normalize_strategy_label(intent.strategy_id)
            family = family_by_strategy.get(strategy_name, "neutral")
            multiplier = 1.0
            if mode == "hard_trend":
                if family == "trend":
                    multiplier = _MULTI_STRATEGY_REGIME_BOOST_MULTIPLIER
                elif family == "mean_reversion":
                    multiplier = _MULTI_STRATEGY_REGIME_PENALTY_MULTIPLIER
            elif mode == "hard_mean_reversion":
                if family == "mean_reversion":
                    multiplier = _MULTI_STRATEGY_REGIME_BOOST_MULTIPLIER
                elif family == "trend":
                    multiplier = _MULTI_STRATEGY_REGIME_PENALTY_MULTIPLIER
            multiplier_by_strategy[strategy_name] = float(multiplier)
            multipliers[(strategy_name, intent.regime_tag)] = float(multiplier)

        total_power = trend_power + mean_reversion_power
        balance = (
            0.0
            if total_power <= FLOAT_COMPARISON_TOLERANCE
            else (trend_power - mean_reversion_power) / max(total_power, FLOAT_COMPARISON_TOLERANCE)
        )
        payload: dict[str, object] = {
            "mode": mode,
            "trend_power": trend_power,
            "mean_reversion_power": mean_reversion_power,
            "balance": balance,
            "family_by_strategy": family_by_strategy,
            "multiplier_by_strategy": multiplier_by_strategy,
        }
        return multipliers, payload

    def apply_scale_out_offset(
        self,
        strategy_name: str,
        target_qty_lots: float,
    ) -> float:
        offset = float(self._state.scale_out_offset_by_name.get(strategy_name, 0.0))
        if abs(offset) <= FLOAT_COMPARISON_TOLERANCE:
            return float(target_qty_lots)
        target = float(target_qty_lots)
        if abs(target) <= FLOAT_COMPARISON_TOLERANCE or (target * offset) <= 0.0:
            self._state.scale_out_offset_by_name[strategy_name] = 0.0
            return target
        if target > 0:
            adjusted = max(0.0, target - offset)
        else:
            adjusted = min(0.0, target - offset)
        consumed = abs(target - adjusted)
        remaining_abs = max(0.0, abs(offset) - consumed)
        self._state.scale_out_offset_by_name[strategy_name] = (
            remaining_abs if offset > 0 else -remaining_abs
        )
        return adjusted

    def apply_scale_out_allocation(
        self,
        position: Position,
        *,
        closed_volume: float,
    ) -> None:
        worker = self._worker
        state = self._state
        if not state.enabled or state.last_decision is None:
            return
        if closed_volume <= 0:
            return
        decision = state.last_decision
        lot_step = (
            state.aggregator.config.lot_step_lots
            if state.aggregator is not None
            else 0.1
        )
        side_sign = 1.0 if position.side == Side.BUY else -1.0
        requested_by_strategy: dict[str, float] = {}
        for item in decision.intents:
            weighted_qty = float(item.weighted_qty)
            if weighted_qty * side_sign <= FLOAT_ROUNDING_TOLERANCE:
                continue
            requested_by_strategy[item.intent.strategy_id] = (
                -abs(weighted_qty) if side_sign > 0 else abs(weighted_qty)
            )
        if not requested_by_strategy:
            return
        fill_qty = -abs(closed_volume) if side_sign > 0 else abs(closed_volume)
        allocation = allocate_partial_fill_seniority_first(
            fill_qty_lots=fill_qty,
            requested_deltas_lots=requested_by_strategy,
            lot_step_lots=lot_step,
            strategy_seniority=state.seniority_by_name,
        )
        payload_allocations: dict[str, float] = {}
        for strategy_name, allocated_delta in allocation.allocations_by_strategy.items():
            allocated = float(allocated_delta)
            if side_sign > 0 and allocated < 0:
                offset_add = abs(allocated)
            elif side_sign < 0 and allocated > 0:
                offset_add = -abs(allocated)
            else:
                continue
            state.scale_out_offset_by_name[strategy_name] = (
                float(state.scale_out_offset_by_name.get(strategy_name, 0.0))
                + offset_add
            )
            payload_allocations[strategy_name] = offset_add
        if payload_allocations:
            worker.store.record_event(
                "INFO",
                worker.symbol,
                "Multi-strategy scale-out allocation applied",
                {
                    "position_id": position.position_id,
                    "closed_volume": closed_volume,
                    "allocation_offsets": payload_allocations,
                    "allocation_mode": "seniority_first",
                    "net_mode": str(decision.mode.value),
                },
            )

    def build_signal(
        self,
        *,
        context: StrategyContext,
        real_position_lots: float,
    ) -> Signal:
        worker = self._worker
        state = self._state
        if not state.enabled or state.aggregator is None:
            return worker.strategy.generate_signal(context)

        now = worker._monotonic_now()
        current_price = worker._context_last_price(context)
        existing_intents = {
            intent.strategy_id: intent
            for intent in state.aggregator.current_intents(worker.symbol)
        }
        signal_by_name: dict[str, Signal] = {}
        for strategy_name, strategy in state.components:
            try:
                strategy_context = worker._build_strategy_context_for_strategy(
                    strategy,
                    base_context=context,
                )
                signal = strategy.generate_signal(strategy_context)
            except Exception as exc:
                logger.exception(
                    "Multi-strategy component failed, using HOLD fallback | symbol=%s component=%s",
                    worker.symbol,
                    strategy_name,
                )
                signal = Signal(
                    side=Side.HOLD,
                    confidence=0.0,
                    stop_loss_pips=worker.min_stop_loss_pips,
                    take_profit_pips=max(worker.min_stop_loss_pips * worker.min_tp_sl_ratio, worker.min_stop_loss_pips),
                    metadata={
                        "indicator": "multi_strategy_component_error",
                        "reason": "component_error",
                        "component": strategy_name,
                        "error": str(exc),
                    },
                )
            signal_by_name[strategy_name] = signal
            seq = state.intent_seq.get(strategy_name, 0) + 1
            state.intent_seq[strategy_name] = seq
            metadata = worker._signal_metadata(signal)
            regime_tag = str(
                metadata.get("regime_tag")
                or metadata.get("regime")
                or metadata.get("trend_session_profile")
                or ""
            ).strip().lower()
            reason_code = str(metadata.get("reason") or "").strip().lower()
            target_qty_lots = worker._signal_target_qty_lots(signal)
            synthetic_exit = worker._synthetic_exit_policy_from_signal(
                signal=signal,
                reference_price=current_price,
            )
            previous_intent = existing_intents.get(strategy_name)
            synthetic_exit_reason = worker._synthetic_exit_reason_for_intent(
                previous_intent,
                mark_price=current_price,
            )
            if (
                synthetic_exit_reason is not None
                and previous_intent is not None
                and (
                    abs(target_qty_lots) <= FLOAT_ROUNDING_TOLERANCE
                    or (target_qty_lots * float(previous_intent.target_qty_lots)) > 0.0
                )
            ):
                target_qty_lots = 0.0
                synthetic_exit = None
                reason_code = synthetic_exit_reason
            target_qty_lots = self.apply_scale_out_offset(
                strategy_name,
                target_qty_lots,
            )
            intent = StrategyIntent.new(
                strategy_id=strategy_name,
                symbol=worker.symbol,
                seq=seq,
                market_data_seq=max(0, int(worker.iteration)),
                target_qty_lots=target_qty_lots,
                max_abs_qty_lots=1.0,
                confidence_raw=worker._confidence_value(signal),
                regime_tag=regime_tag,
                intent_weight=1.0,
                can_flip=True,
                synthetic_exit=synthetic_exit,
                reason_code=reason_code,
                stop_loss_pips=max(0.0, float(signal.stop_loss_pips)),
                take_profit_pips=max(0.0, float(signal.take_profit_pips)),
                ttl_sec=state.intent_ttl_sec,
                created_ts=now,
            )
            state.aggregator.upsert_intent(intent, now_ts=now)

        market_data_fresh, heartbeat_ok, health_payload = self.runtime_health(now)
        base_strategy_name = worker._base_strategy_label()
        runtime_strategy_weights = dict(state.weight_by_name)
        fill_quality_by_strategy: dict[str, dict[str, object]] = {}
        asset_class = worker._multi_strategy_symbol_asset_class()
        family_weight_map = dict(state.family_weight_by_asset_class.get(asset_class) or {})
        family_weight_payload: dict[str, float] = {}
        for strategy_name, _strategy in state.components:
            if strategy_name != base_strategy_name:
                runtime_strategy_weights[strategy_name] = (
                    float(runtime_strategy_weights.get(strategy_name, 1.0))
                    * float(state.secondary_weight_by_name.get(strategy_name, 1.0))
                )
            family_weight_multiplier = float(family_weight_map.get(strategy_name, 1.0))
            runtime_strategy_weights[strategy_name] = (
                float(runtime_strategy_weights.get(strategy_name, 1.0)) * family_weight_multiplier
            )
            family_weight_payload[strategy_name] = family_weight_multiplier
            multiplier, summary, pressure_ratio = worker._fill_quality_weight_multiplier(
                strategy_name,
                current_spread_pips=context.current_spread_pips,
            )
            runtime_strategy_weights[strategy_name] = float(runtime_strategy_weights.get(strategy_name, 1.0)) * multiplier
            fill_quality_by_strategy[strategy_name] = {
                "weight_multiplier": multiplier,
                "sample_count": int(summary.get("sample_count") or 0),
                "avg_total_adverse_slippage_pips": float(summary.get("avg_total_adverse_slippage_pips") or 0.0),
                "adverse_trade_share": float(summary.get("adverse_trade_share") or 0.0),
                "pressure_ratio": pressure_ratio,
            }
        active_intents = state.aggregator.current_intents(worker.symbol)
        regime_multipliers, regime_payload = self.regime_multipliers(
            intents=active_intents,
            signal_by_name=signal_by_name,
        )
        multiplier_by_strategy_raw = regime_payload.get("multiplier_by_strategy")
        multiplier_by_strategy = (
            dict(multiplier_by_strategy_raw) if isinstance(multiplier_by_strategy_raw, dict) else {}
        )
        family_by_strategy_raw = regime_payload.get("family_by_strategy")
        family_by_strategy = (
            dict(family_by_strategy_raw) if isinstance(family_by_strategy_raw, dict) else {}
        )
        family_map: dict[str, str] | None = None
        if state.family_netting_enabled:
            family_map = {}
            for strategy_name, _strategy in state.components:
                sig = signal_by_name.get(strategy_name)
                family_map[strategy_name] = self.component_family(strategy_name, sig)
        decision = state.aggregator.compute_net_intent(
            symbol=worker.symbol,
            real_position_lots=float(real_position_lots),
            strategy_seniority=state.seniority_by_name,
            strategy_weights=runtime_strategy_weights,
            regime_multipliers=(regime_multipliers if regime_multipliers else None),
            now_ts=now,
            market_data_fresh=market_data_fresh,
            heartbeat_ok=heartbeat_ok,
            risk_blocked=False,
            reconcile_error=False,
            family_map=family_map,
        )
        state.last_decision = decision
        state.last_signal_by_name = signal_by_name
        state.last_runtime_health_payload = dict(health_payload)
        if decision.mode == RebalanceMode.EMERGENCY:
            self.maybe_record_emergency(
                decision,
                real_position_lots=float(real_position_lots),
                health_payload=health_payload,
            )
        try:
            worker.store.upsert_real_position_cache(
                symbol=worker.symbol,
                real_qty_lots=float(real_position_lots),
                broker_ts=worker._history_runtime.latest_history_timestamp(),
                source="position_book",
            )
            weighted_qty_by_strategy = {
                item.intent.strategy_id: float(item.weighted_qty)
                for item in decision.intents
            }
            reason_by_strategy = {
                item.intent.strategy_id: (item.intent.reason_code or None)
                for item in decision.intents
            }
            for strategy_name, _strategy in state.components:
                worker.store.upsert_virtual_position_state(
                    symbol=worker.symbol,
                    strategy_id=strategy_name,
                    qty_lots=float(weighted_qty_by_strategy.get(strategy_name, 0.0)),
                    source="multi_strategy_aggregator",
                    reason=reason_by_strategy.get(strategy_name),
                )
            worker.store.upsert_system_error_qty(
                symbol=worker.symbol,
                err_qty_lots=0.0,
                reason=None,
            )
        except Exception:
            logger.exception(
                "Failed to persist multi-strategy hot/cold position state | symbol=%s strategy=%s",
                worker.symbol,
                worker.strategy_name,
            )

        lot_step = state.aggregator.config.lot_step_lots
        threshold = max(lot_step * 0.5, state.aggregator.config.min_open_lot * 0.5, 1e-6)
        target_qty = float(decision.target_net_qty_lots)
        if target_qty > threshold:
            net_side = Side.BUY
        elif target_qty < -threshold:
            net_side = Side.SELL
        else:
            net_side = Side.HOLD

        side = net_side
        soft_overlap_ratio: float | None = None
        soft_overlap_detected = False
        effective_soft_overlap_threshold = state.soft_overlap_hold_ratio
        family_netting_active = bool(state.family_netting_enabled and decision.family_decisions)
        if (
            not family_netting_active
            and net_side in {Side.BUY, Side.SELL}
            and abs(float(real_position_lots)) <= FLOAT_COMPARISON_TOLERANCE
            and not decision.conflict_detected
        ):
            dominant_power = max(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
            opposing_power = min(abs(float(decision.buy_power)), abs(float(decision.sell_power)))
            if dominant_power > max(FLOAT_COMPARISON_TOLERANCE, state.aggregator.config.min_conflict_power):
                soft_overlap_ratio = opposing_power / dominant_power
                if soft_overlap_ratio >= effective_soft_overlap_threshold:
                    side = Side.HOLD
                    soft_overlap_detected = True

        representative_name, representative, representative_payload = self.select_risk_signal(
            net_side,
            decision,
            signal_by_name,
            current_spread_pips=context.current_spread_pips,
        )
        if representative is None:
            representative = signal_by_name.get(base_strategy_name) or next(iter(signal_by_name.values()), None)
            representative_name = base_strategy_name
            representative_payload = {}
        if representative is None:
            fallback = worker.strategy.generate_signal(context)
            representative = fallback
            representative_name = base_strategy_name
            representative_payload = {}
        representative_metadata = worker._signal_metadata(representative)
        confidence, confidence_payload = self.confidence(
            side=side,
            target_qty_lots=target_qty,
            decision=decision,
            representative=representative,
        )

        metadata: dict[str, object] = {
            "indicator": "multi_strategy_netting",
            "reason": "multi_net_signal" if side != Side.HOLD else "multi_net_flat",
            "strategy": worker.strategy_name,
            "multi_market_data_fresh": market_data_fresh,
            "multi_heartbeat_ok": heartbeat_ok,
            "multi_runtime_health": health_payload,
            "multi_asset_class": asset_class,
            "multi_regime_mode": regime_payload.get("mode"),
            "multi_regime_balance": regime_payload.get("balance"),
            "multi_regime_trend_power": regime_payload.get("trend_power"),
            "multi_regime_mean_reversion_power": regime_payload.get("mean_reversion_power"),
            "multi_net_side": net_side.value,
            "multi_soft_overlap_detected": soft_overlap_detected,
            "multi_soft_overlap_ratio": soft_overlap_ratio,
            "multi_soft_overlap_threshold": effective_soft_overlap_threshold,
            "multi_family_netting_enabled": family_netting_active,
            **debug_decision_payload(decision),
        }
        if family_netting_active and decision.family_decisions:
            family_summary = {}
            for fd in decision.family_decisions:
                family_summary[fd.family] = {
                    "target": fd.target_net_qty_lots,
                    "buy_power": round(fd.buy_power, 6),
                    "sell_power": round(fd.sell_power, 6),
                    "conflict": fd.conflict_detected,
                    "intent_count": len(fd.intents),
                    "weighted_stop_loss_pips": fd.weighted_stop_loss_pips,
                    "weighted_take_profit_pips": fd.weighted_take_profit_pips,
                }
            metadata["multi_family_decisions"] = family_summary
        summary = self.directional_summary(decision)
        hold_reason_summary = self.component_hold_reason_summary(signal_by_name)
        metadata["multi_buy_component_count"] = int(summary["buy_component_count"])
        metadata["multi_sell_component_count"] = int(summary["sell_component_count"])
        metadata["multi_hold_component_count"] = int(summary["hold_component_count"])
        metadata["multi_neutral_component_count"] = int(hold_reason_summary.get("neutral_count") or 0)
        metadata["multi_blocked_component_count"] = int(hold_reason_summary.get("blocked_count") or 0)
        metadata["multi_unavailable_component_count"] = int(hold_reason_summary.get("unavailable_count") or 0)
        metadata["multi_active_component_count"] = max(
            0,
            int(len(signal_by_name)) - int(hold_reason_summary.get("unavailable_count") or 0),
        )
        metadata["multi_directional_component_count"] = int(summary["directional_component_count"])
        metadata["multi_dominant_power"] = float(summary["dominant_power"])
        metadata["multi_opposing_power"] = float(summary["opposing_power"])
        power_ratio = float(summary["power_ratio"])
        metadata["multi_power_ratio"] = power_ratio if math.isfinite(power_ratio) else "inf"
        metadata["multi_balance_ratio"] = float(summary["balance_ratio"])
        metadata["multi_confidence_net_qty"] = float(confidence_payload.get("net_qty") or 0.0)
        metadata["multi_confidence_representative"] = float(confidence_payload.get("representative") or 0.0)
        metadata["multi_confidence_support_ratio"] = float(confidence_payload.get("support_ratio") or 0.0)
        metadata["multi_confidence_dominant_power"] = float(confidence_payload.get("dominant_power") or 0.0)
        if family_weight_payload:
            metadata["multi_family_weight_by_strategy"] = family_weight_payload
        metadata["multi_hold_reason_count"] = int(hold_reason_summary.get("count") or 0)
        top_hold_reasons_text = str(hold_reason_summary.get("top_reasons_text") or "").strip()
        if top_hold_reasons_text:
            metadata["multi_top_hold_reasons"] = top_hold_reasons_text
        top_neutral_reasons_text = str(hold_reason_summary.get("top_neutral_reasons_text") or "").strip()
        if top_neutral_reasons_text:
            metadata["multi_top_neutral_reasons"] = top_neutral_reasons_text
        top_blocked_reasons_text = str(hold_reason_summary.get("top_blocked_reasons_text") or "").strip()
        if top_blocked_reasons_text:
            metadata["multi_top_blocked_reasons"] = top_blocked_reasons_text
        top_unavailable_reasons_text = str(hold_reason_summary.get("top_unavailable_reasons_text") or "").strip()
        if top_unavailable_reasons_text:
            metadata["multi_top_unavailable_reasons"] = top_unavailable_reasons_text
        top_soft_reasons_text = str(hold_reason_summary.get("top_soft_reasons_text") or "").strip()
        if top_soft_reasons_text:
            metadata["multi_top_soft_reasons"] = top_soft_reasons_text
        hold_reason_by_strategy_text = str(hold_reason_summary.get("by_strategy_text") or "").strip()
        if hold_reason_by_strategy_text:
            metadata["multi_hold_reason_by_strategy"] = hold_reason_by_strategy_text
        neutral_reason_by_strategy_text = str(hold_reason_summary.get("by_strategy_neutral_text") or "").strip()
        if neutral_reason_by_strategy_text:
            metadata["multi_neutral_reason_by_strategy"] = neutral_reason_by_strategy_text
        blocked_reason_by_strategy_text = str(hold_reason_summary.get("by_strategy_blocked_text") or "").strip()
        if blocked_reason_by_strategy_text:
            metadata["multi_blocked_reason_by_strategy"] = blocked_reason_by_strategy_text
        unavailable_reason_by_strategy_text = str(
            hold_reason_summary.get("by_strategy_unavailable_text") or ""
        ).strip()
        if unavailable_reason_by_strategy_text:
            metadata["multi_unavailable_reason_by_strategy"] = unavailable_reason_by_strategy_text
        soft_reason_by_strategy_text = str(hold_reason_summary.get("soft_by_strategy_text") or "").strip()
        if soft_reason_by_strategy_text:
            metadata["multi_soft_reason_by_strategy"] = soft_reason_by_strategy_text
        if soft_overlap_ratio is not None:
            metadata["multi_soft_overlap_ratio"] = soft_overlap_ratio
        if representative_name and (
            side != Side.HOLD
            or int(summary["directional_component_count"]) > 0
            or representative.side != Side.HOLD
        ):
            metadata["multi_representative_strategy"] = representative_name
        if representative_payload:
            metadata["multi_representative_selection_score"] = representative_payload.get("score")
            metadata["multi_representative_penalty"] = representative_payload.get("penalty")
            metadata["multi_representative_order_type_hint"] = representative_payload.get("order_type_hint")
            metadata["multi_representative_spread_to_stop_ratio"] = representative_payload.get("spread_to_stop_ratio")
            metadata["multi_representative_spread_to_take_profit_ratio"] = representative_payload.get("spread_to_take_profit_ratio")
            metadata["multi_representative_reward_to_spread_ratio"] = representative_payload.get("reward_to_spread_ratio")
            if representative_payload.get("reasons"):
                metadata["multi_representative_reasons"] = representative_payload.get("reasons")
        if "confidence_threshold_cap" in representative_metadata:
            metadata["confidence_threshold_cap"] = representative_metadata.get("confidence_threshold_cap")
        trailing_meta = representative_metadata.get("trailing_stop")
        if isinstance(trailing_meta, dict):
            metadata["trailing_stop"] = dict(trailing_meta)
        for key in _MULTI_STRATEGY_REPRESENTATIVE_METADATA_KEYS:
            if key in representative_metadata:
                metadata[key] = representative_metadata[key]
        if side == Side.HOLD:
            metadata["reason"] = self.hold_reason(
                decision=decision,
                soft_overlap_detected=soft_overlap_detected,
                real_position_lots=float(real_position_lots),
                hold_reason_summary=hold_reason_summary,
            )
            metadata[HOLD_STATE_METADATA_KEY] = classify_hold_reason(metadata.get("reason"), metadata)

        components: dict[str, dict[str, object]] = {}
        for item in decision.intents:
            per_signal = signal_by_name.get(item.intent.strategy_id)
            per_meta = worker._signal_metadata(per_signal) if per_signal is not None else {}
            fill_quality = fill_quality_by_strategy.get(item.intent.strategy_id, {})
            components[item.intent.strategy_id] = {
                "side": per_signal.side.value if per_signal is not None else "hold",
                "hold_state": (
                    worker._signal_hold_state(per_signal)
                    if per_signal is not None
                    else HOLD_STATE_UNAVAILABLE
                ),
                "confidence": per_signal.confidence if per_signal is not None else 0.0,
                "trend_signal": (
                    per_meta.get("base_trend_signal")
                    if per_meta.get("base_trend_signal")
                    else per_meta.get("trend_signal")
                ),
                "target_qty_lots": item.intent.target_qty_lots,
                "confidence_norm": item.confidence_norm,
                "effective_weight": item.effective_weight,
                "configured_weight": float(state.weight_by_name.get(item.intent.strategy_id, 1.0)),
                "secondary_weight_multiplier": (
                    1.0
                    if item.intent.strategy_id == base_strategy_name
                    else float(state.secondary_weight_by_name.get(item.intent.strategy_id, 1.0))
                ),
                "family_weight_multiplier": float(family_weight_payload.get(item.intent.strategy_id, 1.0)),
                "regime_tag": item.intent.regime_tag,
                "regime_family": family_by_strategy.get(item.intent.strategy_id, "neutral"),
                "regime_multiplier": float(multiplier_by_strategy.get(item.intent.strategy_id, 1.0)),
                "weighted_qty": item.weighted_qty,
                "reason": per_meta.get("reason"),
                "intent_reason": item.intent.reason_code,
                "fill_quality_weight_multiplier": fill_quality.get("weight_multiplier"),
                "fill_quality_sample_count": fill_quality.get("sample_count"),
                "fill_quality_avg_total_adverse_slippage_pips": fill_quality.get("avg_total_adverse_slippage_pips"),
                "fill_quality_adverse_trade_share": fill_quality.get("adverse_trade_share"),
                "fill_quality_pressure_ratio": fill_quality.get("pressure_ratio"),
            }
        metadata["multi_components"] = components
        metadata["multi_component_count"] = len(components)
        dominant_component_name = (
            self.multi_component_strategy_from_metadata(metadata, side=net_side)
            if net_side in {Side.BUY, Side.SELL}
            else None
        )
        if dominant_component_name:
            metadata["multi_dominant_component_strategy"] = dominant_component_name
        if (
            side in {Side.BUY, Side.SELL}
            and abs(float(real_position_lots)) <= FLOAT_COMPARISON_TOLERANCE
            and dominant_component_name == "donchian_breakout"
            and base_strategy_name != "donchian_breakout"
        ):
            support_metrics = self.multi_component_support_metrics(
                metadata,
                side=side,
                dominant_component=dominant_component_name,
            )
            metadata["multi_secondary_breakout_support"] = support_metrics
            support_ratio = worker._safe_float(support_metrics.get("secondary_breakout_support_ratio"))
            if support_ratio is None or support_ratio < _MULTI_STRATEGY_SECONDARY_BREAKOUT_CONFIRM_RATIO:
                side = Side.HOLD
                confidence = 0.0
                metadata["reason"] = "multi_secondary_breakout_unconfirmed"
        if side in {Side.BUY, Side.SELL}:
            metadata["multi_entry_strategy"] = base_strategy_name
            metadata["multi_entry_strategy_component"] = dominant_component_name or base_strategy_name
            component_payload = components.get(dominant_component_name or "")
            if isinstance(component_payload, dict):
                dominant_signal = str(component_payload.get("trend_signal") or "").strip().lower()
                if dominant_signal:
                    metadata["multi_entry_strategy_signal"] = dominant_signal

        weighted_stop_loss_pips = worker._coerce_positive_finite(decision.weighted_stop_loss_pips)
        weighted_take_profit_pips = worker._coerce_positive_finite(decision.weighted_take_profit_pips)
        if weighted_stop_loss_pips is not None:
            metadata["multi_weighted_stop_loss_pips"] = weighted_stop_loss_pips
        if weighted_take_profit_pips is not None:
            metadata["multi_weighted_take_profit_pips"] = weighted_take_profit_pips
        metadata["multi_weighted_stop_coverage"] = float(decision.weighted_stop_coverage)
        metadata["multi_weighted_take_profit_coverage"] = float(decision.weighted_take_profit_coverage)

        signal_stop_loss_pips = float(representative.stop_loss_pips)
        signal_take_profit_pips = float(representative.take_profit_pips)
        if side in {Side.BUY, Side.SELL}:
            if weighted_stop_loss_pips is not None:
                signal_stop_loss_pips = weighted_stop_loss_pips
            if weighted_take_profit_pips is not None:
                signal_take_profit_pips = weighted_take_profit_pips

        return Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=signal_stop_loss_pips,
            take_profit_pips=signal_take_profit_pips,
            metadata=metadata,
        )
