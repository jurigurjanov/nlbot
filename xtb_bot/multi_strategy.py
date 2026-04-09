from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections import deque
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from enum import Enum
import math
import threading
import time
import uuid
from typing import Any, Iterable


EPSILON = FLOAT_ROUNDING_TOLERANCE
SYSTEM_ERROR_STRATEGY_ID = "__SYSTEM_ERROR__"


class IntentRejectReason(str, Enum):
    INVALID = "invalid"
    STALE_SEQ = "stale_seq"
    EXPIRED = "expired"


class AggregatorLifecycleState(str, Enum):
    BOOTSTRAP = "bootstrap"
    LIVE_SYNCED = "live_synced"
    REBALANCING = "rebalancing"
    DEGRADED = "degraded"
    RISK_BLOCKED = "risk_blocked"
    RECONCILING = "reconciling"
    EMERGENCY = "emergency"


class RebalanceMode(str, Enum):
    NORMAL = "normal"
    CONFLICT_HOLD = "conflict_hold"
    RISK_BLOCKED = "risk_blocked"
    DEGRADED = "degraded"
    EMERGENCY = "emergency"


@dataclass(frozen=True, slots=True)
class SyntheticExitPolicy:
    stop_loss_price: float | None = None
    take_profit_price: float | None = None
    trailing_distance_pips: float | None = None
    invalidation_reason: str | None = None


@dataclass(frozen=True, slots=True)
class StrategyIntent:
    intent_id: str
    strategy_id: str
    symbol: str
    seq: int
    created_ts: float
    expires_ts: float
    market_data_seq: int
    target_qty_lots: float
    max_abs_qty_lots: float
    confidence_raw: float
    regime_tag: str
    intent_weight: float
    can_flip: bool
    synthetic_exit: SyntheticExitPolicy | None
    reason_code: str
    stop_loss_pips: float = 0.0
    take_profit_pips: float = 0.0

    @staticmethod
    def new(
        *,
        strategy_id: str,
        symbol: str,
        seq: int,
        market_data_seq: int,
        target_qty_lots: float,
        max_abs_qty_lots: float,
        confidence_raw: float,
        regime_tag: str = "",
        intent_weight: float = 1.0,
        can_flip: bool = True,
        synthetic_exit: SyntheticExitPolicy | None = None,
        reason_code: str = "",
        stop_loss_pips: float = 0.0,
        take_profit_pips: float = 0.0,
        ttl_sec: float = 15.0,
        created_ts: float | None = None,
    ) -> "StrategyIntent":
        now = time.time() if created_ts is None else float(created_ts)
        return StrategyIntent(
            intent_id=str(uuid.uuid4()),
            strategy_id=str(strategy_id).strip(),
            symbol=str(symbol).strip().upper(),
            seq=int(seq),
            created_ts=now,
            expires_ts=now + max(0.0, float(ttl_sec)),
            market_data_seq=int(market_data_seq),
            target_qty_lots=float(target_qty_lots),
            max_abs_qty_lots=max(0.0, float(max_abs_qty_lots)),
            confidence_raw=float(confidence_raw),
            regime_tag=str(regime_tag).strip().lower(),
            intent_weight=max(0.0, float(intent_weight)),
            can_flip=bool(can_flip),
            synthetic_exit=synthetic_exit,
            reason_code=str(reason_code).strip().lower(),
            stop_loss_pips=max(0.0, float(stop_loss_pips)),
            take_profit_pips=max(0.0, float(take_profit_pips)),
        )

    def is_valid(self) -> bool:
        if not self.strategy_id or not self.symbol:
            return False
        if self.seq < 0:
            return False
        if not math.isfinite(self.created_ts) or not math.isfinite(self.expires_ts):
            return False
        if (self.expires_ts - self.created_ts) <= EPSILON:
            return False
        if not math.isfinite(self.target_qty_lots):
            return False
        if not math.isfinite(self.max_abs_qty_lots) or self.max_abs_qty_lots < 0.0:
            return False
        if not math.isfinite(self.confidence_raw):
            return False
        if not math.isfinite(self.intent_weight) or self.intent_weight < 0.0:
            return False
        if not math.isfinite(self.stop_loss_pips) or self.stop_loss_pips < 0.0:
            return False
        if not math.isfinite(self.take_profit_pips) or self.take_profit_pips < 0.0:
            return False
        return True

    def clamped(self) -> "StrategyIntent":
        max_abs = max(0.0, float(self.max_abs_qty_lots))
        clamped_target = max(-max_abs, min(max_abs, float(self.target_qty_lots)))
        if abs(clamped_target - float(self.target_qty_lots)) <= EPSILON:
            return self
        return replace(self, target_qty_lots=clamped_target)


@dataclass(frozen=True, slots=True)
class IntentAcceptance:
    accepted: bool
    reason: IntentRejectReason | None
    stored_intent: StrategyIntent | None


@dataclass(frozen=True, slots=True)
class NormalizedIntent:
    intent: StrategyIntent
    confidence_norm: float
    effective_weight: float
    weighted_qty: float


@dataclass(frozen=True, slots=True)
class NetIntentDecision:
    symbol: str
    state: AggregatorLifecycleState
    mode: RebalanceMode
    reduce_only: bool
    requested_net_qty_lots: float
    target_net_qty_lots: float
    execution_delta_lots: float
    buy_power: float
    sell_power: float
    weighted_stop_loss_pips: float | None
    weighted_take_profit_pips: float | None
    weighted_stop_coverage: float
    weighted_take_profit_coverage: float
    conflict_detected: bool
    intents: tuple[NormalizedIntent, ...]


@dataclass(frozen=True, slots=True)
class FillAllocationResult:
    allocations_by_strategy: dict[str, float]
    allocated_qty_lots: float
    dust_qty_lots: float
    fill_qty_lots: float
    lot_step_lots: float


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    real_position_lots: float
    virtual_sum_lots: float
    system_error_lots: float
    expected_real_lots: float
    diff_lots: float
    requires_adjustment: bool
    system_error_adjustment_lots: float


@dataclass(frozen=True, slots=True)
class AggregatorConfig:
    lot_step_lots: float = 0.01
    min_open_lot: float = 0.01
    deadband_lots: float = 0.0
    intent_ttl_grace_sec: float = 0.0
    min_conflict_power: float = 0.0
    conflict_ratio_low: float = 0.75
    conflict_ratio_high: float = 1.18
    reconciliation_epsilon_lots: float = 1e-6


def clip01(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _coerce_positive_finite(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0.0:
        return None
    return parsed


def quantize_to_step(value: float, step: float) -> float:
    if not math.isfinite(value):
        return 0.0
    if not math.isfinite(step) or step <= 0.0:
        return value
    d_step = Decimal(str(step))
    d_value = Decimal(str(value))
    d_steps = d_value / d_step
    rounded_steps = d_steps.to_integral_value(rounding=ROUND_HALF_UP)
    return float(rounded_steps * d_step)


def floor_to_step(value: float, step: float) -> float:
    if not math.isfinite(value) or value <= 0.0:
        return 0.0
    if not math.isfinite(step) or step <= 0.0:
        return value
    d_step = Decimal(str(step))
    d_value = Decimal(str(value))
    d_steps = (d_value / d_step).to_integral_value(rounding=ROUND_FLOOR)
    return float(d_steps * d_step)


class RollingPercentileNormalizer:
    def __init__(
        self,
        *,
        window: int = 256,
        min_samples: int = 32,
        default_value: float = 0.5,
        prior_strength: float | None = None,
    ) -> None:
        self._window = max(2, int(window))
        self._min_samples = max(1, int(min_samples))
        self._default_value = clip01(float(default_value))
        if prior_strength is None:
            prior_strength = float(self._min_samples)
        self._prior_strength = max(0.0, float(prior_strength))
        self._history: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def observe(self, strategy_id: str, value: float) -> None:
        if not strategy_id:
            return
        if not math.isfinite(value):
            return
        with self._lock:
            bucket = self._history.setdefault(strategy_id, deque(maxlen=self._window))
            bucket.append(float(value))

    def normalize(self, strategy_id: str, value: float) -> float:
        if not strategy_id:
            return self._default_value
        if not math.isfinite(value):
            return self._default_value
        with self._lock:
            history = self._history.setdefault(strategy_id, deque(maxlen=self._window))
            samples = list(history)
            sample_count = len(samples)
            if sample_count <= 0:
                normalized = self._default_value
            else:
                rank = sum(1 for sample in samples if sample <= value)
                empirical = clip01(rank / max(sample_count, 1))
                if self._prior_strength <= EPSILON:
                    normalized = empirical
                else:
                    sample_weight = sample_count / (sample_count + self._prior_strength)
                    normalized = clip01((sample_weight * empirical) + ((1.0 - sample_weight) * self._default_value))
            history.append(float(value))
        return normalized

    def cleanup(self, active_strategy_ids: Iterable[str]) -> int:
        active = {str(item or "").strip() for item in active_strategy_ids}
        with self._lock:
            to_delete = [strategy_id for strategy_id in self._history if strategy_id not in active]
            for strategy_id in to_delete:
                self._history.pop(strategy_id, None)
        return len(to_delete)


class VirtualPositionAggregator:
    def __init__(
        self,
        *,
        config: AggregatorConfig | None = None,
        normalizer: RollingPercentileNormalizer | None = None,
    ) -> None:
        self.config = config or AggregatorConfig()
        self.normalizer = normalizer or RollingPercentileNormalizer()
        self._lock = threading.Lock()
        self._intents_by_key: dict[tuple[str, str], StrategyIntent] = {}
        self._last_seq_by_key: dict[tuple[str, str], int] = {}
        self._state_by_symbol: dict[str, AggregatorLifecycleState] = {}

    def _symbol_key(self, symbol: str) -> str:
        return str(symbol or "").strip().upper()

    def _intent_key(self, strategy_id: str, symbol: str) -> tuple[str, str]:
        return str(strategy_id).strip(), self._symbol_key(symbol)

    def _lots_epsilon(self) -> float:
        return max(EPSILON, float(self.config.reconciliation_epsilon_lots))

    def upsert_intent(self, intent: StrategyIntent, *, now_ts: float | None = None) -> IntentAcceptance:
        now = time.time() if now_ts is None else float(now_ts)
        if not intent.is_valid():
            return IntentAcceptance(False, IntentRejectReason.INVALID, None)
        if intent.expires_ts + self.config.intent_ttl_grace_sec < now:
            return IntentAcceptance(False, IntentRejectReason.EXPIRED, None)

        key = self._intent_key(intent.strategy_id, intent.symbol)
        with self._lock:
            last_seq = self._last_seq_by_key.get(key)
            if last_seq is not None and int(intent.seq) <= int(last_seq):
                return IntentAcceptance(False, IntentRejectReason.STALE_SEQ, None)

            stored = intent.clamped()
            self._last_seq_by_key[key] = int(stored.seq)
            self._intents_by_key[key] = stored
        return IntentAcceptance(True, None, stored)

    def expire_intents(self, *, now_ts: float | None = None) -> int:
        now = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            to_delete: list[tuple[str, str]] = []
            for key, intent in self._intents_by_key.items():
                if intent.expires_ts + self.config.intent_ttl_grace_sec < now:
                    to_delete.append(key)
            for key in to_delete:
                self._intents_by_key.pop(key, None)
        return len(to_delete)

    def _intents_for_symbol(self, symbol: str, *, now_ts: float) -> list[StrategyIntent]:
        self.expire_intents(now_ts=now_ts)
        with self._lock:
            active_strategy_ids = {intent.strategy_id for intent in self._intents_by_key.values()}
            self.normalizer.cleanup(active_strategy_ids)
            target_symbol = self._symbol_key(symbol)
            result: list[StrategyIntent] = []
            for (_strategy_id, intent_symbol), intent in self._intents_by_key.items():
                if intent_symbol != target_symbol:
                    continue
                result.append(intent)
        return result

    def _in_conflict(self, buy_power: float, sell_power: float) -> bool:
        if buy_power < self.config.min_conflict_power:
            return False
        if sell_power < self.config.min_conflict_power:
            return False
        if sell_power <= 0.0:
            return False
        ratio = buy_power / sell_power
        low = min(self.config.conflict_ratio_low, self.config.conflict_ratio_high)
        high = max(self.config.conflict_ratio_low, self.config.conflict_ratio_high)
        return low <= ratio <= high

    def _clip_reduce_only_target(self, *, real_position_lots: float, target_net_qty_lots: float) -> float:
        lots_epsilon = self._lots_epsilon()
        real = float(real_position_lots)
        target = float(target_net_qty_lots)
        if abs(real) <= lots_epsilon:
            return 0.0
        if real > 0.0:
            clipped = min(max(target, 0.0), real)
        else:
            clipped = max(min(target, 0.0), real)
        if clipped * real < 0.0:
            return 0.0
        return clipped

    def compute_net_intent(
        self,
        *,
        symbol: str,
        real_position_lots: float,
        strategy_seniority: dict[str, float] | None = None,
        strategy_weights: dict[str, float] | None = None,
        regime_multipliers: dict[tuple[str, str], float] | None = None,
        now_ts: float | None = None,
        market_data_fresh: bool = True,
        heartbeat_ok: bool = True,
        risk_blocked: bool = False,
        reconcile_error: bool = False,
    ) -> NetIntentDecision:
        now = time.time() if now_ts is None else float(now_ts)
        symbol_key = self._symbol_key(symbol)
        intents = self._intents_for_symbol(symbol_key, now_ts=now)
        normalized: list[NormalizedIntent] = []
        buy_power = 0.0
        sell_power = 0.0
        buy_abs_weight = 0.0
        sell_abs_weight = 0.0
        buy_weighted_stop_sum = 0.0
        sell_weighted_stop_sum = 0.0
        buy_weighted_take_sum = 0.0
        sell_weighted_take_sum = 0.0
        buy_stop_covered_weight = 0.0
        sell_stop_covered_weight = 0.0
        buy_take_covered_weight = 0.0
        sell_take_covered_weight = 0.0

        for intent in intents:
            conf_norm = self.normalizer.normalize(intent.strategy_id, intent.confidence_raw)
            seniority = 1.0 if strategy_seniority is None else float(strategy_seniority.get(intent.strategy_id, 1.0))
            weight = 1.0 if strategy_weights is None else float(strategy_weights.get(intent.strategy_id, 1.0))
            regime = (
                1.0
                if regime_multipliers is None
                else float(regime_multipliers.get((intent.strategy_id, intent.regime_tag), 1.0))
            )
            effective_weight = max(0.0, intent.intent_weight * max(0.0, seniority) * max(0.0, weight) * max(0.0, regime))
            weighted_qty = float(intent.target_qty_lots) * conf_norm * effective_weight
            if weighted_qty > 0:
                buy_power += weighted_qty
                buy_abs_weight += weighted_qty
                stop_pips = _coerce_positive_finite(intent.stop_loss_pips)
                if stop_pips is not None:
                    buy_weighted_stop_sum += weighted_qty * stop_pips
                    buy_stop_covered_weight += weighted_qty
                take_pips = _coerce_positive_finite(intent.take_profit_pips)
                if take_pips is not None:
                    buy_weighted_take_sum += weighted_qty * take_pips
                    buy_take_covered_weight += weighted_qty
            elif weighted_qty < 0:
                abs_weight = abs(weighted_qty)
                sell_power += abs_weight
                sell_abs_weight += abs_weight
                stop_pips = _coerce_positive_finite(intent.stop_loss_pips)
                if stop_pips is not None:
                    sell_weighted_stop_sum += abs_weight * stop_pips
                    sell_stop_covered_weight += abs_weight
                take_pips = _coerce_positive_finite(intent.take_profit_pips)
                if take_pips is not None:
                    sell_weighted_take_sum += abs_weight * take_pips
                    sell_take_covered_weight += abs_weight
            normalized.append(
                NormalizedIntent(
                    intent=intent,
                    confidence_norm=conf_norm,
                    effective_weight=effective_weight,
                    weighted_qty=weighted_qty,
                )
            )

        conflict = self._in_conflict(buy_power, sell_power)
        reduce_only = False
        mode = RebalanceMode.NORMAL
        lots_epsilon = self._lots_epsilon()
        requested_target_net = sum(item.weighted_qty for item in normalized)
        requested_target_net = quantize_to_step(requested_target_net, self.config.lot_step_lots)
        if abs(requested_target_net) < max(self.config.min_open_lot, lots_epsilon):
            requested_target_net = 0.0
        target_net = requested_target_net

        if not heartbeat_ok:
            mode = RebalanceMode.EMERGENCY
            reduce_only = True
            target_net = 0.0
        elif reconcile_error:
            mode = RebalanceMode.DEGRADED
            reduce_only = True
            target_net = 0.0
        elif risk_blocked:
            mode = RebalanceMode.RISK_BLOCKED
            reduce_only = True
            target_net = 0.0
        elif not market_data_fresh:
            mode = RebalanceMode.DEGRADED
            reduce_only = True
            target_net = 0.0
        elif conflict:
            mode = RebalanceMode.CONFLICT_HOLD
            reduce_only = True
        if reduce_only:
            target_net = self._clip_reduce_only_target(
                real_position_lots=float(real_position_lots),
                target_net_qty_lots=target_net,
            )
            if abs(target_net) > lots_epsilon:
                target_sign = 1.0 if target_net > 0.0 else -1.0
                target_net = target_sign * floor_to_step(abs(target_net), self.config.lot_step_lots)
            if abs(target_net) < max(self.config.min_open_lot, lots_epsilon):
                target_net = 0.0

        exec_delta = target_net - float(real_position_lots)
        deadband = self.config.deadband_lots if self.config.deadband_lots > 0 else self.config.lot_step_lots
        if abs(exec_delta) < max(deadband, lots_epsilon):
            exec_delta = 0.0

        weighted_stop_loss_pips: float | None = None
        weighted_take_profit_pips: float | None = None
        weighted_stop_coverage = 0.0
        weighted_take_profit_coverage = 0.0
        if target_net > lots_epsilon:
            if buy_abs_weight > lots_epsilon:
                weighted_stop_coverage = buy_stop_covered_weight / buy_abs_weight
                weighted_take_profit_coverage = buy_take_covered_weight / buy_abs_weight
            if buy_stop_covered_weight > lots_epsilon:
                weighted_stop_loss_pips = buy_weighted_stop_sum / buy_stop_covered_weight
            if buy_take_covered_weight > lots_epsilon:
                weighted_take_profit_pips = buy_weighted_take_sum / buy_take_covered_weight
        elif target_net < -lots_epsilon:
            if sell_abs_weight > lots_epsilon:
                weighted_stop_coverage = sell_stop_covered_weight / sell_abs_weight
                weighted_take_profit_coverage = sell_take_covered_weight / sell_abs_weight
            if sell_stop_covered_weight > lots_epsilon:
                weighted_stop_loss_pips = sell_weighted_stop_sum / sell_stop_covered_weight
            if sell_take_covered_weight > lots_epsilon:
                weighted_take_profit_pips = sell_weighted_take_sum / sell_take_covered_weight

        state = self._resolve_state(
            symbol=symbol_key,
            intents_available=bool(normalized),
            execution_delta_lots=exec_delta,
            market_data_fresh=market_data_fresh,
            heartbeat_ok=heartbeat_ok,
            risk_blocked=risk_blocked,
            reconcile_error=reconcile_error,
        )

        return NetIntentDecision(
            symbol=symbol_key,
            state=state,
            mode=mode,
            reduce_only=reduce_only,
            requested_net_qty_lots=requested_target_net,
            target_net_qty_lots=target_net,
            execution_delta_lots=exec_delta,
            buy_power=buy_power,
            sell_power=sell_power,
            weighted_stop_loss_pips=weighted_stop_loss_pips,
            weighted_take_profit_pips=weighted_take_profit_pips,
            weighted_stop_coverage=clip01(weighted_stop_coverage),
            weighted_take_profit_coverage=clip01(weighted_take_profit_coverage),
            conflict_detected=conflict,
            intents=tuple(normalized),
        )

    def _resolve_state(
        self,
        *,
        symbol: str,
        intents_available: bool,
        execution_delta_lots: float,
        market_data_fresh: bool,
        heartbeat_ok: bool,
        risk_blocked: bool,
        reconcile_error: bool,
    ) -> AggregatorLifecycleState:
        if not heartbeat_ok:
            state = AggregatorLifecycleState.EMERGENCY
        elif reconcile_error:
            state = AggregatorLifecycleState.RECONCILING
        elif risk_blocked:
            state = AggregatorLifecycleState.RISK_BLOCKED
        elif not market_data_fresh:
            state = AggregatorLifecycleState.DEGRADED
        elif not intents_available:
            state = AggregatorLifecycleState.BOOTSTRAP
        elif abs(execution_delta_lots) > self._lots_epsilon():
            state = AggregatorLifecycleState.REBALANCING
        else:
            state = AggregatorLifecycleState.LIVE_SYNCED
        with self._lock:
            self._state_by_symbol[symbol] = state
        return state

    def state_for_symbol(self, symbol: str) -> AggregatorLifecycleState:
        with self._lock:
            return self._state_by_symbol.get(self._symbol_key(symbol), AggregatorLifecycleState.BOOTSTRAP)

    def current_intents(self, symbol: str | None = None) -> list[StrategyIntent]:
        with self._lock:
            if symbol is None:
                return list(self._intents_by_key.values())
            target = self._symbol_key(symbol)
            return [
                intent
                for (_strategy, item_symbol), intent in self._intents_by_key.items()
                if item_symbol == target
            ]


def allocate_partial_fill_pro_rata(
    *,
    fill_qty_lots: float,
    requested_deltas_lots: dict[str, float],
    lot_step_lots: float,
) -> FillAllocationResult:
    fill_qty = float(fill_qty_lots)
    step = max(EPSILON, float(lot_step_lots))
    allocations: dict[str, float] = {str(key): 0.0 for key in requested_deltas_lots}
    if abs(fill_qty) <= EPSILON:
        return FillAllocationResult(allocations, 0.0, 0.0, fill_qty, step)

    side = 1.0 if fill_qty > 0 else -1.0
    eligible: dict[str, float] = {}
    requested_steps: dict[str, int] = {}
    for strategy_id, delta in requested_deltas_lots.items():
        signed = float(delta)
        if signed * side <= EPSILON:
            continue
        req_abs = abs(signed)
        req_stepped = floor_to_step(req_abs, step)
        if req_stepped <= EPSILON:
            continue
        eligible[str(strategy_id)] = req_stepped
        requested_steps[str(strategy_id)] = int(round(req_stepped / step))

    if not eligible:
        return FillAllocationResult(allocations, 0.0, fill_qty, fill_qty, step)

    fill_abs = abs(fill_qty)
    fill_stepped = floor_to_step(fill_abs, step)
    dust_abs = max(0.0, fill_abs - fill_stepped)
    fill_steps = int(round(fill_stepped / step))
    if fill_steps <= 0:
        return FillAllocationResult(allocations, 0.0, side * dust_abs, fill_qty, step)

    total_requested_steps = sum(requested_steps.values())
    if total_requested_steps <= 0:
        return FillAllocationResult(allocations, 0.0, fill_qty, fill_qty, step)

    base_steps: dict[str, int] = {}
    remainders: dict[str, int] = {}
    for strategy_id in sorted(eligible):
        numerator = fill_steps * requested_steps[strategy_id]
        base = numerator // total_requested_steps
        capped = min(base, requested_steps[strategy_id])
        base_steps[strategy_id] = capped
        remainders[strategy_id] = max(0, numerator - (capped * total_requested_steps))

    allocated_steps = sum(base_steps.values())
    remaining_steps = max(0, fill_steps - allocated_steps)
    if remaining_steps > 0:
        order = sorted(
            remainders.items(),
            key=lambda item: (-item[1], item[0]),
        )
        while remaining_steps > 0 and order:
            changed = False
            for strategy_id, _remainder in order:
                if remaining_steps <= 0:
                    break
                if base_steps[strategy_id] >= requested_steps[strategy_id]:
                    continue
                base_steps[strategy_id] += 1
                remaining_steps -= 1
                changed = True
            if not changed:
                break

    allocated_steps = sum(base_steps.values())
    allocated_qty_abs = allocated_steps * step
    for strategy_id, steps in base_steps.items():
        allocations[strategy_id] = side * (steps * step)

    unallocated_abs = max(0.0, fill_stepped - allocated_qty_abs)
    dust_qty = side * (dust_abs + unallocated_abs)
    return FillAllocationResult(
        allocations_by_strategy=allocations,
        allocated_qty_lots=side * allocated_qty_abs,
        dust_qty_lots=dust_qty,
        fill_qty_lots=fill_qty,
        lot_step_lots=step,
    )


def allocate_partial_fill_seniority_first(
    *,
    fill_qty_lots: float,
    requested_deltas_lots: dict[str, float],
    lot_step_lots: float,
    strategy_seniority: dict[str, float] | None = None,
) -> FillAllocationResult:
    fill_qty = float(fill_qty_lots)
    step = max(EPSILON, float(lot_step_lots))
    allocations: dict[str, float] = {str(key): 0.0 for key in requested_deltas_lots}
    if abs(fill_qty) <= EPSILON:
        return FillAllocationResult(allocations, 0.0, 0.0, fill_qty, step)

    side = 1.0 if fill_qty > 0 else -1.0
    eligible: list[tuple[str, int, float]] = []
    for strategy_id, delta in requested_deltas_lots.items():
        signed = float(delta)
        if signed * side <= EPSILON:
            continue
        req_abs = abs(signed)
        req_stepped = floor_to_step(req_abs, step)
        req_steps = int(round(req_stepped / step))
        if req_steps <= 0:
            continue
        raw_priority = (
            float(strategy_seniority.get(str(strategy_id), 1.0))
            if strategy_seniority is not None
            else 1.0
        )
        priority = raw_priority if math.isfinite(raw_priority) else 1.0
        eligible.append((str(strategy_id), req_steps, priority))

    if not eligible:
        return FillAllocationResult(allocations, 0.0, fill_qty, fill_qty, step)

    fill_abs = abs(fill_qty)
    fill_stepped = floor_to_step(fill_abs, step)
    dust_abs = max(0.0, fill_abs - fill_stepped)
    fill_steps = int(round(fill_stepped / step))
    if fill_steps <= 0:
        return FillAllocationResult(allocations, 0.0, side * dust_abs, fill_qty, step)

    order = sorted(
        eligible,
        key=lambda item: (-item[2], -item[1], item[0]),
    )
    remaining_steps = fill_steps
    allocated_steps = 0
    for strategy_id, req_steps, _priority in order:
        if remaining_steps <= 0:
            break
        assigned_steps = min(req_steps, remaining_steps)
        if assigned_steps <= 0:
            continue
        allocations[strategy_id] = side * (assigned_steps * step)
        allocated_steps += assigned_steps
        remaining_steps -= assigned_steps

    allocated_qty_abs = allocated_steps * step
    unallocated_abs = max(0.0, fill_stepped - allocated_qty_abs)
    dust_qty = side * (dust_abs + unallocated_abs)
    return FillAllocationResult(
        allocations_by_strategy=allocations,
        allocated_qty_lots=side * allocated_qty_abs,
        dust_qty_lots=dust_qty,
        fill_qty_lots=fill_qty,
        lot_step_lots=step,
    )


def reconcile_positions(
    *,
    real_position_lots: float,
    virtual_positions_lots: dict[str, float],
    system_error_lots: float = 0.0,
    epsilon_lots: float = 1e-6,
) -> ReconciliationResult:
    real = float(real_position_lots)
    system_error = float(system_error_lots)
    virtual_sum = sum(float(value) for value in virtual_positions_lots.values())
    expected_real = virtual_sum + system_error
    diff = real - expected_real
    requires_adjustment = abs(diff) > max(0.0, float(epsilon_lots))
    return ReconciliationResult(
        real_position_lots=real,
        virtual_sum_lots=virtual_sum,
        system_error_lots=system_error,
        expected_real_lots=expected_real,
        diff_lots=diff,
        requires_adjustment=requires_adjustment,
        system_error_adjustment_lots=diff if requires_adjustment else 0.0,
    )


def compute_slippage_cost(*, reference_price: float, fill_price: float, signed_qty_lots: float) -> float:
    if not (math.isfinite(reference_price) and math.isfinite(fill_price) and math.isfinite(signed_qty_lots)):
        return 0.0
    return float(signed_qty_lots) * (float(reference_price) - float(fill_price))


def allocate_cost_pro_rata(notional_by_strategy: dict[str, float], total_cost: float) -> dict[str, float]:
    if not math.isfinite(total_cost):
        return {str(key): 0.0 for key in notional_by_strategy}
    absolute_weights: dict[str, float] = {}
    for strategy_id, notional in notional_by_strategy.items():
        weight = abs(float(notional))
        if not math.isfinite(weight) or weight <= 0:
            continue
        absolute_weights[str(strategy_id)] = weight

    allocations: dict[str, float] = {str(key): 0.0 for key in notional_by_strategy}
    total_weight = sum(absolute_weights.values())
    if total_weight <= EPSILON:
        return allocations

    running = 0.0
    ordered_ids = sorted(absolute_weights.keys())
    for index, strategy_id in enumerate(ordered_ids):
        if index == len(ordered_ids) - 1:
            value = float(total_cost) - running
        else:
            value = float(total_cost) * (absolute_weights[strategy_id] / total_weight)
            running += value
        allocations[strategy_id] = value
    return allocations


def debug_decision_payload(decision: NetIntentDecision) -> dict[str, Any]:
    return {
        "symbol": decision.symbol,
        "state": decision.state.value,
        "mode": decision.mode.value,
        "reduce_only": decision.reduce_only,
        "requested_net_qty_lots": decision.requested_net_qty_lots,
        "target_net_qty_lots": decision.target_net_qty_lots,
        "execution_delta_lots": decision.execution_delta_lots,
        "buy_power": decision.buy_power,
        "sell_power": decision.sell_power,
        "weighted_stop_loss_pips": decision.weighted_stop_loss_pips,
        "weighted_take_profit_pips": decision.weighted_take_profit_pips,
        "weighted_stop_coverage": decision.weighted_stop_coverage,
        "weighted_take_profit_coverage": decision.weighted_take_profit_coverage,
        "conflict_detected": decision.conflict_detected,
        "intent_count": len(decision.intents),
    }
