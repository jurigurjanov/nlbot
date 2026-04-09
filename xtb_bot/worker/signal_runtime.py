from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math

from xtb_bot.models import Signal, Side
from xtb_bot.multi_strategy import SyntheticExitPolicy, StrategyIntent, clip01
from xtb_bot.strategies.base import (
    HOLD_STATE_SIGNAL,
    Strategy,
    StrategyContext,
    classify_hold_reason,
)


class WorkerSignalRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def build_strategy_context(
        self,
        *,
        current_volume: float | None,
        current_spread_pips: float | None,
    ) -> StrategyContext:
        worker = self.worker
        prices, timestamps, volumes = worker._history_runtime.history_snapshot()
        return StrategyContext(
            symbol=worker.symbol,
            prices=prices,
            timestamps=timestamps,
            volumes=volumes,
            current_volume=current_volume,
            current_spread_pips=current_spread_pips,
            tick_size=worker.symbol_spec.tick_size if worker.symbol_spec is not None else None,
            pip_size=worker._context_pip_size(),
        )

    def build_strategy_context_for_strategy(
        self,
        strategy: Strategy,
        *,
        base_context: StrategyContext,
    ) -> StrategyContext:
        worker = self.worker
        candle_context = worker._build_candle_history_context(
            strategy=strategy,
            base_context=base_context,
        )
        if candle_context is not None:
            return candle_context
        return base_context

    @staticmethod
    def signal_target_qty_lots(signal: Signal) -> float:
        if signal.side == Side.BUY:
            return 1.0
        if signal.side == Side.SELL:
            return -1.0
        return 0.0

    @staticmethod
    def context_last_price(context: StrategyContext) -> float | None:
        if not context.prices:
            return None
        try:
            last_price = float(context.prices[-1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(last_price) or last_price <= 0.0:
            return None
        return last_price

    @staticmethod
    def metadata_trailing_distance_pips(metadata: dict[str, object]) -> float | None:
        trailing = metadata.get("trailing_stop")
        if not isinstance(trailing, dict):
            return None
        for key in ("distance_pips", "trail_distance_pips", "trailing_distance_pips"):
            raw = trailing.get(key)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0.0:
                return value
        return None

    @staticmethod
    def synthetic_exit_reason_for_intent(
        intent: StrategyIntent | None,
        *,
        mark_price: float | None,
    ) -> str | None:
        if intent is None:
            return None
        if mark_price is None or not math.isfinite(mark_price) or mark_price <= 0.0:
            return None
        qty = float(intent.target_qty_lots)
        if abs(qty) <= FLOAT_ROUNDING_TOLERANCE:
            return None
        policy = intent.synthetic_exit
        if policy is None:
            return None
        stop = policy.stop_loss_price
        take = policy.take_profit_price
        if qty > 0.0:
            if stop is not None and mark_price <= float(stop):
                return "synthetic_exit_stop_loss"
            if take is not None and mark_price >= float(take):
                return "synthetic_exit_take_profit"
        else:
            if stop is not None and mark_price >= float(stop):
                return "synthetic_exit_stop_loss"
            if take is not None and mark_price <= float(take):
                return "synthetic_exit_take_profit"
        return None

    def synthetic_exit_policy_from_signal(
        self,
        *,
        signal: Signal,
        reference_price: float | None,
    ) -> SyntheticExitPolicy | None:
        worker = self.worker
        if signal.side not in {Side.BUY, Side.SELL}:
            return None
        if reference_price is None or not math.isfinite(reference_price) or reference_price <= 0.0:
            return None
        pip_size = worker._context_pip_size()
        if pip_size <= 0.0 or not math.isfinite(pip_size):
            return None

        stop_loss_price: float | None = None
        take_profit_price: float | None = None
        stop_pips = max(0.0, float(signal.stop_loss_pips))
        take_pips = max(0.0, float(signal.take_profit_pips))
        if signal.side == Side.BUY:
            if stop_pips > 0.0:
                stop_loss_price = float(reference_price) - (stop_pips * pip_size)
            if take_pips > 0.0:
                take_profit_price = float(reference_price) + (take_pips * pip_size)
        else:
            if stop_pips > 0.0:
                stop_loss_price = float(reference_price) + (stop_pips * pip_size)
            if take_pips > 0.0:
                take_profit_price = float(reference_price) - (take_pips * pip_size)

        trailing_distance_pips = self.metadata_trailing_distance_pips(worker._signal_metadata(signal))
        if (
            stop_loss_price is None
            and take_profit_price is None
            and trailing_distance_pips is None
        ):
            return None
        return SyntheticExitPolicy(
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            trailing_distance_pips=trailing_distance_pips,
        )

    def current_real_position_virtual_lots(self) -> float:
        worker = self.worker
        active = worker.position_book.get(worker.symbol)
        if active is None:
            return 0.0
        return 1.0 if active.side == Side.BUY else -1.0

    @staticmethod
    def confidence_value(signal: Signal) -> float:
        try:
            return clip01(float(signal.confidence))
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def signal_metadata(signal: Signal) -> dict[str, object]:
        if isinstance(signal.metadata, dict):
            return signal.metadata
        return {}

    @classmethod
    def signal_hold_state(cls, signal: Signal) -> str:
        if signal.side != Side.HOLD:
            return HOLD_STATE_SIGNAL
        metadata = cls.signal_metadata(signal)
        return classify_hold_reason(metadata.get("reason"), metadata)

    def generate_signal(
        self,
        *,
        context: StrategyContext,
        real_position_lots: float,
    ) -> Signal:
        worker = self.worker
        if worker._multi_strategy_enabled:
            return worker._build_multi_strategy_signal(
                context=context,
                real_position_lots=real_position_lots,
            )
        strategy_context = self.build_strategy_context_for_strategy(
            worker.strategy,
            base_context=context,
        )
        return worker.strategy.generate_signal(strategy_context)

    def strategy_exit_signal(self) -> Signal | None:
        worker = self.worker
        current_volume = worker._history_runtime.latest_history_volume()
        context = self.build_strategy_context(
            current_volume=current_volume,
            current_spread_pips=None,
        )
        effective_context = self.build_strategy_context_for_strategy(
            worker.strategy,
            base_context=context,
        )
        if len(effective_context.prices) < max(
            1,
            int(getattr(worker.strategy, "min_history", 1) or 1),
        ):
            return None
        return self.generate_signal(
            context=effective_context,
            real_position_lots=self.current_real_position_virtual_lots(),
        )
