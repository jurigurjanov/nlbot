from __future__ import annotations

from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, zscore
from xtb_bot.strategies.pip_size import pip_size as _pip_size_lookup
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanBreakoutStrategy(Strategy):
    name = "mean_breakout"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.window = max(2, int(params.get("mean_breakout_window", 30)))
        self.breakout_window = max(2, int(params.get("mean_breakout_breakout_window", 20)))
        self.zscore_threshold = float(params.get("mean_breakout_zscore_threshold", 2.0))
        self.min_stop_loss_pips = float(
            params.get("mean_breakout_stop_loss_pips", params.get("stop_loss_pips", 30.0))
        )
        self.min_take_profit_pips = float(
            params.get("mean_breakout_take_profit_pips", params.get("take_profit_pips", 75.0))
        )
        self.atr_window = max(2, int(params.get("mean_breakout_atr_window", 14)))
        self.atr_multiplier = max(0.1, float(params.get("mean_breakout_atr_multiplier", 2.0)))
        self.risk_reward_ratio = max(1.0, float(params.get("mean_breakout_risk_reward_ratio", 2.5)))
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = self.min_take_profit_pips
        self.min_history = max(self.window, self.breakout_window) + 2

    @staticmethod
    def _zscore(values: Sequence[float]) -> float:
        return zscore(values, min_std=1e-12)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        return _pip_size_lookup(symbol)

    def _dynamic_sl_tp(self, ctx: StrategyContext, prices: Sequence[float]) -> tuple[float, float, float | None]:
        stop_pips = self.min_stop_loss_pips
        take_pips = max(self.min_take_profit_pips, stop_pips * self.risk_reward_ratio)
        atr = atr_wilder(prices, self.atr_window)
        if atr is None or atr <= 0:
            return stop_pips, take_pips, None
        pip_size = float(ctx.tick_size) if ctx.tick_size is not None and ctx.tick_size > 0 else self._pip_size(ctx.symbol)
        atr_pips = atr / max(pip_size, 1e-9)
        stop_pips = max(stop_pips, atr_pips * self.atr_multiplier)
        take_pips = max(take_pips, stop_pips * self.risk_reward_ratio)
        return stop_pips, take_pips, atr_pips

    def _hold(self, reason: str) -> Signal:
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata={"reason": reason, "indicator": "mean_breakout"},
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        prices = [float(value) for value in ctx.prices]
        if len(prices) < self.min_history:
            return self._hold("insufficient_price_history")

        z = self._zscore(prices[-self.window :])
        reference = prices[-(self.breakout_window + 1) : -1]
        channel_upper = max(reference)
        channel_lower = min(reference)
        prev = prices[-2]
        last = prices[-1]
        stop_loss_pips, take_profit_pips, atr_pips = self._dynamic_sl_tp(ctx, prices)

        breakout_up = last > channel_upper and prev <= channel_upper
        breakout_down = last < channel_lower and prev >= channel_lower

        if breakout_up and z >= self.zscore_threshold:
            breakout_strength = max(0.0, (last - channel_upper) / max(abs(last), 1e-9))
            confidence = min(
                1.0,
                abs(z) / max(self.zscore_threshold * 2.0, 1e-9) * 0.4
                + min(1.0, breakout_strength * 50.0) * 0.4
                + 0.2,
            )
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "mean_breakout",
                    "direction": "up",
                    "zscore": z,
                    "zscore_threshold": self.zscore_threshold,
                    "zscore_window": self.window,
                    "breakout_window": self.breakout_window,
                    "channel_upper": channel_upper,
                    "channel_lower": channel_lower,
                    "atr_pips": atr_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                },
            )

        if breakout_down and z <= -self.zscore_threshold:
            breakout_strength = max(0.0, (channel_lower - last) / max(abs(last), 1e-9))
            confidence = min(
                1.0,
                abs(z) / max(self.zscore_threshold * 2.0, 1e-9) * 0.4
                + min(1.0, breakout_strength * 50.0) * 0.4
                + 0.2,
            )
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "mean_breakout",
                    "direction": "down",
                    "zscore": z,
                    "zscore_threshold": self.zscore_threshold,
                    "zscore_window": self.window,
                    "breakout_window": self.breakout_window,
                    "channel_upper": channel_upper,
                    "channel_lower": channel_lower,
                    "atr_pips": atr_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                },
            )

        if not breakout_up and not breakout_down:
            return self._hold("no_breakout")
        return self._hold("zscore_not_confirmed")
