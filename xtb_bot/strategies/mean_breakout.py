from __future__ import annotations

from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import zscore
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanBreakoutStrategy(Strategy):
    name = "mean_breakout"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.window = max(2, int(params.get("mean_breakout_window", 30)))
        self.breakout_window = max(2, int(params.get("mean_breakout_breakout_window", 20)))
        self.zscore_threshold = float(params.get("mean_breakout_zscore_threshold", 2.0))
        self.stop_loss_pips = float(
            params.get("mean_breakout_stop_loss_pips", params.get("stop_loss_pips", 30.0))
        )
        self.take_profit_pips = float(
            params.get("mean_breakout_take_profit_pips", params.get("take_profit_pips", 75.0))
        )
        self.min_history = max(self.window, self.breakout_window) + 2

    @staticmethod
    def _zscore(values: Sequence[float]) -> float:
        return zscore(values, min_std=0.0)

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

        zscore = self._zscore(prices[-self.window :])
        reference = prices[-(self.breakout_window + 1) : -1]
        channel_upper = max(reference)
        channel_lower = min(reference)
        prev = prices[-2]
        last = prices[-1]

        breakout_up = last > channel_upper and prev <= channel_upper
        breakout_down = last < channel_lower and prev >= channel_lower

        if breakout_up and zscore >= self.zscore_threshold:
            breakout_strength = max(0.0, (last - channel_upper) / max(abs(last), 1e-9))
            confidence = min(
                1.0,
                abs(zscore) / max(self.zscore_threshold * 2.0, 1e-9) * 0.4
                + min(1.0, breakout_strength * 50.0) * 0.4
                + 0.2,
            )
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=self.stop_loss_pips,
                take_profit_pips=self.take_profit_pips,
                metadata={
                    "indicator": "mean_breakout",
                    "direction": "up",
                    "zscore": zscore,
                    "zscore_threshold": self.zscore_threshold,
                    "zscore_window": self.window,
                    "breakout_window": self.breakout_window,
                    "channel_upper": channel_upper,
                    "channel_lower": channel_lower,
                },
            )

        if breakout_down and zscore <= -self.zscore_threshold:
            breakout_strength = max(0.0, (channel_lower - last) / max(abs(last), 1e-9))
            confidence = min(
                1.0,
                abs(zscore) / max(self.zscore_threshold * 2.0, 1e-9) * 0.4
                + min(1.0, breakout_strength * 50.0) * 0.4
                + 0.2,
            )
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=self.stop_loss_pips,
                take_profit_pips=self.take_profit_pips,
                metadata={
                    "indicator": "mean_breakout",
                    "direction": "down",
                    "zscore": zscore,
                    "zscore_threshold": self.zscore_threshold,
                    "zscore_window": self.window,
                    "breakout_window": self.breakout_window,
                    "channel_upper": channel_upper,
                    "channel_lower": channel_lower,
                },
            )

        if not breakout_up and not breakout_down:
            return self._hold("no_breakout")
        return self._hold("zscore_not_confirmed")
