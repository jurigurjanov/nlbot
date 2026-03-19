from __future__ import annotations

from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, tail_mean, zscore
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanReversionStrategy(Strategy):
    name = "mean_reversion"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.window = max(
            5,
            int(
                params.get(
                    "mean_reversion_zscore_window",
                    params.get("zscore_window", 20),
                )
            ),
        )
        self.threshold = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_zscore_threshold",
                    params.get("zscore_threshold", 1.8),
                )
            ),
        )
        self.exit_zscore = max(
            0.0,
            float(params.get("mean_reversion_exit_zscore", 0.15)),
        )
        self.trend_filter_enabled = self._as_bool(
            params.get("mean_reversion_trend_filter_enabled", True),
            True,
        )
        self.trend_ma_window = max(
            self.window + 1,
            int(params.get("mean_reversion_trend_ma_window", 200)),
        )
        self.use_atr_sl_tp = self._as_bool(
            params.get("mean_reversion_use_atr_sl_tp", True),
            True,
        )
        self.atr_window = max(
            2,
            int(params.get("mean_reversion_atr_window", 14)),
        )
        self.atr_multiplier = max(
            0.1,
            float(params.get("mean_reversion_atr_multiplier", 2.0)),
        )
        self.risk_reward_ratio = max(
            1.0,
            float(params.get("mean_reversion_risk_reward_ratio", 1.8)),
        )
        self.min_stop_loss_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_min_stop_loss_pips",
                    params.get("stop_loss_pips", 20.0),
                )
            ),
        )
        self.min_take_profit_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_min_take_profit_pips",
                    params.get("take_profit_pips", 30.0),
                )
            ),
        )
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = self.min_take_profit_pips
        trend_history = self.trend_ma_window + 1 if self.trend_filter_enabled else self.window + 1
        atr_history = self.atr_window + 1 if self.use_atr_sl_tp else self.window + 1
        self.min_history = max(self.window + 1, trend_history, atr_history)

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default

    def _zscore(self, values: Sequence[float]) -> float:
        return zscore(values, min_std=1e-12)

    @staticmethod
    def _atr(values: Sequence[float], window: int) -> float | None:
        return atr_wilder(values, window)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        upper = symbol.upper()
        if upper in {"US100", "US500", "US30", "DE40", "UK100", "FRA40", "JP225", "EU50"}:
            return 1.0
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return 1.0
        if upper.endswith("JPY"):
            return 0.01
        if upper.startswith("XAU") or upper.startswith("XAG"):
            return 0.1
        if upper in {"WTI", "BRENT"}:
            return 0.1
        return 0.0001

    def _dynamic_sl_tp(self, ctx: StrategyContext, prices: Sequence[float]) -> tuple[float, float, float | None]:
        stop_pips = self.min_stop_loss_pips
        take_pips = max(self.min_take_profit_pips, stop_pips * self.risk_reward_ratio)
        atr_pips: float | None = None
        if not self.use_atr_sl_tp:
            return stop_pips, take_pips, atr_pips
        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return stop_pips, take_pips, atr_pips
        pip_size = float(ctx.tick_size) if ctx.tick_size is not None and ctx.tick_size > 0 else self._pip_size(ctx.symbol)
        atr_pips = atr / max(pip_size, 1e-9)
        stop_pips = max(stop_pips, atr_pips * self.atr_multiplier)
        take_pips = max(take_pips, stop_pips * self.risk_reward_ratio)
        return stop_pips, take_pips, atr_pips

    def _hold(self, reason: str, zscore: float, prices_len: int, **extra: object) -> Signal:
        metadata: dict[str, object] = {
            "indicator": "mean_reversion",
            "reason": reason,
            "zscore": zscore,
            "zscore_threshold": self.threshold,
            "prices_len": prices_len,
        }
        metadata.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.min_stop_loss_pips,
            take_profit_pips=self.min_take_profit_pips,
            metadata=metadata,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        prices = [float(value) for value in ctx.prices]
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                zscore=0.0,
                prices_len=len(prices),
                min_history=self.min_history,
            )

        z = self._zscore(prices[-self.window :])
        last_price = prices[-1]
        slow_ma = tail_mean(prices, self.trend_ma_window) if self.trend_filter_enabled else None
        stop_loss_pips, take_profit_pips, atr_pips = self._dynamic_sl_tp(ctx, prices)
        confidence = min(1.0, abs(z) / (self.threshold * 2.0))

        if z <= -self.threshold:
            if slow_ma is not None and last_price < slow_ma:
                return self._hold(
                    "blocked_by_trend_filter",
                    zscore=z,
                    prices_len=len(prices),
                    trend="below_sma",
                    trend_ma=slow_ma,
                    last_price=last_price,
                )
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "mean_reversion",
                    "zscore": z,
                    "trend_ma": slow_ma,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                },
            )

        if z >= self.threshold:
            if slow_ma is not None and last_price > slow_ma:
                return self._hold(
                    "blocked_by_trend_filter",
                    zscore=z,
                    prices_len=len(prices),
                    trend="above_sma",
                    trend_ma=slow_ma,
                    last_price=last_price,
                )
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "mean_reversion",
                    "zscore": z,
                    "trend_ma": slow_ma,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                },
            )

        if abs(z) <= self.exit_zscore:
            return self._hold(
                "mean_reversion_target_reached",
                zscore=z,
                prices_len=len(prices),
                exit_hint="close_on_mean_reversion",
                exit_zscore=self.exit_zscore,
            )

        return self._hold("no_signal", zscore=z, prices_len=len(prices))
