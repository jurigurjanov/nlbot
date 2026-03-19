from __future__ import annotations

from xtb_bot.strategies.trend_following import TrendFollowingStrategy


class CryptoTrendFollowingStrategy(TrendFollowingStrategy):
    name = "crypto_trend_following"

    _DEFAULT_OVERRIDES: dict[str, object] = {
        "fast_ema_window": 21,
        "slow_ema_window": 89,
        "donchian_window": 20,
        "use_donchian_filter": True,
        "trend_breakout_lookback_bars": 10,
        "trend_pullback_max_distance_ratio": 0.0025,
        "trend_pullback_max_distance_atr": 1.5,
        "trend_crypto_max_pullback_distance_atr": 1.5,
        "trend_pullback_ema_tolerance_ratio": 0.0015,
        "trend_crypto_pullback_ema_tolerance_ratio": 0.015,
        "trend_crypto_min_ema_gap_ratio": 0.0015,
        "trend_crypto_min_fast_slope_ratio": 0.00025,
        "trend_crypto_min_slow_slope_ratio": 0.00008,
        "trend_crypto_min_atr_pct": 0.20,
        "trend_pullback_bounce_required": False,
        "trend_spread_buffer_factor": 1.0,
        "trend_max_spread_to_stop_ratio": 0.50,
        "trend_crypto_min_stop_pct": 1.0,
        "trend_crypto_max_stop_loss_atr": 3.0,
        "trend_max_timestamp_gap_sec": 3600.0,
        "trend_risk_reward_ratio": 2.0,
        "trend_min_stop_loss_pips": 150.0,
        "trend_min_take_profit_pips": 120.0,
        "trend_strength_norm_ratio": 0.0025,
    }

    def __init__(self, params: dict[str, object]):
        merged: dict[str, object] = dict(self._DEFAULT_OVERRIDES)
        merged.update(params)
        super().__init__(merged)

    def supports_symbol(self, symbol: str) -> bool:
        return self._is_crypto_symbol(symbol)
