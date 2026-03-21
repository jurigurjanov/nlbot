from __future__ import annotations

from collections.abc import Sequence
import json
import math

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema
from xtb_bot.strategies.base import Strategy, StrategyContext
from xtb_bot.strategies.pip_size import pip_size as _pip_size_lookup, is_crypto_symbol as _is_crypto, is_index_symbol as _is_index


class TrendFollowingStrategy(Strategy):
    name = "trend_following"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.atr_window = max(2, int(params.get("trend_atr_window", 14)))
        self.fast_ema_window = max(2, int(params.get("fast_ema_window", 20)))
        self.slow_ema_window = max(self.fast_ema_window + 1, int(params.get("slow_ema_window", 80)))
        self.donchian_window = max(2, int(params.get("donchian_window", 20)))
        self.use_donchian_filter = str(params.get("use_donchian_filter", "true")).lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        tick_size_guard_raw = params.get("trend_require_context_tick_size", False)
        if isinstance(tick_size_guard_raw, str):
            self.require_context_tick_size = tick_size_guard_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.require_context_tick_size = bool(tick_size_guard_raw)
        self.breakout_lookback_bars = max(1, int(params.get("trend_breakout_lookback_bars", 6)))
        self.pullback_max_distance_ratio = max(
            1e-6, float(params.get("trend_pullback_max_distance_ratio", 0.003))
        )
        self.pullback_max_distance_atr = max(
            0.0, float(params.get("trend_pullback_max_distance_atr", 0.0))
        )
        self.crypto_max_pullback_distance_atr = max(
            0.0, float(params.get("trend_crypto_max_pullback_distance_atr", 1.8))
        )
        self.pullback_ema_tolerance_ratio = max(
            0.0, float(params.get("trend_pullback_ema_tolerance_ratio", 0.001))
        )
        self.crypto_pullback_ema_tolerance_ratio = max(
            0.0, float(params.get("trend_crypto_pullback_ema_tolerance_ratio", 0.0))
        )
        self.crypto_min_ema_gap_ratio = max(
            0.0, float(params.get("trend_crypto_min_ema_gap_ratio", 0.0012))
        )
        self.crypto_min_fast_slope_ratio = max(
            0.0, float(params.get("trend_crypto_min_fast_slope_ratio", 0.0002))
        )
        self.crypto_min_slow_slope_ratio = max(
            0.0, float(params.get("trend_crypto_min_slow_slope_ratio", 0.00005))
        )
        self.crypto_min_atr_pct = max(
            0.0, float(params.get("trend_crypto_min_atr_pct", 0.18))
        )
        self.slope_mode = str(params.get("trend_slope_mode", "fast_with_slow_tolerance")).strip().lower()
        if self.slope_mode not in {"fast_only", "fast_and_slow", "fast_with_slow_tolerance"}:
            self.slope_mode = "fast_with_slow_tolerance"
        self.slow_slope_tolerance_ratio = max(
            0.0, float(params.get("trend_slow_slope_tolerance_ratio", 0.0003))
        )
        self.pullback_bounce_required = str(
            params.get("trend_pullback_bounce_required", "true")
        ).lower() in {"1", "true", "yes", "on"}
        self.risk_reward_ratio = max(
            1.0,
            float(
                params.get(
                    "trend_risk_reward_ratio",
                    float(params.get("take_profit_pips", 75.0)) / max(float(params.get("stop_loss_pips", 30.0)), 1e-9),
                )
            ),
        )
        self.min_stop_loss_pips = max(
            0.1,
            float(params.get("trend_min_stop_loss_pips", params.get("stop_loss_pips", 30.0))),
        )
        self.min_take_profit_pips = max(
            0.1,
            float(params.get("trend_min_take_profit_pips", params.get("take_profit_pips", 75.0))),
        )
        self.index_min_stop_pct = max(0.0, float(params.get("trend_index_min_stop_pct", 0.25)))
        self.crypto_min_stop_pct = max(0.0, float(params.get("trend_crypto_min_stop_pct", 0.0)))
        self.max_stop_loss_atr = max(0.0, float(params.get("trend_max_stop_loss_atr", 0.0)))
        self.crypto_max_stop_loss_atr = max(
            0.0, float(params.get("trend_crypto_max_stop_loss_atr", self.max_stop_loss_atr))
        )
        self.spread_buffer_factor = max(0.0, float(params.get("trend_spread_buffer_factor", 0.0)))
        self.max_spread_to_stop_ratio = max(
            0.0, float(params.get("trend_max_spread_to_stop_ratio", 0.0))
        )
        self.max_spread_to_stop_ratio_by_symbol = self._parse_spread_ratio_limits(
            params.get("trend_max_spread_to_stop_ratio_by_symbol")
        )
        self.max_timestamp_gap_sec = max(0.0, float(params.get("trend_max_timestamp_gap_sec", 0.0)))
        self.trend_strength_norm = max(
            1e-6, float(params.get("trend_strength_norm_ratio", self.pullback_max_distance_ratio))
        )
        self.confidence_velocity_norm_ratio = max(
            1e-6,
            float(
                params.get(
                    "trend_confidence_velocity_norm_ratio",
                    self.slow_slope_tolerance_ratio if self.slow_slope_tolerance_ratio > 0 else 0.0005,
                )
            ),
        )
        volume_confirmation_raw = params.get("trend_volume_confirmation", False)
        if isinstance(volume_confirmation_raw, str):
            self.volume_confirmation = volume_confirmation_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_confirmation = bool(volume_confirmation_raw)
        self.volume_window = max(2, int(params.get("trend_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("trend_min_volume_ratio", 1.4)))
        self.volume_min_samples = max(1, int(params.get("trend_volume_min_samples", 8)))
        volume_allow_missing_raw = params.get("trend_volume_allow_missing", True)
        if isinstance(volume_allow_missing_raw, str):
            self.volume_allow_missing = volume_allow_missing_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_allow_missing = bool(volume_allow_missing_raw)
        volume_require_spike_raw = params.get("trend_volume_require_spike", False)
        if isinstance(volume_require_spike_raw, str):
            self.volume_require_spike = volume_require_spike_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_require_spike = bool(volume_require_spike_raw)
        self.volume_confidence_boost = max(0.0, float(params.get("trend_volume_confidence_boost", 0.1)))
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(self.min_take_profit_pips, self.stop_loss_pips * self.risk_reward_ratio)
        self.min_history = max(self.fast_ema_window, self.slow_ema_window, self.donchian_window) + 2

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        return _pip_size_lookup(symbol)

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        return _is_index(symbol)

    @staticmethod
    def _is_crypto_symbol(symbol: str) -> bool:
        return _is_crypto(symbol)

    @staticmethod
    def _extract_finite_positive_volumes(raw_values: Sequence[float]) -> list[float]:
        values: list[float] = []
        for raw in raw_values:
            try:
                volume = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(volume) or volume <= 0.0:
                continue
            values.append(volume)
        return values

    @staticmethod
    def _parse_spread_ratio_limits(raw: object) -> dict[str, float]:
        if raw is None:
            return {}
        payload = raw
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return {}
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return {}
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, float] = {}
        for key, value in payload.items():
            symbol = str(key).strip().upper()
            if not symbol:
                continue
            try:
                limit = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(limit) or limit <= 0:
                continue
            parsed[symbol] = limit
        return parsed

    def _spread_to_stop_limit_for_symbol(self, symbol: str) -> tuple[float, str]:
        upper_symbol = str(symbol).strip().upper()
        override = self.max_spread_to_stop_ratio_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return self.max_spread_to_stop_ratio, "global"

    def _evaluate_volume_confirmation(self, ctx: StrategyContext) -> tuple[dict[str, object], str | None, float]:
        metadata: dict[str, object] = {
            "volume_confirmation_enabled": self.volume_confirmation,
            "volume_window": self.volume_window,
            "min_volume_ratio": self.min_volume_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_require_spike": self.volume_require_spike,
            "volume_confidence_boost_config": self.volume_confidence_boost,
            "volume_data_available": False,
            "volume_spike": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
        }
        if not self.volume_confirmation:
            return metadata, None, 0.0

        raw_samples = self._extract_finite_positive_volumes(ctx.volumes[-(self.volume_window + 5) :])
        current_volume = None
        if ctx.current_volume is not None:
            try:
                current_volume = float(ctx.current_volume)
            except (TypeError, ValueError):
                current_volume = None
            if current_volume is not None and (not math.isfinite(current_volume) or current_volume <= 0.0):
                current_volume = None
        if current_volume is not None:
            raw_samples.append(current_volume)

        if len(raw_samples) < 2:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = raw_samples[-1]
        history = raw_samples[:-1]
        if len(history) > self.volume_window:
            history = history[-self.volume_window :]
        if len(history) < self.volume_min_samples:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        avg_volume = sum(history) / max(len(history), 1)
        if avg_volume <= 0.0:
            if self.volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        volume_ratio = current / avg_volume
        spike = volume_ratio >= self.min_volume_ratio
        metadata.update(
            {
                "volume_data_available": True,
                "volume_spike": spike,
                "volume_current": current,
                "volume_avg": avg_volume,
                "volume_ratio": volume_ratio,
            }
        )
        if self.volume_require_spike and not spike:
            return metadata, "volume_not_confirmed", 0.0
        return metadata, None, self.volume_confidence_boost if spike else 0.0

    def _resolve_pip_size(self, ctx: StrategyContext, is_crypto: bool) -> tuple[float, str]:
        if ctx.tick_size not in (None, 0.0):
            return float(ctx.tick_size), "context_tick_size"
        if is_crypto:
            return 1.0, "crypto_default_ig_point"
        return self._pip_size(ctx.symbol), "fallback_symbol_map"

    def _donchian_breakout(self, prices: Sequence[float]) -> tuple[bool, bool, float, float]:
        if len(prices) < (self.donchian_window + 1):
            return False, False, float(prices[-1]), float(prices[-1])
        reference = prices[-(self.donchian_window + 1) : -1]
        upper = float(max(reference))
        lower = float(min(reference))
        last = float(prices[-1])
        return last > upper, last < lower, upper, lower

    def _recent_donchian_breakout(self, prices: Sequence[float]) -> tuple[bool, bool]:
        up = False
        down = False
        checks = min(self.breakout_lookback_bars, max(0, len(prices) - self.donchian_window))
        for offset in range(checks):
            end = len(prices) - offset
            if end < (self.donchian_window + 1):
                break
            reference = prices[end - (self.donchian_window + 1) : end - 1]
            current = float(prices[end - 1])
            upper = float(max(reference))
            lower = float(min(reference))
            up = up or current > upper
            down = down or current < lower
        return up, down

    def _hold(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        metadata: dict[str, object] = {"reason": reason, "indicator": "ema_trend_following"}
        if extra:
            metadata.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=metadata,
        )

    def _confidence(
        self,
        ema_gap_ratio: float,
        pullback_distance_ratio: float,
        overextension_ratio: float,
        bounce_confirmed: bool,
        directional_velocity_ratio: float,
        price_aligned_with_fast_ma: bool,
    ) -> float:
        trend_strength = min(1.0, ema_gap_ratio / max(self.trend_strength_norm, 1e-9))
        velocity_score = min(1.0, directional_velocity_ratio / max(self.confidence_velocity_norm_ratio, 1e-9))
        pullback_quality = 1.0 - min(1.0, pullback_distance_ratio / max(self.pullback_max_distance_ratio, 1e-9))
        overextension_penalty = min(1.0, overextension_ratio / max(self.pullback_max_distance_ratio, 1e-9))
        bounce_bonus = 0.06 if bounce_confirmed else 0.0
        price_alignment_bonus = 0.08 if price_aligned_with_fast_ma else 0.0
        value = (
            0.1
            + trend_strength * 0.24
            + velocity_score * 0.42
            + pullback_quality * 0.20
            + bounce_bonus
            + price_alignment_bonus
            - overextension_penalty * 0.30
        )
        return max(0.05, min(1.0, value))

    @staticmethod
    def _slope_ratio(current: float, previous: float) -> float:
        return (current - previous) / max(abs(previous), 1e-9)

    def _trend_direction(self, fast_now: float, slow_now: float, fast_prev: float, slow_prev: float) -> tuple[bool, bool]:
        fast_slope_ratio = self._slope_ratio(fast_now, fast_prev)
        slow_slope_ratio = self._slope_ratio(slow_now, slow_prev)
        is_up = fast_now > slow_now
        is_down = fast_now < slow_now

        if self.slope_mode == "fast_only":
            return is_up and fast_slope_ratio >= 0.0, is_down and fast_slope_ratio <= 0.0

        if self.slope_mode == "fast_and_slow":
            return (
                is_up and fast_slope_ratio >= 0.0 and slow_slope_ratio >= 0.0,
                is_down and fast_slope_ratio <= 0.0 and slow_slope_ratio <= 0.0,
            )

        tolerance = self.slow_slope_tolerance_ratio
        return (
            is_up and fast_slope_ratio >= 0.0 and slow_slope_ratio >= -tolerance,
            is_down and fast_slope_ratio <= 0.0 and slow_slope_ratio <= tolerance,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        signal = self._generate_signal_impl(ctx)
        if signal.side == Side.HOLD and getattr(self, "_last_fast_ema", None) is not None:
            signal.metadata.setdefault("fast_ema", self._last_fast_ema)
            signal.metadata.setdefault("slow_ema", self._last_slow_ema)
        return signal

    def _generate_signal_impl(self, ctx: StrategyContext) -> Signal:
        prices: list[float] = []
        invalid_prices = 0
        for raw in ctx.prices:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                invalid_prices += 1
                continue
            if not math.isfinite(value):
                invalid_prices += 1
                continue
            prices.append(value)
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                {
                    "history": len(prices),
                    "required_history": self.min_history,
                    "invalid_prices_dropped": invalid_prices,
                },
            )
        if self.require_context_tick_size and ctx.tick_size in (None, 0.0):
            return self._hold(
                "tick_size_unavailable",
                {"trend_require_context_tick_size": True},
            )
        if self.max_timestamp_gap_sec > 0 and len(ctx.timestamps) >= 2:
            last_ts = float(ctx.timestamps[-1])
            prev_ts = float(ctx.timestamps[-2])
            gap = last_ts - prev_ts
            if last_ts > 10_000_000_000:
                gap = gap / 1000.0
            if gap > self.max_timestamp_gap_sec:
                return self._hold(
                    "timestamp_gap_too_wide",
                    {
                        "timestamp_gap_sec": gap,
                        "trend_max_timestamp_gap_sec": self.max_timestamp_gap_sec,
                    },
                )

        fast_now = self._ema(prices, self.fast_ema_window)
        slow_now = self._ema(prices, self.slow_ema_window)
        self._last_fast_ema = fast_now
        self._last_slow_ema = slow_now
        fast_prev = self._ema(prices[:-1], self.fast_ema_window)
        slow_prev = self._ema(prices[:-1], self.slow_ema_window)
        fast_slope_ratio = self._slope_ratio(fast_now, fast_prev)
        slow_slope_ratio = self._slope_ratio(slow_now, slow_prev)
        trend_up, trend_down = self._trend_direction(fast_now, slow_now, fast_prev, slow_prev)

        breakout_up, breakout_down, channel_upper, channel_lower = self._donchian_breakout(prices)
        recent_breakout_up, recent_breakout_down = self._recent_donchian_breakout(prices)
        breakout_filter_up = recent_breakout_up if self.use_donchian_filter else True
        breakout_filter_down = recent_breakout_down if self.use_donchian_filter else True

        last_price = float(prices[-1])
        prev_price = float(prices[-2])
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), 1e-9)
        channel_mid = (channel_upper + channel_lower) / 2.0
        pullback_distance_ratio = min(
            abs(last_price - fast_now) / max(abs(last_price), 1e-9),
            abs(last_price - channel_mid) / max(abs(last_price), 1e-9),
        )
        atr = atr_wilder(prices, self.atr_window)
        pullback_distance_atr = (
            abs(last_price - fast_now) / max(atr, 1e-9)
            if atr is not None
            else None
        )
        atr_pct = (atr / max(abs(last_price), 1e-9) * 100.0) if atr is not None else None
        is_crypto = self._is_crypto_symbol(ctx.symbol)
        max_pullback_distance_atr = self.pullback_max_distance_atr
        if is_crypto and self.crypto_max_pullback_distance_atr > 0:
            if max_pullback_distance_atr <= 0:
                max_pullback_distance_atr = self.crypto_max_pullback_distance_atr
            else:
                max_pullback_distance_atr = min(max_pullback_distance_atr, self.crypto_max_pullback_distance_atr)
        pullback_ema_tolerance_ratio = self.pullback_ema_tolerance_ratio
        if is_crypto and self.crypto_pullback_ema_tolerance_ratio > 0:
            pullback_ema_tolerance_ratio = max(
                pullback_ema_tolerance_ratio,
                self.crypto_pullback_ema_tolerance_ratio,
            )
        pullback_buy_zone = last_price <= fast_now * (1.0 + pullback_ema_tolerance_ratio)
        pullback_sell_zone = last_price >= fast_now * (1.0 - pullback_ema_tolerance_ratio)
        pip_size, pip_size_source = self._resolve_pip_size(ctx, is_crypto)

        if is_crypto:
            if atr_pct is None:
                return self._hold("crypto_atr_unavailable")
            if atr_pct < self.crypto_min_atr_pct:
                return self._hold(
                    "crypto_atr_below_threshold",
                    {"atr_pct": atr_pct, "crypto_min_atr_pct": self.crypto_min_atr_pct},
                )
            if ema_gap_ratio < self.crypto_min_ema_gap_ratio:
                return self._hold(
                    "crypto_trend_too_weak",
                    {
                        "ema_gap_ratio": ema_gap_ratio,
                        "crypto_min_ema_gap_ratio": self.crypto_min_ema_gap_ratio,
                    },
                )

        if trend_up and breakout_filter_up:
            if is_crypto and (
                fast_slope_ratio < self.crypto_min_fast_slope_ratio
                or slow_slope_ratio < self.crypto_min_slow_slope_ratio
            ):
                return self._hold(
                    "crypto_slope_too_weak",
                    {
                        "trend": "up",
                        "fast_slope_ratio": fast_slope_ratio,
                        "slow_slope_ratio": slow_slope_ratio,
                        "crypto_min_fast_slope_ratio": self.crypto_min_fast_slope_ratio,
                        "crypto_min_slow_slope_ratio": self.crypto_min_slow_slope_ratio,
                    },
                )
            if not pullback_buy_zone or last_price <= slow_now:
                return self._hold(
                    "pullback_not_in_zone",
                    {
                        "trend": "up",
                        "pullback_buy_zone": pullback_buy_zone,
                        "price_above_slow_ema": last_price > slow_now,
                        "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                    },
                )
            if pullback_distance_ratio > self.pullback_max_distance_ratio:
                return self._hold(
                    "no_pullback_entry",
                    {
                        "trend": "up",
                        "pullback_distance_ratio": pullback_distance_ratio,
                        "pullback_max_distance_ratio": self.pullback_max_distance_ratio,
                    },
                )
            if (
                max_pullback_distance_atr > 0
                and pullback_distance_atr is not None
                and pullback_distance_atr > max_pullback_distance_atr
            ):
                return self._hold(
                    "no_pullback_entry_atr",
                    {
                        "trend": "up",
                        "pullback_distance_atr": pullback_distance_atr,
                        "pullback_max_distance_atr": max_pullback_distance_atr,
                    },
                )
            bounce_confirmed = last_price >= prev_price
            if self.pullback_bounce_required and not bounce_confirmed:
                return self._hold("pullback_not_confirmed", {"trend": "up"})

            structure_stop_pips = (last_price - channel_lower) / max(pip_size, 1e-9)
            if structure_stop_pips <= 0:
                return self._hold("invalid_structure_stop", {"trend": "up"})

            stop_floor_pips = self.min_stop_loss_pips
            if self._is_index_symbol(ctx.symbol) and self.index_min_stop_pct > 0:
                index_stop_pips = (last_price * self.index_min_stop_pct / 100.0) / max(pip_size, 1e-9)
                stop_floor_pips = max(stop_floor_pips, index_stop_pips)
            if is_crypto and self.crypto_min_stop_pct > 0:
                crypto_stop_pips = (last_price * self.crypto_min_stop_pct / 100.0) / max(pip_size, 1e-9)
                stop_floor_pips = max(stop_floor_pips, crypto_stop_pips)

            stop_loss_pips = max(stop_floor_pips, structure_stop_pips)
            stop_cap_atr = self.max_stop_loss_atr
            if is_crypto and self.crypto_max_stop_loss_atr > 0:
                stop_cap_atr = self.crypto_max_stop_loss_atr
            stop_loss_cap_pips = None
            if stop_cap_atr > 0:
                if atr is None or atr <= 0:
                    return self._hold(
                        "stop_loss_cap_requires_atr",
                        {"trend": "up", "stop_cap_atr": stop_cap_atr},
                    )
                stop_loss_cap_pips = (atr * stop_cap_atr) / max(pip_size, 1e-9)
                if stop_loss_cap_pips < stop_floor_pips:
                    return self._hold(
                        "stop_loss_cap_below_required_floor",
                        {
                            "trend": "up",
                            "stop_loss_cap_pips": stop_loss_cap_pips,
                            "required_stop_floor_pips": stop_floor_pips,
                            "stop_cap_atr": stop_cap_atr,
                        },
                    )
                stop_loss_pips = min(stop_loss_pips, stop_loss_cap_pips)

            spread_pips = max(0.0, float(ctx.current_spread_pips or 0.0))
            if self.spread_buffer_factor > 0 and spread_pips > 0:
                stop_loss_pips += spread_pips * self.spread_buffer_factor
            spread_to_stop_ratio = (
                spread_pips / max(stop_loss_pips, 1e-9)
                if spread_pips > 0 and stop_loss_pips > 0
                else 0.0
            )
            spread_to_stop_limit, spread_to_stop_limit_source = self._spread_to_stop_limit_for_symbol(ctx.symbol)
            if (
                spread_to_stop_limit > 0
                and spread_pips > 0
                and spread_to_stop_ratio >= spread_to_stop_limit
            ):
                return self._hold(
                    "spread_too_high_relative_to_stop",
                    {
                        "trend": "up",
                        "spread_pips": spread_pips,
                        "stop_loss_pips": stop_loss_pips,
                        "spread_to_stop_ratio": spread_to_stop_ratio,
                        "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                        "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    },
                )
            take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)
            overextension_ratio = max(0.0, last_price - channel_upper) / max(abs(last_price), 1e-9)
            directional_velocity_ratio = max(0.0, fast_slope_ratio) + max(0.0, slow_slope_ratio) * 0.5
            price_aligned_with_fast_ma = last_price >= fast_now
            confidence = self._confidence(
                ema_gap_ratio=ema_gap_ratio,
                pullback_distance_ratio=pullback_distance_ratio,
                overextension_ratio=overextension_ratio,
                bounce_confirmed=bounce_confirmed,
                directional_velocity_ratio=directional_velocity_ratio,
                price_aligned_with_fast_ma=price_aligned_with_fast_ma,
            )
            volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
            if volume_hold_reason is not None:
                hold_payload = dict(volume_meta)
                hold_payload["trend"] = "up"
                return self._hold(volume_hold_reason, hold_payload)
            confidence = min(1.0, confidence + volume_confidence_boost)
            return Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ema_trend_following",
                    "trend": "up",
                    "entry_type": "pullback",
                    "fast_ema_window": self.fast_ema_window,
                    "slow_ema_window": self.slow_ema_window,
                    "donchian_window": self.donchian_window,
                    "donchian_breakout": breakout_up,
                    "recent_breakout": recent_breakout_up,
                    "channel_upper": channel_upper,
                    "channel_mid": channel_mid,
                    "channel_lower": channel_lower,
                    "pullback_distance_ratio": pullback_distance_ratio,
                    "pullback_distance_atr": pullback_distance_atr,
                    "ema_gap_ratio": ema_gap_ratio,
                    "fast_slope_ratio": fast_slope_ratio,
                    "slow_slope_ratio": slow_slope_ratio,
                    "directional_velocity_ratio": directional_velocity_ratio,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "slope_mode": self.slope_mode,
                    "confidence_model": "velocity_weighted_v2",
                    "trend_confidence_velocity_norm_ratio": self.confidence_velocity_norm_ratio,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "atr": atr,
                    "atr_pct": atr_pct,
                    "spread_pips": spread_pips,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "trend_spread_buffer_factor": self.spread_buffer_factor,
                    "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                    "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    "stop_cap_atr": stop_cap_atr,
                    "stop_loss_cap_pips": stop_loss_cap_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "stop_source": "channel_lower",
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    **volume_meta,
                },
            )

        if trend_down and breakout_filter_down:
            if is_crypto and (
                fast_slope_ratio > -self.crypto_min_fast_slope_ratio
                or slow_slope_ratio > -self.crypto_min_slow_slope_ratio
            ):
                return self._hold(
                    "crypto_slope_too_weak",
                    {
                        "trend": "down",
                        "fast_slope_ratio": fast_slope_ratio,
                        "slow_slope_ratio": slow_slope_ratio,
                        "crypto_min_fast_slope_ratio": self.crypto_min_fast_slope_ratio,
                        "crypto_min_slow_slope_ratio": self.crypto_min_slow_slope_ratio,
                    },
                )
            if not pullback_sell_zone or last_price >= slow_now:
                return self._hold(
                    "pullback_not_in_zone",
                    {
                        "trend": "down",
                        "pullback_sell_zone": pullback_sell_zone,
                        "price_below_slow_ema": last_price < slow_now,
                        "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                    },
                )
            if pullback_distance_ratio > self.pullback_max_distance_ratio:
                return self._hold(
                    "no_pullback_entry",
                    {
                        "trend": "down",
                        "pullback_distance_ratio": pullback_distance_ratio,
                        "pullback_max_distance_ratio": self.pullback_max_distance_ratio,
                    },
                )
            if (
                max_pullback_distance_atr > 0
                and pullback_distance_atr is not None
                and pullback_distance_atr > max_pullback_distance_atr
            ):
                return self._hold(
                    "no_pullback_entry_atr",
                    {
                        "trend": "down",
                        "pullback_distance_atr": pullback_distance_atr,
                        "pullback_max_distance_atr": max_pullback_distance_atr,
                    },
                )
            bounce_confirmed = last_price <= prev_price
            if self.pullback_bounce_required and not bounce_confirmed:
                return self._hold("pullback_not_confirmed", {"trend": "down"})

            structure_stop_pips = (channel_upper - last_price) / max(pip_size, 1e-9)
            if structure_stop_pips <= 0:
                return self._hold("invalid_structure_stop", {"trend": "down"})

            stop_floor_pips = self.min_stop_loss_pips
            if self._is_index_symbol(ctx.symbol) and self.index_min_stop_pct > 0:
                index_stop_pips = (last_price * self.index_min_stop_pct / 100.0) / max(pip_size, 1e-9)
                stop_floor_pips = max(stop_floor_pips, index_stop_pips)
            if is_crypto and self.crypto_min_stop_pct > 0:
                crypto_stop_pips = (last_price * self.crypto_min_stop_pct / 100.0) / max(pip_size, 1e-9)
                stop_floor_pips = max(stop_floor_pips, crypto_stop_pips)

            stop_loss_pips = max(stop_floor_pips, structure_stop_pips)
            stop_cap_atr = self.max_stop_loss_atr
            if is_crypto and self.crypto_max_stop_loss_atr > 0:
                stop_cap_atr = self.crypto_max_stop_loss_atr
            stop_loss_cap_pips = None
            if stop_cap_atr > 0:
                if atr is None or atr <= 0:
                    return self._hold(
                        "stop_loss_cap_requires_atr",
                        {"trend": "down", "stop_cap_atr": stop_cap_atr},
                    )
                stop_loss_cap_pips = (atr * stop_cap_atr) / max(pip_size, 1e-9)
                if stop_loss_cap_pips < stop_floor_pips:
                    return self._hold(
                        "stop_loss_cap_below_required_floor",
                        {
                            "trend": "down",
                            "stop_loss_cap_pips": stop_loss_cap_pips,
                            "required_stop_floor_pips": stop_floor_pips,
                            "stop_cap_atr": stop_cap_atr,
                        },
                    )
                stop_loss_pips = min(stop_loss_pips, stop_loss_cap_pips)

            spread_pips = max(0.0, float(ctx.current_spread_pips or 0.0))
            if self.spread_buffer_factor > 0 and spread_pips > 0:
                stop_loss_pips += spread_pips * self.spread_buffer_factor
            spread_to_stop_ratio = (
                spread_pips / max(stop_loss_pips, 1e-9)
                if spread_pips > 0 and stop_loss_pips > 0
                else 0.0
            )
            spread_to_stop_limit, spread_to_stop_limit_source = self._spread_to_stop_limit_for_symbol(ctx.symbol)
            if (
                spread_to_stop_limit > 0
                and spread_pips > 0
                and spread_to_stop_ratio >= spread_to_stop_limit
            ):
                return self._hold(
                    "spread_too_high_relative_to_stop",
                    {
                        "trend": "down",
                        "spread_pips": spread_pips,
                        "stop_loss_pips": stop_loss_pips,
                        "spread_to_stop_ratio": spread_to_stop_ratio,
                        "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                        "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    },
                )
            take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)
            overextension_ratio = max(0.0, channel_lower - last_price) / max(abs(last_price), 1e-9)
            directional_velocity_ratio = max(0.0, -fast_slope_ratio) + max(0.0, -slow_slope_ratio) * 0.5
            price_aligned_with_fast_ma = last_price <= fast_now
            confidence = self._confidence(
                ema_gap_ratio=ema_gap_ratio,
                pullback_distance_ratio=pullback_distance_ratio,
                overextension_ratio=overextension_ratio,
                bounce_confirmed=bounce_confirmed,
                directional_velocity_ratio=directional_velocity_ratio,
                price_aligned_with_fast_ma=price_aligned_with_fast_ma,
            )
            volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
            if volume_hold_reason is not None:
                hold_payload = dict(volume_meta)
                hold_payload["trend"] = "down"
                return self._hold(volume_hold_reason, hold_payload)
            confidence = min(1.0, confidence + volume_confidence_boost)
            return Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ema_trend_following",
                    "trend": "down",
                    "entry_type": "pullback",
                    "fast_ema_window": self.fast_ema_window,
                    "slow_ema_window": self.slow_ema_window,
                    "donchian_window": self.donchian_window,
                    "donchian_breakout": breakout_down,
                    "recent_breakout": recent_breakout_down,
                    "channel_upper": channel_upper,
                    "channel_mid": channel_mid,
                    "channel_lower": channel_lower,
                    "pullback_distance_ratio": pullback_distance_ratio,
                    "pullback_distance_atr": pullback_distance_atr,
                    "ema_gap_ratio": ema_gap_ratio,
                    "fast_slope_ratio": fast_slope_ratio,
                    "slow_slope_ratio": slow_slope_ratio,
                    "directional_velocity_ratio": directional_velocity_ratio,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "slope_mode": self.slope_mode,
                    "confidence_model": "velocity_weighted_v2",
                    "trend_confidence_velocity_norm_ratio": self.confidence_velocity_norm_ratio,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "atr": atr,
                    "atr_pct": atr_pct,
                    "spread_pips": spread_pips,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "trend_spread_buffer_factor": self.spread_buffer_factor,
                    "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                    "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    "stop_cap_atr": stop_cap_atr,
                    "stop_loss_cap_pips": stop_loss_cap_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "stop_source": "channel_upper",
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    **volume_meta,
                },
            )

        if self.use_donchian_filter and ((trend_up and not breakout_filter_up) or (trend_down and not breakout_filter_down)):
            return self._hold(
                "trend_filter_not_confirmed",
                {
                    "trend_up": trend_up,
                    "trend_down": trend_down,
                    "recent_breakout_up": recent_breakout_up,
                    "recent_breakout_down": recent_breakout_down,
                },
            )

        return self._hold("no_signal")
