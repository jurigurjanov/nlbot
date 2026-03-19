from __future__ import annotations

from collections.abc import Sequence
import json
import math
import threading

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, tail_mean
from xtb_bot.strategies.base import Strategy, StrategyContext


class DonchianBreakoutStrategy(Strategy):
    name = "donchian_breakout"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.window = max(2, int(params.get("donchian_breakout_window", 20)))
        self.base_stop_loss_pips = float(
            params.get("donchian_breakout_stop_loss_pips", params.get("stop_loss_pips", 30.0))
        )
        self.base_take_profit_pips = float(
            params.get("donchian_breakout_take_profit_pips", params.get("take_profit_pips", 75.0))
        )
        default_atr_window = min(14, self.window)
        self.atr_window = max(2, int(params.get("donchian_breakout_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("donchian_breakout_atr_multiplier", 2.0)))
        default_rr = self.base_take_profit_pips / max(self.base_stop_loss_pips, 1e-9)
        self.risk_reward_ratio = max(1.0, float(params.get("donchian_breakout_risk_reward_ratio", default_rr)))
        self.min_stop_loss_pips = max(
            0.1,
            float(params.get("donchian_breakout_min_stop_loss_pips", self.base_stop_loss_pips)),
        )
        self.min_take_profit_pips = max(
            0.1,
            float(params.get("donchian_breakout_min_take_profit_pips", self.base_take_profit_pips)),
        )
        self.min_relative_stop_pct = max(0.0, float(params.get("donchian_breakout_min_relative_stop_pct", 0.0008)))
        self.min_breakout_atr_ratio = max(
            0.0,
            float(params.get("donchian_breakout_min_breakout_atr_ratio", 0.15)),
        )
        self.max_breakout_atr_ratio = max(
            self.min_breakout_atr_ratio,
            float(params.get("donchian_breakout_max_breakout_atr_ratio", 1.8)),
        )
        self.min_channel_width_atr = max(
            0.0,
            float(params.get("donchian_breakout_min_channel_width_atr", 0.6)),
        )
        max_spread_raw = params.get("donchian_breakout_max_spread_pips")
        self.max_spread_pips = float(max_spread_raw) if max_spread_raw is not None else None
        self.max_spread_pips_by_symbol = self._parse_spread_limits(
            params.get("donchian_breakout_max_spread_pips_by_symbol")
        )

        volume_check_raw = params.get("donchian_breakout_volume_confirmation", False)
        if isinstance(volume_check_raw, str):
            self.volume_confirmation = volume_check_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_confirmation = bool(volume_check_raw)
        self.volume_window = max(2, int(params.get("donchian_breakout_volume_window", 20)))
        self.volume_min_ratio = max(1.0, float(params.get("donchian_breakout_min_volume_ratio", 1.2)))
        self.volume_min_samples = max(1, int(params.get("donchian_breakout_volume_min_samples", 5)))
        volume_allow_missing_raw = params.get("donchian_breakout_volume_allow_missing", True)
        if isinstance(volume_allow_missing_raw, str):
            self.volume_allow_missing = volume_allow_missing_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.volume_allow_missing = bool(volume_allow_missing_raw)

        self.confidence_base = max(0.0, min(1.0, float(params.get("donchian_breakout_confidence_base", 0.2))))
        self.confidence_breakout_weight = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_confidence_breakout_weight", 0.45))),
        )
        self.confidence_channel_weight = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_confidence_channel_weight", 0.25))),
        )
        self.confidence_extension_weight = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_confidence_extension_weight", 0.20))),
        )
        self.confidence_breakout_target_atr = max(
            1e-9,
            float(params.get("donchian_breakout_confidence_breakout_target_atr", 0.6)),
        )
        self.confidence_breakout_tolerance_atr = max(
            1e-9,
            float(params.get("donchian_breakout_confidence_breakout_tolerance_atr", 0.6)),
        )
        self.confidence_channel_norm_atr = max(
            1e-9,
            float(params.get("donchian_breakout_confidence_channel_norm_atr", 2.0)),
        )
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(self.min_take_profit_pips, self.stop_loss_pips * self.risk_reward_ratio)
        self.min_history = max(self.window + 2, self.atr_window + 2)
        self._signal_lock = threading.Lock()

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        upper = symbol.upper()
        if upper in {"US100", "US500", "US30", "DE40", "UK100", "FRA40", "JP225", "EU50"}:
            return 0.1
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return 0.1
        if upper.endswith("JPY"):
            return 0.01
        if upper.startswith("XAU") or upper.startswith("XAG"):
            return 0.1
        if upper in {"WTI", "BRENT"}:
            return 0.1
        return 0.0001

    @staticmethod
    def _parse_spread_limits(raw: object) -> dict[str, float]:
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

    def _spread_limit_for_symbol(self, symbol: str) -> tuple[float | None, str]:
        upper_symbol = str(symbol).strip().upper()
        override = self.max_spread_pips_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return self.max_spread_pips, "global"

    def _hold(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "donchian_breakout"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=payload,
        )

    def _volume_check(self, ctx: StrategyContext) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "volume_confirmation": self.volume_confirmation,
            "volume_window": self.volume_window,
            "volume_min_ratio": self.volume_min_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
        }
        if not self.volume_confirmation:
            payload["volume_check_status"] = "disabled"
            return True, payload

        current_volume = ctx.current_volume
        history = []
        for value in ctx.volumes:
            try:
                fv = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(fv) and fv > 0:
                history.append(fv)
        payload["current_volume"] = current_volume
        payload["volume_history_len"] = len(history)

        if current_volume is None or current_volume <= 0:
            payload["volume_check_status"] = "missing_current_volume"
            return self.volume_allow_missing, payload

        lookback = min(self.volume_window, len(history))
        if lookback < self.volume_min_samples:
            payload["volume_check_status"] = "insufficient_volume_history"
            payload["volume_lookback"] = lookback
            return self.volume_allow_missing, payload

        avg_volume = tail_mean(history, lookback)
        threshold = avg_volume * self.volume_min_ratio
        payload["volume_check_status"] = "ok" if current_volume >= threshold else "below_threshold"
        payload["avg_volume"] = avg_volume
        payload["min_required_volume"] = threshold
        payload["volume_lookback"] = lookback
        return current_volume >= threshold, payload

    def _confidence(self, breakout_atr_ratio: float, channel_width_atr: float) -> float:
        breakout_target = self.confidence_breakout_target_atr
        breakout_tolerance = self.confidence_breakout_tolerance_atr
        breakout_quality = 1.0 - min(1.0, abs(breakout_atr_ratio - breakout_target) / breakout_tolerance)
        channel_score = min(1.0, max(0.0, channel_width_atr) / self.confidence_channel_norm_atr)
        extension_score = min(1.0, max(0.0, breakout_atr_ratio) / max(self.max_breakout_atr_ratio, 1e-9))
        raw = (
            self.confidence_base
            + breakout_quality * self.confidence_breakout_weight
            + channel_score * self.confidence_channel_weight
            + extension_score * self.confidence_extension_weight
        )
        return max(0.0, min(1.0, raw))

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        with self._signal_lock:
            prices = ctx.prices
            if len(prices) < self.min_history:
                return self._hold(
                    "insufficient_price_history",
                    {"history": len(prices), "required_history": self.min_history},
                )

            reference = prices[-(self.window + 1) : -1]
            upper = float(max(reference))
            lower = float(min(reference))
            prev = float(prices[-2])
            last = float(prices[-1])
            channel_width = upper - lower

            breakout_up = last > upper and prev <= upper
            breakout_down = last < lower and prev >= lower

            spread_limit, spread_limit_source = self._spread_limit_for_symbol(ctx.symbol)
            if spread_limit is not None and ctx.current_spread_pips is not None and ctx.current_spread_pips > spread_limit:
                return self._hold(
                    "spread_too_wide",
                    {
                        "spread_pips": ctx.current_spread_pips,
                        "max_spread_pips": spread_limit,
                        "max_spread_pips_source": spread_limit_source,
                    },
                )

            volume_ok, volume_payload = self._volume_check(ctx)
            if not volume_ok:
                return self._hold("volume_below_threshold", volume_payload)

            atr = self._atr(prices, self.atr_window)
            if atr is None or atr <= 0:
                return self._hold("atr_unavailable", {"atr_window": self.atr_window})

            if ctx.tick_size not in (None, 0.0):
                pip_size = float(ctx.tick_size)
                pip_size_source = "context_tick_size"
            else:
                pip_size = self._pip_size(ctx.symbol)
                pip_size_source = "fallback_symbol_map"

            atr_pips = atr / max(pip_size, 1e-9)
            stop_loss_pips = max(self.min_stop_loss_pips, atr_pips * self.atr_multiplier)
            if self.min_relative_stop_pct > 0:
                min_relative_stop_pips = (last * self.min_relative_stop_pct) / max(pip_size, 1e-9)
                stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)
            take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)
            self.stop_loss_pips = stop_loss_pips
            self.take_profit_pips = take_profit_pips

            channel_width_atr = channel_width / max(atr, 1e-9)
            if channel_width_atr < self.min_channel_width_atr:
                return self._hold(
                    "channel_too_narrow",
                    {
                        "channel_width_atr": channel_width_atr,
                        "min_channel_width_atr": self.min_channel_width_atr,
                        "atr": atr,
                        **volume_payload,
                    },
                )

            common_meta: dict[str, object] = {
                "indicator": "donchian_breakout",
                "window": self.window,
                "channel_upper": upper,
                "channel_lower": lower,
                "channel_width": channel_width,
                "channel_width_atr": channel_width_atr,
                "atr_window": self.atr_window,
                "atr": atr,
                "atr_pips": atr_pips,
                "atr_multiplier": self.atr_multiplier,
                "risk_reward_ratio": self.risk_reward_ratio,
                "max_spread_pips": spread_limit,
                "max_spread_pips_source": spread_limit_source,
                "pip_size": pip_size,
                "pip_size_source": pip_size_source,
                "min_breakout_atr_ratio": self.min_breakout_atr_ratio,
                "max_breakout_atr_ratio": self.max_breakout_atr_ratio,
                **volume_payload,
            }

            if breakout_up:
                breakout_distance = last - upper
                breakout_atr_ratio = breakout_distance / max(atr, 1e-9)
                if breakout_atr_ratio < self.min_breakout_atr_ratio:
                    return self._hold(
                        "breakout_too_shallow",
                        {
                            **common_meta,
                            "direction": "up",
                            "breakout_atr_ratio": breakout_atr_ratio,
                        },
                    )
                if breakout_atr_ratio > self.max_breakout_atr_ratio:
                    return self._hold(
                        "breakout_overextended",
                        {
                            **common_meta,
                            "direction": "up",
                            "breakout_atr_ratio": breakout_atr_ratio,
                        },
                    )
                confidence = self._confidence(breakout_atr_ratio, channel_width_atr)
                return Signal(
                    side=Side.BUY,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        **common_meta,
                        "direction": "up",
                        "breakout_atr_ratio": breakout_atr_ratio,
                    },
                )

            if breakout_down:
                breakout_distance = lower - last
                breakout_atr_ratio = breakout_distance / max(atr, 1e-9)
                if breakout_atr_ratio < self.min_breakout_atr_ratio:
                    return self._hold(
                        "breakout_too_shallow",
                        {
                            **common_meta,
                            "direction": "down",
                            "breakout_atr_ratio": breakout_atr_ratio,
                        },
                    )
                if breakout_atr_ratio > self.max_breakout_atr_ratio:
                    return self._hold(
                        "breakout_overextended",
                        {
                            **common_meta,
                            "direction": "down",
                            "breakout_atr_ratio": breakout_atr_ratio,
                        },
                    )
                confidence = self._confidence(breakout_atr_ratio, channel_width_atr)
                return Signal(
                    side=Side.SELL,
                    confidence=confidence,
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                    metadata={
                        **common_meta,
                        "direction": "down",
                        "breakout_atr_ratio": breakout_atr_ratio,
                    },
                )

            return self._hold("no_breakout", common_meta)
