from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Sequence
import json
import math

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, tail_mean
from xtb_bot.strategies.base import Strategy, StrategyContext
from xtb_bot.strategies.pip_size import pip_size as _pip_size_lookup


class MomentumStrategy(Strategy):
    name = "momentum"

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.fast_window = int(params.get("fast_window", 8))
        self.slow_window = int(params.get("slow_window", 21))
        self.ma_type = str(params.get("momentum_ma_type", "sma")).strip().lower()
        if self.ma_type not in {"sma", "ema"}:
            self.ma_type = "sma"
        self.entry_mode = str(params.get("momentum_entry_mode", "cross_only")).strip().lower()
        if self.entry_mode not in {"cross_only", "cross_or_trend"}:
            self.entry_mode = "cross_only"
        self.confirm_bars = max(
            1,
            int(params.get("momentum_confirm_bars", params.get("confirm_bars", 1))),
        )
        self.low_tf_min_confirm_bars = max(
            1,
            int(params.get("momentum_low_tf_min_confirm_bars", 2)),
        )
        self.low_tf_max_confirm_bars = max(
            self.low_tf_min_confirm_bars,
            int(params.get("momentum_low_tf_max_confirm_bars", 3)),
        )
        self.high_tf_max_confirm_bars = max(
            1,
            int(params.get("momentum_high_tf_max_confirm_bars", 1)),
        )
        auto_confirm_raw = params.get("momentum_auto_confirm_by_timeframe", True)
        if isinstance(auto_confirm_raw, str):
            self.auto_confirm_by_timeframe = auto_confirm_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.auto_confirm_by_timeframe = bool(auto_confirm_raw)
        timeframe_raw = params.get("momentum_timeframe_sec")
        self.timeframe_sec = float(timeframe_raw) if timeframe_raw is not None else None
        if self.timeframe_sec is not None and self.timeframe_sec <= 0:
            self.timeframe_sec = None
        max_spread_raw = params.get("momentum_max_spread_pips", params.get("max_spread_pips"))
        self.max_spread_pips = float(max_spread_raw) if max_spread_raw is not None else None
        self.max_spread_pips_by_symbol = self._parse_spread_limits(
            params.get("momentum_max_spread_pips_by_symbol")
        )
        tick_size_guard_raw = params.get("momentum_require_context_tick_size", False)
        if isinstance(tick_size_guard_raw, str):
            self.require_context_tick_size = tick_size_guard_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.require_context_tick_size = bool(tick_size_guard_raw)
        self.base_stop_loss_pips = max(0.1, float(params.get("stop_loss_pips", 25.0)))
        self.base_take_profit_pips = max(0.1, float(params.get("take_profit_pips", 50.0)))
        default_atr_window = 14
        self.atr_window = max(2, int(params.get("momentum_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("momentum_atr_multiplier", 2.0)))
        self.min_stop_loss_pips = max(
            0.1,
            float(params.get("momentum_min_stop_loss_pips", self.base_stop_loss_pips)),
        )
        default_rr = self.base_take_profit_pips / max(self.base_stop_loss_pips, 1e-9)
        self.risk_reward_ratio = max(1.0, float(params.get("momentum_risk_reward_ratio", default_rr)))
        self.min_take_profit_pips = max(
            0.1,
            float(params.get("momentum_min_take_profit_pips", self.base_take_profit_pips)),
        )
        self.min_relative_stop_pct = max(0.0, float(params.get("momentum_min_relative_stop_pct", 0.0008)))
        self.max_price_slow_gap_atr = max(0.1, float(params.get("momentum_max_price_slow_gap_atr", 2.5)))
        self.price_gap_mode = str(params.get("momentum_price_gap_mode", "wait_pullback")).strip().lower()
        if self.price_gap_mode not in {"block", "wait_pullback"}:
            self.price_gap_mode = "wait_pullback"
        self.pullback_entry_max_gap_atr = max(
            0.0,
            float(
                params.get(
                    "momentum_pullback_entry_max_gap_atr",
                    self.max_price_slow_gap_atr,
                )
            ),
        )
        self.confirm_gap_relief_per_bar = max(
            0.0,
            float(params.get("momentum_confirm_gap_relief_per_bar", 0.5)),
        )
        self.min_slope_atr_ratio = max(0.0, float(params.get("momentum_min_slope_atr_ratio", 0.05)))
        self.min_trend_gap_atr = max(0.0, float(params.get("momentum_min_trend_gap_atr", 0.0)))
        session_filter_raw = params.get("momentum_session_filter_enabled", False)
        if isinstance(session_filter_raw, str):
            self.session_filter_enabled = session_filter_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.session_filter_enabled = bool(session_filter_raw)
        self.session_start_hour_utc = int(float(params.get("momentum_session_start_hour_utc", 6))) % 24
        self.session_end_hour_utc = int(float(params.get("momentum_session_end_hour_utc", 22))) % 24
        volume_check_raw = params.get("momentum_volume_confirmation", False)
        if isinstance(volume_check_raw, str):
            self.volume_confirmation = volume_check_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_confirmation = bool(volume_check_raw)
        self.volume_window = max(2, int(params.get("momentum_volume_window", 20)))
        self.volume_min_ratio = max(1.0, float(params.get("momentum_min_volume_ratio", 1.2)))
        self.volume_min_samples = max(1, int(params.get("momentum_volume_min_samples", 5)))
        volume_allow_missing_raw = params.get("momentum_volume_allow_missing", True)
        if isinstance(volume_allow_missing_raw, str):
            self.volume_allow_missing = volume_allow_missing_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.volume_allow_missing = bool(volume_allow_missing_raw)

        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(
            self.min_take_profit_pips,
            self.stop_loss_pips * self.risk_reward_ratio,
        )
        self.min_history = max(
            self.fast_window,
            self.slow_window + self.confirm_bars,
            self.atr_window + 1,
        )

    @staticmethod
    def _prefix_sums(values: Sequence[float]) -> list[float]:
        prefix = [0.0]
        running = 0.0
        for value in values:
            running += float(value)
            prefix.append(running)
        return prefix

    @staticmethod
    def _sma_from_prefix(prefix: Sequence[float], end: int, window: int) -> float:
        return (float(prefix[end]) - float(prefix[end - window])) / float(window)

    def _atr(self, prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _required_indices(total: int, points: int) -> list[int]:
        start_idx = max(0, total - points)
        required = {total - 1, total - 2}
        required.update(range(start_idx, total))
        return sorted(idx for idx in required if idx >= 0)

    def _ma_snapshot_sma(
        self,
        prices: Sequence[float],
        points: int,
    ) -> tuple[float, float, float, float, list[float]]:
        total = len(prices)
        required_indices = self._required_indices(total, points)
        prefix = self._prefix_sums(prices)

        fast_map: dict[int, float] = {}
        slow_map: dict[int, float] = {}
        for idx in required_indices:
            end = idx + 1
            fast_map[idx] = self._sma_from_prefix(prefix, end, self.fast_window)
            slow_map[idx] = self._sma_from_prefix(prefix, end, self.slow_window)

        start_idx = max(0, total - points)
        diffs = [fast_map[idx] - slow_map[idx] for idx in range(start_idx, total)]
        fast_now = fast_map[total - 1]
        slow_now = slow_map[total - 1]
        fast_prev = fast_map[total - 2]
        slow_prev = slow_map[total - 2]
        return fast_now, slow_now, fast_prev, slow_prev, diffs

    def _ma_snapshot_ema(
        self,
        prices: Sequence[float],
        points: int,
    ) -> tuple[float, float, float, float, list[float]]:
        total = len(prices)
        required_indices = set(self._required_indices(total, points))
        alpha_fast = 2.0 / (self.fast_window + 1.0)
        alpha_slow = 2.0 / (self.slow_window + 1.0)

        ema_fast = float(prices[0])
        ema_slow = float(prices[0])
        fast_map: dict[int, float] = {}
        slow_map: dict[int, float] = {}
        if 0 in required_indices:
            fast_map[0] = ema_fast
            slow_map[0] = ema_slow
        for idx in range(1, total):
            price = float(prices[idx])
            ema_fast = alpha_fast * price + (1.0 - alpha_fast) * ema_fast
            ema_slow = alpha_slow * price + (1.0 - alpha_slow) * ema_slow
            if idx in required_indices:
                fast_map[idx] = ema_fast
                slow_map[idx] = ema_slow

        start_idx = max(0, total - points)
        diffs = [fast_map[idx] - slow_map[idx] for idx in range(start_idx, total)]
        fast_now = fast_map[total - 1]
        slow_now = slow_map[total - 1]
        fast_prev = fast_map[total - 2]
        slow_prev = slow_map[total - 2]
        return fast_now, slow_now, fast_prev, slow_prev, diffs

    @staticmethod
    def _pip_size(symbol: str) -> float:
        return _pip_size_lookup(symbol)

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

    @staticmethod
    def _confidence(ema_gap_atr: float, slope_strength_atr: float, price_aligned_with_fast_ma: bool) -> float:
        # Velocity-weighted confidence: slope drives early entries, gap confirms structure.
        gap_component = min(1.0, max(0.0, ema_gap_atr) / 1.5) * 0.3
        slope_component = min(1.0, max(0.0, slope_strength_atr) / 0.1) * 0.5
        price_bonus = 0.2 if price_aligned_with_fast_ma else 0.0
        return min(1.0, 0.1 + gap_component + slope_component + price_bonus)

    @staticmethod
    def _infer_timeframe_sec(timestamps: Sequence[float]) -> float | None:
        if len(timestamps) < 2:
            return None
        deltas = [
            float(timestamps[idx]) - float(timestamps[idx - 1])
            for idx in range(1, len(timestamps))
        ]
        positive = sorted(delta for delta in deltas if delta > 0)
        if not positive:
            return None
        return positive[len(positive) // 2]

    def _resolve_timeframe_sec(self, ctx: StrategyContext) -> float | None:
        if self.timeframe_sec is not None:
            return self.timeframe_sec
        return self._infer_timeframe_sec(ctx.timestamps)

    @staticmethod
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    def _session_allows_entry(self, ctx: StrategyContext) -> tuple[bool, dict[str, object]]:
        if not self.session_filter_enabled:
            return True, {"session_filter_enabled": False}
        if not ctx.timestamps:
            return False, {
                "session_filter_enabled": True,
                "reason": "missing_timestamps",
                "session_start_hour_utc": self.session_start_hour_utc,
                "session_end_hour_utc": self.session_end_hour_utc,
            }
        try:
            raw_ts = float(ctx.timestamps[-1])
            if raw_ts > 10_000_000_000:
                raw_ts = raw_ts / 1000.0
            hour_utc = datetime.fromtimestamp(raw_ts, tz=timezone.utc).hour
        except (ValueError, OSError, OverflowError):
            return False, {
                "session_filter_enabled": True,
                "reason": "invalid_timestamp",
                "session_start_hour_utc": self.session_start_hour_utc,
                "session_end_hour_utc": self.session_end_hour_utc,
            }
        allowed = self._hour_in_window(hour_utc, self.session_start_hour_utc, self.session_end_hour_utc)
        return allowed, {
            "session_filter_enabled": True,
            "session_hour_utc": hour_utc,
            "session_start_hour_utc": self.session_start_hour_utc,
            "session_end_hour_utc": self.session_end_hour_utc,
        }

    def _effective_confirm_bars(self, timeframe_sec: float | None) -> int:
        bars = self.confirm_bars
        if not self.auto_confirm_by_timeframe or timeframe_sec is None:
            return bars
        if timeframe_sec <= 5 * 60:
            bars = max(bars, self.low_tf_min_confirm_bars)
            return min(bars, self.low_tf_max_confirm_bars)
        if timeframe_sec >= 60 * 60:
            return min(bars, self.high_tf_max_confirm_bars)
        return bars

    def _hold_with_reason(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "ma_cross"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=payload,
        )

    def _volume_check(
        self,
        ctx: StrategyContext,
    ) -> tuple[bool, dict[str, object]]:
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

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        signal = self._generate_signal_impl(ctx)
        if signal.side == Side.HOLD and getattr(self, "_last_fast_ma", None) is not None:
            signal.metadata.setdefault("fast_ma", self._last_fast_ma)
            signal.metadata.setdefault("slow_ma", self._last_slow_ma)
        return signal

    def _generate_signal_impl(self, ctx: StrategyContext) -> Signal:
        prices = ctx.prices
        timeframe_sec = self._resolve_timeframe_sec(ctx)
        effective_confirm_bars = self._effective_confirm_bars(timeframe_sec)
        required_history = max(
            self.fast_window,
            self.slow_window + effective_confirm_bars,
            self.atr_window + 1,
        )
        if len(prices) < required_history:
            return self._hold_with_reason(
                "insufficient_price_history",
                {
                    "history": len(prices),
                    "required_history": required_history,
                    "configured_confirm_bars": self.confirm_bars,
                    "effective_confirm_bars": effective_confirm_bars,
                    "timeframe_sec": timeframe_sec,
                },
            )
        if self.require_context_tick_size and ctx.tick_size in (None, 0.0):
            return self._hold_with_reason(
                "tick_size_unavailable",
                {
                    "momentum_require_context_tick_size": True,
                },
            )

        spread_limit, spread_limit_source = self._spread_limit_for_symbol(ctx.symbol)
        if (
            spread_limit is not None
            and ctx.current_spread_pips is not None
            and ctx.current_spread_pips > spread_limit
        ):
            return self._hold_with_reason(
                "spread_too_wide",
                {
                    "spread_pips": ctx.current_spread_pips,
                    "max_spread_pips": spread_limit,
                    "max_spread_pips_source": spread_limit_source,
                },
            )

        session_allowed, session_payload = self._session_allows_entry(ctx)
        if not session_allowed:
            return self._hold_with_reason("outside_trading_session", session_payload)

        if self.ma_type == "ema":
            fast_now, slow_now, fast_prev, slow_prev, diffs = self._ma_snapshot_ema(
                prices, effective_confirm_bars + 1
            )
        else:
            fast_now, slow_now, fast_prev, slow_prev, diffs = self._ma_snapshot_sma(
                prices, effective_confirm_bars + 1
            )
        self._last_fast_ma = fast_now
        self._last_slow_ma = slow_now
        confirmation_tail = diffs[-max(1, effective_confirm_bars) :]
        end = diffs[-1]
        current_price = float(prices[-1])

        bullish_cross_indices = [
            idx
            for idx in range(1, len(diffs))
            if diffs[idx - 1] <= 0 and diffs[idx] > 0
        ]
        bearish_cross_indices = [
            idx
            for idx in range(1, len(diffs))
            if diffs[idx - 1] >= 0 and diffs[idx] < 0
        ]
        latest_bullish_cross = bullish_cross_indices[-1] if bullish_cross_indices else None
        latest_bearish_cross = bearish_cross_indices[-1] if bearish_cross_indices else None

        bullish_cross_age = (
            len(diffs) - latest_bullish_cross
            if latest_bullish_cross is not None
            else None
        )
        bearish_cross_age = (
            len(diffs) - latest_bearish_cross
            if latest_bearish_cross is not None
            else None
        )
        bullish_cross_in_window = bullish_cross_age is not None
        bearish_cross_in_window = bearish_cross_age is not None
        required_cross_bars = 1 if effective_confirm_bars <= 1 else 2

        bullish_confirmed = (
            bullish_cross_age is not None
            and required_cross_bars <= bullish_cross_age <= effective_confirm_bars
            and diffs[latest_bullish_cross:]
            and all(diff > 0 for diff in diffs[latest_bullish_cross:])
        )
        bearish_confirmed = (
            bearish_cross_age is not None
            and required_cross_bars <= bearish_cross_age <= effective_confirm_bars
            and diffs[latest_bearish_cross:]
            and all(diff < 0 for diff in diffs[latest_bearish_cross:])
        )
        bullish_trend = (
            end > 0
            and confirmation_tail
            and all(diff > 0 for diff in confirmation_tail)
            and fast_now >= fast_prev
        )
        bearish_trend = (
            end < 0
            and confirmation_tail
            and all(diff < 0 for diff in confirmation_tail)
            and fast_now <= fast_prev
        )

        candidate_side = Side.HOLD
        trend_signal = "none"
        if bullish_confirmed:
            candidate_side = Side.BUY
            trend_signal = "ma_cross_up"
        elif bearish_confirmed:
            candidate_side = Side.SELL
            trend_signal = "ma_cross_down"
        elif self.entry_mode == "cross_or_trend":
            if bullish_trend:
                candidate_side = Side.BUY
                trend_signal = "ma_trend_up"
            elif bearish_trend:
                candidate_side = Side.SELL
                trend_signal = "ma_trend_down"

        if candidate_side != Side.HOLD:
            volume_ok, volume_payload = self._volume_check(ctx)
            if not volume_ok:
                return self._hold_with_reason("volume_below_threshold", volume_payload)

            atr = self._atr(prices, self.atr_window)
            if atr is None or atr <= 0:
                return self._hold_with_reason("atr_unavailable", {"atr_window": self.atr_window})

            ema_gap_atr = abs(fast_now - slow_now) / max(atr, 1e-9)
            if ema_gap_atr < self.min_trend_gap_atr:
                return self._hold_with_reason(
                    "ma_gap_too_small",
                    {
                        "ema_gap_atr": ema_gap_atr,
                        "min_trend_gap_atr": self.min_trend_gap_atr,
                    },
                )

            if ctx.tick_size not in (None, 0.0):
                pip_size = float(ctx.tick_size)
                pip_size_source = "context_tick_size"
            else:
                pip_size = self._pip_size(ctx.symbol)
                pip_size_source = "fallback_symbol_map"
            atr_pips = atr / max(pip_size, 1e-9)
            stop_loss_pips = max(self.min_stop_loss_pips, atr_pips * self.atr_multiplier)

            if self.min_relative_stop_pct > 0:
                min_relative_stop_pips = (current_price * self.min_relative_stop_pct) / max(pip_size, 1e-9)
                stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)

            take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)

            price_slow_gap_atr = abs(current_price - slow_now) / max(atr, 1e-9)
            gap_relief_atr = max(0.0, float(effective_confirm_bars - 1) * self.confirm_gap_relief_per_bar)
            max_price_slow_gap_atr_effective = self.max_price_slow_gap_atr + gap_relief_atr
            pullback_entry_max_gap_atr_effective = self.pullback_entry_max_gap_atr + gap_relief_atr
            extra_payload = {
                "price_slow_gap_atr": price_slow_gap_atr,
                "max_price_slow_gap_atr": self.max_price_slow_gap_atr,
                "max_price_slow_gap_atr_effective": max_price_slow_gap_atr_effective,
                "pullback_entry_max_gap_atr": self.pullback_entry_max_gap_atr,
                "pullback_entry_max_gap_atr_effective": pullback_entry_max_gap_atr_effective,
                "confirm_gap_relief_per_bar": self.confirm_gap_relief_per_bar,
                "confirm_gap_relief_atr": gap_relief_atr,
                "confirm_bars": effective_confirm_bars,
                "pullback_entry_price": slow_now,
                "pullback_distance_price": abs(current_price - slow_now),
                "price_gap_mode": self.price_gap_mode,
                "suggested_order_type": "limit",
            }
            if self.price_gap_mode == "wait_pullback":
                effective_gap_limit = min(max_price_slow_gap_atr_effective, pullback_entry_max_gap_atr_effective)
                if price_slow_gap_atr > effective_gap_limit:
                    return self._hold_with_reason("wait_pullback_entry", extra_payload)
            elif price_slow_gap_atr > max_price_slow_gap_atr_effective:
                return self._hold_with_reason(
                    "price_too_far_from_slow_ma",
                    extra_payload,
                )

            fast_slope = fast_now - fast_prev
            slow_slope = slow_now - slow_prev
            if candidate_side == Side.BUY:
                if fast_slope <= 0:
                    return self._hold_with_reason(
                        "ma_slope_direction_mismatch",
                        {
                            "direction": "up",
                            "fast_slope": fast_slope,
                            "slow_slope": slow_slope,
                            "slope_filter_mode": "fast_only",
                        },
                    )
            else:
                if fast_slope >= 0:
                    return self._hold_with_reason(
                        "ma_slope_direction_mismatch",
                        {
                            "direction": "down",
                            "fast_slope": fast_slope,
                            "slow_slope": slow_slope,
                            "slope_filter_mode": "fast_only",
                        },
                    )

            slope_strength_atr = abs(fast_slope) / max(atr, 1e-9)
            if slope_strength_atr < self.min_slope_atr_ratio:
                return self._hold_with_reason(
                    "ma_slope_too_flat",
                    {
                        "slope_strength_atr": slope_strength_atr,
                        "min_slope_atr_ratio": self.min_slope_atr_ratio,
                        "slope_filter_mode": "fast_only",
                    },
                )

            if candidate_side == Side.BUY:
                price_aligned_with_fast_ma = current_price > fast_now
            else:
                price_aligned_with_fast_ma = current_price < fast_now
            confidence = self._confidence(ema_gap_atr, slope_strength_atr, price_aligned_with_fast_ma)
            return Signal(
                side=candidate_side,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ma_cross",
                    "trend": "up" if candidate_side == Side.BUY else "down",
                    "trend_signal": trend_signal,
                    "entry_mode": self.entry_mode,
                    "ma_type": self.ma_type,
                    "confirm_bars": effective_confirm_bars,
                    "configured_confirm_bars": self.confirm_bars,
                    "timeframe_sec": timeframe_sec,
                    "atr_window": self.atr_window,
                    "atr_multiplier": self.atr_multiplier,
                    "atr_pips": atr_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "price_slow_gap_atr": price_slow_gap_atr,
                    "pullback_entry_max_gap_atr": self.pullback_entry_max_gap_atr,
                    "pullback_entry_max_gap_atr_effective": pullback_entry_max_gap_atr_effective,
                    "max_price_slow_gap_atr_effective": max_price_slow_gap_atr_effective,
                    "ema_gap_atr": ema_gap_atr,
                    "slope_strength_atr": slope_strength_atr,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "confidence_model": "velocity_weighted_v2",
                    "fast_slope": fast_slope,
                    "slow_slope": slow_slope,
                    "slope_filter_mode": "fast_only",
                    "fast_ma": fast_now,
                    "slow_ma": slow_now,
                    "reverse_exit_supported": True,
                    **session_payload,
                    **volume_payload,
                },
            )

        if bullish_cross_in_window or bearish_cross_in_window:
            direction = "up"
            bars_since_cross = (bullish_cross_age - 1) if bullish_cross_age is not None else None
            if bullish_cross_in_window and bearish_cross_in_window:
                bullish_idx = latest_bullish_cross if latest_bullish_cross is not None else -1
                bearish_idx = latest_bearish_cross if latest_bearish_cross is not None else -1
                if bearish_idx > bullish_idx:
                    direction = "down"
                    bars_since_cross = (bearish_cross_age - 1) if bearish_cross_age is not None else None
            elif bearish_cross_in_window:
                direction = "down"
                bars_since_cross = (bearish_cross_age - 1) if bearish_cross_age is not None else None
            return self._hold_with_reason(
                "cross_not_confirmed",
                {
                    "confirm_bars": effective_confirm_bars,
                    "configured_confirm_bars": self.confirm_bars,
                    "timeframe_sec": timeframe_sec,
                    "direction": direction,
                    "bars_since_cross": bars_since_cross,
                    "entry_mode": self.entry_mode,
                },
            )

        return self._hold_with_reason(
            "no_ma_cross",
            {
                "entry_mode": self.entry_mode,
                "ma_type": self.ma_type,
            },
        )
