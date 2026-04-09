from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections.abc import Sequence
from datetime import datetime, timezone
import json
import math

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import adx_from_close, atr_wilder, ema, rsi_wilder, tail_mean
from xtb_bot.pip_size import is_fx_symbol
from xtb_bot.symbols import is_index_symbol
from xtb_bot.strategies.base import Strategy, StrategyContext


class DonchianBreakoutStrategy(Strategy):
    name = "donchian_breakout"
    _SOFT_FILTER_PENALTIES: dict[str, float] = {
        "volume_below_threshold": 0.10,
        "volume_unavailable": 0.06,
        "session_filter_blocked": 0.08,
        "rsi_filter_blocked": 0.08,
        "regime_filter_blocked": 0.10,
    }

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.window = max(2, int(params.get("donchian_breakout_window", 20)))
        default_exit_window = max(2, self.window // 2)
        self.exit_window = max(2, int(params.get("donchian_breakout_exit_window", default_exit_window)))
        self.candle_timeframe_sec = max(
            0,
            int(float(params.get("donchian_breakout_candle_timeframe_sec", 60.0))),
        )
        self.resample_mode = str(params.get("donchian_breakout_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        self.ignore_sunday_candles = self._as_bool(
            params.get("donchian_breakout_ignore_sunday_candles", True),
            True,
        )
        self.base_stop_loss_pips = float(
            params.get("donchian_breakout_stop_loss_pips", params.get("stop_loss_pips", 30.0))
        )
        self.base_take_profit_pips = float(
            params.get("donchian_breakout_take_profit_pips", params.get("take_profit_pips", 75.0))
        )
        default_atr_window = min(14, self.window)
        self.atr_window = max(2, int(params.get("donchian_breakout_atr_window", default_atr_window)))
        self.atr_multiplier = max(0.1, float(params.get("donchian_breakout_atr_multiplier", 2.0)))
        default_rr = self.base_take_profit_pips / max(self.base_stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
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

        regime_filter_raw = params.get("donchian_breakout_regime_filter_enabled", False)
        self.regime_filter_enabled = self._as_bool(regime_filter_raw, False)
        self.regime_ema_window = max(
            self.window + 1,
            int(params.get("donchian_breakout_regime_ema_window", max(self.window * 3, 55))),
        )
        self.regime_adx_window = max(
            2,
            int(params.get("donchian_breakout_regime_adx_window", max(8, self.atr_window))),
        )
        self.regime_min_adx = max(
            0.0,
            float(params.get("donchian_breakout_regime_min_adx", 18.0)),
        )
        self.regime_adx_hysteresis = max(
            0.0,
            float(params.get("donchian_breakout_regime_adx_hysteresis", 1.0)),
        )
        self._regime_adx_active_by_symbol: dict[str, bool] = {}
        self.session_filter_enabled = self._as_bool(
            params.get("donchian_breakout_session_filter_enabled", False),
            False,
        )
        self.session_filter_fx_only = self._as_bool(
            params.get("donchian_breakout_session_filter_fx_only", True),
            True,
        )
        self.session_start_hour_utc = int(float(params.get("donchian_breakout_session_start_hour_utc", 6.0))) % 24
        self.session_end_hour_utc = int(float(params.get("donchian_breakout_session_end_hour_utc", 22.0))) % 24
        self.rsi_filter_enabled = self._as_bool(
            params.get("donchian_breakout_rsi_filter_enabled", False),
            False,
        )
        self.rsi_period = max(2, int(params.get("donchian_breakout_rsi_period", 14)))
        self.rsi_buy_max = min(
            100.0,
            max(50.0, float(params.get("donchian_breakout_rsi_buy_max", 82.0))),
        )
        self.rsi_sell_min = max(
            0.0,
            min(50.0, float(params.get("donchian_breakout_rsi_sell_min", 18.0))),
        )
        self.breakout_quality_filter_enabled = self._as_bool(
            params.get("donchian_breakout_breakout_quality_filter_enabled", True),
            True,
        )
        self.breakout_min_body_to_range_ratio = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_breakout_min_body_to_range_ratio", 0.40))),
        )
        self.breakout_buy_min_close_location = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_breakout_buy_min_close_location", 0.60))),
        )
        self.breakout_sell_max_close_location = max(
            0.0,
            min(1.0, float(params.get("donchian_breakout_breakout_sell_max_close_location", 0.40))),
        )

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
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("donchian_breakout_confidence_breakout_target_atr", 0.6)),
        )
        self.confidence_breakout_tolerance_atr = max(
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("donchian_breakout_confidence_breakout_tolerance_atr", 0.6)),
        )
        self.confidence_channel_norm_atr = max(
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("donchian_breakout_confidence_channel_norm_atr", 2.0)),
        )
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(self.min_take_profit_pips, self.stop_loss_pips * self.risk_reward_ratio)
        self.min_history = max(
            self.window + 2,
            self.exit_window + 2,
            self.atr_window + 2,
            (self.regime_ema_window + 1) if self.regime_filter_enabled else 0,
            (self.regime_adx_window + 2) if self.regime_filter_enabled else 0,
            (self.rsi_period + 2) if self.rsi_filter_enabled else 0,
        )

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _ema(prices: Sequence[float], window: int) -> float:
        return ema(prices, window)

    @staticmethod
    def _adx(prices: Sequence[float], window: int) -> float | None:
        return adx_from_close(prices, window)

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        return Strategy._as_bool(value, default, strict_strings=True)

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
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    @staticmethod
    def _rsi(prices: Sequence[float], period: int) -> float:
        return rsi_wilder(prices, period)

    @staticmethod
    def _candle_rejection_metrics(
        *,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
    ) -> tuple[float, float, float, float, float]:
        range_size = max(high_price - low_price, FLOAT_COMPARISON_TOLERANCE)
        body_size = abs(close_price - open_price)
        upper_wick = max(0.0, high_price - max(open_price, close_price))
        lower_wick = max(0.0, min(open_price, close_price) - low_price)
        close_location = (close_price - low_price) / range_size
        return range_size, body_size, upper_wick, lower_wick, close_location

    def _is_candle_like_spacing(self, timestamps_sec: Sequence[float]) -> bool:
        if len(timestamps_sec) < 3:
            return False
        deltas = sorted(
            timestamps_sec[idx] - timestamps_sec[idx - 1]
            for idx in range(1, len(timestamps_sec))
            if (timestamps_sec[idx] - timestamps_sec[idx - 1]) > 0
        )
        if not deltas:
            return False
        timeframe = max(1.0, float(self.candle_timeframe_sec))
        median_delta = deltas[len(deltas) // 2]
        if median_delta < timeframe * 0.8:
            return False
        return median_delta <= timeframe * 1.25

    def _resample_closed_candle_ohlc(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], list[float], list[float], list[float], list[float], bool]:
        closes = [float(value) for value in prices]
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return list(closes), closes, list(closes), list(closes), [float(value) for value in timestamps], False
        if not timestamps or len(timestamps) != len(prices):
            return list(closes), closes, list(closes), list(closes), [float(value) for value in timestamps], False

        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and not self._is_candle_like_spacing(normalized_timestamps):
            return list(closes), closes, list(closes), list(closes), list(normalized_timestamps), False
        candle_opens: list[float] = []
        candle_closes: list[float] = []
        candle_highs: list[float] = []
        candle_lows: list[float] = []
        close_timestamps: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_price, ts in zip(prices, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            price = float(raw_price)
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                candle_opens.append(price)
                candle_closes.append(price)
                candle_highs.append(price)
                candle_lows.append(price)
                close_timestamps.append(float(ts))
                last_bucket = bucket
            else:
                candle_closes[-1] = price
                candle_highs[-1] = max(candle_highs[-1], price)
                candle_lows[-1] = min(candle_lows[-1], price)
                close_timestamps[-1] = float(ts)

        if len(candle_closes) < 2:
            return [], [], [], [], [], True
        return (
            candle_opens[:-1],
            candle_closes[:-1],
            candle_highs[:-1],
            candle_lows[:-1],
            close_timestamps[:-1],
            True,
        )

    def _session_allows_entry(
        self,
        symbol: str,
        latest_timestamp: float | None,
    ) -> tuple[bool, dict[str, object]]:
        enabled = self.session_filter_enabled and (not self.session_filter_fx_only or is_fx_symbol(symbol))
        payload: dict[str, object] = {
            "session_filter_enabled": enabled,
            "session_filter_configured": self.session_filter_enabled,
            "session_filter_fx_only": self.session_filter_fx_only,
            "session_start_hour_utc": self.session_start_hour_utc,
            "session_end_hour_utc": self.session_end_hour_utc,
        }
        if not enabled:
            payload["session_status"] = "disabled"
            return True, payload
        if latest_timestamp is None or latest_timestamp <= 0.0:
            payload["session_status"] = "missing_timestamp"
            return False, payload
        try:
            hour_utc = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).hour
        except (ValueError, OSError, OverflowError):
            payload["session_status"] = "invalid_timestamp"
            return False, payload
        payload["session_hour_utc"] = hour_utc
        allowed = self._hour_in_window(hour_utc, self.session_start_hour_utc, self.session_end_hour_utc)
        payload["session_status"] = "ok" if allowed else "outside_window"
        return allowed, payload

    def _breakout_quality_allows_entry(
        self,
        *,
        side: Side,
        open_price: float | None,
        high_price: float | None,
        low_price: float | None,
        close_price: float | None,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "breakout_quality_filter_enabled": self.breakout_quality_filter_enabled,
            "breakout_min_body_to_range_ratio": self.breakout_min_body_to_range_ratio,
            "breakout_buy_min_close_location": self.breakout_buy_min_close_location,
            "breakout_sell_max_close_location": self.breakout_sell_max_close_location,
        }
        if not self.breakout_quality_filter_enabled:
            payload["breakout_quality_status"] = "disabled"
            return True, payload
        if (
            open_price is None
            or high_price is None
            or low_price is None
            or close_price is None
        ):
            payload["breakout_quality_status"] = "unavailable"
            return True, payload

        range_size, body_size, upper_wick, lower_wick, close_location = self._candle_rejection_metrics(
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )
        body_to_range_ratio = body_size / max(range_size, FLOAT_COMPARISON_TOLERANCE)
        payload.update(
            {
                "breakout_candle_open": open_price,
                "breakout_candle_high": high_price,
                "breakout_candle_low": low_price,
                "breakout_candle_close": close_price,
                "breakout_candle_range_size": range_size,
                "breakout_candle_body_size": body_size,
                "breakout_candle_upper_wick": upper_wick,
                "breakout_candle_lower_wick": lower_wick,
                "breakout_candle_close_location": close_location,
                "breakout_candle_body_to_range_ratio": body_to_range_ratio,
            }
        )
        if body_to_range_ratio + FLOAT_COMPARISON_TOLERANCE < self.breakout_min_body_to_range_ratio:
            payload["breakout_quality_status"] = "body_too_small"
            return False, payload
        if side == Side.BUY:
            if close_location + FLOAT_COMPARISON_TOLERANCE < self.breakout_buy_min_close_location:
                payload["breakout_quality_status"] = "close_location_not_bullish"
                return False, payload
        else:
            if close_location - FLOAT_COMPARISON_TOLERANCE > self.breakout_sell_max_close_location:
                payload["breakout_quality_status"] = "close_location_not_bearish"
                return False, payload
        payload["breakout_quality_status"] = "ok"
        return True, payload

    def _rsi_allows_entry(
        self,
        prices: Sequence[float],
        side: Side,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "rsi_filter_enabled": self.rsi_filter_enabled,
            "rsi_period": self.rsi_period,
            "rsi_buy_max": self.rsi_buy_max,
            "rsi_sell_min": self.rsi_sell_min,
        }
        if not self.rsi_filter_enabled:
            payload["rsi_filter_status"] = "disabled"
            return True, payload
        if len(prices) < 2:
            payload["rsi_filter_status"] = "insufficient_history"
            return False, payload
        rsi = self._rsi(prices, self.rsi_period)
        payload["rsi"] = rsi
        if side == Side.BUY:
            allowed = rsi <= self.rsi_buy_max
            payload["rsi_filter_status"] = "ok" if allowed else "overbought"
            return allowed, payload
        allowed = rsi >= self.rsi_sell_min
        payload["rsi_filter_status"] = "ok" if allowed else "oversold"
        return allowed, payload

    def _hold(
        self,
        reason: str,
        extra: dict[str, object] | None = None,
        *,
        stop_loss_pips: float | None = None,
        take_profit_pips: float | None = None,
    ) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "donchian_breakout"}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips if stop_loss_pips is None else float(stop_loss_pips),
            take_profit_pips=self.take_profit_pips if take_profit_pips is None else float(take_profit_pips),
            metadata=payload,
        )

    def _soft_filter_penalty(self, reason: str | None) -> float:
        if reason is None:
            return 0.0
        return self._SOFT_FILTER_PENALTIES.get(str(reason).strip().lower(), 0.0)

    @staticmethod
    def _volume_hold_reason_from_payload(volume_ok: bool, payload: dict[str, object]) -> str | None:
        if volume_ok:
            return None
        status = str(payload.get("volume_check_status") or "").strip().lower()
        if status == "below_threshold":
            return "volume_below_threshold"
        return "volume_unavailable"

    def _volume_check(self, ctx: StrategyContext) -> tuple[bool, dict[str, object]]:
        index_missing_volume_relaxed = is_index_symbol(ctx.symbol)
        effective_volume_allow_missing = self.volume_allow_missing or index_missing_volume_relaxed
        payload: dict[str, object] = {
            "volume_confirmation": self.volume_confirmation,
            "volume_window": self.volume_window,
            "volume_min_ratio": self.volume_min_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_allow_missing_effective": effective_volume_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not self.volume_allow_missing,
        }
        if not self.volume_confirmation:
            payload["volume_check_status"] = "disabled"
            return True, payload

        current_volume = self._coerce_positive_finite(ctx.current_volume)
        history = self._extract_positive_finite_values(ctx.volumes)
        payload["current_volume"] = current_volume
        payload["volume_history_len"] = len(history)

        if current_volume is None or current_volume <= 0:
            payload["volume_check_status"] = "missing_current_volume"
            return effective_volume_allow_missing, payload

        volume_current_source = "context_current_volume"
        volume_current_deduplicated = False
        if history and self._values_close(history[-1], current_volume):
            current_volume = history[-1]
            history = history[:-1]
            volume_current_source = "ctx_volumes_last"
            volume_current_deduplicated = True
        payload["volume_current_source"] = volume_current_source
        payload["volume_current_deduplicated"] = volume_current_deduplicated
        payload["volume_baseline_history_len"] = len(history)

        lookback = min(self.volume_window, len(history))
        if lookback < self.volume_min_samples:
            payload["volume_check_status"] = "insufficient_volume_history"
            payload["volume_lookback"] = lookback
            return effective_volume_allow_missing, payload

        avg_volume = tail_mean(history, lookback)
        threshold = avg_volume * self.volume_min_ratio
        payload["volume_check_status"] = "ok" if current_volume >= threshold else "below_threshold"
        payload["avg_volume"] = avg_volume
        payload["min_required_volume"] = threshold
        payload["volume_lookback"] = lookback
        payload["volume_ratio"] = current_volume / max(avg_volume, FLOAT_COMPARISON_TOLERANCE)
        return current_volume >= threshold, payload

    def _regime_filter(self, prices: Sequence[float], side: Side) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "regime_filter_enabled": self.regime_filter_enabled,
            "regime_ema_window": self.regime_ema_window,
            "regime_adx_window": self.regime_adx_window,
            "regime_min_adx": self.regime_min_adx,
            "regime_adx_hysteresis": self.regime_adx_hysteresis,
        }
        if not self.regime_filter_enabled:
            payload["regime_filter_status"] = "disabled"
            return True, payload

        ema_now = self._ema(prices, self.regime_ema_window)
        ema_prev = self._ema(prices[:-1], self.regime_ema_window)
        adx = self._adx(prices, self.regime_adx_window)
        if not math.isfinite(ema_now) or not math.isfinite(ema_prev):
            payload["regime_filter_status"] = "ema_unavailable"
            return False, payload
        if adx is None or not math.isfinite(adx):
            payload["regime_filter_status"] = "adx_unavailable"
            return False, payload

        last = float(prices[-1])
        ema_slope = ema_now - ema_prev
        trend_up = last > ema_now
        trend_down = last < ema_now
        active_before = self._regime_adx_active_by_symbol.get("global", False)
        entry_threshold = self.regime_min_adx + self.regime_adx_hysteresis
        exit_threshold = max(0.0, self.regime_min_adx - self.regime_adx_hysteresis)
        if active_before:
            adx_ok = adx >= exit_threshold
        else:
            adx_ok = adx >= entry_threshold
        self._regime_adx_active_by_symbol["global"] = adx_ok
        direction_ok = trend_up if side == Side.BUY else trend_down

        payload.update(
            {
                "regime_ema": ema_now,
                "regime_ema_prev": ema_prev,
                "regime_ema_slope": ema_slope,
                "regime_adx": adx,
                "regime_adx_active_before": active_before,
                "regime_adx_active_after": adx_ok,
                "regime_adx_entry_threshold": entry_threshold,
                "regime_adx_exit_threshold": exit_threshold,
                "regime_trend_up": trend_up,
                "regime_trend_down": trend_down,
                "regime_direction_ok": direction_ok,
                "regime_adx_ok": adx_ok,
                "regime_price_vs_ema_ratio": (last - ema_now) / max(abs(ema_now), FLOAT_COMPARISON_TOLERANCE),
            }
        )

        if direction_ok and adx_ok:
            payload["regime_filter_status"] = "ok"
            return True, payload
        if not direction_ok and not adx_ok:
            payload["regime_filter_status"] = "direction_and_adx_blocked"
            return False, payload
        if not direction_ok:
            payload["regime_filter_status"] = "direction_blocked"
            return False, payload
        payload["regime_filter_status"] = "adx_below_threshold"
        return False, payload

    def _raw_confidence(self, breakout_atr_ratio: float, channel_width_atr: float) -> float:
        breakout_target = self.confidence_breakout_target_atr
        breakout_tolerance = self.confidence_breakout_tolerance_atr
        breakout_quality = 1.0 - min(1.0, abs(breakout_atr_ratio - breakout_target) / breakout_tolerance)
        channel_score = min(1.0, max(0.0, channel_width_atr) / self.confidence_channel_norm_atr)
        extension_score = min(1.0, max(0.0, breakout_atr_ratio) / max(self.max_breakout_atr_ratio, FLOAT_COMPARISON_TOLERANCE))
        raw = (
            self.confidence_base
            + breakout_quality * self.confidence_breakout_weight
            + channel_score * self.confidence_channel_weight
            + extension_score * self.confidence_extension_weight
        )
        return max(0.0, min(1.0, raw))

    def _confidence(
        self,
        breakout_atr_ratio: float,
        channel_width_atr: float,
        *,
        regime_ok: bool,
        volume_ok: bool,
    ) -> tuple[float, dict[str, object]]:
        raw = self._raw_confidence(breakout_atr_ratio, channel_width_atr)
        if regime_ok:
            raw += 0.08
        if self.volume_confirmation and volume_ok:
            raw += 0.03
        raw = max(0.0, min(1.0, raw))
        if raw >= 0.80:
            confidence = 0.82
            tier = "high"
        elif raw >= 0.62:
            confidence = 0.72
            tier = "confirmed"
        else:
            confidence = 0.62
            tier = "base"
        return confidence, {
            "confidence_model": "tiered_breakout_v2",
            "confidence_raw": raw,
            "confidence_tier": tier,
        }

    def _trailing_stop_payload(
        self,
        *,
        exit_channel_upper: float,
        exit_channel_lower: float,
        direction: str | None = None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "trailing_enabled": True,
            "trailing_mode": "fast_ma",
            "trailing_activation_r_multiple": 0.0,
            "trailing_activation_min_profit_pips": 0.0,
            "fast_ma_buffer_atr": 0.0,
            "fast_ma_buffer_pips": 0.0,
            "fast_ma_min_step_pips": 0.0,
            "fast_ma_update_cooldown_sec": 0.0,
            "anchor_source": "donchian_exit_channel",
            "exit_window": self.exit_window,
            "long_anchor_value": exit_channel_lower,
            "short_anchor_value": exit_channel_upper,
        }
        if direction == "up":
            payload["fast_ma_value"] = exit_channel_lower
            payload["fast_ma_source"] = "donchian_exit_channel_long"
        elif direction == "down":
            payload["fast_ma_value"] = exit_channel_upper
            payload["fast_ma_source"] = "donchian_exit_channel_short"
        return payload

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raw_prices, raw_timestamps, invalid_prices = self._extract_finite_prices_and_timestamps(
            ctx.prices,
            ctx.timestamps,
            timestamp_normalizer=self._timestamp_to_seconds,
        )
        candle_opens, candle_closes, candle_highs, candle_lows, close_timestamps, using_closed_candles = (
            self._resample_closed_candle_ohlc(raw_prices, raw_timestamps)
        )
        prices = candle_closes if using_closed_candles and candle_closes else raw_prices
        latest_timestamp = (
            close_timestamps[-1]
            if using_closed_candles and close_timestamps
            else (raw_timestamps[-1] if raw_timestamps else None)
        )
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                {
                    "history": len(prices),
                    "required_history": self.min_history,
                    "invalid_prices_dropped": invalid_prices,
                    "raw_history": len(raw_prices),
                    "resampled_history": len(candle_closes),
                    "using_closed_candles": using_closed_candles,
                },
            )

        reference = prices[-(self.window + 1) : -1]
        exit_reference = prices[-(self.exit_window + 1) : -1]
        upper = float(max(reference))
        lower = float(min(reference))
        exit_upper = float(max(exit_reference))
        exit_lower = float(min(exit_reference))
        prev = float(prices[-2])
        last = float(prices[-1])
        last_open = float(candle_opens[-1]) if using_closed_candles and len(candle_opens) == len(prices) else None
        last_high = float(candle_highs[-1]) if using_closed_candles and len(candle_highs) == len(prices) else None
        last_low = float(candle_lows[-1]) if using_closed_candles and len(candle_lows) == len(prices) else None
        channel_width = upper - lower
        early_trailing_meta = {
            "exit_window": self.exit_window,
            "exit_channel_upper": exit_upper,
            "exit_channel_lower": exit_lower,
            "raw_history": len(raw_prices),
            "resampled_history": len(candle_closes),
            "using_closed_candles": using_closed_candles,
            "candle_timeframe_sec": self.candle_timeframe_sec,
            "resample_mode": self.resample_mode,
            "trailing_stop": self._trailing_stop_payload(
                exit_channel_upper=exit_upper,
                exit_channel_lower=exit_lower,
            ),
        }

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
                    **early_trailing_meta,
                },
            )

        volume_ok, volume_payload = self._volume_check(ctx)
        volume_hold_reason = self._volume_hold_reason_from_payload(volume_ok, volume_payload)
        volume_softened = volume_hold_reason is not None
        volume_penalty = self._soft_filter_penalty(volume_hold_reason)

        atr = self._atr(prices, self.atr_window)
        if atr is None or atr <= 0:
            return self._hold(
                "atr_unavailable",
                {"atr_window": self.atr_window, **early_trailing_meta},
            )

        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)

        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        stop_loss_pips = max(self.min_stop_loss_pips, atr_pips * self.atr_multiplier)
        if self.min_relative_stop_pct > 0:
            min_relative_stop_pips = (last * self.min_relative_stop_pct) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)
        take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)

        channel_width_atr = channel_width / max(atr, FLOAT_COMPARISON_TOLERANCE)
        if channel_width_atr < self.min_channel_width_atr:
            return self._hold(
                "channel_too_narrow",
                {
                    "channel_width_atr": channel_width_atr,
                    "min_channel_width_atr": self.min_channel_width_atr,
                    "atr": atr,
                    **volume_payload,
                },
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
            )

        common_meta: dict[str, object] = {
            "indicator": "donchian_breakout",
            "window": self.window,
            "exit_window": self.exit_window,
            "raw_history": len(raw_prices),
            "resampled_history": len(candle_closes),
            "using_closed_candles": using_closed_candles,
            "candle_timeframe_sec": self.candle_timeframe_sec,
            "resample_mode": self.resample_mode,
            "latest_timestamp": latest_timestamp,
            "channel_upper": upper,
            "channel_lower": lower,
            "exit_channel_upper": exit_upper,
            "exit_channel_lower": exit_lower,
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
            "invalid_prices_dropped": invalid_prices,
            "trailing_stop": self._trailing_stop_payload(
                exit_channel_upper=exit_upper,
                exit_channel_lower=exit_lower,
            ),
            "volume_softened": volume_softened,
            **volume_payload,
        }

        if breakout_up:
            soft_filter_reasons: list[str] = []
            soft_filter_penalty_total = volume_penalty
            if volume_hold_reason is not None:
                soft_filter_reasons.append(volume_hold_reason)
            session_ok, session_payload = self._session_allows_entry(ctx.symbol, latest_timestamp)
            if not session_ok:
                soft_filter_reasons.append("session_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("session_filter_blocked")
            quality_ok, quality_payload = self._breakout_quality_allows_entry(
                side=Side.BUY,
                open_price=last_open,
                high_price=last_high,
                low_price=last_low,
                close_price=last,
            )
            if not quality_ok:
                return self._hold(
                    "breakout_candle_weak",
                    {
                        **common_meta,
                        "direction": "up",
                        **session_payload,
                        **quality_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
            )
            rsi_ok, rsi_payload = self._rsi_allows_entry(prices, Side.BUY)
            if not rsi_ok:
                soft_filter_reasons.append("rsi_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("rsi_filter_blocked")
            breakout_distance = last - upper
            breakout_atr_ratio = breakout_distance / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if breakout_atr_ratio < self.min_breakout_atr_ratio:
                return self._hold(
                    "breakout_too_shallow",
                    {
                        **common_meta,
                        "direction": "up",
                        "breakout_atr_ratio": breakout_atr_ratio,
                        **session_payload,
                        **quality_payload,
                        **rsi_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                )
            if breakout_atr_ratio > self.max_breakout_atr_ratio:
                return self._hold(
                    "breakout_overextended",
                    {
                        **common_meta,
                        "direction": "up",
                        "breakout_atr_ratio": breakout_atr_ratio,
                        **session_payload,
                        **quality_payload,
                        **rsi_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                )
            regime_ok, regime_payload = self._regime_filter(prices, Side.BUY)
            if not regime_ok:
                soft_filter_reasons.append("regime_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("regime_filter_blocked")
            confidence, confidence_meta = self._confidence(
                breakout_atr_ratio,
                channel_width_atr,
                regime_ok=regime_ok,
                volume_ok=volume_ok,
            )
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            signal = Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    **common_meta,
                    "direction": "up",
                    "breakout_atr_ratio": breakout_atr_ratio,
                    "reverse_exit_supported": True,
                    "trailing_stop": self._trailing_stop_payload(
                        exit_channel_upper=exit_upper,
                        exit_channel_lower=exit_lower,
                        direction="up",
                    ),
                    **session_payload,
                    **quality_payload,
                    **rsi_payload,
                    **regime_payload,
                    **confidence_meta,
                    "session_softened": not session_ok,
                    "rsi_softened": not rsi_ok,
                    "regime_softened": not regime_ok,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    "confidence_raw_after_soft_penalties": confidence,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if breakout_down:
            soft_filter_reasons: list[str] = []
            soft_filter_penalty_total = volume_penalty
            if volume_hold_reason is not None:
                soft_filter_reasons.append(volume_hold_reason)
            session_ok, session_payload = self._session_allows_entry(ctx.symbol, latest_timestamp)
            if not session_ok:
                soft_filter_reasons.append("session_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("session_filter_blocked")
            quality_ok, quality_payload = self._breakout_quality_allows_entry(
                side=Side.SELL,
                open_price=last_open,
                high_price=last_high,
                low_price=last_low,
                close_price=last,
            )
            if not quality_ok:
                return self._hold(
                    "breakout_candle_weak",
                    {
                        **common_meta,
                        "direction": "down",
                        **session_payload,
                        **quality_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
            )
            rsi_ok, rsi_payload = self._rsi_allows_entry(prices, Side.SELL)
            if not rsi_ok:
                soft_filter_reasons.append("rsi_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("rsi_filter_blocked")
            breakout_distance = lower - last
            breakout_atr_ratio = breakout_distance / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if breakout_atr_ratio < self.min_breakout_atr_ratio:
                return self._hold(
                    "breakout_too_shallow",
                    {
                        **common_meta,
                        "direction": "down",
                        "breakout_atr_ratio": breakout_atr_ratio,
                        **session_payload,
                        **quality_payload,
                        **rsi_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                )
            if breakout_atr_ratio > self.max_breakout_atr_ratio:
                return self._hold(
                    "breakout_overextended",
                    {
                        **common_meta,
                        "direction": "down",
                        "breakout_atr_ratio": breakout_atr_ratio,
                        **session_payload,
                        **quality_payload,
                        **rsi_payload,
                    },
                    stop_loss_pips=stop_loss_pips,
                    take_profit_pips=take_profit_pips,
                )
            regime_ok, regime_payload = self._regime_filter(prices, Side.SELL)
            if not regime_ok:
                soft_filter_reasons.append("regime_filter_blocked")
                soft_filter_penalty_total += self._soft_filter_penalty("regime_filter_blocked")
            confidence, confidence_meta = self._confidence(
                breakout_atr_ratio,
                channel_width_atr,
                regime_ok=regime_ok,
                volume_ok=volume_ok,
            )
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            signal = Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    **common_meta,
                    "direction": "down",
                    "breakout_atr_ratio": breakout_atr_ratio,
                    "reverse_exit_supported": True,
                    "trailing_stop": self._trailing_stop_payload(
                        exit_channel_upper=exit_upper,
                        exit_channel_lower=exit_lower,
                        direction="down",
                    ),
                    **session_payload,
                    **quality_payload,
                    **rsi_payload,
                    **regime_payload,
                    **confidence_meta,
                    "session_softened": not session_ok,
                    "rsi_softened": not rsi_ok,
                    "regime_softened": not regime_ok,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    "confidence_raw_after_soft_penalties": confidence,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        return self._hold(
            "no_breakout",
            common_meta,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
        )
