from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from dataclasses import dataclass
from collections.abc import Sequence
from datetime import datetime, timezone
import math
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import atr_wilder, ema, rsi_wilder, session_vwap_bands
from xtb_bot.symbols import is_index_symbol, normalize_symbol, resolve_index_market
from xtb_bot.strategies.base import Strategy, StrategyContext


@dataclass(frozen=True, slots=True)
class _Bar:
    ts: float
    open: float
    high: float
    low: float
    close: float


class G2Strategy(Strategy):
    name = "g2"
    _SECONDARY_SESSION_PENALTY = 0.10
    _SECONDARY_VWAP_BIAS_PENALTY = 0.08
    _SECONDARY_VWAP_RECLAIM_PENALTY = 0.10

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.candle_timeframe_sec = max(60, int(float(params.get("g2_candle_timeframe_sec", 60.0))))
        self.resample_mode = str(params.get("g2_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        ignore_sunday_raw = params.get("g2_ignore_sunday_candles", True)
        if isinstance(ignore_sunday_raw, str):
            self.ignore_sunday_candles = ignore_sunday_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.ignore_sunday_candles = bool(ignore_sunday_raw)

        self.ema_fast_window = max(2, int(float(params.get("g2_ema_fast", 10))))
        self.ema_trend_window = max(self.ema_fast_window + 1, int(float(params.get("g2_ema_trend", 50))))
        self.ema_macro_window = max(self.ema_trend_window + 1, int(float(params.get("g2_ema_macro", 200))))

        self.rsi_window = max(2, int(float(params.get("g2_rsi_window", 14))))
        self.rsi_oversold = min(50.0, max(1.0, float(params.get("g2_rsi_oversold", 35.0))))
        self.rsi_overbought = max(50.0, min(99.0, float(params.get("g2_rsi_overbought", 65.0))))

        self.atr_window = max(2, int(float(params.get("g2_atr_window", 14))))
        self.atr_sl_multiplier = max(0.5, float(params.get("g2_atr_sl_multiplier", 2.4)))
        self.risk_reward_ratio = max(1.0, float(params.get("g2_risk_reward_ratio", 2.2)))
        self.min_stop_loss_pips = max(1.0, float(params.get("g2_min_stop_loss_pips", 35.0)))
        self.min_take_profit_pips = max(1.0, float(params.get("g2_min_take_profit_pips", 75.0)))

        allow_shorts_raw = params.get("g2_allow_shorts", False)
        if isinstance(allow_shorts_raw, str):
            self.allow_shorts = allow_shorts_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.allow_shorts = bool(allow_shorts_raw)

        self.min_trend_gap_ratio = max(0.0, float(params.get("g2_min_trend_gap_ratio", 0.00045)))
        self.min_trend_slope_ratio = max(0.0, float(params.get("g2_min_trend_slope_ratio", 0.00008)))
        self.min_macro_slope_ratio = max(0.0, float(params.get("g2_min_macro_slope_ratio", 0.00005)))
        self.min_atr_pct = max(0.0, float(params.get("g2_min_atr_pct", 0.00045)))
        self.min_pullback_depth_atr = max(0.0, float(params.get("g2_min_pullback_depth_atr", 0.12)))
        self.max_pullback_depth_atr = max(
            self.min_pullback_depth_atr,
            float(params.get("g2_max_pullback_depth_atr", 0.65)),
        )
        self.max_trend_ema_breach_atr = max(0.0, float(params.get("g2_max_trend_ema_breach_atr", 0.35)))
        self.max_close_to_fast_ema_distance_atr = max(
            0.0,
            float(params.get("g2_max_close_to_fast_ema_distance_atr", 0.25)),
        )
        self.reclaim_buffer_atr = max(0.0, float(params.get("g2_reclaim_buffer_atr", 0.18)))
        self.recovery_min_close_location = min(
            1.0,
            max(0.0, float(params.get("g2_recovery_min_close_location", 0.55))),
        )

        self.max_spread_pips = max(0.0, float(params.get("g2_max_spread_pips", 0.0)))
        self.max_spread_to_stop_ratio = min(
            1.0,
            max(0.0, float(params.get("g2_max_spread_to_stop_ratio", 0.25))),
        )
        self.max_spread_pips_by_symbol = self._parse_float_mapping(params.get("g2_max_spread_pips_by_symbol"))
        self.vwap_filter_enabled = self._as_bool(params.get("g2_vwap_filter_enabled", True), True)
        self.vwap_reclaim_required = self._as_bool(params.get("g2_vwap_reclaim_required", False), False)
        self.vwap_min_session_bars = max(2, int(float(params.get("g2_vwap_min_session_bars", 8))))
        self.vwap_min_volume_samples = max(1, int(float(params.get("g2_vwap_min_volume_samples", 8))))
        self.confidence_vwap_reclaim_weight = max(
            0.0,
            float(params.get("g2_confidence_vwap_reclaim_weight", 0.06)),
        )

        session_filter_raw = params.get("g2_session_filter_enabled", False)
        if isinstance(session_filter_raw, str):
            self.session_filter_enabled = session_filter_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.session_filter_enabled = bool(session_filter_raw)
        self.session_open_delay_minutes = max(
            0,
            int(float(params.get("g2_session_open_delay_minutes", 0))),
        )
        self.america_session_timezone = self._timezone_name(
            params.get("g2_america_session_timezone"),
            "America/New_York",
        )
        self.europe_session_timezone = self._timezone_name(
            params.get("g2_europe_session_timezone"),
            "Europe/London",
        )
        self.japan_session_timezone = self._timezone_name(
            params.get("g2_japan_session_timezone"),
            "Asia/Tokyo",
        )
        self.australia_session_timezone = self._timezone_name(
            params.get("g2_australia_session_timezone"),
            "Australia/Sydney",
        )
        self.america_session_start_minute_local = self._parse_session_minutes(
            params.get("g2_america_session_start_local", "09:30"),
            9,
            30,
        )
        self.america_session_end_minute_local = self._parse_session_minutes(
            params.get("g2_america_session_end_local", "16:00"),
            16,
            0,
        )
        self.europe_session_start_minute_local = self._parse_session_minutes(
            params.get("g2_europe_session_start_local", "08:00"),
            8,
            0,
        )
        self.europe_session_end_minute_local = self._parse_session_minutes(
            params.get("g2_europe_session_end_local", "17:00"),
            17,
            0,
        )
        self.japan_session_start_minute_local = self._parse_session_minutes(
            params.get("g2_japan_session_start_local", "09:00"),
            9,
            0,
        )
        self.japan_session_end_minute_local = self._parse_session_minutes(
            params.get("g2_japan_session_end_local", "15:00"),
            15,
            0,
        )
        self.australia_session_start_minute_local = self._parse_session_minutes(
            params.get("g2_australia_session_start_local", "10:00"),
            10,
            0,
        )
        self.australia_session_end_minute_local = self._parse_session_minutes(
            params.get("g2_australia_session_end_local", "16:00"),
            16,
            0,
        )

        self.confidence_base = max(0.0, float(params.get("g2_confidence_base", 0.50)))
        self.confidence_trend_gap_weight = max(0.0, float(params.get("g2_confidence_trend_gap_weight", 0.14)))
        self.confidence_slope_weight = max(0.0, float(params.get("g2_confidence_slope_weight", 0.10)))
        self.confidence_pullback_depth_weight = max(
            0.0,
            float(params.get("g2_confidence_pullback_depth_weight", 0.08)),
        )
        self.confidence_recovery_weight = max(0.0, float(params.get("g2_confidence_recovery_weight", 0.10)))
        self.confidence_rsi_recovery_weight = max(
            0.0,
            float(params.get("g2_confidence_rsi_recovery_weight", 0.08)),
        )
        self.confidence_max = min(1.0, max(0.05, float(params.get("g2_confidence_max", 0.90))))
        self.confidence_threshold_cap = min(
            1.0,
            max(0.05, float(params.get("g2_confidence_threshold_cap", 0.82))),
        )

        # Fast MA trailing stop params
        self._trailing_enabled = self._param_bool("g2_fast_ma_trailing_enabled", True)
        self._trailing_activation_r_multiple = self._param_float(
            "g2_fast_ma_trailing_activation_r_multiple", 0.8, min_val=0.0,
        )
        self._trailing_buffer_atr = self._param_float(
            "g2_fast_ma_trailing_buffer_atr", 0.15, min_val=0.0,
        )
        self._trailing_buffer_pips = self._param_float(
            "g2_fast_ma_trailing_buffer_pips", 0.0, min_val=0.0,
        )
        self._trailing_min_step_pips = self._param_float(
            "g2_fast_ma_trailing_min_step_pips", 0.5, min_val=0.0,
        )
        self._trailing_update_cooldown_sec = self._param_float(
            "g2_fast_ma_trailing_update_cooldown_sec", 5.0, min_val=0.0,
        )

        self.min_history = max(self.ema_macro_window + 5, self.rsi_window + 4, self.atr_window + 4)

    def _trailing_payload(self, fast_ma_value: float) -> dict[str, object]:
        if not self._trailing_enabled:
            return {}
        return {
            "trailing_stop": {
                "trailing_enabled": True,
                "trailing_mode": "fast_ma",
                "trailing_activation_r_multiple": self._trailing_activation_r_multiple,
                "trailing_activation_min_profit_pips": 0.0,
                "fast_ma_value": fast_ma_value,
                "fast_ma_source": "fast_ema",
                "fast_ma_use_closed_candle": True,
                "fast_ma_buffer_atr": self._trailing_buffer_atr,
                "fast_ma_buffer_pips": self._trailing_buffer_pips,
                "fast_ma_min_step_pips": self._trailing_min_step_pips,
                "fast_ma_update_cooldown_sec": self._trailing_update_cooldown_sec,
            }
        }

    @staticmethod
    def _timezone_name(raw: object, default: str) -> str:
        name = str(raw or "").strip()
        return name or default

    @staticmethod
    def _parse_float_mapping(raw: object) -> dict[str, float]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            payload = raw
        else:
            text = str(raw).strip()
            if not text:
                return {}
            try:
                import json

                payload = json.loads(text)
            except Exception:
                return {}
        if not isinstance(payload, dict):
            return {}
        result: dict[str, float] = {}
        for key, value in payload.items():
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(parsed) or parsed <= 0.0:
                continue
            result[normalize_symbol(key)] = parsed
        return result

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

    def _raw_bars_from_samples(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[_Bar], str | None]:
        if not timestamps or len(timestamps) != len(prices):
            return [], "timestamps_unavailable"
        bars: list[_Bar] = []
        for raw_price, raw_ts in zip(prices, timestamps):
            ts = self._timestamp_to_seconds(raw_ts)
            if ts <= 0:
                return [], "timestamps_invalid"
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            price = float(raw_price)
            bars.append(_Bar(ts=ts, open=price, high=price, low=price, close=price))
        return bars, None

    @staticmethod
    def _parse_session_minutes(raw: object, default_hour: int, default_minute: int) -> int:
        fallback = ((default_hour % 24) * 60 + (default_minute % 60)) % (24 * 60)
        if raw is None:
            return fallback
        text = str(raw).strip()
        if not text:
            return fallback
        if ":" not in text:
            try:
                hour = int(float(text))
            except (TypeError, ValueError):
                return fallback
            return ((hour % 24) * 60) % (24 * 60)
        hour_text, minute_text = text.split(":", 1)
        try:
            hour = int(hour_text)
            minute = int(minute_text)
        except ValueError:
            return fallback
        if minute < 0 or minute > 59:
            return fallback
        return ((hour % 24) * 60 + minute) % (24 * 60)

    @staticmethod
    def _minute_in_window(minute_of_day: int, start_minute: int, end_minute: int) -> bool:
        current = minute_of_day % (24 * 60)
        start = start_minute % (24 * 60)
        end = end_minute % (24 * 60)
        if start == end:
            return True
        if start < end:
            return start <= current < end
        return current >= start or current < end

    @staticmethod
    def _minutes_since_session_start(current_minute: int, start_minute: int) -> int:
        return (current_minute - start_minute) % (24 * 60)

    @staticmethod
    def _timestamp_to_seconds(raw_ts: float | int | str | None) -> float:
        ts = Strategy._timestamp_to_seconds(raw_ts)
        if not math.isfinite(ts) or ts <= 0:
            return 0.0
        return ts

    def supports_symbol(self, symbol: str) -> bool:
        return is_index_symbol(symbol)

    def _hold(self, reason: str, payload: dict[str, object] | None = None) -> Signal:
        metadata: dict[str, object] = {"indicator": "g2_index_pullback", "reason": reason}
        if payload:
            metadata.update(payload)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.min_stop_loss_pips,
            take_profit_pips=max(self.min_take_profit_pips, self.min_stop_loss_pips * self.risk_reward_ratio),
            metadata=metadata,
        )

    def _market_session_spec(self, symbol: str) -> tuple[str, str, int, int]:
        market = resolve_index_market(symbol)
        if market == "america":
            return (
                "america",
                self.america_session_timezone,
                self.america_session_start_minute_local,
                self.america_session_end_minute_local,
            )
        if market == "japan":
            return (
                "japan",
                self.japan_session_timezone,
                self.japan_session_start_minute_local,
                self.japan_session_end_minute_local,
            )
        if market == "australia":
            return (
                "australia",
                self.australia_session_timezone,
                self.australia_session_start_minute_local,
                self.australia_session_end_minute_local,
            )
        return (
            "europe",
            self.europe_session_timezone,
            self.europe_session_start_minute_local,
            self.europe_session_end_minute_local,
        )

    def _session_allows_entry(self, symbol: str, latest_timestamp: float | None) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {"session_filter_enabled": self.session_filter_enabled}
        if not self.session_filter_enabled:
            payload["session_status"] = "disabled"
            return True, payload
        if latest_timestamp is None or latest_timestamp <= 0:
            payload["session_status"] = "missing_timestamp"
            return False, payload
        profile, timezone_name, start_minute, end_minute = self._market_session_spec(symbol)
        payload["session_profile"] = profile
        payload["session_timezone"] = timezone_name
        payload["session_start_minute_local"] = start_minute
        payload["session_end_minute_local"] = end_minute
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            zone = timezone.utc
            payload["session_timezone_fallback_utc"] = True
        try:
            local_dt = datetime.fromtimestamp(latest_timestamp, tz=zone)
        except (ValueError, OSError, OverflowError):
            payload["session_status"] = "invalid_timestamp"
            return False, payload
        local_minute = local_dt.hour * 60 + local_dt.minute
        payload["session_local_hour"] = local_dt.hour
        payload["session_local_minute"] = local_dt.minute
        payload["session_utc_hour"] = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).hour
        if not self._minute_in_window(local_minute, start_minute, end_minute):
            payload["session_status"] = "outside_window"
            return False, payload
        if self.session_open_delay_minutes > 0:
            minutes_from_open = self._minutes_since_session_start(local_minute, start_minute)
            payload["session_minutes_from_open"] = minutes_from_open
            if minutes_from_open < self.session_open_delay_minutes:
                payload["session_status"] = "open_delay"
                payload["session_open_delay_minutes"] = self.session_open_delay_minutes
                return False, payload
        payload["session_status"] = "ok"
        return True, payload

    def _session_start_timestamp(self, symbol: str, latest_timestamp: float | None) -> float | None:
        if latest_timestamp is None or latest_timestamp <= 0:
            return None
        profile, timezone_name, start_minute, _ = self._market_session_spec(symbol)
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            zone = timezone.utc
        try:
            local_dt = datetime.fromtimestamp(latest_timestamp, tz=zone)
        except (ValueError, OSError, OverflowError):
            return None
        start_hour = start_minute // 60
        start_minute_only = start_minute % 60
        session_start_local = local_dt.replace(
            hour=start_hour,
            minute=start_minute_only,
            second=0,
            microsecond=0,
        )
        current_minute = local_dt.hour * 60 + local_dt.minute
        if current_minute < start_minute:
            from datetime import timedelta

            session_start_local -= timedelta(days=1)
        return session_start_local.astimezone(timezone.utc).timestamp()

    def _resample_closed_candle_ohlc(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[_Bar], bool, str | None]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            bars, error = self._raw_bars_from_samples(prices, timestamps)
            return bars, False, error
        if not timestamps or len(timestamps) != len(prices):
            return [], False, "timestamps_unavailable"

        normalized_timestamps = [self._timestamp_to_seconds(raw_ts) for raw_ts in timestamps]

        bars: list[_Bar] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_price, ts in zip(prices, normalized_timestamps):
            if ts <= 0:
                return [], False, "timestamps_invalid"
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            price = float(raw_price)
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                bars.append(_Bar(ts=ts, open=price, high=price, low=price, close=price))
                last_bucket = bucket
                continue
            previous = bars[-1]
            bars[-1] = _Bar(
                ts=ts,
                open=previous.open,
                high=max(previous.high, price),
                low=min(previous.low, price),
                close=price,
            )

        if len(bars) < 2:
            return [], True, None
        return bars[:-1], True, None

    def _resample_closed_candle_series(
        self,
        values: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [float(value) for value in values], False
        if not timestamps or len(timestamps) != len(values):
            return [], True
        normalized_timestamps = [self._timestamp_to_seconds(raw_ts) for raw_ts in timestamps]
        closes: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_value, ts in zip(values, normalized_timestamps):
            if ts <= 0:
                return [], True
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            value = float(raw_value)
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                closes.append(value)
                last_bucket = bucket
            else:
                closes[-1] = value
        if len(closes) < 2:
            return [], True
        return closes[:-1], True

    def _session_vwap_payload(
        self,
        symbol: str,
        bars: Sequence[_Bar],
        volume_series: Sequence[float],
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "g2_vwap_filter_enabled": self.vwap_filter_enabled,
            "g2_vwap_reclaim_required": self.vwap_reclaim_required,
            "g2_vwap_min_session_bars": self.vwap_min_session_bars,
            "g2_vwap_min_volume_samples": self.vwap_min_volume_samples,
        }
        if not self.vwap_filter_enabled:
            payload["vwap_status"] = "disabled"
            return payload
        if not bars or not volume_series or len(bars) != len(volume_series):
            payload["vwap_status"] = "unavailable"
            return payload
        session_start_ts = self._session_start_timestamp(symbol, bars[-1].ts)
        if session_start_ts is None:
            payload["vwap_status"] = "missing_session_anchor"
            return payload
        payload["vwap_session_start_ts"] = session_start_ts
        session_prices: list[float] = []
        session_volumes: list[float] = []
        for bar, volume in zip(bars, volume_series):
            if bar.ts < session_start_ts:
                continue
            session_prices.append(float(bar.close))
            session_volumes.append(float(volume))
        payload["vwap_session_bars"] = len(session_prices)
        if len(session_prices) < self.vwap_min_session_bars:
            payload["vwap_status"] = "insufficient_session_bars"
            return payload
        vwap_payload = session_vwap_bands(session_prices, session_volumes, band_multipliers=(1.0, 2.0))
        if vwap_payload is None:
            payload["vwap_status"] = "volume_unavailable"
            return payload
        valid_samples = int(round(float(vwap_payload.get("valid_samples", 0.0))))
        payload["vwap_valid_volume_samples"] = valid_samples
        if valid_samples < self.vwap_min_volume_samples:
            payload["vwap_status"] = "insufficient_volume_samples"
            payload.update(vwap_payload)
            return payload
        payload["vwap_status"] = "ok"
        payload.update(vwap_payload)
        return payload

    @staticmethod
    def _close_location(bar: _Bar) -> float:
        bar_range = max(bar.high - bar.low, FLOAT_COMPARISON_TOLERANCE)
        return (bar.close - bar.low) / bar_range

    @staticmethod
    def _triangular_score(value: float, target: float, half_width: float) -> float:
        if half_width <= 0:
            return 0.0
        return max(0.0, 1.0 - abs(value - target) / half_width)

    def _spread_limit_for_symbol(self, symbol: str) -> float:
        override = self.max_spread_pips_by_symbol.get(normalize_symbol(symbol))
        if override is not None and override > 0.0:
            return override
        return self.max_spread_pips

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        if not self.supports_symbol(ctx.symbol):
            return self._hold("symbol_not_index", {"symbol": normalize_symbol(ctx.symbol)})

        raw_prices, raw_timestamps, invalid_prices = self._extract_finite_prices_and_timestamps(
            ctx.prices,
            ctx.timestamps,
            timestamp_normalizer=self._timestamp_to_seconds,
        )
        bars, using_closed_candles, resample_error = self._resample_closed_candle_ohlc(raw_prices, raw_timestamps)
        if resample_error is not None:
            return self._hold(
                "timestamps_unavailable" if resample_error == "timestamps_unavailable" else "timestamps_invalid",
                {
                    "using_closed_candles": using_closed_candles,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "invalid_prices_dropped": invalid_prices,
                },
            )
        if len(bars) < self.min_history:
            return self._hold(
                "insufficient_candle_history" if using_closed_candles else "insufficient_price_history",
                {
                    "prices_len": len(bars),
                    "raw_prices_len": len(raw_prices),
                    "min_history": self.min_history,
                    "using_closed_candles": using_closed_candles,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "invalid_prices_dropped": invalid_prices,
                },
            )

        latest_bar = bars[-1]
        latest_ts = latest_bar.ts
        session_allowed, session_payload = self._session_allows_entry(ctx.symbol, latest_ts)

        closes = [bar.close for bar in bars]
        atr_input = [(bar.open, bar.high, bar.low, bar.close) for bar in bars]
        atr = atr_wilder(atr_input, self.atr_window)
        if atr is None or atr <= 0.0:
            return self._hold("atr_unavailable")

        last_close = latest_bar.close
        atr_pct = atr / max(abs(last_close), FLOAT_COMPARISON_TOLERANCE)
        if atr_pct < self.min_atr_pct:
            return self._hold(
                "low_volatility",
                {
                    "atr": atr,
                    "atr_pct": atr_pct,
                    "g2_min_atr_pct": self.min_atr_pct,
                },
            )

        ema_fast_now = ema(closes, self.ema_fast_window)
        ema_trend_now = ema(closes, self.ema_trend_window)
        ema_macro_now = ema(closes, self.ema_macro_window)
        ema_trend_prev = ema(closes[:-1], self.ema_trend_window)
        ema_macro_prev = ema(closes[:-1], self.ema_macro_window)
        trend_gap_ratio = abs(ema_trend_now - ema_macro_now) / max(abs(last_close), FLOAT_COMPARISON_TOLERANCE)
        trend_slope_ratio = (ema_trend_now - ema_trend_prev) / max(abs(last_close), FLOAT_COMPARISON_TOLERANCE)
        macro_slope_ratio = (ema_macro_now - ema_macro_prev) / max(abs(last_close), FLOAT_COMPARISON_TOLERANCE)

        rsi_now = rsi_wilder(closes, self.rsi_window)
        rsi_prev = rsi_wilder(closes[:-1], self.rsi_window)
        if not math.isfinite(rsi_now) or not math.isfinite(rsi_prev):
            return self._hold("rsi_unavailable")

        prev_bar = bars[-2]
        close_location = self._close_location(latest_bar)
        pullback_depth_buy_atr = max(ema_fast_now - latest_bar.low, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        pullback_depth_sell_atr = max(latest_bar.high - ema_fast_now, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        close_to_fast_buy_atr = max(last_close - ema_fast_now, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        close_to_fast_sell_atr = max(ema_fast_now - last_close, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        trend_breach_buy_atr = max(ema_trend_now - latest_bar.low, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        trend_breach_sell_atr = max(latest_bar.high - ema_trend_now, 0.0) / max(atr, FLOAT_COMPARISON_TOLERANCE)

        bullish_macro = (
            ema_trend_now > ema_macro_now
            and trend_gap_ratio >= self.min_trend_gap_ratio
            and trend_slope_ratio >= self.min_trend_slope_ratio
            and macro_slope_ratio >= self.min_macro_slope_ratio
        )
        bearish_macro = (
            ema_trend_now < ema_macro_now
            and trend_gap_ratio >= self.min_trend_gap_ratio
            and trend_slope_ratio <= -self.min_trend_slope_ratio
            and macro_slope_ratio <= -self.min_macro_slope_ratio
        )

        bullish_recovery = (
            latest_bar.close > ema_fast_now - (atr * self.reclaim_buffer_atr)
            and latest_bar.close > prev_bar.close
            and close_location >= self.recovery_min_close_location
            and pullback_depth_buy_atr >= self.min_pullback_depth_atr
            and pullback_depth_buy_atr <= self.max_pullback_depth_atr
            and trend_breach_buy_atr <= self.max_trend_ema_breach_atr
            and close_to_fast_buy_atr <= self.max_close_to_fast_ema_distance_atr
            and (
                (rsi_prev <= self.rsi_oversold and rsi_now >= self.rsi_oversold)
                or (latest_bar.low <= ema_fast_now and last_close > ema_fast_now and rsi_now > rsi_prev)
            )
        )
        bearish_recovery = (
            latest_bar.close < ema_fast_now + (atr * self.reclaim_buffer_atr)
            and latest_bar.close < prev_bar.close
            and close_location <= (1.0 - self.recovery_min_close_location)
            and pullback_depth_sell_atr >= self.min_pullback_depth_atr
            and pullback_depth_sell_atr <= self.max_pullback_depth_atr
            and trend_breach_sell_atr <= self.max_trend_ema_breach_atr
            and close_to_fast_sell_atr <= self.max_close_to_fast_ema_distance_atr
            and (
                (rsi_prev >= self.rsi_overbought and rsi_now <= self.rsi_overbought)
                or (latest_bar.high >= ema_fast_now and last_close < ema_fast_now and rsi_now < rsi_prev)
            )
        )

        volume_series: list[float] = []
        if raw_timestamps and len(ctx.volumes) == len(raw_prices):
            resampled_volumes, _ = self._resample_closed_candle_series(ctx.volumes, raw_timestamps)
            volume_series = [float(value) for value in resampled_volumes]
        vwap_payload = self._session_vwap_payload(ctx.symbol, bars, volume_series)
        vwap_status = str(vwap_payload.get("vwap_status") or "")
        vwap_available = vwap_status == "ok"
        vwap_price = float(vwap_payload["vwap"]) if vwap_available else None
        buy_vwap_bias_confirmed = vwap_price is None or last_close >= vwap_price
        sell_vwap_bias_confirmed = vwap_price is None or last_close <= vwap_price
        buy_vwap_reclaim_confirmed = (
            vwap_price is not None
            and prev_bar.close <= vwap_price
            and latest_bar.close >= vwap_price
        )
        sell_vwap_reclaim_confirmed = (
            vwap_price is not None
            and prev_bar.close >= vwap_price
            and latest_bar.close <= vwap_price
        )

        side = Side.HOLD
        signal_reason = "waiting_for_pullback"
        if bullish_macro and bullish_recovery:
            side = Side.BUY
            signal_reason = "buy_the_dip"
        elif self.allow_shorts and bearish_macro and bearish_recovery:
            side = Side.SELL
            signal_reason = "sell_the_rally"

        base_payload: dict[str, object] = {
            "ema_fast": ema_fast_now,
            "ema_trend": ema_trend_now,
            "ema_macro": ema_macro_now,
            "trend_gap_ratio": trend_gap_ratio,
            "trend_slope_ratio": trend_slope_ratio,
            "macro_slope_ratio": macro_slope_ratio,
            "rsi_now": rsi_now,
            "rsi_prev": rsi_prev,
            "atr": atr,
            "atr_pct": atr_pct,
            "pullback_depth_buy_atr": pullback_depth_buy_atr,
            "pullback_depth_sell_atr": pullback_depth_sell_atr,
            "trend_breach_buy_atr": trend_breach_buy_atr,
            "trend_breach_sell_atr": trend_breach_sell_atr,
            "close_location": close_location,
            "bullish_macro": bullish_macro,
            "bearish_macro": bearish_macro,
            "bullish_recovery": bullish_recovery,
            "bearish_recovery": bearish_recovery,
            "candle_timeframe_sec": self.candle_timeframe_sec,
            "using_closed_candles": using_closed_candles,
            "session_filter_enabled": self.session_filter_enabled,
            "vwap_bias_confirmed": (
                buy_vwap_bias_confirmed if side != Side.SELL else sell_vwap_bias_confirmed
            ),
            "vwap_reclaim_confirmed": (
                buy_vwap_reclaim_confirmed if side != Side.SELL else sell_vwap_reclaim_confirmed
            ),
            **session_payload,
            **vwap_payload,
        }
        if side == Side.HOLD:
            return self._hold(signal_reason, base_payload)

        session_penalty = 0.0
        session_softened = False
        soft_filter_reasons: list[str] = []
        if not session_allowed:
            session_penalty = self._SECONDARY_SESSION_PENALTY
            session_softened = True
            soft_filter_reasons.append("session_filter_blocked")

        vwap_bias_penalty = 0.0
        vwap_reclaim_penalty = 0.0
        vwap_softened = False
        if self.vwap_filter_enabled and vwap_available:
            if side == Side.BUY:
                if not buy_vwap_bias_confirmed:
                    vwap_bias_penalty = self._SECONDARY_VWAP_BIAS_PENALTY
                    vwap_softened = True
                    soft_filter_reasons.append("vwap_bias_not_confirmed")
                if self.vwap_reclaim_required and not buy_vwap_reclaim_confirmed:
                    vwap_reclaim_penalty = self._SECONDARY_VWAP_RECLAIM_PENALTY
                    vwap_softened = True
                    soft_filter_reasons.append("vwap_reclaim_not_confirmed")
            else:
                if not sell_vwap_bias_confirmed:
                    vwap_bias_penalty = self._SECONDARY_VWAP_BIAS_PENALTY
                    vwap_softened = True
                    soft_filter_reasons.append("vwap_bias_not_confirmed")
                if self.vwap_reclaim_required and not sell_vwap_reclaim_confirmed:
                    vwap_reclaim_penalty = self._SECONDARY_VWAP_RECLAIM_PENALTY
                    vwap_softened = True
                    soft_filter_reasons.append("vwap_reclaim_not_confirmed")

        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if side == Side.BUY:
            stop_anchor = min(latest_bar.low, prev_bar.low, ema_trend_now - (atr * 0.10))
            structural_stop_pips = max(0.0, (last_close - stop_anchor) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))
            pullback_depth_atr = pullback_depth_buy_atr
            rsi_recovery_score = max(0.0, min(1.0, (self.rsi_oversold + 8.0 - min(rsi_prev, rsi_now)) / 8.0))
        else:
            stop_anchor = max(latest_bar.high, prev_bar.high, ema_trend_now + (atr * 0.10))
            structural_stop_pips = max(0.0, (stop_anchor - last_close) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))
            pullback_depth_atr = pullback_depth_sell_atr
            rsi_recovery_score = max(0.0, min(1.0, (max(rsi_prev, rsi_now) - self.rsi_overbought + 8.0) / 8.0))

        stop_loss_pips = max(self.min_stop_loss_pips, atr_pips * self.atr_sl_multiplier, structural_stop_pips)
        spread_pips = max(0.0, float(ctx.current_spread_pips or 0.0))
        max_spread_limit = self._spread_limit_for_symbol(ctx.symbol)
        if max_spread_limit > 0.0 and spread_pips >= max_spread_limit:
            return self._hold(
                "spread_too_wide",
                {
                    **base_payload,
                    "spread_pips": spread_pips,
                    "g2_max_spread_pips": max_spread_limit,
                },
            )
        spread_to_stop_ratio = spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
        if self.max_spread_to_stop_ratio > 0.0 and spread_to_stop_ratio >= self.max_spread_to_stop_ratio:
            return self._hold(
                "spread_too_high_relative_to_stop",
                {
                    **base_payload,
                    "spread_pips": spread_pips,
                    "stop_loss_pips": stop_loss_pips,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "g2_max_spread_to_stop_ratio": self.max_spread_to_stop_ratio,
                },
            )

        pullback_depth_score = self._triangular_score(
            pullback_depth_atr,
            (self.min_pullback_depth_atr + self.max_pullback_depth_atr) / 2.0,
            max(0.15, (self.max_pullback_depth_atr - self.min_pullback_depth_atr) / 2.0),
        )
        trend_gap_score = min(1.0, trend_gap_ratio / max(self.min_trend_gap_ratio * 2.0, FLOAT_COMPARISON_TOLERANCE))
        slope_score = min(
            1.0,
            max(abs(trend_slope_ratio), abs(macro_slope_ratio))
            / max(self.min_trend_slope_ratio * 4.0, FLOAT_COMPARISON_TOLERANCE),
        )
        recovery_score = min(1.0, max(0.0, close_location if side == Side.BUY else (1.0 - close_location)))
        confidence = self.confidence_base
        confidence += trend_gap_score * self.confidence_trend_gap_weight
        confidence += slope_score * self.confidence_slope_weight
        confidence += pullback_depth_score * self.confidence_pullback_depth_weight
        confidence += recovery_score * self.confidence_recovery_weight
        confidence += rsi_recovery_score * self.confidence_rsi_recovery_weight
        if self.vwap_filter_enabled and vwap_available:
            if side == Side.BUY and buy_vwap_reclaim_confirmed:
                confidence += self.confidence_vwap_reclaim_weight
            elif side == Side.SELL and sell_vwap_reclaim_confirmed:
                confidence += self.confidence_vwap_reclaim_weight
        soft_filter_penalty_total = session_penalty + vwap_bias_penalty + vwap_reclaim_penalty
        confidence = max(0.0, confidence - soft_filter_penalty_total)
        confidence = min(self.confidence_max, max(0.0, confidence))

        take_profit_pips = max(self.min_take_profit_pips, stop_loss_pips * self.risk_reward_ratio)
        metadata = {
            "indicator": "g2_index_pullback",
            "signal_reason": signal_reason,
            "trend_signal": "g2_pullback_buy" if side == Side.BUY else "g2_pullback_sell",
            "entry_type": "pullback_recovery",
            "candle_timeframe_sec": self.candle_timeframe_sec,
            "using_closed_candles": using_closed_candles,
            "pip_size": pip_size,
            "pip_size_source": pip_size_source,
            "atr_pips": atr_pips,
            "stop_loss_pips": stop_loss_pips,
            "take_profit_pips": take_profit_pips,
            "risk_reward_ratio": self.risk_reward_ratio,
            "spread_pips": spread_pips,
            "spread_to_stop_ratio": spread_to_stop_ratio,
            "session_penalty": session_penalty,
            "session_softened": session_softened,
            "vwap_bias_penalty": vwap_bias_penalty,
            "vwap_reclaim_penalty": vwap_reclaim_penalty,
            "vwap_softened": vwap_softened,
            "soft_filter_penalty_total": soft_filter_penalty_total,
            "soft_filter_reasons": soft_filter_reasons,
            "soft_filter_count": len(soft_filter_reasons),
            "pullback_depth_atr": pullback_depth_atr,
            "pullback_depth_score": pullback_depth_score,
            "trend_gap_score": trend_gap_score,
            "slope_score": slope_score,
            "recovery_score": recovery_score,
            "rsi_recovery_score": rsi_recovery_score,
            "confidence_threshold_cap": self.confidence_threshold_cap,
            **base_payload,
            **self._trailing_payload(ema_fast_now),
        }
        return Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            metadata=metadata,
        )
