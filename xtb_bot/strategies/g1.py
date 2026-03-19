from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from datetime import datetime, timezone
import json
import math
import threading

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import adx_from_close, atr_wilder, ema, ema_last_two
from xtb_bot.strategies.base import Strategy, StrategyContext


@dataclass(frozen=True, slots=True)
class _G1Profile:
    name: str
    fast_ema_window: int
    slow_ema_window: int
    adx_window: int
    adx_threshold: float
    adx_hysteresis: float
    atr_window: int
    atr_multiplier: float
    risk_reward_ratio: float
    max_spread_pips: float
    max_spread_pips_by_symbol: dict[str, float]
    min_stop_loss_pips: float
    max_price_ema_gap_ratio: float


class G1Strategy(Strategy):
    name = "g1"

    _INDEX_SYMBOLS = {
        "US100",
        "US500",
        "US30",
        "DE40",
        "UK100",
        "FRA40",
        "JP225",
        "EU50",
    }

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.profile_override = str(params.get("g1_profile_override", "auto")).strip().lower()
        if self.profile_override not in {"auto", "fx", "index"}:
            self.profile_override = "auto"
        self.candle_timeframe_sec = max(0, int(float(params.get("g1_candle_timeframe_sec", 60.0))))
        self.candle_confirm_bars = max(1, int(float(params.get("g1_candle_confirm_bars", 1))))
        use_incomplete_candle_raw = params.get("g1_use_incomplete_candle_for_entry", False)
        if isinstance(use_incomplete_candle_raw, str):
            self.use_incomplete_candle_for_entry = (
                use_incomplete_candle_raw.strip().lower() not in {"0", "false", "no", "off"}
            )
        else:
            self.use_incomplete_candle_for_entry = bool(use_incomplete_candle_raw)
        self.entry_mode = str(params.get("g1_entry_mode", "cross_or_trend")).strip().lower()
        if self.entry_mode not in {"cross_only", "cross_or_trend"}:
            self.entry_mode = "cross_or_trend"
        self.min_trend_gap_ratio = max(0.0, float(params.get("g1_min_trend_gap_ratio", 0.0)))
        self.min_cross_gap_ratio = max(0.0, float(params.get("g1_min_cross_gap_ratio", 0.0)))
        self.continuation_adx_multiplier = max(0.1, float(params.get("g1_continuation_adx_multiplier", 0.55)))
        self.continuation_min_adx = max(0.0, float(params.get("g1_continuation_min_adx", 0.0)))
        self.adx_warmup_multiplier = max(2.0, float(params.get("g1_adx_warmup_multiplier", 2.0)))
        self.adx_warmup_extra_bars = max(0, int(float(params.get("g1_adx_warmup_extra_bars", 2))))
        self.adx_warmup_cap_bars = max(0, int(float(params.get("g1_adx_warmup_cap_bars", 0))))
        hysteresis_raw = params.get("g1_use_adx_hysteresis_state", True)
        if isinstance(hysteresis_raw, str):
            self.use_adx_hysteresis_state = hysteresis_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.use_adx_hysteresis_state = bool(hysteresis_raw)
        self.resample_mode = str(params.get("g1_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        self.index_low_vol_atr_pct_threshold = max(
            0.0, float(params.get("g1_index_low_vol_atr_pct_threshold", 0.1))
        )
        self.index_low_vol_multiplier = max(1.0, float(params.get("g1_index_low_vol_multiplier", 1.5)))
        self.min_relative_stop_pct = max(0.0, float(params.get("g1_min_relative_stop_pct", 0.0008)))
        ignore_sunday_raw = params.get("g1_ignore_sunday_candles", True)
        if isinstance(ignore_sunday_raw, str):
            self.ignore_sunday_candles = ignore_sunday_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.ignore_sunday_candles = bool(ignore_sunday_raw)
        index_tick_size_guard_raw = params.get("g1_index_require_context_tick_size", False)
        if isinstance(index_tick_size_guard_raw, str):
            self.index_require_context_tick_size = index_tick_size_guard_raw.strip().lower() not in {
                "0",
                "false",
                "no",
                "off",
            }
        else:
            self.index_require_context_tick_size = bool(index_tick_size_guard_raw)
        volume_confirmation_raw = params.get("g1_volume_confirmation", False)
        if isinstance(volume_confirmation_raw, str):
            self.volume_confirmation = volume_confirmation_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_confirmation = bool(volume_confirmation_raw)
        self.volume_window = max(2, int(params.get("g1_volume_window", 20)))
        self.min_volume_ratio = max(0.1, float(params.get("g1_min_volume_ratio", 1.4)))
        self.volume_min_samples = max(1, int(params.get("g1_volume_min_samples", 8)))
        volume_allow_missing_raw = params.get("g1_volume_allow_missing", True)
        if isinstance(volume_allow_missing_raw, str):
            self.volume_allow_missing = volume_allow_missing_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_allow_missing = bool(volume_allow_missing_raw)
        volume_require_spike_raw = params.get("g1_volume_require_spike", False)
        if isinstance(volume_require_spike_raw, str):
            self.volume_require_spike = volume_require_spike_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.volume_require_spike = bool(volume_require_spike_raw)
        self.volume_confidence_boost = max(0.0, float(params.get("g1_volume_confidence_boost", 0.1)))
        self.confidence_velocity_norm_ratio = max(
            1e-6, float(params.get("g1_confidence_velocity_norm_ratio", 0.0005))
        )
        self._adx_regime_active_by_symbol: dict[str, bool] = {}
        self._signal_lock = threading.Lock()

        self.fx_profile = self._build_profile(
            params=params,
            prefix="g1_fx",
            default_name="fx",
            defaults={
                "fast_ema_window": 20,
                "slow_ema_window": 50,
                "adx_window": 14,
                "adx_threshold": 25.0,
                "adx_hysteresis": 2.0,
                "atr_window": 14,
                "atr_multiplier": 2.0,
                "risk_reward_ratio": 3.0,
                "max_spread_pips": 1.0,
                "min_stop_loss_pips": 15.0,
                "max_price_ema_gap_ratio": 0.005,
            },
        )
        self.index_profile = self._build_profile(
            params=params,
            prefix="g1_index",
            default_name="index",
            defaults={
                "fast_ema_window": 20,
                "slow_ema_window": 50,
                "adx_window": 14,
                "adx_threshold": 22.0,
                "adx_hysteresis": 2.0,
                "atr_window": 14,
                "atr_multiplier": 3.0,
                "risk_reward_ratio": 3.0,
                "max_spread_pips": 8.0,
                "min_stop_loss_pips": 50.0,
                "max_price_ema_gap_ratio": 0.008,
            },
        )
        self.min_history = max(
            self._profile_min_history(self.fx_profile),
            self._profile_min_history(self.index_profile),
            self.candle_confirm_bars + max(self.fx_profile.slow_ema_window, self.index_profile.slow_ema_window) + 2,
        )

    @staticmethod
    def _as_int(params: dict[str, object], key: str, fallback_key: str, default: int, min_value: int) -> int:
        raw = params.get(key, params.get(fallback_key, default))
        return max(min_value, int(raw))

    @staticmethod
    def _as_float(
        params: dict[str, object],
        key: str,
        fallback_key: str,
        default: float,
        min_value: float,
    ) -> float:
        raw = params.get(key, params.get(fallback_key, default))
        return max(min_value, float(raw))

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

    @staticmethod
    def _spread_limit_for_symbol(profile: _G1Profile, symbol: str) -> tuple[float, str]:
        upper_symbol = str(symbol).strip().upper()
        override = profile.max_spread_pips_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return profile.max_spread_pips, "global"

    def _build_profile(
        self,
        params: dict[str, object],
        prefix: str,
        default_name: str,
        defaults: dict[str, float | int],
    ) -> _G1Profile:
        fast = self._as_int(
            params,
            f"{prefix}_fast_ema_window",
            "g1_fast_ema_window",
            int(defaults["fast_ema_window"]),
            2,
        )
        slow = self._as_int(
            params,
            f"{prefix}_slow_ema_window",
            "g1_slow_ema_window",
            int(defaults["slow_ema_window"]),
            fast + 1,
        )
        adx_window = self._as_int(
            params,
            f"{prefix}_adx_window",
            "g1_adx_window",
            int(defaults["adx_window"]),
            2,
        )
        atr_window = self._as_int(
            params,
            f"{prefix}_atr_window",
            "g1_atr_window",
            int(defaults["atr_window"]),
            2,
        )
        min_stop_default = float(defaults["min_stop_loss_pips"])
        min_stop = self._as_float(
            params,
            f"{prefix}_min_stop_loss_pips",
            "g1_min_stop_loss_pips",
            min_stop_default,
            0.1,
        )
        if f"{prefix}_min_stop_loss_pips" not in params and "g1_min_stop_loss_pips" not in params:
            min_stop = max(min_stop, float(params.get("stop_loss_pips", min_stop)))

        return _G1Profile(
            name=default_name,
            fast_ema_window=fast,
            slow_ema_window=slow,
            adx_window=adx_window,
            adx_threshold=self._as_float(
                params,
                f"{prefix}_adx_threshold",
                "g1_adx_threshold",
                float(defaults["adx_threshold"]),
                0.0,
            ),
            adx_hysteresis=self._as_float(
                params,
                f"{prefix}_adx_hysteresis",
                "g1_adx_hysteresis",
                float(defaults["adx_hysteresis"]),
                0.0,
            ),
            atr_window=atr_window,
            atr_multiplier=self._as_float(
                params,
                f"{prefix}_atr_multiplier",
                "g1_atr_multiplier",
                float(defaults["atr_multiplier"]),
                0.1,
            ),
            risk_reward_ratio=self._as_float(
                params,
                f"{prefix}_risk_reward_ratio",
                "g1_risk_reward_ratio",
                float(defaults["risk_reward_ratio"]),
                1.0,
            ),
            max_spread_pips=self._as_float(
                params,
                f"{prefix}_max_spread_pips",
                "g1_max_spread_pips",
                float(defaults["max_spread_pips"]),
                0.0,
            ),
            max_spread_pips_by_symbol=self._parse_spread_limits(
                params.get(
                    f"{prefix}_max_spread_pips_by_symbol",
                    params.get("g1_max_spread_pips_by_symbol"),
                )
            ),
            min_stop_loss_pips=min_stop,
            max_price_ema_gap_ratio=self._as_float(
                params,
                f"{prefix}_max_price_ema_gap_ratio",
                "g1_max_price_ema_gap_ratio",
                float(defaults["max_price_ema_gap_ratio"]),
                0.0,
            ),
        )

    def _profile_min_history(self, profile: _G1Profile) -> int:
        adx_required = int(profile.adx_window * self.adx_warmup_multiplier) + self.adx_warmup_extra_bars
        adx_required = max(profile.adx_window + 2, adx_required)
        if self.adx_warmup_cap_bars > 0:
            adx_required = min(adx_required, self.adx_warmup_cap_bars)
        return max(
            profile.slow_ema_window + 2,
            profile.atr_window + 2,
            adx_required,
        )

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _ema_last_two(values: Sequence[float], window: int) -> tuple[float, float]:
        return ema_last_two(values, window)

    @staticmethod
    def _pip_size(symbol: str) -> float:
        upper = symbol.upper()
        if upper in {"AUS200", "AU200"}:
            return 1.0
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

    def _resample_closed_candle_closes(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [float(value) for value in prices], False
        if not timestamps or len(timestamps) != len(prices):
            return [float(value) for value in prices], False
        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and self._is_candle_like_spacing(normalized_timestamps):
            return [float(value) for value in prices], False

        closes: list[float] = []
        last_bucket: int | None = None
        timeframe = max(1, self.candle_timeframe_sec)
        for raw_price, ts in zip(prices, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            price = float(raw_price)
            if last_bucket is None or bucket != last_bucket:
                closes.append(price)
                last_bucket = bucket
            else:
                closes[-1] = price

        if self.use_incomplete_candle_for_entry:
            # Optional low-latency mode: include the in-progress candle for faster entries.
            # Kept opt-in because it can increase signal flicker on noisy symbols.
            return closes, True

        # Default behavior: exclude current in-progress candle to avoid tick-level flickering.
        if len(closes) < 2:
            return [], True
        return closes[:-1], True

    @staticmethod
    def _timestamp_to_seconds(raw_ts: float) -> float:
        ts = float(raw_ts)
        abs_ts = abs(ts)
        if abs_ts > 10_000_000_000_000:
            return ts / 1_000_000.0
        if abs_ts > 10_000_000_000:
            return ts / 1_000.0
        return ts

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
        if min(deltas) < timeframe * 0.5:
            return False
        median_delta = deltas[len(deltas) // 2]
        return median_delta >= timeframe * 0.8

    def _confirmed_by_closed_candles(
        self,
        prices: Sequence[float],
        profile: _G1Profile,
        side: Side,
    ) -> bool:
        bars = max(1, self.candle_confirm_bars)
        if len(prices) < profile.slow_ema_window + bars:
            return False
        for offset in range(bars):
            end = len(prices) - offset
            subset = prices[:end]
            fast = self._ema(subset, profile.fast_ema_window)
            slow = self._ema(subset, profile.slow_ema_window)
            close = float(subset[-1])
            if side == Side.BUY and close <= max(fast, slow):
                return False
            if side == Side.SELL and close >= min(fast, slow):
                return False
        return True

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        upper = symbol.upper()
        if upper in {"AUS200", "AU200"}:
            return True
        if upper in G1Strategy._INDEX_SYMBOLS:
            return True
        if upper.startswith(("US", "DE", "UK", "FRA", "JP", "EU")) and any(ch.isdigit() for ch in upper):
            return True
        return False

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

    def _profile_for_symbol(self, symbol: str) -> _G1Profile:
        if self.profile_override == "fx":
            return self.fx_profile
        if self.profile_override == "index":
            return self.index_profile
        if self._is_index_symbol(symbol):
            return self.index_profile
        return self.fx_profile

    @staticmethod
    def _atr(prices: Sequence[float], window: int) -> float | None:
        return atr_wilder(prices, window)

    @staticmethod
    def _adx(prices: Sequence[float], window: int) -> float | None:
        return adx_from_close(prices, window)

    def _hold(
        self,
        reason: str,
        profile: _G1Profile,
        extra: dict[str, object] | None = None,
    ) -> Signal:
        payload: dict[str, object] = {"reason": reason, "indicator": "g1", "profile": profile.name}
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=profile.min_stop_loss_pips,
            take_profit_pips=profile.min_stop_loss_pips * profile.risk_reward_ratio,
            metadata=payload,
        )

    def _adx_gate_passes(
        self,
        symbol: str,
        adx: float,
        entry_threshold: float,
        exit_threshold: float,
    ) -> tuple[bool, bool, bool]:
        key = symbol.upper()
        active_before = self._adx_regime_active_by_symbol.get(key, False)
        if not self.use_adx_hysteresis_state:
            passes = adx >= entry_threshold
            self._adx_regime_active_by_symbol[key] = passes
            return passes, active_before, passes

        if active_before:
            active_after = adx >= exit_threshold
            self._adx_regime_active_by_symbol[key] = active_after
            return active_after, active_before, active_after

        active_after = adx >= entry_threshold
        self._adx_regime_active_by_symbol[key] = active_after
        return active_after, active_before, active_after

    @staticmethod
    def _slope_ratio(current: float, previous: float) -> float:
        return (current - previous) / max(abs(previous), 1e-9)

    def _confidence(
        self,
        adx: float,
        effective_adx_entry_threshold: float,
        ema_gap_ratio: float,
        ema_gap_norm: float,
        directional_velocity_ratio: float,
        price_aligned_with_fast_ma: bool,
    ) -> float:
        adx_score = min(1.0, adx / max(effective_adx_entry_threshold, 1e-9))
        gap_score = min(1.0, ema_gap_ratio / max(ema_gap_norm, 1e-9))
        velocity_score = min(1.0, directional_velocity_ratio / max(self.confidence_velocity_norm_ratio, 1e-9))
        price_alignment_bonus = 0.08 if price_aligned_with_fast_ma else 0.0
        value = 0.1 + adx_score * 0.40 + gap_score * 0.20 + velocity_score * 0.30 + price_alignment_bonus
        return max(0.05, min(1.0, value))

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        with self._signal_lock:
            return self._generate_signal_locked(ctx)

    def _generate_signal_locked(self, ctx: StrategyContext) -> Signal:
        raw_prices = [float(value) for value in ctx.prices]
        raw_timestamps = [float(value) for value in ctx.timestamps]
        prices, using_closed_candles = self._resample_closed_candle_closes(raw_prices, raw_timestamps)
        profile = self._profile_for_symbol(ctx.symbol)
        min_history = self._profile_min_history(profile)
        if len(prices) < min_history:
            reason = "insufficient_candle_history" if using_closed_candles else "insufficient_price_history"
            return self._hold(
                reason,
                profile,
                {
                    "prices_len": len(prices),
                    "min_history": min_history,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                },
            )
        if self.index_require_context_tick_size and self._is_index_symbol(ctx.symbol) and ctx.tick_size in (None, 0.0):
            return self._hold(
                "tick_size_unavailable",
                profile,
                {"index_require_context_tick_size": True},
            )

        fast_prev, fast_now = self._ema_last_two(prices, profile.fast_ema_window)
        slow_prev, slow_now = self._ema_last_two(prices, profile.slow_ema_window)
        fast_slope_ratio = self._slope_ratio(fast_now, fast_prev)
        slow_slope_ratio = self._slope_ratio(slow_now, slow_prev)
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), 1e-9)

        bullish_cross = fast_prev <= slow_prev and fast_now > slow_now
        bearish_cross = fast_prev >= slow_prev and fast_now < slow_now
        trend_up = fast_now > slow_now and fast_now >= fast_prev
        trend_down = fast_now < slow_now and fast_now <= fast_prev

        trend_signal = ""
        side = Side.HOLD
        if bullish_cross:
            side = Side.BUY
            trend_signal = "ema_cross_up"
        elif bearish_cross:
            side = Side.SELL
            trend_signal = "ema_cross_down"
        elif self.entry_mode == "cross_or_trend":
            if trend_up and ema_gap_ratio >= self.min_trend_gap_ratio:
                side = Side.BUY
                trend_signal = "ema_trend_up"
            elif trend_down and ema_gap_ratio >= self.min_trend_gap_ratio:
                side = Side.SELL
                trend_signal = "ema_trend_down"

        if side == Side.HOLD:
            return self._hold(
                "no_ema_cross",
                profile,
                {
                    "entry_mode": self.entry_mode,
                    "ema_gap_ratio": ema_gap_ratio,
                    "min_trend_gap_ratio": self.min_trend_gap_ratio,
                    "fast_ema": fast_now,
                    "slow_ema": slow_now,
                    "bullish_cross": bullish_cross,
                    "bearish_cross": bearish_cross,
                },
            )
        if trend_signal in {"ema_cross_up", "ema_cross_down"} and ema_gap_ratio < self.min_cross_gap_ratio:
            return self._hold(
                "ema_cross_gap_too_small",
                profile,
                {
                    "trend_signal": trend_signal,
                    "ema_gap_ratio": ema_gap_ratio,
                    "g1_min_cross_gap_ratio": self.min_cross_gap_ratio,
                    "entry_mode": self.entry_mode,
                },
            )

        if not self._confirmed_by_closed_candles(prices, profile, side):
            return self._hold(
                "candle_not_confirmed",
                profile,
                {
                    "candle_confirm_bars": self.candle_confirm_bars,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "using_closed_candles": using_closed_candles,
                    "entry_mode": self.entry_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                },
            )

        adx = self._adx(prices, profile.adx_window)
        if adx is None:
            return self._hold("adx_unavailable", profile)
        adx_entry_threshold = profile.adx_threshold + profile.adx_hysteresis
        adx_exit_threshold = max(0.0, profile.adx_threshold - profile.adx_hysteresis)
        is_continuation_entry = trend_signal in {"ema_trend_up", "ema_trend_down"}
        effective_adx_entry_threshold = adx_entry_threshold
        effective_adx_exit_threshold = adx_exit_threshold
        continuation_adx_floor_applied = 0.0
        if is_continuation_entry:
            raw_effective_adx_entry_threshold = adx_entry_threshold * self.continuation_adx_multiplier
            raw_effective_adx_exit_threshold = adx_exit_threshold * self.continuation_adx_multiplier
            continuation_adx_floor_applied = self.continuation_min_adx
            effective_adx_entry_threshold = max(raw_effective_adx_entry_threshold, self.continuation_min_adx)
            effective_adx_exit_threshold = max(raw_effective_adx_exit_threshold, self.continuation_min_adx)
        adx_gate_passes, adx_regime_active_before, adx_regime_active_after = self._adx_gate_passes(
            ctx.symbol,
            adx,
            effective_adx_entry_threshold,
            effective_adx_exit_threshold,
        )
        if not adx_gate_passes:
            return self._hold(
                "adx_below_threshold",
                profile,
                {
                    "adx": adx,
                    "adx_threshold": profile.adx_threshold,
                    "adx_hysteresis": profile.adx_hysteresis,
                    "adx_entry_threshold": adx_entry_threshold,
                    "adx_exit_threshold": adx_exit_threshold,
                    "effective_adx_entry_threshold": effective_adx_entry_threshold,
                    "effective_adx_exit_threshold": effective_adx_exit_threshold,
                    "is_continuation_entry": is_continuation_entry,
                    "continuation_adx_multiplier": self.continuation_adx_multiplier,
                    "continuation_min_adx": self.continuation_min_adx,
                    "continuation_adx_floor_applied": continuation_adx_floor_applied,
                    "use_adx_hysteresis_state": self.use_adx_hysteresis_state,
                    "adx_regime_active_before": adx_regime_active_before,
                    "adx_regime_active_after": adx_regime_active_after,
                    "fast_ema": fast_now,
                    "slow_ema": slow_now,
                    "ema_gap_ratio": ema_gap_ratio,
                },
            )

        spread_limit, spread_limit_source = self._spread_limit_for_symbol(profile, ctx.symbol)
        if ctx.current_spread_pips is not None and ctx.current_spread_pips > spread_limit:
            return self._hold(
                "spread_too_wide",
                profile,
                {
                    "spread_pips": ctx.current_spread_pips,
                    "max_spread_pips": spread_limit,
                    "max_spread_pips_source": spread_limit_source,
                },
            )

        atr = self._atr(prices, profile.atr_window)
        if atr is None or atr <= 0:
            return self._hold("atr_unavailable", profile)

        price_ema_gap_ratio = abs(prices[-1] - slow_now) / max(abs(slow_now), 1e-9)
        if price_ema_gap_ratio > profile.max_price_ema_gap_ratio:
            return self._hold(
                "price_too_far_from_ema",
                profile,
                {
                    "price_ema_gap_ratio": price_ema_gap_ratio,
                    "max_price_ema_gap_ratio": profile.max_price_ema_gap_ratio,
                },
            )

        if ctx.tick_size not in (None, 0.0):
            pip_size = float(ctx.tick_size)
            pip_size_source = "context_tick_size"
        else:
            pip_size = self._pip_size(ctx.symbol)
            pip_size_source = "fallback_symbol_map"
        current_price = max(abs(prices[-1]), 1e-9)
        atr_percent = (atr / current_price) * 100.0
        dynamic_multiplier = profile.atr_multiplier
        if self._is_index_symbol(ctx.symbol) and atr_percent < self.index_low_vol_atr_pct_threshold:
            dynamic_multiplier *= self.index_low_vol_multiplier

        atr_pips = atr / max(pip_size, 1e-9)
        stop_loss_pips = max(profile.min_stop_loss_pips, atr_pips * dynamic_multiplier)

        min_relative_stop_pips = 0.0
        if self.min_relative_stop_pct > 0.0:
            min_relative_stop_pips = (current_price * self.min_relative_stop_pct) / max(pip_size, 1e-9)
            stop_loss_pips = max(stop_loss_pips, min_relative_stop_pips)

        take_profit_pips = stop_loss_pips * profile.risk_reward_ratio

        ema_gap_norm = max(self.min_trend_gap_ratio, profile.max_price_ema_gap_ratio * 0.5, 1e-4)
        directional_velocity_ratio = (
            max(0.0, fast_slope_ratio) + max(0.0, slow_slope_ratio) * 0.5
            if side == Side.BUY
            else max(0.0, -fast_slope_ratio) + max(0.0, -slow_slope_ratio) * 0.5
        )
        price_aligned_with_fast_ma = prices[-1] >= fast_now if side == Side.BUY else prices[-1] <= fast_now
        confidence = self._confidence(
            adx=adx,
            effective_adx_entry_threshold=effective_adx_entry_threshold,
            ema_gap_ratio=ema_gap_ratio,
            ema_gap_norm=ema_gap_norm,
            directional_velocity_ratio=directional_velocity_ratio,
            price_aligned_with_fast_ma=price_aligned_with_fast_ma,
        )
        volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
        if volume_hold_reason is not None:
            return self._hold(
                volume_hold_reason,
                profile,
                {
                    "trend_signal": trend_signal,
                    "entry_mode": self.entry_mode,
                    "ema_gap_ratio": ema_gap_ratio,
                    **volume_meta,
                },
            )
        confidence = min(1.0, confidence + volume_confidence_boost)

        return Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=stop_loss_pips,
            take_profit_pips=take_profit_pips,
            metadata={
                "indicator": "g1",
                "profile": profile.name,
                "entry_mode": self.entry_mode,
                "use_incomplete_candle_for_entry": self.use_incomplete_candle_for_entry,
                "trend_signal": trend_signal,
                "fast_ema_window": profile.fast_ema_window,
                "slow_ema_window": profile.slow_ema_window,
                "fast_ema": fast_now,
                "slow_ema": slow_now,
                "fast_slope_ratio": fast_slope_ratio,
                "slow_slope_ratio": slow_slope_ratio,
                "directional_velocity_ratio": directional_velocity_ratio,
                "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                "adx": adx,
                "adx_threshold": profile.adx_threshold,
                "adx_hysteresis": profile.adx_hysteresis,
                "adx_entry_threshold": adx_entry_threshold,
                "adx_exit_threshold": adx_exit_threshold,
                "effective_adx_entry_threshold": effective_adx_entry_threshold,
                "effective_adx_exit_threshold": effective_adx_exit_threshold,
                "is_continuation_entry": is_continuation_entry,
                "continuation_adx_multiplier": self.continuation_adx_multiplier,
                "continuation_min_adx": self.continuation_min_adx,
                "continuation_adx_floor_applied": continuation_adx_floor_applied,
                "use_adx_hysteresis_state": self.use_adx_hysteresis_state,
                "adx_regime_active_before": adx_regime_active_before,
                "adx_regime_active_after": adx_regime_active_after,
                "atr": atr,
                "atr_window": profile.atr_window,
                "atr_multiplier": profile.atr_multiplier,
                "dynamic_atr_multiplier": dynamic_multiplier,
                "atr_percent": atr_percent,
                "index_low_vol_atr_pct_threshold": self.index_low_vol_atr_pct_threshold,
                "index_low_vol_multiplier": self.index_low_vol_multiplier,
                "atr_pips": atr_pips,
                "pip_size": pip_size,
                "pip_size_source": pip_size_source,
                "min_relative_stop_pct": self.min_relative_stop_pct,
                "min_relative_stop_pips": min_relative_stop_pips,
                "risk_reward_ratio": profile.risk_reward_ratio,
                "spread_pips": ctx.current_spread_pips,
                "max_spread_pips": spread_limit,
                "max_spread_pips_source": spread_limit_source,
                "price_ema_gap_ratio": price_ema_gap_ratio,
                "max_price_ema_gap_ratio": profile.max_price_ema_gap_ratio,
                "profile_override": self.profile_override,
                "ema_gap_norm": ema_gap_norm,
                "confidence_model": "velocity_weighted_v2",
                "g1_confidence_velocity_norm_ratio": self.confidence_velocity_norm_ratio,
                "g1_min_cross_gap_ratio": self.min_cross_gap_ratio,
                "candle_timeframe_sec": self.candle_timeframe_sec,
                "candle_confirm_bars": self.candle_confirm_bars,
                "using_closed_candles": using_closed_candles,
                "ignore_sunday_candles": self.ignore_sunday_candles,
                "resample_mode": self.resample_mode,
                "adx_warmup_multiplier": self.adx_warmup_multiplier,
                "adx_warmup_extra_bars": self.adx_warmup_extra_bars,
                "volume_confidence_boost_applied": volume_confidence_boost,
                **volume_meta,
            },
        )
