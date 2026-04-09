from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

from collections.abc import Sequence
from datetime import datetime, timezone
import inspect
import json
import math

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import adx_from_close, atr_wilder, efficiency_ratio, ema, kama_last_two
from xtb_bot.symbols import is_index_symbol
from xtb_bot.strategies.base import Strategy, StrategyContext


class TrendFollowingStrategy(Strategy):
    name = "trend_following"
    _SECONDARY_TREND_GAP_PENALTY = 0.12
    _SECONDARY_TREND_SLOPE_PENALTY = 0.10
    _SECONDARY_KAMA_PENALTY = 0.12
    _SECONDARY_VOLUME_PENALTY = 0.10
    _SECONDARY_VOLUME_UNAVAILABLE_PENALTY = 0.12
    _CRYPTO_BASES = {
        "BTC",
        "ETH",
        "LTC",
        "BCH",
        "DOGE",
        "XRP",
        "SOL",
        "ADA",
        "DOT",
        "LINK",
        "UNI",
        "AVAX",
        "MATIC",
        "SHIB",
        "TRX",
        "XLM",
        "ATOM",
        "NEAR",
        "APT",
        "ARB",
        "OP",
        "FIL",
        "ETC",
        "XMR",
        "ALGO",
        "AAVE",
        "SUI",
        "PEPE",
        "BONK",
        "INJ",
        "RNDR",
        "TON",
    }

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
        self.pullback_ema_tolerance_atr_multiplier = max(
            0.0,
            float(params.get("trend_pullback_ema_tolerance_atr_multiplier", 0.0)),
        )
        self.slope_window = max(2, int(params.get("trend_slope_window", 4)))
        self.crypto_pullback_ema_tolerance_ratio = max(
            0.0, float(params.get("trend_crypto_pullback_ema_tolerance_ratio", 0.0))
        )
        self.fx_min_ema_gap_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_fx_min_ema_gap_ratio",
                    params.get("trend_min_ema_gap_ratio", 0.0002),
                )
            ),
        )
        self.index_min_ema_gap_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_index_min_ema_gap_ratio",
                    params.get("trend_min_ema_gap_ratio", 0.00025),
                )
            ),
        )
        self.fx_min_fast_slope_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_fx_min_fast_slope_ratio",
                    params.get("trend_min_fast_slope_ratio", 0.00003),
                )
            ),
        )
        self.index_min_fast_slope_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_index_min_fast_slope_ratio",
                    params.get("trend_min_fast_slope_ratio", 0.00002),
                )
            ),
        )
        self.fx_min_slow_slope_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_fx_min_slow_slope_ratio",
                    params.get("trend_min_slow_slope_ratio", 0.00001),
                )
            ),
        )
        self.index_min_slow_slope_ratio = max(
            0.0,
            float(
                params.get(
                    "trend_index_min_slow_slope_ratio",
                    params.get("trend_min_slow_slope_ratio", 0.00001),
                )
            ),
        )
        self.slow_slope_tolerance_ratio_cap_non_crypto = max(
            0.0,
            float(
                params.get(
                    "trend_slow_slope_tolerance_ratio_cap_non_crypto",
                    0.00025,
                )
            ),
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
        self.crypto_min_atr_pct_baseline_sec = max(
            1.0, float(params.get("trend_crypto_min_atr_pct_baseline_sec", 60.0))
        )
        self.crypto_min_atr_pct_min_ratio = max(
            0.0, min(1.0, float(params.get("trend_crypto_min_atr_pct_min_ratio", 0.25)))
        )
        self.crypto_min_atr_pct_scale_mode = str(
            params.get("trend_crypto_min_atr_pct_scale_mode", "linear")
        ).strip().lower()
        if self.crypto_min_atr_pct_scale_mode not in {"linear", "sqrt", "none"}:
            self.crypto_min_atr_pct_scale_mode = "linear"
        self.crypto_default_pip_size = max(
            FLOAT_COMPARISON_TOLERANCE,
            float(params.get("trend_crypto_default_pip_size", 1.0)),
        )
        crypto_allow_pair_default_raw = params.get("trend_crypto_allow_pair_default_pip_size", False)
        if isinstance(crypto_allow_pair_default_raw, str):
            self.crypto_allow_pair_default_pip_size = (
                crypto_allow_pair_default_raw.strip().lower() not in {"0", "false", "no", "off"}
            )
        else:
            self.crypto_allow_pair_default_pip_size = bool(crypto_allow_pair_default_raw)
        self.slope_mode = str(params.get("trend_slope_mode", "fast_with_slow_tolerance")).strip().lower()
        if self.slope_mode not in {"fast_only", "fast_and_slow", "fast_with_slow_tolerance"}:
            self.slope_mode = "fast_with_slow_tolerance"
        self.slow_slope_tolerance_ratio = max(
            0.0, float(params.get("trend_slow_slope_tolerance_ratio", 0.0003))
        )
        self.kama_gate_enabled = self._as_bool(params.get("trend_kama_gate_enabled", False), False)
        self.kama_er_window = max(2, int(float(params.get("trend_kama_er_window", 10))))
        self.kama_fast_window = max(2, int(float(params.get("trend_kama_fast_window", 2))))
        self.kama_slow_window = max(
            self.kama_fast_window + 1,
            int(float(params.get("trend_kama_slow_window", 30))),
        )
        self.kama_min_efficiency_ratio = max(
            0.0,
            min(1.0, float(params.get("trend_kama_min_efficiency_ratio", 0.14))),
        )
        self.kama_min_slope_atr_ratio = max(
            0.0,
            float(params.get("trend_kama_min_slope_atr_ratio", 0.04)),
        )
        self.pullback_bounce_required = str(
            params.get("trend_pullback_bounce_required", "true")
        ).lower() in {"1", "true", "yes", "on"}
        bounce_rejection_raw = params.get("trend_bounce_rejection_enabled", True)
        if isinstance(bounce_rejection_raw, str):
            self.bounce_rejection_enabled = bounce_rejection_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.bounce_rejection_enabled = bool(bounce_rejection_raw)
        self.pullback_bounce_min_retrace_atr_ratio = max(
            0.0,
            float(params.get("trend_pullback_bounce_min_retrace_atr_ratio", 0.08)),
        )
        self.bounce_rejection_min_wick_to_range_ratio = max(
            0.0,
            min(1.0, float(params.get("trend_bounce_rejection_min_wick_to_range_ratio", 0.30))),
        )
        self.bounce_rejection_min_wick_to_body_ratio = max(
            0.0,
            float(params.get("trend_bounce_rejection_min_wick_to_body_ratio", 0.65)),
        )
        self.bounce_rejection_buy_min_close_location = max(
            0.0,
            min(1.0, float(params.get("trend_bounce_rejection_buy_min_close_location", 0.50))),
        )
        self.bounce_rejection_sell_max_close_location = max(
            0.0,
            min(1.0, float(params.get("trend_bounce_rejection_sell_max_close_location", 0.50))),
        )
        self.pullback_max_countermove_ratio = max(
            0.0,
            float(params.get("trend_pullback_max_countermove_ratio", 0.0)),
        )
        runaway_entry_raw = params.get("trend_runaway_entry_enabled", True)
        if isinstance(runaway_entry_raw, str):
            self.runaway_entry_enabled = runaway_entry_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.runaway_entry_enabled = bool(runaway_entry_raw)
        self.runaway_max_distance_atr = max(
            0.0,
            float(params.get("trend_runaway_max_distance_atr", 0.9)),
        )
        self.runaway_adx_window = max(2, int(params.get("trend_runaway_adx_window", 14)))
        self.runaway_strong_trend_min_adx = max(
            0.0,
            float(params.get("trend_runaway_strong_trend_min_adx", 30.0)),
        )
        self.runaway_strong_trend_distance_atr = max(
            0.0,
            float(
                params.get(
                    "trend_runaway_strong_trend_distance_atr",
                    max(self.runaway_max_distance_atr, 1.2),
                )
            ),
        )
        self.runaway_confidence_penalty = max(
            0.0,
            float(params.get("trend_runaway_confidence_penalty", 0.08)),
        )
        self.runaway_strong_trend_confidence_penalty = max(
            0.0,
            float(params.get("trend_runaway_strong_trend_confidence_penalty", 0.0)),
        )
        self.index_mature_trend_confidence_bonus = max(
            0.0,
            float(params.get("trend_index_mature_trend_confidence_bonus", 0.04)),
        )
        self.confidence_gap_velocity_positive_threshold = max(
            0.0,
            float(params.get("trend_confidence_gap_velocity_positive_threshold", 0.20)),
        )
        self.confidence_gap_velocity_negative_threshold = min(
            0.0,
            float(params.get("trend_confidence_gap_velocity_negative_threshold", -0.10)),
        )
        self.confidence_gap_velocity_positive_bonus = max(
            0.0,
            float(params.get("trend_confidence_gap_velocity_positive_bonus", 0.12)),
        )
        self.confidence_gap_velocity_negative_penalty = max(
            0.0,
            float(params.get("trend_confidence_gap_velocity_negative_penalty", 0.15)),
        )
        self.risk_reward_ratio = max(
            1.0,
            float(
                params.get(
                    "trend_risk_reward_ratio",
                    float(params.get("take_profit_pips", 75.0)) / max(float(params.get("stop_loss_pips", 30.0)), FLOAT_COMPARISON_TOLERANCE),
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
        self.index_min_stop_pct_max_entry_ratio = max(
            0.0,
            float(params.get("trend_index_min_stop_pct_max_entry_ratio", 1.0)),
        )
        self.crypto_min_stop_pct = max(0.0, float(params.get("trend_crypto_min_stop_pct", 0.0)))
        self.max_stop_loss_atr = max(0.0, float(params.get("trend_max_stop_loss_atr", 0.0)))
        self.crypto_max_stop_loss_atr = max(
            0.0, float(params.get("trend_crypto_max_stop_loss_atr", self.max_stop_loss_atr))
        )
        entry_stop_invalidation_raw = params.get("trend_entry_stop_invalidation_enabled", True)
        if isinstance(entry_stop_invalidation_raw, str):
            self.entry_stop_invalidation_enabled = (
                entry_stop_invalidation_raw.strip().lower() not in {"0", "false", "no", "off"}
            )
        else:
            self.entry_stop_invalidation_enabled = bool(entry_stop_invalidation_raw)
        self.entry_stop_invalidation_atr_buffer = max(
            0.0,
            float(params.get("trend_entry_stop_invalidation_atr_buffer", 0.25)),
        )
        crypto_entry_stop_invalidation_raw = params.get(
            "trend_crypto_entry_stop_invalidation_enabled",
            self.entry_stop_invalidation_enabled,
        )
        self.crypto_entry_stop_invalidation_enabled = self._as_bool(
            crypto_entry_stop_invalidation_raw,
            self.entry_stop_invalidation_enabled,
        )
        self.spread_buffer_factor = max(0.0, float(params.get("trend_spread_buffer_factor", 0.0)))
        self.max_spread_pips = max(0.0, float(params.get("trend_max_spread_pips", 0.0)))
        self.max_spread_pips_by_symbol = self._parse_spread_ratio_limits(
            params.get("trend_max_spread_pips_by_symbol")
        )
        self.max_spread_to_stop_ratio = max(
            0.0, float(params.get("trend_max_spread_to_stop_ratio", 0.0))
        )
        self.max_spread_to_stop_ratio_by_symbol = self._parse_spread_ratio_limits(
            params.get("trend_max_spread_to_stop_ratio_by_symbol")
        )
        self.default_timeframe_sec = float(params.get("trend_timeframe_sec", 60.0))
        if self.default_timeframe_sec <= 0:
            self.default_timeframe_sec = 60.0
        self.candle_timeframe_sec = max(
            0,
            int(float(params.get("trend_candle_timeframe_sec", self.default_timeframe_sec))),
        )
        live_candle_entry_raw = params.get("trend_live_candle_entry_enabled", False)
        self.live_candle_entry_enabled = self._as_bool(live_candle_entry_raw, False)
        self.resample_mode = str(params.get("trend_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "always", "off"}:
            self.resample_mode = "auto"
        ignore_sunday_raw = params.get("trend_ignore_sunday_candles", True)
        if isinstance(ignore_sunday_raw, str):
            self.ignore_sunday_candles = ignore_sunday_raw.strip().lower() not in {"0", "false", "no", "off"}
        else:
            self.ignore_sunday_candles = bool(ignore_sunday_raw)
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
        (
            self.internal_min_confidence_for_entry,
            self.internal_min_confidence_source,
            self.internal_confidence_mode,
        ) = self._resolve_internal_min_confidence(params)
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = max(self.min_take_profit_pips, self.stop_loss_pips * self.risk_reward_ratio)
        # ATR uses close-to-close diffs and needs at least (atr_window + 1) closes.
        self.min_history = max(
            self.fast_ema_window,
            self.slow_ema_window,
            self.donchian_window,
            self.atr_window + 1,
        ) + 2
        self._last_invalid_prices_dropped = 0

    @staticmethod
    def _ema(values: Sequence[float], window: int) -> float:
        return ema(values, window)

    @staticmethod
    def _adx(values: Sequence[float], window: int) -> float | None:
        return adx_from_close(values, window)

    @staticmethod
    def _is_index_symbol(symbol: str) -> bool:
        return is_index_symbol(symbol)

    @staticmethod
    def _is_crypto_symbol(symbol: str) -> bool:
        upper = symbol.upper().strip()
        if upper in TrendFollowingStrategy._CRYPTO_BASES:
            return True
        compact = upper.replace("-", "").replace("_", "").replace(".", "")
        quote_suffixes = ("USD", "USDT", "USDC", "EUR", "GBP", "JPY", "BTC", "ETH")
        for quote in quote_suffixes:
            if not compact.endswith(quote) or len(compact) <= len(quote):
                continue
            base = compact[: -len(quote)]
            if base in TrendFollowingStrategy._CRYPTO_BASES:
                return True
        return False

    @staticmethod
    def _is_crypto_base_symbol(symbol: str) -> bool:
        return symbol.upper().strip() in TrendFollowingStrategy._CRYPTO_BASES

    def _non_crypto_trend_profile(self, symbol: str) -> str:
        return "index" if self._is_index_symbol(symbol) else "fx"

    def _non_crypto_strength_thresholds(self, symbol: str) -> tuple[str, float, float, float]:
        profile = self._non_crypto_trend_profile(symbol)
        if profile == "index":
            return (
                profile,
                self.index_min_ema_gap_ratio,
                self.index_min_fast_slope_ratio,
                self.index_min_slow_slope_ratio,
            )
        return (
            profile,
            self.fx_min_ema_gap_ratio,
            self.fx_min_fast_slope_ratio,
            self.fx_min_slow_slope_ratio,
        )

    @staticmethod
    def _normalize_timestamp_seconds(raw_ts: object) -> float:
        return Strategy._timestamp_to_seconds(raw_ts)

    @staticmethod
    def _median_sampling_interval_sec(timestamps: Sequence[float]) -> float | None:
        if len(timestamps) < 2:
            return None
        normalized: list[float] = []
        for raw in timestamps[-256:]:
            try:
                ts = TrendFollowingStrategy._normalize_timestamp_seconds(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ts) or ts <= 0:
                continue
            normalized.append(ts)
        if len(normalized) < 2:
            return None
        deltas: list[float] = []
        previous = normalized[0]
        for current in normalized[1:]:
            gap = current - previous
            previous = current
            if not math.isfinite(gap) or gap <= 0:
                continue
            deltas.append(gap)
        if not deltas:
            return None
        deltas.sort()
        middle = len(deltas) // 2
        if len(deltas) % 2 == 1:
            return deltas[middle]
        return (deltas[middle - 1] + deltas[middle]) / 2.0

    def _resolve_timeframe_sec(
        self,
        timestamps: Sequence[float],
        *,
        using_closed_candles: bool,
    ) -> float:
        if using_closed_candles and self.candle_timeframe_sec > 1:
            return float(self.candle_timeframe_sec)
        inferred = self._median_sampling_interval_sec(timestamps)
        if inferred is not None and inferred > 0:
            return float(inferred)
        return float(self.default_timeframe_sec)

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
        timeframe = max(1.0, float(self.candle_timeframe_sec or self.default_timeframe_sec))
        median_delta = deltas[len(deltas) // 2]
        if median_delta < timeframe * 0.8:
            return False
        # Keep auto mode strict: treat input as candle-like only when cadence
        # is close to configured candle timeframe.
        return median_delta <= timeframe * 1.25

    def _resample_closed_candle_ohlc(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], list[float], list[float], list[float], list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            closes = [float(value) for value in prices]
            return list(closes), closes, list(closes), list(closes), [float(value) for value in timestamps], False
        if not timestamps or len(timestamps) != len(prices):
            closes = [float(value) for value in prices]
            return list(closes), closes, list(closes), list(closes), [float(value) for value in timestamps], False

        normalized_timestamps = [self._normalize_timestamp_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and not self._is_candle_like_spacing(normalized_timestamps):
            closes = [float(value) for value in prices]
            return list(closes), closes, list(closes), list(closes), list(normalized_timestamps), False

        opens: list[float] = []
        closes: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        close_timestamps: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_price, ts in zip(prices, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            price = float(raw_price)
            if last_bucket is None or bucket != last_bucket:
                opens.append(price)
                closes.append(price)
                highs.append(price)
                lows.append(price)
                close_timestamps.append(float(ts))
                last_bucket = bucket
            else:
                closes[-1] = price
                highs[-1] = max(highs[-1], price)
                lows[-1] = min(lows[-1], price)
                close_timestamps[-1] = float(ts)

        # Always exclude the current candle in resample mode. In auto mode,
        # candle-like feeds may still include a forming last bar.
        if len(closes) < 2:
            return [], [], [], [], [], True
        return opens[:-1], closes[:-1], highs[:-1], lows[:-1], close_timestamps[:-1], True

    def _resample_closed_candle_closes(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], list[float], bool]:
        _opens, closes, _highs, _lows, close_timestamps, using_closed_candles = self._resample_closed_candle_ohlc(
            prices,
            timestamps,
        )
        return closes, close_timestamps, using_closed_candles

    def _forming_candle_preview(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
        *,
        last_closed_timestamp: float | None,
    ) -> tuple[float, float, float, float, float] | None:
        if not self.live_candle_entry_enabled or self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return None
        sample_count = min(len(prices), len(timestamps))
        if sample_count <= 0:
            return None
        timeframe = max(1, self.candle_timeframe_sec)
        bucket_prices: list[float] = []
        bucket_timestamps: list[float] = []
        last_bucket: int | None = None
        for idx in range(sample_count):
            try:
                price = float(prices[idx])
                ts = self._normalize_timestamp_seconds(timestamps[idx])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(price) or not math.isfinite(ts):
                continue
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                bucket_prices = [price]
                bucket_timestamps = [float(ts)]
                last_bucket = bucket
            else:
                bucket_prices.append(price)
                bucket_timestamps.append(float(ts))
        if not bucket_prices or not bucket_timestamps:
            return None
        preview_timestamp = bucket_timestamps[-1]
        if last_closed_timestamp is not None and preview_timestamp <= (last_closed_timestamp + FLOAT_COMPARISON_TOLERANCE):
            return None
        return (
            bucket_prices[0],
            bucket_prices[-1],
            max(bucket_prices),
            min(bucket_prices),
            preview_timestamp,
        )

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

    @staticmethod
    def _compress_consecutive_equal_prices(values: Sequence[float]) -> list[float]:
        compressed: list[float] = []
        previous: float | None = None
        for raw in values:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            if previous is not None and abs(value - previous) <= FLOAT_ROUNDING_TOLERANCE:
                continue
            compressed.append(value)
            previous = value
        return compressed

    @staticmethod
    def _ema_series(values: Sequence[float], window: int) -> list[float]:
        if not values:
            return []
        alpha = 2.0 / float(max(window, 1) + 1)
        series: list[float] = []
        current: float | None = None
        for raw in values:
            value = float(raw)
            if current is None:
                current = value
            else:
                current = (alpha * value) + ((1.0 - alpha) * current)
            series.append(float(current))
        return series

    @staticmethod
    def _median(values: Sequence[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(float(value) for value in values)
        middle = len(ordered) // 2
        if len(ordered) % 2 == 1:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / 2.0

    def _smoothed_slope_ratio(self, ema_series: Sequence[float]) -> float | None:
        if len(ema_series) < 2:
            return None
        effective_window = min(max(2, self.slope_window), len(ema_series))
        start = len(ema_series) - effective_window + 1
        slope_samples = [
            self._slope_ratio(ema_series[idx], ema_series[idx - 1])
            for idx in range(start, len(ema_series))
        ]
        return self._median(slope_samples)

    def _effective_crypto_atr_threshold(
        self,
        timestamps: Sequence[float],
    ) -> tuple[float, float | None, float]:
        base_threshold = self.crypto_min_atr_pct
        sample_interval_sec = self._median_sampling_interval_sec(timestamps)
        if sample_interval_sec is None or self.crypto_min_atr_pct_scale_mode == "none":
            return base_threshold, sample_interval_sec, 1.0

        ratio = sample_interval_sec / max(self.crypto_min_atr_pct_baseline_sec, FLOAT_COMPARISON_TOLERANCE)
        if self.crypto_min_atr_pct_scale_mode == "sqrt":
            ratio = math.sqrt(max(0.0, ratio))
        ratio = max(self.crypto_min_atr_pct_min_ratio, min(1.0, ratio))
        return base_threshold * ratio, sample_interval_sec, ratio

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

    @staticmethod
    def _resolve_internal_min_confidence(
        params: dict[str, object],
    ) -> tuple[float, str | None, str]:
        mode = str(params.get("_worker_mode", "") or "").strip().lower()
        strategy_name = TrendFollowingStrategy.name
        candidates: list[str] = []
        if mode:
            candidates.extend(
                [
                    f"{strategy_name}_{mode}_min_confidence_for_entry",
                    f"trend_{mode}_min_confidence_for_entry",
                    f"{mode}_min_confidence_for_entry",
                ]
            )
        candidates.extend(
            [
                "trend_min_confidence_for_entry",
                f"{strategy_name}_min_confidence_for_entry",
                "min_confidence_for_entry",
            ]
        )
        for key in candidates:
            if key not in params:
                continue
            try:
                value = float(params.get(key, 0.0))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            return max(0.0, min(1.0, value)), key, mode
        return 0.0, None, mode

    def _spread_to_stop_limit_for_symbol(self, symbol: str) -> tuple[float, str]:
        upper_symbol = str(symbol).strip().upper()
        override = self.max_spread_to_stop_ratio_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return self.max_spread_to_stop_ratio, "global"

    def _spread_pips_limit_for_symbol(self, symbol: str) -> tuple[float, str]:
        upper_symbol = str(symbol).strip().upper()
        override = self.max_spread_pips_by_symbol.get(upper_symbol)
        if override is not None:
            return override, "symbol_override"
        return self.max_spread_pips, "global"

    def _entry_invalidation_stop_pips(
        self,
        *,
        trend: str,
        last_price: float,
        fast_now: float,
        slow_now: float,
        atr: float | None,
        pip_size: float,
    ) -> tuple[float | None, float, float]:
        if trend == "up":
            anchor_price = min(fast_now, slow_now)
            distance_pips = (last_price - anchor_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        else:
            anchor_price = max(fast_now, slow_now)
            distance_pips = (anchor_price - last_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        if distance_pips <= 0:
            return None, anchor_price, 0.0

        buffer_pips = 0.0
        if atr is not None and atr > 0 and self.entry_stop_invalidation_atr_buffer > 0:
            buffer_pips = (atr * self.entry_stop_invalidation_atr_buffer) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        return distance_pips + buffer_pips, anchor_price, buffer_pips

    def _effective_index_stop_floor_pips(
        self,
        *,
        symbol: str,
        last_price: float,
        pip_size: float,
        entry_stop_invalidation_pips: float | None,
    ) -> tuple[float | None, float | None, bool]:
        if not self._is_index_symbol(symbol) or self.index_min_stop_pct <= 0:
            return None, None, False
        raw_pips = (last_price * self.index_min_stop_pct / 100.0) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        effective_pips = raw_pips
        clipped = False
        if (
            self.index_min_stop_pct_max_entry_ratio > 0
            and entry_stop_invalidation_pips is not None
            and entry_stop_invalidation_pips > 0
        ):
            max_floor_from_entry = entry_stop_invalidation_pips * self.index_min_stop_pct_max_entry_ratio
            if max_floor_from_entry < effective_pips:
                effective_pips = max_floor_from_entry
                clipped = True
        return raw_pips, effective_pips, clipped

    def _evaluate_volume_confirmation(self, ctx: StrategyContext) -> tuple[dict[str, object], str | None, float]:
        index_missing_volume_relaxed = self._is_index_symbol(ctx.symbol)
        effective_volume_allow_missing = self.volume_allow_missing or index_missing_volume_relaxed
        metadata: dict[str, object] = {
            "volume_confirmation_enabled": self.volume_confirmation,
            "volume_window": self.volume_window,
            "min_volume_ratio": self.min_volume_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_allow_missing_effective": effective_volume_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not self.volume_allow_missing,
            "volume_require_spike": self.volume_require_spike,
            "volume_confidence_boost_config": self.volume_confidence_boost,
            "volume_data_available": False,
            "volume_spike": False,
            "volume_current": None,
            "volume_avg": None,
            "volume_ratio": None,
            "volume_current_source": None,
            "volume_current_deduplicated": False,
        }
        if not self.volume_confirmation:
            return metadata, None, 0.0

        raw_samples = self._extract_finite_positive_volumes(ctx.volumes[-(self.volume_window + 5) :])
        current_volume = self._coerce_positive_finite(ctx.current_volume)
        current_from_context = False
        current_deduplicated = False
        if current_volume is not None:
            if raw_samples and self._values_close(raw_samples[-1], current_volume):
                current_deduplicated = True
            else:
                raw_samples.append(current_volume)
                current_from_context = True

        if len(raw_samples) < 2:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        current = raw_samples[-1]
        history = raw_samples[:-1]
        if len(history) > self.volume_window:
            history = history[-self.volume_window :]
        if len(history) < self.volume_min_samples:
            if effective_volume_allow_missing:
                return metadata, None, 0.0
            return metadata, "volume_unavailable", 0.0

        avg_volume = sum(history) / max(len(history), 1)
        if avg_volume <= 0.0:
            if effective_volume_allow_missing:
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
                "volume_current_source": ("current_volume" if current_from_context else "volumes"),
                "volume_current_deduplicated": current_deduplicated,
            }
        )
        if self.volume_require_spike and not spike:
            return metadata, "volume_not_confirmed", 0.0
        return metadata, None, self.volume_confidence_boost if spike else 0.0

    def _resolve_pip_size(self, ctx: StrategyContext, is_crypto: bool) -> tuple[float | None, str]:
        context_pip_size, context_source = self._context_pip_size(ctx)
        if context_pip_size is not None:
            return context_pip_size, context_source
        runtime_pip_size = self._runtime_symbol_pip_size(ctx.symbol)
        if runtime_pip_size is not None and runtime_pip_size > 0:
            return float(runtime_pip_size), "db_symbol_map"
        if is_crypto:
            if self._is_crypto_base_symbol(ctx.symbol) or self.crypto_allow_pair_default_pip_size:
                return self.crypto_default_pip_size, "crypto_default_ig_point"
            return None, "crypto_pair_fallback_blocked"
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
        if self._last_invalid_prices_dropped > 0 and "invalid_prices_dropped" not in metadata:
            metadata["invalid_prices_dropped"] = self._last_invalid_prices_dropped
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=metadata,
        )

    def _kama_gate_payload(
        self,
        prices: Sequence[float],
        atr: float | None,
        *,
        trend: str,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "trend_kama_gate_enabled": self.kama_gate_enabled,
            "trend_kama_er_window": self.kama_er_window,
            "trend_kama_fast_window": self.kama_fast_window,
            "trend_kama_slow_window": self.kama_slow_window,
            "trend_kama_min_efficiency_ratio": self.kama_min_efficiency_ratio,
            "trend_kama_min_slope_atr_ratio": self.kama_min_slope_atr_ratio,
        }
        if not self.kama_gate_enabled:
            payload["kama_gate_status"] = "disabled"
            return True, payload
        if atr is None or atr <= 0.0:
            payload["kama_gate_status"] = "atr_unavailable"
            return True, payload
        er = efficiency_ratio(prices, self.kama_er_window)
        kama_prev, kama_now = kama_last_two(
            prices,
            self.kama_er_window,
            fast_window=self.kama_fast_window,
            slow_window=self.kama_slow_window,
        )
        payload["kama_efficiency_ratio"] = er
        payload["kama_previous"] = kama_prev
        payload["kama_current"] = kama_now
        if er is None or kama_prev is None or kama_now is None:
            payload["kama_gate_status"] = "unavailable"
            return True, payload
        raw_slope_atr_ratio = (kama_now - kama_prev) / max(atr, FLOAT_COMPARISON_TOLERANCE)
        directional_slope_atr_ratio = raw_slope_atr_ratio if trend == "up" else -raw_slope_atr_ratio
        payload["kama_slope_atr_ratio"] = raw_slope_atr_ratio
        payload["kama_directional_slope_atr_ratio"] = directional_slope_atr_ratio
        passes = (
            er >= self.kama_min_efficiency_ratio
            and directional_slope_atr_ratio >= self.kama_min_slope_atr_ratio
        )
        payload["kama_gate_status"] = "ok" if passes else "chop"
        return passes, payload

    def _confidence(
        self,
        ema_gap_ratio: float,
        pullback_distance_ratio: float,
        overextension_ratio: float,
        bounce_confirmed: bool,
        directional_velocity_ratio: float,
        price_aligned_with_fast_ma: bool,
        entry_type: str = "pullback",
        trend_profile: str = "fx",
        ema_gap_velocity_ratio: float = 0.0,
        runaway_confidence_penalty: float | None = None,
    ) -> float:
        trend_strength = min(1.0, ema_gap_ratio / max(self.trend_strength_norm, FLOAT_COMPARISON_TOLERANCE))
        velocity_score = min(1.0, directional_velocity_ratio / max(self.confidence_velocity_norm_ratio, FLOAT_COMPARISON_TOLERANCE))
        pullback_quality = 1.0 - min(1.0, pullback_distance_ratio / max(self.pullback_max_distance_ratio, FLOAT_COMPARISON_TOLERANCE))
        overextension_penalty = min(1.0, overextension_ratio / max(self.pullback_max_distance_ratio, FLOAT_COMPARISON_TOLERANCE))
        bounce_bonus = 0.06 if bounce_confirmed else 0.0
        price_alignment_bonus = 0.08 if price_aligned_with_fast_ma else 0.0
        gap_velocity_bonus = 0.0
        gap_velocity_penalty = 0.0
        if ema_gap_velocity_ratio >= self.confidence_gap_velocity_positive_threshold:
            gap_velocity_bonus = self.confidence_gap_velocity_positive_bonus
        elif ema_gap_velocity_ratio <= self.confidence_gap_velocity_negative_threshold:
            gap_velocity_penalty = self.confidence_gap_velocity_negative_penalty
        index_mature_trend_bonus = self._index_mature_trend_confidence_bonus(
            trend_profile=trend_profile,
            ema_gap_ratio=ema_gap_ratio,
            directional_velocity_ratio=directional_velocity_ratio,
            ema_gap_velocity_ratio=ema_gap_velocity_ratio,
            overextension_ratio=overextension_ratio,
        )
        resolved_runaway_penalty = (
            max(0.0, runaway_confidence_penalty)
            if runaway_confidence_penalty is not None
            else (self.runaway_confidence_penalty if entry_type == "runaway_continuation" else 0.0)
        )
        value = (
            0.1
            + trend_strength * 0.24
            + velocity_score * 0.42
            + pullback_quality * 0.20
            + bounce_bonus
            + price_alignment_bonus
            + gap_velocity_bonus
            + index_mature_trend_bonus
            - overextension_penalty * 0.30
            - gap_velocity_penalty
            - resolved_runaway_penalty
        )
        return max(0.05, min(1.0, value))

    def _index_mature_trend_confidence_bonus(
        self,
        *,
        trend_profile: str,
        ema_gap_ratio: float,
        directional_velocity_ratio: float,
        ema_gap_velocity_ratio: float,
        overextension_ratio: float,
    ) -> float:
        overextension_penalty = min(
            1.0,
            max(0.0, overextension_ratio) / max(self.pullback_max_distance_ratio, FLOAT_COMPARISON_TOLERANCE),
        )
        if (
            trend_profile == "index"
            and ema_gap_velocity_ratio >= self.confidence_gap_velocity_positive_threshold
            and directional_velocity_ratio >= (self.confidence_velocity_norm_ratio * 0.60)
            and ema_gap_ratio >= max(self.index_min_ema_gap_ratio, self.trend_strength_norm * 0.25)
            and overextension_penalty < 0.70
        ):
            return self.index_mature_trend_confidence_bonus
        return 0.0

    @staticmethod
    def _slope_ratio(current: float, previous: float) -> float:
        return (current - previous) / max(abs(previous), FLOAT_COMPARISON_TOLERANCE)

    def _gap_velocity_ratio(self, current_gap_ratio: float, previous_gap_ratio: float) -> float:
        baseline = max(previous_gap_ratio, self.trend_strength_norm * 0.10, FLOAT_COMPARISON_TOLERANCE)
        return (current_gap_ratio - previous_gap_ratio) / baseline

    def _trend_direction(
        self,
        fast_now: float,
        slow_now: float,
        fast_prev: float,
        slow_prev: float,
        slow_slope_tolerance_ratio: float | None = None,
        *,
        fast_slope_ratio: float | None = None,
        slow_slope_ratio: float | None = None,
    ) -> tuple[bool, bool]:
        if fast_slope_ratio is None:
            fast_slope_ratio = self._slope_ratio(fast_now, fast_prev)
        if slow_slope_ratio is None:
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

        tolerance = (
            max(0.0, slow_slope_tolerance_ratio)
            if slow_slope_tolerance_ratio is not None
            else self.slow_slope_tolerance_ratio
        )
        return (
            is_up and fast_slope_ratio >= 0.0 and slow_slope_ratio >= -tolerance,
            is_down and fast_slope_ratio <= 0.0 and slow_slope_ratio <= tolerance,
        )

    def _pullback_bounce_confirmation(
        self,
        prices: Sequence[float],
        *,
        trend: str,
        atr: float | None,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "trend_pullback_bounce_required": self.pullback_bounce_required,
            "trend_pullback_bounce_min_retrace_atr_ratio": self.pullback_bounce_min_retrace_atr_ratio,
            "trend_pullback_bounce_window": 3,
            "trend_bounce_rejection_enabled": self.bounce_rejection_enabled,
            "trend_bounce_rejection_min_wick_to_range_ratio": self.bounce_rejection_min_wick_to_range_ratio,
            "trend_bounce_rejection_min_wick_to_body_ratio": self.bounce_rejection_min_wick_to_body_ratio,
            "trend_bounce_rejection_buy_min_close_location": self.bounce_rejection_buy_min_close_location,
            "trend_bounce_rejection_sell_max_close_location": self.bounce_rejection_sell_max_close_location,
        }
        last_price = close_price
        prev_price = float(prices[-2])
        if len(prices) < 3:
            directional_close_confirmed = (
                last_price >= prev_price if trend == "up" else last_price <= prev_price
            )
            payload.update(
                {
                    "bounce_directional_close_confirmed": directional_close_confirmed,
                    "bounce_local_extreme_confirmed": directional_close_confirmed,
                    "bounce_retrace_atr_ratio": None,
                    "bounce_price_action_confirmed": directional_close_confirmed,
                    "bounce_rejection_available": False,
                    "bounce_rejection_confirmed": False,
                    "bounce_rejection_status": "insufficient_history",
                    "bounce_confirmed": directional_close_confirmed,
                }
            )
            return directional_close_confirmed, payload

        prev2_price = float(prices[-3])
        if trend == "up":
            directional_close_confirmed = last_price >= prev_price
            local_extreme_confirmed = prev_price <= prev2_price and prev_price <= last_price
            retrace_price = max(0.0, last_price - prev_price) if local_extreme_confirmed else 0.0
        else:
            directional_close_confirmed = last_price <= prev_price
            local_extreme_confirmed = prev_price >= prev2_price and prev_price >= last_price
            retrace_price = max(0.0, prev_price - last_price) if local_extreme_confirmed else 0.0
        bounce_retrace_atr_ratio = (
            retrace_price / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if atr is not None and atr > 0
            else None
        )
        retrace_confirmed = (
            self.pullback_bounce_min_retrace_atr_ratio <= 0
            or bounce_retrace_atr_ratio is None
            or bounce_retrace_atr_ratio >= self.pullback_bounce_min_retrace_atr_ratio
        )
        price_action_confirmed = directional_close_confirmed and local_extreme_confirmed and retrace_confirmed
        range_size, body_size, upper_wick, lower_wick, close_location = self._candle_rejection_metrics(
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )
        body_denominator = max(body_size, range_size * 0.05, FLOAT_COMPARISON_TOLERANCE)
        rejection_available = not (
            self._values_close(open_price, close_price)
            and self._values_close(high_price, low_price)
            and self._values_close(open_price, high_price)
        )
        rejection_confirmed = False
        continuation_candle_blocked = False
        rejection_status = "disabled"
        wick_to_body_ratio = 0.0
        wick_to_range_ratio = 0.0
        if self.bounce_rejection_enabled and rejection_available:
            if trend == "up":
                wick_to_body_ratio = lower_wick / body_denominator
                wick_to_range_ratio = lower_wick / range_size
                wick_to_range_ok = wick_to_range_ratio >= self.bounce_rejection_min_wick_to_range_ratio
                wick_to_body_ok = wick_to_body_ratio >= self.bounce_rejection_min_wick_to_body_ratio
                close_location_ok = close_location >= self.bounce_rejection_buy_min_close_location
                rejection_confirmed = wick_to_range_ok and wick_to_body_ok and close_location_ok
                continuation_candle_blocked = (
                    close_price < open_price
                    and wick_to_range_ratio < (self.bounce_rejection_min_wick_to_range_ratio * 0.5)
                    and close_location < self.bounce_rejection_buy_min_close_location
                )
                if rejection_confirmed:
                    rejection_status = "ok"
                elif not wick_to_range_ok:
                    rejection_status = "wick_to_range_too_small"
                elif not wick_to_body_ok:
                    rejection_status = "wick_to_body_too_small"
                elif not close_location_ok:
                    rejection_status = "close_location_not_bullish"
                else:
                    rejection_status = "blocked"
            else:
                wick_to_body_ratio = upper_wick / body_denominator
                wick_to_range_ratio = upper_wick / range_size
                wick_to_range_ok = wick_to_range_ratio >= self.bounce_rejection_min_wick_to_range_ratio
                wick_to_body_ok = wick_to_body_ratio >= self.bounce_rejection_min_wick_to_body_ratio
                close_location_ok = close_location <= self.bounce_rejection_sell_max_close_location
                rejection_confirmed = wick_to_range_ok and wick_to_body_ok and close_location_ok
                continuation_candle_blocked = (
                    close_price > open_price
                    and wick_to_range_ratio < (self.bounce_rejection_min_wick_to_range_ratio * 0.5)
                    and close_location > self.bounce_rejection_sell_max_close_location
                )
                if rejection_confirmed:
                    rejection_status = "ok"
                elif not wick_to_range_ok:
                    rejection_status = "wick_to_range_too_small"
                elif not wick_to_body_ok:
                    rejection_status = "wick_to_body_too_small"
                elif not close_location_ok:
                    rejection_status = "close_location_not_bearish"
                else:
                    rejection_status = "blocked"
        elif self.bounce_rejection_enabled:
            rejection_status = "unavailable"

        if rejection_confirmed:
            bounce_confirmed = True
        elif not rejection_available or not self.bounce_rejection_enabled:
            bounce_confirmed = price_action_confirmed
        else:
            bounce_confirmed = price_action_confirmed and not continuation_candle_blocked
        payload.update(
            {
                "bounce_directional_close_confirmed": directional_close_confirmed,
                "bounce_local_extreme_confirmed": local_extreme_confirmed,
                "bounce_retrace_price": retrace_price,
                "bounce_retrace_atr_ratio": bounce_retrace_atr_ratio,
                "bounce_retrace_confirmed": retrace_confirmed,
                "bounce_price_action_confirmed": price_action_confirmed,
                "bounce_rejection_range_size": range_size,
                "bounce_rejection_body_size": body_size,
                "bounce_rejection_upper_wick": upper_wick,
                "bounce_rejection_lower_wick": lower_wick,
                "bounce_rejection_close_location": close_location,
                "bounce_rejection_wick_to_body_ratio": wick_to_body_ratio,
                "bounce_rejection_wick_to_range_ratio": wick_to_range_ratio,
                "bounce_rejection_available": rejection_available,
                "bounce_rejection_confirmed": rejection_confirmed,
                "bounce_rejection_status": rejection_status,
                "bounce_continuation_candle_blocked": continuation_candle_blocked,
                "bounce_confirmed": bounce_confirmed,
            }
        )
        return bounce_confirmed, payload

    def _runaway_entry_allows(
        self,
        *,
        trend: str,
        prices: Sequence[float],
        last_price: float,
        prev_price: float,
        fast_now: float,
        fast_prev: float,
        ema_gap_ratio: float,
        directional_velocity_ratio: float,
        effective_min_ema_gap_ratio: float,
        effective_min_fast_slope_ratio: float,
        effective_min_slow_slope_ratio: float,
        pullback_distance_atr: float | None,
        max_pullback_distance_atr: float,
        atr: float | None,
        channel_upper: float,
        channel_lower: float,
        adx: float | None,
        ema_gap_velocity_ratio: float,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "trend_runaway_entry_enabled": self.runaway_entry_enabled,
            "trend_runaway_max_distance_atr": self.runaway_max_distance_atr,
            "trend_runaway_adx_window": self.runaway_adx_window,
            "trend_runaway_strong_trend_min_adx": self.runaway_strong_trend_min_adx,
            "trend_runaway_strong_trend_distance_atr": self.runaway_strong_trend_distance_atr,
            "trend_runaway_confidence_penalty": self.runaway_confidence_penalty,
            "trend_runaway_strong_trend_confidence_penalty": self.runaway_strong_trend_confidence_penalty,
            "runaway_entry_candidate": False,
            "runaway_entry_allowed": False,
            "runaway_confidence_penalty": 0.0,
        }
        if not self.runaway_entry_enabled or len(prices) < 3:
            payload["runaway_entry_status"] = "disabled"
            return False, payload

        prev2_price = float(prices[-3])
        payload["runaway_entry_candidate"] = True
        if trend == "up":
            continuation_structure_ok = last_price >= prev_price >= prev2_price
            price_stayed_above_fast = last_price >= fast_now and prev_price >= fast_prev
            breakout_extension_atr = (
                max(0.0, last_price - channel_upper) / max(atr, FLOAT_COMPARISON_TOLERANCE)
                if atr is not None and atr > 0
                else None
            )
        else:
            continuation_structure_ok = last_price <= prev_price <= prev2_price
            price_stayed_above_fast = last_price <= fast_now and prev_price <= fast_prev
            breakout_extension_atr = (
                max(0.0, channel_lower - last_price) / max(atr, FLOAT_COMPARISON_TOLERANCE)
                if atr is not None and atr > 0
                else None
            )
        strong_gap_threshold = max(
            effective_min_ema_gap_ratio * 1.6,
            self.trend_strength_norm * 0.2,
        )
        strong_gap_ok = ema_gap_ratio >= strong_gap_threshold
        strong_velocity_threshold = max(
            self.confidence_velocity_norm_ratio * 1.15,
            (effective_min_fast_slope_ratio * 1.8) + (effective_min_slow_slope_ratio * 0.8),
        )
        strong_velocity_ok = directional_velocity_ratio >= strong_velocity_threshold
        strong_trend_adx_ok = adx is not None and adx >= self.runaway_strong_trend_min_adx
        strong_trend_gap_velocity_ok = (
            ema_gap_velocity_ratio >= self.confidence_gap_velocity_positive_threshold
        )
        runaway_strong_trend = (
            strong_gap_ok
            and strong_velocity_ok
            and strong_trend_adx_ok
            and strong_trend_gap_velocity_ok
        )
        distance_limit_atr = self.runaway_max_distance_atr
        if max_pullback_distance_atr > 0 and distance_limit_atr > 0:
            distance_limit_atr = min(distance_limit_atr, max_pullback_distance_atr)
        elif max_pullback_distance_atr > 0 and distance_limit_atr <= 0:
            distance_limit_atr = max_pullback_distance_atr
        if runaway_strong_trend and self.runaway_strong_trend_distance_atr > 0:
            strong_distance_limit_atr = self.runaway_strong_trend_distance_atr
            if max_pullback_distance_atr > 0:
                strong_distance_limit_atr = min(strong_distance_limit_atr, max_pullback_distance_atr)
            distance_limit_atr = max(distance_limit_atr, strong_distance_limit_atr)
        distance_ok = (
            distance_limit_atr > 0
            and pullback_distance_atr is not None
            and pullback_distance_atr <= distance_limit_atr
        )
        breakout_extension_ok = breakout_extension_atr is None or breakout_extension_atr <= 0.5
        runaway_confidence_penalty = self.runaway_confidence_penalty
        if runaway_strong_trend:
            runaway_confidence_penalty = self.runaway_strong_trend_confidence_penalty
        payload.update(
            {
                "runaway_continuation_structure_ok": continuation_structure_ok,
                "runaway_price_stayed_above_fast": price_stayed_above_fast,
                "runaway_breakout_extension_atr": breakout_extension_atr,
                "runaway_breakout_extension_ok": breakout_extension_ok,
                "runaway_distance_atr": pullback_distance_atr,
                "runaway_distance_limit_atr": distance_limit_atr,
                "runaway_distance_ok": distance_ok,
                "runaway_strong_gap_threshold": strong_gap_threshold,
                "runaway_strong_gap_ok": strong_gap_ok,
                "runaway_strong_velocity_threshold": strong_velocity_threshold,
                "runaway_strong_velocity_ok": strong_velocity_ok,
                "runaway_adx": adx,
                "runaway_gap_velocity_ratio": ema_gap_velocity_ratio,
                "runaway_gap_velocity_positive_threshold": self.confidence_gap_velocity_positive_threshold,
                "runaway_strong_trend_adx_ok": strong_trend_adx_ok,
                "runaway_strong_trend_gap_velocity_ok": strong_trend_gap_velocity_ok,
                "runaway_strong_trend": runaway_strong_trend,
                "runaway_confidence_penalty": runaway_confidence_penalty,
            }
        )
        allowed = (
            continuation_structure_ok
            and price_stayed_above_fast
            and distance_ok
            and strong_gap_ok
            and strong_velocity_ok
            and breakout_extension_ok
        )
        payload["runaway_entry_allowed"] = allowed
        payload["runaway_entry_status"] = "ok" if allowed else "blocked"
        return allowed, payload

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        raw_prices: list[float] = []
        raw_timestamps: list[float] = []
        invalid_prices = 0
        timestamps_aligned = len(ctx.timestamps) == len(ctx.prices)
        timestamps_usable = timestamps_aligned
        for idx, raw in enumerate(ctx.prices):
            try:
                value = float(raw)
            except (TypeError, ValueError):
                invalid_prices += 1
                continue
            if not math.isfinite(value):
                invalid_prices += 1
                continue
            raw_prices.append(value)
            if timestamps_usable:
                try:
                    ts = self._normalize_timestamp_seconds(ctx.timestamps[idx])
                except (TypeError, ValueError):
                    timestamps_usable = False
                    continue
                if not math.isfinite(ts) or ts < 0:
                    timestamps_usable = False
                    continue
                raw_timestamps.append(float(ts))
        if not timestamps_usable:
            raw_timestamps = []
        self._last_invalid_prices_dropped = invalid_prices

        if len(raw_prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                {
                    "history": len(raw_prices),
                    "required_history": self.min_history,
                    "invalid_prices_dropped": invalid_prices,
                },
            )

        candle_opens, prices, candle_highs, candle_lows, price_timestamps, using_closed_candles = (
            self._resample_closed_candle_ohlc(
                raw_prices,
                raw_timestamps,
            )
        )
        using_live_candle_entry = False
        live_candle_preview = self._forming_candle_preview(
            raw_prices,
            raw_timestamps,
            last_closed_timestamp=(price_timestamps[-1] if price_timestamps else None),
        )
        if live_candle_preview is not None:
            live_open, live_close, live_high, live_low, live_timestamp = live_candle_preview
            candle_opens = [*candle_opens, float(live_open)]
            prices = [*prices, float(live_close)]
            candle_highs = [*candle_highs, float(live_high)]
            candle_lows = [*candle_lows, float(live_low)]
            price_timestamps = [*price_timestamps, float(live_timestamp)]
            using_live_candle_entry = True
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_price_history",
                {
                    "history": len(prices),
                    "required_history": self.min_history,
                    "raw_history": len(raw_prices),
                    "using_closed_candles": using_closed_candles,
                    "trend_live_candle_entry_enabled": self.live_candle_entry_enabled,
                    "using_live_candle_entry": using_live_candle_entry,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "invalid_prices_dropped": invalid_prices,
                },
            )

        timeframe_sec = self._resolve_timeframe_sec(
            price_timestamps if price_timestamps else raw_timestamps,
            using_closed_candles=using_closed_candles,
        )

        if self.require_context_tick_size and not self._has_context_pip_size(ctx):
            return self._hold(
                "tick_size_unavailable",
                {"trend_require_context_tick_size": True},
            )
        gap_check_timestamps = raw_timestamps if len(raw_timestamps) >= 2 else price_timestamps
        if self.max_timestamp_gap_sec > 0 and len(gap_check_timestamps) >= 2:
            try:
                last_ts = float(gap_check_timestamps[-1])
                prev_ts = float(gap_check_timestamps[-2])
            except (TypeError, ValueError):
                return self._hold("invalid_timestamp", {"trend_max_timestamp_gap_sec": self.max_timestamp_gap_sec})
            if (last_ts - prev_ts) > self.max_timestamp_gap_sec:
                return self._hold(
                    "timestamp_gap_too_wide",
                    {
                        "timestamp_gap_sec": last_ts - prev_ts,
                        "trend_max_timestamp_gap_sec": self.max_timestamp_gap_sec,
                    },
                )

        fast_ema_series = self._ema_series(prices, self.fast_ema_window)
        slow_ema_series = self._ema_series(prices, self.slow_ema_window)
        fast_now = float(fast_ema_series[-1])
        slow_now = float(slow_ema_series[-1])
        fast_prev = float(fast_ema_series[-2])
        slow_prev = float(slow_ema_series[-2])
        fast_slope_ratio_raw = self._slope_ratio(fast_now, fast_prev)
        slow_slope_ratio_raw = self._slope_ratio(slow_now, slow_prev)
        fast_slope_ratio = self._smoothed_slope_ratio(fast_ema_series)
        if fast_slope_ratio is None:
            fast_slope_ratio = fast_slope_ratio_raw
        slow_slope_ratio = self._smoothed_slope_ratio(slow_ema_series)
        if slow_slope_ratio is None:
            slow_slope_ratio = slow_slope_ratio_raw
        is_crypto = self._is_crypto_symbol(ctx.symbol)
        non_crypto_profile, non_crypto_min_ema_gap_ratio, non_crypto_min_fast_slope_ratio, non_crypto_min_slow_slope_ratio = (
            self._non_crypto_strength_thresholds(ctx.symbol)
        )
        trend_profile = "crypto" if is_crypto else non_crypto_profile
        effective_slow_slope_tolerance_ratio = self.slow_slope_tolerance_ratio
        if not is_crypto:
            effective_slow_slope_tolerance_ratio = min(
                self.slow_slope_tolerance_ratio,
                self.slow_slope_tolerance_ratio_cap_non_crypto,
            )
        trend_direction_params = inspect.signature(self._trend_direction).parameters
        if "fast_slope_ratio" in trend_direction_params and "slow_slope_ratio" in trend_direction_params:
            trend_up, trend_down = self._trend_direction(
                fast_now,
                slow_now,
                fast_prev,
                slow_prev,
                effective_slow_slope_tolerance_ratio,
                fast_slope_ratio=fast_slope_ratio,
                slow_slope_ratio=slow_slope_ratio,
            )
        else:
            trend_up, trend_down = self._trend_direction(
                fast_now,
                slow_now,
                fast_prev,
                slow_prev,
                effective_slow_slope_tolerance_ratio,
            )

        breakout_up, breakout_down, channel_upper, channel_lower = self._donchian_breakout(prices)
        recent_breakout_up, recent_breakout_down = self._recent_donchian_breakout(prices)
        breakout_filter_up = recent_breakout_up if self.use_donchian_filter else trend_up
        breakout_filter_down = recent_breakout_down if self.use_donchian_filter else trend_down

        last_price = float(prices[-1])
        prev_price = float(prices[-2])
        candle_open_last = float(candle_opens[-1]) if candle_opens else last_price
        candle_high_last = float(candle_highs[-1]) if candle_highs else last_price
        candle_low_last = float(candle_lows[-1]) if candle_lows else last_price
        ema_gap_ratio = abs(fast_now - slow_now) / max(abs(slow_now), FLOAT_COMPARISON_TOLERANCE)
        prev_ema_gap_ratio = abs(fast_prev - slow_prev) / max(abs(slow_prev), FLOAT_COMPARISON_TOLERANCE)
        ema_gap_velocity_ratio = self._gap_velocity_ratio(ema_gap_ratio, prev_ema_gap_ratio)
        channel_mid = (channel_upper + channel_lower) / 2.0
        pullback_ema_distance_ratio = abs(last_price - fast_now) / max(abs(fast_now), FLOAT_COMPARISON_TOLERANCE)
        pullback_channel_mid_distance_ratio = abs(last_price - channel_mid) / max(abs(last_price), FLOAT_COMPARISON_TOLERANCE)
        # Pullback quality for trend entries must be anchored to fast EMA.
        # Channel mid distance is diagnostic only; it must not relax entry gates.
        pullback_distance_ratio = pullback_ema_distance_ratio
        atr_prices = prices
        atr_price_source = "raw_prices"
        if is_crypto:
            compressed_prices = self._compress_consecutive_equal_prices(prices)
            if len(compressed_prices) >= (self.atr_window + 1) and len(compressed_prices) < len(prices):
                atr_prices = compressed_prices
                atr_price_source = "dedup_consecutive_equal"
        atr = atr_wilder(atr_prices, self.atr_window)
        pullback_distance_atr = (
            abs(last_price - fast_now) / max(atr, FLOAT_COMPARISON_TOLERANCE)
            if atr is not None and atr > 0
            else None
        )
        atr_pct = (max(0.0, atr) / max(abs(last_price), FLOAT_COMPARISON_TOLERANCE) * 100.0) if atr is not None else None
        effective_crypto_min_atr_pct = self.crypto_min_atr_pct
        atr_threshold_sampling_interval_sec: float | None = None
        atr_threshold_sampling_ratio = 1.0
        if is_crypto:
            (
                effective_crypto_min_atr_pct,
                atr_threshold_sampling_interval_sec,
                atr_threshold_sampling_ratio,
            ) = self._effective_crypto_atr_threshold(price_timestamps)
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
        pullback_ema_tolerance_ratio_effective = pullback_ema_tolerance_ratio
        if atr is not None and atr > 0 and self.pullback_ema_tolerance_atr_multiplier > 0:
            atr_pullback_tolerance_ratio = (
                (atr / max(abs(last_price), FLOAT_COMPARISON_TOLERANCE)) * self.pullback_ema_tolerance_atr_multiplier
            )
            pullback_ema_tolerance_ratio_effective = max(
                pullback_ema_tolerance_ratio_effective,
                atr_pullback_tolerance_ratio,
            )
        pullback_buy_zone = pullback_ema_distance_ratio <= pullback_ema_tolerance_ratio_effective
        pullback_sell_zone = pullback_ema_distance_ratio <= pullback_ema_tolerance_ratio_effective
        pip_size, pip_size_source = self._resolve_pip_size(ctx, is_crypto)
        if pip_size is None or pip_size <= 0:
            return self._hold(
                "pip_size_unavailable",
                {
                    "trend": "up" if trend_up else ("down" if trend_down else "none"),
                    "symbol": ctx.symbol,
                    "pip_size_source": pip_size_source,
                    "is_crypto": is_crypto,
                    "trend_crypto_default_pip_size": self.crypto_default_pip_size,
                    "trend_crypto_allow_pair_default_pip_size": self.crypto_allow_pair_default_pip_size,
                },
            )

        if is_crypto:
            if atr is None:
                return self._hold(
                    "crypto_atr_unavailable",
                    {
                        "prices_len": len(prices),
                        "atr_prices_len": len(atr_prices),
                        "atr_price_source": atr_price_source,
                        "atr_window": self.atr_window,
                        "required_for_atr": self.atr_window + 1,
                        "crypto_min_atr_pct": self.crypto_min_atr_pct,
                        "effective_crypto_min_atr_pct": effective_crypto_min_atr_pct,
                        "crypto_min_atr_pct_scale_mode": self.crypto_min_atr_pct_scale_mode,
                        "crypto_min_atr_pct_baseline_sec": self.crypto_min_atr_pct_baseline_sec,
                        "crypto_min_atr_pct_min_ratio": self.crypto_min_atr_pct_min_ratio,
                        "atr_threshold_sampling_interval_sec": atr_threshold_sampling_interval_sec,
                        "atr_threshold_sampling_ratio": atr_threshold_sampling_ratio,
                        "invalid_prices_dropped": invalid_prices,
                    },
                )
            if atr_pct is not None and atr_pct < effective_crypto_min_atr_pct:
                return self._hold(
                    "crypto_atr_below_threshold",
                    {
                        "atr_pct": atr_pct,
                        "crypto_min_atr_pct": self.crypto_min_atr_pct,
                        "effective_crypto_min_atr_pct": effective_crypto_min_atr_pct,
                        "crypto_min_atr_pct_scale_mode": self.crypto_min_atr_pct_scale_mode,
                        "crypto_min_atr_pct_baseline_sec": self.crypto_min_atr_pct_baseline_sec,
                        "crypto_min_atr_pct_min_ratio": self.crypto_min_atr_pct_min_ratio,
                        "atr_threshold_sampling_interval_sec": atr_threshold_sampling_interval_sec,
                        "atr_threshold_sampling_ratio": atr_threshold_sampling_ratio,
                        "atr_price_source": atr_price_source,
                    },
                )
        trend_gap_penalty = 0.0
        trend_gap_softened = False
        trend_gap_soft_reason: str | None = None
        trend_slope_penalty = 0.0
        trend_slope_softened = False
        trend_slope_soft_reason: str | None = None
        if is_crypto and (trend_up or trend_down):
            if ema_gap_ratio < self.crypto_min_ema_gap_ratio:
                trend_gap_penalty = self._SECONDARY_TREND_GAP_PENALTY
                trend_gap_softened = True
                trend_gap_soft_reason = "crypto_trend_too_weak"

        effective_min_ema_gap_ratio = self.crypto_min_ema_gap_ratio if is_crypto else non_crypto_min_ema_gap_ratio
        effective_min_fast_slope_ratio = self.crypto_min_fast_slope_ratio if is_crypto else non_crypto_min_fast_slope_ratio
        effective_min_slow_slope_ratio = self.crypto_min_slow_slope_ratio if is_crypto else non_crypto_min_slow_slope_ratio
        directional_velocity_ratio_up = max(0.0, fast_slope_ratio) + max(0.0, slow_slope_ratio) * 0.5
        directional_velocity_ratio_down = max(0.0, -fast_slope_ratio) + max(0.0, -slow_slope_ratio) * 0.5
        runaway_adx = self._adx(prices, self.runaway_adx_window)

        if not is_crypto and (trend_up or trend_down):
            if ema_gap_ratio < non_crypto_min_ema_gap_ratio:
                trend_gap_penalty = self._SECONDARY_TREND_GAP_PENALTY
                trend_gap_softened = True
                trend_gap_soft_reason = "trend_too_weak"
            if trend_up:
                fast_slope_ok = fast_slope_ratio >= non_crypto_min_fast_slope_ratio
                slow_slope_ok = slow_slope_ratio >= non_crypto_min_slow_slope_ratio
            else:
                fast_slope_ok = fast_slope_ratio <= -non_crypto_min_fast_slope_ratio
                slow_slope_ok = slow_slope_ratio <= -non_crypto_min_slow_slope_ratio
            if not (fast_slope_ok and slow_slope_ok):
                trend_slope_penalty = self._SECONDARY_TREND_SLOPE_PENALTY
                trend_slope_softened = True
                trend_slope_soft_reason = "trend_slope_too_weak"

        if trend_up and breakout_filter_up:
            kama_gate_passes, kama_payload = self._kama_gate_payload(prices, atr, trend="up")
            if not kama_gate_passes:
                kama_penalty = self._SECONDARY_KAMA_PENALTY
                kama_softened = True
            else:
                kama_penalty = 0.0
                kama_softened = False
            runaway_entry_allowed, runaway_payload = self._runaway_entry_allows(
                trend="up",
                prices=prices,
                last_price=last_price,
                prev_price=prev_price,
                fast_now=fast_now,
                fast_prev=fast_prev,
                ema_gap_ratio=ema_gap_ratio,
                directional_velocity_ratio=directional_velocity_ratio_up,
                effective_min_ema_gap_ratio=effective_min_ema_gap_ratio,
                effective_min_fast_slope_ratio=effective_min_fast_slope_ratio,
                effective_min_slow_slope_ratio=effective_min_slow_slope_ratio,
                pullback_distance_atr=pullback_distance_atr,
                max_pullback_distance_atr=max_pullback_distance_atr,
                atr=atr,
                channel_upper=channel_upper,
                channel_lower=channel_lower,
                adx=runaway_adx,
                ema_gap_velocity_ratio=ema_gap_velocity_ratio,
            )
            if is_crypto and (
                fast_slope_ratio < self.crypto_min_fast_slope_ratio
                or slow_slope_ratio < self.crypto_min_slow_slope_ratio
            ):
                trend_slope_penalty = max(trend_slope_penalty, self._SECONDARY_TREND_SLOPE_PENALTY)
                trend_slope_softened = True
                trend_slope_soft_reason = "crypto_slope_too_weak"
            entry_type = "pullback"
            if last_price <= slow_now:
                return self._hold(
                    "pullback_not_in_zone",
                    {
                        "trend": "up",
                        "pullback_buy_zone": pullback_buy_zone,
                        "price_above_slow_ema": last_price > slow_now,
                        "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                        "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                        "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                        **runaway_payload,
                    },
                )
            if not pullback_buy_zone:
                if not runaway_entry_allowed:
                    return self._hold(
                        "pullback_not_in_zone",
                        {
                            "trend": "up",
                            "pullback_buy_zone": pullback_buy_zone,
                            "price_above_slow_ema": last_price > slow_now,
                            "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                            "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                            "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                            **runaway_payload,
                        },
                    )
                entry_type = "runaway_continuation"
            if pullback_distance_ratio > self.pullback_max_distance_ratio:
                return self._hold(
                    "no_pullback_entry",
                    {
                        "trend": "up",
                        "pullback_distance_ratio": pullback_distance_ratio,
                        "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                        "pullback_channel_mid_distance_ratio": pullback_channel_mid_distance_ratio,
                        "pullback_max_distance_ratio": self.pullback_max_distance_ratio,
                        **runaway_payload,
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
                        **runaway_payload,
                    },
                )
            pullback_countermove_ratio = max(0.0, (prev_price - last_price) / max(abs(prev_price), FLOAT_COMPARISON_TOLERANCE))
            bounce_confirmed = False
            bounce_payload: dict[str, object] = {
                "trend_pullback_bounce_required": self.pullback_bounce_required,
                "trend_pullback_bounce_min_retrace_atr_ratio": self.pullback_bounce_min_retrace_atr_ratio,
                "bounce_confirmed": False,
            }
            if entry_type == "pullback":
                bounce_confirmed, bounce_payload = self._pullback_bounce_confirmation(
                    prices,
                    trend="up",
                    atr=atr,
                    open_price=candle_open_last,
                    high_price=candle_high_last,
                    low_price=candle_low_last,
                    close_price=last_price,
                )
            if pullback_countermove_ratio > self.pullback_max_countermove_ratio:
                return self._hold(
                    "pullback_countermove_in_progress",
                    {
                        "trend": "up",
                        "bounce_confirmed": bounce_confirmed,
                        "pullback_countermove_ratio": pullback_countermove_ratio,
                        "trend_pullback_max_countermove_ratio": self.pullback_max_countermove_ratio,
                        "trend_pullback_bounce_required": self.pullback_bounce_required,
                        **bounce_payload,
                        **runaway_payload,
                    },
                )
            if entry_type == "pullback" and self.pullback_bounce_required and not bounce_confirmed:
                return self._hold(
                    "pullback_not_confirmed",
                    {
                        "trend": "up",
                        **bounce_payload,
                        **runaway_payload,
                    },
                )

            structure_stop_pips = (last_price - channel_lower) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            if structure_stop_pips <= 0:
                return self._hold("invalid_structure_stop", {"trend": "up"})

            entry_stop_invalidation_pips = None
            entry_stop_invalidation_anchor_price = None
            entry_stop_invalidation_buffer_pips = 0.0
            entry_stop_invalidation_enabled_for_entry = self.entry_stop_invalidation_enabled and (
                not is_crypto or self.crypto_entry_stop_invalidation_enabled
            )
            if entry_stop_invalidation_enabled_for_entry:
                (
                    entry_stop_invalidation_pips,
                    entry_stop_invalidation_anchor_price,
                    entry_stop_invalidation_buffer_pips,
                ) = self._entry_invalidation_stop_pips(
                    trend="up",
                    last_price=last_price,
                    fast_now=fast_now,
                    slow_now=slow_now,
                    atr=atr,
                    pip_size=pip_size,
                )

            stop_floor_pips = self.min_stop_loss_pips
            index_stop_pct_raw_pips, index_stop_pct_effective_pips, index_stop_pct_clipped_by_entry_ratio = (
                self._effective_index_stop_floor_pips(
                    symbol=ctx.symbol,
                    last_price=last_price,
                    pip_size=pip_size,
                    entry_stop_invalidation_pips=entry_stop_invalidation_pips,
                )
            )
            if index_stop_pct_effective_pips is not None:
                stop_floor_pips = max(stop_floor_pips, index_stop_pct_effective_pips)
            if is_crypto and self.crypto_min_stop_pct > 0:
                crypto_stop_pips = (last_price * self.crypto_min_stop_pct / 100.0) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
                stop_floor_pips = max(stop_floor_pips, crypto_stop_pips)

            stop_loss_pips = max(stop_floor_pips, structure_stop_pips)
            stop_loss_before_entry_invalidation_pips = stop_loss_pips
            entry_stop_invalidation_applied = False
            if entry_stop_invalidation_enabled_for_entry:
                if entry_stop_invalidation_pips is not None:
                    entry_capped_stop_pips = max(stop_floor_pips, entry_stop_invalidation_pips)
                    if entry_capped_stop_pips < stop_loss_pips:
                        stop_loss_pips = entry_capped_stop_pips
                        entry_stop_invalidation_applied = True
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
                stop_loss_cap_pips = (atr * stop_cap_atr) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
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
            max_spread_pips_limit, max_spread_pips_source = self._spread_pips_limit_for_symbol(ctx.symbol)
            if max_spread_pips_limit > 0 and spread_pips >= max_spread_pips_limit:
                return self._hold(
                    "spread_too_wide",
                    {
                        "trend": "up",
                        "spread_pips": spread_pips,
                        "trend_max_spread_pips": max_spread_pips_limit,
                        "trend_max_spread_pips_source": max_spread_pips_source,
                    },
                )
            if self.spread_buffer_factor > 0 and spread_pips > 0:
                stop_loss_pips += spread_pips * self.spread_buffer_factor
            spread_to_stop_ratio = (
                spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
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
            overextension_ratio = max(0.0, last_price - channel_upper) / max(abs(last_price), FLOAT_COMPARISON_TOLERANCE)
            directional_velocity_ratio = directional_velocity_ratio_up
            price_aligned_with_fast_ma = last_price >= fast_now
            confidence = self._confidence(
                ema_gap_ratio=ema_gap_ratio,
                pullback_distance_ratio=pullback_distance_ratio,
                overextension_ratio=overextension_ratio,
                bounce_confirmed=bounce_confirmed,
                directional_velocity_ratio=directional_velocity_ratio,
                price_aligned_with_fast_ma=price_aligned_with_fast_ma,
                entry_type=entry_type,
                trend_profile=trend_profile,
                ema_gap_velocity_ratio=ema_gap_velocity_ratio,
                runaway_confidence_penalty=runaway_payload.get("runaway_confidence_penalty"),
            )
            volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
            if volume_hold_reason is not None:
                volume_penalty = (
                    self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                    if volume_hold_reason == "volume_unavailable"
                    else self._SECONDARY_VOLUME_PENALTY
                )
                volume_softened = True
            else:
                volume_penalty = 0.0
                volume_softened = False
            confidence = min(1.0, confidence + volume_confidence_boost)
            soft_filter_reasons: list[str] = []
            if trend_gap_softened and trend_gap_soft_reason is not None:
                soft_filter_reasons.append(trend_gap_soft_reason)
            if trend_slope_softened and trend_slope_soft_reason is not None:
                soft_filter_reasons.append(trend_slope_soft_reason)
            if kama_softened:
                soft_filter_reasons.append("kama_chop_regime")
            if volume_softened and volume_hold_reason is not None:
                soft_filter_reasons.append(volume_hold_reason)
            soft_filter_penalty_total = trend_gap_penalty + trend_slope_penalty + kama_penalty + volume_penalty
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            if confidence < self.internal_min_confidence_for_entry:
                return self._hold(
                    "confidence_below_internal_threshold",
                    {
                        "trend": "up",
                        "confidence": confidence,
                        "trend_internal_min_confidence_for_entry": self.internal_min_confidence_for_entry,
                        "trend_internal_min_confidence_source": self.internal_min_confidence_source,
                        "trend_internal_confidence_mode": self.internal_confidence_mode,
                    },
                )
            signal = Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ema_trend_following",
                    "invalid_prices_dropped": invalid_prices,
                    "trend": "up",
                    "trend_profile": trend_profile,
                    "entry_type": entry_type,
                    "fast_ema_window": self.fast_ema_window,
                    "slow_ema_window": self.slow_ema_window,
                    "donchian_window": self.donchian_window,
                    "use_donchian_filter_configured": self.use_donchian_filter,
                    "donchian_filter_applied": self.use_donchian_filter,
                    "donchian_breakout": breakout_up,
                    "recent_breakout": recent_breakout_up,
                    "channel_upper": channel_upper,
                    "channel_mid": channel_mid,
                    "channel_lower": channel_lower,
                    "pullback_distance_ratio": pullback_distance_ratio,
                    "pullback_distance_atr": pullback_distance_atr,
                    "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                    "pullback_channel_mid_distance_ratio": pullback_channel_mid_distance_ratio,
                    "pullback_countermove_ratio": pullback_countermove_ratio,
                    "trend_pullback_max_countermove_ratio": self.pullback_max_countermove_ratio,
                    "trend_pullback_bounce_min_retrace_atr_ratio": self.pullback_bounce_min_retrace_atr_ratio,
                    "ema_gap_ratio": ema_gap_ratio,
                    "prev_ema_gap_ratio": prev_ema_gap_ratio,
                    "ema_gap_velocity_ratio": ema_gap_velocity_ratio,
                    "fast_slope_ratio": fast_slope_ratio,
                    "slow_slope_ratio": slow_slope_ratio,
                    "fast_slope_ratio_raw": fast_slope_ratio_raw,
                    "slow_slope_ratio_raw": slow_slope_ratio_raw,
                    "trend_slope_window": self.slope_window,
                    "trend_min_ema_gap_ratio": effective_min_ema_gap_ratio,
                    "trend_min_fast_slope_ratio": effective_min_fast_slope_ratio,
                    "trend_min_slow_slope_ratio": effective_min_slow_slope_ratio,
                    "slow_slope_tolerance_ratio_configured": self.slow_slope_tolerance_ratio,
                    "slow_slope_tolerance_ratio_effective": effective_slow_slope_tolerance_ratio,
                    "trend_slow_slope_tolerance_ratio_cap_non_crypto": self.slow_slope_tolerance_ratio_cap_non_crypto,
                    "directional_velocity_ratio": directional_velocity_ratio,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "slope_mode": self.slope_mode,
                    "confidence_model": "velocity_weighted_v4",
                    "trend_confidence_velocity_norm_ratio": self.confidence_velocity_norm_ratio,
                    "trend_confidence_gap_velocity_positive_threshold": self.confidence_gap_velocity_positive_threshold,
                    "trend_confidence_gap_velocity_negative_threshold": self.confidence_gap_velocity_negative_threshold,
                    "trend_confidence_gap_velocity_positive_bonus": self.confidence_gap_velocity_positive_bonus,
                    "trend_confidence_gap_velocity_negative_penalty": self.confidence_gap_velocity_negative_penalty,
                    "trend_gap_penalty": trend_gap_penalty,
                    "trend_gap_softened": trend_gap_softened,
                    "trend_slope_penalty": trend_slope_penalty,
                    "trend_slope_softened": trend_slope_softened,
                    "kama_penalty": kama_penalty,
                    "kama_softened": kama_softened,
                    **kama_payload,
                    "trend_index_mature_trend_confidence_bonus": self.index_mature_trend_confidence_bonus,
                    "trend_index_mature_trend_confidence_bonus_applied": (
                        self._index_mature_trend_confidence_bonus(
                            trend_profile=trend_profile,
                            ema_gap_ratio=ema_gap_ratio,
                            directional_velocity_ratio=directional_velocity_ratio,
                            ema_gap_velocity_ratio=ema_gap_velocity_ratio,
                            overextension_ratio=overextension_ratio,
                        )
                        > 0.0
                    ),
                    "trend_internal_min_confidence_for_entry": self.internal_min_confidence_for_entry,
                    "trend_internal_min_confidence_source": self.internal_min_confidence_source,
                    "trend_internal_confidence_mode": self.internal_confidence_mode,
                    "timeframe_sec": timeframe_sec,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "using_closed_candles": using_closed_candles,
                    "trend_live_candle_entry_enabled": self.live_candle_entry_enabled,
                    "using_live_candle_entry": using_live_candle_entry,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "trend_crypto_default_pip_size": self.crypto_default_pip_size,
                    "trend_crypto_allow_pair_default_pip_size": self.crypto_allow_pair_default_pip_size,
                    "atr": atr,
                    "atr_pct": atr_pct,
                    "atr_price_source": atr_price_source,
                    "crypto_min_atr_pct": self.crypto_min_atr_pct,
                    "effective_crypto_min_atr_pct": effective_crypto_min_atr_pct,
                    "crypto_min_atr_pct_scale_mode": self.crypto_min_atr_pct_scale_mode,
                    "crypto_min_atr_pct_baseline_sec": self.crypto_min_atr_pct_baseline_sec,
                    "crypto_min_atr_pct_min_ratio": self.crypto_min_atr_pct_min_ratio,
                    "atr_threshold_sampling_interval_sec": atr_threshold_sampling_interval_sec,
                    "atr_threshold_sampling_ratio": atr_threshold_sampling_ratio,
                    "spread_pips": spread_pips,
                    "trend_max_spread_pips": max_spread_pips_limit,
                    "trend_max_spread_pips_source": max_spread_pips_source,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "trend_spread_buffer_factor": self.spread_buffer_factor,
                    "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                    "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    "trend_entry_stop_invalidation_enabled": self.entry_stop_invalidation_enabled,
                    "trend_crypto_entry_stop_invalidation_enabled": self.crypto_entry_stop_invalidation_enabled,
                    "trend_entry_stop_invalidation_enabled_effective": entry_stop_invalidation_enabled_for_entry,
                    "trend_entry_stop_invalidation_atr_buffer": self.entry_stop_invalidation_atr_buffer,
                    "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                    "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                    "trend_index_min_stop_pct": self.index_min_stop_pct,
                    "trend_index_min_stop_pct_max_entry_ratio": self.index_min_stop_pct_max_entry_ratio,
                    "index_stop_pct_raw_pips": index_stop_pct_raw_pips,
                    "index_stop_pct_effective_pips": index_stop_pct_effective_pips,
                    "index_stop_pct_clipped_by_entry_ratio": index_stop_pct_clipped_by_entry_ratio,
                    "structure_stop_pips": structure_stop_pips,
                    "stop_loss_before_entry_invalidation_pips": stop_loss_before_entry_invalidation_pips,
                    "entry_stop_invalidation_pips": entry_stop_invalidation_pips,
                    "entry_stop_invalidation_anchor_price": entry_stop_invalidation_anchor_price,
                    "entry_stop_invalidation_buffer_pips": entry_stop_invalidation_buffer_pips,
                    "entry_stop_invalidation_applied": entry_stop_invalidation_applied,
                    "stop_cap_atr": stop_cap_atr,
                    "stop_loss_cap_pips": stop_loss_cap_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "stop_source": "channel_lower",
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    "volume_penalty": volume_penalty,
                    "volume_softened": volume_softened,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    **bounce_payload,
                    **runaway_payload,
                    **volume_meta,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if trend_down and breakout_filter_down:
            kama_gate_passes, kama_payload = self._kama_gate_payload(prices, atr, trend="down")
            if not kama_gate_passes:
                kama_penalty = self._SECONDARY_KAMA_PENALTY
                kama_softened = True
            else:
                kama_penalty = 0.0
                kama_softened = False
            runaway_entry_allowed, runaway_payload = self._runaway_entry_allows(
                trend="down",
                prices=prices,
                last_price=last_price,
                prev_price=prev_price,
                fast_now=fast_now,
                fast_prev=fast_prev,
                ema_gap_ratio=ema_gap_ratio,
                directional_velocity_ratio=directional_velocity_ratio_down,
                effective_min_ema_gap_ratio=effective_min_ema_gap_ratio,
                effective_min_fast_slope_ratio=effective_min_fast_slope_ratio,
                effective_min_slow_slope_ratio=effective_min_slow_slope_ratio,
                pullback_distance_atr=pullback_distance_atr,
                max_pullback_distance_atr=max_pullback_distance_atr,
                atr=atr,
                channel_upper=channel_upper,
                channel_lower=channel_lower,
                adx=runaway_adx,
                ema_gap_velocity_ratio=ema_gap_velocity_ratio,
            )
            if is_crypto and (
                fast_slope_ratio > -self.crypto_min_fast_slope_ratio
                or slow_slope_ratio > -self.crypto_min_slow_slope_ratio
            ):
                trend_slope_penalty = max(trend_slope_penalty, self._SECONDARY_TREND_SLOPE_PENALTY)
                trend_slope_softened = True
                trend_slope_soft_reason = "crypto_slope_too_weak"
            entry_type = "pullback"
            if last_price >= slow_now:
                return self._hold(
                    "pullback_not_in_zone",
                    {
                        "trend": "down",
                        "pullback_sell_zone": pullback_sell_zone,
                        "price_below_slow_ema": last_price < slow_now,
                        "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                        "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                        "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                        **runaway_payload,
                    },
                )
            if not pullback_sell_zone:
                if not runaway_entry_allowed:
                    return self._hold(
                        "pullback_not_in_zone",
                        {
                            "trend": "down",
                            "pullback_sell_zone": pullback_sell_zone,
                            "price_below_slow_ema": last_price < slow_now,
                            "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                            "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                            "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                            **runaway_payload,
                        },
                    )
                entry_type = "runaway_continuation"
            if pullback_distance_ratio > self.pullback_max_distance_ratio:
                return self._hold(
                    "no_pullback_entry",
                    {
                        "trend": "down",
                        "pullback_distance_ratio": pullback_distance_ratio,
                        "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                        "pullback_channel_mid_distance_ratio": pullback_channel_mid_distance_ratio,
                        "pullback_max_distance_ratio": self.pullback_max_distance_ratio,
                        **runaway_payload,
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
                        **runaway_payload,
                    },
                )
            pullback_countermove_ratio = max(0.0, (last_price - prev_price) / max(abs(prev_price), FLOAT_COMPARISON_TOLERANCE))
            bounce_confirmed = False
            bounce_payload: dict[str, object] = {
                "trend_pullback_bounce_required": self.pullback_bounce_required,
                "trend_pullback_bounce_min_retrace_atr_ratio": self.pullback_bounce_min_retrace_atr_ratio,
                "bounce_confirmed": False,
            }
            if entry_type == "pullback":
                bounce_confirmed, bounce_payload = self._pullback_bounce_confirmation(
                    prices,
                    trend="down",
                    atr=atr,
                    open_price=candle_open_last,
                    high_price=candle_high_last,
                    low_price=candle_low_last,
                    close_price=last_price,
                )
            if pullback_countermove_ratio > self.pullback_max_countermove_ratio:
                return self._hold(
                    "pullback_countermove_in_progress",
                    {
                        "trend": "down",
                        "bounce_confirmed": bounce_confirmed,
                        "pullback_countermove_ratio": pullback_countermove_ratio,
                        "trend_pullback_max_countermove_ratio": self.pullback_max_countermove_ratio,
                        "trend_pullback_bounce_required": self.pullback_bounce_required,
                        **bounce_payload,
                        **runaway_payload,
                    },
                )
            if entry_type == "pullback" and self.pullback_bounce_required and not bounce_confirmed:
                return self._hold(
                    "pullback_not_confirmed",
                    {
                        "trend": "down",
                        **bounce_payload,
                        **runaway_payload,
                    },
                )

            structure_stop_pips = (channel_upper - last_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
            if structure_stop_pips <= 0:
                return self._hold("invalid_structure_stop", {"trend": "down"})

            entry_stop_invalidation_pips = None
            entry_stop_invalidation_anchor_price = None
            entry_stop_invalidation_buffer_pips = 0.0
            entry_stop_invalidation_enabled_for_entry = self.entry_stop_invalidation_enabled and (
                not is_crypto or self.crypto_entry_stop_invalidation_enabled
            )
            if entry_stop_invalidation_enabled_for_entry:
                (
                    entry_stop_invalidation_pips,
                    entry_stop_invalidation_anchor_price,
                    entry_stop_invalidation_buffer_pips,
                ) = self._entry_invalidation_stop_pips(
                    trend="down",
                    last_price=last_price,
                    fast_now=fast_now,
                    slow_now=slow_now,
                    atr=atr,
                    pip_size=pip_size,
                )

            stop_floor_pips = self.min_stop_loss_pips
            index_stop_pct_raw_pips, index_stop_pct_effective_pips, index_stop_pct_clipped_by_entry_ratio = (
                self._effective_index_stop_floor_pips(
                    symbol=ctx.symbol,
                    last_price=last_price,
                    pip_size=pip_size,
                    entry_stop_invalidation_pips=entry_stop_invalidation_pips,
                )
            )
            if index_stop_pct_effective_pips is not None:
                stop_floor_pips = max(stop_floor_pips, index_stop_pct_effective_pips)
            if is_crypto and self.crypto_min_stop_pct > 0:
                crypto_stop_pips = (last_price * self.crypto_min_stop_pct / 100.0) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
                stop_floor_pips = max(stop_floor_pips, crypto_stop_pips)

            stop_loss_pips = max(stop_floor_pips, structure_stop_pips)
            stop_loss_before_entry_invalidation_pips = stop_loss_pips
            entry_stop_invalidation_applied = False
            if entry_stop_invalidation_enabled_for_entry:
                if entry_stop_invalidation_pips is not None:
                    entry_capped_stop_pips = max(stop_floor_pips, entry_stop_invalidation_pips)
                    if entry_capped_stop_pips < stop_loss_pips:
                        stop_loss_pips = entry_capped_stop_pips
                        entry_stop_invalidation_applied = True
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
                stop_loss_cap_pips = (atr * stop_cap_atr) / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
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
            max_spread_pips_limit, max_spread_pips_source = self._spread_pips_limit_for_symbol(ctx.symbol)
            if max_spread_pips_limit > 0 and spread_pips >= max_spread_pips_limit:
                return self._hold(
                    "spread_too_wide",
                    {
                        "trend": "down",
                        "spread_pips": spread_pips,
                        "trend_max_spread_pips": max_spread_pips_limit,
                        "trend_max_spread_pips_source": max_spread_pips_source,
                    },
                )
            if self.spread_buffer_factor > 0 and spread_pips > 0:
                stop_loss_pips += spread_pips * self.spread_buffer_factor
            spread_to_stop_ratio = (
                spread_pips / max(stop_loss_pips, FLOAT_COMPARISON_TOLERANCE)
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
            overextension_ratio = max(0.0, channel_lower - last_price) / max(abs(last_price), FLOAT_COMPARISON_TOLERANCE)
            directional_velocity_ratio = directional_velocity_ratio_down
            price_aligned_with_fast_ma = last_price <= fast_now
            confidence = self._confidence(
                ema_gap_ratio=ema_gap_ratio,
                pullback_distance_ratio=pullback_distance_ratio,
                overextension_ratio=overextension_ratio,
                bounce_confirmed=bounce_confirmed,
                directional_velocity_ratio=directional_velocity_ratio,
                price_aligned_with_fast_ma=price_aligned_with_fast_ma,
                entry_type=entry_type,
                trend_profile=trend_profile,
                ema_gap_velocity_ratio=ema_gap_velocity_ratio,
                runaway_confidence_penalty=runaway_payload.get("runaway_confidence_penalty"),
            )
            volume_meta, volume_hold_reason, volume_confidence_boost = self._evaluate_volume_confirmation(ctx)
            if volume_hold_reason is not None:
                volume_penalty = (
                    self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                    if volume_hold_reason == "volume_unavailable"
                    else self._SECONDARY_VOLUME_PENALTY
                )
                volume_softened = True
            else:
                volume_penalty = 0.0
                volume_softened = False
            confidence = min(1.0, confidence + volume_confidence_boost)
            soft_filter_reasons: list[str] = []
            if trend_gap_softened and trend_gap_soft_reason is not None:
                soft_filter_reasons.append(trend_gap_soft_reason)
            if trend_slope_softened and trend_slope_soft_reason is not None:
                soft_filter_reasons.append(trend_slope_soft_reason)
            if kama_softened:
                soft_filter_reasons.append("kama_chop_regime")
            if volume_softened and volume_hold_reason is not None:
                soft_filter_reasons.append(volume_hold_reason)
            soft_filter_penalty_total = trend_gap_penalty + trend_slope_penalty + kama_penalty + volume_penalty
            confidence = max(0.0, confidence - soft_filter_penalty_total)
            if confidence < self.internal_min_confidence_for_entry:
                return self._hold(
                    "confidence_below_internal_threshold",
                    {
                        "trend": "down",
                        "confidence": confidence,
                        "trend_internal_min_confidence_for_entry": self.internal_min_confidence_for_entry,
                        "trend_internal_min_confidence_source": self.internal_min_confidence_source,
                        "trend_internal_confidence_mode": self.internal_confidence_mode,
                    },
                )
            signal = Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "ema_trend_following",
                    "invalid_prices_dropped": invalid_prices,
                    "trend": "down",
                    "trend_profile": trend_profile,
                    "entry_type": entry_type,
                    "fast_ema_window": self.fast_ema_window,
                    "slow_ema_window": self.slow_ema_window,
                    "donchian_window": self.donchian_window,
                    "use_donchian_filter_configured": self.use_donchian_filter,
                    "donchian_filter_applied": self.use_donchian_filter,
                    "donchian_breakout": breakout_down,
                    "recent_breakout": recent_breakout_down,
                    "channel_upper": channel_upper,
                    "channel_mid": channel_mid,
                    "channel_lower": channel_lower,
                    "pullback_distance_ratio": pullback_distance_ratio,
                    "pullback_distance_atr": pullback_distance_atr,
                    "pullback_ema_distance_ratio": pullback_ema_distance_ratio,
                    "pullback_channel_mid_distance_ratio": pullback_channel_mid_distance_ratio,
                    "pullback_countermove_ratio": pullback_countermove_ratio,
                    "trend_pullback_max_countermove_ratio": self.pullback_max_countermove_ratio,
                    "trend_pullback_bounce_min_retrace_atr_ratio": self.pullback_bounce_min_retrace_atr_ratio,
                    "ema_gap_ratio": ema_gap_ratio,
                    "prev_ema_gap_ratio": prev_ema_gap_ratio,
                    "ema_gap_velocity_ratio": ema_gap_velocity_ratio,
                    "fast_slope_ratio": fast_slope_ratio,
                    "slow_slope_ratio": slow_slope_ratio,
                    "fast_slope_ratio_raw": fast_slope_ratio_raw,
                    "slow_slope_ratio_raw": slow_slope_ratio_raw,
                    "trend_slope_window": self.slope_window,
                    "trend_min_ema_gap_ratio": effective_min_ema_gap_ratio,
                    "trend_min_fast_slope_ratio": effective_min_fast_slope_ratio,
                    "trend_min_slow_slope_ratio": effective_min_slow_slope_ratio,
                    "slow_slope_tolerance_ratio_configured": self.slow_slope_tolerance_ratio,
                    "slow_slope_tolerance_ratio_effective": effective_slow_slope_tolerance_ratio,
                    "trend_slow_slope_tolerance_ratio_cap_non_crypto": self.slow_slope_tolerance_ratio_cap_non_crypto,
                    "directional_velocity_ratio": directional_velocity_ratio,
                    "price_aligned_with_fast_ma": price_aligned_with_fast_ma,
                    "slope_mode": self.slope_mode,
                    "confidence_model": "velocity_weighted_v4",
                    "trend_confidence_velocity_norm_ratio": self.confidence_velocity_norm_ratio,
                    "trend_confidence_gap_velocity_positive_threshold": self.confidence_gap_velocity_positive_threshold,
                    "trend_confidence_gap_velocity_negative_threshold": self.confidence_gap_velocity_negative_threshold,
                    "trend_confidence_gap_velocity_positive_bonus": self.confidence_gap_velocity_positive_bonus,
                    "trend_confidence_gap_velocity_negative_penalty": self.confidence_gap_velocity_negative_penalty,
                    "trend_gap_penalty": trend_gap_penalty,
                    "trend_gap_softened": trend_gap_softened,
                    "trend_slope_penalty": trend_slope_penalty,
                    "trend_slope_softened": trend_slope_softened,
                    "kama_penalty": kama_penalty,
                    "kama_softened": kama_softened,
                    **kama_payload,
                    "trend_index_mature_trend_confidence_bonus": self.index_mature_trend_confidence_bonus,
                    "trend_index_mature_trend_confidence_bonus_applied": (
                        self._index_mature_trend_confidence_bonus(
                            trend_profile=trend_profile,
                            ema_gap_ratio=ema_gap_ratio,
                            directional_velocity_ratio=directional_velocity_ratio,
                            ema_gap_velocity_ratio=ema_gap_velocity_ratio,
                            overextension_ratio=overextension_ratio,
                        )
                        > 0.0
                    ),
                    "trend_internal_min_confidence_for_entry": self.internal_min_confidence_for_entry,
                    "trend_internal_min_confidence_source": self.internal_min_confidence_source,
                    "trend_internal_confidence_mode": self.internal_confidence_mode,
                    "timeframe_sec": timeframe_sec,
                    "candle_timeframe_sec": self.candle_timeframe_sec,
                    "resample_mode": self.resample_mode,
                    "ignore_sunday_candles": self.ignore_sunday_candles,
                    "using_closed_candles": using_closed_candles,
                    "trend_live_candle_entry_enabled": self.live_candle_entry_enabled,
                    "using_live_candle_entry": using_live_candle_entry,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "trend_crypto_default_pip_size": self.crypto_default_pip_size,
                    "trend_crypto_allow_pair_default_pip_size": self.crypto_allow_pair_default_pip_size,
                    "atr": atr,
                    "atr_pct": atr_pct,
                    "atr_price_source": atr_price_source,
                    "crypto_min_atr_pct": self.crypto_min_atr_pct,
                    "effective_crypto_min_atr_pct": effective_crypto_min_atr_pct,
                    "crypto_min_atr_pct_scale_mode": self.crypto_min_atr_pct_scale_mode,
                    "crypto_min_atr_pct_baseline_sec": self.crypto_min_atr_pct_baseline_sec,
                    "crypto_min_atr_pct_min_ratio": self.crypto_min_atr_pct_min_ratio,
                    "atr_threshold_sampling_interval_sec": atr_threshold_sampling_interval_sec,
                    "atr_threshold_sampling_ratio": atr_threshold_sampling_ratio,
                    "spread_pips": spread_pips,
                    "trend_max_spread_pips": max_spread_pips_limit,
                    "trend_max_spread_pips_source": max_spread_pips_source,
                    "spread_to_stop_ratio": spread_to_stop_ratio,
                    "trend_spread_buffer_factor": self.spread_buffer_factor,
                    "trend_max_spread_to_stop_ratio": spread_to_stop_limit,
                    "trend_max_spread_to_stop_ratio_source": spread_to_stop_limit_source,
                    "trend_entry_stop_invalidation_enabled": self.entry_stop_invalidation_enabled,
                    "trend_crypto_entry_stop_invalidation_enabled": self.crypto_entry_stop_invalidation_enabled,
                    "trend_entry_stop_invalidation_enabled_effective": entry_stop_invalidation_enabled_for_entry,
                    "trend_entry_stop_invalidation_atr_buffer": self.entry_stop_invalidation_atr_buffer,
                    "pullback_ema_tolerance_ratio": pullback_ema_tolerance_ratio,
                    "pullback_ema_tolerance_ratio_effective": pullback_ema_tolerance_ratio_effective,
                    "trend_index_min_stop_pct": self.index_min_stop_pct,
                    "trend_index_min_stop_pct_max_entry_ratio": self.index_min_stop_pct_max_entry_ratio,
                    "index_stop_pct_raw_pips": index_stop_pct_raw_pips,
                    "index_stop_pct_effective_pips": index_stop_pct_effective_pips,
                    "index_stop_pct_clipped_by_entry_ratio": index_stop_pct_clipped_by_entry_ratio,
                    "structure_stop_pips": structure_stop_pips,
                    "stop_loss_before_entry_invalidation_pips": stop_loss_before_entry_invalidation_pips,
                    "entry_stop_invalidation_pips": entry_stop_invalidation_pips,
                    "entry_stop_invalidation_anchor_price": entry_stop_invalidation_anchor_price,
                    "entry_stop_invalidation_buffer_pips": entry_stop_invalidation_buffer_pips,
                    "entry_stop_invalidation_applied": entry_stop_invalidation_applied,
                    "stop_cap_atr": stop_cap_atr,
                    "stop_loss_cap_pips": stop_loss_cap_pips,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "stop_source": "channel_upper",
                    "volume_confidence_boost_applied": volume_confidence_boost,
                    "volume_penalty": volume_penalty,
                    "volume_softened": volume_softened,
                    "soft_filter_penalty_total": soft_filter_penalty_total,
                    "soft_filter_reasons": soft_filter_reasons,
                    "soft_filter_count": len(soft_filter_reasons),
                    **bounce_payload,
                    **runaway_payload,
                    **volume_meta,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if (trend_up and not breakout_filter_up) or (trend_down and not breakout_filter_down):
            return self._hold(
                "trend_filter_not_confirmed",
                {
                    "trend_up": trend_up,
                    "trend_down": trend_down,
                    "recent_breakout_up": recent_breakout_up,
                    "recent_breakout_down": recent_breakout_down,
                    "use_donchian_filter_configured": self.use_donchian_filter,
                    "donchian_filter_applied": self.use_donchian_filter,
                },
            )

        return self._hold("no_signal")
