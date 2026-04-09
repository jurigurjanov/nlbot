from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import json
import math
from datetime import datetime, timezone
from datetime import timedelta
from collections.abc import Sequence

from xtb_bot.models import Side, Signal
from xtb_bot.numeric import adx_from_close, atr_wilder, mean_std, rsi_sma, rsi_wilder, session_vwap_bands, tail_mean
from xtb_bot.pip_size import is_fx_symbol
from xtb_bot.symbols import is_index_symbol, normalize_symbol
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanReversionBbStrategy(Strategy):
    name = "mean_reversion_bb"
    _SECONDARY_VWAP_RECLAIM_PENALTY = 0.10
    _SECONDARY_SESSION_PENALTY = 0.10
    _SECONDARY_REGIME_ADX_PENALTY = 0.12
    _SECONDARY_REGIME_VOLATILITY_PENALTY = 0.10
    _SECONDARY_TREND_FILTER_PENALTY = 0.12
    _SECONDARY_VOLUME_PENALTY = 0.10
    _SECONDARY_VOLUME_UNAVAILABLE_PENALTY = 0.12
    _SECONDARY_VOLUME_REVERSAL_PENALTY = 0.10
    _ALL_ASSET_CLASSES = frozenset({"fx", "index", "commodity", "crypto", "other"})
    _ASSET_CLASS_ALIASES = {
        "indices": "index",
        "commodities": "commodity",
        "metals": "commodity",
        "metal": "commodity",
        "energy": "commodity",
    }
    _COMMODITY_SYMBOLS = frozenset(
        {
            "GOLD",
            "SILVER",
            "WTI",
            "BRENT",
            "XAUUSD",
            "XAGUSD",
            "USOIL",
            "UKOIL",
            "NATGAS",
            "NGAS",
        }
    )
    _CRYPTO_BASES = frozenset(
        {
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
    )
    _CRYPTO_QUOTES = ("USD", "USDT", "USDC", "EUR", "GBP", "JPY", "BTC", "ETH")

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self._base_params: dict[str, object] = dict(params)
        self.allowed_asset_classes = self._parse_asset_class_set(
            params.get("mean_reversion_bb_allowed_asset_classes", "fx,index")
        )
        self.symbol_whitelist = self._parse_symbol_set(params.get("mean_reversion_bb_symbol_whitelist", ""))
        self.symbol_blacklist = self._parse_symbol_set(params.get("mean_reversion_bb_symbol_blacklist", ""))
        self.asset_class_param_overrides = self._parse_asset_class_overrides(
            params.get("mean_reversion_bb_params_by_asset_class", {})
        )
        self._active_asset_class: str | None = None
        self._last_synced_symbol: str | None = None
        self._regime_range_active_by_symbol: dict[str, bool] = {}
        self._last_invalid_prices_dropped = 0
        self._configure_runtime_params(self._base_params)

    def _configure_runtime_params(self, params: dict[str, object]) -> None:
        self.params = dict(params)
        self.bb_window = max(5, int(params.get("mean_reversion_bb_window", 20)))
        self.bb_std_dev = max(0.1, float(params.get("mean_reversion_bb_std_dev", 2.2)))
        self.entry_mode = str(params.get("mean_reversion_bb_entry_mode", "reentry")).strip().lower()
        if self.entry_mode not in {"touch", "reentry"}:
            self.entry_mode = "reentry"
        self.reentry_tolerance_sigma = max(
            0.0,
            float(params.get("mean_reversion_bb_reentry_tolerance_sigma", 0.18)),
        )
        self.reentry_min_reversal_sigma = max(
            0.0,
            float(params.get("mean_reversion_bb_reentry_min_reversal_sigma", 0.22)),
        )
        self.use_rsi_filter = self._as_bool(params.get("mean_reversion_bb_use_rsi_filter", True), True)
        self.rsi_period = max(2, int(params.get("mean_reversion_bb_rsi_period", 14)))
        self.rsi_history_multiplier = max(
            1.0,
            float(params.get("mean_reversion_bb_rsi_history_multiplier", 3.0)),
        )
        self.rsi_method = str(params.get("mean_reversion_bb_rsi_method", "wilder")).strip().lower()
        if self.rsi_method not in {"wilder", "sma"}:
            self.rsi_method = "wilder"
        self.rsi_overbought = min(100.0, max(0.0, float(params.get("mean_reversion_bb_rsi_overbought", 70.0))))
        self.rsi_oversold = min(100.0, max(0.0, float(params.get("mean_reversion_bb_rsi_oversold", 30.0))))
        self.oscillator_mode = str(params.get("mean_reversion_bb_oscillator_mode", "rsi")).strip().lower()
        if self.oscillator_mode not in {"rsi", "connors"}:
            self.oscillator_mode = "rsi"
        self.oscillator_gate_mode = str(
            params.get("mean_reversion_bb_oscillator_gate_mode", "hard")
        ).strip().lower()
        if self.oscillator_gate_mode not in {"hard", "soft"}:
            self.oscillator_gate_mode = "hard"
        self.connors_price_rsi_period = max(
            2,
            int(params.get("mean_reversion_bb_connors_price_rsi_period", 3)),
        )
        self.connors_streak_rsi_period = max(
            2,
            int(params.get("mean_reversion_bb_connors_streak_rsi_period", 2)),
        )
        self.connors_rank_period = max(
            5,
            int(params.get("mean_reversion_bb_connors_rank_period", 20)),
        )
        self.connors_price_rsi_weight = max(
            0.0,
            float(params.get("mean_reversion_bb_connors_price_rsi_weight", 0.5)),
        )
        self.connors_streak_rsi_weight = max(
            0.0,
            float(params.get("mean_reversion_bb_connors_streak_rsi_weight", 0.4)),
        )
        self.connors_rank_weight = max(
            0.0,
            float(params.get("mean_reversion_bb_connors_rank_weight", 0.1)),
        )
        connors_weight_sum = (
            self.connors_price_rsi_weight
            + self.connors_streak_rsi_weight
            + self.connors_rank_weight
        )
        if connors_weight_sum <= FLOAT_COMPARISON_TOLERANCE:
            self.connors_price_rsi_weight = 1.0
            self.connors_streak_rsi_weight = 1.0
            self.connors_rank_weight = 1.0
            connors_weight_sum = 3.0
        self.connors_weight_sum = connors_weight_sum
        self.oscillator_soft_zone_width = min(
            50.0,
            max(0.0, float(params.get("mean_reversion_bb_oscillator_soft_zone_width", 8.0))),
        )
        self.oscillator_soft_confidence_penalty = min(
            0.5,
            max(0.0, float(params.get("mean_reversion_bb_oscillator_soft_confidence_penalty", 0.18))),
        )
        self.candle_timeframe_sec = max(
            0,
            int(float(params.get("mean_reversion_bb_candle_timeframe_sec", 60.0))),
        )
        self.resample_mode = str(params.get("mean_reversion_bb_resample_mode", "auto")).strip().lower()
        if self.resample_mode not in {"auto", "off"}:
            self.resample_mode = "auto"
        self.ignore_sunday_candles = self._as_bool(
            params.get("mean_reversion_bb_ignore_sunday_candles", True),
            True,
        )

        self.min_std_ratio = max(0.0, float(params.get("mean_reversion_bb_min_std_ratio", 0.00005)))
        self.min_band_extension_ratio = max(
            0.0, float(params.get("mean_reversion_bb_min_band_extension_ratio", 0.08))
        )
        self.max_band_extension_ratio = max(
            0.0, float(params.get("mean_reversion_bb_max_band_extension_ratio", 2.0))
        )
        self.trend_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_trend_filter_enabled", False),
            False,
        )
        self.trend_ma_window = max(2, int(params.get("mean_reversion_bb_trend_ma_window", 100)))
        self.trend_filter_mode = str(
            params.get("mean_reversion_bb_trend_filter_mode", "strict")
        ).strip().lower()
        if self.trend_filter_mode not in {"strict", "extreme_override"}:
            self.trend_filter_mode = "strict"
        self.trend_filter_extreme_sigma = max(
            0.0, float(params.get("mean_reversion_bb_trend_filter_extreme_sigma", 2.5))
        )
        self.trend_slope_lookback_bars = max(
            1,
            int(params.get("mean_reversion_bb_trend_slope_lookback_bars", 5)),
        )
        self.trend_slope_strict_threshold = max(
            0.0,
            float(params.get("mean_reversion_bb_trend_slope_strict_threshold", 0.00015)),
        )
        self.trend_countertrend_min_distance_sigma = max(
            0.0,
            float(params.get("mean_reversion_bb_trend_countertrend_min_distance_sigma", 2.0)),
        )
        self.trend_slope_block_max_distance_sigma = max(
            0.0,
            float(params.get("mean_reversion_bb_trend_slope_block_max_distance_sigma", 1.8)),
        )
        self.regime_adx_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_regime_adx_filter_enabled", False),
            False,
        )
        self.regime_adx_window = max(2, int(params.get("mean_reversion_bb_regime_adx_window", 8)))
        self.regime_min_adx = max(0.0, float(params.get("mean_reversion_bb_regime_min_adx", 0.0)))
        self.regime_max_adx = max(0.0, float(params.get("mean_reversion_bb_regime_max_adx", 20.0)))
        self.regime_adx_hysteresis = max(
            0.0,
            float(params.get("mean_reversion_bb_regime_adx_hysteresis", 1.0)),
        )
        self.regime_use_hysteresis_state = self._as_bool(
            params.get("mean_reversion_bb_regime_use_hysteresis_state", True),
            True,
        )
        self.regime_volatility_expansion_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_regime_volatility_expansion_filter_enabled", False),
            False,
        )
        self.regime_volatility_short_window = max(
            2,
            int(params.get("mean_reversion_bb_regime_volatility_short_window", 14)),
        )
        self.regime_volatility_long_window = max(
            self.regime_volatility_short_window + 1,
            int(params.get("mean_reversion_bb_regime_volatility_long_window", 56)),
        )
        self.regime_volatility_expansion_max_ratio = max(
            1.0,
            float(params.get("mean_reversion_bb_regime_volatility_expansion_max_ratio", 1.35)),
        )
        self.session_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_session_filter_enabled", False),
            False,
        )
        self.session_start_hour_utc = int(float(params.get("mean_reversion_bb_session_start_hour_utc", 6.0))) % 24
        self.session_end_hour_utc = int(float(params.get("mean_reversion_bb_session_end_hour_utc", 22.0))) % 24
        self.vwap_filter_enabled = self._as_bool(
            params.get("mean_reversion_bb_vwap_filter_enabled", True),
            True,
        )
        self.vwap_target_enabled = self._as_bool(
            params.get("mean_reversion_bb_vwap_target_enabled", True),
            True,
        )
        self.vwap_reclaim_required = self._as_bool(
            params.get("mean_reversion_bb_vwap_reclaim_required", True),
            True,
        )
        self.vwap_entry_band_sigma = max(
            0.5,
            float(params.get("mean_reversion_bb_vwap_entry_band_sigma", 2.0)),
        )
        self.vwap_min_session_bars = max(
            2,
            int(params.get("mean_reversion_bb_vwap_min_session_bars", 8)),
        )
        self.vwap_min_volume_samples = max(
            1,
            int(params.get("mean_reversion_bb_vwap_min_volume_samples", 8)),
        )
        self.exit_on_midline = self._as_bool(params.get("mean_reversion_bb_exit_on_midline", False), False)
        self.exit_midline_tolerance_sigma = max(
            0.0, float(params.get("mean_reversion_bb_exit_midline_tolerance_sigma", 0.15))
        )
        self.rejection_gate_enabled = self._as_bool(
            params.get("mean_reversion_bb_rejection_gate_enabled", False),
            False,
        )
        self.rejection_min_wick_to_body_ratio = max(
            0.0,
            float(params.get("mean_reversion_bb_rejection_min_wick_to_body_ratio", 0.5)),
        )
        self.rejection_min_wick_to_range_ratio = max(
            0.0,
            float(params.get("mean_reversion_bb_rejection_min_wick_to_range_ratio", 0.25)),
        )
        self.rejection_sell_max_close_location = min(
            1.0,
            max(0.0, float(params.get("mean_reversion_bb_rejection_sell_max_close_location", 0.45))),
        )
        self.rejection_buy_min_close_location = min(
            1.0,
            max(0.0, float(params.get("mean_reversion_bb_rejection_buy_min_close_location", 0.55))),
        )

        self.use_atr_sl_tp = self._as_bool(params.get("mean_reversion_bb_use_atr_sl_tp", False), False)
        self.atr_window = max(2, int(params.get("mean_reversion_bb_atr_window", 14)))
        self.atr_multiplier = max(0.1, float(params.get("mean_reversion_bb_atr_multiplier", 1.5)))
        self.risk_reward_ratio = max(1.0, float(params.get("mean_reversion_bb_risk_reward_ratio", 2.0)))
        self.take_profit_mode = str(
            params.get("mean_reversion_bb_take_profit_mode", "rr")
        ).strip().lower()
        if self.take_profit_mode not in {"rr", "midline", "midline_or_rr"}:
            self.take_profit_mode = "rr"

        self.min_stop_loss_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_bb_min_stop_loss_pips",
                    params.get("mean_reversion_bb_stop_loss_pips", params.get("stop_loss_pips", 25.0)),
                )
            ),
        )
        self.min_take_profit_pips = max(
            0.1,
            float(
                params.get(
                    "mean_reversion_bb_min_take_profit_pips",
                    params.get("mean_reversion_bb_take_profit_pips", params.get("take_profit_pips", 40.0)),
                )
            ),
        )
        self.stop_loss_pips = self.min_stop_loss_pips
        self.take_profit_pips = self.min_take_profit_pips
        self.reentry_base_confidence = min(
            1.0,
            max(0.0, float(params.get("mean_reversion_bb_reentry_base_confidence", 0.70))),
        )
        self.reentry_rsi_bonus = min(
            0.5,
            max(0.0, float(params.get("mean_reversion_bb_reentry_rsi_bonus", 0.10))),
        )
        self.reentry_extension_confidence_weight = min(
            0.5,
            max(0.0, float(params.get("mean_reversion_bb_reentry_extension_confidence_weight", 0.15))),
        )
        volume_confirmation_raw = params.get("mean_reversion_bb_volume_confirmation", True)
        self.volume_confirmation = self._as_bool(volume_confirmation_raw, True)
        self.volume_window = max(2, int(params.get("mean_reversion_bb_volume_window", 20)))
        self.volume_min_ratio = max(1.0, float(params.get("mean_reversion_bb_min_volume_ratio", 1.5)))
        self.volume_min_samples = max(1, int(params.get("mean_reversion_bb_volume_min_samples", 10)))
        self.volume_allow_missing = self._as_bool(params.get("mean_reversion_bb_volume_allow_missing", True), True)
        self.volume_require_spike = self._as_bool(params.get("mean_reversion_bb_volume_require_spike", False), False)
        self.volume_require_spike_fx = self._as_optional_bool(
            params.get("mean_reversion_bb_volume_require_spike_fx")
        )
        self.volume_spike_mode = str(
            params.get("mean_reversion_bb_volume_spike_mode", "reversal_gate")
        ).strip().lower()
        if self.volume_spike_mode not in {"agnostic", "reversal_boost_only", "reversal_gate"}:
            self.volume_spike_mode = "reversal_gate"
        self.volume_confidence_boost = min(
            0.4,
            max(0.0, float(params.get("mean_reversion_bb_volume_confidence_boost", 0.20))),
        )

        rsi_history = 0
        if self.use_rsi_filter:
            if self.oscillator_mode == "connors":
                price_history = max(
                    self.connors_price_rsi_period + 1,
                    int(self.connors_price_rsi_period * self.rsi_history_multiplier) + 1,
                )
                streak_history = max(
                    self.connors_streak_rsi_period + 1,
                    int(self.connors_streak_rsi_period * self.rsi_history_multiplier) + 1,
                )
                rank_history = self.connors_rank_period + 1
                rsi_history = max(price_history, streak_history, rank_history)
            else:
                rsi_history = max(self.rsi_period + 1, int(self.rsi_period * self.rsi_history_multiplier) + 1)
        self.min_history = max(
            self.bb_window + 1,
            rsi_history,
            (self.atr_window + 1) if self.use_atr_sl_tp else 0,
            (self.trend_ma_window + self.trend_slope_lookback_bars) if self.trend_filter_enabled else 0,
            (self.regime_adx_window + 2) if self.regime_adx_filter_enabled else 0,
            (self.regime_volatility_long_window + 2)
            if self.regime_volatility_expansion_filter_enabled
            else 0,
        )

    @staticmethod
    def _split_tokens(raw: object) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            return [part.strip() for part in raw.split(",") if part.strip()]
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            tokens: list[str] = []
            for item in raw:
                text = str(item).strip()
                if text:
                    tokens.append(text)
            return tokens
        text = str(raw).strip()
        return [text] if text else []

    @classmethod
    def _parse_symbol_set(cls, raw: object) -> set[str]:
        result: set[str] = set()
        for token in cls._split_tokens(raw):
            normalized = normalize_symbol(token)
            if normalized:
                result.add(normalized)
        return result

    @classmethod
    def _normalize_asset_class(cls, raw: str) -> str | None:
        token = str(raw).strip().lower()
        if not token:
            return None
        token = cls._ASSET_CLASS_ALIASES.get(token, token)
        if token in cls._ALL_ASSET_CLASSES:
            return token
        return None

    @classmethod
    def _parse_asset_class_set(cls, raw: object) -> set[str]:
        classes: set[str] = set()
        for token in cls._split_tokens(raw):
            lowered = token.strip().lower()
            if lowered == "all":
                return set(cls._ALL_ASSET_CLASSES)
            normalized = cls._normalize_asset_class(token)
            if normalized is not None:
                classes.add(normalized)
        if classes:
            return classes
        return set(cls._ALL_ASSET_CLASSES)

    @classmethod
    def _parse_asset_class_overrides(cls, raw: object) -> dict[str, dict[str, object]]:
        payload: object = raw
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return {}
            try:
                payload = json.loads(text)
            except ValueError:
                return {}
        if not isinstance(payload, dict):
            return {}
        parsed: dict[str, dict[str, object]] = {}
        for key, value in payload.items():
            asset_class = cls._normalize_asset_class(str(key))
            if asset_class is None or not isinstance(value, dict):
                continue
            parsed[asset_class] = dict(value)
        return parsed

    @classmethod
    def _is_commodity_symbol(cls, symbol: str) -> bool:
        upper = normalize_symbol(symbol)
        if upper in cls._COMMODITY_SYMBOLS:
            return True
        return upper.startswith("XAU") or upper.startswith("XAG")

    @classmethod
    def _is_crypto_symbol(cls, symbol: str) -> bool:
        upper = normalize_symbol(symbol)
        if upper in cls._CRYPTO_BASES:
            return True
        compact = upper.replace("-", "").replace("_", "").replace(".", "")
        for quote in cls._CRYPTO_QUOTES:
            if not compact.endswith(quote) or len(compact) <= len(quote):
                continue
            base = compact[: -len(quote)]
            if base in cls._CRYPTO_BASES:
                return True
        return False

    @classmethod
    def _classify_symbol(cls, symbol: str) -> str:
        upper = normalize_symbol(symbol)
        if is_index_symbol(upper):
            return "index"
        if is_fx_symbol(upper):
            return "fx"
        if cls._is_commodity_symbol(upper):
            return "commodity"
        if cls._is_crypto_symbol(upper):
            return "crypto"
        return "other"

    def _resolve_params_for_asset_class(self, asset_class: str) -> dict[str, object]:
        merged: dict[str, object] = dict(self._base_params)
        override = self.asset_class_param_overrides.get(asset_class)
        if override:
            merged.update(override)
        return merged

    def _sync_params_for_symbol(self, symbol: str) -> str:
        asset_class = self._classify_symbol(symbol)
        normalized = normalize_symbol(symbol)
        if asset_class == self._active_asset_class and normalized == self._last_synced_symbol:
            return asset_class
        if asset_class != self._active_asset_class:
            self._configure_runtime_params(self._resolve_params_for_asset_class(asset_class))
            self._active_asset_class = asset_class
        self._last_synced_symbol = normalized
        return asset_class

    def supports_symbol(self, symbol: str) -> bool:
        normalized = normalize_symbol(symbol)
        if not normalized:
            return False
        if normalized in self.symbol_blacklist:
            return False
        if self.symbol_whitelist and normalized not in self.symbol_whitelist:
            return False
        asset_class = self._classify_symbol(normalized)
        return asset_class in self.allowed_asset_classes

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        return Strategy._as_bool(value, default, strict_strings=True)

    @staticmethod
    def _as_optional_bool(value: object) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if not text:
            return None
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return None

    def _effective_volume_require_spike(self) -> bool:
        # Forex tick-volume spikes frequently reflect MM spread dynamics rather than true
        # participation, so treat strict spike requirement as disabled by default for FX.
        if self._active_asset_class == "fx":
            if self.volume_require_spike_fx is not None:
                return bool(self.volume_require_spike_fx)
            return False
        return bool(self.volume_require_spike)

    @staticmethod
    def _mean_std(values: Sequence[float]) -> tuple[float, float]:
        return mean_std(values)

    def _rsi_sma(self, prices: Sequence[float]) -> float:
        return rsi_sma(prices, self.rsi_period)

    def _rsi_wilder(self, prices: Sequence[float]) -> float:
        return rsi_wilder(prices, self.rsi_period)

    def _rsi(self, prices: Sequence[float]) -> float:
        if self.rsi_method == "sma":
            return self._rsi_sma(prices)
        return self._rsi_wilder(prices)

    @staticmethod
    def _signed_streaks(prices: Sequence[float]) -> list[float]:
        if not prices:
            return []
        streaks: list[float] = [0.0]
        streak = 0.0
        for idx in range(1, len(prices)):
            current = float(prices[idx])
            prev = float(prices[idx - 1])
            if current > prev:
                streak = streak + 1.0 if streak > 0.0 else 1.0
            elif current < prev:
                streak = streak - 1.0 if streak < 0.0 else -1.0
            else:
                streak = 0.0
            streaks.append(streak)
        return streaks

    @staticmethod
    def _percent_rank(value: float, samples: Sequence[float]) -> float:
        if not samples:
            return 50.0
        count_less = 0
        count_equal = 0
        target = float(value)
        for raw in samples:
            sample = float(raw)
            if sample < target:
                count_less += 1
            elif abs(sample - target) <= FLOAT_ROUNDING_TOLERANCE:
                count_equal += 1
        return 100.0 * ((count_less + (0.5 * count_equal)) / float(len(samples)))

    def _connors_rsi(self, prices: Sequence[float]) -> tuple[float, dict[str, float]] | None:
        price_window = max(
            self.connors_price_rsi_period + 1,
            int(self.connors_price_rsi_period * self.rsi_history_multiplier) + 1,
        )
        streak_window = max(
            self.connors_streak_rsi_period + 1,
            int(self.connors_streak_rsi_period * self.rsi_history_multiplier) + 1,
        )
        rank_window = self.connors_rank_period
        if len(prices) < max(price_window, streak_window, rank_window + 1):
            return None
        price_rsi = rsi_wilder(prices[-price_window:], self.connors_price_rsi_period)
        streaks = self._signed_streaks(prices)
        streak_rsi = rsi_wilder(streaks[-streak_window:], self.connors_streak_rsi_period)
        roc1 = [float(prices[idx]) - float(prices[idx - 1]) for idx in range(1, len(prices))]
        if len(roc1) < rank_window:
            return None
        rank_samples = roc1[-rank_window:]
        rank_percent = self._percent_rank(roc1[-1], rank_samples)
        connors = (
            (price_rsi * self.connors_price_rsi_weight)
            + (streak_rsi * self.connors_streak_rsi_weight)
            + (rank_percent * self.connors_rank_weight)
        ) / max(self.connors_weight_sum, FLOAT_COMPARISON_TOLERANCE)
        components = {
            "connors_price_rsi": price_rsi,
            "connors_streak_rsi": streak_rsi,
            "connors_rank_percent": rank_percent,
            "connors_price_rsi_weight": self.connors_price_rsi_weight,
            "connors_streak_rsi_weight": self.connors_streak_rsi_weight,
            "connors_rank_weight": self.connors_rank_weight,
        }
        return connors, components

    def _oscillator_alignment(self, value: float, expected_side: Side) -> float:
        if not self.use_rsi_filter:
            return 1.0
        if expected_side == Side.SELL:
            threshold = self.rsi_overbought
            soft_edge = max(0.0, threshold - self.oscillator_soft_zone_width)
            if value >= threshold:
                return 1.0
            if value <= soft_edge:
                return 0.0
            return (value - soft_edge) / max(threshold - soft_edge, FLOAT_COMPARISON_TOLERANCE)
        threshold = self.rsi_oversold
        soft_edge = min(100.0, threshold + self.oscillator_soft_zone_width)
        if value <= threshold:
            return 1.0
        if value >= soft_edge:
            return 0.0
        return (soft_edge - value) / max(soft_edge - threshold, FLOAT_COMPARISON_TOLERANCE)

    @staticmethod
    def _atr(values: Sequence[float | Sequence[float]], window: int) -> float | None:
        return atr_wilder(values, window)

    @staticmethod
    def _adx(values: Sequence[float], window: int) -> float | None:
        return adx_from_close(values, window)

    @staticmethod
    def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    def _session_allows_entry(self, latest_timestamp: float | None) -> tuple[bool, dict[str, object]]:
        if not self.session_filter_enabled:
            return True, {"session_filter_enabled": False}
        payload: dict[str, object] = {
            "session_filter_enabled": True,
            "session_start_hour_utc": self.session_start_hour_utc,
            "session_end_hour_utc": self.session_end_hour_utc,
        }
        if latest_timestamp is None or latest_timestamp <= 0:
            payload["session_status"] = "missing_timestamp"
            return False, payload
        try:
            hour_utc = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).hour
        except (ValueError, OSError, OverflowError):
            payload["session_status"] = "invalid_timestamp"
            return False, payload
        payload["session_hour_utc"] = hour_utc
        allowed = self._hour_in_window(
            hour_utc,
            self.session_start_hour_utc,
            self.session_end_hour_utc,
        )
        payload["session_status"] = "ok" if allowed else "outside_window"
        return allowed, payload

    def _vwap_session_start_timestamp_utc(self, latest_timestamp: float | None) -> float | None:
        if latest_timestamp is None or latest_timestamp <= 0:
            return None
        try:
            latest_dt = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
        session_start = latest_dt.replace(
            hour=self.session_start_hour_utc,
            minute=0,
            second=0,
            microsecond=0,
        )
        if latest_dt.hour < self.session_start_hour_utc:
            session_start -= timedelta(days=1)
        return session_start.timestamp()

    def _session_vwap_payload(
        self,
        prices: Sequence[float],
        timestamps: Sequence[float],
        volumes: Sequence[float],
        latest_timestamp: float | None,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "vwap_filter_enabled": self.vwap_filter_enabled,
            "vwap_target_enabled": self.vwap_target_enabled,
            "vwap_reclaim_required": self.vwap_reclaim_required,
            "vwap_entry_band_sigma": self.vwap_entry_band_sigma,
            "vwap_min_session_bars": self.vwap_min_session_bars,
            "vwap_min_volume_samples": self.vwap_min_volume_samples,
        }
        if not (self.vwap_filter_enabled or self.vwap_target_enabled):
            payload["vwap_status"] = "disabled"
            return payload
        if not timestamps or not volumes or len(timestamps) != len(prices) or len(volumes) != len(prices):
            payload["vwap_status"] = "unavailable"
            return payload
        session_start_ts = self._vwap_session_start_timestamp_utc(latest_timestamp)
        if session_start_ts is None:
            payload["vwap_status"] = "missing_session_anchor"
            return payload
        payload["vwap_session_start_ts"] = session_start_ts

        session_prices: list[float] = []
        session_volumes: list[float] = []
        for price, volume, ts in zip(prices, volumes, timestamps):
            try:
                normalized_ts = self._timestamp_to_seconds(ts)
            except (TypeError, ValueError):
                continue
            if normalized_ts < session_start_ts:
                continue
            session_prices.append(float(price))
            session_volumes.append(float(volume))
        payload["vwap_session_bars"] = len(session_prices)
        if len(session_prices) < self.vwap_min_session_bars:
            payload["vwap_status"] = "insufficient_session_bars"
            return payload

        vwap_payload = session_vwap_bands(session_prices, session_volumes, band_multipliers=(1.0, self.vwap_entry_band_sigma))
        if vwap_payload is None:
            payload["vwap_status"] = "volume_unavailable"
            return payload
        valid_volume_samples = int(round(float(vwap_payload.get("valid_samples", 0.0))))
        payload["vwap_valid_volume_samples"] = valid_volume_samples
        if valid_volume_samples < self.vwap_min_volume_samples:
            payload["vwap_status"] = "insufficient_volume_samples"
            payload.update(vwap_payload)
            return payload

        vwap = float(vwap_payload["vwap"])
        sigma = float(vwap_payload["sigma"])
        payload.update(vwap_payload)
        payload["vwap_status"] = "ok"
        payload["vwap"] = vwap
        payload["vwap_sigma"] = sigma
        payload["vwap_upper_entry"] = vwap + sigma * self.vwap_entry_band_sigma
        payload["vwap_lower_entry"] = vwap - sigma * self.vwap_entry_band_sigma
        return payload

    def _regime_adx_allows_entry(
        self,
        symbol: str,
        adx: float | None,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "regime_adx_filter_enabled": self.regime_adx_filter_enabled,
            "regime_adx": adx,
            "regime_min_adx": self.regime_min_adx,
            "regime_max_adx": self.regime_max_adx,
            "regime_adx_hysteresis": self.regime_adx_hysteresis,
            "regime_use_hysteresis_state": self.regime_use_hysteresis_state,
        }
        if not self.regime_adx_filter_enabled:
            payload["regime_adx_status"] = "disabled"
            return True, payload
        if adx is None or adx < 0.0:
            payload["regime_adx_status"] = "unavailable"
            return False, payload
        key = normalize_symbol(symbol)
        if not key:
            key = str(symbol).strip().upper()
        min_adx = self.regime_min_adx
        max_adx = max(min_adx, self.regime_max_adx)
        if not self.regime_use_hysteresis_state:
            allowed = min_adx <= adx <= max_adx
            payload["regime_adx_entry_threshold"] = max_adx
            payload["regime_adx_exit_threshold"] = max_adx
            payload["regime_adx_min_entry_threshold"] = min_adx
            payload["regime_adx_min_exit_threshold"] = min_adx
            payload["regime_range_active_before"] = allowed
            payload["regime_range_active_after"] = allowed
            if allowed:
                payload["regime_adx_status"] = "ok"
            elif adx < min_adx:
                payload["regime_adx_status"] = "below_minimum"
            else:
                payload["regime_adx_status"] = "above_threshold"
            self._regime_range_active_by_symbol[key] = allowed
            return allowed, payload

        min_entry_threshold = min_adx + self.regime_adx_hysteresis
        min_exit_threshold = max(0.0, min_adx - self.regime_adx_hysteresis)
        entry_threshold = max(min_entry_threshold, max_adx - self.regime_adx_hysteresis)
        exit_threshold = max_adx + self.regime_adx_hysteresis
        active_before = self._regime_range_active_by_symbol.get(key, min_adx <= adx <= max_adx)
        if active_before:
            active_after = min_exit_threshold <= adx <= exit_threshold
        else:
            active_after = min_entry_threshold <= adx <= entry_threshold
        self._regime_range_active_by_symbol[key] = active_after
        payload["regime_adx_min_entry_threshold"] = min_entry_threshold
        payload["regime_adx_min_exit_threshold"] = min_exit_threshold
        payload["regime_adx_entry_threshold"] = entry_threshold
        payload["regime_adx_exit_threshold"] = exit_threshold
        payload["regime_range_active_before"] = active_before
        payload["regime_range_active_after"] = active_after
        if active_after:
            payload["regime_adx_status"] = "ok"
        elif adx < min_exit_threshold:
            payload["regime_adx_status"] = "below_minimum"
        else:
            payload["regime_adx_status"] = "above_threshold"
        return active_after, payload

    def _regime_volatility_allows_entry(
        self,
        atr_values: Sequence[float | Sequence[float]],
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "regime_volatility_expansion_filter_enabled": self.regime_volatility_expansion_filter_enabled,
            "regime_volatility_short_window": self.regime_volatility_short_window,
            "regime_volatility_long_window": self.regime_volatility_long_window,
            "regime_volatility_expansion_max_ratio": self.regime_volatility_expansion_max_ratio,
        }
        if not self.regime_volatility_expansion_filter_enabled:
            payload["regime_volatility_status"] = "disabled"
            return True, payload
        short_atr = self._atr(atr_values, self.regime_volatility_short_window)
        long_atr = self._atr(atr_values, self.regime_volatility_long_window)
        payload["regime_short_atr"] = short_atr
        payload["regime_long_atr"] = long_atr
        if (
            short_atr is None
            or long_atr is None
            or short_atr <= 0.0
            or long_atr <= 0.0
        ):
            payload["regime_volatility_status"] = "unavailable"
            return False, payload
        expansion_ratio = short_atr / max(long_atr, FLOAT_COMPARISON_TOLERANCE)
        payload["regime_volatility_expansion_ratio"] = expansion_ratio
        allowed = expansion_ratio <= self.regime_volatility_expansion_max_ratio
        payload["regime_volatility_status"] = "ok" if allowed else "expansion_too_high"
        return allowed, payload

    @staticmethod
    def _positive_finite_volumes(values: Sequence[float]) -> list[float]:
        return Strategy._extract_positive_finite_values(values)

    @staticmethod
    def _looks_like_cumulative_volume(samples: Sequence[float]) -> tuple[bool, int]:
        if len(samples) < 4:
            return False, 0
        non_decreasing = 0
        resets = 0
        comparisons = 0
        prev = float(samples[0])
        for raw in samples[1:]:
            current = float(raw)
            comparisons += 1
            if current >= prev:
                non_decreasing += 1
            elif current < (prev * 0.7):
                resets += 1
            prev = current
        if comparisons <= 0:
            return False, resets
        monotonic_ratio = non_decreasing / float(comparisons)
        return resets > 0 and monotonic_ratio >= 0.55, resets

    @staticmethod
    def _completed_candle_volumes_from_cumulative(samples: Sequence[float]) -> list[float]:
        if not samples:
            return []
        completed: list[float] = []
        segment_max = float(samples[0])
        prev = float(samples[0])
        for raw in samples[1:]:
            current = float(raw)
            if current < (prev * 0.7):
                completed.append(segment_max)
                segment_max = current
            else:
                segment_max = max(segment_max, current)
            prev = current
        return completed

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

    def _rejection_gate_allows_entry(
        self,
        *,
        expected_side: Side,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        upper: float,
        lower: float,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "rejection_gate_enabled": self.rejection_gate_enabled,
            "rejection_min_wick_to_body_ratio": self.rejection_min_wick_to_body_ratio,
            "rejection_min_wick_to_range_ratio": self.rejection_min_wick_to_range_ratio,
            "rejection_sell_max_close_location": self.rejection_sell_max_close_location,
            "rejection_buy_min_close_location": self.rejection_buy_min_close_location,
        }
        if not self.rejection_gate_enabled:
            payload["rejection_status"] = "disabled"
            return True, payload
        range_size, body_size, upper_wick, lower_wick, close_location = self._candle_rejection_metrics(
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )
        body_denominator = max(body_size, range_size * 0.05, FLOAT_COMPARISON_TOLERANCE)
        payload.update(
            {
                "rejection_range_size": range_size,
                "rejection_body_size": body_size,
                "rejection_upper_wick": upper_wick,
                "rejection_lower_wick": lower_wick,
                "rejection_close_location": close_location,
            }
        )
        if expected_side == Side.SELL:
            wick = upper_wick
            wick_to_body_ratio = wick / body_denominator
            wick_to_range_ratio = wick / range_size
            payload["rejection_wick_to_body_ratio"] = wick_to_body_ratio
            payload["rejection_wick_to_range_ratio"] = wick_to_range_ratio
            if close_price >= upper:
                payload["rejection_status"] = "close_not_back_inside"
                return False, payload
            if wick_to_body_ratio < self.rejection_min_wick_to_body_ratio:
                payload["rejection_status"] = "wick_to_body_too_small"
                return False, payload
            if wick_to_range_ratio < self.rejection_min_wick_to_range_ratio:
                payload["rejection_status"] = "wick_to_range_too_small"
                return False, payload
            if close_location > self.rejection_sell_max_close_location:
                payload["rejection_status"] = "close_location_not_bearish"
                return False, payload
            payload["rejection_status"] = "ok"
            return True, payload

        wick = lower_wick
        wick_to_body_ratio = wick / body_denominator
        wick_to_range_ratio = wick / range_size
        payload["rejection_wick_to_body_ratio"] = wick_to_body_ratio
        payload["rejection_wick_to_range_ratio"] = wick_to_range_ratio
        if close_price <= lower:
            payload["rejection_status"] = "close_not_back_inside"
            return False, payload
        if wick_to_body_ratio < self.rejection_min_wick_to_body_ratio:
            payload["rejection_status"] = "wick_to_body_too_small"
            return False, payload
        if wick_to_range_ratio < self.rejection_min_wick_to_range_ratio:
            payload["rejection_status"] = "wick_to_range_too_small"
            return False, payload
        if close_location < self.rejection_buy_min_close_location:
            payload["rejection_status"] = "close_location_not_bullish"
            return False, payload
        payload["rejection_status"] = "ok"
        return True, payload

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

        # Always exclude the current candle in resample mode. In auto mode,
        # candle-like feeds may still include a forming last bar.
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

    def _resample_closed_candle_series(
        self,
        values: Sequence[float],
        timestamps: Sequence[float],
    ) -> tuple[list[float], list[float], bool]:
        if self.candle_timeframe_sec <= 1 or self.resample_mode == "off":
            return [float(value) for value in values], [float(value) for value in timestamps], False
        if not timestamps or len(timestamps) != len(values):
            return [float(value) for value in values], [float(value) for value in timestamps], False

        normalized_timestamps = [self._timestamp_to_seconds(value) for value in timestamps]
        if self.resample_mode == "auto" and not self._is_candle_like_spacing(normalized_timestamps):
            return [float(value) for value in values], list(normalized_timestamps), False

        closes: list[float] = []
        close_timestamps: list[float] = []
        timeframe = max(1, self.candle_timeframe_sec)
        last_bucket: int | None = None
        for raw_value, ts in zip(values, normalized_timestamps):
            if self.ignore_sunday_candles:
                weekday = datetime.fromtimestamp(ts, tz=timezone.utc).weekday()
                if weekday == 6:
                    continue
            value = float(raw_value)
            bucket = int(ts // timeframe)
            if last_bucket is None or bucket != last_bucket:
                closes.append(value)
                close_timestamps.append(float(ts))
                last_bucket = bucket
            else:
                closes[-1] = value
                close_timestamps[-1] = float(ts)

        # Always exclude the current candle in resample mode. In auto mode,
        # candle-like feeds may still include a forming last bar.
        if len(closes) < 2:
            return [], [], True
        return closes[:-1], close_timestamps[:-1], True

    def _dynamic_sl_tp(
        self,
        ctx: StrategyContext,
        prices: Sequence[float],
        *,
        atr_values: Sequence[float | Sequence[float]] | None = None,
    ) -> tuple[float, float, float | None, float, str]:
        stop_pips = self.min_stop_loss_pips
        take_pips = max(self.min_take_profit_pips, stop_pips * self.risk_reward_ratio)
        atr_pips: float | None = None
        pip_size, pip_size_source = self._resolve_context_pip_size(ctx)
        if not self.use_atr_sl_tp:
            return stop_pips, take_pips, atr_pips, pip_size, pip_size_source
        atr_source = atr_values if atr_values is not None else prices
        atr = self._atr(atr_source, self.atr_window)
        if atr is None or atr <= 0:
            return stop_pips, take_pips, atr_pips, pip_size, pip_size_source
        atr_pips = atr / max(pip_size, FLOAT_COMPARISON_TOLERANCE)
        stop_pips = max(stop_pips, atr_pips * self.atr_multiplier)
        take_pips = max(take_pips, stop_pips * self.risk_reward_ratio)
        return stop_pips, take_pips, atr_pips, pip_size, pip_size_source

    def _target_take_profit(
        self,
        last: float,
        mean: float,
        pip_size: float,
        rr_take_pips: float,
        *,
        fair_value_price: float | None = None,
    ) -> tuple[float, float, float | None, float, bool]:
        midline_target_pips = max(0.0, abs(last - mean) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))
        fair_value_target_pips = None
        if fair_value_price is not None and math.isfinite(fair_value_price):
            fair_value_target_pips = max(0.0, abs(last - fair_value_price) / max(pip_size, FLOAT_COMPARISON_TOLERANCE))
        anchor_target_pips = (
            fair_value_target_pips
            if fair_value_target_pips is not None
            else midline_target_pips
        )
        if self.take_profit_mode == "rr":
            strategy_target_pips = rr_take_pips
        elif self.take_profit_mode == "midline":
            strategy_target_pips = anchor_target_pips
        else:
            strategy_target_pips = min(rr_take_pips, anchor_target_pips)
        execution_target_pips = max(self.min_take_profit_pips, strategy_target_pips)
        execution_floor_applied = execution_target_pips > (strategy_target_pips + FLOAT_COMPARISON_TOLERANCE)
        return (
            strategy_target_pips,
            midline_target_pips,
            fair_value_target_pips,
            execution_target_pips,
            execution_floor_applied,
        )

    def _trend_filter_allows(
        self,
        *,
        expected_side: Side,
        last: float,
        trend_ma: float | None,
        distance_sigma: float,
        trend_distance_sigma: float | None,
        trend_slope_ratio: float | None,
        reentry_confirmed: bool,
    ) -> tuple[bool, bool, dict[str, object]]:
        payload: dict[str, object] = {
            "trend_filter_mode": self.trend_filter_mode,
            "trend_filter_extreme_sigma": self.trend_filter_extreme_sigma,
            "trend_countertrend_min_distance_sigma": self.trend_countertrend_min_distance_sigma,
            "trend_override_reentry_confirmed": reentry_confirmed,
            "trend_override_distance_sigma_ready": distance_sigma >= self.trend_filter_extreme_sigma,
            "trend_override_trend_distance_sigma_ready": (
                trend_distance_sigma is not None
                and trend_distance_sigma >= self.trend_countertrend_min_distance_sigma
            ),
        }
        if not self.trend_filter_enabled or trend_ma is None:
            payload["trend_filter_status"] = "disabled"
            return True, False, payload

        blocked = (expected_side == Side.SELL and last >= trend_ma) or (
            expected_side == Side.BUY and last <= trend_ma
        )
        payload["trend_filter_blocked_by_trend_ma"] = blocked
        if not blocked:
            payload["trend_filter_status"] = "aligned"
            return True, False, payload

        adverse_slope_blocked = (
            trend_slope_ratio is not None
            and (
                (expected_side == Side.SELL and trend_slope_ratio > self.trend_slope_strict_threshold)
                or (expected_side == Side.BUY and trend_slope_ratio < -self.trend_slope_strict_threshold)
            )
        )
        payload["trend_override_adverse_slope_blocked"] = adverse_slope_blocked

        if self.trend_filter_mode != "extreme_override":
            payload["trend_filter_status"] = "blocked_strict_mode"
            return False, False, payload
        if not reentry_confirmed:
            payload["trend_filter_status"] = "override_requires_reentry"
            return False, False, payload
        if distance_sigma < self.trend_filter_extreme_sigma:
            payload["trend_filter_status"] = "override_not_extreme_enough"
            return False, False, payload
        if (
            trend_distance_sigma is None
            or trend_distance_sigma < self.trend_countertrend_min_distance_sigma
        ):
            payload["trend_filter_status"] = "override_not_far_enough_from_trend"
            return False, False, payload
        if adverse_slope_blocked:
            payload["trend_filter_status"] = "override_blocked_by_adverse_slope"
            return False, False, payload
        payload["trend_filter_status"] = "override_ok"
        return True, True, payload

    def _fx_volume_liquidity_allows(
        self,
        latest_timestamp: float | None,
    ) -> tuple[bool, dict[str, object]]:
        payload: dict[str, object] = {
            "volume_fx_liquidity_guard_enabled": self._active_asset_class == "fx",
            "volume_fx_liquidity_session_start_hour_utc": self.session_start_hour_utc,
            "volume_fx_liquidity_session_end_hour_utc": self.session_end_hour_utc,
        }
        if self._active_asset_class != "fx":
            payload["volume_fx_liquidity_status"] = "not_fx"
            return True, payload
        if latest_timestamp is None or latest_timestamp <= 0:
            payload["volume_fx_liquidity_status"] = "missing_timestamp"
            return False, payload
        try:
            hour_utc = datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).hour
        except (ValueError, OSError, OverflowError):
            payload["volume_fx_liquidity_status"] = "invalid_timestamp"
            return False, payload
        payload["volume_fx_liquidity_hour_utc"] = hour_utc
        allowed = self._hour_in_window(
            hour_utc,
            self.session_start_hour_utc,
            self.session_end_hour_utc,
        )
        payload["volume_fx_liquidity_status"] = "ok" if allowed else "outside_window"
        return allowed, payload

    def _trend_slope_ratio(self, prices: Sequence[float]) -> float | None:
        if not self.trend_filter_enabled:
            return None
        lookback = max(self.trend_slope_lookback_bars, 1)
        required = self.trend_ma_window + lookback
        if len(prices) < required:
            return None
        trend_now_slice = prices[-self.trend_ma_window :]
        trend_prev_slice = prices[-required:-lookback]
        trend_ma_now = sum(trend_now_slice) / max(len(trend_now_slice), 1)
        trend_ma_prev = sum(trend_prev_slice) / max(len(trend_prev_slice), 1)
        if abs(trend_ma_prev) <= FLOAT_ROUNDING_TOLERANCE:
            return 0.0
        return (trend_ma_now - trend_ma_prev) / trend_ma_prev

    def _volume_spike_payload(self, ctx: StrategyContext) -> tuple[bool, dict[str, float | str | bool]]:
        effective_volume_require_spike = self._effective_volume_require_spike()
        index_missing_volume_relaxed = is_index_symbol(ctx.symbol)
        effective_volume_allow_missing = self.volume_allow_missing or index_missing_volume_relaxed
        payload: dict[str, float | str | bool] = {
            "volume_confirmation": self.volume_confirmation,
            "volume_window": self.volume_window,
            "volume_min_ratio": self.volume_min_ratio,
            "volume_min_samples": self.volume_min_samples,
            "volume_allow_missing": self.volume_allow_missing,
            "volume_allow_missing_effective": effective_volume_allow_missing,
            "volume_missing_relaxed_for_index": index_missing_volume_relaxed and not self.volume_allow_missing,
            "volume_require_spike": effective_volume_require_spike,
            "volume_require_spike_configured": self.volume_require_spike,
            "volume_require_spike_fx_override": self.volume_require_spike_fx,
            "volume_spike_mode": self.volume_spike_mode,
        }
        if not self.volume_confirmation:
            payload["volume_status"] = "disabled"
            payload["vol_spike"] = False
            return False, payload

        history = self._positive_finite_volumes(ctx.volumes)
        current_volume = self._coerce_positive_finite(ctx.current_volume)
        payload["current_volume"] = current_volume
        payload["volume_history_len"] = len(history)
        if current_volume is None:
            if history:
                payload["current_volume_fallback_from_history"] = float(history[-1])
            payload["volume_status"] = "missing_current_volume"
            payload["vol_spike"] = False
            payload["volume_data_available"] = False
            return False, payload

        stream = list(history)
        current_deduplicated = bool(stream and self._values_close(stream[-1], current_volume))
        payload["volume_current_source"] = "ctx_volumes_last" if current_deduplicated else "context_current_volume"
        payload["volume_current_deduplicated"] = current_deduplicated
        if not current_deduplicated:
            stream.append(current_volume)
        cumulative_mode, resets_detected = self._looks_like_cumulative_volume(stream)
        payload["volume_resets_detected"] = resets_detected
        if cumulative_mode:
            baseline = self._completed_candle_volumes_from_cumulative(stream)
            payload["volume_series_mode"] = "cumulative_candle_max"
        else:
            baseline = list(history)
            if baseline and self._values_close(baseline[-1], current_volume):
                baseline = baseline[:-1]
            payload["volume_series_mode"] = "raw_samples"
        payload["volume_baseline_len"] = len(baseline)
        lookback = min(self.volume_window, len(baseline))
        payload["volume_lookback"] = lookback
        if lookback < self.volume_min_samples:
            payload["volume_status"] = "insufficient_volume_history"
            payload["vol_spike"] = False
            payload["volume_data_available"] = False
            return False, payload

        avg_volume = tail_mean(baseline, lookback)
        min_required = avg_volume * self.volume_min_ratio
        is_spike = current_volume >= min_required
        payload["avg_volume"] = avg_volume
        payload["min_required_volume"] = min_required
        payload["volume_ratio"] = current_volume / max(avg_volume, FLOAT_COMPARISON_TOLERANCE)
        payload["volume_status"] = "ok" if is_spike else "below_threshold"
        payload["vol_spike"] = is_spike
        payload["volume_data_available"] = True
        return is_spike, payload

    @staticmethod
    def _volume_reversal_confirmed(
        expected_side: Side,
        *,
        last: float,
        prev_last: float,
        upper: float,
        lower: float,
        reentry_sell: bool,
        reentry_buy: bool,
    ) -> bool:
        if expected_side == Side.SELL:
            if reentry_sell:
                return True
            return last < upper and last <= prev_last
        if reentry_buy:
            return True
        return last > lower and last >= prev_last

    def _volume_spike_context(
        self,
        *,
        expected_side: Side,
        volume_spike: bool,
        last: float,
        prev_last: float,
        upper: float,
        lower: float,
        reentry_sell: bool,
        reentry_buy: bool,
    ) -> tuple[bool, bool]:
        reversal_confirmed = self._volume_reversal_confirmed(
            expected_side,
            last=last,
            prev_last=prev_last,
            upper=upper,
            lower=lower,
            reentry_sell=reentry_sell,
            reentry_buy=reentry_buy,
        )
        boost_allowed = volume_spike and (
            self.volume_spike_mode == "agnostic"
            or (self.volume_spike_mode in {"reversal_boost_only", "reversal_gate"} and reversal_confirmed)
        )
        return reversal_confirmed, boost_allowed

    def _hold(self, reason: str, **extra: object) -> Signal:
        payload: dict[str, object] = {
            "indicator": "bollinger_bands",
            "reason": reason,
            "asset_class": self._active_asset_class,
        }
        payload.update(extra)
        if self._last_invalid_prices_dropped > 0 and "invalid_prices_dropped" not in payload:
            payload["invalid_prices_dropped"] = self._last_invalid_prices_dropped
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.min_stop_loss_pips,
            take_profit_pips=self.min_take_profit_pips,
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        self._last_invalid_prices_dropped = 0
        asset_class = self._sync_params_for_symbol(ctx.symbol)
        if not self.supports_symbol(ctx.symbol):
            return self._hold(
                "symbol_not_supported",
                symbol=normalize_symbol(ctx.symbol),
                asset_class=asset_class,
                allowed_asset_classes=sorted(self.allowed_asset_classes),
                symbol_whitelist=sorted(self.symbol_whitelist),
                symbol_blacklist=sorted(self.symbol_blacklist),
            )
        raw_prices, raw_timestamps, invalid_prices = self._extract_finite_prices_and_timestamps(
            ctx.prices,
            ctx.timestamps,
            timestamp_normalizer=self._timestamp_to_seconds,
        )
        self._last_invalid_prices_dropped = invalid_prices
        candle_opens, prices, candle_highs, candle_lows, price_timestamps, using_closed_candles = (
            self._resample_closed_candle_ohlc(
            raw_prices,
            raw_timestamps,
            )
        )
        if len(prices) < self.min_history:
            return self._hold(
                "insufficient_candle_history" if using_closed_candles else "insufficient_price_history",
                prices_len=len(prices),
                raw_prices_len=len(raw_prices),
                min_history=self.min_history,
                using_closed_candles=using_closed_candles,
                candle_timeframe_sec=self.candle_timeframe_sec,
                resample_mode=self.resample_mode,
                invalid_prices_dropped=invalid_prices,
            )

        bb_slice = prices[-self.bb_window :]
        mean, std = self._mean_std(bb_slice)
        if std <= FLOAT_ROUNDING_TOLERANCE:
            return self._hold("flat_volatility", bb_midline=mean, bb_std=std)

        volatility_ratio = std / max(abs(mean), FLOAT_COMPARISON_TOLERANCE)
        if volatility_ratio < self.min_std_ratio:
            return self._hold(
                "low_volatility",
                bb_midline=mean,
                bb_std=std,
                volatility_ratio=volatility_ratio,
                min_volatility_ratio=self.min_std_ratio,
            )

        upper = mean + self.bb_std_dev * std
        lower = mean - self.bb_std_dev * std
        last = float(prices[-1])
        prev_last = float(prices[-2]) if len(prices) >= 2 else last
        candle_open_last = float(candle_opens[-1]) if candle_opens else last
        candle_high_last = float(candle_highs[-1]) if candle_highs else last
        candle_low_last = float(candle_lows[-1]) if candle_lows else last
        oscillator_payload: dict[str, object] = {
            "oscillator_mode": self.oscillator_mode,
            "oscillator_gate_mode": self.oscillator_gate_mode,
            "oscillator_soft_zone_width": self.oscillator_soft_zone_width,
            "oscillator_soft_confidence_penalty": self.oscillator_soft_confidence_penalty,
        }
        rsi_history_len = max(self.rsi_period + 1, int(self.rsi_period * self.rsi_history_multiplier) + 1)
        connors_history_len = max(
            max(
                self.connors_price_rsi_period + 1,
                int(self.connors_price_rsi_period * self.rsi_history_multiplier) + 1,
            ),
            max(
                self.connors_streak_rsi_period + 1,
                int(self.connors_streak_rsi_period * self.rsi_history_multiplier) + 1,
            ),
            self.connors_rank_period + 1,
        )
        rsi = 50.0
        oscillator_value = 50.0
        oscillator_history_len = 0
        if self.use_rsi_filter:
            if self.oscillator_mode == "connors":
                oscillator_history_len = connors_history_len
                if len(prices) < oscillator_history_len:
                    return self._hold(
                        "insufficient_oscillator_history",
                        prices_len=len(prices),
                        oscillator_mode=self.oscillator_mode,
                        oscillator_history_len=oscillator_history_len,
                        connors_price_rsi_period=self.connors_price_rsi_period,
                        connors_streak_rsi_period=self.connors_streak_rsi_period,
                        connors_rank_period=self.connors_rank_period,
                    )
                connors = self._connors_rsi(prices)
                if connors is None:
                    return self._hold(
                        "oscillator_unavailable",
                        oscillator_mode=self.oscillator_mode,
                        prices_len=len(prices),
                    )
                oscillator_value, connors_payload = connors
                oscillator_payload.update(connors_payload)
                rsi = float(connors_payload.get("connors_price_rsi", 50.0))
            else:
                oscillator_history_len = rsi_history_len
                if len(prices) < oscillator_history_len:
                    return self._hold(
                        "insufficient_rsi_history",
                        prices_len=len(prices),
                        rsi_history_len=oscillator_history_len,
                        rsi_period=self.rsi_period,
                        rsi_history_multiplier=self.rsi_history_multiplier,
                    )
                rsi = self._rsi(prices[-oscillator_history_len:])
                oscillator_value = rsi
        oscillator_payload["oscillator_history_len"] = oscillator_history_len
        oscillator_payload["oscillator_value"] = oscillator_value
        atr_values = [(candle_highs[idx], candle_lows[idx], prices[idx]) for idx in range(len(prices))]
        stop_loss_pips, rr_take_profit_pips, atr_pips, pip_size, pip_size_source = self._dynamic_sl_tp(
            ctx,
            prices,
            atr_values=atr_values,
        )
        (
            take_profit_pips,
            midline_target_pips,
            fair_value_target_pips,
            execution_take_profit_pips,
            execution_tp_floor_applied,
        ) = self._target_take_profit(last, mean, pip_size, rr_take_profit_pips)
        anchor_target_pips = fair_value_target_pips if fair_value_target_pips is not None else midline_target_pips
        midline_exit_target_respects_floor = anchor_target_pips >= (self.min_take_profit_pips - FLOAT_COMPARISON_TOLERANCE)
        midline_exit_armed = (
            self.exit_on_midline
            and not execution_tp_floor_applied
            and midline_exit_target_respects_floor
        )
        volume_ctx = ctx
        volume_series_for_bars: list[float] = []
        fair_value_anchor = mean
        fair_value_target_kind = "bb_midline"
        fair_value_distance_sigma = abs(last - mean) / max(std, FLOAT_COMPARISON_TOLERANCE)
        if using_closed_candles:
            closed_volume_history: list[float] = []
            closed_current_volume: float | None = None
            if raw_timestamps and len(ctx.volumes) == len(raw_prices):
                closed_volumes, _, _ = self._resample_closed_candle_series(ctx.volumes, raw_timestamps)
                if closed_volumes:
                    volume_series_for_bars = [float(value) for value in closed_volumes]
                    closed_volume_history = [float(value) for value in closed_volumes[:-1]]
                    candidate_current = float(closed_volumes[-1])
                    if candidate_current > 0.0:
                        closed_current_volume = candidate_current
            volume_ctx = StrategyContext(
                symbol=ctx.symbol,
                prices=prices,
                timestamps=price_timestamps,
                volumes=closed_volume_history,
                current_volume=closed_current_volume,
                current_spread_pips=ctx.current_spread_pips,
                tick_size=ctx.tick_size,
                pip_size=ctx.pip_size,
            )
        elif len(ctx.volumes) == len(prices):
            volume_series_for_bars = [float(value) for value in ctx.volumes]
        volume_spike, volume_payload = self._volume_spike_payload(volume_ctx)
        volume_require_spike = bool(
            volume_payload.get("volume_require_spike", self._effective_volume_require_spike())
        )
        volume_payload["using_closed_candles"] = using_closed_candles
        volume_payload["candle_timeframe_sec"] = float(self.candle_timeframe_sec)
        volume_payload["resample_mode"] = self.resample_mode
        trend_ma = None
        if self.trend_filter_enabled:
            trend_slice = prices[-self.trend_ma_window :]
            trend_ma = sum(trend_slice) / max(len(trend_slice), 1)
        trend_slope_ratio = self._trend_slope_ratio(prices)
        distance_sigma = abs(last - mean) / max(std, FLOAT_COMPARISON_TOLERANCE)
        trend_distance_sigma = (
            abs(last - trend_ma) / max(std, FLOAT_COMPARISON_TOLERANCE)
            if trend_ma is not None
            else None
        )
        latest_timestamp_sec: float | None = None
        if price_timestamps:
            latest_timestamp_sec = self._timestamp_to_seconds(price_timestamps[-1])
        vwap_payload = self._session_vwap_payload(
            prices,
            price_timestamps,
            volume_series_for_bars,
            latest_timestamp_sec,
        )
        vwap_status = str(vwap_payload.get("vwap_status") or "")
        vwap_available = vwap_status == "ok"
        vwap_price = float(vwap_payload["vwap"]) if vwap_available else None
        vwap_sigma = float(vwap_payload["vwap_sigma"]) if vwap_available else None
        vwap_upper_entry = float(vwap_payload["vwap_upper_entry"]) if vwap_available else None
        vwap_lower_entry = float(vwap_payload["vwap_lower_entry"]) if vwap_available else None
        if self.vwap_target_enabled and vwap_available and vwap_price is not None:
            fair_value_anchor = vwap_price
            fair_value_target_kind = "vwap"
            fair_value_distance_sigma = abs(last - vwap_price) / max(vwap_sigma or 0.0, FLOAT_COMPARISON_TOLERANCE)
            (
                take_profit_pips,
                midline_target_pips,
                fair_value_target_pips,
                execution_take_profit_pips,
                execution_tp_floor_applied,
            ) = self._target_take_profit(
                last,
                mean,
                pip_size,
                rr_take_profit_pips,
                fair_value_price=vwap_price,
            )
        session_allowed, session_payload = self._session_allows_entry(latest_timestamp_sec)
        fx_volume_liquidity_allowed, fx_volume_liquidity_payload = self._fx_volume_liquidity_allows(
            latest_timestamp_sec
        )
        volume_payload.update(fx_volume_liquidity_payload)
        raw_volume_spike = volume_spike
        volume_payload["volume_spike_raw"] = raw_volume_spike
        if raw_volume_spike and not fx_volume_liquidity_allowed:
            volume_spike = False
            volume_payload["volume_status"] = "outside_fx_liquid_session"
            volume_payload["vol_spike"] = False
        regime_adx = self._adx(prices, self.regime_adx_window) if self.regime_adx_filter_enabled else None
        regime_adx_allowed, regime_adx_payload = self._regime_adx_allows_entry(ctx.symbol, regime_adx)
        regime_volatility_allowed, regime_volatility_payload = self._regime_volatility_allows_entry(atr_values)
        regime_payload: dict[str, object] = {}
        regime_payload.update(session_payload)
        regime_payload.update(regime_adx_payload)
        regime_payload.update(regime_volatility_payload)

        oscillator_sell_alignment = self._oscillator_alignment(oscillator_value, Side.SELL)
        oscillator_buy_alignment = self._oscillator_alignment(oscillator_value, Side.BUY)
        if not self.use_rsi_filter:
            rsi_allows_sell = True
            rsi_allows_buy = True
        elif self.oscillator_gate_mode == "hard":
            rsi_allows_sell = oscillator_value >= self.rsi_overbought
            rsi_allows_buy = oscillator_value <= self.rsi_oversold
        else:
            rsi_allows_sell = True
            rsi_allows_buy = True

        reentry_sell = False
        reentry_buy = False
        prev_extension_ratio = 0.0
        reentry_depth_price = 0.0
        reentry_min_reversal_price = 0.0
        if self.entry_mode == "reentry" and len(prices) >= self.bb_window + 1:
            prev_bb_slice = prices[-(self.bb_window + 1) : -1]
            prev_mean, prev_std = self._mean_std(prev_bb_slice)
            prev_upper = prev_mean + self.bb_std_dev * prev_std
            prev_lower = prev_mean - self.bb_std_dev * prev_std
            prev_tolerance = self.reentry_tolerance_sigma * max(prev_std, 0.0)
            reentry_depth_price = self.reentry_tolerance_sigma * max(std, 0.0)
            reentry_min_reversal_price = self.reentry_min_reversal_sigma * max(prev_std, std, 0.0)
            # Require both re-entry into the band and first sign of reversal momentum.
            reentry_sell = (
                prev_last >= (prev_upper - prev_tolerance)
                and last <= (upper - reentry_depth_price)
                and last <= prev_last
                and (prev_last - last) >= reentry_min_reversal_price
            )
            reentry_buy = (
                prev_last <= (prev_lower + prev_tolerance)
                and last >= (lower + reentry_depth_price)
                and last >= prev_last
                and (last - prev_last) >= reentry_min_reversal_price
            )
            prev_band_width = max(prev_upper - prev_lower, FLOAT_COMPARISON_TOLERANCE)
            if prev_last >= prev_upper:
                prev_extension_ratio = (prev_last - prev_upper) / prev_band_width
            elif prev_last <= prev_lower:
                prev_extension_ratio = (prev_lower - prev_last) / prev_band_width

        bb_distance = 0.0
        if last >= upper:
            bb_distance = (last - upper) / max(abs(last), FLOAT_COMPARISON_TOLERANCE)
        elif last <= lower:
            bb_distance = (lower - last) / max(abs(last), FLOAT_COMPARISON_TOLERANCE)

        band_width = max(upper - lower, FLOAT_COMPARISON_TOLERANCE)
        band_extension_ratio = 0.0
        if last >= upper:
            band_extension_ratio = (last - upper) / band_width
        elif last <= lower:
            band_extension_ratio = (lower - last) / band_width

        if self.entry_mode == "touch":
            # One signal per excursion: trigger only on fresh crossing to the outer band.
            sell_trigger = last >= upper and prev_last < upper
            buy_trigger = last <= lower and prev_last > lower
        else:
            sell_trigger = reentry_sell
            buy_trigger = reentry_buy
        effective_extension_ratio = (
            band_extension_ratio if self.entry_mode == "touch" else max(prev_extension_ratio, 0.0)
        )
        extension_guard_active = (
            ((last >= upper) or (last <= lower))
            if self.entry_mode == "touch"
            else (sell_trigger or buy_trigger)
        )

        if (
            self.max_band_extension_ratio > 0
            and extension_guard_active
            and effective_extension_ratio > self.max_band_extension_ratio
        ):
            return self._hold(
                "band_extension_too_extreme",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                rsi=rsi,
                rsi_method=self.rsi_method,
                band_extension_ratio=band_extension_ratio,
                effective_extension_ratio=effective_extension_ratio,
                prev_extension_ratio=prev_extension_ratio,
                max_band_extension_ratio=self.max_band_extension_ratio,
                trend_ma=trend_ma,
            )

        if (
            self.min_band_extension_ratio > 0
            and extension_guard_active
            and effective_extension_ratio < self.min_band_extension_ratio
        ):
            return self._hold(
                "band_touch_too_shallow",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                rsi=rsi,
                rsi_method=self.rsi_method,
                band_extension_ratio=band_extension_ratio,
                effective_extension_ratio=effective_extension_ratio,
                prev_extension_ratio=prev_extension_ratio,
                min_band_extension_ratio=self.min_band_extension_ratio,
                trend_ma=trend_ma,
            )

        if self.entry_mode == "reentry" and (last >= upper or last <= lower) and not (sell_trigger or buy_trigger):
            return self._hold(
                "band_walk_no_reentry",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                last=last,
                prev_last=prev_last,
                rsi=rsi,
                rsi_method=self.rsi_method,
                entry_mode=self.entry_mode,
                trend_ma=trend_ma,
                trend_slope_ratio=trend_slope_ratio,
            )
        if self.entry_mode == "touch" and (last >= upper or last <= lower) and not (sell_trigger or buy_trigger):
            return self._hold(
                "band_walk_touch_no_fresh_cross",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                bb_std=std,
                last=last,
                prev_last=prev_last,
                rsi=rsi,
                rsi_method=self.rsi_method,
                entry_mode=self.entry_mode,
                trend_ma=trend_ma,
                trend_slope_ratio=trend_slope_ratio,
            )

        def _secondary_filter_penalty_payload(
            *,
            expected_side: Side,
            vwap_reclaim_confirmed: bool,
            trend_allowed: bool,
            volume_reversal_confirmed: bool,
        ) -> dict[str, object]:
            side_label = "buy" if expected_side == Side.BUY else "sell"
            vwap_reclaim_penalty = 0.0
            vwap_softened = False
            soft_filter_reasons: list[str] = []
            if self.vwap_filter_enabled and vwap_available and self.vwap_reclaim_required and not vwap_reclaim_confirmed:
                vwap_reclaim_penalty = self._SECONDARY_VWAP_RECLAIM_PENALTY
                vwap_softened = True
                soft_filter_reasons.append("vwap_reclaim_not_confirmed")

            session_penalty = 0.0
            session_softened = False
            if not session_allowed:
                session_penalty = self._SECONDARY_SESSION_PENALTY
                session_softened = True
                soft_filter_reasons.append("session_filter_blocked")

            regime_adx_penalty = 0.0
            regime_adx_softened = False
            if not regime_adx_allowed:
                regime_adx_penalty = self._SECONDARY_REGIME_ADX_PENALTY
                regime_adx_softened = True
                regime_adx_status = str(regime_adx_payload.get("regime_adx_status") or "")
                soft_filter_reasons.append(
                    "regime_adx_unavailable" if regime_adx_status == "unavailable" else "regime_adx_filter_blocked"
                )

            regime_volatility_penalty = 0.0
            regime_volatility_softened = False
            if not regime_volatility_allowed:
                regime_volatility_penalty = self._SECONDARY_REGIME_VOLATILITY_PENALTY
                regime_volatility_softened = True
                regime_volatility_status = str(regime_volatility_payload.get("regime_volatility_status") or "")
                soft_filter_reasons.append(
                    (
                        "regime_volatility_unavailable"
                        if regime_volatility_status == "unavailable"
                        else "regime_volatility_filter_blocked"
                    )
                )

            trend_filter_penalty = 0.0
            trend_filter_softened = False
            if not trend_allowed:
                trend_filter_penalty = self._SECONDARY_TREND_FILTER_PENALTY
                trend_filter_softened = True
                soft_filter_reasons.append("trend_filter_blocked")

            volume_penalty = 0.0
            volume_softened = False
            volume_soft_reason: str | None = None
            if self.volume_confirmation and volume_require_spike and not fx_volume_liquidity_allowed:
                volume_penalty = self._SECONDARY_VOLUME_PENALTY
                volume_softened = True
                volume_soft_reason = "volume_session_filter_blocked"
            elif self.volume_confirmation and not volume_spike:
                status = str(volume_payload.get("volume_status") or "")
                effective_allow_missing = bool(
                    volume_payload.get("volume_allow_missing_effective", self.volume_allow_missing)
                )
                if status in {"missing_current_volume", "insufficient_volume_history"} and not effective_allow_missing:
                    volume_penalty = self._SECONDARY_VOLUME_UNAVAILABLE_PENALTY
                    volume_softened = True
                    volume_soft_reason = "volume_data_unavailable"
                elif status in {"below_threshold", "outside_fx_liquid_session"} and volume_require_spike:
                    volume_penalty = self._SECONDARY_VOLUME_PENALTY
                    volume_softened = True
                    volume_soft_reason = "volume_below_threshold"
            if volume_soft_reason is not None:
                soft_filter_reasons.append(volume_soft_reason)

            volume_reversal_penalty = 0.0
            volume_reversal_softened = False
            if volume_spike and self.volume_spike_mode == "reversal_gate" and not volume_reversal_confirmed:
                volume_reversal_penalty = self._SECONDARY_VOLUME_REVERSAL_PENALTY
                volume_reversal_softened = True
                soft_filter_reasons.append("volume_spike_without_reversal")

            soft_filter_penalty_total = (
                vwap_reclaim_penalty
                + session_penalty
                + regime_adx_penalty
                + regime_volatility_penalty
                + trend_filter_penalty
                + volume_penalty
                + volume_reversal_penalty
            )
            return {
                "expected_side": side_label,
                "vwap_expected_reclaim": "lower_band_reclaim" if expected_side == Side.BUY else "upper_band_reclaim",
                "vwap_reclaim_penalty": vwap_reclaim_penalty,
                "vwap_softened": vwap_softened,
                "session_penalty": session_penalty,
                "session_softened": session_softened,
                "regime_adx_penalty": regime_adx_penalty,
                "regime_adx_softened": regime_adx_softened,
                "regime_volatility_penalty": regime_volatility_penalty,
                "regime_volatility_softened": regime_volatility_softened,
                "trend_filter_penalty": trend_filter_penalty,
                "trend_filter_softened": trend_filter_softened,
                "volume_penalty": volume_penalty,
                "volume_softened": volume_softened,
                "volume_soft_reason": volume_soft_reason,
                "volume_reversal_penalty": volume_reversal_penalty,
                "volume_reversal_softened": volume_reversal_softened,
                "soft_filter_penalty_total": soft_filter_penalty_total,
                "soft_filter_reasons": soft_filter_reasons,
                "soft_filter_count": len(soft_filter_reasons),
            }

        if sell_trigger and rsi_allows_sell:
            sell_vwap_reclaim_confirmed = (
                vwap_available
                and vwap_upper_entry is not None
                and prev_last >= vwap_upper_entry
                and last < vwap_upper_entry
            )
            sell_rejection_allowed, sell_rejection_payload = self._rejection_gate_allows_entry(
                expected_side=Side.SELL,
                open_price=candle_open_last,
                high_price=candle_high_last,
                low_price=candle_low_last,
                close_price=last,
                upper=upper,
                lower=lower,
            )
            if not sell_rejection_allowed:
                return self._hold(
                    "candle_rejection_not_confirmed",
                    expected_side="sell",
                    **oscillator_payload,
                    **sell_rejection_payload,
                )
            if (
                self.trend_filter_enabled
                and self.trend_filter_mode == "strict"
                and trend_slope_ratio is not None
                and trend_slope_ratio > self.trend_slope_strict_threshold
            ):
                return self._hold(
                    "trend_too_strong_up",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="sell",
                    trend_ma=trend_ma,
                    trend_slope_ratio=trend_slope_ratio,
                    trend_slope_strict_threshold=self.trend_slope_strict_threshold,
                    distance_sigma=distance_sigma,
                    trend_distance_sigma=trend_distance_sigma,
                    trend_slope_block_max_distance_sigma=self.trend_slope_block_max_distance_sigma,
                )
            trend_allowed, trend_override, trend_filter_payload = self._trend_filter_allows(
                expected_side=Side.SELL,
                last=last,
                trend_ma=trend_ma,
                distance_sigma=distance_sigma,
                trend_distance_sigma=trend_distance_sigma,
                trend_slope_ratio=trend_slope_ratio,
                reentry_confirmed=reentry_sell,
            )
            sell_reversal_confirmed, sell_volume_boost_allowed = self._volume_spike_context(
                expected_side=Side.SELL,
                volume_spike=volume_spike,
                last=last,
                prev_last=prev_last,
                upper=upper,
                lower=lower,
                reentry_sell=reentry_sell,
                reentry_buy=reentry_buy,
            )
            secondary_filter_payload = _secondary_filter_penalty_payload(
                expected_side=Side.SELL,
                vwap_reclaim_confirmed=sell_vwap_reclaim_confirmed,
                trend_allowed=trend_allowed,
                volume_reversal_confirmed=sell_reversal_confirmed,
            )
            extension_ratio = effective_extension_ratio
            band_score = min(1.0, max(0.0, extension_ratio) / 0.15)
            if self.use_rsi_filter:
                if self.oscillator_gate_mode == "soft":
                    rsi_score = oscillator_sell_alignment
                else:
                    rsi_score = min(
                        1.0,
                        max(
                            0.0,
                            (oscillator_value - self.rsi_overbought) / max(100.0 - self.rsi_overbought, FLOAT_COMPARISON_TOLERANCE),
                        ),
                    )
            else:
                rsi_score = min(1.0, abs(oscillator_value - 50.0) / 50.0)
            if self.entry_mode == "reentry":
                confidence = self.reentry_base_confidence
                confidence += rsi_score * self.reentry_rsi_bonus
                confidence += band_score * self.reentry_extension_confidence_weight
                if sell_volume_boost_allowed:
                    confidence += self.volume_confidence_boost
            else:
                confidence = 0.2 + band_score * 0.55 + rsi_score * 0.25
            oscillator_soft_penalty_applied = 0.0
            if self.use_rsi_filter and self.oscillator_gate_mode == "soft":
                oscillator_soft_penalty_applied = (
                    (1.0 - oscillator_sell_alignment) * self.oscillator_soft_confidence_penalty
                )
                confidence -= oscillator_soft_penalty_applied
            confidence -= float(secondary_filter_payload["soft_filter_penalty_total"])
            confidence = min(1.0, max(0.0, confidence))
            signal = Signal(
                side=Side.SELL,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "bollinger_bands",
                    "invalid_prices_dropped": invalid_prices,
                    "band": "upper",
                    "bb_window": self.bb_window,
                    "bb_std_dev": self.bb_std_dev,
                    "bb_midline": mean,
                    "upper": upper,
                    "lower": lower,
                    "rsi": rsi,
                    "rsi_method": self.rsi_method,
                    "use_rsi_filter": self.use_rsi_filter,
                    "oscillator_value": oscillator_value,
                    "oscillator_sell_alignment": oscillator_sell_alignment,
                    "oscillator_buy_alignment": oscillator_buy_alignment,
                    "oscillator_soft_penalty_applied": oscillator_soft_penalty_applied,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "take_profit_mode": self.take_profit_mode,
                    "strategy_take_profit_pips": take_profit_pips,
                    "execution_take_profit_pips": execution_take_profit_pips,
                    "execution_tp_floor_applied": execution_tp_floor_applied,
                    "midline_target_pips": midline_target_pips,
                    "fair_value_target_pips": fair_value_target_pips,
                    "fair_value_target_kind": fair_value_target_kind,
                    "fair_value_anchor_price": fair_value_anchor,
                    "fair_value_distance_sigma": fair_value_distance_sigma,
                    "rr_target_pips": rr_take_profit_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "distance_sigma": distance_sigma,
                    "trend_distance_sigma": trend_distance_sigma,
                    "asset_class": asset_class,
                    "entry_mode": self.entry_mode,
                    "reentry_tolerance_sigma": self.reentry_tolerance_sigma,
                    "reentry_min_reversal_sigma": self.reentry_min_reversal_sigma,
                    "reentry_depth_price": reentry_depth_price,
                    "reentry_min_reversal_price": reentry_min_reversal_price,
                    "trend_filter_mode": self.trend_filter_mode,
                    "trend_filter_override": trend_override,
                    "trend_slope_override": False,
                    "trend_ma": trend_ma,
                    "trend_slope_ratio": trend_slope_ratio,
                    "trend_slope_strict_threshold": self.trend_slope_strict_threshold,
                    "trend_countertrend_min_distance_sigma": self.trend_countertrend_min_distance_sigma,
                    "trend_slope_block_max_distance_sigma": self.trend_slope_block_max_distance_sigma,
                    "midline_exit_armed": midline_exit_armed,
                    "midline_exit_suppressed_by_tp_floor": self.exit_on_midline and execution_tp_floor_applied,
                    "midline_exit_target_respects_floor": midline_exit_target_respects_floor,
                    "midline_exit_suppressed_by_target_floor": (
                        self.exit_on_midline
                        and not execution_tp_floor_applied
                        and not midline_exit_target_respects_floor
                    ),
                    "midline_exit_min_take_profit_pips": self.min_take_profit_pips,
                    "volume_spike": volume_spike,
                    "volume_reversal_confirmed": sell_reversal_confirmed,
                    "volume_spike_boost_applied": sell_volume_boost_allowed,
                    "vwap_reclaim_confirmed": sell_vwap_reclaim_confirmed,
                    **oscillator_payload,
                    **sell_rejection_payload,
                    **trend_filter_payload,
                    **regime_payload,
                    **volume_payload,
                    **vwap_payload,
                    **secondary_filter_payload,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if buy_trigger and rsi_allows_buy:
            buy_vwap_reclaim_confirmed = (
                vwap_available
                and vwap_lower_entry is not None
                and prev_last <= vwap_lower_entry
                and last > vwap_lower_entry
            )
            buy_rejection_allowed, buy_rejection_payload = self._rejection_gate_allows_entry(
                expected_side=Side.BUY,
                open_price=candle_open_last,
                high_price=candle_high_last,
                low_price=candle_low_last,
                close_price=last,
                upper=upper,
                lower=lower,
            )
            if not buy_rejection_allowed:
                return self._hold(
                    "candle_rejection_not_confirmed",
                    expected_side="buy",
                    **oscillator_payload,
                    **buy_rejection_payload,
                )
            if (
                self.trend_filter_enabled
                and self.trend_filter_mode == "strict"
                and trend_slope_ratio is not None
                and trend_slope_ratio < -self.trend_slope_strict_threshold
            ):
                return self._hold(
                    "trend_too_strong_down",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    expected_side="buy",
                    trend_ma=trend_ma,
                    trend_slope_ratio=trend_slope_ratio,
                    trend_slope_strict_threshold=self.trend_slope_strict_threshold,
                    distance_sigma=distance_sigma,
                    trend_distance_sigma=trend_distance_sigma,
                    trend_slope_block_max_distance_sigma=self.trend_slope_block_max_distance_sigma,
                )
            trend_allowed, trend_override, trend_filter_payload = self._trend_filter_allows(
                expected_side=Side.BUY,
                last=last,
                trend_ma=trend_ma,
                distance_sigma=distance_sigma,
                trend_distance_sigma=trend_distance_sigma,
                trend_slope_ratio=trend_slope_ratio,
                reentry_confirmed=reentry_buy,
            )
            buy_reversal_confirmed, buy_volume_boost_allowed = self._volume_spike_context(
                expected_side=Side.BUY,
                volume_spike=volume_spike,
                last=last,
                prev_last=prev_last,
                upper=upper,
                lower=lower,
                reentry_sell=reentry_sell,
                reentry_buy=reentry_buy,
            )
            secondary_filter_payload = _secondary_filter_penalty_payload(
                expected_side=Side.BUY,
                vwap_reclaim_confirmed=buy_vwap_reclaim_confirmed,
                trend_allowed=trend_allowed,
                volume_reversal_confirmed=buy_reversal_confirmed,
            )
            extension_ratio = effective_extension_ratio
            band_score = min(1.0, max(0.0, extension_ratio) / 0.15)
            if self.use_rsi_filter:
                if self.oscillator_gate_mode == "soft":
                    rsi_score = oscillator_buy_alignment
                else:
                    rsi_score = min(
                        1.0,
                        max(
                            0.0,
                            (self.rsi_oversold - oscillator_value) / max(self.rsi_oversold, FLOAT_COMPARISON_TOLERANCE),
                        ),
                    )
            else:
                rsi_score = min(1.0, abs(oscillator_value - 50.0) / 50.0)
            if self.entry_mode == "reentry":
                confidence = self.reentry_base_confidence
                confidence += rsi_score * self.reentry_rsi_bonus
                confidence += band_score * self.reentry_extension_confidence_weight
                if buy_volume_boost_allowed:
                    confidence += self.volume_confidence_boost
            else:
                confidence = 0.2 + band_score * 0.55 + rsi_score * 0.25
            oscillator_soft_penalty_applied = 0.0
            if self.use_rsi_filter and self.oscillator_gate_mode == "soft":
                oscillator_soft_penalty_applied = (
                    (1.0 - oscillator_buy_alignment) * self.oscillator_soft_confidence_penalty
                )
                confidence -= oscillator_soft_penalty_applied
            confidence -= float(secondary_filter_payload["soft_filter_penalty_total"])
            confidence = min(1.0, max(0.0, confidence))
            signal = Signal(
                side=Side.BUY,
                confidence=confidence,
                stop_loss_pips=stop_loss_pips,
                take_profit_pips=take_profit_pips,
                metadata={
                    "indicator": "bollinger_bands",
                    "invalid_prices_dropped": invalid_prices,
                    "band": "lower",
                    "bb_window": self.bb_window,
                    "bb_std_dev": self.bb_std_dev,
                    "bb_midline": mean,
                    "upper": upper,
                    "lower": lower,
                    "rsi": rsi,
                    "rsi_method": self.rsi_method,
                    "use_rsi_filter": self.use_rsi_filter,
                    "oscillator_value": oscillator_value,
                    "oscillator_sell_alignment": oscillator_sell_alignment,
                    "oscillator_buy_alignment": oscillator_buy_alignment,
                    "oscillator_soft_penalty_applied": oscillator_soft_penalty_applied,
                    "atr_pips": atr_pips,
                    "atr_multiplier": self.atr_multiplier,
                    "risk_reward_ratio": self.risk_reward_ratio,
                    "take_profit_mode": self.take_profit_mode,
                    "strategy_take_profit_pips": take_profit_pips,
                    "execution_take_profit_pips": execution_take_profit_pips,
                    "execution_tp_floor_applied": execution_tp_floor_applied,
                    "midline_target_pips": midline_target_pips,
                    "fair_value_target_pips": fair_value_target_pips,
                    "fair_value_target_kind": fair_value_target_kind,
                    "fair_value_anchor_price": fair_value_anchor,
                    "fair_value_distance_sigma": fair_value_distance_sigma,
                    "rr_target_pips": rr_take_profit_pips,
                    "pip_size": pip_size,
                    "pip_size_source": pip_size_source,
                    "distance_sigma": distance_sigma,
                    "trend_distance_sigma": trend_distance_sigma,
                    "asset_class": asset_class,
                    "entry_mode": self.entry_mode,
                    "reentry_tolerance_sigma": self.reentry_tolerance_sigma,
                    "reentry_min_reversal_sigma": self.reentry_min_reversal_sigma,
                    "reentry_depth_price": reentry_depth_price,
                    "reentry_min_reversal_price": reentry_min_reversal_price,
                    "trend_filter_mode": self.trend_filter_mode,
                    "trend_filter_override": trend_override,
                    "trend_slope_override": False,
                    "trend_ma": trend_ma,
                    "trend_slope_ratio": trend_slope_ratio,
                    "trend_slope_strict_threshold": self.trend_slope_strict_threshold,
                    "trend_countertrend_min_distance_sigma": self.trend_countertrend_min_distance_sigma,
                    "trend_slope_block_max_distance_sigma": self.trend_slope_block_max_distance_sigma,
                    "midline_exit_armed": midline_exit_armed,
                    "midline_exit_suppressed_by_tp_floor": self.exit_on_midline and execution_tp_floor_applied,
                    "midline_exit_target_respects_floor": midline_exit_target_respects_floor,
                    "midline_exit_suppressed_by_target_floor": (
                        self.exit_on_midline
                        and not execution_tp_floor_applied
                        and not midline_exit_target_respects_floor
                    ),
                    "midline_exit_min_take_profit_pips": self.min_take_profit_pips,
                    "volume_spike": volume_spike,
                    "volume_reversal_confirmed": buy_reversal_confirmed,
                    "volume_spike_boost_applied": buy_volume_boost_allowed,
                    "vwap_reclaim_confirmed": buy_vwap_reclaim_confirmed,
                    **oscillator_payload,
                    **buy_rejection_payload,
                    **trend_filter_payload,
                    **regime_payload,
                    **volume_payload,
                    **vwap_payload,
                    **secondary_filter_payload,
                },
            )
            return self._finalize_entry_signal(signal, prices=prices)

        if midline_exit_armed:
            if fair_value_distance_sigma <= self.exit_midline_tolerance_sigma:
                return self._hold(
                    "mean_reversion_target_reached",
                    bb_midline=mean,
                    upper=upper,
                    lower=lower,
                    bb_std=std,
                    rsi=rsi,
                    rsi_method=self.rsi_method,
                    exit_hint=("close_on_vwap_touch" if fair_value_target_kind == "vwap" else "close_on_bb_midline"),
                    exit_action="close_position",
                    distance_sigma=distance_sigma,
                    fair_value_distance_sigma=fair_value_distance_sigma,
                    fair_value_target_kind=fair_value_target_kind,
                    fair_value_anchor_price=fair_value_anchor,
                    exit_tolerance_sigma=self.exit_midline_tolerance_sigma,
                    take_profit_mode=self.take_profit_mode,
                    strategy_take_profit_pips=take_profit_pips,
                    execution_take_profit_pips=execution_take_profit_pips,
                    execution_tp_floor_applied=execution_tp_floor_applied,
                    midline_target_pips=midline_target_pips,
                    fair_value_target_pips=fair_value_target_pips,
                    rr_target_pips=rr_take_profit_pips,
                    midline_exit_armed=midline_exit_armed,
                    midline_exit_suppressed_by_tp_floor=self.exit_on_midline and execution_tp_floor_applied,
                    midline_exit_target_respects_floor=midline_exit_target_respects_floor,
                    midline_exit_suppressed_by_target_floor=(
                        self.exit_on_midline
                        and not execution_tp_floor_applied
                        and not midline_exit_target_respects_floor
                    ),
                    midline_exit_min_take_profit_pips=self.min_take_profit_pips,
                    trend_ma=trend_ma,
                    **oscillator_payload,
                    **vwap_payload,
                )

        if last >= upper and not rsi_allows_sell:
            return self._hold(
                "rsi_filter_not_confirmed",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                rsi=rsi,
                rsi_method=self.rsi_method,
                expected_side="sell",
                trend_ma=trend_ma,
                **oscillator_payload,
            )
        if last <= lower and not rsi_allows_buy:
            return self._hold(
                "rsi_filter_not_confirmed",
                bb_midline=mean,
                upper=upper,
                lower=lower,
                rsi=rsi,
                rsi_method=self.rsi_method,
                expected_side="buy",
                trend_ma=trend_ma,
                **oscillator_payload,
            )
        return self._hold(
            "no_signal",
            bb_midline=mean,
            upper=upper,
            lower=lower,
                rsi=rsi,
                rsi_method=self.rsi_method,
                bb_distance=bb_distance,
                band_extension_ratio=band_extension_ratio,
                max_band_extension_ratio=self.max_band_extension_ratio,
                entry_mode=self.entry_mode,
                pip_size=pip_size,
                pip_size_source=pip_size_source,
                distance_sigma=distance_sigma,
                take_profit_mode=self.take_profit_mode,
                strategy_take_profit_pips=take_profit_pips,
                execution_take_profit_pips=execution_take_profit_pips,
                execution_tp_floor_applied=execution_tp_floor_applied,
                midline_target_pips=midline_target_pips,
                fair_value_target_pips=fair_value_target_pips,
                fair_value_target_kind=fair_value_target_kind,
                fair_value_anchor_price=fair_value_anchor,
                fair_value_distance_sigma=fair_value_distance_sigma,
                rr_target_pips=rr_take_profit_pips,
                midline_exit_armed=midline_exit_armed,
                midline_exit_suppressed_by_tp_floor=self.exit_on_midline and execution_tp_floor_applied,
                midline_exit_target_respects_floor=midline_exit_target_respects_floor,
                midline_exit_suppressed_by_target_floor=(
                    self.exit_on_midline
                    and not execution_tp_floor_applied
                    and not midline_exit_target_respects_floor
                ),
                midline_exit_min_take_profit_pips=self.min_take_profit_pips,
                trend_ma=trend_ma,
                trend_slope_ratio=trend_slope_ratio,
                volume_spike=volume_spike,
                **oscillator_payload,
                **regime_payload,
                **volume_payload,
                **vwap_payload,
            )
