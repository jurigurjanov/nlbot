from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import json
import logging
import math

from xtb_bot.models import RunMode


logger = logging.getLogger("xtb_bot.worker")


class WorkerConfigRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    @staticmethod
    def parse_strategy_names(raw: object) -> list[str]:
        if raw is None:
            return []
        values: list[str] = []
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                text = str(item or "").strip().lower()
                if text:
                    values.append(text)
            return values
        text = str(raw).strip()
        if not text:
            return []
        parts = [part.strip().lower() for part in text.split(",")]
        return [part for part in parts if part]

    @staticmethod
    def parse_weight_map(raw: object) -> dict[str, float]:
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
        result: dict[str, float] = {}
        for key, value in payload.items():
            name = str(key or "").strip().lower()
            if not name:
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(parsed) or parsed < 0:
                continue
            result[name] = parsed
        return result

    @staticmethod
    def parse_symbol_float_map(raw: object) -> dict[str, float]:
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
        result: dict[str, float] = {}
        for key, value in payload.items():
            symbol = str(key or "").strip().upper()
            if not symbol:
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(parsed):
                continue
            result[symbol] = parsed
        return result

    @staticmethod
    def normalize_strategy_symbols_scope(
        raw_map: dict[str, list[str]] | None,
    ) -> dict[str, set[str]]:
        if not isinstance(raw_map, dict):
            return {}
        normalized: dict[str, set[str]] = {}
        for raw_name, raw_symbols in raw_map.items():
            name = str(raw_name or "").strip().lower()
            if not name:
                continue
            if isinstance(raw_symbols, (list, tuple, set)):
                items = raw_symbols
            elif isinstance(raw_symbols, str):
                items = raw_symbols.split(",")
            else:
                continue
            symbols = {
                str(item or "").strip().upper()
                for item in items
                if str(item or "").strip()
            }
            if symbols:
                normalized[name] = symbols
        return normalized

    @staticmethod
    def normalize_strategy_label(value: object, fallback: str | None = None) -> str:
        normalized = str(value or "").strip().lower()
        if normalized:
            return normalized
        return str(fallback or "").strip().lower()

    def resolve_strategy_param_source_key(
        self,
        strategy_params: dict[str, object],
        suffix: str,
    ) -> str | None:
        worker = self.worker
        strategy_key = worker._strategy_param_namespace_label()
        mode_value = worker.mode.value if isinstance(worker.mode, RunMode) else str(worker.mode or "")
        mode_key = mode_value.strip().lower()
        candidates: list[str] = []
        if mode_key and strategy_key:
            candidates.append(f"{strategy_key}_{mode_key}_{suffix}")
        if mode_key:
            candidates.append(f"{mode_key}_{suffix}")
        if strategy_key:
            candidates.append(f"{strategy_key}_{suffix}")
        candidates.append(suffix)
        for key in candidates:
            if key in strategy_params:
                return key
        return None

    def log_effective_entry_gates(self, strategy_params: dict[str, object]) -> None:
        worker = self.worker
        min_conf_key = self.resolve_strategy_param_source_key(strategy_params, "min_confidence_for_entry")
        cooldown_key = self.resolve_strategy_param_source_key(strategy_params, "trade_cooldown_sec")
        cooldown_win_key = self.resolve_strategy_param_source_key(strategy_params, "trade_cooldown_win_sec")
        cooldown_loss_key = self.resolve_strategy_param_source_key(strategy_params, "trade_cooldown_loss_sec")
        cooldown_flat_key = self.resolve_strategy_param_source_key(strategy_params, "trade_cooldown_flat_sec")
        same_side_key = self.resolve_strategy_param_source_key(
            strategy_params,
            "same_side_reentry_win_cooldown_sec",
        )
        continuation_guard_key = self.resolve_strategy_param_source_key(
            strategy_params,
            "continuation_reentry_guard_enabled",
        )
        continuation_reset_key = self.resolve_strategy_param_source_key(
            strategy_params,
            "continuation_reentry_reset_on_opposite_signal",
        )
        logger.info(
            "Entry gates resolved | symbol=%s strategy=%s mode=%s min_conf=%.4f src=%s cooldown=%.1fs src=%s win=%.1fs src=%s loss=%.1fs src=%s flat=%.1fs src=%s same_side_win=%.1fs src=%s continuation_guard=%s src=%s continuation_reset=%s src=%s",
            worker.symbol,
            worker.strategy_name,
            worker.mode.value,
            worker.min_confidence_for_entry,
            (min_conf_key or "default"),
            worker.trade_cooldown_sec,
            (cooldown_key or "default"),
            worker.trade_cooldown_win_sec,
            (cooldown_win_key or "default"),
            worker.trade_cooldown_loss_sec,
            (cooldown_loss_key or "default"),
            worker.trade_cooldown_flat_sec,
            (cooldown_flat_key or "default"),
            worker.same_side_reentry_win_cooldown_sec,
            (same_side_key or "default"),
            worker.continuation_reentry_guard_enabled,
            (continuation_guard_key or "default"),
            worker.continuation_reentry_reset_on_opposite_signal,
            (continuation_reset_key or "default"),
        )

    def log_strategy_param_hints(self, strategy_params: dict[str, object]) -> None:
        worker = self.worker
        strategy_name_lower = str(worker.strategy_name or "").strip().lower()
        if strategy_name_lower == "g1":
            min_conf_key = self.resolve_strategy_param_source_key(strategy_params, "min_confidence_for_entry")
            cooldown_key = self.resolve_strategy_param_source_key(strategy_params, "trade_cooldown_sec")
            protective_enabled_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "protective_exit_enabled",
            )
            protective_loss_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "protective_exit_loss_ratio",
            )
            protective_adx_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "protective_exit_allow_adx_regime_loss",
            )
            debug_indicators_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "debug_indicators",
            )
            debug_interval_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "debug_indicators_interval_sec",
            )
            connectivity_key = self.resolve_strategy_param_source_key(
                strategy_params,
                "worker_connectivity_check_interval_sec",
            )
            logger.info(
                "G1 executor controls resolved | symbol=%s mode=%s min_conf=%.4f src=%s cooldown=%.1fs src=%s protective_enabled=%s src=%s protective_loss_ratio=%.2f src=%s protective_adx_loss=%s src=%s debug=%s src=%s debug_interval=%.1fs src=%s connectivity_interval=%.1fs src=%s",
                worker.symbol,
                worker.mode.value,
                worker.min_confidence_for_entry,
                (min_conf_key or "default"),
                worker.trade_cooldown_sec,
                (cooldown_key or "default"),
                worker.protective_exit_enabled,
                (protective_enabled_key or "default"),
                worker.protective_exit_loss_ratio,
                (protective_loss_key or "default"),
                worker.protective_exit_allow_adx_regime_loss,
                (protective_adx_key or "default"),
                worker.debug_indicators_enabled,
                (debug_indicators_key or "default"),
                worker.debug_indicators_interval_sec,
                (debug_interval_key or "default"),
                worker.connectivity_check_interval_sec,
                (connectivity_key or "default"),
            )

        if strategy_name_lower != "momentum":
            return
        strategy = worker.strategy
        auto_confirm = bool(getattr(strategy, "auto_confirm_by_timeframe", True))
        if not auto_confirm:
            ignored_keys = [
                key
                for key in (
                    "momentum_low_tf_min_confirm_bars",
                    "momentum_low_tf_max_confirm_bars",
                    "momentum_high_tf_max_confirm_bars",
                )
                if key in strategy_params
            ]
            if ignored_keys:
                logger.warning(
                    "Momentum auto-confirm disabled; timeframe confirm params are inert | symbol=%s ignored=%s",
                    worker.symbol,
                    ",".join(ignored_keys),
                )

        try:
            min_stop_loss_pips = float(getattr(strategy, "min_stop_loss_pips", 0.0))
            risk_reward_ratio = float(getattr(strategy, "risk_reward_ratio", 0.0))
            min_take_profit_pips = float(getattr(strategy, "min_take_profit_pips", 0.0))
        except (TypeError, ValueError):
            return
        if min_stop_loss_pips <= 0 or risk_reward_ratio <= 0:
            return
        rr_floor = min_stop_loss_pips * risk_reward_ratio
        if min_take_profit_pips <= (rr_floor + FLOAT_COMPARISON_TOLERANCE):
            logger.info(
                "Momentum min_take_profit_pips is dominated by RR floor | symbol=%s min_tp=%.2f rr_floor=%.2f (min_sl=%.2f rr=%.2f)",
                worker.symbol,
                min_take_profit_pips,
                rr_floor,
                min_stop_loss_pips,
                risk_reward_ratio,
            )
