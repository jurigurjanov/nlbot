from __future__ import annotations

import hashlib
import logging
import math
from typing import TYPE_CHECKING

from xtb_bot.bot._assignment import (
    WorkerAssignment,
    _strategy_params_signature,
)
from xtb_bot.bot._utils import _BoundedTtlCache
from xtb_bot.config import DEFAULT_MULTI_STRATEGY_COMPONENT_NAMES
from xtb_bot.models import PendingOpen, Position, RunMode
from xtb_bot.strategies import create_strategy

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)

_MULTI_STRATEGY_CARRIER_NAME = "multi_strategy"
_MULTI_STRATEGY_BASE_COMPONENT_PARAM = "_multi_strategy_base_component"


class BotStrategyAssignmentRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        self._strategy_symbol_filter = create_strategy(
            bot.config.strategy, bot.config.strategy_params
        )
        self._strategy_support_cache = _BoundedTtlCache(max_entries=2048, ttl_sec=1800.0)
        self._multi_strategy_rollout_mode_override_by_symbol = (
            self._resolve_multi_strategy_rollout_mode_overrides()
        )

    @staticmethod
    def _strategy_cache_identity(
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[str, tuple[tuple[str, object], ...]]:
        return (
            str(strategy_name).strip().lower(),
            _strategy_params_signature(strategy_params),
        )

    def _strategy_params_for(self, strategy_name: str) -> dict[str, object]:
        normalized = str(strategy_name).strip().lower()
        params = self._bot.config.strategy_params_map.get(normalized)
        if params is None:
            if normalized == str(self._bot.config.strategy).strip().lower():
                return dict(self._bot.config.strategy_params)
            return {}
        return dict(params)

    def _multi_strategy_carrier_params(
        self,
        *,
        base_strategy_name: str,
        strategy_params: dict[str, object],
    ) -> dict[str, object]:
        params = dict(strategy_params)
        normalized_base = str(base_strategy_name or "").strip().lower()
        if normalized_base and normalized_base != _MULTI_STRATEGY_CARRIER_NAME:
            params[_MULTI_STRATEGY_BASE_COMPONENT_PARAM] = normalized_base
        return params

    def _worker_assignment_payload(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> tuple[str, dict[str, object]]:
        normalized = str(strategy_name or "").strip().lower()
        params = dict(strategy_params)
        if normalized == _MULTI_STRATEGY_CARRIER_NAME:
            base_name = str(
                params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or self._bot.config.strategy
            ).strip().lower()
            return (
                _MULTI_STRATEGY_CARRIER_NAME,
                self._multi_strategy_carrier_params(
                    base_strategy_name=base_name,
                    strategy_params=params,
                ),
            )
        if self._multi_strategy_enabled_for_params(params):
            return (
                _MULTI_STRATEGY_CARRIER_NAME,
                self._multi_strategy_carrier_params(
                    base_strategy_name=normalized,
                    strategy_params=params,
                ),
            )
        return normalized, params

    @staticmethod
    def _normalize_strategy_label(value: object) -> str | None:
        text = str(value or "").strip().lower()
        return text or None

    def _strategy_base_label(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
    ) -> str | None:
        normalized_strategy = self._normalize_strategy_label(strategy_name)
        if normalized_strategy != _MULTI_STRATEGY_CARRIER_NAME:
            return None
        base_hint = self._normalize_strategy_label(strategy_entry_hint)
        if base_hint and base_hint != normalized_strategy:
            return base_hint
        params = strategy_params if isinstance(strategy_params, dict) else {}
        normalized_base = self._normalize_strategy_label(params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM))
        if normalized_base and normalized_base != normalized_strategy:
            return normalized_base
        return None

    def _strategy_labels(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
    ) -> tuple[str | None, str | None]:
        normalized_strategy = self._normalize_strategy_label(strategy_name)
        if not normalized_strategy:
            return None, None
        return normalized_strategy, self._strategy_base_label(
            normalized_strategy,
            strategy_params,
            strategy_entry_hint=strategy_entry_hint,
        )

    def _assignment_strategy_labels(self, assignment: WorkerAssignment | None) -> tuple[str | None, str | None]:
        if assignment is None:
            return None, None
        return self._strategy_labels(
            assignment.strategy_name,
            assignment.strategy_params,
        )

    def _strategy_event_payload(
        self,
        strategy_name: object,
        strategy_params: dict[str, object] | None = None,
        *,
        strategy_entry_hint: object | None = None,
        strategy_key: str = "strategy",
        base_key: str = "strategy_base",
    ) -> dict[str, object]:
        normalized_strategy, strategy_base = self._strategy_labels(
            strategy_name,
            strategy_params,
            strategy_entry_hint=strategy_entry_hint,
        )
        payload: dict[str, object] = {}
        if normalized_strategy:
            payload[strategy_key] = normalized_strategy
        if strategy_base:
            payload[base_key] = strategy_base
        return payload

    def _default_strategy_entry_for_assignment(self, assignment: WorkerAssignment | None) -> str | None:
        strategy_name, strategy_base = self._assignment_strategy_labels(assignment)
        return strategy_base or strategy_name

    def _apply_position_trade_identity(
        self,
        position: Position,
        *,
        strategy: object,
        strategy_entry: object | None = None,
        strategy_entry_component: object | None = None,
        strategy_entry_signal: object | None = None,
    ) -> None:
        normalized_strategy = self._normalize_strategy_label(strategy)
        normalized_entry = self._normalize_strategy_label(strategy_entry or strategy)
        normalized_component = self._normalize_strategy_label(
            strategy_entry_component or normalized_entry
        )
        normalized_signal = self._normalize_strategy_label(strategy_entry_signal)
        position.strategy = normalized_strategy
        position.strategy_base = self._strategy_base_label(
            normalized_strategy,
            strategy_entry_hint=normalized_entry,
        )
        position.strategy_entry = normalized_entry
        position.strategy_entry_component = normalized_component
        position.strategy_entry_signal = normalized_signal

    def _resolved_recovery_trade_identity(
        self,
        *,
        symbol: str,
        existing_row: dict[str, object] | None,
        matched_pending: PendingOpen | None,
        default_assignment: WorkerAssignment | None,
    ) -> tuple[str, str, str, str | None, str | None, str]:
        default_strategy = default_assignment.strategy_name if default_assignment is not None else self._bot.config.strategy
        default_strategy_entry = self._default_strategy_entry_for_assignment(default_assignment)
        pending_strategy = self._normalize_strategy_label(matched_pending.strategy) if matched_pending is not None else None
        pending_strategy_entry = (
            self._normalize_strategy_label(matched_pending.strategy_entry)
            if matched_pending is not None
            else None
        )
        pending_strategy_entry_component = (
            self._normalize_strategy_label(matched_pending.strategy_entry_component)
            if matched_pending is not None
            else None
        )
        pending_strategy_entry_signal = (
            self._normalize_strategy_label(matched_pending.strategy_entry_signal)
            if matched_pending is not None
            else None
        )
        thread_name = (
            str(existing_row.get("thread_name") or f"worker-{symbol}")
            if existing_row
            else (matched_pending.thread_name if matched_pending is not None else f"worker-{symbol}")
        )
        strategy = (
            str(existing_row.get("strategy") or default_strategy)
            if existing_row
            else (pending_strategy or default_strategy)
        )
        strategy_entry = (
            self._normalize_strategy_label(existing_row.get("strategy_entry") or strategy)
            if existing_row
            else (pending_strategy_entry or pending_strategy or (default_strategy_entry or strategy))
        )
        strategy_entry_component = (
            self._normalize_strategy_label(existing_row.get("strategy_entry_component"))
            if existing_row
            else (pending_strategy_entry_component or pending_strategy_entry)
        )
        strategy_entry_signal = (
            self._normalize_strategy_label(existing_row.get("strategy_entry_signal"))
            if existing_row
            else pending_strategy_entry_signal
        )
        mode = (
            str(existing_row.get("mode") or self._bot.config.mode.value)
            if existing_row
            else (matched_pending.mode if matched_pending is not None else self._bot.config.mode.value)
        )
        return (
            thread_name,
            strategy,
            strategy_entry or strategy,
            strategy_entry_component,
            strategy_entry_signal,
            mode,
        )

    @staticmethod
    def _multi_strategy_enabled_for_params(strategy_params: dict[str, object]) -> bool:
        raw = strategy_params.get("multi_strategy_enabled", True)
        if isinstance(raw, str):
            return raw.strip().lower() not in {"0", "false", "no", "off"}
        return bool(raw)

    @staticmethod
    def _parse_strategy_names(raw: object) -> list[str]:
        if isinstance(raw, str):
            items = raw.split(",")
        elif isinstance(raw, (list, tuple, set)):
            items = raw
        else:
            return []

        names: list[str] = []
        seen: set[str] = set()
        for item in items:
            name = str(item or "").strip().lower()
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)
        return names

    def _resolve_multi_strategy_component_names(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
    ) -> list[str]:
        base_name = str(strategy_name).strip().lower()
        if not base_name:
            return []
        if not self._multi_strategy_enabled_for_params(strategy_params):
            return [base_name]

        names = self._parse_strategy_names(strategy_params.get("multi_strategy_names"))
        if not names:
            names = self._parse_strategy_names(strategy_params.get("multi_strategy_default_names"))
        if not names and "multi_strategy_enabled" in strategy_params:
            names = list(DEFAULT_MULTI_STRATEGY_COMPONENT_NAMES)
        if not names:
            return [base_name]
        if base_name not in names:
            names.insert(0, base_name)
        return names

    def _strategy_supports_symbol(
        self,
        strategy_name: str,
        strategy_params: dict[str, object],
        symbol: str,
    ) -> bool:
        support_cache_key = (
            *self._strategy_cache_identity(strategy_name, strategy_params),
            str(symbol).strip().upper(),
        )
        supported = self._strategy_support_cache.get(support_cache_key)
        if supported is not None:
            return bool(supported)

        try:
            normalized_strategy = str(strategy_name).strip().lower()
            if normalized_strategy == _MULTI_STRATEGY_CARRIER_NAME:
                base_strategy_name = str(
                    strategy_params.get(_MULTI_STRATEGY_BASE_COMPONENT_PARAM) or self._bot.config.strategy
                ).strip().lower()
                component_names = self._resolve_multi_strategy_component_names(
                    base_strategy_name,
                    strategy_params,
                )
                supported = False
                for component_name in component_names:
                    normalized_component = str(component_name).strip().lower()
                    if not normalized_component or normalized_component == _MULTI_STRATEGY_CARRIER_NAME:
                        continue
                    component_params = (
                        dict(strategy_params)
                        if normalized_component == base_strategy_name
                        else self._strategy_params_for(normalized_component)
                    )
                    if self._strategy_supports_symbol(
                        normalized_component,
                        component_params,
                        symbol,
                    ):
                        supported = True
                        break
            else:
                # Use the core module's binding so monkeypatches in tests are visible.
                import xtb_bot.bot.core as _core_mod
                strategy_filter = _core_mod.create_strategy(strategy_name, strategy_params)
                supported = bool(strategy_filter.supports_symbol(symbol))
        except Exception:
            logger.exception(
                "Failed to check strategy symbol support | strategy=%s symbol=%s",
                strategy_name,
                symbol,
            )
            supported = False
        self._strategy_support_cache[support_cache_key] = supported
        return bool(supported)

    def _schedule_disabled_by_multi_strategy(self) -> bool:
        params = self._strategy_params_for(self._bot.config.strategy)
        return self._multi_strategy_enabled_for_params(params)

    def _resolve_multi_strategy_rollout_mode_overrides(self) -> dict[str, RunMode]:
        if self._bot.config.mode != RunMode.EXECUTION:
            return {}
        params = self._strategy_params_for(self._bot.config.strategy)
        if not self._multi_strategy_enabled_for_params(params):
            return {}

        stage = str(params.get("multi_strategy_rollout_stage") or "full").strip().lower()
        if stage in {"", "off", "none", "disabled", "full", "execution"}:
            return {}

        symbols: list[str] = []
        seen: set[str] = set()
        for raw_symbol in self._bot.config.symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
        if not symbols:
            return {}

        if stage in {"shadow", "signal_only", "signal-only", "dry_run", "dry-run"}:
            logger.warning(
                "Multi-strategy rollout stage=shadow: all configured symbols run in signal_only mode | symbols=%d",
                len(symbols),
            )
            self._bot.store.record_event(
                "WARN",
                None,
                "Multi-strategy rollout stage enabled",
                {
                    "stage": "shadow",
                    "execution_symbols": 0,
                    "signal_only_symbols": len(symbols),
                },
            )
            return {symbol: RunMode.SIGNAL_ONLY for symbol in symbols}

        if stage in {"canary", "pilot"}:
            ratio = self._bot._as_float_param(params.get("multi_strategy_rollout_canary_ratio"), 0.30)
            ratio = max(0.0, min(1.0, ratio))
            explicit_canary = {
                symbol
                for symbol in self._bot._as_symbol_list_param(params.get("multi_strategy_rollout_canary_symbols"))
                if symbol in seen
            }

            canary_symbols: set[str] = set()
            source = "explicit"
            if explicit_canary:
                canary_symbols = explicit_canary
            else:
                source = "ratio"
                if ratio > 0:
                    count = max(1, int(math.ceil(len(symbols) * ratio)))
                    seed = str(
                        params.get("multi_strategy_rollout_seed")
                        or self._bot.bot_magic_instance
                        or "multi_strategy_rollout"
                    )
                    ranked = sorted(
                        symbols,
                        key=lambda item: hashlib.sha1(f"{seed}:{item}".encode("utf-8")).hexdigest(),
                    )
                    canary_symbols = set(ranked[:count])

            overrides = {
                symbol: (RunMode.EXECUTION if symbol in canary_symbols else RunMode.SIGNAL_ONLY)
                for symbol in symbols
            }
            logger.warning(
                (
                    "Multi-strategy rollout stage=canary | execution_symbols=%d signal_only_symbols=%d "
                    "source=%s ratio=%.3f"
                ),
                len(canary_symbols),
                len(symbols) - len(canary_symbols),
                source,
                ratio,
            )
            self._bot.store.record_event(
                "WARN",
                None,
                "Multi-strategy rollout stage enabled",
                {
                    "stage": "canary",
                    "execution_symbols": sorted(canary_symbols),
                    "signal_only_symbols_count": len(symbols) - len(canary_symbols),
                    "selection_source": source,
                    "canary_ratio": ratio,
                },
            )
            return overrides

        logger.warning(
            "Ignoring unknown multi-strategy rollout stage=%s (expected: full|canary|shadow)",
            stage,
        )
        return {}

    def _mode_override_for_symbol(self, symbol: str) -> RunMode | None:
        return self._multi_strategy_rollout_mode_override_by_symbol.get(str(symbol).strip().upper())
