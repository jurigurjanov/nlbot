from __future__ import annotations

from xtb_bot.config import resolve_strategy_param
from xtb_bot.symbols import is_index_symbol as _shared_is_index_symbol


class WorkerStrategyParamRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def strategy_param_key(self, suffix: str) -> str:
        namespace = self.strategy_param_namespace_label()
        return f"{namespace}_{suffix}"

    def strategy_param_namespace_label(self) -> str:
        worker = self.worker
        if worker._multi_strategy_carrier_enabled and worker._multi_strategy_base_component_name:
            return worker._multi_strategy_base_component_name
        return worker._normalize_strategy_label(worker.strategy_name)

    def base_strategy_label(self) -> str:
        return self.strategy_param_namespace_label()

    def multi_strategy_component_runtime_name(self, strategy_name: str) -> str:
        worker = self.worker
        normalized = worker._normalize_strategy_label(strategy_name)
        if normalized != "momentum":
            return normalized
        upper = worker.symbol.upper()
        if _shared_is_index_symbol(upper):
            return "momentum_index"
        if upper.startswith(("XAU", "XAG")) or upper in {
            "WTI",
            "BRENT",
            "XAUUSD",
            "XAGUSD",
            "GOLD",
            "SILVER",
            "NGAS",
            "NATGAS",
            "COPPER",
        }:
            return normalized
        if upper.startswith(("BTC", "ETH", "LTC", "BCH", "DOG", "XRP", "SOL")) or upper.endswith(
            ("BTC", "ETH", "LTC", "BCH", "DOG", "XRP", "SOL")
        ):
            return normalized
        if len(upper) == 6 and upper.isalpha():
            return "momentum_fx"
        return normalized

    def strategy_bool_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
        default: bool,
    ) -> bool:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_param_namespace_label(),
            suffix,
            default,
            mode=self.worker.mode,
        )
        if isinstance(raw, str):
            return raw.strip().lower() not in {"0", "false", "no", "off"}
        return bool(raw)

    def strategy_float_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
        default: float,
    ) -> float:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_param_namespace_label(),
            suffix,
            default,
            mode=self.worker.mode,
        )
        try:
            return float(raw)
        except (TypeError, ValueError):
            return float(default)

    def strategy_float_param_for_label(
        self,
        strategy_label: str | None,
        suffix: str,
        default: float,
    ) -> float:
        worker = self.worker
        requested = worker._normalize_strategy_label(strategy_label)
        fallbacks: list[str] = []
        if requested:
            fallbacks.append(requested)
        base_label = self.base_strategy_label()
        if base_label and base_label not in fallbacks:
            fallbacks.append(base_label)
        for label in fallbacks:
            strategy_params = worker._strategy_params_map.get(label)
            if not isinstance(strategy_params, dict):
                continue
            raw = resolve_strategy_param(
                strategy_params,
                label,
                suffix,
                default,
                mode=worker.mode,
            )
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return float(default)

    def strategy_symbol_float_map_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
    ) -> dict[str, float]:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_param_namespace_label(),
            suffix,
            {},
            mode=self.worker.mode,
        )
        return self.worker._parse_symbol_float_map(raw)

    @staticmethod
    def symbol_override_float(
        symbol: str,
        *,
        default: float,
        mapping: dict[str, float],
        floor: float | None = None,
        ceil: float | None = None,
    ) -> float:
        resolved = float(mapping.get(symbol.upper(), default))
        if floor is not None:
            resolved = max(float(floor), resolved)
        if ceil is not None:
            resolved = min(float(ceil), resolved)
        return resolved

    def strategy_choice_param(
        self,
        strategy_params: dict[str, object],
        suffix: str,
        allowed: tuple[str, ...],
        default: str,
    ) -> str:
        raw = resolve_strategy_param(
            strategy_params,
            self.strategy_param_namespace_label(),
            suffix,
            default,
            mode=self.worker.mode,
        )
        value = str(raw or "").strip().lower()
        if value in allowed:
            return value
        return default
