from __future__ import annotations

from dataclasses import dataclass

from xtb_bot.models import RunMode


def _freeze_cache_value(value: object) -> object:
    if isinstance(value, dict):
        return tuple(
            (str(key), _freeze_cache_value(item))
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_cache_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        frozen_items = [_freeze_cache_value(item) for item in value]
        return tuple(sorted(frozen_items, key=repr))
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _strategy_params_signature(strategy_params: dict[str, object]) -> tuple[tuple[str, object], ...]:
    return tuple(
        (str(key), _freeze_cache_value(value))
        for key, value in sorted(strategy_params.items(), key=lambda entry: str(entry[0]))
    )


@dataclass(frozen=True)
class WorkerAssignment:
    symbol: str
    strategy_name: str
    strategy_params: dict[str, object]
    mode_override: RunMode | None = None
    source: str = "static"
    label: str | None = None

    def signature(self) -> tuple[str, str, str, str, tuple[tuple[str, object], ...]]:
        return (
            self.symbol,
            self.strategy_name,
            (self.mode_override.value if isinstance(self.mode_override, RunMode) else ""),
            self.source,
            _strategy_params_signature(self.strategy_params),
        )

    def runtime_signature(self) -> tuple[str, str, str, tuple[tuple[str, object], ...]]:
        return (
            self.symbol,
            self.strategy_name,
            (self.mode_override.value if isinstance(self.mode_override, RunMode) else ""),
            _strategy_params_signature(self.strategy_params),
        )
