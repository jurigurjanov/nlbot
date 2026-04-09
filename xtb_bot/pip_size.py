from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import math
from typing import Any

from xtb_bot.symbols import is_australia_index_symbol, is_index_symbol, normalize_symbol

_FX_CODES = {
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "AUD",
    "CAD",
    "NZD",
}


def symbol_pip_size_fallback(
    symbol: str,
    *,
    index_pip_size: float,
    energy_pip_size: float,
) -> float:
    upper = normalize_symbol(symbol)
    if is_australia_index_symbol(upper):
        return 1.0
    if is_index_symbol(upper):
        return max(float(index_pip_size), FLOAT_COMPARISON_TOLERANCE)
    if upper.endswith("JPY"):
        return 0.01
    if upper.startswith("XAU") or upper.startswith("XAG"):
        return 0.1
    if upper in {"GOLD", "SILVER"}:
        return 0.1
    if upper in {"WTI", "BRENT"}:
        return max(float(energy_pip_size), FLOAT_COMPARISON_TOLERANCE)
    return 0.0001


def is_fx_symbol(symbol: str) -> bool:
    upper = normalize_symbol(symbol)
    if len(upper) != 6 or not upper.isalpha():
        return False
    return upper[:3] in _FX_CODES and upper[3:] in _FX_CODES


def normalize_tick_size_to_pip_size(
    symbol: str,
    raw_tick_size: float | None,
    *,
    index_pip_size: float,
    energy_pip_size: float,
) -> float | None:
    if raw_tick_size is None:
        return None
    try:
        value = float(raw_tick_size)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    # FX brokers often expose quote-step tick size (e.g. 0.00001), while
    # strategy/risk pips are 0.0001 (or 0.01 for JPY). Promote only FX sizes.
    if is_fx_symbol(symbol):
        fx_pip = symbol_pip_size_fallback(
            symbol,
            index_pip_size=index_pip_size,
            energy_pip_size=energy_pip_size,
        )
        if value + FLOAT_ROUNDING_TOLERANCE < fx_pip:
            return float(fx_pip)
    return value


def parse_symbol_pip_size_map(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    parsed: dict[str, float] = {}
    for key, value in raw.items():
        symbol = normalize_symbol(str(key))
        if not symbol:
            continue
        try:
            pip_size = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(pip_size) or pip_size <= 0:
            continue
        parsed[symbol] = pip_size
    return parsed
