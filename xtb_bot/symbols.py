from __future__ import annotations

from collections.abc import Mapping

# Shared symbol configuration used across strategies and risk helpers.
# Keep symbol/group hardcodes in one place only.
INDEX_SYMBOLS_BY_MARKET: dict[str, frozenset[str]] = {
    "america": frozenset({"US100", "US500", "US30", "US2000"}),
    "europe": frozenset(
        {
            "DE40",
            "DE30",
            "UK100",
            "NK20",
            "FRA40",
            "FR40",
            "EU50",
            "IT40",
            "ES35",
            "SK20",
            "SE30",
            "DEMID50",
            "MDAX50",
        }
    ),
    "japan": frozenset({"JP225", "JPN225", "TOPIX"}),
    "australia": frozenset({"AUS200", "AU200"}),
}

INDEX_MARKET_BY_SYMBOL: dict[str, str] = {
    symbol: market
    for market, symbols in INDEX_SYMBOLS_BY_MARKET.items()
    for symbol in symbols
}

# Prefix fallback supports broker aliases not explicitly listed above.
# Prefix-based matching only applies when symbol contains digits.
INDEX_PREFIXES_BY_MARKET: dict[str, tuple[str, ...]] = {
    "america": ("US",),
    "europe": ("DE", "UK", "FRA", "FR", "EU", "IT", "ES", "SE", "SK", "NK"),
    "japan": ("JP", "JPN"),
    "australia": ("AU", "AUS"),
}

COMMODITY_SYMBOLS: frozenset[str] = frozenset(
    {
        "CARBON",
        "COCOA",
        "CORN",
        "GASOLINE",
        "GOLD",
        "OATS",
        "SOYBEANOIL",
        "SILVER",
        "UKNATGAS",
        "WTI",
        "BRENT",
        "WHEAT",
        "XAUUSD",
        "XAGUSD",
        "USOIL",
        "UKOIL",
        "NATGAS",
        "NGAS",
    }
)


def normalize_symbol(symbol: str | None) -> str:
    return str(symbol or "").strip().upper()


def has_digits(value: str) -> bool:
    return any(ch.isdigit() for ch in value)


def _symbol_in_market_symbols(symbol: str, symbols_by_market: Mapping[str, str]) -> str | None:
    return symbols_by_market.get(symbol)


def _symbol_in_market_prefixes(symbol: str, prefixes_by_market: Mapping[str, tuple[str, ...]]) -> str | None:
    if not has_digits(symbol):
        return None
    for market, prefixes in prefixes_by_market.items():
        if symbol.startswith(prefixes):
            return market
    return None


def resolve_index_market(symbol: str) -> str | None:
    upper = normalize_symbol(symbol)
    if not upper:
        return None
    by_symbol = _symbol_in_market_symbols(upper, INDEX_MARKET_BY_SYMBOL)
    if by_symbol is not None:
        return by_symbol
    return _symbol_in_market_prefixes(upper, INDEX_PREFIXES_BY_MARKET)


def is_index_symbol(symbol: str) -> bool:
    return resolve_index_market(symbol) is not None


def is_australia_index_symbol(symbol: str) -> bool:
    return resolve_index_market(symbol) == "australia"


def is_commodity_symbol(symbol: str) -> bool:
    upper = normalize_symbol(symbol)
    if not upper:
        return False
    return upper in COMMODITY_SYMBOLS or upper.startswith(("XAU", "XAG"))
