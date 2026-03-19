"""Centralised pip-size lookup used by every strategy.

Keep one authoritative table so that all strategies agree on pip sizes
for every symbol.  Individual strategies should call ``pip_size(symbol)``
instead of maintaining their own ``_pip_size`` static methods.
"""

from __future__ import annotations

# Explicit per-symbol overrides (highest priority).
_SYMBOL_PIP_SIZE: dict[str, float] = {
    # Australian indices
    "AUS200": 1.0,
    "AU200": 1.0,
    # Major world indices
    "US100": 0.1,
    "US500": 0.1,
    "US30": 0.1,
    "DE40": 0.1,
    "UK100": 0.1,
    "FRA40": 0.1,
    "JP225": 0.1,
    "EU50": 0.1,
    # Commodities
    "WTI": 0.1,
    "BRENT": 0.1,
}

# Crypto symbols – used by _is_crypto_symbol as well.
CRYPTO_SYMBOLS: frozenset[str] = frozenset({
    "BTC", "ETH", "LTC", "BCH", "DOGE", "XRP", "SOL",
    "ADA", "DOT", "AVAX", "LINK", "MATIC", "SHIB",
    "UNI", "ATOM", "XLM", "ALGO", "NEAR", "FTM",
    "MANA", "SAND", "APE", "CRO", "FIL", "ICP",
    "EOS", "XTZ", "AAVE", "MKR", "COMP", "SNX",
    "SUSHI", "YFI", "1INCH", "ENJ", "BAT", "ZRX",
})

# Index prefix patterns – used by _is_index_symbol as well.
_INDEX_PREFIXES: tuple[str, ...] = ("US", "DE", "UK", "FRA", "JP", "EU", "AUS", "AU")


def pip_size(symbol: str) -> float:
    """Return the pip size for *symbol*."""
    upper = symbol.upper()

    # 1) Exact match.
    if upper in _SYMBOL_PIP_SIZE:
        return _SYMBOL_PIP_SIZE[upper]

    # 2) Generic index detection (prefix + contains digit).
    if upper.startswith(_INDEX_PREFIXES) and any(ch.isdigit() for ch in upper):
        return 0.1

    # 3) JPY pairs.
    if upper.endswith("JPY"):
        return 0.01

    # 4) Precious metals.
    if upper.startswith(("XAU", "XAG")):
        return 0.1

    # 5) Crypto (IG typically uses 1.0 point).
    if upper in CRYPTO_SYMBOLS:
        return 1.0

    # 6) Default (standard FX).
    return 0.0001


def is_crypto_symbol(symbol: str) -> bool:
    """Return *True* if *symbol* is a recognised crypto ticker."""
    return symbol.upper() in CRYPTO_SYMBOLS


def is_index_symbol(symbol: str) -> bool:
    """Return *True* if *symbol* looks like a stock-index CFD."""
    upper = symbol.upper()
    if upper in _SYMBOL_PIP_SIZE:
        # It's an index only if its pip size is in [0.1, 1.0] and it isn't a
        # commodity.
        return upper not in {"WTI", "BRENT"} and _SYMBOL_PIP_SIZE[upper] >= 0.1
    return upper.startswith(_INDEX_PREFIXES) and any(ch.isdigit() for ch in upper)
