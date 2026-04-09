from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import copy
from collections import OrderedDict, deque
from contextlib import contextmanager
from queue import Empty, Full, Queue
import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib import error, parse, request

try:
    from lightstreamer.client import (
        ClientListener as _LightstreamerClientListenerBase,
        LightstreamerClient as _LightstreamerClient,
        Subscription as _LightstreamerSubscription,
        SubscriptionListener as _LightstreamerSubscriptionListenerBase,
    )
except Exception:  # pragma: no cover - optional dependency for legacy compatibility
    _LightstreamerClient = None
    _LightstreamerSubscription = None
    _LightstreamerClientListenerBase = object  # type: ignore[assignment]
    _LightstreamerSubscriptionListenerBase = object  # type: ignore[assignment]

from xtb_bot.client import BaseBrokerClient, BrokerError
from xtb_bot.models import (
    AccountType,
    AccountSnapshot,
    ConnectivityStatus,
    NewsEvent,
    PendingOpen,
    Position,
    PriceTick,
    Side,
    StreamHealthStatus,
    SymbolSpec,
)
from xtb_bot.symbols import is_index_symbol as _shared_is_index_symbol


logger = logging.getLogger(__name__)

_RETRYABLE_HTTP_STATUS_CODES = frozenset({500, 502, 503, 504})
_FLOAT_TEXT_PATTERN = re.compile(r"^[0-9,.\-+eE]+$")


@dataclass(slots=True)
class _QueuedRequestJob:
    method: str
    path: str
    payload: dict[str, Any] | None
    version: str
    auth: bool
    extra_headers: dict[str, str] | None
    query: dict[str, Any] | None
    retry_on_auth_failure: bool
    critical_bypass: bool = False
    done: threading.Event = field(default_factory=threading.Event)
    result: tuple[dict[str, Any], dict[str, str]] | None = None
    error: Exception | None = None


DEFAULT_IG_EPICS: dict[str, str] = {
    "EURUSD": "CS.D.EURUSD.CFD.IP",
    "GBPUSD": "CS.D.GBPUSD.CFD.IP",
    "USDJPY": "CS.D.USDJPY.CFD.IP",
    "GBPJPY": "CS.D.GBPJPY.CFD.IP",
    "USDCAD": "CS.D.USDCAD.CFD.IP",
    "EURGBP": "CS.D.EURGBP.CFD.IP",
    "EURCHF": "CS.D.EURCHF.CFD.IP",
    "AUDUSD": "CS.D.AUDUSD.CFD.IP",
    "USDCHF": "CS.D.USDCHF.CFD.IP",
    "US100": "IX.D.NASDAQ.CASH.IP",
    "US500": "IX.D.SPTRD.CASH.IP",
    "US30": "IX.D.DOW.CASH.IP",
    "DE40": "IX.D.DAX.CASH.IP",
    "FR40": "IX.D.CAC.CASH.IP",
    "FRA40": "IX.D.CAC.CASH.IP",
    "EU50": "IX.D.EUSTX50.CASH.IP",
    "UK100": "IX.D.FTSE.CASH.IP",
    "AUS200": "IX.D.ASX.CASH.IP",
    "JP225": "IX.D.NIKKEI.CASH.IP",
    "JPN225": "IX.D.NIKKEI.CASH.IP",
    "XAUUSD": "CS.D.GOLD.CFD.IP",
    "GOLD": "CS.D.GOLD.CFD.IP",
    "WTI": "CC.D.CL.UNC.IP",
    "BRENT": "CC.D.LCO.USS.IP",
    "BTC": "CS.D.BITCOIN.CFD.IP",
    "ETH": "CS.D.ETHUSD.CFD.IP",
    "LTC": "CS.D.LTCUSD.CFD.IP",
    "SOL": "CS.D.SOLUSD.CFD.IP",
    "XRP": "CS.D.XRPUSD.CFD.IP",
    "DOGE": "CS.D.DOGUSD.CFD.IP",
    "AAPL": "UA.D.AAPL.CASH.IP",
    "MSFT": "UA.D.MSFT.CASH.IP",
}
DEFAULT_IG_EPIC_CANDIDATES: dict[str, list[str]] = {
    "EURUSD": ["CS.D.EURUSD.CFD.IP"],
    "GBPUSD": ["CS.D.GBPUSD.CFD.IP"],
    "USDJPY": ["CS.D.USDJPY.CFD.IP"],
    "GBPJPY": ["CS.D.GBPJPY.CFD.IP"],
    "USDCAD": ["CS.D.USDCAD.CFD.IP"],
    "EURGBP": ["CS.D.EURGBP.CFD.IP"],
    "EURCHF": ["CS.D.EURCHF.CFD.IP"],
    "AUDUSD": ["CS.D.AUDUSD.CFD.IP"],
    "USDCHF": ["CS.D.USDCHF.CFD.IP"],
    "US100": [
        "IX.D.NASDAQ.CASH.IP",
        "IX.D.NASDAQ.DAILY.IP",
        "IX.D.NASDAQ.IFS.IP",
        "IX.D.NASDAQ.CFD.IP",
    ],
    "US500": [
        "IX.D.SPTRD.CASH.IP",
        "IX.D.SPTRD.DAILY.IP",
        "IX.D.SPTRD.IFS.IP",
        "IX.D.SPTRD.CFD.IP",
    ],
    "US30": [
        "IX.D.DOW.CASH.IP",
        "IX.D.DOW.DAILY.IP",
        "IX.D.DOW.IFS.IP",
        "IX.D.DOW.CFD.IP",
    ],
    "DE40": [
        "IX.D.DAX.CASH.IP",
        "IX.D.DAX.DAILY.IP",
        "IX.D.DAX.IFS.IP",
        "IX.D.DAX.CFD.IP",
    ],
    "FR40": [
        "IX.D.CAC.CASH.IP",
        "IX.D.CAC.DAILY.IP",
        "IX.D.CAC.IFS.IP",
        "IX.D.CAC40.CASH.IP",
    ],
    "FRA40": [
        "IX.D.CAC.CASH.IP",
        "IX.D.CAC.DAILY.IP",
        "IX.D.CAC.IFS.IP",
        "IX.D.CAC40.CASH.IP",
    ],
    "EU50": [
        "IX.D.EUSTX50.CASH.IP",
        "IX.D.EUSTX50.DAILY.IP",
        "IX.D.EUSTX50.IFS.IP",
        "IX.D.STX50.CASH.IP",
    ],
    "UK100": [
        "IX.D.FTSE.CASH.IP",
        "IX.D.FTSE.DAILY.IP",
        "IX.D.FTSE.IFS.IP",
        "IX.D.FTSE.CFD.IP",
    ],
    "AUS200": [
        "IX.D.ASX.CASH.IP",
        "IX.D.ASX.DAILY.IP",
        "IX.D.ASX.IFS.IP",
    ],
    "JP225": [
        "IX.D.NIKKEI.CASH.IP",
        "IX.D.NIKKEI.DAILY.IP",
        "IX.D.NIKKEI.IFS.IP",
        "IX.D.JP225.CASH.IP",
        "IX.D.JPN225.CASH.IP",
    ],
    "JPN225": [
        "IX.D.NIKKEI.CASH.IP",
        "IX.D.NIKKEI.DAILY.IP",
        "IX.D.NIKKEI.IFS.IP",
        "IX.D.JP225.CASH.IP",
        "IX.D.JPN225.CASH.IP",
    ],
    "XAUUSD": [
        "CS.D.GOLD.CFD.IP",
        "CS.D.XAUUSD.CFD.IP",
        "CS.D.GOLD.CASH.IP",
        "CS.D.GOLD.TODAY.IP",
        "CS.D.XAUUSD.CASH.IP",
        "CS.D.XAUUSD.TODAY.IP",
    ],
    "GOLD": [
        "CS.D.GOLD.CFD.IP",
        "CS.D.XAUUSD.CFD.IP",
        "CS.D.GOLD.CASH.IP",
        "CS.D.GOLD.TODAY.IP",
        "CS.D.XAUUSD.CASH.IP",
        "CS.D.XAUUSD.TODAY.IP",
    ],
    "WTI": [
        "CC.D.CL.UNC.IP",
        "CC.D.CL.USS.IP",
        "CC.D.WTI.USS.IP",
        "CC.D.USCRUDE.CFD.IP",
    ],
    "BRENT": [
        "CC.D.LCO.USS.IP",
        "CC.D.LCO.CFD.IP",
        "CC.D.LCO.UNC.IP",
        "CC.D.BRENT.CFD.IP",
    ],
    "BTC": ["CS.D.BITCOIN.CFD.IP", "CS.D.BITCOIN.TODAY.IP"],
    "ETH": ["CS.D.ETHUSD.CFD.IP", "CS.D.ETHUSD.TODAY.IP", "CS.D.ETH.CFD.IP"],
    "LTC": ["CS.D.LTCUSD.CFD.IP", "CS.D.LTCUSD.TODAY.IP", "CS.D.LTC.CFD.IP"],
    "SOL": ["CS.D.SOLUSD.CFD.IP", "CS.D.SOLUSD.TODAY.IP", "CS.D.SOL.CFD.IP"],
    "XRP": ["CS.D.XRPUSD.CFD.IP", "CS.D.XRPUSD.TODAY.IP", "CS.D.XRP.CFD.IP"],
    "DOGE": [
        "CS.D.DOGUSD.CFD.IP",
        "CS.D.DOGUSD.TODAY.IP",
        "CS.D.DOGEUSD.CFD.IP",
        "CS.D.DOGEUSD.TODAY.IP",
    ],
    "AAPL": [
        "UA.D.AAPL.CASH.IP",
        "UA.D.AAPL.DAILY.IP",
        "SA.D.AAPL.CASH.IP",
    ],
    "MSFT": [
        "UA.D.MSFT.CASH.IP",
        "UA.D.MSFT.DAILY.IP",
        "SA.D.MSFT.CASH.IP",
    ],
}
IG_EPIC_SEARCH_TERMS: dict[str, list[str]] = {
    "US500": ["S&P 500", "US 500", "SPX"],
    "US100": ["NASDAQ 100", "US 100", "NASDAQ", "US TECH 100", "USTECH 100", "US TECH"],
    "US2000": ["RUSSELL 2000", "US RUSSELL 2000", "US 2000", "RUSSELL", "RUT", "RUS 2000"],
    "DE40": ["DAX 40", "GERMANY 40", "DAX"],
    "UK100": ["FTSE 100", "UK 100", "FTSE"],
    "FR40": ["CAC 40", "FRANCE 40", "FR40"],
    "EU50": ["EURO STOXX 50", "EU50", "STOXX 50"],
    "IT40": ["ITALY 40", "FTSE MIB", "MIB 40", "ITALY40"],
    "ES35": ["SPAIN 35", "IBEX 35", "SPAIN35"],
    "SK20": ["SWEDEN 30", "OMX STOCKHOLM 30", "OMXS30", "SK20"],
    "NK20": ["NORWAY 25", "NORWAY 25 CASH", "OBX", "OSLO BORS BENCHMARK", "NK20"],
    "SE30": ["SWEDEN 30", "OMX STOCKHOLM 30", "OMXS30", "SE30"],
    "DEMID50": ["GERMANY MID-CAP 50", "GERMANY MID CAP 50", "MDAX", "DEMID50", "MIDCAP 50"],
    "MDAX50": ["GERMANY MID-CAP 50", "GERMANY MID CAP 50", "MDAX", "MDAX50", "MIDCAP 50"],
    "AUS200": ["AUSTRALIA 200", "AUS200", "ASX 200"],
    "JP225": ["JAPAN 225", "NIKKEI 225", "NIKKEI", "JP225", "JPN225"],
    "JPN225": ["JAPAN 225", "NIKKEI 225", "NIKKEI", "JP225", "JPN225"],
    "TOPIX": ["TOPIX", "TOKYO FIRST SECTION", "TOKYO STOCK PRICE INDEX"],
    "CARBON": ["CARBON EMISSIONS", "CARBON", "EU CARBON", "EUA"],
    "UKNATGAS": ["NATURAL GAS - UK", "UK NATURAL GAS", "NATURAL GAS UK", "UK GAS"],
    "GASOLINE": ["GASOLINE", "RBOB GASOLINE", "RB GASOLINE"],
    "WHEAT": ["WHEAT - CHICAGO", "CHICAGO WHEAT", "WHEAT"],
    "COCOA": ["COCOA - NEW YORK", "NEW YORK COCOA", "COCOA"],
    "CORN": ["CORN", "CHICAGO CORN"],
    "SOYBEANOIL": ["SOYBEAN OIL", "SOY OIL", "BEAN OIL"],
    "OATS": ["OATS", "CHICAGO OATS"],
    "GOLD": ["GOLD", "XAUUSD", "SPOT GOLD"],
    "XAUUSD": ["XAUUSD", "GOLD", "SPOT GOLD"],
    "BRENT": ["BRENT", "UK CRUDE"],
    "WTI": ["WTI", "US CRUDE", "CRUDE OIL", "LIGHT SWEET CRUDE", "CL"],
    "BTC": ["BITCOIN", "BTC", "BTCUSD"],
    "ETH": ["ETHEREUM", "ETH", "ETHUSD"],
    "LTC": ["LITECOIN", "LTC", "LTCUSD"],
    "SOL": ["SOLANA", "SOL", "SOLUSD"],
    "XRP": ["RIPPLE", "XRP", "XRPUSD"],
    "DOGE": ["DOGECOIN", "DOGE", "DOGUSD", "DOGEUSD"],
    "AAPL": ["APPLE", "AAPL"],
    "MSFT": ["MICROSOFT", "MSFT"],
}

COMMODITY_EPIC_HINTS: dict[str, tuple[str, ...]] = {
    "CARBON": ("CARBON", "EUA", "EMISS"),
    "UKNATGAS": ("NGASUK", "NATGASUK", "UKGAS", "NGUK"),
    "GASOLINE": ("GASOLINE", "RBOB", ".RB."),
    "WHEAT": ("WHEAT", ".ZW."),
    "COCOA": ("COCOA", "COCO"),
    "CORN": ("CORN", ".ZC."),
    "SOYBEANOIL": ("SOY", "BEAN", ".ZL."),
    "OATS": ("OATS", "OAT", ".ZO."),
}

_FX_MARGIN_CURRENCIES = {
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "CNH",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "JPY",
    "MXN",
    "NOK",
    "NZD",
    "PLN",
    "SEK",
    "SGD",
    "TRY",
    "USD",
    "ZAR",
}

_CRYPTO_SYMBOL_BASES = frozenset(
    {
        "ADA",
        "ATOM",
        "AVAX",
        "BCH",
        "BTC",
        "DOGE",
        "DOT",
        "ETC",
        "ETH",
        "LINK",
        "LTC",
        "MATIC",
        "SOL",
        "UNI",
        "XLM",
        "XRP",
    }
)
_CRYPTO_SYMBOL_QUOTES = ("USD", "USDT", "USDC", "EUR", "GBP", "JPY", "BTC", "ETH")


LIGHTSTREAMER_CID = "mgQkwtwdysogQz2BJ4Ji kOj2Bg"
LIGHTSTREAMER_ADAPTER_SET = "DEFAULT"
LIGHTSTREAMER_PRICE_ADAPTER = "Pricing"
# IG PRICE subscriptions use a space-separated field list.
# Keep the requested schema compact and strictly aligned with IG docs.
LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS = (
    "BIDPRICE1",
    "ASKPRICE1",
    "TIMESTAMP",
)
# Internal decode aliases; includes legacy fallback names seen in tests/older feeds.
LIGHTSTREAMER_PRICE_FIELDS = (
    "BIDPRICE1",
    "ASKPRICE1",
    "TIMESTAMP",
    "BID",
    "OFFER",
    "UPDATE_TIME",
    "OFR",
    "UTM",
)


class _IgLightstreamerClientListener(_LightstreamerClientListenerBase):
    def __init__(self, owner: "IgApiClient") -> None:
        self._owner = owner

    def onStatusChange(self, status: str) -> None:  # noqa: N802
        try:
            logger.info("IG Lightstreamer SDK status change: %s", status)
            self._owner._on_stream_sdk_status_change(status)
        except Exception:
            logger.warning("IG Lightstreamer SDK status callback failed", exc_info=True)

    def onServerError(self, code: int, message: str) -> None:  # noqa: N802
        try:
            logger.warning("IG Lightstreamer SDK server error: code=%s message=%s", code, message)
            self._owner._on_stream_sdk_server_error(code, message)
        except Exception:
            logger.warning("IG Lightstreamer SDK server error callback failed", exc_info=True)


class _IgLightstreamerSubscriptionListener(_LightstreamerSubscriptionListenerBase):
    def __init__(self, owner: "IgApiClient", symbol: str) -> None:
        self._owner = owner
        self._symbol = str(symbol).upper()

    def onSubscription(self) -> None:  # noqa: N802
        try:
            logger.info("IG Lightstreamer SDK subscription active: %s", self._symbol)
            self._owner._on_stream_sdk_subscription(self._symbol)
        except Exception:
            logger.warning("IG Lightstreamer SDK subscription callback failed: %s", self._symbol, exc_info=True)

    def onUnsubscription(self) -> None:  # noqa: N802
        try:
            logger.info("IG Lightstreamer SDK unsubscription: %s", self._symbol)
            self._owner._on_stream_sdk_unsubscription(self._symbol)
        except Exception:
            logger.warning("IG Lightstreamer SDK unsubscription callback failed: %s", self._symbol, exc_info=True)

    def onSubscriptionError(self, code: int, message: str) -> None:  # noqa: N802
        try:
            logger.warning("IG Lightstreamer SDK subscription error: %s code=%s message=%s", self._symbol, code, message)
            self._owner._on_stream_sdk_subscription_error(self._symbol, code, message)
        except Exception:
            logger.warning("IG Lightstreamer SDK subscription-error callback failed: %s", self._symbol, exc_info=True)

    def onItemUpdate(self, update: Any) -> None:  # noqa: N802
        try:
            self._owner._on_stream_sdk_item_update(self._symbol, update)
        except Exception:
            logger.debug("IG Lightstreamer SDK item-update callback failed", exc_info=True)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        if isinstance(value, str):
            text = value.strip().replace("\xa0", "").replace(" ", "")
            if not text or text in {"-", "--", "N/A", "null", "None"}:
                return default
            if not _FLOAT_TEXT_PATTERN.fullmatch(text):
                return default
            if "," in text and "." in text:
                if text.rfind(",") > text.rfind("."):
                    text = text.replace(".", "").replace(",", ".")
                else:
                    text = text.replace(",", "")
            elif "," in text:
                text = text.replace(",", ".")
            if text in {"", "-", "+", ".", "-.", "+."}:
                return default
            try:
                return float(text)
            except Exception:
                return default
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "y"}:
        return True
    if normalized in {"0", "false", "no", "off", "n"}:
        return False
    return default


def _as_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_datetime_to_unix_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        if not math.isfinite(parsed) or parsed <= 0:
            return None
        # Milliseconds epoch compatibility.
        if parsed > 1e12:
            parsed /= 1000.0
        return parsed

    text = str(value).strip()
    if not text:
        return None

    variants = [text]
    normalized = text.replace("/", "-")
    if normalized not in variants:
        variants.append(normalized)

    for candidate in variants:
        iso_candidate = candidate
        if iso_candidate.endswith("Z"):
            iso_candidate = f"{iso_candidate[:-1]}+00:00"
        try:
            parsed_iso = datetime.fromisoformat(iso_candidate)
        except ValueError:
            parsed_iso = None
        if parsed_iso is not None:
            dt = parsed_iso if parsed_iso.tzinfo is not None else parsed_iso.replace(tzinfo=timezone.utc)
            return dt.timestamp()

        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(candidate, fmt)
            except ValueError:
                continue
            dt = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
            return dt.timestamp()
    return None


def _first_positive_float(mapping: dict[str, Any], *keys: str) -> float:
    for key in keys:
        parsed = _as_float(mapping.get(key), 0.0)
        if parsed > 0:
            return parsed
    return 0.0


def _precision_from_step(step: float) -> int:
    text = f"{step:.10f}".rstrip("0")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def _normalize_price_floor_for_spec(spec: SymbolSpec, price: float) -> float:
    tick = Decimal(str(max(float(spec.tick_size), FLOAT_COMPARISON_TOLERANCE)))
    value = Decimal(str(float(price)))
    steps = (value / tick).to_integral_value(rounding=ROUND_FLOOR)
    normalized = steps * tick
    return round(float(normalized), spec.price_precision)


def _normalize_price_ceil_for_spec(spec: SymbolSpec, price: float) -> float:
    tick = Decimal(str(max(float(spec.tick_size), FLOAT_COMPARISON_TOLERANCE)))
    value = Decimal(str(float(price)))
    steps = (value / tick).to_integral_value(rounding=ROUND_CEILING)
    normalized = steps * tick
    return round(float(normalized), spec.price_precision)


def _normalize_volume_floor_for_spec(spec: SymbolSpec, raw_volume: float) -> float:
    if raw_volume <= 0:
        return 0.0
    step = max(0.001, float(spec.lot_step) if float(spec.lot_step) > 0 else 0.01)
    step_dec = Decimal(str(step))
    raw_dec = Decimal(str(float(raw_volume))) + (step_dec * Decimal(str(FLOAT_COMPARISON_TOLERANCE)))
    steps = (raw_dec / step_dec).to_integral_value(rounding=ROUND_FLOOR)
    normalized = float(steps * step_dec)
    normalized = min(normalized, float(spec.effective_lot_max()))
    if normalized + FLOAT_ROUNDING_TOLERANCE < float(spec.lot_min):
        return 0.0
    return round(normalized, int(spec.lot_precision))


def _sanitize_deal_reference(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch.isalnum() or ch in {"-", "_"})


def _format_open_deal_reference(value: str) -> str:
    normalized = _sanitize_deal_reference(value)
    return normalized[:30]


def _format_open_deal_reference_for_attempt(value: str, attempt_index: int) -> str:
    base = _format_open_deal_reference(value)
    if attempt_index <= 0:
        return base
    suffix = f"{int(attempt_index) % 100:02d}"
    if len(base) <= 27:
        return f"{base}_{suffix}"
    if len(base) <= 28:
        return f"{base}{suffix}"
    return f"{base[:28]}{suffix}"


def _deal_reference_matches(expected: str, actual: str) -> bool:
    expected_norm = _sanitize_deal_reference(str(expected or "").strip())
    actual_norm = _sanitize_deal_reference(str(actual or "").strip())
    if not expected_norm or not actual_norm:
        return False
    if expected_norm == actual_norm:
        return True
    # Allow short retry suffixes appended to the base reference for re-attempted POSTs.
    if actual_norm.startswith(expected_norm) and (len(actual_norm) - len(expected_norm)) <= 3:
        return True
    if expected_norm.startswith(actual_norm) and (len(expected_norm) - len(actual_norm)) <= 3:
        return True
    return False


def _snapshot_missing_quote_error(
    symbol: str,
    epic: str,
    snapshot: dict[str, Any],
    market: dict[str, Any],
    body: dict[str, Any],
) -> BrokerError:
    market_status = str(
        snapshot.get("marketStatus")
        or body.get("marketStatus")
        or market.get("marketStatus")
        or "-"
    ).strip()
    snapshot_keys = ",".join(sorted(str(key) for key in snapshot.keys())) or "-"
    return BrokerError(
        f"IG snapshot for {symbol} does not contain bid/offer "
        f"(epic={epic} market_status={market_status} "
        f"raw_bid={snapshot.get('bid')!r} raw_offer={snapshot.get('offer')!r} "
        f"snapshot_keys={snapshot_keys})"
    )


def _dedupe_epics(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item).strip().upper()
        if not text or text in seen:
            continue
        unique.append(text)
        seen.add(text)
    return unique


def _normalize_currency_code(value: Any) -> str | None:
    text = str(value or "").strip().upper().replace(".", "")
    if not text:
        return None
    aliases = {
        "€": "EUR",
        "$": "USD",
        "£": "GBP",
        "#": "GBP",
        "E": "EUR",
    }
    mapped = aliases.get(text, text)
    if len(mapped) == 3 and mapped.isalpha():
        return mapped
    return None


def _is_plausible_epic_for_symbol(symbol: str, epic: str) -> bool:
    upper_symbol = str(symbol or "").strip().upper()
    upper_epic = str(epic or "").strip().upper()
    if not upper_symbol or not upper_epic:
        return False

    if upper_symbol.startswith(("CS.", "IX.", "CC.", "UA.", "SA.", "KC.")):
        return upper_symbol == upper_epic

    if _is_index_symbol(upper_symbol):
        return upper_epic.startswith("IX.")

    if _is_fx_pair_symbol(upper_symbol):
        return upper_epic.startswith("CS.D.") and upper_symbol in upper_epic

    if upper_symbol == "WTI":
        if not upper_epic.startswith("CC."):
            return False
        return any(token in upper_epic for token in ("WTI", "CL", "CRUDE", "OIL"))

    if upper_symbol == "BRENT":
        if not upper_epic.startswith("CC."):
            return False
        return any(token in upper_epic for token in ("BRENT", "LCO", "BRN", "CRUDE", "OIL"))

    commodity_hints = COMMODITY_EPIC_HINTS.get(upper_symbol)
    if commodity_hints is not None:
        if not upper_epic.startswith("CC."):
            return False
        return any(token in upper_epic for token in commodity_hints)

    if upper_symbol in {"GOLD", "XAUUSD"}:
        if not upper_epic.startswith(("CS.", "CC.")):
            return False
        return "GOLD" in upper_epic or "XAU" in upper_epic

    crypto_base = _crypto_symbol_base(upper_symbol)
    if crypto_base is not None:
        if not upper_epic.startswith("CS.D."):
            return False
        symbol_matches = (
            upper_symbol in upper_epic
            or f"{crypto_base}USD" in upper_epic
            or (crypto_base == "DOGE" and "DOGUSD" in upper_epic)
        )
        name_matches = (
            (crypto_base == "BTC" and "BITCOIN" in upper_epic)
            or (crypto_base == "ETH" and "ETHEREUM" in upper_epic)
            or (crypto_base == "LTC" and "LITECOIN" in upper_epic)
            or (crypto_base == "SOL" and "SOLANA" in upper_epic)
            or (crypto_base == "XRP" and "RIPPLE" in upper_epic)
            or (crypto_base == "DOGE" and "DOGECOIN" in upper_epic)
        )
        return symbol_matches or name_matches

    if upper_symbol in {"AAPL", "MSFT"}:
        return upper_epic.startswith(("UA.", "SA."))

    return upper_symbol in upper_epic


def _is_index_symbol(symbol: str, epic: str | None = None) -> bool:
    upper = str(symbol).upper()
    if _shared_is_index_symbol(upper):
        return True
    epic_upper = str(epic or "").upper()
    if epic_upper.startswith("IX."):
        return True
    return False


def _is_non_fx_cfd_symbol(symbol: str, epic: str | None = None) -> bool:
    upper_symbol = str(symbol).upper().strip()
    upper_epic = str(epic or "").upper().strip()
    if _is_index_symbol(upper_symbol, upper_epic):
        return True
    if upper_epic.startswith(("UA.", "SA.", "CC.")):
        return True
    if upper_epic.startswith("CS.D."):
        return not _is_fx_pair_symbol(upper_symbol)
    return upper_symbol in {
        "WTI",
        "BRENT",
        "GOLD",
        "XAUUSD",
        "XAGUSD",
        "SILVER",
        "BTC",
        "AAPL",
        "MSFT",
    }


def _epic_variant_key(epic: str) -> str:
    upper_epic = str(epic or "").strip().upper()
    if not upper_epic:
        return "unknown"
    if ".CASH." in upper_epic:
        return "cash"
    if ".DAILY." in upper_epic:
        return "daily"
    if ".IFS." in upper_epic:
        return "ifs"
    if ".IFD." in upper_epic:
        return "ifd"
    if ".CFD." in upper_epic:
        return "cfd"
    return "other"


def _is_fx_pair_symbol(symbol: str) -> bool:
    upper = str(symbol or "").strip().upper()
    if len(upper) != 6 or not upper.isalpha():
        return False
    if _is_crypto_symbol(upper):
        return False
    base = upper[:3]
    quote = upper[3:]
    if base == quote:
        return False
    return base in _FX_MARGIN_CURRENCIES and quote in _FX_MARGIN_CURRENCIES


def _is_crypto_symbol(symbol: str) -> bool:
    return _crypto_symbol_base(symbol) is not None


def _crypto_symbol_base(symbol: str) -> str | None:
    upper = str(symbol or "").strip().upper()
    if not upper:
        return None
    if upper in _CRYPTO_SYMBOL_BASES:
        return upper
    for quote in _CRYPTO_SYMBOL_QUOTES:
        if upper.endswith(quote):
            base = upper[: -len(quote)]
            if base in _CRYPTO_SYMBOL_BASES:
                return base
    return None


def _first_non_empty_text(values: list[str] | tuple[str, ...]) -> str | None:
    for value in values:
        normalized = str(value or "").strip().upper()
        if normalized:
            return normalized
    return None


def _infer_margin_price_scale_divisor(
    symbol: str,
    pip_size: float,
    one_pip_means: float,
    *,
    scaling_factor: float = 1.0,
    pip_position: float | None = None,
) -> float:
    upper = str(symbol or "").strip().upper()
    pip_value = abs(float(pip_size))
    one_pip_value = abs(float(one_pip_means))
    if pip_value > 0.0 and one_pip_value > 0.0:
        divisor = pip_value / one_pip_value
        if math.isfinite(divisor):
            if _is_fx_pair_symbol(upper):
                if divisor >= 100.0:
                    return float(divisor)
            elif divisor >= 10.0:
                return float(divisor)
    if not _is_fx_pair_symbol(upper):
        return 1.0
    scale = abs(float(scaling_factor))
    if math.isfinite(scale) and scale >= 100.0:
        return float(scale)
    if pip_position is not None:
        try:
            pip_position_value = abs(int(float(pip_position)))
        except (TypeError, ValueError):
            pip_position_value = 0
        if pip_position_value >= 2:
            divisor = float(10**pip_position_value)
            if math.isfinite(divisor) and divisor >= 100.0:
                return divisor
    return 1.0


@dataclass(slots=True)
class IgApiClient(BaseBrokerClient):
    identifier: str
    password: str
    api_key: str
    account_type: AccountType
    account_id: str | None = None
    endpoint: str | None = None
    symbol_epics: dict[str, str] = field(default_factory=dict)
    stream_enabled: bool = True
    timeout_sec: float = 10.0
    confirm_timeout_sec: float = 10.0
    stream_read_timeout_sec: float = 35.0
    stream_tick_max_age_sec: float = 15.0
    rest_market_min_interval_sec: float = 2.0
    connect_retry_attempts: int = 4
    connect_retry_base_sec: float = 2.0
    connect_retry_max_sec: float = 30.0
    position_update_callback: Callable[[dict[str, Any]], None] | None = None

    def __post_init__(self) -> None:
        self.identifier = str(self.identifier).strip()
        self.api_key = str(self.api_key).strip().strip('"').strip("'")
        self._lock = threading.RLock()
        self._connectivity_probe_lock = threading.Lock()
        self._auth_refresh_lock = threading.Lock()
        self._auth_refresh_last_attempt_ts = 0.0
        self._auth_refresh_last_success_ts = 0.0
        self._auth_refresh_min_interval_sec = 2.0
        self._runtime_reconnect_lock = threading.Lock()
        self._runtime_reconnect_last_attempt_ts = 0.0
        raw_runtime_reconnect_min_interval = (
            os.getenv("IG_RUNTIME_RECONNECT_MIN_INTERVAL_SEC")
            or os.getenv("XTB_IG_RUNTIME_RECONNECT_MIN_INTERVAL_SEC")
            or "2.0"
        )
        try:
            self._runtime_reconnect_min_interval_sec = max(0.5, float(raw_runtime_reconnect_min_interval))
        except (TypeError, ValueError):
            self._runtime_reconnect_min_interval_sec = 2.0
        self._connected = False
        self._cst: str | None = None
        self._security_token: str | None = None
        self._epics: dict[str, str] = {str(k).upper(): str(v).strip().upper() for k, v in DEFAULT_IG_EPICS.items()}
        for symbol, epic in self.symbol_epics.items():
            self._epics[str(symbol).upper()] = str(epic).strip().upper()
        self._epic_candidates: dict[str, list[str]] = {
            str(symbol).upper(): _dedupe_epics(list(candidates))
            for symbol, candidates in DEFAULT_IG_EPIC_CANDIDATES.items()
        }
        for symbol, epic in self._epics.items():
            existing = self._epic_candidates.get(symbol, [])
            self._epic_candidates[symbol] = _dedupe_epics([str(epic).strip().upper(), *existing])

        self._endpoint = (self.endpoint or self._default_endpoint()).rstrip("/")
        self._symbol_spec_cache: dict[str, SymbolSpec] = {}
        self._symbol_spec_cache_by_epic: dict[tuple[str, str], SymbolSpec] = {}
        self._last_tick_by_symbol: dict[str, float] = {}
        self._tick_cache: dict[str, PriceTick] = {}
        self._stream_tick_handler: Callable[[PriceTick], None] | None = None
        self._stream_enabled = bool(self.stream_enabled)
        raw_stream_sdk_enabled = (
            os.getenv("IG_STREAM_SDK_ENABLED")
            or os.getenv("XTB_IG_STREAM_SDK_ENABLED")
            or "true"
        )
        self._stream_sdk_enabled = str(raw_stream_sdk_enabled).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._stream_sdk_forced_transport = (
            os.getenv("IG_STREAM_SDK_FORCED_TRANSPORT")
            or os.getenv("XTB_IG_STREAM_SDK_FORCED_TRANSPORT")
            or None
        )
        self._stream_sdk_available = _LightstreamerClient is not None and _LightstreamerSubscription is not None
        self._stream_use_sdk = bool(self._stream_enabled and self._stream_sdk_enabled and self._stream_sdk_available)
        self._stream_sdk_client: Any | None = None
        self._stream_sdk_client_listener: Any | None = None
        self._stream_sdk_subscriptions: dict[str, Any] = {}
        self._stream_sdk_subscription_listeners: dict[str, Any] = {}
        self._stream_sdk_pending_subscriptions: set[str] = set()
        self._stream_sdk_connected = False
        self._stream_sdk_status: str | None = None
        if self._stream_enabled and self._stream_sdk_enabled and not self._stream_sdk_available:
            logger.debug(
                "IG stream SDK is not installed; using legacy Lightstreamer transport fallback"
            )
        raw_epic_search_enabled = (
            os.getenv("IG_EPIC_SEARCH_ENABLED")
            or os.getenv("XTB_IG_EPIC_SEARCH_ENABLED")
            or "true"
        )
        self._epic_search_enabled = str(raw_epic_search_enabled).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        raw_epic_failover_enabled = (
            os.getenv("IG_EPIC_FAILOVER_ENABLED")
            or os.getenv("XTB_IG_EPIC_FAILOVER_ENABLED")
            or "true"
        )
        self._epic_failover_enabled = str(raw_epic_failover_enabled).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._price_requests_total = 0
        self._stream_price_hits_total = 0
        self._rest_fallback_hits_total = 0

        self._stream_endpoint: str | None = None
        self._stream_session_id: str | None = None
        self._stream_control_url: str | None = None
        self._stream_keepalive_ms: int | None = None
        self._stream_thread: threading.Thread | None = None
        self._stream_stop_event = threading.Event()
        self._stream_http_response: Any | None = None
        self._stream_desired_subscriptions: set[str] = set()
        self._stream_symbol_to_table: dict[str, int] = {}
        self._stream_table_to_symbol: dict[int, str] = {}
        self._stream_table_field_values: dict[int, list[str | None]] = {}
        self._stream_next_table_id = 1
        self._stream_last_error: str | None = None
        self._stream_reconnect_attempts = 0
        self._stream_total_reconnects = 0
        self._stream_last_disconnect_ts: float | None = None
        self._stream_last_reconnect_ts: float | None = None
        self._stream_next_retry_at: float | None = None
        self._stream_backoff_base_sec = 0.5
        self._stream_backoff_max_sec = 30.0
        raw_stream_stale_tick_max_age = (
            os.getenv("IG_STREAM_STALE_TICK_MAX_AGE_SEC")
            or os.getenv("XTB_IG_STREAM_STALE_TICK_MAX_AGE_SEC")
            or "180.0"
        )
        try:
            self._stream_stale_tick_max_age_sec = max(
                float(self.stream_tick_max_age_sec),
                float(raw_stream_stale_tick_max_age),
            )
        except (TypeError, ValueError):
            self._stream_stale_tick_max_age_sec = max(float(self.stream_tick_max_age_sec), 180.0)
        raw_stream_rest_fallback_min_interval = (
            os.getenv("IG_STREAM_REST_FALLBACK_MIN_INTERVAL_SEC")
            or os.getenv("XTB_IG_STREAM_REST_FALLBACK_MIN_INTERVAL_SEC")
            or "30.0"
        )
        try:
            self._stream_rest_fallback_min_interval_sec = max(0.0, float(raw_stream_rest_fallback_min_interval))
        except (TypeError, ValueError):
            self._stream_rest_fallback_min_interval_sec = 30.0
        self._last_stream_rest_fallback_ts_by_symbol: dict[str, float] = {}
        raw_stream_stagnant_quote_max_age = (
            os.getenv("IG_STREAM_STAGNANT_QUOTE_MAX_AGE_SEC")
            or os.getenv("XTB_IG_STREAM_STAGNANT_QUOTE_MAX_AGE_SEC")
            or "120.0"
        )
        try:
            self._stream_stagnant_quote_max_age_sec = max(0.0, float(raw_stream_stagnant_quote_max_age))
        except (TypeError, ValueError):
            self._stream_stagnant_quote_max_age_sec = 120.0
        self._stream_last_quote_change_ts_by_symbol: dict[str, float] = {}

        self._allowance_cooldown_until_ts = 0.0
        self._allowance_cooldown_sec = 1.0
        self._allowance_cooldown_max_sec = 60.0
        self._allowance_last_error: str | None = None
        self._allowance_last_scope: str | None = None
        self._last_market_rest_request_ts = 0.0
        self._last_market_rest_request_monotonic = 0.0
        rest_interval_floor = _as_float(self.rest_market_min_interval_sec, 0.0)
        if not math.isfinite(rest_interval_floor) or rest_interval_floor <= 0:
            rest_interval_floor = 0.2
        self._rest_market_min_interval_floor_sec = max(0.2, rest_interval_floor)
        raw_rest_market_max_interval = (
            os.getenv("IG_REST_MARKET_MIN_INTERVAL_MAX_SEC")
            or os.getenv("XTB_IG_REST_MARKET_MIN_INTERVAL_MAX_SEC")
            or "8.0"
        )
        try:
            self._rest_market_min_interval_max_sec = max(
                self._rest_market_min_interval_floor_sec,
                float(raw_rest_market_max_interval),
            )
        except (TypeError, ValueError):
            self._rest_market_min_interval_max_sec = max(
                self._rest_market_min_interval_floor_sec,
                8.0,
            )
        raw_epic_unavailable_retry_sec = (
            os.getenv("IG_EPIC_UNAVAILABLE_RETRY_SEC")
            or os.getenv("XTB_IG_EPIC_UNAVAILABLE_RETRY_SEC")
            or "45.0"
        )
        try:
            self._epic_unavailable_retry_sec = max(5.0, float(raw_epic_unavailable_retry_sec))
        except (TypeError, ValueError):
            self._epic_unavailable_retry_sec = 45.0
        self._epic_unavailable_until_ts_by_symbol: dict[str, float] = {}
        raw_invalid_epic_retry_sec = (
            os.getenv("IG_INVALID_EPIC_RETRY_SEC")
            or os.getenv("XTB_IG_INVALID_EPIC_RETRY_SEC")
            or "900.0"
        )
        try:
            self._invalid_epic_retry_sec = max(30.0, float(raw_invalid_epic_retry_sec))
        except (TypeError, ValueError):
            self._invalid_epic_retry_sec = 900.0
        self._invalid_epic_until_ts_by_symbol: dict[str, dict[str, float]] = {}
        self._invalid_epic_warn_last_ts_by_key: dict[tuple[str, str, str], float] = {}
        self._epic_remap_warn_last_ts_by_symbol: dict[str, float] = {}
        self._epic_remap_warn_last_signature_by_symbol: dict[str, tuple[str, str]] = {}
        self._stream_subscription_retry_not_before_ts_by_symbol: dict[str, float] = {}
        self._stream_subscription_retry_gap_sec = 1.0
        self._stream_rest_fallback_block_until_ts_by_symbol: dict[str, float] = {}
        self._stream_rest_fallback_block_reason_by_symbol: dict[str, str] = {}
        raw_index_pip_fallback_one_point = (
            os.getenv("IG_INDEX_PIP_FALLBACK_ONE_POINT")
            or os.getenv("XTB_IG_INDEX_PIP_FALLBACK_ONE_POINT")
            or "true"
        )
        self._index_pip_fallback_one_point = str(raw_index_pip_fallback_one_point).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        raw_guaranteed_stop_mode = (
            os.getenv("IG_GUARANTEED_STOP_MODE")
            or os.getenv("XTB_IG_GUARANTEED_STOP_MODE")
            or "auto"
        )
        normalized_guaranteed_stop_mode = str(raw_guaranteed_stop_mode).strip().lower()
        if normalized_guaranteed_stop_mode not in {"off", "auto", "required"}:
            logger.warning(
                "Invalid IG_GUARANTEED_STOP_MODE=%s, fallback to auto",
                raw_guaranteed_stop_mode,
            )
            normalized_guaranteed_stop_mode = "auto"
        self._guaranteed_stop_mode = normalized_guaranteed_stop_mode
        self._guaranteed_stop_required_symbols: set[str] = set()
        raw_open_level_tolerance_pips = (
            os.getenv("IG_OPEN_LEVEL_TOLERANCE_PIPS")
            or os.getenv("XTB_IG_OPEN_LEVEL_TOLERANCE_PIPS")
            or "0.0"
        )
        try:
            self._open_level_tolerance_pips = max(0.0, float(raw_open_level_tolerance_pips))
        except (TypeError, ValueError):
            self._open_level_tolerance_pips = 0.0
        raw_open_use_quote_id = (
            os.getenv("IG_OPEN_USE_QUOTE_ID")
            or os.getenv("XTB_IG_OPEN_USE_QUOTE_ID")
            or "false"
        )
        self._open_use_quote_id = _as_bool(raw_open_use_quote_id, False)
        raw_open_force_open = (
            os.getenv("IG_OPEN_FORCE_OPEN")
            or os.getenv("XTB_IG_OPEN_FORCE_OPEN")
            or "true"
        )
        self._open_force_open = _as_bool(raw_open_force_open, True)
        self._critical_trade_active_total = 0
        self._critical_trade_owner_threads: dict[int, int] = {}
        raw_trade_submit_min_interval = (
            os.getenv("IG_TRADE_SUBMIT_MIN_INTERVAL_SEC")
            or os.getenv("XTB_IG_TRADE_SUBMIT_MIN_INTERVAL_SEC")
            or "0.75"
        )
        try:
            self._trade_submit_min_interval_sec = max(0.0, float(raw_trade_submit_min_interval))
        except (TypeError, ValueError):
            self._trade_submit_min_interval_sec = 0.75
        self._trade_submit_next_allowed_ts = 0.0
        self._trade_submit_lock = threading.RLock()
        self._request_context = threading.local()

        raw_rate_limit_enabled = (
            os.getenv("IG_RATE_LIMIT_ENABLED")
            or os.getenv("XTB_IG_RATE_LIMIT_ENABLED")
            or "true"
        )
        self._rate_limit_enabled = _as_bool(raw_rate_limit_enabled, True)
        raw_app_non_trading_limit = (
            os.getenv("IG_RATE_LIMIT_APP_NON_TRADING_PER_MIN")
            or os.getenv("XTB_IG_RATE_LIMIT_APP_NON_TRADING_PER_MIN")
            or "60"
        )
        raw_account_non_trading_limit = (
            os.getenv("IG_RATE_LIMIT_ACCOUNT_NON_TRADING_PER_MIN")
            or os.getenv("XTB_IG_RATE_LIMIT_ACCOUNT_NON_TRADING_PER_MIN")
            or "30"
        )
        raw_account_trading_limit = (
            os.getenv("IG_RATE_LIMIT_ACCOUNT_TRADING_PER_MIN")
            or os.getenv("XTB_IG_RATE_LIMIT_ACCOUNT_TRADING_PER_MIN")
            or "100"
        )
        raw_historical_points_limit = (
            os.getenv("IG_RATE_LIMIT_HISTORICAL_POINTS_PER_WEEK")
            or os.getenv("XTB_IG_RATE_LIMIT_HISTORICAL_POINTS_PER_WEEK")
            or "10000"
        )
        try:
            self._rate_limit_app_non_trading_per_min = max(
                0,
                int(float(raw_app_non_trading_limit)),
            )
        except (TypeError, ValueError):
            self._rate_limit_app_non_trading_per_min = 60
        try:
            self._rate_limit_account_non_trading_per_min = max(
                0,
                int(float(raw_account_non_trading_limit)),
            )
        except (TypeError, ValueError):
            self._rate_limit_account_non_trading_per_min = 30
        try:
            self._rate_limit_account_trading_per_min = max(
                0,
                int(float(raw_account_trading_limit)),
            )
        except (TypeError, ValueError):
            self._rate_limit_account_trading_per_min = 100
        try:
            self._rate_limit_historical_points_per_week = max(
                0,
                int(float(raw_historical_points_limit)),
            )
        except (TypeError, ValueError):
            self._rate_limit_historical_points_per_week = 10000
        self._rate_limit_window_sec = 60.0
        self._rate_limit_week_window_sec = 7.0 * 24.0 * 60.0 * 60.0
        self._rate_limit_app_non_trading_requests: deque[float] = deque()
        self._rate_limit_account_non_trading_requests: deque[float] = deque()
        self._rate_limit_account_trading_requests: deque[float] = deque()
        self._rate_limit_historical_points_events: deque[tuple[float, int]] = deque()
        self._rate_limit_historical_points_week_total = 0
        self._rate_limit_worker_keys = (
            "app_non_trading",
            "account_trading",
            "account_non_trading",
            "historical_points",
        )
        self._rate_limit_worker_blocked_total: dict[str, int] = {
            key: 0 for key in self._rate_limit_worker_keys
        }
        self._rate_limit_worker_last_request_ts: dict[str, float] = {
            key: 0.0 for key in self._rate_limit_worker_keys
        }
        self._rate_limit_worker_last_block_ts: dict[str, float] = {
            key: 0.0 for key in self._rate_limit_worker_keys
        }
        raw_request_dispatch_enabled = (
            os.getenv("IG_REQUEST_DISPATCH_ENABLED")
            or os.getenv("XTB_IG_REQUEST_DISPATCH_ENABLED")
            or "true"
        )
        self._request_dispatch_enabled = _as_bool(raw_request_dispatch_enabled, True)
        raw_request_dispatch_timeout = (
            os.getenv("IG_REQUEST_DISPATCH_TIMEOUT_SEC")
            or os.getenv("XTB_IG_REQUEST_DISPATCH_TIMEOUT_SEC")
            or "45.0"
        )
        try:
            self._request_dispatch_timeout_sec = max(5.0, float(raw_request_dispatch_timeout))
        except (TypeError, ValueError):
            self._request_dispatch_timeout_sec = 45.0
        self._request_worker_queue_maxsize = 2048
        self._request_worker_stop_event = threading.Event()
        self._request_worker_thread_ids: dict[int, str] = {}
        self._request_worker_threads: dict[str, threading.Thread] = {}
        self._request_worker_queues: dict[str, Queue[_QueuedRequestJob | None]] = {
            key: Queue(maxsize=self._request_worker_queue_maxsize) for key in self._rate_limit_worker_keys
        }
        raw_historical_http_cache_enabled = (
            os.getenv("IG_HISTORICAL_HTTP_CACHE_ENABLED")
            or os.getenv("XTB_IG_HISTORICAL_HTTP_CACHE_ENABLED")
            or "true"
        )
        self._historical_http_cache_enabled = _as_bool(raw_historical_http_cache_enabled, True)
        raw_historical_http_cache_ttl_sec = (
            os.getenv("IG_HISTORICAL_HTTP_CACHE_TTL_SEC")
            or os.getenv("XTB_IG_HISTORICAL_HTTP_CACHE_TTL_SEC")
            or "600"
        )
        try:
            self._historical_http_cache_ttl_sec = max(1.0, float(raw_historical_http_cache_ttl_sec))
        except (TypeError, ValueError):
            self._historical_http_cache_ttl_sec = 600.0
        raw_historical_http_cache_max_entries = (
            os.getenv("IG_HISTORICAL_HTTP_CACHE_MAX_ENTRIES")
            or os.getenv("XTB_IG_HISTORICAL_HTTP_CACHE_MAX_ENTRIES")
            or "256"
        )
        try:
            self._historical_http_cache_max_entries = max(
                8,
                int(float(raw_historical_http_cache_max_entries)),
            )
        except (TypeError, ValueError):
            self._historical_http_cache_max_entries = 256
        self._historical_http_cache: OrderedDict[str, tuple[float, dict[str, Any], dict[str, str]]] = OrderedDict()

        self._account_snapshot_cache: AccountSnapshot | None = None
        self._account_snapshot_cached_at = 0.0
        self._account_snapshot_cache_ttl_sec = 10.0
        raw_connectivity_status_cache_ttl_sec = (
            os.getenv("IG_CONNECTIVITY_STATUS_CACHE_TTL_SEC")
            or os.getenv("XTB_IG_CONNECTIVITY_STATUS_CACHE_TTL_SEC")
            or "10.0"
        )
        try:
            self._connectivity_status_cache_ttl_sec = max(0.0, float(raw_connectivity_status_cache_ttl_sec))
        except (TypeError, ValueError):
            self._connectivity_status_cache_ttl_sec = 10.0
        raw_connectivity_status_unhealthy_cache_ttl_sec = (
            os.getenv("IG_CONNECTIVITY_STATUS_UNHEALTHY_CACHE_TTL_SEC")
            or os.getenv("XTB_IG_CONNECTIVITY_STATUS_UNHEALTHY_CACHE_TTL_SEC")
            or "2.0"
        )
        try:
            self._connectivity_status_unhealthy_cache_ttl_sec = max(
                0.0,
                float(raw_connectivity_status_unhealthy_cache_ttl_sec),
            )
        except (TypeError, ValueError):
            self._connectivity_status_unhealthy_cache_ttl_sec = 2.0
        self._connectivity_status_cache: ConnectivityStatus | None = None
        self._connectivity_status_cache_ts = 0.0
        self._account_currency_code: str | None = None
        self._account_currency_code_lock = threading.Lock()
        self._position_open_sync: dict[str, dict[str, Any]] = {}
        self._position_close_sync: dict[str, dict[str, Any]] = {}
        self._pending_confirm_last_probe_ts: dict[str, float] = {}
        self._pending_confirm_probe_min_interval_sec = 30.0
        raw_sync_cache_max_age_sec = (
            os.getenv("IG_SYNC_CACHE_MAX_AGE_SEC")
            or os.getenv("XTB_IG_SYNC_CACHE_MAX_AGE_SEC")
            or "86400"
        )
        try:
            self._sync_cache_max_age_sec = max(300.0, float(raw_sync_cache_max_age_sec))
        except (TypeError, ValueError):
            self._sync_cache_max_age_sec = 86400.0
        raw_cache_cleanup_interval_sec = (
            os.getenv("IG_CACHE_CLEANUP_INTERVAL_SEC")
            or os.getenv("XTB_IG_CACHE_CLEANUP_INTERVAL_SEC")
            or "60"
        )
        try:
            self._cache_cleanup_interval_sec = max(5.0, float(raw_cache_cleanup_interval_sec))
        except (TypeError, ValueError):
            self._cache_cleanup_interval_sec = 60.0
        self._last_cache_cleanup_ts = 0.0
        self.connect_retry_attempts = max(1, int(self.connect_retry_attempts))
        self.connect_retry_base_sec = max(0.5, float(self.connect_retry_base_sec))
        self.connect_retry_max_sec = max(self.connect_retry_base_sec, float(self.connect_retry_max_sec))

    @staticmethod
    def _normalize_lightstreamer_endpoint(endpoint: str | None) -> str | None:
        if endpoint is None:
            return None
        value = str(endpoint).strip()
        if not value:
            return None
        if not value.startswith(("http://", "https://")):
            value = f"https://{value.lstrip('/')}"
        value = value.rstrip("/")
        if not value.endswith("/lightstreamer"):
            value = f"{value}/lightstreamer"
        return value

    def _default_endpoint(self) -> str:
        if self.account_type == AccountType.LIVE:
            return "https://api.ig.com/gateway/deal"
        return "https://demo-api.ig.com/gateway/deal"

    def _auth_headers(self, include_session_tokens: bool = True) -> dict[str, str]:
        headers = {
            "Accept": "application/json; charset=UTF-8",
            "Content-Type": "application/json; charset=UTF-8",
            "X-IG-API-KEY": self.api_key,
        }
        if include_session_tokens and self._cst:
            headers["CST"] = self._cst
        if include_session_tokens and self._security_token:
            headers["X-SECURITY-TOKEN"] = self._security_token
        return headers

    @staticmethod
    def _form_body(params: dict[str, Any]) -> bytes:
        payload = {key: str(value) for key, value in params.items() if value is not None}
        return parse.urlencode(payload).encode("utf-8")

    @staticmethod
    def _is_trade_critical_request(method: str, path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if normalized_path.startswith("/confirms/") and upper_method == "GET":
            return True
        if normalized_path == "/positions/otc" and upper_method in {"POST", "DELETE"}:
            return True
        if normalized_path.startswith("/positions/otc/") and upper_method == "PUT":
            return True
        return False

    @staticmethod
    def _is_trading_allowance_request(method: str, path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if normalized_path == "/positions/otc" and upper_method in {"POST", "DELETE", "PUT"}:
            return True
        if normalized_path.startswith("/positions/otc/") and upper_method in {"PUT", "DELETE"}:
            return True
        if normalized_path == "/workingorders/otc" and upper_method in {"POST", "DELETE", "PUT"}:
            return True
        if normalized_path.startswith("/workingorders/otc/") and upper_method in {"PUT", "DELETE"}:
            return True
        return False

    @staticmethod
    def _is_historical_price_request(path: str, method: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        return upper_method == "GET" and normalized_path.startswith("/prices/")

    @staticmethod
    def _is_position_update_request_path(path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        return (
            normalized_path == "/positions"
            or normalized_path.startswith("/positions/")
            or normalized_path.startswith("/confirms/")
        )

    @staticmethod
    def _position_update_operation(method: str, path: str, request_payload: dict[str, Any] | None = None) -> str:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if normalized_path.startswith("/confirms/"):
            return "confirm"
        if normalized_path == "/positions/otc" and upper_method == "POST":
            payload = _as_mapping(request_payload)
            has_deal_id = bool(str(payload.get("dealId") or "").strip())
            has_currency_code = bool(str(payload.get("currencyCode") or "").strip())
            if has_deal_id and not has_currency_code:
                return "close_position"
            return "open_position"
        if normalized_path == "/positions/otc" and upper_method == "DELETE":
            return "close_position"
        if normalized_path.startswith("/positions/otc/") and upper_method == "PUT":
            return "modify_position"
        if normalized_path == "/positions" and upper_method == "GET":
            return "positions_snapshot"
        if normalized_path.startswith("/positions/") and upper_method == "GET":
            return "position_details"
        return "position_request"

    @staticmethod
    def _iter_nested_mappings(value: Any) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                result.append(node)
                for nested in node.values():
                    _walk(nested)
                return
            if isinstance(node, list):
                for nested in node:
                    _walk(nested)

        _walk(value)
        return result

    @staticmethod
    def _extract_first_text_from_payload(value: Any, *keys: str) -> str | None:
        for mapping in IgApiClient._iter_nested_mappings(value):
            raw = IgApiClient._history_get(mapping, *keys)
            if raw in (None, ""):
                continue
            text = str(raw).strip()
            if text:
                return text
        return None

    @staticmethod
    def _position_id_from_path(path: str) -> str | None:
        normalized_path = str(path).split("?", 1)[0]
        if normalized_path.startswith("/positions/otc/"):
            value = parse.unquote(normalized_path[len("/positions/otc/") :]).strip()
            return value or None
        if normalized_path.startswith("/positions/"):
            value = parse.unquote(normalized_path[len("/positions/") :]).strip()
            if value and value.lower() != "otc":
                return value
        return None

    @staticmethod
    def _deal_reference_from_path(path: str) -> str | None:
        normalized_path = str(path).split("?", 1)[0]
        if not normalized_path.startswith("/confirms/"):
            return None
        value = parse.unquote(normalized_path[len("/confirms/") :]).strip()
        return value or None

    def _emit_position_update(
        self,
        *,
        method: str,
        path: str,
        request_payload: dict[str, Any] | None,
        query: dict[str, Any] | None,
        response_payload: dict[str, Any] | None,
        http_status: int | None,
        success: bool,
        error_text: str | None,
    ) -> None:
        callback = self.position_update_callback
        if callback is None:
            return
        if not self._is_position_update_request_path(path):
            return

        normalized_path = str(path).split("?", 1)[0]
        position_id = (
            self._position_id_from_path(normalized_path)
            or self._extract_first_text_from_payload(
                response_payload,
                "dealId",
                "positionId",
                "position_id",
            )
            or self._extract_first_text_from_payload(
                request_payload,
                "dealId",
                "positionId",
                "position_id",
            )
        )
        deal_reference = (
            self._deal_reference_from_path(normalized_path)
            or self._extract_first_text_from_payload(
                response_payload,
                "dealReference",
                "deal_reference",
                "reference",
            )
            or self._extract_first_text_from_payload(
                request_payload,
                "dealReference",
                "deal_reference",
                "reference",
            )
        )
        epic = (
            self._extract_first_text_from_payload(request_payload, "epic", "marketEpic")
            or self._extract_first_text_from_payload(response_payload, "epic", "marketEpic")
        )
        symbol = (
            self._symbol_for_epic(epic) if epic else None
        ) or self._extract_first_text_from_payload(
            request_payload,
            "symbol",
            "instrumentName",
        ) or self._extract_first_text_from_payload(
            response_payload,
            "symbol",
            "instrumentName",
        )

        event_payload: dict[str, Any] = {
            "ts": time.time(),
            "source": "ig_rest",
            "operation": self._position_update_operation(method, normalized_path, request_payload),
            "method": str(method).upper().strip(),
            "path": normalized_path,
            "symbol": str(symbol).strip().upper() if symbol else None,
            "position_id": position_id,
            "deal_reference": deal_reference,
            "http_status": int(http_status) if http_status is not None else None,
            "success": bool(success),
            "error_text": str(error_text).strip() if error_text else None,
            "request": request_payload if request_payload is not None else None,
            "response": response_payload if response_payload is not None else None,
        }
        if isinstance(query, dict) and query:
            event_payload["query"] = dict(query)

        try:
            callback(event_payload)
        except Exception:
            logger.debug(
                "IG position update callback failed | method=%s path=%s",
                str(method).upper(),
                normalized_path,
                exc_info=True,
            )

    @staticmethod
    def _parse_positive_int(value: Any) -> int:
        try:
            parsed = int(float(value))
        except (TypeError, ValueError):
            return 0
        return max(0, parsed)

    def _extract_requested_history_points(self, path: str, query: dict[str, Any] | None) -> int:
        if not self._is_historical_price_request(path, "GET"):
            return 0
        candidates: list[Any] = []
        if isinstance(query, dict):
            for key in ("max", "numPoints", "pageSize", "count"):
                if key in query and query.get(key) is not None:
                    candidates.append(query.get(key))
        parsed_url = parse.urlsplit(str(path))
        if parsed_url.query:
            query_params = parse.parse_qs(parsed_url.query, keep_blank_values=False)
            for key in ("max", "numPoints", "pageSize", "count"):
                values = query_params.get(key)
                if values:
                    candidates.append(values[-1])
        for candidate in candidates:
            points = self._parse_positive_int(candidate)
            if points > 0:
                return points
        return 1

    @staticmethod
    def _request_window_wait_locked(
        timestamps: deque[float],
        limit_per_window: int,
        now_ts: float,
        window_sec: float,
    ) -> float:
        if limit_per_window <= 0:
            return float("inf")
        while timestamps and (now_ts - timestamps[0]) >= window_sec:
            timestamps.popleft()
        if len(timestamps) < limit_per_window:
            return 0.0
        wait_sec = window_sec - (now_ts - timestamps[0])
        if not math.isfinite(wait_sec):
            return 0.0
        return max(0.0, wait_sec)

    def _prune_historical_points_window_locked(self, now_ts: float) -> None:
        window_sec = float(self._rate_limit_week_window_sec)
        while self._rate_limit_historical_points_events:
            event_ts, points = self._rate_limit_historical_points_events[0]
            if (now_ts - event_ts) < window_sec:
                break
            self._rate_limit_historical_points_events.popleft()
            self._rate_limit_historical_points_week_total = max(
                0,
                int(self._rate_limit_historical_points_week_total) - int(points),
            )

    def _record_rate_limit_worker_block_locked(self, worker_key: str, now_ts: float) -> None:
        normalized = str(worker_key).strip().lower()
        if normalized not in self._rate_limit_worker_blocked_total:
            return
        self._rate_limit_worker_blocked_total[normalized] = (
            int(self._rate_limit_worker_blocked_total.get(normalized, 0)) + 1
        )
        self._rate_limit_worker_last_block_ts[normalized] = float(now_ts)

    def _record_rate_limit_worker_request_locked(self, worker_key: str, now_ts: float) -> None:
        normalized = str(worker_key).strip().lower()
        if normalized not in self._rate_limit_worker_last_request_ts:
            return
        self._rate_limit_worker_last_request_ts[normalized] = float(now_ts)

    def _rate_limit_worker_usage_snapshot_locked(self, now_ts: float) -> list[dict[str, Any]]:
        self._request_window_wait_locked(
            self._rate_limit_app_non_trading_requests,
            int(self._rate_limit_app_non_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._request_window_wait_locked(
            self._rate_limit_account_non_trading_requests,
            int(self._rate_limit_account_non_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._request_window_wait_locked(
            self._rate_limit_account_trading_requests,
            int(self._rate_limit_account_trading_per_min),
            now_ts,
            self._rate_limit_window_sec,
        )
        self._prune_historical_points_window_locked(now_ts)

        app_used = int(len(self._rate_limit_app_non_trading_requests))
        app_limit = int(self._rate_limit_app_non_trading_per_min)
        account_non_trading_used = int(len(self._rate_limit_account_non_trading_requests))
        account_non_trading_limit = int(self._rate_limit_account_non_trading_per_min)
        trading_used = int(len(self._rate_limit_account_trading_requests))
        trading_limit = int(self._rate_limit_account_trading_per_min)
        historical_used = int(self._rate_limit_historical_points_week_total)
        historical_limit = int(self._rate_limit_historical_points_per_week)

        return [
            {
                "worker_key": "app_non_trading",
                "limit_scope": "per_app",
                "limit_unit": "requests_per_minute",
                "limit_value": app_limit,
                "used_value": app_used,
                "remaining_value": max(0, app_limit - app_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("app_non_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("app_non_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("app_non_trading", 0.0) or 0.0),
                "notes": "IG per-app non-trading requests/minute",
            },
            {
                "worker_key": "account_trading",
                "limit_scope": "per_account",
                "limit_unit": "requests_per_minute",
                "limit_value": trading_limit,
                "used_value": trading_used,
                "remaining_value": max(0, trading_limit - trading_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("account_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("account_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("account_trading", 0.0) or 0.0),
                "notes": "IG per-account trading requests/minute",
            },
            {
                "worker_key": "account_non_trading",
                "limit_scope": "per_account",
                "limit_unit": "requests_per_minute",
                "limit_value": account_non_trading_limit,
                "used_value": account_non_trading_used,
                "remaining_value": max(0, account_non_trading_limit - account_non_trading_used),
                "window_sec": float(self._rate_limit_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("account_non_trading", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("account_non_trading", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("account_non_trading", 0.0) or 0.0),
                "notes": "IG per-account non-trading requests/minute",
            },
            {
                "worker_key": "historical_points",
                "limit_scope": "per_app",
                "limit_unit": "points_per_week",
                "limit_value": historical_limit,
                "used_value": historical_used,
                "remaining_value": max(0, historical_limit - historical_used),
                "window_sec": float(self._rate_limit_week_window_sec),
                "blocked_total": int(self._rate_limit_worker_blocked_total.get("historical_points", 0)),
                "last_request_ts": float(self._rate_limit_worker_last_request_ts.get("historical_points", 0.0) or 0.0),
                "last_block_ts": float(self._rate_limit_worker_last_block_ts.get("historical_points", 0.0) or 0.0),
                "notes": "IG historical price data points/week",
            },
        ]

    def get_rate_limit_workers_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            now_ts = time.time()
            return self._rate_limit_worker_usage_snapshot_locked(now_ts)

    def _reserve_request_rate_limit_slot(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None,
    ) -> None:
        if not self._rate_limit_enabled:
            return
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        is_trading = self._is_trading_allowance_request(upper_method, normalized_path)
        requested_history_points = (
            self._extract_requested_history_points(path, query)
            if self._is_historical_price_request(normalized_path, upper_method)
            else 0
        )
        is_historical = requested_history_points > 0
        sleep_cap_sec = 0.25
        while True:
            sleep_sec = 0.0
            with self._lock:
                now_ts = time.time()
                if is_trading:
                    trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_account_trading_requests,
                        int(self._rate_limit_account_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    sleep_sec = trading_wait_sec
                    if trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("account_trading", now_ts)
                else:
                    account_non_trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_account_non_trading_requests,
                        int(self._rate_limit_account_non_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    app_non_trading_wait_sec = self._request_window_wait_locked(
                        self._rate_limit_app_non_trading_requests,
                        int(self._rate_limit_app_non_trading_per_min),
                        now_ts,
                        self._rate_limit_window_sec,
                    )
                    sleep_sec = max(account_non_trading_wait_sec, app_non_trading_wait_sec)
                    if account_non_trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("account_non_trading", now_ts)
                    if app_non_trading_wait_sec > 0:
                        self._record_rate_limit_worker_block_locked("app_non_trading", now_ts)

                if not math.isfinite(sleep_sec):
                    worker_key = "account_trading" if is_trading else "account_non_trading"
                    raise BrokerError(
                        "IG local rate limiter blocks all requests for this allowance window "
                        f"(worker={worker_key}, limit=0)"
                    )
                if sleep_sec <= 0:
                    if requested_history_points > 0 and self._rate_limit_historical_points_per_week > 0:
                        requested = int(requested_history_points)
                        weekly_limit = int(self._rate_limit_historical_points_per_week)
                        if requested > weekly_limit:
                            self._record_rate_limit_worker_block_locked("historical_points", now_ts)
                            raise BrokerError(
                                "IG historical price request exceeds weekly allowance "
                                f"(requested={requested}, weekly_limit={weekly_limit})"
                            )
                        self._prune_historical_points_window_locked(now_ts)
                        weekly_used = int(self._rate_limit_historical_points_week_total)
                        if (weekly_used + requested) > weekly_limit:
                            self._record_rate_limit_worker_block_locked("historical_points", now_ts)
                            raise BrokerError(
                                "IG historical price weekly allowance exceeded by local limiter "
                                f"(used={weekly_used}, requested={requested}, weekly_limit={weekly_limit})"
                            )
                    if is_trading:
                        self._rate_limit_account_trading_requests.append(now_ts)
                        self._record_rate_limit_worker_request_locked("account_trading", now_ts)
                    else:
                        self._rate_limit_account_non_trading_requests.append(now_ts)
                        self._rate_limit_app_non_trading_requests.append(now_ts)
                        self._record_rate_limit_worker_request_locked("account_non_trading", now_ts)
                        self._record_rate_limit_worker_request_locked("app_non_trading", now_ts)
                    if requested_history_points > 0 and self._rate_limit_historical_points_per_week > 0:
                        points = int(requested_history_points)
                        self._rate_limit_historical_points_events.append((now_ts, points))
                        self._rate_limit_historical_points_week_total += points
                    if is_historical:
                        self._record_rate_limit_worker_request_locked("historical_points", now_ts)
                    return
            time.sleep(min(sleep_cap_sec, sleep_sec))

    @staticmethod
    def _is_non_critical_rest_request(method: str, path: str) -> bool:
        return not IgApiClient._is_trade_critical_request(method, path)

    def _critical_trade_active_for_other_threads(self) -> bool:
        thread_id = threading.get_ident()
        with self._lock:
            if self._critical_trade_active_total <= 0:
                return False
            return self._critical_trade_owner_threads.get(thread_id, 0) <= 0

    def _current_thread_has_critical_trade_context(self) -> bool:
        thread_id = threading.get_ident()
        with self._lock:
            return self._critical_trade_owner_threads.get(thread_id, 0) > 0

    def _current_thread_request_critical_bypass(self) -> bool:
        return bool(getattr(self._request_context, "critical_bypass", False))

    def _should_defer_non_critical_request(self, method: str, path: str) -> bool:
        if not self._critical_trade_active_for_other_threads():
            return False
        return self._is_non_critical_rest_request(method, path)

    def _critical_trade_defer_remaining_sec(self) -> float:
        now = time.time()
        with self._lock:
            remaining = max(0.0, self._trade_submit_next_allowed_ts - now)
            if self._critical_trade_active_total > 0:
                remaining = max(remaining, float(self._trade_submit_min_interval_sec))
        return max(0.1, remaining)

    @contextmanager
    def _critical_trade_operation(self, operation: str):
        _ = operation
        thread_id = threading.get_ident()
        with self._trade_submit_lock:
            while True:
                with self._lock:
                    wait_sec = self._trade_submit_next_allowed_ts - time.time()
                if wait_sec <= 0:
                    break
                time.sleep(min(wait_sec, 0.25))
            with self._lock:
                self._critical_trade_active_total += 1
                self._critical_trade_owner_threads[thread_id] = self._critical_trade_owner_threads.get(thread_id, 0) + 1
        try:
            yield
        finally:
            with self._lock:
                current = self._critical_trade_owner_threads.get(thread_id, 0)
                if current <= 1:
                    self._critical_trade_owner_threads.pop(thread_id, None)
                else:
                    self._critical_trade_owner_threads[thread_id] = current - 1
                self._critical_trade_active_total = max(0, self._critical_trade_active_total - 1)
                min_interval = max(0.0, float(self._trade_submit_min_interval_sec))
                if min_interval > 0:
                    self._trade_submit_next_allowed_ts = max(
                        self._trade_submit_next_allowed_ts,
                        time.time() + min_interval,
                    )

    def _request_worker_key(self, method: str, path: str) -> str:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if self._is_historical_price_request(normalized_path, upper_method):
            return "historical_points"
        if self._is_trading_allowance_request(upper_method, normalized_path):
            return "account_trading"
        if normalized_path.startswith("/accounts"):
            return "account_non_trading"
        if normalized_path.startswith("/positions"):
            return "account_non_trading"
        if normalized_path.startswith("/workingorders"):
            return "account_non_trading"
        if normalized_path.startswith("/confirms/"):
            return "account_non_trading"
        if normalized_path.startswith("/history/"):
            return "account_non_trading"
        return "app_non_trading"

    def _historical_http_cache_key(
        self,
        method: str,
        path: str,
        version: str,
        query: dict[str, Any] | None,
    ) -> str | None:
        normalized_path = str(path).split("?", 1)[0]
        upper_method = str(method).upper().strip()
        if not self._is_historical_price_request(normalized_path, upper_method):
            return None
        normalized_query = {
            str(key): str(value)
            for key, value in (query or {}).items()
            if value is not None
        }
        query_blob = json.dumps(normalized_query, sort_keys=True, separators=(",", ":"))
        return f"{upper_method}|{normalized_path}|v{str(version)}|{query_blob}"

    def _historical_http_cache_get(
        self,
        method: str,
        path: str,
        version: str,
        query: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, str]] | None:
        if not self._historical_http_cache_enabled:
            return None
        key = self._historical_http_cache_key(method, path, version, query)
        if not key:
            return None
        now = time.time()
        with self._lock:
            row = self._historical_http_cache.get(key)
            if row is None:
                return None
            cached_at, body, headers = row
            if (now - cached_at) > self._historical_http_cache_ttl_sec:
                self._historical_http_cache.pop(key, None)
                return None
            self._historical_http_cache.move_to_end(key)
            return copy.deepcopy(body), dict(headers)

    def _historical_http_cache_put(
        self,
        method: str,
        path: str,
        version: str,
        query: dict[str, Any] | None,
        body: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        if not self._historical_http_cache_enabled:
            return
        key = self._historical_http_cache_key(method, path, version, query)
        if not key:
            return
        now = time.time()
        with self._lock:
            self._historical_http_cache[key] = (now, copy.deepcopy(body), dict(headers))
            self._historical_http_cache.move_to_end(key)
            while len(self._historical_http_cache) > self._historical_http_cache_max_entries:
                self._historical_http_cache.popitem(last=False)

    @staticmethod
    def _extract_mid_history_price_component(item: dict[str, Any], field_name: str) -> float | None:
        price_component = _as_mapping(item.get(field_name))
        bid = _as_float(price_component.get("bid"), float("nan"))
        ask = _as_float(price_component.get("ask"), float("nan"))
        last_traded = _as_float(price_component.get("lastTraded"), float("nan"))
        if math.isfinite(bid) and math.isfinite(ask):
            return (bid + ask) / 2.0
        if math.isfinite(last_traded):
            return float(last_traded)
        if math.isfinite(bid):
            return float(bid)
        if math.isfinite(ask):
            return float(ask)
        fallback = _as_float(item.get(field_name), float("nan"))
        if math.isfinite(fallback):
            return float(fallback)
        return None

    @staticmethod
    def _extract_mid_close_from_history_price_item(item: dict[str, Any]) -> float | None:
        return IgApiClient._extract_mid_history_price_component(item, "closePrice")

    @staticmethod
    def _extract_timestamp_from_history_price_item(item: dict[str, Any]) -> float | None:
        for key in ("snapshotTimeUTC", "snapshotTime", "updateTimeUTC", "updateTime"):
            parsed = _parse_datetime_to_unix_seconds(item.get(key))
            if parsed is not None and parsed > 0:
                return parsed
        return None

    def fetch_recent_price_history(
        self,
        symbol: str,
        *,
        resolution: str = "MINUTE",
        points: int = 120,
    ) -> list[tuple[float, float, float | None, float | None, float | None, float | None]]:
        upper = str(symbol).strip().upper()
        if not upper:
            return []
        try:
            requested_points = int(points)
        except (TypeError, ValueError):
            requested_points = 120
        requested_points = max(2, min(requested_points, 2000))
        requested_resolution = str(resolution or "MINUTE").strip().upper() or "MINUTE"
        query = {"resolution": requested_resolution, "max": requested_points}
        attempts = self._epic_attempt_order(upper)
        for epic in attempts:
            try:
                body, _ = self._request(
                    "GET",
                    f"/prices/{epic}",
                    version="3",
                    auth=True,
                    query=query,
                )
            except BrokerError as exc:
                if self._is_epic_unavailable_error_text(str(exc)):
                    continue
                raise
            prices_raw = body.get("prices")
            prices = prices_raw if isinstance(prices_raw, list) else []
            if not prices:
                continue

            parsed_rows: dict[float, tuple[float, float | None, float | None, float | None, float | None]] = {}
            for raw_item in prices:
                item = _as_mapping(raw_item)
                ts = self._extract_timestamp_from_history_price_item(item)
                if ts is None:
                    continue
                close = self._extract_mid_close_from_history_price_item(item)
                if close is None or not math.isfinite(close) or close <= 0:
                    continue
                open_price = self._extract_mid_history_price_component(item, "openPrice")
                high_price = self._extract_mid_history_price_component(item, "highPrice")
                low_price = self._extract_mid_history_price_component(item, "lowPrice")
                volume_raw = _as_float(item.get("lastTradedVolume"), float("nan"))
                volume: float | None = None
                if math.isfinite(volume_raw) and volume_raw > 0:
                    volume = float(volume_raw)
                parsed_rows[float(ts)] = (
                    float(close),
                    volume,
                    float(open_price) if open_price is not None and math.isfinite(open_price) and open_price > 0 else None,
                    float(high_price) if high_price is not None and math.isfinite(high_price) and high_price > 0 else None,
                    float(low_price) if low_price is not None and math.isfinite(low_price) and low_price > 0 else None,
                )

            if not parsed_rows:
                continue
            self._activate_epic(upper, epic)
            ordered: list[tuple[float, float, float | None, float | None, float | None, float | None]] = []
            for ts in sorted(parsed_rows.keys()):
                close, volume, open_price, high_price, low_price = parsed_rows[ts]
                ordered.append((float(ts), float(close), volume, open_price, high_price, low_price))
            return ordered
        return []

    def _ensure_request_workers_started(self) -> None:
        if not self._request_dispatch_enabled:
            return
        with self._lock:
            self._ensure_request_workers_started_locked()

    def _ensure_request_workers_started_locked(self) -> None:
        if not self._request_dispatch_enabled:
            return
        self._request_worker_stop_event.clear()
        for worker_key in self._rate_limit_worker_keys:
            thread = self._request_worker_threads.get(worker_key)
            if thread is not None and thread.is_alive():
                continue
            worker_queue = self._request_worker_queues.setdefault(
                worker_key,
                Queue(maxsize=self._request_worker_queue_maxsize),
            )
            worker_thread = threading.Thread(
                target=self._request_worker_loop,
                args=(worker_key, worker_queue),
                name=f"ig-req-{worker_key}",
                daemon=True,
            )
            self._request_worker_threads[worker_key] = worker_thread
            worker_thread.start()

    def _stop_request_workers_locked(self) -> list[threading.Thread]:
        threads = [thread for thread in self._request_worker_threads.values() if thread is not None]
        if not threads:
            return []
        self._request_worker_stop_event.set()
        for worker_key in self._rate_limit_worker_keys:
            worker_queue = self._request_worker_queues.get(worker_key)
            if worker_queue is None:
                continue
            try:
                worker_queue.put(None, timeout=0.1)
            except Full:
                logger.warning("IG request worker queue remained full during shutdown: %s", worker_key)
        self._request_worker_threads = {}
        return threads

    def _request_worker_loop(
        self,
        worker_key: str,
        worker_queue: Queue[_QueuedRequestJob | None],
    ) -> None:
        thread_id = threading.get_ident()
        with self._lock:
            self._request_worker_thread_ids[thread_id] = str(worker_key)
        try:
            while not self._request_worker_stop_event.is_set():
                try:
                    job = worker_queue.get(timeout=0.2)
                except Empty:
                    continue
                if job is None:
                    worker_queue.task_done()
                    break
                try:
                    prior_bypass = self._current_thread_request_critical_bypass()
                    self._request_context.critical_bypass = bool(job.critical_bypass)
                    job.result = self._request_direct(
                        job.method,
                        job.path,
                        payload=job.payload,
                        version=job.version,
                        auth=job.auth,
                        extra_headers=job.extra_headers,
                        query=job.query,
                        _retry_on_auth_failure=job.retry_on_auth_failure,
                        _critical_bypass=bool(job.critical_bypass),
                    )
                except Exception as exc:  # pragma: no cover - forwarded to caller
                    job.error = exc
                finally:
                    self._request_context.critical_bypass = prior_bypass
                    job.done.set()
                    worker_queue.task_done()
        finally:
            with self._lock:
                self._request_worker_thread_ids.pop(thread_id, None)

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        version: str = "1",
        auth: bool = True,
        extra_headers: dict[str, str] | None = None,
        query: dict[str, Any] | None = None,
        _retry_on_auth_failure: bool = True,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        if not self._request_dispatch_enabled:
            return self._request_direct(
                method,
                path,
                payload=payload,
                version=version,
                auth=auth,
                extra_headers=extra_headers,
                query=query,
                _retry_on_auth_failure=_retry_on_auth_failure,
                _critical_bypass=self._current_thread_request_critical_bypass(),
            )

        current_thread_id = threading.get_ident()
        with self._lock:
            current_worker_key = self._request_worker_thread_ids.get(current_thread_id)
        if current_worker_key:
            return self._request_direct(
                method,
                path,
                payload=payload,
                version=version,
                auth=auth,
                extra_headers=extra_headers,
                query=query,
                _retry_on_auth_failure=_retry_on_auth_failure,
            )

        worker_key = self._request_worker_key(method, path)
        self._ensure_request_workers_started()
        with self._lock:
            worker_queue = self._request_worker_queues.get(worker_key)
        if worker_queue is None:
            return self._request_direct(
                method,
                path,
                payload=payload,
                version=version,
                auth=auth,
                extra_headers=extra_headers,
                query=query,
                _retry_on_auth_failure=_retry_on_auth_failure,
            )

        job = _QueuedRequestJob(
            method=str(method),
            path=str(path),
            payload=payload,
            version=str(version),
            auth=bool(auth),
            extra_headers=dict(extra_headers) if isinstance(extra_headers, dict) else extra_headers,
            query=dict(query) if isinstance(query, dict) else query,
            retry_on_auth_failure=bool(_retry_on_auth_failure),
            critical_bypass=self._current_thread_has_critical_trade_context(),
        )
        try:
            worker_queue.put(
                job,
                timeout=min(0.5, max(0.05, float(self._request_dispatch_timeout_sec))),
            )
        except Full as exc:
            raise BrokerError(
                "IG request dispatch queue full "
                f"(worker={worker_key}, method={str(method).upper()}, path={str(path).split('?', 1)[0]})"
            ) from exc
        dispatch_timeout_sec = max(
            5.0,
            float(self._request_dispatch_timeout_sec),
        )
        if not job.done.wait(timeout=dispatch_timeout_sec):
            raise BrokerError(
                "IG request dispatch timeout "
                f"(worker={worker_key}, method={str(method).upper()}, path={str(path).split('?', 1)[0]})"
            )
        if job.error is not None:
            raise job.error
        if job.result is None:
            raise BrokerError(
                "IG request dispatch returned no result "
                f"(worker={worker_key}, method={str(method).upper()}, path={str(path).split('?', 1)[0]})"
            )
        return job.result

    def _request_direct(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        version: str = "1",
        auth: bool = True,
        extra_headers: dict[str, str] | None = None,
        query: dict[str, Any] | None = None,
        _retry_on_auth_failure: bool = True,
        _retry_on_transient_http: bool = True,
        _retry_on_network_error: bool = True,
        _critical_bypass: bool = False,
    ) -> tuple[dict[str, Any], dict[str, str]]:
        if auth and not self._is_connected():
            if not self._maybe_reconnect_after_disconnected(method=method, path=path):
                raise BrokerError("IG API client is not connected")
        is_historical_request = self._is_historical_price_request(path, method)
        # Keep historical weekly limiter authoritative even on HTTP cache hits.
        if is_historical_request:
            self._reserve_request_rate_limit_slot(method, path, query)
        cached_response = self._historical_http_cache_get(method, path, version, query)
        if cached_response is not None:
            return cached_response
        allow_critical_bypass = bool(_critical_bypass or self._current_thread_request_critical_bypass())
        if auth and (not allow_critical_bypass) and self._should_defer_non_critical_request(method, path):
            normalized_path = str(path).split("?", 1)[0]
            remaining_sec = self._critical_trade_defer_remaining_sec()
            raise BrokerError(
                "IG non-critical REST request deferred: "
                f"critical_trade_operation_active method={str(method).upper()} path={normalized_path} "
                f"({remaining_sec:.1f}s remaining)"
            )
        if not is_historical_request:
            self._reserve_request_rate_limit_slot(method, path, query)

        base = self._endpoint + path
        if query:
            query_text = parse.urlencode(
                {key: str(value) for key, value in query.items() if value is not None}
            )
            if query_text:
                base = f"{base}?{query_text}"

        headers = self._auth_headers(include_session_tokens=auth)
        headers["VERSION"] = str(version)
        if extra_headers:
            headers.update(extra_headers)

        data_bytes = None
        if payload is not None:
            data_bytes = json.dumps(payload).encode("utf-8")

        req = request.Request(
            url=base,
            method=method.upper(),
            data=data_bytes,
            headers=headers,
        )

        try:
            with request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace") if resp.length != 0 else ""
                body: dict[str, Any]
                if not raw:
                    body = {}
                else:
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        parsed = {}
                    if isinstance(parsed, dict):
                        body = parsed
                    elif parsed is None:
                        body = {}
                    else:
                        body = {"data": parsed}
                response_headers = {k: v for k, v in resp.headers.items()}
                self._historical_http_cache_put(
                    method,
                    path,
                    version,
                    query,
                    body,
                    response_headers,
                )
                http_status_raw = getattr(resp, "status", None)
                http_status: int | None = None
                if http_status_raw is not None:
                    try:
                        http_status = int(http_status_raw)
                    except (TypeError, ValueError):
                        http_status = None
                self._emit_position_update(
                    method=method,
                    path=path,
                    request_payload=payload,
                    query=query,
                    response_payload=body,
                    http_status=http_status,
                    success=True,
                    error_text=None,
                )
                return body, response_headers
        except error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            error_message = f"IG API {method.upper()} {path} failed: {exc.code} {exc.reason} {error_body}"
            normalized_path = str(path).split("?", 1)[0]
            error_payload: dict[str, Any] | None = None
            if error_body.strip():
                try:
                    parsed_error_payload = json.loads(error_body)
                except Exception:
                    parsed_error_payload = None
                if isinstance(parsed_error_payload, dict):
                    error_payload = parsed_error_payload
            self._emit_position_update(
                method=method,
                path=path,
                request_payload=payload,
                query=query,
                response_payload=error_payload,
                http_status=int(exc.code),
                success=False,
                error_text=error_message,
            )
            if self._is_allowance_error_text(error_message):
                self._maybe_start_allowance_cooldown(
                    error_message,
                    f"{method.upper()} {normalized_path}",
                )
            if (
                auth
                and _retry_on_auth_failure
                and self._is_auth_token_invalid_error_text(error_message)
                and self._maybe_refresh_auth_session_after_failure(method=method, path=path, error_text=error_message)
            ):
                return self._request_direct(
                    method,
                    path,
                    payload=payload,
                    version=version,
                    auth=auth,
                    extra_headers=extra_headers,
                    query=query,
                    _retry_on_auth_failure=False,
                    _retry_on_transient_http=_retry_on_transient_http,
                    _retry_on_network_error=_retry_on_network_error,
                )
            # Retry once on transient 5xx responses. Do not retry 429/allowance
            # failures here; repeating the same request only burns more quota.
            if int(exc.code) in _RETRYABLE_HTTP_STATUS_CODES and _retry_on_transient_http:
                retry_delay = min(2.0, max(0.5, self.timeout_sec * 0.1))
                logger.warning(
                    "IG API transient HTTP %d on %s %s, retrying in %.1fs",
                    exc.code, method.upper(), path, retry_delay,
                )
                time.sleep(retry_delay)
                return self._request_direct(
                    method,
                    path,
                    payload=payload,
                    version=version,
                    auth=auth,
                    extra_headers=extra_headers,
                    query=query,
                    _retry_on_auth_failure=_retry_on_auth_failure,
                    _retry_on_transient_http=False,
                    _retry_on_network_error=_retry_on_network_error,
                    _critical_bypass=_critical_bypass,
                )
            raise BrokerError(
                error_message
            ) from exc
        except error.URLError as exc:
            error_message = f"IG API {method.upper()} {path} failed: {exc.reason}"
            self._emit_position_update(
                method=method,
                path=path,
                request_payload=payload,
                query=query,
                response_payload=None,
                http_status=None,
                success=False,
                error_text=error_message,
            )
            # Retry once on network errors (connection reset, timeout, etc.)
            if _retry_on_network_error:
                retry_delay = min(2.0, max(0.5, self.timeout_sec * 0.1))
                logger.warning(
                    "IG API network error on %s %s: %s, retrying in %.1fs",
                    method.upper(), path, exc.reason, retry_delay,
                )
                time.sleep(retry_delay)
                return self._request_direct(
                    method,
                    path,
                    payload=payload,
                    version=version,
                    auth=auth,
                    extra_headers=extra_headers,
                    query=query,
                    _retry_on_auth_failure=_retry_on_auth_failure,
                    _retry_on_transient_http=_retry_on_transient_http,
                    _retry_on_network_error=False,
                    _critical_bypass=_critical_bypass,
                )
            raise BrokerError(error_message) from exc

    @staticmethod
    def _is_auth_token_invalid_error_text(text: str) -> bool:
        lowered = str(text or "").lower()
        markers = (
            "error.security.client-token-invalid",
            "error.security.client-token-missing",
            "error.security.account-token-invalid",
            "error.security.account-token-missing",
            "error.public-api.failure.missing.credentials",
        )
        return any(marker in lowered for marker in markers)

    @staticmethod
    def _is_retryable_close_request_error_text(text: str) -> bool:
        lowered = str(text or "").lower()
        markers = (
            # IG gateway failed to parse DELETE body.
            "validation.null-not-allowed.request",
            # POST /positions/otc was interpreted as open position (method override not applied).
            "error.service.marketdata.position.notional.details.null.error",
            "error.service.create.otc.position.instrument.invalid",
            "error.service.marketdata.position.details.null.error",
        )
        return any(marker in lowered for marker in markers)

    def _maybe_refresh_auth_session_after_failure(
        self,
        *,
        method: str,
        path: str,
        error_text: str,
    ) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        if normalized_path.startswith("/session"):
            return False

        acquired = self._auth_refresh_lock.acquire(timeout=max(1.0, self.timeout_sec))
        if not acquired:
            return False
        try:
            now_ts = time.time()
            # Avoid reconnect storm across worker threads.
            if (now_ts - self._auth_refresh_last_attempt_ts) < self._auth_refresh_min_interval_sec:
                return (now_ts - self._auth_refresh_last_success_ts) < self._auth_refresh_min_interval_sec
            self._auth_refresh_last_attempt_ts = now_ts
            logger.warning(
                "IG auth tokens invalid during %s %s, attempting session refresh",
                str(method).upper(),
                normalized_path,
            )
            original_connect_retry_attempts = self.connect_retry_attempts
            try:
                # Keep auth recovery bounded to a single login cycle; repeated
                # POST /session retries under load tend to amplify allowance storms.
                self.connect_retry_attempts = 1
                self.connect()
            except Exception as refresh_exc:
                logger.warning(
                    "IG session refresh failed on %s %s: %s | original_error=%s",
                    str(method).upper(),
                    normalized_path,
                    refresh_exc,
                    error_text,
                )
                return False
            finally:
                self.connect_retry_attempts = original_connect_retry_attempts
            self._auth_refresh_last_success_ts = time.time()
            return True
        finally:
            self._auth_refresh_lock.release()

    def _maybe_reconnect_after_disconnected(self, *, method: str, path: str) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        if normalized_path.startswith("/session"):
            return False

        if not self._runtime_reconnect_lock.acquire(blocking=False):
            return self._is_connected()
        try:
            if self._is_connected():
                return True

            now_ts = time.time()
            if (now_ts - self._runtime_reconnect_last_attempt_ts) < self._runtime_reconnect_min_interval_sec:
                return False
            self._runtime_reconnect_last_attempt_ts = now_ts
            logger.warning(
                "IG request while disconnected during %s %s, attempting runtime reconnect",
                str(method).upper(),
                normalized_path,
            )
            try:
                self.connect()
            except Exception as reconnect_exc:
                logger.warning(
                    "IG runtime reconnect failed after disconnected request on %s %s: %s",
                    str(method).upper(),
                    normalized_path,
                    reconnect_exc,
                )
                return False
            return self._is_connected()
        finally:
            self._runtime_reconnect_lock.release()

    def _is_connected(self) -> bool:
        with self._lock:
            return bool(self._connected)

    def _epic_for_symbol(self, symbol: str) -> str:
        upper = symbol.upper().strip()
        epic = self._epics.get(upper)
        if epic:
            return epic
        if upper.startswith(("CS.", "IX.", "CC.")):
            return upper
        raise BrokerError(
            f"No IG epic mapping for symbol {symbol}. "
            "Set ig_symbol_epics in config or IG_SYMBOL_EPICS env (JSON object)."
        )

    @staticmethod
    def _is_epic_unavailable_error_text(text: str) -> bool:
        return "instrument.epic.unavailable" in str(text).lower()

    @staticmethod
    def _is_instrument_invalid_error_text(text: str) -> bool:
        lowered = str(text).lower()
        return any(
            marker in lowered
            for marker in (
                "error.service.create.otc.position.instrument.invalid",
                "instrument.invalid",
                "instrument_not_valid",
                "instrument not valid",
                "invalid_instrument",
                "invalid instrument",
            )
        )

    @staticmethod
    def _is_stream_subscription_invalid_group_error_text(text: str | None) -> bool:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return False
        return "invalid group" in lowered or lowered.startswith("error:21:")

    def _promote_stream_subscription_epic_after_invalid_group(
        self,
        symbol: str,
        failed_epic: str,
        *,
        reason: str,
    ) -> str | None:
        upper_symbol = str(symbol).upper().strip()
        normalized_failed = str(failed_epic).upper().strip()
        if not upper_symbol or not normalized_failed:
            return None

        self._mark_epic_temporarily_invalid(upper_symbol, normalized_failed, reason=reason)

        current_ts = time.time()
        for candidate in self._epic_attempt_order(upper_symbol):
            if candidate == normalized_failed:
                continue
            if self._is_epic_temporarily_invalid(upper_symbol, candidate, now_ts=current_ts):
                continue
            self._activate_epic(upper_symbol, candidate, log_warning=False)
            self._clear_stream_rest_fallback_block(upper_symbol)
            self._schedule_stream_subscription_retry(upper_symbol)
            logger.warning(
                "IG stream epic failover queued for %s: %s -> %s (retry_in=%.1fs %s)",
                upper_symbol,
                normalized_failed,
                candidate,
                self._stream_subscription_retry_gap_sec,
                reason,
            )
            return candidate
        self._mark_stream_rest_fallback_block(upper_symbol, reason=reason)
        return None

    def _stream_subscription_retry_remaining(self, symbol: str, *, now_ts: float | None = None) -> float:
        upper = str(symbol).upper().strip()
        if not upper:
            return 0.0
        current_ts = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            until_ts = float(self._stream_subscription_retry_not_before_ts_by_symbol.get(upper, 0.0))
            remaining = until_ts - current_ts
            if remaining > 0:
                return remaining
            self._stream_subscription_retry_not_before_ts_by_symbol.pop(upper, None)
            return 0.0

    def _schedule_stream_subscription_retry(self, symbol: str, delay_sec: float | None = None) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        normalized_delay = self._stream_subscription_retry_gap_sec if delay_sec is None else float(delay_sec)
        if not math.isfinite(normalized_delay):
            normalized_delay = self._stream_subscription_retry_gap_sec
        normalized_delay = max(0.0, normalized_delay)
        with self._lock:
            self._stream_subscription_retry_not_before_ts_by_symbol[upper] = max(
                float(self._stream_subscription_retry_not_before_ts_by_symbol.get(upper, 0.0)),
                time.time() + normalized_delay,
            )

    def _clear_stream_subscription_retry(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        with self._lock:
            self._stream_subscription_retry_not_before_ts_by_symbol.pop(upper, None)

    def _stream_rest_fallback_block_remaining(self, symbol: str, *, now_ts: float | None = None) -> float:
        upper = str(symbol).upper().strip()
        if not upper:
            return 0.0
        current_ts = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            until_ts = float(self._stream_rest_fallback_block_until_ts_by_symbol.get(upper, 0.0))
            remaining = until_ts - current_ts
            if remaining > 0:
                return remaining
            self._stream_rest_fallback_block_until_ts_by_symbol.pop(upper, None)
            self._stream_rest_fallback_block_reason_by_symbol.pop(upper, None)
            return 0.0

    def _stream_rest_fallback_block_reason(self, symbol: str) -> str | None:
        upper = str(symbol).upper().strip()
        if not upper:
            return None
        with self._lock:
            reason = str(self._stream_rest_fallback_block_reason_by_symbol.get(upper, "")).strip()
        return reason or None

    def _mark_stream_rest_fallback_block(self, symbol: str, *, reason: str | None = None) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        cooldown_sec = max(30.0, float(self._invalid_epic_retry_sec))
        until_ts = time.time() + cooldown_sec
        with self._lock:
            self._stream_rest_fallback_block_until_ts_by_symbol[upper] = max(
                float(self._stream_rest_fallback_block_until_ts_by_symbol.get(upper, 0.0)),
                until_ts,
            )
            normalized_reason = str(reason or "").strip()
            if normalized_reason:
                self._stream_rest_fallback_block_reason_by_symbol[upper] = normalized_reason

    def _clear_stream_rest_fallback_block(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)

    def _clear_stream_rest_fallback_block_locked(self, upper: str) -> None:
        self._stream_rest_fallback_block_until_ts_by_symbol.pop(upper, None)
        self._stream_rest_fallback_block_reason_by_symbol.pop(upper, None)

    def _is_epic_temporarily_invalid(
        self,
        symbol: str,
        epic: str,
        *,
        now_ts: float | None = None,
    ) -> bool:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        if not upper_symbol or not upper_epic:
            return False
        current_ts = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            symbol_invalid = self._invalid_epic_until_ts_by_symbol.get(upper_symbol)
            if not symbol_invalid:
                return False
            until_ts = float(symbol_invalid.get(upper_epic, 0.0))
            if until_ts > current_ts:
                return True
            symbol_invalid.pop(upper_epic, None)
            if not symbol_invalid:
                self._invalid_epic_until_ts_by_symbol.pop(upper_symbol, None)
            return False

    def _mark_epic_temporarily_invalid(self, symbol: str, epic: str, reason: str | None = None) -> None:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        if not upper_symbol or not upper_epic:
            return
        cooldown_sec = max(30.0, float(self._invalid_epic_retry_sec))
        until_ts = time.time() + cooldown_sec
        normalized_reason = str(reason or "n/a").strip() or "n/a"
        should_log = False
        with self._lock:
            symbol_invalid = self._invalid_epic_until_ts_by_symbol.setdefault(upper_symbol, {})
            symbol_invalid[upper_epic] = max(float(symbol_invalid.get(upper_epic, 0.0)), until_ts)
            warn_key = (upper_symbol, upper_epic, normalized_reason)
            last_warn_ts = float(self._invalid_epic_warn_last_ts_by_key.get(warn_key, 0.0))
            warn_interval_sec = max(300.0, min(cooldown_sec, 900.0))
            if (time.time() - last_warn_ts) >= warn_interval_sec:
                self._invalid_epic_warn_last_ts_by_key[warn_key] = time.time()
                should_log = True
        if should_log:
            logger.warning(
                "IG epic marked temporarily invalid for %s: %s (cooldown=%.0fs reason=%s)",
                upper_symbol,
                upper_epic,
                cooldown_sec,
                normalized_reason,
            )

    def _clear_epic_temporarily_invalid(self, symbol: str, epic: str) -> None:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        if not upper_symbol or not upper_epic:
            return
        with self._lock:
            symbol_invalid = self._invalid_epic_until_ts_by_symbol.get(upper_symbol)
            if not symbol_invalid:
                return
            symbol_invalid.pop(upper_epic, None)
            if not symbol_invalid:
                self._invalid_epic_until_ts_by_symbol.pop(upper_symbol, None)

    def _epic_attempt_order(self, symbol: str) -> list[str]:
        upper = symbol.upper().strip()
        if upper.startswith(("CS.", "IX.", "CC.")):
            return [upper]

        with self._lock:
            active = str(self._epics.get(upper) or "").strip().upper()
            candidates = list(self._epic_candidates.get(upper, []))

        if not self._epic_failover_enabled:
            if active:
                return [active]
            first_candidate = _first_non_empty_text(tuple(candidates))
            if first_candidate is not None:
                return [first_candidate]
            defaults = DEFAULT_IG_EPIC_CANDIDATES.get(upper, [])
            first_default = _first_non_empty_text(tuple(defaults))
            if first_default is not None:
                return [first_default]
            return []

        defaults = DEFAULT_IG_EPIC_CANDIDATES.get(upper, [])
        candidates.extend(str(epic).strip().upper() for epic in defaults)
        if active:
            candidates.insert(0, active)
        ordered = _dedupe_epics(candidates)
        plausible = [epic for epic in ordered if _is_plausible_epic_for_symbol(upper, epic)]
        now_ts = time.time()
        filtered = [epic for epic in plausible if not self._is_epic_temporarily_invalid(upper, epic, now_ts=now_ts)]
        return filtered or plausible

    def _wait_for_market_rest_slot(self) -> None:
        interval = _as_float(self.rest_market_min_interval_sec, 0.0)
        if not math.isfinite(interval) or interval <= 0:
            return
        interval = max(0.0, interval)
        if interval <= 0:
            return
        while True:
            with self._lock:
                now_mono = time.monotonic()
                now_wall = time.time()
                if self._last_market_rest_request_monotonic <= 0:
                    self._last_market_rest_request_monotonic = now_mono
                    self._last_market_rest_request_ts = now_wall
                    return
                elapsed = now_mono - self._last_market_rest_request_monotonic
                wait_sec = interval - elapsed
                if not math.isfinite(wait_sec):
                    wait_sec = 0.0
                if wait_sec <= 0:
                    self._last_market_rest_request_monotonic = now_mono
                    self._last_market_rest_request_ts = now_wall
                    return
            time.sleep(min(wait_sec, 0.2))

    def _epic_unavailable_retry_remaining(self, symbol: str) -> float:
        upper = str(symbol).upper().strip()
        if not upper:
            return 0.0
        with self._lock:
            until_ts = float(self._epic_unavailable_until_ts_by_symbol.get(upper, 0.0))
        remaining = until_ts - time.time()
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def _mark_epic_unavailable_retry(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        retry_sec = max(5.0, float(self._epic_unavailable_retry_sec))
        with self._lock:
            self._epic_unavailable_until_ts_by_symbol[upper] = max(
                float(self._epic_unavailable_until_ts_by_symbol.get(upper, 0.0)),
                time.time() + retry_sec,
            )

    def _clear_epic_unavailable_retry(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        with self._lock:
            self._epic_unavailable_until_ts_by_symbol.pop(upper, None)

    @staticmethod
    def _extract_search_item_epic(item: dict[str, Any]) -> str | None:
        direct = str(item.get("epic") or "").strip().upper()
        if direct:
            return direct
        instrument = _as_mapping(item.get("instrument"))
        nested = str(instrument.get("epic") or "").strip().upper()
        if nested:
            return nested
        return None

    @staticmethod
    def _epic_search_score(symbol: str, epic: str) -> int:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        score = 0

        is_index = _is_index_symbol(upper_symbol)
        if is_index and upper_epic.startswith("IX."):
            score += 30
        elif is_index and not upper_epic.startswith("IX."):
            score -= 20
        elif (not is_index) and upper_epic.startswith("CS."):
            score += 20

        if upper_symbol in upper_epic:
            score += 20

        if upper_symbol in {"GOLD", "XAUUSD"} and ("GOLD" in upper_epic or "XAU" in upper_epic):
            score += 30
        if upper_symbol == "BRENT" and ("BRENT" in upper_epic or "LCO" in upper_epic):
            score += 30
        if upper_symbol == "WTI" and any(token in upper_epic for token in ("WTI", "CL", "CRUDE", "OIL")):
            score += 30
        commodity_hints = COMMODITY_EPIC_HINTS.get(upper_symbol)
        if commodity_hints is not None and any(token in upper_epic for token in commodity_hints):
            score += 30
        crypto_base = _crypto_symbol_base(upper_symbol)
        if crypto_base is not None and (
            upper_symbol in upper_epic
            or crypto_base + "USD" in upper_epic
            or (crypto_base == "DOGE" and "DOGUSD" in upper_epic)
            or (crypto_base == "BTC" and "BITCOIN" in upper_epic)
            or (crypto_base == "ETH" and "ETHEREUM" in upper_epic)
            or (crypto_base == "LTC" and "LITECOIN" in upper_epic)
            or (crypto_base == "SOL" and "SOLANA" in upper_epic)
            or (crypto_base == "XRP" and "RIPPLE" in upper_epic)
            or (crypto_base == "DOGE" and "DOGECOIN" in upper_epic)
        ):
            score += 30
        if upper_symbol in {"AAPL", "MSFT"} and upper_epic.startswith(("UA.", "SA.")):
            score += 30

        if upper_epic.endswith(".IP"):
            score += 1
        return score

    def _search_terms_for_symbol(self, symbol: str) -> list[str]:
        upper = str(symbol).upper().strip()
        terms: list[str] = []
        terms.extend(IG_EPIC_SEARCH_TERMS.get(upper, []))
        terms.append(upper)

        if upper == "GOLD":
            terms.extend(["XAUUSD", "XAU/USD"])
        elif upper == "XAUUSD":
            terms.extend(["GOLD", "XAU/USD"])

        if _is_fx_pair_symbol(upper):
            terms.append(f"{upper[:3]}/{upper[3:]}")

        seen: set[str] = set()
        unique: list[str] = []
        for term in terms:
            text = str(term).strip()
            if not text:
                continue
            key = text.upper()
            if key in seen:
                continue
            seen.add(key)
            unique.append(text)
        return unique

    def _discover_epics_via_search(self, symbol: str) -> list[str]:
        upper = symbol.upper().strip()
        discovered: list[str] = []

        for term in self._search_terms_for_symbol(upper):
            try:
                self._wait_for_market_rest_slot()
                body, _ = self._request(
                    "GET",
                    "/markets",
                    version="1",
                    auth=True,
                    query={"searchTerm": term},
                )
            except BrokerError as exc:
                text = str(exc)
                if self._is_allowance_error_text(text):
                    raise
                continue

            markets = body.get("markets")
            if not isinstance(markets, list):
                continue

            for item in markets:
                if not isinstance(item, dict):
                    continue
                epic = self._extract_search_item_epic(item)
                if epic and _is_plausible_epic_for_symbol(upper, epic):
                    discovered.append(epic)

        deduped = _dedupe_epics(discovered)
        deduped.sort(key=lambda epic: self._epic_search_score(upper, epic), reverse=True)
        return deduped

    def _extend_epic_candidates_from_search(self, symbol: str) -> list[str]:
        if not self._epic_failover_enabled:
            return []
        if not self._epic_search_enabled:
            return []
        upper = symbol.upper().strip()
        search_candidates = self._discover_epics_via_search(upper)
        if not search_candidates:
            return []

        with self._lock:
            existing = self._epic_candidates.get(upper, [])
            merged = _dedupe_epics([*existing, *search_candidates])
            self._epic_candidates[upper] = merged
            active = str(self._epics.get(upper) or "").strip().upper()

        ordered = list(search_candidates)
        if active:
            ordered = [epic for epic in ordered if epic != active]
        return ordered

    def _activate_epic(self, symbol: str, epic: str, *, log_warning: bool = True) -> None:
        upper = symbol.upper().strip()
        normalized = str(epic).strip().upper()
        if not normalized:
            return
        if not _is_plausible_epic_for_symbol(upper, normalized):
            logger.warning(
                "Ignoring IG epic remap for %s to non-matching epic=%s",
                upper,
                normalized,
            )
            return

        with self._lock:
            previous = self._epics.get(upper)
            self._epics[upper] = normalized
            existing = self._epic_candidates.get(upper, [])
            self._epic_candidates[upper] = _dedupe_epics([normalized, *existing])
            symbol_invalid = self._invalid_epic_until_ts_by_symbol.get(upper)
            if symbol_invalid:
                symbol_invalid.pop(normalized, None)
                if not symbol_invalid:
                    self._invalid_epic_until_ts_by_symbol.pop(upper, None)
            epic_changed = bool(previous and previous != normalized)
            self._stream_subscription_retry_not_before_ts_by_symbol.pop(upper, None)
            if epic_changed:
                self._symbol_spec_cache.pop(upper, None)

                # Force re-subscribe only when epic mapping actually changed.
                table_id = self._stream_symbol_to_table.pop(upper, None)
                if table_id is not None:
                    self._stream_table_to_symbol.pop(table_id, None)
                    self._stream_table_field_values.pop(table_id, None)

        if log_warning and previous and previous != normalized:
            should_log = False
            current_ts = time.time()
            with self._lock:
                signature = (str(previous).strip().upper(), normalized)
                prior_signature = self._epic_remap_warn_last_signature_by_symbol.get(upper)
                prior_warn_ts = float(self._epic_remap_warn_last_ts_by_symbol.get(upper, 0.0))
                if signature != prior_signature or (current_ts - prior_warn_ts) >= 300.0:
                    self._epic_remap_warn_last_signature_by_symbol[upper] = signature
                    self._epic_remap_warn_last_ts_by_symbol[upper] = current_ts
                    should_log = True
            if should_log:
                logger.warning("IG epic remapped for %s: %s -> %s", upper, previous, normalized)

    def _request_market_details_with_epic_failover(self, symbol: str) -> tuple[str, dict[str, Any]]:
        upper = symbol.upper().strip()
        retry_remaining = self._epic_unavailable_retry_remaining(upper)
        if retry_remaining > 0:
            raise BrokerError(
                "IG market epic retry cooldown is active for "
                f"{upper} ({retry_remaining:.1f}s remaining) | "
                '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )

        attempts = self._epic_attempt_order(upper)
        first_epic = attempts[0] if attempts else None
        last_epic_unavailable: BrokerError | None = None

        for idx, epic in enumerate(attempts):
            try:
                if idx > 0:
                    self._wait_for_market_rest_slot()
                body, _ = self._request("GET", f"/markets/{epic}", version="3", auth=True)
                self._activate_epic(upper, epic)
                self._clear_epic_unavailable_retry(upper)
                if first_epic and epic != first_epic:
                    logger.warning(
                        "IG epic failover resolved for %s: using %s instead of %s",
                        upper,
                        epic,
                        first_epic,
                    )
                return epic, body
            except BrokerError as exc:
                if self._is_epic_unavailable_error_text(str(exc)):
                    last_epic_unavailable = exc
                    continue
                raise

        dynamic_attempts: list[str] = []
        if self._epic_failover_enabled and (last_epic_unavailable is not None or not attempts):
            dynamic_attempts = [
                epic for epic in self._extend_epic_candidates_from_search(upper) if epic not in attempts
            ]

        for epic in dynamic_attempts:
            try:
                self._wait_for_market_rest_slot()
                body, _ = self._request("GET", f"/markets/{epic}", version="3", auth=True)
                self._activate_epic(upper, epic)
                self._clear_epic_unavailable_retry(upper)
                logger.warning(
                    "IG epic auto-discovered for %s via search: using %s",
                    upper,
                    epic,
                )
                return epic, body
            except BrokerError as exc:
                if self._is_epic_unavailable_error_text(str(exc)):
                    last_epic_unavailable = exc
                    continue
                raise

        if last_epic_unavailable is not None:
            self._mark_epic_unavailable_retry(upper)
            raise last_epic_unavailable
        raise BrokerError(f"No IG epic mapping candidates for symbol {upper}")

    @staticmethod
    def _select_account_from_list(accounts: list[dict[str, Any]], account_id: str | None) -> dict[str, Any]:
        selected: dict[str, Any] | None = None
        if account_id:
            for account in accounts:
                item = _as_mapping(account)
                if str(item.get("accountId")) == account_id:
                    selected = item
                    break
        if selected is None:
            preferred = next(
                (
                    _as_mapping(acc)
                    for acc in accounts
                    if isinstance(acc, dict) and bool(acc.get("preferred"))
                ),
                None,
            )
            selected = preferred if preferred is not None else _as_mapping(accounts[0] if accounts else {})
        return selected

    @staticmethod
    def _extract_account_currency(selected: dict[str, Any]) -> str | None:
        for key in ("currency", "currencyIsoCode", "currencyCode"):
            value = _normalize_currency_code(selected.get(key))
            if value is not None:
                return value
        balance = _as_mapping(selected.get("balance"))
        for key in ("currency", "currencyIsoCode", "currencyCode"):
            value = _normalize_currency_code(balance.get(key))
            if value is not None:
                return value
        return None

    @staticmethod
    def _extract_deal_currencies(instrument: dict[str, Any], body: dict[str, Any]) -> tuple[list[str], str | None]:
        currencies_raw = instrument.get("currencies")
        if not isinstance(currencies_raw, list):
            currencies_raw = body.get("currencies")
        currencies = currencies_raw if isinstance(currencies_raw, list) else []

        codes: list[str] = []
        default_code: str | None = None
        for item in currencies:
            mapping = _as_mapping(item)
            code = _normalize_currency_code(
                mapping.get("code")
                or mapping.get("currency")
                or mapping.get("currencyCode")
                or mapping.get("isoCode")
            )
            if code is None:
                continue
            if code not in codes:
                codes.append(code)
            if default_code is None and bool(mapping.get("isDefault")):
                default_code = code

        if default_code is None:
            for key in ("currency", "currencyCode", "dealCurrency", "depositBandCurrency"):
                candidate = _normalize_currency_code(instrument.get(key) or body.get(key))
                if candidate is not None:
                    default_code = candidate
                    if candidate not in codes:
                        codes.insert(0, candidate)
                    break

        return codes, default_code

    @staticmethod
    def _default_lot_min_fallback(symbol: str, epic: str) -> tuple[float, str]:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        if _is_fx_pair_symbol(upper_symbol) and upper_epic.startswith("CS.D."):
            return 0.5, "fallback_fx_0.5"
        if _is_index_symbol(upper_symbol, upper_epic):
            return 0.1, "fallback_index_0.1"
        if upper_epic.startswith(("UA.", "SA.")):
            return 1.0, "fallback_share_1.0"
        return 0.01, "fallback_default_0.01"

    def _get_account_currency_code(self) -> str | None:
        with self._lock:
            cached = str(self._account_currency_code or "").strip().upper()
        if cached:
            return cached

        with self._account_currency_code_lock:
            with self._lock:
                cached = str(self._account_currency_code or "").strip().upper()
            if cached:
                return cached

            try:
                body, _ = self._request("GET", "/accounts", version="1", auth=True)
            except BrokerError:
                return None

            accounts_raw = body.get("accounts")
            accounts = accounts_raw if isinstance(accounts_raw, list) else []
            if not accounts:
                return None

            selected = self._select_account_from_list([_as_mapping(acc) for acc in accounts], self.account_id)
            currency = self._extract_account_currency(selected)
            if currency:
                with self._lock:
                    self._account_currency_code = currency
            return currency

    def get_account_currency_code(self) -> str | None:
        return self._get_account_currency_code()

    def _order_currency_attempts(
        self,
        symbol: str,
        spec: SymbolSpec | None,
        *,
        allow_account_refresh: bool = True,
    ) -> list[str]:
        if allow_account_refresh:
            account_currency = self._get_account_currency_code()
        else:
            with self._lock:
                account_currency = _normalize_currency_code(self._account_currency_code)
        if spec is None:
            attempts = [account_currency] if account_currency else []
            if not account_currency:
                attempts.extend(["USD", "EUR"])
            return _dedupe_epics([code for code in attempts if code])

        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        raw_currencies = metadata.get("deal_currencies")
        deal_currencies = (
            [str(item).strip().upper() for item in raw_currencies if str(item).strip()]
            if isinstance(raw_currencies, list)
            else []
        )
        default_currency = str(metadata.get("default_currency_code") or "").strip().upper()
        default_currency = _normalize_currency_code(default_currency)
        deal_currencies = [code for code in deal_currencies if _normalize_currency_code(code) is not None]

        attempts: list[str] = []
        if deal_currencies:
            # IG restricts currencyCode to instrument currencies. Avoid guaranteed rejects
            # by preferring only explicit market currencies when they are known.
            if account_currency and account_currency in deal_currencies:
                attempts.append(account_currency)
            if default_currency and default_currency in deal_currencies:
                attempts.append(default_currency)
            attempts.extend(deal_currencies)
            return _dedupe_epics([code for code in attempts if code])

        if account_currency:
            attempts.append(account_currency)
        if default_currency:
            attempts.append(default_currency)
        if not attempts:
            attempts.extend(["USD", "EUR"])
        # If market metadata has no currencies and account currency is single-source,
        # probe one major alternative for robustness.
        if len(attempts) == 1 and not default_currency:
            if attempts[0] == "EUR":
                attempts.append("USD")
            elif attempts[0] == "USD":
                attempts.append("EUR")
        return _dedupe_epics([code for code in attempts if code])

    @staticmethod
    def _should_try_next_currency_on_open_reject(confirm: dict[str, Any]) -> bool:
        reason = IgApiClient._confirm_rejection_reason(confirm)
        return reason in {
            "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY",
            "UNKNOWN",
            "ERROR",
        }

    @staticmethod
    def _should_try_next_currency_on_open_post_error(error_text: str) -> bool:
        lowered = str(error_text).lower()
        return any(
            marker in lowered
            for marker in (
                "instrument_not_tradeable_in_this_currency",
                "not tradeable in this currency",
                "failed to retrieve price information for this currency",
            )
        )

    @staticmethod
    def _should_probe_positions_after_open_reject(confirm: dict[str, Any]) -> bool:
        reason = IgApiClient._confirm_rejection_reason(confirm)
        return reason in {
            "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY",
            "UNKNOWN",
            "ERROR",
        }

    @staticmethod
    def _is_minimum_order_size_reject(reason: str) -> bool:
        normalized = str(reason or "").strip().upper()
        return normalized in {
            "MINIMUM_ORDER_SIZE_ERROR",
            "MINIMUM_ORDER_SIZE",
            "ORDER_SIZE_INCREMENT_ERROR",
            "SIZE_INCREMENT",
            "ERROR.SERVICE.OTC.INVALID_SIZE",
            "ERROR.SERVICE.OTC.INVALID.SIZE",
        }

    @staticmethod
    def _is_minimum_order_size_error_text(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return any(
            marker in lowered
            for marker in (
                "minimum_order_size_error",
                "minimum order size",
                "size below minimum requirement",
                "order_size_increment_error",
                "size_increment",
                "order size must be traded in set increments",
                "set increments",
                "invalid_size",
                "invalid.size",
                "invalid size",
            )
        )

    @staticmethod
    def _infer_min_lot_after_reject(
        symbol: str,
        epic: str,
        *,
        attempted_volume: float,
        current_lot_min: float,
        lot_step: float,
    ) -> float:
        upper_symbol = str(symbol).strip().upper()
        upper_epic = str(epic).strip().upper()
        step = max(0.001, float(lot_step))
        attempted = max(0.0, float(attempted_volume))
        current_min = max(0.0, float(current_lot_min))

        # IG can reject with minimum-size constraints that are not exposed via minDealSize.
        # Raise the local minimum just above the rejected volume and snap to broker step.
        bump = max(1e-6, step * 0.05)
        required = max(current_min + bump, attempted + bump)
        required_steps = (Decimal(str(required)) / Decimal(str(step))).to_integral_value(rounding=ROUND_CEILING)
        stepped_required = float(required_steps * Decimal(str(step)))

        ladder: tuple[float, ...]
        if _is_fx_pair_symbol(upper_symbol) and upper_epic.startswith("CS.D."):
            ladder = (0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0)
        elif _is_index_symbol(upper_symbol, upper_epic):
            ladder = (0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0)
        elif upper_epic.startswith(("CC.", "UA.", "SA.")):
            ladder = (0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0)
        else:
            ladder = (0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0)

        inferred = stepped_required
        for candidate in ladder:
            if candidate + FLOAT_ROUNDING_TOLERANCE >= stepped_required:
                inferred = max(stepped_required, candidate)
                break
        precision = max(3, _precision_from_step(step))
        return round(max(step, inferred), precision)

    @staticmethod
    def _apply_adaptive_lot_min(
        spec: SymbolSpec,
        *,
        new_lot_min: float,
        reason: str,
        epic: str,
        attempted_volume: float,
    ) -> float:
        previous = float(spec.lot_min)
        updated = max(previous, float(new_lot_min))
        if updated <= previous + FLOAT_ROUNDING_TOLERANCE:
            return previous
        spec.lot_min = updated
        spec.lot_max = max(float(spec.lot_max), updated)
        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        metadata["lot_min_source"] = f"adaptive_reject_{str(reason).strip().lower()}"
        metadata["lot_min_update_reason"] = str(reason).strip().upper()
        metadata["lot_min_update_epic"] = str(epic).strip().upper()
        metadata["lot_min_update_attempted"] = float(attempted_volume)
        metadata["lot_min_update_ts"] = time.time()
        spec.metadata = metadata
        return updated

    def _promote_symbol_lot_min_from_market_spec(
        self,
        symbol: str,
        epic: str,
        candidate_spec: SymbolSpec,
        attempted_volume: float,
    ) -> float | None:
        upper_symbol = str(symbol).strip().upper()
        upper_epic = str(epic).strip().upper()
        candidate_min = max(0.0, float(candidate_spec.lot_min))
        if candidate_min <= 0:
            return None

        with self._lock:
            cached_spec = self._get_or_seed_cached_symbol_spec_for_epic_locked(
                upper_symbol,
                upper_epic,
                seed_spec=candidate_spec,
            )

            before = float(cached_spec.lot_min)
            if candidate_min <= before + FLOAT_ROUNDING_TOLERANCE:
                return None

            cached_spec.lot_min = candidate_min
            cached_spec.lot_max = max(float(cached_spec.lot_max), candidate_min)
            metadata = cached_spec.metadata if isinstance(cached_spec.metadata, dict) else {}
            candidate_source = ""
            if isinstance(candidate_spec.metadata, dict):
                candidate_source = str(candidate_spec.metadata.get("lot_min_source") or "").strip()
            metadata["lot_min_source"] = (
                f"market_spec_{candidate_source}" if candidate_source else "market_spec"
            )
            metadata["lot_min_update_reason"] = "MARKET_MIN_DEAL_SIZE"
            metadata["lot_min_update_epic"] = upper_epic
            metadata["lot_min_update_attempted"] = max(0.0, float(attempted_volume))
            metadata["lot_min_update_ts"] = time.time()
            cached_spec.metadata = metadata
            after = float(cached_spec.lot_min)

        logger.warning(
            "IG lot_min update from market spec for %s: %.6f -> %.6f (attempted=%.6f epic=%s)",
            upper_symbol,
            before,
            after,
            max(0.0, float(attempted_volume)),
            upper_epic,
        )
        return after

    @staticmethod
    def _trusted_cached_lot_min(spec: SymbolSpec | None) -> float | None:
        if spec is None:
            return None
        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        source = str(metadata.get("lot_min_source") or "").strip().lower()
        if not source.startswith("adaptive_reject_") and not source.startswith("market_spec_"):
            return None
        value = float(spec.lot_min)
        if not math.isfinite(value) or value <= 0:
            return None
        return value

    def _promote_symbol_lot_min_after_reject(
        self,
        symbol: str,
        epic: str,
        candidate_spec: SymbolSpec,
        attempted_volume: float,
        reason: str,
    ) -> float | None:
        if not self._is_minimum_order_size_reject(reason):
            return None

        upper_symbol = str(symbol).strip().upper()
        upper_epic = str(epic).strip().upper()
        attempted = max(0.0, float(attempted_volume))
        if attempted <= 0:
            return None
        reason_code = str(reason or "").strip().upper()

        if reason_code in {"ORDER_SIZE_INCREMENT_ERROR", "SIZE_INCREMENT"}:
            with self._lock:
                cached_spec = self._get_or_seed_cached_symbol_spec_for_epic_locked(
                    upper_symbol,
                    upper_epic,
                    seed_spec=candidate_spec,
                )

                candidate_before_step = float(candidate_spec.lot_step)
                cached_before_step = float(cached_spec.lot_step)
                promoted_step = max(0.001, candidate_before_step, cached_before_step)
                # On non-FX CFDs IG commonly rejects sub-minimum fractional increments.
                # Keep volume step at least as coarse as minimum deal size.
                if _is_non_fx_cfd_symbol(upper_symbol, upper_epic):
                    promoted_step = max(
                        promoted_step,
                        float(candidate_spec.lot_min),
                        float(cached_spec.lot_min),
                    )

                step_changed = False
                if promoted_step > candidate_before_step + FLOAT_ROUNDING_TOLERANCE:
                    candidate_spec.lot_step = promoted_step
                    candidate_spec.lot_precision = max(
                        int(candidate_spec.lot_precision),
                        _precision_from_step(promoted_step),
                    )
                    candidate_metadata = candidate_spec.metadata if isinstance(candidate_spec.metadata, dict) else {}
                    candidate_metadata["lot_step_source"] = "adaptive_reject_order_size_increment"
                    candidate_metadata["lot_step_update_reason"] = "ORDER_SIZE_INCREMENT_ERROR"
                    candidate_metadata["lot_step_update_epic"] = upper_epic
                    candidate_metadata["lot_step_update_attempted"] = attempted
                    candidate_metadata["lot_step_update_ts"] = time.time()
                    candidate_spec.metadata = candidate_metadata
                    step_changed = True

                if promoted_step > cached_before_step + FLOAT_ROUNDING_TOLERANCE:
                    cached_spec.lot_step = promoted_step
                    cached_spec.lot_precision = max(
                        int(cached_spec.lot_precision),
                        _precision_from_step(promoted_step),
                    )
                    cached_metadata = cached_spec.metadata if isinstance(cached_spec.metadata, dict) else {}
                    cached_metadata["lot_step_source"] = "adaptive_reject_order_size_increment"
                    cached_metadata["lot_step_update_reason"] = "ORDER_SIZE_INCREMENT_ERROR"
                    cached_metadata["lot_step_update_epic"] = upper_epic
                    cached_metadata["lot_step_update_attempted"] = attempted
                    cached_metadata["lot_step_update_ts"] = time.time()
                    cached_spec.metadata = cached_metadata
                    step_changed = True

            if not step_changed:
                return None

            logger.warning(
                "IG adaptive lot_step update for %s after increment reject: %.6f/%.6f -> %.6f (attempted=%.6f epic=%s)",
                upper_symbol,
                candidate_before_step,
                cached_before_step,
                promoted_step,
                attempted,
                upper_epic,
            )
            return max(float(candidate_spec.lot_min), float(cached_spec.lot_min))

        inferred = self._infer_min_lot_after_reject(
            upper_symbol,
            upper_epic,
            attempted_volume=attempted,
            current_lot_min=float(candidate_spec.lot_min),
            lot_step=float(candidate_spec.lot_step),
        )

        with self._lock:
            cached_spec = self._get_or_seed_cached_symbol_spec_for_epic_locked(
                upper_symbol,
                upper_epic,
                seed_spec=candidate_spec,
            )

            before = min(float(candidate_spec.lot_min), float(cached_spec.lot_min))
            self._apply_adaptive_lot_min(
                candidate_spec,
                new_lot_min=inferred,
                reason=reason,
                epic=upper_epic,
                attempted_volume=attempted,
            )
            self._apply_adaptive_lot_min(
                cached_spec,
                new_lot_min=inferred,
                reason=reason,
                epic=upper_epic,
                attempted_volume=attempted,
            )
            after = max(float(candidate_spec.lot_min), float(cached_spec.lot_min))

        if after <= before + FLOAT_ROUNDING_TOLERANCE:
            return None

        logger.warning(
            "IG adaptive lot_min update for %s after reject %s: %.6f -> %.6f (attempted=%.6f epic=%s)",
            upper_symbol,
            str(reason).strip().upper() or "UNKNOWN",
            before,
            after,
            attempted,
            upper_epic,
        )
        return after

    def _wait_for_deal_confirm(self, deal_reference: str) -> dict[str, Any] | None:
        deadline = time.time() + max(1.0, self.confirm_timeout_sec)
        while time.time() < deadline:
            try:
                confirm, _ = self._request(
                    "GET",
                    f"/confirms/{deal_reference}",
                    version="1",
                    auth=True,
                )
            except BrokerError:
                time.sleep(0.3)
                continue

            status = str(confirm.get("dealStatus") or "").upper()
            if status in {"ACCEPTED", "REJECTED"}:
                return confirm
            time.sleep(0.3)
        return None

    @staticmethod
    def _compact_confirm_payload(confirm: dict[str, Any]) -> str:
        summary: dict[str, Any] = {}
        for key in (
            "dealStatus",
            "status",
            "reason",
            "statusReason",
            "rejectionReason",
            "errorCode",
            "message",
            "dealId",
            "dealReference",
            "epic",
            "direction",
            "size",
            "level",
            "stopLevel",
            "limitLevel",
            "profit",
            "profitCurrency",
            "date",
        ):
            value = confirm.get(key)
            if value in (None, "", [], {}):
                continue
            summary[key] = value
        if not summary:
            return "{}"
        return json.dumps(summary, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _confirm_rejection_reason(confirm: dict[str, Any]) -> str:
        extracted: list[str] = []
        fallback_reason = ""
        for key in ("reason", "statusReason", "rejectionReason", "errorCode", "status"):
            value = str(confirm.get(key) or "").strip().upper()
            if not value:
                continue
            extracted.append(value)
            if not fallback_reason:
                fallback_reason = value
            if value not in {"UNKNOWN", "ERROR"}:
                # Keep scanning for richer textual hints first, but remember the first explicit code.
                fallback_reason = value

        message = str(confirm.get("message") or "").strip().upper()
        if message:
            extracted.append(message)

        combined = " | ".join(extracted)
        if "INSTRUMENT NOT VALID" in combined or "INSTRUMENT.INVALID" in combined:
            return "INSTRUMENT_NOT_VALID"
        if (
            "NOT TRADEABLE IN THIS CURRENCY" in combined
            or "FAILED TO RETRIEVE PRICE INFORMATION FOR THIS CURRENCY" in combined
        ):
            return "INSTRUMENT_NOT_TRADEABLE_IN_THIS_CURRENCY"
        if (
            "ORDER SIZE MUST BE TRADED IN SET INCREMENTS" in combined
            or "SET INCREMENTS" in combined
            or "SIZE_INCREMENT" in combined
            or "INVALID_SIZE" in combined
            or "INVALID.SIZE" in combined
            or "INVALID SIZE" in combined
        ):
            return "ORDER_SIZE_INCREMENT_ERROR"
        if "MINIMUM ORDER SIZE" in combined or "SIZE BELOW MINIMUM REQUIREMENT" in combined:
            return "MINIMUM_ORDER_SIZE_ERROR"
        if "ATTACHED ORDER LEVEL" in combined or "STOP TOO CLOSE" in combined:
            return "ATTACHED_ORDER_LEVEL_ERROR"
        if "MARKET NOT AVAILABLE" in combined:
            return "MARKET_NOT_AVAILABLE"

        for value in extracted:
            if value not in {"UNKNOWN", "ERROR"}:
                return value
        if fallback_reason:
            return fallback_reason
        return "UNKNOWN"

    def _should_try_next_epic_on_open_reject(self, symbol: str, epic: str, confirm: dict[str, Any]) -> bool:
        upper_symbol = str(symbol).strip().upper()
        upper_epic = str(epic).strip().upper()
        reason = self._confirm_rejection_reason(confirm)
        if reason in {
            "INSTRUMENT_NOT_VALID",
            "INSTRUMENT NOT VALID",
            "INVALID_INSTRUMENT",
            "INVALID_MARKET",
            "MARKET_NOT_AVAILABLE",
            "MARKET_NOT_ALLOWED",
            "ATTACHED_ORDER_LEVEL_ERROR",
            "GUARANTEED_STOP_REQUIRED",
            "CONTROLLED_RISK_REQUIRED",
            "LIMITED_RISK_REQUIRED",
        }:
            return True
        if _is_non_fx_cfd_symbol(upper_symbol, upper_epic) and reason in {"UNKNOWN", "ERROR"}:
            return True
        return False

    @staticmethod
    def _should_try_next_epic_on_open_post_error(symbol: str, epic: str, error_text: str) -> bool:
        upper_symbol = str(symbol).strip().upper()
        upper_epic = str(epic).strip().upper()
        lowered = str(error_text).lower()
        if any(
            marker in lowered
            for marker in (
                "error.service.create.otc.position.instrument.invalid",
                "instrument.invalid",
                "invalid_instrument",
                "instrument not valid",
                "attached_order_level_error",
                "guaranteed_stop",
                "controlled_risk",
                "limited_risk",
            )
        ):
            return True
        if _is_non_fx_cfd_symbol(upper_symbol, upper_epic) and "400 bad request" in lowered:
            return True
        return False

    def _build_symbol_spec_from_market_details(
        self,
        symbol: str,
        epic: str,
        body: dict[str, Any],
        *,
        cache_symbol: bool,
    ) -> SymbolSpec:
        upper = symbol.upper()
        instrument = _as_mapping(body.get("instrument"))
        dealing_rules = _as_mapping(body.get("dealingRules"))
        snapshot = _as_mapping(body.get("snapshot"))

        decimal_places_raw = snapshot.get("decimalPlacesFactor")
        try:
            decimal_places = int(decimal_places_raw) if decimal_places_raw is not None else 4
        except (TypeError, ValueError):
            decimal_places = 4
        decimal_places = max(0, decimal_places)
        quote_step = float(10 ** (-decimal_places))
        precision = max(0, decimal_places)
        scaling_factor = _as_float(instrument.get("scalingFactor"), 1.0)
        if scaling_factor <= 0:
            scaling_factor = 1.0
        pip_position = instrument.get("pipPosition")

        one_pip_means = _as_float(instrument.get("onePipMeans"), 0.0)
        pip_size = 0.0
        pip_size_source = "fallback_decimal_places"
        if one_pip_means > 0:
            pip_size = one_pip_means
            pip_size_source = "onePipMeans"
            # IG can return onePipMeans in normalized scale (e.g. 0.0001) while
            # snapshot prices are scaled (e.g. 11696.2). Bring pip to quote scale.
            if pip_size < quote_step and scaling_factor > 1.0:
                pip_size *= scaling_factor
                pip_size_source = "onePipMeans_scaled_by_scalingFactor"
        if pip_size <= 0:
            pip_decimals = max(0, decimal_places - 1)
            pip_size = float(10 ** (-pip_decimals))
        pip_size = max(pip_size, quote_step, FLOAT_COMPARISON_TOLERANCE)
        # Some index epics (notably US500 variants) expose decimal places that imply
        # a sub-point pip fallback (0.1). For strategy/risk sizing we treat index pip
        # as 1.0 point unless IG provides explicit onePipMeans.
        if (
            self._index_pip_fallback_one_point
            and one_pip_means <= 0
            and _is_index_symbol(symbol, epic)
            and pip_size < 1.0
        ):
            pip_size = 1.0
            pip_size_source = "index_fallback_1_point"

        value_of_one_pip = _as_float(instrument.get("valueOfOnePip"), 0.0)
        value_per_pip = _as_float(instrument.get("valuePerPip"), 0.0)
        value_of_one_point = _as_float(instrument.get("valueOfOnePoint"), 0.0)
        value_per_point = _as_float(instrument.get("valuePerPoint"), 0.0)
        contract_size = _as_float(instrument.get("contractSize"), 1.0)

        tick_value = value_of_one_pip
        tick_value_source = "valueOfOnePip"
        tick_value_kind = "pip"
        if tick_value <= 0:
            tick_value = value_per_pip
            tick_value_source = "valuePerPip"
            tick_value_kind = "pip"
        if tick_value <= 0:
            tick_value = value_of_one_point
            tick_value_source = "valueOfOnePoint"
            tick_value_kind = "point"
        if tick_value <= 0:
            tick_value = value_per_point
            tick_value_source = "valuePerPoint"
            tick_value_kind = "point"
        if tick_value <= 0:
            tick_value = 1.0
            tick_value_source = "default_1.0"
            tick_value_kind = "fallback"
        tick_value_rescale_ratio: float | None = None
        # IG market-details are inconsistent across non-FX CFDs:
        # - some energy markets expose `valueOfOnePip` with an explicit
        #   `onePipMeans` smaller than a full point (e.g. 0.01 for WTI), and the
        #   raw pip value then double-counts contractSize;
        # - index / spot-metal contracts often expose `valueOfOnePip` already as
        #   the tradable contract point-value (`US100` = 100, `US30` = 10, etc.).
        #
        # Blindly dividing every pip-valued non-FX CFD by contractSize makes risk
        # sizing 10-100x too small while margin still uses the full contract
        # notional. Only normalize when IG gives an explicit fractional
        # `onePipMeans`, which is the pattern used by scaled energy contracts.
        if (
            tick_value > 0
            and contract_size > 1.0
            and tick_value_kind == "pip"
            and _is_non_fx_cfd_symbol(symbol, epic)
            and one_pip_means > 0.0
            and one_pip_means < 1.0
        ):
            implied_ratio = tick_value / contract_size
            if math.isfinite(implied_ratio) and implied_ratio > 0:
                tick_value_rescale_ratio = contract_size
                tick_value = implied_ratio
                tick_value_source = f"{tick_value_source}_normalized_by_contractSize"
        deal_currencies, default_currency = self._extract_deal_currencies(instrument, body)
        min_deal = _as_mapping(dealing_rules.get("minDealSize"))
        lot_min_value = _as_float(min_deal.get("value"), 0.0)
        if lot_min_value > 0:
            lot_min = lot_min_value
            lot_min_source = "minDealSize"
        else:
            lot_min, lot_min_source = self._default_lot_min_fallback(symbol, epic)
        max_deal = _as_mapping(dealing_rules.get("maxDealSize"))
        lot_max = _as_float(max_deal.get("value"), 100.0)
        min_step = _as_mapping(dealing_rules.get("minStepDistance"))
        step = _as_float(min_step.get("value"), 0.0)
        if step <= 0:
            lot_step = 0.01 if lot_min < 1.0 else 0.1
            lot_step_source = "fallback_by_lot_min"
        else:
            lot_step = min(1.0, step)
            lot_step_source = "minStepDistance"
        # Some markets expose minStepDistance for stop distances (points), not
        # for volume increment. Keep volume step from becoming coarser than min size.
        if lot_min > 0 and lot_min < lot_step:
            lot_step = lot_min
            lot_step_source = "capped_by_lot_min"
        # For non-FX CFDs, sub-minimum fractional sizes are commonly invalid on IG.
        if _is_non_fx_cfd_symbol(symbol, epic) and lot_min > 0 and lot_step + FLOAT_ROUNDING_TOLERANCE < lot_min:
            lot_step = lot_min
            lot_step_source = "promoted_to_lot_min_non_fx"
        lot_step = max(0.001, lot_step)
        lot_precision = max(
            2,
            _precision_from_step(lot_step),
            _precision_from_step(max(0.001, lot_min)),
        )
        min_stop = _as_mapping(dealing_rules.get("minNormalStopOrLimitDistance"))
        min_stop_value = _as_float(min_stop.get("value"), 0.0)
        min_stop_unit = str(min_stop.get("unit") or "").upper()
        min_stop_distance_price = 0.0
        if min_stop_value > 0:
            if min_stop_unit in {"POINT", "POINTS", "PIP", "PIPS"}:
                min_stop_distance_price = min_stop_value * max(quote_step, pip_size)
            else:
                # Keep raw value for unknown units; better to be conservative than to disable the guard.
                min_stop_distance_price = min_stop_value
        min_controlled_risk_stop = _as_mapping(dealing_rules.get("minControlledRiskStopDistance"))
        min_controlled_risk_stop_value = _as_float(min_controlled_risk_stop.get("value"), 0.0)
        min_controlled_risk_stop_unit = str(min_controlled_risk_stop.get("unit") or "").upper()
        min_controlled_risk_stop_distance_price = 0.0
        if min_controlled_risk_stop_value > 0:
            if min_controlled_risk_stop_unit in {"POINT", "POINTS", "PIP", "PIPS"}:
                min_controlled_risk_stop_distance_price = min_controlled_risk_stop_value * max(quote_step, pip_size)
            else:
                min_controlled_risk_stop_distance_price = min_controlled_risk_stop_value
        controlled_risk_allowed = _as_bool(instrument.get("controlledRiskAllowed"), False)
        guaranteed_stop_required = (
            controlled_risk_allowed
            and min_controlled_risk_stop_distance_price > 0
            and min_stop_distance_price <= 0
        )
        quote_id = str(snapshot.get("quoteId") or "").strip()

        margin_factor = _as_float(instrument.get("marginFactor"), 0.0)
        margin_factor_unit = str(instrument.get("marginFactorUnit") or "").upper()
        leverage = _as_float(instrument.get("leverage"), 0.0)
        margin_price_scale_divisor = _infer_margin_price_scale_divisor(
            symbol=symbol,
            pip_size=pip_size,
            one_pip_means=one_pip_means,
            scaling_factor=scaling_factor,
            pip_position=_as_float(pip_position, 0.0) if pip_position is not None else None,
        )
        if (
            tick_value_rescale_ratio is not None
            and margin_price_scale_divisor <= 1.0
            and _is_non_fx_cfd_symbol(symbol, epic)
        ):
            margin_price_scale_divisor = tick_value_rescale_ratio

        spec = SymbolSpec(
            symbol=symbol,
            tick_size=pip_size,
            tick_value=max(tick_value, FLOAT_COMPARISON_TOLERANCE),
            contract_size=contract_size,
            lot_min=max(0.001, lot_min),
            lot_max=max(lot_min, lot_max),
            lot_step=lot_step,
            min_stop_distance_price=(min_stop_distance_price if min_stop_distance_price > 0 else None),
            one_pip_means=(one_pip_means if one_pip_means > 0 else None),
            price_precision=precision,
            lot_precision=lot_precision,
            metadata={
                "broker": "ig",
                "epic": epic,
                "epic_variant": _epic_variant_key(epic),
                "quote_step": quote_step,
                "decimal_places_factor": decimal_places,
                "scaling_factor": scaling_factor,
                "pip_position": pip_position,
                "one_pip_means": one_pip_means,
                "pip_size_source": pip_size_source,
                "value_of_one_pip": value_of_one_pip,
                "value_per_pip": value_per_pip,
                "value_of_one_point": value_of_one_point,
                "value_per_point": value_per_point,
                "tick_value_source": tick_value_source,
                "tick_value_kind": tick_value_kind,
                "tick_value_rescale_ratio": tick_value_rescale_ratio,
                "deal_currencies": deal_currencies,
                "default_currency_code": default_currency,
                "lot_min_source": lot_min_source,
                "lot_step_source": lot_step_source,
                "min_stop_distance_value": min_stop_value,
                "min_stop_distance_unit": min_stop_unit,
                "min_stop_distance_price": min_stop_distance_price,
                "controlled_risk_allowed": controlled_risk_allowed,
                "guaranteed_stop_required": guaranteed_stop_required,
                "min_controlled_risk_stop_distance_value": min_controlled_risk_stop_value,
                "min_controlled_risk_stop_distance_unit": min_controlled_risk_stop_unit,
                "min_controlled_risk_stop_distance_price": min_controlled_risk_stop_distance_price,
                "quote_id": quote_id,
                "margin_factor": margin_factor,
                "margin_factor_unit": margin_factor_unit,
                "leverage": leverage,
                "margin_price_scale_divisor": margin_price_scale_divisor,
                "expiry": str(instrument.get("expiry") or "").strip(),
            },
        )
        if cache_symbol:
            self._cache_symbol_spec_entry(upper, spec, promote_symbol_cache=True)
        return spec

    def _get_symbol_spec_with_body_for_epic(self, symbol: str, epic: str) -> tuple[SymbolSpec, dict[str, Any]]:
        self._wait_for_market_rest_slot()
        body, _ = self._request("GET", f"/markets/{epic}", version="3", auth=True)
        self._clear_epic_unavailable_retry(symbol)
        spec = self._build_symbol_spec_from_market_details(symbol, epic, body, cache_symbol=False)
        return spec, body

    def _get_symbol_spec_for_epic(self, symbol: str, epic: str) -> SymbolSpec:
        spec, _ = self._get_symbol_spec_with_body_for_epic(symbol, epic)
        return spec

    @staticmethod
    def _normalize_symbol_spec_cache_symbol(symbol: str) -> str:
        return str(symbol).upper().strip()

    @staticmethod
    def _normalize_symbol_spec_cache_epic(epic: str) -> str:
        return str(epic).upper().strip()

    def _cache_symbol_spec_entry(
        self,
        symbol: str,
        spec: SymbolSpec,
        *,
        promote_symbol_cache: bool = True,
    ) -> None:
        upper_symbol = self._normalize_symbol_spec_cache_symbol(symbol)
        if not upper_symbol:
            return
        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        upper_epic = self._normalize_symbol_spec_cache_epic(metadata.get("epic") or "")
        with self._lock:
            if promote_symbol_cache:
                self._symbol_spec_cache[upper_symbol] = spec
            if upper_epic:
                self._symbol_spec_cache_by_epic[(upper_symbol, upper_epic)] = spec

    def _get_cached_symbol_spec_for_epic(self, symbol: str, epic: str) -> SymbolSpec | None:
        upper_symbol = self._normalize_symbol_spec_cache_symbol(symbol)
        upper_epic = self._normalize_symbol_spec_cache_epic(epic)
        if not upper_symbol or not upper_epic:
            return None
        with self._lock:
            return self._symbol_spec_cache_by_epic.get((upper_symbol, upper_epic))

    def _get_or_seed_cached_symbol_spec_for_epic_locked(
        self,
        symbol: str,
        epic: str,
        *,
        seed_spec: SymbolSpec,
    ) -> SymbolSpec:
        upper_symbol = self._normalize_symbol_spec_cache_symbol(symbol)
        upper_epic = self._normalize_symbol_spec_cache_epic(epic)
        cached_spec = self._symbol_spec_cache_by_epic.get((upper_symbol, upper_epic))
        if cached_spec is None:
            self._symbol_spec_cache_by_epic[(upper_symbol, upper_epic)] = seed_spec
            cached_spec = seed_spec
        current_symbol_spec = self._symbol_spec_cache.get(upper_symbol)
        current_symbol_epic = ""
        if current_symbol_spec is not None and isinstance(current_symbol_spec.metadata, dict):
            current_symbol_epic = self._normalize_symbol_spec_cache_epic(
                current_symbol_spec.metadata.get("epic") or "",
            )
        if current_symbol_spec is None or current_symbol_epic == upper_epic:
            self._symbol_spec_cache[upper_symbol] = cached_spec
        return cached_spec

    def _get_cached_symbol_spec(self, symbol: str) -> SymbolSpec | None:
        upper = self._normalize_symbol_spec_cache_symbol(symbol)
        with self._lock:
            return self._symbol_spec_cache.get(upper)

    @staticmethod
    def _clone_symbol_spec_for_epic(
        symbol: str,
        epic: str,
        base_spec: SymbolSpec,
        *,
        origin: str,
    ) -> SymbolSpec:
        metadata = dict(base_spec.metadata) if isinstance(base_spec.metadata, dict) else {}
        metadata["epic"] = str(epic).strip().upper()
        metadata["epic_variant"] = _epic_variant_key(epic)
        metadata["spec_origin"] = str(origin).strip() or "cached_clone"
        return SymbolSpec(
            symbol=str(symbol).upper(),
            tick_size=float(base_spec.tick_size),
            tick_value=float(base_spec.tick_value),
            contract_size=float(base_spec.contract_size),
            lot_min=float(base_spec.lot_min),
            lot_max=float(base_spec.lot_max),
            lot_step=float(base_spec.lot_step),
            min_stop_distance_price=(
                float(base_spec.min_stop_distance_price)
                if base_spec.min_stop_distance_price is not None
                else None
            ),
            one_pip_means=(
                float(base_spec.one_pip_means)
                if base_spec.one_pip_means is not None
                else None
            ),
            price_precision=int(base_spec.price_precision),
            lot_precision=int(base_spec.lot_precision),
            metadata=metadata,
        )

    def _build_fallback_symbol_spec_for_epic(self, symbol: str, epic: str) -> SymbolSpec:
        upper_symbol = str(symbol).upper().strip()
        upper_epic = str(epic).upper().strip()
        lot_min, lot_min_source = self._default_lot_min_fallback(upper_symbol, upper_epic)
        if _is_fx_pair_symbol(upper_symbol):
            tick_size = 0.0001
            price_precision = 5
        elif _is_index_symbol(upper_symbol, upper_epic):
            tick_size = 1.0
            price_precision = 1
        else:
            tick_size = 0.01
            price_precision = 2
        lot_step = 0.01 if lot_min < 1.0 else 0.1
        return SymbolSpec(
            symbol=upper_symbol,
            tick_size=tick_size,
            tick_value=1.0,
            contract_size=1.0,
            lot_min=max(0.001, float(lot_min)),
            lot_max=max(100.0, float(lot_min)),
            lot_step=lot_step,
            price_precision=price_precision,
            lot_precision=max(2, _precision_from_step(max(0.001, lot_step))),
            metadata={
                "broker": "ig",
                "epic": upper_epic,
                "epic_variant": _epic_variant_key(upper_epic),
                "lot_min_source": lot_min_source,
                "spec_origin": "critical_fallback",
            },
        )

    def _resolve_open_candidate_spec(
        self,
        *,
        symbol: str,
        epic: str,
        reference_spec: SymbolSpec | None,
        allow_market_details: bool,
    ) -> tuple[SymbolSpec, dict[str, Any]]:
        if allow_market_details:
            return self._get_symbol_spec_with_body_for_epic(symbol, epic)
        cached_for_epic = self._get_cached_symbol_spec_for_epic(symbol, epic)
        if cached_for_epic is not None:
            return (
                self._clone_symbol_spec_for_epic(
                    symbol,
                    epic,
                    cached_for_epic,
                    origin="critical_cached_epic",
                ),
                {},
            )
        if reference_spec is not None:
            reference_metadata = (
                reference_spec.metadata
                if isinstance(reference_spec.metadata, dict)
                else {}
            )
            reference_epic = str(reference_metadata.get("epic") or "").strip().upper()
            if reference_epic == str(epic).strip().upper():
                return (
                    self._clone_symbol_spec_for_epic(
                        symbol,
                        epic,
                        reference_spec,
                        origin="critical_cached_reference",
                    ),
                    {},
                )
        cached = self._get_cached_symbol_spec(symbol)
        if cached is not None:
            cached_metadata = cached.metadata if isinstance(cached.metadata, dict) else {}
            cached_epic = str(cached_metadata.get("epic") or "").strip().upper()
            if cached_epic == str(epic).strip().upper():
                return (
                    self._clone_symbol_spec_for_epic(
                        symbol,
                        epic,
                        cached,
                        origin="critical_cached_symbol",
                    ),
                    {},
                )
        return self._build_fallback_symbol_spec_for_epic(symbol, epic), {}

    def _extract_close_sync_from_confirm(self, position_id: str, confirm: dict[str, Any]) -> dict[str, Any]:
        sync: dict[str, Any] = {"position_id": position_id, "source": "ig_confirm"}
        deal_reference = str(confirm.get("dealReference") or "").strip()
        if deal_reference:
            sync["deal_reference"] = deal_reference
        level = _as_float(confirm.get("level"), 0.0)
        if level > 0:
            sync["close_price"] = level

        # IG confirm can provide realized profit in account currency.
        realized = _as_float(confirm.get("profit"), 0.0)
        if realized != 0.0 or "profit" in confirm:
            sync["realized_pnl"] = realized

        profit_currency = _normalize_currency_code(confirm.get("profitCurrency")) or str(
            confirm.get("profitCurrency") or ""
        ).strip()
        if profit_currency:
            sync["pnl_currency"] = profit_currency

        close_deal_id = str(confirm.get("dealId") or "").strip()
        if close_deal_id:
            sync["close_deal_id"] = close_deal_id

        status = str(confirm.get("dealStatus") or "").strip()
        if status:
            sync["deal_status"] = status
        reason = str(confirm.get("reason") or "").strip()
        if reason:
            sync["deal_reason"] = reason
        closed_at = _parse_datetime_to_unix_seconds(
            confirm.get("date")
            or confirm.get("dateUtc")
            or confirm.get("createdDateUTC")
            or confirm.get("createdDate")
        )
        if closed_at is not None:
            sync["closed_at"] = closed_at
        return sync

    def _maybe_cleanup_internal_caches(self, *, force: bool = False) -> None:
        now_ts = time.time()
        with self._lock:
            if not force and (now_ts - self._last_cache_cleanup_ts) < self._cache_cleanup_interval_sec:
                return
            self._last_cache_cleanup_ts = now_ts
            cutoff_ts = now_ts - self._sync_cache_max_age_sec
            if cutoff_ts <= 0:
                return

            for cache in (self._position_open_sync, self._position_close_sync):
                stale_keys = [
                    key
                    for key, payload in cache.items()
                    if _as_float(_as_mapping(payload).get("_cache_ts"), 0.0) < cutoff_ts
                ]
                for key in stale_keys:
                    cache.pop(key, None)

            stale_pending_keys = [
                key
                for key, ts in self._pending_confirm_last_probe_ts.items()
                if _as_float(ts, 0.0) < cutoff_ts
            ]
            for key in stale_pending_keys:
                self._pending_confirm_last_probe_ts.pop(key, None)

            stale_invalid_symbols: list[str] = []
            for symbol, epic_map in self._invalid_epic_until_ts_by_symbol.items():
                if not isinstance(epic_map, dict):
                    stale_invalid_symbols.append(symbol)
                    continue
                stale_epics = [
                    epic
                    for epic, until_ts in epic_map.items()
                    if _as_float(until_ts, 0.0) <= now_ts
                ]
                for epic in stale_epics:
                    epic_map.pop(epic, None)
                if not epic_map:
                    stale_invalid_symbols.append(symbol)
            for symbol in stale_invalid_symbols:
                self._invalid_epic_until_ts_by_symbol.pop(symbol, None)

            stale_stream_block_symbols = [
                symbol
                for symbol, until_ts in self._stream_rest_fallback_block_until_ts_by_symbol.items()
                if _as_float(until_ts, 0.0) <= now_ts
            ]
            for symbol in stale_stream_block_symbols:
                self._stream_rest_fallback_block_until_ts_by_symbol.pop(symbol, None)
                self._stream_rest_fallback_block_reason_by_symbol.pop(symbol, None)

    def _cache_close_sync_payload(self, position_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        cached_payload = dict(payload)
        cached_payload["_cache_ts"] = time.time()
        with self._lock:
            self._position_close_sync[str(position_id)] = cached_payload
        return cached_payload

    @staticmethod
    def _close_sync_has_factual_details(payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        return any(payload.get(key) is not None for key in ("close_price", "realized_pnl", "closed_at"))

    @staticmethod
    def _close_sync_has_realized_pnl(payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        return payload.get("realized_pnl") is not None

    @staticmethod
    def _close_sync_candidate_identity(payload: dict[str, Any]) -> tuple[Any, ...]:
        close_deal_id = str(payload.get("close_deal_id") or "").strip()
        if close_deal_id:
            return ("deal", close_deal_id)
        history_reference = str(payload.get("history_reference") or payload.get("deal_reference") or "").strip()
        closed_at = _as_float(payload.get("closed_at"), 0.0)
        close_price = payload.get("close_price")
        realized_pnl = payload.get("realized_pnl")
        return (
            "value",
            history_reference,
            round(closed_at, 3) if closed_at > 0 else 0.0,
            round(_as_float(close_price, 0.0), 10) if close_price is not None else None,
            round(_as_float(realized_pnl, 0.0), 10) if realized_pnl is not None else None,
        )

    @staticmethod
    def _aggregate_close_sync_candidates(
        position_id: str,
        source: str,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not candidates:
            return None
        deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            deduped[IgApiClient._close_sync_candidate_identity(candidate)] = dict(candidate)
        ordered = list(deduped.values())
        if not ordered:
            return None

        def sort_key(candidate: dict[str, Any]) -> tuple[int, float, int, int]:
            closed_at = _as_float(candidate.get("closed_at"), 0.0)
            has_price = 1 if candidate.get("close_price") is not None else 0
            has_pnl = 1 if candidate.get("realized_pnl") is not None else 0
            return (1 if closed_at > 0 else 0, closed_at, has_price, has_pnl)

        latest = max(ordered, key=sort_key)
        aggregate = dict(latest)
        aggregate["position_id"] = position_id
        aggregate["source"] = source

        realized_pnls = [_as_float(candidate.get("realized_pnl"), float("nan")) for candidate in ordered]
        realized_pnls = [value for value in realized_pnls if value == value]
        if realized_pnls:
            aggregate["realized_pnl"] = float(sum(realized_pnls))

        closed_ats = [_as_float(candidate.get("closed_at"), 0.0) for candidate in ordered]
        closed_ats = [value for value in closed_ats if value > 0]
        if closed_ats:
            aggregate["closed_at"] = max(closed_ats)

        match_rank = 0
        for candidate in ordered:
            if str(candidate.get("history_match_mode") or "").strip().lower() == "position_id":
                match_rank = 2
                break
            if str(candidate.get("history_match_mode") or "").strip().lower() == "deal_reference":
                match_rank = max(match_rank, 1)
        if match_rank == 2:
            aggregate["history_match_mode"] = "position_id"
        elif match_rank == 1:
            aggregate["history_match_mode"] = "deal_reference"

        currencies = {
            str(candidate.get("pnl_currency") or "").strip().upper()
            for candidate in ordered
            if str(candidate.get("pnl_currency") or "").strip()
        }
        if len(currencies) == 1:
            aggregate["pnl_currency"] = next(iter(currencies))

        if len(ordered) > 1:
            aggregate["partial_close_count"] = len(ordered) - 1
        return aggregate

    @staticmethod
    def _iter_history_transaction_items(body: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for key in ("transactions", "activities", "data", "Transactions", "Activities", "Data"):
            raw = body.get(key)
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        candidates.append(item)
        if not candidates and isinstance(body, dict):
            candidates.append(body)
        return candidates

    @staticmethod
    def _history_get(item: dict[str, Any], *keys: str) -> Any:
        if not isinstance(item, dict):
            return None
        lowered_lookup: dict[str, Any] | None = None
        for key in keys:
            if key in item:
                value = item.get(key)
                if value not in (None, ""):
                    return value
            lowered_key = str(key).lower()
            if lowered_lookup is None:
                lowered_lookup = {
                    str(raw_key).lower(): raw_value
                    for raw_key, raw_value in item.items()
                    if raw_value not in (None, "")
                }
            if lowered_key in lowered_lookup:
                return lowered_lookup[lowered_key]
        return None

    @staticmethod
    def _history_text_values(item: dict[str, Any]) -> list[str]:
        values: list[str] = []
        keys = (
            "result",
            "summary",
            "description",
            "details",
            "activityType",
            "transactionType",
            "type",
            "status",
            "statusReason",
            "reason",
            "marketName",
            "instrumentName",
            "market",
            "comment",
            "note",
        )
        for key in keys:
            raw = IgApiClient._history_get(item, key)
            if raw in (None, ""):
                continue
            text = str(raw).strip()
            if text:
                values.append(text)
        return values

    @staticmethod
    def _item_contains_position_id(item: dict[str, Any], position_id: str) -> bool:
        target = str(position_id).strip()
        if not target:
            return False
        for key in (
            "dealId",
            "openDealId",
            "affectedDealId",
        ):
            value = IgApiClient._history_get(item, key)
            if value is not None and str(value).strip() == target:
                return True

        affected = IgApiClient._history_get(item, "affectedDeals")
        if isinstance(affected, list):
            for row in affected:
                if not isinstance(row, dict):
                    continue
                for key in ("dealId", "affectedDealId"):
                    value = IgApiClient._history_get(row, key)
                    if value is not None and str(value).strip() == target:
                        return True
        target_upper = target.upper()
        short_target = target_upper[-8:] if len(target_upper) >= 8 else target_upper
        compact_short_target = _sanitize_deal_reference(short_target)
        if compact_short_target:
            token_pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(compact_short_target)}(?![A-Z0-9])")
            for text in IgApiClient._history_text_values(item):
                upper_text = str(text).upper()
                if target_upper in upper_text:
                    return True
                if token_pattern.search(upper_text):
                    return True
        return False

    @staticmethod
    def _item_contains_deal_reference(item: dict[str, Any], deal_reference: str) -> bool:
        raw_target = str(deal_reference).strip()
        target = _sanitize_deal_reference(raw_target) if raw_target else ""
        if not target:
            return False
        for key in ("reference", "dealReference"):
            value = IgApiClient._history_get(item, key)
            if value is not None and _deal_reference_matches(target, str(value).strip()):
                return True

        affected = IgApiClient._history_get(item, "affectedDeals")
        if isinstance(affected, list):
            for row in affected:
                if not isinstance(row, dict):
                    continue
                for key in ("reference", "dealReference"):
                    value = IgApiClient._history_get(row, key)
                    if value is not None and _deal_reference_matches(target, str(value).strip()):
                        return True
        return False

    def _history_item_has_close_evidence(self, item: dict[str, Any]) -> bool:
        # Explicit close-only numeric fields.
        explicit_close_level = _as_float(self._history_get(item, "closeLevel"), float("nan"))
        if explicit_close_level == explicit_close_level and explicit_close_level > 0:
            return True
        if self._extract_realized_pnl_from_history_item(item) is not None:
            return True
        close_reason = str(
            self._history_get(item, "transactionType")
            or self._history_get(item, "activityType")
            or self._history_get(item, "type")
            or ""
        ).strip().upper()
        if "CLOSE" in close_reason:
            return True
        for text in self._history_text_values(item):
            upper_text = str(text).upper()
            if "POSITION OPENED" in upper_text:
                continue
            if re.search(r"\bPOSITION(?:/S)?\s+CLOSED\b", upper_text):
                return True
        for action in self._history_item_actions(item):
            action_type = str(self._history_get(action, "actionType") or "").strip().upper()
            if "CLOSE" in action_type:
                return True
            if action_type in {"POSITION_PARTIALLY_CLOSED", "POSITION_CLOSED"}:
                return True
        return False

    @staticmethod
    def _history_item_is_rejected(item: dict[str, Any]) -> bool:
        def _is_reject_text(value: Any) -> bool:
            text = str(value or "").strip().upper()
            if not text:
                return False
            return (
                "REJECT" in text
                or "FAILED" in text
                or text in {"ERROR", "INVALID"}
            )

        candidate_sources: list[dict[str, Any]] = [item]
        details = _as_mapping(IgApiClient._history_get(item, "details"))
        if details:
            candidate_sources.append(details)
        candidate_sources.extend(IgApiClient._history_item_actions(item))

        for source in candidate_sources:
            for key in (
                "actionStatus",
                "status",
                "dealStatus",
                "orderStatus",
                "result",
                "summary",
                "description",
                "activityType",
            ):
                if _is_reject_text(IgApiClient._history_get(source, key)):
                    return True
        return False

    @staticmethod
    def _extract_numeric_amount(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, dict):
            for key in ("amount", "value", "profitAndLoss", "profit", "pnl"):
                if key not in value:
                    continue
                parsed = IgApiClient._extract_numeric_amount(value.get(key))
                if parsed is not None:
                    return parsed
            return None
        if isinstance(value, list):
            for item in value:
                parsed = IgApiClient._extract_numeric_amount(item)
                if parsed is not None:
                    return parsed
            return None

        parsed = _as_float(value, float("nan"))
        if parsed == parsed:
            return parsed

        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace(",", "").replace("€", "").replace("$", "").replace("£", "").replace("#", "")
        if normalized.startswith("(") and normalized.endswith(")"):
            normalized = f"-{normalized[1:-1]}"
        matched = re.search(r"[-+]?\d+(?:\.\d+)?", normalized)
        if not matched:
            return None
        parsed = _as_float(matched.group(0), float("nan"))
        if parsed == parsed:
            return parsed
        return None

    @staticmethod
    def _extract_realized_pnl_from_history_item(item: dict[str, Any]) -> float | None:
        direct_keys = ("profitAndLoss", "profit", "pnl")
        candidate_sources: list[dict[str, Any]] = [item]
        details = _as_mapping(IgApiClient._history_get(item, "details"))
        if details:
            candidate_sources.append(details)
        candidate_sources.extend(IgApiClient._history_item_actions(item))

        for source in candidate_sources:
            for key in direct_keys:
                value = IgApiClient._history_get(source, key)
                parsed = IgApiClient._extract_numeric_amount(value)
                if parsed is not None:
                    return parsed
        return None

    @staticmethod
    def _extract_close_price_from_history_item(item: dict[str, Any]) -> float | None:
        # Prefer explicit close fields first; fallback to "level" for history rows
        # that only expose generic execution level for close events.
        for key in ("closeLevel", "transactionPrice", "closePrice", "closingPrice", "level"):
            parsed = _as_float(IgApiClient._history_get(item, key), float("nan"))
            if parsed == parsed and parsed > 0:
                return parsed
        return None

    @staticmethod
    def _extract_history_closed_at(item: dict[str, Any]) -> float | None:
        for key in (
            "dateUtc",
            "dateUTC",
            "date",
            "createdDateUTC",
            "createdDate",
            "timestamp",
            "transactionTime",
        ):
            parsed = _parse_datetime_to_unix_seconds(IgApiClient._history_get(item, key))
            if parsed is not None:
                return parsed
        date_text = str(IgApiClient._history_get(item, "date") or "").strip()
        time_text = str(IgApiClient._history_get(item, "time") or "").strip()
        if date_text and time_text:
            for fmt in (
                "%d/%m/%y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%y %H:%M",
                "%d/%m/%Y %H:%M",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
            ):
                try:
                    parsed_dt = datetime.strptime(f"{date_text} {time_text}", fmt)
                except ValueError:
                    continue
                return parsed_dt.replace(tzinfo=timezone.utc).timestamp()
        return None

    @staticmethod
    def _extract_history_opened_at(item: dict[str, Any]) -> float | None:
        for key in (
            "openDateUtc",
            "openDateUTC",
            "openDate",
            "openTime",
            "createdDateUTC",
            "createdDate",
        ):
            parsed = _parse_datetime_to_unix_seconds(IgApiClient._history_get(item, key))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _extract_symbol_from_history_item(item: dict[str, Any]) -> str | None:
        explicit = str(IgApiClient._history_get(item, "symbol") or "").strip().upper()
        if len(explicit) == 6 and explicit.isalpha():
            return explicit

        epic = str(
            IgApiClient._history_get(item, "epic")
            or IgApiClient._history_get(item, "marketEpic")
            or IgApiClient._history_get(item, "textEpic")
            or ""
        ).strip().upper()
        if epic:
            matched = re.search(r"\.D\.([A-Z]{6})\.", epic)
            if matched:
                return matched.group(1)

        for key in ("marketName", "instrumentName", "summary", "market"):
            text = str(IgApiClient._history_get(item, key) or "").strip().upper()
            if not text:
                continue
            slash_match = re.search(r"\b([A-Z]{3})/([A-Z]{3})\b", text)
            if slash_match:
                return f"{slash_match.group(1)}{slash_match.group(2)}"
            plain_match = re.search(r"\b([A-Z]{6})\b", text)
            if plain_match:
                candidate = plain_match.group(1)
                if candidate.isalpha():
                    return candidate
        return None

    @staticmethod
    def _extract_history_reference(item: dict[str, Any]) -> str:
        return str(
            IgApiClient._history_get(item, "reference")
            or IgApiClient._history_get(item, "dealReference")
            or ""
        ).strip()

    def _build_close_sync_from_history_item(self, position_id: str, item: dict[str, Any]) -> dict[str, Any]:
        sync: dict[str, Any] = {"position_id": position_id, "source": "ig_history_transactions"}

        close_price = self._extract_close_price_from_history_item(item)
        if close_price is not None:
            sync["close_price"] = close_price

        realized_pnl = self._extract_realized_pnl_from_history_item(item)
        if realized_pnl is not None:
            sync["realized_pnl"] = realized_pnl

        currency = _normalize_currency_code(
            self._history_get(item, "currency")
            or self._history_get(item, "profitCurrency")
            or self._history_get(item, "currencyIsoCode")
        ) or str(
            self._history_get(item, "currency")
            or self._history_get(item, "profitCurrency")
            or self._history_get(item, "currencyIsoCode")
            or ""
        ).strip()
        if currency:
            sync["pnl_currency"] = currency

        reference = self._extract_history_reference(item)
        if reference:
            sync["history_reference"] = reference
            sync["deal_reference"] = reference

        close_deal_id = str(
            self._history_get(item, "dealId")
            or self._history_get(item, "affectedDealId")
            or ""
        ).strip()
        if close_deal_id:
            sync["close_deal_id"] = close_deal_id

        closed_at = self._extract_history_closed_at(item)
        if closed_at is not None:
            sync["closed_at"] = closed_at
        return sync

    @staticmethod
    def _history_item_contains_close_deal_id(item: dict[str, Any], close_deal_id: str) -> bool:
        target = str(close_deal_id or "").strip()
        if not target:
            return False
        for key in ("dealId", "affectedDealId"):
            value = IgApiClient._history_get(item, key)
            if value is not None and str(value).strip() == target:
                return True
        for action in IgApiClient._history_item_actions(item):
            for key in ("dealId", "affectedDealId"):
                value = IgApiClient._history_get(action, key)
                if value is not None and str(value).strip() == target:
                    return True
        return False

    @staticmethod
    def _history_item_matches_close_signature(
        item: dict[str, Any],
        *,
        close_price: float | None,
        closed_at: float | None,
        symbol: str | None,
        tick_size: float | None,
    ) -> bool:
        candidate_close_price = IgApiClient._extract_close_price_from_history_item(item)
        candidate_closed_at = IgApiClient._extract_history_closed_at(item)
        if (
            close_price is None
            or close_price <= 0.0
            or candidate_close_price is None
            or candidate_closed_at is None
            or closed_at is None
            or closed_at <= 0.0
        ):
            return False
        price_tolerance = max(
            abs(float(tick_size or 0.0)),
            abs(float(close_price)) * 1e-6,
            1e-6,
        )
        if abs(float(candidate_close_price) - float(close_price)) > price_tolerance:
            return False
        if abs(float(candidate_closed_at) - float(closed_at)) > 300.0:
            return False
        normalized_symbol = str(symbol or "").strip().upper()
        if normalized_symbol:
            candidate_symbol = IgApiClient._extract_symbol_from_history_item(item)
            if candidate_symbol and candidate_symbol != normalized_symbol:
                return False
        return True

    def _fetch_close_sync_from_history(
        self,
        position_id: str,
        *,
        deal_reference: str | None = None,
        opened_at: float | None = None,
        symbol: str | None = None,
        close_deal_id: str | None = None,
        close_price: float | None = None,
        closed_at: float | None = None,
        tick_size: float | None = None,
    ) -> dict[str, Any] | None:
        now_utc = datetime.now(timezone.utc)
        from_utc = now_utc - timedelta(hours=48)
        page_size = 200
        max_pages = 5
        raw_reference_target = str(deal_reference or "").strip()
        reference_target = _sanitize_deal_reference(raw_reference_target) if raw_reference_target else ""
        close_deal_target = str(close_deal_id or "").strip()
        close_price_target = _as_float(close_price, float("nan"))
        if close_price_target != close_price_target or close_price_target <= 0.0:
            close_price_target = 0.0
        closed_at_target = _as_float(closed_at, float("nan"))
        if closed_at_target != closed_at_target or closed_at_target <= 0.0:
            closed_at_target = 0.0
        tick_size_target = _as_float(tick_size, float("nan"))
        if tick_size_target != tick_size_target or tick_size_target <= 0.0:
            tick_size_target = 0.0
        opened_at_value = _as_float(opened_at, 0.0) if opened_at is not None else 0.0
        normalized_symbol = str(symbol or "").strip().upper()

        matched_candidates: list[dict[str, Any]] = []

        query_modes: list[dict[str, Any]] = [
            {
                "mode": "date_range",
                "query": {
                    "from": from_utc.isoformat().replace("+00:00", "Z"),
                    "to": now_utc.isoformat().replace("+00:00", "Z"),
                },
            },
            {
                "mode": "max_span",
                "query": {
                    "maxSpanSeconds": int(48 * 60 * 60),
                },
            },
        ]

        for query_mode in query_modes:
            had_any_items = False
            for page_number in range(1, max_pages + 1):
                query = dict(_as_mapping(query_mode.get("query")))
                query["pageSize"] = page_size
                query["pageNumber"] = page_number
                try:
                    body, _ = self._request("GET", "/history/transactions", version="2", auth=True, query=query)
                except Exception:
                    break

                items = self._iter_history_transaction_items(body)
                if not items:
                    break
                had_any_items = True

                for item in items:
                    if self._history_item_is_rejected(item):
                        continue
                    match_mode: str | None = None
                    if self._item_contains_position_id(item, position_id):
                        match_mode = "position_id"
                    elif reference_target and self._item_contains_deal_reference(item, reference_target):
                        match_mode = "deal_reference"
                    elif close_deal_target and self._history_item_contains_close_deal_id(item, close_deal_target):
                        match_mode = "close_deal_id"
                    elif self._history_item_matches_close_signature(
                        item,
                        close_price=(close_price_target if close_price_target > 0.0 else None),
                        closed_at=(closed_at_target if closed_at_target > 0.0 else None),
                        symbol=normalized_symbol or None,
                        tick_size=(tick_size_target if tick_size_target > 0.0 else None),
                    ):
                        match_mode = "close_signature"
                    if match_mode is None:
                        continue
                    if not self._history_item_has_close_evidence(item):
                        continue

                    item_symbol = self._extract_symbol_from_history_item(item)
                    if normalized_symbol and item_symbol and item_symbol != normalized_symbol:
                        continue

                    candidate = self._build_close_sync_from_history_item(position_id, item)
                    candidate["history_match_mode"] = match_mode
                    candidate_closed_at = _as_float(candidate.get("closed_at"), 0.0)
                    if (
                        opened_at_value > 0
                        and candidate_closed_at > 0
                        and (candidate_closed_at + 2.0) < opened_at_value
                    ):
                        continue

                    matched_candidates.append(candidate)

                if len(items) < page_size:
                    break
            if matched_candidates:
                return self._aggregate_close_sync_candidates(
                    position_id,
                    "ig_history_transactions",
                    matched_candidates,
                )
            if had_any_items:
                continue

        return self._aggregate_close_sync_candidates(
            position_id,
            "ig_history_transactions",
            matched_candidates,
        )

    def _complete_close_sync_from_activity_hints(
        self,
        position_id: str,
        activity_payload: dict[str, Any] | None,
        *,
        deal_reference: str | None = None,
        opened_at: float | None = None,
        symbol: str | None = None,
        tick_size: float | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(activity_payload, dict):
            return activity_payload
        if self._close_sync_has_realized_pnl(activity_payload):
            return activity_payload
        hinted_history = self._fetch_close_sync_from_history(
            position_id,
            deal_reference=deal_reference,
            opened_at=opened_at,
            symbol=symbol,
            close_deal_id=str(activity_payload.get("close_deal_id") or "").strip() or None,
            close_price=_as_float(activity_payload.get("close_price"), float("nan")),
            closed_at=_as_float(activity_payload.get("closed_at"), float("nan")),
            tick_size=tick_size,
        )
        if not isinstance(hinted_history, dict):
            return activity_payload
        merged = dict(activity_payload)
        for key in (
            "close_price",
            "realized_pnl",
            "pnl_currency",
            "history_reference",
            "deal_reference",
            "close_deal_id",
            "closed_at",
            "history_match_mode",
        ):
            value = hinted_history.get(key)
            if value not in (None, ""):
                merged[key] = value
        if self._close_sync_has_realized_pnl(hinted_history):
            merged["source"] = str(hinted_history.get("source") or "ig_history_transactions")
        return merged

    @staticmethod
    def _iter_history_activity_items(body: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for key in ("activities", "activity", "data", "Activities", "Activity", "Data"):
            raw = body.get(key)
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        candidates.append(item)
        if not candidates and isinstance(body, dict):
            candidates.append(body)
        return candidates

    @staticmethod
    def _history_item_actions(item: dict[str, Any]) -> list[dict[str, Any]]:
        details_raw = IgApiClient._history_get(item, "details")
        details = _as_mapping(details_raw)
        actions_raw = IgApiClient._history_get(details, "actions")
        if isinstance(actions_raw, list):
            return [row for row in actions_raw if isinstance(row, dict)]
        return []

    @staticmethod
    def _activity_item_contains_position_id(item: dict[str, Any], position_id: str) -> bool:
        if IgApiClient._item_contains_position_id(item, position_id):
            return True
        target = str(position_id).strip()
        if not target:
            return False
        for action in IgApiClient._history_item_actions(item):
            for key in ("affectedDealId", "dealId", "openDealId"):
                value = IgApiClient._history_get(action, key)
                if value is not None and str(value).strip() == target:
                    return True
        return False

    @staticmethod
    def _activity_item_contains_deal_reference(item: dict[str, Any], deal_reference: str) -> bool:
        if IgApiClient._item_contains_deal_reference(item, deal_reference):
            return True
        target = _sanitize_deal_reference(str(deal_reference or ""))
        if not target:
            return False
        details = _as_mapping(IgApiClient._history_get(item, "details"))
        detail_reference = str(IgApiClient._history_get(details, "dealReference") or "").strip()
        if detail_reference and _deal_reference_matches(target, detail_reference):
            return True
        for action in IgApiClient._history_item_actions(item):
            action_reference = str(IgApiClient._history_get(action, "dealReference") or "").strip()
            if action_reference and _deal_reference_matches(target, action_reference):
                return True
        return False

    def _activity_item_has_close_evidence(self, item: dict[str, Any]) -> bool:
        if self._history_item_has_close_evidence(item):
            return True
        for action in IgApiClient._history_item_actions(item):
            action_type = str(IgApiClient._history_get(action, "actionType") or "").strip().upper()
            if "CLOSE" in action_type:
                return True
            if action_type in {"POSITION_PARTIALLY_CLOSED", "POSITION_CLOSED"}:
                return True
        return False

    def _build_close_sync_from_activity_item(self, position_id: str, item: dict[str, Any]) -> dict[str, Any]:
        sync: dict[str, Any] = {"position_id": position_id, "source": "ig_history_activity"}
        details = _as_mapping(self._history_get(item, "details"))
        actions = self._history_item_actions(item)

        close_price: float | None = None
        for action in actions:
            action_type = str(self._history_get(action, "actionType") or "").strip().upper()
            if "CLOSE" not in action_type and action_type not in {"POSITION_PARTIALLY_CLOSED", "POSITION_CLOSED"}:
                continue
            parsed = _as_float(self._history_get(action, "level"), float("nan"))
            if parsed == parsed and parsed > 0:
                close_price = parsed
                break
        if close_price is None:
            close_price = self._extract_close_price_from_history_item(item)
        if close_price is None:
            close_price = self._extract_close_price_from_history_item(details)
        if close_price is not None:
            sync["close_price"] = close_price

        realized_pnl = self._extract_realized_pnl_from_history_item(item)
        if realized_pnl is None:
            realized_pnl = self._extract_realized_pnl_from_history_item(details)
        if realized_pnl is not None:
            sync["realized_pnl"] = realized_pnl

        currency = _normalize_currency_code(
            self._history_get(item, "currency")
            or self._history_get(details, "currency")
            or self._history_get(item, "profitCurrency")
            or self._history_get(item, "currencyIsoCode")
        ) or str(
            self._history_get(item, "currency")
            or self._history_get(details, "currency")
            or self._history_get(item, "profitCurrency")
            or self._history_get(item, "currencyIsoCode")
            or ""
        ).strip()
        if currency:
            sync["pnl_currency"] = currency

        reference = str(
            self._history_get(details, "dealReference")
            or self._extract_history_reference(item)
            or ""
        ).strip()
        if not reference:
            for action in actions:
                action_reference = str(self._history_get(action, "dealReference") or "").strip()
                if action_reference:
                    reference = action_reference
                    break
        if reference:
            sync["history_reference"] = reference
            sync["deal_reference"] = reference

        close_deal_id = str(
            self._history_get(item, "dealId")
            or ""
        ).strip()
        if not close_deal_id:
            for action in actions:
                action_id = str(
                    self._history_get(action, "dealId")
                    or self._history_get(action, "affectedDealId")
                    or ""
                ).strip()
                if action_id:
                    close_deal_id = action_id
                    break
        if close_deal_id:
            sync["close_deal_id"] = close_deal_id

        closed_at = self._extract_history_closed_at(item)
        if closed_at is None:
            closed_at = self._extract_history_closed_at(details)
        if closed_at is not None:
            sync["closed_at"] = closed_at
        return sync

    def _fetch_close_sync_from_activity(
        self,
        position_id: str,
        *,
        deal_reference: str | None = None,
        opened_at: float | None = None,
        symbol: str | None = None,
    ) -> dict[str, Any] | None:
        now_utc = datetime.now(timezone.utc)
        from_utc = now_utc - timedelta(hours=48)
        page_size = 200
        max_pages = 5
        reference_target = _sanitize_deal_reference(str(deal_reference or ""))
        opened_at_value = _as_float(opened_at, 0.0) if opened_at is not None else 0.0
        normalized_symbol = str(symbol or "").strip().upper()

        matched_candidates: list[dict[str, Any]] = []
        endpoint_versions = ("3", "2")
        for version in endpoint_versions:
            query_modes: list[dict[str, Any]] = []
            if version == "3":
                query_modes.append(
                    {
                        "mode": "v3_date_range",
                        "query": {
                            "from": from_utc.isoformat().replace("+00:00", "Z"),
                            "to": now_utc.isoformat().replace("+00:00", "Z"),
                            "detailed": "true",
                            "dealId": str(position_id).strip(),
                        },
                    }
                )
                query_modes.append(
                    {
                        "mode": "v3_deal_only",
                        "query": {
                            "detailed": "true",
                            "dealId": str(position_id).strip(),
                        },
                    }
                )
            else:
                query_modes.append(
                    {
                        "mode": "v2_date_range",
                        "query": {
                            "from": from_utc.isoformat().replace("+00:00", "Z"),
                            "to": now_utc.isoformat().replace("+00:00", "Z"),
                        },
                    }
                )
                query_modes.append(
                    {
                        "mode": "v2_max_span",
                        "query": {
                            "maxSpanSeconds": int(48 * 60 * 60),
                        },
                    }
                )

            for query_mode in query_modes:
                had_any_items = False
                for page_number in range(1, max_pages + 1):
                    query = dict(_as_mapping(query_mode.get("query")))
                    query["pageSize"] = page_size
                    query["pageNumber"] = page_number
                    try:
                        body, _ = self._request("GET", "/history/activity", version=version, auth=True, query=query)
                    except Exception:
                        break

                    items = self._iter_history_activity_items(body)
                    if not items:
                        break
                    had_any_items = True

                    for item in items:
                        if self._history_item_is_rejected(item):
                            continue
                        match_mode: str | None = None
                        if self._activity_item_contains_position_id(item, position_id):
                            match_mode = "position_id"
                        elif reference_target and self._activity_item_contains_deal_reference(item, reference_target):
                            match_mode = "deal_reference"
                        if match_mode is None:
                            continue
                        if not self._activity_item_has_close_evidence(item):
                            continue

                        item_symbol = self._extract_symbol_from_history_item(item)
                        if normalized_symbol and item_symbol and item_symbol != normalized_symbol:
                            continue

                        candidate = self._build_close_sync_from_activity_item(position_id, item)
                        candidate["history_match_mode"] = match_mode
                        candidate_closed_at = _as_float(candidate.get("closed_at"), 0.0)
                        if (
                            opened_at_value > 0
                            and candidate_closed_at > 0
                            and (candidate_closed_at + 2.0) < opened_at_value
                        ):
                            continue

                        matched_candidates.append(candidate)

                    if len(items) < page_size:
                        break
                if matched_candidates:
                    return self._aggregate_close_sync_candidates(
                        position_id,
                        "ig_history_activity",
                        matched_candidates,
                    )
                if had_any_items:
                    continue
        return self._aggregate_close_sync_candidates(
            position_id,
            "ig_history_activity",
            matched_candidates,
        )

    @staticmethod
    def _is_position_not_found_error_text(text: str) -> bool:
        lowered = str(text).lower()
        if "error.position.notfound" in lowered:
            return True
        return (
            "404" in lowered
            and "position" in lowered
            and "notfound" in lowered
        )

    def _probe_position_open_state_by_deal_id(self, position_id: str) -> bool | None:
        normalized_position_id = str(position_id).strip()
        if not normalized_position_id:
            return None
        safe_position_id = parse.quote(normalized_position_id, safe="")
        try:
            body, _ = self._request(
                "GET",
                f"/positions/{safe_position_id}",
                version="2",
                auth=True,
            )
        except BrokerError as exc:
            if self._is_position_not_found_error_text(str(exc)):
                return False
            return None
        except Exception:
            return None
        if not isinstance(body, dict):
            return True
        position_payload = _as_mapping(body.get("position"))
        deal_id = str(
            position_payload.get("dealId")
            or body.get("dealId")
            or ""
        ).strip()
        if deal_id and deal_id != normalized_position_id:
            return None
        return True

    def get_position_close_sync(
        self,
        position_id: str,
        *,
        deal_reference: str | None = None,
        opened_at: float | None = None,
        symbol: str | None = None,
        tick_size: float | None = None,
        include_history: bool = True,
        **_: Any,
    ) -> dict[str, Any] | None:
        self._maybe_cleanup_internal_caches()
        key = str(position_id)
        with self._lock:
            cached_payload = self._position_close_sync.get(key)
        if isinstance(cached_payload, dict):
            payload = dict(cached_payload)
            source = str(payload.get("source") or "").strip().lower()
            if source != "ig_confirm" and self._close_sync_has_factual_details(payload):
                if include_history and not self._close_sync_has_realized_pnl(payload):
                    with self._lock:
                        self._position_close_sync.pop(key, None)
                else:
                    return payload
            if source == "ig_confirm":
                close_complete = payload.get("close_complete")
                if isinstance(close_complete, bool) and (not close_complete):
                    with self._lock:
                        self._position_close_sync.pop(key, None)
                    return payload
                open_state = self._probe_position_open_state_by_deal_id(key)
                if open_state is True:
                    return None
                if include_history:
                    history_payload = self._fetch_close_sync_from_history(
                        key,
                        deal_reference=deal_reference,
                        opened_at=opened_at,
                        symbol=symbol,
                        tick_size=tick_size,
                    )
                    if history_payload is None:
                        history_payload = self._fetch_close_sync_from_activity(
                            key,
                            deal_reference=deal_reference,
                            opened_at=opened_at,
                            symbol=symbol,
                        )
                        history_payload = self._complete_close_sync_from_activity_hints(
                            key,
                            history_payload,
                            deal_reference=deal_reference,
                            opened_at=opened_at,
                            symbol=symbol,
                            tick_size=tick_size,
                        )
                    if history_payload is not None:
                        if open_state is False:
                            history_payload["position_found"] = False
                        with self._lock:
                            self._position_close_sync.pop(key, None)
                        if self._close_sync_has_realized_pnl(history_payload):
                            return self._cache_close_sync_payload(key, history_payload)
                        return history_payload
                if open_state is False:
                    payload["position_found"] = False
                    with self._lock:
                        self._position_close_sync.pop(key, None)
                    return payload
                return None
            with self._lock:
                self._position_close_sync.pop(key, None)
            return payload
        open_state = self._probe_position_open_state_by_deal_id(key)
        if open_state is True:
            return None
        if not include_history:
            if open_state is False:
                return {
                    "position_id": key,
                    "source": "ig_positions_deal_id",
                    "position_found": False,
                }
            return None
        history_payload = self._fetch_close_sync_from_history(
            key,
            deal_reference=deal_reference,
            opened_at=opened_at,
            symbol=symbol,
            tick_size=tick_size,
        )
        if history_payload is None:
            history_payload = self._fetch_close_sync_from_activity(
                key,
                deal_reference=deal_reference,
                opened_at=opened_at,
                symbol=symbol,
            )
            history_payload = self._complete_close_sync_from_activity_hints(
                key,
                history_payload,
                deal_reference=deal_reference,
                opened_at=opened_at,
                symbol=symbol,
                tick_size=tick_size,
            )
        if history_payload is None:
            if open_state is False:
                return {
                    "position_id": key,
                    "source": "ig_positions_deal_id",
                    "position_found": False,
                }
            return None
        if open_state is False:
            history_payload["position_found"] = False
        if self._close_sync_has_realized_pnl(history_payload):
            return self._cache_close_sync_payload(key, history_payload)
        return history_payload

    def get_position_open_sync(self, position_id: str) -> dict[str, Any] | None:
        self._maybe_cleanup_internal_caches()
        key = str(position_id)
        with self._lock:
            payload = self._position_open_sync.pop(key, None)
        if payload is not None:
            return dict(payload)

        # Best-effort fallback: confirm payload can be sparse in some IG environments.
        # Query open positions by deal id to retrieve factual stop/limit/open levels.
        cooldown_remaining = self._allowance_cooldown_remaining()
        if cooldown_remaining > 0:
            return None
        try:
            body, _ = self._request("GET", "/positions", version="2", auth=True)
            self._reset_allowance_cooldown()
        except BrokerError as exc:
            self._maybe_start_allowance_cooldown(str(exc), "open sync fallback")
            return None

        for row in self._extract_open_positions_rows(body):
            position_payload = _as_mapping(row.get("position"))
            deal_id = str(
                position_payload.get("dealId")
                or position_payload.get("positionId")
                or row.get("dealId")
                or ""
            ).strip()
            if not deal_id or deal_id != key:
                continue

            sync_payload: dict[str, Any] = {
                "position_id": key,
                "source": "ig_positions_deal_id",
            }
            open_price = _as_float(
                position_payload.get("level")
                or position_payload.get("openLevel")
                or row.get("level"),
                0.0,
            )
            stop_loss = _as_float(position_payload.get("stopLevel"), 0.0)
            take_profit = _as_float(position_payload.get("limitLevel"), 0.0)
            deal_reference = str(
                position_payload.get("dealReference")
                or position_payload.get("reference")
                or row.get("dealReference")
                or row.get("reference")
                or ""
            ).strip()
            if open_price > 0:
                sync_payload["open_price"] = open_price
            if stop_loss > 0:
                sync_payload["stop_loss"] = stop_loss
            if take_profit > 0:
                sync_payload["take_profit"] = take_profit
            if deal_reference:
                sync_payload["deal_reference"] = deal_reference
            return sync_payload
        return None

    def mark_symbol_guaranteed_stop_required(self, symbol: str, source: str | None = None) -> None:
        self._mark_symbol_guaranteed_stop_required(
            symbol,
            source=str(source or "runtime"),
        )

    def get_public_api_backoff_remaining_sec(self) -> float:
        return self._allowance_cooldown_remaining()

    def get_market_data_wait_remaining_sec(self) -> float:
        interval = _as_float(self.rest_market_min_interval_sec, 0.0)
        if not math.isfinite(interval) or interval <= 0:
            return 0.0
        interval = max(0.0, interval)
        if interval <= 0:
            return 0.0
        with self._lock:
            elapsed = time.monotonic() - self._last_market_rest_request_monotonic
        remaining = interval - elapsed
        if not math.isfinite(remaining) or remaining <= 0:
            return 0.0
        return remaining

    def get_stream_rest_fallback_block_remaining_sec(self, symbol: str) -> float:
        return self._stream_rest_fallback_block_remaining(symbol)

    def _fetch_open_confirm_once(self, deal_reference: str) -> dict[str, Any] | None:
        reference = str(deal_reference).strip()
        if not reference:
            return None
        try:
            confirm, _ = self._request(
                "GET",
                f"/confirms/{reference}",
                version="1",
                auth=True,
            )
        except BrokerError:
            return None
        if not isinstance(confirm, dict):
            return None
        return confirm

    def _symbols_for_epic(self, epic: str, preferred_symbols: list[str] | None = None) -> list[str]:
        normalized_epic = str(epic).strip().upper()
        if not normalized_epic:
            return []

        preferred = {str(symbol).strip().upper() for symbol in (preferred_symbols or []) if str(symbol).strip()}
        candidates: list[str] = []
        seen: set[str] = set()

        def _append(symbol: str) -> None:
            text = str(symbol).strip().upper()
            if not text or text in seen:
                return
            candidates.append(text)
            seen.add(text)

        with self._lock:
            active_map = dict(self._epics)
            candidate_map = {key: list(values) for key, values in self._epic_candidates.items()}

        for symbol, active_epic in active_map.items():
            if str(active_epic).strip().upper() == normalized_epic and (not preferred or symbol in preferred):
                _append(symbol)
        for symbol, epics in candidate_map.items():
            if normalized_epic in {str(value).strip().upper() for value in epics} and (not preferred or symbol in preferred):
                _append(symbol)
        for symbol, active_epic in active_map.items():
            if str(active_epic).strip().upper() == normalized_epic:
                _append(symbol)
        for symbol, epics in candidate_map.items():
            if normalized_epic in {str(value).strip().upper() for value in epics}:
                _append(symbol)
        return candidates

    def _symbol_for_epic(self, epic: str, preferred_symbols: list[str] | None = None) -> str | None:
        candidates = self._symbols_for_epic(epic, preferred_symbols=preferred_symbols)
        return next((candidate for candidate in candidates if str(candidate).strip()), None)

    @staticmethod
    def _extract_open_positions_rows(body: dict[str, Any]) -> list[dict[str, Any]]:
        rows = body.get("positions")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        data = body.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return []

    @staticmethod
    def _parse_position_opened_at(position_payload: dict[str, Any], now_ts: float) -> float:
        raw_candidates = (
            position_payload.get("createdDateUTC"),
            position_payload.get("createdDate"),
            position_payload.get("openDateUTC"),
            position_payload.get("openDate"),
            position_payload.get("updateTimeUTC"),
            position_payload.get("updateTime"),
        )
        for raw in raw_candidates:
            if raw in (None, ""):
                continue
            numeric_ts = _as_float(raw, float("nan"))
            if numeric_ts == numeric_ts and numeric_ts > 0:
                if numeric_ts > 10_000_000_000_000:
                    return numeric_ts / 1_000_000.0
                if numeric_ts > 10_000_000_000:
                    return numeric_ts / 1_000.0
                return numeric_ts

            text = str(raw).strip()
            iso_text = text.replace("Z", "+00:00") if text.endswith("Z") else text
            try:
                parsed = datetime.fromisoformat(iso_text)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.timestamp()
            except ValueError:
                pass
            for fmt in (
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S %Z",
            ):
                try:
                    parsed = datetime.strptime(text, fmt)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    return parsed.timestamp()
                except ValueError:
                    continue
        return now_ts

    def get_managed_open_positions(
        self,
        magic_prefix: str,
        magic_instance: str,
        preferred_symbols: list[str] | None = None,
        known_deal_references: list[str] | None = None,
        known_position_ids: list[str] | None = None,
        pending_opens: list[PendingOpen] | None = None,
        include_unmatched_preferred: bool = False,
    ) -> dict[str, Position]:
        self._maybe_cleanup_internal_caches()
        _ = (magic_prefix, magic_instance)
        cooldown_remaining = self._allowance_cooldown_remaining()
        if cooldown_remaining > 0:
            raise BrokerError(
                f"IG public API allowance cooldown is active ({cooldown_remaining:.1f}s remaining)"
            )

        try:
            body, _ = self._request("GET", "/positions", version="2", auth=True)
            self._reset_allowance_cooldown()
        except BrokerError as exc:
            self._maybe_start_allowance_cooldown(str(exc), "managed positions")
            raise
        now_ts = time.time()
        preferred_set = {str(symbol).strip().upper() for symbol in (preferred_symbols or []) if str(symbol).strip()}
        known_ids = {str(value).strip() for value in (known_position_ids or []) if str(value).strip()}
        known_references = {
            _sanitize_deal_reference(str(value or ""))
            for value in (known_deal_references or [])
            if str(value or "").strip()
        }
        known_references.discard("")
        pending_items = list(pending_opens or [])
        pending_position_ids: set[str] = set()
        pending_by_reference: dict[str, PendingOpen] = {}
        pending_confirm_probes = 0
        max_pending_confirm_probes = 1
        can_probe_pending_confirms = self._allowance_cooldown_remaining() <= 0.0
        for pending in pending_items:
            normalized_pending_id = _sanitize_deal_reference(str(pending.pending_id or ""))
            if normalized_pending_id:
                pending_by_reference[normalized_pending_id] = pending
            if pending.position_id:
                pending_position_ids.add(str(pending.position_id).strip())
                continue
            if not normalized_pending_id or not can_probe_pending_confirms:
                continue
            if pending_confirm_probes >= max_pending_confirm_probes:
                continue
            pending_age_sec = max(0.0, now_ts - float(pending.created_at))
            if pending_age_sec > 300.0:
                continue
            with self._lock:
                last_probe_ts = float(self._pending_confirm_last_probe_ts.get(normalized_pending_id) or 0.0)
            if (now_ts - last_probe_ts) < self._pending_confirm_probe_min_interval_sec:
                continue
            with self._lock:
                self._pending_confirm_last_probe_ts[normalized_pending_id] = now_ts
                if len(self._pending_confirm_last_probe_ts) > 512:
                    cutoff = now_ts - 3600.0
                    stale_keys = [
                        key
                        for key, ts in self._pending_confirm_last_probe_ts.items()
                        if float(ts) < cutoff
                    ]
                    for key in stale_keys:
                        self._pending_confirm_last_probe_ts.pop(key, None)
            pending_confirm_probes += 1
            confirm = self._fetch_open_confirm_once(normalized_pending_id)
            if not confirm:
                continue
            status = str(confirm.get("dealStatus") or "").strip().upper()
            if status != "ACCEPTED":
                continue
            confirmed_deal_id = str(confirm.get("dealId") or "").strip()
            if confirmed_deal_id:
                pending_position_ids.add(confirmed_deal_id)
                pending.position_id = confirmed_deal_id
        restored: dict[str, Position] = {}

        for row in self._extract_open_positions_rows(body):
            position_payload = _as_mapping(row.get("position"))
            market_payload = _as_mapping(row.get("market"))
            deal_id = str(
                position_payload.get("dealId")
                or position_payload.get("positionId")
                or row.get("dealId")
                or ""
            ).strip()
            raw_reference = (
                position_payload.get("dealReference")
                or position_payload.get("reference")
                or row.get("dealReference")
                or row.get("reference")
            )
            normalized_reference = _sanitize_deal_reference(str(raw_reference or ""))
            managed_by_reference = bool(normalized_reference) and any(
                _deal_reference_matches(reference, normalized_reference) for reference in known_references
            )
            managed_by_known_id = bool(deal_id) and deal_id in known_ids

            epic = str(
                market_payload.get("epic")
                or position_payload.get("epic")
                or row.get("epic")
                or ""
            ).strip().upper()
            symbol = self._symbol_for_epic(epic, preferred_symbols=preferred_symbols)
            if symbol is None:
                logger.warning(
                    "IG managed open position skipped: cannot map epic=%s deal_reference=%s to symbol",
                    epic or "-",
                    normalized_reference or "-",
                )
                continue

            if not deal_id:
                continue

            direction = str(position_payload.get("direction") or row.get("direction") or "").strip().upper()
            if direction == "BUY":
                side = Side.BUY
            elif direction == "SELL":
                side = Side.SELL
            else:
                continue

            open_price = _as_float(
                position_payload.get("level")
                or position_payload.get("openLevel")
                or row.get("level"),
                0.0,
            )
            volume = _as_float(position_payload.get("size") or row.get("size"), 0.0)
            stop_loss = _as_float(position_payload.get("stopLevel"), 0.0)
            take_profit = _as_float(position_payload.get("limitLevel"), 0.0)
            opened_at = self._parse_position_opened_at(position_payload, now_ts)
            if open_price <= 0 or volume <= 0:
                continue

            managed_by_pending_position_id = bool(deal_id) and deal_id in pending_position_ids
            pending_match = pending_by_reference.get(normalized_reference) if normalized_reference else None
            if pending_match is None and normalized_reference:
                for pending_reference, candidate_pending in pending_by_reference.items():
                    if _deal_reference_matches(pending_reference, normalized_reference):
                        pending_match = candidate_pending
                        break
            managed_by_pending_reference = pending_match is not None
            if pending_match is not None and not pending_match.position_id:
                pending_match.position_id = deal_id
                pending_position_ids.add(deal_id)
            matched_managed_identity = (
                managed_by_reference
                or managed_by_known_id
                or managed_by_pending_position_id
                or managed_by_pending_reference
            )
            if not matched_managed_identity:
                if not include_unmatched_preferred:
                    continue
                if preferred_set and symbol not in preferred_set:
                    continue

            restored[deal_id] = Position(
                position_id=deal_id,
                symbol=symbol,
                side=side,
                volume=volume,
                open_price=open_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                opened_at=opened_at,
                status="open",
                epic=epic or None,
                epic_variant=_epic_variant_key(epic) if epic else None,
            )

        return restored

    @staticmethod
    def _timestamp_from_utc_day_seconds(now_ts: float, seconds_of_day: float) -> float:
        seconds = max(0.0, min(float(seconds_of_day), 86_399.999))
        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        day_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        # IG often sends time-of-day without date. Prefer the latest candidate that is
        # not significantly in the future to avoid treating stale quotes as fresh.
        candidates = [
            (day_start - 24 * 60 * 60) + seconds,
            day_start + seconds,
            (day_start + 24 * 60 * 60) + seconds,
        ]
        max_future_sec = 120.0
        allowed = [candidate for candidate in candidates if candidate <= (now_ts + max_future_sec)]
        if allowed:
            return max(allowed)
        return min(candidates, key=lambda candidate: abs(candidate - now_ts))

    @staticmethod
    def _parse_time_of_day_seconds(raw: Any) -> float | None:
        text = str(raw or "").strip()
        if not text:
            return None

        # Common IG textual formats.
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                parsed = datetime.strptime(text, fmt)
            except ValueError:
                continue
            return float(parsed.hour * 3600 + parsed.minute * 60 + parsed.second)

        compact = text
        if "." in compact:
            head, _, tail = compact.partition(".")
            if tail and set(tail) <= {"0"}:
                compact = head
            else:
                return None
        if not compact.isdigit():
            return None

        # Numeric compact forms: HHMM / HHMMSS.
        if len(compact) <= 2:
            seconds = int(compact)
            if seconds < 60:
                return float(seconds)
            return None
        if len(compact) <= 4:
            hhmm = compact.rjust(4, "0")
            hh = int(hhmm[:2])
            mm = int(hhmm[2:])
            if hh < 24 and mm < 60:
                return float(hh * 3600 + mm * 60)
            return None
        if len(compact) <= 6:
            hhmmss = compact.rjust(6, "0")
            hh = int(hhmmss[:2])
            mm = int(hhmmss[2:4])
            ss = int(hhmmss[4:])
            if hh < 24 and mm < 60 and ss < 60:
                return float(hh * 3600 + mm * 60 + ss)
            return None
        return None

    @staticmethod
    def _parse_ts(snapshot: dict[str, Any], now_ts: float) -> float:
        raw = snapshot.get("updateTimeUTC") or snapshot.get("updateTime")
        if not raw:
            return now_ts
        time_of_day_seconds = IgApiClient._parse_time_of_day_seconds(raw)
        if time_of_day_seconds is not None:
            return IgApiClient._timestamp_from_utc_day_seconds(now_ts, time_of_day_seconds)
        numeric_ts = _as_float(raw, float("nan"))
        if math.isfinite(numeric_ts) and numeric_ts > 0:
            if numeric_ts > 10_000_000_000_000:  # microseconds
                return numeric_ts / 1_000_000.0
            if numeric_ts > 10_000_000_000:  # milliseconds
                return numeric_ts / 1_000.0
            if numeric_ts > 1_000_000_000:  # seconds since epoch
                return numeric_ts

            if numeric_ts < 86_400:
                return IgApiClient._timestamp_from_utc_day_seconds(now_ts, numeric_ts)
            if numeric_ts < 86_400_000:
                return IgApiClient._timestamp_from_utc_day_seconds(now_ts, numeric_ts / 1000.0)
            return numeric_ts
        text = str(raw).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%H:%M:%S",
            "%H:%M",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                if fmt in {"%H:%M:%S", "%H:%M"}:
                    day_seconds = (parsed.hour * 3600) + (parsed.minute * 60) + parsed.second
                    return IgApiClient._timestamp_from_utc_day_seconds(now_ts, day_seconds)
                return parsed.replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
        return now_ts

    def _tick_from_market_body(self, symbol: str, payload: dict[str, Any]) -> PriceTick | None:
        upper = str(symbol).upper().strip()
        snapshot = _as_mapping(payload.get("snapshot"))
        market = _as_mapping(payload.get("instrument")) or _as_mapping(payload.get("market"))

        now_ts = time.time()
        bid = _first_positive_float(snapshot, "bid", "bidPrice", "bidPrice1", "buy", "buyPrice")
        ask = _first_positive_float(
            snapshot,
            "offer",
            "ask",
            "offerPrice",
            "askPrice",
            "offerPrice1",
            "sell",
            "sellPrice",
        )
        if bid <= 0 and ask > 0:
            bid = ask
        if ask <= 0 and bid > 0:
            ask = bid
        if bid <= 0 and ask <= 0:
            last_price = _first_positive_float(
                snapshot,
                "lastTraded",
                "lastTradedPrice",
                "lastPrice",
                "last",
                "mid",
            )
            if last_price <= 0:
                last_price = _first_positive_float(
                    market,
                    "lastTraded",
                    "lastTradedPrice",
                    "lastPrice",
                    "last",
                )
            if last_price <= 0:
                return None
            bid = last_price
            ask = last_price

        tick_ts = self._parse_ts(snapshot, now_ts)
        volume_raw = snapshot.get(
            "lastTradedVolume",
            snapshot.get("volume", snapshot.get("lastTradedVolumeInContracts")),
        )
        volume = _as_float(volume_raw, 0.0)
        return PriceTick(
            symbol=upper,
            bid=bid,
            ask=ask,
            timestamp=tick_ts,
            volume=volume if volume > 0 else None,
            received_at=now_ts,
        )

    @staticmethod
    def _spec_min_stop_distance_price(spec: SymbolSpec | None) -> float:
        if spec is None:
            return 0.0
        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        return max(0.0, _as_float(metadata.get("min_stop_distance_price"), 0.0))

    @staticmethod
    def _extract_quote_id_from_market_body(body: dict[str, Any]) -> str:
        snapshot = _as_mapping(body.get("snapshot"))
        for key in ("quoteId", "quoteID", "quote_id"):
            value = str(snapshot.get(key) or body.get(key) or "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _is_guaranteed_stop_required_text(value: str) -> bool:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return False
        markers = (
            "guaranteed",
            "controlled_risk",
            "controlled risk",
            "limited_risk",
            "limited risk",
            "guarantee",
        )
        return any(marker in normalized for marker in markers)

    def _mark_symbol_guaranteed_stop_required(self, symbol: str, *, source: str) -> None:
        upper = str(symbol).strip().upper()
        if not upper:
            return
        now_ts = time.time()
        with self._lock:
            already_marked = upper in self._guaranteed_stop_required_symbols
            self._guaranteed_stop_required_symbols.add(upper)
            cached_specs: list[SymbolSpec] = []
            symbol_spec = self._symbol_spec_cache.get(upper)
            if symbol_spec is not None:
                cached_specs.append(symbol_spec)
            cached_specs.extend(
                spec
                for (cached_symbol, _), spec in self._symbol_spec_cache_by_epic.items()
                if cached_symbol == upper and spec not in cached_specs
            )
            for spec in cached_specs:
                metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
                metadata["guaranteed_stop_required"] = True
                metadata["guaranteed_stop_required_source"] = source
                metadata["guaranteed_stop_required_ts"] = now_ts
                spec.metadata = metadata
        if not already_marked:
            logger.warning(
                "IG guaranteed stop requirement learned for %s (source=%s)",
                upper,
                source,
            )

    def _should_use_guaranteed_stop(self, symbol: str, spec: SymbolSpec | None) -> bool:
        mode = str(self._guaranteed_stop_mode).strip().lower()
        if mode == "off":
            return False
        if mode == "required":
            return True
        upper = str(symbol).strip().upper()
        with self._lock:
            if upper in self._guaranteed_stop_required_symbols:
                return True
        metadata = spec.metadata if isinstance(getattr(spec, "metadata", None), dict) else {}
        return _as_bool(metadata.get("guaranteed_stop_required"), False)

    def _effective_min_stop_distance_for_open(self, spec: SymbolSpec, use_guaranteed_stop: bool) -> float:
        metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
        normal_distance = max(0.0, _as_float(metadata.get("min_stop_distance_price"), 0.0))
        if not use_guaranteed_stop:
            return normal_distance
        controlled_distance = max(
            0.0,
            _as_float(metadata.get("min_controlled_risk_stop_distance_price"), 0.0),
        )
        return max(normal_distance, controlled_distance)

    def _enforce_open_stop_distance(
        self,
        *,
        side: Side,
        entry: float | None,
        stop_loss: float,
        spec: SymbolSpec,
        min_distance: float,
    ) -> float:
        if entry is None or min_distance <= 0:
            return stop_loss
        normalized_entry = float(entry)
        normalized_stop = float(stop_loss)
        if side == Side.BUY:
            max_stop = _normalize_price_floor_for_spec(spec, normalized_entry - min_distance)
            if normalized_stop >= normalized_entry or normalized_stop > max_stop:
                normalized_stop = max_stop
        else:
            min_stop = _normalize_price_ceil_for_spec(spec, normalized_entry + min_distance)
            if normalized_stop <= normalized_entry or normalized_stop < min_stop:
                normalized_stop = min_stop
        return float(normalized_stop)

    def _open_level_with_tolerance(self, *, side: Side, entry: float | None, spec: SymbolSpec) -> float | None:
        if entry is None:
            return None
        tolerance_pips = float(self._open_level_tolerance_pips)
        if not math.isfinite(tolerance_pips) or tolerance_pips <= 0:
            return None
        distance = tolerance_pips * max(float(spec.tick_size), FLOAT_COMPARISON_TOLERANCE)
        if side == Side.BUY:
            return float(_normalize_price_ceil_for_spec(spec, float(entry) + distance))
        return float(_normalize_price_floor_for_spec(spec, float(entry) - distance))

    def _build_open_sync_payload(
        self,
        position_id: str,
        confirm: dict[str, Any] | None,
        *,
        deal_reference: str | None = None,
        open_price: float,
        stop_loss: float,
        take_profit: float,
        epic: str,
    ) -> dict[str, Any]:
        sync: dict[str, Any] = {
            "position_id": position_id,
            "source": "ig_confirm" if isinstance(confirm, dict) else "ig_estimated",
            "epic": epic,
        }
        normalized_reference = str(
            deal_reference or _as_mapping(confirm).get("dealReference") or ""
        ).strip()
        if normalized_reference:
            sync["deal_reference"] = normalized_reference
        confirmed_open = _as_float(_as_mapping(confirm).get("level"), 0.0)
        confirmed_stop = _as_float(_as_mapping(confirm).get("stopLevel"), 0.0)
        confirmed_take = _as_float(_as_mapping(confirm).get("limitLevel"), 0.0)
        sync["open_price"] = confirmed_open if confirmed_open > 0 else open_price
        sync["stop_loss"] = confirmed_stop if confirmed_stop > 0 else stop_loss
        sync["take_profit"] = confirmed_take if confirmed_take > 0 else take_profit
        return sync

    def _cache_open_sync_payload(self, position_id: str, sync_payload: dict[str, Any]) -> None:
        payload = dict(sync_payload)
        payload["_cache_ts"] = time.time()
        with self._lock:
            self._position_open_sync[str(position_id)] = payload

    def _recover_open_position_after_confirm_timeout(
        self,
        *,
        symbol: str,
        side: Side,
        volume: float,
        epic: str,
        deal_reference: str,
        fallback_open_price: float,
        fallback_stop_loss: float,
        fallback_take_profit: float,
        direction: str,
        currency_code: str,
        auto_discovered: bool = False,
    ) -> str:
        late_reject_error: BrokerError | None = None
        late_confirm = self._fetch_open_confirm_once(deal_reference)
        if late_confirm is not None:
            late_status = str(late_confirm.get("dealStatus") or "").strip().upper()
            late_reason = self._confirm_rejection_reason(late_confirm)
            if late_status == "ACCEPTED":
                position_id = str(late_confirm.get("dealId") or "").strip()
                if position_id:
                    self._reset_allowance_cooldown()
                    self._activate_epic(symbol, epic)
                    if auto_discovered:
                        logger.warning(
                            "IG open_position epic auto-discovered for %s: using %s",
                            str(symbol).upper(),
                            epic,
                        )
                    sync_payload = self._build_open_sync_payload(
                        position_id,
                        late_confirm,
                        deal_reference=deal_reference,
                        open_price=float(fallback_open_price),
                        stop_loss=float(fallback_stop_loss),
                        take_profit=float(fallback_take_profit),
                        epic=epic,
                    )
                    self._cache_open_sync_payload(position_id, sync_payload)
                    return position_id
            elif late_status == "REJECTED":
                self._maybe_start_allowance_cooldown(late_reason, "trade open confirm timeout")
                if self._is_instrument_invalid_error_text(late_reason):
                    self._mark_epic_temporarily_invalid(symbol, epic, reason=late_reason)
                confirm_summary = self._compact_confirm_payload(late_confirm)
                late_reject_error = BrokerError(
                    "IG deal rejected after confirm timeout: "
                    f"{late_reason or 'UNKNOWN'} | epic={epic} direction={direction} "
                    f"size={float(volume):g} currency={currency_code} "
                    f"stop={float(fallback_stop_loss):g} limit={float(fallback_take_profit):g} "
                    f"confirm={confirm_summary}"
                )

        recovered_open = self._recover_open_position_after_rejected_confirm(
            symbol=symbol,
            side=side,
            volume=float(volume),
            epic=epic,
            deal_reference=deal_reference,
            fallback_open_price=float(fallback_open_price),
            fallback_stop_loss=float(fallback_stop_loss),
            fallback_take_profit=float(fallback_take_profit),
        )
        if recovered_open is not None:
            self._reset_allowance_cooldown()
            recovered_position_id = str(recovered_open.get("position_id") or "").strip()
            if recovered_position_id:
                self._activate_epic(symbol, epic)
                if auto_discovered:
                    logger.warning(
                        "IG open_position epic auto-discovered for %s: using %s",
                        str(symbol).upper(),
                        epic,
                    )
                sync_payload = self._build_open_sync_payload(
                    recovered_position_id,
                    None,
                    deal_reference=str(recovered_open.get("deal_reference") or deal_reference).strip(),
                    open_price=float(recovered_open.get("open_price") or 0.0),
                    stop_loss=float(recovered_open.get("stop_loss") or fallback_stop_loss),
                    take_profit=float(recovered_open.get("take_profit") or fallback_take_profit),
                    epic=str(recovered_open.get("epic") or epic),
                )
                sync_payload["source"] = str(
                    recovered_open.get("source") or "ig_positions_after_confirm_timeout"
                )
                self._cache_open_sync_payload(recovered_position_id, sync_payload)
                return recovered_position_id

        if late_reject_error is not None:
            raise late_reject_error
        raise BrokerError(
            "IG open position confirm timed out before dealId was available; "
            f"pending recovery required | deal_reference={deal_reference} epic={epic} "
            f"direction={direction} size={float(volume):g} currency={currency_code} "
            f"stop={float(fallback_stop_loss):g} limit={float(fallback_take_profit):g}"
        )

    def _recover_open_position_after_rejected_confirm(
        self,
        *,
        symbol: str,
        side: Side,
        volume: float,
        epic: str,
        deal_reference: str,
        fallback_open_price: float,
        fallback_stop_loss: float,
        fallback_take_profit: float,
    ) -> dict[str, Any] | None:
        normalized_reference = _sanitize_deal_reference(str(deal_reference or "").strip())
        if not normalized_reference:
            return None

        expected_symbol = str(symbol).strip().upper()
        expected_epic = str(epic).strip().upper()
        expected_direction = "BUY" if side == Side.BUY else "SELL"
        expected_volume = max(0.0, float(volume))
        volume_tolerance = max(0.001, expected_volume * 0.001)
        deadline = time.time() + min(3.0, max(1.0, float(self.confirm_timeout_sec)))

        while True:
            try:
                body, _ = self._request("GET", "/positions", version="2", auth=True)
            except BrokerError:
                if time.time() >= deadline:
                    return None
                time.sleep(0.25)
                continue

            for row in self._extract_open_positions_rows(body):
                position_payload = _as_mapping(row.get("position"))
                market_payload = _as_mapping(row.get("market"))
                raw_reference = (
                    position_payload.get("dealReference")
                    or position_payload.get("reference")
                    or row.get("dealReference")
                    or row.get("reference")
                )
                if not _deal_reference_matches(normalized_reference, str(raw_reference or "")):
                    continue

                deal_id = str(
                    position_payload.get("dealId")
                    or position_payload.get("positionId")
                    or row.get("dealId")
                    or ""
                ).strip()
                if not deal_id:
                    continue

                row_epic = str(
                    market_payload.get("epic")
                    or position_payload.get("epic")
                    or row.get("epic")
                    or ""
                ).strip().upper()
                if expected_epic and row_epic and row_epic != expected_epic:
                    continue

                row_symbol = self._symbol_for_epic(row_epic, preferred_symbols=[expected_symbol]) if row_epic else None
                if row_symbol is not None and row_symbol != expected_symbol:
                    continue

                row_direction = str(
                    position_payload.get("direction")
                    or row.get("direction")
                    or ""
                ).strip().upper()
                if row_direction and row_direction != expected_direction:
                    continue

                row_volume = _as_float(position_payload.get("size") or row.get("size"), 0.0)
                if row_volume > 0 and not math.isclose(
                    row_volume,
                    expected_volume,
                    rel_tol=0.0,
                    abs_tol=volume_tolerance,
                ):
                    continue

                open_price = _as_float(
                    position_payload.get("level")
                    or position_payload.get("openLevel")
                    or row.get("level"),
                    0.0,
                )
                stop_loss = _as_float(position_payload.get("stopLevel"), 0.0)
                take_profit = _as_float(position_payload.get("limitLevel"), 0.0)

                return {
                    "position_id": deal_id,
                    "deal_reference": normalized_reference,
                    "open_price": open_price if open_price > 0 else float(fallback_open_price),
                    "stop_loss": stop_loss if stop_loss > 0 else float(fallback_stop_loss),
                    "take_profit": take_profit if take_profit > 0 else float(fallback_take_profit),
                    "epic": row_epic or expected_epic,
                    "source": "ig_positions_after_rejected_confirm",
                }

            if time.time() >= deadline:
                return None
            time.sleep(0.25)

    def _remap_open_levels_for_candidate(
        self,
        *,
        symbol: str,
        side: Side,
        entry_price: float | None,
        requested_stop_loss: float,
        requested_take_profit: float,
        reference_spec: SymbolSpec | None,
        candidate_spec: SymbolSpec,
        candidate_body: dict[str, Any],
    ) -> tuple[float | None, float, float]:
        candidate_tick = self._tick_from_market_body(symbol, candidate_body)
        if candidate_tick is None:
            return entry_price, requested_stop_loss, requested_take_profit

        candidate_entry = float(candidate_tick.ask if side == Side.BUY else candidate_tick.bid)
        if entry_price is None or entry_price <= 0 or reference_spec is None:
            return candidate_entry, requested_stop_loss, requested_take_profit

        reference_tick = max(float(reference_spec.tick_size), FLOAT_COMPARISON_TOLERANCE)
        candidate_tick_size = max(float(candidate_spec.tick_size), FLOAT_COMPARISON_TOLERANCE)
        stop_distance_points = abs(float(entry_price) - float(requested_stop_loss)) / reference_tick
        take_distance_points = abs(float(requested_take_profit) - float(entry_price)) / reference_tick

        if side == Side.BUY:
            stop_loss = candidate_entry - (stop_distance_points * candidate_tick_size)
            take_profit = candidate_entry + (take_distance_points * candidate_tick_size)
            stop_loss = _normalize_price_floor_for_spec(candidate_spec, stop_loss)
            take_profit = _normalize_price_ceil_for_spec(candidate_spec, take_profit)
        else:
            stop_loss = candidate_entry + (stop_distance_points * candidate_tick_size)
            take_profit = candidate_entry - (take_distance_points * candidate_tick_size)
            stop_loss = _normalize_price_ceil_for_spec(candidate_spec, stop_loss)
            take_profit = _normalize_price_floor_for_spec(candidate_spec, take_profit)

        min_stop_distance = self._spec_min_stop_distance_price(candidate_spec)
        if min_stop_distance > 0:
            bid = float(candidate_tick.bid)
            ask = float(candidate_tick.ask)
            if side == Side.BUY:
                max_allowed_stop = _normalize_price_floor_for_spec(candidate_spec, bid - min_stop_distance)
                min_allowed_take_profit = _normalize_price_ceil_for_spec(candidate_spec, ask + min_stop_distance)
                stop_loss = _normalize_price_floor_for_spec(candidate_spec, min(stop_loss, max_allowed_stop))
                take_profit = _normalize_price_ceil_for_spec(candidate_spec, max(take_profit, min_allowed_take_profit))
            else:
                min_allowed_stop = _normalize_price_ceil_for_spec(candidate_spec, ask + min_stop_distance)
                max_allowed_take_profit = _normalize_price_floor_for_spec(candidate_spec, bid - min_stop_distance)
                stop_loss = _normalize_price_ceil_for_spec(candidate_spec, max(stop_loss, min_allowed_stop))
                take_profit = _normalize_price_floor_for_spec(candidate_spec, min(take_profit, max_allowed_take_profit))

        # Final safety guard: keep attached levels on the correct side of the entry,
        # even when broker metadata has missing/unstable min-stop fields.
        min_separation = max(candidate_tick_size, min_stop_distance if min_stop_distance > 0 else 0.0)
        if side == Side.BUY:
            max_stop = _normalize_price_floor_for_spec(candidate_spec, candidate_entry - min_separation)
            min_take = _normalize_price_ceil_for_spec(candidate_spec, candidate_entry + min_separation)
            stop_loss = _normalize_price_floor_for_spec(candidate_spec, min(stop_loss, max_stop))
            take_profit = _normalize_price_ceil_for_spec(candidate_spec, max(take_profit, min_take))
        else:
            min_stop = _normalize_price_ceil_for_spec(candidate_spec, candidate_entry + min_separation)
            max_take = _normalize_price_floor_for_spec(candidate_spec, candidate_entry - min_separation)
            stop_loss = _normalize_price_ceil_for_spec(candidate_spec, max(stop_loss, min_stop))
            take_profit = _normalize_price_floor_for_spec(candidate_spec, min(take_profit, max_take))

        return candidate_entry, stop_loss, take_profit

    @staticmethod
    def _decode_stream_field(encoded: str, previous: str | None) -> str | None:
        if encoded == "":
            return previous
        if encoded == "$":
            return ""
        if encoded == "#":
            return None
        if encoded.startswith("$$") or encoded.startswith("##"):
            return encoded[1:]
        if encoded[0] in {"$", "#"}:
            return encoded[1:]
        return encoded

    @staticmethod
    def _read_stream_line(stream_response: Any) -> str | None:
        raw = stream_response.readline()
        if raw is None:
            return None
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw)
        if text == "":
            return None
        return text.rstrip("\r\n")

    def _stream_backoff_delay(self, attempt: int) -> float:
        if attempt <= 0:
            return self._stream_backoff_base_sec
        return min(self._stream_backoff_max_sec, self._stream_backoff_base_sec * (2 ** (attempt - 1)))

    def _resolve_stream_control_url(self, control_address: str | None) -> str | None:
        stream_endpoint = self._stream_endpoint
        if not stream_endpoint:
            return None
        if not control_address:
            return f"{stream_endpoint}/control.txt"

        raw = control_address.strip().rstrip("/")
        if not raw:
            return f"{stream_endpoint}/control.txt"

        parsed_addr = parse.urlparse(raw)
        if parsed_addr.scheme:
            base = raw
        else:
            stream_scheme = parse.urlparse(stream_endpoint).scheme or "https"
            base = f"{stream_scheme}://{raw}"

        base = base.rstrip("/")
        if not base.endswith("/lightstreamer"):
            base = f"{base}/lightstreamer"
        return f"{base}/control.txt"

    def _mark_stream_disconnected_locked(self, error_text: str | None, schedule_retry: bool = True) -> None:
        response = self._stream_http_response
        self._stream_http_response = None
        if response is not None:
            try:
                response.close()
            except Exception:
                pass

        self._stream_session_id = None
        self._stream_control_url = None
        self._stream_keepalive_ms = None
        self._stream_symbol_to_table.clear()
        self._stream_table_to_symbol.clear()
        self._stream_table_field_values.clear()
        self._stream_last_disconnect_ts = time.time()

        if error_text:
            self._stream_last_error = error_text

        if schedule_retry:
            self._stream_reconnect_attempts += 1
            delay = self._stream_backoff_delay(self._stream_reconnect_attempts)
            self._stream_next_retry_at = time.time() + delay
        else:
            self._stream_reconnect_attempts = 0
            self._stream_next_retry_at = None

    def _mark_stream_connected_locked(
        self,
        session_id: str,
        control_url: str | None,
        keepalive_ms: int | None,
    ) -> None:
        was_reconnect = self._stream_last_disconnect_ts is not None
        self._stream_session_id = session_id
        self._stream_control_url = control_url
        self._stream_keepalive_ms = keepalive_ms
        self._stream_last_error = None
        self._stream_reconnect_attempts = 0
        self._stream_next_retry_at = None
        self._stream_last_reconnect_ts = time.time()

        if was_reconnect:
            self._stream_total_reconnects += 1
            logger.warning(
                "IG Lightstreamer reconnected | total_reconnects=%s",
                self._stream_total_reconnects,
            )

    def _on_stream_sdk_status_change(self, status: str) -> None:
        normalized = str(status or "").strip()
        lowered = normalized.lower()
        now_ts = time.time()
        should_resubscribe = False

        with self._lock:
            was_connected = bool(self._stream_sdk_connected)
            is_connected = lowered.startswith("connected")
            self._stream_sdk_status = normalized or None
            self._stream_sdk_connected = is_connected
            if is_connected:
                self._stream_session_id = "lightstreamer_sdk"
                self._stream_control_url = None
                self._stream_keepalive_ms = None
                self._stream_last_error = None
                self._stream_reconnect_attempts = 0
                self._stream_next_retry_at = None
                self._stream_last_reconnect_ts = now_ts
                if was_connected:
                    return
                if self._stream_last_disconnect_ts is not None:
                    self._stream_total_reconnects += 1
                    logger.warning(
                        "IG Lightstreamer SDK reconnected | total_reconnects=%s",
                        self._stream_total_reconnects,
                    )
                should_resubscribe = True
            else:
                self._stream_session_id = None
                self._stream_control_url = None
                self._stream_keepalive_ms = None
                if normalized:
                    self._stream_last_error = f"sdk_status:{lowered}"
                self._stream_symbol_to_table.clear()
                self._stream_table_to_symbol.clear()
                self._stream_table_field_values.clear()
                if was_connected:
                    self._stream_last_disconnect_ts = now_ts
                    self._stream_reconnect_attempts += 1
                    delay = self._stream_backoff_delay(self._stream_reconnect_attempts)
                    self._stream_next_retry_at = now_ts + delay

        if should_resubscribe:
            self._resubscribe_desired_symbols()

    def _on_stream_sdk_server_error(self, code: int, message: str) -> None:
        with self._lock:
            self._stream_last_error = f"sdk_server_error:{code}:{message}"
            self._stream_sdk_connected = False
            self._stream_session_id = None

    def _ensure_stream_sdk_symbol_table_locked(self, upper: str) -> int:
        self._stream_sdk_pending_subscriptions.discard(upper)
        table_id = self._stream_symbol_to_table.get(upper)
        if table_id is None:
            table_id = self._stream_next_table_id
            self._stream_next_table_id += 1
            self._stream_symbol_to_table[upper] = table_id
            self._stream_table_to_symbol[table_id] = upper
            self._stream_table_field_values[table_id] = [None] * len(LIGHTSTREAMER_PRICE_FIELDS)
        self._stream_last_error = None
        return table_id

    def _on_stream_sdk_subscription(self, symbol: str) -> None:
        upper = str(symbol).upper()
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)
            self._ensure_stream_sdk_symbol_table_locked(upper)

    def _on_stream_sdk_unsubscription(self, symbol: str) -> None:
        upper = str(symbol).upper()
        with self._lock:
            self._stream_sdk_pending_subscriptions.discard(upper)
            table_id = self._stream_symbol_to_table.pop(upper, None)
            if table_id is not None:
                self._stream_table_to_symbol.pop(table_id, None)
                self._stream_table_field_values.pop(table_id, None)

    def _on_stream_sdk_subscription_error(self, symbol: str, code: int, message: str) -> None:
        upper = str(symbol).upper()
        reason = f"error:{code}:{message}"
        failed_epic = ""
        with self._lock:
            self._stream_sdk_pending_subscriptions.discard(upper)
            self._stream_sdk_subscriptions.pop(upper, None)
            self._stream_sdk_subscription_listeners.pop(upper, None)
            table_id = self._stream_symbol_to_table.pop(upper, None)
            if table_id is not None:
                self._stream_table_to_symbol.pop(table_id, None)
                self._stream_table_field_values.pop(table_id, None)
            self._stream_last_error = f"subscription_failed:{upper}:{reason}"
            failed_epic = str(self._epics.get(upper) or "").strip().upper()

        if not self._is_stream_subscription_invalid_group_error_text(reason):
            return

        next_epic = self._promote_stream_subscription_epic_after_invalid_group(
            upper,
            failed_epic,
            reason=f"stream_subscription:{reason}",
        )
        if next_epic:
            return

    @staticmethod
    def _stream_sdk_value(update: Any, field_name: str) -> str | None:
        getter = getattr(update, "getValue", None)
        if callable(getter):
            try:
                value = getter(field_name)
            except Exception:
                value = None
            if value is not None:
                return str(value)
        fields_getter = getattr(update, "getFields", None)
        if callable(fields_getter):
            try:
                fields = fields_getter()
            except Exception:
                fields = None
            if isinstance(fields, dict):
                value = fields.get(field_name)
                if value is not None:
                    return str(value)
        return None

    def _on_stream_sdk_item_update(self, symbol: str, update: Any) -> None:
        upper = str(symbol).upper()
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)
            table_id = self._ensure_stream_sdk_symbol_table_locked(upper)
            previous_values = list(
                self._stream_table_field_values.get(table_id, [None] * len(LIGHTSTREAMER_PRICE_FIELDS))
            )
        decoded = list(previous_values)
        for idx, field_name in enumerate(LIGHTSTREAMER_PRICE_FIELDS):
            value = self._stream_sdk_value(update, field_name)
            if value is not None:
                decoded[idx] = value
        with self._lock:
            if table_id is not None:
                self._stream_table_field_values[table_id] = decoded
        self._update_tick_from_stream_fields(upper, decoded)

    def _start_stream_sdk_locked(self) -> None:
        if _LightstreamerClient is None or _LightstreamerSubscription is None:
            self._stream_last_error = "lightstreamer_sdk_not_installed"
            return
        if self._stream_sdk_client is not None:
            return
        if not self._stream_endpoint:
            self._stream_last_error = "lightstreamer_endpoint_missing"
            return
        if not self.account_id:
            self._stream_last_error = "lightstreamer_account_id_missing"
            return
        if not self._cst or not self._security_token:
            self._stream_last_error = "lightstreamer_auth_tokens_missing"
            return

        try:
            # IG Lightstreamer does not support WebSocket (returns 403),
            # so force HTTP-STREAMING unless overridden via env.
            forced_transport = self._stream_sdk_forced_transport or "HTTP-STREAMING"
            logger.info(
                "IG Lightstreamer SDK connecting | endpoint=%s account=%s adapter=%s transport=%s",
                self._stream_endpoint, self.account_id, LIGHTSTREAMER_ADAPTER_SET, forced_transport,
            )
            client = _LightstreamerClient(self._stream_endpoint, LIGHTSTREAMER_ADAPTER_SET)
            client.connectionDetails.setUser(self.account_id)
            client.connectionDetails.setPassword(f"CST-{self._cst}|XST-{self._security_token}")
            client.connectionOptions.setForcedTransport(forced_transport)
            listener = _IgLightstreamerClientListener(self)
            client.addListener(listener)
            self._stream_sdk_client = client
            self._stream_sdk_client_listener = listener
            self._stream_sdk_connected = False
            self._stream_sdk_status = "connecting"
            client.connect()
            logger.info("IG Lightstreamer SDK connect() returned, waiting for status callback")
        except Exception as exc:
            self._stream_sdk_client = None
            self._stream_sdk_client_listener = None
            self._stream_sdk_connected = False
            self._stream_sdk_status = None
            self._stream_last_error = f"sdk_connect_failed:{exc}"

    def _stop_stream_sdk_locked(self) -> None:
        client = self._stream_sdk_client
        listener = self._stream_sdk_client_listener
        subscriptions = list(self._stream_sdk_subscriptions.values())
        self._stream_sdk_client = None
        self._stream_sdk_client_listener = None
        self._stream_sdk_subscriptions.clear()
        self._stream_sdk_subscription_listeners.clear()
        self._stream_sdk_pending_subscriptions.clear()
        self._stream_sdk_connected = False
        self._stream_sdk_status = None
        self._stream_session_id = None
        self._stream_control_url = None
        self._stream_keepalive_ms = None
        self._stream_symbol_to_table.clear()
        self._stream_table_to_symbol.clear()
        self._stream_table_field_values.clear()
        if client is None:
            return
        for subscription in subscriptions:
            try:
                client.unsubscribe(subscription)
            except Exception:
                pass
        if listener is not None:
            try:
                client.removeListener(listener)
            except Exception:
                pass
        try:
            client.disconnect()
        except Exception:
            pass

    def _start_stream_thread_locked(self) -> None:
        if not self._stream_enabled:
            self._stream_last_error = "stream_disabled_by_config"
            return
        if self._stream_use_sdk:
            self._start_stream_sdk_locked()
            return
        thread = self._stream_thread
        if thread is not None and thread.is_alive():
            return
        if not self._stream_endpoint:
            self._stream_last_error = "lightstreamer_endpoint_missing"
            return

        self._stream_stop_event.clear()
        self._stream_thread = threading.Thread(
            target=self._stream_loop,
            name="ig-lightstreamer-stream",
            daemon=True,
        )
        self._stream_thread.start()

    def _stop_stream_thread_locked(self) -> threading.Thread | None:
        if self._stream_use_sdk:
            self._stop_stream_sdk_locked()
            return None
        self._stream_stop_event.set()
        self._mark_stream_disconnected_locked(None, schedule_retry=False)
        thread = self._stream_thread
        self._stream_thread = None
        if thread is threading.current_thread():
            return None
        return thread

    def _send_stream_control(self, control_url: str, params: dict[str, Any]) -> tuple[bool, str]:
        req = request.Request(
            url=control_url,
            method="POST",
            data=self._form_body(params),
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "text/plain",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_sec) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            return False, str(exc)

        lines = [line.strip() for line in payload.replace("\r", "\n").split("\n") if line.strip()]
        if not lines:
            return False, "empty_control_response"

        first = lines[0]
        if first == "OK":
            return True, "ok"
        if first == "SYNC ERROR":
            return False, "sync_error"
        if first == "ERROR":
            code = lines[1] if len(lines) > 1 else "unknown"
            message = lines[2] if len(lines) > 2 else "unknown"
            return False, f"error:{code}:{message}"
        return False, first

    def _subscribe_symbol(self, symbol: str) -> bool:
        if self._stream_use_sdk:
            with self._lock:
                sdk_client_ready = self._stream_sdk_client is not None
            if sdk_client_ready:
                return self._subscribe_symbol_sdk(symbol)
        upper = symbol.upper()

        with self._lock:
            if upper in self._stream_symbol_to_table:
                return True
            session_id = self._stream_session_id
            control_url = self._stream_control_url
            account_id = self.account_id
            if not session_id or not control_url or not account_id:
                return False
            table_id = self._stream_next_table_id
            self._stream_next_table_id += 1

        try:
            epic = self._epic_for_symbol(upper)
        except Exception as exc:
            with self._lock:
                self._stream_last_error = f"epic_resolution_failed:{upper}:{exc}"
            return False

        attempted_epics: set[str] = set()
        final_reason = "unknown"

        while True:
            normalized_epic = str(epic).strip().upper()
            if not normalized_epic or normalized_epic in attempted_epics:
                break
            attempted_epics.add(normalized_epic)

            item = f"PRICE:{account_id}:{normalized_epic}"
            # Lightstreamer field lists are space-separated (+ encoded in form body).
            schema = " ".join(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS)
            ok, reason = self._send_stream_control(
                control_url,
                {
                    "LS_session": session_id,
                    "LS_table": table_id,
                    "LS_op": "add",
                    "LS_data_adapter": LIGHTSTREAMER_PRICE_ADAPTER,
                    "LS_id": item,
                    "LS_schema": schema,
                    "LS_mode": "MERGE",
                    "LS_snapshot": "true",
                },
            )
            if ok:
                self._activate_epic(upper, normalized_epic)
                self._clear_stream_rest_fallback_block(upper)

                with self._lock:
                    if session_id != self._stream_session_id:
                        return False
                    self._stream_symbol_to_table[upper] = table_id
                    self._stream_table_to_symbol[table_id] = upper
                    self._stream_table_field_values[table_id] = [None] * len(LIGHTSTREAMER_PRICE_FIELDS)
                return True

            final_reason = str(reason or "unknown")
            if reason == "sync_error":
                with self._lock:
                    self._stream_last_error = f"subscription_failed:{upper}:{final_reason}"
                    self._mark_stream_disconnected_locked("stream_sync_error", schedule_retry=True)
                return False

            if self._is_stream_subscription_invalid_group_error_text(reason):
                next_epic = self._promote_stream_subscription_epic_after_invalid_group(
                    upper,
                    normalized_epic,
                    reason=f"stream_subscription:{final_reason}",
                )
                if next_epic:
                    return False

            break

        if self._is_stream_subscription_invalid_group_error_text(final_reason):
            self._mark_stream_rest_fallback_block(upper, reason=f"stream_subscription:{final_reason}")
        with self._lock:
            self._stream_last_error = f"subscription_failed:{upper}:{final_reason}"
        return False

    def _subscribe_symbol_sdk(self, symbol: str) -> bool:
        if _LightstreamerSubscription is None:
            with self._lock:
                self._stream_last_error = "lightstreamer_sdk_not_installed"
            return False

        upper = str(symbol).upper()
        with self._lock:
            client = self._stream_sdk_client
            account_id = self.account_id
            if upper in self._stream_symbol_to_table:
                return True
            if upper in self._stream_sdk_pending_subscriptions:
                return True
            if upper in self._stream_sdk_subscriptions:
                return True
            if client is None or not account_id:
                return False

        try:
            epic = self._epic_for_symbol(upper)
        except Exception as exc:
            with self._lock:
                self._stream_last_error = f"epic_resolution_failed:{upper}:{exc}"
            return False

        item = f"PRICE:{account_id}:{epic}"
        try:
            subscription = _LightstreamerSubscription(
                "MERGE",
                [item],
                list(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS),
            )
            subscription.setDataAdapter(LIGHTSTREAMER_PRICE_ADAPTER)
            subscription.setRequestedSnapshot("yes")
            listener = _IgLightstreamerSubscriptionListener(self, upper)
            subscription.addListener(listener)
            with self._lock:
                self._stream_sdk_subscriptions[upper] = subscription
                self._stream_sdk_subscription_listeners[upper] = listener
                self._stream_sdk_pending_subscriptions.add(upper)
            client.subscribe(subscription)
            return True
        except Exception as exc:
            with self._lock:
                self._stream_sdk_pending_subscriptions.discard(upper)
                self._stream_sdk_subscriptions.pop(upper, None)
                self._stream_sdk_subscription_listeners.pop(upper, None)
                self._stream_last_error = f"subscription_failed:{upper}:{exc}"
            return False

    def _ensure_stream_subscription(self, symbol: str) -> bool:
        if not self._stream_enabled:
            return False
        upper = symbol.upper()
        block_remaining = self._stream_rest_fallback_block_remaining(upper)
        retry_remaining = self._stream_subscription_retry_remaining(upper)
        with self._lock:
            self._stream_desired_subscriptions.add(upper)
            already_subscribed = upper in self._stream_symbol_to_table
            if self._stream_use_sdk:
                stream_ready = self._stream_sdk_client is not None
            else:
                stream_ready = bool(self._stream_session_id and self._stream_control_url)
        if already_subscribed:
            return True
        if block_remaining > 0:
            return False
        if retry_remaining > 0:
            return False
        if not stream_ready:
            return False
        return self._subscribe_symbol(upper)

    def _resubscribe_desired_symbols(self) -> None:
        with self._lock:
            desired = sorted(self._stream_desired_subscriptions)
        for symbol in desired:
            self._ensure_stream_subscription(symbol)

    def set_stream_tick_handler(self, handler: Callable[[PriceTick], None] | None) -> None:
        with self._lock:
            self._stream_tick_handler = handler if callable(handler) else None

    def _emit_stream_tick(self, tick: PriceTick) -> None:
        with self._lock:
            handler = self._stream_tick_handler
        if handler is None:
            return
        try:
            handler(self._clone_tick(tick))
        except Exception:
            logger.debug("Stream tick handler failed", exc_info=True)

    def _update_tick_from_stream_fields(self, symbol: str, field_values: list[str | None]) -> None:
        field_map: dict[str, str | None] = {}
        for idx, name in enumerate(LIGHTSTREAMER_PRICE_FIELDS):
            field_map[name] = field_values[idx] if idx < len(field_values) else None

        def _first_positive_field(*names: str) -> float:
            for name in names:
                parsed = _as_float(field_map.get(name), 0.0)
                if parsed > 0:
                    return parsed
            return 0.0

        bid = _first_positive_field("BIDPRICE1", "BID")
        ask = _first_positive_field("ASKPRICE1", "OFR", "OFFER")
        timestamp_raw: str | None = None
        for key in ("TIMESTAMP", "UTM", "UPDATE_TIME"):
            raw = field_map.get(key)
            if raw not in (None, ""):
                timestamp_raw = raw
                break

        if bid <= 0 and ask <= 0:
            return
        if bid <= 0:
            bid = ask
        if ask <= 0:
            ask = bid

        now_ts = time.time()
        tick_ts = self._parse_ts({"updateTimeUTC": timestamp_raw}, now_ts)

        tick = PriceTick(
            symbol=symbol,
            bid=bid,
            ask=ask,
            timestamp=tick_ts,
            received_at=now_ts,
        )
        with self._lock:
            previous_tick = self._tick_cache.get(symbol)
            quote_changed = (
                previous_tick is None
                or abs(float(previous_tick.bid) - bid) > FLOAT_ROUNDING_TOLERANCE
                or abs(float(previous_tick.ask) - ask) > FLOAT_ROUNDING_TOLERANCE
            )
            if quote_changed or symbol not in self._stream_last_quote_change_ts_by_symbol:
                self._stream_last_quote_change_ts_by_symbol[symbol] = now_ts
            self._tick_cache[symbol] = tick
            self._last_tick_by_symbol[symbol] = tick_ts
        self._emit_stream_tick(tick)

    def _is_stream_quote_stagnant_locked(self, symbol: str, now_ts: float) -> bool:
        max_age = float(self._stream_stagnant_quote_max_age_sec)
        if max_age <= 0:
            return False
        changed_at = self._stream_last_quote_change_ts_by_symbol.get(symbol)
        if changed_at is None:
            return False
        return (now_ts - changed_at) > max_age

    def _handle_lightstreamer_table_update(self, table_id: int, raw_values: str) -> bool:
        with self._lock:
            symbol = self._stream_table_to_symbol.get(table_id)
            previous_values = list(
                self._stream_table_field_values.get(table_id, [None] * len(LIGHTSTREAMER_PRICE_FIELDS))
            )

        if symbol is None:
            return True

        encoded_values = raw_values.split("|")
        decoded: list[str | None] = []
        for idx in range(len(LIGHTSTREAMER_PRICE_FIELDS)):
            prev = previous_values[idx] if idx < len(previous_values) else None
            encoded = encoded_values[idx] if idx < len(encoded_values) else ""
            decoded.append(self._decode_stream_field(encoded, prev))

        with self._lock:
            self._stream_table_field_values[table_id] = decoded

        self._update_tick_from_stream_fields(symbol, decoded)
        return True

    def _handle_stream_line(self, line: str) -> bool:
        if line == "" or line == "PROBE":
            return True

        if line.startswith("Preamble"):
            return True

        if line.startswith(("ERROR", "END", "LOOP", "SYNC ERROR")):
            logger.warning("IG Lightstreamer control event: %s", line)
            return False

        if line.startswith("U,"):
            # Some servers may emit TLCP-style updates; try to read table id and pipe payload.
            parts = line.split(",", 3)
            if len(parts) >= 4:
                try:
                    table_id = int(parts[1])
                except ValueError:
                    return True
                return self._handle_lightstreamer_table_update(table_id, parts[3])
            return True

        if "|" in line and "," in line:
            prefix, values = line.split("|", 1)
            prefix_parts = prefix.split(",")
            if len(prefix_parts) >= 2 and prefix_parts[0].isdigit() and prefix_parts[1].isdigit():
                return self._handle_lightstreamer_table_update(int(prefix_parts[0]), values)
        elif "|" in line:
            # Some servers emit table updates as "<table>|<values>" in MERGE mode.
            prefix, values = line.split("|", 1)
            if prefix.isdigit():
                return self._handle_lightstreamer_table_update(int(prefix), values)

        if ",EOS" in line or ",OV" in line:
            return True

        return True

    def _open_stream_session(self) -> Any:
        with self._lock:
            stream_endpoint = self._stream_endpoint
            account_id = self.account_id
            cst = self._cst
            security_token = self._security_token

        if not stream_endpoint:
            raise BrokerError("lightstreamer_endpoint_missing")
        if not account_id:
            raise BrokerError("lightstreamer_account_id_missing")
        if not cst or not security_token:
            raise BrokerError("lightstreamer_auth_tokens_missing")

        create_url = f"{stream_endpoint}/create_session.txt"
        body = self._form_body(
            {
                "LS_op2": "create",
                "LS_cid": LIGHTSTREAMER_CID,
                "LS_adapter_set": LIGHTSTREAMER_ADAPTER_SET,
                "LS_user": account_id,
                "LS_password": f"CST-{cst}|XST-{security_token}",
                "LS_keepalive_millis": 5000,
                "LS_content_length": 50000000,
            }
        )

        req = request.Request(
            url=create_url,
            method="POST",
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "text/plain",
            },
        )

        timeout = max(self.stream_read_timeout_sec, self.timeout_sec)
        return request.urlopen(req, timeout=timeout)

    _STREAM_MAX_RECONNECT_ATTEMPTS = 50

    def _stream_loop(self) -> None:
        while not self._stream_stop_event.is_set():
            with self._lock:
                if not self._connected:
                    return
                retry_at = self._stream_next_retry_at
                reconnect_attempts = self._stream_reconnect_attempts

            # Circuit breaker: stop reconnecting after too many consecutive failures
            if reconnect_attempts >= self._STREAM_MAX_RECONNECT_ATTEMPTS:
                logger.error(
                    "IG Lightstreamer circuit breaker: %d consecutive reconnect failures, "
                    "stopping stream loop. REST fallback remains active.",
                    reconnect_attempts,
                )
                return

            now = time.time()
            if retry_at is not None and now < retry_at:
                self._stream_stop_event.wait(max(0.1, retry_at - now))
                continue

            stream_response: Any | None = None
            try:
                stream_response = self._open_stream_session()
                first_line = self._read_stream_line(stream_response)
                if first_line != "OK":
                    raise BrokerError(f"stream_create_failed:{first_line}")

                session_id: str | None = None
                control_address: str | None = None
                keepalive_ms: int | None = None

                while not self._stream_stop_event.is_set():
                    header_line = self._read_stream_line(stream_response)
                    if header_line is None:
                        raise BrokerError("stream_header_unexpected_eof")
                    if header_line == "":
                        break
                    if ":" not in header_line:
                        continue
                    key, value = header_line.split(":", 1)
                    key = key.strip()
                    value = value.strip()
                    if key == "SessionId":
                        session_id = value
                    elif key == "ControlAddress":
                        control_address = value
                    elif key == "KeepaliveMillis":
                        try:
                            keepalive_ms = int(value)
                        except ValueError:
                            keepalive_ms = None

                if not session_id:
                    raise BrokerError("stream_session_id_missing")

                control_url = self._resolve_stream_control_url(control_address)
                with self._lock:
                    self._stream_http_response = stream_response
                    self._mark_stream_connected_locked(session_id, control_url, keepalive_ms)

                self._resubscribe_desired_symbols()

                while not self._stream_stop_event.is_set():
                    line = self._read_stream_line(stream_response)
                    if line is None:
                        raise BrokerError("stream_eof")
                    handled = self._handle_stream_line(line)
                    if handled:
                        continue
                    raise BrokerError(f"stream_control_message:{line}")

            except Exception as exc:
                if self._stream_stop_event.is_set():
                    break
                with self._lock:
                    self._mark_stream_disconnected_locked(str(exc), schedule_retry=True)
                    retry_in = (
                        max(0.1, (self._stream_next_retry_at or time.time()) - time.time())
                        if self._stream_next_retry_at is not None
                        else self._stream_backoff_base_sec
                    )
                logger.warning("IG Lightstreamer degraded, switching to REST fallback: %s", exc)
                self._stream_stop_event.wait(retry_in)
            finally:
                if stream_response is not None:
                    try:
                        stream_response.close()
                    except Exception:
                        pass

    def _get_cached_tick_locked(self, symbol: str, max_age_sec: float) -> PriceTick | None:
        tick = self._tick_cache.get(symbol)
        if tick is None:
            return None
        freshness_ts = float(getattr(tick, "received_at", tick.timestamp) or tick.timestamp)
        if max_age_sec > 0 and (time.time() - freshness_ts) > max_age_sec:
            return None
        return self._clone_tick(tick)

    @staticmethod
    def _clone_tick(tick: PriceTick) -> PriceTick:
        volume = float(tick.volume) if tick.volume is not None else None
        received_at = getattr(tick, "received_at", None)
        return PriceTick(
            symbol=str(tick.symbol),
            bid=float(tick.bid),
            ask=float(tick.ask),
            timestamp=float(tick.timestamp),
            volume=volume,
            received_at=(float(received_at) if received_at is not None else None),
        )

    @staticmethod
    def _is_allowance_error_text(text: str) -> bool:
        lowered = text.lower()
        return (
            "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )

    @staticmethod
    def _is_trade_allowance_error_text(text: str) -> bool:
        lowered = str(text).lower()
        return "exceeded-account-trading-allowance" in lowered

    def _maybe_start_allowance_cooldown(self, error_text: str, scope: str) -> float | None:
        if not self._is_allowance_error_text(error_text):
            return None
        minimum_cooldown_sec = 30.0 if self._is_trade_allowance_error_text(error_text) else 0.0
        cooldown_sec = self._start_allowance_cooldown(error_text, minimum_sec=minimum_cooldown_sec)
        with self._lock:
            self._allowance_last_scope = str(scope)
        self._adapt_market_rest_interval_after_allowance(scope)
        logger.warning(
            "IG allowance exceeded for %s, pausing REST calls for %.1fs",
            scope,
            cooldown_sec,
        )
        return cooldown_sec

    def _allowance_cooldown_remaining(self) -> float:
        with self._lock:
            return max(0.0, self._allowance_cooldown_until_ts - time.time())

    def _allowance_cooldown_remaining_for_market_data(self) -> float:
        remaining = self._allowance_cooldown_remaining()
        if remaining <= 0:
            return 0.0
        with self._lock:
            last_error = str(self._allowance_last_error or "")
            last_scope = str(self._allowance_last_scope or "").strip().lower()
        if (
            self._is_trade_allowance_error_text(last_error)
            and last_scope in {"trade open", "trade open confirm", "managed positions"}
        ):
            # Trading allowance should not block quote acquisition if quotes are still available.
            return 0.0
        return remaining

    def _adapt_market_rest_interval_after_allowance(self, scope: str) -> None:
        normalized_scope = str(scope or "").strip().lower()
        if normalized_scope not in {"market data", "symbol spec", "managed positions", "accounts"}:
            return
        with self._lock:
            previous = float(self.rest_market_min_interval_sec)
            target = min(
                float(self._rest_market_min_interval_max_sec),
                max(previous + 0.2, previous * 1.4),
            )
            if target <= (previous + FLOAT_COMPARISON_TOLERANCE):
                return
            self.rest_market_min_interval_sec = target
        logger.warning(
            "IG REST market interval auto-raised after allowance exceed: %.2fs -> %.2fs",
            previous,
            target,
        )

    def _start_allowance_cooldown(self, error_text: str, *, minimum_sec: float = 0.0) -> float:
        now = time.time()
        with self._lock:
            remaining = self._allowance_cooldown_until_ts - now
            if remaining > 0:
                self._allowance_last_error = error_text
                return remaining
            previous = self._allowance_cooldown_sec
            next_delay = min(
                self._allowance_cooldown_max_sec,
                max(2.0, float(minimum_sec), previous * 2.0),
            )
            self._allowance_cooldown_sec = next_delay
            self._allowance_cooldown_until_ts = now + next_delay
            self._allowance_last_error = error_text
            return next_delay

    def _reset_allowance_cooldown(self) -> None:
        with self._lock:
            self._allowance_cooldown_until_ts = 0.0
            self._allowance_cooldown_sec = 1.0
            self._allowance_last_error = None
            self._allowance_last_scope = None

    @staticmethod
    def _extract_session_tokens(
        headers: dict[str, str],
        body: dict[str, Any] | None,
    ) -> tuple[str | None, str | None]:
        normalized_headers = {str(key).strip().lower(): str(value) for key, value in headers.items()}
        cst = normalized_headers.get("cst")
        security = normalized_headers.get("x-security-token")

        payload = body if isinstance(body, dict) else {}
        if not cst:
            for key in ("cst", "CST", "clientSessionToken"):
                raw = payload.get(key)
                if raw not in (None, ""):
                    cst = str(raw)
                    break
        if not security:
            for key in ("x-security-token", "X-SECURITY-TOKEN", "securityToken", "xst"):
                raw = payload.get(key)
                if raw not in (None, ""):
                    security = str(raw)
                    break

        return cst, security

    @staticmethod
    def _is_invalid_client_security_token_error_text(text: str) -> bool:
        lowered = str(text).lower()
        return "invalid-client-security-token" in lowered

    def _api_key_tail(self) -> str:
        key = str(self.api_key or "").strip()
        if not key:
            return "empty"
        if len(key) <= 4:
            return key
        return key[-4:]

    def _reset_auth_state_for_login(self) -> None:
        with self._lock:
            self._connected = False
            self._cst = None
            self._security_token = None
            self._connectivity_status_cache = None
            self._connectivity_status_cache_ts = 0.0

    def connect(self) -> None:
        # Ensure login starts from a clean auth state.
        self._reset_auth_state_for_login()

        payload = {"identifier": self.identifier, "password": self.password}
        transient_delay_sec = self.connect_retry_base_sec
        for attempt in range(1, self.connect_retry_attempts + 1):
            self._reset_auth_state_for_login()
            try:
                login_version = "3"
                try:
                    body_raw, headers = self._request(
                        "POST", "/session", payload=payload, version=login_version, auth=False
                    )
                    body = _as_mapping(body_raw)
                except BrokerError as exc:
                    text = str(exc).lower()
                    if not self._is_invalid_client_security_token_error_text(text):
                        raise
                    # Some IG environments intermittently reject v3 login with this error.
                    # Retry once with v2 before giving up.
                    login_version = "2"
                    body_raw, headers = self._request(
                        "POST", "/session", payload=payload, version=login_version, auth=False
                    )
                    body = _as_mapping(body_raw)

                cst, security = self._extract_session_tokens(headers, body)
                if login_version == "3" and (not cst or not security):
                    # Some IG accounts return session headers only for v2 login.
                    try:
                        body_v2_raw, headers_v2 = self._request(
                            "POST",
                            "/session",
                            payload=payload,
                            version="2",
                            auth=False,
                        )
                    except BrokerError as exc:
                        text = str(exc).lower()
                        if self._is_invalid_client_security_token_error_text(text):
                            raise BrokerError(
                                f"IG login v2 fallback failed: {exc}"
                            ) from exc
                        raise
                    body_v2 = _as_mapping(body_v2_raw)
                    cst_v2, security_v2 = self._extract_session_tokens(headers_v2, body_v2)
                    if cst_v2 and security_v2:
                        cst = cst_v2
                        security = security_v2
                        if not body.get("currentAccountId"):
                            body["currentAccountId"] = body_v2.get("currentAccountId")
                        if not body.get("lightstreamerEndpoint"):
                            body["lightstreamerEndpoint"] = body_v2.get("lightstreamerEndpoint")
                if not cst or not security:
                    raise BrokerError("IG login succeeded but session tokens are missing")

                explicit_account_id = str(self.account_id).strip() if self.account_id not in (None, "") else None
                current_account_id = str(body.get("currentAccountId") or "").strip() or None
                resolved_account_id = explicit_account_id or current_account_id

                with self._lock:
                    self._cst = cst
                    self._security_token = security
                    self._connected = True
                    self.account_id = resolved_account_id
                    self._stream_endpoint = self._normalize_lightstreamer_endpoint(body.get("lightstreamerEndpoint"))
                    if self._stream_enabled and self._stream_endpoint is None:
                        self._stream_last_error = "lightstreamer_endpoint_missing"

                should_switch_account = bool(
                    explicit_account_id and (current_account_id is None or explicit_account_id != current_account_id)
                )
                if should_switch_account and explicit_account_id:
                    try:
                        self._request(
                            "PUT",
                            "/session",
                            payload={"accountId": explicit_account_id, "defaultAccount": True},
                            version="1",
                            auth=True,
                        )
                    except Exception as exc:
                        logger.warning("IG account switch failed for account_id=%s: %s", explicit_account_id, exc)

                with self._lock:
                    self._start_stream_thread_locked()
                self._reset_allowance_cooldown()
                return
            except BrokerError as exc:
                error_text = str(exc)
                lowered = error_text.lower()
                if attempt >= self.connect_retry_attempts:
                    if self._is_invalid_client_security_token_error_text(lowered):
                        endpoint = str(self._endpoint or "")
                        account_type = self.account_type.value
                        raise BrokerError(
                            f"{error_text} | hint: login rejected with invalid-client-security-token; "
                            "verify IG credentials and that endpoint matches account type "
                            "(demo->demo-api, live->api). If you use .env, it now overrides shell env "
                            "by default; set BOT_DOTENV_PREFER_ENV=1 to keep existing shell values. "
                            f"account_type={account_type} endpoint={endpoint} api_key_tail={self._api_key_tail()}"
                        ) from exc
                    raise

                delay_sec = 0.0
                if self._is_allowance_error_text(lowered):
                    delay_sec = self._start_allowance_cooldown(error_text)
                    logger.warning(
                        "IG login allowance exceeded on connect attempt %s/%s; retrying in %.1fs",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                    )
                elif self._is_invalid_client_security_token_error_text(lowered):
                    delay_sec = min(self.connect_retry_max_sec, transient_delay_sec)
                    transient_delay_sec = min(self.connect_retry_max_sec, transient_delay_sec * 2.0)
                    logger.warning(
                        "IG login invalid-client-security-token on attempt %s/%s; retrying in %.1fs",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                    )
                elif any(
                    token in lowered
                    for token in (
                        "timed out",
                        "timeout",
                        "temporarily unavailable",
                        "service unavailable",
                        "bad gateway",
                        "gateway timeout",
                        "connection reset",
                        "connection refused",
                        "network is unreachable",
                    )
                ):
                    delay_sec = min(self.connect_retry_max_sec, transient_delay_sec)
                    transient_delay_sec = min(self.connect_retry_max_sec, transient_delay_sec * 2.0)
                    logger.warning(
                        "IG connect transient error on attempt %s/%s; retrying in %.1fs: %s",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                        error_text,
                    )
                else:
                    raise
                time.sleep(max(0.1, delay_sec))

    def close(self) -> None:
        stream_thread_to_join: threading.Thread | None = None
        request_threads_to_join: list[threading.Thread] = []
        with self._lock:
            stream_thread_to_join = self._stop_stream_thread_locked()

        if stream_thread_to_join is not None and stream_thread_to_join.is_alive():
            stream_thread_to_join.join(timeout=2.0)

        try:
            if self._connected:
                self._request("DELETE", "/session", version="1", auth=True)
        except Exception:
            pass
        finally:
            with self._lock:
                request_threads_to_join = self._stop_request_workers_locked()
                self._connected = False
                self._cst = None
                self._security_token = None
                self._tick_cache.clear()
                self._last_tick_by_symbol.clear()
                self._stream_desired_subscriptions.clear()
                self._stream_last_error = None
                self._stream_endpoint = None
                self._account_snapshot_cache = None
                self._account_snapshot_cached_at = 0.0
                self._connectivity_status_cache = None
                self._connectivity_status_cache_ts = 0.0
                self._position_open_sync.clear()
                self._position_close_sync.clear()
                self._epic_unavailable_until_ts_by_symbol.clear()
                self._last_stream_rest_fallback_ts_by_symbol.clear()
                self._request_worker_thread_ids.clear()
                self._historical_http_cache.clear()
                self._stream_sdk_client = None
                self._stream_sdk_client_listener = None
                self._stream_sdk_subscriptions.clear()
                self._stream_sdk_subscription_listeners.clear()
                self._stream_sdk_pending_subscriptions.clear()
                self._stream_sdk_connected = False
                self._stream_sdk_status = None
                self._stream_tick_handler = None

        for thread in request_threads_to_join:
            if thread.is_alive():
                thread.join(timeout=2.0)

    def _get_price_rest(self, symbol: str) -> PriceTick:
        upper = symbol.upper()

        epic, body = self._request_market_details_with_epic_failover(upper)
        tick = self._tick_from_market_body(upper, body)
        if tick is None:
            attempts = [candidate for candidate in self._epic_attempt_order(upper) if candidate != epic]
            last_missing_quote_error = _snapshot_missing_quote_error(
                upper,
                epic,
                _as_mapping(body.get("snapshot")),
                _as_mapping(body.get("instrument")) or _as_mapping(body.get("market")),
                body,
            )
            remaining_candidates = list(attempts)
            search_loaded = False
            candidate_index = 0
            while candidate_index < len(remaining_candidates):
                candidate = remaining_candidates[candidate_index]
                candidate_index += 1
                try:
                    self._wait_for_market_rest_slot()
                    candidate_body, _ = self._request("GET", f"/markets/{candidate}", version="3", auth=True)
                except BrokerError as exc:
                    if self._is_epic_unavailable_error_text(str(exc)):
                        candidate_tick = None
                    else:
                        raise
                else:
                    candidate_tick = self._tick_from_market_body(upper, candidate_body)
                    if candidate_tick is None:
                        last_missing_quote_error = _snapshot_missing_quote_error(
                            upper,
                            candidate,
                            _as_mapping(candidate_body.get("snapshot")),
                            _as_mapping(candidate_body.get("instrument")) or _as_mapping(candidate_body.get("market")),
                            candidate_body,
                        )
                    else:
                        self._activate_epic(upper, candidate)
                        self._clear_epic_unavailable_retry(upper)
                        logger.warning(
                            "IG quote failover resolved for %s: using %s instead of %s",
                            upper,
                            candidate,
                            epic,
                        )
                        tick = candidate_tick
                        break

                if candidate_index >= len(remaining_candidates) and not search_loaded:
                    search_loaded = True
                    dynamic_attempts = [
                        discovered
                        for discovered in self._extend_epic_candidates_from_search(upper)
                        if discovered not in remaining_candidates and discovered != epic
                    ]
                    remaining_candidates.extend(dynamic_attempts)
            if tick is None:
                raise last_missing_quote_error

        tick_for_cache = self._clone_tick(tick)
        with self._lock:
            self._tick_cache[upper] = tick_for_cache
            self._last_tick_by_symbol[upper] = tick_for_cache.timestamp
        return self._clone_tick(tick_for_cache)

    def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
        upper = symbol.upper()
        with self._lock:
            stream_enabled = self._stream_enabled
            fresh_tick_max_age_sec = max(0.5, float(self.stream_tick_max_age_sec))
        if not stream_enabled:
            return None

        self._ensure_stream_subscription(upper)
        wait_timeout = min(1.0, self.timeout_sec, max(0.0, float(wait_timeout_sec)))
        deadline = time.time() + wait_timeout
        stream_alive = False

        while True:
            with self._lock:
                cached = self._get_cached_tick_locked(upper, self.stream_tick_max_age_sec)
                stream_alive = bool(self._stream_session_id)
                stream_quote_stagnant = (
                    cached is not None and self._is_stream_quote_stagnant_locked(upper, time.time())
                )
            if cached is not None:
                if not stream_quote_stagnant:
                    with self._lock:
                        self._price_requests_total += 1
                        self._stream_price_hits_total += 1
                    return cached
                break
            if wait_timeout <= 0 or time.time() >= deadline:
                break
            if not stream_alive:
                break
            time.sleep(0.05)

        if stream_alive:
            with self._lock:
                stale_cached = self._get_cached_tick_locked(upper, fresh_tick_max_age_sec)
                stream_quote_stagnant = (
                    stale_cached is not None and self._is_stream_quote_stagnant_locked(upper, time.time())
                )
            if stale_cached is not None:
                if not stream_quote_stagnant:
                    with self._lock:
                        self._price_requests_total += 1
                        self._stream_price_hits_total += 1
                    return stale_cached

        return None

    def get_price(self, symbol: str) -> PriceTick:
        upper = symbol.upper()
        with self._lock:
            self._price_requests_total += 1
            stream_enabled = self._stream_enabled
            stale_tick_max_age_sec = self._stream_stale_tick_max_age_sec
            fresh_tick_max_age_sec = max(0.5, float(self.stream_tick_max_age_sec))
        if stream_enabled:
            self._ensure_stream_subscription(upper)

            deadline = time.time() + min(1.0, self.timeout_sec)
            stream_alive = False
            while time.time() < deadline:
                with self._lock:
                    cached = self._get_cached_tick_locked(upper, self.stream_tick_max_age_sec)
                    stream_alive = bool(self._stream_session_id)
                    stream_quote_stagnant = (
                        cached is not None and self._is_stream_quote_stagnant_locked(upper, time.time())
                    )
                if cached is not None:
                    if not stream_quote_stagnant:
                        with self._lock:
                            self._stream_price_hits_total += 1
                        return cached
                    break
                if not stream_alive:
                    break
                time.sleep(0.05)
            if stream_alive:
                with self._lock:
                    stale_cached = self._get_cached_tick_locked(upper, fresh_tick_max_age_sec)
                    stream_quote_stagnant = (
                        stale_cached is not None and self._is_stream_quote_stagnant_locked(upper, time.time())
                    )
                if stale_cached is not None:
                    if not stream_quote_stagnant:
                        with self._lock:
                            self._stream_price_hits_total += 1
                        return stale_cached

            blocked_remaining = self._stream_rest_fallback_block_remaining(upper)
            if blocked_remaining > 0:
                with self._lock:
                    cached = self._get_cached_tick_locked(upper, max_age_sec=stale_tick_max_age_sec)
                if cached is not None:
                    return cached
                block_reason = self._stream_rest_fallback_block_reason(upper) or "stream_subscription_unavailable"
                raise BrokerError(
                    "IG stream subscription unavailable for "
                    f"{upper}; REST fallback disabled ({blocked_remaining:.1f}s remaining) "
                    f"| reason={block_reason}"
                )

        cooldown_remaining = self._allowance_cooldown_remaining_for_market_data()
        if cooldown_remaining > 0:
            with self._lock:
                fallback_max_age = stale_tick_max_age_sec if stream_enabled else 5.0
                cached = self._get_cached_tick_locked(upper, max_age_sec=fallback_max_age)
            if cached is not None:
                return cached
            raise BrokerError(
                f"IG public API allowance cooldown is active ({cooldown_remaining:.1f}s remaining)"
            )

        if stream_enabled:
            with self._lock:
                stream_alive = bool(self._stream_session_id)
                throttle_sec = float(self._stream_rest_fallback_min_interval_sec)
                last_stream_rest_ts = float(self._last_stream_rest_fallback_ts_by_symbol.get(upper, 0.0))
            if stream_alive and throttle_sec > 0:
                since_last_rest = time.time() - last_stream_rest_ts
                throttle_remaining = throttle_sec - since_last_rest
                if throttle_remaining > 0:
                    with self._lock:
                        cached = self._get_cached_tick_locked(upper, max_age_sec=fresh_tick_max_age_sec)
                        stream_quote_stagnant = (
                            cached is not None and self._is_stream_quote_stagnant_locked(upper, time.time())
                        )
                    if cached is not None:
                        if not stream_quote_stagnant:
                            return cached

        interval = _as_float(self.rest_market_min_interval_sec, 0.0)
        if not math.isfinite(interval) or interval <= 0:
            interval = 0.0
        if interval > 0:
            while True:
                cached_stream_quote_stagnant = False
                with self._lock:
                    now_mono = time.monotonic()
                    now_wall = time.time()
                    if self._last_market_rest_request_monotonic <= 0:
                        self._last_market_rest_request_monotonic = now_mono
                        self._last_market_rest_request_ts = now_wall
                    elapsed = now_mono - self._last_market_rest_request_monotonic
                    wait_sec = interval - elapsed
                    if not math.isfinite(wait_sec):
                        wait_sec = 0.0
                    if wait_sec <= 0:
                        self._last_market_rest_request_monotonic = now_mono
                        self._last_market_rest_request_ts = now_wall
                        if stream_enabled:
                            self._last_stream_rest_fallback_ts_by_symbol[upper] = now_wall
                        break
                    if stream_enabled:
                        cached = self._get_cached_tick_locked(upper, max_age_sec=fresh_tick_max_age_sec)
                        cached_stream_quote_stagnant = (
                            cached is not None and self._is_stream_quote_stagnant_locked(upper, now_wall)
                        )
                    else:
                        cached = self._get_cached_tick_locked(
                            upper,
                            max_age_sec=max(1.0, self.stream_tick_max_age_sec),
                        )
                if cached is not None:
                    if not cached_stream_quote_stagnant:
                        return cached
                time.sleep(min(wait_sec, 0.2))
        elif stream_enabled:
            with self._lock:
                self._last_stream_rest_fallback_ts_by_symbol[upper] = time.time()

        try:
            tick = self._get_price_rest(upper)
            self._reset_allowance_cooldown()
        except BrokerError as exc:
            text = str(exc)
            if self._maybe_start_allowance_cooldown(text, "market data") is not None:
                with self._lock:
                    fallback_max_age = stale_tick_max_age_sec if stream_enabled else 5.0
                    cached = self._get_cached_tick_locked(upper, max_age_sec=fallback_max_age)
                if cached is not None:
                    return cached
            raise
        with self._lock:
            self._rest_fallback_hits_total += 1
        return tick

    def get_symbol_spec(self, symbol: str, *, force_refresh: bool = False) -> SymbolSpec:
        upper = symbol.upper()
        cached: SymbolSpec | None = None
        with self._lock:
            cached = self._symbol_spec_cache.get(upper)
        if cached is not None and not force_refresh:
            metadata = cached.metadata if isinstance(cached.metadata, dict) else {}
            origin = str(metadata.get("spec_origin") or "").strip().lower()
            is_allowance_fallback = origin.startswith("critical_fallback")
            if not is_allowance_fallback:
                return cached
            if self._allowance_cooldown_remaining_for_market_data() > 0:
                return cached

        cooldown_remaining = self._allowance_cooldown_remaining_for_market_data()
        if cooldown_remaining > 0:
            if cached is not None:
                return cached
            attempts = self._epic_attempt_order(upper)
            fallback_epic = attempts[0] if attempts else f"CS.D.{upper}.CFD.IP"
            fallback_spec = self._build_fallback_symbol_spec_for_epic(upper, fallback_epic)
            fallback_metadata = (
                dict(fallback_spec.metadata)
                if isinstance(fallback_spec.metadata, dict)
                else {}
            )
            fallback_metadata["spec_origin"] = "critical_fallback_allowance"
            fallback_metadata["allowance_cooldown_remaining_sec"] = round(cooldown_remaining, 2)
            fallback_spec.metadata = fallback_metadata
            self._cache_symbol_spec_entry(upper, fallback_spec, promote_symbol_cache=True)
            logger.warning(
                "IG symbol spec fallback activated for %s during allowance cooldown (%.1fs remaining)",
                upper,
                cooldown_remaining,
            )
            return fallback_spec

        if _as_float(self.rest_market_min_interval_sec, 0.0) > 0:
            self._wait_for_market_rest_slot()

        try:
            epic, body = self._request_market_details_with_epic_failover(symbol)
            self._reset_allowance_cooldown()
        except BrokerError as exc:
            text = str(exc)
            self._maybe_start_allowance_cooldown(text, "symbol spec")
            raise
        return self._build_symbol_spec_from_market_details(symbol, epic, body, cache_symbol=True)

    def get_symbol_spec_for_epic(
        self,
        symbol: str,
        epic: str,
        *,
        force_refresh: bool = False,
    ) -> SymbolSpec:
        upper = str(symbol).upper().strip()
        normalized_epic = str(epic).strip().upper()
        if not upper or not normalized_epic:
            return self.get_symbol_spec(symbol, force_refresh=force_refresh)

        cached = self._get_cached_symbol_spec_for_epic(upper, normalized_epic)
        if cached is not None and not force_refresh:
            metadata = cached.metadata if isinstance(cached.metadata, dict) else {}
            origin = str(metadata.get("spec_origin") or "").strip().lower()
            is_allowance_fallback = origin.startswith("critical_fallback")
            if not is_allowance_fallback:
                return cached
            if self._allowance_cooldown_remaining_for_market_data() > 0:
                return cached

        cooldown_remaining = self._allowance_cooldown_remaining_for_market_data()
        if cooldown_remaining > 0:
            if cached is not None:
                cached_metadata = dict(cached.metadata) if isinstance(cached.metadata, dict) else {}
                cached_metadata["allowance_cooldown_remaining_sec"] = round(cooldown_remaining, 2)
                cached.metadata = cached_metadata
                self._cache_symbol_spec_entry(upper, cached, promote_symbol_cache=True)
                return cached
            fallback_spec = self._build_fallback_symbol_spec_for_epic(upper, normalized_epic)
            fallback_metadata = (
                dict(fallback_spec.metadata)
                if isinstance(fallback_spec.metadata, dict)
                else {}
            )
            fallback_metadata["spec_origin"] = "critical_fallback_allowance_epic_override"
            fallback_metadata["allowance_cooldown_remaining_sec"] = round(cooldown_remaining, 2)
            fallback_spec.metadata = fallback_metadata
            self._cache_symbol_spec_entry(upper, fallback_spec, promote_symbol_cache=True)
            return fallback_spec

        if _as_float(self.rest_market_min_interval_sec, 0.0) > 0:
            self._wait_for_market_rest_slot()

        try:
            spec = self._get_symbol_spec_for_epic(upper, normalized_epic)
            self._activate_epic(upper, normalized_epic)
            self._cache_symbol_spec_entry(upper, spec, promote_symbol_cache=True)
            self._reset_allowance_cooldown()
            return spec
        except BrokerError as exc:
            text = str(exc)
            self._maybe_start_allowance_cooldown(text, "symbol spec epic override")
            raise

    def get_symbol_spec_candidates_for_entry(
        self,
        symbol: str,
        *,
        force_refresh: bool = False,
    ) -> list[SymbolSpec]:
        upper = str(symbol).upper().strip()
        if not upper:
            return []

        cooldown_remaining = self._allowance_cooldown_remaining_for_market_data()
        if cooldown_remaining > 0:
            specs: list[SymbolSpec] = []
            for epic in self._epic_attempt_order(upper):
                cached = self._get_cached_symbol_spec_for_epic(upper, epic)
                if cached is not None:
                    specs.append(cached)
            if specs:
                return specs
            cached = self._get_cached_symbol_spec(upper)
            if cached is not None:
                return [cached]
            try:
                return [self.get_symbol_spec(symbol)]
            except BrokerError:
                return []

        specs: list[SymbolSpec] = []
        seen_epics: set[str] = set()

        for epic in self._epic_attempt_order(upper):
            normalized_epic = str(epic).strip().upper()
            if not normalized_epic or normalized_epic in seen_epics:
                continue
            try:
                cached = self._get_cached_symbol_spec_for_epic(upper, normalized_epic)
                cached_metadata = (
                    cached.metadata
                    if isinstance(cached, SymbolSpec) and isinstance(cached.metadata, dict)
                    else {}
                )
                cached_origin = str(cached_metadata.get("spec_origin") or "").strip().lower()
                cached_is_allowance_fallback = cached_origin.startswith("critical_fallback")
                if not force_refresh and cached is not None and not cached_is_allowance_fallback:
                    spec = cached
                else:
                    if _as_float(self.rest_market_min_interval_sec, 0.0) > 0:
                        self._wait_for_market_rest_slot()
                    spec = self._get_symbol_spec_for_epic(upper, normalized_epic)
                seen_epics.add(normalized_epic)
                specs.append(spec)
            except BrokerError as exc:
                text = str(exc)
                if self._is_epic_unavailable_error_text(text):
                    continue
                if self._is_instrument_invalid_error_text(text):
                    self._mark_epic_temporarily_invalid(upper, normalized_epic, reason=text)
                    continue
                self._maybe_start_allowance_cooldown(text, "symbol spec candidates")
                if specs:
                    break
                raise

        if specs:
            return specs
        try:
            return [self.get_symbol_spec(symbol)]
        except BrokerError:
            return []

    def get_account_snapshot(self) -> AccountSnapshot:
        now = time.time()
        with self._lock:
            cached = self._account_snapshot_cache
            cached_age = now - self._account_snapshot_cached_at
        if cached is not None and cached_age <= self._account_snapshot_cache_ttl_sec:
            return AccountSnapshot(
                balance=cached.balance,
                equity=cached.equity,
                margin_free=cached.margin_free,
                timestamp=now,
            )

        cooldown_remaining = self._allowance_cooldown_remaining_for_market_data()
        if cooldown_remaining > 0:
            if cached is not None:
                return AccountSnapshot(
                    balance=cached.balance,
                    equity=cached.equity,
                    margin_free=cached.margin_free,
                    timestamp=now,
                )
            raise BrokerError(
                f"IG public API allowance cooldown is active ({cooldown_remaining:.1f}s remaining)"
            )

        try:
            body, _ = self._request("GET", "/accounts", version="1", auth=True)
            self._reset_allowance_cooldown()
        except BrokerError as exc:
            text = str(exc)
            if self._maybe_start_allowance_cooldown(text, "accounts") is not None:
                with self._lock:
                    stale = self._account_snapshot_cache
                if stale is not None:
                    return AccountSnapshot(
                        balance=stale.balance,
                        equity=stale.equity,
                        margin_free=stale.margin_free,
                        timestamp=now,
                    )
            raise

        accounts_raw = body.get("accounts")
        accounts = accounts_raw if isinstance(accounts_raw, list) else []
        if not accounts:
            raise BrokerError("IG API returned no accounts")

        selected = self._select_account_from_list([_as_mapping(acc) for acc in accounts], self.account_id)
        account_currency = self._extract_account_currency(selected)

        balance_info = _as_mapping(selected.get("balance"))
        balance = _as_float(balance_info.get("balance"), 0.0)
        pnl = _as_float(balance_info.get("profitLoss"), 0.0)
        equity = balance + pnl
        margin_free = _as_float(balance_info.get("available"), equity)

        snapshot = AccountSnapshot(
            balance=balance,
            equity=equity,
            margin_free=margin_free,
            timestamp=now,
        )
        with self._lock:
            if account_currency:
                self._account_currency_code = account_currency
            self._account_snapshot_cache = snapshot
            self._account_snapshot_cached_at = now
        return snapshot

    def get_session_close_utc(self, symbol: str, now_ts: float) -> float | None:
        _ = (symbol, now_ts)
        # IG market hours are available, but timezone-aware conversion depends on market timezone metadata.
        # Returning None avoids incorrect forced exits on timezone mismatch.
        return None

    def get_upcoming_high_impact_events(self, now_ts: float, within_sec: int) -> list[NewsEvent]:
        _ = (now_ts, within_sec)
        return []

    def get_connectivity_status(
        self,
        max_latency_ms: float,
        pong_timeout_sec: float,
    ) -> ConnectivityStatus:
        _ = pong_timeout_sec
        now = time.time()
        with self._lock:
            connected = bool(self._connected)
            cached = self._connectivity_status_cache
            cached_ts = float(self._connectivity_status_cache_ts)

        if not connected:
            status = ConnectivityStatus(
                healthy=False,
                reason="broker_not_connected",
                latency_ms=None,
                pong_ok=False,
            )
            with self._lock:
                self._connectivity_status_cache = status
                self._connectivity_status_cache_ts = now
            return status

        allowance_remaining = self._allowance_cooldown_remaining()
        if allowance_remaining > 0.0:
            status = ConnectivityStatus(
                healthy=False,
                reason=(
                    "connectivity_check_failed:"
                    f"IG public API allowance cooldown is active ({allowance_remaining:.1f}s remaining)"
                ),
                latency_ms=None,
                pong_ok=False,
            )
            with self._lock:
                self._connectivity_status_cache = status
                self._connectivity_status_cache_ts = now
            return status

        if cached is not None:
            ttl_sec = self._connectivity_status_cache_ttl_for_status(cached)
            if ttl_sec > 0 and (now - cached_ts) <= ttl_sec:
                return ConnectivityStatus(
                    healthy=bool(cached.healthy),
                    reason=str(cached.reason),
                    latency_ms=cached.latency_ms,
                    pong_ok=cached.pong_ok,
                )

        with self._connectivity_probe_lock:
            now = time.time()
            allowance_remaining = self._allowance_cooldown_remaining()
            if allowance_remaining > 0.0:
                status = ConnectivityStatus(
                    healthy=False,
                    reason=(
                        "connectivity_check_failed:"
                        f"IG public API allowance cooldown is active ({allowance_remaining:.1f}s remaining)"
                    ),
                    latency_ms=None,
                    pong_ok=False,
                )
                with self._lock:
                    self._connectivity_status_cache = status
                    self._connectivity_status_cache_ts = now
                return status

            with self._lock:
                cached = self._connectivity_status_cache
                cached_ts = float(self._connectivity_status_cache_ts)
            if cached is not None:
                ttl_sec = self._connectivity_status_cache_ttl_for_status(cached)
                if ttl_sec > 0 and (now - cached_ts) <= ttl_sec:
                    return ConnectivityStatus(
                        healthy=bool(cached.healthy),
                        reason=str(cached.reason),
                        latency_ms=cached.latency_ms,
                        pong_ok=cached.pong_ok,
                    )

            started = now
            try:
                self._request("GET", "/session", version="1", auth=True)
            except Exception as exc:
                text = str(exc)
                lowered = text.lower()
                if self._is_allowance_error_text(text):
                    cooldown_sec = self._start_allowance_cooldown(text, minimum_sec=30.0)
                    with self._lock:
                        self._allowance_last_scope = "connectivity"
                    status = ConnectivityStatus(
                        healthy=False,
                        reason=(
                            "connectivity_check_failed:"
                            f"IG public API allowance cooldown is active ({cooldown_sec:.1f}s remaining)"
                        ),
                        latency_ms=None,
                        pong_ok=False,
                    )
                elif "critical_trade_operation_active" in lowered:
                    status = ConnectivityStatus(
                        healthy=True,
                        reason="critical_trade_operation_active",
                        latency_ms=0.0,
                        pong_ok=True,
                    )
                else:
                    status = ConnectivityStatus(
                        healthy=False,
                        reason=f"connectivity_check_failed:{exc}",
                        latency_ms=None,
                        pong_ok=False,
                    )
                with self._lock:
                    self._connectivity_status_cache = status
                    self._connectivity_status_cache_ts = time.time()
                return status

        latency_ms = (time.time() - started) * 1000.0
        healthy = latency_ms <= max_latency_ms
        status = ConnectivityStatus(
            healthy=healthy,
            reason="ok" if healthy else "latency_too_high",
            latency_ms=latency_ms,
            pong_ok=True,
        )
        with self._lock:
            self._connectivity_status_cache = status
            self._connectivity_status_cache_ts = time.time()
        return status

    def _connectivity_status_cache_ttl_for_status(self, status: ConnectivityStatus | None) -> float:
        if status is None:
            return 0.0
        if bool(status.healthy):
            return float(self._connectivity_status_cache_ttl_sec)
        ttl_sec = float(self._connectivity_status_unhealthy_cache_ttl_sec)
        reason = str(status.reason or "").lower()
        if "request dispatch timeout" in reason:
            # Dispatch queue saturation is shared across all workers. Keep the
            # degraded result longer so many symbol workers do not stampede /session.
            ttl_sec = max(ttl_sec, 15.0)
        return ttl_sec

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        if not self._connected:
            return StreamHealthStatus(
                healthy=False,
                connected=False,
                reason="broker_not_connected",
                symbol=symbol.upper() if symbol else None,
            )

        now = time.time()
        upper = symbol.upper() if symbol else None

        with self._lock:
            stream_thread = self._stream_thread
            stream_enabled = self._stream_enabled
            legacy_stream_connected = (
                self._stream_session_id is not None
                and stream_thread is not None
                and stream_thread.is_alive()
            )
            sdk_stream_connected = bool(self._stream_use_sdk and self._stream_sdk_client and self._stream_sdk_connected)
            stream_connected = legacy_stream_connected or sdk_stream_connected

            last_tick_age_sec: float | None = None
            quote_stagnation_age_sec: float | None = None
            if upper:
                tick = self._tick_cache.get(upper)
                if tick is not None:
                    last_tick_age_sec = max(0.0, now - tick.timestamp)
                changed_at = self._stream_last_quote_change_ts_by_symbol.get(upper)
                if changed_at is not None:
                    quote_stagnation_age_sec = max(0.0, now - changed_at)
                symbol_subscription_active = upper in self._stream_symbol_to_table
            elif self._tick_cache:
                freshest_ts = max(
                    float(getattr(item, "received_at", item.timestamp) or item.timestamp)
                    for item in self._tick_cache.values()
                )
                last_tick_age_sec = max(0.0, now - freshest_ts)
                symbol_subscription_active = False
            else:
                symbol_subscription_active = False

            next_retry_in_sec = None
            if self._stream_next_retry_at is not None:
                next_retry_in_sec = max(0.0, self._stream_next_retry_at - now)

            desired_subscriptions = len(self._stream_desired_subscriptions)
            active_subscriptions = len(self._stream_symbol_to_table)
            last_error = self._stream_last_error
            reconnect_attempts = self._stream_reconnect_attempts
            total_reconnects = self._stream_total_reconnects
            last_disconnect_ts = self._stream_last_disconnect_ts
            last_reconnect_ts = self._stream_last_reconnect_ts
            stream_endpoint_configured = self._stream_endpoint is not None
            price_requests_total = self._price_requests_total
            stream_hits_total = self._stream_price_hits_total
            rest_fallback_hits_total = self._rest_fallback_hits_total

        stream_hit_rate_pct: float | None = None
        if price_requests_total > 0:
            stream_hit_rate_pct = (stream_hits_total / price_requests_total) * 100.0

        if not stream_enabled:
            reason = "stream_disabled_rest_only"
        elif stream_connected:
            if upper and upper in self._stream_desired_subscriptions:
                if not symbol_subscription_active:
                    if last_error and "subscription_failed:" in str(last_error):
                        reason = f"stream_subscription_failed_rest_fallback:{last_error}"
                    else:
                        reason = "stream_subscription_pending_rest_fallback"
                elif last_tick_age_sec is None:
                    reason = "stream_warmup_rest_fallback"
                elif last_tick_age_sec > max(0.1, max_tick_age_sec):
                    reason = f"stream_tick_stale_rest_fallback:{last_tick_age_sec:.2f}s>{max_tick_age_sec:.2f}s"
                elif (
                    self._stream_stagnant_quote_max_age_sec > 0
                    and quote_stagnation_age_sec is not None
                    and quote_stagnation_age_sec > self._stream_stagnant_quote_max_age_sec
                ):
                    reason = (
                        "stream_quote_stagnant_rest_fallback:"
                        f"{quote_stagnation_age_sec:.2f}s>{self._stream_stagnant_quote_max_age_sec:.2f}s"
                    )
                else:
                    reason = "ok"
            else:
                reason = "ok"
        else:
            if self._stream_sdk_enabled and not self._stream_sdk_available:
                reason = "lightstreamer_sdk_missing_rest_fallback"
            elif not stream_endpoint_configured:
                reason = "stream_endpoint_missing_rest_fallback"
            elif last_error:
                reason = f"stream_disconnected_rest_fallback:{last_error}"
            else:
                reason = "stream_disconnected_rest_fallback"

        # IG client keeps REST fallback active; stream degradation should not hard-block openings.
        return StreamHealthStatus(
            healthy=True,
            connected=stream_connected,
            reason=reason,
            symbol=upper,
            last_tick_age_sec=last_tick_age_sec,
            reconnect_attempts=reconnect_attempts,
            total_reconnects=total_reconnects,
            last_disconnect_ts=last_disconnect_ts,
            last_reconnect_ts=last_reconnect_ts,
            next_retry_in_sec=next_retry_in_sec,
            desired_subscriptions=desired_subscriptions,
            active_subscriptions=active_subscriptions,
            last_error=last_error,
            price_requests_total=price_requests_total,
            stream_hits_total=stream_hits_total,
            rest_fallback_hits_total=rest_fallback_hits_total,
            stream_hit_rate_pct=stream_hit_rate_pct,
        )

    def open_position(
        self,
        symbol: str,
        side: Side,
        volume: float,
        stop_loss: float,
        take_profit: float,
        comment: str,
        entry_price: float | None = None,
    ) -> str:
        with self._critical_trade_operation(f"open_position:{str(symbol).upper()}"):
            return self._open_position_impl(
                symbol=symbol,
                side=side,
                volume=volume,
                stop_loss=stop_loss,
                take_profit=take_profit,
                comment=comment,
                entry_price=entry_price,
            )

    def _open_position_impl(
        self,
        symbol: str,
        side: Side,
        volume: float,
        stop_loss: float,
        take_profit: float,
        comment: str,
        entry_price: float | None = None,
    ) -> str:
        direction = "BUY" if side == Side.BUY else "SELL"
        cooldown_remaining = self._allowance_cooldown_remaining()
        with self._lock:
            allowance_last_error = str(self._allowance_last_error or "")
        if cooldown_remaining > 0 and self._is_trade_allowance_error_text(allowance_last_error):
            raise BrokerError(
                f"IG public API allowance cooldown is active ({cooldown_remaining:.1f}s remaining)"
            )
        last_epic_unavailable: BrokerError | None = None
        last_rejected_error: BrokerError | None = None
        reference_spec = self._get_cached_symbol_spec(symbol)
        allow_market_metadata_requests = cooldown_remaining <= 0.0
        if not allow_market_metadata_requests:
            reason = f"allowance cooldown ({cooldown_remaining:.1f}s remaining)"
            logger.info(
                "IG open_position using cached/fallback metadata for %s (%s)",
                str(symbol).upper(),
                reason,
            )

        attempts = self._epic_attempt_order(symbol)
        open_attempt_index = 0
        for epic in attempts:
            try:
                candidate_spec, candidate_body = self._resolve_open_candidate_spec(
                    symbol=symbol,
                    epic=epic,
                    reference_spec=reference_spec,
                    allow_market_details=allow_market_metadata_requests,
                )
            except BrokerError as exc:
                error_text = str(exc)
                if self._is_epic_unavailable_error_text(error_text):
                    last_epic_unavailable = exc
                    continue
                raise
            if reference_spec is None:
                reference_spec = self._clone_symbol_spec_for_epic(
                    symbol,
                    epic,
                    candidate_spec,
                    origin="open_reference_seed",
                )
            candidate_reference_spec = self._get_cached_symbol_spec_for_epic(symbol, epic)
            if candidate_reference_spec is None and reference_spec is not None:
                reference_metadata = (
                    reference_spec.metadata
                    if isinstance(reference_spec.metadata, dict)
                    else {}
                )
                if str(reference_metadata.get("epic") or "").strip().upper() == str(epic).strip().upper():
                    candidate_reference_spec = reference_spec

            currency_attempts = self._order_currency_attempts(
                symbol,
                candidate_spec,
                allow_account_refresh=False,
            )
            effective_lot_min = float(candidate_spec.lot_min)
            trusted_reference_lot_min = self._trusted_cached_lot_min(candidate_reference_spec)
            if trusted_reference_lot_min is not None:
                effective_lot_min = max(effective_lot_min, trusted_reference_lot_min)
            if float(volume) + FLOAT_ROUNDING_TOLERANCE < effective_lot_min:
                self._promote_symbol_lot_min_from_market_spec(
                    symbol=symbol,
                    epic=epic,
                    candidate_spec=candidate_spec,
                    attempted_volume=float(volume),
                )
                lot_min_source = str(candidate_spec.metadata.get("lot_min_source") or "unknown")
                if trusted_reference_lot_min is not None and trusted_reference_lot_min > float(candidate_spec.lot_min):
                    lot_min_source = "cached_adaptive_min"
                last_rejected_error = BrokerError(
                    f"Requested size below broker minimum for {str(symbol).upper()} "
                    f"(requested={float(volume):g}, min={effective_lot_min:g}, "
                    f"epic={epic}, "
                    f"lot_min_source={lot_min_source})"
                )
                logger.warning(
                    "IG open_position skipping epic=%s for %s because requested size %.6f is below broker min %.6f",
                    epic,
                    str(symbol).upper(),
                    float(volume),
                    effective_lot_min,
                )
                continue
            candidate_volume = _normalize_volume_floor_for_spec(candidate_spec, float(volume))
            if candidate_volume <= 0:
                last_rejected_error = BrokerError(
                    f"Requested size below broker minimum after step normalization for {str(symbol).upper()} "
                    f"(requested={float(volume):g}, normalized={candidate_volume:g}, "
                    f"min={effective_lot_min:g}, step={float(candidate_spec.lot_step):g}, epic={epic})"
                )
                logger.warning(
                    "IG open_position skipping epic=%s for %s because normalized size %.6f is below broker minimum %.6f "
                    "(requested=%.6f step=%.6f)",
                    epic,
                    str(symbol).upper(),
                    candidate_volume,
                    effective_lot_min,
                    float(volume),
                    float(candidate_spec.lot_step),
                )
                continue
            advance_epic = False
            for currency_code in currency_attempts:
                candidate_entry, candidate_stop_loss, candidate_take_profit = self._remap_open_levels_for_candidate(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    requested_stop_loss=stop_loss,
                    requested_take_profit=take_profit,
                    reference_spec=reference_spec,
                    candidate_spec=candidate_spec,
                    candidate_body=candidate_body,
                )
                use_guaranteed_stop = self._should_use_guaranteed_stop(symbol, candidate_spec)
                min_stop_distance = self._effective_min_stop_distance_for_open(
                    candidate_spec,
                    use_guaranteed_stop,
                )
                candidate_stop_loss = self._enforce_open_stop_distance(
                    side=side,
                    entry=candidate_entry,
                    stop_loss=candidate_stop_loss,
                    spec=candidate_spec,
                    min_distance=min_stop_distance,
                )
                attempt_reference = _format_open_deal_reference_for_attempt(comment, open_attempt_index)
                open_attempt_index += 1
                payload = {
                    "epic": epic,
                    "expiry": "-",
                    "direction": direction,
                    "size": float(candidate_volume),
                    "timeInForce": "EXECUTE_AND_ELIMINATE",
                    "forceOpen": bool(self._open_force_open),
                    "guaranteedStop": bool(use_guaranteed_stop),
                    "currencyCode": currency_code,
                    "stopLevel": float(candidate_stop_loss),
                    "limitLevel": float(candidate_take_profit),
                    "dealReference": attempt_reference,
                }
                order_type = "MARKET"
                if self._open_use_quote_id:
                    quote_id = self._extract_quote_id_from_market_body(candidate_body)
                    open_level = self._open_level_with_tolerance(
                        side=side,
                        entry=candidate_entry,
                        spec=candidate_spec,
                    )
                    if quote_id and open_level is not None:
                        order_type = "QUOTE"
                        payload["quoteId"] = quote_id
                        payload["level"] = float(open_level)
                payload["orderType"] = order_type
                try:
                    body, _ = self._request("POST", "/positions/otc", payload=payload, version="2", auth=True)
                except BrokerError as exc:
                    error_text = str(exc)
                    if self._is_instrument_invalid_error_text(error_text):
                        self._mark_epic_temporarily_invalid(symbol, epic, reason=error_text)
                    if self._is_minimum_order_size_error_text(error_text):
                        self._promote_symbol_lot_min_after_reject(
                            symbol=symbol,
                            epic=epic,
                            candidate_spec=candidate_spec,
                            attempted_volume=float(candidate_volume),
                            reason="ORDER_SIZE_INCREMENT_ERROR"
                            if (
                                "set increments" in error_text.lower()
                                or "invalid_size" in error_text.lower()
                                or "invalid.size" in error_text.lower()
                                or "invalid size" in error_text.lower()
                            )
                            else "MINIMUM_ORDER_SIZE_ERROR",
                        )
                    self._maybe_start_allowance_cooldown(error_text, "trade open")
                    contextual_error = BrokerError(
                        f"{error_text} | epic={epic} direction={direction} "
                        f"size={float(candidate_volume):g} currency={currency_code} "
                        f"stop={float(candidate_stop_loss):g} limit={float(candidate_take_profit):g}"
                    )
                    if self._is_guaranteed_stop_required_text(error_text) and not use_guaranteed_stop:
                        self._mark_symbol_guaranteed_stop_required(symbol, source="open_post_reject")
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position retrying with guaranteed stop for %s after POST reject: epic=%s currency=%s error=%s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        advance_epic = True
                        break
                    if self._is_epic_unavailable_error_text(error_text):
                        last_epic_unavailable = exc
                        break
                    if (
                        self._should_try_next_currency_on_open_post_error(error_text)
                        and currency_code != currency_attempts[-1]
                    ):
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position POST failed for %s on epic=%s currency=%s; trying next currency: %s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        continue
                    if self._should_try_next_epic_on_open_post_error(symbol, epic, error_text):
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position POST failed for %s on epic=%s currency=%s; trying next candidate: %s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        advance_epic = True
                        break
                    raise contextual_error

                deal_reference = str(body.get("dealReference") or "").strip()
                if not deal_reference:
                    raise BrokerError("IG API open position did not return dealReference")

                deadline = time.time() + max(1.0, self.confirm_timeout_sec)
                while time.time() < deadline:
                    try:
                        confirm, _ = self._request(
                            "GET",
                            f"/confirms/{deal_reference}",
                            version="1",
                            auth=True,
                        )
                    except BrokerError:
                        time.sleep(0.3)
                        continue

                    status = str(confirm.get("dealStatus") or "").upper()
                    reason = self._confirm_rejection_reason(confirm)
                    if status == "ACCEPTED":
                        self._reset_allowance_cooldown()
                        self._activate_epic(symbol, epic)
                        position_id = str(confirm.get("dealId") or deal_reference)
                        sync_payload = self._build_open_sync_payload(
                            position_id,
                            confirm,
                            deal_reference=deal_reference,
                            open_price=float(candidate_entry or entry_price or 0.0),
                            stop_loss=float(candidate_stop_loss),
                            take_profit=float(candidate_take_profit),
                            epic=epic,
                        )
                        self._cache_open_sync_payload(position_id, sync_payload)
                        return position_id
                    if status == "REJECTED":
                        self._maybe_start_allowance_cooldown(reason, "trade open confirm")
                        if self._is_instrument_invalid_error_text(reason):
                            self._mark_epic_temporarily_invalid(symbol, epic, reason=reason)
                        confirm_summary = self._compact_confirm_payload(confirm)
                        reject_error = BrokerError(
                            "IG deal rejected: "
                            f"{reason or 'UNKNOWN'} | epic={epic} direction={direction} "
                            f"size={float(candidate_volume):g} currency={currency_code} "
                            f"stop={float(candidate_stop_loss):g} limit={float(candidate_take_profit):g} "
                            f"confirm={confirm_summary}"
                        )
                        self._promote_symbol_lot_min_after_reject(
                            symbol=symbol,
                            epic=epic,
                            candidate_spec=candidate_spec,
                            attempted_volume=float(candidate_volume),
                            reason=reason,
                        )
                        if self._is_guaranteed_stop_required_text(reason) and not use_guaranteed_stop:
                            self._mark_symbol_guaranteed_stop_required(symbol, source="open_confirm_reject")
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position retrying with guaranteed stop for %s after confirm reject: epic=%s currency=%s reason=%s",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            advance_epic = True
                            break
                        if self._should_try_next_currency_on_open_reject(confirm) and currency_code != currency_attempts[-1]:
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position rejected for %s on epic=%s currency=%s with reason=%s; trying next currency",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            break
                        if self._should_try_next_epic_on_open_reject(symbol, epic, confirm):
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position rejected for %s on epic=%s currency=%s with reason=%s; trying next candidate",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            advance_epic = True
                            break
                        if self._should_probe_positions_after_open_reject(confirm):
                            recovered_open = self._recover_open_position_after_rejected_confirm(
                                symbol=symbol,
                                side=side,
                                volume=float(candidate_volume),
                                epic=epic,
                                deal_reference=deal_reference,
                                fallback_open_price=float(candidate_entry or entry_price or 0.0),
                                fallback_stop_loss=float(candidate_stop_loss),
                                fallback_take_profit=float(candidate_take_profit),
                            )
                            if recovered_open is not None:
                                self._reset_allowance_cooldown()
                                recovered_position_id = str(recovered_open.get("position_id") or "").strip()
                                if recovered_position_id:
                                    self._activate_epic(symbol, epic)
                                    recovered_sync = self._build_open_sync_payload(
                                        recovered_position_id,
                                        None,
                                        deal_reference=str(recovered_open.get("deal_reference") or deal_reference).strip(),
                                        open_price=float(recovered_open.get("open_price") or 0.0),
                                        stop_loss=float(recovered_open.get("stop_loss") or candidate_stop_loss),
                                        take_profit=float(recovered_open.get("take_profit") or candidate_take_profit),
                                        epic=str(recovered_open.get("epic") or epic),
                                    )
                                    recovered_sync["source"] = str(
                                        recovered_open.get("source") or "ig_positions_after_rejected_confirm"
                                    )
                                    self._cache_open_sync_payload(recovered_position_id, recovered_sync)
                                    logger.warning(
                                        "IG open_position recovered accepted deal after rejected confirm for %s | "
                                        "epic=%s deal_reference=%s recovered_position_id=%s",
                                        str(symbol).upper(),
                                        epic,
                                        str(deal_reference),
                                        recovered_position_id,
                                    )
                                    return recovered_position_id
                        raise reject_error
                    time.sleep(0.3)
                else:
                    return self._recover_open_position_after_confirm_timeout(
                        symbol=symbol,
                        side=side,
                        volume=float(candidate_volume),
                        epic=epic,
                        deal_reference=deal_reference,
                        fallback_open_price=float(candidate_entry or entry_price or 0.0),
                        fallback_stop_loss=float(candidate_stop_loss),
                        fallback_take_profit=float(candidate_take_profit),
                        direction=direction,
                        currency_code=currency_code,
                    )
                if advance_epic:
                    break

        dynamic_attempts: list[str] = []
        if not allow_market_metadata_requests:
            dynamic_attempts = []
        elif last_epic_unavailable is not None:
            dynamic_attempts = [
                epic
                for epic in self._extend_epic_candidates_from_search(symbol)
                if epic not in attempts
            ]
        elif last_rejected_error is not None:
            rejected_text = str(last_rejected_error).lower()
            should_probe_search = "requested size below broker minimum" not in rejected_text
            if should_probe_search:
                dynamic_attempts = [
                    epic
                    for epic in self._extend_epic_candidates_from_search(symbol)
                    if epic not in attempts
                ]
            else:
                dynamic_attempts = []

        for epic in dynamic_attempts:
            try:
                candidate_spec, candidate_body = self._resolve_open_candidate_spec(
                    symbol=symbol,
                    epic=epic,
                    reference_spec=reference_spec,
                    allow_market_details=allow_market_metadata_requests,
                )
            except BrokerError as exc:
                error_text = str(exc)
                if self._is_epic_unavailable_error_text(error_text):
                    last_epic_unavailable = exc
                    continue
                raise
            if reference_spec is None:
                reference_spec = self._clone_symbol_spec_for_epic(
                    symbol,
                    epic,
                    candidate_spec,
                    origin="open_reference_seed",
                )
                trusted_reference_lot_min = self._trusted_cached_lot_min(reference_spec)

            currency_attempts = self._order_currency_attempts(
                symbol,
                candidate_spec,
                allow_account_refresh=False,
            )
            effective_lot_min = float(candidate_spec.lot_min)
            if trusted_reference_lot_min is not None:
                effective_lot_min = max(effective_lot_min, trusted_reference_lot_min)
            if float(volume) + FLOAT_ROUNDING_TOLERANCE < effective_lot_min:
                self._promote_symbol_lot_min_from_market_spec(
                    symbol=symbol,
                    epic=epic,
                    candidate_spec=candidate_spec,
                    attempted_volume=float(volume),
                )
                lot_min_source = str(candidate_spec.metadata.get("lot_min_source") or "unknown")
                if trusted_reference_lot_min is not None and trusted_reference_lot_min > float(candidate_spec.lot_min):
                    lot_min_source = "cached_adaptive_min"
                last_rejected_error = BrokerError(
                    f"Requested size below broker minimum for {str(symbol).upper()} "
                    f"(requested={float(volume):g}, min={effective_lot_min:g}, "
                    f"epic={epic}, "
                    f"lot_min_source={lot_min_source})"
                )
                logger.warning(
                    "IG open_position skipping auto-discovered epic=%s for %s because requested size %.6f is below broker min %.6f",
                    epic,
                    str(symbol).upper(),
                    float(volume),
                    effective_lot_min,
                )
                continue
            candidate_volume = _normalize_volume_floor_for_spec(candidate_spec, float(volume))
            if candidate_volume <= 0:
                last_rejected_error = BrokerError(
                    f"Requested size below broker minimum after step normalization for {str(symbol).upper()} "
                    f"(requested={float(volume):g}, normalized={candidate_volume:g}, "
                    f"min={effective_lot_min:g}, step={float(candidate_spec.lot_step):g}, epic={epic})"
                )
                logger.warning(
                    "IG open_position skipping auto-discovered epic=%s for %s because normalized size %.6f "
                    "is below broker minimum %.6f (requested=%.6f step=%.6f)",
                    epic,
                    str(symbol).upper(),
                    candidate_volume,
                    effective_lot_min,
                    float(volume),
                    float(candidate_spec.lot_step),
                )
                continue
            advance_epic = False
            for currency_code in currency_attempts:
                candidate_entry, candidate_stop_loss, candidate_take_profit = self._remap_open_levels_for_candidate(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    requested_stop_loss=stop_loss,
                    requested_take_profit=take_profit,
                    reference_spec=reference_spec,
                    candidate_spec=candidate_spec,
                    candidate_body=candidate_body,
                )
                use_guaranteed_stop = self._should_use_guaranteed_stop(symbol, candidate_spec)
                min_stop_distance = self._effective_min_stop_distance_for_open(
                    candidate_spec,
                    use_guaranteed_stop,
                )
                candidate_stop_loss = self._enforce_open_stop_distance(
                    side=side,
                    entry=candidate_entry,
                    stop_loss=candidate_stop_loss,
                    spec=candidate_spec,
                    min_distance=min_stop_distance,
                )
                attempt_reference = _format_open_deal_reference_for_attempt(comment, open_attempt_index)
                open_attempt_index += 1
                payload = {
                    "epic": epic,
                    "expiry": "-",
                    "direction": direction,
                    "size": float(candidate_volume),
                    "timeInForce": "EXECUTE_AND_ELIMINATE",
                    "forceOpen": bool(self._open_force_open),
                    "guaranteedStop": bool(use_guaranteed_stop),
                    "currencyCode": currency_code,
                    "stopLevel": float(candidate_stop_loss),
                    "limitLevel": float(candidate_take_profit),
                    "dealReference": attempt_reference,
                }
                order_type = "MARKET"
                if self._open_use_quote_id:
                    quote_id = self._extract_quote_id_from_market_body(candidate_body)
                    open_level = self._open_level_with_tolerance(
                        side=side,
                        entry=candidate_entry,
                        spec=candidate_spec,
                    )
                    if quote_id and open_level is not None:
                        order_type = "QUOTE"
                        payload["quoteId"] = quote_id
                        payload["level"] = float(open_level)
                payload["orderType"] = order_type
                try:
                    body, _ = self._request("POST", "/positions/otc", payload=payload, version="2", auth=True)
                except BrokerError as exc:
                    error_text = str(exc)
                    if self._is_instrument_invalid_error_text(error_text):
                        self._mark_epic_temporarily_invalid(symbol, epic, reason=error_text)
                    if self._is_minimum_order_size_error_text(error_text):
                        self._promote_symbol_lot_min_after_reject(
                            symbol=symbol,
                            epic=epic,
                            candidate_spec=candidate_spec,
                            attempted_volume=float(candidate_volume),
                            reason="ORDER_SIZE_INCREMENT_ERROR"
                            if (
                                "set increments" in error_text.lower()
                                or "invalid_size" in error_text.lower()
                                or "invalid.size" in error_text.lower()
                                or "invalid size" in error_text.lower()
                            )
                            else "MINIMUM_ORDER_SIZE_ERROR",
                        )
                    self._maybe_start_allowance_cooldown(error_text, "trade open")
                    contextual_error = BrokerError(
                        f"{error_text} | epic={epic} direction={direction} "
                        f"size={float(candidate_volume):g} currency={currency_code} "
                        f"stop={float(candidate_stop_loss):g} limit={float(candidate_take_profit):g}"
                    )
                    if self._is_guaranteed_stop_required_text(error_text) and not use_guaranteed_stop:
                        self._mark_symbol_guaranteed_stop_required(symbol, source="open_post_reject")
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position retrying with guaranteed stop for %s after POST reject on auto-discovered epic=%s currency=%s error=%s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        advance_epic = True
                        break
                    if self._is_epic_unavailable_error_text(error_text):
                        last_epic_unavailable = exc
                        advance_epic = True
                        break
                    if (
                        self._should_try_next_currency_on_open_post_error(error_text)
                        and currency_code != currency_attempts[-1]
                    ):
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position POST failed for %s on auto-discovered epic=%s currency=%s; trying next currency: %s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        continue
                    if self._should_try_next_epic_on_open_post_error(symbol, epic, error_text):
                        last_rejected_error = contextual_error
                        logger.warning(
                            "IG open_position POST failed for %s on auto-discovered epic=%s currency=%s; trying next candidate: %s",
                            str(symbol).upper(),
                            epic,
                            currency_code,
                            error_text,
                        )
                        advance_epic = True
                        break
                    raise contextual_error

                deal_reference = str(body.get("dealReference") or "").strip()
                if not deal_reference:
                    raise BrokerError("IG API open position did not return dealReference")

                deadline = time.time() + max(1.0, self.confirm_timeout_sec)
                while time.time() < deadline:
                    try:
                        confirm, _ = self._request(
                            "GET",
                            f"/confirms/{deal_reference}",
                            version="1",
                            auth=True,
                        )
                    except BrokerError:
                        time.sleep(0.3)
                        continue

                    status = str(confirm.get("dealStatus") or "").upper()
                    reason = self._confirm_rejection_reason(confirm)
                    if status == "ACCEPTED":
                        self._reset_allowance_cooldown()
                        self._activate_epic(symbol, epic)
                        logger.warning(
                            "IG open_position epic auto-discovered for %s: using %s",
                            str(symbol).upper(),
                            epic,
                        )
                        position_id = str(confirm.get("dealId") or deal_reference)
                        sync_payload = self._build_open_sync_payload(
                            position_id,
                            confirm,
                            deal_reference=deal_reference,
                            open_price=float(candidate_entry or entry_price or 0.0),
                            stop_loss=float(candidate_stop_loss),
                            take_profit=float(candidate_take_profit),
                            epic=epic,
                        )
                        self._cache_open_sync_payload(position_id, sync_payload)
                        return position_id
                    if status == "REJECTED":
                        self._maybe_start_allowance_cooldown(reason, "trade open confirm")
                        if self._is_instrument_invalid_error_text(reason):
                            self._mark_epic_temporarily_invalid(symbol, epic, reason=reason)
                        confirm_summary = self._compact_confirm_payload(confirm)
                        reject_error = BrokerError(
                            "IG deal rejected: "
                            f"{reason or 'UNKNOWN'} | epic={epic} direction={direction} "
                            f"size={float(candidate_volume):g} currency={currency_code} "
                            f"stop={float(candidate_stop_loss):g} limit={float(candidate_take_profit):g} "
                            f"confirm={confirm_summary}"
                        )
                        self._promote_symbol_lot_min_after_reject(
                            symbol=symbol,
                            epic=epic,
                            candidate_spec=candidate_spec,
                            attempted_volume=float(candidate_volume),
                            reason=reason,
                        )
                        if self._is_guaranteed_stop_required_text(reason) and not use_guaranteed_stop:
                            self._mark_symbol_guaranteed_stop_required(symbol, source="open_confirm_reject")
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position retrying with guaranteed stop for %s after confirm reject on auto-discovered epic=%s currency=%s reason=%s",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            advance_epic = True
                            break
                        if self._should_try_next_currency_on_open_reject(confirm) and currency_code != currency_attempts[-1]:
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position rejected for %s on auto-discovered epic=%s currency=%s with reason=%s; trying next currency",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            break
                        if self._should_try_next_epic_on_open_reject(symbol, epic, confirm):
                            last_rejected_error = reject_error
                            logger.warning(
                                "IG open_position rejected for %s on auto-discovered epic=%s currency=%s with reason=%s; trying next candidate",
                                str(symbol).upper(),
                                epic,
                                currency_code,
                                reason,
                            )
                            advance_epic = True
                            break
                        if self._should_probe_positions_after_open_reject(confirm):
                            recovered_open = self._recover_open_position_after_rejected_confirm(
                                symbol=symbol,
                                side=side,
                                volume=float(candidate_volume),
                                epic=epic,
                                deal_reference=deal_reference,
                                fallback_open_price=float(candidate_entry or entry_price or 0.0),
                                fallback_stop_loss=float(candidate_stop_loss),
                                fallback_take_profit=float(candidate_take_profit),
                            )
                            if recovered_open is not None:
                                self._reset_allowance_cooldown()
                                recovered_position_id = str(recovered_open.get("position_id") or "").strip()
                                if recovered_position_id:
                                    self._activate_epic(symbol, epic)
                                    logger.warning(
                                        "IG open_position recovered accepted deal after rejected confirm for %s | "
                                        "epic=%s deal_reference=%s recovered_position_id=%s",
                                        str(symbol).upper(),
                                        epic,
                                        str(deal_reference),
                                        recovered_position_id,
                                    )
                                    recovered_sync = self._build_open_sync_payload(
                                        recovered_position_id,
                                        None,
                                        deal_reference=str(recovered_open.get("deal_reference") or deal_reference).strip(),
                                        open_price=float(recovered_open.get("open_price") or 0.0),
                                        stop_loss=float(recovered_open.get("stop_loss") or candidate_stop_loss),
                                        take_profit=float(recovered_open.get("take_profit") or candidate_take_profit),
                                        epic=str(recovered_open.get("epic") or epic),
                                    )
                                    recovered_sync["source"] = str(
                                        recovered_open.get("source") or "ig_positions_after_rejected_confirm"
                                    )
                                    self._cache_open_sync_payload(recovered_position_id, recovered_sync)
                                    return recovered_position_id
                        raise reject_error
                    time.sleep(0.3)
                else:
                    return self._recover_open_position_after_confirm_timeout(
                        symbol=symbol,
                        side=side,
                        volume=float(candidate_volume),
                        epic=epic,
                        deal_reference=deal_reference,
                        fallback_open_price=float(candidate_entry or entry_price or 0.0),
                        fallback_stop_loss=float(candidate_stop_loss),
                        fallback_take_profit=float(candidate_take_profit),
                        direction=direction,
                        currency_code=currency_code,
                        auto_discovered=True,
                    )
                if advance_epic:
                    break

        if last_rejected_error is not None:
            raise last_rejected_error
        if last_epic_unavailable is not None:
            raise last_epic_unavailable
        raise BrokerError(f"No IG epic mapping candidates for symbol {symbol.upper()}")

    def close_position(self, position: Position, volume: float | None = None) -> None:
        symbol = str(position.symbol).upper().strip()
        with self._critical_trade_operation(f"close_position:{symbol}"):
            direction = "SELL" if position.side == Side.BUY else "BUY"
            deal_id = str(position.position_id or "").strip()
            if not deal_id:
                raise BrokerError("close position requires non-empty dealId")
            close_volume = float(position.volume if volume is None else volume)
            if not math.isfinite(close_volume) or close_volume <= 0:
                raise BrokerError("close volume must be positive")
            if close_volume - float(position.volume) > FLOAT_COMPARISON_TOLERANCE:
                raise BrokerError(
                    f"close volume exceeds open position volume (requested={close_volume:g}, open={float(position.volume):g})"
                )
            payload = {
                "dealId": deal_id,
                "direction": direction,
                "size": close_volume,
                "orderType": "MARKET",
                "timeInForce": "EXECUTE_AND_ELIMINATE",
            }
            payload_without_tif = {
                "dealId": deal_id,
                "direction": direction,
                "size": close_volume,
                "orderType": "MARKET",
            }
            retry_payload = {
                "dealId": deal_id,
                "direction": direction,
                "size": close_volume,
            }
            override_headers = {
                "_method": "DELETE",
                "X-HTTP-Method-Override": "DELETE",
            }

            # IG FAQ explicitly recommends POST + _method: DELETE when DELETE payloads are not
            # reliably forwarded by the client stack or intermediate proxies.
            attempts: list[tuple[str, dict[str, Any], dict[str, str] | None, dict[str, Any] | None]] = [
                ("POST", payload, override_headers, {"_method": "DELETE"}),
                ("POST", payload_without_tif, override_headers, {"_method": "DELETE"}),
                ("POST", retry_payload, override_headers, {"_method": "DELETE"}),
                ("DELETE", payload, None, None),
                ("DELETE", payload_without_tif, None, None),
                ("DELETE", retry_payload, None, None),
            ]

            body: dict[str, Any] | None = None
            last_close_error: BrokerError | None = None
            for method_name, attempt_payload, attempt_headers, attempt_query in attempts:
                try:
                    body, _ = self._request(
                        method_name,
                        "/positions/otc",
                        payload=attempt_payload,
                        version="1",
                        auth=True,
                        extra_headers=attempt_headers,
                        query=attempt_query,
                    )
                    break
                except BrokerError as exc:
                    last_close_error = exc
                    if not self._is_retryable_close_request_error_text(str(exc)):
                        raise
                    continue
            if body is None:
                if last_close_error is not None:
                    raise last_close_error
                raise BrokerError(
                    "IG close position failed: no close request attempts were executed "
                    f"(position_id={position.position_id})"
                )
            deal_reference = str(body.get("dealReference") or "").strip()
            if not deal_reference:
                raise BrokerError(
                    "IG close position did not return dealReference "
                    f"(position_id={position.position_id})"
                )

            confirm = self._wait_for_deal_confirm(deal_reference)
            if not isinstance(confirm, dict):
                raise BrokerError(
                    "IG close deal confirm unavailable "
                    f"(position_id={position.position_id}, deal_reference={deal_reference})"
                )
            status = str(confirm.get("dealStatus") or "").upper()
            reason = str(confirm.get("reason") or "").upper()
            if status == "REJECTED":
                raise BrokerError(f"IG close deal rejected: {reason or 'unknown'}")
            if status != "ACCEPTED":
                raise BrokerError(
                    "IG close deal not accepted "
                    f"(status={status or 'UNKNOWN'}, reason={reason or 'UNKNOWN'}, "
                    f"position_id={position.position_id}, deal_reference={deal_reference})"
                )

            sync_payload = self._extract_close_sync_from_confirm(position.position_id, confirm)
            sync_payload["closed_volume"] = close_volume
            sync_payload["close_complete"] = close_volume >= (float(position.volume) - FLOAT_COMPARISON_TOLERANCE)
            sync_payload["_cache_ts"] = time.time()
            with self._lock:
                self._position_close_sync[str(position.position_id)] = sync_payload

    def modify_position(self, position: Position, stop_loss: float, take_profit: float) -> None:
        symbol = str(position.symbol).upper().strip()
        with self._critical_trade_operation(f"modify_position:{symbol}"):
            payload = {
                "stopLevel": float(stop_loss),
                "limitLevel": float(take_profit),
            }
            self._request(
                "PUT",
                f"/positions/otc/{position.position_id}",
                payload=payload,
                version="2",
                auth=True,
            )
