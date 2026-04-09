from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from xtb_bot.ig_client import IgApiClient

from xtb_bot.client import BrokerError

logger = logging.getLogger(__name__)


class IgEpicMixin:
    """Epic resolution, search, and failover methods for IgApiClient."""

    __slots__ = ()

    # ------------------------------------------------------------------
    # Epic lookup
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Static error-text classification
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Epic search & scoring
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_search_item_epic(item: dict[str, Any]) -> str | None:
        from xtb_bot.ig_client import _as_mapping

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
        from xtb_bot.ig_client import (
            COMMODITY_EPIC_HINTS,
            _crypto_symbol_base,
            _is_index_symbol,
        )

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
        from xtb_bot.ig_client import IG_EPIC_SEARCH_TERMS, _is_fx_pair_symbol

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
        from xtb_bot.ig_client import _dedupe_epics, _is_plausible_epic_for_symbol

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
        from xtb_bot.ig_client import _dedupe_epics

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

    # ------------------------------------------------------------------
    # Epic activation / remapping
    # ------------------------------------------------------------------

    def _activate_epic(self, symbol: str, epic: str, *, log_warning: bool = True) -> None:
        from xtb_bot.ig_client import _dedupe_epics, _is_plausible_epic_for_symbol

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

    # ------------------------------------------------------------------
    # Epic attempt ordering & failover helpers
    # ------------------------------------------------------------------

    def _epic_attempt_order(self, symbol: str) -> list[str]:
        from xtb_bot.ig_client import (
            DEFAULT_IG_EPIC_CANDIDATES,
            _dedupe_epics,
            _first_non_empty_text,
            _is_plausible_epic_for_symbol,
        )

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

    # ------------------------------------------------------------------
    # Epic unavailability tracking
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Temporary-invalid epic tracking
    # ------------------------------------------------------------------

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
