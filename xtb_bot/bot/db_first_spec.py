from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from xtb_bot.models import SymbolSpec
from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotDbFirstSpecRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        cfg = bot.config

        self._db_first_symbol_spec_poll_interval_sec = max(
            1.0,
            bot._env_float("XTB_DB_FIRST_SYMBOL_SPEC_POLL_INTERVAL_SEC", 6.0),
        )
        db_first_symbol_spec_refresh_age_sec = bot._env_float(
            "XTB_DB_FIRST_SYMBOL_SPEC_REFRESH_AGE_SEC",
            bot._env_float("XTB_DB_FIRST_SYMBOL_SPEC_MAX_AGE_SEC", 900.0),
        )
        self._db_first_symbol_spec_refresh_age_sec = max(10.0, db_first_symbol_spec_refresh_age_sec)
        self._db_first_symbol_spec_retry_after_ts_by_symbol: dict[str, float] = {}
        self._db_first_symbol_spec_retry_backoff_sec_by_symbol: dict[str, float] = {}

    def _symbols_for_db_first_symbol_spec_refresh(self, now_utc=None):
        return self._bot._runtime_symbols_for_db_first_requests(now_utc)

    def _db_first_symbol_spec_cache_loop(self) -> None:
        cursor = 0
        request_interval_sec = self._db_first_symbol_spec_poll_interval_sec
        while not self._bot.stop_event.is_set() and not self._bot._db_first_cache_stop_event.is_set():
            symbols = self._symbols_for_db_first_symbol_spec_refresh()
            if not symbols:
                self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)
                continue
            symbols_to_refresh = [
                symbol
                for symbol in symbols
                if self._bot.store.load_broker_symbol_spec(
                    symbol,
                    max_age_sec=self._db_first_symbol_spec_refresh_age_sec,
                )
                is None
            ]
            symbols_to_refresh, next_retry_remaining_sec = self._filter_db_first_symbol_spec_refresh_candidates(
                symbols_to_refresh
            )
            if not symbols_to_refresh:
                wait_timeout_sec = request_interval_sec
                if next_retry_remaining_sec is not None:
                    wait_timeout_sec = max(1.0, min(120.0, next_retry_remaining_sec))
                self._bot._db_first_cache_stop_event.wait(timeout=wait_timeout_sec)
                continue
            backoff_remaining = self._bot._broker_public_api_backoff_remaining_sec()
            if backoff_remaining > 0.0:
                self._bot._db_first_cache_stop_event.wait(timeout=max(0.5, min(backoff_remaining, request_interval_sec)))
                continue
            symbol = symbols_to_refresh[cursor % len(symbols_to_refresh)]
            cursor += 1
            if not self._bot._reserve_ig_non_trading_budget(
                scope="db_first_symbol_spec",
                wait_timeout_sec=min(0.5, request_interval_sec),
            ):
                self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)
                continue
            try:
                spec = self._bot.broker.get_symbol_spec(symbol)
                self._bot.store.upsert_broker_symbol_spec(
                    symbol=symbol,
                    spec=spec,
                    ts=time.time(),
                    source="db_first_symbol_spec_cache",
                )
                self._clear_db_first_symbol_spec_refresh_retry(symbol)
            except Exception as exc:
                self._record_db_first_symbol_spec_refresh_failure(
                    symbol,
                    str(exc),
                    request_interval_sec=request_interval_sec,
                )
                if not self._bot._is_allowance_related_error(str(exc)):
                    logger.debug("DB-first symbol spec refresh failed for %s: %s", symbol, exc)
            self._bot._db_first_cache_stop_event.wait(timeout=request_interval_sec)

    def _filter_db_first_symbol_spec_refresh_candidates(
        self,
        symbols: list[str],
        *,
        now_ts: float | None = None,
    ) -> tuple[list[str], float | None]:
        current_ts = time.time() if now_ts is None else float(now_ts)
        eligible: list[str] = []
        next_retry_remaining_sec: float | None = None
        for symbol in symbols:
            upper = str(symbol).strip().upper()
            retry_after_ts = float(self._db_first_symbol_spec_retry_after_ts_by_symbol.get(upper, 0.0))
            remaining_sec = retry_after_ts - current_ts
            if remaining_sec > 0.0:
                if next_retry_remaining_sec is None or remaining_sec < next_retry_remaining_sec:
                    next_retry_remaining_sec = remaining_sec
                continue
            eligible.append(symbol)
        return eligible, next_retry_remaining_sec

    def _record_db_first_symbol_spec_refresh_failure(
        self,
        symbol: str,
        error_text: str,
        *,
        request_interval_sec: float,
        now_ts: float | None = None,
    ) -> float:
        upper = str(symbol).strip().upper()
        if not upper:
            return 0.0
        current_ts = time.time() if now_ts is None else float(now_ts)
        prior_backoff_sec = float(self._db_first_symbol_spec_retry_backoff_sec_by_symbol.get(upper, 0.0))
        base_backoff_sec = max(30.0, float(request_interval_sec) * 2.0)
        next_backoff_sec = min(
            1800.0,
            max(
                base_backoff_sec,
                prior_backoff_sec * 2.0 if prior_backoff_sec > 0.0 else base_backoff_sec,
            ),
        )
        self._db_first_symbol_spec_retry_backoff_sec_by_symbol[upper] = next_backoff_sec
        self._db_first_symbol_spec_retry_after_ts_by_symbol[upper] = current_ts + next_backoff_sec
        return next_backoff_sec

    def _clear_db_first_symbol_spec_refresh_retry(self, symbol: str) -> None:
        upper = str(symbol).strip().upper()
        if not upper:
            return
        self._db_first_symbol_spec_retry_after_ts_by_symbol.pop(upper, None)
        self._db_first_symbol_spec_retry_backoff_sec_by_symbol.pop(upper, None)

    @staticmethod
    def _symbol_spec_change_payload(
        previous: SymbolSpec | None,
        current: SymbolSpec,
    ) -> dict[str, dict[str, object]] | None:
        if previous is None:
            return None

        def _changed_float(before: float, after: float) -> bool:
            return not math.isclose(float(before), float(after), rel_tol=FLOAT_COMPARISON_TOLERANCE, abs_tol=FLOAT_ROUNDING_TOLERANCE)

        changes: dict[str, dict[str, object]] = {}
        if _changed_float(previous.tick_size, current.tick_size):
            changes["tick_size"] = {"previous": float(previous.tick_size), "current": float(current.tick_size)}
        if _changed_float(previous.tick_value, current.tick_value):
            changes["tick_value"] = {"previous": float(previous.tick_value), "current": float(current.tick_value)}
        if _changed_float(previous.contract_size, current.contract_size):
            changes["contract_size"] = {
                "previous": float(previous.contract_size),
                "current": float(current.contract_size),
            }
        if _changed_float(previous.lot_min, current.lot_min):
            changes["lot_min"] = {"previous": float(previous.lot_min), "current": float(current.lot_min)}
        if _changed_float(previous.lot_max, current.lot_max):
            changes["lot_max"] = {"previous": float(previous.lot_max), "current": float(current.lot_max)}
        if _changed_float(previous.lot_step, current.lot_step):
            changes["lot_step"] = {"previous": float(previous.lot_step), "current": float(current.lot_step)}
        if (
            previous.min_stop_distance_price is not None
            or current.min_stop_distance_price is not None
        ):
            previous_stop = (
                float(previous.min_stop_distance_price)
                if previous.min_stop_distance_price is not None
                else None
            )
            current_stop = (
                float(current.min_stop_distance_price)
                if current.min_stop_distance_price is not None
                else None
            )
            if previous_stop != current_stop:
                changes["min_stop_distance_price"] = {"previous": previous_stop, "current": current_stop}

        previous_meta = previous.metadata if isinstance(previous.metadata, dict) else {}
        current_meta = current.metadata if isinstance(current.metadata, dict) else {}
        previous_epic = str(previous_meta.get("epic") or "").strip().upper()
        current_epic = str(current_meta.get("epic") or "").strip().upper()
        if previous_epic != current_epic:
            changes["epic"] = {"previous": previous_epic, "current": current_epic}

        return changes or None

    def _symbol_spec_preload_is_strict(self) -> bool:
        if self._bot.config.broker != "ig":
            return False
        for candidate in self._broker_candidate_chain():
            connected_raw = getattr(candidate, "_connected", None)
            if isinstance(connected_raw, bool):
                return connected_raw
        return True

    def _broker_candidate_chain(self, max_depth: int = 4) -> list[object]:
        chain: list[object] = []
        candidate: object = self._bot.broker
        for _ in range(max(1, int(max_depth))):
            chain.append(candidate)
            underlying = getattr(candidate, "underlying", None)
            if underlying is None or underlying is candidate:
                break
            candidate = underlying
        return chain

    @staticmethod
    def _is_startup_symbol_spec_fallback_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return (
            "request dispatch timeout" in lowered
            or "epic.unavailable" in lowered
            or "local token bucket timeout" in lowered
        )

    def _build_startup_symbol_spec_fallback(
        self,
        symbol: str,
        *,
        error_text: str = "",
    ) -> SymbolSpec | None:
        upper_symbol = str(symbol).strip().upper()
        normalized_error = str(error_text or "").strip()
        allowance_related = (
            self._bot._is_allowance_related_error(normalized_error)
            or self._bot._broker_public_api_backoff_remaining_sec() > 0.0
        )
        if (
            normalized_error
            and not allowance_related
            and not self._is_startup_symbol_spec_fallback_error(normalized_error)
        ):
            return None

        for candidate in self._broker_candidate_chain():
            epic_order_getter = getattr(candidate, "_epic_attempt_order", None)
            fallback_builder = getattr(candidate, "_build_fallback_symbol_spec_for_epic", None)
            if not callable(epic_order_getter) or not callable(fallback_builder):
                continue

            try:
                attempts = epic_order_getter(upper_symbol)
            except Exception:
                continue
            normalized_attempts = [
                str(epic).strip().upper()
                for epic in (attempts or [])
                if str(epic).strip()
            ]
            epic = normalized_attempts[0] if normalized_attempts else ""
            if not epic:
                cached_spec_getter = getattr(candidate, "_get_cached_symbol_spec", None)
                if callable(cached_spec_getter):
                    try:
                        cached_spec = cached_spec_getter(upper_symbol)
                    except Exception:
                        cached_spec = None
                    if cached_spec is not None and isinstance(cached_spec.metadata, dict):
                        epic = str(cached_spec.metadata.get("epic") or "").strip().upper()
            if not epic:
                continue

            try:
                spec = fallback_builder(upper_symbol, epic)
            except Exception:
                continue

            metadata = dict(spec.metadata) if isinstance(spec.metadata, dict) else {}
            metadata["spec_origin"] = (
                "critical_fallback_allowance" if allowance_related else "critical_fallback_startup"
            )
            metadata["startup_fallback_reason"] = self._bot._slug_reason(
                normalized_error,
                fallback="startup_preload",
            )
            if normalized_error:
                metadata["startup_fallback_error"] = normalized_error[:240]
            spec.metadata = metadata
            return spec
        return None

    def _preload_symbol_specs_on_startup(self) -> None:
        if not self._bot._db_first_enabled():
            return
        getter = getattr(self._bot.broker, "get_symbol_spec", None)
        if not callable(getter):
            return

        symbols = self._bot._symbols_for_db_first_cache()
        if not symbols:
            return

        loaded_live_count = 0
        loaded_cached_count = 0
        loaded_fallback_count = 0
        failed: list[dict[str, str]] = []
        allowance_short_circuit = False

        def _store_preloaded_spec(symbol: str, spec: SymbolSpec, *, source: str) -> None:
            self._bot.store.upsert_broker_symbol_spec(
                symbol=symbol,
                spec=spec,
                ts=time.time(),
                source=source,
            )

        def _load_startup_fallback_spec(symbol: str, *, error_text: str) -> SymbolSpec | None:
            spec = self._build_startup_symbol_spec_fallback(
                symbol,
                error_text=error_text,
            )
            if spec is None:
                return None
            _store_preloaded_spec(
                symbol,
                spec,
                source="startup_symbol_spec_preload_fallback",
            )
            return spec

        for symbol in symbols:
            if self._bot.stop_event.is_set():
                break
            cached_fresh = self._bot.store.load_broker_symbol_spec(
                symbol,
                max_age_sec=self._db_first_symbol_spec_refresh_age_sec,
            )
            if cached_fresh is not None:
                loaded_cached_count += 1
                continue

            previous = self._bot.store.load_broker_symbol_spec(symbol, max_age_sec=0.0)
            if allowance_short_circuit or self._bot._broker_public_api_backoff_remaining_sec() > 0.0:
                if previous is not None:
                    loaded_cached_count += 1
                else:
                    fallback_spec = _load_startup_fallback_spec(
                        symbol,
                        error_text="allowance_backoff_active",
                    )
                    if fallback_spec is not None:
                        loaded_fallback_count += 1
                    else:
                        failed.append({"symbol": symbol, "error": "allowance_backoff_active"})
                continue

            budget_acquired = False
            wait_started_mono = time.monotonic()
            while not self._bot.stop_event.is_set():
                if self._bot._reserve_ig_non_trading_budget(
                    scope="startup_symbol_spec_preload",
                    wait_timeout_sec=1.0,
                ):
                    budget_acquired = True
                    break
                if (time.monotonic() - wait_started_mono) >= 30.0:
                    failed.append({"symbol": symbol, "error": "local_non_trading_budget_timeout"})
                    break
            if not budget_acquired:
                continue

            try:
                spec = getter(symbol)
                _store_preloaded_spec(
                    symbol,
                    spec,
                    source="startup_symbol_spec_preload",
                )
            except Exception as exc:
                error_text = str(exc)
                if self._bot._is_allowance_related_error(error_text):
                    allowance_short_circuit = True
                    if previous is not None:
                        loaded_cached_count += 1
                    else:
                        fallback_spec = _load_startup_fallback_spec(
                            symbol,
                            error_text=error_text,
                        )
                        if fallback_spec is not None:
                            loaded_fallback_count += 1
                        else:
                            failed.append({"symbol": symbol, "error": error_text})
                    continue
                fallback_spec = _load_startup_fallback_spec(
                    symbol,
                    error_text=error_text,
                )
                if fallback_spec is not None:
                    loaded_fallback_count += 1
                    continue
                failed.append({"symbol": symbol, "error": error_text})
                continue

            loaded_live_count += 1
            changes = self._symbol_spec_change_payload(previous, spec)
            if changes:
                self._bot.store.record_event(
                    "WARN",
                    symbol,
                    "Startup symbol specification changed",
                    {
                        "changes": changes,
                        "source": "startup_symbol_spec_preload",
                    },
                )

        loaded_count = loaded_live_count + loaded_cached_count + loaded_fallback_count
        payload: dict[str, object] = {
            "symbols_total": len(symbols),
            "loaded_count": loaded_count,
            "loaded_live_count": loaded_live_count,
            "loaded_cached_count": loaded_cached_count,
            "loaded_fallback_count": loaded_fallback_count,
            "failed_count": len(failed),
            "symbol_spec_refresh_age_sec": self._db_first_symbol_spec_refresh_age_sec,
            "allowance_short_circuit": allowance_short_circuit,
            "mode": self._bot.config.mode.value,
        }
        if failed:
            payload["failed"] = failed[:10]
        level = "INFO" if not failed else "WARN"
        self._bot.store.record_event(level, None, "Startup symbol specification preload complete", payload)
        if failed:
            strict_mode = self._symbol_spec_preload_is_strict()
            logger.warning(
                "Startup symbol specification preload finished with failures | total=%d loaded=%d failed=%d strict=%s",
                len(symbols),
                loaded_count,
                len(failed),
                strict_mode,
            )
            if strict_mode:
                failed_symbols = [item.get("symbol", "") for item in failed if str(item.get("symbol") or "").strip()]
                raise RuntimeError(
                    "Startup symbol specification preload failed for symbols: "
                    + ",".join(failed_symbols[:10])
                )
        else:
            logger.info(
                "Startup symbol specification preload complete | total=%d loaded=%d",
                len(symbols),
                loaded_count,
            )
