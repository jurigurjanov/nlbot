from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import math
import threading

from xtb_bot.config import RiskConfig
from xtb_bot.models import AccountSnapshot, SymbolSpec
from xtb_bot.state_store import StateStore


logger = logging.getLogger(__name__)

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


@dataclass(slots=True)
class RiskStats:
    daily_pnl: float
    daily_drawdown_pct: float
    total_drawdown_pct: float


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    suggested_volume: float


@dataclass(slots=True)
class SlotDecision:
    acquired: bool
    reason: str
    reservation_id: str | None = None


class RiskManager:
    def __init__(self, cfg: RiskConfig, store: StateStore):
        self.cfg = cfg
        self.store = store
        self._open_slot_lease_sec = max(1.0, float(self.cfg.open_slot_lease_sec))
        self._max_positions_slot_alert_key = "risk.max_positions_slot_alert_active"
        self._lock = threading.Lock()

    @staticmethod
    def _is_fx_pair_symbol(symbol: str) -> bool:
        upper = str(symbol or "").strip().upper()
        if len(upper) != 6 or not upper.isalpha():
            return False
        base = upper[:3]
        quote = upper[3:]
        if base == quote:
            return False
        return base in _FX_MARGIN_CURRENCIES and quote in _FX_MARGIN_CURRENCIES

    @staticmethod
    def _is_plausible_fx_entry(symbol: str, entry: float) -> bool:
        value = abs(float(entry))
        if not math.isfinite(value) or value <= 0:
            return False
        upper = str(symbol or "").strip().upper()
        if upper.endswith("JPY"):
            return 20.0 <= value <= 500.0
        return 0.2 <= value <= 50.0

    @staticmethod
    def _read_positive_float(value: object, default: float = 0.0) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return float(default)
        if not math.isfinite(parsed) or parsed <= 0:
            return float(default)
        return float(parsed)

    @staticmethod
    def _infer_fx_entry_scale_divisor(symbol: str, entry: float) -> float:
        upper = str(symbol or "").strip().upper()
        value = abs(float(entry))
        if value <= 0:
            return 1.0

        if upper.endswith("JPY"):
            min_plausible = 20.0
            max_plausible = 500.0
            target = 150.0
            candidates = (10.0, 100.0, 1_000.0, 10_000.0, 100_000.0)
        else:
            min_plausible = 0.2
            max_plausible = 50.0
            target = 1.2
            candidates = (10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0)

        best_divisor = 1.0
        best_error = float("inf")
        for candidate in candidates:
            scaled = value / candidate
            if scaled < min_plausible or scaled > max_plausible:
                continue
            error = abs(math.log(max(scaled, 1e-12) / target))
            if error < best_error:
                best_error = error
                best_divisor = candidate
        return best_divisor

    def _normalize_entry_for_margin(self, entry: float, symbol_spec: SymbolSpec) -> tuple[float, str]:
        normalized_entry = abs(float(entry))
        if normalized_entry <= 0:
            return 0.0, ""

        metadata = symbol_spec.metadata if isinstance(symbol_spec.metadata, dict) else {}
        raw_divisor = metadata.get("margin_price_scale_divisor")
        divisor = self._read_positive_float(raw_divisor, 1.0)
        if math.isfinite(divisor) and divisor > 1.0:
            scaled_entry = normalized_entry / divisor
            return scaled_entry, f"entry_scale:metadata:{divisor:g}"

        symbol = str(symbol_spec.symbol or "").strip().upper()
        if not self._is_fx_pair_symbol(symbol):
            return normalized_entry, ""

        raw_scaling_factor = metadata.get("scaling_factor", metadata.get("scalingFactor"))
        scaling_factor = self._read_positive_float(raw_scaling_factor, 1.0)
        if scaling_factor >= 100.0 and normalized_entry >= 100.0:
            scaled_entry = normalized_entry / scaling_factor
            if self._is_plausible_fx_entry(symbol, scaled_entry):
                return scaled_entry, f"entry_scale:metadata_scaling:{scaling_factor:g}"

        one_pip_means = self._read_positive_float(metadata.get("one_pip_means"), 0.0)
        expected_pip = one_pip_means if one_pip_means > 0 else (0.01 if symbol.endswith("JPY") else 0.0001)
        tick_size = max(float(symbol_spec.tick_size), 1e-9)
        inferred_divisor = tick_size / expected_pip
        if math.isfinite(inferred_divisor) and inferred_divisor >= 100.0:
            scaled_entry = normalized_entry / inferred_divisor
            if self._is_plausible_fx_entry(symbol, scaled_entry):
                return scaled_entry, f"entry_scale:heuristic_fx_tick_ratio:{inferred_divisor:g}"

        if normalized_entry >= (1000.0 if symbol.endswith("JPY") else 100.0):
            magnitude_divisor = self._infer_fx_entry_scale_divisor(symbol, normalized_entry)
            if magnitude_divisor > 1.0:
                scaled_entry = normalized_entry / magnitude_divisor
                if self._is_plausible_fx_entry(symbol, scaled_entry):
                    return (
                        scaled_entry,
                        f"entry_scale:heuristic_fx_magnitude:{magnitude_divisor:g}",
                    )

        return normalized_entry, ""

    def _resolve_margin_conversion_rate(self, symbol_spec: SymbolSpec) -> tuple[float, str]:
        metadata = symbol_spec.metadata if isinstance(symbol_spec.metadata, dict) else {}
        for key in (
            "account_currency_conversion",
            "account_currency_conversion_rate",
            "margin_currency_conversion_rate",
            "currency_conversion_rate",
        ):
            rate = self._read_positive_float(metadata.get(key), 0.0)
            if rate > 0:
                return rate, key
        return 1.0, ""

    def _estimate_required_margin(
        self,
        entry: float,
        volume: float,
        symbol_spec: SymbolSpec,
    ) -> tuple[float, str, dict[str, float]]:
        margin_entry, entry_scale_source = self._normalize_entry_for_margin(entry, symbol_spec)
        conversion_rate, conversion_source = self._resolve_margin_conversion_rate(symbol_spec)
        notional_native = margin_entry * max(symbol_spec.contract_size, 1.0) * max(volume, 0.0)
        notional = notional_native * conversion_rate
        diagnostics: dict[str, float] = {
            "margin_entry": margin_entry,
            "volume": max(volume, 0.0),
            "notional_native": max(notional_native, 0.0),
            "conversion_rate": conversion_rate,
            "notional": max(notional, 0.0),
            "effective_leverage": 0.0,
            "margin_ratio": 0.0,
        }
        if notional <= 0:
            return 0.0, "notional_unavailable", diagnostics

        metadata = symbol_spec.metadata if isinstance(symbol_spec.metadata, dict) else {}
        margin_factor_raw = metadata.get("margin_factor", metadata.get("marginFactor"))
        margin_factor_unit = str(
            metadata.get("margin_factor_unit", metadata.get("marginFactorUnit", ""))
        ).upper()
        margin_factor = self._read_positive_float(margin_factor_raw, 0.0)

        if margin_factor > 0:
            if "PERCENT" in margin_factor_unit or "%" in margin_factor_unit:
                ratio = margin_factor / 100.0
            else:
                ratio = margin_factor if margin_factor <= 1.0 else (margin_factor / 100.0)
            ratio = max(ratio, 1e-9)
            diagnostics["margin_ratio"] = ratio
            diagnostics["effective_leverage"] = 1.0 / ratio
            source = f"margin_factor:{margin_factor}:{margin_factor_unit or 'unknown'}"
            if entry_scale_source:
                source = f"{source}|{entry_scale_source}"
            if conversion_source:
                source = f"{source}|conversion:{conversion_source}:{conversion_rate:g}"
            return notional * ratio, source, diagnostics

        leverage_raw = metadata.get("leverage")
        fallback_leverage = float(self.cfg.margin_fallback_leverage)
        if not math.isfinite(fallback_leverage) or fallback_leverage <= 0:
            fallback_leverage = 20.0

        leverage = self._read_positive_float(leverage_raw, fallback_leverage)

        if not math.isfinite(leverage) or leverage <= 0:
            leverage = fallback_leverage
            source = f"fallback_leverage:{leverage}"
            if entry_scale_source:
                source = f"{source}|{entry_scale_source}"
            if conversion_source:
                source = f"{source}|conversion:{conversion_source}:{conversion_rate:g}"
            diagnostics["effective_leverage"] = leverage
            return notional / leverage, source, diagnostics

        source = f"symbol_leverage:{leverage}"
        if entry_scale_source:
            source = f"{source}|{entry_scale_source}"
        if conversion_source:
            source = f"{source}|conversion:{conversion_source}:{conversion_rate:g}"
        diagnostics["effective_leverage"] = leverage
        return notional / leverage, source, diagnostics

    def _is_weekend_margin_window(self) -> bool:
        now_utc = datetime.now(timezone.utc)
        weekday = now_utc.weekday()
        weekend_start_hour = int(self.cfg.margin_weekend_start_hour_utc)
        if weekday in {5, 6}:
            return True
        if weekday == 4 and now_utc.hour >= weekend_start_hour:
            return True
        return False

    def _is_holiday_margin_window(self) -> bool:
        holiday_dates = tuple(self.cfg.margin_holiday_dates_utc)
        if not holiday_dates:
            return False
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return today in set(holiday_dates)

    def _estimate_volume(
        self,
        capital_base: float,
        entry: float,
        stop_loss: float,
        symbol_spec: SymbolSpec | None,
        current_spread_pips: float | None = None,
    ) -> tuple[float, dict[str, float | str]]:
        risk_amount = capital_base * (self.cfg.max_risk_per_trade_pct / 100.0)
        diagnostics: dict[str, float | str] = {
            "capital_base": capital_base,
            "risk_amount": risk_amount,
            "entry": entry,
            "stop_loss": stop_loss,
            "spread_pips": float(current_spread_pips or 0.0),
        }
        if risk_amount <= 0:
            diagnostics["reason"] = "non_positive_risk_amount"
            return 0.0, diagnostics
        if symbol_spec is None:
            diagnostics["reason"] = "symbol_spec_unavailable"
            return 0.0, diagnostics

        tick_size = max(symbol_spec.tick_size, 1e-9)
        tick_value = max(symbol_spec.tick_value, 1e-9)
        sl_distance = abs(entry - stop_loss)
        min_distance = max(self.cfg.min_stop_loss_pips, 1.0) * tick_size
        spread_distance = max(float(current_spread_pips or 0.0), 0.0) * tick_size
        effective_distance = max(sl_distance + spread_distance, min_distance)
        ticks = effective_distance / tick_size
        risk_per_lot = ticks * tick_value

        diagnostics.update(
            {
                "tick_size": tick_size,
                "tick_value": tick_value,
                "sl_distance": sl_distance,
                "min_distance": min_distance,
                "spread_distance": spread_distance,
                "effective_distance": effective_distance,
                "risk_per_lot": risk_per_lot,
            }
        )

        if risk_per_lot <= 0:
            diagnostics["reason"] = "non_positive_risk_per_lot"
            return 0.0, diagnostics

        raw_volume = risk_amount / risk_per_lot
        diagnostics["raw_volume"] = raw_volume
        diagnostics["lot_min"] = symbol_spec.lot_min
        diagnostics["lot_max"] = symbol_spec.lot_max
        diagnostics["lot_step"] = symbol_spec.lot_step

        if raw_volume < symbol_spec.lot_min:
            diagnostics["reason"] = "below_instrument_min_lot"
            return 0.0, diagnostics

        rounded = symbol_spec.round_volume(raw_volume)
        diagnostics["rounded_volume"] = rounded
        if rounded <= 0:
            diagnostics["reason"] = "rounded_to_zero"
            return 0.0, diagnostics

        diagnostics["reason"] = "ok"
        return rounded, diagnostics

    def _current_day(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _ensure_anchor(
        self,
        snapshot: AccountSnapshot,
        open_positions_count: int | None = None,
    ) -> dict[str, float | str | bool]:
        day = self._current_day()
        allow_external_cashflow_rebase = bool(
            self.cfg.external_cashflow_rebase_enabled and open_positions_count == 0
        )
        anchor_state = self.store.sync_risk_anchors(
            day=day,
            snapshot_equity=snapshot.equity,
            snapshot_balance=snapshot.balance,
            start_balance=self.cfg.start_balance,
            allow_external_cashflow_rebase=allow_external_cashflow_rebase,
            external_cashflow_rebase_min_abs=self.cfg.external_cashflow_rebase_min_abs,
            external_cashflow_rebase_min_pct=self.cfg.external_cashflow_rebase_min_pct,
        )
        if anchor_state.get("released_daily_lock"):
            prev_locked_day = str(anchor_state.get("previous_locked_day") or "")
            prev_unlock_at = str(anchor_state.get("previous_unlock_at") or "")
            self.store.record_event(
                "INFO",
                None,
                "Daily drawdown lock released",
                {
                    "previous_locked_day": prev_locked_day,
                    "previous_unlock_at": prev_unlock_at,
                    "released_day": day,
                },
            )
            logger.info(
                "Daily drawdown lock released for new day=%s (prev_day=%s, unlock_at=%s)",
                day,
                prev_locked_day,
                prev_unlock_at,
            )
        external_cashflow_rebased = float(anchor_state.get("external_cashflow_rebased") or 0.0)
        if external_cashflow_rebased != 0.0:
            payload = {
                "day": day,
                "external_cashflow_rebased": external_cashflow_rebased,
                "day_start_equity": float(anchor_state.get("day_start_equity") or 0.0),
                "day_hwm_equity": float(anchor_state.get("day_hwm_equity") or 0.0),
                "day_start_balance": float(anchor_state.get("day_start_balance") or 0.0),
                "min_abs": self.cfg.external_cashflow_rebase_min_abs,
                "min_pct": self.cfg.external_cashflow_rebase_min_pct,
            }
            self.store.record_event(
                "INFO",
                None,
                "Risk anchors rebased for external cashflow",
                payload,
            )
            logger.warning(
                "Risk anchors rebased for external cashflow: day=%s delta=%.2f",
                day,
                external_cashflow_rebased,
            )
        return anchor_state

    def compute_stats(
        self,
        snapshot: AccountSnapshot,
        open_positions_count: int | None = None,
    ) -> RiskStats:
        day = self._current_day()
        anchor_state = self._ensure_anchor(snapshot, open_positions_count=open_positions_count)

        start_equity = float(anchor_state.get("start_equity") or max(snapshot.equity, self.cfg.start_balance))
        day_start_equity = float(anchor_state.get("day_start_equity") or snapshot.equity)
        day_hwm_equity = float(
            anchor_state.get("day_hwm_equity") or max(day_start_equity, snapshot.equity)
        )

        daily_pnl = snapshot.equity - day_start_equity
        daily_drawdown = max(0.0, day_hwm_equity - snapshot.equity)
        total_drawdown = max(0.0, start_equity - snapshot.equity)

        daily_drawdown_pct = (daily_drawdown / day_hwm_equity * 100.0) if day_hwm_equity > 0 else 0.0
        total_drawdown_pct = (total_drawdown / start_equity * 100.0) if start_equity > 0 else 0.0

        return RiskStats(
            daily_pnl=daily_pnl,
            daily_drawdown_pct=daily_drawdown_pct,
            total_drawdown_pct=total_drawdown_pct,
        )

    def _is_daily_locked(self, day: str) -> bool:
        return self.store.get_kv("risk.daily_locked_day") == day

    def _next_day_unlock_at(self, day: str) -> str:
        try:
            day_dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            day_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        unlock_dt = day_dt + timedelta(days=1)
        return unlock_dt.strftime("%Y-%m-%d 00:00:00")

    def _lock_daily_drawdown(self, day: str, stats: RiskStats, snapshot: AccountSnapshot) -> None:
        unlock_at = self._next_day_unlock_at(day)
        reason = (
            f"Daily drawdown {stats.daily_drawdown_pct:.2f}% "
            f">= limit {self.cfg.max_daily_drawdown_pct:.2f}%"
        )
        self.store.activate_daily_drawdown_lock(day=day, reason=reason, unlock_at=unlock_at)

        self.store.record_event(
            "WARN",
            None,
            "Daily drawdown lock activated",
            {
                "reason": reason,
                "lock_day": day,
                "unlock_at": unlock_at,
                "equity": snapshot.equity,
                "balance": snapshot.balance,
                "daily_drawdown_pct": stats.daily_drawdown_pct,
                "daily_drawdown_limit_pct": self.cfg.max_daily_drawdown_pct,
            },
        )
        logger.warning("Daily lock activated: %s | unlock_at=%s", reason, unlock_at)

    def suggest_volume(
        self,
        symbol: str,
        capital_base: float,
        entry: float,
        stop_loss: float,
        symbol_spec: SymbolSpec | None = None,
        current_spread_pips: float | None = None,
    ) -> float:
        _ = symbol
        volume, _ = self._estimate_volume(
            capital_base=capital_base,
            entry=entry,
            stop_loss=stop_loss,
            symbol_spec=symbol_spec,
            current_spread_pips=current_spread_pips,
        )
        return volume

    def can_open_trade(
        self,
        snapshot: AccountSnapshot,
        symbol: str,
        open_positions_count: int,
        entry: float,
        stop_loss: float,
        symbol_spec: SymbolSpec | None = None,
        current_spread_pips: float | None = None,
    ) -> RiskDecision:
        with self._lock:
            stats = self.compute_stats(snapshot, open_positions_count=open_positions_count)
            day = self._current_day()

            if self._is_daily_locked(day):
                unlock_at = self.store.get_kv("risk.daily_lock_until") or self._next_day_unlock_at(day)
                reason = self.store.get_kv("risk.daily_lock_reason") or "Daily drawdown lock active"
                return RiskDecision(
                    allowed=False,
                    reason=f"Daily drawdown lock active until {unlock_at} ({reason})",
                    suggested_volume=0.0,
                )

            if stats.total_drawdown_pct >= self.cfg.max_total_drawdown_pct:
                return RiskDecision(
                    allowed=False,
                    reason="Total drawdown limit reached",
                    suggested_volume=0.0,
                )

            if stats.daily_drawdown_pct >= self.cfg.max_daily_drawdown_pct:
                self._lock_daily_drawdown(day, stats, snapshot)
                unlock_at = self.store.get_kv("risk.daily_lock_until") or self._next_day_unlock_at(day)
                return RiskDecision(
                    allowed=False,
                    reason=f"Daily drawdown limit reached; trading locked until {unlock_at}",
                    suggested_volume=0.0,
                )

            if snapshot.margin_free <= 0:
                return RiskDecision(
                    allowed=False,
                    reason="No free margin",
                    suggested_volume=0.0,
                )

            if open_positions_count >= self.cfg.max_open_positions:
                if self.store.get_kv("risk.max_positions_alert_active") != "1":
                    self.store.set_kv("risk.max_positions_alert_active", "1")
                    self.store.record_event(
                        "WARN",
                        symbol,
                        "Trade blocked: max open positions limit reached",
                        {
                            "symbol": symbol,
                            "current_open_positions": open_positions_count,
                            "max_open_positions": self.cfg.max_open_positions,
                        },
                    )
                    logger.warning(
                        "Trade blocked by max open positions: current=%s limit=%s symbol=%s",
                        open_positions_count,
                        self.cfg.max_open_positions,
                        symbol,
                    )
                return RiskDecision(
                    allowed=False,
                    reason="Max open positions reached",
                    suggested_volume=0.0,
                )
            if self.store.get_kv("risk.max_positions_alert_active") == "1":
                self.store.set_kv("risk.max_positions_alert_active", "0")
            pending_slots = self.pending_open_slots()
            effective_open_positions = open_positions_count + pending_slots
            if pending_slots > 0 and effective_open_positions >= self.cfg.max_open_positions:
                if self.store.get_kv(self._max_positions_slot_alert_key) != "1":
                    self.store.set_kv(self._max_positions_slot_alert_key, "1")
                    self.store.record_event(
                        "WARN",
                        symbol,
                        "Trade blocked: max open positions limit reached",
                        {
                            "symbol": symbol,
                            "current_open_positions": open_positions_count,
                            "pending_open_slots": pending_slots,
                            "effective_open_positions": effective_open_positions,
                            "max_open_positions": self.cfg.max_open_positions,
                        },
                    )
                    logger.warning(
                        "Trade blocked by max open positions (with pending): current=%s pending=%s effective=%s limit=%s symbol=%s",
                        open_positions_count,
                        pending_slots,
                        effective_open_positions,
                        self.cfg.max_open_positions,
                        symbol,
                    )
                return RiskDecision(
                    allowed=False,
                    reason="Max open positions reached",
                    suggested_volume=0.0,
                )
            if self.store.get_kv(self._max_positions_slot_alert_key) == "1":
                self.store.set_kv(self._max_positions_slot_alert_key, "0")

            if symbol_spec is None:
                return RiskDecision(
                    allowed=False,
                    reason="Symbol specification unavailable",
                    suggested_volume=0.0,
                )

            # Use realized balance as a conservative base for per-trade risk sizing.
            capital_base = max(min(snapshot.balance, snapshot.equity), 0.0)
            volume, diagnostics = self._estimate_volume(
                capital_base=capital_base,
                entry=entry,
                stop_loss=stop_loss,
                symbol_spec=symbol_spec,
                current_spread_pips=current_spread_pips,
            )
            if volume <= 0:
                reason = (
                    "Calculated volume is zero or below instrument minimum "
                    f"(raw={float(diagnostics.get('raw_volume', 0.0)):.6f}, "
                    f"lot_min={float(diagnostics.get('lot_min', 0.0)):.6f}, "
                    f"risk_per_lot={float(diagnostics.get('risk_per_lot', 0.0)):.6f}, "
                    f"spread_pips={float(diagnostics.get('spread_pips', 0.0)):.4f}, "
                    f"reason={diagnostics.get('reason')})"
                )
                return RiskDecision(
                    allowed=False,
                    reason=reason,
                    suggested_volume=0.0,
                )

            if self.cfg.margin_check_enabled:
                required_margin, margin_source, margin_diagnostics = self._estimate_required_margin(
                    entry, volume, symbol_spec
                )
                adjusted_required_margin = max(required_margin, 0.0)
                margin_overlays: list[str] = []

                overhead_pct = max(float(self.cfg.margin_overhead_pct), 0.0)
                if overhead_pct > 0:
                    adjusted_required_margin *= 1.0 + (overhead_pct / 100.0)
                    margin_overlays.append(f"overhead_pct:{overhead_pct:.2f}")

                weekend_multiplier = max(float(self.cfg.margin_weekend_multiplier), 1.0)
                if weekend_multiplier > 1.0 and self._is_weekend_margin_window():
                    adjusted_required_margin *= weekend_multiplier
                    margin_overlays.append(f"weekend_multiplier:{weekend_multiplier:.2f}")
                holiday_multiplier = max(float(self.cfg.margin_holiday_multiplier), 1.0)
                if holiday_multiplier > 1.0 and self._is_holiday_margin_window():
                    adjusted_required_margin *= holiday_multiplier
                    margin_overlays.append(f"holiday_multiplier:{holiday_multiplier:.2f}")

                commission_per_lot = max(float(self.cfg.margin_commission_per_lot), 0.0)
                if commission_per_lot > 0:
                    commission_estimate = commission_per_lot * max(volume, 0.0)
                    adjusted_required_margin += commission_estimate
                    margin_overlays.append(f"commission_estimate:{commission_estimate:.2f}")

                if margin_overlays:
                    margin_source = f"{margin_source}|{'|'.join(margin_overlays)}"

                required_with_buffer = adjusted_required_margin * max(self.cfg.margin_safety_buffer, 1.0)
                if required_with_buffer > snapshot.margin_free:
                    effective_leverage = float(margin_diagnostics.get("effective_leverage", 0.0))
                    leverage_text = f"{effective_leverage:.2f}" if effective_leverage > 0 else "n/a"
                    return RiskDecision(
                        allowed=False,
                        reason=(
                            "Insufficient free margin for suggested volume "
                            f"(required={required_with_buffer:.2f}, base_required={required_margin:.2f}, "
                            f"adjusted_required={adjusted_required_margin:.2f}, free={snapshot.margin_free:.2f}, "
                            f"buffer={max(self.cfg.margin_safety_buffer, 1.0):.2f}, "
                            f"notional={float(margin_diagnostics.get('notional', 0.0)):.2f}, "
                            f"effective_leverage={leverage_text}, source={margin_source}, volume={volume:.4f})"
                        ),
                        suggested_volume=0.0,
                    )
                min_free_after_open = max(
                    float(self.cfg.margin_min_free_after_open),
                    snapshot.equity * (max(float(self.cfg.margin_min_free_after_open_pct), 0.0) / 100.0),
                )
                free_after_open = snapshot.margin_free - required_with_buffer
                if free_after_open < min_free_after_open:
                    return RiskDecision(
                        allowed=False,
                        reason=(
                            "Free margin reserve would be breached after opening trade "
                            f"(free_after_open={free_after_open:.2f}, reserve={min_free_after_open:.2f}, "
                            f"required={required_with_buffer:.2f}, source={margin_source}, volume={volume:.4f})"
                        ),
                        suggested_volume=0.0,
                    )

            return RiskDecision(
                allowed=True,
                reason="OK",
                suggested_volume=volume,
            )

    def try_acquire_open_slot(self, symbol: str, open_positions_count: int) -> SlotDecision:
        reservation_id, pending_slots, effective_open_positions = self.store.acquire_open_slot(
            open_positions_count=open_positions_count,
            max_open_positions=self.cfg.max_open_positions,
            lease_sec=self._open_slot_lease_sec,
        )
        if reservation_id is None:
            if self.store.get_kv(self._max_positions_slot_alert_key) != "1":
                self.store.set_kv(self._max_positions_slot_alert_key, "1")
                self.store.record_event(
                    "WARN",
                    symbol,
                    "Trade blocked: max open positions limit reached",
                    {
                        "symbol": symbol,
                        "current_open_positions": open_positions_count,
                        "pending_open_slots": pending_slots,
                        "effective_open_positions": effective_open_positions,
                        "max_open_positions": self.cfg.max_open_positions,
                    },
                )
                logger.warning(
                    "Trade blocked by max open positions (with pending): current=%s pending=%s effective=%s limit=%s symbol=%s",
                    open_positions_count,
                    pending_slots,
                    effective_open_positions,
                    self.cfg.max_open_positions,
                    symbol,
                )
            return SlotDecision(
                acquired=False,
                reason="Max open positions reached",
            )

        if self.store.get_kv(self._max_positions_slot_alert_key) == "1":
            self.store.set_kv(self._max_positions_slot_alert_key, "0")

        return SlotDecision(acquired=True, reason="OK", reservation_id=reservation_id)

    def release_open_slot(self, reservation_id: str | None = None) -> None:
        if reservation_id is None:
            return
        self.store.release_open_slot(reservation_id)

    def pending_open_slots(self) -> int:
        return self.store.count_open_slot_reservations(self._open_slot_lease_sec)

    def should_force_flatten(
        self,
        snapshot: AccountSnapshot,
        open_positions_count: int | None = None,
    ) -> tuple[bool, str]:
        with self._lock:
            if open_positions_count is not None and open_positions_count <= 0:
                return False, ""
            stats = self.compute_stats(snapshot, open_positions_count=open_positions_count)
            day = self._current_day()
            if self._is_daily_locked(day):
                unlock_at = self.store.get_kv("risk.daily_lock_until") or self._next_day_unlock_at(day)
                return True, f"Daily drawdown lock active until {unlock_at}"
            if stats.total_drawdown_pct >= self.cfg.max_total_drawdown_pct:
                return True, "Total drawdown exceeded"
            if stats.daily_drawdown_pct >= self.cfg.max_daily_drawdown_pct:
                return True, "Daily drawdown exceeded"
            return False, ""
