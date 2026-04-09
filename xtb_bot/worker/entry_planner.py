from __future__ import annotations

from xtb_bot.tolerances import FLOAT_COMPARISON_TOLERANCE, FLOAT_ROUNDING_TOLERANCE

import logging
import math
from typing import Any

from xtb_bot.models import AccountSnapshot, Position, Side, Signal, SymbolSpec


logger = logging.getLogger("xtb_bot.worker")


class WorkerEntryPlanner:
    def __init__(self, worker: Any) -> None:
        self._worker = worker

    def build_sl_tp(
        self,
        side: Side,
        entry: float,
        stop_pips: float,
        take_pips: float,
    ) -> tuple[float, float]:
        worker = self._worker
        return self.build_sl_tp_for_spec(
            worker.symbol_spec,
            side,
            entry,
            stop_pips,
            take_pips,
        )

    def build_sl_tp_for_spec(
        self,
        symbol_spec: SymbolSpec | None,
        side: Side,
        entry: float,
        stop_pips: float,
        take_pips: float,
    ) -> tuple[float, float]:
        worker = self._worker
        pip_size = worker._execution_pip_size_for_spec(symbol_spec)
        effective_stop_pips = max(stop_pips, worker.min_stop_loss_pips)
        effective_take_pips = max(take_pips, effective_stop_pips * worker.min_tp_sl_ratio)
        stop_dist = effective_stop_pips * pip_size
        take_dist = effective_take_pips * pip_size

        if side == Side.BUY:
            return entry - stop_dist, entry + take_dist
        return entry + stop_dist, entry - take_dist

    def is_ambiguous_ig_tick_value_spec(self, spec: SymbolSpec | None) -> bool:
        worker = self._worker
        if spec is None or not isinstance(spec.metadata, dict):
            return False
        metadata = spec.metadata
        if str(metadata.get("broker") or "").strip().lower() != "ig":
            return False
        tick_value_source = str(metadata.get("tick_value_source") or "").strip()
        if tick_value_source not in {"valueOfOnePip", "valuePerPip"}:
            return False
        if str(metadata.get("tick_value_kind") or "").strip().lower() != "pip":
            return False
        one_pip_means = worker._safe_float(metadata.get("one_pip_means")) or 0.0
        if one_pip_means > 0.0:
            return False
        value_of_one_point = worker._safe_float(metadata.get("value_of_one_point")) or 0.0
        value_per_point = worker._safe_float(metadata.get("value_per_point")) or 0.0
        if value_of_one_point > 0.0 or value_per_point > 0.0:
            return False
        return True

    def apply_broker_tick_value_calibration(self, spec: SymbolSpec | None) -> SymbolSpec | None:
        worker = self._worker
        if spec is None or not self.is_ambiguous_ig_tick_value_spec(spec):
            return spec
        calibration = worker.store.load_broker_tick_value_calibration(spec.symbol)
        if calibration is None:
            calibration = worker.store.infer_broker_tick_value_calibration_from_recent_closes(
                symbol=spec.symbol,
                tick_size=spec.tick_size,
            )
        if not isinstance(calibration, dict):
            return spec
        calibrated_tick_value = worker._safe_float(calibration.get("tick_value"))
        raw_tick_value = worker._safe_float(spec.tick_value)
        if calibrated_tick_value is None or calibrated_tick_value <= 0.0:
            return spec
        if raw_tick_value is None or raw_tick_value <= 0.0:
            return spec
        ratio = max(raw_tick_value, calibrated_tick_value) / max(
            min(raw_tick_value, calibrated_tick_value),
            FLOAT_COMPARISON_TOLERANCE,
        )
        if not math.isfinite(ratio) or ratio < 5.0:
            return spec
        metadata = dict(spec.metadata) if isinstance(spec.metadata, dict) else {}
        original_source = str(metadata.get("tick_value_source") or "").strip() or "unknown"
        metadata["raw_tick_value_broker"] = raw_tick_value
        metadata["tick_value_source"] = f"{original_source}|broker_close_calibration"
        metadata["tick_value_calibration_source"] = str(calibration.get("source") or "broker_close_calibration")
        metadata["tick_value_calibration_samples"] = int(calibration.get("samples") or 1)
        metadata["tick_value_calibration_ratio"] = float(ratio)
        metadata["tick_value_calibrated"] = float(calibrated_tick_value)
        return SymbolSpec(
            symbol=spec.symbol,
            tick_size=spec.tick_size,
            tick_value=float(calibrated_tick_value),
            contract_size=spec.contract_size,
            lot_min=spec.lot_min,
            lot_max=spec.lot_max,
            lot_step=spec.lot_step,
            min_stop_distance_price=spec.min_stop_distance_price,
            one_pip_means=spec.one_pip_means,
            price_precision=spec.price_precision,
            lot_precision=spec.lot_precision,
            metadata=metadata,
        )

    def maybe_record_broker_tick_value_calibration(
        self,
        *,
        position: Position,
        final_close_price: float | None,
        broker_pnl_account: float | None,
        broker_sync: dict[str, object] | None = None,
    ) -> None:
        worker = self._worker
        if worker.symbol_spec is None or not self.is_ambiguous_ig_tick_value_spec(worker.symbol_spec):
            return
        if final_close_price is None or broker_pnl_account is None:
            return
        volume = float(position.volume)
        if volume <= 0.0:
            return
        price_delta = abs(float(final_close_price) - float(position.open_price))
        if price_delta <= 0.0:
            return
        tick_size = max(float(worker.symbol_spec.tick_size), FLOAT_COMPARISON_TOLERANCE)
        ticks = price_delta / tick_size
        if not math.isfinite(ticks) or ticks <= 0.0:
            return
        observed_tick_value = abs(float(broker_pnl_account)) / (ticks * volume)
        if not math.isfinite(observed_tick_value) or observed_tick_value <= 0.0:
            return
        source = str((broker_sync or {}).get("source") or "broker_close_sync")
        worker.store.save_broker_tick_value_calibration(
            symbol=position.symbol,
            tick_size=tick_size,
            tick_value=observed_tick_value,
            source=f"position_close:{source}",
            samples=1,
        )

    @staticmethod
    def raw_broker_contract_value(spec: SymbolSpec | None) -> float | None:
        if spec is None or not isinstance(spec.metadata, dict):
            return None
        metadata = spec.metadata
        for key in ("value_of_one_point", "value_per_point", "value_of_one_pip", "value_per_pip"):
            try:
                value = float(metadata.get(key) or 0.0)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0.0:
                return value
        return None

    def entry_variant_affordability_key(self, spec: SymbolSpec | None) -> tuple[float, float, float, int, str]:
        worker = self._worker
        raw_contract_value = self.raw_broker_contract_value(spec)
        lot_min = max(float(spec.lot_min), 0.0) if spec is not None else 0.0
        min_contract_cost = (
            raw_contract_value * lot_min
            if raw_contract_value is not None and raw_contract_value > 0.0 and lot_min > 0.0
            else float("inf")
        )
        variant_rank = {
            "cash": 0,
            "daily": 1,
            "ifs": 2,
            "ifd": 3,
            "cfd": 4,
        }.get(worker._spec_epic_variant(spec), 5)
        return (
            min_contract_cost,
            raw_contract_value if raw_contract_value is not None else float("inf"),
            lot_min if lot_min > 0.0 else float("inf"),
            variant_rank,
            worker._spec_epic(spec),
        )

    @staticmethod
    def is_affordability_risk_reject_reason(reason: str | None) -> bool:
        lowered = str(reason or "").strip().lower()
        return bool(lowered) and (
            "below instrument minimum" in lowered
            or "below_instrument_min_lot" in lowered
        )

    def maybe_retry_with_more_affordable_entry_variant(
        self,
        *,
        snapshot: AccountSnapshot,
        signal: Signal,
        entry: float,
        bid: float,
        ask: float,
        current_spread_pips: float,
        decision: object,
    ) -> tuple[object, float, float, dict[str, float] | None]:
        worker = self._worker
        stop_loss, take_profit = self.build_sl_tp(
            signal.side,
            entry,
            signal.stop_loss_pips,
            signal.take_profit_pips,
        )
        stop_loss, take_profit, open_level_guard_payload = worker._apply_broker_open_level_guard(
            signal.side,
            stop_loss,
            take_profit,
            bid,
            ask,
        )
        if not worker._is_non_fx_cfd_symbol():
            return decision, stop_loss, take_profit, open_level_guard_payload
        if not self.is_affordability_risk_reject_reason(getattr(decision, "reason", "")):
            return decision, stop_loss, take_profit, open_level_guard_payload

        worker._save_state(last_price=entry, last_error=None, force=True)
        current_epic = worker._epic_text()
        live_current_spec: SymbolSpec | None = None
        try:
            with worker._watchdog_blocking_operation(
                "entry_affordability_refresh_current_spec",
                last_price=entry,
                grace_sec=180.0,
            ):
                if current_epic:
                    live_current_spec = worker._broker_get_symbol_spec_for_epic(
                        worker.symbol,
                        current_epic,
                        force_refresh=True,
                    )
                else:
                    live_current_spec = worker._broker_get_symbol_spec(
                        worker.symbol,
                        force_refresh=True,
                    )
        except Exception:
            logger.debug("Failed to refresh live symbol spec for entry sizing", exc_info=True)
            live_current_spec = None

        if isinstance(live_current_spec, SymbolSpec):
            worker._adopt_symbol_spec_for_entry(
                live_current_spec,
                source="entry_symbol_spec_refresh",
            )
            stop_loss, take_profit = self.build_sl_tp(
                signal.side,
                entry,
                signal.stop_loss_pips,
                signal.take_profit_pips,
            )
            stop_loss, take_profit, open_level_guard_payload = worker._apply_broker_open_level_guard(
                signal.side,
                stop_loss,
                take_profit,
                bid,
                ask,
            )
            decision = worker.risk.can_open_trade(
                snapshot=snapshot,
                symbol=worker.symbol,
                open_positions_count=worker.position_book.count(),
                entry=entry,
                stop_loss=stop_loss,
                symbol_spec=worker.symbol_spec,
                current_spread_pips=current_spread_pips,
            )
            if getattr(decision, "allowed", False):
                return decision, stop_loss, take_profit, open_level_guard_payload
            current_epic = worker._epic_text()

        try:
            with worker._watchdog_blocking_operation(
                "entry_affordability_load_candidates",
                last_price=entry,
                grace_sec=180.0,
            ):
                candidates = worker._broker_get_symbol_spec_candidates_for_entry(
                    worker.symbol,
                    force_refresh=True,
                )
        except Exception:
            logger.debug("Failed to load live entry symbol spec candidates", exc_info=True)
            return decision, stop_loss, take_profit, open_level_guard_payload
        if not candidates:
            return decision, stop_loss, take_profit, open_level_guard_payload

        selected_spec: SymbolSpec | None = None
        selected_decision = decision
        for candidate in sorted(
            (self.apply_broker_tick_value_calibration(candidate) for candidate in candidates),
            key=worker._entry_variant_affordability_key,
        ):
            candidate_epic = worker._spec_epic(candidate)
            if current_epic and candidate_epic == current_epic:
                continue
            candidate_stop_loss, candidate_take_profit = self.build_sl_tp_for_spec(
                candidate,
                signal.side,
                entry,
                signal.stop_loss_pips,
                signal.take_profit_pips,
            )
            candidate_stop_loss, candidate_take_profit, _ = worker._apply_broker_open_level_guard_for_spec(
                candidate,
                signal.side,
                candidate_stop_loss,
                candidate_take_profit,
                bid,
                ask,
            )
            candidate_decision = worker.risk.can_open_trade(
                snapshot=snapshot,
                symbol=worker.symbol,
                open_positions_count=worker.position_book.count(),
                entry=entry,
                stop_loss=candidate_stop_loss,
                symbol_spec=candidate,
                current_spread_pips=current_spread_pips,
            )
            if candidate_decision.allowed:
                selected_spec = candidate
                selected_decision = candidate_decision
                break

        if selected_spec is None:
            return decision, stop_loss, take_profit, open_level_guard_payload

        previous_spec = worker.symbol_spec
        selected_epic = worker._spec_epic(selected_spec)
        if selected_epic:
            try:
                with worker._watchdog_blocking_operation(
                    "entry_affordability_activate_selected_epic",
                    last_price=entry,
                    grace_sec=180.0,
                ):
                    refreshed_selected_spec = worker._broker_get_symbol_spec_for_epic(
                        worker.symbol,
                        selected_epic,
                        force_refresh=True,
                    )
                    if isinstance(refreshed_selected_spec, SymbolSpec):
                        selected_spec = self.apply_broker_tick_value_calibration(
                            refreshed_selected_spec
                        )
            except Exception:
                logger.debug("Failed to activate selected entry epic %s", selected_epic, exc_info=True)

        worker._adopt_symbol_spec_for_entry(selected_spec, source="entry_variant_switch")

        stop_loss, take_profit = self.build_sl_tp(
            signal.side,
            entry,
            signal.stop_loss_pips,
            signal.take_profit_pips,
        )
        stop_loss, take_profit, open_level_guard_payload = worker._apply_broker_open_level_guard(
            signal.side,
            stop_loss,
            take_profit,
            bid,
            ask,
        )
        selected_decision = worker.risk.can_open_trade(
            snapshot=snapshot,
            symbol=worker.symbol,
            open_positions_count=worker.position_book.count(),
            entry=entry,
            stop_loss=stop_loss,
            symbol_spec=worker.symbol_spec,
            current_spread_pips=current_spread_pips,
        )
        worker.store.record_event(
            "INFO",
            worker.symbol,
            "Switched to more affordable broker variant",
            {
                "previous_epic": worker._spec_epic(previous_spec) or None,
                "previous_epic_variant": worker._spec_epic_variant(previous_spec) or None,
                "previous_contract_value": self.raw_broker_contract_value(previous_spec),
                "previous_lot_min": float(previous_spec.lot_min) if previous_spec is not None else None,
                "selected_epic": worker._spec_epic(worker.symbol_spec) or None,
                "selected_epic_variant": worker._spec_epic_variant(worker.symbol_spec) or None,
                "selected_contract_value": self.raw_broker_contract_value(worker.symbol_spec),
                "selected_lot_min": float(worker.symbol_spec.lot_min),
                "initial_reason": getattr(decision, "reason", None),
                "selected_reason": getattr(selected_decision, "reason", None),
                "signal": signal.side.value,
            },
        )
        return selected_decision, stop_loss, take_profit, open_level_guard_payload
