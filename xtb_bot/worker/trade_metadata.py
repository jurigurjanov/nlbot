from __future__ import annotations

from xtb_bot.models import Position, Signal


class WorkerTradeMetadataRuntime:
    def __init__(self, worker: object) -> None:
        self.worker = worker

    def effective_entry_strategy_from_signal(self, signal: Signal) -> str:
        worker = self.worker
        base_strategy = worker._base_strategy_label()
        metadata = worker._signal_metadata(signal)
        if (not worker._multi_strategy_enabled) or metadata.get("indicator") != "multi_strategy_netting":
            return base_strategy
        for key in ("multi_entry_strategy", "strategy"):
            normalized = worker._normalize_strategy_label(metadata.get(key))
            if normalized:
                return normalized
        return base_strategy

    def effective_entry_component_from_signal(self, signal: Signal) -> str:
        worker = self.worker
        base_strategy = worker._base_strategy_label()
        metadata = worker._signal_metadata(signal)
        if (not worker._multi_strategy_enabled) or metadata.get("indicator") != "multi_strategy_netting":
            for key in ("strategy", "indicator"):
                normalized = worker._normalize_strategy_label(metadata.get(key))
                if normalized:
                    return normalized
            return base_strategy
        for key in (
            "multi_entry_strategy_component",
            "multi_dominant_component_strategy",
            "multi_representative_strategy",
        ):
            normalized = worker._normalize_strategy_label(metadata.get(key))
            if normalized:
                return normalized
        component = worker._multi_component_strategy_from_metadata(metadata, side=signal.side)
        if component:
            return component
        return base_strategy

    def effective_entry_signal_from_signal(self, signal: Signal) -> str | None:
        worker = self.worker
        metadata = worker._signal_metadata(signal)
        if (not worker._multi_strategy_enabled) or metadata.get("indicator") != "multi_strategy_netting":
            for key in ("base_trend_signal", "trend_signal"):
                raw = str(metadata.get(key) or "").strip().lower()
                if raw:
                    return raw
            component = self.effective_entry_component_from_signal(signal)
            if component == "index_hybrid":
                regime = str(metadata.get("regime") or "").strip().lower()
                if regime in {"trend_following", "mean_reversion"}:
                    return f"index_hybrid:{regime}"
            return None
        for key in ("multi_entry_strategy_signal", "base_trend_signal", "trend_signal"):
            raw = str(metadata.get(key) or "").strip().lower()
            if raw:
                return raw
        component_name = self.effective_entry_component_from_signal(signal)
        components = metadata.get("multi_components")
        if isinstance(components, dict):
            component_payload = components.get(component_name)
            if isinstance(component_payload, dict):
                raw = str(component_payload.get("trend_signal") or "").strip().lower()
                if raw:
                    return raw
        if component_name == "index_hybrid":
            regime = str(metadata.get("regime") or "").strip().lower()
            if regime in {"trend_following", "mean_reversion"}:
                return f"index_hybrid:{regime}"
        return None

    def position_entry_strategy(self, position: Position) -> str:
        worker = self.worker
        position_id = str(position.position_id or "").strip()
        base_strategy = worker._base_strategy_label()
        direct = worker._normalize_strategy_label(getattr(position, "strategy_entry", None))
        if direct:
            if position_id:
                worker._position_entry_strategy_by_id[position_id] = direct
            return direct
        if not position_id:
            return base_strategy
        cached = worker._position_entry_strategy_by_id.get(position_id)
        if cached:
            return cached
        row = worker.store.get_trade_record(position_id)
        resolved = base_strategy
        if isinstance(row, dict):
            resolved = worker._normalize_strategy_label(
                row.get("strategy_entry"),
                fallback=worker._normalize_strategy_label(row.get("strategy"), fallback=base_strategy),
            )
        worker._position_entry_strategy_by_id[position_id] = resolved
        return resolved

    def position_entry_component(self, position: Position) -> str:
        worker = self.worker
        position_id = str(position.position_id or "").strip()
        base_component = worker._base_strategy_label()
        direct = worker._normalize_strategy_label(getattr(position, "strategy_entry_component", None))
        if direct:
            if position_id:
                worker._position_entry_component_by_id[position_id] = direct
            return direct
        if not position_id:
            return base_component
        cached = worker._position_entry_component_by_id.get(position_id)
        if cached:
            return cached
        row = worker.store.get_trade_record(position_id)
        resolved = base_component
        if isinstance(row, dict):
            resolved = worker._normalize_strategy_label(
                row.get("strategy_entry_component"),
                fallback=worker._normalize_strategy_label(
                    row.get("strategy_entry"),
                    fallback=worker._normalize_strategy_label(row.get("strategy"), fallback=base_component),
                ),
            )
        worker._position_entry_component_by_id[position_id] = resolved
        return resolved

    def position_entry_signal(self, position: Position) -> str | None:
        worker = self.worker
        position_id = str(position.position_id or "").strip()
        direct = str(getattr(position, "strategy_entry_signal", "") or "").strip().lower()
        if direct:
            if position_id:
                worker._position_entry_signal_by_id[position_id] = direct
            return direct
        if not position_id:
            return None
        cached = worker._position_entry_signal_by_id.get(position_id)
        if cached:
            return cached
        row = worker.store.get_trade_record(position_id)
        resolved = ""
        if isinstance(row, dict):
            resolved = str(row.get("strategy_entry_signal") or "").strip().lower()
        if resolved:
            worker._position_entry_signal_by_id[position_id] = resolved
            return resolved
        return None

    def apply_position_strategy_metadata(
        self,
        position: Position,
        *,
        strategy: str | None = None,
        strategy_entry: str | None = None,
        strategy_entry_component: str | None = None,
        strategy_entry_signal: str | None = None,
        strategy_exit: str | None = None,
        strategy_exit_component: str | None = None,
        strategy_exit_signal: str | None = None,
    ) -> None:
        worker = self.worker
        carrier_strategy = worker._normalize_strategy_label(strategy, fallback=worker.strategy_name)
        base_strategy = worker._base_strategy_label()
        normalized_entry = worker._normalize_strategy_label(
            strategy_entry,
            fallback=(
                worker._normalize_strategy_label(getattr(position, "strategy_entry", None))
                or base_strategy
            ),
        )
        normalized_entry_component = worker._normalize_strategy_label(
            strategy_entry_component,
            fallback=(
                worker._normalize_strategy_label(getattr(position, "strategy_entry_component", None))
                or normalized_entry
            ),
        )
        normalized_entry_signal = str(
            strategy_entry_signal if strategy_entry_signal is not None else getattr(position, "strategy_entry_signal", "")
        ).strip().lower() or None
        normalized_exit = worker._normalize_strategy_label(
            strategy_exit,
            fallback=worker._normalize_strategy_label(getattr(position, "strategy_exit", None)),
        )
        normalized_exit_component = worker._normalize_strategy_label(
            strategy_exit_component,
            fallback=(
                worker._normalize_strategy_label(getattr(position, "strategy_exit_component", None))
                or normalized_exit
            ),
        )
        normalized_exit_signal = str(
            strategy_exit_signal if strategy_exit_signal is not None else getattr(position, "strategy_exit_signal", "")
        ).strip().lower() or None
        position.strategy = carrier_strategy
        position.strategy_base = base_strategy
        position.strategy_entry = normalized_entry
        position.strategy_entry_component = normalized_entry_component
        position.strategy_entry_signal = normalized_entry_signal
        position.strategy_exit = normalized_exit
        position.strategy_exit_component = normalized_exit_component
        position.strategy_exit_signal = normalized_exit_signal
        position_id = str(position.position_id or "").strip()
        if position_id:
            worker._position_entry_strategy_by_id[position_id] = normalized_entry
            worker._position_entry_component_by_id[position_id] = normalized_entry_component
            if normalized_entry_signal:
                worker._position_entry_signal_by_id[position_id] = normalized_entry_signal
            else:
                worker._position_entry_signal_by_id.pop(position_id, None)

    def persist_position_trade_state(
        self,
        position: Position,
        *,
        strategy_exit: str | None = None,
        strategy_exit_component: str | None = None,
        strategy_exit_signal: str | None = None,
    ) -> None:
        worker = self.worker
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry=self.position_entry_strategy(position),
            strategy_entry_component=self.position_entry_component(position),
            strategy_entry_signal=self.position_entry_signal(position),
            strategy_exit=strategy_exit,
            strategy_exit_component=strategy_exit_component,
            strategy_exit_signal=strategy_exit_signal,
        )

    def effective_exit_metadata_from_reason(
        self,
        position: Position,
        reason: str,
    ) -> tuple[str | None, str | None, str | None]:
        worker = self.worker
        text = str(reason or "").strip().lower()
        if not text:
            return None, None, None
        position_exit_component = self.position_entry_component(position)
        marker = "strategy_exit:"
        if marker in text:
            tail = text.split(marker, 1)[1]
            scope = worker._normalize_strategy_label(tail.split(":", 1)[0])
            if scope and scope not in {"protective"}:
                return scope, scope, tail or None
            return position_exit_component, position_exit_component, tail or None
        if text.startswith("multi_strategy_emergency_mode"):
            return "multi_strategy_aggregator", "multi_strategy_aggregator", text
        if text.startswith("multi_"):
            return "multi_strategy_aggregator", "multi_strategy_aggregator", text
        if "reverse_signal:" in text or "signal_invalidation:" in text:
            return position_exit_component, position_exit_component, text
        if text.startswith("profit_lock") or text.startswith("early_loss_cut"):
            return position_exit_component, position_exit_component, text
        if (
            text.startswith("broker_manual_close:")
            or text.startswith("broker_position_missing")
            or text.endswith(":pending_broker_sync")
        ):
            return position_exit_component, position_exit_component, text
        return None, None, None
