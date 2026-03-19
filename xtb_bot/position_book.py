from __future__ import annotations

import threading
from typing import Any

from xtb_bot.models import Position


class PositionBook:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._positions_by_id: dict[str, Position] = {}
        self._symbol_index: dict[str, set[str]] = {}

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return str(symbol).strip().upper()

    @staticmethod
    def _position_sort_key(position: Position) -> tuple[float, str]:
        return (float(position.opened_at), str(position.position_id))

    def bootstrap(self, positions: dict[str, Position] | list[Position]) -> None:
        if isinstance(positions, dict):
            values = list(positions.values())
        else:
            values = list(positions)
        with self._lock:
            self._positions_by_id.clear()
            self._symbol_index.clear()
            for position in values:
                position_id = str(position.position_id).strip()
                if not position_id or position.status != "open":
                    continue
                symbol = self._normalize_symbol(position.symbol)
                self._positions_by_id[position_id] = position
                self._symbol_index.setdefault(symbol, set()).add(position_id)

    def get(self, symbol: str) -> Position | None:
        normalized_symbol = self._normalize_symbol(symbol)
        with self._lock:
            ids = list(self._symbol_index.get(normalized_symbol, set()))
            candidates = [self._positions_by_id[position_id] for position_id in ids if position_id in self._positions_by_id]
        if not candidates:
            return None
        return min(candidates, key=self._position_sort_key)

    def get_all(self, symbol: str) -> list[Position]:
        normalized_symbol = self._normalize_symbol(symbol)
        with self._lock:
            ids = list(self._symbol_index.get(normalized_symbol, set()))
            candidates = [self._positions_by_id[position_id] for position_id in ids if position_id in self._positions_by_id]
        return sorted(candidates, key=self._position_sort_key)

    def get_by_id(self, position_id: str) -> Position | None:
        normalized_position_id = str(position_id).strip()
        if not normalized_position_id:
            return None
        with self._lock:
            return self._positions_by_id.get(normalized_position_id)

    def upsert(self, position: Position) -> None:
        position_id = str(position.position_id).strip()
        if not position_id:
            return
        normalized_symbol = self._normalize_symbol(position.symbol)
        with self._lock:
            previous = self._positions_by_id.get(position_id)
            previous_symbol = self._normalize_symbol(previous.symbol) if previous is not None else None
            if previous_symbol and previous_symbol != normalized_symbol:
                previous_ids = self._symbol_index.get(previous_symbol)
                if previous_ids is not None:
                    previous_ids.discard(position_id)
                    if not previous_ids:
                        self._symbol_index.pop(previous_symbol, None)
            if position.status == "open":
                self._positions_by_id[position_id] = position
                self._symbol_index.setdefault(normalized_symbol, set()).add(position_id)
            else:
                self._positions_by_id.pop(position_id, None)
                ids = self._symbol_index.get(normalized_symbol)
                if ids is not None:
                    ids.discard(position_id)
                    if not ids:
                        self._symbol_index.pop(normalized_symbol, None)

    def remove(self, symbol: str) -> None:
        normalized_symbol = self._normalize_symbol(symbol)
        with self._lock:
            ids = list(self._symbol_index.pop(normalized_symbol, set()))
            for position_id in ids:
                self._positions_by_id.pop(position_id, None)

    def remove_by_id(self, position_id: str) -> None:
        normalized_position_id = str(position_id).strip()
        if not normalized_position_id:
            return
        with self._lock:
            position = self._positions_by_id.pop(normalized_position_id, None)
            if position is None:
                return
            normalized_symbol = self._normalize_symbol(position.symbol)
            ids = self._symbol_index.get(normalized_symbol)
            if ids is not None:
                ids.discard(normalized_position_id)
                if not ids:
                    self._symbol_index.pop(normalized_symbol, None)

    def count(self) -> int:
        with self._lock:
            return len(self._positions_by_id)

    def all_open(self) -> list[Position]:
        with self._lock:
            values = list(self._positions_by_id.values())
        return sorted(values, key=self._position_sort_key)

    def as_dict(self) -> dict[str, Any]:
        with self._lock:
            return {position_id: pos.to_dict() for position_id, pos in self._positions_by_id.items()}
