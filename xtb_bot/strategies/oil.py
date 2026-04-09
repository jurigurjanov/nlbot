from __future__ import annotations

from typing import Any

from xtb_bot.models import Side, Signal
from xtb_bot.strategies.base import StrategyContext
from xtb_bot.strategies.g1 import G1Strategy


class OilStrategy(G1Strategy):
    name = "oil"
    _SUPPORTED_SYMBOLS = {"WTI", "BRENT", "USOIL", "UKOIL"}
    _CONTINUATION_SIGNALS = {"ema_trend_up", "ema_trend_down"}

    def supports_symbol(self, symbol: str) -> bool:
        return str(symbol or "").strip().upper() in self._SUPPORTED_SYMBOLS

    @staticmethod
    def _reverse_side(side: Side) -> Side:
        if side == Side.BUY:
            return Side.SELL
        if side == Side.SELL:
            return Side.BUY
        return Side.HOLD

    def _oil_metadata(
        self,
        metadata: dict[str, Any] | None,
        *,
        base_side: Side,
        side: Side,
    ) -> dict[str, Any]:
        payload = dict(metadata or {})
        base_indicator = str(payload.get("indicator") or "g1").strip().lower() or "g1"
        payload["indicator"] = "oil"
        payload["base_indicator"] = base_indicator
        payload["oil_contrarian_reversal"] = True
        payload["oil_base_strategy"] = "g1"
        payload["oil_base_side"] = base_side.value

        if "trend_signal" in payload:
            payload["base_trend_signal"] = payload.pop("trend_signal")
        if "fast_ema" in payload:
            payload["base_fast_ema"] = payload.pop("fast_ema")
        if "slow_ema" in payload:
            payload["base_slow_ema"] = payload.pop("slow_ema")
        if "price_aligned_with_fast_ma" in payload:
            payload["base_price_aligned_with_fast_ma"] = payload.pop("price_aligned_with_fast_ma")

        if side != Side.HOLD:
            payload["signal_side_implied"] = side.value
            payload["direction"] = "up" if side == Side.BUY else "down"
            payload["oil_signal_side"] = side.value
        else:
            payload.pop("signal_side_implied", None)
            payload.pop("direction", None)
            payload.pop("trend", None)

        return payload

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        if not self.supports_symbol(ctx.symbol):
            signal = self.hold()
            metadata = dict(signal.metadata or {})
            metadata["indicator"] = "oil"
            metadata["reason"] = "unsupported_symbol"
            return Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=float(signal.stop_loss_pips),
                take_profit_pips=float(signal.take_profit_pips),
                metadata=metadata,
            )

        base_signal = super().generate_signal(ctx)
        base_metadata = base_signal.metadata if isinstance(base_signal.metadata, dict) else {}
        base_trend_signal = str(base_metadata.get("trend_signal") or "").strip().lower()
        if base_signal.side == Side.HOLD:
            metadata = self._oil_metadata(
                base_metadata,
                base_side=base_signal.side,
                side=Side.HOLD,
            )
            return Signal(
                side=Side.HOLD,
                confidence=float(base_signal.confidence),
                stop_loss_pips=float(base_signal.stop_loss_pips),
                take_profit_pips=float(base_signal.take_profit_pips),
                metadata=metadata,
            )

        if base_trend_signal not in self._CONTINUATION_SIGNALS:
            metadata = self._oil_metadata(
                base_metadata,
                base_side=base_signal.side,
                side=Side.HOLD,
            )
            metadata["reason"] = "oil_waiting_continuation_tail"
            metadata["oil_pattern_gate"] = "g1_continuation_only"
            return Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=float(base_signal.stop_loss_pips),
                take_profit_pips=float(base_signal.take_profit_pips),
                metadata=metadata,
            )

        reversed_side = self._reverse_side(base_signal.side)
        metadata = self._oil_metadata(
            base_metadata,
            base_side=base_signal.side,
            side=reversed_side,
        )
        metadata["oil_pattern_gate"] = "g1_continuation_only"
        return Signal(
            side=reversed_side,
            confidence=float(base_signal.confidence),
            stop_loss_pips=float(base_signal.stop_loss_pips),
            take_profit_pips=float(base_signal.take_profit_pips),
            metadata=metadata,
        )
