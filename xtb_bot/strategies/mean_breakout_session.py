from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from xtb_bot.models import Side, Signal
from xtb_bot.strategies.base import Strategy, StrategyContext


class MeanBreakoutSessionStrategy(Strategy):
    name = "mean_breakout_session"

    _PREFERRED_SYMBOLS = {"DE30", "DE40", "OIL", "WTI", "BRENT"}

    def __init__(self, params: dict[str, object]):
        super().__init__(params)
        self.session_timezone = str(params.get("mean_breakout_session_timezone", "Europe/Kyiv")).strip()
        self.session_start = str(params.get("mean_breakout_session_start", "09:00")).strip()
        self.box_minutes = max(
            5,
            int(params.get("mean_breakout_session_box_minutes", params.get("mean_breakout_box_minutes", 30))),
        )
        self.trade_window_minutes = max(
            0,
            int(
                params.get(
                    "mean_breakout_session_trade_window_minutes",
                    params.get("mean_breakout_trade_window_minutes", 360),
                )
            ),
        )
        self.one_trade_per_session = self._as_bool(
            params.get(
                "mean_breakout_session_one_trade_per_session",
                params.get("mean_breakout_one_trade_per_session", True),
            ),
            default=True,
        )
        self.stop_loss_pips = float(
            params.get(
                "mean_breakout_session_stop_loss_pips",
                params.get("mean_breakout_stop_loss_pips", params.get("stop_loss_pips", 30.0)),
            )
        )
        self.take_profit_pips = float(
            params.get(
                "mean_breakout_session_take_profit_pips",
                params.get("mean_breakout_take_profit_pips", params.get("take_profit_pips", 75.0)),
            )
        )

        self._tz = self._resolve_timezone(self.session_timezone)
        self._session_start_hour, self._session_start_minute = self._parse_hhmm(self.session_start)
        self._last_traded_session_key: str | None = None
        self.min_history = 4

    @staticmethod
    def _as_bool(value: object, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        raw = str(value).strip().lower()
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _resolve_timezone(name: str) -> timezone | ZoneInfo:
        try:
            return ZoneInfo(name)
        except Exception:
            return timezone.utc

    @staticmethod
    def _parse_hhmm(raw: str) -> tuple[int, int]:
        try:
            hh_text, mm_text = raw.split(":", 1)
            hh = max(0, min(23, int(hh_text)))
            mm = max(0, min(59, int(mm_text)))
            return hh, mm
        except Exception:
            return 9, 0

    def _session_window(self, now_ts: float) -> tuple[float, float, str]:
        now_local = datetime.fromtimestamp(now_ts, tz=self._tz)
        session_open_local = now_local.replace(
            hour=self._session_start_hour,
            minute=self._session_start_minute,
            second=0,
            microsecond=0,
        )
        box_end_local = session_open_local + timedelta(minutes=self.box_minutes)
        session_key = session_open_local.strftime("%Y-%m-%d")
        return session_open_local.timestamp(), box_end_local.timestamp(), session_key

    def _opening_box(
        self,
        prices: list[float],
        timestamps: list[float],
        box_start_ts: float,
        box_end_ts: float,
    ) -> tuple[float, float, int] | None:
        box_prices = [
            float(price)
            for price, ts in zip(prices, timestamps)
            if box_start_ts <= float(ts) < box_end_ts
        ]
        if len(box_prices) < 2:
            return None
        return max(box_prices), min(box_prices), len(box_prices)

    def _hold_with_reason(self, reason: str, extra: dict[str, object] | None = None) -> Signal:
        payload: dict[str, object] = {
            "reason": reason,
            "indicator": "mean_breakout_session",
        }
        if extra:
            payload.update(extra)
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata=payload,
        )

    def generate_signal(self, ctx: StrategyContext) -> Signal:
        prices = [float(value) for value in ctx.prices]
        timestamps = [float(value) for value in ctx.timestamps]

        if len(prices) < self.min_history:
            return self._hold_with_reason("insufficient_price_history")
        if not timestamps or len(timestamps) != len(prices):
            return self._hold_with_reason("missing_or_misaligned_timestamps")

        now_ts = float(timestamps[-1])
        session_open_ts, box_end_ts, session_key = self._session_window(now_ts)

        if now_ts < session_open_ts:
            return self._hold_with_reason(
                "before_session_open",
                {
                    "session_timezone": self.session_timezone,
                    "session_start": self.session_start,
                },
            )

        if now_ts < box_end_ts:
            return self._hold_with_reason(
                "building_opening_box",
                {
                    "box_minutes": self.box_minutes,
                },
            )

        if self.trade_window_minutes > 0:
            trade_window_end = box_end_ts + self.trade_window_minutes * 60.0
            if now_ts > trade_window_end:
                return self._hold_with_reason(
                    "outside_trade_window",
                    {
                        "trade_window_minutes": self.trade_window_minutes,
                    },
                )

        opening_box = self._opening_box(prices, timestamps, session_open_ts, box_end_ts)
        if opening_box is None:
            return self._hold_with_reason(
                "opening_box_not_ready",
                {
                    "box_minutes": self.box_minutes,
                },
            )
        box_high, box_low, box_samples = opening_box

        if self.one_trade_per_session and self._last_traded_session_key == session_key:
            return self._hold_with_reason(
                "session_trade_already_taken",
                {
                    "session_key": session_key,
                },
            )

        prev = prices[-2]
        last = prices[-1]
        breakout_up = last > box_high and prev <= box_high
        breakout_down = last < box_low and prev >= box_low

        if not breakout_up and not breakout_down:
            return self._hold_with_reason(
                "no_box_breakout",
                {
                    "box_high": box_high,
                    "box_low": box_low,
                    "box_samples": box_samples,
                },
            )

        direction = "up" if breakout_up else "down"
        side = Side.BUY if breakout_up else Side.SELL
        breakout_distance = (last - box_high) if breakout_up else (box_low - last)
        box_range = max(1e-9, box_high - box_low)
        confidence = min(1.0, (max(0.0, breakout_distance) / box_range) * 2.5 + 0.25)

        self._last_traded_session_key = session_key
        symbol_upper = str(ctx.symbol).upper()
        preferred_symbol = symbol_upper in self._PREFERRED_SYMBOLS

        return Signal(
            side=side,
            confidence=confidence,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata={
                "indicator": "mean_breakout_session",
                "direction": direction,
                "session_key": session_key,
                "session_timezone": self.session_timezone,
                "session_start": self.session_start,
                "box_minutes": self.box_minutes,
                "trade_window_minutes": self.trade_window_minutes,
                "box_high": box_high,
                "box_low": box_low,
                "box_range": box_range,
                "box_samples": box_samples,
                "breakout_distance": breakout_distance,
                "preferred_symbol": preferred_symbol,
            },
        )
