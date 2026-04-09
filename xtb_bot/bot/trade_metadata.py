from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotTradeMetadataRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        self._trade_reason_summary_enabled = bot._env_bool("XTB_TRADE_REASON_SUMMARY_ENABLED", True)
        trade_reason_summary_interval_sec = bot._env_float("XTB_TRADE_REASON_SUMMARY_INTERVAL_SEC", 3600.0)
        trade_reason_summary_window_sec = bot._env_float("XTB_TRADE_REASON_SUMMARY_WINDOW_SEC", 3600.0)
        trade_reason_summary_scan_limit = bot._env_int("XTB_TRADE_REASON_SUMMARY_SCAN_LIMIT", 20_000)
        trade_reason_summary_top = bot._env_int("XTB_TRADE_REASON_SUMMARY_TOP", 20)
        self._trade_reason_summary_interval_sec = max(60.0, trade_reason_summary_interval_sec)
        self._trade_reason_summary_window_sec = max(60.0, trade_reason_summary_window_sec)
        self._trade_reason_summary_scan_limit = max(100, trade_reason_summary_scan_limit)
        self._trade_reason_summary_top = max(1, trade_reason_summary_top)
        self._last_trade_reason_summary_monotonic = 0.0
        self._last_trade_reason_summary_error_monotonic = 0.0

    @staticmethod
    def _is_allowance_related_error(error_text: str) -> bool:
        lowered = str(error_text or "").lower()
        return (
            "allowance cooldown is active" in lowered
            or "critical_trade_operation_active" in lowered
            or "exceeded-account-allowance" in lowered
            or "exceeded-api-key-allowance" in lowered
            or "exceeded-account-trading-allowance" in lowered
        )


    @staticmethod
    def _slug_reason(value: str, fallback: str = "unknown") -> str:
        text = str(value or "").strip().lower()
        if not text:
            return fallback

        parts: list[str] = []
        pending_sep = False
        for char in text:
            if char.isalnum():
                if pending_sep and parts:
                    parts.append("_")
                parts.append(char)
                pending_sep = False
            else:
                pending_sep = True

        normalized = "".join(parts).strip("_")
        if not normalized:
            return fallback
        return normalized[:120].rstrip("_")


    def _extract_ig_error_code(self, error_text: str) -> str | None:
        marker = '"errorCode":"'
        start = error_text.find(marker)
        if start < 0:
            return None
        start += len(marker)
        end = error_text.find('"', start)
        if end <= start:
            return None
        raw_code = error_text[start:end].strip()
        if not raw_code:
            return None
        return self._slug_reason(raw_code, fallback="unknown")


    def _extract_ig_api_endpoint(self, error_text: str) -> tuple[str, str] | None:
        marker = "IG API "
        start = error_text.find(marker)
        if start < 0:
            return None
        request_text = error_text[start + len(marker) :].strip()
        if not request_text:
            return None
        parts = request_text.split()
        if len(parts) < 2:
            return None
        method = self._slug_reason(parts[0], fallback="request")
        raw_path = parts[1].strip()
        segments = [segment for segment in raw_path.split("/") if segment]
        if not segments:
            return method, "root"
        resource = self._slug_reason(segments[0], fallback="unknown")
        if resource in {"positions", "workingorders"} and len(segments) > 1 and segments[1].lower() == "otc":
            resource = f"{resource}_otc"
        return method, resource


    def _normalize_broker_error_reason(self, error_text: str) -> str:
        text = str(error_text or "").strip()
        if not text:
            return "broker_error_unknown"
        lowered = text.lower()
        if lowered.startswith("ig deal rejected:"):
            tail = text.split(":", 1)[1].strip()
            reject_reason = tail.split("|", 1)[0].strip()
            return f"deal_rejected:{self._slug_reason(reject_reason, fallback='unknown')}"
        if "requested size below broker minimum" in lowered:
            return "requested_size_below_broker_minimum"
        if "allowance cooldown is active" in lowered:
            return "allowance_cooldown_active"
        endpoint = self._extract_ig_api_endpoint(text)
        if endpoint is not None:
            method, resource = endpoint
            error_code = self._extract_ig_error_code(text)
            if error_code:
                return f"ig_api_{method}_{resource}:{error_code}"
            return f"ig_api_{method}_{resource}:failed"
        return self._slug_reason(text, fallback="broker_error_unknown")


    def _event_reason(self, event: dict[str, object]) -> tuple[str, str] | None:
        message = str(event.get("message") or "")
        payload_raw = event.get("payload")
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        if message == "Trade blocked by risk manager":
            return "block", str(payload.get("reason") or "risk_manager")
        if message == "Trade blocked by spread filter":
            return "block", "spread_too_wide"
        if message == "Trade blocked by confidence threshold":
            return "block", "confidence_below_threshold"
        if message == "Trade blocked by entry cooldown":
            return "block", "entry_cooldown"
        if message == "Trade blocked by connectivity check":
            return "block", str(payload.get("reason") or "connectivity_check_failed")
        if message == "Trade blocked by stream health check":
            return "block", str(payload.get("reason") or "stream_health_degraded")
        if message == "Broker allowance backoff active":
            kind = str(payload.get("kind") or "").strip()
            if kind:
                return "block", f"allowance:{self._slug_reason(kind, fallback='unknown')}"
            error_text = str(payload.get("error") or "")
            return "block", f"allowance:{self._normalize_broker_error_reason(error_text)}"
        if message == "Broker error":
            error_text = str(payload.get("error") or "")
            return "reject", self._normalize_broker_error_reason(error_text)
        if message == "Signal hold reason":
            return "hold", str(payload.get("reason") or "unknown")
        return None


    def _maybe_record_trade_reason_summary(self, now_monotonic: float, *, force: bool = False) -> None:
        if not self._trade_reason_summary_enabled:
            return
        if (
            not force
            and (now_monotonic - self._last_trade_reason_summary_monotonic) < self._trade_reason_summary_interval_sec
        ):
            return
        self._last_trade_reason_summary_monotonic = now_monotonic
        since_ts = time.time() - self._trade_reason_summary_window_sec
        try:
            events = self._bot.store.load_events_since(since_ts, limit=self._trade_reason_summary_scan_limit)
            aggregates: dict[tuple[str, str], dict[str, object]] = {}
            matched = 0
            for event in events:
                reason_key = self._event_reason(event)
                if reason_key is None:
                    continue
                matched += 1
                kind, reason = reason_key
                key = (kind, reason)
                bucket = aggregates.get(key)
                if bucket is None:
                    bucket = {
                        "count": 0,
                        "last_ts": 0.0,
                        "symbols": set(),
                    }
                    aggregates[key] = bucket
                bucket["count"] = int(bucket["count"]) + 1
                bucket["last_ts"] = max(float(bucket["last_ts"]), float(event.get("ts") or 0.0))
                symbol = str(event.get("symbol") or "").strip().upper()
                if symbol:
                    symbols = bucket["symbols"]
                    if isinstance(symbols, set):
                        symbols.add(symbol)

            if matched == 0:
                return

            rows: list[dict[str, object]] = []
            by_kind: dict[str, int] = {}
            for (kind, reason), bucket in aggregates.items():
                count = int(bucket["count"])
                by_kind[kind] = by_kind.get(kind, 0) + count
                symbols_raw = bucket.get("symbols")
                symbols = sorted(str(item) for item in symbols_raw) if isinstance(symbols_raw, set) else []
                rows.append(
                    {
                        "kind": kind,
                        "reason": reason,
                        "count": count,
                        "last_ts": float(bucket["last_ts"]),
                        "symbols": symbols[:10],
                    }
                )
            rows.sort(key=lambda item: (-int(item["count"]), -float(item["last_ts"]), str(item["kind"]), str(item["reason"])))
            top_rows = rows[: self._trade_reason_summary_top]
            top_text = ", ".join(
                f"{row['kind']}:{row['reason']}={int(row['count'])}"
                for row in top_rows
            )
            logger.info(
                "Trade reason summary snapshot | window_sec=%d scanned=%d matched=%d unique=%d by_kind=%s top=%s",
                int(self._trade_reason_summary_window_sec),
                len(events),
                matched,
                len(rows),
                by_kind,
                top_text or "-",
            )
            self._bot.store.record_event(
                "INFO",
                None,
                "Trade reason summary snapshot",
                {
                    "window_sec": int(self._trade_reason_summary_window_sec),
                    "scanned_events": len(events),
                    "matched_events": matched,
                    "unique_reasons": len(rows),
                    "by_kind": by_kind,
                    "top_reasons": top_rows,
                },
            )
        except Exception as exc:
            if force or (now_monotonic - self._last_trade_reason_summary_error_monotonic) >= 60.0:
                logger.warning("Trade reason summary snapshot failed: %s", exc)
                self._last_trade_reason_summary_error_monotonic = now_monotonic



