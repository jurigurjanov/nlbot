from __future__ import annotations

import logging
import math
import time
from typing import TYPE_CHECKING

from xtb_bot.bot._utils import _TokenBucket

if TYPE_CHECKING:
    from xtb_bot.bot.core import TradingBot

logger = logging.getLogger(__name__)


class BotIgBudgetRuntime:
    def __init__(self, bot: TradingBot) -> None:
        self._bot = bot
        cfg = bot.config

        self._rate_limit_metrics_flush_interval_sec = max(
            1.0,
            bot._env_float("XTB_IG_RATE_LIMIT_METRICS_FLUSH_INTERVAL_SEC", 5.0),
        )
        self._last_rate_limit_metrics_flush_monotonic = 0.0
        self._last_rate_limit_metrics_flush_error_monotonic = 0.0

        self._enabled = bot._env_bool(
            "XTB_IG_NON_TRADING_BUDGET_ENABLED",
            cfg.broker == "ig",
        )
        ig_non_trading_budget_rpm = bot._env_float("XTB_IG_NON_TRADING_BUDGET_RPM", 24.0)
        self._rpm = max(1.0, min(29.0, ig_non_trading_budget_rpm))
        ig_non_trading_reserve_default = "12" if cfg.broker == "ig" else "4"
        reserve_rpm = bot._env_float(
            "XTB_IG_NON_TRADING_BUDGET_RESERVE_RPM",
            float(ig_non_trading_reserve_default),
        )
        max_reserve_rpm = max(0.0, self._rpm - 1.0)
        self._reserve_rpm = max(0.0, min(max_reserve_rpm, reserve_rpm))
        ig_non_trading_budget_burst = bot._env_int("XTB_IG_NON_TRADING_BUDGET_BURST", 4)
        self._burst = max(1, min(30, ig_non_trading_budget_burst))
        self._bucket = _TokenBucket(
            capacity=self._burst,
            refill_per_sec=self._rpm / 60.0,
        )
        self._last_warn_monotonic = 0.0
        self._blocked_total = 0
        self._warn_interval_sec = max(
            5.0,
            bot._env_float("XTB_IG_NON_TRADING_BUDGET_WARN_INTERVAL_SEC", 30.0),
        )
        self._account_non_trading_warn_at = max(1, bot._env_int("XTB_IG_ACCOUNT_NON_TRADING_WARN_AT", 24))
        self._last_account_non_trading_warn_monotonic = 0.0
        self._last_account_non_trading_snapshot: dict[str, float | int] | None = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def rpm(self) -> float:
        return self._rpm

    @property
    def reserve_rpm(self) -> float:
        return self._reserve_rpm

    @property
    def burst(self) -> int:
        return self._burst

    @property
    def last_account_non_trading_snapshot(self) -> dict[str, float | int] | None:
        return self._last_account_non_trading_snapshot

    def reserve(
        self,
        *,
        scope: str,
        wait_timeout_sec: float = 0.0,
    ) -> bool:
        bot = self._bot
        if (not self._enabled) or bot.config.broker != "ig":
            return True
        deadline = time.monotonic() + max(0.0, float(wait_timeout_sec))
        while not bot.stop_event.is_set():
            if self._bucket.try_consume():
                return True
            if time.monotonic() >= deadline:
                break
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

        now_monotonic = time.monotonic()
        self._blocked_total += 1
        if (now_monotonic - self._last_warn_monotonic) >= self._warn_interval_sec:
            available_tokens = self._bucket.available_tokens()
            payload = {
                "scope": str(scope).strip().lower(),
                "budget_rpm": self._rpm,
                "budget_burst": self._burst,
                "available_tokens": round(available_tokens, 3),
                "blocked_total": self._blocked_total,
                "mode": bot.config.mode.value,
            }
            bot.store.record_event(
                "WARN",
                None,
                "IG non-trading request deferred by local budget",
                payload,
            )
            logger.warning(
                "IG non-trading request deferred by local budget | scope=%s blocked_total=%s tokens=%.3f rpm=%.1f burst=%s",
                str(scope).strip().lower(),
                self._blocked_total,
                available_tokens,
                self._rpm,
                self._burst,
            )
            self._last_warn_monotonic = now_monotonic
        return False

    def account_non_trading_under_pressure(self) -> bool:
        snapshot = self._last_account_non_trading_snapshot
        if not isinstance(snapshot, dict):
            return False
        used_value = int(float(snapshot.get("used_value") or 0))
        limit_value = max(0, int(float(snapshot.get("limit_value") or 0)))
        remaining_value = max(0, int(float(snapshot.get("remaining_value") or 0)))
        if limit_value <= 0:
            return False
        if used_value >= self._account_non_trading_warn_at:
            return True
        if remaining_value <= 0:
            return True
        return False

    def flush_worker_metrics(self, now_monotonic: float) -> None:
        bot = self._bot
        if bot.config.broker != "ig":
            return
        if (now_monotonic - self._last_rate_limit_metrics_flush_monotonic) < self._rate_limit_metrics_flush_interval_sec:
            return

        getter = getattr(bot.broker, "get_rate_limit_workers_snapshot", None)
        if not callable(getter):
            return
        self._last_rate_limit_metrics_flush_monotonic = now_monotonic

        try:
            snapshot_rows = getter()
        except Exception as exc:
            if (now_monotonic - self._last_rate_limit_metrics_flush_error_monotonic) >= 60.0:
                logger.warning("Rate-limit worker metrics flush failed: %s", exc)
                bot.store.record_event(
                    "WARN",
                    None,
                    "Rate-limit worker metrics flush failed",
                    {"error": str(exc)},
                )
                self._last_rate_limit_metrics_flush_error_monotonic = now_monotonic
            return

        self._last_rate_limit_metrics_flush_error_monotonic = 0.0
        if not isinstance(snapshot_rows, list):
            return
        for row in snapshot_rows:
            if not isinstance(row, dict):
                continue
            try:
                worker_key = str(row.get("worker_key") or "").strip().lower()
                limit_scope = str(row.get("limit_scope") or "unknown")
                limit_unit = str(row.get("limit_unit") or "requests_per_minute")
                limit_value = int(float(row.get("limit_value") or 0))
                used_value = int(float(row.get("used_value") or 0))
                remaining_value = int(float(row.get("remaining_value") or 0))
                window_sec = float(row.get("window_sec") or 60.0)
                blocked_total = int(float(row.get("blocked_total") or 0))
                last_request_ts = float(row.get("last_request_ts")) if row.get("last_request_ts") else None
                last_block_ts = float(row.get("last_block_ts")) if row.get("last_block_ts") else None
                notes = str(row.get("notes")) if row.get("notes") is not None else None
                bot.store.upsert_ig_rate_limit_worker(
                    worker_key=worker_key,
                    limit_scope=limit_scope,
                    limit_unit=limit_unit,
                    limit_value=limit_value,
                    used_value=used_value,
                    remaining_value=remaining_value,
                    window_sec=window_sec,
                    blocked_total=blocked_total,
                    last_request_ts=last_request_ts,
                    last_block_ts=last_block_ts,
                    notes=notes,
                )
            except Exception as exc:
                logger.debug("Failed to persist rate-limit worker row %s: %s", row, exc)
                continue
            if worker_key == "account_non_trading" and used_value >= self._account_non_trading_warn_at:
                self._last_account_non_trading_snapshot = {
                    "used_value": used_value,
                    "limit_value": limit_value,
                    "remaining_value": remaining_value,
                    "window_sec": window_sec,
                    "blocked_total": blocked_total,
                }
                if (now_monotonic - self._last_account_non_trading_warn_monotonic) < 60.0:
                    continue
                self._last_account_non_trading_warn_monotonic = now_monotonic
                payload = {
                    "used_value": used_value,
                    "limit_value": limit_value,
                    "remaining_value": remaining_value,
                    "window_sec": window_sec,
                    "blocked_total": blocked_total,
                    "warn_at": self._account_non_trading_warn_at,
                    "mode": bot.config.mode.value,
                }
                bot.store.record_event(
                    "WARN",
                    None,
                    "IG account non-trading usage is high",
                    payload,
                )
                logger.warning(
                    "IG account non-trading usage is high | used=%s limit=%s remaining=%s window=%.1fs blocked_total=%s",
                    used_value,
                    limit_value,
                    remaining_value,
                    window_sec,
                    blocked_total,
                )
            elif worker_key == "account_non_trading":
                self._last_account_non_trading_snapshot = {
                    "used_value": used_value,
                    "limit_value": limit_value,
                    "remaining_value": remaining_value,
                    "window_sec": window_sec,
                    "blocked_total": blocked_total,
                }
