from __future__ import annotations

import logging
import math
import threading
import time
from typing import TYPE_CHECKING, Any, Callable
from urllib import parse, request

if TYPE_CHECKING:
    from xtb_bot.ig_client import IgApiClient

from xtb_bot.client import BrokerError
from xtb_bot.models import PriceTick, StreamHealthStatus
from xtb_bot.tolerances import FLOAT_ROUNDING_TOLERANCE

logger = logging.getLogger(__name__)


class IgStreamMixin:
    """Lightstreamer streaming, subscription, and tick-handling methods for IgApiClient."""

    __slots__ = ()

    # ------------------------------------------------------------------
    # Stream subscription scheduling helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_stream_subscription_invalid_group_error_text(text: str | None) -> bool:
        lowered = str(text or "").strip().lower()
        if not lowered:
            return False
        return "invalid group" in lowered or lowered.startswith("error:21:")

    def _promote_stream_subscription_epic_after_invalid_group(
        self,
        symbol: str,
        failed_epic: str,
        *,
        reason: str,
    ) -> str | None:
        upper_symbol = str(symbol).upper().strip()
        normalized_failed = str(failed_epic).upper().strip()
        if not upper_symbol or not normalized_failed:
            return None

        self._mark_epic_temporarily_invalid(upper_symbol, normalized_failed, reason=reason)

        current_ts = time.time()
        for candidate in self._epic_attempt_order(upper_symbol):
            if candidate == normalized_failed:
                continue
            if self._is_epic_temporarily_invalid(upper_symbol, candidate, now_ts=current_ts):
                continue
            self._activate_epic(upper_symbol, candidate, log_warning=False)
            self._clear_stream_rest_fallback_block(upper_symbol)
            self._schedule_stream_subscription_retry(upper_symbol)
            logger.warning(
                "IG stream epic failover queued for %s: %s -> %s (retry_in=%.1fs %s)",
                upper_symbol,
                normalized_failed,
                candidate,
                self._stream_subscription_retry_gap_sec,
                reason,
            )
            return candidate
        self._mark_stream_rest_fallback_block(upper_symbol, reason=reason)
        return None

    def _stream_subscription_retry_remaining(self, symbol: str, *, now_ts: float | None = None) -> float:
        upper = str(symbol).upper().strip()
        if not upper:
            return 0.0
        current_ts = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            until_ts = float(self._stream_subscription_retry_not_before_ts_by_symbol.get(upper, 0.0))
            remaining = until_ts - current_ts
            if remaining > 0:
                return remaining
            self._stream_subscription_retry_not_before_ts_by_symbol.pop(upper, None)
            return 0.0

    def _schedule_stream_subscription_retry(self, symbol: str, delay_sec: float | None = None) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        normalized_delay = self._stream_subscription_retry_gap_sec if delay_sec is None else float(delay_sec)
        if not math.isfinite(normalized_delay):
            normalized_delay = self._stream_subscription_retry_gap_sec
        normalized_delay = max(0.0, normalized_delay)
        with self._lock:
            self._stream_subscription_retry_not_before_ts_by_symbol[upper] = max(
                float(self._stream_subscription_retry_not_before_ts_by_symbol.get(upper, 0.0)),
                time.time() + normalized_delay,
            )

    def _clear_stream_subscription_retry(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        with self._lock:
            self._stream_subscription_retry_not_before_ts_by_symbol.pop(upper, None)

    # ------------------------------------------------------------------
    # Stream REST-fallback block tracking
    # ------------------------------------------------------------------

    def _stream_rest_fallback_block_remaining(self, symbol: str, *, now_ts: float | None = None) -> float:
        upper = str(symbol).upper().strip()
        if not upper:
            return 0.0
        current_ts = time.time() if now_ts is None else float(now_ts)
        with self._lock:
            until_ts = float(self._stream_rest_fallback_block_until_ts_by_symbol.get(upper, 0.0))
            remaining = until_ts - current_ts
            if remaining > 0:
                return remaining
            self._stream_rest_fallback_block_until_ts_by_symbol.pop(upper, None)
            self._stream_rest_fallback_block_reason_by_symbol.pop(upper, None)
            return 0.0

    def _stream_rest_fallback_block_reason(self, symbol: str) -> str | None:
        upper = str(symbol).upper().strip()
        if not upper:
            return None
        with self._lock:
            reason = str(self._stream_rest_fallback_block_reason_by_symbol.get(upper, "")).strip()
        return reason or None

    def _mark_stream_rest_fallback_block(self, symbol: str, *, reason: str | None = None) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        cooldown_sec = max(30.0, float(self._invalid_epic_retry_sec))
        until_ts = time.time() + cooldown_sec
        with self._lock:
            self._stream_rest_fallback_block_until_ts_by_symbol[upper] = max(
                float(self._stream_rest_fallback_block_until_ts_by_symbol.get(upper, 0.0)),
                until_ts,
            )
            normalized_reason = str(reason or "").strip()
            if normalized_reason:
                self._stream_rest_fallback_block_reason_by_symbol[upper] = normalized_reason

    def _clear_stream_rest_fallback_block(self, symbol: str) -> None:
        upper = str(symbol).upper().strip()
        if not upper:
            return
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)

    def _clear_stream_rest_fallback_block_locked(self, upper: str) -> None:
        self._stream_rest_fallback_block_until_ts_by_symbol.pop(upper, None)
        self._stream_rest_fallback_block_reason_by_symbol.pop(upper, None)

    # ------------------------------------------------------------------
    # Low-level stream decoding
    # ------------------------------------------------------------------

    @staticmethod
    def _decode_stream_field(encoded: str, previous: str | None) -> str | None:
        if encoded == "":
            return previous
        if encoded == "$":
            return ""
        if encoded == "#":
            return None
        if encoded.startswith("$$") or encoded.startswith("##"):
            return encoded[1:]
        if encoded[0] in {"$", "#"}:
            return encoded[1:]
        return encoded

    @staticmethod
    def _read_stream_line(stream_response: Any) -> str | None:
        raw = stream_response.readline()
        if raw is None:
            return None
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw)
        if text == "":
            return None
        return text.rstrip("\r\n")

    # ------------------------------------------------------------------
    # Stream backoff & control
    # ------------------------------------------------------------------

    def _stream_backoff_delay(self, attempt: int) -> float:
        if attempt <= 0:
            return self._stream_backoff_base_sec
        return min(self._stream_backoff_max_sec, self._stream_backoff_base_sec * (2 ** (attempt - 1)))

    def _resolve_stream_control_url(self, control_address: str | None) -> str | None:
        stream_endpoint = self._stream_endpoint
        if not stream_endpoint:
            return None
        if not control_address:
            return f"{stream_endpoint}/control.txt"

        raw = control_address.strip().rstrip("/")
        if not raw:
            return f"{stream_endpoint}/control.txt"

        parsed_addr = parse.urlparse(raw)
        if parsed_addr.scheme:
            base = raw
        else:
            stream_scheme = parse.urlparse(stream_endpoint).scheme or "https"
            base = f"{stream_scheme}://{raw}"

        base = base.rstrip("/")
        if not base.endswith("/lightstreamer"):
            base = f"{base}/lightstreamer"
        return f"{base}/control.txt"

    # ------------------------------------------------------------------
    # Stream state transitions (called under self._lock)
    # ------------------------------------------------------------------

    def _mark_stream_disconnected_locked(self, error_text: str | None, schedule_retry: bool = True) -> None:
        response = self._stream_http_response
        self._stream_http_response = None
        if response is not None:
            try:
                response.close()
            except Exception:
                pass

        self._stream_session_id = None
        self._stream_control_url = None
        self._stream_keepalive_ms = None
        self._stream_symbol_to_table.clear()
        self._stream_table_to_symbol.clear()
        self._stream_table_field_values.clear()
        self._stream_last_disconnect_ts = time.time()

        if error_text:
            self._stream_last_error = error_text

        if schedule_retry:
            self._stream_reconnect_attempts += 1
            delay = self._stream_backoff_delay(self._stream_reconnect_attempts)
            self._stream_next_retry_at = time.time() + delay
        else:
            self._stream_reconnect_attempts = 0
            self._stream_next_retry_at = None

    def _mark_stream_connected_locked(
        self,
        session_id: str,
        control_url: str | None,
        keepalive_ms: int | None,
    ) -> None:
        was_reconnect = self._stream_last_disconnect_ts is not None
        self._stream_session_id = session_id
        self._stream_control_url = control_url
        self._stream_keepalive_ms = keepalive_ms
        self._stream_last_error = None
        self._stream_reconnect_attempts = 0
        self._stream_next_retry_at = None
        self._stream_last_reconnect_ts = time.time()

        if was_reconnect:
            self._stream_total_reconnects += 1
            logger.warning(
                "IG Lightstreamer reconnected | total_reconnects=%s",
                self._stream_total_reconnects,
            )

    # ------------------------------------------------------------------
    # SDK callbacks
    # ------------------------------------------------------------------

    def _on_stream_sdk_status_change(self, status: str) -> None:
        normalized = str(status or "").strip()
        lowered = normalized.lower()
        now_ts = time.time()
        should_resubscribe = False

        with self._lock:
            was_connected = bool(self._stream_sdk_connected)
            is_connected = lowered.startswith("connected")
            self._stream_sdk_status = normalized or None
            self._stream_sdk_connected = is_connected
            if is_connected:
                self._stream_session_id = "lightstreamer_sdk"
                self._stream_control_url = None
                self._stream_keepalive_ms = None
                self._stream_last_error = None
                self._stream_reconnect_attempts = 0
                self._stream_next_retry_at = None
                self._stream_last_reconnect_ts = now_ts
                if was_connected:
                    return
                if self._stream_last_disconnect_ts is not None:
                    self._stream_total_reconnects += 1
                    logger.warning(
                        "IG Lightstreamer SDK reconnected | total_reconnects=%s",
                        self._stream_total_reconnects,
                    )
                should_resubscribe = True
            else:
                self._stream_session_id = None
                self._stream_control_url = None
                self._stream_keepalive_ms = None
                if normalized:
                    self._stream_last_error = f"sdk_status:{lowered}"
                self._stream_symbol_to_table.clear()
                self._stream_table_to_symbol.clear()
                self._stream_table_field_values.clear()
                if was_connected:
                    self._stream_last_disconnect_ts = now_ts
                    self._stream_reconnect_attempts += 1
                    delay = self._stream_backoff_delay(self._stream_reconnect_attempts)
                    self._stream_next_retry_at = now_ts + delay

        if should_resubscribe:
            self._resubscribe_desired_symbols()

    def _on_stream_sdk_server_error(self, code: int, message: str) -> None:
        with self._lock:
            self._stream_last_error = f"sdk_server_error:{code}:{message}"
            self._stream_sdk_connected = False
            self._stream_session_id = None
            if self._stream_last_disconnect_ts is None:
                self._stream_last_disconnect_ts = time.time()
            self._stream_reconnect_attempts += 1
            delay = self._stream_backoff_delay(self._stream_reconnect_attempts)
            self._stream_next_retry_at = time.time() + delay
            # Tear down the broken SDK client so _start_stream_sdk_locked can
            # create a fresh one on the next connect() call.
            self._stop_stream_sdk_locked()
        logger.warning(
            "IG Lightstreamer SDK server error, tearing down client | code=%s message=%s reconnect_in=%.1fs attempt=%s",
            code,
            message,
            delay,
            self._stream_reconnect_attempts,
        )

    def _ensure_stream_sdk_symbol_table_locked(self, upper: str) -> int:
        from xtb_bot.ig_client import LIGHTSTREAMER_PRICE_FIELDS

        self._stream_sdk_pending_subscriptions.discard(upper)
        table_id = self._stream_symbol_to_table.get(upper)
        if table_id is None:
            table_id = self._stream_next_table_id
            self._stream_next_table_id += 1
            self._stream_symbol_to_table[upper] = table_id
            self._stream_table_to_symbol[table_id] = upper
            self._stream_table_field_values[table_id] = [None] * len(LIGHTSTREAMER_PRICE_FIELDS)
        self._stream_last_error = None
        return table_id

    def _on_stream_sdk_subscription(self, symbol: str) -> None:
        upper = str(symbol).upper()
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)
            self._ensure_stream_sdk_symbol_table_locked(upper)

    def _on_stream_sdk_unsubscription(self, symbol: str) -> None:
        upper = str(symbol).upper()
        with self._lock:
            self._stream_sdk_pending_subscriptions.discard(upper)
            table_id = self._stream_symbol_to_table.pop(upper, None)
            if table_id is not None:
                self._stream_table_to_symbol.pop(table_id, None)
                self._stream_table_field_values.pop(table_id, None)

    def _on_stream_sdk_subscription_error(self, symbol: str, code: int, message: str) -> None:
        upper = str(symbol).upper()
        reason = f"error:{code}:{message}"
        failed_epic = ""
        with self._lock:
            self._stream_sdk_pending_subscriptions.discard(upper)
            self._stream_sdk_subscriptions.pop(upper, None)
            self._stream_sdk_subscription_listeners.pop(upper, None)
            table_id = self._stream_symbol_to_table.pop(upper, None)
            if table_id is not None:
                self._stream_table_to_symbol.pop(table_id, None)
                self._stream_table_field_values.pop(table_id, None)
            self._stream_last_error = f"subscription_failed:{upper}:{reason}"
            failed_epic = str(self._epics.get(upper) or "").strip().upper()

        if not self._is_stream_subscription_invalid_group_error_text(reason):
            return

        next_epic = self._promote_stream_subscription_epic_after_invalid_group(
            upper,
            failed_epic,
            reason=f"stream_subscription:{reason}",
        )
        if next_epic:
            return

    @staticmethod
    def _stream_sdk_value(update: Any, field_name: str) -> str | None:
        getter = getattr(update, "getValue", None)
        if callable(getter):
            try:
                value = getter(field_name)
            except Exception:
                value = None
            if value is not None:
                return str(value)
        fields_getter = getattr(update, "getFields", None)
        if callable(fields_getter):
            try:
                fields = fields_getter()
            except Exception:
                fields = None
            if isinstance(fields, dict):
                value = fields.get(field_name)
                if value is not None:
                    return str(value)
        return None

    def _on_stream_sdk_item_update(self, symbol: str, update: Any) -> None:
        from xtb_bot.ig_client import LIGHTSTREAMER_PRICE_FIELDS

        upper = str(symbol).upper()
        with self._lock:
            self._clear_stream_rest_fallback_block_locked(upper)
            table_id = self._ensure_stream_sdk_symbol_table_locked(upper)
            previous_values = list(
                self._stream_table_field_values.get(table_id, [None] * len(LIGHTSTREAMER_PRICE_FIELDS))
            )
        decoded = list(previous_values)
        for idx, field_name in enumerate(LIGHTSTREAMER_PRICE_FIELDS):
            value = self._stream_sdk_value(update, field_name)
            if value is not None:
                decoded[idx] = value
        with self._lock:
            if table_id is not None:
                self._stream_table_field_values[table_id] = decoded
        self._update_tick_from_stream_fields(upper, decoded)

    # ------------------------------------------------------------------
    # SDK start/stop
    # ------------------------------------------------------------------

    def _start_stream_sdk_locked(self) -> None:
        from xtb_bot.ig_client import (
            LIGHTSTREAMER_ADAPTER_SET,
            _IgLightstreamerClientListener,
            _LightstreamerClient,
            _LightstreamerSubscription,
        )

        if _LightstreamerClient is None or _LightstreamerSubscription is None:
            self._stream_last_error = "lightstreamer_sdk_not_installed"
            return
        if self._stream_sdk_client is not None:
            return
        if not self._stream_endpoint:
            self._stream_last_error = "lightstreamer_endpoint_missing"
            return
        if not self.account_id:
            self._stream_last_error = "lightstreamer_account_id_missing"
            return
        if not self._cst or not self._security_token:
            self._stream_last_error = "lightstreamer_auth_tokens_missing"
            return

        try:
            # SDK v2 appends /lightstreamer/ to the server address internally,
            # so strip the suffix that _normalize_lightstreamer_endpoint added.
            sdk_endpoint = self._stream_endpoint
            if sdk_endpoint and sdk_endpoint.endswith("/lightstreamer"):
                sdk_endpoint = sdk_endpoint[: -len("/lightstreamer")]
            # IG Lightstreamer does not support WebSocket (returns 403),
            # so force HTTP-STREAMING unless overridden via env.
            forced_transport = self._stream_sdk_forced_transport or "HTTP-STREAMING"
            logger.info(
                "IG Lightstreamer SDK connecting | endpoint=%s account=%s adapter=%s transport=%s",
                sdk_endpoint, self.account_id, LIGHTSTREAMER_ADAPTER_SET, forced_transport,
            )
            client = _LightstreamerClient(sdk_endpoint, LIGHTSTREAMER_ADAPTER_SET)
            client.connectionDetails.setUser(self.account_id)
            client.connectionDetails.setPassword(f"CST-{self._cst}|XST-{self._security_token}")
            client.connectionOptions.setForcedTransport(forced_transport)
            listener = _IgLightstreamerClientListener(self)
            client.addListener(listener)
            self._stream_sdk_client = client
            self._stream_sdk_client_listener = listener
            self._stream_sdk_connected = False
            self._stream_sdk_status = "connecting"
            client.connect()
            logger.info("IG Lightstreamer SDK connect() returned, waiting for status callback")
        except Exception as exc:
            self._stream_sdk_client = None
            self._stream_sdk_client_listener = None
            self._stream_sdk_connected = False
            self._stream_sdk_status = None
            self._stream_last_error = f"sdk_connect_failed:{exc}"

    def _stop_stream_sdk_locked(self) -> None:
        client = self._stream_sdk_client
        listener = self._stream_sdk_client_listener
        subscriptions = list(self._stream_sdk_subscriptions.values())
        self._stream_sdk_client = None
        self._stream_sdk_client_listener = None
        self._stream_sdk_subscriptions.clear()
        self._stream_sdk_subscription_listeners.clear()
        self._stream_sdk_pending_subscriptions.clear()
        self._stream_sdk_connected = False
        self._stream_sdk_status = None
        self._stream_session_id = None
        self._stream_control_url = None
        self._stream_keepalive_ms = None
        self._stream_symbol_to_table.clear()
        self._stream_table_to_symbol.clear()
        self._stream_table_field_values.clear()
        if client is None:
            return
        for subscription in subscriptions:
            try:
                client.unsubscribe(subscription)
            except Exception:
                pass
        if listener is not None:
            try:
                client.removeListener(listener)
            except Exception:
                pass
        try:
            client.disconnect()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Thread start/stop
    # ------------------------------------------------------------------

    def _start_stream_thread_locked(self) -> None:
        if not self._stream_enabled:
            self._stream_last_error = "stream_disabled_by_config"
            return
        if self._stream_use_sdk:
            self._start_stream_sdk_locked()
            return
        thread = self._stream_thread
        if thread is not None and thread.is_alive():
            return
        if not self._stream_endpoint:
            self._stream_last_error = "lightstreamer_endpoint_missing"
            return

        self._stream_stop_event.clear()
        self._stream_thread = threading.Thread(
            target=self._stream_loop,
            name="ig-lightstreamer-stream",
            daemon=True,
        )
        self._stream_thread.start()

    def _stop_stream_thread_locked(self) -> threading.Thread | None:
        if self._stream_use_sdk:
            self._stop_stream_sdk_locked()
            return None
        self._stream_stop_event.set()
        self._mark_stream_disconnected_locked(None, schedule_retry=False)
        thread = self._stream_thread
        self._stream_thread = None
        if thread is threading.current_thread():
            return None
        return thread

    # ------------------------------------------------------------------
    # Control & subscription
    # ------------------------------------------------------------------

    def _send_stream_control(self, control_url: str, params: dict[str, Any]) -> tuple[bool, str]:
        req = request.Request(
            url=control_url,
            method="POST",
            data=self._form_body(params),
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "text/plain",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout_sec) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            return False, str(exc)

        lines = [line.strip() for line in payload.replace("\r", "\n").split("\n") if line.strip()]
        if not lines:
            return False, "empty_control_response"

        first = lines[0]
        if first == "OK":
            return True, "ok"
        if first == "SYNC ERROR":
            return False, "sync_error"
        if first == "ERROR":
            code = lines[1] if len(lines) > 1 else "unknown"
            message = lines[2] if len(lines) > 2 else "unknown"
            return False, f"error:{code}:{message}"
        return False, first

    def _subscribe_symbol(self, symbol: str) -> bool:
        from xtb_bot.ig_client import (
            LIGHTSTREAMER_PRICE_ADAPTER,
            LIGHTSTREAMER_PRICE_FIELDS,
            LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS,
        )

        if self._stream_use_sdk:
            with self._lock:
                sdk_client_ready = self._stream_sdk_client is not None
            if sdk_client_ready:
                return self._subscribe_symbol_sdk(symbol)
        upper = symbol.upper()

        with self._lock:
            if upper in self._stream_symbol_to_table:
                return True
            session_id = self._stream_session_id
            control_url = self._stream_control_url
            account_id = self.account_id
            if not session_id or not control_url or not account_id:
                return False
            table_id = self._stream_next_table_id
            self._stream_next_table_id += 1

        try:
            epic = self._epic_for_symbol(upper)
        except Exception as exc:
            with self._lock:
                self._stream_last_error = f"epic_resolution_failed:{upper}:{exc}"
            return False

        attempted_epics: set[str] = set()
        final_reason = "unknown"

        while True:
            normalized_epic = str(epic).strip().upper()
            if not normalized_epic or normalized_epic in attempted_epics:
                break
            attempted_epics.add(normalized_epic)

            item = f"PRICE:{account_id}:{normalized_epic}"
            # Lightstreamer field lists are space-separated (+ encoded in form body).
            schema = " ".join(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS)
            ok, reason = self._send_stream_control(
                control_url,
                {
                    "LS_session": session_id,
                    "LS_table": table_id,
                    "LS_op": "add",
                    "LS_data_adapter": LIGHTSTREAMER_PRICE_ADAPTER,
                    "LS_id": item,
                    "LS_schema": schema,
                    "LS_mode": "MERGE",
                    "LS_snapshot": "true",
                },
            )
            if ok:
                self._activate_epic(upper, normalized_epic)
                self._clear_stream_rest_fallback_block(upper)

                with self._lock:
                    if session_id != self._stream_session_id:
                        return False
                    self._stream_symbol_to_table[upper] = table_id
                    self._stream_table_to_symbol[table_id] = upper
                    self._stream_table_field_values[table_id] = [None] * len(LIGHTSTREAMER_PRICE_FIELDS)
                return True

            final_reason = str(reason or "unknown")
            if reason == "sync_error":
                with self._lock:
                    self._stream_last_error = f"subscription_failed:{upper}:{final_reason}"
                    self._mark_stream_disconnected_locked("stream_sync_error", schedule_retry=True)
                return False

            if self._is_stream_subscription_invalid_group_error_text(reason):
                next_epic = self._promote_stream_subscription_epic_after_invalid_group(
                    upper,
                    normalized_epic,
                    reason=f"stream_subscription:{final_reason}",
                )
                if next_epic:
                    return False

            break

        if self._is_stream_subscription_invalid_group_error_text(final_reason):
            self._mark_stream_rest_fallback_block(upper, reason=f"stream_subscription:{final_reason}")
        with self._lock:
            self._stream_last_error = f"subscription_failed:{upper}:{final_reason}"
        return False

    def _subscribe_symbol_sdk(self, symbol: str) -> bool:
        from xtb_bot.ig_client import (
            LIGHTSTREAMER_PRICE_ADAPTER,
            LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS,
            _IgLightstreamerSubscriptionListener,
            _LightstreamerSubscription,
        )

        if _LightstreamerSubscription is None:
            with self._lock:
                self._stream_last_error = "lightstreamer_sdk_not_installed"
            return False

        upper = str(symbol).upper()
        with self._lock:
            client = self._stream_sdk_client
            account_id = self.account_id
            if upper in self._stream_symbol_to_table:
                return True
            if upper in self._stream_sdk_pending_subscriptions:
                return True
            if upper in self._stream_sdk_subscriptions:
                return True
            if client is None or not account_id:
                return False
            if not self._stream_sdk_connected:
                return False

        try:
            epic = self._epic_for_symbol(upper)
        except Exception as exc:
            with self._lock:
                self._stream_last_error = f"epic_resolution_failed:{upper}:{exc}"
            return False

        item = f"PRICE:{account_id}:{epic}"
        try:
            subscription = _LightstreamerSubscription(
                "MERGE",
                [item],
                list(LIGHTSTREAMER_PRICE_SUBSCRIPTION_FIELDS),
            )
            subscription.setDataAdapter(LIGHTSTREAMER_PRICE_ADAPTER)
            subscription.setRequestedSnapshot("yes")
            listener = _IgLightstreamerSubscriptionListener(self, upper)
            subscription.addListener(listener)
            with self._lock:
                self._stream_sdk_subscriptions[upper] = subscription
                self._stream_sdk_subscription_listeners[upper] = listener
                self._stream_sdk_pending_subscriptions.add(upper)
            client.subscribe(subscription)
            return True
        except Exception as exc:
            with self._lock:
                self._stream_sdk_pending_subscriptions.discard(upper)
                self._stream_sdk_subscriptions.pop(upper, None)
                self._stream_sdk_subscription_listeners.pop(upper, None)
                self._stream_last_error = f"subscription_failed:{upper}:{exc}"
            return False

    def _ensure_stream_subscription(self, symbol: str) -> bool:
        if not self._stream_enabled:
            return False
        upper = symbol.upper()
        block_remaining = self._stream_rest_fallback_block_remaining(upper)
        retry_remaining = self._stream_subscription_retry_remaining(upper)
        with self._lock:
            self._stream_desired_subscriptions.add(upper)
            already_subscribed = upper in self._stream_symbol_to_table
            if self._stream_use_sdk:
                stream_ready = self._stream_sdk_client is not None
            else:
                stream_ready = bool(self._stream_session_id and self._stream_control_url)
        if already_subscribed:
            return True
        if block_remaining > 0:
            return False
        if retry_remaining > 0:
            return False
        if not stream_ready:
            return False
        return self._subscribe_symbol(upper)

    def _resubscribe_desired_symbols(self) -> None:
        with self._lock:
            desired = sorted(self._stream_desired_subscriptions)
        for symbol in desired:
            self._ensure_stream_subscription(symbol)

    # ------------------------------------------------------------------
    # Tick handler
    # ------------------------------------------------------------------

    def set_stream_tick_handler(self, handler: Callable[[PriceTick], None] | None) -> None:
        with self._lock:
            self._stream_tick_handler = handler if callable(handler) else None

    def _emit_stream_tick(self, tick: PriceTick) -> None:
        with self._lock:
            handler = self._stream_tick_handler
        if handler is None:
            return
        try:
            handler(self._clone_tick(tick))
        except Exception:
            logger.debug("Stream tick handler failed", exc_info=True)

    # ------------------------------------------------------------------
    # Tick construction from stream fields
    # ------------------------------------------------------------------

    def _update_tick_from_stream_fields(self, symbol: str, field_values: list[str | None]) -> None:
        from xtb_bot.ig_client import LIGHTSTREAMER_PRICE_FIELDS, _as_float

        field_map: dict[str, str | None] = {}
        for idx, name in enumerate(LIGHTSTREAMER_PRICE_FIELDS):
            field_map[name] = field_values[idx] if idx < len(field_values) else None

        def _first_positive_field(*names: str) -> float:
            for name in names:
                parsed = _as_float(field_map.get(name), 0.0)
                if parsed > 0:
                    return parsed
            return 0.0

        bid = _first_positive_field("BIDPRICE1", "BID")
        ask = _first_positive_field("ASKPRICE1", "OFR", "OFFER")
        timestamp_raw: str | None = None
        for key in ("TIMESTAMP", "UTM", "UPDATE_TIME"):
            raw = field_map.get(key)
            if raw not in (None, ""):
                timestamp_raw = raw
                break

        if bid <= 0 and ask <= 0:
            return
        if bid <= 0:
            bid = ask
        if ask <= 0:
            ask = bid

        now_ts = time.time()
        tick_ts = self._parse_ts({"updateTimeUTC": timestamp_raw}, now_ts)

        tick = PriceTick(
            symbol=symbol,
            bid=bid,
            ask=ask,
            timestamp=tick_ts,
            received_at=now_ts,
        )
        with self._lock:
            previous_tick = self._tick_cache.get(symbol)
            quote_changed = (
                previous_tick is None
                or abs(float(previous_tick.bid) - bid) > FLOAT_ROUNDING_TOLERANCE
                or abs(float(previous_tick.ask) - ask) > FLOAT_ROUNDING_TOLERANCE
            )
            if quote_changed or symbol not in self._stream_last_quote_change_ts_by_symbol:
                self._stream_last_quote_change_ts_by_symbol[symbol] = now_ts
            self._tick_cache[symbol] = tick
            self._last_tick_by_symbol[symbol] = tick_ts
        self._emit_stream_tick(tick)

    def _is_stream_quote_stagnant_locked(self, symbol: str, now_ts: float) -> bool:
        max_age = float(self._stream_stagnant_quote_max_age_sec)
        if max_age <= 0:
            return False
        changed_at = self._stream_last_quote_change_ts_by_symbol.get(symbol)
        if changed_at is None:
            return False
        return (now_ts - changed_at) > max_age

    # ------------------------------------------------------------------
    # Line / table-update parsing
    # ------------------------------------------------------------------

    def _handle_lightstreamer_table_update(self, table_id: int, raw_values: str) -> bool:
        from xtb_bot.ig_client import LIGHTSTREAMER_PRICE_FIELDS

        with self._lock:
            symbol = self._stream_table_to_symbol.get(table_id)
            previous_values = list(
                self._stream_table_field_values.get(table_id, [None] * len(LIGHTSTREAMER_PRICE_FIELDS))
            )

        if symbol is None:
            return True

        encoded_values = raw_values.split("|")
        decoded: list[str | None] = []
        for idx in range(len(LIGHTSTREAMER_PRICE_FIELDS)):
            prev = previous_values[idx] if idx < len(previous_values) else None
            encoded = encoded_values[idx] if idx < len(encoded_values) else ""
            decoded.append(self._decode_stream_field(encoded, prev))

        with self._lock:
            self._stream_table_field_values[table_id] = decoded

        self._update_tick_from_stream_fields(symbol, decoded)
        return True

    def _handle_stream_line(self, line: str) -> bool:
        if line == "" or line == "PROBE":
            return True

        if line.startswith("Preamble"):
            return True

        if line.startswith(("ERROR", "END", "LOOP", "SYNC ERROR")):
            logger.warning("IG Lightstreamer control event: %s", line)
            return False

        if line.startswith("U,"):
            # Some servers may emit TLCP-style updates; try to read table id and pipe payload.
            parts = line.split(",", 3)
            if len(parts) >= 4:
                try:
                    table_id = int(parts[1])
                except ValueError:
                    return True
                return self._handle_lightstreamer_table_update(table_id, parts[3])
            return True

        if "|" in line and "," in line:
            prefix, values = line.split("|", 1)
            prefix_parts = prefix.split(",")
            if len(prefix_parts) >= 2 and prefix_parts[0].isdigit() and prefix_parts[1].isdigit():
                return self._handle_lightstreamer_table_update(int(prefix_parts[0]), values)
        elif "|" in line:
            # Some servers emit table updates as "<table>|<values>" in MERGE mode.
            prefix, values = line.split("|", 1)
            if prefix.isdigit():
                return self._handle_lightstreamer_table_update(int(prefix), values)

        if ",EOS" in line or ",OV" in line:
            return True

        return True

    # ------------------------------------------------------------------
    # Session creation and main stream loop
    # ------------------------------------------------------------------

    def _open_stream_session(self) -> Any:
        from xtb_bot.ig_client import LIGHTSTREAMER_ADAPTER_SET, LIGHTSTREAMER_CID

        with self._lock:
            stream_endpoint = self._stream_endpoint
            account_id = self.account_id
            cst = self._cst
            security_token = self._security_token

        if not stream_endpoint:
            raise BrokerError("lightstreamer_endpoint_missing")
        if not account_id:
            raise BrokerError("lightstreamer_account_id_missing")
        if not cst or not security_token:
            raise BrokerError("lightstreamer_auth_tokens_missing")

        create_url = f"{stream_endpoint}/create_session.txt"
        body = self._form_body(
            {
                "LS_op2": "create",
                "LS_cid": LIGHTSTREAMER_CID,
                "LS_adapter_set": LIGHTSTREAMER_ADAPTER_SET,
                "LS_user": account_id,
                "LS_password": f"CST-{cst}|XST-{security_token}",
                "LS_keepalive_millis": 5000,
                "LS_content_length": 50000000,
            }
        )

        req = request.Request(
            url=create_url,
            method="POST",
            data=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "text/plain",
            },
        )

        timeout = max(self.stream_read_timeout_sec, self.timeout_sec)
        return request.urlopen(req, timeout=timeout)

    _STREAM_MAX_RECONNECT_ATTEMPTS = 50

    def _stream_loop(self) -> None:
        while not self._stream_stop_event.is_set():
            with self._lock:
                if not self._connected:
                    return
                retry_at = self._stream_next_retry_at
                reconnect_attempts = self._stream_reconnect_attempts

            # Circuit breaker: stop reconnecting after too many consecutive failures
            if reconnect_attempts >= self._STREAM_MAX_RECONNECT_ATTEMPTS:
                logger.error(
                    "IG Lightstreamer circuit breaker: %d consecutive reconnect failures, "
                    "stopping stream loop. REST fallback remains active.",
                    reconnect_attempts,
                )
                return

            now = time.time()
            if retry_at is not None and now < retry_at:
                self._stream_stop_event.wait(max(0.1, retry_at - now))
                continue

            stream_response: Any | None = None
            try:
                stream_response = self._open_stream_session()
                first_line = self._read_stream_line(stream_response)
                if first_line != "OK":
                    raise BrokerError(f"stream_create_failed:{first_line}")

                session_id: str | None = None
                control_address: str | None = None
                keepalive_ms: int | None = None

                while not self._stream_stop_event.is_set():
                    header_line = self._read_stream_line(stream_response)
                    if header_line is None:
                        raise BrokerError("stream_header_unexpected_eof")
                    if header_line == "":
                        break
                    if ":" not in header_line:
                        continue
                    key, value = header_line.split(":", 1)
                    key = key.strip()
                    value = value.strip()
                    if key == "SessionId":
                        session_id = value
                    elif key == "ControlAddress":
                        control_address = value
                    elif key == "KeepaliveMillis":
                        try:
                            keepalive_ms = int(value)
                        except ValueError:
                            keepalive_ms = None

                if not session_id:
                    raise BrokerError("stream_session_id_missing")

                control_url = self._resolve_stream_control_url(control_address)
                with self._lock:
                    self._stream_http_response = stream_response
                    self._mark_stream_connected_locked(session_id, control_url, keepalive_ms)

                self._resubscribe_desired_symbols()

                while not self._stream_stop_event.is_set():
                    line = self._read_stream_line(stream_response)
                    if line is None:
                        raise BrokerError("stream_eof")
                    handled = self._handle_stream_line(line)
                    if handled:
                        continue
                    raise BrokerError(f"stream_control_message:{line}")

            except Exception as exc:
                if self._stream_stop_event.is_set():
                    break
                with self._lock:
                    self._mark_stream_disconnected_locked(str(exc), schedule_retry=True)
                    retry_in = (
                        max(0.1, (self._stream_next_retry_at or time.time()) - time.time())
                        if self._stream_next_retry_at is not None
                        else self._stream_backoff_base_sec
                    )
                logger.warning("IG Lightstreamer degraded, switching to REST fallback: %s", exc)
                self._stream_stop_event.wait(retry_in)
            finally:
                if stream_response is not None:
                    try:
                        stream_response.close()
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Public health status
    # ------------------------------------------------------------------

    def get_stream_health_status(
        self,
        symbol: str | None,
        max_tick_age_sec: float,
    ) -> StreamHealthStatus:
        if not self._connected:
            return StreamHealthStatus(
                healthy=False,
                connected=False,
                reason="broker_not_connected",
                symbol=symbol.upper() if symbol else None,
            )

        now = time.time()
        upper = symbol.upper() if symbol else None

        with self._lock:
            stream_thread = self._stream_thread
            stream_enabled = self._stream_enabled
            legacy_stream_connected = (
                self._stream_session_id is not None
                and stream_thread is not None
                and stream_thread.is_alive()
            )
            sdk_stream_connected = bool(self._stream_use_sdk and self._stream_sdk_client and self._stream_sdk_connected)
            stream_connected = legacy_stream_connected or sdk_stream_connected

            last_tick_age_sec: float | None = None
            quote_stagnation_age_sec: float | None = None
            if upper:
                tick = self._tick_cache.get(upper)
                if tick is not None:
                    last_tick_age_sec = max(0.0, now - tick.timestamp)
                changed_at = self._stream_last_quote_change_ts_by_symbol.get(upper)
                if changed_at is not None:
                    quote_stagnation_age_sec = max(0.0, now - changed_at)
                symbol_subscription_active = upper in self._stream_symbol_to_table
            elif self._tick_cache:
                freshest_ts = max(
                    float(getattr(item, "received_at", item.timestamp) or item.timestamp)
                    for item in self._tick_cache.values()
                )
                last_tick_age_sec = max(0.0, now - freshest_ts)
                symbol_subscription_active = False
            else:
                symbol_subscription_active = False

            next_retry_in_sec = None
            if self._stream_next_retry_at is not None:
                next_retry_in_sec = max(0.0, self._stream_next_retry_at - now)

            desired_subscriptions = len(self._stream_desired_subscriptions)
            active_subscriptions = len(self._stream_symbol_to_table)
            last_error = self._stream_last_error
            reconnect_attempts = self._stream_reconnect_attempts
            total_reconnects = self._stream_total_reconnects
            last_disconnect_ts = self._stream_last_disconnect_ts
            last_reconnect_ts = self._stream_last_reconnect_ts
            stream_endpoint_configured = self._stream_endpoint is not None
            price_requests_total = self._price_requests_total
            stream_hits_total = self._stream_price_hits_total
            rest_fallback_hits_total = self._rest_fallback_hits_total

        stream_hit_rate_pct: float | None = None
        if price_requests_total > 0:
            stream_hit_rate_pct = (stream_hits_total / price_requests_total) * 100.0

        if not stream_enabled:
            reason = "stream_disabled_rest_only"
        elif stream_connected:
            if upper and upper in self._stream_desired_subscriptions:
                if not symbol_subscription_active:
                    if last_error and "subscription_failed:" in str(last_error):
                        reason = f"stream_subscription_failed_rest_fallback:{last_error}"
                    else:
                        reason = "stream_subscription_pending_rest_fallback"
                elif last_tick_age_sec is None:
                    reason = "stream_warmup_rest_fallback"
                elif last_tick_age_sec > max(0.1, max_tick_age_sec):
                    reason = f"stream_tick_stale_rest_fallback:{last_tick_age_sec:.2f}s>{max_tick_age_sec:.2f}s"
                elif (
                    self._stream_stagnant_quote_max_age_sec > 0
                    and quote_stagnation_age_sec is not None
                    and quote_stagnation_age_sec > self._stream_stagnant_quote_max_age_sec
                ):
                    reason = (
                        "stream_quote_stagnant_rest_fallback:"
                        f"{quote_stagnation_age_sec:.2f}s>{self._stream_stagnant_quote_max_age_sec:.2f}s"
                    )
                else:
                    reason = "ok"
            else:
                reason = "ok"
        else:
            if self._stream_sdk_enabled and not self._stream_sdk_available:
                reason = "lightstreamer_sdk_missing_rest_fallback"
            elif not stream_endpoint_configured:
                reason = "stream_endpoint_missing_rest_fallback"
            elif last_error:
                reason = f"stream_disconnected_rest_fallback:{last_error}"
            else:
                reason = "stream_disconnected_rest_fallback"

        # IG client keeps REST fallback active; stream degradation should not hard-block openings.
        return StreamHealthStatus(
            healthy=True,
            connected=stream_connected,
            reason=reason,
            symbol=upper,
            last_tick_age_sec=last_tick_age_sec,
            reconnect_attempts=reconnect_attempts,
            total_reconnects=total_reconnects,
            last_disconnect_ts=last_disconnect_ts,
            last_reconnect_ts=last_reconnect_ts,
            next_retry_in_sec=next_retry_in_sec,
            desired_subscriptions=desired_subscriptions,
            active_subscriptions=active_subscriptions,
            last_error=last_error,
            price_requests_total=price_requests_total,
            stream_hits_total=stream_hits_total,
            rest_fallback_hits_total=rest_fallback_hits_total,
            stream_hit_rate_pct=stream_hit_rate_pct,
        )
