from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from xtb_bot.ig_client import IgApiClient

from xtb_bot.client import BrokerError

logger = logging.getLogger(__name__)


def _as_mapping(value: Any) -> dict[str, Any]:
    """Import-free fallback; the real implementation lives in ig_client."""
    return value if isinstance(value, dict) else {}


class IgAuthMixin:
    """Authentication & session management methods for IgApiClient."""

    __slots__ = ()

    # ------------------------------------------------------------------
    # Header construction
    # ------------------------------------------------------------------

    def _auth_headers(self, include_session_tokens: bool = True) -> dict[str, str]:
        headers = {
            "Accept": "application/json; charset=UTF-8",
            "Content-Type": "application/json; charset=UTF-8",
            "X-IG-API-KEY": self.api_key,
        }
        if include_session_tokens and self._cst:
            headers["CST"] = self._cst
        if include_session_tokens and self._security_token:
            headers["X-SECURITY-TOKEN"] = self._security_token
        return headers

    # ------------------------------------------------------------------
    # Token extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_session_tokens(
        headers: dict[str, str],
        body: dict[str, Any] | None,
    ) -> tuple[str | None, str | None]:
        normalized_headers = {str(key).strip().lower(): str(value) for key, value in headers.items()}
        cst = normalized_headers.get("cst")
        security = normalized_headers.get("x-security-token")

        payload = body if isinstance(body, dict) else {}
        if not cst:
            for key in ("cst", "CST", "clientSessionToken"):
                raw = payload.get(key)
                if raw not in (None, ""):
                    cst = str(raw)
                    break
        if not security:
            for key in ("x-security-token", "X-SECURITY-TOKEN", "securityToken", "xst"):
                raw = payload.get(key)
                if raw not in (None, ""):
                    security = str(raw)
                    break

        return cst, security

    # ------------------------------------------------------------------
    # Auth-error detection
    # ------------------------------------------------------------------

    @staticmethod
    def _is_auth_token_invalid_error_text(text: str) -> bool:
        lowered = str(text or "").lower()
        markers = (
            "error.security.client-token-invalid",
            "error.security.client-token-missing",
            "error.security.account-token-invalid",
            "error.security.account-token-missing",
            "error.public-api.failure.missing.credentials",
        )
        return any(marker in lowered for marker in markers)

    @staticmethod
    def _is_invalid_client_security_token_error_text(text: str) -> bool:
        lowered = str(text).lower()
        return "invalid-client-security-token" in lowered

    # ------------------------------------------------------------------
    # Auth state management
    # ------------------------------------------------------------------

    def _api_key_tail(self) -> str:
        key = str(self.api_key or "").strip()
        if not key:
            return "empty"
        if len(key) <= 4:
            return key
        return key[-4:]

    def _reset_auth_state_for_login(self) -> None:
        with self._lock:
            self._connected = False
            self._cst = None
            self._security_token = None
            self._connectivity_status_cache = None
            self._connectivity_status_cache_ts = 0.0

    # ------------------------------------------------------------------
    # Session refresh
    # ------------------------------------------------------------------

    def _maybe_refresh_auth_session_after_failure(
        self,
        *,
        method: str,
        path: str,
        error_text: str,
    ) -> bool:
        normalized_path = str(path).split("?", 1)[0]
        if normalized_path.startswith("/session"):
            return False

        acquired = self._auth_refresh_lock.acquire(timeout=max(1.0, self.timeout_sec))
        if not acquired:
            return False
        try:
            now_ts = time.time()
            # Avoid reconnect storm across worker threads.
            if (now_ts - self._auth_refresh_last_attempt_ts) < self._auth_refresh_min_interval_sec:
                return (now_ts - self._auth_refresh_last_success_ts) < self._auth_refresh_min_interval_sec
            self._auth_refresh_last_attempt_ts = now_ts
            logger.warning(
                "IG auth tokens invalid during %s %s, attempting session refresh",
                str(method).upper(),
                normalized_path,
            )
            original_connect_retry_attempts = self.connect_retry_attempts
            try:
                # Keep auth recovery bounded to a single login cycle; repeated
                # POST /session retries under load tend to amplify allowance storms.
                self.connect_retry_attempts = 1
                self.connect()
            except Exception as refresh_exc:
                logger.warning(
                    "IG session refresh failed on %s %s: %s | original_error=%s",
                    str(method).upper(),
                    normalized_path,
                    refresh_exc,
                    error_text,
                )
                return False
            finally:
                self.connect_retry_attempts = original_connect_retry_attempts
            self._auth_refresh_last_success_ts = time.time()
            return True
        finally:
            self._auth_refresh_lock.release()

    # ------------------------------------------------------------------
    # connect / close
    # ------------------------------------------------------------------

    def connect(self) -> None:
        # Ensure login starts from a clean auth state.
        self._reset_auth_state_for_login()

        payload = {"identifier": self.identifier, "password": self.password}
        transient_delay_sec = self.connect_retry_base_sec
        for attempt in range(1, self.connect_retry_attempts + 1):
            self._reset_auth_state_for_login()
            try:
                login_version = "3"
                try:
                    body_raw, headers = self._request(
                        "POST", "/session", payload=payload, version=login_version, auth=False
                    )
                    body = _as_mapping(body_raw)
                except BrokerError as exc:
                    text = str(exc).lower()
                    if not self._is_invalid_client_security_token_error_text(text):
                        raise
                    # Some IG environments intermittently reject v3 login with this error.
                    # Retry once with v2 before giving up.
                    login_version = "2"
                    body_raw, headers = self._request(
                        "POST", "/session", payload=payload, version=login_version, auth=False
                    )
                    body = _as_mapping(body_raw)

                cst, security = self._extract_session_tokens(headers, body)
                if login_version == "3" and (not cst or not security):
                    # Some IG accounts return session headers only for v2 login.
                    try:
                        body_v2_raw, headers_v2 = self._request(
                            "POST",
                            "/session",
                            payload=payload,
                            version="2",
                            auth=False,
                        )
                    except BrokerError as exc:
                        text = str(exc).lower()
                        if self._is_invalid_client_security_token_error_text(text):
                            raise BrokerError(
                                f"IG login v2 fallback failed: {exc}"
                            ) from exc
                        raise
                    body_v2 = _as_mapping(body_v2_raw)
                    cst_v2, security_v2 = self._extract_session_tokens(headers_v2, body_v2)
                    if cst_v2 and security_v2:
                        cst = cst_v2
                        security = security_v2
                        if not body.get("currentAccountId"):
                            body["currentAccountId"] = body_v2.get("currentAccountId")
                        if not body.get("lightstreamerEndpoint"):
                            body["lightstreamerEndpoint"] = body_v2.get("lightstreamerEndpoint")
                if not cst or not security:
                    raise BrokerError("IG login succeeded but session tokens are missing")

                explicit_account_id = str(self.account_id).strip() if self.account_id not in (None, "") else None
                current_account_id = str(body.get("currentAccountId") or "").strip() or None
                resolved_account_id = explicit_account_id or current_account_id

                with self._lock:
                    self._cst = cst
                    self._security_token = security
                    self._connected = True
                    self.account_id = resolved_account_id
                    self._stream_endpoint = self._normalize_lightstreamer_endpoint(body.get("lightstreamerEndpoint"))
                    if self._stream_enabled and self._stream_endpoint is None:
                        self._stream_last_error = "lightstreamer_endpoint_missing"

                should_switch_account = bool(
                    explicit_account_id and (current_account_id is None or explicit_account_id != current_account_id)
                )
                if should_switch_account and explicit_account_id:
                    try:
                        self._request(
                            "PUT",
                            "/session",
                            payload={"accountId": explicit_account_id, "defaultAccount": True},
                            version="1",
                            auth=True,
                        )
                    except Exception as exc:
                        logger.warning("IG account switch failed for account_id=%s: %s", explicit_account_id, exc)

                with self._lock:
                    self._start_stream_thread_locked()
                self._reset_allowance_cooldown()
                return
            except BrokerError as exc:
                error_text = str(exc)
                lowered = error_text.lower()
                if attempt >= self.connect_retry_attempts:
                    if self._is_invalid_client_security_token_error_text(lowered):
                        endpoint = str(self._endpoint or "")
                        account_type = self.account_type.value
                        raise BrokerError(
                            f"{error_text} | hint: login rejected with invalid-client-security-token; "
                            "verify IG credentials and that endpoint matches account type "
                            "(demo->demo-api, live->api). If you use .env, it now overrides shell env "
                            "by default; set BOT_DOTENV_PREFER_ENV=1 to keep existing shell values. "
                            f"account_type={account_type} endpoint={endpoint} api_key_tail={self._api_key_tail()}"
                        ) from exc
                    raise

                delay_sec = 0.0
                if self._is_allowance_error_text(lowered):
                    delay_sec = self._start_allowance_cooldown(error_text)
                    logger.warning(
                        "IG login allowance exceeded on connect attempt %s/%s; retrying in %.1fs",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                    )
                elif self._is_invalid_client_security_token_error_text(lowered):
                    delay_sec = min(self.connect_retry_max_sec, transient_delay_sec)
                    transient_delay_sec = min(self.connect_retry_max_sec, transient_delay_sec * 2.0)
                    logger.warning(
                        "IG login invalid-client-security-token on attempt %s/%s; retrying in %.1fs",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                    )
                elif any(
                    token in lowered
                    for token in (
                        "timed out",
                        "timeout",
                        "temporarily unavailable",
                        "service unavailable",
                        "bad gateway",
                        "gateway timeout",
                        "connection reset",
                        "connection refused",
                        "network is unreachable",
                    )
                ):
                    delay_sec = min(self.connect_retry_max_sec, transient_delay_sec)
                    transient_delay_sec = min(self.connect_retry_max_sec, transient_delay_sec * 2.0)
                    logger.warning(
                        "IG connect transient error on attempt %s/%s; retrying in %.1fs: %s",
                        attempt,
                        self.connect_retry_attempts,
                        delay_sec,
                        error_text,
                    )
                else:
                    raise
                time.sleep(max(0.1, delay_sec))

    def close(self) -> None:
        stream_thread_to_join: "threading.Thread | None" = None
        request_threads_to_join: "list[threading.Thread]" = []
        with self._lock:
            stream_thread_to_join = self._stop_stream_thread_locked()

        if stream_thread_to_join is not None and stream_thread_to_join.is_alive():
            stream_thread_to_join.join(timeout=2.0)

        try:
            if self._connected:
                self._request("DELETE", "/session", version="1", auth=True)
        except Exception:
            pass
        finally:
            with self._lock:
                request_threads_to_join = self._stop_request_workers_locked()
                self._connected = False
                self._cst = None
                self._security_token = None
                self._tick_cache.clear()
                self._last_tick_by_symbol.clear()
                self._stream_desired_subscriptions.clear()
                self._stream_last_error = None
                self._stream_endpoint = None
                self._account_snapshot_cache = None
                self._account_snapshot_cached_at = 0.0
                self._connectivity_status_cache = None
                self._connectivity_status_cache_ts = 0.0
                self._position_open_sync.clear()
                self._position_close_sync.clear()
                self._epic_unavailable_until_ts_by_symbol.clear()
                self._last_stream_rest_fallback_ts_by_symbol.clear()
                self._request_worker_thread_ids.clear()
                self._historical_http_cache.clear()
                self._stream_sdk_client = None
                self._stream_sdk_client_listener = None
                self._stream_sdk_subscriptions.clear()
                self._stream_sdk_subscription_listeners.clear()
                self._stream_sdk_pending_subscriptions.clear()
                self._stream_sdk_connected = False
                self._stream_sdk_status = None
                self._stream_tick_handler = None

        import threading
        for thread in request_threads_to_join:
            if thread.is_alive():
                thread.join(timeout=2.0)
