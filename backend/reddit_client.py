from __future__ import annotations

import json
import os
import random
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar
from urllib.error import HTTPError

from backend.reddit_dev_only_config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_REQUEST_STATE_PATH,
    REDDIT_USER_AGENT,
)

try:
    import praw
except ImportError:  # pragma: no cover - handled at runtime
    praw = None

try:  # pragma: no cover - optional dependency detail
    import prawcore
except ImportError:  # pragma: no cover - handled at runtime
    prawcore = None

_REDDIT_CLIENT = None
_T = TypeVar("_T")
_REQUEST_CONTEXT = threading.local()

_REQUEST_LOCK_PATH = Path(f"{REDDIT_REQUEST_STATE_PATH}.lock")
_REQUEST_LOCK_STALE_SECONDS = 5 * 60
_REQUEST_LOCK_TIMEOUT_SECONDS = 90.0
_MIN_REQUEST_INTERVAL_SECONDS = 2.0
_AUTH_FAILURE_BACKOFF_SECONDS = 15 * 60
_RATE_LIMIT_BACKOFF_SECONDS = 10 * 60
_BLOCK_BACKOFF_SECONDS = 30 * 60
_TEMP_FAILURE_BACKOFF_SECONDS = 5 * 60


class RedditBackoffActiveError(RuntimeError):
    def __init__(self, reason: str, retry_after_seconds: float) -> None:
        super().__init__(
            f"reddit request backoff active: {reason} "
            f"(retry after {max(1.0, retry_after_seconds):.1f}s)"
        )
        self.reason = reason
        self.retry_after_seconds = retry_after_seconds


class RedditBlockedResponseError(RuntimeError):
    pass


def _default_runtime_state() -> dict[str, Any]:
    return {
        "version": 1,
        "requestCount": 0,
        "successCount": 0,
        "failureCount": 0,
        "cooldownSkipCount": 0,
        "prawRequestCount": 0,
        "publicJsonRequestCount": 0,
        "tokenFailureCount": 0,
        "rateLimitCount": 0,
        "blockCount": 0,
        "temporaryFailureCount": 0,
        "lastOperation": None,
        "lastTransport": None,
        "lastRequestAt": None,
        "lastSuccessAt": None,
        "lastFailureAt": None,
        "lastFailureReason": None,
        "lastError": None,
        "lastBackoffReason": None,
        "nextAllowedAt": None,
        "authBackoffUntil": None,
        "lastTokenFailureAt": None,
        "lastRateLimitAt": None,
        "lastBlockAt": None,
        "lastTemporaryFailureAt": None,
        "lastClientCreatedAt": None,
        "clientCreateCount": 0,
    }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _seconds_until(value: str | None, *, reference_time: datetime | None = None) -> float:
    parsed = _parse_iso(value)
    if parsed is None:
        return 0.0
    reference = reference_time or _now()
    return max(0.0, (parsed - reference).total_seconds())


def _with_jitter(seconds: float) -> float:
    return seconds + random.uniform(0.0, max(1.0, seconds * 0.2))


def _normalize_runtime_state(state: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = {**_default_runtime_state(), **dict(state or {})}
    for key in (
        "requestCount",
        "successCount",
        "failureCount",
        "cooldownSkipCount",
        "prawRequestCount",
        "publicJsonRequestCount",
        "tokenFailureCount",
        "rateLimitCount",
        "blockCount",
        "temporaryFailureCount",
        "clientCreateCount",
    ):
        normalized[key] = int(normalized.get(key, 0) or 0)
    return normalized


def _read_runtime_state() -> dict[str, Any]:
    try:
        return _normalize_runtime_state(
            json.loads(REDDIT_REQUEST_STATE_PATH.read_text(encoding="utf-8"))
        )
    except Exception:
        return _default_runtime_state()


def _write_runtime_state(state: dict[str, Any]) -> None:
    REDDIT_REQUEST_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    REDDIT_REQUEST_STATE_PATH.write_text(
        json.dumps(_normalize_runtime_state(state), indent=2),
        encoding="utf-8",
    )


@contextmanager
def _request_lock(timeout_seconds: float = _REQUEST_LOCK_TIMEOUT_SECONDS):
    start = time.monotonic()
    while True:
        try:
            fd = os.open(str(_REQUEST_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(
                fd,
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "createdAt": _now().isoformat(),
                    }
                ).encode("utf-8"),
            )
            os.close(fd)
            break
        except FileExistsError:
            try:
                payload = json.loads(_REQUEST_LOCK_PATH.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            created_at = _parse_iso(payload.get("createdAt"))
            if created_at is None or (_now() - created_at).total_seconds() > _REQUEST_LOCK_STALE_SECONDS:
                try:
                    _REQUEST_LOCK_PATH.unlink()
                except FileNotFoundError:
                    pass
                continue
            if time.monotonic() - start > timeout_seconds:
                raise RuntimeError("timed out waiting for reddit request lock")
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            _REQUEST_LOCK_PATH.unlink()
        except FileNotFoundError:
            pass


def _classify_error(error: BaseException) -> str:
    if isinstance(error, RedditBackoffActiveError):
        return "backoff"
    if isinstance(error, RedditBlockedResponseError):
        return "blocked"
    if isinstance(error, HTTPError):
        if error.code == 401:
            return "auth"
        if error.code == 429:
            return "rate_limit"
        if error.code == 403:
            return "blocked"
        if error.code >= 500:
            return "temporary"
    if prawcore is not None:
        if isinstance(error, getattr(prawcore.exceptions, "TooManyRequests", tuple())):
            return "rate_limit"
        if isinstance(error, getattr(prawcore.exceptions, "Forbidden", tuple())):
            return "blocked"
        if isinstance(error, getattr(prawcore.exceptions, "ServerError", tuple())):
            return "temporary"
        if isinstance(error, getattr(prawcore.exceptions, "RequestException", tuple())):
            return "temporary"
        if isinstance(error, getattr(prawcore.exceptions, "ResponseException", tuple())):
            message = str(error)
            if "401" in message:
                return "auth"
            if "429" in message or "Too Many Requests" in message:
                return "rate_limit"
            if "403" in message:
                return "blocked"
    message = str(error).lower()
    if "401" in message or "unauthorized" in message:
        return "auth"
    if "429" in message or "too many requests" in message or "ratelimit" in message:
        return "rate_limit"
    if "block" in message or "forbidden" in message or "whoa there" in message:
        return "blocked"
    return "temporary"


def _backoff_seconds(reason: str) -> float:
    if reason == "auth":
        return _with_jitter(_AUTH_FAILURE_BACKOFF_SECONDS)
    if reason == "rate_limit":
        return _with_jitter(_RATE_LIMIT_BACKOFF_SECONDS)
    if reason == "blocked":
        return _with_jitter(_BLOCK_BACKOFF_SECONDS)
    return _with_jitter(_TEMP_FAILURE_BACKOFF_SECONDS)


def _increment_transport_count(state: dict[str, Any], transport: str) -> None:
    if transport == "praw":
        state["prawRequestCount"] = int(state.get("prawRequestCount", 0) or 0) + 1
    elif transport == "public-json":
        state["publicJsonRequestCount"] = int(state.get("publicJsonRequestCount", 0) or 0) + 1


def get_reddit_runtime_state() -> dict[str, Any]:
    state = _read_runtime_state()
    reference_time = _now()
    next_allowed_seconds = _seconds_until(state.get("nextAllowedAt"), reference_time=reference_time)
    auth_backoff_seconds = _seconds_until(state.get("authBackoffUntil"), reference_time=reference_time)
    return {
        **state,
        "nextAllowedInSeconds": round(next_allowed_seconds, 1),
        "authBackoffInSeconds": round(auth_backoff_seconds, 1),
        "cooldownActive": next_allowed_seconds > 0,
        "authBackoffActive": auth_backoff_seconds > 0,
    }


def get_reddit_backoff_wait_seconds(*, transport: str | None = None) -> int:
    state = get_reddit_runtime_state()
    wait_seconds = float(state.get("nextAllowedInSeconds", 0.0) or 0.0)
    if transport == "praw":
        wait_seconds = max(wait_seconds, float(state.get("authBackoffInSeconds", 0.0) or 0.0))
    return int(wait_seconds + 0.999) if wait_seconds > 0 else 0


def public_json_fallback_allowed() -> bool:
    return not has_reddit_credentials()


def _invalidate_cached_client() -> None:
    global _REDDIT_CLIENT
    _REDDIT_CLIENT = None


def perform_reddit_request(
    operation: str,
    transport: str,
    func: Callable[[], _T],
) -> _T:
    if int(getattr(_REQUEST_CONTEXT, "depth", 0) or 0) > 0:
        return func()

    with _request_lock():
        reference_time = _now()
        state = _read_runtime_state()
        global_wait_seconds = _seconds_until(state.get("nextAllowedAt"), reference_time=reference_time)
        auth_wait_seconds = (
            _seconds_until(state.get("authBackoffUntil"), reference_time=reference_time)
            if transport == "praw"
            else 0.0
        )
        wait_seconds = max(global_wait_seconds, auth_wait_seconds)
        if wait_seconds > 0:
            state["cooldownSkipCount"] = int(state.get("cooldownSkipCount", 0) or 0) + 1
            _write_runtime_state(state)
            raise RedditBackoffActiveError(
                "auth" if auth_wait_seconds >= global_wait_seconds and auth_wait_seconds > 0 else "global",
                wait_seconds,
            )

        start_time = _now().isoformat()
        state["requestCount"] = int(state.get("requestCount", 0) or 0) + 1
        state["lastOperation"] = operation
        state["lastTransport"] = transport
        state["lastRequestAt"] = start_time
        _increment_transport_count(state, transport)
        _write_runtime_state(state)

        try:
            _REQUEST_CONTEXT.depth = int(getattr(_REQUEST_CONTEXT, "depth", 0) or 0) + 1
            result = func()
        except BaseException as error:
            failure_reason = _classify_error(error)
            failure_time = _now()
            state = _read_runtime_state()
            state["failureCount"] = int(state.get("failureCount", 0) or 0) + 1
            state["lastFailureAt"] = failure_time.isoformat()
            state["lastFailureReason"] = failure_reason
            state["lastBackoffReason"] = failure_reason
            state["lastError"] = str(error)
            if failure_reason == "auth":
                state["tokenFailureCount"] = int(state.get("tokenFailureCount", 0) or 0) + 1
                state["lastTokenFailureAt"] = failure_time.isoformat()
                state["authBackoffUntil"] = (
                    failure_time.timestamp() + _backoff_seconds("auth")
                )
                state["authBackoffUntil"] = datetime.fromtimestamp(
                    float(state["authBackoffUntil"]),
                    tz=timezone.utc,
                ).isoformat()
                _invalidate_cached_client()
            elif failure_reason == "rate_limit":
                state["rateLimitCount"] = int(state.get("rateLimitCount", 0) or 0) + 1
                state["lastRateLimitAt"] = failure_time.isoformat()
            elif failure_reason == "blocked":
                state["blockCount"] = int(state.get("blockCount", 0) or 0) + 1
                state["lastBlockAt"] = failure_time.isoformat()
            else:
                state["temporaryFailureCount"] = int(state.get("temporaryFailureCount", 0) or 0) + 1
                state["lastTemporaryFailureAt"] = failure_time.isoformat()
            state["nextAllowedAt"] = datetime.fromtimestamp(
                failure_time.timestamp() + _backoff_seconds(failure_reason),
                tz=timezone.utc,
            ).isoformat()
            _write_runtime_state(state)
            raise
        finally:
            _REQUEST_CONTEXT.depth = max(0, int(getattr(_REQUEST_CONTEXT, "depth", 1) or 1) - 1)

        success_time = _now()
        state = _read_runtime_state()
        state["successCount"] = int(state.get("successCount", 0) or 0) + 1
        state["lastSuccessAt"] = success_time.isoformat()
        state["lastFailureReason"] = None
        state["lastError"] = None
        state["lastBackoffReason"] = None
        state["nextAllowedAt"] = datetime.fromtimestamp(
            success_time.timestamp() + _MIN_REQUEST_INTERVAL_SECONDS,
            tz=timezone.utc,
        ).isoformat()
        if transport == "praw":
            state["authBackoffUntil"] = None
        _write_runtime_state(state)
        return result


def has_reddit_credentials() -> bool:
    return not (
        REDDIT_CLIENT_ID.startswith("REPLACE_WITH_")
        or REDDIT_CLIENT_SECRET.startswith("REPLACE_WITH_")
    )


def _decorate_reddit_client(reddit: "praw.Reddit") -> None:
    requestor = reddit._core._requestor
    if getattr(requestor, "_sentimeter_wrapped", False):
        return
    authorizer = reddit._core._authorizer

    original_request = requestor.request
    original_refresh = authorizer.refresh

    def disciplined_request(*args: Any, **kwargs: Any):
        path = kwargs.get("path")
        if path is None and len(args) > 1:
            path = args[1]
        operation = f"praw:{path}" if path else "praw:request"
        return perform_reddit_request(
            operation=operation,
            transport="praw",
            func=lambda: original_request(*args, **kwargs),
        )

    def disciplined_refresh():
        return perform_reddit_request(
            operation="praw:auth:refresh",
            transport="praw",
            func=original_refresh,
        )

    requestor.request = disciplined_request
    authorizer.refresh = disciplined_refresh
    requestor._sentimeter_wrapped = True


def create_reddit_client() -> Optional["praw.Reddit"]:
    global _REDDIT_CLIENT

    if praw is None:
        raise RuntimeError("praw is not installed. Run `pip install -r requirements.txt`.")

    if not has_reddit_credentials():
        return None

    if _REDDIT_CLIENT is not None:
        return _REDDIT_CLIENT

    _REDDIT_CLIENT = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        check_for_async=False,
    )
    _REDDIT_CLIENT.read_only = True
    _decorate_reddit_client(_REDDIT_CLIENT)

    state = _read_runtime_state()
    state["clientCreateCount"] = int(state.get("clientCreateCount", 0) or 0) + 1
    state["lastClientCreatedAt"] = _now().isoformat()
    _write_runtime_state(state)
    return _REDDIT_CLIENT
