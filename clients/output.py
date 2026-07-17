"""Async client for POSTing processed records to the output API.

Two retry levels (see IMPL.md / PLAN.md improvement 3):

  1. **Internal retry (tenacity)** — up to 3 HTTP attempts with exponential
     backoff for transient errors (5xx, 429, timeout).  These are invisible
     to `retry_count`; the caller sees a single `PostResult.attempts` value.

  2. **External retry (retry_count)** — the caller (scheduler job or flush
     job) increments `retry_count` once per *delivery attempt*.  The flush
     job filters on `retry_count < N` to bound total retries.

Permanent errors (4xx except 429) set `status=failed` immediately with no
internal retry.  `post_record` does NOT touch `retry_count` — the caller
must increment it once per call to this function.

The caller persists the mutated record dict to MongoDB after this returns.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

import httpx
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from clients.base import TimedClient
from db.models import RecordStatus

logger = logging.getLogger(__name__)

# Max internal (tenacity) attempts per delivery.
MAX_INTERNAL_ATTEMPTS = 3


class ErrorClass(Enum):
    """Classification of an HTTP error for retry decisions."""

    NONE = "none"             # 2xx — success
    TRANSIENT = "transient"   # 5xx, 429, timeout — retryable
    PERMANENT = "permanent"   # other 4xx — no retry, set failed


class _TransientError(Exception):
    """Internal exception to signal tenacity to retry.

    Raised when a transient HTTP error is encountered inside the retry loop.
    Carries the PostResult so the outer function can extract it after
    tenacity gives up.
    """

    def __init__(self, result: PostResult) -> None:
        self.result = result


@dataclass(frozen=True)
class PostResult:
    """Outcome of a single delivery attempt (which may include internal retries)."""

    success: bool
    error_class: ErrorClass
    status_code: int | None = None
    error_message: str | None = None
    attempts: int = 1  # total HTTP calls made within this delivery attempt


def _classify_error(status_code: int | None = None, is_timeout: bool = False) -> ErrorClass:
    """Classify an HTTP response into retryable / permanent / success."""
    if is_timeout:
        return ErrorClass.TRANSIENT
    if status_code is None:
        return ErrorClass.TRANSIENT
    if 200 <= status_code < 300:
        return ErrorClass.NONE
    if status_code == 429 or status_code >= 500:
        return ErrorClass.TRANSIENT
    # 4xx (except 429) = permanent
    return ErrorClass.PERMANENT


async def _do_post(
    client: TimedClient,
    url: str,
    record: dict,
    headers: dict[str, str],
    attempt_counter: list[int],
) -> PostResult:
    """Execute a single HTTP POST and classify the result.

    Raises _TransientError on transient failures so tenacity can retry.
    Raises nothing on success or permanent failure.

    `attempt_counter` is mutated in-place so the outer function can read
    the total attempt count after tenacity exits (success, permanent, or
    RetryError).
    """
    attempt_counter[0] += 1
    attempts = attempt_counter[0]

    try:
        response = await client.post(url, json=record, headers=headers)
        error_class = _classify_error(status_code=response.status_code)

        if error_class == ErrorClass.NONE:
            return PostResult(
                success=True,
                error_class=ErrorClass.NONE,
                status_code=response.status_code,
                attempts=attempts,
            )

        if error_class == ErrorClass.PERMANENT:
            body_preview = response.text[:200] if response.text else ""
            return PostResult(
                success=False,
                error_class=ErrorClass.PERMANENT,
                status_code=response.status_code,
                error_message=f"HTTP {response.status_code}: {body_preview}",
                attempts=attempts,
            )

        # Transient — raise for tenacity to retry.
        raise _TransientError(PostResult(
            success=False,
            error_class=ErrorClass.TRANSIENT,
            status_code=response.status_code,
            error_message=f"HTTP {response.status_code}",
            attempts=attempts,
        ))

    except _TransientError:
        raise  # re-raise — tenacity catches this
    except httpx.TimeoutException as exc:
        raise _TransientError(PostResult(
            success=False,
            error_class=ErrorClass.TRANSIENT,
            error_message=f"Timeout: {exc}",
            attempts=attempts,
        ))
    except httpx.HTTPError as exc:
        raise _TransientError(PostResult(
            success=False,
            error_class=ErrorClass.TRANSIENT,
            error_message=f"HTTP error: {exc}",
            attempts=attempts,
        ))


async def post_record(
    client: TimedClient,
    record: dict,
    *,
    base_url: str,
    token: str = "",
) -> PostResult:
    """POST one processed record to the output API with internal retry.

    Mutates `record` in-place:
      - Sets `status` to `posted` or `failed` based on the response.
      - Sets `last_error` on failure (None on success).
      - Sets `last_attempt_at` to current UTC time.

    Does NOT modify `retry_count` — the caller increments it once per
    delivery attempt (i.e. once per call to this function).

    Args:
        client:   Shared TimedClient (created via clients/base.py).
        record:   ProcessedRecord.model_dump(mode="json") dict.
        base_url: Output API base URL (e.g. "https://api.example.com").
        token:    Bearer token for the output API (empty if no auth).

    Returns:
        PostResult with success flag, error classification, details, and
        the total number of HTTP attempts made.
    """
    url = f"{base_url.rstrip("/")}/records"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    now = datetime.now(UTC)
    attempt_counter = [0]  # mutable counter for _do_post

    # Build the tenacity-wrapped callable.
    retrying_post = retry(
        retry=retry_if_exception_type(_TransientError),
        stop=stop_after_attempt(MAX_INTERNAL_ATTEMPTS),
        wait=wait_exponential_jitter(initial=1, max=10, jitter=1),
        reraise=True,
    )(_do_post)

    result: PostResult
    try:
        result = await retrying_post(client, url, record, headers, attempt_counter)
    except _TransientError as exc:
        # Tenacity exhausted all retries — use the last transient result.
        result = exc.result

    # Mutate the record dict so the caller can persist.
    record["last_attempt_at"] = now.isoformat()

    if result.success:
        record["status"] = RecordStatus.POSTED
        record["last_error"] = None
    elif result.error_class == ErrorClass.PERMANENT:
        record["status"] = RecordStatus.FAILED
        record["last_error"] = result.error_message
    else:
        # Transient exhausted — status stays as-is (posting/pending).
        record["last_error"] = result.error_message

    logger.info(
        "POST %s%s → %s (status=%s, attempts=%d)",
        base_url, "/records",
        result.status_code or "timeout",
        record.get("status"),
        result.attempts,
    )

    return result
