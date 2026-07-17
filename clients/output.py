"""Async client for POSTing processed records to the output API.

This module (Step 7) handles:
  - Serialising a ProcessedRecord to JSON
  - Classifying HTTP errors into permanent / transient
  - Updating the record dict with status, last_error, last_attempt_at

It does **not** retry — that is added in Step 8 via tenacity wrapping
`post_record`.

The caller (scheduler job or flush job) is responsible for persisting the
updated record dict back to MongoDB after this function returns.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum

import httpx

from clients.base import TimedClient
from db.models import RecordStatus

logger = logging.getLogger(__name__)


class ErrorClass(Enum):
    """Classification of an HTTP error for retry decisions."""

    NONE = "none"           # 2xx — success
    TRANSIENT = "transient" # 5xx, 429, timeout — retryable
    PERMANENT = "permanent" # other 4xx — no retry, set failed


@dataclass(frozen=True)
class PostResult:
    """Outcome of a single POST attempt."""

    success: bool
    error_class: ErrorClass
    status_code: int | None = None
    error_message: str | None = None


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


async def post_record(
    client: TimedClient,
    record: dict,
    *,
    base_url: str,
    token: str = "",
) -> PostResult:
    """POST one processed record to the output API.

    Mutates `record` in-place:
      - Sets `status` to `posted` or `failed` based on the response.
      - Sets `last_error` on failure (None on success).
      - Sets `last_attempt_at` to current UTC time.

    The caller must persist the updated record to MongoDB after this returns.

    Args:
        client:   Shared TimedClient (created via clients/base.py).
        record:   ProcessedRecord.model_dump(mode="json") dict.
        base_url: Output API base URL (e.g. "https://api.example.com").
        token:    Bearer token for the output API (empty if no auth).

    Returns:
        PostResult with success flag, error classification, and details.
    """
    url = f"{base_url.rstrip('/')}/records"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    now = datetime.now(UTC)
    result: PostResult

    try:
        response = await client.post(url, json=record, headers=headers)
        error_class = _classify_error(status_code=response.status_code)

        if error_class == ErrorClass.NONE:
            result = PostResult(success=True, error_class=ErrorClass.NONE, status_code=response.status_code)
        elif error_class == ErrorClass.PERMANENT:
            body_preview = response.text[:200] if response.text else ""
            result = PostResult(
                success=False,
                error_class=ErrorClass.PERMANENT,
                status_code=response.status_code,
                error_message=f"HTTP {response.status_code}: {body_preview}",
            )
        else:
            # Transient — don't set failed yet; caller decides whether to retry.
            result = PostResult(
                success=False,
                error_class=ErrorClass.TRANSIENT,
                status_code=response.status_code,
                error_message=f"HTTP {response.status_code}",
            )

    except httpx.TimeoutException as exc:
        result = PostResult(
            success=False,
            error_class=ErrorClass.TRANSIENT,
            error_message=f"Timeout: {exc}",
        )
    except httpx.HTTPError as exc:
        result = PostResult(
            success=False,
            error_class=ErrorClass.TRANSIENT,
            error_message=f"HTTP error: {exc}",
        )

    # Mutate the record dict so the caller can persist.
    record["last_attempt_at"] = now.isoformat()

    if result.success:
        record["status"] = RecordStatus.POSTED
        record["last_error"] = None
    elif result.error_class == ErrorClass.PERMANENT:
        record["status"] = RecordStatus.FAILED
        record["last_error"] = result.error_message
    # Transient: status stays as-is (posting/pending); caller decides retry.
    else:
        record["last_error"] = result.error_message

    logger.info(
        "POST %s%s → %s (status=%s)",
        base_url, "/records",
        result.status_code or "timeout",
        record.get("status"),
    )

    return result
