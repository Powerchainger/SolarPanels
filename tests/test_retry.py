"""Tests for tenacity retry behavior in clients/output.py.

Verifies the two-level retry model:
  - Internal (tenacity): retries transient errors up to MAX_INTERNAL_ATTEMPTS.
  - External (retry_count): NOT touched by post_record; caller's responsibility.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from clients.base import TimedClient
from clients.output import ErrorClass, PostResult, post_record
from db.models import RecordStatus


BASE_URL = "https://output.example.com"
TOKEN = "test-token"


def _make_record(**overrides) -> dict:
    defaults = {
        "provider": "growatt",
        "account_id": "acc-1",
        "record": {"device_sn": "SN001", "ac_power_watts": 1500.0},
        "status": "posting",
        "retry_count": 0,
        "last_error": None,
        "last_attempt_at": None,
    }
    return {**defaults, **overrides}


# ---------------------------------------------------------------------------
# Transient → success (recovery)
# ---------------------------------------------------------------------------


@respx.mock
async def test_503_then_200_recovers():
    """503→503→200: tenacity retries, succeeds on 3rd attempt."""
    route = respx.post(f"{BASE_URL}/records").mock(
        side_effect=[
            httpx.Response(503, text="unavailable"),
            httpx.Response(503, text="unavailable"),
            httpx.Response(200, json={"id": "1"}),
        ]
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.success is True
    assert result.attempts == 3
    assert record["status"] == RecordStatus.POSTED
    assert record["last_error"] is None
    assert route.call_count == 3


@respx.mock
async def test_timeout_then_200_recovers():
    """Timeout→200: tenacity retries, succeeds on 2nd attempt."""
    route = respx.post(f"{BASE_URL}/records").mock(
        side_effect=[
            httpx.TimeoutException("timeout"),
            httpx.Response(200),
        ]
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.success is True
    assert result.attempts == 2
    assert record["status"] == RecordStatus.POSTED
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# Transient exhausted (all retries fail)
# ---------------------------------------------------------------------------


@respx.mock
async def test_503_exhausted_stays_posting():
    """503→503→503: all 3 internal attempts fail, status stays posting."""
    route = respx.post(f"{BASE_URL}/records").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(503),
        ]
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.success is False
    assert result.error_class == ErrorClass.TRANSIENT
    assert result.attempts == 3
    # Status must NOT flip to failed — transient exhaustion stays retriable.
    assert record["status"] == "posting"
    assert "503" in record["last_error"]
    assert route.call_count == 3


# ---------------------------------------------------------------------------
# Permanent failure — no retry
# ---------------------------------------------------------------------------


@respx.mock
async def test_400_no_retry_sets_failed():
    """400: permanent error, exactly 1 attempt, status=failed."""
    route = respx.post(f"{BASE_URL}/records").mock(
        return_value=httpx.Response(400, text="Bad Request")
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.success is False
    assert result.error_class == ErrorClass.PERMANENT
    assert result.attempts == 1
    assert record["status"] == RecordStatus.FAILED
    assert "400" in record["last_error"]
    # Exactly 1 HTTP call — no retry for permanent errors.
    assert route.call_count == 1


@respx.mock
async def test_422_no_retry():
    """422: another permanent error, 1 attempt."""
    route = respx.post(f"{BASE_URL}/records").mock(
        return_value=httpx.Response(422, text="Unprocessable")
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.attempts == 1
    assert record["status"] == RecordStatus.FAILED
    assert route.call_count == 1


# ---------------------------------------------------------------------------
# retry_count is NOT modified
# ---------------------------------------------------------------------------


@respx.mock
async def test_retry_count_untouched_on_success():
    """post_record never touches retry_count — caller's job."""
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(200))

    record = _make_record(retry_count=0)
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert record["retry_count"] == 0


@respx.mock
async def test_retry_count_untouched_on_failure():
    """Even after retries, retry_count stays as the caller set it."""
    respx.post(f"{BASE_URL}/records").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), httpx.Response(503)]
    )

    record = _make_record(retry_count=2)
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    # Caller set retry_count=2; post_record must not increment it.
    assert record["retry_count"] == 2


# ---------------------------------------------------------------------------
# last_attempt_at always updated
# ---------------------------------------------------------------------------


@respx.mock
async def test_last_attempt_at_set_on_every_call():
    """last_attempt_at is written regardless of outcome."""
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(200))

    record = _make_record()
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert record["last_attempt_at"] is not None


@respx.mock
async def test_last_attempt_at_set_on_timeout():
    respx.post(f"{BASE_URL}/records").mock(side_effect=httpx.TimeoutException("t"))

    record = _make_record()
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert record["last_attempt_at"] is not None
