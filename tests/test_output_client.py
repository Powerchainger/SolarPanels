"""Tests for clients/base.py and clients/output.py.

Uses respx to mock HTTP at the httpx transport level — no real server needed.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from clients.base import DEFAULT_TIMEOUT, TimedClient, create_client
from clients.output import ErrorClass, PostResult, post_record
from db.models import RecordStatus


# ---------------------------------------------------------------------------
# clients/base.py
# ---------------------------------------------------------------------------


def test_create_client_returns_async_client():
    client = create_client()
    assert isinstance(client, httpx.AsyncClient)


def test_create_client_with_base_url():
    client = create_client(base_url="https://api.example.com")
    assert client.base_url == httpx.URL("https://api.example.com")


def test_timed_client_defaults_to_default_timeout():
    # httpx's own default is a 5 s read timeout, which EnPhase exceeds.
    assert TimedClient().timeout == DEFAULT_TIMEOUT
    assert DEFAULT_TIMEOUT.read == 30.0


def test_timed_client_timeout_can_be_overridden():
    assert TimedClient(timeout=httpx.Timeout(2.0)).timeout == httpx.Timeout(2.0)


def test_create_client_default_headers():
    client = create_client(headers={"X-Custom": "value"})
    assert client.headers["X-Custom"] == "value"


def test_timed_client_is_subclass():
    client = TimedClient()
    assert isinstance(client, httpx.AsyncClient)


@respx.mock
async def test_timed_client_logs_request_response(caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    respx.get("https://api.example.com/ping").mock(return_value=httpx.Response(200, text="ok"))

    async with TimedClient() as client:
        resp = await client.get("https://api.example.com/ping")

    assert resp.status_code == 200
    # Check that log messages were emitted.
    log_messages = [r.message for r in caplog.records]
    assert any("HTTP →" in m for m in log_messages)
    assert any("HTTP ←" in m for m in log_messages)


# ---------------------------------------------------------------------------
# clients/output.py — helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://output.example.com"
TOKEN = "secret-token"


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
# post_record — success
# ---------------------------------------------------------------------------


@respx.mock
async def test_post_record_success_sets_posted():
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(201, json={"id": "123"}))

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    assert result.success is True
    assert result.error_class == ErrorClass.NONE
    assert record["status"] == RecordStatus.POSTED
    assert record["last_error"] is None
    assert record["last_attempt_at"] is not None


# ---------------------------------------------------------------------------
# post_record — permanent failure (4xx)
# ---------------------------------------------------------------------------


@respx.mock
async def test_post_record_400_sets_failed():
    respx.post(f"{BASE_URL}/records").mock(
        return_value=httpx.Response(400, text="Bad Request: missing field 'timestamp'")
    )

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL)

    assert result.success is False
    assert result.error_class == ErrorClass.PERMANENT
    assert record["status"] == RecordStatus.FAILED
    assert "400" in record["last_error"]


@respx.mock
async def test_post_record_404_is_permanent():
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(404, text="not found"))

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL)

    assert result.error_class == ErrorClass.PERMANENT
    assert record["status"] == RecordStatus.FAILED


# ---------------------------------------------------------------------------
# post_record — transient failure (5xx, 429)
# ---------------------------------------------------------------------------


@respx.mock
async def test_post_record_503_is_transient():
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(503, text="Service Unavailable"))

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL)

    assert result.success is False
    assert result.error_class == ErrorClass.TRANSIENT
    # Status should NOT be set to failed for transient errors.
    assert record["status"] == "posting"
    assert "503" in record["last_error"]


@respx.mock
async def test_post_record_429_is_transient():
    respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(429, text="Rate limited"))

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL)

    assert result.error_class == ErrorClass.TRANSIENT
    assert record["status"] == "posting"


@respx.mock
async def test_post_record_timeout_is_transient():
    respx.post(f"{BASE_URL}/records").mock(side_effect=httpx.TimeoutException("timeout"))

    record = _make_record()
    async with TimedClient() as client:
        result = await post_record(client, record, base_url=BASE_URL)

    assert result.success is False
    assert result.error_class == ErrorClass.TRANSIENT
    assert record["status"] == "posting"
    assert "Timeout" in record["last_error"]
    assert record["last_attempt_at"] is not None


# ---------------------------------------------------------------------------
# post_record — authorization header
# ---------------------------------------------------------------------------


@respx.mock
async def test_post_record_sends_bearer_token():
    route = respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(200))

    record = _make_record()
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token=TOKEN)

    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Bearer {TOKEN}"


@respx.mock
async def test_post_record_no_auth_header_when_no_token():
    route = respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(200))

    record = _make_record()
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL, token="")

    request = route.calls.last.request
    assert "Authorization" not in request.headers


# ---------------------------------------------------------------------------
# post_record — JSON body
# ---------------------------------------------------------------------------


@respx.mock
async def test_post_record_sends_record_as_json():
    route = respx.post(f"{BASE_URL}/records").mock(return_value=httpx.Response(200))

    record = _make_record()
    async with TimedClient() as client:
        await post_record(client, record, base_url=BASE_URL)

    import json
    body = json.loads(route.calls.last.request.content)
    assert body["provider"] == "growatt"
    assert body["account_id"] == "acc-1"
    assert body["record"]["device_sn"] == "SN001"
