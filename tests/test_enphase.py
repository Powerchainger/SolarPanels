"""Tests for clients/enphase.py — EnPhase API client.

Uses respx for HTTP mocking and AsyncMock for MongoDB.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import respx

from clients.base import AuthFailedError, TimedClient
from clients.enphase import (
    BASE_URL,
    fetch_cycle,
    fetch_inverter_summary,
    refresh_access_token,
    resolve_site_id,
    should_refresh,
)
from db.models import ApiConfig, EnphaseCredentials, SiteCache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_SYSTEMS_RESPONSE = {
    "systems": [
        {"system_id": 12345, "name": "Sunnyville"},
    ]
}

SAMPLE_INVERTER_SUMMARY = [
    {
        "micro_inverters": [
            {
                "last_report_date": "2026-07-17T14:00:00+02:00",
                "power_produced": {"value": 150.5},
            },
            {
                "last_report_date": "2026-07-17T14:00:00+02:00",
                "power_produced": {"value": 200.3},
            },
        ]
    }
]

SAMPLE_TOKEN_RESPONSE = {
    "access_token": "new-access-token",
    "refresh_token": "new-refresh-token",
    "token_type": "Bearer",
    "expires_in": 86399,
}


def _make_creds(**overrides) -> EnphaseCredentials:
    defaults = dict(
        client_id="cid",
        client_secret="csec",
        redirect_uri="https://example.com/cb",
        api_key="key123",
        access_token="old-access",
        refresh_token="old-refresh",
        expires_in=86399,
        expires_at=datetime.now(UTC) + timedelta(hours=24),  # valid
        token_type="Bearer",
    )
    defaults.update(overrides)
    return EnphaseCredentials(**defaults)


def _make_config(**overrides) -> ApiConfig:
    creds = _make_creds()
    defaults = dict(
        provider="enphase",
        account_id="acc-enphase",
        interval_seconds=3600,
        credentials=creds,
        enabled=True,
        site_cache=None,
    )
    defaults.update(overrides)
    return ApiConfig(**defaults)


def _mock_db() -> MagicMock:
    db = MagicMock()
    db.api_configs.find_one = AsyncMock(return_value=None)
    db.api_configs.update_one = AsyncMock()
    return db


# ---------------------------------------------------------------------------
# should_refresh
# ---------------------------------------------------------------------------


def test_should_refresh_none():
    assert should_refresh(None) is True


def test_should_refresh_expired():
    assert should_refresh(datetime.now(UTC) - timedelta(hours=1)) is True


def test_should_refresh_within_2h_window():
    assert should_refresh(datetime.now(UTC) + timedelta(hours=1)) is True


def test_should_refresh_still_valid():
    assert should_refresh(datetime.now(UTC) + timedelta(hours=3)) is False


# ---------------------------------------------------------------------------
# refresh_access_token
# ---------------------------------------------------------------------------


@respx.mock
async def test_refresh_access_token_sends_basic_auth():
    route = respx.post(f"{BASE_URL}/oauth/token").mock(
        return_value=httpx.Response(200, json=SAMPLE_TOKEN_RESPONSE)
    )

    creds = _make_creds()
    db = _mock_db()

    async with TimedClient() as client:
        new_creds = await refresh_access_token(client, creds, db, "acc-enphase")

    assert new_creds.access_token == "new-access-token"
    assert new_creds.refresh_token == "new-refresh-token"

    # Verify Basic auth header.
    request = route.calls.last.request
    encoded = base64.b64encode(b"cid:csec").decode()
    assert request.headers["Authorization"] == f"Basic {encoded}"


@respx.mock
async def test_refresh_access_token_writes_to_mongo():
    respx.post(f"{BASE_URL}/oauth/token").mock(
        return_value=httpx.Response(200, json=SAMPLE_TOKEN_RESPONSE)
    )

    creds = _make_creds()
    db = _mock_db()

    async with TimedClient() as client:
        await refresh_access_token(client, creds, db, "acc-enphase")

    db.api_configs.update_one.assert_called_once()
    call_args = db.api_configs.update_one.call_args[0]
    assert call_args[0] == {"_id": "enphase::acc-enphase"}
    assert "credentials" in call_args[1]["$set"]
    # Write-back must encrypt at rest: tokens are Fernet, client_id is not.
    creds = call_args[1]["$set"]["credentials"]
    assert creds["access_token"].startswith("gAAAA")
    assert creds["refresh_token"].startswith("gAAAA")
    assert creds["client_id"] == "cid"


@respx.mock
async def test_refresh_access_token_http_error():
    respx.post(f"{BASE_URL}/oauth/token").mock(
        return_value=httpx.Response(401, text="invalid_grant")
    )

    creds = _make_creds()
    db = _mock_db()

    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await refresh_access_token(client, creds, db, "acc-enphase")

    # A rejected refresh must not touch the stored credentials.
    db.api_configs.update_one.assert_not_called()


@respx.mock
async def test_refresh_access_token_auth_rejection_message_has_no_body():
    respx.post(f"{BASE_URL}/oauth/token").mock(
        return_value=httpx.Response(401, text="secret-looking-body")
    )
    async with TimedClient() as client:
        with pytest.raises(AuthFailedError) as exc:
            await refresh_access_token(client, _make_creds(), _mock_db(), "acc-enphase")
    assert "secret-looking-body" not in str(exc.value)
    assert "401" in str(exc.value)


@respx.mock
async def test_refresh_access_token_400_is_not_auth_failure():
    """Only 401 is known to signal a dead token; a 400 stays an ordinary error."""
    respx.post(f"{BASE_URL}/oauth/token").mock(return_value=httpx.Response(400))
    async with TimedClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await refresh_access_token(client, _make_creds(), _mock_db(), "acc-enphase")


@respx.mock
async def test_refresh_access_token_server_error_is_not_auth_failure():
    respx.post(f"{BASE_URL}/oauth/token").mock(return_value=httpx.Response(503))
    async with TimedClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await refresh_access_token(client, _make_creds(), _mock_db(), "acc-enphase")


# ---------------------------------------------------------------------------
# resolve_site_id
# ---------------------------------------------------------------------------


@respx.mock
async def test_resolve_site_id_cache_hit():
    """When site_cache is already in Mongo, no API call is made."""
    cached = SiteCache(site_id=99999, user="CachedSite")
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value={
        "site_cache": cached.model_dump(mode="json"),
    })

    creds = _make_creds()

    async with TimedClient() as client:
        result = await resolve_site_id(client, creds, db, "acc-enphase")

    assert result.site_id == 99999
    assert result.user == "CachedSite"


@respx.mock
async def test_resolve_site_id_cache_miss_fetches():
    """When no cache, fetches from /api/v4/systems and writes cache."""
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)

    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )

    creds = _make_creds()

    async with TimedClient() as client:
        result = await resolve_site_id(client, creds, db, "acc-enphase")

    assert result.site_id == 12345
    assert result.user == "Sunnyville"

    # Cache written back to Mongo.
    db.api_configs.update_one.assert_called_once()


@respx.mock
async def test_resolve_site_id_no_systems_raises():
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)

    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json={"systems": []})
    )

    creds = _make_creds()

    async with TimedClient() as client:
        with pytest.raises(RuntimeError, match="No systems"):
            await resolve_site_id(client, creds, db, "acc-enphase")


# ---------------------------------------------------------------------------
# fetch_inverter_summary
# ---------------------------------------------------------------------------


@respx.mock
async def test_fetch_inverter_summary_returns_raw():
    respx.get(
        f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY))

    creds = _make_creds()

    async with TimedClient() as client:
        result = await fetch_inverter_summary(client, creds, site_id=12345)

    assert result == SAMPLE_INVERTER_SUMMARY


@respx.mock
async def test_fetch_inverter_summary_sends_auth():
    route = respx.get(
        f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY))

    creds = _make_creds()

    async with TimedClient() as client:
        await fetch_inverter_summary(client, creds, site_id=12345)

    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer old-access"
    assert request.headers["key"] == "key123"
    assert "site_id=12345" in str(request.url)


# ---------------------------------------------------------------------------
# fetch_cycle (full orchestration)
# ---------------------------------------------------------------------------


@respx.mock
async def test_fetch_cycle_full_flow():
    """Token valid + cache miss → resolve site → fetch inverters."""
    db = _mock_db()

    # No site cache → triggers /systems call.
    db.api_configs.find_one = AsyncMock(return_value=None)

    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(
        f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY))

    config = _make_config()

    async with TimedClient() as client:
        result = await fetch_cycle(db, config, client)

    assert len(result) == 1
    label, raw_body = result[0]
    assert label == "inverter_summary"
    assert raw_body == SAMPLE_INVERTER_SUMMARY


@respx.mock
async def test_fetch_cycle_refreshes_expired_token():
    """Token within 2h window → refresh fires before fetch."""
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)

    respx.post(f"{BASE_URL}/oauth/token").mock(
        return_value=httpx.Response(200, json=SAMPLE_TOKEN_RESPONSE)
    )
    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(
        f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY))

    config = _make_config(
        credentials=_make_creds(expires_at=datetime.now(UTC) + timedelta(hours=1))
    )

    async with TimedClient() as client:
        result = await fetch_cycle(db, config, client)

    assert len(result) == 1
    # Token refresh should have been called.
    db.api_configs.update_one.assert_called()


@respx.mock
async def test_fetch_cycle_uses_site_cache():
    """When site_cache exists, /systems is NOT called."""
    cached = SiteCache(site_id=55555, user="CachedSite")
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value={
        "site_cache": cached.model_dump(mode="json"),
    })

    inverter_route = respx.get(
        f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    ).mock(return_value=httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY))

    config = _make_config(site_cache=cached)

    async with TimedClient() as client:
        result = await fetch_cycle(db, config, client)

    assert len(result) == 1
    # Only one HTTP call (inverter_summary), not two.
    assert inverter_route.call_count == 1


@respx.mock
async def test_resolve_site_id_401_is_auth_failure():
    respx.get(f"{BASE_URL}/api/v4/systems").mock(return_value=httpx.Response(401))
    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await resolve_site_id(client, _make_creds(), _mock_db(), "acc-enphase")


@respx.mock
async def test_fetch_inverter_summary_401_is_auth_failure():
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        return_value=httpx.Response(401)
    )
    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await fetch_inverter_summary(client, _make_creds(), 1)


@respx.mock
async def test_fetch_inverter_summary_403_is_not_auth_failure():
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        return_value=httpx.Response(403)
    )
    async with TimedClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_inverter_summary(client, _make_creds(), 1)


def _refresh_ok():
    return httpx.Response(200, json=SAMPLE_TOKEN_RESPONSE)


@respx.mock
async def test_fetch_cycle_data_401_forces_one_refresh_and_retries():
    """Valid-looking token rejected (e.g. rotated elsewhere) → refresh once, retry, succeed."""
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)
    token = respx.post(f"{BASE_URL}/oauth/token").mock(return_value=_refresh_ok())
    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        side_effect=[httpx.Response(401), httpx.Response(200, json=SAMPLE_INVERTER_SUMMARY)]
    )

    async with TimedClient() as client:
        result = await fetch_cycle(db, _make_config(), client)  # token not near expiry

    assert result[0][1] == SAMPLE_INVERTER_SUMMARY
    assert token.call_count == 1


@respx.mock
async def test_fetch_cycle_data_401_after_forced_refresh_is_auth_failure():
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)
    token = respx.post(f"{BASE_URL}/oauth/token").mock(return_value=_refresh_ok())
    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        return_value=httpx.Response(401)
    )

    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await fetch_cycle(db, _make_config(), client)

    assert token.call_count == 1  # exactly one forced refresh, no loop


@respx.mock
async def test_fetch_cycle_data_401_right_after_scheduled_refresh_does_not_refresh_again():
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)
    token = respx.post(f"{BASE_URL}/oauth/token").mock(return_value=_refresh_ok())
    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        return_value=httpx.Response(401)
    )
    config = _make_config(
        credentials=_make_creds(expires_at=datetime.now(UTC) + timedelta(hours=1))
    )

    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await fetch_cycle(db, config, client)

    assert token.call_count == 1  # the scheduled refresh only


@respx.mock
async def test_fetch_cycle_refresh_rejected_during_retry_is_auth_failure():
    db = _mock_db()
    db.api_configs.find_one = AsyncMock(return_value=None)
    respx.post(f"{BASE_URL}/oauth/token").mock(return_value=httpx.Response(401))
    respx.get(f"{BASE_URL}/api/v4/systems").mock(
        return_value=httpx.Response(200, json=SAMPLE_SYSTEMS_RESPONSE)
    )
    respx.get(f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site").mock(
        return_value=httpx.Response(401)
    )

    async with TimedClient() as client:
        with pytest.raises(AuthFailedError):
            await fetch_cycle(db, _make_config(), client)
