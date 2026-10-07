"""Async EnPhase API client.

Wraps three EnPhase cloud endpoints via httpx:

  1. Token refresh:  POST /oauth/token  (Basic auth with client_id:client_secret)
  2. Site ID lookup: GET  /api/v4/systems
  3. Inverter data:  GET  /api/v4/systems/inverters_summary_by_envoy_or_site

Legacy equivalents:
  Token refresh  → EnPhase/auth.py:65-106
  Site ID lookup → EnPhase/site_id.py:3-25
  Inverter data  → EnPhase/cloud_measurements.py:18-50

Token refresh is called **inline** before every fetch cycle (not as a separate
scheduled job).  Refreshed tokens are written back to Mongo so the service
survives restarts without re-bootstrapping.
"""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from pymongo.asynchronous.database import AsyncDatabase

from clients.base import AuthFailedError, TimedClient
from db.models import ApiConfig, EnphaseCredentials, SiteCache

logger = logging.getLogger(__name__)

BASE_URL = "https://api.enphaseenergy.com"

# Refresh the token when its expiry is within this window.
REFRESH_WINDOW_HOURS = 2


def _doc_id(provider: str, account_id: str) -> str:
    return f"{provider}::{account_id}"


def _raise_if_unauthorized(response: httpx.Response) -> None:
    """Map a 401 on a data call to AuthFailedError (the token was revoked).

    403 is left alone: it more often means a plan/permission problem than a
    dead credential.
    """
    if response.status_code == 401:
        raise AuthFailedError("EnPhase rejected the access token (HTTP 401)")


# ---------------------------------------------------------------------------
# Token refresh
# ---------------------------------------------------------------------------


def should_refresh(expires_at: datetime | None) -> bool:
    """Return True if the token should be refreshed now.

    Refreshes if:
      - expires_at is None (never set), or
      - current time is within REFRESH_WINDOW_HOURS of expiry.
    """
    if expires_at is None:
        return True
    cutoff = expires_at - timedelta(hours=REFRESH_WINDOW_HOURS)
    return datetime.now(UTC) >= cutoff


async def refresh_access_token(
    client: TimedClient,
    creds: EnphaseCredentials,
    db: AsyncDatabase,
    account_id: str,
) -> EnphaseCredentials:
    """Refresh the OAuth access token and write new tokens back to Mongo.

    Uses the `refresh_token` grant with Basic auth (base64 of
    client_id:client_secret).

    Returns the updated EnphaseCredentials (caller should also re-read
    the api_config from Mongo after this call if needed).
    """
    basic_cred = base64.b64encode(
        f"{creds.client_id}:{creds.client_secret}".encode()
    ).decode()

    url = f"{BASE_URL}/oauth/token"
    headers = {
        "Authorization": f"Basic {basic_cred}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    params = {
        "grant_type": "refresh_token",
        "refresh_token": creds.refresh_token,
    }

    response = await client.post(url, headers=headers, data=params)
    if response.status_code == 401:
        # EnPhase answers a dead or revoked refresh token with 401
        # (error=invalid_token). Other 4xx, such as a 400, stay ordinary
        # errors: they are not known to mean a dead credential. The body is
        # deliberately left out of the message.
        raise AuthFailedError(
            f"EnPhase rejected the token refresh for {account_id} "
            f"(HTTP 401) — re-authorization needed"
        )
    response.raise_for_status()
    data = response.json()

    new_creds = EnphaseCredentials(
        client_id=creds.client_id,
        client_secret=creds.client_secret,
        redirect_uri=creds.redirect_uri,
        api_key=creds.api_key,
        access_token=data["access_token"],
        refresh_token=data.get("refresh_token", creds.refresh_token),
        expires_in=data.get("expires_in"),
        token_type=data.get("token_type", "Bearer"),
        expires_at=datetime.now(UTC) + timedelta(seconds=data.get("expires_in", 86399)),
    )

    # Persist new tokens to Mongo so the service survives restarts.
    creds_dict = new_creds.model_dump(mode="json")
    await db.api_configs.update_one(
        {"_id": _doc_id("enphase", account_id)},
        {"$set": {"credentials": creds_dict, "updated_at": datetime.now(UTC)}},
    )

    logger.info("refreshed EnPhase token for account %s", account_id)
    return new_creds


# ---------------------------------------------------------------------------
# Site ID resolution
# ---------------------------------------------------------------------------


async def resolve_site_id(
    client: TimedClient,
    creds: EnphaseCredentials,
    db: AsyncDatabase,
    account_id: str,
) -> SiteCache:
    """Look up the site_id and site name from /api/v4/systems.

    If the site_cache is already populated, returns the cached value
    without making an API call.  On cache miss, fetches and writes back
    to Mongo.

    Raises RuntimeError if no systems are found.
    """
    # Check cache first.
    raw_doc = await db.api_configs.find_one({"_id": _doc_id("enphase", account_id)})
    if raw_doc and raw_doc.get("site_cache"):
        cache = raw_doc["site_cache"]
        logger.debug("site_cache hit for %s: site_id=%s", account_id, cache.get("site_id"))
        return SiteCache(**cache)

    # Cache miss — fetch from API.
    url = f"{BASE_URL}/api/v4/systems"
    headers = {
        "Authorization": f"{creds.token_type} {creds.access_token}",
        "key": creds.api_key,
    }

    response = await client.get(url, headers=headers)
    _raise_if_unauthorized(response)
    response.raise_for_status()
    data = response.json()

    systems = data.get("systems", [])
    if not systems:
        raise RuntimeError(f"No systems found for EnPhase account {account_id}")

    site_cache = SiteCache(
        site_id=systems[0]["system_id"],
        user=systems[0]["name"],
    )

    # Write cache back to Mongo.
    await db.api_configs.update_one(
        {"_id": _doc_id("enphase", account_id)},
        {"$set": {
            "site_cache": site_cache.model_dump(mode="json"),
            "updated_at": datetime.now(UTC),
        }},
    )

    logger.info("resolved site_id=%s user=%s for %s", site_cache.site_id, site_cache.user, account_id)
    return site_cache


# ---------------------------------------------------------------------------
# Inverter summary fetch
# ---------------------------------------------------------------------------


async def fetch_inverter_summary(
    client: TimedClient,
    creds: EnphaseCredentials,
    site_id: int,
) -> list[dict[str, Any]]:
    """Fetch /api/v4/systems/inverters_summary_by_envoy_or_site.

    Returns the raw JSON body (a list) so the transform pipeline can
    parse it.
    """
    url = f"{BASE_URL}/api/v4/systems/inverters_summary_by_envoy_or_site"
    headers = {
        "Authorization": f"{creds.token_type} {creds.access_token}",
        "key": creds.api_key,
    }
    params = {"site_id": str(site_id)}

    response = await client.get(url, headers=headers, params=params)
    _raise_if_unauthorized(response)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# High-level fetch (called by providers.py registry)
# ---------------------------------------------------------------------------


async def fetch_cycle(
    db: AsyncDatabase,
    api_config: ApiConfig,
    client: TimedClient,
) -> list[tuple[str, dict[str, Any]]]:
    """Run a full EnPhase fetch cycle.

    1. Refresh token if needed (writes new tokens to Mongo).
    2. Resolve site_id (uses cache if available).
    3. Fetch inverter summary.
    4. Return [("inverter_summary", raw_body)].

    The provider registry passes this to the transform function.
    """
    creds = api_config.credentials
    assert isinstance(creds, EnphaseCredentials)

    # 1. Token refresh.
    refreshed = False
    if should_refresh(creds.expires_at):
        creds = await refresh_access_token(client, creds, db, api_config.account_id)
        refreshed = True

    async def attempt(c: EnphaseCredentials) -> Any:
        # 2. Site ID, then 3. Fetch.
        site_cache = await resolve_site_id(client, c, db, api_config.account_id)
        return await fetch_inverter_summary(client, c, site_cache.site_id)

    try:
        raw_body = await attempt(creds)
    except AuthFailedError:
        # A 401 on a still-"valid" token usually means another process rotated
        # it (or EnPhase revoked it). One forced refresh can recover that; if
        # this cycle already refreshed, the token really is rejected.
        if refreshed:
            raise
        creds = await refresh_access_token(client, creds, db, api_config.account_id)
        raw_body = await attempt(creds)

    return [("inverter_summary", raw_body)]
