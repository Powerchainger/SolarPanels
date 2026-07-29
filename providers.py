"""Provider dispatch registry.

Maps each provider name to a `ProviderSpec` dataclass holding the credentials
type, an async fetch function, and a transform function.  The scheduler,
transform pipeline, and `manage_keys.py` look up here instead of branching on
provider string — adding a future provider is one registry entry plus its
implementation files.

This is **not** a unified `Provider` ABC (see PLAN.md "Provider extensibility"):
the two providers have fundamentally different auth models (OAuth vs. static
token), different response shapes, and different output cardinalities (one
row per cycle vs. N rows per cycle).  A shared ABC would force leaky abstractions.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

from db.models import (
    ApiConfig,
    EnphaseCredentials,
    GrowattCredentials,
    GrowattCache,
)

logger = logging.getLogger(__name__)

from pymongo.asynchronous.database import AsyncDatabase
from clients.base import TimedClient

# Type aliases for the signatures the scheduler expects.
#
# FetchFn receives an ApiConfig, db handle, and http client; returns
#   a list of (label, raw_dict) pairs.
#   - For Growatt: label = device_sn, raw = min_energy dict.
#   - For EnPhase: label = "inverter_summary", raw = full response array.
#
# TransformFn receives the raw fetch output and a context dict, returns
#   a list of row dicts (ready for ProcessedRecord.record).
FetchFn = Callable[
    [ApiConfig, AsyncDatabase, TimedClient],
    Awaitable[list[tuple[str, dict[str, Any]]]],
]
ContextDict = dict[str, Any]
TransformFn = Callable[
    [list[tuple[str, dict[str, Any]]], ContextDict],
    list[dict[str, Any]],
]


@dataclass(frozen=True)
class ProviderSpec:
    """Everything the scheduler needs to know about one provider."""

    credentials_class: type
    fetch: FetchFn
    transform: TransformFn


# ---------------------------------------------------------------------------
# Growatt
# ---------------------------------------------------------------------------


# Cache freshness threshold — re-fetch plant/device list after this long.
GROWATT_CACHE_TTL_HOURS = 24


async def _growatt_fetch(
    cfg: ApiConfig,
    db: AsyncDatabase,
    _client: TimedClient,
) -> list[tuple[str, dict[str, Any]]]:
    """Fetch a full cycle for one Growatt account.

    Uses cached plant_id and device list when available and fresh (<24h).
    On cache miss, fetches and stores the cache eagerly before calling
    fetch_cycle, so the cache is populated even if min_energy calls fail.
    """
    from clients.growatt import fetch_cycle, list_plants, list_devices

    creds = cfg.credentials
    assert isinstance(creds, GrowattCredentials)

    # Check cache freshness.
    cached_plant_id = None
    cached_devices = None
    cache_expired = True
    if cfg.growatt_cache:
        age = datetime.now(UTC) - cfg.growatt_cache.cached_at
        if age < timedelta(hours=GROWATT_CACHE_TTL_HOURS):
            cached_plant_id = cfg.growatt_cache.plant_id
            cached_devices = cfg.growatt_cache.devices
            cache_expired = False
            logger.debug(
                "growatt cache hit for %s (age=%.1fh)",
                cfg.account_id, age.total_seconds() / 3600,
            )
        else:
            logger.debug(
                "growatt cache expired for %s (age=%.1fh)",
                cfg.account_id, age.total_seconds() / 3600,
            )

    # On cache miss, populate cache eagerly before calling fetch_cycle.
    if cache_expired:
        try:
            plants = await list_plants(creds.api_token)
            if plants:
                plant_id = str(plants[0].get("plant_id", ""))
                devices = await list_devices(creds.api_token, plant_id, device_type=7)
                new_cache = GrowattCache(plant_id=plant_id, devices=devices)
                doc_id = f"{cfg.provider}::{cfg.account_id}"
                await db.api_configs.update_one(
                    {"_id": doc_id},
                    {"$set": {"growatt_cache": new_cache.model_dump(mode="json")}},
                )
                cached_plant_id = plant_id
                cached_devices = devices
                logger.info("populated growatt cache for %s", cfg.account_id)
        except Exception:
            logger.warning("failed to populate growatt cache for %s, will retry next cycle", cfg.account_id)

    raw, _fetched_plant_id, _fetched_devices = await fetch_cycle(
        creds.api_token,
        device_type=7,
        cached_plant_id=cached_plant_id,
        cached_devices=cached_devices,
    )

    return raw


def _growatt_transform(
    raw_responses: list[tuple[str, dict[str, Any]]],
    context: ContextDict,
) -> list[dict[str, Any]]:
    """Transform raw Growatt device responses into GrowattRow dicts."""
    from transform.pipeline import parse_growatt

    timestamp = context.get("timestamp") or datetime.now(UTC)
    rows = parse_growatt(raw_responses, timestamp=timestamp)
    return [r.model_dump(mode="json") for r in rows]


# ---------------------------------------------------------------------------
# EnPhase
# ---------------------------------------------------------------------------


async def _enphase_fetch(
    cfg: ApiConfig,
    db: AsyncDatabase,
    client: TimedClient,
) -> list[tuple[str, dict[str, Any]]]:
    """Fetch inverter summary for one EnPhase account.

    Handles token refresh, site_id resolution, and the actual API call
    via clients/enphase.py.
    """
    from clients.enphase import fetch_cycle

    return await fetch_cycle(db, cfg, client)


def _enphase_transform(
    raw_responses: list[tuple[str, dict[str, Any]]],
    context: ContextDict,
) -> list[dict[str, Any]]:
    """Transform raw EnPhase response into EnphaseRow dicts."""
    from transform.pipeline import parse_enphase

    site_name = context.get("site_name", "")
    rows = []
    for _label, raw_body in raw_responses:
        rows.extend(parse_enphase(raw_body, site_name=site_name))
    return [r.model_dump(mode="json") for r in rows]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


REGISTRY: dict[str, ProviderSpec] = {
    "growatt": ProviderSpec(
        credentials_class=GrowattCredentials,
        fetch=_growatt_fetch,
        transform=_growatt_transform,
    ),
    "enphase": ProviderSpec(
        credentials_class=EnphaseCredentials,
        fetch=_enphase_fetch,
        transform=_enphase_transform,
    ),
}


def get_spec(provider: str) -> ProviderSpec:
    """Look up a provider spec by name.  Raises KeyError for unknown providers."""
    try:
        return REGISTRY[provider]
    except KeyError:
        raise KeyError(
            f"Unknown provider '{provider}'. "
            f"Known providers: {sorted(REGISTRY.keys())}"
        ) from None
