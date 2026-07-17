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
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable

from db.models import (
    ApiConfig,
    EnphaseCredentials,
    GrowattCredentials,
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


async def _growatt_fetch(
    cfg: ApiConfig,
    _db: AsyncDatabase,
    _client: TimedClient,
) -> list[tuple[str, dict[str, Any]]]:
    """Fetch a full cycle for one Growatt account."""
    from clients.growatt import fetch_cycle

    creds = cfg.credentials
    assert isinstance(creds, GrowattCredentials)

    return await fetch_cycle(
        creds.api_token,
        device_type=7,
    )


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
