"""MongoDB connection and index setup.

Uses PyMongo's AsyncMongoClient (native async, no Motor needed).
Called once at service startup; creates indexes idempotently.

Collections:
  api_configs       Compound _id = (provider, account_id). One doc per account.
  raw_responses      Verbatim API bodies. TTL index deletes after RAW_TTL_DAYS.
  processed_results  One doc per output record. Status index for flush-job queries;
                     posted records expire after POSTED_TTL_DAYS.
"""

from __future__ import annotations

import logging
import math
import sys
from typing import Any
from urllib.parse import urlparse

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

logger = logging.getLogger(__name__)

COLLECTIONS = ["api_configs", "raw_responses", "processed_results"]

TTL_INDEX = "created_at_1"  # MongoDB's default name for the raw_responses TTL index
POSTED_TTL_INDEX = "posted_ttl"  # processed_results: deletes delivered records


async def get_db(uri: str, db_name: str = "solar_scraper") -> AsyncDatabase:
    """Return an AsyncDatabase handle. Probes connectivity before returning."""
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(uri)
    try:
        await client.admin.command("ping")
    except OperationFailure:
        logger.error(
            "MongoDB authentication failed — check MONGO_URI and the "
            "credentials in .env (MONGO_APP_USER / MONGO_APP_PASSWORD)"
        )
        sys.exit(1)
    except ConnectionFailure:
        host = urlparse(uri).hostname or uri
        logger.error(
            "MongoDB not reachable at %s — is the container running?", host
        )
        sys.exit(1)
    return client[db_name]


async def _ensure_ttl_index(
    coll: Any, name: str, seconds: int | None, **options: Any
) -> None:
    """Create, change or (with seconds=None) remove a TTL index on created_at.

    create_index refuses to change an existing index's options
    (IndexOptionsConflict), and collMod needs more than the app user's
    readWrite role, so a changed TTL rebuilds the index instead.
    """
    existing = (await coll.index_information()).get(name)
    if existing is not None and existing.get("expireAfterSeconds") != seconds:
        try:
            await coll.drop_index(name)
        except OperationFailure:
            pass  # another worker dropped it first
        logger.info("%s: TTL changed, dropped index %s", coll.name, name)
    if seconds is not None:
        await coll.create_index(
            "created_at", name=name, expireAfterSeconds=seconds, **options
        )


async def init_indexes(
    db: AsyncDatabase, ttl_days: int = 30, posted_ttl_days: float = 7
) -> None:
    """Create all required indexes. Idempotent — safe to call on every startup.

    Indexes created:
      raw_responses.created_at   TTL (expires after ttl_days)
      processed_results.created_at  TTL on posted records only (after
                                    posted_ttl_days; none if infinite)
      processed_results.status+retry_count  Compound for flush-job selector
      processed_results.last_attempt_at     For stale-posting reclamation
    """
    await _ensure_ttl_index(db.raw_responses, TTL_INDEX, ttl_days * 86400)

    pr = db.processed_results
    # Partial: undelivered records are never deleted, however old.
    await _ensure_ttl_index(
        pr,
        POSTED_TTL_INDEX,
        None if math.isinf(posted_ttl_days) else int(posted_ttl_days * 86400),
        partialFilterExpression={"status": "posted"},
    )
    await pr.create_index([("status", ASCENDING), ("retry_count", ASCENDING)])
    await pr.create_index("last_attempt_at")

    logger.info(
        "MongoDB indexes ensured (raw TTL=%dd, posted TTL=%s)",
        ttl_days, "never" if math.isinf(posted_ttl_days) else f"{posted_ttl_days:g}d",
    )
