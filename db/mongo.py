"""MongoDB connection and index setup.

Uses PyMongo's AsyncMongoClient (native async, no Motor needed).
Called once at service startup; creates indexes idempotently.

Collections:
  api_configs       Compound _id = (provider, account_id). One doc per account.
  raw_responses      Verbatim API bodies. TTL index deletes after RAW_TTL_DAYS.
  processed_results  One doc per output record. Status index for flush-job queries.
"""

from __future__ import annotations

import logging
from typing import Any

from pymongo import ASCENDING, DESCENDING
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

logger = logging.getLogger(__name__)

COLLECTIONS = ["api_configs", "raw_responses", "processed_results"]


async def get_db(uri: str, db_name: str = "solar_scraper") -> AsyncDatabase:
    """Return an AsyncDatabase handle. Connection is established lazily on first op."""
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(uri)
    return client[db_name]


async def init_indexes(db: AsyncDatabase, ttl_days: int = 30) -> None:
    """Create all required indexes. Idempotent — safe to call on every startup.

    Indexes created:
      raw_responses.created_at   TTL (expires after ttl_days)
      processed_results.status+retry_count  Compound for flush-job selector
      processed_results.last_attempt_at     For stale-posting reclamation
    """
    rc = db.raw_responses
    await rc.create_index("created_at", expireAfterSeconds=ttl_days * 86400)

    pr = db.processed_results
    await pr.create_index([("status", ASCENDING), ("retry_count", ASCENDING)])
    await pr.create_index("last_attempt_at")

    logger.info("MongoDB indexes ensured (TTL=%dd)", ttl_days)
