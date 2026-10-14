"""Lease claiming and heartbeat renewal.

Implements atomic account claiming via MongoDB findOneAndUpdate:
  - A worker claims an account by setting `lease = {worker_id, expires_at}`
    only if the current lease is null or expired.
  - The heartbeat coroutine renews `expires_at` periodically.
  - If a worker dies, its accounts' leases expire and other workers adopt them.

Lease TTL is per-account: `interval_seconds * lease_ttl_multiplier`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.results import UpdateResult

from db.models import ApiConfig, Lease

logger = logging.getLogger(__name__)


def _doc_id(provider: str, account_id: str) -> str:
    return f"{provider}::{account_id}"


def _lease_expiry(interval_seconds: int, ttl_multiplier: int) -> datetime:
    """Compute lease expiry from account interval and TTL multiplier."""
    return datetime.now(UTC) + timedelta(seconds=interval_seconds * ttl_multiplier)


async def claim_account(
    db: AsyncDatabase,
    provider: str,
    account_id: str,
    worker_id: str,
    *,
    interval_seconds: int,
    ttl_multiplier: int,
) -> bool:
    """Atomically claim one account for a worker.

    Returns True if the claim succeeded (this worker now owns the account),
    False if another active worker already holds it.

    Atomic condition:
      lease == null  OR  lease.expires_at < now
    """
    doc_id = _doc_id(provider, account_id)
    now = datetime.now(UTC)
    expiry = _lease_expiry(interval_seconds, ttl_multiplier)

    lease_doc = {"worker_id": worker_id, "expires_at": expiry}

    result = await db.api_configs.find_one_and_update(
        filter={
            "_id": doc_id,
            "$or": [
                {"lease": None},
                {"lease.expires_at": {"$lt": now}},
            ],
        },
        update={"$set": {"lease": lease_doc, "updated_at": now}},
    )

    if result is None:
        logger.debug("account %s already claimed by another worker", doc_id)
        return False

    logger.info("claimed account %s (lease until %s)", doc_id, expiry.isoformat())
    return True


async def claim_leases(
    db: AsyncDatabase,
    worker_id: str,
    *,
    ttl_multiplier: int,
) -> list[ApiConfig]:
    """Claim all unclaimed/expired accounts and return the successfully claimed ones.

    Iterates over all enabled api_configs, attempting an atomic claim for each.
    Only accounts where the claim succeeds are returned.
    """
    now = datetime.now(UTC)
    claimed: list[ApiConfig] = []

    cursor = db.api_configs.find({"enabled": True})
    async for raw_doc in cursor:
        doc_id = raw_doc.get("_id", "")
        # Parse lease to check current state.
        current_lease = raw_doc.get("lease")
        if current_lease is not None:
            expires_at = current_lease.get("expires_at")
            if isinstance(expires_at, datetime):
                # MongoDB may return naive datetimes; treat as UTC.
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=UTC)
                if expires_at > now:
                    # Active lease held by another worker — skip.
                    owner = current_lease.get("worker_id")
                    if owner != worker_id:
                        continue
                    # Our own lease — already claimed, will be renewed by heartbeat.
                    raw_doc.pop("_id", None)
                    try:
                        claimed.append(ApiConfig(**raw_doc))
                    except Exception:
                        logger.exception("failed to parse owned api_config %s", doc_id)
                    continue

        provider = raw_doc.get("provider", "")
        account_id = raw_doc.get("account_id", "")
        interval_seconds = raw_doc.get("interval_seconds", 300)

        ok = await claim_account(
            db, provider, account_id, worker_id,
            interval_seconds=interval_seconds,
            ttl_multiplier=ttl_multiplier,
        )
        if ok:
            raw_doc.pop("_id", None)
            try:
                claimed.append(ApiConfig(**raw_doc))
            except Exception:
                logger.exception("failed to parse claimed api_config %s", doc_id)

    # Debug: the sync job calls this every minute; new claims log above.
    logger.debug("worker %s holds %d account(s)", worker_id, len(claimed))
    return claimed


async def renew_lease(
    db: AsyncDatabase,
    provider: str,
    account_id: str,
    worker_id: str,
    *,
    interval_seconds: int,
    ttl_multiplier: int,
) -> bool:
    """Renew the lease on one account (heartbeat tick).

    Only succeeds if the current lease is held by `worker_id`.
    Returns True if renewed, False if the lease was stolen or expired.
    """
    doc_id = _doc_id(provider, account_id)
    now = datetime.now(UTC)
    expiry = _lease_expiry(interval_seconds, ttl_multiplier)

    result: UpdateResult = await db.api_configs.update_one(
        filter={
            "_id": doc_id,
            "lease.worker_id": worker_id,
        },
        update={
            "$set": {
                "lease.expires_at": expiry,
                "updated_at": now,
            },
        },
    )

    if result.modified_count == 0:
        logger.warning("lease renewal failed for %s — no longer owner", doc_id)
        return False

    return True


async def release_lease(
    db: AsyncDatabase,
    provider: str,
    account_id: str,
    worker_id: str,
) -> None:
    """Release the lease on one account (graceful shutdown)."""
    doc_id = _doc_id(provider, account_id)

    await db.api_configs.update_one(
        filter={
            "_id": doc_id,
            "lease.worker_id": worker_id,
        },
        update={"$set": {"lease": None}},
    )

    logger.info("released lease on %s", doc_id)


async def heartbeat(
    db: AsyncDatabase,
    worker_id: str,
    accounts: list[ApiConfig],
    *,
    ttl_multiplier: int,
    renewal_interval: int = 30,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Periodic heartbeat coroutine: renew leases for all claimed accounts.

    Runs in a loop until `stop_event` is set or the coroutine is cancelled.
    Sleeps `renewal_interval` seconds between ticks.
    """
    logger.info(
        "heartbeat started for worker %s (%d accounts, interval=%ds)",
        worker_id, len(accounts), renewal_interval,
    )

    while not (stop_event and stop_event.is_set()):
        # Mutating `accounts` while iterating it would skip the account after
        # each removal, so collect the lost ones and drop them after the loop.
        lost: list[ApiConfig] = []
        for acct in list(accounts):
            try:
                ok = await renew_lease(
                    db, acct.provider, acct.account_id, worker_id,
                    interval_seconds=acct.interval_seconds,
                    ttl_multiplier=ttl_multiplier,
                )
                if not ok:
                    logger.warning(
                        "lost lease on %s::%s — removing from active accounts",
                        acct.provider, acct.account_id,
                    )
                    lost.append(acct)
            except Exception:
                logger.exception(
                    "heartbeat error for %s::%s",
                    acct.provider, acct.account_id,
                )

        # In place: main.py releases whatever is left in its `claimed` list at shutdown.
        accounts[:] = [a for a in accounts if not any(a is x for x in lost)]

        try:
            await asyncio.wait_for(
                stop_event.wait() if stop_event else asyncio.sleep(renewal_interval),
                timeout=renewal_interval,
            )
        except asyncio.TimeoutError:
            continue  # normal timeout — next tick
        except asyncio.CancelledError:
            break

    logger.info("heartbeat stopped for worker %s", worker_id)
