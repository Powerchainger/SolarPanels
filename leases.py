"""Lease claiming and heartbeat renewal.

Implements atomic account claiming via MongoDB findOneAndUpdate:
  - A worker claims an account by setting `lease = {worker_id, expires_at}`
    only if the current lease is null or expired.
  - The heartbeat coroutine renews `expires_at` periodically.
  - If a worker dies, other workers adopt its accounts: as soon as its
    heartbeat is seen to have stopped (`StaleLeases`), or at the latest when
    the leases expire.

Lease TTL is per-account: `interval_seconds * lease_ttl_multiplier`.

The worker id is unique per process (`new_worker_id`), so two processes never
treat the same lease as their own, whatever WORKER_ID they were given.

Load balancing: each worker records itself in the `workers` collection on
every heartbeat and claims at most its fair share of the enabled accounts,
`ceil(enabled / live workers)` (`fair_share`). The sync job releases what a
worker holds beyond that, so accounts spread out when workers are added.
Balanced by account count, not by calls per hour.
"""

from __future__ import annotations

import asyncio
import logging
import math
import secrets
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase
from pymongo.results import UpdateResult

from db.models import ApiConfig, Lease

logger = logging.getLogger(__name__)

# A lease whose holder and expiry stay unchanged for this many heartbeat
# renewal intervals belongs to a stopped worker (see StaleLeases).
STALE_AFTER_RENEWALS = 3


def new_worker_id(prefix: str = "") -> str:
    """This process's lease-owner id: `<prefix or hostname>-<random suffix>`.

    The suffix makes it unique per process, so replicas given the same
    WORKER_ID (`podman compose up --scale`) still hold separate leases. A
    restarted worker gets a new id; StaleLeases lets it, or any other worker,
    take over what its predecessor left behind.
    """
    return f"{prefix or socket.gethostname()}-{secrets.token_hex(4)}"


class StaleLeases:
    """Spots live-looking leases whose holder no longer renews them.

    A running holder's heartbeat moves `expires_at` every renewal interval.
    A lease seen with the same holder and `expires_at` for `stale_after`
    seconds belongs to a worker that crashed or was killed; it can be taken
    over now instead of after it expires (interval × LEASE_TTL_MULTIPLIER, up
    to 2 h for EnPhase). Observations are kept in memory between sync runs.
    """

    def __init__(self, stale_after: float) -> None:
        self.stale_after = timedelta(seconds=stale_after)
        # doc id -> (holder, expires_at, first seen with these values)
        self._seen: dict[str, tuple[str, datetime, datetime]] = {}

    def is_stale(self, doc_id: str, holder: str, expires_at: datetime, now: datetime) -> bool:
        """Record this sighting; True if the lease has not moved for `stale_after`."""
        seen = self._seen.get(doc_id)
        if seen is None or seen[:2] != (holder, expires_at):
            self._seen[doc_id] = (holder, expires_at, now)
            return False
        return now - seen[2] >= self.stale_after

    def keep_only(self, doc_ids: set[str]) -> None:
        """Forget leases not seen held by another worker in the latest pass."""
        for doc_id in self._seen.keys() - doc_ids:
            del self._seen[doc_id]


def _doc_id(provider: str, account_id: str) -> str:
    return f"{provider}::{account_id}"


def _lease_expiry(interval_seconds: int, ttl_multiplier: int) -> datetime:
    """Compute lease expiry from account interval and TTL multiplier."""
    return datetime.now(UTC) + timedelta(seconds=interval_seconds * ttl_multiplier)


async def register_worker(db: AsyncDatabase, worker_id: str) -> None:
    """Record that this worker is alive (every heartbeat tick, and at boot)."""
    await db.workers.update_one(
        {"_id": worker_id},
        {"$set": {"last_seen": datetime.now(UTC)}},
        upsert=True,
    )


async def unregister_worker(db: AsyncDatabase, worker_id: str) -> None:
    """Drop this worker from the registry (graceful shutdown)."""
    await db.workers.delete_one({"_id": worker_id})


async def fair_share(db: AsyncDatabase, *, alive_within: float) -> int:
    """How many accounts one worker should hold: ceil(enabled / live workers).

    A worker is live if it registered within `alive_within` seconds. The
    caller has registered itself first, so the count is at least one.
    """
    since = datetime.now(UTC) - timedelta(seconds=alive_within)
    workers = await db.workers.count_documents({"last_seen": {"$gte": since}})
    enabled = await db.api_configs.count_documents({"enabled": True})
    return math.ceil(enabled / max(workers, 1))


def _held_by(lease: dict[str, Any] | None, worker_id: str, now: datetime) -> bool:
    """Whether `lease` is an unexpired lease of `worker_id`."""
    if not lease or lease.get("worker_id") != worker_id:
        return False
    expires_at = lease.get("expires_at")
    if not isinstance(expires_at, datetime):
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at > now


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


async def take_over_account(
    db: AsyncDatabase,
    provider: str,
    account_id: str,
    worker_id: str,
    *,
    holder: str,
    holder_expires_at: datetime,
    interval_seconds: int,
    ttl_multiplier: int,
) -> bool:
    """Atomically take a live lease from a holder that stopped renewing it.

    Succeeds only if the lease still carries the holder and expiry that were
    found stale, so of several workers trying at once exactly one wins, and
    a holder that renewed in the meantime keeps its lease.
    """
    doc_id = _doc_id(provider, account_id)
    now = datetime.now(UTC)
    expiry = _lease_expiry(interval_seconds, ttl_multiplier)

    result = await db.api_configs.find_one_and_update(
        filter={
            "_id": doc_id,
            "lease.worker_id": holder,
            "lease.expires_at": holder_expires_at,
        },
        update={
            "$set": {
                "lease": {"worker_id": worker_id, "expires_at": expiry},
                "updated_at": now,
            },
        },
    )

    if result is None:
        logger.debug("takeover of %s lost: lease changed since it was seen", doc_id)
        return False

    logger.warning(
        "took over account %s from %s, which stopped renewing its lease",
        doc_id, holder,
    )
    return True


async def claim_leases(
    db: AsyncDatabase,
    worker_id: str,
    *,
    ttl_multiplier: int,
    stale: StaleLeases | None = None,
    limit: int | None = None,
) -> list[ApiConfig]:
    """Claim all unclaimed/expired accounts and return the ones this worker holds.

    Iterates over all enabled api_configs, attempting an atomic claim for each.
    Returns the accounts claimed now plus those this worker already held.
    With `stale`, live leases of other workers are watched across calls and
    taken over once their holder has stopped renewing them. With `limit`, new
    claims and takeovers stop once this worker holds that many; accounts it
    already holds are always returned, even beyond the limit.
    """
    now = datetime.now(UTC)
    claimed: list[ApiConfig] = []
    foreign: set[str] = set()

    docs = [d async for d in db.api_configs.find({"enabled": True})]
    owned = sum(1 for d in docs if _held_by(d.get("lease"), worker_id, now))
    room = math.inf if limit is None else limit - owned

    for raw_doc in docs:
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
                    owner = current_lease.get("worker_id")
                    if owner != worker_id:
                        # Active lease held by another worker: skip, unless
                        # that worker has stopped renewing it.
                        foreign.add(doc_id)
                        if stale is None or not stale.is_stale(
                            doc_id, owner, expires_at, now
                        ):
                            continue
                        if room <= 0:
                            continue
                        ok = await take_over_account(
                            db, raw_doc.get("provider", ""), raw_doc.get("account_id", ""),
                            worker_id,
                            holder=owner,
                            holder_expires_at=current_lease["expires_at"],
                            interval_seconds=raw_doc.get("interval_seconds", 300),
                            ttl_multiplier=ttl_multiplier,
                        )
                        if ok:
                            room -= 1
                            raw_doc.pop("_id", None)
                            try:
                                claimed.append(ApiConfig(**raw_doc))
                            except Exception:
                                logger.exception(
                                    "failed to parse taken-over api_config %s", doc_id
                                )
                        continue
                    # Our own lease — already claimed, will be renewed by heartbeat.
                    raw_doc.pop("_id", None)
                    try:
                        claimed.append(ApiConfig(**raw_doc))
                    except Exception:
                        logger.exception("failed to parse owned api_config %s", doc_id)
                    continue

        if room <= 0:
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
            room -= 1
            raw_doc.pop("_id", None)
            try:
                claimed.append(ApiConfig(**raw_doc))
            except Exception:
                logger.exception("failed to parse claimed api_config %s", doc_id)

    if stale is not None:
        stale.keep_only(foreign)
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
    alive_file: str | None = None,
) -> None:
    """Periodic heartbeat coroutine: renew leases for all claimed accounts.

    Runs in a loop until `stop_event` is set or the coroutine is cancelled.
    Sleeps `renewal_interval` seconds between ticks. Each tick touches
    `alive_file`, if given, for the container health check (healthcheck.py).
    """
    logger.info(
        "heartbeat started for worker %s (%d accounts, interval=%ds)",
        worker_id, len(accounts), renewal_interval,
    )

    while not (stop_event and stop_event.is_set()):
        if alive_file:
            try:
                Path(alive_file).touch()
            except OSError:
                logger.warning("could not touch %s; health check will fail", alive_file)
        try:
            await register_worker(db, worker_id)
        except Exception:
            logger.exception("could not register worker %s", worker_id)
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
