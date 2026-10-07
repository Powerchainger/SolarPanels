"""APScheduler job definitions and scheduler bootstrap.

Two job types:
  1. **Fetch job** — one per enabled (provider, account_id).  Fetches raw
     data via the provider registry, runs the transform, writes
     RawResponse + ProcessedRecord docs, POSTs records, updates status.
  2. **Flush job** — picks up pending/failed/stale records and retries them.

Scheduler config (IMPL.md Step 9):
  - AsyncIOScheduler
  - max_instances=1, coalesce=True per fetch job
  - Staggered start: hash(account_id) % interval_seconds
  - Flush job runs on its own interval with per-worker jitter

MongoDB is accessed via the `db` (AsyncDatabase) parameter — no global state.
"""

from __future__ import annotations

import hashlib
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pymongo.asynchronous.database import AsyncDatabase

from clients.base import AuthFailedError, TimedClient
from clients.output import post_record
from db.models import (
    ApiConfig,
    ProcessedRecord,
    RawResponse,
    RecordStatus,
)
from providers import get_spec

logger = logging.getLogger(__name__)

# Flush job defaults.
DEFAULT_MAX_RETRIES = 5
DEFAULT_FLUSH_INTERVAL_SECONDS = 300
DEFAULT_STALE_THRESHOLD_SECONDS = 300  # 5 minutes

# Consecutive auth rejections before an account is frozen. One glitch (a
# proxy, a briefly wrong response) must not stop collection for good.
AUTH_FAILURE_THRESHOLD = 3


async def _record_health(
    db: AsyncDatabase, doc_id: str, update: dict[str, Any], *, extra_filter: dict | None = None
) -> bool:
    """Best-effort health write: never lets bookkeeping fail a cycle.

    Returns False if the write raised or (with `extra_filter`) matched nothing.
    """
    try:
        result = await db.api_configs.update_one(
            {"_id": doc_id, **(extra_filter or {})}, update
        )
    except Exception:  # noqa: BLE001
        logger.warning("could not record health for %s", doc_id, exc_info=True)
        return False
    return result.matched_count != 0


# ---------------------------------------------------------------------------
# Fetch job
# ---------------------------------------------------------------------------


async def fetch_job(
    db: AsyncDatabase,
    provider: str,
    account_id: str,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    worker_id: str = "",
) -> None:
    """Run a single fetch cycle for one provider/account.

    1. Read api_config (fresh credentials) from Mongo.
    2. Skip if this worker no longer holds the lease.
    3. Dispatch via providers.py registry (no branching).
    4. Call provider's fetch function.
    5. Write RawResponse docs (if store_raw).
    6. Call provider's transform function.
    7. Write ProcessedRecord docs (status=pending).
    8. POST each record; update status.
    """
    spec = get_spec(provider)

    # Refresh api_config from Mongo (credentials may have been rotated).
    doc_id = f"{provider}::{account_id}"
    raw_doc = await db.api_configs.find_one({"_id": doc_id})
    if raw_doc is None:
        logger.warning("api_config %s not found, skipping cycle", doc_id)
        return

    # --- Lease check -------------------------------------------------------
    if worker_id:
        lease = raw_doc.get("lease")
        if lease is None or lease.get("worker_id") != worker_id:
            logger.info("skipping %s — not lease holder", doc_id)
            return

    raw_doc.pop("_id", None)
    api_config = ApiConfig(**raw_doc)

    if not api_config.enabled:
        logger.debug("account %s disabled, skipping", doc_id)
        return

    if api_config.auth_failed:
        logger.warning("skipping %s — needs re-authorization (see manage_keys.py list)", doc_id)
        return

    now = datetime.now(UTC)
    context: dict[str, Any] = {"timestamp": now}

    # --- Fetch ---------------------------------------------------------------
    try:
        raw_responses = await spec.fetch(api_config, db, http_client)
    except AuthFailedError as exc:
        # Handled state, not an error to propagate. Freeze the account only
        # after AUTH_FAILURE_THRESHOLD rejections in a row.
        rejections = api_config.consecutive_auth_failures + 1
        freeze = rejections >= AUTH_FAILURE_THRESHOLD
        health_update: dict[str, Any] = {
            "$set": {"consecutive_auth_failures": rejections},
            "$inc": {"consecutive_failures": 1},
        }
        if freeze:
            health_update["$set"]["auth_failed"] = True
            logger.error(
                "%s — account %s is now skipped until re-seeded", exc, doc_id
            )
        else:
            logger.warning(
                "%s — %s (%d/%d before it is skipped)",
                exc, doc_id, rejections, AUTH_FAILURE_THRESHOLD,
            )
        # Only if the account has not been re-seeded since this cycle read it:
        # a stale result must not undo a fresh re-seed. `seeded_at` changes
        # only on a re-seed, not on a token refresh or lease heartbeat.
        if not await _record_health(
            db, doc_id, health_update, extra_filter={"seeded_at": raw_doc.get("seeded_at")}
        ):
            logger.info("auth failure for %s not recorded (re-seeded meanwhile)", doc_id)
        return
    except Exception:
        logger.exception("fetch failed for %s", doc_id)
        await _record_health(db, doc_id, {"$inc": {"consecutive_failures": 1}})
        raise

    # The provider answered: this cycle counts as healthy even if it was empty.
    await _record_health(
        db,
        doc_id,
        {"$set": {
            "last_success_at": now,
            "consecutive_failures": 0,
            "consecutive_auth_failures": 0,
        }},
    )

    if not raw_responses:
        logger.debug("empty fetch result for %s", doc_id)
        return

    # Re-read api_config after fetch — provider may have updated site_cache,
    # credentials, etc. in Mongo (e.g. EnPhase resolve_site_id runs during fetch).
    fresh_doc = await db.api_configs.find_one({"_id": doc_id})
    if fresh_doc:
        fresh_doc.pop("_id", None)
        api_config = ApiConfig(**fresh_doc)

    # --- Store raw responses (optional) --------------------------------------
    if api_config.store_raw:
        for label, raw_body in raw_responses:
            rr = RawResponse(
                provider=provider,
                account_id=account_id,
                body={"label": label, "data": raw_body},
                created_at=now,
            )
            # Python mode, not JSON: the TTL index on created_at only expires
            # a BSON date, never an ISO string.
            await db.raw_responses.insert_one(rr.model_dump())

    # --- Transform -----------------------------------------------------------
    try:
        record_dicts = spec.transform(raw_responses, context)
    except Exception:
        logger.exception("transform failed for %s", doc_id)
        raise

    # --- Write ProcessedRecords + POST ---------------------------------------
    for rec_dict in record_dicts:
        pr = ProcessedRecord(
            provider=provider,
            account_id=account_id,
            record=rec_dict,
            status=RecordStatus.POSTING,
            last_attempt_at=now,
            created_at=now,
        )
        pr_doc = pr.model_dump(mode="json")
        insert_result = await db.processed_results.insert_one(pr_doc)
        pr_id = insert_result.inserted_id
        pr_doc["_id"] = pr_id

        result = await post_record(
            http_client,
            pr_doc["record"],
            base_url=output_base_url,
            token=output_token,
        )

        # Persist the post outcome — use PostResult to determine wrapper status.
        if result.success:
            new_status = RecordStatus.POSTED
            last_error = None
        elif result.error_class.value == "permanent":
            new_status = RecordStatus.FAILED
            last_error = result.error_message
        else:
            # Transient: stays posting for next flush attempt.
            new_status = RecordStatus.POSTING
            last_error = result.error_message

        update: dict[str, Any] = {
            "status": new_status,
            "last_error": last_error,
            "last_attempt_at": now.isoformat(),
            "retry_count": 0,
        }

        await db.processed_results.update_one(
            {"_id": pr_id},
            {"$set": update},
        )


# ---------------------------------------------------------------------------
# Flush job
# ---------------------------------------------------------------------------


async def flush_job(
    db: AsyncDatabase,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    stale_seconds: float = DEFAULT_STALE_THRESHOLD_SECONDS,
) -> None:
    """Pick up unposted records and retry them.

    Two queries:
      1. status ∈ {pending, failed} AND retry_count < max_retries
         → increment retry_count, POST, update status.
      2. status = posting AND last_attempt_at < (now - stale_seconds)
         → reclaim stale records left by a crashed worker.

    Records that exhaust retry_count stay as `failed` and are logged —
    they are never silently dropped.
    """
    now = datetime.now(UTC)

    # --- Pass 1: pending/failed with retries remaining -----------------------
    query_retriable = {
        "status": {"$in": [RecordStatus.PENDING, RecordStatus.FAILED]},
        "retry_count": {"$lt": max_retries},
    }

    cursor = db.processed_results.find(query_retriable).limit(100)
    retried = 0
    async for doc in cursor:
        doc_id = doc["_id"]
        new_retry_count = doc.get("retry_count", 0) + 1

        result = await post_record(
            http_client,
            doc["record"],
            base_url=output_base_url,
            token=output_token,
        )

        update: dict[str, Any] = {
            "retry_count": new_retry_count,
            "last_attempt_at": now.isoformat(),
        }

        if result.success:
            update["status"] = RecordStatus.POSTED
            update["last_error"] = None
        elif result.error_class.value == "permanent":
            update["status"] = RecordStatus.FAILED
            update["last_error"] = result.error_message
            if new_retry_count >= max_retries:
                logger.warning(
                    "record %s exhausted retries (%d), stays failed",
                    doc_id, new_retry_count,
                )
        else:
            # Transient exhausted at this level — stays failed-like for next flush.
            update["last_error"] = result.error_message
            if new_retry_count >= max_retries:
                update["status"] = RecordStatus.FAILED
                logger.warning(
                    "record %s exhausted retries (%d), stays failed",
                    doc_id, new_retry_count,
                )

        await db.processed_results.update_one({"_id": doc_id}, {"$set": update})
        retried += 1

    if retried:
        logger.info("flush: retried %d record(s)", retried)

    # --- Pass 2: reclaim stale posting records -------------------------------
    stale_cutoff = now - timedelta(seconds=stale_seconds)
    query_stale = {
        "status": RecordStatus.POSTING,
        "last_attempt_at": {"$lt": stale_cutoff.isoformat()},
    }

    stale_cursor = db.processed_results.find(query_stale).limit(100)
    reclaimed = 0
    async for doc in stale_cursor:
        doc_id = doc["_id"]

        result = await post_record(
            http_client,
            doc["record"],
            base_url=output_base_url,
            token=output_token,
        )

        update = {"last_attempt_at": now.isoformat()}
        if result.success:
            update["status"] = RecordStatus.POSTED
            update["last_error"] = None
        else:
            update["status"] = RecordStatus.FAILED
            update["last_error"] = result.error_message

        await db.processed_results.update_one({"_id": doc_id}, {"$set": update})
        reclaimed += 1

    if reclaimed:
        logger.info("flush: reclaimed %d stale posting record(s)", reclaimed)


# ---------------------------------------------------------------------------
# Scheduler bootstrap
# ---------------------------------------------------------------------------


def stagger_seconds(account_id: str, interval: int) -> int:
    """Deterministic stagger offset: hash(account_id) % interval."""
    return int(hashlib.sha256(account_id.encode()).hexdigest(), 16) % interval


def build_scheduler(
    accounts: list[ApiConfig],
    *,
    flush_interval: int = DEFAULT_FLUSH_INTERVAL_SECONDS,
) -> AsyncIOScheduler:
    """Create and configure the scheduler (does NOT start it).

    Jobs are added but the scheduler is returned in a stopped state so the
    caller can wire up signal handlers and start it explicitly.

    Args:
        accounts:       List of enabled ApiConfig docs (claimed by this worker).
        flush_interval: Seconds between flush job runs.
    """
    scheduler = AsyncIOScheduler(
        job_defaults={
            "misfire_grace_time": 60,
        }
    )
    return scheduler


def add_account_jobs(
    scheduler: AsyncIOScheduler,
    accounts: list[ApiConfig],
    db: AsyncDatabase,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    worker_id: str = "",
) -> None:
    """Add one fetch job per enabled account to the scheduler.

    Each job is staggered by `stagger_seconds(account_id, interval)` to
    spread load across accounts that share the same polling interval.
    """
    for acct in accounts:
        job_id = f"fetch::{acct.provider}::{acct.account_id}"
        stagger = stagger_seconds(acct.account_id, acct.interval_seconds)

        scheduler.add_job(
            fetch_job,
            trigger=IntervalTrigger(seconds=acct.interval_seconds),
            id=job_id,
            name=job_id,
            kwargs={
                "db": db,
                "provider": acct.provider,
                "account_id": acct.account_id,
                "output_base_url": output_base_url,
                "output_token": output_token,
                "http_client": http_client,
                "worker_id": worker_id,
            },
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            next_run_time=datetime.now(UTC) + timedelta(seconds=stagger),
        )

        logger.info(
            "registered job %s (interval=%ds, stagger=%ds)",
            job_id, acct.interval_seconds, stagger,
        )


def add_flush_job(
    scheduler: AsyncIOScheduler,
    db: AsyncDatabase,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    flush_interval: int = DEFAULT_FLUSH_INTERVAL_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> None:
    """Add the periodic flush job with per-worker jitter.

    Jitter prevents all replicas from thundering-herd on the same stale
    records at the same instant.
    """
    jitter = random.uniform(5, 30)  # noqa: S311 — not security-sensitive

    scheduler.add_job(
        flush_job,
        trigger=IntervalTrigger(seconds=flush_interval, jitter=int(jitter)),
        id="flush",
        name="flush",
        kwargs={
            "db": db,
            "output_base_url": output_base_url,
            "output_token": output_token,
            "http_client": http_client,
            "max_retries": max_retries,
        },
        max_instances=1,
        coalesce=True,
    )

    logger.info(
        "registered flush job (interval=%ds, jitter=%.1fs)", flush_interval, jitter,
    )
