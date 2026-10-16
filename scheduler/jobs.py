"""APScheduler job definitions and scheduler bootstrap.

Three job types:
  1. **Fetch job** — one per enabled (provider, account_id).  Fetches raw
     data via the provider registry, runs the transform, writes
     RawResponse + ProcessedRecord docs, POSTs records, updates status.
  2. **Flush job** — picks up pending/failed/stale records and retries them.
  3. **Sync job** — re-claims leases and adds/removes/reschedules fetch jobs
     to match, so account changes apply without a restart.

Scheduler config (IMPL.md Step 9):
  - AsyncIOScheduler
  - max_instances=1, coalesce=True per fetch job
  - Staggered start: hash(account_id) % interval_seconds
  - Flush job runs on its own interval with per-worker jitter

MongoDB is accessed via the `db` (AsyncDatabase) parameter — no global state.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
    JobEvent,
    JobSubmissionEvent,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pymongo.asynchronous.database import AsyncDatabase

from clients.base import AuthFailedError, TimedClient
from clients.output import post_record
from leases import StaleLeases, claim_leases, fair_share, release_lease
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
# Records per flush pass per run (both passes: up to 2× this per run).
FLUSH_BATCH = 100

# How often a worker re-claims leases: picks up new or re-enabled accounts,
# changed intervals, and accounts a dead worker left behind.
DEFAULT_SYNC_INTERVAL_SECONDS = 60

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
    context: dict[str, Any] = {"timestamp": now, "account_id": account_id}

    # --- Fetch ---------------------------------------------------------------
    try:
        raw_responses = await spec.fetch(api_config, db, http_client)
    except AuthFailedError as exc:
        # Handled state, not an error to propagate. Freeze the account only
        # after AUTH_FAILURE_THRESHOLD rejections in a row.
        rejections = api_config.consecutive_auth_failures + 1
        freeze = rejections >= AUTH_FAILURE_THRESHOLD
        health_update: dict[str, Any] = {
            "$set": {"consecutive_auth_failures": rejections, "last_fetch_at": now},
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
    except Exception as exc:
        # One line, no traceback: the re-raise lets APScheduler mark the run
        # failed and log the traceback once ("Job ... raised an exception").
        logger.error(
            "fetch failed for %s (%d in a row): %s: %s",
            doc_id, api_config.consecutive_failures + 1, type(exc).__name__, exc,
        )
        await _record_health(
            db, doc_id,
            {"$set": {"last_fetch_at": now}, "$inc": {"consecutive_failures": 1}},
        )
        raise

    # The provider answered: this cycle counts as healthy even if it was empty.
    await _record_health(
        db,
        doc_id,
        {"$set": {
            "last_success_at": now,
            "last_fetch_at": now,
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
    except Exception as exc:
        # Traceback comes from APScheduler on the re-raise, as for fetch.
        logger.error("transform failed for %s: %s: %s", doc_id, type(exc).__name__, exc)
        raise

    # --- Write ProcessedRecords + POST ---------------------------------------
    for rec_dict in record_dicts:
        # Stamped now, not at cycle start: a slow cycle would otherwise insert
        # a record that flush pass 2 already sees as stale while we POST it.
        claimed_at = _claim_time()
        pr = ProcessedRecord(
            provider=provider,
            account_id=account_id,
            record=rec_dict,
            status=RecordStatus.POSTING,
            last_attempt_at=claimed_at,
            created_at=now,
        )
        # Python mode: dates stay BSON dates, so date queries (and a future TTL)
        # compare dates, not strings.
        pr_doc = pr.model_dump()
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

        await _write_outcome(
            db, pr_id, claimed_at, {"status": new_status, "last_error": last_error}
        )


# ---------------------------------------------------------------------------
# Flush job
# ---------------------------------------------------------------------------


def _claim_time() -> datetime:
    """Now, truncated to Mongo's millisecond precision.

    A claim stamps `last_attempt_at` with this value and the outcome write
    matches on it, so it must survive the round trip through BSON unchanged.
    """
    now = datetime.now(UTC)
    return now.replace(microsecond=now.microsecond // 1000 * 1000)


async def _write_outcome(
    db: AsyncDatabase, doc_id: Any, claimed_at: datetime, update: dict[str, Any]
) -> None:
    """Record a POST outcome, but only while our claim on the record holds.

    If the POST outlasted the stale threshold, another flush may have
    reclaimed the record (new `last_attempt_at`) and owns its outcome now;
    overwriting it could undo a `posted` or reset a retry count.
    """
    result = await db.processed_results.update_one(
        {"_id": doc_id, "status": RecordStatus.POSTING, "last_attempt_at": claimed_at},
        {"$set": update},
    )
    if result.matched_count == 0:
        logger.warning(
            "record %s was reclaimed during its POST, outcome %s not written",
            doc_id, update.get("status"),
        )


async def _claim_record(
    db: AsyncDatabase, query: dict[str, Any], update: dict[str, Any], seen: list[Any]
) -> dict[str, Any] | None:
    """Atomically take one matching record, or return None when none is left.

    The update moves the record out of `query` (status `posting`, or a fresh
    `last_attempt_at`), so a concurrent flush on another worker cannot take
    it too. `seen` excludes records this run already handled, so a record
    that fails again is not retried a second time within the same run.
    Returns the document as it was before the update.
    """
    doc = await db.processed_results.find_one_and_update(
        {**query, "_id": {"$nin": seen}}, update
    )
    if doc is not None:
        seen.append(doc["_id"])
    return doc


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

    Two passes, up to FLUSH_BATCH records each:
      1. status ∈ {pending, failed} AND retry_count < max_retries
         → claim (status posting, retry_count + 1), POST, update status.
      2. status = posting AND last_attempt_at < (now - stale_seconds)
         → claim (fresh last_attempt_at), POST — reclaims records left by a
         crashed worker.

    Each record is claimed atomically before its POST, so several workers
    can flush at once without sending a record twice. Each claim stamps
    `last_attempt_at` with its own time, and the outcome is only written
    while that stamp is still on the record. Delivery is still
    at-least-once: a crash between a successful POST and the status write
    leaves the record `posting`, and pass 2 sends it again.

    Records that exhaust retry_count stay as `failed` and are logged —
    they are never silently dropped.
    """
    now = datetime.now(UTC)
    seen: list[Any] = []

    # --- Pass 1: pending/failed with retries remaining -----------------------
    query_retriable = {
        "status": {"$in": [RecordStatus.PENDING, RecordStatus.FAILED]},
        "retry_count": {"$lt": max_retries},
    }

    retried = 0
    for _ in range(FLUSH_BATCH):
        # Stamp each claim with its own time, not the run's start: a record
        # claimed late in a long run must not already look stale to pass 2
        # of another worker's flush.
        claimed_at = _claim_time()
        claim_retriable = {
            "$set": {"status": RecordStatus.POSTING, "last_attempt_at": claimed_at},
            "$inc": {"retry_count": 1},
        }
        doc = await _claim_record(db, query_retriable, claim_retriable, seen)
        if doc is None:
            break
        doc_id = doc["_id"]
        new_retry_count = doc.get("retry_count", 0) + 1
        exhausted = new_retry_count >= max_retries

        result = await post_record(
            http_client,
            doc["record"],
            base_url=output_base_url,
            token=output_token,
        )

        update: dict[str, Any] = {}
        if result.success:
            update["status"] = RecordStatus.POSTED
            update["last_error"] = None
        else:
            update["last_error"] = result.error_message
            # Permanent errors fail at once; a transient one returns the
            # record to the state it was claimed from, until retries run out.
            permanent = result.error_class.value == "permanent"
            update["status"] = (
                RecordStatus.FAILED if permanent or exhausted else doc["status"]
            )
            if exhausted:
                logger.warning(
                    "record %s exhausted retries (%d), stays failed",
                    doc_id, new_retry_count,
                )

        await _write_outcome(db, doc_id, claimed_at, update)
        retried += 1

    if retried:
        logger.info("flush: retried %d record(s)", retried)

    # --- Pass 2: reclaim stale posting records -------------------------------
    stale_cutoff = now - timedelta(seconds=stale_seconds)
    query_stale = {
        "status": RecordStatus.POSTING,
        "last_attempt_at": {"$lt": stale_cutoff},
    }

    reclaimed = 0
    for _ in range(FLUSH_BATCH):
        claimed_at = _claim_time()
        claim_stale = {"$set": {"last_attempt_at": claimed_at}}
        doc = await _claim_record(db, query_stale, claim_stale, seen)
        if doc is None:
            break
        doc_id = doc["_id"]

        result = await post_record(
            http_client,
            doc["record"],
            base_url=output_base_url,
            token=output_token,
        )

        if result.success:
            update = {"status": RecordStatus.POSTED, "last_error": None}
        else:
            update = {"status": RecordStatus.FAILED, "last_error": result.error_message}

        await _write_outcome(db, doc_id, claimed_at, update)
        reclaimed += 1

    if reclaimed:
        logger.info("flush: reclaimed %d stale posting record(s)", reclaimed)


# ---------------------------------------------------------------------------
# Scheduler bootstrap
# ---------------------------------------------------------------------------


def stagger_seconds(account_id: str, interval: int) -> int:
    """Deterministic stagger offset: hash(account_id) % interval."""
    return int(hashlib.sha256(account_id.encode()).hexdigest(), 16) % interval


def first_run_at(acct: ApiConfig, now: datetime) -> datetime:
    """When a freshly started worker should first fetch this account.

    If the last fetch attempt was less than one interval ago, resume that
    rhythm (`last_fetch_at + interval`); otherwise `now + stagger`.
    Providers rate-limit by call frequency (Growatt answers
    `10012 error_frequently_access`), and without this every restart would
    fetch again soon after the previous worker's last call. Resuming also
    keeps the spread between accounts that the original stagger created.
    """
    interval = timedelta(seconds=acct.interval_seconds)
    last = acct.last_fetch_at
    if last is not None:
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)  # pymongo hands back naive UTC
        if last + interval > now:
            return last + interval
    return now + timedelta(seconds=stagger_seconds(acct.account_id, acct.interval_seconds))


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


class RunningJobs:
    """Tracks which scheduler jobs are running, so shutdown can wait for them.

    APScheduler 3's asyncio executor cancels running jobs on any
    `scheduler.shutdown()` (its `wait` flag is ignored), so a graceful stop
    pauses the scheduler, waits here, and only then shuts it down.

    Built on the scheduler's public job events. A submission carries its run
    times, and each run time ends in exactly one EXECUTED, ERROR or MISSED
    event. A cancelled submission reports a single ERROR; with
    `coalesce=True` (all jobs here) a submission has one run time, so that
    still balances.
    """

    def __init__(self, scheduler: AsyncIOScheduler) -> None:
        self._running: Counter[str] = Counter()
        self._idle = asyncio.Event()
        self._idle.set()
        scheduler.add_listener(self._on_submitted, EVENT_JOB_SUBMITTED)
        scheduler.add_listener(
            self._on_finished, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED
        )

    def _on_submitted(self, event: JobSubmissionEvent) -> None:
        self._running[event.job_id] += len(event.scheduled_run_times)
        self._idle.clear()

    def _on_finished(self, event: JobEvent) -> None:
        self._running[event.job_id] -= 1
        if self._running[event.job_id] <= 0:
            del self._running[event.job_id]
        if not self._running:
            self._idle.set()

    def job_ids(self) -> list[str]:
        return sorted(self._running)

    async def wait_idle(self, timeout: float, *, abort: asyncio.Event | None = None) -> bool:
        """Wait until no job is running, `timeout` runs out, or `abort` is set.

        Returns whether no job is running.
        """
        waiters = [asyncio.ensure_future(self._idle.wait())]
        if abort is not None:
            waiters.append(asyncio.ensure_future(abort.wait()))
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()
        return self._idle.is_set()


def _fetch_job_id(acct: ApiConfig) -> str:
    return f"fetch::{acct.provider}::{acct.account_id}"


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

    The first run is at `first_run_at`: one interval after the last fetch
    attempt if that is still ahead, else staggered by
    `stagger_seconds(account_id, interval)` to spread accounts that share
    an interval.
    """
    now = datetime.now(UTC)
    for acct in accounts:
        job_id = _fetch_job_id(acct)
        first_run = first_run_at(acct, now)

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
            next_run_time=first_run,
        )

        logger.info(
            "registered job %s (interval=%ds, first run in %ds)",
            job_id, acct.interval_seconds, (first_run - now).total_seconds(),
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


# ---------------------------------------------------------------------------
# Sync job
# ---------------------------------------------------------------------------


async def sync_accounts_job(
    scheduler: AsyncIOScheduler,
    claimed: list[ApiConfig],
    db: AsyncDatabase,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    worker_id: str,
    ttl_multiplier: int,
    stale: StaleLeases | None = None,
    alive_within: float | None = None,
    running: RunningJobs | None = None,
) -> None:
    """Bring this worker's fetch jobs in line with the leases it can hold.

    Claims free enabled accounts and, with `stale`, takes over those whose
    holder stopped renewing (claim_leases also returns the ones this worker
    already holds). With `alive_within`, claims stop at this worker's fair
    share and accounts held beyond it are released (`_release_surplus`).
    Then:
      - adds a fetch job for each newly held account,
      - reschedules a job whose account interval changed,
      - removes fetch jobs for accounts no longer held (deleted, disabled,
        or taken over by another worker).

    `claimed` is replaced in place: the heartbeat renews and shutdown
    releases exactly what is in it.
    """
    limit = None
    if alive_within is not None:
        limit = await fair_share(db, alive_within=alive_within)
    held = await claim_leases(
        db, worker_id, ttl_multiplier=ttl_multiplier, stale=stale, limit=limit,
    )
    if limit is not None and len(held) > limit:
        held = await _release_surplus(scheduler, held, limit, db, worker_id, running)
    held_ids = {_fetch_job_id(a): a for a in held}

    for job_id, acct in held_ids.items():
        job = scheduler.get_job(job_id)
        if job is None:
            add_account_jobs(
                scheduler, [acct], db, output_base_url, output_token, http_client,
                worker_id=worker_id,
            )
        elif job.trigger.interval != timedelta(seconds=acct.interval_seconds):
            scheduler.reschedule_job(
                job_id, trigger=IntervalTrigger(seconds=acct.interval_seconds)
            )
            logger.info("rescheduled %s (interval=%ds)", job_id, acct.interval_seconds)

    for job in scheduler.get_jobs():
        if job.id.startswith("fetch::") and job.id not in held_ids:
            scheduler.remove_job(job.id)
            logger.info("removed %s — account deleted, disabled or held elsewhere", job.id)

    claimed[:] = held


async def _release_surplus(
    scheduler: AsyncIOScheduler,
    held: list[ApiConfig],
    limit: int,
    db: AsyncDatabase,
    worker_id: str,
    running: RunningJobs | None,
) -> list[ApiConfig]:
    """Release the accounts held beyond `limit`; return the ones kept.

    Never releases an account whose fetch is running. Releases the ones with
    the latest next run first: their new holder resumes the rhythm from
    `last_fetch_at` (first_run_at), so the handover skips no cycle. The fetch
    job goes before the lease, so no fetch starts on a released account.
    """
    busy = set(running.job_ids()) if running else set()
    never = datetime.max.replace(tzinfo=UTC)

    def next_run(acct: ApiConfig) -> datetime:
        job = scheduler.get_job(_fetch_job_id(acct))
        return job.next_run_time if job and job.next_run_time else never

    candidates = sorted(
        (a for a in held if _fetch_job_id(a) not in busy), key=next_run, reverse=True,
    )
    release = candidates[: len(held) - limit]
    for acct in release:
        job_id = _fetch_job_id(acct)
        if scheduler.get_job(job_id) is not None:
            scheduler.remove_job(job_id)
        try:
            await release_lease(db, acct.provider, acct.account_id, worker_id)
        except Exception:
            # Unrenewed from now on, so another worker takes it over anyway.
            logger.exception("could not release %s while balancing", job_id)
    if release:
        logger.info(
            "released %d account(s) to balance load (fair share %d): %s",
            len(release), limit, ", ".join(_fetch_job_id(a) for a in release),
        )
    return [a for a in held if not any(a is r for r in release)]


def add_sync_job(
    scheduler: AsyncIOScheduler,
    claimed: list[ApiConfig],
    db: AsyncDatabase,
    output_base_url: str,
    output_token: str,
    http_client: TimedClient,
    *,
    worker_id: str,
    ttl_multiplier: int,
    stale: StaleLeases | None = None,
    alive_within: float | None = None,
    running: RunningJobs | None = None,
    sync_interval: int = DEFAULT_SYNC_INTERVAL_SECONDS,
) -> None:
    """Add the periodic lease re-claim job (see `sync_accounts_job`).

    `stale` must outlive the job's runs: it remembers what each run saw.
    """
    scheduler.add_job(
        sync_accounts_job,
        trigger=IntervalTrigger(seconds=sync_interval),
        id="sync",
        name="sync",
        kwargs={
            "scheduler": scheduler,
            "claimed": claimed,
            "db": db,
            "output_base_url": output_base_url,
            "output_token": output_token,
            "http_client": http_client,
            "worker_id": worker_id,
            "ttl_multiplier": ttl_multiplier,
            "stale": stale,
            "alive_within": alive_within,
            "running": running,
        },
        max_instances=1,
        coalesce=True,
    )
    logger.info("registered sync job (interval=%ds)", sync_interval)
