"""Solar-scraper service entrypoint.

Boot sequence:
  1. Load config from environment.
  2. Connect to MongoDB, ensure indexes, probe the output API (exits
     before any lease is claimed if it is unreachable).
  3. Register in the `workers` collection and claim up to this worker's
     fair share of the accounts, under a per-process worker id (WORKER_ID
     or the hostname, plus a random suffix). The sync job re-checks every
     minute: it picks up accounts added later or left behind by a stopped
     worker, and releases accounts beyond the share when workers join.
  4. Start heartbeat renewal coroutine.
  5. Set default executor for growattServer.to_thread calls.
  6. Build scheduler from claimed accounts.
  7. Install SIGTERM/SIGINT handlers for graceful shutdown.
  8. Run until a signal arrives, then `shutdown_gracefully`.

Usage:
    uv run python main.py
    # or via Docker (see Dockerfile)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from config import Settings
from healthcheck import ALIVE_FILE
from db.mongo import get_db, init_indexes
from leases import (
    STALE_AFTER_RENEWALS,
    StaleLeases,
    claim_leases,
    fair_share,
    heartbeat,
    new_worker_id,
    register_worker,
    release_lease,
    unregister_worker,
)
from scheduler.jobs import (
    RunningJobs,
    add_account_jobs,
    add_flush_job,
    add_sync_job,
    build_scheduler,
)

logger = logging.getLogger("solar-scraper")


async def _main() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # httpx logs every request at INFO. TimedClient (clients/base.py) already
    # logs failed responses at WARNING and the rest at DEBUG, and
    # clients.output logs each POST outcome at INFO, so that line is noise.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    worker_id = new_worker_id(settings.worker_id)
    logger.info("Starting solar-scraper worker %s", worker_id)

    # --- MongoDB --------------------------------------------------------
    db = await get_db(settings.mongo_uri)
    await init_indexes(
        db, ttl_days=settings.raw_ttl_days, posted_ttl_days=settings.posted_ttl_days
    )

    # --- Probe output API ----------------------------------------------
    # Before claiming leases: exiting here holds nothing, so the restart
    # policy can retry without blocking the accounts for interval × 2.
    output_base_url = settings.output_api_url
    output_token = settings.output_api_token
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as probe:
            await probe.get(output_base_url)
    except httpx.TransportError as exc:
        # Any HTTP status means reachable; only transport failures (refused,
        # connect/read timeout, DNS) mean it is not.
        logger.error(
            "Output API not reachable at %s (%s) — is the container running?",
            output_base_url, type(exc).__name__,
        )
        sys.exit(1)

    # --- Claim leases ---------------------------------------------------
    # Registered first, so the fair share already counts this worker. A
    # worker counts as alive as long as a stopped one's leases look live.
    alive_within = STALE_AFTER_RENEWALS * settings.lease_renewal_interval
    await register_worker(db, worker_id)
    claimed = await claim_leases(
        db,
        worker_id,
        ttl_multiplier=settings.lease_ttl_multiplier,
        limit=await fair_share(db, alive_within=alive_within),
    )

    if claimed:
        logger.info(
            "claimed %d account(s): %s",
            len(claimed),
            ", ".join(f"{a.provider}/{a.account_id}" for a in claimed),
        )
    else:
        # Stay up: the sync job claims accounts as they are added or freed.
        logger.warning("no accounts claimed yet — checking again every minute")

    # --- Set default executor for to_thread calls -----------------------
    loop = asyncio.get_running_loop()
    loop.set_default_executor(
        ThreadPoolExecutor(max_workers=settings.threadpool_workers)
    )

    # --- Build scheduler ------------------------------------------------
    scheduler = build_scheduler(claimed)

    # We need an http_client for fetch + flush jobs. Import here to keep
    # the startup sequence readable.
    from clients.base import TimedClient

    http_client = TimedClient()

    add_account_jobs(
        scheduler,
        claimed,
        db=db,
        output_base_url=output_base_url,
        output_token=output_token,
        http_client=http_client,
        worker_id=worker_id,
    )

    add_flush_job(
        scheduler,
        db=db,
        output_base_url=output_base_url,
        output_token=output_token,
        http_client=http_client,
    )

    running_jobs = RunningJobs(scheduler)

    add_sync_job(
        scheduler,
        claimed,
        db=db,
        output_base_url=output_base_url,
        output_token=output_token,
        http_client=http_client,
        worker_id=worker_id,
        ttl_multiplier=settings.lease_ttl_multiplier,
        stale=StaleLeases(alive_within),
        alive_within=alive_within,
        running=running_jobs,
    )

    # --- Heartbeat coroutine --------------------------------------------
    # Kept running during shutdown until in-flight jobs are done, so their
    # leases stay renewed while they finish.
    heartbeat_stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat(
            db,
            worker_id,
            claimed,
            ttl_multiplier=settings.lease_ttl_multiplier,
            renewal_interval=settings.lease_renewal_interval,
            stop_event=heartbeat_stop,
            alive_file=ALIVE_FILE,
        )
    )

    # --- Signal handlers ------------------------------------------------
    # The handler only flags the request; the awaited sequence below does
    # the work, so nothing is left to un-awaited tasks. A second signal skips
    # the wait for running jobs.
    shutdown_requested = asyncio.Event()
    force_shutdown = asyncio.Event()

    def _request_shutdown() -> None:
        if not shutdown_requested.is_set():
            logger.info("shutdown signal received (send again to skip waiting for jobs)")
            shutdown_requested.set()
        elif not force_shutdown.is_set():
            logger.warning("second shutdown signal — cancelling running jobs now")
            force_shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _request_shutdown)

    # --- Start ----------------------------------------------------------
    scheduler.start()
    logger.info(
        "scheduler started — %d job(s) registered",
        len(scheduler.get_jobs()),
    )

    await shutdown_requested.wait()

    async def release_all() -> None:
        # `claimed` is kept current in place: pruned by the heartbeat when a
        # lease is lost, replaced by the sync job (paused by now).
        results = await asyncio.gather(
            *(
                release_lease(db, acct.provider, acct.account_id, worker_id)
                for acct in claimed
            ),
            return_exceptions=True,
        )
        for acct, result in zip(claimed, results):
            if isinstance(result, BaseException):
                logger.error(
                    "could not release lease on %s::%s (it expires on its own): %r",
                    acct.provider, acct.account_id, result,
                )
        try:
            await unregister_worker(db, worker_id)
        except Exception as exc:
            logger.error(
                "could not unregister worker %s (counted for another %ss): %r",
                worker_id, alive_within, exc,
            )

    await shutdown_gracefully(
        scheduler=scheduler,
        running_jobs=running_jobs,
        heartbeat_stop=heartbeat_stop,
        heartbeat_task=heartbeat_task,
        release_leases=release_all,
        http_client=http_client,
        timeout=settings.shutdown_timeout_seconds,
        force=force_shutdown,
    )
    logger.info("solar-scraper stopped")


# Budgets after the wait for running jobs. With the default timeout, the
# whole shutdown needs SHUTDOWN_TIMEOUT_SECONDS + both of these.
UNWIND_SECONDS = 5    # for cancelled jobs to actually stop
CLEANUP_SECONDS = 5   # for stopping the heartbeat and releasing leases


async def shutdown_gracefully(
    *,
    scheduler: Any,
    running_jobs: RunningJobs,
    heartbeat_stop: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
    release_leases: Callable[[], Awaitable[None]],
    http_client: Any,
    timeout: float,
    force: asyncio.Event | None = None,
) -> None:
    """Stop in an order that lets running jobs finish.

    1. Pause the scheduler: no new runs start, running ones continue.
    2. Wait up to `timeout` (or until `force` is set) for running jobs; an
       EnPhase credential write interrupted here can lose a rotated refresh
       token.
    3. Shut the scheduler down, which cancels whatever is still running, and
       give cancelled jobs UNWIND_SECONDS to stop: cancellation lands on a
       later loop iteration, and their leases must not be released first.
    4. Stop the heartbeat, then release the leases, so no renewal lands
       after a release. Bounded by CLEANUP_SECONDS; leases not released in
       time expire on their own.
    5. Close the shared HTTP client.

    Steps 4 and 5 run even if an earlier step raises. The container's stop
    grace period must cover the whole sequence (docker-compose.yml).
    """
    try:
        scheduler.pause()
        running = running_jobs.job_ids()
        if running:
            logger.info("shutdown: waiting up to %ss for %s", timeout, ", ".join(running))
        if not await running_jobs.wait_idle(timeout, abort=force):
            logger.warning(
                "shutdown: cancelling job(s) still running: %s",
                ", ".join(running_jobs.job_ids()),
            )
        scheduler.shutdown(wait=False)
        if not await running_jobs.wait_idle(UNWIND_SECONDS):
            logger.warning(
                "shutdown: job(s) still unwinding after %ss: %s",
                UNWIND_SECONDS, ", ".join(running_jobs.job_ids()),
            )
    finally:
        heartbeat_stop.set()
        try:
            async with asyncio.timeout(CLEANUP_SECONDS):
                try:
                    await heartbeat_task
                except Exception:
                    logger.exception("shutdown: heartbeat had failed")
                await release_leases()
        except TimeoutError:
            heartbeat_task.cancel()
            logger.warning(
                "shutdown: heartbeat stop and lease release took over %ss; "
                "unreleased leases expire on their own",
                CLEANUP_SECONDS,
            )
        finally:
            await http_client.aclose()


def main() -> None:
    """Entry point for console_scripts."""
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
