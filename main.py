"""Solar-scraper service entrypoint.

Boot sequence:
  1. Load config from environment.
  2. Connect to MongoDB, ensure indexes.
  3. Claim leases for this WORKER_ID.
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
from db.mongo import get_db, init_indexes
from leases import claim_leases, heartbeat, release_lease
from scheduler.jobs import RunningJobs, add_account_jobs, add_flush_job, build_scheduler

logger = logging.getLogger("solar-scraper")


async def _main() -> None:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logger.info("Starting solar-scraper worker %s", settings.worker_id)

    # --- MongoDB --------------------------------------------------------
    db = await get_db(settings.mongo_uri)
    await init_indexes(db, ttl_days=settings.raw_ttl_days)

    # --- Claim leases ---------------------------------------------------
    claimed = await claim_leases(
        db,
        settings.worker_id,
        ttl_multiplier=settings.lease_ttl_multiplier,
    )

    if not claimed:
        logger.warning("no accounts claimed — nothing to do, exiting")
        return

    logger.info(
        "claimed %d account(s): %s",
        len(claimed),
        ", ".join(f"{a.provider}/{a.account_id}" for a in claimed),
    )

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

    # Inject output URL from config — fallback to empty (echo-server mode
    # for dev_run.py, which passes its own URL directly).
    output_base_url = settings.output_api_url
    output_token = settings.output_api_token

    # --- Probe output API ----------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as probe:
            await probe.get(output_base_url)
    except httpx.ConnectError:
        logger.error(
            "Output API not reachable at %s — is the container running?",
            output_base_url,
        )
        sys.exit(1)

    add_account_jobs(
        scheduler,
        claimed,
        db=db,
        output_base_url=output_base_url,
        output_token=output_token,
        http_client=http_client,
        worker_id=settings.worker_id,
    )

    add_flush_job(
        scheduler,
        db=db,
        output_base_url=output_base_url,
        output_token=output_token,
        http_client=http_client,
    )

    running_jobs = RunningJobs(scheduler)

    # --- Heartbeat coroutine --------------------------------------------
    # Kept running during shutdown until in-flight jobs are done, so their
    # leases stay renewed while they finish.
    heartbeat_stop = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat(
            db,
            settings.worker_id,
            claimed,
            ttl_multiplier=settings.lease_ttl_multiplier,
            renewal_interval=settings.lease_renewal_interval,
            stop_event=heartbeat_stop,
        )
    )

    # --- Signal handlers ------------------------------------------------
    # The handler only flags the request; the awaited sequence below does
    # the work, so nothing is left to un-awaited tasks.
    shutdown_requested = asyncio.Event()

    def _request_shutdown() -> None:
        if not shutdown_requested.is_set():
            logger.info("shutdown signal received")
            shutdown_requested.set()

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
        # `claimed` is pruned in place by the heartbeat when a lease is lost.
        results = await asyncio.gather(
            *(
                release_lease(db, acct.provider, acct.account_id, settings.worker_id)
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

    await shutdown_gracefully(
        scheduler=scheduler,
        running_jobs=running_jobs,
        heartbeat_stop=heartbeat_stop,
        heartbeat_task=heartbeat_task,
        release_leases=release_all,
        http_client=http_client,
        timeout=settings.shutdown_timeout_seconds,
    )
    logger.info("solar-scraper stopped")


async def shutdown_gracefully(
    *,
    scheduler: Any,
    running_jobs: RunningJobs,
    heartbeat_stop: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
    release_leases: Callable[[], Awaitable[None]],
    http_client: Any,
    timeout: float,
) -> None:
    """Stop in an order that lets running jobs finish.

    1. Pause the scheduler: no new runs start, running ones continue.
    2. Wait up to `timeout` for running jobs (an EnPhase credential write
       interrupted here can lose a rotated refresh token).
    3. Shut the scheduler down, which cancels whatever is still running.
    4. Stop the heartbeat, then release the leases, so no renewal lands
       after a release.
    5. Close the shared HTTP client.

    The container's stop grace period must exceed `timeout`, or the runtime
    kills the process mid-wait (`stop_grace_period` in docker-compose.yml).
    """
    scheduler.pause()
    if running_jobs.job_ids():
        logger.info("shutdown: waiting up to %ss for %s", timeout, ", ".join(running_jobs.job_ids()))
    if not await running_jobs.wait_idle(timeout):
        logger.warning(
            "shutdown: cancelling job(s) still running after %ss: %s",
            timeout, ", ".join(running_jobs.job_ids()),
        )
    scheduler.shutdown(wait=False)

    heartbeat_stop.set()
    await heartbeat_task

    await release_leases()
    await http_client.aclose()


def main() -> None:
    """Entry point for console_scripts."""
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
