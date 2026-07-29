"""Solar-scraper service entrypoint.

Boot sequence:
  1. Load config from environment.
  2. Connect to MongoDB, ensure indexes.
  3. Claim leases for this WORKER_ID.
  4. Start heartbeat renewal coroutine.
  5. Set default executor for growattServer.to_thread calls.
  6. Build scheduler from claimed accounts.
  7. Install SIGTERM/SIGINT handlers for graceful shutdown.
  8. Run forever.

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
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx

from config import Settings
from db.mongo import get_db, init_indexes
from leases import claim_leases, heartbeat, release_lease
from scheduler.jobs import add_account_jobs, add_flush_job, build_scheduler

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

    # --- Heartbeat coroutine --------------------------------------------
    stop_event = asyncio.Event()
    heartbeat_task = asyncio.create_task(
        heartbeat(
            db,
            settings.worker_id,
            claimed,
            ttl_multiplier=settings.lease_ttl_multiplier,
            renewal_interval=settings.lease_renewal_interval,
            stop_event=stop_event,
        )
    )

    # --- Signal handlers ------------------------------------------------
    shutting_down = False

    def _shutdown() -> None:
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        logger.info("shutdown signal received — stopping scheduler + heartbeat")
        stop_event.set()
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass  # already shut down
        # Release leases for all claimed accounts.
        for acct in claimed:
            asyncio.create_task(
                release_lease(db, acct.provider, acct.account_id, settings.worker_id)
            )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    # --- Start ----------------------------------------------------------
    scheduler.start()
    logger.info(
        "scheduler started — %d job(s) registered",
        len(scheduler.get_jobs()),
    )

    try:
        # Keep the event loop alive until shutdown.
        while not stop_event.is_set():
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

    # Wait for heartbeat to finish.
    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass

    logger.info("solar-scraper stopped")


def main() -> None:
    """Entry point for console_scripts."""
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
