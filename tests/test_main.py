"""Tests for main.py entrypoint.

Covers startup orchestration, signal handling, and shutdown sequence.
Uses mocks for MongoDB and HTTP; the shutdown tests drive a real
AsyncIOScheduler, because its cancel-on-shutdown behaviour is the point.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Imported before any test patches httpx.AsyncClient: clients/base.py
# subclasses it at import time.
from main import _main, shutdown_gracefully
from scheduler.jobs import RunningJobs


@pytest.fixture
def mock_settings():
    """Return a Settings-like object with test values."""
    s = MagicMock()
    s.mongo_uri = "mongodb://localhost:27017"
    s.output_api_url = "http://localhost:9999"
    s.output_api_token = ""
    s.log_level = "DEBUG"
    s.raw_ttl_days = 30
    s.threadpool_workers = 2
    s.worker_id = "test-worker"
    s.lease_ttl_multiplier = 2
    s.lease_renewal_interval = 30
    s.shutdown_timeout_seconds = 7
    return s


@pytest.fixture
def mock_account():
    """Return an ApiConfig-like claimed account."""
    acct = MagicMock()
    acct.provider = "growatt"
    acct.account_id = "test-acct"
    acct.interval_seconds = 300
    return acct


@contextlib.contextmanager
def _patched_main(mock_settings, mock_account):
    """Patch _main's IO; the scheduler's start() fires SIGTERM straight away."""
    handlers: dict = {}

    def capture_handler(sig, callback, *args):
        handlers[sig] = callback

    mock_scheduler = MagicMock()
    mock_scheduler.start.side_effect = lambda: handlers[signal.SIGTERM]()
    probe = AsyncMock()
    with (
        patch("httpx.AsyncClient") as mock_httpx,
        patch("main.Settings", return_value=mock_settings),
        patch("main.get_db", new_callable=AsyncMock),
        patch("main.init_indexes", new_callable=AsyncMock),
        patch("main.claim_leases", new_callable=AsyncMock, return_value=[mock_account]) as claim,
        patch("main.build_scheduler", return_value=mock_scheduler) as build,
        patch("main.add_account_jobs") as add_jobs,
        patch("main.add_flush_job") as add_flush,
        patch("main.heartbeat", new_callable=AsyncMock),
        patch("main.release_lease", new_callable=AsyncMock) as release,
        patch("main.shutdown_gracefully", new_callable=AsyncMock) as shutdown,
        patch.object(asyncio.get_event_loop(), "add_signal_handler", side_effect=capture_handler),
    ):
        mock_httpx.return_value.__aenter__ = AsyncMock(return_value=probe)
        mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)
        yield MagicMock(
            handlers=handlers, scheduler=mock_scheduler, claim=claim, build=build,
            add_jobs=add_jobs, add_flush=add_flush, release=release, shutdown=shutdown,
        )


@pytest.mark.asyncio
async def test_main_claims_leases(mock_settings, mock_account):
    """_main calls claim_leases and builds scheduler from claimed accounts."""
    with _patched_main(mock_settings, mock_account) as m:
        await _main()

    m.claim.assert_called_once()
    m.build.assert_called_once_with([mock_account])
    m.add_jobs.assert_called_once()
    m.add_flush.assert_called_once()
    m.scheduler.start.assert_called_once()


@pytest.mark.asyncio
async def test_main_exits_when_no_accounts(mock_settings):
    """_main exits early if no accounts are claimed."""
    with (
        patch("main.Settings", return_value=mock_settings),
        patch("main.get_db", new_callable=AsyncMock),
        patch("main.init_indexes", new_callable=AsyncMock),
        patch("main.claim_leases", new_callable=AsyncMock) as mock_claim,
        patch("main.build_scheduler") as mock_build,
    ):
        mock_claim.return_value = []

        await _main()

        mock_build.assert_not_called()


@pytest.mark.asyncio
async def test_main_signal_runs_graceful_shutdown(mock_settings, mock_account):
    """SIGTERM/SIGINT only flag the request; _main then awaits the shutdown sequence."""
    with _patched_main(mock_settings, mock_account) as m:
        await _main()

        assert set(m.handlers) == {signal.SIGINT, signal.SIGTERM}
        # The handler itself must not stop anything.
        m.scheduler.shutdown.assert_not_called()
        m.shutdown.assert_awaited_once()
        kwargs = m.shutdown.call_args.kwargs
        assert kwargs["scheduler"] is m.scheduler
        assert kwargs["timeout"] == 7

        # The lease-release callback releases every claimed account.
        await kwargs["release_leases"]()
        m.release.assert_awaited_once_with(
            m.release.call_args.args[0], "growatt", "test-acct", "test-worker"
        )


@pytest.mark.asyncio
async def test_main_release_failure_does_not_stop_other_releases(mock_settings, mock_account):
    other = MagicMock(provider="enphase", account_id="other")
    with _patched_main(mock_settings, mock_account) as m:
        m.claim.return_value = [mock_account, other]
        m.release.side_effect = [RuntimeError("mongo down"), None]
        await _main()
        await m.shutdown.call_args.kwargs["release_leases"]()  # must not raise

    assert m.release.await_count == 2


# ---------------------------------------------------------------------------
# shutdown_gracefully, with a real AsyncIOScheduler
# ---------------------------------------------------------------------------


async def _start_job(scheduler: AsyncIOScheduler, seconds: float, outcome: dict) -> None:
    """Add a job that runs now, and wait until it is running."""
    started = asyncio.Event()

    async def job() -> None:
        started.set()
        try:
            await asyncio.sleep(seconds)
            outcome["finished"] = True
        except asyncio.CancelledError:
            outcome["cancelled"] = True
            raise

    scheduler.add_job(job, id="fetch:growatt:acc")
    await asyncio.wait_for(started.wait(), 2)


def _shutdown_parts(order: list[str]):
    heartbeat_stop = asyncio.Event()

    async def heartbeat() -> None:
        await heartbeat_stop.wait()
        order.append("heartbeat stopped")

    async def release() -> None:
        order.append("leases released")

    client = MagicMock()
    client.aclose = AsyncMock(side_effect=lambda: order.append("client closed"))
    return heartbeat_stop, asyncio.create_task(heartbeat()), release, client


@pytest.mark.asyncio
async def test_shutdown_waits_for_running_job_then_releases():
    scheduler = AsyncIOScheduler()
    running = RunningJobs(scheduler)
    scheduler.start()
    outcome: dict = {}
    order: list[str] = []
    await _start_job(scheduler, 0.2, outcome)
    assert running.job_ids() == ["fetch:growatt:acc"]

    heartbeat_stop, heartbeat_task, release, client = _shutdown_parts(order)
    await shutdown_gracefully(
        scheduler=scheduler, running_jobs=running, heartbeat_stop=heartbeat_stop,
        heartbeat_task=heartbeat_task, release_leases=release, http_client=client,
        timeout=5,
    )

    assert outcome == {"finished": True}
    assert running.job_ids() == []
    assert order == ["heartbeat stopped", "leases released", "client closed"]
    assert not scheduler.running


@pytest.mark.asyncio
async def test_shutdown_cancels_job_after_timeout_and_still_releases(caplog):
    scheduler = AsyncIOScheduler()
    running = RunningJobs(scheduler)
    scheduler.start()
    outcome: dict = {}
    order: list[str] = []
    await _start_job(scheduler, 30, outcome)

    heartbeat_stop, heartbeat_task, release, client = _shutdown_parts(order)
    await shutdown_gracefully(
        scheduler=scheduler, running_jobs=running, heartbeat_stop=heartbeat_stop,
        heartbeat_task=heartbeat_task, release_leases=release, http_client=client,
        timeout=0.1,
    )
    await asyncio.sleep(0)  # let the cancellation reach the job

    assert outcome == {"cancelled": True}
    assert "cancelling job(s) still running after 0.1s: fetch:growatt:acc" in caplog.text
    assert order == ["heartbeat stopped", "leases released", "client closed"]


@pytest.mark.asyncio
async def test_shutdown_starts_no_new_runs():
    """Paused first: a job that comes due during the wait is not started."""
    scheduler = AsyncIOScheduler()
    running = RunningJobs(scheduler)
    scheduler.start()
    outcome: dict = {}
    await _start_job(scheduler, 0.3, outcome)
    runs = []

    async def due_soon() -> None:
        runs.append(1)

    scheduler.add_job(due_soon, "interval", seconds=0.05, id="flush")

    heartbeat_stop, heartbeat_task, release, client = _shutdown_parts([])
    await shutdown_gracefully(
        scheduler=scheduler, running_jobs=running, heartbeat_stop=heartbeat_stop,
        heartbeat_task=heartbeat_task, release_leases=release, http_client=client,
        timeout=5,
    )

    assert outcome == {"finished": True}
    assert runs == []


@pytest.mark.asyncio
async def test_running_jobs_idle_without_jobs():
    running = RunningJobs(AsyncIOScheduler())
    assert running.job_ids() == []
    assert await running.wait_idle(0.01) is True
