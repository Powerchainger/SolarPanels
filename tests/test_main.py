"""Tests for main.py entrypoint.

Covers startup orchestration, signal handling, and shutdown sequence.
Uses mocks for all IO (MongoDB, scheduler, HTTP client).
"""

from __future__ import annotations

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


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
    return s


@pytest.fixture
def mock_account():
    """Return an ApiConfig-like claimed account."""
    acct = MagicMock()
    acct.provider = "growatt"
    acct.account_id = "test-acct"
    acct.interval_seconds = 300
    return acct


@pytest.mark.asyncio
async def test_main_claims_leases(mock_settings, mock_account):
    """_main calls claim_leases and builds scheduler from claimed accounts."""
    with (
        patch("main.Settings", return_value=mock_settings),
        patch("main.get_db", new_callable=AsyncMock) as mock_get_db,
        patch("main.init_indexes", new_callable=AsyncMock),
        patch("main.claim_leases", new_callable=AsyncMock) as mock_claim,
        patch("main.build_scheduler") as mock_build,
        patch("main.add_account_jobs") as mock_add_jobs,
        patch("main.add_flush_job") as mock_add_flush,
        patch("main.heartbeat", new_callable=AsyncMock) as mock_hb,
    ):
        mock_claim.return_value = [mock_account]
        mock_scheduler = MagicMock()
        mock_build.return_value = mock_scheduler

        # Create a stop_event that triggers immediately so _main exits.
        async def _hb_side_effect(*args, **kwargs):
            await asyncio.sleep(0.01)

        mock_hb.side_effect = _hb_side_effect

        from main import _main

        # Patch stop_event so the main loop exits quickly.
        with patch("main.asyncio.Event") as mock_event_cls:
            mock_event = MagicMock()
            mock_event.is_set.return_value = True  # exit immediately
            mock_event_cls.return_value = mock_event

            await _main()

        mock_claim.assert_called_once()
        mock_build.assert_called_once_with([mock_account])
        mock_add_jobs.assert_called_once()
        mock_add_flush.assert_called_once()
        mock_scheduler.start.assert_called_once()


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

        from main import _main

        await _main()

        mock_build.assert_not_called()


@pytest.mark.asyncio
async def test_main_installs_signal_handlers(mock_settings, mock_account):
    """_main installs SIGINT and SIGTERM handlers."""
    with (
        patch("main.Settings", return_value=mock_settings),
        patch("main.get_db", new_callable=AsyncMock),
        patch("main.init_indexes", new_callable=AsyncMock),
        patch("main.claim_leases", new_callable=AsyncMock) as mock_claim,
        patch("main.build_scheduler") as mock_build,
        patch("main.add_account_jobs"),
        patch("main.add_flush_job"),
        patch("main.heartbeat", new_callable=AsyncMock),
    ):
        mock_claim.return_value = [mock_account]
        mock_scheduler = MagicMock()
        mock_build.return_value = mock_scheduler

        from main import _main

        # Capture the signal handler callback.
        handlers = {}
        original_add_signal_handler = asyncio.get_event_loop().add_signal_handler

        def capture_handler(sig, callback, *args, **kwargs):
            handlers[sig] = callback

        with (
            patch("main.asyncio.Event") as mock_event_cls,
            patch.object(asyncio.get_event_loop(), "add_signal_handler", side_effect=capture_handler),
        ):
            mock_event = MagicMock()
            mock_event.is_set.return_value = True
            mock_event_cls.return_value = mock_event

            await _main()

        # Verify signal handlers were registered for SIGINT and SIGTERM.
        assert signal.SIGINT in handlers
        assert signal.SIGTERM in handlers

        # Simulate calling the shutdown handler and verify it sets stop_event.
        handlers[signal.SIGTERM]()
        mock_event.set.assert_called()
        mock_scheduler.shutdown.assert_called_once()
