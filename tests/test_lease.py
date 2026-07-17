"""Tests for leases.py — claim, heartbeat, release.

All MongoDB operations are mocked.  No real DB connections.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from db.models import ApiConfig, GrowattCredentials
from leases import (
    claim_account,
    claim_leases,
    heartbeat,
    release_lease,
    renew_lease,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(provider: str = "growatt", account_id: str = "acc-1", **overrides) -> dict:
    base = {
        "_id": f"{provider}::{account_id}",
        "provider": provider,
        "account_id": account_id,
        "interval_seconds": 300,
        "enabled": True,
        "credentials": {"provider": "growatt", "api_token": "tok"},
        "store_raw": True,
        "lease": None,
    }
    base.update(overrides)
    return base


class FakeAsyncCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._docs:
            raise StopAsyncIteration
        return self._docs.pop(0)


# ---------------------------------------------------------------------------
# claim_account
# ---------------------------------------------------------------------------


async def test_claim_account_unclaimed():
    db = MagicMock()
    db.api_configs.find_one_and_update = AsyncMock(return_value=_make_config())

    ok = await claim_account(
        db, "growatt", "acc-1", "worker-1",
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is True
    db.api_configs.find_one_and_update.assert_called_once()


async def test_claim_account_already_owned_by_other():
    db = MagicMock()
    db.api_configs.find_one_and_update = AsyncMock(return_value=None)

    ok = await claim_account(
        db, "growatt", "acc-1", "worker-2",
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is False


async def test_claim_account_expired_lease():
    """An account with an expired lease should be claimable."""
    db = MagicMock()
    old_config = _make_config()
    db.api_configs.find_one_and_update = AsyncMock(return_value=old_config)

    ok = await claim_account(
        db, "growatt", "acc-1", "worker-2",
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is True


# ---------------------------------------------------------------------------
# claim_leases
# ---------------------------------------------------------------------------


async def test_claim_leases_returns_claimed_configs():
    db = MagicMock()
    configs = [
        _make_config("growatt", "acc-1", lease=None),
        _make_config("growatt", "acc-2", lease=None),
    ]
    db.api_configs.find = MagicMock(return_value=FakeAsyncCursor(configs))
    # Both claims succeed.
    db.api_configs.find_one_and_update = AsyncMock(side_effect=configs)

    result = await claim_leases(db, "worker-1", ttl_multiplier=2)

    assert len(result) == 2
    assert all(isinstance(cfg, ApiConfig) for cfg in result)


async def test_claim_leases_skips_active_foreign_leases():
    other_lease = {
        "worker_id": "worker-2",
        "expires_at": datetime.now(UTC) + timedelta(hours=1),
    }
    db = MagicMock()
    configs = [
        _make_config("growatt", "acc-1", lease=other_lease),
        _make_config("growatt", "acc-2", lease=None),
    ]
    db.api_configs.find = MagicMock(return_value=FakeAsyncCursor(configs))
    # Only acc-2 should be claimable.
    db.api_configs.find_one_and_update = AsyncMock(return_value=configs[1])

    result = await claim_leases(db, "worker-1", ttl_multiplier=2)

    assert len(result) == 1
    assert result[0].account_id == "acc-2"


# ---------------------------------------------------------------------------
# renew_lease
# ---------------------------------------------------------------------------


async def test_renew_lease_success():
    db = MagicMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    db.api_configs.update_one = AsyncMock(return_value=mock_result)

    ok = await renew_lease(
        db, "growatt", "acc-1", "worker-1",
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is True


async def test_renew_lease_lost_ownership():
    db = MagicMock()
    mock_result = MagicMock()
    mock_result.modified_count = 0
    db.api_configs.update_one = AsyncMock(return_value=mock_result)

    ok = await renew_lease(
        db, "growatt", "acc-1", "worker-1",
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is False


# ---------------------------------------------------------------------------
# release_lease
# ---------------------------------------------------------------------------


async def test_release_lease():
    db = MagicMock()
    db.api_configs.update_one = AsyncMock()

    await release_lease(db, "growatt", "acc-1", "worker-1")

    db.api_configs.update_one.assert_called_once()


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


async def test_heartbeat_renews_leases():
    db = MagicMock()
    mock_result = MagicMock()
    mock_result.modified_count = 1
    db.api_configs.update_one = AsyncMock(return_value=mock_result)

    config = ApiConfig(
        provider="growatt",
        account_id="acc-1",
        interval_seconds=300,
        credentials=GrowattCredentials(api_token="tok"),
    )

    stop_event = asyncio.Event()

    async def stop_after_tick():
        await asyncio.sleep(0.05)
        stop_event.set()

    await asyncio.gather(
        heartbeat(
            db, "worker-1", [config],
            ttl_multiplier=2, renewal_interval=1,
            stop_event=stop_event,
        ),
        stop_after_tick(),
    )

    # At least one renewal should have happened.
    assert db.api_configs.update_one.call_count >= 1


async def test_heartbeat_removes_lost_leases():
    db = MagicMock()
    mock_result = MagicMock()
    mock_result.modified_count = 0  # lease lost
    db.api_configs.update_one = AsyncMock(return_value=mock_result)

    config = ApiConfig(
        provider="growatt",
        account_id="acc-1",
        interval_seconds=300,
        credentials=GrowattCredentials(api_token="tok"),
    )
    accounts = [config]

    stop_event = asyncio.Event()

    async def stop_after_tick():
        await asyncio.sleep(0.1)
        stop_event.set()

    await asyncio.gather(
        heartbeat(
            db, "worker-1", accounts,
            ttl_multiplier=2, renewal_interval=1,
            stop_event=stop_event,
        ),
        stop_after_tick(),
    )

    # The lost account should have been removed from the list.
    assert len(accounts) == 0
