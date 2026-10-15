"""Tests for dev.py test-* commands (mocked Mongo and provider, no network)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

import dev

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def _account(lease: dict | None = None) -> dict:
    return {
        "_id": "growatt::acc-1",
        "provider": "growatt",
        "account_id": "acc-1",
        "interval_seconds": 600,
        "credentials": {"provider": "growatt", "api_token": "fake"},
        "lease": lease,
    }


def test_live_lease_owner():
    live = {"worker_id": "worker-1", "expires_at": NOW + timedelta(minutes=5)}
    expired = {"worker_id": "worker-1", "expires_at": NOW - timedelta(minutes=5)}
    naive = {"worker_id": "worker-1", "expires_at": (NOW + timedelta(minutes=5)).replace(tzinfo=None)}
    assert dev._live_lease_owner(_account(live), NOW) == "worker-1"
    assert dev._live_lease_owner(_account(naive), NOW) == "worker-1"
    assert dev._live_lease_owner(_account(expired), NOW) is None
    assert dev._live_lease_owner(_account(None), NOW) is None


def test_describe_prints_no_values():
    text = dev._describe({"account_id": "acc-1", "inverters": {"SN1": 1.5, "SN2": None},
                          "total_power": 1.5, "soc_percent": None})
    assert text == "account_id, inverters[2, 1 null], total_power, soc_percent=null"
    assert "1.5" not in text and "acc-1" not in text


def _db(doc: dict) -> MagicMock:
    db = MagicMock()
    db.api_configs.find_one = AsyncMock(return_value=doc)
    return db


async def test_run_cycle_refuses_while_a_worker_holds_the_lease():
    lease = {"worker_id": "worker-1", "expires_at": datetime.now(UTC) + timedelta(minutes=5)}
    spec = MagicMock(fetch=AsyncMock())
    with (
        patch("db.mongo.get_db", AsyncMock(return_value=_db(_account(lease)))),
        patch("providers.get_spec", return_value=spec),
        pytest.raises(typer.Exit),
    ):
        await dev._run_cycle("growatt", "acc-1", "mongodb://unused")
    spec.fetch.assert_not_called()


async def test_run_cycle_fetches_and_transforms_without_storing_records():
    db = _db(_account())
    spec = MagicMock(
        fetch=AsyncMock(return_value=[("SN001", {"pac": "1"})]),
        transform=MagicMock(return_value=[{"account_id": "acc-1", "device_sn": "SN001"}]),
    )
    with (
        patch("db.mongo.get_db", AsyncMock(return_value=db)),
        patch("providers.get_spec", return_value=spec),
    ):
        await dev._run_cycle("growatt", "acc-1", "mongodb://unused")

    spec.fetch.assert_awaited_once()
    spec.transform.assert_called_once()
    db.processed_results.insert_one.assert_not_called()
    db.raw_responses.insert_one.assert_not_called()
    db.api_configs.update_one.assert_not_called()  # no health fields


@pytest.mark.parametrize("provider,reason,absent", [
    ("growatt", "rate-limit", "EnPhase"),
    ("enphase", "refresh the same token", "Growatt"),
])
async def test_lease_refusal_names_the_provider_specific_reason(capsys, provider, reason, absent):
    doc = _account({"worker_id": "worker-1",
                    "expires_at": datetime.now(UTC) + timedelta(minutes=5)})
    with (
        patch("db.mongo.get_db", AsyncMock(return_value=_db(doc))),
        pytest.raises(typer.Exit),
    ):
        await dev._run_cycle(provider, "acc-1", "mongodb://unused")
    err = capsys.readouterr().err
    assert reason in err and absent not in err
