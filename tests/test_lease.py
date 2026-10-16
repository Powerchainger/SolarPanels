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


async def test_heartbeat_lost_lease_does_not_skip_next_account():
    """Dropping one account mid-loop must not stop renewal of the one after it."""
    lost_id = "growatt::acc-1"
    renewed: list[str] = []

    async def update_one(filter, update):
        result = MagicMock()
        if filter["_id"] == lost_id:
            result.modified_count = 0  # lease lost
        else:
            result.modified_count = 1
            renewed.append(filter["_id"])
        return result

    db = MagicMock()
    db.api_configs.update_one = update_one

    accounts = [
        ApiConfig(
            provider="growatt",
            account_id=f"acc-{n}",
            interval_seconds=300,
            credentials=GrowattCredentials(api_token="tok"),
        )
        for n in (1, 2, 3)
    ]
    shared = accounts  # the caller's list must be updated in place

    stop_event = asyncio.Event()

    async def stop_after_first_tick():
        await asyncio.sleep(0.1)
        stop_event.set()

    await asyncio.gather(
        heartbeat(
            db, "worker-1", accounts,
            ttl_multiplier=2, renewal_interval=1,
            stop_event=stop_event,
        ),
        stop_after_first_tick(),
    )

    # Both surviving accounts were renewed in the first tick.
    assert "growatt::acc-2" in renewed
    assert "growatt::acc-3" in renewed
    assert [a.account_id for a in shared] == ["acc-2", "acc-3"]


async def test_heartbeat_touches_alive_file(tmp_path):
    """The container health check reads this file's age (healthcheck.py)."""
    db = MagicMock()
    alive = tmp_path / "alive"
    stop_event = asyncio.Event()

    async def stop_after_tick():
        await asyncio.sleep(0.05)
        stop_event.set()

    # No accounts: an idle worker is still alive.
    await asyncio.gather(
        heartbeat(
            db, "worker-1", [], ttl_multiplier=2, renewal_interval=1,
            stop_event=stop_event, alive_file=str(alive),
        ),
        stop_after_tick(),
    )

    assert alive.exists()


# ---------------------------------------------------------------------------
# Worker ids and stale-lease takeover
# ---------------------------------------------------------------------------


def test_new_worker_id_is_unique_per_process_for_the_same_name():
    from leases import new_worker_id

    a, b = new_worker_id("worker-1"), new_worker_id("worker-1")
    assert a != b
    assert a.startswith("worker-1-") and b.startswith("worker-1-")


def test_new_worker_id_falls_back_to_hostname():
    from leases import new_worker_id

    with patch("leases.socket.gethostname", return_value="c0ffee"):
        assert new_worker_id("").startswith("c0ffee-")


def test_stale_leases_needs_an_unchanged_lease_for_the_whole_window():
    from leases import StaleLeases

    stale = StaleLeases(stale_after=90)
    t0 = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    exp = t0 + timedelta(minutes=10)

    assert not stale.is_stale("g::a", "w-dead", exp, t0)  # first sighting
    assert not stale.is_stale("g::a", "w-dead", exp, t0 + timedelta(seconds=60))
    assert stale.is_stale("g::a", "w-dead", exp, t0 + timedelta(seconds=90))


def test_stale_leases_restarts_the_window_when_the_lease_moves():
    from leases import StaleLeases

    stale = StaleLeases(stale_after=90)
    t0 = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    exp = t0 + timedelta(minutes=10)

    stale.is_stale("g::a", "w-live", exp, t0)
    renewed = exp + timedelta(seconds=30)
    assert not stale.is_stale("g::a", "w-live", renewed, t0 + timedelta(seconds=120))
    assert not stale.is_stale("g::a", "w-live", renewed, t0 + timedelta(seconds=180))
    assert stale.is_stale("g::a", "w-live", renewed, t0 + timedelta(seconds=210))
    # A new holder with the same expiry is a new lease, too.
    assert not stale.is_stale("g::a", "w-other", renewed, t0 + timedelta(seconds=300))


def test_stale_leases_forgets_leases_no_longer_seen():
    from leases import StaleLeases

    stale = StaleLeases(stale_after=0)
    t0 = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)
    stale.is_stale("g::a", "w", t0, t0)
    stale.keep_only(set())
    # Seen again later: the window starts over.
    assert not stale.is_stale("g::a", "w", t0, t0 + timedelta(hours=1))


async def test_take_over_account_matches_the_stale_holder_and_expiry():
    from leases import take_over_account

    db = MagicMock()
    db.api_configs.find_one_and_update = AsyncMock(return_value=_make_config())
    old_exp = datetime(2026, 10, 16, 12, 10)

    ok = await take_over_account(
        db, "growatt", "acc-1", "w-new",
        holder="w-dead", holder_expires_at=old_exp,
        interval_seconds=300, ttl_multiplier=2,
    )

    assert ok is True
    kwargs = db.api_configs.find_one_and_update.call_args.kwargs
    assert kwargs["filter"] == {
        "_id": "growatt::acc-1", "lease.worker_id": "w-dead", "lease.expires_at": old_exp,
    }
    assert kwargs["update"]["$set"]["lease"]["worker_id"] == "w-new"


class FakeApiConfigs:
    """In-memory api_configs supporting exactly the lease queries in leases.py.

    Each call reads and writes one document without awaiting in between,
    which is the per-document atomicity MongoDB gives find_one_and_update.
    """

    def __init__(self, docs: list[dict]) -> None:
        self.docs = {d["_id"]: d for d in docs}

    @staticmethod
    def _get(doc: dict, path: str):
        for part in path.split("."):
            if not isinstance(doc, dict):
                return None
            doc = doc.get(part)
        return doc

    def _matches(self, doc: dict, flt: dict) -> bool:
        for key, want in flt.items():
            if key == "$or":
                if not any(self._matches(doc, sub) for sub in want):
                    return False
                continue
            have = self._get(doc, key)
            if isinstance(want, dict) and "$lt" in want:
                if have is None or not have < want["$lt"]:
                    return False
            elif have != want:
                return False
        return True

    def _apply(self, doc: dict, update: dict) -> None:
        for path, value in update["$set"].items():
            *parents, last = path.split(".")
            target = doc
            for part in parents:
                target = target[part]
            target[last] = value

    def find(self, flt: dict) -> FakeAsyncCursor:
        import copy

        return FakeAsyncCursor(
            copy.deepcopy(d) for d in self.docs.values() if self._matches(d, flt)
        )

    async def find_one_and_update(self, filter: dict, update: dict):
        doc = self.docs.get(filter["_id"])
        if doc is None or not self._matches(doc, filter):
            return None
        before = {**doc}
        self._apply(doc, update)
        return before

    async def update_one(self, filter: dict, update: dict):
        doc = self.docs.get(filter["_id"])
        ok = doc is not None and self._matches(doc, filter)
        if ok:
            self._apply(doc, update)
        return MagicMock(modified_count=int(ok))


def _fake_db(*docs: dict) -> MagicMock:
    db = MagicMock()
    db.api_configs = FakeApiConfigs(list(docs))
    return db


def _live_lease(holder: str) -> dict:
    return {"worker_id": holder, "expires_at": datetime.now(UTC) + timedelta(minutes=10)}


async def test_replicas_given_the_same_worker_id_do_not_share_leases():
    """The --scale bug: identical WORKER_IDs made every replica a holder."""
    from leases import new_worker_id

    db = _fake_db(_make_config("growatt", "acc-1"), _make_config("growatt", "acc-2"))
    first, second = new_worker_id("worker-1"), new_worker_id("worker-1")

    held_first = await claim_leases(db, first, ttl_multiplier=2)
    held_second = await claim_leases(db, second, ttl_multiplier=2)

    assert {a.account_id for a in held_first} == {"acc-1", "acc-2"}
    assert held_second == []
    # The first replica still sees both as its own on its next sync run.
    assert len(await claim_leases(db, first, ttl_multiplier=2)) == 2


async def test_live_holder_keeps_its_lease_while_it_renews():
    from leases import StaleLeases

    db = _fake_db(_make_config("growatt", "acc-1", lease=_live_lease("w-live")))
    stale = StaleLeases(stale_after=0)

    for _ in range(3):
        assert await claim_leases(db, "w-standby", ttl_multiplier=2, stale=stale) == []
        assert await renew_lease(
            db, "growatt", "acc-1", "w-live", interval_seconds=300, ttl_multiplier=2,
        )

    assert db.api_configs.docs["growatt::acc-1"]["lease"]["worker_id"] == "w-live"


async def test_stopped_holder_is_taken_over_by_exactly_one_of_several_workers():
    from leases import StaleLeases

    db = _fake_db(_make_config("growatt", "acc-1", lease=_live_lease("w-dead")))
    stales = {w: StaleLeases(stale_after=0) for w in ("w-b", "w-c", "w-d")}

    # First sighting: nobody acts yet. The holder does not renew.
    for w, stale in stales.items():
        assert await claim_leases(db, w, ttl_multiplier=2, stale=stale) == []

    # Unchanged on the second sighting: all three try at once.
    results = await asyncio.gather(*(
        claim_leases(db, w, ttl_multiplier=2, stale=stale) for w, stale in stales.items()
    ))

    winners = [w for w, held in zip(stales, results) if held]
    assert len(winners) == 1
    assert db.api_configs.docs["growatt::acc-1"]["lease"]["worker_id"] == winners[0]
    # The dead holder's late renewal must not take it back.
    assert not await renew_lease(
        db, "growatt", "acc-1", "w-dead", interval_seconds=300, ttl_multiplier=2,
    )


async def test_takeover_is_not_attempted_without_a_stale_tracker():
    """Boot claims only free accounts; takeovers need sightings across sync runs."""
    db = _fake_db(_make_config("growatt", "acc-1", lease=_live_lease("w-dead")))

    for _ in range(3):
        assert await claim_leases(db, "w-new", ttl_multiplier=2) == []
