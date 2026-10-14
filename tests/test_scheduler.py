"""Tests for providers.py and scheduler/jobs.py.

All Mongo and HTTP calls are mocked.  No network, no real DB.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from db.models import (
    ApiConfig,
    GrowattCredentials,
    RecordStatus,
)
from providers import get_spec, REGISTRY, ProviderSpec
from scheduler.jobs import stagger_seconds


# ---------------------------------------------------------------------------
# providers.py — registry
# ---------------------------------------------------------------------------


def test_registry_has_growatt():
    assert "growatt" in REGISTRY
    spec = get_spec("growatt")
    assert isinstance(spec, ProviderSpec)
    assert spec.credentials_class is GrowattCredentials


def test_registry_has_enphase():
    spec = get_spec("enphase")
    assert spec.credentials_class.__name__ == "EnphaseCredentials"


def test_unknown_provider_raises():
    with pytest.raises(KeyError, match="Unknown provider"):
        get_spec("nonexistent")


def test_registry_dispatch_no_branching():
    """Both providers resolve through the registry — no if/elif chain."""
    providers_found = set()
    for name in ("growatt", "enphase"):
        spec = get_spec(name)
        providers_found.add(name)
        assert callable(spec.fetch)
        assert callable(spec.transform)
    assert providers_found == {"growatt", "enphase"}


# ---------------------------------------------------------------------------
# stagger_seconds
# ---------------------------------------------------------------------------


def test_stagger_is_deterministic():
    a = stagger_seconds("account-1", 300)
    b = stagger_seconds("account-1", 300)
    assert a == b


def test_stagger_within_interval():
    result = stagger_seconds("any-id", 60)
    assert 0 <= result < 60


def test_stagger_differs_for_different_accounts():
    """Two different account_ids should almost always produce different offsets."""
    a = stagger_seconds("acc-alpha", 3600)
    b = stagger_seconds("acc-beta", 3600)
    # Not guaranteed but astronomically unlikely to collide.
    assert a != b


# ---------------------------------------------------------------------------
# scheduler build + job registration
# ---------------------------------------------------------------------------


def _make_growatt_config(account_id: str = "acc-1", interval: int = 300) -> ApiConfig:
    return ApiConfig(
        provider="growatt",
        account_id=account_id,
        interval_seconds=interval,
        credentials=GrowattCredentials(api_token="fake-token"),
        enabled=True,
    )


def test_build_scheduler_returns_stopped():
    from scheduler.jobs import build_scheduler
    sched = build_scheduler([_make_growatt_config()])
    assert not sched.running


def test_add_account_jobs_registers_jobs():
    from scheduler.jobs import build_scheduler, add_account_jobs

    accounts = [
        _make_growatt_config("acc-1", 300),
        _make_growatt_config("acc-2", 600),
    ]

    sched = build_scheduler(accounts)
    db = MagicMock()
    http_client = MagicMock()

    add_account_jobs(
        sched, accounts, db,
        output_base_url="https://output.example.com",
        output_token="tok",
        http_client=http_client,
    )

    jobs = sched.get_jobs()
    assert len(jobs) == 2
    job_ids = {j.id for j in jobs}
    assert "fetch::growatt::acc-1" in job_ids
    assert "fetch::growatt::acc-2" in job_ids


def test_add_flush_job_registers():
    from scheduler.jobs import build_scheduler, add_flush_job

    sched = build_scheduler([])
    db = MagicMock()
    http_client = MagicMock()

    add_flush_job(
        sched, db,
        output_base_url="https://output.example.com",
        output_token="tok",
        http_client=http_client,
    )

    jobs = sched.get_jobs()
    assert len(jobs) == 1
    assert jobs[0].id == "flush"


# ---------------------------------------------------------------------------
# fetch_job (mocked)
# ---------------------------------------------------------------------------


def _mock_db_with_config(api_config_dict: dict) -> MagicMock:
    """Create a mock db where find_one returns the given config dict."""
    db = MagicMock()
    db.api_configs.find_one = AsyncMock(return_value=api_config_dict)
    db.api_configs.update_one = AsyncMock()  # health fields written by fetch_job
    db.raw_responses.insert_one = AsyncMock()
    db.processed_results.insert_one = AsyncMock()
    # Make insert_one return something with inserted_id
    insert_result = MagicMock()
    insert_result.inserted_id = "mock-oid-123"
    db.processed_results.insert_one.return_value = insert_result
    db.processed_results.update_one = AsyncMock()
    return db


@respx.mock
async def test_fetch_job_writes_raw_and_processed():
    from scheduler.jobs import fetch_job
    from clients.base import TimedClient

    cfg = _make_growatt_config()
    cfg_dict = cfg.model_dump(mode="json")
    cfg_dict["_id"] = f"{cfg.provider}::{cfg.account_id}"

    db = _mock_db_with_config(cfg_dict)
    respx.post("https://output.example.com/records").mock(
        return_value=httpx.Response(200)
    )

    async with TimedClient() as http_client:
        with patch("scheduler.jobs.get_spec") as mock_get_spec:
            spec = MagicMock()
            spec.fetch = AsyncMock(return_value=[
                ("SN001", {"pac": "1500", "ppv": "3200", "bdc1Soc": "85"}),
            ])
            spec.transform = MagicMock(return_value=[
                {"account_id": "acc-1", "timestamp": "2026-07-17T12:00:00Z", "device_sn": "SN001",
                 "ac_power_watts": 1500.0, "pv_power_watts": 3200.0, "soc_percent": 85},
            ])
            mock_get_spec.return_value = spec

            await fetch_job(
                db, "growatt", "acc-1",
                output_base_url="https://output.example.com",
                output_token="tok",
                http_client=http_client,
            )

    # The transform gets the account, so every record can name it.
    assert spec.transform.call_args[0][1]["account_id"] == "acc-1"
    # Should have written raw response and processed record.
    db.raw_responses.insert_one.assert_called_once()
    db.processed_results.insert_one.assert_called_once()
    db.processed_results.update_one.assert_called_once()
    # The TTL index on created_at only expires a real date, not an ISO string.
    raw_doc = db.raw_responses.insert_one.call_args[0][0]
    assert isinstance(raw_doc["created_at"], datetime)
    assert raw_doc["body"]["data"] == {"pac": "1500", "ppv": "3200", "bdc1Soc": "85"}
    # processed_results dates are dates too, on insert and on the post-outcome update.
    pr_doc = db.processed_results.insert_one.call_args[0][0]
    assert isinstance(pr_doc["created_at"], datetime)
    assert isinstance(pr_doc["last_attempt_at"], datetime)
    assert pr_doc["status"] == "posting"
    pr_update = db.processed_results.update_one.call_args[0][1]["$set"]
    assert isinstance(pr_update["last_attempt_at"], datetime)


async def test_fetch_job_skipps_missing_config():
    from scheduler.jobs import fetch_job

    db = MagicMock()
    db.api_configs.find_one = AsyncMock(return_value=None)

    http_client = MagicMock()
    await fetch_job(
        db, "growatt", "nonexistent",
        output_base_url="https://x.com", output_token="", http_client=http_client,
    )

    # No raw/processed writes.
    assert not hasattr(db.raw_responses, 'insert_one') or not db.raw_responses.insert_one.called


async def test_fetch_job_skips_disabled_account():
    from scheduler.jobs import fetch_job

    cfg = _make_growatt_config()
    cfg.enabled = False
    cfg_dict = cfg.model_dump(mode="json")
    cfg_dict["_id"] = f"{cfg.provider}::{cfg.account_id}"

    db = _mock_db_with_config(cfg_dict)
    http_client = MagicMock()

    await fetch_job(
        db, "growatt", "acc-1",
        output_base_url="https://x.com", output_token="", http_client=http_client,
    )

    db.raw_responses.insert_one.assert_not_called()


def _health_db(**overrides) -> MagicMock:
    cfg = _make_growatt_config()
    cfg_dict = cfg.model_dump(mode="json")
    cfg_dict["_id"] = f"{cfg.provider}::{cfg.account_id}"
    cfg_dict.update(overrides)
    return _mock_db_with_config(cfg_dict)


async def _run_fetch_job(db, fetch, transform=None):
    from scheduler.jobs import fetch_job
    from clients.base import TimedClient

    async with TimedClient() as http_client:
        with patch("scheduler.jobs.get_spec") as mock_get_spec:
            spec = MagicMock()
            spec.fetch = fetch
            spec.transform = transform or MagicMock(return_value=[])
            mock_get_spec.return_value = spec
            await fetch_job(
                db, "growatt", "acc-1",
                output_base_url="https://output.example.com",
                output_token="", http_client=http_client,
            )


async def test_fetch_job_success_records_health():
    db = _health_db(consecutive_failures=4)
    await _run_fetch_job(db, AsyncMock(return_value=[("SN001", {})]))

    (q, update), _ = db.api_configs.update_one.call_args
    assert q == {"_id": "growatt::acc-1"}
    assert update["$set"]["consecutive_failures"] == 0
    assert isinstance(update["$set"]["last_success_at"], datetime)


async def test_fetch_job_empty_result_still_counts_as_success():
    db = _health_db()
    await _run_fetch_job(db, AsyncMock(return_value=[]))
    update = db.api_configs.update_one.call_args[0][1]
    assert "last_success_at" in update["$set"]


async def test_fetch_job_generic_failure_counts_and_reraises():
    db = _health_db()
    with pytest.raises(RuntimeError, match="boom"):
        await _run_fetch_job(db, AsyncMock(side_effect=RuntimeError("boom")))

    update = db.api_configs.update_one.call_args[0][1]
    assert update["$inc"] == {"consecutive_failures": 1}
    assert set(update["$set"]) == {"last_fetch_at"}  # no auth_failed
    assert isinstance(update["$set"]["last_fetch_at"], datetime)


async def test_fetch_job_first_auth_failure_counts_but_does_not_freeze():
    from clients.base import AuthFailedError

    db = _health_db()
    await _run_fetch_job(db, AsyncMock(side_effect=AuthFailedError("dead")))  # must not raise

    update = db.api_configs.update_one.call_args[0][1]
    assert update["$set"]["consecutive_auth_failures"] == 1
    assert "auth_failed" not in update["$set"]  # not yet
    assert isinstance(update["$set"]["last_fetch_at"], datetime)  # a rejected call is still a call
    assert update["$inc"] == {"consecutive_failures": 1}
    db.raw_responses.insert_one.assert_not_called()


async def test_fetch_job_third_consecutive_auth_failure_freezes_account():
    from clients.base import AuthFailedError
    from scheduler.jobs import AUTH_FAILURE_THRESHOLD

    db = _health_db(consecutive_auth_failures=AUTH_FAILURE_THRESHOLD - 1)
    await _run_fetch_job(db, AsyncMock(side_effect=AuthFailedError("dead")))

    update = db.api_configs.update_one.call_args[0][1]
    assert update["$set"]["auth_failed"] is True
    assert update["$set"]["consecutive_auth_failures"] == AUTH_FAILURE_THRESHOLD


async def test_fetch_job_success_resets_auth_failure_streak():
    db = _health_db(consecutive_auth_failures=2)
    await _run_fetch_job(db, AsyncMock(return_value=[("SN001", {})]))
    update = db.api_configs.update_one.call_args[0][1]
    assert update["$set"]["consecutive_auth_failures"] == 0


async def test_fetch_job_auth_failure_write_is_conditioned_on_seeded_at():
    """A re-seed that landed mid-cycle must not be undone by a stale failure."""
    from clients.base import AuthFailedError

    seeded = datetime(2026, 10, 1, 12, 0)
    db = _health_db(consecutive_auth_failures=2, seeded_at=seeded)
    await _run_fetch_job(db, AsyncMock(side_effect=AuthFailedError("dead")))

    filt = db.api_configs.update_one.call_args[0][0]
    assert filt == {"_id": "growatt::acc-1", "seeded_at": seeded}


async def test_fetch_job_auth_failure_guard_matches_never_reseeded_account():
    """No seeded_at stored → filter on None, which Mongo matches for a missing field."""
    from clients.base import AuthFailedError

    db = _health_db()
    await _run_fetch_job(db, AsyncMock(side_effect=AuthFailedError("dead")))
    assert db.api_configs.update_one.call_args[0][0]["seeded_at"] is None


async def test_fetch_job_health_write_failure_does_not_lose_the_cycle():
    db = _health_db()
    db.api_configs.update_one = AsyncMock(side_effect=RuntimeError("mongo blip"))

    with respx.mock:
        respx.post("https://output.example.com/records").mock(return_value=httpx.Response(200))
        await _run_fetch_job(
            db,
            AsyncMock(return_value=[("SN001", {})]),
            MagicMock(return_value=[{"device_sn": "SN001"}]),
        )

    db.raw_responses.insert_one.assert_called_once()
    db.processed_results.insert_one.assert_called_once()


async def test_fetch_job_health_write_failure_keeps_original_fetch_error():
    db = _health_db()
    db.api_configs.update_one = AsyncMock(side_effect=ConnectionError("mongo down"))
    with pytest.raises(RuntimeError, match="boom"):
        await _run_fetch_job(db, AsyncMock(side_effect=RuntimeError("boom")))


async def test_fetch_job_transform_failure_after_fetch_still_counts_fetch_healthy():
    """Documents the semantics: health tracks the provider fetch, not delivery."""
    db = _health_db(consecutive_failures=2)
    with pytest.raises(ValueError):
        await _run_fetch_job(
            db,
            AsyncMock(return_value=[("SN001", {})]),
            MagicMock(side_effect=ValueError("schema changed")),
        )

    update = db.api_configs.update_one.call_args[0][1]
    assert update["$set"]["consecutive_failures"] == 0


async def test_fetch_job_skips_auth_failed_account_without_fetching():
    db = _health_db(auth_failed=True)
    fetch = AsyncMock()
    await _run_fetch_job(db, fetch)

    fetch.assert_not_called()
    db.api_configs.update_one.assert_not_called()


# ---------------------------------------------------------------------------
# flush_job (mocked)
# ---------------------------------------------------------------------------


def _flush_db(*claimed: dict | None) -> MagicMock:
    """Mock db whose find_one_and_update hands out `claimed` in order, then None."""
    db = MagicMock()
    db.processed_results.find_one_and_update = AsyncMock(side_effect=[*claimed, *[None] * 4])
    db.processed_results.update_one = AsyncMock()
    return db


def _failed_doc(doc_id: str = "doc-1", retry_count: int = 1) -> dict:
    return {
        "_id": doc_id,
        "provider": "growatt",
        "account_id": "acc-1",
        "record": {"device_sn": "SN001", "ac_power_watts": 100.0},
        "status": RecordStatus.FAILED,
        "retry_count": retry_count,
        "last_error": "HTTP 503",
        "last_attempt_at": datetime(2026, 7, 17, 11, 0, tzinfo=UTC),
    }


async def _run_flush(db, **kwargs):
    from scheduler.jobs import flush_job
    from clients.base import TimedClient

    async with TimedClient() as http_client:
        await flush_job(
            db, "https://output.example.com", "tok", http_client=http_client, **kwargs
        )


@respx.mock
async def test_flush_job_retries_failed_records():
    db = _flush_db(_failed_doc(retry_count=1))
    respx.post("https://output.example.com/records").mock(return_value=httpx.Response(200))

    await _run_flush(db, max_retries=5)

    # The claim moves the record to `posting` and counts the retry atomically.
    claim_filter, claim_update = db.processed_results.find_one_and_update.call_args_list[0][0]
    assert claim_filter["status"] == {"$in": [RecordStatus.PENDING, RecordStatus.FAILED]}
    assert claim_filter["retry_count"] == {"$lt": 5}
    assert claim_update["$set"]["status"] == RecordStatus.POSTING
    assert claim_update["$inc"] == {"retry_count": 1}

    db.processed_results.update_one.assert_called_once()
    update_dict = db.processed_results.update_one.call_args[0][1]["$set"]
    assert update_dict["status"] == RecordStatus.POSTED


@respx.mock
async def test_flush_job_exhausts_retries_stays_failed():
    db = _flush_db(_failed_doc("doc-exhausted", retry_count=4))  # 5th attempt
    respx.post("https://output.example.com/records").mock(return_value=httpx.Response(503))

    await _run_flush(db, max_retries=5)

    update_dict = db.processed_results.update_one.call_args[0][1]["$set"]
    assert update_dict["status"] == RecordStatus.FAILED


@respx.mock
async def test_flush_job_transient_failure_returns_record_to_its_state():
    """A claimed record must not stay `posting` after a transient failure."""
    db = _flush_db(_failed_doc(retry_count=1))
    respx.post("https://output.example.com/records").mock(return_value=httpx.Response(503))

    await _run_flush(db, max_retries=5)

    update_dict = db.processed_results.update_one.call_args[0][1]["$set"]
    assert update_dict["status"] == RecordStatus.FAILED
    assert update_dict["last_error"] == "HTTP 503"


@respx.mock
async def test_flush_job_skips_records_already_handled_this_run():
    """A record that fails again must not be retried twice in one run."""
    db = _flush_db(_failed_doc("doc-1"), None, None)
    respx.post("https://output.example.com/records").mock(return_value=httpx.Response(503))

    await _run_flush(db, max_retries=5)

    second_filter = db.processed_results.find_one_and_update.call_args_list[1][0][0]
    assert second_filter["_id"] == {"$nin": ["doc-1"]}


@respx.mock
async def test_flush_job_posts_nothing_when_nothing_can_be_claimed():
    """Records claimed by another worker's flush are not ours to send."""
    db = _flush_db()
    route = respx.post("https://output.example.com/records").mock(
        return_value=httpx.Response(200)
    )

    await _run_flush(db)

    assert not route.called
    db.processed_results.update_one.assert_not_called()


@respx.mock
async def test_flush_job_reclaims_stale_posting_records():
    stale_doc = {
        "_id": "stale-1",
        "provider": "growatt",
        "account_id": "acc-1",
        "record": {"device_sn": "SN001"},
        "status": RecordStatus.POSTING,
        "retry_count": 0,
        "last_error": None,
        "last_attempt_at": datetime.now(UTC) - timedelta(minutes=10),
    }
    # Pass 1 claims nothing, pass 2 claims the stale record.
    db = _flush_db(None, stale_doc)
    respx.post("https://output.example.com/records").mock(return_value=httpx.Response(200))

    await _run_flush(db, stale_seconds=300)

    db.processed_results.update_one.assert_called_once()
    update_dict = db.processed_results.update_one.call_args[0][1]["$set"]
    assert update_dict["status"] == RecordStatus.POSTED
    # The claim refreshes last_attempt_at, so another worker's stale query no
    # longer matches. Dates, not ISO strings: Mongo compares a date with an
    # ISO string by BSON type, not by time, so the stale query must use dates.
    stale_filter, stale_claim = db.processed_results.find_one_and_update.call_args_list[1][0]
    assert stale_filter["status"] == RecordStatus.POSTING
    assert isinstance(stale_filter["last_attempt_at"]["$lt"], datetime)
    assert isinstance(stale_claim["$set"]["last_attempt_at"], datetime)
    assert isinstance(update_dict["last_attempt_at"], datetime)


async def test_fetch_job_failure_logs_one_line_without_traceback(caplog):
    db = _health_db(consecutive_failures=2)
    with pytest.raises(RuntimeError, match="boom"):
        await _run_fetch_job(db, AsyncMock(side_effect=RuntimeError("boom")))

    ours = [r for r in caplog.records if r.name == "scheduler.jobs" and r.levelname == "ERROR"]
    assert [r.getMessage() for r in ours] == [
        "fetch failed for growatt::acc-1 (3 in a row): RuntimeError: boom"
    ]
    assert ours[0].exc_info is None  # APScheduler logs the traceback on the re-raise


async def test_fetch_job_transform_failure_logs_one_line_without_traceback(caplog):
    db = _health_db()
    with pytest.raises(ValueError):
        await _run_fetch_job(
            db,
            AsyncMock(return_value=[("SN001", {})]),
            MagicMock(side_effect=ValueError("schema changed")),
        )

    ours = [r for r in caplog.records if r.name == "scheduler.jobs" and r.levelname == "ERROR"]
    assert [r.getMessage() for r in ours] == [
        "transform failed for growatt::acc-1: ValueError: schema changed"
    ]
    assert ours[0].exc_info is None


async def test_failed_fetch_under_scheduler_logs_one_traceback(caplog):
    """End to end with a real AsyncIOScheduler: one traceback per failed run."""
    import asyncio

    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    db = _health_db()
    done = asyncio.Event()

    async def job() -> None:
        try:
            await _run_fetch_job(db, AsyncMock(side_effect=RuntimeError("boom")))
        finally:
            done.set()

    scheduler = AsyncIOScheduler()
    scheduler.start()
    scheduler.add_job(job, id="fetch::growatt::acc-1")
    await asyncio.wait_for(done.wait(), 2)
    await asyncio.sleep(0.05)  # let APScheduler's done-callback log
    scheduler.shutdown(wait=False)

    tracebacks = [r for r in caplog.records if r.exc_info]
    assert len(tracebacks) == 1
    assert tracebacks[0].name.startswith("apscheduler")
    assert "fetch failed for growatt::acc-1 (1 in a row): RuntimeError: boom" in caplog.text


async def test_fetch_job_success_records_last_fetch_at():
    db = _health_db()
    await _run_fetch_job(db, AsyncMock(return_value=[]))
    update = db.api_configs.update_one.call_args[0][1]
    assert update["$set"]["last_fetch_at"] == update["$set"]["last_success_at"]


# ---------------------------------------------------------------------------
# first_run_at: resume the rhythm across worker restarts
# ---------------------------------------------------------------------------


def _cfg_fetched(last_fetch_at, interval=360):
    cfg = _make_growatt_config()
    return cfg.model_copy(update={"last_fetch_at": last_fetch_at, "interval_seconds": interval})


def test_first_run_never_fetched_uses_stagger():
    from scheduler.jobs import first_run_at

    now = datetime(2026, 10, 7, 15, 0, tzinfo=UTC)
    cfg = _cfg_fetched(None)
    assert first_run_at(cfg, now) == now + timedelta(seconds=stagger_seconds(cfg.account_id, 360))


def test_first_run_waits_one_interval_after_a_recent_fetch():
    """The live case: a restart 4.5 min after a Growatt fetch tripped 10012."""
    from scheduler.jobs import first_run_at

    now = datetime(2026, 10, 7, 15, 43, 0, tzinfo=UTC)
    last = now - timedelta(minutes=4, seconds=30)
    assert first_run_at(_cfg_fetched(last), now) == last + timedelta(seconds=360)


def test_first_run_after_long_downtime_uses_stagger():
    from scheduler.jobs import first_run_at

    now = datetime(2026, 10, 7, 15, 0, tzinfo=UTC)
    cfg = _cfg_fetched(now - timedelta(hours=3))
    assert first_run_at(cfg, now) == now + timedelta(seconds=stagger_seconds(cfg.account_id, 360))


def test_first_run_accepts_naive_utc_from_mongo():
    from scheduler.jobs import first_run_at

    now = datetime(2026, 10, 7, 15, 0, tzinfo=UTC)
    naive_last = datetime(2026, 10, 7, 14, 58)  # how pymongo returns dates
    assert first_run_at(_cfg_fetched(naive_last), now) == datetime(2026, 10, 7, 15, 4, tzinfo=UTC)


def test_add_account_jobs_uses_first_run_at():
    from scheduler.jobs import add_account_jobs

    scheduler = MagicMock()
    last = datetime.now(UTC) - timedelta(minutes=1)
    add_account_jobs(
        scheduler, [_cfg_fetched(last)], db=MagicMock(),
        output_base_url="http://x", output_token="", http_client=MagicMock(),
    )
    next_run = scheduler.add_job.call_args.kwargs["next_run_time"]
    assert next_run == last + timedelta(seconds=360)


# ---------------------------------------------------------------------------
# sync_accounts_job
# ---------------------------------------------------------------------------


async def _sync(sched, claimed, held):
    from scheduler.jobs import sync_accounts_job

    with patch("scheduler.jobs.claim_leases", new_callable=AsyncMock, return_value=held):
        await sync_accounts_job(
            sched, claimed, MagicMock(), "https://x.com", "", MagicMock(),
            worker_id="w1", ttl_multiplier=2,
        )


def _fetch_job_ids(sched) -> set[str]:
    return {j.id for j in sched.get_jobs() if j.id.startswith("fetch::")}


async def test_sync_adds_newly_claimed_account():
    """An account added after start is scheduled without a restart."""
    from scheduler.jobs import build_scheduler

    sched = build_scheduler([])
    sched.start(paused=True)
    try:
        claimed: list[ApiConfig] = []
        new = _make_growatt_config("acc-new", 600)
        await _sync(sched, claimed, [new])

        assert _fetch_job_ids(sched) == {"fetch::growatt::acc-new"}
        assert claimed == [new]
    finally:
        sched.shutdown(wait=False)


async def test_sync_removes_jobs_for_accounts_no_longer_held():
    """Deleted, disabled or taken-over accounts lose their fetch job."""
    from scheduler.jobs import build_scheduler, add_account_jobs, add_flush_job

    sched = build_scheduler([])
    sched.start(paused=True)
    try:
        keep, gone = _make_growatt_config("keep"), _make_growatt_config("gone")
        add_account_jobs(sched, [keep, gone], MagicMock(), "https://x.com", "", MagicMock())
        add_flush_job(sched, MagicMock(), "https://x.com", "", MagicMock())
        claimed = [keep, gone]

        await _sync(sched, claimed, [keep])

        assert _fetch_job_ids(sched) == {"fetch::growatt::keep"}
        assert sched.get_job("flush") is not None  # only fetch jobs are touched
        assert claimed == [keep]
    finally:
        sched.shutdown(wait=False)


async def test_sync_reschedules_changed_interval():
    from scheduler.jobs import build_scheduler, add_account_jobs

    sched = build_scheduler([])
    sched.start(paused=True)
    try:
        old = _make_growatt_config("acc-1", 300)
        add_account_jobs(sched, [old], MagicMock(), "https://x.com", "", MagicMock())

        await _sync(sched, [old], [_make_growatt_config("acc-1", 900)])

        job = sched.get_job("fetch::growatt::acc-1")
        assert job.trigger.interval == timedelta(seconds=900)
    finally:
        sched.shutdown(wait=False)


async def test_sync_keeps_unchanged_job():
    """A held account with the same interval keeps its job and next run time."""
    from scheduler.jobs import build_scheduler, add_account_jobs

    sched = build_scheduler([])
    sched.start(paused=True)
    try:
        acct = _make_growatt_config("acc-1", 300)
        add_account_jobs(sched, [acct], MagicMock(), "https://x.com", "", MagicMock())
        before = sched.get_job("fetch::growatt::acc-1").next_run_time

        await _sync(sched, [acct], [_make_growatt_config("acc-1", 300)])

        assert sched.get_job("fetch::growatt::acc-1").next_run_time == before
    finally:
        sched.shutdown(wait=False)
