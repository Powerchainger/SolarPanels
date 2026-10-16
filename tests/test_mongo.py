"""Tests for db/mongo.py index setup (mocked MongoDB)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from db.mongo import POSTED_TTL_INDEX, TTL_INDEX, init_indexes


def _coll(existing_indexes: dict) -> MagicMock:
    coll = MagicMock()
    coll.index_information = AsyncMock(return_value=existing_indexes)
    coll.drop_index = AsyncMock()
    coll.create_index = AsyncMock()
    return coll


def _db(raw_indexes: dict | None = None, pr_indexes: dict | None = None) -> MagicMock:
    db = MagicMock()
    db.raw_responses = _coll(raw_indexes or {"_id_": {}})
    db.processed_results = _coll(pr_indexes or {"_id_": {}})
    db.workers = _coll({"_id_": {}})
    return db


def _posted_ttl_calls(db) -> list:
    return [
        c for c in db.processed_results.create_index.await_args_list
        if c.kwargs.get("name") == POSTED_TTL_INDEX
    ]


async def test_init_indexes_fresh_database_creates_ttl_index():
    db = _db()
    await init_indexes(db, ttl_days=30)
    db.raw_responses.drop_index.assert_not_awaited()
    db.raw_responses.create_index.assert_awaited_once_with(
        "created_at", name=TTL_INDEX, expireAfterSeconds=30 * 86400
    )


async def test_init_indexes_same_ttl_keeps_index():
    db = _db({TTL_INDEX: {"key": [("created_at", 1)], "expireAfterSeconds": 30 * 86400}})
    await init_indexes(db, ttl_days=30)
    db.raw_responses.drop_index.assert_not_awaited()


async def test_init_indexes_changed_ttl_rebuilds_index():
    """A changed RAW_TTL_DAYS must not make create_index fail at boot."""
    db = _db({TTL_INDEX: {"key": [("created_at", 1)], "expireAfterSeconds": 30 * 86400}})
    await init_indexes(db, ttl_days=14)
    db.raw_responses.drop_index.assert_awaited_once_with(TTL_INDEX)
    db.raw_responses.create_index.assert_awaited_once_with(
        "created_at", name=TTL_INDEX, expireAfterSeconds=14 * 86400
    )


async def test_init_indexes_posted_ttl_only_covers_posted_records():
    db = _db()
    await init_indexes(db, posted_ttl_days=7)
    (call,) = _posted_ttl_calls(db)
    assert call.args == ("created_at",)
    assert call.kwargs["expireAfterSeconds"] == 7 * 86400
    assert call.kwargs["partialFilterExpression"] == {"status": "posted"}


async def test_init_indexes_infinite_posted_ttl_removes_index():
    """POSTED_TTL_DAYS=inf: an existing expiry index is dropped, none created."""
    db = _db(pr_indexes={POSTED_TTL_INDEX: {"expireAfterSeconds": 7 * 86400}})
    await init_indexes(db, posted_ttl_days=float("inf"))
    db.processed_results.drop_index.assert_awaited_once_with(POSTED_TTL_INDEX)
    assert _posted_ttl_calls(db) == []


async def test_init_indexes_changed_posted_ttl_rebuilds_index():
    db = _db(pr_indexes={POSTED_TTL_INDEX: {"expireAfterSeconds": 7 * 86400}})
    await init_indexes(db, posted_ttl_days=1.5)
    db.processed_results.drop_index.assert_awaited_once_with(POSTED_TTL_INDEX)
    (call,) = _posted_ttl_calls(db)
    assert call.kwargs["expireAfterSeconds"] == int(1.5 * 86400)
