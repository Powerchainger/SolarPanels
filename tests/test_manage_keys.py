"""Tests for manage_keys.py credential-migration helpers — no Mongo needed.

Exercises the pure field transformation used by the migrate-encrypt
subcommand (idempotency + rollback); the Mongo loop itself is a thin
find/update wrapper verified by hand at migration time.
"""

from manage_keys import _transform_credentials
from crypto import is_encrypted


def _enphase_creds() -> dict:
    return {
        "provider": "enphase",
        "client_id": "cid",
        "client_secret": "csec",
        "redirect_uri": "https://example.com/cb",
        "api_key": "key123",
        "access_token": "at",
        "refresh_token": "rt",
    }


def test_migrate_encrypt_pass():
    out = _transform_credentials(_enphase_creds(), decrypt=False)
    for f in ("client_secret", "api_key", "access_token", "refresh_token"):
        assert is_encrypted(out[f]), f"{f} not encrypted"
    # Non-secrets untouched.
    assert out["client_id"] == "cid"
    assert out["redirect_uri"] == "https://example.com/cb"


def test_migrate_encrypt_idempotent():
    once = _transform_credentials(_enphase_creds(), decrypt=False)
    twice = _transform_credentials(once, decrypt=False)
    assert once == twice  # re-running must not double-encrypt


def test_migrate_decrypt_rollback():
    encrypted = _transform_credentials(_enphase_creds(), decrypt=False)
    out = _transform_credentials(encrypted, decrypt=True)
    assert out == _enphase_creds()


def test_migrate_decrypt_skips_plaintext():
    out = _transform_credentials(_enphase_creds(), decrypt=True)
    assert out == _enphase_creds()


def test_migrate_empty_values_untouched():
    creds = _enphase_creds()
    creds["access_token"] = ""
    out = _transform_credentials(creds, decrypt=False)
    assert out["access_token"] == ""


# ---------------------------------------------------------------------------
# _upsert: re-seeding must not wipe runtime state
# ---------------------------------------------------------------------------

import asyncio

import manage_keys
from db.models import ApiConfig, EnphaseCredentials, GrowattCredentials
from manage_keys import _plan_upsert


def _enphase_cfg(client_id="cid", refresh="rt-new") -> ApiConfig:
    return ApiConfig(
        provider="enphase", account_id="a", interval_seconds=3600,
        credentials=EnphaseCredentials(
            client_id=client_id, client_secret="csec", redirect_uri="https://x/cb",
            api_key="k", access_token="at-new", refresh_token=refresh,
        ),
    )


def _growatt_cfg(token="tok") -> ApiConfig:
    return ApiConfig(
        provider="growatt", account_id="g", interval_seconds=600,
        credentials=GrowattCredentials(api_token=token),
    )


def _stored(cfg: ApiConfig) -> dict:
    """An existing doc as the service would have left it, with runtime state."""
    doc = cfg.model_dump(mode="json")
    doc["_id"] = f"{cfg.provider}::{cfg.account_id}"
    doc["lease"] = {"worker_id": "w1", "expires_at": "2099-01-01T00:00:00Z"}
    doc["site_cache"] = {"site_id": 1, "user": "u"}
    doc["growatt_cache"] = {"plant_id": "p", "devices": []}
    return doc


def test_plan_new_account_inserts_everything():
    cfg = _enphase_cfg()
    up = _plan_upsert(None, cfg)
    assert set(up["$set"]) == {
        "credentials", "interval_seconds", "enabled", "updated_at", "seeded_at",
        "auth_failed", "consecutive_failures", "consecutive_auth_failures",
    }
    assert {"provider", "account_id", "store_raw", "lease", "site_cache"} <= set(up["$setOnInsert"])
    assert not set(up["$set"]) & set(up["$setOnInsert"])  # Mongo rejects overlap
    assert "$unset" not in up


def test_plan_enphase_reseed_keeps_lease_and_drops_only_site_cache():
    up = _plan_upsert(_stored(_enphase_cfg(refresh="rt-old")), _enphase_cfg(refresh="rt-new"))
    assert "$setOnInsert" not in up
    assert up["$unset"] == {"site_cache": ""}
    assert not {"lease", "site_cache", "growatt_cache"} & set(up["$set"])
    assert up["$set"]["credentials"]["refresh_token"] != "rt-old"


def test_plan_enphase_client_change_also_drops_site_cache():
    up = _plan_upsert(_stored(_enphase_cfg("cid")), _enphase_cfg("other"))
    assert up["$unset"] == {"site_cache": ""}


def test_plan_growatt_token_change_drops_only_growatt_cache():
    up = _plan_upsert(_stored(_growatt_cfg("old")), _growatt_cfg("new"))
    assert up["$unset"] == {"growatt_cache": ""}


def test_plan_growatt_same_token_keeps_cache():
    assert "$unset" not in _plan_upsert(_stored(_growatt_cfg("t")), _growatt_cfg("t"))


def test_plan_undecryptable_old_credentials_drop_cache():
    stored = _stored(_growatt_cfg("t"))
    stored["credentials"]["api_token"] = "gAAAAA-not-valid-ciphertext"
    assert _plan_upsert(stored, _growatt_cfg("t"))["$unset"] == {"growatt_cache": ""}


class _FakeCol:
    def __init__(self, doc=None):
        self.doc, self.calls = doc, []

    async def find_one(self, q):
        return self.doc

    async def update_one(self, q, update, upsert=False):
        self.calls.append((q, update, upsert))


def test_upsert_uses_update_not_replace(monkeypatch):
    col = _FakeCol(_stored(_enphase_cfg()))
    monkeypatch.setattr(manage_keys, "_mongo", lambda: type("D", (), {"api_configs": col})())
    asyncio.run(manage_keys._upsert(_enphase_cfg(refresh="rt2")))
    (q, update, upsert), = col.calls
    assert q == {"_id": "enphase::a"} and upsert is True
    assert "lease" not in update["$set"]


def test_plan_reseed_clears_auth_failure_but_keeps_last_success():
    stored = _stored(_enphase_cfg())
    stored.update(
        auth_failed=True, consecutive_failures=7, consecutive_auth_failures=3,
        last_success_at="2026-08-14T00:00:00Z",
    )
    up = _plan_upsert(stored, _enphase_cfg())
    assert up["$set"]["auth_failed"] is False
    assert up["$set"]["consecutive_failures"] == 0
    assert up["$set"]["consecutive_auth_failures"] == 0
    assert "last_success_at" not in up["$set"]


def test_plan_reseed_stamps_seeded_at():
    up = _plan_upsert(_stored(_enphase_cfg()), _enphase_cfg())
    assert up["$set"]["seeded_at"] == up["$set"]["updated_at"]


def test_health_summary():
    from datetime import UTC, datetime
    from manage_keys import _health

    assert _health({}) == "last_success=never"
    ok = {"last_success_at": datetime(2026, 8, 14, 9, 30, tzinfo=UTC)}
    assert _health(ok) == "last_success=2026-08-14 09:30 UTC"
    dead = {**ok, "consecutive_failures": 3, "auth_failed": True}
    out = _health(dead)
    assert "failures=3" in out and "AUTH FAILED" in out


# ---------------------------------------------------------------------------
# migrate-raw-dates
# ---------------------------------------------------------------------------


class _Cursor:
    def __init__(self, docs):
        self._docs = iter(docs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._docs)
        except StopIteration:
            raise StopAsyncIteration


def _raw_db(docs):
    from unittest.mock import AsyncMock, MagicMock

    db = MagicMock()
    db.raw_responses.find = MagicMock(return_value=_Cursor(docs))
    db.raw_responses.bulk_write = AsyncMock()
    return db


def test_parse_created_at_formats():
    from datetime import UTC, datetime

    from manage_keys import _parse_created_at

    want = datetime(2026, 10, 7, 12, 51, 9, 156601, tzinfo=UTC)
    assert _parse_created_at("2026-10-07T12:51:09.156601Z") == want
    assert _parse_created_at("2026-10-07T12:51:09.156601+00:00") == want
    assert _parse_created_at("2026-10-07T14:51:09.156601+02:00") == want
    assert _parse_created_at("2026-10-07T12:51:09.156601") == want  # naive → UTC
    assert _parse_created_at("not a date") is None


async def test_migrate_raw_dates_converts_strings_only(capsys):
    from datetime import UTC, datetime, timedelta

    from manage_keys import _migrate_raw_dates

    old = (datetime.now(UTC) - timedelta(days=40)).isoformat()
    new = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    db = _raw_db([
        {"_id": 1, "created_at": old},
        {"_id": 2, "created_at": new},
        {"_id": 3, "created_at": "garbage"},
    ])

    await _migrate_raw_dates(db, dry_run=False, ttl_days=30)

    query = db.raw_responses.find.call_args[0][0]
    assert query == {"created_at": {"$type": "string"}}
    ops = db.raw_responses.bulk_write.call_args[0][0]
    assert [op._filter for op in ops] == [
        {"_id": 1, "created_at": old},
        {"_id": 2, "created_at": new},
    ]
    assert all(isinstance(op._doc["$set"]["created_at"], datetime) for op in ops)
    out = capsys.readouterr().out
    assert "2 raw_responses doc(s) converted" in out
    assert "1 of them are older than 30 days" in out
    assert "1 doc(s) skipped" in out


async def test_migrate_raw_dates_dry_run_writes_nothing(capsys):
    from manage_keys import _migrate_raw_dates

    db = _raw_db([{"_id": 1, "created_at": "2026-01-01T00:00:00Z"}])
    await _migrate_raw_dates(db, dry_run=True, ttl_days=30)
    db.raw_responses.bulk_write.assert_not_called()
    assert "[dry-run] 1 raw_responses doc(s) would be converted" in capsys.readouterr().out


async def test_migrate_raw_dates_batches(monkeypatch):
    import manage_keys

    monkeypatch.setattr(manage_keys, "RAW_DATES_BATCH", 2)
    db = _raw_db([{"_id": i, "created_at": "2026-01-01T00:00:00Z"} for i in range(5)])
    await manage_keys._migrate_raw_dates(db, dry_run=False, ttl_days=30)
    sizes = [len(c[0][0]) for c in db.raw_responses.bulk_write.call_args_list]
    assert sizes == [2, 2, 1]
