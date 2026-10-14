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
    doc["site_cache"] = {"site_id": 1}
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
