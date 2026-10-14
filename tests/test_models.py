"""Tests for db/models.py — no database or network required."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from db.models import (
    ApiConfig,
    EnphaseCredentials,
    GrowattCredentials,
    Lease,
    ProcessedRecord,
    RawResponse,
    RecordStatus,
    SiteCache,
)

NOW = datetime.now(UTC)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _enphase_config(**overrides) -> ApiConfig:
    defaults = dict(
        provider="enphase",
        account_id="acc-enphase",
        interval_seconds=3600,
        credentials={
            "provider": "enphase",
            "client_id": "cid",
            "client_secret": "csec",
            "redirect_uri": "https://example.com/cb",
            "api_key": "key123",
        },
        updated_at=NOW,
    )
    return ApiConfig(**{**defaults, **overrides})


def _growatt_config(**overrides) -> ApiConfig:
    defaults = dict(
        provider="growatt",
        account_id="acc-growatt",
        interval_seconds=300,
        credentials={"provider": "growatt", "api_token": "tok"},
        updated_at=NOW,
    )
    return ApiConfig(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# RecordStatus
# ---------------------------------------------------------------------------


def test_record_status_string_values():
    assert RecordStatus.PENDING == "pending"
    assert RecordStatus.POSTING == "posting"
    assert RecordStatus.POSTED == "posted"
    assert RecordStatus.FAILED == "failed"


def test_record_status_is_str():
    # StrEnum members are plain strings — safe to store in MongoDB as-is.
    assert isinstance(RecordStatus.PENDING, str)


# ---------------------------------------------------------------------------
# EnphaseCredentials
# ---------------------------------------------------------------------------


def test_enphase_credentials_minimal():
    creds = EnphaseCredentials(
        client_id="cid",
        client_secret="csec",
        redirect_uri="https://example.com/cb",
        api_key="key",
    )
    assert creds.provider == "enphase"
    assert creds.access_token == ""
    assert creds.token_type == "Bearer"
    assert creds.expires_at is None


def test_enphase_credentials_with_tokens():
    creds = EnphaseCredentials(
        client_id="cid",
        client_secret="csec",
        redirect_uri="https://example.com/cb",
        api_key="key",
        access_token="at",
        refresh_token="rt",
        expires_at=NOW,
        expires_in=3600,
    )
    assert creds.access_token == "at"
    assert creds.expires_in == 3600


def test_enphase_credentials_missing_required():
    with pytest.raises(ValidationError):
        EnphaseCredentials(client_id="cid")  # missing client_secret, redirect_uri, api_key


# ---------------------------------------------------------------------------
# GrowattCredentials
# ---------------------------------------------------------------------------


def test_growatt_credentials_valid():
    creds = GrowattCredentials(api_token="tok")
    assert creds.provider == "growatt"
    assert creds.api_token == "tok"


def test_growatt_credentials_missing_required():
    with pytest.raises(ValidationError):
        GrowattCredentials()  # missing api_token


# ---------------------------------------------------------------------------
# ApiConfig — valid construction
# ---------------------------------------------------------------------------


def test_enphase_api_config_defaults():
    cfg = _enphase_config()
    assert cfg.provider == "enphase"
    assert isinstance(cfg.credentials, EnphaseCredentials)
    assert cfg.enabled is True
    assert cfg.store_raw is True
    assert cfg.lease is None
    assert cfg.site_cache is None
    assert cfg.auth_failed is False
    assert cfg.last_success_at is None
    assert cfg.consecutive_failures == 0
    assert cfg.seeded_at is None


def test_growatt_api_config_defaults():
    cfg = _growatt_config()
    assert cfg.provider == "growatt"
    assert isinstance(cfg.credentials, GrowattCredentials)


def test_api_config_with_lease():
    cfg = _enphase_config(lease={"worker_id": "worker-1", "expires_at": NOW})
    assert isinstance(cfg.lease, Lease)
    assert cfg.lease.worker_id == "worker-1"


def test_api_config_with_site_cache():
    """Older documents still carry the site name; it loads but is dropped."""
    cfg = _enphase_config(site_cache={"site_id": 42, "user": "Sunnyville"})
    assert isinstance(cfg.site_cache, SiteCache)
    assert cfg.site_cache.site_id == 42
    assert "user" not in cfg.site_cache.model_dump()


def test_api_config_ignores_removed_device_filter_field():
    """Documents written before the field was removed must still load."""
    cfg = _growatt_config(device_filter={"type": 7})
    assert not hasattr(cfg, "device_filter")


def test_api_config_disabled():
    cfg = _growatt_config(enabled=False)
    assert cfg.enabled is False


def test_api_config_updated_at_defaults_to_now():
    cfg = ApiConfig(
        provider="growatt",
        account_id="x",
        interval_seconds=300,
        credentials={"provider": "growatt", "api_token": "tok"},
    )
    assert cfg.updated_at is not None


# ---------------------------------------------------------------------------
# ApiConfig — invalid construction
# ---------------------------------------------------------------------------


def test_api_config_missing_interval_seconds():
    with pytest.raises(ValidationError):
        ApiConfig(
            provider="growatt",
            account_id="x",
            credentials={"provider": "growatt", "api_token": "tok"},
        )


def test_api_config_missing_credentials():
    with pytest.raises(ValidationError):
        ApiConfig(provider="growatt", account_id="x", interval_seconds=300)


def test_api_config_unknown_provider():
    with pytest.raises(ValidationError):
        ApiConfig(
            provider="unknown",
            account_id="x",
            interval_seconds=300,
            credentials={"provider": "growatt", "api_token": "tok"},
        )


def test_api_config_credential_provider_mismatch():
    # provider="enphase" but credentials say "growatt" — model validator rejects this.
    with pytest.raises(ValidationError, match="does not match provider"):
        ApiConfig(
            provider="enphase",
            account_id="x",
            interval_seconds=3600,
            credentials={"provider": "growatt", "api_token": "tok"},
        )


# ---------------------------------------------------------------------------
# Encrypted credentials at rest (GDPR Art. 32)
# ---------------------------------------------------------------------------


def test_credentials_roundtrip_via_json_dump():
    """Plaintext in memory → ciphertext in model_dump(json) → plaintext on load."""
    cfg = _enphase_config()
    dump = cfg.model_dump(mode="json")
    loaded = ApiConfig(**dump)
    assert loaded.credentials.client_secret == "csec"
    assert loaded.credentials.api_key == "key123"
    assert loaded.credentials.access_token == ""
    assert loaded.credentials.refresh_token == ""
    assert loaded.credentials.client_id == "cid"


def test_credentials_ciphertext_at_rest():
    """model_dump(json) carries no plaintext secret; secrets are Fernet tokens."""
    cfg = _enphase_config(credentials={
        "provider": "enphase",
        "client_id": "cid",
        "client_secret": "csec",
        "redirect_uri": "https://example.com/cb",
        "api_key": "key123",
        "access_token": "at-secret",
        "refresh_token": "rt-secret",
    })
    dump = cfg.model_dump(mode="json")
    creds = dump["credentials"]
    raw = json.dumps(dump)
    for secret in ("csec", "key123", "at-secret", "rt-secret"):
        assert secret not in raw, f"plaintext secret leaked into dump: {secret}"
    for f in ("client_secret", "api_key", "access_token", "refresh_token"):
        assert creds[f].startswith("gAAAA"), f"{f} not encrypted at rest"
    # Non-secrets stay plaintext.
    assert creds["client_id"] == "cid"
    assert creds["redirect_uri"] == "https://example.com/cb"


def test_growatt_token_ciphertext_at_rest():
    cfg = _growatt_config()
    dump = cfg.model_dump(mode="json")
    assert dump["credentials"]["api_token"].startswith("gAAAA")
    assert dump["credentials"]["api_token"] != "tok"
    loaded = ApiConfig(**dump)
    assert loaded.credentials.api_token == "tok"


def test_empty_tokens_stay_empty():
    cfg = _enphase_config()
    dump = cfg.model_dump(mode="json")
    assert dump["credentials"]["access_token"] == ""
    assert dump["credentials"]["refresh_token"] == ""


# ---------------------------------------------------------------------------
# RawResponse
# ---------------------------------------------------------------------------


def test_raw_response_valid():
    r = RawResponse(
        provider="growatt",
        account_id="acc-growatt",
        body={"pac": "1234", "ppv": "5678"},
    )
    assert r.body["pac"] == "1234"
    assert r.created_at is not None


def test_raw_response_missing_body():
    with pytest.raises(ValidationError):
        RawResponse(provider="growatt", account_id="acc-growatt")


# ---------------------------------------------------------------------------
# ProcessedRecord
# ---------------------------------------------------------------------------


def test_processed_record_defaults():
    r = ProcessedRecord(
        provider="growatt",
        account_id="acc-growatt",
        record={"device_sn": "SN001", "ac_power_watts": 1500.0},
    )
    assert r.status == RecordStatus.PENDING
    assert r.retry_count == 0
    assert r.last_error is None
    assert r.last_attempt_at is None
    assert r.created_at is not None


def test_processed_record_failed_state():
    r = ProcessedRecord(
        provider="enphase",
        account_id="acc-enphase",
        record={"total_power": 3200},
        status=RecordStatus.FAILED,
        retry_count=3,
        last_error="HTTP 400 Bad Request",
        last_attempt_at=NOW,
    )
    assert r.status == RecordStatus.FAILED
    assert r.retry_count == 3
    assert "400" in r.last_error


def test_processed_record_missing_required():
    with pytest.raises(ValidationError):
        ProcessedRecord(provider="growatt", account_id="x")  # missing record


def test_encryption_key_falls_back_to_dotenv(tmp_path, monkeypatch):
    """Like Settings, crypto reads ENCRYPTION_KEY from .env when it is not exported."""
    import crypto
    from cryptography.fernet import Fernet

    key = Fernet.generate_key().decode()
    (tmp_path / ".env").write_text(f"ENCRYPTION_KEY={key}\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ENCRYPTION_KEY")
    crypto._fernet.cache_clear()
    try:
        token = crypto.encrypt_str("secret")
        assert Fernet(key.encode()).decrypt(token.encode()) == b"secret"
    finally:
        crypto._fernet.cache_clear()  # later tests use the conftest key again


def test_encryption_key_environment_wins_over_dotenv(tmp_path, monkeypatch):
    import crypto
    from cryptography.fernet import Fernet

    (tmp_path / ".env").write_text(f"ENCRYPTION_KEY={Fernet.generate_key().decode()}\n")
    monkeypatch.chdir(tmp_path)
    env_key = Fernet.generate_key().decode()
    monkeypatch.setenv("ENCRYPTION_KEY", env_key)
    crypto._fernet.cache_clear()
    try:
        token = crypto.encrypt_str("secret")
        assert Fernet(env_key.encode()).decrypt(token.encode()) == b"secret"
    finally:
        crypto._fernet.cache_clear()
