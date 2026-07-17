"""Tests for db/models.py — no database or network required."""

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
    assert cfg.device_filter is None


def test_growatt_api_config_defaults():
    cfg = _growatt_config()
    assert cfg.provider == "growatt"
    assert isinstance(cfg.credentials, GrowattCredentials)


def test_api_config_with_lease():
    cfg = _enphase_config(lease={"worker_id": "worker-1", "expires_at": NOW})
    assert isinstance(cfg.lease, Lease)
    assert cfg.lease.worker_id == "worker-1"


def test_api_config_with_site_cache():
    cfg = _enphase_config(site_cache={"site_id": 42, "user": "Sunnyville"})
    assert isinstance(cfg.site_cache, SiteCache)
    assert cfg.site_cache.site_id == 42
    assert cfg.site_cache.user == "Sunnyville"


def test_api_config_with_device_filter():
    cfg = _growatt_config(device_filter={"type": 7})
    assert cfg.device_filter == {"type": 7}


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
