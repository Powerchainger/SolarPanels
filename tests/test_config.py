"""Tests for config.py settings parsing."""

from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from config import Settings

REQUIRED = {"MONGO_URI": "mongodb://x", "OUTPUT_API_URL": "http://x"}


@pytest.fixture
def env(monkeypatch):
    for k, v in REQUIRED.items():
        monkeypatch.setenv(k, v)
    return monkeypatch


def _settings() -> Settings:
    return Settings(_env_file=None)


def test_posted_ttl_days_defaults_to_seven(env):
    env.delenv("POSTED_TTL_DAYS", raising=False)
    assert _settings().posted_ttl_days == 7


@pytest.mark.parametrize("value", ["inf", "infinity", "Infinity"])
def test_posted_ttl_days_accepts_infinity(env, value):
    env.setenv("POSTED_TTL_DAYS", value)
    assert math.isinf(_settings().posted_ttl_days)


@pytest.mark.parametrize("value", ["0", "-1", "30000"])
def test_posted_ttl_days_rejects_invalid(env, value):
    """Zero/negative make no sense; 30000 days exceeds MongoDB's TTL maximum."""
    env.setenv("POSTED_TTL_DAYS", value)
    with pytest.raises(ValidationError):
        _settings()


def test_invalid_encryption_key_is_not_echoed_in_the_error(env):
    """The error is printed before logging is set up and ends up in podman logs."""
    env.setenv("ENCRYPTION_KEY", "not-a-real-key-SENTINEL")
    with pytest.raises(ValidationError) as exc_info:
        _settings()
    assert "SENTINEL" not in str(exc_info.value)


def test_missing_setting_does_not_echo_the_other_settings(env):
    env.setenv("ENCRYPTION_KEY", "not-a-real-key-SENTINEL")
    env.setenv("MONGO_URI", "mongodb://user:PASSWORD@host")
    env.delenv("OUTPUT_API_URL")
    with pytest.raises(ValidationError) as exc_info:
        _settings()
    assert "SENTINEL" not in str(exc_info.value)
    assert "PASSWORD" not in str(exc_info.value)


def test_worker_id_is_optional(env):
    env.delenv("WORKER_ID", raising=False)
    assert _settings().worker_id == ""
