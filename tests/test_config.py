"""Tests for config.py settings parsing."""

from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from config import Settings

REQUIRED = {"MONGO_URI": "mongodb://x", "OUTPUT_API_URL": "http://x", "WORKER_ID": "w"}


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
