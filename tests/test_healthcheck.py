"""Tests for healthcheck.py."""

from __future__ import annotations

import os
import time

import pytest

import healthcheck


@pytest.fixture
def alive(tmp_path, monkeypatch):
    path = tmp_path / "alive"
    monkeypatch.setattr(healthcheck, "ALIVE_FILE", str(path))
    monkeypatch.setenv("LEASE_RENEWAL_INTERVAL", "30")
    return path


def test_healthy_when_heartbeat_is_recent(alive):
    alive.touch()
    assert healthcheck.check() is None


def test_unhealthy_without_heartbeat(alive):
    assert healthcheck.check() == "no heartbeat yet"


def test_unhealthy_when_heartbeat_is_stale(alive):
    alive.touch()
    old = time.time() - 4 * 30 - 1
    os.utime(alive, (old, old))
    assert "last heartbeat" in healthcheck.check()


def test_stale_limit_follows_renewal_interval(alive, monkeypatch):
    """A longer LEASE_RENEWAL_INTERVAL must not make a healthy service look dead."""
    monkeypatch.setenv("LEASE_RENEWAL_INTERVAL", "300")
    alive.touch()
    old = time.time() - 200
    os.utime(alive, (old, old))
    assert healthcheck.check() is None
