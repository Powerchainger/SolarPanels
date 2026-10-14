"""Container health check: is the service's event loop still alive?

The lease heartbeat (`leases.heartbeat`) touches ALIVE_FILE on every tick.
This exits non-zero when that file is missing or older than a few ticks:
the event loop hangs, or the heartbeat died (then leases are no longer
renewed either). It says nothing about the providers or the output API;
`manage_keys.py list` shows those.

Standard library only, so it starts fast. Run by docker-compose.yml:
    python healthcheck.py
"""

from __future__ import annotations

import os
import sys
import time

ALIVE_FILE = "/tmp/solar-scraper.alive"

# Heartbeat ticks that may be missed before the service counts as unhealthy.
MISSED_TICKS = 4


def check() -> str | None:
    """Return why the service is unhealthy, or None if it is healthy."""
    interval = int(os.environ.get("LEASE_RENEWAL_INTERVAL", "30"))
    try:
        age = time.time() - os.path.getmtime(ALIVE_FILE)
    except OSError:
        return "no heartbeat yet"
    if age > MISSED_TICKS * interval:
        return f"last heartbeat {age:.0f}s ago"
    return None


if __name__ == "__main__":
    problem = check()
    if problem:
        print(problem)
        sys.exit(1)
