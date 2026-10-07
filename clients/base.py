"""Shared HTTP client factory and structured request/response logging.

Used by `clients/output.py` (output API) and the future `clients/enphase.py`.
Growatt uses a vendor SDK (`growattServer.OpenApiV1`) via `clients/growatt.py`,
not httpx — so this module does not apply there.

The factory creates a single `httpx.AsyncClient` per process, configured with
sensible defaults.  Callers create the client once at startup and reuse it.

No retry logic here — that lives in `clients/output.py` (Step 8).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from httpx import Request, Response

logger = logging.getLogger(__name__)

# Default timeout: generous enough for API gateways, tight enough to fail fast.
DEFAULT_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)


def create_client(
    *,
    base_url: str = "",
    headers: dict[str, str] | None = None,
    timeout: httpx.Timeout = DEFAULT_TIMEOUT,
) -> httpx.AsyncClient:
    """Create a configured async HTTP client.

    Args:
        base_url:  If set, all request URLs are relative to this base.
        headers:   Default headers sent on every request (e.g. auth tokens).
        timeout:   Override the default timeout if the target API is slow.

    Returns:
        An httpx.AsyncClient ready for use.  Caller is responsible for
        `async with client` or `await client.aclose()`.
    """
    return httpx.AsyncClient(
        base_url=base_url,
        headers=headers or {},
        timeout=timeout,
        follow_redirects=False,
    )


def log_request(request: Request) -> None:
    """Log an outgoing HTTP request (DEBUG level)."""
    body_size = len(request.content) if request.content else 0
    logger.debug(
        "HTTP → %s %s (body=%d bytes)",
        request.method,
        request.url,
        body_size,
    )


def log_response(response: Response, *, elapsed: float) -> None:
    """Log an HTTP response with status and duration (DEBUG for 2xx, WARNING for 4xx+)."""
    level = logging.WARNING if response.status_code >= 400 else logging.DEBUG
    body_size = len(response.content) if response.content else 0
    logger.log(
        level,
        "HTTP ← %d %s %s (%.3fs, %d bytes)",
        response.status_code,
        response.request.method,
        response.url,
        elapsed,
        body_size,
    )


class TimedClient(httpx.AsyncClient):
    """httpx.AsyncClient subclass that emits structured request/response logs.

    Drop-in replacement for httpx.AsyncClient.  Logs every request at DEBUG
    and every response with status-dependent level.  Defaults to
    `DEFAULT_TIMEOUT` rather than httpx's 5 s, which is too short for the
    EnPhase API; pass `timeout=` to override.
    """

    def __init__(self, *, timeout: httpx.Timeout = DEFAULT_TIMEOUT, **kwargs) -> None:
        super().__init__(timeout=timeout, **kwargs)

    async def send(self, request: Request, **kwargs) -> Response:  # type: ignore[override]
        log_request(request)
        start = time.monotonic()
        response = await super().send(request, **kwargs)
        elapsed = time.monotonic() - start
        log_response(response, elapsed=elapsed)
        return response
