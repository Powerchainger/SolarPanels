#!/usr/bin/env -S uv run
"""Development tools: echo server + live fetch cycle tests.

Commands:
    dev.py serve                          Echo server — receives POSTs, prints to terminal
    dev.py test-growatt --account-id X    One Growatt fetch + transform, dry run
    dev.py test-enphase --account-id X    One EnPhase fetch + transform, dry run

MongoDB must be running for test commands (podman compose up mongo).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import typer
from dotenv import load_dotenv

load_dotenv()  # dev.py reads MONGO_URI via typer envvar — uv does not load .env

app = typer.Typer(help="Development tools for solar-scraper.")

# ---------------------------------------------------------------------------
# Echo server (shared between serve + test commands)
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


_request_count = 0


class _EchoHandler(BaseHTTPRequestHandler):
    """Accepts any method, prints the request, returns 201."""

    def do_POST(self):  # noqa: N802
        global _request_count
        _request_count += 1
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8")

            try:
                parsed = json.loads(body)
                typer.echo(f"\n{'─'*60}")
                typer.echo(f"  POST {self.path} (#{_request_count})")
                typer.echo(f"  {json.dumps(parsed, indent=2, default=str)}")
            except (json.JSONDecodeError, ValueError):
                typer.echo(f"\n  POST {self.path} ({length} bytes)")
                typer.echo(f"  {body[:500]}")

            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"id":"echo-1","status":"received"}')
        except Exception:
            import traceback
            typer.echo(f"\n  POST {self.path} (#{_request_count}) FAILED:", err=True)
            traceback.print_exc()
            try:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"internal"}')
            except Exception:
                pass

    def do_GET(self):  # noqa: N802
        """Health check endpoint."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format, *args):
        typer.echo(f"  [server] {format % args}", err=True)


def _start_echo_server(host: str = "0.0.0.0", port: int = 0) -> tuple[HTTPServer, int]:
    """Start echo server on host:port (port=0 → random free port).

    Returns (server, actual_port). Server is already running in a daemon thread.
    """
    actual_port = port or _find_free_port()
    server = HTTPServer((host, actual_port), _EchoHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, actual_port


# ---------------------------------------------------------------------------
# serve — standalone echo server
# ---------------------------------------------------------------------------


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host", help="Bind address"),
    port: int = typer.Option(8080, "--port", "-p", help="Port (0 = random)"),
) -> None:
    """Start an echo server that prints received POSTs to the terminal.

    Set OUTPUT_API_URL=http://127.0.0.1:8080 in .env to point main.py here.
    """
    server, actual_port = _start_echo_server(host, port)
    typer.echo(f"Echo server listening on {host}:{actual_port}")
    typer.echo(f"Set OUTPUT_API_URL=http://<host-ip>:{actual_port} in .env")
    typer.echo("Press Ctrl-C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        typer.echo("\nShutting down.")
        server.shutdown()


# ---------------------------------------------------------------------------
# test-growatt / test-enphase — single-cycle tests
# ---------------------------------------------------------------------------


@app.command("test-growatt")
def test_growatt(
    account_id: str = typer.Option(..., "--account-id", help="Growatt account ID seeded in Mongo"),
    mongo_uri: str = typer.Option("mongodb://localhost:27017", "--mongo-uri", envvar="MONGO_URI"),
    log_level: str = typer.Option("DEBUG", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Run a single live Growatt fetch cycle (dry run, nothing is delivered)."""
    logging.basicConfig(level=log_level, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(_run_cycle("growatt", account_id, mongo_uri))


@app.command("test-enphase")
def test_enphase(
    account_id: str = typer.Option(..., "--account-id", help="EnPhase account ID seeded in Mongo"),
    mongo_uri: str = typer.Option("mongodb://localhost:27017", "--mongo-uri", envvar="MONGO_URI"),
    log_level: str = typer.Option("DEBUG", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Run a single live EnPhase fetch cycle (dry run, nothing is delivered)."""
    logging.basicConfig(level=log_level, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(_run_cycle("enphase", account_id, mongo_uri))


# Why a test must not run alongside the service, per provider.
_LEASE_REASON = {
    "enphase": "if both refresh the same token at once, one of them can lose it.",
    "growatt": "Growatt allows about one call per endpoint per 5 minutes, so a "
               "test now can make the service's poll fail with a rate-limit error.",
}

_DEFAULT_LEASE_REASON = "both would call the provider and update the account at once."


def _live_lease_owner(raw_doc: dict[str, Any], now: datetime) -> str | None:
    """The worker holding an unexpired lease on the account, if any."""
    lease = raw_doc.get("lease") or {}
    expires_at = lease.get("expires_at")
    if not isinstance(expires_at, datetime):
        return None
    if expires_at.tzinfo is None:  # Mongo returns naive UTC
        expires_at = expires_at.replace(tzinfo=UTC)
    if expires_at <= now:
        return None
    return lease.get("worker_id") or "?"


def _describe(record: dict[str, Any]) -> str:
    """Field names and inverter counts, never values."""
    parts = []
    for key, value in record.items():
        if isinstance(value, dict):
            nulls = sum(v is None for v in value.values())
            parts.append(f"{key}[{len(value)}, {nulls} null]")
        else:
            parts.append(key if value is not None else f"{key}=null")
    return ", ".join(parts)


async def _run_cycle(provider: str, account_id: str, mongo_uri: str) -> None:
    """One provider fetch + transform, without the delivery side of fetch_job.

    Writes no records and no health fields, so running it on the production
    machine cannot mark anything delivered that the output API never got.
    The provider's own state is still stored (refreshed EnPhase tokens must
    be, or a rotated refresh token is lost; site and device caches), so it
    refuses to run while a worker holds the account's lease: the service
    and this command must not refresh the same token at once.
    """
    from clients.base import TimedClient
    from db.models import ApiConfig
    from db.mongo import get_db
    from providers import get_spec

    doc_id = f"{provider}::{account_id}"
    typer.echo(f"\n{'='*60}")
    typer.echo(f"  dev: {provider} live cycle (dry run)")
    typer.echo(f"  account_id  : {account_id}")
    typer.echo(f"{'='*60}\n")

    db = await get_db(mongo_uri)
    raw_doc = await db.api_configs.find_one({"_id": doc_id})
    if raw_doc is None:
        typer.echo(f"✗ no account {doc_id} (see manage_keys.py list)", err=True)
        raise typer.Exit(code=1)
    owner = _live_lease_owner(raw_doc, datetime.now(UTC))
    if owner is not None:
        typer.echo(
            f"✗ {doc_id} is held by worker {owner!r}, so the service is polling "
            f"it: {_LEASE_REASON.get(provider, _DEFAULT_LEASE_REASON)} Check its next poll with "
            f"`manage_keys.py list` instead, or stop the service first "
            f"(`podman compose stop app`).",
            err=True,
        )
        raise typer.Exit(code=1)
    raw_doc.pop("_id", None)
    api_config = ApiConfig(**raw_doc)
    if api_config.auth_failed:
        typer.echo("  ! account is frozen (auth_failed): re-seed it first, see accounts docs")

    spec = get_spec(provider)
    context: dict[str, Any] = {"timestamp": datetime.now(UTC), "account_id": account_id}
    try:
        async with TimedClient() as http_client:
            typer.echo(f"→ Fetching (live {provider} API call)...")
            raw_responses = await spec.fetch(api_config, db, http_client)
        typer.echo(f"  raw responses : {len(raw_responses)}")
        records = spec.transform(raw_responses, context)
        typer.echo(f"  records       : {len(records)}")
        for i, record in enumerate(records, start=1):
            typer.echo(f"  record {i}      : {_describe(record)}")
    except Exception as e:
        typer.echo(f"\n✗ ERROR: {type(e).__name__}: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        typer.echo(f"\n{'='*60}")
        typer.echo("  Done. Nothing was stored as a record or sent anywhere.")
        typer.echo(f"{'='*60}")


if __name__ == "__main__":
    app()
