"""Development tools: echo server + live fetch cycle tests.

Commands:
    dev.py serve                          Echo server — receives POSTs, prints to terminal
    dev.py test-growatt --account-id X    One Growatt fetch cycle (in-process echo server)
    dev.py test-enphase --account-id X    One EnPhase fetch cycle (in-process echo server)

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


def _start_echo_server(host: str = "127.0.0.1", port: int = 0) -> tuple[HTTPServer, int]:
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
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address"),
    port: int = typer.Option(8080, "--port", "-p", help="Port (0 = random)"),
) -> None:
    """Start an echo server that prints received POSTs to the terminal.

    Set OUTPUT_API_URL=http://127.0.0.1:8080 in .env to point main.py here.
    """
    server, actual_port = _start_echo_server(host, port)
    typer.echo(f"Echo server listening on {host}:{actual_port}")
    typer.echo(f"Set OUTPUT_API_URL=http://127.0.0.1:{actual_port} in .env")
    typer.echo("Press Ctrl-C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        typer.echo("\nShutting down.")
        server.shutdown()


# ---------------------------------------------------------------------------
# test-growatt / test-enphase — single-cycle tests
# ---------------------------------------------------------------------------


def _common_options(f):
    """Shared CLI options for test commands."""
    return (
        f,
        typer.Option(..., "--account-id", help="Account ID seeded in Mongo"),
        typer.Option("mongodb://localhost:27017", "--mongo-uri", envvar="MONGO_URI"),
        typer.Option("DEBUG", "--log-level", envvar="LOG_LEVEL"),
    )


@app.command("test-growatt")
def test_growatt(
    account_id: str = typer.Option(..., "--account-id", help="Growatt account ID seeded in Mongo"),
    mongo_uri: str = typer.Option("mongodb://localhost:27017", "--mongo-uri", envvar="MONGO_URI"),
    log_level: str = typer.Option("DEBUG", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Run a single live Growatt fetch cycle."""
    logging.basicConfig(level=log_level, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(_run_cycle("growatt", account_id, mongo_uri))


@app.command("test-enphase")
def test_enphase(
    account_id: str = typer.Option(..., "--account-id", help="EnPhase account ID seeded in Mongo"),
    mongo_uri: str = typer.Option("mongodb://localhost:27017", "--mongo-uri", envvar="MONGO_URI"),
    log_level: str = typer.Option("DEBUG", "--log-level", envvar="LOG_LEVEL"),
) -> None:
    """Run a single live EnPhase fetch cycle."""
    logging.basicConfig(level=log_level, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(_run_cycle("enphase", account_id, mongo_uri))


async def _run_cycle(provider: str, account_id: str, mongo_uri: str) -> None:
    """Shared test cycle logic for both providers."""
    from clients.base import TimedClient
    from db.mongo import get_db
    from scheduler.jobs import fetch_job

    echo_server, echo_port = _start_echo_server()
    echo_url = f"http://127.0.0.1:{echo_port}"
    received_posts: list[dict[str, Any]] = []

    # Patch _EchoHandler to capture posts in this scope.
    original_do_post = _EchoHandler.do_POST

    def _capturing_post(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        received_posts.append({"path": self.path, "body": body})
        try:
            parsed = json.loads(body)
            typer.echo(f"  [echo] POST {self.path}")
            typer.echo(f"  {json.dumps(parsed, indent=2, default=str)}")
        except (json.JSONDecodeError, ValueError):
            typer.echo(f"  [echo] POST {self.path} ({length} bytes)")
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"id":"echo-1","status":"received"}')

    _EchoHandler.do_POST = _capturing_post  # type: ignore

    typer.echo(f"\n{'='*60}")
    typer.echo(f"  dev: {provider} live cycle")
    typer.echo(f"  account_id  : {account_id}")
    typer.echo(f"  echo server : {echo_url}")
    typer.echo(f"  mongo_uri   : {mongo_uri}")
    typer.echo(f"{'='*60}\n")

    db = await get_db(mongo_uri)

    try:
        async with TimedClient() as http_client:
            before_raw = await db.raw_responses.count_documents({"provider": provider, "account_id": account_id})
            before_proc = await db.processed_results.count_documents({"provider": provider, "account_id": account_id})

            typer.echo(f"→ Running fetch_job (live {provider} API call)...")
            await fetch_job(
                db=db,
                provider=provider,
                account_id=account_id,
                output_base_url=echo_url,
                output_token="",
                http_client=http_client,
            )

            typer.echo("\n→ Results:")
            typer.echo(f"  raw_responses      : {await db.raw_responses.count_documents({'provider': provider, 'account_id': account_id}) - before_raw} new")  # noqa: E501
            typer.echo(f"  processed_results  : {await db.processed_results.count_documents({'provider': provider, 'account_id': account_id}) - before_proc} new")  # noqa: E501
            typer.echo(f"  echo POSTs received: {len(received_posts)}")

            typer.echo("\n→ Processed record statuses:")
            cursor = db.processed_results.find(
                {"provider": provider, "account_id": account_id},
                {"status": 1, "last_error": 1, "retry_count": 1, "record": 1},
            ).sort("created_at", -1).limit(5)
            async for doc in cursor:
                rec = doc.get("record", {})
                typer.echo(
                    f"  status={doc.get('status')}  "
                    f"retry_count={doc.get('retry_count', 0)}  "
                    f"last_error={doc.get('last_error')}  "
                    f"record={json.dumps(rec, default=str)[:120]}"
                )

    except Exception as e:
        typer.echo(f"\n✗ ERROR: {e}", err=True)
        import traceback
        traceback.print_exc()
        raise typer.Exit(code=1)
    finally:
        _EchoHandler.do_POST = original_do_post
        echo_server.shutdown()
        typer.echo(f"\n{'='*60}")
        typer.echo("  Done.")
        typer.echo(f"{'='*60}")


if __name__ == "__main__":
    app()
