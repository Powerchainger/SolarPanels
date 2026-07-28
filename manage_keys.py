"""CLI for managing api_configs documents in MongoDB.

Commands:
  enphase bootstrap  Seed an EnPhase account with OAuth2 client credentials.
                     After this, the running service will handle token exchange.
  growatt set        Create/update a Growatt account with a static API token.
  list               Show all api_configs documents.
  delete             Remove an api_configs document by (provider, account_id).

No live API calls are made by this script — it only reads/writes MongoDB.

Sensitive values (client_secret, access/refresh tokens, api_token) are
prompted with hidden input when not passed via flags, preventing them
from appearing in shell history.  Alternatively, use --from-file with
a JSON file to supply all values at once (also no history).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typer
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from db.models import ApiConfig, EnphaseCredentials, GrowattCredentials

app = typer.Typer(name="manage-keys", help="Manage solar-scraper api_configs.")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "solar_scraper"


def _mongo():
    client: AsyncMongoClient = AsyncMongoClient(MONGO_URI)
    return client[DB_NAME]


def _prompt_secret(label: str) -> str:
    """Prompt for a sensitive value with hidden input."""
    return typer.prompt(label, hide_input=True)


# ---------------------------------------------------------------------------
# enphase bootstrap
# ---------------------------------------------------------------------------


@app.command("enphase-bootstrap")
def enphase_bootstrap(
    account_id: str = typer.Option(..., "--account-id", help="Unique account identifier"),
    client_id: str | None = typer.Option(None, "--client-id", help="EnPhase OAuth client_id"),
    client_secret: str | None = typer.Option(
        None, "--client-secret",
        help="EnPhase OAuth client secret (prompted with hidden input if omitted)",
    ),
    redirect_uri: str | None = typer.Option(None, "--redirect-uri", help="EnPhase OAuth redirect_uri"),
    api_key: str | None = typer.Option(None, "--api-key", help="EnPhase API key"),
    access_token: str | None = typer.Option(
        None, "--access-token",
        help="Pre-exchanged access token (prompted with hidden input if omitted)",
    ),
    refresh_token: str | None = typer.Option(
        None, "--refresh-token",
        help="Pre-exchanged refresh token (prompted with hidden input if omitted)",
    ),
    expires_in: int | None = typer.Option(None, "--expires-in", help="Token lifetime in seconds (required with --access-token)"),
    from_file: Path | None = typer.Option(
        None, "--from-file",
        help="Read all credential values from a JSON file instead of flags/prompts",
        exists=True,
    ),
    interval: int = typer.Option(3600, "--interval", help="Poll interval in seconds"),
    enabled: bool = typer.Option(True, "--enabled/--disabled"),
) -> None:
    """Seed an EnPhase account.

    Sensitive fields (client_secret, access_token, refresh_token) are
    prompted with hidden input if not passed as flags.  Use --from-file
    to load all values from a JSON file (no prompts, no history).

    JSON file format:
    {
      "client_id": "...", "client_secret": "...",
      "redirect_uri": "...", "api_key": "...",
      "access_token": "...", "refresh_token": "...",
      "expires_in": 86399
    }
    """
    # Load from file if provided — overrides all flags except account_id/interval/enabled.
    if from_file:
        data = json.loads(from_file.read_text())
        client_id = data.get("client_id", client_id)
        client_secret = data.get("client_secret", client_secret)
        redirect_uri = data.get("redirect_uri", redirect_uri)
        api_key = data.get("api_key", api_key)
        access_token = data.get("access_token", access_token)
        refresh_token = data.get("refresh_token", refresh_token)
        expires_in = data.get("expires_in", expires_in)

    # Prompt for missing non-sensitive fields (plain input).
    if not client_id:
        client_id = typer.prompt("Client ID")
    if not redirect_uri:
        redirect_uri = typer.prompt("Redirect URI")
    if not api_key:
        api_key = typer.prompt("API key")

    # Prompt for missing sensitive fields (hidden input).
    if not client_secret:
        client_secret = _prompt_secret("Client secret")
    if not access_token:
        access_token = _prompt_secret("Access token")
    if not refresh_token:
        refresh_token = _prompt_secret("Refresh token")

    expires_at = None
    if access_token and expires_in:
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)

    creds = EnphaseCredentials(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        api_key=api_key,
        access_token=access_token or "",
        refresh_token=refresh_token or "",
        expires_in=expires_in,
        expires_at=expires_at,
    )
    cfg = ApiConfig(
        provider="enphase",
        account_id=account_id,
        interval_seconds=interval,
        credentials=creds,
        enabled=enabled,
    )
    asyncio.run(_upsert(cfg))
    typer.echo(f"✓ enphase/{account_id} written")


# ---------------------------------------------------------------------------
# growatt set
# ---------------------------------------------------------------------------


@app.command("growatt-set")
def growatt_set(
    account_id: str = typer.Option(..., "--account-id", help="Unique account identifier"),
    api_token: str | None = typer.Option(
        None, "--api-token",
        help="Growatt API token (prompted with hidden input if omitted)",
    ),
    from_file: Path | None = typer.Option(
        None, "--from-file",
        help="Read values from a JSON file: {\"api_token\": \"...\"}",
        exists=True,
    ),
    interval: int = typer.Option(360, "--interval", help="Poll interval in seconds"),
    enabled: bool = typer.Option(True, "--enabled/--disabled"),
) -> None:
    """Create or update a Growatt account with a static API token.

    api_token is prompted with hidden input if not passed as a flag.
    Use --from-file to load from a JSON file (no prompts, no history).
    """
    if from_file:
        data = json.loads(from_file.read_text())
        api_token = data.get("api_token", api_token)

    if not api_token:
        api_token = _prompt_secret("API token")

    creds = GrowattCredentials(api_token=api_token)
    cfg = ApiConfig(
        provider="growatt",
        account_id=account_id,
        interval_seconds=interval,
        credentials=creds,
        enabled=enabled,
    )
    asyncio.run(_upsert(cfg))
    typer.echo(f"✓ growatt/{account_id} written")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command("list")
def list_configs() -> None:
    """Show all api_configs documents."""
    asyncio.run(_list())


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


@app.command("delete")
def delete(
    provider: str = typer.Option(..., "--provider", help="Provider name (enphase|growatt)"),
    account_id: str = typer.Option(..., "--account-id", help="Account ID to remove"),
) -> None:
    """Delete an api_configs document by (provider, account_id)."""
    asyncio.run(_delete(provider, account_id))


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _composite_id(provider: str, account_id: str) -> str:
    return f"{provider}::{account_id}"


async def _upsert(cfg: ApiConfig) -> None:
    db = _mongo()
    doc = cfg.model_dump(mode="json")
    doc["_id"] = _composite_id(cfg.provider, cfg.account_id)
    doc["updated_at"] = datetime.now(UTC)
    await db.api_configs.replace_one({"_id": doc["_id"]}, doc, upsert=True)


async def _list() -> None:
    db = _mongo()
    col = db.api_configs
    cursor = col.find({}, {"_id": 1, "provider": 1, "account_id": 1,
                          "enabled": 1, "interval_seconds": 1})
    count = 0
    async for doc in cursor:
        typer.echo(
            f"  {doc['provider']}/{doc['account_id']}"
            f"  interval={doc['interval_seconds']}s"
            f"  enabled={doc['enabled']}"
        )
        count += 1
    if count == 0:
        typer.echo("  (no configs found)")
    typer.echo(f"\n  {count} config(s) total")


async def _delete(provider: str, account_id: str) -> None:
    db = _mongo()
    _id = _composite_id(provider, account_id)
    result = await db.api_configs.delete_one({"_id": _id})
    if result.deleted_count == 0:
        typer.echo(f"✗ {provider}/{account_id} not found", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"✓ {provider}/{account_id} deleted")


if __name__ == "__main__":
    app()
