"""CLI for managing api_configs documents in MongoDB.

Commands:
  enphase bootstrap  Seed an EnPhase account with OAuth2 client credentials.
                     After this, the running service will handle token exchange.
  growatt set        Create/update a Growatt account with a static API token.
  list               Show all api_configs documents.
  delete             Remove an api_configs document by (provider, account_id).

No live API calls are made by this script — it only reads/writes MongoDB.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC, datetime

import typer
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from db.models import ApiConfig, EnphaseCredentials, GrowattCredentials

app = typer.Typer(name="manage-keys", help="Manage solar-scraper api_configs.")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "solar_scraper"


def _mongo():
    client: AsyncMongoClient = AsyncMongoClient(MONGO_URI)
    return client[DB_NAME]


# ---------------------------------------------------------------------------
# enphase bootstrap
# ---------------------------------------------------------------------------


@app.command("enphase-bootstrap")
def enphase_bootstrap(
    account_id: str = typer.Option(..., "--account-id", help="Unique account identifier"),
    client_id: str = typer.Option(..., "--client-id", help="EnPhase OAuth client_id"),
    client_secret: str = typer.Option(..., "--client-secret", help="EnPhase OAuth client_secret"),
    redirect_uri: str = typer.Option(..., "--redirect-uri", help="EnPhase OAuth redirect_uri"),
    api_key: str = typer.Option(..., "--api-key", help="EnPhase API key"),
    interval: int = typer.Option(3600, "--interval", help="Poll interval in seconds"),
    enabled: bool = typer.Option(True, "--enabled/--disabled"),
) -> None:
    """Seed an EnPhase account. Tokens will be exchanged by the running service."""
    creds = EnphaseCredentials(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        api_key=api_key,
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
    api_token: str = typer.Option(..., "--api-token", help="Growatt API token (static)"),
    interval: int = typer.Option(300, "--interval", help="Poll interval in seconds"),
    enabled: bool = typer.Option(True, "--enabled/--disabled"),
) -> None:
    """Create or update a Growatt account with a static API token."""
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
