"""CLI for managing api_configs documents in MongoDB.

Commands:
  enphase set      Seed an EnPhase account with OAuth2 client credentials.
                   After this, the running service will handle token exchange.
  growatt set      Create/update a Growatt account with a static API token.
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
from dotenv import load_dotenv
from pymongo.asynchronous.mongo_client import AsyncMongoClient

from crypto import SECRET_FIELDS, decrypt_str, encrypt_str, is_encrypted
from db.models import (
    MIN_INTERVAL_SECONDS,
    ApiConfig,
    EnphaseCredentials,
    GrowattCredentials,
)

load_dotenv()  # uv does not auto-load .env — without this, MONGO_URI falls
# back to unauthenticated localhost once Mongo requires auth.

app = typer.Typer(name="manage-keys", help="Manage solar-scraper api_configs.")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "solar_scraper"


def _mongo():
    client: AsyncMongoClient = AsyncMongoClient(MONGO_URI)
    return client[DB_NAME]


def _prompt_secret(label: str) -> str:
    """Prompt for a sensitive value with hidden input.

    Shows character count after entry; warns if empty.
    """
    value = typer.prompt(label, hide_input=True)
    if not value:
        typer.echo("\u26a0 Warning: value is empty", err=True)
    else:
        typer.echo(f"\u2713 {len(value)} characters entered")
    return value


# ---------------------------------------------------------------------------
# enphase set
# ---------------------------------------------------------------------------


@app.command("enphase-set")
def enphase_set(
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
    interval: int = typer.Option(
        3600, "--interval", min=MIN_INTERVAL_SECONDS, help="Poll interval in seconds"
    ),
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
    interval: int = typer.Option(
        600, "--interval", min=MIN_INTERVAL_SECONDS, help="Poll interval in seconds"
    ),
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
# migrate-encrypt
# ---------------------------------------------------------------------------


def _transform_credentials(creds: dict, *, decrypt: bool) -> dict:
    """Encrypt (or decrypt) the secret fields of one credentials dict in place.

    Idempotent: fields already in the target state (ciphertext on the
    encrypt pass, plaintext on the decrypt pass) are left untouched.
    Non-secret fields (client_id, redirect_uri, …) pass through unchanged.
    """
    out = dict(creds)
    for field in SECRET_FIELDS.get(creds.get("provider", ""), []):
        value = creds.get(field)
        if not isinstance(value, str) or not value:
            continue
        if decrypt:
            if is_encrypted(value):
                out[field] = decrypt_str(value)
        elif not is_encrypted(value):
            out[field] = encrypt_str(value)
    return out


@app.command("migrate-encrypt")
def migrate_encrypt(
    decrypt: bool = typer.Option(
        False, "--decrypt",
        help="Reverse pass: decrypt ciphertext back to plaintext (rollback)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Report what would change without writing to MongoDB",
    ),
) -> None:
    """Encrypt (or decrypt with --decrypt) credential fields in all api_configs docs.

    Idempotent and surgical: only the SECRET_FIELDS values change; _id,
    updated_at, site_cache, lease and all BSON types are untouched (no
    model round-trip). Run with the service stopped, AFTER a mongodump
    backup to ./backups/.

    Rotation path: with the OLD key set, run `--decrypt`; then set the new
    ENCRYPTION_KEY and run the encrypt pass. A plain encrypt pass with the
    new key does not rotate: it skips values that are already ciphertext.
    """
    asyncio.run(_migrate(decrypt=decrypt, dry_run=dry_run))


async def _migrate(*, decrypt: bool, dry_run: bool) -> None:
    db = _mongo()
    col = db.api_configs
    changed = 0
    total = 0
    skipped = 0
    cursor = col.find({})
    async for doc in cursor:
        total += 1
        creds = doc.get("credentials")
        if not isinstance(creds, dict):
            skipped += 1
            continue
        new_creds = _transform_credentials(creds, decrypt=decrypt)
        if new_creds != creds:
            changed += 1
            if not dry_run:
                res = await col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"credentials": new_creds}},
                )
                if res.matched_count == 0:
                    typer.echo(
                        f"  ! warning: no doc matched _id {doc['_id']!r} — "
                        "write skipped",
                        err=True,
                    )
    verb = "decrypted" if decrypt else "encrypted"
    if dry_run:
        typer.echo(
            f"[dry-run] {changed}/{total} api_configs doc(s) would be {verb}"
        )
    else:
        typer.echo(f"{changed}/{total} api_configs doc(s) {verb}")
    if skipped:
        typer.echo(f"  {skipped} doc(s) skipped (no credentials dict)")


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


# Runtime state a running worker keeps on the document. Re-seeding must not
# touch it, or the worker silently loses its lease (and re-pays the cache).
_CACHE_FIELD = {"enphase": "site_cache", "growatt": "growatt_cache"}


def _cache_stale(existing: dict, cfg: ApiConfig) -> bool:
    """Whether the provider's cache may describe a different account now.

    EnPhase: always — the cached site_id is never re-validated, and a wrong
    one yields wrong data without any error. Re-resolving costs one /systems
    call. Growatt: only when the api_token changed (it is the account's
    identity; the cache also expires on its own after 24 h). Old credentials
    that cannot be decrypted count as a change.
    """
    if cfg.provider == "enphase":
        return True
    try:
        old = GrowattCredentials.model_validate(existing["credentials"]).api_token
    except Exception:  # noqa: BLE001
        return True
    return old != cfg.credentials.api_token


def _plan_upsert(existing: dict | None, cfg: ApiConfig) -> dict:
    """Build the update document for (re-)seeding one account.

    New account: everything is written. Existing account: only the fields the
    CLI controls are `$set`, so `lease` survives; the provider's cache is
    dropped when `_cache_stale` says so.
    """
    doc = cfg.model_dump(mode="json")
    doc["updated_at"] = datetime.now(UTC)
    doc["seeded_at"] = doc["updated_at"]
    # Re-seeding means new credentials, so a previous auth failure is cleared.
    controlled = {
        k: doc[k]
        for k in (
            "credentials", "interval_seconds", "enabled", "updated_at", "seeded_at",
            "auth_failed", "consecutive_failures", "consecutive_auth_failures",
        )
    }
    update: dict = {"$set": controlled}
    if existing is None:
        update["$setOnInsert"] = {k: v for k, v in doc.items() if k not in controlled}
    elif cfg.provider in _CACHE_FIELD and _cache_stale(existing, cfg):
        update["$unset"] = {_CACHE_FIELD[cfg.provider]: ""}
    return update


async def _upsert(cfg: ApiConfig) -> None:
    db = _mongo()
    _id = _composite_id(cfg.provider, cfg.account_id)
    existing = await db.api_configs.find_one({"_id": _id})
    await db.api_configs.update_one({"_id": _id}, _plan_upsert(existing, cfg), upsert=True)


def _health(doc: dict) -> str:
    """One-line health summary of an api_configs document."""
    last = doc.get("last_success_at")
    parts = [f"last_success={last.strftime('%Y-%m-%d %H:%M UTC') if last else 'never'}"]
    if doc.get("consecutive_failures"):
        parts.append(f"failures={doc['consecutive_failures']}")
    if doc.get("auth_failed"):
        parts.append("AUTH FAILED — re-authorize and re-seed")
    return "  ".join(parts)


async def _list() -> None:
    db = _mongo()
    col = db.api_configs
    cursor = col.find({}, {"_id": 1, "provider": 1, "account_id": 1,
                          "enabled": 1, "interval_seconds": 1, "auth_failed": 1,
                          "last_success_at": 1, "consecutive_failures": 1})
    count = 0
    async for doc in cursor:
        typer.echo(
            f"  {doc['provider']}/{doc['account_id']}"
            f"  interval={doc['interval_seconds']}s"
            f"  enabled={doc['enabled']}"
            f"  {_health(doc)}"
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
