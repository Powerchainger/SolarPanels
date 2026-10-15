# Adding a provider

This page walks through adding a third vendor next to EnPhase and Growatt.
The examples use a made-up provider called `acme`. Read
[Architecture](architecture.md) first: the fetch cycle, `seeded_at` and the
lease are assumed knowledge here.

There is no shared provider base class. The scheduler only knows the three
things in `providers.ProviderSpec`: a credentials class, a `fetch` function
and a `transform` function. Everything else about the vendor stays in its
own files. The two existing providers are deliberately different (OAuth with
one record per cycle, a static token with one record per inverter), so copy
whichever one is closer.

## 1. Credentials model (`db/models.py`)

```python
class AcmeCredentials(BaseModel):
    """API key from the Acme portal."""

    model_config = ConfigDict(hide_input_in_errors=True)

    provider: Literal["acme"] = "acme"
    api_key: EncryptedSecret
```

- `provider` must be a `Literal` with the provider name: it is the
  discriminator of the credentials union.
- Type every secret as `EncryptedSecret`, so it is Fernet-encrypted in
  MongoDB and decrypted when loaded.
- Keep `hide_input_in_errors=True`. Validation errors end up in logs, and
  without it pydantic prints the offending values, secrets included.

Then register it on `ApiConfig`:

- add `"acme"` to `provider: Literal[...]`;
- add `AcmeCredentials` to the `credentials` union;
- if the provider needs to remember something between cycles (a site ID, a
  device list), add an `acme_cache: AcmeCache | None = None` field, like
  `site_cache` and `growatt_cache`.

## 2. Secret fields (`crypto.py`)

Add the secret field names to `SECRET_FIELDS`:

```python
SECRET_FIELDS = {
    ...
    "acme": ["api_key"],
}
```

`manage_keys.py migrate-encrypt` uses this list. A field that is missing
here stays in plaintext after a migration.

## 3. Client (`clients/acme.py`)

The client talks to the vendor API and returns the raw responses unchanged.

- **Use `TimedClient`** (`clients/base.py`), the `http_client` that `fetch`
  receives. It logs every request and has sensible timeouts.
- **A vendor SDK must get a timeout.** If you wrap a synchronous SDK in
  `asyncio.to_thread`, as Growatt does, make sure every request it makes
  has a timeout. Without one, a stalled connection blocks the account
  forever (see `GROWATT_TIMEOUT` in `clients/growatt.py`). Sync SDK calls
  use the thread pool sized by `THREADPOOL_WORKERS`.
- **Raise `AuthFailedError`** (`clients/base.py`) only when the vendor says
  the credentials are dead, such as an HTTP 401. `fetch_job` counts these,
  and after 3 in a row the account is frozen as `AUTH FAILED` until it is
  re-seeded. Raise anything else for other errors: they are counted as
  ordinary failures and retried at the next poll.
- **Keep secrets and response bodies out of exception messages and logs.**
  Exception messages are logged.
- **Credentials that change** (OAuth tokens): write them back to the account
  yourself, conditioned on `seeded_at`, and handle another process having
  refreshed first. `clients/enphase.py` (`refresh_access_token`,
  `fetch_cycle`) shows both.

## 4. Record schema and transform (`transform/`)

In `transform/schemas.py`, define the record your output API receives:

```python
class AcmeRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: str
    timestamp: datetime        # UTC
    power_watts: float | None  # None when Acme sent no value
```

- Every record carries `account_id` and a UTC `timestamp`.
- When the vendor sends no value (missing, `null`, `""`), send `null`, not
  `0`, so the consumer can tell "no data" from "producing nothing". The
  `_optional` helper in `transform/pipeline.py` does this.
- Don't copy personal data such as site or customer names into records.

In `transform/pipeline.py`, add a pure `parse_acme(...)` that turns raw
responses into rows: no I/O, and it raises on a response it cannot read.
Any new record format must be agreed with whoever runs the output API, and
it goes into [Overview](../index.md#what-gets-sent-to-the-output-api).

## 5. Registry (`providers.py`)

Write the two functions the scheduler calls and register them:

```python
async def _acme_fetch(cfg: ApiConfig, db: AsyncDatabase, client: TimedClient):
    from clients.acme import fetch_cycle
    return await fetch_cycle(cfg, client)   # [(label, raw_dict), ...]


def _acme_transform(raw_responses, context):
    from transform.pipeline import parse_acme
    rows = parse_acme(raw_responses, timestamp=context["timestamp"],
                      account_id=context["account_id"])
    return [r.model_dump(mode="json") for r in rows]


REGISTRY = {
    ...
    "acme": ProviderSpec(
        credentials_class=AcmeCredentials,
        fetch=_acme_fetch,
        transform=_acme_transform,
    ),
}
```

- `fetch` returns a list of `(label, raw_response)` pairs. They are stored
  in `raw_responses` (when `store_raw` is on) and passed to `transform`.
- `context` holds `timestamp` (the cycle start, UTC) and `account_id`.
- **Every write `fetch` makes to the account document must be conditioned on
  `seeded_at`**, e.g. `update_one({"_id": ..., "seeded_at": cfg.seeded_at},
  ...)`. Otherwise a cycle that started before a re-seed can overwrite the
  new credentials. `_growatt_fetch` shows this for its cache.

The scheduler, leases, flush job and delivery need no change.

## 6. Account management (`manage_keys.py`)

- Add an `acme-set` command, modelled on `growatt-set`. Ask for secrets
  with `_prompt_secret` (hidden input) when they are not given, and offer
  `--from-file`, so secrets stay out of shell history. Keep `--interval`
  with `min=MIN_INTERVAL_SECONDS` and `--enabled/--disabled`.
- If you added a cache, add it to `_CACHE_FIELD` and decide in
  `_cache_stale` when a re-seed must drop it: when the new credentials may
  belong to a different installation.
- Update the `--provider` help text of `delete`.

## 7. Dev tooling (`dev.py`)

Add a `test-acme` command next to `test-growatt`, and an `"acme"` entry in
`_LEASE_REASON` that explains why the test must not run while the service
polls the account (rate limits, token refresh).

## 8. Tests

The suite never touches a real API. Mock HTTP with `respx` (see
`tests/test_enphase.py`), or patch the SDK class (see
`tests/test_growatt.py`). Cover at least:

- the credentials round-trip encrypted (`tests/test_models.py`);
- the transform, including missing and `null` values (`tests/test_transform.py`);
- the client's auth failure and timeout paths;
- the registry entry (`tests/test_scheduler.py`, `test_registry_*`);
- `_plan_upsert` for a re-seed, if there is a cache (`tests/test_manage_keys.py`).

```bash
uv run pytest tests/
```

## 9. Documentation

- [Managing accounts](../operations/accounts.md): an "Adding an Acme
  account" section, with how the system owner gets the credentials.
- [Overview](../index.md#what-gets-sent-to-the-output-api): the record format.
- [Architecture](architecture.md#providers): a column in the providers table
  (auth, calls per cycle, cache, records per cycle).
- [Troubleshooting](../operations/troubleshooting.md): the vendor's error
  codes that operators will see, such as rate limits.
