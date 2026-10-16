# Plan: Async scraper service for EnPhase + Growatt → MongoDB → Output API

> **How to read this file.** This is the original design record (July 2026),
> kept for the reasoning behind the decisions. Where the implementation later
> went another way, the original text stays and is marked **Superseded**
> with the date and what replaced it. For how the service works *now*, read
> `docs/` (start with `docs/technical/architecture.md`); for open issues,
> `HANDOVER.md`.

## Context

The existing repo contains two synchronous, per-folder scrapers that log to CSV (`EnPhase/inverter_log.csv`, `Growatt/growatt_log.csv`) using two separate `BackgroundScheduler` processes. The goal is to consolidate them into a single Python asyncio service that:

1. Schedules both scrapers under one event loop (per-API, per-account interval, stored in Mongo).
2. Persists raw and processed records to MongoDB.
3. Forwards processed records to an external output API, with retry and a flush job for failures.
4. Runs as a containerised app alongside a MongoDB container via Docker Compose.

Two prerequisite findings before any of this is built:

- **`EnPhase/auth.py` and `EnPhase/access_token_gen.py` contain unresolved git merge conflict markers** (`<<<<<<< HEAD` / `=======` / `>>>>>>> EnPhase_automation`). They are not valid Python in their current state. The recent merge in `23bf515` was committed with conflicts still in the files. These must be resolved before — or as part of — porting the logic.
- **`requirements.txt` is encoded as UTF-16 with a BOM** (each ASCII byte is interleaved with `\x00`, hence the spaced-out look). It needs to be saved as UTF-8 before any `pip install -r` can succeed.

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                           Central MongoDB                               │
 │                                                                         │
 │  api_configs                raw_responses      processed_results        │
 │  ┌────────────────────┐    ┌──────────────┐   ┌─────────────────────┐   │
 │  │ _id: {provider,    │    │ provider     │   │ provider            │   │
 │  │       account_id}  │    │ account_id   │   │ account_id          │   │
 │  │ credentials        │    │ body         │   │ record (JSON)       │   │
 │  │ interval_secs      │    │ created_at   │   │ status: pending |   │   │
 │  │ lease:             │    │ [TTL index]  │   │  posting | posted | │   │
 │  │   worker_id        │    └──────────────┘   │  failed             │   │
 │  │   expires_at       │                       │ retry_count         │   │
 │  │ site_cache         │                       │ last_error          │   │
 │  └────────────────────┘                       └─────────────────────┘   │
 └──────────────────────────────┬──────────────────────────────────────────┘
                                │  claim lease · renew heartbeat
                                │  read credentials · read/write results
                   ┌────────────┼────────────┐
                   │            │            │
            ┌──────┴──────┐   · · ·   ┌──────┴──────┐
            │  Worker A   │           │  Worker B   │
            │ WORKER_ID=a │           │ WORKER_ID=b │
            │             │           │             │
            │ AsyncIO-    │           │ AsyncIO-    │
            │ Scheduler   │           │ Scheduler   │
            │ ┌─────────┐ │           │ ┌─────────┐ │
            │ │EnPhase  │ │           │ │EnPhase  │ │
            │ │(httpx)  │ │           │ │(httpx)  │ │
            │ ├─────────┤ │           │ ├─────────┤ │
            │ │Growatt  │ │           │ │Growatt  │ │
            │ │(thread*)│ │           │ │(thread*)│ │
            │ ├─────────┤ │           │ ├─────────┤ │
            │ │flush job│ │           │ │flush job│ │
            │ │(jitter) │ │           │ │(jitter) │ │
            │ └─────────┘ │           │ └─────────┘ │
            └──┬──────┬───┘           └──┬──────┬───┘
               │      │                  │      │
               ▼      ▼                  ▼      ▼
          ┌───────┐ ┌───────┐       ┌───────┐ ┌───────┐
          │EnPhase│ │Growatt│       │EnPhase│ │Growatt│
          │  API  │ │  API  │       │  API  │ │  API  │
          └───┬───┘ └───┬───┘       └───┬───┘ └───┬───┘
              │         │               │         │
              └─────────┴─────┬─────────┴─────────┘
                              │
                              │  POST one record · tenacity retry
                              ▼
                     ┌────────────────┐
                     │   Output API   │
                     └────────────────┘

 * thread = asyncio.to_thread wrapping growattServer.OpenApiV1
   Upgrade path: replace with native httpx in clients/growatt.py only.
```

> **Superseded** (2026-08-03 / 2026-10-14): `_id` is the string
> `"<provider>::<account_id>"`, not a sub-document. One worker is the decided
> target for the ~100-account horizon; a second worker works as a standby
> (sync job, 2026-10-14) but accounts are not balanced between workers.

## Summary of the two source APIs

### EnPhase (REST, OAuth2)

- **Host:** `api.enphaseenergy.com` (HTTPS).
- **Auth:** OAuth2 Bearer access_token + static `key` header carrying an API key. Access token is refreshed via `POST /oauth/token?grant_type=refresh_token&refresh_token=…` using HTTP Basic of `client_id:client_secret`. Refresh is triggered when within 2h of expiry (`EnPhase/auth.py:65-74` once conflicts are resolved).
- **Endpoints used:**
  - `GET /api/v4/systems?key={api_key}` → returns `systems[]`; we use `systems[0].system_id` and `systems[0].name` (`EnPhase/site_id.py:8-20`).
  - `GET /api/v4/systems/inverters_summary_by_envoy_or_site?site_id={site_id}` → returns a JSON array; element 0 has `micro_inverters[]`, each with `last_report_date` (ISO) and `power_produced.value` (W) (`EnPhase/cloud_measurements.py:26-50`).
- **Processed row shape (from existing code):** `{timestamp (Europe/Amsterdam), inverter_1..N, total_power, user_value (site name)}`.
- **Cadence in existing code:** hourly (`EnPhase/run_scheduler.py:61`).
- **State that must persist across restarts:** `access_token`, `refresh_token`, `expires_at`, `expires_in`, `token_type`, `api_key`, `client_id`, `client_secret`, plus a memoised `site_id` + site `name` (the existing in-process `SESSION_CACHE` is lost on restart).

  > **Superseded** (2026-10-14): the site `name` is no longer stored or sent
  > (it is a customer name under GDPR); only `site_id` is cached.

### Growatt (vendor SDK, sync)

- **Library:** `growattServer.OpenApiV1(token=…)` — a synchronous library built on `requests`. We will not re-implement the HTTP shape; we will reuse the library via `asyncio.to_thread`.
- **Auth:** single static API token (from the ShinePhone app).
- **Calls used (`Growatt/measurements.py:25-52`):**
  - `api.plant_list()` → `{plants:[{plant_id, …}]}` — first plant used.
  - `api.device_list(plant_id)` → `{devices:[{device_sn, type, …}]}` — filter `type == 7` (MIN/TLX hybrid inverters).
  - `api.min_energy(device_sn=sn)` → `{pac, ppv, bdc1Soc, …}` (strings).
- **Processed row shape (one row per inverter per cycle):** `{timestamp (Europe/Amsterdam), device_sn, ac_power_watts (float), pv_power_watts (float), soc_percent (int)}`.
- **Cadence in existing code:** every 5 minutes; a 2s sleep between per-device calls inside one cycle (the user reports frequent timeouts otherwise — preserve this).

  > **Superseded** (2026-07-28): the default Growatt interval is 10 minutes.
  > Growatt allows about one call per endpoint per 5 minutes per token and
  > answers faster polling with error `10012`; blocks have lasted far longer
  > than 5 minutes (see `docs/operations/troubleshooting.md`). The 2 s
  > per-device sleep is kept.

## Review of the proposed implementation plan

The proposed architecture is sound. Below are the deltas and improvements to bake in.

### Resolutions against the product-owner requirements

Where this plan deviates from the product owner's bullet list, the deviation is deliberate:

- **`posted` boolean → status enum.** With both a scheduler tick and a periodic flush job,
  a boolean cannot express "in flight" and double-posts when they race. The
  `{pending, posting, posted, failed}` enum with an atomic claim-to-`posting` transition
  (improvement 3 below) is required, not gold-plating.
- **"Last response only" for `processed_results` — rejected.** It is incompatible with the
  retry/flush requirement from the same list: the backlog of unposted records *is* the
  recovery mechanism when the output API is down. Keeping only the last record loses data
  on any outage longer than one cycle. If storage becomes a concern, add a TTL on
  `status=posted` records only — the unposted backlog must stay.
- **"Last response only" for `raw_responses` — TTL is better and equally cheap.** Raw
  responses exist for debugging, and the bug usually happened a few cycles ago. Growatt at
  5-minute cadence is ~288 docs/day/account at a few KB each — under 1 MB/day/account.
  A 30-day TTL index plus the per-account `store_raw` opt-out dominates last-only.
- **"One job per API" → one job per (provider, account_id).** Intervals, credentials, and
  leases are all per-account; `max_instances=1` per account gives overlap protection
  without serialising unrelated accounts behind each other.
- **"CLI script … Also API call" — needs clarification from the product owner.** If it
  means the CLI performs the EnPhase auth-code exchange API call, the plan covers it
  (`enphase bootstrap`, improvement 15). If it means an *HTTP admin API* for key
  management: pushed back — that is a network-exposed credentials-write path needing its
  own auth, for something an operator does a few times a year. Defer until there is a
  concrete consumer.
- **Polars "(or nothing)" → nothing.** Both transforms are a handful of row-shaped dicts
  per cycle (improvements 1 and 11).
- **Pydantic "(nice to have)" → core dependency.** See "Why Pydantic is not optional"
  below.

### Why Pydantic is not optional

The "nice to have" framing imagines validating measurement rows; the real value is
elsewhere:

- **It guards the riskiest data, not the bulk data.** The discriminated union on
  `credentials.provider` plus the cross-field validator on `ApiConfig` means a malformed
  config doc fails the container at boot with a precise error, instead of silently
  no-op'ing or crashing mid-cycle (improvement 7).
- **Growatt returns numbers as strings** (`"pac": "1234.5"`). Type coercion at the
  boundary is exactly Pydantic's job; without it the same conversions are hand-written
  with worse error messages.
- **One model serves as both Mongo document and POST wire format** (improvement 10) —
  that de-duplication only works if there is a schema object at all.
- The cost is one well-maintained dependency, and `pydantic-settings` (used for env
  config) would pull it in transitively anyway. It is also already built: `db/models.py`
  and its tests exist (IMPL.md Step 2 complete).

Polars is the right thing to cut, not Pydantic.

### Decisions confirmed
1. **Growatt client** — wrap `growattServer.OpenApiV1` calls in `asyncio.to_thread`. Keep the 2s inter-device sleep; preferably as `asyncio.sleep` between threaded calls rather than `time.sleep` inside one big thread, so the event loop stays responsive.
2. **OAuth state in MongoDB** — store EnPhase tokens (`access_token`, `refresh_token`, `expires_at`, `expires_in`, `token_type`) alongside the per-account config. `manage_keys.py` also handles the one-time bootstrap (exchanging `auth_code` → tokens), since `auth_code` expires very fast and that flow is awkward to retry by hand.
3. **Output API** — single record per POST, as planned. For Growatt this means one POST per filtered device per cycle.
4. **Python 3.13** — see "Python version" below.
5. **Mongo driver — PyMongo async (`AsyncMongoClient`), not Motor.** Motor is deprecated (≈EOL 2026); `pymongo>=4.9` ships a native async client and is already a dependency. `motor` is dropped from `pyproject.toml` when `db/mongo.py` is built (Step 3).
6. **Orchestration via podman compose.** The `mongo` compose service is stood up from Step 3 onward; the app image and its compose service are built at Step 13. (Step 13 originally bundled both — now split.)

### Multi-machine parallelism and scaling

The system is designed for **multiple worker machines**, each responsible for a subset of accounts. The schema keys on `(provider, account_id)`; machine assignment is managed via a **lease with heartbeat** rather than a static `machine_id` field.

> **Scope note.** The lease/heartbeat machinery goes beyond the product owner's stated requirements — it is the one place this plan spends complexity that was not asked for. It stays in the plan because it is cheap where it lives (isolated in IMPL.md Step 11 and `main.py`; the schema field is already built), but it is also the first thing to defer under timeline pressure: a single worker can run all enabled accounts and ignore the `lease` field, and the "no code changes between deployment stages" property means adding it later is contained.

**Lease / heartbeat mechanism.** Each `api_configs` document carries a `lease: {worker_id, expires_at}`. At boot a worker atomically claims all unclaimed or expired leases (`findOneAndUpdate` filtering on `expires_at < now || lease == null`). Every `lease_renewal_interval` seconds (default: half the shortest account interval, minimum 30 s) the worker renews its held leases by bumping `expires_at`. If a worker dies, its leases expire (default TTL: `2 × interval_seconds` per account) and any other worker picks them up automatically — no manual DB intervention required for failover.

> **Superseded in part** (2026-10-16): the lease's `worker_id` is unique per
> process (`WORKER_ID` or the hostname, plus a random suffix), and a lease
> whose holder stopped renewing it is taken over after three renewal
> intervals instead of waiting for it to expire. See
> `docs/technical/architecture.md#leases`.

**Failure buffering: central MongoDB, not per-machine.** `processed_results` stays in the central DB (as already planned). Reasons:
- Any worker — or a future dedicated flush worker — can retry any account's failures, not just the one that created them.
- Single source of truth for monitoring total backlog across the fleet.
- If the central DB is unreachable a worker cannot claim leases or read credentials either; heroic local spooling buys nothing. Alert and wait.

**Coordinated retry / thundering herd.** When the output API recovers after an outage, all workers' flush jobs fire in near-unison. Mitigations:
- Jitter each worker's flush job start by `hash(WORKER_ID) % flush_interval_seconds` (already implicit if lease start times are staggered at boot).
- Future option: a "global flush leader" lease — one machine holds it, others skip their own flush job — eliminates the problem entirely.

**Does AsyncIOScheduler work with the sync `growattServer` library?** Yes. The cleaner pattern is an `async def` wrapper that calls `await asyncio.to_thread(sync_call)` and `await asyncio.sleep(2)` between per-device calls (replacing the in-thread `time.sleep(2)`).

**Scaling tiers (per machine, Growatt):**
- **Dozens of accounts:** `to_thread` + `ThreadPoolExecutor(max_workers=64)`. Comfortable.
- **Hundreds per machine:** still doable; memory grows (~8 MB stack per thread). Add machines instead.
- **Thousands total:** scale out horizontally — more workers, each holding its leased subset.
- **Thousands per machine:** swap `to_thread` for native httpx in `clients/growatt.py` (see Growatt async upgrade path below). No other changes required.

**Per-key parallelism is free.** Each account has its own API token / OAuth credentials, so rate-limit buckets are fully independent. The constraint is local (threadpool), not remote.

> **Superseded in part** (2026-10-15): independent across accounts, yes;
> but within one Growatt account the per-token rate limit is the binding
> constraint, and it is remote. See the Growatt cadence note above.

**Growatt async upgrade path.** When horizontal scaling is no longer sufficient, replace the three `growattServer` calls with native httpx equivalents *inside `clients/growatt.py` only*. The async facade interface (`list_plants`, `list_devices`, `device_energy`) stays identical; nothing outside that file changes. Estimated effort: ~4 hours. A full upstream `AsyncOpenApiV1` contribution to [indykoning/PyPi_GrowattServer](https://github.com/indykoning/PyPi_GrowattServer) is a cleaner long-term option (the V1 API is ~11 endpoints; the mechanical httpx conversion is 2–4 days) but is decoupled from the product timeline.

**Design implications baked into the plan:**

1. `clients/growatt.py` exposes only three async methods (`list_plants`, `list_devices`, `device_energy`). The `to_thread` implementation is an internal detail — the httpx upgrade does not ripple outward.
2. `main.py` at boot: (a) claims available leases for this `WORKER_ID`; (b) starts a heartbeat renewal coroutine; (c) sets `loop.set_default_executor(ThreadPoolExecutor(max_workers=THREADPOOL_WORKERS))`; (d) builds the scheduler from claimed accounts only.
3. APScheduler `max_instances=1` per `(provider, account_id)` — same account never overlaps itself; different accounts run in parallel.
4. Stagger initial fire times across accounts (`hash(account_id) % interval_seconds` offset) to avoid thundering-herd on restart.

> **Superseded** (2026-08-03 / 2026-10-07): single worker is the decision for
> the ~100-account horizon; the lease machinery is built and used by that one
> worker. Lease renewal is a fixed `LEASE_RENEWAL_INTERVAL` (30 s), not half
> the shortest interval. A worker's first fetch of an account resumes the
> previous rhythm (`last_fetch_at + interval`) and falls back to the hash
> stagger only when the last fetch is older than one interval, because a
> restart shortly after a fetch tripped Growatt's rate limit. The flush
> jitter is random (5–30 s), not derived from `WORKER_ID`.

### Improvements / corrections

1. **Drop Polars.** Both transforms are row-shaped (one EnPhase summary row per cycle, N device rows per Growatt cycle). Pydantic models alone cover validation and shape. Polars adds a heavyweight dependency without aggregation work to justify it.

2. **Use `tenacity` for retry/backoff**, applied at the `clients/base.py` HTTP layer and the output-poster. Distinguish:
   - Transient (httpx `ConnectError`/`TimeoutException`, HTTP 5xx, 429) → retry up to 3× exponential.
   - 4xx other than 429 → do not retry; mark record `status=failed`, `last_error`, `retry_count`, leave for human review (don't burn retries on a permanent 401).

   > **Superseded in part:** tenacity is used only for the output POST
   > (`clients/output.py`). Provider calls are not retried within a cycle;
   > a failed poll is lost until the next one (`HANDOVER.md`, question 5).

3. **Record status as an enum, not just `posted: bool`.** `processed_results` document: `status ∈ {pending, posting, posted, failed}`, `retry_count`, `last_error`, `last_attempt_at`. The flush job picks `status ∈ {pending, failed}` with `retry_count < N`, sets `posting` (atomic find-and-update) before sending — prevents double-post if a scheduler tick races a flush.

   Two failure modes the flush job must also handle:

   - **Stuck `posting` records.** If a worker crashes between claiming a record (`status=posting`) and completing the POST, that record is orphaned — the `{pending, failed}` filter never picks it up again. The flush job must additionally reclaim `posting` records whose `last_attempt_at` is older than a staleness threshold (a few minutes).
   - **Two retry levels with separate counters.** Tenacity's up-to-3× backoff is *internal to one delivery attempt* and must not increment the `retry_count` that gates the flush job — otherwise a single bad cycle (3 tenacity attempts) immediately disqualifies the record from ever being flushed. `retry_count` counts delivery attempts (flush-level), with its own cap. Records that exhaust the cap stay `failed` for human review — log/count them so the backlog monitoring (see multi-machine section) surfaces them rather than letting them sit silently.

   > **Note** (2026-10-15): the atomic claim was only implemented on
   > 2026-10-14 (`3c624e2`); until then the flush did a plain `find()`. Each
   > claim is now stamped with its own time and the outcome is written only
   > while that stamp holds (`3e7f842`).

4. **TTL on `raw_responses`.** Useful for debugging but grows unboundedly. Add a Mongo TTL index on `created_at` (default 30 days, configurable via env). Storing raw responses can also be made opt-in via an `api_configs.store_raw: bool` flag to control bytes.

5. **Store timestamps as UTC in Mongo.** Existing code converts to `Europe/Amsterdam` at parse time and writes that into CSV. For Mongo, convert to UTC at the storage boundary and let the output API / display layer localise. Keep `Europe/Amsterdam` as a derived field if downstream needs it, but don't lose UTC.

6. **EnPhase token refresh stays inline, not a separate job.** Check `should_refresh(expires_at)` immediately before each EnPhase fetch (as the current code does). Persist refreshed tokens back to Mongo. No second scheduled job needed — keeps state transitions in one place.

7. **Validate `api_configs` documents with Pydantic on boot.** A bad doc (missing `interval_seconds`, missing `api_key`, etc.) should fail the container fast with a clear error rather than silently no-op.

   > **Superseded:** documents are validated, but a bad one degrades only
   > that account: `claim_leases` logs it and skips it, so one broken account
   > cannot stop polling for all others. Settings (env) errors still stop the
   > container at boot.

8. **`api_configs` schema sketch** (one doc per account, keyed on `(provider, account_id)`):
   ```
   {
     "_id": {"provider": "enphase" | "growatt", "account_id": "<user-supplied label>"},
     "enabled": true,
     "interval_seconds": 3600 | 300,
     "credentials": {
        # enphase: client_id, client_secret, redirect_uri, api_key,
        #          access_token, refresh_token, expires_at, expires_in, token_type
        # growatt: api_token
     },
     "site_cache": {"site_id": ..., "user": ...},  # enphase only
     "lease": {"worker_id": "<WORKER_ID>", "expires_at": ISODate},
     "store_raw": true,
     "updated_at": ISODate
   }
   ```
   EnPhase's per-account `site_id` is cached in `site_cache` (replacing the in-process `SESSION_CACHE`), and refreshed on a cache miss or when systems API returns a new one.

   > **Superseded:** the actual document is `db/models.py:ApiConfig`. `_id`
   > is `"<provider>::<account_id>"`; `site_cache` holds only `site_id`;
   > Growatt has a `growatt_cache` (plant and devices, 24 h); there are
   > health fields (`last_success_at`, `last_fetch_at`, failure counters,
   > `auth_failed`) and `seeded_at`, which guards provider writes against a
   > concurrent re-seed. Default intervals are 3600 s (EnPhase) and 600 s
   > (Growatt), minimum 60 s (2026-10-15). Secret credential fields are
   > Fernet-encrypted (2026-08-13).

9. **`clients/base.py` responsibilities** — shared async httpx client (one per process), `tenacity` retry decorator, structured logging hook, request/response size + duration logging. Both EnPhase and the output poster use it; Growatt does not (vendor SDK).

   > **Superseded in part:** `clients/base.py` provides `TimedClient`
   > (logging, default timeouts) and `AuthFailedError`; the retry lives in
   > `clients/output.py` (see item 2). The Growatt SDK got its own timeout
   > (2026-10-15).

10. **Single schema = storage + wire.** Define one Pydantic model per source for the *processed* row (e.g. `EnphaseRow`, `GrowattRow`) and reuse the same model for both the Mongo document and the POST body. One source of truth, fewer drift bugs.

11. **No Polars, no pandas.** The existing CSV writing was pandas-driven; that goes away. Pandas/Polars are unnecessary for this workload. **Action item:** `pyproject.toml` currently still lists `pandas>=3.0.3` — nothing in the new code uses it; remove it (it is the heaviest item in the dependency tree). `motor` is likewise still listed; its removal is already scheduled for IMPL.md Step 3 when `db/mongo.py` is built.

    > **Done:** neither `pandas` nor `motor` is a dependency any more.

12. **APScheduler config**: `AsyncIOScheduler`, one job per enabled `(provider, account_id)` loaded at boot from `api_configs`, `max_instances=1`, `coalesce=True` (matches existing semantics), `misfire_grace_time` set so a paused container resuming doesn't fire a backlog. Config changes require a restart — documented, not silently reloaded.

    > **Superseded** (2026-10-14): a sync job re-claims accounts every
    > minute and adds, reschedules or removes fetch jobs, so account changes
    > (new, disabled, changed interval) apply without a restart.

13. **Dockerfile**: `python:3.13-slim` — see "Python version" below. Multi-stage (build wheels in one stage, copy into runtime), non-root user, no compilers in final image. No CSV volume needed — Mongo is the store now.

14. **docker-compose.yml**: two services (`app`, `mongo`); named volume for `mongo_data`; the app depends on mongo with a healthcheck; `.env` provides `MONGO_URI`, `OUTPUT_API_URL`, `OUTPUT_API_TOKEN` (if any), `LOG_LEVEL`, `RAW_TTL_DAYS`, `THREADPOOL_WORKERS`, `WORKER_ID`, `LEASE_TTL_MULTIPLIER`, `LEASE_RENEWAL_INTERVAL`. For multi-machine deployments the `app` service is run as separate containers on separate hosts (or as replicas in a compose stack / k8s deployment), each with a distinct `WORKER_ID`; the `mongo` service remains a single shared instance (or replica set) pointed to by all workers via `MONGO_URI`.

    > **Superseded in part** (2026-10-16): `WORKER_ID` is optional and need
    > not be distinct; replicas of one `app` service (`--scale`) are safe,
    > because each process adds a random suffix to it.

15. **Bootstrap UX for EnPhase**: `manage_keys.py` subcommands:
    - `manage_keys.py enphase bootstrap --account-id <id> --client-id … --client-secret … --redirect-uri … --auth-code … --api-key …` — performs the one-time `authorization_code` exchange (replacing `EnPhase/access_token_gen.py`) and writes the full config doc into Mongo.
    - `manage_keys.py growatt set --account-id <id> --token …`
    - `manage_keys.py list` / `manage_keys.py delete --provider <p> --account-id <id>`
    The README's warning that `auth_code` expires quickly applies here too — the bootstrap must be a single self-contained command, not a multi-step manual procedure.

    > **Superseded / not built:** there is no `enphase bootstrap`. The
    > commands are `enphase-set`, `growatt-set`, `list`, `delete` and
    > `migrate-encrypt`. The auth-code exchange still uses the legacy
    > `EnPhase/access_token_gen.py`, and `enphase-set --from-file` stores the
    > result; the procedure is in `docs/operations/accounts.md`. Folding the
    > exchange into one command remains a known limitation (`HANDOVER.md`).

16. **Don't copy `EnPhase/auth.py` and `access_token_gen.py` verbatim.** They have merge conflicts and use `http.client` synchronously. Rewrite as small async functions in `clients/enphase.py` using httpx. The logic is short and well-understood from the existing code; reimplementation is lower risk than untangling the conflict.

### Schema / model implementation decisions

Choices realised in `db/models.py` while building the Pydantic models:

- **Discriminated union on `credentials.provider`** — Pydantic uses the `provider` key
  inside the credentials dict to pick which model to instantiate, giving precise validation
  errors instead of a confusing "tried X, tried Y" message.
- **`model_validator` on `ApiConfig`** — enforces that `credentials.provider` matches the
  outer `provider` field, catching the case where an enphase config doc is accidentally
  paired with growatt credentials.
- **`StrEnum` for `RecordStatus`** — members are plain strings, so they round-trip through
  MongoDB without any serialisation step.
- **`default_factory=lambda: datetime.now(UTC)` on `created_at`/`updated_at`** —
  auto-populates when creating new documents in code, but uses the stored value when
  loading from MongoDB.

### Provider extensibility

Adding a third upstream provider should touch two layers, kept deliberately separate:

- **Model layer (`db/models.py`)** — add a new `XCredentials` model with
  `provider: Literal["x"]`, add `"x"` to `ApiConfig.provider`, and add the new model to the
  `credentials` discriminated union. This stays a *static* Pydantic union on purpose: a
  runtime registry cannot replace it without losing the precise per-provider validation
  errors that the discriminated union gives us. The edit is one line per location.

- **Orchestration layer (`providers.py`)** — a single dispatch registry maps each provider
  name to a `ProviderSpec(credentials_class, client_factory, transform_fn)`. The scheduler
  (`scheduler/jobs.py`), the transform pipeline, and `manage_keys.py` look up the spec
  instead of branching on the provider string. Adding a provider is then one registry entry
  plus its three implementation files (client, credentials model, transform function) — no
  `if/elif` chains anywhere.

  > **Superseded in part:** built as `ProviderSpec(credentials_class, fetch,
  > transform)`, and a new provider touches more than three files (secret
  > fields, `manage_keys.py`, `dev.py`, docs). The full checklist is
  > `docs/technical/adding-a-provider.md`.

Deliberately **not** a unified `Provider` ABC that bundles fetch + transform: that would
collapse the client/transform seam that IMPL.md Step 4 relies on (transforms are pure
functions testable against fixture JSON with no network). The registry sits *above* that
seam — it does not erase it.

The registry is introduced at **Step 9**, when `scheduler/jobs.py` first needs to dispatch
by provider. Writing that dispatch as a dict from the start costs nothing and avoids a
later refactor; it is not worth adding earlier, while only two providers exist in
still-unwritten code.

### Python version

The existing repo's README pins `3.11.9`. `growattServer` 2.1.0 declares no `Requires-Python` and only the generic `Programming Language :: Python :: 3` classifier — so it does not constrain us. Pick the most recent Python that the whole stack (growattServer + pymongo async + httpx + apscheduler + pydantic v2 + tenacity) has stable wheels for, to delay re-bumping when 3.11 hits end-of-life:

- **Recommended: Python 3.13** (stable since Oct 2024; full wheel coverage for every dep we use; supported by `python:3.13-slim`).
- Python 3.14 is newer but ecosystem coverage is patchier; revisit once PyMongo and APScheduler publish 3.14 wheels and minor compat issues settle.
- Confirm in the very first build that `growattServer` actually imports under 3.13. If it does not (it makes no version claim either way), pin 3.12 and add a TODO to track when upstream confirms 3.13 support. This is the only place a Python downgrade would be necessary.

Pin the version in both `Dockerfile` (base image) and a `.python-version` / `pyproject.toml` `requires-python = ">=3.13,<3.14"` so local dev and the container agree.

### Open questions noted but not blocking

- Output API: contract not in scope here. Assumes a JSON POST accepting one record at a time. If the receiver later requires bulk, change `clients/output.py` only.
  *(2026-10-15: the output API does not exist yet and is built by someone
  else; `dev.py serve` stands in. The record format in `docs/index.md` is
  the spec.)*
- Multi-site / multi-plant within one account: existing code only ever uses `systems[0]` / `plants[0]`. The new service keeps that single-site-per-account assumption for parity. If multi-site is ever needed, `api_configs` documents can be split per (account, site).

## Deployment upgrade path

The application code is multi-machine-ready from the start (lease mechanism, `WORKER_ID`,
central MongoDB). The upgrade between stages is purely operational — no code changes
required at any stage.

> **Superseded in part** (2026-08-03 / 2026-10-14): only Stage 0 is in use,
> and single worker is the decision for the current horizon. Stage 0's
> MongoDB requires authentication (2026-08-12), so `MONGO_URI` carries
> credentials. In Stage 2, accounts are **not** distributed automatically:
> they go first-come to whichever worker claims them, with no rebalancing
> (a second worker acts as a standby). `podman compose --scale` would give
> every replica the same `WORKER_ID`.

### Stage 0 — Single machine (initial target)

```
 ┌──────────────────────────────────┐
 │            Host machine          │
 │                                  │
 │  ┌─────────────┐  ┌───────────┐  │
 │  │     app     │  │   mongo   │  │
 │  │  container  │  │ container │  │
 │  └─────────────┘  └───────────┘  │
 │                                  │
 │  docker-compose.yml (both svcs)  │
 └──────────────────────────────────┘
```

- Single `docker-compose.yml` with `app` + `mongo` on one host.
- `WORKER_ID=worker-1`; leases are claimed but only one worker exists.
- Suitable for development and initial production with a small account count.

---

### Stage 1 — Externalise MongoDB *(prerequisite for multi-machine)*

The only structural change needed before adding workers. No app code changes.

```
 ┌──────────────────────────┐     ┌──────────────────────────┐
 │       Host machine        │     │     Dedicated DB host    │
 │                          │     │   (or managed / Atlas)   │
 │  ┌─────────────┐         │     │                          │
 │  │     app     │─────────┼────►│  MongoDB  :27017         │
 │  │  container  │         │     │                          │
 │  └─────────────┘         │     └──────────────────────────┘
 │                          │
 │  docker-compose.yml      │
 │  (app service only)      │
 └──────────────────────────┘
```

Changes:
- Drop the `mongo` service from `docker-compose.yml`.
- Point `MONGO_URI` at the external host.
- Firewall: worker hosts → MongoDB host port 27017 only; MongoDB not publicly reachable.
- Data already lives in Mongo, so no migration is needed — just move the container.

---

### Stage 2 — Additional worker machines

No code changes. Each machine runs the `app` container with a distinct `WORKER_ID`.
The lease mechanism distributes accounts automatically and recovers from worker failure
without human intervention.

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │   Worker A   │   │   Worker B   │   │   Worker C   │
 │ WORKER_ID=a  │   │ WORKER_ID=b  │   │ WORKER_ID=c  │
 │              │   │              │   │              │
 │ ┌──────────┐ │   │ ┌──────────┐ │   │ ┌──────────┐ │
 │ │   app    │ │   │ │   app    │ │   │ │   app    │ │
 │ └──────────┘ │   │ └──────────┘ │   │ └──────────┘ │
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │    MongoDB      │
                  └─────────────────┘
```

Changes:
- Each host gets its own `docker-compose.yml` (app only) or uses a shared compose file
  with a per-host `.env` supplying `WORKER_ID`.
- Secrets (`MONGO_URI`, `OUTPUT_API_TOKEN`, etc.) must be distributed consistently to
  all hosts. Simple approach: configuration management (Ansible, Chef). Better: a
  secrets manager (see Stage 4).

---

### Stage 3 — MongoDB high availability *(optional)*

For production workloads where MongoDB availability is critical. No app code changes.

- **Replica set** (self-hosted): 3-node deployment (primary + 2 secondaries). Automatic
  failover; `MONGO_URI` becomes
  `mongodb://h1,h2,h3/?replicaSet=rs0&readPreference=primaryPreferred`.
- **Atlas** (managed): simplest ops story; `MONGO_URI` swaps to the Atlas SRV string.

---

### Stage 4 — Orchestration *(optional)*

For automated scaling, rolling deploys, and centralised secret management.

**Docker Swarm** — minimal operational lift:
- Deploy `app` as a global service (one container per Swarm node).
- Set `WORKER_ID` via the Swarm node hostname: `WORKER_ID={{.Node.Hostname}}`.
- Secrets via `docker secret`; no plaintext `.env` files on disk.

**Kubernetes** — full platform:
- `Deployment` with `replicas: N`; env vars from `ConfigMap` / `Secret`.
- Unique, stable `WORKER_ID` per pod via the downward API:
  `WORKER_ID=$(POD_NAME)` injected as `valueFrom.fieldRef.fieldPath: metadata.name`.
- MongoDB as a `StatefulSet` or external Atlas; `MONGO_URI` in a `Secret`.

**Centralised logging** — needed by Stage 2, more urgent at Stage 4:
- Workers write structured JSON to stdout; a log aggregator (Loki, CloudWatch Logs,
  Datadog) collects across hosts.
- The central MongoDB already provides a unified view of `processed_results` statuses
  across all workers — the primary operational dashboard for account health.

---

### Summary of what changes at each stage

| Stage | `docker-compose.yml` | `MONGO_URI` | `WORKER_ID` | Code |
|-------|----------------------|-------------|-------------|------|
| 0 | app + mongo, one host | `mongodb://mongo:27017` | `worker-1` | — |
| 1 | app only, one host | external host / Atlas | `worker-1` | none |
| 2 | app only, N hosts | same external | unique per host | none |
| 3 | app only, N hosts | replica set / Atlas SRV | unique per host | none |
| 4 | Swarm/k8s workload | Secret/ConfigMap | node/pod name | none |

## Development infrastructure

### dev_run.py — live test harness (IMPL.md Step 6)

> **Superseded:** the script is `dev.py` (`serve`, `test-growatt`,
> `test-enphase`) and it was kept rather than absorbed into `main.py`. Since
> 2026-10-15 `test-*` is a dry run: fetch and transform only, prints field
> names (no values), writes no records, and refuses while a worker holds
> the account. The description below is the original harness.

`dev_run.py` runs a single live fetch cycle against a real API without starting
the scheduler.  It spins up a stdlib `http.server` echo server in a background
thread (zero new dependencies), calls the production `fetch_job` from
`scheduler/jobs.py`, and prints a summary:

```
uv run python dev_run.py enphase --account-id home
uv run python dev_run.py growatt --account-id home  # once Growatt token is available
```

What it verifies end-to-end:
- `providers.py` registry dispatch (no `if/elif` branching on provider)
- Provider client fetches real API data
- Transform pipeline parses the response
- `raw_responses` and `processed_results` written to Mongo with correct shapes
- POST sent to echo server with correct JSON body
- `status=posted` on the processed record
- Token refresh and site_cache behaviour (EnPhase)

This is a throwaway script — its logic is absorbed into `main.py` at Step 12.
The echo server pattern (stdlib `BaseHTTPRequestHandler` in a thread, random
free port) is reused at Step 12 for the `docker compose up` smoke test.

### Credential management — `manage_keys.py`

Sensitive values (`client_secret`, `access_token`, `refresh_token`, `api_token`)
are **never** required as CLI flags.  The CLI prompts with hidden input when
these flags are omitted, preventing them from appearing in shell history.

Alternatively, `--from-file creds.json` loads all credentials from a JSON file
in a single operation — suitable for scripting and one-time setup.

Supported file formats:

**Growatt:**
```json
{"api_token": "..."}
```

**EnPhase:**
```json
{
  "client_id": "...",
  "client_secret": "...",
  "redirect_uri": "...",
  "api_key": "...",
  "access_token": "...",
  "refresh_token": "...",
  "expires_in": 86399
}
```

## Implementation

See [IMPL.md](IMPL.md) for the step-by-step implementation plan, file inventory,
functions to port, and verification checklist.

### Current status (2026-10-15)

*Replaces the July 2026 status, which is in git history.*

All IMPL.md steps are done and the service runs in containers (single
worker, two test accounts). Maintenance items: M1 (README) done, M2/M4
(`uv.lock` committed) done, M3 (console scripts) dropped on 2026-10-14
because they never installed in this virtual project.

Current behaviour is documented in `docs/`; known limitations, open
product questions and possible future work are in `HANDOVER.md`.
