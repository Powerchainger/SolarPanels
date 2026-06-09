# Plan: Async scraper service for EnPhase + Growatt → MongoDB → Output API

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
   Upgrade path: replace with native httpx in clients/api_b.py only.
```

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

### Growatt (vendor SDK, sync)

- **Library:** `growattServer.OpenApiV1(token=…)` — a synchronous library built on `requests`. We will not re-implement the HTTP shape; we will reuse the library via `asyncio.to_thread`.
- **Auth:** single static API token (from the ShinePhone app).
- **Calls used (`Growatt/measurements.py:25-52`):**
  - `api.plant_list()` → `{plants:[{plant_id, …}]}` — first plant used.
  - `api.device_list(plant_id)` → `{devices:[{device_sn, type, …}]}` — filter `type == 7` (MIN/TLX hybrid inverters).
  - `api.min_energy(device_sn=sn)` → `{pac, ppv, bdc1Soc, …}` (strings).
- **Processed row shape (one row per inverter per cycle):** `{timestamp (Europe/Amsterdam), device_sn, ac_power_watts (float), pv_power_watts (float), soc_percent (int)}`.
- **Cadence in existing code:** every 5 minutes; a 2s sleep between per-device calls inside one cycle (the user reports frequent timeouts otherwise — preserve this).

## Review of the proposed implementation plan

The proposed architecture is sound. Below are the deltas and improvements to bake in.

### Decisions confirmed
1. **Growatt client** — wrap `growattServer.OpenApiV1` calls in `asyncio.to_thread`. Keep the 2s inter-device sleep; preferably as `asyncio.sleep` between threaded calls rather than `time.sleep` inside one big thread, so the event loop stays responsive.
2. **OAuth state in MongoDB** — store EnPhase tokens (`access_token`, `refresh_token`, `expires_at`, `expires_in`, `token_type`) alongside the per-account config. `manage_keys.py` also handles the one-time bootstrap (exchanging `auth_code` → tokens), since `auth_code` expires very fast and that flow is awkward to retry by hand.
3. **Output API** — single record per POST, as planned. For Growatt this means one POST per filtered device per cycle.
4. **Python 3.13** — see "Python version" below.

### Multi-machine parallelism and scaling

The system is designed for **multiple worker machines**, each responsible for a subset of accounts. The schema keys on `(provider, account_id)`; machine assignment is managed via a **lease with heartbeat** rather than a static `machine_id` field.

**Lease / heartbeat mechanism.** Each `api_configs` document carries a `lease: {worker_id, expires_at}`. At boot a worker atomically claims all unclaimed or expired leases (`findOneAndUpdate` filtering on `expires_at < now || lease == null`). Every `lease_renewal_interval` seconds (default: half the shortest account interval, minimum 30 s) the worker renews its held leases by bumping `expires_at`. If a worker dies, its leases expire (default TTL: `2 × interval_seconds` per account) and any other worker picks them up automatically — no manual DB intervention required for failover.

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
- **Thousands per machine:** swap `to_thread` for native httpx in `clients/api_b.py` (see Growatt async upgrade path below). No other changes required.

**Per-key parallelism is free.** Each account has its own API token / OAuth credentials, so rate-limit buckets are fully independent. The constraint is local (threadpool), not remote.

**Growatt async upgrade path.** When horizontal scaling is no longer sufficient, replace the three `growattServer` calls with native httpx equivalents *inside `clients/api_b.py` only*. The async facade interface (`list_plants`, `list_devices`, `device_energy`) stays identical; nothing outside that file changes. Estimated effort: ~4 hours. A full upstream `AsyncOpenApiV1` contribution to [indykoning/PyPi_GrowattServer](https://github.com/indykoning/PyPi_GrowattServer) is a cleaner long-term option (the V1 API is ~11 endpoints; the mechanical httpx conversion is 2–4 days) but is decoupled from the product timeline.

**Design implications baked into the plan:**

1. `clients/api_b.py` exposes only three async methods (`list_plants`, `list_devices`, `device_energy`). The `to_thread` implementation is an internal detail — the httpx upgrade does not ripple outward.
2. `main.py` at boot: (a) claims available leases for this `WORKER_ID`; (b) starts a heartbeat renewal coroutine; (c) sets `loop.set_default_executor(ThreadPoolExecutor(max_workers=THREADPOOL_WORKERS))`; (d) builds the scheduler from claimed accounts only.
3. APScheduler `max_instances=1` per `(provider, account_id)` — same account never overlaps itself; different accounts run in parallel.
4. Stagger initial fire times across accounts (`hash(account_id) % interval_seconds` offset) to avoid thundering-herd on restart.

### Improvements / corrections

1. **Drop Polars.** Both transforms are row-shaped (one EnPhase summary row per cycle, N device rows per Growatt cycle). Pydantic models alone cover validation and shape. Polars adds a heavyweight dependency without aggregation work to justify it.

2. **Use `tenacity` for retry/backoff**, applied at the `clients/base.py` HTTP layer and the output-poster. Distinguish:
   - Transient (httpx `ConnectError`/`TimeoutException`, HTTP 5xx, 429) → retry up to 3× exponential.
   - 4xx other than 429 → do not retry; mark record `status=failed`, `last_error`, `retry_count`, leave for human review (don't burn retries on a permanent 401).

3. **Record status as an enum, not just `posted: bool`.** `processed_results` document: `status ∈ {pending, posting, posted, failed}`, `retry_count`, `last_error`, `last_attempt_at`. The flush job picks `status ∈ {pending, failed}` with `retry_count < N`, sets `posting` (atomic find-and-update) before sending — prevents double-post if a scheduler tick races a flush.

4. **TTL on `raw_responses`.** Useful for debugging but grows unboundedly. Add a Mongo TTL index on `created_at` (default 30 days, configurable via env). Storing raw responses can also be made opt-in via an `api_configs.store_raw: bool` flag to control bytes.

5. **Store timestamps as UTC in Mongo.** Existing code converts to `Europe/Amsterdam` at parse time and writes that into CSV. For Mongo, convert to UTC at the storage boundary and let the output API / display layer localise. Keep `Europe/Amsterdam` as a derived field if downstream needs it, but don't lose UTC.

6. **EnPhase token refresh stays inline, not a separate job.** Check `should_refresh(expires_at)` immediately before each EnPhase fetch (as the current code does). Persist refreshed tokens back to Mongo. No second scheduled job needed — keeps state transitions in one place.

7. **Validate `api_configs` documents with Pydantic on boot.** A bad doc (missing `interval_seconds`, missing `api_key`, etc.) should fail the container fast with a clear error rather than silently no-op.

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
     "device_filter": {"type": 7},  # growatt only
     "site_cache": {"site_id": ..., "user": ...},  # enphase only
     "lease": {"worker_id": "<WORKER_ID>", "expires_at": ISODate},
     "store_raw": true,
     "updated_at": ISODate
   }
   ```
   EnPhase's per-account `site_id` is cached in `site_cache` (replacing the in-process `SESSION_CACHE`), and refreshed on a cache miss or when systems API returns a new one.

9. **`clients/base.py` responsibilities** — shared async httpx client (one per process), `tenacity` retry decorator, structured logging hook, request/response size + duration logging. Both EnPhase and the output poster use it; Growatt does not (vendor SDK).

10. **Single schema = storage + wire.** Define one Pydantic model per source for the *processed* row (e.g. `EnphaseRow`, `GrowattRow`) and reuse the same model for both the Mongo document and the POST body. One source of truth, fewer drift bugs.

11. **No Polars, no pandas.** The existing CSV writing was pandas-driven; that goes away. Pandas/Polars are unnecessary for this workload.

12. **APScheduler config**: `AsyncIOScheduler`, one job per enabled `(provider, account_id)` loaded at boot from `api_configs`, `max_instances=1`, `coalesce=True` (matches existing semantics), `misfire_grace_time` set so a paused container resuming doesn't fire a backlog. Config changes require a restart — documented, not silently reloaded.

13. **Dockerfile**: `python:3.13-slim` — see "Python version" below. Multi-stage (build wheels in one stage, copy into runtime), non-root user, no compilers in final image. No CSV volume needed — Mongo is the store now.

14. **docker-compose.yml**: two services (`app`, `mongo`); named volume for `mongo_data`; the app depends on mongo with a healthcheck; `.env` provides `MONGO_URI`, `OUTPUT_API_URL`, `OUTPUT_API_TOKEN` (if any), `LOG_LEVEL`, `RAW_TTL_DAYS`, `THREADPOOL_WORKERS`, `WORKER_ID`, `LEASE_TTL_MULTIPLIER`, `LEASE_RENEWAL_INTERVAL`. For multi-machine deployments the `app` service is run as separate containers on separate hosts (or as replicas in a compose stack / k8s deployment), each with a distinct `WORKER_ID`; the `mongo` service remains a single shared instance (or replica set) pointed to by all workers via `MONGO_URI`.

15. **Bootstrap UX for EnPhase**: `manage_keys.py` subcommands:
    - `manage_keys.py enphase bootstrap --account-id <id> --client-id … --client-secret … --redirect-uri … --auth-code … --api-key …` — performs the one-time `authorization_code` exchange (replacing `EnPhase/access_token_gen.py`) and writes the full config doc into Mongo.
    - `manage_keys.py growatt set --account-id <id> --token …`
    - `manage_keys.py list` / `manage_keys.py delete --provider <p> --account-id <id>`
    The README's warning that `auth_code` expires quickly applies here too — the bootstrap must be a single self-contained command, not a multi-step manual procedure.

16. **Don't copy `EnPhase/auth.py` and `access_token_gen.py` verbatim.** They have merge conflicts and use `http.client` synchronously. Rewrite as small async functions in `clients/api_a.py` using httpx. The logic is short and well-understood from the existing code; reimplementation is lower risk than untangling the conflict.

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

Deliberately **not** a unified `Provider` ABC that bundles fetch + transform: that would
collapse the client/transform seam that IMPL.md Step 4 relies on (transforms are pure
functions testable against fixture JSON with no network). The registry sits *above* that
seam — it does not erase it.

The registry is introduced at **Step 9**, when `scheduler/jobs.py` first needs to dispatch
by provider. Writing that dispatch as a dict from the start costs nothing and avoids a
later refactor; it is not worth adding earlier, while only two providers exist in
still-unwritten code.

### Python version

The existing repo's README pins `3.11.9`. `growattServer` 2.1.0 declares no `Requires-Python` and only the generic `Programming Language :: Python :: 3` classifier — so it does not constrain us. Pick the most recent Python that the whole stack (growattServer + motor + httpx + apscheduler + pydantic v2 + tenacity) has stable wheels for, to delay re-bumping when 3.11 hits end-of-life:

- **Recommended: Python 3.13** (stable since Oct 2024; full wheel coverage for every dep we use; supported by `python:3.13-slim`).
- Python 3.14 is newer but ecosystem coverage is patchier; revisit once Motor and APScheduler publish 3.14 wheels and minor compat issues settle.
- Confirm in the very first build that `growattServer` actually imports under 3.13. If it does not (it makes no version claim either way), pin 3.12 and add a TODO to track when upstream confirms 3.13 support. This is the only place a Python downgrade would be necessary.

Pin the version in both `Dockerfile` (base image) and a `.python-version` / `pyproject.toml` `requires-python = ">=3.13,<3.14"` so local dev and the container agree.

### Open questions noted but not blocking

- Output API: contract not in scope here. Assumes a JSON POST accepting one record at a time. If the receiver later requires bulk, change `clients/output.py` only.
- Multi-site / multi-plant within one account: existing code only ever uses `systems[0]` / `plants[0]`. The new service keeps that single-site-per-account assumption for parity. If multi-site is ever needed, `api_configs` documents can be split per (account, site).

## Deployment upgrade path

The application code is multi-machine-ready from the start (lease mechanism, `WORKER_ID`,
central MongoDB). The upgrade between stages is purely operational — no code changes
required at any stage.

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

## Implementation

See [IMPL.md](IMPL.md) for the step-by-step implementation plan, file inventory,
functions to port, and verification checklist.
