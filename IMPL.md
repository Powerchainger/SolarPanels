# Implementation Plan

> Architecture, schema design, and technology decisions: see [PLAN.md](PLAN.md).

## Status (resume point)

Last updated 2026-07-17. Completed through **Step 11** (all non-live-infrastructure code
and tests), plus **Step 10** (EnPhase client) and **Step 6 partial** (EnPhase live
cycle verified end-to-end against real API).

**Test suite: 109/109 passing** (23 models + 11 transform + 11 Growatt client + 14 output
client + 9 retry + 16 scheduler + 10 lease + 15 EnPhase client).

**Files created since Step 4:**
- `clients/base.py` — `TimedClient` (httpx subclass), `create_client()` factory, request/response logger
- `clients/enphase.py` — async OAuth refresh, site_id resolution, inverter fetch
- `clients/growatt.py` — async wrapper over `growattServer.OpenApiV1` via `asyncio.to_thread`
- `clients/output.py` — POST with tenacity retry, error classification, status mutation
- `providers.py` — dispatch registry (`provider name → ProviderSpec`)
- `scheduler/jobs.py` — `AsyncIOScheduler` bootstrap, `fetch_job`, `flush_job` (stale reclamation + retry cap)
- `leases.py` — atomic lease claiming via `findOneAndUpdate`, heartbeat coroutine, graceful release
- `dev_run.py` — throwaway live-test script with in-process echo server (Step 6)

**EnPhase live test (Step 6) confirmed:**
- Token refresh fires when `expires_at` is within 2h window, writes new tokens to Mongo.
- `site_id` resolved from `/api/v4/systems` and cached in `api_configs.site_cache`.
- 9 IQ8M micro-inverters fetched, transformed to `EnphaseRow`, written to `raw_responses` and `processed_results`.
- POST to local echo server succeeded, `status=posted`, `retry_count=0`.
- Bug fix applied: `fetch_job` re-reads `api_config` from Mongo after fetch to pick up
  `site_cache` written during the run (first-cycle `user_value` was empty without this).
- Real API response shape matches synthesised fixtures — no transform tweaks needed.

**Blocked on external inputs:**
- **Growatt API token** — not yet available. `clients/growatt.py` has full mock test coverage (11 tests). Growatt live cycle (Step 6 for Growatt) deferred until token arrives.

**Recommended resume point:** Step 6 (Growatt live cycle) once API token is available, then Step 12 (wire `main.py` + smoke test).

**Privacy guard added:** `manage_keys.py` prompts for sensitive values (hidden input) and accepts `--from-file` for JSON-file credential loading — no sensitive data in shell history. Documented in PLAN.md.

## Decisions locked this session
- **Mongo driver: PyMongo async (`AsyncMongoClient`), not Motor.** Motor is deprecated
  (≈EOL 2026) and `pymongo>=4.9` is already a dependency. Drop `motor` from
  `pyproject.toml` when `db/mongo.py` is written.
- **Orchestration: podman compose.** The `mongo` service is brought up from **Step 3**
  onward (split out of Step 13); the app image is still built at Step 13.

## Incremental steps

Each step leaves the system in a runnable or testable state and has a clear done-signal.
Steps 1, 2 and 4 need no infrastructure; Step 3 onward use MongoDB via the podman compose
service. Step 6 is the first full pipeline cycle against live infrastructure.

---

### Step 1 — Scaffold + config

Build `config.py` (Pydantic Settings for all env vars), UTF-8 `requirements.txt`,
`.env.example`, `.python-version` (3.13), and empty `__init__.py` stubs for each
module directory.

Done when: `python -c "from config import Settings; Settings()"` fails fast with a
readable validation error on any missing required var.

---

### Step 2 — DB models

Build `db/models.py`: all Pydantic types — `ApiConfig`, `EnphaseCredentials`,
`GrowattCredentials`, `RawResponse`, `ProcessedRecord`, `RecordStatus` enum.

Done when: unit tests cover valid and invalid model construction. No MongoDB, no network.

---

### Step 3 — MongoDB connection + `manage_keys.py`

First bring up the `mongo` service via `podman compose up mongo` (a minimal
`docker-compose.yml` with just the mongo service — named volume + healthcheck; the app
service is added later at Step 13).

Build `db/mongo.py` (PyMongo `AsyncMongoClient`, TTL index on `raw_responses.created_at`,
status index on `processed_results`) and drop the now-unused `motor` and `pandas`
dependencies from `pyproject.toml` (see PLAN.md improvement 11 — nothing in the new code
uses pandas). Full `manage_keys.py` with `growatt set`, `enphase bootstrap`, `list`,
`delete`.

Done when: `growatt set → list → delete` round-trips cleanly against the podman mongo
service (a placeholder token is fine — no live API call needed). This is the first live
infrastructure test and validates the `ApiConfig` model end-to-end.

---

### Step 4 — Transform pipeline (pure functions)

Build `transform/schemas.py` (`EnphaseRow`, `GrowattRow`) and `transform/pipeline.py`,
porting parse logic from `EnPhase/cloud_measurements.py:18-50` and
`Growatt/measurements.py:39-53`.

Done when: unit tests feed captured fixture JSON (captured once from the real APIs,
redacted, committed as test fixtures) and assert correct output shapes. No network, no DB.

---

### Step 5 — Growatt client

Build `clients/growatt.py`: three async methods (`list_plants`, `list_devices`,
`device_energy`) wrapping `growattServer.OpenApiV1` via `asyncio.to_thread`, with
`asyncio.sleep(2)` between per-device calls.

Done when: `unittest.mock` patches `OpenApiV1`; tests assert correct output shapes and
that the 2 s sleep fires between device calls.

---

### Step 6 — First full Growatt cycle (no scheduler)

Build a throwaway `dev_run.py`: load config → connect Mongo → read one Growatt
`api_configs` doc → run one fetch cycle → write `raw_responses` + `processed_results`.

Done when: both documents appear in Mongo with the correct shapes. **This is the first
time all layers touch real infrastructure together** — model/DB mismatches surface here.

---

### Step 7 — Output client + base HTTP

Build `clients/base.py` (shared `httpx.AsyncClient`, structured request/response logger)
and `clients/output.py` (POST one record, basic error classification, no retry yet).

Done when: a minimal echo server receives the POST with the correct JSON body.

---

### Step 8 — Tenacity retry + status transitions

Add `tenacity` retry to `clients/output.py`: transient errors (5xx, 429, timeout) retry
up to 3× with exponential backoff; permanent errors (other 4xx) set `status=failed`
immediately without retrying.

**Two retry levels, two counters** (see PLAN.md improvement 3): tenacity's up-to-3×
backoff is *internal to one delivery attempt* and must not touch `retry_count`.
`retry_count` increments once per delivery attempt (scheduler tick or flush pass), and is
what gates the flush job's `retry_count < N` filter. `last_error` and `last_attempt_at`
are written on every delivery attempt.

Done when: a mock 503→200 sequence produces two HTTP attempts, `status=posted`, and
`retry_count` incremented by exactly 1 (one delivery attempt); a mock 400 produces no
retry and `status=failed`.

---

### Step 9 — APScheduler + flush job

Build `scheduler/jobs.py`: `AsyncIOScheduler` with one job per enabled
`(provider, account_id)`, `max_instances=1`, `coalesce=True`, staggered start offsets
(`hash(account_id) % interval_seconds`); plus a flush job with per-worker jitter that
picks up:

- `status ∈ {pending, failed}` with `retry_count < N`;
- **stuck `posting` records** — `status=posting` with `last_attempt_at` older than a
  staleness threshold (a few minutes), left behind by a worker that crashed mid-POST
  (see PLAN.md improvement 3);
- records that exhaust the `retry_count` cap stay `failed` and are logged/counted for
  human review — never silently dropped from monitoring.

Also build `providers.py`: a dispatch registry mapping each provider name to a
`ProviderSpec(credentials_class, client_factory, transform_fn)`. The scheduler dispatches
by lookup rather than an `if/elif` chain on the provider string, so a future provider is
one registry entry plus its implementation files. The transform pipeline and
`manage_keys.py` consume the same registry. See PLAN.md → "Provider extensibility" for the
rationale (and why this is *not* a unified `Provider` ABC).

Done when: two accounts with different intervals each get a job registered; a manually
inserted `failed` record transitions to `posted` after the flush job fires; a manually
inserted stale `posting` record (old `last_attempt_at`) is reclaimed and reposted; the
scheduler resolves both providers through the `providers.py` registry with no provider
branching.

---

### Step 10 — EnPhase client

Build `clients/enphase.py`: async `fetch_inverter_summary`, OAuth token refresh (written
fresh — do **not** copy the conflict-ridden `EnPhase/auth.py`), site_id lookup with
`site_cache` write-back to Mongo.

Done when: `respx` fixtures for the systems and inverter summary endpoints; token refresh
triggers when `expires_at` is within 2 h; `site_cache` is written back to Mongo on a
cache miss.

---

### Step 11 — Lease claiming + heartbeat

Add atomic lease claim to `main.py` (`findOneAndUpdate` on
`lease.expires_at < now || lease == null`), plus a heartbeat renewal coroutine.

Done when: two worker processes with distinct `WORKER_ID` values against the same Mongo
each claim disjoint account sets; killing one worker and waiting for the lease TTL causes
the survivor to adopt the orphaned accounts automatically.

---

### Step 12 — Wire `main.py` + smoke test

Build the full `main.py`: claim leases → start heartbeat coroutine → set
`ThreadPoolExecutor` → init Mongo indexes → build scheduler from claimed accounts →
install `SIGTERM`/`SIGINT` handlers for graceful shutdown (release leases, drain
scheduler).

Done when: `docker compose up` with a third echo-server service shows one
`raw_responses` doc, one `processed_results` doc with `status=posted`, and one POST
received — for each provider — after one cycle.

---

### Step 13 — Dockerfile + app service in Compose

Build a multi-stage `Dockerfile` (`python:3.13-slim`, wheels built in build stage,
non-root user in runtime stage) and add the `app` service to the existing
`docker-compose.yml` (the `mongo` service was already added at Step 3). Finalise the mongo
health check, named volume, and `WORKER_ID` per replica.

Done when: `podman compose up --build` passes the step 12 smoke test inside the
container.

---

### Notes on ordering

- **Steps 1–4** are pure Python — no infrastructure needed. Good for nailing data shapes
  before touching a network.
- **Growatt before EnPhase** (steps 5–6 before step 10): Growatt auth is a single static
  token; no OAuth dance to debug while the pipeline is still new.
- **Retry before leasing** (step 8 before step 11): retry logic is self-contained and
  easy to mock; leasing requires multiple live processes.
- `dev_run.py` from step 6 is explicitly throwaway — its logic is absorbed into
  `main.py` at step 12.

---

## Files to be created

```
project/
├── config.py                # Pydantic Settings (MONGO_URI, OUTPUT_API_URL,
│                            #   OUTPUT_API_TOKEN, LOG_LEVEL, RAW_TTL_DAYS,
│                            #   THREADPOOL_WORKERS, WORKER_ID,
│                            #   LEASE_TTL_MULTIPLIER, LEASE_RENEWAL_INTERVAL)
├── db/
│   ├── mongo.py             # PyMongo AsyncMongoClient, index setup (TTL on
│   │                        #   raw_responses.created_at, status index on
│   │                        #   processed_results)
│   └── models.py            # Pydantic: ApiConfig, EnphaseCredentials,
│                            #   GrowattCredentials, RawResponse,
│                            #   ProcessedRecord, RecordStatus enum
├── clients/
│   ├── base.py              # Shared httpx.AsyncClient, tenacity retry
│   │                        #   decorator, request/response logger
│   ├── enphase.py           # EnPhase: async fetch_inverter_summary,
│   │                        #   OAuth refresh, site_id resolution;
│   │                        #   writes refreshed tokens + site_cache to Mongo
│   ├── growatt.py           # Growatt: 3-method async facade
│   │                        #   (list_plants / list_devices / device_energy)
│   │                        #   over growattServer.OpenApiV1 via
│   │                        #   asyncio.to_thread; asyncio.sleep(2) between
│   │                        #   per-device calls
│   └── output.py            # Async POST one record, tenacity retry,
│                            #   status/error classification
├── transform/
│   ├── schemas.py           # EnphaseRow, GrowattRow Pydantic models
│   │                        #   (also serve as output-API wire format)
│   └── pipeline.py          # raw_response → list[ProcessedRecord];
│                            #   one EnphaseRow per cycle,
│                            #   N GrowattRows per cycle
├── scheduler/
│   └── jobs.py              # AsyncIOScheduler bootstrap from claimed
│                            #   api_configs; one job per enabled
│                            #   (provider, account_id); flush job for
│                            #   status ∈ {pending, failed}
├── providers.py             # Dispatch registry: provider name →
│                            #   ProviderSpec(credentials_class,
│                            #   client_factory, transform_fn). Scheduler,
│                            #   pipeline, and manage_keys look up here
│                            #   instead of branching on provider string.
│                            #   Introduced at Step 9 (see PLAN.md).
├── manage_keys.py           # CLI: enphase bootstrap / growatt set /
│                            #   list / delete (keyed on --account-id)
├── main.py                  # Entrypoint: claim leases for WORKER_ID,
│                            #   start heartbeat renewal coroutine, set
│                            #   default executor (THREADPOOL_WORKERS),
│                            #   init Mongo indexes, build scheduler from
│                            #   claimed accounts, install signal handlers,
│                            #   run forever
├── Dockerfile               # python:3.13-slim + uv, multi-stage, non-root
│                            #   user; installs deps via `uv sync --no-dev`
├── docker-compose.yml       # mongo service added at Step 3 (run via
│                            #   `podman compose`); app service added at
│                            #   Step 13. Named volume, healthcheck.
│                            #   for multi-machine: run app as separate
│                            #   containers with distinct WORKER_ID,
│                            #   all pointing at the same MONGO_URI
├── pyproject.toml           # dependency spec and project metadata
├── uv.lock                  # pinned lockfile (committed); regenerate with
│                            #   `uv sync` — source of truth for Docker too
└── .env.example             # MONGO_URI=mongodb://mongo:27017
                             # OUTPUT_API_URL=
                             # OUTPUT_API_TOKEN=
                             # LOG_LEVEL=INFO
                             # RAW_TTL_DAYS=30
                             # THREADPOOL_WORKERS=32
                             # WORKER_ID=worker-1
                             # LEASE_TTL_MULTIPLIER=2
                             # LEASE_RENEWAL_INTERVAL=30
```

---

## Existing functions to reuse / port

- Parsing logic from `EnPhase/cloud_measurements.py:18-50` (`parse_inverter_data`) →
  port into `transform/pipeline.py` as a pure function over a validated Pydantic model.
- Token-refresh predicate (`should_refresh`) and Basic-auth-encoded refresh call from
  `EnPhase/auth.py:65-106` → port into `clients/enphase.py` using httpx. Do **not** carry
  over the merge-conflict text; rewrite from the logic only.
- Site ID lookup from `EnPhase/site_id.py:3-25` → port into `clients/enphase.py`.
- Device-iteration + `type == 7` filter and field extraction from
  `Growatt/measurements.py:39-53` → port into `clients/growatt.py` /
  `transform/pipeline.py`.

---

## Verification plan

1. **Unit-level Pydantic round-trips**: feed fixture EnPhase and Growatt responses
   through `transform/pipeline.py`; assert `EnphaseRow` / `GrowattRow` shapes.

2. **`docker compose up`** brings up `app` + `mongo`. Run `manage_keys.py enphase
   bootstrap` and `manage_keys.py growatt set` to seed `api_configs`.

3. **Confirm scheduling**: app logs show one `AsyncIOScheduler` job per enabled
   `(provider, account_id)` with the configured intervals. First fire is staggered.

4. **End-to-end happy path**: point `OUTPUT_API_URL` at a temporary echo server (e.g. a
   small FastAPI receiver as a third compose service). After one EnPhase cycle: one
   `raw_responses` doc, one `processed_results` doc with `status=posted`, one POST
   received. After one Growatt cycle: one `raw_responses` doc, N `processed_results`
   docs, N POSTs received.

5. **Retry / flush path**: stop the echo server, run a cycle, observe `processed_results`
   accumulate with `status=failed` and `retry_count` incrementing. Restart the echo
   server; wait for the flush job; verify all rows reach `status=posted`.

6. **OAuth refresh path**: manually set `expires_at` in an EnPhase config doc to within
   the 2 h window. Trigger the EnPhase job. Verify the token-refresh POST fires, the new
   token is written back to Mongo, and the fetch succeeds.

7. **Restart durability**: `docker compose restart app`. Verify the next cycle uses the
   persisted `site_id` from `api_configs.site_cache` (no `/api/v4/systems` call) and the
   persisted EnPhase tokens (no re-bootstrap).

8. **Lease failover**: start two worker containers. Confirm disjoint account sets. Stop
   one; wait for lease TTL; confirm the survivor adopts the orphaned accounts.

9. **Multi-account scaling smoke test**: seed 10 dummy Growatt accounts (mocked
   `OpenApiV1`), set `THREADPOOL_WORKERS=4`, verify cycles complete without deadlock.
   Raise to 32 and confirm full parallelism.
