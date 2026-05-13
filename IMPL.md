# Implementation Plan

> Architecture, schema design, and technology decisions: see [PLAN.md](PLAN.md).

## Incremental steps

Each step leaves the system in a runnable or testable state and has a clear done-signal.
Steps 1–4 require no infrastructure. Step 6 is the first real integration test.

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

Build `db/mongo.py` (Motor client, TTL index on `raw_responses.created_at`, status index
on `processed_results`). Full `manage_keys.py` with `growatt set`, `enphase bootstrap`,
`list`, `delete`.

Done when: `growatt set → list → delete` round-trips cleanly against a real Mongo
instance. This is the first live infrastructure test and validates the `ApiConfig` model
end-to-end.

---

### Step 4 — Transform pipeline (pure functions)

Build `transform/schemas.py` (`EnphaseRow`, `GrowattRow`) and `transform/pipeline.py`,
porting parse logic from `EnPhase/cloud_measurements.py:18-50` and
`Growatt/measurements.py:39-53`.

Done when: unit tests feed captured fixture JSON (captured once from the real APIs,
redacted, committed as test fixtures) and assert correct output shapes. No network, no DB.

---

### Step 5 — Growatt client

Build `clients/api_b.py`: three async methods (`list_plants`, `list_devices`,
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
immediately without retrying. Write `retry_count`, `last_error`, `last_attempt_at` on
each attempt.

Done when: a mock 503→200 sequence produces two attempts and `status=posted`; a mock 400
produces no retry and `status=failed`.

---

### Step 9 — APScheduler + flush job

Build `scheduler/jobs.py`: `AsyncIOScheduler` with one job per enabled
`(provider, account_id)`, `max_instances=1`, `coalesce=True`, staggered start offsets
(`hash(account_id) % interval_seconds`); plus a flush job for
`status ∈ {pending, failed}` with per-worker jitter.

Done when: two accounts with different intervals each get a job registered; a manually
inserted `failed` record transitions to `posted` after the flush job fires.

---

### Step 10 — EnPhase client

Build `clients/api_a.py`: async `fetch_inverter_summary`, OAuth token refresh (written
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

### Step 13 — Dockerfile + Compose

Build a multi-stage `Dockerfile` (`python:3.13-slim`, wheels built in build stage,
non-root user in runtime stage) and finalise `docker-compose.yml` (Mongo health check,
named volume, `WORKER_ID` per replica).

Done when: `docker compose up --build` passes the step 12 smoke test inside the
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
│   ├── mongo.py             # Motor client, index setup (TTL on
│   │                        #   raw_responses.created_at, status index on
│   │                        #   processed_results)
│   └── models.py            # Pydantic: ApiConfig, EnphaseCredentials,
│                            #   GrowattCredentials, RawResponse,
│                            #   ProcessedRecord, RecordStatus enum
├── clients/
│   ├── base.py              # Shared httpx.AsyncClient, tenacity retry
│   │                        #   decorator, request/response logger
│   ├── api_a.py             # EnPhase: async fetch_inverter_summary,
│   │                        #   OAuth refresh, site_id resolution;
│   │                        #   writes refreshed tokens + site_cache to Mongo
│   ├── api_b.py             # Growatt: 3-method async facade
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
├── manage_keys.py           # CLI: enphase bootstrap / growatt set /
│                            #   list / delete (keyed on --account-id)
├── main.py                  # Entrypoint: claim leases for WORKER_ID,
│                            #   start heartbeat renewal coroutine, set
│                            #   default executor (THREADPOOL_WORKERS),
│                            #   init Mongo indexes, build scheduler from
│                            #   claimed accounts, install signal handlers,
│                            #   run forever
├── Dockerfile               # python:3.13-slim multi-stage, non-root user
├── docker-compose.yml       # app + mongo (named volume, healthcheck);
│                            #   for multi-machine: run app as separate
│                            #   containers with distinct WORKER_ID,
│                            #   all pointing at the same MONGO_URI
├── requirements.txt         # httpx, motor, pymongo, pydantic,
│                            #   pydantic-settings, apscheduler, tenacity,
│                            #   growattServer, typer, python-dateutil,
│                            #   tzdata, respx (test)
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
  `EnPhase/auth.py:65-106` → port into `clients/api_a.py` using httpx. Do **not** carry
  over the merge-conflict text; rewrite from the logic only.
- Site ID lookup from `EnPhase/site_id.py:3-25` → port into `clients/api_a.py`.
- Device-iteration + `type == 7` filter and field extraction from
  `Growatt/measurements.py:39-53` → port into `clients/api_b.py` /
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
