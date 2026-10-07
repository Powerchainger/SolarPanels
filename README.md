# Solar Scraper

Async service that fetches solar inverter data from **EnPhase** and **Growatt** APIs,
stores raw + processed records in MongoDB, and forwards processed records to an external
output API.

## Architecture

Single asyncio service with:
- APScheduler polling each account at its configured interval
- Atomic lease-based multi-worker scaling (add workers, accounts distribute automatically)
- Tenacity retry with flush job for failed POSTs
- MongoDB TTL on raw responses (default 30 days)
- Growatt plant/device caching (24h TTL) to reduce API calls and avoid rate limits

## Quick start

```bash
# 1. Install dependencies
uv sync --all-extras

# 2. Configure Mongo credentials
cp .env.example .env   # then fill in MONGO_ROOT_USERNAME/PASSWORD and
                       # MONGO_APP_USER/PASSWORD (alphanumeric only), and set
                       # MONGO_URI to the credentialed 127.0.0.1 form

# 3. Start MongoDB
podman compose up -d mongo

# 4. Seed credentials
uv run python manage_keys.py growatt-set --account-id my-home --api-token YOUR_TOKEN
uv run python manage_keys.py enphase-set --account-id my-home   # see "EnPhase accounts" below
uv run python manage_keys.py list

# 5. Run the service
uv run python main.py
# or
uv run solar-scraper
```

### EnPhase accounts

EnPhase uses OAuth2. Each account stores `client_id`, `client_secret`,
`redirect_uri`, `api_key` and a token pair, all encrypted at rest except
`client_id` and `redirect_uri`.

**Token lifecycle.** The access token lasts about 24 h. Before every fetch the
service checks it (`clients/enphase.py: fetch_cycle`) and, within 2 h of expiry,
refreshes it with the refresh token (`refresh_access_token`). The new tokens are
written back to MongoDB, so nothing needs doing by hand while the service runs.
There is no CLI command for refreshing.

**When it breaks.** The refresh token can die: the service was stopped long enough
for it to lapse, a rotated token was lost, or the customer revoked access. EnPhase then
rejects the refresh (HTTP 401), or a data call (HTTP 401). A data-call 401 first
triggers one forced token refresh and retry, in case another process rotated the token.
After 3 rejections with no successful fetch in between, the service marks the account
`auth_failed`: it logs one error, stops fetching that account, and does not retry. Any
successful fetch resets the count. `manage_keys.py list` shows the state (`AUTH FAILED`,
`last_success=`, `failures=`), and every skipped cycle logs a warning, so a dead account
stays visible. Re-seeding the account with new credentials clears the flag.
`dev.py test-enphase` goes through the same job, so it also skips a flagged account, and
prints the account's health after each run.

`last_success` and `failures` track whether the provider *fetch* worked. Transform and
delivery problems show up on the records in `processed_results`, not there. Growatt
accounts get the failure counts but are never frozen: only EnPhase reports auth failures
this way.

**Authorizing (first time, or to recover).** `enphase-set` makes no API calls, so
the token exchange happens outside the service:

1. Open the authorization URL in a browser, signed in as the system owner, and approve:
   `https://api.enphaseenergy.com/oauth/authorize?response_type=code&client_id=<CLIENT_ID>&redirect_uri=<REDIRECT_URI>`
2. EnPhase redirects to `redirect_uri` with `?code=<AUTH_CODE>`. Copy the code. It is
   short-lived and single-use.
3. Exchange it for tokens with the legacy helper. Fill `EnPhase/access_gen.json`
   (see `access_gen.json.example`), then, from inside `EnPhase/`:
   ```bash
   cd EnPhase && uv run python access_token_gen.py
   ```
   This writes `credentials.json` with `access_token`, `refresh_token` and `expires_in`.
   Treat both JSON files as secrets and delete them afterwards.
4. Seed the account (safe on a running service). Use `--from-file` so nothing lands in shell
   history. The file needs `client_id`, `client_secret`, `redirect_uri`, `api_key`,
   `access_token`, `refresh_token` and `expires_in`:
   ```bash
   uv run python manage_keys.py enphase-set --account-id my-home --from-file creds.json
   ```
5. Verify with `uv run python dev.py test-enphase --account-id my-home`, then restart.

Re-seeding updates only credentials, interval and enabled flag. A running worker keeps its
lease. The cached EnPhase site is always dropped (one extra `/systems` call on the next
cycle); the Growatt plant/device cache is dropped only when the `api_token` changes.

### MongoDB access control

Mongo requires authentication and binds to `127.0.0.1` only (host tooling
uses the credentialed `MONGO_URI` from `.env`; nothing on the LAN can reach
it). Two users: a root/admin user (`MONGO_ROOT_*`, healthcheck + admin) and a
least-privilege app user (`MONGO_APP_*`, `readWrite` on `solar_scraper` only).

**Fresh install:** set the four vars in `.env`, then `podman compose up -d`.
`mongo-init.js` (mounted into `/docker-entrypoint-initdb.d/`) creates the
app user automatically on the first start.

**Migrating an existing database** (created before auth, like the current
one): `MONGO_INITDB_ROOT_*` and `mongo-init.js` only run on an *empty* data
dir, so create both users manually **before** restarting with auth, or the
server locks itself out:

```bash
# while Mongo is still running WITHOUT auth:
podman compose up -d mongo
mongosh mongodb://127.0.0.1:27017/admin --eval 'db.createUser({user: "root", pwd: "<MONGO_ROOT_PASSWORD>", roles: ["root"]})'
mongosh mongodb://127.0.0.1:27017/solar_scraper --eval 'db.createUser({user: "<MONGO_APP_USER>", pwd: "<MONGO_APP_PASSWORD>", roles: [{role: "readWrite", db: "solar_scraper"}]})'
# then restart to apply auth:
podman compose up -d
```

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MONGO_URI` | Yes | — | MongoDB connection string |
| `OUTPUT_API_URL` | Yes | — | API that receives processed records |
| `WORKER_ID` | Yes | — | Unique per-container identifier |
| `OUTPUT_API_TOKEN` | No | `""` | Bearer token for output API |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `RAW_TTL_DAYS` | No | `30` | Days to keep raw responses |
| `THREADPOOL_WORKERS` | No | `32` | Max threads for Growatt SDK calls |
| `LEASE_TTL_MULTIPLIER` | No | `2` | Lease expiry = interval × this |
| `LEASE_RENEWAL_INTERVAL` | No | `30` | Seconds between heartbeat renewals |
| `SHUTDOWN_TIMEOUT_SECONDS` | No | `50` | On SIGTERM, how long running jobs may finish before being cancelled; the compose `stop_grace_period` must cover it (see `config.py`) |

## Docker

```bash
podman compose up --build
```

Stop the app with `podman compose stop app` (or `down`/`restart`), not plain
`podman stop`. podman-compose applies `stop_grace_period` only on its own
stop commands; it does not set it on the container, so a direct
`podman stop` or a host shutdown allows podman's default 10 s. That is
enough when the app is idle but cuts short the wait for a running job
(see `SHUTDOWN_TIMEOUT_SECONDS`).

## Development

```bash
# Run tests
uv run pytest tests/

# Echo server — prints received records to terminal
# The echo server listens on 0.0.0.0 by default (reachable from containers).
uv run python dev.py serve
uv run python dev.py serve --port 9999
#
# Set .env OUTPUT_API_URL depending on where the echo server runs:
#   Same machine as Podman:
#     OUTPUT_API_URL=http://host.containers.internal:8080
#     (host.containers.internal is a Podman/Docker built-in DNS name that
#      resolves to the host from inside any container — use this instead of
#      the host's LAN IP, which may be blocked by the firewall's reverse-
#      path filtering.)
#   Different machine on the network:
#     OUTPUT_API_URL=http://<machine-ip>:8080
#     (The machine running the echo server must allow inbound traffic on
#      that port — check its firewall settings.)

# Live test a single cycle (requires MongoDB + real API token)
uv run python dev.py test-growatt --account-id my-home
uv run python dev.py test-enphase --account-id my-home

# Manage credentials
uv run python manage_keys.py list
uv run python manage_keys.py growatt-set --account-id my-home
uv run python manage_keys.py enphase-set --account-id my-home
uv run python manage_keys.py delete --provider growatt --account-id my-home
```

## Design

- See [PLAN.md](PLAN.md) for architecture decisions, schema design, and deployment stages.
- See [IMPL.md](IMPL.md) for implementation details and status.

### Growatt caching

The Growatt API has a rate limit of ~1 request per endpoint per 5 minutes. To avoid
exceeding this (especially when the same API token is used by ShinePhone or Home
Assistant), the service caches `plant_list` and `device_list` responses in MongoDB
for 24 hours. This reduces API calls from 4 to 2 per cycle (only the per-device
`min_energy` calls are made each cycle).
