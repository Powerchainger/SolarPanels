# Configuration

All settings are environment variables, read from the `.env` file in the
project directory. Compose passes `.env` to the service container, and the
account tools on the host read it too.

After changing `.env`, restart the service to apply it:

```bash
podman compose up -d
```

## Required

| Variable | Description |
|---|---|
| `MONGO_URI` | MongoDB connection string used by the **host** tools (`manage_keys.py`, `dev.py`). Points at `127.0.0.1`. The service container ignores it and builds its own from `MONGO_APP_USER`/`MONGO_APP_PASSWORD`. |
| `MONGO_ROOT_USERNAME`, `MONGO_ROOT_PASSWORD` | MongoDB admin user. Only used to create the admin user on a fresh database, and for the container health check and backups. |
| `MONGO_APP_USER`, `MONGO_APP_PASSWORD` | The service's own MongoDB user, with access to the `solar_scraper` database only. Letters and digits only. |
| `ENCRYPTION_KEY` | Key that encrypts the provider credentials stored in MongoDB. Generate with `openssl rand -base64 32`. The service refuses to start without a valid key. **Losing it makes all stored credentials unreadable**, so keep a copy outside the machine. |
| `OUTPUT_API_URL` | Base URL of your output API. Records are POSTed to `<OUTPUT_API_URL>/records`. Must be reachable when the service starts. |

## Optional

| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | *(hostname)* | Readable name for this [worker](concepts.md#workers) (running copy of the service), e.g. `worker-1`. A random suffix is added at each start, so workers sharing a name still work correctly. |
| `OUTPUT_API_TOKEN` | *(empty)* | Sent as `Authorization: Bearer <token>`. Leave empty if your API needs no authentication. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. `DEBUG` also logs every API call. |
| `RAW_TTL_DAYS` | `30` | How many days the unprocessed provider responses are kept in MongoDB, for troubleshooting. Takes effect on restart. |
| `POSTED_TTL_DAYS` | `7` | How many days records are kept in MongoDB once they have been delivered to the output API, counted from when they were collected. `inf` keeps them forever. Records that were not delivered are never deleted. Takes effect on restart. |
| `THREADPOOL_WORKERS` | `32` | How many Growatt accounts can be polled at the same moment. Only matters with many Growatt accounts. |
| `LEASE_TTL_MULTIPLIER` | `2` | How long a worker's [lease](concepts.md#workers) on an account lasts without renewal: *poll interval × this value*. When a worker stops without shutting down cleanly, other workers usually take its accounts over within about three minutes, because its heartbeat stops; this value is the upper bound. |
| `LEASE_RENEWAL_INTERVAL` | `30` | Seconds between [heartbeats](concepts.md#workers), which renew the leases a worker holds. The container health check allows four missed heartbeats. |
| `SHUTDOWN_TIMEOUT_SECONDS` | `50` | When stopped, how long the service waits for running polls to finish. The Compose `stop_grace_period` (70 s) must be at least this + 10 s. If you raise one, raise the other. |

## Per-account settings

Poll interval and enabled/disabled are set per account with `manage_keys.py`,
not in `.env`. See [Managing accounts](accounts.md).

## Fixed values

These are set in the code, not configurable:

| What | Value |
|---|---|
| Retry of undelivered records | every 5 minutes, up to 5 retries per record |
| HTTP retries within one delivery | 3 attempts, with backoff |
| EnPhase token refresh | when less than 2 hours of validity remain |
| Growatt plant/device list cache | 24 hours |
| EnPhase authentication failures before an account is paused | 3 in a row |
| Check for new, changed or orphaned accounts | every minute |
