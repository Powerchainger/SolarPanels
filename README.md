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

## Quick start

```bash
# 1. Install dependencies
uv sync --all-extras

# 2. Start MongoDB
podman compose up -d mongo

# 3. Seed credentials
uv run python manage_keys.py growatt set --account-id my-home --api-token YOUR_TOKEN
uv run python manage_keys.py list

# 4. Run the service
uv run python main.py
# or
uv run solar-scraper
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

## Docker

```bash
podman compose up --build
```

## Development

```bash
# Run tests
uv run pytest tests/

# Echo server — prints received records to terminal
uv run python dev.py serve
uv run python dev.py serve --port 9999

# Live test a single cycle (requires MongoDB + real API token)
uv run python dev.py test-growatt --account-id my-home
uv run python dev.py test-enphase --account-id my-home

# Manage credentials
uv run python manage_keys.py list
uv run python manage_keys.py growatt set --account-id my-home
uv run python manage_keys.py delete --provider growatt --account-id my-home
```

## Design

- See [PLAN.md](PLAN.md) for architecture decisions, schema design, and deployment stages.
- See [IMPL.md](IMPL.md) for implementation details and status.
