"""
Central configuration for the solar-scraper service.

All settings are read from environment variables (or a .env file in the working
directory). Copy .env.example to .env and fill in the required values before
running the service locally. In production, inject the variables directly into
the container environment — do not ship a .env file.

Required variables (the service will refuse to start if any are missing):
    MONGO_URI        Connection string for the central MongoDB instance, e.g.
                     mongodb://localhost:27017 or a replica-set/Atlas SRV URI.
    OUTPUT_API_URL   Base URL of the external API that receives processed records.
    WORKER_ID        Unique identifier for this worker instance, e.g. "worker-1".
                     Used for lease ownership — every running container must have
                     a distinct value.

Optional variables (defaults shown):
    OUTPUT_API_TOKEN  Bearer token for the output API. Leave empty if the API
                      requires no authentication. (default: "")
    LOG_LEVEL         Python logging level: DEBUG, INFO, WARNING, ERROR.
                      (default: "INFO")
    RAW_TTL_DAYS      How long raw API responses are kept in MongoDB before the
                      TTL index deletes them. (default: 30)
    THREADPOOL_WORKERS  Max threads for wrapping synchronous Growatt SDK calls
                        via asyncio.to_thread. One thread is held per in-flight
                        Growatt account cycle. (default: 32)
    LEASE_TTL_MULTIPLIER  Lease expiry = interval_seconds × this value. A worker
                          that stops renewing its heartbeat will have its accounts
                          reclaimed by other workers after this multiple of the
                          account's poll interval. (default: 2)
    LEASE_RENEWAL_INTERVAL  Seconds between heartbeat renewals. Should be well
                             below LEASE_TTL_MULTIPLIER × shortest interval.
                             (default: 30)
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # extra="ignore": .env also carries compose/mongosh-only keys
    # (MONGO_ROOT_*, MONGO_APP_*) that are not app settings.
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    mongo_uri: str
    output_api_url: str
    output_api_token: str = ""
    log_level: str = "INFO"
    raw_ttl_days: int = 30
    threadpool_workers: int = 32
    worker_id: str
    lease_ttl_multiplier: int = 2
    lease_renewal_interval: int = 30
