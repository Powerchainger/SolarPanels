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
    ENCRYPTION_KEY    Fernet key for encrypting credentials at rest. Generate
                     with `openssl rand -base64 32`. The service refuses to
                     start without a valid key; a lost key makes stored
                     credentials unrecoverable (back it up).

Optional variables (defaults shown):
    OUTPUT_API_TOKEN  Bearer token for the output API. Leave empty if the API
                      requires no authentication. (default: "")
    LOG_LEVEL         Python logging level: DEBUG, INFO, WARNING, ERROR.
                      (default: "INFO")
    RAW_TTL_DAYS      How long raw API responses are kept in MongoDB before the
                      TTL index deletes them. (default: 30)
    POSTED_TTL_DAYS   How long records are kept in MongoDB after they were
                      delivered to the output API. `inf` keeps them forever.
                      Undelivered records are never deleted. (default: 7)
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
    SHUTDOWN_TIMEOUT_SECONDS  On SIGTERM/SIGINT, how long running fetch/flush
                              jobs may take to finish before they are
                              cancelled. Shutdown then needs up to 10 s more
                              (main.UNWIND_SECONDS + main.CLEANUP_SECONDS), so
                              the container's stop_grace_period
                              (docker-compose.yml) must exceed this + 10 s.
                              Must be > 0. (default: 50)
"""

import math

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from crypto import validate_key


class Settings(BaseSettings):
    # extra="ignore": .env also carries compose/mongosh-only keys
    # (MONGO_ROOT_*, MONGO_APP_*) that are not app settings.
    # hide_input_in_errors: a validation error is printed before logging is
    # set up, and pydantic would echo the offending value (or, for a missing
    # field, every other setting) — ENCRYPTION_KEY and the credentialed
    # MONGO_URI would land in the container logs.
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore",
        hide_input_in_errors=True,
    )

    mongo_uri: str
    output_api_url: str
    output_api_token: str = ""
    log_level: str = "INFO"
    raw_ttl_days: int = 30
    # float so that "inf" (keep forever) parses; MongoDB caps a TTL at
    # 2**31 - 1 seconds (~68 years), so a finite value must stay below that.
    posted_ttl_days: float = Field(default=7, gt=0)
    threadpool_workers: int = 32
    worker_id: str
    lease_ttl_multiplier: int = 2
    lease_renewal_interval: int = 30
    shutdown_timeout_seconds: float = Field(default=50, gt=0)
    encryption_key: str

    @field_validator("posted_ttl_days")
    @classmethod
    def _posted_ttl_fits_mongo(cls, v: float) -> float:
        if not math.isinf(v) and v * 86400 > 2**31 - 1:
            raise ValueError("too large for MongoDB; use `inf` to keep records forever")
        return v

    @field_validator("encryption_key")
    @classmethod
    def _encryption_key_format(cls, v: str) -> str:
        # Fail fast at boot: a malformed key must not silently degrade every
        # account (decision C) once crypto ops start failing per-document.
        return validate_key(v)
