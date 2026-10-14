"""
Pydantic models for every MongoDB document the service reads and writes.

Three collections:

  api_configs        One document per (provider, account_id). Holds credentials,
                     scheduling interval, lease ownership, and cached lookup data.
                     In MongoDB the compound _id is {"provider": ..., "account_id": ...};
                     that mapping is handled by db/mongo.py — these two fields are
                     plain attributes here.

  raw_responses      Verbatim JSON body returned by each upstream API call.
                     Deleted automatically after RAW_TTL_DAYS by a TTL index on
                     created_at.

  processed_results  One document per record forwarded (or to be forwarded) to
                     the output API. Tracks posting status and retry history.

All datetimes must be UTC-aware. Use datetime.now(UTC) — never datetime.utcnow(),
which returns a naive datetime and causes silent comparison bugs.
"""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    model_validator,
)

from crypto import decrypt_str, encrypt_str, is_encrypted


# ---------------------------------------------------------------------------
# Shared primitives
# ---------------------------------------------------------------------------


class RecordStatus(StrEnum):
    """Lifecycle state of a ProcessedRecord.

    Transitions as implemented:
        fetch_job inserts records as `posting` and POSTs them at once:
            posting → posted
            posting → failed    (permanent error, e.g. HTTP 4xx)
            posting → posting   (transient error; reclaimed by the flush job
                                 once last_attempt_at is stale)
        flush_job retries `failed` (and `pending`) while retry_count < max:
            failed  → posted / failed

    `pending` is the model default but no code path writes it today; the
    flush job still picks it up. Nothing claims a record before a POST, so
    two workers' flush jobs could send the same record twice.
    """

    PENDING = "pending"
    POSTING = "posting"
    POSTED = "posted"
    FAILED = "failed"


class Lease(BaseModel):
    """Worker ownership record embedded in an ApiConfig document.

    A worker claims a lease atomically at boot and renews it every
    LEASE_RENEWAL_INTERVAL seconds. If expires_at passes without renewal
    (e.g. the worker crashed), any other worker may claim the account.
    """

    worker_id: str
    expires_at: datetime  # UTC


class SiteCache(BaseModel):
    """Cached result of the EnPhase /api/v4/systems call.

    Avoids a round-trip on every cycle. Populated on the first successful
    fetch; invalidated by writing None and will be refreshed on the next
    cycle.
    """

    site_id: int
    user: str  # site name, used as user_value in the processed row


class GrowattCache(BaseModel):
    """Cached Growatt plant_id and device list.

    Avoids plant_list and device_list API calls every cycle. These rarely
    change (you don't add plants/inverters often), so caching for 24h is
    safe. Reduces API calls from 4 to 2 per cycle for 2 inverters.
    """

    plant_id: str
    devices: list[dict[str, Any]]  # filtered device dicts with device_sn, type, etc.
    cached_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# Encrypted secrets
# ---------------------------------------------------------------------------


def _decrypt_if_needed(value: Any) -> Any:
    """Decrypt a Fernet token on load; leave plaintext values untouched.

    Plaintext arrives from the CLI/tests (before a write) or from
    pre-migration docs. A value that only *looks* encrypted but fails to
    decrypt raises InvalidToken → per-account degradation (decision C).
    """
    if isinstance(value, str) and is_encrypted(value):
        return decrypt_str(value)
    return value


EncryptedSecret = Annotated[
    str,
    BeforeValidator(_decrypt_if_needed),
    PlainSerializer(lambda v: encrypt_str(v), return_type=str, when_used="json"),
]
"""A str field encrypted at rest (GDPR Art. 32): ciphertext in MongoDB,
plaintext in memory. All existing read paths (leases.py, scheduler/jobs.py)
and write paths (manage_keys.py, clients/enphase.py) work unchanged."""


# ---------------------------------------------------------------------------
# Credentials — one model per provider
# ---------------------------------------------------------------------------


class EnphaseCredentials(BaseModel):
    """OAuth2 credentials for the EnPhase cloud API.

    access_token and refresh_token are empty strings until the one-time
    bootstrap (manage_keys.py enphase bootstrap) has been run. After that,
    the service keeps them up-to-date in MongoDB automatically.
    """

    provider: Literal["enphase"] = "enphase"
    client_id: str
    client_secret: EncryptedSecret
    redirect_uri: str
    api_key: EncryptedSecret
    access_token: EncryptedSecret = ""
    refresh_token: EncryptedSecret = ""
    expires_at: datetime | None = None  # UTC; refresh when within 2 h of expiry
    expires_in: int | None = None       # seconds, as returned by the token endpoint
    token_type: str = "Bearer"


class GrowattCredentials(BaseModel):
    """Static API token obtained from the ShinePhone app."""

    provider: Literal["growatt"] = "growatt"
    api_token: EncryptedSecret


# ---------------------------------------------------------------------------
# api_configs collection
# ---------------------------------------------------------------------------


class ApiConfig(BaseModel):
    """One document per (provider, account_id) in the api_configs collection.

    Use manage_keys.py to create, update, and delete these documents — never
    edit them in MongoDB directly, as the credentials format must stay in sync
    with what the client code expects.
    """

    model_config = ConfigDict(populate_by_name=True)

    provider: Literal["enphase", "growatt"]
    account_id: str
    enabled: bool = True
    interval_seconds: int
    credentials: Annotated[
        EnphaseCredentials | GrowattCredentials,
        Field(discriminator="provider"),
    ]
    site_cache: SiteCache | None = None           # EnPhase only
    growatt_cache: GrowattCache | None = None     # Growatt only
    lease: Lease | None = None
    store_raw: bool = True
    # Health, written by the fetch job. `auth_failed` means the provider rejected
    # the credentials: the account is skipped until it is re-seeded.
    # `last_success_at` / `consecutive_failures` track whether the provider
    # *fetch* worked, not delivery: transform and POST outcomes live on
    # `processed_results`. `seeded_at` lets the fetch job tell a re-seed from a token refresh.
    # `consecutive_auth_failures` must reach
    # AUTH_FAILURE_THRESHOLD (scheduler/jobs.py) before `auth_failed` is set.
    auth_failed: bool = False
    seeded_at: datetime | None = None  # set only by manage_keys.py (re-seed marker)
    last_success_at: datetime | None = None
    # Every fetch attempt, failed ones included: provider rate limits count
    # calls, so a worker start resumes the rhythm from here (first_run_at).
    last_fetch_at: datetime | None = None
    consecutive_failures: int = 0
    consecutive_auth_failures: int = 0
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def credentials_match_provider(self) -> "ApiConfig":
        if self.credentials.provider != self.provider:
            raise ValueError(
                f"credentials.provider '{self.credentials.provider}' "
                f"does not match provider '{self.provider}'"
            )
        return self


# ---------------------------------------------------------------------------
# raw_responses collection
# ---------------------------------------------------------------------------


class RawResponse(BaseModel):
    """Verbatim API response body stored for debugging purposes.

    created_at carries a MongoDB TTL index (RAW_TTL_DAYS). Only written when
    the parent ApiConfig has store_raw=True.
    """

    provider: str
    account_id: str
    body: dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ---------------------------------------------------------------------------
# processed_results collection
# ---------------------------------------------------------------------------


class ProcessedRecord(BaseModel):
    """A single record destined for (or already sent to) the output API.

    The record field holds a serialised EnphaseRow or GrowattRow dict
    (defined in transform/schemas.py). It also serves as the POST body,
    so the output API sees exactly the same shape as what is stored here.
    """

    provider: str
    account_id: str
    record: dict[str, Any]
    status: RecordStatus = RecordStatus.PENDING
    retry_count: int = 0
    last_error: str | None = None
    last_attempt_at: datetime | None = None  # UTC
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
