# Concepts

The terms below are specific to this service or to the two inverter
vendors. Where logs, commands or settings use a different word, it's given
in brackets.

## Accounts and polling

**Account**
:   One solar installation the service collects data for: one Growatt API
    token, or one EnPhase authorization. Identified by its provider and an
    **account ID** you choose, written `provider/account-id` in command
    output and `provider::account-id` in logs. The account ID is also sent
    with every record. See [Managing accounts](accounts.md).

**Provider**
:   The inverter vendor's cloud API an account is read from: `growatt` or
    `enphase`.

**Poll** (fetch, cycle)
:   One round of calls to a provider for one account. Each account is
    polled every **poll interval** seconds (`interval_seconds`, set per
    account with `--interval`).

**Seed, re-seed**
:   Storing an account with `manage_keys.py growatt-set` / `enphase-set`.
    Running the command again for an existing account is a *re-seed*: it
    replaces the credentials, interval and enabled flag, and clears the
    *auth failed* state.

**Account health** (`last_success`, `failures`, `AUTH FAILED`)
:   Fields shown by `manage_keys.py list`. `last_success` is the last time
    the provider answered a poll; `failures` counts failed polls in a row.
    **Auth failed** means EnPhase rejected the account's authorization three
    times in a row, so the account is paused until it is authorized again
    and re-seeded. Health only describes the provider side, not delivery to
    your API.

## Records and delivery

**Record** (processed result)
:   One JSON object sent to your output API, built from a poll: one per
    EnPhase installation per poll, one per Growatt inverter per poll. The
    format is on the [overview](../index.md#what-gets-sent-to-the-output-api).
    Each record is also stored in MongoDB with its **delivery status**
    (`posting`, `posted`, `failed`); see
    [Monitoring](monitoring.md#delivery-status).

**Raw response**
:   The unprocessed answer from a provider, kept in MongoDB for
    `RAW_TTL_DAYS` to help investigate records that look wrong.

**Retry job** (flush)
:   Runs every 5 minutes and sends records that weren't delivered yet, up
    to 5 times per record. Its log lines start with `flush:`.

## Workers

**Worker**
:   One running copy of the service. Its name is `WORKER_ID` (or the
    container's hostname) plus a random suffix chosen at each start, e.g.
    `worker-1-3f9a0c2e`, so two workers never share a name. A normal
    installation has one worker. Several workers can share one MongoDB to
    divide the accounts between them; see
    [Scaling](maintenance.md#scaling-to-more-workers).

**Lease** (claim, lost lease)
:   A worker's reservation of an account, stored on the account in MongoDB,
    so that each account is polled by exactly one worker. A worker
    **claims** every enabled account that has no lease or whose lease has
    expired. Leases are released when a worker stops cleanly.

**Heartbeat** (lease renewal)
:   Every `LEASE_RENEWAL_INTERVAL` seconds (30), a worker extends the leases
    it holds to *poll interval × `LEASE_TTL_MULTIPLIER`* from now. If a
    worker crashes, its leases are no longer extended. Another worker, or
    the restarted one, notices within about three minutes and takes the
    accounts over; at the latest they become free when the leases expire. `lost lease on …` in the logs means another
    worker now holds that account. The heartbeat also feeds the container
    health check.

**Sync** (sync job)
:   Every minute, a worker claims any free accounts and updates its
    schedule to match: new and re-enabled accounts start, changed intervals
    apply, and deleted, disabled or lost accounts stop. This is why account
    changes need no restart.

## Growatt

**ShinePhone**
:   Growatt's app for system owners. The API token for an account comes
    from there.

**Plant**
:   Growatt's word for an installation. One token can see several plants;
    the service reads only the first.

**MIN/TLX inverters** (device type 7)
:   The Growatt hybrid inverter series this service reads. Growatt reports
    each device with a numeric type, and these are type 7. Other devices in
    a plant, such as **dataloggers** (the communication module that connects
    an inverter to Growatt's cloud), are ignored.

**Error 10012** (`error_frequently_access`)
:   Growatt's answer when a token calls the same API function more often
    than about once per 5 minutes. See
    [Troubleshooting](troubleshooting.md#growatt-error-10012-or-error_frequently_access).

## EnPhase

**System** (site)
:   EnPhase's word for an installation, identified by a numeric system ID
    (`site_id` in logs). One EnPhase login can own several systems; the
    service reads only the first.

**Developer application**
:   An application registered in the EnPhase developer portal. It supplies
    the `client_id`, `client_secret` and `api_key`, and determines the API
    plan (monthly call quota). System owners grant access to this
    application, so every EnPhase account depends on it. See
    [Adding an EnPhase account](accounts.md#adding-an-enphase-account).

**Authorization, access token, refresh token**
:   The result of a system owner approving the developer application (OAuth2
    authorization code flow). The access token is valid for about a day and
    the service renews it automatically with the refresh token. When the
    refresh token stops working, the account shows *auth failed* and must be
    authorized again.
