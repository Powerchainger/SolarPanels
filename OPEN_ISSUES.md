# Open issues

Everything still open for the solar-scraper service, from the project's
task list in the team vault. Done work is not repeated here; see the git
history and `docs/`.

Ordered by section, most urgent first within each.

## First deployment

- [ ] **Deploy the service.** It has never been deployed, so this is a
      fresh install with nothing to migrate: follow
      `docs/operations/installation.md`.

## Correctness

- [ ] **A failed provider call loses that cycle's data.** A 429, 5xx or
      timeout from Growatt or EnPhase raises out of `spec.fetch` in
      `fetch_job` (`scheduler/jobs.py`) with no retry, no requeue and no
      `Retry-After` handling; the next attempt is one interval later.
      Output delivery has retries, provider polling has none. Whether gaps
      matter is product-owner question 8.
- [ ] **Growatt auth failures are never frozen.** Only EnPhase raises
      `AuthFailedError`, so a revoked Growatt token fails every cycle as an
      ordinary error and the account is never marked `auth_failed`. Needs a
      look at how the `growattServer` SDK reports a bad token first.
- [ ] **Growatt doubles its lookup calls on the failing path.** When the
      eager cache fill in `providers.py` (Growatt fetch) throws,
      `cached_plant_id` stays `None` and `fetch_cycle` repeats
      `list_plants` + `list_devices`: extra load exactly when the API is
      already failing.
- [ ] **`post_record` mutates a dict nobody reads.**
      `clients/output.py:post_record` sets `status`, `last_error` and
      `last_attempt_at` on the dict it is given, and its docstring says
      that dict is the whole `ProcessedRecord` dump, but callers pass only
      the `record` payload, so the changes are discarded. Harmless but
      misleading: return the outcome instead and fix the docstring.
- [ ] **SIGTERM during startup can leave leases held.** A signal between
      the boot claim and the installation of the signal handlers in
      `main.py` (a ~100 ms window) exits without releasing. The stale-lease
      rule now recovers those accounts within about three minutes, so the
      impact is small.

## Operability

- [ ] **Flush capacity is below the rate records are created.** The flush job
      handles `FLUSH_BATCH` (100) records per pass every 300 s, about 0.33
      records/s, while 100 accounts produce about 0.61 records/s. A
      one-hour output-API outage takes about 1.9 h to drain. Derive the
      batch from the backlog depth instead.
- [ ] **EnPhase onboarding is manual.** `manage_keys.py enphase-set` needs
      an already-exchanged access/refresh token pair; the only
      authorization-code exchange is `EnPhase/access_token_gen.py`, which
      reads a hand-pasted code from a JSON file. Blocked on choosing a CLI
      exchange or a full OAuth callback endpoint, which depends on
      product-owner questions 1, 4 and 5.

## Efficiency (nothing breaks without these)

- [ ] **Redundant Mongo reads per fetch cycle.** `fetch_job` reads the
      account document, and `clients/enphase.py` reads it again for a
      `site_cache` that `fetch_job` already loaded.
- [ ] **Sequential per-record writes and POSTs.** `fetch_job` does
      `insert_one` → POST → `update_one` per record; a 30-panel EnPhase
      site is 90 sequential awaits. `insert_many`, gathered POSTs and one
      `bulk_write` would do.
- [ ] **Missing indexes.** `api_configs` has none beyond `_id`, so
      `claim_leases`' `find({"enabled": True})` (every minute, on every
      worker) scans and pulls every credential document.
      `processed_results` wants a compound `(status, last_attempt_at)`
      index for the flush job's second pass.

## GDPR

- [ ] **Right to erasure (Art. 17) — P2.** `manage_keys.py delete` removes
      only the `api_configs` document; the customer's `raw_responses` and
      `processed_results` stay. A purge must remove all three per account,
      plus any backup copies. Whether there is a deletion obligation is
      product-owner question 3.
- [ ] **Encrypted backups — P3.** A `mongodump` backup holds the
      credentials encrypted but the household energy data in plain form.
      A formal encrypted backup flow is missing.
- [ ] **Credential-leak log review — P4.** Go through every
      `logger.exception` call and error string for credential leakage; an
      earlier pass was partial.
- [ ] **Non-code artefacts for the product owner / legal — P4.** Privacy
      notice (Art. 12–14), record of processing (Art. 30), data processing
      agreement with the host (Art. 28), breach procedure (Art. 33/34),
      TLS-in-transit and incident-response review.

## Blocked on the product owner

From the questions raised with the product owner; all unanswered.
Questions 1, 2, 3 and 9 matter most.

1. **EnPhase application model:** does each customer register their own
   EnPhase developer app, or authorize a shared one of ours? If shared, API
   quota is a hard ceiling for all customers (the free plan covers about
   one hourly-polled account), and `clients/enphase.py` taking
   `systems[0]` becomes a bug that can read the wrong customer's site.
   Gates the onboarding design.
2. **Sampling interval:** how often does the data actually need to be
   polled? The largest single lever on API calls, storage and delivery
   load.
3. **Retention:** how long must delivered records be kept, and is there a
   deletion obligation? Decides `POSTED_TTL_DAYS` and the erasure item.
4. **Broken authorization:** when a customer's EnPhase authorization
   breaks, who contacts them, and how quickly? The service now detects it
   (`auth_failed`, shown by `manage_keys.py list`) but alerts no one.
5. **Customer mix and arrival:** expected Growatt/EnPhase split (the
   capacity numbers assume 50/50), and whether customers arrive in
   batches.
6. **Scale ceiling:** is ~100 accounts the ceiling or a waypoint?
7. **Multiple installations:** does any customer have more than one plant
   or site? Only the first is read (`clients/growatt.py` takes
   `plants[0]`, `clients/enphase.py` takes `systems[0]`); any others are
   silently ignored.
8. **Gaps:** is an occasional missing data point acceptable?
9. **Duplicates:** does the receiving API tolerate a record arriving twice
   (delivery is at-least-once), and does it offer an idempotency key or a
   batch endpoint? Ask whoever owns that API.
10. **Downtime:** how much is acceptable, and does anyone monitor for it?
    Nothing alerts on a stopped service today.
11. **Credential encryption standard:** is TeSoPs required to encrypt
    customer credentials in a particular way, or does it have a secrets
    manager to use? App-level Fernet is in place; an answer only changes
    where `ENCRYPTION_KEY` comes from.

Also open: whether `store_raw` (raw provider responses, the bulk of the
write volume) should stay on by default. No CLI flag sets it today, so a
per-account opt-out also needs `--store-raw/--no-store-raw` on the two
`*-set` commands.

## Not yet confirmed in a live run

Behaviour that is implemented and tested but has not been observed in a
real run yet.

The local database is empty, so these need test accounts seeded first
(`manage_keys.py growatt-set` / `enphase-set`), with fresh tokens. The
EnPhase checks need a working EnPhase refresh token.

- [ ] **Load balancing between workers:** releasing accounts beyond a
      worker's fair share. Needs at least three enabled accounts to
      trigger; covered by a multi-worker simulation test only. (Unique
      worker ids, standby workers and the stale-lease takeover were
      confirmed with `--scale app=3` on the local stack.)
- [ ] **Whether EnPhase rotates refresh tokens.** The first live token
      refresh log line answers it.
- [ ] **EnPhase read timeouts:** whether the 30 s read timeout ended the
      ~5 s `ReadTimeout`s on EnPhase fetches.
- [ ] **EnPhase inverter `serial_number`:** records key inverters by it,
      falling back to `inverter_N`; its presence in real responses is not
      confirmed.

## Deferred on purpose: revisit only at a larger scale

Known limits that are fine at ~100 accounts. Listed so they are not
re-investigated.

- **Heartbeat renews leases one by one.** About 1.5 s per 30 s tick at 100
  accounts; around 1000, leases start expiring under their own heartbeat.
  Fix: one `update_many` per distinct `interval_seconds`.
- **Growatt opens a new session per call** (`clients/growatt.py`), so each
  API call does a full TLS handshake. Costs nothing measurable at ~0.33
  calls/s.
- **One httpx client for everything:** EnPhase polling and output delivery
  share one `TimedClient`; `clients/base.py:create_client` is unused.
- **`PLAN.md` overstates the threadpool as the scaling limit.**
  `THREADPOOL_WORKERS=32` runs at about 1% at 100 accounts. Correct the
  design doc so nobody tunes the wrong setting.
- **Load balancing counts accounts, not calls.** A Growatt account (every
  6 min) costs about ten times the provider calls of an EnPhase account
  (hourly). Weight by `3600 / interval_seconds` if the mix gets lopsided.
