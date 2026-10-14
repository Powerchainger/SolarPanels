# Troubleshooting

Start with the logs. Most problems leave a clear message there:

```bash
podman compose logs --since 1h app
uv run python manage_keys.py list
```

## The service does not start, or keeps restarting

Look at the last lines of `podman compose logs app`:

**`MongoDB authentication failed — check MONGO_URI and the credentials`**
:   The MongoDB password in `.env` does not match the one in the database.
    Passwords in `.env` only apply when the database is first created; see
    [Changing MongoDB passwords](maintenance.md#changing-mongodb-passwords).

**`MongoDB not reachable at mongo — is the container running?`**
:   Check `podman compose ps`. Start MongoDB with `podman compose up -d mongo`
    and look at `podman compose logs mongo`.

**`Output API not reachable at … — is the container running?`**
:   The service checks your output API at startup and stops if it cannot
    connect at all (connection refused, timeout, unknown host). Check
    `OUTPUT_API_URL` and that the API is up and reachable from this machine:
    `curl -v <OUTPUT_API_URL>`. Any HTTP response, even an error, counts as
    reachable.


**`validation error for Settings`**
:   A required setting is missing or invalid; the lines below it name the
    setting. For `encryption_key`: the value must be exactly what
    `openssl rand -base64 32` generated, with nothing added.

## An EnPhase account shows AUTH FAILED

EnPhase rejected the stored authorization three times in a row, so the
service stopped polling this account. Common reasons:

- The service was stopped for a long time, and the authorization ran out.
- The system owner revoked access in their EnPhase account.
- The application's client secret was changed in the EnPhase developer
  portal.

Retrying does not help. Authorize the account again by following
[Authorizing an EnPhase account](accounts.md#authorizing-an-enphase-account).
Storing the new credentials clears the AUTH FAILED state, and the service
resumes polling at the next interval. No restart is needed.

## Growatt: error 10012, or `error_frequently_access`

Growatt limits each token to about one call per endpoint per 5 minutes.
Possible causes:

- The poll interval is below 300 seconds. Raise it with
  `growatt-set --interval 600`.
- The same token is also used by other software (ShinePhone, Home
  Assistant) at the same time.
- `dev.py test-growatt` was run while the service was also polling.

Occasional `10012` errors are harmless: the next poll usually works.

## Records are not reaching the output API

1. Check the [delivery status](monitoring.md#delivery-status) and look at
   `last_error` on the failed records.
2. An `HTTP 4xx` error means your API rejected the record (for example
   `401`: wrong `OUTPUT_API_TOKEN`; `400`/`422`: your API does not accept the
   format). Fix the cause at the API side or in `.env`.
3. Temporary errors (timeouts, `5xx`) are retried automatically every 5
   minutes, up to 5 times.

Records that used up all 5 retries are not tried again by themselves. Once
the cause is fixed, give them a new set of retries, and the retry job
delivers them within about 5 minutes:

```js
db.processed_results.updateMany(
  {status: "failed", retry_count: {$gte: 5}},
  {$set: {retry_count: 0}}
)
```

(Run it in the database shell; see [Monitoring](monitoring.md#delivery-status).)
Records are sent in batches of 100 per retry run, so a large backlog takes a
few rounds.

## An account is not polled

- **Added it recently?** New accounts are picked up within a minute, and
  their first poll can take up to one interval after that.
- **Held by an old name?** After a crash or forced stop followed by a
  `WORKER_ID` change, an account stays reserved for the old name for
  *poll interval × 2* (up to 2 hours for EnPhase), then is taken over.
- **Disabled?** Check `enabled=` in `manage_keys.py list`.
- **`last_success=never` and a growing `failures` count?** The provider call
  fails every time. Find the reason with `podman compose logs app | grep
  <account-id>`, or run `dev.py test-growatt` / `test-enphase` for that account.

## `InvalidToken` in the logs

Stored credentials could not be decrypted, which means `ENCRYPTION_KEY` is
not the key they were encrypted with. This typically happens after a restore
or a move to another machine with a different key. Put the original key back
in `.env` and restart. If the original key is lost, the affected accounts
have to be stored again with `manage_keys.py`.

## EnPhase: `No systems found for EnPhase account`

The authorization works, but the EnPhase account it belongs to has no
systems. Check that the system owner approved access with the correct
EnPhase login.
