# Managing accounts

An **account** is one solar installation the service polls: one Growatt API
token, or one EnPhase authorization. Accounts are stored in MongoDB and
managed with `manage_keys.py`, run from the project directory on the host:

```bash
uv run python manage_keys.py --help
```

Each account has a **provider** (`growatt` or `enphase`) and an
**account ID**, a name you choose, such as `smith-residence` or
`office-roof`. The pair must be unique. The account ID is included in every
record sent to the output API, so choose a neutral name: no customer names or
addresses.

!!! warning "Never edit accounts directly in MongoDB"
    Credentials are stored encrypted. Always use `manage_keys.py`.

## When changes take effect

No restart is needed for any change. The service checks for changed
accounts every minute:

| Change | Takes effect |
|---|---|
| New credentials for an existing account | next poll |
| Disable or delete an account | next poll |
| Add a new account | within a minute |
| Change the poll interval | within a minute |
| Re-enable a disabled account | within a minute |

A new account's first poll may come up to one interval later, so that
accounts don't all poll at the same moment.

## Listing accounts

```bash
uv run python manage_keys.py list
```

```
  growatt/my-home  interval=600s  enabled=True  last_success=2026-10-14 12:10 UTC
  enphase/office  interval=3600s  enabled=True  last_success=2026-10-14 11:42 UTC  failures=2
  enphase/barn  interval=3600s  enabled=True  last_success=2026-10-02 08:00 UTC  failures=5  AUTH FAILED — re-authorize and re-seed

  3 config(s) total
```

- `last_success`: last time the provider answered a poll.
- `failures`: polls that failed in a row (only shown when not zero).
- `AUTH FAILED`: EnPhase rejected the [authorization](concepts.md#enphase), and the account is paused
  until you [re-authorize it](#authorizing-an-enphase-account).

## Adding a Growatt account

You need the account's **Growatt API token**, which the system owner can get
from the ShinePhone app.

```bash
uv run python manage_keys.py growatt-set --account-id my-home
```

The token is asked for with hidden input, so it does not end up in your
shell history. Options:

| Option | Default | Meaning |
|---|---|---|
| `--interval` | `600` | Seconds between polls. Don't go below `300`: Growatt allows about one call per endpoint per 5 minutes per token, and answers faster polling with error `10012`. |
| `--disabled` | enabled | Store the account without polling it. |
| `--from-file` | | Read `{"api_token": "..."}` from a JSON file instead of prompting. |

!!! note "What is collected"
    The service reads the **first plant** visible to the token, and from it
    only the **MIN/TLX inverters** (Growatt device type 7). Other device
    types, such as dataloggers, are ignored. If one token sees several
    plants, only the first one is collected. These terms are explained
    under [Concepts](concepts.md#growatt).

If the same token is also used by other software (the ShinePhone app, Home
Assistant), they share the rate limit. The service caches the plant and
device list for 24 hours to use as few calls as possible.

## Adding an EnPhase account

EnPhase uses OAuth2: the system owner has to approve access once, in a
browser. You need, from your **EnPhase developer account** (the application
registered at developer-v4.enphase.com):

- `client_id` and `client_secret`
- `api_key`
- `redirect_uri` (as registered for the application; EnPhase's default is
  `https://api.enphaseenergy.com/oauth/redirect_uri`)

and the **system owner**, who must log in and approve.

!!! warning "Who owns the EnPhase developer application?"
    Every authorization is tied to the
    [developer application](concepts.md#enphase) it was made
    with. If that application is deleted, or its owner loses access to the
    developer account, **every EnPhase account authorized with it** has to
    be authorized again under a new application. Make sure the application
    is registered under an account your organization controls, not a
    personal one, and note which EnPhase API plan it is on (the plan limits
    the number of calls per month).

    *Open point at handover:* the owner and plan of the current application
    still have to be confirmed.

### Authorizing an EnPhase account

Use these steps for a new account, and also to recover an account marked
`AUTH FAILED`.

1. **Get approval.** Have the system owner open this URL, log in to their
   EnPhase account, and approve:

    ```
    https://api.enphaseenergy.com/oauth/authorize?response_type=code&client_id=<CLIENT_ID>&redirect_uri=<REDIRECT_URI>
    ```

2. **Copy the code.** EnPhase redirects the browser to
   `<REDIRECT_URI>?code=<AUTH_CODE>`. Copy the code from the address bar.
   It can only be used once and expires after a short time, so go straight
   on to the next step.

3. **Exchange the code for tokens.** Create `EnPhase/access_gen.json`
   from `EnPhase/access_gen.json.example`, fill in the values and the code,
   then run:

    ```bash
    cd EnPhase && uv run python access_token_gen.py && cd ..
    ```

    This writes `EnPhase/credentials.json` with `access_token`,
    `refresh_token` and `expires_in`.

4. **Combine everything into one file**, e.g. `creds.json`:

    ```json
    {
      "client_id": "...",
      "client_secret": "...",
      "redirect_uri": "...",
      "api_key": "...",
      "access_token": "...",
      "refresh_token": "...",
      "expires_in": 86399
    }
    ```

5. **Store the account**, disabled for now so that you can test it before
    the service starts polling it:

    ```bash
    uv run python manage_keys.py enphase-set --account-id office --from-file creds.json --disabled
    ```

    Add `--interval <seconds>` to change the default poll interval of 3600
    seconds (1 hour). EnPhase API plans limit the number of calls per month,
    so check your plan's quota before polling more often.

6. **Test it** with one poll (see [Testing an account](#testing-an-account)):

    ```bash
    uv run python dev.py test-enphase --account-id office
    ```

    It should report `records : 1` and end without an error.

7. **Enable it** by storing it again without `--disabled` (keep any
    `--interval` you passed):

    ```bash
    uv run python manage_keys.py enphase-set --account-id office --from-file creds.json
    ```

8. **Delete the secret files:**

    ```bash
    rm EnPhase/access_gen.json EnPhase/credentials.json creds.json
    ```

From now on the service keeps the tokens valid by itself: the access token
lasts about 24 hours and is refreshed automatically shortly before it
expires. As long as the service runs regularly, no further action is needed.

!!! warning "The authorization can still run out"
    If the service is stopped for a long time, or the system owner revokes
    access in their EnPhase account, the stored authorization stops working.
    The service then pauses the account after 3 rejected attempts in a row
    and shows it as `AUTH FAILED`. Repeat the steps above to fix it; for an
    account the service already holds, store it enabled straight away and
    check the next poll instead of running `dev.py` (see
    [Testing an account](#testing-an-account)).
    See [Troubleshooting](troubleshooting.md#an-enphase-account-shows-auth-failed).

!!! note "What is collected"
    The service reads the **first system** of the EnPhase account and sends
    the current output of each micro-inverter plus the total.

## Changing an account

Run the same `growatt-set` or `enphase-set` command again with the same
`--account-id` (a [re-seed](concepts.md#accounts-and-polling)). This replaces the credentials, the interval and the enabled
flag, and clears an `AUTH FAILED` state. It is safe while the service is
running.

The options you leave out return to their defaults, so pass `--interval`
again if the account uses a non-default interval.

## Pausing an account

```bash
uv run python manage_keys.py growatt-set --account-id my-home --disabled
```

You have to enter the credentials again. To resume, run the command again
without `--disabled`.

## Deleting an account

```bash
uv run python manage_keys.py delete --provider growatt --account-id my-home
```

This removes the account and its credentials. Records that were already
collected stay in the database.

## Testing an account

`dev.py` runs one poll for one account as a dry run: it calls the provider,
converts the answer into records and prints how many there are and which
fields they have, without values. Nothing is stored as a record and nothing
is sent to your output API.

```bash
uv run python dev.py test-growatt --account-id my-home
uv run python dev.py test-enphase --account-id office
```

It does store what the provider needs to keep working, such as refreshed
EnPhase tokens. That is why it refuses to run while a worker holds the
account: the service and the test must not refresh the same token at once.

- **A new account:** add it with `--disabled`, test it, then store it again
  without `--disabled`. A disabled account is never held by a worker.
- **An account the service already polls:** the next poll is the test; check
  `manage_keys.py list` or the logs afterwards. To run `dev.py` anyway, stop
  the service first (`podman compose stop app`, which releases its accounts)
  and start it again afterwards. Disabling the account is not enough: the
  worker's hold on it only runs out after *poll interval × 2*.

It is a real call to the provider and counts towards its rate limit, so
don't run it repeatedly against a Growatt account.
