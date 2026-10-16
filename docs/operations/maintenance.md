# Maintenance

## Starting and stopping

```bash
podman compose up -d          # start everything
podman compose stop app       # stop the service, keep MongoDB running
podman compose restart app    # restart the service
podman compose down           # stop everything
```

Use the `podman compose` commands rather than plain `podman stop`. On
shutdown the service lets polls in progress finish (up to 50 seconds) before
it stops. `podman compose` allows for this. A plain `podman stop`, or a
host shutdown, waits only 10 seconds and may interrupt a poll. That is
usually harmless, but in the worst case it can lose an EnPhase token that
was just renewed, and the account then needs to be
[authorized again](accounts.md#authorizing-an-enphase-account).

Stopping MongoDB (`down`) is safe: the data lives in the `mongo-data` volume
and survives.

## Backups

MongoDB holds everything the service needs: the accounts (with encrypted
credentials) and the collected records. Back it up regularly, for example
daily.

**Create a backup:**

```bash
mkdir -p backups
podman compose exec -T mongo sh -c \
  'mongodump -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" \
     --authenticationDatabase admin --db solar_scraper --archive --gzip' \
  > "backups/solar_scraper-$(date +%Y%m%d-%H%M%S).archive.gz"
```

This works while the service is running. To run it daily, put the command
in a small script and call it from cron or a systemd timer. Copy the backups
off the machine; a backup on the same disk does not protect against losing
that disk.

**Restore a backup** (replaces the current `solar_scraper` database):

```bash
podman compose stop app
podman compose exec -T mongo sh -c \
  'mongorestore -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" \
     --authenticationDatabase admin --archive --gzip --drop' \
  < backups/solar_scraper-20261014-030000.archive.gz
podman compose start app
```

!!! danger "A backup is useless without the encryption key"
    The credentials in the backup are encrypted with `ENCRYPTION_KEY`. Keep
    the key stored safely and separately from the backups, but make sure
    it is available when you need to restore.

## Upgrading the service

```bash
git pull
podman compose build app
podman compose up -d --force-recreate app
podman compose logs -f app
```

Use these two steps rather than `podman compose up -d --build app`: with
podman-compose (1.5.0 at least) that rebuilds the image but keeps running
the existing container on the old one. To check which image runs:
`podman ps --format "{{.Names}} {{.ImageID}}"`.

Take a backup first. Check the release notes or commit history for changes
that need extra steps, for example new settings in `.env.example`.

## Rotating the encryption key

Do this when the key may have been exposed, or as part of a regular
rotation policy. The service must be stopped throughout.

1. Stop the service and take a [backup](#backups):

    ```bash
    podman compose stop app
    ```

2. With the **old** key still in `.env`, decrypt the stored credentials:

    ```bash
    uv run python manage_keys.py migrate-encrypt --decrypt
    ```

3. Generate a new key (`openssl rand -base64 32`), put it in `.env` as
   `ENCRYPTION_KEY`, and encrypt again:

    ```bash
    uv run python manage_keys.py migrate-encrypt
    ```

4. Start the service and check that the accounts still work:

    ```bash
    podman compose start app
    uv run python manage_keys.py list
    ```

5. Store the new key where you kept the old one. Backups made before the
   rotation still need the **old** key, so keep it as long as you keep those
   backups.

Both commands accept `--dry-run` to show what would change without writing.

## Changing MongoDB passwords

The MongoDB users are created only when the database is first set up.
Changing the passwords in `.env` afterwards does **not** change them in
MongoDB, and the service will fail to log in. Change them in MongoDB first:

```bash
podman compose exec mongo mongosh -u root -p --authenticationDatabase admin solar_scraper \
  --eval 'db.changeUserPassword("<MONGO_APP_USER>", "<new app password>")'
```

Then update `MONGO_APP_PASSWORD` and `MONGO_URI` in `.env` and run
`podman compose up -d`. The root password works the same way
(`db.getSiblingDB("admin").changeUserPassword("root", …)`), then update
`MONGO_ROOT_PASSWORD`.

## Scaling to more workers

A single [worker](concepts.md#workers) comfortably handles dozens of
accounts. More workers only become useful for hundreds of accounts.

The service can run as several workers sharing one MongoDB. Each account
is then polled by exactly one worker, which holds its
[lease](concepts.md#workers). On one machine, start more workers with
`podman compose up -d --scale app=2`; they reach MongoDB over the Compose
network. Workers on other machines need MongoDB to be reachable from
there, which the provided Compose setup does not allow (MongoDB only
listens on `127.0.0.1`), so they need a separate MongoDB deployment.

How accounts are divided:

- Every minute, each worker claims any account that no other running worker
  holds. Which worker gets a new account is first come, first served;
  accounts are not rebalanced between workers.
- If a worker dies, its heartbeat stops, and another worker takes over its
  accounts within about three minutes. A worker that holds nothing simply
  waits, so a second worker also works as a standby.
- Workers may share a `WORKER_ID`: each adds a random suffix to it at
  start, so their leases stay separate. A distinct `WORKER_ID` per worker
  only makes the logs easier to read.
- Retries are coordinated: each record is reserved before it is sent, so
  workers don't deliver the same record at the same time. As with a single
  worker, a crash between sending a record and recording that it was sent
  means it is sent again later, so your output API should tolerate an
  occasional duplicate.
