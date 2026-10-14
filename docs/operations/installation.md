# Installation

This page sets up the service on a single Linux machine: MongoDB and the
service each run in a container, managed by Compose. The examples use Podman;
Docker works the same way (`docker compose` instead of `podman compose`).

## Prerequisites

- **Podman** with `podman-compose`, or Docker with the Compose plugin.
- **uv** ([installation guide](https://docs.astral.sh/uv/getting-started/installation/)).
  The account tools (`manage_keys.py`, `dev.py`) run on the host with uv.
  uv installs the right Python (3.13) by itself.
- **openssl**, to generate passwords and the encryption key.
- Network access from the machine to `api.enphaseenergy.com`, the Growatt API,
  and your output API.

## 1. Get the code

```bash
git clone <repository-url> solar-scraper
cd solar-scraper
uv sync
```

## 2. Create the configuration file

```bash
cp .env.example .env
chmod 600 .env
```

Then fill in `.env`:

1. **MongoDB passwords.** Generate two passwords and put them in
   `MONGO_ROOT_PASSWORD` and `MONGO_APP_PASSWORD`:

    ```bash
    openssl rand -hex 24
    ```

    Use only letters and digits. Special characters break the connection
    string.

2. **`MONGO_URI`.** Replace `APP_USER` and `APP_PASSWORD` with the values of
   `MONGO_APP_USER` and `MONGO_APP_PASSWORD`. The account tools on the host use
   this URI. The service container builds its own.

3. **`ENCRYPTION_KEY`.** All provider credentials are encrypted in the
   database with this key. Generate it once:

    ```bash
    openssl rand -base64 32
    ```

    !!! danger "Store this key somewhere safe, outside this machine"
        Without it, the stored credentials cannot be decrypted, and every
        account would have to be set up again from scratch. Keep it in your
        password manager or secrets store, next to your database backups.

4. **`OUTPUT_API_URL`** (and **`OUTPUT_API_TOKEN`** if your API needs one).
   Records are POSTed to `<OUTPUT_API_URL>/records`. The service checks that
   this URL is reachable when it starts, and stops if it is not.

5. **`WORKER_ID`.** Keep `worker-1` for a single machine. See
   [Scaling](maintenance.md#scaling-to-more-workers) if you run more than one.

All settings are described in [Configuration](configuration.md).

## 3. Start MongoDB

```bash
podman compose up -d mongo
```

On the first start MongoDB creates its admin user and the service's own
database user (`mongo-init.js`). MongoDB listens on `127.0.0.1` only, so it is
not reachable from the network.

## 4. Add your accounts

You can also add accounts later, while the service runs.

```bash
uv run python manage_keys.py growatt-set --account-id my-home
uv run python manage_keys.py list
```

[Managing accounts](accounts.md) covers both providers, including the
one-time EnPhase authorization.

To check an account before going live, run one poll against a local test
receiver. Nothing is sent to your output API:

```bash
uv run python dev.py test-growatt --account-id my-home
```

## 5. Start the service

```bash
podman compose build app
podman compose up -d
podman compose logs -f app
```

A healthy start looks like this:

```
INFO solar-scraper: Starting solar-scraper worker worker-1
INFO db.mongo: MongoDB indexes ensured (raw TTL=30d, posted TTL=7d)
INFO leases: claimed account growatt::my-home (lease until 2026-10-14T12:20:00+00:00)
INFO leases: claimed account enphase::office (lease until 2026-10-14T14:00:00+00:00)
INFO solar-scraper: claimed 2 account(s): growatt/my-home, enphase/office
INFO scheduler.jobs: registered job fetch::growatt::my-home (interval=600s, first run in 212s)
INFO scheduler.jobs: registered job fetch::enphase::office (interval=3600s, first run in 1904s)
INFO scheduler.jobs: registered flush job (interval=300s, jitter=17.0s)
INFO scheduler.jobs: registered sync job (interval=60s)
INFO solar-scraper: scheduler started — 4 job(s) registered
```

The [worker](concepts.md#workers) has claimed both accounts and scheduled a
poll for each, plus the retry job (`flush`) and the account sync job. Each
account gets its first poll within one interval, spread out so that
accounts don't all poll at once. From then on, each successful delivery logs
a line like:

```
INFO clients.output: POST https://api.example.com/records → 201 (status=posted, attempts=1)
```

Both containers restart automatically after a crash
(`restart: unless-stopped`). Podman has no always-running daemon, so whether
they also come back after a **reboot** depends on the host setup (for example
`podman-restart.service`, or systemd units generated for the containers).
Reboot the machine once after installation and check that both containers
are running again.

## 6. Set up backups

Before you rely on the service, set up the database backup described in
[Maintenance](maintenance.md#backups).
