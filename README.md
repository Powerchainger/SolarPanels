# Solar Scraper

Service that polls solar inverter data from the **EnPhase** and **Growatt**
cloud APIs, stores it in MongoDB, and delivers each record to an output API.

Full documentation is in [`docs/`](docs/index.md): concepts, installation,
configuration, managing accounts, monitoring, maintenance, troubleshooting,
and an architecture overview.

## Quick start

```bash
uv sync
cp .env.example .env && chmod 600 .env   # fill in, see docs/operations/installation.md
podman compose up -d mongo
uv run python manage_keys.py growatt-set --account-id my-home
podman compose build app && podman compose up -d app
podman compose logs -f app
```

[Installation](docs/operations/installation.md) covers each step, including
the encryption key you must back up, and
[Managing accounts](docs/operations/accounts.md) the one-time EnPhase
authorization.

## Development

```bash
uv sync --all-extras
uv run --all-extras python -m pytest tests/   # plain `uv run` drops the test extras
uv run python dev.py serve                     # echo server standing in for the output API
uv run python dev.py test-growatt --account-id my-home   # one live poll
```

## Documentation site

The docs are plain Markdown and read fine on GitHub. To build the site with
[Zensical](https://zensical.org):

```bash
uv run --group docs zensical serve            # preview on http://localhost:8000
uv run --group docs zensical build --strict   # into site/, fails on broken links
```

## Background

[`PLAN.md`](PLAN.md) and [`IMPL.md`](IMPL.md) are the original design and
implementation plans. They record intent and the reasoning behind it; where
they disagree with the code or `docs/`, the code and `docs/` are current.
