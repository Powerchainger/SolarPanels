# Python Version

Python 3.13 (pinned via `requires-python` in `pyproject.toml`).

# Dependencies

The project uses [uv](https://docs.astral.sh/uv/) for dependency management, inside a
nix-provided shell. From the nix shell, a single command sets up a full development
environment (including the dev extras — pytest, respx, etc.):

```bash
uv sync --extra dev
```

---

Check README.md files inside each folder on how to run inverter scripts. 