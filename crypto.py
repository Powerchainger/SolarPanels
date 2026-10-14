"""Fernet credential encryption helpers (GDPR Art. 32 — credentials at rest).

The key comes from ENCRYPTION_KEY (env or .env). It is loaded lazily on
first use, so tooling (manage_keys.py, dev.py) only needs the key when it
actually touches credentials, while the app fails fast at boot via
config.py's required field + format validator.

A lost key = all stored credentials unrecoverable. Back up ENCRYPTION_KEY
with the backups in ./backups/. Key rotation = `manage_keys.py
migrate-encrypt --decrypt` with the old key, then `migrate-encrypt` with the
new one (see its docstring).
"""

from __future__ import annotations

import functools
import os

from cryptography.fernet import Fernet

# Fernet tokens always start with this prefix — used to tell encrypted
# values apart from plaintext ones (e.g. pre-migration docs).
_TOKEN_PREFIX = "gAAAA"

# ponytail: prefix sniffing means a plaintext value that happens to start
# with "gAAAA" would be misread as ciphertext. Real provider tokens/keys
# don't look like this; a wrong decrypt raises InvalidToken and degrades
# that account only (decision C).

# Single source of truth for which credential fields are secrets.
SECRET_FIELDS = {
    "enphase": ["client_secret", "api_key", "access_token", "refresh_token"],
    "growatt": ["api_token"],
}


def _get_key() -> str:
    key = os.environ.get("ENCRYPTION_KEY", "")
    if not key:
        raise RuntimeError(
            "ENCRYPTION_KEY is not set — refusing to encrypt/decrypt "
            "credentials. Set it in .env (openssl rand -base64 32) or the "
            "container environment."
        )
    return key


@functools.lru_cache(maxsize=1)
def _fernet() -> Fernet:
    return Fernet(_get_key())


def validate_key(key: str) -> str:
    """Return the key if it is a valid Fernet key, else raise ValueError."""
    Fernet(key)
    return key


def is_encrypted(value: str) -> bool:
    """True if value looks like a Fernet token (pre-encrypted)."""
    return value.startswith(_TOKEN_PREFIX)


def encrypt_str(value: str) -> str:
    """Encrypt a string. Empty strings pass through untouched."""
    if not value:
        return value
    return _fernet().encrypt(value.encode()).decode()


def decrypt_str(value: str) -> str:
    """Decrypt a Fernet token. Raises InvalidToken on wrong key/corruption."""
    return _fernet().decrypt(value.encode()).decode()
