"""Tests for manage_keys.py credential-migration helpers — no Mongo needed.

Exercises the pure field transformation used by the migrate-encrypt
subcommand (idempotency + rollback); the Mongo loop itself is a thin
find/update wrapper verified by hand at migration time.
"""

from manage_keys import _transform_credentials
from crypto import is_encrypted


def _enphase_creds() -> dict:
    return {
        "provider": "enphase",
        "client_id": "cid",
        "client_secret": "csec",
        "redirect_uri": "https://example.com/cb",
        "api_key": "key123",
        "access_token": "at",
        "refresh_token": "rt",
    }


def test_migrate_encrypt_pass():
    out = _transform_credentials(_enphase_creds(), decrypt=False)
    for f in ("client_secret", "api_key", "access_token", "refresh_token"):
        assert is_encrypted(out[f]), f"{f} not encrypted"
    # Non-secrets untouched.
    assert out["client_id"] == "cid"
    assert out["redirect_uri"] == "https://example.com/cb"


def test_migrate_encrypt_idempotent():
    once = _transform_credentials(_enphase_creds(), decrypt=False)
    twice = _transform_credentials(once, decrypt=False)
    assert once == twice  # re-running must not double-encrypt


def test_migrate_decrypt_rollback():
    encrypted = _transform_credentials(_enphase_creds(), decrypt=False)
    out = _transform_credentials(encrypted, decrypt=True)
    assert out == _enphase_creds()


def test_migrate_decrypt_skips_plaintext():
    out = _transform_credentials(_enphase_creds(), decrypt=True)
    assert out == _enphase_creds()


def test_migrate_empty_values_untouched():
    creds = _enphase_creds()
    creds["access_token"] = ""
    out = _transform_credentials(creds, decrypt=False)
    assert out["access_token"] == ""
