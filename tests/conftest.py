"""Shared test fixtures.

Sets a test ENCRYPTION_KEY so any test that serializes credential models
(model_dump(mode="json")) or round-trips encrypted values works without a
real key. Tests never touch the real .env key.
"""

import os

from cryptography.fernet import Fernet

os.environ.setdefault("ENCRYPTION_KEY", Fernet.generate_key().decode())
