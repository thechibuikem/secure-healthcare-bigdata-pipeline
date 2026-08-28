import os
os.environ.setdefault("ENCRYPTION_KEY", "kx6E9v0m0lF8V1S0v3o1e2t3d3nWKxD9L1e0K3s1h1Y=")
# a fixed test key - never use this for real data, only for local test runs

from security.encryption import encrypt_value, decrypt_value
from security.decrypt_view import decrypt_if_allowed


def test_encrypt_decrypt_round_trip():
    original = "123-45-6789"
    encrypted = encrypt_value(original)
    assert encrypted != original
    assert decrypt_value(encrypted) == original


def test_encrypted_value_looks_nothing_like_original():
    original = "John Smith"
    encrypted = encrypt_value(original)
    assert original not in encrypted


def test_decrypt_if_allowed_clinician():
    encrypted = encrypt_value("secret-value")
    result = decrypt_if_allowed("clinician", encrypted)
    assert result == "secret-value"


def test_decrypt_if_allowed_analyst_denied():
    encrypted = encrypt_value("secret-value")
    result = decrypt_if_allowed("analyst", encrypted)
    assert result is None