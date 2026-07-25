"""Unit tests for utils/crypto.py — Fernet credential encryption at rest."""
import pytest

from utils import crypto

pytestmark = pytest.mark.unit


def test_encrypt_decrypt_round_trip():
    plaintext = "ya29.a0ARrdaM-super-secret-refresh-token"
    ciphertext = crypto.encrypt(plaintext)

    assert ciphertext != plaintext
    assert ciphertext.startswith("enc::")
    assert crypto.is_encrypted(ciphertext)
    assert crypto.decrypt(ciphertext) == plaintext


def test_encrypt_none_and_empty_are_noop():
    assert crypto.encrypt(None) is None
    assert crypto.encrypt("") == ""
    assert crypto.decrypt(None) is None
    assert crypto.decrypt("") == ""


def test_decrypt_passes_through_legacy_plaintext():
    """Values written before encryption existed have no enc:: prefix."""
    legacy_plaintext = "some-old-plaintext-smtp-password"
    assert crypto.decrypt(legacy_plaintext) == legacy_plaintext
    assert not crypto.is_encrypted(legacy_plaintext)


def test_encrypt_is_idempotent_on_already_encrypted_value():
    once = crypto.encrypt("a-token")
    twice = crypto.encrypt(once)
    assert once == twice
    assert crypto.decrypt(twice) == "a-token"


def test_decrypt_with_no_key_configured_returns_none(monkeypatch):
    """If ENCRYPTION_KEY is unset, decrypting a real ciphertext must fail safe (None), not crash."""
    ciphertext = crypto.encrypt("a-secret")
    assert ciphertext.startswith("enc::")

    # Simulate a missing/invalid key without touching the process-wide lru_cache.
    monkeypatch.setattr(crypto, "_get_fernet", lambda: None)
    assert crypto.decrypt(ciphertext) is None


def test_encrypt_with_no_key_configured_stores_plaintext(monkeypatch):
    """No key configured (e.g. local dev) — encrypt() must not raise, just store as-is."""
    monkeypatch.setattr(crypto, "_get_fernet", lambda: None)
    assert crypto.encrypt("plain-value") == "plain-value"
