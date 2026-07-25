"""Offline tests for Unipile webhook signature verification.

Covers the HMAC-SHA256 hardening plus backward-compatible plain-secret
handling and the fail-closed behavior outside dev/test.
"""

import hashlib
import hmac
import time

import pytest

import routes.webhooks as webhook_routes

pytestmark = pytest.mark.unit

SECRET = "unipile-shared-secret"
BODY = b'{"event":"messages","id":1}'


def _hmac(secret: str, payload: bytes) -> str:
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def _hmac_ts(secret: str, payload: bytes, timestamp: str) -> str:
    signed = f"{timestamp}.".encode() + payload
    return hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


def test_valid_hmac_over_raw_body_is_accepted():
    sig = _hmac(SECRET, BODY)
    assert webhook_routes._verify_unipile_signature(SECRET, BODY, sig, "")


def test_sha256_prefixed_hmac_is_accepted():
    sig = "sha256=" + _hmac(SECRET, BODY)
    assert webhook_routes._verify_unipile_signature(SECRET, BODY, sig, "")


def test_tampered_body_fails_hmac():
    sig = _hmac(SECRET, BODY)
    assert not webhook_routes._verify_unipile_signature(SECRET, b'{"evil":1}', sig, "")


def test_wrong_secret_fails_hmac():
    sig = _hmac("other-secret", BODY)
    assert not webhook_routes._verify_unipile_signature(SECRET, BODY, sig, "")


def test_empty_signature_is_rejected(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "app_env", "production")
    assert not webhook_routes._verify_unipile_signature(SECRET, BODY, "", "")


def test_timestamped_hmac_within_tolerance_is_accepted():
    ts = str(int(time.time()))
    sig = _hmac_ts(SECRET, BODY, ts)
    assert webhook_routes._verify_unipile_signature(SECRET, BODY, sig, ts)


def test_timestamped_hmac_outside_tolerance_is_rejected():
    stale = str(int(time.time()) - 3600)
    sig = _hmac_ts(SECRET, BODY, stale)
    assert not webhook_routes._verify_unipile_signature(SECRET, BODY, sig, stale)


def test_non_numeric_timestamp_is_rejected():
    sig = _hmac(SECRET, BODY)
    assert not webhook_routes._verify_unipile_signature(SECRET, BODY, sig, "not-a-number")


def test_plain_secret_accepted_in_transition_mode(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "unipile_webhook_require_hmac", False)
    assert webhook_routes._verify_unipile_signature(SECRET, BODY, SECRET, "")


def test_plain_secret_rejected_when_hmac_required(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "unipile_webhook_require_hmac", True)
    # Plain secret no longer accepted...
    assert not webhook_routes._verify_unipile_signature(SECRET, BODY, SECRET, "")
    # ...but a valid HMAC still is.
    sig = _hmac(SECRET, BODY)
    assert webhook_routes._verify_unipile_signature(SECRET, BODY, sig, "")


def test_unsigned_fails_closed_without_secret_in_production(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "app_env", "production")
    assert not webhook_routes._verify_unipile_signature("", BODY, "", "")


def test_unsigned_allowed_without_secret_in_test_env(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "app_env", "test")
    assert webhook_routes._verify_unipile_signature("", BODY, "", "")
