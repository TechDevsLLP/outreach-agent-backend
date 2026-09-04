"""API tests: LinkedIn hosted-auth connection capture.

Covers the /connect/notify public Unipile callback (the fix for onboarding
never reflecting a completed LinkedIn connection) plus the authenticated
/connect/webhook fallback. Unipile's account/profile fetch is mocked at the
route-module level (_fetch_unipile_profile / _fetch_unipile_user_profile) —
no real Unipile HTTP calls are made. replan_channels_on_sender_add is mocked
so no campaign side effects run.
"""
import pytest

import database
import routes.linkedin_accounts as linkedin_accounts_module
from config import get_settings

pytestmark = pytest.mark.api

NOTIFY_URL = "/api/linkedin-accounts/connect/notify"
WEBHOOK_URL = "/api/linkedin-accounts/connect/webhook"


async def _fake_fetch_profile(unipile_account_id, settings):
    return {
        "account": {
            "connection_params": {"im": {"publicIdentifier": "johndoe", "id": "prov-123"}},
            "name": "John Doe",
            "sources": [{"status": "OK"}],
        }
    }


async def _fake_fetch_user_profile(unipile_account_id, public_id, settings):
    return {
        "first_name": "John",
        "last_name": "Doe",
        "headline": "Head of Sales",
        "follower_count": 10,
        "connections_count": 5,
    }


@pytest.fixture
def mock_unipile_fetch(monkeypatch):
    monkeypatch.setattr(linkedin_accounts_module, "_fetch_unipile_profile", _fake_fetch_profile)
    monkeypatch.setattr(linkedin_accounts_module, "_fetch_unipile_user_profile", _fake_fetch_user_profile)


@pytest.fixture
def mock_replan(monkeypatch):
    from services import campaign_launch_service

    calls = []

    async def _fake(account_id, sender_type):
        calls.append((account_id, sender_type))

    monkeypatch.setattr(campaign_launch_service, "replan_channels_on_sender_add", _fake)
    return calls


@pytest.fixture(autouse=True)
async def _clean_linkedin_accounts():
    """Each test starts with an empty collection (unique index on unipile_account_id
    would otherwise leak state across tests within this module)."""
    await database.linkedin_accounts_collection.delete_many({})
    await database.linkedin_auth_requests_collection.delete_many({})
    yield
    await database.linkedin_accounts_collection.delete_many({})
    await database.linkedin_auth_requests_collection.delete_many({})


async def test_notify_creates_account_and_fires_replan(
    client, identity_a, mock_unipile_fetch, mock_replan
):
    unipile_id = "unipile-acc-1"
    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": unipile_id,
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "ok"}

    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": unipile_id})
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]
    assert doc["user_id"] == identity_a["user_id"]
    assert doc["public_id"] == "johndoe"
    assert doc["name"] == "John Doe"

    assert mock_replan == [(identity_a["account_id"], "linkedin")]


async def test_notify_idempotent_on_replay(client, identity_a, mock_unipile_fetch, mock_replan):
    unipile_id = "unipile-acc-replay"
    payload = {
        "status": "CREATION_SUCCESS",
        "account_id": unipile_id,
        "name": f"{identity_a['account_id']}:{identity_a['user_id']}",
    }
    first = await client.post(NOTIFY_URL, json=payload)
    second = await client.post(NOTIFY_URL, json=payload)
    assert first.status_code == 200
    assert second.status_code == 200

    docs = await database.linkedin_accounts_collection.find(
        {"unipile_account_id": unipile_id}
    ).to_list(length=10)
    assert len(docs) == 1
    # replan fires only on the first (creating) delivery, not the replay
    assert mock_replan == [(identity_a["account_id"], "linkedin")]


async def test_notify_ignores_non_success_status(client, identity_a, mock_unipile_fetch, mock_replan):
    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_FAILED",
            "account_id": "unipile-acc-failed",
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": "unipile-acc-failed"})
    assert doc is None
    assert mock_replan == []


async def test_notify_handles_legacy_name_without_colon(client, identity_a, mock_unipile_fetch, mock_replan):
    """Defensive fallback: a name with no ':' is treated entirely as account_id
    (matches the pre-fix hosted-auth payload shape)."""
    unipile_id = "unipile-acc-legacy"
    resp = await client.post(
        NOTIFY_URL,
        json={"status": "CREATION_SUCCESS", "account_id": unipile_id, "name": identity_a["account_id"]},
    )
    assert resp.status_code == 200
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": unipile_id})
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]
    assert doc["user_id"] == ""


async def test_notify_rejects_bad_signature_when_secret_configured(
    client, identity_a, mock_unipile_fetch, mock_replan, monkeypatch
):
    monkeypatch.setattr(get_settings(), "unipile_webhook_secret", "top-secret")
    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": "unipile-acc-badsig",
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}",
        },
        headers={"X-Webhook-Secret": "wrong"},
    )
    assert resp.status_code == 403
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": "unipile-acc-badsig"})
    assert doc is None
    assert mock_replan == []


async def test_notify_accepts_correct_signature(
    client, identity_a, mock_unipile_fetch, mock_replan, monkeypatch
):
    monkeypatch.setattr(get_settings(), "unipile_webhook_secret", "top-secret")
    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": "unipile-acc-goodsig",
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}",
        },
        headers={"X-Webhook-Secret": "top-secret"},
    )
    assert resp.status_code == 200
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": "unipile-acc-goodsig"})
    assert doc is not None


async def test_connect_webhook_authenticated_fallback_still_works(
    client, identity_a, auth_headers_a, mock_unipile_fetch, mock_replan
):
    resp = await client.post(
        WEBHOOK_URL,
        json={"unipile_account_id": "unipile-acc-auth-fallback", "status": "OK"},
        headers=auth_headers_a,
    )
    assert resp.status_code == 200, resp.text
    doc = await database.linkedin_accounts_collection.find_one(
        {"unipile_account_id": "unipile-acc-auth-fallback"}
    )
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]
    assert doc["user_id"] == identity_a["user_id"]
    assert mock_replan == [(identity_a["account_id"], "linkedin")]


async def test_connect_webhook_requires_auth(client):
    resp = await client.post(
        WEBHOOK_URL, json={"unipile_account_id": "unipile-acc-noauth", "status": "OK"}
    )
    assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# Unsigned hosted-auth callbacks (the real Unipile behaviour)
#
# Unipile's hosted-auth link has no field for a shared secret or signing key on
# notify_url, so the callback it sends is unsigned. Production sets
# UNIPILE_WEBHOOK_SECRET (config.py requires it), which used to make every one of
# these callbacks 403 — the account linked at Unipile and never reached the
# dashboard. These tests pin the nonce handshake that replaced the signature check.
# ---------------------------------------------------------------------------


@pytest.fixture
def prod_secret(monkeypatch):
    """Production-shaped config: a webhook secret is configured, so the old code
    path rejected every unsigned notify."""
    monkeypatch.setattr(get_settings(), "unipile_webhook_secret", "top-secret")


async def _issue_nonce(identity) -> str:
    nonce, _ = await linkedin_accounts_module._issue_hosted_auth_nonce(
        identity["account_id"], identity["user_id"]
    )
    return nonce


async def test_notify_unsigned_with_nonce_creates_account(
    client, identity_a, prod_secret, mock_unipile_fetch, mock_replan
):
    """Regression: an unsigned callback bearing a nonce we issued must link the
    account even though a webhook secret is configured."""
    nonce = await _issue_nonce(identity_a)
    unipile_id = "unipile-acc-nonce"

    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": unipile_id,
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}:{nonce}",
        },
    )

    assert resp.status_code == 200, resp.text
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": unipile_id})
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]
    assert doc["user_id"] == identity_a["user_id"]
    assert mock_replan == [(identity_a["account_id"], "linkedin")]


async def test_notify_unsigned_without_nonce_is_rejected(
    client, identity_a, prod_secret, mock_unipile_fetch, mock_replan
):
    """An unsigned callback naming no pending request is still refused, so the
    endpoint stays closed to forged bodies."""
    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": "unipile-acc-no-nonce",
            "name": f"{identity_a['account_id']}:{identity_a['user_id']}:not-a-real-nonce",
        },
    )

    assert resp.status_code == 403
    doc = await database.linkedin_accounts_collection.find_one(
        {"unipile_account_id": "unipile-acc-no-nonce"}
    )
    assert doc is None
    assert mock_replan == []


async def test_notify_nonce_is_single_use_and_replay_is_acked(
    client, identity_a, prod_secret, mock_unipile_fetch, mock_replan
):
    """Unipile retries a delivery it couldn't confirm. The nonce is spent, but the
    account is already linked, so the retry is ack'd rather than 403'd — and it
    creates no duplicate and re-fires no replan."""
    nonce = await _issue_nonce(identity_a)
    unipile_id = "unipile-acc-replay-nonce"
    payload = {
        "status": "CREATION_SUCCESS",
        "account_id": unipile_id,
        "name": f"{identity_a['account_id']}:{identity_a['user_id']}:{nonce}",
    }

    first = await client.post(NOTIFY_URL, json=payload)
    second = await client.post(NOTIFY_URL, json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "ignored"

    docs = await database.linkedin_accounts_collection.find(
        {"unipile_account_id": unipile_id}
    ).to_list(length=10)
    assert len(docs) == 1
    assert mock_replan == [(identity_a["account_id"], "linkedin")]


async def test_notify_tenant_comes_from_nonce_not_body(
    client, identity_a, identity_b, prod_secret, mock_unipile_fetch, mock_replan
):
    """The body only proves possession of the nonce; the tenant is read from the
    pending request, so a body claiming another tenant cannot steal the account."""
    nonce = await _issue_nonce(identity_a)
    unipile_id = "unipile-acc-tenant-bind"

    resp = await client.post(
        NOTIFY_URL,
        json={
            "status": "CREATION_SUCCESS",
            "account_id": unipile_id,
            "name": f"{identity_b['account_id']}:{identity_b['user_id']}:{nonce}",
        },
    )

    assert resp.status_code == 200, resp.text
    doc = await database.linkedin_accounts_collection.find_one({"unipile_account_id": unipile_id})
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]
    assert doc["user_id"] == identity_a["user_id"]


async def test_notify_failure_status_does_not_burn_the_nonce(
    client, identity_a, prod_secret, mock_unipile_fetch, mock_replan
):
    """A failed LinkedIn login must leave the pending request usable, so the user's
    retry inside the same hosted-auth link still connects."""
    nonce = await _issue_nonce(identity_a)
    name = f"{identity_a['account_id']}:{identity_a['user_id']}:{nonce}"

    failed = await client.post(
        NOTIFY_URL,
        json={"status": "CREATION_FAILED", "account_id": "unipile-acc-fail-then-ok", "name": name},
    )
    assert failed.status_code == 200
    assert failed.json()["status"] == "ignored"

    retried = await client.post(
        NOTIFY_URL,
        json={"status": "CREATION_SUCCESS", "account_id": "unipile-acc-fail-then-ok", "name": name},
    )
    assert retried.status_code == 200, retried.text
    doc = await database.linkedin_accounts_collection.find_one(
        {"unipile_account_id": "unipile-acc-fail-then-ok"}
    )
    assert doc is not None
    assert doc["account_id"] == identity_a["account_id"]


async def test_expired_nonce_is_not_accepted(identity_a):
    """Mongo's TTL reaper only runs about once a minute, so expiry is enforced in
    code as well as by the index."""
    from datetime import datetime, timedelta, timezone

    nonce = await _issue_nonce(identity_a)
    await database.linkedin_auth_requests_collection.update_one(
        {"nonce": nonce},
        {"$set": {"expires_at": datetime.now(timezone.utc) - timedelta(minutes=1)}},
    )

    assert await linkedin_accounts_module._consume_hosted_auth_nonce(nonce) is None


async def test_hosted_auth_link_carries_a_persisted_nonce(
    client, identity_a, auth_headers_a, monkeypatch
):
    """initiate_hosted_auth must mint a pending request and put its nonce in the
    `name` Unipile echoes back — without it the callback has nothing to prove."""
    sent = {}

    class _FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"url": "https://account.unipile.com/hosted/xyz"}

    class _FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, json=None, headers=None):
            sent["url"] = url
            sent["payload"] = json
            return _FakeResponse()

    monkeypatch.setattr(linkedin_accounts_module.httpx, "AsyncClient", _FakeClient)

    resp = await client.post(
        "/api/linkedin-accounts/connect/hosted-auth", headers=auth_headers_a
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["auth_url"] == "https://account.unipile.com/hosted/xyz"

    account_id, _, rest = sent["payload"]["name"].partition(":")
    user_id, _, nonce = rest.partition(":")
    assert account_id == identity_a["account_id"]
    assert user_id == identity_a["user_id"]
    assert nonce

    pending = await database.linkedin_auth_requests_collection.find_one({"nonce": nonce})
    assert pending is not None
    assert pending["account_id"] == identity_a["account_id"]
