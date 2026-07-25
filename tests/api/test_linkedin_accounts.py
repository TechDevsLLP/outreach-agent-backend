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
    yield
    await database.linkedin_accounts_collection.delete_many({})


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
