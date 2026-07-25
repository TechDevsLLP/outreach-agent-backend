"""Focused regressions for launch-critical provider security boundaries.

All provider HTTP and Mongo operations are replaced with local fakes.
"""

from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import BackgroundTasks, HTTPException

import routes.email_accounts as email_routes
import routes.linkedin_outreach as linkedin_routes
import routes.onboarding_wizard as onboarding_routes
import routes.webhooks as webhook_routes
from services.email_providers import zoho
from services.unipile_service import UnipileClient
from utils import crypto

pytestmark = pytest.mark.unit


class _UpdateResult:
    def __init__(self, matched_count: int):
        self.matched_count = matched_count


class _NonceCollection:
    def __init__(self):
        self.docs: dict[str, dict] = {}

    async def insert_one(self, doc: dict):
        assert doc["_id"] not in self.docs
        self.docs[doc["_id"]] = dict(doc)

    async def update_one(self, query: dict, update: dict):
        doc = self.docs.get(query["_id"])
        if not doc:
            return _UpdateResult(0)
        matches = (
            doc["account_id"] == query["account_id"]
            and doc["provider"] == query["provider"]
            and doc["redirect_uri"] == query["redirect_uri"]
            and doc["consumed_at"] is query["consumed_at"]
            and doc["expires_at"] > query["expires_at"]["$gt"]
        )
        if not matches:
            return _UpdateResult(0)
        doc.update(update["$set"])
        return _UpdateResult(1)


@pytest.mark.parametrize(
    ("accounts_origin", "api_origin"),
    [
        ("https://accounts.zoho.com", "https://mail.zoho.com"),
        ("https://accounts.zoho.eu/", "https://mail.zoho.eu"),
        ("https://accounts.zohocloud.ca", "https://mail.zohocloud.ca"),
        ("https://accounts.zoho.sa", "https://mail.zoho.sa"),
    ],
)
def test_zoho_origins_are_exactly_mapped(accounts_origin, api_origin):
    assert zoho.resolve_zoho_domains(accounts_origin) == (
        accounts_origin.rstrip("/"),
        api_origin,
    )


@pytest.mark.parametrize(
    "malicious_origin",
    [
        "http://accounts.zoho.com",
        "https://accounts.zoho.com.evil.test",
        "https://accounts.zoho.com@evil.test",
        "https://accounts.zoho.com/oauth/v2/token",
        "https://169.254.169.254/latest/meta-data",
        "https://accounts.zoho.com?next=https://evil.test",
    ],
)
def test_zoho_origin_allowlist_rejects_ssrf_inputs(malicious_origin):
    with pytest.raises(ValueError, match="Unsupported Zoho accounts server"):
        zoho.resolve_zoho_domains(malicious_origin)


async def test_zoho_refresh_rejects_legacy_malicious_origin_before_http(monkeypatch):
    class _NetworkMustNotRun:
        def AsyncClient(self, *args, **kwargs):  # pragma: no cover - assertion path
            raise AssertionError("provider HTTP must not run for an untrusted origin")

    monkeypatch.setattr(zoho, "httpx", _NetworkMustNotRun())
    result = await zoho.refresh_zoho_token_if_needed(
        {
            "_id": "email-account-1",
            "oauth_refresh_token": "refresh-token",
            "zoho_accounts_domain": "https://attacker.example",
        }
    )
    assert result is None


async def test_zoho_exchange_rejects_ssrf_before_provider_http(monkeypatch):
    async def _accept_state(*args, **kwargs):
        return {}

    class _NetworkMustNotRun:
        def AsyncClient(self, *args, **kwargs):  # pragma: no cover - assertion path
            raise AssertionError("provider HTTP must not run for an untrusted origin")

    monkeypatch.setattr(email_routes, "_consume_oauth_state", _accept_state)
    monkeypatch.setattr(email_routes, "httpx", _NetworkMustNotRun())

    with pytest.raises(HTTPException) as exc:
        await email_routes.zoho_oauth_exchange(
            body=email_routes.ZohoExchangeRequest(
                code="code",
                redirect_uri="https://app.example/api/auth/zoho/callback",
                state="state",
                accounts_server="https://attacker.example",
            ),
            background_tasks=BackgroundTasks(),
            account_ctx={"account": {"_id": "tenant-a"}, "user": {"_id": "user-a"}},
        )
    assert exc.value.status_code == 400


async def test_oauth_state_is_bound_and_one_time(monkeypatch):
    nonces = _NonceCollection()
    monkeypatch.setattr(email_routes.database, "oauth_state_nonces_collection", nonces)
    redirect_uri = "https://app.example/api/auth/google/callback"

    token = await email_routes._issue_oauth_state(
        account_id="tenant-a",
        provider="google",
        redirect_uri=redirect_uri,
        return_to="/home",
    )
    payload = await email_routes._consume_oauth_state(
        token,
        account_id="tenant-a",
        provider="google",
        redirect_uri=redirect_uri,
    )
    assert payload["return_to"] == "/home"
    assert nonces.docs[payload["jti"]]["consumed_at"] is not None

    with pytest.raises(HTTPException) as replay:
        await email_routes._consume_oauth_state(
            token,
            account_id="tenant-a",
            provider="google",
            redirect_uri=redirect_uri,
        )
    assert replay.value.status_code == 400


async def test_oauth_state_rejects_cross_tenant_binding(monkeypatch):
    nonces = _NonceCollection()
    monkeypatch.setattr(email_routes.database, "oauth_state_nonces_collection", nonces)
    redirect_uri = "https://app.example/api/auth/zoho/callback"
    token = await email_routes._issue_oauth_state(
        account_id="tenant-a",
        provider="zoho",
        redirect_uri=redirect_uri,
        return_to=None,
    )

    with pytest.raises(HTTPException):
        await email_routes._consume_oauth_state(
            token,
            account_id="tenant-b",
            provider="zoho",
            redirect_uri=redirect_uri,
        )
    assert all(doc["consumed_at"] is None for doc in nonces.docs.values())


@pytest.mark.parametrize(
    "return_to",
    ["https://evil.example/callback", "//evil.example/callback", "javascript:alert(1)"],
)
def test_oauth_state_rejects_external_return_destination(return_to):
    with pytest.raises(HTTPException):
        email_routes._validated_return_to(return_to)


async def test_onboarding_uses_shared_signed_state_contract(monkeypatch):
    calls = []

    async def _issue(**kwargs):
        calls.append(kwargs)
        return "signed-one-time-state"

    monkeypatch.setattr(email_routes, "_issue_oauth_state", _issue)
    response = await onboarding_routes.stage5_oauth_init(
        {"provider": "google"},
        account_ctx={"account": {"_id": "tenant-a"}},
    )
    state = parse_qs(urlsplit(response["auth_url"]).query)["state"]
    assert state == ["signed-one-time-state"]
    assert calls == [
        {
            "account_id": "tenant-a",
            "provider": "google",
            "redirect_uri": onboarding_routes.settings.google_redirect_uri,
            "return_to": "/home",
        }
    ]


async def test_linkedin_send_chat_carries_bound_provider_account(monkeypatch):
    calls = []
    client = UnipileClient(account_id="unipile-tenant-a")

    async def _request(method, endpoint, **kwargs):
        calls.append((method, endpoint, kwargs))
        return {"message_id": "message-1"}

    monkeypatch.setattr(client, "_request", _request)
    await client.send_message("chat-1", "hello")

    assert calls == [
        (
            "POST",
            "chats/chat-1/messages",
            {"json": {"text": "hello", "account_id": "unipile-tenant-a"}},
        )
    ]


async def test_linkedin_chat_read_discards_other_account_messages(monkeypatch):
    client = UnipileClient(account_id="unipile-tenant-a")

    async def _request(method, endpoint, **kwargs):
        return {
            "items": [
                {"id": "owned", "account_id": "unipile-tenant-a"},
                {"id": "foreign", "account_id": "unipile-tenant-b"},
                {"id": "unbound"},
            ]
        }

    monkeypatch.setattr(client, "_request", _request)
    result = await client.get_chat_messages("chat-1")
    assert [message["id"] for message in result["items"]] == ["owned"]


async def test_manual_linkedin_client_resolves_only_authenticated_tenant(monkeypatch):
    seen = {}

    class _Collection:
        async def find_one(self, query, sort=None):
            seen["query"] = query
            seen["sort"] = sort
            return {"unipile_account_id": "unipile-tenant-a"}

    monkeypatch.setattr(linkedin_routes.database, "linkedin_accounts_collection", _Collection())
    client = await linkedin_routes._tenant_unipile_client(
        {"account": {"_id": "tenant-a"}}
    )
    assert client._account_id == "unipile-tenant-a"
    assert seen["query"]["account_id"] == "tenant-a"


def test_unsigned_webhook_fails_closed_outside_dev_and_test(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "app_env", "production")
    assert not webhook_routes._verify_unipile_signature("", b"{}", "", "")


def test_unsigned_webhook_is_explicitly_allowed_in_test(monkeypatch):
    monkeypatch.setattr(webhook_routes.settings, "app_env", "test")
    assert webhook_routes._verify_unipile_signature("", b"{}", "", "")


def test_production_encryption_refuses_missing_key(monkeypatch):
    monkeypatch.setattr(crypto, "_get_fernet", lambda: None)
    monkeypatch.setattr(crypto, "_is_production", lambda: True)
    with pytest.raises(crypto.CredentialEncryptionError):
        crypto.encrypt("secret")
    with pytest.raises(crypto.CredentialEncryptionError):
        crypto.decrypt("legacy-plaintext-secret")


async def test_microsoft_connection_and_exchange_are_disabled():
    with pytest.raises(HTTPException) as url_error:
        await email_routes.microsoft_oauth_url(
            redirect_uri=None,
            account_ctx={"account": {"_id": "tenant-a"}},
        )
    assert url_error.value.status_code == 410

    with pytest.raises(HTTPException) as exchange_error:
        await email_routes.microsoft_oauth_exchange(
            body=email_routes.MicrosoftExchangeRequest(
                code="code", redirect_uri="https://app.example/callback"
            ),
            background_tasks=BackgroundTasks(),
            account_ctx={"account": {"_id": "tenant-a"}, "user": {"_id": "user-a"}},
        )
    assert exchange_error.value.status_code == 410


async def test_microsoft_refresh_is_disabled_before_token_or_network(monkeypatch):
    async def _legacy_account(*args, **kwargs):
        return {
            "_id": "legacy-microsoft-account",
            "provider": "microsoft",
            "oauth_refresh_token": "legacy-token",
        }

    monkeypatch.setattr(email_routes, "_get_email_account_or_404", _legacy_account)
    with pytest.raises(HTTPException) as refresh_error:
        await email_routes.refresh_oauth_token(
            "legacy-microsoft-account",
            account_ctx={"account": {"_id": "tenant-a"}},
        )
    assert refresh_error.value.status_code == 410
