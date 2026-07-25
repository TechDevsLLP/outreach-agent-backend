"""
Tests for the Gmail API / Zoho Mail API / custom SMTP+IMAP email providers.

External HTTP (httpx.AsyncClient) is faked at the module level so these run
offline. aiosmtplib.send and the blocking imaplib helpers are monkeypatched
directly rather than faking a real SMTP/IMAP server.
"""
import base64
import email as email_lib
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Shared fake httpx.AsyncClient
# ---------------------------------------------------------------------------

class _FakeHttpResponse:
    def __init__(self, status_code, json_body=None, text_body=""):
        self.status_code = status_code
        self._json_body = json_body
        self.text = text_body or (str(json_body) if json_body is not None else "")
        self.headers = {}

    def json(self):
        return self._json_body


def _make_fake_async_client_class(queue, calls):
    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, headers=None, json=None, params=None):
            calls.append({"method": "POST", "url": url, "headers": headers, "json": json})
            if not queue:
                raise AssertionError("fake httpx client: no queued response left")
            return queue.pop(0)

        async def get(self, url, headers=None, params=None):
            calls.append({"method": "GET", "url": url, "headers": headers, "params": params})
            if not queue:
                raise AssertionError("fake httpx client: no queued response left")
            return queue.pop(0)

    return _FakeAsyncClient


def _fresh_token_account(**overrides) -> dict:
    from bson import ObjectId
    doc = {
        "_id": ObjectId(),
        "email": "sender@example.com",
        "oauth_access_token": "valid-access-token",
        "oauth_refresh_token": "valid-refresh-token",
        "oauth_token_expiry": datetime.now(timezone.utc) + timedelta(hours=1),
    }
    doc.update(overrides)
    return doc


def _decode_gmail_raw(payload_json: dict) -> email_lib.message.Message:
    raw = payload_json["raw"]
    padded = raw + "=" * (-len(raw) % 4)
    mime_bytes = base64.urlsafe_b64decode(padded)
    return email_lib.message_from_bytes(mime_bytes)


# ---------------------------------------------------------------------------
# Gmail API
# ---------------------------------------------------------------------------

class TestGmailService:
    async def test_send_gmail_email_new_thread_has_no_reply_headers(self, monkeypatch):
        from services import gmail_service

        calls = []
        queue = [_FakeHttpResponse(200, {"id": "msg1", "threadId": "thread1"})]
        monkeypatch.setattr(gmail_service, "httpx", _StubHttpxModule(_make_fake_async_client_class(queue, calls)))

        account = _fresh_token_account()
        result = await gmail_service.send_gmail_email(account, "to@example.com", "Hi", "<p>hello</p>")

        assert result == {"message_id": "msg1", "thread_id": "thread1", "rfc_message_id": result["rfc_message_id"]}
        sent_mime = _decode_gmail_raw(calls[0]["json"])
        assert sent_mime["In-Reply-To"] is None
        assert "threadId" not in calls[0]["json"]

    async def test_send_gmail_email_reply_sets_threading_headers(self, monkeypatch):
        from services import gmail_service

        calls = []
        queue = [_FakeHttpResponse(200, {"id": "msg2", "threadId": "thread1"})]
        monkeypatch.setattr(gmail_service, "httpx", _StubHttpxModule(_make_fake_async_client_class(queue, calls)))

        account = _fresh_token_account()
        result = await gmail_service.send_gmail_email(
            account, "to@example.com", "Re: Hi", "<p>reply</p>",
            in_reply_to="<orig@outflo.app>", thread_id="thread1",
        )

        assert result["message_id"] == "msg2"
        assert calls[0]["json"]["threadId"] == "thread1"
        sent_mime = _decode_gmail_raw(calls[0]["json"])
        assert sent_mime["In-Reply-To"] == "<orig@outflo.app>"
        assert sent_mime["References"] == "<orig@outflo.app>"

    async def test_create_gmail_draft_posts_to_drafts_endpoint(self, monkeypatch):
        from services import gmail_service

        calls = []
        queue = [_FakeHttpResponse(200, {"id": "draft1", "message": {"id": "msg3", "threadId": "thread1"}})]
        monkeypatch.setattr(gmail_service, "httpx", _StubHttpxModule(_make_fake_async_client_class(queue, calls)))

        account = _fresh_token_account()
        result = await gmail_service.create_gmail_draft(account, "to@example.com", "Subj", "<p>draft</p>")

        assert result == {"draft_id": "draft1", "message_id": "msg3", "thread_id": "thread1"}
        assert calls[0]["url"].endswith("/drafts")

    async def test_refresh_token_skips_http_when_not_expired(self, monkeypatch):
        from services import gmail_service

        called = {"count": 0}

        class _ExplodingClient:
            def __init__(self, *a, **kw):
                called["count"] += 1

        monkeypatch.setattr(gmail_service, "httpx", _StubHttpxModule(_ExplodingClient))
        account = _fresh_token_account()
        token = await gmail_service.refresh_token_if_needed(account)
        assert token == "valid-access-token"
        assert called["count"] == 0


class _StubHttpxModule:
    """Minimal stand-in for the `httpx` module exposing only AsyncClient."""
    def __init__(self, async_client_cls):
        self.AsyncClient = async_client_cls


# ---------------------------------------------------------------------------
# Zoho Mail API
# ---------------------------------------------------------------------------

class TestZohoProvider:
    def _account(self, **overrides):
        doc = _fresh_token_account(
            zoho_account_id="zacct1",
            zoho_api_domain="https://mail.zoho.com",
            zoho_from_address="sender@example.com",
        )
        doc.update(overrides)
        return doc

    async def test_send_new_email_builds_correct_payload(self, monkeypatch):
        from services.email_providers import zoho

        calls = []
        fake_resp = _FakeHttpResponse(200, {"data": {"messageId": "z1", "threadId": "t1"}})
        monkeypatch.setattr(zoho, "httpx", _StubHttpxModule(_make_fake_async_client_class([fake_resp], calls)))

        account = self._account()
        result = await zoho.send_zoho_email(account, "to@example.com", "Subject", "<p>hi</p>")

        assert result == {"message_id": "z1", "thread_id": "t1"}
        sent = calls[0]
        assert sent["url"] == "https://mail.zoho.com/api/accounts/zacct1/messages"
        assert sent["headers"]["Authorization"] == "Zoho-oauthtoken valid-access-token"
        assert sent["json"]["fromAddress"] == "sender@example.com"
        assert sent["json"]["toAddress"] == "to@example.com"
        assert sent["json"]["mailFormat"] == "html"
        assert "mode" not in sent["json"]
        assert "action" not in sent["json"]

    async def test_send_reply_hits_message_id_endpoint_with_action_reply(self, monkeypatch):
        from services.email_providers import zoho

        calls = []
        fake_resp = _FakeHttpResponse(200, {"data": {"messageId": "z2", "threadId": "t1"}})
        monkeypatch.setattr(zoho, "httpx", _StubHttpxModule(_make_fake_async_client_class([fake_resp], calls)))

        account = self._account()
        result = await zoho.send_zoho_email(
            account, "to@example.com", "Re: Subject", "<p>reply</p>", reply_to_message_id="orig123"
        )

        assert result["message_id"] == "z2"
        assert calls[0]["url"].endswith("/messages/orig123")
        assert calls[0]["json"]["action"] == "reply"

    async def test_send_draft_sets_mode_draft(self, monkeypatch):
        from services.email_providers import zoho

        calls = []
        fake_resp = _FakeHttpResponse(200, {"data": {"messageId": "z3"}})
        monkeypatch.setattr(zoho, "httpx", _StubHttpxModule(_make_fake_async_client_class([fake_resp], calls)))

        account = self._account()
        await zoho.send_zoho_email(account, "to@example.com", "Draft subj", "body", mode="draft")

        assert calls[0]["json"]["mode"] == "draft"

    @pytest.mark.parametrize("accounts_domain,expected", [
        ("https://accounts.zoho.com", "https://mail.zoho.com"),
        ("https://accounts.zoho.eu", "https://mail.zoho.eu"),
        ("https://accounts.zoho.in", "https://mail.zoho.in"),
        ("https://accounts.zoho.com.au", "https://mail.zoho.com.au"),
        (None, "https://mail.zoho.com"),
    ])
    def test_derive_api_domain(self, accounts_domain, expected):
        from services.email_providers.zoho import derive_api_domain
        assert derive_api_domain(accounts_domain) == expected

    async def test_fetch_new_replies_filters_out_own_sent_messages(self, monkeypatch):
        from services.email_providers.zoho import ZohoProvider

        async def _fake_list_zoho_messages(account, *, thread_id=None, status="all", limit=20):
            return [
                {"messageId": "m1", "fromAddress": "sender@example.com", "subject": "orig"},
                {"messageId": "m2", "fromAddress": "prospect@acme.com", "subject": "Re: orig",
                 "summary": "sounds good", "sentDateInGMT": "123"},
            ]

        import services.email_providers.zoho as zoho_module
        monkeypatch.setattr(zoho_module, "list_zoho_messages", _fake_list_zoho_messages)

        provider = ZohoProvider(self._account())
        replies = await provider.fetch_new_replies("t1", "sender@example.com")

        assert len(replies) == 1
        assert replies[0].from_email == "prospect@acme.com"
        assert replies[0].provider_message_id == "m2"


# ---------------------------------------------------------------------------
# Custom SMTP + IMAP
# ---------------------------------------------------------------------------

class TestSmtpImapProvider:
    def _account(self, **overrides):
        from bson import ObjectId
        doc = {
            "_id": ObjectId(),
            "email": "sender@example.com",
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "smtp_username": "sender@example.com",
            "smtp_password": "smtp-pass",
            "smtp_encryption": "tls",
            "imap_host": "imap.example.com",
            "imap_port": 993,
            "imap_username": "sender@example.com",
            "imap_password": "imap-pass",
            "imap_encryption": "ssl",
        }
        doc.update(overrides)
        return doc

    async def test_send_smtp_email_calls_aiosmtplib_with_expected_args(self, monkeypatch):
        import aiosmtplib
        from services.email_providers import smtp_imap

        captured = {}

        async def _fake_send(msg, **kwargs):
            captured["msg"] = msg
            captured["kwargs"] = kwargs

        monkeypatch.setattr(aiosmtplib, "send", _fake_send)

        account = self._account()
        result = await smtp_imap.send_smtp_email(account, "to@example.com", "Subject", "<p>hi</p>")

        assert result["message_id"].startswith("<")
        assert captured["kwargs"]["hostname"] == "smtp.example.com"
        assert captured["kwargs"]["port"] == 587
        assert captured["kwargs"]["username"] == "sender@example.com"
        assert captured["kwargs"]["password"] == "smtp-pass"
        assert captured["kwargs"]["start_tls"] is True
        assert captured["msg"]["To"] == "to@example.com"

    async def test_send_smtp_email_reply_sets_threading_headers(self, monkeypatch):
        import aiosmtplib
        from services.email_providers import smtp_imap

        captured = {}

        async def _fake_send(msg, **kwargs):
            captured["msg"] = msg

        monkeypatch.setattr(aiosmtplib, "send", _fake_send)

        account = self._account()
        await smtp_imap.send_smtp_email(
            account, "to@example.com", "Re: Subject", "<p>reply</p>",
            in_reply_to="<root@outflo.app>", references="<root@outflo.app>",
        )

        assert captured["msg"]["In-Reply-To"] == "<root@outflo.app>"
        assert captured["msg"]["References"] == "<root@outflo.app>"

    async def test_create_draft_invokes_imap_append_via_to_thread(self, monkeypatch):
        from services.email_providers import smtp_imap

        calls = {}

        def _fake_sync_create_draft(account, msg):
            calls["account"] = account
            calls["subject"] = msg["Subject"]
            return True

        monkeypatch.setattr(smtp_imap, "_sync_create_draft", _fake_sync_create_draft)

        provider = smtp_imap.SmtpImapProvider(self._account())
        result = await provider.create_draft("to@example.com", "Draft subj", "<p>draft</p>")

        assert result is not None
        assert result.provider == "smtp"
        assert calls["subject"] == "Draft subj"

    async def test_fetch_new_replies_returns_empty_list_without_thread_ref(self):
        from services.email_providers import smtp_imap

        provider = smtp_imap.SmtpImapProvider(self._account())
        replies = await provider.fetch_new_replies("", "sender@example.com")
        assert replies == []

    def test_find_draft_folder_picks_folder_with_draft_in_name(self):
        from services.email_providers.smtp_imap import _find_draft_folder

        class _FakeImap:
            def list(self):
                return "OK", [
                    b'(\\HasNoChildren) "/" "INBOX"',
                    b'(\\HasNoChildren \\Drafts) "/" "INBOX.Drafts"',
                ]

        assert _find_draft_folder(_FakeImap()) == "INBOX.Drafts"
