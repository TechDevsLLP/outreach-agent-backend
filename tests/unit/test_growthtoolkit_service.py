"""Unit tests for services/growthtoolkit_service.py with mocked HTTP.

The fixture mock_growthtoolkit_http (conftest) replaces httpx.AsyncClient
inside the module and zeroes rate-limiter/poll delays. Usage tracking is
pointed at the test DB (outflo_v3_test) via the session-wide env override.
"""
import pytest

from services import growthtoolkit_service as gts

pytestmark = pytest.mark.unit


OK_EMAIL_BODY = {"success": True, "code": "200", "data": {"email": "jane@acme.com"}}
NOT_FOUND_BODY = {"success": False, "code": "resource_not_found", "message": "no result"}
CREDITS_BODY = {"success": False, "code": "402", "message": "credits exhausted"}
RATE_LIMIT_BODY = {"success": False, "code": "429", "error_data": {"sec": 0.01}}
AUTH_BODY = {"success": False, "code": "401", "message": "bad key"}
INVALID_BODY = {"success": False, "code": "417", "message": "bad domain"}


async def test_find_email_success(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, OK_EMAIL_BODY)]
    email = await gts.find_email("Jane", "Doe", "acme.com", account_id="acct1")
    assert email == "jane@acme.com"
    call = mock_growthtoolkit_http.calls[0]
    assert call["params"] == {"first_name": "Jane", "last_name": "Doe", "domain": "acme.com"}
    assert "/email-finder/" in call["url"]


async def test_find_email_not_found_returns_none(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, NOT_FOUND_BODY)]
    assert await gts.find_email("No", "Body", "ghost.com") is None


async def test_find_email_credits_exhausted_raises(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, CREDITS_BODY)]
    with pytest.raises(gts.CreditsExhausted):
        await gts.find_email("Jane", "Doe", "acme.com")


async def test_429_retry_then_success(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [
        (200, RATE_LIMIT_BODY),
        (200, RATE_LIMIT_BODY),
        (200, OK_EMAIL_BODY),
    ]
    email = await gts.find_email("Jane", "Doe", "acme.com")
    assert email == "jane@acme.com"
    assert len(mock_growthtoolkit_http.calls) == 3


async def test_429_retries_exhausted_raises(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, RATE_LIMIT_BODY)] * 4  # 1 + 3 retries
    with pytest.raises(gts.GrowthToolkitError) as exc:
        await gts.find_email("Jane", "Doe", "acme.com")
    assert "retries exhausted" in str(exc.value)


async def test_auth_error_raises(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, AUTH_BODY)]
    with pytest.raises(gts.AuthError):
        await gts.find_email("Jane", "Doe", "acme.com")


async def test_invalid_input_raises(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, INVALID_BODY)]
    with pytest.raises(gts.InvalidInput):
        await gts.enrich_linkedin("not-a-url")


async def test_missing_api_key_raises_auth_error(mock_growthtoolkit_http, monkeypatch):
    monkeypatch.setattr(gts.settings, "growthtoolkit_api_key", "")
    with pytest.raises(gts.AuthError):
        await gts.find_email("Jane", "Doe", "acme.com")
    assert mock_growthtoolkit_http.calls == []  # no HTTP call made


async def test_5xx_retry_then_success(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [
        (502, {}),
        (200, OK_EMAIL_BODY),
    ]
    assert await gts.find_email("Jane", "Doe", "acme.com") == "jane@acme.com"


async def test_enrich_linkedin_task_polling(mock_growthtoolkit_http):
    """Action response with task_id must transparently poll /tasks/status/."""
    person = {"full_name": "Jane Doe", "unlock_details": {"phone_numbers": ["+1-555"]}}

    def handler(method, url, params, json_body):
        if "/enrichment/linkedin/" in url:
            return (200, {"success": True, "code": "200", "data": {"task_id": "t-123"}})
        if "/tasks/status/t-123/" in url:
            return (200, {"success": True, "code": "200",
                          "data": {"status": "finished", "result": person}})
        raise AssertionError(f"unexpected url {url}")

    mock_growthtoolkit_http.handler = handler
    result = await gts.enrich_linkedin("https://linkedin.com/in/jane", unlock_phone=True)
    assert result == person


async def test_enrich_linkedin_task_failure(mock_growthtoolkit_http):
    def handler(method, url, params, json_body):
        if "/enrichment/linkedin/" in url:
            return (200, {"success": True, "code": "200", "data": {"task_id": "t-bad"}})
        return (200, {"success": True, "code": "200", "data": {"status": "failed"}})

    mock_growthtoolkit_http.handler = handler
    with pytest.raises(gts.GrowthToolkitError) as exc:
        await gts.enrich_linkedin("https://linkedin.com/in/jane")
    assert "failed" in str(exc.value)


async def test_usage_doc_written_on_success(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, OK_EMAIL_BODY)]
    await gts.find_email("Jane", "Doe", "acme.com", account_id="acct-42", prospect_id="p-1")
    doc = mock_growthtoolkit_http.usage_docs[-1]
    assert doc["endpoint"] == "email-finder"
    assert doc["success"] is True
    assert doc["credits_used"] == 1
    assert doc["prospect_id"] == "p-1"


async def test_usage_doc_written_on_credit_error(mock_growthtoolkit_http):
    mock_growthtoolkit_http.queue = [(200, CREDITS_BODY)]
    with pytest.raises(gts.CreditsExhausted):
        await gts.find_email("Jane", "Doe", "acme.com", account_id="acct-43")
    doc = mock_growthtoolkit_http.usage_docs[-1]
    assert doc["success"] is False
    assert doc["credits_used"] == 0
    assert doc["code"] == "402"


# ---------------------------------------------------------------------------
# email_finder_service facade
# ---------------------------------------------------------------------------

from services import email_finder_service  # noqa: E402


def test_split_full_name():
    assert email_finder_service._split_full_name("Jane Doe") == ("Jane", "Doe")
    assert email_finder_service._split_full_name("Jane Van Der Berg") == ("Jane", "Van Der Berg")
    assert email_finder_service._split_full_name("Mononym") == ("", "")
    assert email_finder_service._split_full_name("") == ("", "")


def test_extract_first_last_prefers_explicit_fields():
    assert email_finder_service._extract_first_last(
        {"first_name": "A", "last_name": "B", "full_name": "C D"}) == ("A", "B")
    assert email_finder_service._extract_first_last({"full_name": "C D"}) == ("C", "D")
    assert email_finder_service._extract_first_last({}) == ("", "")


async def test_facade_never_raises_on_provider_error(monkeypatch):
    async def _boom(*a, **k):
        raise gts.CreditsExhausted("no credits", code="402")

    monkeypatch.setattr(gts, "find_email", _boom)
    assert await email_finder_service.find_email("J", "D", "x.com") is None


async def test_find_emails_batch_shapes(monkeypatch):
    async def _fake(first, last, domain, *, account_id=None):
        return f"{first.lower()}@{domain}" if first == "Hit" else None

    monkeypatch.setattr(gts, "find_email", _fake)
    entries = [
        {"first_name": "Hit", "last_name": "One", "domain": "a.com", "key": "k1"},
        {"first_name": "Miss", "last_name": "Two", "domain": "b.com", "key": "k2"},
    ]
    results = await email_finder_service.find_emails(entries)
    assert results == {"k1": "hit@a.com", "k2": None}


async def test_find_emails_empty_entries():
    assert await email_finder_service.find_emails([]) == {}


async def test_find_emails_surfaces_credit_warning(monkeypatch):
    """A GrowthToolkit credit-exhaustion error must not raise out of find_emails
    (missing emails can't block discovery), but must append one human-readable
    message to the caller-supplied `credit_warnings` sink."""
    async def _boom(first, last, domain, *, account_id=None):
        raise gts.CreditsExhausted("no credits left", code="402")

    monkeypatch.setattr(gts, "find_email", _boom)
    entries = [
        {"first_name": "A", "last_name": "One", "domain": "a.com", "key": "k1"},
        {"first_name": "B", "last_name": "Two", "domain": "b.com", "key": "k2"},
    ]
    warnings: list[str] = []
    results = await email_finder_service.find_emails(entries, credit_warnings=warnings)

    assert results == {"k1": None, "k2": None}
    # One deduped message even though both lookups failed with the same code.
    assert len(warnings) == 1
    assert "credits" in warnings[0].lower()


async def test_find_emails_credit_warning_checks_error_code_not_subclass(monkeypatch):
    """GrowthToolkit's own code allowlist doesn't cover every credit-exhaustion
    code the provider actually returns (e.g. `recharge_credit_immediately`
    observed in production) — email_finder_service must key off the raised
    error's `.code` string, not the raised exception's class, so it still
    surfaces a warning even for a plain GrowthToolkitError."""
    async def _boom(first, last, domain, *, account_id=None):
        raise gts.GrowthToolkitError("recharge now", code="recharge_credit_immediately")

    monkeypatch.setattr(gts, "find_email", _boom)
    entries = [{"first_name": "A", "last_name": "One", "domain": "a.com", "key": "k1"}]
    warnings: list[str] = []
    result = await email_finder_service.find_emails(entries, credit_warnings=warnings)

    assert result == {"k1": None}
    assert len(warnings) == 1
    assert "recharge_credit_immediately" in warnings[0]


async def test_find_emails_no_warning_for_non_credit_error(monkeypatch):
    """Non-credit errors (rate limit, invalid input, etc.) must not populate
    credit_warnings — only credit exhaustion is user-facing."""
    async def _boom(first, last, domain, *, account_id=None):
        raise gts.InvalidInput("bad domain", code="417")

    monkeypatch.setattr(gts, "find_email", _boom)
    entries = [{"first_name": "A", "last_name": "One", "domain": "a.com", "key": "k1"}]
    warnings: list[str] = []
    result = await email_finder_service.find_emails(entries, credit_warnings=warnings)

    assert result == {"k1": None}
    assert warnings == []


async def test_find_emails_credit_warnings_default_none_is_safe():
    """Existing callers that don't pass credit_warnings must keep working
    exactly as before (no side effects, no crash)."""
    assert await email_finder_service.find_emails([]) == {}


# ---------------------------------------------------------------------------
# Iteration 2: shared module-level httpx client (lazy init, reuse, aclose)
# ---------------------------------------------------------------------------

async def test_shared_client_lazy_init_reuse_and_aclose(monkeypatch):
    """_get_client must create exactly one AsyncClient, reuse it across calls,
    and aclose() must close + reset it so the next call re-creates one."""
    created = []

    class _CountingClient:
        def __init__(self, *args, **kwargs):
            self.closed = False
            created.append(self)

        async def aclose(self):
            self.closed = True

    monkeypatch.setattr(gts.httpx, "AsyncClient", _CountingClient)
    monkeypatch.setattr(gts, "_client", None)

    c1 = gts._get_client()
    c2 = gts._get_client()
    assert c1 is c2
    assert len(created) == 1

    await gts.aclose()
    assert gts._client is None
    assert c1.closed

    c3 = gts._get_client()
    assert c3 is not c1
    assert len(created) == 2
    await gts.aclose()


async def test_requests_reuse_single_shared_client(mock_growthtoolkit_http):
    """Two API calls must go through the same shared client instance (the
    fixture installs exactly one fake as gts._client; both calls hit it)."""
    shared = gts._get_client()
    mock_growthtoolkit_http.queue = [(200, OK_EMAIL_BODY), (200, OK_EMAIL_BODY)]
    await gts.find_email("Jane", "Doe", "acme.com")
    assert gts._get_client() is shared
    await gts.find_email("John", "Roe", "acme.com")
    assert gts._get_client() is shared
    assert len(mock_growthtoolkit_http.calls) == 2
