"""Unit tests for the 429/5xx retry added to UnipileClient._request /
_request_form (services/unipile_service.py). Mocks httpx.AsyncClient
entirely — no real Unipile calls."""
import pytest

from services import unipile_service
from services.unipile_service import UnipileAPIError, UnipileClient

pytestmark = pytest.mark.unit


class _FakeResponse:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = str(self._body)

    def json(self):
        return self._body


class _FakeAsyncClient:
    """Replaces httpx.AsyncClient(...) inside _request/_request_form. Each
    instantiation pops the next response off the shared queue."""

    def __init__(self, queue):
        self._queue = queue

    def __call__(self, *args, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def request(self, method, url, headers=None, **kwargs):
        if not self._queue:
            raise AssertionError("no queued response left")
        return self._queue.pop(0)


def _patch_httpx(monkeypatch, responses):
    queue = list(responses)
    fake_client_factory = _FakeAsyncClient(queue)
    monkeypatch.setattr(unipile_service.httpx, "AsyncClient", fake_client_factory)
    monkeypatch.setattr(unipile_service, "backoff_with_jitter", lambda attempt, **kw: 0.001)
    return queue


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(unipile_service.settings, "unipile_token", "test-token")
    monkeypatch.setattr(unipile_service.settings, "unipile_base_url", "https://fake.unipile.test/api/v1")
    return UnipileClient()


async def test_request_succeeds_first_try(monkeypatch, client):
    _patch_httpx(monkeypatch, [_FakeResponse(200, {"items": []})])
    result = await client._request("GET", "accounts")
    assert result == {"items": []}


async def test_request_retries_429_then_succeeds(monkeypatch, client):
    queue = _patch_httpx(monkeypatch, [
        _FakeResponse(429, {"error": "rate limited"}),
        _FakeResponse(429, {"error": "rate limited"}),
        _FakeResponse(200, {"items": ["ok"]}),
    ])
    result = await client._request("GET", "accounts")
    assert result == {"items": ["ok"]}
    assert queue == []  # all 3 canned responses consumed


async def test_request_retries_5xx_then_succeeds(monkeypatch, client):
    _patch_httpx(monkeypatch, [
        _FakeResponse(503, {"error": "upstream down"}),
        _FakeResponse(200, {"ok": True}),
    ])
    result = await client._request("POST", "users/invite", json={})
    assert result == {"ok": True}


async def test_request_honors_retry_after_header(monkeypatch, client):
    _patch_httpx(monkeypatch, [
        _FakeResponse(429, {}, headers={"Retry-After": "0.01"}),
        _FakeResponse(200, {"ok": True}),
    ])
    # backoff_with_jitter is stubbed to 0.001s already; this test just checks
    # that a Retry-After header doesn't break the retry path and the call
    # still eventually succeeds.
    result = await client._request("GET", "accounts")
    assert result == {"ok": True}


async def test_request_exhausts_retries_and_raises(monkeypatch, client):
    _patch_httpx(monkeypatch, [_FakeResponse(429, {"error": "still limited"})] * (unipile_service.MAX_RETRIES + 1))
    with pytest.raises(UnipileAPIError) as exc:
        await client._request("GET", "accounts")
    assert exc.value.status_code == 429


async def test_request_does_not_retry_4xx_other_than_429(monkeypatch, client):
    _patch_httpx(monkeypatch, [_FakeResponse(404, {"error": "not found"})])
    with pytest.raises(UnipileAPIError) as exc:
        await client._request("GET", "users/ghost")
    assert exc.value.status_code == 404


async def test_request_form_retries_5xx_then_succeeds(monkeypatch, client):
    _patch_httpx(monkeypatch, [
        _FakeResponse(500, {"error": "boom"}),
        _FakeResponse(200, {"ok": True}),
    ])
    result = await client._request_form("POST", "chats", {"text": "hi"})
    assert result == {"ok": True}
