"""Unit tests for services/apify_service.call_actor_with_retry — the retry
+ global-concurrency-semaphore helper added around the raw apify_client
actor().call() pattern. Mocks apify_client entirely; no real Apify calls."""
import asyncio

import pytest

from services import apify_service

pytestmark = pytest.mark.unit


def _make_apify_api_error(status_code: int, err_type: str = "invalid-input") -> Exception:
    """Build an apify_client.errors.ApifyApiError without needing a real
    impit.Response object (its __init__ requires one)."""
    err = Exception.__new__(apify_service.ApifyApiError)
    err.message = "boom"
    err.type = err_type
    err.data = {}
    err.status_code = status_code
    err.attempt = 1
    err.http_method = "POST"
    Exception.__init__(err, "boom")
    return err


class _FakeActorHandle:
    def __init__(self, call_fn):
        self._call_fn = call_fn

    def call(self, **kwargs):
        return self._call_fn(**kwargs)


class _FakeApifyClient:
    """Stand-in for the module-level `apify_client` ApifyClient instance."""

    def __init__(self, call_fn):
        self._call_fn = call_fn
        self.calls = 0

    def actor(self, actor_id):
        def _tracked(**kwargs):
            self.calls += 1
            return self._call_fn(**kwargs)
        return _FakeActorHandle(_tracked)


@pytest.fixture(autouse=True)
def _reset_semaphore(monkeypatch):
    """The actor semaphore is created lazily and cached at module scope —
    reset it between tests so each test gets a fresh one sized off current
    settings (or a monkeypatched limit)."""
    monkeypatch.setattr(apify_service, "_actor_semaphore", None)
    monkeypatch.setattr(apify_service, "backoff_with_jitter", lambda attempt, **kw: 0.001)
    yield
    monkeypatch.setattr(apify_service, "_actor_semaphore", None)


async def test_call_actor_with_retry_succeeds_first_try(monkeypatch):
    fake = _FakeApifyClient(lambda **kw: {"id": "run1", "status": "SUCCEEDED", "defaultDatasetId": "ds1"})
    monkeypatch.setattr(apify_service, "apify_client", fake)

    run = await apify_service.call_actor_with_retry("actor123", {"foo": "bar"})

    assert run["status"] == "SUCCEEDED"
    assert fake.calls == 1


async def test_retries_on_network_error_then_succeeds(monkeypatch):
    attempts = {"n": 0}

    def _call_fn(**kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionError("transient network blip")
        return {"id": "run2", "status": "SUCCEEDED", "defaultDatasetId": "ds2"}

    fake = _FakeApifyClient(_call_fn)
    monkeypatch.setattr(apify_service, "apify_client", fake)

    run = await apify_service.call_actor_with_retry("actor123", {}, max_retries=1)

    assert run["status"] == "SUCCEEDED"
    assert attempts["n"] == 2


async def test_retries_on_failed_run_status_then_succeeds(monkeypatch):
    attempts = {"n": 0}

    def _call_fn(**kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return {"id": "run3", "status": "FAILED", "defaultDatasetId": "ds3"}
        return {"id": "run3b", "status": "SUCCEEDED", "defaultDatasetId": "ds3b"}

    fake = _FakeApifyClient(_call_fn)
    monkeypatch.setattr(apify_service, "apify_client", fake)

    run = await apify_service.call_actor_with_retry("actor123", {}, max_retries=1)

    assert run["status"] == "SUCCEEDED"
    assert attempts["n"] == 2


async def test_returns_last_failed_run_when_retries_exhausted(monkeypatch):
    fake = _FakeApifyClient(lambda **kw: {"id": "run4", "status": "ABORTED", "defaultDatasetId": "ds4"})
    monkeypatch.setattr(apify_service, "apify_client", fake)

    run = await apify_service.call_actor_with_retry("actor123", {}, max_retries=1)

    assert run["status"] == "ABORTED"
    assert fake.calls == 2  # initial + 1 retry


async def test_network_error_exhausted_raises(monkeypatch):
    def _call_fn(**kwargs):
        raise ConnectionError("still down")

    fake = _FakeApifyClient(_call_fn)
    monkeypatch.setattr(apify_service, "apify_client", fake)

    with pytest.raises(ConnectionError):
        await apify_service.call_actor_with_retry("actor123", {}, max_retries=1)
    assert fake.calls == 2


async def test_invalid_input_error_is_not_retried(monkeypatch):
    err = _make_apify_api_error(400, "invalid-input")

    def _call_fn(**kwargs):
        raise err

    fake = _FakeApifyClient(_call_fn)
    monkeypatch.setattr(apify_service, "apify_client", fake)

    with pytest.raises(apify_service.ApifyApiError):
        await apify_service.call_actor_with_retry("actor123", {}, max_retries=3)

    assert fake.calls == 1  # no retries attempted


async def test_timeout_secs_forwarded_to_call(monkeypatch):
    captured = {}

    def _call_fn(**kwargs):
        captured.update(kwargs)
        return {"id": "run5", "status": "SUCCEEDED", "defaultDatasetId": "ds5"}

    fake = _FakeApifyClient(_call_fn)
    monkeypatch.setattr(apify_service, "apify_client", fake)

    await apify_service.call_actor_with_retry("actor123", {"a": 1}, timeout_secs=120)

    assert captured["timeout_secs"] == 120
    assert captured["run_input"] == {"a": 1}


async def test_global_semaphore_bounds_concurrency(monkeypatch):
    monkeypatch.setattr(apify_service.settings, "apify_actor_concurrency_limit", 2)

    concurrent = {"current": 0, "max": 0}
    lock = asyncio.Lock()

    async def _slow_call(**kwargs):
        async with lock:
            concurrent["current"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["current"])
        await asyncio.sleep(0.05)
        async with lock:
            concurrent["current"] -= 1
        return {"id": "run", "status": "SUCCEEDED", "defaultDatasetId": "ds"}

    class _FakeAsyncActorClient:
        def actor(self, actor_id):
            class _Handle:
                def call(self, **kwargs):
                    # apify_service runs this via asyncio.to_thread, so this
                    # runs on a worker thread — bridge back to the event loop.
                    return asyncio.run_coroutine_threadsafe(
                        _slow_call(**kwargs), loop
                    ).result()
            return _Handle()

    loop = asyncio.get_event_loop()
    monkeypatch.setattr(apify_service, "apify_client", _FakeAsyncActorClient())

    await asyncio.gather(*[
        apify_service.call_actor_with_retry(f"actor{i}", {})
        for i in range(5)
    ])

    assert concurrent["max"] <= 2
