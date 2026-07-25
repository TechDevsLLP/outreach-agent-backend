"""Unit tests for the rate limiter + retry wrapper added around the direct
Gemini SDK call in services/company_sourcing_service.py. Mocks
google.genai.Client entirely — no real Gemini calls."""
import asyncio

import pytest
from google import genai

from services import company_sourcing_service as css
from services import openrouter_service

pytestmark = pytest.mark.unit


class _FakeUsageMeta:
    prompt_token_count = 10
    candidates_token_count = 20


class _FakeResponse:
    def __init__(self, text):
        self.text = text
        self.usage_metadata = _FakeUsageMeta()


class _FakeAio:
    def __init__(self, generate_content_fn):
        self.models = self
        self._fn = generate_content_fn

    async def generate_content(self, **kwargs):
        return await self._fn(**kwargs)


class _FakeGenaiClient:
    def __init__(self, generate_content_fn):
        self.aio = _FakeAio(generate_content_fn)


ONE_COMPANY_JSON = (
    '{"companies": [{"name": "Acme Inc", "linkedin_url": "https://linkedin.com/company/acme", '
    '"domain": "acme.com", "website": "https://acme.com", "industry": "SaaS", "country": "US", '
    '"employee_size_estimate": "51-200", "description": "A widget company."}]}'
)


@pytest.fixture(autouse=True)
def _no_usage_recording(monkeypatch):
    """The batch runner fire-and-forgets a usage-tracking write to Mongo via
    asyncio.create_task — stub it out so tests don't depend on a live DB."""
    async def _noop(*args, **kwargs):
        return None
    monkeypatch.setattr(openrouter_service, "_record_openrouter_usage", _noop)


@pytest.fixture(autouse=True)
def _reset_rate_limiter_registry(monkeypatch):
    """Rate limiters are cached process-wide by key; give each test a clean
    registry so budgets from other tests don't leak in."""
    from utils import rate_limiter as rl
    monkeypatch.setattr(rl, "_limiters", {})


async def test_source_companies_succeeds_first_try(monkeypatch):
    calls = {"n": 0}

    async def _generate_content(**kwargs):
        calls["n"] += 1
        return _FakeResponse(ONE_COMPANY_JSON)

    monkeypatch.setattr(genai, "Client", lambda api_key: _FakeGenaiClient(_generate_content))

    companies, meta = await css.source_companies(
        icp_prompt="B2B SaaS companies", target_count=1, validate_urls=False
    )

    assert calls["n"] == 1
    assert len(companies) == 1
    assert companies[0]["company_name"] == "Acme Inc"
    assert meta["batches_run"] == 1


async def test_retries_on_429_then_succeeds(monkeypatch):
    calls = {"n": 0}

    async def _generate_content(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            err = Exception("429 RESOURCE_EXHAUSTED: quota exceeded")
            err.code = 429
            raise err
        return _FakeResponse(ONE_COMPANY_JSON)

    monkeypatch.setattr(genai, "Client", lambda api_key: _FakeGenaiClient(_generate_content))
    monkeypatch.setattr(css, "backoff_with_jitter", lambda attempt, **kw: 0.001)

    companies, meta = await css.source_companies(
        icp_prompt="B2B SaaS companies", target_count=1, validate_urls=False
    )

    assert calls["n"] == 2
    assert len(companies) == 1


async def test_honors_retry_delay_from_gemini_error(monkeypatch):
    calls = {"n": 0}
    sleep_calls = []

    async def _generate_content(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            err = Exception('429 error: {"error": {"details": [{"retryDelay": "0.02s"}]}}')
            err.code = 429
            raise err
        return _FakeResponse(ONE_COMPANY_JSON)

    real_sleep = asyncio.sleep

    async def _tracked_sleep(secs):
        sleep_calls.append(secs)
        await real_sleep(0)  # don't actually wait in the test

    monkeypatch.setattr(genai, "Client", lambda api_key: _FakeGenaiClient(_generate_content))
    monkeypatch.setattr(css.asyncio, "sleep", _tracked_sleep)

    companies, meta = await css.source_companies(
        icp_prompt="B2B SaaS companies", target_count=1, validate_urls=False
    )

    assert calls["n"] == 2
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(0.02, abs=1e-6)


async def test_exhausts_retries_and_batch_returns_empty(monkeypatch):
    """Every batch permanently fails (e.g. quota fully exhausted): each of the
    _MAX_BATCHES sequential batches retries _GEMINI_RETRY_MAX times and gives
    up, and source_companies() returns no companies rather than raising."""
    calls = {"n": 0}

    async def _generate_content(**kwargs):
        calls["n"] += 1
        err = Exception("429 RESOURCE_EXHAUSTED: quota exceeded")
        err.code = 429
        raise err

    monkeypatch.setattr(genai, "Client", lambda api_key: _FakeGenaiClient(_generate_content))
    monkeypatch.setattr(css, "backoff_with_jitter", lambda attempt, **kw: 0.001)

    companies, meta = await css.source_companies(
        icp_prompt="B2B SaaS companies", target_count=1, validate_urls=False
    )

    assert calls["n"] == css._GEMINI_RETRY_MAX * css._MAX_BATCHES
    assert companies == []
    assert meta["batches_run"] == 0


def test_extract_retry_delay_from_structured_details():
    err = Exception("boom")
    err.details = {"error": {"details": [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "13s"}]}}
    assert css._extract_gemini_retry_delay_seconds(err) == 13.0


def test_extract_retry_delay_falls_back_to_regex():
    err = Exception('some wrapper text retryDelay: "7.5s" trailing junk')
    assert css._extract_gemini_retry_delay_seconds(err) == 7.5


def test_extract_retry_delay_returns_none_when_absent():
    assert css._extract_gemini_retry_delay_seconds(Exception("plain error")) is None


def test_is_gemini_rate_limit_error_detects_code_429():
    err = Exception("boom")
    err.code = 429
    assert css._is_gemini_rate_limit_error(err) is True


def test_is_gemini_rate_limit_error_false_for_other_errors():
    assert css._is_gemini_rate_limit_error(ValueError("not a rate limit")) is False
