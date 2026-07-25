"""Unit tests for structured website scraping (services/website_scraper_service)
and its wiring into company enrichment (services/employee_scraper_service).

httpx is faked at the module level — no live network calls.
"""
from datetime import datetime, timedelta

import httpx
import pytest

from services import website_scraper_service as wss

pytestmark = pytest.mark.unit


SAMPLE_HTML = """
<html>
<head>
  <title>  Acme Robotics — Home  </title>
  <meta name="description" content="We build friendly warehouse robots.">
  <meta name="generator" content="WordPress 6.4">
  <script src="/wp-includes/js/jquery.min.js"></script>
  <script src="https://js.stripe.com/v3/"></script>
</head>
<body>
  <nav>Home About Contact</nav>
  <main>
    <h1>About Acme</h1>
    <p>Acme Robotics automates fulfillment centers worldwide.</p>
    <style>.x{color:red}</style>
    <a href="https://www.linkedin.com/company/acme-robotics">LinkedIn</a>
    <a href="/social/twitter" onclick="x">ignore-relative</a>
    <a href="https://twitter.com/acme">Twitter</a>
    <a href="https://github.com/acme">GitHub</a>
  </main>
</body>
</html>
"""


class _FakeResponse:
    def __init__(self, text="", content_type="text/html; charset=utf-8",
                 status=200, url="https://acme.example/"):
        self.text = text
        self.headers = {} if content_type is None else {"content-type": content_type}
        self._status = status
        self.url = url

    def raise_for_status(self):
        if self._status >= 400:
            raise httpx.HTTPStatusError(
                "err", request=httpx.Request("GET", self.url),
                response=httpx.Response(self._status, request=httpx.Request("GET", self.url)),
            )


class _FakeAsyncClient:
    """Async-context-manager stand-in for httpx.AsyncClient."""

    def __init__(self, response=None, exc=None):
        self._response = response
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url):
        if self._exc is not None:
            raise self._exc
        return self._response


def _patch_httpx(monkeypatch, *, response=None, exc=None):
    monkeypatch.setattr(
        wss.httpx, "AsyncClient",
        lambda *a, **k: _FakeAsyncClient(response=response, exc=exc),
    )


# ── structured extraction ──────────────────────────────────────────


async def test_fetch_website_data_structured(monkeypatch):
    _patch_httpx(monkeypatch, response=_FakeResponse(SAMPLE_HTML))
    data = await wss.fetch_website_data("acme.example")

    assert data is not None
    assert data["url"] == "https://acme.example/"
    assert data["title"] == "Acme Robotics — Home"
    assert data["meta_desc"] == "We build friendly warehouse robots."
    assert "Acme Robotics automates fulfillment centers" in data["about_text"]
    # tag-stripped: no style/script leakage
    assert "color:red" not in data["about_text"]
    # socials detected + relative href resolved against base
    assert data["socials"]["linkedin"] == "https://www.linkedin.com/company/acme-robotics"
    assert data["socials"]["twitter"] == "https://twitter.com/acme"
    assert data["socials"]["github"] == "https://github.com/acme"
    # tech hints from generator meta + script srcs
    assert "WordPress" in data["tech"]
    assert "Stripe" in data["tech"]
    assert "jQuery" in data["tech"]
    assert isinstance(data["scraped_at"], datetime)


async def test_fetch_website_data_bounds_about_text(monkeypatch):
    big = "<html><body>" + ("word " * 5000) + "</body></html>"
    _patch_httpx(monkeypatch, response=_FakeResponse(big))
    data = await wss.fetch_website_data("acme.example")
    assert data is not None
    assert len(data["about_text"]) <= wss._ABOUT_MAX_CHARS


# ── content-type handling ──────────────────────────────────────────


async def test_fetch_website_data_non_html_returns_none(monkeypatch):
    _patch_httpx(monkeypatch, response=_FakeResponse("%PDF-1.5", content_type="application/pdf"))
    assert await wss.fetch_website_data("acme.example/doc.pdf") is None


async def test_fetch_website_data_missing_content_type_parsed(monkeypatch):
    # No content-type header → optimistically parsed as HTML.
    _patch_httpx(monkeypatch, response=_FakeResponse(SAMPLE_HTML, content_type=None))
    data = await wss.fetch_website_data("acme.example")
    assert data is not None
    assert data["title"] == "Acme Robotics — Home"


# ── error handling → failure marker ────────────────────────────────


async def test_fetch_website_data_timeout_returns_none(monkeypatch):
    _patch_httpx(monkeypatch, exc=httpx.TimeoutException("slow"))
    assert await wss.fetch_website_data("acme.example") is None


async def test_fetch_website_data_http_error_returns_none(monkeypatch):
    _patch_httpx(monkeypatch, response=_FakeResponse("", status=503))
    assert await wss.fetch_website_data("acme.example") is None


def test_failure_marker_is_distinguishable():
    marker = wss.website_data_failure_marker("https://acme.example", "timeout")
    assert marker["error"] == "timeout"
    assert isinstance(marker["scraped_at"], datetime)
    # A failure marker counts as "tried": not stale immediately.
    assert wss.is_website_data_stale(marker) is False


# ── staleness logic ────────────────────────────────────────────────


def test_staleness_missing_is_stale():
    assert wss.is_website_data_stale(None) is True
    assert wss.is_website_data_stale({}) is True


def test_staleness_recent_is_fresh():
    recent = {"scraped_at": datetime.utcnow() - timedelta(days=10)}
    assert wss.is_website_data_stale(recent) is False


def test_staleness_old_is_stale():
    old = {"scraped_at": datetime.utcnow() - timedelta(days=120)}
    assert wss.is_website_data_stale(old) is True


def test_staleness_custom_window():
    d = {"scraped_at": datetime.utcnow() - timedelta(days=40)}
    assert wss.is_website_data_stale(d, max_age_days=30) is True
    assert wss.is_website_data_stale(d, max_age_days=90) is False


# ── raw text dump (onboarding path still works) ────────────────────


async def test_scrape_website_text_dump(monkeypatch):
    _patch_httpx(monkeypatch, response=_FakeResponse(SAMPLE_HTML))
    text = await wss.scrape_website("https://acme.example")
    assert "Acme Robotics automates fulfillment centers" in text
    assert "color:red" not in text


async def test_scrape_website_error_returns_empty(monkeypatch):
    _patch_httpx(monkeypatch, exc=httpx.TimeoutException("slow"))
    assert await wss.scrape_website("https://acme.example") == ""


# ── enrichment wiring: upsert populates website_data ───────────────


class _FakeCompanies:
    def __init__(self, doc):
        self._doc = doc
        self.updates = []

    async def find_one(self, query, projection=None):
        return self._doc

    async def update_one(self, query, update):
        self.updates.append(update)
        return None


async def test_upsert_scrapes_when_website_data_missing(monkeypatch):
    from services import employee_scraper_service as ess

    fake_doc = {"_id": "0" * 24, "website_data": None}
    fake_col = _FakeCompanies(fake_doc)
    monkeypatch.setattr(ess, "companies_collection", fake_col)

    called = {}

    async def _fake_fetch(url):
        called["url"] = url
        return {"url": url, "title": "Acme", "scraped_at": datetime.utcnow(),
                "tech": [], "socials": {}, "about_text": "x", "meta_desc": None}

    monkeypatch.setattr(wss, "fetch_website_data", _fake_fetch)

    await ess._maybe_scrape_website_data(
        "0" * 24, website="https://acme.example", domain=None, now=datetime.utcnow(),
    )

    assert called["url"] == "https://acme.example"
    assert len(fake_col.updates) == 1
    wd = fake_col.updates[0]["$set"]["website_data"]
    assert wd["title"] == "Acme"


async def test_upsert_records_failure_marker_when_fetch_fails(monkeypatch):
    from services import employee_scraper_service as ess

    fake_col = _FakeCompanies({"_id": "0" * 24, "website_data": None})
    monkeypatch.setattr(ess, "companies_collection", fake_col)

    async def _fake_fetch(url):
        return None  # typed failure

    monkeypatch.setattr(wss, "fetch_website_data", _fake_fetch)

    await ess._maybe_scrape_website_data(
        "0" * 24, website=None, domain="acme.example", now=datetime.utcnow(),
    )

    wd = fake_col.updates[0]["$set"]["website_data"]
    assert wd["error"]  # distinguishable failure marker
    assert wd["url"] == "https://acme.example"
    assert isinstance(wd["scraped_at"], datetime)


async def test_upsert_skips_when_website_data_fresh(monkeypatch):
    from services import employee_scraper_service as ess

    fresh = {"scraped_at": datetime.utcnow() - timedelta(days=5), "title": "Old"}
    fake_col = _FakeCompanies({"_id": "0" * 24, "website_data": fresh})
    monkeypatch.setattr(ess, "companies_collection", fake_col)

    async def _boom(url):
        raise AssertionError("should not fetch when website_data is fresh")

    monkeypatch.setattr(wss, "fetch_website_data", _boom)

    await ess._maybe_scrape_website_data(
        "0" * 24, website="https://acme.example", domain=None, now=datetime.utcnow(),
    )
    assert fake_col.updates == []


async def test_upsert_noop_without_website_or_domain(monkeypatch):
    from services import employee_scraper_service as ess

    fake_col = _FakeCompanies({"_id": "0" * 24, "website_data": None})
    monkeypatch.setattr(ess, "companies_collection", fake_col)

    await ess._maybe_scrape_website_data(
        "0" * 24, website=None, domain=None, now=datetime.utcnow(),
    )
    assert fake_col.updates == []
