"""Unit tests confirming the raw `apify_client.actor(...).call(...)` sites in
services/employee_scraper_service.py, services/company_scraper_service.py and
services/linkedin_post_scraper_service.py were migrated to
services.apify_service.call_actor_with_retry.

Mocks call_actor_with_retry entirely — no real Apify calls, no network I/O.
"""
import pytest

pytestmark = pytest.mark.unit


class _FakeDataset:
    def __init__(self, items):
        self._items = items

    def iterate_items(self):
        return iter(self._items)


class _FakeApifyClientForDataset:
    """Only need `.dataset(id).iterate_items()` — the actor call itself goes
    through the mocked call_actor_with_retry, not this client."""

    def __init__(self, items):
        self._items = items

    def dataset(self, dataset_id):
        return _FakeDataset(self._items)


# ---------------------------------------------------------------------------
# services/employee_scraper_service.py — 3 migrated sites
# ---------------------------------------------------------------------------

async def test_scrape_companies_uses_call_actor_with_retry(monkeypatch):
    from services import employee_scraper_service as svc

    calls = []

    async def _fake_retry(actor_id, run_input, **kwargs):
        calls.append((actor_id, run_input))
        return {"id": "run1", "defaultDatasetId": "ds1"}

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)
    monkeypatch.setattr(svc, "apify_client", _FakeApifyClientForDataset([{"name": "Acme"}]))

    result = await svc._scrape_companies(["https://linkedin.com/company/acme"])

    assert result == [{"name": "Acme"}]
    assert len(calls) == 1
    assert calls[0][0] == svc.COMPANY_SCRAPER_ACTOR_ID


async def test_scrape_employees_for_company_uses_call_actor_with_retry(monkeypatch):
    from services import employee_scraper_service as svc

    calls = []

    async def _fake_retry(actor_id, run_input, **kwargs):
        calls.append((actor_id, run_input))
        return {"id": "run2", "defaultDatasetId": "ds2"}

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)
    monkeypatch.setattr(svc, "apify_client", _FakeApifyClientForDataset([{"name": "Jane"}]))

    result = await svc._scrape_employees_for_company("https://linkedin.com/company/acme")

    assert result == [{"name": "Jane"}]
    assert len(calls) == 1
    assert calls[0][0] == svc.EMPLOYEE_SCRAPER_ACTOR_ID


async def test_run_one_employee_scrape_uses_call_actor_with_retry(monkeypatch):
    from services import employee_scraper_service as svc

    calls = []

    async def _fake_retry(actor_id, run_input, **kwargs):
        calls.append((actor_id, run_input))
        return {"id": "run3", "defaultDatasetId": "ds3"}

    async def _noop_track(*args, **kwargs):
        class _Tracker:
            def set_run_id(self, *a, **k):
                pass

        class _Ctx:
            async def __aenter__(self_inner):
                return _Tracker()

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)
    monkeypatch.setattr(svc, "apify_client", _FakeApifyClientForDataset([{"name": "Bob"}]))

    import services.apify_service as apify_service_mod

    def _fake_track_apify_run(*args, **kwargs):
        class _Tracker:
            def set_run_id(self, *a, **k):
                pass

        class _Ctx:
            async def __aenter__(self_inner):
                return _Tracker()

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    monkeypatch.setattr(apify_service_mod, "track_apify_run", _fake_track_apify_run)

    result = await svc._run_one_employee_scrape(
        ["https://linkedin.com/company/acme"],
        max_items_per_company=5,
        max_items=5,
        seniority_level_ids=None,
        functional_level_ids=None,
        profile_scraper_mode="Short ($4 per 1k)",
        account_id=None,
        campaign_id=None,
    )

    assert result == [{"name": "Bob"}]
    assert len(calls) == 1
    assert calls[0][0] == svc.EMPLOYEE_SCRAPER_ACTOR_ID


# ---------------------------------------------------------------------------
# services/company_scraper_service.py — scrape_company_pages is now async
# ---------------------------------------------------------------------------

async def test_scrape_company_pages_uses_call_actor_with_retry(monkeypatch):
    from services import company_scraper_service as svc

    calls = []

    async def _fake_retry(actor_id, run_input, **kwargs):
        calls.append((actor_id, run_input))
        return {"id": "run4", "defaultDatasetId": "ds4"}

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)
    monkeypatch.setattr(svc, "apify_client", _FakeApifyClientForDataset([{"name": "Acme"}]))

    run_id, results = await svc.scrape_company_pages(["https://linkedin.com/company/acme"])

    assert run_id == "run4"
    assert results == [{"name": "Acme"}]
    assert len(calls) == 1
    assert calls[0][0] == svc.settings.apify_company_scraper_id


async def test_scrape_company_pages_empty_urls_short_circuits(monkeypatch):
    from services import company_scraper_service as svc

    async def _fake_retry(*args, **kwargs):
        raise AssertionError("should not call Apify for an empty url list")

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)

    run_id, results = await svc.scrape_company_pages([])
    assert run_id == "no_urls"
    assert results == []


# ---------------------------------------------------------------------------
# services/linkedin_post_scraper_service.py
# ---------------------------------------------------------------------------

async def test_scrape_linkedin_posts_bulk_uses_call_actor_with_retry(monkeypatch):
    from services import linkedin_post_scraper_service as svc

    calls = []

    async def _fake_retry(actor_id, run_input, **kwargs):
        calls.append((actor_id, run_input))
        return {"id": "run5", "defaultDatasetId": "ds5", "status": "SUCCEEDED"}

    async def _noop_track(*args, **kwargs):
        class _Tracker:
            def set_run_id(self, *a, **k):
                pass

        class _Ctx:
            async def __aenter__(self_inner):
                return _Tracker()

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)
    monkeypatch.setattr(svc, "apify_client", _FakeApifyClientForDataset([]))

    import services.apify_service as apify_service_mod

    def _fake_track_apify_run(*args, **kwargs):
        class _Tracker:
            def set_run_id(self, *a, **k):
                pass

        class _Ctx:
            async def __aenter__(self_inner):
                return _Tracker()

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()

    monkeypatch.setattr(apify_service_mod, "track_apify_run", _fake_track_apify_run)

    result = await svc.scrape_linkedin_posts_bulk(["https://linkedin.com/in/jane"])

    assert result == {"https://linkedin.com/in/jane": []}
    assert len(calls) == 1
    assert calls[0][0] == svc.LINKEDIN_POST_SCRAPER_ACTOR_ID


async def test_scrape_linkedin_posts_bulk_handles_actor_failure(monkeypatch):
    """Preserves prior try/except behavior: on failure, returns empty-list
    result dict rather than raising."""
    from services import linkedin_post_scraper_service as svc

    async def _fake_retry(actor_id, run_input, **kwargs):
        raise RuntimeError("apify down")

    monkeypatch.setattr("services.apify_service.call_actor_with_retry", _fake_retry)

    result = await svc.scrape_linkedin_posts_bulk(["https://linkedin.com/in/jane"])
    assert result == {"https://linkedin.com/in/jane": []}
