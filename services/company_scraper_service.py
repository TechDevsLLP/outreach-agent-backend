"""
Apify LinkedIn company page scraper integration.
Uses actor UwSdACBp7ymaGUJjS with deduplication across leads.
"""

import asyncio
import threading

from apify_client import ApifyClient
from config import get_settings
import logging

logger = logging.getLogger(__name__)

settings = get_settings()


class _LazyApifyClient:
    """Construct the synchronous dataset client only on first provider use."""

    def __init__(self, api_key: str):
        self._api_key = api_key
        self._client = None
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = ApifyClient(self._api_key)
        return self._client

    def dataset(self, dataset_id: str):
        return self._get_client().dataset(dataset_id)


apify_client = _LazyApifyClient(settings.apify_api_key)


def extract_unique_company_urls(leads_with_profiles: list[dict]) -> dict[str, list[str]]:
    """
    Extract unique company LinkedIn URLs from prospects and their profile data.
    Deduplicates companies across leads.

    Returns dict[company_url -> list[prospect_ids]] mapping each unique company
    to all prospect IDs at that company.
    """
    company_to_leads: dict[str, list[str]] = {}

    for prospect in leads_with_profiles:
        prospect_id = str(prospect.get("_id", ""))
        company_url = None

        # Try profile data first (most reliable)
        profile_data = prospect.get("linkedin_profile_data")
        if profile_data:
            company_url = profile_data.get("companyLinkedin") or profile_data.get("companyUrl")

        # Fall back to lead's company_linkedin field
        if not company_url:
            company_url = prospect.get("company_linkedin")

        if company_url:
            normalized = _normalize_company_url(company_url)
            if normalized not in company_to_leads:
                company_to_leads[normalized] = []
            company_to_leads[normalized].append(prospect_id)
        else:
            logger.debug(f"No company URL found for prospect {prospect_id}")

    total_prospects = sum(len(ids) for ids in company_to_leads.values())
    logger.info(
        f"Deduplicated {total_prospects} prospects into {len(company_to_leads)} unique companies "
        f"(saved {total_prospects - len(company_to_leads)} scrapes)"
    )
    return company_to_leads


async def scrape_company_pages(urls: list[str]) -> tuple[str, list[dict]]:
    """
    Scrape LinkedIn company pages via Apify actor.
    Batches up to 50 URLs per actor call.
    Returns (run_id, list_of_company_dicts).

    The actor call itself is retried/semaphore-bounded via
    services.apify_service.call_actor_with_retry; dataset iteration stays
    off the event loop via asyncio.to_thread, same as when this whole
    function was run in an executor by its callers.
    """
    if not urls:
        return "no_urls", []

    logger.info(f"Scraping {len(urls)} company pages via Apify")

    run_input = {
        "companies": urls,
    }

    from services.apify_service import call_actor_with_retry
    run = await call_actor_with_retry(settings.apify_company_scraper_id, run_input)
    run_id = run.get("id", "unknown")

    logger.info(f"Company scrape completed: {run_id}")

    results = []
    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        results = await asyncio.to_thread(lambda: list(apify_client.dataset(dataset_id).iterate_items()))

    logger.info(f"Fetched {len(results)} company results from Apify")
    return run_id, results


def match_companies_to_urls(urls: list[str], results: list[dict]) -> dict[str, dict]:
    """
    Map scraped company results back to their LinkedIn URLs.
    Returns dict[normalized_url -> company_data].
    """
    matched = {}

    # Build lookup from results by various URL fields
    result_lookup = {}
    for result in results:
        for url_field in ["linkedinUrl", "url", "companyUrl", "linkedInUrl"]:
            url = result.get(url_field, "")
            if url:
                normalized = _normalize_company_url(url)
                result_lookup[normalized] = result

    # Match input URLs to results
    for url in urls:
        normalized = _normalize_company_url(url)
        if normalized in result_lookup:
            matched[url] = result_lookup[normalized]
        else:
            logger.warning(f"No company result found for URL: {url}")

    logger.info(f"Matched {len(matched)}/{len(urls)} companies")
    return matched


def _normalize_company_url(url: str) -> str:
    """Normalize company LinkedIn URL for comparison."""
    url = url.strip().rstrip("/").lower()
    if "?" in url:
        url = url.split("?")[0]
    return url
