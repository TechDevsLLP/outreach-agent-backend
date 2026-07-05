"""
Apify LinkedIn profile scraper integration.
Uses Apify actor LpVuK3Zozwuipa5bp (harvestapi/linkedin-profile-scraper) for LinkedIn profile scraping.
Top 100 prospects by pre-enrichment score get profiles; the rest are assessed without profile data.
"""

import asyncio
from apify_client import ApifyClient
from config import get_settings
import logging

logger = logging.getLogger(__name__)

settings = get_settings()
apify_client = ApifyClient(settings.apify_api_key)


def _normalize_profile(item: dict) -> dict:
    """Map harvestapi actor output keys to the shape consumers expect."""
    profile = dict(item)

    # followerCount (singular) → followersCount
    if "followersCount" not in profile and "followerCount" in profile:
        profile["followersCount"] = profile["followerCount"]

    # experience[].position → experience[].title
    experiences = profile.get("experience")
    if isinstance(experiences, list):
        normalized_exp = []
        for entry in experiences:
            e = dict(entry)
            if "title" not in e and "position" in e:
                e["title"] = e["position"]
            normalized_exp.append(e)
        profile["experience"] = normalized_exp

    # currentPosition: actor returns a LIST — take first item
    current = profile.get("currentPosition")
    if isinstance(current, list) and current:
        current = current[0]
    if isinstance(current, dict):
        company_url = current.get("companyLinkedinUrl", "")
        if company_url:
            if "companyLinkedin" not in profile:
                profile["companyLinkedin"] = company_url
            if "companyUrl" not in profile:
                profile["companyUrl"] = company_url

    # location: actor returns {linkedinText, countryCode, parsed} — flatten to text
    location = profile.get("location")
    if isinstance(location, dict):
        profile["location_text"] = location.get("linkedinText", "")
        parsed = location.get("parsed") or {}
        if isinstance(parsed, dict):
            profile["location_city"] = parsed.get("city", "")
            profile["location_country"] = parsed.get("country", "")

    # emails: actor returns [{email, catchAllDomain, ...}] — extract best address
    emails_list = profile.get("emails")
    if isinstance(emails_list, list) and emails_list and "extracted_email" not in profile:
        first = emails_list[0]
        email_val = first.get("email") if isinstance(first, dict) else None
        if email_val:
            profile["extracted_email"] = email_val

    # Ensure posts exists so downstream prompt builders skip cleanly rather than KeyError
    if "posts" not in profile and "recentPosts" not in profile:
        profile["posts"] = []

    return profile


async def scrape_linkedin_profiles(urls: list[str]) -> tuple[str, list[dict]]:
    """
    Scrape LinkedIn profiles using Apify PROFILE_SCRAPER only.
    Returns (run_id, list_of_profile_dicts).
    """
    if not urls:
        return "no_urls", []

    logger.info(f"Scraping {len(urls)} LinkedIn profiles via Apify PROFILE_SCRAPER")
    try:
        run_input = {
            "profileScraperMode": "Profile details + email search ($10 per 1k)",
            "queries": urls,
        }
        run = await asyncio.to_thread(
            lambda: apify_client.actor(settings.apify_profile_scraper_id).call(run_input=run_input)
        )
        apify_results = []
        for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
            apify_results.append(_normalize_profile(item))
        logger.info(f"Apify profile scraper returned {len(apify_results)} raw results for {len(urls)} URLs (run: {run.get('id', 'unknown')})")
        if apify_results:
            sample = apify_results[0]
            url_in_result = sample.get("linkedinUrl") or sample.get("url") or sample.get("profileUrl") or "(no url field)"
            logger.debug(f"Sample result URL field: {url_in_result!r}  |  input URL sample: {urls[0]!r}")
        matched = match_profiles_to_urls(urls, apify_results)
        logger.info(
            f"Apify profile scraper matched {len(matched)}/{len(urls)} profiles "
            f"(run: {run.get('id', 'unknown')})"
        )
        return f"apify-{run.get('id', 'unknown')}", list(matched.values())
    except Exception as e:
        logger.error(f"Apify profile scraper failed: {e}", exc_info=True)
        return "apify-failed", []


def match_profiles_to_urls(urls: list[str], results: list[dict]) -> dict[str, dict]:
    """
    Map scraped profile results back to their LinkedIn URLs.
    Returns dict[original_url -> profile_data].

    Priority order for matching:
    1. originalQuery.query  — the exact input URL Apify received; most reliable since
       the actor resolves member-ID URLs (/in/ACwAAA...) to public-identifier URLs
       (/in/john-doe) so linkedinUrl in the result never matches the input URL.
    2. linkedinUrl / url / profileUrl — fallback for re-matching already-resolved profiles.
    """
    matched = {}

    result_lookup = {}
    for result in results:
        # Primary: originalQuery.query matches the exact URL we sent to Apify
        original_query_url = (result.get("originalQuery") or {}).get("query", "")
        if original_query_url:
            result_lookup[_normalize_linkedin_url(original_query_url)] = result

        # Fallback: resolved public-identifier URL (used when re-matching already-fetched profiles)
        for url_field in ["linkedinUrl", "url", "profileUrl"]:
            url = result.get(url_field, "")
            if url:
                result_lookup[_normalize_linkedin_url(url)] = result

    for url in urls:
        normalized = _normalize_linkedin_url(url)
        if normalized in result_lookup:
            matched[url] = result_lookup[normalized]
        else:
            logger.warning(f"No profile result found for URL: {url}")

    logger.info(f"Matched {len(matched)}/{len(urls)} profiles")
    return matched


def _normalize_linkedin_url(url: str) -> str:
    """Normalize LinkedIn URL for comparison."""
    url = url.strip().rstrip("/").lower()
    if "?" in url:
        url = url.split("?")[0]
    # Normalize www vs non-www (harvestapi actor omits www in its output)
    url = url.replace("://www.", "://")
    return url
