import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import logging

from apify_client import ApifyClient
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()
_apify_client = ApifyClient(settings.apify_api_key)


@dataclass
class FreshnessResult:
    linkedin_url: str
    apollo_company: str
    linkedin_current_company: Optional[str]
    matches: bool
    confidence: float
    profile_snapshot: dict
    linkedin_url_invalid: bool = False
    error: Optional[str] = None
    skipped: bool = False


def _normalize_profile(item: dict) -> dict:
    profile = dict(item)

    if "followersCount" not in profile and "followerCount" in profile:
        profile["followersCount"] = profile["followerCount"]

    experiences = profile.get("experience")
    if isinstance(experiences, list):
        normalized_exp = []
        for entry in experiences:
            e = dict(entry)
            if "title" not in e and "position" in e:
                e["title"] = e["position"]
            normalized_exp.append(e)
        profile["experience"] = normalized_exp

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

    location = profile.get("location")
    if isinstance(location, dict):
        profile["location_text"] = location.get("linkedinText", "")
        parsed = location.get("parsed") or {}
        if isinstance(parsed, dict):
            profile["location_city"] = parsed.get("city", "")
            profile["location_country"] = parsed.get("country", "")

    emails_list = profile.get("emails")
    if isinstance(emails_list, list) and emails_list and "extracted_email" not in profile:
        first = emails_list[0]
        email_val = first.get("email") if isinstance(first, dict) else None
        if email_val:
            profile["extracted_email"] = email_val

    if "posts" not in profile and "recentPosts" not in profile:
        profile["posts"] = []

    return profile


def _extract_current_company(profile: dict) -> Optional[str]:
    current = profile.get("currentPosition")
    if isinstance(current, list) and current:
        current = current[0]
    if isinstance(current, dict):
        name = current.get("companyName") or current.get("company") or ""
        if name:
            return name.strip()

    for exp in (profile.get("experience") or [])[:1]:
        end_date = exp.get("end") or exp.get("endDate") or exp.get("timePeriod", {}).get("endDate")
        if not end_date:
            name = exp.get("companyName") or exp.get("company") or ""
            if name:
                return name.strip()

    return profile.get("companyName") or profile.get("company") or None


_LEGAL_SUFFIXES = re.compile(
    r"\b(inc\.?|llc\.?|corp\.?|ltd\.?|gmbh\.?|limited|co\.|co)\b",
    re.IGNORECASE,
)
_PUNCT = re.compile(r"[^\w\s]")


def _normalize_name(name: str) -> str:
    name = name.lower().strip()
    name = _LEGAL_SUFFIXES.sub("", name)
    name = _PUNCT.sub("", name)
    name = re.sub(r"^the\s+", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _compare_company_names(a: str, b: str) -> tuple[bool, float]:
    if not a or not b:
        return (False, 0.0)

    if a == b:
        return (True, 1.0)

    na = _normalize_name(a)
    nb = _normalize_name(b)

    if not na or not nb:
        return (False, 0.0)

    if na == nb:
        return (True, 0.7)

    set_a = set(na.split())
    set_b = set(nb.split())
    if not set_a or not set_b:
        return (False, 0.0)

    intersection = set_a & set_b
    union = set_a | set_b
    jaccard = len(intersection) / len(union)

    if jaccard >= 0.6:
        return (True, 0.5)

    return (False, 0.0)


def _normalize_linkedin_url(url: str) -> str:
    url = url.strip().rstrip("/").lower()
    if "?" in url:
        url = url.split("?")[0]
    url = url.replace("://www.", "://")
    return url


def _scrape_profiles_blocking(urls: list[str], short_mode: bool) -> list[dict]:
    mode = "Profile details no email ($4 per 1k)" if short_mode else "Profile details + email search ($10 per 1k)"
    run_input = {
        "profileScraperMode": mode,
        "queries": urls,
    }
    run = _apify_client.actor(settings.apify_profile_scraper_id).call(run_input=run_input)
    results = []
    for item in _apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(_normalize_profile(item))
    return results


def _match_profiles(urls: list[str], results: list[dict]) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for result in results:
        original_query_url = (result.get("originalQuery") or {}).get("query", "")
        if original_query_url:
            lookup[_normalize_linkedin_url(original_query_url)] = result
        for url_field in ["linkedinUrl", "url", "profileUrl"]:
            url = result.get(url_field, "")
            if url:
                lookup[_normalize_linkedin_url(url)] = result

    matched: dict[str, dict] = {}
    for url in urls:
        normalized = _normalize_linkedin_url(url)
        if normalized in lookup:
            matched[url] = lookup[normalized]
    return matched


async def validate_employer_freshness(
    candidates: list[dict],
    *,
    short_mode: bool = True,
    concurrency: int = 8,
) -> list[FreshnessResult]:
    results: list[FreshnessResult] = []
    with_linkedin: list[dict] = []
    without_linkedin: list[dict] = []

    for c in candidates:
        if c.get("linkedin"):
            with_linkedin.append(c)
        else:
            without_linkedin.append(c)

    for c in without_linkedin:
        results.append(FreshnessResult(
            linkedin_url="",
            apollo_company=c.get("company_name", ""),
            linkedin_current_company=None,
            matches=True,
            confidence=1.0,
            profile_snapshot={},
            skipped=True,
        ))

    if not with_linkedin:
        return results

    urls = [c["linkedin"] for c in with_linkedin]
    logger.info(f"freshness_validator: scraping {len(urls)} LinkedIn profiles")

    try:
        raw_profiles = await asyncio.to_thread(_scrape_profiles_blocking, urls, short_mode)
    except Exception as e:
        logger.error(f"freshness_validator: Apify call failed: {e}", exc_info=True)
        for c in with_linkedin:
            results.append(FreshnessResult(
                linkedin_url=c["linkedin"],
                apollo_company=c.get("company_name", ""),
                linkedin_current_company=None,
                matches=False,
                confidence=0.0,
                profile_snapshot={},
                error=str(e),
            ))
        return results

    matched = _match_profiles(urls, raw_profiles)

    for c in with_linkedin:
        url = c["linkedin"]
        apollo_company = c.get("company_name", "")
        profile = matched.get(url)

        if not profile:
            results.append(FreshnessResult(
                linkedin_url=url,
                apollo_company=apollo_company,
                linkedin_current_company=None,
                matches=False,
                confidence=0.0,
                profile_snapshot={},
                linkedin_url_invalid=True,
            ))
            continue

        current_company = _extract_current_company(profile)
        matches, confidence = _compare_company_names(apollo_company, current_company or "")

        snapshot = {
            "current_company": current_company,
            "current_title": None,
            "headline": profile.get("headline"),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        current_pos = profile.get("currentPosition")
        if isinstance(current_pos, list) and current_pos:
            current_pos = current_pos[0]
        if isinstance(current_pos, dict):
            snapshot["current_title"] = current_pos.get("title") or current_pos.get("position")

        results.append(FreshnessResult(
            linkedin_url=url,
            apollo_company=apollo_company,
            linkedin_current_company=current_company,
            matches=matches,
            confidence=confidence,
            profile_snapshot=snapshot,
        ))

    return results
