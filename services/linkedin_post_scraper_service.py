"""
LinkedIn post scraper using Apify actor r4oNX7IHlW4RQAjKP.
Scrapes recent posts for a batch of LinkedIn profile URLs.
"""

import asyncio
import logging
import re
import threading

from apify_client import ApifyClient
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


class _LazyApifyClient:
    """Construct the synchronous Apify client only on first dataset access."""

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

LINKEDIN_POST_SCRAPER_ACTOR_ID = settings.apify_post_scraper_actor_id


def _li_slug(url: str) -> str:
    """Extract the vanity slug from a LinkedIn profile URL."""
    if not url:
        return ""
    m = re.search(r"/in/([^/?#]+)", url.lower())
    return m.group(1).rstrip("/") if m else url.strip().rstrip("/").lower()


async def scrape_linkedin_posts_bulk(
    linkedin_urls: list[str],
    posts_per_profile: int = 5,
) -> dict[str, list[dict]]:
    """
    Scrapes up to posts_per_profile posts for each LinkedIn URL.
    Returns dict mapping linkedin_url -> list of post dicts.
    Each post dict has: text, posted_at, stats (reactions, comments), url, post_type.
    Missing/private profiles return an empty list.
    """
    if not linkedin_urls:
        return {}

    result: dict[str, list[dict]] = {url: [] for url in linkedin_urls}

    # Build slug→original-url map for result matching
    slug_to_url: dict[str, str] = {_li_slug(u): u for u in linkedin_urls}

    logger.info(
        f"[post-scraper] CALLING actor {LINKEDIN_POST_SCRAPER_ACTOR_ID} — "
        f"{len(linkedin_urls)} profiles, {posts_per_profile} posts each"
    )

    try:
        from services.apify_service import call_actor_with_retry
        run = await call_actor_with_retry(
            LINKEDIN_POST_SCRAPER_ACTOR_ID,
            {
                "usernames": linkedin_urls,
                "limit": posts_per_profile,
                "total_posts": posts_per_profile,
            },
        )
    except Exception as e:
        logger.exception(
            f"[post-scraper] actor {LINKEDIN_POST_SCRAPER_ACTOR_ID} FAILED: {e}"
        )
        return result

    logger.info(
        f"[post-scraper] run_id={run.get('id')} status={run.get('status')} "
        f"dataset={run.get('defaultDatasetId')}"
    )

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return result

    try:
        from services.apify_service import track_apify_run
        async with track_apify_run(LINKEDIN_POST_SCRAPER_ACTOR_ID) as tracker:
            tracker.set_run_id(run.get("id", ""))
    except Exception:
        pass

    items = list(apify_client.dataset(dataset_id).iterate_items())
    logger.info(f"[post-scraper] {len(items)} items returned")

    for item in items:
        # Match by profile_input — the actor echoes our exact input URL here.
        # This is the only reliable match key because reposts have a different author.
        profile_input = (item.get("profile_input") or "").strip().rstrip("/")
        if profile_input in result:
            orig_url = profile_input
        else:
            # Fall back: match by slug on author.profile_url
            orig_url = None
            author = item.get("author") or {}
            if isinstance(author, dict):
                author_profile_url = author.get("profile_url") or ""
                slug = _li_slug(str(author_profile_url))
                if slug:
                    orig_url = slug_to_url.get(slug)
            if not orig_url:
                slug = _li_slug(profile_input)
                orig_url = slug_to_url.get(slug)

        if not orig_url:
            continue

        # Extract using the actor's real field names (verified against live dataset)
        posted_at_raw = item.get("posted_at") or {}
        if isinstance(posted_at_raw, dict):
            posted_at = posted_at_raw.get("date") or posted_at_raw.get("timestamp")
        else:
            posted_at = posted_at_raw

        stats_raw = item.get("stats") or {}
        if isinstance(stats_raw, dict):
            reactions = stats_raw.get("total_reactions") or stats_raw.get("like") or 0
            comments = stats_raw.get("comments") or 0
        else:
            reactions = 0
            comments = 0

        post = {
            "text": (item.get("text") or "").strip(),
            "posted_at": posted_at,
            "stats": {
                "reactions": reactions,
                "comments": comments,
            },
            "url": item.get("url") or "",
            "post_type": item.get("post_type") or "post",
        }

        if post["text"]:
            result[orig_url].append(post)

    found = sum(1 for posts in result.values() if posts)
    logger.info(f"[post-scraper] {found}/{len(linkedin_urls)} profiles had posts")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Company-page posts (separate actor — the member actor rejects /company/ URLs)
# ──────────────────────────────────────────────────────────────────────────────

COMPANY_POSTS_ACTOR_ID = settings.apify_company_posts_actor_id


def _company_slug(url: str) -> str:
    """Extract the company slug from a LinkedIn company URL."""
    if not url:
        return ""
    m = re.search(r"/company/([^/?#]+)", url.lower())
    return m.group(1).rstrip("/") if m else ""


def _normalize_company_post_item(item: dict) -> dict | None:
    """Normalize one harvestapi dataset item to the research contract post shape:
    {text, posted_at, url, reactions, comments}. Returns None for empty/non-post items."""
    if not isinstance(item, dict):
        return None
    # Reactions/comments come back as separate dataset items when enabled; we
    # disable them, but guard anyway.
    if item.get("type") not in (None, "post"):
        return None
    text = item.get("content") or item.get("text") or item.get("commentary") or ""
    if isinstance(text, dict):
        text = text.get("text") or ""
    text = str(text).strip()
    if not text:
        return None

    posted_raw = item.get("postedAt") or item.get("posted_at") or item.get("date") or ""
    if isinstance(posted_raw, dict):
        posted_at = posted_raw.get("date") or posted_raw.get("timestamp") or ""
    else:
        posted_at = posted_raw

    engagement = item.get("engagement") or {}
    if isinstance(engagement, dict) and engagement:
        reactions = engagement.get("likes") or 0
        comments = engagement.get("comments") or 0
    else:
        stats = item.get("stats") or {}
        reactions = stats.get("total_reactions") or item.get("reactions") or item.get("likes") or 0
        comments = stats.get("comments") or item.get("comments_count") or 0

    return {
        "text": text[:1000],
        "posted_at": posted_at,
        "url": item.get("linkedinUrl") or item.get("url") or item.get("post_url") or "",
        "reactions": reactions,
        "comments": comments,
    }


def _company_post_author_slug(item: dict) -> str:
    """Company slug of the item's author, for matching posts back to input URLs."""
    author = item.get("author") or {}
    if not isinstance(author, dict):
        return ""
    slug = _company_slug(str(author.get("linkedinUrl") or ""))
    if slug:
        return slug
    for key in ("universalName", "publicIdentifier"):
        val = author.get(key)
        if val:
            return str(val).strip().lower()
    return ""


async def scrape_company_posts_bulk(
    company_urls: list[str],
    posts_per_company: int = 5,
) -> dict[str, list[dict]]:
    """
    Scrape recent COMPANY-PAGE posts for a batch of LinkedIn company URLs using
    the harvestapi LinkedIn Profile Posts actor WI0tj4Ieb5Kq458gB
    (config: apify_company_posts_actor_id). One bulk run — the actor takes
    `targetUrls` (profile or company URLs) and scrapes 6 targets concurrently.

    Fail-soft by design: any actor/input/parse error yields empty lists
    (logged) — the research pipeline must never break on this.

    Returns {input_company_url: [{text, posted_at, url, reactions, comments}, ...]}.
    """
    result: dict[str, list[dict]] = {url: [] for url in company_urls or []}
    if not company_urls:
        return result

    slug_to_url = {_company_slug(u): u for u in company_urls if _company_slug(u)}

    logger.info(
        f"[company-post-scraper] CALLING actor {COMPANY_POSTS_ACTOR_ID} — "
        f"{len(company_urls)} companies, {posts_per_company} posts each"
    )

    try:
        from services.apify_service import call_actor_with_retry
        run = await call_actor_with_retry(
            COMPANY_POSTS_ACTOR_ID,
            {
                "targetUrls": company_urls,
                "maxPosts": posts_per_company,
                "includeQuotePosts": True,
                "includeReposts": False,
                "scrapeReactions": False,
                "scrapeComments": False,
            },
        )
    except Exception as e:
        logger.warning(
            f"[company-post-scraper] actor {COMPANY_POSTS_ACTOR_ID} failed "
            f"(fail-soft, returning no posts): {e}"
        )
        return result

    try:
        from services.apify_service import track_apify_run
        async with track_apify_run(COMPANY_POSTS_ACTOR_ID) as tracker:
            tracker.set_run_id(run.get("id", ""))
    except Exception:
        pass

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        return result

    try:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(
            None, lambda: list(apify_client.dataset(dataset_id).iterate_items())
        )
    except Exception as e:
        logger.warning(f"[company-post-scraper] dataset read failed (fail-soft): {e}")
        return result

    for item in items:
        post = _normalize_company_post_item(item)
        if not post:
            continue
        orig_url = slug_to_url.get(_company_post_author_slug(item))
        if not orig_url or len(result[orig_url]) >= posts_per_company:
            continue
        result[orig_url].append(post)

    found = sum(1 for posts in result.values() if posts)
    logger.info(f"[company-post-scraper] {found}/{len(company_urls)} companies had posts")
    return result
