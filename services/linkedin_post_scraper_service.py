"""
LinkedIn post scraper using Apify actor r4oNX7IHlW4RQAjKP.
Scrapes recent posts for a batch of LinkedIn profile URLs.
"""

import asyncio
import logging
import re

from apify_client import ApifyClient
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()
apify_client = ApifyClient(settings.apify_api_key)

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

    loop = asyncio.get_event_loop()
    logger.info(
        f"[post-scraper] CALLING actor {LINKEDIN_POST_SCRAPER_ACTOR_ID} — "
        f"{len(linkedin_urls)} profiles, {posts_per_profile} posts each"
    )

    try:
        run = await loop.run_in_executor(
            None,
            lambda: apify_client.actor(LINKEDIN_POST_SCRAPER_ACTOR_ID).call(
                run_input={
                    "usernames": linkedin_urls,
                    "limit": posts_per_profile,
                    "total_posts": posts_per_profile,
                }
            ),
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
