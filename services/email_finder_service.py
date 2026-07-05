"""
Email finder service using Apify actor bfH8Ermocz8oYKQVO.
Batch LinkedIn email finder — sends all URLs in a single call.
If no email is found the prospect is still created; the UI displays
"Email not available in any GDPR compliant datasource" for those prospects.
"""

import asyncio
import logging
import re

from apify_client import ApifyClient
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()
apify_client = ApifyClient(settings.apify_api_key)

ACTOR_ID = "bfH8Ermocz8oYKQVO"


def _find_emails_sync(linkedin_urls: list[str]) -> dict[str, dict | None]:
    """
    Call the batch email-finder actor once with all URLs.
    Returns dict mapping linkedin_url → result item (with 'email' key) or None.
    """
    results: dict[str, dict | None] = {url: None for url in linkedin_urls}
    try:
        run = apify_client.actor(ACTOR_ID).call(run_input={"urls": linkedin_urls})
        items = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        for item in items:
            url = item.get("linkedin_url")
            if url and item.get("found") and item.get("email"):
                results[url] = item
        found = sum(1 for v in results.values() if v)
        logger.info(f"Email finder: {found}/{len(linkedin_urls)} emails found")
    except Exception as e:
        logger.error(f"Email finder actor failed: {e}")
    return results


async def find_emails_batch(linkedin_urls: list[str], concurrency: int = 3) -> dict[str, dict | None]:
    """
    Find emails for multiple LinkedIn profiles via a single Apify batch call.
    Returns dict mapping linkedin_url → result dict (with 'email') or None.
    The `concurrency` parameter is kept for signature compatibility but is unused
    since the batch actor is one blocking call.
    """
    if not linkedin_urls:
        return {}
    logger.info(f"Finding emails for {len(linkedin_urls)} profiles")
    return await asyncio.to_thread(_find_emails_sync, linkedin_urls)


async def find_email_for_linkedin_profile(linkedin_url: str) -> dict | None:
    """Find email for a single LinkedIn profile."""
    results = await find_emails_batch([linkedin_url])
    return results.get(linkedin_url)


async def find_emails_for_linkedin_urls(
    linkedin_urls: list[str],
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> dict[str, str | None]:
    """Bulk-find emails via the standalone email-finder actor. Returns {url: email_or_None}."""
    if not linkedin_urls:
        return {}

    loop = asyncio.get_event_loop()
    run = await loop.run_in_executor(
        None,
        lambda: apify_client.actor(ACTOR_ID).call(run_input={"urls": linkedin_urls}),
    )

    try:
        from services.apify_service import track_apify_run
        async with track_apify_run(
            ACTOR_ID,
            account_id=account_id,
            campaign_id=campaign_id,
        ) as tracker:
            tracker.set_run_id(run.get("id", ""))
    except Exception:
        pass

    result: dict[str, str | None] = {url: None for url in linkedin_urls}
    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        for item in apify_client.dataset(dataset_id).iterate_items():
            url = item.get("linkedin_url") or item.get("url") or item.get("linkedinUrl")
            email = item.get("email") or item.get("workEmail")
            if url and item.get("found") is False:
                email = None
            if url:
                result[url] = email
    return result


# ── New bulk email-finder (snipercoder/bulk-linkedin-email-finder, ~$0.8/1k) ──────────
# Used ONLY by fast curated discovery (curated_discovery_service.py phase ④+⑥).
# To revert: switch the call in curated_discovery_service.py back to
# find_emails_for_linkedin_urls() and restore the OLD ACTOR comments.
ACTOR_ID_BULK = "ddgw2oGFaH645BFAq"


def _li_slug(url_or_id: str) -> str:
    """Canonicalize a LinkedIn profile URL or bare vanity id to its slug for matching.

    The new actor echoes back the input identifier in 17_Query_linkedin, which may be
    a bare slug even when a full URL was sent — so we match both sides by slug.
    """
    if not url_or_id:
        return ""
    s = url_or_id.strip().lower().rstrip("/")
    m = re.search(r"/in/([^/?#]+)", s)
    return (m.group(1) if m else s).split("?")[0]


async def find_emails_bulk(
    linkedin_urls: list[str],
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> dict[str, str | None]:
    """Bulk email finder via actor ddgw2oGFaH645BFAq (snipercoder/bulk-linkedin-email-finder).

    Returns {input_url: email_or_None} — same shape as find_emails_for_linkedin_urls().

    Output schema (confirmed):
      04_Email          — email address, null when not found
      17_Query_linkedin — echoes the input identifier (may be slug, not full URL)
      02_First_name     — "email not found." sentinel when not found (ignored here)
    """
    if not linkedin_urls:
        return {}

    # Filter out LinkedIn internal-ID URLs (ACw... base64 member IDs).
    # The employee scraper's Short mode returns these; the actor cannot resolve them.
    # Full mode returns proper vanity slugs — only those should be passed here.
    _INTERNAL_ID_RE = re.compile(r"/in/ACw", re.IGNORECASE)
    resolvable = [u for u in linkedin_urls if not _INTERNAL_ID_RE.search(u)]
    skipped = len(linkedin_urls) - len(resolvable)
    if skipped:
        logger.warning(
            f"[bulk-email] skipped {skipped} internal-ID URLs (ACw... format) — "
            "switch employee scraper to Full mode to resolve these"
        )
    if not resolvable:
        logger.warning("[bulk-email] all URLs are internal IDs — nothing to send to actor")
        return {u: None for u in linkedin_urls}

    # Build slug→original-url map for result matching (actor echoes slugs not full URLs).
    slug_to_url: dict[str, str] = {_li_slug(u): u for u in resolvable}
    result: dict[str, str | None] = {u: None for u in linkedin_urls}

    loop = asyncio.get_event_loop()
    logger.info(
        f"[bulk-email] CALLING actor {ACTOR_ID_BULK} — {len(resolvable)} urls; "
        f"sample={resolvable[:2]}"
    )
    # Retry/backoff: actor returns 502 on transient infra issues; retry up to 3 times.
    _BULK_MAX_RETRIES = 3
    _BULK_BACKOFFS = (4, 8, 16)
    run = None
    for _attempt in range(_BULK_MAX_RETRIES):
        try:
            run = await loop.run_in_executor(
                None,
                lambda: apify_client.actor(ACTOR_ID_BULK).call(
                    run_input={"linkedin_url_or_ids": resolvable}
                ),
            )
            break  # success
        except Exception as e:
            _wait = _BULK_BACKOFFS[min(_attempt, len(_BULK_BACKOFFS) - 1)]
            if _attempt < _BULK_MAX_RETRIES - 1:
                logger.warning(
                    f"[bulk-email] actor {ACTOR_ID_BULK} attempt {_attempt + 1}/{_BULK_MAX_RETRIES} "
                    f"failed ({e}), retrying in {_wait}s"
                )
                await asyncio.sleep(_wait)
            else:
                logger.exception(
                    f"[bulk-email] actor {ACTOR_ID_BULK} FAILED after {_BULK_MAX_RETRIES} attempts on "
                    f"{len(resolvable)} urls: {e}"
                )
                return result
    if run is None:
        return result
    logger.info(
        f"[bulk-email] actor run_id={run.get('id')} status={run.get('status')} "
        f"dataset={run.get('defaultDatasetId')}"
    )

    # Cost-tracking parity with find_emails_for_linkedin_urls().
    try:
        from services.apify_service import track_apify_run
        async with track_apify_run(
            ACTOR_ID_BULK,
            account_id=account_id,
            campaign_id=campaign_id,
        ) as tracker:
            tracker.set_run_id(run.get("id", ""))
    except Exception:
        pass

    dataset_id = run.get("defaultDatasetId")
    if dataset_id:
        items = list(apify_client.dataset(dataset_id).iterate_items())
        if items:
            # One-time key verification: read this log after first real run, then remove.
            logger.info(f"[bulk-email] sample item keys={list(items[0].keys())} item={items[0]}")
        for item in items:
            email = item.get("04_Email") or None   # null / "" → not found; ignore "email not found." sentinel
            query = item.get("17_Query_linkedin") or ""
            orig = slug_to_url.get(_li_slug(query))
            if orig and email:
                result[orig] = email

    found = sum(1 for v in result.values() if v)
    logger.info(f"Bulk email finder: {found}/{len(resolvable)} resolvable tried, {found}/{len(linkedin_urls)} total")
    return result
