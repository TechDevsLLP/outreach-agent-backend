"""
Deep company research — one entry point unifying competitor analysis,
news, buying signals, and company-page LinkedIn posts per company.

CONTRACT (other agents build against this exact shape — do not drift):

research = {
    "competitors": [
        {"name", "website_url", "linkedin_url", "differentiation",
         "market_position", "is_best_performer": bool, "why_winning": str|None}
    ],
    "best_performer": {"name": str, "why_winning": str} | None,
    "news": [
        {"title", "url", "published_date", "source", "summary", "sentiment"}
    ],
    "buying_signals": [str],
    "funding": {"latest_round": str|None, "amount": str|None, "date": str|None,
                "investors": [str], "summary": str|None},
    "hiring_signals": [str],   # roles/teams actively being hired = buying intent
    "tech_stack": [str],
    "recent_launches": [str],  # products/features/markets launched, last 12 months
    "company_posts": [
        {"text", "posted_at", "url", "reactions", "comments"}
    ],
    "status": "ok" | "partial" | "failed",  # partial = some sections empty due to error
    "errors": [str],           # which section(s) failed and why
    "fetched_at": datetime,
}

Stored on companies_collection.research (see models/company.py).
All OpenRouter calls are cost-tagged (account_id, campaign_id,
feature="company_research"). Every sub-fetch is fail-soft: an individual
failure yields an empty section (recorded in research["errors"]), never an
exception to the caller. Each Perplexity sub-call has a hard 60s timeout and
ONE retry. Bulk research reuses stored research fresher than 30 days with
status "ok" instead of re-calling.
"""

import asyncio
import logging
from datetime import datetime, timedelta

import database
from services.competitor_research_service import research_competitors
from services.news_research_service import research_company_news_and_signals
from services.openrouter_service import OpenRouterClient

logger = logging.getLogger(__name__)

_RESEARCH_FEATURE = "company_research"
_SUB_CALL_TIMEOUT_SECONDS = 60.0
_RESEARCH_FRESH_DAYS = 30


def _empty_research() -> dict:
    """Fresh research dict matching the module contract (all sections empty)."""
    return {
        "competitors": [],
        "best_performer": None,
        "news": [],
        "buying_signals": [],
        "funding": {
            "latest_round": None,
            "amount": None,
            "date": None,
            "investors": [],
            "summary": None,
        },
        "hiring_signals": [],
        "tech_stack": [],
        "recent_launches": [],
        "company_posts": [],
        "status": "failed",
        "errors": [],
        "fetched_at": datetime.utcnow(),
    }


async def _call_with_retry(coro_factory, section: str, company_name: str, errors: list[str]):
    """Run one research sub-call with a hard timeout and ONE retry.

    coro_factory: zero-arg callable returning a fresh coroutine per attempt.
    Returns the sub-call result, or None after the final failure — which is
    recorded in ``errors`` as "<section>: <error>".
    """
    last_err: Exception | None = None
    for attempt in (1, 2):
        try:
            return await asyncio.wait_for(
                coro_factory(), timeout=_SUB_CALL_TIMEOUT_SECONDS
            )
        except Exception as e:
            last_err = e
            if attempt == 1:
                logger.warning(
                    f"[research] {section} attempt 1 failed for {company_name} "
                    f"({type(e).__name__}: {e}); retrying once"
                )
    errors.append(f"{section}: {type(last_err).__name__}: {last_err}"[:300])
    logger.warning(
        f"[research] {section} failed for {company_name} after retry: {last_err}"
    )
    return None


def _seller_context_from_profile(account_profile: dict | None) -> str | None:
    """Compress the seller's company profile into a short context string used to
    rank the best-performing competitor relative to the service we pitch."""
    if not account_profile:
        return None
    parts = []
    if account_profile.get("company_name"):
        parts.append(str(account_profile["company_name"]))
    if account_profile.get("value_proposition"):
        parts.append(str(account_profile["value_proposition"]))
    services = account_profile.get("services") or []
    if services:
        parts.append("Services: " + ", ".join(str(s) for s in services[:4]))
    return " | ".join(parts) or None


def _derive_best_performer(competitors: list[dict]) -> dict | None:
    """Pick the flagged best performer out of the competitor list."""
    for c in competitors or []:
        if c.get("is_best_performer") and c.get("name"):
            return {
                "name": c["name"],
                "why_winning": c.get("why_winning") or c.get("differentiation") or "",
            }
    return None


async def deep_research_company(
    company: dict,
    account_profile: dict | None,
    *,
    cost_tags: dict,
    posts_by_url: dict[str, list[dict]] | None = None,
    client: OpenRouterClient | None = None,
) -> dict:
    """Run deep research for ONE company and return the contract dict above.

    company: needs at least {"name"}; uses {"domain"/"website", "industry",
        "linkedin_url"} when present.
    account_profile: seller's company profile (company_name, value_proposition,
        services) — used to contextualize the best-performer ranking.
    cost_tags: {"account_id": ..., "campaign_id": ..., "feature": ...} — the
        feature defaults to "company_research" when absent.
    posts_by_url: optional pre-fetched company posts (from
        scrape_company_posts_bulk) keyed by company linkedin_url — pass when
        researching many companies so posts are scraped in one batch.
    client: optional shared OpenRouterClient.
    """
    account_id = cost_tags.get("account_id")
    campaign_id = cost_tags.get("campaign_id")
    feature = cost_tags.get("feature") or _RESEARCH_FEATURE

    name = (company.get("name") or company.get("company_name") or "").strip()
    research: dict = _empty_research()
    if not name:
        research["errors"] = ["missing company name"]
        return research

    domain = company.get("domain") or company.get("website") or company.get("company_domain")
    industry = company.get("industry")
    if isinstance(industry, dict):
        industry = industry.get("label") or industry.get("raw") or ""
    industry = str(industry or "") or None

    seller_context = _seller_context_from_profile(account_profile)

    errors: list[str] = []
    owns_client = client is None
    if owns_client:
        client = OpenRouterClient()
    try:
        # Each Perplexity sub-call gets a hard timeout + ONE retry; failures are
        # recorded per-section in `errors` (raise_on_error=True lets errors
        # surface to the retry wrapper instead of being swallowed downstream).
        competitors_res, news_signals_res = await asyncio.gather(
            _call_with_retry(
                lambda: research_competitors(
                    name,
                    company_website=domain or None,
                    industry=industry,
                    limit=3,
                    client=client,
                    seller_context=seller_context,
                    account_id=str(account_id) if account_id else None,
                    campaign_id=str(campaign_id) if campaign_id else None,
                    feature=feature,
                    raise_on_error=True,
                ),
                "competitors", name, errors,
            ),
            # News + buying signals + funding + hiring + tech + launches folded
            # into one Perplexity call (cost stays flat).
            _call_with_retry(
                lambda: research_company_news_and_signals(
                    name,
                    limit=3,
                    days_back=90,
                    client=client,
                    account_id=str(account_id) if account_id else None,
                    campaign_id=str(campaign_id) if campaign_id else None,
                    feature=feature,
                    raise_on_error=True,
                ),
                "news_signals", name, errors,
            ),
        )

        research["competitors"] = competitors_res or []
        research["best_performer"] = _derive_best_performer(research["competitors"])
        if isinstance(news_signals_res, dict):
            for key in (
                "news", "buying_signals", "funding",
                "hiring_signals", "tech_stack", "recent_launches",
            ):
                if news_signals_res.get(key) is not None:
                    research[key] = news_signals_res[key]
    finally:
        if owns_client:
            await client.close()

    # Company-page LinkedIn posts (fail-soft actor; may be pre-fetched in bulk)
    li_url = (company.get("linkedin_url") or company.get("company_linkedin_url") or "").rstrip("/")
    if posts_by_url is not None:
        research["company_posts"] = (
            posts_by_url.get(li_url) or posts_by_url.get(li_url.lower()) or []
        )
    elif li_url:
        try:
            from services.linkedin_post_scraper_service import scrape_company_posts_bulk
            single = await scrape_company_posts_bulk([li_url], posts_per_company=5)
            research["company_posts"] = single.get(li_url) or []
        except Exception as e:
            errors.append(f"company_posts: {type(e).__name__}: {e}"[:300])
            logger.warning(f"[research] company posts failed for {name} (fail-soft): {e}")

    # Observability: ok = no section errored; partial = some sections empty due
    # to error; failed = both Perplexity sections errored out.
    #
    # Special case — "successful but empty": both calls returned without error
    # yet produced zero substantive content across every section. A real company
    # almost always has at least competitors or some news, so total emptiness
    # signals a soft failure (blank/malformed model response) rather than genuine
    # absence. Mark it "partial" so the 30-day freshness cache does NOT lock the
    # empty result in — it will be retried on the next bulk run instead of being
    # served empty for a month.
    _content_keys = (
        "competitors", "news", "buying_signals", "funding",
        "hiring_signals", "tech_stack", "recent_launches",
    )
    _all_empty = all(not research.get(k) for k in _content_keys)

    if not errors and _all_empty:
        errors.append("all_sections_empty: research returned no content (soft failure suspected)")
        research["errors"] = errors
        research["status"] = "partial"
        logger.warning(f"[research] {name}: all sections empty despite no call error — marking partial for retry")
    elif not errors:
        research["errors"] = errors
        research["status"] = "ok"
    elif competitors_res is None and news_signals_res is None:
        research["errors"] = errors
        research["status"] = "failed"
    else:
        research["errors"] = errors
        research["status"] = "partial"

    return research


async def deep_research_companies_bulk(
    companies_by_url: dict[str, dict],
    account_profile: dict | None,
    *,
    cost_tags: dict,
    max_concurrency: int = 8,
    persist: bool = True,
    scrape_posts: bool = True,
) -> dict[str, dict]:
    """Deep-research many companies. Returns {company_linkedin_url: research}.

    companies_by_url: {linkedin_url: {"name", "domain", "industry", ...}}.
    When persist=True the research dict is upserted onto
    companies_collection.research keyed by linkedin_url (contract shape).
    Freshness skip: companies whose stored research is fresher than
    _RESEARCH_FRESH_DAYS with status "ok" are returned from cache without any
    new calls. Company posts are scraped in ONE bulk pass first (fail-soft),
    then the Perplexity research runs with bounded concurrency.
    """
    if not companies_by_url:
        return {}

    # 0) Freshness skip — reuse stored research < 30 days old with status "ok".
    cached_by_url: dict[str, dict] = {}
    try:
        fresh_cutoff = datetime.utcnow() - timedelta(days=_RESEARCH_FRESH_DAYS)
        all_urls = [u for u in companies_by_url.keys() if u]
        if all_urls:
            async for co in database.companies_collection.find(
                {
                    "linkedin_url": {"$in": all_urls},
                    "research.status": "ok",
                    "research.fetched_at": {"$gte": fresh_cutoff},
                },
                {"linkedin_url": 1, "research": 1},
            ):
                url = co.get("linkedin_url")
                if url in companies_by_url and co.get("research"):
                    cached_by_url[url] = co["research"]
        if cached_by_url:
            logger.info(
                f"[research] reusing fresh research for {len(cached_by_url)}/"
                f"{len(companies_by_url)} companies (<{_RESEARCH_FRESH_DAYS}d, status=ok)"
            )
    except Exception as e:
        logger.warning(f"[research] freshness check failed (fail-soft, re-researching all): {e}")
        cached_by_url = {}

    to_research = {u: c for u, c in companies_by_url.items() if u not in cached_by_url}
    if not to_research:
        return dict(cached_by_url)

    # 1) Bulk company posts first (fail-soft — never blocks research)
    posts_by_url: dict[str, list[dict]] = {}
    if scrape_posts:
        try:
            from services.linkedin_post_scraper_service import scrape_company_posts_bulk
            urls = [u for u in to_research.keys() if u]
            posts_by_url = await scrape_company_posts_bulk(urls, posts_per_company=5)
        except Exception as e:
            logger.warning(f"[research] bulk company post scrape failed (fail-soft): {e}")

    sem = asyncio.Semaphore(max(1, max_concurrency))

    async def _one(li_url: str, co: dict) -> tuple[str, dict]:
        async with sem:
            client = OpenRouterClient()
            try:
                co_in = dict(co)
                co_in.setdefault("linkedin_url", li_url)
                research = await deep_research_company(
                    co_in,
                    account_profile,
                    cost_tags=cost_tags,
                    posts_by_url=posts_by_url,
                    client=client,
                )
            except Exception as e:
                logger.warning(f"[research] deep research failed for {li_url}: {e}")
                research = _empty_research()
                research["errors"] = [f"deep_research: {type(e).__name__}: {e}"[:300]]
            finally:
                await client.close()
            return li_url, research

    results = await asyncio.gather(
        *[_one(u, c) for u, c in to_research.items()]
    )
    research_by_url = {u: r for u, r in results}

    if persist:
        try:
            from pymongo import UpdateOne
            ops = [
                UpdateOne(
                    {"linkedin_url": li_url},
                    {"$set": {"research": research}},
                    upsert=True,
                )
                for li_url, research in research_by_url.items()
                if li_url
            ]
            if ops:
                await database.companies_collection.bulk_write(ops, ordered=False)
        except Exception as e:
            logger.warning(f"[research] persist to companies_collection failed: {e}")

    # Merge cached entries back into the returned map (disjoint keys).
    research_by_url.update(cached_by_url)
    return research_by_url
