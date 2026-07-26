"""
Prospect intelligence generation — enriches prospects with deep insights from
LinkedIn posts + profile + company data for hyper-personalized outreach.

Hybrid storage model:
  - generate_base_intelligence_batch()  → tenant-agnostic intel → prospects.prospect_intelligence_base
  - generate_pitch_batch()              → tenant-specific pitch → prospect_state.pitch (per account)
  - store_base_intelligence()           → writes prospect_intelligence_base on prospect doc
  - store_pitch_for_account()           → writes pitch overlay on prospect_state doc

generate_prospect_intelligence_batch() is a DEPRECATED shim kept for existing callers.
"""

import asyncio
import logging
from datetime import datetime, timezone

from bson import ObjectId

import database
from config import get_settings
from services.openrouter_service import OpenRouterClient
from utils.prompts import _format_funding_line

logger = logging.getLogger(__name__)
settings = get_settings()

# ---------------------------------------------------------------------------
# Schemas (documentation + type hints)
# ---------------------------------------------------------------------------

# Base intelligence — tenant-agnostic, stored on prospects.prospect_intelligence_base
PROSPECT_INTELLIGENCE_SCHEMA = {
    "writing_voice": str,       # e.g. "analytical, bullet-heavy, ~300 words, professional"
    "top_topics": list,         # e.g. ["GEO", "AI search", "SEO agencies"]
    "pain_signals": list,       # e.g. ["paid ads cost", "AI search gap"]
    "best_hook": str,           # specific post reference to open outreach with
    "competitors": list,        # e.g. ["Victorious", "Siege Media"]
    "dont_pitch": list,         # things to avoid in outreach
    "engagement_style": str,    # e.g. "responds with long-form comments"
}

# Pitch overlay — tenant-specific, stored on prospect_state.pitch per account
PROSPECT_PITCH_SCHEMA = {
    "pitch_angle": str,         # how to position THE OFFER for this specific person
    "why_they_need_us": str,    # specific reason based on their content + our offer
    # Hyper-personalized free-value CTA — a named, zero-commitment asset derived
    # from company research (competitors/best performer/news/buying signals).
    # e.g. {"asset_type": "teardown", "asset_name": "5-point teardown of how
    # Acme wins organic traffic vs {company}", "cta_line": "Worth sending over?"}
    "free_value_cta": {
        "asset_type": str,      # "teardown" | "audit" | "playbook" | "deck" | "benchmark"
        "asset_name": str,      # specific, named asset (references real research facts)
        "cta_line": str,        # single-question yes ask, e.g. "Worth sending over?"
    },
}

_INTELLIGENCE_BATCH_SIZE = 5

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

_INTELLIGENCE_SYSTEM_PROMPT = """You are an elite B2B sales intelligence analyst.

Given LinkedIn posts, profile, and company data for each prospect, generate a concise intelligence object
that enables hyper-personalized outreach. Focus on SPECIFICS — exact post references, real pain signals,
concrete hook ideas. Do not genericize.

Return a JSON object: {"intelligence": [<one object per prospect in same order as input>]}

Each intelligence object must have these exact keys:
- writing_voice: 1-sentence description of their communication style (length, tone, format, vocabulary)
- top_topics: array of 2-4 topics they post/care about most
- pain_signals: array of 1-3 specific pains evident from their content or role
- best_hook: ONE specific signal to open outreach with (quote a post directly if posts exist)
- competitors: array of competitor names mentioned or implied in their posts/profile (empty array if none)
- dont_pitch: array of topics/angles to avoid (e.g. if they're clearly anti-cold-email, list that)
- engagement_style: brief description of how they engage (lurker, commenter, poster, debater, etc.)

Note: pitch_angle and why_they_need_us are NOT included here — they are generated separately per seller.

CRITICAL — ALWAYS return a fully-populated object for EVERY prospect. Never return an
empty object, null, or empty strings for writing_voice / best_hook / top_topics /
pain_signals / engagement_style. Posts are ONE input, not a requirement.

When a prospect has NO posts ("Recent Posts: none available"), build every field from
the remaining signals — job title, seniority, headline, About text, industry, company
description/specialties, and the injected company research (recent news, competitors,
best-performing competitor, buying signals, funding, hiring signals, company posts):
- best_hook: derive from the strongest available non-post signal — their exact
  title/role and its mandate, a recent company news item or buying signal, a funding
  round, a hiring push, or their headline. Reference a concrete fact, never generic.
- top_topics: infer from title, function, headline/About, and company focus.
- pain_signals: infer from the role's typical mandate + company situation (e.g. a
  "Digital Optimization Manager" at a large enterprise → driving digital transformation
  across complex legacy processes).
- writing_voice: infer a plausible professional tone from headline/About wording; if
  there is genuinely nothing to infer from, use "Professional — inferred from profile
  (no posts available)". Do NOT leave it blank.
- engagement_style: if no posts, use "No recent posts — profile-based inference".
Only competitors and dont_pitch may be empty arrays when there is truly no signal."""

_PITCH_SYSTEM_PROMPT = """You are a B2B outreach strategist.

Given base intelligence summaries for prospects (plus company research: competitors, the best-performing
competitor and why it's winning, recent news, buying signals) and a seller's context, generate a pitch
overlay for each prospect.

Return a JSON object: {"pitches": [<one object per prospect in same order as input>]}

Each pitch object must have these exact keys:
- pitch_angle: 1-sentence recommendation for how to position the seller's offer for THIS specific person
- why_they_need_us: 1-2 sentences, specific to their content, role, and pain signals — no generic claims
- free_value_cta: an object {"asset_type", "asset_name", "cta_line"} describing ONE free, zero-commitment
  asset the seller can offer this prospect. Rules:
  * asset_type: one of "teardown" | "audit" | "playbook" | "deck" | "benchmark"
  * asset_name: SPECIFIC and NAMED, built from the research facts — reference the best-performing
    competitor, a buying signal, or a concrete news item. Good: "5-point teardown of how {best
    competitor} wins {category} traffic vs {company}". Bad: "a free consultation", "some ideas".
  * The asset must be obviously valuable to THIS person's role and require nothing from them to receive.
  * cta_line: ONE short question inviting a simple yes — e.g. "Worth sending over?" — never a meeting ask.
  If the research is too thin to name a specific asset, still produce the best possible named asset from
  the prospect's role + industry (never return an empty free_value_cta)."""

# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_base_intelligence_prompt(prospects: list[dict]) -> str:
    """Build a batch prompt for up to 5 prospects (no seller context)."""
    parts = []
    parts.append(f"## Prospects ({len(prospects)} total)")
    parts.append("")

    for i, p in enumerate(prospects, 1):
        parts.append(f"### Prospect {i}")
        parts.append(f"Name: {p.get('full_name') or 'Unknown'}")
        parts.append(f"Title: {p.get('job_title') or p.get('title') or 'Unknown'}")
        parts.append(f"Company: {p.get('company_name') or 'Unknown'}")
        parts.append(f"Industry: {p.get('industry') or 'Unknown'}")
        parts.append(f"Seniority: {p.get('seniority') or p.get('seniority_level') or 'Unknown'}")

        profile = p.get("linkedin_profile_data") or {}
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        if profile.get("summary") or profile.get("about"):
            about = (profile.get("summary") or profile.get("about") or "")[:300]
            parts.append(f"About: {about}")

        company_data = p.get("company_linkedin_data") or {}
        if company_data.get("description"):
            parts.append(f"Company Description: {str(company_data['description'])[:200]}")
        if company_data.get("specialties"):
            specs = company_data["specialties"]
            if isinstance(specs, list):
                parts.append(f"Company Specialties: {', '.join(str(s) for s in specs[:5])}")

        # ── Injected company research (news / competitors / best performer /
        # buying signals / company posts) — see company_research_service contract.
        research = p.get("company_research") or {}
        news = p.get("company_news") or research.get("news") or []
        if news:
            parts.append("Recent Company News:")
            for item in news[:3]:
                if not isinstance(item, dict):
                    continue
                title = item.get("title") or ""
                summary = (item.get("summary") or "")[:150]
                published = item.get("published_date") or ""
                if title:
                    parts.append(f"  - [{published}] {title}" + (f": {summary}" if summary else ""))

        competitors = p.get("company_competitors") or research.get("competitors") or []
        if competitors:
            comp_bits = []
            for c in competitors[:3]:
                if isinstance(c, dict) and c.get("name"):
                    diff = (c.get("differentiation") or "")[:80]
                    comp_bits.append(f"{c['name']}" + (f" ({diff})" if diff else ""))
                elif isinstance(c, str):
                    comp_bits.append(c)
            if comp_bits:
                parts.append(f"Known Competitors: {'; '.join(comp_bits)}")

        best = research.get("best_performer") or None
        if isinstance(best, dict) and best.get("name"):
            why = (best.get("why_winning") or "")[:200]
            parts.append(
                f"Best-Performing Competitor: {best['name']}"
                + (f" — why winning: {why}" if why else "")
            )

        buying_signals = research.get("buying_signals") or []
        if buying_signals:
            parts.append(
                "Company Buying Signals: "
                + "; ".join(str(s)[:120] for s in buying_signals[:4])
            )

        funding_line = _format_funding_line(research.get("funding"))
        if funding_line:
            parts.append(f"Company Funding: {funding_line}")

        hiring_signals = research.get("hiring_signals") or []
        if hiring_signals:
            parts.append(
                "Company Hiring Signals (buying intent): "
                + "; ".join(str(h)[:100] for h in hiring_signals[:4])
            )

        company_posts = research.get("company_posts") or []
        if company_posts:
            parts.append(f"Recent COMPANY-page Posts ({len(company_posts)} total):")
            for cpost in company_posts[:3]:
                if not isinstance(cpost, dict):
                    continue
                ctext = (cpost.get("text") or "").strip()
                if ctext:
                    parts.append(
                        f'  [{cpost.get("posted_at") or ""}] '
                        f'[{cpost.get("reactions", 0)} reactions] "{ctext[:200]}"'
                    )

        posts = p.get("posts") or []
        if posts:
            parts.append(f"Recent Posts ({len(posts)} total):")
            for post in posts[:5]:
                text = (post.get("text") or "").strip()
                if text:
                    reactions = (post.get("stats") or {}).get("reactions", 0)
                    posted = post.get("posted_at") or ""
                    parts.append(f'  [{posted}] [{reactions} reactions] "{text[:300]}"')
        else:
            parts.append("Recent Posts: none available")

        parts.append("")

    parts.append(f"Generate intelligence for all {len(prospects)} prospects in the order listed above.")
    return "\n".join(parts)


def _build_pitch_batch_prompt(
    prospects_with_intel: list[dict],
    account_profile: dict,
) -> str:
    """Build a pitch prompt using base intel summaries + seller context."""
    parts = []

    seller_name = (account_profile or {}).get("company_name") or ""
    seller_vp = (account_profile or {}).get("value_proposition") or ""
    seller_services = (account_profile or {}).get("services") or []

    parts.append("## Seller Context")
    if seller_name:
        parts.append(f"Company: {seller_name}")
    if seller_vp:
        parts.append(f"Value Proposition: {seller_vp}")
    if seller_services:
        parts.append(f"Services: {', '.join(str(s) for s in seller_services[:4])}")
    parts.append("")

    parts.append(f"## Prospects ({len(prospects_with_intel)} total)")
    parts.append("")

    for i, p in enumerate(prospects_with_intel, 1):
        parts.append(f"### Prospect {i}")
        parts.append(f"Name: {p.get('full_name') or 'Unknown'}")
        parts.append(f"Title: {p.get('job_title') or p.get('title') or 'Unknown'}")
        parts.append(f"Company: {p.get('company_name') or 'Unknown'}")

        base = p.get("prospect_intelligence_base") or {}
        if base.get("pain_signals"):
            parts.append(f"Pain Signals: {', '.join(str(x) for x in base['pain_signals'])}")
        if base.get("top_topics"):
            parts.append(f"Top Topics: {', '.join(str(x) for x in base['top_topics'])}")
        if base.get("best_hook"):
            parts.append(f"Best Hook: {base['best_hook']}")
        if base.get("dont_pitch"):
            parts.append(f"Avoid: {', '.join(str(x) for x in base['dont_pitch'])}")

        # Company research context — fuels the named free_value_cta asset
        research = p.get("company_research") or {}
        best = research.get("best_performer")
        if isinstance(best, dict) and best.get("name"):
            why = (best.get("why_winning") or "")[:200]
            parts.append(
                f"Best-Performing Competitor: {best['name']}"
                + (f" — {why}" if why else "")
            )
        buying_signals = research.get("buying_signals") or []
        if buying_signals:
            parts.append("Buying Signals: " + "; ".join(str(s)[:100] for s in buying_signals[:3]))
        funding_line = _format_funding_line(research.get("funding"))
        if funding_line:
            parts.append(f"Funding: {funding_line}")
        hiring_signals = research.get("hiring_signals") or []
        if hiring_signals:
            parts.append(
                "Hiring Now (buying intent): "
                + "; ".join(str(h)[:100] for h in hiring_signals[:3])
            )
        news = research.get("news") or p.get("company_news") or []
        if news:
            headlines = [n.get("title", "") for n in news[:2] if isinstance(n, dict) and n.get("title")]
            if headlines:
                parts.append("Recent News: " + "; ".join(headlines))

        parts.append("")

    parts.append(f"Generate pitch overlays for all {len(prospects_with_intel)} prospects in the order listed above.")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Core generation functions
# ---------------------------------------------------------------------------

async def generate_base_intelligence_batch(
    prospects: list[dict],
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Generate tenant-agnostic prospect intelligence for a list of prospects.
    Each prospect dict should have: linkedin_profile_data, company_linkedin_data, posts.
    Returns list of intelligence dicts (PROSPECT_INTELLIGENCE_SCHEMA) in same order as input.
    Missing/failed prospects get an empty dict at their position.
    Store results with store_base_intelligence().
    """
    if not prospects:
        return []

    client = OpenRouterClient()
    results: list[dict] = [{}] * len(prospects)

    try:
        for batch_start in range(0, len(prospects), _INTELLIGENCE_BATCH_SIZE):
            batch = prospects[batch_start: batch_start + _INTELLIGENCE_BATCH_SIZE]
            batch_results = await _process_base_intelligence_batch(
                client, batch, account_id=account_id, campaign_id=campaign_id
            )
            for j, intel in enumerate(batch_results):
                results[batch_start + j] = intel
    finally:
        await client.close()

    return results


async def _process_base_intelligence_batch(
    client: OpenRouterClient,
    batch: list[dict],
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[dict]:
    """Process one batch of up to 5 prospects for base intelligence."""
    prompt = _build_base_intelligence_prompt(batch)

    try:
        resp = await client.chat_completion(
            messages=[
                {"role": "system", "content": _INTELLIGENCE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            model=settings.outreach_model,
            temperature=0.3,
            max_tokens=max(1500, 400 * len(batch)),
            response_format={"type": "json_object"},
            account_id=account_id,
            campaign_id=campaign_id,
            feature="prospect_intelligence",
        )
    except Exception as e:
        logger.warning(f"[intelligence] base batch call failed: {e}")
        return [{}] * len(batch)

    if not isinstance(resp, dict):
        logger.warning(f"[intelligence] unexpected response type: {type(resp)}")
        return [{}] * len(batch)

    intel_arr = resp.get("intelligence")
    if not isinstance(intel_arr, list):
        intel_arr = [resp] if len(batch) == 1 else [{}] * len(batch)

    while len(intel_arr) < len(batch):
        intel_arr.append({})
    return intel_arr[:len(batch)]


async def generate_pitch_batch(
    prospects_with_base: list[dict],
    account_profile: dict,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[dict]:
    """
    Generate tenant-specific pitch overlay for prospects that already have base intelligence.
    prospects_with_base: prospect dicts, each may have prospect_intelligence_base populated.
    account_profile: seller's company profile dict (company_name, value_proposition, services).
    Returns list of {pitch_angle, why_they_need_us} dicts in same order as input.
    Missing/failed entries get an empty dict at their position.
    Store results with store_pitch_for_account().
    """
    if not prospects_with_base:
        return []

    client = OpenRouterClient()
    results: list[dict] = [{}] * len(prospects_with_base)

    try:
        for batch_start in range(0, len(prospects_with_base), _INTELLIGENCE_BATCH_SIZE):
            batch = prospects_with_base[batch_start: batch_start + _INTELLIGENCE_BATCH_SIZE]
            batch_results = await _process_pitch_batch(
                client, batch, account_profile,
                account_id=account_id, campaign_id=campaign_id,
            )
            for j, pitch in enumerate(batch_results):
                results[batch_start + j] = pitch
    finally:
        await client.close()

    return results


async def _process_pitch_batch(
    client: OpenRouterClient,
    batch: list[dict],
    account_profile: dict,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[dict]:
    """Process one batch of up to 5 prospects for pitch generation."""
    prompt = _build_pitch_batch_prompt(batch, account_profile)

    try:
        resp = await client.chat_completion(
            messages=[
                {"role": "system", "content": _PITCH_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            model=settings.outreach_model,
            temperature=0.4,
            max_tokens=max(600, 150 * len(batch)),
            response_format={"type": "json_object"},
            account_id=account_id,
            campaign_id=campaign_id,
            feature="prospect_pitch",
        )
    except Exception as e:
        logger.warning(f"[intelligence] pitch batch call failed: {e}")
        return [{}] * len(batch)

    if not isinstance(resp, dict):
        logger.warning(f"[intelligence] pitch unexpected response type: {type(resp)}")
        return [{}] * len(batch)

    pitch_arr = resp.get("pitches")
    if not isinstance(pitch_arr, list):
        pitch_arr = [resp] if len(batch) == 1 else [{}] * len(batch)

    while len(pitch_arr) < len(batch):
        pitch_arr.append({})
    return pitch_arr[:len(batch)]


# ---------------------------------------------------------------------------
# Storage functions
# ---------------------------------------------------------------------------

async def store_base_intelligence(
    prospect_ids: list[ObjectId],
    intelligence_list: list[dict],
) -> None:
    """
    Persist base intelligence onto each prospect doc in MongoDB.
    Writes to prospects.prospect_intelligence_base (tenant-agnostic).
    A failed generation bumps intel_attempts + intel_last_failed_at instead of
    being silently skipped, so callers (e.g. backfill_missing_intelligence) can
    stop re-paying for Apify scrape + AI calls on prospects that keep failing.
    A success resets the counter.
    """
    now = datetime.now(timezone.utc)
    for pid, intel in zip(prospect_ids, intelligence_list):
        try:
            if intel:
                await database.prospects_collection.update_one(
                    {"_id": pid},
                    {
                        "$set": {"prospect_intelligence_base": intel},
                        "$unset": {"intel_attempts": "", "intel_last_failed_at": ""},
                    },
                )
            else:
                await database.prospects_collection.update_one(
                    {"_id": pid},
                    {
                        "$inc": {"intel_attempts": 1},
                        "$set": {"intel_last_failed_at": now},
                    },
                )
        except Exception as e:
            logger.warning(f"[intelligence] failed to store base intel for prospect {pid}: {e}")


async def store_pitch_for_account(
    prospect_ids: list[ObjectId],
    pitch_list: list[dict],
    account_id,
) -> None:
    """
    Persist pitch overlay for a specific account onto prospect_state docs.
    Writes to prospect_state.pitch (tenant-specific overlay).
    Uses upsert so existing prospect_state docs are updated non-destructively.
    Silently skips missing/failed entries.
    """
    now = datetime.now(timezone.utc)
    for pid, pitch in zip(prospect_ids, pitch_list):
        if not pitch:
            continue
        try:
            # Refresh denormalized list-filter keys too (the upsert may create
            # the state row, which must carry pk for routes/prospects.py filters).
            from utils.prospect_filter_keys import fetch_filter_keys
            set_fields = {"pitch": pitch, "last_updated_at": now}
            try:
                pk = await fetch_filter_keys(pid)
                if pk is not None:
                    set_fields["pk"] = pk
            except Exception:
                pass
            await database.prospect_state_collection.update_one(
                {"account_id": str(account_id), "prospect_id": str(pid)},
                {
                    "$set": set_fields,
                    "$setOnInsert": {
                        "status": "new",
                        "used_by": [],
                        "tags": [],
                        "created_at": now,
                    },
                },
                upsert=True,
            )
        except Exception as e:
            logger.warning(f"[intelligence] failed to store pitch for prospect {pid} / account {account_id}: {e}")


# ---------------------------------------------------------------------------
# DEPRECATED — kept for existing callers
# ---------------------------------------------------------------------------

async def store_intelligence_for_prospects(
    prospect_ids: list[ObjectId],
    intelligence_list: list[dict],
) -> None:
    """DEPRECATED — use store_base_intelligence() instead."""
    logger.warning(
        "store_intelligence_for_prospects is deprecated; use store_base_intelligence()"
    )
    await store_base_intelligence(prospect_ids, intelligence_list)


async def generate_prospect_intelligence_batch(
    prospects: list[dict],
    account_profile: dict,
) -> list[dict]:
    """DEPRECATED — use generate_base_intelligence_batch() + generate_pitch_batch() instead."""
    logger.warning(
        "generate_prospect_intelligence_batch is deprecated; "
        "use generate_base_intelligence_batch + generate_pitch_batch"
    )
    return await generate_base_intelligence_batch(prospects)
