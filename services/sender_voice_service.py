"""
Sender Voice Service.
Scrapes the sender's LinkedIn posts via Apify and synthesizes a structured voice profile
for use in AI-generated outreach messages.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from apify_client import ApifyClient
from bson import ObjectId

from config import get_settings
from database import company_profiles_collection
from services.openrouter_service import OpenRouterClient
from utils.prompts import (
    SENDER_VOICE_SYNTHESIS_PROMPT,
    build_sender_voice_synthesis_prompt,
)

logger = logging.getLogger(__name__)

settings = get_settings()
_apify = ApifyClient(settings.apify_api_key)

SYNTHESIS_MODEL = "anthropic/claude-haiku-4-5"
MAX_POSTS_STORED = 25   # cap raw posts stored in company_profile
MIN_POSTS_FOR_SYNTHESIS = 3  # below this, mark as low-confidence


async def scrape_sender_linkedin(linkedin_url: str) -> tuple[list[dict], dict]:
    """
    Scrape the sender's LinkedIn profile and posts via Apify PROFILE_SCRAPER.
    Returns (posts_list, raw_profile_dict).
    """
    logger.info(f"Scraping sender LinkedIn: {linkedin_url}")
    try:
        run_input = {
            "profileScraperMode": "Profile details + email search ($10 per 1k)",
            "queries": [linkedin_url],
        }
        run = await asyncio.to_thread(
            lambda: _apify.actor(settings.apify_profile_scraper_id).call(run_input=run_input)
        )
        results = []
        for item in _apify.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)

        if not results:
            logger.warning(f"No profile data returned for {linkedin_url}")
            return [], {}

        profile = results[0]

        # Extract posts from known field names
        posts = (
            profile.get("posts")
            or profile.get("recentPosts")
            or profile.get("activities")
            or []
        )

        # Normalize post dicts to {text, likes, comments, posted_at}
        normalized_posts = []
        for p in posts:
            if isinstance(p, dict):
                text = p.get("text") or p.get("content") or p.get("commentary", "")
                if text and text.strip():
                    normalized_posts.append({
                        "text": text.strip()[:2000],
                        "likes": p.get("likes") or p.get("numLikes", 0),
                        "comments": p.get("comments") or p.get("numComments", 0),
                        "posted_at": p.get("postedAt") or p.get("date", ""),
                    })

        logger.info(f"Scraped {len(normalized_posts)} posts for {linkedin_url}")
        return normalized_posts[:MAX_POSTS_STORED], profile

    except Exception as e:
        logger.error(f"Failed to scrape sender LinkedIn {linkedin_url}: {e}", exc_info=True)
        return [], {}


async def synthesize_voice_profile(
    posts: list[dict],
    sender_name: str,
    sender_role: str,
    raw_profile: dict,
) -> dict:
    """
    Call Haiku to synthesize a structured voice profile from LinkedIn posts.
    Returns the parsed voice profile dict.
    """
    headline = raw_profile.get("headline") or raw_profile.get("title", "")

    user_prompt = build_sender_voice_synthesis_prompt(
        posts=posts,
        sender_name=sender_name,
        sender_role=sender_role,
        headline=headline,
    )

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": SENDER_VOICE_SYNTHESIS_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=SYNTHESIS_MODEL,
        max_tokens=600,
        temperature=0.3,
    )

    content = (result.get("content") or "").strip()
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]

    try:
        profile = json.loads(content)
    except Exception:
        logger.error(f"Voice synthesis returned non-JSON: {content[:300]}")
        profile = {
            "tone_markers": ["professional"],
            "sentence_patterns": ["direct"],
            "vocab_signature": [],
            "formality_level": "semi-formal",
            "synthesized_summary": f"{sender_name} uses a professional tone in their communications.",
        }

    profile["post_count"] = len(posts)
    profile["source"] = "linkedin_scrape"
    profile["synthesized_at"] = datetime.now(timezone.utc).isoformat()
    if len(posts) < MIN_POSTS_FOR_SYNTHESIS:
        profile["low_confidence"] = True

    return profile


async def update_sender_voice_for_account(
    account_id: str,
    linkedin_url: str,
    sender_name: str,
    sender_role: str,
) -> dict:
    """
    Full pipeline: scrape → synthesize → persist to company_profile.
    Returns the synthesized voice profile.
    """
    posts, raw_profile = await scrape_sender_linkedin(linkedin_url)

    voice_profile = await synthesize_voice_profile(
        posts=posts,
        sender_name=sender_name,
        sender_role=sender_role,
        raw_profile=raw_profile,
    )

    # Persist to company_profile
    await company_profiles_collection.update_one(
        {"account_id": account_id},
        {
            "$set": {
                "sender_linkedin_url": linkedin_url,
                "sender_voice_profile": voice_profile,
                "sender_linkedin_posts": posts[:MAX_POSTS_STORED],
            }
        },
        upsert=False,
    )

    logger.info(
        f"Voice profile synthesized for account {account_id}: "
        f"{len(posts)} posts, tone={voice_profile.get('tone_markers', [])}"
    )
    return voice_profile


async def update_sender_voice_from_unipile(account_id: str) -> dict:
    """
    Pull the user's OWN voice data from Unipile (never Apify).
    Returns {voice_profile, sender_name, sender_role, low_confidence}.
    Falls back to manual tone (low_confidence=True) if no connected account or no data.
    """
    from database import linkedin_accounts_collection
    from services.unipile_service import UnipileClient

    # Resolve tenant-scoped LinkedIn account — do NOT use get_linkedin_account_id() which is global
    li = await linkedin_accounts_collection.find_one(
        {"account_id": account_id},
        sort=[("created_at", -1)],
    )
    if not li or not li.get("public_id"):
        return {"voice_profile": None, "sender_name": "", "sender_role": "", "low_confidence": True}

    public_id = li["public_id"]
    profile_url = li.get("profile_url") or f"https://www.linkedin.com/in/{public_id}"
    client = UnipileClient()

    # Fetch about/headline/experience via Unipile
    norm = await client.get_profile_data_for_enrichment(profile_url) or {}
    # Fetch own posts via Unipile (defensive — returns [] if endpoint unavailable)
    posts = await client.get_user_posts(public_id)

    # Derive name and role
    sender_name = (li.get("name") or "").strip()
    sender_role = (norm.get("headline") or li.get("headline") or "").strip()
    if not sender_role:
        exp = norm.get("experience") or []
        if exp:
            sender_role = (exp[0].get("title") or "").strip()

    raw_profile = {
        "headline": norm.get("headline") or "",
        "about": norm.get("about") or "",
        "experience": norm.get("experience") or [],
    }

    voice_profile = await synthesize_voice_profile(
        posts=posts,
        sender_name=sender_name or "the sender",
        sender_role=sender_role,
        raw_profile=raw_profile,
    )
    voice_profile["source"] = "unipile"

    await company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {
            "sender_name": sender_name,
            "sender_role": sender_role,
            "sender_linkedin_url": profile_url,
            "sender_voice_profile": voice_profile,
            "sender_linkedin_posts": posts[:MAX_POSTS_STORED],
        }},
        upsert=False,
    )

    return {
        "voice_profile": voice_profile,
        "sender_name": sender_name,
        "sender_role": sender_role,
        "low_confidence": bool(voice_profile.get("low_confidence")),
    }


async def resynthesize_voice_profile(account_id: str) -> dict:
    """
    Re-synthesize voice profile using stored raw posts (no re-scrape).
    If no posts are cached yet (e.g. LinkedIn was connected after onboarding, or the
    onboarding voice-pull never ran), fall back to a full pull from the connected
    LinkedIn account via Unipile instead of failing outright.
    """
    profile_doc = await company_profiles_collection.find_one({"account_id": account_id})
    if not profile_doc:
        raise ValueError(f"No company profile found for account {account_id}")

    posts = profile_doc.get("sender_linkedin_posts") or []
    if not posts:
        result = await update_sender_voice_from_unipile(account_id)
        if not result.get("voice_profile"):
            raise ValueError(
                "No LinkedIn account connected — connect LinkedIn, then resync your voice profile."
            )
        return result["voice_profile"]

    sender_name = profile_doc.get("sender_name", "")
    sender_role = profile_doc.get("sender_role", "")

    voice_profile = await synthesize_voice_profile(
        posts=posts,
        sender_name=sender_name,
        sender_role=sender_role,
        raw_profile={},
    )

    await company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {"sender_voice_profile": voice_profile}},
    )

    return voice_profile
