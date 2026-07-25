"""
Sender Voice Service.
Pulls the sender's own LinkedIn posts via Unipile and synthesizes a structured
voice profile for use in AI-generated outreach messages.
"""

import json
import logging
from datetime import datetime, timezone

from config import get_settings
from database import company_profiles_collection
from services.openrouter_service import OpenRouterClient
from utils.prompts import (
    SENDER_VOICE_SYNTHESIS_PROMPT,
    build_sender_voice_synthesis_prompt,
)

logger = logging.getLogger(__name__)

settings = get_settings()

SYNTHESIS_MODEL = "anthropic/claude-haiku-4-5"
MAX_POSTS_STORED = 25   # cap raw posts stored in company_profile
MIN_POSTS_FOR_SYNTHESIS = 3  # below this, mark as low-confidence
EXCERPT_COUNT = 5        # how many posts to surface as excerpts
EXCERPT_MAX_CHARS = 200  # truncate each excerpt to roughly this length


def _derive_post_excerpts(posts: list[dict], count: int = EXCERPT_COUNT, max_chars: int = EXCERPT_MAX_CHARS) -> list[str]:
    """
    Build display-ready excerpts from the raw stored posts (plain string slicing,
    no LLM call — must stay faithful to what was actually posted).
    Truncates each post's text to ~max_chars at a word boundary, appending "…" if cut.
    """
    excerpts: list[str] = []
    for p in posts[:count]:
        text = (p.get("text") or "").strip()
        if not text:
            continue
        if len(text) <= max_chars:
            excerpts.append(text)
            continue
        truncated = text[:max_chars]
        last_space = truncated.rfind(" ")
        if last_space > 0:
            truncated = truncated[:last_space]
        excerpts.append(truncated.rstrip() + "…")
    return excerpts


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
        max_tokens=2000,
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
            "synthesis_fallback": True,
        }

    profile["post_count"] = len(posts)
    profile["source"] = "linkedin_scrape"
    profile["synthesized_at"] = datetime.now(timezone.utc).isoformat()
    profile["post_excerpts"] = _derive_post_excerpts(posts)
    if len(posts) < MIN_POSTS_FOR_SYNTHESIS or profile.get("synthesis_fallback"):
        profile["low_confidence"] = True

    return profile


async def update_sender_voice_from_unipile(account_id: str) -> dict:
    """
    Pull the user's OWN voice data from Unipile (never Apify).
    Returns {voice_profile, sender_name, sender_role, low_confidence}.
    Falls back to manual tone (low_confidence=True) if no connected account or no data.
    """
    from database import linkedin_accounts_collection
    from services.unipile_service import UnipileClient

    async def _record_sync_error(reason: str) -> None:
        """Persist a diagnostic on the company profile so Settings can surface why sync failed."""
        logger.error(f"[voice-sync] account={account_id}: {reason}")
        await company_profiles_collection.update_one(
            {"account_id": account_id},
            {"$set": {
                "sender_voice_sync_error": reason,
                "sender_voice_synced_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )

    # Resolve tenant-scoped LinkedIn account — do NOT use get_linkedin_account_id() which is global
    li = await linkedin_accounts_collection.find_one(
        {"account_id": account_id},
        sort=[("created_at", -1)],
    )
    if not li or not li.get("public_id"):
        await _record_sync_error(
            "No connected LinkedIn account with a public_id — connect LinkedIn, then resync."
        )
        return {"voice_profile": None, "sender_name": "", "sender_role": "", "low_confidence": True}

    public_id = li["public_id"]
    profile_url = li.get("profile_url") or f"https://www.linkedin.com/in/{public_id}"
    unipile_account_id = li.get("unipile_account_id")
    if not unipile_account_id:
        await _record_sync_error("Connected LinkedIn record has no provider account identity.")
        return {"voice_profile": None, "sender_name": "", "sender_role": "", "low_confidence": True}
    client = UnipileClient(account_id=str(unipile_account_id))

    # Fetch about/headline/experience via Unipile
    norm = await client.get_profile_data_for_enrichment(profile_url) or {}
    # Fetch own posts via Unipile (defensive — returns [] if endpoint unavailable).
    # Pass the tenant-scoped unipile_account_id explicitly so this never falls back to the
    # global "first LinkedIn account in the workspace" lookup (see get_user_posts docstring).
    posts = await client.get_user_posts(public_id, account_id=unipile_account_id)
    logger.info(
        f"[voice-sync] account={account_id} public_id={public_id}: fetched {len(posts)} posts"
    )
    if not posts:
        # Still synthesize (low-confidence) from headline/about, but record why.
        await _record_sync_error(
            f"No LinkedIn posts returned by Unipile for {public_id} — "
            "voice profile synthesized without post data (low confidence)."
        )

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

    try:
        voice_profile = await synthesize_voice_profile(
            posts=posts,
            sender_name=sender_name or "the sender",
            sender_role=sender_role,
            raw_profile=raw_profile,
        )
    except Exception as e:
        await _record_sync_error(f"Voice synthesis failed: {e}")
        raise
    voice_profile["source"] = "unipile"

    update_set: dict = {
        "sender_name": sender_name,
        "sender_role": sender_role,
        "sender_linkedin_url": profile_url,
        "sender_voice_profile": voice_profile,
        "sender_linkedin_posts": posts[:MAX_POSTS_STORED],
        "sender_voice_synced_at": datetime.now(timezone.utc),
    }
    update_doc: dict = {"$set": update_set}
    if posts:
        # Full-fidelity sync — clear any stale diagnostic
        update_doc["$unset"] = {"sender_voice_sync_error": ""}
    await company_profiles_collection.update_one(
        {"account_id": account_id},
        update_doc,
        upsert=True,
    )
    logger.info(
        f"[voice-sync] account={account_id}: voice profile saved "
        f"(posts={len(posts)}, low_confidence={bool(voice_profile.get('low_confidence'))})"
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
