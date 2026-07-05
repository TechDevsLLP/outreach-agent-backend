"""
Onboarding wizard API — stages 1-6 plus session management.
Separate from the legacy /api/onboarding/analyze flow in routes/auth.py.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from pymongo import ReturnDocument
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import database
from auth import get_account_context, get_current_user
from config import get_settings
from services.onboarding_analyzer_service import analyze_company, run_analysis, create_job, get_job
from services.sender_voice_service import update_sender_voice_from_unipile

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/api/onboarding", tags=["onboarding_wizard"])


# ── Session management ────────────────────────────────────────────────────────

class StartSessionRequest(BaseModel):
    company_url: Optional[str] = None


@router.post("/start")
async def start_wizard_session(
    body: StartSessionRequest,
    user: dict = Depends(get_current_user),
):
    """Create or resume an onboarding wizard session."""
    account_id = str(user.get("current_account_id", ""))
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Upsert so a second call (retry / double-click) resumes rather than 500ing.
    existing = await database.onboarding_sessions_collection.find_one_and_update(
        {"account_id": account_id},
        {
            "$setOnInsert": {
                "_id": ObjectId(),
                "session_id": session_id,
                "account_id": account_id,
                "user_id": str(user["_id"]),
                "current_stage": 1,
                "stage_data": {},
                "refinement_chat_history": [],
                "created_at": now,
                "updated_at": now,
            }
        },
        upsert=True,
        return_document=True,  # pymongo ReturnDocument.AFTER equivalent
    )
    # Return the existing session if one was found, or the freshly created one
    returned_session_id = (existing or {}).get("session_id") or session_id
    returned_stage = (existing or {}).get("current_stage") or 1

    return {"session_id": returned_session_id, "current_stage": returned_stage}


# ── Stage 1: Company analysis ─────────────────────────────────────────────────

class ScrapeCompanyRequest(BaseModel):
    session_id: str
    url: str
    company_name: Optional[str] = ""
    description: Optional[str] = ""


@router.post("/stage-1/scrape-company")
async def stage1_scrape_company(
    body: ScrapeCompanyRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
):
    """Start a company scrape + AI analysis job for stage 1."""
    job_id = create_job()
    background_tasks.add_task(
        run_analysis,
        job_id=job_id,
        company_name=body.company_name or "",
        url=body.url,
        description=body.description or "",
    )
    # Persist job_id against session
    await database.onboarding_sessions_collection.update_one(
        {"session_id": body.session_id},
        {"$set": {"stage_data.stage1_job_id": job_id, "updated_at": datetime.now(timezone.utc)}},
    )
    return {"job_id": job_id, "status": "pending"}


@router.get("/stage-1/result/{job_id}")
async def stage1_result(job_id: str, user: dict = Depends(get_current_user)):
    """Poll stage 1 analysis job status."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "status": job["status"],
        "progress": job.get("progress", 0),
        "result": job.get("result"),
        "error": job.get("error"),
    }


@router.post("/stage-1/save")
async def stage1_save(
    body: dict,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Save stage 1 analysis results (possibly edited) to company_profile."""
    account_id = str(account_ctx["account"]["_id"])
    session_id = body.get("session_id")
    analysis = body.get("analysis", {})
    now = datetime.now(timezone.utc)

    update_doc: dict = {}
    if analysis.get("services"):
        update_doc["services"] = analysis["services"]
    if analysis.get("industries"):
        update_doc["target_industries"] = analysis["industries"]
    if analysis.get("pain_points"):
        update_doc["pain_points"] = analysis["pain_points"]
    if analysis.get("icp_description"):
        update_doc["icp_description"] = analysis["icp_description"]
    if analysis.get("case_studies"):
        update_doc["case_studies"] = analysis["case_studies"]
    update_doc["updated_at"] = now
    update_doc["onboarding_stage"] = 1

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": update_doc},
        upsert=True,
    )
    if session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {"current_stage": 2, "stage_data.stage1_analysis": analysis, "updated_at": now}},
        )
    return {"ok": True}


# ── Stage 2: Sender voice ─────────────────────────────────────────────────────

class LinkedInVoiceRequest(BaseModel):
    session_id: Optional[str] = None


@router.post("/stage-2/linkedin-voice")
async def stage2_linkedin_voice(
    body: LinkedInVoiceRequest,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Pull the user's OWN voice data from their connected LinkedIn (Unipile only)."""
    account_id = str(account_ctx["account"]["_id"])
    now = datetime.now(timezone.utc)

    result = await update_sender_voice_from_unipile(account_id)

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {"onboarding_stage": 2, "updated_at": now}},
        upsert=True,
    )
    if body.session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": body.session_id},
            {"$set": {"current_stage": 3, "updated_at": now}},
        )
    return result


# ── Stage 3: ICP ──────────────────────────────────────────────────────────────

class ICPRequest(BaseModel):
    session_id: Optional[str] = None
    industries: list[str] = []
    job_titles: list[str] = []
    seniority_levels: list[str] = []
    geographies: list[str] = []
    company_size_ranges: list[str] = []
    revenue_range: Optional[str] = None
    locked_industry: Optional[str] = None


@router.post("/stage-3/icp")
async def stage3_icp(
    body: ICPRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Save ICP definition to company_profile."""
    account_id = str(account_ctx["account"]["_id"])
    now = datetime.now(timezone.utc)

    icp_data = {
        "target_industries": body.industries,
        "target_job_titles": body.job_titles,
        "target_seniority": body.seniority_levels,
        "target_geographies": (body.geographies or [])[:3],
        "target_company_sizes": body.company_size_ranges,
        "target_revenue_range": body.revenue_range,
    }

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {
            **icp_data,
            "onboarding_stage": 3,
            "updated_at": now,
        }},
        upsert=True,
    )

    if body.session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": body.session_id},
            {"$set": {"current_stage": 4, "updated_at": now}},
        )

    # Fire-and-forget: canonicalize ICP into structured filters
    account_oid_str = account_id
    asyncio.create_task(_canonicalize_and_save_company_profile_icp(account_oid_str, body))

    if body.locked_industry and body.session_id:
        from services.onboarding_scrape_service import start_onboarding_scrape
        background_tasks.add_task(
            start_onboarding_scrape,
            account_id=account_id,
            session_id=body.session_id,
            locked_industry=body.locked_industry,
            icp_data=icp_data,
        )

    return {"ok": True}


async def _canonicalize_and_save_company_profile_icp(account_id: str, body) -> None:
    """Background: canonicalize free-text ICP and persist canonical fields to company_profiles."""
    try:
        from services.icp_canonicalizer import canonicalize_icp
        icp_data = {
            "target_industries": getattr(body, "industries", []) or [],
            "target_geographies": getattr(body, "geographies", []) or [],
            "target_seniority": getattr(body, "seniority_levels", []) or [],
            "target_company_sizes": getattr(body, "company_size_ranges", []) or [],
            "target_job_titles": getattr(body, "job_titles", []) or [],
        }
        canonical = await canonicalize_icp(icp_data)
        await database.company_profiles_collection.update_one(
            {"account_id": account_id},
            {"$set": canonical},
        )
        logger.info(f"[onboarding] ICP canonicalized for account {account_id}: "
                    f"{len(canonical.get('industry_ids', []))} industries, "
                    f"{len(canonical.get('country_codes', []))} countries")
    except Exception as e:
        logger.warning(f"[onboarding] ICP canonicalization failed for {account_id}: {e}")


# ── Stage 4: Offer ────────────────────────────────────────────────────────────

class OfferRequest(BaseModel):
    session_id: Optional[str] = None
    primary_cta: str
    discovery_call_agenda: Optional[str] = None
    qualifier_questions: list[str] = []


@router.post("/stage-4/offer")
async def stage4_offer(
    body: OfferRequest,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Save offer details to company_profile."""
    account_id = str(account_ctx["account"]["_id"])
    now = datetime.now(timezone.utc)

    update_doc: dict = {
        "primary_cta": body.primary_cta,
        "onboarding_stage": 4,
        "updated_at": now,
    }
    if body.discovery_call_agenda:
        update_doc["discovery_call_agenda"] = body.discovery_call_agenda
    if body.qualifier_questions:
        update_doc["qualifier_questions"] = body.qualifier_questions

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": update_doc},
        upsert=True,
    )

    if body.session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": body.session_id},
            {"$set": {"current_stage": 5, "updated_at": now}},
        )
        background_tasks.add_task(
            _prefetch_preview_prospects,
            account_id=account_id,
            session_id=body.session_id,
        )

    return {"ok": True}


# ── Stage 5: OAuth init ───────────────────────────────────────────────────────

@router.post("/stage-5/oauth/init")
async def stage5_oauth_init(
    body: dict,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Return the OAuth authorization URL for the requested provider."""
    import urllib.parse
    provider = body.get("provider", "google")

    if provider == "google":
        import json, base64
        # Encode return destination in state so the callback can redirect back to onboarding
        state_payload = json.dumps({"return_to": "/home", "account_id": str(account_ctx["account"]["_id"])})
        state = base64.urlsafe_b64encode(state_payload.encode()).decode()
        params = {
            "client_id": settings.google_client_id,
            "redirect_uri": settings.google_redirect_uri,
            "response_type": "code",
            "scope": (
                "https://www.googleapis.com/auth/gmail.send "
                "https://www.googleapis.com/auth/gmail.readonly "
                "https://www.googleapis.com/auth/userinfo.email "
                "https://www.googleapis.com/auth/calendar.events "
                "https://www.googleapis.com/auth/calendar.freebusy"
            ),
            "access_type": "offline",
            "prompt": "consent",
            "state": state,
        }
        auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
        return {"redirect_url": auth_url, "provider": "google"}

    raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider}")


# ── Stage 6: Refinement chat ──────────────────────────────────────────────────

class RefinementChatRequest(BaseModel):
    session_id: str
    user_message: str


@router.post("/stage-6/chat")
async def stage6_chat(
    body: RefinementChatRequest,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Multi-turn AI chat to capture objection bank, competitor bank, banned phrases."""
    from services.openrouter_service import OpenRouterClient
    from utils.prompts import ONBOARDING_REFINEMENT_SYSTEM_PROMPT

    account_id = str(account_ctx["account"]["_id"])

    # Load session
    session = await database.onboarding_sessions_collection.find_one(
        {"session_id": body.session_id}
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    history = session.get("refinement_chat_history", [])
    company_profile = await database.company_profiles_collection.find_one(
        {"account_id": account_id}
    )

    # Build messages for AI
    messages = [{"role": "system", "content": ONBOARDING_REFINEMENT_SYSTEM_PROMPT}]
    for turn in history:
        messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": body.user_message})

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=messages,
        model="anthropic/claude-haiku-4-5",
        max_tokens=600,
        temperature=0.4,
    )

    assistant_reply = (result.get("content") or "").strip()
    now = datetime.now(timezone.utc)

    # Append to history
    new_history = history + [
        {"role": "user", "content": body.user_message, "ts": now.isoformat()},
        {"role": "assistant", "content": assistant_reply, "ts": now.isoformat()},
    ]

    # Try to extract structured data from reply (JSON blocks)
    import json, re
    extracted = {}
    json_match = re.search(r"```json\s*(\{.*?\})\s*```", assistant_reply, re.DOTALL)
    if json_match:
        try:
            extracted = json.loads(json_match.group(1))
        except Exception:
            pass

    # Merge extracted data into company_profile
    profile_update: dict = {"updated_at": now}
    if extracted.get("objection"):
        obj = extracted["objection"]
        if isinstance(obj, dict) and obj.get("phrasing"):
            obj.setdefault("objection_id", str(uuid.uuid4()))
            await database.company_profiles_collection.update_one(
                {"account_id": account_id},
                {"$push": {"objection_bank": obj}},
                upsert=True,
            )
    if extracted.get("competitor"):
        comp = extracted["competitor"]
        if isinstance(comp, dict) and comp.get("name"):
            await database.company_profiles_collection.update_one(
                {"account_id": account_id},
                {"$push": {"competitor_bank": comp}},
                upsert=True,
            )
    if extracted.get("banned_phrase") and isinstance(extracted["banned_phrase"], str):
        await database.company_profiles_collection.update_one(
            {"account_id": account_id},
            {"$addToSet": {"banned_phrases": extracted["banned_phrase"]}},
            upsert=True,
        )

    await database.onboarding_sessions_collection.update_one(
        {"session_id": body.session_id},
        {"$set": {
            "refinement_chat_history": new_history,
            "current_stage": 6,
            "updated_at": now,
        }},
    )

    return {
        "reply": assistant_reply,
        "extracted": extracted,
    }


# ── Complete wizard ───────────────────────────────────────────────────────────

@router.post("/wizard/complete")
async def complete_wizard(
    body: dict,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """Mark wizard complete; mark user onboarding_complete flag."""
    account_id = str(account_ctx["account"]["_id"])
    session_id = body.get("session_id")
    now = datetime.now(timezone.utc)

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {"onboarding_stage": 6, "onboarding_completed_at": now, "updated_at": now}},
        upsert=True,
    )

    from database import users_collection
    await users_collection.update_one(
        {"_id": ObjectId(str(user["_id"]))},
        {"$set": {"onboarding_complete": True}},
    )

    if session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {"status": "completed", "completed_at": now, "updated_at": now}},
        )

    # Return AI campaign suggestion
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    suggested_campaign = {
        "name": f"Discovery Outreach — {(profile or {}).get('company_name', 'Target Accounts')}",
        "target_count": 100,
        "aggression_preset": (profile or {}).get("aggression_preset", "aggressive"),
        "industries": (profile or {}).get("target_industries", [])[:3],
    }

    return {"ok": True, "suggested_campaign": suggested_campaign}


# ── Session resume ────────────────────────────────────────────────────────────

@router.get("/session")
async def get_onboarding_session(
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Return the current onboarding session state so any device can resume.

    Response shape:
      { current_stage, status, session_id, profile: {...}, prospect_preview: [...], prospect_preview_excluded_companies: [...] }
    """
    account_id = str(account_ctx["account"]["_id"])
    user_id = str(user["_id"])

    # Fetch most recent session for this user/account
    session = await database.onboarding_sessions_collection.find_one(
        {"user_id": user_id},
        sort=[("created_at", -1)],
    )

    # Fetch saved profile
    profile = await database.company_profiles_collection.find_one({"account_id": account_id}) or {}

    # Build serialisable profile slice (only what the wizard needs to rehydrate)
    profile_out = {
        "company_name":         profile.get("company_name"),
        "website_url":          profile.get("website_url"),
        "services":             profile.get("services") or [],
        "pain_points":          profile.get("pain_points") or [],
        "icp_description":      profile.get("icp_description"),
        "target_industries":    profile.get("target_industries") or [],
        "target_job_titles":    profile.get("target_job_titles") or [],
        "target_seniority":     profile.get("target_seniority") or [],
        "target_geographies":   profile.get("target_geographies") or [],
        "target_company_sizes": profile.get("target_company_sizes") or [],
        "primary_cta":          profile.get("primary_cta"),
        "discovery_call_agenda": profile.get("discovery_call_agenda"),
        "qualifier_questions":  profile.get("qualifier_questions") or [],
        "sender_name":          profile.get("sender_name"),
        "sender_role":          profile.get("sender_role"),
        "sender_linkedin_url":  profile.get("sender_linkedin_url"),
        "sender_voice_profile": profile.get("sender_voice_profile"),
        "onboarding_stage":     profile.get("onboarding_stage", 0),
    }

    # Prospect preview: use session data if present
    raw_preview = list((session or {}).get("prospect_preview") or [])
    prospect_preview = [_serialise_prospect_for_table(p) for p in raw_preview]
    excluded_companies = list((session or {}).get("prospect_preview_excluded_companies") or [])

    return {
        "session_id":     (session or {}).get("session_id"),
        "current_stage":  (session or {}).get("current_stage", profile.get("onboarding_stage", 1)),
        "status":         (session or {}).get("status", "active"),
        "profile":        profile_out,
        "prospect_preview":                   prospect_preview,
        "prospect_preview_excluded_companies": excluded_companies,
    }


# ── Prospect preview (post-wizard gate) ───────────────────────────────────────

class ProspectPreviewRequest(BaseModel):
    session_id: Optional[str] = None


class ProspectPreviewRerollRequest(BaseModel):
    session_id: Optional[str] = None
    override_job_titles: Optional[list[str]] = None
    override_industries: Optional[list[str]] = None


class LaunchFirstCampaignRequest(BaseModel):
    session_id: Optional[str] = None
    campaign_name: Optional[str] = None
    target_total: int = 50


def _serialise_prospect_for_table(p: dict) -> dict:
    """Return only the fields needed for the prospect preview table."""
    return {
        "full_name": p.get("full_name"),
        "linkedin": p.get("linkedin"),
        "email": p.get("email"),
        "company_name": p.get("company_name"),
        "job_title": p.get("job_title"),
        "company_linkedin": p.get("company_linkedin"),
        "company_domain": p.get("company_domain"),
        "industry": p.get("industry"),
        "country": p.get("country"),
    }


async def _prefetch_preview_prospects(account_id: str, session_id: str) -> None:
    """Background task: pre-source 5 prospects after offer is saved so preview is instant."""
    from services.onboarding_prospect_service import source_preview_prospects
    try:
        profile = await database.company_profiles_collection.find_one({"account_id": account_id})
        if not profile:
            return
        prospects = await source_preview_prospects(profile, count=5, account_id=account_id)
        if not prospects:
            logger.info(f"[prefetch] No prospects found for account {account_id}")
            return
        now = datetime.now(timezone.utc)
        names = [p.get("company_name") for p in prospects if p.get("company_name")]
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {
                "prospect_preview": prospects,
                "prospect_preview_excluded_companies": names,
                "prospect_preview_at": now,
                "updated_at": now,
            }},
        )
        logger.info(f"[prefetch] Cached {len(prospects)} prospects for session {session_id}")
    except Exception as e:
        logger.warning(f"[prefetch] Prospect prefetch failed for {session_id}: {e}")


@router.post("/prospect-preview")
async def get_prospect_preview(
    body: ProspectPreviewRequest,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Source 5 target companies + 1 prospect/company (with email) from the
    user's ICP profile.  Returns a table of 5 prospects for user confirmation.
    Results are stored on the onboarding session for the re-roll + launch steps.
    """
    from services.onboarding_prospect_service import source_preview_prospects

    account_id = str(account_ctx["account"]["_id"])
    session_id = body.session_id

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    if not profile:
        raise HTTPException(status_code=400, detail="Company profile not found. Complete onboarding stages 1-4 first.")

    # Return cached result if background prefetch already ran
    if session_id:
        existing_session = await database.onboarding_sessions_collection.find_one(
            {"session_id": session_id}
        )
        if existing_session and existing_session.get("prospect_preview"):
            cached = list(existing_session["prospect_preview"])
            return {
                "prospects": [_serialise_prospect_for_table(p) for p in cached],
                "count": len(cached),
            }

    prospects = await source_preview_prospects(profile, count=5, account_id=account_id)

    if not prospects:
        raise HTTPException(
            status_code=422,
            detail="Could not find valid target companies for your ICP. Try adjusting industries or job titles.",
        )

    now = datetime.now(timezone.utc)

    # Persist to session so re-roll can access excluded company names + launch can use confirmed list
    if session_id:
        sourced_company_names = [p.get("company_name") for p in prospects if p.get("company_name")]
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {
                "prospect_preview": prospects,
                "prospect_preview_excluded_companies": sourced_company_names,
                "prospect_preview_at": now,
                "updated_at": now,
            }},
            upsert=True,
        )

    return {
        "prospects": [_serialise_prospect_for_table(p) for p in prospects],
        "count": len(prospects),
    }


@router.post("/prospect-preview/reroll")
async def reroll_prospect_preview(
    body: ProspectPreviewRerollRequest,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """
    Re-source 5 new companies (excluding previously shown ones).
    Optionally override target job titles or industries for this pass.
    """
    from services.onboarding_prospect_service import source_preview_prospects

    account_id = str(account_ctx["account"]["_id"])
    session_id = body.session_id
    now = datetime.now(timezone.utc)

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    if not profile:
        raise HTTPException(status_code=400, detail="Company profile not found.")

    # Build cumulative exclude list from session history
    exclude_names: list[str] = []
    if session_id:
        session = await database.onboarding_sessions_collection.find_one({"session_id": session_id})
        if session:
            exclude_names = list(session.get("prospect_preview_excluded_companies") or [])

    prospects = await source_preview_prospects(
        profile,
        count=5,
        exclude_company_names=exclude_names if exclude_names else None,
        override_job_titles=body.override_job_titles,
        override_industries=body.override_industries,
        account_id=account_id,
    )

    if not prospects:
        raise HTTPException(
            status_code=422,
            detail="Could not find additional target companies. All good options may be exhausted.",
        )

    # Accumulate excluded companies across re-rolls
    new_company_names = [p.get("company_name") for p in prospects if p.get("company_name")]
    all_excluded = list(set(exclude_names + new_company_names))

    if session_id:
        await database.onboarding_sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {
                "prospect_preview": prospects,
                "prospect_preview_excluded_companies": all_excluded,
                "prospect_preview_at": now,
                "updated_at": now,
            }},
            upsert=True,
        )

    return {
        "prospects": [_serialise_prospect_for_table(p) for p in prospects],
        "count": len(prospects),
        "excluded_companies": all_excluded,
    }


@router.post("/launch-first-campaign")
async def launch_first_campaign(
    request: Request,
    account_ctx: dict = Depends(get_account_context),
):
    """
    The onboarding campaign was created + discovery started when the user locked
    their industry at stage 3. This endpoint re-plans channel assignments now that
    sender accounts are connected, then returns campaign_id for approve-day/1.
    """
    account_id = str(account_ctx["account"]["_id"])
    body = await request.json()
    session_id = body.get("session_id") or body.get("sessionId") or ""

    if not session_id:
        raise HTTPException(status_code=422, detail="session_id required")

    # Find the session to get onboarding_campaign_id
    from bson import ObjectId as _OId
    _session_filter = {"_id": _OId(session_id)} if len(session_id) == 24 else {"session_id": session_id}
    session_doc = await database.onboarding_sessions_collection.find_one(_session_filter)
    if not session_doc:
        # Try alternate session lookup
        session_doc = await database.onboarding_sessions_collection.find_one({"session_id": session_id})

    campaign_id = str(session_doc.get("onboarding_campaign_id") or "") if session_doc else ""

    if not campaign_id:
        # Fallback: look up via onboarding_scrape_jobs
        job_doc = await database.onboarding_scrape_jobs_collection.find_one(
            {"session_id": session_id}
        )
        campaign_id = str(job_doc.get("campaign_id") or "") if job_doc else ""

    if not campaign_id:
        raise HTTPException(
            status_code=404,
            detail="Onboarding campaign not found. Ensure your industry was locked at stage 3."
        )

    # Re-plan channels now that sender accounts are connected
    from services.curated_discovery_service import replan_and_launch
    result = await replan_and_launch(campaign_id, account_id)

    return {
        "success": True,
        "campaign_id": campaign_id,
        "assigned": result.get("assigned", 0),
        "day_totals": result.get("day_totals", {}),
    }


# ── Scrape progress polling endpoint ─────────────────────────────────────────

@router.get("/scrape-status/{session_id}")
async def get_scrape_status(
    session_id: str,
    account_ctx: dict = Depends(get_account_context),
):
    """Plain polling endpoint for onboarding scrape progress."""
    job = await database.onboarding_scrape_jobs_collection.find_one(
        {"session_id": session_id}
    )
    if not job:
        return {"status": "not_started", "prospects_found": 0, "day1_ready": False, "campaign_id": None}
    return {
        "status": job.get("status", "running"),
        "prospects_found": int(job.get("prospects_found") or 0),
        "day1_ready": bool(job.get("day1_ready")),
        "campaign_id": str(job.get("campaign_id") or ""),
        "error": job.get("error"),
    }


# ── Legacy SSE endpoint (deprecated — use /scrape-status/{session_id} instead) ──

@router.get("/scrape-progress/{session_id}")
async def scrape_progress_stream(
    session_id: str,
    user: dict = Depends(get_current_user),
    account_ctx: dict = Depends(get_account_context),
):
    """
    DEPRECATED: Use GET /scrape-status/{session_id} for polling instead.
    Legacy SSE stream kept for backward compatibility.
    Returns a single JSON status blob as an SSE event.
    """
    job = await database.onboarding_scrape_jobs_collection.find_one(
        {"session_id": session_id}
    )
    import json as _json

    async def _single_event():
        status = (job or {}).get("status", "not_started")
        yield f"data: {_json.dumps({'type': 'status', 'status': status, 'prospects_found': (job or {}).get('prospects_found', 0), 'day1_ready': (job or {}).get('day1_ready', False)})}\n\n"

    return StreamingResponse(
        _single_event(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
