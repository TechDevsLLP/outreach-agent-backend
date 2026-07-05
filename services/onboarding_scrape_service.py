"""
Onboarding background scrape — fires after Stage 3 ICP lock.
Runs run_fast_discovery() with the locked industry as primary ICP, storing
progress in onboarding_scrape_jobs so the SSE endpoint can stream it to the UI.
"""

import asyncio
import logging
from datetime import datetime, timezone

from bson import ObjectId

import database

logger = logging.getLogger(__name__)


async def start_onboarding_scrape(
    account_id: str,
    session_id: str,
    locked_industry: str,
    icp_data: dict,
) -> None:
    """
    Background task: create an onboarding campaign and run fast discovery.
    Progress is tracked in onboarding_scrape_jobs collection.
    """
    now = datetime.now(timezone.utc)

    job_doc = {
        "account_id": account_id,
        "session_id": session_id,
        "status": "running",
        "prospects_found": 0,
        "day1_ready": False,
        "day1_enrolled": 0,
        "created_at": now,
        "updated_at": now,
    }
    await database.onboarding_scrape_jobs_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {"status": "running", "updated_at": now},
            "$setOnInsert": {k: v for k, v in job_doc.items() if k != "updated_at"},
        },
        upsert=True,
    )

    try:
        # Build an ICP prompt from the locked industry + ICP data
        icp_parts = [locked_industry]
        job_titles = icp_data.get("target_job_titles") or []
        seniority = icp_data.get("target_seniority") or []
        geographies = icp_data.get("target_geographies") or []
        company_sizes = icp_data.get("target_company_sizes") or []

        if job_titles:
            icp_parts.append(f"Titles: {', '.join(job_titles[:5])}")
        if seniority:
            icp_parts.append(f"Seniority: {', '.join(seniority[:3])}")
        if geographies:
            icp_parts.append(f"Countries: {', '.join(geographies[:3])}")
        if company_sizes:
            icp_parts.append(f"Company size: {', '.join(company_sizes[:2])}")

        icp_prompt = " | ".join(icp_parts)

        # Create a synthetic onboarding campaign doc for the discovery pipeline
        from config import get_settings
        settings = get_settings()

        campaign_oid = ObjectId()
        campaign_id = str(campaign_oid)

        campaign_doc = {
            "_id": campaign_oid,
            "account_id": ObjectId(account_id) if len(account_id) == 24 else account_id,
            "name": f"Onboarding — {locked_industry}",
            "type": "smart",
            "is_smart_campaign": True,
            "status": "draft",
            "discovery_status": "pending",
            "curated_icp_prompt": icp_prompt,
            "icp_industries": [locked_industry],
            "icp_job_titles": job_titles,
            "icp_seniority_levels": seniority,
            "icp_countries": geographies,
            "prospect_count_target": settings.enrolled_target_first_campaign,
            "daily_caps": {
                "linkedin_connection": 20,
                "email": 20,
                "linkedin_inmail": 5,
            },
            "max_prospects_per_company": 3,
            "discovery_mode": "curated",
            "message_tone": "professional",
            "cta_type": "reply",
            "created_at": now,
            "updated_at": now,
        }
        await database.campaigns_collection.insert_one(campaign_doc)

        # Canonicalize ICP fields and persist on the campaign
        asyncio.create_task(_canonicalize_campaign_icp(campaign_id))

        # Persist campaign_id to session so launch-first-campaign can find it
        try:
            from bson import ObjectId as _ObjId
            _session_filter = {"_id": _ObjId(session_id)} if len(session_id) == 24 else {"session_id": session_id}
            await database.onboarding_sessions_collection.update_one(
                _session_filter,
                {"$set": {"onboarding_campaign_id": campaign_id}},
            )
        except Exception as _e:
            logger.warning(f"[onboarding_scrape] could not persist campaign_id to session: {_e}")

        # Run discovery — this is a long-running async task
        from services.curated_discovery_service import run_fast_discovery

        # Wrap discovery so we can poll progress and update the job doc
        asyncio.create_task(
            _run_and_track(campaign_id, account_id, session_id)
        )

    except Exception as e:
        logger.exception(f"[onboarding_scrape] start failed for session {session_id}: {e}")
        await database.onboarding_scrape_jobs_collection.update_one(
            {"session_id": session_id},
            {"$set": {"status": "failed", "error": str(e)[:400], "updated_at": datetime.now(timezone.utc)}},
        )


async def _run_and_track(campaign_id: str, account_id: str, session_id: str) -> None:
    """Run discovery and continuously update the job doc with prospect counts."""
    try:
        from services.curated_discovery_service import run_fast_discovery

        # Poll prospects count while discovery runs in background
        discovery_task = asyncio.create_task(
            run_fast_discovery(campaign_id, account_id)
        )

        # Poll every 5s updating the job counter until discovery finishes
        campaign_oid = ObjectId(campaign_id)
        account_oid = ObjectId(account_id) if len(account_id) == 24 else account_id

        while not discovery_task.done():
            await asyncio.sleep(5)
            count = await database.campaign_enrollments_collection.count_documents(
                {"campaign_id": campaign_oid}
            )
            # Check if day1 cohort is ready (discovery_status = completed means day1 is ready)
            campaign = await database.campaigns_collection.find_one(
                {"_id": campaign_oid}, {"discovery_status": 1}
            )
            day1_ready = (campaign or {}).get("discovery_status") in ("completed", "enriching", "awaiting_approval")
            await database.onboarding_scrape_jobs_collection.update_one(
                {"session_id": session_id},
                {"$set": {
                    "prospects_found": count,
                    "day1_ready": day1_ready,
                    "day1_enrolled": min(count, 45),
                    "updated_at": datetime.now(timezone.utc),
                }},
            )

        # Discovery finished — get final count via enrollments (prospects is a global
        # shared pool with no account_id field, so filtering by account_id returns 0).
        await discovery_task  # re-raise if it failed
        final_count = await database.campaign_enrollments_collection.count_documents(
            {"campaign_id": campaign_oid}
        )
        await database.onboarding_scrape_jobs_collection.update_one(
            {"session_id": session_id},
            {"$set": {
                "status": "completed",
                "prospects_found": final_count,
                "day1_ready": True,
                "day1_enrolled": min(final_count, 45),
                "campaign_id": campaign_id,
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        logger.info(f"[onboarding_scrape] completed session={session_id} prospects={final_count}")

    except Exception as e:
        logger.exception(f"[onboarding_scrape] _run_and_track failed session={session_id}: {e}")
        await database.onboarding_scrape_jobs_collection.update_one(
            {"session_id": session_id},
            {"$set": {"status": "failed", "error": str(e)[:400], "updated_at": datetime.now(timezone.utc)}},
        )


async def _canonicalize_campaign_icp(campaign_id: str) -> None:
    """Background: canonicalize the campaign's ICP free-text into structured filters."""
    try:
        from services.icp_canonicalizer import canonicalize_icp
        campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
        if not campaign:
            return
        canonical = await canonicalize_icp(campaign)
        await database.campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": canonical},
        )
    except Exception as e:
        logger.warning(f"[onboarding_scrape] ICP canonicalization failed for campaign {campaign_id}: {e}")
