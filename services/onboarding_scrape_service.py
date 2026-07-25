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

# Strong references to fire-and-forget tasks. The event loop only keeps weak
# refs, so an unreferenced task can be garbage-collected before it runs —
# exactly the silent-death mode that stranded onboarding discoveries.
_bg_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task

# discovery_status values that mean a discovery run is currently in flight
DISCOVERY_ACTIVE_STATUSES = [
    "queued", "searching_db", "scraping", "enriching", "scoring",
    "sourcing_companies", "scraping_employees",
]


async def create_onboarding_campaign(
    account_id: str,
    session_id: str,
    locked_industry: str,
    icp_data: dict,
) -> str:
    """
    Synchronously create the onboarding campaign doc and persist its id on the
    onboarding session. Idempotent per session: if the session already has an
    onboarding_campaign_id pointing at an existing campaign, returns it.

    Called inline from the stage-3 route so the response can carry campaign_id
    (Bug B: previously the campaign was created inside a background task and
    the stage-3 response had no campaign reference; launch-first-campaign then
    404ed if the background task failed or hadn't run).
    """
    now = datetime.now(timezone.utc)

    # Idempotency: reuse an existing campaign for this session
    _session_filter = {"_id": ObjectId(session_id)} if len(session_id) == 24 else {"session_id": session_id}
    session_doc = await database.onboarding_sessions_collection.find_one(_session_filter) \
        or await database.onboarding_sessions_collection.find_one({"session_id": session_id})
    existing_id = str((session_doc or {}).get("onboarding_campaign_id") or "")
    if existing_id:
        existing = await database.campaigns_collection.find_one(
            {"_id": ObjectId(existing_id)}, {"_id": 1}
        )
        if existing:
            logger.info(f"[onboarding_scrape] session={session_id} reusing campaign {existing_id}")
            return existing_id

    icp_prompt = _build_icp_prompt(locked_industry, icp_data)

    from config import get_settings
    settings = get_settings()

    campaign_oid = ObjectId()
    campaign_id = str(campaign_oid)

    campaign_doc = {
        "_id": campaign_oid,
        # account_id is ALWAYS stored as a string on tenant-scoped docs
        # (Bug B: this was ObjectId(account_id), breaking string-keyed lookups)
        "account_id": str(account_id),
        "name": f"Onboarding — {locked_industry}",
        "type": "smart",
        "is_smart_campaign": True,
        "is_onboarding_campaign": True,
        "status": "draft",
        "discovery_status": "pending",
        "curated_icp_prompt": icp_prompt,
        "icp_industries": [locked_industry],
        "icp_job_titles": icp_data.get("target_job_titles") or [],
        "icp_seniority_levels": icp_data.get("target_seniority") or [],
        "icp_countries": icp_data.get("target_geographies") or [],
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
        "onboarding_session_id": session_id,
        "created_at": now,
        "updated_at": now,
    }
    await database.campaigns_collection.insert_one(campaign_doc)
    logger.info(
        f"[onboarding_scrape] created onboarding campaign {campaign_id} "
        f"account={account_id} session={session_id} industry={locked_industry!r}"
    )

    # Canonicalize ICP fields in the background
    _spawn(_canonicalize_campaign_icp(campaign_id))

    # Persist campaign_id to session so launch-first-campaign can find it
    await database.onboarding_sessions_collection.update_one(
        _session_filter,
        {"$set": {"onboarding_campaign_id": campaign_id, "updated_at": now}},
    )

    return campaign_id


def _build_icp_prompt(locked_industry: str, icp_data: dict) -> str:
    """Build the curated-discovery ICP prompt string from locked industry + ICP data."""
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
    return " | ".join(icp_parts)


async def start_onboarding_scrape(
    account_id: str,
    session_id: str,
    locked_industry: str,
    icp_data: dict,
    campaign_id: str | None = None,
) -> None:
    """
    Background task: run fast discovery for the onboarding campaign.
    If campaign_id is not supplied (legacy callers), the campaign is created here.
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
        # Create the campaign if the route didn't already do it synchronously
        if not campaign_id:
            campaign_id = await create_onboarding_campaign(
                account_id=account_id,
                session_id=session_id,
                locked_industry=locked_industry,
                icp_data=icp_data,
            )

        # Record the campaign on the job doc up front so scrape-status polling
        # can return it even while discovery is still running.
        await database.onboarding_scrape_jobs_collection.update_one(
            {"session_id": session_id},
            {"$set": {"campaign_id": campaign_id, "updated_at": datetime.now(timezone.utc)}},
        )
        logger.info(f"[onboarding_scrape] starting discovery campaign={campaign_id} session={session_id}")

        await enqueue_onboarding_discovery(
            campaign_id=campaign_id,
            account_id=account_id,
            session_id=session_id,
        )

    except Exception as e:
        logger.exception(f"[onboarding_scrape] start failed for session {session_id}: {e}")
        await database.onboarding_scrape_jobs_collection.update_one(
            {"session_id": session_id},
            {"$set": {"status": "failed", "error": str(e)[:400], "updated_at": datetime.now(timezone.utc)}},
        )


async def enqueue_onboarding_discovery(
    campaign_id: str,
    account_id: str,
    session_id: str,
) -> bool:
    """Queue durable discovery for an onboarding campaign.

    Uses the same Mongo-leased job queue as smart campaigns so the work
    survives process restarts and is resumed by the scheduler. Idempotent:
    a campaign whose discovery is already queued/running is left alone.

    Returns True if a new discovery job was queued, False if one was already
    in flight.
    """
    from pymongo import ReturnDocument

    # Atomically advance the generation so each (re)trigger owns a distinct
    # deterministic job while an in-flight discovery cannot be double-queued —
    # same pattern as the smart-campaign retrigger route.
    queued_campaign = await database.campaigns_collection.find_one_and_update(
        {
            "_id": ObjectId(campaign_id),
            "account_id": {"$in": [account_id, ObjectId(account_id) if len(account_id) == 24 else account_id]},
            "discovery_status": {"$nin": DISCOVERY_ACTIVE_STATUSES},
        },
        {"$set": {
            "discovery_status": "queued",
            "discovery_error": None,
            "updated_at": datetime.now(timezone.utc),
        }, "$inc": {"discovery_generation": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if not queued_campaign:
        logger.info(
            f"[onboarding_scrape] discovery already queued/running for campaign={campaign_id} — skipping enqueue"
        )
        # Make sure a tracker is following the in-flight run for the UI.
        _spawn(_track_progress(campaign_id, account_id, session_id))
        return False

    from services.enrichment_job_service import enqueue_campaign_discovery

    try:
        await enqueue_campaign_discovery(
            account_id=str(account_id),
            campaign_id=campaign_id,
            generation=int(queued_campaign.get("discovery_generation") or 1),
        )
    except Exception:
        await database.campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": {
                "discovery_status": "failed",
                "discovery_error": "Could not queue discovery work",
                "updated_at": datetime.now(timezone.utc),
            }},
        )
        raise

    await database.onboarding_scrape_jobs_collection.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "status": "running",
                "campaign_id": campaign_id,
                "error": None,
                "updated_at": datetime.now(timezone.utc),
            },
            "$setOnInsert": {
                "account_id": account_id,
                "session_id": session_id,
                "prospects_found": 0,
                "day1_ready": False,
                "day1_enrolled": 0,
                "created_at": datetime.now(timezone.utc),
            },
        },
        upsert=True,
    )
    logger.info(
        f"[onboarding_scrape] queued durable discovery campaign={campaign_id} "
        f"generation={queued_campaign.get('discovery_generation')} session={session_id}"
    )
    _spawn(_track_progress(campaign_id, account_id, session_id))
    return True


_TRACK_TIMEOUT_SECONDS = 3 * 60 * 60  # give up tracking (not the job itself) after 3h


async def _track_progress(campaign_id: str, account_id: str, session_id: str) -> None:
    """Mirror durable-discovery progress into the onboarding_scrape_jobs doc.

    Discovery itself runs in the leased job queue (survives restarts); this
    task only watches the campaign doc + enrollment counts so scrape-status
    polling and the SSE stream keep working. If this watcher dies, the job
    still completes — the next launch-first-campaign call restarts a watcher.
    """
    campaign_oid = ObjectId(campaign_id)
    deadline = asyncio.get_event_loop().time() + _TRACK_TIMEOUT_SECONDS
    try:
        while True:
            await asyncio.sleep(5)
            campaign = await database.campaigns_collection.find_one(
                {"_id": campaign_oid}, {"discovery_status": 1, "discovery_error": 1}
            )
            if not campaign:
                return
            status = campaign.get("discovery_status")
            count = await database.campaign_enrollments_collection.count_documents(
                {"campaign_id": campaign_oid}
            )

            if status == "failed":
                await database.onboarding_scrape_jobs_collection.update_one(
                    {"session_id": session_id},
                    {"$set": {
                        "status": "failed",
                        "error": str(campaign.get("discovery_error") or "Discovery failed")[:400],
                        "prospects_found": count,
                        "updated_at": datetime.now(timezone.utc),
                    }},
                )
                logger.warning(f"[onboarding_scrape] discovery failed session={session_id} campaign={campaign_id}")
                return

            day1_ready = status in ("completed", "enriching", "awaiting_approval")
            done = status in ("completed", "awaiting_approval")
            await database.onboarding_scrape_jobs_collection.update_one(
                {"session_id": session_id},
                {"$set": {
                    "status": "completed" if done else "running",
                    "prospects_found": count,
                    "day1_ready": day1_ready,
                    "day1_enrolled": min(count, 45),
                    "campaign_id": campaign_id,
                    "updated_at": datetime.now(timezone.utc),
                }},
            )
            if done:
                logger.info(f"[onboarding_scrape] completed session={session_id} prospects={count}")
                return
            if asyncio.get_event_loop().time() > deadline:
                logger.warning(f"[onboarding_scrape] tracker timed out session={session_id} campaign={campaign_id}")
                return
    except Exception as e:
        logger.exception(f"[onboarding_scrape] tracker failed session={session_id}: {e}")


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
