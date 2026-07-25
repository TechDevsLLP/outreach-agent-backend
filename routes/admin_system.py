"""
Superadmin — system health, jobs, stuck campaigns, webhook log, audit log,
and runtime gate-flag toggles.
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

import database
from auth import get_super_admin
from config import get_settings
from services.admin_audit_service import log_admin_action
from services import system_settings_service

router = APIRouter(prefix="/api/admin", tags=["Admin"])
settings = get_settings()

# Campaign discovery statuses considered transitional (from models/campaign.py)
TRANSITIONAL_DISCOVERY_STATUSES = [
    "searching_db", "running", "scraping", "enriching", "scoring",
    "generating_messages", "sourcing_companies", "scraping_employees",
]

# Flags that may be overridden at runtime. Only names with wired call sites
# (or env-config counterparts) are accepted, so a typo can't create a dead flag.
KNOWN_FLAGS = {
    "quality_gates_enabled": settings.quality_gates_enabled,
    "title_gate_enabled": settings.title_gate_enabled,
    "prefilter_gate_enabled": settings.prefilter_gate_enabled,
}


def _serialize(doc):
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: _serialize(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_serialize(v) for v in doc]
    return doc


# ---------------------------------------------------------------------------
# GET /api/admin/health/deep
# ---------------------------------------------------------------------------

# Atlas list_search_indexes() costs ~850 ms per collection round-trip and the
# result changes rarely — cache it in-process for 5 minutes. ?refresh=true
# bypasses the cache.
_SEARCH_INDEX_CACHE_TTL = 300.0
_search_index_cache: dict = {"ts": 0.0, "value": None}


async def _fetch_search_indexes(collection):
    """Best effort — not all drivers/tiers support list_search_indexes."""
    try:
        return [
            {"name": ix.get("name"), "type": ix.get("type"), "status": ix.get("status")}
            async for ix in collection.list_search_indexes()
        ]
    except Exception:
        return "unknown"


async def _get_search_index_status(refresh: bool = False) -> dict:
    now = time.monotonic()
    if (
        not refresh
        and _search_index_cache["value"] is not None
        and now - _search_index_cache["ts"] < _SEARCH_INDEX_CACHE_TTL
    ):
        return _search_index_cache["value"]

    prospects_search_indexes, companies_search_indexes = await asyncio.gather(
        _fetch_search_indexes(database.prospects_collection),
        _fetch_search_indexes(database.companies_collection),
    )
    value = {
        "prospects": prospects_search_indexes,
        "companies": companies_search_indexes,
    }
    _search_index_cache["ts"] = now
    _search_index_cache["value"] = value
    return value


async def _read_heartbeats() -> list[dict]:
    """Shared heartbeat read for /health/deep and /jobs (identical query)."""
    return await database.scheduler_heartbeats_collection.find({}).sort("job_id", 1).to_list(100)


@router.get("/health/deep")
async def deep_health(
    refresh: bool = Query(False, description="Bypass the search-index status cache"),
    _admin: dict = Depends(get_super_admin),
):
    # Mongo ping
    try:
        await database.client.admin.command("ping")
        mongo_status = "connected"
    except Exception as e:
        mongo_status = f"error: {e}"

    # Collection counts (estimated — fast, no full scans)
    count_targets = {
        "prospects": database.prospects_collection,
        "companies": database.companies_collection,
        "prospect_state": database.prospect_state_collection,
        "campaigns": database.campaigns_collection,
        "campaign_enrollments": database.campaign_enrollments_collection,
        "users": database.users_collection,
        "accounts": database.accounts_collection,
        "conversations": database.conversations_collection,
        "suppressions": database.suppressions_collection,
    }
    counts = {}
    try:
        values = await asyncio.gather(*(c.estimated_document_count() for c in count_targets.values()))
        counts = dict(zip(count_targets.keys(), values))
    except Exception as e:
        counts = {"error": str(e)}

    # Atlas Search / Vector Search index status (cached ~5 min; ?refresh=true bypasses)
    search_indexes = await _get_search_index_status(refresh=refresh)

    # Scheduler heartbeats
    heartbeats = await _read_heartbeats()

    # Provider key presence — booleans ONLY, never values
    provider_keys_configured = {
        "apify": bool(settings.apify_api_key),
        "openrouter": bool(settings.openrouter_api_key),
        "gemini": bool(settings.gemini_api_key),
        "growthtoolkit": bool(settings.growthtoolkit_api_key),
        "unipile": bool(settings.unipile_token),
        "google_oauth": bool(settings.google_client_id and settings.google_client_secret),
        "zoho_oauth": bool(settings.zoho_client_id and settings.zoho_client_secret),
    }

    return {
        "mongo": {"status": mongo_status, "database": settings.mongodb_database, "collection_counts": counts},
        "search_indexes": search_indexes,
        "scheduler_heartbeats": [_serialize(h) for h in heartbeats],
        "provider_keys_configured": provider_keys_configured,
        "app_env": settings.app_env,
        "app_role": settings.app_role,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# GET /api/admin/jobs — scheduler heartbeat listing
# ---------------------------------------------------------------------------

@router.get("/jobs")
async def list_jobs(_admin: dict = Depends(get_super_admin)):
    heartbeats = await _read_heartbeats()
    now = datetime.now(timezone.utc)
    jobs = []
    for h in heartbeats:
        last_run = h.get("last_run_at")
        if isinstance(last_run, datetime) and last_run.tzinfo is None:
            last_run_aware = last_run.replace(tzinfo=timezone.utc)
        else:
            last_run_aware = last_run
        age_seconds = (now - last_run_aware).total_seconds() if last_run_aware else None
        jobs.append({
            **_serialize(h),
            "seconds_since_last_run": round(age_seconds) if age_seconds is not None else None,
        })
    return {"jobs": jobs}


# ---------------------------------------------------------------------------
# GET /api/admin/campaigns/stuck
# ---------------------------------------------------------------------------

@router.get("/campaigns/stuck")
async def get_stuck_campaigns(
    hours: int = Query(24, ge=1, le=720),
    _admin: dict = Depends(get_super_admin),
):
    """Campaigns sitting in a transitional discovery/message-gen state with no
    update in the last N hours."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    query = {
        "$and": [
            {"$or": [
                {"discovery_status": {"$in": TRANSITIONAL_DISCOVERY_STATUSES}},
                {"message_gen_status": "running"},
            ]},
            {"$or": [
                {"updated_at": {"$lt": cutoff}},
                {"updated_at": {"$exists": False}},
            ]},
        ]
    }
    rows = await database.campaigns_collection.find(
        query,
        {
            "name": 1, "account_id": 1, "status": 1,
            "discovery_status": 1, "discovery_started_at": 1, "discovery_error": 1,
            "message_gen_status": 1, "message_gen_started_at": 1,
            "created_at": 1, "updated_at": 1,
        },
    ).sort("updated_at", 1).to_list(200)

    return {
        "hours": hours,
        "count": len(rows),
        "campaigns": [_serialize(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# POST /api/admin/campaigns/{campaign_id}/force-pause | /force-resume
# ---------------------------------------------------------------------------

async def _load_campaign(campaign_id: str) -> dict:
    try:
        oid = ObjectId(campaign_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Campaign not found")
    campaign = await database.campaigns_collection.find_one({"_id": oid})
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.post("/campaigns/{campaign_id}/force-pause")
async def force_pause_campaign(
    campaign_id: str,
    admin: dict = Depends(get_super_admin),
):
    """Set campaign status to paused. The campaign engine only executes
    enrollments whose campaign status is 'active', so this halts sends."""
    campaign = await _load_campaign(campaign_id)
    await database.campaigns_collection.update_one(
        {"_id": campaign["_id"]},
        {"$set": {
            "status": "paused",
            "admin_forced_pause": True,
            "admin_forced_pause_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }},
    )
    await log_admin_action(
        admin["email"], "campaign.force_pause", "campaign", campaign_id,
        {"previous_status": campaign.get("status"), "name": campaign.get("name")},
    )
    return {"campaign_id": campaign_id, "status": "paused"}


@router.post("/campaigns/{campaign_id}/force-resume")
async def force_resume_campaign(
    campaign_id: str,
    admin: dict = Depends(get_super_admin),
):
    campaign = await _load_campaign(campaign_id)
    await database.campaigns_collection.update_one(
        {"_id": campaign["_id"]},
        {"$set": {
            "status": "active",
            "admin_forced_pause": False,
            "updated_at": datetime.utcnow(),
        }},
    )
    await log_admin_action(
        admin["email"], "campaign.force_resume", "campaign", campaign_id,
        {"previous_status": campaign.get("status"), "name": campaign.get("name")},
    )
    return {"campaign_id": campaign_id, "status": "active"}


# ---------------------------------------------------------------------------
# GET /api/admin/webhooks/recent
# ---------------------------------------------------------------------------

@router.get("/webhooks/recent")
async def get_recent_webhooks(
    source: Optional[str] = Query(None, description="unipile"),
    status: Optional[str] = Query(None, description="ok|error"),
    limit: int = Query(100, ge=1, le=500),
    _admin: dict = Depends(get_super_admin),
):
    query: dict = {}
    if source:
        query["source"] = source
    if status:
        query["status"] = status
    rows = await (
        database.webhook_log_collection.find(query)
        .sort("received_at", -1).limit(limit).to_list(limit)
    )
    return {"webhooks": [_serialize(r) for r in rows], "count": len(rows)}


# ---------------------------------------------------------------------------
# GET /api/admin/audit-log
# ---------------------------------------------------------------------------

@router.get("/audit-log")
async def get_audit_log(
    action: Optional[str] = Query(None),
    admin_email: Optional[str] = Query(None),
    target_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(get_super_admin),
):
    query: dict = {}
    if action:
        query["action"] = action
    if admin_email:
        query["admin_email"] = admin_email
    if target_id:
        query["target_id"] = target_id

    total = await database.admin_audit_log_collection.count_documents(query)
    skip = (page - 1) * page_size
    rows = await (
        database.admin_audit_log_collection.find(query)
        .sort("created_at", -1).skip(skip).limit(page_size)
        .to_list(page_size)
    )
    return {
        "entries": [_serialize(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ---------------------------------------------------------------------------
# Runtime gate flags — GET / PATCH /api/admin/settings/flags
# ---------------------------------------------------------------------------

@router.get("/settings/flags")
async def get_flags(_admin: dict = Depends(get_super_admin)):
    overrides = await system_settings_service.get_all_flags()
    overrides_by_key = {o["key"]: o for o in overrides}
    flags = []
    for name, env_default in KNOWN_FLAGS.items():
        o = overrides_by_key.get(name)
        flags.append({
            "name": name,
            "env_default": env_default,
            "override": o["value"] if o else None,
            "effective": o["value"] if o else env_default,
            "updated_at": o.get("updated_at") if o else None,
            "updated_by": o.get("updated_by") if o else None,
        })
    return {"flags": flags}


class FlagsPatchRequest(BaseModel):
    """{name: true|false} sets an override; {name: null} clears it."""
    flags: dict[str, Optional[bool]]


@router.patch("/settings/flags")
async def patch_flags(
    body: FlagsPatchRequest,
    admin: dict = Depends(get_super_admin),
):
    unknown = [k for k in body.flags if k not in KNOWN_FLAGS]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown flag(s): {unknown}. Allowed: {sorted(KNOWN_FLAGS)}",
        )

    results = {}
    for name, value in body.flags.items():
        if value is None:
            await system_settings_service.delete_flag(name)
            results[name] = {"override": None, "effective": KNOWN_FLAGS[name]}
        else:
            await system_settings_service.set_flag(name, bool(value), updated_by=admin["email"])
            results[name] = {"override": bool(value), "effective": bool(value)}

    await log_admin_action(
        admin["email"], "settings.flags_update", "system_settings", None,
        {"changes": body.flags},
    )
    return {"flags": results}
