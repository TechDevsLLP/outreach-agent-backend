"""
Campaign discovery SSE stream.

GET /api/campaigns/{campaign_id}/discovery-stream

Streams live discovery progress to the UI while run_fast_discovery is working:
  event: progress  — counters/status snapshot, emitted whenever any field changes
  event: company   — one per newly sourced company (db_match or gemini_grounded)
  event: prospect  — one per newly created enrollment (name/title/company/fit)
Heartbeat comments every ~30s keep proxies from closing the connection. The
stream ends (server closes) when discovery_status reaches a terminal state.

Modeled on services/notification_service.event_stream: a Mongo-polling async
generator behind a StreamingResponse (no broker; ~2s poll granularity, which
matches the incremental writes made by curated_discovery_service).
"""

import asyncio
import json
import logging
from datetime import datetime

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

import database
from auth import get_account_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/campaigns", tags=["campaigns"])

_POLL_SECONDS = 2.0
_HEARTBEAT_SECONDS = 30
_MAX_STREAM_SECONDS = 45 * 60  # hard cap so abandoned tabs can't leak generators
_TERMINAL_STATUSES = {"completed", "failed", "awaiting_approval"}

_PROGRESS_FIELDS = [
    "discovery_status",
    "discovery_error",
    "discovery_companies_matched",
    "curated_companies_sourced",
    "curated_companies_scraped",
    "curated_companies_approved",
    "discovery_companies_found",
    "discovery_prospects_found",
    "discovery_prospects_from_db",
    "discovery_prospects_from_apify",
    "discovery_prospects_planned",
    "discovery_prospects_enrolled",
    "total_enrolled",
    "message_gen_status",
    "discovery_topup_active",
    "discovery_topup_message",
]


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _progress_payload(campaign: dict) -> dict:
    return {f: campaign.get(f) for f in _PROGRESS_FIELDS}


async def _discovery_event_stream(campaign_oid: ObjectId, campaign_id: str, account_values: list):
    started = datetime.utcnow()
    last_progress: dict | None = None
    last_company_id: ObjectId | None = None
    last_enrollment_id: ObjectId | None = None
    last_heartbeat = datetime.utcnow()
    first_cycle = True

    try:
        while True:
            campaign = await database.campaigns_collection.find_one(
                {"_id": campaign_oid, "account_id": {"$in": account_values}},
                {f: 1 for f in _PROGRESS_FIELDS},
            )
            if not campaign:
                break

            progress = _progress_payload(campaign)
            if progress != last_progress:
                last_progress = progress
                yield _sse("progress", progress)

            # New sourced companies since the last cycle (rows are inserted
            # incrementally by curated_discovery_service as batches land).
            co_filter: dict = {"campaign_id": campaign_id}
            if last_company_id is not None:
                co_filter["_id"] = {"$gt": last_company_id}
            async for co in database.sourced_companies_collection.find(co_filter).sort("_id", 1).limit(200):
                last_company_id = co["_id"]
                # On the first cycle just advance the cursor — replaying the full
                # backlog would duplicate what the list endpoint already returned.
                if first_cycle:
                    continue
                yield _sse("company", {
                    "name": co.get("company_name"),
                    "linkedin_url": co.get("company_linkedin_url"),
                    "source": co.get("source"),
                    "employee_scrape_status": co.get("employee_scrape_status"),
                })

            # New enrollments since the last cycle (pre-enrolled per chunk).
            enr_filter: dict = {"campaign_id": campaign_oid}
            if last_enrollment_id is not None:
                enr_filter["_id"] = {"$gt": last_enrollment_id}
            new_enrollments: list[dict] = []
            async for enr in database.campaign_enrollments_collection.find(
                enr_filter, {"prospect_id": 1}
            ).sort("_id", 1).limit(200):
                last_enrollment_id = enr["_id"]
                if not first_cycle:
                    new_enrollments.append(enr)
            if new_enrollments:
                p_ids = [e["prospect_id"] for e in new_enrollments if e.get("prospect_id")]
                prospects_by_id = {}
                if p_ids:
                    async for p in database.prospects_collection.find(
                        {"_id": {"$in": p_ids}},
                        {"full_name": 1, "job_title": 1, "company_name": 1, "fit_score": 1},
                    ):
                        prospects_by_id[p["_id"]] = p
                for enr in new_enrollments:
                    p = prospects_by_id.get(enr.get("prospect_id")) or {}
                    yield _sse("prospect", {
                        "full_name": p.get("full_name"),
                        "job_title": p.get("job_title"),
                        "company_name": p.get("company_name"),
                        "fit_score": p.get("fit_score"),
                    })

            if (campaign.get("discovery_status") or "") in _TERMINAL_STATUSES:
                break

            first_cycle = False
            now = datetime.utcnow()
            if (now - last_heartbeat).total_seconds() >= _HEARTBEAT_SECONDS:
                last_heartbeat = now
                yield ": heartbeat\n\n"
            if (now - started).total_seconds() > _MAX_STREAM_SECONDS:
                break
            await asyncio.sleep(_POLL_SECONDS)
    except asyncio.CancelledError:
        # Client disconnected — normal termination for an SSE generator.
        raise
    except Exception as e:
        logger.warning(f"[discovery-stream:{campaign_id}] stream error: {e}")


@router.get("/{campaign_id}/discovery-stream")
async def discovery_stream(
    campaign_id: str,
    account_ctx=Depends(get_account_context),
):
    """Tenant-scoped SSE stream of live discovery progress (cookie or bearer auth)."""
    account_id = str(account_ctx["account"]["_id"])
    if not ObjectId.is_valid(campaign_id):
        raise HTTPException(status_code=400, detail="Invalid campaign id")
    campaign_oid = ObjectId(campaign_id)

    account_values: list = [account_id]
    if ObjectId.is_valid(account_id):
        account_values.append(ObjectId(account_id))

    exists = await database.campaigns_collection.find_one(
        {"_id": campaign_oid, "account_id": {"$in": account_values}}, {"_id": 1}
    )
    if not exists:
        raise HTTPException(status_code=404, detail="Campaign not found")

    return StreamingResponse(
        _discovery_event_stream(campaign_oid, campaign_id, account_values),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
