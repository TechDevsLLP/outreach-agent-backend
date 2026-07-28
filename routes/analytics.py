"""
Analytics Dashboard Routes (Phase 6.5)
Pipeline velocity, enrichment ROI, industry performance, outreach effectiveness.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId

from database import (
    prospects_collection, enrichment_runs_collection, conversations_collection,
    campaigns_collection, campaign_enrollments_collection, meetings_collection,
    reply_classifications_collection, prospect_state_collection,
    campaign_prospect_state_collection,
)
import database
from auth import get_account_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


def _acct_any(account_id) -> dict:
    """Match an account_id stored as either a string or an ObjectId.

    Collections disagree on the storage type: campaign_enrollments and
    industries keep ObjectId, while conversations / meetings /
    reply_classifications / prospect_state keep strings. Querying with the
    wrong type silently returns zero rows — which is exactly how this
    dashboard used to report 0 emails sent on accounts with live outreach.
    """
    account_id_str = str(account_id)
    values: list = [account_id_str]
    try:
        values.append(ObjectId(account_id_str))
    except Exception:
        pass
    return {"$in": values}


@router.get("/overview")
async def get_analytics_overview(account_ctx=Depends(get_account_context)):
    """
    High-level analytics overview.

    Everything reported here is derived from a source that is actually written
    to by the live pipeline:
      - prospects/enrichment  → prospect_state + prospects
      - sends                 → conversations (outbound messages)
      - replies               → conversations (inbound messages), counted as
                                distinct prospects who replied, not raw messages
      - meetings              → meetings collection

    Open tracking was removed from the product (July 2026), so no open counts
    or open rates are returned.
    """
    account_id_str = str(account_ctx["account"]["_id"])
    acct = _acct_any(account_id_str)

    # Prospect counts: keyed off prospect_state (multi-tenant overlay)
    total_prospects = await prospect_state_collection.count_documents({"account_id": account_id_str})

    # Enrichment state and fit scores both live on campaign_prospect_state, NOT
    # on the shared prospect doc or the prospect_state overlay:
    #   - prospects.enrichment_status is only ever None/"not_started" now
    #   - prospect_state has no ai_score field at all
    # Reading either of those (as this endpoint used to) reports a hard zero on
    # accounts with thousands of scored, enriched prospects.
    enrichment_pipeline = [
        {"$match": {"account_id": account_id_str}},
        {"$group": {
            "_id": {"$ifNull": ["$enrichment.state", "not_started"]},
            "count": {"$sum": 1},
        }},
    ]
    enrichment_rows = await campaign_prospect_state_collection.aggregate(
        enrichment_pipeline
    ).to_list(None)
    enrichment_states = {r["_id"]: r["count"] for r in enrichment_rows}

    # State vocabulary written by enrichment_job_service:
    #   queued / running → in flight
    #   succeeded        → enriched
    #   retryable_failure / not_found → failed to enrich
    def _states(*names: str) -> int:
        return sum(enrichment_states.get(n, 0) for n in names)

    enriched = _states("succeeded")
    in_progress = _states("queued", "running")
    failed = _states("retryable_failure", "not_found", "failed")

    # Average campaign-fit score. There is one score per prospect
    # (campaign_prospect_state.score.value) — the old response exposed that same
    # number three times under three different labels, which read as three
    # independent metrics.
    score_pipeline = [
        {"$match": {"account_id": account_id_str, "score.value": {"$ne": None}}},
        {"$group": {"_id": None, "avg": {"$avg": "$score.value"}, "scored": {"$sum": 1}}},
    ]
    score_result = await campaign_prospect_state_collection.aggregate(score_pipeline).to_list(1)
    score_data = score_result[0] if score_result else {}

    # ── Sends and replies, per channel, from conversations ────────────────────
    # Sends are counted as messages; replies are counted as *conversations with
    # at least one inbound message*, i.e. distinct people who replied. Counting
    # raw inbound messages would inflate the rate for chatty threads.
    channel_pipeline = [
        {"$match": {"account_id": acct, "channel": {"$in": ["email", "linkedin"]}}},
        {"$project": {
            "channel": 1,
            "outbound": {
                "$size": {"$filter": {
                    "input": {"$ifNull": ["$messages", []]},
                    "cond": {"$eq": ["$$this.direction", "outbound"]},
                }}
            },
            "inbound": {
                "$size": {"$filter": {
                    "input": {"$ifNull": ["$messages", []]},
                    "cond": {"$eq": ["$$this.direction", "inbound"]},
                }}
            },
        }},
        {"$group": {
            "_id": "$channel",
            "sent": {"$sum": "$outbound"},
            "contacted": {"$sum": {"$cond": [{"$gt": ["$outbound", 0]}, 1, 0]}},
            "replied": {"$sum": {"$cond": [{"$gt": ["$inbound", 0]}, 1, 0]}},
        }},
    ]
    channel_rows = await conversations_collection.aggregate(channel_pipeline).to_list(None)
    by_channel = {r["_id"]: r for r in channel_rows}

    def _ch(name: str) -> dict:
        return by_channel.get(name) or {"sent": 0, "contacted": 0, "replied": 0}

    email_stats = _ch("email")
    linkedin_stats = _ch("linkedin")

    # LinkedIn outbound split by outreach type (connection request vs InMail vs DM)
    linkedin_type_pipeline = [
        {"$match": {"account_id": acct, "channel": "linkedin"}},
        {"$unwind": "$messages"},
        {"$match": {"messages.direction": "outbound"}},
        {"$group": {
            "_id": {"$ifNull": ["$messages.outreach_type", "connection_request"]},
            "count": {"$sum": 1},
        }},
    ]
    linkedin_type_rows = await conversations_collection.aggregate(linkedin_type_pipeline).to_list(None)
    type_counts = {r["_id"]: r["count"] for r in linkedin_type_rows}

    def _type_count(*names: str) -> int:
        return sum(type_counts.get(n, 0) for n in names)

    total_linkedin_connections = _type_count("connection_request", "linkedin_connection")
    total_linkedin_inmails = _type_count("inmail", "linkedin_inmail")

    # ── Meetings booked ───────────────────────────────────────────────────────
    # Reported at account level only: meeting docs carry no channel field, so a
    # per-channel split would be a permanent pair of zeros rather than a signal.
    total_meetings = await meetings_collection.count_documents(
        {"account_id": acct, "status": {"$in": ["booked", "confirmed"]}}
    )
    if total_meetings == 0:
        # Older meetings may predate the status field entirely.
        total_meetings = await meetings_collection.count_documents({"account_id": acct})

    total_sent = email_stats["sent"] + linkedin_stats["sent"]
    total_contacted = email_stats["contacted"] + linkedin_stats["contacted"]
    total_replied = email_stats["replied"] + linkedin_stats["replied"]

    def _rate(numerator: int, denominator: int) -> float:
        return round(numerator / denominator * 100, 1) if denominator > 0 else 0.0

    def _channel_block(stats: dict) -> dict:
        return {
            "sent": stats["sent"],
            "contacted": stats["contacted"],
            "replied": stats["replied"],
            # Reply rate is people-who-replied ÷ people-contacted, not ÷ messages.
            "reply_rate": _rate(stats["replied"], stats["contacted"]),
        }

    return {
        "status": "success",
        "prospects": {
            "total": total_prospects,
            "enriched": enriched,
            "in_progress": in_progress,
            "failed": failed,
            "enrichment_rate": round(enriched / total_prospects * 100, 1) if total_prospects > 0 else 0,
        },
        "scores": {
            "avg_fit_score": round(score_data.get("avg") or 0, 1),
            "scored_prospects": score_data.get("scored", 0),
        },
        "outreach": {
            "total_sent": total_sent,
            "total_contacted": total_contacted,
            "total_replied": total_replied,
            "reply_rate": _rate(total_replied, total_contacted),
            "total_emails_sent": email_stats["sent"],
            "total_linkedin_connections": total_linkedin_connections,
            "total_linkedin_inmails": total_linkedin_inmails,
            "meetings_booked": total_meetings,
        },
        "channel_performance": {
            "email": _channel_block(email_stats),
            "linkedin": _channel_block(linkedin_stats),
        },
    }


@router.get("/pipeline-velocity")
async def get_pipeline_velocity(
    days: int = Query(30, description="Time period in days"),
    account_ctx=Depends(get_account_context)
):
    """
    Pipeline velocity: Average time spent in each enrichment stage.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    since = datetime.utcnow() - timedelta(days=days)

    runs = await enrichment_runs_collection.find({
        "account_id": account_id,
        "status": "completed",
        "created_at": {"$gte": since},
    }).to_list(None)

    if not runs:
        return {"status": "success", "message": "No completed runs in period", "velocity": {}}

    # Calculate duration stats
    durations = []
    for run in runs:
        created_at = run.get("created_at")
        completed_at = run.get("completed_at")
        if created_at and completed_at:
            duration_mins = (completed_at - created_at).total_seconds() / 60
            durations.append(duration_mins)

    avg_duration = sum(durations) / len(durations) if durations else 0
    min_duration = min(durations) if durations else 0
    max_duration = max(durations) if durations else 0

    # Prospects per run stats
    prospects_per_run = [r.get("prospects_processed", 0) for r in runs]
    avg_prospects = sum(prospects_per_run) / len(prospects_per_run) if prospects_per_run else 0

    # Throughput: prospects per hour
    throughput = (avg_prospects / avg_duration * 60) if avg_duration > 0 else 0

    return {
        "status": "success",
        "period_days": days,
        "total_runs": len(runs),
        "velocity": {
            "avg_duration_minutes": round(avg_duration, 2),
            "min_duration_minutes": round(min_duration, 2),
            "max_duration_minutes": round(max_duration, 2),
            "avg_prospects_per_run": round(avg_prospects, 1),
            "throughput_prospects_per_hour": round(throughput, 1),
        },
        "run_stats": {
            "total_profiles_scraped": sum(r.get("profiles_scraped", 0) for r in runs),
            "total_companies_scraped": sum(r.get("companies_scraped", 0) for r in runs),
            "total_ai_assessments": sum(r.get("ai_assessments_done", 0) for r in runs),
            "total_outreach_generated": sum(r.get("outreach_generated", 0) for r in runs),
            "total_failed": sum(r.get("prospects_failed", 0) for r in runs),
        },
    }


@router.get("/enrichment-roi")
async def get_enrichment_roi(account_ctx=Depends(get_account_context)):
    """
    Enrichment ROI:
    - Cost per enriched prospect
    - Cost per replied prospect
    - Conversion funnel
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    # Cost constants (approximate)
    COST_PER_PROFILE_SCRAPE = 0.004   # Apify
    COST_PER_COMPANY_SCRAPE = 0.002   # Apify
    COST_PER_NEWS_RESEARCH = 0.006    # Apify Google News + Gemini Flash validation
    COST_PER_AI_ASSESSMENT = 0.015    # Claude
    COST_PER_OUTREACH_GEN = 0.0008    # Gemini
    COST_PER_EMAIL = 0.0004           # rough per-send estimate (Gmail/Zoho/SMTP)

    # Aggregate stats — counts via prospect_state (new multi-tenant schema)
    total_prospects = await prospect_state_collection.count_documents({"account_id": account_id_str})
    # For enrichment/replied counts join through prospect IDs
    roi_state_docs = await prospect_state_collection.find(
        {"account_id": account_id_str}, {"prospect_id": 1}
    ).to_list(None)
    roi_pids = [d["prospect_id"] for d in roi_state_docs if d.get("prospect_id")]

    def _roi_oid(x):
        try:
            return ObjectId(x) if isinstance(x, str) else x
        except Exception:
            return None

    roi_oids = [o for x in roi_pids if (o := _roi_oid(str(x)))]
    enriched = await prospects_collection.count_documents({"_id": {"$in": roi_oids}, "enrichment_status": "completed"}) if roi_oids else 0
    replied = await prospects_collection.count_documents({"_id": {"$in": roi_oids}, "enrichment_status": "replied"}) if roi_oids else 0

    # Outreach sent count from conversations (reliable source)
    email_sent_pipeline = [
        {"$match": {"account_id": account_id, "channel": "email"}},
        {"$unwind": "$messages"},
        {"$match": {"messages.direction": "outbound"}},
        {"$count": "total"},
    ]
    email_sent_result = await conversations_collection.aggregate(email_sent_pipeline).to_list(1)
    total_sent = email_sent_result[0]["total"] if email_sent_result else 0

    # Estimated costs
    total_cost = (
        enriched * COST_PER_PROFILE_SCRAPE +
        enriched * COST_PER_COMPANY_SCRAPE +
        enriched * COST_PER_NEWS_RESEARCH +
        enriched * COST_PER_AI_ASSESSMENT +
        enriched * COST_PER_OUTREACH_GEN +
        total_sent * COST_PER_EMAIL
    )

    cost_per_enriched = total_cost / enriched if enriched > 0 else 0
    cost_per_replied = total_cost / replied if replied > 0 else 0

    # Conversion funnel
    funnel = {
        "prospects_generated": total_prospects,
        "prospects_enriched": enriched,
        "emails_sent": total_sent,
        "prospects_replied": replied,
    }

    funnel_rates = {
        "generation_to_enrichment": round(enriched / total_prospects * 100, 2) if total_prospects > 0 else 0,
        "enrichment_to_sent": round(total_sent / enriched * 100, 2) if enriched > 0 else 0,
        "sent_to_replied": round(replied / total_sent * 100, 2) if total_sent > 0 else 0,
        "overall_conversion": round(replied / total_prospects * 100, 2) if total_prospects > 0 else 0,
    }

    return {
        "status": "success",
        "roi": {
            "estimated_total_cost": round(total_cost, 2),
            "cost_per_enriched_prospect": round(cost_per_enriched, 4),
            "cost_per_replied_prospect": round(cost_per_replied, 4),
        },
        "funnel": funnel,
        "funnel_rates": funnel_rates,
    }


@router.get("/industry-performance")
async def get_industry_performance(account_ctx=Depends(get_account_context)):
    """
    Industry performance comparison:
    - Prospects generated per industry
    - Average scores per industry
    - Reply rates per industry
    """
    # TODO: Rearchitect to join prospect_state (account_id) → prospects (_id) → group by company_industry_id.
    # Skipped for now — prospects no longer carry account_id so the old pipeline returns empty.
    return {
        "status": "success",
        "total_industries": 0,
        "industry_performance": [],
    }


@router.get("/score-distribution")
async def get_score_distribution(account_ctx=Depends(get_account_context)):
    """
    Distribution of prospect scores across score bands.
    Useful for understanding the quality of your prospect pool.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    bands = [
        {"label": "Excellent (90-100)", "min": 90, "max": 100},
        {"label": "Good (75-89)", "min": 75, "max": 90},
        {"label": "Average (50-74)", "min": 50, "max": 75},
        {"label": "Poor (25-49)", "min": 25, "max": 50},
        {"label": "Very Poor (0-24)", "min": 0, "max": 25},
    ]

    # Scores now live on prospect_state as ai_score
    total = await prospect_state_collection.count_documents(
        {"account_id": account_id_str, "ai_score": {"$exists": True, "$ne": None}}
    )
    score_field = "ai_score"

    distribution = []
    for band in bands:
        count = await prospect_state_collection.count_documents({
            "account_id": account_id_str,
            score_field: {"$gte": band["min"], "$lt": band["max"]}
        })
        distribution.append({
            "label": band["label"],
            "count": count,
            "percentage": round(count / total * 100, 2) if total > 0 else 0,
        })

    return {
        "status": "success",
        "score_field_used": score_field,
        "total_scored_prospects": total,
        "distribution": distribution,
    }


@router.get("/timezone-distribution")
async def get_timezone_distribution(account_ctx=Depends(get_account_context)):
    """
    Distribution of prospects by timezone.
    Useful for scheduling bulk email campaigns.
    """
    # TODO: Rearchitect to join prospect_state (account_id) → prospects (_id) → group by timezone.
    # Skipped for now — prospects no longer carry account_id so the old pipeline returns empty.
    return {
        "status": "success",
        "timezones": [],
        "prospects_without_timezone": 0,
    }


@router.get("/recent-activity")
async def get_recent_activity(
    days: int = Query(7, description="Days to look back"),
    account_ctx=Depends(get_account_context)
):
    """
    Recent activity: new prospects, enrichments, replies in the last N days.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)
    since = datetime.utcnow() - timedelta(days=days)

    # new_prospects: prospect_state docs created in period
    new_prospects = await prospect_state_collection.count_documents(
        {"account_id": account_id_str, "created_at": {"$gte": since}}
    )

    # For enrichment/reply counts we need to look at the global prospect docs for this account's pids
    ra_state_docs = await prospect_state_collection.find(
        {"account_id": account_id_str}, {"prospect_id": 1}
    ).to_list(None)
    ra_pids = [d["prospect_id"] for d in ra_state_docs if d.get("prospect_id")]

    def _ra_oid(x):
        try:
            return ObjectId(x) if isinstance(x, str) else x
        except Exception:
            return None

    ra_oids = [o for x in ra_pids if (o := _ra_oid(str(x)))]

    if ra_oids:
        newly_enriched = await prospects_collection.count_documents({
            "_id": {"$in": ra_oids},
            "enrichment_completed_at": {"$gte": since}
        })
        new_replies = await prospects_collection.count_documents({
            "_id": {"$in": ra_oids},
            "last_outreach_replied": {"$gte": since}
        })
        new_opens = await prospects_collection.count_documents({
            "_id": {"$in": ra_oids},
            "last_outreach_opened": {"$gte": since}
        })
    else:
        newly_enriched = new_replies = new_opens = 0

    recent_runs = await enrichment_runs_collection.count_documents({
        "account_id": account_id,
        "created_at": {"$gte": since}
    })

    return {
        "status": "success",
        "period_days": days,
        "activity": {
            "new_prospects": new_prospects,
            "enriched_prospects": newly_enriched,
            "email_opens": new_opens,
            "replies_received": new_replies,
            "enrichment_runs": recent_runs,
        }
    }


# ── Phase 6.6: Duplicate Detection Endpoints ──

@router.get("/duplicates/stats")
async def get_duplication_stats(account_ctx=Depends(get_account_context)):
    """
    Global duplicate detection statistics.
    Shows how many prospects appear in multiple industries.
    """
    from services.duplicate_detection_service import get_duplication_stats
    stats = await get_duplication_stats()
    return {"status": "success", **stats}


@router.get("/duplicates/cross-industry")
async def get_cross_industry_prospects(
    min_industry_count: int = Query(2, description="Minimum number of industries a prospect must appear in"),
    account_ctx=Depends(get_account_context)
):
    """
    Find prospects appearing in multiple industries.
    These are high-value prospects worth prioritizing - multiple industries wanted them.
    """
    from services.duplicate_detection_service import find_cross_industry_prospects

    prospects = await find_cross_industry_prospects(min_industry_count)

    # Serialize ObjectIds
    serialized = []
    for p in prospects:
        p["_id"] = str(p["_id"])
        serialized.append(p)

    return {
        "status": "success",
        "count": len(serialized),
        "prospects": serialized,
    }


# ── Per-Industry Outreach Tracking ──

@router.get("/industry/{industry_id}/outreach")
async def get_industry_outreach(
    industry_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    channel: Optional[str] = Query(None, description="Filter by channel: email, linkedin_connection, linkedin_inmail"),
    account_ctx=Depends(get_account_context),
):
    """
    All sent outreach for a specific industry — emails, LinkedIn connections, InMails.
    Combines data from conversations (emails/LinkedIn messages) and prospect tracking
    (connection requests) for a complete picture.
    """
    from database import industries_collection
    account_id = ObjectId(account_ctx["account"]["_id"])

    # Validate industry exists
    try:
        ind_oid = ObjectId(industry_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid industry ID")

    industry = await industries_collection.find_one({"_id": ind_oid, "account_id": account_id}, {"name": 1})
    if not industry:
        raise HTTPException(status_code=404, detail="Industry not found")

    industry_name = industry.get("name", industry_id)

    # Find all prospects in this industry
    prospect_query = {
        "account_id": account_id,
        "$or": [
            {"source_industry_ids": industry_id},
            {"industry_id": industry_id},
        ]
    }
    prospect_ids_cursor = prospects_collection.find(
        prospect_query,
        {"_id": 1, "full_name": 1, "email": 1, "company_name": 1, "linkedin": 1,
         "connection_request_sent_at": 1, "connection_accepted_at": 1},
    )
    prospects_list = await prospect_ids_cursor.to_list(None)
    prospect_map = {}
    for p in prospects_list:
        pid = str(p["_id"])
        prospect_map[pid] = {
            "name": p.get("full_name") or "",
            "email": p.get("email") or "",
            "company": p.get("company_name") or "",
            "linkedin": p.get("linkedin") or "",
            "connection_request_sent_at": p.get("connection_request_sent_at"),
            "connection_accepted_at": p.get("connection_accepted_at"),
        }

    if not prospect_map:
        return {
            "status": "success",
            "industry_id": industry_id,
            "industry_name": industry_name,
            "total_prospects": 0,
            "summary": {
                "total_outreach": 0,
                "emails_sent": 0,
                "linkedin_connections_sent": 0,
                "linkedin_inmails_sent": 0,
                "connections_accepted": 0,
            },
            "outreach_items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0,
        }

    prospect_id_list = list(prospect_map.keys())

    # ── Gather all outreach items from conversations ──
    # Each outbound message in a conversation = one outreach event
    conv_query: dict = {"account_id": account_id, "prospect_id": {"$in": prospect_id_list}}
    if channel == "email":
        conv_query["channel"] = "email"
    elif channel in ("linkedin_connection", "linkedin_inmail"):
        conv_query["channel"] = "linkedin"

    conversations = await conversations_collection.find(
        conv_query,
        {"prospect_id": 1, "channel": 1, "messages": 1, "unipile_chat_id": 1,
         "email_thread_subject": 1},
    ).to_list(None)

    outreach_items = []

    for conv in conversations:
        pid = conv["prospect_id"]
        prospect_info = prospect_map.get(pid, {})
        conv_channel = conv.get("channel", "")

        for msg in conv.get("messages", []):
            if msg.get("direction") != "outbound":
                continue

            # Determine the specific outreach type
            if conv_channel == "email":
                outreach_type = "email"
            elif conv_channel == "linkedin":
                # Use explicit outreach_type if available (new messages)
                msg_outreach_type = msg.get("outreach_type")
                if msg_outreach_type == "connection_request":
                    outreach_type = "linkedin_connection"
                elif msg_outreach_type == "inmail":
                    outreach_type = "linkedin_inmail"
                elif msg_outreach_type == "linkedin_message":
                    outreach_type = "linkedin_message"
                elif msg.get("subject"):
                    # Fallback for old messages without outreach_type
                    outreach_type = "linkedin_inmail"
                else:
                    outreach_type = "linkedin_connection"
            else:
                outreach_type = conv_channel

            # Apply channel filter if specified
            if channel and outreach_type != channel:
                continue

            outreach_items.append({
                "prospect_id": pid,
                "prospect_name": prospect_info.get("name", ""),
                "prospect_email": prospect_info.get("email", ""),
                "prospect_company": prospect_info.get("company", ""),
                "prospect_linkedin": prospect_info.get("linkedin", ""),
                "channel": outreach_type,
                "subject": msg.get("subject") or conv.get("email_thread_subject") or "",
                "message_preview": (msg.get("content_text") or "")[:200],
                "sent_at": msg.get("timestamp"),
                "status": msg.get("status", "sent"),
                "variant_id": msg.get("variant_id"),
                "message_id": msg.get("message_id"),
                "connection_accepted": prospect_info.get("connection_accepted_at") is not None
                    if outreach_type == "linkedin_connection" else None,
            })

    # Sort by sent_at descending
    outreach_items.sort(key=lambda x: x.get("sent_at") or datetime.min, reverse=True)

    # ── Summary counts (unique prospects per channel, not total messages) ──
    emails_sent = len({i["prospect_id"] for i in outreach_items if i["channel"] == "email"})
    linkedin_connections_sent = len({i["prospect_id"] for i in outreach_items if i["channel"] == "linkedin_connection"})
    linkedin_inmails_sent = len({i["prospect_id"] for i in outreach_items if i["channel"] == "linkedin_inmail"})
    linkedin_messages_sent = len({i["prospect_id"] for i in outreach_items if i["channel"] == "linkedin_message"})
    connections_accepted = sum(
        1 for pid, info in prospect_map.items()
        if info.get("connection_accepted_at") is not None
    )

    total = len(outreach_items)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0

    # Paginate
    start = (page - 1) * page_size
    end = start + page_size
    paginated_items = outreach_items[start:end]

    # Serialize datetimes for JSON
    for item in paginated_items:
        if item.get("sent_at") and hasattr(item["sent_at"], "isoformat"):
            item["sent_at"] = item["sent_at"].isoformat()

    return {
        "status": "success",
        "industry_id": industry_id,
        "industry_name": industry_name,
        "total_prospects": len(prospect_map),
        "summary": {
            "total_outreach": total,
            "emails_sent": emails_sent,
            "linkedin_connections_sent": linkedin_connections_sent,
            "linkedin_inmails_sent": linkedin_inmails_sent,
            "linkedin_messages_sent": linkedin_messages_sent,
            "connections_accepted": connections_accepted,
        },
        "outreach_items": paginated_items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Funnel analytics
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/funnel")
async def get_funnel_analytics(
    campaign_id: Optional[str] = None,
    account_ctx=Depends(get_account_context),
):
    """
    Returns a top-of-funnel breakdown:
    sourced → enrolled → sent → replied → classified → meeting_proposed →
    meeting_booked.

    If campaign_id is provided, scoped to that campaign only; otherwise
    aggregated across all campaigns for the account.

    Sourced/enrolled come from campaign_enrollments. Everything downstream is
    read from the collections the pipeline actually writes — send_attempts,
    conversations, meetings — because the enrollment doc's `messages_sent`,
    `last_sent_at`, and post-send statuses are never populated, which pinned
    the bottom four stages of this funnel at zero.
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_ctx["account"]["_id"])

    # Build the base enrollment match filter
    enroll_match: dict = {"account_id": account_id}
    if campaign_id:
        try:
            enroll_match["campaign_id"] = ObjectId(campaign_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid campaign_id")

    # ── Stage 1: sourced — unique prospects ever enrolled ──────────────────
    sourced_pipeline = [
        {"$match": enroll_match},
        {"$group": {"_id": "$prospect_id"}},
        {"$count": "total"},
    ]
    sourced_result = await campaign_enrollments_collection.aggregate(sourced_pipeline).to_list(1)
    sourced = sourced_result[0]["total"] if sourced_result else 0

    # Helper: avoid division by zero
    def conv(n: int) -> float:
        if sourced == 0:
            return 0.0
        return round(n / sourced * 100, 1)

    # ── Stage 2: enrolled — status not in (created, pending) ──────────────
    enrolled_match = {**enroll_match, "status": {"$nin": ["created", "pending", "cascade_waiting"]}}
    enrolled = await campaign_enrollments_collection.count_documents(enrolled_match)

    # Enrollment ids for this scope — the join key for send_attempts and
    # meetings, both of which record enrollment_id rather than campaign_id.
    enrollment_ids: list[str] = [
        str(d["_id"])
        for d in await campaign_enrollments_collection.find(enroll_match, {"_id": 1}).to_list(None)
    ]

    # ── Stage 3: sent — distinct enrollments with a dispatched message ─────
    if enrollment_ids:
        sent_rows = await database.send_attempts_collection.aggregate([
            {"$match": {"enrollment_id": {"$in": enrollment_ids}, "state": "sent"}},
            {"$group": {"_id": "$enrollment_id"}},
            {"$count": "total"},
        ]).to_list(1)
        sent = sent_rows[0]["total"] if sent_rows else 0
    else:
        sent = 0

    # ── Stage 4: replied — people with an inbound message ─────────────────
    # (There is no "opened" stage: open tracking was removed from the product
    # in July 2026, so messages_opened is never written and the stage always
    # rendered as a 0% bar.)
    #
    # Conversations are keyed by prospect, not enrollment, so a campaign-scoped
    # request narrows by that campaign's prospect ids.
    reply_match: dict = {"account_id": _acct_any(account_id_str)}
    if campaign_id:
        prospect_ids = [
            str(d["prospect_id"])
            for d in await campaign_enrollments_collection.find(
                enroll_match, {"prospect_id": 1}
            ).to_list(None)
            if d.get("prospect_id")
        ]
        if not prospect_ids:
            reply_match["prospect_id"] = {"$in": []}
        else:
            reply_match["prospect_id"] = {"$in": prospect_ids}

    replied_rows = await conversations_collection.aggregate([
        {"$match": reply_match},
        {"$match": {"messages.direction": "inbound"}},
        {"$group": {"_id": "$prospect_id"}},
        {"$count": "total"},
    ]).to_list(1)
    replied = replied_rows[0]["total"] if replied_rows else 0

    # ── Stage 5: classified — reply_classifications for account/campaign ──
    classif_match: dict = {"account_id": account_id_str}
    if campaign_id:
        classif_match["campaign_id"] = campaign_id
    classified = await reply_classifications_collection.count_documents(classif_match)

    # ── Stages 6 & 7: meetings, from the meetings collection ──────────────
    # A meeting is "booked" once a slot is confirmed; every meeting record
    # counts as proposed.
    meeting_scope: dict = {"account_id": _acct_any(account_id_str)}
    if campaign_id:
        meeting_scope["enrollment_id"] = {"$in": enrollment_ids} if enrollment_ids else {"$in": []}

    meeting_proposed = await meetings_collection.count_documents(meeting_scope)
    meeting_booked = await meetings_collection.count_documents(
        {**meeting_scope, "status": {"$in": ["booked", "confirmed"]}}
    )

    stages = [
        {"stage": "sourced",          "count": sourced,          "conversion_rate": conv(sourced)},
        {"stage": "enrolled",         "count": enrolled,         "conversion_rate": conv(enrolled)},
        {"stage": "sent",             "count": sent,             "conversion_rate": conv(sent)},
        {"stage": "replied",          "count": replied,          "conversion_rate": conv(replied)},
        {"stage": "classified",       "count": classified,       "conversion_rate": conv(classified)},
        {"stage": "meeting_proposed", "count": meeting_proposed, "conversion_rate": conv(meeting_proposed)},
        {"stage": "meeting_booked",   "count": meeting_booked,   "conversion_rate": conv(meeting_booked)},
    ]

    return {
        "status": "success",
        "campaign_id": campaign_id,
        "funnel": stages,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Per-classifier (reply category) breakdown
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/per-classifier")
async def get_per_classifier_analytics(
    campaign_id: Optional[str] = None,
    days: int = Query(default=30, ge=1, le=365),
    account_ctx=Depends(get_account_context),
):
    """
    Breakdown of reply classifications by category over the given period.
    Always returns rows for all canonical categories even if count = 0.
    """
    account_id_str = str(account_ctx["account"]["_id"])
    since = datetime.utcnow() - timedelta(days=days)

    # Build match filter (account_id stored as string in this collection)
    classif_match: dict = {
        "account_id": account_id_str,
        "created_at": {"$gte": since},
    }
    if campaign_id:
        classif_match["campaign_id"] = campaign_id

    # Aggregation pipeline: group by category, count total / auto / escalated
    pipeline = [
        {"$match": classif_match},
        {
            "$group": {
                "_id": "$category",
                "count": {"$sum": 1},
                "auto_handled": {
                    "$sum": {"$cond": [{"$eq": ["$auto_handled", True]}, 1, 0]}
                },
                "escalated": {
                    "$sum": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": ["$escalated", True]},
                                    {"$eq": ["$auto_handled", False]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
    ]

    raw_results = await reply_classifications_collection.aggregate(pipeline).to_list(None)

    # Index results by category
    by_category: dict = {}
    for row in raw_results:
        cat = row["_id"] or "UNKNOWN"
        by_category[cat] = {
            "count": row["count"],
            "auto_handled": row["auto_handled"],
            "escalated": row["escalated"],
        }

    total_classified = sum(v["count"] for v in by_category.values())

    canonical_categories = [
        "POSITIVE",
        "QUESTION",
        "SOFT_OBJECTION",
        "HARD_OBJECTION",
        "OOO",
        "UNSUBSCRIBE",
    ]

    # Ensure all canonical categories appear
    for cat in canonical_categories:
        if cat not in by_category:
            by_category[cat] = {"count": 0, "auto_handled": 0, "escalated": 0}

    # Build sorted category list (canonical first, then any extras)
    extra_cats = [c for c in by_category if c not in canonical_categories]
    ordered_cats = canonical_categories + sorted(extra_cats)

    def pct(n: int) -> float:
        if total_classified == 0:
            return 0.0
        return round(n / total_classified * 100, 1)

    categories = []
    for cat in ordered_cats:
        data = by_category[cat]
        categories.append({
            "category": cat,
            "count": data["count"],
            "pct": pct(data["count"]),
            "auto_handled": data["auto_handled"],
            "escalated": data["escalated"],
        })

    total_auto = sum(v["auto_handled"] for v in by_category.values())
    total_escalated = sum(v["escalated"] for v in by_category.values())

    auto_handle_rate = round(total_auto / total_classified * 100, 1) if total_classified else 0.0
    escalation_rate = round(total_escalated / total_classified * 100, 1) if total_classified else 0.0

    return {
        "status": "success",
        "period_days": days,
        "campaign_id": campaign_id,
        "total_classified": total_classified,
        "categories": categories,
        "auto_handle_rate": auto_handle_rate,
        "escalation_rate": escalation_rate,
    }
