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
)
import database
from auth import get_account_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


@router.get("/overview")
async def get_analytics_overview(account_ctx=Depends(get_account_context)):
    """
    High-level analytics overview:
    - Total prospects, enriched, replied
    - Average scores
    - Outreach sent/opened/replied
    """
    account_id = ObjectId(account_ctx["account"]["_id"])
    account_id_str = str(account_id)

    # Prospect counts: keyed off prospect_state (new multi-tenant schema)
    total_prospects = await prospect_state_collection.count_documents({"account_id": account_id_str})

    # Enrichment status counts: join prospect_ids from prospect_state then query prospects
    state_pid_docs = await prospect_state_collection.find(
        {"account_id": account_id_str}, {"prospect_id": 1}
    ).to_list(None)
    prospect_ids_for_acct = [d["prospect_id"] for d in state_pid_docs if d.get("prospect_id")]

    def _oid(x):
        try:
            return ObjectId(x) if isinstance(x, str) else x
        except Exception:
            return None

    prospect_oids = [o for x in prospect_ids_for_acct if (o := _oid(str(x)))]

    enriched = await prospects_collection.count_documents({"_id": {"$in": prospect_oids}, "enrichment_status": "completed"}) if prospect_oids else 0
    replied = await prospects_collection.count_documents({"_id": {"$in": prospect_oids}, "enrichment_status": "replied"}) if prospect_oids else 0
    failed = await prospects_collection.count_documents({"_id": {"$in": prospect_oids}, "enrichment_status": "failed"}) if prospect_oids else 0
    in_progress = await prospects_collection.count_documents({"_id": {"$in": prospect_oids}, "enrichment_status": "in_progress"}) if prospect_oids else 0

    # Average scores: from prospect_state (ai_score = ai_prospect_score equivalent)
    score_pipeline = [
        {"$match": {"account_id": account_id_str, "ai_score": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": None,
            "avg_prospect_score": {"$avg": "$ai_score"},
            "avg_ai_score": {"$avg": "$ai_score"},
            "avg_enhanced_score": {"$avg": "$ai_score"},
        }}
    ]
    score_result = await prospect_state_collection.aggregate(score_pipeline).to_list(1)
    score_data = score_result[0] if score_result else {}

    # Email sent count: count outbound messages in conversations (most reliable source)
    email_sent_pipeline = [
        {"$match": {"account_id": account_id, "channel": "email"}},
        {"$unwind": "$messages"},
        {"$match": {"messages.direction": "outbound"}},
        {"$count": "total"},
    ]
    email_sent_result = await conversations_collection.aggregate(email_sent_pipeline).to_list(1)
    total_emails_sent = email_sent_result[0]["total"] if email_sent_result else 0

    # Open/reply counts from campaign_enrollments (outreach_messages moved off prospect doc)
    outreach_pipeline = [
        {"$match": {"account_id": account_id, "generated_messages.cold_email.subject_variants": {"$exists": True}}},
        {"$unwind": "$generated_messages.cold_email.subject_variants"},
        {"$group": {
            "_id": None,
            "total_opened": {"$sum": "$generated_messages.cold_email.subject_variants.times_opened"},
            "total_replied": {"$sum": "$generated_messages.cold_email.subject_variants.times_replied"},
        }}
    ]
    outreach_result = await campaign_enrollments_collection.aggregate(outreach_pipeline).to_list(1)
    outreach_data = outreach_result[0] if outreach_result else {}

    total_opened = outreach_data.get("total_opened", 0)
    total_replied = outreach_data.get("total_replied", 0)

    # LinkedIn outreach counts from conversations collection (channel = "linkedin")
    linkedin_pipeline = [
        {"$match": {"account_id": account_id, "channel": "linkedin"}},
        {"$unwind": "$messages"},
        {"$match": {"messages.direction": "outbound"}},
        {"$group": {
            "_id": {"$ifNull": ["$messages.outreach_type", "linkedin_connection"]},
            "count": {"$sum": 1},
        }},
    ]
    linkedin_results = await conversations_collection.aggregate(linkedin_pipeline).to_list(None)
    channel_counts = {r["_id"]: r["count"] for r in linkedin_results}

    total_linkedin_connections = channel_counts.get("linkedin_connection", 0)
    total_linkedin_inmails = channel_counts.get("linkedin_inmail", 0)

    # Total industries
    from database import industries_collection
    total_industries = await industries_collection.count_documents({"account_id": account_id})

    return {
        "status": "success",
        "prospects": {
            "total": total_prospects,
            "enriched": enriched,
            "in_progress": in_progress,
            "replied": replied,
            "failed": failed,
            "enrichment_rate": round(enriched / total_prospects * 100, 2) if total_prospects > 0 else 0,
        },
        "scores": {
            "avg_prospect_score": round(score_data.get("avg_prospect_score", 0), 2),
            "avg_ai_score": round(score_data.get("avg_ai_score", 0), 2),
            "avg_enhanced_score": round(score_data.get("avg_enhanced_score", 0), 2),
        },
        "outreach": {
            "total_sent": total_emails_sent,
            "total_opened": total_opened,
            "total_replied": total_replied,
            "open_rate": round(total_opened / total_emails_sent * 100, 2) if total_emails_sent > 0 else 0,
            "reply_rate": round(total_replied / total_emails_sent * 100, 2) if total_emails_sent > 0 else 0,
            "total_emails_sent": total_emails_sent,
            "total_linkedin_connections": total_linkedin_connections,
            "total_linkedin_inmails": total_linkedin_inmails,
        },
        "total_industries": total_industries,
        # Channel performance breakdown expected by the frontend analytics dashboard
        "channel_performance": {
            "email": {
                "assigned": total_prospects,
                "sent": total_emails_sent,
                "replied": total_replied,
                "meetings_booked": 0,
                "reply_rate": round(total_replied / total_emails_sent * 100, 2) if total_emails_sent > 0 else 0,
            },
            "linkedin": {
                "assigned": total_prospects,
                "sent": total_linkedin_connections + total_linkedin_inmails,
                "replied": 0,
                "meetings_booked": 0,
                "reply_rate": 0,
            },
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
    Returns a top-of-funnel breakdown across all stages:
    sourced → enrolled → sent → opened → replied → classified →
    meeting_proposed → meeting_booked.

    If campaign_id is provided, scoped to that campaign only; otherwise
    aggregated across all campaigns for the account.
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

    # ── Stage 3: sent — messages_sent > 0 OR last_sent_at exists ──────────
    sent_match = {
        **enroll_match,
        "$or": [
            {"messages_sent": {"$gt": 0}},
            {"last_sent_at": {"$exists": True, "$ne": None}},
        ],
    }
    sent = await campaign_enrollments_collection.count_documents(sent_match)

    # ── Stage 4: opened — sum of messages_opened field ────────────────────
    opened_pipeline = [
        {"$match": enroll_match},
        {"$group": {"_id": None, "total": {"$sum": {"$ifNull": ["$messages_opened", 0]}}}},
    ]
    opened_result = await campaign_enrollments_collection.aggregate(opened_pipeline).to_list(1)
    opened = opened_result[0]["total"] if opened_result else 0

    # ── Stage 5: replied ──────────────────────────────────────────────────
    replied_statuses = ["replied", "meeting_proposed", "meeting_booked", "completed"]
    replied_match = {
        **enroll_match,
        "$or": [
            {"status": {"$in": replied_statuses}},
            {"conversations": {"$exists": True, "$not": {"$size": 0}}},
        ],
    }
    replied = await campaign_enrollments_collection.count_documents(replied_match)

    # ── Stage 6: classified — reply_classifications for account/campaign ──
    classif_match: dict = {"account_id": account_id_str}
    if campaign_id:
        classif_match["campaign_id"] = campaign_id
    classified = await reply_classifications_collection.count_documents(classif_match)

    # ── Stage 7: meeting_proposed ─────────────────────────────────────────
    proposed_match = {
        **enroll_match,
        "status": {"$in": ["meeting_proposed", "meeting_booked"]},
    }
    meeting_proposed = await campaign_enrollments_collection.count_documents(proposed_match)

    # ── Stage 8: meeting_booked ───────────────────────────────────────────
    booked_match = {**enroll_match, "status": "meeting_booked"}
    meeting_booked = await campaign_enrollments_collection.count_documents(booked_match)

    stages = [
        {"stage": "sourced",          "count": sourced,          "conversion_rate": conv(sourced)},
        {"stage": "enrolled",         "count": enrolled,         "conversion_rate": conv(enrolled)},
        {"stage": "sent",             "count": sent,             "conversion_rate": conv(sent)},
        {"stage": "opened",           "count": opened,           "conversion_rate": conv(opened)},
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
