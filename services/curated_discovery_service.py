"""
Fast curated discovery — single end-to-end BackgroundTask.

run_fast_discovery(campaign_id, account_id)
  ① Gemini source (up to 3 iterations, hard cost cap)
  ② Haiku batch score companies → drop < 50
  ③ ONE bulk Apify call (companyBatchMode=one_by_one, maxItemsPerCompany=5, Short mode)
  ④ Haiku batch score employees → drop < 60
  ⑤ Unconditional recovery: re-scrape 0-employee companies with broadened IDs
  ⑥ Bulk email finder for all kept prospects
  ⑦ Upsert prospects + pre-enroll
  ⑧ Day-1 message generation (parallel, fail-soft)
  → discovery_status = completed, campaign.status = awaiting_approval
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from pymongo import ReturnDocument, UpdateOne

import database
from config import get_settings
from services.company_sourcing_service import source_companies
from services.employee_scraper_service import (
    bulk_scrape_employees_for_companies,
    transform_employee_to_prospect,
)
from services.campaign_prospect_state_service import (
    DEFAULT_SCORING_VERSION,
    bulk_transition_enrichment,
    bulk_write_state_operations,
    ensure_cohort_membership,
    persist_campaign_scores,
    score_update_operation,
    transition_enrichment_operation,
)

logger = logging.getLogger(__name__)
settings = get_settings()

_MAX_GEMINI_ITERATIONS = 10  # loop until _QUALITY_COMPANY_TARGET or budget exhausted
_QUALITY_COMPANY_TARGET = 120   # minimum kept (score ≥50) companies before stopping
_COMPANY_SCORE_THRESHOLD = 50
# Score floor for the discovery-time employee gate. score_prospect_for_campaign is a
# 0-100 additive scale where company-level signals alone (industry 18 + size 12 +
# linkedin 5 + country 5) reach ~40, so the score is only a SECONDARY ranking signal.
# Person-level fit is now enforced by the deterministic hard gate
# (utils.scoring.person_fit_gate applied in _gate_and_select) — the score threshold
# just removes low-signal rows among gate survivors.
_EMPLOYEE_SCORE_THRESHOLD = 25
# Relax ladder: if hard gates leave fewer than this fraction of prospect_target,
# the function-inference rejection (ONLY) is relaxed and best-scored rejects are
# re-admitted. Title blocklist + icp_exclude_keywords are never relaxed.
_GATE_RELAX_FRACTION = 0.5
_SCORING_DROPOUT_BUFFER = 2.5   # scrape ~2.5x target raw employees to survive score gate + contactability + dedup
_COMPANY_BUFFER = 1.3           # source ~1.3x the companies strictly needed (company-score dropout)
_MIN_PER_COMPANY = 2            # floor so a company is worth a scrape
_MAX_PER_COMPANY_CAP = 10       # ceiling regardless of campaign setting
_SCRAPE_DEPTH = 5               # employees per company to scrape (pick 1 primary + 2 backups)
_PER_COMPANY_ENROLLMENT_CAP = 1 # ONE primary enrolled per company; next-best kept as cascade backups
_PER_COMPANY_BACKUP_COUNT = 2   # backups stored as cascade_waiting (no email spend, no messages)

# ── Company-first sizing (company count is the ONLY user input) ──────────────
# prospect_target is derived: _company_target * _PER_COMPANY_TARGET.
_PER_COMPANY_TARGET = 1          # ONE enrolled prospect per company (rotation covers the rest)
_DEFAULT_COMPANY_TARGET = 100    # default when the campaign didn't set one
# Auto top-up: if a discovery generation yields fewer than this fraction of the
# full prospect target, an append-only follow-up generation is enqueued (up to
# _MAX_TOPUP_GENERATIONS total generations) sourcing NEW companies only.
_TOPUP_THRESHOLD = 0.7
_MAX_TOPUP_GENERATIONS = 3
_MIN_COMPANY_TARGET = 10
_MAX_COMPANY_TARGET = 300
_DEFAULT_SOURCING_CONCURRENCY = 8  # parallel Gemini sourcing calls (rate limiter allows ~150/min)
_DEFAULT_SCRAPE_CONCURRENCY = 5    # parallel Apify employee-scrape runs (chunks of companies)
_SCRAPE_CHUNK_SIZE = 10            # ~companies per parallel Apify run
# Full mode ($8/1k) is the primary mode: Short mode returned urn-style/unreliable
# prospect profile URLs (breaking LinkedIn sends) and frequently omitted
# companyLinkedinUrl (breaking per-company attribution + recovery detection).
# Emails still come from GrowthToolkit's Email Finder, not the actor.
# Override per campaign via `discovery_profile_scraper_mode`.
_PROFILE_SCRAPER_MODE = "Full ($8 per 1k)"


# ──────────────────────────────────────────────────────────────────────────────
# Channel planning helpers (public — called by onboarding wizard)
# ──────────────────────────────────────────────────────────────────────────────

# Human-readable explanation for each channel-planning skip reason, so the UI can
# say WHY a campaign enrolled 0 prospects instead of showing an empty list.
_SKIP_REASON_MESSAGES = {
    "no_sending_account": (
        "Your prospects are ready but there's nowhere to send from — connect an "
        "email or LinkedIn account in Settings and they'll be scheduled "
        "automatically. No need to re-run discovery."
    ),
    "no_contact_info": (
        "No email or LinkedIn contact could be found for the scraped prospects. "
        "Try broadening the ICP (seniority/titles) so contactable people are found."
    ),
    "channel_mismatch": (
        "Scraped prospects had no channel compatible with your connected sending "
        "accounts (e.g. LinkedIn-only prospects but only an email account is connected)."
    ),
    "below_min_score": (
        "All scraped prospects scored below the enrollment threshold for this ICP. "
        "Try broadening the ICP description."
    ),
    "terminal_status": (
        "All candidate prospects were opted-out, bounced, or disqualified."
    ),
}


def _skip_reason_message(skip_reasons: dict[str, int] | None) -> str | None:
    """Pick the dominant skip reason and map it to a user-facing sentence."""
    if not skip_reasons:
        return None
    dominant = max(skip_reasons.items(), key=lambda kv: kv[1])[0]
    return _SKIP_REASON_MESSAGES.get(
        dominant, "No prospects could be enrolled from the scraped candidates."
    )


async def _finalize_sequence_plan(
    campaign_id: str,
    campaign: dict,
    enrollments_for_plan: list,
    prospects_by_id: dict,
) -> dict:
    """Finalize channel planning for a branching sequence campaign.

    Resolves the graph's routes (one per start node — a hybrid campaign has a
    LinkedIn-first, Email-first and InMail route), distributes prospects across
    them to fill each channel's daily cap (20/20/5), and seeds each enrollment's
    ``sequence_state`` at *its route's* start node. Mirrors the day_totals /
    used_by / campaign metadata writes of the classic ``finalize_channel_plan``.
    """
    from pymongo import UpdateOne as _PlanUpdateOne
    from services import sequence_service as seq

    campaign_oid = campaign["_id"]
    graph = campaign["sequence_graph"]
    routes = seq.resolve_routes(graph, campaign)
    if not routes:
        logger.warning(f"[finalize_plan:{campaign_id}] sequence_graph has no start node")
        return {"assigned": 0, "skip_reasons": {"no_start_node": len(enrollments_for_plan)}, "day_totals": {}}

    # Seed the planner with days already filled by earlier top-up generations of
    # this campaign, so this batch continues from the first day that still has
    # room instead of re-piling onto day 1 (which is what pushed 33 connection
    # requests — 3 generations × ~11 — onto a single day past the 20 cap).
    # Scoring/reset enrollments have send_day=None so they're naturally excluded.
    existing_counts: dict[tuple[int, str], int] = {}
    async for _e in database.campaign_enrollments_collection.find(
        {
            "campaign_id": campaign_oid,
            "smart_campaign_send_day": {"$ne": None},
            "smart_campaign_channel": {"$ne": None},
            "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting", "failed", "scoring"]},
        },
        {"smart_campaign_send_day": 1, "smart_campaign_channel": 1},
    ):
        _d = _e.get("smart_campaign_send_day")
        _c = _e.get("smart_campaign_channel")
        if _d is None or _c is None:
            continue
        existing_counts[(int(_d), str(_c))] = existing_counts.get((int(_d), str(_c)), 0) + 1

    assignments, skip_reasons = seq.plan_route_first_touch_days(
        campaign, enrollments_for_plan, prospects_by_id, routes, existing_counts=existing_counts
    )
    logger.info(
        f"[finalize_plan:{campaign_id}] SEQUENCE routes="
        f"{[(r['channel'], r['cap']) for r in routes]} "
        f"existing_day_load={ {f'{d}:{c}': n for (d, c), n in existing_counts.items()} } "
        f"assigned={len(assignments)}, skip={skip_reasons}"
    )

    plan_ops = []
    assigned_ids = set()
    for enr, channel, start_node_id, send_day in assignments:
        assigned_ids.add(enr["_id"])
        plan_ops.append(_PlanUpdateOne(
            {"_id": enr["_id"]},
            {"$set": {
                "smart_campaign_channel": channel,
                "smart_campaign_send_day": send_day,
                "status": "active",
                "next_action_at": None,
                "smart_campaign_scheduled_utc": None,
                "message_gen_status": "pending" if send_day == 1 else "scheduled_later",
                "generated_messages": None,
                "message_gen_error": None,
                "sequence_state": seq.build_initial_sequence_state(graph, start_node_id=start_node_id),
            }},
        ))
    for enr in enrollments_for_plan:
        if enr["_id"] not in assigned_ids:
            plan_ops.append(_PlanUpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "status": "skipped_no_channel",
                    "smart_campaign_channel": None,
                    "smart_campaign_send_day": None,
                    "next_action_at": None,
                    "smart_campaign_scheduled_utc": None,
                    "message_gen_status": "skipped",
                }},
            ))
    if plan_ops:
        await database.campaign_enrollments_collection.bulk_write(plan_ops, ordered=False)

    # Sync used_by → "active" for assigned prospects on prospect_state overlay
    if assignments:
        try:
            from services.prospect_search_service import update_used_by_status as _upd_used_by
            import asyncio as _asyncio
            _sync_ops = [
                _upd_used_by(
                    database.db,
                    account_id=str(enr.get("account_id", "")),
                    prospect_id=str(enr.get("prospect_id", "")),
                    campaign_id=str(enr.get("campaign_id", "")),
                    new_status="active",
                )
                for enr, _ch, _nid, _sd in assignments
            ]
            await _asyncio.gather(*_sync_ops, return_exceptions=True)
        except Exception as _usync_e:
            logger.warning(f"[finalize_plan:{campaign_id}] used_by sync failed: {_usync_e}")

    # day_totals keyed by each route's channel
    day_totals: dict = {}
    for enr, channel, _nid, d in assignments:
        day_totals.setdefault(str(d), {}).setdefault(channel, 0)
        day_totals[str(d)][channel] += 1

    total_assigned = len(assignments)
    _plan_update: dict = {
        "discovery_prospects_eligible": total_assigned,
        "discovery_day_totals": day_totals,
        "discovery_skip_reasons": skip_reasons or {},
    }
    if total_assigned == 0 and enrollments_for_plan:
        _reason = _skip_reason_message(skip_reasons)
        _plan_update["discovery_error"] = _reason
        _plan_update["discovery_failure_reason"] = _reason
        logger.warning(f"[finalize_plan:{campaign_id}] 0 enrolled (sequence) — reason: {_reason}")
    elif total_assigned > 0:
        _plan_update["discovery_error"] = None
        _plan_update["discovery_failure_reason"] = None
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": _plan_update},
    )

    return {"assigned": total_assigned, "skip_reasons": skip_reasons, "day_totals": day_totals}


async def finalize_channel_plan(campaign_id: str, account_id: str) -> dict:
    """
    Auto-pick sender accounts, run plan_channel_assignments, persist channel/day
    to enrollments, and write day_totals to the campaign doc.
    Called both at the end of run_fast_discovery and by replan_and_launch.
    """
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id) if (isinstance(account_id, str) and len(account_id) == 24) else ObjectId(account_id)
    account_id_filter = {"$in": [account_oid, str(account_oid)]}

    campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
    if not campaign:
        return {"assigned": 0, "skip_reasons": {}, "day_totals": {}}

    # Auto-pick sender accounts if not already set
    auto_set: dict = {}
    if not campaign.get("email_account_id"):
        email_acc = await database.email_accounts_collection.find_one(
            {"account_id": account_id_filter, "status": {"$in": ["connected", "active"]}}
        )
        if email_acc:
            auto_set["email_account_id"] = email_acc["_id"]
            campaign["email_account_id"] = email_acc["_id"]
    if not campaign.get("linkedin_account_id"):
        li_acc = await database.linkedin_accounts_collection.find_one(
            {"account_id": account_id_filter, "unipile_status": {"$in": ["OK", "CONNECTING"]}}
        )
        if li_acc:
            auto_set["linkedin_account_id"] = li_acc["_id"]
            campaign["linkedin_account_id"] = li_acc["_id"]
    if auto_set:
        await database.campaigns_collection.update_one({"_id": campaign_oid}, {"$set": auto_set})
        logger.info(f"[finalize_plan:{campaign_id}] auto-picked senders: { {k: str(v) for k, v in auto_set.items()} }")

    # Load enrollments for planning
    enrollments_for_plan = await database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "status": "scoring"}
    ).to_list(length=5000)

    if not enrollments_for_plan:
        return {"assigned": 0, "skip_reasons": {}, "day_totals": {}}

    pids = list({e["prospect_id"] for e in enrollments_for_plan})
    prospects_by_id = {}
    if pids:
        for p in await database.prospects_collection.find({"_id": {"$in": pids}}).to_list(length=len(pids)):
            prospects_by_id[p["_id"]] = p

    # ── Branching sequence campaigns ──────────────────────────────────────────
    # When the campaign carries a sequence_graph, every prospect enters on the
    # sequence's start node. Assign the start node's channel + a send day
    # (honouring the start channel's daily cap, top-score-first) and seed each
    # enrollment's sequence_state. The rest of the smart-campaign machinery
    # (message gen, per-day approval, scheduling) is reused unchanged.
    if campaign.get("sequence_graph"):
        return await _finalize_sequence_plan(
            campaign_id, campaign, enrollments_for_plan, prospects_by_id
        )

    from services.campaign_launch_service import plan_channel_assignments
    # Enrollment score floor: per-campaign override via discovery_min_enroll_score,
    # default 25 (matches _EMPLOYEE_SCORE_THRESHOLD) — previously hardcoded 0,
    # which let every scraped row through regardless of fit.
    try:
        _min_enroll_score = float(campaign.get("discovery_min_enroll_score") or 25)
    except (TypeError, ValueError):
        _min_enroll_score = 25.0
    # Seed with days already filled by earlier top-up generations so this batch
    # continues from the first day with room instead of re-piling onto day 1.
    _existing_counts: dict[tuple[int, str], int] = {}
    async for _e in database.campaign_enrollments_collection.find(
        {
            "campaign_id": campaign_oid,
            "smart_campaign_send_day": {"$ne": None},
            "smart_campaign_channel": {"$ne": None},
            "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting", "failed", "scoring"]},
        },
        {"smart_campaign_send_day": 1, "smart_campaign_channel": 1},
    ):
        _d = _e.get("smart_campaign_send_day")
        _c = _e.get("smart_campaign_channel")
        if _d is None or _c is None:
            continue
        _existing_counts[(int(_d), str(_c))] = _existing_counts.get((int(_d), str(_c)), 0) + 1
    assignments, skip_reasons = plan_channel_assignments(
        campaign, enrollments_for_plan, prospects_by_id,
        min_score=_min_enroll_score, existing_counts=_existing_counts,
    )
    logger.info(
        f"[finalize_plan:{campaign_id}] assigned={len(assignments)}, "
        f"min_score={_min_enroll_score}, skip={skip_reasons}"
    )

    from pymongo import UpdateOne as _PlanUpdateOne
    plan_ops = []
    assigned_ids = set()
    for enr, channel, send_day in assignments:
        assigned_ids.add(enr["_id"])
        plan_ops.append(_PlanUpdateOne(
            {"_id": enr["_id"]},
            {"$set": {
                "smart_campaign_channel": channel,
                "smart_campaign_send_day": send_day,
                "status": "active",
                "next_action_at": None,
                "smart_campaign_scheduled_utc": None,
                "message_gen_status": "pending" if send_day == 1 else "scheduled_later",
                "generated_messages": None,
                "message_gen_error": None,
            }},
        ))
    for enr in enrollments_for_plan:
        if enr["_id"] not in assigned_ids:
            plan_ops.append(_PlanUpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "status": "skipped_no_channel",
                    "smart_campaign_channel": None,
                    "smart_campaign_send_day": None,
                    "next_action_at": None,
                    "smart_campaign_scheduled_utc": None,
                    "message_gen_status": "skipped",
                }},
            ))
    if plan_ops:
        await database.campaign_enrollments_collection.bulk_write(plan_ops, ordered=False)

    # Sync used_by → "active" for assigned prospects on prospect_state overlay
    if assignments:
        try:
            from services.prospect_search_service import update_used_by_status as _upd_used_by
            _sync_ops = []
            for enr, channel, send_day in assignments:
                _sync_ops.append(
                    _upd_used_by(
                        database.db,
                        account_id=str(enr.get("account_id", "")),
                        prospect_id=str(enr.get("prospect_id", "")),
                        campaign_id=str(enr.get("campaign_id", "")),
                        new_status="active",
                    )
                )
            import asyncio as _asyncio
            await _asyncio.gather(*_sync_ops, return_exceptions=True)
        except Exception as _usync_e:
            logger.warning(f"[finalize_plan:{campaign_id}] used_by sync failed: {_usync_e}")

    # Compute day_totals
    day_totals: dict = {}
    for enr, ch, d in assignments:
        day_totals.setdefault(str(d), {}).setdefault(ch, 0)
        day_totals[str(d)][ch] += 1

    total_assigned = len(assignments)
    # NOTE: discovery_status is intentionally NOT set here.
    # The pipeline already wrote "completed" before calling finalize_channel_plan.
    # Overwriting it with "awaiting_approval" would create a terminal-signal race
    # where the frontend sees "awaiting_approval" instead of "completed".
    # campaign.status="awaiting_approval" (set at the same time as "completed") is
    # the authoritative signal that discovery is done and the review gate is open.
    _plan_update: dict = {
        "discovery_prospects_eligible": total_assigned,
        "discovery_day_totals": day_totals,
        "discovery_skip_reasons": skip_reasons or {},
    }
    # Surface WHY 0 prospects were enrolled (else clear any stale reason on a good plan).
    if total_assigned == 0 and enrollments_for_plan:
        _reason = _skip_reason_message(skip_reasons)
        _plan_update["discovery_error"] = _reason
        _plan_update["discovery_failure_reason"] = _reason
        logger.warning(f"[finalize_plan:{campaign_id}] 0 enrolled — reason: {_reason}")
    elif total_assigned > 0:
        _plan_update["discovery_error"] = None
        _plan_update["discovery_failure_reason"] = None
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": _plan_update},
    )

    return {"assigned": total_assigned, "skip_reasons": skip_reasons, "day_totals": day_totals}


async def replan_and_launch(campaign_id: str, account_id: str) -> dict:
    """
    Re-plan channel assignments now that sender accounts have been connected.
    Called at onboarding launch time (discovery fired at stage 3 before accounts existed).
    Re-queries enrollments in {scoring, skipped_no_channel} statuses, resets them
    to 'scoring', then calls finalize_channel_plan with now-connected senders.
    Also triggers Day-1 deep enrichment + message gen.
    """
    campaign_oid = ObjectId(campaign_id)

    # Re-query all enrollments that weren't successfully planned
    enrollments_to_replan = await database.campaign_enrollments_collection.find(
        {
            "campaign_id": campaign_oid,
            "status": {"$in": ["scoring", "skipped_no_channel"]},
        }
    ).to_list(length=5000)

    if not enrollments_to_replan:
        # Try active enrollments with no next_action_at (planned but not yet sent)
        enrollments_to_replan = await database.campaign_enrollments_collection.find(
            {
                "campaign_id": campaign_oid,
                "status": "active",
                "next_action_at": None,
            }
        ).to_list(length=5000)

    if enrollments_to_replan:
        reset_ids = [e["_id"] for e in enrollments_to_replan]
        await database.campaign_enrollments_collection.update_many(
            {"_id": {"$in": reset_ids}},
            {"$set": {
                "status": "scoring",
                "smart_campaign_channel": None,
                "smart_campaign_send_day": None,
                "message_gen_status": "pending",
                "generated_messages": None,
                "next_action_at": None,
            }},
        )
        logger.info(f"[replan:{campaign_id}] reset {len(reset_ids)} enrollments for re-planning")

    # Run finalize_channel_plan with now-connected sender accounts
    result = await finalize_channel_plan(campaign_id, account_id)

    # Queue Day-1 enrichment + message generation durably if prospects were assigned.
    if result.get("assigned", 0) > 0:
        await _enqueue_day1_enrichment_and_messages(campaign_id, account_id)

    logger.info(f"[replan:{campaign_id}] done: assigned={result.get('assigned')}, days={list(result.get('day_totals', {}).keys())}")
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────────

async def _enqueue_day1_enrichment_and_messages(
    campaign_id: str, account_id: str
) -> None:
    """Create leased post-discovery work.

    Splits enrichment from message generation:
      • ENRICHMENT covers ALL enrolled prospects (status scoring/active). Day-1 is
        enriched inside the Day-1 job below; the remaining enrolled cohort is
        enriched by a separate leased enrichment run (skip_outreach → no messages).
        Because this runs on every discovery generation, top-up additions that are
        still un-enriched get picked up here too.
      • MESSAGE GENERATION stays Day-1 only (the Day-1 job).
    """
    from pymongo import ReturnDocument
    from services.enrichment_job_service import (
        enqueue_campaign_day_run,
        enqueue_enrichment_run,
    )

    campaign_oid = ObjectId(campaign_id)
    account_values: list[object] = [str(account_id)]
    if ObjectId.is_valid(str(account_id)):
        account_values.append(ObjectId(str(account_id)))
    campaign = await database.campaigns_collection.find_one_and_update(
        {"_id": campaign_oid, "account_id": {"$in": account_values}},
        {
            "$set": {
                "message_gen_status": "queued",
                "message_gen_started_at": datetime.utcnow(),
                "message_gen_error": None,
            },
            "$inc": {"message_gen_generation": 1},
        },
        return_document=ReturnDocument.AFTER,
    )
    if not campaign:
        raise PermissionError("campaign is not owned by discovery tenant")

    # Day-1: enrich + generate messages (message generation is scoped to Day-1).
    await enqueue_campaign_day_run(
        account_id=str(account_id),
        campaign_id=campaign_id,
        day=1,
        generation=int(campaign.get("message_gen_generation") or 1),
        request={
            "instructions": {},
            "regenerate_channels": {
                "email": True,
                "linkedin_connection": True,
                "linkedin_inmail": True,
                "linkedin_message": True,
            },
            "send_empty_connection_request": bool(
                campaign.get("send_empty_connection_request", False)
            ),
        },
    )

    # Enrich ALL other enrolled prospects (Day 2+) now, with NO message generation.
    # Keyed on a fresh run_id so every generation (incl. top-ups) enqueues cleanly
    # rather than coalescing onto a completed campaign-scoped job.
    try:
        enrolled = await database.campaign_enrollments_collection.find(
            {
                "campaign_id": campaign_oid,
                "status": {"$in": ["scoring", "active"]},
            },
            {"prospect_id": 1, "smart_campaign_send_day": 1},
        ).to_list(length=None)

        remaining_pids = [
            str(e["prospect_id"])
            for e in enrolled
            if e.get("prospect_id") and e.get("smart_campaign_send_day") != 1
        ]
        # Dedupe while preserving order.
        remaining_pids = list(dict.fromkeys(remaining_pids))

        if remaining_pids:
            run_doc = {
                "account_id": str(account_id),
                "campaign_id": campaign_id,
                "status": "queued",
                "trigger": "post_discovery_enrich_all",
                "total_prospects": len(remaining_pids),
                "prospects_processed": 0,
                "prospects_skipped": 0,
                "prospects_failed": 0,
                "profiles_scraped": 0,
                "companies_scraped": 0,
                "companies_deduplicated": 0,
                "ai_assessments_done": 0,
                "outreach_generated": 0,
                "prospect_ids": remaining_pids,
                "started_at": None,
                "created_at": datetime.utcnow(),
                "completed_at": None,
                "current_step": "queued",
                "error": None,
            }
            run_result = await database.enrichment_runs_collection.insert_one(run_doc)
            run_id = str(run_result.inserted_id)
            await enqueue_enrichment_run(
                account_id=str(account_id),
                run_id=run_id,
                prospect_ids=remaining_pids,
                options={
                    "skip_outreach": True,
                    "skip_pre_enrichment_triage": True,
                    "account_id": str(account_id),
                },
                triggered_by="post_discovery_enrich_all",
                campaign_id=campaign_id,
            )
            logger.info(
                f"[fast:{campaign_id}] enqueued enrich-all run {run_id} for "
                f"{len(remaining_pids)} non-Day-1 enrolled prospects"
            )
    except Exception as _ea_e:
        logger.warning(
            f"[fast:{campaign_id}] enrich-all enqueue failed (non-fatal): {_ea_e}"
        )

async def run_fast_discovery(campaign_id: str, account_id: str, generation: int = 1) -> dict:
    """End-to-end curated discovery owned by a leased durable worker.

    ``generation`` > 1 marks an append-only top-up pass: the destructive reset is
    skipped, companies already used by this campaign are excluded so new ones are
    sourced, and the enrolled/planned counters accumulate across generations.
    """
    campaign_oid = ObjectId(campaign_id)
    now = datetime.utcnow()

    account_values: list[object] = [str(account_id)]
    if ObjectId.is_valid(str(account_id)):
        account_values.append(ObjectId(str(account_id)))
    campaign = await database.campaigns_collection.find_one(
        {"_id": campaign_oid, "account_id": {"$in": account_values}}
    )
    if not campaign:
        raise PermissionError("campaign is not owned by discovery tenant")

    if generation == 1:
        # First generation is destructive: clear prior sourced companies + counters.
        await database.sourced_companies_collection.delete_many(
            {"campaign_id": campaign_id, "account_id": {"$in": account_values}}
        )
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": {"$in": account_values}},
            {"$set": {
                "discovery_status": "sourcing_companies",
                "discovery_started_at": now,
                "discovery_error": None,
                "curated_companies_sourced": 0,
                "curated_companies_approved": 0,
                "curated_companies_scraped": 0,
            }},
        )
    else:
        # Top-up pass: append only — do NOT delete sourced companies or reset
        # counters, just flip the status spinner back to "sourcing".
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": {"$in": account_values}},
            {"$set": {
                "discovery_status": "sourcing_companies",
                "discovery_error": None,
            }},
        )

    try:
        icp_prompt = _build_icp_prompt_from_campaign(campaign)
        import math
        # Company-first sizing: the company count is the single sizing input, and the
        # prospect target is derived from it (~3 ideal prospects per company).
        _company_target = int(campaign.get("curated_company_count_target") or _DEFAULT_COMPANY_TARGET)
        _company_target = max(_MIN_COMPANY_TARGET, min(_company_target, _MAX_COMPANY_TARGET))
        per_company = _PER_COMPANY_TARGET
        prospect_target = _company_target * per_company

        # Honor an explicit prospect_count_target (the wizard slider AND the
        # "Scrape more prospects" dialog write it) as the authoritative TOTAL
        # prospect goal. Company-first sizing otherwise reads only
        # curated_company_count_target and silently ignores the slider — e.g. a
        # campaign asking for 150 was sized for 50 (company_target × 1), so
        # "scrape more" found almost nothing once ~50 were enrolled. Derive the
        # company need from the prospect goal so we actually source enough.
        try:
            _explicit_prospect_target = int(campaign.get("prospect_count_target") or 0)
        except (TypeError, ValueError):
            _explicit_prospect_target = 0
        if _explicit_prospect_target > prospect_target:
            prospect_target = _explicit_prospect_target
            _company_target = max(
                _company_target,
                min(_MAX_COMPANY_TARGET, math.ceil(prospect_target / max(1, per_company))),
            )

        # Full intended target is stable across generations; the top-up decision and
        # generation>1 sizing compare enrolled counts against it (the working
        # `prospect_target` gets decremented by pool reuse later on).
        prospect_target_full = prospect_target

        # ── Top-up (generation > 1): size for the shortfall + exclude used companies ──
        # An append pass sources only NEW companies (not already enrolled in this
        # campaign) and only enough to cover the remaining gap to the full target.
        _topup_excluded_co_urls: set[str] = set()
        _topup_excluded_co_ids: set[str] = set()
        if generation > 1:
            _current_enrolled = await database.campaign_enrollments_collection.count_documents({
                "campaign_id": campaign_oid,
                "status": {"$nin": ["skipped_no_channel", "archived", "pending_teammate_review", "cascade_waiting"]},
            })
            _shortfall = max(0, prospect_target_full - _current_enrolled)
            # Source extra companies with buffer headroom to survive score/contact dropout.
            _extra_cos = math.ceil((_shortfall / max(1, per_company)) * _COMPANY_BUFFER)
            _company_target = max(_MIN_COMPANY_TARGET, min(_extra_cos, _MAX_COMPANY_TARGET))
            prospect_target = _shortfall
            # Build the exclusion set from already-enrolled prospects' companies.
            _used_prospect_oids = [
                d["prospect_id"]
                async for d in database.campaign_enrollments_collection.find(
                    {"campaign_id": campaign_oid}, {"prospect_id": 1}
                )
                if d.get("prospect_id")
            ]
            if _used_prospect_oids:
                async for _pdoc in database.prospects_collection.find(
                    {"_id": {"$in": _used_prospect_oids}},
                    {"company_id": 1, "company_linkedin": 1},
                ):
                    _cid = _pdoc.get("company_id")
                    if _cid:
                        _topup_excluded_co_ids.add(str(_cid))
                    _curl = (_pdoc.get("company_linkedin") or "").rstrip("/").lower()
                    if _curl:
                        _topup_excluded_co_urls.add(_curl)
            logger.info(
                f"[fast:{campaign_id}] top-up gen {generation}: enrolled={_current_enrolled}, "
                f"shortfall={_shortfall}, sourcing ~{_company_target} new companies, "
                f"excluding {len(_topup_excluded_co_ids)} company ids / "
                f"{len(_topup_excluded_co_urls)} linkedin urls"
            )

        # Fetch sender context for employee scoring
        company_profile = await database.company_profiles_collection.find_one(
            {"account_id": ObjectId(account_id) if len(account_id) == 24 else account_id}
        )
        # sender_context no longer needed — LLM employee scoring replaced by deterministic scoring
        # sender_context = _build_sender_context(company_profile)

        # ── MOCK MODE: skip all paid calls, inject synthetic prospects ────────────
        # Enabled when campaign.discovery_mock_mode=True AND app_env != production.
        # This lets E2E harnesses exercise the real enroll/plan/message path for free.
        _is_mock = bool(campaign.get("discovery_mock_mode")) and settings.app_env != "production"
        if not _is_mock and settings.discovery_mock_mode and settings.app_env != "production":
            _is_mock = True

        if _is_mock:
            import uuid as _uuid_mod
            logger.info(f"[fast:{campaign_id}] *** MOCK MODE *** — generating synthetic prospects (no Apify/Gemini)")
            _mock_enroll_cap = int(campaign.get("discovery_enrollment_cap") or _PER_COMPANY_ENROLLMENT_CAP)
            _mock_scrape_depth = int(campaign.get("discovery_scrape_depth") or _SCRAPE_DEPTH)
            _mock_per_co = min(_mock_enroll_cap, per_company, max(1, _mock_scrape_depth))
            _mock_n_co = max(3, min(20, math.ceil(prospect_target / per_company)))
            _mock_industries = campaign.get("icp_industries") or ["Retail"]
            _mock_titles = campaign.get("icp_job_titles") or ["Owner"]
            _mock_seniorities = campaign.get("icp_seniority_levels") or ["owner"]
            _mock_countries = campaign.get("icp_countries") or ["United States"]

            new_prospect_oids: list[ObjectId] = []
            for _ci in range(_mock_n_co):
                _co_slug = f"mockco-{_ci}-{_uuid_mod.uuid4().hex[:6]}"
                for _pi in range(_mock_per_co):
                    _uid = _uuid_mod.uuid4().hex[:8]
                    _pt: dict = {
                        "full_name": f"Mock Prospect {_ci}-{_pi}",
                        "first_name": "Mock",
                        "last_name": f"Prospect{_ci}{_pi}",
                        "linkedin": f"https://www.linkedin.com/in/mock-{_uid}",
                        "email": f"mock-{_uid}@{_co_slug}.example.com",
                        "job_title": _mock_titles[_ci % len(_mock_titles)],
                        "headline": f"{_mock_titles[_ci % len(_mock_titles)]} at MockCo-{_ci}",
                        "company_name": f"MockCo-{_ci}",
                        "company_linkedin": f"https://www.linkedin.com/company/{_co_slug}",
                        "company_linkedin_url": f"https://www.linkedin.com/company/{_co_slug}",
                        "company_domain": f"{_co_slug}.example.com",
                        "industry": _mock_industries[_ci % len(_mock_industries)],
                        "company_employee_band": "51-200",
                        "company_size": 100 + _ci * 10,
                        "seniority": _mock_seniorities[_ci % len(_mock_seniorities)],
                        "function": "Operations",
                        "fit_score": 75.0,
                        "location": {
                            "city": "Atlanta", "region": "Georgia",
                            "country": _mock_countries[0], "country_code": "US",
                        },
                        "linkedin_profile_data": {
                            "posts": [{
                                "text": (
                                    f"Growing our {_mock_industries[_ci % len(_mock_industries)]} "
                                    "footprint — local discovery on Google Maps is everything."
                                ),
                                "likes": 12,
                            }]
                        },
                    }
                    _oid = await _upsert_curated_prospect(_pt, campaign_oid, account_id)
                    if _oid:
                        new_prospect_oids.append(_oid)

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_prospects_found": len(new_prospect_oids),
                    "discovery_prospects_from_apify": len(new_prospect_oids),
                    "discovery_prospects_from_db": 0,
                    "discovery_status": "enriching",
                }},
            )

            from services.prospect_enrollment_service import _pre_enroll_prospects
            _mock_prospects_full = await database.prospects_collection.find(
                {"_id": {"$in": new_prospect_oids}}
            ).to_list(length=None)
            if _mock_prospects_full:
                await _pre_enroll_prospects(campaign, _mock_prospects_full)

            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "completed",
                    "discovery_completed_at": datetime.utcnow(),
                    "status": "awaiting_approval",
                    "approved_send_days": [],
                }},
            )
            _mock_plan = await finalize_channel_plan(campaign_id, account_id)
            _mock_assigned = _mock_plan.get("assigned", 0)
            # Recompute total_enrolled from the live enrollments (not = assigned) so
            # it stays correct across top-up generations.
            _mock_total_enrolled = await database.campaign_enrollments_collection.count_documents({
                "campaign_id": campaign_oid,
                "status": {"$nin": ["skipped_no_channel", "archived", "pending_teammate_review", "cascade_waiting"]},
            })
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_prospects_planned": _mock_assigned,
                    "discovery_prospects_enrolled": _mock_assigned,
                    "total_enrolled": _mock_total_enrolled,
                }},
            )
            if _mock_assigned > 0:
                await _enqueue_day1_enrichment_and_messages(
                    campaign_id, str(account_id)
                )
            logger.info(
                f"[fast:{campaign_id}] MOCK done — {len(new_prospect_oids)} synthetic prospects, "
                f"{_mock_assigned} assigned"
            )
            return {"campaign_id": campaign_id, "prospects_created": len(new_prospect_oids), "mock": True}
        # ── END MOCK MODE ─────────────────────────────────────────────────────────

        # ── Lazy ICP canonicalization ─────────────────────────────────────────────
        from services.prospect_search_service import (
            search_companies_structured as _search_cos,
            search_companies_vector as _search_cos_vector,
            search_prospects_structured as _search_pool_structured,
            build_exclusion_set as _build_exclusion_set,
        )
        from services.icp_canonicalizer import canonicalize_icp as _canonicalize_icp

        # Re-canonicalize whenever EITHER field Stage A (DB company match) needs is
        # missing — previously this only fired when BOTH were missing, so a campaign
        # with country_codes set but industry_ids empty never retried and ran Stage A
        # with no industry filter (over-broad) or skipped it entirely.
        if not campaign.get("industry_ids") or not campaign.get("country_codes"):
            try:
                _canonical = await _canonicalize_icp(campaign)
                if any(_canonical.values()):
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid}, {"$set": _canonical}
                    )
                    campaign.update(_canonical)
                    logger.info(f"[fast:{campaign_id}] lazy-canonicalized ICP: {_canonical.get('industry_ids')}")
            except Exception as _ce:
                logger.warning(f"[fast:{campaign_id}] ICP canonicalization failed: {_ce}")

        # _company_target already computed above (company-first sizing).

        # Title-gate fallback: if the prefill left icp_job_titles empty, synthesize
        # target titles from function × seniority so the person-fit title gate and
        # the AI title judge never run blind — an empty list disabled the title
        # gate entirely and let adjacent "manager" roles through.
        if not campaign.get("icp_job_titles"):
            _synth_titles = _synthesize_icp_titles(
                campaign.get("icp_functional_departments") or [],
                campaign.get("icp_seniority_levels") or [],
            )
            if _synth_titles:
                campaign["icp_job_titles"] = _synth_titles
                try:
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid}, {"$set": {"icp_job_titles": _synth_titles}}
                    )
                except Exception:
                    pass
                logger.info(f"[fast:{campaign_id}] synthesized icp_job_titles: {_synth_titles}")

        # Build exclusion set (90-day cooldown from completion, per-user)
        _user_id = str(campaign.get("created_by") or account_id)
        _excluded = await _build_exclusion_set(
            database.db,
            account_id=str(account_id),
            user_id=_user_id,
            cooldown_days=90,
            campaign_id=campaign_id,
        )

        _icp_industry_ids = campaign.get("industry_ids") or []
        _icp_country_codes = campaign.get("country_codes") or []
        _icp_employee_bands = campaign.get("employee_bands") or []

        # ── STAGE A: DB company match ──────────────────────────────────────────────
        # Query the shared companies_collection by canonical ICP fields.
        # Returns up to _company_target companies sorted by prospect_count desc.
        _matched_cos: list[dict] = []
        if _icp_industry_ids or _icp_country_codes:
            try:
                _matched_cos = await _search_cos(
                    database.db,
                    industry_ids=_icp_industry_ids or None,
                    country_codes=_icp_country_codes or None,
                    employee_bands=_icp_employee_bands or None,
                    limit=_company_target,
                )
                logger.info(
                    f"[fast:{campaign_id}] Stage A: {len(_matched_cos)} companies matched in DB "
                    f"(target={_company_target})"
                )
            except Exception as _ca_e:
                logger.warning(f"[fast:{campaign_id}] Stage A company search failed: {_ca_e}")
                _matched_cos = []

        # Vector fallback: when canonicalization produced nothing usable (or the
        # structured filter matched nothing), semantically match the free-text ICP
        # against the shared company pool via the companies_vec index instead of
        # silently falling through to the all-Gemini path.
        if not _matched_cos:
            try:
                from services.embedding_service import embed_one as _embed_one
                _icp_vec = await _embed_one(icp_prompt, task_type="RETRIEVAL_QUERY")
                if _icp_vec is not None:
                    _matched_cos = await _search_cos_vector(
                        database.db,
                        profile_query_vec=_icp_vec,
                        # Keep whatever canonical filters DO exist as pre-filters.
                        industry_ids=_icp_industry_ids or None,
                        country_codes=_icp_country_codes or None,
                        employee_bands=_icp_employee_bands or None,
                        limit=_company_target,
                    )
                    logger.info(
                        f"[fast:{campaign_id}] Stage A vector fallback: "
                        f"{len(_matched_cos)} companies matched semantically"
                    )
            except Exception as _cv_e:
                logger.warning(f"[fast:{campaign_id}] Stage A vector fallback failed: {_cv_e}")

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_companies_matched": len(_matched_cos)}},
        )

        def _db_company_to_sc(co_doc: dict) -> dict:
            """Normalize a DB company doc to the sourced-company dict format used downstream."""
            _loc = co_doc.get("location") or {}
            return {
                # Canonical full URL (https://www.linkedin.com/company/<slug>) — the DB
                # stores bare 'linkedin.com/company/<slug>', which neither the Apify
                # employee actor nor the pool-reuse matcher (exact-match on the pool's
                # full form) can resolve. Returns None for non-/company/ junk URLs.
                "company_linkedin_url": _canonical_company_li_url(co_doc.get("linkedin_url")),
                "company_name": co_doc.get("name"),
                "company_domain": co_doc.get("domain") or co_doc.get("website"),
                "company_website": co_doc.get("website"),
                # Keep as dict so transform_employee_to_prospect extracts canonical fields
                "industry": co_doc.get("industry"),
                "location": co_doc.get("location"),
                "employee_band": co_doc.get("employee_band"),
                "employee_size_estimate": (
                    str(co_doc["employee_count"]) if co_doc.get("employee_count") else None
                ),
                "description": co_doc.get("description"),
                "country": (_loc.get("country") if isinstance(_loc, dict) else None) or "",
                "_db_company_id": str(co_doc["_id"]),
                "_source": "db",
            }

        # Drop companies whose linkedin_url canonicalizes to None (missing/junk, e.g.
        # a /search/ URL) — they can't be scraped and would poison attribution.
        _matched_sc_list: list[dict] = [
            _sc for co in _matched_cos
            if (_sc := _db_company_to_sc(co)).get("company_linkedin_url")
        ]
        _dropped_bad_url = len(_matched_cos) - len(_matched_sc_list)
        if _dropped_bad_url:
            logger.info(f"[fast:{campaign_id}] Stage A: dropped {_dropped_bad_url} companies with missing/invalid linkedin_url")

        # Top-up: drop companies already used by this campaign so the pass finds NEW ones.
        if generation > 1 and (_topup_excluded_co_urls or _topup_excluded_co_ids):
            _before_excl = len(_matched_sc_list)
            _matched_sc_list = [
                _sc for _sc in _matched_sc_list
                if (_sc.get("company_linkedin_url") or "").rstrip("/").lower()
                not in _topup_excluded_co_urls
                and str(_sc.get("_db_company_id") or "") not in _topup_excluded_co_ids
            ]
            if _before_excl - len(_matched_sc_list):
                logger.info(
                    f"[fast:{campaign_id}] Stage A: top-up excluded "
                    f"{_before_excl - len(_matched_sc_list)} already-used companies"
                )

        # Score DB companies with the same deterministic scorer + threshold gate used
        # for Stage B sourced companies below, instead of trusting a flat pre-qualified
        # score. A DB match on industry/country alone doesn't guarantee ICP fit (e.g. a
        # giant off-ICP company can still satisfy the industry/country filter), and the
        # un-gated hard-coded 80.0 let those surface ahead of genuinely good matches.
        for _sc in _matched_sc_list:
            _sc["_icp_score"] = _score_company_deterministic(_sc, icp_prompt)
        _kept_matched_sc_list = [
            _sc for _sc in _matched_sc_list if _sc.get("_icp_score", 0) >= _COMPANY_SCORE_THRESHOLD
        ]
        _dropped_low_score = len(_matched_sc_list) - len(_kept_matched_sc_list)
        if _dropped_low_score:
            logger.info(
                f"[fast:{campaign_id}] Stage A: dropped {_dropped_low_score} DB companies "
                f"below score threshold {_COMPANY_SCORE_THRESHOLD}"
            )
        _matched_sc_list = _kept_matched_sc_list

        # Name-normalized dedup ("Barnes & Noble" vs "Barnes andNoble", double
        # Arbonne) — shared set also consumed by the Stage B callback below.
        from services.company_gate_service import ai_company_gate, normalize_company_name
        _seen_co_names: set[str] = set()
        _name_deduped: list[dict] = []
        for _sc in _matched_sc_list:
            _nk = normalize_company_name(_sc.get("company_name"))
            if _nk and _nk in _seen_co_names:
                continue
            if _nk:
                _seen_co_names.add(_nk)
            _name_deduped.append(_sc)
        if len(_name_deduped) != len(_matched_sc_list):
            logger.info(
                f"[fast:{campaign_id}] Stage A: name-dedup dropped "
                f"{len(_matched_sc_list) - len(_name_deduped)} duplicate companies"
            )
        _matched_sc_list = _name_deduped

        # AI company-fit judge: the DB industry taxonomy can't tell an oil major
        # with gas-station "retail" tags from a D2C brand — Gemini judges actual
        # business-model fit against the ICP. Fail-open on infra errors.
        if _matched_sc_list:
            try:
                _cg_verdicts = await ai_company_gate(
                    [
                        {
                            "index": i,
                            "name": c.get("company_name"),
                            "description": c.get("description"),
                            "industry": (
                                (c.get("industry") or {}).get("label")
                                if isinstance(c.get("industry"), dict) else c.get("industry")
                            ),
                            "employee_size": c.get("employee_size_estimate") or c.get("employee_band"),
                        }
                        for i, c in enumerate(_matched_sc_list)
                    ],
                    icp_prompt,
                    account_id=str(account_id),
                    campaign_id=str(campaign_id),
                )
                _cg_kept = [
                    c for i, c in enumerate(_matched_sc_list)
                    if (_cg_verdicts.get(i) or {}).get("match", True)
                ]
                if len(_cg_kept) != len(_matched_sc_list):
                    _cg_rejected = [
                        c.get("company_name") for i, c in enumerate(_matched_sc_list)
                        if not (_cg_verdicts.get(i) or {}).get("match", True)
                    ]
                    logger.info(
                        f"[fast:{campaign_id}] Stage A company gate rejected "
                        f"{len(_matched_sc_list) - len(_cg_kept)}: {_cg_rejected[:10]}"
                    )
                _matched_sc_list = _cg_kept
            except Exception as _cg_e:
                logger.warning(f"[fast:{campaign_id}] Stage A company gate failed (fail-open): {_cg_e}")

        _matched_urls: set[str] = {
            (sc.get("company_linkedin_url") or "").rstrip("/").lower()
            for sc in _matched_sc_list
            if sc.get("company_linkedin_url")
        }

        # Persist Stage A DB-matched companies to sourced_companies IMMEDIATELY so the
        # UI shows them within seconds of discovery starting (previously only Gemini
        # companies were persisted, making DB-first work invisible). Upsert keyed by
        # campaign+URL so top-up generations don't duplicate rows.
        if _matched_sc_list:
            try:
                _db_sc_ops = [
                    UpdateOne(
                        {"campaign_id": campaign_id, "company_linkedin_url": c.get("company_linkedin_url")},
                        {"$setOnInsert": {
                            **{k: v for k, v in c.items() if not k.startswith("_")},
                            "_icp_score": c.get("_icp_score"),
                            "campaign_id": campaign_id,
                            "account_id": account_id,
                            "source": "db_match",
                            "user_excluded": False,
                            "employee_scrape_status": "pending",
                            "employees_scraped_count": 0,
                            "prospects_created_count": 0,
                            "created_at": now,
                            "updated_at": now,
                        }},
                        upsert=True,
                    )
                    for c in _matched_sc_list
                ]
                await database.sourced_companies_collection.bulk_write(_db_sc_ops, ordered=False)
            except Exception as _ap_e:
                logger.warning(f"[fast:{campaign_id}] Stage A sourced_companies persist failed: {_ap_e}")

        # ── STAGE B: Gap-fill sourcing (only if matched < target) ─────────────────
        # gap == 0 → skip Gemini entirely (the "≥100 companies → don't source" rule).
        _gap = max(0, _company_target - len(_matched_sc_list))
        logger.info(
            f"[fast:{campaign_id}] Stage B: gap={_gap} "
            f"(target={_company_target}, matched={len(_matched_sc_list)})"
        )

        _sourced_sc_list: list[dict] = []
        if _gap > 0:
            _sourcing_concurrency = int(campaign.get("discovery_sourcing_concurrency") or _DEFAULT_SOURCING_CONCURRENCY)
            _want = math.ceil(_gap * _COMPANY_BUFFER)
            logger.info(f"[fast:{campaign_id}] Stage B: sourcing ~{_want} companies (gap={_gap})")

            # Structured hard-requirement hints from canonical ICP → tighter Gemini results.
            # ALWAYS derive these when possible — previously this block only fired off
            # the campaign's canonical icp_industries/icp_country_codes/size fields, so
            # a campaign with only a free-text icp_prompt (no canonical fields) sourced
            # with zero hard industry/size/geo filters and let wrong-fit companies in.
            _hint_lines: list[str] = []

            _industry_labels = list(campaign.get("icp_industries") or [])
            if not _industry_labels and campaign.get("industry_ids"):
                # Fall back to the canonical industry_ids resolved by _canonicalize_icp
                # above (from icp_industry / other campaign fields) and translate them
                # back to human-readable labels for the sourcing prompt.
                try:
                    from services.industry_canonicalizer import get_taxonomy_entry
                    for _iid in campaign["industry_ids"][:8]:
                        _entry = get_taxonomy_entry(_iid)
                        _label = (_entry or {}).get("label") if _entry else None
                        if _label:
                            _industry_labels.append(_label)
                except Exception as _il_e:
                    logger.warning(f"[fast:{campaign_id}] industry label lookup for hints failed: {_il_e}")
            if _industry_labels:
                _hint_lines.append(f"- Industry must be one of: {', '.join(_industry_labels[:8])}")

            # _icp_country_codes already reflects the lazily-canonicalized
            # campaign["country_codes"] (derived above from icp_countries /
            # icp_locations / free text when canonical fields were missing).
            if _icp_country_codes:
                _hint_lines.append(f"- Headquarters country code must be one of: {', '.join(_icp_country_codes[:8])}")

            _size_min = campaign.get("icp_company_size_min")
            _size_max = campaign.get("icp_company_size_max")
            if not _size_min and not _size_max and campaign.get("employee_bands"):
                # Fall back to the canonical employee_bands derived above (e.g. from
                # free-text company-size phrases) and translate the widest span into
                # an approximate min/max headcount for the prompt.
                _BAND_RANGES = {
                    "1-10": (1, 10), "11-50": (11, 50), "51-200": (51, 200),
                    "201-1000": (201, 1000), "1000+": (1000, None),
                }
                _bands = [_BAND_RANGES[b] for b in campaign["employee_bands"] if b in _BAND_RANGES]
                if _bands:
                    _size_min = min(b[0] for b in _bands)
                    _maxes = [b[1] for b in _bands]
                    _size_max = None if any(m is None for m in _maxes) else max(_maxes)
            if _size_min or _size_max:
                _hint_lines.append(
                    f"- Employee count between {_size_min or 1} and {_size_max or '10000+'}"
                )

            if not _hint_lines:
                logger.warning(
                    f"[fast:{campaign_id}] Stage B sourcing: no structured industry/geo/size "
                    f"hints could be derived (canonical + free-text fields both empty) — "
                    f"running with free-text icp_prompt only, no hard filters"
                )
            _structured_hints = "\n".join(_hint_lines) or None

            # Negative feedback: names rejected by the deterministic company score are fed
            # back into subsequent batch prompts (the list is mutated by the callback below,
            # and _build_prompt reads it at batch-execution time).
            _negative_hints: list[str] = []

            _seen_in_gap: set[str] = set()
            _incremental_kept: list[dict] = []

            async def _on_sourcing_batch(new_items: list[dict]) -> None:
                """Process each Gemini batch as it lands: dedup, DB-hit rescue, score,
                threshold-gate, and persist accepted companies to sourced_companies so
                the UI streams rows in while other batches are still running."""
                _batch_urls = [
                    (c.get("company_linkedin_url") or "").rstrip("/").lower()
                    for c in new_items if c.get("company_linkedin_url")
                ]
                # Companies Gemini found that ALREADY exist in the shared pool: use the
                # richer DB doc (canonical industry/location/prospect_count) instead of
                # dropping them like the old dedup did — they're DB-first wins.
                _db_docs_by_url: dict[str, dict] = {}
                if _batch_urls:
                    async for _ex in database.companies_collection.find(
                        {"linkedin_url": {"$in": _batch_urls}}
                    ):
                        _db_docs_by_url[(_ex.get("linkedin_url") or "").rstrip("/").lower()] = _ex

                _accepted_docs: list[dict] = []
                _raw_count = len(new_items)
                _batch_pass: list[dict] = []
                for _co in new_items:
                    _co_url = (_co.get("company_linkedin_url") or "").rstrip("/").lower()
                    if not _co_url or _co_url in _matched_urls or _co_url in _seen_in_gap:
                        continue
                    if generation > 1 and _co_url in _topup_excluded_co_urls:
                        continue
                    _seen_in_gap.add(_co_url)
                    # Name-normalized dedup vs Stage A + earlier batches.
                    _nk = normalize_company_name(_co.get("company_name"))
                    if _nk and _nk in _seen_co_names:
                        continue
                    if _nk:
                        _seen_co_names.add(_nk)
                    _db_doc = _db_docs_by_url.get(_co_url)
                    if _db_doc is not None:
                        _co = _db_company_to_sc(_db_doc)
                        if not _co.get("company_linkedin_url"):
                            continue
                        _co["_source"] = "db"
                    else:
                        _co["_source"] = "gemini"
                    _co["_icp_score"] = _score_company_deterministic(_co, icp_prompt)
                    if _co.get("_icp_score", 0) < _COMPANY_SCORE_THRESHOLD:
                        if len(_negative_hints) < 20 and _co.get("company_name"):
                            _negative_hints.append(f"{_co['company_name']} (weak ICP fit)")
                        continue
                    _batch_pass.append(_co)

                # AI company-fit judge on this batch's score survivors (fail-open).
                if _batch_pass:
                    try:
                        _bg_verdicts = await ai_company_gate(
                            [
                                {
                                    "index": i,
                                    "name": c.get("company_name"),
                                    "description": c.get("description"),
                                    "industry": (
                                        (c.get("industry") or {}).get("label")
                                        if isinstance(c.get("industry"), dict) else c.get("industry")
                                    ),
                                    "employee_size": c.get("employee_size_estimate") or c.get("employee_band"),
                                }
                                for i, c in enumerate(_batch_pass)
                            ],
                            icp_prompt,
                            account_id=str(account_id),
                            campaign_id=str(campaign_id),
                        )
                        _bg_kept: list[dict] = []
                        for i, c in enumerate(_batch_pass):
                            _v = _bg_verdicts.get(i) or {}
                            if _v.get("match", True):
                                _bg_kept.append(c)
                            elif len(_negative_hints) < 20 and c.get("company_name"):
                                _negative_hints.append(
                                    f"{c['company_name']} ({_v.get('reason') or 'poor business-model fit'})"
                                )
                        _batch_pass = _bg_kept
                    except Exception as _bg_e:
                        logger.warning(f"[fast:{campaign_id}] Stage B company gate failed (fail-open): {_bg_e}")

                for _co in _batch_pass:
                    if len(_incremental_kept) >= _gap:
                        break  # target reached — count raw but stop accepting
                    _incremental_kept.append(_co)
                    _accepted_docs.append({
                        **{k: v for k, v in _co.items() if not k.startswith("_")},
                        "_icp_score": _co.get("_icp_score"),
                        "campaign_id": campaign_id,
                        "account_id": account_id,
                        "source": "db_match" if _co.get("_source") == "db" else "gemini_grounded",
                        "user_excluded": False,
                        "employee_scrape_status": "pending",
                        "employees_scraped_count": 0,
                        "prospects_created_count": 0,
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                    })

                # Count ACCEPTED companies (not raw) so matched + sourced == rows the
                # UI actually shows in the companies list.
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {"curated_companies_sourced": len(_accepted_docs)}},
                )
                if _accepted_docs:
                    try:
                        await database.sourced_companies_collection.insert_many(_accepted_docs, ordered=False)
                    except Exception as _ib_e:
                        logger.warning(f"[fast:{campaign_id}] incremental sourced_companies persist failed: {_ib_e}")

            try:
                # Single retry layer: source_companies retries each Gemini batch
                # internally (3x with backoff); the old outer _with_retries wrap made
                # worst-case 9 attempts per batch and tripled tail latency.
                _gemini_raw, _gemini_meta = await source_companies(
                    icp_prompt=icp_prompt,
                    target_count=_want,
                    exclude_names=[sc.get("company_name") or "" for sc in _matched_sc_list],
                    account_id=account_id,
                    campaign_id=campaign_id,
                    validate_urls=False,
                    max_concurrency=_sourcing_concurrency,
                    structured_hints=_structured_hints,
                    negative_hints=_negative_hints,
                    on_batch=_on_sourcing_batch,
                )
            except Exception as _sb_e:
                logger.warning(f"[fast:{campaign_id}] Stage B Gemini sourcing failed: {_sb_e}")
                _gemini_raw, _gemini_meta = [], {}

            _sourced_sc_list = _incremental_kept
            logger.info(
                f"[fast:{campaign_id}] Stage B: raw={len(_gemini_raw)}, "
                f"kept={len(_sourced_sc_list)} (streamed incrementally, "
                f"{len(_negative_hints)} negative-feedback hints accumulated)"
            )
        else:
            logger.info(f"[fast:{campaign_id}] Stage B: skipped (DB already has enough companies)")

        # Merge DB-matched + Gemini-sourced into the working company set
        # (both cohorts already persisted to sourced_companies incrementally above)
        kept_companies: list[dict] = _matched_sc_list + _sourced_sc_list

        if not kept_companies:
            if generation > 1:
                # A top-up pass that exhausted new companies is a normal terminal
                # state, NOT a failure — the earlier generation(s) already enrolled
                # prospects. Settle the campaign as completed and stop topping up.
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {
                        "discovery_status": "completed",
                        "discovery_completed_at": datetime.utcnow(),
                        "discovery_topup_active": False,
                        "discovery_topup_message": None,
                    }},
                )
                logger.info(
                    f"[fast:{campaign_id}] top-up gen {generation}: no new companies "
                    "left to source — settling as completed"
                )
                return {"campaign_id": campaign_id, "sourced": 0, "generation": generation}
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "discovery_status": "failed",
                    "discovery_error": (
                        "No companies found matching your criteria. "
                        "Try broadening your ICP description or removing location/industry filters."
                    ),
                }},
            )
            return {"campaign_id": campaign_id, "sourced": 0}

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "curated_companies_approved": len(kept_companies),
                "discovery_companies_found": len(kept_companies),
            }},
        )
        logger.info(
            f"[fast:{campaign_id}] company set: {len(kept_companies)} "
            f"(db={len(_matched_sc_list)}, gemini={len(_sourced_sc_list)}), "
            f"icp={icp_prompt[:80]!r}"
        )

        # ── STAGE C: Per-company prospect reuse sub-step ───────────────────────────
        # For each company in the set, check if the shared prospect pool already has
        # contactable prospects there that this account hasn't used. Reuse them, skip
        # scraping that company. Companies without enough reusable prospects go to Apify.
        _enroll_cap = int(campaign.get("discovery_enrollment_cap") or _PER_COMPANY_ENROLLMENT_CAP)
        _co_urls_list = [
            (c.get("company_linkedin_url") or "").rstrip("/").lower()
            for c in kept_companies
            if c.get("company_linkedin_url")
        ]

        _reuse_by_co: dict[str, list[dict]] = {}
        if _co_urls_list:
            try:
                _reuse_candidates = await _search_pool_structured(
                    database.db,
                    account_id=str(account_id),
                    industry_ids=_icp_industry_ids or None,
                    country_codes=_icp_country_codes or None,
                    employee_bands=_icp_employee_bands or None,
                    company_linkedin_urls=_co_urls_list,
                    exclude_ids=_excluded,
                    limit=len(_co_urls_list) * _enroll_cap * 3,
                )
                for _rp in _reuse_candidates:
                    _rp_co_url = (_rp.get("company_linkedin") or "").rstrip("/").lower()
                    if _rp_co_url:
                        _reuse_by_co.setdefault(_rp_co_url, []).append(_rp)
                logger.info(
                    f"[fast:{campaign_id}] Stage C reuse: "
                    f"{sum(len(v) for v in _reuse_by_co.values())} prospects "
                    f"at {len(_reuse_by_co)}/{len(_co_urls_list)} companies"
                )
            except Exception as _rc_e:
                logger.warning(f"[fast:{campaign_id}] Stage C reuse query failed: {_rc_e}")

        # Partition companies into: fully/partially covered by pool vs. must scrape
        companies_to_scrape: list[dict] = []
        _reused_pairs: list[tuple[dict, dict]] = []  # (prospect_doc, sourced_company)

        for _co in kept_companies:
            _co_url = (_co.get("company_linkedin_url") or "").rstrip("/").lower()
            if not _co_url:
                companies_to_scrape.append(_co)
                continue
            _existing = _reuse_by_co.get(_co_url, [])[:_enroll_cap]
            if len(_existing) >= _enroll_cap:
                # Fully covered — skip scraping this company
                _reused_pairs.extend((_rp, _co) for _rp in _existing)
            elif _existing:
                # Partial — reuse what exists, still scrape for more
                _reused_pairs.extend((_rp, _co) for _rp in _existing)
                companies_to_scrape.append(_co)
            else:
                companies_to_scrape.append(_co)

        _db_enrolled_count = len(_reused_pairs)
        logger.info(
            f"[fast:{campaign_id}] Stage C: "
            f"reused={_db_enrolled_count}, scraping={len(companies_to_scrape)} companies"
        )

        # Reduce prospect_target by how many we already have from the reuse pool
        prospect_target = max(0, prospect_target - _db_enrolled_count)

        # ── ③ Streaming Apify employee scrape + per-chunk gate/enroll ────────────
        # Companies are scraped in chunks of ~_SCRAPE_CHUNK_SIZE run concurrently; each
        # chunk's employees are gated, emailed, upserted, and PRE-ENROLLED as soon as
        # that chunk's Apify run returns — so prospects stream into the UI instead of
        # appearing all at once after the slowest scrape finishes.
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "scraping_employees"}},
        )

        # ── Per-campaign tuning overrides (campaign doc keys; fall back to module constants) ──
        _scrape_depth = int(campaign.get("discovery_scrape_depth") or _SCRAPE_DEPTH)
        _dropout_buffer = float(campaign.get("discovery_dropout_buffer") or _SCORING_DROPOUT_BUFFER)
        _profile_mode = campaign.get("discovery_profile_scraper_mode") or _PROFILE_SCRAPER_MODE
        # _enroll_cap already set in Stage C above

        seniority_ids = _icp_seniority_to_actor_ids(campaign.get("icp_seniority_levels") or [])
        functional_ids = _icp_function_to_actor_ids(campaign.get("icp_functional_departments") or [])
        headcount_bands = _icp_size_to_headcount_bands(
            campaign.get("icp_company_size_min"), campaign.get("icp_company_size_max")
        )
        # Only scrape companies not covered by Stage C reuse pool
        company_urls = [c["company_linkedin_url"] for c in companies_to_scrape]

        _scrape_concurrency = int(
            campaign.get("discovery_scrape_concurrency")
            or min(_DEFAULT_SCRAPE_CONCURRENCY, max(1, math.ceil(len(company_urls) / _SCRAPE_CHUNK_SIZE)))
        )

        logger.info(
            f"[fast:{campaign_id}] streaming Apify — {len(company_urls)} cos, "
            f"scrape_depth={_scrape_depth}, dropout_buffer={_dropout_buffer}, enroll_cap={_enroll_cap}, "
            f"concurrency={_scrape_concurrency}, seniority={seniority_ids}, function={functional_ids}, "
            f"headcount={headcount_bands}"
        )

        from services.email_finder_service import find_emails, EmailLookupEntry
        from utils.scoring import score_prospect_for_campaign as _score_for_campaign
        from services.campaign_scoring_service import compute_campaign_score
        from services.prospect_enrollment_service import _pre_enroll_prospects

        url_to_sc = {
            (_normalize_li_url(c.get("company_linkedin_url")) or ""): c
            for c in kept_companies
            if c.get("company_linkedin_url")
        }
        _url_to_sc_norm = {
            (c.get("company_linkedin_url") or "").rstrip("/").lower(): c
            for c in kept_companies
            if c.get("company_linkedin_url")
        }

        # ── Shared accumulators across chunks ──
        raw_employees: list[dict] = []               # all raw employees (recovery yield calc)
        returned_company_urls: set[str] = set()
        employee_pairs: list[tuple[dict, dict]] = [] # all attributed (raw_employee, sc) pairs
        _relax_pool: list[tuple[dict, dict]] = []    # relaxable gate rejects (global relax ladder)
        _gate_stats_total: dict[str, int] = {}
        new_prospect_oids: list[ObjectId] = []
        _seen_prospect_oids: set = set()
        per_company_counts: dict = {}
        _campaign_source_by_oid: dict[ObjectId, dict] = {}
        _enrolled_running = 0                        # scraped prospects enrolled so far
        _dropped_off_icp = 0
        _dropped_no_url = 0
        _ai_gate_rejected = 0
        _email_credit_warnings: list[str] = []
        email_by_url: dict[str, str | None] = {}
        emails_applied = 0

        _scoring_version = campaign.get("scoring_version") or DEFAULT_SCORING_VERSION
        _cohort_id = f"campaign:{campaign_id}:selected"

        _ai_gate_icp = {
            "job_titles": campaign.get("icp_job_titles") or [],
            "seniorities": campaign.get("icp_seniority_levels") or [],
            "functions": campaign.get("icp_functional_departments") or [],
            "notes": (icp_prompt or "")[:500],
        }

        async def _apply_ai_title_gate(pairs: list[tuple[dict, dict]]) -> list[tuple[dict, dict]]:
            """Cheap-LLM judge on titles that survived the deterministic gates. Catches
            qualifier-mismatch roles token matching can't ("merchandise manager" for a
            "marketing manager" ICP). Fail-open: gate infra errors keep everyone."""
            nonlocal _ai_gate_rejected
            if not pairs:
                return pairs
            try:
                from services.title_gate_service import ai_title_gate
            except Exception:
                return pairs
            try:
                _cands = [
                    {"index": i, "title": t.get("job_title") or "", "headline": t.get("headline") or ""}
                    for i, (t, _) in enumerate(pairs)
                ]
                _verdicts = await ai_title_gate(
                    _cands, _ai_gate_icp,
                    account_id=str(account_id), campaign_id=str(campaign_id),
                )
                _kept = [
                    pair for i, pair in enumerate(pairs)
                    if (_verdicts.get(i) or {}).get("match", True)
                ]
                _ai_gate_rejected += len(pairs) - len(_kept)
                return _kept
            except Exception as _ag_e:
                logger.warning(f"[fast:{campaign_id}] AI title gate failed (fail-open): {_ag_e}")
                return pairs

        async def _find_emails_for(pairs: list[tuple[dict, dict]]) -> None:
            """Bulk GrowthToolkit email lookup for one chunk's kept prospects."""
            nonlocal emails_applied
            _entries = [
                EmailLookupEntry(
                    first_name=t["first_name"],
                    last_name=t["last_name"],
                    domain=t["company_domain"],
                    key=t["linkedin"],
                )
                for t, _ in pairs
                if not t.get("email") and t.get("linkedin") and t["linkedin"] not in email_by_url
                and t.get("first_name") and t.get("last_name") and t.get("company_domain")
            ]
            if not _entries:
                return
            try:
                _found = await find_emails(
                    _entries,
                    account_id=str(account_id) if account_id else None,
                    credit_warnings=_email_credit_warnings,
                )
                email_by_url.update(_found)
            except Exception as _ef_e:
                logger.warning(f"[fast:{campaign_id}] email finder failed (continuing): {_ef_e}")
            for t, _ in pairs:
                if not t.get("email") and t.get("linkedin"):
                    _f = email_by_url.get(t["linkedin"])
                    if _f:
                        t["email"] = _f
                        emails_applied += 1

        async def _persist_and_enroll(pairs: list[tuple[dict, dict]], *, label: str) -> int:
            """Upsert + cohort-score + pre-enroll a list of (prospect_dict, sc) pairs.
            Idempotent per prospect (dedup via _seen_prospect_oids; _pre_enroll_prospects
            skips already-enrolled). Returns number of newly persisted prospects."""
            nonlocal new_prospect_oids
            if not pairs:
                return 0
            _chunk_oids: list[ObjectId] = []
            _cascade_groups: list[tuple[ObjectId, str]] = []  # (prospect_oid, company_url)
            # Each upsert is keyed by its own linkedin/email and independent of
            # the others — run them concurrently instead of one round trip at a
            # time; bookkeeping below still runs in the original pair order.
            _upsert_oids = await asyncio.gather(
                *[_upsert_curated_prospect(t, campaign_oid, account_id) for t, sc in pairs]
            )
            for (t, sc), oid in zip(pairs, _upsert_oids):
                if not oid or oid in _seen_prospect_oids:
                    continue
                _seen_prospect_oids.add(oid)
                new_prospect_oids.append(oid)
                _chunk_oids.append(oid)
                _campaign_source_by_oid[oid] = t
                sc_id = sc.get("_id") or sc.get("company_linkedin_url", "")
                per_company_counts[sc_id] = per_company_counts.get(sc_id, 0) + 1
                _cg_url = (t.get("company_linkedin") or sc.get("company_linkedin_url") or "").rstrip("/").lower()
                if _cg_url:
                    _cascade_groups.append((oid, _cg_url))

            if not _chunk_oids:
                return 0

            _score_ops = []
            for _oid in _chunk_oids:
                _source = _campaign_source_by_oid.get(_oid)
                if not _source:
                    continue
                _score_result = compute_campaign_score(
                    _source,
                    campaign,
                    profile=_source.get("linkedin_profile_data"),
                    company=_source.get("company_data"),
                )
                _source["_campaign_fit_score"] = _score_result["fit_score"]
                _source["_campaign_priority_tier"] = _score_result["priority_tier"]
                _score_ops.append(score_update_operation(
                    account_id=account_id,
                    campaign_id=campaign_oid,
                    prospect_id=_oid,
                    result=_score_result,
                    scoring_version=_scoring_version,
                    cohort_id=_cohort_id,
                    cohort_label="selected",
                ))
            await persist_campaign_scores(_score_ops)
            await ensure_cohort_membership(
                account_id=account_id,
                campaign_id=campaign_oid,
                prospect_ids=_chunk_oids,
                cohort_id=_cohort_id,
                cohort_label="selected",
                scoring_version=_scoring_version,
            )

            _chunk_full = await database.prospects_collection.find(
                {"_id": {"$in": _chunk_oids}}
            ).to_list(length=None)
            for _persisted in _chunk_full:
                _source = _campaign_source_by_oid.get(_persisted.get("_id")) or {}
                if _source.get("_campaign_fit_score") is not None:
                    _persisted["_campaign_fit_score"] = _source["_campaign_fit_score"]
                if _source.get("_campaign_priority_tier") is not None:
                    _persisted["_campaign_priority_tier"] = _source["_campaign_priority_tier"]
            if _chunk_full:
                await _pre_enroll_prospects(campaign, _chunk_full)

            # Tag primaries with cascade grouping (position 0) so the rotation
            # engine can find their backups when the sequence exhausts unanswered.
            if _cascade_groups:
                try:
                    await database.campaign_enrollments_collection.bulk_write(
                        [
                            UpdateOne(
                                {"campaign_id": campaign_oid, "prospect_id": _p_oid},
                                {"$set": {"cascade_group_id": _cg, "cascade_position": 0, "cascade_status": "primary"}},
                            )
                            for _p_oid, _cg in _cascade_groups
                        ],
                        ordered=False,
                    )
                except Exception as _cgt_e:
                    logger.warning(f"[fast:{campaign_id}] cascade tagging failed: {_cgt_e}")

            logger.info(f"[fast:{campaign_id}] {label}: persisted+enrolled {len(_chunk_oids)} prospects")
            return len(_chunk_oids)

        _backup_counts: dict = {}

        async def _create_backup_enrollments(pairs: list[tuple[dict, dict]]) -> int:
            """Store next-best gate survivors per company as cascade_waiting backups:
            prospect upserted to the pool, minimal enrollment doc created — NO email
            lookup, NO message generation. The engine activates position N+1 when the
            primary (position 0) exhausts the sequence with status not_replied."""
            if not pairs:
                return 0
            _now = datetime.utcnow()
            # Independent upserts (own linkedin/email key each) — run concurrently,
            # then build enrollment docs in the original pair order below.
            _upsert_oids = await asyncio.gather(
                *[_upsert_curated_prospect(t, campaign_oid, account_id) for t, sc in pairs]
            )
            _backup_ops: list[UpdateOne] = []
            for (t, sc), oid in zip(pairs, _upsert_oids):
                if not oid or oid in _seen_prospect_oids:
                    continue
                _seen_prospect_oids.add(oid)
                _cg_url = (t.get("company_linkedin") or sc.get("company_linkedin_url") or "").rstrip("/").lower()
                if not _cg_url or _backup_counts.get(f"pos:{_cg_url}", 0) >= _PER_COMPANY_BACKUP_COUNT:
                    continue
                _pos = _backup_counts.get(f"pos:{_cg_url}", 0) + 1
                _backup_counts[f"pos:{_cg_url}"] = _pos
                _backup_ops.append(UpdateOne(
                    {"campaign_id": campaign_oid, "prospect_id": oid},
                    {"$setOnInsert": {
                        "campaign_id": campaign_oid,
                        "account_id": campaign.get("account_id"),
                        "prospect_id": oid,
                        "status": "cascade_waiting",
                        "cascade_status": "waiting",
                        "cascade_group_id": _cg_url,
                        "cascade_position": _pos,
                        "cascade_activate_at": None,  # event-driven, not timed
                        "created_at": _now,
                        "updated_at": _now,
                    }},
                    upsert=True,
                ))

            _created = 0
            if _backup_ops:
                try:
                    await database.campaign_enrollments_collection.bulk_write(_backup_ops, ordered=False)
                    _created = len(_backup_ops)
                except Exception as _be_e:
                    logger.warning(f"[fast:{campaign_id}] backup enrollment bulk_write failed: {_be_e}")
            if _created:
                logger.info(f"[fast:{campaign_id}] stored {_created} cascade backups")
            return _created

        async def _process_scraped_employees(
            chunk_employees: list[dict],
            chunk_urls: list[str],
            *,
            label: str,
            skip_ai_gate: bool = False,
        ) -> int:
            """Full per-chunk pipeline: attribute → transform → score → strict gate →
            AI title gate → per-company cap → remaining-target cap → email → re-score →
            persist/enroll → live counter updates. Returns enrolled count for the chunk."""
            nonlocal _enrolled_running, _dropped_off_icp, _dropped_no_url
            raw_employees.extend(chunk_employees)

            _pairs: list[tuple[dict, dict]] = []
            for emp in chunk_employees:
                co_url = _extract_company_url_from_employee(emp)
                if co_url:
                    returned_company_urls.add(co_url)
                    sc = url_to_sc.get(co_url) or _find_closest_sc(co_url, url_to_sc)
                    if sc is None:
                        _dropped_off_icp += 1
                        continue
                else:
                    _dropped_no_url += 1
                    continue
                _pairs.append((emp, sc))
            employee_pairs.extend(_pairs)

            _transformed = [transform_employee_to_prospect(emp, sc) for emp, sc in _pairs]
            # Re-resolve SC via the prospect's own company_linkedin (reliable in batch mode)
            for i in range(len(_pairs)):
                emp_i, sc_i = _pairs[i]
                t_co_url = (_transformed[i].get("company_linkedin") or "").rstrip("/").lower()
                if not t_co_url or (sc_i.get("company_linkedin_url") or "").rstrip("/").lower() == t_co_url:
                    continue
                resolved = _url_to_sc_norm.get(t_co_url)
                if resolved:
                    _pairs[i] = (emp_i, resolved)
                    _transformed[i] = transform_employee_to_prospect(emp_i, resolved)

            _candidates: list[tuple[dict, dict]] = []
            for t, (_, sc) in zip(_transformed, _pairs):
                score = _score_for_campaign(t, campaign)
                t["fit_score"] = score
                t["ai_prospect_score"] = float(score)
                _candidates.append((t, sc))

            # Strict-only gate per chunk; relaxable rejects pool globally and the relax
            # ladder runs ONCE after all chunks (same floor semantics as before).
            _kept, _stats = _gate_and_select(
                _candidates, campaign, prospect_target,
                campaign_id=campaign_id, label=label, collect_relaxable=_relax_pool,
            )
            for k, v in _stats.items():
                _gate_stats_total[k] = _gate_stats_total.get(k, 0) + v

            if not skip_ai_gate:
                _kept = await _apply_ai_title_gate(_kept)

            # Per-company enrollment cap (global counter — companies can span chunks
            # after re-attribution).
            _capped: list[tuple[dict, dict]] = []
            _chunk_co_counts: dict = {}
            _kept.sort(key=lambda p: p[0].get("fit_score", 0), reverse=True)
            for t, sc in _kept:
                sc_id = sc.get("_id") or sc.get("company_linkedin_url", "")
                if per_company_counts.get(sc_id, 0) + _chunk_co_counts.get(sc_id, 0) >= _enroll_cap:
                    continue
                _chunk_co_counts[sc_id] = _chunk_co_counts.get(sc_id, 0) + 1
                _capped.append((t, sc))

            # Cap to remaining global target
            _remaining = max(0, prospect_target - _enrolled_running)
            _capped = _capped[:_remaining]

            # Backups: next-best gate survivors per company beyond the primary cap —
            # stored as cascade_waiting (no email spend), rotated in on no-reply.
            _sel = {id(t) for t, _ in _capped}
            _backup_pairs: list[tuple[dict, dict]] = []
            for t, sc in _kept:
                if id(t) in _sel:
                    continue
                _bk_url = (t.get("company_linkedin") or sc.get("company_linkedin_url") or "").rstrip("/").lower()
                if not _bk_url or _backup_counts.get(f"pos:{_bk_url}", 0) >= _PER_COMPANY_BACKUP_COUNT:
                    continue
                _backup_pairs.append((t, sc))
            if _backup_pairs:
                await _create_backup_enrollments(_backup_pairs)

            if not _capped:
                return 0

            await _find_emails_for(_capped)
            # Re-score with email+industry populated (activates the 15-pt email and
            # 18-pt industry components).
            for t, _ in _capped:
                _up = _score_for_campaign(t, campaign)
                t["fit_score"] = _up
                t["ai_prospect_score"] = float(_up)

            _n = await _persist_and_enroll(_capped, label=label)
            _enrolled_running += _n

            # Live progress: counters tick + per-company scrape status flips as each
            # chunk lands (consumed by the discovery SSE stream + polling UI).
            try:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {
                        "discovery_prospects_found": _n,
                        "discovery_prospects_from_apify": _n,
                    }},
                )
                if chunk_urls:
                    await database.sourced_companies_collection.update_many(
                        {"campaign_id": campaign_id, "company_linkedin_url": {"$in": chunk_urls}},
                        {"$set": {"employee_scrape_status": "completed", "updated_at": datetime.utcnow()}},
                    )
            except Exception as _lp_e:
                logger.warning(f"[fast:{campaign_id}] live progress update failed: {_lp_e}")
            return _n

        # ── Stage C reused prospects: enroll FIRST (instant, no scraping needed) ──
        # Pool reuse never implies score reuse — evaluate each against this campaign.
        for _rp, _ in _reused_pairs:
            _reused_score = _score_for_campaign(_rp, campaign)
            _rp["fit_score"] = _reused_score
            _rp["_campaign_fit_score"] = float(_reused_score)
        _db_enrolled_count = await _persist_and_enroll(_reused_pairs, label="pool_reuse")
        if _db_enrolled_count:
            try:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$inc": {
                        "discovery_prospects_found": _db_enrolled_count,
                        "discovery_prospects_from_db": _db_enrolled_count,
                    }},
                )
            except Exception:
                pass

        # ── Scrape chunks concurrently; process each as it completes ──
        _chunks: list[list[str]] = [
            company_urls[i:i + _SCRAPE_CHUNK_SIZE]
            for i in range(0, len(company_urls), _SCRAPE_CHUNK_SIZE)
        ]
        _scrape_sem = asyncio.Semaphore(max(1, _scrape_concurrency))

        async def _scrape_chunk(idx: int, urls: list[str]) -> tuple[list[str], list[dict]]:
            async with _scrape_sem:
                try:
                    _emps = await _with_retries(
                        lambda: bulk_scrape_employees_for_companies(
                            urls,
                            max_items_per_company=_scrape_depth,
                            max_total_items=math.ceil(len(urls) * _scrape_depth),
                            seniority_level_ids=seniority_ids or None,
                            functional_level_ids=functional_ids or None,
                            profile_scraper_mode=_profile_mode,
                            account_id=account_id,
                            campaign_id=campaign_id,
                            max_concurrency=1,
                            company_headcount_bands=headcount_bands or None,
                        ),
                        retries=2,
                    )
                    return urls, _emps
                except Exception as _sc_e:
                    logger.warning(f"[fast:{campaign_id}] scrape chunk {idx} failed: {_sc_e}")
                    return urls, []

        for _fut in asyncio.as_completed([_scrape_chunk(i, u) for i, u in enumerate(_chunks)]):
            _c_urls, _c_emps = await _fut
            if _c_emps:
                await _process_scraped_employees(_c_emps, _c_urls, label="first_pass")

        logger.info(
            f"[fast:{campaign_id}] streaming scrape done — raw={len(raw_employees)}, "
            f"enrolled={_enrolled_running}, ai_gate_rejected={_ai_gate_rejected}, "
            f"dropped_off_icp={_dropped_off_icp}, dropped_no_url={_dropped_no_url}"
        )

        # ── Global relax ladder (runs once, after all chunks) ──
        import math as _math_relax
        _relax_floor = int(_math_relax.ceil(_GATE_RELAX_FRACTION * max(1, prospect_target)))
        if _enrolled_running < _relax_floor and _relax_pool:
            _relax_pool.sort(key=lambda p: p[0].get("fit_score", 0), reverse=True)
            _need = _relax_floor - _enrolled_running
            # Relax admissions must STILL pass the AI title judge — skipping it here
            # was the hole that let function-adjacent titles back in.
            _readmit = await _apply_ai_title_gate(_relax_pool[:_need * 2])
            _readmit = _readmit[:_need]
            logger.warning(
                f"[fast:{campaign_id}] gate relax: enrolled {_enrolled_running} < floor "
                f"{_relax_floor} — re-admitting {len(_readmit)} best-scored "
                f"title-ambiguous rejects (function mismatches + blocklist stay enforced)"
            )
            await _find_emails_for(_readmit)
            _relaxed_n = await _persist_and_enroll(_readmit, label="relax_ladder")
            _enrolled_running += _relaxed_n
            _gate_stats_total["relaxed_in"] = _relaxed_n
            if _relaxed_n:
                try:
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$inc": {
                            "discovery_prospects_found": _relaxed_n,
                            "discovery_prospects_from_apify": _relaxed_n,
                        }},
                    )
                except Exception:
                    pass

        try:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"discovery_gate_stats": {"first_pass": _gate_stats_total}}},
            )
        except Exception:
            pass

        # ── ⑤ Recovery for low-yield companies (reuses the streaming chunk pipeline) ──
        # Short mode doesn't return companyLinkedinUrl, so per-company URL tracking
        # is unreliable. Trigger recovery on ALL companies if first-pass yield is
        # < 1 employee per company on average (indicates most got 0 results).
        zero_emp_urls = company_urls  # fall back to full list if URL matching fails
        if returned_company_urls:
            # URL matching worked (Full mode or actor returned companyLinkedinUrl)
            zero_emp_urls = [
                url for url in company_urls
                if not any(url == ret for ret in returned_company_urls)
            ]

        low_yield = len(raw_employees) < len(company_urls)  # < 1 emp per company
        remaining_needed = prospect_target - _enrolled_running
        if zero_emp_urls and low_yield and remaining_needed > 0:
            logger.info(
                f"[fast:{campaign_id}] recovery: {len(zero_emp_urls)} companies, "
                f"first-pass yield={len(raw_employees)}/{len(company_urls)}, broadening seniority+function"
            )
            broad_seniority = _broaden_seniority_ids(seniority_ids)
            broad_function = _broaden_function_ids(functional_ids)

            try:
                recovery_employees = await _with_retries(
                    lambda: bulk_scrape_employees_for_companies(
                        zero_emp_urls,
                        max_items_per_company=_scrape_depth,
                        max_total_items=math.ceil(remaining_needed * _dropout_buffer),
                        seniority_level_ids=broad_seniority or None,
                        functional_level_ids=broad_function or None,
                        profile_scraper_mode=_profile_mode,
                        account_id=account_id,
                        campaign_id=campaign_id,
                        company_headcount_bands=headcount_bands or None,
                        max_concurrency=int(
                            campaign.get("discovery_scrape_concurrency")
                            or min(_DEFAULT_SCRAPE_CONCURRENCY, max(1, math.ceil(len(zero_emp_urls) / _SCRAPE_CHUNK_SIZE)))
                        ),
                    ),
                    retries=2,
                )
                if recovery_employees:
                    # Same attribution + gates + enroll tail as first-pass chunks.
                    _rn = await _process_scraped_employees(
                        recovery_employees, zero_emp_urls, label="recovery",
                    )
                    logger.info(f"[fast:{campaign_id}] recovery added {_rn} prospects")
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] recovery scrape failed (skipping): {e}")

        if _email_credit_warnings:
            logger.error(
                f"[fast:{campaign_id}] email finder credit warning(s): {_email_credit_warnings}"
            )
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$addToSet": {"discovery_warnings": {"$each": _email_credit_warnings}}},
            )

        _all_kept_sources = list(_campaign_source_by_oid.values())
        with_email = sum(1 for t in _all_kept_sources if t.get("email"))
        logger.info(
            f"[fast:{campaign_id}] email fill: {with_email}/{len(_all_kept_sources)} have email "
            f"({emails_applied} new from finder)"
        )
        if _all_kept_sources:
            _sc_vals = [t.get("fit_score", 0) for t in _all_kept_sources]
            logger.info(
                f"[fast:{campaign_id}] FINAL SCORES: n={len(_sc_vals)} "
                f"min={min(_sc_vals):.1f} max={max(_sc_vals):.1f} "
                f"mean={sum(_sc_vals)/len(_sc_vals):.1f}"
            )

        # ── Final per-company stats + counter sync (absolute values reconcile the
        # incremental $inc updates made during streaming) ──
        for sc in kept_companies:
            sc_id = sc.get("_id") or sc.get("company_linkedin_url", "")
            co_url = sc.get("company_linkedin_url")
            scrape_count = sum(
                1 for emp, _ in employee_pairs
                if _extract_company_url_from_employee(emp) == co_url
            )
            await database.sourced_companies_collection.update_one(
                {"campaign_id": campaign_id, "company_linkedin_url": co_url},
                {"$set": {
                    "employee_scrape_status": "completed",
                    "employees_scraped_count": scrape_count,
                    "prospects_created_count": per_company_counts.get(sc_id, 0),
                }},
            )

        total_prospects = len(new_prospect_oids)
        _scraped_count = total_prospects - _db_enrolled_count
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "curated_companies_scraped": len(companies_to_scrape),
                "discovery_prospects_found": total_prospects,
                "discovery_prospects_from_apify": max(0, _scraped_count),
                "discovery_prospects_from_db": _db_enrolled_count,
                "discovery_status": "enriching",
            }},
        )

        # ── ⑧ Channel planning + Day-1 message generation ─────────
        # (upsert + cohort scoring + pre-enroll already happened per chunk above)

        # NOTE: the terminal completed/awaiting_approval flip is deferred until after
        # channel planning + the top-up decision below, so a top-up generation isn't
        # prematurely marked "completed" (which would make the next generation's job
        # short-circuit as already-completed and hide the "finding more" banner).

        # ── Channel planning (auto-pick senders + assign channel+day to enrollments) ──
        plan_result = await finalize_channel_plan(campaign_id, account_id)
        total_assigned = plan_result.get("assigned", 0)

        # Recompute total_enrolled from the live enrollments (not = assigned) so it
        # stays correct across top-up generations (each generation appends).
        total_enrolled = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "status": {"$nin": ["skipped_no_channel", "archived", "pending_teammate_review", "cascade_waiting"]},
        })

        # ── Auto top-up decision ──────────────────────────────────────────────────
        # If this generation's yield leaves us under the target and we haven't hit
        # the generation cap AND this pass actually added prospects, enqueue an
        # append-only follow-up generation sourcing NEW companies.
        _should_topup = (
            total_enrolled < prospect_target_full * _TOPUP_THRESHOLD
            and generation < _MAX_TOPUP_GENERATIONS
            and total_prospects > 0
        )

        _meta_set: dict = {
            "discovery_prospects_planned": total_assigned,
            "discovery_prospects_enrolled": total_assigned,
            "total_enrolled": total_enrolled,
        }
        if _should_topup:
            _next_gen = generation + 1
            _topup_msg = (
                f"Finding more prospects — {total_enrolled} of ~{prospect_target_full} so far…"
            )
            _meta_set.update({
                "discovery_status": "topping_up",
                "discovery_topup_active": True,
                "discovery_topup_message": _topup_msg,
                # The follow-up job's tenant-ownership check matches on
                # discovery_generation, so advance it before enqueueing.
                "discovery_generation": _next_gen,
            })
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid}, {"$set": _meta_set}
            )
            from services.enrichment_job_service import enqueue_campaign_discovery
            await enqueue_campaign_discovery(
                account_id=str(account_id),
                campaign_id=campaign_id,
                generation=_next_gen,
            )
            logger.info(
                f"[fast:{campaign_id}] top-up: enrolled={total_enrolled} < "
                f"{prospect_target_full * _TOPUP_THRESHOLD:.0f} — enqueued generation {_next_gen}"
            )
        else:
            # Terminal: target met, capped, or this pass added nothing new.
            _meta_set.update({
                "discovery_status": "completed",
                "discovery_completed_at": datetime.utcnow(),
                "status": "awaiting_approval",
                "discovery_topup_active": False,
            })
            # Only clear approvals on the first generation — a top-up must not wipe an
            # approval the user already granted on an earlier generation.
            if generation == 1:
                _meta_set["approved_send_days"] = []
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid}, {"$set": _meta_set}
            )

        # Queue Day-1 enrichment (all enrolled) + Day-1 message generation under the
        # durable worker lease. This runs on every generation so top-up additions
        # also get enriched.
        if total_assigned > 0:
            await _enqueue_day1_enrichment_and_messages(
                campaign_id, str(account_id)
            )
        else:
            # No prospects assigned (no sender or no contactable prospects) — resolve
            # the spinner explicitly so the UI shows the actionable "No schedule yet" state.
            # Surface WHY: distinguish "0 scraped/found" from "found but unplannable"
            # (finalize_channel_plan only sets a reason for the latter). Skip the error
            # when a top-up is in flight — the next generation may still find prospects.
            _zero_update: dict = {
                "message_gen_status": "completed",
                "message_gen_completed_at": datetime.utcnow(),
            }
            if (
                generation == 1
                and not _should_topup
                and total_prospects == 0
                and not campaign.get("discovery_error")
            ):
                _zero_msg = (
                    f"Found {len(kept_companies)} matching companies but couldn't extract any "
                    "contactable people from them. Try broadening the ICP (seniority/titles/"
                    "departments) or removing narrow filters, then re-run discovery."
                )
                _zero_update["discovery_error"] = _zero_msg
                _zero_update["discovery_failure_reason"] = _zero_msg
                logger.warning(f"[fast:{campaign_id}] 0 prospects from {len(kept_companies)} companies — {_zero_msg}")
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": _zero_update},
            )

        logger.info(
            f"[fast:{campaign_id}] complete (gen {generation}) — "
            f"companies={len(kept_companies)} (scraped={len(companies_to_scrape)}, reused_co={len(kept_companies)-len(companies_to_scrape)}), "
            f"prospects={total_prospects} (scraped={max(0,_scraped_count)}, reused={_db_enrolled_count}), "
            f"assigned={total_assigned}, enrolled={total_enrolled}, topping_up={_should_topup}"
        )
        return {
            "campaign_id": campaign_id,
            "prospects_created": total_prospects,
            "prospects_from_db": _db_enrolled_count,
            "generation": generation,
            "topping_up": _should_topup,
        }

    except Exception as e:
        logger.exception(f"[fast:{campaign_id}] failed")
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "discovery_status": "failed",
                "discovery_error": str(e)[:500],
            }},
        )
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Day-1 message generation (background, fail-soft)
# ──────────────────────────────────────────────────────────────────────────────

async def _run_day1_message_gen(campaign_id: str, account_id: str) -> None:
    """
    Campaign-level Day-1 message generation.
    Replaces the old per-enrollment _generate_day1_messages.
    Uses generate_messages_for_campaign so that campaign-level message_gen_status
    advances running→completed (resolves the Schedule tab spinner).
    send_day=1 keeps generation scoped to Day-1 enrollments (message_gen_status:pending).
    """
    try:
        from services.campaign_message_generator_service import generate_messages_for_campaign
        await generate_messages_for_campaign(campaign_id, account_id, send_day=1)
        logger.info(f"[fast:{campaign_id}] Day-1 messages generated via campaign-level generator")
    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] Day-1 message gen failed: {e}")
        try:
            await database.campaigns_collection.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"message_gen_status": "failed", "message_gen_error": str(e)[:300]}},
            )
        except Exception:
            pass


async def _run_deep_enrichment_then_messages(
    campaign_id: str,
    account_id: str,
    day1_prospect_oids: list[ObjectId],
    all_prospect_oids: list[ObjectId],
) -> None:
    """
    Full deep enrichment pipeline:
    0. Scrape enrolled company LinkedIn pages (one-time)
    1. Scrape LinkedIn posts for Day-1 cohort (up to 45 prospects)
    2. Generate prospect_intelligence for Day-1 cohort in batches of 5
    3. Store intelligence on prospect docs
    4. Run Day-1 message generation (uses intelligence when available)
    5. In background: enrich remaining prospects (Days 2-3) and generate their messages
    """
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        # Read skip-message-gen flag from campaign doc (gated — default False so prod is unchanged)
        skip_message_gen = False
        try:
            _flag_doc = await database.campaigns_collection.find_one(
                {"_id": ObjectId(campaign_id)}, {"discovery_skip_message_gen": 1}
            )
            skip_message_gen = bool((_flag_doc or {}).get("discovery_skip_message_gen", False))
        except Exception:
            pass
        if skip_message_gen:
            logger.info(f"[fast:{campaign_id}] discovery_skip_message_gen=True — will skip message gen")

        # ── Company LinkedIn scrape (enrolled companies, one-time) ─────────────────
        try:
            # Get enrolled company URLs from the campaign's prospects
            all_enrolled_prospects = await database.prospects_collection.find(
                {"_id": {"$in": all_prospect_oids}}
            ).to_list(length=len(all_prospect_oids))

            enrolled_co_urls = list({
                (p.get("company_linkedin") or "").rstrip("/")
                for p in all_enrolled_prospects
                if p.get("company_linkedin")
            })

            if enrolled_co_urls:
                logger.info(f"[fast:{campaign_id}] scraping {len(enrolled_co_urls)} enrolled company LinkedIn pages")
                from services.company_scraper_service import scrape_company_pages
                _, co_pages = await scrape_company_pages(enrolled_co_urls)

                if co_pages:
                    from services.employee_scraper_service import _save_company
                    co_by_url: dict[str, dict] = {}
                    for cp in co_pages:
                        # Build url-keyed lookup for the research step (handles both camelCase and snake_case keys)
                        li_url = (
                            cp.get("linkedinUrl") or cp.get("linkedin_url")
                            or cp.get("url") or cp.get("companyUrl") or ""
                        ).rstrip("/")
                        if li_url:
                            co_by_url[li_url.lower()] = cp

                    # Canonicalize each company page (sets industry.id + location.country_code + employee_band)
                    # so companies are matchable by Stage-A DB search on the next campaign.
                    _save_results = await asyncio.gather(
                        *[_save_company(cp) for cp in co_pages],
                        return_exceptions=True,
                    )
                    _saved_count = sum(1 for r in _save_results if r and not isinstance(r, Exception))
                    logger.info(
                        f"[fast:{campaign_id}] saved {_saved_count}/{len(co_pages)} company pages "
                        f"to companies_collection (canonical)"
                    )

                    # company data is now stored on companies_collection (not inline on prospects)
        except Exception as _co_err:
            logger.warning(f"[fast:{campaign_id}] company LinkedIn scrape failed (non-fatal): {_co_err}")

        # ── Per-company news + competitor research (gated by campaign doc flag) ─────────────
        # Runs ONCE per company, cached in co_research_by_url, shared across all prospects.
        # Enable: set campaign.discovery_enable_company_research = True (script only for now).
        co_research_by_url: dict = {}
        campaign_doc_for_research = await database.campaigns_collection.find_one(
            {"_id": ObjectId(campaign_id)}, {"discovery_enable_company_research": 1}
        )
        enable_company_research = bool(
            (campaign_doc_for_research or {}).get("discovery_enable_company_research", True)
        )

        if enable_company_research:
            try:
                from services.company_research_service import deep_research_companies_bulk

                # Collect unique companies from all enrolled prospects
                _all_enrolled = await database.prospects_collection.find(
                    {"_id": {"$in": all_prospect_oids}},
                    {"company_linkedin": 1, "company_name": 1, "company_domain": 1, "industry": 1},
                ).to_list(length=len(all_prospect_oids))

                _companies_for_research: dict[str, dict] = {}
                for _p in _all_enrolled:
                    _li_url = (_p.get("company_linkedin") or "").rstrip("/")
                    if _li_url and _li_url not in _companies_for_research:
                        _companies_for_research[_li_url] = {
                            "name": _p.get("company_name") or "",
                            "domain": _p.get("company_domain") or "",
                            "industry": _p.get("industry") or "",
                        }

                logger.info(
                    f"[fast:{campaign_id}] company research: {len(_companies_for_research)} unique companies"
                )

                # Seller context for best-performer ranking (relative to what we pitch)
                _acct_profile = await database.company_profiles_collection.find_one(
                    {"account_id": account_id}
                )

                co_research_by_url = await deep_research_companies_bulk(
                    _companies_for_research,
                    _acct_profile,
                    cost_tags={
                        "account_id": str(account_id),
                        "campaign_id": str(campaign_id),
                        "feature": "company_research",
                    },
                    max_concurrency=8,
                    persist=True,   # stored on companies_collection.research per contract
                )

                logger.info(
                    f"[fast:{campaign_id}] company research complete: "
                    f"{len(co_research_by_url)} companies, "
                    f"{sum(len(v.get('news', [])) for v in co_research_by_url.values())} news items, "
                    f"{sum(len(v.get('competitors', [])) for v in co_research_by_url.values())} competitors, "
                    f"{sum(len(v.get('buying_signals', [])) for v in co_research_by_url.values())} buying signals, "
                    f"{sum(len(v.get('company_posts', [])) for v in co_research_by_url.values())} company posts"
                )
            except Exception as _re:
                logger.warning(f"[fast:{campaign_id}] company research phase failed (non-fatal): {_re}")
                co_research_by_url = {}
                # Durable breadcrumb: without this, a total research failure is
                # indistinguishable from "companies genuinely have no research".
                try:
                    await database.campaigns_collection.update_one(
                        {"_id": ObjectId(campaign_id)},
                        {"$set": {"discovery_research_error": f"{type(_re).__name__}: {_re}"[:300]}},
                    )
                except Exception:
                    pass

        # ── Day-1 deep enrichment ─────────────────────────────────────────────
        if day1_prospect_oids:
            await _enrich_prospect_cohort(
                prospect_oids=day1_prospect_oids,
                campaign_id=campaign_id,
                account_id=account_id,
                label="day1",
                co_research_by_url=co_research_by_url or None,
            )

        # ── Day-1 message generation (now with intelligence) ──────────────────
        if skip_message_gen:
            # Mark day1 enrollments as skipped so the poll's pending count drains
            await database.campaign_enrollments_collection.update_many(
                {
                    "campaign_id": ObjectId(campaign_id),
                    "prospect_id": {"$in": day1_prospect_oids},
                    "message_gen_status": {"$in": [None, "pending"]},
                },
                {"$set": {"message_gen_status": "skipped"}},
            )
            logger.info(f"[fast:{campaign_id}] Day-1 message gen skipped ({len(day1_prospect_oids)} enrollments marked skipped)")
        else:
            await generate_messages_for_campaign(campaign_id, account_id, send_day=1)
            logger.info(f"[fast:{campaign_id}] Day-1 messages generated")

        # ── Days 2-5 enrichment + message gen (durable, out-of-band) ─────────
        # Enqueue a leased job instead of a fire-and-forget task so the work
        # survives a worker restart. Keyed by campaign so a discovery re-run
        # coalesces onto the single in-flight remaining-days job. Fail closed on
        # missing tenant context: without an account_id the job cannot be owned.
        remaining_oids = [oid for oid in all_prospect_oids if oid not in set(day1_prospect_oids)]
        if remaining_oids:
            if not account_id:
                raise ValueError("cannot enqueue remaining-days enrichment without account_id")
            from services.enrichment_job_service import enqueue_campaign_remaining_days
            await enqueue_campaign_remaining_days(
                account_id=account_id,
                campaign_id=campaign_id,
                remaining_oids=remaining_oids,
                co_research_by_url=co_research_by_url or None,
                skip_message_gen=skip_message_gen,
            )

    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] deep enrichment+messages failed: {e}")
        try:
            await database.campaigns_collection.update_one(
                {"_id": ObjectId(campaign_id)},
                {"$set": {"message_gen_status": "failed", "message_gen_error": str(e)[:300]}},
            )
        except Exception:
            pass


async def _enrich_prospect_cohort(
    prospect_oids: list[ObjectId],
    campaign_id: str,
    account_id: str,
    label: str = "cohort",
    co_research_by_url: dict | None = None,
) -> None:
    """Scrape posts + generate intelligence for a batch of prospect OIDs.

    co_research_by_url: optional {company_linkedin_url: {competitors, news}} cache
    from _run_deep_enrichment_then_messages. When present, injected into each prospect
    dict before intelligence generation so news/competitors reach the AI prompt.
    """
    from services.linkedin_post_scraper_service import scrape_linkedin_posts_bulk
    from services.prospect_intelligence_service import (
        generate_base_intelligence_batch,
        store_base_intelligence,
        generate_pitch_batch,
        store_pitch_for_account,
    )

    if not prospect_oids:
        return

    _campaign_meta = await database.campaigns_collection.find_one(
        {"_id": ObjectId(campaign_id)}, {"scoring_version": 1}
    )
    _scoring_version = (_campaign_meta or {}).get("scoring_version") or DEFAULT_SCORING_VERSION

    async def _transition_many(state: str, ids: list[ObjectId], **kwargs) -> None:
        # Same state + kwargs for every id in this cohort — one bulk_write
        # instead of one find_one_and_update round trip per prospect.
        await bulk_transition_enrichment(
            account_id=account_id,
            campaign_id=campaign_id,
            prospect_ids=ids,
            state=state,
            scoring_version=_scoring_version,
            **kwargs,
        )

    await _transition_many("running", prospect_oids)

    try:
        prospects = await database.prospects_collection.find(
            {"_id": {"$in": prospect_oids}}
        ).to_list(length=len(prospect_oids))

        if not prospects:
            await _transition_many("not_found", prospect_oids, outcome="prospect_missing")
            return

        found_ids = {p["_id"] for p in prospects}
        missing_ids = [pid for pid in prospect_oids if pid not in found_ids]
        if missing_ids:
            await _transition_many("not_found", missing_ids, outcome="prospect_missing")

        company_profile = await database.company_profiles_collection.find_one(
            {"account_id": account_id}
        )

        # Scrape posts for all prospects with a LinkedIn URL
        li_urls = [p["linkedin"] for p in prospects if p.get("linkedin")]
        posts_by_url: dict = {}
        if li_urls:
            try:
                posts_by_url = await scrape_linkedin_posts_bulk(li_urls, posts_per_profile=5)
                logger.info(
                    f"[fast:{campaign_id}] {label} post scrape: "
                    f"{sum(1 for v in posts_by_url.values() if v)}/{len(li_urls)} profiles had posts"
                )
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] {label} post scrape failed (continuing): {e}")

        # Attach posts + company research to prospect dicts for intelligence generation
        enriched_prospects = []
        for p in prospects:
            p_copy = dict(p)
            p_copy["posts"] = posts_by_url.get(p.get("linkedin") or "", [])
            # Inject company-level research so AI intel can reference news, competitors,
            # best performer, buying signals and company posts (full contract object).
            if co_research_by_url:
                co_url = (p.get("company_linkedin") or "").rstrip("/")
                research = (
                    co_research_by_url.get(co_url)
                    or co_research_by_url.get(co_url.lower())
                    or {}
                )
                if research:
                    p_copy["company_research"] = research
                    p_copy["company_competitors"] = research.get("competitors", [])
                    p_copy["company_news"] = research.get("news", [])
                    # Also inject into fields the batch message prompt reads
                    p_copy["recent_news"] = research.get("news", [])
                    if research.get("buying_signals"):
                        p_copy["buying_signals"] = research["buying_signals"]
                    _comp_names = [c.get("name", "") for c in research.get("competitors", [])[:3] if c.get("name")]
                    if _comp_names:
                        p_copy.setdefault("ai_assessment", {})["competitor_summary"] = ", ".join(_comp_names)
            enriched_prospects.append(p_copy)

        # ── Base intelligence (tenant-agnostic, stored on prospects.prospect_intelligence_base) ──
        intelligence_list = await generate_base_intelligence_batch(
            enriched_prospects,
            account_id=account_id,
            campaign_id=campaign_id,
        )
        await store_base_intelligence(prospect_oids, intelligence_list)
        logger.info(
            f"[fast:{campaign_id}] {label} base intelligence stored for "
            f"{sum(1 for i in intelligence_list if i)}/{len(prospects)} prospects"
        )

        # Inject base intel back so pitch generation has it
        for p_copy, intel in zip(enriched_prospects, intelligence_list):
            p_copy["prospect_intelligence_base"] = intel

        # ── Per-tenant pitch (stored on prospect_state.pitch) ────────────────────────────────
        if company_profile:
            try:
                pitches = await generate_pitch_batch(
                    enriched_prospects,
                    company_profile,
                    account_id=account_id,
                    campaign_id=campaign_id,
                )
                await store_pitch_for_account(prospect_oids, pitches, account_id)
                logger.info(
                    f"[fast:{campaign_id}] {label} pitch stored for "
                    f"{sum(1 for p in pitches if p)}/{len(prospects)} prospects"
                )
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] {label} pitch generation failed (continuing): {e}")

        # Persist scraped posts to prospect documents
        if posts_by_url:
            from pymongo import UpdateOne as _UpdateOne
            _posts_ops = []
            for _li_url, _posts_list in posts_by_url.items():
                _p = next(
                    (p for p in prospects
                     if (p.get("linkedin") or "").rstrip("/").lower() == _li_url.rstrip("/").lower()),
                    None,
                )
                if _p and _p.get("_id") and _posts_list:
                    _posts_ops.append(_UpdateOne(
                        {"_id": _p["_id"]},
                        {"$set": {"posts": _posts_list}}
                    ))
            if _posts_ops:
                await database.prospects_collection.bulk_write(_posts_ops, ordered=False)
                logger.info(f"[fast:{campaign_id}] persisted posts for {len(_posts_ops)} prospects")

        # One durable terminal outcome per prospect. A provider returning no
        # intelligence is distinct from an exception and remains retryable by
        # policy only when explicitly classified that way. State/result vary
        # per prospect, so this batches into one bulk_write of heterogeneous
        # ops rather than one find_one_and_update round trip each.
        _terminal_ops = [
            transition_enrichment_operation(
                account_id=account_id,
                campaign_id=campaign_id,
                prospect_id=_prospect["_id"],
                scoring_version=_scoring_version,
                state="succeeded" if _intel else "not_found",
                outcome="intelligence_generated" if _intel else "no_intelligence",
                result={
                    "prospect_intelligence": _intel,
                    "posts": posts_by_url.get(_prospect.get("linkedin") or "", []),
                } if _intel else None,
            )
            for _prospect, _intel in zip(prospects, intelligence_list)
        ]
        if _terminal_ops:
            await bulk_write_state_operations(_terminal_ops)

    except Exception as e:
        await _transition_many(
            "retryable_failure",
            prospect_oids,
            outcome="pipeline_exception",
            error_code=type(e).__name__,
            error_message=str(e),
        )
        logger.warning(f"[fast:{campaign_id}] {label} enrichment failed (continuing): {e}")


async def _enrich_remaining_days(
    campaign_id: str,
    account_id: str,
    remaining_oids: list[ObjectId],
    co_research_by_url: dict | None = None,
    skip_message_gen: bool = False,
) -> None:
    """Enrich Days 2-5 prospects and generate their messages in background."""
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        await _enrich_prospect_cohort(
            prospect_oids=remaining_oids,
            campaign_id=campaign_id,
            account_id=account_id,
            label="days2-5",
            co_research_by_url=co_research_by_url,
        )
        if skip_message_gen:
            # Mark remaining enrollments as skipped so the poll's pending count drains
            await database.campaign_enrollments_collection.update_many(
                {
                    "campaign_id": ObjectId(campaign_id),
                    "prospect_id": {"$in": remaining_oids},
                    "message_gen_status": {"$in": [None, "pending"]},
                },
                {"$set": {"message_gen_status": "skipped"}},
            )
            logger.info(f"[fast:{campaign_id}] days2-5 message gen skipped ({len(remaining_oids)} enrollments marked skipped)")
        else:
            # Pre-generate day 2 messages so the user sees them immediately on the review page.
            # Days 3+ are generated on demand when the user approves each preceding day (D5).
            try:
                await generate_messages_for_campaign(campaign_id, account_id, send_day=2)
            except Exception as e:
                logger.warning(f"[fast:{campaign_id}] day 2 message gen failed: {e}")
    except Exception as e:
        logger.warning(f"[fast:{campaign_id}] remaining days enrichment failed: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# LLM batch scoring
# ──────────────────────────────────────────────────────────────────────────────

def _score_company_deterministic(company: dict, icp_prompt: str) -> float:
    """Deterministic company score (0-100). No LLM calls."""
    score = 0.0
    icp_lower = icp_prompt.lower()

    # Industry match (0-50): check if company industry keywords appear in ICP prompt.
    # industry may be a string (Gemini-sourced) or a dict (DB company with canonical fields).
    _ind_raw = company.get("industry") or ""
    if isinstance(_ind_raw, dict):
        industry = (_ind_raw.get("label") or _ind_raw.get("raw") or "").lower().strip()
    else:
        industry = str(_ind_raw).lower().strip()
    if industry:
        industry_words = [w for w in industry.replace("-", " ").split() if len(w) > 3]
        if any(word in icp_lower for word in industry_words):
            score += 50
        elif industry and len(industry) > 3 and industry[:4] in icp_lower:
            score += 30
    else:
        score += 25  # unknown industry: partial credit to not over-exclude

    # Company size hint (0-30): prefer companies that have size info
    size_raw = company.get("employee_size_estimate") or company.get("company_size")
    if size_raw:
        score += 30
    else:
        score += 10  # no size data: partial credit

    # Has LinkedIn URL (0-20): required for employee scraping
    if company.get("company_linkedin_url"):
        score += 20

    # Location match (0-10): soft boost if company country appears in ICP prompt
    _co_loc = company.get("location") or {}
    _country = (
        (_co_loc.get("country") if isinstance(_co_loc, dict) else None)
        or company.get("country") or ""
    ).lower()
    if _country and _country in icp_lower:
        score += 10

    # Funding match (0-10): soft boost if company funding stage appears in ICP prompt
    _funding = (company.get("funding_stage") or company.get("funding_raw") or "").lower()
    if _funding and _funding in icp_lower:
        score += 10

    return min(100.0, score)


# ──────────────────────────────────────────────────────────────────────────────
# Retry wrapper
# ──────────────────────────────────────────────────────────────────────────────

async def _with_retries(fn, retries: int = 3, backoffs: tuple = (1, 4, 16), fail_sentinel=None):
    """Retry an async callable on any exception, with exponential backoff."""
    last_exc = None
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                wait = backoffs[min(attempt, len(backoffs) - 1)]
                logger.warning(f"[fast] retry {attempt + 1}/{retries} after {wait}s: {e}")
                await asyncio.sleep(wait)
    if fail_sentinel is not None:
        logger.error(f"[fast] all {retries} retries exhausted, returning sentinel: {last_exc}")
        return fail_sentinel
    raise last_exc


# ──────────────────────────────────────────────────────────────────────────────
# ICP title synthesis (fallback when prefill left icp_job_titles empty)
# ──────────────────────────────────────────────────────────────────────────────

_FUNCTION_TITLE_NOUNS = {
    "marketing": "Marketing",
    "sales": "Sales",
    "engineering": "Engineering",
    "product": "Product",
    "operations": "Operations",
    "finance": "Finance",
    "hr": "People",
    "human resources": "People",
    "it": "IT",
    "customer success": "Customer Success",
    "design": "Design",
    "legal": "Legal",
    "growth": "Growth",
    "ecommerce": "Ecommerce",
}

_C_SUITE_BY_FUNCTION = {
    "marketing": ["CMO", "Chief Marketing Officer"],
    "sales": ["CRO", "Chief Revenue Officer"],
    "engineering": ["CTO", "Chief Technology Officer"],
    "product": ["CPO", "Chief Product Officer"],
    "operations": ["COO", "Chief Operating Officer"],
    "finance": ["CFO", "Chief Financial Officer"],
}


def _synthesize_icp_titles(functions: list[str], seniorities: list[str]) -> list[str]:
    """Derive concrete target titles from function × seniority when the campaign
    has none (e.g. ["marketing"] × ["manager"] → ["Marketing Manager",
    "Head of Marketing", ...]). Keeps the title gates active instead of blind."""
    titles: list[str] = []
    fns = [str(f).strip().lower() for f in functions if f and str(f).strip()]
    sens = {str(s).strip().lower() for s in seniorities if s and str(s).strip()}

    for fn in fns:
        noun = _FUNCTION_TITLE_NOUNS.get(fn) or fn.title()
        if not sens or "manager" in sens or "senior" in sens:
            titles += [f"{noun} Manager", f"Senior {noun} Manager"]
        if "director" in sens:
            titles += [f"Director of {noun}", f"{noun} Director"]
        if "vp" in sens:
            titles += [f"VP {noun}", f"Vice President of {noun}"]
        if sens & {"c_suite", "csuite"}:
            titles += _C_SUITE_BY_FUNCTION.get(fn, [f"Chief {noun} Officer"])
        titles.append(f"Head of {noun}")

    if sens & {"founder", "owner"}:
        titles += ["Founder", "Co-Founder", "CEO", "Owner"]

    # Dedup, preserve order, cap
    seen: set[str] = set()
    out: list[str] = []
    for t in titles:
        k = t.lower()
        if k not in seen:
            seen.add(k)
            out.append(t)
    return out[:12]


# ──────────────────────────────────────────────────────────────────────────────
# Deterministic person-fit gate + strict-then-relax selection
# ──────────────────────────────────────────────────────────────────────────────

def _gate_and_select(
    candidates: list[tuple[dict, dict]],
    campaign: dict,
    target_count: int,
    *,
    campaign_id: str = "",
    label: str = "first_pass",
    collect_relaxable: list | None = None,
) -> tuple[list[tuple[dict, dict]], dict]:
    """Apply the deterministic person-fit hard gate + score threshold to scored
    (prospect_dict, sourced_company) pairs, with a strict-then-relax ladder.

    collect_relaxable: when a list is passed, relaxable rejects are APPENDED to it
    and the internal relax ladder is skipped — the caller runs one global ladder
    after all streaming chunks complete (per-chunk ladders would over-relax).

    Hard gates (per utils.scoring.person_fit_gate):
      - title blocklist + icp_exclude_keywords  (NEVER relaxed)
      - function inference vs icp_functional_departments  (relaxable)
      - title-or-seniority match when the campaign specified either  (relaxable)
      - contactability (linkedin or email)  (never relaxed)
      - fit_score >= _EMPLOYEE_SCORE_THRESHOLD as secondary ranking  (never relaxed)

    Relax ladder: if strict survivors < _GATE_RELAX_FRACTION * target_count,
    re-admit the best-scored candidates rejected ONLY by the relaxable gates
    ("function_mismatch" / "no_title_or_seniority_match") until the floor is met.

    Returns (kept_pairs, stats_dict).
    """
    from utils.scoring import person_fit_gate

    kept: list[tuple[dict, dict]] = []
    relaxable_rejects: list[tuple[dict, dict]] = []
    stats: dict = {
        "candidates": len(candidates),
        "no_contact": 0,
        "below_score": 0,
        "title_keyword_blocklisted": 0,
        "non_decision_maker_title": 0,
        "function_mismatch": 0,
        "no_title_or_seniority_match": 0,
        "kept_strict": 0,
        "relaxed_in": 0,
    }
    # July 23: function_mismatch is NO LONGER relaxable — the relax ladder was
    # re-admitting grocery buyers / merchandise managers into marketing campaigns
    # (verified in prod gate stats: 30 function rejects, 24 re-admitted). A known
    # wrong function is a hard no; only title/seniority ambiguity may relax.
    _RELAXABLE = {"no_title_or_seniority_match"}

    # Whether the campaign actually specifies title/seniority criteria for
    # the deterministic gate to match against. When it doesn't (title gate
    # effectively disabled / no targets), person_fit_gate's title-or-
    # seniority sub-gate trivially passes everyone — that's NOT a real
    # "title gate passed" and must not bypass the score floor below.
    _has_title_criteria = bool(
        [x for x in (campaign.get("icp_job_titles") or []) if x and str(x).strip()]
    ) or bool(campaign.get("icp_seniority_levels") or campaign.get("seniorities"))

    for t, sc in candidates:
        if not (t.get("linkedin") or t.get("email")):
            stats["no_contact"] += 1
            continue
        ok, reason = person_fit_gate(t, campaign)
        if not ok:
            stats[reason] = stats.get(reason, 0) + 1
            if reason in _RELAXABLE:
                relaxable_rejects.append((t, sc))
            continue
        # title_gate_passed: this prospect matched the campaign's title/
        # seniority/function targets deterministically (and its company
        # already passed company_gate_service upstream, since `sc` only
        # exists for approved companies). Stamped on the prospect dict so
        # it survives the upsert into prospects_collection and can be read
        # later by campaign_launch_service.plan_channel_assignments — a
        # title-gate pass must not be silently dropped by the score floor,
        # even though scoring still runs for ranking/prioritization. When
        # the campaign has no title/seniority criteria, the gate isn't
        # really "passed" in this sense, so the floor still applies.
        title_gate_passed = ok and _has_title_criteria
        t["title_gate_passed"] = title_gate_passed
        if not title_gate_passed and (t.get("fit_score") or 0) < _EMPLOYEE_SCORE_THRESHOLD:
            stats["below_score"] += 1
            continue
        kept.append((t, sc))

    stats["kept_strict"] = len(kept)

    if collect_relaxable is not None:
        collect_relaxable.extend(relaxable_rejects)
        logger.info(f"[fast:{campaign_id}] {label} person-fit gate (strict-only): {stats}")
        return kept, stats

    import math as _math
    floor = int(_math.ceil(_GATE_RELAX_FRACTION * max(1, target_count)))
    if len(kept) < floor and relaxable_rejects:
        relaxable_rejects.sort(key=lambda pair: pair[0].get("fit_score", 0), reverse=True)
        need = floor - len(kept)
        readmitted = relaxable_rejects[:need]
        kept.extend(readmitted)
        stats["relaxed_in"] = len(readmitted)
        logger.warning(
            f"[fast:{campaign_id}] {label} gate relax: strict survivors "
            f"{stats['kept_strict']} < floor {floor} — re-admitted {len(readmitted)} "
            f"best-scored function/title-gate rejects (blocklist stays enforced)"
        )

    logger.info(f"[fast:{campaign_id}] {label} person-fit gate: {stats}")
    return kept, stats


# ──────────────────────────────────────────────────────────────────────────────
# Seniority / function broadening for recovery pass
# ──────────────────────────────────────────────────────────────────────────────

_SENIORITY_ORDERING = ["120", "210", "220", "300", "310", "320"]  # senior→manager→director→vp→csuite→founder

def _broaden_seniority_ids(ids: list[str]) -> list[str]:
    """Add 1 adjacent tier on each side of each selected ID.

    Empty input returns empty: no filter means the actor already scrapes
    unfiltered, and recovery must NOT silently relax targeting gates."""
    if not ids:
        return []
    result = set(ids)
    for sid in ids:
        if sid in _SENIORITY_ORDERING:
            idx = _SENIORITY_ORDERING.index(sid)
            if idx > 0:
                result.add(_SENIORITY_ORDERING[idx - 1])
            if idx < len(_SENIORITY_ORDERING) - 1:
                result.add(_SENIORITY_ORDERING[idx + 1])
    return sorted(result)


# LinkedIn standard function taxonomy (functionIds 1-26). Adjacency used only to
# broaden the recovery pass when a first-pass scrape comes back near-empty.
_FUNCTION_ADJACENCY: dict[str, list[str]] = {
    "8": ["19", "18"],    # engineering → product management, operations
    "19": ["8", "25"],    # product management → engineering, sales
    "25": ["15", "18"],   # sales → marketing, operations
    "15": ["25", "19"],   # marketing → sales, product management
    "10": ["18", "12"],   # finance → operations, hr
    "12": ["18", "10"],   # human resources → operations, finance
    "18": ["25", "10"],   # operations → sales, finance
    "4": ["25", "15"],    # business development → sales, marketing
    "13": ["8", "18"],    # information technology → engineering, operations
    "14": ["18", "6"],    # legal → operations, consulting
    "6": ["4", "18"],     # consulting → business development, operations
    "26": ["25", "18"],   # customer success and support → sales, operations
}

def _broaden_function_ids(ids: list[str]) -> list[str]:
    """Add 1 adjacent function for each selected ID.

    Empty input returns empty: no filter means the actor already scrapes
    unfiltered, and recovery must NOT silently relax targeting gates."""
    if not ids:
        return []
    result = set(ids)
    for fid in ids:
        for adj in _FUNCTION_ADJACENCY.get(fid, [])[:1]:
            result.add(adj)
    return sorted(result)


# ──────────────────────────────────────────────────────────────────────────────
# Prospect upsert
# ──────────────────────────────────────────────────────────────────────────────

async def _upsert_curated_prospect(
    prospect_doc: dict,
    campaign_oid: ObjectId,
    account_id: str,
) -> Optional[ObjectId]:
    li = prospect_doc.get("linkedin")
    email = prospect_doc.get("email")

    if li:
        query = {"linkedin": li}
    elif email:
        query = {"email": email}
    else:
        return None

    now = datetime.utcnow()
    # Ensure stage is contactable so this prospect is visible to search_prospects on next campaign.
    # transform_employee_to_prospect already sets "contactable", but guard other call paths too.
    prospect_doc.setdefault("stage", "contactable")
    # Strip ICP-relative / tenant-scoped fields from the shared prospect doc
    _skip = {"account_id", "is_smart_campaign", "source_industry_ids", "status", "tags",
              "ai_prospect_score", "prospect_score", "ai_assessment", "ai_score_breakdown",
              "prospect_intelligence", "outreach_messages", "priority_tier",
              "enrichment_status", "first_seen_at"}  # reserved for $setOnInsert
    update_set = {k: v for k, v in prospect_doc.items() if v is not None and k not in _skip}
    update_set["last_updated_at"] = now

    from pymongo.errors import DuplicateKeyError as MongoDupKeyError
    try:
        result = await database.prospects_collection.find_one_and_update(
            query,
            {
                "$set": update_set,
                "$setOnInsert": {"first_seen_at": now, "enrichment_status": "not_started"},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        prospect_oid = result["_id"] if result else None
    except MongoDupKeyError:
        fallback_query = {"email": email} if email and query.get("linkedin") else {"linkedin": li}
        result = await database.prospects_collection.find_one_and_update(
            fallback_query,
            {"$set": {"last_updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        prospect_oid = result["_id"] if result else None

    # Ensure prospect_state overlay exists for this tenant
    if prospect_oid and account_id:
        try:
            _aid = str(account_id)
            _pid = str(prospect_oid)
            from utils.prospect_filter_keys import build_filter_keys
            await database.prospect_state_collection.update_one(
                {"account_id": _aid, "prospect_id": _pid},
                {
                    # `result` is the post-upsert prospect doc — refresh the
                    # denormalized filter keys (routes/prospects.py list
                    # filters match on pk.*) on every pass so they track the
                    # prospect doc.
                    "$set": {"pk": build_filter_keys(result)},
                    "$setOnInsert": {
                        "account_id": _aid,
                        "prospect_id": _pid,
                        "status": "new",
                        "tags": [],
                        "used_by": [],
                        "created_at": now,
                        "last_updated_at": now,
                    },
                },
                upsert=True,
            )
        except Exception as _oe:
            logger.debug(f"prospect_state upsert failed: {_oe}")

    return prospect_oid


# ──────────────────────────────────────────────────────────────────────────────
# ICP prompt / ID mappings
# ──────────────────────────────────────────────────────────────────────────────

def _build_icp_prompt_from_campaign(campaign: dict) -> str:
    explicit = (campaign.get("curated_icp_prompt") or "").strip()
    if explicit:
        return explicit
    parts = []
    if campaign.get("icp_industries"):
        parts.append(f"Industries: {', '.join(campaign['icp_industries'])}")
    if campaign.get("icp_countries"):
        parts.append(f"Countries: {', '.join(campaign['icp_countries'])}")
    if campaign.get("icp_keywords"):
        parts.append(f"Keywords: {', '.join(campaign['icp_keywords'])}")
    if campaign.get("icp_company_size_min") or campaign.get("icp_company_size_max"):
        lo = campaign.get("icp_company_size_min") or "any"
        hi = campaign.get("icp_company_size_max") or "any"
        parts.append(f"Company size: {lo} to {hi} employees")
    if campaign.get("icp_funding_stages"):
        parts.append(f"Funding stages: {', '.join(campaign['icp_funding_stages'])}")
    return " | ".join(parts) or "B2B technology companies"


def _build_sender_context(company_profile: dict | None) -> str:
    if not company_profile:
        return ""
    parts = []
    if company_profile.get("company_name"):
        parts.append(company_profile["company_name"])
    if company_profile.get("value_proposition"):
        parts.append(company_profile["value_proposition"])
    if company_profile.get("target_customer_description"):
        parts.append(company_profile["target_customer_description"])
    return " | ".join(parts)


def _normalize_icp_label(label: str) -> str:
    """lowercase, strip punctuation, collapse spaces/hyphens to underscores."""
    import re
    s = (label or "").lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)      # strip punctuation except hyphens
    s = re.sub(r"[\s\-]+", "_", s).strip("_")   # "C-Level" → "c_level"
    return s


# Actor seniorityLevelIds: 120=Senior, 210=Manager, 220=Director, 300=VP,
# 310=CXO, 320=Owner/Partner (verified against the live actor schema).
_SENIORITY_LABEL_TO_ACTOR_ID: dict[str, list[str]] = {
    # C-suite / executive tier → CXO
    "c_suite": ["310"],
    "csuite": ["310"],
    "c_level": ["310"],
    "clevel": ["310"],
    "cxo": ["310"],
    "executive": ["310"],
    "exec": ["310"],
    "chief": ["310"],
    "president": ["320", "310"],
    # Founders / owners / partners
    "founder": ["320"],
    "co_founder": ["320"],
    "cofounder": ["320"],
    "owner": ["320"],
    "partner": ["320"],
    # VP tier
    "vp": ["300"],
    "vice_president": ["300"],
    "svp": ["300"],
    "evp": ["300"],
    # Director tier
    "director": ["220"],
    "head": ["220"],
    "head_of": ["220"],
    # Manager tier
    "manager": ["210"],
    # Senior IC tier
    "senior": ["120"],
    "lead": ["120"],
    "principal": ["120"],
}


def _icp_seniority_to_actor_ids(labels: list[str]) -> list[str]:
    out: set[str] = set()
    for label in labels:
        key = _normalize_icp_label(label)
        ids = _SENIORITY_LABEL_TO_ACTOR_ID.get(key)
        if not ids:
            # substring fallback for compound labels ("c_level_executives", "vp_of_sales")
            for lk, lids in _SENIORITY_LABEL_TO_ACTOR_ID.items():
                if lk and lk in key:
                    ids = lids
                    break
        if ids:
            out.update(ids)
        elif key:
            logger.warning(
                f"[icp_map] unmapped ICP seniority label {label!r} — "
                f"no actor seniorityLevelIds filter contributed for it"
            )
    return sorted(out)


# LinkedIn standard function taxonomy (harvestapi/linkedin-company-employees
# actor `functionIds`, enum 1-26). Verified against the live actor schema.
# Synonyms only map to IDs already verified above — never guess new IDs.
_FUNCTION_LABEL_TO_ACTOR_IDS: dict[str, list[str]] = {
    "engineering": ["8"],
    "eng": ["8"],
    "software": ["8"],
    "tech": ["8", "13"],
    "technology": ["8", "13"],
    "sales": ["25"],
    "revenue": ["25"],
    "marketing": ["15"],
    "growth": ["25", "15"],
    "gtm": ["25", "15"],
    "go_to_market": ["25", "15"],
    "revops": ["25", "18"],
    "revenue_operations": ["25", "18"],
    "product": ["19"],
    "product_management": ["19"],
    "finance": ["10"],
    "accounting": ["10"],
    "hr": ["12"],
    "human_resources": ["12"],
    "people": ["12"],
    "talent": ["12"],
    "recruiting": ["12"],
    "operations": ["18"],
    "ops": ["18"],
    "business_development": ["4"],
    "biz_dev": ["4"],
    "bd": ["4"],
    "partnerships": ["4"],
    "information_technology": ["13"],
    "it": ["13"],
    "legal": ["14"],
    "compliance": ["14"],
    "consulting": ["6"],
    "customer_success": ["26"],
    "customer_support": ["26"],
    "support": ["26"],
    "cs": ["26"],
}


def _icp_function_to_actor_ids(labels: list[str]) -> list[str]:
    out: set[str] = set()
    for label in labels:
        key = _normalize_icp_label(label)
        ids = _FUNCTION_LABEL_TO_ACTOR_IDS.get(key)
        if not ids:
            # substring fallback for compound labels ("sales_and_marketing")
            for lk, lids in _FUNCTION_LABEL_TO_ACTOR_IDS.items():
                if len(lk) >= 4 and lk in key:
                    ids = lids
                    break
        if ids:
            out.update(ids)
        elif key:
            logger.warning(
                f"[icp_map] unmapped ICP function label {label!r} — "
                f"no actor functionIds filter contributed for it"
            )
    return sorted(out)


# harvestapi/linkedin-company-employees actor `companyHeadcount` bands (verified
# against the live actor schema). Each tuple is (band_letter, min_size, max_size);
# max_size None means unbounded.
_COMPANY_HEADCOUNT_BANDS: list[tuple[str, int, Optional[int]]] = [
    ("A", 0, 0),          # Self-Employed
    ("B", 1, 10),
    ("C", 11, 50),
    ("D", 51, 200),
    ("E", 201, 500),
    ("F", 501, 1000),
    ("G", 1001, 5000),
    ("H", 5001, 10000),
    ("I", 10001, None),
]


def _icp_size_to_headcount_bands(size_min: int | None, size_max: int | None) -> list[str]:
    """Map an ICP company-size range to the actor's companyHeadcount bands
    (inclusive overlap). Returns [] when no size range is set (no filter)."""
    if size_min is None and size_max is None:
        return []
    lo = size_min if size_min is not None else 0
    hi = size_max  # None == unbounded
    bands: list[str] = []
    for letter, band_lo, band_hi in _COMPANY_HEADCOUNT_BANDS:
        band_hi_cmp = band_hi if band_hi is not None else float("inf")
        hi_cmp = hi if hi is not None else float("inf")
        if band_hi_cmp >= lo and band_lo <= hi_cmp:
            bands.append(letter)
    return bands


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _extract_company_url_from_employee(emp: dict) -> str | None:
    """Extract the company LinkedIn URL from an employee record returned by Apify.

    Priority order (highest reliability first):
    1. _meta.query.currentCompanies[0] — echoes the exact URL we passed to the actor (100% reliable)
    2. currentPositions[0].companyLinkedinUrl (plural — the actor's primary field in both
       Short and Full profileScraperMode)
    3. currentPosition[0].companyLinkedinUrl (singular — seen on some response variants)
    4. Top-level companyUrl / companyLinkedinUrl
    """
    # Primary: actor echoes the requested URL in _meta.query.currentCompanies[0]
    try:
        meta_companies = (emp.get("_meta") or {}).get("query", {}).get("currentCompanies") or []
        if meta_companies:
            url = meta_companies[0]
            if url:
                return _normalize_li_url(str(url))
    except Exception:
        pass

    # Secondary: currentPositions (plural) — the actor's standard field name
    positions = emp.get("currentPositions") or []
    if positions:
        url = positions[0].get("companyLinkedinUrl") or positions[0].get("companyUrl")
        if url:
            return _normalize_li_url(url)

    # Fallback: currentPosition (singular) — seen on some response variants
    legacy = emp.get("currentPosition") or []
    if legacy:
        url = legacy[0].get("companyLinkedinUrl") or legacy[0].get("companyUrl")
        if url:
            return _normalize_li_url(url)

    # Top-level fallback
    return (
        _normalize_li_url(emp.get("companyUrl"))
        or _normalize_li_url(emp.get("companyLinkedinUrl"))
    )


def _normalize_li_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = "https://" + url.lstrip("/")
    return url.lower()


def _canonical_company_li_url(raw: str | None) -> str | None:
    """Canonicalize any LinkedIn company URL to the exact form the prospect pool and
    the Apify employee actor expect: 'https://www.linkedin.com/company/<slug>'
    (lowercased, no scheme/www variance, no trailing slash, query stripped).

    Returns None for missing values or non-company URLs (e.g. '/search/results/...'),
    so junk company rows are dropped upstream instead of scraping nothing.
    """
    if not raw:
        return None
    u = raw.strip().lower()
    if "/company/" not in u:
        return None
    slug = u.split("/company/", 1)[1].split("/")[0].split("?")[0].strip()
    if not slug:
        return None
    return f"https://www.linkedin.com/company/{slug}"


def _find_closest_sc(company_url: str, url_to_sc: dict) -> dict | None:
    """Find a sourced company by partial URL match when exact key misses."""
    if not company_url:
        return None
    norm = company_url.lower().rstrip("/")
    for key, sc in url_to_sc.items():
        if key and norm in key.lower() or (key and key.lower() in norm):
            return sc
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-day approval: generate next day's messages on demand (D5)
# ──────────────────────────────────────────────────────────────────────────────

async def backfill_missing_intelligence(
    prospect_ids: list,
    campaign_id: str,
    account_id: str,
    label: str,
) -> int:
    """Run deep cohort enrichment for prospects that lack it, in place.

    ``run_enrichment_pipeline`` (Path B: company scrape + AI score) does NOT
    produce ``prospect_intelligence_base``, ``pitch``, person ``posts`` or reuse
    company deep-research — but the prospect-detail page and message
    personalization all expect them. This backfills that rich data for any of
    the given prospects still missing ``prospect_intelligence_base`` by running
    the curated cohort enrichment (Path A), reusing already-stored company
    research. Shared by the per-day approval flow and the durable day-enrichment
    worker. Returns the number of prospects enriched.
    """
    oids = [ObjectId(str(p)) for p in prospect_ids if ObjectId.is_valid(str(p))]
    if not oids:
        return 0
    need_intel = await database.prospects_collection.find(
        {
            "_id": {"$in": oids},
            "prospect_intelligence_base": {"$exists": False},
            # Exclude prospects that have already failed generation twice —
            # otherwise every day-approval/worker run re-pays for a fresh
            # Apify post scrape + AI call on a permanently-failing prospect.
            "$or": [
                {"intel_attempts": {"$exists": False}},
                {"intel_attempts": {"$lt": 2}},
            ],
        },
        {"_id": 1},
    ).to_list(length=len(oids))
    if not need_intel:
        return 0
    need_oids = [d["_id"] for d in need_intel]

    # Reload already-stored company research so enrichment reuses it instead of
    # re-running the (paid) deep-research pass.
    co_research_by_url: dict = {}
    enrolled_prospects = await database.prospects_collection.find(
        {"_id": {"$in": need_oids}},
        {"company_linkedin": 1},
    ).to_list(length=len(need_oids))
    co_urls = list({
        (p.get("company_linkedin") or "").rstrip("/")
        for p in enrolled_prospects
        if p.get("company_linkedin")
    })
    if co_urls:
        async for co_doc in database.companies_collection.find(
            {"linkedin_url": {"$in": co_urls}},
            {"linkedin_url": 1, "research": 1},
        ):
            url = (co_doc.get("linkedin_url") or "").rstrip("/")
            research = co_doc.get("research")
            if url and research:
                co_research_by_url[url.lower()] = research

    await _enrich_prospect_cohort(
        prospect_oids=need_oids,
        campaign_id=campaign_id,
        account_id=account_id,
        label=label,
        co_research_by_url=co_research_by_url,
    )
    return len(need_oids)


async def ensure_day_ready_then_generate(
    campaign_id: str,
    account_id: str,
    day: int,
) -> None:
    """
    Idempotent: generate messages for `day` of this campaign.

    Called by the approve-day endpoint after approving day N to pre-fill day N+1.
    Safe to call multiple times — `generate_messages_for_campaign` only generates
    'pending' / 'scheduled_later' enrollments, so already-generated days are a
    cheap no-op.

    Also guards against missing enrichment: if a day-N prospect somehow lacks
    `prospect_intelligence_base` (e.g. enrichment for that cohort failed earlier),
    runs a targeted enrichment pass before message generation.
    """
    from services.campaign_message_generator_service import generate_messages_for_campaign

    try:
        campaign_oid = ObjectId(campaign_id)

        # Check if this day has any enrollments
        enr_count = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "smart_campaign_send_day": day,
            "status": {"$nin": ["archived", "skipped_no_channel", "failed", "cascade_waiting"]},
        })
        if enr_count == 0:
            logger.info(f"[day_gen:{campaign_id}] day {day} has no eligible enrollments — no-op")
            return

        # Backfill deep enrichment (intelligence/pitch/posts/research) for any
        # day-N prospect that lacks it, before generating messages.
        enr_docs = await database.campaign_enrollments_collection.find(
            {
                "campaign_id": campaign_oid,
                "smart_campaign_send_day": day,
                "status": {"$nin": ["archived", "skipped_no_channel", "failed", "cascade_waiting"]},
                "message_gen_status": {"$in": [None, "pending", "scheduled_later"]},
            },
            {"prospect_id": 1},
        ).to_list(length=500)

        if enr_docs:
            try:
                n_enriched = await backfill_missing_intelligence(
                    prospect_ids=[e["prospect_id"] for e in enr_docs],
                    campaign_id=campaign_id,
                    account_id=account_id,
                    label=f"day{day}_on_demand",
                )
                if n_enriched:
                    logger.info(
                        f"[day_gen:{campaign_id}] day {day}: backfilled intelligence for "
                        f"{n_enriched} prospects before message gen"
                    )
            except Exception as _enr_e:
                logger.warning(
                    f"[day_gen:{campaign_id}] day {day} spot-enrichment failed (continuing): {_enr_e}"
                )

        # Generate messages for this day (idempotent — skips already-generated enrollments)
        logger.info(f"[day_gen:{campaign_id}] generating messages for day {day}")
        await generate_messages_for_campaign(campaign_id, account_id, send_day=day)

    except Exception as e:
        logger.warning(f"[day_gen:{campaign_id}] day {day} generation failed: {e}")
