"""
Prospect enrollment for smart campaigns.

Pre-enrolls prospects (provisional, engine-safe) as soon as they're scraped,
then promotes the top-N to active enrollment once scoring/enrichment finishes.
"""

import logging
from datetime import datetime

import database

logger = logging.getLogger(__name__)


async def _pre_enroll_prospects(campaign: dict, prospects: list[dict]) -> None:
    """
    Create provisional enrollment records immediately after scraping so that
    enrolled-prospects API returns them while enrichment is still running.
    status='enriching' is engine-safe (engine only runs on status='active').
    _enroll_prospects() promotes these to 'active' for top-N; the rest are deleted.
    """
    if not prospects:
        return

    campaign_oid = campaign["_id"]
    account_oid = campaign["account_id"]
    now = datetime.utcnow()

    prospect_oids = [p["_id"] for p in prospects]
    existing_cursor = database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
        {"prospect_id": 1},
    )
    already_enrolled = {doc["prospect_id"] async for doc in existing_cursor}

    # MW-3: Skip prospects actively enrolled in another campaign for this account
    cross_cursor = database.campaign_enrollments_collection.find(
        {
            "account_id": account_oid,
            "campaign_id": {"$ne": campaign_oid},
            "prospect_id": {"$in": prospect_oids},
            "status": {"$in": ["active", "enrolled"]},
        },
        {"prospect_id": 1},
    )
    cross_enrolled = {doc["prospect_id"] async for doc in cross_cursor}
    if cross_enrolled:
        logger.info(
            f"[_pre_enroll] Skipping {len(cross_enrolled)} prospects already active "
            f"in another campaign for account {account_oid}"
        )

    # Check for teammate conflicts: same account, DIFFERENT user, used_by within 90 days
    from datetime import timedelta
    _cooldown_cutoff = now - timedelta(days=90)
    _account_id_str = str(account_oid)

    # campaign.get("created_by") is the current user; fall back to account_id
    _current_user_id = str(campaign.get("created_by") or campaign.get("account_id") or account_oid)

    teammate_conflict_map: dict = {}  # prospect_id -> conflict metadata
    _p_id_strs = [str(p["_id"]) for p in prospects]

    async for state_doc in database.prospect_state_collection.find(
        {
            "account_id": _account_id_str,
            "prospect_id": {"$in": _p_id_strs},
            "used_by": {
                "$elemMatch": {
                    "user_id": {"$ne": _current_user_id},  # different user
                    "$or": [
                        {"status": {"$in": ["active", "paused", "enrolled", "scoring"]}},
                        {
                            "status": "completed",
                            "completed_at": {"$gte": _cooldown_cutoff},
                        },
                    ],
                }
            },
        },
        {"prospect_id": 1, "used_by": 1},
    ):
        _pid = str(state_doc.get("prospect_id", ""))
        # Find the conflicting used_by entry (teammate)
        for _entry in (state_doc.get("used_by") or []):
            if str(_entry.get("user_id", "")) != _current_user_id:
                teammate_conflict_map[_pid] = {
                    "teammate_user_id": str(_entry.get("user_id", "")),
                    "teammate_campaign_id": str(_entry.get("campaign_id", "")),
                    "teammate_status": _entry.get("status"),
                    "teammate_enrolled_at": _entry.get("enrolled_at"),
                }
                break

    if teammate_conflict_map:
        logger.info(
            f"[_pre_enroll] {len(teammate_conflict_map)} prospects have teammate conflicts "
            f"for campaign {campaign_oid}"
        )

    docs = []
    _pid_to_prospect: dict[str, dict] = {}
    for p in prospects:
        if p["_id"] in already_enrolled or p["_id"] in cross_enrolled:
            continue
        _pid_str = str(p["_id"])
        _pid_to_prospect[_pid_str] = p
        _conflict = teammate_conflict_map.get(_pid_str)
        _campaign_score = p.get("_campaign_fit_score")
        if _campaign_score is None:
            # Compatibility for callers not yet migrated to the explicit
            # campaign-state service. Do not use ``or``: zero is a real score.
            for _field in ("fit_score", "ai_prospect_score", "prospect_score"):
                if p.get(_field) is not None:
                    _campaign_score = p[_field]
                    break
        _doc = {
            "campaign_id": campaign_oid,
            "account_id": account_oid,
            "prospect_id": p["_id"],
            "status": "pending_teammate_review" if _conflict else "scoring",
            "current_step": 0,
            "next_action_at": None,
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
            "smart_campaign_channel": None,
            "smart_campaign_send_day": None,
            "smart_campaign_scheduled_utc": None,
            "generated_messages": None,
            "message_gen_status": "pending",
            "message_gen_error": None,
            "campaign_rule_score": float(_campaign_score) if _campaign_score is not None else None,
            # Channel eligibility. Defaults to "full" for AI-discovered prospects.
            # BYOL email-only leads carry "email_only" so message generation and
            # channel planning never route them to a LinkedIn/InMail node.
            "channel_eligibility": p.get("channel_eligibility") or "full",
        }
        # BYOL: link the enrollment back to its source spreadsheet row (for the
        # review "rows needing attention" panel). Absent for AI-discovered leads.
        if p.get("upload_row_index") is not None:
            _doc["upload_row_index"] = p["upload_row_index"]
        if _conflict:
            _doc["teammate_conflict"] = _conflict
        docs.append(_doc)

    if docs:
        try:
            result = await database.campaign_enrollments_collection.insert_many(docs, ordered=False)
            inserted = len(result.inserted_ids) if result and result.inserted_ids else len(docs)
            logger.info(f"Pre-enrolled {inserted}/{len(docs)} provisional prospects for campaign {campaign['_id']}")
        except Exception as e:
            from pymongo.errors import BulkWriteError
            if isinstance(e, BulkWriteError):
                inserted = e.details.get("nInserted", 0)
                n_errors = len(e.details.get("writeErrors", []))
                logger.error(
                    f"_pre_enroll_prospects partial insert: {inserted}/{len(docs)} inserted, "
                    f"{n_errors} errors for campaign {campaign['_id']}: {e}"
                )
            else:
                logger.error(f"_pre_enroll_prospects insert_many failed for campaign {campaign['_id']}: {e}", exc_info=True)
            await database.campaigns_collection.update_one(
                {"_id": campaign["_id"]},
                {"$set": {"discovery_partial_enroll_error": str(e)[:200]}},
            )

        # Sync used_by on prospect_state overlay (best-effort)
        try:
            _aid = str(account_oid)
            _cid = str(campaign_oid)
            _now = now
            state_ops = []
            from pymongo import UpdateOne as _SUO
            from utils.prospect_filter_keys import build_filter_keys as _build_pk
            for doc in docs:
                _pid = str(doc["prospect_id"])
                _set_fields = {
                    "last_updated_at": _now,
                }
                _p_doc = _pid_to_prospect.get(_pid)
                if _p_doc is not None:
                    # Denormalized filter keys for routes/prospects.py list
                    # filters (utils/prospect_filter_keys.py).
                    _set_fields["pk"] = _build_pk(_p_doc)
                state_ops.append(_SUO(
                    {"account_id": _aid, "prospect_id": _pid},
                    {
                        "$setOnInsert": {"account_id": _aid, "prospect_id": _pid, "status": "new", "tags": [], "created_at": _now},
                        # Campaign scoring is stored in campaign_prospect_state;
                        # this account overlay owns only account-wide fields.
                        "$set": _set_fields,
                        "$addToSet": {"used_by": {"user_id": _current_user_id, "campaign_id": _cid, "status": "scoring", "enrolled_at": _now}},
                    },
                    upsert=True,
                ))
            if state_ops:
                await database.prospect_state_collection.bulk_write(state_ops, ordered=False)
        except Exception as _se:
            logger.debug(f"_pre_enroll used_by sync failed: {_se}")


async def _enroll_prospects(campaign: dict, prospects: list[dict]) -> list[str]:
    """
    Enroll top N prospects into the campaign.
    Creates enrollment docs with next_action_at=None (set at approve-and-launch).
    Returns list of enrollment ID strings.
    """
    if not prospects:
        return []

    campaign_oid = campaign["_id"]
    account_oid = campaign["account_id"]
    now = datetime.utcnow()

    # Find existing enrollments for these prospects, noting their status
    prospect_oids = [p["_id"] for p in prospects]
    existing_cursor = database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
        {"prospect_id": 1, "status": 1},
    )
    existing_map = {doc["prospect_id"]: doc["status"] async for doc in existing_cursor}

    to_promote = []   # Pre-enrolled as "enriching" or "scoring" — promote to active
    to_insert = []    # Truly new — create fresh enrollment

    for prospect in prospects:
        pid = prospect["_id"]
        existing_status = existing_map.get(pid)
        if existing_status is None:
            to_insert.append(prospect)
        elif existing_status in ("enriching", "scoring"):
            to_promote.append(pid)
        # else already active/completed — skip

    # Promote provisional enrollments to active
    if to_promote:
        await database.campaign_enrollments_collection.update_many(
            {"campaign_id": campaign_oid, "prospect_id": {"$in": to_promote}, "status": {"$in": ["enriching", "scoring"]}},
            {"$set": {"status": "active"}},
        )

    # Create enrollment docs for prospects not yet enrolled at all
    docs = []
    for prospect in to_insert:
        docs.append({
            "campaign_id": campaign_oid,
            "account_id": account_oid,
            "prospect_id": prospect["_id"],
            "status": "active",
            "current_step": 0,
            "next_action_at": None,
            "step_history": [],
            "enrolled_at": now,
            "completed_at": None,
            "last_activity_at": None,
            "smart_campaign_channel": None,
            "smart_campaign_send_day": None,
            "smart_campaign_scheduled_utc": None,
            "generated_messages": None,
            "message_gen_status": "pending",
            "message_gen_error": None,
        })

    if docs:
        await database.campaign_enrollments_collection.insert_many(docs, ordered=False)

    newly_enrolled = len(to_promote) + len(docs)
    if newly_enrolled > 0:
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$inc": {"total_enrolled": newly_enrolled, "active_count": newly_enrolled}},
        )

    # Sync used_by on prospect_state for all enrolled prospects
    try:
        _aid = str(account_oid)
        _cid = str(campaign_oid)
        from pymongo import UpdateOne as _EUO
        from utils.prospect_filter_keys import build_filter_keys as _build_pk
        state_ops = []
        for p in prospects:
            _pid = str(p["_id"])
            state_ops.append(_EUO(
                {"account_id": _aid, "prospect_id": _pid},
                {
                    # Denormalized filter keys for routes/prospects.py list filters.
                    "$set": {"pk": _build_pk(p)},
                    "$setOnInsert": {"account_id": _aid, "prospect_id": _pid, "status": "new", "tags": [], "created_at": now, "last_updated_at": now},
                    "$addToSet": {"used_by": {"user_id": _aid, "campaign_id": _cid, "status": "active", "enrolled_at": now}},
                },
                upsert=True,
            ))
        if state_ops:
            await database.prospect_state_collection.bulk_write(state_ops, ordered=False)
    except Exception as _se:
        logger.debug(f"_enroll_prospects used_by sync failed: {_se}")

    # Return IDs for all selected prospects (promoted + newly inserted)
    if prospect_oids:
        cursor = database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid, "prospect_id": {"$in": prospect_oids}},
            {"_id": 1},
        )
        return [str(doc["_id"]) async for doc in cursor]
    return []
