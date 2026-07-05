"""
Polls Unipile for connection acceptance status.
When accepted: records timestamp on prospect + advances sequence enrollment state.

Strategy: Fetch recent connections from Unipile's /users/relations endpoint
and cross-reference against prospects with pending connection requests.
This is a single API call instead of N calls per prospect.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from database import prospects_collection, conversations_collection, campaign_enrollments_collection, campaigns_collection, campaign_daily_stats_collection
from bson import ObjectId
from services.unipile_service import UnipileClient, _extract_linkedin_username
from services.notification_service import create_notification

logger = logging.getLogger(__name__)

CHECK_INTERVAL_MINUTES = 5


async def check_pending_connections():
    """
    Batch check for accepted LinkedIn connections.

    1. Fetch all prospects with pending connection requests (sent but not accepted).
    2. Fetch recent connections from Unipile /users/relations (sorted by created_at desc).
    3. Cross-reference by LinkedIn username to find newly accepted connections.
    4. For each match: record acceptance, notify, schedule follow-up.
    """
    logger.info("Checking pending LinkedIn connections...")

    # Step 1: Get pending prospects
    pending = await prospects_collection.find({
        "connection_request_sent_at": {"$ne": None},
        "connection_accepted_at": None,
        "status": {"$in": ["contacted", "new", "replied"]},
    }).to_list(200)

    if not pending:
        logger.info("No pending connections to check")
        return {"checked": 0, "accepted": 0, "followups_queued": 0}

    logger.info(f"Checking {len(pending)} pending connections")

    # Build a lookup of linkedin username -> prospect
    pending_by_username: dict[str, dict] = {}
    for p in pending:
        linkedin_url = p.get("linkedin", "")
        if not linkedin_url:
            continue
        try:
            username = _extract_linkedin_username(linkedin_url)
            pending_by_username[username.lower()] = p
        except ValueError:
            continue

    if not pending_by_username:
        logger.info("No pending prospects have valid LinkedIn URLs")
        return {"checked": 0, "accepted": 0, "followups_queued": 0}

    # Step 2: Fetch recent connections from Unipile
    # We only need connections created after the earliest pending request
    unipile = UnipileClient()
    connected_usernames = await _fetch_recent_connections(unipile)

    # Step 3: Cross-reference
    accepted_count = 0
    followup_count = 0

    for username in connected_usernames:
        prospect = pending_by_username.get(username.lower())
        if not prospect:
            continue

        # This person accepted our connection request
        now = datetime.now(timezone.utc)
        prospect_id = str(prospect["_id"])

        # Atomic update: only set if connection_accepted_at is still None
        # This prevents duplicate processing from concurrent/rapid checks
        result = await prospects_collection.update_one(
            {"_id": prospect["_id"], "connection_accepted_at": None},
            {
                "$set": {"connection_accepted_at": now},
                "$push": {
                    "outreach_history": {
                        "event": "linkedin_connection_accepted",
                        "channel": "linkedin",
                        "timestamp": now,
                    }
                },
            },
        )

        if result.modified_count == 0:
            # Already processed by a concurrent check — skip
            logger.debug(f"Connection already recorded for {prospect_id}, skipping")
            del pending_by_username[username.lower()]
            continue

        accepted_count += 1

        # Update campaign counters for all active enrollments of this prospect
        try:
            enrollments = await campaign_enrollments_collection.find(
                {
                    "prospect_id": prospect_id,
                    "status": {"$nin": ["opted_out", "bounced", "completed"]},
                },
                {"campaign_id": 1, "account_id": 1},
            ).to_list(10)
            today_str = now.strftime("%Y-%m-%d")
            for enr in enrollments:
                camp_id_str = str(enr.get("campaign_id", ""))
                if not camp_id_str:
                    continue
                try:
                    camp_oid = ObjectId(camp_id_str)
                except Exception:
                    continue
                await campaigns_collection.update_one(
                    {"_id": camp_oid},
                    {"$inc": {"linkedin_connections_accepted": 1}},
                )
                await campaign_daily_stats_collection.update_one(
                    {"campaign_id": camp_id_str, "date": today_str},
                    {
                        "$inc": {"linkedin_connections_accepted": 1},
                        "$setOnInsert": {
                            "account_id": enr.get("account_id"),
                            "emails_sent": 0,
                            "emails_opened": 0,
                            "emails_replied": 0,
                            "linkedin_connections_sent": 0,
                        },
                    },
                    upsert=True,
                )
        except Exception as e:
            logger.warning(f"Failed to update campaign accepted counter for {prospect_id}: {e}")

        await create_notification(
            type="connection_accepted",
            title=f"{prospect.get('full_name', 'Prospect')} accepted your connection",
            body=f"{prospect.get('full_name')} at {prospect.get('company_name', '')} accepted your LinkedIn connection request.",
            prospect_id=prospect_id,
        )

        logger.info(f"Connection accepted: {prospect.get('full_name')} ({prospect_id})")

        # Update flow_state for flow-engine-based campaigns
        asyncio.create_task(
            _update_flow_state_on_connection_accepted(prospect, now)
        )
        followup_count += 1

        # Remove from lookup so we don't process again
        del pending_by_username[username.lower()]

    logger.info(f"Connection check complete: {accepted_count} accepted, {followup_count} sequence updates queued")
    return {
        "checked": len(pending),
        "accepted": accepted_count,
        "followups_queued": followup_count,
    }


async def _fetch_recent_connections(unipile: UnipileClient, max_pages: int = 3) -> set[str]:
    """
    Fetch recent connections from Unipile /users/relations endpoint.
    Returns a set of public_identifier (LinkedIn usernames) for recent connections.

    Fetches up to max_pages of results (most recent first).
    """
    usernames = set()
    account_id = await unipile.get_linkedin_account_id()
    cursor = None

    for page in range(max_pages):
        try:
            params = {"account_id": account_id, "limit": 100}
            if cursor:
                params["cursor"] = cursor

            data = await unipile._request("GET", "users/relations", params=params)
            items = data.get("items", [])

            for item in items:
                pub_id = item.get("public_identifier")
                if pub_id:
                    usernames.add(pub_id.lower())

            cursor = data.get("cursor")
            if not cursor or not items:
                break

            logger.debug(f"Fetched {len(items)} relations (page {page + 1}), total so far: {len(usernames)}")

        except Exception as e:
            logger.error(f"Error fetching relations page {page + 1}: {e}")
            break

    logger.info(f"Fetched {len(usernames)} recent connections from Unipile")
    return usernames


async def check_pending_linkedin_replies():
    """
    Backstop poll: detects inbound LinkedIn messages that may have been missed by the webhook.
    For each active flow-engine enrollment at a LinkedIn node, checks the prospect's conversation
    for new inbound messages since the last outbound send and transitions via the 'replied' event.

    Note: the primary reply handler is webhook_service.process_unipile_webhook (event=message_received).
    This function is a backstop only — it is safe to run frequently as it only acts when
    the webhook hasn't already set status='replied'.
    """
    from services import flow_engine as _flow_engine

    # Only look at enrollments still active (webhook already sets status='replied')
    waiting_enrollments = await campaign_enrollments_collection.find({
        "status": {"$in": ["active", "enrolled"]},
        "flow_state": {"$exists": True, "$ne": None},
    }).to_list(200)

    if not waiting_enrollments:
        return {"checked": 0, "replied": 0}

    # Batch-load campaigns that have a follow_up_flow
    campaign_ids = list({e.get("campaign_id") for e in waiting_enrollments if e.get("campaign_id")})
    campaigns_list = await campaigns_collection.find(
        {"_id": {"$in": [ObjectId(str(c)) if not isinstance(c, ObjectId) else c for c in campaign_ids]},
         "follow_up_flow": {"$exists": True, "$ne": None}}
    ).to_list(len(campaign_ids))
    campaigns_by_id = {c["_id"]: c for c in campaigns_list}

    now = datetime.now(timezone.utc)
    checked = 0
    replied_count = 0

    for enrollment in waiting_enrollments:
        try:
            flow_state = enrollment.get("flow_state")
            if not flow_state or _flow_engine.is_stopped(flow_state):
                continue

            camp_id = enrollment.get("campaign_id")
            campaign = campaigns_by_id.get(camp_id) or campaigns_by_id.get(
                ObjectId(str(camp_id)) if camp_id else None
            )
            if not campaign:
                continue

            flow = campaign.get("follow_up_flow")
            if not flow:
                continue

            current_node = _flow_engine.get_current_node(flow_state, flow)
            if not current_node:
                continue

            # Only act on LinkedIn channel nodes
            node_channel = current_node.get("channel", "")
            if "linkedin" not in node_channel:
                continue

            checked += 1

            # Determine when the last outbound action was sent (from flow history)
            history = flow_state.get("history", [])
            last_sent_at: Optional[datetime] = None
            for entry in reversed(history):
                if entry.get("event") == "sent":
                    try:
                        last_sent_at = datetime.fromisoformat(entry["at"])
                        if last_sent_at.tzinfo is None:
                            last_sent_at = last_sent_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        pass
                    break

            if not last_sent_at:
                continue

            # Look for inbound LinkedIn message after last_sent_at
            prospect_id = enrollment.get("prospect_id")
            account_id = enrollment.get("account_id")
            conversation = await conversations_collection.find_one({
                "$or": [
                    {"prospect_id": str(prospect_id), "account_id": account_id},
                    {"prospect_id": prospect_id, "account_id": account_id},
                ],
            })
            if not conversation:
                continue

            messages = conversation.get("messages", [])
            new_inbound = [
                m for m in messages
                if m.get("direction") == "inbound"
                and m.get("created_at") is not None
                and m["created_at"] > last_sent_at
            ]
            if not new_inbound:
                continue

            # Reply detected — fetch prospect and transition
            prospect = None
            if prospect_id:
                try:
                    prospect = await prospects_collection.find_one({"_id": ObjectId(str(prospect_id))})
                except Exception:
                    pass

            new_state = _flow_engine.transition(flow_state, flow, "replied", prospect or {})
            update: dict = {
                "flow_state": new_state,
                "status": "replied",
                "next_action_at": None,
            }
            if new_state.get("next_action_at"):
                try:
                    update["next_action_at"] = datetime.fromisoformat(new_state["next_action_at"])
                except Exception:
                    pass
            await campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": update},
            )
            logger.info(
                f"LinkedIn reply detected (backstop): enrollment {enrollment['_id']} "
                f"prospect {prospect_id}"
            )
            replied_count += 1

        except Exception as e:
            logger.warning(
                f"Error in check_pending_linkedin_replies for enrollment "
                f"{enrollment.get('_id')}: {e}"
            )

    return {"checked": checked, "replied": replied_count}


async def _update_flow_state_on_connection_accepted(prospect: dict, now: datetime):
    """Update flow_state on flow-engine-based campaign enrollments when a LinkedIn connection is accepted."""
    from services import flow_engine as _flow_engine
    prospect_id = str(prospect["_id"])
    try:
        # Find active smart campaign enrollments (non-sequence campaigns that use flow_state)
        enrollments = await campaign_enrollments_collection.find({
            "$or": [
                {"prospect_id": prospect_id, "status": {"$in": ["active", "enrolled"]}},
                {"prospect_id": prospect["_id"], "status": {"$in": ["active", "enrolled"]}},
            ],
            "flow_state": {"$exists": True, "$ne": None},
        }).to_list(10)

        for enrollment in enrollments:
            flow_state = enrollment.get("flow_state")
            if not flow_state or _flow_engine.is_stopped(flow_state):
                continue
            camp_id = enrollment.get("campaign_id")
            if not camp_id:
                continue
            campaign_doc = await campaigns_collection.find_one(
                {"_id": ObjectId(str(camp_id))},
                {"follow_up_flow": 1},
            )
            if not campaign_doc:
                continue
            flow = campaign_doc.get("follow_up_flow")
            if not flow:
                continue
            new_state = _flow_engine.transition(flow_state, flow, "accepted", prospect)
            update = {"flow_state": new_state}
            if new_state.get("next_action_at"):
                try:
                    update["next_action_at"] = datetime.fromisoformat(new_state["next_action_at"])
                except Exception:
                    pass
            await campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": update},
            )
            logger.info(
                f"Flow state updated (accepted) for enrollment {enrollment['_id']} "
                f"prospect {prospect_id}"
            )
    except Exception as e:
        logger.warning(f"Failed to update flow_state on connection accepted for {prospect_id}: {e}")
