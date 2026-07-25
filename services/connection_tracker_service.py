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
from database import prospects_collection, prospect_state_collection, conversations_collection, campaign_enrollments_collection, campaigns_collection, campaign_daily_stats_collection, linkedin_accounts_collection
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

    # Step 1: Get tenant-owned pending activity, then load canonical URL/name.
    pending_states = await prospect_state_collection.find({
        "connection_request_sent_at": {"$ne": None},
        "connection_accepted_at": None,
        "status": {"$in": ["contacted", "new", "replied"]},
    }).to_list(200)
    for state in pending_states:
        state["_has_overlay"] = True

    pending_enrollments = await campaign_enrollments_collection.find(
        {
            "linkedin_activity.connection_request_sent_at": {"$ne": None},
            "linkedin_activity.connection_accepted_at": None,
            "account_id": {"$exists": True, "$ne": None},
            "prospect_id": {"$exists": True, "$ne": None},
            "status": {"$nin": ["opted_out", "bounced", "completed"]},
        },
        {"account_id": 1, "prospect_id": 1},
    ).to_list(200)
    seen = {(str(state.get("account_id")), str(state.get("prospect_id"))) for state in pending_states}
    for enrollment in pending_enrollments:
        key = (str(enrollment.get("account_id")), str(enrollment.get("prospect_id")))
        if key not in seen:
            pending_states.append({
                "account_id": enrollment.get("account_id"),
                "prospect_id": enrollment.get("prospect_id"),
                "_has_overlay": False,
            })
            seen.add(key)

    if not pending_states:
        logger.info("No pending connections to check")
        return {"checked": 0, "accepted": 0, "followups_queued": 0}

    prospect_oids = [ObjectId(str(s["prospect_id"])) for s in pending_states if ObjectId.is_valid(str(s.get("prospect_id")))]
    canonical = await prospects_collection.find(
        {"_id": {"$in": prospect_oids}},
        {"full_name": 1, "company_name": 1, "linkedin": 1},
    ).to_list(len(prospect_oids))
    canonical_by_id = {str(doc["_id"]): doc for doc in canonical}
    logger.info(f"Checking {len(pending_states)} pending connections")

    # Build a lookup of linkedin username -> prospect
    pending_by_username: dict[tuple[str, str], tuple[dict, bool]] = {}
    for state in pending_states:
        p = canonical_by_id.get(str(state.get("prospect_id")))
        if not p or not state.get("account_id"):
            continue
        linkedin_url = p.get("linkedin", "")
        if not linkedin_url:
            continue
        try:
            username = _extract_linkedin_username(linkedin_url)
            pending_by_username[(str(state["account_id"]), username.lower())] = (
                p, bool(state.get("_has_overlay"))
            )
        except ValueError:
            continue

    if not pending_by_username:
        logger.info("No pending prospects have valid LinkedIn URLs")
        return {"checked": 0, "accepted": 0, "followups_queued": 0}

    # Step 2: Fetch separately for every explicitly tenant-owned provider
    # account. Never union an unbound workspace account into another tenant.
    linked_accounts = await linkedin_accounts_collection.find(
        {
            "unipile_status": "OK",
            "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
            "account_id": {"$exists": True, "$ne": None},
        },
        {"account_id": 1, "unipile_account_id": 1},
    ).to_list(500)
    connected_by_tenant: list[tuple[str, str]] = []
    for linked_account in linked_accounts:
        tenant_id = str(linked_account["account_id"])
        try:
            unipile = UnipileClient(account_id=str(linked_account["unipile_account_id"]))
            for username in await _fetch_recent_connections(unipile):
                connected_by_tenant.append((tenant_id, username))
        except Exception as e:
            logger.error(f"Failed to fetch recent connections for tenant {tenant_id}: {e}")
            continue

    # Step 3: Cross-reference
    accepted_count = 0
    followup_count = 0

    for tenant_id, username in connected_by_tenant:
        pending_match = pending_by_username.get((tenant_id, username.lower()))
        if not pending_match:
            continue
        prospect, has_overlay = pending_match

        tenant_values: list[object] = [tenant_id]
        try:
            tenant_values.append(ObjectId(tenant_id))
        except Exception:
            pass
        tenant_enrollment = await campaign_enrollments_collection.find_one(
            {
                "account_id": {"$in": tenant_values},
                "prospect_id": {"$in": [str(prospect["_id"]), prospect["_id"]]},
                "status": {"$nin": ["opted_out", "bounced", "completed"]},
            },
            {"_id": 1, "campaign_id": 1},
        )
        if not tenant_enrollment:
            continue

        # This person accepted our connection request
        now = datetime.now(timezone.utc)
        prospect_id = str(prospect["_id"])

        from services.prospect_activity_state_service import record_prospect_activity
        enrollment_recorded = await record_prospect_activity(
            account_id=tenant_id, prospect_id=prospect_id,
            enrollment_id=tenant_enrollment["_id"],
            fields={"connection_accepted_at": now},
            event={
                "event": "linkedin_connection_accepted",
                "channel": "linkedin", "timestamp": now,
            },
            only_if_missing="connection_accepted_at",
            write_overlay=False,
        )
        overlay_recorded = False
        if has_overlay:
            overlay_recorded = await record_prospect_activity(
                account_id=tenant_id, prospect_id=prospect_id,
                fields={"connection_accepted_at": now},
                event={
                    "event": "linkedin_connection_accepted",
                    "channel": "linkedin", "timestamp": now,
                },
                only_if_missing="connection_accepted_at",
            )
        recorded = overlay_recorded if has_overlay else enrollment_recorded

        if not recorded:
            # Already processed by a concurrent check — skip
            logger.debug(f"Connection already recorded for {prospect_id}, skipping")
            continue

        accepted_count += 1

        # Update campaign counters for all active enrollments of this prospect
        try:
            enrollments = await campaign_enrollments_collection.find(
                {
                    "account_id": {"$in": tenant_values},
                    "prospect_id": {"$in": [prospect_id, prospect["_id"]]},
                    "status": {"$nin": ["opted_out", "bounced", "completed"]},
                },
                {"campaign_id": 1, "account_id": 1},
            ).to_list(10)
            today_str = now.strftime("%Y-%m-%d")
            for enr in enrollments:
                await record_prospect_activity(
                    account_id=tenant_id, prospect_id=prospect_id,
                    enrollment_id=enr["_id"],
                    fields={"connection_accepted_at": now},
                    event={
                        "event": "linkedin_connection_accepted",
                        "channel": "linkedin", "timestamp": now,
                    },
                    only_if_missing="connection_accepted_at",
                    write_overlay=False,
                )
                # This enrollment was in the pending-connection-wait set we just
                # matched an acceptance for — pull its next touch forward so the
                # follow-up fires promptly instead of waiting for the next
                # scheduled next_action_at. Forward-only: never pushes it later.
                await campaign_enrollments_collection.update_one(
                    {
                        "_id": enr["_id"],
                        "account_id": {"$in": tenant_values},
                        "next_action_at": {"$gt": now},
                    },
                    {"$set": {"next_action_at": now}},
                )
                camp_id_str = str(enr.get("campaign_id", ""))
                if not camp_id_str:
                    continue
                try:
                    camp_oid = ObjectId(camp_id_str)
                except Exception:
                    continue
                await campaigns_collection.update_one(
                    {"_id": camp_oid, "account_id": {"$in": tenant_values}},
                    {"$inc": {"linkedin_connections_accepted": 1}},
                )
                await campaign_daily_stats_collection.update_one(
                    {
                        "campaign_id": camp_id_str,
                        "date": today_str,
                        "account_id": {"$in": tenant_values},
                    },
                    {
                        "$inc": {"linkedin_connections_accepted": 1},
                        "$setOnInsert": {
                            "account_id": enr.get("account_id"),
                            "emails_sent": 0,
                            "emails_replied": 0,
                        },
                    },
                    upsert=True,
                )
        except Exception as e:
            logger.warning(f"Failed to update campaign accepted counter for {prospect_id}: {e}")

        await create_notification(
            account_id=tenant_id,
            type="connection_accepted",
            title=f"{prospect.get('full_name', 'Prospect')} accepted your connection",
            body=f"{prospect.get('full_name')} at {prospect.get('company_name', '')} accepted your LinkedIn connection request.",
            prospect_id=prospect_id,
            campaign_id=(
                str(tenant_enrollment.get("campaign_id"))
                if tenant_enrollment.get("campaign_id")
                else None
            ),
            channel="linkedin",
        )

        logger.info(f"Connection accepted: {prospect.get('full_name')} ({prospect_id})")

        # Surface the acceptance in the inbox thread opened by the connection
        # request. Recorded as outbound deliberately: Message.direction has no
        # "system" value, and an inbound row would make _seq_detect_reply treat
        # the acceptance as a reply and stop the sequence.
        try:
            from services.conversation_service import record_outbound_linkedin_message
            _name = prospect.get("full_name") or "They"
            await record_outbound_linkedin_message(
                prospect_id=prospect_id,
                prospect_name=prospect.get("full_name"),
                prospect_company=prospect.get("company_name"),
                message_text=f"{_name} accepted your connection request.",
                outreach_type="connection_accepted",
                account_id=str(tenant_id),
            )
        except Exception as _conv_err:
            logger.warning(
                "Failed to record connection acceptance in conversation for %s: %s",
                prospect_id, _conv_err,
            )

        # Update flow_state for flow-engine-based campaigns
        asyncio.create_task(
            _update_flow_state_on_connection_accepted(prospect, now, tenant_id)
        )
        followup_count += 1

    logger.info(f"Connection check complete: {accepted_count} accepted, {followup_count} sequence updates queued")
    return {
        "checked": len(pending_states),
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


def _coerce_utc_timestamp(value) -> Optional[datetime]:
    """Normalize a message timestamp (datetime or ISO string) to tz-aware UTC."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


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
            account_values: list[object] = [str(account_id)]
            try:
                account_values.append(ObjectId(str(account_id)))
            except Exception:
                pass
            linked_account_id = campaign.get("linkedin_account_id")
            if not linked_account_id:
                continue
            try:
                linked_account_oid = ObjectId(str(linked_account_id))
            except Exception:
                continue
            linked_account = await linkedin_accounts_collection.find_one(
                {
                    "_id": linked_account_oid,
                    "account_id": {"$in": account_values},
                    "unipile_status": "OK",
                },
                {"unipile_account_id": 1},
            )
            if not linked_account or not linked_account.get("unipile_account_id"):
                continue
            conversation = await conversations_collection.find_one({
                "channel": "linkedin",
                "provider_account_id": str(linked_account["unipile_account_id"]),
                "$or": [
                    {"prospect_id": str(prospect_id), "account_id": {"$in": account_values}},
                    {"prospect_id": prospect_id, "account_id": {"$in": account_values}},
                ],
            })
            if not conversation:
                continue

            messages = conversation.get("messages", [])
            new_inbound = []
            for m in messages:
                if m.get("direction") != "inbound":
                    continue
                msg_ts = _coerce_utc_timestamp(m.get("timestamp"))
                if msg_ts is not None and msg_ts > last_sent_at:
                    new_inbound.append(m)
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


async def _update_flow_state_on_connection_accepted(
    prospect: dict, now: datetime, account_id: str
):
    """Update flow_state on flow-engine-based campaign enrollments when a LinkedIn connection is accepted."""
    from services import flow_engine as _flow_engine
    prospect_id = str(prospect["_id"])
    account_values: list[object] = [str(account_id)]
    try:
        account_values.append(ObjectId(str(account_id)))
    except Exception:
        pass
    try:
        # Find active smart campaign enrollments (non-sequence campaigns that use flow_state)
        enrollments = await campaign_enrollments_collection.find({
            "account_id": {"$in": account_values},
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
                {
                    "_id": ObjectId(str(camp_id)),
                    "account_id": {"$in": account_values},
                },
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
                {"_id": enrollment["_id"], "account_id": {"$in": account_values}},
                {"$set": update},
            )
            logger.info(
                f"Flow state updated (accepted) for enrollment {enrollment['_id']} "
                f"prospect {prospect_id}"
            )
    except Exception as e:
        logger.warning(f"Failed to update flow_state on connection accepted for {prospect_id}: {e}")
