"""
Campaign Engine - executes due enrollment steps every 5 minutes.
Finds enrollments where status="active" and next_action_at <= now.
Sends email or LinkedIn message, records the outcome, advances to next step.
"""

import asyncio
import logging
import os
import re
import socket
from datetime import datetime, timedelta
from bson import ObjectId
from typing import Optional

import database
from config import get_settings
from models.send_attempt import SendAttemptIdentity, SendAttemptState
from services.send_attempt_service import (
    SendAttemptService,
    claim_due_enrollment,
    release_enrollment_lease,
)
from services.unipile_service import UnipileClient
from utils.email_html import plain_text_to_html

logger = logging.getLogger(__name__)
settings = get_settings()

ENGINE_WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


def _send_attempt_identity(
    campaign: dict,
    enrollment: dict,
    channel: str,
) -> SendAttemptIdentity:
    """Return the stable identity shared by cap accounting and provider dispatch."""
    state = enrollment.get("sequence_state") or {}
    node_id = (
        state.get("current_node_id")
        or (enrollment.get("flow_state") or {}).get("current_node_id")
        or f"step-{enrollment.get('current_step', 0)}-{channel}"
    )
    return SendAttemptIdentity(
        account_id=str(enrollment.get("account_id", "")),
        enrollment_id=str(enrollment["_id"]),
        sequence_version=str(
            campaign.get("sequence_version") or campaign.get("version") or "1"
        ),
        node_id=str(node_id),
        generation=int(
            state.get("generation") or enrollment.get("send_generation") or 0
        ),
    )


async def _existing_send_attempt(
    campaign: dict,
    enrollment: dict,
    channel: str,
) -> Optional[dict]:
    identity = _send_attempt_identity(campaign, enrollment, channel)
    return await database.send_attempts_collection.find_one(
        {
            "send_key": identity.send_key,
            "account_id": identity.account_id,
            "enrollment_id": identity.enrollment_id,
        }
    )


def _attempt_owns_daily_cap(attempt: Optional[dict]) -> bool:
    """Whether an earlier execution already consumed this touch's cap slot.

    PREPARED is included because the cap is reserved immediately before the
    durable attempt is prepared. RETRY_SCHEDULED and FAILED_TERMINAL have had
    their slot released by the engine (or by explicit reconciliation).
    """
    return bool(
        attempt
        and attempt.get("state")
        in {
            SendAttemptState.PREPARED.value,
            SendAttemptState.DISPATCHING.value,
            SendAttemptState.AMBIGUOUS.value,
            SendAttemptState.SENT.value,
        }
    )


async def _release_cap_slots_once(
    send_key: Optional[str],
    campaign_id,
    channel: str,
    sender_id: Optional[str],
) -> None:
    """Release the campaign + sender daily-cap slots reserved for one send
    attempt, guarded so a replayed/re-leased failure never double-decrements.

    ``send_key`` identifies the durable send attempt; when it's unknown (the
    provider call raised before a send attempt record could be created) we
    fall back to a direct, unguarded release — there is no attempt doc to
    guard against, and that case was never double-released before this fix.
    """
    from services.daily_cap_service import release_slot, release_sender_slot as _release_sender_slot

    if send_key:
        service = SendAttemptService(database.send_attempts_collection)
        if not await service.mark_cap_released(send_key):
            return
    await release_slot(database.db, campaign_id, channel)
    if sender_id:
        await _release_sender_slot(database.db, sender_id, channel)


def get_unipile_service(account_id: Optional[str] = None) -> UnipileClient:
    """Compatibility factory that cannot construct an unbound provider client."""
    if not account_id:
        raise ValueError("Explicit tenant-owned Unipile account id is required")
    return UnipileClient(account_id=str(account_id))


def _account_values(account_id) -> list[object]:
    values: list[object] = [str(account_id)]
    try:
        values.append(ObjectId(str(account_id)))
    except Exception:
        pass
    return values


def personalize(template: str, prospect: dict, sender_context: dict = None) -> str:
    """Replace {{variable}} placeholders with prospect/sender data."""
    full_name = prospect.get("full_name", "")
    first_from_full = full_name.split()[0] if full_name else ""

    vars = {
        "firstName": prospect.get("first_name") or first_from_full,
        "lastName": prospect.get("last_name", ""),
        "fullName": full_name,
        "companyName": prospect.get("company_name", ""),
        "jobTitle": prospect.get("job_title", ""),
        "industry": prospect.get("industry", ""),
        "location": prospect.get("city") or prospect.get("country", ""),
        "headline": prospect.get("headline", ""),
    }
    if sender_context:
        vars.update({
            "senderName": sender_context.get("sender_name", ""),
            "senderCompany": sender_context.get("company_name", ""),
            "senderRole": sender_context.get("sender_role", ""),
            "bookingLink": sender_context.get("booking_link", ""),
        })
    return re.sub(r'\{\{(\w+)\}\}', lambda m: vars.get(m.group(1), ""), template)


def is_in_send_window(campaign: dict, prospect: Optional[dict] = None) -> bool:
    """Check if current time is within the campaign's send window (using prospect timezone if available)."""
    from datetime import timezone as tz
    import pytz
    try:
        if prospect and prospect.get("timezone"):
            target_tz_str = prospect["timezone"]
        else:
            target_tz_str = campaign.get("timezone", "America/New_York")
            
        target_tz = pytz.timezone(target_tz_str)
        now_local = datetime.now(tz.utc).astimezone(target_tz)
        hour = now_local.hour
        day_name = now_local.strftime("%A").lower()
        send_days = campaign.get("send_days", ["monday", "tuesday", "wednesday", "thursday", "friday"])
        send_start = campaign.get("send_hour_start", 9)
        send_end = campaign.get("send_hour_end", 17)
        return day_name in send_days and send_start <= hour < send_end
    except Exception as exc:
        # Invalid timezone/window configuration must fail closed. Sending at an
        # unintended local hour is a compliance and sender-reputation risk.
        logger.warning("Invalid campaign send window; deferring send: %s", exc)
        return False


def get_next_send_window(campaign: dict, prospect: Optional[dict] = None) -> datetime:
    """Get the next valid send window start time in UTC."""
    from datetime import timezone as tz
    import pytz
    try:
        if prospect and prospect.get("timezone"):
            target_tz_str = prospect["timezone"]
        else:
            target_tz_str = campaign.get("timezone", "America/New_York")
            
        target_tz = pytz.timezone(target_tz_str)
        now_local = datetime.now(tz.utc).astimezone(target_tz)
        send_days = campaign.get("send_days", ["monday", "tuesday", "wednesday", "thursday", "friday"])
        send_start = campaign.get("send_hour_start", 9)
        
        # Try today first if we are before send_start
        if now_local.strftime("%A").lower() in send_days and now_local.hour < send_start:
            next_window = now_local.replace(hour=send_start, minute=0, second=0, microsecond=0)
            return next_window.astimezone(tz.utc).replace(tzinfo=None)
            
        # Try next 7 days
        for delta in range(1, 8):
            candidate = now_local + timedelta(days=delta)
            if candidate.strftime("%A").lower() in send_days:
                # Return candidate at send_start hour in campaign timezone
                next_window = candidate.replace(hour=send_start, minute=0, second=0, microsecond=0)
                return next_window.astimezone(tz.utc).replace(tzinfo=None)
    except Exception:
        pass
    return datetime.utcnow() + timedelta(hours=24)


async def send_email_via_account(
    email_account: dict,
    to_email: str,
    subject: str,
    body: str,
    prospect_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Send email via the appropriate provider (Gmail API / Zoho Mail API / custom SMTP+IMAP).

    Thin wrapper over services.email_delivery_service.send_email — kept here for
    call-site compatibility across the campaign engine. Click-link rewriting happens
    inside the facade; open-pixel tracking has been removed (replies + clicks only).

    Returns a dict with at minimum {"message_id": str, "provider": str} on success,
    or None on failure. May also include {"thread_ref": str, "rfc_message_id": str}.
    """
    from services.email_delivery_service import send_email
    return await send_email(
        email_account, to_email, subject, body,
        prospect_id=prospect_id, campaign_id=campaign_id,
    )


async def record_campaign_message(
    enrollment: dict,
    campaign: dict,
    step_def: dict,
    content_text: str,
    subject: str,
    provider_message_id: Optional[str],
    status: str,
    provider_thread_id: Optional[str] = None,
    provider: Optional[str] = None,
    ab_variant: Optional[str] = None,
    rfc_message_id: Optional[str] = None,
    send_key: Optional[str] = None,
    prospect: Optional[dict] = None,
    provider_account_id: Optional[str] = None,
    content_html: Optional[str] = None,
):
    """
    Record a sent (or failed) message in campaign_messages.

    `content_html` should be the exact HTML the provider was handed (the delivery
    facade returns it). When it is missing — a failed send, or a non-email
    channel — we fall back to rendering the plain text, so the stored HTML always
    matches what a client would display rather than being raw text that renders
    as one collapsed paragraph.
    """
    if content_html is None:
        content_html = (
            plain_text_to_html(content_text)
            if step_def.get("channel") == "email"
            else content_text
        )
    doc = {
        "campaign_id": campaign["_id"],
        "campaign_enrollment_id": enrollment["_id"],
        "account_id": enrollment["account_id"],
        "prospect_id": enrollment["prospect_id"],
        "step_number": int(enrollment.get("current_step") or 0),
        "channel": step_def["channel"],
        "action": step_def["action"],
        "direction": "outbound",
        "subject": subject,
        "content_text": content_text,
        "content_html": content_html,
        "provider_message_id": provider_message_id,
        "email_account_id": campaign.get("email_account_id"),
        "status": status,
        "scheduled_at": datetime.utcnow(),
        "sent_at": datetime.utcnow() if status == "sent" else None,
        "created_at": datetime.utcnow(),
    }
    if provider_thread_id:
        doc["provider_thread_id"] = provider_thread_id
    if provider:
        doc["provider"] = provider
    if ab_variant:
        doc["ab_variant"] = ab_variant
    if rfc_message_id:
        doc["rfc_message_id"] = rfc_message_id
    if send_key:
        doc["send_key"] = send_key
        write_result = await database.campaign_messages_collection.update_one(
            {"send_key": send_key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        inserted = write_result.upserted_id is not None
        transitioned_to_sent = False
        if not inserted and status == "sent":
            sent_fields = {
                "status": "sent",
                "provider_message_id": provider_message_id,
                "sent_at": datetime.utcnow(),
            }
            if provider_thread_id:
                sent_fields["provider_thread_id"] = provider_thread_id
            if provider:
                sent_fields["provider"] = provider
            if rfc_message_id:
                sent_fields["rfc_message_id"] = rfc_message_id
            transition = await database.campaign_messages_collection.update_one(
                {"send_key": send_key, "status": {"$ne": "sent"}},
                {"$set": sent_fields},
            )
            transitioned_to_sent = transition.modified_count > 0
    else:
        await database.campaign_messages_collection.insert_one(doc)
        inserted = True
        transitioned_to_sent = False

    # Update campaign counter
    counter_map = {
        "email": "emails_sent",
        "connection_request": "linkedin_connections_sent",
        "inmail": "linkedin_inmails_sent",
        "linkedin_message": "linkedin_messages_sent",
    }
    counter = counter_map.get(step_def["action"])
    if (inserted or transitioned_to_sent) and counter and status == "sent":
        await database.campaigns_collection.update_one(
            {
                "_id": campaign["_id"],
                "account_id": {"$in": _account_values(enrollment.get("account_id"))},
            },
            {"$inc": {counter: 1}},
        )

    # A LinkedIn DM/InMail send never created a conversations doc, so the reply
    # webhook had nothing to match provider_thread_id against. Connection
    # requests have no chat yet at send time, so they open a placeholder thread
    # keyed on ``prospect:<id>``; get_or_create_conversation adopts that
    # placeholder once the real chat id turns up. Without this the inbox never
    # shows campaign-sent connection requests at all, while the manual send
    # path in routes/linkedin_outreach.py does create them.
    if (
        status == "sent"
        and step_def.get("channel") == "linkedin"
        and (provider_thread_id or step_def.get("action") == "connection_request")
    ):
        try:
            from services.conversation_service import record_outbound_linkedin_message
            await record_outbound_linkedin_message(
                prospect_id=str(enrollment["prospect_id"]),
                prospect_name=(prospect or {}).get("full_name"),
                prospect_company=(prospect or {}).get("company_name"),
                message_text=content_text,
                unipile_chat_id=provider_thread_id,
                unipile_message_id=provider_message_id,
                outreach_type=step_def.get("action"),
                account_id=str(enrollment.get("account_id", "")),
                provider_account_id=provider_account_id,
            )
        except Exception as _conv_e:
            logger.warning(
                "Failed to record conversation for LinkedIn send (enrollment %s): %s",
                enrollment.get("_id"), _conv_e,
            )

    # Campaign emails were never recorded as conversations either, so a sent
    # email stayed invisible in the inbox until the prospect replied and the
    # poller created the thread. Record it here so the outbound message is
    # already in the thread the reply will land on.
    if status == "sent" and step_def.get("channel") == "email":
        try:
            from services.conversation_service import record_outbound_email
            await record_outbound_email(
                prospect_id=str(enrollment["prospect_id"]),
                prospect_name=(prospect or {}).get("full_name"),
                prospect_email=(prospect or {}).get("email"),
                prospect_company=(prospect or {}).get("company_name"),
                subject=subject,
                body_text=content_text,
                provider=provider,
                provider_message_id=provider_message_id,
                email_message_id=rfc_message_id,
                account_id=str(enrollment.get("account_id", "")),
                provider_account_id=provider_account_id,
                provider_thread_id=provider_thread_id,
            )
        except Exception as _conv_e:
            logger.warning(
                "Failed to record conversation for email send (enrollment %s): %s",
                enrollment.get("_id"), _conv_e,
            )


async def advance_enrollment(enrollment: dict, campaign: dict):
    """Move enrollment to next step or mark as completed."""
    next_step = enrollment["current_step"] + 1
    now = datetime.utcnow()

    if next_step >= len(campaign.get("steps", [])):
        # All steps done — mark completed
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {
                "status": "completed",
                "completed_at": now,
                "last_activity_at": now,
            }},
        )
        await database.campaigns_collection.update_one(
            {"_id": campaign["_id"]},
            {"$inc": {"completed_count": 1, "active_count": -1}},
        )
        # Sync used_by lifecycle on prospect_state overlay
        try:
            from services.prospect_search_service import update_used_by_status
            await update_used_by_status(
                database.db,
                account_id=str(enrollment.get("account_id", "")),
                prospect_id=str(enrollment.get("prospect_id", "")),
                campaign_id=str(enrollment.get("campaign_id", "")),
                new_status="completed",
                completed_at=now,
            )
        except Exception as _sync_e:
            logger.warning("used_by sync failed for enrollment %s: %s", enrollment.get("_id"), _sync_e)
    else:
        step_def = campaign["steps"][next_step]
        delay_days = step_def.get("delay_days", 0)
        next_action_at = now + timedelta(days=delay_days)

        # Add step history entry for the step we just finished
        completed_step = campaign["steps"][enrollment["current_step"]]
        history_entry = {
            "step_number": enrollment["current_step"],
            "executed_at": now,
            "channel": completed_step["channel"],
            "action": completed_step["action"],
            "status": "sent",
        }

        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {
                "$set": {
                    "current_step": next_step,
                    "next_action_at": next_action_at,
                    "last_activity_at": now,
                },
                "$push": {"step_history": history_entry},
            },
        )


async def _generate_step_message_bg(
    campaign: dict,
    enrollment_id: str,
    next_node_id: str,
    flow: dict,
    prospect: dict,
    sent_subject: str,
    sent_body: str,
    sent_channel: str,
) -> None:
    """
    Background helper: generate the message for the next flow node after a successful send.
    Builds prior-step context from the just-sent message and writes to generated_messages_by_step.
    """
    try:
        from services.campaign_message_generator_service import generate_message_for_node
        enrollment = await database.campaign_enrollments_collection.find_one(
            {"_id": ObjectId(enrollment_id)}
        )
        if not enrollment:
            logger.warning(f"_generate_step_message_bg: enrollment {enrollment_id} not found")
            return

        # Find the next node definition in the flow
        nodes = flow.get("nodes", [])
        next_node = next((n for n in nodes if n.get("id") == next_node_id), None)
        if not next_node:
            logger.warning(f"_generate_step_message_bg: node {next_node_id} not in flow")
            return

        # Build prior-step context: existing generated_messages_by_step + the just-sent message
        existing_by_step: dict = enrollment.get("generated_messages_by_step", {})
        prior_step_messages = []
        for n in nodes:
            nid = n.get("id", "")
            if nid == next_node_id:
                break
            step_msg = existing_by_step.get(nid)
            if step_msg:
                prior_step_messages.append({
                    "channel": step_msg.get("channel", n.get("channel", "")),
                    "subject": step_msg.get("subject", ""),
                    "body_excerpt": (step_msg.get("body") or "")[:150],
                })
            elif nid == enrollment.get("flow_state", {}).get("history", [{}])[-1:][0].get("node_id"):
                # Fallback: use the just-sent data
                prior_step_messages.append({
                    "channel": sent_channel,
                    "subject": sent_subject,
                    "body_excerpt": sent_body[:150],
                })

        # If prior list is empty but we did send something, add the just-sent message
        if not prior_step_messages and (sent_subject or sent_body):
            prior_step_messages.append({
                "channel": sent_channel,
                "subject": sent_subject,
                "body_excerpt": sent_body[:150],
            })

        await generate_message_for_node(
            campaign=campaign,
            enrollment=enrollment,
            prospect=prospect,
            node=next_node,
            prior_step_messages=prior_step_messages,
        )
    except Exception as e:
        logger.error(
            f"_generate_step_message_bg failed for enrollment {enrollment_id} node {next_node_id}: {e}",
            exc_info=True,
        )


async def _execute_smart_enrollment(
    enrollment: dict,
    campaign: dict,
    prospect: dict,
    channel: str,
):
    """
    Execute outreach for a smart campaign enrollment using pre-generated messages.
    channel: "email" | "linkedin_connection" | "linkedin_inmail"
    """
    from services.daily_cap_service import reserve_slot, release_slot, reserve_sender_slot
    from services import flow_engine

    # Short-circuit for terminal prospect/enrollment states
    # Read status from prospect_state overlay (moved from prospect doc in DB rearchitecture)
    _overlay_status = None
    try:
        _state_doc = await database.prospect_state_collection.find_one(
            {
                "account_id": str(enrollment.get("account_id", "")),
                "prospect_id": str(enrollment.get("prospect_id", "")),
            },
            {"status": 1},
        )
        if _state_doc:
            _overlay_status = _state_doc.get("status")
    except Exception as _e:
        logger.debug("prospect_state lookup failed, falling back to prospect doc: %s", _e)

    # Fallback to prospect doc status if overlay not found
    _effective_status = _overlay_status or prospect.get("status") or ""

    enrollment_status_check = enrollment.get("status", "")
    if _effective_status in ("opted_out", "bounced", "disqualified") or enrollment_status_check == "archived":
        logger.info(
            f"Skipping smart enrollment {enrollment['_id']}: "
            f"prospect_status={_effective_status!r} enrollment_status={enrollment_status_check!r}"
        )
        return {"status": "skipped", "reason": "terminal_state"}

    # Suppression check — skip and mark opted_out if prospect is suppressed
    account_id_str = str(enrollment.get("account_id", ""))
    try:
        from services.suppression_service import is_suppressed
        if await is_suppressed(account_id_str, prospect):
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"status": "opted_out", "last_transition_reason": "suppressed"}},
            )
            logger.info(f"Suppressed prospect {prospect.get('_id')} — enrollment {enrollment['_id']} opted_out")
            return {"status": "skipped", "reason": "suppressed"}
    except Exception as _sup_err:
        logger.warning(f"Suppression check failed for enrollment {enrollment['_id']}: {_sup_err}")

    # Determine channel from flow_state if available (may override passed channel)
    flow_state = enrollment.get("flow_state") or {}
    flow = campaign.get("follow_up_flow")
    if flow and flow_state:
        current_node = flow_engine.get_current_node(flow_state, flow)
        if current_node:
            channel = current_node.get("channel", channel)

    # A provider-confirmed replay finalizes local state without requiring the
    # sender to still be connected. A new/retry dispatch must resolve the exact
    # tenant-owned sender before reserving that mailbox's cap.
    existing_attempt = await _existing_send_attempt(campaign, enrollment, channel)
    cap_already_reserved = _attempt_owns_daily_cap(existing_attempt)
    resolved_sender = None
    if channel == "email":
        resolved_sender = await _get_email_account_for_campaign(campaign, enrollment)
    elif channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message"):
        resolved_sender = await _get_linkedin_account_for_campaign(campaign, enrollment)
    channel_available = bool(resolved_sender) or bool(
        existing_attempt
        and existing_attempt.get("state") == SendAttemptState.SENT.value
    )

    if not channel_available:
        skip_count = enrollment.get("consecutive_channel_skip_count", 0) + 1
        if skip_count >= 3:
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"status": "skipped_no_channel", "consecutive_channel_skip_count": skip_count}},
            )
            logger.warning(f"Smart enrollment {enrollment['_id']} marked skipped_no_channel after {skip_count} unavailable-sender skips")
            return {"status": "skipped", "reason": "sender_unavailable"}
        from datetime import timezone, timedelta
        defer_to = datetime.now(timezone.utc) + timedelta(hours=1)
        defer_to = defer_to.replace(tzinfo=None)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {"next_action_at": defer_to, "consecutive_channel_skip_count": skip_count}},
        )
        logger.info(f"Smart enrollment {enrollment['_id']} deferred 1h — sender channel {channel!r} unavailable")
        return {"status": "deferred", "reason": "sender_unavailable"}

    # Reserve each touch once. Replays of a prepared/in-flight/ambiguous/sent
    # attempt use the original reservation and never double-count capacity.
    slot_reserved = cap_already_reserved or await reserve_slot(
        database.db, enrollment["campaign_id"], channel
    )
    if not slot_reserved:
        # Defer to tomorrow at 9am UTC
        from datetime import timezone, timedelta
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=None)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {"next_action_at": tomorrow}},
        )
        logger.info(f"Smart enrollment {enrollment['_id']} deferred to tomorrow (daily cap hit for {channel})")
        return {"status": "deferred", "reason": "daily_cap_hit"}

    # Also enforce sender-level daily cap (prevents multiple campaigns from
    # collectively overrunning a single LinkedIn/email account's daily limit)
    _sender_id = str(
        (existing_attempt or {}).get("sender_record_id")
        or (resolved_sender or {}).get("_id")
        or ""
    )
    if _sender_id and not cap_already_reserved:
        sender_slot_reserved = await reserve_sender_slot(
            database.db, _sender_id, channel, sender=resolved_sender
        )
        if not sender_slot_reserved:
            await release_slot(database.db, enrollment["campaign_id"], channel)
            from services.daily_cap_service import get_sender_defer_until
            tomorrow = await get_sender_defer_until(database.db, _sender_id, channel)
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"next_action_at": tomorrow}},
            )
            logger.info(f"Smart enrollment {enrollment['_id']} deferred to tomorrow (sender {_sender_id} cap hit for {channel})")
            return {"status": "deferred", "reason": "sender_cap_hit"}

    node_id = flow_state.get("current_node_id", "") if flow_state else ""
    msgs = enrollment.get("generated_messages", {})
    # Prefer generated_messages_by_step[node_id] (new), then message_drafts[node_id] (legacy)
    if node_id:
        step_msg = enrollment.get("generated_messages_by_step", {}).get(node_id)
        if step_msg:
            if channel == "email":
                msgs = {"cold_email": {"subject_a": step_msg.get("subject", ""), "body": step_msg.get("body", "")}}
            elif channel in ("linkedin_connection", "linkedin_message"):
                msgs = {"linkedin_connection": {"note": step_msg.get("body", "")}}
            elif channel == "linkedin_inmail":
                msgs = {"linkedin_inmail": {"subject": step_msg.get("subject", ""), "body": step_msg.get("body", "")}}
        elif enrollment.get("message_drafts", {}).get(node_id):
            draft = enrollment["message_drafts"][node_id]
            if channel == "email":
                msgs = {"cold_email": {"subject_a": draft.get("subject", ""), "body": draft.get("body", "")}}
            elif channel in ("linkedin_connection", "linkedin_message"):
                msgs = {"linkedin_connection": {"note": draft.get("body", "")}}
            elif channel == "linkedin_inmail":
                msgs = {"linkedin_inmail": {"subject": draft.get("subject", ""), "body": draft.get("body", "")}}

    now = datetime.utcnow()
    provider_message_id = None
    status = "failed"
    subject = ""
    body = ""

    try:
        if channel == "email":
            cold_email = msgs.get("cold_email", {})
            import random as _random
            subject_a = cold_email.get("subject_a", "")
            subject_b = cold_email.get("subject_b", "")
            if subject_a and subject_b:
                _ab_variant = _random.choice(["A", "B"])
                subject = subject_a if _ab_variant == "A" else subject_b
            else:
                _ab_variant = "A"
                subject = subject_a or subject_b
            body = cold_email.get("body", "")
        elif channel in ("linkedin_connection", "linkedin_message"):
            body = msgs.get("linkedin_connection", {}).get("note", "")
        elif channel == "linkedin_inmail":
            inmail = msgs.get("linkedin_inmail", {})
            subject = inmail.get("subject", "")
            body = inmail.get("body", "")

        status, provider_message_id, send_result = await _send_via_channel(
            campaign, enrollment, prospect, channel, subject, body,
        )
        subject = send_result.get("frozen_subject", subject)
        body = send_result.get("frozen_body", body)

    except Exception as e:
        logger.error(
            f"Smart enrollment send error for enrollment {enrollment['_id']} (channel={channel}): {e}",
            exc_info=True,
        )
        status = "failed"

    # Build synthetic step_def for record_campaign_message compatibility
    _channel_to_action = {
        "email": "email",
        "linkedin_connection": "connection_request",
        "linkedin_inmail": "inmail",
        "linkedin_message": "linkedin_message",
    }
    _channel_to_msg_channel = {
        "email": "email",
        "linkedin_connection": "linkedin",
        "linkedin_inmail": "linkedin",
        "linkedin_message": "linkedin",
    }
    step_def = {
        "channel": _channel_to_msg_channel.get(channel, "email"),
        "action": _channel_to_action.get(channel, "email"),
    }

    _send_result = locals().get("send_result") or {}
    await record_campaign_message(
        enrollment, campaign, step_def, body, subject, provider_message_id, status,
        provider_thread_id=_send_result.get("thread_ref"),
        provider=_send_result.get("provider"),
        rfc_message_id=_send_result.get("rfc_message_id"),
        ab_variant=locals().get("_ab_variant"),
        send_key=_send_result.get("send_key"),
        prospect=prospect,
        provider_account_id=_send_result.get("provider_account_id"),
        content_html=_send_result.get("content_html"),
    )

    if status == "ambiguous":
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {
                "status": "send_ambiguous",
                "next_action_at": None,
                "ambiguous_send_key": _send_result.get("send_key"),
                "last_transition_reason": "provider_outcome_requires_reconciliation",
                "last_activity_at": now,
            }},
        )
        return {"status": "ambiguous", "send_key": _send_result.get("send_key")}

    if status == "sent":
        # Use flow_engine to transition to next state if flow is configured
        if flow and flow_state and not flow_engine.is_stopped(flow_state):
            _prospect_for_flow = await database.prospects_collection.find_one({"_id": enrollment["prospect_id"]})
            new_flow_state = flow_engine.transition(flow_state, flow, "sent", _prospect_for_flow or {})
            next_action_at = None
            if new_flow_state.get("next_action_at"):
                try:
                    from services.campaign_launch_service import clamp_to_send_window
                    next_action_at = datetime.fromisoformat(new_flow_state["next_action_at"])
                    next_action_at = clamp_to_send_window(next_action_at, campaign)
                except Exception:
                    pass
            update_fields = {
                "flow_state": new_flow_state,
                "last_activity_at": now,
                "current_step": 1,
            }
            if flow_engine.is_stopped(new_flow_state):
                update_fields["status"] = "completed"
                update_fields["completed_at"] = now
            elif next_action_at:
                update_fields["next_action_at"] = next_action_at
                update_fields["status"] = "active"
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": update_fields},
            )
            if flow_engine.is_stopped(new_flow_state):
                await database.campaigns_collection.update_one(
                    {"_id": campaign["_id"]},
                    {"$inc": {"completed_count": 1, "active_count": -1}},
                )
                # Sync used_by lifecycle on prospect_state overlay
                try:
                    from services.prospect_search_service import update_used_by_status
                    await update_used_by_status(
                        database.db,
                        account_id=str(enrollment.get("account_id", "")),
                        prospect_id=str(enrollment.get("prospect_id", "")),
                        campaign_id=str(enrollment.get("campaign_id", "")),
                        new_status="completed",
                        completed_at=now,
                    )
                except Exception as _sync_e:
                    logger.warning("used_by sync failed for enrollment %s: %s", enrollment.get("_id"), _sync_e)
            else:
                # Lazy-generate message for the next flow step in background
                next_node_id = new_flow_state.get("current_node_id")
                if next_node_id and next_node_id != "STOP":
                    asyncio.create_task(
                        _generate_step_message_bg(
                            campaign=campaign,
                            enrollment_id=str(enrollment["_id"]),
                            next_node_id=next_node_id,
                            flow=flow,
                            prospect=_prospect_for_flow or prospect,
                            sent_subject=subject,
                            sent_body=body,
                            sent_channel=channel,
                        )
                    )
        else:
            # No flow configured — classic v1 behaviour: complete after single send
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {
                    "status": "completed",
                    "completed_at": now,
                    "last_activity_at": now,
                    "current_step": 1,
                }},
            )
            await database.campaigns_collection.update_one(
                {"_id": campaign["_id"]},
                {"$inc": {"completed_count": 1, "active_count": -1}},
            )
            # Sync used_by lifecycle on prospect_state overlay
            try:
                from services.prospect_search_service import update_used_by_status
                await update_used_by_status(
                    database.db,
                    account_id=str(enrollment.get("account_id", "")),
                    prospect_id=str(enrollment.get("prospect_id", "")),
                    campaign_id=str(enrollment.get("campaign_id", "")),
                    new_status="completed",
                    completed_at=now,
                )
            except Exception as _sync_e:
                logger.warning("used_by sync failed for enrollment %s: %s", enrollment.get("_id"), _sync_e)
    else:
        # On failure: release cap slot (once per attempt) and transition flow state via send_failed
        await _release_cap_slots_once(_send_result.get("send_key"), enrollment["campaign_id"], channel, _sender_id)
        if flow and flow_state and not flow_engine.is_stopped(flow_state):
            _prospect_for_flow = await database.prospects_collection.find_one({"_id": enrollment["prospect_id"]})
            new_flow_state = flow_engine.transition(flow_state, flow, "send_failed", _prospect_for_flow or {})
            next_action_at = None
            if new_flow_state.get("next_action_at"):
                try:
                    next_action_at = datetime.fromisoformat(new_flow_state["next_action_at"])
                except Exception:
                    pass
            update_fields = {
                "flow_state": new_flow_state,
                "last_activity_at": now,
            }
            if flow_engine.is_stopped(new_flow_state):
                update_fields["status"] = "completed"
                update_fields["completed_at"] = now
            elif next_action_at:
                update_fields["next_action_at"] = next_action_at
            else:
                # Cap consecutive failures: after 5 attempts, mark terminal to avoid infinite hourly retries
                _fails = int(enrollment.get("consecutive_failures") or 0) + 1
                if _fails >= 5:
                    update_fields["next_action_at"] = None
                    update_fields["status"] = "failed"
                    update_fields["last_transition_reason"] = "max_send_retries"
                else:
                    update_fields["next_action_at"] = now + timedelta(hours=1)
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": update_fields, "$inc": {"consecutive_failures": 1}},
            )
            if flow_engine.is_stopped(new_flow_state):
                # Sync used_by lifecycle on prospect_state overlay
                try:
                    from services.prospect_search_service import update_used_by_status
                    await update_used_by_status(
                        database.db,
                        account_id=str(enrollment.get("account_id", "")),
                        prospect_id=str(enrollment.get("prospect_id", "")),
                        campaign_id=str(enrollment.get("campaign_id", "")),
                        new_status="completed",
                        completed_at=now,
                    )
                except Exception as _sync_e:
                    logger.warning("used_by sync failed for enrollment %s: %s", enrollment.get("_id"), _sync_e)
        else:
            _fails = int(enrollment.get("consecutive_failures") or 0) + 1
            _fail_set: dict = {"last_activity_at": now}
            if _fails >= 5:
                _fail_set["next_action_at"] = None
                _fail_set["status"] = "failed"
                _fail_set["last_transition_reason"] = "max_send_retries"
            else:
                _fail_set["next_action_at"] = now + timedelta(hours=1)
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": _fail_set, "$inc": {"consecutive_failures": 1}},
            )


async def _get_linkedin_account_for_campaign(campaign: dict, enrollment: dict) -> Optional[dict]:
    """Fetch exactly one connected sender owned by the enrollment tenant."""
    tenant_filter = {"$in": _account_values(enrollment.get("account_id"))}
    linkedin_account_id = campaign.get("linkedin_account_id")
    if linkedin_account_id:
        return await database.linkedin_accounts_collection.find_one(
            {
                "_id": ObjectId(str(linkedin_account_id)),
                "account_id": tenant_filter,
                "unipile_status": "OK",
                "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
            }
        )
    candidates = await database.linkedin_accounts_collection.find(
        {
            "account_id": tenant_filter,
            "is_default": True,
            "unipile_status": "OK",
            "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
        }
    ).limit(2).to_list(2)
    return candidates[0] if len(candidates) == 1 else None


async def _get_email_account_for_campaign(campaign: dict, enrollment: dict) -> Optional[dict]:
    email_account_id = campaign.get("email_account_id")
    if not email_account_id:
        return None
    return await database.email_accounts_collection.find_one(
        {
            "_id": ObjectId(str(email_account_id)),
            "account_id": {"$in": _account_values(enrollment.get("account_id"))},
            "status": {"$in": ["connected", "active"]},
        }
    )


# ═══════════════════════════════════════════════════════════════════════════
# Branching sequence campaigns (multi-touch DAG) — see services/sequence_service.py
# ═══════════════════════════════════════════════════════════════════════════

def _seq_extract_subject_body(engine_channel: str, node_msg: Optional[dict], classic_msg: Optional[dict]) -> tuple[str, str]:
    """Resolve (subject, body) for a touch from either a per-node message
    (generated_messages_by_step[node_id]) or the legacy flat generated_messages.

    A thin adapter over the message generator's output shapes — kept in the
    engine so campaign_message_generator_service is not modified.
    """
    if node_msg:
        return node_msg.get("subject", "") or "", node_msg.get("body", "") or ""
    if classic_msg:
        if engine_channel == "email":
            ce = classic_msg.get("cold_email", {})
            return (ce.get("subject_a") or ce.get("subject") or ""), (ce.get("body") or "")
        if engine_channel in ("linkedin_connection", "linkedin_message"):
            note = classic_msg.get("linkedin_connection", {}).get("note", "")
            return "", note
        if engine_channel == "linkedin_inmail":
            im = classic_msg.get("linkedin_inmail", {})
            return im.get("subject", ""), im.get("body", "")
    return "", ""


async def _generate_sequence_node_message(campaign: dict, enrollment: dict, prospect: dict, node: dict) -> None:
    """Lazily generate the message for a sequence node when it becomes current.

    Reuses campaign_message_generator_service.generate_message_for_node (which we
    call but do not edit). The contract node uses channel names like "inmail";
    we adapt to the engine channel the generator understands.
    """
    from services import sequence_service as seq
    from services.campaign_message_generator_service import generate_message_for_node

    gen_node = {
        "id": node.get("id"),
        "channel": seq.engine_channel(node.get("channel")),
        "message_intent": node.get("message_intent"),
        # Optional per-node authoring hints from the sequence builder. A hard
        # subject/body override is applied at send time; guidance steers the AI.
        "subject_template": node.get("subject"),
        "guidance": node.get("guidance"),
    }

    # Build prior-step context from already-generated node messages.
    by_step = enrollment.get("generated_messages_by_step") or {}
    prior_step_messages = []
    for nid, msg in by_step.items():
        prior_step_messages.append({
            "channel": msg.get("channel", ""),
            "subject": msg.get("subject", ""),
            "body_excerpt": (msg.get("body") or "")[:150],
        })

    await generate_message_for_node(
        campaign=campaign,
        enrollment=enrollment,
        prospect=prospect,
        node=gen_node,
        prior_step_messages=prior_step_messages,
    )


async def _send_via_channel(
    campaign: dict,
    enrollment: dict,
    prospect: dict,
    channel: str,
    subject: str,
    body: str,
) -> tuple[str, Optional[str], dict]:
    """Durably dispatch one touch and return its persisted provider outcome."""
    identity = _send_attempt_identity(campaign, enrollment, channel)
    service = SendAttemptService(database.send_attempts_collection)
    payload = {
        "campaign_id": str(campaign["_id"]),
        "prospect_id": str(prospect["_id"]),
        "channel": channel,
        "subject": subject,
        "body": body,
    }

    async def _preflight():
        if channel == "email":
            sender = await _get_email_account_for_campaign(campaign, enrollment)
            recipient = prospect.get("email", "")
            if not sender:
                raise ValueError("No tenant-owned connected email sender")
            if not recipient:
                raise ValueError("Prospect has no email")
            return {
                "sender": sender,
                "recipient": recipient,
                "_attempt_metadata": {
                    "provider": sender.get("provider"),
                    "provider_account_id": str(sender["_id"]),
                    "sender_record_id": str(sender["_id"]),
                },
            }
        if channel in ("linkedin_connection", "linkedin_message", "linkedin_inmail"):
            sender = await _get_linkedin_account_for_campaign(campaign, enrollment)
            profile_url = prospect.get("linkedin", "") or prospect.get("linkedin_url", "")
            if not sender:
                raise ValueError("No tenant-owned connected LinkedIn sender")
            if not profile_url:
                raise ValueError("Prospect has no LinkedIn URL")
            return {
                "sender": sender,
                "profile_url": profile_url,
                "client": UnipileClient(account_id=str(sender["unipile_account_id"])),
                "_attempt_metadata": {
                    "provider": "unipile",
                    "provider_account_id": str(sender["unipile_account_id"]),
                    "sender_record_id": str(sender["_id"]),
                },
            }
        raise ValueError(f"Unsupported channel: {channel}")

    async def _provider_call(context: dict) -> dict | None:
        resolved = context["resolved"]
        frozen = context["payload"]
        frozen_subject = frozen.get("subject", "")
        frozen_body = frozen.get("body", "")
        if channel == "email":
            return await send_email_via_account(
                resolved["sender"], resolved["recipient"], frozen_subject, frozen_body,
                prospect_id=str(prospect["_id"]), campaign_id=str(campaign["_id"]),
            )
        client: UnipileClient = resolved["client"]
        profile_url = resolved["profile_url"]
        if channel == "linkedin_connection":
            raw = await client.send_connection_request(profile_url, frozen_body or None)
        elif channel == "linkedin_message":
            raw = await client.start_new_chat(profile_url, frozen_body)
        else:
            raw = await client.send_inmail(
                profile_url, frozen_body, frozen_subject or None
            )
        if not raw:
            return None
        return {
            "provider": "unipile",
            "message_id": raw.get("message_id") or raw.get("id"),
            "thread_ref": raw.get("chat_id"),
            "raw": raw,
        }

    attempt = await service.dispatch(
        identity=identity,
        channel=channel,
        payload=payload,
        worker_id=ENGINE_WORKER_ID,
        before_provider=_preflight,
        provider_call=_provider_call,
    )
    attempt_state = attempt.get("state")
    result = attempt.get("provider_result") or {}
    result["send_key"] = identity.send_key
    result["provider_account_id"] = attempt.get("provider_account_id")
    frozen_payload = attempt.get("payload") or {}
    result["frozen_subject"] = frozen_payload.get("subject", subject)
    result["frozen_body"] = frozen_payload.get("body", body)
    if attempt_state == SendAttemptState.SENT.value:
        pmid = result.get("message_id")
        return "sent", str(pmid) if pmid else None, result
    if attempt_state in {
        SendAttemptState.AMBIGUOUS.value,
        SendAttemptState.DISPATCHING.value,
    }:
        return "ambiguous", None, result
    return "failed", None, result


async def _seq_detect_reply(enrollment: dict, prospect: dict) -> bool:
    """Detect whether the prospect has replied on any channel (backstop check).

    The reply-webhook / poller paths stamp sequence_state.stopped_reason directly,
    so this is a defensive re-check at timeout evaluation time.
    """
    if prospect.get("status") == "replied":
        return True
    try:
        state_doc = await database.prospect_state_collection.find_one(
            {
                "account_id": str(enrollment.get("account_id", "")),
                "prospect_id": str(enrollment.get("prospect_id", "")),
            },
            {"status": 1},
        )
        if state_doc and state_doc.get("status") == "replied":
            return True
    except Exception:
        pass
    # Inbound message in the conversation after our last send
    last_sent = (enrollment.get("sequence_state") or {}).get("last_sent_at")
    try:
        conv = await database.conversations_collection.find_one(
            {
                "account_id": {
                    "$in": _account_values(enrollment.get("account_id"))
                },
                "prospect_id": {
                    "$in": [
                        str(enrollment.get("prospect_id", "")),
                        enrollment.get("prospect_id"),
                    ]
                },
            },
        )
        if conv:
            for m in conv.get("messages", []):
                if m.get("direction") != "inbound":
                    continue
                ts = m.get("created_at") or m.get("timestamp")
                if last_sent is None or (ts is not None and ts > last_sent):
                    return True
    except Exception:
        pass
    return False


def _seq_detect_accepted(prospect: dict, enrollment: dict) -> bool:
    """Detect whether the prospect accepted the connection request since our last send."""
    accepted_at = (enrollment.get("linkedin_activity") or {}).get("connection_accepted_at")
    if not accepted_at:
        return False
    last_sent = (enrollment.get("sequence_state") or {}).get("last_sent_at")
    if last_sent is not None and isinstance(accepted_at, datetime) and isinstance(last_sent, datetime):
        return accepted_at >= last_sent - timedelta(minutes=1)
    return True


async def _complete_sequence_enrollment(enrollment: dict, campaign: dict, now: datetime, stopped_reason: str) -> None:
    """Terminate a sequence enrollment, updating counters + used_by lifecycle.

    Status mapping: ``replied`` → "replied"; a sequence exhausted with no
    response (``max_touches`` / ``sequence_end``) → "not_replied" (terminal —
    the prospect was contacted but never answered); every other reason (e.g.
    ``suppressed``) keeps the legacy "completed".
    """
    from services import sequence_service as seq

    if stopped_reason == seq.STOP_REPLIED:
        status = "replied"
    elif stopped_reason in (seq.STOP_MAX_TOUCHES, seq.STOP_SEQUENCE_END):
        status = "not_replied"
    else:
        status = "completed"
    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment["_id"]},
        {"$set": {
            "status": status,
            "completed_at": now,
            "last_activity_at": now,
            "next_action_at": None,
            "sequence_state.stopped_reason": stopped_reason,
            "sequence_state.phase": "stopped",
            "sequence_state.next_action_at": None,
        }},
    )
    inc = {"active_count": -1}
    if status == "replied":
        inc["replied_count"] = 1
    elif status == "not_replied":
        inc["not_replied_count"] = 1
    else:
        inc["completed_count"] = 1
    await database.campaigns_collection.update_one({"_id": campaign["_id"]}, {"$inc": inc})

    # ── Event-driven cascade rotation ──
    # When the primary of a company cascade group exhausts its sequence without
    # a reply, wake the next backup prospect for that company. Fail-soft: a
    # rotation error must never break enrollment termination.
    if status == "not_replied" and enrollment.get("cascade_group_id"):
        try:
            from services.cascade_rotation_service import activate_next_backup
            activated = await activate_next_backup(enrollment)
            if activated:
                logger.info(
                    "Cascade rotation: activated backup enrollment %s (group=%s pos=%s) "
                    "after primary %s went not_replied",
                    activated.get("_id"), enrollment.get("cascade_group_id"),
                    activated.get("cascade_position"), enrollment.get("_id"),
                )
        except Exception as _casc_e:
            logger.warning(
                "Cascade rotation failed for enrollment %s (group=%s): %s",
                enrollment.get("_id"), enrollment.get("cascade_group_id"), _casc_e,
            )
    try:
        from services.prospect_search_service import update_used_by_status
        await update_used_by_status(
            database.db,
            account_id=str(enrollment.get("account_id", "")),
            prospect_id=str(enrollment.get("prospect_id", "")),
            campaign_id=str(enrollment.get("campaign_id", "")),
            new_status="completed",
            completed_at=now,
        )
    except Exception as _sync_e:
        logger.warning("used_by sync failed for sequence enrollment %s: %s", enrollment.get("_id"), _sync_e)


async def _persist_sequence_state(enrollment: dict, campaign: dict, new_state: dict, now: datetime) -> None:
    """Write an advanced sequence_state back to the enrollment (and terminate if stopped)."""
    if new_state.get("stopped_reason"):
        await _complete_sequence_enrollment(enrollment, campaign, now, new_state["stopped_reason"])
        return
    update_fields = {
        "sequence_state": new_state,
        "next_action_at": new_state.get("next_action_at"),
        "last_activity_at": now,
        "status": "active",
        "current_step": int(enrollment.get("current_step", 0)) + 1,
    }
    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment["_id"]},
        {"$set": update_fields},
    )


async def _execute_sequence_enrollment(enrollment: dict, campaign: dict, prospect: dict) -> None:
    """Execute one tick for a branching-sequence enrollment.

    Two phases per node:
      * ``pending_send`` — deliver the current node's touch (reserving daily caps,
        lazily generating the message if needed), then advance on ``sent``.
      * ``awaiting`` — the timeout fired; check reply / acceptance signals and
        advance via ``replied`` / ``accepted`` / ``no_reply_timeout``.
    """
    from services import sequence_service as seq
    from services.daily_cap_service import (
        reserve_slot, release_slot, reserve_sender_slot,
        release_sender_slot as _release_sender_slot,
    )

    now = datetime.utcnow()
    state = enrollment.get("sequence_state") or {}
    graph = campaign.get("sequence_graph") or {}

    # Already stopped — finalize and exit.
    if state.get("stopped_reason"):
        await _complete_sequence_enrollment(enrollment, campaign, now, state["stopped_reason"])
        return

    node = seq.get_node(graph, state.get("current_node_id"))
    if not node:
        await _complete_sequence_enrollment(enrollment, campaign, now, "sequence_end")
        return

    phase = state.get("phase", "pending_send")
    node_engine_channel = seq.engine_channel(node.get("channel"))

    # ── AWAITING: evaluate signals at the timeout ──
    if phase == "awaiting":
        if await _seq_detect_reply(enrollment, prospect):
            new_state = seq.advance_sequence(enrollment, campaign, "replied", now=now)
        elif node_engine_channel == "linkedin_connection" and _seq_detect_accepted(prospect, enrollment):
            new_state = seq.advance_sequence(enrollment, campaign, "accepted", now=now)
        else:
            new_state = seq.advance_sequence(enrollment, campaign, "no_reply_timeout", now=now)
        await _persist_sequence_state(enrollment, campaign, new_state, now)
        return

    # ── PENDING_SEND: deliver the current node's touch ──

    # Suppression check — opt out if the prospect is suppressed.
    try:
        from services.suppression_service import is_suppressed
        if await is_suppressed(str(enrollment.get("account_id", "")), prospect):
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {
                    "status": "opted_out",
                    "last_transition_reason": "suppressed",
                    "sequence_state.stopped_reason": "suppressed",
                    "sequence_state.phase": "stopped",
                    "sequence_state.next_action_at": None,
                }},
            )
            await database.campaigns_collection.update_one(
                {"_id": campaign["_id"]}, {"$inc": {"active_count": -1, "opted_out_count": 1}},
            )
            return
    except Exception as _sup_err:
        logger.warning(f"Suppression check failed for sequence enrollment {enrollment['_id']}: {_sup_err}")

    existing_attempt = await _existing_send_attempt(
        campaign, enrollment, node_engine_channel
    )
    cap_already_reserved = _attempt_owns_daily_cap(existing_attempt)

    resolved_sender = None
    if not (
        existing_attempt
        and existing_attempt.get("state") == SendAttemptState.SENT.value
    ):
        if node_engine_channel == "email":
            resolved_sender = await _get_email_account_for_campaign(campaign, enrollment)
        elif node_engine_channel in {
            "linkedin_connection",
            "linkedin_inmail",
            "linkedin_message",
        }:
            resolved_sender = await _get_linkedin_account_for_campaign(campaign, enrollment)
        if not resolved_sender:
            skip_count = enrollment.get("consecutive_channel_skip_count", 0) + 1
            if skip_count >= 3:
                await database.campaign_enrollments_collection.update_one(
                    {"_id": enrollment["_id"]},
                    {"$set": {
                        "status": "skipped_no_channel",
                        "consecutive_channel_skip_count": skip_count,
                        "last_transition_reason": "sender_unavailable",
                    }},
                )
                logger.warning(f"Sequence enrollment {enrollment['_id']} marked skipped_no_channel after {skip_count} unavailable-sender skips")
                return
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {
                    "next_action_at": now + timedelta(hours=1),
                    "sequence_state.next_action_at": now + timedelta(hours=1),
                    "last_transition_reason": "sender_unavailable",
                    "consecutive_channel_skip_count": skip_count,
                }},
            )
            return

    # Reserve a campaign-level daily-cap slot once per deterministic touch.
    if not cap_already_reserved and not await reserve_slot(
        database.db, enrollment["campaign_id"], node_engine_channel
    ):
        from datetime import timezone as _tz
        tomorrow = datetime.now(_tz.utc) + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=None)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {"next_action_at": tomorrow, "sequence_state.next_action_at": tomorrow}},
        )
        logger.info(f"Sequence enrollment {enrollment['_id']} deferred to tomorrow (daily cap hit for {node_engine_channel})")
        return

    # Enforce the sender-level daily cap too.
    sender_id = str(
        (existing_attempt or {}).get("sender_record_id")
        or (resolved_sender or {}).get("_id")
        or ""
    )
    if sender_id and not cap_already_reserved:
        if not await reserve_sender_slot(
            database.db, sender_id, node_engine_channel, sender=resolved_sender
        ):
            await release_slot(database.db, enrollment["campaign_id"], node_engine_channel)
            from services.daily_cap_service import get_sender_defer_until
            tomorrow = await get_sender_defer_until(
                database.db, sender_id, node_engine_channel
            )
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"next_action_at": tomorrow, "sequence_state.next_action_at": tomorrow}},
            )
            logger.info(f"Sequence enrollment {enrollment['_id']} deferred to tomorrow (sender cap hit for {node_engine_channel})")
            return

    # Ensure a message exists for this node. First touch is generated for all
    # prospects at approval (generated_messages); later nodes are generated
    # lazily here when the node becomes current.
    node_id = node.get("id")
    # Any route's start node is a "first touch" whose copy was generated at
    # approval time (flat generated_messages); downstream nodes generate lazily.
    is_start = (node_id in seq.get_start_node_ids(graph))
    node_msg = (enrollment.get("generated_messages_by_step") or {}).get(node_id)
    if not node_msg:
        if is_start:
            # Fall back to the legacy flat message written at approval time.
            pass
        else:
            try:
                await _generate_sequence_node_message(campaign, enrollment, prospect, node)
                refreshed = await database.campaign_enrollments_collection.find_one({"_id": enrollment["_id"]})
                if refreshed:
                    enrollment = refreshed
                    node_msg = (enrollment.get("generated_messages_by_step") or {}).get(node_id)
            except Exception as _gen_err:
                logger.warning(f"Sequence message gen failed for enrollment {enrollment['_id']} node {node_id}: {_gen_err}")
                await release_slot(database.db, enrollment["campaign_id"], node_engine_channel)
                if sender_id:
                    await _release_sender_slot(database.db, sender_id, node_engine_channel)
                _gen_fails = int(enrollment.get("consecutive_failures") or 0) + 1
                _gen_fail_set: dict = {"last_activity_at": now}
                if _gen_fails >= 5:
                    _gen_fail_set["next_action_at"] = None
                    _gen_fail_set["status"] = "failed"
                    _gen_fail_set["last_transition_reason"] = "max_message_gen_retries"
                else:
                    _gen_fail_set["next_action_at"] = now + timedelta(hours=1)
                await database.campaign_enrollments_collection.update_one(
                    {"_id": enrollment["_id"]},
                    {"$set": _gen_fail_set, "$inc": {"consecutive_failures": 1}},
                )
                return

    subject, body = _seq_extract_subject_body(
        node_engine_channel, node_msg, enrollment.get("generated_messages"),
    )

    # Per-node manual override: if the sequence builder set explicit copy on this
    # node, it wins over AI-generated copy (personalized via {{variable}} tokens).
    override_subject = (node.get("subject") or "").strip()
    override_body = (node.get("body") or "").strip()
    if override_body:
        body = personalize(override_body, prospect)
    if override_subject:
        subject = personalize(override_subject, prospect)

    status = "failed"
    provider_message_id = None
    send_result: dict = {}
    try:
        status, provider_message_id, send_result = await _send_via_channel(
            campaign, enrollment, prospect, node_engine_channel, subject, body,
        )
        subject = send_result.get("frozen_subject", subject)
        body = send_result.get("frozen_body", body)
    except Exception as e:
        logger.error(
            f"Sequence send error for enrollment {enrollment['_id']} (channel={node_engine_channel}): {e}",
            exc_info=True,
        )
        status = "failed"

    # Record the touch.
    _channel_to_action = {
        "email": "email",
        "linkedin_connection": "connection_request",
        "linkedin_inmail": "inmail",
        "linkedin_message": "linkedin_message",
    }
    _channel_to_msg_channel = {
        "email": "email",
        "linkedin_connection": "linkedin",
        "linkedin_inmail": "linkedin",
        "linkedin_message": "linkedin",
    }
    step_def = {
        "channel": _channel_to_msg_channel.get(node_engine_channel, "email"),
        "action": _channel_to_action.get(node_engine_channel, "email"),
    }
    await record_campaign_message(
        enrollment, campaign, step_def, body, subject, provider_message_id, status,
        provider_thread_id=send_result.get("thread_ref"),
        provider=send_result.get("provider"),
        rfc_message_id=send_result.get("rfc_message_id"),
        send_key=send_result.get("send_key"),
        prospect=prospect,
        provider_account_id=send_result.get("provider_account_id"),
        content_html=send_result.get("content_html"),
    )

    if status == "ambiguous":
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {
                "status": "send_ambiguous",
                "next_action_at": None,
                "sequence_state.next_action_at": None,
                "ambiguous_send_key": send_result.get("send_key"),
                "last_transition_reason": "provider_outcome_requires_reconciliation",
                "last_activity_at": now,
            }},
        )
        return

    if status == "sent":
        # A connection request must be stamped on the tenant overlay + enrollment,
        # otherwise connection_tracker_service.check_pending_connections never
        # picks the prospect up and the graph's "accepted" edge can never fire —
        # every prospect would silently fall through to the not_accepted timeout.
        # Mirrors the manual send path in routes/linkedin_outreach.py.
        if node_engine_channel == "linkedin_connection":
            try:
                from services.prospect_activity_state_service import (
                    record_prospect_activity,
                )
                await record_prospect_activity(
                    account_id=enrollment.get("account_id"),
                    prospect_id=enrollment.get("prospect_id"),
                    enrollment_id=enrollment["_id"],
                    fields={
                        "status": "contacted",
                        "connection_request_sent_at": now,
                    },
                    event={
                        "type": "connection_request_sent",
                        "channel": "linkedin",
                        "at": now,
                        "campaign_id": str(campaign["_id"]),
                        "node_id": node_id,
                    },
                )
            except Exception as _activity_err:
                logger.warning(
                    "Failed to record connection_request_sent_at for enrollment %s: %s",
                    enrollment["_id"], _activity_err,
                )

        new_state = seq.advance_sequence(enrollment, campaign, "sent", now=now)
        await _persist_sequence_state(enrollment, campaign, new_state, now)
    else:
        # Release reserved slots (once per attempt) and back off (cap consecutive failures).
        await _release_cap_slots_once(send_result.get("send_key"), enrollment["campaign_id"], node_engine_channel, sender_id)
        fails = int(enrollment.get("consecutive_failures") or 0) + 1
        fail_set: dict = {"last_activity_at": now}
        if fails >= 5:
            fail_set["next_action_at"] = None
            fail_set["status"] = "failed"
            fail_set["last_transition_reason"] = "max_send_retries"
        else:
            fail_set["next_action_at"] = now + timedelta(hours=1)
            fail_set["sequence_state.next_action_at"] = now + timedelta(hours=1)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": fail_set, "$inc": {"consecutive_failures": 1}},
        )


async def execute_enrollment(enrollment: dict, campaign: dict, prospect: dict):
    """Execute the current step for one enrollment."""
    now = datetime.utcnow()

    # ── Branching sequence campaign (multi-touch DAG) ──
    # Gated strictly on the presence of a sequence_graph + sequence_state so
    # legacy campaigns are never routed here.
    if campaign.get("sequence_graph") and enrollment.get("sequence_state"):
        await _execute_sequence_enrollment(enrollment, campaign, prospect)
        return

    # ── Smart Campaign (v2): pre-generated messages, single step ──
    smart_channel = enrollment.get("smart_campaign_channel")
    if smart_channel and enrollment.get("generated_messages"):
        await _execute_smart_enrollment(enrollment, campaign, prospect, smart_channel)
        return

    # ── Classic Campaign: step template logic below ──
    current_step_idx = enrollment.get("current_step", 0)
    steps = campaign.get("steps", [])

    if current_step_idx >= len(steps):
        # Out of bounds — complete enrollment
        _oob_now = datetime.utcnow()
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {"status": "completed", "completed_at": _oob_now}},
        )
        # Sync used_by lifecycle on prospect_state overlay
        try:
            from services.prospect_search_service import update_used_by_status
            await update_used_by_status(
                database.db,
                account_id=str(enrollment.get("account_id", "")),
                prospect_id=str(enrollment.get("prospect_id", "")),
                campaign_id=str(enrollment.get("campaign_id", "")),
                new_status="completed",
                completed_at=_oob_now,
            )
        except Exception as _sync_e:
            logger.warning("used_by sync failed for enrollment %s: %s", enrollment.get("_id"), _sync_e)
        return

    step_def = steps[current_step_idx]

    # Load sender context from company profile
    sender_context = {}
    company_profile = await database.company_profiles_collection.find_one(
        {"account_id": enrollment["account_id"]}
    )
    if company_profile:
        sender_context = company_profile

    # Personalize templates
    body = personalize(step_def.get("body_template", ""), prospect, sender_context)
    subject = personalize(step_def.get("subject_template", ""), prospect, sender_context)

    action = step_def.get("action", "email")
    engine_channel = {
        "email": "email",
        "connection_request": "linkedin_connection",
        "inmail": "linkedin_inmail",
        "linkedin_message": "linkedin_message",
    }.get(action, action)

    # Classic steps must respect the same daily caps as smart/sequence
    # enrollments — without this, a classic campaign can blow past the
    # campaign- and sender-level daily quotas entirely.
    from services.daily_cap_service import reserve_slot, release_slot, reserve_sender_slot

    existing_attempt = await _existing_send_attempt(campaign, enrollment, engine_channel)
    cap_already_reserved = _attempt_owns_daily_cap(existing_attempt)

    resolved_sender = None
    if engine_channel == "email":
        resolved_sender = await _get_email_account_for_campaign(campaign, enrollment)
    elif engine_channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message"):
        resolved_sender = await _get_linkedin_account_for_campaign(campaign, enrollment)

    slot_reserved = cap_already_reserved or await reserve_slot(
        database.db, enrollment["campaign_id"], engine_channel
    )
    if not slot_reserved:
        from datetime import timezone as _tz
        tomorrow = datetime.now(_tz.utc) + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=None)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {"next_action_at": tomorrow}},
        )
        logger.info(f"Classic enrollment {enrollment['_id']} deferred to tomorrow (daily cap hit for {engine_channel})")
        return

    _sender_id = str(
        (existing_attempt or {}).get("sender_record_id")
        or (resolved_sender or {}).get("_id")
        or ""
    )
    if _sender_id and not cap_already_reserved:
        sender_slot_reserved = await reserve_sender_slot(
            database.db, _sender_id, engine_channel, sender=resolved_sender
        )
        if not sender_slot_reserved:
            await release_slot(database.db, enrollment["campaign_id"], engine_channel)
            from services.daily_cap_service import get_sender_defer_until
            tomorrow = await get_sender_defer_until(database.db, _sender_id, engine_channel)
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"next_action_at": tomorrow}},
            )
            logger.info(f"Classic enrollment {enrollment['_id']} deferred to tomorrow (sender {_sender_id} cap hit for {engine_channel})")
            return

    provider_message_id = None
    status = "failed"
    send_result: dict = {}

    try:
        if action == "connection_request":
            body = personalize(
                step_def.get("connection_note_template", body), prospect, sender_context
            )
        elif action == "inmail":
            subject = personalize(
                step_def.get("inmail_subject_template", subject), prospect, sender_context
            )
            body = personalize(
                step_def.get("inmail_body_template", body), prospect, sender_context
            )
        status, provider_message_id, send_result = await _send_via_channel(
            campaign, enrollment, prospect, engine_channel, subject, body,
        )
        subject = send_result.get("frozen_subject", subject)
        body = send_result.get("frozen_body", body)

    except Exception as e:
        logger.error(
            f"Error executing step for enrollment {enrollment['_id']}: {e}",
            exc_info=True,
        )
        status = "failed"

    # Record the message outcome
    await record_campaign_message(
        enrollment, campaign, step_def, body, subject, provider_message_id, status,
        provider_thread_id=send_result.get("thread_ref"),
        provider=send_result.get("provider"),
        rfc_message_id=send_result.get("rfc_message_id"),
        send_key=send_result.get("send_key"),
        prospect=prospect,
        provider_account_id=send_result.get("provider_account_id"),
        content_html=send_result.get("content_html"),
    )

    if status == "ambiguous":
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {"$set": {
                "status": "send_ambiguous",
                "next_action_at": None,
                "ambiguous_send_key": send_result.get("send_key"),
                "last_transition_reason": "provider_outcome_requires_reconciliation",
                "last_activity_at": datetime.utcnow(),
            }},
        )
        return

    # Advance to next step if successful; otherwise track failure
    if status == "sent":
        await advance_enrollment(enrollment, campaign)
    else:
        await _release_cap_slots_once(send_result.get("send_key"), enrollment["campaign_id"], engine_channel, _sender_id)
        await database.campaign_enrollments_collection.update_one(
            {"_id": enrollment["_id"]},
            {
                "$set": {
                    "last_activity_at": datetime.utcnow(),
                    "next_action_at": datetime.utcnow() + timedelta(hours=1),
                },
                "$inc": {"consecutive_failures": 1},
            },
        )


async def _activate_due_cascade_enrollments(now: datetime):
    """
    Legacy TIME-BASED cascade activation (API-route enrollments that carry a
    concrete ``cascade_activate_at``): once the timestamp passes, activate the
    backup unless the primary already engaged.

    Event-driven backups (discovery-created, ``cascade_activate_at: None``) are
    deliberately excluded — they are only woken by ``activate_next_backup``
    when their primary's sequence ends "not_replied".
    """
    due_cascade = await database.campaign_enrollments_collection.find({
        "cascade_status": "waiting",
        # $ne None keeps event-driven backups (activate_at unset/None) out of
        # the time-based sweep even though $lte would already skip None.
        "cascade_activate_at": {"$ne": None, "$lte": now},
    }).to_list(length=100)

    if not due_cascade:
        return

    logger.info(f"Cascade check: {len(due_cascade)} waiting cascade enrollments")

    for enrollment in due_cascade:
        group_id = enrollment.get("cascade_group_id")
        account_id = enrollment.get("account_id")
        if not group_id or not account_id:
            continue

        # Check if the primary (position=0) in this group has replied
        primary = await database.campaign_enrollments_collection.find_one({
            "account_id": {"$in": _account_values(account_id)},
            "cascade_group_id": group_id,
            "cascade_position": 0,
        })

        # Suppress the backup only when the primary genuinely engaged/closed
        # (replied / completed / opted_out). A primary that finished
        # "not_replied" must NOT suppress backups — that is exactly the case
        # where the next prospect at the company should be tried.
        if primary and primary.get("status") in ("replied", "completed", "opted_out"):
            # Primary replied — mark this cascade as dormant (won't need outreach)
            await database.campaign_enrollments_collection.update_one(
                {
                    "_id": enrollment["_id"],
                    "account_id": {"$in": _account_values(account_id)},
                },
                {"$set": {"cascade_status": "dormant", "status": "opted_out", "last_activity_at": now}},
            )
            logger.info(f"Cascade dormant (primary replied): group={group_id} pos={enrollment.get('cascade_position')}")
        else:
            # Primary has NOT replied → activate this prospect
            await database.campaign_enrollments_collection.update_one(
                {
                    "_id": enrollment["_id"],
                    "account_id": {"$in": _account_values(account_id)},
                },
                {"$set": {
                    "status": "active",
                    "cascade_status": "activated",
                    "next_action_at": now,
                    "last_activity_at": now,
                }},
            )
            # Backups only count as enrolled/active once activated.
            try:
                await database.campaigns_collection.update_one(
                    {"_id": ObjectId(str(enrollment["campaign_id"]))},
                    {"$inc": {"active_count": 1, "total_enrolled": 1}},
                )
            except Exception as _cnt_e:
                logger.warning(f"Cascade activation counter update failed: {_cnt_e}")
            logger.info(f"Cascade activated: group={group_id} pos={enrollment.get('cascade_position')}")




async def execute_pending_steps():
    """
    Main campaign engine loop. Called every 5 minutes by APScheduler.
    Finds all active enrollments with next_action_at <= now and executes them.
    """
    now = datetime.utcnow()
    logger.info(f"Campaign engine running at {now.isoformat()}")

    try:
        # Activate cascade enrollments whose primary prospect hasn't replied
        await _activate_due_cascade_enrollments(now)
        processed = 0
        for _ in range(200):
            enrollment = await claim_due_enrollment(
                database.campaign_enrollments_collection,
                worker_id=ENGINE_WORKER_ID,
                now=now,
                lease_seconds=300,
            )
            if not enrollment:
                break
            processed += 1
            try:
                account_values = _account_values(enrollment.get("account_id"))
                try:
                    campaign_oid = ObjectId(str(enrollment["campaign_id"]))
                    prospect_oid = ObjectId(str(enrollment["prospect_id"]))
                except Exception:
                    await database.campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$set": {
                            "status": "failed",
                            "next_action_at": None,
                            "last_transition_reason": "invalid_campaign_or_prospect_id",
                        }},
                    )
                    continue

                campaign = await database.campaigns_collection.find_one({
                    "_id": campaign_oid,
                    "account_id": {"$in": account_values},
                    "status": "active",
                })
                if not campaign:
                    # Campaign is paused/draft/completed (or missing) — back off for
                    # hours, not minutes, so a long pause doesn't spin the engine on
                    # this enrollment every tick.
                    await database.campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$set": {"next_action_at": now + timedelta(hours=6)}},
                    )
                    continue

                prospect = await database.prospects_collection.find_one({"_id": prospect_oid})
                if not prospect:
                    await database.campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$set": {
                            "status": "failed",
                            "next_action_at": None,
                            "last_transition_reason": "prospect_missing",
                        }},
                    )
                    continue

                if not is_in_send_window(campaign, prospect):
                    next_window = get_next_send_window(campaign, prospect)
                    await database.campaign_enrollments_collection.update_one(
                        {"_id": enrollment["_id"]},
                        {"$set": {"next_action_at": next_window}},
                    )
                    continue

                await execute_enrollment(enrollment, campaign, prospect)
            finally:
                await release_enrollment_lease(
                    database.campaign_enrollments_collection, enrollment
                )

        if processed == 0:
            logger.info("Campaign engine: no due enrollments")
        else:
            logger.info("Campaign engine: processed %s leased enrollments", processed)

    except Exception as e:
        logger.error(f"Campaign engine error: {e}", exc_info=True)


async def reconcile_ambiguous_send(
    send_key: str,
    outcome: str,
    provider_result: Optional[dict] = None,
) -> Optional[dict]:
    """Apply provider evidence and safely re-queue enrollment finalization.

    A reconciled ``sent`` attempt is reprocessed through the normal engine: the
    attempt state returns the stored provider result without another provider
    call, then message recording and sequence advancement finish idempotently.
    ``not_sent`` similarly re-opens the provider dispatch. ``unknown`` stays
    quarantined.
    """
    service = SendAttemptService(database.send_attempts_collection)
    attempt = await service.reconcile(
        send_key=send_key,
        outcome=outcome,
        provider_result=provider_result,
    )
    if not attempt:
        return None
    if outcome == "not_sent":
        # The original provider-boundary attempt consumed both caps. The
        # ambiguous -> retry_scheduled transition is atomic, so this executes
        # exactly once and the retry can reserve fresh capacity.
        from services.daily_cap_service import release_slot, release_sender_slot

        payload = attempt.get("payload") or {}
        campaign_id = payload.get("campaign_id")
        channel = attempt.get("channel") or payload.get("channel")
        if campaign_id and channel:
            await release_slot(database.db, campaign_id, channel)
        sender_id = attempt.get("sender_record_id")
        if sender_id and channel:
            await release_sender_slot(database.db, str(sender_id), channel)
    if attempt.get("state") in {
        SendAttemptState.SENT.value,
        SendAttemptState.RETRY_SCHEDULED.value,
    }:
        await database.campaign_enrollments_collection.update_one(
            {
                "_id": ObjectId(str(attempt["enrollment_id"])),
                "account_id": {"$in": _account_values(attempt["account_id"])},
                "ambiguous_send_key": send_key,
            },
            {
                "$set": {
                    "status": "active",
                    "next_action_at": datetime.utcnow(),
                    "last_transition_reason": f"send_reconciled_{outcome}",
                },
                "$unset": {"ambiguous_send_key": ""},
            },
        )
    return attempt


async def compute_campaign_daily_stats():
    """Nightly job: compute daily stats for all active/paused/completed campaigns."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Computing daily stats for {yesterday}")

    try:
        campaigns_cursor = database.campaigns_collection.find(
            {"status": {"$in": ["active", "paused", "completed"]}}
        )
        async for campaign in campaigns_cursor:
            campaign_id = campaign["_id"]
            campaign_id_str = str(campaign_id)

            msg_filter = {
                "campaign_id": campaign_id,
                "sent_at": {
                    "$gte": datetime.strptime(f"{yesterday} 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    "$lt": datetime.strptime(f"{today} 00:00:00", "%Y-%m-%d %H:%M:%S"),
                },
            }

            emails_sent = await database.campaign_messages_collection.count_documents(
                {**msg_filter, "action": "email", "status": "sent"}
            )
            emails_replied = await database.campaign_messages_collection.count_documents(
                {**msg_filter, "action": "email", "status": "replied"}
            )
            linkedin_sent = await database.campaign_messages_collection.count_documents(
                {**msg_filter, "channel": "linkedin", "status": "sent"}
            )
            new_enrollments = await database.campaign_enrollments_collection.count_documents(
                {
                    "campaign_id": campaign_id,
                    "enrolled_at": {
                        "$gte": datetime.strptime(f"{yesterday} 00:00:00", "%Y-%m-%d %H:%M:%S"),
                        "$lt": datetime.strptime(f"{today} 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    },
                }
            )

            stat_doc = {
                "campaign_id": campaign_id_str,
                "account_id": campaign["account_id"],
                "date": yesterday,
                "emails_sent": emails_sent,
                "emails_replied": emails_replied,
                "linkedin_sent": linkedin_sent,
                "new_enrollments": new_enrollments,
                "computed_at": datetime.utcnow(),
            }

            await database.campaign_daily_stats_collection.update_one(
                {"campaign_id": campaign_id_str, "date": yesterday},
                {"$set": stat_doc},
                upsert=True,
            )

    except Exception as e:
        logger.error(f"Daily stats error: {e}", exc_info=True)
