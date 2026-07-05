"""
Campaign Engine - executes due enrollment steps every 5 minutes.
Finds enrollments where status="active" and next_action_at <= now.
Sends email or LinkedIn message, records the outcome, advances to next step.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta
from bson import ObjectId
from typing import Optional

import database
from config import get_settings
from services.unipile_service import UnipileClient as UnipileService

logger = logging.getLogger(__name__)
settings = get_settings()

# Lazy-loaded service instances
_unipile_service: Optional[UnipileService] = None


def get_unipile_service() -> UnipileService:
    global _unipile_service
    if _unipile_service is None:
        _unipile_service = UnipileService()
    return _unipile_service


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
    except Exception:
        return True  # If timezone lookup fails, allow sending


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
    tracking_token: Optional[str] = None,
    prospect_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Send email via the appropriate provider.

    Returns a dict with at minimum {"message_id": str} on success, or None on failure.
    Gmail additionally returns {"thread_id": str, "rfc_message_id": str}.
    """
    provider = email_account.get("provider", "sendgrid")

    if provider == "sendgrid":
        try:
            import httpx
            headers = {
                "Authorization": f"Bearer {email_account['sendgrid_api_key']}",
                "Content-Type": "application/json",
            }
            payload = {
                "personalizations": [{"to": [{"email": to_email}]}],
                "from": {
                    "email": email_account.get("sendgrid_sender_email", ""),
                    "name": email_account.get("sendgrid_sender_name", ""),
                },
                "subject": subject,
                "content": [{"type": "text/html", "value": body}],
            }
            if email_account.get("sendgrid_reply_to"):
                payload["reply_to"] = {"email": email_account["sendgrid_reply_to"]}

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.sendgrid.com/v3/mail/send",
                    headers=headers,
                    json=payload,
                )
                if resp.status_code == 202:
                    msg_id = resp.headers.get("X-Message-Id", "sendgrid-sent")
                    return {"message_id": msg_id, "provider": "sendgrid"}
                else:
                    logger.error(f"SendGrid error {resp.status_code}: {resp.text}")
                    return None
        except Exception as e:
            logger.error(f"Email send failed via SendGrid: {e}")
            return None

    elif provider == "google":
        try:
            from services.gmail_service import send_gmail_email
            from routes.email_tracking import (
                build_pixel_url,
                inject_tracking_pixel,
                rewrite_links_for_tracking,
            )

            html_body = body

            # 1. Inject open-tracking pixel
            if tracking_token:
                try:
                    pixel_url = build_pixel_url(tracking_token)
                    html_body = inject_tracking_pixel(html_body, pixel_url)
                except Exception as px_err:
                    logger.warning(f"Failed to inject tracking pixel: {px_err}")

            # 2. Rewrite links for click tracking
            if prospect_id:
                try:
                    html_body = await rewrite_links_for_tracking(
                        html_body,
                        prospect_id=prospect_id,
                        email_account_id=str(email_account["_id"]),
                        campaign_id=campaign_id,
                    )
                except Exception as cl_err:
                    logger.warning(f"Failed to rewrite links for click tracking: {cl_err}")

            result = await send_gmail_email(email_account, to_email, subject, html_body)
            if result:
                result["provider"] = "gmail"
            return result
        except Exception as e:
            logger.error(f"Gmail send failed for {to_email}: {e}")
            return None

    elif provider == "microsoft":
        logger.info(f"Outlook send to {to_email} (stub)")
        return {"message_id": f"outlook-{datetime.utcnow().timestamp()}", "provider": "microsoft"}

    elif provider == "smtp":
        try:
            import aiosmtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            from routes.email_tracking import (
                build_pixel_url,
                inject_tracking_pixel,
                rewrite_links_for_tracking,
            )

            html_body = body

            # Inject tracking pixel for open tracking
            if tracking_token:
                try:
                    pixel_url = build_pixel_url(tracking_token)
                    html_body = inject_tracking_pixel(html_body, pixel_url)
                except Exception as px_err:
                    logger.warning(f"Failed to inject tracking pixel: {px_err}")

            # Rewrite links for click tracking
            if prospect_id:
                try:
                    html_body = await rewrite_links_for_tracking(
                        html_body,
                        prospect_id=prospect_id,
                        email_account_id=str(email_account["_id"]),
                        campaign_id=campaign_id,
                    )
                except Exception as cl_err:
                    logger.warning(f"Failed to rewrite links for click tracking: {cl_err}")

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = email_account.get("smtp_username", "")
            msg["To"] = to_email
            msg.attach(MIMEText(html_body, "html"))

            await aiosmtplib.send(
                msg,
                hostname=email_account["smtp_host"],
                port=email_account.get("smtp_port", 587),
                username=email_account["smtp_username"],
                password=email_account["smtp_password"],
                use_tls=(email_account.get("smtp_encryption") == "ssl"),
                start_tls=(email_account.get("smtp_encryption") == "tls"),
            )
            return {"message_id": f"smtp-{datetime.utcnow().timestamp()}", "provider": "smtp"}
        except Exception as e:
            logger.error(f"SMTP send failed: {e}")
            return None

    return None


async def record_campaign_message(
    enrollment: dict,
    campaign: dict,
    step_def: dict,
    content_text: str,
    subject: str,
    provider_message_id: Optional[str],
    status: str,
    gmail_thread_id: Optional[str] = None,
    provider: Optional[str] = None,
    ab_variant: Optional[str] = None,
):
    """Record a sent (or failed) message in campaign_messages."""
    doc = {
        "campaign_id": campaign["_id"],
        "campaign_enrollment_id": enrollment["_id"],
        "account_id": enrollment["account_id"],
        "prospect_id": enrollment["prospect_id"],
        "step_number": enrollment["current_step"],
        "channel": step_def["channel"],
        "action": step_def["action"],
        "direction": "outbound",
        "subject": subject,
        "content_text": content_text,
        "content_html": content_text,  # TODO: proper HTML wrapping
        "provider_message_id": provider_message_id,
        "email_account_id": campaign.get("email_account_id"),
        "status": status,
        "scheduled_at": datetime.utcnow(),
        "sent_at": datetime.utcnow() if status == "sent" else None,
        "created_at": datetime.utcnow(),
    }
    if gmail_thread_id:
        doc["gmail_thread_id"] = gmail_thread_id
    if provider:
        doc["provider"] = provider
    if ab_variant:
        doc["ab_variant"] = ab_variant
    await database.campaign_messages_collection.insert_one(doc)

    # Update campaign counter
    counter_map = {
        "email": "emails_sent",
        "connection_request": "linkedin_connections_sent",
        "inmail": "linkedin_inmails_sent",
        "linkedin_message": "linkedin_replies",
    }
    counter = counter_map.get(step_def["action"])
    if counter and status == "sent":
        await database.campaigns_collection.update_one(
            {"_id": campaign["_id"]},
            {"$inc": {counter: 1}},
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
    from services.daily_cap_service import reserve_slot, release_slot, reserve_sender_slot, release_sender_slot as _release_sender_slot
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

    # Re-check channel availability at execution time (sender may have disconnected)
    channel_available = False
    if channel == "email":
        email_account_id = campaign.get("email_account_id")
        if email_account_id:
            email_acc = await database.email_accounts_collection.find_one(
                {"_id": ObjectId(str(email_account_id))},
                {"status": 1},
            )
            channel_available = bool(email_acc and email_acc.get("status") in ("connected", "active"))
    elif channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message"):
        linkedin_account_id = campaign.get("linkedin_account_id")
        if linkedin_account_id:
            li_acc = await database.linkedin_accounts_collection.find_one(
                {"_id": ObjectId(str(linkedin_account_id))},
                {"unipile_status": 1},
            )
            channel_available = bool(li_acc and li_acc.get("unipile_status") in ("OK", "CONNECTING"))

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

    # Reserve a daily cap slot (campaign-level)
    slot_reserved = await reserve_slot(database.db, str(enrollment["campaign_id"]), channel)
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
    _sender_id = None
    if channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message"):
        _sender_id = str(campaign.get("linkedin_account_id") or "")
    elif channel == "email":
        _sender_id = str(campaign.get("email_account_id") or "")
    if _sender_id:
        sender_slot_reserved = await reserve_sender_slot(database.db, _sender_id, channel)
        if not sender_slot_reserved:
            await release_slot(database.db, str(enrollment["campaign_id"]), channel)
            from datetime import timezone as _tz, timedelta as _td
            tomorrow = datetime.now(_tz.utc) + _td(days=1)
            tomorrow = tomorrow.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=None)
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
            # A/B subject selection: randomly pick subject_a or subject_b when both exist
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

            email_account_id = campaign.get("email_account_id")
            if not email_account_id:
                logger.warning(f"Smart campaign {campaign['_id']} has no email_account_id")
                return
            email_account = await database.email_accounts_collection.find_one(
                {"_id": ObjectId(str(email_account_id))}
            )
            if not email_account:
                logger.warning(f"Email account {email_account_id} not found")
                return
            to_email = prospect.get("email", "")
            if not to_email:
                logger.warning(f"Prospect {prospect['_id']} has no email for smart campaign email send")
                return

            # Generate open-tracking token for non-SendGrid providers
            tracking_token = None
            if email_account.get("provider", "sendgrid") != "sendgrid":
                try:
                    from routes.email_tracking import create_tracking_token
                    tracking_token = await create_tracking_token(
                        prospect_id=str(prospect["_id"]),
                        email_account_id=str(email_account_id),
                        campaign_id=str(campaign["_id"]),
                    )
                except Exception as tok_err:
                    logger.warning(f"Failed to create tracking token (smart): {tok_err}")

            send_result = await send_email_via_account(
                email_account,
                to_email,
                subject,
                body,
                tracking_token=tracking_token,
                prospect_id=str(prospect["_id"]),
                campaign_id=str(campaign["_id"]),
            )
            provider_message_id = send_result["message_id"] if send_result else None
            status = "sent" if provider_message_id else "failed"

        elif channel == "linkedin_connection":
            note = msgs.get("linkedin_connection", {}).get("note", "")
            body = note

            linkedin_account = await _get_linkedin_account_for_campaign(campaign, enrollment)
            if not linkedin_account:
                logger.warning(f"No LinkedIn account for smart campaign {campaign['_id']}")
                return

            unipile = get_unipile_service()
            linkedin_url = prospect.get("linkedin", "") or prospect.get("linkedin_url", "")
            if not linkedin_url:
                logger.warning(f"Prospect {prospect['_id']} has no LinkedIn URL for connection request")
                return

            result = await unipile.send_connection_request_async(
                provider_id=linkedin_account.get("profile_id", ""),
                profile_url=linkedin_url,
                message=note,
            )
            provider_message_id = str(result) if result else None
            status = "sent" if result else "failed"

            if status == "sent":
                await database.prospects_collection.update_one(
                    {"_id": prospect["_id"]},
                    {"$set": {"connection_request_sent_at": now}},
                )

        elif channel == "linkedin_inmail":
            inmail = msgs.get("linkedin_inmail", {})
            subject = inmail.get("subject", "")
            body = inmail.get("body", "")

            linkedin_account = await _get_linkedin_account_for_campaign(campaign, enrollment)
            if not linkedin_account:
                logger.warning(f"No LinkedIn account for smart campaign {campaign['_id']}")
                return

            unipile = get_unipile_service()
            linkedin_url = prospect.get("linkedin", "") or prospect.get("linkedin_url", "")
            if not linkedin_url:
                logger.warning(f"Prospect {prospect['_id']} has no LinkedIn URL for InMail")
                return

            result = await unipile.send_inmail_async(
                provider_id=linkedin_account.get("profile_id", ""),
                profile_url=linkedin_url,
                subject=subject,
                message=body,
            )
            provider_message_id = str(result) if result else None
            status = "sent" if result else "failed"

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
    }
    _channel_to_msg_channel = {
        "email": "email",
        "linkedin_connection": "linkedin",
        "linkedin_inmail": "linkedin",
    }
    step_def = {
        "channel": _channel_to_msg_channel.get(channel, "email"),
        "action": _channel_to_action.get(channel, "email"),
    }

    await record_campaign_message(
        enrollment, campaign, step_def, body, subject, provider_message_id, status,
        ab_variant=locals().get("_ab_variant"),
    )

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
        # On failure: release cap slot and transition flow state via send_failed
        await release_slot(database.db, str(enrollment["campaign_id"]), channel)
        if _sender_id:
            await _release_sender_slot(database.db, _sender_id, channel)
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
    """Fetch the LinkedIn account for a campaign, falling back to account default."""
    linkedin_account_id = campaign.get("linkedin_account_id")
    if linkedin_account_id:
        return await database.linkedin_accounts_collection.find_one(
            {"_id": ObjectId(str(linkedin_account_id))}
        )
    return await database.linkedin_accounts_collection.find_one(
        {"account_id": enrollment["account_id"], "is_default": True}
    )


async def execute_enrollment(enrollment: dict, campaign: dict, prospect: dict):
    """Execute the current step for one enrollment."""
    now = datetime.utcnow()

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
    provider_message_id = None
    gmail_thread_id = None
    send_provider = None
    status = "failed"

    try:
        if action == "email":
            email_account_id = campaign.get("email_account_id")
            if not email_account_id:
                logger.warning(f"Campaign {campaign['_id']} has no email_account_id")
                return
            email_account = await database.email_accounts_collection.find_one(
                {"_id": ObjectId(str(email_account_id))}
            )
            if not email_account:
                logger.warning(f"Email account {email_account_id} not found")
                return

            to_email = prospect.get("email", "")
            if not to_email:
                logger.warning(f"Prospect {prospect['_id']} has no email")
                return

            # For non-SendGrid providers, generate a tracking token for open detection
            tracking_token = None
            provider = email_account.get("provider", "sendgrid")
            if provider != "sendgrid":
                try:
                    from routes.email_tracking import create_tracking_token
                    tracking_token = await create_tracking_token(
                        prospect_id=str(prospect["_id"]),
                        email_account_id=str(email_account_id),
                        campaign_id=str(campaign["_id"]),
                    )
                except Exception as tok_err:
                    logger.warning(f"Failed to create tracking token: {tok_err}")

            send_result = await send_email_via_account(
                email_account,
                to_email,
                subject,
                body,
                tracking_token=tracking_token,
                prospect_id=str(prospect["_id"]),
                campaign_id=str(campaign["_id"]),
            )
            provider_message_id = send_result["message_id"] if send_result else None
            gmail_thread_id = send_result.get("thread_id") if send_result else None
            send_provider = send_result.get("provider") if send_result else None
            status = "sent" if provider_message_id else "failed"

        elif action in ("connection_request", "inmail", "linkedin_message"):
            linkedin_account_id = campaign.get("linkedin_account_id")
            if linkedin_account_id:
                linkedin_account = await database.linkedin_accounts_collection.find_one(
                    {"_id": ObjectId(str(linkedin_account_id))}
                )
            else:
                linkedin_account = await database.linkedin_accounts_collection.find_one(
                    {"account_id": enrollment["account_id"], "is_default": True}
                )

            if not linkedin_account:
                logger.warning(f"No LinkedIn account for campaign {campaign['_id']}")
                return

            unipile = get_unipile_service()
            linkedin_url = prospect.get("linkedin", "") or prospect.get("linkedin_url", "")

            if action == "connection_request":
                connection_note = personalize(
                    step_def.get("connection_note_template", body), prospect, sender_context
                )
                result = await unipile.send_connection_request_async(
                    provider_id=linkedin_account.get("profile_id", ""),
                    profile_url=linkedin_url,
                    message=connection_note,
                )
                provider_message_id = str(result) if result else None
                status = "sent" if result else "failed"
                if status == "sent":
                    await database.prospects_collection.update_one(
                        {"_id": enrollment["prospect_id"]},
                        {"$set": {"connection_request_sent_at": datetime.utcnow()}},
                    )

            elif action == "inmail":
                inmail_subject = personalize(
                    step_def.get("inmail_subject_template", subject), prospect, sender_context
                )
                inmail_body = personalize(
                    step_def.get("inmail_body_template", body), prospect, sender_context
                )
                result = await unipile.send_inmail_async(
                    provider_id=linkedin_account.get("profile_id", ""),
                    profile_url=linkedin_url,
                    subject=inmail_subject,
                    message=inmail_body,
                )
                provider_message_id = str(result) if result else None
                status = "sent" if result else "failed"

            elif action == "linkedin_message":
                result = await unipile.send_message_async(
                    provider_id=linkedin_account.get("profile_id", ""),
                    profile_url=linkedin_url,
                    message=body,
                )
                provider_message_id = str(result) if result else None
                status = "sent" if result else "failed"

    except Exception as e:
        logger.error(
            f"Error executing step for enrollment {enrollment['_id']}: {e}",
            exc_info=True,
        )
        status = "failed"

    # Record the message outcome
    await record_campaign_message(
        enrollment, campaign, step_def, body, subject, provider_message_id, status,
        gmail_thread_id=gmail_thread_id,
        provider=send_provider,
    )

    # Advance to next step if successful; otherwise track failure
    if status == "sent":
        await advance_enrollment(enrollment, campaign)
    else:
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
    For each cascade_waiting enrollment whose cascade_activate_at has passed,
    check if the primary prospect in the same group has replied.
    If not replied → activate the cascade enrollment.
    """
    due_cascade = await database.campaign_enrollments_collection.find({
        "cascade_status": "waiting",
        "cascade_activate_at": {"$lte": now},
    }).to_list(length=100)

    if not due_cascade:
        return

    logger.info(f"Cascade check: {len(due_cascade)} waiting cascade enrollments")

    for enrollment in due_cascade:
        group_id = enrollment.get("cascade_group_id")
        if not group_id:
            continue

        # Check if the primary (position=0) in this group has replied
        primary = await database.campaign_enrollments_collection.find_one({
            "cascade_group_id": group_id,
            "cascade_position": 0,
        })

        if primary and primary.get("status") in ("replied", "completed", "opted_out"):
            # Primary replied — mark this cascade as dormant (won't need outreach)
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {"cascade_status": "dormant", "status": "opted_out", "last_activity_at": now}},
            )
            logger.info(f"Cascade dormant (primary replied): group={group_id} pos={enrollment.get('cascade_position')}")
        else:
            # Primary has NOT replied → activate this prospect
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment["_id"]},
                {"$set": {
                    "status": "active",
                    "cascade_status": "activated",
                    "next_action_at": now,
                    "last_activity_at": now,
                }},
            )
            logger.info(f"Cascade activated: group={group_id} pos={enrollment.get('cascade_position')}")




async def execute_pending_steps():
    """
    Main campaign engine loop. Called every 5 minutes by APScheduler.
    Finds all active enrollments with next_action_at <= now and executes them.
    """
    now = datetime.utcnow()
    logger.info(f"Campaign engine running at {now.isoformat()}")

    try:
        # Find due enrollments — uses the critical compound index (sorted oldest-due first
        # to drain backlog fairly and prevent newer campaigns from starving old ones).
        due_cursor = database.campaign_enrollments_collection.find({
            "status": "active",
            "next_action_at": {"$lte": now},
        }).sort("next_action_at", 1).limit(200)  # Process max 200 per run to stay within the 5-min window

        due_enrollments = await due_cursor.to_list(length=200)

        if not due_enrollments:
            logger.info("Campaign engine: no due enrollments")
            return

        logger.info(f"Campaign engine: {len(due_enrollments)} enrollments due")

        # Activate cascade enrollments whose primary prospect hasn't replied
        await _activate_due_cascade_enrollments(now)

        # Batch-load campaigns and prospects
        campaign_ids = []
        for c in {e["campaign_id"] for e in due_enrollments}:
            try:
                campaign_ids.append(ObjectId(str(c)))
            except Exception:
                pass
                
        prospect_ids = []
        for p in {e["prospect_id"] for e in due_enrollments}:
            try:
                prospect_ids.append(ObjectId(str(p)))
            except Exception:
                pass

        campaigns_list = await database.campaigns_collection.find(
            {"_id": {"$in": campaign_ids}, "status": "active"}
        ).to_list(length=len(campaign_ids))
        campaigns_by_id = {str(c["_id"]): c for c in campaigns_list}

        prospects_list = await database.prospects_collection.find(
            {"_id": {"$in": prospect_ids}}
        ).to_list(length=len(prospect_ids))
        prospects_by_id = {str(p["_id"]): p for p in prospects_list}

        # Execute each enrollment
        for enrollment in due_enrollments:
            campaign = campaigns_by_id.get(str(enrollment["campaign_id"]))
            if not campaign:
                continue  # Campaign paused or archived

            prospect = prospects_by_id.get(str(enrollment["prospect_id"]))
            if not prospect:
                continue

            # Respect the campaign's send window, tailored for prospect timezone if available
            if not is_in_send_window(campaign, prospect):
                next_window = get_next_send_window(campaign, prospect)
                await database.campaign_enrollments_collection.update_one(
                    {"_id": enrollment["_id"]},
                    {"$set": {"next_action_at": next_window}},
                )
                continue

            await execute_enrollment(enrollment, campaign, prospect)

    except Exception as e:
        logger.error(f"Campaign engine error: {e}", exc_info=True)


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
                "campaign_id": campaign_id,
                "account_id": campaign["account_id"],
                "date": yesterday,
                "emails_sent": emails_sent,
                "emails_replied": emails_replied,
                "linkedin_sent": linkedin_sent,
                "new_enrollments": new_enrollments,
                "computed_at": datetime.utcnow(),
            }

            await database.campaign_daily_stats_collection.update_one(
                {"campaign_id": campaign_id, "date": yesterday},
                {"$set": stat_doc},
                upsert=True,
            )

    except Exception as e:
        logger.error(f"Daily stats error: {e}", exc_info=True)
