"""
Webhook Processing Service.
Handles inbound emails (SendGrid), delivery events (SendGrid), and LinkedIn messages (Unipile).
Separated from routes for testability.
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId

from database import prospects_collection, conversations_collection, campaign_enrollments_collection, campaigns_collection
from models.conversation import Message
from services.conversation_service import (
    get_or_create_conversation,
    add_message_to_conversation,
    update_message_status,
    find_prospect_by_email,
)

logger = logging.getLogger(__name__)


def _random_delay_for_channel(channel: str) -> timedelta:
    """Return a random reply delay based on channel.
    LinkedIn: 45–60s (fast enough for human-like feel)
    Email: 2–3 min (120–180s)
    """
    if channel == "linkedin":
        return timedelta(seconds=random.uniform(45, 60))
    return timedelta(seconds=random.uniform(120, 180))


# SendGrid event type → our status mapping
SENDGRID_EVENT_MAP = {
    "delivered": "delivered",
    "open": "opened",
    "click": "clicked",
    "bounce": "bounced",
    "dropped": "failed",
    "spamreport": "bounced",
    "deferred": "sent",  # Still trying to deliver
}


async def process_sendgrid_inbound_email(
    from_email: str,
    to_email: str,
    subject: str,
    text: Optional[str] = None,
    html: Optional[str] = None,
    headers: Optional[str] = None,
) -> dict:
    """
    Process an inbound email from SendGrid Inbound Parse.

    1. Parse from_email to extract email address
    2. Match to prospect
    3. Get or create email conversation
    4. Add inbound message
    5. Stop active followup sequence
    6. Update prospect status
    """
    # Parse from_email (may be "Name <email@domain.com>" format)
    email_addr = from_email
    if "<" in from_email and ">" in from_email:
        email_addr = from_email.split("<")[1].split(">")[0]
    email_addr = email_addr.strip().lower()

    logger.info(f"Processing inbound email from {email_addr}, subject: {subject}")

    # Match sender to prospect
    prospect = await find_prospect_by_email(email_addr)
    if not prospect:
        logger.info(f"No prospect found for inbound email from {email_addr}")
        return {"status": "ignored", "reason": "unknown_sender", "email": email_addr}

    prospect_id = str(prospect["_id"])
    prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
    prospect_company = prospect.get("company_name")

    # Extract In-Reply-To header for threading
    in_reply_to = None
    if headers:
        for line in headers.split("\n"):
            if line.lower().startswith("in-reply-to:"):
                in_reply_to = line.split(":", 1)[1].strip()
                break

    # Look up existing conversation to inherit account_id (for reply threading)
    existing_email_conv = await conversations_collection.find_one(
        {"prospect_email": email_addr, "channel": "email"},
        {"account_id": 1},
        sort=[("updated_at", -1)],
    )
    inherited_account_id = existing_email_conv.get("account_id") if existing_email_conv else None

    # Get or create conversation
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="email",
        prospect_name=prospect_name,
        prospect_email=email_addr,
        prospect_company=prospect_company,
        email_thread_subject=subject,
        account_id=inherited_account_id,
    )

    # Add inbound message
    msg = Message(
        direction="inbound",
        content_text=text,
        content_html=html,
        subject=subject,
        status="received",
        email_in_reply_to=in_reply_to,
    )

    conv_id = str(conversation["_id"])
    await add_message_to_conversation(conv_id, msg)

    # Mark conversation for classifier processing with scheduled reply delay
    try:
        scheduled_at = datetime.utcnow() + _random_delay_for_channel("email")
        await conversations_collection.update_one(
            {"_id": conversation["_id"]},
            {"$set": {
                "needs_classification": True,
                "scheduled_reply_at": scheduled_at,
            }},
        )
    except Exception as e:
        logger.warning(f"Failed to set classifier flag for email reply {conv_id}: {e}")

    # Transition flow_state for any flow-based smart campaign enrollment
    try:
        from services import flow_engine as _flow_engine
        _enrollment = await campaign_enrollments_collection.find_one({
            "prospect_id": ObjectId(prospect_id),
            "status": {"$in": ["active", "enrolled"]},
        })
        if not _enrollment:
            _enrollment = await campaign_enrollments_collection.find_one({
                "prospect_id": prospect_id,
                "status": {"$in": ["active", "enrolled"]},
            })
        if _enrollment and _enrollment.get("flow_state"):
            _campaign_doc = await campaigns_collection.find_one(
                {"_id": _enrollment.get("campaign_id")},
                {"follow_up_flow": 1},
            )
            _flow = (_campaign_doc or {}).get("follow_up_flow")
            if _flow:
                _new_state = _flow_engine.transition(
                    _enrollment["flow_state"], _flow, "replied", prospect
                )
                await campaign_enrollments_collection.update_one(
                    {"_id": _enrollment["_id"]},
                    {"$set": {
                        "flow_state": _new_state,
                        "status": "replied",
                        "next_action_at": None,
                        "completed_at": datetime.utcnow(),
                    }},
                )
                await campaigns_collection.update_one(
                    {"_id": _enrollment.get("campaign_id")},
                    {"$inc": {"replied_count": 1, "active_count": -1}},
                )
    except Exception as _fe:
        logger.warning(f"Failed to update flow_state on email reply for {prospect_id}: {_fe}")

    # Update prospect status
    await prospects_collection.update_one(
        {"_id": ObjectId(prospect_id)},
        {
            "$set": {
                "status": "replied",
                "last_updated_at": datetime.utcnow(),
            },
            "$push": {
                "outreach_history": {
                    "event": "email_reply_received",
                    "channel": "email",
                    "timestamp": datetime.utcnow(),
                    "subject": subject,
                    "preview": (text or "")[:100],
                }
            },
        },
    )

    # Create notification for email reply
    try:
        from services.notification_service import create_notification
        sender_label = prospect_name or email_addr
        await create_notification(
            type="email_reply",
            title=f"Email reply from {sender_label}",
            body=(text or "")[:200] or f"Re: {subject}",
            prospect_id=prospect_id,
            conversation_id=conv_id,
            channel="email",
        )
    except Exception as e:
        logger.warning(f"Failed to create notification for email reply: {e}")

    logger.info(f"Inbound email processed for prospect {prospect_id} from {email_addr}")
    return {"status": "processed", "prospect_id": prospect_id, "conversation_id": conv_id}


async def _resolve_prospect_id(event: dict) -> Optional[str]:
    """
    Resolve prospect_id from a SendGrid event.
    First tries custom_args, then falls back to matching by recipient email.
    """
    # Try custom_args first
    prospect_id = event.get("prospect_id")
    if prospect_id:
        return prospect_id

    # Fall back to email match
    recipient_email = event.get("email", "").strip().lower()
    if recipient_email:
        prospect = await find_prospect_by_email(recipient_email)
        if prospect:
            return str(prospect["_id"])

    return None


async def process_sendgrid_events(events: list[dict]) -> dict:
    """
    Process SendGrid delivery/engagement events.

    For each event:
    1. Extract sg_message_id, event type, prospect_id (or match by email)
    2. Map event type to our status
    3. Update message status in conversations
    4. Update prospect-level tracking (opens, bounces, etc.)
    """
    processed = 0
    errors = 0

    for event in events:
        try:
            event_type = event.get("event", "")
            sg_message_id = event.get("sg_message_id", "")

            # Map event type
            new_status = SENDGRID_EVENT_MAP.get(event_type)
            if not new_status:
                continue

            # Update message status in conversations
            if sg_message_id:
                await update_message_status(sg_message_id, new_status)

            # Resolve prospect_id (custom_args or email match)
            prospect_id = await _resolve_prospect_id(event)

            if prospect_id and event_type in ("bounce", "spamreport", "dropped"):
                try:
                    await prospects_collection.update_one(
                        {"_id": ObjectId(prospect_id)},
                        {
                            "$set": {
                                "status": "bounced" if event_type == "bounce" else "disqualified",
                                "last_updated_at": datetime.utcnow(),
                            },
                            "$push": {
                                "outreach_history": {
                                    "event": f"email_{event_type}",
                                    "channel": "email",
                                    "timestamp": datetime.utcnow(),
                                    "detail": event.get("reason", ""),
                                }
                            },
                        },
                    )
                except Exception:
                    pass

                # Transition flow_state for smart campaign enrollments on bounce
                if event_type == "bounce":
                    try:
                        from services import flow_engine as _flow_engine
                        _enr = await campaign_enrollments_collection.find_one({
                            "prospect_id": ObjectId(prospect_id),
                            "status": {"$in": ["active", "enrolled"]},
                        })
                        if _enr and _enr.get("flow_state"):
                            _camp = await campaigns_collection.find_one(
                                {"_id": _enr.get("campaign_id")}, {"follow_up_flow": 1}
                            )
                            _fl = (_camp or {}).get("follow_up_flow")
                            if _fl:
                                _ns = _flow_engine.transition(_enr["flow_state"], _fl, "bounced", {})
                                await campaign_enrollments_collection.update_one(
                                    {"_id": _enr["_id"]},
                                    {"$set": {"flow_state": _ns, "next_action_at": None}},
                                )
                    except Exception as _bfe:
                        logger.warning(f"Failed to transition flow_state on bounce for {prospect_id}: {_bfe}")

            # For open events: update prospect open tracking + campaign counters
            if prospect_id and event_type == "open":
                variant_id = event.get("variant_id")
                now = datetime.utcnow()
                update: dict = {
                    "$set": {
                        "last_outreach_opened": now,
                        "last_updated_at": now,
                    },
                    "$push": {
                        "outreach_history": {
                            "event": "email_opened",
                            "channel": "email",
                            "timestamp": now,
                            "variant_id": variant_id,
                        }
                    },
                }
                try:
                    await prospects_collection.update_one(
                        {"_id": ObjectId(prospect_id)}, update
                    )
                except Exception:
                    pass

                # Update campaign_messages status and campaign counter
                if sg_message_id:
                    try:
                        from database import campaign_messages_collection, campaigns_collection, campaign_daily_stats_collection
                        msg_doc = await campaign_messages_collection.find_one(
                            {"provider_message_id": sg_message_id},
                            {"_id": 1, "campaign_id": 1, "account_id": 1, "status": 1},
                        )
                        if msg_doc and msg_doc.get("status") not in ("opened", "clicked", "replied"):
                            await campaign_messages_collection.update_one(
                                {"_id": msg_doc["_id"]},
                                {"$set": {"status": "opened", "opened_at": now}},
                            )
                            camp_id = msg_doc.get("campaign_id")
                            if camp_id:
                                await campaigns_collection.update_one(
                                    {"_id": camp_id},
                                    {"$inc": {"emails_opened": 1}},
                                )
                                today_str = now.strftime("%Y-%m-%d")
                                await campaign_daily_stats_collection.update_one(
                                    {"campaign_id": str(camp_id), "date": today_str},
                                    {
                                        "$inc": {"emails_opened": 1},
                                        "$setOnInsert": {
                                            "account_id": msg_doc.get("account_id"),
                                            "emails_sent": 0,
                                            "emails_replied": 0,
                                            "linkedin_connections_sent": 0,
                                            "linkedin_connections_accepted": 0,
                                        },
                                    },
                                    upsert=True,
                                )
                    except Exception as e:
                        logger.warning(f"Failed to update campaign open counter: {e}")

            # For delivered events: update prospect delivery tracking
            if prospect_id and event_type == "delivered":
                try:
                    await prospects_collection.update_one(
                        {"_id": ObjectId(prospect_id)},
                        {
                            "$set": {
                                "email_delivered": True,
                                "last_updated_at": datetime.utcnow(),
                            },
                        },
                    )
                except Exception:
                    pass

            processed += 1

        except Exception as e:
            logger.error(f"Error processing SendGrid event: {e}")
            errors += 1

    logger.info(f"Processed {processed} SendGrid events ({errors} errors)")
    return {"processed": processed, "errors": errors}


async def process_unipile_webhook(payload: dict) -> dict:
    """
    Process a Unipile webhook for incoming LinkedIn messages.

    1. Check event_type == "message_received"
    2. Extract chat_id, message text, sender info
    3. Look up conversation by unipile_chat_id
    4. If no conversation, try to match sender to prospect
    5. Add inbound message to conversation
    6. Stop active followup sequence, update prospect status
    """
    event_type = payload.get("event") or payload.get("event_type", "")
    if event_type != "message_received":
        logger.debug(f"Ignoring Unipile event type: {event_type}")
        return {"status": "ignored", "reason": f"event_type={event_type}"}

    # Extract message data
    message_data = payload.get("message") or payload.get("data", {}).get("message", {})
    chat_id = payload.get("chat_id") or message_data.get("chat_id")
    message_text = message_data.get("text", "")
    sender_info = message_data.get("sender", {})
    unipile_message_id = message_data.get("id")

    if not chat_id:
        logger.warning("Unipile webhook missing chat_id")
        return {"status": "ignored", "reason": "no_chat_id"}

    logger.info(f"Processing Unipile message in chat {chat_id}")

    # Look up conversation by chat_id
    conversation = await conversations_collection.find_one({"unipile_chat_id": chat_id})

    prospect_id = None
    prospect_name = None
    prospect_company = None

    if conversation:
        prospect_id = conversation.get("prospect_id")
    else:
        # Try to match sender to prospect via LinkedIn URL
        sender_url = sender_info.get("profile_url") or sender_info.get("linkedin_url", "")
        if sender_url:
            prospect = await prospects_collection.find_one({"linkedin": {"$regex": sender_url, "$options": "i"}})
            if prospect:
                prospect_id = str(prospect["_id"])
                prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
                prospect_company = prospect.get("company_name")

    if not prospect_id:
        logger.info(f"No prospect found for Unipile chat {chat_id}")
        return {"status": "ignored", "reason": "unknown_sender", "chat_id": chat_id}

    # Get or create conversation
    if not conversation:
        # Try to inherit account_id from another conversation with this prospect
        prior_conv = await conversations_collection.find_one(
            {"prospect_id": prospect_id, "account_id": {"$ne": None}},
            {"account_id": 1},
        ) if prospect_id else None
        inherited_account_id = prior_conv.get("account_id") if prior_conv else None
        conversation = await get_or_create_conversation(
            prospect_id=prospect_id,
            channel="linkedin",
            prospect_name=prospect_name,
            prospect_company=prospect_company,
            unipile_chat_id=chat_id,
            account_id=inherited_account_id,
        )

    # Add inbound message
    msg = Message(
        direction="inbound",
        content_text=message_text,
        status="received",
        unipile_message_id=unipile_message_id,
    )

    conv_id = str(conversation["_id"])
    await add_message_to_conversation(conv_id, msg)

    # Mark conversation for classifier processing with scheduled reply delay
    try:
        scheduled_at = datetime.utcnow() + _random_delay_for_channel("linkedin")
        await conversations_collection.update_one(
            {"_id": conversation["_id"]},
            {"$set": {
                "needs_classification": True,
                "scheduled_reply_at": scheduled_at,
            }},
        )
    except Exception as e:
        logger.warning(f"Failed to set classifier flag for LinkedIn reply {conv_id}: {e}")

    # Update flow_state for any flow-based campaign enrollment
    try:
        from services import flow_engine as _flow_engine
        _enrollment = await campaign_enrollments_collection.find_one({
            "prospect_id": prospect_id,
            "status": {"$in": ["active", "enrolled"]},
        })
        if not _enrollment:
            # Also try with ObjectId prospect_id
            _enrollment = await campaign_enrollments_collection.find_one({
                "prospect_id": ObjectId(prospect_id),
                "status": {"$in": ["active", "enrolled"]},
            })
        if _enrollment:
            _flow = None
            _campaign_doc = await campaigns_collection.find_one(
                {"_id": ObjectId(str(_enrollment.get("campaign_id", "")))},
                {"follow_up_flow": 1},
            )
            if _campaign_doc:
                _flow = _campaign_doc.get("follow_up_flow")
            if _flow and _enrollment.get("flow_state"):
                _prospect_doc = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
                _new_state = _flow_engine.transition(
                    _enrollment["flow_state"], _flow, "replied", _prospect_doc or {}
                )
                await campaign_enrollments_collection.update_one(
                    {"_id": _enrollment["_id"]},
                    {"$set": {
                        "flow_state": _new_state,
                        "status": "replied",
                        "next_action_at": None,
                        "completed_at": datetime.utcnow(),
                    }},
                )
    except Exception as _fe:
        logger.warning(f"Failed to update flow_state on reply for {prospect_id}: {_fe}")

    # Update prospect status
    try:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {
                "$set": {
                    "status": "replied",
                    "last_updated_at": datetime.utcnow(),
                },
                "$push": {
                    "outreach_history": {
                        "event": "linkedin_reply_received",
                        "channel": "linkedin",
                        "timestamp": datetime.utcnow(),
                        "preview": message_text[:100],
                    }
                },
            },
        )
    except Exception as e:
        logger.warning(f"Failed to update prospect status for {prospect_id}: {e}")

    # Create notification for LinkedIn reply
    try:
        from services.notification_service import create_notification
        sender_label = prospect_name or f"Prospect {prospect_id[:8]}"
        await create_notification(
            type="linkedin_reply",
            title=f"LinkedIn message from {sender_label}",
            body=message_text[:200] if message_text else "New LinkedIn message",
            prospect_id=prospect_id,
            conversation_id=conv_id,
            channel="linkedin",
        )
    except Exception as e:
        logger.warning(f"Failed to create notification for LinkedIn reply: {e}")

    logger.info(f"Unipile message processed for prospect {prospect_id} in chat {chat_id}")
    return {"status": "processed", "prospect_id": prospect_id, "conversation_id": conv_id}
