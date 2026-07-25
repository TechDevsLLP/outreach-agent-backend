"""
Webhook Processing Service.
Handles inbound LinkedIn messages (Unipile). SendGrid inbound/event processing
has been removed — email reply detection is handled by the unified poller
(services/email_reply_poller.py) instead of provider webhooks.
Separated from routes for testability.
"""

import logging
import random
import re
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId

from database import (
    prospects_collection,
    conversations_collection,
    campaign_enrollments_collection,
    campaigns_collection,
    linkedin_accounts_collection,
)
from models.conversation import Message
from services.conversation_service import (
    _account_filter,
    add_message_to_conversation,
    get_or_create_conversation,
)
from services.unipile_service import UnipileClient

logger = logging.getLogger(__name__)


def _random_delay_for_channel(channel: str) -> timedelta:
    """Return a random reply delay based on channel.
    LinkedIn: 45–60s (fast enough for human-like feel)
    Email: 2–3 min (120–180s)
    """
    if channel == "linkedin":
        return timedelta(seconds=random.uniform(45, 60))
    return timedelta(seconds=random.uniform(120, 180))


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

    # Extract message data. Unipile has two payload shapes: the flat one puts
    # the body text directly in `message` (a string) with `message_id`, `sender`
    # and `chat_id` as siblings, while the nested one puts a message object
    # there. Treating the flat shape as an object raised
    # "'str' object has no attribute 'get'" and 500'd every inbound reply.
    raw_message = payload.get("message")
    if isinstance(raw_message, dict):
        message_data = raw_message
    elif isinstance(raw_message, str):
        message_data = {
            "text": raw_message,
            "id": payload.get("message_id") or payload.get("provider_message_id"),
            "chat_id": payload.get("chat_id"),
            "sender": payload.get("sender"),
            "is_sender": payload.get("is_sender"),
            "account_id": payload.get("account_id"),
        }
    else:
        nested = payload.get("data")
        nested_message = nested.get("message") if isinstance(nested, dict) else None
        message_data = nested_message if isinstance(nested_message, dict) else {}

    chat_id = payload.get("chat_id") or message_data.get("chat_id")
    message_text = message_data.get("text", "")
    unipile_message_id = message_data.get("id")
    if not unipile_message_id:
        logger.warning("Unipile webhook missing provider message id")
        return {"status": "ignored", "reason": "no_provider_message_id"}

    # Sender identity, used both to drop echoes of our own outbound sends and
    # to resolve the counterpart when no conversation exists yet (see below).
    sender_container = message_data.get("sender") if isinstance(message_data.get("sender"), dict) else {}
    sender_attendee_id = (
        message_data.get("sender_id")
        or message_data.get("sender_attendee_id")
        or sender_container.get("attendee_provider_id")
        or sender_container.get("provider_id")
        or sender_container.get("id")
    )
    is_own_message = message_data.get("is_sender")
    if is_own_message is None:
        is_own_message = payload.get("is_sender")

    # The HTTP route authenticates the provider signature before invoking this
    # service. Tenant identity must then come from the signed provider-account
    # metadata, never from a shared prospect or an existing unrelated thread.
    account_container = payload.get("account") or {}
    data_container = payload.get("data") or {}
    provider_account_candidates = {
        str(value)
        for value in (
            payload.get("account_id"),
            account_container.get("id") if isinstance(account_container, dict) else None,
            data_container.get("account_id") if isinstance(data_container, dict) else None,
            message_data.get("account_id") if isinstance(message_data, dict) else None,
        )
        if value is not None and str(value).strip()
    }
    if len(provider_account_candidates) != 1:
        logger.warning("Unipile webhook missing or conflicting provider account identity")
        return {"status": "ignored", "reason": "invalid_provider_account"}
    provider_account_id = provider_account_candidates.pop()

    linked_account = await linkedin_accounts_collection.find_one(
        {
            "unipile_account_id": provider_account_id,
            "unipile_status": {"$in": ["OK", "CONNECTING"]},
        },
        {"account_id": 1, "unipile_account_id": 1, "profile_id": 1},
    )
    if not linked_account or not linked_account.get("account_id"):
        logger.warning("Unipile webhook provider account is not linked to a tenant")
        return {"status": "ignored", "reason": "unlinked_provider_account"}
    account_id = str(linked_account["account_id"])

    # Drop echoes of our own outbound sends — only a message clearly sent by
    # the counterpart should ever be treated as a reply.
    own_provider_id = linked_account.get("profile_id")
    if is_own_message is True or (
        own_provider_id and sender_attendee_id and str(sender_attendee_id) == str(own_provider_id)
    ):
        logger.debug(f"Ignoring own-message echo in chat {chat_id}")
        return {"status": "ignored", "reason": "own_message"}

    if not chat_id:
        logger.warning("Unipile webhook missing chat_id")
        return {"status": "ignored", "reason": "no_chat_id"}

    logger.info(f"Processing Unipile message in chat {chat_id}")

    conversation = await conversations_collection.find_one(
        {
            **_account_filter(account_id),
            "channel": "linkedin",
            "provider_account_id": provider_account_id,
            "provider_thread_id": str(chat_id),
        }
    )
    if not conversation and sender_attendee_id:
        # First reply on a connection-request campaign: Unipile creates the
        # chat on the prospect's side, so no conversation row exists yet.
        # Resolve the sender to a prospect this tenant is actively engaging
        # and create the conversation instead of silently dropping the reply.
        conversation = await _create_conversation_for_unknown_thread(
            account_id=account_id,
            provider_account_id=provider_account_id,
            chat_id=str(chat_id),
            sender_attendee_id=str(sender_attendee_id),
        )

    if not conversation:
        logger.info("No tenant/provider-owned conversation for Unipile chat")
        return {"status": "ignored", "reason": "unknown_provider_thread"}

    prospect_id = conversation.get("prospect_id")
    prospect_name = conversation.get("prospect_name")
    if not prospect_id:
        return {"status": "ignored", "reason": "conversation_missing_prospect"}

    # Add inbound message
    msg = Message(
        direction="inbound",
        content_text=message_text,
        status="received",
        provider="unipile",
        unipile_message_id=unipile_message_id,
    )

    conv_id = str(conversation["_id"])
    appended = await add_message_to_conversation(
        conv_id,
        msg,
        account_id=account_id,
        provider_account_id=provider_account_id,
    )
    if appended and appended.get("_was_replay"):
        # Unipile redelivered a message we already processed — the message
        # itself was deduped, so skip re-arming classification/notifications.
        logger.info(f"Ignoring replayed Unipile message {unipile_message_id} in chat {chat_id}")
        return {"status": "ignored", "reason": "replayed_message", "prospect_id": prospect_id, "conversation_id": conv_id}

    # Mark conversation for classifier processing with scheduled reply delay
    _enrollment = None
    try:
        scheduled_at = datetime.utcnow() + _random_delay_for_channel("linkedin")
        await conversations_collection.update_one(
            {
                "_id": conversation["_id"],
                **_account_filter(account_id),
                "provider_account_id": provider_account_id,
                "provider_thread_id": str(chat_id),
            },
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
            **_account_filter(account_id),
            "prospect_id": prospect_id,
            "status": {"$in": ["active", "enrolled"]},
        })
        if not _enrollment:
            # Also try with ObjectId prospect_id
            _enrollment = await campaign_enrollments_collection.find_one({
                **_account_filter(account_id),
                "prospect_id": ObjectId(prospect_id),
                "status": {"$in": ["active", "enrolled"]},
            })
        if _enrollment:
            _flow = None
            _campaign_doc = await campaigns_collection.find_one(
                {
                    "_id": ObjectId(str(_enrollment.get("campaign_id", ""))),
                    **_account_filter(account_id),
                },
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
                    {"_id": _enrollment["_id"], **_account_filter(account_id)},
                    {"$set": {
                        "flow_state": _new_state,
                        "status": "replied",
                        "next_action_at": None,
                        "completed_at": datetime.utcnow(),
                    }},
                )
    except Exception as _fe:
        logger.warning(f"Failed to update flow_state on reply for {prospect_id}: {_fe}")

    # Stop any branching-sequence enrollment for this prospect (parallel to the
    # flow_state path above; sequence enrollments carry no flow_state).
    try:
        from services.sequence_service import mark_sequence_replied
        await mark_sequence_replied(
            prospect_id,
            campaign_id=_enrollment.get("campaign_id") if _enrollment else None,
        )
    except Exception as _se:
        logger.warning(f"Failed to stop sequence on LinkedIn reply for {prospect_id}: {_se}")

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
            account_id=account_id,
            type="linkedin_reply",
            title=f"LinkedIn message from {sender_label}",
            body=message_text[:200] if message_text else "New LinkedIn message",
            prospect_id=prospect_id,
            conversation_id=conv_id,
            channel="linkedin",
            campaign_id=(
                str(_enrollment.get("campaign_id"))
                if _enrollment and _enrollment.get("campaign_id")
                else None
            ),
        )
    except Exception as e:
        logger.warning(f"Failed to create notification for LinkedIn reply: {e}")

    logger.info(f"Unipile message processed for prospect {prospect_id} in chat {chat_id}")
    return {"status": "processed", "prospect_id": prospect_id, "conversation_id": conv_id}


async def _create_conversation_for_unknown_thread(
    account_id: str,
    provider_account_id: str,
    chat_id: str,
    sender_attendee_id: str,
) -> Optional[dict]:
    """Recover a first-reply chat Unipile created on the prospect's side (e.g.
    a connection-request campaign) with no matching conversation row yet.

    Resolves the sender to a prospect this tenant is actively engaging and
    creates the conversation; returns None if the sender can't be matched to
    an in-flight prospect for this tenant.
    """
    try:
        client = UnipileClient(account_id=provider_account_id)
        sender_profile = await client.get_profile(sender_attendee_id)
    except Exception as e:
        logger.warning(f"Failed to resolve sender profile for unknown chat {chat_id}: {e}")
        return None

    sender_public_id = sender_profile.get("public_identifier") or sender_profile.get("public_id")
    if not sender_public_id:
        return None

    prospect = await prospects_collection.find_one(
        {"linkedin": {"$regex": f"/in/{re.escape(sender_public_id)}/?$", "$options": "i"}}
    )
    if not prospect:
        return None

    prospect_id = str(prospect["_id"])
    tenant_values: list[object] = [account_id]
    try:
        tenant_values.append(ObjectId(account_id))
    except Exception:
        pass
    enrollment = await campaign_enrollments_collection.find_one(
        {
            "account_id": {"$in": tenant_values},
            "prospect_id": {"$in": [prospect_id, prospect["_id"]]},
            "status": {"$nin": ["opted_out", "bounced", "completed"]},
        },
        {"_id": 1},
    )
    if not enrollment:
        return None

    return await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="linkedin",
        prospect_name=prospect.get("full_name"),
        prospect_company=prospect.get("company_name"),
        account_id=account_id,
        provider_account_id=provider_account_id,
        provider_thread_id=chat_id,
    )
