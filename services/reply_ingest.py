"""
Shared reply-ingestion logic used by the unified email reply poller
(services/email_reply_poller.py). Provider-agnostic: takes a
services.email_providers.base.ReplyMeta plus the original outbound
campaign_messages doc and fans the reply out to every downstream system
(campaign counters, prospect status, enrollment status, conversations inbox,
notifications).

Extracted from the original Gmail-only poller (services/gmail_reply_poller.py)
verbatim in behavior — only the reply-metadata shape changed from a raw Gmail
dict to the provider-agnostic ReplyMeta dataclass.
"""

import logging
import uuid
from datetime import datetime, timedelta

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

import database
from models.conversation import Message
from services.email_providers.base import ReplyMeta
from services.conversation_service import (
    _account_filter,
    add_message_to_conversation,
    get_or_create_conversation,
)

logger = logging.getLogger(__name__)


async def _claim_reply_once(
    message_id,
    account_id,
    provider_account_id,
    provider_message_id: str,
    now: datetime,
) -> bool:
    """Atomically claim one provider reply before any fan-out side effects.

    Provider message ids are not globally unique.  The stored replay key binds
    the event to both the tenant and sender mailbox, and the conditional update
    makes concurrent poller runs idempotent.
    """
    if account_id is None or not str(account_id).strip():
        raise ValueError("Reply ingest requires account_id")
    if provider_account_id is None or not str(provider_account_id).strip():
        raise ValueError("Reply ingest requires provider_account_id")
    if not provider_message_id:
        raise ValueError("Reply ingest requires provider_message_id")

    replay_key = f"{account_id}:{provider_account_id}:{provider_message_id}"
    try:
        result = await database.campaign_messages_collection.update_one(
            {
                "_id": message_id,
                **_account_filter(account_id),
                "processed_reply_keys": {"$ne": replay_key},
            },
            {
                "$set": {"status": "replied", "replied_at": now},
                "$addToSet": {"processed_reply_keys": replay_key},
            },
        )
    except DuplicateKeyError:
        # The unique multikey replay index proves another campaign-message row
        # already claimed this tenant/provider event.
        return False
    return result.modified_count > 0


async def process_reply(
    original_msg: dict,
    reply_meta: ReplyMeta,
    reply_body: str,
    now: datetime,
):
    """
    Record a confirmed reply:
    1. Update campaign_messages status
    2. Increment campaign.emails_replied
    3. Update campaign_daily_stats
    4. Update prospect status and outreach_history
    5. Update enrollment status
    6. Create/update conversation entry
    7. Create notification
    """
    msg_id = original_msg["_id"]
    prospect_id = original_msg.get("prospect_id")
    campaign_id = original_msg.get("campaign_id")
    account_id = original_msg.get("account_id")
    provider_account_id = original_msg.get("email_account_id")
    provider_thread_id = (
        reply_meta.thread_ref
        or original_msg.get("provider_thread_id")
        or original_msg.get("gmail_thread_id")
    )
    enrollment_id = (
        original_msg.get("campaign_enrollment_id")
        or original_msg.get("enrollment_id")
    )
    reply_from = reply_meta.from_email
    reply_snippet = (reply_meta.snippet or "")[:200]

    if not provider_thread_id:
        raise ValueError("Reply ingest requires provider_thread_id")

    # 1. Atomically claim the provider event. A replay returns before counters,
    # enrollment transitions, notifications, or conversation messages repeat.
    claimed = await _claim_reply_once(
        msg_id,
        account_id,
        provider_account_id,
        reply_meta.provider_message_id,
        now,
    )
    if not claimed:
        return {"status": "duplicate"}

    # 2. Increment campaign counter
    if campaign_id:
        await database.campaigns_collection.update_one(
            {"_id": campaign_id, **_account_filter(account_id)},
            {"$inc": {"emails_replied": 1}},
        )

        # 3. Campaign daily stats
        today_str = now.strftime("%Y-%m-%d")
        await database.campaign_daily_stats_collection.update_one(
            {
                "campaign_id": str(campaign_id),
                "date": today_str,
                **_account_filter(account_id),
            },
            {
                "$inc": {"emails_replied": 1},
                "$setOnInsert": {
                    "emails_sent": 0,
                    "emails_clicked": 0,
                    "linkedin_connections_sent": 0,
                    "linkedin_connections_accepted": 0,
                    "account_id": str(account_id),
                },
            },
            upsert=True,
        )

    # 4. Update prospect
    if prospect_id:
        try:
            await database.prospects_collection.update_one(
                {"_id": ObjectId(str(prospect_id))},
                {
                    "$set": {
                        "status": "replied",
                        "last_updated_at": now,
                    },
                    "$push": {
                        "outreach_history": {
                            "event": "email_reply_received",
                            "channel": "email",
                            "timestamp": now,
                            "preview": reply_snippet,
                            "from_email": reply_from,
                        }
                    },
                },
            )
        except Exception as e:
            logger.warning(f"Reply ingest: failed to update prospect {prospect_id}: {e}")

    # 5. Update enrollment status to "replied"
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment_id, **_account_filter(account_id)},
                {"$set": {"status": "replied", "replied_at": now, "last_activity_at": now}},
            )
            if campaign_id:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_id, **_account_filter(account_id)},
                    {"$inc": {"replied_count": 1, "active_count": -1}},
                )
        except Exception as e:
            logger.warning(f"Reply ingest: failed to update enrollment {enrollment_id}: {e}")

    # 5b. Stop any branching-sequence enrollment for this prospect (reply on ANY
    #     channel halts remaining touches; stamps sequence_state.stopped_reason).
    if prospect_id:
        try:
            from services.sequence_service import mark_sequence_replied
            await mark_sequence_replied(prospect_id, campaign_id=campaign_id)
        except Exception as e:
            logger.warning(f"Reply ingest: sequence stop failed for prospect {prospect_id}: {e}")

    # 6. Create/update conversation entry
    try:
        await _record_reply_in_conversation(
            prospect_id=prospect_id,
            account_id=account_id,
            campaign_id=campaign_id,
            reply_body=reply_body,
            reply_from=reply_from,
            original_subject=original_msg.get("subject", ""),
            provider=original_msg.get("provider"),
            provider_account_id=str(provider_account_id),
            provider_thread_id=str(provider_thread_id),
            provider_message_id=reply_meta.provider_message_id,
            now=now,
        )
    except Exception as e:
        logger.warning(f"Reply ingest: failed to record conversation for prospect {prospect_id}: {e}")

    # 7. Create notification
    try:
        prospect_name = ""
        if prospect_id:
            p = await database.prospects_collection.find_one(
                {"_id": ObjectId(str(prospect_id))},
                {"full_name": 1, "first_name": 1},
            )
            if p:
                prospect_name = p.get("full_name") or p.get("first_name", reply_from)

        from services.notification_service import create_notification

        await create_notification(
            account_id=str(account_id),
            type="email_reply",
            title=f"{prospect_name or reply_from} replied to your email",
            body=reply_snippet,
            prospect_id=str(prospect_id) if prospect_id else None,
            campaign_id=str(campaign_id) if campaign_id else None,
            channel="email",
        )
    except Exception as e:
        logger.warning(f"Reply ingest: failed to create notification: {e}")

    return {"status": "processed"}


async def _record_reply_in_conversation(
    prospect_id,
    account_id,
    campaign_id,
    reply_body: str,
    reply_from: str,
    original_subject: str,
    provider,
    provider_account_id: str,
    provider_thread_id: str,
    provider_message_id: str,
    now: datetime,
):
    """Create or update a conversation document in the inbox with the reply."""
    if not prospect_id:
        return None
    if account_id is None or not str(account_id).strip():
        raise ValueError("Conversation reply ingest requires account_id")
    if not provider_account_id or not provider_thread_id:
        raise ValueError("Conversation reply ingest requires provider identity")

    prospect_id_str = str(prospect_id)

    # Look up prospect details
    prospect = await database.prospects_collection.find_one(
        {"_id": ObjectId(prospect_id_str)},
        {"full_name": 1, "email": 1, "company_name": 1},
    )
    prospect_email = (prospect or {}).get("email", reply_from)
    prospect_name = (prospect or {}).get("full_name", "")
    prospect_company = (prospect or {}).get("company_name", "")

    conversation = await get_or_create_conversation(
        prospect_id=prospect_id_str,
        channel="email",
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        prospect_company=prospect_company,
        email_thread_subject=original_subject,
        account_id=str(account_id),
        provider_account_id=provider_account_id,
        provider_thread_id=provider_thread_id,
    )

    inbound_message = Message(
        message_id=str(uuid.uuid4()),
        direction="inbound",
        content_text=reply_body,
        content_html=reply_body,
        subject=f"Re: {original_subject}",
        timestamp=now,
        status="received",
        provider=provider,
        provider_message_id=provider_message_id,
    )

    import random
    scheduled_at = now + timedelta(seconds=random.uniform(120, 180))  # email: 2-3 min delay

    updated = await add_message_to_conversation(
        str(conversation["_id"]),
        inbound_message,
        account_id=str(account_id),
        provider_account_id=provider_account_id,
    )
    if not updated:
        raise ValueError("Conversation no longer belongs to reply tenant/provider")

    await database.conversations_collection.update_one(
        {
            "_id": conversation["_id"],
            **_account_filter(account_id),
            "provider_account_id": provider_account_id,
            "provider_thread_id": provider_thread_id,
        },
        {"$set": {
            "needs_classification": True,
            "scheduled_reply_at": scheduled_at,
        }},
    )
    return updated
