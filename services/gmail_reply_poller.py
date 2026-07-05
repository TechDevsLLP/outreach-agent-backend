"""
Gmail Reply Poller — runs every 5 minutes via APScheduler.

Queries campaign_messages for Gmail-sent emails that haven't been replied to,
polls each Gmail thread via the Gmail API, and processes any replies found.

Mirrors the behaviour of the SendGrid inbound parse webhook handler but works
by polling instead of receiving push events.
"""

import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

from bson import ObjectId

import database
from services.gmail_service import get_thread_messages, get_message_body, refresh_token_if_needed

logger = logging.getLogger(__name__)

# Maximum threads to poll per email account per cycle (stays well within Gmail quota)
_MAX_THREADS_PER_ACCOUNT = 50

# Only poll messages sent within the last N days
_POLL_WINDOW_DAYS = 30


async def check_gmail_replies():
    """
    Main job: find all unresolved Gmail campaign messages and check their
    threads for replies. Processes up to _MAX_THREADS_PER_ACCOUNT per account.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(days=_POLL_WINDOW_DAYS)

        # Find Gmail campaign messages that may have received replies
        messages = await database.campaign_messages_collection.find(
            {
                "provider": "gmail",
                "channel": "email",
                "status": {"$in": ["sent", "opened", "clicked"]},
                "gmail_thread_id": {"$exists": True, "$ne": None},
                "sent_at": {"$gte": cutoff},
            }
        ).sort("sent_at", -1).to_list(length=500)

        if not messages:
            return

        # Group by email_account_id to batch API calls per account
        by_account: dict[str, list[dict]] = defaultdict(list)
        for msg in messages:
            acct_id = str(msg.get("email_account_id", ""))
            if acct_id:
                by_account[acct_id].append(msg)

        for account_id, account_messages in by_account.items():
            try:
                await _process_account_replies(account_id, account_messages[:_MAX_THREADS_PER_ACCOUNT])
            except Exception as e:
                logger.error(f"Gmail reply poll failed for account {account_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"check_gmail_replies job failed: {e}", exc_info=True)


async def _process_account_replies(account_id: str, messages: list[dict]):
    """Poll threads for one email account and process any replies found."""
    try:
        email_account = await database.email_accounts_collection.find_one(
            {"_id": ObjectId(account_id)}
        )
    except Exception:
        logger.warning(f"Gmail reply poll: email account {account_id} not found")
        return

    if not email_account:
        return

    # Check that this account has gmail.readonly scope (needed for thread reading)
    scopes = email_account.get("oauth_scopes", [])
    has_readonly = any("gmail.readonly" in s or "gmail" == s for s in scopes)
    if not has_readonly:
        # Silently skip — user needs to re-authorize with new scopes
        return

    access_token = await refresh_token_if_needed(email_account)
    if not access_token:
        logger.warning(f"Gmail reply poll: cannot get access token for {email_account.get('email')}")
        return

    sender_email = email_account.get("email", "")

    # De-duplicate thread IDs (same thread can appear in multiple messages)
    seen_threads: set[str] = set()

    for msg in messages:
        thread_id = msg.get("gmail_thread_id")
        if not thread_id or thread_id in seen_threads:
            continue
        seen_threads.add(thread_id)

        try:
            await _check_thread(msg, thread_id, access_token, sender_email)
        except Exception as e:
            logger.warning(
                f"Gmail reply poll: error checking thread {thread_id} for message {msg['_id']}: {e}"
            )


async def _check_thread(
    original_msg: dict,
    thread_id: str,
    access_token: str,
    sender_email: str,
):
    """
    Check a single Gmail thread for replies.
    If a reply is found, process it (update message, prospect, campaign, conversation).
    """
    thread_messages = await get_thread_messages(access_token, thread_id, sender_email)
    if not thread_messages:
        return

    # Find the first message in the thread NOT sent by us
    reply_msg = next((m for m in thread_messages if m.get("is_reply")), None)
    if not reply_msg:
        return  # No reply yet

    logger.info(
        f"Gmail reply detected: thread={thread_id} "
        f"from={reply_msg['from_email']} "
        f"campaign_message={original_msg['_id']}"
    )

    now = datetime.utcnow()
    reply_gmail_id = reply_msg.get("id", "")

    # Fetch reply body for storing in conversation
    reply_body = ""
    try:
        reply_body = await get_message_body(access_token, reply_gmail_id)
    except Exception:
        reply_body = reply_msg.get("snippet", "")

    await _process_reply(original_msg, reply_msg, reply_body, now)


async def _process_reply(
    original_msg: dict,
    reply_meta: dict,
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
    enrollment_id = (
        original_msg.get("campaign_enrollment_id")
        or original_msg.get("enrollment_id")
    )
    reply_from = reply_meta.get("from_email", "")
    reply_snippet = reply_meta.get("snippet", "")[:200]

    # 1. Update campaign_messages
    await database.campaign_messages_collection.update_one(
        {"_id": msg_id},
        {"$set": {"status": "replied", "replied_at": now}},
    )

    # 2. Increment campaign counter
    if campaign_id:
        await database.campaigns_collection.update_one(
            {"_id": campaign_id},
            {"$inc": {"emails_replied": 1}},
        )

        # 3. Campaign daily stats
        today_str = now.strftime("%Y-%m-%d")
        await database.campaign_daily_stats_collection.update_one(
            {"campaign_id": str(campaign_id), "date": today_str},
            {
                "$inc": {"emails_replied": 1},
                "$setOnInsert": {
                    "emails_sent": 0,
                    "emails_opened": 0,
                    "linkedin_connections_sent": 0,
                    "linkedin_connections_accepted": 0,
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
            logger.warning(f"Gmail reply poll: failed to update prospect {prospect_id}: {e}")

    # 5. Update enrollment status to "replied"
    if enrollment_id:
        try:
            await database.campaign_enrollments_collection.update_one(
                {"_id": enrollment_id},
                {"$set": {"status": "replied", "replied_at": now, "last_activity_at": now}},
            )
            if campaign_id:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_id},
                    {"$inc": {"replied_count": 1, "active_count": -1}},
                )
        except Exception as e:
            logger.warning(f"Gmail reply poll: failed to update enrollment {enrollment_id}: {e}")

    # 6. Create/update conversation entry
    try:
        await _record_reply_in_conversation(
            prospect_id=prospect_id,
            account_id=account_id,
            campaign_id=campaign_id,
            reply_body=reply_body,
            reply_from=reply_from,
            original_subject=original_msg.get("subject", ""),
            now=now,
        )
    except Exception as e:
        logger.warning(f"Gmail reply poll: failed to record conversation for prospect {prospect_id}: {e}")

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

        await database.notifications_collection.insert_one({
            "account_id": account_id,
            "type": "email_reply",
            "title": f"{prospect_name or reply_from} replied to your email",
            "message": reply_snippet,
            "prospect_id": str(prospect_id) if prospect_id else None,
            "campaign_id": str(campaign_id) if campaign_id else None,
            "is_read": False,
            "created_at": now,
        })
    except Exception as e:
        logger.warning(f"Gmail reply poll: failed to create notification: {e}")


async def _record_reply_in_conversation(
    prospect_id,
    account_id,
    campaign_id,
    reply_body: str,
    reply_from: str,
    original_subject: str,
    now: datetime,
):
    """Create or update a conversation document in the inbox with the reply."""
    if not prospect_id:
        return

    prospect_id_str = str(prospect_id)

    # Look up prospect details
    prospect = await database.prospects_collection.find_one(
        {"_id": ObjectId(prospect_id_str)},
        {"full_name": 1, "email": 1, "company_name": 1},
    )
    prospect_email = (prospect or {}).get("email", reply_from)
    prospect_name = (prospect or {}).get("full_name", "")
    prospect_company = (prospect or {}).get("company_name", "")

    # Find or create a conversation for this prospect
    conversation = await database.conversations_collection.find_one(
        {"prospect_id": prospect_id_str, "channel": "email"},
    )

    inbound_message = {
        "message_id": str(uuid.uuid4()),
        "direction": "inbound",
        "content_text": reply_body,
        "content_html": reply_body,
        "subject": f"Re: {original_subject}",
        "timestamp": now,
        "status": "received",
    }

    import random
    from datetime import timedelta
    scheduled_at = now + timedelta(seconds=random.uniform(120, 180))  # email: 2-3 min delay

    if conversation:
        await database.conversations_collection.update_one(
            {"_id": conversation["_id"]},
            {
                "$push": {"messages": inbound_message},
                "$set": {
                    "last_message_at": now,
                    "last_message_preview": (reply_body or "")[:150],
                    "last_message_direction": "inbound",
                    "is_read": False,
                    "needs_classification": True,
                    "scheduled_reply_at": scheduled_at,
                },
                "$inc": {"message_count": 1},
            },
        )
    else:
        await database.conversations_collection.insert_one({
            "account_id": account_id,
            "prospect_id": prospect_id_str,
            "prospect_name": prospect_name,
            "prospect_email": prospect_email,
            "prospect_company": prospect_company,
            "channel": "email",
            "email_thread_subject": original_subject,
            "messages": [inbound_message],
            "is_read": False,
            "last_message_at": now,
            "last_message_preview": (reply_body or "")[:150],
            "last_message_direction": "inbound",
            "message_count": 1,
            "created_at": now,
            "needs_classification": True,
            "scheduled_reply_at": scheduled_at,
        })
