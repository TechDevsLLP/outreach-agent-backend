"""
Unified Email Reply Poller — runs every 5 minutes via APScheduler.

Queries campaign_messages for sent emails (any provider: Gmail, Zoho, SMTP/IMAP)
that haven't been replied to yet, polls each provider's thread through the
common EmailProvider interface, and processes any replies found via
services.reply_ingest. Replaces the previous Gmail-only poller
(services/gmail_reply_poller.py, deleted) now that Zoho and SMTP/IMAP are
first-class channels.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId

import database
from services.email_providers import get_provider
from services.email_providers.base import ReplyMeta
from services.reply_ingest import process_reply

logger = logging.getLogger(__name__)

# Maximum threads to poll per email account per cycle (stays well within provider quotas)
_MAX_THREADS_PER_ACCOUNT = 50

# Only poll messages sent within the last N days
_POLL_WINDOW_DAYS = 30

# Exponential backoff for re-checking a thread with no reply yet: 5m -> 15m ->
# 30m, capped. Reset to the first step whenever a reply is found, so a fast
# back-and-forth still gets checked promptly.
#
# The ceiling is deliberately 30 minutes rather than the several hours a pure
# cost-saving ladder would suggest: a prospect reply is the single highest-value
# event in the system, and worst-case detection latency is what the user
# actually feels. With _MAX_THREADS_PER_ACCOUNT capping each tick, the extra
# provider calls stay well inside Gmail/Zoho quotas.
_BACKOFF_LADDER_MINUTES = [5, 15, 30]


def _next_backoff_minutes(current: Optional[int]) -> int:
    if not current:
        return _BACKOFF_LADDER_MINUTES[0]
    for step in _BACKOFF_LADDER_MINUTES:
        if step > current:
            return step
    return _BACKOFF_LADDER_MINUTES[-1]


async def check_email_replies():
    """
    Main job: find all unresolved campaign email messages (any provider) and
    check their threads for replies. Processes up to _MAX_THREADS_PER_ACCOUNT
    threads per account.
    """
    try:
        cutoff = datetime.utcnow() - timedelta(days=_POLL_WINDOW_DAYS)
        now = datetime.utcnow()

        base_filter = {
            "channel": "email",
            # "replied" stays in the poll rotation too — a thread with one
            # reply can still get a second, later prospect email.
            "status": {"$in": ["sent", "clicked", "replied"]},
            "sent_at": {"$gte": cutoff},
            "$or": [
                {"next_reply_poll_at": {"$exists": False}},
                {"next_reply_poll_at": {"$lte": now}},
            ],
        }

        messages = await database.campaign_messages_collection.find(
            {**base_filter, "provider_thread_id": {"$exists": True, "$ne": None}}
        ).sort("next_reply_poll_at", 1).to_list(length=500)

        # Back-compat: pick up messages written before the gmail_thread_id -> provider_thread_id rename
        legacy_messages = await database.campaign_messages_collection.find(
            {
                **base_filter,
                "provider_thread_id": {"$exists": False},
                "gmail_thread_id": {"$exists": True, "$ne": None},
            }
        ).sort("next_reply_poll_at", 1).to_list(length=500)

        all_messages = messages + legacy_messages
        if not all_messages:
            return

        # Group by email_account_id to batch API calls per account
        by_account: dict[str, list[dict]] = defaultdict(list)
        for msg in all_messages:
            acct_id = str(msg.get("email_account_id", ""))
            if acct_id:
                by_account[acct_id].append(msg)

        for account_id, account_messages in by_account.items():
            try:
                await _process_account_replies(account_id, account_messages[:_MAX_THREADS_PER_ACCOUNT])
            except Exception as e:
                logger.error(f"Email reply poll failed for account {account_id}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"check_email_replies job failed: {e}", exc_info=True)


async def _process_account_replies(account_id: str, messages: list[dict]):
    """Poll threads for one email account and process any replies found."""
    try:
        email_account = await database.email_accounts_collection.find_one(
            {"_id": ObjectId(account_id)}
        )
    except Exception:
        logger.warning(f"Email reply poll: email account {account_id} not found")
        return

    if not email_account:
        return

    provider = get_provider(email_account)
    if not provider:
        return  # unsupported/legacy provider (e.g. the retired microsoft stub)

    # Gmail needs the gmail.readonly scope to read threads — skip silently if the
    # account was connected before that scope was requested (user needs to re-auth).
    if email_account.get("provider") == "google":
        scopes = email_account.get("oauth_scopes", [])
        has_readonly = any("gmail.readonly" in s or s == "gmail" for s in scopes)
        if not has_readonly:
            return

    sender_email = email_account.get("email", "")

    # De-duplicate thread refs (same thread can appear in multiple messages)
    seen_threads: set[str] = set()

    for msg in messages:
        thread_ref = msg.get("provider_thread_id") or msg.get("gmail_thread_id")
        if not thread_ref or thread_ref in seen_threads:
            continue
        seen_threads.add(thread_ref)

        try:
            await _check_thread(provider, msg, thread_ref, sender_email)
        except Exception as e:
            logger.warning(
                f"Email reply poll: error checking thread {thread_ref} for message {msg['_id']}: {e}"
            )


async def _check_thread(provider, original_msg: dict, thread_ref: str, sender_email: str):
    """
    Check a single provider thread for replies.
    Processes every reply found (reply_ingest dedups by provider_message_id,
    so re-scanning an already-seen reply is a no-op) and reschedules the next
    poll with exponential backoff.
    """
    replies: list[ReplyMeta] = await provider.fetch_new_replies(thread_ref, sender_email)
    now = datetime.utcnow()

    if not replies:
        backoff = _next_backoff_minutes(original_msg.get("reply_poll_backoff_minutes"))
        await database.campaign_messages_collection.update_one(
            {"_id": original_msg["_id"]},
            {"$set": {
                "next_reply_poll_at": now + timedelta(minutes=backoff),
                "reply_poll_backoff_minutes": backoff,
            }},
        )
        return

    logger.info(
        f"Email reply detected: thread={thread_ref} count={len(replies)} "
        f"campaign_message={original_msg['_id']}"
    )

    for reply_meta in replies:
        # Fetch the full reply body for storing in the conversation; fall back
        # to whatever snippet the listing call already gave us.
        reply_body = reply_meta.snippet
        try:
            fetched_body = await provider.get_message_body(reply_meta.provider_message_id)
            if fetched_body:
                reply_body = fetched_body
        except Exception:
            pass

        await process_reply(original_msg, reply_meta, reply_body, now)

    # A reply just came in — reset backoff so a quick follow-up is caught fast.
    backoff = _BACKOFF_LADDER_MINUTES[0]
    await database.campaign_messages_collection.update_one(
        {"_id": original_msg["_id"]},
        {"$set": {
            "next_reply_poll_at": now + timedelta(minutes=backoff),
            "reply_poll_backoff_minutes": backoff,
        }},
    )
