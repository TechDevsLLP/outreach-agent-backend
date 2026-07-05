"""
Sender Pool Service.
Picks the best sender for a given channel + account, with thread continuity.

v1: round-robin weighted by remaining daily cap.
    Once an enrollment has an assigned_sender_id, all subsequent touches on that
    channel stay on the same sender (thread continuity). Falls back to round-robin
    if the assigned sender is paused or over quota.
"""
import logging
from typing import Optional

from bson import ObjectId

import database

logger = logging.getLogger(__name__)


async def pick_sender_for_send(
    account_id: str,
    channel: str,
    enrollment_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Return the best sender account doc for this channel.

    Priority:
    1. If enrollment has assigned_sender_id and it's still healthy → return it.
    2. Otherwise, round-robin across connected senders weighted by remaining daily cap.
    3. Returns None if no sender has remaining capacity.
    """
    # Thread continuity: check if enrollment already has an assigned sender
    if enrollment_id:
        enrolled = await database.campaign_enrollments_collection.find_one(
            {"_id": ObjectId(enrollment_id)},
            {"assigned_sender_id": 1},
        )
        assigned_id = (enrolled or {}).get("assigned_sender_id")
        if assigned_id:
            sender = await _get_healthy_sender(assigned_id, channel)
            if sender:
                return sender
            logger.info(f"Assigned sender {assigned_id} unhealthy for {channel}, falling back to pool")

    # Pool selection: round-robin by remaining cap
    sender = await _pick_from_pool(account_id, channel)
    if sender and enrollment_id:
        # Persist assignment for thread continuity
        await database.campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {"assigned_sender_id": str(sender["_id"])}},
        )
    return sender


async def _get_healthy_sender(sender_id: str, channel: str) -> Optional[dict]:
    """Return the sender doc if it exists, is active, and has remaining cap for this channel."""
    try:
        collection = _collection_for_channel(channel)
        if not collection:
            return None
        sender = await collection.find_one({"_id": ObjectId(sender_id)})
        if not sender:
            return None
        if sender.get("status") in ("paused", "disabled", "error"):
            return None
        if not _has_remaining_cap(sender, channel):
            return None
        return sender
    except Exception as e:
        logger.warning(f"_get_healthy_sender failed for {sender_id}: {e}")
        return None


async def _pick_from_pool(account_id: str, channel: str) -> Optional[dict]:
    """Pick the sender with most remaining daily cap for this channel."""
    collection = _collection_for_channel(channel)
    if not collection:
        return None

    query = {"account_id": account_id}
    if channel in ("email",):
        query["status"] = {"$nin": ["paused", "disabled", "error"]}
    else:
        query["status"] = {"$nin": ["paused", "disabled", "error"]}

    candidates = await collection.find(query).to_list(length=20)
    if not candidates:
        return None

    # Sort by remaining daily cap descending
    def remaining_cap(sender: dict) -> int:
        cap_key = _cap_key_for_channel(channel)
        limit = sender.get("daily_cap", {}).get(cap_key) or _default_cap(channel)
        used = sender.get("daily_usage", {}).get(cap_key, 0)
        return max(0, limit - used)

    candidates_with_cap = [(s, remaining_cap(s)) for s in candidates]
    candidates_with_cap.sort(key=lambda x: x[1], reverse=True)

    best_sender, best_remaining = candidates_with_cap[0]
    if best_remaining <= 0:
        logger.debug(f"No sender has remaining cap for channel={channel} account={account_id}")
        return None
    return best_sender


def _collection_for_channel(channel: str):
    """Return the appropriate sender collection for the channel."""
    if channel in ("email",):
        return database.email_accounts_collection
    if channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message", "linkedin_dm"):
        return database.linkedin_accounts_collection
    return None


def _cap_key_for_channel(channel: str) -> str:
    cap_map = {
        "email": "email",
        "linkedin_connection": "connections",
        "linkedin_inmail": "inmails",
        "linkedin_message": "messages",
        "linkedin_dm": "messages",
    }
    return cap_map.get(channel, channel)


def _default_cap(channel: str) -> int:
    defaults = {
        "email": 50,
        "linkedin_connection": 25,
        "linkedin_inmail": 10,
        "linkedin_message": 30,
        "linkedin_dm": 30,
    }
    return defaults.get(channel, 0)


def _has_remaining_cap(sender: dict, channel: str) -> bool:
    cap_key = _cap_key_for_channel(channel)
    limit = sender.get("daily_cap", {}).get(cap_key) or _default_cap(channel)
    used = sender.get("daily_usage", {}).get(cap_key, 0)
    return (limit - used) > 0
