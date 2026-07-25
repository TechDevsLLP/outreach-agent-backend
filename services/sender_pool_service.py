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
        if collection is None:
            return None
        sender = await collection.find_one({"_id": ObjectId(sender_id)})
        if not sender:
            return None
        if sender.get("status") in ("paused", "disabled", "error"):
            return None
        if await _remaining_cap(sender, channel) <= 0:
            return None
        return sender
    except Exception as e:
        logger.warning(f"_get_healthy_sender failed for {sender_id}: {e}")
        return None


async def _pick_from_pool(account_id: str, channel: str) -> Optional[dict]:
    """Pick the sender with most remaining daily cap for this channel."""
    collection = _collection_for_channel(channel)
    if collection is None:
        return None

    query = {"account_id": account_id, "status": {"$nin": ["paused", "disabled", "error"]}}

    candidates = await collection.find(query).to_list(length=20)
    if not candidates:
        return None

    # Sort by remaining daily cap (real usage from sender_daily_caps) descending
    candidates_with_cap = [(s, await _remaining_cap(s, channel)) for s in candidates]
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


async def _remaining_cap(sender: dict, channel: str) -> int:
    """Real remaining daily capacity for this sender+channel.

    Reads today's usage from the ``sender_daily_caps`` collection — the same
    collection ``daily_cap_service.reserve_sender_slot`` atomically increments
    (``daily_send_state.{date}.{channel}``) — rather than the ``daily_cap`` /
    ``daily_usage`` fields on the sender doc, which nothing in the codebase
    ever writes. The configured limit comes from
    ``daily_cap_service.sender_policy`` so warm-up ramps and provider ceilings
    are honored identically to the reservation path.
    """
    from datetime import datetime, timezone
    from services.daily_cap_service import sender_policy, CHANNEL_TO_CAP_KEY

    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return 0
    limit = sender_policy(sender, channel)["limit"]
    if limit <= 0:
        return 0
    date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usage_doc = await database.db.sender_daily_caps.find_one(
        {"_id": str(sender["_id"])}, {f"daily_send_state.{date_key}.{cap_key}": 1}
    )
    used = (((usage_doc or {}).get("daily_send_state") or {}).get(date_key) or {}).get(cap_key, 0)
    return max(0, limit - used)
