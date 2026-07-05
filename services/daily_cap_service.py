"""
Per-campaign and per-sender-account daily rate limiting for outreach channels.
Caps: {connect:20, email:20, inmail:5, linkedin_message:20} by default.
"""
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorDatabase
import logging

logger = logging.getLogger(__name__)

DEFAULT_CAPS = {
    "linkedin_connection": 20,
    "email": 20,
    "linkedin_inmail": 5,
    "linkedin_message": 20,
}

# Min/max minute gaps between consecutive sends of the same channel on the same day.
# Randomized gaps within these bounds prevent detectable fixed-cadence patterns.
SEND_GAP_BOUNDS: dict[str, tuple[int, int]] = {
    "linkedin_connection": (15, 45),
    "email":               (10, 35),
    "linkedin_inmail":     (60, 180),
    "linkedin_message":    (10, 35),
}

# Map channel strings that appear in flow nodes to cap keys
CHANNEL_TO_CAP_KEY = {
    "linkedin_connection": "linkedin_connection",
    "email": "email",
    "linkedin_inmail": "linkedin_inmail",
    "linkedin_message": "linkedin_message",
}


def _today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def reserve_slot(db: AsyncIOMotorDatabase, campaign_id: str, channel: str) -> bool:
    """
    Atomically increment the daily counter for channel on campaign_id.
    Returns True if slot was reserved (under cap), False if cap already hit.
    """
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return True  # unknown channel, don't cap
    date_key = _today_key()
    # First check current count without incrementing
    campaign = await db.campaigns.find_one(
        {"_id": campaign_id},
        {"daily_caps": 1, "daily_caps_state": 1},
    )
    if not campaign:
        return False
    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    cap_limit = caps.get(cap_key, DEFAULT_CAPS.get(cap_key, 999))
    caps_state = campaign.get("daily_caps_state") or {}
    today_state = caps_state.get(date_key) or {}
    current_count = today_state.get(cap_key, 0)
    if current_count >= cap_limit:
        return False
    # Atomic increment
    field_path = f"daily_caps_state.{date_key}.{cap_key}"
    result = await db.campaigns.find_one_and_update(
        {
            "_id": campaign_id,
            f"daily_caps_state.{date_key}.{cap_key}": {"$lt": cap_limit},
        },
        {"$inc": {field_path: 1}},
        upsert=False,
    )
    if result is None:
        # Cap was just hit by a concurrent request; try initializing the field first
        await db.campaigns.update_one(
            {"_id": campaign_id, f"daily_caps_state.{date_key}.{cap_key}": {"$exists": False}},
            {"$set": {field_path: 0}},
        )
        result = await db.campaigns.find_one_and_update(
            {
                "_id": campaign_id,
                f"daily_caps_state.{date_key}.{cap_key}": {"$lt": cap_limit},
            },
            {"$inc": {field_path: 1}},
        )
        if result is None:
            return False
    return True


async def release_slot(db: AsyncIOMotorDatabase, campaign_id: str, channel: str) -> None:
    """Release a previously reserved slot (on send failure)."""
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return
    date_key = _today_key()
    field_path = f"daily_caps_state.{date_key}.{cap_key}"
    await db.campaigns.update_one(
        {"_id": campaign_id},
        {"$inc": {field_path: -1}},
    )


async def get_today_usage(db: AsyncIOMotorDatabase, campaign_id: str) -> dict:
    """Return dict of today's channel usage counts."""
    date_key = _today_key()
    campaign = await db.campaigns.find_one(
        {"_id": campaign_id},
        {"daily_caps": 1, "daily_caps_state": 1},
    )
    if not campaign:
        return {}
    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    caps_state = campaign.get("daily_caps_state") or {}
    today_state = caps_state.get(date_key) or {}
    return {
        "date": date_key,
        "usage": today_state,
        "limits": caps,
        "remaining": {
            k: max(0, caps.get(k, DEFAULT_CAPS.get(k, 999)) - today_state.get(k, 0))
            for k in DEFAULT_CAPS
        },
    }


SENDER_DEFAULT_CAPS = {
    "email": 50,
    "linkedin_connection": 25,
    "linkedin_inmail": 10,
    "linkedin_message": 30,
    "voice_note": 5,
    "video_dm": 3,
}


async def reserve_sender_slot(db: AsyncIOMotorDatabase, sender_id: str, channel: str) -> bool:
    """
    Atomically increment the sender-level daily counter for channel.
    Prevents multiple campaigns from collectively exceeding a sender's daily limit.
    Returns True if slot reserved, False if cap hit.
    """
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return True
    cap_limit = SENDER_DEFAULT_CAPS.get(cap_key, 999)
    date_key = _today_key()
    field_path = f"daily_send_state.{date_key}.{cap_key}"

    result = await db.sender_daily_caps.find_one_and_update(
        {
            "_id": sender_id,
            f"daily_send_state.{date_key}.{cap_key}": {"$lt": cap_limit},
        },
        {"$inc": {field_path: 1}},
        upsert=False,
    )
    if result is None:
        # Initialize then retry
        await db.sender_daily_caps.update_one(
            {"_id": sender_id},
            {"$setOnInsert": {"_id": sender_id}, "$set": {}},
            upsert=True,
        )
        await db.sender_daily_caps.update_one(
            {"_id": sender_id, f"daily_send_state.{date_key}.{cap_key}": {"$exists": False}},
            {"$set": {field_path: 0}},
        )
        result = await db.sender_daily_caps.find_one_and_update(
            {
                "_id": sender_id,
                f"daily_send_state.{date_key}.{cap_key}": {"$lt": cap_limit},
            },
            {"$inc": {field_path: 1}},
        )
        if result is None:
            return False
    return True


async def release_sender_slot(db: AsyncIOMotorDatabase, sender_id: str, channel: str) -> None:
    """Release a previously reserved sender-level slot (on send failure)."""
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return
    date_key = _today_key()
    field_path = f"daily_send_state.{date_key}.{cap_key}"
    await db.sender_daily_caps.update_one(
        {"_id": sender_id},
        {"$inc": {field_path: -1}},
    )


async def reset_all_campaigns_daily_caps(db: AsyncIOMotorDatabase) -> int:
    """Called at midnight UTC. Clears yesterday's state (keeps only today's key)."""
    today_key = _today_key()
    # Find all campaigns that have daily_caps_state with keys other than today
    campaigns = await db.campaigns.find(
        {"daily_caps_state": {"$exists": True}},
        {"_id": 1, "daily_caps_state": 1},
    ).to_list(None)
    updated = 0
    for camp in campaigns:
        state = camp.get("daily_caps_state") or {}
        old_keys = [k for k in state if k != today_key]
        if old_keys:
            unset_dict = {f"daily_caps_state.{k}": "" for k in old_keys}
            await db.campaigns.update_one(
                {"_id": camp["_id"]},
                {"$unset": unset_dict},
            )
            updated += 1
    logger.info(f"Daily cap reset: cleaned {updated} campaigns")
    return updated


async def compute_required_daily_capacity(campaign: dict) -> dict:
    """
    Estimate required daily send capacity for a campaign.
    Returns {channel: required_per_day} and {shortfall: bool, message: str}.
    """
    prospect_count = campaign.get("target_count", 100)
    preset = campaign.get("aggression_preset", "aggressive")
    # Rough touch estimates per preset
    touches = {
        "aggressive": {"email": 6, "linkedin_connection": 1, "linkedin_inmail": 1, "linkedin_message": 1},
        "moderate":   {"email": 4, "linkedin_connection": 1, "linkedin_inmail": 1, "linkedin_message": 1},
        "conservative": {"email": 2, "linkedin_connection": 1, "linkedin_message": 1},
    }.get(preset, {"email": 6, "linkedin_connection": 1, "linkedin_inmail": 1, "linkedin_message": 1})
    duration_days = 28
    required = {ch: round((prospect_count * count) / duration_days, 1) for ch, count in touches.items()}
    return {"required_per_day": required, "preset": preset, "prospect_count": prospect_count}
