"""
Per-campaign and per-sender-account daily rate limiting for outreach channels.
Caps: {connect:20, email:20, inmail:5, linkedin_message:20} by default.
"""
from datetime import datetime, timedelta, timezone
import random
from bson import ObjectId
from bson.errors import InvalidId
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


def _campaign_object_id(campaign_id: str | ObjectId) -> ObjectId | None:
    """Return the canonical Mongo campaign id, or ``None`` for invalid input.

    Enrollment documents and API boundaries may expose an ObjectId as its safe
    string representation. Campaign documents themselves use an ObjectId `_id`,
    so every campaign-cap query must normalize at this service boundary.
    """
    if isinstance(campaign_id, ObjectId):
        return campaign_id
    try:
        return ObjectId(str(campaign_id))
    except (InvalidId, TypeError, ValueError):
        return None


# Cap keys that a superadmin may override per-account
# (set via PATCH /api/admin/accounts/{id}/quotas → accounts.quota_overrides)
ACCOUNT_OVERRIDE_KEYS = frozenset(DEFAULT_CAPS.keys())


async def get_account_cap_overrides(db: AsyncIOMotorDatabase, account_id) -> dict:
    """Return admin-set per-account cap overrides ({cap_key: int}); {} when none.

    Overrides live in accounts.quota_overrides. When present, they clamp the
    campaign-level cap (min of the two). When absent, behavior is unchanged.
    """
    if not account_id:
        return {}
    try:
        oid = account_id if isinstance(account_id, ObjectId) else ObjectId(str(account_id))
    except Exception:
        return {}
    try:
        acct = await db.accounts.find_one({"_id": oid}, {"quota_overrides": 1})
    except Exception as e:
        logger.warning(f"Account quota override lookup failed for {account_id}: {e}")
        return {}
    overrides = (acct or {}).get("quota_overrides") or {}
    return {
        k: int(v)
        for k, v in overrides.items()
        if k in ACCOUNT_OVERRIDE_KEYS and isinstance(v, (int, float))
    }


async def reserve_slot(
    db: AsyncIOMotorDatabase,
    campaign_id: str | ObjectId,
    channel: str,
) -> bool:
    """
    Atomically increment the daily counter for channel on campaign_id.
    Returns True if slot was reserved (under cap), False if cap already hit.
    """
    campaign_oid = _campaign_object_id(campaign_id)
    if campaign_oid is None:
        logger.warning("Cannot reserve daily-cap slot for invalid campaign id %r", campaign_id)
        return False

    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return True  # unknown channel, don't cap
    date_key = _today_key()
    # First check current count without incrementing
    campaign = await db.campaigns.find_one(
        {"_id": campaign_oid},
        {"daily_caps": 1, "daily_caps_state": 1, "account_id": 1},
    )
    if not campaign:
        return False
    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    cap_limit = caps.get(cap_key, DEFAULT_CAPS.get(cap_key, 999))
    # Superadmin per-account override clamps the campaign-level cap
    account_overrides = await get_account_cap_overrides(db, campaign.get("account_id"))
    if cap_key in account_overrides:
        cap_limit = min(cap_limit, account_overrides[cap_key])
    # The atomic find_one_and_update below already enforces the cap via its
    # filter — no need for a separate (and racy) pre-check read here.
    field_path = f"daily_caps_state.{date_key}.{cap_key}"
    result = await db.campaigns.find_one_and_update(
        {
            "_id": campaign_oid,
            f"daily_caps_state.{date_key}.{cap_key}": {"$lt": cap_limit},
        },
        {"$inc": {field_path: 1}},
        upsert=False,
    )
    if result is None:
        # Cap was just hit by a concurrent request; try initializing the field first
        await db.campaigns.update_one(
            {"_id": campaign_oid, f"daily_caps_state.{date_key}.{cap_key}": {"$exists": False}},
            {"$set": {field_path: 0}},
        )
        result = await db.campaigns.find_one_and_update(
            {
                "_id": campaign_oid,
                f"daily_caps_state.{date_key}.{cap_key}": {"$lt": cap_limit},
            },
            {"$inc": {field_path: 1}},
        )
        if result is None:
            return False
    return True


async def release_slot(
    db: AsyncIOMotorDatabase,
    campaign_id: str | ObjectId,
    channel: str,
) -> None:
    """Release a previously reserved slot (on send failure)."""
    campaign_oid = _campaign_object_id(campaign_id)
    if campaign_oid is None:
        logger.warning("Cannot release daily-cap slot for invalid campaign id %r", campaign_id)
        return
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return
    date_key = _today_key()
    field_path = f"daily_caps_state.{date_key}.{cap_key}"
    await db.campaigns.update_one(
        {"_id": campaign_oid, field_path: {"$gt": 0}},
        {"$inc": {field_path: -1}},
    )


async def get_today_usage(
    db: AsyncIOMotorDatabase,
    campaign_id: str | ObjectId,
) -> dict:
    """Return dict of today's channel usage counts."""
    campaign_oid = _campaign_object_id(campaign_id)
    if campaign_oid is None:
        return {}
    date_key = _today_key()
    campaign = await db.campaigns.find_one(
        {"_id": campaign_oid},
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

# Product safety ceilings, independent of provider-advertised hard quotas. A
# mailbox may configure a lower value; warm-up ramps clamp it further.
EMAIL_PROVIDER_MAX_CAPS = {
    "google": 500,
    "gmail": 500,
    "zoho": 200,
    "smtp": 100,
}
LINKEDIN_PROVIDER_MAX_CAPS = {
    "linkedin_connection": 80,
    "linkedin_inmail": 20,
    "linkedin_message": 50,
}


def _warmup_limit(sender: dict, channel: str, configured_limit: int) -> int:
    if not sender.get("warmup_enabled", False):
        return configured_limit
    status = str(sender.get("warmup_status") or "warming").lower()
    if status == "paused":
        return 0
    if status == "active":
        return configured_limit
    day = max(0, int(sender.get("warmup_day") or 0))
    started_at = sender.get("warmup_started_at")
    if isinstance(started_at, datetime):
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        elapsed_days = max(
            0, (datetime.now(timezone.utc).date() - started_at.date()).days
        )
        day = max(day, elapsed_days)
    if channel == "email":
        ramp = 10 + (5 * day)
    elif channel == "linkedin_connection":
        ramp = 5 + (3 * day)
    elif channel == "linkedin_inmail":
        ramp = 1 + day
    else:
        ramp = 5 + (3 * day)
    return min(configured_limit, ramp)


def sender_policy(sender: dict, channel: str) -> dict:
    """Resolve effective daily limit and provider-specific runtime spacing."""
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return {"limit": 999, "min_gap": 0, "max_gap": 0, "provider": "unknown"}

    if channel == "email":
        provider = str(sender.get("provider") or "smtp").lower()
        configured = int(sender.get("daily_send_limit") or SENDER_DEFAULT_CAPS["email"])
        configured = min(configured, EMAIL_PROVIDER_MAX_CAPS.get(provider, 100))
        warmed = str(sender.get("warmup_status") or "").lower() == "active"
        gap_by_provider = {
            "google": (2, 8) if warmed and configured > 50 else (8, 15),
            "gmail": (2, 8) if warmed and configured > 50 else (8, 15),
            "zoho": (4, 12) if warmed and configured > 40 else (10, 20),
            "smtp": (6, 15) if warmed and configured > 30 else (12, 25),
        }
        min_gap, max_gap = gap_by_provider.get(provider, (12, 25))
    else:
        provider = "unipile_linkedin"
        field_by_channel = {
            "linkedin_connection": "daily_connection_limit",
            "linkedin_inmail": "daily_inmail_limit",
            "linkedin_message": "daily_message_limit",
        }
        configured = int(
            sender.get(field_by_channel[channel])
            or SENDER_DEFAULT_CAPS[channel]
        )
        configured = min(configured, LINKEDIN_PROVIDER_MAX_CAPS[channel])
        min_gap, max_gap = SEND_GAP_BOUNDS[channel]

    effective = max(0, _warmup_limit(sender, channel, max(0, configured)))
    effective_warmup_day = max(0, int(sender.get("warmup_day") or 0))
    started_at = sender.get("warmup_started_at")
    if isinstance(started_at, datetime):
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        effective_warmup_day = max(
            effective_warmup_day,
            max(0, (datetime.now(timezone.utc).date() - started_at.date()).days),
        )
    return {
        "limit": effective,
        "configured_limit": configured,
        "min_gap": min_gap,
        "max_gap": max_gap,
        "provider": provider,
        "warmup_status": sender.get("warmup_status"),
        "warmup_day": effective_warmup_day,
    }


async def _load_sender(db: AsyncIOMotorDatabase, sender_id: str, channel: str) -> dict | None:
    try:
        sender_oid = ObjectId(str(sender_id))
    except Exception:
        return None
    collection = db.email_accounts if channel == "email" else db.linkedin_accounts
    return await collection.find_one({"_id": sender_oid})


async def reserve_sender_slot(
    db: AsyncIOMotorDatabase,
    sender_id: str,
    channel: str,
    sender: dict | None = None,
) -> bool:
    """
    Atomically increment the sender-level daily counter for channel.
    Prevents multiple campaigns from collectively exceeding a sender's daily limit.
    Returns True if slot reserved, False if cap hit.
    """
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return True
    sender = sender or await _load_sender(db, sender_id, channel)
    if not sender:
        logger.warning("Cannot reserve cap for unknown sender %s", sender_id)
        return False
    policy = sender_policy(sender, channel)
    cap_limit = policy["limit"]
    if cap_limit <= 0:
        return False
    date_key = _today_key()
    field_path = f"daily_send_state.{date_key}.{cap_key}"
    throttle_path = f"next_send_not_before.{cap_key}"
    now = datetime.now(timezone.utc)
    next_allowed = now + timedelta(
        minutes=random.uniform(policy["min_gap"], policy["max_gap"])
    )

    result = await db.sender_daily_caps.find_one_and_update(
        {
            "_id": sender_id,
            f"daily_send_state.{date_key}.{cap_key}": {"$lt": cap_limit},
            "$or": [
                {throttle_path: {"$exists": False}},
                {throttle_path: None},
                {throttle_path: {"$lte": now}},
            ],
        },
        {
            "$inc": {field_path: 1},
            "$set": {
                throttle_path: next_allowed,
                "provider": policy["provider"],
                "effective_daily_limit": cap_limit,
                "warmup_status": policy.get("warmup_status"),
                "warmup_day": policy.get("warmup_day", 0),
                "updated_at": now,
            },
        },
        upsert=False,
    )
    if result is None:
        # Initialize then retry
        await db.sender_daily_caps.update_one(
            {"_id": sender_id},
            {"$setOnInsert": {"_id": sender_id}},
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
                "$or": [
                    {throttle_path: {"$exists": False}},
                    {throttle_path: None},
                    {throttle_path: {"$lte": now}},
                ],
            },
            {
                "$inc": {field_path: 1},
                "$set": {
                    throttle_path: next_allowed,
                    "provider": policy["provider"],
                    "effective_daily_limit": cap_limit,
                    "warmup_status": policy.get("warmup_status"),
                    "warmup_day": policy.get("warmup_day", 0),
                    "updated_at": now,
                },
            },
        )
        if result is None:
            return False
    return True


async def get_sender_defer_until(
    db: AsyncIOMotorDatabase,
    sender_id: str,
    channel: str,
) -> datetime:
    """Return the sender throttle time, or tomorrow when its daily cap is full."""
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    now = datetime.now(timezone.utc)
    if cap_key:
        doc = await db.sender_daily_caps.find_one(
            {"_id": sender_id}, {f"next_send_not_before.{cap_key}": 1}
        )
        next_at = ((doc or {}).get("next_send_not_before") or {}).get(cap_key)
        if isinstance(next_at, datetime):
            if next_at.tzinfo is None:
                next_at = next_at.replace(tzinfo=timezone.utc)
            if next_at > now:
                return next_at.replace(tzinfo=None)
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=9, minute=0, second=0, microsecond=0, tzinfo=None)


async def release_sender_slot(db: AsyncIOMotorDatabase, sender_id: str, channel: str) -> None:
    """Release a previously reserved sender-level slot (on send failure)."""
    cap_key = CHANNEL_TO_CAP_KEY.get(channel)
    if not cap_key:
        return
    date_key = _today_key()
    field_path = f"daily_send_state.{date_key}.{cap_key}"
    await db.sender_daily_caps.update_one(
        {"_id": sender_id, field_path: {"$gt": 0}},
        {
            "$inc": {field_path: -1},
            "$unset": {f"next_send_not_before.{cap_key}": ""},
        },
    )


async def reset_all_campaigns_daily_caps(db: AsyncIOMotorDatabase) -> int:
    """Called at midnight UTC. Clears yesterday's state (keeps only today's key).

    Uses a single aggregation-pipeline update_many instead of a load-all +
    per-doc update_one loop — one round trip regardless of campaign count.
    Also purges stale date keys from sender_daily_caps.daily_send_state,
    which nothing else ever cleans up and otherwise grows forever.
    """
    today_key = _today_key()
    keep_today_key = {
        "$arrayToObject": {
            "$filter": {
                "input": {"$objectToArray": {"$ifNull": ["$daily_caps_state", {}]}},
                "cond": {"$eq": ["$$this.k", today_key]},
            }
        }
    }
    result = await db.campaigns.update_many(
        {"daily_caps_state": {"$exists": True}},
        [{"$set": {"daily_caps_state": keep_today_key}}],
    )

    keep_today_send_state = {
        "$arrayToObject": {
            "$filter": {
                "input": {"$objectToArray": {"$ifNull": ["$daily_send_state", {}]}},
                "cond": {"$eq": ["$$this.k", today_key]},
            }
        }
    }
    await db.sender_daily_caps.update_many(
        {"daily_send_state": {"$exists": True}},
        [{"$set": {"daily_send_state": keep_today_send_state}}],
    )

    logger.info(f"Daily cap reset: cleaned {result.modified_count} campaigns")
    return result.modified_count
