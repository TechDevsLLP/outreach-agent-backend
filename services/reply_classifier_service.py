"""
Reply Classifier Service.
Classifies inbound prospect messages into 6 categories using Haiku 4.5.
Designed to be called by the scheduler poller (every 20s) on conversations
where needs_classification=True AND scheduled_reply_at <= now.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId

from database import (
    conversations_collection,
    reply_classifications_collection,
    campaign_enrollments_collection,
)
from models.reply_classification import REPLY_CATEGORIES
from services.openrouter_service import OpenRouterClient
from utils.prompts import (
    REPLY_CLASSIFIER_SYSTEM_PROMPT,
    build_reply_classifier_user_prompt,
)

logger = logging.getLogger(__name__)

CLASSIFIER_MODEL = "anthropic/claude-haiku-4-5"


async def classify_reply(
    conversation_id: str,
    message_text: str,
    channel: str,
    account_id: str,
    enrollment_id: Optional[str] = None,
    prospect_id: Optional[str] = None,
) -> dict:
    """
    Classify a single inbound reply message.
    Persists the classification to reply_classifications collection.
    Returns the classification dict.
    """
    # Build conversation context (last 4 messages)
    conv = await conversations_collection.find_one(
        {"_id": ObjectId(conversation_id)},
        {"messages": {"$slice": -6}},
    )
    context_lines = []
    if conv:
        for m in (conv.get("messages") or []):
            direction = "You" if m.get("direction") == "outbound" else "Them"
            context_lines.append(f"{direction}: {(m.get('content_text') or '')[:300]}")
    conversation_context = "\n".join(context_lines)

    user_prompt = build_reply_classifier_user_prompt(
        message_text=message_text,
        channel=channel,
        conversation_context=conversation_context,
    )

    client = OpenRouterClient()
    raw = await client.chat_completion(
        messages=[
            {"role": "system", "content": REPLY_CLASSIFIER_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=CLASSIFIER_MODEL,
        max_tokens=400,
        temperature=0.1,
    )

    content = (raw.get("content") or "").strip()

    # Parse JSON — strip markdown fences if present
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]

    try:
        parsed = json.loads(content)
    except Exception:
        logger.error(f"Classifier returned non-JSON for conversation {conversation_id}: {content[:200]}")
        parsed = {
            "category": "QUESTION",
            "confidence": 0.5,
            "signals": {"sentiment": "neutral", "hostility_score": 0.0},
        }

    category = parsed.get("category", "QUESTION")
    if category not in REPLY_CATEGORIES:
        category = "QUESTION"

    confidence = float(parsed.get("confidence", 0.5))
    signals = parsed.get("signals", {})

    classification = {
        "account_id": account_id,
        "conversation_id": conversation_id,
        "enrollment_id": enrollment_id,
        "prospect_id": prospect_id,
        "message_text": message_text[:2000],
        "message_direction": "inbound",
        "channel": channel,
        "category": category,
        "confidence": confidence,
        "signals": signals,
        "prompt_slug": "reply_classifier_v1",
        "handler_triggered": False,
        "auto_reply_sent": False,
        "classified_at": datetime.now(timezone.utc),
    }

    await reply_classifications_collection.insert_one(classification)

    # Update the enrollment with the latest classification
    if enrollment_id:
        try:
            await campaign_enrollments_collection.update_one(
                {"_id": ObjectId(enrollment_id)},
                {
                    "$set": {
                        "last_classification": {
                            "category": category,
                            "confidence": confidence,
                            "signals": signals,
                            "classified_at": datetime.now(timezone.utc),
                            "conversation_id": conversation_id,
                        }
                    },
                    "$push": {
                        "classification_history": {
                            "category": category,
                            "confidence": confidence,
                            "classified_at": datetime.now(timezone.utc),
                            "conversation_id": conversation_id,
                        }
                    },
                },
            )
        except Exception as e:
            logger.warning(f"Failed to update enrollment classification for {enrollment_id}: {e}")

    logger.info(
        f"Classified reply for conversation {conversation_id}: {category} "
        f"(confidence={confidence:.2f}, channel={channel})"
    )
    return classification


async def get_latest_classification(conversation_id: str) -> Optional[dict]:
    """Fetch the most recent classification for a conversation."""
    return await reply_classifications_collection.find_one(
        {"conversation_id": conversation_id},
        sort=[("classified_at", -1)],
    )


async def mark_handler_triggered(classification_id: str, auto_reply_sent: bool = False) -> None:
    """Mark a classification record as handled."""
    await reply_classifications_collection.update_one(
        {"_id": ObjectId(classification_id)},
        {
            "$set": {
                "handler_triggered": True,
                "handler_triggered_at": datetime.now(timezone.utc),
                "auto_reply_sent": auto_reply_sent,
            }
        },
    )


async def process_due_conversations() -> dict:
    """
    Called by the APScheduler job every 20 seconds.
    Finds conversations with needs_classification=True AND scheduled_reply_at <= now,
    classifies them, and triggers the appropriate reply handler.
    """
    from services.reply_handler_service import handle_classified_reply

    now = datetime.now(timezone.utc)
    # Also match naive datetimes stored by older code paths
    now_naive = datetime.utcnow()

    cursor = conversations_collection.find(
        {
            "needs_classification": True,
            "$or": [
                {"scheduled_reply_at": {"$lte": now}},
                {"scheduled_reply_at": {"$lte": now_naive}},
            ],
        },
        limit=50,  # process at most 50 per tick to avoid thundering-herd
    )

    processed = 0
    errors = 0

    async for conv in cursor:
        conv_id = str(conv["_id"])
        try:
            # Find the latest inbound message to classify
            messages = conv.get("messages") or []
            inbound_msg = None
            for m in reversed(messages):
                if m.get("direction") == "inbound":
                    inbound_msg = m
                    break

            if not inbound_msg:
                # Nothing to classify — clear the flag
                await conversations_collection.update_one(
                    {"_id": conv["_id"]},
                    {"$set": {"needs_classification": False}},
                )
                continue

            message_text = inbound_msg.get("content_text") or ""
            channel = conv.get("channel", "email")
            account_id = conv.get("account_id", "")
            prospect_id = conv.get("prospect_id")

            # Find active enrollment for context
            enrollment_id = None
            if prospect_id:
                enr = await campaign_enrollments_collection.find_one(
                    {
                        "prospect_id": {"$in": [prospect_id, ObjectId(prospect_id)]},
                        "status": {"$in": ["active", "enrolled", "replied"]},
                    },
                    {"_id": 1},
                )
                if enr:
                    enrollment_id = str(enr["_id"])

            # Classify
            classification = await classify_reply(
                conversation_id=conv_id,
                message_text=message_text,
                channel=channel,
                account_id=account_id,
                enrollment_id=enrollment_id,
                prospect_id=prospect_id,
            )

            # Clear the flag immediately (before handler — idempotent on retry)
            await conversations_collection.update_one(
                {"_id": conv["_id"]},
                {
                    "$set": {
                        "needs_classification": False,
                        "classifier_processed_at": datetime.now(timezone.utc),
                    }
                },
            )

            # Trigger the handler
            await handle_classified_reply(
                classification=classification,
                conversation=conv,
                message_text=message_text,
            )

            processed += 1

        except Exception as e:
            logger.error(f"Error processing classification for conversation {conv_id}: {e}", exc_info=True)
            # Clear the flag to prevent infinite retry loop — a human can re-trigger if needed
            try:
                await conversations_collection.update_one(
                    {"_id": conv["_id"]},
                    {"$set": {"needs_classification": False}},
                )
            except Exception:
                pass
            errors += 1

    if processed or errors:
        logger.info(f"Classifier poller: {processed} processed, {errors} errors")

    return {"processed": processed, "errors": errors}
