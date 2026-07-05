"""
Reply Handler Service.
Routes classified inbound replies to category-specific handlers.

Reply latency contract: by the time this handler is called, the scheduled_reply_at
delay has already elapsed (enforced by the classifier poller in reply_classifier_service.py).
Handlers that auto-send do so immediately — no additional delay here.

Escalated categories (HARD_OBJECTION hostile, UNSUBSCRIBE) never auto-send.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from bson import ObjectId

from database import (
    conversations_collection,
    campaign_enrollments_collection,
    prospects_collection,
    company_profiles_collection,
    suppressions_collection,
)
from services.openrouter_service import OpenRouterClient
from utils.prompts import (
    build_reply_positive_prompt,
    build_reply_question_prompt,
    build_reply_soft_objection_prompt,
    build_reply_hard_objection_prompt,
    build_reply_ooo_prompt,
    REPLY_POSITIVE_SYSTEM_PROMPT_FALLBACK,
    REPLY_QUESTION_SYSTEM_PROMPT,
    REPLY_SOFT_OBJECTION_SYSTEM_PROMPT,
    REPLY_HARD_OBJECTION_SYSTEM_PROMPT,
    REPLY_OOO_SYSTEM_PROMPT,
    _redact_banned_phrases,
)

logger = logging.getLogger(__name__)

REPLY_MODEL = "anthropic/claude-haiku-4-5"
HOSTILE_THRESHOLD = 0.6  # hostility_score >= this → escalate HARD_OBJECTION


async def handle_classified_reply(
    classification: dict,
    conversation: dict,
    message_text: str,
) -> None:
    """Route to the appropriate category handler."""
    category = classification.get("category", "QUESTION")
    enrollment_id = classification.get("enrollment_id")
    conversation_id = classification.get("conversation_id")
    account_id = classification.get("account_id", "")
    prospect_id = classification.get("prospect_id")

    # Fetch supporting context
    enrollment = None
    if enrollment_id:
        enrollment = await campaign_enrollments_collection.find_one({"_id": ObjectId(enrollment_id)})

    company_profile = None
    if account_id:
        company_profile = await company_profiles_collection.find_one({"account_id": account_id})

    prospect = None
    if prospect_id:
        prospect = await prospects_collection.find_one(
            {"_id": ObjectId(prospect_id)}, {"full_name": 1, "company_name": 1}
        )

    prospect_name = (prospect or {}).get("full_name", "there")
    company_name = (prospect or {}).get("company_name", "your company")
    conversation_context = _build_context_str(conversation)

    handler_map = {
        "POSITIVE": _handle_positive,
        "QUESTION": _handle_question,
        "SOFT_OBJECTION": _handle_soft_objection,
        "HARD_OBJECTION": _handle_hard_objection,
        "OOO": _handle_ooo,
        "UNSUBSCRIBE": _handle_unsubscribe,
    }
    handler = handler_map.get(category, _handle_question)

    try:
        await handler(
            classification=classification,
            conversation=conversation,
            enrollment=enrollment,
            company_profile=company_profile or {},
            prospect_name=prospect_name,
            company_name=company_name,
            message_text=message_text,
            conversation_context=conversation_context,
            account_id=account_id,
            enrollment_id=enrollment_id,
            conversation_id=conversation_id,
            prospect_id=prospect_id,
        )
    except Exception as e:
        logger.error(f"Handler {category} failed for conversation {conversation_id}: {e}", exc_info=True)


# ── Private handlers ──────────────────────────────────────────────────────────

async def _handle_positive(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """POSITIVE: propose 3 meeting slots and send immediately."""
    from services.meeting_service import propose_meeting

    if enrollment_id:
        await propose_meeting(
            enrollment_id=enrollment_id,
            conversation_id=conversation_id,
            account_id=account_id,
            prospect_id=prospect_id,
            company_profile=company_profile,
            prospect_name=prospect_name,
            company_name=company_name,
            message_text=message_text,
            conversation_context=conversation_context,
        )
    else:
        # Fallback: generate a positive reply without full meeting flow
        banned = (company_profile or {}).get("banned_phrases", [])
        voice_profile = (company_profile or {}).get("sender_voice_profile")

        user_prompt = f"""Prospect: {prospect_name} @ {company_name}
Their message: {message_text[:800]}
Context: {conversation_context[:400]}

Express enthusiasm and propose booking a discovery call. Keep it under 80 words."""

        client = OpenRouterClient()
        result = await client.chat_completion(
            messages=[
                {"role": "system", "content": REPLY_POSITIVE_SYSTEM_PROMPT_FALLBACK},
                {"role": "user", "content": user_prompt},
            ],
            model=REPLY_MODEL,
            max_tokens=250,
            temperature=0.7,
        )
        reply_text = _redact_banned_phrases(result.get("content", "").strip(), banned)
        if reply_text:
            await _send_auto_reply(conversation_id, reply_text, classification)

    # Update prospect status
    if prospect_id:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {"$set": {"status": "engaged_positive", "last_updated_at": datetime.utcnow()}},
        )


async def _handle_question(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """QUESTION: answer and pivot toward a call. Status stays active."""
    banned = (company_profile or {}).get("banned_phrases", [])
    voice_profile = (company_profile or {}).get("sender_voice_profile")

    user_prompt = build_reply_question_prompt(
        message_text=message_text,
        conversation_context=conversation_context,
        prospect_name=prospect_name,
        company_name=company_name,
        company_profile=company_profile or {},
        voice_profile=voice_profile,
    )

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": REPLY_QUESTION_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=REPLY_MODEL,
        max_tokens=250,
        temperature=0.7,
    )
    reply_text = _redact_banned_phrases(result.get("content", "").strip(), banned)
    if reply_text:
        await _send_auto_reply(conversation_id, reply_text, classification)

    if prospect_id:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {"$set": {"status": "engaged_question", "last_updated_at": datetime.utcnow()}},
        )


async def _handle_soft_objection(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """SOFT_OBJECTION: acknowledge, reframe, pause enrollment for 3 days."""
    banned = (company_profile or {}).get("banned_phrases", [])
    objection_bank = (company_profile or {}).get("objection_bank", [])
    voice_profile = (company_profile or {}).get("sender_voice_profile")

    user_prompt = build_reply_soft_objection_prompt(
        message_text=message_text,
        conversation_context=conversation_context,
        prospect_name=prospect_name,
        company_name=company_name,
        objection_bank=objection_bank,
        company_profile=company_profile or {},
        voice_profile=voice_profile,
    )

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": REPLY_SOFT_OBJECTION_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=REPLY_MODEL,
        max_tokens=200,
        temperature=0.7,
    )
    reply_text = _redact_banned_phrases(result.get("content", "").strip(), banned)
    if reply_text:
        await _send_auto_reply(conversation_id, reply_text, classification)

    # Pause enrollment for 3 days
    if enrollment_id:
        resume_at = datetime.utcnow() + timedelta(days=3)
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {"status": "paused", "next_action_at": resume_at}},
        )

    if prospect_id:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {"$set": {"status": "engaged_objection", "last_updated_at": datetime.utcnow()}},
        )


async def _handle_hard_objection(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """HARD_OBJECTION: if hostile → escalate to human. Otherwise → graceful close."""
    signals = classification.get("signals") or {}
    hostility = float(signals.get("hostility_score", 0.0))
    banned = (company_profile or {}).get("banned_phrases", [])

    if hostility >= HOSTILE_THRESHOLD:
        # Escalate — never auto-send
        await _escalate_to_human(
            enrollment_id=enrollment_id,
            conversation_id=conversation_id,
            account_id=account_id,
            reason=f"HARD_OBJECTION with hostility_score={hostility:.2f}",
        )
        return

    # Graceful close
    user_prompt = build_reply_hard_objection_prompt(
        message_text=message_text,
        prospect_name=prospect_name,
        company_name=company_name,
        voice_profile=(company_profile or {}).get("sender_voice_profile"),
    )

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": REPLY_HARD_OBJECTION_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=REPLY_MODEL,
        max_tokens=150,
        temperature=0.6,
    )
    reply_text = _redact_banned_phrases(result.get("content", "").strip(), banned)
    if reply_text:
        await _send_auto_reply(conversation_id, reply_text, classification)

    # Close enrollment
    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {"status": "completed", "next_action_at": None}},
        )

    if prospect_id:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {"$set": {"status": "replied", "last_updated_at": datetime.utcnow()}},
        )


async def _handle_ooo(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """OOO: parse return date, pause enrollment until then."""
    import json as _json

    user_prompt = build_reply_ooo_prompt(
        message_text=message_text,
        current_date_iso=datetime.utcnow().strftime("%Y-%m-%d"),
    )

    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": REPLY_OOO_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=REPLY_MODEL,
        max_tokens=150,
        temperature=0.1,
    )

    resume_at = datetime.utcnow() + timedelta(days=7)  # default fallback
    try:
        parsed = _json.loads(result.get("content", "{}"))
        date_str = parsed.get("resume_date")
        if date_str:
            resume_at = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass

    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {
                "status": "paused_ooo",
                "ooo_resume_at": resume_at,
                "next_action_at": resume_at,
            }},
        )
    # No auto-reply for OOO


async def _handle_unsubscribe(
    classification, conversation, enrollment, company_profile,
    prospect_name, company_name, message_text, conversation_context,
    account_id, enrollment_id, conversation_id, prospect_id, **_
):
    """UNSUBSCRIBE: insert suppression, opt out enrollment, ALWAYS escalate. Never auto-send."""
    # Insert suppression records for all known identifiers
    suppression_ops = []
    conv = await conversations_collection.find_one({"_id": ObjectId(conversation_id)}) if conversation_id else None

    if conv:
        email = conv.get("prospect_email")
        if email:
            suppression_ops.append({
                "account_id": account_id,
                "identifier_type": "email",
                "identifier": email.lower(),
                "reason": "unsubscribe",
                "source": "system",
                "prospect_id": prospect_id,
                "enrollment_id": enrollment_id,
                "conversation_id": conversation_id,
            })

    if prospect_id:
        prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)}, {"linkedin": 1})
        li_url = (prospect or {}).get("linkedin")
        if li_url:
            suppression_ops.append({
                "account_id": account_id,
                "identifier_type": "linkedin_url",
                "identifier": li_url.lower(),
                "reason": "unsubscribe",
                "source": "system",
                "prospect_id": prospect_id,
                "enrollment_id": enrollment_id,
                "conversation_id": conversation_id,
            })

    for sup in suppression_ops:
        try:
            await suppressions_collection.update_one(
                {"account_id": sup["account_id"], "identifier_type": sup["identifier_type"], "identifier": sup["identifier"]},
                {"$setOnInsert": sup},
                upsert=True,
            )
        except Exception as e:
            logger.warning(f"Suppression insert failed: {e}")

    # Opt out enrollment
    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {"status": "opted_out", "next_action_at": None}},
        )

    # Update prospect status
    if prospect_id:
        await prospects_collection.update_one(
            {"_id": ObjectId(prospect_id)},
            {"$set": {"status": "unsubscribed", "last_updated_at": datetime.utcnow()}},
        )

    # Always escalate
    await _escalate_to_human(
        enrollment_id=enrollment_id,
        conversation_id=conversation_id,
        account_id=account_id,
        reason="UNSUBSCRIBE request",
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _send_auto_reply(conversation_id: str, reply_text: str, classification: dict) -> None:
    """Send a reply message via the conversation's channel."""
    from services.conversation_service import send_reply

    try:
        await send_reply(conversation_id=conversation_id, content_text=reply_text)
        await conversations_collection.update_one(
            {"_id": ObjectId(conversation_id)},
            {"$set": {"auto_reply_sent_at": datetime.now(timezone.utc)}},
        )
        logger.info(f"Auto-reply sent for conversation {conversation_id}")
    except Exception as e:
        logger.error(f"Failed to send auto-reply for conversation {conversation_id}: {e}")


async def _escalate_to_human(
    enrollment_id: Optional[str],
    conversation_id: Optional[str],
    account_id: str,
    reason: str,
) -> None:
    """Mark enrollment as awaiting human attention and create a notification."""
    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id)},
            {"$set": {
                "escalated_to_human": True,
                "escalated_reason": reason,
                "status": "awaiting_human",
                "next_action_at": None,
            }},
        )

    try:
        from services.notification_service import create_notification
        await create_notification(
            type="escalation_required",
            title="Human attention required",
            body=reason,
            prospect_id=None,
            conversation_id=conversation_id,
            channel="system",
        )
    except Exception as e:
        logger.warning(f"Failed to create escalation notification: {e}")

    logger.info(f"Escalated conversation {conversation_id} to human: {reason}")


def _build_context_str(conversation: dict) -> str:
    """Build a short conversation context string from last 4 messages."""
    messages = conversation.get("messages") or []
    recent = messages[-4:] if len(messages) > 4 else messages
    lines = []
    for m in recent:
        direction = "You" if m.get("direction") == "outbound" else "Them"
        lines.append(f"{direction}: {(m.get('content_text') or '')[:300]}")
    return "\n".join(lines)
