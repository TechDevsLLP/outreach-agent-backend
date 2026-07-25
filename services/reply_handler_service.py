"""
Reply Handler Service.
Routes classified inbound replies to category-specific handlers.

No handler ever sends to a prospect. Every category that produces copy stages it
on the conversation as ``ai_draft_reply`` (status "pending"); sending happens only
when a human approves it via ai_reply_service.approve_draft, optionally with edits.

POSITIVE stages a meeting proposal (slots and booking token are created up front so
the approved copy is exactly what was reviewed). QUESTION, SOFT_OBJECTION and
non-hostile HARD_OBJECTION stage a drafted reply. Hostile HARD_OBJECTION and
UNSUBSCRIBE produce no copy at all and escalate to a human. OOO only pauses the
enrollment until the parsed return date.

Reply latency contract: by the time this handler is called, the scheduled_reply_at
delay has already elapsed (enforced by the classifier poller in reply_classifier_service.py).
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
from services.conversation_service import _account_filter
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
    sanitize_generated_text,
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

    if not account_id:
        raise ValueError("Classified reply is missing tenant account_id")

    # Fetch supporting context
    enrollment = None
    if enrollment_id:
        enrollment = await campaign_enrollments_collection.find_one(
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)}
        )

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
    handler = handler_map.get(category)
    if handler is None:
        # Unparseable/unrecognized category (e.g. PARSE_FAILED) — never guess
        # an auto-send category, escalate to a human instead.
        await _escalate_to_human(
            enrollment_id=enrollment_id,
            conversation_id=conversation_id,
            account_id=account_id,
            reason=f"Unrecognized classification category: {category}",
        )
        return

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
    """POSITIVE: propose 3 meeting slots as a draft awaiting human approval."""
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
            draft_only=True,
        )
    else:
        # Fallback: generate a positive reply without full meeting flow
        banned = (company_profile or {}).get("banned_phrases", [])

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
        reply_text = sanitize_generated_text(_redact_banned_phrases(result.get("content", "").strip(), banned))
        if reply_text:
            # Draft for approval rather than auto-sending, matching the
            # enrollment-backed path above.
            import uuid as _uuid
            await conversations_collection.update_one(
                {"_id": ObjectId(conversation_id), **_account_filter(account_id)},
                {"$set": {
                    "ai_draft_reply": {
                        "draft_id": str(_uuid.uuid4()),
                        "draft_text": reply_text,
                        "generated_at": datetime.now(timezone.utc),
                        "status": "pending",
                        "conversation_id": conversation_id,
                        "in_response_to": (message_text or "")[:200],
                        "source": "positive_fallback",
                    },
                    "updated_at": datetime.now(timezone.utc),
                }},
            )

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
    reply_text = sanitize_generated_text(_redact_banned_phrases(result.get("content", "").strip(), banned))
    if reply_text:
        await _stage_draft_reply(conversation_id, reply_text, classification)

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
    reply_text = sanitize_generated_text(_redact_banned_phrases(result.get("content", "").strip(), banned))
    if reply_text:
        await _stage_draft_reply(conversation_id, reply_text, classification)

    # Pause enrollment for 3 days
    if enrollment_id:
        resume_at = datetime.utcnow() + timedelta(days=3)
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
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
    reply_text = sanitize_generated_text(_redact_banned_phrases(result.get("content", "").strip(), banned))
    if reply_text:
        await _stage_draft_reply(conversation_id, reply_text, classification)

    # Close enrollment
    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
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
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
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
    conv = await conversations_collection.find_one(
        {"_id": ObjectId(conversation_id), **_account_filter(account_id)}
    ) if conversation_id else None

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
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
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

async def _stage_draft_reply(conversation_id: str, reply_text: str, classification: dict) -> None:
    """Stage a reply as a pending draft for human approval.

    Nothing is ever sent to a prospect automatically. The draft lands on the
    conversation as ``ai_draft_reply`` with status "pending"; approving it via
    ai_reply_service.approve_draft (optionally with edits) is what sends it.
    """
    import uuid as _uuid

    try:
        account_id = classification.get("account_id")
        now = datetime.now(timezone.utc)
        await conversations_collection.update_one(
            {"_id": ObjectId(conversation_id), **_account_filter(account_id)},
            {"$set": {
                "ai_draft_reply": {
                    "draft_id": str(_uuid.uuid4()),
                    "draft_text": reply_text,
                    "generated_at": now,
                    "status": "pending",
                    "conversation_id": conversation_id,
                    "in_response_to": (classification.get("message_text") or "")[:200],
                    "source": f"reply_handler:{classification.get('category', 'UNKNOWN')}",
                },
                "updated_at": now,
            }},
        )
        logger.info(
            "Reply drafted (awaiting approval) for conversation %s [%s]",
            conversation_id, classification.get("category"),
        )
    except Exception as e:
        logger.error(f"Failed to stage draft reply for conversation {conversation_id}: {e}")


async def _escalate_to_human(
    enrollment_id: Optional[str],
    conversation_id: Optional[str],
    account_id: str,
    reason: str,
) -> None:
    """Mark enrollment and conversation as awaiting human attention and create a notification."""
    if enrollment_id:
        await campaign_enrollments_collection.update_one(
            {"_id": ObjectId(enrollment_id), **_account_filter(account_id)},
            {"$set": {
                "escalated_to_human": True,
                "escalated_reason": reason,
                "status": "awaiting_human",
                "next_action_at": None,
            }},
        )

    if conversation_id:
        try:
            await conversations_collection.update_one(
                {"_id": ObjectId(conversation_id), **_account_filter(account_id)},
                {"$set": {"status": "awaiting_human"}},
            )
        except Exception as e:
            logger.warning(f"Failed to set awaiting_human status on conversation {conversation_id}: {e}")

    try:
        from services.notification_service import create_notification
        await create_notification(
            account_id=account_id,
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
