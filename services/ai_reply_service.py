"""
Generates AI draft replies when a prospect responds.
Goal: move the conversation toward booking a call.
"""
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional
from database import conversations_collection, prospects_collection, users_collection
from bson import ObjectId
from services.openrouter_service import OpenRouterClient, get_free_model

logger = logging.getLogger(__name__)

REPLY_SYSTEM_PROMPT_TEMPLATE = """You are a professional business development representative. Your goal is to reply to the prospect's message and guide the conversation toward booking a discovery call.

Guidelines:
- Be warm, genuine, and conversational — not salesy or pushy
- Acknowledge what they said specifically (show you read their message)
{booking_link_instructions}
- Keep it under 100 words
- Don't use generic phrases like "I'd love to connect" or "synergize"
- Match their communication style (formal if they're formal, casual if casual)
- Sign off with the sender's first name only

IMPORTANT: The reply should feel like a natural human response, not an AI-generated message."""


def _build_reply_system_prompt(booking_link: Optional[str], is_positive: bool = True) -> str:
    if booking_link and is_positive:
        link_instructions = (
            f"- If they're positive/interested or open to a call: include this calendar booking link so they can pick a time: {booking_link}\n"
            f'  Frame it naturally, e.g. "Here\'s my calendar if you\'d like to grab a quick 15-min slot: {booking_link}"\n'
            "- If they're neutral/asking questions: answer concisely, then pivot to a call and share the booking link\n"
            "- If they're negative/uninterested: be gracious, leave the door open, don't push — do NOT include the booking link"
        )
    else:
        link_instructions = (
            "- If they're positive/interested: suggest scheduling a call (no link available)\n"
            "- If they're negative/uninterested: be gracious, leave the door open, don't push"
        )
    return REPLY_SYSTEM_PROMPT_TEMPLATE.format(booking_link_instructions=link_instructions)


async def generate_reply_draft(
    conversation_id: str,
    inbound_message_text: str,
) -> dict:
    """
    Generate an AI draft reply for an inbound message.
    Returns: {draft_id, draft_text, generated_at, status: "pending"}
    """
    # Fetch conversation context
    conversation = await conversations_collection.find_one({"_id": ObjectId(conversation_id)})
    if not conversation:
        raise ValueError(f"Conversation {conversation_id} not found")

    prospect_id = conversation.get("prospect_id")
    account_id = conversation.get("account_id")
    prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)}) if prospect_id else None

    # Fetch booking link from the account's sender user
    booking_link: Optional[str] = None
    if account_id:
        try:
            from database import account_members_collection
            member = await account_members_collection.find_one(
                {"account_id": account_id, "role": {"$in": ["owner", "admin"]}},
                {"user_id": 1},
            )
            if member:
                user_doc = await users_collection.find_one(
                    {"_id": ObjectId(str(member["user_id"]))},
                    {"booking_link": 1},
                )
                booking_link = (user_doc or {}).get("booking_link")
        except Exception:
            pass  # Non-fatal — proceed without booking link

    # Build context for the LLM
    # Get recent message history (last 6 messages for context)
    messages = conversation.get("messages", [])
    recent_messages = messages[-6:] if len(messages) > 6 else messages

    conversation_history = ""
    for msg in recent_messages:
        direction = "You" if msg.get("direction") == "outbound" else "Them"
        text = msg.get("content_text", "")[:500]
        conversation_history += f"{direction}: {text}\n\n"

    # Build prospect context
    prospect_context = ""
    if prospect:
        prospect_context = f"""
Prospect: {prospect.get('full_name', 'Unknown')}
Title: {prospect.get('job_title', 'Unknown')}
Company: {prospect.get('company_name', 'Unknown')}
Industry: {prospect.get('industry', 'Unknown')}
"""

    user_prompt = f"""{prospect_context}

Conversation so far:
{conversation_history}

Their latest message:
{inbound_message_text}

Write a reply that naturally moves toward booking a 15-minute discovery call. Reply only with the message text, no subject line or greeting prefix."""

    system_prompt = _build_reply_system_prompt(booking_link=booking_link)

    # Generate via OpenRouter
    client = OpenRouterClient()
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=get_free_model(0),
        max_tokens=300,
    )
    draft_text = result.get("content", "")

    draft = {
        "draft_id": str(uuid.uuid4()),
        "draft_text": draft_text.strip(),
        "generated_at": datetime.now(timezone.utc),
        "status": "pending",  # pending | approved | edited | rejected
        "conversation_id": conversation_id,
        "in_response_to": inbound_message_text[:200],
    }

    # Store the draft on the conversation
    await conversations_collection.update_one(
        {"_id": ObjectId(conversation_id)},
        {
            "$set": {
                "ai_draft_reply": draft,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    logger.info(f"Generated AI reply draft for conversation {conversation_id}")
    return draft


async def regenerate_reply_draft(conversation_id: str) -> dict:
    """Regenerate a new draft for an existing conversation."""
    conversation = await conversations_collection.find_one({"_id": ObjectId(conversation_id)})
    if not conversation:
        raise ValueError("Conversation not found")

    # Get the latest inbound message
    messages = conversation.get("messages", [])
    latest_inbound = None
    for msg in reversed(messages):
        if msg.get("direction") == "inbound":
            latest_inbound = msg.get("content_text", "")
            break

    if not latest_inbound:
        raise ValueError("No inbound message to reply to")

    return await generate_reply_draft(conversation_id, latest_inbound)


async def approve_draft(conversation_id: str, edited_text: Optional[str] = None) -> dict:
    """
    Approve (and optionally edit) a draft, then send it.
    Returns the send result.
    """
    from services.conversation_service import send_reply

    conversation = await conversations_collection.find_one({"_id": ObjectId(conversation_id)})
    if not conversation:
        raise ValueError("Conversation not found")

    draft = conversation.get("ai_draft_reply")
    if not draft:
        raise ValueError("No draft to approve")

    # Use edited text if provided, otherwise use the original draft
    final_text = edited_text if edited_text else draft.get("draft_text", "")

    # Mark draft as approved/edited
    status = "edited" if edited_text else "approved"
    await conversations_collection.update_one(
        {"_id": ObjectId(conversation_id)},
        {
            "$set": {
                "ai_draft_reply.status": status,
                "ai_draft_reply.approved_at": datetime.now(timezone.utc),
                "ai_draft_reply.final_text": final_text,
            }
        },
    )

    # Send the reply
    result = await send_reply(
        conversation_id=conversation_id,
        content_text=final_text,
    )

    # Clear the draft after sending
    await conversations_collection.update_one(
        {"_id": ObjectId(conversation_id)},
        {"$set": {"ai_draft_reply": None}},
    )

    return result


async def reject_draft(conversation_id: str) -> dict:
    """Reject and clear a draft."""
    await conversations_collection.update_one(
        {"_id": ObjectId(conversation_id)},
        {"$set": {"ai_draft_reply": None}},
    )
    return {"status": "rejected"}
