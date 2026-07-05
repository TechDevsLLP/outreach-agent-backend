"""
Conversation Service - Core business logic for the unified communications system.
Handles CRUD operations, message recording, replies, and inbox stats.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

from bson import ObjectId

from database import conversations_collection, prospects_collection
from models.conversation import (
    Message,
    ConversationDocument,
    ConversationStats,
)

logger = logging.getLogger(__name__)


# ── Helpers ──

def _stringify_objectids(obj):
    """Recursively convert any ObjectId values to strings so FastAPI can serialize them."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _stringify_objectids(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_stringify_objectids(item) for item in obj]
    return obj


def _preview_text(text: Optional[str], max_len: int = 100) -> str:
    """Truncate text for preview."""
    if not text:
        return ""
    return text[:max_len].replace("\n", " ").strip()


def _serialize_message(msg: Message) -> dict:
    """Convert a Message model to a dict for MongoDB embedding."""
    data = msg.model_dump()
    # Ensure timestamp is a datetime
    if isinstance(data.get("timestamp"), str):
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
    return data


# ── Core CRUD ──

async def get_or_create_conversation(
    prospect_id: str,
    channel: str,
    prospect_name: Optional[str] = None,
    prospect_email: Optional[str] = None,
    prospect_company: Optional[str] = None,
    unipile_chat_id: Optional[str] = None,
    email_thread_subject: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """Find existing or create new conversation by prospect+channel."""
    query = {"prospect_id": prospect_id, "channel": channel}

    # For LinkedIn, also match by chat_id if provided
    if channel == "linkedin" and unipile_chat_id:
        existing = await conversations_collection.find_one({"unipile_chat_id": unipile_chat_id})
        if existing:
            # Backfill account_id if missing and we now have it
            if account_id and not existing.get("account_id"):
                await conversations_collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"account_id": account_id}},
                )
                existing["account_id"] = account_id
            return existing

    existing = await conversations_collection.find_one(query)
    if existing:
        if account_id and not existing.get("account_id"):
            await conversations_collection.update_one(
                {"_id": existing["_id"]},
                {"$set": {"account_id": account_id}},
            )
            existing["account_id"] = account_id
        return existing

    # Create new conversation
    now = datetime.utcnow()
    doc = ConversationDocument(
        prospect_id=prospect_id,
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        prospect_company=prospect_company,
        channel=channel,
        unipile_chat_id=unipile_chat_id,
        email_thread_subject=email_thread_subject,
        account_id=account_id,
        created_at=now,
        updated_at=now,
    )

    result = await conversations_collection.insert_one(doc.model_dump())
    return await conversations_collection.find_one({"_id": result.inserted_id})


async def add_message_to_conversation(
    conversation_id: str,
    message: Message,
) -> dict:
    """Append message, update last_message_at/preview/count, set is_read=False for inbound."""
    msg_dict = _serialize_message(message)
    now = datetime.utcnow()

    update = {
        "$push": {"messages": msg_dict},
        "$set": {
            "last_message_at": message.timestamp or now,
            "last_message_preview": _preview_text(message.content_text),
            "last_message_direction": message.direction,
            "updated_at": now,
        },
        "$inc": {"message_count": 1},
    }

    # Mark unread for inbound messages
    if message.direction == "inbound":
        update["$set"]["is_read"] = False

    try:
        oid = ObjectId(conversation_id)
    except Exception:
        raise ValueError(f"Invalid conversation ID: {conversation_id}")

    result = await conversations_collection.find_one_and_update(
        {"_id": oid},
        update,
        return_document=True,
    )
    if result:
        result["_id"] = str(result["_id"])
    return result


async def list_conversations(
    page: int = 1,
    page_size: int = 20,
    channel: Optional[str] = None,
    is_read: Optional[bool] = None,
    search: Optional[str] = None,
    prospect_id: Optional[str] = None,
    sort_by: str = "last_message_at",
    sort_order: str = "desc",
    account_id: Optional[str] = None,
) -> dict:
    """Paginated list with filters."""
    query = {}

    if account_id:
        # account_id may be stored as ObjectId or string — query both
        try:
            from bson import ObjectId as _OId
            query["account_id"] = {"$in": [account_id, _OId(account_id)]}
        except Exception:
            query["account_id"] = account_id
    if channel:
        query["channel"] = channel
    if is_read is not None:
        query["is_read"] = is_read
    if prospect_id:
        query["prospect_id"] = prospect_id
    if search:
        query["$or"] = [
            {"prospect_name": {"$regex": search, "$options": "i"}},
            {"prospect_email": {"$regex": search, "$options": "i"}},
            {"last_message_preview": {"$regex": search, "$options": "i"}},
            {"prospect_company": {"$regex": search, "$options": "i"}},
        ]

    sort_dir = -1 if sort_order == "desc" else 1
    skip = (page - 1) * page_size

    # Don't return full messages array in list view
    projection = {
        "messages": 0,
    }

    total = await conversations_collection.count_documents(query)
    cursor = conversations_collection.find(query, projection).sort(
        sort_by, sort_dir
    ).skip(skip).limit(page_size)

    conversations = []
    async for doc in cursor:
        doc = _stringify_objectids(doc)
        conversations.append(doc)

    return {
        "conversations": conversations,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


async def get_conversation(conversation_id: str) -> Optional[dict]:
    """Single conversation with all messages."""
    try:
        oid = ObjectId(conversation_id)
    except Exception:
        return None

    doc = await conversations_collection.find_one({"_id": oid})
    if doc:
        doc = _stringify_objectids(doc)
    return doc


async def get_conversations_for_prospect(prospect_id: str) -> list[dict]:
    """All conversations for a prospect."""
    cursor = conversations_collection.find(
        {"prospect_id": prospect_id}
    ).sort("last_message_at", -1)

    conversations = []
    async for doc in cursor:
        doc = _stringify_objectids(doc)
        conversations.append(doc)
    return conversations


async def mark_conversation_read(conversation_id: str) -> bool:
    """Set is_read=True."""
    try:
        oid = ObjectId(conversation_id)
    except Exception:
        return False

    result = await conversations_collection.update_one(
        {"_id": oid},
        {"$set": {"is_read": True, "updated_at": datetime.utcnow()}},
    )
    return result.modified_count > 0


# ── Send Reply ──

async def send_reply(
    conversation_id: str,
    content_text: str,
    content_html: Optional[str] = None,
    subject: Optional[str] = None,
) -> dict:
    """Dispatch reply to email (SendGrid) or LinkedIn (Unipile), store outbound message."""
    conversation = await get_conversation(conversation_id)
    if not conversation:
        raise ValueError(f"Conversation not found: {conversation_id}")

    channel = conversation["channel"]
    prospect_id = conversation["prospect_id"]

    # Look up prospect for email/LinkedIn info
    try:
        prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
    except Exception:
        prospect = None

    msg = Message(
        direction="outbound",
        content_text=content_text,
        content_html=content_html,
        subject=subject,
        status="sent",
    )

    if channel == "email":
        # Send via SendGrid
        to_email = conversation.get("prospect_email")
        if not to_email and prospect:
            to_email = prospect.get("email")
        if not to_email:
            raise ValueError("No email address for this conversation")

        to_name = conversation.get("prospect_name", "")
        reply_subject = subject or conversation.get("email_thread_subject", "Re: Following up")
        if not reply_subject.startswith("Re:"):
            reply_subject = f"Re: {reply_subject}"

        # Find the last email_message_id for In-Reply-To
        in_reply_to = None
        for m in reversed(conversation.get("messages", [])):
            if m.get("email_message_id"):
                in_reply_to = m["email_message_id"]
                break

        try:
            from services.email_sender_service import EmailSender
            sender = EmailSender()
            rfc_message_id = f"<{uuid.uuid4()}@outflo.ai>"
            result = await sender.send_cold_email(
                to_email=to_email,
                to_name=to_name,
                subject=reply_subject,
                body=content_html or content_text,
                custom_args={"prospect_id": prospect_id},
            )
            msg.sendgrid_message_id = result.get("message_id")
            msg.email_message_id = rfc_message_id
            msg.email_in_reply_to = in_reply_to
            msg.status = result.get("status", "sent")
        except Exception as e:
            logger.error(f"Failed to send email reply: {e}")
            msg.status = "failed"
            raise

    elif channel == "linkedin":
        # Send via Unipile
        chat_id = conversation.get("unipile_chat_id")

        try:
            from services.unipile_service import UnipileClient
            client = UnipileClient()

            if chat_id:
                # Existing chat — send message in thread
                result = await client.send_message(chat_id, content_text)
            else:
                # No chat_id — start a new chat using prospect's LinkedIn URL
                linkedin_url = None
                if prospect:
                    linkedin_url = prospect.get("linkedin")
                if not linkedin_url:
                    raise ValueError("No LinkedIn URL found for this prospect. Cannot send message.")
                result = await client.start_new_chat(linkedin_url, content_text)
                # Save the chat_id for future messages
                new_chat_id = result.get("chat_id")
                if new_chat_id:
                    await conversations_collection.update_one(
                        {"_id": ObjectId(conversation_id)},
                        {"$set": {"unipile_chat_id": new_chat_id}},
                    )

            msg.unipile_message_id = result.get("message_id")
            msg.status = "sent"
        except Exception as e:
            logger.error(f"Failed to send LinkedIn reply: {e}")
            msg.status = "failed"
            raise

    # If this is a LinkedIn reply to an accepted connection, mark followup as sent
    if channel == "linkedin" and prospect and msg.status == "sent":
        if prospect.get("connection_accepted_at") and not prospect.get("connection_followup_sent_at"):
            await prospects_collection.update_one(
                {"_id": ObjectId(prospect_id)},
                {
                    "$set": {"connection_followup_sent_at": datetime.utcnow()},
                    "$push": {
                        "outreach_history": {
                            "event": "followup_sent",
                            "channel": "linkedin",
                            "timestamp": datetime.utcnow(),
                            "preview": content_text[:100],
                        }
                    },
                },
            )

    # Store the outbound message
    updated = await add_message_to_conversation(conversation_id, msg)
    return {
        "status": "sent",
        "channel": channel,
        "message_id": msg.message_id,
        "conversation": updated,
    }


# ── Compose New Message ──

async def compose_new_message(
    prospect_id: str,
    channel: str,
    content_text: str,
    content_html: Optional[str] = None,
    subject: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """Start new conversation - compose email or start LinkedIn chat."""
    try:
        prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
    except Exception:
        prospect = None

    if not prospect:
        raise ValueError(f"Prospect not found: {prospect_id}")

    prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
    prospect_email = prospect.get("email")
    prospect_company = prospect.get("company_name")

    msg = Message(
        direction="outbound",
        content_text=content_text,
        content_html=content_html,
        subject=subject,
        status="sent",
    )

    if channel == "email":
        if not prospect_email:
            raise ValueError("Prospect has no email address")

        try:
            from services.email_sender_service import EmailSender
            sender = EmailSender()
            rfc_message_id = f"<{uuid.uuid4()}@outflo.ai>"
            result = await sender.send_cold_email(
                to_email=prospect_email,
                to_name=prospect_name,
                subject=subject or "Let's connect",
                body=content_html or content_text,
                custom_args={"prospect_id": prospect_id},
            )
            msg.sendgrid_message_id = result.get("message_id")
            msg.email_message_id = rfc_message_id
            msg.status = result.get("status", "sent")
        except Exception as e:
            logger.error(f"Failed to compose email: {e}")
            msg.status = "failed"
            raise

    elif channel == "linkedin":
        linkedin_url = prospect.get("linkedin")
        if not linkedin_url:
            raise ValueError("Prospect has no LinkedIn URL")

        try:
            from services.unipile_service import UnipileClient
            client = UnipileClient()
            result = await client.start_new_chat(linkedin_url, content_text)
            msg.unipile_message_id = result.get("message_id")
            msg.status = "sent"
            chat_id = result.get("chat_id")
        except Exception as e:
            logger.error(f"Failed to start LinkedIn chat: {e}")
            msg.status = "failed"
            raise
    else:
        raise ValueError(f"Unsupported channel: {channel}")

    # Create conversation
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel=channel,
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        prospect_company=prospect_company,
        unipile_chat_id=chat_id if channel == "linkedin" else None,
        email_thread_subject=subject if channel == "email" else None,
        account_id=account_id,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(conv_id, msg)
    return {
        "status": "sent",
        "channel": channel,
        "conversation_id": conv_id,
        "message_id": msg.message_id,
        "conversation": updated,
    }


# ── Message Status Updates ──

async def update_message_status(sendgrid_message_id: str, new_status: str) -> bool:
    """Update delivery status by sendgrid_message_id."""
    result = await conversations_collection.update_one(
        {"messages.sendgrid_message_id": sendgrid_message_id},
        {"$set": {
            "messages.$[elem].status": new_status,
            "updated_at": datetime.utcnow(),
        }},
        array_filters=[{"elem.sendgrid_message_id": sendgrid_message_id}],
    )
    return result.modified_count > 0


# ── Inbox Stats ──

async def get_inbox_stats(account_id: Optional[str] = None) -> ConversationStats:
    """Unread counts by channel, response rate, today's inbound count."""
    base_match: dict = {}
    if account_id:
        try:
            from bson import ObjectId as _OId
            base_match["account_id"] = {"$in": [account_id, _OId(account_id)]}
        except Exception:
            base_match["account_id"] = account_id

    pipeline = [
        *([ {"$match": base_match} ] if base_match else []),
        {"$facet": {
            "total_unread": [
                {"$match": {"is_read": False}},
                {"$count": "count"},
            ],
            "unread_email": [
                {"$match": {"is_read": False, "channel": "email"}},
                {"$count": "count"},
            ],
            "unread_linkedin": [
                {"$match": {"is_read": False, "channel": "linkedin"}},
                {"$count": "count"},
            ],
            "total_conversations": [
                {"$count": "count"},
            ],
            "with_inbound": [
                {"$match": {"last_message_direction": "inbound"}},
                {"$count": "count"},
            ],
        }}
    ]

    result = await conversations_collection.aggregate(pipeline).to_list(1)
    facets = result[0] if result else {}

    total_unread = facets.get("total_unread", [{}])[0].get("count", 0) if facets.get("total_unread") else 0
    unread_email = facets.get("unread_email", [{}])[0].get("count", 0) if facets.get("unread_email") else 0
    unread_linkedin = facets.get("unread_linkedin", [{}])[0].get("count", 0) if facets.get("unread_linkedin") else 0
    total_conversations = facets.get("total_conversations", [{}])[0].get("count", 0) if facets.get("total_conversations") else 0
    with_inbound = facets.get("with_inbound", [{}])[0].get("count", 0) if facets.get("with_inbound") else 0

    response_rate = (with_inbound / total_conversations * 100) if total_conversations > 0 else 0.0

    # Today's inbound count
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_inbound = await conversations_collection.count_documents({
        "last_message_direction": "inbound",
        "last_message_at": {"$gte": today_start},
    })

    return ConversationStats(
        total_unread=total_unread,
        unread_email=unread_email,
        unread_linkedin=unread_linkedin,
        total_conversations=total_conversations,
        response_rate=round(response_rate, 1),
        today_inbound=today_inbound,
    )


# ── Outbound Recording (called after sending emails/LinkedIn messages) ──

async def record_outbound_email(
    prospect_id: str,
    prospect_name: Optional[str] = None,
    prospect_email: Optional[str] = None,
    prospect_company: Optional[str] = None,
    subject: Optional[str] = None,
    body_text: Optional[str] = None,
    body_html: Optional[str] = None,
    sendgrid_message_id: Optional[str] = None,
    email_message_id: Optional[str] = None,
    variant_id: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """Called after sending email - creates/updates conversation with outbound message."""
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="email",
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        prospect_company=prospect_company,
        email_thread_subject=subject,
        account_id=account_id,
    )

    msg = Message(
        direction="outbound",
        content_text=body_text,
        content_html=body_html,
        subject=subject,
        status="sent",
        sendgrid_message_id=sendgrid_message_id,
        email_message_id=email_message_id,
        variant_id=variant_id,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(conv_id, msg)
    logger.info(f"Recorded outbound email in conversation {conv_id} for prospect {prospect_id}")
    return updated


async def record_outbound_linkedin_message(
    prospect_id: str,
    prospect_name: Optional[str] = None,
    prospect_company: Optional[str] = None,
    message_text: Optional[str] = None,
    unipile_chat_id: Optional[str] = None,
    unipile_message_id: Optional[str] = None,
    outreach_type: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """Called after sending LinkedIn message - creates/updates conversation with outbound message."""
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="linkedin",
        prospect_name=prospect_name,
        prospect_company=prospect_company,
        unipile_chat_id=unipile_chat_id,
        account_id=account_id,
    )

    msg = Message(
        direction="outbound",
        content_text=message_text,
        status="sent",
        unipile_message_id=unipile_message_id,
        outreach_type=outreach_type,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(conv_id, msg)
    logger.info(f"Recorded outbound LinkedIn message in conversation {conv_id} for prospect {prospect_id}")
    return updated


# ── Prospect Matching ──

async def find_prospect_by_email(email: str) -> Optional[dict]:
    """Match inbound email sender to prospect (checks both email and personal_email)."""
    email_lower = email.lower().strip()
    prospect = await prospects_collection.find_one({
        "$or": [
            {"email": {"$regex": f"^{email_lower}$", "$options": "i"}},
            {"personal_email": {"$regex": f"^{email_lower}$", "$options": "i"}},
        ]
    })
    return prospect
