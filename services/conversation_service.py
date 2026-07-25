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


def _require_account_id(account_id) -> str:
    """Return the canonical tenant id or fail closed before touching MongoDB."""
    if account_id is None or not str(account_id).strip():
        raise ValueError("account_id is required for conversation access")
    return str(account_id)


def _account_filter(account_id, field: str = "account_id") -> dict:
    """Match the temporary string/ObjectId storage mix without widening tenancy."""
    account_id_str = _require_account_id(account_id)
    values: list[object] = [account_id_str]
    try:
        values.append(ObjectId(account_id_str))
    except Exception:
        pass
    return {field: values[0] if len(values) == 1 else {"$in": values}}


def _conversation_scope(account_id, **extra) -> dict:
    query = _account_filter(account_id)
    query.update(extra)
    return query


async def _resolve_linkedin_provider_account(account_id) -> Optional[str]:
    """Resolve a connected Unipile account inside, and only inside, the tenant."""
    from database import linkedin_accounts_collection

    doc = await linkedin_accounts_collection.find_one(
        {
            **_account_filter(account_id),
            "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
            "unipile_status": {"$in": ["OK", "CONNECTING"]},
        },
        {"unipile_account_id": 1},
    )
    return str(doc["unipile_account_id"]) if doc else None


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
    provider_account_id: Optional[str] = None,
    provider_thread_id: Optional[str] = None,
) -> dict:
    """Find/create a conversation without ever crossing a tenant/provider boundary.

    Canonical identity is ``(account_id, channel, provider_account_id,
    provider_thread_id)``.  Email-only legacy rows may fall back to
    tenant+prospect when they have no provider identity yet.  LinkedIn never
    falls back to a globally shared prospect or chat id.
    """
    account_id = _require_account_id(account_id)
    if not prospect_id:
        raise ValueError("prospect_id is required for conversation access")
    if channel not in {"email", "linkedin"}:
        raise ValueError(f"Unsupported channel: {channel}")

    provider_account_id = str(provider_account_id) if provider_account_id else None
    provider_thread_id = str(provider_thread_id or unipile_chat_id) if (provider_thread_id or unipile_chat_id) else None

    existing = None
    if provider_account_id and provider_thread_id:
        existing = await conversations_collection.find_one(
            _conversation_scope(
                account_id,
                channel=channel,
                provider_account_id=provider_account_id,
                provider_thread_id=provider_thread_id,
            )
        )

    # Safe compatibility path: only email, only within the tenant, and only a
    # row that does not already belong to a different provider identity.
    if not existing and channel == "email":
        legacy_query = _conversation_scope(
            account_id,
            channel="email",
            prospect_id=str(prospect_id),
            provider_thread_id={"$in": [None, ""]},
        )
        existing = await conversations_collection.find_one(legacy_query)
        if existing and (provider_account_id or provider_thread_id):
            identity_set = {}
            if provider_account_id:
                identity_set["provider_account_id"] = provider_account_id
            if provider_thread_id:
                identity_set["provider_thread_id"] = provider_thread_id
            await conversations_collection.update_one(
                _conversation_scope(account_id, _id=existing["_id"]),
                {"$set": identity_set},
            )
            existing.update(identity_set)

    # LinkedIn: a connection request has no chat yet, so its thread is keyed on
    # the synthetic ``prospect:<id>`` id. Once the real chat id first appears
    # (the follow-up DM, or an inbound reply) adopt that placeholder in place
    # rather than forking a second thread for the same person.
    if (
        not existing
        and channel == "linkedin"
        and provider_account_id
        and provider_thread_id
        and not provider_thread_id.startswith("prospect:")
    ):
        placeholder = await conversations_collection.find_one(
            _conversation_scope(
                account_id,
                channel="linkedin",
                provider_account_id=provider_account_id,
                provider_thread_id=f"prospect:{prospect_id}",
            )
        )
        if placeholder:
            identity_set = {"provider_thread_id": provider_thread_id}
            if unipile_chat_id:
                identity_set["unipile_chat_id"] = unipile_chat_id
            await conversations_collection.update_one(
                _conversation_scope(account_id, _id=placeholder["_id"]),
                {"$set": identity_set},
            )
            placeholder.update(identity_set)
            existing = placeholder

    if existing:
        return existing

    if channel == "linkedin" and not (provider_account_id and provider_thread_id):
        raise ValueError(
            "provider_account_id and provider_thread_id are required for LinkedIn conversations"
        )

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
        provider_account_id=provider_account_id,
        provider_thread_id=provider_thread_id,
        created_at=now,
        updated_at=now,
    )

    result = await conversations_collection.insert_one(doc.model_dump())
    return await conversations_collection.find_one(
        _conversation_scope(account_id, _id=result.inserted_id)
    )


async def add_message_to_conversation(
    conversation_id: str,
    message: Message,
    account_id: Optional[str] = None,
    provider_account_id: Optional[str] = None,
) -> dict:
    """Tenant-scoped append with atomic provider replay de-duplication."""
    account_id = _require_account_id(account_id)
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

    query = _conversation_scope(account_id, _id=oid)
    if provider_account_id:
        query["provider_account_id"] = str(provider_account_id)
    provider_message_id = message.provider_message_id or message.unipile_message_id
    provider_message_field = (
        "messages.provider_message_id"
        if message.provider_message_id
        else "messages.unipile_message_id"
    )
    if provider_message_id:
        query[provider_message_field] = {"$ne": provider_message_id}

    result = await conversations_collection.find_one_and_update(
        query,
        update,
        return_document=True,
    )
    # A failed conditional update can mean a harmless provider replay. Return
    # the scoped conversation so callers retain their existing response shape,
    # flagged so replay-sensitive callers (e.g. webhook_service) can skip
    # re-running side effects that already fired on the first delivery.
    was_replay = False
    if not result and provider_message_id:
        replay_query = {
            **_conversation_scope(account_id, _id=oid),
            provider_message_field: provider_message_id,
        }
        if provider_account_id:
            replay_query["provider_account_id"] = str(provider_account_id)
        result = await conversations_collection.find_one(replay_query)
        was_replay = result is not None
    if result:
        result["_id"] = str(result["_id"])
        result["_was_replay"] = was_replay
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
    query = _account_filter(account_id)
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


async def get_conversation(conversation_id: str, account_id: Optional[str] = None) -> Optional[dict]:
    """Single conversation with all messages."""
    try:
        oid = ObjectId(conversation_id)
    except Exception:
        return None

    doc = await conversations_collection.find_one(_conversation_scope(account_id, _id=oid))
    if doc:
        doc = _stringify_objectids(doc)
    return doc


async def get_conversations_for_prospect(
    prospect_id: str, account_id: Optional[str] = None
) -> list[dict]:
    """All conversations for a prospect."""
    cursor = conversations_collection.find(
        _conversation_scope(account_id, prospect_id=prospect_id)
    ).sort("last_message_at", -1)

    conversations = []
    async for doc in cursor:
        doc = _stringify_objectids(doc)
        conversations.append(doc)
    return conversations


async def mark_conversation_read(
    conversation_id: str, account_id: Optional[str] = None
) -> bool:
    """Set is_read=True."""
    try:
        oid = ObjectId(conversation_id)
    except Exception:
        return False

    result = await conversations_collection.update_one(
        _conversation_scope(account_id, _id=oid),
        {"$set": {"is_read": True, "updated_at": datetime.utcnow()}},
    )
    return result.modified_count > 0


# ── Send Reply ──

async def send_reply(
    conversation_id: str,
    content_text: str,
    content_html: Optional[str] = None,
    subject: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """Dispatch reply to email (Gmail/Zoho/SMTP) or LinkedIn (Unipile), store outbound message."""
    account_id = _require_account_id(account_id)
    conversation = await get_conversation(conversation_id, account_id=account_id)
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
        to_email = conversation.get("prospect_email")
        if not to_email and prospect:
            to_email = prospect.get("email")
        if not to_email:
            raise ValueError("No email address for this conversation")

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
            from services.email_delivery_service import resolve_email_thread_context, send_reply as delivery_send_reply

            thread_ctx = await resolve_email_thread_context(conversation)
            email_account = thread_ctx["email_account"]
            if not email_account:
                raise ValueError("No connected email account available to reply from")

            result = await delivery_send_reply(
                email_account,
                to_email,
                reply_subject,
                content_html or content_text,
                thread_ref=thread_ctx.get("thread_ref"),
                provider_message_id=thread_ctx.get("provider_message_id"),
                in_reply_to=in_reply_to or thread_ctx.get("provider_message_id"),
                prospect_id=prospect_id,
            )
            if not result:
                raise ValueError("Email reply send failed — check provider credentials/logs")

            msg.provider = result.get("provider")
            msg.provider_message_id = result.get("message_id")
            msg.email_message_id = result.get("rfc_message_id") or f"<{uuid.uuid4()}@outflo.ai>"
            msg.email_in_reply_to = in_reply_to
            # Persist the bodies the facade actually put on the wire — a reply
            # composed as plain text is sent as generated HTML, and the thread
            # view should show that same rendering.
            msg.content_html = result.get("content_html") or msg.content_html
            msg.content_text = result.get("content_text") or msg.content_text
            msg.status = "sent"
        except Exception as e:
            logger.error(f"Failed to send email reply: {e}")
            msg.status = "failed"
            raise

    elif channel == "linkedin":
        # Send via Unipile
        chat_id = conversation.get("unipile_chat_id")

        try:
            from services.unipile_service import UnipileClient
            provider_account_id = conversation.get("provider_account_id")
            if not provider_account_id:
                raise ValueError("LinkedIn conversation has no provider account identity")
            client = UnipileClient(account_id=provider_account_id)

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
                        _conversation_scope(account_id, _id=ObjectId(conversation_id)),
                        {"$set": {
                            "unipile_chat_id": new_chat_id,
                            "provider_thread_id": new_chat_id,
                        }},
                    )

            msg.unipile_message_id = result.get("message_id")
            msg.status = "sent"
        except Exception as e:
            logger.error(f"Failed to send LinkedIn reply: {e}")
            msg.status = "failed"
            raise

    # If this is a LinkedIn reply to an accepted connection, mark followup as sent
    if channel == "linkedin" and prospect and msg.status == "sent":
        from services.prospect_activity_state_service import get_prospect_activity, record_prospect_activity
        activity = await get_prospect_activity(
            account_id=account_id, prospect_id=prospect_id
        )
        if activity.get("connection_accepted_at") and not activity.get("connection_followup_sent_at"):
            now = datetime.utcnow()
            await record_prospect_activity(
                account_id=account_id, prospect_id=prospect_id,
                campaign_id=conversation.get("campaign_id"),
                fields={"connection_followup_sent_at": now},
                event={
                    "event": "followup_sent", "channel": "linkedin",
                    "timestamp": now, "preview": content_text[:100],
                },
                only_if_missing="connection_followup_sent_at",
            )

    # A human just successfully replied — clear any pending escalation flag.
    if conversation.get("status") == "awaiting_human":
        await conversations_collection.update_one(
            _conversation_scope(account_id, _id=ObjectId(conversation_id)),
            {"$set": {"status": "active"}},
        )

    # Store the outbound message
    updated = await add_message_to_conversation(
        conversation_id,
        msg,
        account_id=account_id,
        provider_account_id=conversation.get("provider_account_id"),
    )
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
    account_id = _require_account_id(account_id)
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
            from services.email_delivery_service import send_email as delivery_send_email
            from services.sender_pool_service import pick_sender_for_send

            email_account = await pick_sender_for_send(account_id, "email") if account_id else None
            if not email_account:
                raise ValueError("No connected email account available to send from")

            result = await delivery_send_email(
                email_account,
                prospect_email,
                subject or "Let's connect",
                content_html or content_text,
                prospect_id=prospect_id,
            )
            if not result:
                raise ValueError("Email send failed — check provider credentials/logs")

            msg.provider = result.get("provider")
            msg.provider_message_id = result.get("message_id")
            msg.email_message_id = result.get("rfc_message_id") or f"<{uuid.uuid4()}@outflo.ai>"
            msg.content_html = result.get("content_html") or msg.content_html
            msg.content_text = result.get("content_text") or msg.content_text
            msg.status = "sent"
            provider_account_id = str(email_account["_id"])
            provider_thread_id = result.get("thread_ref")
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
            provider_account_id = await _resolve_linkedin_provider_account(account_id)
            if not provider_account_id:
                raise ValueError("No connected LinkedIn account available to send from")
            client = UnipileClient(account_id=provider_account_id)
            result = await client.start_new_chat(linkedin_url, content_text)
            msg.unipile_message_id = result.get("message_id")
            msg.status = "sent"
            chat_id = result.get("chat_id")
            provider_thread_id = chat_id
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
        provider_account_id=provider_account_id,
        provider_thread_id=provider_thread_id,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(
        conv_id,
        msg,
        account_id=account_id,
        provider_account_id=provider_account_id,
    )
    return {
        "status": "sent",
        "channel": channel,
        "conversation_id": conv_id,
        "message_id": msg.message_id,
        "conversation": updated,
    }


# ── Inbox Stats ──

async def get_inbox_stats(account_id: Optional[str] = None) -> ConversationStats:
    """Unread counts by channel, response rate, today's inbound count."""
    base_match: dict = _account_filter(account_id)

    pipeline = [
        {"$match": base_match},
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
        **base_match,
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
    provider: Optional[str] = None,
    provider_message_id: Optional[str] = None,
    email_message_id: Optional[str] = None,
    variant_id: Optional[str] = None,
    account_id: Optional[str] = None,
    provider_account_id: Optional[str] = None,
    provider_thread_id: Optional[str] = None,
) -> dict:
    """Called after sending email - creates/updates conversation with outbound message."""
    account_id = _require_account_id(account_id)
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="email",
        prospect_name=prospect_name,
        prospect_email=prospect_email,
        prospect_company=prospect_company,
        email_thread_subject=subject,
        account_id=account_id,
        provider_account_id=provider_account_id,
        provider_thread_id=provider_thread_id,
    )

    msg = Message(
        direction="outbound",
        content_text=body_text,
        content_html=body_html,
        subject=subject,
        status="sent",
        provider=provider,
        provider_message_id=provider_message_id,
        email_message_id=email_message_id,
        variant_id=variant_id,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(
        conv_id,
        msg,
        account_id=account_id,
        provider_account_id=provider_account_id,
    )
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
    provider_account_id: Optional[str] = None,
) -> dict:
    """Called after sending LinkedIn message - creates/updates conversation with outbound message."""
    account_id = _require_account_id(account_id)
    provider_account_id = provider_account_id or await _resolve_linkedin_provider_account(account_id)
    provider_thread_id = unipile_chat_id or f"prospect:{prospect_id}"
    conversation = await get_or_create_conversation(
        prospect_id=prospect_id,
        channel="linkedin",
        prospect_name=prospect_name,
        prospect_company=prospect_company,
        unipile_chat_id=unipile_chat_id,
        account_id=account_id,
        provider_account_id=provider_account_id,
        provider_thread_id=provider_thread_id,
    )

    msg = Message(
        direction="outbound",
        content_text=message_text,
        status="sent",
        unipile_message_id=unipile_message_id,
        outreach_type=outreach_type,
    )

    conv_id = str(conversation["_id"])
    updated = await add_message_to_conversation(
        conv_id,
        msg,
        account_id=account_id,
        provider_account_id=provider_account_id,
    )
    logger.info(f"Recorded outbound LinkedIn message in conversation {conv_id} for prospect {prospect_id}")
    return updated
