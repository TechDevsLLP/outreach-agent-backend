"""
LinkedIn Outreach Routes - Manual Connection Requests via Unipile
"""

import logging
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from bson import ObjectId

import database
from database import prospects_collection
from auth import get_account_context
from services.unipile_service import UnipileClient, UnipileAPIError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/linkedin", tags=["linkedin_outreach"])


# ── Request Models ──

class ConnectionRequestPayload(BaseModel):
    prospect_id: str
    message: Optional[str] = None  # Custom message; if None, uses generated outreach
    use_generated: bool = True


class LinkedInMessagePayload(BaseModel):
    prospect_id: str
    text: Optional[str] = None
    use_generated: bool = True
    chat_id: Optional[str] = None


class DirectLinkedInMessagePayload(BaseModel):
    profile_url: str
    text: str


class InMailPayload(BaseModel):
    prospect_id: str
    subject: Optional[str] = None
    message: Optional[str] = None
    use_generated: bool = True


class DirectInMailPayload(BaseModel):
    profile_url: str
    subject: str
    message: str


# ── Helpers ──

async def _tenant_unipile_client(account_ctx: dict) -> UnipileClient:
    """Return a client bound to an authenticated tenant-owned LinkedIn account."""
    account_id = str(account_ctx["account"]["_id"])
    doc = await database.linkedin_accounts_collection.find_one(
        {
            "account_id": account_id,
            "unipile_account_id": {"$exists": True, "$nin": [None, ""]},
            "unipile_status": {"$nin": ["DELETED", "STOPPED"]},
        },
        sort=[("is_default", -1), ("created_at", -1)],
    )
    if not doc:
        raise HTTPException(
            status_code=404,
            detail="No LinkedIn account is connected for this workspace",
        )
    return UnipileClient(account_id=str(doc["unipile_account_id"]))


async def _get_prospect(prospect_id: str, account_id: str) -> dict:
    """Fetch and validate a prospect by ID."""
    try:
        oid = ObjectId(prospect_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid prospect ID")

    from services.prospect_activity_state_service import require_prospect_access, get_prospect_activity
    try:
        await require_prospect_access(account_id=account_id, prospect_id=prospect_id)
    except PermissionError:
        raise HTTPException(status_code=404, detail="Prospect not found")
    prospect = await prospects_collection.find_one(
        {"_id": oid},
        {"full_name": 1, "first_name": 1, "company_name": 1, "linkedin": 1},
    )
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")
    prospect.update(await get_prospect_activity(account_id=account_id, prospect_id=prospect_id))
    return prospect


def _get_linkedin_url(prospect: dict) -> str:
    """Extract LinkedIn URL from prospect, raising if missing."""
    linkedin_url = prospect.get("linkedin")
    if not linkedin_url:
        raise HTTPException(
            status_code=400,
            detail="Prospect has no LinkedIn URL. Cannot send LinkedIn outreach.",
        )
    return linkedin_url


def _get_generated_connection_message(prospect: dict) -> Optional[str]:
    """Extract AI-generated connection request message from prospect outreach data."""
    outreach = prospect.get("outreach_messages", {})
    conn = outreach.get("linkedin_connection_request", {})
    return conn.get("message") or conn.get("body") or conn.get("text")


async def _record_outreach_event(
    account_id: str, prospect_id: str, event_type: str, channel: str,
    detail: dict, *, fields: dict | None = None,
):
    """Record an outreach event in the prospect's history."""
    history_entry = {
        "event": event_type,
        "channel": channel,
        "timestamp": datetime.utcnow(),
        **detail,
    }

    from services.prospect_activity_state_service import record_prospect_activity
    await record_prospect_activity(
        account_id=account_id, prospect_id=prospect_id,
        fields=fields, event=history_entry,
    )


# ── GET Endpoints (must be before POST to avoid path conflicts) ──

@router.get("/account")
async def get_linkedin_account(
    account_ctx: dict = Depends(get_account_context),
):
    """Get the connected LinkedIn account info."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        linkedin_account = await client.get_account()
        return {"status": "success", "account": linkedin_account}
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/chats")
async def list_chats(
    limit: int = 20,
    account_ctx: dict = Depends(get_account_context),
):
    """List LinkedIn chats."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        data = await client.list_chats(limit=limit)
        chats = data.get("items", [])
        return {"status": "success", "count": len(chats), "chats": chats}
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


@router.get("/chats/{chat_id}/messages")
async def get_chat_messages(
    chat_id: str,
    limit: int = 50,
    account_ctx: dict = Depends(get_account_context),
):
    """Get messages for a specific LinkedIn chat."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        data = await client.get_chat_messages(chat_id, limit=limit)
        messages = data.get("items", [])
        return {"status": "success", "count": len(messages), "messages": messages}
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


# ── Connection Request Endpoint ──

@router.post("/connect")
async def send_connection_request(
    payload: ConnectionRequestPayload,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Manually send a LinkedIn connection request to a single prospect.

    Uses the AI-generated connection request message by default,
    or a custom message if provided.
    """
    account_id = str(account_ctx["account"]["_id"])
    prospect = await _get_prospect(payload.prospect_id, account_id)
    linkedin_url = _get_linkedin_url(prospect)

    message = payload.message
    if not message and payload.use_generated:
        message = _get_generated_connection_message(prospect)

    try:
        client = await _tenant_unipile_client(account_ctx)
        result = await client.send_connection_request(linkedin_url, message)
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    await _record_outreach_event(
        account_id,
        payload.prospect_id,
        "connection_request_sent",
        "linkedin",
        {"profile_url": linkedin_url, "message_preview": (message or "")[:100]},
        fields={"status": "contacted", "connection_request_sent_at": datetime.utcnow()},
    )

    # Record in conversations
    try:
        from services.conversation_service import record_outbound_linkedin_message
        prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
        await record_outbound_linkedin_message(
            prospect_id=payload.prospect_id,
            prospect_name=prospect_name,
            prospect_company=prospect.get("company_name"),
            message_text=message or "[Connection request]",
            outreach_type="connection_request",
            account_id=account_id,
            provider_account_id=client.bound_account_id,
        )
    except Exception as e:
        logger.warning(f"Failed to record connection request in conversations: {e}")

    return {
        "status": "success",
        "message": "Connection request sent",
        "prospect_id": payload.prospect_id,
        "linkedin_url": linkedin_url,
        "unipile_response": result,
    }


# ── LinkedIn Message Endpoints ──

@router.post("/message")
async def send_linkedin_message(
    payload: LinkedInMessagePayload,
    account_ctx: dict = Depends(get_account_context),
):
    """Send a LinkedIn DM to an existing connection."""
    account_id = str(account_ctx["account"]["_id"])
    prospect = await _get_prospect(payload.prospect_id, account_id)

    text = payload.text
    if not text and payload.use_generated:
        outreach = prospect.get("outreach_messages", {})
        linkedin_msg = outreach.get("linkedin_message", {})
        text = linkedin_msg.get("message") or linkedin_msg.get("body") or linkedin_msg.get("text")

    if not text:
        raise HTTPException(status_code=400, detail="No message text provided or generated")

    chat_id = payload.chat_id
    client = await _tenant_unipile_client(account_ctx)

    try:
        if chat_id:
            result = await client.send_message(chat_id, text)
        else:
            linkedin_url = _get_linkedin_url(prospect)
            result = await client.start_new_chat(linkedin_url, text)
            chat_id = result.get("chat_id")
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    await _record_outreach_event(
        account_id,
        payload.prospect_id,
        "linkedin_message_sent",
        "linkedin",
        {"message_preview": text[:100], "chat_id": chat_id},
        fields={"status": "contacted"},
    )

    # Record in conversations
    try:
        from services.conversation_service import record_outbound_linkedin_message
        prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
        await record_outbound_linkedin_message(
            prospect_id=payload.prospect_id,
            prospect_name=prospect_name,
            prospect_company=prospect.get("company_name"),
            message_text=text,
            unipile_chat_id=chat_id,
            unipile_message_id=result.get("message_id"),
            outreach_type="linkedin_message",
            account_id=account_id,
            provider_account_id=client.bound_account_id,
        )
    except Exception as e:
        logger.warning(f"Failed to record LinkedIn message in conversations: {e}")

    return {
        "status": "success",
        "message": "LinkedIn message sent",
        "prospect_id": payload.prospect_id,
        "unipile_response": result,
    }


@router.post("/message/direct")
async def send_linkedin_message_direct(
    payload: DirectLinkedInMessagePayload,
    account_ctx: dict = Depends(get_account_context),
):
    """Send a LinkedIn message directly using profile URL and text."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        result = await client.start_new_chat(payload.profile_url, payload.text)
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    return {
        "status": "success",
        "message": "LinkedIn message sent",
        "profile_url": payload.profile_url,
        "unipile_response": result,
    }


# ── InMail Endpoints ──

@router.get("/inmail/balance")
async def get_inmail_balance(
    account_ctx: dict = Depends(get_account_context),
):
    """Check remaining InMail credits on the connected LinkedIn account."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        balance = await client.get_inmail_balance()
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    return {"status": "success", "balance": balance}


@router.post("/inmail")
async def send_inmail(
    payload: InMailPayload,
    account_ctx: dict = Depends(get_account_context),
):
    """
    Send a LinkedIn InMail to a prospect (no connection required).

    Requires LinkedIn Premium on the connected account.
    Uses AI-generated cold email subject/body by default.
    """
    account_id = str(account_ctx["account"]["_id"])
    prospect = await _get_prospect(payload.prospect_id, account_id)
    linkedin_url = _get_linkedin_url(prospect)

    subject = payload.subject
    message = payload.message

    if payload.use_generated:
        outreach = prospect.get("outreach_messages", {})
        # Primary: dedicated linkedin_inmail outreach data
        inmail_data = outreach.get("linkedin_inmail", {})
        # Fallback: cold_email data
        cold_email = outreach.get("cold_email", {})
        if not subject:
            subject = inmail_data.get("subject") or cold_email.get("subject")
        if not message:
            message = (
                inmail_data.get("message") or inmail_data.get("body")
                or cold_email.get("body") or cold_email.get("message") or cold_email.get("text")
            )

    if not subject or not message:
        available_keys = list(prospect.get("outreach_messages", {}).keys())
        raise HTTPException(
            status_code=400,
            detail=f"No subject/message provided and no generated outreach found. "
                   f"Available outreach keys: {available_keys}. "
                   f"Provide both subject and message, or run enrichment first.",
        )

    try:
        client = await _tenant_unipile_client(account_ctx)
        result = await client.send_inmail(linkedin_url, message, subject)
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    await _record_outreach_event(
        account_id,
        payload.prospect_id,
        "inmail_sent",
        "linkedin",
        {"subject": subject, "message_preview": message[:100], "profile_url": linkedin_url},
        fields={"status": "contacted"},
    )

    # Record in conversations
    try:
        from services.conversation_service import record_outbound_linkedin_message
        prospect_name = prospect.get("full_name") or prospect.get("first_name", "")
        await record_outbound_linkedin_message(
            prospect_id=payload.prospect_id,
            prospect_name=prospect_name,
            prospect_company=prospect.get("company_name"),
            message_text=f"[InMail] {subject}\n\n{message}",
            outreach_type="inmail",
            account_id=account_id,
            provider_account_id=client.bound_account_id,
        )
    except Exception as e:
        logger.warning(f"Failed to record InMail in conversations: {e}")

    return {
        "status": "success",
        "message": "InMail sent",
        "prospect_id": payload.prospect_id,
        "linkedin_url": linkedin_url,
        "unipile_response": result,
    }


@router.post("/inmail/direct")
async def send_inmail_direct(
    payload: DirectInMailPayload,
    account_ctx: dict = Depends(get_account_context),
):
    """Send an InMail directly using a profile URL, subject, and message."""
    try:
        client = await _tenant_unipile_client(account_ctx)
        result = await client.send_inmail(payload.profile_url, payload.message, payload.subject)
    except UnipileAPIError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)

    return {
        "status": "success",
        "message": "InMail sent",
        "profile_url": payload.profile_url,
        "unipile_response": result,
    }
