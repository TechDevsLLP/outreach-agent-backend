"""
Gmail API service for sending emails and reading threads.

Uses httpx for async HTTP calls (consistent with the Zoho/SMTP providers),
avoiding the synchronous google-api-python-client library.
"""

import base64
import logging
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import httpx
from bson import ObjectId

import database
from utils.crypto import encrypt
from utils.email_html import html_to_plain_text

logger = logging.getLogger(__name__)

GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


def _attach_alternatives(
    msg: MIMEMultipart,
    html_body: str,
    text_body: Optional[str],
) -> None:
    """
    Attach the text/plain and text/html parts of a multipart/alternative message.

    Order matters: RFC 2046 says the last part is the most-preferred rendering,
    so plain text must be attached before HTML. A caller that omits `text_body`
    still gets a plain part derived from the HTML, so no message ever ships with
    an empty text alternative.
    """
    msg.attach(MIMEText(text_body if text_body is not None else html_to_plain_text(html_body), "plain"))
    msg.attach(MIMEText(html_body, "html"))


async def refresh_token_if_needed(email_account: dict) -> Optional[str]:
    """
    Return a valid OAuth access token, refreshing it first if expired.
    Updates the email_accounts collection on refresh.
    Returns None if the refresh fails.
    """
    access_token = email_account.get("oauth_access_token")
    expiry = email_account.get("oauth_token_expiry")

    # Treat as expired if within 60 seconds of expiry
    if expiry:
        if isinstance(expiry, datetime):
            expiry_aware = expiry.replace(tzinfo=timezone.utc) if expiry.tzinfo is None else expiry
        else:
            return access_token  # Unknown format — use as-is

        now_aware = datetime.now(timezone.utc)
        if expiry_aware > now_aware + timedelta(seconds=60):
            return access_token  # Still valid

    # Refresh the token
    refresh_token = email_account.get("oauth_refresh_token")
    if not refresh_token:
        logger.error(f"No refresh token for email account {email_account.get('email')}")
        return None

    from config import get_settings
    settings = get_settings()

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )

        if resp.status_code != 200:
            logger.error(
                f"Gmail token refresh failed for {email_account.get('email')}: {resp.text}"
            )
            return None

        token_data = resp.json()
        new_access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        new_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        # Persist updated token (encrypted at rest)
        await database.email_accounts_collection.update_one(
            {"_id": email_account["_id"]},
            {
                "$set": {
                    "oauth_access_token": encrypt(new_access_token),
                    "oauth_token_expiry": new_expiry,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

        logger.info(f"Refreshed Gmail token for {email_account.get('email')}")
        return new_access_token

    except Exception as e:
        logger.error(f"Gmail token refresh error for {email_account.get('email')}: {e}")
        return None


async def send_gmail_email(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    text_body: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    thread_id: Optional[str] = None,
    references: Optional[str] = None,
) -> Optional[dict]:
    """
    Send an email via Gmail API.

    Pass `in_reply_to` (the RFC Message-ID being replied to) and `thread_id`
    (Gmail's threadId) to send a threaded reply instead of a new thread.

    Returns a dict with:
      - message_id: Gmail's message ID (used as provider_message_id)
      - thread_id: Gmail thread ID (used for reply polling)
      - rfc_message_id: RFC 2822 Message-ID header value

    Returns None on failure.
    """
    access_token = await refresh_token_if_needed(email_account)
    if not access_token:
        logger.error(f"Cannot send Gmail email — no valid access token for {email_account.get('email')}")
        return None

    sender_email = email_account.get("email", "")
    rfc_message_id = f"<{uuid.uuid4()}@outflo.app>"

    # Build RFC 2822 MIME message
    msg = MIMEMultipart("alternative")
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg["Message-ID"] = rfc_message_id
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to
    _attach_alternatives(msg, html_body, text_body)

    # Base64url encode for Gmail API
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    payload = {"raw": raw}
    if thread_id:
        payload["threadId"] = thread_id

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{GMAIL_API_BASE}/messages/send",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )

        if resp.status_code not in (200, 201):
            logger.error(
                f"Gmail send failed for {sender_email} -> {to_email}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return None

        data = resp.json()
        return {
            "message_id": data.get("id", ""),
            "thread_id": data.get("threadId", ""),
            "rfc_message_id": rfc_message_id,
        }

    except Exception as e:
        logger.error(f"Gmail send error for {sender_email} -> {to_email}: {e}")
        return None


async def create_gmail_draft(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    text_body: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    thread_id: Optional[str] = None,
    references: Optional[str] = None,
) -> Optional[dict]:
    """
    Create a native Gmail draft (visible in the user's Drafts folder).

    Returns a dict with `draft_id`, `message_id`, `thread_id` or None on failure.
    """
    access_token = await refresh_token_if_needed(email_account)
    if not access_token:
        logger.error(f"Cannot create Gmail draft — no valid access token for {email_account.get('email')}")
        return None

    sender_email = email_account.get("email", "")

    msg = MIMEMultipart("alternative")
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to
    _attach_alternatives(msg, html_body, text_body)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    message_payload = {"raw": raw}
    if thread_id:
        message_payload["threadId"] = thread_id

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{GMAIL_API_BASE}/drafts",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                json={"message": message_payload},
            )

        if resp.status_code not in (200, 201):
            logger.error(
                f"Gmail draft creation failed for {sender_email} -> {to_email}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return None

        data = resp.json()
        message = data.get("message", {})
        return {
            "draft_id": data.get("id", ""),
            "message_id": message.get("id", ""),
            "thread_id": message.get("threadId", ""),
        }

    except Exception as e:
        logger.error(f"Gmail draft creation error for {sender_email} -> {to_email}: {e}")
        return None


async def get_thread_messages(
    access_token: str,
    thread_id: str,
    sender_email: str,
) -> list[dict]:
    """
    Fetch all messages in a Gmail thread and return metadata for each.

    Returns a list of dicts with:
      - id: Gmail message ID
      - from_email: sender address
      - date: datetime (UTC)
      - subject: str
      - snippet: str (short preview from Gmail)
      - is_reply: True if the sender is NOT the account owner (i.e. recipient replied)

    Returns empty list on error.
    """
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{GMAIL_API_BASE}/threads/{thread_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"format": "metadata", "metadataHeaders": ["From", "Subject", "Date"]},
            )

        if resp.status_code == 404:
            return []

        if resp.status_code != 200:
            logger.warning(
                f"Gmail thread fetch failed for thread {thread_id}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return []

        data = resp.json()
        messages = data.get("messages", [])
        sender_lower = sender_email.lower()

        result = []
        for m in messages:
            headers = {
                h["name"].lower(): h["value"]
                for h in m.get("payload", {}).get("headers", [])
            }
            from_header = headers.get("from", "")
            from_email = _extract_email(from_header)

            result.append({
                "id": m.get("id", ""),
                "from_email": from_email,
                "from_header": from_header,
                "date": headers.get("date", ""),
                "subject": headers.get("subject", ""),
                "snippet": m.get("snippet", ""),
                "is_reply": from_email.lower() != sender_lower,
            })

        return result

    except Exception as e:
        logger.error(f"Gmail thread read error for thread {thread_id}: {e}")
        return []


async def get_message_body(access_token: str, message_id: str) -> str:
    """
    Fetch the plain-text body of a single Gmail message for storing in conversations.
    Returns empty string on failure.
    """
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{GMAIL_API_BASE}/messages/{message_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                params={"format": "full"},
            )

        if resp.status_code != 200:
            return ""

        data = resp.json()
        payload = data.get("payload", {})

        # Try to find text/plain part
        body_text = _extract_body_text(payload)
        return body_text or data.get("snippet", "")

    except Exception as e:
        logger.warning(f"Gmail message body fetch error for {message_id}: {e}")
        return ""


def _extract_body_text(payload: dict) -> str:
    """Recursively extract plain text from Gmail message payload."""
    mime_type = payload.get("mimeType", "")
    if mime_type == "text/plain":
        body_data = payload.get("body", {}).get("data", "")
        if body_data:
            return base64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        text = _extract_body_text(part)
        if text:
            return text

    return ""


def _extract_email(from_header: str) -> str:
    """Extract plain email address from 'Name <email>' or bare 'email' header."""
    if "<" in from_header and ">" in from_header:
        return from_header[from_header.index("<") + 1 : from_header.index(">")].strip().lower()
    return from_header.strip().lower()
