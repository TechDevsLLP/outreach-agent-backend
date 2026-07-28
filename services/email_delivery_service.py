"""
Email delivery facade — the single entry point every code path uses to send,
reply to, or draft an email, regardless of provider (Gmail API / Zoho Mail API /
custom SMTP+IMAP).

Click-link rewriting happens here (open-pixel tracking has been removed —
replies and clicks are the only tracked engagement signals).
"""

import logging
from typing import Optional

from bson import ObjectId

import database
from services.conversation_service import _account_filter
from services.email_providers import get_provider
from utils.email_html import build_email_bodies


def _id_variants(value) -> list:
    """Both id representations, for collections that disagree on the type.

    campaign_messages stores prospect_id as an ObjectId while conversations
    store it as a string, so an exact-type match silently finds nothing.
    """
    if value is None:
        return []
    variants: list = [str(value)]
    try:
        oid = value if isinstance(value, ObjectId) else ObjectId(str(value))
        if oid not in variants:
            variants.append(oid)
    except Exception:
        pass
    return variants

logger = logging.getLogger(__name__)


def _warmup_ok(email_account: dict) -> bool:
    """Last line before a provider call: never send from an unwarmed mailbox.

    The planner already keeps email out of campaigns whose mailbox is not
    warmed, but a message planned while it was warmed can still fire after the
    user turns the flag back off — so the check lives here too, not only
    upstream. Draft creation is deliberately not gated: a draft only becomes an
    email when a human presses send.
    """
    from services.email_warmup_gate import is_warmed_up

    if is_warmed_up(email_account):
        return True
    logger.warning(
        "[warmup-gate] Blocked send from mailbox %s (%s): not marked as warmed up",
        email_account.get("_id"),
        email_account.get("email"),
    )
    return False


async def _maybe_rewrite_clicks(
    html_body: str,
    email_account: dict,
    prospect_id: Optional[str],
    campaign_id: Optional[str],
) -> str:
    if not prospect_id:
        return html_body
    try:
        from routes.email_tracking import rewrite_links_for_tracking
        return await rewrite_links_for_tracking(
            html_body,
            prospect_id=prospect_id,
            email_account_id=str(email_account["_id"]),
            campaign_id=campaign_id,
        )
    except Exception as e:
        logger.warning(f"Failed to rewrite links for click tracking: {e}")
        return html_body


async def send_email(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    prospect_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Send a brand-new email (no existing thread). Returns a result dict or None.

    `html_body` accepts either plain text or real HTML — see `build_email_bodies`.
    The returned dict echoes the exact `content_text` / `content_html` that went
    on the wire so callers can persist what was actually sent.
    """
    if not _warmup_ok(email_account):
        return None

    provider = get_provider(email_account)
    if not provider:
        logger.error(f"No email provider available for account {email_account.get('_id')}")
        return None

    text_part, html_part = build_email_bodies(html_body)
    # Click tracking only rewrites <a href> attributes, so it applies to the
    # HTML part alone; the plain part keeps the original untracked URLs.
    html_part = await _maybe_rewrite_clicks(html_part, email_account, prospect_id, campaign_id)

    result = await provider.send(to_email, subject, html_part, text_body=text_part)
    if not result:
        return None
    return {
        "message_id": result.message_id,
        "provider": result.provider,
        "thread_ref": result.thread_ref,
        "rfc_message_id": result.rfc_message_id,
        "content_text": text_part,
        "content_html": html_part,
    }


async def send_reply(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    thread_ref: Optional[str] = None,
    provider_message_id: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    prospect_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
) -> Optional[dict]:
    """
    Send a threaded reply within an existing conversation. Returns a result dict or None.

    Accepts plain text or HTML in `html_body`, same as `send_email`. Meeting
    proposals arrive here as plain text with bulleted time slots, which is
    exactly the shape that collapsed when it was sent as HTML unconverted.
    """
    if not _warmup_ok(email_account):
        return None

    provider = get_provider(email_account)
    if not provider:
        logger.error(f"No email provider available for account {email_account.get('_id')}")
        return None

    text_part, html_part = build_email_bodies(html_body)
    html_part = await _maybe_rewrite_clicks(html_part, email_account, prospect_id, campaign_id)

    result = await provider.reply(
        to_email,
        subject,
        html_part,
        text_body=text_part,
        thread_ref=thread_ref,
        provider_message_id=provider_message_id,
        in_reply_to=in_reply_to,
    )
    if not result:
        return None
    return {
        "message_id": result.message_id,
        "provider": result.provider,
        "thread_ref": result.thread_ref,
        "rfc_message_id": result.rfc_message_id,
        "content_text": text_part,
        "content_html": html_part,
    }


async def create_mailbox_draft(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    in_reply_to: Optional[str] = None,
    thread_ref: Optional[str] = None,
) -> Optional[dict]:
    """
    Create a native draft in the provider's mailbox. Returns a result dict or None.

    A draft the user later hits Send on is a real outbound email, so it needs the
    same plain-text → HTML conversion as a direct send.
    """
    provider = get_provider(email_account)
    if not provider:
        logger.error(f"No email provider available for account {email_account.get('_id')}")
        return None

    text_part, html_part = build_email_bodies(html_body)

    result = await provider.create_draft(
        to_email, subject, html_part,
        text_body=text_part, in_reply_to=in_reply_to, thread_ref=thread_ref,
    )
    if not result:
        return None
    return {
        "draft_id": result.draft_id,
        "provider": result.provider,
        "thread_ref": result.thread_ref,
        "content_text": text_part,
        "content_html": html_part,
    }


async def resolve_email_thread_context(conversation: dict) -> dict:
    """
    Resolve everything needed to continue this conversation's email thread:
    the sending account, the provider's thread handle, and the most recent
    outbound provider_message_id (used as the reply target for Zoho, and as
    an In-Reply-To fallback for Gmail/SMTP).

    Returns {"email_account": dict|None, "thread_ref": str|None, "provider_message_id": str|None}.
    """
    prospect_id = conversation.get("prospect_id")
    account_id = conversation.get("account_id")
    result = {"email_account": None, "thread_ref": None, "provider_message_id": None}

    # The conversation is the durable record of the thread. Prefer it over
    # campaign_messages, which is scoped to a campaign and deleted along with
    # it — losing the thread handle there made every later reply start a brand
    # new email thread with the prospect.
    conv_thread_ref = conversation.get("provider_thread_id") or conversation.get("gmail_thread_id")
    if conv_thread_ref:
        result["thread_ref"] = str(conv_thread_ref)

    # Reply to the newest message actually in the thread, whichever side sent
    # it, so the In-Reply-To target is the message we are responding to.
    for msg in reversed(conversation.get("messages") or []):
        provider_msg_id = msg.get("provider_message_id") or msg.get("email_message_id")
        if provider_msg_id:
            result["provider_message_id"] = str(provider_msg_id)
            break

    conv_provider_account = conversation.get("provider_account_id")
    if conv_provider_account:
        try:
            acct = await database.email_accounts_collection.find_one(
                {"_id": ObjectId(str(conv_provider_account))}
            )
            if acct:
                result["email_account"] = acct
        except Exception:
            pass

    if prospect_id and not (result["thread_ref"] and result["email_account"]):
        last_msg = await database.campaign_messages_collection.find_one(
            {
                **_account_filter(account_id),
                # prospect_id is written as an ObjectId by campaign_engine but
                # arrives here as a string from the conversation, so match both.
                "prospect_id": {"$in": _id_variants(prospect_id)},
                "channel": "email",
                "direction": "outbound",
                "email_account_id": {"$ne": None},
            },
            sort=[("sent_at", -1)],
        )
        if last_msg:
            # Fill only what the conversation could not supply; never overwrite
            # the thread handle the conversation already established.
            if not result["thread_ref"]:
                result["thread_ref"] = (
                    last_msg.get("provider_thread_id") or last_msg.get("gmail_thread_id")
                )
            if not result["provider_message_id"]:
                result["provider_message_id"] = last_msg.get("provider_message_id")
            raw_id = last_msg.get("email_account_id")
            if raw_id and result["email_account"] is None:
                try:
                    oid = raw_id if isinstance(raw_id, ObjectId) else ObjectId(str(raw_id))
                    acct = await database.email_accounts_collection.find_one({"_id": oid})
                    if acct:
                        result["email_account"] = acct
                except Exception:
                    pass

    if result["email_account"] is None:
        if account_id:
            from services.sender_pool_service import pick_sender_for_send
            result["email_account"] = await pick_sender_for_send(str(account_id), "email")

    return result
