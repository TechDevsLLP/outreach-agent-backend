"""
Zoho Mail API service + provider.

Reference (verified against https://www.zoho.com/mail/help/api/using-oauth-2.html
and https://www.zoho.com/mail/help/api/email-api.html):

  OAuth authorize:  GET  {accounts_domain}/oauth/v2/auth
  Token exchange:   POST {accounts_domain}/oauth/v2/token   grant_type=authorization_code
  Token refresh:    POST {accounts_domain}/oauth/v2/token   grant_type=refresh_token
  Auth header:      Authorization: Zoho-oauthtoken <access_token>   (NOT "Bearer")
  Access token TTL: 1 hour. Refresh does not rotate the refresh token.
  Scopes:           ZohoMail.accounts.READ, ZohoMail.messages.ALL

  Get accountId:    GET  {api_domain}/api/accounts
  Send:             POST {api_domain}/api/accounts/{accountId}/messages
  Reply (threaded): POST {api_domain}/api/accounts/{accountId}/messages/{messageId}
                         body includes action="reply"
  Save draft:       POST {api_domain}/api/accounts/{accountId}/messages
                         body includes mode="draft"
  List/search:      GET  {api_domain}/api/accounts/{accountId}/messages/view
  Message content:  GET  {api_domain}/api/accounts/{accountId}/folders/{folderId}/messages/{messageId}/content
  List folders:     GET  {api_domain}/api/accounts/{accountId}/folders

Data centers: the OAuth authorize/callback round-trips an `accounts-server` value
(e.g. "https://accounts.zoho.eu") identifying the user's DC. That value is
attacker-controlled callback input, so it is resolved through the exact mapping
below before it is ever used as an outbound request origin.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx

import database
from services.email_providers.base import DraftResult, EmailProvider, ReplyMeta, SendResult
from utils.crypto import encrypt

logger = logging.getLogger(__name__)

ZOHO_ACCOUNTS_DOMAIN_DEFAULT = "https://accounts.zoho.com"
ZOHO_API_DOMAIN_DEFAULT = "https://mail.zoho.com"
ZOHO_SCOPES = "ZohoMail.accounts.READ,ZohoMail.messages.ALL"

# Keep OAuth and Mail API origins paired. Never derive a host with string
# replacement: `accounts-server` arrives through the browser callback and an
# arbitrary value here would exfiltrate the platform OAuth client secret.
ZOHO_DATA_CENTER_ORIGINS: dict[str, str] = {
    "https://accounts.zoho.com": "https://mail.zoho.com",
    "https://accounts.zoho.eu": "https://mail.zoho.eu",
    "https://accounts.zoho.in": "https://mail.zoho.in",
    "https://accounts.zoho.com.au": "https://mail.zoho.com.au",
    "https://accounts.zoho.jp": "https://mail.zoho.jp",
    "https://accounts.zohocloud.ca": "https://mail.zohocloud.ca",
    "https://accounts.zoho.com.cn": "https://mail.zoho.com.cn",
    "https://accounts.zoho.ae": "https://mail.zoho.ae",
    "https://accounts.zoho.sa": "https://mail.zoho.sa",
    "https://accounts.zoho.uk": "https://mail.zoho.uk",
}


def resolve_zoho_domains(accounts_domain: Optional[str]) -> tuple[str, str]:
    """Return allowlisted ``(accounts_origin, mail_api_origin)`` for a Zoho DC.

    Only exact HTTPS origins documented by Zoho are accepted. A single trailing
    slash is normalized for compatibility with callback values; paths, query
    strings, userinfo, alternate ports, and lookalike hosts remain rejected.
    """
    candidate = (accounts_domain or ZOHO_ACCOUNTS_DOMAIN_DEFAULT).strip()
    if candidate.endswith("/"):
        candidate = candidate[:-1]

    api_origin = ZOHO_DATA_CENTER_ORIGINS.get(candidate)
    if api_origin is None:
        raise ValueError("Unsupported Zoho accounts server")
    return candidate, api_origin


def derive_api_domain(accounts_domain: Optional[str]) -> str:
    """Map an allowlisted OAuth accounts origin to its Mail API origin."""
    return resolve_zoho_domains(accounts_domain)[1]


def _api_domain_for_account(email_account: dict) -> Optional[str]:
    """Resolve a stored account to a trusted Mail API origin.

    Historical documents may contain an origin derived from untrusted callback
    input. Ignore that stored API origin and recompute it from the allowlisted
    accounts origin so bearer tokens cannot be sent to a legacy malicious host.
    """
    try:
        return resolve_zoho_domains(email_account.get("zoho_accounts_domain"))[1]
    except ValueError:
        logger.error(
            "Refusing Zoho API request for account %s: unsupported accounts origin",
            email_account.get("_id"),
        )
        return None


def _validated_api_origin(api_domain: str) -> Optional[str]:
    """Return an exact allowlisted Mail API origin, otherwise ``None``."""
    candidate = (api_domain or "").strip()
    if candidate.endswith("/"):
        candidate = candidate[:-1]
    return candidate if candidate in ZOHO_DATA_CENTER_ORIGINS.values() else None


async def refresh_zoho_token_if_needed(email_account: dict) -> Optional[str]:
    """
    Return a valid Zoho OAuth access token, refreshing it first if expired.
    Updates the email_accounts collection (encrypted) on refresh.
    Returns None if the refresh fails.
    """
    access_token = email_account.get("oauth_access_token")
    expiry = email_account.get("oauth_token_expiry")

    if expiry:
        if isinstance(expiry, datetime):
            expiry_aware = expiry.replace(tzinfo=timezone.utc) if expiry.tzinfo is None else expiry
        else:
            return access_token  # Unknown format — use as-is

        now_aware = datetime.now(timezone.utc)
        if expiry_aware > now_aware + timedelta(seconds=60):
            return access_token  # Still valid

    refresh_token = email_account.get("oauth_refresh_token")
    if not refresh_token:
        logger.error(f"No Zoho refresh token for email account {email_account.get('email')}")
        return None

    from config import get_settings
    settings = get_settings()
    try:
        accounts_domain, _ = resolve_zoho_domains(email_account.get("zoho_accounts_domain"))
    except ValueError:
        logger.error(
            "Refusing Zoho token refresh for account %s: unsupported accounts origin",
            email_account.get("_id"),
        )
        return None

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{accounts_domain}/oauth/v2/token",
                data={
                    "client_id": settings.zoho_client_id,
                    "client_secret": settings.zoho_client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )

        if resp.status_code != 200:
            logger.error(
                f"Zoho token refresh failed for {email_account.get('email')}: {resp.text}"
            )
            return None

        token_data = resp.json()
        if "error" in token_data:
            logger.error(
                f"Zoho token refresh error for {email_account.get('email')}: {token_data['error']}"
            )
            return None

        new_access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        new_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

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

        logger.info(f"Refreshed Zoho token for {email_account.get('email')}")
        return new_access_token

    except Exception as e:
        logger.error(f"Zoho token refresh error for {email_account.get('email')}: {e}")
        return None


def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }


async def send_zoho_email(
    email_account: dict,
    to_email: str,
    subject: str,
    html_body: str,
    *,
    mode: Optional[str] = None,          # "draft" to save instead of send
    reply_to_message_id: Optional[str] = None,  # Zoho messageId being replied to
) -> Optional[dict]:
    """
    Send (or save as draft / reply to) an email via the Zoho Mail API.

    Returns a dict with `message_id` / `thread_id` (when present in the response)
    or None on failure.
    """
    access_token = await refresh_zoho_token_if_needed(email_account)
    if not access_token:
        logger.error(f"Cannot send Zoho email — no valid access token for {email_account.get('email')}")
        return None

    account_id = email_account.get("zoho_account_id")
    api_domain = _api_domain_for_account(email_account)
    from_address = email_account.get("zoho_from_address") or email_account.get("email", "")

    if not account_id or not api_domain:
        logger.error(f"Cannot send Zoho email — missing zoho_account_id for {email_account.get('email')}")
        return None

    payload = {
        "fromAddress": from_address,
        "toAddress": to_email,
        "subject": subject,
        "content": html_body,
        "mailFormat": "html",
    }
    if mode:
        payload["mode"] = mode
    if reply_to_message_id:
        payload["action"] = "reply"

    url = f"{api_domain}/api/accounts/{account_id}/messages"
    if reply_to_message_id:
        url = f"{url}/{reply_to_message_id}"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=_headers(access_token), json=payload)

        if resp.status_code not in (200, 201):
            logger.error(
                f"Zoho send failed for {from_address} -> {to_email}: "
                f"HTTP {resp.status_code}: {resp.text}"
            )
            return None

        body = resp.json()
        data = body.get("data", body) or {}
        # Zoho's response shape may be a dict or a list with one element depending on endpoint.
        if isinstance(data, list):
            data = data[0] if data else {}

        return {
            "message_id": str(data.get("messageId", "")),
            "thread_id": str(data.get("threadId", "")) or None,
        }

    except Exception as e:
        logger.error(f"Zoho send error for {from_address} -> {to_email}: {e}")
        return None


async def list_zoho_messages(
    email_account: dict,
    *,
    thread_id: Optional[str] = None,
    status: str = "all",
    limit: int = 20,
) -> list[dict]:
    """List messages (optionally scoped to a thread) via the Zoho list-emails API."""
    access_token = await refresh_zoho_token_if_needed(email_account)
    if not access_token:
        return []

    account_id = email_account.get("zoho_account_id")
    api_domain = _api_domain_for_account(email_account)
    if not account_id or not api_domain:
        return []

    params = {"status": status, "limit": limit, "sortBy": "date", "sortorder": "false"}
    if thread_id:
        params["threadId"] = thread_id

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{api_domain}/api/accounts/{account_id}/messages/view",
                headers=_headers(access_token),
                params=params,
            )
        if resp.status_code != 200:
            logger.warning(f"Zoho list messages failed: HTTP {resp.status_code}: {resp.text}")
            return []
        body = resp.json()
        data = body.get("data", [])
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Zoho list messages error: {e}")
        return []


async def get_zoho_message_content(email_account: dict, folder_id: str, message_id: str) -> str:
    """Fetch the content of a single Zoho message. Returns "" on failure."""
    access_token = await refresh_zoho_token_if_needed(email_account)
    if not access_token or not folder_id:
        return ""

    account_id = email_account.get("zoho_account_id")
    api_domain = _api_domain_for_account(email_account)
    if not account_id or not api_domain:
        return ""

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{api_domain}/api/accounts/{account_id}/folders/{folder_id}/messages/{message_id}/content",
                headers=_headers(access_token),
            )
        if resp.status_code != 200:
            return ""
        body = resp.json()
        data = body.get("data", {})
        if isinstance(data, dict):
            return data.get("content", "") or ""
        return ""
    except Exception as e:
        logger.warning(f"Zoho message content fetch error for {message_id}: {e}")
        return ""


async def get_zoho_accounts(access_token: str, api_domain: str) -> list[dict]:
    """GET /api/accounts — used at OAuth-connect time to resolve accountId + inbox folder."""
    api_domain = _validated_api_origin(api_domain)
    if not api_domain:
        logger.error("Refusing Zoho account lookup through an unsupported API origin")
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{api_domain}/api/accounts",
                headers=_headers(access_token),
            )
        if resp.status_code != 200:
            logger.error(f"Zoho get accounts failed: HTTP {resp.status_code}: {resp.text}")
            return []
        body = resp.json()
        data = body.get("data", [])
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.error(f"Zoho get accounts error: {e}")
        return []


async def get_zoho_inbox_folder_id(access_token: str, api_domain: str, account_id: str) -> Optional[str]:
    """GET /api/accounts/{accountId}/folders — resolve the Inbox folderId."""
    api_domain = _validated_api_origin(api_domain)
    if not api_domain:
        logger.error("Refusing Zoho folder lookup through an unsupported API origin")
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{api_domain}/api/accounts/{account_id}/folders",
                headers=_headers(access_token),
            )
        if resp.status_code != 200:
            return None
        body = resp.json()
        folders = body.get("data", [])
        for f in folders:
            if str(f.get("folderName", "")).lower() == "inbox" or f.get("folderType") == "1":
                return str(f.get("folderId"))
        return None
    except Exception as e:
        logger.warning(f"Zoho folders fetch error: {e}")
        return None


class ZohoProvider(EmailProvider):
    async def send(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        thread_ref: Optional[str] = None,
    ) -> Optional[SendResult]:
        # Zoho's send API carries a single `content` + `mailFormat` pair, so there is
        # no way to attach a text/plain alternative — `text_body` is accepted for
        # interface parity and intentionally unused. The HTML we send is already
        # paragraph-converted by the delivery facade, so formatting still survives.
        # in_reply_to for Zoho is the provider messageId of the message being replied to
        result = await send_zoho_email(
            self.account, to_email, subject, html_body, reply_to_message_id=in_reply_to
        )
        if not result:
            return None
        return SendResult(
            message_id=result["message_id"],
            provider="zoho",
            thread_ref=result.get("thread_id") or thread_ref,
        )

    async def reply(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        thread_ref: Optional[str] = None,
        provider_message_id: Optional[str] = None,
        in_reply_to: Optional[str] = None,
    ) -> Optional[SendResult]:
        # Zoho threads replies via POST .../messages/{messageId} with action=reply —
        # provider_message_id (the original message's Zoho messageId) is required.
        target_message_id = provider_message_id or in_reply_to
        if not target_message_id:
            logger.warning("Zoho reply requested without a provider_message_id — sending as new message")
            return await self.send(
                to_email, subject, html_body, text_body=text_body, thread_ref=thread_ref
            )

        result = await send_zoho_email(
            self.account, to_email, subject, html_body, reply_to_message_id=target_message_id
        )
        if not result:
            return None
        return SendResult(
            message_id=result["message_id"],
            provider="zoho",
            thread_ref=result.get("thread_id") or thread_ref,
        )

    async def create_draft(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        *,
        text_body: Optional[str] = None,
        in_reply_to: Optional[str] = None,
        thread_ref: Optional[str] = None,
    ) -> Optional[DraftResult]:
        result = await send_zoho_email(
            self.account, to_email, subject, html_body, mode="draft"
        )
        if not result:
            return None
        return DraftResult(
            draft_id=result["message_id"],
            provider="zoho",
            thread_ref=result.get("thread_id") or thread_ref,
        )

    async def fetch_new_replies(self, thread_ref: str, sender_email: str) -> list[ReplyMeta]:
        messages = await list_zoho_messages(self.account, thread_id=thread_ref, status="all")
        sender_lower = sender_email.lower()
        out = []
        for m in messages:
            from_addr = str(m.get("fromAddress", "")).lower()
            if from_addr == sender_lower or not from_addr:
                continue
            out.append(
                ReplyMeta(
                    provider_message_id=str(m.get("messageId", "")),
                    from_email=from_addr,
                    subject=m.get("subject", ""),
                    snippet=m.get("summary", ""),
                    date=str(m.get("sentDateInGMT", "")),
                    thread_ref=thread_ref,
                )
            )
        return out

    async def get_message_body(self, provider_message_id: str) -> str:
        folder_id = self.account.get("zoho_inbox_folder_id")
        if not folder_id:
            return ""
        return await get_zoho_message_content(self.account, folder_id, provider_message_id)

    async def verify(self) -> bool:
        access_token = await refresh_zoho_token_if_needed(self.account)
        if not access_token:
            return False
        api_domain = _api_domain_for_account(self.account)
        if not api_domain:
            return False
        accounts = await get_zoho_accounts(access_token, api_domain)
        return len(accounts) > 0
