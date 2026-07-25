"""
Unipile LinkedIn API Service.
Supports manual connection requests via the Unipile API.

API Reference: https://developer.unipile.com/docs/linkedin
- GET  /api/v1/accounts              → list connected accounts
- GET  /api/v1/users/{identifier}    → retrieve profile (get provider_id)
- POST /api/v1/users/invite          → send connection request
"""

import asyncio
import logging
import re
from typing import Optional
from urllib.parse import urlparse

import httpx

from config import get_settings
from utils.rate_limiter import backoff_with_jitter, parse_retry_after

logger = logging.getLogger(__name__)
settings = get_settings()

TIMEOUT = 30.0
# Retry budget for transient Unipile failures (429 rate-limited, 5xx upstream
# errors). Sending pacing/throttling itself is handled elsewhere (daily caps,
# scheduler cadence) — this is purely about not hard-failing a single call on
# a momentary blip.
MAX_RETRIES = 3


def _extract_linkedin_username(profile_url: str) -> str:
    """
    Extract the LinkedIn public identifier (username/slug) from a profile URL.

    Handles formats like:
      - https://www.linkedin.com/in/john-doe-123
      - https://linkedin.com/in/john-doe-123/
      - linkedin.com/in/john-doe-123
    """
    url = profile_url.strip().rstrip("/")

    # Try parsing as URL first
    parsed = urlparse(url if "://" in url else f"https://{url}")
    path = parsed.path.strip("/")

    # Match /in/{username} pattern
    match = re.match(r"in/([^/]+)", path)
    if match:
        return match.group(1)

    raise ValueError(f"Cannot extract LinkedIn username from URL: {profile_url}")


class UnipileClient:
    """Async Unipile API client for LinkedIn.

    Tenant-facing callers must pass the tenant-owned Unipile account id. The
    optional unbound mode remains only for account discovery/connection flows
    which intentionally enumerate the platform workspace.
    """

    def __init__(self, account_id: Optional[str] = None):
        self.base_url = settings.unipile_base_url.rstrip("/")
        self.headers = {
            "X-API-KEY": settings.unipile_token,
            "Authorization": f"Bearer {settings.unipile_token}",
            "Content-Type": "application/json",
        }
        self._account_id: Optional[str] = str(account_id) if account_id else None

    @property
    def bound_account_id(self) -> Optional[str]:
        """Return the provider account explicitly bound by the tenant caller."""
        return self._account_id

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an async HTTP request to Unipile API.

        Retries 429 (rate-limited) and 5xx (upstream error) responses up to
        MAX_RETRIES times with jittered exponential backoff, honoring a
        Retry-After header when Unipile sends one. Any other status raises
        immediately. Raises UnipileAPIError once retries are exhausted.
        """
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(MAX_RETRIES + 1):
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    headers=self.headers,
                    **kwargs,
                )

            if response.status_code in (200, 201):
                return response.json()

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    wait = retry_after if retry_after is not None else backoff_with_jitter(attempt)
                    logger.warning(
                        f"Unipile API {method} {endpoint} returned {response.status_code} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES + 1}), retry in {wait:.1f}s"
                    )
                    await asyncio.sleep(wait)
                    continue

            error_text = response.text
            logger.error(f"Unipile API {method} {endpoint} failed: {response.status_code} - {error_text}")
            raise UnipileAPIError(response.status_code, error_text)

        # Unreachable — the loop above always returns or raises.
        raise UnipileAPIError(0, "Unipile API request failed with no response")

    async def _request_form(self, method: str, endpoint: str, data: dict) -> dict:
        """Make a multipart/form-data request to Unipile API (required for InMail).

        Same 429/5xx retry behavior as `_request` (see there for details).
        """
        url = f"{self.base_url}/{endpoint}"
        headers = {
            "X-API-KEY": settings.unipile_token,
            "Authorization": f"Bearer {settings.unipile_token}",
            "accept": "application/json",
        }

        for attempt in range(MAX_RETRIES + 1):
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    data=data,
                )

            if response.status_code in (200, 201):
                return response.json()

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    retry_after = parse_retry_after(response.headers.get("Retry-After"))
                    wait = retry_after if retry_after is not None else backoff_with_jitter(attempt)
                    logger.warning(
                        f"Unipile API {method} {endpoint} returned {response.status_code} "
                        f"(attempt {attempt + 1}/{MAX_RETRIES + 1}), retry in {wait:.1f}s"
                    )
                    await asyncio.sleep(wait)
                    continue

            error_text = response.text
            logger.error(f"Unipile API {method} {endpoint} failed: {response.status_code} - {error_text}")
            raise UnipileAPIError(response.status_code, error_text)

        # Unreachable — the loop above always returns or raises.
        raise UnipileAPIError(0, "Unipile API request failed with no response")

    async def get_accounts(self) -> list[dict]:
        """Get all connected Unipile accounts."""
        data = await self._request("GET", "accounts")
        items = data.get("items", [])
        logger.debug(f"Unipile accounts response keys: {list(data.keys())}, items count: {len(items)}")
        for acc in items:
            logger.debug(f"  Account: id={acc.get('id')} provider={acc.get('provider')} type={acc.get('type')}")
        return items

    async def get_account(self) -> dict:
        """Retrieve only the explicitly bound provider account."""
        account_id = await self.get_linkedin_account_id()
        return await self._request("GET", f"accounts/{account_id}")

    async def get_linkedin_account_id(self) -> str:
        """Get the LinkedIn account ID, caching after first call."""
        if self._account_id:
            return self._account_id

        accounts = await self.get_accounts()
        for account in accounts:
            acct_type = (account.get("type") or account.get("provider") or "").upper()
            if acct_type == "LINKEDIN":
                self._account_id = account["id"]
                logger.info(f"Found LinkedIn account: {self._account_id}")
                return self._account_id

        types = [a.get("type") or a.get("provider") for a in accounts]
        logger.error(f"No LinkedIn account found. Available types: {types}")
        raise UnipileAPIError(404, f"No LinkedIn account connected in Unipile. Available types: {types}")

    async def list_chats(self, limit: int = 20) -> dict:
        """List LinkedIn chats."""
        account_id = await self.get_linkedin_account_id()
        data = await self._request(
            "GET", "chats",
            params={"account_id": account_id, "limit": limit},
        )
        logger.info(f"Listed {len(data.get('items', []))} chats")
        return data

    async def get_chat_messages(self, chat_id: str, limit: int = 50) -> dict:
        """Get messages for a chat, returning only bound-account messages."""
        account_id = await self.get_linkedin_account_id()
        data = await self._request(
            "GET", f"chats/{chat_id}/messages",
            params={"limit": limit},
        )
        items = data.get("items", [])
        owned_items = [
            message
            for message in items
            if str(message.get("account_id") or "") == account_id
        ]
        if len(owned_items) != len(items):
            logger.warning(
                "Discarded %s chat messages outside bound Unipile account",
                len(items) - len(owned_items),
            )
        scoped = dict(data)
        scoped["items"] = owned_items
        logger.info(f"Retrieved {len(owned_items)} messages for chat {chat_id}")
        return scoped

    async def get_profile(self, identifier: str) -> dict:
        """
        Retrieve a LinkedIn user profile by public identifier (username/slug).

        Returns profile dict containing provider_id needed for invitations.
        """
        account_id = await self.get_linkedin_account_id()
        data = await self._request(
            "GET",
            f"users/{identifier}",
            params={"account_id": account_id},
        )
        logger.info(f"Retrieved profile for {identifier}: provider_id={data.get('provider_id')}")
        return data

    async def get_user_posts(self, identifier: str, limit: int = 20, account_id: Optional[str] = None) -> list[dict]:
        """
        Fetch the connected user's own LinkedIn posts via Unipile.
        Fully defensive — returns [] on any failure (endpoint shape varies by Unipile version).

        Pass `account_id` (the tenant-scoped Unipile account id, e.g. linkedin_accounts.unipile_account_id)
        when the caller already knows which LinkedIn account to use — otherwise this falls back to
        get_linkedin_account_id(), which returns the FIRST LinkedIn account found across the whole
        Unipile workspace and can leak a different tenant's account in a multi-tenant deployment.
        """
        try:
            if not account_id:
                account_id = await self.get_linkedin_account_id()

            # Unipile's /users/{id}/posts endpoint requires the internal
            # provider_id, not the public slug. Resolve it first (public slugs
            # like "john-doe-123" 404 or return empty on the posts endpoint).
            provider_id = identifier
            if not identifier.startswith("ACo"):  # LinkedIn provider ids start with "ACo"
                profile = await self._request(
                    "GET",
                    f"users/{identifier}",
                    params={"account_id": account_id},
                )
                provider_id = profile.get("provider_id") or identifier
                logger.info(
                    f"[voice-sync] resolved public_id={identifier} -> provider_id={provider_id}"
                )

            data = await self._request(
                "GET",
                f"users/{provider_id}/posts",
                params={"account_id": account_id, "limit": limit},
            )
            items = data.get("items") or data.get("posts") or []
            out = []
            for p in items:
                if not isinstance(p, dict):
                    continue
                text = p.get("text") or p.get("commentary") or p.get("content") or ""
                if text and text.strip():
                    out.append({
                        "text": text.strip()[:2000],
                        "likes": p.get("reaction_count") or p.get("likes") or 0,
                        "comments": p.get("comment_count") or p.get("comments") or 0,
                        "posted_at": p.get("posted_at") or p.get("date") or "",
                    })
            return out
        except Exception as e:
            logger.error(
                f"[voice-sync] Unipile posts fetch failed for identifier={identifier} "
                f"account_id={account_id}: {e}",
                exc_info=True,
            )
            return []

    async def get_profile_data_for_enrichment(self, linkedin_url: str) -> dict | None:
        """
        Fetch LinkedIn profile via Unipile and normalize to linkedin_profile_data schema.
        Returns None on failure so caller can fall back to Apify.
        """
        try:
            username = _extract_linkedin_username(linkedin_url)
            raw = await self.get_profile(username)

            # Normalize skills: handle both string and dict formats
            raw_skills = raw.get("skills") or []
            skills = []
            for s in raw_skills:
                if isinstance(s, dict):
                    skills.append(s.get("name") or s.get("title") or str(s))
                else:
                    skills.append(str(s))

            # Normalize experience
            raw_exp = raw.get("experience") or raw.get("experiences") or []
            experience = []
            for e in raw_exp:
                if isinstance(e, dict):
                    experience.append({
                        "title": e.get("title") or e.get("position") or "",
                        "company": e.get("company_name") or e.get("company") or e.get("organization") or "",
                        "duration": e.get("duration") or e.get("date_range") or "",
                        "description": (e.get("description") or "")[:100],
                    })

            return {
                "about": (raw.get("summary") or raw.get("about") or raw.get("description") or "")[:400],
                "headline": raw.get("headline") or "",
                "skills": skills,
                "experience": experience,
                "recentPosts": [],   # Unipile doesn't provide posts
                "followersCount": raw.get("follower_count") or raw.get("followers_count") or 0,
                "connectionsCount": raw.get("connection_count") or raw.get("connections_count") or 500,
                "source": "unipile",
            }
        except Exception as e:
            logger.warning(f"Unipile profile fetch failed for {linkedin_url}: {e}")
            return None

    async def send_connection_request(
        self,
        profile_url: str,
        message: Optional[str] = None,
    ) -> dict:
        """
        Send a LinkedIn connection request.

        Flow:
          1. Extract username from LinkedIn profile URL
          2. Retrieve profile to get provider_id
          3. POST /users/invite with provider_id

        Args:
            profile_url: LinkedIn profile URL (e.g. https://linkedin.com/in/john-doe)
            message: Optional personalized note (max 300 chars, LinkedIn limit)

        Returns:
            Unipile API response dict
        """
        account_id = await self.get_linkedin_account_id()

        # Step 1: Extract username from URL
        username = _extract_linkedin_username(profile_url)
        logger.info(f"Extracted LinkedIn username: {username} from {profile_url}")

        # Step 2: Retrieve profile to get provider_id
        profile = await self.get_profile(username)
        provider_id = profile.get("provider_id")
        if not provider_id:
            raise UnipileAPIError(
                400,
                f"Could not resolve provider_id for LinkedIn user: {username}",
            )

        # Step 3: Send invitation via POST /users/invite
        payload = {
            "account_id": account_id,
            "provider_id": provider_id,
        }
        if message:
            payload["message"] = message[:300]

        result = await self._request("POST", "users/invite", json=payload)
        logger.info(f"Connection request sent to {profile_url} (provider_id={provider_id})")
        return result

    async def send_message(self, chat_id: str, text: str) -> dict:
        """
        Send a message in an existing LinkedIn chat.

        Args:
            chat_id: Unipile chat ID
            text: Message text

        Returns:
            API response with message_id
        """
        account_id = await self.get_linkedin_account_id()
        # Unipile rejects an account mismatch when account_id is present,
        # preventing a tenant from sending into another account's chat id.
        payload = {"text": text, "account_id": account_id}
        result = await self._request("POST", f"chats/{chat_id}/messages", json=payload)
        logger.info(f"Message sent in chat {chat_id}")
        return result

    async def start_new_chat(self, profile_url: str, text: str) -> dict:
        """
        Start a new LinkedIn chat with a user.

        Flow:
          1. Extract username from profile URL
          2. Retrieve profile to get provider_id
          3. POST /chats with attendees_ids and text

        Args:
            profile_url: LinkedIn profile URL
            text: Initial message text

        Returns:
            API response with chat_id and message_id
        """
        account_id = await self.get_linkedin_account_id()

        username = _extract_linkedin_username(profile_url)
        profile = await self.get_profile(username)
        provider_id = profile.get("provider_id")
        if not provider_id:
            raise UnipileAPIError(
                400,
                f"Could not resolve provider_id for LinkedIn user: {username}",
            )

        payload = {
            "account_id": account_id,
            "attendees_ids": [provider_id],
            "text": text,
        }

        result = await self._request("POST", "chats", json=payload)
        logger.info(f"New chat started with {profile_url} (provider_id={provider_id})")
        return result

    async def send_inmail(self, profile_url: str, text: str, subject: Optional[str] = None) -> dict:
        """
        Send a LinkedIn InMail to a non-connection.

        Requires the connected LinkedIn account to have Premium/Sales Navigator.
        Uses multipart/form-data with linkedin[inmail]=true.

        Args:
            profile_url: LinkedIn profile URL
            text: InMail message body
            subject: Optional InMail subject line

        Returns:
            API response dict
        """
        account_id = await self.get_linkedin_account_id()

        username = _extract_linkedin_username(profile_url)
        profile = await self.get_profile(username)
        provider_id = profile.get("provider_id")
        if not provider_id:
            raise UnipileAPIError(
                400,
                f"Could not resolve provider_id for LinkedIn user: {username}",
            )

        form_data = {
            "account_id": account_id,
            "attendees_ids": provider_id,
            "text": text,
            "linkedin[api]": "classic",
            "linkedin[inmail]": "true",
        }
        if subject:
            form_data["subject"] = subject

        result = await self._request_form("POST", "chats", form_data)
        logger.info(f"InMail sent to {profile_url} (provider_id={provider_id})")
        return result

    async def check_connection_status(self, linkedin_username: str) -> bool:
        """
        Check if a LinkedIn user is now a 1st-degree connection.
        Uses GET /users/{identifier} and checks network_distance == "FIRST_DEGREE"
        or is_relationship == True.
        """
        try:
            profile = await self.get_profile(linkedin_username)
            network_distance = profile.get("network_distance", "")
            is_relationship = profile.get("is_relationship", False)
            connected = network_distance == "FIRST_DEGREE" or is_relationship is True
            logger.debug(
                f"Connection check for {linkedin_username}: "
                f"network_distance={network_distance}, is_relationship={is_relationship}, connected={connected}"
            )
            return connected
        except Exception as e:
            logger.error(f"Error checking connection status for {linkedin_username}: {e}")
            return False

    async def get_inmail_balance(self) -> dict:
        """
        Check remaining InMail credits on the connected LinkedIn account.

        Returns:
            Dict with InMail balance info
        """
        account_id = await self.get_linkedin_account_id()
        data = await self._request(
            "GET", "linkedin/inmail_balance", params={"account_id": account_id}
        )
        logger.info(f"InMail balance: {data}")
        return data


class UnipileAPIError(Exception):
    """Raised when a Unipile API call fails."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Unipile API error {status_code}: {detail}")
