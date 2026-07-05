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

logger = logging.getLogger(__name__)
settings = get_settings()

TIMEOUT = 30.0


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
    """Async Unipile API client for LinkedIn."""

    def __init__(self):
        self.base_url = settings.unipile_base_url.rstrip("/")
        self.headers = {
            "X-API-KEY": settings.unipile_token,
            "Authorization": f"Bearer {settings.unipile_token}",
            "Content-Type": "application/json",
        }
        self._account_id: Optional[str] = None

    async def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make an async HTTP request to Unipile API. Raises on failure."""
        url = f"{self.base_url}/{endpoint}"

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                headers=self.headers,
                **kwargs,
            )

        if response.status_code in (200, 201):
            return response.json()

        error_text = response.text
        logger.error(f"Unipile API {method} {endpoint} failed: {response.status_code} - {error_text}")
        raise UnipileAPIError(response.status_code, error_text)

    async def _request_form(self, method: str, endpoint: str, data: dict) -> dict:
        """Make a multipart/form-data request to Unipile API (required for InMail)."""
        url = f"{self.base_url}/{endpoint}"
        headers = {
            "X-API-KEY": settings.unipile_token,
            "Authorization": f"Bearer {settings.unipile_token}",
            "accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                data=data,
            )

        if response.status_code in (200, 201):
            return response.json()

        error_text = response.text
        logger.error(f"Unipile API {method} {endpoint} failed: {response.status_code} - {error_text}")
        raise UnipileAPIError(response.status_code, error_text)

    async def get_accounts(self) -> list[dict]:
        """Get all connected Unipile accounts."""
        data = await self._request("GET", "accounts")
        items = data.get("items", [])
        logger.debug(f"Unipile accounts response keys: {list(data.keys())}, items count: {len(items)}")
        for acc in items:
            logger.debug(f"  Account: id={acc.get('id')} provider={acc.get('provider')} type={acc.get('type')}")
        return items

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
        """Get messages for a specific chat."""
        data = await self._request(
            "GET", f"chats/{chat_id}/messages",
            params={"limit": limit},
        )
        logger.info(f"Retrieved {len(data.get('items', []))} messages for chat {chat_id}")
        return data

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

    async def get_user_posts(self, identifier: str, limit: int = 20) -> list[dict]:
        """
        Fetch the connected user's own LinkedIn posts via Unipile.
        Fully defensive — returns [] on any failure (endpoint shape varies by Unipile version).
        """
        try:
            account_id = await self.get_linkedin_account_id()
            data = await self._request(
                "GET",
                f"users/{identifier}/posts",
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
            logger.warning(f"Unipile posts fetch failed for {identifier}: {e}")
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
        payload = {"text": text}
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
        data = await self._request("GET", "linkedin/inmail_balance")
        logger.info(f"InMail balance: {data}")
        return data

    async def send_connection_request_async(self, profile_url: str, message: str = None):
        return await self.send_connection_request(profile_url, message)

    async def send_inmail_async(self, profile_url: str, subject: str, body: str):
        return await self.send_inmail(profile_url, body, subject)

    async def send_message_async(self, chat_id: str, message: str):
        return await self.send_message(chat_id, message)


class UnipileAPIError(Exception):
    """Raised when a Unipile API call fails."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Unipile API error {status_code}: {detail}")
