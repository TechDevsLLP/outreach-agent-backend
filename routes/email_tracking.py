"""
Email click tracking for Gmail/Zoho/SMTP email accounts (open-rate tracking
has been removed — replies and clicks are the only tracked engagement signals).
"""

import uuid
import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

import re

from database import (
    click_tracking_tokens_collection,
    campaign_messages_collection,
    campaigns_collection,
    campaign_daily_stats_collection,
    prospects_collection,
)
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(tags=["email_tracking"])


# ---------------------------------------------------------------------------
# Click tracking
# ---------------------------------------------------------------------------

# Match <a ...href="URL"...> or <a ...href='URL'...>
# Skips mailto:, tel:, and bare anchor (#) links
_HREF_RE = re.compile(r'(<a\s[^>]*href=)(["\'])((?!mailto:|tel:|#)[^"\'>\s]+)\2', re.IGNORECASE)


async def create_click_tracking_token(
    prospect_id: str,
    email_account_id: str,
    original_url: str,
    campaign_id: Optional[str] = None,
) -> str:
    """
    Create a click-tracking token for one link in an outbound email.
    Returns the token UUID.
    """
    token = str(uuid.uuid4())
    doc = {
        "token": token,
        "prospect_id": prospect_id,
        "email_account_id": email_account_id,
        "campaign_id": campaign_id,
        "original_url": original_url,
        "clicked_at": None,
        "created_at": datetime.utcnow(),
    }
    await click_tracking_tokens_collection.insert_one(doc)
    return token


def build_click_url(token: str) -> str:
    """Return the full click-tracking redirect URL."""
    base = settings.backend_base_url.rstrip("/")
    return f"{base}/api/track/click/{token}"


async def rewrite_links_for_tracking(
    html_body: str,
    prospect_id: str,
    email_account_id: str,
    campaign_id: Optional[str] = None,
) -> str:
    """
    Rewrite all <a href="..."> links in html_body with click-tracking redirect URLs.
    Skips mailto:, tel:, and anchor (#) links.
    Returns the rewritten HTML.
    """
    # Collect all unique original URLs first to avoid creating duplicate tokens
    # for the same URL within one email
    seen: dict[str, str] = {}  # original_url -> tracking_url

    async def replace_href(match: re.Match) -> str:
        prefix = match.group(1)   # e.g. '<a href='
        quote = match.group(2)    # " or '
        original_url = match.group(3)

        if original_url in seen:
            tracking_url = seen[original_url]
        else:
            token = await create_click_tracking_token(
                prospect_id=prospect_id,
                email_account_id=email_account_id,
                original_url=original_url,
                campaign_id=campaign_id,
            )
            tracking_url = build_click_url(token)
            seen[original_url] = tracking_url

        return f"{prefix}{quote}{tracking_url}{quote}"

    # We can't use re.sub with an async replacement, so iterate matches manually
    result = []
    last_end = 0
    for match in _HREF_RE.finditer(html_body):
        result.append(html_body[last_end:match.start()])
        result.append(await replace_href(match))
        last_end = match.end()
    result.append(html_body[last_end:])

    return "".join(result)


@router.get("/api/track/click/{token}")
async def track_click(token: str):
    """
    Record an email link click and redirect to the original URL.
    Returns a 302 redirect on success, or 404 if the token is unknown.
    """
    from fastapi.responses import RedirectResponse

    try:
        tok_doc = await click_tracking_tokens_collection.find_one({"token": token})
        if not tok_doc:
            raise HTTPException(status_code=404, detail="Unknown tracking token")

        original_url = tok_doc.get("original_url", "/")
        now = datetime.utcnow()

        # Only record the first click per token
        if tok_doc.get("clicked_at") is None:
            await click_tracking_tokens_collection.update_one(
                {"_id": tok_doc["_id"]},
                {"$set": {"clicked_at": now}},
            )

            prospect_id = tok_doc.get("prospect_id")
            campaign_id = tok_doc.get("campaign_id")

            # Update prospect click history
            if prospect_id:
                try:
                    await prospects_collection.update_one(
                        {"_id": ObjectId(prospect_id)},
                        {
                            "$set": {"last_updated_at": now},
                            "$push": {
                                "outreach_history": {
                                    "event": "email_link_clicked",
                                    "channel": "email",
                                    "timestamp": now,
                                    "url": original_url,
                                }
                            },
                        },
                    )
                except Exception as e:
                    logger.warning(f"Failed to update prospect click for {prospect_id}: {e}")

            # Update campaign counters
            if campaign_id:
                try:
                    camp_oid = ObjectId(campaign_id)
                    await campaigns_collection.update_one(
                        {"_id": camp_oid},
                        {"$inc": {"emails_clicked": 1}},
                    )
                    today_str = now.strftime("%Y-%m-%d")
                    await campaign_daily_stats_collection.update_one(
                        {"campaign_id": campaign_id, "date": today_str},
                        {
                            "$inc": {"emails_clicked": 1},
                            "$setOnInsert": {
                                "emails_sent": 0,
                                "emails_replied": 0,
                                "linkedin_connections_sent": 0,
                                "linkedin_connections_accepted": 0,
                            },
                        },
                        upsert=True,
                    )
                    # Mark most recent sent message for this campaign + prospect as clicked
                    if prospect_id:
                        await campaign_messages_collection.update_one(
                            {
                                "campaign_id": camp_oid,
                                "prospect_id": prospect_id,
                                "channel": "email",
                                "status": "sent",
                            },
                            {"$set": {"status": "clicked"}},
                            # update the most recent one — sort descending by sent_at
                        )
                except Exception as e:
                    logger.warning(f"Failed to update campaign click stats: {e}")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing click tracking for token {token}: {e}")
        # Still redirect even if tracking fails
        original_url = "/"
        try:
            tok_doc = await click_tracking_tokens_collection.find_one({"token": token})
            if tok_doc:
                original_url = tok_doc.get("original_url", "/")
        except Exception:
            pass

    return RedirectResponse(url=original_url, status_code=302)
