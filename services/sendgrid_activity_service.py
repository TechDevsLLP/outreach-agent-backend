"""
SendGrid Email Activity Service
Uses Stats API for aggregate metrics and Suppression APIs for per-email issue data.
The Messages API (Email Activity Feed) requires a paid add-on, so we use what's available.
"""

import logging
from typing import Optional
import httpx

from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

BASE_URL = "https://api.sendgrid.com/v3"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.sendgrid_api_key}",
        "Content-Type": "application/json",
    }


async def get_email_activity(start_date: str = "2025-01-01") -> dict:
    """
    Fetch email activity combining:
    - /v3/stats (daily aggregates: requests, delivered, opens, clicks, bounces, blocks)
    - /v3/suppression/blocks (per-email blocked list)
    - /v3/suppression/bounces (per-email bounce list)
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        headers = _headers()

        # Fetch all three in parallel
        stats_resp, blocks_resp, bounces_resp = await _fetch_all(
            client, headers, start_date
        )

    # Parse daily stats
    daily_stats = []
    totals = {
        "requests": 0, "delivered": 0, "unique_opens": 0,
        "clicks": 0, "bounces": 0, "blocks": 0,
        "spam_reports": 0, "deferred": 0,
    }
    for day in stats_resp:
        m = day["stats"][0]["metrics"]
        if m.get("requests", 0) == 0:
            continue
        entry = {
            "date": day["date"],
            "requests": m.get("requests", 0),
            "delivered": m.get("delivered", 0),
            "unique_opens": m.get("unique_opens", 0),
            "clicks": m.get("unique_clicks", 0),
            "bounces": m.get("bounces", 0),
            "blocks": m.get("blocks", 0),
            "spam_reports": m.get("spam_reports", 0),
            "deferred": m.get("deferred", 0),
        }
        daily_stats.append(entry)
        for k in totals:
            totals[k] += entry.get(k, 0)

    # Parse suppression lists (per-email details)
    blocks = [
        {
            "email": b["email"],
            "reason": b.get("reason", ""),
            "created": b.get("created"),
        }
        for b in blocks_resp
    ]
    bounces = [
        {
            "email": b["email"],
            "reason": b.get("reason", ""),
            "status": b.get("status", ""),
            "created": b.get("created"),
        }
        for b in bounces_resp
    ]

    return {
        "totals": totals,
        "daily_stats": sorted(daily_stats, key=lambda x: x["date"], reverse=True),
        "blocks": blocks,
        "bounces": bounces,
    }


async def _fetch_all(
    client: httpx.AsyncClient, headers: dict, start_date: str
) -> tuple[list, list, list]:
    """Fetch stats, blocks, and bounces concurrently."""
    import asyncio

    async def fetch_stats():
        resp = await client.get(
            f"{BASE_URL}/stats",
            headers=headers,
            params={"start_date": start_date, "aggregated_by": "day"},
        )
        resp.raise_for_status()
        return resp.json()

    async def fetch_blocks():
        resp = await client.get(
            f"{BASE_URL}/suppression/blocks", headers=headers
        )
        resp.raise_for_status()
        return resp.json()

    async def fetch_bounces():
        resp = await client.get(
            f"{BASE_URL}/suppression/bounces", headers=headers
        )
        resp.raise_for_status()
        return resp.json()

    return await asyncio.gather(fetch_stats(), fetch_blocks(), fetch_bounces())
