# version2/routes/activity_feed.py
from fastapi import APIRouter, Depends, Query
from typing import Optional
from datetime import datetime
from auth import get_account_context
from services.activity_feed_service import get_activity_feed, get_activity_summary

router = APIRouter(tags=["Activity Feed"])


@router.get("/api/activity-feed")
async def list_activity(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    activity_type: Optional[str] = None,
    channel: Optional[str] = None,
    since: Optional[str] = None,
    prospect_id: Optional[str] = None,
    account_ctx=Depends(get_account_context),
):
    account_id = str(account_ctx["account"]["_id"])
    since_dt = None
    if since:
        since_dt = datetime.fromisoformat(since)

    result = await get_activity_feed(
        page=page,
        page_size=page_size,
        activity_type=activity_type,
        channel=channel,
        since=since_dt,
        prospect_id=prospect_id,
        account_id=account_id,
    )
    return result


@router.get("/api/activity-feed/summary")
async def activity_summary(account_ctx=Depends(get_account_context)):
    account_id = str(account_ctx["account"]["_id"])
    return await get_activity_summary(account_id=account_id)
