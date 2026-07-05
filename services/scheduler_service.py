"""
Scheduled scraping service using APScheduler.
Runs a cron job (default: every Saturday at 6am) to scrape prospects for all active industries.
Each industry has 3 regions (USA, Europe, India) with configurable fetch counts and redistribution.
"""

import asyncio
import logging
from datetime import datetime
from bson import ObjectId
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import get_settings
from database import industries_collection
from services.prospect_service import process_search_for_region
from services.connection_tracker_service import check_pending_connections, check_pending_linkedin_replies
from services.campaign_engine import execute_pending_steps, compute_campaign_daily_stats
from services.gmail_reply_poller import check_gmail_replies

logger = logging.getLogger(__name__)

settings = get_settings()

scheduler = AsyncIOScheduler()


def start_scheduler():
    """Start the APScheduler with the configured cron triggers."""
    # Weekly scraping job
    scrape_trigger = CronTrigger(
        day_of_week=settings.scrape_day_of_week,
        hour=settings.scrape_hour,
        minute=settings.scrape_minute,
    )
    scheduler.add_job(
        scheduled_scrape_all_industries,
        trigger=scrape_trigger,
        id="weekly_scrape",
        name="Weekly industry scraping",
        replace_existing=True,
    )

    scheduler.add_job(
        check_pending_connections,
        "interval",
        minutes=5,
        id="check_pending_connections",
        name="Check pending LinkedIn connections",
        replace_existing=True,
    )

    scheduler.add_job(
        check_pending_linkedin_replies,
        "interval",
        minutes=5,
        id="check_linkedin_replies",
        name="Check pending LinkedIn replies for sequence campaigns",
        replace_existing=True,
    )

    scheduler.add_job(
        execute_pending_steps,
        "interval",
        minutes=5,
        id="campaign_engine",
        name="Campaign execution engine",
        replace_existing=True,
    )

    scheduler.add_job(
        compute_campaign_daily_stats,
        CronTrigger(hour=0, minute=5),
        id="campaign_daily_stats",
        name="Campaign daily stats aggregation",
        replace_existing=True,
    )

    scheduler.add_job(
        check_gmail_replies,
        "interval",
        minutes=5,
        id="check_gmail_replies",
        name="Check Gmail replies for campaign emails",
        replace_existing=True,
    )

    # Daily cap reset at 00:01 UTC
    scheduler.add_job(
        _reset_daily_caps,
        CronTrigger(hour=0, minute=1),
        id="reset_daily_caps",
        name="Reset campaign daily caps",
        replace_existing=True,
    )

    # Reply classifier processor — every 20s to hit the 45-60s LinkedIn reply latency target
    from services.reply_classifier_service import process_due_conversations
    scheduler.add_job(
        process_due_conversations,
        "interval",
        seconds=20,
        id="reply_classifier_processor",
        name="Reply classifier poller (20s)",
        replace_existing=True,
    )

    # OOO resume check — hourly
    scheduler.add_job(
        _ooo_resume_check,
        "interval",
        hours=1,
        id="ooo_resume_check",
        name="Resume paused_ooo enrollments",
        replace_existing=True,
    )

    # Meeting status sync — every 10 minutes (polls proposed meetings for acceptance)
    scheduler.add_job(
        _meeting_status_sync,
        "interval",
        minutes=10,
        id="meeting_status_sync",
        name="Sync meeting statuses from calendar",
        replace_existing=True,
    )

    # Calendar sync fallback poller — every 15 minutes (catches missed webhooks)
    scheduler.add_job(
        _calendar_sync_poller_fallback,
        "interval",
        minutes=15,
        id="calendar_sync_poller_fallback",
        name="Calendar sync fallback poller",
        replace_existing=True,
    )

    # Calendar webhook channel renewal — daily at 03:00 UTC (channels expire every 7 days)
    scheduler.add_job(
        _renew_calendar_webhook_channels,
        CronTrigger(hour=3, minute=0),
        id="calendar_webhook_renew",
        name="Renew Google Calendar push channels",
        replace_existing=True,
    )

    # 72h no-confirm check — hourly: reactivate meeting_proposed enrollments with no confirmation
    scheduler.add_job(
        _no_confirm_reactivation_check,
        "interval",
        hours=1,
        id="no_confirm_reactivation_check",
        name="Reactivate unconfirmed meeting proposals after 72h",
        replace_existing=True,
    )

    # Cascade escalation check — hourly safety net
    scheduler.add_job(
        _cascade_escalation_check,
        "interval",
        hours=1,
        id="cascade_escalation_check",
        name="Cascade escalation safety check",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        f"Scheduler started: scraping every {settings.scrape_day_of_week} "
        f"at {settings.scrape_hour:02d}:{settings.scrape_minute:02d}"
    )


def shutdown_scheduler():
    """Gracefully shut down the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")


def calculate_effective_fetch_counts(regions: list[dict], total_fetch_count: int) -> list[dict]:
    """
    Calculate effective fetch counts for each region, redistributing
    from exhausted regions proportionally to non-exhausted ones.

    Args:
        regions: List of region config dicts
        total_fetch_count: Total prospects to fetch across all regions

    Returns:
        List of dicts with {name, effective_fetch_count, ...} for non-exhausted regions only
    """
    active_regions = [r for r in regions if not r.get("is_exhausted", False)]

    if not active_regions:
        return []

    # Sum of original fetch_counts for active regions
    active_total = sum(r.get("fetch_count", 0) for r in active_regions)

    if active_total == 0:
        # Equal distribution if all fetch_counts are 0
        per_region = total_fetch_count // len(active_regions)
        for r in active_regions:
            r["effective_fetch_count"] = per_region
        return active_regions

    # Proportional redistribution of total_fetch_count
    for r in active_regions:
        proportion = r.get("fetch_count", 0) / active_total
        r["effective_fetch_count"] = max(1, round(total_fetch_count * proportion))

    return active_regions


async def scheduled_scrape_all_industries():
    """
    Main scheduled job: iterate all active industries with scraping enabled
    and scrape each one.
    """
    logger.info("Starting scheduled scrape for all active industries")

    industries = await industries_collection.find({
        "is_active": True,
        "scrape_enabled": True,
    }).to_list(None)

    if not industries:
        logger.info("No active industries with scraping enabled")
        return

    # Create semaphores for concurrency control
    industry_sem = asyncio.Semaphore(settings.industry_concurrency_limit)
    apify_sem = asyncio.Semaphore(settings.apify_actor_concurrency_limit)

    async def _scrape_one(industry):
        async with industry_sem:
            try:
                return await scrape_industry(industry, apify_semaphore=apify_sem)
            except Exception as e:
                logger.error(f"Failed to scrape industry {industry['name']}: {e}", exc_info=True)
                return {
                    "industry_id": str(industry["_id"]),
                    "industry_name": industry["name"],
                    "status": "failed",
                    "error": str(e),
                }

    results = await asyncio.gather(*[_scrape_one(ind) for ind in industries])

    total_new = sum(r.get("total_new_prospects", 0) for r in results)
    total_fetched = sum(r.get("total_fetched", 0) for r in results)

    logger.info(
        f"Scheduled scrape complete: {len(results)} industries processed, "
        f"{total_new} new prospects, {total_fetched} total fetched"
    )

    return {
        "industries_processed": len(results),
        "total_new_prospects": total_new,
        "total_fetched": total_fetched,
        "results": results,
    }


async def reset_industry_pagination(industry_id: str) -> dict:
    """
    Reset pagination state and exhaustion flags for all regions of an industry.
    Call this when industry params change or when the user wants to re-scrape from scratch.
    """
    industry = await industries_collection.find_one({"_id": ObjectId(industry_id)})
    if not industry:
        raise ValueError(f"Industry not found: {industry_id}")

    regions = industry.get("regions", [])
    for i, region in enumerate(regions):
        region["pagination_state"] = {
            "last_page_fetched": -1,
            "leads_finder_exhausted": False,
            "lead_scraper_exhausted": False,
            "lead_scraper_consecutive_zero_runs": 0,
        }
        region["is_exhausted"] = False
        region["exhausted_at"] = None

    await industries_collection.update_one(
        {"_id": ObjectId(industry_id)},
        {"$set": {"regions": regions}},
    )

    logger.info(f"Reset pagination for {industry['name']} ({len(regions)} regions)")
    return {
        "industry_id": industry_id,
        "industry_name": industry["name"],
        "regions_reset": len(regions),
    }


async def scrape_industry(industry: dict, apify_semaphore: asyncio.Semaphore = None) -> dict:
    """
    Scrape prospects for a single industry across all non-exhausted regions.

    For each region:
    1. Build full params (base params + region locations)
    2. Calculate effective fetch count (with redistribution)
    3. Call process_search_for_region
    4. Update region pagination state and exhaustion in MongoDB

    Args:
        industry: Industry document dict from MongoDB

    Returns:
        Dict with per-region results
    """
    industry_id = str(industry["_id"])
    industry_name = industry["name"]
    regions = industry.get("regions", [])
    total_fetch_count = industry.get("total_fetch_count", 100)
    base_params = industry.get("apify_base_params", {})

    logger.info(f"Scraping industry: {industry_name} (total_fetch_count={total_fetch_count})")

    # Calculate effective fetch counts with redistribution
    active_regions = calculate_effective_fetch_counts(regions, total_fetch_count)

    if not active_regions:
        logger.info(f"All regions exhausted for {industry_name}, skipping")
        return {
            "industry_id": industry_id,
            "industry_name": industry_name,
            "status": "all_regions_exhausted",
            "regions": [],
        }

    async def _scrape_region(region):
        region_name = region["name"]
        effective_count = region.get("effective_fetch_count", 0)
        locations = region.get("locations", [])
        pagination = region.get("pagination_state", {})
        start_page = pagination.get("last_page_fetched", -1) + 1

        # Build full params: base + region locations
        full_params = {**base_params}
        full_params["contact_location"] = locations

        try:
            result = await process_search_for_region(
                industry_id=industry_id,
                region_name=region_name,
                apify_params=full_params,
                fetch_count=effective_count,
                start_page=start_page,
                apify_semaphore=apify_semaphore,
            )

            # Update region pagination and exhaustion in MongoDB
            now = datetime.utcnow()
            region_update = {
                "regions.$[r].pagination_state.last_page_fetched": start_page,
                "regions.$[r].pagination_state.leads_finder_exhausted": result.get("leads_finder_exhausted", False),
                "regions.$[r].pagination_state.lead_scraper_exhausted": result.get("lead_scraper_exhausted", False),
                "regions.$[r].pagination_state.lead_scraper_consecutive_zero_runs": result.get("lead_scraper_consecutive_zero_runs", 0),
            }

            if result.get("is_exhausted"):
                region_update["regions.$[r].is_exhausted"] = True
                region_update["regions.$[r].exhausted_at"] = now
                logger.info(f"Region {region_name} exhausted for {industry_name}")

            await industries_collection.update_one(
                {"_id": ObjectId(industry_id)},
                {"$set": region_update},
                array_filters=[{"r.name": region_name}],
            )

            return result

        except Exception as e:
            logger.error(f"Failed to scrape {industry_name}/{region_name}: {e}", exc_info=True)
            return {
                "region_name": region_name,
                "status": "failed",
                "error": str(e),
            }

    # Run all regions in parallel
    region_results = await asyncio.gather(*[_scrape_region(r) for r in active_regions])

    total_new = sum(r.get("new_prospects", 0) for r in region_results)
    total_fetched = sum(r.get("total_fetched", 0) for r in region_results)

    # Update industry-level stats
    now = datetime.utcnow()
    await industries_collection.update_one(
        {"_id": ObjectId(industry_id)},
        {
            "$set": {"last_run_at": now},
            "$inc": {
                "total_runs": 1,
                "total_prospects_generated": total_new,
            },
        }
    )

    # Check if all regions are now exhausted after this scrape
    all_exhausted = all(r.get("is_exhausted", False) for r in region_results if r.get("status") == "completed")

    result = {
        "industry_id": industry_id,
        "industry_name": industry_name,
        "status": "completed",
        "total_new_prospects": total_new,
        "total_fetched": total_fetched,
        "regions": region_results,
    }

    if total_new == 0 and total_fetched > 0:
        result["message"] = (
            f"No new prospects found — all {total_fetched} results were already in the database. "
            "The Apify actor's result pool for these search criteria is fully consumed. "
            "Try broadening job titles, locations, or company filters to find new prospects, "
            "or use the reset-pagination endpoint to re-scrape from page 0."
        )
    if all_exhausted:
        result["all_regions_exhausted"] = True

    return result


async def _reset_daily_caps():
    from services.daily_cap_service import reset_all_campaigns_daily_caps
    import database as _database
    await reset_all_campaigns_daily_caps(_database.db)


async def _ooo_resume_check():
    """Resume enrollments that were paused for OOO and whose resume date has passed."""
    from database import campaign_enrollments_collection

    now = datetime.utcnow()
    result = await campaign_enrollments_collection.update_many(
        {"status": "paused_ooo", "ooo_resume_at": {"$lte": now}},
        {"$set": {"status": "active", "ooo_resume_at": None}},
    )
    if result.modified_count:
        logger.info(f"OOO resume: reactivated {result.modified_count} enrollments")


async def _meeting_status_sync():
    """Poll all active email accounts and sync proposed meetings older than 1h."""
    try:
        from database import email_accounts_collection, meetings_collection
        from services.meeting_service import sync_meeting_statuses

        # Find accounts that have proposed meetings older than 1h
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=1)
        pipeline = [
            {"$match": {"status": "proposed", "created_at": {"$lte": cutoff}}},
            {"$group": {"_id": "$account_id"}},
        ]
        account_ids = [doc["_id"] async for doc in meetings_collection.aggregate(pipeline)]
        for account_id in account_ids:
            try:
                await sync_meeting_statuses(account_id)
            except Exception as e:
                logger.warning(f"Meeting sync failed for account {account_id}: {e}")
    except Exception as e:
        logger.error(f"_meeting_status_sync job failed: {e}", exc_info=True)


async def _calendar_sync_poller_fallback():
    """Fallback: re-register calendar watches for accounts whose channel may have expired."""
    try:
        from database import email_accounts_collection
        from services.calendar_service import register_calendar_watch
        from config import get_settings
        _settings = get_settings()
        webhook_url = f"{_settings.api_base_url}/api/calendar/webhooks/google"

        # Find Gmail accounts with calendar scope
        cursor = email_accounts_collection.find(
            {"provider": "gmail", "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}}},
            {"account_id": 1},
        )
        async for acct in cursor:
            account_id = str(acct.get("account_id", ""))
            if not account_id:
                continue
            try:
                await register_calendar_watch(account_id, webhook_url)
            except Exception as e:
                logger.debug(f"Calendar watch refresh skipped for {account_id}: {e}")
    except Exception as e:
        logger.error(f"_calendar_sync_poller_fallback failed: {e}", exc_info=True)


async def _renew_calendar_webhook_channels():
    """Daily: renew Google Calendar push channels before they expire (7-day TTL)."""
    await _calendar_sync_poller_fallback()
    logger.info("Calendar webhook channel renewal complete")


async def _cascade_escalation_check():
    """Hourly safety net: escalate enrollments stuck in awaiting_human for >48h."""
    try:
        from database import campaign_enrollments_collection
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=48)
        result = await campaign_enrollments_collection.count_documents(
            {"status": "awaiting_human", "escalated_to_human": True, "last_activity_at": {"$lte": cutoff}}
        )
        if result:
            logger.warning(f"Cascade escalation check: {result} enrollments awaiting human >48h")
    except Exception as e:
        logger.error(f"_cascade_escalation_check failed: {e}", exc_info=True)


async def _no_confirm_reactivation_check():
    """Hourly: reactivate meeting_proposed enrollments where prospect did not confirm within 72h."""
    try:
        from database import campaign_enrollments_collection
        from services.enrollment_state_machine import reactivate_after_no_confirm
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(hours=72)
        cursor = campaign_enrollments_collection.find(
            {"status": "meeting_proposed", "last_activity_at": {"$lte": cutoff}},
            {"_id": 1},
        )
        reactivated = 0
        async for doc in cursor:
            enrollment_id = str(doc["_id"])
            ok = await reactivate_after_no_confirm(enrollment_id)
            if ok:
                reactivated += 1
        if reactivated:
            logger.info(f"No-confirm reactivation: reactivated {reactivated} meeting_proposed enrollments")
    except Exception as e:
        logger.error(f"_no_confirm_reactivation_check failed: {e}", exc_info=True)

