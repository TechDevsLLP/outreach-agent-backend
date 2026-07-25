"""
Background job scheduler using APScheduler.
Runs the campaign engine, reply pollers, daily-cap resets, calendar syncs, etc.

The legacy weekly Apollo-actor industry scraping job was removed — prospect
discovery is now DB-first (shared pool) + employee-scraper based.
"""

import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import get_settings
from services.connection_tracker_service import check_pending_connections, check_pending_linkedin_replies
from services.campaign_engine import execute_pending_steps, compute_campaign_daily_stats, reconcile_ambiguous_send
from services.email_reply_poller import check_email_replies

logger = logging.getLogger(__name__)

settings = get_settings()

scheduler = AsyncIOScheduler()


def _with_heartbeat(job_id: str, func):
    """Wrap a job coroutine so each run upserts a heartbeat doc into the
    `scheduler_heartbeats` collection: {job_id, last_run_at, last_status, last_error}.

    Heartbeat write failures never break the job itself."""
    async def _wrapped(*args, **kwargs):
        started_at = datetime.utcnow()
        last_status, last_error = "ok", None
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_status = "error"
            last_error = str(e)[:500]
            raise
        finally:
            try:
                import database as _database
                await _database.scheduler_heartbeats_collection.update_one(
                    {"job_id": job_id},
                    {"$set": {
                        "job_id": job_id,
                        "last_run_at": started_at,
                        "last_status": last_status,
                        "last_error": last_error,
                    }},
                    upsert=True,
                )
            except Exception as hb_err:
                logger.debug(f"Heartbeat write failed for job {job_id}: {hb_err}")

    _wrapped.__name__ = f"hb_{job_id}"
    return _wrapped


def start_scheduler():
    """Start the APScheduler with the configured cron triggers."""
    scheduler.add_job(
        _with_heartbeat("check_pending_connections", check_pending_connections),
        "interval",
        minutes=5,
        id="check_pending_connections",
        name="Check pending LinkedIn connections",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _with_heartbeat("check_linkedin_replies", check_pending_linkedin_replies),
        "interval",
        minutes=5,
        id="check_linkedin_replies",
        name="Check pending LinkedIn replies for sequence campaigns",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _with_heartbeat("campaign_engine", execute_pending_steps),
        "interval",
        minutes=5,
        id="campaign_engine",
        name="Campaign execution engine",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Durable enrichment dispatcher. API requests only enqueue Mongo-backed
    # jobs; this worker owns provider execution and restart recovery.
    from services.enrichment_job_service import process_enrichment_jobs
    scheduler.add_job(
        _with_heartbeat("enrichment_job_dispatcher", process_enrichment_jobs),
        "interval",
        seconds=15,
        id="enrichment_job_dispatcher",
        name="Durable prospect enrichment dispatcher",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _with_heartbeat("campaign_daily_stats", compute_campaign_daily_stats),
        CronTrigger(hour=0, minute=5),
        id="campaign_daily_stats",
        name="Campaign daily stats aggregation",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,
    )

    scheduler.add_job(
        _with_heartbeat("check_email_replies", check_email_replies),
        "interval",
        minutes=5,
        id="check_email_replies",
        name="Check email replies for campaign emails (Gmail/Zoho/SMTP+IMAP)",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Daily cap reset at 00:01 UTC
    scheduler.add_job(
        _with_heartbeat("reset_daily_caps", _reset_daily_caps),
        CronTrigger(hour=0, minute=1),
        id="reset_daily_caps",
        name="Reset campaign daily caps",
        replace_existing=True,
        coalesce=True,
        misfire_grace_time=3600,
    )

    # Reply classifier processor — every 20s to hit the 45-60s LinkedIn reply latency target
    from services.reply_classifier_service import process_due_conversations
    scheduler.add_job(
        _with_heartbeat("reply_classifier_processor", process_due_conversations),
        "interval",
        seconds=20,
        id="reply_classifier_processor",
        name="Reply classifier poller (20s)",
        replace_existing=True,
    )

    # OOO resume check — hourly
    scheduler.add_job(
        _with_heartbeat("ooo_resume_check", _ooo_resume_check),
        "interval",
        hours=1,
        id="ooo_resume_check",
        name="Resume paused_ooo enrollments",
        replace_existing=True,
    )

    # Meeting status sync — every 10 minutes (polls proposed meetings for acceptance)
    scheduler.add_job(
        _with_heartbeat("meeting_status_sync", _meeting_status_sync),
        "interval",
        minutes=10,
        id="meeting_status_sync",
        name="Sync meeting statuses from calendar",
        replace_existing=True,
    )

    # Calendar sync fallback poller — every 15 minutes (catches missed webhooks)
    scheduler.add_job(
        _with_heartbeat("calendar_sync_poller_fallback", _calendar_sync_poller_fallback),
        "interval",
        minutes=15,
        id="calendar_sync_poller_fallback",
        name="Calendar sync fallback poller",
        replace_existing=True,
    )

    # Calendar sync fallback poller (every 15 min, above) already renews/dedups
    # channels per account via register_calendar_watch — a separate daily
    # renewal job would just call the exact same function again.

    # Ambiguous send reconciliation — every 30 min (resolve provider-boundary timeouts)
    scheduler.add_job(
        _with_heartbeat("reconcile_ambiguous_sends", _reconcile_ambiguous_sends),
        "interval",
        minutes=30,
        id="reconcile_ambiguous_sends",
        name="Reconcile stuck send_ambiguous attempts",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # 72h no-confirm check — hourly: reactivate meeting_proposed enrollments with no confirmation
    scheduler.add_job(
        _with_heartbeat("no_confirm_reactivation_check", _no_confirm_reactivation_check),
        "interval",
        hours=1,
        id="no_confirm_reactivation_check",
        name="Reactivate unconfirmed meeting proposals after 72h",
        replace_existing=True,
    )

    # Cascade escalation check — hourly safety net
    scheduler.add_job(
        _with_heartbeat("cascade_escalation_check", _cascade_escalation_check),
        "interval",
        hours=1,
        id="cascade_escalation_check",
        name="Cascade escalation safety check",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started")


def shutdown_scheduler():
    """Gracefully shut down the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")


async def _reset_daily_caps():
    from services.daily_cap_service import reset_all_campaigns_daily_caps
    import database as _database
    await reset_all_campaigns_daily_caps(_database.db)


async def _ooo_resume_check():
    """Resume enrollments paused for OOO or a soft objection once their resume time passes."""
    from database import campaign_enrollments_collection

    now = datetime.utcnow()
    result = await campaign_enrollments_collection.update_many(
        {"status": "paused_ooo", "ooo_resume_at": {"$lte": now}},
        {"$set": {"status": "active", "ooo_resume_at": None}},
    )
    if result.modified_count:
        logger.info(f"OOO resume: reactivated {result.modified_count} enrollments")

    # Soft-objection handling parks enrollments as status="paused" with
    # next_action_at set to the resume time; nothing else ever reactivates them.
    paused_result = await campaign_enrollments_collection.update_many(
        {"status": "paused", "next_action_at": {"$lte": now}},
        {"$set": {"status": "active"}},
    )
    if paused_result.modified_count:
        logger.info(f"Paused resume: reactivated {paused_result.modified_count} enrollments")


async def _meeting_status_sync():
    """Boundedly poll due meetings partitioned by tenant and Google mailbox."""
    try:
        from database import meetings_collection
        from services.meeting_service import sync_meeting_statuses

        now = datetime.utcnow()
        pipeline = [
            {
                "$match": {
                    "status": {"$in": ["booked", "rescheduled"]},
                    "calendar_provider": "google",
                    "calendar_provider_account_id": {"$type": "string"},
                    "calendar_id": {"$type": "string"},
                    "calendar_event_id": {"$type": "string"},
                    "$or": [
                        {"next_calendar_sync_at": {"$exists": False}},
                        {"next_calendar_sync_at": None},
                        {"next_calendar_sync_at": {"$lte": now}},
                    ],
                }
            },
            {
                "$group": {
                    "_id": {
                        "account_id": "$account_id",
                        "provider_account_id": "$calendar_provider_account_id",
                    }
                }
            },
            {"$limit": 10},
        ]
        bindings = [doc["_id"] async for doc in meetings_collection.aggregate(pipeline)]
        semaphore = asyncio.Semaphore(5)

        async def _sync_binding(binding: dict) -> None:
            account_id = str(binding.get("account_id") or "")
            provider_account_id = str(binding.get("provider_account_id") or "")
            if not account_id or not provider_account_id:
                return
            async with semaphore:
                try:
                    await sync_meeting_statuses(
                        account_id,
                        provider_account_id=provider_account_id,
                        max_meetings=5,
                    )
                except Exception as e:
                    logger.warning(
                        "Meeting sync failed for account %s provider account %s: %s",
                        account_id,
                        provider_account_id,
                        e,
                    )

        await asyncio.gather(*[_sync_binding(binding) for binding in bindings])
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
            {"provider": "google", "oauth_scopes": {"$elemMatch": {"$regex": "calendar"}}},
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


async def _reconcile_ambiguous_sends():
    """Every 30 min: resolve send_attempts stuck in ``ambiguous`` state.

    Ambiguous means the failure happened at the provider boundary — the
    message was very likely delivered before the timeout. Without provider
    evidence we resolve as ``sent``: worst case the prospect misses one touch;
    resolving ``not_sent`` would re-dispatch and risk sending the same
    message twice. Attempts get 15 minutes before being swept so any
    in-flight lease/retry can resolve on its own first.
    """
    try:
        from datetime import timedelta
        from database import send_attempts_collection

        cutoff = datetime.utcnow() - timedelta(minutes=15)
        cursor = send_attempts_collection.find(
            {"state": "ambiguous", "ambiguous_at": {"$lte": cutoff}},
            {"send_key": 1},
        )
        reconciled = 0
        async for doc in cursor:
            send_key = doc.get("send_key")
            if not send_key:
                continue
            try:
                result = await reconcile_ambiguous_send(send_key, "sent")
                if result:
                    reconciled += 1
            except Exception as e:
                logger.warning(f"Ambiguous send reconcile failed for {send_key}: {e}")
        if reconciled:
            logger.info(f"Ambiguous send reconcile: resolved {reconciled} stuck attempts")
    except Exception as e:
        logger.error(f"_reconcile_ambiguous_sends failed: {e}", exc_info=True)


async def _cascade_escalation_check():
    """Hourly safety net: notify once per enrollment stuck in awaiting_human for >48h."""
    try:
        from database import campaign_enrollments_collection
        from services.notification_service import create_notification
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(hours=48)
        cursor = campaign_enrollments_collection.find(
            {
                "status": "awaiting_human",
                "escalated_to_human": True,
                "last_activity_at": {"$lte": cutoff},
                "escalation_notified_at": {"$exists": False},
            },
            {"account_id": 1, "prospect_id": 1, "campaign_id": 1, "escalated_reason": 1},
        )
        notified = 0
        async for enrollment in cursor:
            enrollment_id = enrollment["_id"]
            account_id = str(enrollment.get("account_id") or "")
            if not account_id:
                continue
            # Idempotent claim: only the caller that flips this flag sends the notification.
            claimed = await campaign_enrollments_collection.update_one(
                {"_id": enrollment_id, "escalation_notified_at": {"$exists": False}},
                {"$set": {"escalation_notified_at": datetime.utcnow()}},
            )
            if claimed.modified_count != 1:
                continue
            try:
                await create_notification(
                    account_id=account_id,
                    type="escalation_stale",
                    title="Enrollment stuck awaiting human >48h",
                    body=enrollment.get("escalated_reason")
                    or "This enrollment has been awaiting human attention for over 48 hours.",
                    prospect_id=str(enrollment["prospect_id"]) if enrollment.get("prospect_id") else None,
                    campaign_id=str(enrollment["campaign_id"]) if enrollment.get("campaign_id") else None,
                    channel="system",
                )
                notified += 1
            except Exception as e:
                logger.warning(f"Cascade escalation notification failed for enrollment {enrollment_id}: {e}")
        if notified:
            logger.warning(f"Cascade escalation check: notified {notified} enrollments awaiting human >48h")
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
