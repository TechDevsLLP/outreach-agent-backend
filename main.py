import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

import database
from config import get_settings
from database import create_indexes
from rate_limit import limiter
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from routes.auth import router as auth_router, users_router, onboarding_router
from routes.prospects import router as prospects_router
from routes.industries import router as industries_router
from routes.search_runs import router as search_runs_router
from routes.enrichment import router as enrichment_router
from routes.analytics import router as analytics_router
from routes.linkedin_outreach import router as linkedin_outreach_router
from routes.conversations import router as conversations_router
from routes.webhooks import router as webhooks_router
from routes.sendgrid_activity import router as sendgrid_activity_router
from routes.notifications import router as notifications_router
from routes.activity_feed import router as activity_feed_router
from routes.system_prompts import router as system_prompts_router
from routes.accounts import router as accounts_router
from routes.campaigns import router as campaigns_router
from routes.campaign_enrollments import router as campaign_enrollments_router
from routes.campaign_schedules import router as campaign_schedules_router
from routes.company_profiles import router as company_profiles_router
from routes.companies import router as companies_router
from routes.employees import router as employees_router
from routes.outreach_ai import router as outreach_ai_router
from routes.email_tracking import router as email_tracking_router
from routes.email_accounts import router as email_accounts_router
from routes.linkedin_accounts import router as linkedin_accounts_router
from routes.admin import router as admin_router
from routes.public_booking import router as public_booking_router
from routes.meetings import router as meetings_router
from routes.calendar import router as calendar_router
from routes.onboarding_wizard import router as onboarding_wizard_router
from routes.sender_voice import router as sender_voice_router
from routes.replies import router as replies_router

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def resume_stalled_discoveries():
    """On boot, resume any smart campaign discoveries that were interrupted mid-run."""
    from datetime import timedelta
    import asyncio
    stall_threshold = datetime.utcnow() - timedelta(minutes=30)
    stalled = await database.campaigns_collection.find(
        {
            "is_smart_campaign": True,
            "discovery_status": {"$in": [
                # Curated discovery (live path)
                "sourcing_companies", "scraping_employees", "enriching",
                # Legacy states (kept for backward compat with old campaigns)
                "searching_db", "scraping", "scoring",
            ]},
            "discovery_started_at": {"$lt": stall_threshold},
        },
        {"_id": 1, "account_id": 1},
    ).to_list(None)
    if stalled:
        logger.info(f"Resuming {len(stalled)} stalled smart campaign discoveries on boot")
        from services.curated_discovery_service import run_fast_discovery
        resumed = 0
        for doc in stalled:
            # Atomic claim: only resume if discovery_status hasn't changed since the query
            # (a still-running slow task may have updated it between the find and now)
            claim = await database.campaigns_collection.find_one_and_update(
                {
                    "_id": doc["_id"],
                    "discovery_status": {"$in": [
                        "sourcing_companies", "scraping_employees", "enriching",
                        "searching_db", "scraping", "scoring",
                    ]},
                },
                {"$set": {"discovery_status": "sourcing_companies", "discovery_resumed_at": datetime.utcnow()}},
            )
            if claim:
                asyncio.create_task(
                    run_fast_discovery(str(doc["_id"]), str(doc["account_id"]))
                )
                resumed += 1
        logger.info(f"Claimed and resumed {resumed}/{len(stalled)} stalled discoveries")

    stalled_msg = await database.campaigns_collection.find(
        {
            "is_smart_campaign": True,
            "message_gen_status": "running",
            "discovery_status": "completed",
        },
        {"_id": 1, "account_id": 1},
    ).to_list(None)
    if stalled_msg:
        logger.info(f"Resuming {len(stalled_msg)} stalled message generation tasks on boot")
        from services.campaign_message_generator_service import generate_messages_for_campaign
        resumed_msg = 0
        for doc in stalled_msg:
            claim_msg = await database.campaigns_collection.find_one_and_update(
                {"_id": doc["_id"], "message_gen_status": "running"},
                {"$set": {"message_gen_status": "resuming"}},
            )
            if claim_msg:
                asyncio.create_task(
                    generate_messages_for_campaign(str(doc["_id"]), str(doc["account_id"]))
                )
                resumed_msg += 1
        logger.info(f"Claimed and resumed {resumed_msg}/{len(stalled_msg)} stalled message gen tasks")


async def _sweep_interrupted_enrichment_runs():
    from datetime import datetime, timedelta
    from database import enrichment_runs_collection, prospects_collection
    cutoff = datetime.utcnow() - timedelta(minutes=30)
    result = await enrichment_runs_collection.update_many(
        {"status": "running", "started_at": {"$lt": cutoff}},
        {"$set": {"status": "failed", "error": "interrupted by server restart", "completed_at": datetime.utcnow()}},
    )
    if result.modified_count:
        logger.warning(f"Swept {result.modified_count} interrupted enrichment run(s) to failed")
        # Unstick prospects left in_progress with no active run
        active_run_ids = [
            r["_id"] async for r in enrichment_runs_collection.find({"status": "running"}, {"_id": 1})
        ]
        stuck_result = await prospects_collection.update_many(
            {"enrichment_status": "in_progress", "enrichment_run_id": {"$nin": [str(r) for r in active_run_ids]}},
            {"$set": {"enrichment_status": "failed", "enrichment_error": "interrupted by server restart"}},
        )
        if stuck_result.modified_count:
            logger.warning(f"Reset {stuck_result.modified_count} stuck in_progress prospect(s) to failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Prospect Generation Engine...")
    await create_indexes()
    logger.info("MongoDB indexes ready")

    from services.scheduler_service import start_scheduler, shutdown_scheduler
    if settings.app_role != "web":
        start_scheduler()
        logger.info(f"Scheduler started (app_role={settings.app_role})")
    else:
        logger.info("Scheduler not started (app_role=web)")

    # Resume any schedules interrupted by a previous shutdown
    from services.outreach_executor_service import resume_interrupted_schedules
    await resume_interrupted_schedules()

    # Mark enrichment runs that were interrupted by a previous server restart
    if settings.enrichment_startup_sweep_enabled:
        await _sweep_interrupted_enrichment_runs()

    # Resume stalled smart campaign discoveries and message generation
    await resume_stalled_discoveries()

    yield

    # Shutdown
    from services.notification_service import shutdown_sse
    shutdown_sse()
    if settings.app_role != "web":
        shutdown_scheduler()
    logger.info("Shutting down Prospect Generation Engine...")


app = FastAPI(
    title="OutFlo API",
    description="B2B outreach automation API",
    version="2.0.0",  # Phase 2 - Breaking changes
    lifespan=lifespan,
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Request/Response logging middleware
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip logging for SSE stream (long-lived connection)
        if request.url.path == "/api/notifications/stream":
            return await call_next(request)
        start = time.time()
        logger.info(f"→ {request.method} {request.url.path}{'?' + str(request.query_params) if request.query_params else ''}")
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000
        log_fn = logger.warning if response.status_code >= 400 else logger.info
        log_fn(f"← {request.method} {request.url.path} → {response.status_code} ({duration_ms:.0f}ms)")
        return response

app.add_middleware(RequestLoggingMiddleware)

# CORS
_cors_origin_str = getattr(settings, "cors_origins", None) or settings.frontend_url
_cors_origins = [o.strip() for o in _cors_origin_str.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(onboarding_router)
app.include_router(onboarding_wizard_router)
app.include_router(sender_voice_router)
app.include_router(replies_router)
app.include_router(prospects_router)
app.include_router(industries_router)
app.include_router(search_runs_router)
app.include_router(enrichment_router)
app.include_router(analytics_router)
app.include_router(linkedin_outreach_router)
app.include_router(conversations_router)
app.include_router(webhooks_router)
app.include_router(sendgrid_activity_router)
app.include_router(notifications_router)
app.include_router(activity_feed_router)
app.include_router(system_prompts_router)
app.include_router(accounts_router)
app.include_router(campaigns_router)
app.include_router(campaign_enrollments_router)
app.include_router(campaign_schedules_router)
app.include_router(company_profiles_router)
app.include_router(companies_router)
app.include_router(employees_router)
app.include_router(outreach_ai_router)
app.include_router(email_tracking_router)
app.include_router(email_accounts_router)
app.include_router(linkedin_accounts_router)
app.include_router(admin_router)
app.include_router(public_booking_router)
app.include_router(meetings_router)
app.include_router(calendar_router)


@app.get("/")
async def root():
    return {
        "name": "OutFlo API",
        "version": "2.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    from database import client
    try:
        await client.admin.command("ping")
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {"status": "ok", "database": db_status}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8008, reload=settings.debug)
