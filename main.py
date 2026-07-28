import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

import database
from config import get_settings, validate_production_settings
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
from routes.notifications import router as notifications_router
from routes.activity_feed import router as activity_feed_router
from routes.system_prompts import router as system_prompts_router
from routes.accounts import router as accounts_router
from routes.campaigns import router as campaigns_router
from routes.campaign_uploads import router as campaign_uploads_router
from routes.discovery_stream import router as discovery_stream_router
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
from routes.admin_accounts import router as admin_accounts_router
from routes.admin_costs import router as admin_costs_router
from routes.admin_usage import router as admin_usage_router
from routes.admin_pool import router as admin_pool_router
from routes.admin_system import router as admin_system_router
from routes.public_booking import router as public_booking_router
from routes.meetings import router as meetings_router
from routes.calendar import router as calendar_router
from routes.onboarding_wizard import router as onboarding_wizard_router
from routes.sender_voice import router as sender_voice_router
from routes.replies import router as replies_router
from routes.global_search import router as global_search_router
from routes.overview import router as overview_router

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


async def _sweep_stranded_onboarding_discoveries():
    """Re-queue onboarding campaigns whose discovery was lost to a restart.

    Onboarding discovery used to be a raw fire-and-forget asyncio task; a
    process restart (or dev --reload) could kill it before or during the run,
    leaving the campaign stranded at pending/sourcing with zero enrollments and
    nothing to resume it. Discovery now goes through the durable job queue, but
    this sweep heals campaigns stranded by any remaining gap: it re-enqueues
    discovery for onboarding campaigns stuck in a non-terminal state with no
    live queue job.
    """
    from datetime import datetime, timedelta, timezone
    from database import campaigns_collection, jobs_collection
    from models.job import JobState
    from services.enrichment_job_service import CAMPAIGN_DISCOVERY_JOB_TYPE
    from services.onboarding_scrape_service import enqueue_onboarding_discovery

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
    stranded = campaigns_collection.find(
        {
            "is_onboarding_campaign": True,
            "discovery_status": {"$in": [
                "pending", "queued", "searching_db", "scraping", "enriching",
                "scoring", "sourcing_companies", "scraping_employees",
            ]},
            "updated_at": {"$lt": cutoff},
        },
        {"account_id": 1, "onboarding_session_id": 1, "discovery_status": 1},
    ).limit(20)

    async for campaign in stranded:
        campaign_id = str(campaign["_id"])
        # A live/queued durable job means the lease system already owns recovery.
        live_job = await jobs_collection.find_one({
            "job_type": CAMPAIGN_DISCOVERY_JOB_TYPE,
            "payload.campaign_id": campaign_id,
            "state": {"$in": [
                JobState.QUEUED.value, JobState.RETRY_SCHEDULED.value, JobState.RUNNING.value,
            ]},
        }, {"_id": 1})
        if live_job:
            continue
        try:
            # Reset status first so the idempotency guard doesn't see the stale
            # "in-flight" status and skip the re-enqueue.
            await campaigns_collection.update_one(
                {"_id": campaign["_id"]},
                {"$set": {"discovery_status": "pending", "updated_at": datetime.now(timezone.utc)}},
            )
            queued = await enqueue_onboarding_discovery(
                campaign_id=campaign_id,
                account_id=str(campaign.get("account_id")),
                session_id=str(campaign.get("onboarding_session_id") or ""),
            )
            logger.warning(
                f"Startup sweep: onboarding campaign {campaign_id} was stranded at "
                f"discovery_status={campaign.get('discovery_status')!r} — re-enqueued (queued={queued})"
            )
        except Exception as exc:
            logger.error(f"Startup sweep: failed to re-enqueue onboarding campaign {campaign_id}: {exc}")


async def _sweep_interrupted_enrichment_runs():
    from datetime import datetime, timedelta
    from database import enrichment_runs_collection
    cutoff = datetime.utcnow() - timedelta(minutes=30)
    result = await enrichment_runs_collection.update_many(
        {"status": "running", "started_at": {"$lt": cutoff}},
        {"$set": {"status": "failed", "error": "interrupted by server restart", "completed_at": datetime.utcnow()}},
    )
    if result.modified_count:
        logger.warning(f"Swept {result.modified_count} interrupted enrichment run(s) to failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    validate_production_settings(settings)
    logger.info("Starting Prospect Generation Engine...")
    await create_indexes()
    logger.info("MongoDB indexes ready")

    from services.scheduler_service import start_scheduler, shutdown_scheduler
    if settings.app_role != "web":
        start_scheduler()
        logger.info(f"Scheduler started (app_role={settings.app_role})")
    else:
        logger.info("Scheduler not started (app_role=web)")

    # Mark enrichment runs that were interrupted by a previous server restart
    if settings.enrichment_startup_sweep_enabled:
        await _sweep_interrupted_enrichment_runs()

    # Discovery/enrichment/message work is recovered by Mongo job lease expiry;
    # web replicas must never spawn provider tasks during startup.

    # Heal onboarding campaigns stranded before their discovery job existed
    # (scheduler-capable roles only — the job queue does the actual work).
    if settings.app_role != "web":
        await _sweep_stranded_onboarding_discoveries()

    yield

    # Shutdown
    from services.notification_service import shutdown_sse
    shutdown_sse()
    from services.growthtoolkit_service import aclose as growthtoolkit_aclose
    await growthtoolkit_aclose()
    if settings.app_role != "web":
        shutdown_scheduler()
    logger.info("Shutting down Prospect Generation Engine...")


app = FastAPI(
    title="OutFlo API",
    description="B2B outreach automation API",
    version="2.0.0",  # Phase 2 - Breaking changes
    lifespan=lifespan,
    docs_url=None if settings.app_env.strip().lower() == "production" else "/docs",
    redoc_url=None if settings.app_env.strip().lower() == "production" else "/redoc",
    openapi_url=None if settings.app_env.strip().lower() == "production" else "/openapi.json",
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
        # Query strings frequently carry OAuth codes, reset tokens, filters, and
        # customer data. Keep request logs useful without persisting secrets.
        logger.info(f"→ {request.method} {request.url.path}")
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000
        log_fn = logger.warning if response.status_code >= 400 else logger.info
        log_fn(f"← {request.method} {request.url.path} → {response.status_code} ({duration_ms:.0f}ms)")
        return response

app.add_middleware(RequestLoggingMiddleware)


class SecurityBoundaryMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > settings.max_request_bytes:
                    return JSONResponse(
                        status_code=413,
                        content={"detail": "Request body too large"},
                    )
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "Invalid Content-Length header"},
                )

        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        if settings.app_env.strip().lower() == "production":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )
            response.headers["Content-Security-Policy"] = (
                "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
            )
        return response


app.add_middleware(SecurityBoundaryMiddleware)

_trusted_hosts = [
    host.strip() for host in settings.trusted_hosts.split(",") if host.strip()
]
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_trusted_hosts)

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
app.include_router(notifications_router)
app.include_router(activity_feed_router)
app.include_router(system_prompts_router)
app.include_router(accounts_router)
app.include_router(campaign_uploads_router)
app.include_router(campaigns_router)
app.include_router(discovery_stream_router)
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
app.include_router(admin_accounts_router)
app.include_router(admin_costs_router)
app.include_router(admin_usage_router)
app.include_router(admin_pool_router)
app.include_router(admin_system_router)
app.include_router(public_booking_router)
app.include_router(meetings_router)
app.include_router(calendar_router)
app.include_router(global_search_router)
app.include_router(overview_router)


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
    except Exception:
        logger.warning("Database health check failed", exc_info=True)
        return JSONResponse(
            status_code=503,
            content={"status": "error", "database": "unavailable"},
        )

    return {"status": "ok", "database": db_status}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8008, reload=settings.debug)
