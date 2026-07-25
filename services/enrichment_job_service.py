"""Durable dispatch for tenant-owned enrichment work.

The API only creates a run and enqueues a deterministic job.  The standalone
scheduler claims jobs with Mongo leases, renews the lease while providers are
running, and lets the queue retry work after a process restart.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from contextlib import suppress
from datetime import datetime, timezone

import database
from bson import ObjectId
from models.job import JobState
from services.job_queue_service import JobQueueService


logger = logging.getLogger(__name__)
ENRICHMENT_JOB_TYPE = "prospect_enrichment_v1"
CAMPAIGN_DAY_JOB_TYPE = "campaign_day_enrich_generate_v1"
CAMPAIGN_DISCOVERY_JOB_TYPE = "campaign_discovery_v1"
CAMPAIGN_MESSAGE_GEN_JOB_TYPE = "campaign_message_generation_v1"
CAMPAIGN_REMAINING_DAYS_JOB_TYPE = "campaign_remaining_days_enrich_v1"
LEASE_SECONDS = 180.0

# Every durable job type the standalone scheduler discovers and claims.
DURABLE_JOB_TYPES = [
    ENRICHMENT_JOB_TYPE,
    CAMPAIGN_DAY_JOB_TYPE,
    CAMPAIGN_DISCOVERY_JOB_TYPE,
    CAMPAIGN_MESSAGE_GEN_JOB_TYPE,
    CAMPAIGN_REMAINING_DAYS_JOB_TYPE,
]

# Message generation for one day can be reached by two idempotent service
# functions; the mode selects which one the worker dispatches to.
MESSAGE_GEN_MODE_ENSURE_DAY = "ensure_day"
MESSAGE_GEN_MODE_GENERATE_DAY = "generate_day"


def enrichment_job_key(run_id: str) -> str:
    return f"enrichment-run:{run_id}"


async def enqueue_enrichment_run(
    *,
    account_id: str,
    run_id: str,
    prospect_ids: list[str],
    options: dict,
    triggered_by: str | None = None,
    campaign_id: str | None = None,
):
    queue = JobQueueService(database.jobs_collection)
    return await queue.enqueue(
        account_id=str(account_id),
        job_type=ENRICHMENT_JOB_TYPE,
        job_key=enrichment_job_key(run_id),
        payload={
            "run_id": str(run_id),
            "prospect_ids": [str(value) for value in prospect_ids],
            "options": dict(options),
            "triggered_by": str(triggered_by) if triggered_by else None,
            "campaign_id": str(campaign_id) if campaign_id else None,
        },
        max_attempts=4,
    )


async def enqueue_campaign_day_run(
    *, account_id: str, campaign_id: str, day: int, generation: int,
    request: dict,
):
    queue = JobQueueService(database.jobs_collection)
    return await queue.enqueue(
        account_id=str(account_id),
        job_type=CAMPAIGN_DAY_JOB_TYPE,
        job_key=f"campaign-day:{campaign_id}:{day}:generation:{generation}",
        payload={
            "campaign_id": str(campaign_id), "day": int(day),
            "generation": int(generation), "request": dict(request),
        },
        priority=10,
        max_attempts=4,
    )


async def enqueue_campaign_discovery(
    *, account_id: str, campaign_id: str, generation: int
):
    queue = JobQueueService(database.jobs_collection)
    return await queue.enqueue(
        account_id=str(account_id),
        job_type=CAMPAIGN_DISCOVERY_JOB_TYPE,
        job_key=f"campaign-discovery:{campaign_id}:generation:{int(generation)}",
        payload={
            "campaign_id": str(campaign_id),
            "generation": int(generation),
        },
        priority=20,
        max_attempts=4,
    )


async def enqueue_campaign_message_generation(
    *, account_id: str, campaign_id: str, day: int, mode: str,
):
    """Persist durable message-generation work for one campaign day.

    ``(account_id, campaign_id, day, mode)`` is the idempotency boundary: a
    duplicate approve/generate click for the same day coalesces onto the
    existing job instead of racing a second generation pass.
    """
    if mode not in (MESSAGE_GEN_MODE_ENSURE_DAY, MESSAGE_GEN_MODE_GENERATE_DAY):
        raise ValueError(f"unsupported message-gen mode: {mode}")
    queue = JobQueueService(database.jobs_collection)
    return await queue.enqueue(
        account_id=str(account_id),
        job_type=CAMPAIGN_MESSAGE_GEN_JOB_TYPE,
        job_key=f"campaign-message-gen:{campaign_id}:day:{int(day)}:{mode}",
        payload={
            "campaign_id": str(campaign_id),
            "day": int(day),
            "mode": str(mode),
        },
        priority=10,
        max_attempts=4,
    )


async def enqueue_campaign_remaining_days(
    *,
    account_id: str,
    campaign_id: str,
    remaining_oids: list,
    co_research_by_url: dict | None = None,
    skip_message_gen: bool = False,
):
    """Persist durable Days 2-5 enrichment for one campaign.

    Keyed by ``(account_id, campaign_id)`` so a discovery re-run coalesces onto
    the single in-flight remaining-days job rather than double-enriching.
    """
    queue = JobQueueService(database.jobs_collection)
    return await queue.enqueue(
        account_id=str(account_id),
        job_type=CAMPAIGN_REMAINING_DAYS_JOB_TYPE,
        job_key=f"campaign-remaining-days:{campaign_id}",
        payload={
            "campaign_id": str(campaign_id),
            "remaining_oids": [str(value) for value in remaining_oids],
            "co_research_by_url": dict(co_research_by_url or {}),
            "skip_message_gen": bool(skip_message_gen),
        },
        priority=5,
        max_attempts=4,
    )


async def _renew_lease(queue: JobQueueService, job, worker_id: str) -> None:
    """Renew a lease until the work finishes; loss cancels safe completion."""
    while True:
        await asyncio.sleep(LEASE_SECONDS / 3)
        renewed = await queue.heartbeat(
            account_id=job.account_id,
            job_id=job.id,
            worker_id=worker_id,
            lease_seconds=LEASE_SECONDS,
        )
        if renewed is None:
            raise RuntimeError("enrichment job lease lost")


async def _execute_claimed_job(queue: JobQueueService, job, worker_id: str) -> None:
    payload = job.payload
    work_id = str(payload.get("run_id") or payload.get("campaign_id") or job.id)
    heartbeat_task = asyncio.create_task(_renew_lease(queue, job, worker_id))
    pipeline_task = None
    try:
        await queue.checkpoint(
            account_id=job.account_id,
            job_id=job.id,
            worker_id=worker_id,
            checkpoint={"phase": "work_started", "work_id": work_id, "job_type": job.job_type},
        )
        if job.job_type == ENRICHMENT_JOB_TYPE:
            from services.enrichment_pipeline import run_enrichment_pipeline
            pipeline_task = asyncio.create_task(run_enrichment_pipeline(
                str(payload["run_id"]),
                [str(value) for value in payload.get("prospect_ids") or []],
                dict(payload.get("options") or {}),
                triggered_by=payload.get("triggered_by"),
                account_id=job.account_id,
                campaign_id=payload.get("campaign_id"),
            ))
        elif job.job_type == CAMPAIGN_DAY_JOB_TYPE:
            from services.campaign_day_enrichment_service import run_enrich_and_generate_for_day
            pipeline_task = asyncio.create_task(run_enrich_and_generate_for_day(
                campaign_id=str(payload["campaign_id"]),
                account_id=job.account_id,
                day=int(payload["day"]),
                generation=int(payload["generation"]),
                request=dict(payload.get("request") or {}),
                orchestration_job_id=str(job.id),
            ))
        elif job.job_type == CAMPAIGN_DISCOVERY_JOB_TYPE:
            campaign_id = str(payload["campaign_id"])
            generation = int(payload["generation"])
            if not ObjectId.is_valid(campaign_id) or not ObjectId.is_valid(job.account_id):
                raise ValueError("invalid campaign discovery ownership identifiers")
            campaign = await database.campaigns_collection.find_one(
                {
                    "_id": ObjectId(campaign_id),
                    "account_id": {"$in": [job.account_id, ObjectId(job.account_id)]},
                    "discovery_generation": generation,
                },
                {"discovery_status": 1, "discovery_mode": 1},
            )
            if not campaign:
                raise PermissionError(
                    "campaign discovery job is stale or not tenant-owned"
                )
            if campaign.get("discovery_status") == "completed":
                pipeline_task = asyncio.create_task(
                    asyncio.sleep(0, result={"status": "already_completed"})
                )
            elif campaign.get("discovery_mode") == "upload":
                # BYOL: user-supplied lead list replaces the sourcing stages.
                from services.byol_discovery_service import run_byol_discovery
                pipeline_task = asyncio.create_task(
                    run_byol_discovery(campaign_id, job.account_id, generation=generation)
                )
            else:
                from services.curated_discovery_service import run_fast_discovery
                pipeline_task = asyncio.create_task(
                    run_fast_discovery(campaign_id, job.account_id, generation=generation)
                )
        elif job.job_type == CAMPAIGN_MESSAGE_GEN_JOB_TYPE:
            campaign_id = str(payload["campaign_id"])
            day = int(payload["day"])
            mode = str(payload.get("mode") or MESSAGE_GEN_MODE_GENERATE_DAY)
            if not ObjectId.is_valid(campaign_id) or not ObjectId.is_valid(job.account_id):
                raise ValueError("invalid campaign message-gen ownership identifiers")
            campaign = await database.campaigns_collection.find_one(
                {
                    "_id": ObjectId(campaign_id),
                    "account_id": {"$in": [job.account_id, ObjectId(job.account_id)]},
                },
                {"_id": 1},
            )
            if not campaign:
                raise PermissionError(
                    "campaign message-gen job is stale or not tenant-owned"
                )
            if mode == MESSAGE_GEN_MODE_ENSURE_DAY:
                from services.curated_discovery_service import ensure_day_ready_then_generate
                pipeline_task = asyncio.create_task(
                    ensure_day_ready_then_generate(campaign_id, job.account_id, day)
                )
            else:
                from services.campaign_message_generator_service import (
                    generate_messages_for_campaign,
                )
                pipeline_task = asyncio.create_task(
                    generate_messages_for_campaign(
                        campaign_id, job.account_id, send_day=day
                    )
                )
        elif job.job_type == CAMPAIGN_REMAINING_DAYS_JOB_TYPE:
            campaign_id = str(payload["campaign_id"])
            if not ObjectId.is_valid(campaign_id) or not ObjectId.is_valid(job.account_id):
                raise ValueError("invalid campaign remaining-days ownership identifiers")
            campaign = await database.campaigns_collection.find_one(
                {
                    "_id": ObjectId(campaign_id),
                    "account_id": {"$in": [job.account_id, ObjectId(job.account_id)]},
                },
                {"_id": 1},
            )
            if not campaign:
                raise PermissionError(
                    "campaign remaining-days job is stale or not tenant-owned"
                )
            from services.curated_discovery_service import _enrich_remaining_days
            remaining_oids = [
                ObjectId(value)
                for value in payload.get("remaining_oids") or []
                if ObjectId.is_valid(value)
            ]
            pipeline_task = asyncio.create_task(
                _enrich_remaining_days(
                    campaign_id=campaign_id,
                    account_id=job.account_id,
                    remaining_oids=remaining_oids,
                    co_research_by_url=payload.get("co_research_by_url") or None,
                    skip_message_gen=bool(payload.get("skip_message_gen")),
                )
            )
        else:
            raise ValueError(f"unsupported durable job type: {job.job_type}")
        done, _ = await asyncio.wait(
            {pipeline_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if heartbeat_task in done:
            # Stale workers must stop provider work as soon as lease ownership
            # is lost; they may not continue and later publish completion.
            heartbeat_task.result()
            raise RuntimeError("enrichment heartbeat stopped unexpectedly")
        result = pipeline_task.result()
        await queue.checkpoint(
            account_id=job.account_id,
            job_id=job.id,
            worker_id=worker_id,
            checkpoint={"phase": "work_completed", "work_id": work_id, "job_type": job.job_type},
        )
        completed = await queue.complete(
            account_id=job.account_id,
            job_id=job.id,
            worker_id=worker_id,
            result={"work_id": work_id, "result": result},
        )
        if completed is None:
            raise RuntimeError("enrichment job lease lost before completion")
    except Exception as exc:
        logger.exception("Enrichment job %s failed", job.id)
        if job.job_type == ENRICHMENT_JOB_TYPE:
            with suppress(Exception):
                await database.enrichment_runs_collection.update_one(
                    {"_id": ObjectId(str(job.payload.get("run_id"))), "account_id": job.account_id},
                    {"$set": {"last_worker_error": str(exc)[:500], "updated_at": datetime.utcnow()}},
                )
        await queue.fail(
            account_id=job.account_id,
            job_id=job.id,
            worker_id=worker_id,
            error=str(exc)[:1000],
            retry_delay_seconds=min(300, 15 * (2 ** max(job.attempt_count - 1, 0))),
        )
    finally:
        if pipeline_task is not None and not pipeline_task.done():
            pipeline_task.cancel()
            with suppress(asyncio.CancelledError):
                await pipeline_task
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


async def process_enrichment_jobs(max_jobs: int = 4) -> int:
    """Claim due enrichment jobs across tenant partitions.

    `JobQueueService.claim` deliberately requires a tenant.  The dispatcher
    discovers only tenants with due work, then performs the atomic claim at the
    tenant boundary.
    """
    now = datetime.now(timezone.utc)
    account_ids = await database.jobs_collection.distinct(
        "account_id",
        {
            "job_type": {"$in": list(DURABLE_JOB_TYPES)},
            "$or": [
                {"state": {"$in": [JobState.QUEUED.value, JobState.RETRY_SCHEDULED.value]}, "available_at": {"$lte": now}},
                {"state": JobState.RUNNING.value, "lease_expires_at": {"$lte": now}},
            ],
        },
    )
    queue = JobQueueService(database.jobs_collection)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:enrichment"
    claimed = []
    for account_id in account_ids:
        if len(claimed) >= max_jobs:
            break
        job = await queue.claim(
            account_id=str(account_id),
            worker_id=worker_id,
            lease_seconds=LEASE_SECONDS,
            job_types=list(DURABLE_JOB_TYPES),
            now=now,
        )
        if job is not None:
            claimed.append(job)
    if claimed:
        await asyncio.gather(
            *[_execute_claimed_job(queue, job, worker_id) for job in claimed],
            return_exceptions=False,
        )
    return len(claimed)
