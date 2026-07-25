"""
Orchestrates full enrichment + per-channel message regeneration for a smart campaign day.
Executed only by the Mongo-leased durable worker.
"""
import asyncio
import logging
from datetime import datetime, timezone
from bson import ObjectId

from config import get_settings
from database import (
    campaigns_collection,
    campaign_enrollments_collection,
    enrichment_runs_collection,
    prospects_collection,
)
from services.enrichment_pipeline import run_enrichment_pipeline
from services.campaign_message_generator_service import generate_single_channel_message
from services.openrouter_service import OpenRouterClient

logger = logging.getLogger(__name__)
settings = get_settings()


async def run_enrich_and_generate_for_day(
    campaign_id: str,
    account_id: str,
    day: int,
    generation: int,
    request: dict,
    orchestration_job_id: str,
) -> dict:
    now = datetime.now(timezone.utc)
    if not ObjectId.is_valid(campaign_id) or not ObjectId.is_valid(account_id):
        raise ValueError("invalid campaign/account identifier")
    campaign_oid = ObjectId(campaign_id)
    account_oid = ObjectId(account_id)
    account_variants = [account_id, account_oid]
    campaign_variants = [campaign_id, campaign_oid]

    def enrollment_filter(enrollment_id: ObjectId) -> dict:
        return {
            "_id": enrollment_id,
            "account_id": {"$in": account_variants},
            "campaign_id": {"$in": campaign_variants},
        }

    try:
        campaign = await campaigns_collection.find_one(
            {"_id": campaign_oid, "account_id": {"$in": account_variants}}
        )
        if not campaign:
            raise LookupError("campaign not found in tenant")
        await campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": {"$in": account_variants}},
            {"$set": {"message_gen_status": "running", "message_gen_worker_started_at": now}},
        )

        # Step 1: Load tenant-owned day-N enrollments
        enrollments = await campaign_enrollments_collection.find(
            {
                "account_id": {"$in": account_variants},
                "campaign_id": {"$in": campaign_variants},
                "smart_campaign_send_day": day,
                "status": {"$nin": ["archived", "skipped_no_channel", "cascade_waiting"]},
            }
        ).to_list(length=5000)

        if not enrollments:
            logger.warning(f"[enrich_and_generate] No enrollments for campaign={campaign_id} day={day}")
            await _mark_campaign_done(campaign_id, account_id, 0)
            return {"generated": 0, "failed": 0, "skipped": 0}

        prospect_ids = [str(e["prospect_id"]) for e in enrollments if e.get("prospect_id")]

        # Step 3: Run full enrichment pipeline (idempotent — skips already-enriched prospects).
        # skip_outreach=True because message generation is handled below.
        # skip_pre_enrichment_triage=True to avoid filtering out prospects that are already enrolled.
        # account_id passed via options so the pipeline can fetch the company profile.
        options = {
            "skip_outreach": True,
            "skip_pre_enrichment_triage": True,
            "account_id": ObjectId(account_id),
        }

        run_doc = await enrichment_runs_collection.find_one({
            "account_id": account_id,
            "orchestration_job_id": orchestration_job_id,
        })
        if run_doc is None:
            run_result = await enrichment_runs_collection.insert_one({
                "account_id": account_id,
                "campaign_id": campaign_id,
                "orchestration_job_id": orchestration_job_id,
                "status": "queued",
                "total_prospects": len(prospect_ids),
                "prospects_processed": 0,
                "prospects_skipped": 0,
                "prospects_failed": 0,
                "profiles_scraped": 0,
                "companies_scraped": 0,
                "companies_deduplicated": 0,
                "ai_assessments_done": 0,
                "outreach_generated": 0,
                "prospect_ids": prospect_ids,
                "started_at": None,
                "created_at": now,
                "completed_at": None,
                "current_step": "queued",
                "error": None,
                "triggered_by": f"campaign_day_enrich:{campaign_id}:{day}",
            })
            enrichment_run_id = str(run_result.inserted_id)
            run_status = "queued"
        else:
            enrichment_run_id = str(run_doc["_id"])
            run_status = run_doc.get("status")

        logger.info(f"[enrich_and_generate] Starting enrichment for {len(prospect_ids)} prospects, campaign={campaign_id} day={day}")

        # Enrichment failing (scraping quota, a bad LinkedIn profile, a provider
        # outage) must NOT sink the whole day: Day-1 prospects arrive already
        # enriched from discovery so this call no-ops, but Day-2+ prospects
        # trigger real scraping here — a single provider error would otherwise
        # hard-fail every message. Record the reason and still generate messages
        # from whatever prospect data exists (matching the non-fatal backfill
        # below and the per-enrollment error handling in Step 6).
        enrichment_error: str | None = None
        try:
            if run_status != "completed":
                await run_enrichment_pipeline(
                    enrichment_run_id=enrichment_run_id,
                    prospect_ids=prospect_ids,
                    options=options,
                    triggered_by=None,
                    account_id=str(account_id),
                    campaign_id=str(campaign_id),
                )
        except Exception as e:
            enrichment_error = str(e)[:500]
            logger.error(
                f"[enrich_and_generate] Enrichment failed for campaign={campaign_id} "
                f"day={day} (continuing to message generation): {e}",
                exc_info=True,
            )

        # Step 3.5: Backfill deep enrichment (prospect_intelligence/pitch/posts/
        # company research). run_enrichment_pipeline only does company scrape +
        # AI score, so without this the detail page + message personalization get
        # nothing. Reuses already-stored company research; idempotent (skips
        # prospects that already have intelligence). Non-fatal on failure.
        try:
            from services.curated_discovery_service import backfill_missing_intelligence
            n_enriched = await backfill_missing_intelligence(
                prospect_ids=prospect_ids,
                campaign_id=str(campaign_id),
                account_id=str(account_id),
                label=f"day{day}_worker",
            )
            if n_enriched:
                logger.info(
                    f"[enrich_and_generate] backfilled intelligence for {n_enriched} "
                    f"prospects, campaign={campaign_id} day={day}"
                )
        except Exception as _bf_e:
            logger.warning(
                f"[enrich_and_generate] intelligence backfill failed (continuing): {_bf_e}"
            )

        # Step 4: Determine which enrollments to process based on channel toggles
        instructions = dict(request.get("instructions") or {})
        regenerate_channels = dict(request.get("regenerate_channels") or {})
        send_empty = bool(request.get("send_empty_connection_request", False))

        to_generate = []
        empty_connection_enrollments = []
        succeeded = 0

        for e in enrollments:
            ch = e.get("smart_campaign_channel")
            if not ch:
                continue
            # A discovery top-up bumps message_gen_generation for the whole
            # campaign, which must never be read as "this enrollment's copy is
            # stale" — only a channel with no generated message yet (or one
            # that outright failed) is eligible for regeneration; existing/
            # approved copy is never overwritten.
            already_has_message = bool((e.get("generated_messages") or {}).get(ch))
            if already_has_message and e.get("message_gen_status") != "failed":
                succeeded += 1
                continue
            if not regenerate_channels.get(ch, True):
                continue
            if ch == "linkedin_connection" and send_empty:
                empty_connection_enrollments.append(e)
            else:
                to_generate.append(e)

        # Step 5: Handle empty connection requests (no LLM call — write blank note directly)
        for e in empty_connection_enrollments:
            await campaign_enrollments_collection.update_one(
                enrollment_filter(e["_id"]),
                {
                    "$set": {
                        "generated_messages.linkedin_connection": {"note": ""},
                        "message_gen_status": "done",
                        "message_gen_generation": generation,
                        "message_gen_error": None,
                        "message_gen_completed_at": datetime.now(timezone.utc),
                    },
                    "$inc": {"message_gen_attempts": 1},
                }
            )

        # Step 6: Regenerate messages for the remaining enrollments
        succeeded += len(empty_connection_enrollments)
        failed = 0

        if to_generate:
            # Reload fresh prospect data after enrichment
            prospect_oids_to_load = []
            for enrollment in to_generate:
                value = enrollment.get("prospect_id")
                if ObjectId.is_valid(str(value)):
                    prospect_oids_to_load.append(ObjectId(str(value)))
            prospect_oids_to_load = list(set(prospect_oids_to_load))
            prospects_list = await prospects_collection.find(
                {"_id": {"$in": prospect_oids_to_load}}
            ).to_list(length=5000)
            prospects_by_id = {p["_id"]: p for p in prospects_list}

            client = OpenRouterClient()
            semaphore = asyncio.Semaphore(max(1, min(3, settings.ai_concurrency_limit)))

            async def _regen_one(enrollment: dict) -> bool:
                async with semaphore:
                    ch = enrollment.get("smart_campaign_channel")
                    raw_prospect_id = enrollment.get("prospect_id")
                    prospect_key = ObjectId(str(raw_prospect_id)) if ObjectId.is_valid(str(raw_prospect_id)) else None
                    prospect = prospects_by_id.get(prospect_key)
                    if not prospect:
                        logger.warning(f"[enrich_and_generate] Prospect not found for enrollment {enrollment['_id']}")
                        await campaign_enrollments_collection.update_one(
                            enrollment_filter(enrollment["_id"]),
                            {
                                "$set": {
                                    "message_gen_status": "failed",
                                    "message_gen_error": "Prospect not found after enrichment",
                                },
                                "$inc": {"message_gen_attempts": 1},
                            }
                        )
                        return False

                    extra_instr = instructions.get(ch) if ch else None

                    try:
                        msgs = await generate_single_channel_message(
                            enrollment=enrollment,
                            prospect=prospect,
                            campaign=campaign,
                            client=client,
                            additional_instructions=extra_instr,
                        )
                        if msgs:
                            await campaign_enrollments_collection.update_one(
                                enrollment_filter(enrollment["_id"]),
                                {
                                    "$set": {"message_gen_generation": generation},
                                    "$inc": {"message_gen_attempts": 1},
                                },
                            )
                            return True
                        else:
                            await campaign_enrollments_collection.update_one(
                                enrollment_filter(enrollment["_id"]),
                                {
                                    "$set": {
                                        "message_gen_status": "failed",
                                        "message_gen_error": "Regenerate returned no messages",
                                    },
                                    "$inc": {"message_gen_attempts": 1},
                                }
                            )
                            return False
                    except Exception as ex:
                        logger.error(f"[enrich_and_generate] Message gen failed for enrollment {enrollment['_id']}: {ex}")
                        await campaign_enrollments_collection.update_one(
                            enrollment_filter(enrollment["_id"]),
                            {
                                "$set": {
                                    "message_gen_status": "failed",
                                    "message_gen_error": str(ex)[:500],
                                },
                                "$inc": {"message_gen_attempts": 1},
                            }
                        )
                        return False

            try:
                results = await asyncio.gather(*[_regen_one(e) for e in to_generate], return_exceptions=True)
            finally:
                await client.close()

            for r in results:
                if r is True:
                    succeeded += 1
                else:
                    failed += 1

        await _mark_campaign_done(campaign_id, account_id, succeeded, enrichment_error)
        logger.info(f"[enrich_and_generate] Done: campaign={campaign_id} day={day} succeeded={succeeded} failed={failed}")
        return {"generated": succeeded, "failed": failed, "skipped": 0, "enrichment_error": enrichment_error}

    except Exception as e:
        logger.error(f"[enrich_and_generate] Fatal error for campaign={campaign_id} day={day}: {e}", exc_info=True)
        await campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": {"$in": account_variants}},
            {"$set": {
                "message_gen_status": "failed",
                "message_gen_completed_at": datetime.now(timezone.utc),
            }}
        )
        raise


async def _mark_campaign_done(
    campaign_id: str,
    account_id: str,
    success_count: int,
    enrichment_error: str | None = None,
):
    await campaigns_collection.update_one(
        {
            "_id": ObjectId(campaign_id),
            "account_id": {"$in": [str(account_id), ObjectId(str(account_id))]},
        },
        {"$set": {
            "message_gen_status": "completed",
            "message_gen_completed_at": datetime.now(timezone.utc),
            "message_gen_prospects_done": success_count,
            # Non-fatal enrichment issue (e.g. scraping quota); messages were
            # still generated. Surfaced for observability; cleared on a clean run.
            "message_gen_enrichment_warning": enrichment_error,
        }}
    )
