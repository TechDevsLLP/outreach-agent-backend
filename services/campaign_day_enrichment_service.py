"""
Orchestrates full enrichment + per-channel message regeneration for a smart campaign day.
Called from routes/campaigns.py as a BackgroundTask.
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
    request,  # EnrichAndGenerateRequest — avoid circular import
) -> dict:
    now = datetime.now(timezone.utc)

    try:
        # Step 1: Load day-N enrollments
        enrollments = await campaign_enrollments_collection.find(
            {
                "campaign_id": ObjectId(campaign_id),
                "smart_campaign_send_day": day,
                "status": {"$nin": ["archived", "skipped_no_channel"]},
            }
        ).to_list(length=5000)

        if not enrollments:
            logger.warning(f"[enrich_and_generate] No enrollments for campaign={campaign_id} day={day}")
            await _mark_campaign_done(campaign_id, 0)
            return {"generated": 0, "failed": 0, "skipped": 0}

        prospect_ids = [str(e["prospect_id"]) for e in enrollments if e.get("prospect_id")]

        # Step 2: Load campaign
        campaign = await campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
        if not campaign:
            logger.error(f"[enrich_and_generate] Campaign {campaign_id} not found")
            return {"error": "campaign not found"}

        # Step 3: Run full enrichment pipeline (idempotent — skips already-enriched prospects).
        # skip_outreach=True because message generation is handled below.
        # skip_pre_enrichment_triage=True to avoid filtering out prospects that are already enrolled.
        # account_id passed via options so the pipeline can fetch the company profile.
        options = {
            "skip_outreach": True,
            "skip_pre_enrichment_triage": True,
            "account_id": ObjectId(account_id),
        }

        run_result = await enrichment_runs_collection.insert_one({
            "account_id": ObjectId(account_id),
            "status": "running",
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
            "started_at": now,
            "completed_at": None,
            "current_step": "initializing",
            "error": None,
            "triggered_by": f"campaign_day_enrich:{campaign_id}:{day}",
        })
        enrichment_run_id = str(run_result.inserted_id)

        logger.info(f"[enrich_and_generate] Starting enrichment for {len(prospect_ids)} prospects, campaign={campaign_id} day={day}")

        try:
            await run_enrichment_pipeline(
                enrichment_run_id=enrichment_run_id,
                prospect_ids=prospect_ids,
                options=options,
                triggered_by=None,  # account_id resolved via options["account_id"]
            )
        except Exception as e:
            logger.error(f"[enrich_and_generate] Enrichment failed for campaign={campaign_id}: {e}")
            # Continue to message generation — partial enrichment is fine

        # Step 4: Determine which enrollments to process based on channel toggles
        instructions = request.instructions
        regenerate_channels = request.regenerate_channels
        send_empty = request.send_empty_connection_request

        to_generate = []
        empty_connection_enrollments = []

        for e in enrollments:
            ch = e.get("smart_campaign_channel")
            if not ch:
                continue
            if not getattr(regenerate_channels, ch, True):
                continue
            if ch == "linkedin_connection" and send_empty:
                empty_connection_enrollments.append(e)
            else:
                to_generate.append(e)

        # Step 5: Handle empty connection requests (no LLM call — write blank note directly)
        for e in empty_connection_enrollments:
            await campaign_enrollments_collection.update_one(
                {"_id": e["_id"]},
                {
                    "$set": {
                        "generated_messages.linkedin_connection": {"note": ""},
                        "message_gen_status": "done",
                        "message_gen_error": None,
                        "message_gen_completed_at": datetime.now(timezone.utc),
                    },
                    "$inc": {"message_gen_attempts": 1},
                }
            )

        # Step 6: Regenerate messages for the remaining enrollments
        succeeded = len(empty_connection_enrollments)
        failed = 0

        if to_generate:
            # Reload fresh prospect data after enrichment
            prospect_oids_to_load = list({e["prospect_id"] for e in to_generate if e.get("prospect_id")})
            prospects_list = await prospects_collection.find(
                {"_id": {"$in": prospect_oids_to_load}}
            ).to_list(length=5000)
            prospects_by_id = {p["_id"]: p for p in prospects_list}

            client = OpenRouterClient()
            semaphore = asyncio.Semaphore(max(1, min(3, settings.ai_concurrency_limit)))

            async def _regen_one(enrollment: dict) -> bool:
                async with semaphore:
                    ch = enrollment.get("smart_campaign_channel")
                    prospect = prospects_by_id.get(enrollment.get("prospect_id"))
                    if not prospect:
                        logger.warning(f"[enrich_and_generate] Prospect not found for enrollment {enrollment['_id']}")
                        await campaign_enrollments_collection.update_one(
                            {"_id": enrollment["_id"]},
                            {
                                "$set": {
                                    "message_gen_status": "failed",
                                    "message_gen_error": "Prospect not found after enrichment",
                                },
                                "$inc": {"message_gen_attempts": 1},
                            }
                        )
                        return False

                    extra_instr = getattr(instructions, ch, None) if ch else None

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
                                {"_id": enrollment["_id"]},
                                {"$inc": {"message_gen_attempts": 1}},
                            )
                            return True
                        else:
                            await campaign_enrollments_collection.update_one(
                                {"_id": enrollment["_id"]},
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
                            {"_id": enrollment["_id"]},
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

        await _mark_campaign_done(campaign_id, succeeded)
        logger.info(f"[enrich_and_generate] Done: campaign={campaign_id} day={day} succeeded={succeeded} failed={failed}")
        return {"generated": succeeded, "failed": failed, "skipped": 0}

    except Exception as e:
        logger.error(f"[enrich_and_generate] Fatal error for campaign={campaign_id} day={day}: {e}", exc_info=True)
        await campaigns_collection.update_one(
            {"_id": ObjectId(campaign_id)},
            {"$set": {
                "message_gen_status": "failed",
                "message_gen_completed_at": datetime.now(timezone.utc),
            }}
        )
        raise


async def _mark_campaign_done(campaign_id: str, success_count: int):
    await campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$set": {
            "message_gen_status": "completed",
            "message_gen_completed_at": datetime.now(timezone.utc),
            "message_gen_prospects_done": success_count,
        }}
    )
