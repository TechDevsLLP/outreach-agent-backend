"""
Smart Campaign Message Generator Service.

Generates per-prospect personalized outreach messages for smart campaigns.
Uses the campaign's tone/value_prop/pain_point/CTA to produce:
  - cold_email: subject_a, subject_b, body
  - linkedin_connection: note (<=280 chars)
  - linkedin_inmail: subject, body

Runs concurrently with a semaphore (5 at a time) via asyncio.gather.
"""

import asyncio
import logging
from datetime import datetime
from bson import ObjectId
from typing import Optional

import database
from config import get_settings
from services.openrouter_service import OpenRouterClient, get_free_model, FREE_MODELS
from utils.prompts import (
    build_campaign_outreach_prompt,
    build_campaign_batch_outreach_prompt,
    get_system_prompt,
)

logger = logging.getLogger(__name__)
settings = get_settings()


# Haiku 4.5 is fast + cheap and has high per-key rate limits, so it's the
# right primary for message generation at Day-1 cohort scale (50+ messages
# in parallel batches). Free models are intentionally NOT in this chain —
# they 429 immediately under batch load and degrade quality noticeably.
MESSAGE_GEN_PRIMARY_MODEL = "anthropic/claude-haiku-4-5"
MESSAGE_GEN_FALLBACK_MODEL = "anthropic/claude-sonnet-4-5"
MESSAGE_GEN_BATCH_SIZE = 3
MESSAGE_GEN_BATCH_CONCURRENCY = 4


async def _resolve_sender_first_name(account_id) -> str:
    """
    Look up the configured sender name for an account and return its first token.
    Prefers company_profiles.sender_name; falls back to accounts.sender_name.
    Returns an empty string if neither is set.
    """
    if account_id is None:
        return ""
    profile = await database.company_profiles_collection.find_one(
        {"account_id": account_id}, {"sender_name": 1}
    )
    name = (profile or {}).get("sender_name") or ""
    if not name:
        account = await database.accounts_collection.find_one(
            {"_id": account_id}, {"sender_name": 1}
        )
        name = (account or {}).get("sender_name") or ""
    name = name.strip()
    if not name:
        return ""
    return name.split()[0]


async def _load_company_profile(account_id) -> dict | None:
    """Load the company profile for an account. Returns None if not found."""
    if account_id is None:
        return None
    try:
        profile = await database.company_profiles_collection.find_one({"account_id": account_id})
        return profile
    except Exception as e:
        logger.warning(f"Failed to load company profile for account {account_id}: {e}")
        return None


def _message_gen_fallback_chain() -> list[str]:
    """
    Fallback chain for message generation — paid models only.

    Haiku 4.5 is primary (fast, cheap, high rate limits). Sonnet 4.5 is the
    single insurance fallback if Haiku fails with a structural error (malformed
    JSON, schema violation, etc.). Free models are excluded: they 429 instantly
    under the concurrency we need to finish 55 messages quickly.
    """
    chain: list[str] = []
    if MESSAGE_GEN_PRIMARY_MODEL:
        chain.append(MESSAGE_GEN_PRIMARY_MODEL)
    if MESSAGE_GEN_FALLBACK_MODEL and MESSAGE_GEN_FALLBACK_MODEL not in chain:
        chain.append(MESSAGE_GEN_FALLBACK_MODEL)
    return chain


async def generate_messages_for_campaign(
    campaign_id: str,
    account_id: str,
    send_day: int | None = None,
) -> dict:
    """
    Generate personalized outreach messages for enrolled prospects.

    When ``send_day`` is provided, only generates messages for enrollments on
    that specific day — used by the day-by-day approval flow where Day N+1
    messages are generated right after Day N is approved. The campaign-level
    ``message_gen_status`` is only updated when send_day is None or == 1 to
    preserve the "ready for review" semantics for the initial Day-1 batch.

    Returns: {"generated": N, "failed": M, "total": T}
    """
    from services.campaign_discovery_logger import CampaignDiscoveryLogger

    campaign_oid = ObjectId(campaign_id)
    now = datetime.utcnow()
    _acct_id_str = str(account_id) if account_id else ""
    disc_log = CampaignDiscoveryLogger(campaign_id, _acct_id_str, settings.discovery_log_dir)
    await disc_log.__aenter__()

    # Only flip the campaign-level running flag on initial / Day-1 batch;
    # subsequent day batches run in the background without disturbing the
    # top-level state (the UI shows per-day status via enrollments).
    update_campaign_status = send_day is None or send_day == 1
    if update_campaign_status:
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {
                "message_gen_status": "running",
                "message_gen_started_at": now,
                "message_gen_prospects_done": 0,
            }},
        )

    try:
        campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
        if not campaign:
            raise ValueError(f"Campaign {campaign_id} not found")

        # Stamp sender name and company profile onto the in-memory campaign dict
        # so prompt builders can include full seller identity and case studies.
        account_id = campaign.get("account_id")
        sender_first = await _resolve_sender_first_name(account_id)
        if sender_first:
            campaign["_sender_first_name"] = sender_first
        company_profile = await _load_company_profile(account_id)
        if company_profile:
            campaign["_company_profile"] = company_profile

        # Load pending enrollments, optionally scoped to a specific day.
        # For per-day runs, lift Day-N enrollments out of "scheduled_later"
        # into "pending" first so the rest of the pipeline can treat them
        # uniformly.
        if send_day is not None:
            await database.campaign_enrollments_collection.update_many(
                {
                    "campaign_id": campaign_oid,
                    "smart_campaign_send_day": send_day,
                    "message_gen_status": {"$in": ["scheduled_later", "failed"]},
                },
                {"$set": {"message_gen_status": "pending", "message_gen_error": None}},
            )

        query: dict = {
            "campaign_id": campaign_oid,
            "message_gen_status": "pending",
        }
        if send_day is not None:
            query["smart_campaign_send_day"] = send_day

        cursor = database.campaign_enrollments_collection.find(query)
        enrollments = await cursor.to_list(length=1000)

        if not enrollments:
            logger.info(f"[Campaign {campaign_id}] No pending enrollments for message generation (day={send_day})")
            await disc_log.log(phase="messages", event="no_pending_enrollments", send_day=send_day)
            if update_campaign_status:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid},
                    {"$set": {
                        "message_gen_status": "completed",
                        "message_gen_completed_at": datetime.utcnow(),
                    }},
                )
            return {"generated": 0, "failed": 0, "total": 0}

        # Batch-fetch all prospects
        prospect_ids = [e["prospect_id"] for e in enrollments]
        prospects_list = await database.prospects_collection.find(
            {"_id": {"$in": prospect_ids}}
        ).to_list(length=len(prospect_ids))
        prospects_by_id = {p["_id"]: p for p in prospects_list}

        total = len(enrollments)
        generated = 0
        failed = 0

        await disc_log.log(
            phase="messages", event="generation_started",
            send_day=send_day, total_enrollments=total,
            channel_breakdown={
                ch: len([e for e in enrollments if e.get("smart_campaign_channel") == ch])
                for ch in {"email", "linkedin_connection", "linkedin_inmail"}
                if any(e.get("smart_campaign_channel") == ch for e in enrollments)
            },
        )

        client = OpenRouterClient()

        # ── Batched generation ──────────────────────────────────────────────
        # Group enrollments by their assigned smart-campaign channel. Each
        # channel batches ~MESSAGE_GEN_BATCH_SIZE prospects into a single
        # Haiku call — one request returns N messages, so 55 prospects finish
        # in ~6 calls total (vs 55 in the legacy per-prospect loop).
        #
        # Enrollments WITHOUT an assigned channel fall back to the per-prospect
        # full-schema path (rare — only happens for non-smart campaigns or
        # legacy enrollments that predate channel planning).
        channel_groups: dict[str, list[dict]] = {}
        orphan_enrollments: list[dict] = []
        for enr in enrollments:
            ch = enr.get("smart_campaign_channel")
            if campaign.get("is_smart_campaign") and ch in {"email", "linkedin_connection", "linkedin_inmail"}:
                channel_groups.setdefault(ch, []).append(enr)
            else:
                orphan_enrollments.append(enr)

        batch_semaphore = asyncio.Semaphore(MESSAGE_GEN_BATCH_CONCURRENCY)

        async def _mark_enrollment_failed(enr: dict, err_msg: str) -> None:
            nonlocal failed
            failed += 1
            await database.campaign_enrollments_collection.update_one(
                {"_id": enr["_id"]},
                {"$set": {
                    "message_gen_status": "failed",
                    "message_gen_error": err_msg[:500],
                }, "$inc": {"message_gen_attempts": 1}},
            )

        async def _retry_single(enr: dict, prospect: dict) -> None:
            """Fallback: retry one enrollment via the single-channel path (with jitter to avoid 429s)."""
            nonlocal generated
            import random
            await asyncio.sleep(random.uniform(0.5, 3.0))
            try:
                result = await generate_single_channel_message(enr, prospect, campaign, client)
                if result:
                    generated += 1
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$inc": {"message_gen_prospects_done": 1}},
                    )
                else:
                    await _mark_enrollment_failed(enr, "single-channel retry returned no message")
            except Exception as e:
                await _mark_enrollment_failed(enr, f"single-channel retry failed: {e}")

        async def _process_batch(channel: str, batch: list[dict]) -> None:
            nonlocal generated
            async with batch_semaphore:
                # Skip enrollments whose prospect doc is missing (corner case).
                usable: list[tuple[dict, dict]] = []  # (enrollment, prospect)
                for enr in batch:
                    p = prospects_by_id.get(enr["prospect_id"])
                    if p is None:
                        await _mark_enrollment_failed(enr, "Prospect not found")
                        continue
                    usable.append((enr, p))
                if not usable:
                    return

                try:
                    written = await generate_messages_batch(
                        client, campaign, usable, channel,
                    )
                except Exception as batch_err:
                    logger.error(
                        f"[Campaign {campaign_id}] Batch ({channel}, n={len(usable)}) failed: {batch_err}",
                        exc_info=True,
                    )
                    # Full-batch failure → retry each enrollment individually so
                    # a single bad response doesn't poison the whole channel.
                    for enr, prospect in usable:
                        await _retry_single(enr, prospect)
                    return

                # written is a set of enrollment _ids that got messages in the
                # batch response. Anything missing → retry individually.
                generated += len(written)
                if written:
                    await database.campaigns_collection.update_one(
                        {"_id": campaign_oid},
                        {"$inc": {"message_gen_prospects_done": len(written)}},
                    )
                for enr, prospect in usable:
                    if enr["_id"] not in written:
                        await _retry_single(enr, prospect)

        async def _process_orphan(enr: dict) -> None:
            """Non-smart-campaign path: generate the full 3-channel schema."""
            nonlocal generated
            async with batch_semaphore:
                prospect = prospects_by_id.get(enr["prospect_id"])
                if prospect is None:
                    await _mark_enrollment_failed(enr, "Prospect not found")
                    return
                try:
                    res = await generate_messages_for_enrollment(enr, prospect, campaign, client)
                    if res:
                        generated += 1
                        await database.campaigns_collection.update_one(
                            {"_id": campaign_oid},
                            {"$inc": {"message_gen_prospects_done": 1}},
                        )
                    else:
                        await _mark_enrollment_failed(enr, "Generator returned no messages")
                except Exception as e:
                    logger.error(f"Orphan message gen failed for enrollment {enr['_id']}: {e}", exc_info=True)
                    await _mark_enrollment_failed(enr, str(e))

        tasks = []
        for channel, enrs in channel_groups.items():
            for i in range(0, len(enrs), MESSAGE_GEN_BATCH_SIZE):
                chunk = enrs[i:i + MESSAGE_GEN_BATCH_SIZE]
                tasks.append(_process_batch(channel, chunk))
        for enr in orphan_enrollments:
            tasks.append(_process_orphan(enr))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=False)
        await client.close()

        # Overwrite the $inc-accumulated counter with a ground-truth DB count
        # to self-heal any double-counting from partial batch responses + retries.
        try:
            query_day: dict = {"campaign_id": campaign_oid, "generated_messages": {"$ne": None}}
            query_day["smart_campaign_send_day"] = send_day if send_day is not None else 1
            true_done = await database.campaign_enrollments_collection.count_documents(query_day)
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {"message_gen_prospects_done": true_done}},
            )
            generated = true_done
        except Exception as _recon_err:
            logger.warning(f"[Campaign {campaign_id}] message_gen reconciliation error: {_recon_err}")

        if update_campaign_status:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "message_gen_status": "completed",
                    "message_gen_completed_at": datetime.utcnow(),
                }},
            )

        logger.info(
            f"[Campaign {campaign_id}] Message generation complete (day={send_day}): "
            f"{generated}/{total} generated, {failed} failed"
        )
        await disc_log.log(
            phase="messages", event="generation_complete",
            send_day=send_day, generated=generated, failed=failed, total=total,
        )
        await disc_log.finalize({
            "status": "messages_complete",
            "send_day": send_day,
            "messages_generated": generated,
            "messages_failed": failed,
            "messages_total": total,
        })
        return {"generated": generated, "failed": failed, "total": total}

    except Exception as e:
        logger.error(f"[Campaign {campaign_id}] Message generation error: {e}", exc_info=True)
        await disc_log.error("messages", "generation_failed", exc=e, send_day=send_day)
        await disc_log.finalize({"status": "failed", "phase": "messages", "error": str(e)})
        if update_campaign_status:
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid},
                {"$set": {
                    "message_gen_status": "failed",
                }},
            )
        raise
    finally:
        await disc_log.__aexit__(None, None, None)


async def generate_messages_batch(
    client: OpenRouterClient,
    campaign: dict,
    enrollments_with_prospects: list[tuple[dict, dict]],
    channel: str,
) -> set:
    """
    Generate messages for N enrollments in a single Haiku call.

    Builds a compact batch prompt that emits one channel-specific message per
    prospect, parses the JSON array response, persists each message back to
    its enrollment, and returns the set of enrollment _ids that got a message.

    Caller is responsible for retrying any missing enrollments individually.
    """
    if not enrollments_with_prospects:
        return set()

    # Derive account_id / campaign_id for cost tracking
    _account_id = str(campaign.get("account_id") or "")
    _campaign_id = str(campaign.get("_id") or "")

    # Load per-tenant pitch overlays from prospect_state for all prospects in this batch,
    # then inject merged prospect_intelligence onto each prospect dict so the batch
    # prompt builder can read it from prospect.get("prospect_intelligence").
    # This fixes the intel-key mismatch: new pipeline writes prospect_intelligence_base
    # + prospect_state.pitch, but the batch prompt builder reads prospect_intelligence.
    if _account_id:
        try:
            _pids = [str(enr["prospect_id"]) for enr, _ in enrollments_with_prospects]
            _pitch_docs = await database.prospect_state_collection.find(
                {"account_id": _account_id, "prospect_id": {"$in": _pids}},
                {"prospect_id": 1, "pitch": 1},
            ).to_list(length=len(_pids))
            _pitch_by_pid = {doc["prospect_id"]: (doc.get("pitch") or {}) for doc in _pitch_docs}
        except Exception:
            _pitch_by_pid = {}

        for enr, p in enrollments_with_prospects:
            base = p.get("prospect_intelligence_base") or {}
            pitch = _pitch_by_pid.get(str(enr["prospect_id"]), {})
            if base or pitch:
                p["prospect_intelligence"] = {**base, **pitch}

    enrollment_by_id = {str(enr["_id"]): enr for enr, _p in enrollments_with_prospects}
    prospects_with_ids = [(str(enr["_id"]), p) for enr, p in enrollments_with_prospects]

    system_prompt = await get_system_prompt("campaign_outreach")
    user_prompt = build_campaign_batch_outreach_prompt(campaign, prospects_with_ids, channel)

    fallback_chain = _message_gen_fallback_chain()
    primary_model = fallback_chain[0] if fallback_chain else MESSAGE_GEN_PRIMARY_MODEL
    secondary = [m for m in fallback_chain if m != primary_model]

    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=primary_model,
        fallback_models=secondary,
        temperature=0.7,
        max_tokens=max(2000, 350 * len(prospects_with_ids)),
        response_format={"type": "json_object"},
        account_id=_account_id or None,
        campaign_id=_campaign_id or None,
        feature="message_generation",
    )

    if not result or not isinstance(result, dict):
        raise ValueError(f"Batch AI returned invalid response type: {type(result)}")

    messages_arr = result.get("messages")
    if not isinstance(messages_arr, list):
        raise ValueError("Batch response missing 'messages' array")

    now = datetime.utcnow()
    written: set = set()

    for item in messages_arr:
        if not isinstance(item, dict):
            continue
        eid_raw = item.get("id")
        if eid_raw is None:
            continue
        eid = str(eid_raw)
        if eid not in enrollment_by_id:
            continue
        enr = enrollment_by_id[eid]

        # Extract the per-channel payload from the item. We accept either the
        # flat shape (subject/body on the top-level item) or a nested shape
        # (item has a nested channel object).
        channel_data: dict | None = None
        if channel == "linkedin_connection":
            note = item.get("note") or (item.get("linkedin_connection") or {}).get("note")
            if note:
                channel_data = {"note": _truncate_to_280(str(note))}
        elif channel == "email":
            nested = item.get("cold_email") or {}
            subject_a = item.get("subject_a") or nested.get("subject_a") or item.get("subject") or nested.get("subject")
            subject_b = item.get("subject_b") or nested.get("subject_b") or subject_a
            body = item.get("body") or nested.get("body")
            if body:
                channel_data = {
                    "subject_a": str(subject_a or "").strip(),
                    "subject_b": str(subject_b or "").strip(),
                    "body": str(body).strip(),
                }
        elif channel == "linkedin_inmail":
            nested = item.get("linkedin_inmail") or {}
            subject = item.get("subject") or nested.get("subject")
            body = item.get("body") or nested.get("body")
            if body:
                channel_data = {
                    "subject": str(subject or "").strip(),
                    "body": str(body).strip(),
                }

        if not channel_data:
            # Missing required content — let the caller retry this one alone.
            continue

        generated_messages = {
            "channel": channel,
            channel: channel_data,
            "generated_at": now.isoformat(),
            "generation_model": primary_model,
            "tone_used": campaign.get("message_tone", "professional"),
        }
        generated_messages = _strip_em_dashes_from_messages(generated_messages)

        await database.campaign_enrollments_collection.update_one(
            {"_id": enr["_id"]},
            {"$set": {
                "generated_messages": generated_messages,
                "message_gen_status": "done",
                "message_gen_error": None,
                "message_gen_completed_at": now,
            }, "$inc": {"message_gen_attempts": 1}},
        )
        written.add(enr["_id"])

    return written


async def generate_messages_for_enrollment(
    enrollment: dict,
    prospect: dict,
    campaign: dict,
    client: OpenRouterClient,
    model: str | None = None,
    additional_instructions: str | None = None,
) -> Optional[dict]:
    """
    Generate personalized messages for a single enrollment.

    model: preferred primary model; when None, uses the paid-first fallback chain.
    additional_instructions: optional user-provided guidance appended to the prompt
        (e.g. "make it funnier", "shorten to 2 sentences"). Used for regeneration.
    Returns: generated messages dict or None on failure.
    """
    # Pull LinkedIn profile and company data from prospect
    profile = prospect.get("linkedin_profile_data") or None
    company = prospect.get("company_linkedin_data") or None

    # Load per-tenant pitch from prospect_state
    _pitch = {}
    try:
        _state = await database.prospect_state_collection.find_one(
            {
                "account_id": str(enrollment.get("account_id", "")),
                "prospect_id": str(enrollment.get("prospect_id", "")),
            },
            {"pitch": 1},
        )
        if _state:
            _pitch = _state.get("pitch") or {}
    except Exception:
        pass

    # Merge prospect_intelligence_base with per-tenant pitch (pitch keys override)
    intel_base = prospect.get("prospect_intelligence_base") or {}
    merged_intel = {**intel_base, **_pitch}

    # Build prompts
    system_prompt = await get_system_prompt("campaign_outreach")
    company_profile = campaign.get("_company_profile")
    user_prompt = build_campaign_outreach_prompt(
        prospect, profile, company, campaign,
        additional_instructions=additional_instructions,
        company_profile=company_profile,
        intelligence=merged_intel if merged_intel else None,
    )

    # Paid-primary fallback chain; caller can override with `model`
    fallback_chain = _message_gen_fallback_chain()
    primary_model = model or fallback_chain[0]

    # Call OpenRouter AI
    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=primary_model,
        fallback_models=[m for m in fallback_chain if m != primary_model],
        temperature=0.7,
        max_tokens=4000,
        response_format={"type": "json_object"},
    )

    if not result or not isinstance(result, dict):
        raise ValueError(f"AI returned invalid response: {type(result)}")

    # Validate required keys
    required_keys = ["cold_email", "linkedin_connection", "linkedin_inmail"]
    for key in required_keys:
        if key not in result:
            raise ValueError(f"AI response missing required key: {key}")

    # Validate cold_email sub-keys
    cold_email = result.get("cold_email", {})
    if not isinstance(cold_email, dict):
        raise ValueError("cold_email must be a dict")
    if not cold_email.get("body"):
        raise ValueError("cold_email.body is required")

    # Strip em dashes from all generated text
    result = _strip_em_dashes_from_messages(result)

    # Enforce LinkedIn connection note 280-char limit
    linkedin_conn = result.get("linkedin_connection", {})
    if isinstance(linkedin_conn, dict) and linkedin_conn.get("note"):
        linkedin_conn["note"] = _truncate_to_280(linkedin_conn["note"])
        result["linkedin_connection"] = linkedin_conn

    # Add metadata
    result["generated_at"] = datetime.utcnow().isoformat()
    result["generation_model"] = primary_model
    result["tone_used"] = campaign.get("message_tone", "professional")

    # Store in enrollment
    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment["_id"]},
        {"$set": {
            "generated_messages": result,
            "message_gen_status": "done",
            "message_gen_error": None,
            "message_gen_completed_at": datetime.utcnow(),
        }},
    )

    return result


async def generate_single_channel_message(
    enrollment: dict,
    prospect: dict,
    campaign: dict,
    client: OpenRouterClient,
    model: str | None = None,
    additional_instructions: str | None = None,
) -> Optional[dict]:
    """
    Generate a personalized message for ONLY the enrollment's assigned channel.
    Used for smart campaigns where one channel is pre-assigned per prospect.
    Returns updated generated_messages dict or None on failure.
    """
    channel = enrollment.get("smart_campaign_channel")
    if not channel:
        # No channel assigned — fall back to generating all
        return await generate_messages_for_enrollment(
            enrollment, prospect, campaign, client,
            model=model, additional_instructions=additional_instructions,
        )

    profile = prospect.get("linkedin_profile_data") or None
    company = prospect.get("company_linkedin_data") or None

    # Load per-tenant pitch from prospect_state
    _pitch = {}
    try:
        _state = await database.prospect_state_collection.find_one(
            {
                "account_id": str(enrollment.get("account_id", "")),
                "prospect_id": str(enrollment.get("prospect_id", "")),
            },
            {"pitch": 1},
        )
        if _state:
            _pitch = _state.get("pitch") or {}
    except Exception:
        pass

    # Merge prospect_intelligence_base with per-tenant pitch (pitch keys override)
    intel_base = prospect.get("prospect_intelligence_base") or {}
    merged_intel = {**intel_base, **_pitch}

    system_prompt = await get_system_prompt("campaign_outreach")
    company_profile = campaign.get("_company_profile")
    user_prompt = build_campaign_outreach_prompt(
        prospect, profile, company, campaign,
        additional_instructions=additional_instructions,
        company_profile=company_profile,
        intelligence=merged_intel if merged_intel else None,
    )

    # Append channel-specific instruction
    if channel == "linkedin_connection":
        user_prompt += "\n\nIMPORTANT: Generate ONLY the linkedin_connection object. Set cold_email and linkedin_inmail to minimal placeholders."
    elif channel == "email":
        user_prompt += "\n\nIMPORTANT: Generate ONLY the cold_email object. Set linkedin_connection and linkedin_inmail to minimal placeholders."
    elif channel == "linkedin_inmail":
        user_prompt += "\n\nIMPORTANT: Generate ONLY the linkedin_inmail object. Set cold_email and linkedin_connection to minimal placeholders."

    fallback_chain = _message_gen_fallback_chain()
    primary_model = model or fallback_chain[0]

    result = await client.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=primary_model,
        fallback_models=[m for m in fallback_chain if m != primary_model],
        temperature=0.7,
        max_tokens=2000,
        response_format={"type": "json_object"},
    )

    if not result or not isinstance(result, dict):
        raise ValueError(f"AI returned invalid response: {type(result)}")

    # Extract only the assigned channel data
    now = datetime.utcnow()
    channel_data = None
    if channel == "linkedin_connection":
        raw = result.get("linkedin_connection", {})
        if isinstance(raw, dict) and raw.get("note"):
            note = _truncate_to_280(raw["note"])
            channel_data = {"note": note}
        elif not channel_data:
            raise ValueError("linkedin_connection.note missing from AI response")
    elif channel == "email":
        raw = result.get("cold_email", {})
        if isinstance(raw, dict) and raw.get("body"):
            channel_data = raw
        else:
            raise ValueError("cold_email.body missing from AI response")
    elif channel == "linkedin_inmail":
        raw = result.get("linkedin_inmail", {})
        if isinstance(raw, dict) and raw.get("body"):
            channel_data = raw
        else:
            raise ValueError("linkedin_inmail.body missing from AI response")

    # Build single-channel generated_messages
    messages = {
        "channel": channel,
        channel: channel_data,
        "generated_at": now.isoformat(),
        "generation_model": primary_model,
        "tone_used": campaign.get("message_tone", "professional"),
    }

    # Store in enrollment
    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment["_id"]},
        {"$set": {
            "generated_messages": messages,
            "message_gen_status": "done",
            "message_gen_error": None,
            "message_gen_completed_at": now,
        }},
    )

    return messages


async def generate_message_for_step(
    db,
    campaign_id: str,
    enrollment_id: str,
    node_id: str,
    node: dict,
    campaign: dict,
    prospect: dict,
    linkedin_profile: dict = None,
    company_data: dict = None,
) -> dict:
    """Generate a single message for the given flow node and persist as message_drafts[node_id]."""
    channel = node.get("channel", "email")
    # Build prompt context
    from utils.prompts import build_campaign_outreach_prompt, CAMPAIGN_OUTREACH_SYSTEM_PROMPT
    node_type = "follow_up" if node.get("delay_days", 0) > 0 else "first_touch"

    company_profile = campaign.get("_company_profile")
    user_prompt = build_campaign_outreach_prompt(prospect, linkedin_profile or {}, company_data or {}, campaign, company_profile=company_profile)
    user_prompt += f"\n\nNote: This message is for the '{channel}' channel. Type: {node_type}."
    if channel == "linkedin_connection":
        user_prompt += " Generate ONLY the linkedin_connection note (max 280 chars). Keep other fields minimal."
    elif channel == "email":
        user_prompt += " Generate ONLY the cold_email fields. Keep linkedin fields minimal."
    elif channel == "linkedin_inmail":
        user_prompt += " Generate ONLY the linkedin_inmail fields. Keep other fields minimal."
    elif channel == "linkedin_message":
        user_prompt += " Generate ONLY a linkedin_message (use linkedin_connection.note field). Keep other fields minimal."

    client = OpenRouterClient()
    model = get_free_model(0)
    try:
        result = await client.chat_completion(
            model=model,
            messages=[
                {"role": "system", "content": CAMPAIGN_OUTREACH_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        generated = result if isinstance(result, dict) else {}
        # Strip em dashes from all generated text
        generated = _strip_em_dashes_from_messages(generated)
    except Exception as e:
        logger.error(f"generate_message_for_step AI call failed: {e}")
        generated = {}
    finally:
        await client.close()

    # Extract the relevant message for this channel
    now = datetime.utcnow()
    if channel == "linkedin_connection" or channel == "linkedin_message":
        li_conn = generated.get("linkedin_connection", {})
        note = li_conn.get("note", "")
        if len(note) > 280:
            note = note[:277] + "..."
        draft = {"channel": channel, "body": note, "status": "pending_review", "generated_at": now.isoformat()}
    elif channel == "email":
        email_data = generated.get("cold_email", {})
        draft = {
            "channel": "email",
            "subject": email_data.get("subject_a", ""),
            "body": email_data.get("body", ""),
            "status": "pending_review",
            "generated_at": now.isoformat(),
        }
    elif channel == "linkedin_inmail":
        inmail = generated.get("linkedin_inmail", {})
        draft = {
            "channel": "linkedin_inmail",
            "subject": inmail.get("subject", ""),
            "body": inmail.get("body", ""),
            "status": "pending_review",
            "generated_at": now.isoformat(),
        }
    else:
        draft = {"channel": channel, "body": "", "status": "pending_review", "generated_at": now.isoformat()}

    # Persist to enrollment
    await db.campaign_enrollments.update_one(
        {"_id": ObjectId(enrollment_id)},
        {"$set": {
            f"message_drafts.{node_id}": draft,
            "message_gen_status": "done",
            "message_gen_completed_at": now,
        }},
    )
    return draft


async def generate_messages_for_day_batch(
    db,
    campaign_id: str,
    enrollments_with_nodes: list[dict],
) -> dict:
    """
    Generate messages for a list of {enrollment, node, campaign, prospect} dicts.
    Runs with concurrency 5.
    Returns {"generated": N, "failed": M}
    """
    semaphore = asyncio.Semaphore(5)
    generated = 0
    failed = 0

    async def _one(item: dict):
        nonlocal generated, failed
        async with semaphore:
            try:
                await generate_message_for_step(
                    db=db,
                    campaign_id=campaign_id,
                    enrollment_id=item["enrollment_id"],
                    node_id=item["node_id"],
                    node=item["node"],
                    campaign=item["campaign"],
                    prospect=item["prospect"],
                    linkedin_profile=item.get("linkedin_profile"),
                    company_data=item.get("company_data"),
                )
                generated += 1
            except Exception as e:
                logger.error(f"generate_messages_for_day_batch item failed: {e}")
                failed += 1

    await asyncio.gather(*[_one(item) for item in enrollments_with_nodes], return_exceptions=True)
    return {"generated": generated, "failed": failed}


async def generate_message_for_node(
    campaign: dict,
    enrollment: dict,
    prospect: dict,
    node: dict,
    prior_step_messages: list | None = None,
) -> dict:
    """
    Generate a message for a specific flow node and persist to generated_messages_by_step[node_id].
    For node n1, also writes the legacy flat generated_messages for backward compat.

    prior_step_messages: list of {channel, subject?, body_excerpt} from already-sent steps.
    Returns the stored message dict.
    """
    from utils.prompts import build_campaign_followup_prompt, CAMPAIGN_OUTREACH_SYSTEM_PROMPT

    node_id = node.get("id", "n1")
    channel = node.get("channel", "email")
    subject_template = (node.get("subject_template") or "").strip()
    now = datetime.utcnow()

    linkedin_profile = enrollment.get("linkedin_profile_data") or {}
    company_data = enrollment.get("company_data") or {}
    company_profile = campaign.get("_company_profile")

    # Load company_profile if not already stamped (e.g. called from campaign_engine directly)
    if company_profile is None and campaign.get("account_id"):
        company_profile = await _load_company_profile(campaign["account_id"])

    user_prompt = build_campaign_followup_prompt(
        prospect=prospect,
        profile=linkedin_profile,
        company=company_data,
        campaign=campaign,
        node=node,
        prior_step_messages=prior_step_messages,
        company_profile=company_profile,
    )

    client = OpenRouterClient()
    try:
        result = await client.chat_completion(
            model=MESSAGE_GEN_PRIMARY_MODEL,
            messages=[
                {"role": "system", "content": CAMPAIGN_OUTREACH_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            max_tokens=1500,
            response_format={"type": "json_object"},
        )
        generated = result if isinstance(result, dict) else {}
        generated = _strip_em_dashes_from_messages(generated)
    except Exception as e:
        logger.error(f"generate_message_for_node AI call failed for node {node_id}: {e}")
        raise
    finally:
        await client.close()

    if channel == "email":
        email_data = generated.get("cold_email", {})
        subject = subject_template or email_data.get("subject_a", email_data.get("subject", ""))
        body = email_data.get("body", "")
        msg = {"channel": "email", "subject": subject, "body": body,
               "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
    elif channel == "linkedin_connection":
        note = generated.get("linkedin_connection", {}).get("note", "")
        msg = {"channel": "linkedin_connection", "body": _truncate_to_280(note),
               "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
    elif channel == "linkedin_inmail":
        inmail = generated.get("linkedin_inmail", {})
        msg = {"channel": "linkedin_inmail", "subject": inmail.get("subject", ""),
               "body": inmail.get("body", ""),
               "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
    else:
        msg = {"channel": channel, "body": generated.get("body", ""),
               "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}

    set_ops: dict = {
        f"generated_messages_by_step.{node_id}": msg,
        f"message_gen_status_by_step.{node_id}": "done",
    }

    # Also populate legacy flat generated_messages for n1 (backward compat with approve-day + edit-messages UI)
    if node_id == "n1":
        if channel == "email":
            legacy = {"channel": "email",
                      "cold_email": {"subject_a": msg["subject"], "body": msg["body"]},
                      "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
        elif channel == "linkedin_connection":
            legacy = {"channel": "linkedin_connection",
                      "linkedin_connection": {"note": msg["body"]},
                      "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
        elif channel == "linkedin_inmail":
            legacy = {"channel": "linkedin_inmail",
                      "linkedin_inmail": {"subject": msg.get("subject", ""), "body": msg["body"]},
                      "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
        else:
            legacy = {"channel": channel, "body": msg["body"],
                      "generated_at": now.isoformat(), "generation_model": MESSAGE_GEN_PRIMARY_MODEL}
        set_ops["generated_messages"] = legacy

    await database.campaign_enrollments_collection.update_one(
        {"_id": enrollment["_id"]},
        {"$set": set_ops},
    )

    logger.info(
        f"Generated message for enrollment {enrollment['_id']} node={node_id} channel={channel}"
    )
    return msg


def _truncate_to_280(text: str) -> str:
    """
    Ensure text is <= 280 characters.
    Truncates at the last complete word before the limit.
    """
    if len(text) <= 280:
        return text

    # Truncate at last word boundary before 280
    truncated = text[:280]
    last_space = truncated.rfind(" ")
    if last_space > 200:  # Don't truncate too aggressively
        truncated = truncated[:last_space]

    # Ensure it ends with punctuation
    truncated = truncated.rstrip()
    if truncated and truncated[-1] not in ".!?":
        truncated = truncated.rstrip(",;:") + "."

    return truncated


def _strip_em_dashes(text: str) -> str:
    """Replace em dashes with a comma+space."""
    if not text:
        return text
    import re
    return re.sub(r'\s*—\s*', ', ', text)


def _strip_em_dashes_from_messages(data) -> dict:
    """Recursively strip em dashes from all string values in a messages dict."""
    if isinstance(data, dict):
        return {k: _strip_em_dashes_from_messages(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_strip_em_dashes_from_messages(item) for item in data]
    if isinstance(data, str):
        return _strip_em_dashes(data)
    return data
