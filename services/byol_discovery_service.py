"""
Upload-a-Lead-List (BYOL) discovery orchestration.

`run_byol_discovery(campaign_id, account_id, generation=1)` replaces ONLY the
*sourcing* stages of the smart-campaign pipeline (Gemini company sourcing +
per-company Apify scraping) with a user-supplied spreadsheet, and reuses
everything from the person-fit gate onward:

  * per-lead scoring        → campaign_scoring_service.compute_campaign_score
  * pool upsert + dedupe    → curated_discovery_service._upsert_curated_prospect
  * pre-enroll              → prospect_enrollment_service._pre_enroll_prospects
  * channel planning        → curated_discovery_service.finalize_channel_plan
  * Day-1 enrich + msg gen  → curated_discovery_service._enqueue_day1_enrichment_and_messages

Row handling (classified per-row by lead_mapping_service.classify_row):
  * company            → Apify employee scrape → ICP person-fit gate → cap N/company
  * person + linkedin  → prospect doc directly (full channel)
  * person, email-only → GrowthToolkit email finder (or a mapped email) → email channel only
  * unresolvable       → surfaced in campaign.upload_unresolvable_rows (never enrolled)

Guard skips (cross-campaign double-contact, teammate cooldown, no eligible channel)
are captured into campaign.upload_skipped_rows with reasons — never silently dropped.

Dispatched from the same durable job type as curated discovery (see
enrichment_job_service). Never spends paid credits when campaign.discovery_mock_mode
is set and app_env != production.
"""

import asyncio
import logging
from datetime import datetime

from bson import ObjectId

import database
from config import get_settings
from services.lead_mapping_service import (
    build_lead_fields,
    classify_row,
    company_domain_for_row,
)

logger = logging.getLogger(__name__)
settings = get_settings()

# Reuse the same scoring version + cohort convention as curated discovery.
from services.campaign_prospect_state_service import (  # noqa: E402
    DEFAULT_SCORING_VERSION,
    ensure_cohort_membership,
    persist_campaign_scores,
    score_update_operation,
)

# Per-company enrollment cap for scraped company rows (~2–3).
_DEFAULT_PER_COMPANY_CAP = 3
# Apify employee scrape depth per company (before the ICP gate + cap).
_DEFAULT_SCRAPE_DEPTH = 8


def _account_values(account_id) -> list:
    values: list = [str(account_id)]
    if ObjectId.is_valid(str(account_id)):
        values.append(ObjectId(str(account_id)))
    return values


def _lead_to_prospect_doc(fields: dict, *, channel_eligibility: str, row_index: int) -> dict:
    """Build a prospect doc from mapped lead fields (shape mirrors
    employee_scraper_service.transform_employee_to_prospect output)."""
    location = None
    if fields.get("country"):
        location = {"raw": fields["country"], "country_code": None}
    return {
        "first_name": fields.get("first_name"),
        "last_name": fields.get("last_name"),
        "full_name": fields.get("full_name"),
        "email": fields.get("email"),
        "linkedin": fields.get("linkedin"),
        "job_title": fields.get("job_title"),
        "seniority": fields.get("seniority"),
        "location": location,
        "company_name": fields.get("company_name"),
        "company_linkedin": fields.get("company_linkedin"),
        "company_domain": fields.get("company_domain"),
        "source": "lead_upload",
        "stage": "contactable",
        "enrichment_status": "not_started",
        # BYOL-only, tenant/campaign-scoped — carried through pre-enroll onto the
        # enrollment doc, NOT persisted on the shared prospect pool.
        "channel_eligibility": channel_eligibility,
        "upload_row_index": row_index,
    }


async def run_byol_discovery(campaign_id: str, account_id: str, generation: int = 1) -> dict:
    campaign_oid = ObjectId(campaign_id)
    now = datetime.utcnow()
    acct_values = _account_values(account_id)

    campaign = await database.campaigns_collection.find_one(
        {"_id": campaign_oid, "account_id": {"$in": acct_values}}
    )
    if not campaign:
        raise PermissionError("campaign is not owned by discovery tenant")

    batch_id = campaign.get("upload_batch_id")
    if not batch_id or not ObjectId.is_valid(str(batch_id)):
        await _fail(campaign_oid, "No lead upload batch is attached to this campaign")
        raise ValueError("byol discovery: missing upload_batch_id")

    batch = await database.lead_upload_batches_collection.find_one(
        {"_id": ObjectId(str(batch_id)), "account_id": str(account_id)}
    )
    if not batch:
        await _fail(campaign_oid, "Lead upload batch not found for this account")
        raise ValueError("byol discovery: batch not found")

    mapping = batch.get("mapping")
    rows = batch.get("rows") or []
    if not mapping:
        await _fail(campaign_oid, "Lead upload mapping has not been confirmed")
        raise ValueError("byol discovery: mapping not confirmed")

    await database.campaigns_collection.update_one(
        {"_id": campaign_oid, "account_id": {"$in": acct_values}},
        {"$set": {
            "discovery_status": "running",
            "discovery_started_at": now,
            "discovery_error": None,
        }},
    )

    _is_mock = bool(campaign.get("discovery_mock_mode")) and settings.app_env != "production"
    if not _is_mock and settings.discovery_mock_mode and settings.app_env != "production":
        _is_mock = True

    try:
        # ── 1. Classify + bucket rows ───────────────────────────────────────
        full_channel_docs: list[dict] = []   # person + linkedin
        email_only_fields: list[tuple[dict, int]] = []  # (fields, row_index) needing email work
        company_rows: list[tuple[dict, int]] = []       # (fields, row_index) to scrape
        unresolvable: list[dict] = []                   # {row_index, raw_row, reason}

        rows_person = rows_company = rows_email_only = 0
        for idx, row in enumerate(rows):
            kind = classify_row(row, mapping)
            fields = build_lead_fields(row, mapping)
            if kind == "unresolvable":
                unresolvable.append({
                    "row_index": idx,
                    "raw_row": row,
                    "reason": "no_person_name_or_company_signal",
                })
                continue
            if kind == "company":
                if fields.get("company_linkedin"):
                    company_rows.append((fields, idx))
                    rows_company += 1
                else:
                    # A company row with only a name/domain but no LinkedIn URL can't
                    # be scraped by the employee actor.
                    unresolvable.append({
                        "row_index": idx,
                        "raw_row": row,
                        "reason": "company_without_linkedin_url",
                    })
                continue
            # person
            rows_person += 1
            if fields.get("linkedin"):
                full_channel_docs.append(
                    _lead_to_prospect_doc(fields, channel_eligibility="full", row_index=idx)
                )
            elif fields.get("email"):
                # Already have an email → email-only channel, no lookup needed.
                rows_email_only += 1
                full_channel_docs.append(
                    _lead_to_prospect_doc(fields, channel_eligibility="email_only", row_index=idx)
                )
            else:
                # Needs an email found from name + company domain.
                rows_email_only += 1
                email_only_fields.append((fields, idx))

        # ── 2. Company rows → scrape employees → ICP gate → cap ─────────────
        company_docs, company_skips = await _scrape_company_rows(
            company_rows, campaign, account_id, mock=_is_mock
        )

        # ── 3. Email-only rows (person, no LinkedIn) → resolve domain + find email
        email_warnings: list[str] = []
        email_docs, email_unresolvable = await _resolve_email_only(
            email_only_fields, rows, account_id, mock=_is_mock,
            credit_warnings=email_warnings,
        )
        unresolvable.extend(email_unresolvable)

        all_docs = full_channel_docs + company_docs + email_docs

        # ── 3b. Find emails for EVERY other lead missing one ────────────────
        # Full-channel (LinkedIn) leads and scraped company leads also need an
        # email for the email channel. Curated discovery finds emails for all
        # prospects; BYOL must too — otherwise a list full of LinkedIn URLs
        # (e.g. a YC founders sheet) never gets a single email. Runs BEFORE
        # channel planning so the email channel is eligible (risk #3 ordering).
        await _find_missing_emails(
            all_docs, account_id, mock=_is_mock, credit_warnings=email_warnings,
        )

        # ── 4. Upsert + score + pre-enroll ──────────────────────────────────
        enrolled_meta = await _persist_and_enroll(all_docs, campaign, account_id)

        # ── 5. Channel planning (email persisted before this — risk #3) ─────
        from services.curated_discovery_service import finalize_channel_plan
        plan_result = await finalize_channel_plan(campaign_id, account_id)
        total_assigned = plan_result.get("assigned", 0)

        # ── 6. Capture guard skips + no-eligible-channel with reasons ───────
        skipped_rows = await _capture_skips(enrolled_meta, campaign_oid)
        skipped_rows.extend(company_skips)

        total_enrolled = await database.campaign_enrollments_collection.count_documents({
            "campaign_id": campaign_oid,
            "status": {"$nin": ["skipped_no_channel", "archived", "pending_teammate_review", "cascade_waiting"]},
        })

        # ── 7. Email-only enrichment prep (risk #1) ─────────────────────────
        await prepopulate_email_only_intelligence(all_docs, enrolled_meta, campaign, account_id)

        # ── 8. Terminal status writes ───────────────────────────────────────
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid, "account_id": {"$in": acct_values}},
            {"$set": {
                "discovery_status": "completed",
                "discovery_completed_at": datetime.utcnow(),
                "status": "awaiting_approval",
                "approval_status": "pending",
                "discovery_source": "upload",
                "discovery_prospects_found": len(all_docs),
                "discovery_prospects_planned": total_assigned,
                "discovery_prospects_enrolled": total_assigned,
                "total_enrolled": total_enrolled,
                "upload_rows_total": len(rows),
                "upload_rows_person": rows_person,
                "upload_rows_company": rows_company,
                "upload_rows_email_only": rows_email_only,
                "upload_rows_unresolvable": len(unresolvable),
                "upload_unresolvable_rows": unresolvable,
                "upload_skipped_rows": skipped_rows,
                "discovery_warnings": sorted(set(email_warnings)),
                **({"approved_send_days": []} if generation == 1 else {}),
            }},
        )

        # ── 9. Day-1 enrichment + message generation ────────────────────────
        if total_assigned > 0:
            from services.curated_discovery_service import _enqueue_day1_enrichment_and_messages
            await _enqueue_day1_enrichment_and_messages(campaign_id, str(account_id))
        else:
            _zero = {
                "message_gen_status": "completed",
                "message_gen_completed_at": datetime.utcnow(),
            }
            if not all_docs:
                _zero["discovery_error"] = (
                    "No contactable leads could be built from the uploaded list. "
                    "Check the column mapping or the file contents."
                )
            await database.campaigns_collection.update_one(
                {"_id": campaign_oid}, {"$set": _zero}
            )

        logger.info(
            f"[byol:{campaign_id}] complete — rows={len(rows)} "
            f"(person={rows_person}, company={rows_company}, email_only={rows_email_only}, "
            f"unresolvable={len(unresolvable)}) built={len(all_docs)} assigned={total_assigned} "
            f"enrolled={total_enrolled} skipped={len(skipped_rows)}"
        )
        return {
            "campaign_id": campaign_id,
            "prospects_created": len(all_docs),
            "assigned": total_assigned,
            "enrolled": total_enrolled,
            "generation": generation,
        }

    except Exception as e:
        logger.exception(f"[byol:{campaign_id}] failed")
        await _fail(campaign_oid, str(e)[:500])
        raise


async def _fail(campaign_oid: ObjectId, reason: str) -> None:
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {"discovery_status": "failed", "discovery_error": reason}},
    )


# ---------------------------------------------------------------------------
# Company rows → employee scrape → ICP gate → per-company cap
# ---------------------------------------------------------------------------

async def _scrape_company_rows(
    company_rows: list[tuple[dict, int]],
    campaign: dict,
    account_id: str,
    *,
    mock: bool,
) -> tuple[list[dict], list[dict]]:
    """Return (prospect_docs, skipped_rows). Company rows that yield no contactable,
    ICP-matching employee are surfaced as skipped rows with a reason."""
    if not company_rows:
        return [], []

    campaign_id = str(campaign["_id"])
    per_company_cap = int(campaign.get("max_prospects_per_company") or _DEFAULT_PER_COMPANY_CAP)
    per_company_cap = max(1, min(per_company_cap, _DEFAULT_PER_COMPANY_CAP))
    scrape_depth = int(campaign.get("discovery_scrape_depth") or _DEFAULT_SCRAPE_DEPTH)

    # Map normalized company LinkedIn URL → (fields, row_index).
    url_to_row: dict[str, tuple[dict, int]] = {}
    urls: list[str] = []
    for fields, idx in company_rows:
        url = fields.get("company_linkedin")
        if not url:
            continue
        norm = url.rstrip("/").lower()
        url_to_row[norm] = (fields, idx)
        urls.append(url)

    docs: list[dict] = []
    per_company_docs: dict[str, list[dict]] = {u: [] for u in url_to_row}

    if mock:
        # Synthesize one prospect per company row without paid Apify calls.
        for norm, (fields, idx) in url_to_row.items():
            synth = dict(fields)
            synth.setdefault("first_name", "Mock")
            synth.setdefault("last_name", f"Lead{idx}")
            synth["full_name"] = f"{synth['first_name']} {synth['last_name']}"
            synth["linkedin"] = f"https://www.linkedin.com/in/mock-byol-{campaign_id}-{idx}"
            per_company_docs[norm].append(
                _lead_to_prospect_doc(synth, channel_eligibility="full", row_index=idx)
            )
    else:
        import math
        from services.employee_scraper_service import (
            bulk_scrape_employees_for_companies,
            transform_employee_to_prospect,
        )
        from services.curated_discovery_service import (
            _extract_company_url_from_employee,
        )
        from utils.scoring import person_fit_gate, score_prospect_for_campaign

        raw = await bulk_scrape_employees_for_companies(
            urls,
            max_items_per_company=scrape_depth,
            max_total_items=math.ceil(len(urls) * scrape_depth),
            account_id=str(account_id),
            campaign_id=campaign_id,
        )
        for emp in raw or []:
            co_url = _extract_company_url_from_employee(emp)
            if not co_url:
                continue
            match = url_to_row.get(co_url.rstrip("/").lower())
            if not match:
                continue
            fields, idx = match
            # Build a sourced-company-like dict for the transformer.
            sc = {
                "company_name": fields.get("company_name"),
                "company_linkedin_url": fields.get("company_linkedin"),
                "company_domain": fields.get("company_domain"),
            }
            prospect = transform_employee_to_prospect(emp, sc)
            # ICP person-fit gate (deterministic). Fail → drop this employee.
            ok, _reason = person_fit_gate(prospect, campaign)
            if not ok:
                continue
            prospect["fit_score"] = score_prospect_for_campaign(prospect, campaign)
            prospect["channel_eligibility"] = "full"
            prospect["upload_row_index"] = idx
            per_company_docs[co_url.rstrip("/").lower()].append(prospect)

    skipped: list[dict] = []
    for norm, (fields, idx) in url_to_row.items():
        cohort = sorted(
            per_company_docs.get(norm, []),
            key=lambda p: p.get("fit_score", 0),
            reverse=True,
        )[:per_company_cap]
        if cohort:
            docs.extend(cohort)
        else:
            skipped.append({
                "row_index": idx,
                "name": fields.get("company_name"),
                "company": fields.get("company_name"),
                "reason": "no_contactable_employees_matching_icp",
            })
    return docs, skipped


# ---------------------------------------------------------------------------
# Email-only rows → resolve domain + find email
# ---------------------------------------------------------------------------

async def _resolve_email_only(
    email_only_fields: list[tuple[dict, int]],
    rows: list[dict],
    account_id: str,
    *,
    mock: bool,
    credit_warnings: list[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (prospect_docs, unresolvable_rows). A row with no resolvable company
    domain, or where no email is found, becomes unresolvable."""
    if not email_only_fields:
        return [], []

    resolvable: list[tuple[dict, int, str]] = []  # (fields, idx, domain)
    unresolvable: list[dict] = []
    for fields, idx in email_only_fields:
        domain = company_domain_for_row(fields)
        if not domain or not (fields.get("first_name") and fields.get("last_name")):
            unresolvable.append({
                "row_index": idx,
                "raw_row": rows[idx] if idx < len(rows) else {},
                "reason": "no_company_domain_or_name_for_email_lookup",
            })
            continue
        resolvable.append((fields, idx, domain))

    if not resolvable:
        return [], unresolvable

    docs: list[dict] = []
    if mock:
        for fields, idx, domain in resolvable:
            fields = dict(fields)
            fields["email"] = f"{fields['first_name']}.{fields['last_name']}@{domain}".lower()
            docs.append(_lead_to_prospect_doc(fields, channel_eligibility="email_only", row_index=idx))
        return docs, unresolvable

    from services.email_finder_service import find_emails, EmailLookupEntry

    # key must be unique per lead — use the row index.
    entries = [
        EmailLookupEntry(
            first_name=fields["first_name"],
            last_name=fields["last_name"],
            domain=domain,
            key=str(idx),
        )
        for fields, idx, domain in resolvable
    ]
    try:
        found = await find_emails(
            entries, account_id=str(account_id), credit_warnings=credit_warnings
        )
    except Exception as e:
        logger.warning(f"[byol] email finder failed (email-only rows unresolvable): {e}")
        found = {}

    for fields, idx, domain in resolvable:
        email = found.get(str(idx))
        if email:
            fields = dict(fields)
            fields["email"] = email
            docs.append(_lead_to_prospect_doc(fields, channel_eligibility="email_only", row_index=idx))
        else:
            unresolvable.append({
                "row_index": idx,
                "raw_row": rows[idx] if idx < len(rows) else {},
                "reason": "email_not_found",
            })
    return docs, unresolvable


# ---------------------------------------------------------------------------
# Email lookup for ALL leads missing an email (full-channel + scraped company)
# ---------------------------------------------------------------------------

async def _find_missing_emails(
    docs: list[dict],
    account_id: str,
    *,
    mock: bool,
    credit_warnings: list[str] | None = None,
) -> None:
    """Fill in `email` for any prospect doc that lacks one, given first + last +
    a resolvable company domain. Mutates docs in place.

    Applies to full-channel (LinkedIn) leads and scraped company leads — email-only
    docs already carry an email and are skipped. This mirrors curated discovery,
    which runs the GrowthToolkit email finder for every prospect regardless of
    whether it has a LinkedIn URL.
    """
    from services.lead_mapping_service import normalize_domain

    targets: list[tuple[dict, str, str, str, int]] = []  # (doc, first, last, domain, key)
    for i, d in enumerate(docs):
        if d.get("email"):
            continue
        first = d.get("first_name")
        last = d.get("last_name")
        if not (first and last):
            parts = (d.get("full_name") or "").split()
            if len(parts) >= 2:
                first = first or parts[0]
                last = last or " ".join(parts[1:])
        domain = normalize_domain(d.get("company_domain"))
        if not (first and last and domain):
            continue
        targets.append((d, first, last, domain, i))

    if not targets:
        return

    if mock:
        for d, first, last, domain, _i in targets:
            d["email"] = f"{first}.{last}@{domain}".lower().replace(" ", "")
        return

    from services.email_finder_service import find_emails, EmailLookupEntry

    entries = [
        EmailLookupEntry(first_name=first, last_name=last, domain=domain, key=str(key))
        for (_d, first, last, domain, key) in targets
    ]
    try:
        found = await find_emails(
            entries, account_id=str(account_id), credit_warnings=credit_warnings
        )
    except Exception as e:
        logger.warning(f"[byol] email finder failed for full-channel/company leads: {e}")
        found = {}

    n = 0
    for d, _first, _last, _domain, key in targets:
        email = found.get(str(key))
        if email:
            d["email"] = email
            n += 1
    logger.info(
        f"[byol] email finder: filled {n}/{len(targets)} missing emails "
        f"(full-channel + company leads)"
    )


# ---------------------------------------------------------------------------
# Upsert + score + pre-enroll
# ---------------------------------------------------------------------------

async def _persist_and_enroll(docs: list[dict], campaign: dict, account_id: str) -> dict:
    """Upsert prospects, compute campaign fit scores, pre-enroll.

    Returns a mapping prospect_oid → {upload_row_index, name, company, channel_eligibility}
    for downstream skip capture.
    """
    if not docs:
        return {}

    from services.campaign_scoring_service import compute_campaign_score
    from services.curated_discovery_service import _upsert_curated_prospect
    from services.prospect_enrollment_service import _pre_enroll_prospects

    campaign_oid = campaign["_id"]
    scoring_version = campaign.get("scoring_version") or DEFAULT_SCORING_VERSION
    cohort_id = f"campaign:{str(campaign_oid)}:selected"

    # Upsert each doc (dedupe on linkedin else email). Concurrent — each keyed
    # independently. Rows with neither key already routed to unresolvable upstream.
    oids = await asyncio.gather(
        *[_upsert_curated_prospect(d, campaign_oid, account_id) for d in docs]
    )

    meta: dict = {}
    seen: set = set()
    by_oid_source: dict = {}
    unique_oids: list = []
    for doc, oid in zip(docs, oids):
        if not oid or oid in seen:
            continue
        seen.add(oid)
        unique_oids.append(oid)
        by_oid_source[oid] = doc
        meta[oid] = {
            "upload_row_index": doc.get("upload_row_index"),
            "name": doc.get("full_name"),
            "company": doc.get("company_name"),
            "channel_eligibility": doc.get("channel_eligibility", "full"),
        }

    if not unique_oids:
        return {}

    # Campaign fit score (display/sort only; BYOL never drops on score).
    score_ops = []
    for oid in unique_oids:
        source = by_oid_source[oid]
        result = compute_campaign_score(source, campaign)
        source["_campaign_fit_score"] = result["fit_score"]
        source["_campaign_priority_tier"] = result["priority_tier"]
        score_ops.append(score_update_operation(
            account_id=account_id,
            campaign_id=campaign_oid,
            prospect_id=oid,
            result=result,
            scoring_version=scoring_version,
            cohort_id=cohort_id,
            cohort_label="selected",
        ))
    await persist_campaign_scores(score_ops)
    await ensure_cohort_membership(
        account_id=account_id,
        campaign_id=campaign_oid,
        prospect_ids=unique_oids,
        cohort_id=cohort_id,
        cohort_label="selected",
        scoring_version=scoring_version,
    )

    # Reload persisted prospect docs and inject campaign-scoped fields for pre-enroll.
    chunk_full = await database.prospects_collection.find(
        {"_id": {"$in": unique_oids}}
    ).to_list(length=None)
    for persisted in chunk_full:
        source = by_oid_source.get(persisted["_id"]) or {}
        if source.get("_campaign_fit_score") is not None:
            persisted["_campaign_fit_score"] = source["_campaign_fit_score"]
        if source.get("_campaign_priority_tier") is not None:
            persisted["_campaign_priority_tier"] = source["_campaign_priority_tier"]
        # BYOL: carry channel eligibility + source row index onto the enrollment.
        persisted["channel_eligibility"] = source.get("channel_eligibility", "full")
        persisted["upload_row_index"] = source.get("upload_row_index")

    await _pre_enroll_prospects(campaign, chunk_full)
    return meta


# ---------------------------------------------------------------------------
# Guard-skip capture
# ---------------------------------------------------------------------------

async def _capture_skips(enrolled_meta: dict, campaign_oid: ObjectId) -> list[dict]:
    """Diff intended prospects against actual enrollments to surface every skip.

    - prospect absent from campaign_enrollments  → cross-campaign double-contact guard
    - status == pending_teammate_review          → teammate cooldown
    - status == skipped_no_channel               → no eligible channel
    """
    if not enrolled_meta:
        return []

    oids = list(enrolled_meta.keys())
    rows_by_oid: dict = {}
    async for enr in database.campaign_enrollments_collection.find(
        {"campaign_id": campaign_oid, "prospect_id": {"$in": oids}},
        {"prospect_id": 1, "status": 1},
    ):
        rows_by_oid[enr["prospect_id"]] = enr.get("status")

    skipped: list[dict] = []
    for oid, m in enrolled_meta.items():
        status = rows_by_oid.get(oid)
        reason = None
        if status is None:
            reason = "already_active_in_another_campaign"
        elif status == "pending_teammate_review":
            reason = "teammate_cooldown"
        elif status == "skipped_no_channel":
            reason = "no_eligible_channel"
        if reason:
            skipped.append({
                "row_index": m.get("upload_row_index"),
                "name": m.get("name"),
                "company": m.get("company"),
                "reason": reason,
            })
    return skipped


# ---------------------------------------------------------------------------
# Email-only enrichment prep (critical risk #1)
# ---------------------------------------------------------------------------

async def prepopulate_email_only_intelligence(
    docs: list[dict], enrolled_meta: dict, campaign: dict, account_id: str
) -> None:
    """Pre-populate a minimal `prospect_intelligence_base` for enrolled email-only
    leads (critical risk #1).

    The enrichment pipeline hard-gates on `linkedin` (enrichment_pipeline.py:197-208),
    so an email-only lead is marked "skipped" there and never gets base intelligence.
    Day-1 message generation still runs (message gen keys on message_gen_status, not
    enrichment status), so these leads are NOT dropped — but they would reach the
    generator with no context AND `backfill_missing_intelligence` would keep retrying
    a doomed LinkedIn enrichment on them.

    Writing a small deterministic base-intel dict here (a) gives the generator a real
    hook and (b) makes backfill skip them (it only enriches prospects missing
    `prospect_intelligence_base`). No paid calls.
    """
    email_only_oids = [
        oid for oid, m in (enrolled_meta or {}).items()
        if m.get("channel_eligibility") == "email_only"
    ]
    if not email_only_oids:
        return

    from services.prospect_intelligence_service import store_base_intelligence

    existing = await database.prospects_collection.find(
        {"_id": {"$in": email_only_oids}},
        {"job_title": 1, "company_name": 1, "first_name": 1, "full_name": 1,
         "prospect_intelligence_base": 1},
    ).to_list(length=None)

    oids: list = []
    intel_list: list[dict] = []
    for p in existing:
        if p.get("prospect_intelligence_base"):
            continue  # already has real/prior intelligence — don't overwrite
        title = p.get("job_title")
        company = p.get("company_name")
        name = p.get("first_name") or (p.get("full_name") or "").split(" ")[0] or "there"
        if title and company:
            hook = f"{name} is a {title} at {company} — anchor the opener on that role and company."
        elif company:
            hook = f"{name} works at {company} — anchor the opener on that company."
        else:
            hook = f"Personalize the opener for {name} using the uploaded list context."
        intel = {
            "best_hook": hook,
            "source": "byol_email_only_minimal",
        }
        oids.append(p["_id"])
        intel_list.append(intel)

    if oids:
        await store_base_intelligence(oids, intel_list)
        logger.info(
            f"[byol:{campaign.get('_id')}] pre-populated base intelligence for "
            f"{len(oids)} email-only leads"
        )
