"""
Employee Selection Service.
Allows manual selection of scraped employees, finds their emails,
creates prospect records, and triggers enrichment.
"""

import asyncio
import logging
from datetime import datetime

from bson import ObjectId

from database import (
    employees_collection,
    prospects_collection,
    companies_collection,
    enrichment_runs_collection,
)
from services.email_finder_service import find_emails_batch
from utils.scoring import score_prospect_v2


def _extract_city(location: str | None) -> str | None:
    if not location:
        return None
    parts = [p.strip() for p in location.split(",") if p.strip()]
    return parts[0] if parts else None


def _extract_country(location: str | None) -> str | None:
    if not location:
        return None
    parts = [p.strip() for p in location.split(",") if p.strip()]
    return parts[-1] if len(parts) >= 2 else None


def _infer_seniority_from_title(title: str | None) -> str | None:
    if not title:
        return None
    t = title.lower()
    if any(k in t for k in ("owner", "proprietor")):
        return "owner"
    if any(k in t for k in ("founder", "co-founder", "cofounder")):
        return "founder"
    if any(k in t for k in ("chief ", "ceo", "cto", "cfo", "coo", "cmo", "cro", "cco", "c-suite")):
        return "c_suite"
    if any(k in t for k in ("partner", "principal")):
        return "partner"
    if any(k in t for k in ("vice president", "vp ", "v.p.")):
        return "vp"
    if any(k in t for k in ("director", "head of", "managing director", "general manager")):
        return "director"
    if "manager" in t:
        return "manager"
    if "senior" in t or " sr " in t or t.startswith("sr "):
        return "senior"
    return None

logger = logging.getLogger(__name__)


async def select_and_enrich_employees(
    employee_ids: list[str],
    auto_enrich: bool = True,
    skip_email_finding: bool = False,
    skip_profile_scrape: bool = False,
    skip_company_scrape: bool = False,
    skip_ai_assessment: bool = False,
    skip_outreach: bool = False,
    industry_id: str | None = None,
    tags: list[str] | None = None,
    account_id: str | None = None,
) -> dict:
    """
    Select specific employees, find their emails, create prospects, and trigger enrichment.

    Flow:
    1. Validate — fetch employees by IDs
    2. Deduplicate — check linkedin_url against prospects_collection
    3. Find emails — call find_emails_batch()
    4. Create prospects — insert prospect docs with company data + email
    5. Mark employees — set enriched=True on employee docs
    6. Trigger enrichment — create enrichment run + launch pipeline
    """
    if tags is None:
        tags = ["employee_selection"]

    # 1. Validate — fetch employees by IDs
    not_found = []
    employees = []
    for eid in employee_ids:
        try:
            emp = await employees_collection.find_one({"_id": ObjectId(eid)})
        except Exception:
            not_found.append(eid)
            continue
        if not emp:
            not_found.append(eid)
        else:
            emp["_id"] = str(emp["_id"])
            employees.append(emp)

    # 2. Deduplicate — check each employee's linkedin_url against prospects
    results = []
    to_create = []  # employees that need prospect creation

    for emp in employees:
        linkedin_url = emp.get("linkedin_url")
        if not linkedin_url:
            results.append({
                "employee_id": emp["_id"],
                "full_name": emp.get("full_name"),
                "status": "skipped",
                "reason": "no_linkedin_url",
            })
            continue

        existing_prospect = await prospects_collection.find_one({"linkedin": linkedin_url})
        if existing_prospect:
            results.append({
                "employee_id": emp["_id"],
                "full_name": emp.get("full_name"),
                "status": "duplicate",
                "existing_prospect_id": str(existing_prospect["_id"]),
            })
        else:
            to_create.append(emp)

    # 3. Find emails (batch) for employees that need prospect creation
    email_results = {}
    if not skip_email_finding and to_create:
        urls_to_find = [emp["linkedin_url"] for emp in to_create]
        logger.info(f"Finding emails for {len(urls_to_find)} employees")
        email_results = await find_emails_batch(urls_to_find, concurrency=3)

    # Preload company data for all employees (deduplicated by company_id)
    company_ids = {emp.get("company_id") for emp in to_create if emp.get("company_id")}
    company_cache = {}
    for cid in company_ids:
        try:
            company_doc = await companies_collection.find_one({"_id": ObjectId(cid)})
            if company_doc:
                company_cache[cid] = company_doc
        except Exception:
            pass

    # 4. Create prospects + 5. Mark employees
    new_prospect_ids = []
    emails_found_count = 0

    for emp in to_create:
        linkedin_url = emp["linkedin_url"]
        company_id = emp.get("company_id")
        company_doc = company_cache.get(company_id, {})

        # Extract email from finder results
        email_data = email_results.get(linkedin_url)
        email = None
        if email_data:
            email = email_data.get("email") or email_data.get("emailAddress")

        email_found = email is not None
        if email_found:
            emails_found_count += 1

        position = emp.get("current_position", {}) or {}

        prospect_data = {
            "full_name": emp.get("full_name"),
            "job_title": position.get("title") if isinstance(position, dict) else None,
            "linkedin": linkedin_url,
            "company_name": emp.get("company_name") or company_doc.get("name"),
            "company_linkedin": company_doc.get("linkedin_url"),
            "company_domain": company_doc.get("domain"),
            "company_website": company_doc.get("website"),
            "company_size": company_doc.get("employee_count"),
            "company_founded_year": company_doc.get("founded_year"),
            "company_description": company_doc.get("description") or company_doc.get("tagline"),
            "industry": company_doc.get("industry"),
            "industry_id": industry_id,
            "headline": position.get("description") if isinstance(position, dict) else None,
            "seniority_level": _infer_seniority_from_title(
                position.get("title") if isinstance(position, dict) else None
            ),
            "city": _extract_city(emp.get("location")),
            "country": _extract_country(emp.get("location")),
            "source": "employee_selection",
            "tags": tags,
            "status": "new",
            "enrichment_status": "not_started",
            "first_seen_at": datetime.utcnow(),
            "last_updated_at": datetime.utcnow(),
        }
        if account_id:
            try:
                prospect_data["account_id"] = ObjectId(account_id)
            except Exception:
                pass

        # Only include email if found — avoids unique index collision on null
        if email:
            prospect_data["email"] = email

        # Add location if available
        if emp.get("location"):
            prospect_data["location"] = emp["location"]

        # Compute rule-based base score so the UI has a score immediately (before pipeline runs)
        base_score, base_breakdown = score_prospect_v2(prospect_data)
        prospect_data["prospect_score"] = base_score
        prospect_data["score_breakdown"] = base_breakdown

        try:
            insert_result = await prospects_collection.insert_one(prospect_data)
            prospect_id = str(insert_result.inserted_id)
            new_prospect_ids.append(prospect_id)

            results.append({
                "employee_id": emp["_id"],
                "full_name": emp.get("full_name"),
                "status": "created",
                "prospect_id": prospect_id,
                "email_found": email_found,
                "email": email,
            })

            # Mark employee as enriched
            enrichment_data = {
                "prospect_id": prospect_id,
                "enriched_at": datetime.utcnow(),
                "email_found": email_found,
            }
            if email:
                enrichment_data["email"] = email

            await employees_collection.update_one(
                {"_id": ObjectId(emp["_id"])},
                {"$set": {
                    "enriched": True,
                    "enrichment_data": enrichment_data,
                    "last_updated_at": datetime.utcnow(),
                }},
            )

        except Exception as e:
            logger.error(f"Failed to create prospect for {linkedin_url}: {e}")
            results.append({
                "employee_id": emp["_id"],
                "full_name": emp.get("full_name"),
                "status": "failed",
                "error": str(e),
            })

    # 6. Trigger enrichment
    enrichment_info = None
    if auto_enrich and new_prospect_ids:
        try:
            from services.enrichment_pipeline import run_enrichment_pipeline

            run_doc = {
                "status": "running",
                "trigger": "employee_selection",
                "total_prospects": len(new_prospect_ids),
                "prospects_processed": 0,
                "prospects_skipped": 0,
                "prospects_failed": 0,
                "profiles_scraped": 0,
                "companies_scraped": 0,
                "companies_deduplicated": 0,
                "ai_assessments_done": 0,
                "outreach_generated": 0,
                "prospect_ids": new_prospect_ids,
                "started_at": datetime.utcnow(),
                "completed_at": None,
                "current_step": "initializing",
                "error": None,
            }
            run_result = await enrichment_runs_collection.insert_one(run_doc)
            run_id = str(run_result.inserted_id)

            options = {
                "skip_profile_scrape": skip_profile_scrape,
                "skip_company_scrape": skip_company_scrape,
                "skip_ai_assessment": skip_ai_assessment,
                "skip_outreach": skip_outreach,
                "skip_pre_enrichment_triage": True,
                "account_id": account_id,
            }

            asyncio.create_task(run_enrichment_pipeline(run_id, new_prospect_ids, options))

            enrichment_info = {
                "enrichment_run_id": run_id,
                "prospects_queued": len(new_prospect_ids),
            }
            logger.info(f"Enrichment triggered for {len(new_prospect_ids)} selected employees")

        except Exception as e:
            logger.error(f"Enrichment trigger failed: {e}")

    # Build summary
    status_counts = {}
    for r in results:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    return {
        "total_requested": len(employee_ids),
        "not_found": not_found,
        "results": results,
        "summary": {
            "created": status_counts.get("created", 0),
            "duplicates": status_counts.get("duplicate", 0),
            "skipped": status_counts.get("skipped", 0),
            "failed": status_counts.get("failed", 0),
            "emails_found": emails_found_count,
        },
        "enrichment": enrichment_info,
    }
