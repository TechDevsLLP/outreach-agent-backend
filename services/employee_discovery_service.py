"""
Employee Discovery Service.
Discovers the best contacts at a target company using AI ranking.
Integrates with existing employee scraper and OpenRouter for AI-powered ranking.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional
from bson import ObjectId

from config import get_settings
from database import (
    prospects_collection,
    employees_collection,
    companies_collection,
    enrichment_runs_collection,
)
from services.openrouter_service import OpenRouterClient
from services.employee_scraper_service import _scrape_employees_for_company
from utils.prompts import get_system_prompt

logger = logging.getLogger(__name__)

settings = get_settings()

MIN_EMPLOYEES_THRESHOLD = 5


async def discover_best_contacts(
    prospect_id: Optional[str] = None,
    company_domain: Optional[str] = None,
    company_linkedin_url: Optional[str] = None,
    max_contacts: int = 3,
    auto_enrich: bool = True,
    industry_id: Optional[str] = None,
) -> dict:
    """
    Discover the best contacts at a target company.

    1. Resolve company from input (prospect_id, company_domain, or company_linkedin_url)
    2. Check existing employees. If < 5, scrape via Apify
    3. Send employee list to AI for ranking
    4. Create prospect records for top contacts
    5. Optionally auto-trigger enrichment

    Args:
        prospect_id: Source prospect ID to discover contacts from
        company_domain: Company domain to search
        company_linkedin_url: Company LinkedIn URL to search
        max_contacts: Maximum contacts to return (default 3)
        auto_enrich: Whether to auto-trigger enrichment for new prospects
        industry_id: Industry ID for new prospect records

    Returns:
        Dict with company info, employees found, and ranked top contacts
    """
    # 1. Resolve company information
    company_name = None
    company_url = None
    source_prospect = None

    if prospect_id:
        source_prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
        if not source_prospect:
            raise ValueError(f"Prospect not found: {prospect_id}")
        company_name = source_prospect.get("company_name")
        company_url = source_prospect.get("company_linkedin") or source_prospect.get("company_linkedin_uid")
        company_domain = company_domain or source_prospect.get("company_domain")
        industry_id = industry_id or source_prospect.get("industry_id")

        # Try to build LinkedIn URL from UID
        if not company_url and source_prospect.get("company_linkedin_uid"):
            company_url = f"https://www.linkedin.com/company/{source_prospect['company_linkedin_uid']}"

    if company_linkedin_url:
        company_url = company_linkedin_url

    if not company_url and not company_domain:
        raise ValueError("Must provide prospect_id, company_domain, or company_linkedin_url")

    # 2. Check existing employees in DB
    lookup_query = {}
    if company_url:
        # Normalize the URL for lookup
        normalized_url = company_url.rstrip("/")
        lookup_query = {
            "$or": [
                {"current_position.company_linkedin_url": {"$regex": normalized_url.split("/company/")[-1] if "/company/" in normalized_url else normalized_url, "$options": "i"}},
            ]
        }
        # Also try company collection
        company_doc = await companies_collection.find_one({"linkedin_url": {"$regex": normalized_url.split("/company/")[-1] if "/company/" in normalized_url else normalized_url, "$options": "i"}})
        if company_doc:
            company_name = company_name or company_doc.get("name")
            lookup_query = {"company_id": str(company_doc["_id"])}

    if company_domain and not lookup_query:
        lookup_query = {"company_name": {"$regex": company_domain.split(".")[0], "$options": "i"}}

    existing_employees = []
    if lookup_query:
        existing_employees = await employees_collection.find(lookup_query).to_list(100)

    logger.info(f"Found {len(existing_employees)} existing employees for {company_name or company_url}")

    # If fewer than threshold, scrape more
    if len(existing_employees) < MIN_EMPLOYEES_THRESHOLD and company_url:
        logger.info(f"Scraping employees for {company_url} (below threshold of {MIN_EMPLOYEES_THRESHOLD})")
        try:
            scraped = await asyncio.to_thread(
                _scrape_employees_for_company,
                company_url,
                max_items=50,
            )
            if scraped:
                # Re-fetch from DB after scraping
                if lookup_query:
                    existing_employees = await employees_collection.find(lookup_query).to_list(100)
        except Exception as e:
            logger.error(f"Employee scraping failed for {company_url}: {e}")

    if not existing_employees:
        return {
            "company_name": company_name,
            "company_url": company_url,
            "employees_found": 0,
            "top_contacts": [],
            "message": "No employees found. Try providing a company LinkedIn URL.",
        }

    # 3. AI ranking
    client = OpenRouterClient()
    try:
        employee_list = _format_employees_for_ai(existing_employees)

        user_prompt = (
            f"## Company: {company_name or 'Unknown'}\n\n"
            f"## Employees ({len(existing_employees)} found)\n\n"
            f"{employee_list}\n\n"
            f"Rank the top {max_contacts} contacts for our outreach. "
            f"Return as JSON."
        )

        response = await client.chat_completion(
            model=settings.claude_model,
            system_prompt=await get_system_prompt("employee_ranking"),
            user_prompt=user_prompt,
            response_format="json",
        )

        ranked_contacts = response.get("ranked_contacts", [])[:max_contacts]

    except Exception as e:
        logger.error(f"AI ranking failed: {e}", exc_info=True)
        # Fallback: return employees sorted by seniority
        ranked_contacts = _fallback_ranking(existing_employees, max_contacts)
    finally:
        await client.close()

    # 4. Create prospect records for top contacts
    new_prospect_ids = []
    for contact in ranked_contacts:
        linkedin_url = contact.get("linkedin_url")
        if not linkedin_url:
            continue

        # Check if prospect already exists
        existing = await prospects_collection.find_one({"linkedin": linkedin_url})
        if existing:
            contact["prospect_id"] = str(existing["_id"])
            contact["already_exists"] = True
            continue

        # Create new prospect record
        prospect_data = {
            "full_name": contact.get("full_name"),
            "job_title": contact.get("title"),
            "linkedin": linkedin_url,
            "company_name": company_name,
            "company_linkedin": company_url,
            "company_domain": company_domain,
            "industry_id": industry_id,
            "source": "employee_discovery",
            "discovered_from_prospect_id": prospect_id,
            "discovery_reasoning": contact.get("reasoning"),
            "prospect_score": contact.get("confidence_score", 50),
            "score_breakdown": {"ai_confidence": contact.get("confidence_score", 50)},
            "tags": ["employee_discovery"],
            "status": "new",
            "enrichment_status": "not_started",
            "first_seen_at": datetime.utcnow(),
            "last_updated_at": datetime.utcnow(),
        }

        try:
            result = await prospects_collection.insert_one(prospect_data)
            new_prospect_id = str(result.inserted_id)
            contact["prospect_id"] = new_prospect_id
            contact["already_exists"] = False
            new_prospect_ids.append(new_prospect_id)
        except Exception as e:
            logger.warning(f"Failed to create prospect for {linkedin_url}: {e}")
            contact["prospect_id"] = None
            contact["already_exists"] = False

    # 5. Optionally auto-trigger enrichment
    auto_enrich_result = None
    if auto_enrich and new_prospect_ids:
        try:
            from services.enrichment_pipeline import run_enrichment_pipeline

            run_doc = {
                "status": "running",
                "trigger": "employee_discovery",
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
            result = await enrichment_runs_collection.insert_one(run_doc)
            run_id = str(result.inserted_id)

            options = {
                "skip_profile_scrape": False,
                "skip_company_scrape": False,
                "skip_ai_assessment": False,
                "skip_outreach": False,
            }

            asyncio.create_task(run_enrichment_pipeline(run_id, new_prospect_ids, options))

            auto_enrich_result = {
                "enrichment_run_id": run_id,
                "prospects_queued": len(new_prospect_ids),
            }
            logger.info(f"Auto-enrichment triggered for {len(new_prospect_ids)} discovered contacts")

        except Exception as e:
            logger.error(f"Auto-enrichment trigger failed: {e}")

    return {
        "company_name": company_name,
        "company_url": company_url,
        "employees_found": len(existing_employees),
        "top_contacts": ranked_contacts,
        "new_prospects_created": len(new_prospect_ids),
        "auto_enrich": auto_enrich_result,
    }


def _format_employees_for_ai(employees: list[dict]) -> str:
    """Format employee list for AI ranking prompt."""
    lines = []
    for i, emp in enumerate(employees, 1):
        name = emp.get("full_name") or f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        position = emp.get("current_position", {})
        title = position.get("title", "Unknown") if isinstance(position, dict) else "Unknown"
        linkedin_url = emp.get("linkedin_url", "N/A")
        location = emp.get("location", "Unknown")
        lines.append(f"{i}. {name} | {title} | {location} | {linkedin_url}")
    return "\n".join(lines)


def _fallback_ranking(employees: list[dict], max_contacts: int) -> list[dict]:
    """Simple fallback ranking by title seniority when AI fails."""
    priority_keywords = [
        "chief", "ceo", "cmo", "cto", "cfo", "coo", "president",
        "vp", "vice president", "director", "head of",
        "marketing", "business development", "sales",
    ]

    scored = []
    for emp in employees:
        position = emp.get("current_position", {})
        title = (position.get("title", "") if isinstance(position, dict) else "").lower()
        name = emp.get("full_name") or f"{emp.get('first_name', '')} {emp.get('last_name', '')}".strip()
        score = sum(10 for kw in priority_keywords if kw in title)
        scored.append({
            "rank": 0,
            "linkedin_url": emp.get("linkedin_url"),
            "full_name": name,
            "title": position.get("title", "Unknown") if isinstance(position, dict) else "Unknown",
            "reasoning": "Ranked by title seniority (AI unavailable)",
            "approach_angle": "Generic outreach based on seniority level",
            "confidence_score": min(80, score * 5 + 30),
            "_score": score,
        })

    scored.sort(key=lambda x: x["_score"], reverse=True)
    result = scored[:max_contacts]
    for i, contact in enumerate(result, 1):
        contact["rank"] = i
        del contact["_score"]
    return result
