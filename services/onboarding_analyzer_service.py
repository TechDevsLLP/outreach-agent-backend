"""
Onboarding company analysis service.
Orchestrates website scraping + OpenRouter AI analysis via a background job system.

Job lifecycle: pending → scraping → analyzing → done | error
Jobs are stored in-memory (short-lived, <60s). Not persisted across restarts.
"""

import asyncio
import logging
import uuid

from config import get_settings
from services.openrouter_service import OpenRouterClient
from services.website_scraper_service import scrape_website

logger = logging.getLogger(__name__)
settings = get_settings()

# ---------------------------------------------------------------------------
# In-memory job store
# { job_id: { status, progress, result, error } }
# ---------------------------------------------------------------------------
_jobs: dict[str, dict] = {}
_openrouter = OpenRouterClient()


def create_job() -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "status": "pending",
        "progress": 0,
        "result": None,
        "error": None,
    }
    return job_id


def get_job(job_id: str) -> dict | None:
    return _jobs.get(job_id)


# ---------------------------------------------------------------------------
# Background analysis coroutine
# ---------------------------------------------------------------------------

async def run_analysis(
    job_id: str,
    company_name: str,
    url: str,
    description: str,
) -> None:
    """
    Background coroutine started via FastAPI BackgroundTasks.
    Progresses the job through scraping → analyzing → done/error.
    """
    try:
        # Phase 1: scraping
        _jobs[job_id]["status"] = "scraping"
        _jobs[job_id]["progress"] = 10

        scraped_content = await scrape_website(url) if url else ""
        logger.info(
            f"[{job_id}] Scraped {len(scraped_content)} chars for '{company_name}'"
        )

        _jobs[job_id]["progress"] = 60

        # Phase 2: AI analysis
        _jobs[job_id]["status"] = "analyzing"

        result = await analyze_company(
            company_name=company_name,
            url=url,
            description=description,
            scraped_content=scraped_content,
        )

        _jobs[job_id]["progress"] = 95
        _jobs[job_id]["result"] = result
        _jobs[job_id]["status"] = "done"
        _jobs[job_id]["progress"] = 100

        logger.info(f"[{job_id}] Analysis complete for '{company_name}'")

    except Exception as e:
        logger.error(f"[{job_id}] Analysis failed: {e}", exc_info=True)
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = str(e)


# ---------------------------------------------------------------------------
# AI analysis
# ---------------------------------------------------------------------------

async def analyze_company(
    company_name: str,
    url: str,
    description: str,
    scraped_content: str,
) -> dict:
    """
    Calls OpenRouter in JSON mode to identify target market, industries, services, etc.
    Returns validated dict with all required keys.
    """
    system_prompt = _build_system_prompt()
    user_prompt = _build_user_prompt(company_name, url, description, scraped_content)

    result = await _openrouter.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=settings.claude_model,
        temperature=0.3,
        max_tokens=2048,
        response_format={"type": "json_object"},
    )

    return _validate_result(result)


def _build_system_prompt() -> str:
    return (
        "You are an expert B2B go-to-market analyst. Your job is to analyze a company "
        "and extract structured intelligence about who they serve and how they create value.\n\n"
        "You MUST respond with a valid JSON object and nothing else. No markdown, no explanation — only JSON.\n\n"
        "The JSON must have exactly these keys:\n"
        '- "target_market": string — 1-2 sentence description of their ideal customers '
        "(company size, industry, persona/role)\n"
        '- "services": array of strings — distinct products or services they offer (3-7 items)\n'
        '- "industries": array of strings — specific verticals or industries they serve '
        '(3-8 items, e.g. "Manufacturing", "SaaS", "Healthcare")\n'
        '- "differentiators": array of strings — what makes them unique vs alternatives (2-5 items)\n'
        '- "pain_points": array of strings — customer problems they solve, written from the '
        'customer\'s perspective (3-6 items, e.g. "Slow sales cycles costing revenue")\n'
        '- "icp_description": string — a compact paragraph describing the ideal customer profile '
        "for use in AI prospecting\n"
        '- "sender_role": string — the likely job title/role of the person doing outreach '
        "(infer from About/Team page, scraped meta, or company context; e.g. "
        '"Head of Growth", "Account Executive", "Founder"; return empty string if unclear)\n'
        '- "company_name": string — the company\'s actual name as branded on the site '
        "(use the provided name if given, else extract from the website)\n"
        '- "description": string — 1-2 plain sentences describing what the company does '
        "(their own elevator pitch, not who they sell to)\n"
        '- "value_propositions": array of strings — the concrete value/outcomes customers get '
        "(2-5 items, outcome-oriented, e.g. \"32% average sales lift within 2 months\")\n"
        '- "company_industries": array of strings — the industry/category the company ITSELF '
        'operates in (1-3 items, e.g. "Marketing & Advertising" for an ad agency — distinct '
        'from "industries", which is who they SELL to)\n'
        '- "case_studies": array of objects — up to 3 client success stories found in '
        '"Case Studies", "Customers", "Results", or testimonial sections of the website. '
        "Each object must have: "
        '"client" (company or person name), "outcome" (what result they achieved), '
        '"metric" (quantified result if available, else null), '
        '"industry" (client\'s industry if determinable, else null). '
        "Return empty array [] if no case studies are found.\n\n"
        "Infer intelligently from available content. If uncertain, make educated guesses based on "
        "the company name, URL, and content. Never return empty arrays for the list fields "
        "(except case_studies). JSON only."
    )


def _build_user_prompt(
    company_name: str,
    url: str,
    description: str,
    scraped_content: str,
) -> str:
    parts = [f"Company Name: {company_name}"]
    if url:
        parts.append(f"Website/LinkedIn URL: {url}")
    if description:
        parts.append(f"Company Description (provided by user):\n{description}")
    if scraped_content:
        parts.append(f"Scraped Website Content:\n---\n{scraped_content}\n---")
    else:
        parts.append(
            "(No website content could be scraped — infer from name and description only)"
        )
    parts.append("\nAnalyze this company and return the JSON object as instructed.")
    return "\n\n".join(parts)


def _validate_result(raw: dict) -> dict:
    """Ensure all required keys exist with correct types."""
    defaults: dict = {
        "target_market": "",
        "services": [],
        "industries": [],
        "differentiators": [],
        "pain_points": [],
        "icp_description": "",
        "sender_role": "",
        "case_studies": [],
        "company_name": "",
        "description": "",
        "value_propositions": [],
        "company_industries": [],
    }
    result = {**defaults, **raw}
    # Coerce any string values to single-item lists for list fields
    for key in ("services", "industries", "differentiators", "pain_points",
                "value_propositions", "company_industries"):
        if isinstance(result[key], str):
            result[key] = [result[key]] if result[key] else []
        elif not isinstance(result[key], list):
            result[key] = []
    # Validate case_studies: must be list of dicts
    if not isinstance(result["case_studies"], list):
        result["case_studies"] = []
    else:
        valid_cs = []
        for cs in result["case_studies"]:
            if isinstance(cs, dict) and cs.get("client"):
                valid_cs.append({
                    "client": str(cs.get("client") or ""),
                    "outcome": str(cs.get("outcome") or ""),
                    "metric": cs.get("metric"),
                    "industry": cs.get("industry"),
                })
        result["case_studies"] = valid_cs
    # Coerce sender_role / description / company_name to string
    for key in ("sender_role", "description", "company_name"):
        if not isinstance(result[key], str):
            result[key] = str(result[key]) if result[key] else ""
    return result
