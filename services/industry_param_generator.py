"""
AI-powered industry parameter generation service.
Converts an industry name into structured Apify parameters (excluding location fields).
AI only selects the matching LinkedIn industry and optional keywords — everything else is fixed defaults.
"""

import json
import logging
from services.openrouter_service import OpenRouterClient, get_free_model
from models.industry import ApifyBaseParams

logger = logging.getLogger(__name__)

# Fixed defaults for all industries
DEFAULT_SENIORITY = ["c_suite", "owner", "founder", "director", "partner", "vp", "manager"]
DEFAULT_EMAIL_STATUS = ["validated"]

# Complete list of valid LinkedIn industry categories for the Apify actor
VALID_INDUSTRIES = [
    "packaging & containers", "information technology & services", "construction",
    "computer software", "marketing & advertising", "real estate",
    "health, wellness & fitness", "management consulting", "internet", "retail",
    "financial services", "consumer services", "hospital & health care", "automotive",
    "restaurants", "education management", "food & beverages", "design", "hospitality",
    "accounting", "events services", "nonprofit organization management", "entertainment",
    "electrical/electronic manufacturing", "leisure, travel & tourism",
    "professional training & coaching", "transportation/trucking/railroad", "law practice",
    "apparel & fashion", "architecture & planning", "mechanical or industrial engineering",
    "insurance", "telecommunications", "human resources", "staffing & recruiting", "sports",
    "oil & energy", "legal services", "media production", "machinery", "wholesale",
    "consumer goods", "music", "photography", "medical practice",
    "business supplies & equipment", "facilities services", "publishing", "food production",
    "arts & crafts", "building materials", "civil engineering", "religious institutions",
    "renewables & environment", "public relations & communications", "higher education",
    "graphic design", "printing", "furniture", "mining & metals",
    "logistics & supply chain", "research", "pharmaceuticals",
    "individual & family services", "medical devices", "environmental services",
    "civic & social organization", "e-learning", "security & investigations", "cosmetics",
    "chemicals", "government administration", "online media", "investment management",
    "farming", "writing & editing", "textiles", "mental health care", "broadcast media",
    "biotechnology", "information services", "international trade & development",
    "primary/secondary education", "motion pictures & film", "consumer electronics",
    "banking", "import & export", "industrial automation",
    "recreational facilities & services", "utilities", "sporting goods", "fine art",
    "airlines/aviation", "performing arts", "computer & network security", "maritime",
    "luxury goods & jewelry", "venture capital & private equity", "wine & spirits",
    "plastics", "aviation & aerospace", "veterinary", "commercial real estate",
    "computer games", "executive office", "computer networking", "market research",
    "outsourcing/offshoring", "program development", "computer hardware",
    "translation & localization", "philanthropy", "public safety",
    "alternative medicine", "museums & institutions", "warehousing", "defense & space",
    "newspapers", "paper & forest products", "law enforcement", "investment banking",
    "fund-raising", "think tanks", "glass, ceramics & concrete", "capital markets",
    "government relations", "semiconductors", "animation", "political organization",
    "package/freight delivery", "wireless", "international affairs", "public policy",
    "libraries", "dairy", "supermarkets", "fishery", "military", "ranching",
    "railroad manufacture", "gambling & casinos", "tobacco", "shipbuilding", "judiciary",
    "alternative dispute resolution", "nanotechnology", "agriculture",
    "legislative office",
    "ecommerce", "d2c", "direct-to-consumer",
]

SYSTEM_PROMPT = """You are an expert at mapping business/industry names to LinkedIn industry categories.

Given an industry or business type, you must:
1. Select the most relevant LinkedIn industry categories from the EXACT list provided below.
2. Optionally suggest company_keywords to narrow results for niche industries.
3. Generate a concise description (1-2 sentences) of the industry and target prospects.

## Valid LinkedIn Industry Categories (you MUST only pick from this list):

""" + "\n".join(f"- {ind}" for ind in VALID_INDUSTRIES) + """

## Rules

- Select 1-3 industries that best match the input. Pick the CLOSEST match(es) from the list above.
- ONLY use exact values from the list above for company_industry. Do NOT invent new industry names.
- Add company_keywords ONLY if the input is a niche/sub-industry that needs further filtering (e.g., "can making" -> keywords: ["cans"]). For broad industries (e.g., "construction"), omit keywords entirely.
- Keep keywords short and specific (1-2 words each, max 3 keywords).

## Required JSON Output Format

Return ONLY valid JSON:

{
  "description": "A concise description of the industry and target prospects.",
  "company_industry": ["<exact industry from list>"],
  "company_keywords": ["<keyword>"],
  "functional": ["<Lead Scraper department>"],
  "functional_level": ["<LEADS_FINDER department>"],
  "contact_city": ["<city lowercase>"],
  "min_revenue": "<e.g. 1M>",
  "max_revenue": "<e.g. 100M>",
  "funding": ["<funding stage>"]
}

If no keywords are needed, omit the company_keywords field entirely.
Only include "functional", "functional_level", "contact_city", "min_revenue", "max_revenue", "funding" when clearly relevant from the input.

## Additional field rules

- "functional": list of department names from Lead Scraper whitelist (Title-Case). Only include if clearly relevant. Examples: ["Marketing"] for marketing roles, ["Sales"] for sales roles, ["Engineering"] for tech roles.
- "functional_level": list of LEADS_FINDER department names (lowercase). Map from "functional": Marketing→marketing, Engineering→engineering, Product Management→product, Customer Service→support, HR→hr, Finance→finance, IT→it, Legal→legal, Operations→operations, Sales→sales, Design→design, C-Level→c-level.
- "contact_city": list of city names (lowercase) when user specifies a city rather than a country/state. If user says "United States" or "California" use contact_location instead.
- "min_revenue": optional revenue floor string in format "100K", "1M", "10M", "100M", "1B". Only include if user specifies revenue/company size by revenue.
- "max_revenue": optional revenue ceiling string in same format.
- "funding": list of funding stages. Valid values: "Seed", "Angel", "Series A", "Series B", "Series C", "Series D", "Series E", "Series F", "Venture", "Debt", "Convertible", "PE", "Other". Only include if user specifies funding stage.
"""


async def generate_industry_params(
    industry_name: str,
    client: OpenRouterClient,
) -> tuple[dict, str]:
    """
    Generate Apify parameters and description from an industry name.

    AI selects the matching LinkedIn industry and optional keywords.
    Seniority, email_status, and other params use fixed defaults.

    Returns:
        tuple of (params_dict, description_str)

    Raises:
        ValueError: If AI response cannot be validated
    """

    user_prompt = f'Industry/Business: "{industry_name}"\n\nSelect the matching LinkedIn industry categories and optional keywords. Return JSON only.'

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        logger.info(f"Generating industry params for: {industry_name}")

        response = await client.chat_completion(
            messages=messages,
            model=get_free_model(0),
            temperature=0.2,
            max_tokens=512,
        )

        # Extract content and parse JSON
        if isinstance(response, dict) and "content" in response:
            content = response["content"]
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            result = json.loads(content.strip())
        elif isinstance(response, dict):
            result = response
        else:
            raise ValueError(f"Unexpected response type: {type(response)}")

        description = result.get("description", f"{industry_name} industry prospects")
        company_industry = result.get("company_industry", [])
        company_keywords = result.get("company_keywords")

        # Validate that selected industries are from the allowed list
        valid_set = {ind.lower() for ind in VALID_INDUSTRIES}
        validated_industries = [ind for ind in company_industry if ind.lower() in valid_set]
        if not validated_industries:
            raise ValueError(f"AI returned no valid industries from the allowed list: {company_industry}")

        # Build params with fixed defaults
        params = {
            "seniority_level": DEFAULT_SENIORITY,
            "email_status": DEFAULT_EMAIL_STATUS,
            "company_industry": validated_industries,
        }

        if company_keywords and isinstance(company_keywords, list):
            clean_keywords = [kw.strip() for kw in company_keywords if kw.strip()]
            if clean_keywords:
                params["company_keywords"] = clean_keywords

        # Pass through new optional fields directly — ApifyBaseParams doesn't
        # define them so we attach them post-validation as raw extras.
        extra_params: dict = {}
        if result.get("functional") and isinstance(result["functional"], list):
            extra_params["functional"] = result["functional"]
        if result.get("functional_level") and isinstance(result["functional_level"], list):
            extra_params["functional_level"] = result["functional_level"]
        if result.get("contact_city") and isinstance(result["contact_city"], list):
            extra_params["contact_city"] = [c.lower().strip() for c in result["contact_city"] if c.strip()]
        if result.get("min_revenue") and isinstance(result["min_revenue"], str):
            extra_params["min_revenue"] = result["min_revenue"]
        if result.get("max_revenue") and isinstance(result["max_revenue"], str):
            extra_params["max_revenue"] = result["max_revenue"]
        if result.get("funding") and isinstance(result["funding"], list):
            extra_params["funding"] = result["funding"]

        # Validate against ApifyBaseParams schema
        validated = ApifyBaseParams(**params)

        logger.info(
            f"Generated params for '{industry_name}': "
            f"industries={validated_industries}, keywords={company_keywords or 'none'}"
        )

        apify_params = {**validated.model_dump(exclude_none=True), **extra_params}
        return apify_params, description

    except Exception as e:
        logger.error(f"Failed to generate industry params: {e}", exc_info=True)
        raise ValueError(f"Failed to generate valid Apify parameters: {str(e)}")
