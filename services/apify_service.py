"""
Apify actor integrations.
- Leads Finder (IoSHqwTR9YGhzccez): Primary prospect scraper
- Lead Scraper (T1XDXWc1L92AfIJtd): Apollo-style lead scraper (secondary source)
"""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apify_client import ApifyClient
from config import get_settings
import logging

logger = logging.getLogger(__name__)

settings = get_settings()
apify_client = ApifyClient(settings.apify_api_key)


# ── Primary Leads Finder actor ──────────────────────────────────────────────

def build_actor_input(apify_params: dict, fetch_count: int, start_page: int = 0) -> dict:
    """
    Build the Apify actor input from ICP profile params.
    Converts list params to the format the actor expects.

    Args:
        apify_params: ICP profile parameters
        fetch_count: Number of results to fetch
        start_page: Starting page for pagination (0-indexed)
    """
    run_input = {
        "fetch_count": fetch_count,
        "file_name": "Prospects",
        "email_status": ["validated", "not_validated"],
        "startPage": start_page,  # Phase 3: Pagination support
    }

    # Map profile params to actor input
    # Note: contact_location (person's home country) is intentionally excluded —
    # combining person + company country creates an AND condition that is too strict
    # and drastically reduces results for remote workers.  Company-level country
    # filtering is handled separately in build_lead_scraper_input via companyCountry.
    field_mappings = [
        "contact_job_title", "contact_not_job_title",
        "seniority_level", "functional_level",
        "contact_city",
        "contact_not_location", "contact_not_city",
        "company_domain", "size",
        "company_industry", "company_not_industry",
        "company_keywords", "company_not_keywords",
        "min_revenue", "max_revenue", "funding",
    ]

    # Normalize all list fields to lowercase — Apify requires lowercase for
    # seniority_level, contact_location, contact_city, etc.
    apify_params = {**apify_params}  # shallow copy to avoid mutating original

    _SENIORITY_MAP = {
        "entry": "entry", "training": "trainee", "trainee": "trainee",
        "senior": "senior", "manager": "manager", "director": "director",
        "vp": "vp", "c-suite": "c_suite", "c_suite": "c_suite",
        "csuite": "c_suite", "partner": "partner", "owner": "owner",
        "founder": "founder", "head": "head",
    }
    if isinstance(apify_params.get("seniority_level"), list):
        apify_params["seniority_level"] = [
            _SENIORITY_MAP.get(v.lower().strip(), v.lower().strip())
            for v in apify_params["seniority_level"]
        ]

    # Lowercase location fields — Apify rejects title-case locations
    for loc_field in ["contact_location", "contact_city", "contact_not_location", "contact_not_city"]:
        if isinstance(apify_params.get(loc_field), list):
            apify_params[loc_field] = [v.lower().strip() for v in apify_params[loc_field]]

    # Validate & normalize company_industry — Apify only accepts its exact allowed values.
    # Filter out any value that isn't in the valid set; map common aliases first.
    if isinstance(apify_params.get("company_industry"), list):
        from services.industry_param_generator import VALID_INDUSTRIES
        _VALID_INDUSTRY_SET = {v.lower(): v for v in VALID_INDUSTRIES}
        # Common user-entered aliases → canonical Apify value
        _INDUSTRY_ALIAS: dict[str, str] = {
            "saas": "computer software",
            "software": "computer software",
            "software as a service": "computer software",
            "tech": "information technology & services",
            "technology": "information technology & services",
            "it": "information technology & services",
            "it services": "information technology & services",
            "fintech": "financial services",
            "healthtech": "hospital & health care",
            "health tech": "hospital & health care",
            "healthcare": "hospital & health care",
            "health care": "hospital & health care",
            "ecommerce": "retail",
            "e-commerce": "retail",
            "d2c": "retail",
            "direct to consumer": "retail",
            "direct-to-consumer": "retail",
            "dtc": "retail",
            "online brand": "retail",
            "online retail": "retail",
            "online store": "retail",
            "consumer brand": "consumer goods",
            "cpg": "consumer goods",
            "fmcg": "consumer goods",
            "ai": "computer software",
            "artificial intelligence": "computer software",
            "cybersecurity": "computer & network security",
            "cyber security": "computer & network security",
            "hr tech": "human resources",
            "hrtech": "human resources",
            "edtech": "e-learning",
            "ed tech": "e-learning",
            "education": "education management",
            "legal": "law practice",
            "media": "media production",
            "digital marketing": "marketing & advertising",
            "consulting": "management consulting",
            "logistics": "logistics & supply chain",
            "supply chain": "logistics & supply chain",
            "biotech": "biotechnology",
            "pharma": "pharmaceuticals",
            "manufacturing": "machinery",
            "energy": "oil & energy",
            "clean energy": "renewables & environment",
            "cleantech": "renewables & environment",
            "real estate tech": "real estate",
            "proptech": "real estate",
            "insurance tech": "insurance",
            "insurtech": "insurance",
        }
        validated = []
        for ind in apify_params["company_industry"]:
            ind_lower = ind.lower().strip()
            # 1. Exact match
            if ind_lower in _VALID_INDUSTRY_SET:
                validated.append(_VALID_INDUSTRY_SET[ind_lower])
            # 2. Alias match
            elif ind_lower in _INDUSTRY_ALIAS:
                validated.append(_INDUSTRY_ALIAS[ind_lower])
            # 3. Partial substring match against valid set (pick best)
            else:
                matches = [v for k, v in _VALID_INDUSTRY_SET.items() if ind_lower in k or k in ind_lower]
                if matches:
                    validated.append(matches[0])
                else:
                    logger.warning(f"company_industry value '{ind}' is not in Apify's allowed list — dropping it")
        apify_params["company_industry"] = validated if validated else None

    for field in field_mappings:
        value = apify_params.get(field)
        if value is not None:
            run_input[field] = value
        # Omit field entirely if None — Apify rejects null for array fields

    # Override email_status if specified
    if apify_params.get("email_status"):
        run_input["email_status"] = apify_params["email_status"]

    return run_input


def run_leads_finder(actor_input: dict) -> tuple[str, list[dict]]:
    """Run the Leads Finder actor (IoSHqwTR9YGhzccez) and return normalized results."""
    logger.info(f"Starting Apify Leads Finder with fetch_count={actor_input.get('fetch_count')}")

    run = apify_client.actor(settings.apify_actor_id).call(run_input=actor_input)
    run_id = run.get("id", "unknown")

    logger.info(f"Leads Finder run completed: {run_id}")

    leads = []
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        normalized = _normalize_leads_finder_result(item)
        if normalized:
            leads.append(normalized)

    logger.info(f"Fetched {len(leads)} normalized leads from Leads Finder")
    return run_id, leads


# ── Secondary Lead Scraper actor (Apollo-style) ─────────────────────────────

# Mapping: our seniority_level values → Lead Scraper seniority values
_SENIORITY_TO_LEAD_SCRAPER = {
    "c_suite": ["CEO", "CXO"],
    "owner": ["Founder"],
    "founder": ["Founder"],
    "partner": ["Chairman", "President"],
    "vp": ["Vice President"],
    "director": ["Director"],
    "manager": ["Manager"],
    "senior": ["Manager"],  # closest match
    "head": ["Director"],
}

# Mapping: our LinkedIn industry names → Lead Scraper industry names
# Maps from VALID_INDUSTRIES in industry_param_generator to new actor's format
_INDUSTRY_TO_LEAD_SCRAPER = {
    # Heavy Industry & Energy
    "oil & energy": ["Oil and Gas"],
    "mining & metals": ["Mining"],
    "renewables & environment": ["Renewable Energy Power Generation"],
    "utilities": ["Utilities"],
    # Manufacturing
    "automotive": ["Motor Vehicle Manufacturing"],
    "aviation & aerospace": ["Aviation and Aerospace Component Manufacturing"],
    "chemicals": ["Chemical Manufacturing"],
    "machinery": ["Machinery Manufacturing"],
    "industrial automation": ["Automation Machinery Manufacturing"],
    "construction": ["Construction"],
    "manufacturing": ["Manufacturing"],
    "packaging & containers": ["Packaging and Containers Manufacturing"],
    "textiles": ["Textile Manufacturing"],
    "food production": ["Food and Beverage Manufacturing"],
    "pharmaceuticals": ["Pharmaceutical Manufacturing"],
    "electrical/electronic manufacturing": ["Appliances, Electrical, and Electronics Manufacturing"],
    # IT/SaaS
    "information technology & services": ["IT Services and IT Consulting"],
    "computer software": ["Software Development"],
    "internet": ["Technology, Information and Internet"],
    # Engineering & Consulting
    "mechanical or industrial engineering": ["Engineering Services"],
    "management consulting": ["Business Consulting and Services"],
    "civil engineering": ["Civil Engineering"],
    # Finance
    "financial services": ["Financial Services"],
    "investment management": ["Investment Management"],
    "venture capital & private equity": ["Venture Capital and Private Equity Principals"],
    "banking": ["Banking"],
    "insurance": ["Insurance"],
    # Professional Services
    "legal services": ["Legal Services"],
    "law practice": ["Law Practice"],
    "accounting": ["Accounting"],
    "human resources": ["Human Resources Services"],
    "staffing & recruiting": ["Staffing and Recruiting"],
    "logistics & supply chain": ["Transportation, Logistics, Supply Chain and Storage"],
    "transportation/trucking/railroad": ["Transportation, Logistics, Supply Chain and Storage"],
    "wholesale": ["Wholesale"],
    # Other common ones
    "real estate": ["Real Estate"],
    "marketing & advertising": ["Marketing Services"],
    "design": ["Design Services"],
    "hospitality": ["Hospitality"],
    "restaurants": ["Restaurants"],
    "retail": ["Retail"],
    "ecommerce": ["Retail"],
    "d2c": ["Retail"],
    "direct-to-consumer": ["Retail"],
    "consumer brand": ["Consumer Services"],
    "education management": ["Education Management"],
    "higher education": ["Higher Education"],
    "nonprofit organization management": ["Non-profit Organization Management"],
    "entertainment": ["Entertainment Providers"],
    "architecture & planning": ["Architecture and Planning"],
    "building materials": ["Building Materials"],
    "research": ["Research Services"],
    "biotechnology": ["Biotechnology Research"],
    "medical devices": ["Medical Equipment Manufacturing"],
    "environmental services": ["Environmental Services"],
    "computer & network security": ["Computer and Network Security"],
    "computer hardware": ["Computer Hardware Manufacturing"],
    "computer networking": ["Computer Networking Products"],
    "market research": ["Market Research"],
    "outsourcing/offshoring": ["Outsourcing and Offshoring Consulting"],
    "semiconductors": ["Semiconductor Manufacturing"],
    "government administration": ["Government Administration"],
    "professional training & coaching": ["Professional Training and Coaching"],
    "publishing": ["Book and Periodical Publishing"],
    "printing": ["Printing Services"],
    "furniture": ["Furniture and Home Furnishings Manufacturing"],
    "consumer goods": ["Consumer Services"],
    "sports": ["Sports Teams and Clubs"],
    "arts & crafts": ["Artists and Writers"],
    "religious institutions": ["Religious Institutions"],
    "e-learning": ["E-Learning Providers"],
    "security & investigations": ["Security and Investigations"],
    "cosmetics": ["Personal Care Product Manufacturing"],
    "online media": ["Online Audio and Video Media"],
    "farming": ["Farming"],
    "writing & editing": ["Writing and Editing"],
    "broadcast media": ["Broadcast Media Production and Distribution"],
    "maritime": ["Maritime Transportation"],
    "warehousing": ["Warehousing and Storage"],
    "defense & space": ["Defense and Space Manufacturing"],
    "paper & forest products": ["Paper and Forest Product Manufacturing"],
    "glass, ceramics & concrete": ["Glass, Ceramics and Concrete Manufacturing"],
    "capital markets": ["Capital Markets"],
    "plastics": ["Plastics Manufacturing"],
    "animation": ["Animation and Post-production"],
    "dairy": ["Dairy Product Manufacturing"],
    "gambling & casinos": ["Gambling Facilities and Casinos"],
    "shipbuilding": ["Shipbuilding"],
    "nanotechnology": ["Nanotechnology Research"],
    "wine & spirits": ["Wineries"],
    "luxury goods & jewelry": ["Retail Luxury Goods and Jewelry"],
    "consumer electronics": ["Computers and Electronics Manufacturing"],
    "oil, gas, and mining": ["Oil, Gas, and Mining"],
    "agriculture": ["Farming, Ranching, Forestry"],
    "fine art": ["Fine Arts Schools"],
    "performing arts": ["Performing Arts"],
    "photography": ["Photography"],
    "museums & institutions": ["Museums"],
    "import & export": ["Wholesale Import and Export"],
    "events services": ["Events Services"],
    "facilities services": ["Facilities Services"],
    "medical practice": ["Medical Practices"],
    "health, wellness & fitness": ["Wellness and Fitness Services"],
    "hospital & health care": ["Hospitals and Health Care"],
    "information services": ["Information Services"],
    "individual & family services": ["Individual and Family Services"],
    "public relations & communications": ["Public Relations and Communications Services"],
    "graphic design": ["Graphic Design"],
    "government relations": ["Government Relations Services"],
    "mental health care": ["Mental Health Care"],
    "alternative medicine": ["Alternative Medicine"],
    "program development": ["Software Development"],
    "think tanks": ["Think Tanks"],
    "computer games": ["Computer Games"],
    "fund-raising": ["Philanthropic Fundraising Services"],
    "executive office": ["Executive Offices"],
    "international trade & development": ["International Trade and Development"],
    "political organization": ["Political Organizations"],
    "newspapers": ["Newspaper Publishing"],
    "public safety": ["Public Safety"],
    "package/freight delivery": ["Freight and Package Transportation"],
    "wireless": ["Wireless Services"],
    "international affairs": ["International Affairs"],
    "public policy": ["Public Policy Offices"],
    "libraries": ["Libraries"],
    "supermarkets": ["Retail Groceries"],
    "fishery": ["Fisheries"],
    "military": ["Armed Forces"],
    "ranching": ["Ranching and Fisheries"],
    "railroad manufacture": ["Railroad Equipment Manufacturing"],
    "tobacco": ["Tobacco Manufacturing"],
    "judiciary": ["Administration of Justice"],
    "alternative dispute resolution": ["Alternative Dispute Resolution"],
    "legislative office": ["Legislative Offices"],
    "airlines/aviation": ["Airlines and Aviation"],
    "veterinary": ["Veterinary Services"],
    "leisure, travel & tourism": ["Travel Arrangements"],
    "recreational facilities & services": ["Recreational Facilities"],
    "motion pictures & film": ["Movies and Sound Recording"],
    "investment banking": ["Investment Banking"],
    "primary/secondary education": ["Primary and Secondary Education"],
    "telephone call centers": ["Telephone Call Centers"],
}

# Mapping: our location names → Lead Scraper companyCountry values (title case)
_LOCATION_TO_COUNTRY = {
    "united states": "United States",
    "united kingdom": "United Kingdom",
    "germany": "Germany",
    "france": "France",
    "netherlands": "Netherlands",
    "switzerland": "Switzerland",
    "italy": "Italy",
    "spain": "Spain",
    "sweden": "Sweden",
    "norway": "Norway",
    "denmark": "Denmark",
    "belgium": "Belgium",
    "austria": "Austria",
    "ireland": "Ireland",
    "poland": "Poland",
    "india": "India",
    "canada": "Canada",
    "australia": "Australia",
    "japan": "Japan",
    "china": "China",
    "brazil": "Brazil",
    "mexico": "Mexico",
    "singapore": "Singapore",
    "south korea": "South Korea",
    "united arab emirates": "United Arab Emirates",
    "saudi arabia": "Saudi Arabia",
    "south africa": "South Africa",
    "israel": "Israel",
    "new zealand": "New Zealand",
    "portugal": "Portugal",
    "czech republic": "Czech Republic",
    "finland": "Finland",
    "greece": "Greece",
    "romania": "Romania",
    "hungary": "Hungary",
    "turkey": "Turkey",
    "thailand": "Thailand",
    "indonesia": "Indonesia",
    "malaysia": "Malaysia",
    "philippines": "Philippines",
    "vietnam": "Vietnam",
    "colombia": "Colombia",
    "argentina": "Argentina",
    "chile": "Chile",
    "nigeria": "Nigeria",
    "kenya": "Kenya",
    "egypt": "Egypt",
    "pakistan": "Pakistan",
    "bangladesh": "Bangladesh",
}


def build_lead_scraper_input(
    apify_params: dict,
    locations: list[str],
    fetch_count: int = 100,
) -> dict:
    """
    Build input for the Lead Scraper actor (T1XDXWc1L92AfIJtd).

    Maps our ICP params to the Lead Scraper's expected input format:
    - seniority_level → seniority (title case)
    - company_industry → industry (Lead Scraper format)
    - contact_location → companyCountry (title case)
    - company_keywords → industryKeywords
    - contact_job_title → personTitle (up to 10 values)
    - contact_location → personCountry (via _LOCATION_TO_COUNTRY lookup)
    - functional → functional (Title-Case whitelist values)
    - size → companyEmployeeSize (normalized range strings)
    """
    # Map seniority
    our_seniority = apify_params.get("seniority_level", [])
    lead_scraper_seniority = []
    for s in our_seniority:
        s_lower = s.lower().strip()
        mapped = _SENIORITY_TO_LEAD_SCRAPER.get(s_lower, [])
        lead_scraper_seniority.extend(mapped)
    # Deduplicate while preserving order
    seen = set()
    lead_scraper_seniority = [
        x for x in lead_scraper_seniority
        if not (x in seen or seen.add(x))
    ]
    if not lead_scraper_seniority:
        lead_scraper_seniority = ["Founder", "CEO", "CXO", "Vice President", "Director", "Manager"]

    # Map industries
    our_industries = apify_params.get("company_industry", [])
    lead_scraper_industries = []
    for ind in our_industries:
        ind_lower = ind.lower().strip()
        mapped = _INDUSTRY_TO_LEAD_SCRAPER.get(ind_lower, [])
        lead_scraper_industries.extend(mapped)
    # Deduplicate
    seen = set()
    lead_scraper_industries = [
        x for x in lead_scraper_industries
        if not (x in seen or seen.add(x))
    ]

    # Map locations to countries
    company_countries = []
    for loc in locations:
        loc_lower = loc.lower().strip()
        mapped = _LOCATION_TO_COUNTRY.get(loc_lower)
        if mapped:
            company_countries.append(mapped)
        else:
            # Title-case fallback
            company_countries.append(loc.strip().title())

    # Build input
    run_input = {
        "contactEmailStatus": "verified",
        "includeEmails": True,
        "totalResults": fetch_count,
        "seniority": lead_scraper_seniority,
    }

    if company_countries:
        run_input["companyCountry"] = company_countries

    if lead_scraper_industries:
        run_input["industry"] = lead_scraper_industries

    # Map keywords
    keywords = apify_params.get("company_keywords", [])
    if keywords:
        run_input["industryKeywords"] = keywords

    if apify_params.get("contact_job_title"):
        run_input["personTitle"] = apify_params["contact_job_title"][:10]

    from services.icp_synonyms import LEAD_SCRAPER_FUNCTIONAL as _LS_FUNC
    if apify_params.get("functional"):
        valid_func = [d for d in apify_params["functional"] if d in _LS_FUNC]
        if valid_func:
            run_input["functional"] = valid_func

    # Lead Scraper valid companyEmployeeSize values:
    # "0 - 1","2 - 10","11 - 50","51 - 200","201 - 500","501 - 1000","1001 - 5000","5001 - 10000","10000+"
    if apify_params.get("size"):
        _SIZE_MAP = {
            "0-1": "0 - 1", "0 - 1": "0 - 1",
            "1-10": "2 - 10", "2-10": "2 - 10", "2 - 10": "2 - 10",
            "11-50": "11 - 50", "11 - 50": "11 - 50",
            "51-200": "51 - 200", "51 - 200": "51 - 200",
            "201-500": "201 - 500", "201 - 500": "201 - 500",
            "501-1000": "501 - 1000", "501 - 1000": "501 - 1000",
            "1001-5000": "1001 - 5000", "1001 - 5000": "1001 - 5000",
            "5001-10000": "5001 - 10000", "5001 - 10000": "5001 - 10000",
            "10000+": "10000+",
        }
        mapped_sizes = [_SIZE_MAP.get(s, s) for s in apify_params["size"] if _SIZE_MAP.get(s, s)]
        if mapped_sizes:
            run_input["companyEmployeeSize"] = list(dict.fromkeys(mapped_sizes))

    return run_input


def run_lead_scraper(actor_input: dict) -> tuple[str, list[dict]]:
    """
    Run the Lead Scraper actor (T1XDXWc1L92AfIJtd) and return results.
    Returns (run_id, list_of_normalized_lead_dicts).
    This is a blocking call - waits for the actor to finish.
    """
    logger.info(
        f"Starting Lead Scraper with totalResults={actor_input.get('totalResults')}, "
        f"countries={actor_input.get('companyCountry')}"
    )

    run = apify_client.actor(settings.apify_lead_scraper_id).call(run_input=actor_input)
    run_id = run.get("id", "unknown")

    logger.info(f"Lead Scraper run completed: {run_id}")

    raw_leads = []
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        raw_leads.append(item)

    # Normalize to our prospect format
    normalized = [_normalize_lead_scraper_result(item) for item in raw_leads]
    normalized = [n for n in normalized if n is not None]

    logger.info(f"Fetched {len(raw_leads)} raw, {len(normalized)} normalized leads from Lead Scraper")
    return run_id, normalized


# Size string → midpoint integer mapping for Lead Scraper output
_SIZE_RANGE_MAP = {
    "1 - 10": 5,
    "11 - 50": 30,
    "51 - 200": 125,
    "201 - 500": 350,
    "501 - 1,000": 750,
    "501 - 1000": 750,
    "1,001 - 5,000": 3000,
    "1001 - 5000": 3000,
    "5,001 - 10,000": 7500,
    "5001 - 10000": 7500,
    "10,001+": 15000,
    "10001+": 15000,
}

def _normalize_leads_finder_result(item: dict) -> dict | None:
    """
    Normalize a Leads Finder (Apollo-style) result to our prospect data format.
    Handles both flat and nested org fields common in Apollo actor outputs.
    Returns None if no usable identifier.
    """
    email = (item.get("email") or "").strip().lower() or None
    linkedin = (item.get("linkedin_url") or item.get("linkedin") or "").strip() or None

    if not email and not linkedin:
        return None

    # Org fields may be nested under "organization" or flat with "organization_" prefix
    org = item.get("organization") or {}

    company_name = (
        item.get("company_name") or item.get("organization_name")
        or org.get("name") or item.get("name", "").strip() or None
    )
    company_website = (
        item.get("company_website") or item.get("website_url")
        or item.get("organization_website_url")
        or org.get("website_url") or None
    )
    company_linkedin = (
        item.get("company_linkedin") or item.get("organization_linkedin_url")
        or org.get("linkedin_url") or None
    )
    industry = (
        item.get("industry") or item.get("organization_industry")
        or org.get("industry") or None
    )
    raw_size = (
        item.get("company_size") or item.get("num_employees")
        or item.get("organization_num_employees") or org.get("employee_count") or None
    )
    company_size = None
    if raw_size:
        try:
            company_size = int(str(raw_size).replace(",", "").split("-")[0].strip())
        except (ValueError, AttributeError):
            pass
    company_description = (
        item.get("company_description") or item.get("organization_description")
        or org.get("description") or None
    )
    company_domain = (
        item.get("company_domain") or item.get("primary_domain")
        or item.get("organization_primary_domain") or org.get("primary_domain") or None
    )

    raw_seniority = item.get("seniority_level") or item.get("seniority") or ""
    seniority_level = raw_seniority.lower().strip() if raw_seniority else None

    return {
        "first_name": (item.get("first_name") or "").strip().title() or None,
        "last_name": (item.get("last_name") or "").strip().title() or None,
        "full_name": (item.get("name") or item.get("full_name") or "").strip().title() or None,
        "email": email,
        "linkedin": linkedin,
        "job_title": item.get("title") or item.get("job_title") or None,
        "seniority_level": seniority_level,
        "city": item.get("city") or None,
        "state": item.get("state") or None,
        "country": item.get("country") or None,
        "company_name": company_name,
        "company_website": company_website,
        "company_domain": company_domain,
        "company_linkedin": company_linkedin,
        "industry": industry,
        "company_size": company_size,
        "company_description": company_description,
    }


# Reverse seniority mapping: Lead Scraper seniority → our seniority_level
_LEAD_SCRAPER_SENIORITY_REVERSE = {
    "Founder": "founder",
    "Chairman": "partner",
    "President": "c_suite",
    "CEO": "c_suite",
    "CXO": "c_suite",
    "Vice President": "vp",
    "Director": "director",
    "Manager": "manager",
}


def _normalize_lead_scraper_result(item: dict) -> dict | None:
    """
    Normalize a Lead Scraper result to match our prospect data format.
    Returns None if the lead has no usable identifiers.
    """
    email = (item.get("email") or "").strip().lower() or None
    linkedin = (item.get("linkedinUrl") or "").strip() or None

    if not email and not linkedin:
        return None

    # Parse company size from range string
    size_str = item.get("organizationSize") or ""
    company_size = _SIZE_RANGE_MAP.get(size_str.strip())

    # Map seniority back to our format
    raw_seniority = item.get("seniority") or ""
    seniority_level = _LEAD_SCRAPER_SENIORITY_REVERSE.get(raw_seniority, raw_seniority.lower() if raw_seniority else None)

    # Extract domain from website
    org_website = item.get("organizationWebsite") or ""
    company_domain = None
    if org_website:
        company_domain = org_website.replace("https://", "").replace("http://", "").rstrip("/")

    return {
        "first_name": (item.get("firstName") or "").strip().title() or None,
        "last_name": (item.get("lastName") or "").strip().title() or None,
        "full_name": (item.get("fullName") or "").strip().title() or None,
        "email": email,
        "personal_email": (item.get("personal_email") or "").strip().lower() or None,
        "mobile_number": item.get("phone_numbers") or None,
        "linkedin": linkedin,
        "job_title": item.get("position") or None,
        "headline": None,  # Not provided by this actor
        "seniority_level": seniority_level,
        "functional_level": (item.get("functional") or "").lower() or None,
        "city": item.get("city") or None,
        "state": item.get("state") or None,
        "country": item.get("country") or None,
        "company_name": item.get("organizationName") or None,
        "company_website": org_website or None,
        "company_domain": company_domain,
        "company_linkedin": item.get("organizationLinkedinUrl") or None,
        "company_linkedin_uid": None,
        "industry": item.get("organizationIndustry") or None,
        "company_size": company_size,
        "company_founded_year": str(item["organizationFoundedYear"]) if item.get("organizationFoundedYear") else None,
        "company_phone": None,
        "company_street_address": None,
        "company_full_address": None,
        "company_state": item.get("organizationState") or None,
        "company_city": item.get("organizationCity") or None,
        "company_country": item.get("organizationCountry") or None,
        "company_postal_code": None,
        "keywords": item.get("organizationSpecialities") or None,
        "company_description": item.get("organizationDescription") or None,
        "company_annual_revenue": None,
        "company_annual_revenue_clean": None,
        "company_total_funding": None,
        "company_total_funding_clean": None,
        "company_technologies": None,
    }


# ── Async wrappers for parallel execution ────────────────────────────────────

def iter_leads_finder(actor_input: dict):
    """Generator: yields normalized lead dicts from the Leads Finder actor."""
    logger.info(
        f"Starting Apify Leads Finder (streaming) with fetch_count={actor_input.get('fetch_count')} "
        f"input_keys={list(actor_input.keys())}"
    )
    try:
        run = apify_client.actor(settings.apify_actor_id).call(run_input=actor_input)
    except Exception as e:
        logger.exception(
            f"Leads Finder .call() failed before submitting run: {e!r} input={actor_input}"
        )
        raise
    run_id = run.get("id", "unknown")
    logger.info(f"Leads Finder run completed: {run_id} — streaming items")
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        normalized = _normalize_leads_finder_result(item)
        if normalized:
            yield normalized


def iter_lead_scraper(actor_input: dict):
    """
    Generator: yields normalized lead dicts as they're read from dataset.
    Allows callers to save results to MongoDB immediately per-item.
    """
    logger.info(
        f"Starting Lead Scraper (streaming) with totalResults={actor_input.get('totalResults')} "
        f"input_keys={list(actor_input.keys())}"
    )
    try:
        run = apify_client.actor(settings.apify_lead_scraper_id).call(run_input=actor_input)
    except Exception as e:
        logger.exception(
            f"Lead Scraper .call() failed before submitting run: {e!r} input={actor_input}"
        )
        raise
    run_id = run.get("id", "unknown")
    logger.info(f"Lead Scraper run completed: {run_id} — streaming items")
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        normalized = _normalize_lead_scraper_result(item)
        if normalized:
            yield normalized


async def run_leads_finder_async(actor_input: dict, semaphore: asyncio.Semaphore = None) -> tuple[str, list[dict]]:
    """Async wrapper around run_leads_finder using asyncio.to_thread."""
    if semaphore:
        async with semaphore:
            return await asyncio.to_thread(run_leads_finder, actor_input)
    return await asyncio.to_thread(run_leads_finder, actor_input)


async def run_lead_scraper_async(actor_input: dict, semaphore: asyncio.Semaphore = None) -> tuple[str, list[dict]]:
    """Async wrapper around run_lead_scraper using asyncio.to_thread."""
    if semaphore:
        async with semaphore:
            return await asyncio.to_thread(run_lead_scraper, actor_input)
    return await asyncio.to_thread(run_lead_scraper, actor_input)


class _ApifyRunTracker:
    """Mutable state object yielded by track_apify_run."""

    def __init__(self, actor_id: str, account_id: str | None, campaign_id: str | None) -> None:
        self.actor_id = actor_id
        self.account_id = account_id
        self.campaign_id = campaign_id
        self.run_id: str = ""
        self.items_count: int = 0

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id

    def set_items(self, count: int) -> None:
        self.items_count = count


@asynccontextmanager
async def track_apify_run(
    actor_id: str,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
    run_input: dict | None = None,
):
    """Context manager that records Apify run cost to apify_usage_collection.

    Usage:
        async with track_apify_run(actor_id, account_id=..., campaign_id=...) as tracker:
            result = client.actor(actor_id).call(run_input=...)
            tracker.set_run_id(result["id"])
            tracker.set_items(count)
    """
    tracker = _ApifyRunTracker(actor_id, account_id, campaign_id)
    started_at = datetime.now(timezone.utc)
    try:
        yield tracker
    finally:
        try:
            finished_at = datetime.now(timezone.utc)
            cost_usd = 0.0
            status = "succeeded"
            if tracker.run_id:
                try:
                    run_details = await asyncio.to_thread(
                        lambda: apify_client.run(tracker.run_id).get()
                    )
                    if run_details:
                        cost_usd = (run_details.get("usageTotalUsd") or 0.0)
                        status = (run_details.get("status") or "succeeded").lower()
                except Exception:
                    pass

            from database import apify_usage_collection
            doc = {
                "account_id": tracker.account_id,
                "campaign_id": tracker.campaign_id,
                "actor_id": tracker.actor_id,
                "run_id": tracker.run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "items_returned": tracker.items_count,
                "cost_usd": cost_usd,
                "status": status,
                "metadata": {},
            }
            await apify_usage_collection.insert_one(doc)
        except Exception:
            pass
