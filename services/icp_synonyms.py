"""
Semantic synonym expansion for ICP matching in DB queries.

When a user selects "c_suite" they also mean "founder", "owner", "ceo" etc.
When they select "SaaS" they also mean "Computer Software", "Internet", etc.
These curated maps prevent us from missing prospects stored under equivalent labels.
"""

import re
from typing import Pattern

# ---------------------------------------------------------------------------
# Seniority equivalence groups
# ---------------------------------------------------------------------------
# Any member of a group is semantically equivalent to every other member.
# When expanding, we return the union of all groups that contain the input.

SENIORITY_GROUPS: list[set[str]] = [
    # Top decision-makers — founder / owner / C-suite / president all interchangeable
    {
        "founder", "co_founder", "co-founder", "cofounder",
        "owner", "c_suite", "c-suite", "csuite",
        "ceo", "cmo", "cfo", "cto", "coo", "cio", "chro", "cpo", "cro", "cco",
        "president", "managing_director", "managing-director", "md",
        "executive_director", "executive director",
        "chief executive", "chief marketing", "chief financial",
        "chief technology", "chief operating", "chief information",
    },
    # Executive / Partner tier
    {
        "vp", "vice_president", "vice-president", "vice president",
        "svp", "evp", "partner", "general_partner", "general partner",
        "managing_partner", "managing partner",
    },
    # Director / Head tier
    {
        "director", "head", "head_of", "head-of", "head of",
        "global_director", "global director", "senior_director", "senior director",
        "regional_director", "regional director",
    },
    # Manager tier
    {
        "manager", "senior_manager", "senior-manager", "senior manager",
        "line_manager", "general_manager", "general manager",
    },
    # Senior IC
    {"senior", "lead", "principal", "staff"},
    # Mid IC
    {"mid", "mid_level", "mid-level", "associate"},
    # Junior / Entry
    {"junior", "entry", "entry_level", "entry-level", "graduate", "intern"},
]

# Flat lookup: normalized label → its group members
_SENIORITY_LOOKUP: dict[str, set[str]] = {}
for _group in SENIORITY_GROUPS:
    for _member in _group:
        _SENIORITY_LOOKUP[_member.lower()] = _group


def _normalize(s: str) -> str:
    return s.lower().strip()


def expand_seniorities(levels: list[str]) -> list[str]:
    """
    Given a list of ICP seniority levels, return the full set of equivalent
    stored values to use in a MongoDB $in query.

    Example: expand_seniorities(["c_suite"]) → ["c_suite","c-suite","founder","owner","ceo",...]
    """
    result: set[str] = set()
    for level in levels:
        norm = _normalize(level)
        group = _SENIORITY_LOOKUP.get(norm)
        if group:
            result.update(group)
        else:
            # Not in any group — include the input plus basic dash/underscore variants
            result.add(norm)
            result.add(norm.replace("-", "_"))
            result.add(norm.replace("_", "-"))
    return list(result)


# ---------------------------------------------------------------------------
# Industry alias groups
# ---------------------------------------------------------------------------
# Each key is a trigger keyword; its value is the full list of equivalent
# industry label substrings to match in the DB.

INDUSTRY_ALIASES: dict[str, list[str]] = {
    # Software / SaaS / Tech
    "saas": [
        "saas", "software as a service", "computer software", "software",
        "internet", "cloud computing", "information technology",
        "information technology and services", "it services", "cloud",
    ],
    "software": [
        "software", "saas", "computer software", "internet",
        "information technology", "information technology and services",
        "it services",
    ],
    "technology": [
        "technology", "information technology", "information technology and services",
        "computer", "software", "internet", "it services", "tech",
    ],
    "internet": ["internet", "saas", "software", "e-commerce", "ecommerce", "online"],

    # FinTech / Finance
    "fintech": [
        "fintech", "financial technology", "financial services", "banking",
        "payments", "investment management", "venture capital & private equity",
        "capital markets", "insurance",
    ],
    "finance": [
        "finance", "financial services", "banking", "investment management",
        "venture capital & private equity", "capital markets",
        "accounting", "insurance", "fintech",
    ],
    "banking": ["banking", "financial services", "finance", "fintech", "capital markets"],

    # E-commerce / Retail
    "ecommerce": [
        "ecommerce", "e-commerce", "retail", "consumer goods", "d2c",
        "direct-to-consumer", "online retail", "wholesale", "consumer electronics",
    ],
    "retail": [
        "retail", "ecommerce", "e-commerce", "consumer goods", "wholesale",
        "consumer electronics", "d2c",
    ],

    # Healthcare / MedTech
    "healthcare": [
        "healthcare", "health care", "hospital & health care", "hospital and health care",
        "medical devices", "medical", "pharmaceuticals", "pharma",
        "biotechnology", "biotech", "wellness", "health tech", "healthtech",
        "life sciences",
    ],
    "medical": [
        "medical", "healthcare", "health care", "hospital & health care",
        "medical devices", "pharmaceuticals",
    ],
    "biotech": ["biotech", "biotechnology", "life sciences", "pharmaceuticals", "healthcare"],
    "pharmaceuticals": ["pharmaceuticals", "pharma", "biotech", "biotechnology", "life sciences"],

    # AI / ML / Data
    "ai": [
        "ai", "artificial intelligence", "machine learning", "ml",
        "data science", "deep learning", "generative ai", "gen ai",
    ],
    "machine learning": ["machine learning", "ml", "ai", "artificial intelligence", "data science"],
    "data": ["data", "data science", "analytics", "big data", "business intelligence", "data analytics"],

    # Marketing / Advertising
    "marketing": [
        "marketing", "advertising", "pr", "public relations",
        "marketing and advertising", "market research",
        "digital marketing", "growth marketing", "brand",
    ],
    "advertising": ["advertising", "marketing", "marketing and advertising", "pr", "media"],
    "media": [
        "media", "entertainment", "broadcast", "publishing", "news",
        "digital media", "online media", "media and entertainment",
    ],

    # Manufacturing / Industrial
    "manufacturing": [
        "manufacturing", "industrial", "industrial automation", "automation",
        "machinery", "industrial machinery", "electronics manufacturing",
        "food production", "chemicals", "packaging",
    ],
    "industrial": [
        "industrial", "manufacturing", "industrial automation", "automation",
        "machinery", "industrial machinery",
    ],

    # Real Estate / PropTech
    "real estate": [
        "real estate", "real estate development", "property", "proptech",
        "commercial real estate", "residential real estate",
        "facilities management",
    ],
    "proptech": ["proptech", "real estate", "real estate development", "property technology"],

    # Education / EdTech
    "education": [
        "education", "edtech", "higher education", "e-learning", "elearning",
        "education management", "online learning", "lms", "training",
        "education technology",
    ],
    "edtech": ["edtech", "education technology", "education", "e-learning", "online learning"],

    # Logistics / Supply Chain
    "logistics": [
        "logistics", "supply chain", "logistics and supply chain",
        "transportation", "transportation/trucking/railroad",
        "freight", "shipping", "last mile", "fulfillment",
    ],
    "supply chain": ["supply chain", "logistics", "logistics and supply chain", "transportation"],

    # Legal / LegalTech
    "legal": [
        "legal", "legal services", "law practice", "law firm", "legaltech",
        "legal technology",
    ],

    # Consulting / Professional Services
    "consulting": [
        "consulting", "management consulting", "business consulting",
        "professional services", "strategy consulting",
        "management consulting and services",
    ],
    "professional services": [
        "professional services", "consulting", "management consulting",
        "advisory", "staffing and recruiting",
    ],

    # HR / Talent / Staffing
    "hr": [
        "hr", "human resources", "staffing", "recruiting", "talent",
        "hrtech", "staffing and recruiting", "executive search",
    ],
    "staffing": ["staffing", "recruiting", "staffing and recruiting", "hr", "executive search"],

    # Energy / CleanTech
    "energy": [
        "energy", "renewables", "clean energy", "cleantech", "oil & gas",
        "oil, gas, and mining", "power generation", "utilities",
        "solar", "wind energy",
    ],
    "cleantech": ["cleantech", "clean energy", "renewables", "energy", "sustainability"],

    # Cybersecurity / InfoSec
    "cybersecurity": [
        "cybersecurity", "information security", "infosec", "security",
        "computer and network security", "network security",
    ],

    # Construction / Architecture
    "construction": [
        "construction", "architecture", "real estate development",
        "civil engineering", "construction materials",
    ],

    # Telecom
    "telecom": [
        "telecom", "telecommunications", "wireless", "broadband",
        "telco", "internet service provider",
    ],

    # Automotive
    "automotive": [
        "automotive", "automobile", "car", "mobility", "electric vehicles", "ev",
        "transportation", "auto",
    ],

    # Travel / Hospitality
    "travel": [
        "travel", "hospitality", "tourism", "hotel", "airlines", "aviation",
        "travel and tourism",
    ],

    # Agriculture / AgTech
    "agriculture": [
        "agriculture", "agtech", "agri", "farming", "food & beverages",
        "food and beverages", "food production",
    ],
}

# Build a flat lookup: any alias term → canonical group member list
_INDUSTRY_LOOKUP: dict[str, list[str]] = {}
for _key, _aliases in INDUSTRY_ALIASES.items():
    for _alias in _aliases:
        _existing = _INDUSTRY_LOOKUP.get(_alias.lower(), [])
        for _a in _aliases:
            if _a not in _existing:
                _existing.append(_a)
        _INDUSTRY_LOOKUP[_alias.lower()] = _existing


def expand_industry_terms(industries: list[str]) -> list[str]:
    """
    Return the full set of equivalent industry label strings to match.
    The caller converts these to regex patterns for MongoDB.
    """
    result: set[str] = set()
    for industry in industries:
        norm = industry.lower().strip()
        # Direct alias lookup
        aliases = _INDUSTRY_LOOKUP.get(norm)
        if aliases:
            result.update(aliases)
        else:
            # Substring scan — if any alias key is contained in the input or vice versa
            found = False
            for key, key_aliases in INDUSTRY_ALIASES.items():
                if key in norm or norm in key:
                    result.update(key_aliases)
                    found = True
            if not found:
                result.add(industry)  # Keep original if no alias known
    return list(result)


def expand_industry_regexes(industries: list[str]) -> list[re.Pattern]:
    """
    Return a list of compiled case-insensitive regex patterns covering
    all industry aliases for the given ICP industries list.
    """
    terms = expand_industry_terms(industries)
    patterns = []
    for term in terms:
        try:
            patterns.append(re.compile(re.escape(term), re.IGNORECASE))
        except re.error:
            pass
    return patterns


# ---------------------------------------------------------------------------
# Job title expansion
# ---------------------------------------------------------------------------
# Maps a function domain to keyword variants.
# When a user picks "Marketing Director", we match any title containing
# a marketing keyword AND a seniority token (or the exact input phrase).

FUNCTION_KEYWORDS: dict[str, list[str]] = {
    "marketing": ["marketing", "growth", "brand", "demand gen", "digital marketing", "content"],
    "sales": ["sales", "revenue", "business development", "bd", "account executive",
               "account management", "commercial"],
    "product": ["product", "product management", "pm"],
    "engineering": ["engineering", "technology", "technical", "devops", "infrastructure",
                    "platform", "data engineering"],
    "finance": ["finance", "financial", "accounting", "controller", "treasury", "fp&a"],
    "operations": ["operations", "ops", "supply chain", "procurement", "logistics"],
    "hr": ["hr", "human resources", "people", "talent", "recruiting", "culture"],
    "legal": ["legal", "counsel", "compliance", "regulatory"],
    "data": ["data", "analytics", "business intelligence", "insights", "data science"],
    "design": ["design", "ux", "ui", "creative", "brand design"],
    "customer_success": ["customer success", "customer experience", "cx", "account management"],
    "partnerships": ["partnerships", "alliances", "channels", "business development"],
}

SENIORITY_TOKENS: list[str] = [
    "chief", "head", "vp", "vice president", "director",
    "founder", "owner", "ceo", "cmo", "cfo", "cto", "coo",
    "president", "partner", "managing", "lead", "principal",
]


def expand_job_title_query(titles: list[str]) -> str:
    """
    Return a single regex string (for MongoDB $regex) that matches job titles
    corresponding to the user-provided ICP title list.

    Matching strategy (OR of):
    1. Literal phrase match for each input title.
    2. Function-domain keyword match: for each title, detect the function domain
       (marketing / sales / etc.) and build a pattern matching any title that
       contains both a domain keyword and a seniority token.
    """
    if not titles:
        return ""

    alternatives: list[str] = []

    # 1. Literal matches
    for title in titles:
        alternatives.append(re.escape(title.strip()))

    # 2. Domain-aware expansion
    for title in titles:
        title_lower = title.lower()

        # Detect which function domain this title belongs to
        matched_domains: list[str] = []
        for domain, keywords in FUNCTION_KEYWORDS.items():
            if any(kw in title_lower for kw in keywords):
                matched_domains.append(domain)

        # Detect if the title contains a seniority signal
        has_seniority = any(tok in title_lower for tok in SENIORITY_TOKENS)

        if matched_domains and has_seniority:
            # For each matched domain, emit a lookahead pattern:
            # title must match domain keywords AND seniority tokens
            for domain in matched_domains:
                domain_kws = FUNCTION_KEYWORDS[domain]
                domain_pattern = "|".join(re.escape(k) for k in domain_kws)
                seniority_pattern = "|".join(re.escape(s) for s in SENIORITY_TOKENS)
                # Lookahead: contains at least one domain keyword AND one seniority token
                alternatives.append(
                    f"(?=.*(?:{domain_pattern}))(?=.*(?:{seniority_pattern}))"
                )

    if not alternatives:
        return "|".join(re.escape(t) for t in titles)

    return "|".join(f"(?:{a})" for a in alternatives)


# ---------------------------------------------------------------------------
# Functional department whitelist — actor-specific formats
# ---------------------------------------------------------------------------

LEAD_SCRAPER_FUNCTIONAL = {
    "Marketing", "Sales", "Engineering", "Product Management", "Operations", "HR",
    "Finance", "Legal", "IT", "Customer Service", "Cyber Security", "Cloud",
    "Data Engineering", "Devops", "Digital", "Distribution", "Compliance",
    "Controller", "Analytics", "Admin", "Security", "Fraud", "Hiring",
    "Infrastructure", "Inside Sales", "Learning", "Network Security", "Production",
    "Product Security", "Purchase", "Research", "Risk", "Support", "Testing",
    "Training", "Applications",
}

LEADS_FINDER_FUNCTIONAL = {
    "c_suite", "finance", "product_management", "engineering", "design", "hr", "it",
    "legal", "marketing", "operations", "sales", "support",
}


def to_lead_scraper_functional(dept: str) -> str | None:
    """Map a generic department name to Lead Scraper format (Title-Case)."""
    if dept in LEAD_SCRAPER_FUNCTIONAL:
        return dept
    _lower = dept.lower().strip()
    _MAP = {
        "product management": "Product Management",
        "product": "Product Management",
        "pm": "Product Management",
        "customer service": "Customer Service",
        "customer success": "Customer Service",
        "cyber security": "Cyber Security",
        "cybersecurity": "Cyber Security",
        "data engineering": "Data Engineering",
        "data": "Data Engineering",
        "network security": "Network Security",
        "product security": "Product Security",
        "inside sales": "Inside Sales",
    }
    for key, val in _MAP.items():
        if key in _lower:
            return val
    for lead_func in LEAD_SCRAPER_FUNCTIONAL:
        if lead_func.lower() == _lower:
            return lead_func
    return None


def to_leads_finder_functional(dept: str) -> str | None:
    """Map a generic department name to LEADS_FINDER format (lowercase)."""
    _lower = dept.lower().strip()
    _MAP = {
        "marketing": "marketing",
        "growth": "marketing",
        "brand": "marketing",
        "digital": "marketing",
        "demand generation": "marketing",
        "sales": "sales",
        "account executive": "sales",
        "inside sales": "sales",
        "business development": "sales",
        "engineering": "engineering",
        "software": "engineering",
        "developer": "engineering",
        "devops": "engineering",
        "cloud": "engineering",
        "infrastructure": "engineering",
        "data engineering": "engineering",
        "product management": "product_management",
        "product": "product_management",
        "design": "design",
        "ux": "design",
        "ui": "design",
        "hr": "hr",
        "human resources": "hr",
        "hiring": "hr",
        "recruiting": "hr",
        "it": "it",
        "information technology": "it",
        "legal": "legal",
        "compliance": "legal",
        "fraud": "legal",
        "finance": "finance",
        "controller": "finance",
        "accounting": "finance",
        "operations": "operations",
        "distribution": "operations",
        "supply chain": "operations",
        "customer service": "support",
        "customer success": "support",
        "support": "support",
        "analytics": "product_management",
        "research": "product_management",
        "c-suite": "c_suite",
        "c_suite": "c_suite",
        "csuite": "c_suite",
        "cxo": "c_suite",
        "c-level": "c_suite",
        "executive": "c_suite",
        "security": "it",
        "cyber security": "it",
        "network security": "it",
    }
    # Exact match first to avoid short-key substring collisions (e.g. "ui" in "c-suite")
    if _lower in _MAP:
        return _MAP[_lower]
    for key, val in _MAP.items():
        if key in _lower:
            return val
    if _lower in LEADS_FINDER_FUNCTIONAL:
        return _lower
    return None


def infer_functional_from_titles(job_titles: list[str]) -> tuple[list[str], list[str]]:
    """
    Infer functional departments from a list of job title strings.
    Returns (lead_scraper_functional, leads_finder_functional).
    """
    lead_scraper: list[str] = []
    leads_finder: list[str] = []
    seen_lf: set = set()
    seen_ls: set = set()
    for title in job_titles:
        lower_t = title.lower()
        lf = to_leads_finder_functional(lower_t)
        ls = to_lead_scraper_functional(title)
        if lf and lf not in seen_lf:
            leads_finder.append(lf)
            seen_lf.add(lf)
        if ls and ls not in seen_ls:
            lead_scraper.append(ls)
            seen_ls.add(ls)
    return lead_scraper, leads_finder
