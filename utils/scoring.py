"""
Prospect scoring algorithm.
Scores each prospect 0-100 based on how well they match the configured ideal client profile.
"""

# --- ICP Target Lists ---

TARGET_SENIORITY = {
    "owner": 25,
    "c_suite": 25,
    "founder": 25,
    "partner": 22,
    "vp": 20,
    "director": 15,
    "manager": 10,
    "senior": 8,
}

TARGET_INDUSTRIES = {
    # Heavy Industry & Resources (exact match = 20)
    "oil & gas": 20, "oil, gas, and mining": 20, "mining & metals": 20,
    "mining": 20, "power generation": 20, "energy": 20,
    "renewables & environment": 20, "utilities": 20,
    # Manufacturing (exact match = 20)
    "automotive": 20, "aerospace": 20, "aerospace & defense": 20,
    "chemicals": 20, "chemical": 20, "steel": 20, "metals": 20,
    "industrial machinery": 20, "machinery": 20,
    "construction materials": 20, "construction": 18,
    "manufacturing": 18, "industrial automation": 18,
    "packaging": 16, "textiles": 16, "food production": 16,
    "pharmaceuticals": 16, "electronics": 16,
    # IT/SaaS (related = 15)
    "information technology": 15, "information technology and services": 15,
    "computer software": 15, "internet": 15, "saas": 15,
    # Engineering & Consulting (related = 14)
    "engineering": 14, "management consulting": 14,
    "civil engineering": 14, "mechanical engineering": 14,
    # Finance (related = 12)
    "financial services": 12, "investment management": 12,
    "venture capital & private equity": 12, "banking": 12,
    # Professional Services (related = 10)
    "legal services": 10, "accounting": 10, "human resources": 10,
    "insurance": 10, "logistics and supply chain": 10,
    "transportation/trucking/railroad": 10, "wholesale": 10,
}

TARGET_JOB_KEYWORDS = [
    "chief", "ceo", "cto", "cfo", "coo", "cmo", "cro", "cco",
    "founder", "co-founder", "owner", "partner", "principal",
    "president", "vice president", "vp",
    "director", "head of",
    "business development", "marketing", "sales",
    "sustainability", "government relations",
    "managing director", "general manager",
]

PRIORITY_CITIES_USA = [
    "houston", "dallas", "denver", "chicago", "detroit", "pittsburgh",
    "new york", "los angeles", "san francisco", "seattle", "boston",
    "atlanta", "phoenix", "minneapolis",
]

PRIORITY_CITIES_EUROPE = [
    "london", "manchester", "aberdeen", "birmingham",
    "frankfurt", "munich", "stuttgart", "hamburg",
    "paris", "lyon", "marseille",
    "rotterdam", "amsterdam",
    "zurich", "geneva",
    "milan", "turin",
    "madrid", "barcelona",
    "stockholm", "oslo", "copenhagen",
    "brussels", "antwerp",
    "vienna", "dublin", "warsaw", "krakow",
]

PRIORITY_COUNTRIES = ["united states", "united kingdom", "germany", "france",
                      "netherlands", "switzerland", "italy", "spain",
                      "sweden", "norway", "denmark", "belgium", "austria",
                      "ireland", "poland"]


def score_prospect_v2(lead: dict, icp_context: dict | None = None) -> tuple[float, dict]:
    """
    Score a prospect against the configured ICP (v2).
    Adds ICP alignment, digital maturity gap, engagement opportunity, and recency signal.
    Rebalanced dimensions to still sum to 100 max.

    Returns (total_score, breakdown_dict).
    """
    breakdown = {}

    # 1. Seniority (max 20, was 25)
    seniority = (lead.get("seniority_level") or "").lower().strip()
    seniority_score = 0
    seniority_map = {
        "owner": 20, "c_suite": 20, "founder": 20,
        "partner": 18, "vp": 16, "director": 12, "manager": 8, "senior": 6,
    }
    for key, points in seniority_map.items():
        if key in seniority:
            seniority_score = points
            break
    breakdown["seniority"] = seniority_score

    # 2. Industry match (max 15, was 20)
    # NB: the empty-string guard matters — "" is a substring of every key, so
    # a missing industry used to earn the full 15 points.
    industry = (lead.get("industry") or "").lower().strip()
    industry_score = 0
    if industry:
        for key, points in TARGET_INDUSTRIES.items():
            if key in industry or industry in key:
                # Rescale: old max 20 -> new max 15
                rescaled = round(points * 15 / 20)
                industry_score = max(industry_score, rescaled)
    breakdown["industry"] = industry_score

    # 3. Company size (max 12, was 15)
    company_size = lead.get("company_size")
    size_score = 0
    if company_size is not None:
        try:
            company_size = int(company_size)
        except (ValueError, TypeError):
            company_size = None
    if company_size is not None:
        if 20 <= company_size <= 5000:
            size_score = 12
        elif 10 <= company_size < 20:
            size_score = 6
        elif 5000 < company_size <= 10000:
            size_score = 8
        elif company_size > 10000:
            size_score = 4
    breakdown["company_size"] = size_score

    # 4. Revenue match (max 12, was 15)
    revenue = lead.get("company_annual_revenue_clean")
    revenue_score = 0
    if revenue is not None:
        try:
            revenue = float(revenue)
        except (ValueError, TypeError):
            revenue = None
    if revenue is not None:
        rev_m = revenue / 1_000_000
        if 20 <= rev_m <= 250:
            revenue_score = 12
        elif 5 <= rev_m < 20:
            revenue_score = 8
        elif 250 < rev_m <= 2000:
            revenue_score = 8
        elif rev_m > 2000:
            revenue_score = 4
        elif 1 <= rev_m < 5:
            revenue_score = 2
    breakdown["revenue"] = revenue_score

    # 5. Location match (max 8, was 10)
    city = (lead.get("city") or "").lower().strip()
    country = (lead.get("country") or "").lower().strip()
    location_score = 0
    all_priority_cities = PRIORITY_CITIES_USA + PRIORITY_CITIES_EUROPE
    if city in all_priority_cities:
        location_score = 8
    elif country in PRIORITY_COUNTRIES:
        location_score = 5
    elif country:
        location_score = 2
    breakdown["location"] = location_score

    # 6. Job title keywords (max 8, was 10)
    job_title = (lead.get("job_title") or "").lower()
    headline = (lead.get("headline") or "").lower()
    combined_title = f"{job_title} {headline}"
    matches = sum(1 for kw in TARGET_JOB_KEYWORDS if kw in combined_title)
    title_score = min(8, matches * 2)
    breakdown["job_title"] = title_score

    # 7. Email verified (max 5)
    email = lead.get("email")
    email_score = 5 if email and email.strip() else 0
    breakdown["email_verified"] = email_score

    # 8. ICP alignment (max 5) - NEW
    icp_score = 0
    if icp_context:
        target_industry = (icp_context.get("target_industry") or "").lower()
        target_titles = [t.lower() for t in icp_context.get("target_job_titles", []) if t]
        target_industries = [i.lower() for i in icp_context.get("target_industries", []) if i]

        # Industry alignment with search target
        if target_industry and target_industry in industry:
            icp_score += 2
        if any(ti in industry for ti in target_industries if ti):
            icp_score += 1

        # Job title alignment with search target
        if any(tt in combined_title for tt in target_titles if tt):
            icp_score += 2

        icp_score = min(5, icp_score)
    breakdown["icp_alignment"] = icp_score

    # 9. Digital maturity gap (max 5) - NEW
    # High revenue + no tech stack/no website = opportunity for digital transformation
    digital_gap_score = 0
    has_revenue = revenue is not None and revenue > 0
    has_tech = bool(lead.get("company_technologies"))
    has_website = bool(lead.get("company_website") or lead.get("company_domain"))

    if has_revenue:
        rev_m = revenue / 1_000_000 if revenue else 0
        if rev_m >= 20 and not has_tech:
            digital_gap_score += 3
        if rev_m >= 20 and not has_website:
            digital_gap_score += 2
    digital_gap_score = min(5, digital_gap_score)
    breakdown["digital_maturity_gap"] = digital_gap_score

    # 10. Engagement opportunity (max 5) - NEW
    # Short/missing company description, no tech stack = needs content marketing
    engagement_score = 0
    description = lead.get("company_description") or ""
    if len(description) < 50:
        engagement_score += 2
    if not description:
        engagement_score += 1
    if not has_tech:
        engagement_score += 2
    engagement_score = min(5, engagement_score)
    breakdown["engagement_opportunity"] = engagement_score

    # 11. Recency signal (max 5) - NEW
    # Company age vs size archetype detection
    recency_score = 0
    founded_year = lead.get("company_founded_year")
    if founded_year:
        try:
            founded = int(founded_year)
            company_age = 2026 - founded
            cs = lead.get("company_size")
            try:
                cs = int(cs) if cs else 0
            except (ValueError, TypeError):
                cs = 0

            # Traditional Titan: old (30+ years) + large = high opportunity
            if company_age >= 30 and cs >= 50:
                recency_score = 5
            # Growth Disruptor: 5-15 years
            elif 5 <= company_age <= 15 and cs >= 20:
                recency_score = 4
            # Established but not ancient
            elif 15 < company_age < 30:
                recency_score = 3
            # Very new
            elif company_age < 3:
                recency_score = 1
        except (ValueError, TypeError):
            pass
    breakdown["recency_signal"] = recency_score

    total = sum(breakdown.values())
    return min(100.0, total), breakdown


NON_DECISION_MAKER_TITLES = [
    "software engineer", "software developer", "web developer", "data engineer",
    "devops", "sre", "qa engineer", "test engineer", "frontend", "backend",
    "full stack", "mobile developer", "machine learning engineer",
    "accountant", "bookkeeper", "auditor", "tax",
    "recruiter", "talent acquisition", "hr coordinator", "hr assistant",
    "intern", "trainee", "apprentice", "junior",
    "receptionist", "secretary", "administrative assistant", "office manager",
    "graphic designer", "ui designer", "ux designer",
    "customer service", "customer support", "help desk", "technical support",
    "warehouse", "logistics coordinator", "shipping", "driver",
    "nurse", "physician", "therapist", "pharmacist",
    "teacher", "professor", "lecturer", "instructor",
    "paralegal", "legal assistant",
    "analyst",  # too generic, usually not decision-makers
]

DECISION_MAKER_TITLE_KEYWORDS = [
    "chief", "ceo", "cto", "cfo", "coo", "cmo", "cro", "cco",
    "founder", "co-founder", "owner", "partner", "principal",
    "president", "vice president", "vp",
    "director", "head of", "managing director", "general manager",
    "business development", "marketing", "sales",
    "sustainability", "government relations",
]

DECISION_MAKER_FUNCTIONS = [
    "marketing", "sales", "business development", "biz dev",
    "growth", "revenue", "partnerships", "strategy",
    "operations", "supply chain", "procurement",
    "sustainability", "digital transformation", "innovation",
]


def is_decision_maker_rule_based(prospect: dict) -> tuple[bool, str]:
    """
    Rule-based check for whether a prospect is likely a decision-maker.
    Uses job_title and seniority_level (available from Apify search data, no scraping needed).
    Returns (is_decision_maker, reasoning).
    """
    job_title = (prospect.get("job_title") or "").lower().strip()
    seniority = (prospect.get("seniority_level") or "").lower().strip()

    # High seniority → always a decision maker
    if seniority in ("owner", "c_suite", "founder", "partner"):
        return True, f"High seniority: {seniority}"

    # Check for non-decision-maker titles first
    for ndt in NON_DECISION_MAKER_TITLES:
        if ndt in job_title:
            return False, f"Non-decision-maker title: matched '{ndt}'"

    # VP/Director seniority with decision-maker title keywords
    if seniority in ("vp", "director"):
        for kw in DECISION_MAKER_TITLE_KEYWORDS:
            if kw in job_title:
                return True, f"{seniority} with decision-maker title keyword: '{kw}'"
        # VP/Director even without keyword match is still likely a decision-maker
        return True, f"Seniority level '{seniority}' implies decision-making authority"

    # Manager in relevant function
    if seniority == "manager" or "manager" in job_title:
        for func in DECISION_MAKER_FUNCTIONS:
            if func in job_title:
                return True, f"Manager in relevant function: '{func}'"

    # Senior in relevant function
    if seniority == "senior" or "senior" in job_title:
        for func in DECISION_MAKER_FUNCTIONS:
            if func in job_title:
                return True, f"Senior in relevant function: '{func}'"

    # Check for explicit decision-maker keywords in title
    for kw in DECISION_MAKER_TITLE_KEYWORDS:
        if kw in job_title:
            return True, f"Decision-maker title keyword: '{kw}'"

    return False, f"No decision-maker signals found (title: '{job_title}', seniority: '{seniority}')"


def score_company_fit_rule_based(prospect: dict) -> tuple[float, dict]:
    """
    Evaluate ONLY company-related scoring dimensions (no person dimensions).
    Reuses same logic as score_prospect_v2 for: industry (max 15), company size (max 12),
    revenue (max 12), location (max 8). Max possible ~47.
    Returns (score, breakdown_dict).
    """
    breakdown = {}

    # Industry match (max 15) — same as score_prospect_v2
    # (empty-string guard: "" is a substring of every key)
    industry = (prospect.get("industry") or "").lower().strip()
    industry_score = 0
    if industry:
        for key, points in TARGET_INDUSTRIES.items():
            if key in industry or industry in key:
                rescaled = round(points * 15 / 20)
                industry_score = max(industry_score, rescaled)
    breakdown["industry"] = industry_score

    # Company size (max 12) — same as score_prospect_v2
    company_size = prospect.get("company_size")
    size_score = 0
    if company_size is not None:
        try:
            company_size = int(company_size)
        except (ValueError, TypeError):
            company_size = None
    if company_size is not None:
        if 20 <= company_size <= 5000:
            size_score = 12
        elif 10 <= company_size < 20:
            size_score = 6
        elif 5000 < company_size <= 10000:
            size_score = 8
        elif company_size > 10000:
            size_score = 4
    breakdown["company_size"] = size_score

    # Revenue match (max 12) — same as score_prospect_v2
    revenue = prospect.get("company_annual_revenue_clean")
    revenue_score = 0
    if revenue is not None:
        try:
            revenue = float(revenue)
        except (ValueError, TypeError):
            revenue = None
    if revenue is not None:
        rev_m = revenue / 1_000_000
        if 20 <= rev_m <= 250:
            revenue_score = 12
        elif 5 <= rev_m < 20:
            revenue_score = 8
        elif 250 < rev_m <= 2000:
            revenue_score = 8
        elif rev_m > 2000:
            revenue_score = 4
        elif 1 <= rev_m < 5:
            revenue_score = 2
    breakdown["revenue"] = revenue_score

    # Location match (max 8) — same as score_prospect_v2
    city = (prospect.get("city") or "").lower().strip()
    country = (prospect.get("country") or "").lower().strip()
    location_score = 0
    all_priority_cities = PRIORITY_CITIES_USA + PRIORITY_CITIES_EUROPE
    if city in all_priority_cities:
        location_score = 8
    elif country in PRIORITY_COUNTRIES:
        location_score = 5
    elif country:
        location_score = 2
    breakdown["location"] = location_score

    total = sum(breakdown.values())
    return total, breakdown


# Generic seniority/level/scope words that carry NO functional targeting signal
# on their own. Sharing only these between a target title and a prospect title
# must never earn word-overlap credit — "marketing manager" vs "district
# merchandise manager" share only "manager" and are NOT the same role.
_GENERIC_TITLE_TOKENS = {
    "manager", "director", "head", "chief", "officer", "president", "vp",
    "vice", "senior", "junior", "lead", "principal", "global", "regional",
    "district", "associate", "assistant", "executive", "specialist",
    "coordinator", "analyst", "consultant", "supervisor", "staff", "group",
    "team",
}


def _title_match_score(prospect: dict, campaign: dict, max_pts: float) -> float:
    """Score title/headline alignment against campaign target titles. Returns 0 if no targets specified."""
    import re
    target_titles = [t.lower().strip() for t in (campaign.get("icp_job_titles") or []) if t and t.strip()]
    if not target_titles:
        return 0.0

    pt = (prospect.get("job_title") or prospect.get("title") or "").lower().strip()
    headline = (prospect.get("headline") or "").lower().strip()
    haystack = f"{pt} {headline}".strip()
    if not haystack:
        return 0.0

    def _tokens(s: str) -> set[str]:
        # Strip surrounding punctuation so "manager," == "manager" for both
        # the overlap check and the generic-token stoplist.
        return {w.strip(".,;:()[]&|") for w in re.split(r"[\s\-/]+", s)}

    best = 0.0
    for tt in target_titles:
        if tt == pt:
            best = max_pts
            break
        if tt in haystack or haystack in tt:
            best = max(best, max_pts * 0.8)
            continue
        # Word-overlap fallback: requires at least one shared *qualifier* word
        # ≥4 chars that is NOT a generic seniority/scope token. Sharing only
        # generic words like "manager"/"director" earns nothing.
        common = _tokens(tt) & _tokens(haystack)
        if any(len(w) >= 4 and w not in _GENERIC_TITLE_TOKENS for w in common):
            best = max(best, max_pts * 0.5)

    # Bonus: functional department match
    target_depts = [d.lower().strip() for d in (campaign.get("icp_functional_departments") or []) if d and d.strip()]
    if target_depts and best < max_pts:
        for dept in target_depts:
            if dept in haystack:
                best = min(max_pts, best * 1.2 + max_pts * 0.1)
                break

    return min(max_pts, best)


def score_prospect_for_campaign(prospect: dict, campaign: dict) -> float:
    """
    Campaign-aware 0-100 rule-based score. No AI calls.

    Uses only fields available from the Apify scrape / DB row, so it runs
    instantly on every discovered prospect and drives the initial multi-day
    cohort planning. AI enrichment happens separately, only on the cohort
    scheduled for the currently-approved day.

    Weights:
      - Seniority match to campaign ICP seniorities (0-10)
      - Title / headline match to campaign ICP job titles (0-35)
      - Industry match to campaign ICP industries (0-18)
      - Company size within ICP min/max window (0-12)
      - Country / region match to ICP countries (0-5)
      - Has email (0-15)
      - Has LinkedIn profile URL (0-5)
    Total = 100. Title+company(industry+size) = 35+30 = 65 dominant.
    """
    score = 0.0

    # Per-account weight overrides (tuned from reply-rate feedback via scoring_feedback_service)
    _weights = campaign.get("scoring_weights") or {}
    W_SENIORITY = _weights.get("seniority", 10)
    W_TITLE = _weights.get("title_match", 35)
    W_INDUSTRY = _weights.get("industry", 18)
    W_SIZE = _weights.get("company_size", 12)
    W_COUNTRY = _weights.get("country", 5)
    W_EMAIL = _weights.get("has_email", 15)
    W_LINKEDIN = _weights.get("has_linkedin", 5)

    # 1. Seniority match (0-W_SENIORITY)
    # Merge icp_seniority_levels (free-text) with seniorities (canonical tokens from icp_canonicalizer)
    # so that "C-Level" (free-text) and "c_suite" (canonical) both match a c-suite prospect.
    _raw_seniorities = list(campaign.get("seniorities") or []) + list(campaign.get("icp_seniority_levels") or [])
    target_seniorities = {s.lower().strip() for s in _raw_seniorities if s}
    # "seniority" is the canonical token from the employee transform; "seniority_level" is the legacy name
    prospect_seniority = (
        prospect.get("seniority_level") or prospect.get("seniority") or ""
    ).lower().strip()
    if target_seniorities and prospect_seniority:
        if prospect_seniority in target_seniorities:
            score += W_SENIORITY
        else:
            for ts in target_seniorities:
                if ts and (ts in prospect_seniority or prospect_seniority in ts):
                    score += W_SENIORITY * 0.67
                    break
    elif not target_seniorities and prospect_seniority:
        if prospect_seniority in ("owner", "c_suite", "founder", "vp", "director"):
            score += W_SENIORITY * 0.67
        elif prospect_seniority in ("manager", "senior"):
            score += W_SENIORITY * 0.33

    # 2. Title / headline match to campaign target titles (0-W_TITLE)
    score += _title_match_score(prospect, campaign, W_TITLE)

    # 3. Industry match (0-W_INDUSTRY)
    target_industries = [
        i.lower().strip() for i in (campaign.get("icp_industries") or []) if i
    ]
    # "industry" is the legacy name; new schema stores "company_industry_group" (e.g. "Retail")
    # and "company_industry_id" (numeric LinkedIn ID). Try all three.
    prospect_industry = (
        prospect.get("industry")
        or prospect.get("company_industry_group")
        or prospect.get("company_industry_id")
        or ""
    )
    if isinstance(prospect_industry, dict):
        prospect_industry = prospect_industry.get("label") or prospect_industry.get("group") or ""
    prospect_industry = str(prospect_industry).lower().strip()
    if target_industries and prospect_industry:
        if prospect_industry in target_industries:
            score += W_INDUSTRY
        else:
            for ti in target_industries:
                if ti and (ti in prospect_industry or prospect_industry in ti):
                    score += W_INDUSTRY * 0.72
                    break

    # 4. Company size within ICP window (0-W_SIZE)
    size_min = campaign.get("icp_company_size_min")
    size_max = campaign.get("icp_company_size_max")
    company_size_raw = prospect.get("company_size")
    company_size: int | None
    try:
        company_size = int(company_size_raw) if company_size_raw is not None else None
    except (ValueError, TypeError):
        company_size = None
    if company_size is not None and (size_min or size_max):
        lo = int(size_min) if size_min else 0
        hi = int(size_max) if size_max else 10**9
        if lo <= company_size <= hi:
            score += W_SIZE
        else:
            # Partial credit within a 50% tolerance of the window edge
            tolerance = max(lo * 0.5, 10)
            if abs(company_size - lo) <= tolerance or abs(company_size - hi) <= tolerance:
                score += W_SIZE * 0.47
    elif company_size is not None and company_size >= 10:
        score += W_SIZE * 0.53

    # 5. Country / region match (0-W_COUNTRY)
    target_countries = [
        c.lower().strip() for c in (campaign.get("icp_countries") or []) if c
    ]
    # New schema: country may be in location.country_code, location.country, or location.raw
    _loc = prospect.get("location") or {}
    if isinstance(_loc, dict):
        _loc_country = _loc.get("country") or _loc.get("country_code") or _loc.get("raw") or ""
    else:
        _loc_country = str(_loc)
    prospect_country = (prospect.get("country") or _loc_country or "").lower().strip()
    if target_countries and prospect_country:
        if prospect_country in target_countries:
            score += W_COUNTRY
        else:
            for tc in target_countries:
                if tc and (tc in prospect_country or prospect_country in tc):
                    score += W_COUNTRY * 0.6
                    break
    elif prospect_country:
        score += W_COUNTRY * 0.3

    # 6. Has email (0-W_EMAIL)
    email = (prospect.get("email") or "").strip()
    if email and "@" in email:
        score += W_EMAIL

    # 7. Has LinkedIn URL (0-W_LINKEDIN)
    linkedin = (prospect.get("linkedin") or "").strip().lower()
    if linkedin and ("linkedin.com" in linkedin or linkedin.startswith("http")):
        score += W_LINKEDIN

    return min(100.0, score)


# Seniority tokens that arm the non-decision-maker title blocklist in
# passes_title_gate. Includes "manager" and "lead": manager-targeted campaigns
# still must not enroll interns/trainees/assistants and other blocklisted
# titles. Only "mid"/"junior"-targeted campaigns run with the blocklist off
# (those legitimately target individual contributors).
_DM_SENIORITY_TRIGGER = {
    "c_suite", "csuite", "founder", "owner", "vp", "director", "head",
    "partner", "senior", "manager", "lead",
}


def passes_title_gate(
    prospect: dict,
    icp_seniority_levels: list[str],
    exclude_keywords: list[str] | None = None,
) -> tuple[bool, str | None]:
    """Check if a prospect passes the title-level quality gate.

    Returns (True, None) if passes, (False, reason) if rejected.
    Reasons: "non_decision_maker_title" | "title_keyword_blocklisted"
    """
    job_title = (prospect.get("job_title") or "").lower()
    headline = (prospect.get("headline") or "").lower()
    combined_text = job_title + " " + headline

    if exclude_keywords:
        for kw in exclude_keywords:
            kw_lower = kw.lower()
            if kw_lower in job_title or kw_lower in headline:
                return False, "title_keyword_blocklisted"

    normalized_seniorities = {s.lower().strip() for s in icp_seniority_levels}
    if normalized_seniorities & _DM_SENIORITY_TRIGGER:
        for ndt in NON_DECISION_MAKER_TITLES:
            if ndt in combined_text:
                return False, "non_decision_maker_title"

    return True, None


# ── Person-fit hard gate (deterministic, no AI) ──────────────────────────────
# Used by curated discovery to reject off-target employees BEFORE enrollment.
# The additive score_prospect_for_campaign lets company-level signals carry a
# wrong-role person over the threshold; this gate enforces person-level fit.

# Lightweight function inference from job title / headline keywords.
# Canonical tokens match CAMPAIGN_PREFILL_FIELD_OPTIONS["icp_functional_departments"].
_FUNCTION_KEYWORDS: dict[str, list[str]] = {
    "sales": [
        "sales", "account executive", " ae ", "sdr", "bdr", "business development",
        "revenue", "revops", "revenue operations", "partnerships", "gtm",
        "go-to-market", "go to market", "commercial director", "commercial manager",
    ],
    "marketing": [
        "marketing", "growth", "brand", "demand gen", "demand generation",
        "content", "seo", "sem", "communications", "public relations",
        "social media", "cmo",
        # Ad-buying titles are marketing, but "buyer"/"planner" also key the
        # merchandising function below — listing them here makes the inference
        # ambiguous ({marketing, merchandising}) so the disjointness gate
        # PASSES them for marketing campaigns instead of hard-rejecting.
        "media buyer", "media buying", "media planner", "media planning",
    ],
    "engineering": [
        "engineer", "engineering", "developer", "software", "cto", "devops",
        "sre", "architect", "technical lead", "technology officer",
    ],
    "product": ["product manager", "product management", "head of product", "cpo", "product owner", "product lead"],
    "operations": [
        "operations", "coo", "supply chain", "logistics", "procurement",
        "operational excellence",
    ],
    "finance": ["finance", "financial", "cfo", "accounting", "accountant", "controller", "treasury", "fp&a"],
    "hr": [
        "human resources", "hr business", "hr director", "hr manager", "head of hr",
        "chief people", "people officer", "people operations", "talent", "recruiting",
        "recruiter", "chro",
    ],
    "it": [
        "information technology", "it director", "it manager", "head of it",
        "infrastructure", "cio", "ciso", "information security", "cybersecurity",
        "sysadmin", "system administrator",
    ],
    "legal": ["legal", "counsel", "compliance", "attorney", "lawyer"],
    "customer_success": [
        "customer success", "customer support", "customer service", "customer experience",
        "account manager", "account management", "client success",
    ],
    "data": ["data scientist", "data analyst", "data engineer", "analytics", "machine learning", "chief data"],
    "design": ["designer", "design lead", "head of design", "ux ", "ui ", "creative director"],
    # Retail merchandising / buying / store-ops family. These titles are NOT
    # marketing even though they sound adjacent ("District Merchandise Manager",
    # "Allocation Manager", "Display Manager"). Substring matching on the
    # lowercase title+headline text, same as every other key: "merchandis"
    # catches merchandise/merchandiser/merchandising.
    "merchandising": [
        "merchandis", "allocation", "planner", "buying", "buyer",
        "visual display", "display", "store operations", "retail operations",
        "assortment", "inventory",
    ],
}

# ICP functional-department label → canonical token set. Handles synonyms
# ("GTM", "RevOps", "People", "Tech" …). Multi-token expansions allowed.
_FUNCTION_LABEL_SYNONYMS: dict[str, set[str]] = {
    "sales": {"sales"},
    "marketing": {"marketing"},
    "engineering": {"engineering"},
    "eng": {"engineering"},
    "tech": {"engineering", "it"},
    "technology": {"engineering", "it"},
    "software": {"engineering"},
    "product": {"product"},
    "product_management": {"product"},
    "operations": {"operations"},
    "ops": {"operations"},
    "finance": {"finance"},
    "accounting": {"finance"},
    "hr": {"hr"},
    "human_resources": {"hr"},
    "people": {"hr"},
    "talent": {"hr"},
    "recruiting": {"hr"},
    "it": {"it"},
    "information_technology": {"it"},
    "security": {"it"},
    "legal": {"legal"},
    "compliance": {"legal"},
    "customer_success": {"customer_success"},
    "customer_support": {"customer_success"},
    "support": {"customer_success"},
    "cs": {"customer_success"},
    "data": {"data"},
    "analytics": {"data"},
    "design": {"design"},
    "merchandising": {"merchandising"},
    "merchandise": {"merchandising"},
    "buying": {"merchandising"},
    "retail": {"merchandising", "operations"},
    "store_operations": {"merchandising", "operations"},
    "gtm": {"sales", "marketing"},
    "go_to_market": {"sales", "marketing"},
    "revops": {"sales", "operations"},
    "revenue_operations": {"sales", "operations"},
    "revenue": {"sales"},
    "growth": {"sales", "marketing"},
    "business_development": {"sales"},
    "biz_dev": {"sales"},
    "bd": {"sales"},
    "partnerships": {"sales"},
    "commercial": {"sales"},
}


def _normalize_label(label: str) -> str:
    """lowercase, strip punctuation, collapse separators to underscores."""
    import re
    s = (label or "").lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s\-]+", "_", s).strip("_")
    return s


def normalize_function_labels(labels: list[str]) -> set[str]:
    """Map campaign icp_functional_departments labels to canonical function tokens.
    Unknown labels map to nothing (caller decides how to handle)."""
    out: set[str] = set()
    for label in labels or []:
        key = _normalize_label(label)
        out |= _FUNCTION_LABEL_SYNONYMS.get(key, set())
    return out


def infer_function(title: str | None, headline: str | None = None) -> set[str]:
    """Infer the functional department(s) of a person from their job title/headline.

    Returns a set of canonical tokens (subset of _FUNCTION_KEYWORDS keys).
    Empty set = unknown (callers must treat unknown as PASS, not reject)."""
    text = f" {(title or '').lower()} {(headline or '').lower()} "
    if not text.strip():
        return set()
    matched: set[str] = set()
    for token, keywords in _FUNCTION_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                matched.add(token)
                break
    return matched


# Seniority label → canonical token (mirrors employee_scraper_service._map_seniority_canonical
# but kept local so utils has no service dependency).
_SENIORITY_LABEL_CANONICAL: dict[str, str] = {
    "c_suite": "c_suite", "csuite": "c_suite", "c_level": "c_suite", "clevel": "c_suite",
    "cxo": "c_suite", "executive": "c_suite", "exec": "c_suite", "chief": "c_suite",
    "founder": "founder", "co_founder": "founder", "cofounder": "founder",
    "owner": "owner", "partner": "partner", "president": "c_suite",
    "vp": "vp", "vice_president": "vp", "svp": "vp", "evp": "vp",
    "director": "director", "head": "director", "head_of": "director",
    "manager": "manager", "lead": "senior", "principal": "senior", "senior": "senior",
    "mid": "mid", "junior": "junior", "entry": "junior",
}


def canonical_seniority_from_label(label: str) -> str | None:
    """Normalize a free-text seniority label ('C-Level', 'VP', 'Head of') to a canonical token."""
    key = _normalize_label(label)
    if key in _SENIORITY_LABEL_CANONICAL:
        return _SENIORITY_LABEL_CANONICAL[key]
    # substring fallback for compound labels like "c_level_executive"
    for lk, canon in _SENIORITY_LABEL_CANONICAL.items():
        if lk in key:
            return canon
    return None


def infer_seniority_from_title(title: str | None) -> str | None:
    """Infer canonical seniority token from a job title when the scrape omitted it."""
    t = (title or "").lower()
    if not t:
        return None
    if any(k in t for k in ("ceo", "cto", "cfo", "coo", "cmo", "cro", "chief", "president")):
        return "c_suite"
    if "founder" in t or "co-founder" in t:
        return "founder"
    if "owner" in t:
        return "owner"
    if "partner" in t:
        return "partner"
    if t.startswith("vp") or " vp" in t or "vice president" in t:
        return "vp"
    if "director" in t or "head of" in t or t.startswith("head "):
        return "director"
    if "manager" in t:
        return "manager"
    if any(k in t for k in ("senior ", "sr.", " sr ", "principal", "lead ")):
        return "senior"
    return None


def person_fit_gate(prospect: dict, campaign: dict) -> tuple[bool, str | None]:
    """Deterministic person-level hard gate for curated discovery.

    Rejects a prospect when:
      1. passes_title_gate fails (non-decision-maker blocklist / icp_exclude_keywords) →
         "title_keyword_blocklisted" | "non_decision_maker_title"
      2. Campaign targets functional departments, the person's inferred function is
         non-empty, and the two sets are disjoint → "function_mismatch"
      3. Campaign specifies icp_job_titles or seniority targets, and the person
         matches NEITHER (title match score == 0 AND seniority mismatch) →
         "no_title_or_seniority_match"

    Unknown/empty inference always PASSES (never over-filter on missing data).
    Returns (True, None) or (False, reason).
    """
    target_seniorities_raw = list(campaign.get("seniorities") or []) + list(
        campaign.get("icp_seniority_levels") or []
    )
    # 1. Title blocklist + campaign exclude keywords.
    # Canonicalize labels first: passes_title_gate only arms the blocklist for
    # canonical decision-maker tokens, so free-text labels like "C-Level" must
    # be mapped (→ "c_suite") or the blocklist silently stays off.
    _gate_seniorities = [
        canonical_seniority_from_label(str(s)) or str(s)
        for s in target_seniorities_raw if s
    ]
    ok, reason = passes_title_gate(
        prospect,
        icp_seniority_levels=_gate_seniorities,
        exclude_keywords=campaign.get("icp_exclude_keywords") or None,
    )
    if not ok:
        return False, reason

    # 2. Function inference vs icp_functional_departments
    target_functions = normalize_function_labels(
        campaign.get("icp_functional_departments") or []
    )
    if target_functions:
        inferred = infer_function(
            prospect.get("job_title") or prospect.get("title"),
            prospect.get("headline"),
        )
        if inferred and inferred.isdisjoint(target_functions):
            return False, "function_mismatch"

    # 3. Title-or-seniority sub-gate (only when the campaign specified either)
    has_title_targets = bool(
        [t for t in (campaign.get("icp_job_titles") or []) if t and str(t).strip()]
    )
    target_seniorities = {
        c for s in target_seniorities_raw
        if s and (c := canonical_seniority_from_label(str(s)))
    }
    if not has_title_targets and not target_seniorities:
        return True, None

    # With the _GENERIC_TITLE_TOKENS stoplist in _title_match_score, a
    # title_score > 0 now implies a real qualifier match (shared functional
    # word, substring, or exact match) — never a generic-only overlap like
    # "manager". Generic-only-overlap prospects score 0 here and fall through
    # to the seniority check below; if that also misses they are rejected with
    # the existing (relaxable) "no_title_or_seniority_match" reason.
    title_score = _title_match_score(prospect, campaign, 1.0) if has_title_targets else 0.0
    if title_score > 0:
        return True, None

    prospect_seniority = (
        prospect.get("seniority") or prospect.get("seniority_level") or ""
    ).lower().strip()
    if not prospect_seniority:
        prospect_seniority = infer_seniority_from_title(
            prospect.get("job_title") or prospect.get("title") or prospect.get("headline")
        ) or ""
    if target_seniorities and prospect_seniority:
        canon = canonical_seniority_from_label(prospect_seniority) or prospect_seniority
        if canon in target_seniorities:
            return True, None
        # founder/owner/partner/c_suite are interchangeable at the top of most ICPs
        _top = {"c_suite", "founder", "owner", "partner"}
        if canon in _top and target_seniorities & _top:
            return True, None
    if not target_seniorities:
        # only titles were specified and none matched
        return False, "no_title_or_seniority_match"
    if not prospect_seniority:
        # seniority targets exist but person's seniority is unknown — don't over-filter
        return True, None
    return False, "no_title_or_seniority_match"


def tier_from_score(score: float | None) -> str | None:
    """Canonical priority tier from ai_prospect_score. hot>=80, warm>=60, cold<60."""
    if score is None:
        return None
    if score >= 80:
        return "hot"
    if score >= 60:
        return "warm"
    return "cold"
