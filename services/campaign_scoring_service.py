"""
Deterministic campaign-aware prospect scorer.

Replaces per-prospect AI (OpenRouter) calls during smart campaign discovery.
Produces the same output shape as the AI assessment so downstream code and
the frontend require no changes.

company_fit_score (0-100) weights:
    industry match vs campaign.icp_industries    (0-40)
    country match vs campaign.icp_countries      (0-20)
    company size vs campaign size window         (0-20)
    keyword overlap in company / prospect text   (0-20)
    exclude-keyword hit → score forced to 0

prospect_fit_score (0-100) weights:
    title/headline vs campaign.icp_job_titles    (0-50)
    seniority match vs icp_seniority_levels      (0-30)
    functional dept match                        (0-20)

fit_score = 0.6 × company_fit_score + 0.4 × prospect_fit_score
priority_tier via tier_from_score() (hot ≥80, warm ≥60, cold <60)
"""
from __future__ import annotations
import re

from utils.scoring import _title_match_score, tier_from_score  # noqa: private import intentional

# Neutral baselines — applied when the campaign doesn't specify a given filter.
# Prevents punishing prospects for unconstrained dimensions.
_NEUTRAL_INDUSTRY: float = 20.0   # out of 40
_NEUTRAL_COUNTRY: float = 10.0    # out of 20
_NEUTRAL_SIZE: float = 10.0       # out of 20
_NEUTRAL_TITLE: float = 25.0      # out of 50
_NEUTRAL_SENIORITY: float = 15.0  # out of 30
_NEUTRAL_DEPT: float = 10.0       # out of 20

# Synonym map: canonical industry name → accepted aliases (all lowercase)
_INDUSTRY_SYNONYMS: dict[str, list[str]] = {
    "retail": ["retail", "e-retail", "ecommerce", "e-commerce", "consumer goods", "consumer products"],
    "consumer goods": ["consumer goods", "consumer products", "fmcg", "retail", "ecommerce"],
    "ecommerce": ["ecommerce", "e-commerce", "e-retail", "retail", "direct-to-consumer", "d2c", "online shopping"],
    "saas": ["saas", "software as a service", "information technology", "computer software", "internet"],
    "information technology": ["information technology", "it services", "software", "saas", "internet", "computer software"],
    "financial services": ["financial services", "finance", "banking", "fintech", "investment"],
    "healthcare": ["healthcare", "health", "medical", "hospital", "pharma", "pharmaceuticals", "life sciences",
                   "digital pathology", "drug discovery", "medical imaging", "healthtech", "health tech",
                   "clinical", "medtech", "med tech", "genomics", "diagnostics", "therapeutics", "biomedical"],
    "biotechnology": ["biotechnology", "biotech", "biopharma", "bio-pharma", "genomics", "gene therapy",
                      "cell therapy", "drug discovery", "life sciences", "bioinformatics"],
    "health technology": ["health technology", "healthtech", "health tech", "digital health", "medtech",
                          "medical technology", "medical devices", "clinical ai", "healthcare ai", "health ai"],
    "manufacturing": ["manufacturing", "industrial", "factory", "production", "industrial machinery"],
    "marketing": ["marketing", "advertising", "media", "pr", "public relations", "digital marketing"],
    "logistics": ["logistics", "supply chain", "transportation", "freight", "shipping", "delivery"],
}


# ---------------------------------------------------------------------------
# Company-side dimension scorers
# ---------------------------------------------------------------------------

def _industry_score(prospect: dict, campaign: dict, company: dict | None, max_pts: float) -> tuple[float, str]:
    targets = [t.lower().strip() for t in (campaign.get("icp_industries") or []) if t and t.strip()]
    if not targets:
        return _NEUTRAL_INDUSTRY * (max_pts / 40.0), "no industry filter"

    industry_parts = [
        (prospect.get("industry") or "").lower(),
        (prospect.get("company_industry") or "").lower(),
        ((company or {}).get("industry") or "").lower(),
        (prospect.get("company_description") or "").lower()[:200],
        " ".join(prospect.get("company_keywords") or []).lower(),
    ]
    corpus = " ".join(filter(None, industry_parts))

    for t in targets:
        if t and (t in corpus or corpus in t):
            return max_pts, f"industry match: {t}"
        # Synonym expansion
        for canonical, synonyms in _INDUSTRY_SYNONYMS.items():
            if t in synonyms and any(s in corpus for s in synonyms if s):
                return max_pts * 0.72, f"synonym industry match: {t}"

    # Partial word overlap (≥4 char words)
    for t in targets:
        t_words = {w for w in re.split(r"[\s\-/,&]+", t) if len(w) >= 4}
        c_words = {w for w in re.split(r"[\s\-/,&]+", corpus) if len(w) >= 4}
        if t_words and t_words & c_words:
            return max_pts * 0.4, "partial industry match"

    return 0.0, "no industry match"


def _country_score(prospect: dict, campaign: dict, max_pts: float) -> tuple[float, str]:
    targets = [c.lower().strip() for c in (campaign.get("icp_countries") or []) if c and c.strip()]
    if not targets:
        return _NEUTRAL_COUNTRY * (max_pts / 20.0), "no country filter"

    country = (prospect.get("country") or "").lower().strip()
    if not country:
        return 0.0, "unknown country"
    for t in targets:
        if t and (t == country or t in country or country in t):
            return max_pts, f"country match: {country}"
    return 0.0, f"country mismatch: {country}"


def _size_score(prospect: dict, campaign: dict, max_pts: float) -> tuple[float, str]:
    size_min = campaign.get("icp_company_size_min")
    size_max = campaign.get("icp_company_size_max")
    if not size_min and not size_max:
        cs_raw = prospect.get("company_size")
        if cs_raw:
            try:
                cs = int(cs_raw)
                if 10 <= cs <= 10_000:
                    return _NEUTRAL_SIZE * (max_pts / 20.0), "company size present, no filter"
            except (ValueError, TypeError):
                pass
        return _NEUTRAL_SIZE * (max_pts / 20.0), "no size filter"

    cs_raw = prospect.get("company_size")
    if cs_raw is None:
        return 0.0, "unknown company size"
    try:
        cs = int(cs_raw)
    except (ValueError, TypeError):
        return 0.0, "unparseable company size"

    lo = int(size_min) if size_min else 0
    hi = int(size_max) if size_max else 10 ** 9
    if lo <= cs <= hi:
        return max_pts, f"size {cs} in range [{lo}, {hi}]"
    tol = max(lo * 0.5, 20)
    if abs(cs - lo) <= tol or abs(cs - hi) <= tol:
        return max_pts * 0.5, f"size {cs} adjacent to range"
    return 0.0, f"size {cs} outside range [{lo}, {hi}]"


def _keyword_overlap_score(
    prospect: dict, campaign: dict, company: dict | None, max_pts: float
) -> tuple[float, str]:
    keywords = [k.lower().strip() for k in (campaign.get("icp_keywords") or []) if k and k.strip()]
    if not keywords:
        return 0.0, "no keywords"

    corpus_parts = [
        (prospect.get("company_description") or "").lower(),
        " ".join(prospect.get("company_keywords") or []).lower(),
        " ".join(prospect.get("company_technologies") or []).lower(),
        (prospect.get("headline") or "").lower(),
        (prospect.get("job_title") or prospect.get("title") or "").lower(),
    ]
    if company:
        corpus_parts += [
            (company.get("description") or "").lower(),
            " ".join(company.get("specialties") or []).lower(),
        ]
    corpus = " ".join(filter(None, corpus_parts))

    matches = [k for k in keywords if k in corpus]
    if not matches:
        return 0.0, "no keyword overlap"
    ratio = len(matches) / len(keywords)
    score = min(max_pts, round(ratio * max_pts, 1))
    return score, f"keywords matched: {', '.join(matches[:4])}"


# ---------------------------------------------------------------------------
# Prospect-side dimension scorers
# ---------------------------------------------------------------------------

def _infer_seniority_from_title(title: str) -> str:
    t = title.lower()
    words = set(re.split(r"[\s,.\-@/|]+", t))
    if "chief" in t or words & {"ceo", "cto", "cfo", "coo", "cmo", "cpo", "cdo"}: return "c_suite"
    if "founder" in t or "co-founder" in t: return "founder"
    if "owner" in t and "co-owner" not in t: return "owner"
    if t.startswith("vp ") or " vp " in t or ", vp " in t or "vp," in t or "vice president" in t: return "vp"
    if "director" in t: return "director"
    if "head of" in t or t.startswith("head,") or re.search(r"\bhead\b", t): return "head"
    if "manager" in t: return "manager"
    if "senior" in t or words & {"sr", "sr."}: return "senior"
    return ""


def _seniority_score(prospect: dict, campaign: dict, max_pts: float) -> tuple[float, str]:
    targets = {s.lower().strip() for s in (campaign.get("icp_seniority_levels") or []) if s and s.strip()}
    if not targets:
        return _NEUTRAL_SENIORITY * (max_pts / 30.0), "no seniority filter"

    seniority = (prospect.get("seniority_level") or "").lower().strip()
    if not seniority:
        # Fall back to title/headline inference when seniority_level not stored
        title_text = (prospect.get("job_title") or prospect.get("title") or "").strip()
        headline_text = (prospect.get("headline") or "").strip()
        seniority = _infer_seniority_from_title(title_text) or _infer_seniority_from_title(headline_text)
    if not seniority:
        return 0.0, "unknown seniority"

    if seniority in targets:
        return max_pts, f"seniority match: {seniority}"

    _adjacent: dict[str, set[str]] = {
        "c_suite": {"vp", "director", "founder", "owner"},
        "founder": {"c_suite", "owner"},
        "owner": {"founder", "c_suite"},
        "vp": {"c_suite", "director"},
        "director": {"vp", "manager"},
        "manager": {"director", "senior"},
        "senior": {"manager"},
    }
    for t in targets:
        if seniority in (_adjacent.get(t) or set()) or t in (_adjacent.get(seniority) or set()):
            return max_pts * 0.5, f"adjacent seniority: {seniority}"
    return 0.0, f"seniority mismatch: {seniority}"


def _dept_score(prospect: dict, campaign: dict, max_pts: float) -> tuple[float, str]:
    targets = [d.lower().strip() for d in (campaign.get("icp_functional_departments") or []) if d and d.strip()]
    if not targets:
        return _NEUTRAL_DEPT * (max_pts / 20.0), "no department filter"

    title = (prospect.get("job_title") or prospect.get("title") or "").lower()
    headline = (prospect.get("headline") or "").lower()
    combined = f"{title} {headline}"
    for t in targets:
        if t and t in combined:
            return max_pts, f"department match: {t}"
    return 0.0, "no department match"


def _exclude_keyword_hit(prospect: dict, campaign: dict) -> str | None:
    """Returns the blocking keyword if any exclude keyword matches, else None."""
    excludes = [k.lower().strip() for k in (campaign.get("icp_exclude_keywords") or []) if k and k.strip()]
    if not excludes:
        return None
    title = (prospect.get("job_title") or prospect.get("title") or "").lower()
    company_name = (prospect.get("company_name") or "").lower()
    industry = (prospect.get("industry") or "").lower()
    headline = (prospect.get("headline") or "").lower()
    combined = f"{title} {company_name} {industry} {headline}"
    for kw in excludes:
        if kw in combined:
            return kw
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_campaign_score(
    prospect: dict,
    campaign: dict,
    profile: dict | None = None,
    company: dict | None = None,
) -> dict:
    """
    Deterministic campaign-aware score for a prospect. No AI calls.

    Returns the same shape as the AI assessment:
    {
        fit_score: float,
        company_fit_score: float,
        prospect_fit_score: float,
        priority_tier: str,
        reasoning: str,
        breakdown: dict,
    }
    """
    # Merge LinkedIn profile into prospect for title/headline scoring
    if profile:
        prospect = dict(prospect)
        if not prospect.get("headline") and profile.get("headline"):
            prospect["headline"] = profile["headline"]

    # Hard block on exclude keywords
    blocked = _exclude_keyword_hit(prospect, campaign)
    if blocked:
        return {
            "fit_score": 0.0,
            "company_fit_score": 0.0,
            "prospect_fit_score": 0.0,
            "priority_tier": "cold",
            "reasoning": f"Excluded: keyword '{blocked}' matched in prospect profile.",
            "breakdown": {"exclude_keyword_hit": blocked},
        }

    # ── Company fit (60 % of fit_score) ──────────────────────────────────────
    ind_pts, ind_reason = _industry_score(prospect, campaign, company, max_pts=40.0)
    country_pts, country_reason = _country_score(prospect, campaign, max_pts=20.0)
    size_pts, size_reason = _size_score(prospect, campaign, max_pts=20.0)
    kw_pts, kw_reason = _keyword_overlap_score(prospect, campaign, company, max_pts=20.0)

    company_fit_score = round(min(100.0, max(0.0, ind_pts + country_pts + size_pts + kw_pts)), 1)

    # ── Prospect fit (40 % of fit_score) ─────────────────────────────────────
    title_pts = _title_match_score(prospect, campaign, max_pts=50.0)
    if title_pts == 0 and not (campaign.get("icp_job_titles") or []):
        # No title targets specified → apply neutral baseline instead of zero
        title_pts = _NEUTRAL_TITLE
    seniority_pts, seniority_reason = _seniority_score(prospect, campaign, max_pts=30.0)
    dept_pts, dept_reason = _dept_score(prospect, campaign, max_pts=20.0)

    prospect_fit_score = round(min(100.0, max(0.0, title_pts + seniority_pts + dept_pts)), 1)

    # ── Blended score ─────────────────────────────────────────────────────────
    fit_score = round(0.6 * company_fit_score + 0.4 * prospect_fit_score, 1)
    fit_score = min(100.0, max(0.0, fit_score))

    # ── Human-readable reasoning (top 4 signals) ──────────────────────────────
    signals: list[str] = []
    if ind_pts > 0 and "no industry" not in ind_reason:
        signals.append(ind_reason)
    elif ind_pts == 0 and "no industry" not in ind_reason:
        signals.append(ind_reason)
    if country_pts > 0 and "no country" not in country_reason:
        signals.append(country_reason)
    if kw_pts > 0:
        signals.append(kw_reason)
    if title_pts > 0:
        signals.append(f"title match: {round(title_pts, 0)}/50")
    if seniority_pts > 0 and "no seniority" not in seniority_reason:
        signals.append(seniority_reason)
    if not signals:
        signals.append("no strong ICP signal matches")
    reasoning = "; ".join(signals[:4])

    return {
        "fit_score": fit_score,
        "company_fit_score": company_fit_score,
        "prospect_fit_score": prospect_fit_score,
        "priority_tier": tier_from_score(fit_score),
        "reasoning": reasoning,
        "breakdown": {
            "industry": round(ind_pts, 1),
            "country": round(country_pts, 1),
            "company_size": round(size_pts, 1),
            "keyword_overlap": round(kw_pts, 1),
            "title_match": round(title_pts, 1),
            "seniority": round(seniority_pts, 1),
            "functional_dept": round(dept_pts, 1),
        },
    }
