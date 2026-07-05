"""
AI-powered prospect assessment using OpenRouter.
Combines AI fit scoring with enhanced rule-based scoring for a hybrid score.
"""

import logging
from services.openrouter_service import OpenRouterClient, extract_json, get_free_model
from config import get_settings as _get_settings
from utils.prompts import build_assessment_user_prompt, build_batch_assessment_user_prompt, get_system_prompt

logger = logging.getLogger(__name__)

settings = _get_settings()


async def assess_lead(
    lead: dict,
    profile: dict | None,
    company: dict | None,
    client: OpenRouterClient,
    company_profile: dict | None = None,
    campaign: dict | None = None,
) -> dict:
    """
    Assess a lead's fit using AI.
    When company_profile is provided, the assessment is personalised to the
    triggering user's ICP definition; otherwise a generic fit is scored.
    Returns structured assessment dict.
    """
    user_prompt = build_assessment_user_prompt(lead, profile, company, company_profile=company_profile, campaign=campaign)

    messages = [
        {"role": "system", "content": await get_system_prompt("assessment")},
        {"role": "user", "content": user_prompt},
    ]

    assessment = await client.chat_completion(
        messages=messages,
        model=settings.assessment_model,
        temperature=0.3,
        max_tokens=3072,
        response_format={"type": "json_object"},
    )

    # Validate required fields
    required = ["fit_rating", "fit_score", "reasoning"]
    for field in required:
        if field not in assessment:
            logger.warning(f"Assessment missing field '{field}' for prospect {lead.get('email', 'unknown')}")
            assessment.setdefault(field, "unknown" if isinstance(field, str) and field != "fit_score" else 0)

    # Default company/prospect fit scores to fit_score if missing (backwards compatibility)
    fit_score = assessment.get("fit_score", 0)
    if "company_fit_score" not in assessment:
        assessment["company_fit_score"] = fit_score
    if "prospect_fit_score" not in assessment:
        assessment["prospect_fit_score"] = fit_score

    return assessment


async def batch_assess_leads(
    batch: list[tuple],
    client: OpenRouterClient,
    model: str,
    campaign: dict | None = None,
) -> list[dict | None]:
    """
    Assess multiple prospects in a single OpenRouter call.

    Each tuple in batch: (lead, profile, company, company_profile)
    Returns a list of assessment dicts (same order). Entry is None if that prospect failed.
    Falls back to individual assess_lead() calls if the batch parse fails.
    """
    if not batch:
        return []

    prospects_data = [
        {"lead": t[0], "profile": t[1], "company": t[2], "company_profile": t[3] if len(t) > 3 else None}
        for t in batch
    ]

    user_prompt = build_batch_assessment_user_prompt(prospects_data, campaign=campaign)
    system_prompt = await get_system_prompt("assessment")

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    def _parse_batch_response(raw: dict | str) -> list[dict] | None:
        """
        Flexibly parse a batch response that may be:
          - {"assessments": [...]}        (ideal)
          - [assessment1, assessment2, ...]  (model returned list directly)
          - a raw string needing JSON extraction
        Returns the assessments list if valid length, else None.
        """
        # raw string from no-format call
        if isinstance(raw, dict) and "content" in raw and len(raw) == 1:
            try:
                parsed = extract_json(raw["content"])
            except Exception:
                return None
        else:
            parsed = raw

        # Unwrap {"assessments": [...]}
        if isinstance(parsed, dict):
            parsed = parsed.get("assessments", parsed)

        # If it's now a list, validate length
        if isinstance(parsed, list):
            if len(parsed) == len(batch):
                return [a if isinstance(a, dict) else {} for a in parsed]
            # Sometimes model wraps each prospect in a dict keyed differently
            return None

        return None

    try:
        # Call without response_format so we get raw content — lets us handle
        # models that return a list instead of {"assessments": [...]}
        # Use higher max_tokens to avoid truncation on smaller free models
        raw_response = await client.chat_completion(
            messages=messages,
            model=model,
            temperature=0.3,
            max_tokens=6144,
            response_format={"type": "json_object"},
        )

        assessments = _parse_batch_response(raw_response)
        if assessments is None:
            raise ValueError(f"Could not parse batch response from {model}: {str(raw_response)[:200]}")

        # Fill defaults per assessment
        results = []
        for i, assessment in enumerate(assessments):
            fit_score = assessment.get("fit_score", 0)
            assessment.setdefault("fit_rating", "unknown")
            assessment.setdefault("reasoning", "")
            if "company_fit_score" not in assessment:
                assessment["company_fit_score"] = fit_score
            if "prospect_fit_score" not in assessment:
                assessment["prospect_fit_score"] = fit_score
            results.append(assessment)

        logger.info(f"Batch assessment: {len(results)} prospects assessed with {model}")
        return results

    except Exception as e:
        logger.warning(
            f"Batch assessment failed ({model}), falling back to individual calls: {e}"
        )
        # Graceful degradation: assess one by one sequentially with a small delay
        # Uses assess_lead() which tries the configured model then falls back through FREE_MODELS on 402
        import asyncio as _asyncio
        results = []
        for i, (lead, profile, company, *rest) in enumerate(batch):
            company_profile = rest[0] if rest else None
            if i > 0:
                await _asyncio.sleep(0.1)  # Brief pause between sequential calls to avoid rate-limit spikes
            try:
                assessment = await assess_lead(lead, profile, company, client, company_profile=company_profile)
                assessment.setdefault("fit_rating", "unknown")
                assessment.setdefault("reasoning", "")
                assessment.setdefault("fit_score", 0)
                results.append(assessment)
            except Exception as ie:
                logger.error(f"Individual fallback also failed for {lead.get('email', 'unknown')}: {ie}")
                results.append(None)
        return results


def compute_ai_prospect_score(
    lead: dict,
    profile: dict | None,
    company: dict | None,
    assessment: dict,
) -> tuple[float, dict]:
    """
    Compute hybrid AI-enhanced prospect score (0-100).
    60% from AI fit_score + 40% from enhanced rule-based scoring using deep data.
    Returns (total_score, breakdown_dict).
    """
    # AI component (60%)
    ai_fit_score = assessment.get("fit_score", 50)
    ai_fit_score = max(0, min(100, ai_fit_score))
    ai_component = ai_fit_score * 0.6

    # Enhanced rule-based component (40%)
    rule_scores = {}

    # 1. Seniority verification (max 10 pts)
    seniority = (lead.get("seniority_level") or "").lower()
    seniority_map = {
        "owner": 10, "founder": 10, "c_suite": 10, "partner": 8,
        "vp": 7, "director": 5, "manager": 3, "senior": 2,
    }
    rule_scores["seniority"] = seniority_map.get(seniority, 1)

    # 2. Digital maturity gap (max 10 pts) - low LinkedIn presence = high opportunity
    digital_gap_score = 0
    if company:
        follower_count = company.get("followerCount") or company.get("followersCount", 0)

        if isinstance(follower_count, (int, float)):
            # Score based on follower count relative to company size (revenue-independent)
            company_size = lead.get("company_size") or 0
            try:
                company_size = int(company_size) if isinstance(company_size, str) else (company_size or 0)
            except (ValueError, TypeError):
                company_size = 0
            if company_size >= 100 and follower_count < 2000:
                digital_gap_score = 10  # Large company, small following
            elif company_size >= 50 and follower_count < 3000:
                digital_gap_score = 8
            elif company_size >= 20 and follower_count < 1000:
                digital_gap_score = 6
            elif follower_count < 1000:
                digital_gap_score = 4
            elif follower_count < 3000:
                digital_gap_score = 2
    rule_scores["digital_maturity_gap"] = digital_gap_score

    # 3. Engagement potential from post activity (max 10 pts)
    engagement_score = 0
    if profile:
        posts = profile.get("posts") or profile.get("recentPosts") or []
        connections = profile.get("connectionsCount", 0)
        followers = profile.get("followersCount", 0)

        # Low post activity + senior = big opportunity
        if len(posts) == 0:
            engagement_score = 8  # Content desert
        elif len(posts) < 3:
            engagement_score = 6
        elif len(posts) < 5:
            engagement_score = 4
        else:
            engagement_score = 2  # Already posting - less urgency

        # Bonus if low follower count for their seniority
        if seniority in ("owner", "founder", "c_suite", "vp") and isinstance(followers, (int, float)):
            if followers < 1000:
                engagement_score = min(10, engagement_score + 2)
    else:
        engagement_score = 5  # No profile data, assume moderate
    rule_scores["engagement_potential"] = engagement_score

    # 4. Company size fit (max 5 pts)
    company_size = lead.get("company_size") or 0
    if isinstance(company_size, (int, float)):
        if 20 <= company_size <= 5000:
            size_score = 5
        elif 10 <= company_size < 20 or 5000 < company_size <= 10000:
            size_score = 3
        elif company_size > 10000:
            size_score = 1
        else:
            size_score = 1
    else:
        size_score = 2
    rule_scores["company_size_fit"] = size_score

    # 5. Priority tier alignment (max 5 pts)
    priority_tier = assessment.get("priority_tier", "tier_3")
    tier_scores = {"tier_1": 5, "tier_2": 3, "tier_3": 1}
    rule_scores["priority_tier"] = tier_scores.get(priority_tier, 1)

    # Sum rule-based (out of 40)
    rule_total = sum(rule_scores.values())  # max 40
    rule_component = min(40.0, rule_total)

    total_score = round(ai_component + rule_component, 1)
    total_score = max(0, min(100, total_score))

    breakdown = {
        "ai_fit_score": ai_fit_score,
        "ai_component": round(ai_component, 1),
        "rule_component": round(rule_component, 1),
        "rule_scores": rule_scores,
        "total": total_score,
    }

    return total_score, breakdown
