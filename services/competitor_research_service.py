"""
Competitor research using OpenRouter with Perplexity's online model.
Identifies top competitors and their differentiation.
"""

import logging
from typing import Optional
from services.openrouter_service import OpenRouterClient, extract_json

logger = logging.getLogger(__name__)


async def research_competitors(
    company_name: str,
    company_website: Optional[str] = None,
    industry: Optional[str] = None,
    limit: int = 3,
    client: Optional[OpenRouterClient] = None,
    seller_context: Optional[str] = None,
    account_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    feature: str = "company_research",
    raise_on_error: bool = False,
) -> list[dict]:
    """
    Research top competitors for a company using AI with web search.

    Args:
        company_name: Name of the company
        company_website: Company website (optional, for disambiguation)
        industry: Industry sector (optional, for better targeting)
        limit: Number of competitors to find (default: 3)
        client: Shared OpenRouterClient (creates one if not provided)
        seller_context: what WE (the outreach sender) sell — used to rank which
            competitor performs best relative to the pitched service and why
        account_id / campaign_id / feature: OpenRouter cost tags
        raise_on_error: when True, failures raise instead of returning [] so
            callers (company_research_service) can retry and record the error

    Returns:
        List of competitors with: name, website_url, linkedin_url,
        differentiation, market_position, is_best_performer (bool),
        why_winning (str|None — filled on the best performer).

    Uses Perplexity via OpenRouter for web search capability.
    """

    # Build research prompt
    prompt = f"""Research the top {limit} competitors of {company_name}"""
    if company_website:
        prompt += f" (website: {company_website})"
    if industry:
        prompt += f" in the {industry} industry"

    prompt += """.

For each competitor, provide:
1. Company name
2. Website URL
3. LinkedIn company page URL
4. Brief differentiation note (how they differ or compete, key strengths)
5. Market position (market leader / strong challenger / emerging / niche)
6. is_best_performer: exactly ONE competitor must have true — the one currently outperforming the others"""

    if seller_context:
        prompt += f"""
   Judge "best performing" specifically in the context of this service area (what the analysis will be used to pitch): {seller_context[:400]}"""

    prompt += """
7. why_winning: for the best performer ONLY, 1-2 concrete sentences on WHY it is winning (specific tactics, channels, positioning, or metrics — no generic praise); null for the others.

Return ONLY a JSON array with no additional text:
[
  {
    "name": "...",
    "website_url": "...",
    "linkedin_url": "...",
    "differentiation": "...",
    "market_position": "...",
    "is_best_performer": false,
    "why_winning": null
  }
]
"""

    owns_client = client is None
    if owns_client:
        client = OpenRouterClient()
    try:
        messages = [
            {
                "role": "system",
                "content": "You are a competitive intelligence researcher. Return results as a JSON array only, no additional text."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        # Use Perplexity's online model through OpenRouter for web search
        import json  # needed for json.JSONDecodeError
        response = await client.chat_completion(
            messages=messages,
            model="perplexity/sonar-pro",
            temperature=0.2,
            max_tokens=2048,
            account_id=account_id,
            campaign_id=campaign_id,
            feature=feature,
        )

        # Parse response
        content = response.get("content", "") if isinstance(response, dict) else ""
        logger.debug(f"Raw competitor research response for {company_name}: {content[:500]}")

        try:
            competitors_data = extract_json(content)
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Could not parse JSON from competitor response for {company_name}, content: {content[:300]}")
            competitors_data = []

        # Ensure it's a list
        if not isinstance(competitors_data, list):
            if isinstance(competitors_data, dict) and "competitors" in competitors_data:
                competitors_data = competitors_data["competitors"]
            else:
                competitors_data = [competitors_data] if isinstance(competitors_data, dict) else []

        # Normalize structure
        normalized = []
        for comp in competitors_data[:limit]:
            if not isinstance(comp, dict):
                continue
            normalized.append({
                "name": comp.get("name", ""),
                "website_url": comp.get("website_url") or comp.get("website"),
                "linkedin_url": comp.get("linkedin_url") or comp.get("linkedin"),
                "differentiation": comp.get("differentiation", ""),
                "market_position": comp.get("market_position"),
                "is_best_performer": bool(comp.get("is_best_performer")),
                "why_winning": comp.get("why_winning") or None,
            })

        # Enforce exactly-one best performer: if the model flagged none or many,
        # keep the first flagged one (or the first with a why_winning note).
        flagged = [c for c in normalized if c["is_best_performer"]]
        if len(flagged) != 1:
            best = flagged[0] if flagged else next(
                (c for c in normalized if c.get("why_winning")), None
            )
            for c in normalized:
                c["is_best_performer"] = c is best

        logger.info(f"Found {len(normalized)} competitors for {company_name}")
        return normalized

    except Exception as e:
        logger.error(f"Competitor research failed for {company_name}: {e}", exc_info=True)
        if raise_on_error:
            raise
        return []

    finally:
        if owns_client:
            await client.close()
