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
) -> list[dict]:
    """
    Research top competitors for a company using AI with web search.

    Args:
        company_name: Name of the company
        company_website: Company website (optional, for disambiguation)
        industry: Industry sector (optional, for better targeting)
        limit: Number of competitors to find (default: 3)
        client: Shared OpenRouterClient (creates one if not provided)

    Returns:
        List of competitors with: name, website_url, linkedin_url, differentiation, market_position

    Uses Perplexity via OpenRouter for web search capability.
    """

    # Build research prompt
    prompt = f"""Research the top {limit} competitors of {company_name}"""
    if company_website:
        prompt += f" (website: {company_website})"
    if industry:
        prompt += f" in the {industry} industry"

    prompt += f""".

For each competitor, provide:
1. Company name
2. Website URL
3. LinkedIn company page URL
4. Brief differentiation note (how they differ or compete, key strengths)
5. Market position (market leader / strong challenger / emerging / niche)

Return ONLY a JSON array with no additional text:
[
  {{
    "name": "...",
    "website_url": "...",
    "linkedin_url": "...",
    "differentiation": "...",
    "market_position": "..."
  }}
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
            max_tokens=2048
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
            normalized.append({
                "name": comp.get("name", ""),
                "website_url": comp.get("website_url") or comp.get("website"),
                "linkedin_url": comp.get("linkedin_url") or comp.get("linkedin"),
                "differentiation": comp.get("differentiation", ""),
                "market_position": comp.get("market_position")
            })

        logger.info(f"Found {len(normalized)} competitors for {company_name}")
        return normalized

    except Exception as e:
        logger.error(f"Competitor research failed for {company_name}: {e}", exc_info=True)
        return []

    finally:
        if owns_client:
            await client.close()
