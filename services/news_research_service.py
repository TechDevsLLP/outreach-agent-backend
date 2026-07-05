"""
Company news research using OpenRouter with Perplexity's online model.
Fetches recent material business events to populate prospects.company_news.
"""

import json
import logging
from typing import Optional
from services.openrouter_service import OpenRouterClient, extract_json

logger = logging.getLogger(__name__)

_NEWS_MODEL = "perplexity/sonar-pro"


async def research_company_news(
    company_name: str,
    *,
    limit: int = 5,
    days_back: int = 90,
    client: Optional[OpenRouterClient] = None,
) -> list[dict]:
    """
    Research recent news for a company using AI with web search.

    Returns up to `limit` CompanyNewsItem-compatible dicts:
      title, url, published_date (ISO 8601), source, summary (≤200 chars), sentiment

    Uses Perplexity Sonar Pro via OpenRouter for live web search.
    """
    prompt = f"""Find the top {limit} most recent and significant business news items about {company_name} from the last {days_back} days.

Focus only on material events: funding rounds, acquisitions/M&A, leadership changes, product launches, partnerships, expansions, layoffs, regulatory news, awards, or major customer wins.

Exclude: press releases that are just marketing fluff, minor blog posts, or social posts.

Return ONLY a JSON array (no extra text):
[
  {{
    "title": "Short news headline",
    "url": "https://...",
    "published_date": "YYYY-MM-DD",
    "source": "Publication name (e.g. TechCrunch, Reuters)",
    "summary": "1-2 sentence summary of what happened and why it matters (max 200 chars)",
    "sentiment": "positive|neutral|negative"
  }}
]

If no significant news found in the last {days_back} days, return an empty array: []"""

    owns_client = client is None
    if owns_client:
        client = OpenRouterClient()

    try:
        messages = [
            {
                "role": "system",
                "content": "You are a business news researcher. Return results as a JSON array only, no additional text.",
            },
            {"role": "user", "content": prompt},
        ]

        response = await client.chat_completion(
            messages=messages,
            model=_NEWS_MODEL,
            temperature=0.1,
            max_tokens=2048,
        )

        content = response.get("content", "") if isinstance(response, dict) else ""
        logger.debug(f"Raw news response for {company_name}: {content[:500]}")

        try:
            news_data = extract_json(content)
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Could not parse JSON from news response for {company_name}: {content[:300]}")
            return []

        # Normalise: accept array or {"news": [...]} wrapper
        if isinstance(news_data, dict):
            news_data = news_data.get("news") or news_data.get("items") or news_data.get("results") or []
        if not isinstance(news_data, list):
            return []

        normalized = []
        for item in news_data[:limit]:
            if not isinstance(item, dict):
                continue
            title = (item.get("title") or "").strip()
            if not title:
                continue
            normalized.append({
                "title": title,
                "url": item.get("url") or "",
                "published_date": item.get("published_date") or item.get("date") or "",
                "source": item.get("source") or "",
                "summary": (item.get("summary") or "")[:200],
                "sentiment": item.get("sentiment") or "neutral",
            })

        logger.info(f"Found {len(normalized)} news items for {company_name}")
        return normalized

    except Exception as e:
        logger.error(f"News research failed for {company_name}: {e}", exc_info=True)
        return []

    finally:
        if owns_client:
            await client.close()
