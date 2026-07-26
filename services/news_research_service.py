"""
Company news research. Fetches recent material business events to populate
prospects.company_news.

Provider selection (settings.research_provider, env RESEARCH_PROVIDER):
  "gemini" (default) — Gemini + Google Search grounding, direct google.genai
      SDK call. Cheap (~$0.075/$0.30 per 1M tokens). Gemini helper code
      (client init, 429 retry-delay parsing, rate limiting, usage logging)
      mirrors services/company_sourcing_service.py; duplicated rather than
      imported so this module stays self-contained (same pattern as
      company_gate_service.py / title_gate_service.py).
  "perplexity" — original path: Perplexity Sonar Pro via OpenRouter
      (services/openrouter_service.chat_completion). Kept intact as a
      fallback/opt-out; far more expensive.

Both paths return the SAME output shape (see research_company_news /
research_company_news_and_signals docstrings) and are fail-soft: any error
yields empty defaults rather than raising, unless raise_on_error=True.
"""

import asyncio
import json
import logging
import re
from typing import Optional
from services.openrouter_service import OpenRouterClient, extract_json

logger = logging.getLogger(__name__)

_NEWS_MODEL = "perplexity/sonar-pro"

# ── Gemini grounding path (mirrors services/company_sourcing_service.py) ────

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_GEMINI_RETRY_MAX = 3
_GEMINI_NEWS_RATE = (150, 60)  # (max_calls, window_seconds) -> ~150 req/min
_GEMINI_RATE_LIMITER_KEY = f"gemini-news-research:{_GEMINI_MODEL}"


def _strip_citations(text: str) -> str:
    """Remove inline citation markers like [1], [2], [1,2] before JSON parsing."""
    return re.sub(r'\[\d+(?:,\s*\d+)*\]', '', text)


def _extract_gemini_retry_delay_seconds(exc: Exception) -> float | None:
    """Best-effort extraction of a server-suggested retry delay from a
    google.genai exception (e.g. a 429 ResourceExhausted). Mirrors
    services/company_sourcing_service.py."""
    try:
        details = getattr(exc, "details", None)
        if isinstance(details, dict):
            for detail in (details.get("error", {}).get("details") or []):
                if isinstance(detail, dict) and "retryDelay" in detail:
                    match = re.match(r"([\d.]+)s?$", str(detail["retryDelay"]).strip())
                    if match:
                        return float(match.group(1))
    except Exception:
        pass
    try:
        match = re.search(r'retryDelay["\']?\s*[:=]\s*["\']?(\d+(?:\.\d+)?)', str(exc))
        if match:
            return float(match.group(1))
    except Exception:
        pass
    return None


def _is_gemini_rate_limit_error(exc: Exception) -> bool:
    """True for HTTP 429 / RESOURCE_EXHAUSTED errors from the google.genai SDK."""
    code = getattr(exc, "code", None)
    if code == 429:
        return True
    status = str(getattr(exc, "status", "") or "").upper()
    if status == "RESOURCE_EXHAUSTED":
        return True
    return "429" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc).upper()


async def _call_gemini_grounded(prompt: str, *, system_instruction: str, max_output_tokens: int = 3072):
    """Rate-limited + retried Gemini call with Google Search grounding.
    Returns the raw genai response (caller reads .text and .usage_metadata)."""
    from google import genai
    from google.genai import types
    from config import get_settings
    from utils.rate_limiter import backoff_with_jitter, get_rate_limiter

    settings = get_settings()
    client = genai.Client(api_key=settings.gemini_api_key)
    limiter = await get_rate_limiter(_GEMINI_RATE_LIMITER_KEY, *_GEMINI_NEWS_RATE)

    last_exc: Exception | None = None
    for attempt in range(_GEMINI_RETRY_MAX):
        await limiter.acquire()
        try:
            return await client.aio.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.1,
                    max_output_tokens=max_output_tokens,
                ),
            )
        except Exception as e:
            last_exc = e
            if attempt >= _GEMINI_RETRY_MAX - 1:
                raise
            if _is_gemini_rate_limit_error(e):
                retry_delay = _extract_gemini_retry_delay_seconds(e)
                wait = retry_delay if retry_delay is not None else backoff_with_jitter(attempt)
                logger.warning(f"[news_research] Gemini 429 on attempt {attempt + 1}, retry in {wait:.1f}s")
            else:
                wait = backoff_with_jitter(attempt)
                logger.warning(f"[news_research] Gemini call failed on attempt {attempt + 1}, retry in {wait:.1f}s: {e}")
            await asyncio.sleep(wait)
    raise last_exc  # pragma: no cover


async def _log_gemini_usage(resp, *, account_id, campaign_id, feature, t0):
    """Fire-and-forget cost logging, mirrors company_sourcing_service."""
    try:
        from services.openrouter_service import _record_openrouter_usage
        usage_meta = getattr(resp, "usage_metadata", None)
        prompt_tok = getattr(usage_meta, "prompt_token_count", 0) or 0
        compl_tok = getattr(usage_meta, "candidates_token_count", 0) or 0
        dur_ms = int((asyncio.get_event_loop().time() - t0) * 1000)
        asyncio.create_task(_record_openrouter_usage(
            model=_GEMINI_MODEL,
            prompt_tokens=prompt_tok,
            completion_tokens=compl_tok,
            duration_ms=dur_ms,
            account_id=account_id,
            campaign_id=campaign_id,
            feature=feature,
        ))
    except Exception:
        pass


def _use_gemini() -> bool:
    from config import get_settings
    return (get_settings().research_provider or "gemini").strip().lower() == "gemini"


async def research_company_news(
    company_name: str,
    *,
    limit: int = 5,
    days_back: int = 90,
    client: Optional[OpenRouterClient] = None,
    account_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    feature: str = "company_research",
) -> list[dict]:
    """
    Research recent news for a company using AI with web search.

    Returns up to `limit` CompanyNewsItem-compatible dicts:
      title, url, published_date (ISO 8601), source, summary (≤200 chars), sentiment

    Provider is selected via settings.research_provider ("gemini" default,
    "perplexity" fallback) — see module docstring. Same output shape either way.
    account_id / campaign_id / feature are cost tags (OpenRouter or Gemini usage log).
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

    if _use_gemini():
        try:
            t0 = asyncio.get_event_loop().time()
            resp = await _call_gemini_grounded(
                prompt,
                system_instruction="You are a business news researcher. Return results as a JSON array only, no additional text.",
                max_output_tokens=2048,
            )
            await _log_gemini_usage(resp, account_id=account_id, campaign_id=campaign_id, feature=feature, t0=t0)
            content = _strip_citations(resp.text or "") if resp else ""
        except Exception as e:
            logger.error(f"Gemini news research failed for {company_name}: {e}", exc_info=True)
            return []

        try:
            news_data = extract_json(content)
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Could not parse JSON from Gemini news response for {company_name}: {content[:300]}")
            return []

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

        logger.info(f"Found {len(normalized)} news items for {company_name} (gemini)")
        return normalized

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
            account_id=account_id,
            campaign_id=campaign_id,
            feature=feature,
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


def _empty_news_signals() -> dict:
    """Empty defaults matching the research_company_news_and_signals contract."""
    return {
        "news": [],
        "buying_signals": [],
        "funding": {
            "latest_round": None,
            "amount": None,
            "date": None,
            "investors": [],
            "summary": None,
        },
        "hiring_signals": [],
        "tech_stack": [],
        "recent_launches": [],
    }


def _clean_str_list(raw, limit: int) -> list[str]:
    """Defensively coerce a model-returned list into a bounded list[str]."""
    if not isinstance(raw, list):
        return []
    return [
        str(s).strip() for s in raw
        if isinstance(s, (str, int, float)) and str(s).strip()
    ][:limit]


def _parse_news_signals_payload(data: dict, limit: int) -> dict:
    """Shared parser: raw {"news","buying_signals","funding",...} JSON dict ->
    the research_company_news_and_signals output shape. Used by both the
    Gemini and Perplexity paths so output shape stays identical."""
    result = _empty_news_signals()

    raw_news = data.get("news") or []
    news: list[dict] = []
    if isinstance(raw_news, list):
        for item in raw_news[:limit]:
            if not isinstance(item, dict):
                continue
            title = (item.get("title") or "").strip()
            if not title:
                continue
            news.append({
                "title": title,
                "url": item.get("url") or "",
                "published_date": item.get("published_date") or item.get("date") or "",
                "source": item.get("source") or "",
                "summary": (item.get("summary") or "")[:200],
                "sentiment": item.get("sentiment") or "neutral",
            })
    result["news"] = news

    result["buying_signals"] = _clean_str_list(data.get("buying_signals"), 6)
    result["hiring_signals"] = _clean_str_list(data.get("hiring_signals"), 6)
    result["tech_stack"] = _clean_str_list(data.get("tech_stack"), 8)
    result["recent_launches"] = _clean_str_list(data.get("recent_launches"), 4)

    raw_funding = data.get("funding")
    if isinstance(raw_funding, dict):
        def _opt_str(v):
            s = str(v).strip() if v is not None and not isinstance(v, (list, dict)) else ""
            return s[:120] or None
        result["funding"] = {
            "latest_round": _opt_str(raw_funding.get("latest_round") or raw_funding.get("round")),
            "amount": _opt_str(raw_funding.get("amount")),
            "date": _opt_str(raw_funding.get("date")),
            "investors": _clean_str_list(raw_funding.get("investors"), 5),
            "summary": _opt_str(raw_funding.get("summary")),
        }

    return result


async def research_company_news_and_signals(
    company_name: str,
    *,
    limit: int = 5,
    days_back: int = 90,
    client: Optional[OpenRouterClient] = None,
    account_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    feature: str = "company_research",
    raise_on_error: bool = False,
) -> dict:
    """
    Combined news + buying-signals + funding + hiring + tech + launches research
    in ONE Perplexity call (cost stays flat vs. the old news+signals call).

    Returns a dict (missing/unparseable sections fall back to empty defaults):
      news:            same shape as research_company_news
      buying_signals:  list[str] — hiring surges, funding, expansion, tech
                       adoption, leadership changes, product launches
      funding:         {latest_round, amount, date, investors: [], summary}
      hiring_signals:  list[str] — roles/teams actively being hired (buying intent)
      tech_stack:      list[str]
      recent_launches: list[str]

    raise_on_error: when True, transport/AI failures raise so the caller can
    retry and record the failure (company_research_service). Default False keeps
    the legacy fail-soft behavior (empty defaults).
    """
    prompt = f"""Research {company_name} and return the following.

PART 1 — "news": the top {limit} most recent, significant business news items from the last {days_back} days.
Focus only on material events: funding rounds, acquisitions/M&A, leadership changes, product launches, partnerships, expansions, layoffs, regulatory news, awards, or major customer wins. Exclude marketing fluff, minor blog posts, or social posts.

PART 2 — "buying_signals": up to 6 current BUYING SIGNALS: concrete, verifiable indicators that the company may be ready to buy B2B services. Look for hiring surges (open roles, team growth), recent funding, geographic or product expansion, new technology adoption, leadership changes, and product launches. Each signal must be one specific sentence with the concrete fact (no generic statements).

PART 3 — "funding": the company's latest known funding round (any time, not just {days_back} days): round name (e.g. "Series B"), amount raised, announcement date, lead/notable investors, and a one-sentence summary. Use null for unknown fields; if the company has no known funding, use nulls and empty investors.

PART 4 — "hiring_signals": up to 6 roles or teams the company is actively hiring for right now (job boards, LinkedIn jobs, careers page). Each entry one short phrase like "Hiring 4 enterprise AEs" or "Open Head of Growth role". These indicate buying intent.

PART 5 — "tech_stack": up to 8 technologies/tools the company is known to use or build on (from job posts, engineering blogs, integrations pages).

PART 6 — "recent_launches": up to 4 products, features, or markets launched in the last 12 months, each one short phrase.

Return ONLY a JSON object (no extra text):
{{
  "news": [
    {{
      "title": "Short news headline",
      "url": "https://...",
      "published_date": "YYYY-MM-DD",
      "source": "Publication name",
      "summary": "1-2 sentence summary (max 200 chars)",
      "sentiment": "positive|neutral|negative"
    }}
  ],
  "buying_signals": ["...", "..."],
  "funding": {{
    "latest_round": "Series B" or null,
    "amount": "$30M" or null,
    "date": "YYYY-MM" or null,
    "investors": ["...", "..."],
    "summary": "One sentence" or null
  }},
  "hiring_signals": ["...", "..."],
  "tech_stack": ["...", "..."],
  "recent_launches": ["...", "..."]
}}

If nothing is found for a part, use an empty array (or nulls for funding fields)."""

    if _use_gemini():
        result = _empty_news_signals()
        try:
            t0 = asyncio.get_event_loop().time()
            resp = await _call_gemini_grounded(
                prompt,
                system_instruction="You are a business research analyst. Return results as a single JSON object only, no additional text.",
                max_output_tokens=3072,
            )
            await _log_gemini_usage(resp, account_id=account_id, campaign_id=campaign_id, feature=feature, t0=t0)
            content = _strip_citations(resp.text or "") if resp else ""

            try:
                data = extract_json(content)
            except (json.JSONDecodeError, ValueError):
                logger.warning(
                    f"Could not parse JSON from Gemini news+signals response for {company_name}: {content[:300]}"
                )
                if raise_on_error:
                    raise ValueError(f"unparseable gemini news+signals response: {content[:200]}")
                return result

            if not isinstance(data, dict):
                if raise_on_error:
                    raise ValueError(f"gemini news+signals response not a JSON object: {type(data)}")
                return result

            result = _parse_news_signals_payload(data, limit)
            logger.info(
                f"Found {len(result['news'])} news + {len(result['buying_signals'])} buying signals + "
                f"{len(result['hiring_signals'])} hiring signals for {company_name} (gemini)"
                + (f" (funding: {result['funding']['latest_round']})" if result["funding"]["latest_round"] else "")
            )
            return result
        except Exception as e:
            logger.error(f"Gemini news+signals research failed for {company_name}: {e}", exc_info=True)
            if raise_on_error:
                raise
            return result

    owns_client = client is None
    if owns_client:
        client = OpenRouterClient()

    result = _empty_news_signals()
    try:
        response = await client.chat_completion(
            messages=[
                {
                    "role": "system",
                    "content": "You are a business research analyst. Return results as a single JSON object only, no additional text.",
                },
                {"role": "user", "content": prompt},
            ],
            model=_NEWS_MODEL,
            temperature=0.1,
            max_tokens=3072,
            account_id=account_id,
            campaign_id=campaign_id,
            feature=feature,
        )

        content = response.get("content", "") if isinstance(response, dict) else ""
        try:
            data = extract_json(content)
        except (json.JSONDecodeError, ValueError):
            logger.warning(
                f"Could not parse JSON from news+signals response for {company_name}: {content[:300]}"
            )
            if raise_on_error:
                raise ValueError(f"unparseable news+signals response: {content[:200]}")
            return result

        if not isinstance(data, dict):
            if raise_on_error:
                raise ValueError(f"news+signals response not a JSON object: {type(data)}")
            return result

        raw_news = data.get("news") or []
        news: list[dict] = []
        if isinstance(raw_news, list):
            for item in raw_news[:limit]:
                if not isinstance(item, dict):
                    continue
                title = (item.get("title") or "").strip()
                if not title:
                    continue
                news.append({
                    "title": title,
                    "url": item.get("url") or "",
                    "published_date": item.get("published_date") or item.get("date") or "",
                    "source": item.get("source") or "",
                    "summary": (item.get("summary") or "")[:200],
                    "sentiment": item.get("sentiment") or "neutral",
                })
        result["news"] = news

        result["buying_signals"] = _clean_str_list(data.get("buying_signals"), 6)
        result["hiring_signals"] = _clean_str_list(data.get("hiring_signals"), 6)
        result["tech_stack"] = _clean_str_list(data.get("tech_stack"), 8)
        result["recent_launches"] = _clean_str_list(data.get("recent_launches"), 4)

        raw_funding = data.get("funding")
        if isinstance(raw_funding, dict):
            def _opt_str(v):
                s = str(v).strip() if v is not None and not isinstance(v, (list, dict)) else ""
                return s[:120] or None
            result["funding"] = {
                "latest_round": _opt_str(raw_funding.get("latest_round") or raw_funding.get("round")),
                "amount": _opt_str(raw_funding.get("amount")),
                "date": _opt_str(raw_funding.get("date")),
                "investors": _clean_str_list(raw_funding.get("investors"), 5),
                "summary": _opt_str(raw_funding.get("summary")),
            }

        logger.info(
            f"Found {len(result['news'])} news + {len(result['buying_signals'])} buying signals + "
            f"{len(result['hiring_signals'])} hiring signals for {company_name}"
            + (f" (funding: {result['funding']['latest_round']})" if result["funding"]["latest_round"] else "")
        )
        return result

    except Exception as e:
        logger.error(f"News+signals research failed for {company_name}: {e}", exc_info=True)
        if raise_on_error:
            raise
        return result

    finally:
        if owns_client:
            await client.close()
