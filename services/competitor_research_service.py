"""
Competitor research. Identifies top competitors and their differentiation.

Provider selection (settings.research_provider, env RESEARCH_PROVIDER):
  "gemini" (default) — Gemini + Google Search grounding, direct google.genai
      SDK call. Cheap (~$0.075/$0.30 per 1M tokens). Gemini helper code
      mirrors services/company_sourcing_service.py; duplicated rather than
      imported so this module stays self-contained (same pattern as
      company_gate_service.py / title_gate_service.py).
  "perplexity" — original path: Perplexity Sonar Pro via OpenRouter. Kept
      intact as a fallback/opt-out; far more expensive.

Both paths return the SAME output shape (see research_competitors docstring)
and are fail-soft unless raise_on_error=True.
"""

import asyncio
import logging
import re
from typing import Optional
from services.openrouter_service import OpenRouterClient, extract_json

logger = logging.getLogger(__name__)

# ── Gemini grounding path (mirrors services/company_sourcing_service.py) ────

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_GEMINI_RETRY_MAX = 3
_GEMINI_COMPETITOR_RATE = (150, 60)  # (max_calls, window_seconds) -> ~150 req/min
_GEMINI_RATE_LIMITER_KEY = f"gemini-competitor-research:{_GEMINI_MODEL}"


def _strip_citations(text: str) -> str:
    """Remove inline citation markers like [1], [2], [1,2] before JSON parsing."""
    return re.sub(r'\[\d+(?:,\s*\d+)*\]', '', text)


def _extract_gemini_retry_delay_seconds(exc: Exception) -> float | None:
    """Best-effort extraction of a server-suggested retry delay. Mirrors
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


async def _call_gemini_grounded(prompt: str, *, system_instruction: str, max_output_tokens: int = 2048):
    """Rate-limited + retried Gemini call with Google Search grounding.
    Returns the raw genai response (caller reads .text and .usage_metadata)."""
    from google import genai
    from google.genai import types
    from config import get_settings
    from utils.rate_limiter import backoff_with_jitter, get_rate_limiter

    settings = get_settings()
    client = genai.Client(api_key=settings.gemini_api_key)
    limiter = await get_rate_limiter(_GEMINI_RATE_LIMITER_KEY, *_GEMINI_COMPETITOR_RATE)

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
                    temperature=0.2,
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
                logger.warning(f"[competitor_research] Gemini 429 on attempt {attempt + 1}, retry in {wait:.1f}s")
            else:
                wait = backoff_with_jitter(attempt)
                logger.warning(f"[competitor_research] Gemini call failed on attempt {attempt + 1}, retry in {wait:.1f}s: {e}")
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


def _normalize_competitors(competitors_data, limit: int) -> list[dict]:
    """Shared normalizer: raw model output -> the research_competitors output
    shape, including the exactly-one-best-performer enforcement. Used by both
    the Gemini and Perplexity paths so output shape stays identical."""
    if not isinstance(competitors_data, list):
        if isinstance(competitors_data, dict) and "competitors" in competitors_data:
            competitors_data = competitors_data["competitors"]
        else:
            competitors_data = [competitors_data] if isinstance(competitors_data, dict) else []

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

    flagged = [c for c in normalized if c["is_best_performer"]]
    if len(flagged) != 1:
        best = flagged[0] if flagged else next(
            (c for c in normalized if c.get("why_winning")), None
        )
        for c in normalized:
            c["is_best_performer"] = c is best

    return normalized


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

    if _use_gemini():
        try:
            t0 = asyncio.get_event_loop().time()
            resp = await _call_gemini_grounded(
                prompt,
                system_instruction="You are a competitive intelligence researcher. Return results as a JSON array only, no additional text.",
                max_output_tokens=2048,
            )
            await _log_gemini_usage(resp, account_id=account_id, campaign_id=campaign_id, feature=feature, t0=t0)
            content = _strip_citations(resp.text or "") if resp else ""
            logger.debug(f"Raw Gemini competitor research response for {company_name}: {content[:500]}")

            import json  # needed for json.JSONDecodeError
            try:
                competitors_data = extract_json(content)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Could not parse JSON from Gemini competitor response for {company_name}, content: {content[:300]}")
                competitors_data = []

            normalized = _normalize_competitors(competitors_data, limit)
            logger.info(f"Found {len(normalized)} competitors for {company_name} (gemini)")
            return normalized
        except Exception as e:
            logger.error(f"Gemini competitor research failed for {company_name}: {e}", exc_info=True)
            if raise_on_error:
                raise
            return []

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
