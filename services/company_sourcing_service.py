"""
Curated discovery — Phase A.
Uses Gemini Flash-Lite with Google Search grounding to source companies matching a free-text ICP,
with paginated/diversified retries to reach the target count.
"""

import asyncio
import json
import logging
import re
from typing import Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


def _strip_citations(text: str) -> str:
    """Remove inline citation markers like [1], [2], [1,2] before JSON parsing."""
    return re.sub(r'\[\d+(?:,\s*\d+)*\]', '', text)

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_MAX_BATCHES = 15
_TARGET_PER_BATCH = 50
_BATCH_DIVERSITY_ANGLES = [
    "Focus on companies whose names start with letters A-G.",
    "Focus on companies whose names start with letters H-N.",
    "Focus on companies whose names start with letters O-T.",
    "Focus on companies whose names start with letters U-Z.",
    "Focus on smaller, lesser-known companies (under 500 employees) that are NOT household names.",
    "Focus on recently-founded companies (started in the last 5 years).",
]


def _build_prompt(icp_prompt: str, count: int, exclude_names: list[str], angle: str) -> str:
    exclude_block = (
        f"\n\nEXCLUDE these companies (already in the list):\n{', '.join(exclude_names[:200])}"
        if exclude_names else ""
    )
    return f"""Find {count} real, currently-operating companies that match this ICP:

{icp_prompt}

{angle}

For each company, return:
- name: official company name
- linkedin_url: full LinkedIn company URL (must contain /company/)
- domain: primary website domain (no scheme, e.g., "stripe.com")
- website: full website URL
- industry: industry/sector
- country: HQ country
- employee_size_estimate: rough size like "11-50", "51-200", "201-1000", "1000+"
- description: 1-2 sentence summary of what they do and why they match the ICP

Return ONLY a JSON object — no commentary, no markdown fences — exactly this shape:
{{"companies": [{{"name": "...", "linkedin_url": "...", "domain": "...", "website": "...", "industry": "...", "country": "...", "employee_size_estimate": "...", "description": "..."}}]}}

If you cannot find {count} matching companies in this batch, return fewer rather than fabricating. Verify each LinkedIn URL by reading the company page in your search results — do not invent URLs.{exclude_block}"""


def _normalize_linkedin_url(url: str | None) -> str | None:
    if not url:
        return None
    u = url.strip()
    if not u.startswith("http"):
        u = "https://" + u.lstrip("/")
    try:
        p = urlparse(u)
    except Exception:
        return None
    if "linkedin.com" not in (p.netloc or "").lower():
        return None
    if "/company/" not in (p.path or "").lower():
        return None
    return f"https://www.linkedin.com{p.path.rstrip('/')}"


async def _validate_linkedin_url_reachable(url: str, client: httpx.AsyncClient) -> bool:
    try:
        r = await client.head(url, follow_redirects=True, timeout=10.0)
        if r.status_code == 404:
            return False
        return r.status_code < 500
    except Exception:
        return False


async def source_companies(
    *,
    icp_prompt: str,
    target_count: int,
    exclude_names: list[str] | None = None,
    account_id: str | None = None,
    campaign_id: str | None = None,
    validate_urls: bool = True,
    max_concurrency: int = 1,
) -> tuple[list[dict], dict]:
    """
    Best-effort source up to `target_count` companies.
    Returns (companies, meta) where meta = {batches_run, raw_received, after_dedup, after_url_validation}.

    max_concurrency: how many Gemini batches to fire in parallel. Default=1 (sequential, prod-safe).
    Set to 4-5 from scripts/campaigns to cut sourcing wall-clock ~4x with no behavioral change.
    """
    from google import genai
    from google.genai import types
    from config import get_settings

    settings = get_settings()
    gemini_client = genai.Client(api_key=settings.gemini_api_key)

    seen_names: set[str] = {n.lower() for n in (exclude_names or [])}
    seen_urls: set[str] = set()
    accumulated: list[dict] = []
    meta = {"batches_run": 0, "raw_received": 0, "after_dedup": 0, "after_url_validation": 0}

    async def _run_one_batch(batch_index: int, req_count: int, angle: str, exclude_list: list[str]) -> list[dict]:
        """Run a single Gemini grounded sourcing call. Returns raw company list or []."""
        prompt = _build_prompt(icp_prompt, req_count, exclude_list, angle)
        try:
            _t0 = asyncio.get_event_loop().time()
            resp = await gemini_client.aio.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction="You are a B2B research assistant. Return ONLY valid JSON.",
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.2,
                    max_output_tokens=8192,
                ),
            )
            # Track Gemini usage (normally bypassed by openrouter_service)
            try:
                from services.openrouter_service import _record_openrouter_usage
                _usage_meta = getattr(resp, "usage_metadata", None)
                _prompt_tok = getattr(_usage_meta, "prompt_token_count", 0) or 0
                _compl_tok = getattr(_usage_meta, "candidates_token_count", 0) or 0
                _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
                asyncio.create_task(_record_openrouter_usage(
                    model=_GEMINI_MODEL,
                    prompt_tokens=_prompt_tok,
                    completion_tokens=_compl_tok,
                    duration_ms=_dur_ms,
                    account_id=account_id,
                    campaign_id=campaign_id,
                    feature="company_sourcing",
                ))
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"[curated] Gemini batch {batch_index + 1} failed: {e}")
            return []

        raw_content = resp.text if resp else None
        if not raw_content:
            logger.warning(f"[curated] Gemini batch {batch_index + 1} — empty content in response")
            return []

        clean = _strip_citations(raw_content)
        try:
            from services.openrouter_service import extract_json
            parsed = extract_json(clean)
        except Exception as e:
            logger.warning(
                f"[curated] Gemini batch {batch_index + 1} — could not parse response as JSON: {e!r}. "
                f"Raw (first 400): {raw_content[:400]!r}"
            )
            return []

        if not isinstance(parsed, dict):
            logger.warning(f"[curated] Gemini batch {batch_index + 1} — expected dict, got {type(parsed).__name__}")
            return []
        companies_raw = parsed.get("companies") or parsed.get("results") or []
        if isinstance(companies_raw, str):
            try:
                companies_raw = json.loads(companies_raw)
            except Exception:
                companies_raw = []
        if not isinstance(companies_raw, list):
            return []
        return companies_raw

    def _dedup_and_append(companies_raw: list[dict], batch_index: int) -> None:
        """Dedup incoming companies against seen sets and append to accumulated (mutates shared state)."""
        meta["batches_run"] += 1
        meta["raw_received"] += len(companies_raw)
        for c in companies_raw:
            if not isinstance(c, dict):
                continue
            name = (c.get("name") or "").strip()
            if not name:
                continue
            name_key = name.lower()
            if name_key in seen_names:
                continue
            linkedin = _normalize_linkedin_url(c.get("linkedin_url"))
            if linkedin and linkedin in seen_urls:
                continue
            seen_names.add(name_key)
            if linkedin:
                seen_urls.add(linkedin)
            accumulated.append({
                "company_name": name,
                "company_linkedin_url": linkedin,
                "company_domain": (c.get("domain") or "").strip().lower() or None,
                "company_website": c.get("website") or None,
                "industry": c.get("industry") or None,
                "country": c.get("country") or None,
                "employee_size_estimate": c.get("employee_size_estimate") or None,
                "description": c.get("description") or None,
                "citation_urls": [],
            })

    if max_concurrency <= 1:
        # Sequential path (production default — unchanged behavior)
        for i in range(_MAX_BATCHES):
            if len(accumulated) >= target_count:
                break
            angle = _BATCH_DIVERSITY_ANGLES[i % len(_BATCH_DIVERSITY_ANGLES)]
            remaining = target_count - len(accumulated)
            req_count = min(_TARGET_PER_BATCH, max(remaining * 2, 20))
            companies_raw = await _run_one_batch(i, req_count, angle, list(seen_names))
            if companies_raw:
                _dedup_and_append(companies_raw, i)
    else:
        # Parallel path: fire up to max_concurrency batches simultaneously.
        # We don't need sequential dedup precision here; duplicates across concurrent
        # batches are harmless (they're dropped by seen_names/seen_urls in _dedup_and_append).
        sem = asyncio.Semaphore(max_concurrency)
        remaining = target_count - len(accumulated)
        # Fire enough batches to cover 2x the target with diversity angles
        n_batches = min(_MAX_BATCHES, max(1, (remaining * 2 + _TARGET_PER_BATCH - 1) // _TARGET_PER_BATCH))
        req_count = min(_TARGET_PER_BATCH, max(remaining // max(n_batches, 1) * 2, 20))
        exclude_snapshot = list(seen_names)  # snapshot; concurrent batches share the same exclude list

        async def _bounded_batch(i: int) -> list[dict]:
            async with sem:
                angle = _BATCH_DIVERSITY_ANGLES[i % len(_BATCH_DIVERSITY_ANGLES)]
                return await _run_one_batch(i, req_count, angle, exclude_snapshot)

        batch_results = await asyncio.gather(*[_bounded_batch(i) for i in range(n_batches)])
        for i, companies_raw in enumerate(batch_results):
            if companies_raw:
                _dedup_and_append(companies_raw, i)

    meta["after_dedup"] = len(accumulated)

    if validate_urls and accumulated:
        async def _false() -> bool:
            return False

        async with httpx.AsyncClient(timeout=15.0) as http:
            results = await asyncio.gather(
                *[
                    _validate_linkedin_url_reachable(c["company_linkedin_url"], http)
                    if c.get("company_linkedin_url")
                    else _false()
                    for c in accumulated
                ],
                return_exceptions=False,
            )
        for c, ok in zip(accumulated, results):
            c["linkedin_url_validated"] = bool(ok)
    else:
        for c in accumulated:
            c["linkedin_url_validated"] = False

    meta["after_url_validation"] = sum(1 for c in accumulated if c.get("linkedin_url_validated"))
    return accumulated, meta
