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

from utils.rate_limiter import backoff_with_jitter, get_rate_limiter

logger = logging.getLogger(__name__)


def _strip_citations(text: str) -> str:
    """Remove inline citation markers like [1], [2], [1,2] before JSON parsing."""
    return re.sub(r'\[\d+(?:,\s*\d+)*\]', '', text)

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_MAX_BATCHES = 15
_TARGET_PER_BATCH = 50

# Conservative default budget for direct Gemini SDK calls from this service.
# There's no dedicated settings knob for this yet — fan-out here is bounded
# (max_concurrency callers use 4, up to _MAX_BATCHES=15 batches per sourcing
# run), so a flat per-process limit is enough to keep bursts from tripping
# Gemini's own per-minute quota.
_GEMINI_SOURCING_RATE = (150, 60)  # (max_calls, window_seconds) -> ~150 req/min
_GEMINI_RETRY_MAX = 3
_GEMINI_RATE_LIMITER_KEY = f"gemini-sourcing:{_GEMINI_MODEL}"


def _extract_gemini_retry_delay_seconds(exc: Exception) -> float | None:
    """Best-effort extraction of a server-suggested retry delay from a
    google.genai exception (e.g. a 429 ResourceExhausted). The SDK doesn't
    expose a stable typed field for this across versions, so this is
    defensive: try known attribute shapes first, then fall back to a regex
    over the string form of the error. Returns None if nothing usable is found.
    """
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


_BATCH_DIVERSITY_ANGLES = [
    "Focus on companies whose names start with letters A-G.",
    "Focus on companies whose names start with letters H-N.",
    "Focus on companies whose names start with letters O-T.",
    "Focus on companies whose names start with letters U-Z.",
    "Focus on smaller, lesser-known companies (under 500 employees) that are NOT household names.",
    "Focus on recently-founded companies (started in the last 5 years).",
]


def _build_prompt(
    icp_prompt: str,
    count: int,
    exclude_names: list[str],
    angle: str,
    structured_hints: str | None = None,
    negative_hints: list[str] | None = None,
) -> str:
    exclude_block = (
        f"\n\nEXCLUDE these companies (already in the list):\n{', '.join(exclude_names[:200])}"
        if exclude_names else ""
    )
    structured_block = f"\n\nHARD REQUIREMENTS (every company must satisfy these):\n{structured_hints}" if structured_hints else ""
    negative_block = (
        "\n\nDO NOT include companies like these (rejected in earlier batches for poor ICP fit):\n- "
        + "\n- ".join(negative_hints[:20])
        if negative_hints else ""
    )
    return f"""Find {count} real, currently-operating companies that match this ICP:

{icp_prompt}{structured_block}{negative_block}

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
    structured_hints: str | None = None,
    negative_hints: list[str] | None = None,
    on_batch=None,
) -> tuple[list[dict], dict]:
    """
    Best-effort source up to `target_count` companies.
    Returns (companies, meta) where meta = {batches_run, raw_received, after_dedup, after_url_validation}.

    max_concurrency: how many Gemini batches to fire in parallel. Default=1 (sequential, prod-safe).
    Set to 4-8 from scripts/campaigns to cut sourcing wall-clock ~4-8x with no behavioral change.
    structured_hints: extra hard-requirement lines (canonical industry/size/geo) appended to every batch prompt.
    negative_hints: short "avoid companies like X (reason)" lines fed back into every batch prompt.
    on_batch: optional async callback invoked with the list of NEWLY accepted (deduped, normalized)
    company dicts after each Gemini batch completes — enables incremental UI persistence. Callback
    exceptions are swallowed; note URL validation runs after all batches, so `linkedin_url_validated`
    is not yet set on dicts passed to on_batch.
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

    async def _call_gemini_with_retry(prompt: str):
        """Rate-limited + retried call to the Gemini grounded-search endpoint.

        Acquires the shared sliding-window limiter before every attempt (including
        retries) so a burst of retries can't itself blow through the budget, then
        retries transient/429 failures up to _GEMINI_RETRY_MAX times with jittered
        exponential backoff, honoring the server's suggested retry delay when the
        SDK exposes one.
        """
        limiter = await get_rate_limiter(_GEMINI_RATE_LIMITER_KEY, *_GEMINI_SOURCING_RATE)
        last_exc: Exception | None = None
        for attempt in range(_GEMINI_RETRY_MAX):
            await limiter.acquire()
            try:
                return await gemini_client.aio.models.generate_content(
                    model=_GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        system_instruction="You are a B2B research assistant. Return ONLY valid JSON.",
                        tools=[types.Tool(google_search=types.GoogleSearch())],
                        temperature=0.2,
                        max_output_tokens=8192,
                    ),
                )
            except Exception as e:
                last_exc = e
                if attempt >= _GEMINI_RETRY_MAX - 1:
                    raise
                if _is_gemini_rate_limit_error(e):
                    retry_delay = _extract_gemini_retry_delay_seconds(e)
                    wait = retry_delay if retry_delay is not None else backoff_with_jitter(attempt)
                    logger.warning(
                        f"[curated] Gemini 429/rate-limited on attempt {attempt + 1}, retry in {wait:.1f}s"
                    )
                else:
                    # Treat other transient errors (timeouts, transport hiccups) the
                    # same way — retry with backoff rather than failing the batch.
                    wait = backoff_with_jitter(attempt)
                    logger.warning(
                        f"[curated] Gemini call failed on attempt {attempt + 1}, retry in {wait:.1f}s: {e}"
                    )
                await asyncio.sleep(wait)
        raise last_exc  # pragma: no cover — loop always returns or raises above

    async def _run_one_batch(batch_index: int, req_count: int, angle: str, exclude_list: list[str]) -> list[dict]:
        """Run a single Gemini grounded sourcing call. Returns raw company list or []."""
        prompt = _build_prompt(
            icp_prompt, req_count, exclude_list, angle,
            structured_hints=structured_hints, negative_hints=negative_hints,
        )
        try:
            _t0 = asyncio.get_event_loop().time()
            resp = await _call_gemini_with_retry(prompt)
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

    def _dedup_and_append(companies_raw: list[dict], batch_index: int) -> list[dict]:
        """Dedup incoming companies against seen sets and append to accumulated (mutates shared state).
        Returns the list of newly accepted normalized dicts (for the on_batch callback)."""
        meta["batches_run"] += 1
        meta["raw_received"] += len(companies_raw)
        _new: list[dict] = []
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
            _norm = {
                "company_name": name,
                "company_linkedin_url": linkedin,
                "company_domain": (c.get("domain") or "").strip().lower() or None,
                "company_website": c.get("website") or None,
                "industry": c.get("industry") or None,
                "country": c.get("country") or None,
                "employee_size_estimate": c.get("employee_size_estimate") or None,
                "description": c.get("description") or None,
                "citation_urls": [],
            }
            accumulated.append(_norm)
            _new.append(_norm)
        return _new

    async def _emit_batch(new_items: list[dict]) -> None:
        """Fire the on_batch callback fail-soft — incremental persistence must never sink sourcing."""
        if on_batch is None or not new_items:
            return
        try:
            await on_batch(new_items)
        except Exception as _cb_e:
            logger.warning(f"[curated] on_batch callback failed (continuing): {_cb_e}")

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
                await _emit_batch(_dedup_and_append(companies_raw, i))
    else:
        # Parallel path: fire up to max_concurrency batches simultaneously and process
        # each result AS IT COMPLETES (as_completed, not gather) so on_batch streams
        # companies to the UI while slower batches are still running. Duplicates across
        # concurrent batches are harmless (dropped by seen_names/seen_urls).
        sem = asyncio.Semaphore(max_concurrency)
        remaining = target_count - len(accumulated)
        # Fire enough batches to cover 2x the target with diversity angles
        n_batches = min(_MAX_BATCHES, max(1, (remaining * 2 + _TARGET_PER_BATCH - 1) // _TARGET_PER_BATCH))
        req_count = min(_TARGET_PER_BATCH, max(remaining // max(n_batches, 1) * 2, 20))
        exclude_snapshot = list(seen_names)  # snapshot; concurrent batches share the same exclude list

        async def _bounded_batch(i: int) -> tuple[int, list[dict]]:
            async with sem:
                angle = _BATCH_DIVERSITY_ANGLES[i % len(_BATCH_DIVERSITY_ANGLES)]
                return i, await _run_one_batch(i, req_count, angle, exclude_snapshot)

        for fut in asyncio.as_completed([_bounded_batch(i) for i in range(n_batches)]):
            i, companies_raw = await fut
            if companies_raw:
                await _emit_batch(_dedup_and_append(companies_raw, i))

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
