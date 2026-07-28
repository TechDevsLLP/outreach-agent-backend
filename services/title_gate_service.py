"""
AI title gate — semantic judge for prospect job titles.

Runs AFTER deterministic gating (utils/scoring.person_fit_gate) as a final
check on whether each candidate's job title actually describes a person in the
campaign's target role/function. Deterministic keyword matching cannot always
tell "Marketing Manager" from "District Merchandise Manager"; this gate asks
Gemini to judge the functional qualifier of each title.

FAIL-OPEN by design: any infrastructure failure (client init, timeout,
rate-limit exhaustion, malformed model output, missing row) defaults the
affected candidate to {"match": True, "reason": "gate_unavailable"}. The
pipeline must never lose prospects to gate infrastructure failure — only to
confident semantic rejections.

Gemini conventions (client init, 429 retry-delay parsing, rate limiting)
mirror services/company_sourcing_service.py. Helpers are duplicated rather
than imported so this module stays self-contained. No search grounding is
used here — plain JSON-mode judging only.
"""

import asyncio
import json
import logging
import re

from utils.rate_limiter import backoff_with_jitter, get_rate_limiter

logger = logging.getLogger(__name__)

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_BATCH_SIZE = 50
_CALL_TIMEOUT_SECONDS = 45.0
_GEMINI_RETRY_MAX = 3
# Flat per-process budget, same shape as company_sourcing_service.
_GEMINI_TITLE_GATE_RATE = (150, 60)  # (max_calls, window_seconds) -> ~150 req/min
_GEMINI_RATE_LIMITER_KEY = f"gemini-title-gate:{_GEMINI_MODEL}"

_FAIL_OPEN_REASON = "gate_unavailable"


def _fail_open() -> dict:
    return {"match": True, "reason": _FAIL_OPEN_REASON}


# ── Gemini error helpers (mirrored from company_sourcing_service) ────────────

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


# ── Prompt ───────────────────────────────────────────────────────────────────

def _build_prompt(batch: list[dict], icp: dict) -> str:
    target_titles = [str(t).strip() for t in (icp.get("job_titles") or []) if t and str(t).strip()]
    seniorities = [str(s).strip() for s in (icp.get("seniorities") or []) if s and str(s).strip()]
    functions = [str(f).strip() for f in (icp.get("functions") or []) if f and str(f).strip()]
    notes = str(icp.get("notes") or "").strip()

    icp_lines = [f"Target roles/titles: {', '.join(target_titles) or '(none specified)'}"]
    if functions:
        icp_lines.append(f"Target functional departments: {', '.join(functions)}")
    if seniorities:
        icp_lines.append(f"Target seniority levels: {', '.join(seniorities)}")
    if notes:
        icp_lines.append(f"Campaign targeting notes: {notes}")
    icp_block = "\n".join(icp_lines)

    # Candidate rows: headline included only when present.
    rows = []
    for c in batch:
        row = {"i": c["index"], "title": c.get("title") or ""}
        if c.get("headline"):
            row["headline"] = c["headline"]
        rows.append(row)
    candidates_block = json.dumps(rows, ensure_ascii=False)

    return f"""You are a strict B2B targeting judge. Given the target ICP below, decide for EACH candidate job title whether it describes a person in that target role/function.

TARGET ICP:
{icp_block}

Judging rules:
- Judge the FUNCTION qualifier of the title, not the seniority word. "Manager", "Director", "Lead" alone say nothing about function.
- A title matches only if the person actually works in one of the target roles/functions.
- Retail merchandising, allocation, planning, buying, visual display, and store operations are NOT marketing. Example: "District Merchandise Manager", "Allocation Manager", "Display Manager" do NOT match a "marketing manager" ICP.
- Adjacent-sounding but different functions do not match (e.g. "Account Manager" is not sales leadership, "Office Manager" is not operations leadership).
- When a title is genuinely ambiguous but plausibly in the target function, lean towards match=true.

CANDIDATES (JSON array; "i" is the candidate id):
{candidates_block}

Return ONLY a JSON array — no commentary, no markdown fences — with exactly one object per candidate:
[{{"i": <int>, "match": <true|false>, "reason": "<max 10 words>"}}]"""


# ── Model call ───────────────────────────────────────────────────────────────

async def _call_gemini_with_retry(gemini_client, prompt: str):
    """Rate-limited + retried Gemini call (JSON mode, no grounding).

    Acquires the shared sliding-window limiter before every attempt (including
    retries), then retries transient/429 failures up to _GEMINI_RETRY_MAX times
    with jittered exponential backoff, honoring the server's suggested retry
    delay when the SDK exposes one.
    """
    from google.genai import types

    limiter = await get_rate_limiter(_GEMINI_RATE_LIMITER_KEY, *_GEMINI_TITLE_GATE_RATE)
    last_exc: Exception | None = None
    for attempt in range(_GEMINI_RETRY_MAX):
        await limiter.acquire()
        try:
            return await gemini_client.aio.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction="You are a strict B2B targeting judge. Return ONLY valid JSON.",
                    temperature=0.1,
                    max_output_tokens=8192,
                    response_mime_type="application/json",
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
                    f"[title-gate] Gemini 429/rate-limited on attempt {attempt + 1}, retry in {wait:.1f}s"
                )
            else:
                wait = backoff_with_jitter(attempt)
                logger.warning(
                    f"[title-gate] Gemini call failed on attempt {attempt + 1}, retry in {wait:.1f}s: {e}"
                )
            await asyncio.sleep(wait)
    raise last_exc  # pragma: no cover — loop always returns or raises above


def _parse_rows(raw_content: str) -> list[dict]:
    """Parse the model's JSON array. Returns [] on any failure (fail-open)."""
    if not raw_content:
        return []
    try:
        from services.openrouter_service import extract_json
        parsed = extract_json(raw_content)
    except Exception:
        try:
            parsed = json.loads(raw_content.strip().strip("`"))
        except Exception:
            return []
    if isinstance(parsed, dict):
        # Model sometimes wraps the array: {"results": [...]}
        for key in ("results", "candidates", "rows", "items"):
            if isinstance(parsed.get(key), list):
                parsed = parsed[key]
                break
    if not isinstance(parsed, list):
        return []
    return [r for r in parsed if isinstance(r, dict)]


async def _record_gemini_usage(resp, account_id: str | None, campaign_id: str | None, started_at: float):
    """Log this Gemini call's token spend so it shows up in the cost portal.

    Direct-SDK Gemini calls bypass OpenRouter, so without this the title gate —
    which runs on every discovery batch — was unaccounted spend. Recorded into
    the same collection as OpenRouter usage (the model name identifies the
    provider).
    """
    try:
        from services.openrouter_service import _record_openrouter_usage
        usage = getattr(resp, "usage_metadata", None)
        if usage is None:
            return
        await _record_openrouter_usage(
            model=_GEMINI_MODEL,
            prompt_tokens=getattr(usage, "prompt_token_count", 0) or 0,
            completion_tokens=getattr(usage, "candidates_token_count", 0) or 0,
            duration_ms=int((asyncio.get_event_loop().time() - started_at) * 1000),
            account_id=account_id,
            campaign_id=campaign_id,
            feature="title_gate",
        )
    except Exception:
        pass


async def _judge_batch(
    gemini_client, batch: list[dict], icp: dict, batch_num: int,
    account_id: str | None = None, campaign_id: str | None = None,
) -> dict[int, dict]:
    """Judge one batch. Every index in `batch` gets a verdict; failures fail open."""
    verdicts: dict[int, dict] = {c["index"]: _fail_open() for c in batch}
    batch_indices = set(verdicts.keys())

    prompt = _build_prompt(batch, icp)
    _started_at = asyncio.get_event_loop().time()
    try:
        resp = await asyncio.wait_for(
            _call_gemini_with_retry(gemini_client, prompt), _CALL_TIMEOUT_SECONDS
        )
    except Exception as e:
        logger.warning(f"[title-gate] batch {batch_num} failed (fail-open for {len(batch)}): {e}")
        return verdicts

    await _record_gemini_usage(resp, account_id, campaign_id, _started_at)

    raw_content = getattr(resp, "text", None) if resp else None
    for row in _parse_rows(raw_content or ""):
        try:
            idx = int(row["i"])
            if idx not in batch_indices:
                continue
            match = row["match"]
            if not isinstance(match, bool):
                continue  # malformed row → stays fail-open
            reason = str(row.get("reason") or "").strip()[:120] or (
                "title matches target role" if match else "title outside target role"
            )
            verdicts[idx] = {"match": match, "reason": reason}
        except (KeyError, TypeError, ValueError):
            continue  # malformed row → stays fail-open

    kept = sum(1 for v in verdicts.values() if v["match"] and v["reason"] != _FAIL_OPEN_REASON)
    rejected = sum(1 for v in verdicts.values() if not v["match"])
    unavailable = sum(1 for v in verdicts.values() if v["reason"] == _FAIL_OPEN_REASON)
    logger.info(
        f"[title-gate] batch {batch_num}: {len(batch)} judged — kept {kept}, "
        f"rejected {rejected}, fail-open {unavailable}"
    )
    return verdicts


# ── Public API ───────────────────────────────────────────────────────────────

async def ai_title_gate(
    candidates: list[dict],
    icp: dict,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> dict[int, dict]:
    """AI semantic title gate. Call AFTER deterministic gating.

    Args:
        candidates: list of {"index": int, "title": str, "headline": str (optional)}.
        icp: {"job_titles": [...], "seniorities": [...], "functions": [...],
              "notes": str (optional, campaign targeting description)}.
        account_id / campaign_id: cost attribution tags for the usage log.

    Returns:
        {index: {"match": bool, "reason": str}} for EVERY input index.
        On any infrastructure failure the affected indices default to
        {"match": True, "reason": "gate_unavailable"} (fail-open).
    """
    results: dict[int, dict] = {}
    valid: list[dict] = []
    for c in candidates or []:
        if not isinstance(c, dict):
            continue
        try:
            idx = int(c.get("index"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        results[idx] = _fail_open()
        title = str(c.get("title") or "").strip()
        headline = str(c.get("headline") or "").strip()
        if not title and not headline:
            # Nothing to judge — fail open rather than reject on missing data.
            continue
        valid.append({"index": idx, "title": title, "headline": headline})

    if not valid:
        return results

    try:
        from google import genai
        from config import get_settings

        settings = get_settings()
        gemini_client = genai.Client(api_key=settings.gemini_api_key)
    except Exception as e:
        logger.warning(f"[title-gate] Gemini client unavailable, fail-open for all {len(valid)}: {e}")
        return results

    for batch_num, start in enumerate(range(0, len(valid), _BATCH_SIZE), start=1):
        batch = valid[start:start + _BATCH_SIZE]
        try:
            verdicts = await _judge_batch(
                gemini_client, batch, icp or {}, batch_num,
                account_id=account_id, campaign_id=campaign_id,
            )
            results.update(verdicts)
        except Exception as e:  # belt-and-braces: _judge_batch already fails open
            logger.warning(f"[title-gate] batch {batch_num} crashed (fail-open): {e}")

    return results
