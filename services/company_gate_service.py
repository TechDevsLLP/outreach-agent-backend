"""
AI company gate — semantic judge for company-ICP fit.

Runs AFTER the deterministic company score gate in curated discovery, on both
DB-matched (Stage A) and Gemini-sourced (Stage B) companies. The DB pool's
industry taxonomy cannot tell an oil major with gas-station retail
classification from a D2C consumer brand — this gate asks Gemini to judge
actual business-model fit against the campaign ICP (e.g. Chevron, bp, Whole
Foods, Best Buy all carry "retail" industry tags but are NOT D2C brands).

FAIL-OPEN by design for whole-batch/infra failures: a network error, total
Gemini outage, or a timeout with zero usable response always defaults the
affected companies to {"match": True, "reason": "gate_unavailable"} —
discovery must never stall, and a transient outage must never silently
zero-out an entire campaign's companies. This whole-batch behavior is NOT
configurable.

Per-company ambiguity (a company comes back in an otherwise-successful batch
but its verdict is unparseable or missing) is configurable via
`settings.company_gate_fail_closed` (env `COMPANY_GATE_FAIL_CLOSED`,
default False): when False (default) these ambiguous rows fail open exactly
as before; when True they fail CLOSED — dropped with reason
"gate_ambiguous_failclosed". Only confident semantic rejections, or
ambiguous rows under the fail-closed flag, drop companies.

Gemini conventions mirror services/title_gate_service.py (which mirrors
company_sourcing_service.py). Helpers duplicated to stay self-contained.
"""

import asyncio
import json
import logging
import re

from utils.rate_limiter import backoff_with_jitter, get_rate_limiter

logger = logging.getLogger(__name__)

_GEMINI_MODEL = "gemini-3.1-flash-lite"
_BATCH_SIZE = 40
_CALL_TIMEOUT_SECONDS = 45.0
_GEMINI_RETRY_MAX = 3
_GEMINI_COMPANY_GATE_RATE = (150, 60)  # ~150 req/min shared window
_GEMINI_RATE_LIMITER_KEY = f"gemini-company-gate:{_GEMINI_MODEL}"

_FAIL_OPEN_REASON = "gate_unavailable"
_AMBIGUOUS_FAIL_CLOSED_REASON = "gate_ambiguous_failclosed"


def _fail_open() -> dict:
    return {"match": True, "reason": _FAIL_OPEN_REASON}


def _fail_closed_ambiguous() -> dict:
    return {"match": False, "reason": _AMBIGUOUS_FAIL_CLOSED_REASON}


def normalize_company_name(name: str | None) -> str:
    """Canonical key for name-level dedup: lowercase, strip punctuation and
    connective noise so "Barnes & Noble" == "Barnes andNoble" == "barnes-noble".
    """
    if not name:
        return ""
    s = str(name).lower()
    s = re.sub(r"[&+]", " and ", s)
    s = re.sub(r"\b(inc|llc|ltd|corp|corporation|co|company|the)\b\.?", " ", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


# ── Gemini error helpers (mirrored from title_gate_service) ──────────────────

def _extract_gemini_retry_delay_seconds(exc: Exception) -> float | None:
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
    code = getattr(exc, "code", None)
    if code == 429:
        return True
    status = str(getattr(exc, "status", "") or "").upper()
    if status == "RESOURCE_EXHAUSTED":
        return True
    return "429" in str(exc) or "RESOURCE_EXHAUSTED" in str(exc).upper()


# ── Prompt ───────────────────────────────────────────────────────────────────

def _build_prompt(batch: list[dict], icp_prompt: str) -> str:
    rows = []
    for c in batch:
        row: dict = {"i": c["index"], "name": c.get("name") or ""}
        if c.get("industry"):
            row["industry"] = c["industry"]
        if c.get("employee_size"):
            row["employees"] = c["employee_size"]
        if c.get("description"):
            row["description"] = str(c["description"])[:280]
        rows.append(row)
    candidates_block = json.dumps(rows, ensure_ascii=False)

    return f"""You are a strict B2B targeting judge. Given the campaign ICP below, decide for EACH company whether it genuinely fits the ICP as an OUTREACH TARGET (a company whose employees we would contact to sell this service).

CAMPAIGN ICP:
{icp_prompt}

Judging rules:
- Judge the actual BUSINESS MODEL, not the industry tag. Industry taxonomies mislabel: oil majors carry "retail" tags for gas stations; giant grocery chains and big-box retailers are tagged "retail & consumer" but are NOT D2C brands.
- If the ICP targets D2C / direct-to-consumer / consumer brands: match only companies whose primary business is selling their OWN branded products to consumers (mostly online). Reject oil & energy companies, supermarket/grocery chains, big-box retailers, department stores, bookstore chains, marketplaces, distributors, and wholesalers.
- Reject agencies, consultancies, staffing firms, and service providers unless the ICP explicitly targets them.
- Respect any company-size expectations implied by the ICP (a Fortune-500 giant rarely fits an ICP aimed at growing brands).
- When genuinely ambiguous but plausible, lean towards match=true.

COMPANIES (JSON array; "i" is the company id):
{candidates_block}

Return ONLY a JSON array — no commentary, no markdown fences — with exactly one object per company:
[{{"i": <int>, "match": <true|false>, "reason": "<max 10 words>"}}]"""


# ── Model call ───────────────────────────────────────────────────────────────

async def _call_gemini_with_retry(gemini_client, prompt: str):
    from google.genai import types

    limiter = await get_rate_limiter(_GEMINI_RATE_LIMITER_KEY, *_GEMINI_COMPANY_GATE_RATE)
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
                    f"[company-gate] Gemini 429/rate-limited on attempt {attempt + 1}, retry in {wait:.1f}s"
                )
            else:
                wait = backoff_with_jitter(attempt)
                logger.warning(
                    f"[company-gate] Gemini call failed on attempt {attempt + 1}, retry in {wait:.1f}s: {e}"
                )
            await asyncio.sleep(wait)
    raise last_exc  # pragma: no cover


def _parse_rows(raw_content: str) -> list[dict]:
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
        for key in ("results", "companies", "rows", "items"):
            if isinstance(parsed.get(key), list):
                parsed = parsed[key]
                break
    if not isinstance(parsed, list):
        return []
    return [r for r in parsed if isinstance(r, dict)]


async def _record_gemini_usage(resp, account_id: str | None, campaign_id: str | None, started_at: float):
    """Log this Gemini call's token spend so it shows up in the cost portal.

    Direct-SDK Gemini calls bypass OpenRouter, so without this they were
    invisible to /api/admin/costs — the gates run on every discovery batch and
    were silently unaccounted spend. Recorded into the same collection as
    OpenRouter usage (the model name distinguishes the provider).
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
            feature="company_gate",
        )
    except Exception:
        pass


async def _judge_batch(
    gemini_client, batch: list[dict], icp_prompt: str, batch_num: int, fail_closed_ambiguous: bool = False,
    account_id: str | None = None, campaign_id: str | None = None,
) -> dict[int, dict]:
    # WHOLE-BATCH / infra failure path (below, on exception): always fail-open,
    # regardless of `fail_closed_ambiguous` — a transient Gemini outage must never
    # silently zero out an entire campaign's companies.
    verdicts: dict[int, dict] = {c["index"]: _fail_open() for c in batch}
    batch_indices = set(verdicts.keys())

    prompt = _build_prompt(batch, icp_prompt)
    _started_at = asyncio.get_event_loop().time()
    try:
        resp = await asyncio.wait_for(
            _call_gemini_with_retry(gemini_client, prompt), _CALL_TIMEOUT_SECONDS
        )
    except Exception as e:
        logger.warning(f"[company-gate] batch {batch_num} failed (fail-open for {len(batch)}): {e}")
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
                continue
            reason = str(row.get("reason") or "").strip()[:120] or (
                "fits ICP" if match else "outside ICP"
            )
            verdicts[idx] = {"match": match, "reason": reason}
        except (KeyError, TypeError, ValueError):
            continue

    # PER-COMPANY ambiguity: rows that came back in this otherwise-successful batch
    # but couldn't be parsed into a verdict (missing/malformed) are still sitting at
    # the fail-open default here. Under the configurable flag, fail these CLOSED
    # instead — this is deliberately scoped to per-company gaps only, never applied
    # to the whole-batch exception path above.
    if fail_closed_ambiguous:
        for idx in batch_indices:
            if verdicts[idx]["reason"] == _FAIL_OPEN_REASON:
                verdicts[idx] = _fail_closed_ambiguous()

    kept = sum(1 for v in verdicts.values() if v["match"] and v["reason"] not in (_FAIL_OPEN_REASON,))
    rejected = sum(1 for v in verdicts.values() if not v["match"])
    unavailable = sum(1 for v in verdicts.values() if v["reason"] == _FAIL_OPEN_REASON)
    ambiguous_closed = sum(1 for v in verdicts.values() if v["reason"] == _AMBIGUOUS_FAIL_CLOSED_REASON)
    logger.info(
        f"[company-gate] batch {batch_num}: {len(batch)} judged — kept {kept}, "
        f"rejected {rejected}, fail-open {unavailable}, ambiguous-fail-closed {ambiguous_closed}"
    )
    return verdicts


# ── Public API ───────────────────────────────────────────────────────────────

async def ai_company_gate(
    companies: list[dict],
    icp_prompt: str,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> dict[int, dict]:
    """AI semantic company-fit gate. Call AFTER the deterministic score gate.

    Args:
        companies: list of {"index": int, "name": str, "description": str?,
                            "industry": str?, "employee_size": str?}.
        icp_prompt: the campaign's free-text ICP description.
        account_id / campaign_id: cost attribution tags for the usage log.

    Returns:
        {index: {"match": bool, "reason": str}} for EVERY input index; on any
        infrastructure failure the affected indices fail open.
    """
    results: dict[int, dict] = {}
    valid: list[dict] = []
    for c in companies or []:
        if not isinstance(c, dict):
            continue
        try:
            idx = int(c.get("index"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        results[idx] = _fail_open()
        name = str(c.get("name") or "").strip()
        if not name:
            continue  # nothing to judge — fail open
        valid.append({
            "index": idx,
            "name": name,
            "description": c.get("description"),
            "industry": c.get("industry"),
            "employee_size": c.get("employee_size"),
        })

    if not valid:
        return results

    try:
        from google import genai
        from config import get_settings

        settings = get_settings()
        gemini_client = genai.Client(api_key=settings.gemini_api_key)
    except Exception as e:
        # Whole-batch / infra failure (client unavailable entirely) — always fail-open.
        logger.warning(f"[company-gate] Gemini client unavailable, fail-open for all {len(valid)}: {e}")
        return results

    fail_closed_ambiguous = bool(getattr(settings, "company_gate_fail_closed", False))

    for batch_num, start in enumerate(range(0, len(valid), _BATCH_SIZE), start=1):
        batch = valid[start:start + _BATCH_SIZE]
        try:
            verdicts = await _judge_batch(
                gemini_client, batch, icp_prompt or "", batch_num, fail_closed_ambiguous,
                account_id=account_id, campaign_id=campaign_id,
            )
            results.update(verdicts)
        except Exception as e:
            # Whole-batch / infra failure (unexpected crash) — always fail-open.
            logger.warning(f"[company-gate] batch {batch_num} crashed (fail-open): {e}")

    return results
