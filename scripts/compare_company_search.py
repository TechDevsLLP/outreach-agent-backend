"""
Compare company-search API lanes for OutFlo smart campaigns.

Lanes:
  A) Perplexity Sonar Pro via OpenRouter (current production path)
  B) Gemini 3.1 Flash-Lite with Google Search grounding (direct Gemini API)

Industry fixture: Healthcare AI (200-company target)

Usage:
    cd backend
    export GEMINI_API_KEY="<from aistudio.google.com>"
    python scripts/compare_company_search.py [--target 200] [--no-validate-urls]

Outputs to: scripts/results/healthcare_ai_YYYYMMDD_HHMMSS/
    sonar_pro.json
    gemini_grounded.json
    comparison.md
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Backend root on path so production modules are importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

from services.company_sourcing_service import (
    _BATCH_DIVERSITY_ANGLES,
    _TARGET_PER_BATCH,
    _build_prompt,
    _normalize_linkedin_url,
    _strip_citations,
    _validate_linkedin_url_reachable,
)
from services.openrouter_service import OpenRouterClient, extract_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── ICP presets ───────────────────────────────────────────────────────────────

_HEALTHCARE_AI_ICP = (
    "B2B Healthcare AI companies and scale-ups (Seed through Series D, or recently IPO'd), "
    "20-1000 employees, headquartered in US, UK, Canada, or Western Europe, building AI "
    "products for clinical decision support, medical imaging / radiology AI, ambient clinical "
    "documentation, drug discovery, EHR intelligence, patient triage, clinical trial intelligence, "
    "or clinical workflow automation. Exclude pure consumer health apps, general healthcare "
    "consultancies, and pre-product stealth companies."
)

ICP_PRESETS: dict[str, str] = {
    "healthcare_ai": _HEALTHCARE_AI_ICP,
}


# ── Pricing (per 1 M tokens) ──────────────────────────────────────────────────

# Perplexity Sonar Pro via OpenRouter — billed at OpenRouter's Sonar Pro rates
_SONAR_PRICE_IN = 3.0    # $/1M input tokens
_SONAR_PRICE_OUT = 15.0  # $/1M output tokens

# Gemini 3.1 Flash-Lite — Google AI Studio pricing (mid-2026 estimate)
_GEMINI_PRICE_IN = 0.075   # $/1M input tokens
_GEMINI_PRICE_OUT = 0.30   # $/1M output tokens


# ── Batch config ──────────────────────────────────────────────────────────────

_MAX_BATCHES = 10           # up from production's 6; ensures we can reach 200
_SONAR_MODEL = "perplexity/sonar-pro"
_GEMINI_MODEL = "gemini-3.1-flash-lite"


# ── Shared dedup / post-processing ───────────────────────────────────────────

def _process_batch(companies_raw: list, seen_names: set, seen_urls: set) -> list[dict]:
    out = []
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
        out.append({
            "company_name": name,
            "company_linkedin_url": linkedin,
            "company_domain": (c.get("domain") or "").strip().lower() or None,
            "company_website": c.get("website") or None,
            "industry": c.get("industry") or None,
            "country": c.get("country") or None,
            "employee_size_estimate": c.get("employee_size_estimate") or None,
            "description": c.get("description") or None,
        })
    return out


async def _validate_urls(companies: list[dict]) -> list[dict]:
    async def _false() -> bool:
        return False

    async with httpx.AsyncClient(timeout=15.0) as http:
        results = await asyncio.gather(
            *[
                _validate_linkedin_url_reachable(c["company_linkedin_url"], http)
                if c.get("company_linkedin_url")
                else _false()
                for c in companies
            ],
        )
    for c, ok in zip(companies, results):
        c["linkedin_url_validated"] = bool(ok)
    return companies


def _req_count(target_count: int, accumulated: int) -> int:
    remaining = target_count - accumulated
    return min(_TARGET_PER_BATCH, max(remaining * 2, 25))


# ── Lane A: Perplexity Sonar Pro via OpenRouter ───────────────────────────────

async def run_sonar_pro(icp_prompt: str, target_count: int, validate_urls: bool) -> dict:
    or_client = OpenRouterClient()
    seen_names: set[str] = set()
    seen_urls: set[str] = set()
    accumulated: list[dict] = []
    batches: list[dict] = []
    total_in = total_out = 0
    t0 = time.monotonic()

    for i in range(_MAX_BATCHES):
        if len(accumulated) >= target_count:
            break
        angle = _BATCH_DIVERSITY_ANGLES[i % len(_BATCH_DIVERSITY_ANGLES)]
        prompt = _build_prompt(icp_prompt, _req_count(target_count, len(accumulated)), list(seen_names), angle)

        bt = time.monotonic()
        try:
            resp = await or_client.chat_completion(
                messages=[
                    {"role": "system", "content": "You are a B2B research assistant. Return ONLY valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                model=_SONAR_MODEL,
                temperature=0.2,
                max_tokens=4096,
                feature="compare_sonar_pro",
            )
        except Exception as e:
            logger.warning(f"[sonar] batch {i+1} failed: {e}")
            batches.append({"batch": i + 1, "error": str(e), "latency_s": round(time.monotonic() - bt, 2)})
            continue

        lat = round(time.monotonic() - bt, 2)
        usage = resp.get("_usage") or {}
        in_tok = usage.get("prompt_tokens", 0) or 0
        out_tok = usage.get("completion_tokens", 0) or 0
        total_in += in_tok
        total_out += out_tok

        raw = resp.get("content") if isinstance(resp, dict) else None
        if not raw:
            logger.warning(f"[sonar] batch {i+1} empty content")
            batches.append({"batch": i + 1, "raw_count": 0, "latency_s": lat, "input_tokens": in_tok, "output_tokens": out_tok})
            continue

        try:
            parsed = extract_json(_strip_citations(raw))
        except Exception as e:
            logger.warning(f"[sonar] batch {i+1} JSON error: {e!r} | first 200: {raw[:200]!r}")
            batches.append({"batch": i + 1, "raw_count": 0, "latency_s": lat, "parse_error": str(e), "input_tokens": in_tok, "output_tokens": out_tok})
            continue

        raw_list = (parsed.get("companies") if isinstance(parsed, dict) else None) or []
        new = _process_batch(raw_list, seen_names, seen_urls)
        accumulated.extend(new)
        batches.append({
            "batch": i + 1,
            "raw_count": len(raw_list),
            "new_after_dedup": len(new),
            "running_total": len(accumulated),
            "latency_s": lat,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
        })
        logger.info(f"[sonar] batch {i+1}: raw={len(raw_list)} new={len(new)} total={len(accumulated)} {lat}s")

    if validate_urls and accumulated:
        logger.info(f"[sonar] HEAD-validating {len(accumulated)} LinkedIn URLs …")
        accumulated = await _validate_urls(accumulated)

    total_lat = round(time.monotonic() - t0, 2)
    cost = (total_in / 1_000_000) * _SONAR_PRICE_IN + (total_out / 1_000_000) * _SONAR_PRICE_OUT
    return {
        "companies": accumulated,
        "meta": {
            "provider": "Perplexity Sonar Pro via OpenRouter",
            "model": _SONAR_MODEL,
            "batches_run": sum(1 for b in batches if "error" not in b),
            "raw_received": sum(b.get("raw_count", 0) for b in batches),
            "after_dedup": len(accumulated),
            "after_url_validation": sum(1 for c in accumulated if c.get("linkedin_url_validated")),
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_tokens": total_in + total_out,
            "est_cost_usd": round(cost, 4),
            "total_latency_s": total_lat,
            "avg_batch_latency_s": round(
                sum(b.get("latency_s", 0) for b in batches) / max(len(batches), 1), 2
            ),
        },
        "batches": batches,
    }


# ── Lane B: Gemini 3.1 Flash-Lite with Google Search grounding ───────────────

async def run_gemini_grounded(icp_prompt: str, target_count: int, validate_urls: bool) -> dict:
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if not gemini_key:
        raise ValueError("GEMINI_API_KEY is not set. Export it before running.")

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=gemini_key)

    seen_names: set[str] = set()
    seen_urls: set[str] = set()
    accumulated: list[dict] = []
    batches: list[dict] = []
    total_in = total_out = 0
    t0 = time.monotonic()

    for i in range(_MAX_BATCHES):
        if len(accumulated) >= target_count:
            break
        angle = _BATCH_DIVERSITY_ANGLES[i % len(_BATCH_DIVERSITY_ANGLES)]
        prompt = _build_prompt(icp_prompt, _req_count(target_count, len(accumulated)), list(seen_names), angle)

        bt = time.monotonic()
        try:
            resp = await client.aio.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=(
                        "You are a B2B research assistant. "
                        "Return ONLY valid JSON — no markdown fences, no preamble, no citations."
                    ),
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.2,
                    max_output_tokens=8192,
                ),
            )
        except Exception as e:
            logger.warning(f"[gemini] batch {i+1} failed: {e}")
            batches.append({"batch": i + 1, "error": str(e), "latency_s": round(time.monotonic() - bt, 2)})
            continue

        lat = round(time.monotonic() - bt, 2)
        usage = getattr(resp, "usage_metadata", None)
        in_tok = getattr(usage, "prompt_token_count", 0) or 0
        out_tok = getattr(usage, "candidates_token_count", 0) or 0
        total_in += in_tok
        total_out += out_tok

        raw = getattr(resp, "text", None)
        if not raw:
            logger.warning(f"[gemini] batch {i+1} empty text response")
            batches.append({"batch": i + 1, "raw_count": 0, "latency_s": lat, "input_tokens": in_tok, "output_tokens": out_tok})
            continue

        try:
            parsed = extract_json(_strip_citations(raw))
        except Exception as e:
            logger.warning(f"[gemini] batch {i+1} JSON error: {e!r} | first 200: {raw[:200]!r}")
            batches.append({"batch": i + 1, "raw_count": 0, "latency_s": lat, "parse_error": str(e), "input_tokens": in_tok, "output_tokens": out_tok})
            continue

        raw_list = (parsed.get("companies") if isinstance(parsed, dict) else None) or []
        new = _process_batch(raw_list, seen_names, seen_urls)
        accumulated.extend(new)
        batches.append({
            "batch": i + 1,
            "raw_count": len(raw_list),
            "new_after_dedup": len(new),
            "running_total": len(accumulated),
            "latency_s": lat,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
        })
        logger.info(f"[gemini] batch {i+1}: raw={len(raw_list)} new={len(new)} total={len(accumulated)} {lat}s")

    if validate_urls and accumulated:
        logger.info(f"[gemini] HEAD-validating {len(accumulated)} LinkedIn URLs …")
        accumulated = await _validate_urls(accumulated)

    total_lat = round(time.monotonic() - t0, 2)
    cost = (total_in / 1_000_000) * _GEMINI_PRICE_IN + (total_out / 1_000_000) * _GEMINI_PRICE_OUT
    return {
        "companies": accumulated,
        "meta": {
            "provider": "Gemini 3.1 Flash-Lite (Google Search grounded)",
            "model": _GEMINI_MODEL,
            "batches_run": sum(1 for b in batches if "error" not in b),
            "raw_received": sum(b.get("raw_count", 0) for b in batches),
            "after_dedup": len(accumulated),
            "after_url_validation": sum(1 for c in accumulated if c.get("linkedin_url_validated")),
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_tokens": total_in + total_out,
            "est_cost_usd": round(cost, 6),
            "total_latency_s": total_lat,
            "avg_batch_latency_s": round(
                sum(b.get("latency_s", 0) for b in batches) / max(len(batches), 1), 2
            ),
        },
        "batches": batches,
    }


# ── Report ────────────────────────────────────────────────────────────────────

def _freq_table(companies: list[dict], field: str, top: int = 8) -> str:
    counts: dict[str, int] = {}
    for c in companies:
        key = (c.get(field) or "Unknown").strip()
        counts[key] = counts.get(key, 0) + 1
    rows = sorted(counts.items(), key=lambda x: -x[1])[:top]
    return "\n".join(f"| {k:<28} | {v} |" for k, v in rows)


def _sample_rows(companies: list[dict], n: int = 20) -> str:
    rows = []
    for c in companies[:n]:
        name = (c.get("company_name") or "")[:38]
        domain = (c.get("company_domain") or "")[:28]
        country = (c.get("country") or "")[:18]
        size = (c.get("employee_size_estimate") or "")[:14]
        v = "✓" if c.get("linkedin_url_validated") else "✗"
        rows.append(f"| {name:<38} | {domain:<28} | {country:<18} | {size:<14} | {v} |")
    return "\n".join(rows)


def _unique_rows(companies: list[dict], n: int = 10) -> str:
    rows = []
    for c in companies[:n]:
        name = (c.get("company_name") or "")[:42]
        domain = (c.get("company_domain") or "")[:28]
        country = (c.get("country") or "")[:18]
        url = (c.get("company_linkedin_url") or "")[:55]
        rows.append(f"| {name:<42} | {domain:<28} | {country:<18} | {url} |")
    return "\n".join(rows)


def _build_report(sonar: dict, gemini: dict, icp: str, run_ts: str) -> str:
    sm, gm = sonar["meta"], gemini["meta"]
    sc = sonar["companies"]
    gc = gemini["companies"]

    sonar_names = {c["company_name"].lower() for c in sc}
    gemini_names = {c["company_name"].lower() for c in gc}
    sonar_urls = {c["company_linkedin_url"] for c in sc if c.get("company_linkedin_url")}
    gemini_urls = {c["company_linkedin_url"] for c in gc if c.get("company_linkedin_url")}

    # Intersection = name match OR LinkedIn URL match
    intersection = (sonar_names & gemini_names) | (sonar_urls & gemini_urls)
    intersection_count = len(intersection)

    sonar_only = [
        c for c in sc
        if c["company_name"].lower() not in gemini_names
        and c.get("company_linkedin_url") not in gemini_urls
    ]
    gemini_only = [
        c for c in gc
        if c["company_name"].lower() not in sonar_names
        and c.get("company_linkedin_url") not in sonar_urls
    ]

    def _cnt(companies, field):
        return sum(1 for c in companies if c.get(field))

    def _uniq(companies, field):
        return len({(c.get(field) or "Unknown") for c in companies})

    lines = [
        "# Healthcare AI Company Search Comparison",
        "",
        f"**Run:** {run_ts}",
        f"**ICP:** _{icp[:220]}..._",
        "",
        "---",
        "",
        "## Metrics Summary",
        "",
        "| Metric | Sonar Pro (OpenRouter) | Gemini 3.1 Flash-Lite (Grounded) |",
        "|---|---:|---:|",
        f"| Batches run | {sm['batches_run']} | {gm['batches_run']} |",
        f"| Raw companies returned | {sm['raw_received']} | {gm['raw_received']} |",
        f"| After dedup | **{sm['after_dedup']}** | **{gm['after_dedup']}** |",
        f"| LinkedIn URLs returned | {_cnt(sc, 'company_linkedin_url')} | {_cnt(gc, 'company_linkedin_url')} |",
        f"| LinkedIn URLs validated (HEAD ✓) | {sm['after_url_validation']} | {gm['after_url_validation']} |",
        f"| Domains returned | {_cnt(sc, 'company_domain')} | {_cnt(gc, 'company_domain')} |",
        f"| Unique countries | {_uniq(sc, 'country')} | {_uniq(gc, 'country')} |",
        f"| Total input tokens | {sm['total_input_tokens']:,} | {gm['total_input_tokens']:,} |",
        f"| Total output tokens | {sm['total_output_tokens']:,} | {gm['total_output_tokens']:,} |",
        f"| Est. cost (USD) | ${sm['est_cost_usd']:.4f} | ${gm['est_cost_usd']:.6f} |",
        f"| Total wall-clock (s) | {sm['total_latency_s']} | {gm['total_latency_s']} |",
        f"| Avg latency / batch (s) | {sm['avg_batch_latency_s']} | {gm['avg_batch_latency_s']} |",
        "",
        "## Overlap Analysis",
        "",
        f"- **Found by BOTH providers** (name OR LinkedIn URL): **{intersection_count}** companies",
        f"- **Sonar-only unique:** {len(sonar_only)} companies",
        f"- **Gemini-only unique:** {len(gemini_only)} companies",
        f"- **Overlap rate:** {intersection_count / max(len(sc), 1):.0%} of Sonar results appear in Gemini",
        "",
        "---",
        "",
        "## Country Distribution",
        "",
        "### Sonar Pro",
        "| Country | Count |",
        "|---|---|",
        _freq_table(sc, "country"),
        "",
        "### Gemini 3.1 Flash-Lite",
        "| Country | Count |",
        "|---|---|",
        _freq_table(gc, "country"),
        "",
        "## Employee-Size Distribution",
        "",
        "### Sonar Pro",
        "| Size band | Count |",
        "|---|---|",
        _freq_table(sc, "employee_size_estimate"),
        "",
        "### Gemini 3.1 Flash-Lite",
        "| Size band | Count |",
        "|---|---|",
        _freq_table(gc, "employee_size_estimate"),
        "",
        "---",
        "",
        "## Sample — Top 20 from Sonar Pro",
        "",
        "| Company | Domain | Country | Size | LinkedIn ✓ |",
        "|---|---|---|---|---|",
        _sample_rows(sc),
        "",
        "## Sample — Top 20 from Gemini 3.1 Flash-Lite",
        "",
        "| Company | Domain | Country | Size | LinkedIn ✓ |",
        "|---|---|---|---|---|",
        _sample_rows(gc),
        "",
        "---",
        "",
        "## Sonar-Only (first 10 not found by Gemini)",
        "",
        "| Company | Domain | Country | LinkedIn URL |",
        "|---|---|---|---|",
        _unique_rows(sonar_only),
        "",
        "## Gemini-Only (first 10 not found by Sonar)",
        "",
        "| Company | Domain | Country | LinkedIn URL |",
        "|---|---|---|---|",
        _unique_rows(gemini_only),
        "",
        "---",
        "",
        "## Verdict",
        "",
        "| Dimension | Winner |",
        "|---|---|",
        f"| Volume (after dedup) | {'Sonar Pro' if sm['after_dedup'] >= gm['after_dedup'] else 'Gemini'} ({sm['after_dedup']} vs {gm['after_dedup']}) |",
        f"| LinkedIn URL validity | {'Sonar Pro' if sm['after_url_validation'] >= gm['after_url_validation'] else 'Gemini'} ({sm['after_url_validation']} vs {gm['after_url_validation']} validated) |",
        f"| Speed | {'Sonar Pro' if sm['total_latency_s'] <= gm['total_latency_s'] else 'Gemini'} ({sm['total_latency_s']}s vs {gm['total_latency_s']}s) |",
        f"| Cost | {'Sonar Pro' if sm['est_cost_usd'] <= gm['est_cost_usd'] else 'Gemini'} (${sm['est_cost_usd']:.4f} vs ${gm['est_cost_usd']:.4f}) |",
        f"| Geographic diversity | {'Sonar Pro' if _uniq(sc, 'country') >= _uniq(gc, 'country') else 'Gemini'} ({_uniq(sc, 'country')} vs {_uniq(gc, 'country')} countries) |",
    ]
    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare Sonar Pro vs Gemini grounded company search for OutFlo smart campaigns"
    )
    parser.add_argument("--industry", default="healthcare_ai", choices=list(ICP_PRESETS))
    parser.add_argument("--target", type=int, default=200, help="Target companies per lane (default: 200)")
    parser.add_argument("--no-validate-urls", action="store_true", help="Skip LinkedIn HEAD validation")
    args = parser.parse_args()

    validate_urls = not args.no_validate_urls
    icp_prompt = ICP_PRESETS[args.industry]
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(__file__).parent / "results" / f"{args.industry}_{run_ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Fail fast: check Gemini key before burning OpenRouter credits
    if not os.environ.get("GEMINI_API_KEY"):
        print(
            "ERROR: GEMINI_API_KEY not set.\n"
            "  export GEMINI_API_KEY=<your key from aistudio.google.com>"
        )
        sys.exit(1)

    logger.info(f"Industry: {args.industry} | Target: {args.target} | Validate URLs: {validate_urls}")
    logger.info(f"Output dir: {out_dir}")

    # ── Lane B: Gemini first (auth failure caught before Lane A runs) ─────────
    logger.info("══ Lane B: Gemini 3.1 Flash-Lite (grounded) ══════════════════")
    try:
        gemini_result = await run_gemini_grounded(icp_prompt, args.target, validate_urls)
    except ValueError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    gemini_out = out_dir / "gemini_grounded.json"
    gemini_out.write_text(json.dumps(gemini_result, indent=2, default=str))
    logger.info(f"[gemini] {gemini_result['meta']['after_dedup']} companies → {gemini_out}")

    # ── Lane A: Sonar Pro ─────────────────────────────────────────────────────
    logger.info("══ Lane A: Perplexity Sonar Pro (via OpenRouter) ══════════════")
    sonar_result = await run_sonar_pro(icp_prompt, args.target, validate_urls)
    sonar_out = out_dir / "sonar_pro.json"
    sonar_out.write_text(json.dumps(sonar_result, indent=2, default=str))
    logger.info(f"[sonar] {sonar_result['meta']['after_dedup']} companies → {sonar_out}")

    # ── Comparison report ─────────────────────────────────────────────────────
    report = _build_report(sonar_result, gemini_result, icp_prompt, run_ts)
    report_out = out_dir / "comparison.md"
    report_out.write_text(report)
    logger.info(f"Report → {report_out}")

    # ── Console summary ───────────────────────────────────────────────────────
    sm, gm = sonar_result["meta"], gemini_result["meta"]
    print(f"\n{'═' * 62}")
    print(f"  Results → {out_dir}")
    print(f"{'═' * 62}")
    print(f"  {'Metric':<32} {'Sonar Pro':>10} {'Gemini':>12}")
    print(f"  {'─' * 58}")
    print(f"  {'After dedup':<32} {sm['after_dedup']:>10} {gm['after_dedup']:>12}")
    print(f"  {'LinkedIn valid':<32} {sm['after_url_validation']:>10} {gm['after_url_validation']:>12}")
    print(f"  {'Est. cost (USD)':<32} ${sm['est_cost_usd']:>9.4f} ${gm['est_cost_usd']:>11.4f}")
    print(f"  {'Total time (s)':<32} {sm['total_latency_s']:>10} {gm['total_latency_s']:>12}")
    print(f"{'═' * 62}\n")


if __name__ == "__main__":
    asyncio.run(main())
