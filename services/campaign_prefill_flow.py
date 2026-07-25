"""Deterministic step-machine for /campaigns/new chat."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field as datafield
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class Step:
    id: str
    widget: str  # "text" | "single_select" | "multi_select"
    template: str
    options: list[dict] | None = None
    allow_free_text: bool = False
    required: bool = True
    only_if: Callable[[dict], bool] | None = datafield(default=None, repr=False)


_FUNCTION_OPTIONS = [
    {"value": "marketing", "label": "Marketing"},
    {"value": "sales", "label": "Sales"},
    {"value": "operations", "label": "Operations"},
    {"value": "engineering", "label": "Engineering"},
    {"value": "product", "label": "Product"},
    {"value": "finance", "label": "Finance"},
    {"value": "hr", "label": "HR"},
    {"value": "it", "label": "IT"},
    {"value": "legal", "label": "Legal"},
    {"value": "customer_success", "label": "Customer Success"},
    {"value": "data", "label": "Data"},
    {"value": "design", "label": "Design"},
]

_SENIORITY_OPTIONS = [
    {"value": "c_suite", "label": "C-Suite (CEO, CTO, CMO…)"},
    {"value": "owner", "label": "Owner"},
    {"value": "founder", "label": "Founder"},
    {"value": "vp", "label": "VP"},
    {"value": "director", "label": "Director"},
    {"value": "manager", "label": "Manager"},
    {"value": "senior", "label": "Senior"},
]

_COUNTRY_OPTIONS = [
    {"value": "United States", "label": "United States"},
    {"value": "United Kingdom", "label": "United Kingdom"},
    {"value": "Canada", "label": "Canada"},
    {"value": "Germany", "label": "Germany"},
    {"value": "France", "label": "France"},
    {"value": "Netherlands", "label": "Netherlands"},
    {"value": "India", "label": "India"},
    {"value": "Australia", "label": "Australia"},
    {"value": "Singapore", "label": "Singapore"},
    {"value": "United Arab Emirates", "label": "UAE"},
]

_CTA_OPTIONS = [
    {"value": "book_call", "label": "Book a call"},
    {"value": "reply", "label": "Reply to this message"},
    {"value": "visit_link", "label": "Visit a link"},
]

STEPS: list[Step] = [
    Step(
        id="industry_label",
        widget="text",
        template="What industry or type of companies do you want to target? For example: 'B2B SaaS', 'Healthcare providers', 'D2C ecommerce brands'.",
        allow_free_text=True,
    ),
    Step(
        id="icp_functional_departments",
        widget="multi_select",
        template="Which department(s) are you targeting at {industry} companies?",
        options=_FUNCTION_OPTIONS,
        allow_free_text=True,
    ),
    Step(
        id="icp_seniority_levels",
        widget="multi_select",
        template="What seniority level of {function_pretty} are you targeting?",
        options=_SENIORITY_OPTIONS,
        allow_free_text=False,
    ),
    Step(
        id="icp_countries",
        widget="multi_select",
        template="Which regions or countries should we focus on?",
        options=_COUNTRY_OPTIONS,
        allow_free_text=True,
    ),
    Step(
        id="value_proposition",
        widget="text",
        template="What value do you offer to {seniority_pretty} {function_pretty} at {industry} companies?",
        allow_free_text=True,
    ),
    Step(
        id="cta_type",
        widget="single_select",
        template="What action do you want recipients to take?",
        options=_CTA_OPTIONS,
        allow_free_text=False,
    ),
    Step(
        id="cta_url",
        widget="text",
        template="What URL should the CTA link to?",
        allow_free_text=True,
        only_if=lambda c: c.get("cta_type") == "visit_link",
    ),
]

# Required steps (no conditional only_if)
_REQUIRED_STEPS = [s for s in STEPS if s.required and s.only_if is None]


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, str)):
        return len(value) > 0
    return bool(value)


def next_missing_step(captured: dict) -> Step | None:
    for step in STEPS:
        if step.only_if is not None and not step.only_if(captured):
            continue
        if not _is_present(captured.get(step.id)):
            return step
    return None


def count_required_satisfied(captured: dict) -> int:
    return sum(1 for s in _REQUIRED_STEPS if _is_present(captured.get(s.id)))


def count_required_total() -> int:
    return len(_REQUIRED_STEPS)


def strip_empty(d: dict) -> dict:
    return {k: v for k, v in d.items() if _is_present(v)}


_SENIORITY_LABELS: dict[str, str] = {
    "c_suite": "C-Suite",
    "owner": "Owner",
    "founder": "Founder",
    "partner": "Partner",
    "vp": "VP",
    "director": "Director",
    "manager": "Manager",
    "senior": "Senior",
}


def _pretty_list(items: list[str], key_map: dict[str, str] | None = None) -> str:
    if not items:
        return ""
    labels = [key_map.get(it, it.replace("_", " ")) if key_map else it.replace("_", " ") for it in items]
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + " and " + labels[-1]


def _pretty_context(captured: dict) -> dict:
    industry = captured.get("industry_label") or "target"
    if isinstance(industry, list):
        industry = industry[0] if industry else "target"

    funcs = captured.get("icp_functional_departments") or []
    if isinstance(funcs, str):
        funcs = [funcs]
    function_pretty = _pretty_list(funcs) or "professionals"

    seniorities = captured.get("icp_seniority_levels") or []
    if isinstance(seniorities, str):
        seniorities = [seniorities]
    seniority_pretty = _pretty_list(seniorities, _SENIORITY_LABELS) or "decision makers"

    return {
        "industry": industry,
        "function_pretty": function_pretty,
        "seniority_pretty": seniority_pretty,
    }


def render_question(step: Step, captured: dict) -> str:
    ctx = _pretty_context(captured)
    return step.template.format(**ctx)


# ── LLM helpers ───────────────────────────────────────────────────────────────

_EXTRACT_SYSTEM_PROMPT = """You are a data extraction assistant for a B2B outreach tool.
Given the user's latest message and the field we're expecting, extract structured values.

Return ONLY a JSON object with key "patch" containing only the fields you can confidently extract:
{"patch": {"<field>": <value>, ...}}

Rules:
- Only include keys you can confidently extract. Do NOT invent or guess.
- Do NOT include explanatory text.
- Field value formats:
  - industry_label: string (e.g. "B2B SaaS")
  - icp_functional_departments: array from ["marketing","sales","operations","engineering","product","finance","hr","it","legal","customer_success","data","design"]
  - icp_seniority_levels: array from ["c_suite","owner","founder","partner","vp","director","manager","senior"]
  - icp_countries: array of country name strings (e.g. ["United Kingdom","Germany"])
  - value_proposition: string
  - cta_type: one of "book_call","reply","visit_link"
  - cta_url: string

Synonym mappings to apply:
- "CXO","C-level","C-suite","C suite" → icp_seniority_levels: ["c_suite"]
- "marketing folks/team/people" → icp_functional_departments: ["marketing"]
- "sales people/team" → icp_functional_departments: ["sales"]
- "tech team/engineers/developers/devs" → icp_functional_departments: ["engineering"]
- "Europe" → icp_countries: ["United Kingdom","Germany","France","Netherlands","Sweden","Denmark","Belgium","Switzerland","Spain","Italy"]
- "North America","USA and Canada" → icp_countries: ["United States","Canada"]
- "APAC","Asia Pacific" → icp_countries: ["India","Singapore","Australia"]
- "MENA" → icp_countries: ["United Arab Emirates"]
- "Global","Worldwide","International" → icp_countries: ["United States","United Kingdom","Canada","Germany","Australia"]

If expected_field is provided, prioritize extracting that field. If the message contains multiple fields, include all."""


async def llm_extract_step(
    captured: dict,
    last_user_text: str,
    expected_field: str | None,
    company_profile: dict | None,
) -> dict:
    """Extract structured values from user free text. On failure returns captured unchanged."""
    from services.openrouter_service import OpenRouterClient
    from config import get_settings
    settings = get_settings()

    user_parts = []
    if expected_field:
        user_parts.append(f"Expected field: {expected_field}")
    user_parts.append(f"User said: {last_user_text}")

    client = OpenRouterClient()
    try:
        result = await client.chat_completion(
            messages=[
                {"role": "system", "content": _EXTRACT_SYSTEM_PROMPT},
                {"role": "user", "content": "\n".join(user_parts)},
            ],
            model=settings.assessment_model,
            fallback_models=settings.fallback_models,
            temperature=0.1,
            max_tokens=512,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        logger.warning(f"llm_extract_step failed: {exc}")
        return captured
    finally:
        await client.close()

    if not isinstance(result, dict):
        logger.warning(f"llm_extract_step non-dict result: {result!r}")
        return captured

    patch = result.get("patch", {})
    if not isinstance(patch, dict):
        logger.warning(f"llm_extract_step patch not dict: {patch!r}")
        return captured

    merged = dict(captured)
    for k, v in patch.items():
        if _is_present(v):
            merged[k] = v
    return merged


async def llm_compose_campaign(captured: dict, company_profile: dict | None) -> dict:
    """Compose final campaign JSON from all captured fields. Raises HTTPException(502) on failure."""
    from services.openrouter_service import OpenRouterClient
    from config import get_settings
    from fastapi import HTTPException
    settings = get_settings()
    from utils.prompts import get_system_prompt, CAMPAIGN_PREFILL_COMPOSE_SYSTEM_PROMPT

    system_prompt = await get_system_prompt(
        "campaign_prefill_compose",
        str((company_profile or {}).get("account_id") or "") or None,
    )
    if not system_prompt:
        system_prompt = CAMPAIGN_PREFILL_COMPOSE_SYSTEM_PROMPT

    lines = ["Compose a complete campaign based on these confirmed targeting details:"]
    for k, v in captured.items():
        if _is_present(v):
            lines.append(f"- {k}: {v}")
    if company_profile:
        lines.append("\nCompany context:")
        lines.append(f"- Company: {company_profile.get('company_name', '')}")
        lines.append(f"- Industry: {company_profile.get('industry', '')}")
        lines.append(f"- Sender value proposition: {company_profile.get('value_proposition', '')}")

    client = OpenRouterClient()
    try:
        result = await client.chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "\n".join(lines)},
            ],
            model=settings.assessment_model,
            fallback_models=settings.fallback_models,
            temperature=0.3,
            max_tokens=2048,
            response_format={"type": "json_object"},
        )
    except Exception as exc:
        logger.warning(f"llm_compose_campaign failed: {exc}")
        raise HTTPException(status_code=502, detail=f"AI request failed: {exc}")
    finally:
        await client.close()

    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="AI returned unexpected response format")

    if "campaign" not in result:
        if "name" in result and "target" in result:
            return result
        raise HTTPException(status_code=502, detail="AI did not return a campaign configuration")

    return result["campaign"]
