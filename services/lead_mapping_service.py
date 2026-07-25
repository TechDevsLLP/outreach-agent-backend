"""
Lead-list column mapping + row classification for Upload-a-Lead-List (BYOL) campaigns.

Two responsibilities:
  1. `propose_column_mapping(columns, sample_rows)` — one LLM call (OpenRouter +
     the `lead_column_mapping` system prompt, tenant-overridable) that maps the
     uploaded spreadsheet's columns onto a fixed set of canonical fields and emits
     clarifying questions for low-confidence columns. Falls back to a deterministic
     header-heuristic when the LLM is unavailable / returns garbage (and in tests).
  2. `classify_row(row, mapping)` — pure, deterministic per-row classification into
     "person" | "company" | "unresolvable". No I/O, no paid calls.

The canonical field set is shared with byol_discovery_service via `apply_mapping`
and `build_lead_fields`, so the discovery service never re-parses raw columns.
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Canonical fields a column may map to. "ignore" drops the column.
CANONICAL_FIELDS: set[str] = {
    "first_name",
    "last_name",
    "full_name",
    "job_title",
    "linkedin_url",
    "company_name",
    "company_domain",
    "company_linkedin_url",
    "email",
    "country",
    "seniority",
    "ignore",
}

# Free / personal email providers — an email at one of these does NOT yield a
# usable company domain for the email-finder path.
_FREE_EMAIL_DOMAINS: set[str] = {
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mac.com", "proton.me", "protonmail.com", "gmx.com", "zoho.com",
    "yandex.com", "mail.com", "hey.com",
}

# Deterministic header-heuristic used by the LLM fallback. Order matters: more
# specific headers are checked before generic ones (see _heuristic_field).
_HEADER_HEURISTICS: list[tuple[str, str]] = [
    ("company linkedin", "company_linkedin_url"),
    ("company_linkedin", "company_linkedin_url"),
    ("organization linkedin", "company_linkedin_url"),
    ("linkedin", "linkedin_url"),
    ("profile url", "linkedin_url"),
    ("profile link", "linkedin_url"),
    ("profile", "linkedin_url"),
    ("first name", "first_name"),
    ("firstname", "first_name"),
    ("given name", "first_name"),
    ("last name", "last_name"),
    ("lastname", "last_name"),
    ("surname", "last_name"),
    ("full name", "full_name"),
    ("fullname", "full_name"),
    ("contact name", "full_name"),
    ("person name", "full_name"),
    ("name", "full_name"),
    ("title", "job_title"),
    ("job title", "job_title"),
    ("position", "job_title"),
    ("role", "job_title"),
    ("headline", "job_title"),
    ("company name", "company_name"),
    ("company_name", "company_name"),
    ("organization", "company_name"),
    ("organisation", "company_name"),
    ("employer", "company_name"),
    ("account name", "company_name"),
    ("company", "company_name"),
    ("website", "company_domain"),
    ("domain", "company_domain"),
    ("url", "company_domain"),
    ("email", "email"),
    ("e-mail", "email"),
    ("mail", "email"),
    ("country", "country"),
    ("location", "country"),
    ("region", "country"),
    ("seniority", "seniority"),
    ("level", "seniority"),
]


# ---------------------------------------------------------------------------
# Normalizers (self-contained; no paid calls)
# ---------------------------------------------------------------------------

def _clean(value) -> str | None:
    """Trim a cell to a non-empty string or None (handles pandas NaN/None)."""
    if value is None:
        return None
    try:
        # pandas NaN is a float that != itself
        if isinstance(value, float) and value != value:
            return None
    except Exception:
        pass
    s = str(value).strip()
    return s or None


def normalize_linkedin_url(url) -> str | None:
    """Normalize any LinkedIn URL (person /in/ or company /company/) or return None."""
    u = _clean(url)
    if not u:
        return None
    if not u.lower().startswith("http"):
        u = "https://" + u.lstrip("/")
    try:
        p = urlparse(u)
    except Exception:
        return None
    if "linkedin.com" not in (p.netloc or "").lower():
        return None
    path = (p.path or "").rstrip("/")
    if not path:
        return None
    return f"https://www.linkedin.com{path}"


def is_person_linkedin(url) -> bool:
    norm = normalize_linkedin_url(url)
    return bool(norm) and "/in/" in norm.lower()


def is_company_linkedin(url) -> bool:
    norm = normalize_linkedin_url(url)
    return bool(norm) and "/company/" in norm.lower()


def normalize_domain(value) -> str | None:
    """Strip scheme/www/path from a website/URL/bare domain → 'example.com'."""
    v = _clean(value)
    if not v:
        return None
    if "://" in v:
        try:
            v = urlparse(v).netloc or v
        except Exception:
            pass
    v = v.strip().lower().lstrip("@")
    # drop any leading protocol remnants and path/query
    v = v.split("/")[0].split("?")[0]
    if v.startswith("www."):
        v = v[4:]
    # must look like a domain
    if "." not in v or " " in v:
        return None
    return v or None


def domain_from_email(email) -> str | None:
    e = _clean(email)
    if not e or "@" not in e:
        return None
    dom = e.rsplit("@", 1)[-1].strip().lower()
    if "." not in dom:
        return None
    return dom


# ---------------------------------------------------------------------------
# Mapping application
# ---------------------------------------------------------------------------

def apply_mapping(row: dict, mapping: dict) -> dict:
    """Project a raw spreadsheet row onto canonical fields using `mapping`.

    `mapping` is {original_column: canonical_field}. Columns mapped to "ignore"
    or an unknown field are dropped. Later non-empty values win only if the
    canonical slot is still empty (stable, first-non-empty-wins).
    """
    out: dict[str, str] = {}
    for col, field in (mapping or {}).items():
        if field not in CANONICAL_FIELDS or field == "ignore":
            continue
        val = _clean(row.get(col))
        if val is None:
            continue
        if field not in out:
            out[field] = val
    return out


def company_domain_for_row(fields: dict) -> str | None:
    """Best usable *company* domain for the email-finder path.

    Prefers an explicit company_domain column; falls back to the domain of the
    person's email only when it is not a free/personal provider.
    """
    dom = normalize_domain(fields.get("company_domain"))
    if dom:
        return dom
    email_dom = domain_from_email(fields.get("email"))
    if email_dom and email_dom not in _FREE_EMAIL_DOMAINS:
        return email_dom
    return None


def build_lead_fields(row: dict, mapping: dict) -> dict:
    """Canonical, normalized field bundle used by byol_discovery_service.

    Splits full_name into first/last when the discrete columns are absent, and
    normalizes LinkedIn URLs / domain. Person vs company LinkedIn is separated.
    """
    f = apply_mapping(row, mapping)

    first = f.get("first_name")
    last = f.get("last_name")
    full = f.get("full_name")
    if full and not (first or last):
        parts = full.split()
        if len(parts) == 1:
            first = parts[0]
        elif len(parts) >= 2:
            first, last = parts[0], " ".join(parts[1:])
    if not full and (first or last):
        full = " ".join(p for p in [first, last] if p) or None

    li = f.get("linkedin_url")
    person_li = normalize_linkedin_url(li) if is_person_linkedin(li) else None
    # A generic linkedin_url that is actually a /company/ URL counts as a company LI.
    company_li = normalize_linkedin_url(f.get("company_linkedin_url"))
    if not company_li and is_company_linkedin(li):
        company_li = normalize_linkedin_url(li)

    return {
        "first_name": first,
        "last_name": last,
        "full_name": full,
        "job_title": f.get("job_title"),
        "linkedin": person_li,
        "company_name": f.get("company_name"),
        "company_domain": normalize_domain(f.get("company_domain")),
        "company_linkedin": company_li,
        "email": _clean(f.get("email")),
        "country": f.get("country"),
        "seniority": f.get("seniority"),
    }


# ---------------------------------------------------------------------------
# Row classification (pure / deterministic — unit tested)
# ---------------------------------------------------------------------------

def classify_row(row: dict, mapping: dict) -> str:
    """Classify a raw row as "person" | "company" | "unresolvable".

    - person: has a person name AND at least one way to reach/resolve them
      (a person LinkedIn URL, a direct email, or a company signal to find an
      email against).
    - company: no person name, but a company signal (name / domain / company
      LinkedIn) we can scrape employees from.
    - unresolvable: neither — surfaced to the review panel, never enrolled.
    """
    fields = build_lead_fields(row, mapping)

    has_name = bool(fields.get("full_name") or fields.get("first_name"))
    has_person_li = bool(fields.get("linkedin"))
    has_email = bool(fields.get("email"))
    company_signal = bool(
        fields.get("company_name")
        or fields.get("company_domain")
        or fields.get("company_linkedin")
    )

    if has_name and (has_person_li or has_email or company_signal):
        return "person"
    if not has_name and company_signal:
        return "company"
    return "unresolvable"


# ---------------------------------------------------------------------------
# Deterministic mapping heuristic (LLM fallback + tests)
# ---------------------------------------------------------------------------

def _heuristic_field(header: str) -> tuple[str, float]:
    """Map one header to (canonical_field, confidence) via substring heuristics."""
    h = (header or "").strip().lower()
    if not h:
        return "ignore", 0.3
    for needle, field in _HEADER_HEURISTICS:
        if needle in h:
            # exact-ish match → high confidence, loose substring → medium
            conf = 0.9 if h == needle else 0.7
            return field, conf
    return "ignore", 0.4


def heuristic_mapping(columns: list[str]) -> dict:
    """Deterministic column→field mapping used when the LLM is unavailable.

    Guarantees at most one column per single-value field (first wins; later
    duplicates fall back to "ignore").
    """
    mapping: dict[str, str] = {}
    confidence: dict[str, float] = {}
    used_single: set[str] = set()
    _single = {
        "first_name", "last_name", "full_name", "email",
        "linkedin_url", "company_name", "company_domain", "company_linkedin_url",
    }
    for col in columns:
        field, conf = _heuristic_field(str(col))
        if field in _single and field in used_single:
            field, conf = "ignore", 0.4
        if field in _single:
            used_single.add(field)
        mapping[str(col)] = field
        confidence[str(col)] = conf

    questions = _questions_for_low_confidence(mapping, confidence)
    return {"mapping": mapping, "confidence": confidence, "questions": questions}


def _questions_for_low_confidence(mapping: dict, confidence: dict, threshold: float = 0.6) -> list[dict]:
    questions: list[dict] = []
    for col, field in mapping.items():
        if confidence.get(col, 1.0) >= threshold:
            continue
        questions.append({
            "id": f"col::{col}",
            "column": col,
            "question": f'Which field is the "{col}" column?',
            "widget": "single_select",
            "allow_free_text": False,
            "options": [
                {"value": f, "label": _FIELD_LABELS.get(f, f)}
                for f in _QUESTION_FIELD_ORDER
            ],
            "suggested": field,
        })
        if len(questions) >= 3:
            break
    return questions


_FIELD_LABELS = {
    "first_name": "First name",
    "last_name": "Last name",
    "full_name": "Full name",
    "job_title": "Job title",
    "linkedin_url": "LinkedIn URL",
    "company_name": "Company name",
    "company_domain": "Company website / domain",
    "company_linkedin_url": "Company LinkedIn URL",
    "email": "Email",
    "country": "Country / location",
    "seniority": "Seniority",
    "ignore": "Ignore this column",
}

_QUESTION_FIELD_ORDER = [
    "full_name", "first_name", "last_name", "job_title", "email",
    "linkedin_url", "company_name", "company_domain", "company_linkedin_url",
    "country", "seniority", "ignore",
]


async def propose_column_mapping(
    columns: list[str],
    sample_rows: list[dict],
    account_id: str | None = None,
) -> dict:
    """Propose a column→canonical-field mapping via one LLM call.

    Returns {"mapping": {...}, "confidence": {...}, "questions": [...]}.
    On any LLM failure (or unparseable output) falls back to `heuristic_mapping`
    so the upload flow never hard-blocks on the AI provider. Never spends paid
    credits when it short-circuits to the heuristic.
    """
    columns = [str(c) for c in columns]
    if not columns:
        return {"mapping": {}, "confidence": {}, "questions": []}

    try:
        from services.openrouter_service import OpenRouterClient
        from config import get_settings
        from utils.prompts import get_system_prompt, LEAD_COLUMN_MAPPING_SYSTEM_PROMPT

        settings = get_settings()
        system_prompt = await get_system_prompt("lead_column_mapping", account_id)
        if not system_prompt:
            system_prompt = LEAD_COLUMN_MAPPING_SYSTEM_PROMPT

        user_lines = ["Column headers:", ", ".join(columns), "", "Sample rows (JSON):"]
        import json as _json
        user_lines.append(_json.dumps(sample_rows[:5], default=str)[:4000])

        client = OpenRouterClient()
        try:
            result = await client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "\n".join(user_lines)},
                ],
                model=settings.assessment_model,
                fallback_models=settings.fallback_models,
                temperature=0.1,
                max_tokens=2048,
                response_format={"type": "json_object"},
                account_id=account_id,
                feature="lead_column_mapping",
            )
        finally:
            await client.close()

        if isinstance(result, dict) and isinstance(result.get("mapping"), dict):
            return _sanitize_llm_mapping(result, columns)
        logger.warning("propose_column_mapping: LLM returned unexpected shape, using heuristic")
    except Exception as exc:
        logger.warning(f"propose_column_mapping: LLM failed ({exc}); using heuristic")

    return heuristic_mapping(columns)


def _sanitize_llm_mapping(result: dict, columns: list[str]) -> dict:
    """Coerce an LLM mapping result into the canonical contract.

    - Drop unknown canonical fields → "ignore".
    - Ensure every input column has an entry (default "ignore").
    - Preserve/normalize questions; synthesize from low confidence when absent.
    """
    raw_map = result.get("mapping") or {}
    raw_conf = result.get("confidence") or {}
    mapping: dict[str, str] = {}
    confidence: dict[str, float] = {}
    for col in columns:
        field = raw_map.get(col)
        if field not in CANONICAL_FIELDS:
            field = "ignore"
        mapping[col] = field
        try:
            confidence[col] = float(raw_conf.get(col, 0.8))
        except (TypeError, ValueError):
            confidence[col] = 0.8

    questions = result.get("questions")
    if not isinstance(questions, list) or not questions:
        questions = _questions_for_low_confidence(mapping, confidence)
    else:
        # Keep only questions that reference a real column; clamp to 3.
        cleaned = []
        for q in questions:
            if not isinstance(q, dict):
                continue
            col = q.get("column") or (q.get("id", "").replace("col::", "") if isinstance(q.get("id"), str) else None)
            if col not in mapping:
                continue
            q.setdefault("column", col)
            q.setdefault("id", f"col::{col}")
            q.setdefault("widget", "single_select")
            q.setdefault("allow_free_text", False)
            if not q.get("options"):
                q["options"] = [{"value": f, "label": _FIELD_LABELS.get(f, f)} for f in _QUESTION_FIELD_ORDER]
            cleaned.append(q)
            if len(cleaned) >= 3:
                break
        questions = cleaned

    return {"mapping": mapping, "confidence": confidence, "questions": questions}
