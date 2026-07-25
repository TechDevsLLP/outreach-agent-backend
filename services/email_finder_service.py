"""
Email finder service — thin facade over services/growthtoolkit_service.py.
Looks up work emails by (first_name, last_name, company_domain).

Public surface (find_email, find_emails, EmailLookupEntry, _split_full_name,
_extract_first_last) is kept stable for existing consumers:
curated_discovery_service, enrichment_pipeline, onboarding_prospect_service,
employee_selection_service.
"""

import asyncio
import logging
from typing import TypedDict

from services import growthtoolkit_service
from services.growthtoolkit_service import GrowthToolkitError

logger = logging.getLogger(__name__)

# Codes that mean "GrowthToolkit account is out of Email Finder credits" — checked
# against the raised error's own `code` rather than the exception subclass, since
# GrowthToolkit's own credit-code allowlist (services/growthtoolkit_service._CREDIT_CODES)
# doesn't cover every code the provider actually returns for this (e.g.
# `recharge_credit_immediately`, observed in production).
_CREDIT_ERROR_CODES = {
    "402", "406", "407", "428",
    "payment_required", "credits_exhausted", "credits_low",
    "recharge_credit_immediately", "insufficient_credits",
}


class EmailLookupEntry(TypedDict):
    first_name: str
    last_name: str
    domain: str
    key: str


def _split_full_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split()
    if len(parts) < 2:
        return ("", "")
    return (parts[0], " ".join(parts[1:]))


def _extract_first_last(doc: dict) -> tuple[str, str]:
    first = (doc.get("first_name") or "").strip()
    last = (doc.get("last_name") or "").strip()
    if first and last:
        return (first, last)
    return _split_full_name(doc.get("full_name") or "")


async def _call_api(
    first_name: str,
    last_name: str,
    domain: str,
    *,
    account_id: str | None = None,
    credit_warnings: list[str] | None = None,
) -> str | None:
    """Delegate to the consolidated GrowthToolkit client. Preserves the
    historical contract: returns the email or None, never raises.

    When `credit_warnings` is passed, a credit-exhaustion error appends one
    human-readable message to it (deduped) so the caller can surface it to
    the user (e.g. on the campaign doc) instead of it only reaching logs.
    """
    try:
        return await growthtoolkit_service.find_email(
            first_name, last_name, domain, account_id=account_id
        )
    except GrowthToolkitError as e:
        if e.code in _CREDIT_ERROR_CODES:
            logger.error(
                f"GrowthToolkit email finder: out of credits (code={e.code}) — "
                f"lookup for {first_name} {last_name} @ {domain} skipped. "
                f"Recharge GrowthToolkit Email Finder credits to resume."
            )
            if credit_warnings is not None:
                msg = (
                    "GrowthToolkit email finder ran out of credits "
                    f"(code={e.code}). Some prospects may be missing an email — "
                    "recharge Email Finder credits and re-run discovery to fill them in."
                )
                if msg not in credit_warnings:
                    credit_warnings.append(msg)
        else:
            logger.warning(
                f"GrowthToolkit email finder error for {first_name} {last_name} @ {domain}: {e} (code={e.code})"
            )
        return None
    except Exception as e:
        logger.error(f"GrowthToolkit unexpected error for {first_name} {last_name} @ {domain}: {e}")
        return None


async def find_email(
    first_name: str,
    last_name: str,
    domain: str,
    *,
    account_id: str | None = None,
) -> str | None:
    return await _call_api(first_name, last_name, domain, account_id=account_id)


async def find_emails(
    entries: list[EmailLookupEntry],
    *,
    concurrency: int = 10,
    account_id: str | None = None,
    credit_warnings: list[str] | None = None,
) -> dict[str, str | None]:
    """Look up emails for a batch of prospects.

    `credit_warnings`, when passed, receives a human-readable message (deduped)
    if a GrowthToolkit credit-exhaustion error is hit — the caller can persist
    it (e.g. onto the campaign doc's `discovery_warnings`) so the UI can show
    it. Missing emails never abort the batch — prospects can still enroll on
    LinkedIn-only channels.
    """
    if not entries:
        return {}

    results: dict[str, str | None] = {e["key"]: None for e in entries}
    sem = asyncio.Semaphore(concurrency)

    async def _lookup(entry: EmailLookupEntry) -> None:
        async with sem:
            email = await _call_api(
                entry["first_name"], entry["last_name"], entry["domain"],
                account_id=account_id,
                credit_warnings=credit_warnings,
            )
            if email:
                results[entry["key"]] = email

    await asyncio.gather(*[_lookup(e) for e in entries])

    found = sum(1 for v in results.values() if v)
    logger.info(f"GrowthToolkit email finder: {found}/{len(entries)} emails found")
    return results
