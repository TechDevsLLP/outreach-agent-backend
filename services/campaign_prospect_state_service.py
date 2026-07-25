"""Persistence contract for campaign-scoped prospect state.

The natural key is ``(account_id, campaign_id, prospect_id, scoring_version)``.
Callers pass strings at this boundary so tenant identifiers cannot drift between
ObjectId and string representations.  Database indexes are intentionally
declared in the remediation migration, not lazily from request code.
"""

from datetime import datetime
from typing import Any, Iterable, Optional

from pymongo import ReturnDocument, UpdateOne

import database


DEFAULT_SCORING_VERSION = "campaign-fit-v1"
TERMINAL_ENRICHMENT_STATES = {
    "succeeded",
    "not_found",
    "retryable_failure",
    "blocked",
    "exhausted",
}
_ALLOWED_PREVIOUS_STATES = {
    "queued": [None, "retryable_failure"],
    "running": ["queued", "retryable_failure"],
    "succeeded": ["running"],
    "not_found": ["running"],
    "retryable_failure": ["running"],
    "blocked": ["queued", "running", "retryable_failure"],
    "exhausted": ["running", "retryable_failure"],
}


def _collection(collection=None):
    return (
        collection if collection is not None else database.db["campaign_prospect_state"]
    )


def natural_key(
    account_id,
    campaign_id,
    prospect_id,
    scoring_version: str = DEFAULT_SCORING_VERSION,
) -> dict:
    return {
        "account_id": str(account_id),
        "campaign_id": str(campaign_id),
        "prospect_id": str(prospect_id),
        "scoring_version": scoring_version,
    }


def _score_update_filter_and_doc(
    *,
    account_id,
    campaign_id,
    prospect_id,
    result: dict,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    cohort_id: Optional[str] = None,
    cohort_label: Optional[str] = None,
    now: Optional[datetime] = None,
) -> tuple[dict, dict]:
    """Build the (filter, update) pair for an idempotent score upsert without
    collapsing ``0`` into null. Shared by score_update_operation() (bulk_write)
    and persist_campaign_score() (single update_one) so neither has to reach
    into a pymongo UpdateOne's private attributes."""
    now = now or datetime.utcnow()
    raw_value = result.get("fit_score")
    value = float(raw_value) if raw_value is not None else None
    key = natural_key(account_id, campaign_id, prospect_id, scoring_version)
    fields = {
        "score": {
            "value": value,
            "version": scoring_version,
            "company_value": result.get("company_fit_score"),
            "prospect_value": result.get("prospect_fit_score"),
            "priority_tier": result.get("priority_tier"),
            "reasoning": result.get("reasoning"),
            "completeness": result.get("completeness", result.get("data_completeness")),
            "breakdown": result.get("breakdown") or {},
            "scored_at": now if value is not None else None,
            "error_code": result.get("error_code"),
        },
        "updated_at": now,
    }
    if cohort_id is not None:
        fields.update(
            {
                "cohort_id": str(cohort_id),
                "cohort_label": cohort_label,
                "selected_at": now,
            }
        )
    update = {
        "$set": fields,
        "$setOnInsert": {
            **key,
            "created_at": now,
            "enrichment": {"state": "queued", "queued_at": now, "attempt": 0},
        },
    }
    return key, update


def score_update_operation(
    *,
    account_id,
    campaign_id,
    prospect_id,
    result: dict,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    cohort_id: Optional[str] = None,
    cohort_label: Optional[str] = None,
    now: Optional[datetime] = None,
) -> UpdateOne:
    """Build an idempotent score upsert without collapsing ``0`` into null."""
    filter_, update = _score_update_filter_and_doc(
        account_id=account_id,
        campaign_id=campaign_id,
        prospect_id=prospect_id,
        result=result,
        scoring_version=scoring_version,
        cohort_id=cohort_id,
        cohort_label=cohort_label,
        now=now,
    )
    return UpdateOne(filter_, update, upsert=True)


async def persist_campaign_score(
    *,
    account_id,
    campaign_id,
    prospect_id,
    result: dict,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    cohort_id: Optional[str] = None,
    cohort_label: Optional[str] = None,
    collection=None,
) -> None:
    filter_, update = _score_update_filter_and_doc(
        account_id=account_id,
        campaign_id=campaign_id,
        prospect_id=prospect_id,
        result=result,
        scoring_version=scoring_version,
        cohort_id=cohort_id,
        cohort_label=cohort_label,
    )
    await _collection(collection).update_one(filter_, update, upsert=True)


async def persist_campaign_scores(
    operations: Iterable[UpdateOne], collection=None
) -> None:
    operations = list(operations)
    if operations:
        await _collection(collection).bulk_write(operations, ordered=False)


async def bulk_write_state_operations(
    operations: Iterable[UpdateOne], collection=None
) -> None:
    """Generic bulk_write sink for pre-built UpdateOne operations against
    campaign_prospect_state (e.g. a batch of transition_enrichment_operation()
    calls with per-item state/result) — for callers where the ops aren't all
    score updates, so reaching for persist_campaign_scores() would be misleading."""
    operations = list(operations)
    if operations:
        await _collection(collection).bulk_write(operations, ordered=False)


async def ensure_cohort_membership(
    *,
    account_id,
    campaign_id,
    prospect_ids: Iterable,
    cohort_id: str,
    cohort_label: str,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    collection=None,
) -> None:
    now = datetime.utcnow()
    operations = []
    for prospect_id in prospect_ids:
        key = natural_key(account_id, campaign_id, prospect_id, scoring_version)
        operations.append(
            UpdateOne(
                key,
                {
                    "$set": {
                        "cohort_id": str(cohort_id),
                        "cohort_label": cohort_label,
                        "selected_at": now,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        **key,
                        "score": {"value": None, "version": scoring_version},
                        "enrichment": {
                            "state": "queued",
                            "queued_at": now,
                            "attempt": 0,
                        },
                        "created_at": now,
                    },
                },
                upsert=True,
            )
        )
    await persist_campaign_scores(operations, collection=collection)


def _transition_filter_and_doc(
    *,
    account_id,
    campaign_id,
    prospect_id,
    state: str,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    outcome: Optional[str] = None,
    result: Optional[dict] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    now: Optional[datetime] = None,
) -> tuple[dict, dict]:
    """Build the (filter, update) pair for one enrichment state-machine transition.
    Shared by transition_enrichment() (find_one_and_update) and
    transition_enrichment_operation() (bulk_write) so neither has to reach into
    a pymongo UpdateOne's private attributes."""
    if state not in _ALLOWED_PREVIOUS_STATES:
        raise ValueError(f"unsupported enrichment state: {state}")
    now = now or datetime.utcnow()
    query = natural_key(account_id, campaign_id, prospect_id, scoring_version)
    allowed = _ALLOWED_PREVIOUS_STATES[state]
    if None in allowed:
        non_null = [value for value in allowed if value is not None]
        query["$or"] = [
            {"enrichment.state": {"$exists": False}},
            {"enrichment.state": {"$in": non_null}},
        ]
    else:
        query["enrichment.state"] = {"$in": allowed}

    fields = {
        "enrichment.state": state,
        "enrichment.outcome": outcome,
        "enrichment.result": result,
        "enrichment.error_code": error_code,
        "enrichment.error_message": (error_message or "")[:500] or None,
        "updated_at": now,
    }
    update: dict = {"$set": fields}
    if state == "queued":
        fields["enrichment.queued_at"] = now
    elif state == "running":
        fields["enrichment.started_at"] = now
        fields["enrichment.completed_at"] = None
        update["$inc"] = {"enrichment.attempt": 1}
    elif state in TERMINAL_ENRICHMENT_STATES:
        fields["enrichment.completed_at"] = now

    return query, update


async def transition_enrichment(
    *,
    account_id,
    campaign_id,
    prospect_id,
    state: str,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    outcome: Optional[str] = None,
    result: Optional[dict] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    collection=None,
) -> Optional[dict]:
    """Atomically move one cohort member through the enrichment state machine."""
    query, update = _transition_filter_and_doc(
        account_id=account_id,
        campaign_id=campaign_id,
        prospect_id=prospect_id,
        state=state,
        scoring_version=scoring_version,
        outcome=outcome,
        result=result,
        error_code=error_code,
        error_message=error_message,
    )
    return await _collection(collection).find_one_and_update(
        query,
        update,
        return_document=ReturnDocument.AFTER,
    )


def transition_enrichment_operation(
    *,
    account_id,
    campaign_id,
    prospect_id,
    state: str,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    outcome: Optional[str] = None,
    result: Optional[dict] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    now: Optional[datetime] = None,
) -> UpdateOne:
    """Build one enrichment-state transition as an UpdateOne for bulk_write.

    Same state machine as transition_enrichment(), without the round trip to
    read back the updated document — for callers moving many prospects through
    the same or different terminal states at once (e.g. a cohort finishing
    enrichment with per-prospect success/failure outcomes).
    """
    filter_, update = _transition_filter_and_doc(
        account_id=account_id,
        campaign_id=campaign_id,
        prospect_id=prospect_id,
        state=state,
        scoring_version=scoring_version,
        outcome=outcome,
        result=result,
        error_code=error_code,
        error_message=error_message,
        now=now,
    )
    return UpdateOne(filter_, update)


async def bulk_transition_enrichment(
    *,
    account_id,
    campaign_id,
    prospect_ids: Iterable,
    state: str,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    outcome: Optional[str] = None,
    result: Optional[dict] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    collection=None,
) -> None:
    """Move many prospects through the same enrichment transition in one bulk_write.

    For unconditional, same-state transitions across a cohort (e.g. marking a
    whole batch "running" before work starts) — avoids one find_one_and_update
    round trip per prospect.
    """
    now = datetime.utcnow()
    operations = [
        transition_enrichment_operation(
            account_id=account_id,
            campaign_id=campaign_id,
            prospect_id=prospect_id,
            state=state,
            scoring_version=scoring_version,
            outcome=outcome,
            result=result,
            error_code=error_code,
            error_message=error_message,
            now=now,
        )
        for prospect_id in prospect_ids
    ]
    if operations:
        await _collection(collection).bulk_write(operations, ordered=False)


async def get_campaign_prospect_state(
    *,
    account_id,
    campaign_id,
    prospect_id,
    scoring_version: str = DEFAULT_SCORING_VERSION,
    collection=None,
) -> Optional[dict]:
    return await _collection(collection).find_one(
        natural_key(account_id, campaign_id, prospect_id, scoring_version)
    )


def campaign_score_projection(state: Optional[dict]) -> dict:
    """Return the stable public campaign-score contract.

    ``0`` is a valid score and must never be collapsed into the unscored
    ``None`` state.  Keeping this projection in the persistence service makes
    list and detail endpoints expose the exact same shape.
    """
    score = (state or {}).get("score") or {}
    return {
        "value": score.get("value"),
        "version": score.get("version") or (state or {}).get("scoring_version"),
        "reasoning": score.get("reasoning"),
        "completeness": score.get("completeness"),
        "breakdown": score.get("breakdown") or {},
        "scored_at": score.get("scored_at"),
    }


def campaign_enrichment_projection(
    state: Optional[dict], *, shared_source: Optional[str] = None
) -> dict:
    """Expose campaign enrichment provenance without implying shared ownership."""
    enrichment = dict((state or {}).get("enrichment") or {})
    result = enrichment.get("result") or {}
    source = (
        enrichment.get("source")
        or (result.get("source") if isinstance(result, dict) else None)
        or (result.get("provider") if isinstance(result, dict) else None)
    )
    enrichment.update(
        {
            "ownership": "campaign",
            "source": source,
            "shared_pool_source": shared_source,
        }
    )
    return enrichment


def campaign_state_page_pipeline(
    *,
    account_id,
    campaign_id,
    scoring_version: str,
    prospect_ids: Iterable,
    min_score: Optional[float],
    page: int,
    page_size: int,
    sort_order: str,
) -> list[dict[str, Any]]:
    """Build a score-first page over the authoritative campaign cohort.

    Membership is resolved by the caller from tenant-scoped campaign
    enrollments.  The resulting identifiers constrain campaign state before
    score filtering, ordering, and pagination.  Shared prospect and tenant
    overlay joins intentionally happen only after this pipeline returns a
    page, preventing account-level legacy scores from influencing rank.
    """
    match: dict[str, Any] = {
        "account_id": str(account_id),
        "campaign_id": str(campaign_id),
        "scoring_version": scoring_version,
        "prospect_id": {"$in": [str(value) for value in prospect_ids]},
    }
    if min_score is not None:
        match["score.value"] = {"$gte": min_score}

    direction = -1 if sort_order == "desc" else 1
    skip = (page - 1) * page_size
    return [
        {"$match": match},
        {
            "$addFields": {
                "_score_missing": {
                    "$cond": [{"$eq": [{"$ifNull": ["$score.value", None]}, None]}, 1, 0]
                }
            }
        },
        {
            "$facet": {
                "total": [{"$count": "n"}],
                "page": [
                    # Unscored rows are always last; zero remains a scored
                    # numeric value. The prospect-id tie breaker stabilizes
                    # pagination across requests.
                    {"$sort": {"_score_missing": 1, "score.value": direction, "prospect_id": 1}},
                    {"$skip": skip},
                    {"$limit": page_size},
                    {
                        "$project": {
                            "_id": 0,
                            "prospect_id": 1,
                            "score": 1,
                            "scoring_version": 1,
                            "enrichment": 1,
                            "cohort_id": 1,
                            "cohort_label": 1,
                        }
                    },
                ],
            }
        },
    ]
