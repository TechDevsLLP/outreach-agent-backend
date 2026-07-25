"""Tenant-owned outreach activity for people in the shared prospect pool."""

from datetime import datetime
from typing import Any

from bson import ObjectId

import database


def _oid(value: Any, name: str) -> ObjectId:
    if isinstance(value, ObjectId):
        return value
    if not ObjectId.is_valid(str(value)):
        raise ValueError(f"invalid {name}")
    return ObjectId(str(value))


def _variants(value: Any, name: str) -> list[Any]:
    oid = _oid(value, name)
    return [str(oid), oid]


async def get_prospect_activity(
    *, account_id: Any, prospect_id: Any, enrollment: dict | None = None,
) -> dict:
    """Read activity from an existing tenant overlay, merged with enrollment state."""
    state = await database.prospect_state_collection.find_one(
        {
            "account_id": {"$in": _variants(account_id, "account_id")},
            "prospect_id": {"$in": _variants(prospect_id, "prospect_id")},
        },
        {
            "connection_request_sent_at": 1, "connection_accepted_at": 1,
            "connection_followup_sent_at": 1, "outreach_history": 1,
            "outreach_messages": 1, "status": 1,
        },
    ) or {}
    activity = dict(state)
    enrollment_activity = dict((enrollment or {}).get("linkedin_activity") or {})
    for key, value in enrollment_activity.items():
        if key != "outreach_history" and value is not None:
            activity[key] = value
    return activity


async def require_prospect_access(*, account_id: Any, prospect_id: Any) -> dict:
    """Require existing overlay or enrollment proof; never create ownership."""
    account_values = _variants(account_id, "account_id")
    prospect_values = _variants(prospect_id, "prospect_id")
    state = await database.prospect_state_collection.find_one(
        {"account_id": {"$in": account_values}, "prospect_id": {"$in": prospect_values}},
        {"_id": 1, "outreach_messages": 1},
    )
    enrollment = await database.campaign_enrollments_collection.find_one(
        {"account_id": {"$in": account_values}, "prospect_id": {"$in": prospect_values}},
        {"_id": 1, "campaign_id": 1, "linkedin_activity": 1},
    )
    if state is None and enrollment is None:
        raise PermissionError("prospect is outside tenant scope")
    return {"state": state, "enrollment": enrollment}


async def record_prospect_activity(
    *,
    account_id: Any,
    prospect_id: Any,
    fields: dict[str, Any] | None = None,
    event: dict | None = None,
    enrollment_id: Any | None = None,
    campaign_id: Any | None = None,
    only_if_missing: str | None = None,
    write_overlay: bool = True,
) -> bool:
    """Record activity on proven tenant state and optional sequence enrollment.

    Returns true only when at least one existing tenant document was modified.
    No operation uses upsert.
    """
    proof = await require_prospect_access(account_id=account_id, prospect_id=prospect_id)
    account_values = _variants(account_id, "account_id")
    prospect_values = _variants(prospect_id, "prospect_id")
    now = datetime.utcnow()
    fields = dict(fields or {})
    update: dict = {"$set": {**fields, "last_updated_at": now}}
    if event:
        update["$push"] = {"outreach_history": dict(event)}

    modified = 0
    state = proof.get("state")
    if state is not None and write_overlay:
        state_query: dict = {"_id": state["_id"], "account_id": {"$in": account_values}}
        if only_if_missing:
            state_query[only_if_missing] = None
        result = await database.prospect_state_collection.update_one(state_query, update)
        modified += int(result.modified_count)

    enrollment_query: dict | None = None
    if enrollment_id is not None:
        enrollment_query = {
            "_id": _oid(enrollment_id, "enrollment_id"),
            "account_id": {"$in": account_values},
            "prospect_id": {"$in": prospect_values},
        }
    elif campaign_id is not None:
        enrollment_query = {
            "campaign_id": {"$in": _variants(campaign_id, "campaign_id")},
            "account_id": {"$in": account_values},
            "prospect_id": {"$in": prospect_values},
        }
    elif state is None and proof.get("enrollment") is not None:
        enrollment_query = {"_id": proof["enrollment"]["_id"], "account_id": {"$in": account_values}}

    if enrollment_query is not None:
        if only_if_missing:
            enrollment_query[f"linkedin_activity.{only_if_missing}"] = None
        enrollment_update: dict = {
            "$set": {**{f"linkedin_activity.{key}": value for key, value in fields.items()}, "last_activity_at": now}
        }
        if event:
            enrollment_update["$push"] = {"linkedin_activity.outreach_history": dict(event)}
        result = await database.campaign_enrollments_collection.update_many(
            enrollment_query, enrollment_update
        )
        modified += int(result.modified_count)

    return modified > 0
