"""
Event-driven company-cascade rotation.

A cascade group links several prospects at the same company (keyed by
``cascade_group_id`` — the company LinkedIn URL for discovery-created groups).
Position 0 is the primary (a normal enrollment); positions 1+ are minimal
backup docs stored with ``status: "cascade_waiting"`` / ``cascade_status:
"waiting"``, no generated messages and no email lookup done.

When the primary's sequence ends "not_replied" (max touches or sequence end,
never a reply), the engine calls ``activate_next_backup`` which:

1. finds the next waiting backup in the group (lowest ``cascade_position``
   above the primary's),
2. runs a GrowthToolkit email lookup for the backup's prospect if it has no
   email yet (skipped gracefully when name/domain fields are missing),
3. initializes the enrollment on the campaign's sequence graph — status
   "active", ``sequence_state`` at the start node in phase "pending_send" with
   ``next_action_at = now`` (mirrors curated discovery's plan initialization),
4. triggers message generation fail-soft.

Legacy time-based cascades (docs with a concrete ``cascade_activate_at``) keep
being handled by ``campaign_engine._activate_due_cascade_enrollments``; this
module only serves the event-driven path (``cascade_activate_at: None``).
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from bson import ObjectId

import database
from services import sequence_service as seq

logger = logging.getLogger(__name__)


def _id_variants(value) -> list:
    """Return [ObjectId, str] variants of an id for cross-schema matching."""
    variants: list = []
    try:
        variants.append(ObjectId(str(value)))
    except Exception:
        pass
    variants.append(str(value))
    return variants


def _domain_from_prospect(prospect: dict) -> str:
    """Best-effort company domain for the email finder."""
    domain = (prospect.get("company_domain") or "").strip()
    if domain:
        return domain
    website = (prospect.get("company_website") or "").strip()
    if not website:
        return ""
    host = website.replace("https://", "").replace("http://", "").split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host.strip()


async def _ensure_prospect_email(prospect: dict, account_id) -> None:
    """Look up (and persist) the prospect's work email via GrowthToolkit.

    No-op when the prospect already has an email. Skips gracefully — with a log
    line, never an exception — when first/last name or company domain are
    missing, or when the lookup finds nothing.
    """
    if prospect.get("email"):
        return
    try:
        from services.email_finder_service import (
            EmailLookupEntry, _extract_first_last, find_emails,
        )
        first, last = _extract_first_last(prospect)
        domain = _domain_from_prospect(prospect)
        if not (first and last and domain):
            logger.info(
                "Cascade rotation: skipping email lookup for prospect %s "
                "(missing first/last name or company domain)",
                prospect.get("_id"),
            )
            return
        key = str(prospect.get("_id"))
        results = await find_emails(
            [EmailLookupEntry(first_name=first, last_name=last, domain=domain, key=key)],
            account_id=str(account_id) if account_id else None,
        )
        email = results.get(key)
        if not email:
            return
        now = datetime.utcnow()
        await database.prospects_collection.update_one(
            {"_id": prospect["_id"]},
            {"$set": {
                "email": email,
                "email_source": "growthtoolkit_cascade",
                "email_found_at": now,
            }},
        )
        prospect["email"] = email
        logger.info("Cascade rotation: found email for prospect %s", prospect.get("_id"))
    except Exception as e:
        logger.warning(
            "Cascade rotation: email lookup failed for prospect %s: %s",
            prospect.get("_id"), e,
        )


async def _generate_messages_fail_soft(enrollment: dict, prospect: dict, campaign: dict) -> None:
    """Generate first-touch messages for the freshly-activated backup.

    Fail-soft: on any error the enrollment stays active and the engine's lazy
    per-node generation / legacy-flat-message fallback picks it up later.
    """
    try:
        from services.campaign_message_generator_service import (
            generate_messages_for_enrollment,
        )
        from services.openrouter_service import OpenRouterClient

        client = OpenRouterClient()
        try:
            await generate_messages_for_enrollment(enrollment, prospect, campaign, client)
        finally:
            await client.close()
    except Exception as e:
        logger.warning(
            "Cascade rotation: message generation failed for enrollment %s: %s",
            enrollment.get("_id"), e,
        )


async def activate_next_backup(primary_enrollment: dict) -> Optional[dict]:
    """Activate the next waiting backup in the primary's cascade group.

    Returns the activated enrollment dict (with the applied updates merged in)
    or None when there is no backup left / activation was not possible.
    """
    group_id = primary_enrollment.get("cascade_group_id")
    campaign_id = primary_enrollment.get("campaign_id")
    if not group_id or not campaign_id:
        return None
    try:
        primary_position = int(primary_enrollment.get("cascade_position") or 0)
    except (TypeError, ValueError):
        primary_position = 0

    backup = await database.campaign_enrollments_collection.find_one(
        {
            "campaign_id": {"$in": _id_variants(campaign_id)},
            "cascade_group_id": group_id,
            "status": "cascade_waiting",
            "cascade_position": {"$gt": primary_position},
        },
        sort=[("cascade_position", 1)],
    )
    if not backup:
        return None

    # Load the backup's prospect.
    prospect = None
    try:
        prospect = await database.prospects_collection.find_one(
            {"_id": ObjectId(str(backup.get("prospect_id")))}
        )
    except Exception:
        prospect = None
    if not prospect:
        logger.warning(
            "Cascade rotation: prospect %s missing for backup enrollment %s — skipping",
            backup.get("prospect_id"), backup.get("_id"),
        )
        return None

    campaign = None
    try:
        campaign = await database.campaigns_collection.find_one(
            {"_id": ObjectId(str(campaign_id))}
        )
    except Exception:
        campaign = None
    if not campaign:
        logger.warning(
            "Cascade rotation: campaign %s missing for backup enrollment %s — skipping",
            campaign_id, backup.get("_id"),
        )
        return None

    # Backups skip the discovery email lookup — run it now if needed.
    await _ensure_prospect_email(prospect, backup.get("account_id"))

    now = datetime.utcnow()
    set_fields: dict = {
        "status": "active",
        "cascade_status": "activated",
        "cascade_activated_at": now,
        "next_action_at": now,
        "last_activity_at": now,
        "last_transition_reason": "cascade_rotation_primary_not_replied",
    }

    graph = campaign.get("sequence_graph") or {}
    if graph:
        start_node_id = seq.get_start_node_id(graph)
        state = seq.build_initial_sequence_state(graph, now=now, start_node_id=start_node_id)
        state["next_action_at"] = now  # first touch is due immediately
        set_fields["sequence_state"] = state
        start_node = seq.get_node(graph, start_node_id) or {}
        set_fields["smart_campaign_channel"] = seq.engine_channel(start_node.get("channel"))
        set_fields["message_gen_status"] = "pending"

    await database.campaign_enrollments_collection.update_one(
        {"_id": backup["_id"], "status": "cascade_waiting"},
        {"$set": set_fields},
    )
    backup.update(set_fields)

    # The backup now counts as enrolled + active (cascade_waiting docs are
    # excluded from total_enrolled-style counts until activation).
    try:
        await database.campaigns_collection.update_one(
            {"_id": campaign["_id"]},
            {"$inc": {"active_count": 1, "total_enrolled": 1}},
        )
    except Exception as e:
        logger.warning("Cascade rotation: counter update failed for campaign %s: %s", campaign_id, e)

    # First-touch copy — fail-soft (engine falls back to lazy generation).
    await _generate_messages_fail_soft(backup, prospect, campaign)

    logger.info(
        "Cascade rotation: activated enrollment %s (group=%s pos=%s) for campaign %s",
        backup.get("_id"), group_id, backup.get("cascade_position"), campaign_id,
    )
    return backup
