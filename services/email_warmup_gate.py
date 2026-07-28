"""Email warm-up gate.

Sending cold outreach from a mailbox that has not been warmed up burns the
domain's reputation, and no amount of good copy recovers from that. So email is
opt-in per mailbox: the user flips `warmup_complete` in Settings once their
warm-up provider says the mailbox is ready, and until then the platform behaves
as if no email channel exists — campaigns still run, over LinkedIn only.

This module is the single definition of "may we send from this mailbox". Both
the planner (which decides whether email is an available channel) and the
delivery layer (the last line before a provider call) consult it, so a campaign
planned while a mailbox was warmed still cannot send if the flag is turned off
before the send fires.
"""
import logging
from typing import Optional

import database

logger = logging.getLogger(__name__)

# Shown wherever the block surfaces: campaign page, skip reasons, send errors.
WARMUP_REQUIRED_MESSAGE = (
    "Email sending is paused until your mailbox is marked as warmed up. "
    "Turn on “Mailbox is warmed up” in Settings → Email Accounts once your "
    "warm-up is complete. LinkedIn steps continue to run in the meantime."
)

_SENDABLE_STATUSES = ("connected", "active")


def is_warmed_up(email_account: Optional[dict]) -> bool:
    """True when this mailbox is connected AND the user has confirmed warm-up."""
    if not email_account:
        return False
    if email_account.get("status") not in _SENDABLE_STATUSES:
        return False
    return bool(email_account.get("warmup_complete"))


async def get_warmed_email_account(account_id: str) -> Optional[dict]:
    """Return this tenant's first warmed-up mailbox, or None."""
    # account_id is written as a string on some rows and an ObjectId on others
    # (historical inconsistency between insert paths) — match both.
    from bson import ObjectId

    candidates: list = [str(account_id)]
    try:
        candidates.append(ObjectId(str(account_id)))
    except Exception:
        pass

    return await database.email_accounts_collection.find_one({
        "account_id": {"$in": candidates},
        "status": {"$in": list(_SENDABLE_STATUSES)},
        "warmup_complete": True,
    })


async def account_has_warmed_email(account_id: str) -> bool:
    """True when the tenant has at least one mailbox cleared for sending."""
    return await get_warmed_email_account(account_id) is not None
