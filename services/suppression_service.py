"""
Suppression service.
Gate sends against suppression list. Used by campaign_engine before each send.
"""
import logging
from bson import ObjectId
import database

logger = logging.getLogger(__name__)


async def is_suppressed(account_id: str, prospect: dict) -> bool:
    """Return True if this prospect is on the suppression list for this account."""
    identifiers = []
    email = prospect.get("email") or prospect.get("personal_email")
    if email:
        identifiers.append({"account_id": account_id, "identifier_type": "email", "identifier": email.lower()})
    li = prospect.get("linkedin")
    if li:
        identifiers.append({"account_id": account_id, "identifier_type": "linkedin_url", "identifier": li.lower()})
    pid = str(prospect.get("_id", ""))
    if pid:
        identifiers.append({"account_id": account_id, "identifier_type": "prospect_id", "identifier": pid})
    if not identifiers:
        return False
    doc = await database.suppressions_collection.find_one({"$or": identifiers})
    return doc is not None


async def record_suppression(
    account_id: str,
    identifier_type: str,
    identifier: str,
    reason: str,
    prospect_id: str = None,
    enrollment_id: str = None,
    conversation_id: str = None,
) -> None:
    """Insert a suppression record (idempotent via upsert)."""
    doc = {
        "account_id": account_id,
        "identifier_type": identifier_type,
        "identifier": identifier.lower(),
        "reason": reason,
        "source": "system",
        "prospect_id": prospect_id,
        "enrollment_id": enrollment_id,
        "conversation_id": conversation_id,
    }
    try:
        await database.suppressions_collection.update_one(
            {"account_id": account_id, "identifier_type": identifier_type, "identifier": identifier.lower()},
            {"$setOnInsert": doc},
            upsert=True,
        )
    except Exception as e:
        logger.warning(f"Suppression record failed: {e}")
