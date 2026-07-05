from fastapi import APIRouter, Depends, HTTPException
from bson import ObjectId
from datetime import datetime, timezone

from auth import get_account_context
from database import company_profiles_collection
from models.company_profile import CompanyProfileUpsertRequest

router = APIRouter(prefix="/api/company-profile", tags=["Company Profile"])


def _serialize(doc: dict) -> dict:
    """Convert ObjectId fields to strings for JSON serialisation."""
    doc["_id"] = str(doc["_id"])
    if isinstance(doc.get("account_id"), ObjectId):
        doc["account_id"] = str(doc["account_id"])
    if isinstance(doc.get("user_id"), ObjectId):
        doc["user_id"] = str(doc["user_id"])
    return doc


@router.get("")
async def get_company_profile(ctx: dict = Depends(get_account_context)):
    """Return the company profile for this account, or null if not yet configured."""
    account_id = ObjectId(ctx["account"]["_id"])
    profile = await company_profiles_collection.find_one({"account_id": account_id})
    if profile is None:
        return {"profile": None}
    return {"profile": _serialize(profile)}


@router.patch("")
@router.put("")
async def upsert_company_profile(
    body: CompanyProfileUpsertRequest,
    ctx: dict = Depends(get_account_context),
):
    """Create or update the company profile for this account."""
    account_id = ObjectId(ctx["account"]["_id"])
    user_id = ObjectId(ctx["user"]["_id"])

    update_fields = body.model_dump(exclude_none=True)
    now = datetime.now(timezone.utc)

    existing = await company_profiles_collection.find_one({"account_id": account_id})

    if existing is None:
        # Create
        doc = {
            "account_id": account_id,
            "user_id": user_id,
            # Defaults for required fields not supplied in this request
            "company_name": "",
            "website_url": "",
            "description": "",
            "services": [],
            "target_market": "",
            "differentiators": [],
            "pain_points": [],
            "value_propositions": [],
            "tone_of_voice": "professional",
            "case_studies": [],
            "sender_name": ctx["account"].get("sender_name", ""),
            "sender_role": "",
            "outreach_strategy": "email_first",
            "connection_request_guidance": None,
            "email_guidance": None,
            "inmail_guidance": None,
            "icp_description": "",
            "scoring_weights": None,
            "created_at": now,
            "updated_at": now,
            **update_fields,
        }
        result = await company_profiles_collection.insert_one(doc)
        profile = await company_profiles_collection.find_one({"_id": result.inserted_id})
    else:
        # Update
        await company_profiles_collection.update_one(
            {"account_id": account_id},
            {"$set": {**update_fields, "updated_at": now}},
        )
        profile = await company_profiles_collection.find_one({"account_id": account_id})

    if profile is None:
        raise HTTPException(status_code=500, detail="Failed to retrieve profile after upsert")

    return {"profile": _serialize(profile)}
