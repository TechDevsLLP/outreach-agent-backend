"""Shared user provisioning logic used by auth registration and admin panel."""
import re
import secrets
from datetime import datetime

from bson import ObjectId
from fastapi import HTTPException, status

import database
from auth import hash_password


def _slugify(name: str) -> str:
    base = re.sub(r"[^a-z0-9-]", "", name.lower().replace(" ", "-"))
    suffix = secrets.token_hex(2)
    return f"{base}-{suffix}"


async def create_user_with_account(
    email: str,
    password: str,
    name: str,
    company_name: str | None = None,
    plan: str = "free",
) -> tuple[ObjectId, ObjectId]:
    """
    Create a user + their first account (owner role).
    Returns (user_id, account_id) as ObjectIds.
    Raises HTTP 409 if email already exists.
    """
    email = email.strip().lower()
    name = name.strip()

    existing = await database.users_collection.find_one({"email": email})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A user with that email already exists",
        )

    now = datetime.utcnow()

    user_doc = {
        "email": email,
        "password_hash": hash_password(password),
        "name": name,
        "plan": plan,
        "onboarding_complete": False,
        "is_active": True,
        "booking_link": None,
        "current_account_id": None,
        "created_at": now,
        "updated_at": now,
    }
    user_result = await database.users_collection.insert_one(user_doc)
    user_id: ObjectId = user_result.inserted_id

    account_display_name = company_name.strip() if company_name else f"{name}'s Account"
    account_doc = {
        "name": account_display_name,
        "slug": _slugify(account_display_name),
        "plan": plan,
        "status": "active",
        "trial_ends_at": None,
        "sender_name": None,
        "sender_email": None,
        "reply_to_email": None,
        "daily_email_quota": 50,
        "daily_linkedin_connection_quota": 20,
        "daily_linkedin_inmail_quota": 5,
        "enrichment_batch_limit": 50,
        "created_at": now,
        "updated_at": now,
    }
    account_result = await database.accounts_collection.insert_one(account_doc)
    account_id: ObjectId = account_result.inserted_id

    await database.account_members_collection.insert_one({
        "account_id": account_id,
        "user_id": user_id,
        "role": "owner",
        "joined_at": now,
    })

    await database.users_collection.update_one(
        {"_id": user_id},
        {"$set": {"current_account_id": account_id, "updated_at": now}},
    )

    return user_id, account_id
