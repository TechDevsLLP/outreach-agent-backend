import re
import secrets
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

import database
from auth import (
    get_account_context,
    get_current_user,
    is_browser_session_request,
    rotate_user_access_token,
    set_session_cookies,
)
from models.account import AccountUpdateRequest

router = APIRouter(prefix="/api/accounts", tags=["Accounts"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    base = re.sub(r"[^a-z0-9-]", "", name.lower().replace(" ", "-"))
    suffix = secrets.token_hex(2)
    return f"{base}-{suffix}"


def _account_to_dict(account: dict) -> dict:
    return {
        "_id": str(account["_id"]),
        "name": account["name"],
        "slug": account["slug"],
        "plan": account.get("plan", "free"),
        "status": account.get("status", "active"),
        "trial_ends_at": account.get("trial_ends_at"),
        "sender_name": account.get("sender_name"),
        "sender_email": account.get("sender_email"),
        "reply_to_email": account.get("reply_to_email"),
        "daily_email_quota": account.get("daily_email_quota", 50),
        "daily_linkedin_connection_quota": account.get("daily_linkedin_connection_quota", 20),
        "daily_linkedin_inmail_quota": account.get("daily_linkedin_inmail_quota", 5),
        "enrichment_batch_limit": account.get("enrichment_batch_limit", 50),
        "created_at": account.get("created_at"),
        "updated_at": account.get("updated_at"),
    }


def _member_to_dict(member: dict, user_info: dict | None = None) -> dict:
    d = {
        "_id": str(member["_id"]),
        "account_id": str(member["account_id"]),
        "user_id": str(member["user_id"]),
        "role": member.get("role", "member"),
        "joined_at": member.get("joined_at"),
    }
    if user_info:
        d["user"] = {
            "_id": str(user_info["_id"]),
            "name": user_info.get("name"),
            "email": user_info.get("email"),
        }
    return d


async def _require_owner(account_id: ObjectId, user_id: ObjectId) -> dict:
    """Raise 403 if the user is not an owner of the account."""
    member = await database.account_members_collection.find_one(
        {"account_id": account_id, "user_id": user_id}
    )
    if not member or member.get("role") != "owner":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only account owners can perform this action",
        )
    return member


# ---------------------------------------------------------------------------
# GET /api/accounts/current
# ---------------------------------------------------------------------------

@router.get("/current")
async def get_current_account(ctx: dict = Depends(get_account_context)):
    """Return the current account and its members (with embedded user info)."""
    account = ctx["account"]
    account_oid = ObjectId(account["_id"])

    members_cursor = database.account_members_collection.find({"account_id": account_oid})
    members_raw = await members_cursor.to_list(200)

    members_with_users = []
    for m in members_raw:
        user_doc = await database.users_collection.find_one(
            {"_id": m["user_id"]}, {"name": 1, "email": 1}
        )
        members_with_users.append(_member_to_dict(m, user_doc))

    return {
        "account": _account_to_dict(account),
        "members": members_with_users,
    }


# ---------------------------------------------------------------------------
# PATCH /api/accounts/current
# ---------------------------------------------------------------------------

@router.patch("/current")
async def update_current_account(
    body: AccountUpdateRequest,
    ctx: dict = Depends(get_account_context),
):
    """Update mutable fields on the current account."""
    account = ctx["account"]
    account_oid = ObjectId(account["_id"])

    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No fields provided for update",
        )

    updates["updated_at"] = datetime.utcnow()

    await database.accounts_collection.update_one(
        {"_id": account_oid},
        {"$set": updates},
    )

    updated = await database.accounts_collection.find_one({"_id": account_oid})
    return {"account": _account_to_dict(updated)}


# ---------------------------------------------------------------------------
# PATCH /api/accounts/me/quotas
# ---------------------------------------------------------------------------

@router.patch("/me/quotas")
async def update_account_quotas(
    body: dict,
    account_ctx: dict = Depends(get_account_context),
):
    """Update outreach daily quotas for the current account."""
    account_id = ObjectId(account_ctx["account"]["_id"])

    # Validate bounds
    email_quota = body.get("daily_email_quota")
    linkedin_quota = body.get("daily_linkedin_connection_quota")
    inmail_quota = body.get("daily_linkedin_inmail_quota")

    updates = {}
    if email_quota is not None:
        if not (1 <= int(email_quota) <= 500):
            raise HTTPException(400, "daily_email_quota must be between 1 and 500")
        updates["daily_email_quota"] = int(email_quota)
    if linkedin_quota is not None:
        if not (1 <= int(linkedin_quota) <= 100):
            raise HTTPException(400, "daily_linkedin_connection_quota must be between 1 and 100")
        updates["daily_linkedin_connection_quota"] = int(linkedin_quota)
    if inmail_quota is not None:
        if not (1 <= int(inmail_quota) <= 20):
            raise HTTPException(400, "daily_linkedin_inmail_quota must be between 1 and 20")
        updates["daily_linkedin_inmail_quota"] = int(inmail_quota)

    if not updates:
        raise HTTPException(400, "No valid quota fields provided")

    updates["updated_at"] = datetime.utcnow()

    await database.accounts_collection.update_one(
        {"_id": account_id},
        {"$set": updates},
    )

    account = await database.accounts_collection.find_one({"_id": account_id})
    return {"account": _account_to_dict(account)}


# ---------------------------------------------------------------------------
# GET /api/accounts/members
# ---------------------------------------------------------------------------

@router.get("/members")
async def list_members(ctx: dict = Depends(get_account_context)):
    """List all members of the current account with embedded user info."""
    account = ctx["account"]
    account_oid = ObjectId(account["_id"])

    members_raw = await database.account_members_collection.find(
        {"account_id": account_oid}
    ).to_list(200)

    result = []
    for m in members_raw:
        user_doc = await database.users_collection.find_one(
            {"_id": m["user_id"]}, {"name": 1, "email": 1}
        )
        result.append(_member_to_dict(m, user_doc))

    return result


# ---------------------------------------------------------------------------
# POST /api/accounts/members/invite
# ---------------------------------------------------------------------------

class InviteRequest(BaseModel):
    email: str
    role: str = "member"


@router.post("/members/invite", status_code=status.HTTP_201_CREATED)
async def invite_member(
    body: InviteRequest,
    ctx: dict = Depends(get_account_context),
):
    """
    Invite an existing user (by email) into the current account.

    The invited user must already have an OutFlo account.
    """
    account = ctx["account"]
    account_oid = ObjectId(account["_id"])

    invited_user = await database.users_collection.find_one(
        {"email": body.email.strip().lower()}
    )
    if not invited_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No user found with that email address",
        )

    invited_user_oid = invited_user["_id"]

    # Check not already a member
    existing_member = await database.account_members_collection.find_one(
        {"account_id": account_oid, "user_id": invited_user_oid}
    )
    if existing_member:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User is already a member of this account",
        )

    now = datetime.utcnow()
    member_doc = {
        "account_id": account_oid,
        "user_id": invited_user_oid,
        "role": body.role,
        "joined_at": now,
    }
    result = await database.account_members_collection.insert_one(member_doc)
    member_doc["_id"] = result.inserted_id

    return {
        "message": f"User {body.email} added to account",
        "member": _member_to_dict(member_doc, invited_user),
    }


# ---------------------------------------------------------------------------
# DELETE /api/accounts/members/{member_id}
# ---------------------------------------------------------------------------

@router.delete("/members/{member_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_member(
    member_id: str,
    ctx: dict = Depends(get_account_context),
):
    """Remove a member from the current account. Owners cannot remove themselves."""
    account = ctx["account"]
    user = ctx["user"]
    account_oid = ObjectId(account["_id"])
    current_user_oid = ObjectId(user["_id"])

    try:
        member_oid = ObjectId(member_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid member_id")

    member = await database.account_members_collection.find_one(
        {"_id": member_oid, "account_id": account_oid}
    )
    if not member:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found")

    # Owners cannot delete themselves
    if member["user_id"] == current_user_oid and member.get("role") == "owner":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Account owners cannot remove themselves",
        )

    await database.account_members_collection.delete_one({"_id": member_oid})


# ---------------------------------------------------------------------------
# PATCH /api/accounts/members/{member_id}
# ---------------------------------------------------------------------------

class RoleUpdateRequest(BaseModel):
    role: str


@router.patch("/members/{member_id}")
async def update_member_role(
    member_id: str,
    body: RoleUpdateRequest,
    ctx: dict = Depends(get_account_context),
):
    """Change a member's role (owner-only action)."""
    account = ctx["account"]
    user = ctx["user"]
    account_oid = ObjectId(account["_id"])
    current_user_oid = ObjectId(user["_id"])

    # Caller must be owner
    await _require_owner(account_oid, current_user_oid)

    try:
        member_oid = ObjectId(member_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid member_id")

    member = await database.account_members_collection.find_one(
        {"_id": member_oid, "account_id": account_oid}
    )
    if not member:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Member not found")

    await database.account_members_collection.update_one(
        {"_id": member_oid},
        {"$set": {"role": body.role}},
    )

    updated = await database.account_members_collection.find_one({"_id": member_oid})
    user_doc = await database.users_collection.find_one(
        {"_id": updated["user_id"]}, {"name": 1, "email": 1}
    )
    return {"member": _member_to_dict(updated, user_doc)}


# ---------------------------------------------------------------------------
# POST /api/accounts/switch/{account_id}
# ---------------------------------------------------------------------------

@router.post("/switch/{account_id}", response_model=None)
async def switch_account(
    account_id: str,
    request: Request,
    response: Response,
    user: dict = Depends(get_current_user),
):
    """
    Switch the calling user's active account.

    The user must already be a member of the target account. Returns a new
    JWT reflecting the updated account_id.
    """
    try:
        account_oid = ObjectId(account_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid account_id")

    user_oid = ObjectId(user["_id"])

    # Verify membership
    member = await database.account_members_collection.find_one(
        {"account_id": account_oid, "user_id": user_oid}
    )
    if not member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not a member of that account",
        )

    # Verify account exists and is active
    account = await database.accounts_collection.find_one({"_id": account_oid})
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Account not found",
        )

    # Update user.current_account_id
    await database.users_collection.update_one(
        {"_id": user_oid},
        {"$set": {"current_account_id": account_oid, "updated_at": datetime.utcnow()}},
    )

    access_token = rotate_user_access_token(user, account_id)
    if is_browser_session_request(request):
        set_session_cookies(response, access_token)
        response.headers["Cache-Control"] = "no-store"
        return {"switched": True, "account_id": account_id}
    return {"access_token": access_token, "token_type": "bearer"}


# ---------------------------------------------------------------------------
# GET /api/accounts/mine
# ---------------------------------------------------------------------------

@router.get("/mine")
async def list_my_accounts(user: dict = Depends(get_current_user)):
    """List all accounts the current user is a member of."""
    user_oid = ObjectId(user["_id"])

    memberships = await database.account_members_collection.find(
        {"user_id": user_oid}
    ).to_list(100)

    result = []
    for m in memberships:
        account = await database.accounts_collection.find_one({"_id": m["account_id"]})
        if account:
            result.append({
                "account": _account_to_dict(account),
                "role": m.get("role", "member"),
            })

    return result
