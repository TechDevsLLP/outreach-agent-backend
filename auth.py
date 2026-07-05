from datetime import datetime, timedelta, timezone

import bcrypt
from bson import ObjectId
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from config import get_settings

settings = get_settings()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        hashed_password.encode("utf-8"),
    )


def hash_password(plain_password: str) -> str:
    return bcrypt.hashpw(plain_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    delta = expires_delta if expires_delta is not None else timedelta(minutes=settings.jwt_expiry_minutes)
    expire = datetime.now(timezone.utc) + delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """Decode JWT and return the full user document from MongoDB.

    The JWT ``sub`` claim must be the user ``_id`` as a string (ObjectId hex).
    Raises HTTP 401 if the token is invalid or the user is not found.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    # Import here to avoid circular imports at module load time
    import database

    try:
        oid = ObjectId(user_id)
    except Exception:
        raise credentials_exception

    user = await database.users_collection.find_one({"_id": oid})
    if user is None:
        raise credentials_exception

    # Normalise ObjectId to string for callers
    user["_id"] = str(user["_id"])
    return user


async def get_account_context(user: dict = Depends(get_current_user)) -> dict:
    """Return ``{"user": ..., "account": ...}`` for the user's current account.

    Reads ``current_account_id`` from the user document, verifies the user is
    a member of that account, and returns both objects.  Raises HTTP 403 if the
    account is not found or the user is not a member.
    """
    import database

    forbidden_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Account not found or access denied",
    )

    account_id_str: str | None = user.get("current_account_id")
    if not account_id_str:
        raise forbidden_exception

    try:
        account_oid = ObjectId(account_id_str)
    except Exception:
        raise forbidden_exception

    account = await database.accounts_collection.find_one({"_id": account_oid})
    if account is None:
        raise forbidden_exception

    # Plan gating — check account status
    account_status = account.get("status")
    if account_status and account_status not in ("active", "trial", ""):
        raise HTTPException(
            status_code=402,
            detail="Account suspended. Please contact support.",
        )

    # Trial expiry check
    trial_ends = account.get("trial_ends_at")
    if account.get("plan") == "trial" and trial_ends:
        from datetime import datetime
        if isinstance(trial_ends, datetime) and trial_ends < datetime.utcnow():
            raise HTTPException(
                status_code=402,
                detail="Trial expired. Please upgrade to continue.",
            )

    # Verify membership
    user_oid = ObjectId(user["_id"])
    member = await database.account_members_collection.find_one(
        {"account_id": account_oid, "user_id": user_oid}
    )
    if member is None:
        raise forbidden_exception

    account["_id"] = str(account["_id"])
    return {"user": user, "account": account}


# ---------------------------------------------------------------------------
# Backward-compatible alias so existing routes that depend on get_current_admin
# continue to work without immediate changes.  New routes should use
# get_current_user or get_account_context directly.
# ---------------------------------------------------------------------------
get_current_admin = get_current_user


def create_impersonation_token(
    target_user_id: str,
    target_account_id: str,
    target_email: str,
    admin_user_id: str,
    ttl_minutes: int = 30,
) -> str:
    """Mint a short-lived JWT that lets the admin act as target_user_id."""
    return create_access_token(
        data={
            "sub": target_user_id,
            "account_id": target_account_id,
            "email": target_email,
            "impersonated_by": admin_user_id,
        },
        expires_delta=timedelta(minutes=ttl_minutes),
    )


async def get_super_admin(user: dict = Depends(get_current_user)) -> dict:
    """Dependency: only the configured SUPER_ADMIN_EMAIL user may proceed."""
    admin_email = settings.super_admin_email
    if not admin_email:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin access is not configured on this instance",
        )
    if user["email"].lower() != admin_email.lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access denied",
        )
    return user
