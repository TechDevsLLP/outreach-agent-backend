import hmac
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
from bson import ObjectId
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from config import get_settings

settings = get_settings()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)

SESSION_COOKIE_NAME = "auth_token"
ADMIN_SESSION_COOKIE_NAME = "admin_session"
CSRF_COOKIE_NAME = "outflo_csrf"
CSRF_HEADER_NAME = "X-CSRF-Token"
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}


def id_representations(value) -> list[object]:
    """Return the supported legacy/canonical representations of a Mongo id.

    Membership rows were historically written with ObjectIds while several
    later call sites queried strings. Keeping this conversion at the trust
    boundary prevents valid users being denied and makes mixed-schema rollout
    deterministic until the migration canonicalizes existing rows.
    """
    values: list[object] = [str(value)]
    try:
        oid = value if isinstance(value, ObjectId) else ObjectId(str(value))
        if oid not in values:
            values.append(oid)
    except Exception:
        pass
    return values


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


def rotate_user_access_token(user: dict, account_id: str) -> str:
    """Rotate a user token without dropping or extending impersonation scope."""
    claims = {
        "sub": str(user["_id"]),
        "account_id": account_id,
        "email": user["email"],
    }
    expires_delta = None
    impersonated_by = user.get("_impersonated_by")
    if impersonated_by:
        claims["impersonated_by"] = impersonated_by
        expires_at = user.get("_session_expires_at")
        if isinstance(expires_at, (int, float)):
            remaining = datetime.fromtimestamp(expires_at, timezone.utc) - datetime.now(timezone.utc)
            # JWT decoding already rejects expired tokens. Keep a small floor
            # for clock-boundary rotations without ever extending the session.
            expires_delta = max(remaining, timedelta(seconds=1))
    return create_access_token(claims, expires_delta=expires_delta)


def decode_access_token(token: str) -> dict:
    """Strictly decode an OutFlo access token using the configured algorithm."""
    return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])


def _cookie_secure() -> bool:
    return settings.app_env.strip().lower() == "production" or settings.session_cookie_secure


def _cookie_max_age(ttl_minutes: int | None = None) -> int:
    return int(60 * (ttl_minutes or settings.jwt_expiry_minutes))


def set_session_cookies(
    response: Response,
    token: str,
    *,
    ttl_minutes: int | None = None,
    admin_token: str | None = None,
) -> str:
    """Set the browser session and rotate its double-submit CSRF token.

    The JWT is deliberately HttpOnly.  The CSRF value is the only
    JavaScript-readable cookie and is not an authentication credential.
    """
    secure = _cookie_secure()
    max_age = _cookie_max_age(ttl_minutes)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=max_age,
        httponly=True,
        secure=secure,
        samesite="lax",
        path="/",
    )
    if admin_token is not None:
        response.set_cookie(
            ADMIN_SESSION_COOKIE_NAME,
            admin_token,
            max_age=max_age,
            httponly=True,
            secure=secure,
            samesite="strict",
            path="/",
        )
    csrf_token = secrets.token_urlsafe(32)
    response.set_cookie(
        CSRF_COOKIE_NAME,
        csrf_token,
        max_age=max_age,
        httponly=False,
        secure=secure,
        samesite="lax",
        path="/",
    )
    return csrf_token


def clear_session_cookies(response: Response) -> None:
    secure = _cookie_secure()
    for name, httponly, samesite in (
        (SESSION_COOKIE_NAME, True, "lax"),
        (ADMIN_SESSION_COOKIE_NAME, True, "strict"),
        (CSRF_COOKIE_NAME, False, "lax"),
    ):
        response.delete_cookie(
            name,
            path="/",
            secure=secure,
            httponly=httponly,
            samesite=samesite,
        )


def clear_admin_session_cookie(response: Response) -> None:
    response.delete_cookie(
        ADMIN_SESSION_COOKIE_NAME,
        path="/",
        secure=_cookie_secure(),
        httponly=True,
        samesite="strict",
    )


def _allowed_browser_origins() -> set[str]:
    configured = settings.cors_origins or settings.frontend_url
    return {origin.strip().rstrip("/") for origin in configured.split(",") if origin.strip()}


def validate_cookie_csrf(request: Request) -> None:
    """Protect an unsafe request authenticated by the session cookie.

    Explicit bearer clients are outside this check because browsers do not
    attach an Authorization header cross-site without a successful CORS
    preflight.  Production cookie mutations additionally require an exact
    trusted Origin.
    """
    if request.method.upper() in _SAFE_METHODS:
        return

    origin = (request.headers.get("origin") or "").rstrip("/")
    allowed_origins = _allowed_browser_origins()
    is_production = settings.app_env.strip().lower() == "production"
    if (is_production and not origin) or (origin and origin not in allowed_origins):
        raise HTTPException(status_code=403, detail="Untrusted request origin")

    cookie_token = request.cookies.get(CSRF_COOKIE_NAME, "")
    header_token = request.headers.get(CSRF_HEADER_NAME, "")
    if not cookie_token or not header_token or not hmac.compare_digest(cookie_token, header_token):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def is_browser_session_request(request: Request) -> bool:
    """Identify the first-party web client without changing public API auth."""
    return request.headers.get("X-OutFlo-Client", "").lower() == "browser"


async def get_current_user(
    request: Request,
    bearer_token: str | None = Depends(oauth2_scheme),
) -> dict:
    """Decode JWT and return the full user document from MongoDB.

    The JWT ``sub`` claim must be the user ``_id`` as a string (ObjectId hex).
    Raises HTTP 401 if the token is invalid or the user is not found.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    cookie_token = request.cookies.get(SESSION_COOKIE_NAME)
    token = bearer_token or cookie_token
    if not token:
        raise credentials_exception
    if bearer_token is None and cookie_token:
        validate_cookie_csrf(request)

    try:
        payload = decode_access_token(token)
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
    # Request-only metadata; response serializers explicitly shape user data.
    user["_is_impersonating"] = bool(payload.get("impersonated_by"))
    user["_impersonated_by"] = payload.get("impersonated_by")
    user["_session_expires_at"] = payload.get("exp")
    return user


async def get_account_context(user: dict = Depends(get_current_user)) -> dict:
    """Return ``{"user": ..., "account": ...}`` for the user's current account.

    Reads ``current_account_id`` from the user document, verifies the user is
    a member of that account, and returns both objects.  Raises HTTP 403 if the
    account is not found or the user is not a member.

    If ``current_account_id`` is not set on the user doc, the function attempts
    to auto-resolve it from ``account_members`` and persists it on the user
    record so subsequent calls are fast.
    """
    import database

    forbidden_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Account not found or access denied",
    )


    account_id_str: str | None = user.get("current_account_id")
    user_oid = ObjectId(user["_id"])

    # ── Auto-resolve current_account_id if missing ──────────────────────────
    if not account_id_str:
        first_membership = await database.account_members_collection.find_one(
            {"user_id": user_oid},
            sort=[("joined_at", 1)],
        )
        if first_membership is None:
            raise forbidden_exception
        account_id_str = str(first_membership["account_id"])
        # Persist back so future calls are fast
        await database.users_collection.update_one(
            {"_id": user_oid},
            {"$set": {"current_account_id": first_membership["account_id"]}},
        )
        user["current_account_id"] = account_id_str

    try:
        account_oid = ObjectId(account_id_str)
    except Exception:
        raise forbidden_exception

    account = await database.accounts_collection.find_one(
        {"_id": {"$in": id_representations(account_id_str)}}
    )
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
    member = await database.account_members_collection.find_one(
        {
            "account_id": {"$in": id_representations(account_id_str)},
            "user_id": {"$in": id_representations(user_oid)},
        }
    )

    if member is None:
        raise forbidden_exception

    account["_id"] = str(account["_id"])
    return {"user": user, "account": account}


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
