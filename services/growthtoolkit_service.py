"""
Consolidated GrowthToolkit (api.appconnector.pro) client.

Single integration layer for all GrowthToolkit APIs:
  - Email Finder:          GET  /email-finder/
  - LinkedIn Enrichment:   POST /enrichment/linkedin/
  - Async task polling:    GET  /tasks/status/{task_id}/

API behavior (per official docs):
  - Auth via `x-api-key` header.
  - ALL responses are HTTP 200; success is signaled by the `success` bool and
    the `code` field in the JSON body.
  - Error codes: 401 invalid key, 402/406/407/428 credits exhausted/low,
    400/417 invalid input, 429/too_many_requests rate limited
    (`error_data.sec` = seconds to wait before retrying).
  - Action APIs may return `{"success": true, "data": {"task_id": "..."}}` —
    poll GET /tasks/status/{task_id}/ (Misc scope) until "finished"/failure.
  - Rate limits by scope: Action 2 req/s, List 1 req/s, Export 10 req/10s,
    Misc 1 req/2s.

Every action call inserts a usage doc into the `growthtoolkit_usage` Mongo
collection (see _track_usage) for admin credit reporting.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

_REQUEST_TIMEOUT = 30.0
_MAX_429_RETRIES = 3
_DEFAULT_429_WAIT = 2.0
_TASK_POLL_INTERVAL = 3.0        # seconds between task status polls (Misc scope: 1 req/2s)
_TASK_POLL_TIMEOUT = 90.0        # give up polling after this many seconds
_5XX_RETRIES = 2
_5XX_BACKOFF = 2.0

# Body `code` values, normalized to str for comparison
_CREDIT_CODES = {"402", "406", "407", "428", "payment_required", "credits_exhausted", "credits_low", "recharge_credit_immediately"}
_INVALID_INPUT_CODES = {"400", "417", "invalid_input", "expectation_failed"}
_AUTH_CODES = {"401", "unauthorized", "invalid_api_key"}
_RATE_LIMIT_CODES = {"429", "too_many_requests"}
_NOT_FOUND_CODES = {"404", "resource_not_found", "not_found"}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class GrowthToolkitError(Exception):
    """Base error for GrowthToolkit API failures."""

    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class CreditsExhausted(GrowthToolkitError):
    """GrowthToolkit credits exhausted or too low (codes 402/406/407/428)."""


class InvalidInput(GrowthToolkitError):
    """Invalid input rejected by GrowthToolkit (codes 400/417)."""


class AuthError(GrowthToolkitError):
    """Invalid or missing GrowthToolkit API key (code 401)."""


# ---------------------------------------------------------------------------
# Per-scope client-side rate limiting
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Simple min-interval rate limiter: serializes callers per scope and
    spaces requests at least `min_interval` seconds apart."""

    def __init__(self, min_interval: float):
        self._min_interval = min_interval
        self._lock = asyncio.Lock()
        self._next_at = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_at = max(now, self._next_at) + self._min_interval


_LIMITERS: dict[str, _RateLimiter] = {
    "action": _RateLimiter(0.5),   # 2 req/s
    "list": _RateLimiter(1.0),     # 1 req/s
    "export": _RateLimiter(1.0),   # 10 req/10s (evenly spaced)
    "misc": _RateLimiter(2.0),     # 1 req/2s
}


# ---------------------------------------------------------------------------
# Shared HTTP client (lazy init, reused across calls for connection pooling)
# ---------------------------------------------------------------------------

_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    """Return the shared module-level AsyncClient, creating it on first use.

    A single client reuses TCP/TLS connections across calls instead of paying
    connection setup per request. Closed via aclose() on app shutdown."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=_REQUEST_TIMEOUT)
    return _client


async def aclose() -> None:
    """Close the shared client (wired into the FastAPI lifespan shutdown)."""
    global _client
    if _client is not None:
        client, _client = _client, None
        await client.aclose()


# ---------------------------------------------------------------------------
# Core request handling
# ---------------------------------------------------------------------------

def _body_code(body: dict) -> Optional[str]:
    code = body.get("code")
    return str(code) if code is not None else None


def _raise_for_code(body: dict) -> None:
    """Map a non-success response body to a typed exception (if the code is
    one we treat as an error). Codes like resource_not_found do NOT raise —
    callers inspect the returned body instead."""
    code = _body_code(body)
    message = str(body.get("message") or body.get("error") or f"GrowthToolkit error (code={code})")
    if code in _AUTH_CODES:
        raise AuthError(message, code=code)
    if code in _CREDIT_CODES:
        raise CreditsExhausted(message, code=code)
    if code in _INVALID_INPUT_CODES:
        raise InvalidInput(message, code=code)


async def _request(
    method: str,
    path: str,
    *,
    scope: str = "action",
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> dict:
    """Perform one GrowthToolkit API call with auth, per-scope rate limiting,
    429 backoff (honoring error_data.sec, max 3 retries), and 5xx retry.

    Returns the parsed JSON body. Raises typed exceptions for auth/credit/
    input errors. Non-success bodies with other codes (e.g. resource_not_found)
    are returned as-is for the caller to interpret.
    """
    api_key = settings.growthtoolkit_api_key
    if not api_key:
        raise AuthError("GROWTHTOOLKIT_API_KEY not configured", code="401")

    base = settings.growthtoolkit_base_url.rstrip("/")
    url = f"{base}/{path.lstrip('/')}"
    headers = {"x-api-key": api_key}
    limiter = _LIMITERS.get(scope, _LIMITERS["action"])

    retries_429 = 0
    retries_5xx = 0

    while True:
        await limiter.acquire()
        try:
            resp = await _get_client().request(method, url, params=params, json=json_body, headers=headers)
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if retries_5xx < _5XX_RETRIES:
                retries_5xx += 1
                await asyncio.sleep(_5XX_BACKOFF)
                continue
            raise GrowthToolkitError(f"GrowthToolkit network error: {e}") from e

        # Docs say all responses are HTTP 200, but guard against gateway errors.
        if resp.status_code >= 500:
            if retries_5xx < _5XX_RETRIES:
                retries_5xx += 1
                await asyncio.sleep(_5XX_BACKOFF)
                continue
            raise GrowthToolkitError(f"GrowthToolkit HTTP {resp.status_code}")

        try:
            body = resp.json()
        except Exception as e:
            raise GrowthToolkitError(f"GrowthToolkit non-JSON response (HTTP {resp.status_code})") from e

        if body.get("success"):
            return body

        code = _body_code(body)
        if code in _RATE_LIMIT_CODES:
            if retries_429 < _MAX_429_RETRIES:
                retries_429 += 1
                wait = _DEFAULT_429_WAIT
                error_data = body.get("error_data") or {}
                try:
                    wait = float(error_data.get("sec", wait))
                except (TypeError, ValueError):
                    pass
                logger.info(f"GrowthToolkit rate limited on {path}; retrying in {wait:.1f}s (attempt {retries_429}/{_MAX_429_RETRIES})")
                await asyncio.sleep(wait)
                continue
            raise GrowthToolkitError("GrowthToolkit rate limit: retries exhausted", code=code)

        _raise_for_code(body)  # raises for auth/credits/invalid-input codes
        return body            # e.g. resource_not_found — caller interprets


async def _poll_task(task_id: str) -> dict:
    """Poll GET /tasks/status/{task_id}/ (Misc scope) every ~3s until the task
    is finished or fails; cap at ~90s total."""
    deadline = time.monotonic() + _TASK_POLL_TIMEOUT
    while True:
        body = await _request("GET", f"/tasks/status/{task_id}/", scope="misc")
        data = body.get("data") or {}
        status = str(data.get("status") or body.get("status") or "").lower()
        if status == "finished":
            return body
        if status in ("failed", "failure", "error", "cancelled"):
            raise GrowthToolkitError(f"GrowthToolkit task {task_id} failed (status={status})", code=_body_code(body))
        if time.monotonic() >= deadline:
            raise GrowthToolkitError(f"GrowthToolkit task {task_id} timed out after {_TASK_POLL_TIMEOUT:.0f}s")
        await asyncio.sleep(_TASK_POLL_INTERVAL)


async def _action_request(
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> dict:
    """Action-scope request that transparently resolves async task_id responses."""
    body = await _request(method, path, scope="action", params=params, json_body=json_body)
    data = body.get("data")
    if body.get("success") and isinstance(data, dict) and data.get("task_id") and len(data) <= 2:
        task_body = await _poll_task(str(data["task_id"]))
        task_data = task_body.get("data") or {}
        result = task_data.get("result", task_data.get("data"))
        if result is not None:
            return {"success": True, "code": task_body.get("code"), "data": result}
        return task_body
    return body


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------

def _get_usage_collection():
    """Resolve usage persistence lazily so provider calls stay import-safe.

    Keeping this boundary injectable also lets the entire unit suite prove the
    provider contract without waiting for or writing to MongoDB.
    """
    from database import growthtoolkit_usage_collection

    return growthtoolkit_usage_collection

async def _track_usage(
    endpoint: str,
    *,
    success: bool,
    code: Optional[str],
    duration_ms: int,
    account_id: Optional[str] = None,
    prospect_id: Optional[str] = None,
) -> None:
    """Insert a usage doc into `growthtoolkit_usage` (best-effort — never
    lets tracking failures break the API call itself)."""
    try:
        await _get_usage_collection().insert_one({
            "account_id": str(account_id) if account_id is not None else None,
            "endpoint": endpoint,
            "credits_used": 1 if success else 0,
            "success": success,
            "code": code,
            "duration_ms": duration_ms,
            "prospect_id": str(prospect_id) if prospect_id is not None else None,
            "created_at": datetime.utcnow(),
        })
    except Exception as e:
        logger.warning(f"GrowthToolkit usage tracking failed for {endpoint}: {e}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def find_email(
    first_name: str,
    last_name: str,
    domain: str,
    *,
    account_id: Optional[str] = None,
    prospect_id: Optional[str] = None,
) -> Optional[str]:
    """Look up a work email by name + company domain.

    Returns the email string, or None when the provider has no result
    (code resource_not_found). Raises typed GrowthToolkitError subclasses
    for auth / credit / input errors.
    """
    started = time.monotonic()
    success = False
    code: Optional[str] = None
    try:
        body = await _action_request(
            "GET",
            "/email-finder/",
            params={"first_name": first_name, "last_name": last_name, "domain": domain},
        )
        code = _body_code(body)
        data = body.get("data") or {}
        email = data.get("email") if isinstance(data, dict) else None
        success = bool(body.get("success") and email)
        return email if success else None
    except GrowthToolkitError as e:
        code = e.code
        raise
    finally:
        await _track_usage(
            "email-finder",
            success=success,
            code=code,
            duration_ms=int((time.monotonic() - started) * 1000),
            account_id=account_id,
            prospect_id=prospect_id,
        )


async def enrich_linkedin(
    url: str,
    unlock_emails: bool = False,
    unlock_phone: bool = False,
    *,
    account_id: Optional[str] = None,
    prospect_id: Optional[str] = None,
) -> Optional[dict]:
    """Enrich a LinkedIn profile URL. 1 credit per call.

    Returns the rich person object (incl. `phone_numbers`,
    `unlock_details.phone_numbers`, `work_email`, `personal_email`, `emails`,
    job/company/location fields), or None when the profile is not found
    (code resource_not_found). Raises typed GrowthToolkitError subclasses
    for auth / credit / input errors.
    """
    started = time.monotonic()
    success = False
    code: Optional[str] = None
    try:
        body = await _action_request(
            "POST",
            "/enrichment/linkedin/",
            json_body={
                "url": url,
                "unlock_emails": 1 if unlock_emails else 0,
                "unlock_phone": 1 if unlock_phone else 0,
            },
        )
        code = _body_code(body)
        if not body.get("success"):
            return None  # e.g. resource_not_found
        data = body.get("data")
        success = isinstance(data, dict)
        return data if success else None
    except GrowthToolkitError as e:
        code = e.code
        raise
    finally:
        await _track_usage(
            "linkedin-enrichment",
            success=success,
            code=code,
            duration_ms=int((time.monotonic() - started) * 1000),
            account_id=account_id,
            prospect_id=prospect_id,
        )
