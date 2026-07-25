"""
Webhook Routes - Public endpoints for Unipile webhooks.
No JWT auth required - these are called by external services.

Email replies are no longer received via provider webhooks (SendGrid Inbound
Parse / Event Webhook have been removed) — they're detected by the unified
poller in services/email_reply_poller.py instead.
"""

import logging
import hashlib
import hmac
import time
from datetime import datetime, timezone
from fastapi import APIRouter, Request, HTTPException

from config import get_settings
from services.webhook_service import process_unipile_webhook

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


async def _log_webhook(source: str, event_type: str, status: str, error=None, payload_size: int = 0):
    """Lightweight delivery log for admin observability (webhook_log collection).

    Never stores payloads; caps field sizes; never raises."""
    try:
        import database
        await database.webhook_log_collection.insert_one({
            "source": source,
            "event_type": (event_type or "unknown")[:100],
            "status": status,  # "ok" | "error"
            "error": str(error)[:500] if error else None,
            "received_at": datetime.now(timezone.utc),
            "payload_size": int(payload_size or 0),
        })
    except Exception:
        logger.debug("webhook_log insert failed", exc_info=True)


# ── Unipile Webhook ──

def _hmac_sha256_matches(secret: str, payload: bytes, signature: str, timestamp: str) -> bool:
    """Constant-time HMAC-SHA256 verification of the raw webhook body.

    Accepts a hex signature, optionally ``sha256=`` prefixed. Two signing
    schemes are checked (both constant-time), so this works whether Unipile
    signs the raw body alone or a ``"{timestamp}." + body`` payload:

      * ``HMAC(secret, body)``
      * ``HMAC(secret, f"{timestamp}." + body)`` — only when a timestamp header
        is present and within a 300-second tolerance.

    Returns False on any empty/malformed signature.
    """
    if not signature:
        return False
    received = signature.removeprefix("sha256=").strip()
    if not received:
        return False

    key = secret.encode()
    candidates = [hmac.new(key, payload, hashlib.sha256).hexdigest()]

    if timestamp:
        try:
            event_time = int(timestamp)
        except (ValueError, TypeError):
            return False
        if abs(time.time() - event_time) > 300:
            logger.warning("Unipile webhook timestamp outside tolerance: %s", timestamp)
            return False
        signed_content = f"{timestamp}.".encode() + payload
        candidates.append(hmac.new(key, signed_content, hashlib.sha256).hexdigest())

    return any(hmac.compare_digest(expected, received) for expected in candidates)


def _verify_unipile_signature(secret: str, payload: bytes, signature: str, timestamp: str) -> bool:
    """
    Verify a Unipile webhook request, failing closed outside dev/test.

    Two auth schemes are supported for a smooth transition:

      * HMAC-SHA256 over the raw body (constant-time; see
        :func:`_hmac_sha256_matches`). This is the hardened scheme.
      * The legacy plain-text shared secret sent in the ``X-Webhook-Secret``
        header (constant-time equality). Accepted only while
        ``settings.unipile_webhook_require_hmac`` is False.

    Config hooks (config.py): ``unipile_webhook_signature_header`` names the
    HMAC header the route reads; ``unipile_webhook_require_hmac`` flips this to
    HMAC-only once Unipile's signing scheme is confirmed.
    """
    if not secret:
        # Local development and the isolated test harness may deliberately run
        # without a provider secret. Every other environment fails closed.
        app_env = (settings.app_env or "").strip().lower()
        if app_env in {"development", "test"}:
            logger.warning(
                "Accepting unsigned Unipile webhook in explicit %s environment",
                app_env,
            )
            return True
        logger.error(
            "Rejecting Unipile webhook: UNIPILE_WEBHOOK_SECRET is not configured"
        )
        return False

    # Hardened scheme: HMAC-SHA256 of the raw body.
    if _hmac_sha256_matches(secret, payload, signature, timestamp):
        return True

    # Legacy transition scheme: plain-text secret equality, unless HMAC is
    # required by config. Constant-time compare; empty signatures rejected.
    if not getattr(settings, "unipile_webhook_require_hmac", False):
        if signature and hmac.compare_digest(signature, secret):
            return True

    return False


@router.post("/unipile/messages")
async def unipile_messages(request: Request):
    """
    Receive LinkedIn messages from Unipile webhook.
    Content-Type: application/json
    """
    body = await request.body()
    x_webhook_secret = request.headers.get("X-Webhook-Secret", "")
    signature_header = getattr(
        settings, "unipile_webhook_signature_header", "X-Unipile-Signature"
    )
    x_unipile_signature = request.headers.get(signature_header, "")
    x_unipile_timestamp = request.headers.get("X-Unipile-Timestamp", "")

    # Use whichever auth header Unipile sends (legacy = X-Webhook-Secret plain
    # text; hardened = HMAC-SHA256 in the configured signature header).
    sig = x_unipile_signature or x_webhook_secret

    if not _verify_unipile_signature(
        settings.unipile_webhook_secret, body, sig, x_unipile_timestamp
    ):
        logger.warning("Unipile webhook verification failed")
        await _log_webhook("unipile", "messages", "error", error="Invalid webhook secret", payload_size=len(body))
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    import json
    try:
        payload = json.loads(body)
    except Exception:
        await _log_webhook("unipile", "messages", "error", error="Invalid JSON", payload_size=len(body))
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(payload, dict):
        # A well-formed but non-object payload (list/string/number/etc.) would
        # otherwise crash process_unipile_webhook with an AttributeError,
        # returning 500 and causing the provider to retry forever.
        await _log_webhook("unipile", "messages", "error", error="Malformed payload (not an object)", payload_size=len(body))
        return {"ignored": "malformed"}

    event_type = str(payload.get("event") or payload.get("type") or "messages")
    try:
        result = await process_unipile_webhook(payload)
    except Exception as e:
        await _log_webhook("unipile", event_type, "error", error=e, payload_size=len(body))
        raise

    await _log_webhook("unipile", event_type, "ok", payload_size=len(body))
    return result
