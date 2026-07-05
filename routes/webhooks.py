"""
Webhook Routes - Public endpoints for SendGrid and Unipile webhooks.
No JWT auth required - these are called by external services.
"""

import logging
import base64
import hashlib
import hmac
import time
from fastapi import APIRouter, Request, HTTPException

from config import get_settings
from services.webhook_service import (
    process_sendgrid_inbound_email,
    process_sendgrid_events,
    process_unipile_webhook,
)

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


# ── SendGrid Inbound Parse ──

@router.post("/sendgrid/inbound")
async def sendgrid_inbound(request: Request):
    """
    Receive email replies via SendGrid Inbound Parse.
    Content-Type: multipart/form-data (sent by SendGrid)
    """
    try:
        form = await request.form()
    except Exception as e:
        logger.error(f"Failed to parse inbound email form data: {e}")
        raise HTTPException(status_code=400, detail="Invalid form data")

    from_email = form.get("from", "")
    to_email = form.get("to", "")
    subject = form.get("subject", "")
    text = form.get("text", "")
    html = form.get("html", "")
    headers = form.get("headers", "")

    # Validate to_email matches expected inbound parse address
    expected_to = settings.reply_to_email
    if expected_to and expected_to not in str(to_email):
        logger.warning(f"Inbound email to unexpected address: {to_email} (expected {expected_to})")
        # Still process it - may be a valid reply with slightly different formatting

    if not from_email:
        raise HTTPException(status_code=400, detail="Missing 'from' field")

    result = await process_sendgrid_inbound_email(
        from_email=str(from_email),
        to_email=str(to_email),
        subject=str(subject),
        text=str(text) if text else None,
        html=str(html) if html else None,
        headers=str(headers) if headers else None,
    )

    # Always return 200 to SendGrid so it doesn't retry
    return result


# ── SendGrid Event Webhook ──

def _verify_sendgrid_signature(public_key: str, payload: bytes, signature: str, timestamp: str) -> bool:
    """
    Verify a SendGrid V3 Event Webhook signature using ECDSA P-256.

    SendGrid signs the timestamp + payload concatenation with ECDSA using the
    public key shown on your SendGrid Event Webhook settings page.

    Args:
        public_key: The ECDSA public key from SendGrid dashboard (base64 DER or PEM).
        payload: Raw request body bytes.
        signature: Value of X-Twilio-Email-Event-Webhook-Signature header (base64).
        timestamp: Value of X-Twilio-Email-Event-Webhook-Timestamp header.
    """
    if not public_key or not signature or not timestamp:
        return False
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.exceptions import InvalidSignature

        # The signed message is: timestamp bytes + raw payload bytes
        signed_content = timestamp.encode() + payload

        # Decode the base64-encoded DER signature
        sig_bytes = base64.b64decode(signature)

        # Load the public key — try DER-encoded base64 first, then PEM
        try:
            key = serialization.load_der_public_key(base64.b64decode(public_key))
        except Exception:
            pem = (
                public_key
                if public_key.startswith("-----")
                else f"-----BEGIN PUBLIC KEY-----\n{public_key}\n-----END PUBLIC KEY-----"
            )
            key = serialization.load_pem_public_key(pem.encode())

        key.verify(sig_bytes, signed_content, ec.ECDSA(hashes.SHA256()))
        return True
    except InvalidSignature:
        return False
    except Exception as e:
        logger.warning(f"SendGrid signature verification error: {e}")
        return False


@router.post("/sendgrid/events")
async def sendgrid_events(request: Request):
    """
    Receive delivery/engagement events from SendGrid Event Webhook.
    Content-Type: application/json
    """
    body = await request.body()
    signature = request.headers.get("X-Twilio-Email-Event-Webhook-Signature")
    timestamp = request.headers.get("X-Twilio-Email-Event-Webhook-Timestamp")

    public_key = settings.sendgrid_verification_public_key
    if not public_key:
        # No verification key configured — skip silently but log a warning so
        # operators know their endpoint is unprotected.
        logger.warning(
            "SendGrid event webhook received but SENDGRID_VERIFICATION_PUBLIC_KEY is not "
            "configured; skipping ECDSA verification. Set the key from your SendGrid "
            "Event Webhook settings page to enable signature checking."
        )
        return {"skipped": "no verification key configured"}

    if not _verify_sendgrid_signature(public_key, body, signature or "", timestamp or ""):
        logger.warning("SendGrid event webhook ECDSA signature verification failed")
        raise HTTPException(status_code=403, detail="Invalid signature")

    import json
    try:
        events = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(events, list):
        events = [events]

    result = await process_sendgrid_events(events)
    return result


# ── Unipile Webhook ──

def _verify_unipile_signature(secret: str, payload: bytes, signature: str, timestamp: str) -> bool:
    """
    Verify a Unipile webhook request.

    The current Unipile API sends the configured secret directly in the
    X-Webhook-Secret header (plain-text equality check).  A TODO is left here
    to migrate to HMAC-SHA256 + timestamp once Unipile supports that scheme.

    TODO: When Unipile adds HMAC support, replace the direct equality check
    below with HMAC-SHA256 over f"{timestamp}.".encode() + payload, compare
    against a sha256= prefixed signature, and enforce a 300-second clock
    tolerance on the timestamp.
    """
    if not secret:
        # Fail-open when no secret is configured (development / unconfigured).
        logger.warning("Unipile webhook received but UNIPILE_WEBHOOK_SECRET is not configured")
        return True

    # Current Unipile scheme: plain-text secret in X-Webhook-Secret header.
    if signature and hmac.compare_digest(signature, secret):
        return True

    # Future HMAC scheme (guarded by presence of timestamp header).
    if timestamp:
        try:
            event_time = int(timestamp)
            if abs(time.time() - event_time) > 300:
                logger.warning(f"Unipile webhook timestamp too old: {timestamp}")
                return False
        except (ValueError, TypeError):
            return False

        signed_content = f"{timestamp}.".encode() + payload
        expected = hmac.new(secret.encode(), signed_content, hashlib.sha256).hexdigest()
        received = signature.lstrip("sha256=") if signature else ""
        return hmac.compare_digest(expected, received)

    return False


@router.post("/unipile/messages")
async def unipile_messages(request: Request):
    """
    Receive LinkedIn messages from Unipile webhook.
    Content-Type: application/json
    """
    body = await request.body()
    x_webhook_secret = request.headers.get("X-Webhook-Secret", "")
    x_unipile_signature = request.headers.get("X-Unipile-Signature", "")
    x_unipile_timestamp = request.headers.get("X-Unipile-Timestamp", "")

    # Use whichever auth header Unipile sends (current = X-Webhook-Secret plain text;
    # future = X-Unipile-Signature HMAC).
    sig = x_unipile_signature or x_webhook_secret

    if not _verify_unipile_signature(
        settings.unipile_webhook_secret, body, sig, x_unipile_timestamp
    ):
        logger.warning("Unipile webhook verification failed")
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    import json
    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    result = await process_unipile_webhook(payload)
    return result
