"""
Encrypt/decrypt the sensitive credential fields on `email_accounts` documents.

Call `encrypt_account_fields()` on any dict right before it is written to Mongo
(insert_one / $set payload), and `decrypt_account()` on any document right
after it is read from Mongo and before its credentials are used.
"""

from typing import Optional

from utils.crypto import decrypt, encrypt

# Fields that must never be stored in plaintext.
SENSITIVE_FIELDS = (
    "oauth_access_token",
    "oauth_refresh_token",
    "smtp_password",
    "imap_password",
)


def encrypt_account_fields(fields: dict) -> dict:
    """Return a copy of `fields` with any present sensitive keys encrypted."""
    out = dict(fields)
    for key in SENSITIVE_FIELDS:
        if key in out and out[key]:
            out[key] = encrypt(out[key])
    return out


def decrypt_account(doc: Optional[dict]) -> Optional[dict]:
    """Return a copy of `doc` with sensitive fields decrypted for in-process use."""
    if not doc:
        return doc
    out = dict(doc)
    for key in SENSITIVE_FIELDS:
        if out.get(key):
            out[key] = decrypt(out[key])
    return out
