"""
Credential encryption at rest (Fernet).

Used for OAuth tokens (Google/Zoho) and SMTP/IMAP passwords stored on
`email_accounts` documents. Ciphertext is prefixed with `enc::`. Explicit
development and test environments retain legacy plaintext compatibility;
production refuses plaintext writes/reads or an unavailable encryption key.

Generate a key with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

import logging
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

_PREFIX = "enc::"


class CredentialEncryptionError(RuntimeError):
    """Raised when production credential confidentiality cannot be guaranteed."""


def _is_production() -> bool:
    """Treat every environment except explicit development/test as secure runtime."""
    from config import get_settings

    return (get_settings().app_env or "").strip().lower() not in {
        "development",
        "test",
    }


@lru_cache()
def _get_fernet():
    """Return a cached Fernet instance, or None if no key is configured."""
    from config import get_settings

    key = get_settings().encryption_key
    if not key:
        return None

    from cryptography.fernet import Fernet

    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception as e:
        logger.error("Invalid ENCRYPTION_KEY; credential encryption is unavailable: %s", e)
        return None


def encrypt(value: Optional[str]) -> Optional[str]:
    """Encrypt a string, refusing plaintext credential storage in production."""
    if not value:
        return value
    if value.startswith(_PREFIX):
        return value  # already encrypted

    fernet = _get_fernet()
    if fernet is None:
        if _is_production():
            raise CredentialEncryptionError(
                "Credential encryption is unavailable in production"
            )
        return value

    token = fernet.encrypt(value.encode("utf-8")).decode("utf-8")
    return _PREFIX + token


def decrypt(value: Optional[str]) -> Optional[str]:
    """Decrypt a stored string, refusing legacy plaintext in production."""
    if not value:
        return value
    if not value.startswith(_PREFIX):
        if _is_production():
            raise CredentialEncryptionError(
                "Refusing a plaintext credential in production"
            )
        return value

    fernet = _get_fernet()
    if fernet is None:
        logger.error("Encrypted credential found but ENCRYPTION_KEY is unavailable")
        if _is_production():
            raise CredentialEncryptionError(
                "Credential decryption is unavailable in production"
            )
        return None

    token = value[len(_PREFIX):]
    try:
        return fernet.decrypt(token.encode("utf-8")).decode("utf-8")
    except Exception as e:
        logger.error("Failed to decrypt credential: %s", e)
        if _is_production():
            raise CredentialEncryptionError(
                "Credential decryption failed in production"
            ) from e
        return None


def is_encrypted(value: Optional[str]) -> bool:
    return bool(value) and value.startswith(_PREFIX)
