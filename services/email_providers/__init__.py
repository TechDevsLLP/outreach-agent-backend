"""
Email provider factory.

Every caller that needs to send/reply/draft/poll email should go through
`get_provider(email_account)` rather than importing a concrete provider —
this is the single point where stored credentials are decrypted.
"""

from typing import Optional

from services.email_account_crypto import decrypt_account
from services.email_providers.base import DraftResult, EmailProvider, ReplyMeta, SendResult

__all__ = ["get_provider", "EmailProvider", "SendResult", "DraftResult", "ReplyMeta"]


def get_provider(email_account: Optional[dict]) -> Optional[EmailProvider]:
    """
    Return the EmailProvider implementation for this account's `provider` field.
    Decrypts sensitive credential fields (OAuth tokens / SMTP+IMAP passwords)
    before constructing the provider. Returns None for an unknown/missing account
    or an unsupported provider (e.g. the retired `microsoft` stub).
    """
    if not email_account:
        return None

    decrypted = decrypt_account(email_account)
    provider_name = decrypted.get("provider")

    if provider_name == "google":
        from services.email_providers.gmail import GmailProvider
        return GmailProvider(decrypted)
    if provider_name == "zoho":
        from services.email_providers.zoho import ZohoProvider
        return ZohoProvider(decrypted)
    if provider_name == "smtp":
        from services.email_providers.smtp_imap import SmtpImapProvider
        return SmtpImapProvider(decrypted)

    return None
