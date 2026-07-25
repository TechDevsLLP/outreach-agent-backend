"""Unit tests for services/email_providers — provider selection + decrypt-on-load."""
import pytest
from bson import ObjectId

from services.email_providers import get_provider
from services.email_providers.gmail import GmailProvider
from services.email_providers.smtp_imap import SmtpImapProvider
from services.email_providers.zoho import ZohoProvider
from utils import crypto

pytestmark = pytest.mark.unit


def _base_account(provider: str, **overrides) -> dict:
    doc = {
        "_id": ObjectId(),
        "account_id": "acct1",
        "user_id": "user1",
        "email": "sender@example.com",
        "display_name": "Sender",
        "provider": provider,
    }
    doc.update(overrides)
    return doc


def test_get_provider_google_returns_gmail_provider():
    account = _base_account("google", oauth_access_token=crypto.encrypt("secret-token"))
    provider = get_provider(account)
    assert isinstance(provider, GmailProvider)
    # Sensitive field must be decrypted on the instance the provider holds.
    assert provider.account["oauth_access_token"] == "secret-token"


def test_get_provider_zoho_returns_zoho_provider():
    account = _base_account("zoho", oauth_refresh_token=crypto.encrypt("refresh-secret"))
    provider = get_provider(account)
    assert isinstance(provider, ZohoProvider)
    assert provider.account["oauth_refresh_token"] == "refresh-secret"


def test_get_provider_smtp_returns_smtp_imap_provider():
    account = _base_account(
        "smtp",
        smtp_password=crypto.encrypt("smtp-secret"),
        imap_password=crypto.encrypt("imap-secret"),
    )
    provider = get_provider(account)
    assert isinstance(provider, SmtpImapProvider)
    assert provider.account["smtp_password"] == "smtp-secret"
    assert provider.account["imap_password"] == "imap-secret"


def test_get_provider_unknown_provider_returns_none():
    account = _base_account("microsoft")  # retired stub — not a supported EmailProvider
    assert get_provider(account) is None


def test_get_provider_none_account_returns_none():
    assert get_provider(None) is None


def test_get_provider_does_not_mutate_original_dict():
    account = _base_account("google", oauth_access_token=crypto.encrypt("secret-token"))
    get_provider(account)
    # decrypt_account must return a copy, not decrypt in place
    assert account["oauth_access_token"].startswith("enc::")
