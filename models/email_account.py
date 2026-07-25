"""
EmailAccount model - supports google (Gmail API) / zoho (Zoho Mail API) / smtp
(custom SMTP+IMAP) email providers. Credential fields (oauth_access_token,
oauth_refresh_token, smtp_password, imap_password) are encrypted at rest via
services/email_account_crypto.py — always write through encrypt_account_fields()
and read through decrypt_account() (the provider factory does this automatically).
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class EmailAccountBase(BaseModel):
    """Core email account fields."""
    account_id: str
    user_id: str
    email: str
    display_name: str
    provider: str  # google/zoho/smtp (microsoft kept wired but unimplemented — see campaign_engine)

    # ── OAuth specific (Google / Zoho / Microsoft) ──
    oauth_access_token: Optional[str] = None  # encrypted at rest
    oauth_refresh_token: Optional[str] = None  # encrypted at rest
    oauth_token_expiry: Optional[datetime] = None
    oauth_scopes: list[str] = Field(default_factory=list)
    microsoft_tenant_id: Optional[str] = None  # unused — Microsoft send is an unimplemented stub

    # ── Zoho Mail specific ──
    zoho_account_id: Optional[str] = None       # accountId from GET /api/accounts
    zoho_api_domain: Optional[str] = None        # e.g. https://mail.zoho.com (per data center)
    zoho_accounts_domain: Optional[str] = None   # e.g. https://accounts.zoho.com (OAuth DC)
    zoho_from_address: Optional[str] = None
    zoho_inbox_folder_id: Optional[str] = None   # resolved at connect time, used for message content fetch

    # ── SMTP + IMAP specific (IMAP required for reply-check + drafts) ──
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_encryption: Optional[str] = None  # tls/ssl/none
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None  # encrypted at rest
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_encryption: Optional[str] = None  # tls/ssl/none
    imap_username: Optional[str] = None
    imap_password: Optional[str] = None  # encrypted at rest

    # ── Status ──
    status: str = "connected"  # connected/disconnected/error/warming
    error_message: Optional[str] = None
    last_health_check: Optional[datetime] = None

    # ── Send limits & warm-up ──
    daily_send_limit: int = 50
    warmup_enabled: bool = True
    warmup_status: Optional[str] = "warming"  # warming/active/paused
    warmup_day: int = 0
    warmup_started_at: Optional[datetime] = None

    # ── Daily counters ──
    emails_sent_today: int = 0
    emails_sent_today_reset_at: Optional[datetime] = None

    # ── Health & lifetime stats ──
    health: str = "healthy"  # healthy/warning/suspended
    total_sent: int = 0
    total_bounced: int = 0
    bounce_rate: float = 0.0


class EmailAccountDocument(EmailAccountBase):
    """Full email account document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class EmailAccountResponse(BaseModel):
    """
    Email account response - sensitive credential fields are excluded.
    Extends the non-sensitive subset of EmailAccountDocument.
    """
    id: str = Field(alias="_id")
    account_id: str
    user_id: str
    email: str
    display_name: str
    provider: str
    oauth_scopes: list[str] = Field(default_factory=list)
    oauth_token_expiry: Optional[datetime] = None
    microsoft_tenant_id: Optional[str] = None
    zoho_account_id: Optional[str] = None
    zoho_api_domain: Optional[str] = None
    zoho_accounts_domain: Optional[str] = None
    zoho_from_address: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_encryption: Optional[str] = None
    smtp_username: Optional[str] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_encryption: Optional[str] = None
    imap_username: Optional[str] = None
    status: str = "connected"
    error_message: Optional[str] = None
    last_health_check: Optional[datetime] = None
    daily_send_limit: int = 50
    warmup_enabled: bool = True
    warmup_status: Optional[str] = "warming"
    warmup_day: int = 0
    warmup_started_at: Optional[datetime] = None
    emails_sent_today: int = 0
    emails_sent_today_reset_at: Optional[datetime] = None
    health: str = "healthy"
    total_sent: int = 0
    total_bounced: int = 0
    bounce_rate: float = 0.0
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(populate_by_name=True)


class SmtpAccountRequest(BaseModel):
    """
    Request body for connecting a custom SMTP+IMAP email account.
    IMAP credentials are required — without them the account could only send,
    not check for replies or create native mailbox drafts.
    """
    email: str
    display_name: str
    smtp_host: str
    smtp_port: int
    smtp_encryption: str  # tls/ssl/none
    smtp_username: str
    smtp_password: str
    imap_host: str
    imap_port: int
    imap_encryption: str  # tls/ssl/none
    imap_username: str
    imap_password: str
