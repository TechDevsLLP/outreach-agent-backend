"""
EmailAccount model - supports sendgrid/google/microsoft/smtp email providers.
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
    provider: str  # sendgrid/google/microsoft/smtp

    # ── SendGrid specific ──
    sendgrid_api_key: Optional[str] = None  # encrypted in practice
    sendgrid_sender_name: Optional[str] = None
    sendgrid_sender_email: Optional[str] = None
    sendgrid_reply_to: Optional[str] = None

    # ── OAuth specific (Google / Microsoft) ──
    oauth_access_token: Optional[str] = None
    oauth_refresh_token: Optional[str] = None
    oauth_token_expiry: Optional[datetime] = None
    oauth_scopes: list[str] = Field(default_factory=list)
    microsoft_tenant_id: Optional[str] = None

    # ── SMTP specific ──
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_encryption: Optional[str] = None  # tls/ssl/none
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None

    # ── Status ──
    status: str = "connected"  # connected/disconnected/error/warming
    error_message: Optional[str] = None
    last_health_check: Optional[datetime] = None

    # ── Send limits & warm-up ──
    daily_send_limit: int = 50
    warmup_enabled: bool = False
    warmup_status: Optional[str] = None  # warming/active/paused
    warmup_day: int = 0

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
    sendgrid_sender_name: Optional[str] = None
    sendgrid_sender_email: Optional[str] = None
    sendgrid_reply_to: Optional[str] = None
    oauth_scopes: list[str] = Field(default_factory=list)
    microsoft_tenant_id: Optional[str] = None
    oauth_token_expiry: Optional[datetime] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None
    smtp_encryption: Optional[str] = None
    smtp_username: Optional[str] = None
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    status: str = "connected"
    error_message: Optional[str] = None
    last_health_check: Optional[datetime] = None
    daily_send_limit: int = 50
    warmup_enabled: bool = False
    warmup_status: Optional[str] = None
    warmup_day: int = 0
    emails_sent_today: int = 0
    emails_sent_today_reset_at: Optional[datetime] = None
    health: str = "healthy"
    total_sent: int = 0
    total_bounced: int = 0
    bounce_rate: float = 0.0
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(populate_by_name=True)


class SendGridAccountRequest(BaseModel):
    """Request body for connecting a SendGrid email account."""
    display_name: str
    sendgrid_api_key: str
    sendgrid_sender_name: str
    sendgrid_sender_email: str
    sendgrid_reply_to: Optional[str] = None


class SmtpAccountRequest(BaseModel):
    """Request body for connecting an SMTP/IMAP email account."""
    email: str
    display_name: str
    smtp_host: str
    smtp_port: int
    smtp_encryption: str  # tls/ssl/none
    smtp_username: str
    smtp_password: str
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
