"""
Account (organization) and AccountMember models for OutFlo multi-tenant auth.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class AccountBase(BaseModel):
    """Core account (organization) fields."""
    name: str
    slug: str  # unique URL-safe identifier
    plan: str = "free"  # free/starter/pro/enterprise
    status: str = "active"  # active/suspended/trial
    trial_ends_at: Optional[datetime] = None

    # Sender defaults (used when no email account is configured)
    sender_name: Optional[str] = None
    sender_email: Optional[str] = None
    reply_to_email: Optional[str] = None

    # Usage quotas
    daily_email_quota: int = 50
    daily_linkedin_connection_quota: int = 20
    daily_linkedin_inmail_quota: int = 5
    enrichment_batch_limit: int = 50
    monthly_apify_budget_usd: Optional[float] = None  # spending cap; None = unlimited


class AccountDocument(AccountBase):
    """Full account document as stored in MongoDB."""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class AccountResponse(AccountDocument):
    """Account response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class AccountCreateRequest(BaseModel):
    """Request body for account creation during registration."""
    name: str


class AccountUpdateRequest(BaseModel):
    """Fields that can be updated on an account."""
    name: Optional[str] = None
    slug: Optional[str] = None
    plan: Optional[str] = None
    status: Optional[str] = None
    trial_ends_at: Optional[datetime] = None
    sender_name: Optional[str] = None
    sender_email: Optional[str] = None
    reply_to_email: Optional[str] = None
    daily_email_quota: Optional[int] = None
    daily_linkedin_connection_quota: Optional[int] = None
    daily_linkedin_inmail_quota: Optional[int] = None
    enrichment_batch_limit: Optional[int] = None
    monthly_apify_budget_usd: Optional[float] = None


# ── AccountMember ──

class AccountMemberDocument(BaseModel):
    """Membership linking a user to an account with a role."""
    account_id: str
    user_id: str
    role: str = "member"  # owner/admin/member
    joined_at: datetime = Field(default_factory=datetime.utcnow)


class AccountMemberResponse(AccountMemberDocument):
    """AccountMember response with MongoDB _id as string."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)
