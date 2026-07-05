"""
User model for OutFlo multi-user auth.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class UserBase(BaseModel):
    """Core user fields."""
    email: str
    name: str
    plan: str = "free"  # free/starter/pro/enterprise
    current_account_id: Optional[str] = None  # ObjectId as string
    onboarding_complete: bool = False
    booking_link: Optional[str] = None
    is_active: bool = True


class UserDocument(UserBase):
    """Full user document as stored in MongoDB."""
    password_hash: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class UserResponse(UserBase):
    """User response - exposes id, never exposes password_hash."""
    id: str = Field(alias="_id")

    model_config = ConfigDict(populate_by_name=True)


class UserRegisterRequest(BaseModel):
    """Request body for user registration."""
    email: str
    password: str
    name: str


class UserLoginRequest(BaseModel):
    """Request body for user login."""
    email: str
    password: str


class TokenResponse(BaseModel):
    """JWT token response."""
    access_token: str
    token_type: str = "bearer"
