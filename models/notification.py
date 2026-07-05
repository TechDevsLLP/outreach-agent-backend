from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class NotificationDocument(BaseModel):
    type: str  # "email_reply", "linkedin_reply"
    title: str
    body: str
    prospect_id: Optional[str] = None
    conversation_id: Optional[str] = None
    channel: Optional[str] = None  # "email", "linkedin"
    is_read: bool = False
    read_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class NotificationResponse(NotificationDocument):
    id: str = Field(alias="_id")

    class Config:
        populate_by_name = True


class NotificationListParams(BaseModel):
    page: int = 1
    page_size: int = 20
    is_read: Optional[bool] = None
    type: Optional[str] = None
