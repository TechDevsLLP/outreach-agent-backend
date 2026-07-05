"""System prompt model for admin-editable AI prompts."""

from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class SystemPromptDocument(BaseModel):
    slug: str
    name: str
    description: str
    content: str
    updated_at: datetime
    updated_by: str
    version: int = 1


class SystemPromptUpdate(BaseModel):
    content: str
