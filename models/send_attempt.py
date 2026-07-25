"""Durable provider-send attempt contract.

The dispatching state is deliberately a point of no automatic return: once a
worker crosses the provider boundary, a crash can only be resolved by provider
reconciliation or a human decision, never by blind retry.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SendAttemptState(str, Enum):
    PREPARED = "prepared"
    RETRY_SCHEDULED = "retry_scheduled"
    DISPATCHING = "dispatching"
    SENT = "sent"
    AMBIGUOUS = "ambiguous"
    FAILED_TERMINAL = "failed_terminal"


class SendAttemptIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    account_id: str = Field(min_length=1)
    enrollment_id: str = Field(min_length=1)
    sequence_version: str = Field(min_length=1)
    node_id: str = Field(min_length=1)
    generation: int = Field(default=0, ge=0)

    @property
    def send_key(self) -> str:
        return ":".join(
            (
                self.enrollment_id,
                self.sequence_version,
                self.node_id,
                str(self.generation),
            )
        )


class SendAttemptDocument(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    id: Any | None = Field(default=None, alias="_id")
    send_key: str
    account_id: str
    enrollment_id: str
    sequence_version: str
    node_id: str
    generation: int
    state: SendAttemptState
    channel: str
    provider: str | None = None
    provider_account_id: str | None = None
    sender_record_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    provider_result: dict[str, Any] | None = None
    provider_error: str | None = None
    attempt_count: int = 0
    available_at: datetime = Field(default_factory=utc_now)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
