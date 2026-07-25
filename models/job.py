"""Canonical durable-job schema for Mongo-backed background work.

Jobs are tenant-owned and move through a small, explicit state machine.  BSON
stores datetimes as UTC without timezone metadata; the models normalize every
timestamp returned by the repository back to timezone-aware UTC.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


def utc_now() -> datetime:
    """Return an aware UTC timestamp (never ``datetime.utcnow()``)."""

    return datetime.now(timezone.utc)


class JobState(str, Enum):
    """Persisted durable-job states."""

    QUEUED = "queued"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    DEAD_LETTER = "dead_letter"


CLAIMABLE_JOB_STATES = (JobState.QUEUED, JobState.RETRY_SCHEDULED)
TERMINAL_JOB_STATES = (
    JobState.COMPLETED,
    JobState.CANCELLED,
    JobState.DEAD_LETTER,
)


class JobCreate(BaseModel):
    """Validated input for enqueuing a job."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    account_id: str = Field(min_length=1)
    job_type: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any] = Field(default_factory=dict)
    job_key: str | None = Field(default=None, min_length=1, max_length=500)
    priority: int = Field(default=0, ge=-1_000_000, le=1_000_000)
    available_at: AwareDatetime = Field(default_factory=utc_now)
    max_attempts: int = Field(default=5, ge=1, le=100)

    @field_validator("available_at")
    @classmethod
    def _available_at_is_utc(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)


class JobDocument(JobCreate):
    """Full durable-job document as stored in MongoDB."""

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
    )

    id: Any = Field(alias="_id")
    state: JobState = JobState.QUEUED
    attempt_count: int = Field(default=0, ge=0)

    lease_owner: str | None = None
    lease_expires_at: AwareDatetime | None = None
    heartbeat_at: AwareDatetime | None = None

    checkpoint: dict[str, Any] = Field(default_factory=dict)
    result: Any | None = None
    last_error: str | None = None
    dead_letter_reason: str | None = None

    created_at: AwareDatetime = Field(default_factory=utc_now)
    updated_at: AwareDatetime = Field(default_factory=utc_now)
    started_at: AwareDatetime | None = None
    completed_at: AwareDatetime | None = None
    cancelled_at: AwareDatetime | None = None
    dead_lettered_at: AwareDatetime | None = None

    @field_validator(
        "available_at",
        "lease_expires_at",
        "heartbeat_at",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "cancelled_at",
        "dead_lettered_at",
        mode="before",
    )
    @classmethod
    def _timestamps_are_aware_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        # Motor's default codec returns naive datetimes even though BSON stores
        # them as UTC.  Repository reads are therefore safely re-attached to UTC.
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
