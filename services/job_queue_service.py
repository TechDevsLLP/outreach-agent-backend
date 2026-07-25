"""Mongo-backed durable job queue with atomic worker leases.

The collection is injected so this module has no provider/database side effects
at import time.  Every mutation includes ``account_id`` and state/lease guards;
stale workers cannot heartbeat, complete, or retry work after losing a lease.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from bson import ObjectId
from pydantic import ValidationError
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from models.job import CLAIMABLE_JOB_STATES, JobCreate, JobDocument, JobState, utc_now


_CLAIMABLE_VALUES = [state.value for state in CLAIMABLE_JOB_STATES]
_TERMINAL_VALUES = [
    JobState.COMPLETED.value,
    JobState.CANCELLED.value,
    JobState.DEAD_LETTER.value,
]


class JobQueueValidationError(ValueError):
    """Raised before an invalid job operation reaches MongoDB."""


def _require_text(value: str, field_name: str) -> str:
    value = value.strip() if isinstance(value, str) else ""
    if not value:
        raise JobQueueValidationError(f"{field_name} must be a non-empty string")
    return value


def _aware_utc(value: datetime | None = None) -> datetime:
    value = value or utc_now()
    if value.tzinfo is None or value.utcoffset() is None:
        raise JobQueueValidationError("now must be timezone-aware")
    return value.astimezone(timezone.utc)


def _job_id(value: Any) -> Any:
    """Accept native ObjectIds and API-style ObjectId strings."""

    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return value


def _as_job(document: dict[str, Any] | None) -> JobDocument | None:
    return JobDocument.model_validate(document) if document is not None else None


class JobQueueService:
    """Durable queue operations over one Mongo collection.

    Claiming is a single ``find_one_and_update`` operation.  The update both
    chooses and leases one eligible job, which is what makes multiple workers
    safe without process-local locks.
    """

    def __init__(self, collection: Any):
        self.collection = collection

    async def enqueue(
        self,
        *,
        account_id: str,
        job_type: str,
        payload: dict[str, Any] | None = None,
        job_key: str | None = None,
        priority: int = 0,
        available_at: datetime | None = None,
        max_attempts: int = 5,
    ) -> JobDocument:
        """Create a job, or return the existing deterministic-key job.

        ``(account_id, job_type, job_key)`` is the idempotency boundary when a
        key is supplied.  Jobs without a key are always inserted independently.
        """

        try:
            create = JobCreate(
                account_id=account_id,
                job_type=job_type,
                payload=payload or {},
                job_key=job_key,
                priority=priority,
                available_at=available_at or utc_now(),
                max_attempts=max_attempts,
            )
        except ValidationError as exc:
            raise JobQueueValidationError(str(exc)) from exc

        now = utc_now()
        document = create.model_dump(mode="python")
        document.update(
            {
                "state": JobState.QUEUED.value,
                "attempt_count": 0,
                "checkpoint": {},
                "created_at": now,
                "updated_at": now,
            }
        )

        if create.job_key is None:
            result = await self.collection.insert_one(document)
            document["_id"] = result.inserted_id
            return _as_job(document)  # type: ignore[return-value]

        identity = {
            "account_id": create.account_id,
            "job_type": create.job_type,
            "job_key": create.job_key,
        }
        try:
            stored = await self.collection.find_one_and_update(
                identity,
                {"$setOnInsert": document},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # A concurrent upsert may win between match and insert.  The unique
            # index makes that harmless; read and return the winner.
            stored = await self.collection.find_one(identity)
        if stored is None:  # defensive: a conforming Mongo collection returns it
            raise RuntimeError("job enqueue completed without a stored document")
        return _as_job(stored)  # type: ignore[return-value]

    async def reap_exhausted(
        self,
        *,
        account_id: str,
        now: datetime | None = None,
    ) -> int:
        """Dead-letter due jobs that cannot legally receive another lease."""

        account_id = _require_text(account_id, "account_id")
        now = _aware_utc(now)
        due_or_expired = {
            "$or": [
                {
                    "state": {"$in": _CLAIMABLE_VALUES},
                    "available_at": {"$lte": now},
                },
                {
                    "state": JobState.RUNNING.value,
                    "lease_expires_at": {"$lte": now},
                },
            ]
        }
        result = await self.collection.update_many(
            {
                "account_id": account_id,
                "$and": [
                    due_or_expired,
                    {
                        "$expr": {
                            "$gte": [
                                {"$ifNull": ["$attempt_count", 0]},
                                {"$ifNull": ["$max_attempts", 1]},
                            ]
                        }
                    },
                ],
            },
            {
                "$set": {
                    "state": JobState.DEAD_LETTER.value,
                    "dead_letter_reason": "attempts_exhausted",
                    "dead_lettered_at": now,
                    "updated_at": now,
                },
                "$unset": {
                    "lease_owner": "",
                    "lease_expires_at": "",
                    "heartbeat_at": "",
                },
            },
        )
        return int(result.modified_count)

    async def claim(
        self,
        *,
        account_id: str,
        worker_id: str,
        lease_seconds: float = 60.0,
        job_types: Iterable[str] | None = None,
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Atomically lease the highest-priority due job for one tenant."""

        account_id = _require_text(account_id, "account_id")
        worker_id = _require_text(worker_id, "worker_id")
        if lease_seconds <= 0:
            raise JobQueueValidationError("lease_seconds must be greater than zero")
        now = _aware_utc(now)

        normalized_types: list[str] | None = None
        if job_types is not None:
            normalized_types = [_require_text(value, "job_type") for value in job_types]
            if not normalized_types:
                raise JobQueueValidationError("job_types must not be empty")

        # Prevent an expired final attempt from remaining permanently RUNNING.
        await self.reap_exhausted(account_id=account_id, now=now)

        query: dict[str, Any] = {
            "account_id": account_id,
            "$and": [
                {
                    "$or": [
                        {
                            "state": {"$in": _CLAIMABLE_VALUES},
                            "available_at": {"$lte": now},
                        },
                        {
                            "state": JobState.RUNNING.value,
                            "lease_expires_at": {"$lte": now},
                        },
                    ]
                },
                {
                    "$expr": {
                        "$lt": [
                            {"$ifNull": ["$attempt_count", 0]},
                            {"$ifNull": ["$max_attempts", 1]},
                        ]
                    }
                },
            ],
        }
        if normalized_types is not None:
            query["job_type"] = {"$in": normalized_types}

        lease_expires_at = now + timedelta(seconds=lease_seconds)
        document = await self.collection.find_one_and_update(
            query,
            {
                "$set": {
                    "state": JobState.RUNNING.value,
                    "lease_owner": worker_id,
                    "lease_expires_at": lease_expires_at,
                    "heartbeat_at": now,
                    "started_at": now,
                    "updated_at": now,
                },
                "$inc": {"attempt_count": 1},
                "$unset": {"last_error": "", "dead_letter_reason": ""},
            },
            sort=[("priority", -1), ("available_at", 1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(document)

    async def heartbeat(
        self,
        *,
        account_id: str,
        job_id: Any,
        worker_id: str,
        lease_seconds: float = 60.0,
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Renew a live lease; return ``None`` when ownership was lost."""

        account_id = _require_text(account_id, "account_id")
        worker_id = _require_text(worker_id, "worker_id")
        if lease_seconds <= 0:
            raise JobQueueValidationError("lease_seconds must be greater than zero")
        now = _aware_utc(now)
        document = await self.collection.find_one_and_update(
            {
                "_id": _job_id(job_id),
                "account_id": account_id,
                "state": JobState.RUNNING.value,
                "lease_owner": worker_id,
                "lease_expires_at": {"$gt": now},
            },
            {
                "$set": {
                    "heartbeat_at": now,
                    "lease_expires_at": now + timedelta(seconds=lease_seconds),
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(document)

    async def checkpoint(
        self,
        *,
        account_id: str,
        job_id: Any,
        worker_id: str,
        checkpoint: dict[str, Any],
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Persist resumable progress while the caller owns a live lease."""

        account_id = _require_text(account_id, "account_id")
        worker_id = _require_text(worker_id, "worker_id")
        now = _aware_utc(now)
        document = await self.collection.find_one_and_update(
            {
                "_id": _job_id(job_id),
                "account_id": account_id,
                "state": JobState.RUNNING.value,
                "lease_owner": worker_id,
                "lease_expires_at": {"$gt": now},
            },
            {"$set": {"checkpoint": checkpoint, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(document)

    async def complete(
        self,
        *,
        account_id: str,
        job_id: Any,
        worker_id: str,
        result: Any | None = None,
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Complete a job only while the caller still owns its live lease."""

        account_id = _require_text(account_id, "account_id")
        worker_id = _require_text(worker_id, "worker_id")
        now = _aware_utc(now)
        document = await self.collection.find_one_and_update(
            {
                "_id": _job_id(job_id),
                "account_id": account_id,
                "state": JobState.RUNNING.value,
                "lease_owner": worker_id,
                "lease_expires_at": {"$gt": now},
            },
            {
                "$set": {
                    "state": JobState.COMPLETED.value,
                    "result": result,
                    "completed_at": now,
                    "updated_at": now,
                },
                "$unset": {
                    "lease_owner": "",
                    "lease_expires_at": "",
                    "heartbeat_at": "",
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(document)

    async def fail(
        self,
        *,
        account_id: str,
        job_id: Any,
        worker_id: str,
        error: str,
        retry_delay_seconds: float = 0.0,
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Schedule a retry, or dead-letter the exhausted final attempt."""

        account_id = _require_text(account_id, "account_id")
        worker_id = _require_text(worker_id, "worker_id")
        error = _require_text(error, "error")
        if retry_delay_seconds < 0:
            raise JobQueueValidationError("retry_delay_seconds must not be negative")
        now = _aware_utc(now)
        owned_lease = {
            "_id": _job_id(job_id),
            "account_id": account_id,
            "state": JobState.RUNNING.value,
            "lease_owner": worker_id,
            "lease_expires_at": {"$gt": now},
        }
        clear_lease = {
            "lease_owner": "",
            "lease_expires_at": "",
            "heartbeat_at": "",
        }

        # attempt_count is incremented at claim time, so equality means the
        # just-failed execution consumed the final allowed attempt.
        exhausted = await self.collection.find_one_and_update(
            {
                **owned_lease,
                "$expr": {"$gte": ["$attempt_count", "$max_attempts"]},
            },
            {
                "$set": {
                    "state": JobState.DEAD_LETTER.value,
                    "last_error": error,
                    "dead_letter_reason": "attempts_exhausted",
                    "dead_lettered_at": now,
                    "updated_at": now,
                },
                "$unset": clear_lease,
            },
            return_document=ReturnDocument.AFTER,
        )
        if exhausted is not None:
            return _as_job(exhausted)

        retry = await self.collection.find_one_and_update(
            {
                **owned_lease,
                "$expr": {"$lt": ["$attempt_count", "$max_attempts"]},
            },
            {
                "$set": {
                    "state": JobState.RETRY_SCHEDULED.value,
                    "available_at": now + timedelta(seconds=retry_delay_seconds),
                    "last_error": error,
                    "updated_at": now,
                },
                "$unset": clear_lease,
            },
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(retry)

    async def cancel(
        self,
        *,
        account_id: str,
        job_id: Any,
        now: datetime | None = None,
    ) -> JobDocument | None:
        """Cancel non-terminal work for one tenant, including leased work."""

        account_id = _require_text(account_id, "account_id")
        now = _aware_utc(now)
        document = await self.collection.find_one_and_update(
            {
                "_id": _job_id(job_id),
                "account_id": account_id,
                "state": {"$nin": _TERMINAL_VALUES},
            },
            {
                "$set": {
                    "state": JobState.CANCELLED.value,
                    "cancelled_at": now,
                    "updated_at": now,
                },
                "$unset": {
                    "lease_owner": "",
                    "lease_expires_at": "",
                    "heartbeat_at": "",
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        return _as_job(document)

    async def get(self, *, account_id: str, job_id: Any) -> JobDocument | None:
        """Read a job through its tenant boundary."""

        account_id = _require_text(account_id, "account_id")
        return _as_job(
            await self.collection.find_one(
                {"_id": _job_id(job_id), "account_id": account_id}
            )
        )


def get_job_queue_service() -> JobQueueService:
    """Build a service around the application collection without import side effects."""

    from database import jobs_collection

    return JobQueueService(jobs_collection)
