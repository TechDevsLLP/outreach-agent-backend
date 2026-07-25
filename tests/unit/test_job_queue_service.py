"""Offline contract tests for the Mongo-backed durable job queue.

The fake collection serializes every Mongo mutation under an asyncio lock.  It
implements only the query/update operators used by JobQueueService, preserving
the atomic behavior of ``find_one_and_update`` without opening a network socket.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from models.job import JobState
from services.job_queue_service import JobQueueService

pytestmark = pytest.mark.unit


def _field(document: dict[str, Any], name: str) -> Any:
    return document.get(name)


def _expr_value(document: dict[str, Any], expression: Any) -> Any:
    if isinstance(expression, str) and expression.startswith("$"):
        return _field(document, expression[1:])
    if isinstance(expression, dict) and "$ifNull" in expression:
        value, fallback = expression["$ifNull"]
        resolved = _expr_value(document, value)
        return fallback if resolved is None else resolved
    return expression


def _matches(document: dict[str, Any], query: dict[str, Any]) -> bool:
    for key, expected in query.items():
        if key == "$and":
            if not all(_matches(document, clause) for clause in expected):
                return False
            continue
        if key == "$or":
            if not any(_matches(document, clause) for clause in expected):
                return False
            continue
        if key == "$expr":
            operator, operands = next(iter(expected.items()))
            left = _expr_value(document, operands[0])
            right = _expr_value(document, operands[1])
            if operator == "$lt" and not left < right:
                return False
            if operator == "$gte" and not left >= right:
                return False
            continue

        actual = _field(document, key)
        if isinstance(expected, dict):
            for operator, operand in expected.items():
                if operator == "$in" and actual not in operand:
                    return False
                if operator == "$nin" and actual in operand:
                    return False
                if operator == "$lte" and not (
                    actual is not None and actual <= operand
                ):
                    return False
                if operator == "$gt" and not (actual is not None and actual > operand):
                    return False
        elif actual != expected:
            return False
    return True


def _apply_update(document: dict[str, Any], update: dict[str, Any]) -> None:
    for key, value in update.get("$set", {}).items():
        document[key] = deepcopy(value)
    for key, value in update.get("$setOnInsert", {}).items():
        document.setdefault(key, deepcopy(value))
    for key, value in update.get("$inc", {}).items():
        document[key] = document.get(key, 0) + value
    for key in update.get("$unset", {}):
        document.pop(key, None)


class _AtomicJobsCollection:
    """Small async Mongo stand-in with atomic find-and-update semantics."""

    def __init__(self):
        self.documents: dict[str, dict[str, Any]] = {}
        self._sequence = 0
        self._lock = asyncio.Lock()

    def _next_id(self) -> str:
        self._sequence += 1
        return f"job-{self._sequence}"

    async def insert_one(self, document: dict[str, Any]) -> SimpleNamespace:
        async with self._lock:
            job_id = self._next_id()
            stored = deepcopy(document)
            stored["_id"] = job_id
            self.documents[job_id] = stored
            return SimpleNamespace(inserted_id=job_id)

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        async with self._lock:
            return next(
                (
                    deepcopy(doc)
                    for doc in self.documents.values()
                    if _matches(doc, query)
                ),
                None,
            )

    async def find_one_and_update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        *,
        sort: list[tuple[str, int]] | None = None,
        upsert: bool = False,
        return_document: Any = None,
    ) -> dict[str, Any] | None:
        del return_document
        async with self._lock:
            candidates = [
                doc for doc in self.documents.values() if _matches(doc, query)
            ]
            if sort:
                # Stable sorts from least- to most-significant reproduce the
                # compound Mongo ordering used by the claim operation.
                for field, direction in reversed(sort):
                    candidates.sort(
                        key=lambda doc: doc.get(field), reverse=direction < 0
                    )
            if candidates:
                stored = candidates[0]
                _apply_update(stored, update)
                return deepcopy(stored)
            if not upsert:
                return None

            job_id = self._next_id()
            stored = {
                key: deepcopy(value)
                for key, value in query.items()
                if not key.startswith("$") and not isinstance(value, dict)
            }
            _apply_update(stored, update)
            stored["_id"] = job_id
            self.documents[job_id] = stored
            return deepcopy(stored)

    async def update_many(
        self, query: dict[str, Any], update: dict[str, Any]
    ) -> SimpleNamespace:
        async with self._lock:
            modified = 0
            for document in self.documents.values():
                if _matches(document, query):
                    _apply_update(document, update)
                    modified += 1
            return SimpleNamespace(modified_count=modified)


@pytest.fixture
def queue() -> JobQueueService:
    return JobQueueService(_AtomicJobsCollection())


async def test_two_workers_racing_one_job_yield_one_lease_owner():
    now = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    collection = _AtomicJobsCollection()
    worker_process_1 = JobQueueService(collection)
    worker_process_2 = JobQueueService(collection)
    await worker_process_1.enqueue(
        account_id="acct-a", job_type="discover", available_at=now
    )

    first, second = await asyncio.gather(
        worker_process_1.claim(account_id="acct-a", worker_id="worker-1", now=now),
        worker_process_2.claim(account_id="acct-a", worker_id="worker-2", now=now),
    )

    winners = [job for job in (first, second) if job is not None]
    assert len(winners) == 1
    assert winners[0].state is JobState.RUNNING
    assert winners[0].lease_owner in {"worker-1", "worker-2"}
    assert winners[0].attempt_count == 1


async def test_expired_lease_is_reclaimed_and_stale_worker_loses_ownership(queue):
    started = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    created = await queue.enqueue(
        account_id="acct-a",
        job_type="enrich",
        available_at=started,
        max_attempts=3,
    )
    first = await queue.claim(
        account_id="acct-a",
        worker_id="worker-old",
        lease_seconds=10,
        now=started,
    )
    assert first is not None

    reclaimed_at = started + timedelta(seconds=11)
    reclaimed = await queue.claim(
        account_id="acct-a",
        worker_id="worker-new",
        lease_seconds=20,
        now=reclaimed_at,
    )

    assert reclaimed is not None
    assert reclaimed.id == created.id
    assert reclaimed.lease_owner == "worker-new"
    assert reclaimed.attempt_count == 2
    assert (
        await queue.heartbeat(
            account_id="acct-a",
            job_id=created.id,
            worker_id="worker-old",
            now=reclaimed_at,
        )
        is None
    )


async def test_failure_retries_then_dead_letters_on_final_attempt(queue):
    started = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    created = await queue.enqueue(
        account_id="acct-a",
        job_type="generate",
        available_at=started,
        max_attempts=2,
    )
    first = await queue.claim(account_id="acct-a", worker_id="worker-1", now=started)
    assert first is not None

    retried = await queue.fail(
        account_id="acct-a",
        job_id=created.id,
        worker_id="worker-1",
        error="provider timeout",
        retry_delay_seconds=30,
        now=started + timedelta(seconds=1),
    )
    assert retried is not None
    assert retried.state is JobState.RETRY_SCHEDULED
    assert retried.available_at == started + timedelta(seconds=31)
    assert (
        await queue.claim(
            account_id="acct-a",
            worker_id="worker-2",
            now=started + timedelta(seconds=30),
        )
        is None
    )

    second = await queue.claim(
        account_id="acct-a",
        worker_id="worker-2",
        now=started + timedelta(seconds=31),
    )
    assert second is not None and second.attempt_count == 2
    dead = await queue.fail(
        account_id="acct-a",
        job_id=created.id,
        worker_id="worker-2",
        error="provider still unavailable",
        now=started + timedelta(seconds=32),
    )
    assert dead is not None
    assert dead.state is JobState.DEAD_LETTER
    assert dead.dead_letter_reason == "attempts_exhausted"
    assert dead.lease_owner is None


async def test_every_read_and_mutation_is_tenant_scoped(queue):
    now = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    job_a = await queue.enqueue(
        account_id="acct-a", job_type="discover", available_at=now
    )
    job_b = await queue.enqueue(
        account_id="acct-b", job_type="discover", available_at=now
    )

    claimed_a = await queue.claim(account_id="acct-a", worker_id="worker-a", now=now)
    assert claimed_a is not None and claimed_a.id == job_a.id
    assert await queue.get(account_id="acct-a", job_id=job_b.id) is None
    assert await queue.cancel(account_id="acct-a", job_id=job_b.id, now=now) is None
    assert (
        await queue.complete(
            account_id="acct-b",
            job_id=job_a.id,
            worker_id="worker-a",
            now=now + timedelta(seconds=1),
        )
        is None
    )

    claimed_b = await queue.claim(account_id="acct-b", worker_id="worker-b", now=now)
    assert claimed_b is not None and claimed_b.id == job_b.id


async def test_deterministic_key_is_idempotent_within_tenant_and_job_type(queue):
    now = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    first = await queue.enqueue(
        account_id="acct-a",
        job_type="discover",
        job_key="campaign:123:discovery:v1",
        payload={"first": True},
        available_at=now,
    )
    duplicate = await queue.enqueue(
        account_id="acct-a",
        job_type="discover",
        job_key="campaign:123:discovery:v1",
        payload={"first": False},
        available_at=now,
    )
    other_tenant = await queue.enqueue(
        account_id="acct-b",
        job_type="discover",
        job_key="campaign:123:discovery:v1",
        available_at=now,
    )

    assert duplicate.id == first.id
    assert duplicate.payload == {"first": True}
    assert other_tenant.id != first.id


async def test_heartbeat_checkpoint_completion_and_cancellation_are_durable(queue):
    now = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    first = await queue.enqueue(
        account_id="acct-a", job_type="enrich", available_at=now
    )
    leased = await queue.claim(
        account_id="acct-a", worker_id="worker-1", lease_seconds=10, now=now
    )
    assert leased is not None

    heartbeat_at = now + timedelta(seconds=5)
    heartbeat = await queue.heartbeat(
        account_id="acct-a",
        job_id=first.id,
        worker_id="worker-1",
        lease_seconds=20,
        now=heartbeat_at,
    )
    assert heartbeat is not None
    assert heartbeat.lease_expires_at == heartbeat_at + timedelta(seconds=20)
    checkpointed = await queue.checkpoint(
        account_id="acct-a",
        job_id=first.id,
        worker_id="worker-1",
        checkpoint={"cursor": "page-3"},
        now=heartbeat_at,
    )
    assert checkpointed is not None and checkpointed.checkpoint == {"cursor": "page-3"}
    completed = await queue.complete(
        account_id="acct-a",
        job_id=first.id,
        worker_id="worker-1",
        result={"processed": 25},
        now=heartbeat_at,
    )
    assert completed is not None
    assert completed.state is JobState.COMPLETED
    assert completed.result == {"processed": 25}
    assert completed.lease_owner is None

    second = await queue.enqueue(
        account_id="acct-a", job_type="enrich", available_at=now
    )
    cancelled = await queue.cancel(account_id="acct-a", job_id=second.id, now=now)
    assert cancelled is not None and cancelled.state is JobState.CANCELLED
    assert await queue.claim(account_id="acct-a", worker_id="worker-2", now=now) is None
