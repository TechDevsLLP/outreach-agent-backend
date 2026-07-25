"""Atomic send-attempt/outbox state machine and enrollment leasing."""

from __future__ import annotations

import inspect
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from models.send_attempt import SendAttemptIdentity, SendAttemptState


def _utc_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _await(value):
    return await value if inspect.isawaitable(value) else value


class SendAttemptService:
    def __init__(self, collection):
        self.collection = collection

    async def prepare(
        self,
        *,
        identity: SendAttemptIdentity,
        channel: str,
        payload: dict[str, Any],
        now: datetime | None = None,
    ) -> dict:
        now = _utc_now(now)
        doc = {
            "send_key": identity.send_key,
            **identity.model_dump(),
            "state": SendAttemptState.PREPARED.value,
            "channel": channel,
            "payload": payload,
            "attempt_count": 0,
            "available_at": now,
            "created_at": now,
            "updated_at": now,
        }
        try:
            stored = await self.collection.find_one_and_update(
                {"send_key": identity.send_key},
                {"$setOnInsert": doc},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            stored = await self.collection.find_one({"send_key": identity.send_key})
        if not stored:
            raise RuntimeError("send attempt prepare did not return a document")
        # A key collision across tenants is corruption, not an idempotent hit.
        if str(stored.get("account_id")) != identity.account_id:
            raise RuntimeError("send key collision crossed tenant boundary")
        return stored

    async def _expire_dispatch_to_ambiguous(
        self, send_key: str, now: datetime
    ) -> None:
        await self.collection.update_one(
            {
                "send_key": send_key,
                "state": SendAttemptState.DISPATCHING.value,
                "lease_expires_at": {"$lte": now},
            },
            {
                "$set": {
                    "state": SendAttemptState.AMBIGUOUS.value,
                    "provider_error": "worker lease expired after provider boundary",
                    "ambiguous_at": now,
                    "updated_at": now,
                },
                "$unset": {"lease_owner": "", "lease_expires_at": ""},
            },
        )

    async def dispatch(
        self,
        *,
        identity: SendAttemptIdentity,
        channel: str,
        payload: dict[str, Any],
        worker_id: str,
        before_provider: Callable[[], Any] | None,
        provider_call: Callable[[Any], Awaitable[dict | None]],
        lease_seconds: int = 120,
        retry_delay_seconds: int = 300,
        now: datetime | None = None,
    ) -> dict:
        now = _utc_now(now)
        stored = await self.prepare(
            identity=identity, channel=channel, payload=payload, now=now
        )
        await self._expire_dispatch_to_ambiguous(identity.send_key, now)
        stored = await self.collection.find_one({"send_key": identity.send_key}) or stored

        if stored.get("state") in {
            SendAttemptState.SENT.value,
            SendAttemptState.AMBIGUOUS.value,
            SendAttemptState.FAILED_TERMINAL.value,
            SendAttemptState.DISPATCHING.value,
        }:
            return stored

        # Sender/recipient resolution is deliberately before the provider
        # boundary and can therefore be retried safely.
        try:
            context = await _await(before_provider()) if before_provider else None
        except Exception as exc:
            retry_at = now + timedelta(seconds=retry_delay_seconds)
            await self.collection.update_one(
                {
                    "send_key": identity.send_key,
                    "state": {"$in": [
                        SendAttemptState.PREPARED.value,
                        SendAttemptState.RETRY_SCHEDULED.value,
                    ]},
                },
                {"$set": {
                    "state": SendAttemptState.RETRY_SCHEDULED.value,
                    "provider_error": str(exc)[:2000],
                    "failure_phase": "before_provider",
                    "available_at": retry_at,
                    "updated_at": now,
                }},
            )
            return await self.collection.find_one({"send_key": identity.send_key})

        metadata = (
            context.get("_attempt_metadata", {})
            if isinstance(context, dict)
            else {}
        )
        if metadata:
            allowed_metadata = {
                key: metadata[key]
                for key in ("provider", "provider_account_id", "sender_record_id")
                if metadata.get(key) is not None
            }
            if allowed_metadata:
                await self.collection.update_one(
                    {
                        "send_key": identity.send_key,
                        "state": {"$in": [
                            SendAttemptState.PREPARED.value,
                            SendAttemptState.RETRY_SCHEDULED.value,
                        ]},
                    },
                    {"$set": {**allowed_metadata, "updated_at": now}},
                )

        lease_expires = now + timedelta(seconds=lease_seconds)
        claimed = await self.collection.find_one_and_update(
            {
                "send_key": identity.send_key,
                "state": {"$in": [
                    SendAttemptState.PREPARED.value,
                    SendAttemptState.RETRY_SCHEDULED.value,
                ]},
                "available_at": {"$lte": now},
            },
            {
                "$set": {
                    "state": SendAttemptState.DISPATCHING.value,
                    "lease_owner": worker_id,
                    "lease_expires_at": lease_expires,
                    "dispatch_started_at": now,
                    "updated_at": now,
                },
                "$inc": {"attempt_count": 1},
                "$unset": {"provider_error": "", "failure_phase": ""},
            },
            return_document=ReturnDocument.AFTER,
        )
        if not claimed:
            return await self.collection.find_one({"send_key": identity.send_key})

        try:
            result = await provider_call(
                {"resolved": context, "payload": deepcopy(stored.get("payload") or payload)}
            )
            if not result:
                raise RuntimeError("provider returned no durable send result")
        except Exception as exc:
            # The provider boundary was crossed. Even a timeout or apparent
            # error can hide an accepted send, so quarantine for reconciliation.
            await self.collection.update_one(
                {
                    "send_key": identity.send_key,
                    "state": SendAttemptState.DISPATCHING.value,
                    "lease_owner": worker_id,
                },
                {"$set": {
                    "state": SendAttemptState.AMBIGUOUS.value,
                    "provider_error": str(exc)[:2000],
                    "failure_phase": "after_provider_boundary",
                    "ambiguous_at": _utc_now(),
                    "updated_at": _utc_now(),
                }, "$unset": {"lease_owner": "", "lease_expires_at": ""}},
            )
            return await self.collection.find_one({"send_key": identity.send_key})

        completed_at = _utc_now()
        await self.collection.update_one(
            {
                "send_key": identity.send_key,
                "state": SendAttemptState.DISPATCHING.value,
                "lease_owner": worker_id,
            },
            {"$set": {
                "state": SendAttemptState.SENT.value,
                "provider_result": result,
                "sent_at": completed_at,
                "updated_at": completed_at,
            }, "$unset": {"lease_owner": "", "lease_expires_at": ""}},
        )
        return await self.collection.find_one({"send_key": identity.send_key})

    async def mark_cap_released(self, send_key: str, now: datetime | None = None) -> bool:
        """Idempotency guard for daily-cap release: atomically claims the right to
        release this attempt's reserved cap slot(s) exactly once.

        Returns True the first time (caller must release), False on any replay
        (another caller already released — do nothing) so retried/leased-again
        failures never double-decrement the campaign/sender daily counters.
        """
        now = _utc_now(now)
        result = await self.collection.update_one(
            {"send_key": send_key, "cap_released_at": {"$exists": False}},
            {"$set": {"cap_released_at": now}},
        )
        return result.modified_count > 0

    async def reconcile(
        self,
        *,
        send_key: str,
        outcome: str,
        provider_result: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict | None:
        """Resolve an ambiguous attempt from provider evidence.

        ``not_sent`` is the only reconciliation outcome that re-opens dispatch.
        ``unknown`` remains quarantined.
        """
        now = _utc_now(now)
        if outcome == "sent":
            update = {"$set": {
                "state": SendAttemptState.SENT.value,
                "provider_result": provider_result or {},
                "reconciled_at": now,
                "sent_at": now,
                "updated_at": now,
            }}
        elif outcome == "not_sent":
            update = {"$set": {
                "state": SendAttemptState.RETRY_SCHEDULED.value,
                "available_at": now,
                "reconciled_at": now,
                "updated_at": now,
            }, "$unset": {"provider_error": "", "ambiguous_at": ""}}
        elif outcome == "unknown":
            update = {"$set": {"reconciled_at": now, "updated_at": now}}
        else:
            raise ValueError("outcome must be sent, not_sent, or unknown")
        return await self.collection.find_one_and_update(
            {"send_key": send_key, "state": SendAttemptState.AMBIGUOUS.value},
            update,
            return_document=ReturnDocument.AFTER,
        )


async def claim_due_enrollment(
    collection,
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: int = 300,
) -> dict | None:
    """Atomically select and lease one due enrollment across workers."""
    lease_token = f"{worker_id}:{int(_utc_now(now).timestamp() * 1_000_000)}"
    lease_expires = now + timedelta(seconds=lease_seconds)
    return await collection.find_one_and_update(
        {
            "status": "active",
            "next_action_at": {"$lte": now},
            "$or": [
                {"execution_lease_expires_at": {"$exists": False}},
                {"execution_lease_expires_at": None},
                {"execution_lease_expires_at": {"$lte": now}},
            ],
        },
        {"$set": {
            "execution_lease_owner": worker_id,
            "execution_lease_token": lease_token,
            "execution_lease_expires_at": lease_expires,
            "execution_claimed_at": now,
        }},
        sort=[("next_action_at", 1), ("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


async def release_enrollment_lease(collection, enrollment: dict) -> None:
    await collection.update_one(
        {
            "_id": enrollment["_id"],
            "execution_lease_token": enrollment.get("execution_lease_token"),
        },
        {"$unset": {
            "execution_lease_owner": "",
            "execution_lease_token": "",
            "execution_lease_expires_at": "",
        }},
    )
