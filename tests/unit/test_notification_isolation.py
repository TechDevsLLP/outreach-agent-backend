"""Offline notification contract and tenant-boundary regressions."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from services import notification_service

pytestmark = pytest.mark.unit


async def test_create_notification_requires_tenant_before_database_access(monkeypatch):
    collection = SimpleNamespace(insert_one=AsyncMock())
    monkeypatch.setattr(notification_service, "notifications_collection", collection)

    with pytest.raises(ValueError, match="account_id"):
        await notification_service.create_notification(
            account_id="",
            type="reply",
            title="New reply",
            body="Hello",
        )

    collection.insert_one.assert_not_awaited()


async def test_create_notification_persists_one_canonical_schema(monkeypatch):
    inserted_id = ObjectId()
    collection = SimpleNamespace(
        insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=inserted_id))
    )
    monkeypatch.setattr(notification_service, "notifications_collection", collection)

    result = await notification_service.create_notification(
        account_id="tenant-a",
        type="email_reply",
        title="Ada replied",
        body="Interested",
        campaign_id="campaign-a",
        prospect_id="prospect-a",
        channel="email",
    )

    stored = collection.insert_one.await_args.args[0]
    assert stored["account_id"] == "tenant-a"
    assert stored["campaign_id"] == "campaign-a"
    assert stored["body"] == "Interested"
    assert "message" not in stored
    assert stored["is_read"] is False
    assert result["_id"] == inserted_id


async def test_notification_mutations_are_tenant_scoped(monkeypatch):
    notification_id = ObjectId()
    collection = SimpleNamespace(
        update_one=AsyncMock(return_value=SimpleNamespace(modified_count=1)),
        update_many=AsyncMock(return_value=SimpleNamespace(modified_count=2)),
        delete_one=AsyncMock(return_value=SimpleNamespace(deleted_count=1)),
    )
    monkeypatch.setattr(notification_service, "notifications_collection", collection)

    assert await notification_service.mark_read("tenant-a", str(notification_id))
    assert await notification_service.mark_all_read("tenant-a") == 2
    assert await notification_service.delete_notification(
        "tenant-a", str(notification_id)
    )

    assert collection.update_one.await_args_list[0].args[0] == {
        "_id": notification_id,
        "account_id": "tenant-a",
        "is_read": False,
    }
    assert collection.update_many.await_args.args[0] == {
        "account_id": "tenant-a",
        "is_read": False,
    }
    assert collection.delete_one.await_args.args[0] == {
        "_id": notification_id,
        "account_id": "tenant-a",
    }


def test_sse_event_uses_canonical_body_and_campaign_fields():
    event = notification_service._notification_event(
        {
            "_id": ObjectId(),
            "type": "linkedin_reply",
            "title": "New LinkedIn reply",
            "body": "Thanks",
            "campaign_id": "campaign-a",
            "account_id": "tenant-a",
            "created_at": datetime(2026, 7, 15),
        }
    )

    assert event["body"] == "Thanks"
    assert event["campaign_id"] == "campaign-a"
    assert "message" not in event
    assert "account_id" not in event
