"""Regression coverage for campaign daily-cap id handling.

These are pure unit tests: Mongo collections and delivery providers are fakes,
so exercising the scheduler paths cannot connect to Atlas or an outreach API.
"""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from services import campaign_engine as engine
from services import daily_cap_service as caps
from services import suppression_service

pytestmark = pytest.mark.unit


def _fake_db(campaign: dict | None):
    campaigns = SimpleNamespace(
        find_one=AsyncMock(return_value=campaign),
        find_one_and_update=AsyncMock(return_value=campaign),
        update_one=AsyncMock(),
    )
    return SimpleNamespace(
        campaigns=campaigns,
        accounts=SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )


@pytest.mark.parametrize("safe_id", [lambda oid: oid, str])
async def test_reserve_slot_finds_object_id_campaign_from_safe_representations(safe_id):
    campaign_id = ObjectId()
    db = _fake_db({"_id": campaign_id, "daily_caps": {"email": 20}})

    assert await caps.reserve_slot(db, safe_id(campaign_id), "email") is True

    assert db.campaigns.find_one.await_args.args[0] == {"_id": campaign_id}
    assert db.campaigns.find_one_and_update.await_args.args[0]["_id"] == campaign_id


@pytest.mark.parametrize("campaign_id", [None, "", "not-an-object-id", 123])
async def test_reserve_slot_rejects_malformed_campaign_ids_without_db_access(campaign_id):
    db = _fake_db({"_id": ObjectId()})

    assert await caps.reserve_slot(db, campaign_id, "email") is False

    db.campaigns.find_one.assert_not_awaited()
    db.campaigns.find_one_and_update.assert_not_awaited()
    db.campaigns.update_one.assert_not_awaited()


async def test_reserve_slot_fails_safely_when_campaign_does_not_exist():
    campaign_id = ObjectId()
    db = _fake_db(None)

    assert await caps.reserve_slot(db, str(campaign_id), "email") is False

    assert db.campaigns.find_one.await_args.args[0] == {"_id": campaign_id}
    db.campaigns.find_one_and_update.assert_not_awaited()
    db.campaigns.update_one.assert_not_awaited()


async def test_reserve_slot_preserves_campaign_cap_enforcement():
    # Cap enforcement is atomic: the find_one_and_update filter carries
    # {$lt: cap_limit}; when Mongo matches nothing (cap hit) reserve fails.
    campaign_id = ObjectId()
    today = caps._today_key()
    db = _fake_db({
        "_id": campaign_id,
        "daily_caps": {"email": 2},
        "daily_caps_state": {today: {"email": 2}},
    })
    db.campaigns.find_one_and_update.return_value = None

    assert await caps.reserve_slot(db, campaign_id, "email") is False

    update_filter = db.campaigns.find_one_and_update.await_args.args[0]
    assert update_filter["_id"] == campaign_id
    assert update_filter[f"daily_caps_state.{today}.email"] == {"$lt": 2}


async def test_reserve_slot_preserves_account_override_clamp():
    campaign_id = ObjectId()
    account_id = ObjectId()
    today = caps._today_key()
    db = _fake_db({
        "_id": campaign_id,
        "account_id": str(account_id),
        "daily_caps": {"email": 20},
        "daily_caps_state": {today: {"email": 3}},
    })
    db.accounts.find_one.return_value = {"quota_overrides": {"email": 3}}
    db.campaigns.find_one_and_update.return_value = None

    assert await caps.reserve_slot(db, str(campaign_id), "email") is False

    assert db.accounts.find_one.await_args.args[0] == {"_id": account_id}
    # Admin override (3) clamps the campaign cap (20) inside the atomic filter.
    update_filter = db.campaigns.find_one_and_update.await_args.args[0]
    assert update_filter[f"daily_caps_state.{today}.email"] == {"$lt": 3}


async def test_valid_campaign_id_preserves_unknown_channel_bypass():
    db = _fake_db(None)

    assert await caps.reserve_slot(db, ObjectId(), "unsupported_channel") is True

    db.campaigns.find_one.assert_not_awaited()


async def test_release_and_usage_use_the_same_object_id_boundary():
    campaign_id = ObjectId()
    db = _fake_db({"daily_caps": {"email": 20}, "daily_caps_state": {}})

    await caps.release_slot(db, str(campaign_id), "email")
    usage = await caps.get_today_usage(db, str(campaign_id))

    release_filter = db.campaigns.update_one.await_args.args[0]
    assert release_filter["_id"] == campaign_id
    assert release_filter[f"daily_caps_state.{caps._today_key()}.email"] == {"$gt": 0}
    assert db.campaigns.find_one.await_args.args[0] == {"_id": campaign_id}
    assert usage["limits"]["email"] == 20


def test_email_sender_policy_applies_warmup_ramp_and_provider_ceiling():
    warming = caps.sender_policy(
        {
            "provider": "google",
            "daily_send_limit": 120,
            "warmup_enabled": True,
            "warmup_status": "warming",
            "warmup_day": 2,
        },
        "email",
    )
    warmed = caps.sender_policy(
        {
            "provider": "google",
            "daily_send_limit": 120,
            "warmup_enabled": True,
            "warmup_status": "active",
        },
        "email",
    )
    zoho = caps.sender_policy(
        {
            "provider": "zoho",
            "daily_send_limit": 500,
            "warmup_enabled": False,
        },
        "email",
    )

    assert warming["limit"] == 20
    assert warmed["limit"] == 120
    assert (warmed["min_gap"], warmed["max_gap"]) == (2, 8)
    assert zoho["limit"] == 200


def test_linkedin_sender_policy_has_separate_channel_caps_and_pause():
    sender = {
        "daily_connection_limit": 60,
        "daily_inmail_limit": 12,
        "daily_message_limit": 40,
        "warmup_enabled": True,
        "warmup_status": "warming",
        "warmup_day": 1,
    }
    assert caps.sender_policy(sender, "linkedin_connection")["limit"] == 8
    assert caps.sender_policy(sender, "linkedin_inmail")["limit"] == 2
    assert caps.sender_policy(sender, "linkedin_message")["limit"] == 8
    sender["warmup_status"] = "paused"
    assert caps.sender_policy(sender, "linkedin_connection")["limit"] == 0


async def test_sender_reservation_enforces_runtime_spacing_and_effective_cap():
    sender_daily_caps = SimpleNamespace(
        find_one_and_update=AsyncMock(return_value={"_id": "mailbox-1"}),
        update_one=AsyncMock(),
    )
    db = SimpleNamespace(sender_daily_caps=sender_daily_caps)
    sender = {
        "provider": "google",
        "daily_send_limit": 100,
        "warmup_enabled": True,
        "warmup_status": "warming",
        "warmup_day": 0,
    }

    assert await caps.reserve_sender_slot(
        db, "mailbox-1", "email", sender=sender
    ) is True

    query = sender_daily_caps.find_one_and_update.await_args.args[0]
    update = sender_daily_caps.find_one_and_update.await_args.args[1]
    assert query[f"daily_send_state.{caps._today_key()}.email"] == {"$lt": 10}
    assert len(query["$or"]) == 3
    assert update["$set"]["effective_daily_limit"] == 10
    assert update["$set"]["provider"] == "google"


def _patch_engine_collections(monkeypatch, db, *, email=False, linkedin=False):
    monkeypatch.setattr(engine.database, "db", db)
    monkeypatch.setattr(
        engine.database,
        "prospect_state_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        engine.database,
        "campaign_enrollments_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock()),
    )
    monkeypatch.setattr(
        engine.database,
        "campaigns_collection",
        SimpleNamespace(update_one=AsyncMock()),
    )
    monkeypatch.setattr(
        engine.database,
        "send_attempts_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )
    monkeypatch.setattr(
        engine.database,
        "prospects_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None), update_one=AsyncMock()),
    )
    if email:
        monkeypatch.setattr(
            engine.database,
            "email_accounts_collection",
            SimpleNamespace(
                find_one=AsyncMock(
                    return_value={"_id": ObjectId(), "status": "connected"}
                )
            ),
        )
    if linkedin:
        monkeypatch.setattr(
            engine.database,
            "linkedin_accounts_collection",
            SimpleNamespace(
                find_one=AsyncMock(
                    return_value={"_id": ObjectId(), "unipile_status": "OK"}
                )
            ),
        )


async def test_scheduled_smart_email_reaches_sender_cap_with_string_campaign_id(monkeypatch):
    campaign_id = ObjectId()
    db = _fake_db({"_id": campaign_id, "daily_caps": {"email": 20}})
    _patch_engine_collections(monkeypatch, db, email=True)
    monkeypatch.setattr(suppression_service, "is_suppressed", AsyncMock(return_value=False))
    sender_reserve = AsyncMock(return_value=False)
    monkeypatch.setattr(caps, "reserve_sender_slot", sender_reserve)
    monkeypatch.setattr(
        caps, "get_sender_defer_until", AsyncMock(return_value=datetime.utcnow())
    )
    monkeypatch.setattr(
        engine,
        "send_email_via_account",
        AsyncMock(side_effect=AssertionError("provider delivery must not run")),
    )

    result = await engine._execute_smart_enrollment(
        {
            "_id": ObjectId(),
            "campaign_id": str(campaign_id),
            "prospect_id": ObjectId(),
            "account_id": "tenant-1",
            "generated_messages": {"cold_email": {"subject_a": "Hello", "body": "Body"}},
        },
        {"_id": campaign_id, "email_account_id": ObjectId()},
        {"_id": ObjectId(), "email": "prospect@example.com"},
        "email",
    )

    assert result == {"status": "deferred", "reason": "sender_cap_hit"}
    sender_reserve.assert_awaited_once()
    assert db.campaigns.find_one.await_args.args[0] == {"_id": campaign_id}
    engine.send_email_via_account.assert_not_awaited()


async def test_scheduled_sequence_linkedin_reaches_sender_cap_with_string_campaign_id(monkeypatch):
    campaign_id = ObjectId()
    db = _fake_db({"_id": campaign_id, "daily_caps": {"linkedin_connection": 20}})
    _patch_engine_collections(monkeypatch, db, linkedin=True)
    monkeypatch.setattr(suppression_service, "is_suppressed", AsyncMock(return_value=False))
    sender_reserve = AsyncMock(return_value=False)
    monkeypatch.setattr(caps, "reserve_sender_slot", sender_reserve)
    monkeypatch.setattr(
        caps, "get_sender_defer_until", AsyncMock(return_value=datetime.utcnow())
    )
    monkeypatch.setattr(
        engine,
        "get_unipile_service",
        lambda: (_ for _ in ()).throw(AssertionError("provider client must not initialize")),
    )
    graph = {
        "nodes": [{"id": "connect", "channel": "linkedin_connection"}],
        "edges": [],
    }

    await engine._execute_sequence_enrollment(
        {
            "_id": ObjectId(),
            "campaign_id": str(campaign_id),
            "prospect_id": ObjectId(),
            "account_id": "tenant-1",
            "sequence_state": {"current_node_id": "connect", "phase": "pending_send"},
        },
        {"_id": campaign_id, "linkedin_account_id": ObjectId(), "sequence_graph": graph},
        {"_id": ObjectId(), "linkedin": "https://www.linkedin.com/in/example"},
    )

    sender_reserve.assert_awaited_once()
    assert db.campaigns.find_one.await_args.args[0] == {"_id": campaign_id}
    assert engine.database.campaign_enrollments_collection.update_one.await_count == 1
