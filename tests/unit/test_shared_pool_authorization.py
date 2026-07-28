"""Offline tenant-isolation contracts for the shared company/prospect pool."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi import BackgroundTasks, HTTPException

from auth import get_super_admin
from routes import campaign_enrollments, companies, prospects


pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, rows):
        self.rows = list(rows)

    async def to_list(self, _length):
        return list(self.rows)


def _ctx(account_id: ObjectId) -> dict:
    return {"account": {"_id": str(account_id)}, "user": {"_id": str(ObjectId())}}


def _filter_contains_account(query: dict, account_id: ObjectId) -> bool:
    values = (query.get("account_id") or {}).get("$in", [])
    return str(account_id) in values or account_id in values


def test_every_legacy_company_pool_mutation_requires_superadmin():
    protected_paths = {
        "/api/companies/scrape",
        "/api/companies/scrape/upload-excel",
        "/api/companies/bulk-delete",
        "/api/companies/bulk-rescrape",
        "/api/companies/{company_id}/employees/promote-to-prospects",
        "/api/companies/{company_id}/enrich",
        "/api/companies/from-linkedin",
        "/api/companies/{company_id}",
    }
    mutation_routes = {
        route.path: route
        for route in companies.router.routes
        if route.path in protected_paths
        and ({"POST", "DELETE"} & set(route.methods or []))
    }

    assert set(mutation_routes) == protected_paths
    for route in mutation_routes.values():
        dependency_calls = {dep.call for dep in route.dependant.dependencies}
        assert get_super_admin in dependency_calls


async def test_manual_admin_prospect_creation_never_writes_tenant_private_fields(
    monkeypatch,
):
    inserted = {}

    async def insert_one(doc):
        inserted.update(doc)
        return SimpleNamespace(inserted_id=ObjectId())

    monkeypatch.setattr(
        prospects,
        "prospects_collection",
        SimpleNamespace(insert_one=insert_one),
    )

    await prospects.create_prospect_manual(
        {
            "full_name": "Ada Lovelace",
            "email": "ada@example.test",
            "account_id": str(ObjectId()),
            "notes": "tenant secret",
            "tags": ["private"],
            "status": "qualified",
        },
        _admin={"email": "admin@example.test"},
    )

    assert inserted["full_name"] == "Ada Lovelace"
    assert "account_id" not in inserted
    assert "notes" not in inserted
    assert "tags" not in inserted
    assert "status" not in inserted


async def test_prospect_read_and_patch_are_tenant_scoped_and_overlay_only(monkeypatch):
    account_a = ObjectId()
    account_b = ObjectId()
    prospect_id = ObjectId()
    canonical = {
        "_id": prospect_id,
        "full_name": "Shared Person",
        "notes": "legacy cross-tenant secret",
        "tags": ["legacy-private"],
        "status": "qualified",
    }

    prospect_collection = SimpleNamespace(
        find_one=AsyncMock(side_effect=lambda *_args, **_kwargs: dict(canonical)),
        update_one=AsyncMock(),
    )

    async def state_find_one(query, projection=None):
        if not _filter_contains_account(query, account_a):
            return None
        if projection == {"_id": 1}:
            return {"_id": ObjectId()}
        return {
            "_id": ObjectId(),
            "account_id": str(account_a),
            "prospect_id": str(prospect_id),
            "notes": "account A note",
            "tags": ["account-a"],
            "status": "contacted",
        }

    state_collection = SimpleNamespace(
        find_one=state_find_one,
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(prospects, "prospects_collection", prospect_collection)
    monkeypatch.setattr(prospects, "prospect_state_collection", state_collection)
    monkeypatch.setattr(
        prospects,
        "campaign_enrollments_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )
    # The no-campaign score fallback reads database_module.db directly; stub it
    # so this stays a pure unit test with no live Mongo.
    monkeypatch.setattr(
        prospects.database_module,
        "db",
        {"campaign_prospect_state": SimpleNamespace(find_one=AsyncMock(return_value=None))},
    )

    visible = await prospects.get_prospect(
        str(prospect_id), campaign_id=None, account_ctx=_ctx(account_a)
    )
    assert visible["notes"] == "account A note"
    assert visible["tags"] == ["account-a"]
    assert visible["status"] == "contacted"

    # Account B has no overlay and no enrollment, so it gets the READ-ONLY pool
    # view rather than a 404 — the same canonical fields the company prospect
    # list already exposes without a tenancy check.
    pool_view = await prospects.get_prospect(
        str(prospect_id), campaign_id=None, account_ctx=_ctx(account_b)
    )
    assert pool_view["access"] == "pool"
    assert pool_view["full_name"] == "Shared Person"

    # The whole point of the pool view: it must carry NOTHING tenant-scoped.
    # Neither account A's overlay, nor the legacy private fields still sitting
    # on the shared document, may leak through.
    assert pool_view.get("notes") is None
    assert pool_view.get("tags") in (None, [])
    assert pool_view["status"] == "new"
    assert "legacy cross-tenant secret" not in str(pool_view)
    assert "legacy-private" not in str(pool_view)
    assert "account A note" not in str(pool_view)
    assert "account-a" not in str(pool_view)

    # Account A, which owns the overlay, is still marked as such.
    assert visible["access"] == "workspace"

    await prospects.update_prospect(
        str(prospect_id),
        {"notes": "updated A note", "tags": ["private-a"]},
        account_ctx=_ctx(account_a),
    )
    update_filter = state_collection.update_one.await_args.args[0]
    update_doc = state_collection.update_one.await_args.args[1]
    assert update_filter == {
        "account_id": str(account_a),
        "prospect_id": str(prospect_id),
    }
    assert update_doc["$set"]["notes"] == "updated A note"
    assert update_doc["$set"]["tags"] == ["private-a"]
    prospect_collection.update_one.assert_not_awaited()

    state_collection.update_one.reset_mock()
    with pytest.raises(HTTPException) as forbidden_patch:
        await prospects.update_prospect(
            str(prospect_id),
            {"notes": "account B overwrite"},
            account_ctx=_ctx(account_b),
        )
    assert forbidden_patch.value.status_code == 404
    state_collection.update_one.assert_not_awaited()


async def test_bulk_and_single_enrollment_require_prospect_overlay_access(monkeypatch):
    account_a = ObjectId()
    account_b = ObjectId()
    campaign_id = ObjectId()
    prospect_id = ObjectId()

    async def campaign_find_one(query):
        if _filter_contains_account(query, account_b):
            return {"_id": campaign_id, "account_id": str(account_b), "status": "draft"}
        return None

    def state_find(query, _projection):
        rows = (
            [{"prospect_id": str(prospect_id)}]
            if _filter_contains_account(query, account_a)
            else []
        )
        return _Cursor(rows)

    enrollment_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        insert_one=AsyncMock(),
    )
    monkeypatch.setattr(
        campaign_enrollments,
        "campaigns_collection",
        SimpleNamespace(find_one=campaign_find_one, update_one=AsyncMock()),
    )
    monkeypatch.setattr(
        campaign_enrollments,
        "prospect_state_collection",
        SimpleNamespace(find=state_find),
    )
    monkeypatch.setattr(
        campaign_enrollments,
        "prospects_collection",
        SimpleNamespace(find=lambda *_args, **_kwargs: _Cursor([{"_id": prospect_id}])),
    )
    monkeypatch.setattr(
        campaign_enrollments, "campaign_enrollments_collection", enrollment_collection
    )

    with pytest.raises(HTTPException) as bulk_denied:
        await campaign_enrollments.bulk_enroll(
            str(campaign_id),
            {"prospect_ids": [str(prospect_id)]},
            BackgroundTasks(),
            account_ctx=_ctx(account_b),
        )
    assert bulk_denied.value.status_code == 404

    with pytest.raises(HTTPException) as single_denied:
        await campaign_enrollments.add_single_prospect(
            str(campaign_id),
            {"prospect_id": str(prospect_id)},
            BackgroundTasks(),
            account_ctx=_ctx(account_b),
        )
    assert single_denied.value.status_code == 404
    enrollment_collection.insert_one.assert_not_awaited()


async def test_cross_tenant_enrollment_read_and_delete_fail_closed(monkeypatch):
    account_a = ObjectId()
    account_b = ObjectId()
    campaign_id = ObjectId()
    enrollment_id = ObjectId()

    async def campaign_find_one(query):
        if _filter_contains_account(query, account_b):
            return {"_id": campaign_id, "account_id": str(account_b)}
        return None

    async def enrollment_find_one(query, *_args, **_kwargs):
        if query.get("_id") == enrollment_id and _filter_contains_account(
            query, account_a
        ):
            return {
                "_id": enrollment_id,
                "campaign_id": campaign_id,
                "account_id": str(account_a),
            }
        return None

    enrollment_collection = SimpleNamespace(
        find_one=enrollment_find_one,
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(
        campaign_enrollments,
        "campaigns_collection",
        SimpleNamespace(find_one=campaign_find_one, update_one=AsyncMock()),
    )
    monkeypatch.setattr(
        campaign_enrollments, "campaign_enrollments_collection", enrollment_collection
    )

    with pytest.raises(HTTPException) as denied_read:
        await campaign_enrollments.get_enrollment(
            str(campaign_id), str(enrollment_id), account_ctx=_ctx(account_b)
        )
    assert denied_read.value.status_code == 404

    with pytest.raises(HTTPException) as denied_delete:
        await campaign_enrollments.unenroll(
            str(campaign_id), str(enrollment_id), account_ctx=_ctx(account_b)
        )
    assert denied_delete.value.status_code == 404
    enrollment_collection.update_one.assert_not_awaited()


async def test_enrollment_badges_are_scoped_to_requesting_account(monkeypatch):
    account_id = ObjectId()
    campaign_id = ObjectId()
    prospect_id = ObjectId()
    distinct = AsyncMock(return_value=[prospect_id])
    monkeypatch.setattr(
        prospects,
        "campaign_enrollments_collection",
        SimpleNamespace(distinct=distinct),
    )

    docs = await prospects._mark_enrolled(
        [{"_id": prospect_id}], str(campaign_id), str(account_id)
    )

    query = distinct.await_args.args[1]
    assert str(account_id) in query["account_id"]["$in"]
    assert account_id in query["account_id"]["$in"]
    assert docs[0]["already_enrolled"] is True
