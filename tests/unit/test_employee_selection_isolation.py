"""Offline contracts for tenant-safe employee selection and lazy Apify use."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from services import company_scraper_service, employee_selection_service


pytestmark = pytest.mark.unit


async def test_new_prospect_is_canonical_and_private_state_is_overlay_only(monkeypatch):
    account_id = ObjectId()
    employee_id = ObjectId()
    prospect_id = ObjectId()
    employee = {
        "_id": employee_id,
        "account_id": account_id,
        "full_name": "Ada Lovelace",
        "linkedin_url": "https://linkedin.com/in/ada",
        "location": "London, United Kingdom",
        "current_position": {"title": "Founder"},
    }

    employee_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=dict(employee)),
        update_one=AsyncMock(),
    )
    prospect_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        insert_one=AsyncMock(return_value=SimpleNamespace(inserted_id=prospect_id)),
    )
    state_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(
        employee_selection_service, "employees_collection", employee_collection
    )
    monkeypatch.setattr(
        employee_selection_service, "prospects_collection", prospect_collection
    )
    monkeypatch.setattr(
        employee_selection_service, "prospect_state_collection", state_collection
    )
    monkeypatch.setattr(
        employee_selection_service,
        "companies_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=None)),
    )

    result = await employee_selection_service.select_and_enrich_employees(
        [str(employee_id)],
        auto_enrich=False,
        skip_email_finding=True,
        industry_id="software",
        tags=["company_detail"],
        account_id=str(account_id),
    )

    canonical = prospect_collection.insert_one.await_args.args[0]
    for private_field in (
        "account_id",
        "status",
        "tags",
        "prospect_score",
        "score_breakdown",
        "industry_id",
    ):
        assert private_field not in canonical
    assert canonical["location"] == {
        "raw": "London, United Kingdom",
        "city": "London",
        "country": "United Kingdom",
    }

    overlay_filter = state_collection.update_one.await_args.args[0]
    overlay_update = state_collection.update_one.await_args.args[1]
    assert overlay_filter == {
        "account_id": str(account_id),
        "prospect_id": str(prospect_id),
    }
    assert overlay_update["$setOnInsert"]["status"] == "new"
    assert overlay_update["$addToSet"]["tags"]["$each"] == ["company_detail"]
    assert overlay_update["$set"]["source_industry_ids"] == ["software"]
    assert "ai_score" in overlay_update["$set"]
    assert result["summary"]["created"] == 1


async def test_other_tenant_employee_id_cannot_trigger_lookup_or_mutation(monkeypatch):
    account_a = ObjectId()
    account_b = ObjectId()
    employee_id = ObjectId()
    employee_collection = SimpleNamespace(
        find_one=AsyncMock(
            return_value={
                "_id": employee_id,
                "account_id": account_a,
                "full_name": "Private Staging Employee",
                "linkedin_url": "https://linkedin.com/in/private",
            }
        ),
        update_one=AsyncMock(),
    )
    prospect_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        insert_one=AsyncMock(),
    )
    state_collection = SimpleNamespace(
        find_one=AsyncMock(return_value=None),
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(
        employee_selection_service, "employees_collection", employee_collection
    )
    monkeypatch.setattr(
        employee_selection_service, "prospects_collection", prospect_collection
    )
    monkeypatch.setattr(
        employee_selection_service, "prospect_state_collection", state_collection
    )
    email_lookup = AsyncMock()
    monkeypatch.setattr(employee_selection_service, "find_emails", email_lookup)

    result = await employee_selection_service.select_and_enrich_employees(
        [str(employee_id)],
        auto_enrich=False,
        account_id=str(account_b),
    )

    assert result["not_found"] == [str(employee_id)]
    assert result["summary"]["created"] == 0
    prospect_collection.insert_one.assert_not_awaited()
    state_collection.update_one.assert_not_awaited()
    employee_collection.update_one.assert_not_awaited()
    email_lookup.assert_not_awaited()


async def test_existing_shared_prospect_requires_and_receives_tenant_overlay(
    monkeypatch,
):
    account_id = ObjectId()
    employee_id = ObjectId()
    prospect_id = ObjectId()
    existing = {
        "_id": prospect_id,
        "linkedin": "https://linkedin.com/in/shared",
        "full_name": "Shared Prospect",
        "location": {"country": "India"},
    }
    monkeypatch.setattr(
        employee_selection_service,
        "employees_collection",
        SimpleNamespace(
            find_one=AsyncMock(
                return_value={
                    "_id": employee_id,
                    "full_name": "Shared Prospect",
                    "linkedin_url": existing["linkedin"],
                }
            ),
            update_one=AsyncMock(),
        ),
    )
    monkeypatch.setattr(
        employee_selection_service,
        "prospects_collection",
        SimpleNamespace(find_one=AsyncMock(return_value=dict(existing))),
    )
    state_collection = SimpleNamespace(
        find_one=AsyncMock(return_value={"_id": ObjectId()}),
        update_one=AsyncMock(),
    )
    monkeypatch.setattr(
        employee_selection_service, "prospect_state_collection", state_collection
    )

    result = await employee_selection_service.select_and_enrich_employees(
        [str(employee_id)],
        auto_enrich=False,
        skip_email_finding=True,
        tags=["employee_detail"],
        account_id=str(account_id),
    )

    assert result["summary"]["duplicates"] == 1
    assert result["results"][0]["existing_prospect_id"] == str(prospect_id)
    overlay_filter = state_collection.update_one.await_args.args[0]
    assert overlay_filter["account_id"] == str(account_id)
    assert overlay_filter["prospect_id"] == str(prospect_id)


def test_company_apify_client_is_constructed_only_on_first_dataset_access(monkeypatch):
    constructed = []

    class _FakeClient:
        def __init__(self, api_key):
            constructed.append(api_key)

        def dataset(self, dataset_id):
            return {"dataset_id": dataset_id}

    monkeypatch.setattr(company_scraper_service, "ApifyClient", _FakeClient)
    lazy = company_scraper_service._LazyApifyClient("test-key")

    assert constructed == []
    assert lazy.dataset("dataset-1") == {"dataset_id": "dataset-1"}
    assert lazy.dataset("dataset-2") == {"dataset_id": "dataset-2"}
    assert constructed == ["test-key"]
