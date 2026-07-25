"""Mongo-backed integration coverage for prospect filter-key fanout."""

from datetime import datetime

import pytest
from bson import ObjectId

import database
from utils.prospect_filter_keys import (
    fetch_filter_keys,
    resync_filter_keys_from_db,
    sync_filter_keys,
)

pytestmark = pytest.mark.api


async def test_fetch_filter_keys_and_sync_and_resync(identity_a, identity_b):
    now = datetime.utcnow()
    ins = await database.prospects_collection.insert_one({
        "full_name": "PK Unit Target", "email": "pk-unit@t.test",
        "company_name": "PKCo", "job_title": "Ops",
        "company_industry_id": "logistics",
        "location": {"country_code": "FR", "country": "France"},
        "enrichment_status": "not_started", "linkedin": "https://linkedin.com/in/pk-unit",
        "source": "pk_unit_seed", "created_at": now,
    })
    pid = str(ins.inserted_id)
    try:
        pk = await fetch_filter_keys(pid)
        assert pk["email"] == "pk-unit@t.test" and pk["country_code"] == "FR"
        assert await fetch_filter_keys("not-an-oid") is None
        assert await fetch_filter_keys(ObjectId()) is None

        await database.prospect_state_collection.insert_many([
            {"account_id": identity_a["account_id"], "prospect_id": pid,
             "status": "new", "pk": pk, "used_by": [], "tags": [], "created_at": now},
            {"account_id": identity_b["account_id"], "prospect_id": pid,
             "status": "new", "pk": pk, "used_by": [], "tags": [], "created_at": now},
        ])
        await sync_filter_keys([pid], {"enrichment_status": "completed"})
        rows = await database.prospect_state_collection.find({"prospect_id": pid}).to_list(10)
        assert len(rows) == 2
        assert all(r["pk"]["enrichment_status"] == "completed" for r in rows)

        await database.prospects_collection.update_one(
            {"_id": ins.inserted_id}, {"$set": {"email": "changed@t.test"}})
        assert await resync_filter_keys_from_db([pid]) == 2
        rows = await database.prospect_state_collection.find({"prospect_id": pid}).to_list(10)
        assert all(r["pk"]["email"] == "changed@t.test" for r in rows)
        assert all(r["pk"]["enrichment_status"] == "not_started" for r in rows)
    finally:
        await database.prospect_state_collection.delete_many({"prospect_id": pid})
        await database.prospects_collection.delete_many({"source": "pk_unit_seed"})


async def test_ensure_prospect_state_accepts_none_db_and_sets_pk(identity_a):
    from services.prospect_search_service import ensure_prospect_state

    now = datetime.utcnow()
    ins = await database.prospects_collection.insert_one({
        "full_name": "Ensure None DB", "email": "ensure-none@t.test",
        "company_name": "EnsureCo", "enrichment_status": "in_progress",
        "source": "pk_unit_seed", "created_at": now,
    })
    pid = str(ins.inserted_id)
    try:
        state = await ensure_prospect_state(
            None, account_id=identity_a["account_id"], prospect_id=pid
        )
        assert state["prospect_id"] == pid
        row = await database.prospect_state_collection.find_one(
            {"account_id": identity_a["account_id"], "prospect_id": pid}
        )
        assert row is not None
        assert row["pk"]["email"] == "ensure-none@t.test"
        assert row["pk"]["enrichment_status"] == "in_progress"
    finally:
        await database.prospect_state_collection.delete_many({"prospect_id": pid})
        await database.prospects_collection.delete_many({"source": "pk_unit_seed"})
