"""Regression coverage for mixed ObjectId/string membership rows."""

from types import SimpleNamespace

import pytest
from bson import ObjectId

import auth

pytestmark = pytest.mark.unit


class _Collection:
    def __init__(self, doc):
        self.doc = doc
        self.last_query = None

    async def find_one(self, query, *args, **kwargs):
        self.last_query = query
        return dict(self.doc) if self.doc is not None else None

    async def update_one(self, *args, **kwargs):
        return SimpleNamespace(matched_count=1)


def test_id_representations_supports_canonical_and_legacy_memberships():
    oid = ObjectId()
    assert auth.id_representations(oid) == [str(oid), oid]
    assert auth.id_representations(str(oid)) == [str(oid), oid]
    assert auth.id_representations("not-an-object-id") == ["not-an-object-id"]


async def test_account_context_accepts_object_id_membership(monkeypatch):
    user_oid = ObjectId()
    account_oid = ObjectId()
    accounts = _Collection(
        {
            "_id": account_oid,
            "name": "Tenant A",
            "status": "active",
            "plan": "starter",
        }
    )
    members = _Collection(
        {"_id": ObjectId(), "account_id": account_oid, "user_id": user_oid}
    )
    users = _Collection({"_id": user_oid})
    fake_database = SimpleNamespace(
        accounts_collection=accounts,
        account_members_collection=members,
        users_collection=users,
    )
    monkeypatch.setitem(__import__("sys").modules, "database", fake_database)

    result = await auth.get_account_context(
        {
            "_id": str(user_oid),
            "email": "owner@example.com",
            "current_account_id": str(account_oid),
        }
    )

    assert result["account"]["_id"] == str(account_oid)
    assert account_oid in accounts.last_query["_id"]["$in"]
    assert account_oid in members.last_query["account_id"]["$in"]
    assert user_oid in members.last_query["user_id"]["$in"]
