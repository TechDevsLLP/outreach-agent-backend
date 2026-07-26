"""Unit tests for prospect reuse across dead/inactive campaigns.

build_exclusion_set must:
  - RELEASE never-sent prospects whose only owning campaign is paused,
    archived, completed, failed, or hard-deleted (campaign_id no longer maps
    to an occupying campaign).
  - KEEP excluding prospects owned by a still-occupying campaign
    (active / draft / awaiting_approval).
  - KEEP excluding any prospect actually messaged (messages_sent > 0) within
    cooldown, regardless of the owning campaign's status (send-guard).

These run against a minimal in-memory fake of the Motor `db[...]` collections
(no live Mongo needed), emulating only the query operators the function uses.
"""
from datetime import datetime, timedelta

import pytest

from services.prospect_search_service import build_exclusion_set

ACC = "acct-1"
USER = "user-1"


def _now():
    # Naive UTC to mirror how Motor returns BSON dates (and how the function's
    # own `cutoff` is computed via datetime.utcnow()); real server-side Mongo
    # comparison is tz-normalized, but our in-Python fake compares directly.
    return datetime.utcnow()


# ---------------------------------------------------------------------------
# Minimal async Mongo fake — supports eq, $in, $gt, $gte, $elemMatch, top-$or
# ---------------------------------------------------------------------------
def _match_cond(value, cond):
    if isinstance(cond, dict) and any(k.startswith("$") for k in cond):
        for op, operand in cond.items():
            if op == "$in":
                if value not in operand:
                    return False
            elif op == "$gt":
                if not (value is not None and value > operand):
                    return False
            elif op == "$gte":
                if not (value is not None and value >= operand):
                    return False
            elif op == "$elemMatch":
                if not isinstance(value, list):
                    return False
                if not any(_match_doc(el, operand) for el in value):
                    return False
            else:
                raise AssertionError(f"fake db: unsupported op {op}")
        return True
    return value == cond


def _get(doc, dotted):
    cur = doc
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _match_doc(doc, flt):
    for key, cond in flt.items():
        if key == "$or":
            if not any(_match_doc(doc, sub) for sub in cond):
                return False
        else:
            if not _match_cond(_get(doc, key), cond):
                return False
    return True


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, flt, projection=None):
        return _FakeCursor([d for d in self._docs if _match_doc(d, flt)])


class _FakeDB:
    def __init__(self, collections):
        self._c = {name: _FakeCollection(docs) for name, docs in collections.items()}

    def __getitem__(self, name):
        return self._c.get(name) or _FakeCollection([])


def _used_by(campaign_id, status="scoring", completed_at=None):
    entry = {"user_id": USER, "campaign_id": campaign_id, "status": status}
    if completed_at:
        entry["completed_at"] = completed_at
    return entry


def _make_db(*, campaigns, prospect_state, enrollments=None):
    return _FakeDB({
        "campaigns": campaigns,
        "prospect_state": prospect_state,
        "campaign_enrollments": enrollments or [],
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_paused_never_sent_campaign_releases_prospect():
    db = _make_db(
        campaigns=[{"_id": "camp-paused", "account_id": ACC, "status": "paused"}],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-paused", status="scoring")]},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "p1" not in excluded  # released for reuse


@pytest.mark.asyncio
async def test_active_campaign_still_owns_prospect():
    db = _make_db(
        campaigns=[{"_id": "camp-active", "account_id": ACC, "status": "active"}],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-active", status="active")]},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "p1" in excluded


@pytest.mark.asyncio
async def test_draft_and_awaiting_approval_occupy():
    db = _make_db(
        campaigns=[
            {"_id": "camp-draft", "account_id": ACC, "status": "draft"},
            {"_id": "camp-await", "account_id": ACC, "status": "awaiting_approval"},
        ],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "pd", "status": "new",
             "used_by": [_used_by("camp-draft")]},
            {"account_id": ACC, "prospect_id": "pa", "status": "new",
             "used_by": [_used_by("camp-await")]},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert {"pd", "pa"} <= excluded


@pytest.mark.asyncio
async def test_archived_never_sent_campaign_releases_prospect():
    # Archiving a dead, never-sent campaign instantly frees its prospects.
    db = _make_db(
        campaigns=[{"_id": "camp-arch", "account_id": ACC, "status": "archived"}],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-arch", status="active")]},
        ],
        enrollments=[
            {"account_id": ACC, "prospect_id": "p1", "campaign_id": "camp-arch",
             "status": "paused", "messages_sent": 0},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "p1" not in excluded  # archived + never sent → reusable


@pytest.mark.asyncio
async def test_hard_deleted_campaign_releases_prospect():
    # No campaign doc exists for the id referenced in used_by (hard-deleted).
    db = _make_db(
        campaigns=[],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-gone", status="scoring")]},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "p1" not in excluded


@pytest.mark.asyncio
async def test_send_guard_protects_messaged_prospect_even_if_campaign_archived():
    db = _make_db(
        campaigns=[{"_id": "camp-arch", "account_id": ACC, "status": "archived"}],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-arch", status="active")]},
        ],
        enrollments=[
            {"account_id": ACC, "prospect_id": "p1", "campaign_id": "camp-arch",
             "status": "paused", "messages_sent": 2, "last_sent_at": _now() - timedelta(days=3)},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "p1" in excluded  # already contacted → protected by send-guard


@pytest.mark.asyncio
async def test_old_send_beyond_cooldown_releases_prospect():
    db = _make_db(
        campaigns=[{"_id": "camp-arch", "account_id": ACC, "status": "archived"}],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "p1", "status": "new",
             "used_by": [_used_by("camp-arch", status="active")]},
        ],
        enrollments=[
            {"account_id": ACC, "prospect_id": "p1", "campaign_id": "camp-arch",
             "status": "completed", "messages_sent": 3, "last_sent_at": _now() - timedelta(days=200)},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER, cooldown_days=90)
    assert "p1" not in excluded  # last send > cooldown → re-contactable


@pytest.mark.asyncio
async def test_hard_exclude_status_always_excluded():
    db = _make_db(
        campaigns=[],
        prospect_state=[
            {"account_id": ACC, "prospect_id": "pu", "status": "unsubscribed", "used_by": []},
        ],
    )
    excluded = await build_exclusion_set(db, account_id=ACC, user_id=USER)
    assert "pu" in excluded
