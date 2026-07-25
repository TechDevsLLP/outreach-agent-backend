"""Unit tests for the sequence endpoints in routes/campaigns.py.

The shared ASGI/api harness has known event-loop issues, so these exercise the
endpoint coroutines directly with the campaign lookup + DB write stubbed. They
cover the contract: default-template fallback, validation (400), the
active/completed edit lock (409), and successful persistence.
"""
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from routes import campaigns as campaigns_route
from services import sequence_service as seq

pytestmark = pytest.mark.unit

# A valid 24-hex account id so ObjectId(...) succeeds in the handler.
_ACCT = "0123456789abcdef01234567"
_CTX = {"account": {"_id": _ACCT}, "user": {"_id": "u1"}}


@pytest.fixture
def stub_campaign(monkeypatch):
    """Patch _get_campaign_or_404 to return a configurable campaign dict."""
    holder = {"campaign": {"_id": "c1", "status": "draft"}}

    async def _fake_get(campaign_id, account_id):
        return holder["campaign"]

    monkeypatch.setattr(campaigns_route, "_get_campaign_or_404", _fake_get)
    return holder


async def test_get_sequence_returns_default_when_none_stored(stub_campaign):
    stub_campaign["campaign"] = {"_id": "c1", "status": "draft"}
    resp = await campaigns_route.get_campaign_sequence("c1", account_ctx=_CTX)
    assert resp["is_default"] is True
    # The default template is itself valid.
    assert seq.validate_sequence_graph(resp["sequence_graph"]) == []


async def test_get_sequence_returns_stored_graph(stub_campaign):
    graph = seq.build_default_sequence_graph()
    stub_campaign["campaign"] = {"_id": "c1", "status": "draft", "sequence_graph": graph}
    resp = await campaigns_route.get_campaign_sequence("c1", account_ctx=_CTX)
    assert resp["is_default"] is False
    assert resp["sequence_graph"] == graph


async def test_put_sequence_persists_valid_graph(stub_campaign, monkeypatch):
    update_mock = AsyncMock()
    monkeypatch.setattr(campaigns_route.campaigns_collection, "update_one", update_mock)

    graph = seq.build_default_sequence_graph()
    body = campaigns_route.SequencePutRequest(sequence_graph=graph)
    resp = await campaigns_route.put_campaign_sequence("c1", body, account_ctx=_CTX)

    assert resp["is_default"] is False
    assert resp["sequence_graph"] == graph
    update_mock.assert_awaited_once()
    # The persisted $set carries the graph.
    _filter, update = update_mock.await_args.args
    assert update["$set"]["sequence_graph"] == graph


async def test_put_sequence_rejects_invalid_graph_with_400(stub_campaign):
    bad = {"nodes": [{"id": "a", "channel": "carrier_pigeon"}], "edges": []}
    body = campaigns_route.SequencePutRequest(sequence_graph=bad)
    with pytest.raises(HTTPException) as exc:
        await campaigns_route.put_campaign_sequence("c1", body, account_ctx=_CTX)
    assert exc.value.status_code == 400
    assert "errors" in exc.value.detail
    assert exc.value.detail["errors"]  # non-empty list


@pytest.mark.parametrize("status", ["active", "completed"])
async def test_put_sequence_locked_when_active_or_completed(stub_campaign, status):
    stub_campaign["campaign"] = {"_id": "c1", "status": status}
    body = campaigns_route.SequencePutRequest(sequence_graph=seq.build_default_sequence_graph())
    with pytest.raises(HTTPException) as exc:
        await campaigns_route.put_campaign_sequence("c1", body, account_ctx=_CTX)
    assert exc.value.status_code == 409
