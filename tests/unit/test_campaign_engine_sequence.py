"""Unit tests for the branching-sequence execution path in
services/campaign_engine.py — the message adapter, the acceptance signal check,
and (critically) the routing guard that keeps legacy campaigns on their existing
code path.
"""
from datetime import datetime, timedelta

import pytest

from services import campaign_engine as engine

pytestmark = pytest.mark.unit


# ─── Message adapter (_seq_extract_subject_body) ────────────────────────────

def test_extract_prefers_per_node_message():
    node_msg = {"channel": "email", "subject": "Hi", "body": "Body text"}
    subject, body = engine._seq_extract_subject_body("email", node_msg, None)
    assert subject == "Hi"
    assert body == "Body text"


def test_extract_email_from_legacy_cold_email():
    classic = {"cold_email": {"subject_a": "S", "body": "B"}}
    subject, body = engine._seq_extract_subject_body("email", None, classic)
    assert subject == "S" and body == "B"


def test_extract_connection_note_from_legacy():
    classic = {"linkedin_connection": {"note": "Nice to connect"}}
    subject, body = engine._seq_extract_subject_body("linkedin_connection", None, classic)
    assert subject == "" and body == "Nice to connect"


def test_extract_inmail_from_legacy():
    classic = {"linkedin_inmail": {"subject": "IM", "body": "IB"}}
    subject, body = engine._seq_extract_subject_body("linkedin_inmail", None, classic)
    assert subject == "IM" and body == "IB"


def test_extract_returns_empty_when_no_message():
    assert engine._seq_extract_subject_body("email", None, None) == ("", "")


# ─── Acceptance detection (_seq_detect_accepted) ────────────────────────────

def test_detect_accepted_false_without_timestamp():
    assert engine._seq_detect_accepted({}, {"sequence_state": {}}) is False


def test_detect_accepted_ignores_legacy_shared_prospect_timestamp():
    assert engine._seq_detect_accepted(
        {"connection_accepted_at": datetime(2026, 7, 15, 10, 0, 0)},
        {"sequence_state": {}},
    ) is False


def test_detect_accepted_true_when_accepted_after_last_send():
    last_sent = datetime(2026, 7, 15, 10, 0, 0)
    enr = {
        "sequence_state": {"last_sent_at": last_sent},
        "linkedin_activity": {"connection_accepted_at": last_sent + timedelta(hours=2)},
    }
    assert engine._seq_detect_accepted({}, enr) is True


def test_detect_accepted_true_when_no_last_sent_recorded():
    enrollment = {
        "sequence_state": {},
        "linkedin_activity": {"connection_accepted_at": datetime(2026, 7, 15, 10, 0, 0)},
    }
    assert engine._seq_detect_accepted({}, enrollment) is True


# ─── Routing guard: legacy campaigns must not enter the sequence path ────────

async def test_routes_to_sequence_when_graph_and_state_present(monkeypatch):
    calls = {"sequence": 0, "smart": 0}

    async def _fake_seq(enr, camp, prospect):
        calls["sequence"] += 1

    async def _fake_smart(enr, camp, prospect, channel):
        calls["smart"] += 1

    monkeypatch.setattr(engine, "_execute_sequence_enrollment", _fake_seq)
    monkeypatch.setattr(engine, "_execute_smart_enrollment", _fake_smart)

    campaign = {"_id": "c1", "sequence_graph": {"nodes": [], "edges": []}}
    enrollment = {"_id": "e1", "sequence_state": {"current_node_id": "n1"}}
    await engine.execute_enrollment(enrollment, campaign, {"_id": "p1"})

    assert calls == {"sequence": 1, "smart": 0}


async def test_legacy_smart_campaign_never_enters_sequence_path(monkeypatch):
    calls = {"sequence": 0, "smart": 0}

    async def _fake_seq(enr, camp, prospect):
        calls["sequence"] += 1

    async def _fake_smart(enr, camp, prospect, channel):
        calls["smart"] += 1

    monkeypatch.setattr(engine, "_execute_sequence_enrollment", _fake_seq)
    monkeypatch.setattr(engine, "_execute_smart_enrollment", _fake_smart)

    # No sequence_graph → classic smart campaign; must use the smart path.
    campaign = {"_id": "c1"}
    enrollment = {
        "_id": "e1",
        "smart_campaign_channel": "email",
        "generated_messages": {"cold_email": {"subject_a": "s", "body": "b"}},
    }
    await engine.execute_enrollment(enrollment, campaign, {"_id": "p1"})

    assert calls == {"sequence": 0, "smart": 1}


async def test_enrollment_without_sequence_state_does_not_enter_sequence_path(monkeypatch):
    # A campaign carrying a sequence_graph but an enrollment that predates it
    # (no sequence_state) must fall through to the legacy path, never the DAG.
    calls = {"sequence": 0, "smart": 0}

    async def _fake_seq(enr, camp, prospect):
        calls["sequence"] += 1

    async def _fake_smart(enr, camp, prospect, channel):
        calls["smart"] += 1

    monkeypatch.setattr(engine, "_execute_sequence_enrollment", _fake_seq)
    monkeypatch.setattr(engine, "_execute_smart_enrollment", _fake_smart)

    campaign = {"_id": "c1", "sequence_graph": {"nodes": [], "edges": []}}
    enrollment = {
        "_id": "e1",
        "smart_campaign_channel": "email",
        "generated_messages": {"cold_email": {"subject_a": "s", "body": "b"}},
        # no sequence_state
    }
    await engine.execute_enrollment(enrollment, campaign, {"_id": "p1"})

    assert calls == {"sequence": 0, "smart": 1}
