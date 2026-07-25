"""Unit tests for services/sequence_service.py — the branching multi-touch
sequence graph contract: validation, the default aggressive template, and the
``advance_sequence`` state machine.

These are pure-logic tests (no DB). ``advance_sequence`` clamps next_action_at to
the campaign send window, so the test campaign uses a 24/7 window to keep the
math predictable.
"""
from datetime import datetime, timedelta

import pytest

from services import sequence_service as seq

pytestmark = pytest.mark.unit


# A campaign whose send window is every day, all hours — so clamp_to_send_window
# is effectively the identity for any time strictly before hour 23.
def _campaign(graph: dict) -> dict:
    return {
        "_id": "camp1",
        "sequence_graph": graph,
        "send_days": ["monday", "tuesday", "wednesday", "thursday",
                      "friday", "saturday", "sunday"],
        "send_hour_start": 0,
        "send_hour_end": 23,
        "timezone": "UTC",
    }


def _enrollment(state: dict) -> dict:
    return {"_id": "enr1", "sequence_state": state}


# ─── Default template ───────────────────────────────────────────────────────

def test_default_template_is_valid():
    graph = seq.build_default_sequence_graph()
    assert seq.validate_sequence_graph(graph) == []


def test_default_template_start_node_is_connection():
    graph = seq.build_default_sequence_graph()
    start_id = seq.get_start_node_id(graph)
    start = seq.get_node(graph, start_id)
    assert start_id == "n1"
    assert start["channel"] == "linkedin_connection"


def test_default_template_is_single_linkedin_first_route():
    # New default = ONE LinkedIn-first route with 6 touches.
    graph = seq.build_default_sequence_graph()
    start_ids = seq.get_start_node_ids(graph)
    assert start_ids == ["n1"]
    assert len(graph["nodes"]) == 6
    assert graph["settings"]["max_touches"] == 6
    channels = [n["channel"] for n in graph["nodes"]]
    assert channels == [
        "linkedin_connection", "linkedin_message", "email", "email", "email", "email",
    ]


def test_default_template_has_accepted_and_not_accepted_branches():
    graph = seq.build_default_sequence_graph()
    n1_edges = seq.outgoing_edges(graph, "n1")
    conditions = {e["condition"] for e in n1_edges}
    assert conditions == {"accepted", "not_accepted"}


def test_default_template_accepted_edge_is_thirty_minutes():
    graph = seq.build_default_sequence_graph()
    accepted = next(e for e in graph["edges"] if e["condition"] == "accepted")
    assert accepted["delay_days"] == 0
    assert accepted["delay_minutes"] == 30


def test_default_template_no_reply_gaps_are_one_or_two_days():
    graph = seq.build_default_sequence_graph()
    no_reply = [e for e in graph["edges"] if e["condition"] == "no_reply"]
    assert no_reply and all(e["delay_days"] in (1, 2) for e in no_reply)


def test_default_template_nodes_carry_guidance():
    graph = seq.build_default_sequence_graph()
    assert all((n.get("guidance") or "").strip() for n in graph["nodes"])
    assert all((n.get("label") or "").strip() for n in graph["nodes"])


# The legacy 3-route hybrid shape (still fully supported by the contract and
# the multi-route planner) — used by the multi-route planning tests below.
def _hybrid_graph() -> dict:
    nodes = [
        {"id": "h1", "type": "touch", "channel": "linkedin_connection", "message_intent": "intro"},
        {"id": "h2", "type": "touch", "channel": "linkedin_message", "message_intent": "intro"},
        {"id": "h3", "type": "touch", "channel": "email", "message_intent": "intro"},
        {"id": "h4", "type": "touch", "channel": "email", "message_intent": "breakup"},
        {"id": "h5", "type": "touch", "channel": "inmail", "message_intent": "intro"},
    ]
    edges = [
        {"id": "e1", "from": "h1", "to": "h2", "condition": "accepted", "delay_days": 2},
        {"id": "e2", "from": "h3", "to": "h4", "condition": "no_reply", "delay_days": 2},
    ]
    return {"version": 1, "nodes": nodes, "edges": edges,
            "settings": {"max_touches": 6, "stop_on_reply": True, "default_gap_days": 2}}


# ─── Validation ─────────────────────────────────────────────────────────────

def test_valid_minimal_graph():
    graph = {
        "version": 1,
        "nodes": [
            {"id": "a", "channel": "email", "message_intent": "intro"},
            {"id": "b", "channel": "email", "message_intent": "breakup"},
        ],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply", "delay_days": 2}],
        "settings": {"max_touches": 8},
    }
    assert seq.validate_sequence_graph(graph) == []


def test_cycle_is_rejected():
    graph = {
        "nodes": [
            {"id": "a", "channel": "email"},
            {"id": "b", "channel": "email"},
        ],
        "edges": [
            {"id": "e1", "from": "a", "to": "b", "condition": "no_reply", "delay_days": 1},
            {"id": "e2", "from": "b", "to": "a", "condition": "no_reply", "delay_days": 1},
        ],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("cycle" in e.lower() for e in errors)


def test_multiple_start_nodes_are_allowed():
    # Several nodes with no incoming edges → several routes. This is valid now
    # (hybrid campaigns have LinkedIn-first / Email-first / InMail routes).
    graph = {
        "version": 1,
        "nodes": [
            {"id": "a", "channel": "linkedin_connection", "message_intent": "intro"},
            {"id": "b", "channel": "email", "message_intent": "intro"},
            {"id": "c", "channel": "inmail", "message_intent": "intro"},
        ],
        "edges": [
            {"id": "e1", "from": "a", "to": "c", "condition": "no_reply", "delay_days": 1},
        ],
        "settings": {"max_touches": 8},
    }
    errors = seq.validate_sequence_graph(graph)
    assert errors == []
    assert set(seq.get_start_node_ids(graph)) == {"a", "b"}


def test_no_start_node_when_all_have_incoming():
    # A 2-cycle means every node has an incoming edge → no start node.
    graph = {
        "nodes": [
            {"id": "a", "channel": "email"},
            {"id": "b", "channel": "email"},
        ],
        "edges": [
            {"id": "e1", "from": "a", "to": "b", "condition": "no_reply", "delay_days": 1},
            {"id": "e2", "from": "b", "to": "a", "condition": "no_reply", "delay_days": 1},
        ],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("start node" in e.lower() for e in errors)


def test_max_touches_cap_exceeded_is_rejected():
    # A linear chain of 4 nodes with max_touches=3 → longest path 4 > 3.
    nodes = [{"id": f"n{i}", "channel": "email"} for i in range(4)]
    edges = [
        {"id": f"e{i}", "from": f"n{i}", "to": f"n{i+1}", "condition": "no_reply", "delay_days": 1}
        for i in range(3)
    ]
    graph = {"nodes": nodes, "edges": edges, "settings": {"max_touches": 3}}
    errors = seq.validate_sequence_graph(graph)
    assert any("max_touches" in e for e in errors)


def test_invalid_channel_is_rejected():
    graph = {
        "nodes": [{"id": "a", "channel": "carrier_pigeon"}],
        "edges": [],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("invalid channel" in e for e in errors)


def test_invalid_condition_is_rejected():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "maybe", "delay_days": 1}],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("invalid condition" in e for e in errors)


def test_negative_delay_is_rejected():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply", "delay_days": -1}],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("delay_days" in e for e in errors)


def test_edge_to_unknown_node_is_rejected():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "ghost", "condition": "no_reply", "delay_days": 1}],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("unknown node 'ghost'" in e for e in errors)


# ─── advance_sequence: sent ─────────────────────────────────────────────────

def test_sent_moves_to_awaiting_and_schedules_timeout():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    enr = _enrollment(state)
    now = datetime(2026, 7, 15, 10, 0, 0)

    new_state = seq.advance_sequence(enr, campaign, "sent", now=now)

    assert new_state["phase"] == "awaiting"
    assert new_state["touches_done"] == 1
    assert new_state["current_node_id"] == "n1"  # unchanged until timeout resolves
    # n1's timeout branch is not_accepted (1 day).
    assert new_state["next_action_at"] == now + timedelta(days=1)


def test_sent_increments_touches_and_caps_at_max_touches():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    # touches_done already at max-1 → this send hits the cap.
    max_touches = graph["settings"]["max_touches"]
    state = seq.build_initial_sequence_state(graph)
    state["touches_done"] = max_touches - 1
    enr = _enrollment(state)
    new_state = seq.advance_sequence(enr, campaign, "sent")
    assert new_state["touches_done"] == max_touches
    assert new_state["stopped_reason"] == "max_touches"
    assert new_state["next_action_at"] is None


# ─── advance_sequence: accepted / not_accepted ──────────────────────────────

def test_accepted_follows_accepted_edge_with_its_delay():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["phase"] = "awaiting"
    state["touches_done"] = 1
    enr = _enrollment(state)
    now = datetime(2026, 7, 15, 10, 0, 0)

    new_state = seq.advance_sequence(enr, campaign, "accepted", now=now)

    assert new_state["current_node_id"] == "n2"  # li_message followup
    assert new_state["phase"] == "pending_send"
    # Sub-day accepted-edge delay: delay_days=0 + delay_minutes=30.
    assert new_state["next_action_at"] == now + timedelta(minutes=30)


def test_no_reply_timeout_on_connection_takes_not_accepted_branch_immediately():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["phase"] = "awaiting"
    state["touches_done"] = 1
    enr = _enrollment(state)
    now = datetime(2026, 7, 15, 10, 0, 0)

    new_state = seq.advance_sequence(enr, campaign, "no_reply_timeout", now=now)

    # Connection not accepted → email path (n3), sent immediately (wait already served).
    assert new_state["current_node_id"] == "n3"
    assert new_state["phase"] == "pending_send"
    assert new_state["next_action_at"] == now


def test_no_reply_timeout_follows_no_reply_edge():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    # Sit on n2 (li_message intro) whose only edge is no_reply → n3.
    state = seq.build_initial_sequence_state(graph)
    state["current_node_id"] = "n2"
    state["phase"] = "awaiting"
    state["touches_done"] = 2
    enr = _enrollment(state)
    new_state = seq.advance_sequence(enr, campaign, "no_reply_timeout")
    assert new_state["current_node_id"] == "n3"
    assert new_state["phase"] == "pending_send"


# ─── advance_sequence: replied (stop-on-reply) ──────────────────────────────

def test_replied_stops_everything():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["current_node_id"] = "n4"
    state["phase"] = "awaiting"
    state["touches_done"] = 4
    enr = _enrollment(state)
    new_state = seq.advance_sequence(enr, campaign, "replied")
    assert new_state["stopped_reason"] == "replied"
    assert new_state["phase"] == "stopped"
    assert new_state["next_action_at"] is None


def test_reply_stops_even_mid_sequence_regardless_of_node():
    # stop_on_reply is always enforced — from any node a reply terminates.
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    for node_id in ("n1", "n3", "n6"):
        state = seq.build_initial_sequence_state(graph)
        state["current_node_id"] = node_id
        state["phase"] = "awaiting"
        new_state = seq.advance_sequence(_enrollment(state), campaign, "replied")
        assert new_state["stopped_reason"] == "replied"


def test_already_stopped_is_idempotent():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["stopped_reason"] = "replied"
    state["phase"] = "stopped"
    new_state = seq.advance_sequence(_enrollment(state), campaign, "no_reply_timeout")
    assert new_state["stopped_reason"] == "replied"


def test_terminal_breakup_node_ends_sequence_on_send():
    # n6 (email breakup) has no outgoing edges — sending it ends it.
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["current_node_id"] = "n6"
    state["touches_done"] = 3
    new_state = seq.advance_sequence(_enrollment(state), campaign, "sent")
    assert new_state["stopped_reason"] == "sequence_end"
    assert new_state["next_action_at"] is None


# ─── Full walk: accepted path ───────────────────────────────────────────────

def test_full_accepted_path_walk():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    enr = _enrollment(state)

    # n1 sent → awaiting
    state = seq.advance_sequence(enr, campaign, "sent")
    enr = _enrollment(state)
    assert state["current_node_id"] == "n1" and state["phase"] == "awaiting"

    # accepted → move to n2
    state = seq.advance_sequence(enr, campaign, "accepted")
    enr = _enrollment(state)
    assert state["current_node_id"] == "n2"

    # Walk the no_reply chain n2→n3→n4→n5→n6 (n6 is the terminal breakup).
    expected_chain = ["n3", "n4", "n5", "n6"]
    for expected in expected_chain:
        state = seq.advance_sequence(enr, campaign, "sent")   # deliver current node
        enr = _enrollment(state)
        assert state["phase"] == "awaiting"
        state = seq.advance_sequence(enr, campaign, "no_reply_timeout")
        enr = _enrollment(state)
        assert state["current_node_id"] == expected


# ─── engine_channel mapping ─────────────────────────────────────────────────

def test_engine_channel_maps_inmail():
    assert seq.engine_channel("inmail") == "linkedin_inmail"
    assert seq.engine_channel("email") == "email"
    assert seq.engine_channel("linkedin_connection") == "linkedin_connection"
    assert seq.engine_channel("linkedin_message") == "linkedin_message"


# ─── First-touch day planning ───────────────────────────────────────────────

def test_plan_first_touch_days_respects_cap_and_score_order():
    graph = seq.build_default_sequence_graph()
    campaign = {
        "daily_caps": {"linkedin_connection": 2, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20},
    }
    # 5 prospects all with LinkedIn; cap 2/day → 3 days.
    prospects_by_id = {}
    enrollments = []
    for i in range(5):
        pid = f"p{i}"
        prospects_by_id[pid] = {"_id": pid, "linkedin": f"https://linkedin.com/in/{i}"}
        enrollments.append({"_id": f"e{i}", "prospect_id": pid, "campaign_rule_score": float(i)})

    assignments, skip = seq.plan_first_touch_days(campaign, enrollments, prospects_by_id, "linkedin_connection")
    assert len(assignments) == 5
    # Top score first: e4 (score 4) should be day 1.
    by_enr = {e["_id"]: day for e, day in assignments}
    assert by_enr["e4"] == 1 and by_enr["e3"] == 1
    assert by_enr["e2"] == 2 and by_enr["e1"] == 2
    assert by_enr["e0"] == 3


def test_plan_first_touch_days_skips_prospects_without_channel():
    campaign = {"daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20}}
    prospects_by_id = {
        "p0": {"_id": "p0", "linkedin": "x"},          # ok
        "p1": {"_id": "p1", "email": "a@b.com"},        # no linkedin → skipped
    }
    enrollments = [
        {"_id": "e0", "prospect_id": "p0", "campaign_rule_score": 1.0},
        {"_id": "e1", "prospect_id": "p1", "campaign_rule_score": 2.0},
    ]
    assignments, skip = seq.plan_first_touch_days(campaign, enrollments, prospects_by_id, "linkedin_connection")
    assert len(assignments) == 1
    assert skip.get("no_start_channel") == 1


# ─── Multi-route planning (hybrid graphs still supported) ───────────────────

def test_resolve_routes_single_route_for_default_template():
    # The default template is single-route → exactly one schedulable route.
    graph = seq.build_default_sequence_graph()
    campaign = {"daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20}}
    routes = seq.resolve_routes(graph, campaign)
    assert len(routes) == 1
    assert routes[0]["channel"] == "linkedin_connection"
    assert routes[0]["node_id"] == "n1"
    assert routes[0]["cap"] == 20


def test_resolve_routes_yields_three_capped_routes():
    graph = _hybrid_graph()
    campaign = {"daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20}}
    routes = seq.resolve_routes(graph, campaign)
    by_channel = {r["channel"]: r for r in routes}
    assert set(by_channel) == {"linkedin_connection", "email", "linkedin_inmail"}
    assert by_channel["linkedin_connection"]["cap"] == 20
    assert by_channel["email"]["cap"] == 20
    assert by_channel["linkedin_inmail"]["cap"] == 5
    # Each route points at its own start node.
    assert by_channel["linkedin_connection"]["node_id"] == "h1"


def test_plan_route_first_touch_days_fills_quota_per_day():
    graph = _hybrid_graph()
    campaign = {"daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20}}
    routes = seq.resolve_routes(graph, campaign)

    # 90 prospects that all have both email + linkedin → day 1 should place the
    # full 20+20+5 = 45, day 2 the next 45.
    prospects_by_id = {}
    enrollments = []
    for i in range(90):
        pid = f"p{i}"
        prospects_by_id[pid] = {"_id": pid, "email": f"{i}@x.com", "linkedin": f"li/{i}"}
        enrollments.append({"_id": f"e{i}", "prospect_id": pid, "campaign_rule_score": float(90 - i)})

    assignments, skip = seq.plan_route_first_touch_days(campaign, enrollments, prospects_by_id, routes)
    assert len(assignments) == 90

    def day_channel_counts(day):
        counts: dict = {}
        for _enr, ch, _nid, d in assignments:
            if d == day:
                counts[ch] = counts.get(ch, 0) + 1
        return counts

    assert day_channel_counts(1) == {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5}
    assert day_channel_counts(2) == {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5}

    # Every assignment's start node id matches its channel's route.
    node_by_channel = {r["channel"]: r["node_id"] for r in routes}
    for _enr, ch, nid, _d in assignments:
        assert nid == node_by_channel[ch]


def test_plan_route_first_touch_days_respects_identifier_availability():
    graph = _hybrid_graph()
    campaign = {"daily_caps": {"linkedin_connection": 20, "email": 20, "linkedin_inmail": 5, "linkedin_message": 20}}
    routes = seq.resolve_routes(graph, campaign)

    prospects_by_id = {
        "p0": {"_id": "p0", "linkedin": "li/0"},   # linkedin only → connection or inmail
        "p1": {"_id": "p1", "email": "1@x.com"},    # email only → email route
        "p2": {"_id": "p2"},                          # neither → skipped
    }
    enrollments = [
        {"_id": "e0", "prospect_id": "p0", "campaign_rule_score": 3.0},
        {"_id": "e1", "prospect_id": "p1", "campaign_rule_score": 2.0},
        {"_id": "e2", "prospect_id": "p2", "campaign_rule_score": 1.0},
    ]
    assignments, skip = seq.plan_route_first_touch_days(campaign, enrollments, prospects_by_id, routes)
    placed = {enr["_id"]: ch for enr, ch, _nid, _d in assignments}
    assert placed["e0"] == "linkedin_connection"   # linkedin prospect fills the first route
    assert placed["e1"] == "email"
    assert "e2" not in placed
    assert skip.get("no_start_channel") == 1


# ─── Sub-day delays (delay_hours / delay_minutes) ───────────────────────────

def test_validator_accepts_sub_day_delay_fields():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply",
                   "delay_days": 0, "delay_hours": 4, "delay_minutes": 30}],
    }
    assert seq.validate_sequence_graph(graph) == []


def test_validator_rejects_invalid_sub_day_delay_fields():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply",
                   "delay_days": 0, "delay_hours": -1, "delay_minutes": "soon"}],
    }
    errors = seq.validate_sequence_graph(graph)
    assert any("delay_hours" in e for e in errors)
    assert any("delay_minutes" in e for e in errors)


def test_sent_timeout_wait_includes_hours_and_minutes():
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply",
                   "delay_days": 1, "delay_hours": 2, "delay_minutes": 15}],
        "settings": {"max_touches": 6, "stop_on_reply": True, "default_gap_days": 2},
    }
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    now = datetime(2026, 7, 15, 10, 0, 0)
    new_state = seq.advance_sequence(_enrollment(state), campaign, "sent", now=now)
    assert new_state["next_action_at"] == now + timedelta(days=1, hours=2, minutes=15)


def test_days_only_graph_behaves_identically():
    # Backward compat: no delay_hours/delay_minutes → pure delay_days wait.
    graph = {
        "nodes": [{"id": "a", "channel": "email"}, {"id": "b", "channel": "email"}],
        "edges": [{"id": "e1", "from": "a", "to": "b", "condition": "no_reply", "delay_days": 2}],
        "settings": {"max_touches": 6, "stop_on_reply": True, "default_gap_days": 2},
    }
    campaign = _campaign(graph)
    now = datetime(2026, 7, 15, 10, 0, 0)
    state = seq.advance_sequence(
        _enrollment(seq.build_initial_sequence_state(graph)), campaign, "sent", now=now
    )
    assert state["next_action_at"] == now + timedelta(days=2)


def test_accepted_edge_minutes_only_delay():
    graph = seq.build_default_sequence_graph()
    campaign = _campaign(graph)
    state = seq.build_initial_sequence_state(graph)
    state["phase"] = "awaiting"
    state["touches_done"] = 1
    now = datetime(2026, 7, 15, 10, 0, 0)
    new_state = seq.advance_sequence(_enrollment(state), campaign, "accepted", now=now)
    assert new_state["next_action_at"] == now + timedelta(minutes=30)
