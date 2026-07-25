"""
Per-campaign branching outreach sequences (multi-touch DAG).

This module implements the "sequence graph contract" agreed with the frontend
React Flow editor. It is the NEW multi-touch path for smart campaigns and runs
entirely in parallel to the legacy single-touch flow (``follow_up_flow`` /
``flow_state`` handled by ``flow_engine``). A campaign opts into this path only
by carrying a ``sequence_graph`` document; campaigns without one keep their
existing behaviour untouched.

Contract shape (stored on ``campaign.sequence_graph``)::

    {
      "version": 1,
      "nodes": [{"id": "n1", "type": "touch",
                 "channel": "linkedin_connection|linkedin_message|inmail|email",
                 "message_intent": "intro|followup|value|breakup",
                 "label": "Connection request",   # optional display label
                 "guidance": "...",               # optional AI authoring guidance
                 "position": {"x": 0, "y": 0}}],
      "edges": [{"id": "e1", "from": "n1", "to": "n2",
                 "condition": "always|accepted|not_accepted|no_reply",
                 "delay_days": 2,
                 "delay_hours": 0,      # optional, int >= 0, default 0
                 "delay_minutes": 0}],  # optional, int >= 0, default 0
      "settings": {"max_touches": 8, "stop_on_reply": true, "default_gap_days": 2}
    }

Each enrollment in a sequence campaign carries a ``sequence_state``::

    {"current_node_id": "n1", "touches_done": 0, "next_action_at": None,
     "stopped_reason": None, "phase": "pending_send", "last_sent_at": None}

``advance_sequence`` is the state machine that walks the graph in response to
engine events (``sent``, ``accepted``, ``replied``, ``no_reply_timeout``).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from bson import ObjectId

import database

logger = logging.getLogger(__name__)

# ─── Contract vocabulary ────────────────────────────────────────────────────

VALID_CHANNELS = frozenset({"linkedin_connection", "linkedin_message", "inmail", "email"})
VALID_INTENTS = frozenset({"intro", "followup", "value", "breakup"})
VALID_CONDITIONS = frozenset({"always", "accepted", "not_accepted", "no_reply"})

# Map contract channels → the channel keys the engine / daily_cap_service use.
# The contract calls it "inmail"; internally the engine + caps call it
# "linkedin_inmail". Every other channel name is shared verbatim.
CHANNEL_TO_ENGINE = {
    "linkedin_connection": "linkedin_connection",
    "linkedin_message": "linkedin_message",
    "inmail": "linkedin_inmail",
    "email": "email",
}

DEFAULT_SETTINGS = {"max_touches": 8, "stop_on_reply": True, "default_gap_days": 2}

# Conditions that represent the "keep going because nothing happened" branch of a
# node, in the priority order we consult them when scheduling a timeout re-check
# or resolving a ``no_reply_timeout`` event.
_TIMEOUT_CONDITIONS = ("no_reply", "not_accepted", "always")

# Terminal reasons written to ``sequence_state.stopped_reason``.
STOP_REPLIED = "replied"
STOP_MAX_TOUCHES = "max_touches"
STOP_SEQUENCE_END = "sequence_end"


# ─── Graph helpers ──────────────────────────────────────────────────────────

def get_settings(graph: dict) -> dict:
    """Return the graph's settings merged over the contract defaults."""
    merged = dict(DEFAULT_SETTINGS)
    merged.update((graph or {}).get("settings") or {})
    return merged


def get_node(graph: dict, node_id: Optional[str]) -> Optional[dict]:
    if not node_id:
        return None
    for node in (graph or {}).get("nodes", []):
        if node.get("id") == node_id:
            return node
    return None


def outgoing_edges(graph: dict, node_id: str) -> list[dict]:
    return [e for e in (graph or {}).get("edges", []) if e.get("from") == node_id]


def get_start_node_ids(graph: dict) -> list[str]:
    """Every node with no incoming edges — one per route (entry point).

    A hybrid campaign has several start nodes (LinkedIn-first, Email-first,
    InMail…); each is the head of an independent route chain. Order mirrors the
    node declaration order so planning is deterministic.
    """
    nodes = (graph or {}).get("nodes", [])
    if not nodes:
        return []
    targets = {e.get("to") for e in (graph or {}).get("edges", [])}
    starts = [n.get("id") for n in nodes if n.get("id") not in targets]
    return starts


def get_start_node_id(graph: dict) -> Optional[str]:
    """The first node with no incoming edges (a single-route entry point).

    Retained for single-route callers and legacy behaviour. For multi-route
    (hybrid) graphs use ``get_start_node_ids``. Falls back to the first declared
    node if the graph is malformed (callers that care validate first).
    """
    starts = get_start_node_ids(graph)
    if starts:
        return starts[0]
    nodes = (graph or {}).get("nodes", [])
    return nodes[0].get("id") if nodes else None


def engine_channel(contract_channel: Optional[str]) -> str:
    """Translate a contract channel to the engine / daily-cap channel key."""
    return CHANNEL_TO_ENGINE.get(contract_channel or "", contract_channel or "email")


# ─── Validation ─────────────────────────────────────────────────────────────

def validate_sequence_graph(graph: dict) -> list[str]:
    """Validate a sequence graph against the contract.

    Returns a list of human-readable error strings (empty list == valid). The
    checks mirror the frontend editor's invariants:

    * at least one start node (a node with no incoming edges) — hybrid graphs
      have several, one per route (LinkedIn-first, Email-first, InMail…),
    * no cycles (the graph must be a DAG),
    * every edge has a valid condition and ``delay_days >= 0`` (optional
      ``delay_hours`` / ``delay_minutes`` must be integers >= 0 when present),
    * every node has a valid channel (and valid message_intent if present),
    * the longest path length does not exceed ``settings.max_touches``.
    """
    errors: list[str] = []
    if not isinstance(graph, dict):
        return ["sequence_graph must be an object"]

    nodes = graph.get("nodes")
    edges = graph.get("edges")
    settings = get_settings(graph)

    if not isinstance(nodes, list) or not nodes:
        errors.append("nodes must be a non-empty list")
        nodes = nodes if isinstance(nodes, list) else []
    if edges is None:
        edges = []
    if not isinstance(edges, list):
        errors.append("edges must be a list")
        edges = []

    max_touches = settings.get("max_touches", 8)
    if not isinstance(max_touches, int) or isinstance(max_touches, bool) or max_touches < 1:
        errors.append("settings.max_touches must be a positive integer")
        max_touches = 8

    # Nodes
    node_ids: list[str] = []
    seen: set[str] = set()
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            errors.append(f"node[{i}] must be an object")
            continue
        nid = node.get("id")
        if not nid:
            errors.append(f"node[{i}] is missing an id")
            continue
        if nid in seen:
            errors.append(f"duplicate node id '{nid}'")
        seen.add(nid)
        node_ids.append(nid)
        channel = node.get("channel")
        if channel not in VALID_CHANNELS:
            errors.append(
                f"node '{nid}': invalid channel '{channel}' "
                f"(must be one of {sorted(VALID_CHANNELS)})"
            )
        intent = node.get("message_intent")
        if intent is not None and intent not in VALID_INTENTS:
            errors.append(
                f"node '{nid}': invalid message_intent '{intent}' "
                f"(must be one of {sorted(VALID_INTENTS)})"
            )

    node_id_set = set(node_ids)

    # Edges
    incoming: dict[str, int] = {nid: 0 for nid in node_ids}
    adjacency: dict[str, list[str]] = {nid: [] for nid in node_ids}
    for i, edge in enumerate(edges):
        if not isinstance(edge, dict):
            errors.append(f"edge[{i}] must be an object")
            continue
        frm = edge.get("from")
        to = edge.get("to")
        condition = edge.get("condition")
        delay = edge.get("delay_days")
        if frm not in node_id_set:
            errors.append(f"edge[{i}]: 'from' references unknown node '{frm}'")
        if to not in node_id_set:
            errors.append(f"edge[{i}]: 'to' references unknown node '{to}'")
        if condition not in VALID_CONDITIONS:
            errors.append(
                f"edge[{i}]: invalid condition '{condition}' "
                f"(must be one of {sorted(VALID_CONDITIONS)})"
            )
        if not isinstance(delay, int) or isinstance(delay, bool) or delay < 0:
            errors.append(f"edge[{i}]: delay_days must be an integer >= 0")
        # Optional sub-day delay components (coexist with delay_days).
        for _dkey in ("delay_hours", "delay_minutes"):
            _dval = edge.get(_dkey)
            if _dval is not None and (
                not isinstance(_dval, int) or isinstance(_dval, bool) or _dval < 0
            ):
                errors.append(f"edge[{i}]: {_dkey} must be an integer >= 0")
        if frm in node_id_set and to in node_id_set:
            adjacency[frm].append(to)
            incoming[to] += 1

    # Start nodes — at least one node with no incoming edges (one per route).
    if node_ids:
        starts = [nid for nid in node_ids if incoming.get(nid, 0) == 0]
        if not starts:
            errors.append(
                "no start node found — every node has an incoming edge "
                "(each route needs one entry node and the graph must be acyclic)"
            )

    # Cycles / longest path.
    if _has_cycle(node_ids, adjacency):
        errors.append("sequence graph contains a cycle — it must be a DAG")
    elif node_ids:
        longest = _longest_path_length(node_ids, adjacency)
        if longest > max_touches:
            errors.append(
                f"longest path traverses {longest} touches, which exceeds "
                f"settings.max_touches ({max_touches})"
            )

    return errors


def _has_cycle(node_ids: list[str], adjacency: dict[str, list[str]]) -> bool:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {nid: WHITE for nid in node_ids}

    def visit(u: str) -> bool:
        color[u] = GRAY
        for v in adjacency.get(u, []):
            if color.get(v) == GRAY:
                return True
            if color.get(v) == WHITE and visit(v):
                return True
        color[u] = BLACK
        return False

    return any(color[nid] == WHITE and visit(nid) for nid in node_ids)


def _longest_path_length(node_ids: list[str], adjacency: dict[str, list[str]]) -> int:
    """Longest path measured in number of nodes (touches). Assumes a DAG."""
    memo: dict[str, int] = {}

    def dfs(u: str) -> int:
        if u in memo:
            return memo[u]
        best = 1
        for v in adjacency.get(u, []):
            best = max(best, 1 + dfs(v))
        memo[u] = best
        return best

    return max((dfs(nid) for nid in node_ids), default=0)


# ─── Default LinkedIn-first template ────────────────────────────────────────

def build_default_sequence_graph() -> dict:
    """Build the default sequence — a single LinkedIn-first route (6 touches).

    ::

        n1 connection (intro, friendly, no pitch)
          --(accepted, 30min)--> n2 li_message followup (light conversational)
          --(no_reply, 1d)-----> n3 email value  (deck CTA: reply for the deck)
          --(no_reply, 2d)-----> n4 email value  (different angle: proof / case study)
          --(no_reply, 2d)-----> n5 email followup (different angle: ROI / direct question)
          --(no_reply, 2d)-----> n6 email breakup (short, door open)
        n1 --(not_accepted, 1d)--> n3   (connection ignored 1 day → email path)

    Node ``label`` / ``guidance`` are optional authoring metadata: the label is
    a display name for the editor/message drawer; ``guidance`` steers the AI
    message generator (threaded through ``build_campaign_followup_prompt``).
    """
    nodes = [
        {"id": "n1", "type": "touch", "channel": "linkedin_connection",
         "message_intent": "intro", "label": "Connection request",
         "guidance": "Friendly, human connection note. No pitch, no selling — "
                     "just a genuine, specific reason to connect.",
         "position": {"x": 0, "y": 0}},
        {"id": "n2", "type": "touch", "channel": "linkedin_message",
         "message_intent": "followup", "label": "Thanks for connecting",
         "guidance": "Light conversational follow-up right after they accept. "
                     "Casual, curious, no hard pitch.",
         "position": {"x": 280, "y": 0}},
        {"id": "n3", "type": "touch", "channel": "email",
         "message_intent": "value", "label": "Deck offer email",
         "guidance": "Clear CTA: ask them to simply reply if they want the "
                     "detailed deck about the service (use the campaign's "
                     "value proposition for what the deck covers).",
         "position": {"x": 560, "y": 0}},
        {"id": "n4", "type": "touch", "channel": "email",
         "message_intent": "value", "label": "Proof email",
         "guidance": "Different angle from the previous email: lead with proof "
                     "— a case study or customer outcome with a concrete metric.",
         "position": {"x": 840, "y": 0}},
        {"id": "n5", "type": "touch", "channel": "email",
         "message_intent": "followup", "label": "ROI question email",
         "guidance": "Different angle again: ROI framing that ends in one "
                     "short, direct question.",
         "position": {"x": 1120, "y": 0}},
        {"id": "n6", "type": "touch", "channel": "email",
         "message_intent": "breakup", "label": "Breakup email",
         "guidance": "Short break-up note. Two or three sentences, no guilt, "
                     "leave the door open.",
         "position": {"x": 1400, "y": 0}},
    ]
    edges = [
        {"id": "e1", "from": "n1", "to": "n2", "condition": "accepted",
         "delay_days": 0, "delay_minutes": 30},
        {"id": "e2", "from": "n2", "to": "n3", "condition": "no_reply", "delay_days": 1},
        {"id": "e3", "from": "n3", "to": "n4", "condition": "no_reply", "delay_days": 2},
        {"id": "e4", "from": "n4", "to": "n5", "condition": "no_reply", "delay_days": 2},
        {"id": "e5", "from": "n5", "to": "n6", "condition": "no_reply", "delay_days": 2},
        {"id": "e6", "from": "n1", "to": "n3", "condition": "not_accepted", "delay_days": 1},
    ]
    return {
        "version": 1,
        "nodes": nodes,
        "edges": edges,
        "settings": {"max_touches": 6, "stop_on_reply": True, "default_gap_days": 2},
    }


# ─── State machine ──────────────────────────────────────────────────────────

def build_initial_sequence_state(
    graph: dict,
    now: Optional[datetime] = None,
    start_node_id: Optional[str] = None,
) -> dict:
    """Return the sequence_state for a freshly-enrolled prospect.

    ``start_node_id`` pins the prospect to a specific route's entry node (hybrid
    graphs have several). When omitted, falls back to the first start node.
    """
    return {
        "current_node_id": start_node_id or get_start_node_id(graph),
        "touches_done": 0,
        "next_action_at": None,
        "stopped_reason": None,
        "phase": "pending_send",
        "last_sent_at": None,
    }


def _clamp(dt: datetime, campaign: dict) -> datetime:
    """Clamp a naive-UTC datetime into the campaign's send window if possible."""
    try:
        from services.campaign_launch_service import clamp_to_send_window
        return clamp_to_send_window(dt, campaign)
    except Exception:
        return dt


def _edge_delay_component(edge: dict, key: str) -> Optional[int]:
    """Return a validated non-negative int delay component, or None if absent/invalid."""
    value = (edge or {}).get(key)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def edge_wait_delta(edge: dict) -> Optional[timedelta]:
    """Total wait encoded on an edge as ``timedelta(days, hours, minutes)``.

    ``delay_hours`` / ``delay_minutes`` are optional and default to 0, so graphs
    that only carry ``delay_days`` behave exactly as before. Returns None when
    the edge carries no valid delay component at all (caller picks a fallback).
    """
    days = _edge_delay_component(edge, "delay_days")
    hours = _edge_delay_component(edge, "delay_hours")
    minutes = _edge_delay_component(edge, "delay_minutes")
    if days is None and hours is None and minutes is None:
        return None
    return timedelta(days=days or 0, hours=hours or 0, minutes=minutes or 0)


def _timeout_wait_delta(edges: list[dict], settings: dict) -> timedelta:
    """How long to wait before re-checking a node's outgoing timeout branch."""
    for condition in _TIMEOUT_CONDITIONS:
        edge = next((e for e in edges if e.get("condition") == condition), None)
        if edge is not None:
            delta = edge_wait_delta(edge)
            if delta is not None:
                return delta
    return timedelta(days=int(settings.get("default_gap_days", 2)))


def _pick_timeout_edge(edges: list[dict]) -> Optional[dict]:
    """Resolve the outgoing edge taken when a node's touch draws no response."""
    for condition in _TIMEOUT_CONDITIONS:
        edge = next((e for e in edges if e.get("condition") == condition), None)
        if edge is not None:
            return edge
    return None


def advance_sequence(
    enrollment: dict,
    campaign: dict,
    event: str,
    now: Optional[datetime] = None,
) -> dict:
    """Advance an enrollment's ``sequence_state`` in response to an engine event.

    Events:

    * ``sent`` — the current node's touch was just delivered. We record it,
      enforce the ``max_touches`` cap, then schedule the timeout re-check
      (``next_action_at = now + timeout-edge delay``) and move to the
      ``awaiting`` phase without changing ``current_node_id``.
    * ``accepted`` — a LinkedIn connection was accepted. Follow the ``accepted``
      edge (waiting its ``delay_days`` + optional ``delay_hours`` /
      ``delay_minutes``); if none exists, fall through to the timeout branch.
    * ``no_reply_timeout`` — the timeout fired with no reply/acceptance. Follow
      the node's timeout branch (``no_reply``/``not_accepted``/``always``) and
      send the target node immediately (the delay was already served while
      waiting).
    * ``replied`` — the prospect replied on any channel. Stop everything
      (``stopped_reason = "replied"``); ``stop_on_reply`` is always enforced.

    Returns the updated ``sequence_state`` dict (never mutates the input).
    """
    graph = campaign.get("sequence_graph") or {}
    settings = get_settings(graph)
    now = now or datetime.utcnow()

    state = dict(enrollment.get("sequence_state") or build_initial_sequence_state(graph, now))

    # Already terminal — nothing further to do.
    if state.get("stopped_reason"):
        return state

    # Reply stops the whole sequence regardless of the current node.
    if event == "replied":
        state.update(
            stopped_reason=STOP_REPLIED,
            phase="stopped",
            next_action_at=None,
        )
        return state

    current_id = state.get("current_node_id")
    edges = outgoing_edges(graph, current_id)

    if event == "sent":
        state["touches_done"] = int(state.get("touches_done", 0)) + 1
        state["last_sent_at"] = now
        # Enforce the per-prospect touch cap.
        if state["touches_done"] >= int(settings.get("max_touches", 8)):
            state.update(stopped_reason=STOP_MAX_TOUCHES, phase="stopped", next_action_at=None)
            return state
        # No outgoing branch — this was the final touch on this path.
        if not edges:
            state.update(stopped_reason=STOP_SEQUENCE_END, phase="stopped", next_action_at=None)
            return state
        wait = _timeout_wait_delta(edges, settings)
        state.update(
            phase="awaiting",
            next_action_at=_clamp(now + wait, campaign),
        )
        return state

    # ── Transition events (accepted / no_reply_timeout) ──
    edge: Optional[dict] = None
    is_accepted = False
    if event == "accepted":
        edge = next((e for e in edges if e.get("condition") == "accepted"), None)
        is_accepted = edge is not None
    if edge is None:
        # no_reply_timeout, or an acceptance with no dedicated accepted branch.
        edge = _pick_timeout_edge(edges)

    if edge is None:
        state.update(stopped_reason=STOP_SEQUENCE_END, phase="stopped", next_action_at=None)
        return state

    target = edge.get("to")
    if get_node(graph, target) is None:
        state.update(stopped_reason=STOP_SEQUENCE_END, phase="stopped", next_action_at=None)
        return state

    if is_accepted:
        # Acceptance arrives on its own timeline — honour the accepted edge delay
        # (days + optional hours/minutes; no delay fields at all → send now).
        delay = edge_wait_delta(edge) or timedelta(0)
        next_at = _clamp(now + delay, campaign)
    else:
        # The no-reply wait already elapsed during the awaiting phase; send now.
        next_at = _clamp(now, campaign)

    state.update(current_node_id=target, phase="pending_send", next_action_at=next_at)
    return state


# ─── Enrollment first-touch planning (used by discovery finalize) ────────────

def _enrollment_score(enrollment: dict, prospects_by_id: dict) -> float:
    rs = enrollment.get("campaign_rule_score")
    if rs is not None:
        return float(rs)
    p = prospects_by_id.get(enrollment.get("prospect_id"), {})
    return float(p.get("ai_prospect_score") or p.get("prospect_score") or 0)


def _prospect_has_channel(prospect: dict, engine_ch: str) -> bool:
    if engine_ch == "email":
        return bool(prospect.get("email"))
    if engine_ch in ("linkedin_connection", "linkedin_message", "linkedin_inmail"):
        return bool(prospect.get("linkedin") or prospect.get("linkedin_url"))
    return False


def plan_first_touch_days(
    campaign: dict,
    enrollments: list[dict],
    prospects_by_id: dict,
    start_engine_channel: str,
) -> tuple[list[tuple[dict, int]], dict[str, int]]:
    """Assign a send day to every enrollment for the sequence's start node.

    Every prospect enters on the same channel (the start node's), so we simply
    fill the start channel's daily cap day by day, claiming prospects in
    descending score order (top-score-first, mirroring the classic planner).

    Returns ``([(enrollment, day), ...], skip_reasons)``.
    """
    from services.daily_cap_service import DEFAULT_CAPS

    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    per_day = int(caps.get(start_engine_channel, DEFAULT_CAPS.get(start_engine_channel, 20)) or 0)

    ordered = sorted(enrollments, key=lambda e: _enrollment_score(e, prospects_by_id), reverse=True)

    assignments: list[tuple[dict, int]] = []
    skip_reasons: dict[str, int] = {}

    if per_day <= 0:
        skip_reasons["start_channel_disabled"] = len(ordered)
        return assignments, skip_reasons

    day = 1
    used_today = 0
    for enrollment in ordered:
        prospect = prospects_by_id.get(enrollment.get("prospect_id"), {})
        if prospect.get("status") in ("opted_out", "bounced", "disqualified"):
            skip_reasons["terminal_status"] = skip_reasons.get("terminal_status", 0) + 1
            continue
        if not _prospect_has_channel(prospect, start_engine_channel):
            skip_reasons["no_start_channel"] = skip_reasons.get("no_start_channel", 0) + 1
            continue
        if used_today >= per_day:
            day += 1
            used_today = 0
        assignments.append((enrollment, day))
        used_today += 1

    return assignments, skip_reasons


def resolve_routes(graph: dict, campaign: dict) -> list[dict]:
    """Resolve a graph's start nodes into schedulable routes.

    One route per start node, de-duplicated by engine channel (if two start
    nodes share a channel the first wins — a prospect can only enter a channel
    once). Each route carries its daily cap, pulled from ``campaign.daily_caps``
    with a ``DEFAULT_CAPS`` fallback.

    Returns ``[{"channel": engine_ch, "node_id": nid, "cap": int}, ...]`` in
    node-declaration order.
    """
    from services.daily_cap_service import DEFAULT_CAPS

    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    routes: list[dict] = []
    seen_channels: set[str] = set()
    for node_id in get_start_node_ids(graph):
        node = get_node(graph, node_id)
        if not node:
            continue
        engine_ch = engine_channel(node.get("channel"))
        if engine_ch in seen_channels:
            continue
        seen_channels.add(engine_ch)
        cap = int(caps.get(engine_ch, DEFAULT_CAPS.get(engine_ch, 20)) or 0)
        routes.append({"channel": engine_ch, "node_id": node_id, "cap": cap})
    return routes


def plan_route_first_touch_days(
    campaign: dict,
    enrollments: list[dict],
    prospects_by_id: dict,
    routes: list[dict],
    existing_counts: dict[tuple[int, str], int] | None = None,
) -> tuple[list[tuple[dict, str, str, int]], dict[str, int]]:
    """Distribute prospects across a hybrid graph's routes, day by day.

    Strict-quota fill (matches the product spec "20 LinkedIn connections, 20
    emails, 5 InMails each day"): for each day we walk the routes in order and
    fill each route's daily cap from the top-score-first pool, skipping a
    prospect for a route only when it lacks that channel's identifier (no
    LinkedIn URL for LinkedIn routes, no email for the email route). A prospect
    is claimed by exactly one route. Prospects that fit no route at all are
    reported as skipped.

    ``existing_counts`` — ``{(day, engine_channel): count}`` of enrollments that
    are ALREADY planned on those days (e.g. from earlier top-up generations of
    the same campaign). Each day's per-channel cap is seeded from this so a new
    batch continues filling from the first day that still has room instead of
    piling onto day 1 again. Without it, every generation re-buckets from day 1
    and day 1 grows without bound past the quota.

    Returns ``([(enrollment, engine_channel, start_node_id, day), ...],
    skip_reasons)``.
    """
    active_routes = [r for r in routes if r.get("cap", 0) > 0]
    assignments: list[tuple[dict, str, str, int]] = []
    skip_reasons: dict[str, int] = {}

    if not active_routes:
        skip_reasons["start_channel_disabled"] = len(enrollments)
        return assignments, skip_reasons

    # Per-(day, channel) usage, seeded from already-planned enrollments so a
    # top-up generation doesn't overfill days the previous generations filled.
    used: dict[tuple[int, str], int] = {
        (int(d), str(ch)): int(n) for (d, ch), n in (existing_counts or {}).items()
    }
    route_channels = [r["channel"] for r in active_routes]

    # Drop terminal + no-channel prospects up-front, then order by score.
    pool: list[dict] = []
    for enr in enrollments:
        prospect = prospects_by_id.get(enr.get("prospect_id"), {})
        if prospect.get("status") in ("opted_out", "bounced", "disqualified"):
            skip_reasons["terminal_status"] = skip_reasons.get("terminal_status", 0) + 1
            continue
        if not any(_prospect_has_channel(prospect, ch) for ch in route_channels):
            # Fits no active route at all — never schedulable here.
            skip_reasons["no_start_channel"] = skip_reasons.get("no_start_channel", 0) + 1
            continue
        pool.append(enr)
    pool.sort(key=lambda e: _enrollment_score(e, prospects_by_id), reverse=True)

    day = 1
    # Every remaining prospect has at least one route channel, so it WILL place
    # once days advance far enough; the cap only bounds runaway iteration.
    _MAX_DAYS = 3650
    while pool and day <= _MAX_DAYS:
        remaining: list[dict] = pool
        for route in active_routes:
            channel = route["channel"]
            node_id = route["node_id"]
            cap = route["cap"]
            next_remaining: list[dict] = []
            for enr in remaining:
                if used.get((day, channel), 0) >= cap:
                    next_remaining.append(enr)
                    continue
                prospect = prospects_by_id.get(enr.get("prospect_id"), {})
                if not _prospect_has_channel(prospect, channel):
                    next_remaining.append(enr)
                    continue
                assignments.append((enr, channel, node_id, day))
                used[(day, channel)] = used.get((day, channel), 0) + 1
            remaining = next_remaining
        # Whatever is left couldn't fit any route's remaining cap this day —
        # advance to the next day (its caps start fresh) and retry.
        pool = remaining
        day += 1

    if pool:
        # Safety valve: should not happen (channel-capable prospects always
        # place eventually), but never silently drop.
        skip_reasons["day_horizon_exceeded"] = skip_reasons.get("day_horizon_exceeded", 0) + len(pool)

    return assignments, skip_reasons


# ─── Reply fan-out (called from reply-webhook / poller paths) ────────────────

def _id_variants(value) -> list:
    """Return [ObjectId, str] variants of an id for cross-schema matching."""
    variants: list = []
    try:
        variants.append(ObjectId(str(value)))
    except Exception:
        pass
    variants.append(str(value))
    return variants


async def mark_sequence_replied(prospect_id, campaign_id=None) -> int:
    """Stop every sequence enrollment for a prospect that replied on any channel.

    Stamps ``sequence_state.stopped_reason = "replied"`` (and flips the
    enrollment to ``replied``) so the engine never sends another touch. Only
    enrollments that carry a ``sequence_state`` and are not already stopped are
    affected — legacy (flow_state) enrollments are left untouched.

    Returns the number of enrollments updated.
    """
    now = datetime.utcnow()
    query: dict = {
        "prospect_id": {"$in": _id_variants(prospect_id)},
        "sequence_state": {"$exists": True, "$ne": None},
        "sequence_state.stopped_reason": None,
    }
    if campaign_id is not None:
        query["campaign_id"] = {"$in": _id_variants(campaign_id)}
    try:
        result = await database.campaign_enrollments_collection.update_many(
            query,
            {"$set": {
                "status": "replied",
                "replied_at": now,
                "last_activity_at": now,
                "sequence_state.stopped_reason": STOP_REPLIED,
                "sequence_state.phase": "stopped",
                "sequence_state.next_action_at": None,
            }},
        )
        return result.modified_count
    except Exception as e:
        logger.warning(f"mark_sequence_replied failed for prospect {prospect_id}: {e}")
        return 0
