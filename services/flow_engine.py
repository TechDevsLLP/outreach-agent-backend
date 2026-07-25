"""
Event-driven follow-up state machine for smart campaigns.

Each enrolled prospect has a flow_state tracking which node they're on.
Events (sent, accepted, replied, bounced, no_response_timeout, send_failed)
drive transitions through the campaign's follow_up_flow DAG.
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

# ─── Default flow charts ────────────────────────────────────────────────────

DEFAULT_FLOW_LINKEDIN_FIRST = {
    "version": 1,
    "entry_rules": {
        "linkedin_first_required_fields": ["linkedin"],
        "email_first_required_fields": ["email"],
    },
    "nodes": [
        {
            "id": "n1",
            "channel": "linkedin_connection",
            "delay_days": 0,
            "label": "Send LinkedIn Connection",
            "on_accepted": "n3",
            "on_not_accepted_after": {"days": 3, "next": "n2"},
            "on_replied": "STOP",
            "on_send_failed": "n2",
        },
        {
            "id": "n2",
            "channel": "email",
            "delay_days": 0,
            "label": "Send Cold Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n4"},
            "on_bounced": "n4",
            "on_send_failed": "STOP",
        },
        {
            "id": "n3",
            "channel": "linkedin_message",
            "delay_days": 1,
            "label": "LinkedIn Follow-Up Message",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 5, "next": "STOP"},
            "on_send_failed": "STOP",
        },
        {
            "id": "n4",
            "channel": "linkedin_inmail",
            "delay_days": 0,
            "label": "Send LinkedIn InMail",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "STOP"},
            "on_send_failed": "STOP",
        },
    ],
}

DEFAULT_FLOW_EMAIL_FIRST = {
    "version": 1,
    "entry_rules": {
        "linkedin_first_required_fields": ["linkedin"],
        "email_first_required_fields": ["email"],
    },
    "nodes": [
        {
            "id": "n1",
            "channel": "email",
            "delay_days": 0,
            "label": "Send Cold Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n2"},
            "on_bounced": "n3",
            "on_send_failed": "STOP",
        },
        {
            "id": "n2",
            "channel": "linkedin_connection",
            "delay_days": 0,
            "label": "Send LinkedIn Connection",
            "on_accepted": "n4",
            "on_not_accepted_after": {"days": 3, "next": "n3"},
            "on_replied": "STOP",
            "on_send_failed": "n3",
        },
        {
            "id": "n3",
            "channel": "linkedin_inmail",
            "delay_days": 0,
            "label": "Send LinkedIn InMail",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "STOP"},
            "on_send_failed": "STOP",
        },
        {
            "id": "n4",
            "channel": "linkedin_message",
            "delay_days": 1,
            "label": "LinkedIn Follow-Up Message",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 5, "next": "STOP"},
            "on_send_failed": "STOP",
        },
    ],
}

DEFAULT_FLOW_EMAIL_ONLY = {
    "version": 1,
    "nodes": [
        {
            "id": "n1",
            "channel": "email",
            "delay_days": 0,
            "label": "Send Cold Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n2"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
        {
            "id": "n2",
            "channel": "email",
            "delay_days": 0,
            "label": "Follow-Up Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 5, "next": "STOP"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
    ],
}

DEFAULT_FLOW_LINKEDIN_ONLY = {
    "version": 1,
    "nodes": [
        {
            "id": "n1",
            "channel": "linkedin_connection",
            "delay_days": 0,
            "label": "Send LinkedIn Connection",
            "on_accepted": "n2",
            "on_not_accepted_after": {"days": 3, "next": "n3"},
            "on_replied": "STOP",
            "on_send_failed": "n3",
        },
        {
            "id": "n2",
            "channel": "linkedin_message",
            "delay_days": 1,
            "label": "LinkedIn Follow-Up Message",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 5, "next": "STOP"},
            "on_send_failed": "STOP",
        },
        {
            "id": "n3",
            "channel": "linkedin_inmail",
            "delay_days": 0,
            "label": "Send LinkedIn InMail",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "STOP"},
            "on_send_failed": "STOP",
        },
    ],
}

# ─── Validation ─────────────────────────────────────────────────────────────

VALID_CHANNELS = {
    "email", "linkedin_connection", "linkedin_inmail", "linkedin_message", "wait",
    "linkedin_dm", "voice_note", "video_dm", "propose_meeting", "ai_reply", "pause",
}
VALID_EVENTS = {"sent", "accepted", "replied", "bounced", "send_failed", "no_response_timeout", "dm_sent"}

# ─── Aggressive preset — 12 touches / 28 days ───────────────────────────────

AGGRESSIVE_PRESET_FLOW = {
    "version": 2,
    "preset": "aggressive",
    "nodes": [
        {
            "id": "n1", "channel": "email", "delay_days": 0,
            "label": "D0 Cold Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 2, "next": "n3"},
            "on_bounced": "n2_li",
            "on_send_failed": "n2_li",
        },
        {
            "id": "n2_li", "channel": "linkedin_connection", "delay_days": 0,
            "label": "D0 LinkedIn Connection",
            "on_accepted": "n3_dm",
            "on_not_accepted_after": {"days": 7, "next": "n5_inmail"},
            "on_replied": "STOP",
            "on_send_failed": "n3",
        },
        {
            "id": "n3", "channel": "email", "delay_days": 2,
            "label": "D2 Follow-Up Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n5_inmail"},
            "on_bounced": "n5_inmail",
            "on_send_failed": "STOP",
        },
        {
            "id": "n3_dm", "channel": "linkedin_dm", "delay_hours": 12,
            "delay_days": 0,
            "label": "D3 LinkedIn DM (on accept)",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 2, "next": "n5"},
            "on_send_failed": "STOP",
        },
        {
            "id": "n5", "channel": "email", "delay_days": 5,
            "label": "D5 Value-Add Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 2, "next": "n5_inmail"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
        {
            "id": "n5_inmail", "channel": "linkedin_inmail", "delay_days": 7,
            "label": "D7 LinkedIn InMail",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n7"},
            "on_send_failed": "STOP",
        },
        {
            "id": "n7", "channel": "email", "delay_days": 10,
            "label": "D10 Proof/Case Study Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 4, "next": "n8_voice"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
        {
            "id": "n8_voice", "channel": "voice_note", "delay_days": 14,
            "label": "D14 Voice Note",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n9"},
            "on_send_failed": "n9",
        },
        {
            "id": "n9", "channel": "email", "delay_days": 17,
            "label": "D17 Challenge Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 4, "next": "n10_video"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
        {
            "id": "n10_video", "channel": "video_dm", "delay_days": 21,
            "label": "D21 Video DM",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 3, "next": "n11"},
            "on_send_failed": "n11",
        },
        {
            "id": "n11", "channel": "email", "delay_days": 24,
            "label": "D24 Soft Breakup Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 4, "next": "n12"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
        {
            "id": "n12", "channel": "email", "delay_days": 28,
            "label": "D28 Hard Breakup Email",
            "on_replied": "STOP",
            "on_no_reply_after": {"days": 30, "next": "STOP"},
            "on_bounced": "STOP",
            "on_send_failed": "STOP",
        },
    ],
}

MODERATE_PRESET_FLOW = {
    "version": 2,
    "preset": "moderate",
    "nodes": [
        {
            "id": "n1", "channel": "email", "delay_days": 0,
            "label": "D0 Cold Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 3, "next": "n2_li"},
            "on_bounced": "n2_li", "on_send_failed": "n2_li",
        },
        {
            "id": "n2_li", "channel": "linkedin_connection", "delay_days": 3,
            "label": "D3 LinkedIn Connection",
            "on_accepted": "n4_dm",
            "on_not_accepted_after": {"days": 5, "next": "n5_inmail"},
            "on_replied": "STOP", "on_send_failed": "n5_inmail",
        },
        {
            "id": "n4_dm", "channel": "linkedin_dm", "delay_days": 4,
            "label": "D4 LinkedIn DM",
            "on_replied": "STOP", "on_no_reply_after": {"days": 4, "next": "n7"},
            "on_send_failed": "n7",
        },
        {
            "id": "n5_inmail", "channel": "linkedin_inmail", "delay_days": 8,
            "label": "D8 LinkedIn InMail",
            "on_replied": "STOP", "on_no_reply_after": {"days": 4, "next": "n7"},
            "on_send_failed": "n7",
        },
        {
            "id": "n7", "channel": "email", "delay_days": 12,
            "label": "D12 Follow-Up Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 4, "next": "n10"},
            "on_bounced": "STOP", "on_send_failed": "STOP",
        },
        {
            "id": "n10", "channel": "email", "delay_days": 16,
            "label": "D16 Value Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 5, "next": "n14"},
            "on_bounced": "STOP", "on_send_failed": "STOP",
        },
        {
            "id": "n14", "channel": "email", "delay_days": 21,
            "label": "D21 Breakup Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 30, "next": "STOP"},
            "on_bounced": "STOP", "on_send_failed": "STOP",
        },
    ],
}

CONSERVATIVE_PRESET_FLOW = {
    "version": 2,
    "preset": "conservative",
    "nodes": [
        {
            "id": "n1", "channel": "email", "delay_days": 0,
            "label": "D0 Cold Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 4, "next": "n2_li"},
            "on_bounced": "n2_li", "on_send_failed": "STOP",
        },
        {
            "id": "n2_li", "channel": "linkedin_connection", "delay_days": 4,
            "label": "D4 LinkedIn Connection",
            "on_accepted": "n3_dm",
            "on_not_accepted_after": {"days": 7, "next": "n7"},
            "on_replied": "STOP", "on_send_failed": "n7",
        },
        {
            "id": "n3_dm", "channel": "linkedin_dm", "delay_days": 5,
            "label": "D5 LinkedIn DM",
            "on_replied": "STOP", "on_no_reply_after": {"days": 6, "next": "n7"},
            "on_send_failed": "n7",
        },
        {
            "id": "n7", "channel": "email", "delay_days": 14,
            "label": "D14 Breakup Email",
            "on_replied": "STOP", "on_no_reply_after": {"days": 30, "next": "STOP"},
            "on_bounced": "STOP", "on_send_failed": "STOP",
        },
    ],
}

def validate_flow(flow: dict) -> list[str]:
    """Return list of validation error strings. Empty = valid."""
    errors = []
    if not flow or not isinstance(flow, dict):
        return ["flow must be a dict"]
    nodes = flow.get("nodes", [])
    if not nodes:
        return ["flow must have at least one node"]
    node_ids = {n["id"] for n in nodes}
    valid_targets = node_ids | {"STOP"}
    for node in nodes:
        nid = node.get("id")
        if not nid:
            errors.append("every node must have an id")
            continue
        ch = node.get("channel")
        if ch not in VALID_CHANNELS:
            errors.append(f"node {nid}: invalid channel '{ch}', must be one of {VALID_CHANNELS}")
        # collect all transition targets
        for field in ("on_accepted", "on_replied", "on_send_failed", "on_bounced"):
            t = node.get(field)
            if t and t not in valid_targets:
                errors.append(f"node {nid}: {field} references unknown target '{t}'")
        for timeout_field in ("on_not_accepted_after", "on_no_reply_after"):
            t = node.get(timeout_field)
            if t:
                next_id = t.get("next")
                if next_id and next_id not in valid_targets:
                    errors.append(f"node {nid}: {timeout_field}.next references unknown target '{next_id}'")
                days = t.get("days")
                if not isinstance(days, int) or days < 1:
                    errors.append(f"node {nid}: {timeout_field}.days must be positive int")
    # cycle detection via DFS
    adj: dict[str, set[str]] = {n["id"]: set() for n in nodes}
    for node in nodes:
        nid = node["id"]
        for field in ("on_accepted", "on_replied", "on_send_failed", "on_bounced"):
            t = node.get(field)
            if t and t in adj:
                adj[nid].add(t)
        for timeout_field in ("on_not_accepted_after", "on_no_reply_after"):
            t = node.get(timeout_field)
            if t:
                nxt = t.get("next")
                if nxt and nxt in adj:
                    adj[nid].add(nxt)
    visited: set[str] = set()
    in_stack: set[str] = set()
    def has_cycle(v: str) -> bool:
        visited.add(v)
        in_stack.add(v)
        for nb in adj.get(v, set()):
            if nb not in visited:
                if has_cycle(nb):
                    return True
            elif nb in in_stack:
                return True
        in_stack.discard(v)
        return False
    for n in list(adj.keys()):
        if n not in visited:
            if has_cycle(n):
                errors.append("flow graph contains a cycle — all paths must eventually reach STOP")
                break
    # each node must have at least one STOP-reachable path
    def can_reach_stop(start: str, visited_r: set) -> bool:
        if start == "STOP":
            return True
        if start in visited_r or start not in adj:
            return False
        visited_r.add(start)
        node = next((n for n in nodes if n["id"] == start), None)
        if not node:
            return False
        targets = []
        for field in ("on_accepted", "on_replied", "on_send_failed", "on_bounced"):
            t = node.get(field)
            if t:
                targets.append(t)
        for timeout_field in ("on_not_accepted_after", "on_no_reply_after"):
            t = node.get(timeout_field)
            if t and t.get("next"):
                targets.append(t["next"])
        return any(can_reach_stop(t, visited_r) for t in targets)
    if nodes:
        if not can_reach_stop(nodes[0]["id"], set()):
            errors.append(f"entry node '{nodes[0]['id']}' has no path to STOP")
    return errors


def get_default_flow(prospect: dict, campaign: dict) -> dict:
    """Choose default flow variant based on prospect data and campaign type."""
    campaign_type = campaign.get("campaign_type") or campaign.get("type", "hybrid")
    if campaign_type in ("email", "email_only"):
        return DEFAULT_FLOW_EMAIL_ONLY
    if campaign_type in ("linkedin", "linkedin_only"):
        return DEFAULT_FLOW_LINKEDIN_ONLY
    # hybrid: prefer LI-first if prospect has linkedin
    has_linkedin = bool(prospect.get("linkedin"))
    has_email = bool(prospect.get("email") or prospect.get("personal_email"))
    if has_linkedin:
        return DEFAULT_FLOW_LINKEDIN_FIRST
    elif has_email:
        return DEFAULT_FLOW_EMAIL_FIRST
    return DEFAULT_FLOW_LINKEDIN_FIRST


# ─── State management ────────────────────────────────────────────────────────

def build_initial_state(enrollment: dict, flow: dict, prospect: dict) -> dict:
    """
    Build the initial flow_state for an enrollment.
    Determines entry channel based on prospect data and flow entry_rules.
    Returns the flow_state dict to be stored on the enrollment.
    """
    nodes = flow.get("nodes", [])
    if not nodes:
        return {"current_node_id": "STOP", "history": [], "started_at": datetime.now(timezone.utc).isoformat()}
    entry_node = nodes[0]
    entry_channel = entry_node.get("channel", "email")
    # Check if prospect can actually use the entry channel; if not, skip to first viable node
    can_use = _prospect_can_use_channel(prospect, entry_channel)
    if not can_use:
        # Find first node whose channel the prospect can use
        for node in nodes:
            if _prospect_can_use_channel(prospect, node.get("channel")):
                entry_node = node
                entry_channel = node.get("channel")
                break
        else:
            return {
                "current_node_id": "STOP",
                "history": [],
                "started_at": datetime.now(timezone.utc).isoformat(),
                "stopped_reason": "no_viable_channel",
            }
    return {
        "current_node_id": entry_node["id"],
        "entry_channel": entry_channel,
        "history": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "next_timeout_check": _compute_timeout_for_node(entry_node),
    }


def _prospect_can_use_channel(prospect: dict, channel: str) -> bool:
    if channel in ("email",):
        return bool(prospect.get("email") or prospect.get("personal_email"))
    if channel in ("linkedin_connection", "linkedin_inmail", "linkedin_message", "linkedin_dm"):
        return bool(prospect.get("linkedin"))
    return True  # voice_note, video_dm, propose_meeting, ai_reply, pause, wait, unknown


def _compute_timeout_for_node(node: dict) -> Optional[str]:
    """Return ISO timestamp for when we should check for no-response timeout, or None."""
    for timeout_field in ("on_not_accepted_after", "on_no_reply_after"):
        t = node.get(timeout_field)
        if t and t.get("days"):
            timeout_dt = datetime.now(timezone.utc) + timedelta(days=t["days"])
            return timeout_dt.isoformat()
    return None


def transition(
    flow_state: dict,
    flow: dict,
    event: str,
    prospect: dict,
    now: Optional[datetime] = None,
) -> dict:
    """
    Given current flow_state, an event, and the flow definition, return the updated flow_state.
    Also returns 'next_action_at' (ISO string) if a new action is needed, or None if STOP.

    event is one of: sent, accepted, replied, bounced, send_failed, no_response_timeout
    """
    if now is None:
        now = datetime.now(timezone.utc)
    current_node_id = flow_state.get("current_node_id", "STOP")
    if current_node_id == "STOP":
        return {**flow_state, "stopped": True}
    nodes = {n["id"]: n for n in flow.get("nodes", [])}
    current_node = nodes.get(current_node_id)
    if not current_node:
        return {**flow_state, "current_node_id": "STOP", "stopped": True}
    # Determine next node based on event
    next_node_id = _resolve_transition(current_node, event)
    # Record history
    history = list(flow_state.get("history", []))
    history.append({
        "node_id": current_node_id,
        "event": event,
        "at": now.isoformat(),
    })
    if next_node_id == "STOP" or next_node_id is None:
        return {
            **flow_state,
            "current_node_id": "STOP",
            "history": history,
            "stopped": True,
            "stopped_at": now.isoformat(),
            "next_action_at": None,
        }
    next_node = nodes.get(next_node_id)
    if not next_node:
        return {**flow_state, "current_node_id": "STOP", "history": history, "stopped": True}
    # Check if prospect can use next channel; if not, recurse with send_failed
    if not _prospect_can_use_channel(prospect, next_node.get("channel")):
        # Auto-skip: treat as send_failed on the next node
        skip_state = {**flow_state, "current_node_id": next_node_id, "history": history}
        return transition(skip_state, flow, "send_failed", prospect, now)
    delay_days = next_node.get("delay_days", 0)
    delay_hours = next_node.get("delay_hours", 0)
    next_action_at = (now + timedelta(days=delay_days, hours=delay_hours)).isoformat()
    timeout_check = _compute_timeout_for_node(next_node)
    return {
        **flow_state,
        "current_node_id": next_node_id,
        "history": history,
        "next_action_at": next_action_at,
        "next_timeout_check": timeout_check,
        "stopped": False,
    }


def _resolve_transition(node: dict, event: str) -> Optional[str]:
    """Map an event to the next node id (or 'STOP' or None)."""
    if event == "replied":
        return node.get("on_replied", "STOP")
    if event == "accepted":
        return node.get("on_accepted", "STOP")
    if event == "bounced":
        return node.get("on_bounced", "STOP")
    if event == "send_failed":
        return node.get("on_send_failed", "STOP")
    if event == "no_response_timeout":
        # Check which timeout applies
        for timeout_field in ("on_not_accepted_after", "on_no_reply_after"):
            t = node.get(timeout_field)
            if t:
                return t.get("next", "STOP")
        return "STOP"
    if event == "sent":
        # After sending, stay on current node (waiting for response/timeout)
        return None  # None means "stay on this node, no new action_at"
    return "STOP"


def get_current_node(flow_state: dict, flow: dict) -> Optional[dict]:
    """Return the current node dict, or None if STOP/missing."""
    current_id = flow_state.get("current_node_id")
    if not current_id or current_id == "STOP":
        return None
    return next((n for n in flow.get("nodes", []) if n["id"] == current_id), None)


def is_stopped(flow_state: dict) -> bool:
    return flow_state.get("current_node_id") == "STOP" or flow_state.get("stopped", False)
