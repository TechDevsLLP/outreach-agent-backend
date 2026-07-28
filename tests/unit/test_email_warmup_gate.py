"""Email is opt-in per mailbox: nothing sends until the user confirms warm-up."""
import pytest
from bson import ObjectId

from services.campaign_launch_service import plan_channel_assignments
from services.email_delivery_service import _warmup_ok
from services.email_warmup_gate import is_warmed_up


def _prospect(pid, **kw):
    base = {"_id": pid, "full_name": "Test Person", "title_gate_passed": True}
    base.update(kw)
    return base


def _enrollment(pid):
    return {"_id": ObjectId(), "prospect_id": pid, "campaign_rule_score": 90}


# ── the flag itself ───────────────────────────────────────────────────────────

def test_connected_but_unconfirmed_mailbox_is_not_warmed():
    assert is_warmed_up({"status": "connected"}) is False
    assert is_warmed_up({"status": "connected", "warmup_complete": False}) is False


def test_confirmed_mailbox_is_warmed():
    assert is_warmed_up({"status": "connected", "warmup_complete": True}) is True


def test_disconnected_mailbox_is_never_warmed_even_if_flagged():
    assert is_warmed_up({"status": "error", "warmup_complete": True}) is False
    assert is_warmed_up(None) is False


# ── the delivery guard ────────────────────────────────────────────────────────

def test_delivery_layer_blocks_unwarmed_mailbox():
    assert _warmup_ok({"_id": "x", "status": "connected"}) is False
    assert _warmup_ok({"_id": "x", "status": "connected", "warmup_complete": True}) is True


# ── the planner ───────────────────────────────────────────────────────────────

def test_blocked_campaign_plans_linkedin_only():
    pid = ObjectId()
    prospects = {pid: _prospect(pid, email="a@b.com", linkedin="https://linkedin.com/in/x")}
    campaign = {
        "email_account_id": ObjectId(),
        "linkedin_account_id": ObjectId(),
        "email_warmup_blocked": True,
    }
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert skips == {}
    assert len(assignments) == 1
    _enr, channel, _day = assignments[0]
    assert channel.startswith("linkedin"), "email must not be planned for an unwarmed mailbox"


def test_email_only_prospect_is_skipped_with_the_warmup_reason():
    pid = ObjectId()
    prospects = {pid: _prospect(pid, email="a@b.com")}  # no LinkedIn
    campaign = {"email_account_id": ObjectId(), "email_warmup_blocked": True}
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert assignments == []
    assert skips == {"email_not_warmed": 1}


def test_warmed_campaign_plans_email_again():
    pid = ObjectId()
    prospects = {pid: _prospect(pid, email="a@b.com")}
    campaign = {"email_account_id": ObjectId(), "email_warmup_blocked": False}
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert skips == {}
    assert [ch for _e, ch, _d in assignments] == ["email"]
