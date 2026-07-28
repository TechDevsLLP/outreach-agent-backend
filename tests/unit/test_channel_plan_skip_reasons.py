"""Skip reasons drive the failure message users see when a campaign enrolls 0.

Regression: a new user with no sending account connected had every prospect
counted as `no_contact_info`, so the UI told them to broaden their ICP when the
real fix was "connect LinkedIn or email".
"""
from bson import ObjectId

from services.campaign_launch_service import plan_channel_assignments


def _enrollment(pid, score=90):
    return {"_id": ObjectId(), "prospect_id": pid, "campaign_rule_score": score}


def _prospect(**kw):
    base = {"_id": kw.pop("_id"), "full_name": "Test Person", "title_gate_passed": True}
    base.update(kw)
    return base


def test_no_sending_account_is_reported_when_prospects_are_contactable():
    pid = ObjectId()
    prospects = {pid: _prospect(_id=pid, linkedin="https://linkedin.com/in/x")}
    assignments, skips = plan_channel_assignments({}, [_enrollment(pid)], prospects)

    assert assignments == []
    assert skips == {"no_sending_account": 1}


def test_no_contact_info_only_when_the_prospect_really_has_none():
    pid = ObjectId()
    prospects = {pid: _prospect(_id=pid)}
    campaign = {"linkedin_account_id": ObjectId()}
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert assignments == []
    assert skips == {"no_contact_info": 1}


def test_channel_mismatch_when_contact_channel_has_no_account():
    pid = ObjectId()
    prospects = {pid: _prospect(_id=pid, linkedin="https://linkedin.com/in/x")}
    campaign = {"email_account_id": ObjectId()}  # email only, prospect is LinkedIn-only
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert assignments == []
    assert skips == {"channel_mismatch": 1}


def test_contactable_prospect_is_scheduled_when_the_channel_is_connected():
    pid = ObjectId()
    prospects = {pid: _prospect(_id=pid, linkedin="https://linkedin.com/in/x")}
    campaign = {"linkedin_account_id": ObjectId()}
    assignments, skips = plan_channel_assignments(campaign, [_enrollment(pid)], prospects)

    assert skips == {}
    assert len(assignments) == 1
    _enr, channel, day = assignments[0]
    assert channel.startswith("linkedin")
    assert day == 1
