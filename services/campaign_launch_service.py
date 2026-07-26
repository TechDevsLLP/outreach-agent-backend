"""
Campaign launch service.
Extracted from routes/campaigns.py approve-and-launch handler.
Provides run_approve_and_launch() so both the route and auto-launch background task
can share the same business logic.
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta, date, time as dtime
from typing import Optional

import pytz
from bson import ObjectId

import database
from services.daily_cap_service import DEFAULT_CAPS as DEFAULT_DAILY_CAPS, SEND_GAP_BOUNDS

logger = logging.getLogger(__name__)

SEQUENCE_GRAPH_CONTRACT = "sequence_graph_v1"


class SequenceLaunchValidationError(ValueError):
    """Raised before launch when a canonical sequence is missing or invalid."""

    status_code = 409


def ensure_sequence_ready_for_launch(campaign: dict) -> None:
    """Validate the persisted execution graph for canonical multi-touch campaigns.

    Legacy campaigns do not declare a sequence contract and continue through
    their existing execution path. A campaign becomes canonical when creation
    stores ``sequence_contract=sequence_graph_v1`` or when it already carries a
    ``sequence_graph`` field. The explicit marker makes a later missing graph a
    launch-blocking data error instead of silently falling back to another flow.
    """
    uses_sequence_graph = (
        campaign.get("sequence_contract") == SEQUENCE_GRAPH_CONTRACT
        or "sequence_graph" in campaign
    )
    if not uses_sequence_graph:
        return

    graph = campaign.get("sequence_graph")
    if not graph:
        raise SequenceLaunchValidationError(
            "Campaign sequence is not saved. Open the Sequence tab, save a valid "
            "sequence, and try launching again."
        )

    from services.sequence_service import validate_sequence_graph

    errors = validate_sequence_graph(graph)
    if errors:
        raise SequenceLaunchValidationError(
            f"Campaign sequence is invalid: {errors[0]} Open the Sequence tab, "
            "fix and save the sequence, then try launching again."
        )

# Seniority levels that qualify for InMail
_INMAIL_SENIORITY = {
    "c_suite", "c-suite", "csuite",
    "founder", "owner", "partner",
    "vp", "director", "head",
    "chief", "president",
}


def compute_day1_preview(campaign: dict) -> tuple[date, bool]:
    """
    Compute the Day-1 send date (and whether it is today) using the same logic
    as run_approve_and_launch, without mutating anything. Used by the UI so it
    can show the user *when* their messages will start going out.

    If the campaign already has `launch_day1_date` stored (post-launch), that
    value wins — otherwise we compute the preview "would-be" day-1.

    Rule: if now is within the send window on a send-eligible day, day-1 is
    today. Otherwise day-1 is the next send-eligible day.
    """
    send_days = campaign.get("send_days") or [
        "monday", "tuesday", "wednesday", "thursday", "friday",
    ]
    send_hour_end = campaign.get("send_hour_end", 17)
    campaign_tz_str = campaign.get("timezone", "America/New_York")

    stored = campaign.get("launch_day1_date")
    if stored:
        try:
            if isinstance(stored, str):
                parsed = date.fromisoformat(stored[:10])
            elif hasattr(stored, "date"):
                parsed = stored.date()
            else:
                parsed = stored
            today_local = _local_today(campaign_tz_str)
            return parsed, (parsed == today_local)
        except Exception:
            pass

    try:
        campaign_tz = pytz.timezone(campaign_tz_str)
        now_local = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(campaign_tz)
    except Exception:
        now_local = datetime.utcnow().replace(tzinfo=pytz.utc)

    today_local = now_local.date()

    send_days_lower = [d.lower() for d in send_days]
    if now_local.hour >= send_hour_end or today_local.strftime("%A").lower() not in send_days_lower:
        candidate = today_local + timedelta(days=1)
        for _ in range(7):
            if candidate.strftime("%A").lower() in send_days_lower:
                break
            candidate += timedelta(days=1)
        return candidate, False

    return today_local, True


def _local_today(tz_str: str) -> date:
    try:
        tz = pytz.timezone(tz_str)
        return datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(tz).date()
    except Exception:
        return datetime.utcnow().date()


# ═══════════════════════════════════════════════════════════════════════════════
# Pure planners — no DB mutations. Used by discovery (plan all days upfront)
# and by per-day approval (recompute timing when the user actually approves).
# ═══════════════════════════════════════════════════════════════════════════════

def plan_channel_assignments(
    campaign: dict,
    enrollments: list[dict],
    prospects_by_id: dict,
    start_day: int = 1,
    min_score: float | None = None,
    existing_counts: dict[tuple[int, str], int] | None = None,
) -> tuple[list[tuple[dict, str, int]], dict[str, int]]:
    """
    Assign a channel + send_day to every enrollment without setting any times.

    Uses channel-first allocation: fills InMail slots first (fewest/day), then
    connection slots, then email slots — ensuring the scarcest channel is used
    whenever eligible prospects are available. Within each channel, prospects
    are claimed in descending score order.

    No send-time math happens here — that's deferred until approve-day, so
    "Day 2 = tomorrow from approval" works even if the user waits 5 days
    to approve Day 2.

    start_day: first day number to assign (default 1). Use a higher value when
    re-planning only the unapproved tail of a campaign (e.g. after removing a
    prospect from day N, re-plan days N onward starting at N).

    Returns: ([(enrollment_dict, channel, send_day), ...], skip_reasons_dict)
    """
    has_email_account = bool(campaign.get("email_account_id"))
    has_linkedin_account = bool(campaign.get("linkedin_account_id"))

    caps = campaign.get("daily_caps") or DEFAULT_DAILY_CAPS
    for _k, _v in DEFAULT_DAILY_CAPS.items():
        if _k not in caps:
            caps[_k] = _v

    def get_score(e):
        # Prefer the campaign-scoped rule score stamped on the enrollment
        # during discovery. Fall back to prospect-level AI / generic scores
        # for older enrollments that predate the rule-based flow.
        rs = e.get("campaign_rule_score")
        if rs is not None:
            return float(rs)
        p = prospects_by_id.get(e["prospect_id"], {})
        return p.get("ai_prospect_score") or p.get("prospect_score") or 0

    # Channel-planning enrollment floor. When a caller doesn't pass min_score,
    # use the same per-campaign override + 25 default that finalize_channel_plan
    # applies. Keeping this explicit stops callers that omit min_score (e.g.
    # replan_channels_on_sender_add) from silently dropping every prospect
    # scoring below the campaign's own floor.
    _min_enroll = (
        min_score if min_score is not None
        else float(campaign.get("discovery_min_enroll_score") or 25)
    )

    def _channel_ok(enrollment: dict, channel: str) -> bool:
        """Return True if this enrollment can use the given channel (ignoring caps)."""
        prospect = prospects_by_id.get(enrollment["prospect_id"], {})
        has_email = bool(prospect.get("email")) and has_email_account
        has_linkedin = bool(prospect.get("linkedin")) and has_linkedin_account
        seniority = (prospect.get("seniority") or prospect.get("seniority_level") or "").lower()
        if channel == "email":
            return has_email
        if channel == "linkedin_connection":
            return has_linkedin
        if channel == "linkedin_inmail":
            return has_linkedin and seniority in _INMAIL_SENIORITY
        return False

    enrollments_sorted = sorted(enrollments, key=get_score, reverse=True)

    assignments: list[tuple[dict, str, int]] = []
    skip_reasons: dict[str, int] = {}
    day_counts: dict[int, dict[str, int]] = {}

    # Seed per-day channel usage from enrollments already planned by earlier
    # top-up generations so this batch continues from the first day with room
    # instead of re-piling onto day 1 past the caps.
    if existing_counts:
        for (_d, _ch), _n in existing_counts.items():
            bucket = day_counts.setdefault(int(_d), {k: 0 for k in caps})
            bucket[_ch] = bucket.get(_ch, 0) + int(_n)

    # Pre-scan: globally exclude prospects with no valid channel at all.
    # This maintains the same skip_reasons telemetry as before.
    remaining: list[dict] = []
    for enrollment in enrollments_sorted:
        prospect = prospects_by_id.get(enrollment["prospect_id"], {})
        if prospect.get("status") in ("opted_out", "bounced", "disqualified"):
            skip_reasons["terminal_status"] = skip_reasons.get("terminal_status", 0) + 1
            continue
        p_fit = prospect.get("ai_prospect_score") or prospect.get("fit_score") or get_score(enrollment)
        # A prospect that passed the deterministic title/seniority gate
        # during curated discovery (stamped as `title_gate_passed` on the
        # prospect doc — see curated_discovery_service._gate_and_select)
        # must not be dropped merely for scoring below the floor. Scoring
        # still runs and still drives ranking/channel priority above.
        if p_fit < _min_enroll and not prospect.get("title_gate_passed"):
            skip_reasons["below_min_score"] = skip_reasons.get("below_min_score", 0) + 1
            continue
        has_email = bool(prospect.get("email")) and has_email_account
        has_linkedin = bool(prospect.get("linkedin")) and has_linkedin_account
        if not has_email and not has_linkedin:
            skip_reasons["no_contact_info"] = skip_reasons.get("no_contact_info", 0) + 1
            continue
        if not has_email_account and not has_linkedin_account:
            skip_reasons["no_sending_account"] = skip_reasons.get("no_sending_account", 0) + 1
            continue
        remaining.append(enrollment)

    day_cursor = start_day
    while remaining and day_cursor < start_day + 365:
        day_counts.setdefault(day_cursor, {k: 0 for k in caps})
        placed_this_day: set = set()

        # Channel-first: fill InMail (5/day) → connection (20/day) → email (20/day)
        for channel in ("linkedin_inmail", "linkedin_connection", "email"):
            slots_left = caps.get(channel, 0) - day_counts[day_cursor].get(channel, 0)
            if slots_left <= 0:
                continue
            for enrollment in remaining:
                if enrollment["_id"] in placed_this_day:
                    continue
                # Respect a pre-assigned channel from an earlier planning pass
                pinned = enrollment.get("smart_campaign_channel")
                if pinned and pinned != channel:
                    continue
                if not _channel_ok(enrollment, channel):
                    continue
                assignments.append((enrollment, channel, day_cursor))
                placed_this_day.add(enrollment["_id"])
                day_counts[day_cursor][channel] += 1
                slots_left -= 1
                if slots_left == 0:
                    break

        remaining = [e for e in remaining if e["_id"] not in placed_this_day]
        if not placed_this_day:
            # All remaining prospects need a channel that's already saturated;
            # advance the day to open new slots.
            day_cursor += 1
            continue
        if sum(day_counts[day_cursor].values()) >= sum(caps.values()):
            day_cursor += 1

    # Any prospect not placed in 365 days has no compatible channel
    for _ in remaining:
        skip_reasons["channel_mismatch"] = skip_reasons.get("channel_mismatch", 0) + 1

    return assignments, skip_reasons


def compute_send_times_for_day(
    campaign: dict,
    day_enrollments: list[tuple[dict, str]],  # (enrollment, channel)
    target_date: date,
    prospects_by_id: dict,
) -> list[tuple[ObjectId, datetime]]:
    """
    Given a cohort assigned to a specific calendar date, spread them across
    the campaign's send window using per-channel randomized gaps to avoid
    detectable fixed-cadence patterns. Pure function — returns
    [(enrollment_id, scheduled_utc), ...].

    Each channel is scheduled independently: InMail gaps (60-180 min) do not
    interact with email gaps (10-35 min), which is more human-like.
    """
    send_hour_start = campaign.get("send_hour_start", 9)
    send_hour_end = campaign.get("send_hour_end", 17)
    campaign_tz_str = campaign.get("timezone", "America/New_York")
    try:
        campaign_tz = pytz.timezone(campaign_tz_str)
    except Exception:
        campaign_tz = pytz.utc

    window_minutes = max((send_hour_end - send_hour_start) * 60, 1)

    # Group enrollments by channel so each channel gets its own gap clock.
    from collections import defaultdict
    by_channel: dict[str, list[dict]] = defaultdict(list)
    for enrollment, channel in day_enrollments:
        by_channel[channel].append(enrollment)

    results: list[tuple[ObjectId, datetime]] = []

    for channel, enrollments in by_channel.items():
        n = len(enrollments)
        if n == 0:
            continue

        min_gap, max_gap = SEND_GAP_BOUNDS.get(channel, (10, 35))

        # Build random gaps for each send after the first.
        gaps = [random.uniform(min_gap, max_gap) for _ in range(n - 1)]

        # Random initial offset into the window (up to 15 min or 10% of window).
        initial_offset = random.uniform(0, min(15.0, window_minutes * 0.10))
        cumulative = [initial_offset]
        for g in gaps:
            cumulative.append(cumulative[-1] + g)

        # If the last send would exceed the window, compress all offsets proportionally.
        if cumulative[-1] >= window_minutes:
            scale = (window_minutes - 1) / cumulative[-1]
            cumulative = [c * scale for c in cumulative]

        for enrollment, offset_minutes in zip(enrollments, cumulative):
            prospect = prospects_by_id.get(enrollment["prospect_id"], {})
            prospect_tz_str = prospect.get("timezone") or campaign_tz_str
            try:
                prospect_tz = pytz.timezone(prospect_tz_str)
            except Exception:
                prospect_tz = campaign_tz

            local_hour = send_hour_start + int(offset_minutes) // 60
            local_minute = int(offset_minutes) % 60

            try:
                local_dt = prospect_tz.localize(
                    datetime.combine(target_date, dtime(local_hour, local_minute, 0))
                )
                scheduled_utc = local_dt.astimezone(pytz.utc).replace(tzinfo=None)
            except Exception:
                scheduled_utc = datetime.combine(target_date, dtime(local_hour, local_minute))

            results.append((enrollment["_id"], scheduled_utc))

    return results


def next_eligible_day(from_date: date, send_days: list[str]) -> date:
    """Return from_date if it's in send_days, otherwise the next day that is."""
    send_days_lower = [d.lower() for d in (send_days or [])]
    if not send_days_lower:
        return from_date
    candidate = from_date
    for _ in range(14):
        if candidate.strftime("%A").lower() in send_days_lower:
            return candidate
        candidate += timedelta(days=1)
    return from_date


def clamp_to_send_window(dt: datetime, campaign: dict) -> datetime:
    """
    Ensure dt falls within the campaign's send window (send_hour_start..send_hour_end)
    on a send-eligible day (send_days). Rolls dt forward to the next valid moment if needed.

    dt is expected to be a naive UTC datetime. The comparison is done in the campaign timezone.
    Returns a naive UTC datetime.
    """
    send_hour_start = campaign.get("send_hour_start", 9)
    send_hour_end = campaign.get("send_hour_end", 17)
    send_days = campaign.get("send_days") or ["monday", "tuesday", "wednesday", "thursday", "friday"]
    campaign_tz_str = campaign.get("timezone", "America/New_York")

    try:
        campaign_tz = pytz.timezone(campaign_tz_str)
    except Exception:
        campaign_tz = pytz.utc

    # Convert to campaign-local time
    dt_utc = dt.replace(tzinfo=pytz.utc)
    dt_local = dt_utc.astimezone(campaign_tz)

    send_days_lower = [d.lower() for d in send_days]

    # Walk forward until we land on an eligible day+time
    for _ in range(14):  # cap at 2 weeks to avoid infinite loops
        day_name = dt_local.strftime("%A").lower()
        if day_name in send_days_lower:
            if dt_local.hour < send_hour_start:
                # Before window opens — advance to window start
                dt_local = dt_local.replace(hour=send_hour_start, minute=0, second=0, microsecond=0)
                break
            elif dt_local.hour < send_hour_end:
                # Already inside window — no change needed
                break
            else:
                # Past window — advance to next eligible day at window start
                dt_local = (dt_local + timedelta(days=1)).replace(
                    hour=send_hour_start, minute=0, second=0, microsecond=0
                )
        else:
            # Not an eligible day — advance to next day at window start
            dt_local = (dt_local + timedelta(days=1)).replace(
                hour=send_hour_start, minute=0, second=0, microsecond=0
            )

    return dt_local.astimezone(pytz.utc).replace(tzinfo=None)


async def approve_day(campaign: dict, day_n: int) -> dict:
    """
    Approve and schedule a single day's outreach.

    - Day N's calendar date is computed relative to the moment of approval:
      * Day 1 (first-time launch): today if we're still in the send window,
        else the next send-eligible day.
      * Day N≥2: always "tomorrow from now", shifted to the next send-eligible
        day if tomorrow isn't one.
    - All enrollments with smart_campaign_send_day == N get their
      scheduled_utc + next_action_at set, making them visible to the engine.
    - Day 1 approval also flips campaign.status=active + approval_status=launched.
    - Appends N to campaign.approved_send_days.

    Caller is responsible for kicking off Day N+1 message generation (the
    route layer does this so the service stays sync).

    Returns: { approved_day, target_date, total_scheduled, channel_counts }
    """
    campaign_oid = campaign["_id"]

    if not campaign.get("is_smart_campaign"):
        raise ValueError("approve_day is only for smart campaigns")

    ensure_sequence_ready_for_launch(campaign)

    approved_days = set(campaign.get("approved_send_days") or [])
    is_reapproval = day_n in approved_days

    # A sequence-graph campaign's start node id isn't always "n1" (hybrid
    # campaigns declare one start node per route) — derive the real set from
    # the graph instead of hardcoding the classic single-route id.
    from services import sequence_service as seq
    graph = campaign.get("sequence_graph") or {}
    start_node_ids = seq.get_start_node_ids(graph) if graph else []
    message_ready_conditions = [
        {"message_gen_status": "done"},
        {"generated_messages": {"$exists": True, "$ne": None}},
    ] + [
        {f"generated_messages_by_step.{nid}": {"$exists": True}}
        for nid in start_node_ids
    ]

    # Load all enrollments planned for this day that have a message ready.
    enrollment_query: dict = {
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": day_n,
        "status": {"$nin": ["archived", "skipped_no_channel", "pending_teammate_review", "cascade_waiting"]},
        "$or": message_ready_conditions,
    }
    if is_reapproval:
        # Re-approving an already-approved day must be idempotent: only pick up
        # enrollments that missed the first pass (e.g. message-gen failed and
        # was regenerated afterwards) — never re-touch ones already scheduled.
        enrollment_query["next_action_at"] = None

    enrollments = await database.campaign_enrollments_collection.find(
        enrollment_query
    ).to_list(length=5000)

    if not enrollments:
        if is_reapproval:
            raise ValueError(
                f"Day {day_n} is already approved and has no newly-ready "
                f"enrollments to pick up."
            )
        raise ValueError(
            f"No enrollments with generated messages found for Day {day_n}. "
            f"Messages may still be generating — try again in a minute."
        )

    # Batch-fetch prospects for timezone lookup
    prospect_ids = [e["prospect_id"] for e in enrollments]
    prospects_list = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    prospects_by_id = {p["_id"]: p for p in prospects_list}

    # Sort the day's enrollments by score desc so spread-within-day is consistent
    def get_score(e):
        p = prospects_by_id.get(e["prospect_id"], {})
        return p.get("ai_prospect_score") or p.get("prospect_score") or 0
    enrollments_sorted = sorted(enrollments, key=get_score, reverse=True)

    day_enrollments = [
        (enr, enr.get("smart_campaign_channel") or "email")
        for enr in enrollments_sorted
    ]

    # Enforce the per-channel daily cap at schedule-write time. Upstream day
    # bucketing (plan_route_first_touch_days) is *supposed* to keep each
    # send_day within cap, but other enrollment paths (cascade-primary
    # activation, top-up generations) can pin many prospects to a single
    # send_day. Without this guard, approve_day would schedule all of them on
    # one calendar date — e.g. 33 LinkedIn connection requests in a day when the
    # cap is 20. Schedule up to the cap (top-score-first, already sorted) and
    # bump the overflow to the next send day so it flows through the next
    # approval instead of blowing the quota.
    from services.daily_cap_service import DEFAULT_CAPS
    caps = campaign.get("daily_caps") or DEFAULT_CAPS
    placed: list[tuple[dict, str]] = []
    overflow: list[dict] = []
    per_channel_used: dict[str, int] = {}
    for enr, channel in day_enrollments:
        cap = int(caps.get(channel, DEFAULT_CAPS.get(channel, 20)) or 0)
        used = per_channel_used.get(channel, 0)
        if cap > 0 and used >= cap:
            overflow.append(enr)
        else:
            placed.append((enr, channel))
            per_channel_used[channel] = used + 1
    day_enrollments = placed

    # Pick the calendar date. A re-approval must land newly-ready stragglers on
    # the SAME calendar date as the rest of that day's cohort, not a fresh
    # "today"/"tomorrow" computed at the (later) re-approval moment.
    existing_day_approval = (campaign.get("day_approvals") or {}).get(str(day_n))
    if is_reapproval and existing_day_approval and existing_day_approval.get("target_date"):
        target_date = date.fromisoformat(str(existing_day_approval["target_date"])[:10])
    else:
        campaign_tz_str = campaign.get("timezone", "America/New_York")
        send_days = campaign.get("send_days") or ["monday", "tuesday", "wednesday", "thursday", "friday"]
        send_hour_end = campaign.get("send_hour_end", 17)

        try:
            campaign_tz = pytz.timezone(campaign_tz_str)
            now_local = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(campaign_tz)
        except Exception:
            now_local = datetime.utcnow().replace(tzinfo=pytz.utc)
        today_local = now_local.date()

        if day_n == 1:
            send_hour_start = campaign.get("send_hour_start", 9)
            # First launch: today if we're inside the send window on an eligible day
            outside_window = now_local.hour < send_hour_start or now_local.hour >= send_hour_end
            not_send_day = today_local.strftime("%A").lower() not in [d.lower() for d in send_days]
            if outside_window or not_send_day:
                target_date = next_eligible_day(today_local + timedelta(days=1), send_days)
            else:
                target_date = today_local
        else:
            # Day 2+ always shifts to "tomorrow from approval date" (or next eligible day)
            target_date = next_eligible_day(today_local + timedelta(days=1), send_days)

    # Compute per-enrollment send times
    times = compute_send_times_for_day(campaign, day_enrollments, target_date, prospects_by_id)

    from pymongo import UpdateOne
    now = datetime.utcnow()
    bulk_ops = []
    channel_counts: dict[str, int] = {}
    times_by_id = dict(times)

    for enr, channel in day_enrollments:
        scheduled_utc = times_by_id.get(enr["_id"])
        channel_counts[channel] = channel_counts.get(channel, 0) + 1
        bulk_ops.append(UpdateOne(
            {"_id": enr["_id"]},
            {"$set": {
                "smart_campaign_channel": channel,
                "smart_campaign_send_day": day_n,
                "smart_campaign_scheduled_utc": scheduled_utc,
                "next_action_at": scheduled_utc,
                "status": "active",
                "last_activity_at": now,
            }},
        ))

    if bulk_ops:
        await database.campaign_enrollments_collection.bulk_write(bulk_ops, ordered=False)

    # Bump over-cap enrollments to the next send day. They keep their generated
    # messages; clearing the schedule fields makes them re-appear as unscheduled
    # day-(N+1) work for the next approval, which will again cap + bump as needed
    # until everything is scheduled across enough days to respect the quota.
    if overflow:
        # Bump to the first day beyond day_n that isn't already approved — if
        # day_n + 1 was already approved (e.g. by an earlier top-up
        # generation), landing overflow there strands it: that day's approval
        # already ran and nothing re-scans it for newly-added stragglers.
        overflow_target_day = day_n + 1
        while overflow_target_day in approved_days:
            overflow_target_day += 1
        overflow_ops = [
            UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "smart_campaign_send_day": overflow_target_day,
                    "smart_campaign_scheduled_utc": None,
                    "next_action_at": None,
                    "last_activity_at": now,
                }},
            )
            for enr in overflow
        ]
        await database.campaign_enrollments_collection.bulk_write(overflow_ops, ordered=False)
        logger.info(
            f"[approve_day:{campaign_oid}] day {day_n}: scheduled {len(bulk_ops)} within "
            f"caps {dict(per_channel_used)}, bumped {len(overflow)} over-cap enrollments to day {overflow_target_day}"
        )

    # Update campaign state
    campaign_update: dict = {
        "$addToSet": {"approved_send_days": day_n},
        "$set": {
            "updated_at": now,
            f"day_approvals.{day_n}": {
                "approved_at": now,
                "target_date": target_date.isoformat(),
                "total_scheduled": len(bulk_ops),
                "channel_counts": channel_counts,
                "bumped_to_next_day": len(overflow),
            },
        },
    }
    if day_n == 1 and not is_reapproval:
        campaign_update["$set"].update({
            "status": "active",
            "approval_status": "launched",
            "launched_at": now,
            "started_at": now,
            "launch_day1_date": target_date.isoformat(),
        })

    await database.campaigns_collection.update_one({"_id": campaign_oid}, campaign_update)

    return {
        "approved_day": day_n,
        "target_date": target_date.isoformat(),
        "total_scheduled": len(bulk_ops),
        "channel_counts": channel_counts,
    }


async def run_approve_and_launch(campaign: dict, account_id: ObjectId) -> dict:
    """
    Core approve-and-launch business logic.

    Assigns channels and computes timezone-aware send times for all enrolled
    prospects with generated messages, then activates the campaign.

    Args:
        campaign: Full campaign document dict from MongoDB.
        account_id: ObjectId of the owning account.

    Returns:
        dict with keys: launched, total_scheduled, day1_date, schedule_summary, campaign

    Raises:
        ValueError: If no enrollments exist or no prospects can be assigned.
    """
    campaign_oid = campaign["_id"]

    if not campaign.get("is_smart_campaign"):
        raise ValueError("run_approve_and_launch is only for smart campaigns")

    ensure_sequence_ready_for_launch(campaign)

    if campaign.get("message_gen_status") != "completed":
        raise ValueError(
            f"Message generation must be completed before launch "
            f"(current: {campaign.get('message_gen_status', 'idle')})"
        )

    # Determine if this is a v3 sequence campaign (steps have step_id + edges)
    # v2 campaigns also store outreach_sequence as a list but use step_number only
    outreach_sequence = campaign.get("outreach_sequence")
    is_sequence_campaign = bool(
        outreach_sequence
        and isinstance(outreach_sequence, list)
        and len(outreach_sequence) > 0
        and outreach_sequence[0].get("step_id")
    )
    entry_step = None
    if is_sequence_campaign:
        entry_step = next(
            (s for s in outreach_sequence if s.get("is_entry")),
            None,
        )
        if not entry_step:
            entry_step = outreach_sequence[0] if outreach_sequence else None

    # Load all ready enrollments — accept both "done" status and any with generated_messages set
    enrollments = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "status": {"$nin": ["archived", "skipped_no_channel", "pending_teammate_review", "cascade_waiting"]},
        "$or": [
            {"message_gen_status": "done"},
            {"generated_messages": {"$exists": True, "$ne": None}},
        ],
    }).to_list(length=2000)

    if not enrollments:
        total = await database.campaign_enrollments_collection.count_documents({"campaign_id": campaign_oid})
        if total == 0:
            raise ValueError("No prospects enrolled in this campaign")
        raise ValueError(f"No enrollments with generated messages found ({total} enrolled but none with messages)")

    # Batch-fetch prospects
    prospect_ids = [e["prospect_id"] for e in enrollments]
    prospects_list = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=len(prospect_ids))
    prospects_by_id = {p["_id"]: p for p in prospects_list}

    # Sort enrollments by prospect ai_prospect_score descending
    def get_score(e):
        p = prospects_by_id.get(e["prospect_id"], {})
        return p.get("ai_prospect_score") or p.get("prospect_score") or 0

    enrollments_sorted = sorted(enrollments, key=get_score, reverse=True)

    has_email_account = bool(campaign.get("email_account_id"))
    has_linkedin_account = bool(campaign.get("linkedin_account_id"))

    if not has_email_account and not has_linkedin_account:
        raise ValueError(
            "Campaign has no email or LinkedIn account configured – "
            "please connect a sending account before launching"
        )

    # Use campaign-specific daily caps if set, otherwise use defaults
    _DAILY_QUOTAS = campaign.get("daily_caps") or DEFAULT_DAILY_CAPS
    # Ensure all default keys are present (campaign may have partial override)
    for _k, _v in DEFAULT_DAILY_CAPS.items():
        if _k not in _DAILY_QUOTAS:
            _DAILY_QUOTAS[_k] = _v

    day = 1
    day_counts: dict = {1: {k: 0 for k in _DAILY_QUOTAS}}
    assignments = []  # List of (enrollment, channel, day)

    for enrollment in enrollments_sorted:
        prospect = prospects_by_id.get(enrollment["prospect_id"], {})
        has_email = bool(prospect.get("email")) and has_email_account
        has_linkedin = bool(prospect.get("linkedin")) and has_linkedin_account
        seniority = (prospect.get("seniority") or prospect.get("seniority_level") or "").lower()

        # For sequence campaigns: only need to fit the entry step into a daily quota slot
        if is_sequence_campaign and entry_step:
            entry_channel = entry_step.get("channel", "linkedin")
            if entry_channel == "linkedin":
                eligible = ["linkedin_connection"] if has_linkedin else []
            else:
                eligible = ["email"] if has_email else []
        elif enrollment.get("smart_campaign_channel"):
            # Smart campaign with pre-assigned channel (set by build_day1_batch pre-enrichment).
            # Respect it — only schedule timing, don't reassign.
            pre_channel = enrollment["smart_campaign_channel"]
            eligible = [pre_channel]
        else:
            # Fallback: classic smart campaign channel selection
            eligible = []
            if has_email:
                eligible.append("email")
            if has_linkedin:
                inmail_eligible = seniority in _INMAIL_SENIORITY
                if inmail_eligible:
                    eligible.append("linkedin_inmail")
                eligible.append("linkedin_connection")

        if not eligible:
            continue

        assigned = False
        for attempt in range(50):
            current_day = day + attempt
            if current_day not in day_counts:
                day_counts[current_day] = {k: 0 for k in _DAILY_QUOTAS}

            for channel in eligible:
                if day_counts[current_day][channel] < _DAILY_QUOTAS[channel]:
                    day_counts[current_day][channel] += 1
                    assignments.append((enrollment, channel, current_day))
                    assigned = True
                    break

            if assigned:
                total_used = sum(day_counts[current_day].values())
                total_quota = sum(_DAILY_QUOTAS.values())
                if total_used >= total_quota:
                    day = current_day + 1
                break

    if not assignments:
        raise ValueError("Could not assign any prospects to channels")

    # Schedule computation
    send_days = campaign.get("send_days", ["monday", "tuesday", "wednesday", "thursday", "friday"])
    send_hour_start = campaign.get("send_hour_start", 9)
    send_hour_end = campaign.get("send_hour_end", 17)
    campaign_tz_str = campaign.get("timezone", "America/New_York")

    def _next_send_date(from_date: date, n_business_days: int) -> date:
        current = from_date
        days_added = 0
        while days_added < n_business_days:
            current += timedelta(days=1)
            if current.strftime("%A").lower() in send_days:
                days_added += 1
        return current

    now_utc = datetime.utcnow()
    try:
        campaign_tz = pytz.timezone(campaign_tz_str)
        now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(campaign_tz)
    except Exception:
        campaign_tz = pytz.utc
        now_local = now_utc.replace(tzinfo=pytz.utc)

    today_local = now_local.date()
    if now_local.hour >= send_hour_end or today_local.strftime("%A").lower() not in send_days:
        candidate = today_local + timedelta(days=1)
        for _ in range(7):
            if candidate.strftime("%A").lower() in send_days:
                break
            candidate += timedelta(days=1)
        day1_date = candidate
    else:
        day1_date = today_local

    by_day: dict = {}
    for enrollment, channel, send_day in assignments:
        by_day.setdefault(send_day, []).append((enrollment, channel))

    from pymongo import UpdateOne
    bulk_ops = []
    schedule_summary: dict = {}

    for send_day, day_assignments in sorted(by_day.items()):
        if send_day == 1:
            target_date = day1_date
        else:
            target_date = _next_send_date(day1_date, send_day - 1)

        total_in_day = len(day_assignments)
        window_minutes = (send_hour_end - send_hour_start) * 60

        day_summary = {k: 0 for k in _DAILY_QUOTAS}
        day_summary["date"] = target_date.isoformat()

        for idx, (enrollment, channel) in enumerate(day_assignments):
            prospect = prospects_by_id.get(enrollment["prospect_id"], {})
            prospect_tz_str = prospect.get("timezone") or campaign_tz_str
            try:
                prospect_tz = pytz.timezone(prospect_tz_str)
            except Exception:
                prospect_tz = campaign_tz

            position = idx / max(total_in_day, 1)
            offset_minutes = int(position * window_minutes)
            jitter = random.randint(-15, 15)
            offset_minutes = max(0, min(offset_minutes + jitter, window_minutes - 1))

            local_hour = send_hour_start + offset_minutes // 60
            local_minute = offset_minutes % 60

            try:
                local_dt = prospect_tz.localize(
                    datetime.combine(target_date, dtime(local_hour, local_minute, 0))
                )
                next_action_at = local_dt.astimezone(pytz.utc).replace(tzinfo=None)
            except Exception:
                next_action_at = datetime.combine(target_date, dtime(local_hour, local_minute))

            if is_sequence_campaign and entry_step:
                # Sequence campaign (v3): set entry step ID instead of fixed channel
                bulk_ops.append(UpdateOne(
                    {"_id": enrollment["_id"]},
                    {"$set": {
                        "current_step_id": entry_step["step_id"],
                        "smart_campaign_send_day": send_day,
                        "smart_campaign_scheduled_utc": next_action_at,
                        "next_action_at": next_action_at,
                        "status": "active",
                        "auto_respond_mode": True,
                        "last_activity_at": datetime.utcnow(),
                    }},
                ))
            else:
                # Classic smart campaign (v2): set fixed channel
                bulk_ops.append(UpdateOne(
                    {"_id": enrollment["_id"]},
                    {"$set": {
                        "smart_campaign_channel": channel,
                        "smart_campaign_send_day": send_day,
                        "smart_campaign_scheduled_utc": next_action_at,
                        "next_action_at": next_action_at,
                        "status": "active",
                        "last_activity_at": datetime.utcnow(),
                    }},
                ))

            day_summary[channel] = day_summary.get(channel, 0) + 1

        schedule_summary[f"day_{send_day}"] = day_summary

    if bulk_ops:
        await database.campaign_enrollments_collection.bulk_write(bulk_ops, ordered=False)

    # Activate campaign
    now = datetime.utcnow()
    await database.campaigns_collection.update_one(
        {"_id": campaign_oid},
        {"$set": {
            "status": "active",
            "approval_status": "launched",
            "launched_at": now,
            "started_at": now,
            "launch_day1_date": day1_date.isoformat(),
            "updated_at": now,
        }},
    )

    updated_campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})

    return {
        "launched": True,
        "total_scheduled": len(assignments),
        "day1_date": day1_date.isoformat(),
        "schedule_summary": schedule_summary,
        "campaign": updated_campaign,
    }


async def replan_channels_on_sender_add(account_id: str, sender_type: str) -> None:
    """
    Re-run channel planning for completed campaigns that have skipped_no_channel
    enrollments. Call as a BackgroundTask when a new email or LinkedIn account is
    connected. sender_type: "email" | "linkedin"
    """
    from pymongo import UpdateOne
    from services.flow_engine import build_initial_state, get_default_flow

    campaigns = await database.campaigns_collection.find(
        {"account_id": account_id, "discovery_status": "completed"},
    ).to_list(length=50)

    for campaign in campaigns:
        campaign_oid = campaign["_id"]
        campaign_id = str(campaign_oid)

        skip_count = await database.campaign_enrollments_collection.count_documents(
            {"campaign_id": campaign_oid, "status": "skipped_no_channel"}
        )
        if skip_count == 0:
            continue

        # Auto-link the new sender to the campaign if not already set
        if sender_type == "email" and not campaign.get("email_account_id"):
            ea = await database.email_accounts_collection.find_one(
                {"account_id": account_id, "status": {"$in": ["connected", "active"]}},
                sort=[("created_at", -1)],
            )
            if ea:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid}, {"$set": {"email_account_id": ea["_id"]}}
                )
                campaign["email_account_id"] = ea["_id"]
        elif sender_type == "linkedin" and not campaign.get("linkedin_account_id"):
            la = await database.linkedin_accounts_collection.find_one(
                {"account_id": account_id, "unipile_status": {"$in": ["OK", "CONNECTING"]}},
                sort=[("created_at", -1)],
            )
            if la:
                await database.campaigns_collection.update_one(
                    {"_id": campaign_oid}, {"$set": {"linkedin_account_id": la["_id"]}}
                )
                campaign["linkedin_account_id"] = la["_id"]

        # Fetch all non-terminal enrollments so the planner sees correct day caps
        all_enrs = await database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid, "status": {"$in": ["active", "skipped_no_channel"]}},
        ).to_list(length=5000)
        if not all_enrs:
            continue

        pids = list({e["prospect_id"] for e in all_enrs})
        prospect_docs = await database.prospects_collection.find(
            {"_id": {"$in": pids}}
        ).to_list(length=len(pids))
        prospects_by_id = {p["_id"]: p for p in prospect_docs}

        skipped_ids = {e["_id"] for e in all_enrs if e.get("status") == "skipped_no_channel"}
        assignments, _ = plan_channel_assignments(campaign, all_enrs, prospects_by_id)
        newly_assigned = [(enr, ch, sd) for enr, ch, sd in assignments if enr["_id"] in skipped_ids]

        if not newly_assigned:
            continue

        plan_ops = [
            UpdateOne(
                {"_id": enr["_id"]},
                {"$set": {
                    "smart_campaign_channel": channel,
                    "smart_campaign_send_day": send_day,
                    "status": "active",
                    "smart_campaign_scheduled_utc": None,
                    "message_gen_status": "pending",
                }},
            )
            for enr, channel, send_day in newly_assigned
        ]
        await database.campaign_enrollments_collection.bulk_write(plan_ops, ordered=False)

        refreshed = await database.campaigns_collection.find_one({"_id": campaign_oid})
        _flow = (refreshed or {}).get("follow_up_flow") or get_default_flow({}, refreshed or {})

        seed_ops = []
        for enr, _ch, _sd in newly_assigned:
            p = prospects_by_id.get(enr.get("prospect_id"), {})
            state = build_initial_state(enr, _flow, p)
            if state.get("current_node_id") == "STOP" and state.get("stopped_reason") == "no_viable_channel":
                seed_ops.append(UpdateOne(
                    {"_id": enr["_id"]},
                    {"$set": {"status": "skipped_no_channel", "flow_state": state}},
                ))
            else:
                seed_ops.append(UpdateOne(
                    {"_id": enr["_id"]},
                    {"$set": {"flow_state": state, "current_step_index": 1}},
                ))
        if seed_ops:
            await database.campaign_enrollments_collection.bulk_write(seed_ops, ordered=False)

        logger.info(
            f"[replan_on_sender_add] Campaign {campaign_id}: promoted {len(newly_assigned)} "
            f"skipped_no_channel enrollments after {sender_type} sender connected"
        )
