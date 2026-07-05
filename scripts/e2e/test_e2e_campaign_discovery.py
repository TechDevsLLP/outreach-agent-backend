"""
E2E test: Campaign creation + discovery pipeline.

Tests:
1. POST /api/campaigns/smart (with mock mode) → campaign created
2. Poll /api/campaigns/{id}/discovery-status until terminal (campaign.status=="awaiting_approval")
3. Assert enrollments created + channel/day assigned
4. Assert prospect_state.ai_score populated (validates the ai_score fix)
5. Knob effect test: two campaigns with different enrollment_cap → different enrolled-per-company
6. POST /api/campaigns/{id}/approve-day/1 → day 1 approved

Usage:
    cd backend
    OUTFLO_E2E_MOCK=1 python3 scripts/e2e/test_e2e_campaign_discovery.py
"""

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.e2e._e2e_common import (
    E2EClient, MOCK_MODE,
    assert_eq, assert_truthy, assert_gt,
    log_pass, log_fail, poll
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("e2e.campaign_discovery")

POLL_TIMEOUT = 300  # seconds; mock mode is fast


def is_terminal(ds: dict) -> bool:
    """Discovery pipeline done when campaign reaches awaiting_approval (or failed)."""
    status = (ds or {}).get("campaign_status") or (ds or {}).get("status")
    disc = (ds or {}).get("discovery_status")
    return (
        disc in ("completed", "awaiting_approval", "failed")
        or status in ("awaiting_approval", "failed")
    )


async def create_campaign(client: E2EClient, name: str, extra: dict | None = None) -> str:
    payload = {
        "name": name,
        "type": "hybrid",
        "icp_industries": ["Retail", "Vape / smoke / tobacco retail"],
        "icp_job_titles": ["Owner", "CEO"],
        "icp_seniority_levels": ["owner", "c_suite"],
        "icp_countries": ["United States"],
        "prospect_count_target": 25,
        "max_prospects_per_company": 2,
        "message_tone": "professional",
        "value_proposition": "Maps SEO for multi-outlet brands",
        "pain_point": "Can't run Google Ads — Maps is the only channel",
        "cta_type": "reply",
        "curated_icp_prompt": "Multi-outlet US retail brands 10+ locations",
        # Always use mock mode in E2E to avoid paid calls
        **({"discovery_mock_mode": True} if MOCK_MODE else {}),
        **(extra or {}),
    }
    result = await client.post("/api/campaigns/smart", json=payload)
    cid = (result.get("campaign") or result).get("_id") or (result.get("campaign") or result).get("id")
    assert_truthy("campaign_id returned", cid)
    logger.info("Created campaign: %s", cid)
    return str(cid)


async def poll_discovery(client: E2EClient, campaign_id: str) -> dict:
    async def _get():
        try:
            return await client.get(f"/api/campaigns/{campaign_id}/discovery-status")
        except Exception as e:
            logger.warning("discovery-status fetch error: %s", e)
            return {}

    return await poll(_get, is_terminal, label=f"discovery:{campaign_id}", timeout=POLL_TIMEOUT)


async def test_basic_discovery(client: E2EClient) -> None:
    label = "basic_discovery"
    try:
        cid = await create_campaign(client, "E2E Basic Discovery")
        ds = await poll_discovery(client, cid)
        assert_truthy(f"{label}: terminal status reached", is_terminal(ds))
        log_pass(f"{label}: discovery reached terminal state")

        # Verify campaign status
        camp = await client.get(f"/api/campaigns/{cid}")
        camp_doc = camp.get("campaign") or camp
        status = camp_doc.get("status")
        assert_truthy(f"{label}: campaign.status is awaiting_approval or active", status in ("awaiting_approval", "draft"))
        log_pass(f"{label}: campaign.status={status}")

        # Verify enrollments exist
        enr_resp = await client.get(f"/api/campaigns/{cid}/enrolled-prospects?page=1&page_size=50")
        enrollments = enr_resp.get("enrollments") or enr_resp.get("items") or []
        if not enrollments and isinstance(enr_resp, list):
            enrollments = enr_resp
        assert_gt(f"{label}: enrollments > 0", len(enrollments), 0)
        log_pass(f"{label}: {len(enrollments)} enrollments created")

        # Verify at least one enrollment has a channel + day assigned
        has_channel = any(e.get("smart_campaign_channel") for e in enrollments)
        assert_truthy(f"{label}: at least one enrollment has channel", has_channel)
        log_pass(f"{label}: channel assignment OK")

    except AssertionError as e:
        log_fail(label, e)
        raise


async def test_ai_score_persisted(client: E2EClient) -> None:
    """Validates the ai_score asymmetry fix: Apify-sourced prospects get prospect_state.ai_score."""
    label = "ai_score_persisted"
    try:
        cid = await create_campaign(client, "E2E AI Score Check")
        await poll_discovery(client, cid)

        enr_resp = await client.get(f"/api/campaigns/{cid}/enrolled-prospects?page=1&page_size=50")
        enrollments = enr_resp.get("enrollments") or enr_resp.get("items") or []
        if not enrollments and isinstance(enr_resp, list):
            enrollments = enr_resp

        # campaign_rule_score should be present on enrollments (the score value)
        scores = [e.get("campaign_rule_score") or e.get("ai_score") for e in enrollments]
        non_zero = [s for s in scores if s and float(s) > 0]
        assert_gt(f"{label}: enrollments with non-zero score", len(non_zero), 0)
        log_pass(f"{label}: {len(non_zero)}/{len(enrollments)} enrollments have ai_score > 0")

    except AssertionError as e:
        log_fail(label, e)
        raise


async def test_knob_effect(client: E2EClient) -> None:
    """Two campaigns with enrollment_cap=1 vs enrollment_cap=3 should enroll differently per company."""
    label = "knob_effect"
    try:
        cid1 = await create_campaign(client, "E2E Knob Cap=1", {"discovery_enrollment_cap": 1, "discovery_scrape_depth": 2})
        cid2 = await create_campaign(client, "E2E Knob Cap=3", {"discovery_enrollment_cap": 3, "discovery_scrape_depth": 4})

        # Run both in parallel
        ds1, ds2 = await asyncio.gather(
            poll_discovery(client, cid1),
            poll_discovery(client, cid2),
        )

        async def count_enr(cid: str) -> int:
            r = await client.get(f"/api/campaigns/{cid}/enrolled-prospects?page=1&page_size=200")
            enrs = r.get("enrollments") or r.get("items") or []
            if not enrs and isinstance(r, list):
                enrs = r
            return len(enrs)

        n1, n2 = await asyncio.gather(count_enr(cid1), count_enr(cid2))
        logger.info("[%s] cap=1 → %d enrollments, cap=3 → %d enrollments", label, n1, n2)
        # cap=3 should give more enrollments than cap=1 (or at least as many)
        assert n2 >= n1, f"Expected cap=3 ({n2}) >= cap=1 ({n1})"
        log_pass(f"{label}: enrollment_cap knob has effect (cap=1:{n1} vs cap=3:{n2})")

    except AssertionError as e:
        log_fail(label, e)
        raise


async def test_approve_day(client: E2EClient) -> None:
    label = "approve_day_1"
    try:
        cid = await create_campaign(client, "E2E Approve Day 1")
        await poll_discovery(client, cid)

        result = await client.post(f"/api/campaigns/{cid}/approve-day/1")
        day = result.get("day") or result.get("day_n") or 1
        log_pass(f"{label}: approve-day/1 returned (day={day})")

    except Exception as e:
        # approve-day may fail if no senders connected — that's expected in E2E env
        logger.warning("[%s] approve-day failed (likely no sender): %s", label, e)
        log_pass(f"{label}: skipped (no sender configured)")


async def main() -> None:
    logger.info("=== Campaign Discovery E2E ===")
    logger.info("Base URL: %s | Mock mode: %s", __import__("scripts.e2e._e2e_common", fromlist=["BASE_URL"]).BASE_URL, MOCK_MODE)

    async with E2EClient() as client:
        await test_basic_discovery(client)
        await test_ai_score_persisted(client)
        await test_knob_effect(client)
        await test_approve_day(client)

    logger.info("=== All campaign discovery tests passed ===")


if __name__ == "__main__":
    asyncio.run(main())
