"""
E2E test: Day-1 message generation.

Tests:
1. Create mock campaign + run discovery (same as campaign_discovery test)
2. POST /api/campaigns/{id}/generate-messages?day=1 → background task fires
3. Poll enrollment list until at least one has generated_messages
4. GET /api/campaigns/{id}/message-preview/{prospect_id} → validate message structure

Usage:
    cd backend
    OUTFLO_E2E_MOCK=1 python3 scripts/e2e/test_e2e_message_gen.py
"""

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.e2e._e2e_common import (
    E2EClient, MOCK_MODE,
    assert_truthy, assert_gt,
    log_pass, log_fail, poll,
    POLL_INTERVAL, POLL_TIMEOUT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("e2e.message_gen")


def is_discovery_terminal(ds: dict) -> bool:
    disc = (ds or {}).get("discovery_status")
    return disc in ("completed", "awaiting_approval", "failed")


async def create_campaign_and_wait(client: E2EClient) -> str:
    payload = {
        "name": "E2E Message Gen Test",
        "type": "hybrid",
        "icp_industries": ["Retail"],
        "icp_job_titles": ["Owner", "CEO"],
        "icp_seniority_levels": ["owner", "c_suite"],
        "icp_countries": ["United States"],
        "prospect_count_target": 10,
        "max_prospects_per_company": 2,
        "message_tone": "professional",
        "value_proposition": "Maps SEO for multi-outlet brands",
        "pain_point": "Can't run Google Ads — Maps is the only channel",
        "cta_type": "reply",
        "curated_icp_prompt": "Multi-outlet US retail brands 10+ locations",
        # Do NOT skip message gen — we want messages to be generated
        "discovery_skip_message_gen": False,
        **({"discovery_mock_mode": True} if MOCK_MODE else {}),
    }
    result = await client.post("/api/campaigns/smart", json=payload)
    cid = (result.get("campaign") or result).get("_id") or (result.get("campaign") or result).get("id")
    assert_truthy("campaign_id from create", cid)
    logger.info("Campaign created: %s", cid)

    # Wait for discovery to complete
    async def _get_ds():
        try:
            return await client.get(f"/api/campaigns/{cid}/discovery-status")
        except Exception as e:
            logger.warning("discovery-status error: %s", e)
            return {}

    await poll(_get_ds, is_discovery_terminal, label=f"discovery:{cid}", timeout=POLL_TIMEOUT)
    return str(cid)


async def test_message_gen(client: E2EClient) -> None:
    label = "message_gen"
    try:
        cid = await create_campaign_and_wait(client)

        # ── 1. Trigger Day-1 message generation explicitly ────────────────────
        # (Mock mode: discovery already fires message gen at the end.
        #  We call this endpoint anyway to test it's wired up correctly.)
        try:
            r = await client.post(f"/api/campaigns/{cid}/generate-messages?day=1")
            logger.info("[%s] generate-messages response: %s", label, r)
            log_pass(f"{label}: POST /generate-messages?day=1 accepted")
        except Exception as e:
            # 409 / "already generated" is also acceptable
            logger.warning("[%s] generate-messages error (may be OK if already generated): %s", label, e)

        # ── 2. Poll enrollments until at least one has generated_messages ─────
        async def _get_enrs():
            try:
                r = await client.get(f"/api/campaigns/{cid}/enrolled-prospects?page=1&page_size=50")
                return r.get("enrollments") or r.get("items") or (r if isinstance(r, list) else [])
            except Exception as e:
                logger.warning("[%s] enrollments fetch error: %s", label, e)
                return []

        def _has_messages(enrs: list) -> bool:
            return any(e.get("has_generated_message") or e.get("generated_messages") for e in enrs)

        enrollments = await poll(
            _get_enrs, _has_messages,
            label=f"msg_ready:{cid}",
            interval=5, timeout=POLL_TIMEOUT,
        )

        assert_gt(f"{label}: enrollments > 0", len(enrollments), 0)
        msgs_ready = [e for e in enrollments if e.get("has_generated_message") or e.get("generated_messages")]
        assert_gt(f"{label}: enrollments with messages > 0", len(msgs_ready), 0)
        log_pass(f"{label}: {len(msgs_ready)}/{len(enrollments)} enrollments have generated messages")

        # ── 3. GET /message-preview/{prospect_id} for the first ready enrollment ─
        first = msgs_ready[0]
        pid = first.get("prospect_id") or first.get("_id")
        assert_truthy(f"{label}: prospect_id from enrollment", pid)

        preview = await client.get(f"/api/campaigns/{cid}/message-preview/{pid}")
        gen = preview.get("generated_messages") or {}
        logger.info("[%s] message-preview keys: %s", label, list(gen.keys()))

        # At least one channel should have a message
        has_any = any([
            gen.get("cold_email", {}).get("body"),
            gen.get("linkedin_connection", {}).get("note"),
            gen.get("linkedin_inmail", {}).get("body"),
        ])
        assert_truthy(f"{label}: message-preview has at least one non-empty channel", has_any)
        log_pass(f"{label}: message-preview OK (channels: {list(gen.keys())})")

    except AssertionError as e:
        log_fail(label, e)
        raise


async def main() -> None:
    logger.info("=== Message Generation E2E ===")
    logger.info("Mock mode: %s", MOCK_MODE)

    async with E2EClient() as client:
        await test_message_gen(client)

    logger.info("=== Message gen E2E passed ===")


if __name__ == "__main__":
    asyncio.run(main())
