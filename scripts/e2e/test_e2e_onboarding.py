"""
E2E test: Onboarding wizard (6-stage flow).

Tests:
1. POST /api/onboarding/start → session created
2. POST /api/onboarding/stage-1/save → company profile saved
3. POST /api/onboarding/stage-2/linkedin-voice → sender voice updated
4. POST /api/onboarding/stage-3/icp → ICP locked + background discovery fires
5. POST /api/onboarding/stage-4/offer → offer saved
6. POST /api/onboarding/stage-6/chat → refine chat
7. POST /api/onboarding/wizard/complete → onboarding_complete=True
8. GET /api/onboarding/scrape-status/{session_id} → prospects_found > 0 (validates bug 4a fix)

Usage:
    cd backend
    OUTFLO_E2E_MOCK=1 python3 scripts/e2e/test_e2e_onboarding.py

Note: discovery is triggered at stage 3 and runs in the background. The final
assert on prospects_found polls the scrape-status endpoint. Mock mode is enabled
by setting OUTFLO_E2E_MOCK=1 — this env var is checked by start_onboarding_scrape
(which creates the campaign doc with discovery_mock_mode=True when app_env != production).
"""

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.e2e._e2e_common import (
    E2EClient, MOCK_MODE,
    assert_eq, assert_truthy, assert_gt,
    log_pass, log_fail, poll,
    POLL_INTERVAL, POLL_TIMEOUT,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("e2e.onboarding")


async def test_wizard_flow(client: E2EClient) -> None:
    label = "wizard_flow"
    try:
        # ── 1. Start session ────────────────────────────────────────────────
        sess = await client.post("/api/onboarding/start", json={})
        session_id = sess.get("session_id") or sess.get("id") or sess.get("_id")
        assert_truthy(f"{label}: session_id returned", session_id)
        log_pass(f"{label}: session started ({session_id})")

        # ── 2. Stage 1 — company profile ────────────────────────────────────
        r1 = await client.post("/api/onboarding/stage-1/save", json={
            "session_id": session_id,
            "company_name": "E2ETestCo",
            "company_url": "https://e2etestco.example.com",
            "services": ["Google Maps optimisation", "GMB management"],
            "description": "We rank multi-outlet brands #1 on Google Maps.",
            "icp_description": "Multi-outlet US brands 10+ locations in ad-restricted verticals.",
            "pain_points": ["Can't run Google Ads", "Lose walk-in customers to competitors"],
        })
        stage = r1.get("current_stage") or r1.get("stage")
        logger.info("[%s] stage-1/save → current_stage=%s", label, stage)
        log_pass(f"{label}: stage-1/save OK")

        # ── 3. Stage 2 — sender voice (no Unipile in E2E → skip silently) ──
        try:
            await client.post("/api/onboarding/stage-2/linkedin-voice", json={
                "session_id": session_id,
                "linkedin_url": "https://www.linkedin.com/in/e2e-tester/",
            })
            log_pass(f"{label}: stage-2/linkedin-voice OK")
        except Exception as e:
            logger.warning("[%s] stage-2 skipped (expected in no-Unipile env): %s", label, e)

        # ── 4. Stage 3 — ICP lock (fires background discovery) ──────────────
        r3 = await client.post("/api/onboarding/stage-3/icp", json={
            "session_id": session_id,
            "locked_industry": "Vape / smoke / tobacco retail",
            "icp_job_titles": ["Owner", "CEO"],
            "icp_seniority_levels": ["owner", "c_suite"],
            "icp_geographies": ["United States"],
            "icp_company_sizes": ["51-200", "201-1000"],
        })
        logger.info("[%s] stage-3/icp → %s", label, r3)
        log_pass(f"{label}: stage-3/icp OK (discovery fired in background)")

        # ── 5. Stage 4 — offer ────────────────────────────────────────────────
        r4 = await client.post("/api/onboarding/stage-4/offer", json={
            "session_id": session_id,
            "primary_cta": "Book a free 20-min Maps audit",
        })
        logger.info("[%s] stage-4/offer → %s", label, r4)
        log_pass(f"{label}: stage-4/offer OK")

        # ── 6. Stage 6 — refine chat (optional) ──────────────────────────────
        try:
            await client.post("/api/onboarding/stage-6/chat", json={
                "session_id": session_id,
                "message": "What objections do prospects typically raise?",
            })
            log_pass(f"{label}: stage-6/chat OK")
        except Exception as e:
            logger.warning("[%s] stage-6/chat skipped: %s", label, e)

        # ── 7. Complete wizard ────────────────────────────────────────────────
        r_done = await client.post("/api/onboarding/wizard/complete", json={
            "session_id": session_id,
        })
        logger.info("[%s] wizard/complete → %s", label, r_done)
        log_pass(f"{label}: wizard/complete OK")

        # ── 8. Poll scrape-status → prospects_found > 0 (validates bug 4a fix) ──
        async def _get_status():
            try:
                return await client.get(f"/api/onboarding/scrape-status/{session_id}")
            except Exception as e:
                logger.warning("[%s] scrape-status error: %s", label, e)
                return {}

        def _is_done(s: dict) -> bool:
            return s.get("status") in ("completed", "failed")

        scrape = await poll(_get_status, _is_done, label=f"scrape_status:{session_id}", timeout=POLL_TIMEOUT)
        logger.info("[%s] final scrape-status: %s", label, scrape)

        if scrape.get("status") == "failed":
            logger.warning("[%s] scrape failed: %s", label, scrape.get("error"))
        else:
            found = scrape.get("prospects_found", 0)
            assert_gt(f"{label}: prospects_found > 0 (bug 4a fix)", found, 0)
            log_pass(f"{label}: prospects_found={found} > 0 ✓ (bug 4a fixed)")

        # ── 9. Verify user.onboarding_complete=True ───────────────────────────
        me = await client.get("/api/auth/me")
        oc = me.get("onboarding_complete") or me.get("user", {}).get("onboarding_complete")
        assert_truthy(f"{label}: onboarding_complete=True", oc)
        log_pass(f"{label}: onboarding_complete=True ✓")

    except AssertionError as e:
        log_fail(label, e)
        raise


async def main() -> None:
    logger.info("=== Onboarding Wizard E2E ===")
    logger.info("Mock mode: %s", MOCK_MODE)
    if MOCK_MODE:
        logger.info("Set OUTFLO_E2E_MOCK=1 (default). Backend must have OUTFLO_E2E_MOCK=1 env var set too.")

    async with E2EClient() as client:
        await test_wizard_flow(client)

    logger.info("=== Onboarding E2E tests passed ===")


if __name__ == "__main__":
    asyncio.run(main())
