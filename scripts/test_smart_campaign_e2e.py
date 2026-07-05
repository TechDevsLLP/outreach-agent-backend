"""
E2E test for the curated smart-campaign workflow.

Flow:
  1. Resolve account from email
  2. Patch _MAX_EMPLOYEES_PER_COMPANY = 3
  3. Create test campaign (curated / Healthcare AI ICP)
  4. Phase A+B: start_curated_discovery() — Gemini sourcing → sourced_companies
  5. Verify sourced_companies (count, fields, source=gemini_grounded)
  6. Phase C→F: continue_after_company_approval() — scrape 3 employees per company
  7. Poll until discovery_status == "completed" (up to 30 min timeout)
  8. Verify prospects (fields populated)
  9. Verify enrollments (scoring, priority_tier)
  10. Verify Day-1 enrichment (linkedin_profile_data, ai_assessment, generated_messages)
  11. Write report → scripts/results/e2e_test_<ts>/report.md + final_state.json
  12. Optional --cleanup

Usage:
  cd backend
  python3 scripts/test_smart_campaign_e2e.py [--account-email EMAIL] [--target N] [--cleanup]
"""

import argparse
import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId

import database
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("e2e_test")

HEALTHCARE_AI_ICP = (
    "B2B Healthcare AI companies and scale-ups (Seed through Series D, or recently IPO'd), "
    "20-1000 employees, headquartered in US, UK, Canada, or Western Europe, building AI products "
    "for clinical decision support, medical imaging, patient triage, drug discovery, clinical "
    "documentation, or care coordination."
)

REQUIRED_COMPANY_FIELDS = [
    "company_name", "company_linkedin_url", "company_domain", "country", "employee_size_estimate",
]
REQUIRED_PROSPECT_FIELDS = [
    "first_name", "last_name", "linkedin", "job_title", "company_name",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _serialize(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Not serializable: {type(obj)}")


async def resolve_account(email: str) -> tuple[str, str]:
    """Return (account_id_str, user_id_str)."""
    user = await database.users_collection.find_one({"email": email})
    if not user:
        raise ValueError(f"No user found with email {email!r}")
    account_id = str(user.get("current_account_id") or user.get("account_id") or "")
    if not account_id:
        raise ValueError(f"User {email!r} has no current_account_id")
    return account_id, str(user["_id"])


async def create_test_campaign(account_id: str, user_id: str, target: int, ts: str, linkedin_account_id: str | None = None, email_account_id: str | None = None) -> str:
    """Insert a curated test campaign directly to MongoDB; return id string."""
    now = datetime.utcnow()
    doc = {
        "account_id": account_id,
        "created_by": user_id,
        "name": f"E2E TEST — Healthcare AI {ts}",
        "description": "E2E test campaign — safe to delete",
        "type": "email_and_linkedin",
        "status": "draft",
        "steps": [],
        "email_account_id": email_account_id,
        "linkedin_account_id": linkedin_account_id,
        "daily_email_limit": 20,
        "daily_linkedin_limit": 20,
        "daily_caps": {},
        "timezone": "America/New_York",
        "send_days": ["monday", "tuesday", "wednesday", "thursday", "friday"],
        "send_hour_start": 9,
        "send_hour_end": 17,
        "total_enrolled": 0,
        "active_count": 0,
        "completed_count": 0,
        "replied_count": 0,
        "bounced_count": 0,
        "opted_out_count": 0,
        "meetings_booked": 0,
        "emails_sent": 0,
        "emails_delivered": 0,
        "emails_opened": 0,
        "emails_clicked": 0,
        "emails_replied": 0,
        "emails_bounced": 0,
        "linkedin_connections_sent": 0,
        "linkedin_connections_accepted": 0,
        "linkedin_inmails_sent": 0,
        "linkedin_replies": 0,
        "is_smart_campaign": True,
        "prospect_count_target": target,  # match curated_company_count_target to avoid top-up
        "icp_industries": ["healthcare", "biotechnology", "information technology", "health technology"],
        "icp_job_titles": [],
        "icp_seniority_levels": ["vp", "director", "head"],
        "icp_company_size_min": None,
        "icp_company_size_max": None,
        "icp_countries": ["United States", "United Kingdom", "Canada", "Germany", "France"],
        "icp_apify_params": None,
        "icp_industry_label": "Healthcare AI",
        "icp_keywords": ["AI", "machine learning", "clinical", "healthcare", "medical"],
        "icp_exclude_keywords": [],
        "icp_exclude_industries": [],
        "icp_functional_departments": ["engineering", "product"],
        "max_prospects_per_company": 3,
        "message_tone": "professional",
        "value_proposition": "We help Healthcare AI teams automate prospect research and personalized outreach.",
        "pain_point": "Manual prospect research doesn't scale, and generic outreach gets ignored.",
        "cta_type": "book_call",
        "cta_url": None,
        "message_guidance": None,
        "email_guidance": None,
        "connection_request_guidance": None,
        "inmail_guidance": None,
        "discovery_mode": "curated",
        "curated_icp_prompt": HEALTHCARE_AI_ICP,
        "curated_company_count_target": target,
        "curated_companies_sourced": 0,
        "curated_companies_approved": 0,
        "curated_companies_scraped": 0,
        "discovery_status": "idle",
        "discovery_started_at": None,
        "discovery_completed_at": None,
        "discovery_error": None,
        "created_at": now,
        "updated_at": now,
    }
    result = await database.campaigns_collection.insert_one(doc)
    return str(result.inserted_id)


async def create_test_accounts(account_id: str) -> tuple[str, str]:
    """Insert stub LinkedIn + email accounts so all Day-1 slots (45) are available.
    Returns (linkedin_account_id, email_account_id)."""
    now = datetime.utcnow()
    li_doc = {
        "account_id": account_id,
        "unipile_status": "OK",
        "is_test_stub": True,
        "profile_data": {"firstName": "E2E", "lastName": "Test"},
        "created_at": now,
        "updated_at": now,
    }
    li_result = await database.linkedin_accounts_collection.insert_one(li_doc)

    email_doc = {
        "account_id": account_id,
        "status": "connected",
        "is_test_stub": True,
        "provider": "smtp",
        "email": "e2e-test@example.com",
        "created_at": now,
        "updated_at": now,
    }
    email_result = await database.email_accounts_collection.insert_one(email_doc)

    return str(li_result.inserted_id), str(email_result.inserted_id)


async def wait_discovery_complete(campaign_id: str, timeout: int = 1800) -> str:
    """Poll campaign discovery_status until completed or failed; return final status."""
    deadline = time.time() + timeout
    campaign_oid = ObjectId(campaign_id)
    while time.time() < deadline:
        doc = await database.campaigns_collection.find_one(
            {"_id": campaign_oid},
            {"discovery_status": 1},
        )
        status = (doc or {}).get("discovery_status", "unknown")
        if status in ("completed", "failed"):
            return status
        logger.info(f"[e2e] discovery_status={status!r} — waiting 30s...")
        await asyncio.sleep(30)
    return "timeout"


async def do_cleanup(campaign_id: str):
    campaign_oid = ObjectId(campaign_id)
    await database.campaigns_collection.delete_one({"_id": campaign_oid})
    await database.sourced_companies_collection.delete_many({"campaign_id": campaign_id})
    await database.campaign_enrollments_collection.delete_many({"campaign_id": campaign_oid})
    await database.prospects_collection.update_many(
        {"source_industry_ids": f"curated:{campaign_oid}"},
        {"$pull": {"source_industry_ids": f"curated:{campaign_oid}"}},
    )
    logger.info(f"[e2e] Cleanup complete for campaign {campaign_id}")


def write_report(output_dir: Path, results: dict, sample_prospects: list, sample_enrollments: list):
    ts = results.get("ts", "")
    campaign_id = results.get("campaign_id", "N/A")
    lines = [
        "# Smart Campaign E2E Test Report",
        "",
        f"**Run:** {ts}  ",
        f"**Campaign ID:** `{campaign_id}`  ",
        f"**Account ID:** `{results.get('account_id', 'N/A')}`  ",
        f"**Company target:** {results.get('company_target', '?')}  ",
        "",
        "---",
        "",
        "## Phase Results",
        "",
    ]
    for phase, data in results.get("phases", {}).items():
        if isinstance(data, dict):
            status = data.get("status", "?")
            icon = "✅" if status == "pass" else "❌"
            lines.append(f"### {icon} {phase}")
            for k, v in data.items():
                if k != "status":
                    lines.append(f"- **{k}:** {v}")
            lines.append("")
        else:
            lines.append(f"### ⚠️  {phase}: {data}")
            lines.append("")

    lines += ["## Timings", ""]
    for k, v in results.get("timings", {}).items():
        lines.append(f"- **{k}:** {v}s")
    lines.append("")

    lines += ["## Counts", ""]
    lines.append(f"- **Companies sourced:** {results.get('sourced_companies_count', 0)}")
    lines.append(f"- **Prospects created:** {results.get('prospects_count', 0)}")
    lines.append(f"- **Enrollments:** {results.get('enrollments_count', 0)}")
    lines.append("")

    if sample_prospects:
        lines += [
            "## Sample Prospects (up to 5)",
            "",
            "| Name | Job Title | Company | LinkedIn | Email | Score |",
            "|---|---|---|---|---|---|",
        ]
        for p in sample_prospects[:5]:
            name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
            score = p.get("ai_prospect_score") or p.get("prospect_score") or "—"
            lines.append(
                f"| {name} | {p.get('job_title', '')} | {p.get('company_name', '')} "
                f"| {'✓' if p.get('linkedin') else '✗'} "
                f"| {'✓' if p.get('email') else '✗'} "
                f"| {score} |"
            )
        lines.append("")

    if sample_enrollments:
        lines += [
            "## Sample Enrollments (up to 5)",
            "",
            "| Status | Day | Channel | Score | Tier |",
            "|---|---|---|---|---|",
        ]
        for e in sample_enrollments[:5]:
            lines.append(
                f"| {e.get('status', '')} "
                f"| {e.get('smart_campaign_send_day', '—')} "
                f"| {e.get('smart_campaign_channel', '—')} "
                f"| {e.get('ai_prospect_score', '—')} "
                f"| {e.get('priority_tier', '—')} |"
            )
        lines.append("")

    (output_dir / "report.md").write_text("\n".join(lines))
    logger.info(f"[e2e] Report written → {output_dir}/report.md")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main(account_email: str, target: int, cleanup: bool):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(__file__).parent / "results" / f"e2e_test_{ts}"
    output_dir.mkdir(parents=True, exist_ok=True)

    results: dict = {
        "ts": ts,
        "company_target": target,
        "phases": {},
        "timings": {},
        "sourced_companies_count": 0,
        "prospects_count": 0,
        "enrollments_count": 0,
    }
    campaign_id: str | None = None

    # ── Resolve account ───────────────────────────────────────────────────────
    logger.info(f"[e2e] Resolving account for {account_email!r}")
    account_id, user_id = await resolve_account(account_email)
    results["account_id"] = account_id
    logger.info(f"[e2e] account_id={account_id}  user_id={user_id}")

    # ── Patch _MAX_EMPLOYEES_PER_COMPANY ─────────────────────────────────────
    import services.curated_discovery_service as cds
    original_max = cds._MAX_EMPLOYEES_PER_COMPANY
    cds._MAX_EMPLOYEES_PER_COMPANY = 3
    logger.info(f"[e2e] Patched _MAX_EMPLOYEES_PER_COMPANY: {original_max} → 3")

    # ── Disable prefilter gate (AI LLM prefilter over-rejects healthcare AI
    #    roles when icp_functional_departments is set to engineering/product)
    settings = get_settings()
    original_prefilter_gate = getattr(settings, "prefilter_gate_enabled", True)
    settings.prefilter_gate_enabled = False
    logger.info(f"[e2e] Disabled prefilter gate (was: {original_prefilter_gate})")

    # ── Disable top-up Apify scraping (E2E tests only the curated flow, not top-up)
    original_topup_iters = getattr(settings, "score_topup_max_iterations", 3)
    settings.score_topup_max_iterations = 0
    logger.info(f"[e2e] Disabled top-up loop (was: {original_topup_iters} iterations)")

    sample_prospects: list = []
    sample_enrollments: list = []
    test_linkedin_account_id: str | None = None
    test_email_account_id: str | None = None

    try:
        # ── Create stub accounts (LinkedIn + email) for full 45-slot Day-1 ────
        logger.info("[e2e] Creating stub LinkedIn + email accounts for channel assignment...")
        test_linkedin_account_id, test_email_account_id = await create_test_accounts(account_id)
        logger.info(f"[e2e] Stub LinkedIn: {test_linkedin_account_id}  Email: {test_email_account_id}")

        # ── Create campaign ───────────────────────────────────────────────────
        logger.info("[e2e] Creating test campaign in MongoDB...")
        campaign_id = await create_test_campaign(
            account_id, user_id, target, ts,
            linkedin_account_id=test_linkedin_account_id,
            email_account_id=test_email_account_id,
        )
        results["campaign_id"] = campaign_id
        campaign_oid = ObjectId(campaign_id)
        logger.info(f"[e2e] Campaign created: {campaign_id}")

        # ══════════════════════════════════════════════════════════════════════
        # Phase A+B — Gemini company sourcing
        # ══════════════════════════════════════════════════════════════════════
        logger.info("[e2e] ── Phase A: Gemini company sourcing ──")
        t_a = time.time()
        phase_a_result = await cds.start_curated_discovery(campaign_id, account_id)
        results["timings"]["phase_a"] = round(time.time() - t_a, 1)
        sourced_count = phase_a_result.get("sourced", 0)
        results["sourced_companies_count"] = sourced_count
        logger.info(f"[e2e] Phase A done: sourced={sourced_count}  time={results['timings']['phase_a']}s")

        # Verify sourced_companies
        sourced_docs = await database.sourced_companies_collection.find(
            {"campaign_id": campaign_id}
        ).to_list(length=None)

        source_labels = list({d.get("source") for d in sourced_docs})
        validated_count = sum(1 for d in sourced_docs if d.get("linkedin_url_validated"))
        sample5 = sourced_docs[:5]
        missing_fields: dict[str, int] = {}
        for doc in sample5:
            for f in REQUIRED_COMPANY_FIELDS:
                if not doc.get(f):
                    missing_fields[f] = missing_fields.get(f, 0) + 1

        results["phases"]["phase_a_sourcing"] = {
            "status": "pass" if sourced_count >= 50 else "fail",
            "companies_sourced": sourced_count,
            "source_labels": source_labels,
            "linkedin_url_validated": validated_count,
            "missing_fields_in_sample_5": missing_fields or "none",
            "meta": phase_a_result.get("meta", {}),
        }

        if sourced_count == 0:
            logger.error("[e2e] 0 companies sourced — aborting")
            results["phases"]["abort"] = "Phase A returned 0 companies — check Gemini API key / ICP prompt"
            write_report(output_dir, results, [], [])
            _print_summary(results, output_dir)
            return

        # ══════════════════════════════════════════════════════════════════════
        # Phase C→F — employee scraping, prospect upsert, pre-enroll, enrich
        # ══════════════════════════════════════════════════════════════════════
        logger.info("[e2e] ── Phase C: employee scraping + prospect creation + enrollment ──")
        t_c = time.time()
        phase_c_result = await cds.continue_after_company_approval(campaign_id, account_id)
        results["timings"]["phase_c_return"] = round(time.time() - t_c, 1)
        logger.info(
            f"[e2e] Phase C returned: prospects_created={phase_c_result.get('prospects_created', 0)}"
            f"  time={results['timings']['phase_c_return']}s"
        )

        # ══════════════════════════════════════════════════════════════════════
        # Wait for _enrich_and_finalize_discovery background task
        # ══════════════════════════════════════════════════════════════════════
        logger.info("[e2e] ── Waiting for _enrich_and_finalize_discovery (up to 30 min) ──")
        t_enrich = time.time()
        final_status = await wait_discovery_complete(campaign_id, timeout=1800)
        results["timings"]["enrich_total"] = round(time.time() - t_enrich, 1)
        logger.info(f"[e2e] discovery_status={final_status!r}  time={results['timings']['enrich_total']}s")

        # ── Verify sourced_companies scrape status ────────────────────────────
        sc_completed = await database.sourced_companies_collection.count_documents({
            "campaign_id": campaign_id, "employee_scrape_status": "completed",
        })
        sc_failed = await database.sourced_companies_collection.count_documents({
            "campaign_id": campaign_id, "employee_scrape_status": "failed",
        })
        results["phases"]["phase_c_employee_scrape"] = {
            "status": "pass" if sc_completed > 0 else "fail",
            "companies_scraped_ok": sc_completed,
            "companies_scraped_fail": sc_failed,
            "companies_total": sourced_count,
        }

        # ── Verify Phase E: prospects ─────────────────────────────────────────
        all_prospects = await database.prospects_collection.find(
            {"source_industry_ids": f"curated:{campaign_oid}"}
        ).to_list(length=None)
        prospects_count = len(all_prospects)
        results["prospects_count"] = prospects_count

        field_coverage: dict[str, str] = {}
        for f in REQUIRED_PROSPECT_FIELDS + ["email", "company_linkedin", "company_domain"]:
            filled = sum(1 for p in all_prospects if p.get(f))
            field_coverage[f] = f"{filled}/{prospects_count}"
        with_email = sum(1 for p in all_prospects if p.get("email"))
        linkedin_only = sum(1 for p in all_prospects if p.get("linkedin") and not p.get("email"))

        results["phases"]["phase_e_prospects"] = {
            "status": "pass" if prospects_count > 0 else "fail",
            "prospects_total": prospects_count,
            "with_email": with_email,
            "linkedin_only": linkedin_only,
            "field_coverage": field_coverage,
        }

        sample_prospects = random.sample(all_prospects, min(5, prospects_count)) if all_prospects else []

        # ── Verify Phase F: enrollments ───────────────────────────────────────
        all_enrollments = await database.campaign_enrollments_collection.find(
            {"campaign_id": campaign_oid}
        ).to_list(length=None)
        enrollments_count = len(all_enrollments)
        results["enrollments_count"] = enrollments_count

        tier_dist: dict[str, int] = {}
        for e in all_enrollments:
            tier = e.get("priority_tier") or "unscored"
            tier_dist[tier] = tier_dist.get(tier, 0) + 1
        scored_count = sum(1 for e in all_enrollments if e.get("ai_prospect_score") is not None)

        results["phases"]["phase_f_enrollments"] = {
            "status": "pass" if enrollments_count > 0 else "fail",
            "enrollments_total": enrollments_count,
            "scored": scored_count,
            "tier_distribution": tier_dist,
        }

        sample_enrollments = random.sample(all_enrollments, min(5, enrollments_count)) if all_enrollments else []

        # ── Verify Day-1 enrichment ───────────────────────────────────────────
        day1_enrollments = [e for e in all_enrollments if e.get("smart_campaign_send_day") == 1]
        day1_prospect_ids = []
        for e in day1_enrollments:
            pid = e.get("prospect_id")
            if pid:
                day1_prospect_ids.append(pid if isinstance(pid, ObjectId) else ObjectId(str(pid)))

        day1_prospects: list = []
        if day1_prospect_ids:
            day1_prospects = await database.prospects_collection.find(
                {"_id": {"$in": day1_prospect_ids}}
            ).to_list(length=None)

        with_linkedin_profile = sum(1 for p in day1_prospects if p.get("linkedin_profile_data"))
        with_company_data = sum(1 for p in day1_prospects if p.get("company_linkedin_data"))
        with_assessment = sum(1 for p in day1_prospects if p.get("ai_assessment"))
        with_enriched = sum(1 for p in day1_prospects if p.get("enrichment_status") == "completed")
        with_messages = sum(
            1 for e in day1_enrollments
            if (e.get("generated_messages") or {}).get("cold_email")
            and (e.get("generated_messages") or {}).get("linkedin_connection_request")
        )

        results["phases"]["day1_enrichment"] = {
            "status": "pass" if final_status == "completed" else "fail",
            "discovery_final_status": final_status,
            "day1_enrollments": len(day1_enrollments),
            "day1_prospects_fetched": len(day1_prospects),
            "with_linkedin_profile_data": with_linkedin_profile,
            "with_company_linkedin_data": with_company_data,
            "with_ai_assessment": with_assessment,
            "enrichment_status_completed": with_enriched,
            "with_cold_email_and_connection_msg": with_messages,
        }

        # ── Outreach guard ────────────────────────────────────────────────────
        messages_sent = await database.campaign_messages_collection.count_documents(
            {"campaign_id": campaign_id}
        )
        campaign_doc = await database.campaigns_collection.find_one({"_id": campaign_oid})
        results["phases"]["outreach_guard"] = {
            "status": "pass" if messages_sent == 0 and (campaign_doc or {}).get("status") != "active" else "fail",
            "messages_sent": messages_sent,
            "campaign_status": (campaign_doc or {}).get("status"),
        }

        # ── Write final_state.json ────────────────────────────────────────────
        final_state = {
            "campaign": json.loads(json.dumps(campaign_doc, default=_serialize)),
            "sourced_companies": json.loads(json.dumps(sourced_docs, default=_serialize)),
            "sample_prospects": json.loads(json.dumps(sample_prospects, default=_serialize)),
            "sample_enrollments": json.loads(json.dumps(
                random.sample(all_enrollments, min(10, len(all_enrollments))),
                default=_serialize,
            ) if all_enrollments else "[]"),
        }
        (output_dir / "final_state.json").write_text(json.dumps(final_state, indent=2))
        logger.info(f"[e2e] final_state.json written → {output_dir}/final_state.json")

    except Exception as exc:
        logger.exception(f"[e2e] Test failed with exception: {exc}")
        results["phases"]["exception"] = str(exc)

    finally:
        cds._MAX_EMPLOYEES_PER_COMPANY = original_max
        logger.info(f"[e2e] Restored _MAX_EMPLOYEES_PER_COMPANY → {original_max}")
        settings.prefilter_gate_enabled = original_prefilter_gate
        logger.info(f"[e2e] Restored prefilter_gate_enabled → {original_prefilter_gate}")
        settings.score_topup_max_iterations = original_topup_iters
        logger.info(f"[e2e] Restored score_topup_max_iterations → {original_topup_iters}")
        if test_linkedin_account_id:
            await database.linkedin_accounts_collection.delete_one({"_id": ObjectId(test_linkedin_account_id)})
            logger.info(f"[e2e] Deleted stub LinkedIn account {test_linkedin_account_id}")
        if test_email_account_id:
            await database.email_accounts_collection.delete_one({"_id": ObjectId(test_email_account_id)})
            logger.info(f"[e2e] Deleted stub email account {test_email_account_id}")

        if cleanup and campaign_id:
            logger.info("[e2e] --cleanup: deleting test data...")
            try:
                await do_cleanup(campaign_id)
            except Exception as e:
                logger.warning(f"[e2e] Cleanup error (non-fatal): {e}")
        elif campaign_id:
            logger.info(f"[e2e] Test data left in DB. campaign_id={campaign_id}  (use --cleanup to delete)")

    write_report(output_dir, results, sample_prospects, sample_enrollments)
    _print_summary(results, output_dir)


def _print_summary(results: dict, output_dir: Path):
    print("\n" + "=" * 65)
    print("  E2E SMART CAMPAIGN TEST SUMMARY")
    print("=" * 65)
    for phase, data in results.get("phases", {}).items():
        if isinstance(data, dict):
            status = data.get("status", "?")
            icon = "✅" if status == "pass" else "❌"
            print(f"  {icon}  {phase}: {status}")
        else:
            print(f"  ⚠️   {phase}: {data}")
    print()
    print(f"  Companies sourced : {results.get('sourced_companies_count', 0)}")
    print(f"  Prospects created : {results.get('prospects_count', 0)}")
    print(f"  Enrollments       : {results.get('enrollments_count', 0)}")
    print()
    print(f"  Report → {output_dir}/report.md")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2E test for curated smart-campaign workflow")
    parser.add_argument("--account-email", default="techdevsinc@gmail.com",
                        help="Account email to run test under")
    parser.add_argument("--target", type=int, default=200,
                        help="Company count target (default: 200)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete all test data from DB after run")
    args = parser.parse_args()

    asyncio.run(main(args.account_email, args.target, args.cleanup))
