"""
Standalone curated discovery test script.

Mirrors the production curated discovery pipeline end-to-end:
Gemini company sourcing → Haiku company score → Apify bulk employee scrape
→ Haiku employee score → Apify email finder → MongoDB upsert.

Does NOT modify production curated_discovery_service.py.

Usage:
    cd /Users/prasad/Documents/Projects/outflo/backend
    python3 scripts/test_apollo_email_enrichment.py --target 75
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from bson import ObjectId

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import database
from config import get_settings
from services.curated_discovery_service import (
    _COMPANY_SCORE_THRESHOLD,
    _EMPLOYEE_SCORE_THRESHOLD,
    _MAX_ITEMS_PER_COMPANY,
    _PROFILE_SCRAPER_MODE,
    _broaden_function_ids,
    _broaden_seniority_ids,
    _build_icp_prompt_from_campaign,
    _build_sender_context,
    _icp_function_to_actor_ids,
    _icp_seniority_to_actor_ids,
    _score_companies_with_llm,
    _score_employees_with_llm,
    _upsert_curated_prospect,
)
from services.company_sourcing_service import source_companies
from services.email_finder_service import find_emails_for_linkedin_urls
from services.employee_scraper_service import (
    bulk_scrape_employees_for_companies,
    transform_employee_to_prospect,
)
from services.openrouter_service import OpenRouterClient

logger = logging.getLogger(__name__)
settings = get_settings()

# Hardcoded ICP matching the prior test (campaign 6a20692e35e9ddcaec3d8341)
# for direct apples-to-apples comparison.
TEST_ICP = {
    "curated_icp_prompt": (
        "AI-powered healthcare technology companies (SaaS, diagnostics, "
        "clinical decision support) in the US with 10-500 employees"
    ),
    "icp_industries": [
        "healthcare",
        "health technology",
        "biotechnology",
        "artificial intelligence",
    ],
    "icp_seniority_levels": ["vp", "director", "c_suite", "head"],
    "icp_functional_departments": ["sales", "engineering", "product_management"],
    "icp_countries": ["United States"],
    "icp_keywords": ["AI", "healthcare", "diagnostics", "clinical"],
    "icp_company_size_min": 10,
    "icp_company_size_max": 500,
}


async def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    out_dir = (
        Path(__file__).parent / "results" / f"discovery_test_{int(time.time())}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Resolve account ───────────────────────────────────────────────────
    user = await database.users_collection.find_one({"email": args.account_email})
    if not user:
        raise SystemExit(f"No user found for {args.account_email}")
    account_id = str(user["current_account_id"])
    logger.info(f"Running as account_id={account_id}")

    # ── 2. Fetch company profile for sender context ──────────────────────────
    company_profile = await database.company_profiles_collection.find_one(
        {
            "account_id": (
                ObjectId(account_id) if len(account_id) == 24 else account_id
            )
        }
    )

    # ── 3. Create fresh test campaign ────────────────────────────────────────
    now = datetime.utcnow()
    campaign_doc = {
        "account_id": account_id,
        "name": f"Discovery Test {now.strftime('%Y-%m-%d %H:%M')}",
        "description": "Standalone curated discovery test",
        "status": "draft",
        "is_smart_campaign": True,
        "smart_mode": "curated",
        "discovery_mode": "curated",
        "discovery_status": "sourcing_companies",
        **TEST_ICP,
        "curated_company_count_target": 35,
        "prospect_count_target": args.target,
        "created_at": now,
        "updated_at": now,
    }
    insert_res = await database.campaigns_collection.insert_one(campaign_doc)
    campaign_oid = insert_res.inserted_id
    campaign_id = str(campaign_oid)
    campaign_doc["_id"] = campaign_oid
    logger.info(f"[test:{campaign_id}] created test campaign")

    or_client = OpenRouterClient()
    metrics: dict = {"campaign_id": campaign_id, "started_at": now.isoformat()}
    t_total = time.time()

    try:
        # ── 4. Build ICP prompt + sender context ─────────────────────────────
        icp_prompt = _build_icp_prompt_from_campaign(campaign_doc)
        sender_ctx = _build_sender_context(company_profile)

        # ── 5. Gemini source companies ────────────────────────────────────────
        t = time.time()
        target_cos = campaign_doc["curated_company_count_target"]
        companies, _meta = await source_companies(
            icp_prompt=icp_prompt,
            target_count=int(target_cos * 1.5),
            account_id=account_id,
            campaign_id=campaign_id,
        )
        metrics["companies_sourced"] = len(companies)
        metrics["company_source_seconds"] = round(time.time() - t, 2)
        logger.info(f"[test:{campaign_id}] sourced {len(companies)} companies")

        # ── 6. Haiku batch score companies ───────────────────────────────────
        co_scores = await _score_companies_with_llm(companies, icp_prompt, or_client)
        kept_companies = [
            c
            for c, s in zip(companies, co_scores)
            if s >= _COMPANY_SCORE_THRESHOLD and c.get("company_linkedin_url")
        ]
        metrics["companies_kept"] = len(kept_companies)
        logger.info(
            f"[test:{campaign_id}] {len(kept_companies)}/{len(companies)} "
            f"companies kept after Haiku score"
        )

        # ── 7. Persist sourced_companies ──────────────────────────────────────
        if kept_companies:
            sc_docs = [
                {
                    **c,
                    "campaign_id": campaign_id,
                    "account_id": account_id,
                    "source": "gemini_grounded",
                    "user_excluded": False,
                    "employee_scrape_status": "pending",
                    "employees_scraped_count": 0,
                    "prospects_created_count": 0,
                    "created_at": now,
                    "updated_at": now,
                }
                for c in kept_companies
            ]
            await database.sourced_companies_collection.insert_many(
                sc_docs, ordered=False
            )

        # ── 8. Bulk Apify employee scrape ────────────────────────────────────
        seniority_ids = _icp_seniority_to_actor_ids(
            campaign_doc.get("icp_seniority_levels", [])
        )
        functional_ids = _icp_function_to_actor_ids(
            campaign_doc.get("icp_functional_departments", [])
        )
        company_urls = [c["company_linkedin_url"] for c in kept_companies]

        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "scraping_employees"}},
        )

        t = time.time()
        raw_employees = await bulk_scrape_employees_for_companies(
            company_urls,
            max_items_per_company=_MAX_ITEMS_PER_COMPANY,
            seniority_level_ids=seniority_ids or None,
            functional_level_ids=functional_ids or None,
            profile_scraper_mode=_PROFILE_SCRAPER_MODE,
            account_id=account_id,
            campaign_id=campaign_id,
        )
        metrics["employees_scraped"] = len(raw_employees)
        metrics["scrape_seconds"] = round(time.time() - t, 2)
        logger.info(
            f"[test:{campaign_id}] Apify returned {len(raw_employees)} employees "
            f"in {metrics['scrape_seconds']}s"
        )

        # ── 9. Transform + Haiku score employees ─────────────────────────────
        url_to_sc = {c["company_linkedin_url"]: c for c in kept_companies}
        fallback_sc = kept_companies[0] if kept_companies else {}
        pairs = []
        for emp in raw_employees:
            co_url = emp.get("companyLinkedinUrl") or emp.get("companyUrl") or ""
            sc = url_to_sc.get(co_url) or fallback_sc
            pairs.append((emp, sc))

        transformed = [transform_employee_to_prospect(emp, sc) for emp, sc in pairs]
        emp_scores = await _score_employees_with_llm(
            transformed, icp_prompt, sender_ctx, or_client
        )

        kept_prospects = []
        for t_p, s in zip(transformed, emp_scores):
            score = int(s) if isinstance(s, (int, float)) else 0
            t_p["fit_score"] = score
            t_p["ai_prospect_score"] = float(score)
            if score >= _EMPLOYEE_SCORE_THRESHOLD and t_p.get("linkedin"):
                kept_prospects.append(t_p)

        metrics["prospects_after_score"] = len(kept_prospects)
        logger.info(
            f"[test:{campaign_id}] {len(kept_prospects)}/{len(transformed)} "
            f"employees passed Haiku score"
        )

        # Recovery: re-scrape with broadened IDs if yield < 1 emp/company
        if len(raw_employees) < len(company_urls) and kept_companies:
            broad_seniority = _broaden_seniority_ids(seniority_ids)
            broad_function = _broaden_function_ids(functional_ids)
            logger.info(
                f"[test:{campaign_id}] recovery: broadening IDs, "
                f"yield={len(raw_employees)}/{len(company_urls)}"
            )
            try:
                recovery_emps = await bulk_scrape_employees_for_companies(
                    company_urls,
                    max_items_per_company=_MAX_ITEMS_PER_COMPANY,
                    seniority_level_ids=broad_seniority or None,
                    functional_level_ids=broad_function or None,
                    profile_scraper_mode=_PROFILE_SCRAPER_MODE,
                    account_id=account_id,
                    campaign_id=campaign_id,
                )
                if recovery_emps:
                    r_pairs = [
                        (
                            emp,
                            url_to_sc.get(
                                emp.get("companyLinkedinUrl") or emp.get("companyUrl") or ""
                            ) or fallback_sc,
                        )
                        for emp in recovery_emps
                    ]
                    r_transformed = [
                        transform_employee_to_prospect(emp, sc) for emp, sc in r_pairs
                    ]
                    r_scores = await _score_employees_with_llm(
                        r_transformed, icp_prompt, sender_ctx, or_client
                    )
                    for t_p, s in zip(r_transformed, r_scores):
                        score = int(s) if isinstance(s, (int, float)) else 0
                        t_p["fit_score"] = score
                        t_p["ai_prospect_score"] = float(score)
                        if score >= _EMPLOYEE_SCORE_THRESHOLD and t_p.get("linkedin"):
                            kept_prospects.append(t_p)
                    metrics["recovery_employees_scraped"] = len(recovery_emps)
                    logger.info(
                        f"[test:{campaign_id}] recovery added "
                        f"{metrics['recovery_employees_scraped']} employees"
                    )
            except Exception as e:
                logger.warning(f"[test:{campaign_id}] recovery failed (skipping): {e}")

        # Sort by score, take top N
        kept_prospects.sort(key=lambda x: x["fit_score"], reverse=True)
        top = kept_prospects[: args.target]
        metrics["prospects_targeted"] = len(top)

        # ── 10. Apify email finder ────────────────────────────────────────────
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "enriching"}},
        )

        missing_email_urls = [
            p["linkedin"] for p in top if not p.get("email") and p.get("linkedin")
        ]
        t = time.time()
        logger.info(
            f"[test:{campaign_id}] email finder — {len(missing_email_urls)} URLs"
        )
        email_by_url: dict = {}
        if missing_email_urls:
            email_by_url = await find_emails_for_linkedin_urls(
                missing_email_urls,
                account_id=account_id,
                campaign_id=campaign_id,
            )
        metrics["email_finder_seconds"] = round(time.time() - t, 2)

        emails_found = 0
        for p in top:
            email = email_by_url.get(p.get("linkedin") or "")
            if email:
                p["email"] = email
                emails_found += 1

        metrics["emails_found"] = emails_found
        metrics["email_coverage_pct"] = round(
            100 * emails_found / max(len(top), 1), 1
        )
        logger.info(
            f"[test:{campaign_id}] emails found: {emails_found}/{len(top)} "
            f"= {metrics['email_coverage_pct']}%"
        )

        # ── 11. Persist via _upsert_curated_prospect ──────────────────────────
        upserted = 0
        for p in top:
            p["source"] = "curated_discovery"
            res = await _upsert_curated_prospect(p, campaign_oid, account_id)
            if res:
                upserted += 1
        metrics["prospects_persisted"] = upserted

        # ── 12. Update campaign to completed ─────────────────────────────────
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {
                "$set": {
                    "discovery_status": "completed",
                    "status": "awaiting_approval",
                    "discovery_completed_at": datetime.utcnow(),
                    "curated_companies_sourced": len(kept_companies),
                    "curated_companies_approved": len(kept_companies),
                    "prospects_count": upserted,
                }
            },
        )

        metrics["total_seconds"] = round(time.time() - t_total, 2)

        # ── 13. Write report artifacts ────────────────────────────────────────
        report = _build_report(metrics, top[:10])
        (out_dir / "report.md").write_text(report)
        (out_dir / "final_state.json").write_text(
            json.dumps(metrics, indent=2, default=str)
        )

        logger.info(
            f"[test:{campaign_id}] COMPLETE in {metrics['total_seconds']}s — "
            f"email coverage: {emails_found}/{len(top)} = {metrics['email_coverage_pct']}%"
        )
        logger.info(f"Results: {out_dir}")

    except Exception:
        await database.campaigns_collection.update_one(
            {"_id": campaign_oid},
            {"$set": {"discovery_status": "failed"}},
        )
        raise
    finally:
        await or_client.close()


def _build_report(metrics: dict, samples: list) -> str:
    lines = [
        f"# Curated Discovery Test — Campaign {metrics['campaign_id']}",
        "",
        "## Funnel",
        f"- Companies sourced (Gemini): {metrics.get('companies_sourced')}",
        f"- Companies kept (Haiku ≥ 50): {metrics.get('companies_kept')}",
        f"- Employees scraped (Apify Short mode): {metrics.get('employees_scraped')}",
    ]
    if "recovery_employees_scraped" in metrics:
        lines.append(
            f"- Recovery employees scraped: {metrics['recovery_employees_scraped']}"
        )
    lines += [
        f"- Prospects after Haiku score (≥ 60): {metrics.get('prospects_after_score')}",
        f"- Prospects targeted (top N): {metrics.get('prospects_targeted')}",
        f"- **Emails found: {metrics.get('emails_found')} "
        f"({metrics.get('email_coverage_pct')}%)**",
        f"- Prospects persisted to MongoDB: {metrics.get('prospects_persisted')}",
        "",
        "## Timing",
        f"- Gemini company sourcing: {metrics.get('company_source_seconds')}s",
        f"- Apify employee scrape: {metrics.get('scrape_seconds')}s",
        f"- Apify email finder: {metrics.get('email_finder_seconds')}s",
        f"- **Total wall-clock: {metrics.get('total_seconds')}s**",
        "",
        "## Sample top prospects",
    ]
    for p in samples:
        lines.append(
            f"- **{p.get('full_name')}** | {p.get('job_title')} @ "
            f"{p.get('company_name')} | score: {p.get('fit_score')} "
            f"| email: `{p.get('email') or 'n/a'}`"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Standalone curated discovery test — mirrors production pipeline"
    )
    parser.add_argument("--account-email", default="techdevsinc@gmail.com")
    parser.add_argument(
        "--target",
        type=int,
        default=75,
        help="Max prospects to keep after scoring",
    )
    args = parser.parse_args()
    asyncio.run(main(args))
