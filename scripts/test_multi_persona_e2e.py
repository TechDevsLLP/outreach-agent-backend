"""
Multi-persona end-to-end test:
  Onboarding → Sender Voice → Prospect Scraping → Campaign Launch → Day-1 Messages

Runs 5 diverse business personas sequentially.  After every phase, each
persona's JSON file is atomically updated (write-temp → os.replace), and a
rolling summary.json is rewritten — so results land in real time.

Flags:
  --cleanup          delete all test accounts / campaigns / prospects after run
  --count N          prospects to source per persona (default 5)
  --personas 1,3     comma-separated 1-based indexes to run a subset
  --with-topup       allow background top-up discovery (default: disabled for cost control)
  --mock-prospects   skip Gemini + Apify entirely; use hardcoded prospect fixtures.
                     Use when the Apify actor needs permission approval or credits are low.
                     Exercises sender voice synthesis, campaign launch, and Day-1 message
                     generation (all OpenRouter calls) without any scraping.
  --help

Usage:
  cd backend
  python3 scripts/test_multi_persona_e2e.py
  python3 scripts/test_multi_persona_e2e.py --personas 1 --cleanup
  python3 scripts/test_multi_persona_e2e.py --count 3 --cleanup
  python3 scripts/test_multi_persona_e2e.py --mock-prospects --cleanup
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId

import database
from config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_multi_persona")

# ─────────────────────────────────────────────────────────────────────────────
# 5 PERSONAS
# Each has the full company_profiles shape + 5 authored LinkedIn posts.
# ─────────────────────────────────────────────────────────────────────────────

PERSONAS = [
    # ── 1. TechDevs ──────────────────────────────────────────────────────────
    {
        "slug": "techdevs",
        "index": 1,
        "company_name": "TechDevs",
        "company_url": "https://techdevs.in",
        "company_linkedin_url": "https://www.linkedin.com/company/tech-devs/",
        "sender_name": "Prithvi Jadwani",
        "sender_role": "Co-founder",
        "sender_linkedin_url": "https://www.linkedin.com/in/prithvi-jadwani/",
        "services": [
            "Custom software development",
            "Mobile app development",
            "Web application development",
            "UI/UX design",
            "AI/ML integration",
            "Startup MVP development",
        ],
        "value_propositions": [
            "Dedicated offshore development team at 60% lower cost than US/UK rates",
            "Fast MVP delivery in 6–8 weeks",
            "Full-stack expertise: React, Node.js, Python, Flutter, AWS",
            "Transparent communication and agile process",
        ],
        "pain_points": [
            "High cost of hiring local developers",
            "Difficulty finding reliable tech talent quickly",
            "Slow product iteration cycles",
            "Lack of technical expertise in-house for scaling",
        ],
        "icp_description": (
            "Early to growth-stage tech-enabled companies (Series A–C) in the US/UK/Canada "
            "who need to build or scale a software product but want to avoid high local hiring costs. "
            "Typically have a non-technical or semi-technical founder who trusts a reliable offshore team."
        ),
        "target_industries": ["SaaS", "Fintech", "E-commerce", "HealthTech", "EdTech", "B2B software"],
        "target_job_titles": ["CTO", "Co-founder", "CEO", "VP Engineering", "Head of Technology", "Founder"],
        "target_seniority": ["Director", "VP", "C-Level", "Founder", "Partner"],
        "target_geographies": ["United States", "United Kingdom", "Canada", "Australia"],
        "target_company_sizes": ["1-10", "11-50", "51-200"],
        "primary_cta": "Schedule a free 30-min technical discovery call",
        "sender_linkedin_posts": [
            {"text": "Just shipped our 12th MVP in 18 months for a YC-backed fintech founder in SF. 6 weeks, $28k. The team crushed it. Offshore dev gets a bad rap, but the right team + the right process = you can't tell the difference from onshore. Happy to share our SoW template if useful."},
            {"text": "Unpopular opinion: most startups hire devs too early. You don't need a 5-person eng team at idea stage. You need 2 great engineers and a clear spec. We've seen founders burn $500k on salaries before writing a single line of customer-facing code. Build lean first."},
            {"text": "The #1 thing our clients get wrong before working with us: no technical spec. We spend the first week doing a discovery sprint — wireframes, data model, API contracts — before writing a single line. This alone cuts scope creep by 60%. Non-negotiable."},
            {"text": "We're TechDevs — 3 years, 40+ products shipped, zero VC funding, completely bootstrapped. Built this company from a WhatsApp group of 4 devs to a 30-person team. If you're a founder who needs a reliable dev partner, DM me. Always down to chat."},
            {"text": "Hot take: Flutter is underrated for B2B SaaS. One codebase, iOS + Android + web, 40% faster than native dev. We've built 8 products on it this year. Clients save 3–4 months of dev time and ship to mobile without a second thought. Anyone else seeing this?"},
        ],
    },

    # ── 2. LedgerGuard ───────────────────────────────────────────────────────
    {
        "slug": "ledgerguard",
        "index": 2,
        "company_name": "LedgerGuard",
        "company_url": "https://ledgerguard.io",
        "company_linkedin_url": "https://www.linkedin.com/company/ledgerguard-io/",
        "sender_name": "Sarah Mitchell",
        "sender_role": "VP Product",
        "sender_linkedin_url": "https://www.linkedin.com/in/sarah-mitchell-ledger/",
        "services": [
            "AML transaction monitoring",
            "Suspicious Activity Report (SAR) automation",
            "KYC/KYB onboarding compliance",
            "Regulatory change management",
            "Real-time sanctions screening",
            "Compliance audit trail and reporting",
        ],
        "value_propositions": [
            "Reduce false positives by 70% with ML-tuned AML models",
            "SAR filing time cut from 4 hours to 20 minutes",
            "FinCEN and FATF-aligned out of the box",
            "Integrates with core banking systems in under 2 weeks",
        ],
        "pain_points": [
            "Overwhelming SAR filing burden straining compliance teams",
            "Legacy AML systems generating too many false positives",
            "Manual KYC reviews slowing customer onboarding",
            "Regulatory change fatigue — rules update faster than teams can adapt",
        ],
        "icp_description": (
            "Community banks, credit unions, fintechs, and specialty lenders with $100M–$5B in assets "
            "that are under BSA/AML scrutiny and need to modernize compliance without a full rip-and-replace. "
            "Typically have a Head of BSA, Chief Compliance Officer, or CFO who is tired of regulator findings."
        ),
        "target_industries": ["Banking", "Fintech", "Credit Unions", "Lending", "Insurance", "Payments"],
        "target_job_titles": ["Head of Compliance", "Chief Compliance Officer", "CFO", "Chief Risk Officer", "BSA Officer", "VP Risk"],
        "target_seniority": ["Director", "VP", "C-Level"],
        "target_geographies": ["United States", "Canada", "United Kingdom"],
        "target_company_sizes": ["51-200", "201-500", "501-1000"],
        "primary_cta": "Book a 20-min compliance gap assessment",
        "sender_linkedin_posts": [
            {"text": "The OCC issued 23 enforcement actions last quarter related to BSA/AML deficiencies. 23. Community banks are getting hit hardest because their compliance teams are stretched thin and their legacy monitoring tools weren't built for modern transaction patterns. This is solvable. But it requires moving before the exam, not after."},
            {"text": "SAR narrative writing takes the average compliance analyst 3.5 hours per filing. Multiply that by 200 SARs/month. That's one full-time employee doing nothing but writing SAR narratives. We automated 80% of that narrative with LLM + structured data. The analyst reviews and approves in 15 min. The math is obvious."},
            {"text": "Hot take: AML false positive rates above 95% aren't a 'technology problem.' They're a data-quality problem masked by a technology problem. Before you buy a new monitoring system, audit your transaction data completeness. Most banks find 20–30% of their alerts are triggered by bad data, not actual risk."},
            {"text": "Just got back from BAI Beacon. Three conversations that keep coming up: 1) FinCEN's beneficial ownership rule is still confusing everyone. 2) Nobody has solved crypto transaction monitoring for community banks at reasonable cost. 3) Regulators are asking about AI explainability in AML decisions. Buckle up."},
            {"text": "We built LedgerGuard because we spent 8 years on the compliance side at two regional banks and watched millions get spent on systems that made compliance teams' lives harder, not easier. Good compliance software should feel invisible — it should make the right answer obvious and the audit trail automatic."},
        ],
    },

    # ── 3. VitalSignal ───────────────────────────────────────────────────────
    {
        "slug": "vitalsignal",
        "index": 3,
        "company_name": "VitalSignal",
        "company_url": "https://vitalsignal.health",
        "company_linkedin_url": "https://www.linkedin.com/company/vitalsignal-health/",
        "sender_name": "Dr. James Okafor",
        "sender_role": "CEO",
        "sender_linkedin_url": "https://www.linkedin.com/in/james-okafor-md/",
        "services": [
            "Clinical outcomes analytics platform",
            "Population health management dashboards",
            "Care gap identification and prioritization",
            "Readmission risk scoring",
            "Value-based care contract analytics",
            "EHR data integration (Epic, Cerner, Athena)",
        ],
        "value_propositions": [
            "Reduce preventable readmissions by 18% using real-time risk models",
            "Identify high-risk patients 48 hours before deterioration",
            "Pre-built Epic and Cerner integrations — live in 90 days",
            "Supports both FFS and value-based care reporting",
        ],
        "pain_points": [
            "EHR data is siloed and can't drive proactive care decisions",
            "Readmission penalties eating into thin hospital margins",
            "Clinical staff spending hours on manual chart review instead of care",
            "Value-based care contracts hard to manage without real-time analytics",
        ],
        "icp_description": (
            "Regional hospitals (100–500 beds), multi-site health systems, and physician-led ACOs "
            "that are transitioning to value-based care and need actionable analytics — not just dashboards. "
            "Champions are CMOs, VP Clinical Operations, or CMIOs who want to improve outcomes and lower costs simultaneously."
        ),
        "target_industries": ["Healthcare", "Hospitals", "Health Systems", "ACO", "Managed Care", "Payer"],
        "target_job_titles": ["CMO", "Chief Medical Officer", "VP Clinical Operations", "CMIO", "VP Quality", "Medical Director"],
        "target_seniority": ["VP", "C-Level", "Director"],
        "target_geographies": ["United States"],
        "target_company_sizes": ["201-500", "501-1000", "1001-5000"],
        "primary_cta": "Request a 30-min clinical outcomes ROI walkthrough",
        "sender_linkedin_posts": [
            {"text": "We analyzed readmission data from 14 regional hospitals over 3 years. The hospitals with the lowest 30-day readmission rates shared one thing: they identified high-risk patients BEFORE discharge, not after. The intervention window is 48–72 hours pre-discharge. After that, you're playing catch-up. Our models now flag this window with 84% accuracy."},
            {"text": "A CMO asked me last week: 'How do I justify analytics spend to my CFO?' Here's what I told her: Take your readmission penalty last year. Multiply by 0.18. That's the dollar amount a well-implemented risk model returns in year 1 — conservatively. It usually pays for itself in 6 months. The CFO conversation gets much easier."},
            {"text": "I trained as an EM physician before founding VitalSignal. I remember staring at a discharge order thinking 'this patient is going to come back in 10 days.' I had the clinical intuition but no data to act on. We built VitalSignal so that clinical intuition has data behind it. Every physician should have that."},
            {"text": "Epic's sepsis model generates a lot of alerts. So many that alert fatigue is a documented safety problem in hospitals using it. The problem isn't AI — it's specificity. High sensitivity + low specificity = alert fatigue. We tune our models per patient population, which cuts false alert rates by ~55%. Specificity matters more than sensitivity in deployed clinical AI."},
            {"text": "Value-based care contracts are getting more complex — MSSP, BPCI-A, Commercial ACO. Each has different attribution rules, benchmark methods, and quality measures. Most health systems track these in spreadsheets. We built a contract analytics layer that maps your patient panel to each contract in real time so you always know where you stand."},
        ],
    },

    # ── 4. FortiByte ─────────────────────────────────────────────────────────
    {
        "slug": "fortibyte",
        "index": 4,
        "company_name": "FortiByte",
        "company_url": "https://fortibyte.io",
        "company_linkedin_url": "https://www.linkedin.com/company/fortibyte-io/",
        "sender_name": "Rachel Chen",
        "sender_role": "Co-founder & CTO",
        "sender_linkedin_url": "https://www.linkedin.com/in/rachel-chen-security/",
        "services": [
            "Cloud Security Posture Management (CSPM)",
            "Infrastructure-as-Code security scanning",
            "Multi-cloud compliance automation (SOC2, ISO 27001, PCI-DSS)",
            "Misconfiguration detection and auto-remediation",
            "Cloud identity and entitlement management",
            "Security posture benchmarking against CIS controls",
        ],
        "value_propositions": [
            "Detect misconfigurations before they become breaches — median fix time under 4 hours",
            "Continuous compliance for SOC2, PCI-DSS, ISO 27001 from a single pane",
            "Agentless deployment — connected to AWS/GCP/Azure in 15 minutes",
            "Auto-remediation playbooks cut MTTR by 65%",
        ],
        "pain_points": [
            "Cloud misconfigurations are the #1 cause of enterprise breaches",
            "Security teams can't keep up with multi-cloud drift at scale",
            "Audit prep consuming 40+ hours of manual evidence collection",
            "Engineering teams keep over-permissioning IAM roles for convenience",
        ],
        "icp_description": (
            "Mid-market and enterprise companies (500–5000 employees) running workloads on AWS, GCP, or Azure "
            "that need continuous compliance and posture management without adding headcount. "
            "Champions are CISOs, VP Security, or Cloud Security Architects who are accountable for audit outcomes and breach prevention."
        ),
        "target_industries": ["SaaS", "Fintech", "E-commerce", "Healthcare IT", "Media", "Enterprise Software"],
        "target_job_titles": ["CISO", "VP Security", "VP Engineering", "Cloud Security Architect", "IT Director", "Head of Security"],
        "target_seniority": ["Director", "VP", "C-Level"],
        "target_geographies": ["United States", "United Kingdom", "Germany", "Australia"],
        "target_company_sizes": ["201-500", "501-1000", "1001-5000"],
        "primary_cta": "Get a free cloud posture risk assessment (15 min)",
        "sender_linkedin_posts": [
            {"text": "The Snowflake breach earlier this year wasn't a zero-day. It was an exposed credential + no MFA. The misconfiguration that enabled it would have been caught by any CSPM tool in under 5 minutes. Most breaches aren't sophisticated. They're basic hygiene failures at scale. That's the scary part and the fixable part."},
            {"text": "I do a lot of post-mortems on cloud security incidents. The pattern is almost always the same: an S3 bucket was public, an IAM role had * permissions, a security group allowed 0.0.0.0/0 on port 22. These aren't novel attacks. They're configurations that were wrong for months or years before anyone noticed. Drift detection should be table stakes."},
            {"text": "Your DevOps team is not trying to create security risks. They're trying to ship fast. IAM over-permissioning, public subnets, unencrypted buckets — these come from velocity, not malice. The fix isn't a policy. It's guardrails baked into the deployment pipeline. Shift left means the security check happens in the PR, not the audit."},
            {"text": "SOC2 Type II prep used to take our customers 6–8 weeks of manual evidence collection. HR screenshotting access reviews. Security manually pulling CloudTrail logs. Engineers writing policies they'd already implemented. We automated 80% of that evidence pipeline. The first audit is still painful. Every renewal after that is a push of a button."},
            {"text": "Built FortiByte because at my last job (security engineer, $4B SaaS company) I spent 6 months implementing a CSPM solution from a big vendor. It took 6 months to deploy, 3 months to tune, and the signal-to-noise was so bad we turned half the alerts off. That experience felt like a waste. We built something an engineer can set up in an afternoon and trust by end of week."},
        ],
    },

    # ── 5. TalentLoop ────────────────────────────────────────────────────────
    {
        "slug": "talentloop",
        "index": 5,
        "company_name": "TalentLoop",
        "company_url": "https://talentloop.ai",
        "company_linkedin_url": "https://www.linkedin.com/company/talentloop-ai/",
        "sender_name": "Marcus Williams",
        "sender_role": "Head of Growth",
        "sender_linkedin_url": "https://www.linkedin.com/in/marcus-williams-hr/",
        "services": [
            "AI-powered candidate sourcing and outreach",
            "Recruiting workflow automation",
            "Interview scheduling and coordination",
            "Offer management and e-sign",
            "Recruiter productivity analytics",
            "ATS integration (Greenhouse, Lever, Workday)",
        ],
        "value_propositions": [
            "Fill roles 40% faster by automating sourcing and first-touch outreach",
            "Recruiter handles 3x the req load without additional headcount",
            "Candidate response rate 2.8x higher with AI-personalized outreach",
            "Native integrations with Greenhouse, Lever, and Workday — set up in a day",
        ],
        "pain_points": [
            "Time-to-hire stretching past 60 days for senior roles",
            "Sourcers spending 70% of time on manual LinkedIn searches",
            "Interview scheduling eating 30% of recruiter bandwidth",
            "Candidate experience suffering from slow, impersonal process",
        ],
        "icp_description": (
            "Scaling tech-forward companies (100–1000 employees, growing 30%+ YoY) "
            "with a talent team of 2–20 recruiters that need to hire faster without proportionally growing the recruiting function. "
            "Champions are Head of Talent, VP People, or CHRO who are accountable to time-to-hire and quality of hire metrics."
        ),
        "target_industries": ["SaaS", "Fintech", "E-commerce", "Healthcare Tech", "Consumer Tech", "Marketplace"],
        "target_job_titles": ["Head of Talent", "VP People", "CHRO", "Director of Recruiting", "Talent Acquisition Lead", "Head of HR"],
        "target_seniority": ["Director", "VP", "C-Level"],
        "target_geographies": ["United States", "Canada", "United Kingdom"],
        "target_company_sizes": ["51-200", "201-500", "501-1000"],
        "primary_cta": "See a live demo with your actual job reqs",
        "sender_linkedin_posts": [
            {"text": "Real numbers from a TalentLoop customer (Series B SaaS, 280 employees): Before — 58 days average time-to-hire. After 90 days with TalentLoop — 34 days. Same 3-person talent team, same budget. The difference: automated sourcing + AI outreach sequences + scheduling automation. Time-to-hire is the most underrated competitive advantage in tech hiring right now."},
            {"text": "Sourcers are some of the most undervalued people in recruiting. They do the hardest, most repetitive work — finding needles in LinkedIn haystacks — and get the least recognition. AI sourcing doesn't replace great sourcers. It frees them from the needle-finding so they can do the relationship-building that actually closes candidates."},
            {"text": "Candidate ghosting is a hiring team problem, not a candidate problem. If your process takes 5 weeks between application and first call, if scheduling takes 3 email chains, if offer approval takes 2 weeks — candidates leave for faster offers. Speed is candidate experience. Fix the process, and ghosting drops dramatically."},
            {"text": "The 'recruiter headcount vs. efficiency tool' debate is the wrong frame. The question isn't: 'Do I hire another recruiter or buy software?' It's: 'What's my cost per hire, and what's my capacity constraint?' For most scaling companies, the constraint is sourcing volume and scheduling bandwidth — both of which software solves faster and more cheaply than headcount."},
            {"text": "We're TalentLoop. Built by ex-in-house recruiters (not ex-ATS engineers) who got tired of duct-taping tools together. Our customers tell us the first thing they notice isn't the features — it's that the product feels like it was built by someone who's actually done the job. That's the whole point."},
        ],
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# MOCK PROSPECTS  (used when --mock-prospects flag is set)
# Bypasses Gemini company sourcing + Apify employee scraping entirely.
# Shape matches source_preview_prospects() output so the rest of the
# pipeline (upsert → enroll → message-gen → assertions) runs unmodified.
# ─────────────────────────────────────────────────────────────────────────────

MOCK_PROSPECTS_BY_SLUG: dict[str, list[dict]] = {
    # ── 1. TechDevs — SaaS/Fintech startups, CTO / Co-founder ───────────────
    "techdevs": [
        {
            "full_name": "Alex Rivera",
            "linkedin": "https://www.linkedin.com/in/alex-rivera-cto-42891/",
            "email": "alex.rivera@pavefintech.io",
            "company_name": "Pave",
            "job_title": "CTO",
            "company_linkedin": "https://www.linkedin.com/company/pave-pay/",
            "company_domain": "pave.dev",
            "company_website": "https://pave.dev",
            "industry": "Fintech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Pave", "industry": "Fintech"},
        },
        {
            "full_name": "Jordan Kim",
            "linkedin": "https://www.linkedin.com/in/jordan-kim-cofounder-83412/",
            "email": "jordan@learnly.io",
            "company_name": "Learnly",
            "job_title": "Co-founder & CEO",
            "company_linkedin": "https://www.linkedin.com/company/learnly-edtech/",
            "company_domain": "learnly.io",
            "company_website": "https://learnly.io",
            "industry": "EdTech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Learnly", "industry": "EdTech"},
        },
        {
            "full_name": "Priya Nair",
            "linkedin": "https://www.linkedin.com/in/priya-nair-vpe-61204/",
            "email": "priya.nair@healthstream.ai",
            "company_name": "HealthStream AI",
            "job_title": "VP Engineering",
            "company_linkedin": "https://www.linkedin.com/company/healthstream-ai/",
            "company_domain": "healthstream.ai",
            "company_website": "https://healthstream.ai",
            "industry": "HealthTech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "HealthStream AI", "industry": "HealthTech"},
        },
        {
            "full_name": "Marcus Webb",
            "linkedin": "https://www.linkedin.com/in/marcus-webb-founder-99103/",
            "email": "marcus@cartloop.io",
            "company_name": "Cartloop",
            "job_title": "Founder",
            "company_linkedin": "https://www.linkedin.com/company/cartloop/",
            "company_domain": "cartloop.io",
            "company_website": "https://cartloop.io",
            "industry": "E-commerce",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Cartloop", "industry": "E-commerce"},
        },
        {
            "full_name": "Samira Hassan",
            "linkedin": "https://www.linkedin.com/in/samira-hassan-cto-55782/",
            "email": "s.hassan@stackline.io",
            "company_name": "Stackline",
            "job_title": "Technical Co-founder",
            "company_linkedin": "https://www.linkedin.com/company/stackline-io/",
            "company_domain": "stackline.io",
            "company_website": "https://stackline.io",
            "industry": "B2B software",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Stackline", "industry": "B2B software"},
        },
    ],

    # ── 2. LedgerGuard — Banks / fintechs, Compliance / Risk ────────────────
    "ledgerguard": [
        {
            "full_name": "Christine Baxter",
            "linkedin": "https://www.linkedin.com/in/christine-baxter-bsa-72301/",
            "email": "christine.baxter@firstmeridia.bank",
            "company_name": "First Meridia Bank",
            "job_title": "Head of BSA/AML Compliance",
            "company_linkedin": "https://www.linkedin.com/company/first-meridia-bank/",
            "company_domain": "firstmeridia.bank",
            "company_website": "https://firstmeridia.bank",
            "industry": "Banking",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "First Meridia Bank", "industry": "Banking"},
        },
        {
            "full_name": "Derek Chow",
            "linkedin": "https://www.linkedin.com/in/derek-chow-compliance-48812/",
            "email": "d.chow@clearancepay.com",
            "company_name": "ClearancePay",
            "job_title": "Chief Compliance Officer",
            "company_linkedin": "https://www.linkedin.com/company/clearancepay/",
            "company_domain": "clearancepay.com",
            "company_website": "https://clearancepay.com",
            "industry": "Fintech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "ClearancePay", "industry": "Fintech"},
        },
        {
            "full_name": "Laura Mendez",
            "linkedin": "https://www.linkedin.com/in/laura-mendez-cfo-39041/",
            "email": "lmendez@pioneerfcu.org",
            "company_name": "Pioneer Federal Credit Union",
            "job_title": "CFO",
            "company_linkedin": "https://www.linkedin.com/company/pioneer-federal-cu/",
            "company_domain": "pioneerfcu.org",
            "company_website": "https://pioneerfcu.org",
            "industry": "Credit Unions",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Pioneer Federal Credit Union", "industry": "Credit Unions"},
        },
        {
            "full_name": "Thomas Brannigan",
            "linkedin": "https://www.linkedin.com/in/thomas-brannigan-risk-61099/",
            "email": "t.brannigan@apexlending.com",
            "company_name": "Apex Lending",
            "job_title": "Chief Risk Officer",
            "company_linkedin": "https://www.linkedin.com/company/apex-lending/",
            "company_domain": "apexlending.com",
            "company_website": "https://apexlending.com",
            "industry": "Lending",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Apex Lending", "industry": "Lending"},
        },
        {
            "full_name": "Angela Torres",
            "linkedin": "https://www.linkedin.com/in/angela-torres-vprisk-52287/",
            "email": "atorres@bridgepaymentsco.com",
            "company_name": "Bridge Payments",
            "job_title": "VP Risk & Compliance",
            "company_linkedin": "https://www.linkedin.com/company/bridge-payments-co/",
            "company_domain": "bridgepaymentsco.com",
            "company_website": "https://bridgepaymentsco.com",
            "industry": "Payments",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Bridge Payments", "industry": "Payments"},
        },
    ],

    # ── 3. VitalSignal — Hospitals / health systems, CMO / VP Clinical ───────
    "vitalsignal": [
        {
            "full_name": "Dr. Patricia Nguyen",
            "linkedin": "https://www.linkedin.com/in/patricia-nguyen-md-cmo-33812/",
            "email": "p.nguyen@valleyhealth.org",
            "company_name": "Valley Health System",
            "job_title": "Chief Medical Officer",
            "company_linkedin": "https://www.linkedin.com/company/valley-health-system/",
            "company_domain": "valleyhealth.org",
            "company_website": "https://valleyhealth.org",
            "industry": "Healthcare",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Valley Health System", "industry": "Healthcare"},
        },
        {
            "full_name": "Robert Schaefer",
            "linkedin": "https://www.linkedin.com/in/robert-schaefer-vpclinops-80021/",
            "email": "rschaefer@trinityhospitals.org",
            "company_name": "Trinity Hospitals",
            "job_title": "VP Clinical Operations",
            "company_linkedin": "https://www.linkedin.com/company/trinity-hospitals/",
            "company_domain": "trinityhospitals.org",
            "company_website": "https://trinityhospitals.org",
            "industry": "Hospitals",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Trinity Hospitals", "industry": "Hospitals"},
        },
        {
            "full_name": "Dr. Amara Osei",
            "linkedin": "https://www.linkedin.com/in/amara-osei-cmio-44902/",
            "email": "aosei@northlakehealthcare.com",
            "company_name": "Northlake Healthcare",
            "job_title": "CMIO",
            "company_linkedin": "https://www.linkedin.com/company/northlake-healthcare/",
            "company_domain": "northlakehealthcare.com",
            "company_website": "https://northlakehealthcare.com",
            "industry": "Health Systems",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Northlake Healthcare", "industry": "Health Systems"},
        },
        {
            "full_name": "Sandra Eichler",
            "linkedin": "https://www.linkedin.com/in/sandra-eichler-vpquality-61438/",
            "email": "seichler@precisionaco.org",
            "company_name": "Precision ACO",
            "job_title": "VP Quality & Patient Safety",
            "company_linkedin": "https://www.linkedin.com/company/precision-aco/",
            "company_domain": "precisionaco.org",
            "company_website": "https://precisionaco.org",
            "industry": "ACO",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Precision ACO", "industry": "ACO"},
        },
        {
            "full_name": "Dr. Kevin Marsh",
            "linkedin": "https://www.linkedin.com/in/kevin-marsh-meddir-77123/",
            "email": "k.marsh@sunrisepayer.com",
            "company_name": "Sunrise Health Plan",
            "job_title": "Medical Director",
            "company_linkedin": "https://www.linkedin.com/company/sunrise-health-plan/",
            "company_domain": "sunrisepayer.com",
            "company_website": "https://sunrisepayer.com",
            "industry": "Managed Care",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Sunrise Health Plan", "industry": "Managed Care"},
        },
    ],

    # ── 4. FortiByte — Mid-market enterprise, CISO / VP Security ────────────
    "fortibyte": [
        {
            "full_name": "Nathan Goldberg",
            "linkedin": "https://www.linkedin.com/in/nathan-goldberg-ciso-55021/",
            "email": "ngoldberg@buildkite.io",
            "company_name": "Buildkite",
            "job_title": "CISO",
            "company_linkedin": "https://www.linkedin.com/company/buildkite/",
            "company_domain": "buildkite.com",
            "company_website": "https://buildkite.com",
            "industry": "SaaS",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Buildkite", "industry": "SaaS"},
        },
        {
            "full_name": "Olivia Park",
            "linkedin": "https://www.linkedin.com/in/olivia-park-vpsecurity-38812/",
            "email": "o.park@rippling.com",
            "company_name": "Rippling",
            "job_title": "VP Security Engineering",
            "company_linkedin": "https://www.linkedin.com/company/rippling/",
            "company_domain": "rippling.com",
            "company_website": "https://rippling.com",
            "industry": "SaaS",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Rippling", "industry": "SaaS"},
        },
        {
            "full_name": "Daniel Firth",
            "linkedin": "https://www.linkedin.com/in/daniel-firth-cloudsec-92034/",
            "email": "dfirth@paddle.com",
            "company_name": "Paddle",
            "job_title": "Cloud Security Architect",
            "company_linkedin": "https://www.linkedin.com/company/paddle-hq/",
            "company_domain": "paddle.com",
            "company_website": "https://paddle.com",
            "industry": "Fintech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Paddle", "industry": "Fintech"},
        },
        {
            "full_name": "Michelle Tran",
            "linkedin": "https://www.linkedin.com/in/michelle-tran-itdirector-40771/",
            "email": "mtran@grammarly.com",
            "company_name": "Grammarly",
            "job_title": "IT Director",
            "company_linkedin": "https://www.linkedin.com/company/grammarly/",
            "company_domain": "grammarly.com",
            "company_website": "https://grammarly.com",
            "industry": "Enterprise Software",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Grammarly", "industry": "Enterprise Software"},
        },
        {
            "full_name": "James Obi",
            "linkedin": "https://www.linkedin.com/in/james-obi-headsec-83901/",
            "email": "j.obi@brex.com",
            "company_name": "Brex",
            "job_title": "Head of Security",
            "company_linkedin": "https://www.linkedin.com/company/brex-hq/",
            "company_domain": "brex.com",
            "company_website": "https://brex.com",
            "industry": "Fintech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Brex", "industry": "Fintech"},
        },
    ],

    # ── 5. TalentLoop — Scaling tech, Head of Talent / VP People ────────────
    "talentloop": [
        {
            "full_name": "Alicia Monroe",
            "linkedin": "https://www.linkedin.com/in/alicia-monroe-headtalent-60812/",
            "email": "amonroe@retool.com",
            "company_name": "Retool",
            "job_title": "Head of Talent",
            "company_linkedin": "https://www.linkedin.com/company/tryretool/",
            "company_domain": "retool.com",
            "company_website": "https://retool.com",
            "industry": "SaaS",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Retool", "industry": "SaaS"},
        },
        {
            "full_name": "Cameron Brooks",
            "linkedin": "https://www.linkedin.com/in/cameron-brooks-vppeople-72041/",
            "email": "c.brooks@mercury.com",
            "company_name": "Mercury",
            "job_title": "VP People",
            "company_linkedin": "https://www.linkedin.com/company/mercury-technologies/",
            "company_domain": "mercury.com",
            "company_website": "https://mercury.com",
            "industry": "Fintech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Mercury", "industry": "Fintech"},
        },
        {
            "full_name": "Vanessa Ortega",
            "linkedin": "https://www.linkedin.com/in/vanessa-ortega-chro-31290/",
            "email": "vortega@faire.com",
            "company_name": "Faire",
            "job_title": "CHRO",
            "company_linkedin": "https://www.linkedin.com/company/faire-wholesale/",
            "company_domain": "faire.com",
            "company_website": "https://faire.com",
            "industry": "E-commerce",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Faire", "industry": "E-commerce"},
        },
        {
            "full_name": "Brian Hollis",
            "linkedin": "https://www.linkedin.com/in/brian-hollis-dirtaacq-58812/",
            "email": "bhollis@sword-health.com",
            "company_name": "Sword Health",
            "job_title": "Director of Talent Acquisition",
            "company_linkedin": "https://www.linkedin.com/company/sword-health/",
            "company_domain": "sword-health.com",
            "company_website": "https://sword-health.com",
            "industry": "Healthcare Tech",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Sword Health", "industry": "Healthcare Tech"},
        },
        {
            "full_name": "Nina Petrov",
            "linkedin": "https://www.linkedin.com/in/nina-petrov-talentlead-44092/",
            "email": "nina@incident.io",
            "company_name": "Incident.io",
            "job_title": "Talent Acquisition Lead",
            "company_linkedin": "https://www.linkedin.com/company/incident-io/",
            "company_domain": "incident.io",
            "company_website": "https://incident.io",
            "industry": "SaaS",
            "ai_prospect_score": 90.0,
            "fit_score": 0.90,
            "_sourced_company": {"company_name": "Incident.io", "industry": "SaaS"},
        },
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# JSON helpers
# ─────────────────────────────────────────────────────────────────────────────

def _json_default(obj):
    """JSON serializer for ObjectId, datetime, etc."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


def _atomic_write(path: Path, data: dict) -> None:
    """Write JSON atomically (temp → os.replace) so partial reads are impossible."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=_json_default))
    os.replace(tmp, path)


def _new_run(persona: dict) -> dict:
    """Initialise the per-persona run state dict."""
    return {
        "persona_index": persona["index"],
        "persona_slug": persona["slug"],
        "company_name": persona["company_name"],
        "sender_name": persona["sender_name"],
        "sender_role": persona["sender_role"],
        "target_industries": persona.get("target_industries", []),
        "target_job_titles": persona.get("target_job_titles", []),
        "sender_linkedin_posts": persona.get("sender_linkedin_posts", []),
        "phases": {},
        "checks": [],
        "timings": {},
        "account_id": None,
        "user_id": None,
        "campaign_id": None,
        "status": "running",
        "error": None,
    }


def _check(run: dict, name: str, passed: bool, detail: str) -> None:
    run["checks"].append({"name": name, "status": "PASS" if passed else "FAIL", "detail": detail})


# ─────────────────────────────────────────────────────────────────────────────
# Prospect-side post injection
# ─────────────────────────────────────────────────────────────────────────────

def _synthetic_prospect_posts(prospect: dict) -> list[dict]:
    """Generate 2–3 role/industry-templated posts for a scraped prospect.
    These are injected into linkedin_profile_data.posts so the message
    generator's _select_top_signal picks up a 'recent_post' personalization hook.
    """
    title = prospect.get("job_title") or "leader"
    company = prospect.get("company_name") or "our company"
    industry = prospect.get("industry") or "technology"

    return [
        {
            "text": (
                f"Excited to share that {company} just hit a major milestone this quarter. "
                f"It's been a year of building, shipping, and learning. "
                f"Proud of everything the team has accomplished in {industry}."
            )
        },
        {
            "text": (
                f"As {title}, I've been spending a lot of time thinking about how {industry} companies "
                f"can move faster without sacrificing quality. "
                f"The teams that win are the ones that invest in the right tools and partnerships early."
            )
        },
        {
            "text": (
                f"Just wrapped up planning for H2 at {company}. "
                f"Big goals, tight timelines, and a lot of exciting problems to solve. "
                f"If anyone wants to connect on challenges in {industry}, my DMs are open."
            )
        },
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Phase functions
# ─────────────────────────────────────────────────────────────────────────────

async def phase_account(run: dict, persona: dict) -> bool:
    """Create/reuse a tagged throwaway test account + owner user."""
    import bcrypt as _bcrypt

    slug = persona["slug"]
    email = f"persona_{persona['index']:02d}_{slug}@outflo.test"
    name = f"{persona['company_name']} Test"

    user = await database.users_collection.find_one({"email": email})
    if user:
        user_id = str(user["_id"])
        account_id = str(user.get("current_account_id") or "")
        if not account_id:
            member = await database.account_members_collection.find_one(
                {"user_id": ObjectId(user_id)}
            )
            account_id = str(member["account_id"]) if member else ""
        logger.info("[%s] Reusing existing account %s / user %s", slug, account_id, user_id)
        run["account_id"] = account_id
        run["user_id"] = user_id
        run["phases"]["account"] = {"account_id": account_id, "user_id": user_id, "reused": True, "status": "pass"}
        _check(run, "account_bootstrap", True, f"reused account={account_id}")
        return True

    account_oid = ObjectId()
    await database.accounts_collection.insert_one({
        "_id": account_oid,
        "name": name,
        "slug": f"test-{slug}-{str(account_oid)[-6:]}",
        "plan": "trial",
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    })

    user_oid = ObjectId()
    hashed = _bcrypt.hashpw("TestPassword123!".encode("utf-8"), _bcrypt.gensalt()).decode("utf-8")
    await database.users_collection.insert_one({
        "_id": user_oid,
        "email": email,
        "name": name,
        "password_hash": hashed,
        "current_account_id": account_oid,
        "onboarding_complete": False,
        "created_at": datetime.utcnow(),
    })

    await database.account_members_collection.insert_one({
        "_id": ObjectId(),
        "account_id": account_oid,
        "user_id": user_oid,
        "role": "owner",
        "created_at": datetime.utcnow(),
    })

    account_id = str(account_oid)
    user_id = str(user_oid)
    run["account_id"] = account_id
    run["user_id"] = user_id
    run["phases"]["account"] = {"account_id": account_id, "user_id": user_id, "reused": False, "status": "pass"}
    _check(run, "account_bootstrap", True, f"created account={account_id}")
    return True


async def phase_profile(run: dict, persona: dict) -> bool:
    """Upsert onboarding profile (simulates wizard stages 1–4)."""
    account_id = run["account_id"]
    profile_doc = {k: v for k, v in persona.items()
                   if k not in ("slug", "index", "sender_linkedin_posts")}
    profile_doc["account_id"] = account_id
    profile_doc["updated_at"] = datetime.utcnow()

    await database.company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": profile_doc},
        upsert=True,
    )
    saved = await database.company_profiles_collection.find_one({"account_id": account_id})
    ok = bool(saved and saved.get("target_industries"))
    run["phases"]["onboarding_profile"] = {
        "saved": ok,
        "target_industries": (saved or {}).get("target_industries", []),
        "target_job_titles": (saved or {}).get("target_job_titles", []),
        "company_name": persona["company_name"],
        "status": "pass" if ok else "fail",
    }
    _check(run, "profile_saved", ok, f"target_industries={persona.get('target_industries', [])[:2]}")
    return ok


async def phase_sender_voice(run: dict, persona: dict) -> bool:
    """Synthesize sender voice from 5 authored LinkedIn posts (no real scraping)."""
    from services.sender_voice_service import synthesize_voice_profile
    from database import company_profiles_collection

    account_id = run["account_id"]
    posts = persona.get("sender_linkedin_posts", [])

    t0 = time.time()
    try:
        voice_profile = await synthesize_voice_profile(
            posts=posts,
            sender_name=persona["sender_name"],
            sender_role=persona["sender_role"],
            raw_profile={"headline": persona["sender_role"]},
        )
    except Exception as exc:
        logger.error("[%s] Voice synthesis failed: %s", persona["slug"], exc)
        run["phases"]["sender_voice"] = {"error": str(exc), "status": "fail"}
        _check(run, "sender_voice", False, f"synthesis failed: {exc}")
        return False
    elapsed = time.time() - t0

    # Persist to company_profile (mirrors update_sender_voice_for_account)
    MAX_POSTS_STORED = 15
    await company_profiles_collection.update_one(
        {"account_id": account_id},
        {"$set": {
            "sender_linkedin_url": persona["sender_linkedin_url"],
            "sender_voice_profile": voice_profile,
            "sender_linkedin_posts": posts[:MAX_POSTS_STORED],
        }},
    )

    ok = bool(voice_profile.get("tone_markers") or voice_profile.get("synthesized_summary"))
    run["phases"]["sender_voice"] = {
        "voice_profile": voice_profile,
        "post_count": len(posts),
        "tone_markers": voice_profile.get("tone_markers", []),
        "synthesized_summary": voice_profile.get("synthesized_summary", ""),
        "elapsed_s": round(elapsed, 1),
        "status": "pass" if ok else "partial",
    }
    _check(run, "sender_voice", ok, f"tone={voice_profile.get('tone_markers', [])}, elapsed={elapsed:.0f}s")
    return ok


async def phase_scraping(
    run: dict,
    persona: dict,
    count: int,
    mock_prospects: bool = False,
) -> list[dict]:
    """Source prospects via Gemini → Apify → email finder.
    When mock_prospects=True, uses MOCK_PROSPECTS_BY_SLUG instead — no external calls.
    """
    from services.onboarding_prospect_service import build_icp_prompt_from_profile

    account_id = run["account_id"]
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    icp_prompt = build_icp_prompt_from_profile(profile or {})

    if mock_prospects:
        logger.info("[%s] MOCK PROSPECTS MODE — skipping Gemini + Apify", persona["slug"])
        all_mock = MOCK_PROSPECTS_BY_SLUG.get(persona["slug"], [])
        prospects = list(all_mock[:count])
        elapsed = 0.0
        mode_note = "mock"
    else:
        from services.onboarding_prospect_service import source_preview_prospects
        logger.info("[%s] ICP prompt:\n%s", persona["slug"], icp_prompt)
        t0 = time.time()
        try:
            prospects = await source_preview_prospects(profile, count=count, account_id=account_id)
        except Exception as exc:
            logger.error("[%s] Scraping failed: %s", persona["slug"], exc)
            run["phases"]["scraping"] = {"error": str(exc), "status": "fail", "icp_prompt": icp_prompt}
            _check(run, "prospects_found", False, f"scraping failed: {exc}")
            return []
        elapsed = time.time() - t0
        mode_note = "real"

    has_li = sum(1 for p in prospects if p.get("linkedin"))
    has_email = sum(1 for p in prospects if p.get("email"))

    # Inject synthetic prospect posts into each prospect dict (for personalization)
    for p in prospects:
        p["linkedin_profile_data"] = p.get("linkedin_profile_data") or {}
        injected = _synthetic_prospect_posts(p)
        p["linkedin_profile_data"]["posts"] = injected

    prospect_summaries = [
        {
            "full_name": p.get("full_name"),
            "email": p.get("email"),
            "company_name": p.get("company_name"),
            "job_title": p.get("job_title"),
            "linkedin": p.get("linkedin"),
            "industry": p.get("industry"),
            "injected_posts": [post["text"][:100] + "..." for post in
                               (p.get("linkedin_profile_data") or {}).get("posts", [])],
        }
        for p in prospects
    ]

    ok = len(prospects) >= 1 and has_li >= 1
    run["phases"]["scraping"] = {
        "mode": mode_note,
        "icp_prompt": icp_prompt,
        "total": len(prospects),
        "with_linkedin": has_li,
        "with_email": has_email,
        "prospects": prospect_summaries,
        "elapsed_s": round(elapsed, 1),
        "status": "pass" if ok else "fail",
    }
    _check(run, "prospects_found", ok,
           f"[{mode_note}] total={len(prospects)}, linkedin={has_li}, email={has_email}, elapsed={elapsed:.0f}s")
    return prospects


async def phase_launch(
    run: dict,
    persona: dict,
    prospects: list[dict],
    with_topup: bool,
) -> Optional[str]:
    """Launch first campaign, generate Day-1 messages (top-up disabled by default)."""
    import services.onboarding_prospect_service as _ops
    from services.onboarding_prospect_service import launch_onboarding_first_campaign

    account_id = run["account_id"]
    user_id = run["user_id"]
    profile = await database.company_profiles_collection.find_one({"account_id": account_id})

    # Disable background top-up unless explicitly requested (cost control)
    if not with_topup:
        original_topup = _ops._run_topup_discovery

        async def _noop_topup(*args, **kwargs):
            logger.info("[%s] Top-up discovery disabled for test run.", persona["slug"])

        _ops._run_topup_discovery = _noop_topup
    else:
        original_topup = None

    t0 = time.time()
    try:
        campaign_id = await launch_onboarding_first_campaign(
            account_id=account_id,
            user_id=user_id,
            profile=profile,
            confirmed_prospects=prospects,
            target_total=50,
            campaign_name=f"[TEST] {persona['company_name']} — Day-1 Persona Run",
        )
    except Exception as exc:
        logger.error("[%s] Campaign launch failed: %s\n%s", persona["slug"], exc, traceback.format_exc())
        run["phases"]["campaign"] = {"error": str(exc), "status": "fail"}
        _check(run, "campaign_launch", False, f"launch failed: {exc}")
        return None
    finally:
        if not with_topup and original_topup is not None:
            _ops._run_topup_discovery = original_topup
    elapsed = time.time() - t0

    # Fetch campaign state
    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    status = (campaign or {}).get("status", "unknown")

    # Day-1 enrollments
    campaign_oid = ObjectId(campaign_id)
    day1_enrs = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": 1,
    }).to_list(length=None)

    with_msgs = [e for e in day1_enrs if e.get("generated_messages")]

    run["campaign_id"] = campaign_id
    run["phases"]["campaign"] = {
        "campaign_id": campaign_id,
        "campaign_status": status,
        "day1_enrollments": len(day1_enrs),
        "with_messages": len(with_msgs),
        "elapsed_s": round(elapsed, 1),
        "status": "pass" if status == "awaiting_approval" else "fail",
    }
    ok = status == "awaiting_approval"
    _check(run, "campaign_launch", ok,
           f"status={status}, day1={len(day1_enrs)}, messages={len(with_msgs)}, elapsed={elapsed:.0f}s")
    return campaign_id


async def phase_day1_messages(run: dict, persona: dict) -> None:
    """Fetch and record generated Day-1 messages. Check for post references."""
    campaign_id = run.get("campaign_id")
    if not campaign_id:
        run["phases"]["day1_messages"] = {"status": "skip", "reason": "no campaign_id"}
        return

    campaign_oid = ObjectId(campaign_id)
    enrs = await database.campaign_enrollments_collection.find({
        "campaign_id": campaign_oid,
        "smart_campaign_send_day": 1,
    }).to_list(length=None)

    prospect_ids = [e.get("prospect_id") for e in enrs if e.get("prospect_id")]
    prospects_raw = await database.prospects_collection.find(
        {"_id": {"$in": prospect_ids}}
    ).to_list(length=None) if prospect_ids else []
    pid_to_prospect = {p["_id"]: p for p in prospects_raw}

    messages_out = []
    post_ref_count = 0
    generated_count = 0

    for enr in enrs:
        prospect = pid_to_prospect.get(enr.get("prospect_id"), {})
        msgs = enr.get("generated_messages") or {}
        channel = enr.get("smart_campaign_channel", "unknown")
        gen_status = enr.get("message_gen_status", "unknown")

        # Check if any message body references a post marker or prospect-company keywords
        post_signal = ""
        post_data = (prospect.get("linkedin_profile_data") or {}).get("posts", [])
        if post_data:
            post_signal = (post_data[0].get("text") or "")[:80]

        # Scan generated message bodies for post reference keywords
        references_post = False
        company_kw = (prospect.get("company_name") or "").split()[0].lower() if prospect.get("company_name") else ""
        for channel_key, channel_val in msgs.items():
            if isinstance(channel_val, dict):
                body_text = " ".join(str(v) for v in channel_val.values()).lower()
                # Post is referenced if milestone/quarter/team keywords from injected posts appear
                if any(kw in body_text for kw in ["milestone", "quarter", "hiring", "building", "excited"]):
                    references_post = True
                if company_kw and company_kw in body_text:
                    references_post = True

        if msgs:
            generated_count += 1
        if references_post:
            post_ref_count += 1

        messages_out.append({
            "prospect_name": prospect.get("full_name"),
            "prospect_company": prospect.get("company_name"),
            "channel": channel,
            "message_gen_status": gen_status,
            "has_messages": bool(msgs),
            "references_post": references_post,
            "post_signal_used": post_signal,
            "generated_messages": msgs,
        })

    ok = generated_count >= 1
    run["phases"]["day1_messages"] = {
        "total_enrollments": len(enrs),
        "generated_count": generated_count,
        "post_reference_count": post_ref_count,
        "messages": messages_out,
        "status": "pass" if ok else "fail",
    }
    _check(run, "day1_messages_generated", ok,
           f"generated={generated_count}/{len(enrs)}, post_refs={post_ref_count}")


async def phase_assertions(run: dict) -> None:
    """Final guard: campaign in awaiting_approval, nothing dispatched."""
    campaign_id = run.get("campaign_id")
    if not campaign_id:
        run["phases"]["assertions"] = {"status": "skip", "reason": "no campaign_id"}
        return

    campaign_oid = ObjectId(campaign_id)
    campaign = await database.campaigns_collection.find_one({"_id": campaign_oid})
    status = (campaign or {}).get("status", "unknown")

    sent_count = 0
    if hasattr(database, "campaign_messages_collection"):
        sent_count = await database.campaign_messages_collection.count_documents(
            {"campaign_id": campaign_oid}
        )

    completed_enrs = await database.campaign_enrollments_collection.count_documents({
        "campaign_id": campaign_oid,
        "status": "completed",
    })

    ok_status = status == "awaiting_approval"
    ok_nosend = sent_count == 0 and completed_enrs == 0

    run["phases"]["assertions"] = {
        "campaign_status": status,
        "sent_messages": sent_count,
        "completed_enrollments": completed_enrs,
        "status": "pass" if (ok_status and ok_nosend) else "fail",
    }
    _check(run, "awaiting_approval", ok_status, f"status={status}")
    _check(run, "nothing_dispatched", ok_nosend, f"sent={sent_count}, completed_enrs={completed_enrs}")


# ─────────────────────────────────────────────────────────────────────────────
# Summary + incremental save
# ─────────────────────────────────────────────────────────────────────────────

def _persona_filename(persona: dict) -> str:
    return f"persona_{persona['index']:02d}_{persona['slug']}.json"


def save_persona(run: dict, run_dir: Path) -> None:
    """Atomically write/update the per-persona JSON file."""
    fname = f"persona_{run['persona_index']:02d}_{run['persona_slug']}.json"
    _atomic_write(run_dir / fname, run)


def save_summary(all_runs: list[dict], run_dir: Path, run_meta: dict) -> None:
    """Rewrite summary.json with aggregate phase pass/fail + key counts."""
    rows = []
    total_prospects = 0
    total_emails = 0
    total_messages = 0

    for run in all_runs:
        phases_status = {k: v.get("status", "?") for k, v in run.get("phases", {}).items()}
        checks_pass = sum(1 for c in run.get("checks", []) if c["status"] == "PASS")
        checks_fail = sum(1 for c in run.get("checks", []) if c["status"] == "FAIL")

        scraping = run.get("phases", {}).get("scraping", {})
        total_prospects += scraping.get("total", 0)
        total_emails += scraping.get("with_email", 0)

        msgs = run.get("phases", {}).get("day1_messages", {})
        total_messages += msgs.get("generated_count", 0)

        rows.append({
            "persona": run["company_name"],
            "slug": run["persona_slug"],
            "status": run.get("status", "running"),
            "account_id": run.get("account_id"),
            "campaign_id": run.get("campaign_id"),
            "phases": phases_status,
            "checks_pass": checks_pass,
            "checks_fail": checks_fail,
            "prospects_found": scraping.get("total", 0),
            "prospects_with_email": scraping.get("with_email", 0),
            "messages_generated": msgs.get("generated_count", 0),
            "post_references": msgs.get("post_reference_count", 0),
        })

    summary = {
        **run_meta,
        "personas": rows,
        "totals": {
            "personas_run": len(all_runs),
            "personas_passed": sum(1 for r in all_runs if r.get("status") == "PASS"),
            "personas_failed": sum(1 for r in all_runs if r.get("status") in ("FAIL", "ERROR")),
            "total_prospects_sourced": total_prospects,
            "total_emails_found": total_emails,
            "total_messages_generated": total_messages,
        },
        "last_updated": datetime.utcnow().isoformat(),
    }
    _atomic_write(run_dir / "summary.json", summary)


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup
# ─────────────────────────────────────────────────────────────────────────────

async def cleanup_persona(run: dict, persona: dict) -> None:
    """Delete all test data for one persona."""
    account_id = run.get("account_id")
    campaign_id = run.get("campaign_id")
    slug = persona["slug"]
    email = f"persona_{persona['index']:02d}_{slug}@outflo.test"

    if campaign_id:
        c_oid = ObjectId(campaign_id)
        await database.campaign_enrollments_collection.delete_many({"campaign_id": c_oid})
        if hasattr(database, "campaign_messages_collection"):
            await database.campaign_messages_collection.delete_many({"campaign_id": c_oid})
        await database.campaigns_collection.delete_one({"_id": c_oid})
        logger.info("[%s] Deleted campaign + enrollments", slug)

    if account_id:
        account_oid = ObjectId(account_id)
        await database.company_profiles_collection.delete_one({"account_id": account_id})
        await database.onboarding_sessions_collection.delete_many({"account_id": account_id})
        user = await database.users_collection.find_one({"email": email})
        if user:
            await database.users_collection.delete_one({"_id": user["_id"]})
            await database.account_members_collection.delete_many({"user_id": user["_id"]})
        await database.accounts_collection.delete_one({"_id": account_oid})
        logger.info("[%s] Deleted account, profile, session, user", slug)


# ─────────────────────────────────────────────────────────────────────────────
# Console output
# ─────────────────────────────────────────────────────────────────────────────

def _print_final_table(all_runs: list[dict]) -> None:
    print("\n" + "=" * 130)
    print("MULTI-PERSONA TEST SUMMARY")
    print("=" * 130)
    print(f"{'#':<3} {'COMPANY':<18} {'STATUS':<8} {'PROSPECTS':<11} {'EMAIL':<8} {'MSGS':<6} {'POST_REF':<10} {'PHASE_FAIL'}")
    print("-" * 130)
    for run in all_runs:
        sc = run.get("phases", {}).get("scraping", {})
        msgs = run.get("phases", {}).get("day1_messages", {})
        failed_phases = [k for k, v in run.get("phases", {}).items() if v.get("status") == "fail"]
        failed_checks = [c["name"] for c in run.get("checks", []) if c["status"] == "FAIL"]
        icon = "✅" if run.get("status") == "PASS" else ("⚠️" if run.get("status") == "ERROR" else "❌")
        print(
            f"{icon} {run['persona_index']:<2} {run['company_name']:<18} "
            f"{run.get('status', '?'):<8} "
            f"{sc.get('total', '-')!s:<11} "
            f"{sc.get('with_email', '-')!s:<8} "
            f"{msgs.get('generated_count', '-')!s:<6} "
            f"{msgs.get('post_reference_count', '-')!s:<10} "
            f"{', '.join(failed_phases or failed_checks or ['—'])}"
        )
    print("=" * 130 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main(
    do_cleanup: bool = False,
    count: int = 5,
    persona_indexes: Optional[list[int]] = None,
    with_topup: bool = False,
    mock_prospects: bool = False,
) -> None:
    settings = get_settings()
    logger.info(
        "MongoDB: %s / %s | count=%d | cleanup=%s | topup=%s | mock_prospects=%s",
        settings.mongodb_url[:40], settings.mongodb_database, count, do_cleanup, with_topup, mock_prospects,
    )
    if mock_prospects:
        logger.info(
            "*** MOCK PROSPECTS MODE: Gemini + Apify bypassed. "
            "Exercising sender voice, campaign launch, and Day-1 message generation only. "
            "To enable real scraping: approve Apify actor at "
            "https://console.apify.com/actors/Vb6LZkh4EqRlR0Ka9?approvePermissions=true "
            "then re-run without --mock-prospects ***"
        )

    # Build output directory
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(__file__).parent / "results" / f"multi_persona_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Run output: %s", run_dir)

    # Filter personas
    active_personas = PERSONAS
    if persona_indexes:
        active_personas = [p for p in PERSONAS if p["index"] in persona_indexes]
    logger.info("Running %d persona(s): %s", len(active_personas), [p["slug"] for p in active_personas])

    # Write initial run meta
    run_meta: dict = {
        "ts": ts,
        "run_dir": str(run_dir),
        "mongodb_database": settings.mongodb_database,
        "count": count,
        "with_topup": with_topup,
        "mock_prospects": mock_prospects,
        "cleanup": do_cleanup,
        "persona_slugs": [p["slug"] for p in active_personas],
        "started_at": datetime.utcnow().isoformat(),
        "ended_at": None,
    }
    _atomic_write(run_dir / "_run_meta.json", run_meta)

    all_runs: list[dict] = []
    total_start = time.time()

    for persona in active_personas:
        slug = persona["slug"]
        logger.info("\n%s\n=== PERSONA %d: %s ===\n%s",
                    "=" * 60, persona["index"], persona["company_name"], "=" * 60)

        run = _new_run(persona)
        all_runs.append(run)

        try:
            # Phase 1: Account bootstrap
            t = time.time()
            await phase_account(run, persona)
            run["timings"]["account_s"] = round(time.time() - t, 1)
            save_persona(run, run_dir)
            save_summary(all_runs, run_dir, run_meta)

            if not run.get("account_id"):
                raise RuntimeError("account_id not set after phase_account")

            # Phase 2: Onboarding profile
            t = time.time()
            await phase_profile(run, persona)
            run["timings"]["profile_s"] = round(time.time() - t, 1)
            save_persona(run, run_dir)
            save_summary(all_runs, run_dir, run_meta)

            # Phase 3: Sender voice synthesis
            t = time.time()
            await phase_sender_voice(run, persona)
            run["timings"]["sender_voice_s"] = round(time.time() - t, 1)
            save_persona(run, run_dir)
            save_summary(all_runs, run_dir, run_meta)

            # Phase 4: Company sourcing + prospect scraping
            t = time.time()
            prospects = await phase_scraping(run, persona, count=count, mock_prospects=mock_prospects)
            run["timings"]["scraping_s"] = round(time.time() - t, 1)
            save_persona(run, run_dir)
            save_summary(all_runs, run_dir, run_meta)

            # Phase 5+6: Campaign launch + Day-1 messages
            if prospects:
                t = time.time()
                campaign_id = await phase_launch(run, persona, prospects, with_topup=with_topup)
                run["timings"]["launch_s"] = round(time.time() - t, 1)
                save_persona(run, run_dir)
                save_summary(all_runs, run_dir, run_meta)

                # Phase 7: Fetch & record Day-1 messages
                t = time.time()
                await phase_day1_messages(run, persona)
                run["timings"]["messages_s"] = round(time.time() - t, 1)
                save_persona(run, run_dir)
                save_summary(all_runs, run_dir, run_meta)

                # Phase 8: Final assertions
                t = time.time()
                await phase_assertions(run)
                run["timings"]["assertions_s"] = round(time.time() - t, 1)
                save_persona(run, run_dir)
                save_summary(all_runs, run_dir, run_meta)
            else:
                logger.warning("[%s] No prospects — skipping campaign launch & messages", slug)
                _check(run, "campaign_launch", False, "skipped — 0 prospects from scraping")
                _check(run, "day1_messages_generated", False, "skipped — 0 prospects")

        except Exception as exc:
            logger.exception("[%s] Unexpected error: %s", slug, exc)
            run["error"] = traceback.format_exc()
            run["status"] = "ERROR"
            _check(run, "unexpected_error", False, str(exc))
            save_persona(run, run_dir)
            save_summary(all_runs, run_dir, run_meta)
            continue

        # Determine overall persona status
        has_fail = any(c["status"] == "FAIL" for c in run.get("checks", []))
        run["status"] = "FAIL" if has_fail else "PASS"
        run["timings"]["total_s"] = round(sum(
            v for k, v in run["timings"].items() if k != "total_s"
        ), 1)
        save_persona(run, run_dir)
        save_summary(all_runs, run_dir, run_meta)

        logger.info("[%s] Persona complete: %s  (%d checks, fails=%d)",
                    slug, run["status"],
                    len(run["checks"]),
                    sum(1 for c in run["checks"] if c["status"] == "FAIL"))

    # Finalize run meta
    total_elapsed = time.time() - total_start
    run_meta["ended_at"] = datetime.utcnow().isoformat()
    run_meta["total_elapsed_s"] = round(total_elapsed, 1)
    _atomic_write(run_dir / "_run_meta.json", run_meta)
    save_summary(all_runs, run_dir, run_meta)

    # Console summary table
    _print_final_table(all_runs)

    passes = sum(1 for r in all_runs if r.get("status") == "PASS")
    fails = sum(1 for r in all_runs if r.get("status") in ("FAIL", "ERROR"))
    print(f"Results saved to: {run_dir}")
    print(f"Total: {passes} PASS, {fails} FAIL — {total_elapsed:.0f}s\n")

    # Cleanup
    if do_cleanup:
        logger.info("--- Cleanup ---")
        for run, persona in zip(all_runs, active_personas):
            try:
                await cleanup_persona(run, persona)
            except Exception as exc:
                logger.warning("Cleanup failed for %s: %s", persona["slug"], exc)

    if fails > 0:
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-persona e2e test: Onboarding → Sender Voice → Scraping → Campaign → Day-1 Messages"
    )
    parser.add_argument("--cleanup", action="store_true", help="Delete all test data after run")
    parser.add_argument("--count", type=int, default=5, help="Prospects to source per persona (default: 5)")
    parser.add_argument(
        "--personas",
        type=str,
        default="",
        help="Comma-separated 1-based persona indexes to run, e.g. --personas 1,3 (default: all 5)",
    )
    parser.add_argument(
        "--with-topup",
        action="store_true",
        help="Allow background top-up discovery (adds Gemini+Apify cost per persona; default: disabled)",
    )
    parser.add_argument(
        "--mock-prospects",
        action="store_true",
        help=(
            "Skip Gemini company sourcing + Apify employee scraping. "
            "Use hardcoded prospect fixtures to test sender voice, campaign launch, "
            "and Day-1 message generation without any external scraping. "
            "Required when Apify actor Vb6LZkh4EqRlR0Ka9 needs permission approval — "
            "visit https://console.apify.com/actors/Vb6LZkh4EqRlR0Ka9?approvePermissions=true to fix."
        ),
    )
    args = parser.parse_args()

    indexes = None
    if args.personas:
        try:
            indexes = [int(x.strip()) for x in args.personas.split(",") if x.strip()]
        except ValueError:
            print("ERROR: --personas must be comma-separated integers, e.g. --personas 1,3")
            sys.exit(1)

    asyncio.run(main(
        do_cleanup=args.cleanup,
        count=args.count,
        persona_indexes=indexes,
        with_topup=args.with_topup,
        mock_prospects=args.mock_prospects,
    ))
