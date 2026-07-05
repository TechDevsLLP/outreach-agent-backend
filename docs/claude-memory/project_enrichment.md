---
name: OutFlo Enrichment Pipeline
description: How the enrichment pipeline works, file paths, prompt locations, and AI model usage
type: project
originSessionId: 17255886-36f3-43a1-ab77-f9eefab38896
---
Enrichment pipeline at `backend/services/enrichment_pipeline.py` — orchestrates a 5-phase flow per prospect.

**Why:** Core product value — personalized AI assessment and outreach messages based on scraped + enriched data.

**How to apply:** When modifying enrichment logic, start with `enrichment_pipeline.py`. Prompts live in `backend/utils/prompts.py`. AI calls go through `backend/services/openrouter_service.py`.

## Pipeline Phases
```
Phase 0:   Setup — fetch prospects, validate LinkedIn URLs
Phase 0.5: Rule-based triage — decision maker detection (utils/scoring.py)
Phase 1:   LinkedIn profile scraping (Apify PROFILE_SCRAPER: 2SyF0bVxmgGr8IVCZ)
Phase 2:   Company scraping (Apify COMPANY_SCRAPER: wHMoznVs94gOcxcZl) + deduplication
Phase 2.5: Competitor research (parallel with Phase 3)
Phase 3:   AI assessment (Claude Sonnet 4.5, temp 0.2) → fit_score 0-100, priority_tier
Phase 3.5: Contact discovery — find alternate contacts in high-fit companies
Phase 4:   Outreach generation (Gemini 2.5 Flash, temp 0.7) → A/B messages
Phase 5:   Finalize — compute timezones, optimal send times
```

## Prompt Locations (`backend/utils/prompts.py`)
- `ASSESSMENT_SYSTEM_PROMPT` + `build_assessment_user_prompt()` → prospect fit scoring
  - Input: prospect data, LinkedIn profile, company data, company_profiles ICP
  - Output: fit_rating, fit_score, company_fit_score, prospect_fit_score, buying_signals, psychographic_profile, recommended_services, priority_tier, estimated_deal_size
- `CAMPAIGN_OUTREACH_SYSTEM_PROMPT` + `build_campaign_outreach_prompt()` → message generation
  - Input: campaign tone/value_prop/pain_point/CTA + prospect + profile + AI assessment
  - Output: cold_email (subject_a, subject_b, body), linkedin_connection (≤280 chars), linkedin_inmail

## AI Assessment Service (`backend/services/ai_assessment_service.py`)
- Hybrid scoring: 60% AI fit_score + 40% rule-based
- Rule factors: seniority (10pts), digital maturity gap (10pts), company size match, funding stage
- priority_tier: hot(≥80), warm(60-79), cold(<60)

## Smart Campaign Mini-Enrichment
During campaign discovery (campaign_prospect_finder_service.py), new Apify-sourced prospects get:
1. LinkedIn profile scrape only
2. AI assessment only (no full pipeline)
This is faster but less thorough than the full enrichment pipeline.

## Apollo Data on Prospects
Pre-loaded fields used in assessment prompts: companyDescription, companyKeywords, companyTechnologies, companyAnnualRevenue, companyTotalFunding — these provide richer AI context without additional scraping.
