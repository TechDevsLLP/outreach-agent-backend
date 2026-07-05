---
name: project-linkedin-post-scraper
description: LinkedIn post scraper actor and prospect_intelligence generation details
metadata: 
  node_type: memory
  type: project
  originSessionId: 89d972b2-2d4e-4828-98a0-d200802b5071
---

# LinkedIn Post Scraper & Prospect Intelligence

## Actor

Actor ID: `r4oNX7IHlW4RQAjKP`

Input:
```json
{"usernames": [linkedin_urls], "limit": 5, "total_posts": null}
```

Output fields used: `text`, `posted_at.date`, `stats.total_reactions`, `stats.comments`, `url`, `post_type`, `author.username`

## prospect_intelligence Schema

Stored as a sub-document on `prospect.prospect_intelligence` in MongoDB.

| Field | Type | Description |
|-------|------|-------------|
| `writing_voice` | str | Tone/style of the prospect's writing |
| `top_topics` | list[str] | Recurring themes in their posts |
| `pain_signals` | list[str] | Problems/frustrations surfaced in posts |
| `best_hook` | str | Specific post text to reference in outreach |
| `pitch_angle` | str | Most resonant angle for the pitch |
| `why_they_need_us` | str | Personalized reason they need OutFlo |
| `competitors` | list[str] | Competitors mentioned or implied (grounded Gemini search) |
| `dont_pitch` | list[str] | Topics/angles to avoid |
| `engagement_style` | str | How they interact (curious, promotional, thought-leader, etc.) |

## Services

- **Post scraper**: `backend/services/linkedin_post_scraper_service.py`
- **Intelligence generation**: `backend/services/prospect_intelligence_service.py`

## Generation Details

- Model: `gemini-2.5-flash`
- Batching: 5 prospects per Gemini call
- Competitor finding folded into the same Gemini call (grounded search)
- Runs as Step 1–2 of the Deep Enrichment Pipeline on the Day-1 cohort (45 prospects) before message generation

## Frontend

Component: `ProspectIntelCard` in `/frontend/components/campaigns/`
- Collapsible sections per intelligence field
- Shown in campaign prospect view (Intel Card)

## Pipeline Placement

Deep Enrichment Pipeline (Day-1 cohort, runs before message gen):
1. Bulk LinkedIn post scrape — actor `r4oNX7IHlW4RQAjKP`, 5 posts, all 45 URLs batched in one call
2. Gemini 2.5 Flash batched 5/call → generates `prospect_intelligence` → stored on prospect document
3. Message gen prompt updated to use `prospect_intelligence` fields:
   - Email: opens with `best_hook`, mirrors `writing_voice`, targets `pain_signals[0]`
   - Connection note: matches tone exactly
   - InMail: references `pain_signal`, names competitors

Days 2–5: enrichment runs in background during Day 1's 24h send window.
