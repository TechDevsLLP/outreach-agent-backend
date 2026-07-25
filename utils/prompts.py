"""
Prompt templates for AI assessment and outreach generation.
Keeps prompts out of service logic for maintainability.
"""

import re
import json
import time
import logging

logger = logging.getLogger(__name__)

_prompt_cache: dict[tuple[str, str], tuple[str, float]] = {}
_PROMPT_CACHE_TTL = 60  # seconds


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_company(company: dict | None) -> dict:
    if not company:
        return {}
    return {
        "name": company.get("name") or company.get("companyName") or "",
        "description": company.get("description") or "",
        "industry": company.get("industry") or "",
        "size": company.get("employeeCount") or company.get("size") or "",
        "followers": company.get("followerCount") or company.get("followersCount") or 0,
        "specialties": company.get("specialties") or [],
        "website": company.get("websiteUrl") or company.get("website") or "",
    }


def _stringify_list(items, max_items: int = 5) -> str:
    if not items:
        return "None"
    items = [str(i) for i in items if i][:max_items]
    return ", ".join(items) if items else "None"


def _mentions_competitors(text: str, competitors: list) -> bool:
    if not text or not competitors:
        return False
    t = text.lower()
    if any(kw in t for kw in ("competitor", " vs ", " versus ")):
        return True
    for c in competitors:
        name = (c.get("name") or str(c)).lower()
        if name and name in t:
            return True
    return False


# ── Assessment ────────────────────────────────────────────────────────────────

ASSESSMENT_SYSTEM_PROMPT = """You are an expert B2B sales analyst evaluating LinkedIn prospects for fit with a seller's ICP (Ideal Customer Profile).

Your job: analyze the prospect's professional profile, seniority, company signals, and the seller's offering to determine how strong a fit this person is as a sales target.

## Scoring dimensions
1. **Company fit** (company_fit_score 0-100): Does the company match the ICP? Consider: industry, size, growth stage, tech stack, digital maturity gaps, funding.
2. **Prospect fit** (prospect_fit_score 0-100): Is this the right person? Consider: seniority, decision-making authority, role relevance, pain likelihood. When Campaign Context includes Target Titles, Target Seniority, and/or Target Departments, these MUST heavily influence prospect_fit_score. A prospect whose headline or title closely matches a target title should score 80+; a clear mismatch (e.g., targeting "Marketing Manager" but prospect is a Software Engineer) should score ≤40 regardless of other signals.
3. **Overall fit** (fit_score 0-100): Weighted blend (60% company, 40% prospect).
4. **Priority tier**: tier_1 (≥80), tier_2 (60–79), tier_3 (<60).

## Fit ratings
- excellent: fit_score ≥ 80
- good: 65–79
- moderate: 50–64
- poor: <50

## Output — return ONLY valid JSON matching this schema exactly:
```json
{
  "fit_score": 0,
  "fit_rating": "excellent|good|moderate|poor",
  "company_fit_score": 0,
  "prospect_fit_score": 0,
  "priority_tier": "tier_1|tier_2|tier_3",
  "reasoning": "2-3 sentence explanation of the score and key fit signals",
  "buying_signals": ["signal 1", "signal 2"],
  "strengths": ["strength 1", "strength 2"],
  "recommended_services": ["service 1"],
  "psychographic_profile": "Brief description of likely personality/motivations"
}
```

Be direct. Base scores on evidence in the data provided. If data is sparse, default to moderate scores with a reasoning note."""


def _build_icp_section(company_profile: dict) -> str:
    """Build ICP context section from seller's company profile."""
    if not company_profile:
        return ""
    parts = ["## Seller ICP Definition"]
    if company_profile.get("company_name"):
        parts.append(f"Seller: {company_profile['company_name']}")
    if company_profile.get("industry"):
        parts.append(f"Seller Industry: {company_profile['industry']}")
    if company_profile.get("value_proposition"):
        parts.append(f"Value Proposition: {company_profile['value_proposition']}")
    if company_profile.get("target_customer_description"):
        parts.append(f"Target Customer: {company_profile['target_customer_description']}")
    if company_profile.get("icp_industries"):
        parts.append(f"ICP Industries: {_stringify_list(company_profile['icp_industries'])}")
    if company_profile.get("icp_job_titles"):
        parts.append(f"ICP Job Titles: {_stringify_list(company_profile['icp_job_titles'])}")
    if company_profile.get("icp_seniority_levels"):
        parts.append(f"ICP Seniority: {_stringify_list(company_profile['icp_seniority_levels'])}")
    if company_profile.get("icp_company_size_min") or company_profile.get("icp_company_size_max"):
        size_min = company_profile.get("icp_company_size_min", 0)
        size_max = company_profile.get("icp_company_size_max", 0)
        if size_min and size_max:
            parts.append(f"ICP Company Size: {size_min}–{size_max} employees")
        elif size_min:
            parts.append(f"ICP Company Size: {size_min}+ employees")
    if company_profile.get("services"):
        parts.append(f"Services: {_stringify_list(company_profile['services'])}")
    if company_profile.get("differentiators"):
        parts.append(f"Differentiators: {_stringify_list(company_profile['differentiators'])}")
    return "\n".join(parts)


def build_assessment_user_prompt(
    lead: dict,
    profile: dict | None,
    company: dict | None,
    company_profile: dict | None = None,
    campaign: dict | None = None,
) -> str:
    """Build the user-turn prompt for a single prospect assessment."""
    parts = []

    if company_profile:
        icp_section = _build_icp_section(company_profile)
        if icp_section:
            parts.append(icp_section)
            parts.append("")

    if campaign:
        parts.append("## Campaign Context")
        if campaign.get("name"):
            parts.append(f"Campaign: {campaign['name']}")
        if campaign.get("description"):
            parts.append(f"Campaign Description: {str(campaign['description'])[:300]}")
        if campaign.get("value_proposition"):
            parts.append(f"Value Proposition: {campaign['value_proposition']}")
        if campaign.get("pain_point"):
            parts.append(f"Target Pain: {campaign['pain_point']}")
        if campaign.get("icp_job_titles"):
            parts.append(f"Target Titles: {_stringify_list(campaign['icp_job_titles'])}")
        if campaign.get("icp_seniority_levels"):
            parts.append(f"Target Seniority: {_stringify_list(campaign['icp_seniority_levels'])}")
        if campaign.get("icp_functional_departments"):
            parts.append(f"Target Departments: {_stringify_list(campaign['icp_functional_departments'])}")
        parts.append("")

    parts.append("## Prospect")
    parts.append(f"Name: {lead.get('full_name') or (str(lead.get('first_name', '')) + ' ' + str(lead.get('last_name', ''))).strip() or 'Unknown'}")
    parts.append(f"Title: {lead.get('title') or lead.get('job_title') or 'Unknown'}")
    parts.append(f"Company: {lead.get('company_name') or lead.get('company') or 'Unknown'}")
    parts.append(f"Industry: {lead.get('industry') or 'Unknown'}")
    parts.append(f"Location: {lead.get('location') or lead.get('country') or 'Unknown'}")
    parts.append(f"Seniority: {lead.get('seniority_level') or 'Unknown'}")
    parts.append(f"Company Size: {lead.get('company_size') or 'Unknown'}")
    if lead.get('company_total_funding_clean'):
        parts.append(f"Total Funding: {lead['company_total_funding_clean']}")

    if profile:
        parts.append("")
        parts.append("## LinkedIn Profile")
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        if profile.get("summary") or profile.get("about"):
            about = (profile.get("summary") or profile.get("about") or "")[:500]
            parts.append(f"About: {about}")
        if profile.get("connectionsCount"):
            parts.append(f"Connections: {profile['connectionsCount']}")
        if profile.get("followersCount"):
            parts.append(f"Followers: {profile['followersCount']}")
        experiences = profile.get("experiences") or profile.get("positions") or []
        if experiences:
            parts.append("Recent Experience:")
            for exp in experiences[:3]:
                title = exp.get("title") or exp.get("jobTitle") or ""
                company = exp.get("companyName") or exp.get("company") or ""
                if title or company:
                    parts.append(f"  - {title} at {company}")
        posts = profile.get("posts") or profile.get("recentPosts") or []
        if posts:
            parts.append(f"Recent Post Activity: {len(posts)} posts found")
            for post in posts[:2]:
                text = (post.get("text") or post.get("content") or "")[:150]
                if text:
                    parts.append(f"  Post: {text}...")
        skills = profile.get("skills") or []
        if skills:
            parts.append(f"Skills: {_stringify_list(skills, 8)}")

    if company:
        co = _normalize_company(company)
        if co.get("name") or co.get("description"):
            parts.append("")
            parts.append("## Company Data")
            if co.get("description"):
                parts.append(f"Description: {co['description'][:300]}")
            if co.get("industry"):
                parts.append(f"Industry: {co['industry']}")
            if co.get("size"):
                parts.append(f"Size: {co['size']}")
            if co.get("followers"):
                parts.append(f"LinkedIn Followers: {co['followers']}")
            if co.get("specialties"):
                specs = co["specialties"]
                if isinstance(specs, list):
                    parts.append(f"Specialties: {_stringify_list(specs, 5)}")

    tech_stack = lead.get("company_technologies") or []
    if tech_stack:
        parts.append(f"Technology Stack: {_stringify_list(tech_stack, 8)}")

    buying_signals = lead.get("buying_signals") or []
    if buying_signals:
        parts.append(f"Detected Buying Signals: {_stringify_list(buying_signals, 5)}")

    competitors = lead.get("competitors") or []
    if competitors:
        names = [c.get("name") or str(c) for c in competitors[:3] if c]
        if names:
            parts.append(f"Competitors in Space: {', '.join(names)}")

    parts.append("")
    parts.append("Please analyze this prospect and return your assessment as JSON.")
    return "\n".join(parts)


def build_batch_assessment_user_prompt(
    prospects_data: list[dict],
    campaign: dict | None = None,
) -> str:
    """Build a user-turn prompt that assesses multiple prospects in one call."""
    parts = ["Assess the following prospects and return a JSON object with an 'assessments' array, one entry per prospect in the SAME ORDER as the input."]
    parts.append("")

    if campaign:
        parts.append("## Campaign Context")
        if campaign.get("name"):
            parts.append(f"Campaign: {campaign['name']}")
        if campaign.get("description"):
            parts.append(f"Campaign Description: {str(campaign['description'])[:300]}")
        if campaign.get("value_proposition"):
            parts.append(f"Value Proposition: {campaign['value_proposition']}")
        if campaign.get("pain_point"):
            parts.append(f"Pain Point Targeted: {campaign['pain_point']}")
        if campaign.get("icp_job_titles"):
            parts.append(f"Target Titles: {_stringify_list(campaign['icp_job_titles'])}")
        if campaign.get("icp_seniority_levels"):
            parts.append(f"Target Seniority: {_stringify_list(campaign['icp_seniority_levels'])}")
        if campaign.get("icp_functional_departments"):
            parts.append(f"Target Departments: {_stringify_list(campaign['icp_functional_departments'])}")
        parts.append("")

    for i, pd in enumerate(prospects_data, 1):
        lead = pd.get("lead") or {}
        profile = pd.get("profile") or {}
        company = pd.get("company") or {}
        company_profile = pd.get("company_profile")

        parts.append(f"### Prospect {i}")
        parts.append(f"Name: {lead.get('full_name') or (str(lead.get('first_name', '')) + ' ' + str(lead.get('last_name', ''))).strip() or 'Unknown'}")
        parts.append(f"Title: {lead.get('title') or lead.get('job_title') or 'Unknown'}")
        parts.append(f"Company: {lead.get('company_name') or lead.get('company') or 'Unknown'}")
        parts.append(f"Industry: {lead.get('industry') or 'Unknown'}")
        parts.append(f"Seniority: {lead.get('seniority_level') or 'Unknown'}")
        parts.append(f"Company Size: {lead.get('company_size') or 'Unknown'}")
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        if profile.get("summary") or profile.get("about"):
            about = (profile.get("summary") or profile.get("about") or "")[:200]
            parts.append(f"About: {about}")
        co = _normalize_company(company)
        if co.get("description"):
            parts.append(f"Company: {co['description'][:200]}")
        tech_stack = lead.get("company_technologies") or []
        if tech_stack:
            parts.append(f"Tech Stack: {_stringify_list(tech_stack, 5)}")
        if company_profile:
            icp = _build_icp_section(company_profile)
            if icp:
                parts.append(icp)
        parts.append("")

    parts.append(f"Return JSON: {{\"assessments\": [<{len(prospects_data)} assessment objects>]}}")
    parts.append("Each assessment must have: fit_score, fit_rating, company_fit_score, prospect_fit_score, priority_tier, reasoning, buying_signals, strengths, recommended_services, psychographic_profile.")
    return "\n".join(parts)


# ── V2 One-Off Outreach (prospect detail dialog) ──────────────────────────────

OUTREACH_SYSTEM_PROMPT_V2 = """You are an expert B2B outreach copywriter. Your job is to write highly personalized, direct cold outreach messages that feel human and drive responses.

## Core Principles
- Every message must reference a SPECIFIC, CONCRETE signal about this person/company (funding, post, technology, news, role change, competitor)
- Open with the prospect's world, not yours — lead with something you observed, not a claim about your product
- Be direct and conversational, not formal or salesy
- Short is better: cold email body ≤150 words, LinkedIn connection request ≤280 chars
- One clear call to action per message — no multiple asks
- Email sign-off must be EXACTLY "Best," on one line, then the SENDER's first name on the next line. Never sign with the prospect's name. If no sender name is given, end after the CTA with no name at all.

## Banned openers (NEVER use):
- "I hope this finds you well"
- "I noticed you" / "I came across your profile"
- "Just wanted to reach out"
- "Quick question"
- "I was impressed by"

## Banned phrases:
- "innovative solutions", "drive growth", "synergy", "leverage", "circle back", "touch base", "value-add", "game-changer", "cutting-edge"

## Output — return ONLY valid JSON:
```json
{
  "cold_email": {
    "body": "Email body text here (no subject — subject is separate)",
    "subject_variants": [
      {"variant_id": "A", "subject": "Subject line A (most direct)", "is_selected": true},
      {"variant_id": "B", "subject": "Subject line B (curiosity angle)", "is_selected": false},
      {"variant_id": "C", "subject": "Subject line C (benefit/outcome angle)", "is_selected": false}
    ]
  },
  "linkedin_connection_request": {
    "message": "Connection request note ≤280 chars"
  },
  "linkedin_followup": {
    "message": "Follow-up message if no response (≤150 words)"
  },
  "linkedin_inmail": {
    "subject": "InMail subject ≤50 chars",
    "message": "InMail body ≤200 words"
  }
}
```"""


def build_outreach_user_prompt_v2(
    prospect: dict,
    profile: dict | None,
    company: dict | None,
    assessment: dict | None,
    competitors: list | None = None,
    custom_context: str | None = None,
) -> str:
    """Build user-turn prompt for V2 one-off outreach generation."""
    parts = []

    parts.append("## Prospect")
    full_name = prospect.get("full_name") or (
        (prospect.get("first_name") or "") + " " + (prospect.get("last_name") or "")
    ).strip() or "Unknown"
    parts.append(f"Name: {full_name}")
    parts.append(f"First Name: {prospect.get('first_name') or full_name.split()[0]}")
    parts.append(f"Title: {prospect.get('title') or prospect.get('job_title') or 'Unknown'}")
    parts.append(f"Company: {prospect.get('company_name') or prospect.get('company') or 'Unknown'}")
    parts.append(f"Industry: {prospect.get('industry') or 'Unknown'}")
    parts.append(f"Seniority: {prospect.get('seniority_level') or 'Unknown'}")
    parts.append(f"Location: {prospect.get('location') or prospect.get('country') or 'Unknown'}")

    if profile:
        parts.append("")
        parts.append("## LinkedIn Profile")
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        if profile.get("summary") or profile.get("about"):
            about = (profile.get("summary") or profile.get("about") or "")[:400]
            parts.append(f"About: {about}")
        posts = profile.get("posts") or profile.get("recentPosts") or []
        if posts:
            parts.append("Recent Posts:")
            for post in posts[:3]:
                text = (post.get("text") or post.get("content") or "")[:200]
                if text:
                    parts.append(f"  - {text}...")

    co = _normalize_company(company)
    if co.get("name") or co.get("description"):
        parts.append("")
        parts.append("## Company")
        if co.get("description"):
            parts.append(f"Description: {co['description'][:300]}")
        if co.get("industry"):
            parts.append(f"Industry: {co['industry']}")
        if co.get("size"):
            parts.append(f"Size: {co['size']}")
        if co.get("followers"):
            parts.append(f"LinkedIn Followers: {co['followers']}")

    tech_stack = prospect.get("company_technologies") or []
    if tech_stack:
        parts.append(f"Technology Stack: {_stringify_list(tech_stack, 6)}")

    news = prospect.get("company_news") or []
    if news:
        parts.append("")
        parts.append("## Recent Company News")
        for item in news[:2]:
            title = item.get("title") or ""
            summary = item.get("summary") or ""
            if title:
                parts.append(f"  - {title}: {summary[:150]}")

    if assessment:
        parts.append("")
        parts.append("## AI Assessment")
        parts.append(f"Fit Score: {assessment.get('fit_score', 'N/A')}/100")
        parts.append(f"Rating: {assessment.get('fit_rating', 'N/A')}")
        if assessment.get("reasoning"):
            parts.append(f"Reasoning: {assessment['reasoning'][:200]}")
        signals = assessment.get("buying_signals") or []
        if signals:
            parts.append(f"Buying Signals: {_stringify_list(signals, 4)}")
        recommended = assessment.get("recommended_services") or []
        if recommended:
            parts.append(f"Recommended Services: {_stringify_list(recommended, 3)}")

    if competitors and not _mentions_competitors("", []):
        valid_comps = [c for c in competitors[:3] if c]
        if valid_comps:
            parts.append("")
            parts.append("## Competitor Context")
            for c in valid_comps:
                name = c.get("name") or str(c)
                diff = c.get("differentiation") or c.get("description") or ""
                parts.append(f"  - {name}: {diff[:100]}" if diff else f"  - {name}")

    if custom_context:
        parts.append("")
        parts.append(f"## Additional Context\n{custom_context}")

    parts.append("")
    parts.append("Generate personalized outreach messages for all 4 channels. Use the most specific signal available. Return only valid JSON.")
    return "\n".join(parts)


# ── Employee Ranking ──────────────────────────────────────────────────────────

EMPLOYEE_RANKING_SYSTEM_PROMPT = """You are a B2B sales expert who evaluates LinkedIn employees to identify the best prospects for outreach.

Your goal: rank employees by their likelihood of being the key decision-maker or economic buyer for a B2B SaaS/services product.

## Scoring criteria (0-100):
- Seniority: C-suite/Owner/Founder (90-100), VP/Director (70-89), Manager (50-69), Individual (20-49)
- Decision-making authority: budget holder, strategic role, team lead
- Growth/change signals: recent promotions, new role, company expansion
- Digital presence gap: low LinkedIn engagement for seniority = higher opportunity

## Output — return ONLY valid JSON:
```json
{
  "ranked_employees": [
    {
      "linkedin_url": "url",
      "score": 85,
      "reasoning": "Brief explanation",
      "recommended": true
    }
  ]
}
```

Rank in descending order by score. Only include employees with score >= 40."""


# ── Campaign Outreach ─────────────────────────────────────────────────────────

CAMPAIGN_OUTREACH_SYSTEM_PROMPT = """You are an elite B2B outreach copywriter generating personalized sales messages for a smart campaign.

## Your output
Return ONLY a JSON object with this exact schema:
```json
{
  "cold_email": {
    "subject_a": "Primary subject line (direct benefit)",
    "subject_b": "Alternative subject line (curiosity/question angle)",
    "body": "Email body — no subject, no greeting line salutation at start; end with EXACTLY 'Best,' on its own line then the SENDER's first name on the next line (never the prospect's name)"
  },
  "linkedin_connection": {
    "note": "Connection request note — HARD LIMIT 280 chars"
  },
  "linkedin_inmail": {
    "subject": "InMail subject ≤55 chars",
    "body": "InMail body"
  }
}
```

## Tone modes

**professional**: Authoritative, data-driven, ROI-focused. Clear problem → solution → outcome arc.
- Opens with an industry-specific observation or metric
- Leads with business impact
- Formal but not stiff

**challenger**: Provocative, pattern-interrupting, assumption-challenging.
- Opens by challenging a common belief or decision the prospect has made
- "Most [role]s think X, but the data shows Y"
- Respectfully confrontational, invites debate

**conversational**: Warm, direct, peer-to-peer. Like an email from a smart colleague.
- Opens with a genuine observation about their work or company
- Short sentences, casual tone
- Feels like a real human wrote it

**empathetic**: Builds connection through shared struggle and understanding first.
- Opens by acknowledging the pressures of their role/industry
- Validates their challenges before mentioning any solution
- Soft, supportive close

## Hard rules (apply to ALL tones)

1. **Hyperpersonalization**: Every body MUST reference one concrete signal — recent company news, a post they wrote, a funding event, a specific technology they use, a competitor, or a growth signal. Generic messages fail.

2. **Banned openers** (NEVER use): "I hope this finds you well", "I noticed you", "I came across", "Just wanted to reach out", "Quick question", "Hope you're well", "My name is"

3. **Banned phrases**: "innovative solutions", "drive growth", "synergy", "leverage", "circle back", "touch base", "value-add", "game-changer", "cutting-edge", "best-in-class", "world-class"

4. **Length**: cold email body 80-130 words, LinkedIn note ≤280 chars (hard limit), InMail body 100-160 words. The LinkedIn connection note MUST be a COMPLETE thought that ends on a full sentence — aim for 200-260 chars so it never gets cut off. A short complete note always beats a longer one that runs past 280 and gets truncated mid-sentence.

5. **One ask**: Exactly one CTA. No "or" options, no multiple links. Match CTA to the campaign's cta_type.

6. **Sign-off**: Email bodies end with EXACTLY two lines: "Best," then the SENDER's first name (never last name, and NEVER the prospect's name). If no sender name is provided, end after the CTA with no name in the sign-off at all. No "Best regards", no "Thanks in advance". LinkedIn notes/InMails: if signed, sender first name only — never the prospect's.

7. **Subject lines**: ≤55 chars, no clickbait, no ALL CAPS, no emojis."""


TONE_DESCRIPTIONS = {
    "professional": "Authoritative and ROI-focused. Opens with industry observation. Business impact lead.",
    "challenger": "Pattern-interrupting. Challenges assumptions. Respectfully confrontational.",
    "conversational": "Warm peer-to-peer. Short sentences. Feels like a colleague wrote it.",
    "empathetic": "Validates struggles first. Supportive and understanding. Soft close.",
}


def _select_top_signal(prospect: dict) -> tuple[str, str]:
    """Return (signal_kind, signal_text) using priority: news > post > buying_signal > tech > funding."""
    news = prospect.get("company_news") or []
    if news:
        item = news[0]
        title = item.get("title") or ""
        summary = item.get("summary") or ""
        return ("recent_news", f"{title}: {summary}"[:200] if title else summary[:200])

    profile = prospect.get("linkedin_profile_data") or {}
    posts = profile.get("posts") or profile.get("recentPosts") or []
    if posts:
        text = (posts[0].get("text") or posts[0].get("content") or "")[:200]
        if text:
            return ("recent_post", text)

    signals = prospect.get("buying_signals") or []
    if signals:
        return ("buying_signal", str(signals[0]))

    tech = prospect.get("company_technologies") or []
    if tech:
        return ("technology_stack", _stringify_list(tech[:3]))

    funding = prospect.get("company_total_funding_clean")
    if funding:
        return ("funding_event", f"Total funding: {funding}")

    return ("none", "")


def _select_best_case_study(case_studies: list, prospect: dict) -> dict | None:
    """Pick the case study whose industry best matches the prospect's industry.

    Entries may be dicts ({client, outcome, metric, industry} from the
    onboarding analyzer) or plain strings (manual Settings edits) — strings are
    normalized to {"outcome": <text>}.
    """
    normalized = []
    for cs in case_studies or []:
        if isinstance(cs, dict):
            normalized.append(cs)
        elif isinstance(cs, str) and cs.strip():
            normalized.append({"client": "", "outcome": cs.strip(), "metric": None, "industry": None})
    if not normalized:
        return None
    prospect_industry = (prospect.get("industry") or "").lower()
    if not prospect_industry:
        return normalized[0]
    for cs in normalized:
        cs_industry = (cs.get("industry") or cs.get("client") or "").lower()
        if cs_industry and (prospect_industry in cs_industry or cs_industry in prospect_industry):
            return cs
    return normalized[0]


def _format_funding_line(funding: dict | None) -> str:
    """Compact one-line summary of the research `funding` block.
    E.g. 'Raised $30M Series B in 2026-03 — investors: Accel, Index'. Empty
    string when nothing usable is present."""
    if not isinstance(funding, dict):
        return ""
    round_ = str(funding.get("latest_round") or "").strip()
    amount = str(funding.get("amount") or "").strip()
    date = str(funding.get("date") or "").strip()
    summary = str(funding.get("summary") or "").strip()
    bits = []
    if amount and round_:
        bits.append(f"Raised {amount} {round_}")
    elif amount:
        bits.append(f"Raised {amount}")
    elif round_:
        bits.append(round_)
    if bits and date:
        bits.append(f"in {date}")
    line = " ".join(bits)
    investors = funding.get("investors") or []
    if isinstance(investors, list) and investors:
        inv = ", ".join(str(i) for i in investors[:3])
        line = (line + " — investors: " + inv) if line else ("Investors: " + inv)
    if not line and summary:
        line = summary[:150]
    return line


def build_campaign_outreach_prompt(
    prospect: dict,
    profile: dict | None,
    company: dict | None,
    campaign: dict,
    additional_instructions: str | None = None,
    company_profile: dict | None = None,
    intelligence: dict | None = None,
) -> str:
    """Build the user-turn prompt for single-prospect campaign message generation."""
    parts = []

    # Tone
    tone = campaign.get("message_tone") or "professional"
    tone_desc = TONE_DESCRIPTIONS.get(tone, TONE_DESCRIPTIONS["professional"])
    parts.append(f"## Tone: {tone.upper()}")
    parts.append(tone_desc)
    parts.append("")

    # Seller identity
    sender_name = campaign.get("sender_name") or (company_profile or {}).get("sender_name") or ""
    sender_first = sender_name.split()[0] if sender_name else ""
    seller_company = campaign.get("seller_company") or (company_profile or {}).get("company_name") or ""
    sender_role = (company_profile or {}).get("sender_role") or ""

    parts.append("## Seller Identity")
    if sender_first:
        parts.append(f"Sender First Name: {sender_first}")
        parts.append(
            f'Sign off the email body exactly as: "Best,\\n{sender_first}" '
            f'(that is: "Best," on one line, then "{sender_first}" on the next line).'
        )
    else:
        parts.append(
            "Sender name unknown — do not include any name in the sign-off, "
            "and NEVER sign with the prospect's name."
        )
    if sender_role:
        parts.append(f"Sender Role: {sender_role}")
    if seller_company:
        parts.append(f"Company: {seller_company}")

    # Seller value context
    if campaign.get("value_proposition"):
        parts.append(f"Value Proposition: {campaign['value_proposition']}")
    if campaign.get("pain_point"):
        parts.append(f"Pain Point Targeted: {campaign['pain_point']}")

    # Services/differentiators from company profile
    if company_profile:
        services = company_profile.get("services") or []
        if services:
            parts.append(f"Services: {_stringify_list(services[:3])}")
        diffs = company_profile.get("differentiators") or []
        if diffs:
            parts.append(f"Differentiators: {_stringify_list(diffs[:3])}")
        target_market = company_profile.get("target_market") or ""
        if target_market:
            parts.append(f"Target Market: {target_market}")

    # Best-matched case study
    if company_profile:
        case_studies = company_profile.get("case_studies") or []
        best_cs = _select_best_case_study(case_studies, prospect)
        if best_cs:
            client = best_cs.get("client") or ""
            outcome = best_cs.get("outcome") or ""
            metric = best_cs.get("metric") or ""
            label = f"{client} — {outcome}" if client and outcome else (client or outcome)
            if label:
                parts.append(f"Proof Point: {label}" + (f" ({metric})" if metric else ""))

    # Sender voice + banned phrases from onboarding
    if company_profile:
        _inject_voice_profile(parts, company_profile.get("sender_voice_profile"))
        banned = [p for p in (company_profile.get("banned_phrases") or []) if p]
        if banned:
            parts.append("Never use these phrases: " + "; ".join(banned[:10]))

    parts.append("")

    # CTA
    cta_type = campaign.get("cta_type") or "reply"
    cta_url = campaign.get("cta_url") or ""
    # Free-value CTA (from the per-prospect pitch overlay) is the DEFAULT for
    # cold email + InMail whenever it exists: offering a named, zero-commitment
    # asset out-converts generic call/link asks. Connection note stays soft.
    _fv_cta = (intelligence or {}).get("free_value_cta") or {}
    _fv_asset = (_fv_cta.get("asset_name") or "").strip() if isinstance(_fv_cta, dict) else ""
    parts.append("## Call to Action")
    if _fv_asset:
        _fv_line = (_fv_cta.get("cta_line") or "Worth sending over?").strip()
        parts.append(f"CTA MODE: FREE VALUE (use for cold email and InMail)")
        parts.append(f'Offer this specific free asset by name: "{_fv_asset}"')
        parts.append(
            f'Close the email body and InMail body with a single-question yes ask: "{_fv_line}" '
            "— no meeting ask, no links, nothing required from them to receive it."
        )
        parts.append(
            "The connection note stays soft: no asset pitch, no CTA beyond connecting."
        )
        parts.append("Exactly ONE CTA per message — the free-value ask replaces any other ask.")
    elif cta_type == "book_call":
        parts.append(f"CTA: Ask them to book a call." + (f" Link: {cta_url}" if cta_url else ""))
    elif cta_type == "visit_link":
        parts.append(f"CTA: Direct them to visit: {cta_url}" if cta_url else "CTA: Soft ask to visit a resource.")
    elif cta_type == "free_value":
        parts.append(
            "CTA: Offer ONE specific, named, zero-commitment free asset (teardown/audit/"
            "playbook/deck/benchmark) relevant to this prospect, and close with a "
            'single-question yes ask like "Worth sending over?". No meeting ask.'
        )
    else:
        parts.append("CTA: Ask a single question that invites a reply. No links.")
    parts.append("")

    # Top personalization signal
    signal_kind, signal_text = _select_top_signal(prospect)
    parts.append("## Top Personalization Hook")
    if signal_kind != "none":
        parts.append(f"Signal type: {signal_kind}")
        parts.append(f"Signal: {signal_text}")
        parts.append("Weave this into the first 1-2 sentences of the email body and InMail body.")
    else:
        parts.append("No fresh signals available — use industry-specific pain framing for this prospect's role and industry.")
    parts.append("")

    # Prospect
    full_name = prospect.get("full_name") or (
        (prospect.get("first_name") or "") + " " + (prospect.get("last_name") or "")
    ).strip() or "the prospect"
    first_name = prospect.get("first_name") or full_name.split()[0]
    parts.append("## Prospect")
    parts.append(f"Name: {full_name}")
    parts.append(f"First Name: {first_name}")
    parts.append(f"Title: {prospect.get('title') or prospect.get('job_title') or 'Unknown'}")
    parts.append(f"Company: {prospect.get('company_name') or prospect.get('company') or 'Unknown'}")
    parts.append(f"Industry: {prospect.get('industry') or 'Unknown'}")
    parts.append(f"Seniority: {prospect.get('seniority_level') or 'Unknown'}")
    parts.append(f"Location: {prospect.get('location') or prospect.get('country') or 'Unknown'}")
    if prospect.get("company_size"):
        parts.append(f"Company Size: {prospect['company_size']}")
    if prospect.get("company_total_funding_clean"):
        parts.append(f"Total Funding: {prospect['company_total_funding_clean']}")

    # LinkedIn profile signals
    if profile:
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        if profile.get("summary") or profile.get("about"):
            about = (profile.get("summary") or profile.get("about") or "")[:300]
            parts.append(f"About: {about}")
        experiences = profile.get("experiences") or profile.get("positions") or []
        if experiences:
            parts.append("Career:")
            for exp in experiences[:2]:
                title = exp.get("title") or exp.get("jobTitle") or ""
                co_name = exp.get("companyName") or exp.get("company") or ""
                duration = exp.get("duration") or ""
                if title or co_name:
                    parts.append(f"  - {title} at {co_name}" + (f" ({duration})" if duration else ""))
        posts = profile.get("posts") or profile.get("recentPosts") or []
        if posts:
            parts.append("Recent Posts:")
            for post in posts[:2]:
                text = (post.get("text") or post.get("content") or "")[:200]
                if text:
                    parts.append(f"  - {text[:150]}...")

    # Company data
    co = _normalize_company(company)
    if co.get("description") or co.get("industry"):
        parts.append("")
        parts.append("## Company Data")
        if co.get("description"):
            parts.append(f"Description: {co['description'][:250]}")
        if co.get("industry"):
            parts.append(f"Industry: {co['industry']}")
        if co.get("size"):
            parts.append(f"Size: {co['size']}")
        if co.get("followers"):
            parts.append(f"LinkedIn Followers: {co['followers']}")
        if co.get("specialties"):
            specs = co["specialties"]
            if isinstance(specs, list):
                parts.append(f"Specialties: {_stringify_list(specs, 5)}")

    # Tech stack
    tech_stack = prospect.get("company_technologies") or []
    if tech_stack:
        parts.append(f"Technology Stack: {_stringify_list(tech_stack, 6)}")

    # Competitors
    competitors = prospect.get("competitors") or []
    if competitors:
        parts.append("")
        parts.append("## Competitive Context")
        for c in competitors[:3]:
            name = c.get("name") if isinstance(c, dict) else str(c)
            diff = (c.get("differentiation") or c.get("description") or "") if isinstance(c, dict) else ""
            parts.append(f"  - {name}" + (f": {diff[:100]}" if diff else ""))

    # Deep company research (companies_collection.research — best performer,
    # buying signals, funding, hiring, tech stack, launches)
    _research = prospect.get("company_research") or {}
    _best = _research.get("best_performer")
    _signals = _research.get("buying_signals") or []
    _funding_line = _format_funding_line(_research.get("funding"))
    _hiring = _research.get("hiring_signals") or []
    _research_tech = _research.get("tech_stack") or []
    _launches = _research.get("recent_launches") or []
    if (
        (isinstance(_best, dict) and _best.get("name"))
        or _signals or _funding_line or _hiring or _research_tech or _launches
    ):
        parts.append("")
        parts.append("## Company Research (reference these facts concretely in the body)")
        if isinstance(_best, dict) and _best.get("name"):
            _why = (_best.get("why_winning") or "")[:200]
            parts.append(f"Best-Performing Competitor: {_best['name']}" + (f" — why winning: {_why}" if _why else ""))
        for _sig in _signals[:3]:
            parts.append(f"Buying Signal: {str(_sig)[:150]}")
        if _funding_line:
            parts.append(f"Funding: {_funding_line}")
        if _hiring:
            parts.append("Hiring Now (buying intent): " + "; ".join(str(h)[:100] for h in _hiring[:3]))
        if _research_tech:
            parts.append(f"Tech Stack (researched): {_stringify_list(_research_tech, 6)}")
        if _launches:
            parts.append("Recent Launches: " + "; ".join(str(l)[:100] for l in _launches[:2]))
        _co_posts = _research.get("company_posts") or []
        if _co_posts:
            _cp_text = (_co_posts[0].get("text") or "")[:150] if isinstance(_co_posts[0], dict) else ""
            if _cp_text:
                parts.append(f"Latest Company-Page Post: {_cp_text}")

    # Company news
    news = prospect.get("company_news") or []
    if news:
        parts.append("")
        parts.append("## Recent Company News")
        for item in news[:2]:
            title = item.get("title") or ""
            summary = item.get("summary") or ""
            published = item.get("published_date") or ""
            if title:
                parts.append(f"  - [{published}] {title}: {summary[:120]}" if published else f"  - {title}: {summary[:120]}")

    # AI assessment signals
    ai_assessment = prospect.get("ai_assessment") or {}
    if ai_assessment:
        parts.append("")
        parts.append("## Assessment Signals")
        if ai_assessment.get("reasoning"):
            parts.append(f"Fit Reasoning: {ai_assessment['reasoning'][:200]}")
        signals = ai_assessment.get("buying_signals") or prospect.get("buying_signals") or []
        if signals:
            parts.append(f"Buying Signals: {_stringify_list(signals, 4)}")
        recommended = ai_assessment.get("recommended_services") or []
        if recommended:
            parts.append(f"Recommended Services: {_stringify_list(recommended, 3)}")
        strengths = ai_assessment.get("strengths") or []
        if strengths:
            parts.append(f"Prospect Strengths: {_stringify_list(strengths, 3)}")

    if prospect.get("discovery_reasoning"):
        parts.append(f"Discovery Reasoning: {prospect['discovery_reasoning'][:150]}")

    # Prospect intelligence (deep enrichment — highest-priority personalization signal)
    intel = prospect.get("prospect_intelligence") or {}
    if intel:
        parts.append("")
        parts.append("## Prospect Intelligence (USE THESE — highest priority over generic signals)")
        if intel.get("best_hook"):
            parts.append(f"Best Hook: {intel['best_hook']}")
            parts.append("  → Open the email body and InMail body with a reference to this hook.")
        if intel.get("pitch_angle"):
            parts.append(f"Pitch Angle: {intel['pitch_angle']}")
            parts.append("  → Use this framing for the connection note and InMail.")
        if intel.get("top_topics"):
            topics = intel["top_topics"]
            if isinstance(topics, list) and topics:
                parts.append(f"Top Topics: {', '.join(str(t) for t in topics[:4])}")
                parts.append(f"  → Reference {topics[0]} in the email subject line.")
        if intel.get("pain_signals"):
            pains = intel["pain_signals"]
            if isinstance(pains, list) and pains:
                parts.append(f"Pain Signals: {', '.join(str(p) for p in pains[:3])}")
                parts.append(f"  → Target {pains[0]} as the primary pain in the email body.")
        if intel.get("why_they_need_us"):
            parts.append(f"Why They Need Us: {intel['why_they_need_us']}")
            parts.append("  → Use this in InMail body.")
        if intel.get("competitors"):
            comps = intel["competitors"]
            if isinstance(comps, list) and comps:
                parts.append(f"Known Competitors: {', '.join(str(c) for c in comps[:4])}")
                parts.append("  → Name specific competitors in the InMail body.")
        if intel.get("writing_voice"):
            parts.append(f"Writing Voice: {intel['writing_voice']}")
            parts.append("  → Mirror this style in the connection note.")
        if intel.get("dont_pitch"):
            avoid = intel["dont_pitch"]
            if isinstance(avoid, list) and avoid:
                parts.append(f"AVOID: {', '.join(str(a) for a in avoid[:3])}")
        if intel.get("engagement_style"):
            parts.append(f"Engagement Style: {intel['engagement_style']}")

    # Per-campaign prospect intelligence (prospect_intelligence_base merged with per-tenant pitch)
    if intelligence:
        parts.append("")
        parts.append("## Prospect Intelligence (USE THESE — highest priority over generic signals)")
        if intelligence.get("best_hook"):
            parts.append(f"Best Hook: {intelligence['best_hook']}")
            parts.append("  → Open the email body and InMail body with a reference to this hook.")
        if intelligence.get("pitch_angle"):
            parts.append(f"Pitch Angle: {intelligence['pitch_angle']}")
            parts.append("  → Use this framing for the connection note and InMail.")
        if intelligence.get("why_they_need_us"):
            parts.append(f"Why They Need Us: {intelligence['why_they_need_us']}")
            parts.append("  → Use this in InMail body.")
        if intelligence.get("top_topics"):
            topics = intelligence["top_topics"]
            if isinstance(topics, list) and topics:
                parts.append(f"Top Topics: {', '.join(str(t) for t in topics[:4])}")
                parts.append(f"  → Reference {topics[0]} in the email subject line.")
        if intelligence.get("pain_signals"):
            pains = intelligence["pain_signals"]
            if isinstance(pains, list) and pains:
                parts.append(f"Pain Signals: {', '.join(str(p) for p in pains[:3])}")
                parts.append(f"  → Target {pains[0]} as the primary pain in the email body.")
        if intelligence.get("writing_voice"):
            parts.append(f"Writing Voice: {intelligence['writing_voice']}")
            parts.append("  → Mirror this style in the connection note.")
        if intelligence.get("dont_pitch"):
            avoid = intelligence["dont_pitch"]
            if isinstance(avoid, list) and avoid:
                parts.append(f"AVOID: {', '.join(str(a) for a in avoid[:3])}")
        if intelligence.get("engagement_style"):
            parts.append(f"Engagement Style: {intelligence['engagement_style']}")

    # Channel-specific guidance from campaign/company profile
    parts.append("")
    parts.append("## Channel Guidance")
    conn_guidance = campaign.get("connection_request_guidance") or (company_profile or {}).get("connection_request_guidance") or ""
    email_guidance = campaign.get("email_guidance") or (company_profile or {}).get("email_guidance") or ""
    inmail_guidance = campaign.get("inmail_guidance") or (company_profile or {}).get("inmail_guidance") or ""
    if conn_guidance:
        parts.append(f"LinkedIn Connection Note: {conn_guidance}")
    if email_guidance:
        parts.append(f"Email: {email_guidance}")
    if inmail_guidance:
        parts.append(f"InMail: {inmail_guidance}")
    if not (conn_guidance or email_guidance or inmail_guidance):
        parts.append("No specific channel guidance — use your best judgment.")

    # Additional instructions (highest priority)
    if additional_instructions:
        parts.append("")
        parts.append(f"## Additional Instructions (highest priority)\n{additional_instructions}")

    parts.append("")
    parts.append("Generate all 3 channel messages now. Return only valid JSON matching the schema.")
    return "\n".join(parts)


# ── Batch Campaign Outreach ───────────────────────────────────────────────────

_CHANNEL_SCHEMA_HINT = {
    "email": '{"id": "<enrollment_id>", "subject_a": "...", "subject_b": "...", "body": "..."}',
    "linkedin_connection": '{"id": "<enrollment_id>", "note": "≤280 chars"}',
    "linkedin_inmail": '{"id": "<enrollment_id>", "subject": "≤55 chars", "body": "..."}',
}


def build_campaign_batch_outreach_prompt(
    campaign: dict,
    prospects_with_ids: list[tuple[str, dict]],
    channel: str,
    company_profile: dict | None = None,
) -> str:
    """
    Build a batch prompt that asks the model to generate one message per prospect.
    prospects_with_ids: [(enrollment_id_str, prospect_dict), ...]
    channel: "email" | "linkedin_connection" | "linkedin_inmail"
    company_profile: onboarding company profile (services, differentiators,
    case studies, sender voice) — included once for the whole batch.
    """
    tone = campaign.get("message_tone") or "professional"
    tone_desc = TONE_DESCRIPTIONS.get(tone, TONE_DESCRIPTIONS["professional"])
    value_prop = campaign.get("value_proposition") or ""
    pain_point = campaign.get("pain_point") or ""
    sender_name = campaign.get("sender_name") or (company_profile or {}).get("sender_name") or ""
    sender_first = sender_name.split()[0] if sender_name else ""
    cta_type = campaign.get("cta_type") or "reply"
    cta_url = campaign.get("cta_url") or ""

    schema_hint = _CHANNEL_SCHEMA_HINT.get(channel, _CHANNEL_SCHEMA_HINT["email"])

    parts = [
        f"Generate personalized {channel} messages for {len(prospects_with_ids)} prospects.",
        "",
        f"Tone: {tone.upper()} — {tone_desc}",
    ]
    if value_prop:
        parts.append(f"Value Proposition: {value_prop}")
    if pain_point:
        parts.append(f"Pain Point: {pain_point}")
    if sender_first:
        parts.append(
            f'Sign off each email body exactly as: "Best,\\n{sender_first}" '
            f'(that is: "Best," on one line, then "{sender_first}" on the next line). '
            "Never sign with a prospect's name."
        )
    else:
        parts.append(
            "Sender name unknown — do not include any name in the sign-off, "
            "and NEVER sign with a prospect's name."
        )
    if company_profile:
        sender_role = company_profile.get("sender_role") or ""
        seller_company = company_profile.get("company_name") or ""
        if sender_role or seller_company:
            parts.append(
                "Sender: " + " at ".join([v for v in (sender_role, seller_company) if v])
            )
        services = company_profile.get("services") or []
        if services:
            parts.append(f"Services: {_stringify_list(services[:4])}")
        diffs = company_profile.get("differentiators") or []
        if diffs:
            parts.append(f"Differentiators: {_stringify_list(diffs[:3])}")
        target_market = company_profile.get("target_market") or ""
        if target_market:
            parts.append(f"Target Market: {target_market}")
        best_cs = _select_best_case_study(company_profile.get("case_studies") or [], {})
        if best_cs:
            client = best_cs.get("client") or ""
            outcome = best_cs.get("outcome") or ""
            metric = best_cs.get("metric") or ""
            label = f"{client} — {outcome}" if client and outcome else (client or outcome)
            if label:
                parts.append(f"Proof Point (use where relevant): {label}" + (f" ({metric})" if metric else ""))
        _inject_voice_profile(parts, company_profile.get("sender_voice_profile"))
        banned = [p for p in (company_profile.get("banned_phrases") or []) if p]
        if banned:
            parts.append("Never use these phrases: " + "; ".join(banned[:10]))
    if cta_type == "book_call":
        parts.append(f"CTA: Ask to book a call." + (f" Link: {cta_url}" if cta_url else ""))
    elif cta_type == "visit_link":
        parts.append(f"CTA: Visit link: {cta_url}" if cta_url else "CTA: Visit a resource.")
    elif cta_type == "free_value":
        parts.append(
            "CTA: Offer ONE specific, named, zero-commitment free asset and close with a "
            'single-question yes ask like "Worth sending over?". No meeting ask, no links.'
        )
    else:
        parts.append("CTA: Ask a single reply-inviting question.")
    if channel in ("email", "linkedin_inmail"):
        parts.append(
            "FREE-VALUE OVERRIDE: when a prospect below has a [FREE-VALUE CTA] line, it "
            "REPLACES the default CTA for that prospect — offer the named asset and close "
            'with its single-question yes ask (e.g. "Worth sending over?"). Exactly one CTA '
            "per message. Never add a meeting ask on top."
        )
    parts.append("")
    parts.append("## Prospects")
    parts.append("")

    for eid, prospect in prospects_with_ids:
        full_name = prospect.get("full_name") or (
            (prospect.get("first_name") or "") + " " + (prospect.get("last_name") or "")
        ).strip() or "Unknown"
        title = prospect.get("title") or prospect.get("job_title") or ""
        company = prospect.get("company_name") or prospect.get("company") or ""
        industry = prospect.get("industry") or ""
        seniority = prospect.get("seniority_level") or ""

        # Top signal for personalization
        signal_kind, signal_text = _select_top_signal(prospect)

        parts.append(f"ID: {eid}")
        parts.append(f"Name: {full_name} | Title: {title} | Company: {company} | Industry: {industry} | Seniority: {seniority}")
        if signal_kind != "none":
            parts.append(f"Signal ({signal_kind}): {signal_text[:150]}")

        # Enrichment signals (present only for enriched prospects)
        # Campaign fit is passed in via injected intelligence, not the shared
        # tenant-neutral pool. Do not read the legacy prospect.ai_prospect_score copy.
        ai_fit_score = prospect.get("ai_fit_score")
        priority_tier = prospect.get("priority_tier", "")
        ai_assessment = prospect.get("ai_assessment") or {}
        competitor_summary = ai_assessment.get("competitor_summary") or ai_assessment.get("competitors_used", "")
        pain_signals = ai_assessment.get("pain_signals") or ai_assessment.get("company_pain_signals", "")
        company_fit_reason = ai_assessment.get("company_fit_reason") or ai_assessment.get("fit_reason", "")

        recent_news_list = prospect.get("recent_news") or []
        recent_news_str = ""
        if recent_news_list and isinstance(recent_news_list, list):
            first_news = recent_news_list[0] if recent_news_list else {}
            news_title = first_news.get("title") or first_news.get("summary") or ""
            if news_title:
                recent_news_str = news_title[:200]

        if priority_tier:
            parts.append(f"  Priority tier: {priority_tier}")
        if ai_fit_score:
            parts.append(f"  AI fit score: {ai_fit_score}/100")
        if competitor_summary:
            parts.append(f"  Competitors they use: {competitor_summary}")
        if pain_signals:
            parts.append(f"  Known pain signals: {pain_signals}")
        if company_fit_reason:
            parts.append(f"  Why they're a fit: {company_fit_reason}")
        if recent_news_str:
            parts.append(f"  Recent news: {recent_news_str}")

        # Deep company research facts (best performer + buying signals) — reference concretely
        _research = prospect.get("company_research") or {}
        _best = _research.get("best_performer")
        if isinstance(_best, dict) and _best.get("name"):
            _why = (_best.get("why_winning") or "")[:150]
            parts.append(
                f"  Best-performing competitor: {_best['name']}"
                + (f" — why winning: {_why}" if _why else "")
            )
        _signals = _research.get("buying_signals") or []
        if _signals:
            parts.append(
                "  Buying signals: " + "; ".join(str(s)[:100] for s in _signals[:2])
            )
        _funding_line = _format_funding_line(_research.get("funding"))
        if _funding_line:
            parts.append(f"  Funding: {_funding_line[:150]}")
        _hiring = _research.get("hiring_signals") or []
        if _hiring:
            parts.append(
                "  Hiring now (buying intent): " + "; ".join(str(h)[:80] for h in _hiring[:2])
            )
        _research_tech = _research.get("tech_stack") or []
        if _research_tech:
            parts.append(f"  Tech stack: {_stringify_list(_research_tech, 5)}")
        _launches = _research.get("recent_launches") or []
        if _launches:
            parts.append(
                "  Recent launches: " + "; ".join(str(l)[:80] for l in _launches[:2])
            )

        # Prospect intelligence (deep enrichment) — highest-priority personalization
        intel = prospect.get("prospect_intelligence") or {}
        if intel:
            if intel.get("best_hook"):
                parts.append(f"  [INTEL] Best hook: {intel['best_hook'][:200]}")
            if intel.get("pain_signals") and isinstance(intel["pain_signals"], list):
                parts.append(f"  [INTEL] Pain signals: {', '.join(str(p) for p in intel['pain_signals'][:2])}")
            if intel.get("top_topics") and isinstance(intel["top_topics"], list):
                parts.append(f"  [INTEL] Top topics (use in subject): {intel['top_topics'][0] if intel['top_topics'] else ''}")
            if intel.get("pitch_angle"):
                parts.append(f"  [INTEL] Pitch angle: {intel['pitch_angle']}")
            if intel.get("why_they_need_us"):
                parts.append(f"  [INTEL] Why they need us: {intel['why_they_need_us'][:150]}")
            if intel.get("competitors") and isinstance(intel["competitors"], list) and intel["competitors"]:
                parts.append(f"  [INTEL] Competitors (name in InMail): {', '.join(str(c) for c in intel['competitors'][:3])}")
            if intel.get("writing_voice"):
                parts.append(f"  [INTEL] Writing voice (mirror in connection note): {intel['writing_voice']}")
            if intel.get("dont_pitch") and isinstance(intel["dont_pitch"], list) and intel["dont_pitch"]:
                parts.append(f"  [INTEL] AVOID: {', '.join(str(a) for a in intel['dont_pitch'][:2])}")
            _fv = intel.get("free_value_cta") or {}
            if (
                channel in ("email", "linkedin_inmail")
                and isinstance(_fv, dict)
                and (_fv.get("asset_name") or "").strip()
            ):
                _fv_line = (_fv.get("cta_line") or "Worth sending over?").strip()
                parts.append(
                    f'  [FREE-VALUE CTA] Offer by name: "{_fv["asset_name"].strip()}" '
                    f'— close with: "{_fv_line}" (this is the ONLY CTA for this prospect)'
                )

        parts.append("")

    parts.append(f"Return a JSON object: {{\"messages\": [<one per prospect>]}}")
    parts.append(f"Each item: {schema_hint}")
    parts.append("Keep the same order as the input. Use the 'id' field to match each message to its prospect.")
    parts.append("Every message MUST reference the prospect's signal. Prioritize [INTEL] fields over generic signals when present. No generic messages.")

    return "\n".join(parts)


# ── Campaign Follow-Up ────────────────────────────────────────────────────────

def build_campaign_followup_prompt(
    prospect: dict,
    profile: dict | None,
    company: dict | None,
    campaign: dict,
    node: dict,
    prior_step_messages: list | None = None,
    company_profile: dict | None = None,
) -> str:
    """Build prompt for campaign follow-up / subsequent-step messages."""
    parts = []

    tone = campaign.get("message_tone") or "professional"
    tone_desc = TONE_DESCRIPTIONS.get(tone, TONE_DESCRIPTIONS["professional"])
    channel = node.get("channel") or "email"
    step_index = node.get("step_index") or node.get("delay_days") or 2

    parts.append(f"## Follow-Up Message — Step {step_index}")
    parts.append(f"Channel: {channel}")
    parts.append(f"Tone: {tone.upper()} — {tone_desc}")
    parts.append("")

    # Sender identity
    sender_name = campaign.get("sender_name") or (company_profile or {}).get("sender_name") or ""
    sender_first = sender_name.split()[0] if sender_name else ""
    sender_role = (company_profile or {}).get("sender_role") or ""
    if sender_first:
        parts.append(f"Sender: {sender_first}" + (f", {sender_role}" if sender_role else ""))
        parts.append(
            f'For email: sign off exactly as "Best,\\n{sender_first}" '
            f'("Best," on one line, then "{sender_first}" on the next).'
        )
    else:
        parts.append(
            "Sender name unknown — do not include any name in the sign-off, "
            "and NEVER sign with the prospect's name."
        )

    # Value context
    if campaign.get("value_proposition"):
        parts.append(f"Value Proposition: {campaign['value_proposition']}")
    if campaign.get("pain_point"):
        parts.append(f"Pain Point: {campaign['pain_point']}")

    # Onboarding company context: differentiators, real case-study data,
    # sender voice, banned phrases (previously follow-ups had none of these)
    if company_profile:
        diffs = company_profile.get("differentiators") or []
        if diffs:
            parts.append(f"Differentiators: {_stringify_list(diffs[:3])}")
        best_cs = _select_best_case_study(company_profile.get("case_studies") or [], prospect)
        if best_cs:
            client = best_cs.get("client") or ""
            outcome = best_cs.get("outcome") or ""
            metric = best_cs.get("metric") or ""
            label = f"{client} — {outcome}" if client and outcome else (client or outcome)
            if label:
                parts.append(f"Case Study Available: {label}" + (f" ({metric})" if metric else ""))
        _inject_voice_profile(parts, company_profile.get("sender_voice_profile"))
        banned = [p for p in (company_profile.get("banned_phrases") or []) if p]
        if banned:
            parts.append("Never use these phrases: " + "; ".join(banned[:10]))
    parts.append("")

    # Step variation rules
    parts.append("## Step Variation Rule")
    if step_index <= 2:
        parts.append("Step 2: Fresh angle or curiosity question. Not a reminder — a new perspective or piece of value.")
    elif step_index == 3:
        parts.append("Step 3: Introduce a case study, customer outcome, or social proof. Concrete metric preferred.")
    else:
        parts.append("Step 4+: Low-pressure breakup or value drop. 'Last note before I stop — here's one thing worth knowing...'")
    parts.append("")

    # Per-node intent + author guidance from the sequence builder (optional).
    intent = node.get("message_intent")
    _intent_hint = {
        "intro": "Purpose: introduce yourself and open the conversation.",
        "followup": "Purpose: follow up on the prior touch without repeating it.",
        "value": "Purpose: lead with a concrete value or insight for this prospect.",
        "breakup": "Purpose: polite last-touch break-up note.",
    }.get(intent or "")
    if _intent_hint:
        parts.append(_intent_hint)
    guidance = (node.get("guidance") or "").strip()
    if guidance:
        parts.append(f"Author guidance (follow closely): {guidance}")
    if _intent_hint or guidance:
        parts.append("")

    # ── Later-touch sharpening ──
    # Touch number is derived from the prior-message context (each already-sent
    # step contributes one entry); node.touches_done, when threaded by the
    # caller, wins as the explicit signal from sequence_state.
    _prior_count = len(prior_step_messages or [])
    _touches_done = node.get("touches_done")
    if isinstance(_touches_done, int) and not isinstance(_touches_done, bool):
        _prior_count = max(_prior_count, _touches_done)
    _touch_number = _prior_count + 1
    if _touch_number >= 3:
        parts.append("## Later-Touch Sharpening")
        parts.append(
            f"The prospect has not responded to {_prior_count} previous touches. "
            "Change the angle completely from earlier messages. Use one concrete "
            "research hook if available (funding round, hiring signal, competitor "
            "move, recent launch). Keep it shorter than the previous touch."
        )
        if intent == "value":
            if _touch_number <= 3:
                parts.append(
                    "Angle for this touch: offer the detailed deck about the service "
                    "— the only CTA is 'reply if you want me to send it over'."
                )
            else:
                parts.append(
                    "Angle for this touch: proof — lead with a case study or "
                    "customer outcome with a concrete metric. Do NOT re-offer the deck."
                )
        elif intent == "followup":
            parts.append(
                "Angle for this touch: ROI framing that ends in one short, direct question."
            )
        elif intent == "breakup":
            parts.append(
                "Angle for this touch: short break-up note — two sentences max, "
                "no guilt, leave the door open."
            )
        parts.append("")

    # Prospect context
    full_name = prospect.get("full_name") or (
        (prospect.get("first_name") or "") + " " + (prospect.get("last_name") or "")
    ).strip() or "the prospect"
    first_name = prospect.get("first_name") or full_name.split()[0]
    parts.append("## Prospect")
    parts.append(f"Name: {full_name}")
    parts.append(f"First Name: {first_name}")
    parts.append(f"Title: {prospect.get('title') or prospect.get('job_title') or 'Unknown'}")
    parts.append(f"Company: {prospect.get('company_name') or prospect.get('company') or 'Unknown'}")
    parts.append(f"Industry: {prospect.get('industry') or 'Unknown'}")
    parts.append(f"Seniority: {prospect.get('seniority_level') or 'Unknown'}")

    # LinkedIn signals
    if profile:
        if profile.get("headline"):
            parts.append(f"Headline: {profile['headline']}")
        posts = profile.get("posts") or profile.get("recentPosts") or []
        if posts:
            text = (posts[0].get("text") or posts[0].get("content") or "")[:200]
            if text:
                parts.append(f"Latest Post: {text[:150]}...")
        psychographic = prospect.get("psychographic_profile") or profile.get("psychographic_profile") or ""
        if psychographic:
            parts.append(f"Psychographic: {psychographic[:150]}")

    # Fresh signals to weave in
    parts.append("")
    parts.append("## Fresh Signals (use ONE in this follow-up — pick a DIFFERENT signal than prior messages)")
    signal_kind, signal_text = _select_top_signal(prospect)
    if signal_kind != "none":
        parts.append(f"{signal_kind}: {signal_text[:200]}")
    buying_signals = prospect.get("buying_signals") or (prospect.get("ai_assessment") or {}).get("buying_signals") or []
    if buying_signals:
        parts.append(f"Buying Signals: {_stringify_list(buying_signals, 3)}")
    news = prospect.get("company_news") or []
    if news:
        for item in news[:2]:
            title = item.get("title") or ""
            summary = item.get("summary") or ""
            if title:
                parts.append(f"News: {title}: {summary[:100]}")

    # Company context
    co = _normalize_company(company)
    if co.get("industry") or co.get("description"):
        parts.append(f"Company Industry: {co.get('industry', '')}")

    # Prior messages — for anti-repetition
    if prior_step_messages:
        parts.append("")
        parts.append("## Prior Messages Sent (DO NOT repeat angles, phrases, or signals from these)")
        for msg in prior_step_messages:
            ch = msg.get("channel") or ""
            subject = msg.get("subject") or ""
            excerpt = msg.get("body_excerpt") or msg.get("body") or ""
            if isinstance(excerpt, str):
                excerpt = excerpt[:150]
            parts.append(f"  [{ch}] {subject + ': ' if subject else ''}{excerpt}...")

    parts.append("")
    parts.append("## Length Targets")
    if channel == "email":
        parts.append("Email body: 60-90 words. Subject line ≤55 chars.")
    elif channel == "linkedin_connection":
        parts.append("LinkedIn note: 80-120 words equivalent, but ≤280 chars hard limit.")
    elif channel == "linkedin_message":
        parts.append("LinkedIn DM body: 40-70 words. Conversational, no subject line.")
    else:
        parts.append("InMail body: 100-140 words.")

    parts.append("")
    parts.append("## Anti-Repetition Rule")
    parts.append("This message MUST introduce a new specific detail, angle, or signal not used in any prior message above. No 'just following up'. No re-stating what you already said. Sound like a real colleague checking in with something new.")
    parts.append("")
    if channel == "linkedin_message":
        # linkedin_message has no shape in the campaign message schema, so name
        # the contract explicitly — otherwise the model answers with one of the
        # other three shapes and the generated body comes back empty.
        parts.append(
            'Generate the follow-up message. Return JSON exactly as: '
            '{"linkedin_message": {"body": "..."}}'
        )
    else:
        parts.append("Generate the follow-up message. Return JSON matching the campaign message schema (cold_email / linkedin_connection / linkedin_inmail).")
    return "\n".join(parts)


# ── Industries & Campaign Prefill ─────────────────────────────────────────────

_PREFILL_VALID_INDUSTRIES = [
    "packaging & containers", "information technology & services", "construction",
    "computer software", "marketing & advertising", "real estate",
    "health, wellness & fitness", "management consulting", "internet", "retail",
    "financial services", "consumer services", "hospital & health care", "automotive",
    "restaurants", "education management", "food & beverages", "design", "hospitality",
    "accounting", "events services", "nonprofit organization management", "entertainment",
    "electrical/electronic manufacturing", "leisure, travel & tourism",
    "professional training & coaching", "transportation/trucking/railroad", "law practice",
    "apparel & fashion", "architecture & planning", "mechanical or industrial engineering",
    "insurance", "telecommunications", "human resources", "staffing & recruiting", "sports",
    "oil & energy", "legal services", "media production", "machinery", "wholesale",
    "consumer goods", "music", "photography", "medical practice",
    "business supplies & equipment", "facilities services", "publishing", "food production",
    "arts & crafts", "building materials", "civil engineering", "religious institutions",
    "renewables & environment", "public relations & communications", "higher education",
    "graphic design", "printing", "furniture", "mining & metals",
    "logistics & supply chain", "research", "pharmaceuticals",
    "individual & family services", "medical devices", "environmental services",
    "civic & social organization", "e-learning", "security & investigations", "cosmetics",
    "chemicals", "government administration", "online media", "investment management",
    "farming", "writing & editing", "textiles", "mental health care", "broadcast media",
    "biotechnology", "information services", "international trade & development",
    "primary/secondary education", "motion pictures & film", "consumer electronics",
    "banking", "import & export", "industrial automation",
    "recreational facilities & services", "utilities", "sporting goods", "fine art",
    "airlines/aviation", "performing arts", "computer & network security", "maritime",
    "luxury goods & jewelry", "venture capital & private equity", "wine & spirits",
    "plastics", "aviation & aerospace", "veterinary", "commercial real estate",
    "computer games", "executive office", "computer networking", "market research",
    "outsourcing/offshoring", "program development", "computer hardware",
    "translation & localization", "philanthropy", "public safety",
    "alternative medicine", "museums & institutions", "warehousing", "defense & space",
    "newspapers", "paper & forest products", "law enforcement", "investment banking",
    "fund-raising", "think tanks", "glass, ceramics & concrete", "capital markets",
    "government relations", "semiconductors", "animation", "political organization",
    "package/freight delivery", "wireless", "international affairs", "public policy",
    "libraries", "dairy", "supermarkets", "fishery", "military", "ranching",
    "railroad manufacture", "gambling & casinos", "tobacco", "shipbuilding", "judiciary",
    "alternative dispute resolution", "nanotechnology", "agriculture",
    "legislative office",
    "ecommerce", "d2c", "direct-to-consumer",
]

CAMPAIGN_PREFILL_FIELD_OPTIONS = {
    "message_tone": ["professional", "challenger", "conversational", "empathetic"],
    "cta_type": ["book_call", "reply", "visit_link", "free_value"],
    "icp_seniority_levels": ["c_suite", "owner", "founder", "partner", "vp", "director", "manager", "senior"],
    "icp_functional_departments": [
        "marketing", "sales", "operations", "engineering", "product",
        "finance", "hr", "it", "legal", "customer_success", "data", "design",
    ],
    "icp_countries": [
        "United States", "Canada", "United Kingdom", "Germany",
        "France", "Netherlands", "India", "Australia",
        "Singapore", "United Arab Emirates",
    ],
    "channels": ["email", "linkedin_connection", "linkedin_inmail"],
}

CAMPAIGN_PREFILL_SYSTEM_PROMPT = """You are a B2B campaign configuration assistant. Convert a natural-language conversation into a structured campaign configuration.

## Your output
Return ONLY a JSON object in one of two formats:

### Format 1 — Needs clarification:
```json
{
  "needs_clarification": {
    "question": "What specific question to ask the user?",
    "field": "field_name",
    "widget": "select|text|multiselect",
    "options": ["option1", "option2"],
    "allow_free_text": true,
    "progress": {"captured": 2, "total": 5}
  }
}
```

### Format 2 — Complete configuration:
```json
{
  "campaign": {
    "name": "Campaign name",
    "description": "Brief description",
    "message_tone": "professional|challenger|conversational|empathetic",
    "value_proposition": "What you're offering",
    "pain_point": "Problem you solve",
    "cta_type": "book_call|reply|visit_link",
    "cta_url": "URL if applicable",
    "target": {
      "industry_label": "Human-readable industry name",
      "icp_industries": ["linkedin industry slug"],
      "icp_job_titles": ["CEO", "Founder"],
      "icp_seniority_levels": ["c_suite", "owner", "founder"],
      "icp_company_size_min": 10,
      "icp_company_size_max": 500,
      "icp_countries": ["United States"],
      "keywords": [],
      "exclude_keywords": [],
      "exclude_industries": []
    },
    "message_guidance": {
      "email_guidance": "1-2 sentence directive for cold emails: what to lead with, what value/pain to emphasize, what tone, what to avoid.",
      "connection_request_guidance": "1-2 sentence directive for LinkedIn connection notes: keep it under 280 chars, hook + soft CTA.",
      "inmail_guidance": "1-2 sentence directive for LinkedIn InMails: stronger opener, concrete outcome or case study reference."
    }
  }
}
```

## Rules
- Ask ONE clarifying question at a time, in priority order: (1) target industry, (2) target role/seniority, (3) target regions/countries, (4) value proposition, (5) CTA type, (6) if cta_type is "book_call" or "visit_link" and cta_url is not yet captured, ask for the booking link or website URL.
- When asking for cta_url, use widget: "free_text" and set field: "cta_url". For book_call ask "What's your booking link? (e.g. Calendly or Cal.com URL)". For visit_link ask "What's the website URL you want prospects to visit?".
- Only ask if genuinely ambiguous. If the user has already told you, use it.
- Refer to the "Already captured" section in the user prompt before asking. If a field is listed there, treat it as already answered and move to the next priority.
- Never ask the same question twice.
- Map industry names to the closest LinkedIn category slug (e.g., "SaaS" -> "computer software").
- Default message_tone to "professional" if not specified.
- Default cta_type to "reply" if not specified.
- icp_seniority_levels must only use: c_suite, owner, founder, partner, vp, director, manager, senior.
- `keywords`, `exclude_keywords`, and `exclude_industries`: leave `[]` EMPTY unless one of the special-case rules below explicitly mandates a list. Do NOT infer keywords from `value_proposition`, `pain_point`, the sender's `company_profile`, or general industry associations. The user can add or refine keywords in the wizard after this AI step.
- When the user targets D2C, ecommerce, direct-to-consumer, online brands, or consumer brands: set icp_industries to ["retail", "consumer goods", "apparel & fashion", "food & beverages", "cosmetics"] (pick the most relevant 2-3), set keywords to EXACTLY ["d2c", "direct-to-consumer", "ecommerce"] (3 entries, no additions), and set exclude_keywords to EXACTLY ["agency", "consulting", "consultancy", "services", "staffing"] (5 entries, no additions) and exclude_industries to ["marketing & advertising", "staffing & recruiting", "management consulting"].
- Always populate icp_job_titles with the specific role keywords from the user's prompt (e.g., "CMO, marketing manager" -> ["CMO", "marketing manager", "Chief Marketing Officer", "VP Marketing"]). Do not leave icp_job_titles empty when roles are mentioned.
- When asking about regions/countries, set field: "icp_countries" and use widget: "multiselect" with allow_free_text: true.
- progress.total is always 5 (industry, role, regions, value prop, CTA).
- When emitting Format 2, ALWAYS include `message_guidance` with all three keys (email_guidance, connection_request_guidance, inmail_guidance) auto-filled based on the user's industry, value proposition, pain point, tone, and CTA. Be concrete and channel-appropriate. Do NOT ask the user about guidance — infer it.

## Valid LinkedIn industry slugs:
""" + "\n".join(f"- {ind}" for ind in _PREFILL_VALID_INDUSTRIES)


CAMPAIGN_PREFILL_COMPOSE_SYSTEM_PROMPT = """You are a B2B campaign configuration assistant. All required information has been confirmed. Compose the complete campaign configuration now.

## Your output
Return ONLY a JSON object in this format:
{
  "campaign": {
    "name": "Campaign name",
    "description": "Brief description",
    "message_tone": "professional|challenger|conversational|empathetic",
    "value_proposition": "What you're offering",
    "pain_point": "Problem you solve",
    "cta_type": "book_call|reply|visit_link",
    "cta_url": "URL if applicable",
    "target": {
      "industry_label": "Human-readable industry name",
      "icp_industries": ["linkedin industry slug"],
      "icp_job_titles": ["CEO", "Founder", "Chief Executive Officer"],
      "icp_seniority_levels": ["c_suite", "owner"],
      "icp_functional_departments": ["marketing"],
      "icp_company_size_min": null,
      "icp_company_size_max": null,
      "icp_countries": ["United Kingdom"],
      "keywords": [],
      "exclude_keywords": [],
      "exclude_industries": []
    },
    "message_guidance": {
      "email_guidance": "1-2 sentence directive for cold emails.",
      "connection_request_guidance": "1-2 sentence directive for LinkedIn connection notes (under 280 chars).",
      "inmail_guidance": "1-2 sentence directive for LinkedIn InMails."
    }
  }
}

## Rules
- Do NOT return needs_clarification. Return only the campaign JSON.
- All required data is in the captured block — use it exactly.
- Map industry_label to the closest LinkedIn category slugs for icp_industries.
- Default message_tone to "professional" if not specified.
- Always populate message_guidance with all three keys. Be concrete and channel-appropriate.
- icp_seniority_levels must only use: c_suite, owner, founder, partner, vp, director, manager, senior.
- `keywords`, `exclude_keywords`, and `exclude_industries`: leave `[]` EMPTY unless one of the special-case rules below explicitly mandates a list. Do NOT infer keywords from `value_proposition`, `pain_point`, the sender's `company_profile`, or general industry associations. The user can add or refine keywords in the wizard after this AI step.
- When the user targets D2C, ecommerce, direct-to-consumer, online brands, or consumer brands: set icp_industries to ["retail", "consumer goods", "apparel & fashion", "food & beverages", "cosmetics"] (pick the most relevant 2-3), set keywords to EXACTLY ["d2c", "direct-to-consumer", "ecommerce"] (3 entries, no additions), and set exclude_keywords to EXACTLY ["agency", "consulting", "consultancy", "services", "staffing"] (5 entries, no additions) and exclude_industries to ["marketing & advertising", "staffing & recruiting", "management consulting"].
- Always populate icp_job_titles with the specific role keywords from the captured targeting (e.g. "marketing managers" -> ["Marketing Manager", "Head of Marketing", "Growth Marketing Manager"]). Function+seniority filters are used for scraping, but icp_job_titles drives the person-fit title gate — an empty list disables it and lets adjacent roles through.

## Valid LinkedIn industry slugs:
""" + "\n".join(f"- {ind}" for ind in _PREFILL_VALID_INDUSTRIES)


# ── Lead-list column mapping (BYOL / Upload-a-Lead-List) ─────────────────────

LEAD_COLUMN_MAPPING_SYSTEM_PROMPT = """You map spreadsheet columns from an uploaded sales lead list to a fixed set of canonical fields.

You are given the list of column headers and a few sample rows. Decide which canonical field each column represents.

## Canonical fields (map each column to exactly one)
- first_name — person's given/first name
- last_name — person's family/last name
- full_name — person's full name in one column
- job_title — the person's role/title (e.g. "VP Marketing")
- linkedin_url — a LinkedIn profile OR company URL (linkedin.com/in/... or linkedin.com/company/...)
- company_name — the company/organization name
- company_domain — the company website domain or URL (e.g. acme.com, https://acme.io)
- company_linkedin_url — explicitly a company LinkedIn page URL
- email — the person's email address
- country — country or location
- seniority — an explicit seniority level column (e.g. "C-Level", "Manager")
- ignore — column is irrelevant to outreach (notes, ids, timestamps, arbitrary data)

## Rules
- Map every input column to exactly one canonical field. If unsure and it is clearly irrelevant, use "ignore".
- Only ONE column should map to each of first_name/last_name/full_name/email/linkedin_url/company_name/company_domain. If two columns look like the same field, map the better one and set the other to "ignore".
- confidence is 0.0–1.0. Use < 0.6 when the header is ambiguous or the samples are inconsistent.
- For any column you map with confidence < 0.6, ALSO emit a clarifying question so the user can confirm. Do not ask about high-confidence columns.
- Questions use widget "single_select" with the candidate canonical fields as options (value = canonical field, label = human text), plus allow_free_text=false. Keep to at most 3 questions.

## Output — return ONLY valid JSON matching this schema exactly:
```json
{
  "mapping": { "<original column header>": "<canonical field>", ... },
  "confidence": { "<original column header>": 0.0, ... },
  "questions": [
    {
      "id": "col::<original column header>",
      "question": "Which field is the \\"<header>\\" column?",
      "widget": "single_select",
      "options": [ {"value": "company_name", "label": "Company name"}, {"value": "ignore", "label": "Ignore this column"} ],
      "allow_free_text": false,
      "column": "<original column header>"
    }
  ]
}
```
Return mapping keys EXACTLY as the input column headers (same casing/spacing). Output nothing but the JSON object."""


# ── Prompt Registry + DB-backed get_system_prompt ─────────────────────────────

PROMPT_REGISTRY: dict[str, dict] = {
    "assessment": {
        "name": "Prospect Assessment",
        "description": "AI scoring of prospect fit (0-100) with buying signals, priority tier, and reasoning.",
        "default": ASSESSMENT_SYSTEM_PROMPT,
    },
    "campaign_outreach": {
        "name": "Campaign Outreach Generator",
        "description": "Generates personalized cold email, LinkedIn connection, and InMail for smart campaigns.",
        "default": CAMPAIGN_OUTREACH_SYSTEM_PROMPT,
    },
    "outreach_v2": {
        "name": "V2 Outreach (Prospect Detail Dialog)",
        "description": "One-off outreach with A/B/C subject variants for the prospect detail 'Outreach with AI' panel.",
        "default": OUTREACH_SYSTEM_PROMPT_V2,
    },
    "employee_ranking": {
        "name": "Employee Ranking",
        "description": "Ranks LinkedIn employees by decision-maker potential for auto-discovery.",
        "default": EMPLOYEE_RANKING_SYSTEM_PROMPT,
    },
    "campaign_prefill": {
        "name": "Campaign Prefill (NL to Config)",
        "description": "Converts a natural-language conversation into structured campaign configuration JSON.",
        "default": CAMPAIGN_PREFILL_SYSTEM_PROMPT,
    },
    "campaign_prefill_compose": {
        "name": "Campaign Prefill — Compose",
        "description": "Composes the final campaign JSON from pre-captured targeting fields. No clarification questions.",
        "default": CAMPAIGN_PREFILL_COMPOSE_SYSTEM_PROMPT,
    },
    "lead_column_mapping": {
        "name": "Lead List Column Mapping (BYOL)",
        "description": "Maps uploaded spreadsheet columns to canonical lead fields and asks clarifying questions for ambiguous columns.",
        "default": LEAD_COLUMN_MAPPING_SYSTEM_PROMPT,
    },
}


async def get_system_prompt(slug: str, account_id: str | None = None) -> str:
    """
    Fetch a tenant prompt override, or the registry default.

    Missing tenant context deliberately skips DB overrides. A global slug-only
    lookup could otherwise apply one customer's prompt to every account.
    """
    import database

    now = time.time()
    tenant_key = str(account_id) if account_id else "__registry_default__"
    cache_key = (tenant_key, slug)
    cached = _prompt_cache.get(cache_key)
    if cached:
        content, ts = cached
        if now - ts < _PROMPT_CACHE_TTL:
            return content

    try:
        doc = None
        if account_id:
            account_values: list[object] = [str(account_id)]
            try:
                from bson import ObjectId
                account_values.append(ObjectId(str(account_id)))
            except Exception:
                pass
            doc = await database.system_prompts_collection.find_one(
                {"slug": slug, "account_id": {"$in": account_values}}
            )
        if doc and doc.get("content"):
            content = doc["content"]
            _prompt_cache[cache_key] = (content, now)
            return content
    except Exception as e:
        logger.warning(f"Failed to fetch system prompt '{slug}' from DB: {e}")

    default = PROMPT_REGISTRY.get(slug, {}).get("default", "")
    _prompt_cache[cache_key] = (default, now)
    return default


def clear_prompt_cache(slug: str | None = None) -> None:
    """Clear the prompt cache for a specific slug or all slugs."""
    if slug:
        for key in [key for key in _prompt_cache if key[1] == slug]:
            _prompt_cache.pop(key, None)
    else:
        _prompt_cache.clear()


# ── Reply Classifier ─────────────────────────────────────────────────────────

REPLY_CLASSIFIER_SYSTEM_PROMPT = """You are a B2B reply classifier. Classify inbound prospect messages into exactly one of 6 categories.

## Categories
- POSITIVE: prospect expresses interest, wants to learn more, open to a call, asks about next steps
- QUESTION: neutral question about the product/service, pricing, or process — no buying signal yet
- SOFT_OBJECTION: timing ("not now"), budget concern, or "reach out later" — no hostility
- HARD_OBJECTION: firm rejection, hostile or irritated tone, requests to be removed, explicit "not interested"
- OOO: out-of-office auto-reply or person mentions they are away / unavailable until a specific date
- UNSUBSCRIBE: explicit request to stop all contact, unsubscribe, or "remove me from your list"

## Output — return ONLY valid JSON matching this schema exactly:
```json
{
  "category": "POSITIVE|QUESTION|SOFT_OBJECTION|HARD_OBJECTION|OOO|UNSUBSCRIBE",
  "confidence": 0.95,
  "signals": {
    "sentiment": "positive|neutral|negative",
    "ooo_return_date": "2026-06-15 or null",
    "unsubscribe_phrase": "exact phrase or null",
    "objection_category": "pricing|timing|authority|need|trust or null",
    "objection_phrasing": "verbatim objection text or null",
    "hostility_score": 0.0
  }
}
```

## Few-shot examples
POSITIVE: "Would love to hear more — can we jump on a call this week?"
QUESTION: "How does your pricing work for teams under 50 people?"
SOFT_OBJECTION: "We're heads down on a product launch, ping me in Q3."
HARD_OBJECTION (hostility 0.8): "Stop spamming me. This is completely irrelevant to what we do."
HARD_OBJECTION (hostility 0.2): "We're not interested at this time, thanks."
OOO: "I'm out of the office until June 20th. For urgent matters contact jane@company.com."
UNSUBSCRIBE: "Please remove me from your list. I don't want to receive these messages."

Return ONLY the JSON object. No markdown, no explanation."""


def build_reply_classifier_user_prompt(message_text: str, channel: str, conversation_context: str = "") -> str:
    parts = [f"Channel: {channel}"]
    if conversation_context:
        parts.append(f"\nRecent conversation context:\n{conversation_context[:800]}")
    parts.append(f"\nMessage to classify:\n{message_text[:2000]}")
    return "\n".join(parts)


# ── Per-category reply prompts ────────────────────────────────────────────────

REPLY_POSITIVE_SYSTEM_PROMPT = """You are an expert B2B sales development rep. A prospect has expressed interest or is open to a meeting.

Your goal: lock in the meeting. Propose 3 specific time slots and include the booking page link.

Guidelines:
- Open with genuine enthusiasm (brief, 1 sentence)
- Reference one specific detail from their message showing you read it
- Propose 3 slots as concrete times (e.g., "Tuesday June 10 at 2pm ET, Wednesday June 11 at 10am ET, or Thursday June 12 at 3pm ET")
- Include the booking link: {booking_link}
- Close with the meeting agenda (1 sentence)
- Total: under 120 words
- Sign off with sender's first name only
- Never use "synergize", "circle back", "touch base", "pick your brain"

Inject the sender's voice profile tone: {voice_tone}"""

REPLY_POSITIVE_SYSTEM_PROMPT_FALLBACK = """You are an expert B2B sales development rep. A prospect has expressed interest or is open to a meeting.

Your goal: lock in the meeting. Propose 3 specific time slots.

Guidelines:
- Open with genuine enthusiasm (brief, 1 sentence)
- Reference one specific detail from their message showing you read it
- Propose 3 slots as concrete times
- Close with the meeting agenda (1 sentence)
- Total: under 120 words
- Sign off with sender's first name only"""


def build_reply_positive_prompt(
    message_text: str,
    conversation_context: str,
    prospect_name: str,
    company_name: str,
    proposed_slots: list[str],
    booking_link: str,
    voice_profile: dict | None = None,
    discovery_agenda: str = "",
) -> str:
    slots_str = "\n".join(f"- {s}" for s in proposed_slots)
    return f"""Prospect: {prospect_name} @ {company_name}

Their message:
{message_text[:1000]}

Recent conversation:
{conversation_context[:600]}

Proposed time slots (use these exact times):
{slots_str}

Booking link: {booking_link}
Meeting agenda hint: {discovery_agenda or "Learn about their current workflow and explore fit"}

{build_voice_block(voice_profile)}

Write the reply. Reply only with the message text."""


REPLY_QUESTION_SYSTEM_PROMPT = """You are an expert B2B sales development rep. A prospect asked a question.

Your goal: answer it directly and briefly, then pivot toward a discovery call.

Guidelines:
- Answer the question in 1-2 sentences — be specific, not vague
- Use one concrete proof point or stat if relevant
- Pivot: "Happy to go deeper on [X] in a quick call — would [DAY] work?"
- Total: under 100 words
- Sign off with sender's first name only
- Match their communication style (formal/casual)"""


def build_reply_question_prompt(
    message_text: str,
    conversation_context: str,
    prospect_name: str,
    company_name: str,
    company_profile: dict,
    voice_profile: dict | None = None,
) -> str:
    case_studies = (company_profile.get("case_studies") or [])[:2]
    case_study_str = "; ".join(
        f"{cs.get('client', '')}: {cs.get('outcome', '')}" for cs in case_studies
    )
    return f"""Prospect: {prospect_name} @ {company_name}
Their question: {message_text[:800]}

Relevant proof points: {case_study_str or 'None available'}

Recent conversation context:
{conversation_context[:400]}

{build_voice_block(voice_profile)}

Write a reply that answers their question and pivots to a call. Reply only with the message text."""


REPLY_SOFT_OBJECTION_SYSTEM_PROMPT = """You are an expert B2B sales development rep. A prospect gave a soft objection (timing, budget, busy).

Your goal: acknowledge gracefully, reframe, and re-propose connection in 4-6 weeks.

Guidelines:
- Validate their reality in 1 sentence (don't fight it)
- Offer one relevant insight or result that might shift their thinking
- Suggest a light future touch: "Would it make sense to reconnect in [X weeks]?"
- Total: under 90 words
- No pressure, no desperation
- Sign off with sender's first name only"""


def build_reply_soft_objection_prompt(
    message_text: str,
    conversation_context: str,
    prospect_name: str,
    company_name: str,
    objection_bank: list[dict],
    company_profile: dict,
    voice_profile: dict | None = None,
) -> str:
    matching_rebuttal = ""
    if objection_bank:
        msg_lower = message_text.lower()
        for ob in objection_bank:
            phrasing = (ob.get("phrasing") or "").lower()
            if phrasing and any(word in msg_lower for word in phrasing.split()[:3]):
                matching_rebuttal = ob.get("rebuttal_text", "")
                break

    return f"""Prospect: {prospect_name} @ {company_name}
Their objection: {message_text[:600]}

Matching rebuttal from our objection bank: {matching_rebuttal or 'None — use best judgment'}
Our primary CTA: {company_profile.get('primary_cta', 'Schedule a discovery call')}

Recent conversation:
{conversation_context[:400]}

{build_voice_block(voice_profile)}

Write a reply that acknowledges their objection and keeps the door open. Reply only with the message text."""


REPLY_HARD_OBJECTION_SYSTEM_PROMPT = """You are an expert B2B sales development rep. A prospect gave a firm rejection.

Your goal: close gracefully without burning the bridge.

Guidelines:
- Respect their decision completely — no pushback
- One warm sentence acknowledging their response
- Leave the door open: "If anything changes, you know where to find us"
- Total: under 60 words
- Do NOT include a booking link or propose a call
- Sign off with sender's first name only"""


def build_reply_hard_objection_prompt(
    message_text: str,
    prospect_name: str,
    company_name: str,
    voice_profile: dict | None = None,
) -> str:
    return f"""Prospect: {prospect_name} @ {company_name}
Their message: {message_text[:600]}

{build_voice_block(voice_profile)}

Write a graceful closing reply. Reply only with the message text."""


REPLY_OOO_SYSTEM_PROMPT = """You are an expert B2B sales development rep. The prospect sent an out-of-office reply.

Your goal: acknowledge it and set a follow-up reminder (do not send a reply — just generate what to log).

Output format: Return a single JSON object:
{{"action": "pause_and_resume", "resume_date": "YYYY-MM-DD or null if not parseable", "notes": "brief note"}}"""


def build_reply_ooo_prompt(message_text: str, current_date_iso: str) -> str:
    return f"""Today's date: {current_date_iso}

OOO message:
{message_text[:1000]}

Extract the return date if present, or return null. Output only the JSON object."""


# ── Sender Voice Synthesis ────────────────────────────────────────────────────

SENDER_VOICE_SYNTHESIS_PROMPT = """You are a communication style analyst. Analyze the LinkedIn posts below and extract a structured voice profile.

Output ONLY valid JSON matching this schema:
{
  "tone_markers": ["list of 3-5 adjectives describing communication style"],
  "sentence_patterns": ["short/punchy", "storytelling", "question-led", "data-driven", "etc"],
  "vocab_signature": ["3-5 distinctive words or phrases this person favors"],
  "formality_level": "formal|semi-formal|casual",
  "average_post_length": "short (<50 words)|medium (50-150)|long (150+)",
  "uses_emojis": true,
  "call_to_action_style": "direct|soft|question|none",
  "post_topics": ["main themes in their content"],
  "synthesized_summary": "2-3 sentence description of their authentic voice for cold outreach ghostwriting"
}

Base your analysis ONLY on the posts provided. If fewer than 3 posts are available, note lower confidence in synthesized_summary."""


def build_sender_voice_synthesis_prompt(
    posts: list[dict],
    sender_name: str,
    sender_role: str,
    headline: str = "",
) -> str:
    post_texts = []
    for i, p in enumerate(posts[:15], 1):  # cap at 15 posts for prompt size
        text = p.get("text") or p.get("content") or p.get("commentary", "")
        if text:
            post_texts.append(f"Post {i}:\n{text[:500]}")

    posts_str = "\n\n".join(post_texts) if post_texts else "No posts available."
    return f"""Sender: {sender_name}
Role: {sender_role}
Headline: {headline}

LinkedIn Posts:
{posts_str}

Analyze the posts above and return the voice profile JSON."""


# ── Onboarding preview message ────────────────────────────────────────────────

ONBOARDING_PREVIEW_MESSAGE_PROMPT = """You are an expert B2B cold-outreach ghostwriter. Write ONE short LinkedIn connection-style outreach message from the sender to the prospect.

Rules:
- 40-80 words, plain text only. No subject line, no signature block, no placeholders like [Name].
- Personalize with the prospect's role, company, and industry.
- Lead with relevance to the prospect's likely pain points; end with a soft, low-friction call to action.
- If a sender voice profile is provided, match its tone, sentence patterns, and formality exactly — the message must sound like the sender wrote it.
- Never invent facts, metrics, or claims not present in the context.

Output ONLY the message text — no preamble, no quotes, no markdown."""


def build_onboarding_preview_message_prompt(
    company_profile: dict,
    prospect: dict,
    voice_profile: dict | None = None,
) -> str:
    """User prompt for the onboarding sample outreach message (stage 4/5 preview)."""
    services = ", ".join((company_profile.get("services") or [])[:5])
    pain_points = ", ".join((company_profile.get("pain_points") or [])[:5])

    parts = [
        f"Sender: {company_profile.get('sender_name') or 'the sender'}"
        + (f" ({company_profile.get('sender_role')})" if company_profile.get("sender_role") else ""),
        f"Sender's company: {company_profile.get('company_name') or 'their company'}",
    ]
    if services:
        parts.append(f"What they offer: {services}")
    if pain_points:
        parts.append(f"Pain points they solve: {pain_points}")
    diffs = ", ".join((company_profile.get("differentiators") or [])[:3])
    if diffs:
        parts.append(f"Differentiators: {diffs}")
    best_cs = _select_best_case_study(company_profile.get("case_studies") or [], prospect)
    if best_cs:
        client = best_cs.get("client") or ""
        outcome = best_cs.get("outcome") or ""
        metric = best_cs.get("metric") or ""
        label = f"{client} — {outcome}" if client and outcome else (client or outcome)
        if label:
            parts.append(f"Proof point: {label}" + (f" ({metric})" if metric else ""))
    if company_profile.get("primary_cta"):
        parts.append(f"Primary call to action: {company_profile['primary_cta']}")

    parts.append("")
    parts.append(f"Prospect: {prospect.get('full_name') or 'the prospect'}")
    if prospect.get("job_title"):
        parts.append(f"Prospect title: {prospect['job_title']}")
    if prospect.get("company_name"):
        parts.append(f"Prospect company: {prospect['company_name']}")
    if prospect.get("industry"):
        parts.append(f"Prospect industry: {prospect['industry']}")
    if prospect.get("country"):
        parts.append(f"Prospect country: {prospect['country']}")

    parts = _inject_voice_profile(parts, voice_profile)
    parts.append("")
    parts.append("Write the outreach message now.")
    return "\n".join(parts)


# ── Onboarding refinement ─────────────────────────────────────────────────────

ONBOARDING_REFINEMENT_SYSTEM_PROMPT = """You are an expert B2B sales coach helping a user build their AI outreach system.

Your goal is to extract, one piece at a time, the following information from the user:
1. Common objections they hear and their ideal rebuttals
2. Competitors they face and why they are different
3. Phrases or approaches they want to AVOID in outreach

Ask focused questions one at a time. After each answer, either:
a) Acknowledge and ask a follow-up to deepen, OR
b) Acknowledge and move to the next topic

When you capture a piece of structured data, output it as a JSON block inside triple backticks:
```json
{"objection": {"phrasing": "...", "category": "price|timing|fit|competitor|other", "rebuttal_text": "..."}}
{"competitor": {"name": "...", "our_differentiator": "..."}}
{"banned_phrase": "..."}
```

Keep responses conversational, warm, and brief (2-4 sentences). This is a 10-minute conversation."""


# ── Shared helpers ────────────────────────────────────────────────────────────

EM_DASH_RULE = (
    "Never use em dashes (—) or en dashes (–). Use a comma, a full stop, or "
    "rewrite the sentence instead."
)

MARKDOWN_RULE = (
    "Write plain text only. Never use markdown formatting: no **bold**, no "
    "*italics*, no _underscores_, no backticks, no ## headings. Email and "
    "LinkedIn do not render markdown, so the characters show up literally."
)

STYLE_RULES = f"{EM_DASH_RULE}\n{MARKDOWN_RULE}"

# Emphasis wrappers only. Each requires non-space immediately inside the
# markers so a bullet ("* item") or a stray asterisk is left alone, and the
# single-character forms refuse a neighbouring word character so snake_case
# identifiers and mid-word asterisks survive untouched.
_MARKDOWN_EMPHASIS_PATTERNS = [
    (re.compile(r"\*\*\*(?=\S)(.+?)(?<=\S)\*\*\*", re.S), r"\1"),
    (re.compile(r"___(?=\S)(.+?)(?<=\S)___", re.S), r"\1"),
    (re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*", re.S), r"\1"),
    (re.compile(r"__(?=\S)(.+?)(?<=\S)__", re.S), r"\1"),
    (re.compile(r"(?<![\w*])\*(?=\S)([^*\n]+?)(?<=\S)\*(?![\w*])"), r"\1"),
    (re.compile(r"(?<![\w_])_(?=\S)([^_\n]+?)(?<=\S)_(?![\w_])"), r"\1"),
    (re.compile(r"(?<!`)`(?=\S)([^`\n]+?)(?<=\S)`(?!`)"), r"\1"),
]


def strip_em_dashes(text: str) -> str:
    """Replace em/en dashes with a comma so no generated copy ever ships one.

    Canonical implementation. The prompt asks the model to avoid them, this
    enforces it on the way out for every message and reply.
    """
    if not text:
        return text
    text = re.sub(r"\s*[—–]\s*", ", ", text)
    return re.sub(r"[—–]", ", ", text)


def strip_markdown_emphasis(text: str) -> str:
    """Remove markdown emphasis markers, keeping the words they wrapped.

    Neither email nor LinkedIn renders markdown, so `**Tuesday**` reaches the
    prospect with the asterisks visible. Leading list markers are deliberately
    preserved: a "- " bullet reads fine as plain text.
    """
    if not text:
        return text
    for pattern, replacement in _MARKDOWN_EMPHASIS_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def sanitize_generated_text(text: str) -> str:
    """Apply every outgoing-copy rule the prompts also state. Order-independent."""
    return strip_markdown_emphasis(strip_em_dashes(text))


def _stringify_voice_list(value) -> str:
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if v)
    return str(value or "")


def build_voice_block(voice_profile: dict | None) -> str:
    """Render the sender's full voice profile as prompt context.

    Everything the synthesis step captured is passed through, not just the tone
    markers — the summary, sentence patterns, vocabulary, formality and CTA
    style are what actually make generated copy sound like the sender.
    Returns "" when no profile exists.
    """
    if not voice_profile:
        # No profile yet, but the style rules still apply to every message.
        return f"## Style Rules\n{STYLE_RULES}"

    lines = ["## Voice profile - write every word as this person would"]
    fields = [
        ("Tone", voice_profile.get("tone_markers")),
        ("Sentence patterns", voice_profile.get("sentence_patterns")),
        ("Favoured vocabulary", voice_profile.get("vocab_signature")),
        ("Formality", voice_profile.get("formality_level")),
        ("Typical length", voice_profile.get("average_post_length")),
        ("CTA style", voice_profile.get("call_to_action_style")),
        ("Recurring topics", voice_profile.get("post_topics")),
    ]
    for label, value in fields:
        rendered = _stringify_voice_list(value)
        if rendered:
            lines.append(f"{label}: {rendered}")

    uses_emojis = voice_profile.get("uses_emojis")
    if uses_emojis is not None:
        lines.append(
            "Emojis: uses them naturally" if uses_emojis else "Emojis: never uses them"
        )

    summary = (voice_profile.get("synthesized_summary") or "").strip()
    if summary:
        lines.append(f"Voice summary: {summary}")

    if len(lines) == 1:
        return f"## Style Rules\n{STYLE_RULES}"
    lines.append(
        "Match this voice closely, but never fabricate claims to fit it. "
        + STYLE_RULES
    )
    return "\n".join(lines)


def _inject_voice_profile(prompt_parts: list[str], voice_profile: dict | None) -> list[str]:
    """Append voice profile context to a prompt parts list."""
    block = build_voice_block(voice_profile)
    if block:
        prompt_parts.append("\n" + block)
    return prompt_parts


def _redact_banned_phrases(text: str, banned_phrases: list[str]) -> str:
    """Remove banned phrases from outgoing message text (case-insensitive)."""
    if not banned_phrases or not text:
        return text
    for phrase in banned_phrases:
        if phrase and phrase.lower() in text.lower():
            idx = text.lower().find(phrase.lower())
            text = text[:idx] + text[idx + len(phrase):]
    return text.strip()

