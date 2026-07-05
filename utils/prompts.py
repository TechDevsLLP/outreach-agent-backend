"""
Prompt templates for AI assessment and outreach generation.
Keeps prompts out of service logic for maintainability.
"""

import json
import time
import logging

logger = logging.getLogger(__name__)

_prompt_cache: dict[str, tuple[str, float]] = {}
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
- Sign off with first name only

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
    "body": "Email body — no subject, no greeting line salutation at start, sign off with sender name"
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

4. **Length**: cold email body 80-130 words, LinkedIn note ≤280 chars (hard limit), InMail body 100-160 words

5. **One ask**: Exactly one CTA. No "or" options, no multiple links. Match CTA to the campaign's cta_type.

6. **Sign-off**: Use sender's first name only (never last name unless explicitly in sender data). No "Best regards", no "Thanks in advance".

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
    """Pick the case study whose industry best matches the prospect's industry."""
    if not case_studies:
        return None
    prospect_industry = (prospect.get("industry") or "").lower()
    if not prospect_industry:
        return case_studies[0] if case_studies else None
    for cs in case_studies:
        cs_industry = (cs.get("industry") or cs.get("client") or "").lower()
        if prospect_industry in cs_industry or cs_industry in prospect_industry:
            return cs
    return case_studies[0]


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
            parts.append(f"Proof Point: {client} — {outcome}" + (f" ({metric})" if metric else ""))

    parts.append("")

    # CTA
    cta_type = campaign.get("cta_type") or "reply"
    cta_url = campaign.get("cta_url") or ""
    parts.append("## Call to Action")
    if cta_type == "book_call":
        parts.append(f"CTA: Ask them to book a call." + (f" Link: {cta_url}" if cta_url else ""))
    elif cta_type == "visit_link":
        parts.append(f"CTA: Direct them to visit: {cta_url}" if cta_url else "CTA: Soft ask to visit a resource.")
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
            name = c.get("name") or str(c)
            diff = c.get("differentiation") or c.get("description") or ""
            parts.append(f"  - {name}" + (f": {diff[:100]}" if diff else ""))

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
) -> str:
    """
    Build a batch prompt that asks the model to generate one message per prospect.
    prospects_with_ids: [(enrollment_id_str, prospect_dict), ...]
    channel: "email" | "linkedin_connection" | "linkedin_inmail"
    """
    tone = campaign.get("message_tone") or "professional"
    tone_desc = TONE_DESCRIPTIONS.get(tone, TONE_DESCRIPTIONS["professional"])
    value_prop = campaign.get("value_proposition") or ""
    pain_point = campaign.get("pain_point") or ""
    sender_name = campaign.get("sender_name") or ""
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
        parts.append(f"Sign off as: {sender_first}")
    if cta_type == "book_call":
        parts.append(f"CTA: Ask to book a call." + (f" Link: {cta_url}" if cta_url else ""))
    elif cta_type == "visit_link":
        parts.append(f"CTA: Visit link: {cta_url}" if cta_url else "CTA: Visit a resource.")
    else:
        parts.append("CTA: Ask a single reply-inviting question.")
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
        ai_fit_score = prospect.get("ai_fit_score") or prospect.get("ai_prospect_score")
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

    # Value context
    if campaign.get("value_proposition"):
        parts.append(f"Value Proposition: {campaign['value_proposition']}")
    if campaign.get("pain_point"):
        parts.append(f"Pain Point: {campaign['pain_point']}")
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
    else:
        parts.append("InMail body: 100-140 words.")

    parts.append("")
    parts.append("## Anti-Repetition Rule")
    parts.append("This message MUST introduce a new specific detail, angle, or signal not used in any prior message above. No 'just following up'. No re-stating what you already said. Sound like a real colleague checking in with something new.")
    parts.append("")
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
    "cta_type": ["book_call", "reply", "visit_link"],
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
      "icp_job_titles": [],
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
- icp_job_titles: leave empty [] since function+seniority filters are used instead.

## Valid LinkedIn industry slugs:
""" + "\n".join(f"- {ind}" for ind in _PREFILL_VALID_INDUSTRIES)


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
}


async def get_system_prompt(slug: str) -> str:
    """
    Fetch system prompt for the given slug.
    Checks the DB system_prompts collection first (with 60s TTL cache), then falls back to PROMPT_REGISTRY default.
    """
    import database

    now = time.time()
    cached = _prompt_cache.get(slug)
    if cached:
        content, ts = cached
        if now - ts < _PROMPT_CACHE_TTL:
            return content

    try:
        doc = await database.system_prompts_collection.find_one({"slug": slug})
        if doc and doc.get("content"):
            content = doc["content"]
            _prompt_cache[slug] = (content, now)
            return content
    except Exception as e:
        logger.warning(f"Failed to fetch system prompt '{slug}' from DB: {e}")

    default = PROMPT_REGISTRY.get(slug, {}).get("default", "")
    _prompt_cache[slug] = (default, now)
    return default


def clear_prompt_cache(slug: str | None = None) -> None:
    """Clear the prompt cache for a specific slug or all slugs."""
    if slug:
        _prompt_cache.pop(slug, None)
    else:
        _prompt_cache.clear()


# ── Campaign Prefill (user prompt builder) ────────────────────────────────────

PREFILTER_SYSTEM_PROMPT = """You are a B2B lead qualification expert. Assess whether each prospect fits the Ideal Customer Profile (ICP).

Analyze job title, headline, company, industry, and geography against the ICP criteria.
Be strict on clear mismatches; pass uncertain cases with lower confidence.

Return ONLY a valid JSON array — no markdown, no explanation, no extra text.

Format (one object per prospect, preserve input order):
[
  {
    "idx": <integer>,
    "passes": <true|false>,
    "confidence": <float 0.0-1.0>,
    "fit_summary": "<max 120 chars>",
    "reject_reason": <null | "title_mismatch" | "function_mismatch" | "company_stage_mismatch" | "seniority_too_junior" | "out_of_market" | "other">
  }
]

Rules:
- passes=true when role and company clearly match the ICP
- passes=false only on clear mismatch
- reject_reason must be null when passes=true
- When uncertain, set passes=true with confidence < 0.5
"""


def build_prefilter_user_prompt(icp: dict, candidates: list[dict]) -> str:
    import json as _json
    icp_block = {k: v for k, v in {
        "industries": icp.get("icp_industries") or [],
        "job_titles": icp.get("icp_job_titles") or [],
        "seniority": icp.get("icp_seniority_levels") or [],
        "countries": icp.get("icp_countries") or [],
        "company_size_min": icp.get("icp_company_size_min"),
        "company_size_max": icp.get("icp_company_size_max"),
        "value_proposition": icp.get("value_proposition") or "",
        "pain_point": icp.get("pain_point") or "",
        "functional_departments": icp.get("icp_functional_departments") or [],
    }.items() if v not in (None, [], "")}

    return (
        f"ICP:\n{_json.dumps(icp_block, indent=2)}\n\n"
        f"Prospects:\n{_json.dumps(candidates, indent=2)}\n\n"
        f"Return JSON array for idx 0..{len(candidates)-1}."
    )


def build_campaign_prefill_prompt(conversation: list[dict], company_profile: dict | None = None, captured: dict | None = None) -> str:
    """Build the user-turn prompt for campaign prefill from NL."""
    parts = []

    if company_profile:
        parts.append(f"""Company context:
- Company: {company_profile.get('company_name', 'Unknown')}
- Industry: {company_profile.get('industry', '')}
- Value proposition: {company_profile.get('value_proposition', '')}
- Target customer: {company_profile.get('target_customer_description', '')}
""")

    if captured:
        captured_lines = [f"- {k}: {v}" for k, v in captured.items() if v]
        if captured_lines:
            parts.append("Already captured (do NOT ask about these again):")
            parts.extend(captured_lines)
            parts.append("")

    parts.append("Conversation so far:")
    for msg in conversation:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        parts.append(f"{role.capitalize()}: {content}")

    parts.append("\nExtract the campaign configuration from this conversation.")


# ── Curated discovery batch scoring ──────────────────────────────────────────

COMPANY_BATCH_SCORE_SYSTEM_PROMPT = """You are a B2B market research analyst scoring companies for ICP (Ideal Customer Profile) fit.

Given a list of companies and an ICP description, score each company 0–100 for how well it matches.

Scoring criteria:
- Industry alignment with ICP (40 pts)
- Geographic match if ICP specifies countries (20 pts)
- Company size within ICP range (20 pts)
- Keyword/descriptor match in description (20 pts)

Return ONLY valid JSON: {"scores": [<int>, <int>, ...]} — one integer per company in INPUT ORDER.
No explanation. No markdown. Only the JSON object."""


def build_company_batch_score_prompt(companies: list[dict], icp_prompt: str) -> str:
    """Build scoring prompt for a batch of sourced companies."""
    lines = [
        f"ICP Description: {icp_prompt}",
        "",
        "Score each company 0-100 for ICP fit. Return JSON: {\"scores\": [...]} in the same order.",
        "",
        "Companies:",
    ]
    for i, c in enumerate(companies, 1):
        lines.append(
            f"{i}. {c.get('company_name', 'Unknown')} | "
            f"Industry: {c.get('industry', 'Unknown')} | "
            f"Country: {c.get('country', 'Unknown')} | "
            f"Size: {c.get('employee_size_estimate', 'Unknown')} | "
            f"Description: {(c.get('description') or '')[:150]}"
        )
    return "\n".join(lines)


EMPLOYEE_BATCH_SCORE_SYSTEM_PROMPT = """You are a B2B sales analyst scoring LinkedIn prospects for outreach fit.

Given a list of employees and campaign context, score each person 0–100 as a cold outreach target.

Scoring criteria:
- Seniority match with target levels (35 pts)
- Role/function alignment with target departments (30 pts)
- ICP industry and company fit (20 pts)
- Location in target countries (15 pts)

Return ONLY valid JSON: {"scores": [<int>, <int>, ...]} — one integer per employee in INPUT ORDER.
No explanation. No markdown. Only the JSON object."""


def build_employee_batch_score_prompt(
    employees: list[dict],
    icp_prompt: str,
    sender_context: str = "",
) -> str:
    """Build scoring prompt for a batch of scraped employees."""
    lines = [
        f"ICP Description: {icp_prompt}",
    ]
    if sender_context:
        lines.append(f"Sender context: {sender_context[:200]}")
    lines.extend([
        "",
        "Score each employee 0-100 for cold outreach fit. Return JSON: {\"scores\": [...]} in order.",
        "",
        "Employees:",
    ])
    for i, e in enumerate(employees, 1):
        lines.append(
            f"{i}. {e.get('full_name', 'Unknown')} | "
            f"Title: {e.get('job_title') or e.get('headline', 'Unknown')} | "
            f"Seniority: {e.get('seniority_level', 'Unknown')} | "
            f"Company: {e.get('company_name', 'Unknown')} | "
            f"Industry: {e.get('industry', 'Unknown')} | "
            f"Country: {e.get('country', 'Unknown')}"
        )
    return "\n".join(lines)


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
    voice_tone = (voice_profile or {}).get("tone_markers", "professional and direct")
    return f"""Prospect: {prospect_name} @ {company_name}

Their message:
{message_text[:1000]}

Recent conversation:
{conversation_context[:600]}

Proposed time slots (use these exact times):
{slots_str}

Booking link: {booking_link}
Meeting agenda hint: {discovery_agenda or "Learn about their current workflow and explore fit"}
Voice tone: {voice_tone}

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

def _inject_voice_profile(prompt_parts: list[str], voice_profile: dict | None) -> list[str]:
    """Append voice profile context to a prompt parts list."""
    if not voice_profile:
        return prompt_parts
    summary = voice_profile.get("synthesized_summary", "")
    tone = ", ".join(voice_profile.get("tone_markers", []))
    if summary or tone:
        prompt_parts.append(f"\nVoice profile — write in this style:\nTone: {tone}\n{summary}")
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


# ── Meeting Proposal ──────────────────────────────────────────────────────────

MEETING_PROPOSAL_PROMPT = """You are writing the final reply to a prospect who has shown genuine interest.
Your goal: express enthusiasm, propose 3 specific meeting times, and include a booking link.

Rules:
- Warm but professional tone
- Reference something specific from their message if possible
- Propose exactly 3 time slots (they will be filled in by the system)
- Include the booking link naturally
- 80 words max
- Never use: "I hope this email finds you well", "circle back", "reach out", "synergy"
"""


def build_meeting_proposal_prompt(
    prospect_name: str,
    company_name: str,
    message_text: str,
    slots: list[dict],
    booking_url: str,
    voice_profile: dict = None,
) -> str:
    slot_lines = "\n".join(f"  • {s['label']}" for s in slots)
    parts = [
        f"Prospect: {prospect_name} at {company_name}",
        f"Their message: {message_text[:400]}",
        f"",
        f"Proposed slots:\n{slot_lines}",
        f"Booking link: {booking_url}",
        f"",
        f"Write the reply using these exact slots and link.",
    ]
    if voice_profile:
        _inject_voice_profile(parts, voice_profile)
    return "\n".join(parts)


# ── Voice Note Script ─────────────────────────────────────────────────────────

VOICE_NOTE_SCRIPT_PROMPT = """You are writing a 30-45 second voice note script for a B2B sales rep.

Rules:
- Conversational, warm — sounds like talking to a colleague not reading a script
- Reference the prospect's company or role specifically
- One clear ask at the end (reply, book a call, etc.)
- Max 90 words (~45s spoken at natural pace)
- Format: plain text, no stage directions, no "Hi, my name is" openers
- Never use: "I hope this finds you well", "circle back", "reach out", "touch base"
"""


def build_voice_note_script_prompt(
    prospect_name: str,
    company_name: str,
    prospect_title: str,
    sender_name: str,
    sender_role: str,
    company_profile: dict,
    previous_touches: int = 0,
    voice_profile: dict = None,
) -> str:
    parts = [
        f"Prospect: {prospect_name}, {prospect_title} at {company_name}",
        f"Sender: {sender_name}, {sender_role}",
        f"Previous touches: {previous_touches}",
        f"",
        f"Write a 30-45 second voice note script. Be specific to this prospect.",
    ]
    if voice_profile:
        _inject_voice_profile(parts, voice_profile)
    return "\n".join(parts)


# ── Video DM Script ───────────────────────────────────────────────────────────

VIDEO_DM_SCRIPT_PROMPT = """You are writing a 30-45 second video DM script for a B2B sales rep.

Rules:
- Opens with something visible/specific about the prospect (their LinkedIn post, company news, role change)
- Demonstrates research — not a generic pitch
- One specific ask at the end
- Max 90 words
- Format: plain text, as if the person is speaking naturally to camera
- Never use: "I hope this finds you well", "circle back", "synergy"
"""


def build_video_dm_script_prompt(
    prospect_name: str,
    company_name: str,
    prospect_title: str,
    sender_name: str,
    sender_role: str,
    company_profile: dict,
    prospect_recent_activity: str = "",
    voice_profile: dict = None,
) -> str:
    parts = [
        f"Prospect: {prospect_name}, {prospect_title} at {company_name}",
        f"Sender: {sender_name}, {sender_role}",
    ]
    if prospect_recent_activity:
        parts.append(f"Recent prospect activity to reference: {prospect_recent_activity[:200]}")
    parts.extend(["", "Write a 30-45 second video DM script. Open with something specific."])
    if voice_profile:
        _inject_voice_profile(parts, voice_profile)
    return "\n".join(parts)
