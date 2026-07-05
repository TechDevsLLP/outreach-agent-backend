"""
AI-powered outreach message generation using OpenRouter.
Generates cold email, LinkedIn connection request, and follow-up messages.
"""

import logging
from services.openrouter_service import OpenRouterClient
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


def _strip_em_dashes(text: str) -> str:
    """Replace em dashes with a comma+space or plain space depending on context."""
    if not text:
        return text
    import re
    # "word — word" -> "word, word"
    text = re.sub(r'\s*—\s*', ', ', text)
    # "word—word" (no spaces) -> "word, word"
    text = re.sub(r'—', ', ', text)
    return text


def _strip_em_dashes_from_messages(data: dict) -> dict:
    """Recursively strip em dashes from all string values in a messages dict."""
    if isinstance(data, dict):
        return {k: _strip_em_dashes_from_messages(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_strip_em_dashes_from_messages(item) for item in data]
    if isinstance(data, str):
        return _strip_em_dashes(data)
    return data




# ═══════════════════════════════════════════════════════════════════════════════
# Phase 5: Improved Outreach - Direct, Value-First with A/B Testing
# ═══════════════════════════════════════════════════════════════════════════════

async def generate_outreach_v2(
    prospect: dict,
    profile: dict | None,
    company: dict | None,
    assessment: dict | None,
    client: OpenRouterClient,
    custom_context: str | None = None,
) -> dict:
    """
    Generate personalized outreach messages with A/B subject line variants (Phase 5).

    Uses new direct, value-first approach:
    - "This is cold outreach, but only because I know we can help"
    - References company news and competitors for specificity
    - Generates 3 subject line variants for A/B testing
    - Assumes the sale in closing

    Returns:
        {
            "cold_email": {
                "body": "...",
                "subject_variants": [
                    {"variant_id": "A", "subject": "...", "is_selected": true},
                    {"variant_id": "B", "subject": "...", "is_selected": false},
                    {"variant_id": "C", "subject": "...", "is_selected": false}
                ]
            },
            "linkedin_connection_request": {...},
            "linkedin_followup": {...}
        }
    """
    from utils.prompts import build_outreach_user_prompt_v2, get_system_prompt

    # Extract competitors from prospect
    competitors = prospect.get("competitors", [])

    user_prompt = build_outreach_user_prompt_v2(
        prospect, profile, company, assessment, competitors, custom_context
    )

    # Use free model pool for outreach generation
    from services.openrouter_service import get_free_model
    model = settings.outreach_model or get_free_model(0)

    messages = [
        {"role": "system", "content": await get_system_prompt("outreach_v2")},
        {"role": "user", "content": user_prompt},
    ]

    try:
        outreach = await client.chat_completion(
            messages=messages,
            model=model,
            temperature=0.8,  # Higher temp for creative variation in subject lines
            max_tokens=4096,
        )

        # Parse JSON if needed
        if isinstance(outreach, dict) and "content" in outreach:
            import json
            content = outreach["content"]
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            content = content.strip()
            try:
                outreach = json.loads(content)
            except json.JSONDecodeError:
                # Response was truncated — attempt recovery via json_repair
                try:
                    from json_repair import repair_json
                    outreach = json.loads(repair_json(content))
                    logger.warning("json_repair recovered a truncated outreach response")
                except Exception:
                    raise

        # Post-process: Set variant A as default selected
        if "cold_email" in outreach and "subject_variants" in outreach["cold_email"]:
            variants = outreach["cold_email"]["subject_variants"]
            if variants and len(variants) > 0:
                variants[0]["is_selected"] = True
                for v in variants[1:]:
                    v["is_selected"] = False

        # Enforce constraints (LinkedIn 300 char limit, etc.)
        outreach = _enforce_constraints_v2(outreach, prospect)

        logger.info(f"Generated v2 outreach for {prospect.get('company_name', 'unknown')} with {len(variants)} subject variants")

        return outreach

    except Exception as e:
        logger.error(f"Outreach v2 generation failed: {e}", exc_info=True)
        company = prospect.get("company_name") or "your team"
        first_name = (prospect.get("first_name") or (prospect.get("full_name") or "").split(" ")[0] or "there").strip()
        return {
            "cold_email": {
                "body": "",
                "subject_variants": [
                    {"variant_id": "A", "subject": f"{first_name}, one idea for {company}"[:50], "is_selected": True},
                    {"variant_id": "B", "subject": f"worth a look at {company}?"[:50], "is_selected": False},
                    {"variant_id": "C", "subject": f"for {first_name}"[:50], "is_selected": False},
                ],
            },
            "linkedin_connection_request": {"message": ""},
            "linkedin_followup": {"message": ""},
            "linkedin_inmail": {"subject": "", "message": ""},
        }


def _enforce_constraints_v2(outreach: dict, prospect: dict) -> dict:
    """Enforce message constraints for Phase 5 outreach format."""
    # Ensure required structure
    if "cold_email" not in outreach:
        outreach["cold_email"] = {"body": "", "subject_variants": []}
    if "linkedin_connection_request" not in outreach:
        outreach["linkedin_connection_request"] = {"message": ""}
    if "linkedin_followup" not in outreach:
        outreach["linkedin_followup"] = {"message": ""}
    if "linkedin_inmail" not in outreach:
        outreach["linkedin_inmail"] = {"subject": "", "message": ""}

    # LinkedIn connection request: HARD LIMIT 300 chars
    conn_msg = outreach.get("linkedin_connection_request", {}).get("message", "")
    if len(conn_msg) > 300:
        truncated = conn_msg[:297]
        last_period = truncated.rfind(".")
        last_space = truncated.rfind(" ")
        if last_period > 200:
            truncated = truncated[:last_period + 1]
        elif last_space > 200:
            truncated = truncated[:last_space] + "..."
        else:
            truncated = truncated + "..."
        outreach["linkedin_connection_request"]["message"] = truncated
        logger.warning(f"Truncated LinkedIn connection request to {len(truncated)} chars")

    # Validate subject line variants
    if "cold_email" in outreach and "subject_variants" in outreach["cold_email"]:
        variants = outreach["cold_email"]["subject_variants"]
        for variant in variants:
            subject = variant.get("subject", "")
            if len(subject) > 70:
                variant["subject"] = subject[:67] + "..."
                logger.warning(f"Truncated subject variant {variant.get('variant_id')} to 70 chars")

    # LinkedIn InMail: subject <= 50 chars
    inmail = outreach.get("linkedin_inmail", {})
    inmail_subject = inmail.get("subject", "")
    if len(inmail_subject) > 50:
        inmail["subject"] = inmail_subject[:47] + "..."
        logger.warning(f"Truncated InMail subject to 50 chars")

    # Strip any em dashes that slipped through
    outreach = _strip_em_dashes_from_messages(outreach)

    return outreach
