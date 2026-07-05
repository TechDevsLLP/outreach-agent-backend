import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PrefilterResult:
    prospect_key: str
    passes: bool
    confidence: float
    fit_summary: str
    reject_reason: Optional[str]
    error: bool = False


async def prefilter_candidates(
    candidates: list[dict],
    icp: dict,
    *,
    batch_size: int = 20,
    concurrency: int = 4,
    confidence_threshold: float = 0.4,
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[PrefilterResult]:
    from config import get_settings
    from services.openrouter_service import OpenRouterClient
    from utils.prompts import PREFILTER_SYSTEM_PROMPT, build_prefilter_user_prompt

    settings = get_settings()

    keys: list[str] = []
    for i, c in enumerate(candidates):
        keys.append(c.get("linkedin") or c.get("email") or f"idx_{i}")

    batches: list[list[tuple[int, dict]]] = []
    for i in range(0, len(candidates), batch_size):
        chunk = list(enumerate(candidates[i:i + batch_size]))
        batches.append(chunk)

    semaphore = asyncio.Semaphore(concurrency)
    results_map: dict[int, PrefilterResult] = {}

    client = OpenRouterClient()
    try:
        async def process_batch(batch_offset: int, batch: list[tuple[int, dict]]) -> None:
            async with semaphore:
                candidates_for_batch = [
                    {
                        "idx": local_i,
                        "full_name": c.get("full_name") or "",
                        "job_title": c.get("job_title") or c.get("title") or "",
                        "headline": c.get("headline") or "",
                        "company_name": c.get("company_name") or "",
                        "company_industry": c.get("industry") or c.get("company_industry") or "",
                        "company_size": c.get("company_size"),
                        "country": c.get("country") or "",
                        "linkedin_current_company": (c.get("linkedin_snapshot") or {}).get("current_company"),
                    }
                    for local_i, (_, c) in enumerate(batch)
                ]

                user_prompt = build_prefilter_user_prompt(icp, candidates_for_batch)
                messages = [
                    {"role": "system", "content": PREFILTER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ]

                model = getattr(settings, "prefilter_model", "google/gemini-3-flash-preview")

                raw_text: Optional[str] = None
                try:
                    response = await client.chat_completion(
                        messages=messages,
                        model=model,
                        temperature=0.1,
                        max_tokens=4096,
                        account_id=account_id,
                        campaign_id=campaign_id,
                        feature="prefilter",
                    )
                    raw_text = response.get("content", "")
                except Exception as e:
                    logger.error(f"prefilter_candidates: AI call failed for batch offset {batch_offset}: {e}")
                    for local_i, (_, c) in enumerate(batch):
                        gi = batch_offset + local_i
                        results_map[gi] = PrefilterResult(
                            prospect_key=keys[gi],
                            passes=True,
                            confidence=0.0,
                            fit_summary="",
                            reject_reason=None,
                            error=True,
                        )
                    return

                parsed: Optional[list] = None
                if raw_text:
                    text = raw_text.strip()
                    if text.startswith("```"):
                        first_newline = text.index("\n") if "\n" in text else 3
                        text = text[first_newline + 1:]
                        if text.endswith("```"):
                            text = text[:-3]
                        text = text.strip()
                    try:
                        parsed = json.loads(text)
                    except json.JSONDecodeError:
                        m = re.search(r"\[.*\]", text, re.DOTALL)
                        if m:
                            try:
                                parsed = json.loads(m.group(0))
                            except json.JSONDecodeError:
                                pass

                if not isinstance(parsed, list):
                    logger.error(
                        f"prefilter_candidates: could not parse AI response for batch offset {batch_offset}. "
                        f"Raw: {(raw_text or '')[:300]!r}"
                    )
                    for local_i, (_, c) in enumerate(batch):
                        gi = batch_offset + local_i
                        results_map[gi] = PrefilterResult(
                            prospect_key=keys[gi],
                            passes=True,
                            confidence=0.0,
                            fit_summary="",
                            reject_reason=None,
                            error=True,
                        )
                    return

                idx_to_item: dict[int, dict] = {item.get("idx", i): item for i, item in enumerate(parsed) if isinstance(item, dict)}

                for local_i, (_, c) in enumerate(batch):
                    gi = batch_offset + local_i
                    item = idx_to_item.get(local_i)
                    if not item:
                        results_map[gi] = PrefilterResult(
                            prospect_key=keys[gi],
                            passes=True,
                            confidence=0.0,
                            fit_summary="",
                            reject_reason=None,
                            error=True,
                        )
                        continue

                    confidence = float(item.get("confidence", 0.5))
                    passes = bool(item.get("passes", True))
                    if confidence < confidence_threshold and passes:
                        passes = True

                    results_map[gi] = PrefilterResult(
                        prospect_key=keys[gi],
                        passes=passes,
                        confidence=confidence,
                        fit_summary=str(item.get("fit_summary", ""))[:120],
                        reject_reason=item.get("reject_reason") if not passes else None,
                    )

        tasks = [
            process_batch(i * batch_size, batch)
            for i, batch in enumerate(batches)
        ]
        await asyncio.gather(*tasks)
    finally:
        await client.close()

    return [results_map[i] for i in range(len(candidates))]
