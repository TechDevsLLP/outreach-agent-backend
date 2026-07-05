"""
embedding_service.py

Generates 768-dim Gemini text embeddings for companies (profile_vec) and prospects (title_vec).
Model: gemini-embedding-001 via google.genai SDK (async), output_dimensionality=768.

Vectors are stored as BSON Binary subtype 9 (int8, 768 B each) rather than float64 arrays
(9.9 KB each). This is the Atlas Vector Search native int8 format and fits comfortably
within the 512 MB free-tier quota.

Encoding: L2-normalize the float32 values, then quantize to [-127, 127] int8.
          Same encoding is applied for both stored vectors and query vectors so
          cosine similarity is preserved.
"""

import asyncio
import logging
import math
import struct

from bson.binary import Binary
from google import genai
from google.genai import types

from config import get_settings

logger = logging.getLogger(__name__)

_client: genai.Client | None = None

EMBED_MODEL = "gemini-embedding-001"
# BSON Binary subtype 9 = Atlas int8 vector format
_BSON_VEC_SUBTYPE = 9


def quantize_int8(values) -> list[int]:
    """L2-normalize float values then quantize to signed int8 [-127, 127].

    gemini-embedding-001 does NOT auto-normalize, so we must normalize first
    to keep cosine similarity consistent between stored and query vectors.
    """
    norm = math.sqrt(sum(v * v for v in values)) or 1.0
    return [max(-128, min(127, round((v / norm) * 127))) for v in values]


def to_int8_binary(int8_vals: list[int]) -> Binary:
    """Pack int8 values into a BSON Binary subtype 9 (Atlas int8 vector format).

    Wire format: [dtype=0x03 (int8), padding=0x00, <768 signed bytes little-endian>]
    Works with pymongo 4.6.1 — no Binary.from_vector needed.
    """
    header = bytes([0x03, 0x00])
    body = struct.pack(f"<{len(int8_vals)}b", *int8_vals)
    return Binary(header + body, _BSON_VEC_SUBTYPE)



EMBED_DIMENSIONS = 768
BATCH_SIZE = 100
MAX_CONCURRENT_BATCHES = 5
MAX_RETRIES = 3


def _get_client() -> genai.Client:
    """Return the module-level Gemini client, initializing it lazily on first call."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = genai.Client(api_key=settings.gemini_api_key)
    return _client


def build_prospect_title_text(prospect: dict) -> str:
    """
    Build a plain-text string representing a prospect's professional identity.

    Concatenates job_title, headline, seniority, and function (canonical tokens)
    from the prospect document, joining non-empty values with " | ".

    Args:
        prospect: A prospect document dict (may include any subset of the fields).

    Returns:
        A pipe-separated string of non-empty identity tokens, or "" if all are absent.
    """
    fields = [
        prospect.get("job_title"),
        prospect.get("headline"),
        prospect.get("seniority"),
        prospect.get("function"),
    ]
    tokens = [str(f).strip() for f in fields if f is not None and str(f).strip()]
    return " | ".join(tokens)


def build_company_profile_text(company: dict) -> str:
    """
    Build a plain-text string representing a company's profile.

    Concatenates name, description, specialties (joined by ", "), and industry label,
    joining non-empty values with " | ".

    The `specialties` field is expected to be a list[str].
    The `industry` field may be a dict with a "label" key or a plain string.

    Args:
        company: A company document dict.

    Returns:
        A pipe-separated string of non-empty profile tokens, or "" if all are absent.
    """
    name = company.get("name")

    description = company.get("description")

    specialties_raw = company.get("specialties")
    specialties: str | None = None
    if isinstance(specialties_raw, list) and specialties_raw:
        joined = ", ".join(s for s in specialties_raw if s and str(s).strip())
        specialties = joined if joined else None

    industry_raw = company.get("industry")
    industry: str | None = None
    if isinstance(industry_raw, dict):
        label = industry_raw.get("label")
        if label and str(label).strip():
            industry = str(label).strip()
    elif isinstance(industry_raw, str) and industry_raw.strip():
        industry = industry_raw.strip()

    fields = [name, description, specialties, industry]
    tokens = [str(f).strip() for f in fields if f is not None and str(f).strip()]
    return " | ".join(tokens)


async def embed_texts(
    texts: list[str],
    *,
    task_type: str = "RETRIEVAL_DOCUMENT",
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> list[Binary | None]:
    """
    Generate Gemini embeddings for a list of texts.

    Empty or None texts are short-circuited to None without making an API call.
    Non-empty texts are batched in groups of up to 100 and sent concurrently
    (bounded by a semaphore of 5 simultaneous requests). Each batch is retried
    up to 3 times with exponential backoff (2s, 4s, 8s) on failure.

    Results are re-assembled in original input order.

    Args:
        texts: Input strings. May include None or empty strings.
        task_type: Gemini embedding task type (e.g. "RETRIEVAL_DOCUMENT",
                   "RETRIEVAL_QUERY", "SEMANTIC_SIMILARITY").

    Returns:
        A list aligned 1:1 with `texts`. Each element is either a BSON Binary
        (subtype 9, int8 Atlas vector, 770 bytes) or None if the corresponding
        input was empty/failed.
    """
    client = _get_client()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

    # Separate empty slots from non-empty ones, preserving original indices.
    non_empty: list[tuple[int, str]] = []  # (original_index, text)
    for idx, text in enumerate(texts):
        if text and str(text).strip():
            non_empty.append((idx, text))

    # Pre-fill result list with None.
    results: list[Binary | None] = [None] * len(texts)

    if not non_empty:
        return results

    # Build batches of (original_indices, batch_texts).
    batches: list[tuple[list[int], list[str]]] = []
    for batch_start in range(0, len(non_empty), BATCH_SIZE):
        batch_items = non_empty[batch_start : batch_start + BATCH_SIZE]
        batch_indices = [item[0] for item in batch_items]
        batch_texts = [item[1] for item in batch_items]
        batches.append((batch_indices, batch_texts))

    async def _embed_batch(
        indices: list[int],
        batch_texts: list[str],
    ) -> None:
        """Embed a single batch, retrying on failure, and write results in place."""
        async with semaphore:
            last_exc: Exception | None = None
            for attempt in range(MAX_RETRIES):
                try:
                    import time as _time
                    _t0 = _time.monotonic()
                    response = await client.aio.models.embed_content(
                        model=EMBED_MODEL,
                        contents=batch_texts,
                        config=types.EmbedContentConfig(
                            task_type=task_type,
                            output_dimensionality=EMBED_DIMENSIONS,
                        ),
                    )
                    embeddings = response.embeddings
                    for i, embedding in enumerate(embeddings):
                        results[indices[i]] = to_int8_binary(quantize_int8(embedding.values))
                    # Track embedding cost (normally untracked)
                    try:
                        from services.openrouter_service import _record_openrouter_usage
                        _usage_meta = getattr(response, "usage_metadata", None)
                        _prompt_tok = getattr(_usage_meta, "prompt_token_count", 0) or 0
                        if not _prompt_tok:
                            # Fallback: estimate tokens from text length (~4 chars/token)
                            _prompt_tok = max(1, sum(len(t) for t in batch_texts) // 4)
                        _dur_ms = int((_time.monotonic() - _t0) * 1000)
                        asyncio.create_task(_record_openrouter_usage(
                            model=EMBED_MODEL,
                            prompt_tokens=_prompt_tok,
                            completion_tokens=0,
                            duration_ms=_dur_ms,
                            account_id=account_id,
                            campaign_id=campaign_id,
                            feature="embedding",
                        ))
                    except Exception:
                        pass
                    return
                except Exception as exc:
                    last_exc = exc
                    wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                    logger.warning(
                        "embed_texts batch failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1,
                        MAX_RETRIES,
                        wait,
                        exc,
                    )
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(wait)

            logger.warning(
                "embed_texts batch permanently failed after %d attempts: %s",
                MAX_RETRIES,
                last_exc,
            )
            # Leave the slots as None (already initialized).

    await asyncio.gather(*[_embed_batch(idx_list, txt_list) for idx_list, txt_list in batches])

    return results


async def embed_one(
    text: str,
    *,
    task_type: str = "RETRIEVAL_DOCUMENT",
    account_id: str | None = None,
    campaign_id: str | None = None,
) -> Binary | None:
    """
    Generate a single Gemini embedding for one text string.

    A convenience wrapper around embed_texts for single-item use.

    Args:
        text: The input string to embed. Returns None if empty.
        task_type: Gemini embedding task type.

    Returns:
        A BSON Binary (subtype 9, int8 Atlas vector, 770 bytes),
        or None if text is empty or the call failed.
    """
    result = await embed_texts([text], task_type=task_type, account_id=account_id, campaign_id=campaign_id)
    return result[0]
