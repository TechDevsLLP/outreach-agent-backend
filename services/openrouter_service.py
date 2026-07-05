"""
OpenRouter AI API client using httpx.
Provides chat completion with retry logic, model fallback, and JSON mode support.
"""

import httpx
import asyncio
import json
import logging
import random
import time
from collections import deque
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds
MAX_BACKOFF = 30  # cap exponential backoff
RETRY_AFTER_CAP = 60  # upper bound on Retry-After to avoid pathological sleeps

# Free model pool — used ONLY as last-resort fallback
FREE_MODELS = [
    "nvidia/nemotron-3-nano-30b-a3b:free",
    "nvidia/nemotron-3-super-120b-a12b:free",
    "z-ai/glm-4.5-air:free",
]
FREE_FALLBACK_MODEL = FREE_MODELS[0]

# Per-model rate limiting. Free models share very tight per-minute budgets on
# OpenRouter, so we cap them to ~1 req/sec each; paid models get a looser cap.
# Values are (max_calls, window_seconds) — if `max_calls` requests have
# happened within `window_seconds`, further callers wait until one ages out.
_FREE_RATE = (60, 60)     # ~1 req/sec per free model
_PAID_RATE = (300, 60)    # ~5 req/sec per paid model


class _TokenBucket:
    """Lightweight sliding-window rate limiter for a single model."""

    def __init__(self, max_calls: int, window_seconds: float):
        self.max_calls = max_calls
        self.window = window_seconds
        self._timestamps: deque = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                # Drop timestamps outside the window
                while self._timestamps and (now - self._timestamps[0]) >= self.window:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return
                # Compute how long until the oldest entry ages out
                wait = self.window - (now - self._timestamps[0]) + random.uniform(0, 0.1)
            # Sleep outside the lock so others can make progress on different
            # buckets (we never block cross-model).
            await asyncio.sleep(max(wait, 0.05))


_buckets: dict[str, _TokenBucket] = {}
_buckets_lock = asyncio.Lock()


async def _get_bucket(model: str) -> _TokenBucket:
    """Lazily create/fetch the per-model bucket."""
    async with _buckets_lock:
        bucket = _buckets.get(model)
        if bucket is None:
            is_free = model.endswith(":free") or model in FREE_MODELS
            max_calls, window = _FREE_RATE if is_free else _PAID_RATE
            bucket = _TokenBucket(max_calls, window)
            _buckets[model] = bucket
        return bucket


def _parse_retry_after(header_value: str | None) -> float | None:
    """Parse the Retry-After header (seconds or HTTP-date). Returns seconds, capped."""
    if not header_value:
        return None
    try:
        secs = float(header_value)
        return max(0.0, min(secs, RETRY_AFTER_CAP))
    except (ValueError, TypeError):
        pass
    # HTTP-date form — rare on OpenRouter, but handle it
    try:
        from email.utils import parsedate_to_datetime
        from datetime import datetime, timezone
        dt = parsedate_to_datetime(header_value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, min(delta, RETRY_AFTER_CAP))
    except Exception:
        return None


def _backoff_with_jitter(attempt: int) -> float:
    """Exponential backoff with full jitter, capped at MAX_BACKOFF."""
    base = min(RETRY_BACKOFF_BASE ** (attempt + 1), MAX_BACKOFF)
    return base + random.uniform(0, 1.0)


def get_free_model(index: int) -> str:
    """Round-robin free model selection. Kept for backward-compat; prefer settings.assessment_model."""
    return FREE_MODELS[index % len(FREE_MODELS)]


def extract_json(content: str) -> dict | list | str:
    """Parse JSON from content, stripping markdown code fences if present."""
    text = content.strip()

    # Strip markdown code fences: ```json ... ``` or ``` ... ```
    if text.startswith("```"):
        first_newline = text.index("\n") if "\n" in text else 3
        text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Model may return JSON followed by extra text — use raw_decode to grab
    # the first complete JSON value and ignore trailing content.
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch in ('{', '['):
            try:
                obj, _ = decoder.raw_decode(text, i)
                return obj
            except json.JSONDecodeError:
                continue

    raise json.JSONDecodeError("No JSON object found", text, 0)


def _is_refusal(text: str) -> bool:
    """Return True if the model returned a refusal instead of useful content."""
    if not text or len(text.strip()) < 15:
        return True
    lowered = text.strip().lower()
    REFUSAL_PREFIXES = (
        "i can't", "i cannot", "i'm sorry", "i am sorry",
        "as an ai", "as a language model", "i'm unable", "i am unable",
        "unfortunately, i", "i'm not able",
    )
    return any(lowered.startswith(p) for p in REFUSAL_PREFIXES)


async def _record_openrouter_usage(
    *,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    duration_ms: int,
    status: str = "succeeded",
    account_id: str | None = None,
    campaign_id: str | None = None,
    prospect_id: str | None = None,
    feature: str = "other",
):
    try:
        import database
        from config import get_settings as _get_settings
        _settings = _get_settings()
        if not getattr(_settings, "cost_tracking_enabled", True):
            return
        total_tokens = prompt_tokens + completion_tokens
        price_map = getattr(_settings, "openrouter_price_map", {}) or {}
        prices = price_map.get(model, {})
        cost_usd = (
            (prompt_tokens / 1_000_000) * prices.get("prompt", 0)
            + (completion_tokens / 1_000_000) * prices.get("completion", 0)
        )
        from datetime import datetime, timezone
        doc = {
            "account_id": account_id,
            "campaign_id": campaign_id,
            "prospect_id": prospect_id,
            "feature": feature,
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_usd": cost_usd,
            "duration_ms": duration_ms,
            "status": status,
            "requested_at": datetime.now(timezone.utc),
        }
        await database.openrouter_usage_collection.insert_one(doc)
    except Exception:
        pass


class OpenRouterClient:
    def __init__(self):
        self._client = httpx.AsyncClient(
            base_url=settings.openrouter_base_url,
            headers={
                "Authorization": f"Bearer {settings.openrouter_api_key}",
                "Content-Type": "application/json",
            },
            timeout=120.0,
        )

    async def _one_call(
        self,
        model: str,
        messages: list[dict],
        temperature: float,
        max_tokens: int,
        response_format: dict | None,
    ) -> dict:
        """Execute a single API call to one model. Raises on HTTP/parse errors."""
        # Per-model rate limiter (sliding window) — prevents bursty 429s when
        # many tasks fan out to the same free model simultaneously.
        bucket = await _get_bucket(model)
        await bucket.acquire()

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format

        response = await self._client.post("/chat/completions", json=payload)
        response.raise_for_status()

        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise ValueError(f"No choices in response from {model}: {str(data)[:300]}")

        message = choices[0].get("message") or {}
        content = message.get("content")

        # Some reasoning models (nemotron, glm-thinking, deepseek-r1) put output
        # in `message.reasoning` when content is null/empty.
        if not content or not str(content).strip():
            reasoning = message.get("reasoning") or ""
            if reasoning and str(reasoning).strip():
                content = reasoning
                logger.debug(f"[{model}] Used message.reasoning as content fallback")
            else:
                raise ValueError(f"Empty content and reasoning from {model}: {str(data)[:300]}")

        content = str(content).strip()

        if _is_refusal(content):
            raise ValueError(f"Model {model} returned a refusal: {content[:150]!r}")

        usage = data.get("usage") or {}

        # Parse JSON when response_format is json_object
        if response_format and response_format.get("type") == "json_object":
            try:
                parsed = extract_json(content)
            except json.JSONDecodeError as e:
                snippet = content[:400]
                raise json.JSONDecodeError(
                    f"[{model}] JSON parse failed. Raw content: {snippet!r}",
                    e.doc, e.pos
                ) from e
            if not isinstance(parsed, dict):
                raise ValueError(f"[{model}] Expected JSON object, got {type(parsed).__name__}")
            parsed["_usage"] = usage
            return parsed

        return {"content": content, "_usage": usage}

    async def chat_completion(
        self,
        messages: list[dict],
        model: str | None = None,
        models: list[str] | None = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        response_format: dict | None = None,
        max_retries: int | None = None,
        fallback_models: list[str] | None = None,
        account_id: str | None = None,
        campaign_id: str | None = None,
        prospect_id: str | None = None,
        feature: str | None = None,
    ) -> dict:
        """
        Send a chat completion request to OpenRouter with model fallback.

        On parse failures or refusals, escalates through the fallback chain rather
        than hard-failing. HTTP 429/5xx are retried with backoff within each model.

        max_retries: override MAX_RETRIES for this call (useful for high-throughput paths).
        fallback_models: explicit fallback chain; when provided, the default
            assessment_model/fallback_free_model appending is skipped.
        """
        _start_time = time.time()
        # Build candidate list: explicit > single model > default assessment model
        if models:
            candidates = list(models)
        elif model:
            candidates = [model]
        else:
            candidates = [settings.assessment_model]

        # Append fallbacks — use caller-provided list or default chain
        if fallback_models is not None:
            for fb in fallback_models:
                if fb not in candidates:
                    candidates.append(fb)
        else:
            for fallback in [settings.assessment_model, settings.fallback_free_model]:
                if fallback not in candidates:
                    candidates.append(fallback)

        retries = max_retries if max_retries is not None else MAX_RETRIES
        last_exc: Exception | None = None

        for candidate in candidates:
            for attempt in range(retries):
                try:
                    result = await self._one_call(
                        candidate, messages, temperature, max_tokens, response_format
                    )
                    if attempt > 0 or candidate != candidates[0]:
                        logger.info(f"OpenRouter succeeded on model={candidate} attempt={attempt+1}")
                    _usage = result.get("_usage") or {}
                    asyncio.create_task(_record_openrouter_usage(
                        model=candidate,
                        prompt_tokens=_usage.get("prompt_tokens", 0) or 0,
                        completion_tokens=_usage.get("completion_tokens", 0) or 0,
                        duration_ms=int((time.time() - _start_time) * 1000),
                        account_id=account_id,
                        campaign_id=campaign_id,
                        prospect_id=prospect_id,
                        feature=feature or "other",
                    ))
                    return result

                except httpx.HTTPStatusError as e:
                    last_exc = e
                    status_code = e.response.status_code
                    if status_code in (401, 402, 403, 404):
                        # Auth/credit/not-found errors — skip to next model immediately.
                        # Capture the response body so account-level issues (OpenRouter
                        # privacy policy, provider allowlist, etc.) are diagnosable from
                        # logs without having to reproduce the call.
                        body_snippet = (e.response.text or "")[:400].replace("\n", " ")
                        logger.warning(
                            f"[{candidate}] HTTP {status_code}, skipping to next model. Body: {body_snippet!r}"
                        )
                        break
                    if attempt < retries - 1:
                        # On 429, honour the Retry-After header if present; otherwise
                        # fall back to jittered exponential backoff.
                        retry_after = (
                            _parse_retry_after(e.response.headers.get("Retry-After"))
                            if status_code == 429
                            else None
                        )
                        wait = retry_after if retry_after is not None else _backoff_with_jitter(attempt)
                        logger.warning(
                            f"[{candidate}] HTTP {status_code} attempt {attempt+1}, "
                            f"retry in {wait:.1f}s"
                            + (" (Retry-After)" if retry_after is not None else "")
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(f"[{candidate}] HTTP {status_code} exhausted retries, trying next model")
                        break

                except (json.JSONDecodeError, ValueError) as e:
                    last_exc = e
                    logger.warning(f"[{candidate}] parse/content error attempt {attempt+1}: {str(e)[:200]}")
                    # Parse failures are model-specific — skip to next model immediately
                    break

                except (httpx.RequestError, httpx.TimeoutException) as e:
                    last_exc = e
                    if attempt < retries - 1:
                        wait = _backoff_with_jitter(attempt)
                        logger.warning(
                            f"[{candidate}] network error attempt {attempt+1}, retry in {wait:.1f}s: {e}"
                        )
                        await asyncio.sleep(wait)
                    else:
                        logger.warning(f"[{candidate}] network errors exhausted, trying next model")
                        break

        raise RuntimeError(
            f"OpenRouter failed on all models {candidates}: {last_exc}"
        )

    async def close(self):
        """Close the underlying HTTP client."""
        await self._client.aclose()
