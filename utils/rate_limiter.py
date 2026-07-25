"""
Shared async rate-limiting / retry-backoff primitives.

`SlidingWindowRateLimiter` is lifted from the per-model bucket in
services/openrouter_service.py (`_TokenBucket`) so other outbound clients
(direct Gemini SDK calls, Apify, Unipile) can get the same behavior without
duplicating it. openrouter_service.py keeps its own private copy — it is not
refactored to import from here — but any *new* limiter should use this module.

Also provides `parse_retry_after` and `backoff_with_jitter`, the retry-timing
helpers shared by every client that needs to honor a `Retry-After`-style
header/hint and otherwise fall back to jittered exponential backoff.
"""

import asyncio
import random
import time
from collections import deque

# Defaults mirrored from openrouter_service.py — callers may override per use.
DEFAULT_BACKOFF_BASE = 2.0     # seconds
DEFAULT_MAX_BACKOFF = 30.0     # cap on exponential backoff (pre-jitter)
DEFAULT_RETRY_AFTER_CAP = 60.0  # upper bound on any parsed Retry-After value


class SlidingWindowRateLimiter:
    """Lightweight async sliding-window rate limiter.

    Allows up to `max_calls` acquisitions within any trailing `window_seconds`
    window. Callers that would exceed the budget await until the oldest
    timestamp ages out of the window, so throughput smooths out instead of
    bursting and immediately 429ing the upstream provider.
    """

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
            # Sleep outside the lock so other callers can still make progress
            # (this limiter instance is per-key, so there's no cross-key
            # blocking either way, but this keeps the lock hold time minimal).
            await asyncio.sleep(max(wait, 0.05))


_limiters: dict[str, SlidingWindowRateLimiter] = {}
_limiters_lock = asyncio.Lock()


async def get_rate_limiter(key: str, max_calls: int, window_seconds: float) -> SlidingWindowRateLimiter:
    """Lazily create/fetch a named rate limiter (e.g. per model, per actor, per API)."""
    async with _limiters_lock:
        limiter = _limiters.get(key)
        if limiter is None:
            limiter = SlidingWindowRateLimiter(max_calls, window_seconds)
            _limiters[key] = limiter
        return limiter


def parse_retry_after(header_value: str | None, cap: float = DEFAULT_RETRY_AFTER_CAP) -> float | None:
    """Parse a Retry-After style value (seconds or HTTP-date). Returns seconds, capped."""
    if not header_value:
        return None
    try:
        secs = float(header_value)
        return max(0.0, min(secs, cap))
    except (ValueError, TypeError):
        pass
    # HTTP-date form — rare, but handle it
    try:
        from email.utils import parsedate_to_datetime
        from datetime import datetime, timezone
        dt = parsedate_to_datetime(header_value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, min(delta, cap))
    except Exception:
        return None


def backoff_with_jitter(
    attempt: int,
    base: float = DEFAULT_BACKOFF_BASE,
    cap: float = DEFAULT_MAX_BACKOFF,
) -> float:
    """Exponential backoff with full jitter, capped at `cap`."""
    backoff = min(base ** (attempt + 1), cap)
    return backoff + random.uniform(0, 1.0)
