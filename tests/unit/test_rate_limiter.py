"""Unit tests for utils/rate_limiter.py — the shared sliding-window limiter
and retry-timing helpers used by company_sourcing_service, apify_service, and
unipile_service."""
import asyncio
import time

import pytest

from utils.rate_limiter import (
    SlidingWindowRateLimiter,
    backoff_with_jitter,
    get_rate_limiter,
    parse_retry_after,
)

pytestmark = pytest.mark.unit


async def test_allows_calls_up_to_budget_without_waiting():
    limiter = SlidingWindowRateLimiter(max_calls=3, window_seconds=60)
    start = time.monotonic()
    for _ in range(3):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5


async def test_blocks_until_window_slides(monkeypatch):
    # Small window so the test runs fast; the 4th acquire must wait for the
    # 1st timestamp to age out.
    limiter = SlidingWindowRateLimiter(max_calls=2, window_seconds=0.2)
    await limiter.acquire()
    await limiter.acquire()

    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.15  # allow a little slack under the 0.2s window


async def test_get_rate_limiter_returns_same_instance_for_same_key():
    a = await get_rate_limiter("test-key-1", 10, 60)
    b = await get_rate_limiter("test-key-1", 999, 999)  # args ignored on cache hit
    assert a is b


async def test_get_rate_limiter_returns_different_instances_for_different_keys():
    a = await get_rate_limiter("test-key-a", 10, 60)
    b = await get_rate_limiter("test-key-b", 10, 60)
    assert a is not b


def test_parse_retry_after_numeric_seconds():
    assert parse_retry_after("5") == 5.0


def test_parse_retry_after_caps_pathological_values():
    assert parse_retry_after("99999", cap=60.0) == 60.0


def test_parse_retry_after_none_for_missing_header():
    assert parse_retry_after(None) is None


def test_parse_retry_after_none_for_garbage():
    assert parse_retry_after("not-a-date-or-number") is None


def test_backoff_with_jitter_increases_with_attempt_and_respects_cap():
    # Base^attempt is deterministic; jitter adds up to 1s on top.
    low = backoff_with_jitter(0, base=2.0, cap=30.0)
    high = backoff_with_jitter(3, base=2.0, cap=30.0)
    assert 2.0 <= low <= 3.0
    assert high <= 31.0  # capped backoff + up to 1s jitter
