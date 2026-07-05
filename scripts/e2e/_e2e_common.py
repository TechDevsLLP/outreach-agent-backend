"""
Shared helpers for OutFlo API-level E2E harnesses.

Usage pattern:
    import asyncio
    from e2e._e2e_common import E2EClient, E2E_EMAIL, E2E_PASSWORD, poll

    async def main():
        async with E2EClient() as c:
            ...

    asyncio.run(main())

Requires the backend running locally:
    cd backend && uvicorn main:app --port 8008

Set env vars to override:
    OUTFLO_E2E_BASE_URL=http://localhost:8008
    OUTFLO_E2E_MOCK=1            # enable mock mode in discovery
"""

import asyncio
import logging
import os
import sys
from typing import Any, Callable

import httpx

logger = logging.getLogger("e2e")

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = os.environ.get("OUTFLO_E2E_BASE_URL", "http://localhost:8008")
MOCK_MODE = os.environ.get("OUTFLO_E2E_MOCK", "1") == "1"

# A throwaway test account — the factory-reset script also targets this.
E2E_EMAIL = "e2e-test@outflo.example.com"
E2E_PASSWORD = "E2ETest2025!"
E2E_NAME = "E2E Tester"
E2E_COMPANY = "E2ETestCo"

POLL_INTERVAL = 3  # seconds between polls
POLL_TIMEOUT = 300  # max seconds to wait for async pipeline


class E2EClient:
    """Thin async wrapper around httpx.AsyncClient with auth + helpers."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(base_url=BASE_URL, timeout=30.0)
        self._token: str | None = None

    async def __aenter__(self) -> "E2EClient":
        await self._check_server()
        await self._ensure_account()
        return self

    async def __aexit__(self, *_) -> None:
        await self._client.aclose()

    # ── Server health ──────────────────────────────────────────────────────────
    async def _check_server(self) -> None:
        try:
            r = await self._client.get("/health")
            r.raise_for_status()
            logger.info("Server healthy: %s", BASE_URL)
        except Exception as e:
            raise RuntimeError(
                f"Backend not reachable at {BASE_URL}. Start it with:\n"
                f"  cd backend && uvicorn main:app --port 8008\n"
                f"Original error: {e}"
            )

    # ── Auth ───────────────────────────────────────────────────────────────────
    async def _ensure_account(self) -> None:
        """Register the test account (idempotent) then log in."""
        # Try to register — 400/409/422 means it already exists, which is fine.
        await self._client.post(
            "/api/auth/register",
            json={"email": E2E_EMAIL, "password": E2E_PASSWORD, "name": E2E_NAME, "company_name": E2E_COMPANY},
        )
        r = await self._client.post(
            "/api/auth/login",
            json={"email": E2E_EMAIL, "password": E2E_PASSWORD},
        )
        r.raise_for_status()
        data = r.json()
        self._token = data.get("access_token") or data.get("token")
        if not self._token:
            raise RuntimeError(f"Login response has no token: {data}")
        logger.info("Logged in as %s", E2E_EMAIL)

    @property
    def headers(self) -> dict[str, str]:
        if not self._token:
            raise RuntimeError("Not authenticated yet")
        return {"Authorization": f"Bearer {self._token}"}

    # ── HTTP helpers ───────────────────────────────────────────────────────────
    async def get(self, path: str, **kwargs) -> Any:
        r = await self._client.get(path, headers=self.headers, **kwargs)
        r.raise_for_status()
        return r.json()

    async def post(self, path: str, json: dict | None = None, **kwargs) -> Any:
        r = await self._client.post(path, headers=self.headers, json=json, **kwargs)
        r.raise_for_status()
        return r.json()

    async def put(self, path: str, json: dict | None = None, **kwargs) -> Any:
        r = await self._client.put(path, headers=self.headers, json=json, **kwargs)
        r.raise_for_status()
        return r.json()

    async def post_expect_error(self, expected_status: int, path: str, json: dict | None = None) -> Any:
        r = await self._client.post(path, headers=self.headers, json=json)
        assert r.status_code == expected_status, (
            f"Expected HTTP {expected_status} from POST {path} but got {r.status_code}: {r.text}"
        )
        return r.json()


# ── Polling helper ────────────────────────────────────────────────────────────

async def poll(
    fn: Callable,
    done: Callable[[Any], bool],
    label: str = "condition",
    interval: int = POLL_INTERVAL,
    timeout: int = POLL_TIMEOUT,
) -> Any:
    """Poll fn() every `interval` seconds until done(result) is True or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await fn()
        logger.info("[poll:%s] %s", label, result)
        if done(result):
            return result
        await asyncio.sleep(interval)
    raise TimeoutError(f"poll:{label} did not complete within {timeout}s")


# ── Assertion helpers ─────────────────────────────────────────────────────────

def assert_eq(label: str, actual: Any, expected: Any) -> None:
    assert actual == expected, f"ASSERT FAIL [{label}]: expected {expected!r}, got {actual!r}"


def assert_truthy(label: str, value: Any) -> None:
    assert value, f"ASSERT FAIL [{label}]: expected truthy, got {value!r}"


def assert_gt(label: str, value: Any, threshold: Any) -> None:
    assert value > threshold, f"ASSERT FAIL [{label}]: expected > {threshold}, got {value!r}"


def log_pass(label: str) -> None:
    logger.info("  ✓ PASS  %s", label)


def log_fail(label: str, err: Exception) -> None:
    logger.error("  ✗ FAIL  %s — %s", label, err)
