"""
Apify shared infrastructure.

The Apollo-style prospect actors (LEADS_FINDER, Lead Scraper) and their
normalizers were removed — discovery is DB-first (shared pool) + employee
scraping. This module now only provides the shared Apify client and the
usage-tracking context manager used by the remaining actors:
- Employee scraper (services/employee_scraper_service.py)
- Company-details scraper (services/company_scraper_service.py)
- Post scraper (services/linkedin_post_scraper_service.py)
"""

import asyncio
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apify_client import ApifyClient
from config import get_settings
import logging

from utils.rate_limiter import backoff_with_jitter

logger = logging.getLogger(__name__)

settings = get_settings()


class _LazyApifyClient:
    """Construct the provider SDK only when an Apify operation is executed.

    Importing application modules must remain side-effect free.  The SDK's
    HTTP transport performs platform initialization during construction,
    which previously made offline unit-test collection and some management
    commands fail before they could replace the provider with a fake.
    """

    def __init__(self) -> None:
        self._client: ApifyClient | None = None
        self._lock = threading.Lock()

    def _get_client(self) -> ApifyClient:
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = ApifyClient(settings.apify_api_key)
        return self._client

    def __getattr__(self, name: str):
        return getattr(self._get_client(), name)


apify_client = _LazyApifyClient()

try:
    from apify_client.errors import ApifyApiError
except ImportError:  # pragma: no cover — depends on installed apify_client version
    ApifyApiError = None

# Statuses that mean the run itself failed transiently (worth a retry) as
# opposed to succeeding or being actively in-flight.
_TRANSIENT_RUN_STATUSES = {"FAILED", "ABORTED", "TIMED-OUT"}

# Global cap on concurrent Apify actor runs across the whole process, so the
# `apify_actor_concurrency_limit` setting is actually enforced no matter how
# many call sites fire actors at once (previously unenforced — see
# SMART_CAMPAIGN_OVERHAUL_PLAN.md). Created lazily so tests can monkeypatch
# `settings` before first use.
_actor_semaphore: asyncio.Semaphore | None = None
_actor_semaphore_lock = asyncio.Lock()


async def _get_actor_semaphore() -> asyncio.Semaphore:
    global _actor_semaphore
    if _actor_semaphore is None:
        async with _actor_semaphore_lock:
            if _actor_semaphore is None:
                _actor_semaphore = asyncio.Semaphore(settings.apify_actor_concurrency_limit)
    return _actor_semaphore


def _is_invalid_input_error(exc: Exception) -> bool:
    """True when the actor rejected the run input itself — retrying an
    identical call would just fail again the same way."""
    if ApifyApiError is not None and isinstance(exc, ApifyApiError):
        status_code = getattr(exc, "status_code", None)
        err_type = str(getattr(exc, "type", "") or "").lower()
        if status_code == 400 or "invalid-input" in err_type or "invalid_input" in err_type:
            return True
    return False


async def call_actor_with_retry(
    actor_id: str,
    run_input: dict,
    *,
    timeout_secs: int | None = None,
    max_retries: int = 1,
) -> dict:
    """Run an Apify actor via the shared client, bounded by the global actor
    concurrency semaphore (settings.apify_actor_concurrency_limit) and with
    a retry on transient failure.

    Retries (up to `max_retries` times, default 1 — i.e. one retry after the
    first attempt) on:
      - a network/transport error raised by the call itself, or
      - the run finishing with status FAILED/ABORTED/TIMED-OUT.

    Does NOT retry when the actor rejected the input (HTTP 400 / an
    ApifyApiError of type invalid-input) — an identical retry would fail
    identically and just burn quota.

    Returns the raw run dict from apify_client (contains "id",
    "defaultDatasetId", "status", ...). Callers read the dataset via
    `apify_client.dataset(run["defaultDatasetId"]).iterate_items()` as before.
    """
    semaphore = await _get_actor_semaphore()
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        async with semaphore:
            try:
                call_kwargs: dict = {"run_input": run_input}
                if timeout_secs is not None:
                    call_kwargs["timeout_secs"] = timeout_secs
                run = await asyncio.to_thread(
                    lambda: apify_client.actor(actor_id).call(**call_kwargs)
                )
            except Exception as e:
                if _is_invalid_input_error(e):
                    logger.warning(f"[apify] actor {actor_id} rejected input, not retrying: {e}")
                    raise
                last_exc = e
                if attempt < max_retries:
                    wait = backoff_with_jitter(attempt)
                    logger.warning(
                        f"[apify] actor {actor_id} call failed (attempt {attempt + 1}/{max_retries + 1}), "
                        f"retry in {wait:.1f}s: {e}"
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.error(f"[apify] actor {actor_id} call failed, retries exhausted: {e}")
                raise

        status = str((run or {}).get("status") or "").upper()
        if status in _TRANSIENT_RUN_STATUSES:
            if attempt < max_retries:
                wait = backoff_with_jitter(attempt)
                logger.warning(
                    f"[apify] actor {actor_id} run {run.get('id')} ended status={status} "
                    f"(attempt {attempt + 1}/{max_retries + 1}), retry in {wait:.1f}s"
                )
                await asyncio.sleep(wait)
                continue
            logger.error(
                f"[apify] actor {actor_id} run {run.get('id')} ended status={status}, retries exhausted"
            )
            return run  # let the caller inspect status/handle downstream

        return run

    # Unreachable in practice (every loop iteration either returns or raises),
    # but keeps this defensive against future edits to the loop above.
    raise last_exc or RuntimeError(f"Apify actor {actor_id} call failed with no result")


class _ApifyRunTracker:
    """Mutable state object yielded by track_apify_run."""

    def __init__(self, actor_id: str, account_id: str | None, campaign_id: str | None) -> None:
        self.actor_id = actor_id
        self.account_id = account_id
        self.campaign_id = campaign_id
        self.run_id: str = ""
        self.items_count: int = 0

    def set_run_id(self, run_id: str) -> None:
        self.run_id = run_id

    def set_items(self, count: int) -> None:
        self.items_count = count


@asynccontextmanager
async def track_apify_run(
    actor_id: str,
    *,
    account_id: str | None = None,
    campaign_id: str | None = None,
    run_input: dict | None = None,
):
    """Context manager that records Apify run cost to apify_usage_collection.

    Usage:
        async with track_apify_run(actor_id, account_id=..., campaign_id=...) as tracker:
            result = client.actor(actor_id).call(run_input=...)
            tracker.set_run_id(result["id"])
            tracker.set_items(count)
    """
    tracker = _ApifyRunTracker(actor_id, account_id, campaign_id)
    started_at = datetime.now(timezone.utc)
    try:
        yield tracker
    finally:
        try:
            finished_at = datetime.now(timezone.utc)
            cost_usd = 0.0
            status = "succeeded"
            if tracker.run_id:
                try:
                    run_details = await asyncio.to_thread(
                        lambda: apify_client.run(tracker.run_id).get()
                    )
                    if run_details:
                        cost_usd = (run_details.get("usageTotalUsd") or 0.0)
                        status = (run_details.get("status") or "succeeded").lower()
                except Exception:
                    pass

            from database import apify_usage_collection
            doc = {
                "account_id": tracker.account_id,
                "campaign_id": tracker.campaign_id,
                "actor_id": tracker.actor_id,
                "run_id": tracker.run_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "items_returned": tracker.items_count,
                "cost_usd": cost_usd,
                "status": status,
                "metadata": {},
            }
            await apify_usage_collection.insert_one(doc)
        except Exception:
            pass
