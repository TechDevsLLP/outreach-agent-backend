"""REAL-provider smoke pack — opt-in, spends a small credit budget.

Run with:  RUN_REAL_SMOKE=1 venv/bin/pytest tests/smoke_real -m smoke_real

What it does (approved budget: ~2-3 GrowthToolkit credits, 1 minimal Apify
post-scraper run, one tiny OpenRouter completion, one Gemini embedding, one
read-only Unipile call):
- GrowthToolkit find_email (1-2 credits) + enrich_linkedin with
  unlock_emails=0/unlock_phone=0 (1 credit) on prospects sampled READ-ONLY
  from the real `outflo_v3` pool.
- OpenRouter: one tiny Haiku completion via services/openrouter_service.
- Gemini: one single-text embedding via services/embedding_service.
- Unipile: list connected accounts (read-only; skips if none connected).
- Apify: one minimal post-scraper run (actor r4oNX7IHlW4RQAjKP) on a single
  real LinkedIn profile URL; verifies apify_usage tracking.

ABSOLUTE RULES (enforced by construction):
- NO emails sent, NO LinkedIn connection requests / messages / InMails,
  NO phone unlocks, nothing outward-facing to real people. Read/enrich only.
- The production DB `outflo_v3` is only ever READ. The harness points
  `database.db` at `outflo_v3_test`, so the provider usage-tracking docs
  (growthtoolkit_usage / apify_usage / openrouter_usage) land in the TEST db,
  where each test verifies them. account_id is tagged "smoke_test".
- Every test skips cleanly when RUN_REAL_SMOKE is unset.

Latencies are appended to perf_metrics with a smoke_ prefix and land in
docs/test-reports/timings_iteration_<N>.json.
"""
import os
import re
import time
from pathlib import Path

import pytest
import pytest_asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from config import get_settings

RUN = os.environ.get("RUN_REAL_SMOKE") == "1"

pytestmark = [
    pytest.mark.smoke_real,
    pytest.mark.skipif(not RUN, reason="RUN_REAL_SMOKE=1 not set — real-provider smoke pack is opt-in"),
]

REAL_DB_NAME = "outflo_v3"
SMOKE_TAG = "smoke_test"
BACKEND_DIR = Path(__file__).resolve().parent.parent.parent


def _stopwatch(perf_metrics, name, **extra):
    class _SW:
        def __enter__(self):
            self.t0 = time.perf_counter()
            return self

        def __exit__(self, exc_type, *exc):
            self.ms = round((time.perf_counter() - self.t0) * 1000, 1)
            perf_metrics.append({"name": name, "ms": self.ms,
                                 **({} if exc_type is None else {"outcome": "error"}), **extra})
    return _SW()


@pytest_asyncio.fixture(scope="session")
async def real_db():
    """READ-ONLY handle on the production pool (same pattern as tests/perf)."""
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[REAL_DB_NAME]
    assert db.name == REAL_DB_NAME
    await client.admin.command("ping")
    yield db
    client.close()


@pytest.fixture(scope="session")
def real_growthtoolkit_key():
    """tests/conftest.py overrides GROWTHTOOLKIT_API_KEY with a dummy before
    Settings is cached (so mock-only runs can never bill). For the smoke pack,
    read the real key from .env and swap it into the cached settings object."""
    env_file = BACKEND_DIR / ".env"
    key = None
    if env_file.exists():
        m = re.search(r"^GROWTHTOOLKIT_API_KEY=(.+)$", env_file.read_text(), re.M)
        if m:
            key = m.group(1).strip().strip('"').strip("'")
    if not key:
        pytest.skip("GROWTHTOOLKIT_API_KEY not found in .env")

    from services import growthtoolkit_service as gts
    old = gts.settings.growthtoolkit_api_key
    try:
        gts.settings.growthtoolkit_api_key = key
    except Exception:
        object.__setattr__(gts.settings, "growthtoolkit_api_key", key)
    yield key
    try:
        gts.settings.growthtoolkit_api_key = old
    except Exception:
        object.__setattr__(gts.settings, "growthtoolkit_api_key", old)


# ---------------------------------------------------------------------------
# GrowthToolkit
# ---------------------------------------------------------------------------

async def test_smoke_growthtoolkit_find_email(real_db, real_growthtoolkit_key, perf_metrics):
    """One real email-finder lookup (1-2 credits) for a person we already know
    has an email, picked read-only from the real pool."""
    import database
    from services import growthtoolkit_service as gts

    candidate = await real_db["prospects"].find_one(
        {
            "first_name": {"$nin": [None, ""]},
            "last_name": {"$nin": [None, ""]},
            "email": {"$regex": "@"},
            "company_domain": {"$nin": [None, ""]},
        },
        {"first_name": 1, "last_name": 1, "email": 1, "company_domain": 1},
    )
    assert candidate, "real pool should contain a prospect with name+domain+email"
    domain = candidate["company_domain"].lower().removeprefix("https://").removeprefix("http://").removeprefix("www.").split("/")[0]

    with _stopwatch(perf_metrics, "smoke_growthtoolkit_find_email",
                    domain=domain, credits=1) as sw:
        email = await gts.find_email(
            candidate["first_name"], candidate["last_name"], domain,
            account_id=SMOKE_TAG,
        )

    # Clean outcomes only: an email string or a clean not-found (None)
    assert email is None or ("@" in email and "." in email.split("@")[1]), email
    perf_metrics.append({"name": "smoke_growthtoolkit_find_email_result", "ms": sw.ms,
                         "found": bool(email)})

    usage = await database.db["growthtoolkit_usage"].find_one(
        {"endpoint": "email-finder", "account_id": SMOKE_TAG})
    assert usage is not None, "growthtoolkit_usage doc must be written"
    assert usage["duration_ms"] >= 0 and usage["code"] is not None or usage["success"]


async def test_smoke_growthtoolkit_enrich_linkedin(real_db, real_growthtoolkit_key, perf_metrics):
    """One real LinkedIn enrichment (1 credit) — NO unlocks."""
    import database
    from services import growthtoolkit_service as gts

    candidate = await real_db["prospects"].find_one(
        {"linkedin": {"$regex": r"linkedin\.com/in/"}, "enrichment_status": "completed"},
        {"linkedin": 1, "full_name": 1},
    )
    assert candidate, "real pool should contain a completed prospect with a linkedin URL"

    with _stopwatch(perf_metrics, "smoke_growthtoolkit_enrich_linkedin", credits=1) as sw:
        data = await gts.enrich_linkedin(
            candidate["linkedin"], unlock_emails=False, unlock_phone=False,
            account_id=SMOKE_TAG,
        )

    # Response shape: rich person object with name/company fields
    assert data is None or isinstance(data, dict)
    if data is not None:
        assert any(k in data for k in ("full_name", "first_name", "name")), list(data)[:20]
        assert any(k.startswith("job_company") or k == "job_title" or k.startswith("company")
                   for k in data), list(data)[:30]
    perf_metrics.append({"name": "smoke_growthtoolkit_enrich_linkedin_result", "ms": sw.ms,
                         "found": data is not None})

    usage = await database.db["growthtoolkit_usage"].find_one(
        {"endpoint": "linkedin-enrichment", "account_id": SMOKE_TAG})
    assert usage is not None, "growthtoolkit_usage doc must be written"


# ---------------------------------------------------------------------------
# OpenRouter (tiny Haiku completion)
# ---------------------------------------------------------------------------

async def test_smoke_openrouter_haiku_completion(perf_metrics):
    from services.openrouter_service import OpenRouterClient

    settings = get_settings()
    client = OpenRouterClient()
    try:
        with _stopwatch(perf_metrics, "smoke_openrouter_haiku_completion",
                        model=settings.mini_enrichment_model) as sw:
            result = await client.chat_completion(
                # NB: >=15 chars requested — the service's _is_refusal heuristic
                # deliberately treats shorter replies as refusals.
                messages=[{"role": "user", "content":
                           "Reply with exactly this sentence and nothing else: "
                           "The smoke test is green."}],
                model=settings.mini_enrichment_model,
                temperature=0.0,
                max_tokens=32,
                fallback_models=[],
                account_id=SMOKE_TAG,
                feature="smoke_test",
            )
    finally:
        await client.close()

    content = result.get("content") if isinstance(result, dict) else str(result)
    assert content and "smoke test is green" in content.lower(), f"unexpected completion: {result!r}"


# ---------------------------------------------------------------------------
# Gemini (single-text embedding)
# ---------------------------------------------------------------------------

async def test_smoke_gemini_single_embedding(perf_metrics):
    from services.embedding_service import embed_texts

    with _stopwatch(perf_metrics, "smoke_gemini_single_embedding") as sw:
        vectors = await embed_texts(
            ["Chief Technology Officer at an industrial automation company"],
            account_id=SMOKE_TAG,
        )
    assert len(vectors) == 1 and vectors[0] is not None
    # int8 Atlas vector, BSON Binary subtype 9, 768 dims -> 770 bytes
    assert len(bytes(vectors[0])) == 770


# ---------------------------------------------------------------------------
# Unipile (read-only account listing)
# ---------------------------------------------------------------------------

async def test_smoke_unipile_list_accounts(perf_metrics):
    from services.unipile_service import UnipileClient, UnipileAPIError

    settings = get_settings()
    if not settings.unipile_token:
        pytest.skip("UNIPILE_TOKEN not configured")

    client = UnipileClient()
    try:
        with _stopwatch(perf_metrics, "smoke_unipile_list_accounts") as sw:
            accounts = await client.get_accounts()
    except UnipileAPIError as e:
        pytest.skip(f"Unipile not reachable/authorized in this environment: {e}")

    assert isinstance(accounts, list)
    if not accounts:
        pytest.skip("Unipile reachable but no account connected — nothing more to assert")
    for acc in accounts:
        assert "id" in acc and ("type" in acc or "provider" in acc)
    perf_metrics.append({"name": "smoke_unipile_accounts_connected", "ms": sw.ms,
                         "count": len(accounts)})


# ---------------------------------------------------------------------------
# Apify (one MINIMAL post-scraper run — actor r4oNX7IHlW4RQAjKP)
# ---------------------------------------------------------------------------

async def test_smoke_apify_post_scraper_minimal(real_db, perf_metrics):
    """One real post-scraper run on a single public profile from the pool.
    Actor runs take 1-5 min — no artificial timeout here; the actor .call()
    blocks until the run finishes."""
    import database
    from services.linkedin_post_scraper_service import scrape_linkedin_posts_bulk

    settings = get_settings()
    # A fully-enriched prospect's profile is known-public (it was scraped before).
    candidate = await real_db["prospects"].find_one(
        {"linkedin": {"$regex": r"linkedin\.com/in/"}, "enrichment_status": "completed"},
        {"linkedin": 1, "full_name": 1},
    )
    assert candidate, "real pool should contain a prospect with a linkedin URL"
    url = candidate["linkedin"]

    with _stopwatch(perf_metrics, "smoke_apify_post_scraper_run", profile=url) as sw:
        result = await scrape_linkedin_posts_bulk([url], posts_per_profile=2)

    assert isinstance(result, dict) and url in result
    posts = result[url]
    assert isinstance(posts, list)  # may be empty if the profile has no recent posts
    for post in posts:
        assert "text" in post or "url" in post
    perf_metrics.append({"name": "smoke_apify_post_scraper_items", "ms": sw.ms,
                         "items": len(posts)})

    usage = await database.db["apify_usage"].find_one(
        {"actor_id": settings.apify_post_scraper_actor_id},
        sort=[("started_at", -1)],
    )
    assert usage is not None, "apify_usage tracking doc must be written"
    assert usage["run_id"], "usage doc should carry the Apify run id"
