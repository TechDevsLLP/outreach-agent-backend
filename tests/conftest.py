"""
OutFlo backend test harness.

CRITICAL ORDERING: environment overrides below MUST run before config/database/
main are imported anywhere, because config.get_settings() is lru_cached and
database.py builds the Motor client + db handle at import time.

- All app/API tests run against database `outflo_v3_test` (same Atlas cluster).
- The production database `outflo_v3` is NEVER written to by tests. The perf
  suite (tests/perf) opens its own client and only runs find/aggregate.
- External providers (Apify, GrowthToolkit, OpenRouter/Gemini, Unipile,
  Gmail/Zoho/SMTP email) are mocked at the service layer.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))

TEST_DB_NAME = "outflo_v3_test"
SUPERADMIN_EMAIL = "superadmin@test.outflo.local"
TEST_MONGODB_URL = os.environ.get(
    "TEST_MONGODB_URL", "mongodb://127.0.0.1:27017"
)

# ---------------------------------------------------------------------------
# Environment overrides — BEFORE any app import
# ---------------------------------------------------------------------------
os.environ["MONGODB_DATABASE"] = TEST_DB_NAME
os.environ["MONGODB_URL"] = TEST_MONGODB_URL
os.environ["SUPER_ADMIN_EMAIL"] = SUPERADMIN_EMAIL
os.environ["APP_ROLE"] = "web"            # never start APScheduler in tests
os.environ["APP_ENV"] = "test"
os.environ["ENRICHMENT_STARTUP_SWEEP_ENABLED"] = "false"
os.environ["GROWTHTOOLKIT_API_KEY"] = "test-growthtoolkit-key"
# Fernet key for credential encryption tests (utils/crypto.py) — fixed so
# encrypt/decrypt round-trips are stable across test runs. Not a real secret.
os.environ.setdefault(
    "ENCRYPTION_KEY", "-9NY1BUpR-W4QsHvm3CCtQ6CaNh_BLosLZHWcHq2wlw="
)

# ---------------------------------------------------------------------------
# Test-only bcrypt work factor. auth.hash_password calls bcrypt.gensalt()
# (default 12 rounds, ~250 ms per hash/verify) — the auth/api suites hash and
# verify dozens of passwords. 4 rounds (bcrypt's minimum) keeps the exact same
# code path and hash format while cutting ~2-3 s of suite wall time.
# checkpw reads rounds from the stored hash, so verification is unaffected.
# ---------------------------------------------------------------------------
import bcrypt as _bcrypt  # noqa: E402

_orig_gensalt = _bcrypt.gensalt


def _fast_gensalt(rounds: int = 4, prefix: bytes = b"2b"):
    return _orig_gensalt(rounds=4, prefix=prefix)


_bcrypt.gensalt = _fast_gensalt

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from bson import ObjectId  # noqa: E402

import config  # noqa: E402

settings = config.get_settings()
assert settings.mongodb_database == TEST_DB_NAME, (
    f"Settings picked up database {settings.mongodb_database!r} instead of the "
    f"test DB — aborting to protect production data."
)
assert settings.super_admin_email == SUPERADMIN_EMAIL

import database  # noqa: E402

assert database.db.name == TEST_DB_NAME and "_test" in database.db.name, (
    "database.db is not pointing at the test database — aborting."
)

from auth import create_access_token, hash_password  # noqa: E402


# ---------------------------------------------------------------------------
# Timing plugin — every test's name/outcome/duration ->
# timings_iteration_<N>.json (N from TEST_REPORT_ITERATION, default 2)
# ---------------------------------------------------------------------------
_TIMINGS: list[dict] = []
PERF_METRICS: list[dict] = []  # perf tests append {"name": ..., "ms": ...}
_REPORT_DIR = BACKEND_DIR / "docs" / "test-reports"
_REPORT_ITERATION = os.environ.get("TEST_REPORT_ITERATION", "3")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        # count setup errors / skips declared at setup time too
        if report.when == "setup" and (report.failed or report.skipped):
            _TIMINGS.append({
                "test": item.nodeid,
                "outcome": "error" if report.failed else "skipped",
                "phase": "setup",
                "duration_ms": round(report.duration * 1000, 2),
            })
        return
    _TIMINGS.append({
        "test": item.nodeid,
        "outcome": report.outcome,
        "duration_ms": round(report.duration * 1000, 2),
    })


def pytest_sessionfinish(session, exitstatus):
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "exit_status": int(exitstatus),
        "total_tests": len(_TIMINGS),
        "total_duration_ms": round(sum(t["duration_ms"] for t in _TIMINGS), 2),
        "tests": sorted(_TIMINGS, key=lambda t: -t["duration_ms"]),
        "perf_metrics": PERF_METRICS,
    }
    (_REPORT_DIR / f"timings_iteration_{_REPORT_ITERATION}.json").write_text(json.dumps(payload, indent=2))


def pytest_terminal_summary(terminalreporter):
    if _TIMINGS:
        slowest = sorted(_TIMINGS, key=lambda t: -t["duration_ms"])[:10]
        terminalreporter.write_sep("-", "slowest 10 tests (harness timing plugin)")
        for t in slowest:
            terminalreporter.write_line(f"{t['duration_ms']:>10.1f} ms  {t['test']}")
    if PERF_METRICS:
        terminalreporter.write_sep("-", "perf measurements (real outflo_v3, read-only)")
        for m in PERF_METRICS:
            terminalreporter.write_line(f"{m['ms']:>10.1f} ms  perf_{m['name']}")


# ---------------------------------------------------------------------------
# App + HTTP client
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def app():
    """Import the FastAPI app inside the session event loop so Motor binds
    its io_loop to the loop tests run on. Lifespan is intentionally NOT run
    (no scheduler / index creation / resume sweeps needed for tests)."""
    from main import app as fastapi_app
    # First DB op happens here, pinning Motor's cached io_loop to this loop.
    await database.client.admin.command("ping")
    yield fastapi_app


@pytest_asyncio.fixture(scope="session")
async def client(app, _test_db_lifecycle):
    import httpx
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


# ---------------------------------------------------------------------------
# Test-DB lifecycle: clean before the session, drop after
# ---------------------------------------------------------------------------

async def _drop_test_collections():
    assert "_test" in database.db.name, "refusing to drop a non-test database"
    for name in await database.db.list_collection_names():
        await database.db.drop_collection(name)


@pytest_asyncio.fixture(scope="session")
async def _test_db_lifecycle(app):
    """Own the Atlas-backed integration-test lifecycle.

    This fixture is intentionally opt-in through ``client`` and the seeded
    identity fixtures.  Making it autouse forced pure unit tests to import the
    full application and connect to Atlas before their test logic could run.
    """
    await _drop_test_collections()
    yield
    await _drop_test_collections()


# ---------------------------------------------------------------------------
# Seeded identities
# ---------------------------------------------------------------------------

def _mint_token(user_id: str, account_id: str, email: str) -> str:
    return create_access_token(
        data={"sub": user_id, "account_id": account_id, "email": email}
    )


async def _create_identity(email: str, name: str, account_name: str, account_status: str = "active") -> dict:
    now = datetime.utcnow()
    account = await database.accounts_collection.insert_one({
        "name": account_name,
        "slug": account_name.lower().replace(" ", "-") + "-" + str(ObjectId())[-4:],
        "plan": "free",
        "status": account_status,
        "trial_ends_at": None,
        "daily_email_quota": 50,
        "daily_linkedin_connection_quota": 20,
        "daily_linkedin_inmail_quota": 5,
        "enrichment_batch_limit": 50,
        "created_at": now,
        "updated_at": now,
    })
    user = await database.users_collection.insert_one({
        "email": email,
        "password_hash": hash_password("test-password-123"),
        "name": name,
        "plan": "free",
        "onboarding_complete": True,
        "is_active": True,
        "current_account_id": account.inserted_id,
        "created_at": now,
        "updated_at": now,
    })
    await database.account_members_collection.insert_one({
        "account_id": account.inserted_id,
        "user_id": user.inserted_id,
        "role": "owner",
        "joined_at": now,
    })
    uid, aid = str(user.inserted_id), str(account.inserted_id)
    return {
        "user_id": uid,
        "account_id": aid,
        "email": email,
        "token": _mint_token(uid, aid, email),
        "headers": {"Authorization": f"Bearer {_mint_token(uid, aid, email)}"},
    }


@pytest.fixture(scope="session")
def create_identity(_test_db_lifecycle):
    """Factory fixture: create a user + account + membership in the test DB."""
    return _create_identity


@pytest_asyncio.fixture(scope="session")
async def identity_a(_test_db_lifecycle):
    """Primary tenant (Account A)."""
    return await _create_identity("usera@test.outflo.local", "User A", "TestCo A")


@pytest_asyncio.fixture(scope="session")
async def identity_b(_test_db_lifecycle):
    """Second tenant (Account B) for isolation tests."""
    return await _create_identity("userb@test.outflo.local", "User B", "OtherCo B")


@pytest_asyncio.fixture(scope="session")
async def superadmin(_test_db_lifecycle):
    """User whose email matches SUPER_ADMIN_EMAIL (set before Settings cache)."""
    return await _create_identity(SUPERADMIN_EMAIL, "Super Admin", "Admin HQ")


@pytest_asyncio.fixture(scope="session")
async def auth_headers_a(identity_a):
    return identity_a["headers"]


@pytest_asyncio.fixture(scope="session")
async def auth_headers_b(identity_b):
    return identity_b["headers"]


@pytest_asyncio.fixture(scope="session")
async def superadmin_headers(superadmin):
    return superadmin["headers"]


# ---------------------------------------------------------------------------
# Seeded prospects (global pool + prospect_state overlay for account A / B)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def seeded_prospects(identity_a, identity_b):
    now = datetime.utcnow()
    docs = [
        {
            "full_name": "Alice Anderson", "first_name": "Alice", "last_name": "Anderson",
            "email": "alice@acme-a.test", "job_title": "VP of Marketing",
            "seniority": "vp", "company_name": "Acme Industrial",
            "linkedin": "https://www.linkedin.com/in/alice-anderson-test",
            "company_industry_id": "industrial_machinery",
            "location": {"country_code": "US", "country": "United States", "city": "Houston"},
            "enrichment_status": "completed", "stage": "enriched", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
        {
            "full_name": "Bob Brown", "first_name": "Bob", "last_name": "Brown",
            "email": None, "job_title": "Founder & CEO",
            "seniority": "founder", "company_name": "Brown Chemicals",
            "linkedin": "https://www.linkedin.com/in/bob-brown-test",
            "company_industry_id": "chemicals",
            "location": {"country_code": "DE", "country": "Germany", "city": "Frankfurt"},
            "enrichment_status": "not_started", "stage": "new", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
        {
            "full_name": "Carol NoLinked", "first_name": "Carol", "last_name": "NoLinked",
            "email": "carol@nolink.test", "job_title": "Director of Sales",
            "seniority": "director", "company_name": "NoLink GmbH",
            "linkedin": None,
            "company_industry_id": "software_development",
            "location": {"country_code": "GB", "country": "United Kingdom", "city": "London"},
            "enrichment_status": "completed", "stage": "enriched", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
        {
            # belongs to account B only
            "full_name": "Dave OtherTenant", "first_name": "Dave", "last_name": "OtherTenant",
            "email": "dave@other-b.test", "job_title": "COO",
            "seniority": "c_suite", "company_name": "OtherCo Ops",
            "linkedin": "https://www.linkedin.com/in/dave-other-test",
            "company_industry_id": "logistics",
            "location": {"country_code": "US", "country": "United States", "city": "Dallas"},
            "enrichment_status": "completed", "stage": "enriched", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
        {
            # phone unlock -> provider returns no phone
            "full_name": "Frank NoPhone", "first_name": "Frank", "last_name": "NoPhone",
            "email": "frank@nophone.test", "job_title": "Owner",
            "seniority": "owner", "company_name": "Frank & Sons",
            "linkedin": "https://www.linkedin.com/in/frank-nophone-test",
            "company_industry_id": "construction",
            "location": {"country_code": "US", "country": "United States", "city": "Phoenix"},
            "enrichment_status": "completed", "stage": "enriched", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
        {
            # already has a phone — cached unlock path
            "full_name": "Eve Cached", "first_name": "Eve", "last_name": "Cached",
            "email": "eve@cached.test", "job_title": "CTO",
            "seniority": "c_suite", "company_name": "Cached Systems",
            "linkedin": "https://www.linkedin.com/in/eve-cached-test",
            "mobile_number": "+1-555-0100",
            "phone_numbers": ["+1-555-0100"],
            "company_industry_id": "software_development",
            "location": {"country_code": "US", "country": "United States", "city": "Boston"},
            "enrichment_status": "completed", "stage": "enriched", "source": "test_seed",
            "created_at": now, "last_updated_at": now,
        },
    ]
    result = await database.prospects_collection.insert_many(docs)
    ids = [str(i) for i in result.inserted_ids]

    # prospect_state overlay: first three + eve -> account A; dave -> account B.
    # `pk` mirrors what production write sites denormalize
    # (utils/prospect_filter_keys.py) — list filters match on pk.*.
    from utils.prospect_filter_keys import build_filter_keys

    def _state(account_key, idx, **kw):
        return {"account_id": account_key, "prospect_id": ids[idx],
                "pk": build_filter_keys(docs[idx]),
                "used_by": [], "tags": [], "created_at": now, **kw}

    a_states = [
        _state(identity_a["account_id"], 0, status="new", ai_score=85.0, priority_tier="hot"),
        _state(identity_a["account_id"], 1, status="new", ai_score=65.0, priority_tier="warm"),
        _state(identity_a["account_id"], 2, status="contacted", ai_score=45.0, priority_tier="cold"),
        _state(identity_a["account_id"], 4, status="new", ai_score=55.0, priority_tier="cold"),
        _state(identity_a["account_id"], 5, status="new", ai_score=75.0, priority_tier="warm"),
        _state(identity_b["account_id"], 3, status="new", ai_score=70.0, priority_tier="warm"),
    ]
    await database.prospect_state_collection.insert_many(a_states)

    return {
        "ids": ids,
        "alice": ids[0], "bob": ids[1], "carol": ids[2],
        "dave_b": ids[3], "frank_nophone": ids[4], "eve_cached": ids[5],
    }


# ---------------------------------------------------------------------------
# External-provider guardrails
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _no_paid_providers(request, monkeypatch):
    """Fail fast if any test accidentally reaches a paid external provider.

    growthtoolkit unit tests and specific mock fixtures override these
    monkeypatches with their own canned fakes.
    """
    if "perf" in request.keywords or "smoke_real" in request.keywords:
        return

    def _blocked(name):
        async def _fn(*args, **kwargs):
            raise AssertionError(f"Blocked external provider call in test: {name}")
        return _fn

    try:
        from services import openrouter_service
        for attr in ("call_openrouter", "chat_completion", "complete"):
            if hasattr(openrouter_service, attr):
                monkeypatch.setattr(openrouter_service, attr, _blocked(f"openrouter.{attr}"))
    except ImportError:
        pass
    try:
        from services import apify_service
        for attr in ("run_actor", "run_actor_get_items", "call_actor"):
            if hasattr(apify_service, attr):
                monkeypatch.setattr(apify_service, attr, _blocked(f"apify.{attr}"))
    except ImportError:
        pass
    # Note: unlike openrouter/apify (metered, pay-per-call), an accidental real
    # Gmail/Zoho/SMTP call in tests has no valid credentials to succeed with —
    # it fails loudly on its own, so no explicit block is added here (and doing
    # so would fight tests/api/test_email_providers.py, which exercises the
    # real send functions against a faked httpx client).


@pytest.fixture
def mock_growthtoolkit_http(monkeypatch):
    """Replace the shared httpx client inside growthtoolkit_service with a
    canned responder. Tests set responder.queue to a list of
    (status_code, json_body) or a callable(method, url, params, json_body)
    -> (status, body)."""
    from services import growthtoolkit_service as gts

    class _Responder:
        def __init__(self):
            self.queue: list = []
            self.calls: list = []
            self.handler = None

        def next_response(self, method, url, params, json_body):
            self.calls.append({"method": method, "url": url, "params": params, "json": json_body})
            if self.handler:
                return self.handler(method, url, params, json_body)
            if not self.queue:
                raise AssertionError("mock_growthtoolkit_http: no queued response left")
            return self.queue.pop(0)

    responder = _Responder()
    responder.usage_docs = []

    class _UsageCollection:
        async def insert_one(self, doc):
            responder.usage_docs.append(dict(doc))

    monkeypatch.setattr(gts, "_get_usage_collection", lambda: _UsageCollection())

    class _FakeResponse:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self._body = body

        def json(self):
            if isinstance(self._body, Exception):
                raise self._body
            return self._body

    class _FakeAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def request(self, method, url, params=None, json=None, headers=None):
            status, body = responder.next_response(method, url, params, json)
            return _FakeResponse(status, body)

        async def aclose(self):
            pass

    # The service now uses a shared module-level client (gts._client, lazily
    # created by _get_client). Setting _client directly makes _get_client
    # return the fake; monkeypatch restores the previous value afterwards.
    monkeypatch.setattr(gts, "_client", _FakeAsyncClient())
    # Also patch the class so any accidental fresh creation stays offline.
    monkeypatch.setattr(gts.httpx, "AsyncClient", _FakeAsyncClient)
    # Kill client-side rate limiting + poll delays so tests run fast
    monkeypatch.setattr(gts, "_LIMITERS", {
        k: gts._RateLimiter(0.0) for k in ("action", "list", "export", "misc")
    })
    monkeypatch.setattr(gts, "_TASK_POLL_INTERVAL", 0.01)
    monkeypatch.setattr(gts, "_5XX_BACKOFF", 0.01)
    return responder


@pytest.fixture
def mock_enrich_linkedin(monkeypatch):
    """Service-level mock for the phone-unlock endpoint. Configure via
    .result / .exception; records calls."""
    from services import growthtoolkit_service as gts

    state = {"result": None, "exception": None, "calls": []}

    async def _fake(url, unlock_emails=False, unlock_phone=False, *, account_id=None, prospect_id=None):
        state["calls"].append({
            "url": url, "unlock_phone": unlock_phone,
            "account_id": account_id, "prospect_id": prospect_id,
        })
        if state["exception"]:
            raise state["exception"]
        return state["result"]

    monkeypatch.setattr(gts, "enrich_linkedin", _fake)
    return state


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def perf_metrics():
    """Session list the perf suite appends {'name', 'ms', ...} rows to; the
    timing plugin writes them into timings_iteration_1.json."""
    return PERF_METRICS


@pytest.fixture
def timer():
    class _Timer:
        def __enter__(self):
            self._t0 = time.perf_counter()
            return self

        def __exit__(self, *exc):
            self.ms = (time.perf_counter() - self._t0) * 1000

    return _Timer
