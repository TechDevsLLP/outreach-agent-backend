"""
Onboarding wizard tests — Bug A (sender voice sync) and Bug B (stage-3
synchronous campaign creation + self-healing launch) plus the preview-message
endpoint.

External providers are never called: OpenRouter/Unipile/sourcing are mocked at
the service layer; the conftest guardrail blocks anything that slips through.
"""
import pytest
import pytest_asyncio
from bson import ObjectId

import database


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def as_identity_a(app, identity_a):
    """Bypass the known-broken get_account_context ObjectId/string mismatch
    (pre-existing bug flagged separately) with a dependency override so these
    tests exercise the onboarding routes, not the auth resolution bug."""
    from auth import get_account_context

    async def _ctx():
        return {
            "user": {"_id": identity_a["user_id"], "email": identity_a["email"]},
            "account": {"_id": identity_a["account_id"]},
        }

    app.dependency_overrides[get_account_context] = _ctx
    yield identity_a
    app.dependency_overrides.pop(get_account_context, None)


@pytest_asyncio.fixture
async def wizard_session(client, as_identity_a):
    """Start (or resume) an onboarding wizard session for account A."""
    resp = await client.post("/api/onboarding/start", json={}, headers=as_identity_a["headers"])
    assert resp.status_code == 200, resp.text
    return resp.json()["session_id"]


@pytest.fixture
def block_discovery(monkeypatch):
    """Stop stage-3 background discovery + preview prefetch from running for real."""
    from services import onboarding_scrape_service as oss
    from services import onboarding_prospect_service as ops

    calls = {"scrape": [], "source": []}

    async def _fake_scrape(**kwargs):
        calls["scrape"].append(kwargs)

    async def _fake_source(profile, **kwargs):
        calls["source"].append(kwargs)
        return []

    monkeypatch.setattr(oss, "start_onboarding_scrape", _fake_scrape)
    monkeypatch.setattr(ops, "source_preview_prospects", _fake_source)
    return calls


ICP_BODY = {
    "industries": ["Industrial Machinery", "Chemicals"],
    "job_titles": ["CEO", "COO"],
    "seniority_levels": ["c_suite"],
    "geographies": ["United States"],
    "company_size_ranges": ["11-50"],
}


# ---------------------------------------------------------------------------
# Bug B: stage-3 synchronous campaign creation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stage3_creates_campaign_synchronously(client, identity_a, wizard_session, block_discovery):
    body = {**ICP_BODY, "session_id": wizard_session, "locked_industry": "Industrial Machinery"}
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    assert resp.status_code == 200, resp.text
    data = resp.json()

    campaign_id = data.get("campaign_id")
    assert campaign_id, "stage-3 response must include campaign_id"
    assert data["locked_industry"] == "Industrial Machinery"

    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    assert campaign is not None
    # account_id must be a plain string, never ObjectId
    assert campaign["account_id"] == identity_a["account_id"]
    assert isinstance(campaign["account_id"], str)
    assert campaign["icp_industries"] == ["Industrial Machinery"]
    assert campaign.get("is_onboarding_campaign") is True

    session = await database.onboarding_sessions_collection.find_one({"session_id": wizard_session})
    assert str(session.get("onboarding_campaign_id")) == campaign_id


@pytest.mark.asyncio
async def test_stage3_is_idempotent_per_session(client, identity_a, wizard_session, block_discovery):
    body = {**ICP_BODY, "session_id": wizard_session, "locked_industry": "Industrial Machinery"}
    r1 = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    r2 = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["campaign_id"] == r2.json()["campaign_id"]
    count = await database.campaigns_collection.count_documents(
        {"onboarding_session_id": wizard_session}
    )
    assert count == 1


@pytest.mark.asyncio
async def test_stage3_defaults_locked_industry_from_industries(client, identity_a, wizard_session, block_discovery):
    body = {**ICP_BODY, "session_id": wizard_session}  # no locked_industry
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["locked_industry"] == "Industrial Machinery"
    assert data["campaign_id"]


@pytest.mark.asyncio
async def test_stage3_422_when_no_industry_at_all(client, identity_a, wizard_session, block_discovery):
    body = {**ICP_BODY, "industries": [], "session_id": wizard_session}
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    assert resp.status_code == 422
    assert "locked_industry" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_launch_first_campaign_self_heals(client, identity_a, wizard_session, block_discovery, monkeypatch):
    """No campaign on the session -> launch rebuilds it from the company profile
    and queues discovery (replan is skipped — nothing to plan yet)."""
    # Ensure the session has no campaign reference
    await database.onboarding_sessions_collection.update_one(
        {"session_id": wizard_session},
        {"$unset": {"onboarding_campaign_id": ""}},
    )
    await database.onboarding_scrape_jobs_collection.delete_many({"session_id": wizard_session})
    await database.campaigns_collection.delete_many({"onboarding_session_id": wizard_session})

    # Saved profile stage-3 data exists (target industries)
    await database.company_profiles_collection.update_one(
        {"account_id": identity_a["account_id"]},
        {"$set": {"target_industries": ["Chemicals"], "target_job_titles": ["CEO"]}},
        upsert=True,
    )

    from services import curated_discovery_service as cds

    replans = []

    async def _fake_replan(campaign_id, account_id):
        replans.append((campaign_id, account_id))
        return {"assigned": 7, "day_totals": {"1": 7}}

    monkeypatch.setattr(cds, "replan_and_launch", _fake_replan)

    resp = await client.post(
        "/api/onboarding/launch-first-campaign",
        json={"session_id": wizard_session},
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is True
    assert data["campaign_id"]
    # Freshly rebuilt campaign has no enrollments -> discovery is queued and
    # channel replanning is skipped until prospects exist.
    assert data["building"] is True
    assert data["assigned"] == 0
    assert replans == []

    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(data["campaign_id"])})
    assert campaign["account_id"] == identity_a["account_id"]
    assert campaign["icp_industries"] == ["Chemicals"]
    assert campaign["discovery_status"] == "queued"

    # Durable discovery job persisted
    job = await database.jobs_collection.find_one(
        {"job_type": "campaign_discovery_v1", "payload.campaign_id": data["campaign_id"]}
    )
    assert job is not None


@pytest.mark.asyncio
async def test_launch_first_campaign_rekicks_stalled_discovery(client, identity_a, wizard_session, block_discovery, monkeypatch):
    """Campaign exists but discovery never ran (pending, 0 enrollments) ->
    launch must queue durable discovery instead of silently no-op replanning.
    This is the exact bug that stranded onboarding on 'Building your list…'."""
    body = {**ICP_BODY, "session_id": wizard_session, "locked_industry": "Industrial Machinery"}
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    campaign_id = resp.json()["campaign_id"]

    # Simulate the lost stage-3 background task: no scrape job, discovery pending
    await database.onboarding_scrape_jobs_collection.delete_many({"session_id": wizard_session})
    await database.jobs_collection.delete_many({"payload.campaign_id": campaign_id})
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$set": {"discovery_status": "pending"}},
    )

    from services import curated_discovery_service as cds
    replans = []

    async def _fake_replan(cid, aid):
        replans.append(cid)
        return {"assigned": 0, "day_totals": {}}

    monkeypatch.setattr(cds, "replan_and_launch", _fake_replan)

    resp = await client.post(
        "/api/onboarding/launch-first-campaign",
        json={"session_id": wizard_session},
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["campaign_id"] == campaign_id
    assert data["building"] is True
    assert replans == []

    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    assert campaign["discovery_status"] == "queued"
    assert int(campaign.get("discovery_generation") or 0) >= 1

    job = await database.jobs_collection.find_one(
        {"job_type": "campaign_discovery_v1", "payload.campaign_id": campaign_id}
    )
    assert job is not None, "durable discovery job must be enqueued for a stalled campaign"

    scrape_job = await database.onboarding_scrape_jobs_collection.find_one(
        {"session_id": wizard_session}
    )
    assert scrape_job is not None and scrape_job["status"] == "running"


@pytest.mark.asyncio
async def test_launch_first_campaign_no_double_enqueue_while_running(client, identity_a, wizard_session, block_discovery, monkeypatch):
    """Discovery already in flight -> launch must NOT queue a second job."""
    body = {**ICP_BODY, "session_id": wizard_session, "locked_industry": "Industrial Machinery"}
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    campaign_id = resp.json()["campaign_id"]

    await database.jobs_collection.delete_many({"payload.campaign_id": campaign_id})
    await database.campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$set": {"discovery_status": "sourcing_companies", "discovery_generation": 3}},
    )

    from services import curated_discovery_service as cds

    async def _fake_replan(cid, aid):
        return {"assigned": 0, "day_totals": {}}

    monkeypatch.setattr(cds, "replan_and_launch", _fake_replan)

    resp = await client.post(
        "/api/onboarding/launch-first-campaign",
        json={"session_id": wizard_session},
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["building"] is False

    job_count = await database.jobs_collection.count_documents(
        {"job_type": "campaign_discovery_v1", "payload.campaign_id": campaign_id}
    )
    assert job_count == 0, "in-flight discovery must not be double-queued"
    campaign = await database.campaigns_collection.find_one({"_id": ObjectId(campaign_id)})
    assert campaign["discovery_status"] == "sourcing_companies"


@pytest.mark.asyncio
async def test_launch_first_campaign_replans_completed_discovery(client, identity_a, wizard_session, block_discovery, monkeypatch):
    """Healthy path: discovery completed with enrollments -> replan runs as before."""
    body = {**ICP_BODY, "session_id": wizard_session, "locked_industry": "Industrial Machinery"}
    resp = await client.post("/api/onboarding/stage-3/icp", json=body, headers=identity_a["headers"])
    campaign_id = resp.json()["campaign_id"]

    await database.campaigns_collection.update_one(
        {"_id": ObjectId(campaign_id)},
        {"$set": {"discovery_status": "completed"}},
    )
    await database.campaign_enrollments_collection.insert_one({
        "campaign_id": ObjectId(campaign_id),
        "account_id": identity_a["account_id"],
        "status": "active",
    })

    from services import curated_discovery_service as cds
    replans = []

    async def _fake_replan(cid, aid):
        replans.append(cid)
        return {"assigned": 5, "day_totals": {"1": 5}}

    monkeypatch.setattr(cds, "replan_and_launch", _fake_replan)

    resp = await client.post(
        "/api/onboarding/launch-first-campaign",
        json={"session_id": wizard_session},
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["building"] is False
    assert data["assigned"] == 5
    assert replans == [campaign_id]
    job_count = await database.jobs_collection.count_documents(
        {"job_type": "campaign_discovery_v1", "payload.campaign_id": campaign_id}
    )
    assert job_count == 0


@pytest.mark.asyncio
async def test_launch_first_campaign_404_without_profile_industry(client, identity_a, wizard_session, block_discovery):
    await database.onboarding_sessions_collection.update_one(
        {"session_id": wizard_session},
        {"$unset": {"onboarding_campaign_id": ""}},
    )
    await database.onboarding_scrape_jobs_collection.delete_many({"session_id": wizard_session})
    await database.company_profiles_collection.update_one(
        {"account_id": identity_a["account_id"]},
        {"$set": {"target_industries": []}},
        upsert=True,
    )
    resp = await client.post(
        "/api/onboarding/launch-first-campaign",
        json={"session_id": wizard_session},
        headers=identity_a["headers"],
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Bug A: sender voice sync
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_user_posts_resolves_provider_id(monkeypatch):
    from services.unipile_service import UnipileClient

    client = UnipileClient()
    requests = []

    async def _fake_request(method, endpoint, params=None, **kwargs):
        requests.append((method, endpoint, params))
        if endpoint == "users/john-doe-123":
            return {"provider_id": "ACoAAAtest99"}
        if endpoint == "users/ACoAAAtest99/posts":
            return {"items": [{"text": "hello world", "reaction_count": 3}]}
        raise AssertionError(f"unexpected endpoint {endpoint}")

    monkeypatch.setattr(client, "_request", _fake_request)

    posts = await client.get_user_posts("john-doe-123", account_id="unipile-acct-1")
    assert len(posts) == 1 and posts[0]["text"] == "hello world"
    # posts endpoint must be called with the resolved provider_id
    endpoints = [e for _, e, _ in requests]
    assert "users/ACoAAAtest99/posts" in endpoints
    # tenant-scoped account_id passed through
    assert all(p["account_id"] == "unipile-acct-1" for _, _, p in requests)


@pytest.mark.asyncio
async def test_get_user_posts_skips_resolution_for_provider_id(monkeypatch):
    from services.unipile_service import UnipileClient

    client = UnipileClient()
    requests = []

    async def _fake_request(method, endpoint, params=None, **kwargs):
        requests.append(endpoint)
        return {"items": []}

    monkeypatch.setattr(client, "_request", _fake_request)
    await client.get_user_posts("ACoAAAdirect", account_id="acct")
    assert requests == ["users/ACoAAAdirect/posts"]


@pytest.mark.asyncio
async def test_voice_sync_records_error_when_no_linkedin(identity_a):
    from services.sender_voice_service import update_sender_voice_from_unipile

    account_id = identity_a["account_id"]
    await database.linkedin_accounts_collection.delete_many({"account_id": account_id})

    result = await update_sender_voice_from_unipile(account_id)
    assert result["voice_profile"] is None
    assert result["low_confidence"] is True

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    assert profile is not None, "error must be recorded even without a pre-existing profile (upsert)"
    assert "No connected LinkedIn account" in profile["sender_voice_sync_error"]


@pytest.mark.asyncio
async def test_voice_sync_success_upserts_and_clears_error(identity_b, monkeypatch):
    from services import sender_voice_service as svs
    from services.unipile_service import UnipileClient

    account_id = identity_b["account_id"]

    # Start with no company profile at all — upsert must create it
    await database.company_profiles_collection.delete_many({"account_id": account_id})
    await database.linkedin_accounts_collection.delete_many({"account_id": account_id})
    await database.linkedin_accounts_collection.insert_one({
        "account_id": account_id,
        "unipile_account_id": "unipile-b-1",
        "public_id": "user-b-slug",
        "name": "User B",
        "headline": "COO at OtherCo",
    })

    async def _fake_profile(self, url):
        return {"headline": "COO at OtherCo", "about": "ops", "experience": []}

    async def _fake_posts(self, identifier, limit=20, account_id=None):
        assert account_id == "unipile-b-1"
        return [{"text": f"post {i}", "likes": i, "comments": 0, "posted_at": ""} for i in range(4)]

    async def _fake_synthesize(posts, sender_name, sender_role, raw_profile):
        return {"tone_markers": ["direct"], "post_count": len(posts),
                "synthesized_summary": "test", "post_excerpts": []}

    monkeypatch.setattr(UnipileClient, "get_profile_data_for_enrichment", _fake_profile)
    monkeypatch.setattr(UnipileClient, "get_user_posts", _fake_posts)
    monkeypatch.setattr(svs, "synthesize_voice_profile", _fake_synthesize)

    result = await svs.update_sender_voice_from_unipile(account_id)
    assert result["voice_profile"]["source"] == "unipile"
    assert result["low_confidence"] is False

    profile = await database.company_profiles_collection.find_one({"account_id": account_id})
    assert profile is not None, "upsert=True must create the profile"
    assert profile["sender_voice_profile"]["post_count"] == 4
    assert len(profile["sender_linkedin_posts"]) == 4
    assert "sender_voice_sync_error" not in profile
    assert profile.get("sender_voice_synced_at") is not None


# ---------------------------------------------------------------------------
# Preview message endpoint
# ---------------------------------------------------------------------------

PREVIEW_PROSPECT = {
    "full_name": "Alice Anderson", "linkedin": "https://linkedin.com/in/alice",
    "email": "alice@acme.test", "company_name": "Acme Industrial",
    "job_title": "VP of Marketing", "company_linkedin": None,
    "company_domain": "acme.test", "industry": "Industrial Machinery",
    "country": "United States",
}


@pytest.fixture
def mock_preview_llm(monkeypatch):
    from services.openrouter_service import OpenRouterClient

    calls = []

    async def _fake_chat(self, messages, **kwargs):
        calls.append({"messages": messages, **kwargs})
        return {"content": f"Sample outreach message #{len(calls)}"}

    monkeypatch.setattr(OpenRouterClient, "chat_completion", _fake_chat)
    return calls


@pytest.mark.asyncio
async def test_preview_message_generates_and_caches(client, identity_a, wizard_session, mock_preview_llm):
    await database.onboarding_sessions_collection.update_one(
        {"session_id": wizard_session},
        {"$set": {"prospect_preview": [PREVIEW_PROSPECT]}, "$unset": {"preview_message": ""}},
    )
    await database.company_profiles_collection.update_one(
        {"account_id": identity_a["account_id"]},
        {"$set": {
            "company_name": "TestCo A", "services": ["automation"],
            "sender_voice_profile": {"tone_markers": ["direct"], "synthesized_summary": "punchy"},
        }},
        upsert=True,
    )

    r1 = await client.post("/api/onboarding/preview-message",
                           json={"session_id": wizard_session}, headers=identity_a["headers"])
    assert r1.status_code == 200, r1.text
    d1 = r1.json()
    assert d1["message"] == "Sample outreach message #1"
    assert d1["voice_used"] is True
    assert d1["cached"] is False
    assert d1["prospect"]["full_name"] == "Alice Anderson"
    # Voice profile injected into the prompt
    assert "punchy" in mock_preview_llm[0]["messages"][1]["content"]

    # Second call: served from cache, no new LLM call
    r2 = await client.post("/api/onboarding/preview-message",
                           json={"session_id": wizard_session}, headers=identity_a["headers"])
    d2 = r2.json()
    assert d2["cached"] is True and d2["message"] == d1["message"]
    assert len(mock_preview_llm) == 1

    # regenerate=true bypasses the cache
    r3 = await client.post("/api/onboarding/preview-message",
                           json={"session_id": wizard_session, "regenerate": True},
                           headers=identity_a["headers"])
    d3 = r3.json()
    assert d3["cached"] is False and d3["message"] == "Sample outreach message #2"
    assert len(mock_preview_llm) == 2


@pytest.mark.asyncio
async def test_preview_message_voice_used_false_without_profile(client, identity_a, wizard_session, mock_preview_llm):
    await database.onboarding_sessions_collection.update_one(
        {"session_id": wizard_session},
        {"$set": {"prospect_preview": [PREVIEW_PROSPECT]}, "$unset": {"preview_message": ""}},
    )
    await database.company_profiles_collection.update_one(
        {"account_id": identity_a["account_id"]},
        {"$unset": {"sender_voice_profile": ""}},
        upsert=True,
    )
    resp = await client.post("/api/onboarding/preview-message",
                             json={"session_id": wizard_session}, headers=identity_a["headers"])
    assert resp.status_code == 200
    assert resp.json()["voice_used"] is False


@pytest.mark.asyncio
async def test_preview_message_422_without_prospects(client, identity_a, wizard_session, mock_preview_llm):
    await database.onboarding_sessions_collection.update_one(
        {"session_id": wizard_session},
        {"$unset": {"prospect_preview": "", "preview_message": ""}},
    )
    resp = await client.post("/api/onboarding/preview-message",
                             json={"session_id": wizard_session}, headers=identity_a["headers"])
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_preview_message_404_unknown_session(client, as_identity_a, mock_preview_llm):
    resp = await client.post("/api/onboarding/preview-message",
                             json={"session_id": "no-such-session"}, headers=as_identity_a["headers"])
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Draft autosave — partial wizard input must land in the DB immediately
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_draft_persists_partial_profile_without_advancing_stage(
    client, identity_a, wizard_session
):
    resp = await client.post(
        "/api/onboarding/draft",
        json={
            "session_id": wizard_session,
            "profile": {
                "website_url": "  https://acme.test  ",
                "target_job_titles": ["Head of Sales", " ", "VP Marketing"],
                "primary_cta": "Book a 20-min call",
            },
        },
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is True

    profile = await database.company_profiles_collection.find_one(
        {"account_id": identity_a["account_id"]}
    )
    assert profile["website_url"] == "https://acme.test"
    assert profile["target_job_titles"] == ["Head of Sales", "VP Marketing"]
    assert profile["primary_cta"] == "Book a 20-min call"

    session = await database.onboarding_sessions_collection.find_one(
        {"session_id": wizard_session}
    )
    # Drafts record progress but never move the wizard forward.
    assert session["current_stage"] == 1
    assert session["stage_data"]["draft"]["primary_cta"] == "Book a 20-min call"


@pytest.mark.asyncio
async def test_draft_ignores_unknown_fields(client, identity_a, wizard_session):
    resp = await client.post(
        "/api/onboarding/draft",
        json={
            "session_id": wizard_session,
            "profile": {"plan": "enterprise", "account_id": "someone-else", "services": ["SEO"]},
        },
        headers=identity_a["headers"],
    )
    assert resp.status_code == 200
    assert resp.json()["saved"] == 1

    profile = await database.company_profiles_collection.find_one(
        {"account_id": identity_a["account_id"]}
    )
    assert profile["services"] == ["SEO"]
    assert profile.get("plan") is None
    assert profile["account_id"] == identity_a["account_id"]
