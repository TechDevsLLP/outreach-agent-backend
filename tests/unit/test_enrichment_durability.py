"""Offline regressions for enrichment ownership and durable dispatch."""

from types import SimpleNamespace

import pytest
from bson import ObjectId
from fastapi import HTTPException

import routes.enrichment as enrichment_routes
import routes.campaigns as campaign_routes
import services.enrichment_job_service as enrichment_jobs
import services.enrichment_pipeline as enrichment_pipeline
import services.campaign_day_enrichment_service as campaign_day_service


pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.docs):
            raise StopAsyncIteration
        value = self.docs[self.index]
        self.index += 1
        return value


class _FindCollection:
    def __init__(self, docs):
        self.docs = list(docs)
        self.queries = []

    def find(self, query, projection=None):
        self.queries.append(query)
        return _Cursor(self.docs)


async def test_user_supplied_prospect_id_requires_tenant_state_or_enrollment(monkeypatch):
    account_id = str(ObjectId())
    prospect_id = str(ObjectId())
    monkeypatch.setattr(enrichment_routes, "prospect_state_collection", _FindCollection([]))
    monkeypatch.setattr(enrichment_routes, "campaign_enrollments_collection", _FindCollection([]))
    monkeypatch.setattr(
        enrichment_routes, "prospects_collection", _FindCollection([{"_id": ObjectId(prospect_id)}])
    )

    with pytest.raises(HTTPException) as exc:
        await enrichment_routes._authorized_prospect_ids(
            account_id=account_id, prospect_ids=[prospect_id]
        )

    assert exc.value.status_code == 404


async def test_user_supplied_prospect_id_is_returned_only_after_overlay_proof(monkeypatch):
    account_id = str(ObjectId())
    prospect_id = str(ObjectId())
    state = _FindCollection([{"prospect_id": prospect_id}])
    monkeypatch.setattr(enrichment_routes, "prospect_state_collection", state)
    monkeypatch.setattr(enrichment_routes, "campaign_enrollments_collection", _FindCollection([]))
    monkeypatch.setattr(
        enrichment_routes, "prospects_collection", _FindCollection([{"_id": ObjectId(prospect_id)}])
    )

    assert await enrichment_routes._authorized_prospect_ids(
        account_id=account_id, prospect_ids=[prospect_id]
    ) == [prospect_id]
    assert state.queries[0]["account_id"]["$in"] == [account_id, ObjectId(account_id)]


class _StateCollection:
    def __init__(self, matched_count=1):
        self.matched_count = matched_count
        self.calls = []

    async def update_one(self, query, update, **kwargs):
        self.calls.append((query, update, kwargs))
        return SimpleNamespace(matched_count=self.matched_count)


async def test_runtime_status_is_written_to_tenant_overlay_not_shared_pool(monkeypatch):
    account_id = str(ObjectId())
    prospect_id = str(ObjectId())
    state = _StateCollection()
    monkeypatch.setattr(enrichment_pipeline, "prospect_state_collection", state)

    await enrichment_pipeline._update_prospect_status(
        account_id, None, prospect_id, "in_progress", run_id="run-1"
    )

    query, update, _ = state.calls[0]
    assert query == {"account_id": account_id, "prospect_id": prospect_id}
    assert update["$set"]["enrichment_status"] == "in_progress"
    assert update["$set"]["enrichment_run_id"] == "run-1"


async def test_missing_overlay_logs_warning_and_does_not_abort_pipeline(monkeypatch, caplog):
    # A missing/mismatched state doc is bookkeeping only — it must never raise
    # and mask the real error (e.g. an AI timeout) that triggered the call.
    monkeypatch.setattr(
        enrichment_pipeline, "prospect_state_collection", _StateCollection(matched_count=0)
    )
    with caplog.at_level("WARNING"):
        await enrichment_pipeline._update_prospect_status(
            str(ObjectId()), None, str(ObjectId()), "completed"
        )
    assert "bookkeeping miss" in caplog.text


async def test_enqueue_uses_deterministic_tenant_run_key(monkeypatch):
    captured = {}

    class _Queue:
        def __init__(self, collection):
            captured["collection"] = collection

        async def enqueue(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(id="job-1")

    jobs_collection = object()
    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", jobs_collection)
    monkeypatch.setattr(enrichment_jobs, "JobQueueService", _Queue)

    await enrichment_jobs.enqueue_enrichment_run(
        account_id="tenant-a", run_id="run-a", prospect_ids=["prospect-a"],
        options={"skip_outreach": True}, campaign_id="campaign-a",
    )

    assert captured["account_id"] == "tenant-a"
    assert captured["job_type"] == enrichment_jobs.ENRICHMENT_JOB_TYPE
    assert captured["job_key"] == "enrichment-run:run-a"
    assert captured["payload"]["campaign_id"] == "campaign-a"


async def test_campaign_day_enqueue_is_generation_idempotent(monkeypatch):
    captured = {}

    class _Queue:
        def __init__(self, collection):
            pass

        async def enqueue(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(id="job-campaign-day")

    monkeypatch.setattr(enrichment_jobs, "JobQueueService", _Queue)
    await enrichment_jobs.enqueue_campaign_day_run(
        account_id="tenant-a", campaign_id="campaign-a", day=2,
        generation=7, request={"instructions": {"email": "brief"}},
    )

    assert captured["job_type"] == enrichment_jobs.CAMPAIGN_DAY_JOB_TYPE
    assert captured["job_key"] == "campaign-day:campaign-a:2:generation:7"
    assert captured["payload"]["request"]["instructions"]["email"] == "brief"


async def test_campaign_discovery_enqueue_is_generation_idempotent(monkeypatch):
    captured = {}

    class _Queue:
        def __init__(self, collection):
            pass

        async def enqueue(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(id="job-discovery")

    monkeypatch.setattr(enrichment_jobs, "JobQueueService", _Queue)
    await enrichment_jobs.enqueue_campaign_discovery(
        account_id="tenant-a", campaign_id="campaign-a", generation=3
    )

    assert captured["job_type"] == enrichment_jobs.CAMPAIGN_DISCOVERY_JOB_TYPE
    assert captured["job_key"] == "campaign-discovery:campaign-a:generation:3"
    assert captured["priority"] == 20


async def test_dispatcher_discovers_and_claims_campaign_day_jobs_independently(monkeypatch):
    captured = {}

    class _Jobs:
        async def distinct(self, field, query):
            captured["distinct_field"] = field
            captured["distinct_query"] = query
            return ["tenant-a"]

    class _Queue:
        def __init__(self, collection):
            pass

        async def claim(self, **kwargs):
            captured["claim"] = kwargs
            return None

    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", _Jobs())
    monkeypatch.setattr(enrichment_jobs, "JobQueueService", _Queue)

    assert await enrichment_jobs.process_enrichment_jobs() == 0
    assert enrichment_jobs.CAMPAIGN_DAY_JOB_TYPE in captured["distinct_query"]["job_type"]["$in"]
    assert enrichment_jobs.CAMPAIGN_DAY_JOB_TYPE in captured["claim"]["job_types"]
    assert enrichment_jobs.CAMPAIGN_DISCOVERY_JOB_TYPE in captured["distinct_query"]["job_type"]["$in"]
    assert enrichment_jobs.CAMPAIGN_DISCOVERY_JOB_TYPE in captured["claim"]["job_types"]


class _ListCursor:
    def __init__(self, docs):
        self.docs = docs

    async def to_list(self, length):
        return list(self.docs)[:length]


async def test_campaign_day_endpoint_is_queue_only_and_tenant_scoped(monkeypatch):
    account_id = ObjectId()
    campaign_id = ObjectId()
    enrollment_id = ObjectId()

    class _Campaigns:
        async def find_one(self, query):
            assert query == {"_id": campaign_id, "account_id": account_id}
            return {"_id": campaign_id, "account_id": account_id, "is_smart_campaign": True}

        async def find_one_and_update(self, query, update, return_document=None):
            assert query["account_id"] == account_id
            assert query["message_gen_status"]["$nin"] == ["queued", "running"]
            return {"_id": campaign_id, "message_gen_generation": 4}

        async def update_one(self, query, update):
            return SimpleNamespace(matched_count=1)

    class _Enrollments:
        def find(self, query, projection=None):
            assert query["account_id"]["$in"] == [account_id, str(account_id)]
            assert query["campaign_id"]["$in"] == [campaign_id, str(campaign_id)]
            return _ListCursor([{"_id": enrollment_id, "smart_campaign_channel": "email"}])

    captured = {}

    async def _enqueue(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(id="durable-job-1")

    monkeypatch.setattr(campaign_routes, "campaigns_collection", _Campaigns())
    monkeypatch.setattr(campaign_routes, "campaign_enrollments_collection", _Enrollments())
    monkeypatch.setattr(enrichment_jobs, "enqueue_campaign_day_run", _enqueue)

    response = await campaign_routes.enrich_and_generate_for_day_endpoint(
        campaign_id=str(campaign_id), day=2,
        body=campaign_routes.EnrichAndGenerateRequest(),
        account_ctx={"account": {"_id": str(account_id)}},
    )

    assert response["status"] == "queued"
    assert response["job_id"] == "durable-job-1"
    assert captured["generation"] == 4
    assert captured["account_id"] == str(account_id)


class _CampaignCollection:
    def __init__(self, campaign):
        self.campaign = campaign
        self.find_queries = []
        self.update_queries = []

    async def find_one(self, query):
        self.find_queries.append(query)
        return dict(self.campaign)

    async def update_one(self, query, update):
        self.update_queries.append((query, update))
        return SimpleNamespace(matched_count=1)


class _EnrollmentCollection:
    def __init__(self, docs):
        self.docs = docs
        self.find_queries = []

    def find(self, query):
        self.find_queries.append(query)
        return _ListCursor(self.docs)


async def test_campaign_day_worker_reads_campaign_and_enrollments_inside_tenant(monkeypatch):
    account_id = str(ObjectId())
    campaign_id = str(ObjectId())
    campaigns = _CampaignCollection({"_id": ObjectId(campaign_id), "account_id": account_id})
    enrollments = _EnrollmentCollection([])
    monkeypatch.setattr(campaign_day_service, "campaigns_collection", campaigns)
    monkeypatch.setattr(campaign_day_service, "campaign_enrollments_collection", enrollments)

    result = await campaign_day_service.run_enrich_and_generate_for_day(
        campaign_id=campaign_id, account_id=account_id, day=3,
        generation=1, request={}, orchestration_job_id="job-1",
    )

    assert result == {"generated": 0, "failed": 0, "skipped": 0}
    assert campaigns.find_queries[0]["account_id"]["$in"] == [account_id, ObjectId(account_id)]
    enrollment_query = enrollments.find_queries[0]
    assert enrollment_query["account_id"]["$in"] == [account_id, ObjectId(account_id)]
    assert enrollment_query["campaign_id"]["$in"] == [campaign_id, ObjectId(campaign_id)]
    assert all("account_id" in query for query, _ in campaigns.update_queries)


async def test_pre_enrichment_triage_writes_decision_only_to_tenant_state(monkeypatch):
    account_id = str(ObjectId())
    prospect_id = ObjectId()
    state = _StateCollection()

    class _SharedPoolMustNotMutate:
        async def bulk_write(self, *args, **kwargs):
            raise AssertionError("triage must not write tenant decisions to shared prospects")

        async def update_one(self, *args, **kwargs):
            raise AssertionError("triage must not write tenant decisions to shared prospects")

    monkeypatch.setattr(enrichment_pipeline, "prospect_state_collection", state)
    monkeypatch.setattr(enrichment_pipeline, "prospects_collection", _SharedPoolMustNotMutate())
    monkeypatch.setattr(enrichment_pipeline, "score_company_fit_rule_based", lambda prospect: (90, {"size": 90}))
    monkeypatch.setattr(enrichment_pipeline, "is_decision_maker_rule_based", lambda prospect: (True, "founder"))

    stats = {"triage_decision_makers": 0, "triage_wrong_person": 0, "prospects_skipped": 0}
    prospects, returned_stats = await enrichment_pipeline._phase_pre_enrichment_triage(
        [{"_id": prospect_id, "linkedin": "https://linkedin.example/person"}],
        stats, "run-1", account_id=account_id,
    )

    assert prospects[0]["_id"] == prospect_id
    assert returned_stats["triage_decision_makers"] == 1
    query, update, _ = state.calls[0]
    assert query == {"account_id": account_id, "prospect_id": str(prospect_id)}
    assert update["$set"]["pre_enrichment_triage"]["action"] == "proceed"


# ──────────────────────────────────────────────────────────────────────────────
# Durable message generation + Days 2-5 enrichment (OF-P0-013 follow-up)
# ──────────────────────────────────────────────────────────────────────────────


class _CoalescingJobs:
    """Minimal upsert-on-identity fake proving deterministic-key coalescing."""

    def __init__(self):
        self.documents: list[dict] = []
        self.insert_calls = 0

    async def find_one_and_update(self, identity, update, upsert=False, return_document=None):
        for doc in self.documents:
            if all(doc.get(k) == v for k, v in identity.items()):
                return doc
        self.insert_calls += 1
        doc = dict(update.get("$setOnInsert", {}))
        doc["_id"] = ObjectId()
        self.documents.append(doc)
        return doc

    async def find_one(self, identity):
        for doc in self.documents:
            if all(doc.get(k) == v for k, v in identity.items()):
                return doc
        return None


async def test_message_gen_enqueue_uses_deterministic_key_and_coalesces(monkeypatch):
    jobs = _CoalescingJobs()
    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", jobs)

    first = await enrichment_jobs.enqueue_campaign_message_generation(
        account_id="tenant-a", campaign_id="campaign-a", day=3,
        mode=enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY,
    )
    second = await enrichment_jobs.enqueue_campaign_message_generation(
        account_id="tenant-a", campaign_id="campaign-a", day=3,
        mode=enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY,
    )

    assert jobs.insert_calls == 1
    assert first.id == second.id
    assert first.job_type == enrichment_jobs.CAMPAIGN_MESSAGE_GEN_JOB_TYPE
    assert first.job_key == "campaign-message-gen:campaign-a:day:3:generate_day"
    assert first.payload["mode"] == enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY


async def test_message_gen_modes_have_distinct_keys(monkeypatch):
    jobs = _CoalescingJobs()
    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", jobs)

    await enrichment_jobs.enqueue_campaign_message_generation(
        account_id="tenant-a", campaign_id="campaign-a", day=2,
        mode=enrichment_jobs.MESSAGE_GEN_MODE_ENSURE_DAY,
    )
    await enrichment_jobs.enqueue_campaign_message_generation(
        account_id="tenant-a", campaign_id="campaign-a", day=2,
        mode=enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY,
    )

    assert jobs.insert_calls == 2
    keys = sorted(doc["job_key"] for doc in jobs.documents)
    assert keys == [
        "campaign-message-gen:campaign-a:day:2:ensure_day",
        "campaign-message-gen:campaign-a:day:2:generate_day",
    ]


async def test_message_gen_enqueue_rejects_unknown_mode(monkeypatch):
    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", _CoalescingJobs())
    with pytest.raises(ValueError):
        await enrichment_jobs.enqueue_campaign_message_generation(
            account_id="tenant-a", campaign_id="campaign-a", day=1, mode="nonsense",
        )


async def test_remaining_days_enqueue_uses_deterministic_key_and_coalesces(monkeypatch):
    jobs = _CoalescingJobs()
    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", jobs)
    oids = [str(ObjectId()), str(ObjectId())]

    first = await enrichment_jobs.enqueue_campaign_remaining_days(
        account_id="tenant-a", campaign_id="campaign-a", remaining_oids=oids,
        co_research_by_url={"https://x": {"summary": "y"}}, skip_message_gen=False,
    )
    second = await enrichment_jobs.enqueue_campaign_remaining_days(
        account_id="tenant-a", campaign_id="campaign-a", remaining_oids=oids,
    )

    assert jobs.insert_calls == 1
    assert first.id == second.id
    assert first.job_type == enrichment_jobs.CAMPAIGN_REMAINING_DAYS_JOB_TYPE
    assert first.job_key == "campaign-remaining-days:campaign-a"
    assert first.payload["remaining_oids"] == oids
    assert first.payload["co_research_by_url"] == {"https://x": {"summary": "y"}}


async def test_dispatcher_discovers_and_claims_new_durable_job_types(monkeypatch):
    captured = {}

    class _Jobs:
        async def distinct(self, field, query):
            captured["distinct_query"] = query
            return []

    monkeypatch.setattr(enrichment_jobs.database, "jobs_collection", _Jobs())
    assert await enrichment_jobs.process_enrichment_jobs() == 0
    job_types = captured["distinct_query"]["job_type"]["$in"]
    assert enrichment_jobs.CAMPAIGN_MESSAGE_GEN_JOB_TYPE in job_types
    assert enrichment_jobs.CAMPAIGN_REMAINING_DAYS_JOB_TYPE in job_types


class _DispatchQueue:
    """Records worker lifecycle calls; leases always succeed."""

    def __init__(self):
        self.completed = None

    async def checkpoint(self, **kwargs):
        return SimpleNamespace(**kwargs)

    async def heartbeat(self, **kwargs):
        return SimpleNamespace(**kwargs)

    async def complete(self, **kwargs):
        self.completed = kwargs
        return SimpleNamespace(id=kwargs.get("job_id"))

    async def fail(self, **kwargs):
        raise AssertionError(f"job unexpectedly failed: {kwargs.get('error')}")


class _OwnedCampaigns:
    async def find_one(self, query, projection=None):
        self.query = query
        return {"_id": query["_id"]}


async def test_worker_dispatches_message_gen_ensure_day_to_service(monkeypatch):
    account_id = str(ObjectId())
    campaign_id = str(ObjectId())
    monkeypatch.setattr(enrichment_jobs.database, "campaigns_collection", _OwnedCampaigns())

    calls = {}
    import services.curated_discovery_service as cds

    async def _ensure(cid, aid, day):
        calls["args"] = (cid, aid, day)
        return {"generated": 1}

    async def _generate(*a, **k):
        raise AssertionError("ensure_day mode must not call generate_messages_for_campaign")

    monkeypatch.setattr(cds, "ensure_day_ready_then_generate", _ensure)
    import services.campaign_message_generator_service as cmg
    monkeypatch.setattr(cmg, "generate_messages_for_campaign", _generate)

    job = SimpleNamespace(
        id="job-mg", account_id=account_id, attempt_count=1,
        job_type=enrichment_jobs.CAMPAIGN_MESSAGE_GEN_JOB_TYPE,
        payload={"campaign_id": campaign_id, "day": 4,
                 "mode": enrichment_jobs.MESSAGE_GEN_MODE_ENSURE_DAY},
    )
    queue = _DispatchQueue()
    await enrichment_jobs._execute_claimed_job(queue, job, "worker-1")

    assert calls["args"] == (campaign_id, account_id, 4)
    assert queue.completed["job_id"] == "job-mg"


async def test_worker_dispatches_message_gen_generate_day_to_service(monkeypatch):
    account_id = str(ObjectId())
    campaign_id = str(ObjectId())
    monkeypatch.setattr(enrichment_jobs.database, "campaigns_collection", _OwnedCampaigns())

    calls = {}
    import services.campaign_message_generator_service as cmg

    async def _generate(cid, aid, send_day=None):
        calls["args"] = (cid, aid, send_day)
        return {"generated": 2}

    monkeypatch.setattr(cmg, "generate_messages_for_campaign", _generate)

    job = SimpleNamespace(
        id="job-mg2", account_id=account_id, attempt_count=1,
        job_type=enrichment_jobs.CAMPAIGN_MESSAGE_GEN_JOB_TYPE,
        payload={"campaign_id": campaign_id, "day": 2,
                 "mode": enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY},
    )
    queue = _DispatchQueue()
    await enrichment_jobs._execute_claimed_job(queue, job, "worker-1")

    assert calls["args"] == (campaign_id, account_id, 2)
    assert queue.completed["job_id"] == "job-mg2"


async def test_worker_dispatches_remaining_days_to_service(monkeypatch):
    account_id = str(ObjectId())
    campaign_id = str(ObjectId())
    oid_a, oid_b = ObjectId(), ObjectId()
    monkeypatch.setattr(enrichment_jobs.database, "campaigns_collection", _OwnedCampaigns())

    calls = {}
    import services.curated_discovery_service as cds

    async def _enrich(**kwargs):
        calls.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(cds, "_enrich_remaining_days", _enrich)

    job = SimpleNamespace(
        id="job-rd", account_id=account_id, attempt_count=1,
        job_type=enrichment_jobs.CAMPAIGN_REMAINING_DAYS_JOB_TYPE,
        payload={"campaign_id": campaign_id,
                 "remaining_oids": [str(oid_a), str(oid_b)],
                 "co_research_by_url": {"https://x": {"s": 1}},
                 "skip_message_gen": True},
    )
    queue = _DispatchQueue()
    await enrichment_jobs._execute_claimed_job(queue, job, "worker-1")

    assert calls["campaign_id"] == campaign_id
    assert calls["account_id"] == account_id
    assert calls["remaining_oids"] == [oid_a, oid_b]
    assert calls["co_research_by_url"] == {"https://x": {"s": 1}}
    assert calls["skip_message_gen"] is True
    assert queue.completed["job_id"] == "job-rd"


async def test_worker_rejects_message_gen_job_for_foreign_tenant(monkeypatch):
    class _NoCampaign:
        async def find_one(self, query, projection=None):
            return None

    monkeypatch.setattr(enrichment_jobs.database, "campaigns_collection", _NoCampaign())

    failed = {}

    class _FailQueue(_DispatchQueue):
        async def fail(self, **kwargs):
            failed.update(kwargs)
            return SimpleNamespace()

    job = SimpleNamespace(
        id="job-x", account_id=str(ObjectId()), attempt_count=1,
        job_type=enrichment_jobs.CAMPAIGN_MESSAGE_GEN_JOB_TYPE,
        payload={"campaign_id": str(ObjectId()), "day": 1,
                 "mode": enrichment_jobs.MESSAGE_GEN_MODE_GENERATE_DAY},
    )
    await enrichment_jobs._execute_claimed_job(_FailQueue(), job, "worker-1")
    assert "stale or not tenant-owned" in failed["error"]
