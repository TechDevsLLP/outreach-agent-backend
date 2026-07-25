"""Offline contract tests for campaign-scoped scoring and enrichment state."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId

from services.campaign_prospect_state_service import (
    DEFAULT_SCORING_VERSION,
    campaign_enrichment_projection,
    campaign_score_projection,
    campaign_state_page_pipeline,
    ensure_cohort_membership,
    natural_key,
    score_update_operation,
    transition_enrichment,
)
from services.campaign_scoring_service import compute_campaign_score


pytestmark = pytest.mark.unit


def _op_filter(operation):
    return operation._filter


def _op_update(operation):
    return operation._doc


def test_same_pool_prospect_has_independent_campaign_score_keys():
    prospect_id = ObjectId()
    account_id = ObjectId()
    campaign_a = ObjectId()
    campaign_b = ObjectId()

    op_a = score_update_operation(
        account_id=account_id,
        campaign_id=campaign_a,
        prospect_id=prospect_id,
        result={"fit_score": 82, "priority_tier": "hot"},
    )
    op_b = score_update_operation(
        account_id=account_id,
        campaign_id=campaign_b,
        prospect_id=prospect_id,
        result={"fit_score": 12, "priority_tier": "cold"},
    )

    assert _op_filter(op_a) == natural_key(account_id, campaign_a, prospect_id)
    assert _op_filter(op_b) == natural_key(account_id, campaign_b, prospect_id)
    assert _op_filter(op_a) != _op_filter(op_b)
    assert _op_update(op_a)["$set"]["score"]["value"] == 82.0
    assert _op_update(op_b)["$set"]["score"]["value"] == 12.0


def test_zero_is_scored_while_none_remains_unscored():
    common = dict(account_id="a", campaign_id="c", scoring_version="v1")
    zero = score_update_operation(
        **common, prospect_id="zero", result={"fit_score": 0, "breakdown": {}}
    )
    unscored = score_update_operation(
        **common,
        prospect_id="null",
        result={"fit_score": None, "error_code": "INPUTS_MISSING"},
    )

    zero_score = _op_update(zero)["$set"]["score"]
    null_score = _op_update(unscored)["$set"]["score"]
    assert zero_score["value"] == 0.0
    assert zero_score["scored_at"] is not None
    assert null_score["value"] is None
    assert null_score["scored_at"] is None
    assert null_score["error_code"] == "INPUTS_MISSING"


def test_public_score_contract_is_stable_and_preserves_zero():
    scored_at = ObjectId().generation_time
    projected = campaign_score_projection(
        {
            "scoring_version": "v2",
            "score": {
                "value": 0,
                "reasoning": "Excluded by ICP rule",
                "completeness": 0.75,
                "breakdown": {"title_match": 0},
                "scored_at": scored_at,
            },
        }
    )

    assert projected == {
        "value": 0,
        "version": "v2",
        "reasoning": "Excluded by ICP rule",
        "completeness": 0.75,
        "breakdown": {"title_match": 0},
        "scored_at": scored_at,
    }
    assert campaign_score_projection({"scoring_version": "v2"})["value"] is None


def test_enrichment_contract_declares_campaign_ownership_and_provenance():
    projected = campaign_enrichment_projection(
        {
            "enrichment": {
                "state": "succeeded",
                "result": {"provider": "growthtoolkit"},
            }
        },
        shared_source="linkedin_search",
    )

    assert projected["ownership"] == "campaign"
    assert projected["source"] == "growthtoolkit"
    assert projected["shared_pool_source"] == "linkedin_search"


def test_campaign_page_pipeline_filters_sorts_and_pages_before_any_join():
    pipeline = campaign_state_page_pipeline(
        account_id=ObjectId("64b000000000000000000001"),
        campaign_id=ObjectId("64b000000000000000000002"),
        scoring_version="campaign-fit-v7",
        prospect_ids=[
            ObjectId("64b000000000000000000003"),
            "64b000000000000000000004",
        ],
        min_score=0,
        page=2,
        page_size=25,
        sort_order="desc",
    )

    match = pipeline[0]["$match"]
    assert match == {
        "account_id": "64b000000000000000000001",
        "campaign_id": "64b000000000000000000002",
        "scoring_version": "campaign-fit-v7",
        "prospect_id": {
            "$in": [
                "64b000000000000000000003",
                "64b000000000000000000004",
            ]
        },
        "score.value": {"$gte": 0},
    }
    page = pipeline[2]["$facet"]["page"]
    assert page[0]["$sort"] == {
        "_score_missing": 1,
        "score.value": -1,
        "prospect_id": 1,
    }
    assert page[1:] == [
        {"$skip": 25},
        {"$limit": 25},
        page[3],
    ]
    assert all("$lookup" not in stage for stage in pipeline)


def test_reused_prospect_is_recomputed_for_each_campaign():
    reused = {
        "_id": ObjectId(),
        "job_title": "Founder",
        "industry": "Software",
        "country": "US",
    }
    matching = {
        "icp_job_titles": ["Founder"],
        "icp_industries": ["Software"],
        "icp_countries": ["US"],
    }
    excluded = {**matching, "icp_exclude_keywords": ["Founder"]}

    first = compute_campaign_score(reused, matching)
    second = compute_campaign_score(reused, excluded)

    assert first["fit_score"] > 0
    assert second["fit_score"] == 0
    assert second["reasoning"].startswith("Excluded:")


async def test_cohort_selection_persists_explicit_ids_without_source_tags():
    collection = SimpleNamespace(bulk_write=AsyncMock())
    prospect_ids = [ObjectId(), ObjectId()]

    await ensure_cohort_membership(
        account_id="tenant",
        campaign_id="campaign",
        prospect_ids=prospect_ids,
        cohort_id="campaign:campaign:selected",
        cohort_label="day1",
        collection=collection,
    )

    operations = collection.bulk_write.await_args.args[0]
    assert len(operations) == 2
    assert {_op_filter(op)["prospect_id"] for op in operations} == {
        str(pid) for pid in prospect_ids
    }
    assert all("source_industry_ids" not in str(_op_update(op)) for op in operations)
    assert all(
        _op_update(op)["$set"]["cohort_id"] == "campaign:campaign:selected"
        for op in operations
    )


async def test_enrichment_transitions_started_completed_and_failed_are_atomic():
    collection = SimpleNamespace(
        find_one_and_update=AsyncMock(return_value={"ok": True})
    )
    common = dict(
        account_id="tenant",
        campaign_id="campaign",
        prospect_id="prospect",
        scoring_version=DEFAULT_SCORING_VERSION,
        collection=collection,
    )

    await transition_enrichment(state="running", **common)
    running_query = collection.find_one_and_update.await_args.args[0]
    running_update = collection.find_one_and_update.await_args.args[1]
    assert running_query["enrichment.state"] == {"$in": ["queued", "retryable_failure"]}
    assert running_update["$set"]["enrichment.started_at"] is not None
    assert running_update["$inc"] == {"enrichment.attempt": 1}

    await transition_enrichment(
        state="succeeded",
        outcome="intelligence_generated",
        result={"prospect_intelligence": {"hook": "recent launch"}},
        **common,
    )
    success_query = collection.find_one_and_update.await_args.args[0]
    success_update = collection.find_one_and_update.await_args.args[1]
    assert success_query["enrichment.state"] == {"$in": ["running"]}
    assert success_update["$set"]["enrichment.completed_at"] is not None
    assert success_update["$set"]["enrichment.result"]["prospect_intelligence"]

    await transition_enrichment(
        state="retryable_failure",
        outcome="pipeline_exception",
        error_code="TimeoutError",
        error_message="provider timed out",
        **common,
    )
    failed_update = collection.find_one_and_update.await_args.args[1]
    assert failed_update["$set"]["enrichment.state"] == "retryable_failure"
    assert failed_update["$set"]["enrichment.error_code"] == "TimeoutError"
    assert failed_update["$set"]["enrichment.completed_at"] is not None
