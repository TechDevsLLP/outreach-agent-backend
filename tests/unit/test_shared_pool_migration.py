import gzip
import json
from argparse import Namespace
from pathlib import Path

import pytest
from bson import ObjectId, json_util

from scripts.shared_pool_migration import (
    MigrationError,
    build_bundle,
    inventory_documents,
    logical_digest,
    logical_digest_ordered,
    export_database,
    restore_database,
    transform_documents,
    validate_bundle,
    validate_documents,
)


def _company(identifier: str = "c1", **values):
    return {
        "_id": ObjectId(identifier.rjust(24, "0")),
        "linkedin_url": f"https://linkedin.com/company/{identifier}",
        "name": identifier,
        **values,
    }


def _prospect(identifier: str = "p1", company_id=None, **values):
    number = str(abs(hash(identifier)) % (16**20)).rjust(24, "0")
    return {
        "_id": ObjectId(number),
        "linkedin": f"https://linkedin.com/in/{identifier}",
        "full_name": identifier,
        **({"company_id": str(company_id)} if company_id is not None else {}),
        **values,
    }


def _write(path: Path, documents):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for document in documents:
            handle.write(json_util.dumps(document) + "\n")


def test_tenant_fields_move_to_overlay_and_campaign_history_is_quarantined():
    company = _company()
    prospect = _prospect(
        company_id=company["_id"],
        account_id=ObjectId("1" * 24),
        ai_score=0,
        status="new",
        fit_score=99,
        connection_request_sent_at="legacy",
        stage="assessed",
        enrichment_status="completed",
        prefilter_status="passed",
        disqualify_reason="historical decision",
        optimal_send_time="2026-07-15T10:00:00Z",
    )

    result = transform_documents([company], [prospect])

    canonical = result["prospects"][0]
    assert "account_id" not in canonical
    assert "ai_score" not in canonical
    assert "fit_score" not in canonical
    assert "connection_request_sent_at" not in canonical
    assert "stage" not in canonical
    assert "enrichment_status" not in canonical
    assert "prefilter_status" not in canonical
    assert "disqualify_reason" not in canonical
    assert "optimal_send_time" not in canonical
    assert result["prospect_state"][0]["account_id"] == "1" * 24
    assert result["prospect_state"][0]["ai_score"] == 0
    assert "status" not in result["prospect_state"][0]
    assert {item["reason"] for item in result["quarantine"]} >= {
        "campaign_history_not_migrated",
        "outreach_history_not_migrated",
        "operational_state_not_migrated",
    }
    assert validate_documents(result) == []


def test_ambiguous_tenant_payload_is_never_assigned_to_an_overlay():
    prospect = _prospect(
        account_id="account-a",
        enriched_by_account="account-b",
        ai_score=88,
    )

    result = transform_documents([], [prospect])

    assert result["prospect_state"] == []
    ambiguous = [
        item for item in result["quarantine"]
        if item["reason"] == "ambiguous_tenant_ownership"
    ]
    assert len(ambiguous) == 1
    assert ambiguous[0]["context"]["owner_candidates"] == ["account-a", "account-b"]
    assert "ai_score" not in result["prospects"][0]


def test_nonconflicting_duplicates_merge_and_company_reference_is_remapped():
    first = _company("1", domain="example.com")
    second = {
        "_id": ObjectId("2" * 24),
        "linkedin_url": "HTTP://WWW.LINKEDIN.COM/company/1/",
        "description": "complete profile",
    }
    prospect = _prospect("person", company_id=second["_id"])

    result = transform_documents([first, second], [prospect])

    assert len(result["companies"]) == 1
    winner = result["companies"][0]
    assert winner["domain"] == "example.com"
    assert winner["description"] == "complete profile"
    assert result["prospects"][0]["company_id"] == str(winner["_id"])
    assert validate_documents(result) == []


def test_conflicting_duplicate_identity_and_dependent_prospect_are_quarantined():
    first = _company("1", domain="one.example")
    second = {
        "_id": ObjectId("2" * 24),
        "linkedin_url": first["linkedin_url"],
        "domain": "two.example",
    }
    prospect = _prospect("person", company_id=first["_id"])

    result = transform_documents([first, second], [prospect])

    assert result["companies"] == []
    assert result["prospects"] == []
    reasons = {item["reason"] for item in result["quarantine"]}
    assert "ambiguous_duplicate_identity" in reasons
    assert "broken_or_ambiguous_company_reference" in reasons


def test_inventory_reports_duplicates_leakage_and_broken_references():
    company = _company()
    duplicate = {**company, "_id": ObjectId("2" * 24)}
    prospect = _prospect(company_id=ObjectId("f" * 24), account_id="tenant-a")

    report = inventory_documents([company, duplicate], [prospect])

    assert report["collections"]["companies"]["duplicate_identity_groups"] >= 1
    assert report["collections"]["prospects"]["tenant_leakage_paths"]["account_id"] == 1
    assert report["referential_integrity"]["broken_company_reference_count"] == 1


def test_bundle_hash_tampering_fails_closed(tmp_path):
    source = tmp_path / "source"
    bundle = tmp_path / "bundle"
    source.mkdir()
    company = _company()
    _write(source / "companies.jsonl.gz", [company])
    _write(source / "prospects.jsonl.gz", [_prospect(company_id=company["_id"])])
    manifest = build_bundle(source, bundle)
    assert manifest["restore_confirmation"].startswith("RESTORE:")
    validate_bundle(bundle)

    with gzip.open(bundle / "companies.jsonl.gz", "at", encoding="utf-8") as handle:
        handle.write(json_util.dumps(_company("3")) + "\n")

    with pytest.raises(MigrationError, match="count/hash mismatch"):
        validate_bundle(bundle)


def test_logical_hash_is_order_independent():
    documents = [_company("1"), _company("2")]
    assert logical_digest(documents) == logical_digest(list(reversed(documents)))
    assert logical_digest(documents) == logical_digest_ordered(documents)


def test_nonempty_output_directory_fails_before_writing(tmp_path):
    source = tmp_path / "source"
    bundle = tmp_path / "bundle"
    source.mkdir()
    bundle.mkdir()
    (bundle / "operator-note.txt").write_text("keep", encoding="utf-8")
    _write(source / "companies.jsonl.gz", [])
    _write(source / "prospects.jsonl.gz", [])

    with pytest.raises(MigrationError, match="must not already exist"):
        build_bundle(source, bundle)
    assert (bundle / "operator-note.txt").read_text(encoding="utf-8") == "keep"


def test_missing_identity_is_quarantined_not_restored():
    result = transform_documents(
        [{"_id": ObjectId("1" * 24), "name": "No identity"}],
        [{"_id": ObjectId("2" * 24), "full_name": "No identity"}],
    )

    assert result["companies"] == []
    assert result["prospects"] == []
    assert [item["reason"] for item in result["quarantine"]].count(
        "missing_canonical_identity"
    ) == 2


def test_export_preview_never_requires_credentials_or_connects(capsys):
    export_database(Namespace(execute_read=False, output="unused"))
    assert "DRY RUN" in capsys.readouterr().out


def test_restore_preview_validates_bundle_without_credentials(tmp_path, capsys):
    source = tmp_path / "source"
    bundle = tmp_path / "bundle"
    source.mkdir()
    company = _company()
    _write(source / "companies.jsonl.gz", [company])
    _write(source / "prospects.jsonl.gz", [_prospect(company_id=company["_id"])])
    build_bundle(source, bundle)

    restore_database(Namespace(bundle=str(bundle), execute=False))

    assert "DRY RUN" in capsys.readouterr().out


def test_nested_tenant_markers_are_removed_and_audited():
    company = _company(
        linkedin_data={
            "provider_company_id": "universal",
            "account_id": "tenant-a",
            "employees": [{"name": "A", "workspace_id": "workspace-a"}],
        }
    )

    result = transform_documents([company], [])

    raw = result["companies"][0]["linkedin_data"]
    assert raw == {"provider_company_id": "universal", "employees": [{"name": "A"}]}
    nested = [item for item in result["quarantine"] if item["reason"] == "nested_tenant_leakage"]
    assert len(nested) == 1
    assert nested[0]["payload"] == {
        "linkedin_data.account_id": "tenant-a",
        "linkedin_data.employees[0].workspace_id": "workspace-a",
    }


def test_weak_owner_metadata_is_quarantined_not_guessed():
    result = transform_documents([], [_prospect(user_id="user-without-account")])

    assert result["prospect_state"] == []
    reasons = {item["reason"] for item in result["quarantine"]}
    assert "tenant_metadata_without_unique_account" in reasons


def test_validator_rejects_nested_tenant_leakage_and_overlay_history():
    company = _company(linkedin_data={"nested": {"account_id": "tenant-a"}})
    prospect = _prospect()
    result = {
        "companies": [company],
        "prospects": [prospect],
        "prospect_state": [{
            "_id": "overlay",
            "account_id": "tenant-a",
            "prospect_id": str(prospect["_id"]),
            "outreach_messages": {"body": "must not migrate"},
        }],
        "quarantine": [],
    }

    errors = validate_documents(result)

    assert any("nested tenant fields" in error for error in errors)
    assert any("forbidden fields ['outreach_messages']" in error for error in errors)
