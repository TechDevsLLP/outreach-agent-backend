"""Safe, artifact-first migration for OutFlo's preserved shared data.

Only ``companies`` and ``prospects`` are canonical preservation inputs.  Any
tenant data found on a prospect is separated into a new ``prospect_state``
overlay when exactly one account can be proven.  Ambiguous or unsupported data
is written to quarantine and is never silently assigned to a tenant.

The normal workflow is intentionally filesystem-only::

    python scripts/shared_pool_migration.py inventory --input backup --output inventory.json
    python scripts/shared_pool_migration.py transform --input backup --output bundle
    python scripts/shared_pool_migration.py validate --bundle bundle

``export`` is read-only and requires an explicit opt-in.  ``restore`` only
writes to a completely empty target database and requires a manifest-derived
confirmation token.  The tool never updates or drops a source database.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional

from bson import json_util
from pymongo import ASCENDING, IndexModel, MongoClient


FORMAT_VERSION = 1
PRESERVED_COLLECTIONS = ("companies", "prospects")
GENERATED_COLLECTIONS = ("companies", "prospects", "prospect_state", "quarantine")

COMPANY_FIELDS = {
    "_id", "linkedin_url", "linkedin_uid", "name", "website", "domain",
    "description", "tagline", "specialties", "employee_count", "employee_band",
    "follower_count", "founded_year", "company_type", "phone", "industry",
    "location", "geo", "profile_vec", "profile_vec_text", "profile_vec_model",
    "profile_vec_at", "linkedin_data", "website_data", "competitors",
    "competitors_last_fetched", "created_at", "last_updated_at", "last_scraped_at",
}

PROSPECT_FIELDS = {
    "_id", "linkedin", "linkedin_uid", "email", "personal_email", "mobile_number",
    "phone_numbers", "phone_source", "phone_unlocked_at", "first_name", "last_name",
    "full_name", "job_title", "headline", "seniority", "function", "title_vec",
    "title_vec_text", "title_vec_model", "title_vec_at", "company_id",
    "company_linkedin", "company_domain", "company_name", "company_industry_id",
    "company_industry_group", "company_employee_band", "company_size",
    "company_description", "location", "geo", "linkedin_profile_data", "source",
    "timezone", "linkedin_posts", "prospect_intelligence_base",
    "first_seen_at", "last_updated_at",
}

# These values are valid only in a tenant overlay.  Campaign fit belongs in
# campaign_prospect_state and is deliberately quarantined rather than migrated.
OVERLAY_FIELDS = {
    "ai_score", "ai_score_breakdown", "prospect_score", "priority_tier",
    "prospect_intelligence", "pitch", "company_news", "competitors", "posts", "tags",
}
CAMPAIGN_FIELDS = {
    "campaign_id", "campaign_ids", "ai_prospect_score", "ai_match_score", "fit_score",
    "fit_score_breakdown", "score", "score_reason", "scoring_version",
    "campaign_score", "campaign_fit", "generated_messages", "message_drafts",
}
OUTREACH_HISTORY_FIELDS = {
    "outreach_messages", "status", "used_by",
    "connection_request_sent_at", "connection_accepted_at",
    "connection_followup_sent_at", "connection_followup_message_id",
    "last_contacted_at", "scheduled_outreach_date", "source_search_run_ids",
    "source_actor_id", "discovered_from_prospect_id", "discovery_reasoning",
    "enrichment_run_id",
}
OPERATIONAL_FIELDS = {
    "stage", "enrichment_status", "enrichment_error", "enrichment_started_at",
    "enrichment_completed_at", "prefilter_status", "prefilter_confidence",
    "prefilter_reject_reason", "title_gate_status", "disqualify_reason",
    "optimal_send_time",
}
OWNER_FIELDS = (
    "account_id", "tenant_id", "enriched_by_account", "phone_unlocked_by_account"
)
TENANT_MARKERS = set(OWNER_FIELDS) | {"user_id", "owner_id", "workspace_id"}


class MigrationError(RuntimeError):
    """A safety or validation condition prevented migration."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _id(value: Any) -> str:
    return "" if value is None else str(value)


def _nonempty(value: Any) -> bool:
    return value not in (None, "", [], {})


def normalize_email(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    return value or None


def normalize_linkedin(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip().lower().replace("\\", "/")
    value = re.sub(r"^https?://", "", value)
    value = re.sub(r"^www\.", "", value)
    value = value.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return value or None


def normalize_domain(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    value = re.sub(r"^https?://", "", value)
    value = re.sub(r"^www\.", "", value)
    value = value.split("/", 1)[0].split("?", 1)[0].rstrip(".")
    return value or None


def canonical_json(document: Mapping[str, Any]) -> str:
    """Stable canonical Extended JSON used by manifests and restore checks."""
    return json_util.dumps(
        document,
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
        separators=(",", ":"),
    )


def logical_digest(documents: Iterable[Mapping[str, Any]]) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for document in sorted(documents, key=lambda item: _id(item.get("_id"))):
        digest.update(canonical_json(document).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def logical_digest_ordered(documents: Iterable[Mapping[str, Any]]) -> tuple[int, str]:
    """Hash an already `_id`-ordered stream without materializing it."""
    digest = hashlib.sha256()
    count = 0
    for document in documents:
        digest.update(canonical_json(document).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise MigrationError(f"required artifact does not exist: {path}")
    opener = gzip.open if path.suffix == ".gz" else open
    documents: list[dict[str, Any]] = []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json_util.loads(line)
            except Exception as exc:
                raise MigrationError(f"invalid Extended JSON at {path}:{line_number}") from exc
            if not isinstance(value, dict):
                raise MigrationError(f"non-document value at {path}:{line_number}")
            documents.append(value)
    return documents


def load_source_export(input_dir: Path) -> tuple[list[dict], list[dict], Optional[dict]]:
    """Load the two preserved collections and verify an export manifest if present."""
    companies = read_jsonl(collection_path(input_dir, "companies"))
    prospects = read_jsonl(collection_path(input_dir, "prospects"))
    manifest_path = input_dir / "export-manifest.json"
    if not manifest_path.exists():
        return companies, prospects, None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format_version") != FORMAT_VERSION:
        raise MigrationError("unsupported source export format version")
    for name, documents in (("companies", companies), ("prospects", prospects)):
        metadata = manifest.get("collections", {}).get(name)
        count, digest = logical_digest(documents)
        if not metadata or count != metadata.get("count") or digest != metadata.get("sha256"):
            raise MigrationError(f"source export count/hash mismatch for {name}")
    return companies, prospects, manifest


def write_jsonl(path: Path, documents: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(documents, key=lambda item: _id(item.get("_id")))
    digest = hashlib.sha256()
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as handle:
        for document in ordered:
            line = canonical_json(document)
            handle.write(line + "\n")
            digest.update(line.encode("utf-8"))
            digest.update(b"\n")
    return {"file": path.name, "count": len(ordered), "sha256": digest.hexdigest()}


def write_jsonl_ordered(path: Path, documents: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Write an already `_id`-ordered cursor using bounded memory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", compresslevel=6) as handle:
        for document in documents:
            line = canonical_json(document)
            handle.write(line + "\n")
            digest.update(line.encode("utf-8"))
            digest.update(b"\n")
            count += 1
    return {"file": path.name, "count": count, "sha256": digest.hexdigest()}


def collection_path(directory: Path, name: str) -> Path:
    compressed = directory / f"{name}.jsonl.gz"
    return compressed if compressed.exists() else directory / f"{name}.jsonl"


def ownership_candidates(document: Mapping[str, Any]) -> set[str]:
    candidates: set[str] = set()
    for field in OWNER_FIELDS:
        value = document.get(field)
        values = value if isinstance(value, list) else [value]
        for item in values:
            if _nonempty(item):
                candidates.add(str(item))
    return candidates


def _strip_nested_tenant_markers(value: Any, prefix: str = "") -> tuple[Any, dict[str, Any]]:
    """Remove tenant marker keys recursively while preserving an audit payload."""
    residue: dict[str, Any] = {}
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else key
            if key in TENANT_MARKERS:
                residue[path] = copy.deepcopy(nested)
                continue
            cleaned_value, nested_residue = _strip_nested_tenant_markers(nested, path)
            cleaned[key] = cleaned_value
            residue.update(nested_residue)
        return cleaned, residue
    if isinstance(value, list):
        cleaned_list: list[Any] = []
        for index, nested in enumerate(value):
            cleaned_value, nested_residue = _strip_nested_tenant_markers(
                nested, f"{prefix}[{index}]"
            )
            cleaned_list.append(cleaned_value)
            residue.update(nested_residue)
        return cleaned_list, residue
    return copy.deepcopy(value), residue


def _field_paths(value: Any, prefix: str = "") -> Iterator[tuple[str, str]]:
    if isinstance(value, dict):
        for key, nested in value.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path, type(nested).__name__
            yield from _field_paths(nested, path)
    elif isinstance(value, list):
        for nested in value[:10]:
            yield from _field_paths(nested, f"{prefix}[]")


def identity_keys(collection: str, document: Mapping[str, Any]) -> list[str]:
    if collection == "companies":
        linkedin = normalize_linkedin(document.get("linkedin_url"))
        domain = normalize_domain(document.get("domain") or document.get("website"))
        uid = str(document.get("linkedin_uid") or "").strip()
        return [key for key in (f"linkedin:{linkedin}" if linkedin else None,
                                f"domain:{domain}" if domain else None,
                                f"linkedin_uid:{uid}" if uid else None) if key]
    linkedin = normalize_linkedin(document.get("linkedin") or document.get("linkedin_url"))
    email = normalize_email(document.get("email"))
    uid = str(document.get("linkedin_uid") or "").strip()
    return [key for key in (f"linkedin:{linkedin}" if linkedin else None,
                            f"email:{email}" if email else None,
                            f"linkedin_uid:{uid}" if uid else None) if key]


def inventory_documents(companies: list[dict], prospects: list[dict]) -> dict[str, Any]:
    report: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "generated_at": _now(),
        "collections": {},
        "referential_integrity": {},
    }
    for name, documents in (("companies", companies), ("prospects", prospects)):
        field_counts: Counter[str] = Counter()
        field_types: dict[str, Counter[str]] = defaultdict(Counter)
        identity_counts: Counter[str] = Counter()
        leakage: Counter[str] = Counter()
        owner_sets: Counter[str] = Counter()
        for document in documents:
            for path, kind in _field_paths(document):
                field_counts[path] += 1
                field_types[path][kind] += 1
                if path.split(".")[-1].replace("[]", "") in TENANT_MARKERS:
                    leakage[path] += 1
            keys = identity_keys(name, document)
            identity_counts.update(keys)
            owners = ownership_candidates(document)
            owner_sets["none" if not owners else "one" if len(owners) == 1 else "ambiguous"] += 1
        duplicates = {key: count for key, count in identity_counts.items() if count > 1}
        report["collections"][name] = {
            "count": len(documents),
            "missing_identity": sum(1 for item in documents if not identity_keys(name, item)),
            "duplicate_identity_groups": len(duplicates),
            "duplicate_identity_documents": sum(duplicates.values()),
            "duplicate_identities": dict(sorted(duplicates.items())),
            "ownership_evidence": dict(owner_sets),
            "tenant_leakage_paths": dict(sorted(leakage.items())),
            "field_counts": dict(sorted(field_counts.items())),
            "field_types": {
                key: dict(sorted(value.items())) for key, value in sorted(field_types.items())
            },
        }
    company_ids = {_id(item.get("_id")) for item in companies}
    references = [_id(item.get("company_id")) for item in prospects if item.get("company_id")]
    broken = sorted({value for value in references if value not in company_ids})
    report["referential_integrity"] = {
        "prospects_with_company_id": len(references),
        "broken_company_reference_count": sum(value not in company_ids for value in references),
        "broken_company_ids": broken,
    }
    return report


def _quarantine(collection: str, source_id: Any, reason: str, payload: Any, **context: Any) -> dict:
    stable = canonical_json({"collection": collection, "source_id": _id(source_id), "reason": reason, "context": context})
    return {
        "_id": hashlib.sha256(stable.encode()).hexdigest(),
        "collection": collection,
        "source_id": _id(source_id),
        "reason": reason,
        "payload": copy.deepcopy(payload),
        "context": context,
    }


def _canonicalize(collection: str, document: Mapping[str, Any]) -> tuple[dict, list[dict], Optional[dict]]:
    allowed = COMPANY_FIELDS if collection == "companies" else PROSPECT_FIELDS
    source_id = document.get("_id")
    canonical = {key: copy.deepcopy(value) for key, value in document.items() if key in allowed}
    # Sparse unique indexes treat an explicit null as an indexed value.  Omit
    # absent identities so a clean restore cannot fail on repeated nulls.
    identity_fields = ("linkedin_url",) if collection == "companies" else ("linkedin", "email")
    for field in identity_fields:
        if not _nonempty(canonical.get(field)):
            canonical.pop(field, None)
    if collection == "prospects" and "email" in canonical:
        canonical["email"] = normalize_email(canonical["email"])
    quarantine: list[dict] = []
    overlay: Optional[dict] = None

    canonical, nested_tenant_residue = _strip_nested_tenant_markers(canonical)
    if nested_tenant_residue:
        quarantine.append(_quarantine(
            collection, source_id, "nested_tenant_leakage", nested_tenant_residue
        ))

    owners = ownership_candidates(document)
    tenant_payload = {key: copy.deepcopy(document[key]) for key in OVERLAY_FIELDS if key in document}
    attribution = {
        key: str(document[key]) for key in OWNER_FIELDS if _nonempty(document.get(key))
    }
    if collection == "prospects" and (tenant_payload or owners):
        if len(owners) == 1:
            account_id = next(iter(owners))
            overlay = {
                "_id": hashlib.sha256(f"{account_id}:{_id(source_id)}".encode()).hexdigest(),
                "account_id": account_id,
                "prospect_id": _id(source_id),
                **tenant_payload,
                "migration_evidence": {"source": "canonical_leakage", "attribution": attribution},
            }
        else:
            quarantine.append(_quarantine(
                collection, source_id,
                "tenant_payload_without_unique_owner" if not owners else "ambiguous_tenant_ownership",
                tenant_payload, owner_candidates=sorted(owners),
            ))
    root_tenant_metadata = {
        key: copy.deepcopy(document[key]) for key in TENANT_MARKERS if key in document
    }
    if collection == "prospects" and root_tenant_metadata and len(owners) != 1:
        quarantine.append(_quarantine(
            collection, source_id, "tenant_metadata_without_unique_account",
            root_tenant_metadata, owner_candidates=sorted(owners),
        ))

    campaign_payload = {key: copy.deepcopy(document[key]) for key in CAMPAIGN_FIELDS if key in document}
    if campaign_payload:
        quarantine.append(_quarantine(
            collection, source_id, "campaign_history_not_migrated", campaign_payload
        ))
    outreach_payload = {
        key: copy.deepcopy(document[key]) for key in OUTREACH_HISTORY_FIELDS if key in document
    }
    if outreach_payload:
        quarantine.append(_quarantine(
            collection, source_id, "outreach_history_not_migrated", outreach_payload
        ))

    operational_payload = {
        key: copy.deepcopy(document[key]) for key in OPERATIONAL_FIELDS if key in document
    }
    if operational_payload:
        quarantine.append(_quarantine(
            collection, source_id, "operational_state_not_migrated", operational_payload
        ))

    known = (
        allowed | OVERLAY_FIELDS | CAMPAIGN_FIELDS | OUTREACH_HISTORY_FIELDS
        | OPERATIONAL_FIELDS | TENANT_MARKERS
    )
    unsupported = {key: copy.deepcopy(value) for key, value in document.items() if key not in known}
    if unsupported:
        quarantine.append(_quarantine(
            collection, source_id, "unsupported_canonical_fields", unsupported
        ))
    if collection == "companies":
        tenant_payload = {
            key: copy.deepcopy(value) for key, value in document.items()
            if key in OVERLAY_FIELDS or key in TENANT_MARKERS
        }
        if tenant_payload:
            quarantine.append(_quarantine(
                collection, source_id, "company_tenant_payload_has_no_runtime_overlay",
                tenant_payload, owner_candidates=sorted(owners),
            ))
    return canonical, quarantine, overlay


def _critical_conflicts(collection: str, documents: list[dict]) -> dict[str, list[Any]]:
    fields = ("linkedin_url", "domain") if collection == "companies" else ("linkedin", "email", "company_id")
    conflicts: dict[str, list[Any]] = {}
    normalizers = {
        "linkedin_url": normalize_linkedin,
        "linkedin": normalize_linkedin,
        "domain": normalize_domain,
        "email": normalize_email,
        "company_id": lambda value: _id(value) or None,
    }
    for field in fields:
        values: dict[str, Any] = {}
        for document in documents:
            value = document.get(field)
            normalized = normalizers[field](value)
            if normalized:
                values[normalized] = value
        if len(values) > 1:
            conflicts[field] = list(values.values())
    return conflicts


def _deduplicate(collection: str, documents: list[dict]) -> tuple[list[dict], dict[str, str], list[dict]]:
    """Merge only non-conflicting identity components; quarantine ambiguous groups."""
    parent = list(range(len(documents)))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    seen: dict[str, int] = {}
    for index, document in enumerate(documents):
        for key in identity_keys(collection, document):
            if key in seen:
                union(index, seen[key])
            else:
                seen[key] = index

    groups: dict[int, list[dict]] = defaultdict(list)
    for index, document in enumerate(documents):
        groups[find(index)].append(document)

    output: list[dict] = []
    id_map: dict[str, str] = {}
    quarantine: list[dict] = []
    for group in groups.values():
        if len(group) == 1:
            output.append(group[0])
            id_map[_id(group[0].get("_id"))] = _id(group[0].get("_id"))
            continue
        conflicts = _critical_conflicts(collection, group)
        if conflicts:
            quarantine.append(_quarantine(
                collection, ",".join(sorted(_id(item.get("_id")) for item in group)),
                "ambiguous_duplicate_identity", group, conflicts=conflicts,
            ))
            continue
        winner = sorted(
            group,
            key=lambda item: (-sum(_nonempty(value) for value in item.values()), _id(item.get("_id"))),
        )[0]
        merged = copy.deepcopy(winner)
        for document in sorted(group, key=lambda item: _id(item.get("_id"))):
            id_map[_id(document.get("_id"))] = _id(winner.get("_id"))
            for key, value in document.items():
                if not _nonempty(merged.get(key)) and _nonempty(value):
                    merged[key] = copy.deepcopy(value)
        output.append(merged)
    return output, id_map, quarantine


def transform_documents(
    companies: list[dict], prospects: list[dict]
) -> dict[str, list[dict]]:
    """Pure transformation used by the CLI and unit tests."""
    canonical_companies: list[dict] = []
    canonical_prospects: list[dict] = []
    overlays: list[dict] = []
    quarantine: list[dict] = []

    for document in companies:
        canonical, rejected, _ = _canonicalize("companies", document)
        if not identity_keys("companies", canonical):
            quarantine.extend(rejected)
            quarantine.append(_quarantine(
                "companies", canonical.get("_id"), "missing_canonical_identity", canonical
            ))
            continue
        canonical_companies.append(canonical)
        quarantine.extend(rejected)
    canonical_companies, company_map, rejected = _deduplicate("companies", canonical_companies)
    quarantine.extend(rejected)
    surviving_company_ids = {_id(item.get("_id")) for item in canonical_companies}

    for document in prospects:
        canonical, rejected, overlay = _canonicalize("prospects", document)
        if not identity_keys("prospects", canonical):
            quarantine.extend(rejected)
            if overlay:
                quarantine.append(_quarantine(
                    "prospect_state", overlay.get("_id"), "overlay_for_quarantined_prospect", overlay
                ))
            quarantine.append(_quarantine(
                "prospects", canonical.get("_id"), "missing_canonical_identity", canonical
            ))
            continue
        old_company_id = _id(canonical.get("company_id"))
        if old_company_id:
            mapped = company_map.get(old_company_id)
            if mapped:
                canonical["company_id"] = mapped
            if not mapped or mapped not in surviving_company_ids:
                quarantine.extend(rejected)
                if overlay:
                    quarantine.append(_quarantine(
                        "prospect_state", overlay.get("_id"), "overlay_for_quarantined_prospect", overlay
                    ))
                quarantine.append(_quarantine(
                    "prospects", canonical.get("_id"), "broken_or_ambiguous_company_reference",
                    canonical, original_company_id=old_company_id,
                ))
                continue
        canonical_prospects.append(canonical)
        quarantine.extend(rejected)
        if overlay:
            overlays.append(overlay)

    canonical_prospects, prospect_map, rejected = _deduplicate("prospects", canonical_prospects)
    quarantine.extend(rejected)
    surviving_prospect_ids = {_id(item.get("_id")) for item in canonical_prospects}

    final_overlays: dict[tuple[str, str], dict] = {}
    for overlay in overlays:
        prospect_id = prospect_map.get(overlay["prospect_id"])
        if not prospect_id or prospect_id not in surviving_prospect_ids:
            quarantine.append(_quarantine(
                "prospect_state", overlay.get("_id"), "overlay_for_quarantined_prospect", overlay
            ))
            continue
        overlay["prospect_id"] = prospect_id
        overlay["_id"] = hashlib.sha256(
            f"{overlay['account_id']}:{prospect_id}".encode()
        ).hexdigest()
        key = (overlay["account_id"], prospect_id)
        if key in final_overlays:
            existing = final_overlays[key]
            conflicting = {
                field for field in OVERLAY_FIELDS
                if _nonempty(existing.get(field)) and _nonempty(overlay.get(field))
                and existing.get(field) != overlay.get(field)
            }
            if conflicting:
                quarantine.append(_quarantine(
                    "prospect_state", overlay["_id"], "conflicting_overlay_values",
                    [existing, overlay], fields=sorted(conflicting),
                ))
                del final_overlays[key]
                continue
            existing.update({key: value for key, value in overlay.items() if _nonempty(value)})
        else:
            final_overlays[key] = overlay

    return {
        "companies": canonical_companies,
        "prospects": canonical_prospects,
        "prospect_state": list(final_overlays.values()),
        "quarantine": quarantine,
    }


def validate_documents(result: Mapping[str, list[dict]]) -> list[str]:
    errors: list[str] = []
    companies = result.get("companies", [])
    prospects = result.get("prospects", [])
    overlays = result.get("prospect_state", [])
    company_ids = {_id(item.get("_id")) for item in companies}
    prospect_ids = {_id(item.get("_id")) for item in prospects}

    for name, documents, allowed in (
        ("companies", companies, COMPANY_FIELDS), ("prospects", prospects, PROSPECT_FIELDS)
    ):
        ids = [_id(item.get("_id")) for item in documents]
        if "" in ids:
            errors.append(f"{name}: document without _id")
        if len(ids) != len(set(ids)):
            errors.append(f"{name}: duplicate _id")
        for document in documents:
            extra = set(document) - allowed
            if extra:
                errors.append(f"{name}/{_id(document.get('_id'))}: forbidden fields {sorted(extra)}")
            if not identity_keys(name, document):
                errors.append(f"{name}/{_id(document.get('_id'))}: missing canonical identity")
            nested_leakage = [
                path for path, _ in _field_paths(document)
                if path.split(".")[-1].replace("[]", "") in TENANT_MARKERS
            ]
            if nested_leakage:
                errors.append(
                    f"{name}/{_id(document.get('_id'))}: nested tenant fields {sorted(nested_leakage)}"
                )
        keys = [key for item in documents for key in identity_keys(name, item)]
        duplicates = [key for key, count in Counter(keys).items() if count > 1]
        if duplicates:
            errors.append(f"{name}: duplicate canonical identities {sorted(duplicates)}")

    for prospect in prospects:
        company_id = _id(prospect.get("company_id"))
        if company_id and company_id not in company_ids:
            errors.append(f"prospects/{_id(prospect.get('_id'))}: broken company_id {company_id}")
    overlay_keys: set[tuple[str, str]] = set()
    overlay_allowed = {"_id", "account_id", "prospect_id", "migration_evidence"} | OVERLAY_FIELDS
    for overlay in overlays:
        account_id, prospect_id = overlay.get("account_id"), overlay.get("prospect_id")
        extra = set(overlay) - overlay_allowed
        if extra:
            errors.append(
                f"prospect_state/{_id(overlay.get('_id'))}: forbidden fields {sorted(extra)}"
            )
        if not isinstance(account_id, str) or not account_id:
            errors.append(f"prospect_state/{_id(overlay.get('_id'))}: invalid account_id")
        if prospect_id not in prospect_ids:
            errors.append(f"prospect_state/{_id(overlay.get('_id'))}: missing prospect {prospect_id}")
        key = (account_id, prospect_id)
        if key in overlay_keys:
            errors.append(f"prospect_state: duplicate overlay {key}")
        overlay_keys.add(key)
    return errors


def build_bundle(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    if output_dir.exists():
        raise MigrationError(f"output directory must not already exist: {output_dir}")
    companies, prospects, export_manifest = load_source_export(input_dir)
    source_inventory = inventory_documents(companies, prospects)
    result = transform_documents(companies, prospects)
    errors = validate_documents(result)
    if errors:
        raise MigrationError("transformed bundle failed validation: " + "; ".join(errors))

    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    collections: dict[str, Any] = {}
    for name in GENERATED_COLLECTIONS:
        collections[name] = write_jsonl(output_dir / f"{name}.jsonl.gz", result[name])
        (output_dir / collections[name]["file"]).chmod(0o600)
    reasons = Counter(item["reason"] for item in result["quarantine"])
    manifest = {
        "format_version": FORMAT_VERSION,
        "created_at": _now(),
        "scope": {
            "preserved": ["companies", "prospects"],
            "generated": ["prospect_state"],
            "excluded": "all campaign, conversation, notification, usage, and outreach history",
        },
        "source_inventory": source_inventory,
        "source_export": (
            {
                "database": export_manifest.get("database"),
                "created_at": export_manifest.get("created_at"),
                "collections": export_manifest.get("collections"),
            }
            if export_manifest is not None else None
        ),
        "collections": collections,
        "quarantine_reasons": dict(sorted(reasons.items())),
    }
    token_basis = canonical_json({key: value for key, value in manifest.items() if key != "created_at"})
    manifest["bundle_sha256"] = hashlib.sha256(token_basis.encode()).hexdigest()
    manifest["restore_confirmation"] = f"RESTORE:{manifest['bundle_sha256'][:16]}"
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest_path.chmod(0o600)
    return manifest


def validate_bundle(bundle_dir: Path) -> dict[str, Any]:
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.exists():
        raise MigrationError("bundle manifest.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format_version") != FORMAT_VERSION:
        raise MigrationError("unsupported bundle format version")
    token_payload = {
        key: value for key, value in manifest.items()
        if key not in {"created_at", "bundle_sha256", "restore_confirmation"}
    }
    computed_bundle_sha = hashlib.sha256(canonical_json(token_payload).encode()).hexdigest()
    if computed_bundle_sha != manifest.get("bundle_sha256"):
        raise MigrationError("bundle manifest hash mismatch")
    if manifest.get("restore_confirmation") != f"RESTORE:{computed_bundle_sha[:16]}":
        raise MigrationError("bundle restore token mismatch")
    result: dict[str, list[dict]] = {}
    for name in GENERATED_COLLECTIONS:
        metadata = manifest.get("collections", {}).get(name)
        if not metadata:
            raise MigrationError(f"manifest is missing {name}")
        documents = read_jsonl(bundle_dir / metadata["file"])
        count, digest = logical_digest(documents)
        if count != metadata["count"] or digest != metadata["sha256"]:
            raise MigrationError(f"count/hash mismatch for {name}")
        result[name] = documents
    errors = validate_documents(result)
    if errors:
        raise MigrationError("bundle validation failed: " + "; ".join(errors))
    return manifest


def export_database(args: argparse.Namespace) -> None:
    if not args.execute_read:
        print("DRY RUN: would read only companies and prospects; pass --execute-read to export")
        return
    output = Path(args.output)
    if output.exists():
        raise MigrationError(f"output directory must not already exist: {output}")
    output.mkdir(parents=True, exist_ok=True)
    output.chmod(0o700)
    mongo_uri = os.environ.get(args.mongo_uri_env)
    if not mongo_uri:
        raise MigrationError(f"MongoDB URI environment variable is missing: {args.mongo_uri_env}")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000, appname="outflo-shared-pool-export")
    try:
        database = client[args.database]
        client.admin.command("ping")
        collections: dict[str, Any] = {}
        for name in PRESERVED_COLLECTIONS:
            cursor = database[name].find({}).sort("_id", ASCENDING)
            collections[name] = write_jsonl_ordered(output / f"{name}.jsonl.gz", cursor)
            (output / collections[name]["file"]).chmod(0o600)
        # Detect concurrent source changes before approving the export.  The
        # production runbook also freezes canonical ingestion so cross-
        # collection references cannot move between these passes.
        for name in PRESERVED_COLLECTIONS:
            cursor = database[name].find({}).sort("_id", ASCENDING)
            count, digest = logical_digest_ordered(cursor)
            metadata = collections[name]
            if count != metadata["count"] or digest != metadata["sha256"]:
                raise MigrationError(
                    f"source changed during export for {name}; discard this directory and retry"
                )
        manifest = {
            "format_version": FORMAT_VERSION,
            "created_at": _now(),
            "database": args.database,
            "read_only": True,
            "collections": collections,
        }
        manifest_path = output / "export-manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )
        manifest_path.chmod(0o600)
    finally:
        client.close()


def restore_database(args: argparse.Namespace) -> None:
    manifest = validate_bundle(Path(args.bundle))
    expected = manifest["restore_confirmation"]
    if not args.execute:
        print(f"DRY RUN: validated bundle; empty-target restore requires --execute --confirmation {expected}")
        return
    if args.confirmation != expected:
        raise MigrationError("restore confirmation token does not match this bundle")
    mongo_uri = os.environ.get(args.mongo_uri_env)
    if not mongo_uri:
        raise MigrationError(f"MongoDB URI environment variable is missing: {args.mongo_uri_env}")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000, appname="outflo-shared-pool-restore")
    try:
        database = client[args.database]
        client.admin.command("ping")
        existing = [name for name in database.list_collection_names() if not name.startswith("system.")]
        if existing:
            raise MigrationError(
                f"restore target must be empty; found collections: {sorted(existing)}"
            )
        for name in ("companies", "prospects", "prospect_state"):
            metadata = manifest["collections"][name]
            documents = read_jsonl(Path(args.bundle) / metadata["file"])
            if documents:
                database[name].insert_many(documents, ordered=True)
        database["companies"].create_indexes([
            IndexModel([("linkedin_url", ASCENDING)], unique=True, sparse=True),
            IndexModel([("domain", ASCENDING)], sparse=True),
        ])
        database["prospects"].create_indexes([
            IndexModel([("linkedin", ASCENDING)], unique=True, sparse=True),
            IndexModel([("email", ASCENDING)], unique=True, sparse=True),
            IndexModel([("company_id", ASCENDING)]),
        ])
        database["prospect_state"].create_index(
            [("account_id", ASCENDING), ("prospect_id", ASCENDING)], unique=True
        )
        for name in ("companies", "prospects", "prospect_state"):
            restored = database[name].find({}).sort("_id", ASCENDING)
            count, digest = logical_digest_ordered(restored)
            metadata = manifest["collections"][name]
            if count != metadata["count"] or digest != metadata["sha256"]:
                raise MigrationError(f"post-restore verification failed for {name}")
        database["migration_receipts"].insert_one({
            "_id": manifest["bundle_sha256"],
            "bundle_sha256": manifest["bundle_sha256"],
            "restored_at": datetime.now(timezone.utc),
            "counts": {name: manifest["collections"][name]["count"] for name in GENERATED_COLLECTIONS},
        })
    finally:
        client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser("inventory", help="inspect filesystem export without mutation")
    inventory.add_argument("--input", required=True)
    inventory.add_argument("--output", required=True)

    transform = subparsers.add_parser("transform", help="build a validated restore bundle")
    transform.add_argument("--input", required=True)
    transform.add_argument("--output", required=True)

    validate = subparsers.add_parser("validate", help="verify bundle hashes and invariants")
    validate.add_argument("--bundle", required=True)

    export = subparsers.add_parser("export", help="read only preserved source collections")
    export.add_argument("--mongo-uri-env", default="OUTFLO_MIGRATION_MONGODB_URI")
    export.add_argument("--database", required=True)
    export.add_argument("--output", required=True)
    export.add_argument("--execute-read", action="store_true")

    restore = subparsers.add_parser("restore", help="restore into a completely empty database")
    restore.add_argument("--bundle", required=True)
    restore.add_argument("--mongo-uri-env", default="OUTFLO_MIGRATION_MONGODB_URI")
    restore.add_argument("--database", required=True)
    restore.add_argument("--execute", action="store_true")
    restore.add_argument("--confirmation")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "inventory":
            companies, prospects, _ = load_source_export(Path(args.input))
            report = inventory_documents(companies, prospects)
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
            output.chmod(0o600)
            print(f"inventory written to {args.output}")
        elif args.command == "transform":
            manifest = build_bundle(Path(args.input), Path(args.output))
            print(f"bundle validated; restore token: {manifest['restore_confirmation']}")
        elif args.command == "validate":
            manifest = validate_bundle(Path(args.bundle))
            print(f"bundle valid: {manifest['bundle_sha256']}")
        elif args.command == "export":
            export_database(args)
        elif args.command == "restore":
            restore_database(args)
        return 0
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
