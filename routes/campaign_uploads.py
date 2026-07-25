"""
Upload-a-Lead-List (BYOL) routes.

Two endpoints power the "Upload my list" targeting source inside smart-campaign
creation:

  POST /api/campaigns/uploads/parse          — accept a spreadsheet, parse it
    (pandas, first sheet), enforce the 500-row cap, dedupe identical rows,
    NaN→None, persist a `lead_upload_batches` doc, and return the columns +
    a small sample + an AI-proposed column→field mapping.

  POST /api/campaigns/uploads/{batch_id}/mapping — confirm/clarify the mapping.
    Returns {status:"ready"} once the mapping is complete, or
    {status:"needs_clarification", questions:[...]} to drive the clarifying loop.

Mirrors the pandas-parse pattern at routes/companies.py::scrape_from_excel.
The batch is later consumed by services/byol_discovery_service.run_byol_discovery.
"""

import logging
from datetime import datetime
from io import BytesIO

import pandas as pd
from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field
from pymongo import ReturnDocument

from auth import get_account_context
from database import lead_upload_batches_collection
from services.lead_mapping_service import (
    CANONICAL_FIELDS,
    classify_row,
    propose_column_mapping,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/campaigns/uploads", tags=["Campaign Uploads"])

MAX_ROWS = 500
SAMPLE_ROWS = 5


class MappingConfirmRequest(BaseModel):
    """Body for POST /uploads/{batch_id}/mapping.

    `mapping` — the user's confirmed column→canonical-field map. When supplied
        (non-empty) it is authoritative and the batch becomes `ready`.
    `answers` — incremental clarifying answers keyed by question id ("col::<col>")
        or by column name, value = chosen canonical field. Used by the clarifying
        loop when the full mapping is not yet submitted.
    """
    mapping: dict = Field(default_factory=dict)
    answers: dict = Field(default_factory=dict)


def _batch_oid(batch_id: str) -> ObjectId:
    try:
        return ObjectId(batch_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid batch_id")


@router.post("/parse")
async def parse_upload(
    file: UploadFile = File(...),
    account_ctx: dict = Depends(get_account_context),
):
    """Parse an uploaded .xlsx/.xls/.csv lead list and persist a batch."""
    account_id = str(account_ctx["account"]["_id"])
    user_id = str(account_ctx["user"]["_id"])

    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("xlsx", "xls", "csv"):
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, and .csv files are supported")

    contents = await file.read()
    try:
        if ext == "csv":
            df = pd.read_csv(BytesIO(contents))
        else:
            df = pd.read_excel(BytesIO(contents))  # first sheet by default
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

    if df.empty or len(df.columns) == 0:
        raise HTTPException(status_code=400, detail="The uploaded file has no data rows")

    # Normalize headers to strings, drop fully-empty rows, dedupe identical rows.
    df.columns = [str(c) for c in df.columns]
    df = df.dropna(how="all").drop_duplicates()

    total_after_dedupe = len(df)
    truncated = total_after_dedupe > MAX_ROWS
    if truncated:
        df = df.head(MAX_ROWS)

    # NaN → None so the values persist/serialize cleanly.
    df = df.astype(object).where(pd.notnull(df), None)
    rows = df.to_dict(orient="records")
    columns = list(df.columns)

    if not rows:
        raise HTTPException(status_code=400, detail="No usable rows found in the file")

    sample_rows = rows[:SAMPLE_ROWS]

    # One LLM call (with deterministic fallback) to propose the column mapping.
    proposed_mapping = await propose_column_mapping(columns, sample_rows, account_id=account_id)

    now = datetime.utcnow()
    batch_doc = {
        "account_id": account_id,
        "created_by": user_id,
        "filename": filename,
        "status": "parsed",  # parsed -> ready (after mapping confirmed)
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "rows_truncated": truncated,
        "rows_before_cap": total_after_dedupe,
        "sample_rows": sample_rows,
        "proposed_mapping": proposed_mapping,
        "mapping": None,
        "mapping_answers": None,
        "created_at": now,
        "updated_at": now,
    }
    result = await lead_upload_batches_collection.insert_one(batch_doc)
    batch_id = str(result.inserted_id)

    return {
        "batch_id": batch_id,
        "columns": columns,
        "sample_rows": sample_rows,
        "row_count": len(rows),
        "rows_truncated": truncated,
        "proposed_mapping": proposed_mapping,
    }


def _apply_answers(mapping: dict, answers: dict) -> dict:
    """Overlay clarifying answers (id 'col::<col>' or bare column) onto a mapping."""
    merged = dict(mapping or {})
    for key, field in (answers or {}).items():
        col = key[len("col::"):] if isinstance(key, str) and key.startswith("col::") else key
        if field in CANONICAL_FIELDS:
            merged[col] = field
    return merged


@router.post("/{batch_id}/mapping")
async def confirm_mapping(
    batch_id: str,
    body: MappingConfirmRequest,
    account_ctx: dict = Depends(get_account_context),
):
    """Confirm the column mapping (or continue the clarifying loop)."""
    account_id = str(account_ctx["account"]["_id"])
    oid = _batch_oid(batch_id)

    batch = await lead_upload_batches_collection.find_one(
        {"_id": oid, "account_id": account_id}
    )
    if not batch:
        raise HTTPException(status_code=404, detail="Upload batch not found")

    proposed = batch.get("proposed_mapping") or {}
    proposed_map = proposed.get("mapping") or {}
    proposed_questions = proposed.get("questions") or []

    # Start from the proposed mapping, overlay any user-submitted mapping + answers.
    merged = dict(proposed_map)
    if body.mapping:
        merged.update(body.mapping)
    merged = _apply_answers(merged, body.answers)

    # Validate every value is a known canonical field.
    bad = {c: f for c, f in merged.items() if f not in CANONICAL_FIELDS}
    if bad:
        raise HTTPException(status_code=400, detail=f"Invalid field mapping values: {bad}")

    # Determine which columns the user has explicitly resolved this round.
    resolved_cols = set(body.mapping.keys())
    for key in (body.answers or {}).keys():
        resolved_cols.add(key[len("col::"):] if isinstance(key, str) and key.startswith("col::") else key)

    # If the user submitted a full mapping, treat it as authoritative → ready.
    # Otherwise, any still-unresolved low-confidence question re-prompts.
    if not body.mapping:
        remaining = [q for q in proposed_questions if q.get("column") not in resolved_cols]
        if remaining:
            await lead_upload_batches_collection.update_one(
                {"_id": oid, "account_id": account_id},
                {"$set": {
                    "mapping": merged,
                    "mapping_answers": body.answers or {},
                    "updated_at": datetime.utcnow(),
                }},
            )
            return {"status": "needs_clarification", "questions": remaining}

    # Ready. Compute a cheap classification preview so the UI can set expectations.
    rows = batch.get("rows") or []
    counts = {"person": 0, "company": 0, "unresolvable": 0}
    for r in rows:
        counts[classify_row(r, merged)] += 1

    await lead_upload_batches_collection.find_one_and_update(
        {"_id": oid, "account_id": account_id},
        {"$set": {
            "mapping": merged,
            "mapping_answers": body.answers or {},
            "status": "ready",
            "classification_preview": counts,
            "updated_at": datetime.utcnow(),
        }},
        return_document=ReturnDocument.AFTER,
    )

    return {
        "status": "ready",
        "batch_id": batch_id,
        "mapping": merged,
        "row_count": len(rows),
        "classification_preview": counts,
    }
