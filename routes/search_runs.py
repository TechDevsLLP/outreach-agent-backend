from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId
from auth import get_account_context
from database import search_runs_collection

router = APIRouter(prefix="/api/search-runs", tags=["Search Runs"])


@router.get("")
async def list_search_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str = None,
    account_ctx: dict = Depends(get_account_context),
):
    """List search run history with pagination."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    query = {"account_id": account_id}
    if status:
        query["status"] = status

    total = await search_runs_collection.count_documents(query)
    skip = (page - 1) * page_size

    runs = await search_runs_collection.find(query).sort("started_at", -1).skip(skip).limit(page_size).to_list(page_size)

    for run in runs:
        run["_id"] = str(run["_id"])

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "runs": runs,
    }


@router.get("/{run_id}")
async def get_search_run(run_id: str, account_ctx: dict = Depends(get_account_context)):
    """Get details of a specific search run."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    try:
        run = await search_runs_collection.find_one({"_id": ObjectId(run_id), "account_id": account_id})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid run ID")

    if not run:
        raise HTTPException(status_code=404, detail="Search run not found")

    run["_id"] = str(run["_id"])
    return run
