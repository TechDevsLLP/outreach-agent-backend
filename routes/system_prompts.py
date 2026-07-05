"""System prompt management API routes."""

from datetime import datetime
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from auth import get_account_context
from database import system_prompts_collection
from models.system_prompt import SystemPromptUpdate
from utils.prompts import PROMPT_REGISTRY, clear_prompt_cache

router = APIRouter(prefix="/api/system-prompts", tags=["system-prompts"])


@router.get("")
async def list_system_prompts(account_ctx=Depends(get_account_context)):
    """List all system prompts, merging DB overrides with defaults from registry."""
    account_id = ObjectId(account_ctx["account"]["_id"])
    prompts = []

    for slug, meta in PROMPT_REGISTRY.items():
        db_doc = await system_prompts_collection.find_one({"slug": slug, "account_id": account_id})

        prompt_data = {
            "slug": slug,
            "name": meta["name"],
            "description": meta["description"],
            "content": db_doc["content"] if db_doc else meta["default"],
            "is_customized": db_doc is not None,
            "version": db_doc.get("version", 0) if db_doc else 0,
            "updated_at": db_doc.get("updated_at") if db_doc else None,
            "updated_by": db_doc.get("updated_by") if db_doc else None,
        }
        prompts.append(prompt_data)

    return {"prompts": prompts}


@router.get("/{slug}")
async def get_system_prompt(slug: str, account_ctx=Depends(get_account_context)):
    """Get a single system prompt by slug."""
    # TODO: get_system_prompt utility should accept account_id for full per-account isolation
    if slug not in PROMPT_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown prompt slug: {slug}")

    account_id = ObjectId(account_ctx["account"]["_id"])
    meta = PROMPT_REGISTRY[slug]
    db_doc = await system_prompts_collection.find_one({"slug": slug, "account_id": account_id})

    return {
        "slug": slug,
        "name": meta["name"],
        "description": meta["description"],
        "content": db_doc["content"] if db_doc else meta["default"],
        "default_content": meta["default"],
        "is_customized": db_doc is not None,
        "version": db_doc.get("version", 0) if db_doc else 0,
        "updated_at": db_doc.get("updated_at") if db_doc else None,
        "updated_by": db_doc.get("updated_by") if db_doc else None,
    }


@router.put("/{slug}")
async def update_system_prompt(
    slug: str,
    body: SystemPromptUpdate,
    account_ctx=Depends(get_account_context),
):
    """Update a system prompt (upsert). Increments version on each edit."""
    if slug not in PROMPT_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown prompt slug: {slug}")

    account_id = ObjectId(account_ctx["account"]["_id"])
    admin = account_ctx["user"].get("email", "")
    meta = PROMPT_REGISTRY[slug]
    now = datetime.utcnow()

    # Get current version
    existing = await system_prompts_collection.find_one({"slug": slug, "account_id": account_id})
    current_version = existing.get("version", 0) if existing else 0

    await system_prompts_collection.update_one(
        {"slug": slug, "account_id": account_id},
        {
            "$set": {
                "slug": slug,
                "account_id": account_id,
                "name": meta["name"],
                "description": meta["description"],
                "content": body.content,
                "updated_at": now,
                "updated_by": admin,
                "version": current_version + 1,
            }
        },
        upsert=True,
    )

    clear_prompt_cache(slug)

    return {
        "slug": slug,
        "name": meta["name"],
        "content": body.content,
        "version": current_version + 1,
        "updated_at": now,
        "message": f"Prompt '{meta['name']}' updated successfully",
    }


@router.post("/{slug}/reset")
async def reset_system_prompt(slug: str, account_ctx=Depends(get_account_context)):
    """Reset a prompt to its hardcoded default by deleting the DB override."""
    if slug not in PROMPT_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Unknown prompt slug: {slug}")

    account_id = ObjectId(account_ctx["account"]["_id"])
    result = await system_prompts_collection.delete_one({"slug": slug, "account_id": account_id})
    clear_prompt_cache(slug)

    meta = PROMPT_REGISTRY[slug]
    return {
        "slug": slug,
        "name": meta["name"],
        "content": meta["default"],
        "is_customized": False,
        "version": 0,
        "message": f"Prompt '{meta['name']}' reset to default",
    }
