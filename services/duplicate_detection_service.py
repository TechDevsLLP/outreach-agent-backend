"""
Global Duplicate Detection Across Industries (Phase 6.6)
Prevents enriching the same prospect twice, links to all source industries.
"""

import logging
from typing import Optional
from bson import ObjectId
from datetime import datetime

from database import prospects_collection

logger = logging.getLogger(__name__)


async def find_duplicate_prospect(email: str, linkedin_url: Optional[str] = None) -> Optional[dict]:
    """
    Check if a prospect already exists globally (across all industries).

    Priority:
    1. Email match (primary key)
    2. LinkedIn URL match (secondary key)

    Args:
        email: Prospect email
        linkedin_url: LinkedIn profile URL (optional)

    Returns:
        Existing prospect dict or None
    """
    if not email and not linkedin_url:
        return None

    # Email match (primary)
    if email:
        existing = await prospects_collection.find_one({"email": email.lower().strip()})
        if existing:
            logger.info(f"Found duplicate by email: {email}")
            return existing

    # LinkedIn match (secondary)
    if linkedin_url:
        normalized_linkedin = _normalize_linkedin_url(linkedin_url)
        existing = await prospects_collection.find_one({
            "linkedin": {"$in": [linkedin_url, normalized_linkedin]}
        })
        if existing:
            logger.info(f"Found duplicate by LinkedIn: {linkedin_url}")
            return existing

    return None


async def merge_prospect_with_industry(
    prospect_id: str,
    new_industry_id: str,
    new_tags: Optional[list] = None,
) -> dict:
    """
    Add a new industry source to an existing prospect.
    This handles the case where the same prospect appears in multiple industry searches.

    Args:
        prospect_id: Existing prospect ID
        new_industry_id: New industry that also found this prospect
        new_tags: Tags from the new industry

    Returns:
        Updated prospect dict
    """
    try:
        prospect_oid = ObjectId(prospect_id)
    except Exception:
        raise ValueError(f"Invalid prospect ID: {prospect_id}")

    # Add new industry to source_industry_ids (if not already there)
    update = {
        "$addToSet": {"source_industry_ids": new_industry_id},
        "$set": {"updated_at": datetime.utcnow()},
    }

    # Merge tags
    if new_tags:
        update["$addToSet"]["tags"] = {"$each": new_tags}

    await prospects_collection.update_one({"_id": prospect_oid}, update)

    # Return updated prospect
    updated = await prospects_collection.find_one({"_id": prospect_oid})

    logger.info(
        f"Merged prospect {prospect_id} with industry {new_industry_id}. "
        f"Now linked to {len(updated.get('source_industry_ids', []))} industries"
    )

    return updated


async def find_cross_industry_prospects(min_industry_count: int = 2) -> list[dict]:
    """
    Find prospects that appear in multiple industries.
    These are high-value prospects worth prioritizing.

    Args:
        min_industry_count: Minimum number of industries a prospect must appear in

    Returns:
        List of prospects with their industry counts
    """
    pipeline = [
        {"$match": {
            "source_industry_ids": {"$exists": True},
            f"source_industry_ids.{min_industry_count - 1}": {"$exists": True}
        }},
        {"$project": {
            "email": 1,
            "name": 1,
            "company_name": 1,
            "job_title": 1,
            "prospect_score": 1,
            "enhanced_score": 1,
            "source_industry_ids": 1,
            "industry_count": {"$size": "$source_industry_ids"},
        }},
        {"$sort": {"industry_count": -1, "enhanced_score": -1}},
    ]

    prospects = await prospects_collection.aggregate(pipeline).to_list(None)

    logger.info(f"Found {len(prospects)} prospects in {min_industry_count}+ industries")

    return prospects


async def get_duplication_stats() -> dict:
    """
    Get global duplicate detection statistics.

    Returns:
        Stats about duplicate prospects across industries
    """
    total_prospects = await prospects_collection.count_documents({})

    # Count unique vs duplicate
    in_multiple_industries = await prospects_collection.count_documents({
        "source_industry_ids": {"$exists": True},
        "source_industry_ids.1": {"$exists": True}
    })

    in_single_industry = await prospects_collection.count_documents({
        "$or": [
            {"source_industry_ids": {"$exists": False}},
            {"source_industry_ids.1": {"$exists": False}}
        ]
    })

    # Distribution of industry overlap
    distribution_pipeline = [
        {"$match": {"source_industry_ids": {"$exists": True}}},
        {"$project": {"industry_count": {"$size": "$source_industry_ids"}}},
        {"$group": {"_id": "$industry_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    distribution = await prospects_collection.aggregate(distribution_pipeline).to_list(None)

    return {
        "total_prospects": total_prospects,
        "in_single_industry": in_single_industry,
        "in_multiple_industries": in_multiple_industries,
        "overlap_distribution": [
            {"industry_count": d["_id"], "prospect_count": d["count"]}
            for d in distribution
        ],
        "deduplication_savings": in_multiple_industries,
    }


def _normalize_linkedin_url(url: str) -> str:
    """Normalize LinkedIn URL for comparison."""
    if not url:
        return url

    url = url.strip().rstrip("/")

    # Remove trailing query params
    if "?" in url:
        url = url.split("?")[0]

    # Ensure https
    if url.startswith("http://"):
        url = "https://" + url[7:]

    return url.lower()
