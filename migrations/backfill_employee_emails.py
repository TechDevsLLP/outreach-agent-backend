"""
Migration: Backfill employee emails from linked prospects.

For employees that were enriched before the email field was added to
enrichment_data, this script copies the email from the prospect doc
back to the employee's enrichment_data.email field.

Usage:
    cd version2

    # Backfill from existing prospect data:
    python migrations/backfill_employee_emails.py

    # Dry run (preview changes without writing):
    python migrations/backfill_employee_emails.py --dry-run

    # Retry email finding via Apify for prospects that have no email:
    python migrations/backfill_employee_emails.py --retry

    # Dry run retry (see which profiles would be looked up):
    python migrations/backfill_employee_emails.py --retry --dry-run
"""

import asyncio
import argparse
import sys
import os

# Add parent dir to path so imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bson import ObjectId
from datetime import datetime
from database import employees_collection, prospects_collection


async def backfill_employee_emails(dry_run: bool = False):
    # Find enriched employees that have a prospect_id but no email in enrichment_data
    query = {
        "enriched": True,
        "enrichment_data.prospect_id": {"$exists": True},
        "enrichment_data.email": {"$exists": False},
    }

    cursor = employees_collection.find(query)
    employees = await cursor.to_list(None)

    print(f"Found {len(employees)} enriched employees missing email in enrichment_data")

    if not employees:
        print("Nothing to do.")
        return

    updated = 0
    skipped_no_email = 0
    skipped_no_prospect = 0
    errors = 0

    for emp in employees:
        emp_id = str(emp["_id"])
        prospect_id = emp.get("enrichment_data", {}).get("prospect_id")

        if not prospect_id:
            skipped_no_prospect += 1
            continue

        try:
            prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
        except Exception as e:
            print(f"  Error fetching prospect {prospect_id} for employee {emp_id}: {e}")
            errors += 1
            continue

        if not prospect:
            skipped_no_prospect += 1
            continue

        email = prospect.get("email")
        if not email:
            skipped_no_email += 1
            continue

        if dry_run:
            name = emp.get("full_name") or emp.get("first_name") or emp_id
            print(f"  [DRY RUN] Would set email={email} on employee {name} ({emp_id})")
        else:
            await employees_collection.update_one(
                {"_id": emp["_id"]},
                {"$set": {
                    "enrichment_data.email": email,
                    "enrichment_data.email_found": True,
                }},
            )

        updated += 1

    print(f"\nResults:")
    print(f"  {'Would update' if dry_run else 'Updated'}: {updated}")
    print(f"  Skipped (prospect not found): {skipped_no_prospect}")
    print(f"  Skipped (no email on prospect): {skipped_no_email}")
    print(f"  Errors: {errors}")


async def retry_email_finding(dry_run: bool = False):
    """
    Find enriched employees whose linked prospects have no email,
    retry the Apify email finder, and update both prospect and employee.
    """
    from services.email_finder_service import find_emails_batch

    # Find enriched employees missing email
    query = {
        "enriched": True,
        "enrichment_data.prospect_id": {"$exists": True},
        "enrichment_data.email": {"$exists": False},
    }

    cursor = employees_collection.find(query)
    employees = await cursor.to_list(None)

    print(f"Found {len(employees)} enriched employees without email")

    if not employees:
        print("Nothing to retry.")
        return

    # Build list of (employee, prospect, linkedin_url) to retry
    to_retry = []
    skipped = 0

    for emp in employees:
        emp_id = str(emp["_id"])
        prospect_id = emp.get("enrichment_data", {}).get("prospect_id")
        if not prospect_id:
            skipped += 1
            continue

        try:
            prospect = await prospects_collection.find_one({"_id": ObjectId(prospect_id)})
        except Exception:
            skipped += 1
            continue

        if not prospect:
            skipped += 1
            continue

        linkedin_url = prospect.get("linkedin") or emp.get("linkedin_url")
        if not linkedin_url:
            skipped += 1
            continue

        # Only retry if prospect still has no email
        if prospect.get("email"):
            # Prospect already has email, just backfill to employee
            if not dry_run:
                await employees_collection.update_one(
                    {"_id": emp["_id"]},
                    {"$set": {
                        "enrichment_data.email": prospect["email"],
                        "enrichment_data.email_found": True,
                    }},
                )
            name = emp.get("full_name") or emp.get("first_name") or emp_id
            print(f"  Backfilled existing email for {name}")
            continue

        to_retry.append({
            "employee": emp,
            "prospect": prospect,
            "linkedin_url": linkedin_url,
        })

    if not to_retry:
        print("No profiles to retry email finding for.")
        return

    print(f"Will retry email finding for {len(to_retry)} profiles:")
    for item in to_retry:
        name = item["employee"].get("full_name") or item["employee"].get("first_name") or "Unknown"
        print(f"  - {name}: {item['linkedin_url']}")

    if dry_run:
        print("\n[DRY RUN] No Apify calls made.")
        return

    # Call email finder
    urls = [item["linkedin_url"] for item in to_retry]
    print(f"\nCalling Apify email finder for {len(urls)} profiles...")
    email_results = await find_emails_batch(urls, concurrency=3)

    found = 0
    not_found = 0

    for item in to_retry:
        linkedin_url = item["linkedin_url"]
        emp = item["employee"]
        prospect = item["prospect"]
        emp_id = str(emp["_id"])
        prospect_id = str(prospect["_id"])
        name = emp.get("full_name") or emp.get("first_name") or emp_id

        email_data = email_results.get(linkedin_url)
        email = None
        if email_data:
            email = email_data.get("email") or email_data.get("emailAddress")

        if not email:
            not_found += 1
            print(f"  {name}: no email found")
            continue

        found += 1
        print(f"  {name}: found {email}")

        # Update prospect
        await prospects_collection.update_one(
            {"_id": prospect["_id"]},
            {"$set": {
                "email": email,
                "last_updated_at": datetime.utcnow(),
            }},
        )

        # Update employee
        await employees_collection.update_one(
            {"_id": emp["_id"]},
            {"$set": {
                "enrichment_data.email": email,
                "enrichment_data.email_found": True,
            }},
        )

    print(f"\nRetry Results:")
    print(f"  Emails found: {found}")
    print(f"  Not found: {not_found}")
    print(f"  Skipped: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill employee emails from linked prospects")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    parser.add_argument("--retry", action="store_true", help="Retry Apify email finder for missing emails")
    args = parser.parse_args()

    if args.retry:
        asyncio.run(retry_email_finding(dry_run=args.dry_run))
    else:
        asyncio.run(backfill_employee_emails(dry_run=args.dry_run))
