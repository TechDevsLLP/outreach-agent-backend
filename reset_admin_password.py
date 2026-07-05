"""One-shot script: create or reset the super admin account.

Run from the backend directory:
    python reset_admin_password.py
"""
import asyncio
import re
import secrets
from datetime import datetime

import bcrypt
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from config import get_settings


TARGET_EMAIL = "techdevsinc@gmail.com"
TARGET_NAME = "Super Admin"
NEW_PASSWORD = "SuperAdmin@TD@123"


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def slugify(name: str) -> str:
    base = re.sub(r"[^a-z0-9-]", "", name.lower().replace(" ", "-"))
    return f"{base}-{secrets.token_hex(2)}"


async def main():
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_url)
    db = client[settings.mongodb_database]
    users = db["users"]
    accounts = db["accounts"]
    members = db["account_members"]

    now = datetime.utcnow()
    new_hash = hash_password(NEW_PASSWORD)

    user = await users.find_one({"email": TARGET_EMAIL})

    if user:
        # Just update password
        await users.update_one(
            {"email": TARGET_EMAIL},
            {"$set": {"password_hash": new_hash, "updated_at": now}},
        )
        print(f"Password updated for existing user: {TARGET_EMAIL}")
    else:
        # Create user document
        user_doc = {
            "email": TARGET_EMAIL,
            "password_hash": new_hash,
            "name": TARGET_NAME,
            "plan": "enterprise",
            "onboarding_complete": True,
            "is_active": True,
            "booking_link": None,
            "current_account_id": None,
            "created_at": now,
            "updated_at": now,
        }
        user_result = await users.insert_one(user_doc)
        user_id: ObjectId = user_result.inserted_id

        # Create account
        account_doc = {
            "name": "Super Admin Account",
            "slug": slugify("super-admin"),
            "plan": "enterprise",
            "status": "active",
            "trial_ends_at": None,
            "sender_name": None,
            "sender_email": None,
            "reply_to_email": None,
            "daily_email_quota": 500,
            "daily_linkedin_connection_quota": 100,
            "daily_linkedin_inmail_quota": 50,
            "enrichment_batch_limit": 500,
            "created_at": now,
            "updated_at": now,
        }
        account_result = await accounts.insert_one(account_doc)
        account_id: ObjectId = account_result.inserted_id

        # Owner membership
        await members.insert_one({
            "account_id": account_id,
            "user_id": user_id,
            "role": "owner",
            "joined_at": now,
        })

        # Link account to user
        await users.update_one(
            {"_id": user_id},
            {"$set": {"current_account_id": account_id, "updated_at": now}},
        )

        print(f"Created super admin user: {TARGET_EMAIL}")
        print(f"  User ID:   {user_id}")
        print(f"  Account ID: {account_id}")

    print("Done. You can now log in with:")
    print(f"  Email:    {TARGET_EMAIL}")
    print(f"  Password: {NEW_PASSWORD}")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
