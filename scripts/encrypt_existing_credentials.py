"""Encrypt plaintext credential fields on existing email_accounts documents.

Prior to the Gmail API / Zoho Mail API / SMTP+IMAP overhaul, OAuth tokens and
SMTP passwords were stored in plaintext on `email_accounts`. Credential
encryption (utils/crypto.py, Fernet, keyed off ENCRYPTION_KEY) now applies to
new writes automatically — this script backfills any pre-existing plaintext
values.

Idempotent: already-encrypted values are recognized by their `enc::` prefix
and skipped, so this is safe to run repeatedly or after interruption.

Requires ENCRYPTION_KEY to be set in the environment — without it, encrypt()
is a no-op and this script would do nothing (and will refuse to run).

Usage:
    venv/bin/python scripts/encrypt_existing_credentials.py            # dry run
    venv/bin/python scripts/encrypt_existing_credentials.py --apply
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

SENSITIVE_FIELDS = ("oauth_access_token", "oauth_refresh_token", "smtp_password", "imap_password")


async def main(apply: bool) -> None:
    import database
    from config import get_settings
    from utils.crypto import encrypt, is_encrypted

    settings = get_settings()
    if not settings.encryption_key:
        print("ENCRYPTION_KEY is not set — refusing to run (encrypt() would be a no-op).")
        print("Generate one with:")
        print('  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"')
        sys.exit(1)

    print(f"db={database.db.name}  mode={'APPLY' if apply else 'DRY RUN'}")

    total = await database.email_accounts_collection.count_documents({})
    to_update = 0
    field_counts = {f: 0 for f in SENSITIVE_FIELDS}

    cursor = database.email_accounts_collection.find({})
    async for doc in cursor:
        update_fields = {}
        for field in SENSITIVE_FIELDS:
            value = doc.get(field)
            if value and not is_encrypted(value):
                update_fields[field] = encrypt(value)
                field_counts[field] += 1

        if update_fields:
            to_update += 1
            label = f"{doc.get('provider')}:{doc.get('email')}"
            print(f"  {'[APPLY]' if apply else '[DRY RUN]'} {doc['_id']} ({label}) — "
                  f"encrypting: {', '.join(update_fields.keys())}")
            if apply:
                await database.email_accounts_collection.update_one(
                    {"_id": doc["_id"]}, {"$set": update_fields}
                )

    print(f"\nTotal email_accounts: {total}")
    print(f"Accounts needing encryption: {to_update}")
    for field, count in field_counts.items():
        if count:
            print(f"  {field}: {count} value(s)")
    if not apply and to_update:
        print("\nDry run only — re-run with --apply to write changes.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes (default is dry run)")
    args = parser.parse_args()
    asyncio.run(main(apply=args.apply))
