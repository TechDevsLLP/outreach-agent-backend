"""
One-shot script: verify which profileScraperMode of actor Vb6LZkh4EqRlR0Ka9 returns emails.
Run once manually: python scripts/verify_employee_email_mode.py
Record which mode string returns emails and which field name contains the email.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apify_client import ApifyClient
from config import get_settings

settings = get_settings()
client = ApifyClient(settings.apify_api_key)

MODES_TO_TEST = [
    "Short ($4 per 1k)",
    "Full ($8 per 1k)",
    "Email ($12 per 1k)",
]

TEST_COMPANY = "https://www.linkedin.com/company/stripe/"

for mode in MODES_TO_TEST:
    print(f"\n=== Mode: {mode} ===")
    try:
        run = client.actor("Vb6LZkh4EqRlR0Ka9").call(run_input={
            "companies": [TEST_COMPANY],
            "maxItems": 3,
            "profileScraperMode": mode,
            "seniorityLevelIds": ["210", "220", "300", "310"],
        })
        count = 0
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            if count >= 3:
                break
            print(f"  keys present: {sorted(k for k in item.keys() if item[k])}")
            email_candidates = {
                "email": item.get("email"),
                "workEmail": item.get("workEmail"),
                "emailAddress": item.get("emailAddress"),
                "contactInfo.email": (item.get("contactInfo") or {}).get("email"),
            }
            print(f"  email candidates: {email_candidates}")
            count += 1
    except Exception as e:
        print(f"  ERROR: {e}")
