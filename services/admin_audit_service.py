"""
Admin audit log service.

Every superadmin mutation (suspend, quota override, extend-trial, impersonate,
force-pause/resume, re-enrich, suppression change, flag toggle, ...) must call
log_admin_action() explicitly. Explicit calls > clever middleware: the action
name and params are chosen at the call site where intent is unambiguous.

Documents in `admin_audit_log`:
    {admin_email, action, target_type, target_id, params, created_at}
"""
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId

import database

logger = logging.getLogger(__name__)

# Never persist values for keys that look like credentials, even if a caller
# accidentally passes them through in params.
_SENSITIVE_KEY_FRAGMENTS = ("token", "secret", "password", "api_key", "apikey", "credential")


def _sanitize(value: Any, depth: int = 0) -> Any:
    """Make params JSON/BSON-safe and strip anything credential-shaped."""
    if depth > 4:
        return "..."
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if any(frag in str(k).lower() for frag in _SENSITIVE_KEY_FRAGMENTS):
                out[str(k)] = "[redacted]"
            else:
                out[str(k)] = _sanitize(v, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        return [_sanitize(v, depth + 1) for v in value[:50]]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (str, int, float, bool, datetime)) or value is None:
        return value if not isinstance(value, str) else value[:2000]
    return str(value)[:500]


async def log_admin_action(
    admin_email: str,
    action: str,
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    params: Optional[dict] = None,
) -> None:
    """Insert one audit record. Never raises — auditing must not break the mutation."""
    try:
        await database.admin_audit_log_collection.insert_one({
            "admin_email": admin_email,
            "action": action,
            "target_type": target_type,
            "target_id": str(target_id) if target_id is not None else None,
            "params": _sanitize(params or {}),
            "created_at": datetime.now(timezone.utc),
        })
    except Exception as e:
        logger.warning(f"admin_audit_log insert failed for action={action}: {e}")
