"""
Runtime system settings (feature/gate flags) backed by the `system_settings`
collection with a short in-process TTL cache (~30s).

Usage at a call site:
    from services.system_settings_service import get_flag
    if await get_flag("prefilter_gate_enabled", settings.prefilter_gate_enabled):
        ...

Semantics:
- No DB override present  -> the provided default (usually the env setting) wins.
- DB override present     -> the DB value wins (until deleted).
Negative lookups are cached too, so the steady-state cost is one indexed
find_one per flag per 30 seconds per process.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

import database

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 30.0
_MISSING = object()  # sentinel: "checked DB, no override exists"

# (monotonic_ts, value_or_MISSING)
_cache: dict[str, tuple[float, Any]] = {}


def _cache_get(name: str) -> Any:
    hit = _cache.get(name)
    if hit is None:
        return None
    ts, value = hit
    if time.monotonic() - ts > _CACHE_TTL_SECONDS:
        return None
    return (value,)  # wrap so a cached None/False is distinguishable from a miss


async def get_flag(name: str, default: Any = None) -> Any:
    """Return the DB override for `name`, or `default` when no override exists."""
    cached = _cache_get(name)
    if cached is not None:
        value = cached[0]
        return default if value is _MISSING else value

    try:
        doc = await database.system_settings_collection.find_one({"key": name})
    except Exception as e:
        logger.warning(f"system_settings read failed for {name!r}: {e} — using default")
        return default

    value = doc.get("value", _MISSING) if doc else _MISSING
    _cache[name] = (time.monotonic(), value)
    return default if value is _MISSING else value


async def set_flag(name: str, value: Any, updated_by: Optional[str] = None) -> dict:
    """Upsert a flag override and invalidate the local cache entry."""
    now = datetime.now(timezone.utc)
    await database.system_settings_collection.update_one(
        {"key": name},
        {
            "$set": {"key": name, "value": value, "updated_at": now, "updated_by": updated_by},
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )
    _cache.pop(name, None)
    return {"key": name, "value": value}


async def delete_flag(name: str) -> bool:
    """Remove a flag override (env default takes over again)."""
    result = await database.system_settings_collection.delete_one({"key": name})
    _cache.pop(name, None)
    return result.deleted_count > 0


async def get_all_flags() -> list[dict]:
    """List all DB flag overrides (no cache — admin read path)."""
    docs = await database.system_settings_collection.find({}).to_list(200)
    return [
        {
            "key": d.get("key"),
            "value": d.get("value"),
            "updated_at": d.get("updated_at"),
            "updated_by": d.get("updated_by"),
        }
        for d in docs
    ]


def invalidate_cache() -> None:
    """Test/administrative helper: drop all cached flag values."""
    _cache.clear()
