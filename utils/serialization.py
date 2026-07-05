"""Utility to recursively convert ObjectId (and other non-JSON types) to strings."""
from bson import ObjectId
from bson.binary import Binary, OLD_UUID_SUBTYPE, UUID_SUBTYPE
from datetime import datetime, timezone


def serialize_doc(obj):
    """Recursively convert ObjectId / datetime fields to JSON-safe types.

    Naive datetimes are assumed to be UTC (the backend's storage convention) and
    emitted with a `Z` suffix so JS `new Date()` parses them as UTC, not local.
    """
    if isinstance(obj, dict):
        return {k: serialize_doc(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_doc(v) for v in obj]
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat().replace("+00:00", "Z")
    if isinstance(obj, (bytes, bytearray)):
        # bson.Binary subclasses bytes. Without this branch FastAPI's
        # jsonable_encoder does `o.decode()` (UTF-8) and 500s on any non-UTF-8
        # binary — e.g. vector-search embeddings (Binary subtype 9). Render
        # UUID-subtype binaries as their canonical string, decode plain text,
        # and drop anything else rather than crashing serialization.
        subtype = getattr(obj, "subtype", None)
        if subtype in (OLD_UUID_SUBTYPE, UUID_SUBTYPE):
            try:
                return str(obj.as_uuid())
            except Exception:
                return obj.hex()
        try:
            return bytes(obj).decode("utf-8")
        except UnicodeDecodeError:
            return None
    return obj
