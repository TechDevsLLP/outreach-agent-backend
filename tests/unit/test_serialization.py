"""Unit tests for utils/serialization.serialize_doc."""
import uuid
from datetime import datetime, timezone, timedelta

import pytest
from bson import ObjectId
from bson.binary import Binary, UUID_SUBTYPE

from utils.serialization import serialize_doc

pytestmark = pytest.mark.unit


def test_objectid_to_string_recursive():
    oid = ObjectId()
    doc = {"_id": oid, "nested": {"ids": [oid, {"deep": oid}]}}
    out = serialize_doc(doc)
    assert out["_id"] == str(oid)
    assert out["nested"]["ids"][0] == str(oid)
    assert out["nested"]["ids"][1]["deep"] == str(oid)


def test_naive_datetime_assumed_utc_with_z_suffix():
    dt = datetime(2026, 7, 10, 12, 0, 0)  # naive
    out = serialize_doc({"ts": dt})
    assert out["ts"] == "2026-07-10T12:00:00Z"


def test_aware_datetime_utc_gets_z_suffix():
    dt = datetime(2026, 7, 10, 12, 0, 0, tzinfo=timezone.utc)
    assert serialize_doc(dt) == "2026-07-10T12:00:00Z"


def test_aware_non_utc_datetime_preserves_offset():
    tz = timezone(timedelta(hours=5, minutes=30))
    dt = datetime(2026, 7, 10, 12, 0, 0, tzinfo=tz)
    assert serialize_doc(dt) == "2026-07-10T12:00:00+05:30"


def test_non_utf8_binary_becomes_none():
    # e.g. int8 embedding vectors (Binary subtype 9) must not 500 the API
    blob = Binary(bytes([0xFF, 0xFE, 0x80, 0x81]), 9)
    out = serialize_doc({"title_vec": blob})
    assert out["title_vec"] is None


def test_utf8_binary_decoded_as_text():
    blob = Binary(b"hello world", 0)
    assert serialize_doc(blob) == "hello world"


def test_uuid_binary_rendered_as_uuid_string():
    u = uuid.uuid4()
    blob = Binary(u.bytes, UUID_SUBTYPE)
    assert serialize_doc(blob) == str(u)


def test_scalars_passthrough():
    for v in (42, 3.14, True, None, "text"):
        assert serialize_doc(v) == v


def test_list_of_mixed_types():
    oid = ObjectId()
    out = serialize_doc([1, "a", oid, None])
    assert out == [1, "a", str(oid), None]
