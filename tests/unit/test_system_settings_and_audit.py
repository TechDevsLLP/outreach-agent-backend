"""Unit tests for services/system_settings_service.py (flag cache logic with a
stubbed collection) and services/admin_audit_service._sanitize."""
from datetime import datetime

import pytest
from bson import ObjectId

import database
from services import system_settings_service as sss
from services.admin_audit_service import _sanitize

pytestmark = pytest.mark.unit


class _StubCollection:
    """Minimal async stand-in for a Motor collection (find_one only)."""

    def __init__(self):
        self.docs: dict[str, dict] = {}
        self.find_one_calls = 0
        self.raise_on_find = False

    async def find_one(self, query):
        self.find_one_calls += 1
        if self.raise_on_find:
            raise RuntimeError("simulated mongo outage")
        return self.docs.get(query["key"])


@pytest.fixture
def stub_flags(monkeypatch):
    stub = _StubCollection()
    monkeypatch.setattr(database, "system_settings_collection", stub)
    sss.invalidate_cache()
    yield stub
    sss.invalidate_cache()


async def test_get_flag_default_when_no_override(stub_flags):
    assert await sss.get_flag("prefilter_gate_enabled", True) is True
    assert await sss.get_flag("prefilter_gate_enabled", False) is False


async def test_get_flag_db_override_wins(stub_flags):
    stub_flags.docs["title_gate_enabled"] = {"key": "title_gate_enabled", "value": False}
    assert await sss.get_flag("title_gate_enabled", True) is False


async def test_negative_lookup_is_cached(stub_flags):
    await sss.get_flag("missing_flag", "default")
    await sss.get_flag("missing_flag", "default")
    await sss.get_flag("missing_flag", "default")
    assert stub_flags.find_one_calls == 1  # only the first call hits the "DB"


async def test_cached_false_value_not_confused_with_miss(stub_flags):
    stub_flags.docs["f"] = {"key": "f", "value": False}
    assert await sss.get_flag("f", True) is False
    assert await sss.get_flag("f", True) is False  # from cache
    assert stub_flags.find_one_calls == 1


async def test_cache_ttl_expiry(stub_flags, monkeypatch):
    stub_flags.docs["f"] = {"key": "f", "value": 1}
    assert await sss.get_flag("f") == 1
    assert stub_flags.find_one_calls == 1
    # Simulate TTL expiry by rewinding the cached timestamp
    ts, value = sss._cache["f"]
    sss._cache["f"] = (ts - sss._CACHE_TTL_SECONDS - 1, value)
    assert await sss.get_flag("f") == 1
    assert stub_flags.find_one_calls == 2  # refetched after expiry


async def test_db_error_falls_back_to_default(stub_flags):
    stub_flags.raise_on_find = True
    assert await sss.get_flag("f", "safe-default") == "safe-default"


# ---------------------------------------------------------------------------
# admin_audit_service._sanitize
# ---------------------------------------------------------------------------

def test_sanitize_redacts_credential_shaped_keys():
    params = {
        "api_key": "sk-live-abc",
        "nested": {"unipile_token": "tok", "PASSWORD": "hunter2", "ok": 1},
        "apikey": "x",
        "client_secret": "y",
        "safe": "visible",
    }
    out = _sanitize(params)
    assert out["api_key"] == "[redacted]"
    assert out["nested"]["unipile_token"] == "[redacted]"
    assert out["nested"]["PASSWORD"] == "[redacted]"
    assert out["apikey"] == "[redacted]"
    assert out["client_secret"] == "[redacted]"
    assert out["safe"] == "visible"
    assert out["nested"]["ok"] == 1


def test_sanitize_objectid_and_datetime():
    oid = ObjectId()
    now = datetime.utcnow()
    out = _sanitize({"id": oid, "at": now})
    assert out["id"] == str(oid)
    assert out["at"] == now  # datetimes are BSON-safe, kept as-is


def test_sanitize_depth_cap():
    deep = {"a": {"b": {"c": {"d": {"e": {"f": "too deep"}}}}}}
    out = _sanitize(deep)
    assert out["a"]["b"]["c"]["d"]["e"] == "..."


def test_sanitize_truncates_long_strings_and_lists():
    out = _sanitize({"s": "x" * 5000, "l": list(range(100))})
    assert len(out["s"]) == 2000
    assert len(out["l"]) == 50


def test_sanitize_stringifies_unknown_types():
    class Weird:
        def __repr__(self):
            return "weird-object"

    out = _sanitize({"w": Weird()})
    assert isinstance(out["w"], str)
