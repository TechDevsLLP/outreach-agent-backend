"""Tenant isolation for runtime prompt overrides."""

from unittest.mock import AsyncMock

import pytest

import database
from utils import prompts


pytestmark = pytest.mark.unit


async def test_prompt_without_tenant_uses_registry_and_never_queries_override(monkeypatch):
    collection = AsyncMock()
    monkeypatch.setattr(database, "system_prompts_collection", collection)
    prompts.clear_prompt_cache()

    content = await prompts.get_system_prompt("assessment")

    assert content == prompts.PROMPT_REGISTRY["assessment"]["default"]
    collection.find_one.assert_not_awaited()


async def test_prompt_cache_and_lookup_are_partitioned_by_tenant(monkeypatch):
    collection = AsyncMock()

    async def find_one(query):
        account_values = {str(value) for value in query["account_id"]["$in"]}
        if "tenant-a" in account_values:
            return {"content": "Tenant A prompt"}
        if "tenant-b" in account_values:
            return {"content": "Tenant B prompt"}
        return None

    collection.find_one.side_effect = find_one
    monkeypatch.setattr(database, "system_prompts_collection", collection)
    prompts.clear_prompt_cache()

    first_a = await prompts.get_system_prompt("assessment", "tenant-a")
    first_b = await prompts.get_system_prompt("assessment", "tenant-b")
    cached_a = await prompts.get_system_prompt("assessment", "tenant-a")

    assert first_a == cached_a == "Tenant A prompt"
    assert first_b == "Tenant B prompt"
    assert collection.find_one.await_count == 2
