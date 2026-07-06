"""Unit tests for StateStore protocol extensions."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from civitas.plugins.state import InMemoryStateStore, StateStore

# ---------------------------------------------------------------------------
# StateStore protocol compliance helpers
# ---------------------------------------------------------------------------


def _assert_protocol(store: Any) -> None:
    """Verify the store satisfies the StateStore protocol (structural check)."""
    assert isinstance(store, StateStore)


# ---------------------------------------------------------------------------
# InMemoryStateStore — protocol extension (list_agents, close)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_list_agents_empty():
    store = InMemoryStateStore()
    assert await store.list_agents() == []


@pytest.mark.asyncio
async def test_in_memory_list_agents_returns_sorted_names():
    store = InMemoryStateStore()
    await store.set("zebra", {"x": 1})
    await store.set("alpha", {"x": 2})
    await store.set("mango", {"x": 3})
    assert await store.list_agents() == ["alpha", "mango", "zebra"]


@pytest.mark.asyncio
async def test_in_memory_list_agents_excludes_deleted():
    store = InMemoryStateStore()
    await store.set("a", {})
    await store.set("b", {})
    await store.delete("a")
    assert await store.list_agents() == ["b"]


@pytest.mark.asyncio
async def test_in_memory_close_is_noop():
    store = InMemoryStateStore()
    await store.set("agent_a", {"key": "val"})
    await store.close()  # must not raise
    # store still usable (in-memory; close is advisory)
    assert await store.get("agent_a") == {"key": "val"}


def test_in_memory_satisfies_protocol():
    _assert_protocol(InMemoryStateStore())


# ---------------------------------------------------------------------------
# migrate CLI — unit tests with mocked stores
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_copies_all_agents():
    from civitas.cli.state import _do_migrate

    src = InMemoryStateStore()
    await src.set("alpha", {"x": 1})
    await src.set("beta", {"x": 2})

    dst = InMemoryStateStore()

    with (
        patch("civitas.cli.state._parse_dsn", side_effect=[src, dst]),
    ):
        count = await _do_migrate("sqlite:src.db", "sqlite:dst.db", dry_run=False)

    assert count == 2
    assert await dst.get("alpha") == {"x": 1}
    assert await dst.get("beta") == {"x": 2}


@pytest.mark.asyncio
async def test_migrate_dry_run_does_not_write():
    from civitas.cli.state import _do_migrate

    src = InMemoryStateStore()
    await src.set("alpha", {"x": 1})

    dst = InMemoryStateStore()

    with patch("civitas.cli.state._parse_dsn", side_effect=[src, dst]):
        count = await _do_migrate("sqlite:src.db", "sqlite:dst.db", dry_run=True)

    assert count == 1
    assert await dst.get("alpha") is None  # dry-run — nothing written


@pytest.mark.asyncio
async def test_migrate_empty_source_returns_zero():
    from civitas.cli.state import _do_migrate

    src = InMemoryStateStore()
    dst = InMemoryStateStore()

    with patch("civitas.cli.state._parse_dsn", side_effect=[src, dst]):
        count = await _do_migrate("sqlite:src.db", "sqlite:dst.db", dry_run=False)

    assert count == 0
