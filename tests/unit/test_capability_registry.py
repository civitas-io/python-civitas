"""Tests for capability-aware registry: find_by_capability/s, listeners, send_capable."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas.errors import CapabilityNotFoundError
from civitas.process import AgentProcess
from civitas.registry import LocalRegistry, RoutingEntry

# ---------------------------------------------------------------------------
# RoutingEntry capability fields
# ---------------------------------------------------------------------------


def test_routing_entry_defaults_to_empty_capabilities():
    entry = RoutingEntry(name="a", address="a", is_local=True)
    assert entry.capabilities == ()
    assert entry.capability_metadata == {}


def test_routing_entry_stores_capabilities():
    entry = RoutingEntry(
        name="a",
        address="a",
        is_local=True,
        capabilities=("text.summarize", "text.translate"),
        capability_metadata={"model": "gpt-4"},
    )
    assert "text.summarize" in entry.capabilities
    assert entry.capability_metadata["model"] == "gpt-4"


# ---------------------------------------------------------------------------
# register() with capabilities
# ---------------------------------------------------------------------------


def test_register_with_capabilities_stores_them():
    reg = LocalRegistry()
    reg.register("agent_a", capabilities=["text.summarize", "text.translate"])
    entry = reg.lookup("agent_a")
    assert entry is not None
    assert set(entry.capabilities) == {"text.summarize", "text.translate"}


def test_register_with_capability_metadata():
    reg = LocalRegistry()
    reg.register("agent_a", capabilities=["ocr"], capability_metadata={"engine": "tesseract"})
    entry = reg.lookup("agent_a")
    assert entry is not None
    assert entry.capability_metadata["engine"] == "tesseract"


def test_register_capabilities_defaults_to_empty():
    reg = LocalRegistry()
    reg.register("agent_a")
    entry = reg.lookup("agent_a")
    assert entry is not None
    assert entry.capabilities == ()


# ---------------------------------------------------------------------------
# find_by_capability
# ---------------------------------------------------------------------------


def test_find_by_capability_returns_matching_entries():
    reg = LocalRegistry()
    reg.register("a", capabilities=["text.summarize"])
    reg.register("b", capabilities=["text.summarize", "text.translate"])
    reg.register("c", capabilities=["ocr"])

    results = reg.find_by_capability("text.summarize")
    names = {e.name for e in results}
    assert names == {"a", "b"}


def test_find_by_capability_returns_empty_when_no_match():
    reg = LocalRegistry()
    reg.register("a", capabilities=["ocr"])
    assert reg.find_by_capability("text.summarize") == []


def test_find_by_capability_includes_remote_entries():
    reg = LocalRegistry()
    reg.register("local_a", capabilities=["text.summarize"])
    reg.register_remote("remote_b", capabilities=["text.summarize"])

    results = reg.find_by_capability("text.summarize")
    names = {e.name for e in results}
    assert names == {"local_a", "remote_b"}


def test_find_by_capability_excludes_after_deregister():
    reg = LocalRegistry()
    reg.register("agent_a", capabilities=["text.summarize"])
    reg.deregister("agent_a")
    assert reg.find_by_capability("text.summarize") == []


# ---------------------------------------------------------------------------
# find_by_capabilities (multi-tag)
# ---------------------------------------------------------------------------


def test_find_by_capabilities_any_matches_partial():
    reg = LocalRegistry()
    reg.register("a", capabilities=["text.summarize"])
    reg.register("b", capabilities=["text.translate"])
    reg.register("c", capabilities=["ocr"])

    results = reg.find_by_capabilities(["text.summarize", "text.translate"], match="any")
    names = {e.name for e in results}
    assert names == {"a", "b"}


def test_find_by_capabilities_all_requires_every_tag():
    reg = LocalRegistry()
    reg.register("a", capabilities=["text.summarize", "text.translate"])
    reg.register("b", capabilities=["text.summarize"])

    results = reg.find_by_capabilities(["text.summarize", "text.translate"], match="all")
    assert len(results) == 1
    assert results[0].name == "a"


def test_find_by_capabilities_all_returns_empty_when_no_full_match():
    reg = LocalRegistry()
    reg.register("a", capabilities=["text.summarize"])
    reg.register("b", capabilities=["text.translate"])

    assert reg.find_by_capabilities(["text.summarize", "text.translate"], match="all") == []


def test_find_by_capabilities_any_default_match():
    reg = LocalRegistry()
    reg.register("a", capabilities=["text.summarize"])
    # default match="any"
    results = reg.find_by_capabilities(["text.summarize", "ocr"])
    assert len(results) == 1


# ---------------------------------------------------------------------------
# register_remote() with capabilities
# ---------------------------------------------------------------------------


def test_register_remote_with_capabilities():
    reg = LocalRegistry()
    reg.register_remote(
        "remote_a", capabilities=["text.summarize"], capability_metadata={"tier": "premium"}
    )
    entry = reg.lookup("remote_a")
    assert entry is not None
    assert "text.summarize" in entry.capabilities
    assert entry.capability_metadata["tier"] == "premium"


def test_register_remote_idempotent_does_not_raise():
    reg = LocalRegistry()
    reg.register_remote("remote_a", capabilities=["text.summarize"])
    reg.register_remote("remote_a", capabilities=["text.summarize"])  # idempotent
    assert reg.has("remote_a")


# ---------------------------------------------------------------------------
# Listener: called on register and deregister
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_listener_called_on_register():
    reg = LocalRegistry()
    received: list[tuple[str, tuple[str, ...], dict[str, Any], str]] = []

    async def listener(name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str) -> None:
        received.append((name, caps, meta, event))

    reg.add_listener(listener)
    reg.register("agent_a", capabilities=["text.summarize"], capability_metadata={"k": "v"})

    await asyncio.sleep(0.05)  # let fire-and-forget task run
    assert len(received) == 1
    name, caps, meta, event = received[0]
    assert name == "agent_a"
    assert "text.summarize" in caps
    assert meta["k"] == "v"
    assert event == "register"


@pytest.mark.asyncio
async def test_listener_called_on_deregister():
    reg = LocalRegistry()
    events: list[str] = []

    async def listener(name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str) -> None:
        events.append(event)

    reg.add_listener(listener)
    reg.register("agent_a")
    reg.deregister("agent_a")

    await asyncio.sleep(0.05)
    assert events == ["register", "deregister"]


@pytest.mark.asyncio
async def test_listener_called_on_register_remote():
    reg = LocalRegistry()
    received: list[str] = []

    async def listener(name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str) -> None:
        received.append(name)

    reg.add_listener(listener)
    reg.register_remote("remote_x", capabilities=["ocr"])

    await asyncio.sleep(0.05)
    assert "remote_x" in received


@pytest.mark.asyncio
async def test_multiple_listeners_all_called():
    reg = LocalRegistry()
    calls_a: list[str] = []
    calls_b: list[str] = []

    async def listener_a(
        name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str
    ) -> None:
        calls_a.append(name)

    async def listener_b(
        name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str
    ) -> None:
        calls_b.append(name)

    reg.add_listener(listener_a)
    reg.add_listener(listener_b)
    reg.register("agent_a")

    await asyncio.sleep(0.05)
    assert "agent_a" in calls_a
    assert "agent_a" in calls_b


@pytest.mark.asyncio
async def test_remove_listener_stops_callbacks():
    reg = LocalRegistry()
    calls: list[str] = []

    async def listener(name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str) -> None:
        calls.append(name)

    reg.add_listener(listener)
    reg.remove_listener(listener)
    reg.register("agent_a")

    await asyncio.sleep(0.05)
    assert calls == []


@pytest.mark.asyncio
async def test_listener_exception_does_not_crash_registry():
    reg = LocalRegistry()

    async def bad_listener(
        name: str, caps: tuple[str, ...], meta: dict[str, Any], event: str
    ) -> None:
        raise RuntimeError("listener blew up")

    reg.add_listener(bad_listener)
    reg.register("agent_a")  # must not raise

    await asyncio.sleep(0.05)
    # registry is still usable
    assert reg.has("agent_a")


# ---------------------------------------------------------------------------
# CapabilityNotFoundError
# ---------------------------------------------------------------------------


def test_capability_not_found_error_message():
    err = CapabilityNotFoundError("text.summarize")
    assert "text.summarize" in str(err)
    assert err.capability == "text.summarize"


def test_capability_not_found_error_is_civitas_error():
    from civitas.errors import CivitasError

    assert issubclass(CapabilityNotFoundError, CivitasError)


# ---------------------------------------------------------------------------
# AgentProcess.send_capable()
# ---------------------------------------------------------------------------


class _CapableAgent(AgentProcess):
    capabilities = ["text.summarize"]

    async def handle(self, message: Any) -> None:
        pass


@pytest.mark.asyncio
async def test_send_capable_raises_when_not_wired():
    agent = _CapableAgent("sender")
    agent._registry = None  # not wired
    with pytest.raises(RuntimeError, match="not wired"):
        await agent.send_capable("text.summarize", {})


@pytest.mark.asyncio
async def test_send_capable_raises_capability_not_found():
    agent = _CapableAgent("sender")
    registry = LocalRegistry()
    agent._registry = registry  # no agents with this capability
    with pytest.raises(CapabilityNotFoundError):
        await agent.send_capable("text.summarize", {})


@pytest.mark.asyncio
async def test_send_capable_routes_to_capable_agent():
    agent = _CapableAgent("sender")
    registry = LocalRegistry()
    registry.register("target", capabilities=["text.summarize"])
    agent._registry = registry

    bus = MagicMock()
    bus.route = AsyncMock()
    agent._bus = bus

    await agent.send_capable("text.summarize", {"text": "hello"})

    bus.route.assert_called_once()
    msg = bus.route.call_args[0][0]
    assert msg.recipient == "target"


@pytest.mark.asyncio
async def test_send_capable_picks_from_multiple_candidates():
    """With multiple capable agents, send_capable picks one (any)."""
    agent = _CapableAgent("sender")
    registry = LocalRegistry()
    registry.register("target_1", capabilities=["text.summarize"])
    registry.register("target_2", capabilities=["text.summarize"])
    agent._registry = registry

    bus = MagicMock()
    bus.route = AsyncMock()
    agent._bus = bus

    await agent.send_capable("text.summarize", {})

    bus.route.assert_called_once()
    msg = bus.route.call_args[0][0]
    assert msg.recipient in {"target_1", "target_2"}


# ---------------------------------------------------------------------------
# AgentProcess class-level capability declaration
# ---------------------------------------------------------------------------


def test_agent_class_capabilities_default_empty():
    class _Plain(AgentProcess):
        async def handle(self, message: Any) -> None:
            pass

    a = _Plain("a")
    assert a.capabilities == []
    assert a.capability_metadata == {}


def test_agent_class_capabilities_inherited():
    class _Rich(AgentProcess):
        capabilities = ["text.summarize", "ocr"]
        capability_metadata = {"engine": "tesseract"}

        async def handle(self, message: Any) -> None:
            pass

    a = _Rich("a")
    assert "text.summarize" in a.capabilities
    assert a.capability_metadata["engine"] == "tesseract"
