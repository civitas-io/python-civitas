"""Unit tests for ToolRegistry and the ToolProvider protocol (V4, #42).

ToolRegistry is core public API (injected as ``self.tools`` into every agent)
but was omitted from coverage since M1.7 pending "a dedicated plugin testing
sprint" that never happened — until now.
"""

from __future__ import annotations

from typing import Any

import pytest

from civitas.plugins.tools import ToolProvider, ToolRegistry


class StubTool:
    """Minimal ToolProvider implementation."""

    def __init__(self, name: str) -> None:
        self._name = name
        self.calls: list[dict[str, Any]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "description": "stub",
            "input_schema": {"type": "object", "properties": {}},
        }

    async def execute(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return f"{self._name}-result"


def test_stub_satisfies_tool_provider_protocol():
    """ToolProvider is a static (non-runtime_checkable) Protocol — conformance
    is a typing-level contract; here we assert the structural shape."""
    tool: ToolProvider = StubTool("t")  # would fail mypy if the shape drifted
    assert tool.name == "t"
    assert tool.schema["name"] == "t"
    assert callable(tool.execute)


def test_register_and_get():
    registry = ToolRegistry()
    tool = StubTool("web_search")
    registry.register(tool)
    assert registry.get("web_search") is tool


def test_get_unknown_returns_none():
    assert ToolRegistry().get("nope") is None


def test_register_duplicate_name_raises():
    """Silent overwrite would route model tool calls to the wrong implementation."""
    registry = ToolRegistry()
    registry.register(StubTool("dup"))
    with pytest.raises(ValueError, match="already registered"):
        registry.register(StubTool("dup"))


def test_deregister_then_reregister():
    registry = ToolRegistry()
    registry.register(StubTool("t"))
    registry.deregister("t")
    assert registry.get("t") is None
    registry.register(StubTool("t"))  # name freed
    assert registry.get("t") is not None


def test_deregister_unknown_is_noop():
    ToolRegistry().deregister("nope")  # must not raise


def test_deregister_prefix_removes_only_matching():
    """The MCP reconnect pattern: connect_mcp() clears mcp://<server>/* wholesale."""
    registry = ToolRegistry()
    registry.register(StubTool("mcp://files/read"))
    registry.register(StubTool("mcp://files/write"))
    registry.register(StubTool("mcp://web/fetch"))
    registry.register(StubTool("local_tool"))

    registry.deregister_prefix("mcp://files/")

    assert registry.names() == ["mcp://web/fetch", "local_tool"]


def test_list_tools_and_names_are_copies():
    registry = ToolRegistry()
    registry.register(StubTool("a"))
    tools, names = registry.list_tools(), registry.names()
    tools.clear()
    names.clear()
    assert registry.names() == ["a"]  # internal state untouched


def test_list_tools_preserves_registration_order():
    registry = ToolRegistry()
    for n in ("first", "second", "third"):
        registry.register(StubTool(n))
    assert [t.name for t in registry.list_tools()] == ["first", "second", "third"]


async def test_execute_via_registry_lookup():
    """The documented handle() pattern: self.tools.get(name).execute(**input)."""
    registry = ToolRegistry()
    tool = StubTool("calc")
    registry.register(tool)
    looked_up = registry.get("calc")
    assert looked_up is not None
    result = await looked_up.execute(a=1, b=2)
    assert result == "calc-result"
    assert tool.calls == [{"a": 1, "b": 2}]
