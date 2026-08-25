"""MCP integration types — no mcp package dependency at import time."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from civitas.sandbox.config import SandboxConfig


@dataclass
class MCPServerConfig:
    """Configuration for a single MCP server connection.

    For stdio transport: set command (and optionally args/env).
    For sse transport: set url.
    For streamable_http transport: set url. This is the MCP spec's newer
    transport -- a single POST/GET/DELETE endpoint, no separate SSE-upgrade
    endpoint -- and is what most current remote MCP servers actually ship,
    often *instead of* classic sse rather than alongside it (GH #26).
    """

    name: str
    transport: Literal["stdio", "sse", "streamable_http"]

    # stdio fields
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None

    # sse / streamable_http fields
    url: str | None = None

    # sandbox
    sandbox: SandboxConfig | None = None

    def __post_init__(self) -> None:
        if self.transport == "stdio" and not self.command:
            raise ValueError(f"MCPServerConfig '{self.name}': transport=stdio requires 'command'")
        if self.transport in ("sse", "streamable_http") and not self.url:
            raise ValueError(
                f"MCPServerConfig '{self.name}': transport={self.transport} requires 'url'"
            )
        if self.transport not in ("stdio", "sse", "streamable_http"):
            raise ValueError(
                f"MCPServerConfig '{self.name}': unknown transport '{self.transport}'. "
                "Use 'stdio', 'sse', or 'streamable_http'."
            )


@dataclass
class MCPToolSchema:
    """MCP tool schema — decoupled from mcp.types.Tool."""

    name: str
    description: str
    input_schema: dict[str, Any]


class MCPToolError(Exception):
    """Raised when an MCP tool call returns isError=True or fails.

    Real, honest gap found 2026-08-25 while fixing ``AgentProcess.
    connect_mcp()``: nothing in civitas core actually raises or catches
    this class -- the real MCP call path (``fabrica.mcp.client.MCPClient.
    call_tool()``) raises its own, separately-defined ``fabrica.mcp.
    errors.MCPToolError`` instead, which an ``except civitas.mcp.types.
    MCPToolError`` here would NOT catch (different classes, same name).
    ``fabrica.mcp.tool.MCPTool.execute()`` lets fabrica's real exception
    propagate unchanged rather than translating into this one. Kept for
    now as existing public API (removing it is a real, separate,
    semver-relevant decision, not bundled into this fix) -- but treat it
    as dead/aspirational, not a type you can reliably catch a real MCP
    tool failure with.
    """

    def __init__(self, tool_name: str, detail: str) -> None:
        self.tool_name = tool_name
        self.detail = detail
        super().__init__(f"MCP tool '{tool_name}' failed: {detail}")
