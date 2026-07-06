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
    """

    name: str
    transport: Literal["stdio", "sse"]

    # stdio fields
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None

    # sse fields
    url: str | None = None

    # sandbox
    sandbox: SandboxConfig | None = None

    def __post_init__(self) -> None:
        if self.transport == "stdio" and not self.command:
            raise ValueError(f"MCPServerConfig '{self.name}': transport=stdio requires 'command'")
        if self.transport == "sse" and not self.url:
            raise ValueError(f"MCPServerConfig '{self.name}': transport=sse requires 'url'")
        if self.transport not in ("stdio", "sse"):
            raise ValueError(
                f"MCPServerConfig '{self.name}': unknown transport '{self.transport}'. "
                "Use 'stdio' or 'sse'."
            )


@dataclass
class MCPToolSchema:
    """MCP tool schema — decoupled from mcp.types.Tool."""

    name: str
    description: str
    input_schema: dict[str, Any]


class MCPToolError(Exception):
    """Raised when an MCP tool call returns isError=True or fails."""

    def __init__(self, tool_name: str, detail: str) -> None:
        self.tool_name = tool_name
        self.detail = detail
        super().__init__(f"MCP tool '{tool_name}' failed: {detail}")
