"""MCP integration example — agent consuming tools from an MCP server.

This example shows how to connect a civitas agent to an MCP server over stdio
and invoke its tools via the standard mcp://server/tool URI addressing.

Requirements:
    pip install fabrica-context
    npx -y @modelcontextprotocol/server-filesystem /tmp   # or any MCP server

Usage:
    python examples/mcp_agent.py
"""

from __future__ import annotations

import asyncio

from civitas import AgentProcess, Runtime, Supervisor
from civitas.mcp.types import MCPServerConfig
from civitas.messages import Message
from civitas.plugins.tools import ToolRegistry


class FilesystemAgent(AgentProcess):
    """Reads a file via the MCP filesystem server on every incoming message."""

    async def on_start(self) -> None:
        cfg = MCPServerConfig(
            name="filesystem",
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
        )
        await self.connect_mcp(cfg)
        print(f"[{self.name}] MCP tools registered: {self.tools.names() if self.tools else []}")

    async def handle(self, message: Message) -> None:
        path = message.payload.get("path", "/tmp")
        tool = self.tools.get("mcp://filesystem/read_file") if self.tools else None
        if tool is None:
            print(f"[{self.name}] tool not found")
            return
        try:
            content = await tool.execute(path=path)
            print(f"[{self.name}] read {path!r}: {str(content)[:120]}")
        except Exception as exc:
            print(f"[{self.name}] error: {exc}")


async def main() -> None:
    agent = FilesystemAgent("fs-agent")
    agent.tools = ToolRegistry()

    supervisor = Supervisor(name="root", children=[agent])
    runtime = Runtime(supervisor=supervisor)

    await runtime.start()
    await runtime.send("fs-agent", {"path": "/tmp"})
    await asyncio.sleep(1)
    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
