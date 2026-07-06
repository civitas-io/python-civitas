# Design: MCP Integration (M3.4)

**Status:** Implemented — v0.3
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-04

---

## Motivation

MCP (Model Context Protocol) is the emerging standard for connecting LLMs to external tools and data sources. Civitas agents need two capabilities:

1. **Call MCP tool servers** — invoke tools hosted by external MCP servers (GitHub, Slack, filesystem, etc.) using a direct address: `mcp://server_name/tool_name`.
2. **Expose themselves as MCP servers** — so external LLM clients (Claude Desktop, Cursor, OpenAI Agents SDK) can discover and call Civitas agents as tools.

**What this is not:** M3.4 is the protocol wire layer only. It proves the MCP handshake works end-to-end and gets tools into the runtime. Connection pooling, circuit breakers, semantic retrieval, credential isolation, and unified tool namespacing all belong to downstream milestones and products:

| Concern | Owner |
|---------|-------|
| MCP wire protocol, handshake, schemas | M3.4 (this) |
| Per-agent tool namespace | M3.4 (this) |
| Connection pooling, circuit breakers | Fabrica (`MCPToolSource`) |
| Unified cross-agent ToolStore | M4.4 |
| Credential isolation | M4.2 Security Hardening |
| Semantic tool retrieval (`find_tools`) | Fabrica |

---

## Architecture

```
Agent (on_start)
    │
    ├── MCPClient.connect(server_url, transport)
    │       │
    │       ├── MCP handshake (JSON-RPC 2.0 initialize)
    │       ├── list_tools() → MCPTool instances
    │       └── registers MCPTool in agent's ToolRegistry
    │
    └── handle(message)
            │
            └── self.tools.get("mcp://server_name/tool_name")
                    │
                    └── MCPTool.execute(**kwargs)
                            │
                            ├── open ClientSession (one-shot)
                            ├── call_tool(name, arguments)
                            └── return ToolResult

External LLM client
    │
    └── MCP list_tools / call_tool (stdio or SSE)
            │
            └── CivitasMCPServer
                    │
                    └── looks up tool in agent ToolRegistry
                        routes call to AgentProcess via message bus
```

**Connection model:** One-shot per call — `MCPClient` opens a `ClientSession`, executes the call, closes. No persistent connection in M3.4. Persistent pooling is Fabrica's concern.

---

## Transports

| Transport | When to use | SDK support |
|-----------|-------------|-------------|
| `stdio` | Local MCP servers (subprocess) | `mcp.client.stdio.stdio_client` |
| `sse` | Remote MCP servers (HTTP/SSE) | `mcp.client.sse.sse_client` |

Both supported in M3.4. Transport is configured per-server in topology YAML.

---

## Core interfaces

### MCPClient

Manages the connection to one MCP server. Stateless between calls — opens a new session per call (M3.4), delegates pooling to Fabrica later.

```python
# civitas/mcp/client.py

from dataclasses import dataclass
from typing import Any, Literal

@dataclass
class MCPServerConfig:
    name: str                              # used in mcp://name/tool addressing
    transport: Literal["stdio", "sse"]
    # stdio: command to run the MCP server process
    command: str | None = None             # e.g. "npx @modelcontextprotocol/server-github"
    args: list[str] | None = None
    env: dict[str, str] | None = None
    # sse: URL of the running MCP server
    url: str | None = None                 # e.g. "http://localhost:3000/sse"


class MCPClient:
    """Connects to one MCP server, lists tools, executes calls."""

    def __init__(self, config: MCPServerConfig) -> None: ...

    async def list_tools(self) -> list[MCPToolSchema]:
        """Open a session, call list_tools, return schemas, close."""
        ...

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """Open a session, call call_tool, return result, close."""
        ...
```

### MCPTool

A `ToolProvider` implementation wrapping a single MCP tool.

```python
# civitas/mcp/tool.py

class MCPTool:
    """ToolProvider backed by an MCPClient."""

    def __init__(self, client: MCPClient, schema: MCPToolSchema) -> None: ...

    @property
    def name(self) -> str:
        # Returns "mcp://server_name/tool_name"
        return f"mcp://{self._client.config.name}/{self._schema.name}"

    @property
    def schema(self) -> dict[str, Any]:
        return self._schema.input_schema

    async def execute(self, **kwargs: Any) -> Any:
        return await self._client.call_tool(self._schema.name, kwargs)
```

### AgentProcess integration

Agents connect to MCP servers in `on_start()`, or via the topology YAML `mcp` config block:

```python
class MyAgent(AgentProcess):
    async def on_start(self) -> None:
        # Connect and auto-register tools into self.tools
        await self.connect_mcp(MCPServerConfig(
            name="github",
            transport="stdio",
            command="npx @modelcontextprotocol/server-github",
        ))

    async def handle(self, message: Message) -> Message | None:
        # Direct-address tool call
        tool = self.tools.get("mcp://github/create_issue")
        result = await tool.execute(title="Bug", body="...", repo="owner/repo")
        return self.reply({"result": result})
```

`connect_mcp()` is a new method on `AgentProcess` that:
1. Creates an `MCPClient` for the config
2. Calls `client.list_tools()`
3. Registers each tool as an `MCPTool` in `self.tools`

### CivitasMCPServer

A `GenServer` that exposes the local `ToolRegistry` as an MCP server.
External LLM clients can discover and call Civitas-registered tools over MCP.

```python
# civitas/mcp/server.py

class CivitasMCPServer(GenServer):
    """Exposes a ToolRegistry as an MCP server (stdio transport)."""

    def __init__(self, name: str, tool_registry: ToolRegistry) -> None: ...

    async def init(self) -> None:
        # Start the MCP stdio server in a background task
        asyncio.create_task(self._serve())

    async def _serve(self) -> None:
        # Uses mcp.Server + stdio_server context manager
        ...
```

---

## Topology YAML config

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: assistant
      type: agent
      module: myapp.agents
      class: AssistantAgent

mcp:
  servers:
    - name: github
      transport: stdio
      command: npx
      args: ["@modelcontextprotocol/server-github"]
      env:
        GITHUB_TOKEN: !ENV GITHUB_TOKEN
    - name: slack
      transport: sse
      url: http://localhost:3001/sse

  expose:
    enabled: true
    transport: stdio        # expose Civitas tools as an MCP server
```

Agents listed under `supervision` automatically have the configured MCP servers connected at startup. The `expose` block starts a `CivitasMCPServer` as a supervised child.

---

## OTEL spans

MCP tool calls emit a `civitas.mcp.call` span as a child of the agent's `civitas.agent.handle` span.

```python
# In MCPTool.execute():
async def execute(self, **kwargs):
    if self._tracer:
        with self.tool_span(f"mcp.{self._schema.name}"):
            return await self._client.call_tool(self._schema.name, kwargs)
    return await self._client.call_tool(self._schema.name, kwargs)
```

Span attributes:
- `civitas.mcp.server` — server name
- `civitas.mcp.tool` — tool name
- `civitas.mcp.transport` — stdio / sse

---

## Implementation plan

### Phase 1 — MCP client (core)

1. `civitas/mcp/__init__.py` — package init
2. `civitas/mcp/types.py` — `MCPServerConfig`, `MCPToolSchema`
3. `civitas/mcp/client.py` — `MCPClient` with stdio + SSE transport support
4. `civitas/mcp/tool.py` — `MCPTool(ToolProvider)` with `mcp://` name scheme
5. `AgentProcess.connect_mcp()` — connects and registers tools into `self.tools`
6. OTEL span in `MCPTool.execute()`

### Phase 2 — MCP server exposure

7. `civitas/mcp/server.py` — `CivitasMCPServer(GenServer)`, stdio transport
8. `list_tools` handler — return schemas from injected `ToolRegistry`
9. `call_tool` handler — route call to matching `MCPTool` or agent via bus

### Phase 3 — Topology YAML + extras

10. `mcp` section in topology YAML — auto-connect servers at agent startup
11. `civitas[mcp]` optional extra in `pyproject.toml` (`mcp>=1.0`)
12. `civitas topology validate` — recognise `mcp:` section

### Phase 4 — Tests

13. ≥ 10 unit tests (mock MCP server, no real subprocess needed)
14. ≥ 2 integration tests (real MCP echo server via stdio subprocess)

---

## Open questions

| # | Question | Notes |
|---|----------|-------|
| Q1 | Should `connect_mcp()` be idempotent (reconnect if called twice with same server name)? | Yes — deregister old tools first |
| Q2 | What error surface when `execute()` fails mid-call? | Raise `MCPToolError(tool_name, cause)` — let agent's `on_error()` decide |
| Q3 | `CivitasMCPServer` stdio mode: who manages stdin/stdout if agent also uses them? | Needs its own subprocess context; likely run as a dedicated process or pipe pair |
| Q4 | Should topology YAML MCP config apply to all agents or be per-agent? | All agents in v0.3; per-agent config deferred to M4.4 |
| Q5 | MCP schema version negotiation — which MCP spec version to target? | `2024-11-05` (current stable as of Apr 2026) |

---

## Acceptance criteria

- [ ] `MCPClient` connects to a stdio MCP server, calls `list_tools`, returns schemas
- [ ] `MCPClient` connects to an SSE MCP server, calls `list_tools`, returns schemas
- [ ] `MCPTool.name` follows `mcp://server_name/tool_name` convention
- [ ] `AgentProcess.connect_mcp()` registers MCPTool instances in `self.tools`
- [ ] `self.tools.get("mcp://server/tool")` returns the correct `MCPTool`
- [ ] `MCPTool.execute()` calls the tool and returns the result
- [ ] `MCPTool.execute()` emits a `civitas.mcp.call` OTEL span
- [ ] `CivitasMCPServer` exposes `list_tools` returning all registered tools
- [ ] `CivitasMCPServer` exposes `call_tool` routing to the correct tool
- [ ] Topology YAML `mcp.servers` auto-connects at agent startup
- [ ] `civitas[mcp]` extra installs the `mcp>=1.0` dependency
- [ ] ≥ 10 unit tests + ≥ 2 integration tests
