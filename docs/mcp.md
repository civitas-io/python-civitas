# MCP Integration

Civitas agents can connect to external MCP (Model Context Protocol) tool servers and invoke their tools natively — alongside built-in tools, with the same `mcp://server/tool` URI addressing, and with full OTEL tracing.

The actual MCP client lives in a separate package, **fabrica** (`civitas-io/fabrica`), not in civitas core. `civitas.mcp.types` (`MCPServerConfig`, `MCPToolSchema`) is a lightweight, dependency-free config module that both packages share — `AgentProcess.connect_mcp()` lazily imports `fabrica.mcp.client.MCPClient` and `fabrica.mcp.tool.MCPTool` only when actually called, so civitas core never depends on fabrica:

```bash
pip install fabrica-context
```

If fabrica isn't installed, `connect_mcp()` raises `ConfigurationError("MCP support requires fabrica. Install it with: pip install fabrica-context")`.

---

## Connecting to an MCP server

Call `await self.connect_mcp(config)` inside `on_start()`. This starts the MCP server subprocess (stdio) or opens the SSE connection, negotiates capabilities, and registers all advertised tools into `self.tools` under the `mcp://server_name/tool_name` URI scheme.

```python
from civitas import AgentProcess
from civitas.mcp.types import MCPServerConfig
from civitas.messages import Message

class FilesystemAgent(AgentProcess):

    async def on_start(self) -> None:
        await self.connect_mcp(MCPServerConfig(
            name="filesystem",
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
        ))

    async def handle(self, message: Message) -> Message | None:
        tool = self.tools.get("mcp://filesystem/read_file")
        content = await tool.execute(path=message.payload["path"])
        return self.reply({"content": content})
```

After `connect_mcp` returns, all tools from that server are available. List them with `self.tools.names()`.

---

## MCPServerConfig

`MCPServerConfig` is a dataclass. The `transport` field determines which other fields are required.

| Field | Type | Required for | Description |
|---|---|---|---|
| `name` | `str` | all | Logical name used in tool URIs: `mcp://name/tool` |
| `transport` | `"stdio"` \| `"sse"` \| `"streamable_http"` | all | How to connect to the server |
| `command` | `str` | stdio | Executable to launch, e.g. `"npx"` or `"python"` |
| `args` | `list[str]` | stdio | Arguments passed to `command` |
| `env` | `dict[str, str] \| None` | stdio | Extra environment variables for the subprocess |
| `url` | `str` | sse, streamable_http | Endpoint URL, e.g. `"http://localhost:3000/sse"` |
| `sandbox` | `SandboxConfig \| None` | optional | Per-server sandboxing (see [Architecture](architecture.md) — `civitas/sandbox/`) |

`streamable_http` is the MCP spec's newer transport (a single POST/GET/DELETE endpoint, no separate SSE-upgrade endpoint) and is what most current remote MCP servers ship — often *instead of* classic `sse` rather than alongside it.

**stdio transport** — Civitas spawns the command as a subprocess and communicates over stdin/stdout. The subprocess lifecycle is tied to the agent: when the agent stops, the subprocess is terminated.

```python
MCPServerConfig(
    name="github",
    transport="stdio",
    command="npx",
    args=["-y", "@modelcontextprotocol/server-github"],
    env={"GITHUB_TOKEN": os.environ["GITHUB_TOKEN"]},
)
```

**SSE transport** — Civitas opens an HTTP SSE connection to a running MCP server. The server must already be running.

```python
MCPServerConfig(
    name="slack",
    transport="sse",
    url="http://localhost:3001/sse",
)
```

---

## Calling MCP tools

Tools registered via MCP are callable exactly like built-in tools. Retrieve the tool by URI and call `execute()` with keyword arguments matching the tool's input schema:

```python
async def handle(self, message: Message) -> Message | None:
    search = self.tools.get("mcp://github/search_repositories")
    results = await search.execute(query=message.payload["query"], per_page=5)
    return self.reply({"repositories": results})
```

If a tool call fails (the MCP server returns `isError=True` or the subprocess exits), an exception is raised — but **not** `civitas.mcp.types.MCPToolError`. The real call path (`fabrica.mcp.client.MCPClient.call_tool()`, via `fabrica.mcp.tool.MCPTool.execute()`) raises fabrica's own, separately-defined `fabrica.mcp.errors.MCPToolError` and lets it propagate unchanged; civitas's `MCPToolError` is unused dead code kept only as existing public API. Catch `fabrica.mcp.errors.MCPToolError` (or a broad `Exception`) around `tool.execute()`, not `civitas.mcp.types.MCPToolError`.

---

## MCP tools in LLM tool calling

MCP tools registered with `connect_mcp` sit in the same `self.tools` registry as built-in tools. Collect their schemas and pass them to `self.llm.chat(..., tools=[...])` — `ModelProvider.chat()` expects a `list[Any]` of schemas, not the `ToolRegistry` itself:

```python
from civitas import AgentProcess
from civitas.mcp.types import MCPServerConfig
from civitas.messages import Message

class ResearchAgent(AgentProcess):

    async def on_start(self) -> None:
        await self.connect_mcp(MCPServerConfig(
            name="filesystem",
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-filesystem", "/home/user"],
        ))
        await self.connect_mcp(MCPServerConfig(
            name="github",
            transport="stdio",
            command="npx",
            args=["-y", "@modelcontextprotocol/server-github"],
            env={"GITHUB_TOKEN": os.environ["GITHUB_TOKEN"]},
        ))

    async def handle(self, message: Message) -> Message | None:
        response = await self.llm.chat(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": message.payload["question"]}],
            tools=[t.schema for t in self.tools.list_tools()],  # includes mcp://filesystem/* and mcp://github/* tools
        )
        return self.reply({"answer": response.content})
```

The LLM sees MCP tool names in their `mcp://server/tool` form (each `MCPTool.name` is `f"mcp://{server}/{tool}"`). Tool call results are routed back through `tool.execute()` on the Civitas tool registry, not directly through the MCP client, so OTEL tracing applies. As with any non-Anthropic provider, remember the [tool-schema translation gap](plugins.md#toolprovider-and-toolregistry) — `OpenAIProvider`/`MistralProvider` don't convert `input_schema` for you.

---

## Connecting to multiple servers

Call `connect_mcp` once per server in `on_start()`. Each server is registered under its own name prefix:

```python
async def on_start(self) -> None:
    await self.connect_mcp(MCPServerConfig(
        name="filesystem",
        transport="stdio",
        command="npx",
        args=["-y", "@modelcontextprotocol/server-filesystem", "/data"],
    ))
    await self.connect_mcp(MCPServerConfig(
        name="postgres",
        transport="sse",
        url="http://localhost:5433/sse",
    ))
```

Tools from each server are namespaced: `mcp://filesystem/read_file`, `mcp://postgres/query`, etc. There is no collision between servers.

---

## Topology YAML

MCP servers are configured in a single **top-level** `mcp: servers:` block — not per-agent. Every server declared here is connected to **every agent in the runtime**, right after transport startup and before the supervision tree's normal agent lifecycle proceeds:

```yaml
mcp:
  servers:
    - name: filesystem
      transport: stdio
      command: npx
      args: ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
    - name: slack
      transport: sse
      url: http://localhost:3001/sse

supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: researcher
      type: myapp.agents.ResearchAgent
```

If a given agent fails to connect to a declared server (e.g. the agent doesn't call `connect_mcp`-compatible setup, or the server is unreachable), the runtime logs a warning and continues — it does not fail startup. There is currently no way to scope an MCP server to a subset of agents via YAML; if you need per-agent MCP servers, call `connect_mcp()` yourself from that agent's `on_start()` instead (see [Connecting to an MCP server](#connecting-to-an-mcp-server) above).

---

## OTEL tracing

Every MCP tool invocation (via `fabrica.mcp.tool.MCPTool.execute()`) emits a `civitas.mcp.call` span:

| Attribute | Value |
|---|---|
| `civitas.mcp.server` | The MCP server's config name, e.g. `filesystem` |
| `civitas.mcp.tool` | The MCP tool's name, e.g. `read_file` |
| `civitas.agent.name` | The name of the agent that made the call |

On failure the span records the exception via `span.set_error(exc)`. An `mcp.tool.call` audit event is also emitted to the configured `AuditSink`, independent of tracing. These spans are parented to the enclosing call stack, so MCP calls appear inline in your distributed trace alongside LLM calls.

---

## What MCP integration does not do

**No automatic reconnection.** If an SSE server goes down, the connection is not automatically re-established. Wrap `connect_mcp` in retry logic inside `on_start()` if you need resilience.

**No schema validation on tool inputs.** Civitas passes keyword arguments directly to the MCP client. Input validation is the MCP server's responsibility. Catch `fabrica.mcp.errors.MCPToolError` (not `civitas.mcp.types.MCPToolError`, which is dead — see above) to handle server-side failures.

**No tool discovery at runtime.** Tools are registered once in `on_start()`. Tools added to the MCP server after connection are not visible. Restart the agent to pick up new tools.

---

## See also

- [plugins.md](plugins.md) — built-in tool registry and tool providers
- [observability.md](observability.md) — OTEL tracing for tool spans
- [topology.md](topology.md) — YAML topology configuration
