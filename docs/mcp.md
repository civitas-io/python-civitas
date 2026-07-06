# MCP Integration

Civitas agents can connect to external MCP (Model Context Protocol) tool servers and invoke their tools natively — alongside built-in tools, with the same `mcp://server/tool` URI addressing, and with full OTEL tracing. A Civitas agent can also expose itself as an MCP server, making its capabilities callable by any MCP-compatible client.

MCP integration requires the optional `mcp` extra:

```bash
pip install 'civitas[mcp]'
```

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
| `name` | `str` | both | Logical name used in tool URIs: `mcp://name/tool` |
| `transport` | `"stdio"` \| `"sse"` | both | How to connect to the server |
| `command` | `str` | stdio | Executable to launch, e.g. `"npx"` or `"python"` |
| `args` | `list[str]` | stdio | Arguments passed to `command` |
| `env` | `dict[str, str] \| None` | stdio | Extra environment variables for the subprocess |
| `url` | `str` | sse | SSE endpoint URL, e.g. `"http://localhost:3000/sse"` |

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

If a tool call fails (the MCP server returns `isError=True` or the subprocess exits), `MCPToolError` is raised with the tool name and detail message.

---

## MCP tools in LLM tool calling

MCP tools registered with `connect_mcp` are available to the agent's LLM provider automatically. Pass `self.tools` when calling the LLM and the model can select and invoke MCP tools as part of its reasoning:

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
            tools=self.tools,  # includes all mcp://filesystem/* and mcp://github/* tools
        )
        return self.reply({"answer": response.content})
```

The LLM sees MCP tool names in their `mcp://server/tool` form. Tool call results are routed back through the Civitas tool registry, not directly through the MCP client, so OTEL tracing applies.

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

MCP server connections are configured in the agent's `mcp_servers` block:

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: researcher
      type: myapp.agents.ResearchAgent
      mcp_servers:
        - name: filesystem
          transport: stdio
          command: npx
          args: ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
        - name: slack
          transport: sse
          url: http://localhost:3001/sse
```

The runtime calls `connect_mcp` for each entry before `on_start()` is called on the agent.

---

## Exposing Civitas agents as an MCP server

`MCPServer` wraps a running runtime and exposes its agents as tools callable by any MCP client. This lets external systems (Claude Desktop, other MCP hosts) call into your agent graph.

```python
from civitas.mcp.server import MCPServer

mcp = MCPServer(
    runtime=runtime,
    expose=["researcher", "summarizer"],  # agent names to expose as tools
    port=3000,
)
await mcp.start()
# MCP clients can now connect at http://localhost:3000/sse
```

Each exposed agent appears as a tool named after the agent. Calling the tool sends a `call` message to the agent and returns its reply as the tool result.

---

## OTEL tracing

Every MCP tool invocation emits a `tool.execute {name}` span, identical to built-in tool spans:

| Attribute | Value |
|---|---|
| `tool.name` | `mcp://filesystem/read_file` |
| `tool.result_status` | `ok` or `error` |
| `tool.latency_ms` | Round-trip time including MCP server execution |

These spans are parented to the enclosing `civitas.agent.handle` span, so MCP calls appear inline in your distributed trace alongside LLM calls.

---

## What MCP integration does not do

**No automatic reconnection.** If an SSE server goes down, the connection is not automatically re-established. Wrap `connect_mcp` in retry logic inside `on_start()` if you need resilience.

**No schema validation on tool inputs.** Civitas passes keyword arguments directly to the MCP client. Input validation is the MCP server's responsibility. Use `MCPToolError` handling to catch server-side failures.

**No tool discovery at runtime.** Tools are registered once in `on_start()`. Tools added to the MCP server after connection are not visible. Restart the agent to pick up new tools.

---

## See also

- [plugins.md](plugins.md) — built-in tool registry and tool providers
- [observability.md](observability.md) — OTEL tracing for tool spans
- [topology.md](topology.md) — YAML topology configuration
