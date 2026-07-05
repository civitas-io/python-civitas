# AGENTS.md

**Package:** `python-civitas` | **Import:** `import civitas` | **Python:** ≥ 3.12

This file guides AI coding agents (Claude Code, Cursor, Codex, Gemini CLI) working on
this codebase. Read it fully before writing any code.

Cross-cutting context (repo boundaries, positioning, roadmap) lives in the private
`civitas-io/context` repo — clone it alongside this one for full picture.

---

## Project Overview

`python-civitas` is an OSS Python **runtime library + CLI** for building multi-agent
systems. It exposes three surfaces — keep all three in mind:

- **Public SDK / library** — importable by downstream projects (`import civitas`)
- **CLI** — `civitas` entry point for end users
- **Async runtime** — event-loop-based message bus, supervision tree, transport layer

This is infrastructure, not a framework. It does not define how agents reason or call
LLMs — those decisions live in downstream code. Civitas handles process lifecycle,
fault tolerance, message routing, and observability.

This project is meant for wide community adoption. Code must be **simple, readable, and
easy to reason about**. When in doubt, choose clarity over cleverness.

A change to an internal module can break the public SDK, the CLI, or both.

---

## Org Structure — What Lives Where

| Repo | Import | Contains |
|---|---|---|
| `civitas-io/python-civitas` | `civitas` | Core runtime — this repo |
| `civitas-io/civitas-contrib` | `civitas_contrib`, `fabrica` | Provider plugins, framework adapters, eval exporters, MCP gateway |
| `civitas-io/presidium` | `presidium` | Governance layer — policy, registry, audit |

**Dependency direction:** civitas-contrib → civitas. Never import civitas-contrib or
fabrica from inside civitas. Use lazy imports with helpful error messages at call sites
that need contrib features (see `civitas/runtime.py` `_build_exporters` and
`civitas/process.py` `connect_mcp` for the established pattern).

---

## Install

```bash
# Core runtime
pip install civitas

# Transports
pip install civitas[zmq]                  # ZMQ distributed transport
pip install civitas[nats]                 # NATS distributed transport
pip install civitas[nkeys]               # NATS + NKeys auth

# HTTP gateway
pip install civitas[http]                 # uvicorn + pydantic
pip install civitas[http3]               # + QUIC / HTTP3

# Observability
pip install civitas[otel]                 # OTEL exporter backend

# Security
pip install civitas[security]            # transport-level Ed25519 message signing
pip install civitas[encryption]          # encrypted StateStore at rest (ChaCha20-Poly1305 AEAD)

# Model providers, state stores, framework adapters, eval exporters → civitas-contrib
pip install civitas-contrib[anthropic]    # Anthropic Claude
pip install civitas-contrib[openai]       # OpenAI GPT-4o / o-series
pip install civitas-contrib[gemini]       # Google Gemini
pip install civitas-contrib[mistral]      # Mistral
pip install civitas-contrib[litellm]      # 100+ models via LiteLLM
pip install civitas-contrib[postgres]     # PostgreSQL state store

# MCP tools gateway → fabrica (part of civitas-contrib repo)
pip install fabrica[mcp]                  # MCP subprocess gateway + sandboxing
```

---

## Quick Import Reference

```python
# Core — always available
from civitas import AgentProcess, Supervisor, DynamicSupervisor, Runtime, Worker
from civitas import GenServer
from civitas import EvalAgent, EvalEvent, EvalExporter, CorrectionSignal
from civitas import HTTPGateway, GatewayConfig, GatewayRequest, GatewayResponse
from civitas import AuditEvent, AuditSink, JsonlFileSink, NullSink, OtlpSink, SyslogSink
from civitas import SandboxConfig, FilesystemMount
from civitas import SecretsProvider, substitute_vars
from civitas import SecurityConfig
from civitas import RegistryListener, RoutingEntry
from civitas import TopologyServer
from civitas.messages import Message
from civitas.errors import CivitasError, ErrorAction
from civitas.plugins.model import ModelProvider, ModelResponse, ToolCall
from civitas.plugins.tools import ToolProvider, ToolRegistry
from civitas.plugins.state import StateStore, InMemoryStateStore

# Providers — require civitas-contrib
from civitas_contrib.plugins.anthropic import AnthropicProvider    # civitas-contrib[anthropic]
from civitas_contrib.plugins.openai import OpenAIProvider          # civitas-contrib[openai]
from civitas_contrib.plugins.gemini import GeminiProvider          # civitas-contrib[gemini]
from civitas_contrib.plugins.mistral import MistralProvider        # civitas-contrib[mistral]
from civitas_contrib.plugins.litellm import LiteLLMProvider        # civitas-contrib[litellm]
from civitas_contrib.plugins.sqlite_store import SQLiteStateStore  # civitas-contrib
from civitas_contrib.plugins.postgres_store import PostgresStateStore  # civitas-contrib[postgres]

# Framework adapters — require civitas-contrib
from civitas_contrib.adapters.langgraph import LangGraphAgent      # civitas-contrib[langgraph]
from civitas_contrib.adapters.openai import OpenAIAgent            # civitas-contrib[openai]

# MCP gateway — requires fabrica
from fabrica.mcp.client import MCPClient                           # fabrica[mcp]
from fabrica.mcp.tool import MCPTool                               # fabrica[mcp]
from fabrica.sandbox.bubblewrap import BubblewrapSandbox           # fabrica
```

---

## Repository Layout

```
civitas/
  __init__.py            # Public SDK surface — be conservative here
  __main__.py            # Entry point: python -m civitas
  process.py             # AgentProcess — base class for all agents
  supervisor.py          # Supervisor, DynamicSupervisor, RestartStrategy, BackoffPolicy
  runtime.py             # Runtime — wires components, manages lifecycle
  bus.py                 # MessageBus
  messages.py            # Message dataclass
  registry.py            # Registry — agent name → instance lookup, RegistryListener
  serializer.py          # Serializer protocol + msgpack/json implementations
  errors.py              # CivitasError hierarchy + ErrorAction enum
  config.py              # Settings — centralised env var access
  worker.py              # Worker — hosts agents in an OS process
  components.py          # ComponentSet — dependency injection container
  genserver.py           # GenServer — OTP-style call/cast/info server
  evalloop.py            # EvalAgent, EvalLoop, EvalExporter, CorrectionSignal
  topology_server.py     # TopologyServer — live topology API
  cli/                   # CLI package
    __init__.py           # App assembly, exports main()
    app.py                # Shared Typer app, Console, output helpers
    init.py               # civitas init
    run.py                # civitas run
    state.py              # civitas state list|clear|migrate
    topology.py           # civitas topology validate|show|diff
    deploy.py             # civitas deploy
    version.py            # civitas version
    _templates/           # Scaffolding templates ($variable syntax)
  audit/                 # Audit sink protocol + built-in sinks (jsonl, syslog, OTLP)
  dashboard/
    collector.py          # MetricsCollector — in-memory metrics for TUI
    renderer.py           # Rich-based dashboard renderer
  # eval/ does not exist — internals are co-located in evalloop.py
  gateway/
    core.py               # HTTPGateway, GatewayConfig
    types.py              # GatewayRequest, GatewayResponse, NextMiddleware
    h3.py                 # HTTP/3 gateway (requires [http3])
  mcp/
    __init__.py           # Docstring pointing to fabrica — no implementation here
  observability/
    tracer.py             # Tracer — OTEL span creation, context propagation
  plugins/
    loader.py             # Plugin discovery and instantiation
    model.py              # ModelProvider protocol + ModelResponse
    tools.py              # ToolProvider protocol + ToolRegistry
    state.py              # StateStore protocol + InMemoryStateStore
    # Provider implementations (anthropic, openai, etc.) live in civitas-contrib
  sandbox/
    config.py             # SandboxConfig, FilesystemMount (dataclasses only)
    __init__.py
    # BubblewrapSandbox implementation lives in fabrica
  secrets/               # SecretsProvider, env/file providers, ${VAR} substitution
  security/              # Transport-level Ed25519 signing, TLS/CURVE config
  transport/
    __init__.py           # Transport protocol
    inprocess.py          # InProcessTransport (default)
    zmq.py                # ZMQTransport (requires [zmq])
    nats.py               # NATSTransport (requires [nats])
tests/
  unit/
  integration/             # May require API keys or external services
examples/
pyproject.toml
AGENTS.md
```

> This layout is authoritative. If you add or remove a module, update this section.

---

## Environment Setup

This project uses **`uv`** for dependency management.

```bash
# Install uv if not present
curl -Ls https://astral.sh/uv/install.sh | sh

# Install all deps including dev extras
uv sync --all-extras

# Run any command
uv run pytest
uv run civitas --help
```

Never use bare `pip install` — always go through `uv` or edit `pyproject.toml`
and re-run `uv sync`.

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `AGENCY_SERIALIZER` | `json` for human-readable debug output | `msgpack` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTEL collector endpoint (e.g. Jaeger) | console |
| `NATS_URL` | NATS server for distributed transport | `nats://localhost:4222` |
| `CIVITAS_STATE_KEY` | base64 32-byte key for the `civitas[encryption]` `EncryptingStateStore`. Store it in a secret manager and give every process (Runtime + Workers) the same key — **key loss = data loss**. | — |

API keys for model providers (Anthropic, OpenAI, etc.) are configured in civitas-contrib,
not here. Never read `os.environ` directly — use `civitas.config.settings`.

---

## Commands Reference

| Task | Command |
|---|---|
| Run all unit tests | `uv run pytest` |
| Run a single test | `uv run pytest tests/unit/test_foo.py::test_bar -v` |
| Run integration tests | `uv run pytest tests/integration/` |
| Lint | `uv run ruff check .` |
| Format | `uv run ruff format .` |
| Type-check | `uv run mypy civitas/` |
| Run CLI locally | `uv run civitas [args]` |
| Build package | `uv build` |

Run **lint + format + unit tests** before finishing any task:

```bash
uv run ruff check . && uv run ruff format . && uv run pytest tests/unit
```

---

## Core API — AgentProcess

`AgentProcess` is **always subclassed**, never instantiated directly.

```python
class MyAgent(AgentProcess):

    async def on_start(self) -> None:
        # Called once before the first message.
        # DO NOT call self.send() / self.ask() here — MessageBus is not ready.
        # Initialise persistent state here, not instance variables.
        self.state.setdefault("count", 0)

    async def handle(self, message: Message) -> Message | None:
        # Called for every incoming message.
        # All I/O must be async/await — never block here.
        # Return self.reply(...) when the caller uses ask(). Missing it hangs the caller.
        self.state["count"] += 1
        return self.reply({"count": self.state["count"]})

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        # Default if not overridden: ErrorAction.ESCALATE
        if isinstance(error, SomeTransientError):
            return ErrorAction.RETRY
        return ErrorAction.ESCALATE

    async def on_stop(self) -> None:
        # Called on graceful shutdown.
        pass
```

**Lifecycle order:** `on_start()` → `handle()` loop → `on_error()` on exception → `on_stop()`

### Messaging methods (call from inside hooks only)

```python
await self.send("agent_name", {"type": "event", "key": "value"})        # fire-and-forget
result = await self.ask("agent_name", {"type": "query"}, timeout=30.0)  # request-reply
await self.broadcast("tool_agents.*", {"type": "cancel"})               # pattern send
return self.reply({"result": "..."})                                     # respond from handle()
```

### Injected attributes

```python
self.llm      # ModelProvider  — self.llm.chat(model, messages, tools)
self.tools    # ToolRegistry   — self.tools.get("tool_name")
self.store    # StateStore     — raw key/value store
self.state    # dict           — in-memory dict, auto-restored from StateStore on restart
self.name     # str            — this agent's Registry name
```

**`self.state` vs `self.store`** — use `self.state` for everything in normal code:

```python
async def on_start(self) -> None:
    self.state.setdefault("counter", 0)    # initialise with a default

async def handle(self, message: Message) -> Message | None:
    self.state["counter"] += 1
    await self.checkpoint()                # persist — survives supervisor restart
```

`self.store` is the raw `StateStore`. Use it only for advanced cases (e.g. reading
another agent's state):

```python
await self.store.delete("other_agent")    # clear another agent's persisted state
```

`self.checkpoint()` calls `self.store.set(self.name, self.state)`. Call it after
completing a meaningful unit of work.

### ErrorAction values

| Value | Behaviour |
|---|---|
| `ErrorAction.RETRY` | Re-deliver the same message (up to retry limit) |
| `ErrorAction.SKIP` | Discard message, continue with next |
| `ErrorAction.ESCALATE` | Crash process — supervisor applies restart strategy |
| `ErrorAction.STOP` | Graceful shutdown of this process |

---

## Core API — Message Schema

```python
message.id              # str        — UUID7
message.type            # str        — app-defined, e.g. "research_query"
message.sender          # str        — sending agent name
message.recipient       # str        — target agent name
message.payload         # dict       — MUST be JSON-serializable primitives only
message.correlation_id  # str | None — set by ask(); links request to reply
message.reply_to        # str | None — where to send the reply
message.timestamp       # float      — unix epoch
message.trace_id        # str        — OTEL trace ID
message.span_id         # str        — OTEL span ID
message.parent_span_id  # str | None
message.attempt         # int        — 0 = first delivery, incremented on retry
message.ttl             # float | None — expiry in seconds
message.priority        # int        — 0 = normal, 1 = high (system only)
```

**`payload` rule:** only `str`, `int`, `float`, `bool`, `list`, `dict`, `None`.
No Python objects, Pydantic models, or dataclasses — call `.model_dump()` first.

---

## Core API — Supervisor

```python
Supervisor(
    name="root",
    children=[AgentA("a"), AgentB("b"), Supervisor("child_sup", children=[...])],
    strategy="ONE_FOR_ONE",    # "ONE_FOR_ONE" | "ONE_FOR_ALL" | "REST_FOR_ONE"
    max_restarts=3,
    restart_window=60.0,       # seconds
    backoff="EXPONENTIAL",     # "CONSTANT" | "LINEAR" | "EXPONENTIAL"
)
```

**Strategy guide:**
- `ONE_FOR_ONE` — children are independent. One crash restarts only that child. *(most common)*
- `ONE_FOR_ALL` — children are interdependent. One crash restarts all siblings.
- `REST_FOR_ONE` — children form a pipeline. One crash restarts it and all downstream children.

---

## Core API — Runtime

```python
# Code-first
runtime = Runtime(
    supervisor=Supervisor("root", children=[MyAgent("my_agent")])
)

# Config-first
runtime = Runtime.from_config("topology.yaml")

# Lifecycle
await runtime.start()
result = await runtime.ask("agent_name", {"type": "...", "key": "value"}, timeout=30.0)
await runtime.send("agent_name", {"type": "..."})
await runtime.stop()
```

`asyncio.run()` is valid **only** in CLI entry points and test helpers.
Never call it inside `AgentProcess` subclasses or library code.

---

## Core API — Topology YAML

```yaml
transport:
  type: in_process          # in_process | zmq | nats

plugins:
  models:
    - type: anthropic        # short name resolved by civitas_contrib; or full dotted path
      config:
        default_model: claude-sonnet-4-6
  exporters:
    - type: console
  state:
    type: sqlite             # resolved to civitas_contrib.plugins.sqlite_store.SQLiteStateStore
    config:
      db_path: /data/civitas.db

supervision:
  name: root
  strategy: ONE_FOR_ONE
  max_restarts: 3
  restart_window: 60
  children:
    - agent:
        name: research
        type: myapp.agents.ResearchAgent
```

Plugin names like `anthropic`, `sqlite`, `postgres` are resolved by
`civitas.plugins.loader` → `civitas_contrib.*`. Plugin resolution is lazy —
if the contrib package is not installed, a `PluginError` with install instructions
is raised at startup, not at import time.

---

## Core API — Tools

```python
from civitas.plugins.tools import ToolProvider, ToolRegistry

class WebSearchTool:
    @property
    def name(self) -> str:
        return "web_search"

    @property
    def schema(self) -> dict[str, Any]:
        return {
            "name": "web_search",
            "description": "Search the web for a query.",
            "input_schema": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        }

    async def execute(self, **kwargs: Any) -> Any:
        ...
```

> `schema` must match the tool format your model provider expects.
> Anthropic uses `input_schema`; OpenAI uses `parameters`. LiteLLM normalises both.

```python
registry = ToolRegistry()
registry.register(WebSearchTool())

runtime = Runtime(
    supervisor=Supervisor("root", children=[ResearchAgent("researcher")]),
    tool_registry=registry,
)
```

Tool-call loop inside `handle()`:

```python
async def handle(self, message: Message) -> Message | None:
    messages = [{"role": "user", "content": message.payload["query"]}]
    response = await self.llm.chat(
        model="claude-sonnet-4-6",
        messages=messages,
        tools=[t.schema for t in self.tools.list_tools()],
    )
    while response.tool_calls:
        tool_results = []
        for tc in response.tool_calls:
            tool = self.tools.get(tc.name)
            result = await tool.execute(**tc.input)
            tool_results.append({"tool_use_id": tc.id, "content": str(result)})
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})
        response = await self.llm.chat(model="claude-sonnet-4-6", messages=messages,
                                       tools=[t.schema for t in self.tools.list_tools()])
    return self.reply({"answer": response.content})
```

---

## LLM Calls

All LLM calls go through `self.llm.chat()` — never call provider SDKs directly
from `AgentProcess`. Provider implementations live in `civitas-contrib`.

```python
# All providers share the same signature:
response = await self.llm.chat(
    model="claude-sonnet-4-6",
    messages=[{"role": "user", "content": "..."}],
    tools=[...],           # optional
)
```

`ModelResponse` fields:

| Field | Type | Notes |
|---|---|---|
| `response.content` | `str` | Final text |
| `response.model` | `str` | Model name echoed by provider |
| `response.tokens_in` | `int` | Prompt tokens |
| `response.tokens_out` | `int` | Completion tokens |
| `response.cost_usd` | `float \| None` | From pricing table; `None` if model unknown |
| `response.tool_calls` | `list[ToolCall] \| None` | Non-empty when model requests tools |

`ToolCall` fields: `tc.id` (str), `tc.name` (str), `tc.input` (dict).

**Rules:**
- Mock `self.llm` in unit tests — never make real API calls in `tests/unit/`.
- `429` / transient error handling is built into the provider layer.

### Observability helpers

```python
with self.llm_span("claude-sonnet-4-6") as span:
    response = await self.llm.chat(model="claude-sonnet-4-6", messages=[...])
    span.set_attribute("civitas.llm.tokens_out", response.tokens_out)

with self.tool_span("web_search") as span:
    result = await self.tools.get("web_search").execute(query="...")
```

---

## Multi-Agent Pattern

```python
class OrchestratorAgent(AgentProcess):
    async def handle(self, message: Message):
        async with asyncio.TaskGroup() as tg:
            ta = tg.create_task(self.ask("worker_a", {"chunk": message.payload["a"]}))
            tb = tg.create_task(self.ask("worker_b", {"chunk": message.payload["b"]}))
        combined = [ta.result().payload["result"], tb.result().payload["result"]]
        summary = await self.ask("summarizer", {"items": combined})
        return self.reply(summary.payload)
```

Use `asyncio.TaskGroup` (Python ≥ 3.11) over `asyncio.gather` for structured
concurrency — exceptions propagate cleanly and all tasks are cancelled on failure.

---

## Code Style

- **Formatter / linter:** `ruff`. Config in `pyproject.toml`. Do not introduce `black` or `flake8`.
- **Line length:** 100.
- **Imports:** top-level only, sorted by ruff `I` rules.
- **Type hints:** required on all public functions and methods. Use `from __future__ import annotations`.
- **Docstrings:** Google style on all public classes and functions.
- **Private symbols:** prefix `_`. Anything without `_` is public API.
- **Comments:** only when the WHY is non-obvious. Never describe WHAT the code does.

---

## Async Conventions

- All I/O-bound operations **must be async**.
- Never use `time.sleep()` — always `await asyncio.sleep()`.
- Never call `asyncio.run()` inside `AgentProcess` or library code.
- Use `asyncio.TaskGroup` for concurrent tasks.
- Blocking calls: wrap with `asyncio.to_thread`.
- Always use `async with` for async clients and resources.

---

## Testing

- **Unit tests** (`tests/unit/`): no network, no API keys. Mock `self.llm`, `self.store`.
- **Integration tests** (`tests/integration/`): may call real APIs. Skipped in CI by default.
- Coverage target: **≥ 85%** (enforced by `--cov-fail-under`).
- Test file names mirror source: `civitas/bus.py` → `tests/unit/test_bus.py`.
- Use `pytest.fixture`. Prefer function-scoped fixtures.

---

## Public SDK & CLI Stability

- `civitas/__init__.py` is the **public surface**. Removing or renaming anything exported
  there is a breaking change.
- CLI argument names and output formats are also public API.
- Add `# BREAKING CHANGE:` on any line that removes or renames a public symbol.
- Follow semver: patch for fixes, minor for new features, major for breaking changes.

---

## Agent Behavioral Guidelines

Guidelines derived from [Andrej Karpathy's observations](https://x.com/karpathy/status/2015883857489522876) on LLM coding pitfalls. These apply to every task in this repo.

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that **your** changes made unused.
- Don't remove pre-existing dead code unless asked.

Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

## Agent Anti-Patterns

These are mistakes AI coding agents make frequently. Every item here represents
a real class of bug.

### 1. Scoped imports

Never place `import` statements inside functions or methods, except:

1. **`TYPE_CHECKING` guards** — for breaking circular import cycles:
   ```python
   from __future__ import annotations
   from typing import TYPE_CHECKING
   if TYPE_CHECKING:
       from civitas.bus import MessageBus
   ```

2. **Optional dependency gating** — for transport extras that must not fail `import civitas`:
   ```python
   # ZMQ and NATS are optional extras; guard their imports
   if transport_type == "zmq":
       from civitas.transport.zmq import ZMQTransport
   ```

3. **Lazy contrib imports** — when civitas core needs a contrib feature at call time,
   use the established pattern with a helpful error:
   ```python
   try:
       from civitas_contrib.plugins.anthropic import AnthropicProvider
   except ImportError as exc:
       raise ConfigurationError(
           "Anthropic provider requires civitas-contrib. "
           "Install it with: pip install civitas-contrib[anthropic]"
       ) from exc
   ```

Outside these three cases, all imports belong at module top.

### 2. Missing `self.reply()` in a request-reply handler

If a caller uses `ask()`, the handler **must** `return self.reply(...)`.
Omitting it causes the caller to hang silently until timeout.

### 3. Non-serializable payload

`payload` must contain only JSON-serializable primitives. Call `.model_dump()` on
Pydantic models and `.asdict()` on dataclasses before putting them in a payload.

### 4. Sending messages from `on_start()`

The MessageBus is not ready during `on_start()`. Send the first outbound message
from `handle()`, not from `on_start()`.

### 5. Instance variables for persistent state

Instance variables reset on supervisor restart. State that must survive goes in
`self.state` (persisted to StateStore).

```python
# Wrong — resets on restart
def __init__(self, name):
    super().__init__(name)
    self.counter = 0

# Correct
async def on_start(self):
    self.state.setdefault("counter", 0)
```

### 6. Calling agent methods directly

Direct object references bypass message bus, supervision, and tracing. Always
route to agents by name via `self.send()` / `self.ask()`.

### 7. Using the `_agency.` message type prefix

Names starting with `_agency.` are reserved for runtime internals.

```python
# Wrong
await self.send("other", {"type": "_agency.custom"})

# Correct
await self.send("other", {"type": "myapp.custom_event"})
```

### 8. Blocking I/O inside `handle()`

`handle()` runs on the event loop. Use async HTTP clients. Wrap unavoidable blocking
calls with `asyncio.to_thread`.

### 9. `assert` for runtime validation

`assert` is stripped with `-O`. Use `if not condition: raise ValueError(...)`.

### 10. `# type: ignore` as a crutch

Fix the underlying type issue. If suppression is genuinely unavoidable, use a
specific error code and a comment: `# type: ignore[assignment]  # upstream returns Any`.

### 11. Module-level side effects

Never instantiate clients, open files, or make network calls at module level —
they run at import time and break testability.

### 12. Broad exception handling

Never use bare `except:` or `except Exception: pass`. Wrap in domain exceptions
from `civitas.errors`.

### 13. Reading env vars directly

Never call `os.environ["KEY"]` in application code. Use `civitas.config.settings`.

### 14. Patching private attributes for observability hooks

If you need to observe lifecycle events (crashes, restarts, message completions), add a proper
injectable hook or callback to the public API. Never monkey-patch private attributes.

```python
# Wrong — fragile, breaks on internal rename, mypy complains
original = runtime._root_supervisor._handle_crash
runtime._root_supervisor._handle_crash = _patched  # type: ignore[method-assign]

# Correct — use a callback registered through the public API
runtime.on_crash(callback)    # or pass metrics_collector= to Runtime
```

### 15. Opening a span without a matching close in all code paths

Every span opened in a `try` block must close in a `finally`. An exception between
`start_span()` and `span.end()` produces a leaked open span.

```python
# Wrong — exception between start and end leaks the span
span = self._tracer.start_span("civitas.agent.handle", ...)
result = await self.handle(message)     # can raise
span.end()

# Correct
span = self._tracer.start_span("civitas.agent.handle", ...)
try:
    result = await self.handle(message)
finally:
    span.end()
```

### 16. Declaring API surface without wiring it up

A field, method, or configuration key that exists but is never called by any code path
is dead API. It sets user expectations that aren't met.

```python
# Message.ttl declared, documented, but bus.route() never checks it
# MetricsCollector.message_handled() defined but never called

# Rule: if you define an interface, wire it up before merging
```

### 17. Silently accepting unknown configuration keys

`from_config()` and similar loaders should reject unknown keys immediately with a clear
error. Silent acceptance causes "my config is being ignored" bugs that are hard to diagnose.

```python
# Wrong — typo in YAML silently ignored, runtime starts with defaults
config = yaml.safe_load(text)
supervision_cfg = config.get("supervison", {})   # typo

# Correct — validate top-level keys at parse time
KNOWN_KEYS = {"transport", "plugins", "supervision", "topology"}
unknown = set(config.keys()) - KNOWN_KEYS
if unknown:
    raise ConfigurationError(f"Unknown config keys: {unknown!r}")
```

### 18. Importing civitas-contrib or fabrica at module top in civitas core

civitas core must not have top-level imports from civitas-contrib or fabrica.
Use lazy imports at call sites with helpful `ConfigurationError` messages.

---

## What NOT to Do

- Don't add new top-level dependencies without discussion — keep the install footprint small.
- Don't use `print()` in library code — use the `logging` module.
- Don't suppress `ruff` warnings with `# noqa` without an explanatory comment.
- Don't call provider SDKs (Anthropic, OpenAI, etc.) directly from `AgentProcess`.
- Don't commit code that fails `ruff check` or `pytest tests/unit`.
- Don't let `__all__` in `__init__.py` fall out of sync with the actual public surface.
- Don't import from civitas-contrib or fabrica at module top in civitas core.

---

## Pull Request Checklist

- [ ] `uv run ruff check .` passes with no errors
- [ ] `uv run ruff format .` produces no diff
- [ ] `uv run mypy civitas/` passes
- [ ] `uv run pytest tests/unit` passes
- [ ] Type hints on all new public functions
- [ ] Google-style docstrings on all new public classes / functions
- [ ] `CHANGELOG.md` updated for user-visible changes
- [ ] No secrets, `.env` files, or real API keys committed
- [ ] `__all__` in `__init__.py` updated if public surface changed
- [ ] This AGENTS.md updated if repo layout or conventions changed

---

## Changelog

Maintain `CHANGELOG.md` using [Keep a Changelog](https://keepachangelog.com/) format.
Add entries under `[Unreleased]` as you work.
