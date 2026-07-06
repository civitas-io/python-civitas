# Contributing to python-civitas

Thank you for contributing. This document covers dev setup, the test strategy, PR conventions, and how to write and maintain plugins.

---

## Dev setup

Civitas uses [`uv`](https://docs.astral.sh/uv/) for dependency management.

```bash
# Clone and enter the repo
git clone https://github.com/civitas-io/python-civitas
cd python-civitas

# Install all dev dependencies (includes test, lint, type-check tooling)
uv sync --extra dev

# Install pre-commit hooks (ruff + mypy run on every commit)
uv run pre-commit install
```

That's it. No virtualenv activation needed — `uv run` handles the environment.

---

## Running tests

```bash
# All tests (unit + integration)
uv run pytest

# Unit tests only (fast, no external services)
uv run pytest tests/unit/

# A specific test file
uv run pytest tests/integration/test_m1_2_supervision.py

# With verbose output
uv run pytest -v

# Stop on first failure
uv run pytest -x
```

Coverage is enforced at 85% (`--cov-fail-under=85` in `pyproject.toml`). The coverage report is printed after every run.

### Test categories

**Unit tests** (`tests/unit/`) run without any external services or API keys. They test individual components in isolation using mocks where necessary:

| File | What it tests |
|---|---|
| `test_message.py` | Message dataclass, field validation, UUID generation |
| `test_serializer.py` | Msgpack and JSON round-trips |
| `test_registry.py` | LocalRegistry register/lookup/glob |
| `test_bus.py` | MessageBus routing, validation, error cases |
| `test_supervisor.py` | Restart strategies, backoff, escalation, sliding window |
| `test_process.py` | AgentProcess lifecycle, mailbox, state checkpoint |
| `test_runtime.py` | Runtime start/stop, injection, `from_config()` |
| `test_errors.py` | Error hierarchy, ErrorAction |

**Integration tests** (`tests/integration/`) exercise complete runtime scenarios. Most use `InProcessTransport` and require no external services:

| File | What it tests |
|---|---|
| `test_m1_1_hello_agent.py` | Basic agent start + message exchange |
| `test_m1_2_supervision.py` | Crash + restart, all three strategies |
| `test_m1_3_pipeline.py` | Multi-agent pipeline, `ask()` chaining |
| `test_m1_4_llm.py` | LLM integration (mocked provider) |
| `test_m1_5_otel.py` | OTEL span emission, trace propagation |
| `test_m1_6_tree.py` | Nested supervisors, escalation chain |
| `test_m2_1_zmq.py` | ZMQTransport (requires `pyzmq`) |
| `test_m2_2_nats.py` | NATSTransport (requires a running NATS server) |
| `test_m2_3_plugins.py` | Plugin loading from YAML |
| `test_m2_6_adapters.py` | LangGraph and OpenAI SDK adapters |
| `test_m2_7_deploy.py` | `civitas deploy docker-compose` artifact generation |
| `test_m2_8_state.py` | SQLiteStateStore checkpoint/restore |
| `test_m3_1_cli.py` | CLI commands (run, topology validate/show/diff) |
| `test_m3_2_topology.py` | YAML topology parsing, `Runtime.from_config()` |
| `test_m3_3_dashboard.py` | Dashboard command |

The ZMQ and NATS tests are skipped automatically if the required packages or services are not available — you do not need to skip them manually.

### Test fixtures

Shared fixtures and reusable test agents are in `tests/conftest.py`:

- `EchoAgent` — echoes the message payload back to the sender
- `CrashingAgent` — raises `ValueError` on the Nth message (configurable)
- `wait_for_status(agent, status)` — polls until an agent reaches a given status (replaces `asyncio.sleep` in supervision tests)
- `wait_for(condition)` — polls until a condition function returns `True`

Prefer these over writing new polling loops in test code.

---

## Linting and type checking

Pre-commit runs both automatically on `git commit`. Run them manually:

```bash
# Lint + autofix
uv run ruff check --fix civitas/
uv run ruff format civitas/

# Type check
uv run mypy civitas/
```

**Ruff config** (`pyproject.toml`): line length 100, `E`, `F`, `I`, `UP`, `B`, `ASYNC` rule sets. `E501` (line too long) is ignored — Ruff's formatter handles line length. `ASYNC109` is ignored for `timeout` parameters (intentional public API).

**Mypy config**: strict mode, `python_version = "3.12"`, `disallow_untyped_defs = true`. All new code must be fully typed. Third-party stubs (`msgpack`, `zmq`, `nats`, `litellm`, `agents`) are marked `ignore_missing_imports` in `pyproject.toml`.

---

## PR conventions

- **One logical change per PR.** Bug fix, feature, or refactor — not all three.
- **Tests are required** for new functionality. Bug fixes should include a regression test.
- **Update `AGENTS.md`** alongside any public API change (see below).
- **No milestone prefixes** in commit messages or comments (e.g., remove `M2.3` references from code you touch).
- Commit messages: short imperative first line, bullet body if needed. No AI attribution.

### PR checklist

- [ ] `uv run pytest` passes with coverage ≥ 85%
- [ ] `uv run ruff check civitas/` passes (no warnings)
- [ ] `uv run mypy civitas/` passes (no errors)
- [ ] New public API is documented in `AGENTS.md`
- [ ] Relevant doc page updated if behaviour changed

---

## Adding a plugin

Plugins are structural protocols — no base class, no registration macro. Any class with the right method signatures works.

### ModelProvider

```python
# civitas/plugins/my_provider.py
from civitas.plugins.model import ModelResponse

class MyModelProvider:
    async def chat(
        self,
        model: str,
        messages: list[dict],
        tools: list | None = None,
    ) -> ModelResponse:
        result = await call_my_api(model, messages)
        return ModelResponse(
            content=result["text"],
            model=model,
            tokens_in=result["usage"]["input"],
            tokens_out=result["usage"]["output"],
            cost_usd=None,
        )
```

Register a built-in name for YAML loading by adding an entry to `civitas/plugins/loader.py` under `_BUILTIN_MODEL_PROVIDERS`. For third-party packages, use a Python entrypoint instead (see [Plugins — entrypoint registration](plugins.md#registering-a-plugin-via-entrypoint)).

### StateStore

```python
class MyStateStore:
    async def get(self, agent_name: str) -> dict | None: ...
    async def set(self, agent_name: str, state: dict) -> None: ...
    async def delete(self, agent_name: str) -> None: ...
```

### ExportBackend

```python
from civitas.observability.span_queue import SpanData

class MyExportBackend:
    async def export(self, spans: list[SpanData]) -> None: ...
    async def shutdown(self) -> None: ...
```

### Transport

```python
class MyTransport:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def subscribe(self, address: str, handler) -> None: ...
    async def publish(self, address: str, data: bytes) -> None: ...
    async def request(self, address: str, data: bytes, timeout: float) -> bytes: ...
    async def wait_ready(self) -> None: ...
    def has_reply_address(self, address: str) -> bool: ...
```

See [Architecture — Transport protocol](architecture.md) for the full contract.

---

## AGENTS.md maintenance policy

`AGENTS.md` is the machine-readable reference used by AI coding assistants working in this repo. It must stay in sync with the codebase.

**Update `AGENTS.md` whenever you:**
- Add or rename a public class, method, or attribute on `AgentProcess`, `Supervisor`, `Runtime`, or `Message`
- Change a lifecycle hook signature (`on_start`, `handle`, `on_error`, `on_stop`)
- Add or remove a field on `Message`
- Change an import path for any public symbol
- Add a new plugin protocol method

The `AGENTS.md` review is part of the PR checklist — do not merge without it.

---

## Project structure

```
python-civitas/
├── civitas/                  # Runtime source
│   ├── __init__.py          # Public API surface
│   ├── process.py           # AgentProcess
│   ├── supervisor.py        # Supervisor, restart strategies
│   ├── runtime.py           # Runtime, from_config()
│   ├── worker.py            # Worker (multi-process host)
│   ├── bus.py               # MessageBus
│   ├── registry.py          # LocalRegistry
│   ├── messages.py          # Message dataclass, system types
│   ├── serializer.py        # Msgpack + JSON serializers
│   ├── config.py            # Settings, environment variables
│   ├── errors.py            # Error hierarchy, ErrorAction
│   ├── components.py        # ComponentSet, build_component_set()
│   ├── transport/           # Transport protocol + implementations
│   │   ├── __init__.py      # Transport protocol
│   │   ├── inprocess.py     # InProcessTransport
│   │   ├── zmq.py           # ZMQTransport
│   │   └── nats.py          # NATSTransport
│   ├── observability/       # Tracing
│   │   ├── tracer.py        # Tracer, Span, three output modes
│   │   ├── span_queue.py    # SpanQueue, SpanData
│   │   └── export_backend.py # ExportBackend, ConsoleBackend, FanOutBackend
│   ├── plugins/             # Plugin implementations
│   │   ├── model.py         # ModelProvider protocol, ModelResponse
│   │   ├── tools.py         # ToolProvider, ToolRegistry
│   │   ├── state.py         # StateStore, InMemoryStateStore
│   │   ├── sqlite_store.py  # SQLiteStateStore
│   │   ├── anthropic.py     # AnthropicProvider
│   │   ├── litellm.py       # LiteLLMProvider
│   │   └── loader.py        # Plugin resolution (entrypoint → builtin → dotted path)
│   ├── adapters/            # Framework adapters
│   │   ├── langgraph.py     # LangGraphAgent
│   │   └── openai.py        # OpenAIAgent
│   └── cli/                 # Typer CLI
│       ├── app.py           # Root app + console
│       ├── run.py           # civitas run
│       ├── topology.py      # civitas topology validate/show/diff
│       ├── deploy.py        # civitas deploy docker-compose
│       └── state.py         # civitas state list/show/clear
├── tests/
│   ├── conftest.py          # Shared fixtures and test agents
│   ├── unit/                # Fast, isolated unit tests
│   └── integration/         # Full-runtime integration tests
├── docs/                    # Documentation (MkDocs)
├── examples/                # Runnable examples
├── AGENTS.md                # Machine-readable API reference
├── CONTRIBUTING.md          # This file
└── pyproject.toml           # Build config, deps, tool config
```
