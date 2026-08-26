# AGENTS.md

**Package:** `python-civitas` | **Import:** `import civitas` | **Python:** ≥ 3.12

This file guides AI coding agents **contributing to this repo** (civitas core itself).
Read it fully before writing any code.

> **Building an application on top of civitas, not contributing to it?** You want
> [`docs/agents-guide.md`](docs/agents-guide.md) instead — the dense, copy-paste-oriented
> API reference for SDK consumers (hard rules, decision tables, code recipes). This file
> covers repo conventions and contributor-only concerns; it does not duplicate the SDK
> reference, so it won't drift out of sync with it.

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
| `civitas-io/civitas-contrib` | `civitas_contrib` | Provider plugins, framework adapters, DB-backed state/span stores, eval exporters |
| `civitas-io/fabrica` | `fabrica` | Context layer — MCP client/gateway, sandboxing, tools/skills/memory |
| `civitas-io/presidium` | `presidium` | Governance layer — policy, registry, audit |

**Dependency direction:** civitas-contrib → civitas; fabrica → civitas. **Never** import
civitas-contrib or fabrica from inside civitas core. Use lazy imports with helpful error
messages at call sites that need those features — see `civitas/runtime.py`
`_build_exporters` and `civitas/process.py` `connect_mcp` for the established pattern.
This is the single most important rule in this file: violating it breaks the
`import civitas` install for everyone who doesn't have contrib/fabrica installed.

Full install commands (extras, providers, transports) are intentionally not duplicated
here — see [`docs/getting-started.md`](docs/getting-started.md) and
[`docs/plugins.md`](docs/plugins.md#install).

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

Full dev setup, project structure, and PR conventions: [`CONTRIBUTING.md`](CONTRIBUTING.md)
(kept as the single source of truth for repo layout — not duplicated here, since two
copies is exactly how the `TopologyServer`→`TopologyAgent` and stale-import-path doc
regressions happened before).

### Commands Reference

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

### Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `AGENCY_SERIALIZER` | `json` for human-readable debug output | `msgpack` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTEL collector endpoint (e.g. Jaeger) | console |
| `NATS_URL` | NATS server for distributed transport | `nats://localhost:4222` |
| `CIVITAS_STATE_KEY` | base64 32-byte key for `civitas[encryption]`'s `EncryptingStateStore`. Store it in a secret manager and give every process (Runtime + Workers) the same key — **key loss = data loss**. | — |

Never read `os.environ` directly — use `civitas.config.settings`.

---

## SDK / Runtime Reference

The API surface (`AgentProcess`, `Supervisor`, `Runtime`, `Message`, topology YAML, tools,
plugins, GenServer, gateway, MCP) is documented — and kept accurate against real source —
in `docs/`, not duplicated here:

| Topic | Doc |
|---|---|
| Mental model, `AgentProcess`/`Supervisor`/`MessageBus` | [`docs/concepts.md`](docs/concepts.md) |
| Dense copy-paste API reference for coding agents | [`docs/agents-guide.md`](docs/agents-guide.md) |
| Supervision strategies, backoff, restart contract | [`docs/supervision.md`](docs/supervision.md) |
| Messaging semantics, delivery guarantees | [`docs/messaging.md`](docs/messaging.md) |
| Topology YAML schema, CLI, node types | [`docs/topology.md`](docs/topology.md) |
| Model providers, tools, state stores | [`docs/plugins.md`](docs/plugins.md) |
| HTTP gateway, routing, auth | [`docs/gateway.md`](docs/gateway.md) |
| MCP integration | [`docs/mcp.md`](docs/mcp.md) |
| Runtime internals, startup sequence | [`docs/architecture.md`](docs/architecture.md) |

If you change a public symbol's signature, import path, or behavior, update the matching
doc in the same PR — see the PR checklist below.

---

## Code Style

- **Formatter / linter:** `ruff`. Config in `pyproject.toml`. Do not introduce `black` or `flake8`.
- **Line length:** 100.
- **Imports:** top-level only, sorted by ruff `I` rules.
- **Type hints:** required on all public functions and methods. Use `from __future__ import annotations`.
- **Docstrings:** Google style on all public classes and functions.
- **Private symbols:** prefix `_`. Anything without `_` is public API.
- **Comments:** only when the WHY is non-obvious. Never describe WHAT the code does.

## Async Conventions

- All I/O-bound operations **must be async**.
- Never use `time.sleep()` — always `await asyncio.sleep()`.
- Never call `asyncio.run()` inside `AgentProcess` or library code.
- Use `asyncio.TaskGroup` for concurrent tasks.
- Blocking calls: wrap with `asyncio.to_thread`.
- Always use `async with` for async clients and resources.

## Testing

- **Unit tests** (`tests/unit/`): no network, no API keys. Mock `self.llm`, `self.store`.
- **Integration tests** (`tests/integration/`): may call real APIs. Skipped in CI by default.
- Coverage target: **≥ 85%** (enforced by `--cov-fail-under`).
- Test file names mirror source: `civitas/bus.py` → `tests/unit/test_bus.py`.
- Use `pytest.fixture`. Prefer function-scoped fixtures.

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

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked. No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

- Don't "improve" adjacent code, comments, or formatting. Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.
- Remove imports/variables/functions that **your** changes made unused; don't remove
  pre-existing dead code unless asked.
- Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals — "fix the bug" → "write a test that reproduces it,
then make it pass." For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
```

---

## Agent Anti-Patterns

Mistakes AI coding agents make frequently in this codebase specifically. Each item is a
real class of bug found in this repo's own history.

1. **Scoped imports.** Never place `import` inside functions/methods, except: `TYPE_CHECKING`
   guards (circular imports), optional-transport gating (`if transport_type == "zmq": from
   civitas.transport.zmq import ZMQTransport`), and lazy contrib/fabrica imports with a
   `ConfigurationError` on failure (see `civitas/process.py` `connect_mcp`).
2. **Missing `self.reply()`** in a request-reply handler — the caller hangs to timeout.
3. **Non-serializable payload.** `payload` must be JSON primitives only — call
   `.model_dump()` / `.asdict()` first.
4. **Sending messages from `on_start()`** — the MessageBus isn't ready yet. First outbound
   message goes from `handle()`.
5. **Instance variables for persistent state.** A restart builds a fresh instance
   (`__init__` re-runs) — only checkpointed `self.state` survives. Always route by name;
   object references held across a restart go stale.
6. **Calling agent methods directly** instead of `self.send()`/`self.ask()` — bypasses the
   message bus, supervision, and tracing.
7. **Using the `_agency.` or `civitas.stream.` message type prefix** in application code —
   reserved for runtime internals, rejected with `MessageValidationError`.
8. **Blocking I/O inside `handle()`** — stalls every agent in the process. Use async clients;
   wrap unavoidable blocking calls with `asyncio.to_thread`.
9. **`assert` for runtime validation** — stripped with `-O`. Use `if not condition: raise ValueError(...)`.
10. **`# type: ignore` as a crutch** — fix the underlying type issue, or use a specific error
    code with a comment explaining why suppression is unavoidable.
11. **Module-level side effects** — never instantiate clients, open files, or make network
    calls at import time; breaks testability.
12. **Broad exception handling** — never bare `except:` or `except Exception: pass`. Wrap in
    domain exceptions from `civitas.errors`.
13. **Reading env vars directly** — use `civitas.config.settings`, never `os.environ["KEY"]`.
14. **Patching private attributes for observability hooks** — add a proper injectable
    callback to the public API instead of monkey-patching (`runtime.on_crash(callback)`,
    not `runtime._root_supervisor._handle_crash = ...`).
15. **Opening a span without a matching close in all code paths** — every `start_span()`
    needs its `span.end()` in a `finally`, or an exception leaks an open span.
16. **Declaring API surface without wiring it up** — a field/method/config key that exists
    but no code path ever reads sets false expectations. If you define an interface, wire
    it up before merging. (A real, currently-open instance of this: `RouteTable.
    merge_contracts_from()` in `civitas/gateway/router.py` — see `docs/gateway.md` and
    `docs/milestones.md`.)
17. **Silently accepting unknown configuration keys** — `from_config()` and similar loaders
    should reject unknown top-level keys immediately with a clear error, not silently ignore typos.
18. **Importing civitas-contrib or fabrica at module top in civitas core** — always lazy,
    at the call site, with a helpful `ConfigurationError`.

---

## What NOT to Do

- Don't add new top-level dependencies without discussion — keep the install footprint small.
- Don't use `print()` in library code — use the `logging` module.
- Don't suppress `ruff` warnings with `# noqa` without an explanatory comment.
- Don't call provider SDKs (Anthropic, OpenAI, etc.) directly from `AgentProcess`.
- Don't commit code that fails `ruff check` or `pytest tests/unit`.
- Don't let `__all__` in `__init__.py` fall out of sync with the actual public surface.
- Don't import from civitas-contrib or fabrica at module top in civitas core.
- Don't duplicate SDK reference material (API tables, code examples, YAML schemas) into
  this file — it belongs in `docs/`, with exactly one place each fact lives.

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
- [ ] Any `docs/*.md` page affected by this change updated in the same PR (see [SDK / Runtime Reference](#sdk--runtime-reference) above)
- [ ] This `AGENTS.md` updated only if org structure, contributor conventions, or anti-patterns changed — not for SDK/API changes, which belong in `docs/`

---

## Changelog

Maintain `CHANGELOG.md` using [Keep a Changelog](https://keepachangelog.com/) format.
Add entries under `[Unreleased]` as you work.
