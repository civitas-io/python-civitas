# Examples

Every example here is a real, runnable script — not a snippet. Run them from the repo root
(`python examples/<name>.py`), not from inside the `examples/` directory itself.

## How to test these examples

Every example that needs no external service (no real LLM API key, no separate MCP server, no
NATS server, no extra packages outside this repo's own `civitas[...]` extras) is exercised by an
automated smoke test:

```bash
pytest tests/integration/test_examples_smoke.py -v
```

This proves each one **runs without crashing** — not that its output is semantically correct for
every input, just that the example genuinely works end-to-end, right now, on this codebase. It
exists because three separate examples shipped with real, silently-broken API calls before anyone
ran them by hand (v0.9.1's `dynamic_spawning.py`, v0.9.2's `stateful_workflow.py` and
`deployment/level2_multi_process/run_worker.py`) — see `docs/milestones.md`'s v0.9.1/v0.9.2
entries for the full story.

The test file itself (`tests/integration/test_examples_smoke.py`) is the authoritative list of
which examples are covered and why any exclusion exists — read its module docstring and the
`EXCLUDED_EXAMPLES` dict before assuming an example that needs a real API key or external service
is untested by design rather than by oversight.

If you add a new example, the smoke test's `test_every_example_is_accounted_for` will fail CI
until you add it to one of `RUN_TO_COMPLETION`, `LONG_RUNNING_SERVERS`, `PAIRED_LONG_RUNNING`, or
`EXCLUDED_EXAMPLES` (with a real reason) in that file.

---

## Quickstart

The gentlest on-ramp, in order:

| File | Demonstrates |
|---|---|
| `quickstart/01_hello_agent.py` | The absolute minimum: one agent, one message |
| `quickstart/02_supervised_agent.py` | A supervisor restarting a crashed agent |
| `quickstart/03_multi_agent.py` | Two agents talking to each other |
| `quickstart/04_with_llm.py` | An agent calling an LLM (mock by default, `--live` for a real Anthropic call) |

## Core runtime

| File | Demonstrates |
|---|---|
| `hello_agent.py` | Simplest possible agent |
| `supervised_agent.py` | Crash + auto-restart |
| `supervision_tree.py` | Nested supervision strategies |
| `supervision_introspection.py` | Querying live supervision status (`civitas.supervision.status`) |
| `dynamic_spawning.py` | An orchestrator spawning/despawning worker agents on demand |
| `non_blocking_spawn.py` | Spawning without blocking on a slow child's startup (`wait=False`) |
| `cross_process_spawn/` | Spawning a child into a **different OS process's** `DynamicSupervisor` — two scripts, see its own note below |
| `streaming_response.py` | Bus-native agent-to-agent streaming (`stream_reply()` / `.stream()`) |
| `custom_plugin.py` | Writing your own `ModelProvider` plugin from scratch |
| `stateful_workflow.py` | Checkpointing + crash recovery (requires `civitas-contrib`) |
| `rate_limiter.py` | Rate-limiting a downstream call |
| `research_pipeline.py`, `research_assistant.py` | Multi-agent pipelines (mock LLM by default) |
| `observable_pipeline.py` | Full OTEL tracing across a pipeline |
| `eval_agent.py` | Automated evaluation loop |
| `self_sufficient_agent.py` | An agent using a real LLM + tool (needs `ANTHROPIC_API_KEY`) |

## Patterns

| File | Demonstrates |
|---|---|
| `patterns/pipeline.py` | Linear multi-stage pipeline |
| `patterns/fan_out_fan_in.py` | Parallel fan-out, aggregated fan-in |
| `patterns/router.py` | Content-based routing to different agents |
| `patterns/human_in_the_loop.py` | Approval workflow (simulated, no real stdin) |

## Security

| File | Demonstrates |
|---|---|
| `secured_messaging.py` | Ed25519 message signing — topology config **and** the underlying cryptographic guarantee (sign → verify → tamper → rejected). See its module docstring for a real, currently-open gap: a live signed `ask()` over ZMQ times out, tracked in `docs/milestones.md`, not solved here |
| `gateway_auth.py` | HTTP gateway JWT bearer auth (missing / expired / valid token) |

## Gateway

| File | Demonstrates |
|---|---|
| `http_gateway.py` | HTTP gateway exposing an agent (`curl`-able) |
| `grpc_gateway.py` | The generic gRPC `Agent` service (`Invoke` over a real `grpc.aio` channel) |
| `mcp_agent.py` | Consuming tools from an MCP server (needs a real MCP server process) |
| `control_plane_auth.py` | Control-plane write actions (suspend/resume over HTTP) + the "bring your own auth" seam: your middleware sets `request.auth["principal"]`, civitas records it as the honest audit actor (v0.9.6) |

## Dashboard

| File | Demonstrates |
|---|---|
| `dashboard_demo/` | `civitas top`, and (v0.9.3) `civitas telemetry` — one topology now feeds all three observability surfaces below. See its own `README.md` for the walkthrough. |

## Observability

| File | Demonstrates |
|---|---|
| `observability/grafana/` | Real Prometheus + Grafana, fully provisioned (v0.9.3, A3) — `docker compose up` against `dashboard_demo/` gives a live cost/latency/error dashboard with zero manual clicking. See its own `README.md`. |
| `civitas telemetry` (no separate example — see `dashboard_demo/README.md`) | Native SQLite telemetry storage (v0.9.3, B1) + the Textual TUI (v0.9.3, B3) — real cost/rate charts, gauges, and a breakdown table, reading directly from a local SQLite directory, no live process required. |

## Framework adapters

| File | Demonstrates |
|---|---|
| `frameworks/langgraph_on_civitas.py` | Wrapping a LangGraph graph as an `AgentProcess` (needs `langgraph` + `civitas-contrib`) |
| `frameworks/openai_sdk_on_civitas.py` | Wrapping an OpenAI Agents SDK agent (needs `openai-agents` + `civitas-contrib`) |

## Deployment

Progressively more realistic deployment topologies — see `deployment/`'s own structure:

| Directory | Demonstrates |
|---|---|
| `deployment/level1_single_process/` | One process, in-process transport |
| `deployment/level2_multi_process/` | Two processes, ZMQ transport, same machine |
| `deployment/level3_distributed/` | Multiple machines, NATS transport (needs a real NATS server) |
| `deployment/level4_docker/` | Docker Compose (no standalone entrypoint — see its `docker-compose.yml`) |

### `cross_process_spawn/` — a note on run order

Unlike most multi-script examples, **start `run_supervisor.py` first**, then `run_worker.py`
second — the supervisor owns the ZMQ proxy (`zmq_start_proxy=True`); starting the worker first
means it tries to connect before any proxy exists, and the supervisor's own wait for the worker's
announcement just times out. `run_worker.py`'s own module docstring explains this in more detail.

```bash
python examples/cross_process_spawn/run_supervisor.py   # terminal 1, first
python examples/cross_process_spawn/run_worker.py       # terminal 2, second
```
