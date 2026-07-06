# Topology YAML

A topology file is a complete, declarative description of an Civitas system — supervision tree, transport, and plugins in one place. It can be version-controlled, diffed, and validated without running the system.

---

## Minimal example

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent:
        name: greeter
        type: myapp.agents.Greeter
```

Run it:

```bash
civitas run --topology topology.yaml
```

---

## Full schema

```yaml
# ─── Transport ────────────────────────────────────────────────────────────────
transport:
  type: in_process          # in_process | zmq | nats  (default: in_process)

  # ZMQ-specific (only when type: zmq)
  pub_addr: "tcp://127.0.0.1:5559"
  sub_addr: "tcp://127.0.0.1:5560"
  start_proxy: true         # start the XSUB/XPUB proxy in this process

  # NATS-specific (only when type: nats)
  servers: "nats://localhost:4222"   # single URL or list
  jetstream: false                   # enable durable subscriptions
  stream_name: AGENCY                # JetStream stream name (default: AGENCY)


# ─── Plugins ──────────────────────────────────────────────────────────────────
plugins:
  models:
    - type: anthropic                # built-in name, dotted path, or entrypoint
      config:
        default_model: claude-sonnet-4-6
        max_tokens: 4096
        max_retries: 3

  exporters:
    - type: console                  # built-in console exporter

  state:
    type: sqlite                     # in_memory | sqlite | dotted path
    config:
      db_path: agency_state.db


# ─── Supervision tree ─────────────────────────────────────────────────────────
supervision:
  name: root                         # supervisor name (used in logs and traces)
  strategy: ONE_FOR_ONE              # ONE_FOR_ONE | ONE_FOR_ALL | REST_FOR_ONE
  max_restarts: 3                    # crash limit within restart_window (default: 3)
  restart_window: 60.0               # sliding window in seconds (default: 60.0)
  backoff: CONSTANT                  # CONSTANT | LINEAR | EXPONENTIAL (default: CONSTANT)
  backoff_base: 1.0                  # initial backoff delay in seconds (default: 1.0)
  backoff_max: 60.0                  # maximum backoff cap in seconds (default: 60.0)

  children:
    # ── Nested supervisor ────────────────────────────────────────────────────
    - supervisor:
        name: research_sup
        strategy: ONE_FOR_ONE
        max_restarts: 5
        restart_window: 30.0
        backoff: EXPONENTIAL
        backoff_base: 0.5
        backoff_max: 30.0
        children:
          - agent:
              name: web_researcher
              type: myapp.agents.WebResearcher
              process: worker        # optional: assign to a named Worker process

    # ── Direct agent child ───────────────────────────────────────────────────
    - agent:
        name: orchestrator
        type: myapp.agents.Orchestrator

    # ── Compact inline form ──────────────────────────────────────────────────
    - agent: { name: summarizer, type: myapp.agents.Summarizer }
```

---

## Field reference

### `transport`

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | `in_process` | Transport implementation: `in_process`, `zmq`, or `nats` |
| `pub_addr` | string | `tcp://127.0.0.1:5559` | ZMQ only — proxy XSUB frontend address |
| `sub_addr` | string | `tcp://127.0.0.1:5560` | ZMQ only — proxy XPUB backend address |
| `start_proxy` | bool | `false` | ZMQ only — start the proxy in this process |
| `servers` | string or list | `nats://localhost:4222` | NATS only — server URL(s) |
| `jetstream` | bool | `false` | NATS only — enable JetStream durable subscriptions |
| `stream_name` | string | `AGENCY` | NATS only — JetStream stream name |

### `plugins`

| Field | Type | Description |
|---|---|---|
| `plugins.models` | list | One or more `ModelProvider` plugins |
| `plugins.models[].type` | string | Built-in name (`anthropic`, `litellm`), dotted path, or entrypoint |
| `plugins.models[].config` | dict | Constructor kwargs passed to the provider |
| `plugins.exporters` | list | One or more `ExportBackend` plugins |
| `plugins.exporters[].type` | string | Built-in name (`console`), dotted path, or entrypoint |
| `plugins.exporters[].config` | dict | Constructor kwargs |
| `plugins.state` | dict | One `StateStore` plugin |
| `plugins.state.type` | string | `in_memory`, `sqlite`, dotted path, or entrypoint |
| `plugins.state.config` | dict | Constructor kwargs (e.g. `db_path`) |

If `plugins` is omitted entirely, the runtime uses `InMemoryStateStore` and no model provider.

### `supervision`

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Supervisor name — must be unique across supervisors |
| `strategy` | string | `ONE_FOR_ONE` | Restart strategy |
| `max_restarts` | int | `3` | Maximum crashes allowed within `restart_window` |
| `restart_window` | float | `60.0` | Sliding window length in seconds |
| `backoff` | string | `CONSTANT` | Backoff policy between restarts |
| `backoff_base` | float | `1.0` | Initial backoff delay in seconds |
| `backoff_max` | float | `60.0` | Maximum backoff cap in seconds |
| `children` | list | required | List of `agent:` or `supervisor:` nodes |

Strategies and backoff values are case-insensitive in YAML (`one_for_one` and `ONE_FOR_ONE` are equivalent).

### `agent`

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Agent name — must be unique across the entire tree |
| `type` | string | Yes | Dotted Python class path: `myapp.agents.MyAgent` |
| `process` | string | No | Worker process name — assigns agent to a `Worker` |

### `http_gateway`

Exposes the Civitas bus as a REST API. Requires `pip install civitas[http]`.

```yaml
- name: api
  type: http_gateway
  config:
    host: "0.0.0.0"
    port: 8080
    request_timeout: 30.0
    routes:
      - path: /v1/chat
        agent: assistant
        method: POST
        mode: call           # call (sync reply) or cast (fire-and-forget)
      - path: /v1/users/{id}
        agent: user_agent
        method: GET
        mode: call
    middleware:
      - myapp.middleware.require_api_key
    docs:
      enabled: true          # default: true
      path: /docs            # default: /docs
```

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | `0.0.0.0` | Bind address |
| `port` | int | `8080` | HTTP/1.1 + HTTP/2 port |
| `port_quic` | int | — | QUIC/HTTP/3 UDP port (required when `enable_http3: true`) |
| `tls_cert` | string | — | Path to TLS certificate file |
| `tls_key` | string | — | Path to TLS key file |
| `request_timeout` | float | `30.0` | Seconds before upstream timeout (504) |
| `enable_http3` | bool | `false` | Start QUIC server alongside uvicorn (requires `civitas[http3]`) |
| `routes` | list | `[]` | Custom route list (see below); falls back to default routes if empty |
| `middleware` | list | `[]` | Dotted import paths to global middleware callables |
| `docs.enabled` | bool | `true` | Serve Swagger UI at `docs.path` and `/openapi.json` |
| `docs.path` | string | `/docs` | Path for Swagger UI |

**Route fields:**

| Field | Type | Default | Description |
|---|---|---|---|
| `path` | string | required | URL path, `{param}` segments extracted into payload |
| `agent` | string | required | Target agent name |
| `method` | string | required | HTTP method (`GET`, `POST`, etc.) |
| `mode` | string | `call` | `call` (sync, 200 reply) or `cast` (async, 202 accepted) |

**Default routes** (when no `routes:` block is defined):

| HTTP | Agent | Mode |
|---|---|---|
| `POST /agents/{name}` | `{name}` | call |
| `POST /agents/{name}/cast` | `{name}` | cast |
| `GET /agents/{name}/state` | `{name}` | call `{"__op__": "state"}` |

See [HTTP Gateway](gateway.md) for the full guide.

### `dynamic_supervisor`

A `DynamicSupervisor` node. Starts empty — children are spawned at runtime via `self.spawn()`. Always uses `ONE_FOR_ONE`.

```yaml
- type: dynamic_supervisor
  name: workers
  config:
    max_children: 20        # hard limit; SpawnError when reached (default: unlimited)
    max_total_spawns: 1000  # optional lifetime cap across all spawns (default: unlimited)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `max_children` | int | unlimited | Maximum live children at any one time |
| `max_total_spawns` | int | unlimited | Total spawns allowed over the lifetime of the supervisor |

See [Dynamic supervision](supervision.md#dynamic-supervision) for the full guide.

### `topology_server`

A supervised JSON HTTP management endpoint. Use it alongside a `DynamicSupervisor` to expose live topology state.

```yaml
- type: topology_server
  name: topo_server
  config:
    host: 127.0.0.1   # default
    port: 6789        # default
```

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | `127.0.0.1` | Bind address — use `0.0.0.0` to expose externally |
| `port` | int | `6789` | Port for the HTTP server |

Endpoints: `GET /health`, `GET /topology`, `GET /agents`, `GET /agents/{name}`. See [Dynamic supervision — TopologyServer](supervision.md#topologyserver) for details.

### `eval_agent`

Corrective observability loop that monitors agent behavior.

```yaml
- name: monitor
  type: eval_agent
  max_corrections_per_window: 10    # default: 10
  window_seconds: 60.0              # default: 60.0
```

See [EvalLoop](evalloop.md) for the full guide.

### `supervisor` (nested)

Same fields as the root `supervision` block, plus `children`.

---

## Agent type resolution

The `type` field in an `agent:` block is a dotted Python import path:

```yaml
- agent:
    name: researcher
    type: myapp.agents.WebResearcher   # importlib.import_module("myapp.agents") → WebResearcher
```

When loading programmatically via `Runtime.from_config()`, you can pass a short-name map to avoid dotted paths:

```python
runtime = Runtime.from_config(
    "topology.yaml",
    agent_classes={
        "WebResearcher": WebResearcher,
        "Orchestrator":  Orchestrator,
    },
)
```

With this map, the YAML can use short names:

```yaml
- agent: { name: researcher, type: WebResearcher }
```

---

## Process affinity

Mark agents with `process: <name>` to assign them to a `Worker` process:

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent:
        name: orchestrator
        type: myapp.Orchestrator
        # no process: — runs in the supervisor process

    - agent:
        name: researcher
        type: myapp.WebResearcher
        process: worker            # runs in a separate Worker process
```

Start the supervisor, then the worker:

```bash
# Terminal 1 — supervisor process
civitas run --topology topology.yaml

# Terminal 2 — worker process
civitas run --topology topology.yaml --process worker
```

Any string can be the process name. Multiple agents can share the same process name — they will all be hosted in the same Worker.

---

## Python DSL ↔ YAML equivalence

The Python DSL and YAML produce identical runtime behavior. Choose whichever fits your workflow.

```python
# Python DSL
Runtime(
    supervisor=Supervisor(
        "root",
        strategy="ONE_FOR_ONE",
        max_restarts=3,
        children=[
            Supervisor(
                "research_sup",
                strategy="ONE_FOR_ONE",
                max_restarts=5,
                backoff="EXPONENTIAL",
                backoff_base=0.5,
                children=[
                    WebResearcher("researcher"),
                ],
            ),
            Orchestrator("orchestrator"),
            Summarizer("summarizer"),
        ],
    ),
    transport="nats",
    nats_servers="nats://localhost:4222",
    model_provider=AnthropicProvider(default_model="claude-sonnet-4-6"),
    state_store=SQLiteStateStore("state.db"),
)
```

```yaml
# Equivalent YAML
transport:
  type: nats
  servers: "nats://localhost:4222"

plugins:
  models:
    - type: anthropic
      config:
        default_model: claude-sonnet-4-6
  state:
    type: sqlite
    config:
      db_path: state.db

supervision:
  name: root
  strategy: ONE_FOR_ONE
  max_restarts: 3
  children:
    - supervisor:
        name: research_sup
        strategy: ONE_FOR_ONE
        max_restarts: 5
        backoff: EXPONENTIAL
        backoff_base: 0.5
        children:
          - agent:
              name: researcher
              type: myapp.WebResearcher
    - agent: { name: orchestrator, type: myapp.Orchestrator }
    - agent: { name: summarizer,   type: myapp.Summarizer }
```

---

## CLI commands

### `civitas run`

```bash
civitas run --topology topology.yaml            # run as supervisor
civitas run --topology topology.yaml --process worker   # run as worker
civitas run --topology topology.yaml --transport nats   # override transport
civitas run --topology topology.yaml --nats-url nats://prod:4222  # override NATS URL
```

| Flag | Default | Description |
|---|---|---|
| `--topology`, `-t` | `topology.yaml` | Path to topology YAML |
| `--transport` | — | Override `transport.type` without editing the file |
| `--process`, `-p` | — | Run as a Worker hosting agents for this process name |
| `--nats-url` | — | Override `transport.servers` |

### `civitas topology validate`

Checks the topology file for structural and configuration errors before running:

```bash
civitas topology validate topology.yaml
```

```
  Validating topology.yaml

  Structure
  ✔ YAML syntax
  ✔ Supervision section present
  ✔ No empty supervisors
  ✔ Supervision tree well-formed

  Naming
  ✔ All agents named
  ✔ No duplicate names
  ✔ No agent/supervisor name conflicts

  Configuration
  ✔ Strategies valid (ONE_FOR_ALL, ONE_FOR_ONE)
  ✔ Backoff policies valid
  ✔ Transport config valid — nats

  ✔ Valid  4 agents · 2 supervisors · nats
```

Validation catches:
- Missing `supervision` section
- Empty supervisors (no children)
- Invalid strategy or backoff values
- Missing `name` or `type` on agents
- Duplicate agent names
- Agent/supervisor name conflicts
- Invalid transport type

Exit code `0` on success, `1` on validation failure. Safe to use in CI:

```bash
civitas topology validate topology.yaml || exit 1
```

### `civitas topology show`

Renders the supervision tree as a formatted tree with inline policies:

```bash
civitas topology show topology.yaml
```

```
  Civitas Topology: topology.yaml

  root ONE_FOR_ONE  restarts: 3/60.0s  backoff: constant
  ├── research_sup ONE_FOR_ONE  restarts: 5/30.0s  backoff: exponential
  │   └── researcher  myapp.WebResearcher  @worker
  ├── orchestrator  myapp.Orchestrator
  └── summarizer    myapp.Summarizer

  ──────────────────────────────────────────
  Transport   nats  nats://localhost:4222
  Plugins     anthropic  sqlite
  Topology    3 agents  ·  2 supervisors  ·  1 processes
```

### `civitas topology diff`

Shows meaningful differences between two topology files — useful for reviewing changes before deployment:

```bash
civitas topology diff staging.yaml production.yaml
```

```
  Diff: staging.yaml → production.yaml

  Transport
  ~  transport/@type                       zmq → nats
  +  transport/@servers                    nats://prod:4222

  Supervision
  ~  /root/research_sup/@max_restarts      3 → 5
  ~  /root/research_sup/@backoff           CONSTANT → EXPONENTIAL

  2 changed  ·  1 added
```

Differences are grouped by section (Transport, Plugins, Supervision) with `+` (added), `-` (removed), and `~` (changed) indicators.

---

## Complete production example

```yaml
# production.yaml — NATS transport, Anthropic LLM, SQLite state, multi-process

transport:
  type: nats
  servers: "nats://prod-nats:4222"
  jetstream: true
  stream_name: AGENCY

plugins:
  models:
    - type: anthropic
      config:
        default_model: claude-sonnet-4-6
        max_tokens: 8192
        max_retries: 3

  exporters:
    - type: console

  state:
    type: sqlite
    config:
      db_path: /data/agency_state.db

supervision:
  name: root
  strategy: ONE_FOR_ONE
  max_restarts: 3
  restart_window: 60.0
  backoff: EXPONENTIAL
  backoff_base: 2.0
  backoff_max: 60.0
  children:
    - supervisor:
        name: research_sup
        strategy: ONE_FOR_ONE
        max_restarts: 5
        restart_window: 30.0
        backoff: EXPONENTIAL
        backoff_base: 0.5
        backoff_max: 30.0
        children:
          - agent:
              name: web_researcher
              type: myapp.agents.WebResearcher
              process: worker

    - supervisor:
        name: execution_sup
        strategy: ONE_FOR_ALL
        max_restarts: 3
        restart_window: 60.0
        children:
          - agent:
              name: api_caller
              type: myapp.agents.ApiCaller
              process: worker
          - agent:
              name: db_writer
              type: myapp.agents.DbWriter
              process: worker

    - agent:
        name: orchestrator
        type: myapp.agents.Orchestrator

    - agent:
        name: summarizer
        type: myapp.agents.Summarizer
```

Start it:

```bash
# Supervisor process (Machine A)
civitas run --topology production.yaml

# Worker process (Machine B)
civitas run --topology production.yaml --process worker
```

---

## Tips

**Environment variable substitution** is not built into the YAML loader. Keep secrets out of topology files — pass them via environment variables and read them in your plugin constructors or agent `on_start()`:

```yaml
# Good — no secrets in YAML
plugins:
  models:
    - type: anthropic    # reads ANTHROPIC_API_KEY from environment

# Bad — secret in YAML, committed to git
plugins:
  models:
    - type: anthropic
      config:
        api_key: sk-ant-...
```

**Case insensitivity** applies to strategy and backoff values only. Field names (`name`, `type`, `children`, etc.) are case-sensitive.

**Flat agent format** is accepted as a shorthand for simple cases:

```yaml
# Verbose
- agent:
    name: greeter
    type: myapp.Greeter

# Compact — identical behavior
- agent: { name: greeter, type: myapp.Greeter }
```
