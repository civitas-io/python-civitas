# CLI Reference

The `civitas` CLI manages the full lifecycle of an agent system — from scaffolding a new project to running, inspecting, and deploying it.

```
civitas [command] [subcommand] [options]
```

---

## civitas version

Print the installed version.

```bash
civitas version
```

---

## civitas init

Scaffold a new Civitas project in the current directory (or a named subdirectory).

```bash
civitas init <name> [--dir <directory>]
```

| Argument | Default | Description |
|----------|---------|-------------|
| `name` | required | Project name — must be a valid Python identifier |
| `--dir` | `./<name>` | Directory to create the project in |

**Generated files:**

```
<name>/
├── pyproject.toml      # project metadata and dependencies
├── topology.yaml       # supervision tree and transport config
├── agents.py           # starter AgentProcess implementation
├── run.py              # entry point — calls civitas.Runtime
└── README.md
```

---

## civitas run

Start a Civitas runtime from a topology file.

```bash
civitas run [--topology <path>] [--transport <type>] [--process <name>] [--nats-url <url>]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--topology` | `topology.yaml` | Path to topology YAML file |
| `--transport` | from topology | Override transport: `in_process`, `zmq`, `nats` |
| `--process` | — | Run only the agents assigned to this process group (worker mode) |
| `--nats-url` | `nats://localhost:4222` | NATS server URL (only used with `--transport nats`) |

**Supervisor mode** — omit `--process` to run the full runtime including the supervision tree:

```bash
civitas run --topology topology.yaml
```

**Worker mode** — specify `--process` to run agents assigned to a process group. Used in multi-process deployments where each OS process hosts a subset of agents:

```bash
# Terminal 1 — supervisor process
civitas run --topology topology.yaml

# Terminal 2 — worker for process group "inference"
civitas run --topology topology.yaml --process inference
```

Civitas handles `SIGINT` and `SIGTERM` gracefully — on interrupt, all agents are stopped cleanly before the process exits.

---

## civitas topology

Commands for inspecting and comparing topology files.

### civitas topology validate

Validate a topology YAML file for syntax errors and structural issues.

```bash
civitas topology validate <path>
```

Checks performed:

- YAML syntax
- Supervision tree well-formedness (no empty supervisors, no duplicate names)
- Valid supervision strategies: `ONE_FOR_ONE`, `ONE_FOR_ALL`, `REST_FOR_ONE`
- Valid backoff policies: `CONSTANT`, `LINEAR`, `EXPONENTIAL`
- `max_restarts` is a non-negative integer
- Valid transport types: `in_process`, `zmq`, `nats`
- No naming conflicts between agents and supervisors

Exits with a non-zero status code if any errors are found — suitable for use in CI:

```bash
civitas topology validate topology.yaml && echo "topology ok"
```

### civitas topology show

Visualise the supervision tree from a topology file.

```bash
civitas topology show <path>
```

Renders a Rich tree in the terminal showing:

- Supervisor names, strategies, restart limits, and backoff policies
- Agent names and types
- `DynamicSupervisor` nodes with `[dyn]` marker and `max_children` annotation
- `TopologyServer` nodes with `[topo]` marker and bind address
- Process affinity annotations (`@process`)
- Summary footer: transport type, plugin count, agent/supervisor/process counts

**Live mode:** If the topology file declares a `topology_server` node, `topology show` pings `GET /topology` on startup (1 second timeout). When the runtime is running, it renders the **live** tree with real-time agent statuses and dynamic child counts. When unreachable, it falls back to the static YAML tree with a `(runtime not running)` annotation.

**Example output (live):**

```
Civitas Topology: topology.yaml  (live)

root  ONE_FOR_ONE
├── orchestrator  RUNNING
└── workers  dynamic  live: 2/20  status: RUNNING  [dyn]
    ├── researcher-0  RUNNING
    └── researcher-1  RUNNING
```

**Example output (static fallback):**

```
Civitas Topology: topology.yaml  (runtime not running)

root  ONE_FOR_ONE  restarts: 3/60s  backoff: constant
├── topo_server  http://127.0.0.1:6789  [topo]
├── workers  dynamic  max_children: 20  [dyn]
└── orchestrator  myapp.agents.OrchestratorAgent
```

### civitas topology diff

Show what changed between two topology files.

```bash
civitas topology diff <file_a> <file_b>
```

Groups differences by category — Supervision, Transport, Plugins — and shows additions (`+`), removals (`-`), and changes (`~`):

```
Supervision
  ~ root.strategy: ONE_FOR_ONE → ONE_FOR_ALL
  + root.children.monitor

Transport
  ~ type: zmq → nats

Summary: 1 change, 1 addition, 0 removals
```

Useful for reviewing topology changes in pull requests.

---

## civitas deploy

Commands for generating deployment artefacts.

### civitas deploy docker-compose

Generate a Docker Compose deployment from a topology file.

```bash
civitas deploy docker-compose [--topology <path>] [--output <dir>]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--topology` | `topology.yaml` | Path to topology YAML file |
| `--output` | `./deploy` | Directory to write generated files into |

**Generated files:**

```
deploy/
├── docker-compose.yml   # one service per process group + NATS if needed
├── Dockerfile           # Python 3.12-slim base image
├── .env                 # runtime environment variables
└── topology.yaml        # copy of your topology file
```

**docker-compose.yml** includes:
- One service per process group (derived from `process:` annotations in the topology)
- A NATS service with a healthcheck if the topology transport is `nats`
- Each worker service labelled with its assigned agent names

**Environment variables** written to `.env`:
- `AGENCY_SERIALIZER` — serialization format
- `NATS_URL` — NATS connection string
- Plugin-specific API key placeholders (fill these in before deploying)

---

## civitas state

Inspect and manage persisted agent state in the local SQLite store.

### civitas state list

List all agents with persisted state.

```bash
civitas state list [--db <path>]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `agency_state.db` | Path to the SQLite database file |

Renders a table with agent names and their current state objects.

### civitas state clear

Clear persisted state for one or all agents.

```bash
civitas state clear [agent_name] [--db <path>] [--force]
```

| Argument / Option | Default | Description |
|-------------------|---------|-------------|
| `agent_name` | — | Name of a specific agent to clear; omit to clear all |
| `--db` | `agency_state.db` | Path to the SQLite database file |
| `--force` | `False` | Skip confirmation prompt |

Without `--force`, you are prompted to confirm before state is deleted.

---

## civitas dashboard

Launch a live terminal dashboard showing real-time agent statuses.

```bash
civitas dashboard [--topology <path>] [--refresh <seconds>]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--topology` | `topology.yaml` | Path to topology YAML file |
| `--refresh` | `1.0` | Dashboard refresh rate in seconds |

The dashboard instruments the supervisor's crash handler to track restarts. Press `Ctrl+C` to exit cleanly.

!!! note
    The dashboard requires the runtime to be running in a separate process. It connects to the running system rather than starting one itself.
