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

# name may also be a path — parents are created, the basename is the project name
civitas init apps/nested/my_agents
civitas init /abs/path/to/my_agents
```

| Argument | Default | Description |
|----------|---------|-------------|
| `name` | required | Project name, or a path ending in it — the basename must be a valid Python identifier (it names the module and agent class) |
| `--dir` | cwd | Parent directory; a relative path in `name` is joined under it |

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

Launch **`civitas top`** — a live, mouse-clickable Textual dashboard for an already-running
topology (v0.9.1 rebuild; see [design/dashboard-v2.md](design/dashboard-v2.md) for the full
design). It attaches remotely over HTTP to the topology's `topology_server` node and polls
`/topology`, `/snapshot`, and `/processes` independently — it does not start a runtime of its own.
(v0.9.3: `/snapshot` was `/metrics` at the time this section was first written — renamed to make
room for real Prometheus text-format exposition at the standard `/metrics` scrape path; see
[observability.md](observability.md#prometheus-metrics-v0931).)

```bash
civitas dashboard <topology.yaml> [<topology2.yaml> ...] [--refresh <seconds>]
```

**v0.9.4: multiple topologies.** Given more than one topology file, each gets its own tab
(labeled after the file's own name) — all attached to and polled concurrently, switchable
instantly since every tab's data is already live in the background, not fetched on demand.
A single topology (the common case) looks exactly as it always has — no tab bar at all.

| Argument / Option | Default | Description |
|-------------------|---------|-------------|
| `topologies` | — | Path(s) to one or more topology YAML files (required, variadic). Each must declare a `topology_server` node. |
| `--refresh` / `-r` | `1.0` | Poll interval in seconds |

Requires the `dashboard` extra:

```bash
pip install 'civitas[dashboard]'
```

Without it, the command still appears in `--help`, but exits with a clear install instruction the
moment you try to run it.

### Layout

Three equally-sized panes, all visible at once:

- **Tree** (left) — the live supervision tree. Click any agent or supervisor to focus it in the
  detail pane. Dynamically-spawned children (via `DynamicSupervisor`) appear and disappear live.
  Status dots follow a fixed color convention: green = running, yellow = starting/stopping, red =
  crashed, grey = suspended or stopped. A supervisor's crash count within its restart window, and
  a child's own restart count, render inline in amber when non-zero.
- **Detail** (middle) — status, uptime, capabilities, restart count, a **session** row (v0.9.4;
  turn count + duration since this incarnation's first LLM call, only shown once it's made one —
  see [design/dashboard-v2.md](design/dashboard-v2.md#16-p1s-deferred-session-length-shipped-v094)
  for the exact definition, including why it deliberately resets on restart), and — for agents
  reporting LLM usage via `llm_span()` — messages handled/sent, tokens in/out, cost, and last
  model used, for whichever node is currently focused.
- **Processes** (right) — one row per OS process (the runtime itself, plus every distinct Worker
  in a multi-process topology), each with a proportional CPU% gauge bar and RSS memory.

Press `Ctrl+P` for Textual's built-in command palette, including a live light/dark theme switcher.
Press `f` to toggle focus/expand mode (v0.9.4) — widens the detail pane at the expense of the
other two, which stay visible (not hidden). Requires a node to already be selected; a no-op
otherwise. Press `f` again to return to the equal three-pane layout.
Press `q` to quit; the topology you attached to keeps running.

If the topology server becomes unreachable, a banner names the failing endpoint(s) and the
dashboard keeps retrying — it does not exit.

### Try it

`examples/dashboard_demo/` is a small, deliberately noisy topology built to exercise every part of
the dashboard (a crashing agent, a fake-LLM-calling agent, an agent that spawns/despawns dynamic
children) — see its `README.md` for a two-terminal walkthrough.

!!! note
    A single-process topology (the common case) shows one row in the Processes pane — agents
    share one OS process, so per-agent CPU/memory is not real data and is not shown. Run a
    multi-process topology (see [`examples/deployment`](https://github.com/civitas-io/python-civitas/tree/main/examples/deployment))
    to see more than one row there.

## civitas telemetry

Launch the live Textual telemetry TUI over B1/B2's native SQLite store (v0.9.3, B3; see
[design/telemetry-native.md](design/telemetry-native.md) for the full design). Unlike `civitas
dashboard`, this does **not** attach to a live process — it reads directly from a local SQLite
directory, so it works even after the app that wrote the data has stopped.

```bash
civitas telemetry [<db-dir>] [--since <range>] [--window-days <n>] [--refresh <seconds>]
```

| Argument / Option | Default | Description |
|-------------------|---------|-------------|
| `db_dir` | `./civitas_telemetry` | Path to the telemetry SQLite directory |
| `--since` | `24h` | A duration shorthand (`1h`, `24h`, `7d`, `30d`) or an ISO datetime (e.g. `2026-07-01`). Also changeable interactively in the TUI. |
| `--window-days` | `30` | Must match the `SQLiteBackend`'s own `window_days` setting |
| `--refresh` / `-r` | `30.0` | Re-query interval in seconds |

Requires the `telemetry` extra:

```bash
pip install 'civitas[telemetry]'
```

Without it, the command still appears in `--help`, but exits with a clear install instruction the
moment you try to run it.

### Layout

- **Cost over time** / **Message rate over time** (top row) — real line charts (via
  `textual-plotext`), one series per agent (+ model, for cost). Capped at the top 6 series by
  total value — a real multi-agent/multi-model deployment's full cardinality would make a terminal
  legend unreadable well before that.
- **Totals** / **Breakdown** (bottom row) — at-a-glance total spend/messages/top-agent, and a
  per-agent + per-model cost table.

Time range: press `h`/`d`/`w`/`m` to switch between 1h/24h/7d/30d presets interactively (recomputed
against "now" each refresh), or `r` for an immediate manual refresh. `q` to quit.

### Try it

Run any topology with a `SQLiteBackend` exporter configured (see
[observability.md](observability.md#native-sqlite-storage-v093x-track-b-b1)), then point `civitas
telemetry` at the same `db_dir` — it reads live, even while the app is still running and writing.
