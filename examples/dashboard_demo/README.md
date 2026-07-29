# `civitas top` dashboard demo

A small, deliberately noisy topology that lights up every part of the v0.9.1
dashboard: a chatty agent reporting fake LLM cost/tokens, a flaky agent that
crashes and restarts on a timer, and a spawner that spins dynamic children up
and down. Not a realistic workload — just enough motion to see the feature
working.

Since v0.9.3.5, this same topology also feeds a `SQLiteBackend` exporter
(`plugins.exporters:` in `topology.yaml`) — one live run now serves THREE
observability surfaces: `civitas top` (below), the Grafana/Prometheus stack
(`examples/observability/grafana/`), and `civitas telemetry` (also below).

## Prerequisites

```bash
pip install 'civitas[dashboard]'   # or: uv sync --extra dashboard
```

## Run it (two terminals, from the repo root)

**Terminal 1 — start the runtime:**

```bash
civitas run --topology examples/dashboard_demo/topology.yaml
```

Leave this running. It hosts the `topology_server` the dashboard attaches to.

**Terminal 2 — attach the dashboard:**

```bash
civitas dashboard examples/dashboard_demo/topology.yaml
```

## Also try: `civitas telemetry`

With Terminal 1 still running (writing real cost data to `./civitas_telemetry_demo/`
by default), open a third terminal:

```bash
pip install 'civitas[telemetry]'   # or: uv sync --extra telemetry
civitas telemetry civitas_telemetry_demo
```

You'll see real cost-over-time and message-rate charts, a per-agent/per-model
breakdown table (including the dynamically-spawned `job-N` children), and
total-spend/top-agent stats — all reading live from the same directory
`chatty`/`spawner`'s children are writing to. Unlike `civitas top`/`civitas
dashboard`, this does NOT need Terminal 1 to still be running — it reads a
local SQLite directory directly, so it works even after you stop the runtime.

## What to look at

- **Left pane (tree)** — click `chatty`, `flaky`, `workers`, or a `job-N` node.
  `flaky` will flip red and its restart_count will climb every ~4-8s; `workers`
  shows `(dynamic, N live)` changing as `spawner` spawns/despawns `job-N`
  children every ~3s.
- **Middle pane (detail)** — click `chatty` to watch `tokens in/out`, `cost_usd`,
  and `last_model` update every ~2s.
- **Right pane (processes)** — CPU/memory gauge bar for the runtime process
  hosting everything (this demo runs single-process, so there's one row; see
  `examples/deployment/level2_multi_process` for a multi-process topology if
  you want to see more than one row here).
- **Ctrl+P** — Textual's built-in command palette, including a live light/dark
  theme switcher (try `catppuccin-latte` for a light theme).
- **q** — quit the dashboard (the runtime in Terminal 1 keeps running).

## Files

- `topology.yaml` — the topology; also runnable directly with `civitas run` or
  inspected statically with `civitas topology show`. Declares the
  `SQLiteBackend` exporter (v0.9.3.5) alongside the `topology_server`.
- `agents.py` — `ChattyWorker`, `FlakyWorker`, `SpawnerAgent`.
- `civitas_telemetry_demo/` — created at runtime by the `SQLiteBackend`
  exporter (git-ignored, not checked in).
