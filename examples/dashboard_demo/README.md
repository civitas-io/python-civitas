# `civitas top` dashboard demo

A small, deliberately noisy topology that lights up every part of the v0.9.1
dashboard: a chatty agent reporting fake LLM cost/tokens, a flaky agent that
crashes and restarts on a timer, and a spawner that spins dynamic children up
and down. Not a realistic workload — just enough motion to see the feature
working.

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
  inspected statically with `civitas topology show`.
- `agents.py` — `ChattyWorker`, `FlakyWorker`, `SpawnerAgent`.
