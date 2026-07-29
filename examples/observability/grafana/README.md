# Grafana + Prometheus for civitas (v0.9.3, A3)

A fully-provisioned Prometheus + Grafana stack for civitas's real Prometheus metrics
(`docs/observability.md`'s "Prometheus metrics" section). No manual clicking after
`docker compose up` — the Prometheus datasource and the civitas dashboard are both
pre-provisioned.

> **Scope note**: the original backlog item for this deliverable said "OTel-collector
> config" — that's not actually needed here. civitas's `/metrics` is scraped *directly* by
> Prometheus (pull-based); an OTel Collector is only relevant to the separate trace/OTLP
> path already documented in `docs/observability.md`'s Mode 3. A provisioned
> Prometheus+Grafana stack is the more directly useful, actually-runnable deliverable for
> the metrics side.

## Quick start

Terminal 1 — run a real civitas app exposing `/metrics` (the existing dashboard demo
already generates realistic cost/latency/restart/error data):

```bash
cd ../../dashboard_demo
uv run python -m civitas run --topology topology.yaml
```

This starts a `TopologyServer` on `127.0.0.1:6789` (`/metrics` included) alongside
`ChattyWorker` (fake LLM calls with real cost/token numbers every ~2s) and `FlakyWorker`
(crashes roughly every ~8s, so restarts/errors show up too).

Terminal 2 — bring up the stack:

```bash
cd examples/observability/grafana
docker compose up
```

Then open **http://localhost:3000** (anonymous admin access is enabled for this demo
stack only — see `docker-compose.yml`'s `GF_AUTH_ANONYMOUS_*` — never do this in a real
deployment). The "Civitas — Runtime Metrics" dashboard is already there under
Dashboards, already pointed at the already-configured Prometheus datasource, already
showing live data within a few seconds.

## What's in the dashboard

| Panel | Query | What it shows |
|---|---|---|
| Message throughput | `rate(civitas_messages_handled_total[1m])` | Messages/sec per agent |
| Error rate | `rate(civitas_agent_errors_total[1m])` | `handle()` errors/sec per agent |
| LLM cost over time | `increase(civitas_llm_cost_usd_total[5m])` | **The actual cost-tracking value proposition** — spend per agent per model, in 5-minute buckets |
| Average latency | `rate(civitas_message_latency_ms_sum[1m]) / rate(civitas_message_latency_ms_count[1m])` | The honest "sum/count" average over any time window — civitas doesn't track real histogram buckets, so this isn't a fabricated histogram |
| Agent status | `civitas_agent_status == 1` | Current status per agent, as a table |
| Total LLM spend / restarts / uptime | `sum(...)`/instant queries | At-a-glance stat panels |

## Pointing this at your own app instead

Edit `prometheus.yml`'s `targets` to your own `TopologyServer`'s `host:port`. On Linux
without Docker Desktop, `host.docker.internal` needs the `extra_hosts` entry already
present in `docker-compose.yml`'s `prometheus` service (or run civitas inside the same
compose network instead of on the host).

## Importing into an existing Grafana instance

`provisioning/dashboards/civitas.json` is a standard Grafana dashboard export — importable
via Grafana's own Dashboards → Import UI. It hardcodes the datasource UID
(`civitas-prometheus`) to match this stack's own provisioned datasource; importing into a
different Grafana instance will prompt you to remap it to your own Prometheus datasource.
