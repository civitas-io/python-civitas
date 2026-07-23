# Choosing Your Configuration

Every knob in Civitas answers a question about *your* failure modes and load profile. This page
is the decision guide: which setting, when, and why — with the trade-off you're accepting.

---

## Restart strategy — "what should die together?"

| Your children are… | Strategy | Example |
|---|---|---|
| Independent — one's crash means nothing to the others | `ONE_FOR_ONE` *(default, most common)* | A pool of scraper agents; unrelated service agents |
| Interdependent — they share warm state or a session and must reconverge together | `ONE_FOR_ALL` | An agent + its dedicated cache agent; a trio that renegotiates a session on start |
| A pipeline — downstream output depends on upstream state | `REST_FOR_ONE` | ingest → enrich → publish: if *enrich* crashes, *publish* must restart too, but *ingest* keeps running |

**Rule of thumb:** start with `ONE_FOR_ONE`. Reach for the others only when a restarted child
would produce wrong results *because* a sibling kept its old state.

```yaml
supervision:
  name: pipeline
  strategy: REST_FOR_ONE
  children:
    - agent: { name: ingest,  type: myapp.Ingest }
    - agent: { name: enrich,  type: myapp.Enrich }
    - agent: { name: publish, type: myapp.Publish }
```

## Restart budget & backoff — "how hard should we try?"

`max_restarts` / `restart_window` is the supervisor-wide *intensity* budget: more than
`max_restarts` crashes inside `restart_window` seconds ⇒ escalate to the parent (which restarts
this whole subtree with a fresh budget — v0.8.0).

| Situation | Setting | Why |
|---|---|---|
| Crashes are transient (network blips, rate limits) | `max_restarts=5, restart_window=60, backoff="EXPONENTIAL"` | Ride out the blip; exponential + jitter prevents thundering-herd re-crashes |
| Crashes indicate bad input that will recur | Keep the budget *small* (`max_restarts=2`) | Fail fast upward — restarts won't fix a poison message; handle it in `on_error` with `SKIP` instead |
| A child talks to a slow-to-recover dependency (DB failover ~30 s) | `backoff="EXPONENTIAL", backoff_base=2.0, backoff_max=30.0` | Restarting before the dependency is back just burns budget |
| Dev / tests | `backoff="CONSTANT", backoff_base=0.01` | Fast feedback |

**Escalation is a ladder, not a cliff:** child supervisor exhausts → parent restarts the subtree
(fresh budget) → repeated subtree failures burn the *parent's* budget → and so on to the root.
Nest supervisors so blast radius grows one level at a time.

## `handle_timeout` — "can this agent hang?"

Opt-in per agent; off by default.

| Agent profile | Setting |
|---|---|
| Calls external services with client-side timeouts already configured | Leave off — double timeouts add confusion |
| LLM/tool chains with no reliable client timeout | `handle_timeout` ≈ 2–3× your slowest legitimate chain |
| Message-loop workers that should always be sub-second | `handle_timeout=5.0` — cheap insurance against a stuck await |
| Streaming producers (`stream_reply`) | Leave off, or set ≥ max stream duration — the timeout bounds the *whole* `handle()` |

It catches **stuck awaits only** — blocking code never yields, so it's invisible to the watchdog
(see [Messaging → hazards](messaging.md#handle_timeout-what-it-can-and-cannot-catch)).

## Error handling — "what does this failure mean?"

`on_error()` is your failure classifier. Map exception → meaning → action:

```python
async def on_error(self, error: Exception, message: Message) -> ErrorAction:
    if isinstance(error, RateLimitError):
        await asyncio.sleep(2 ** message.attempt)   # backoff, then ordered in-place retry
        return ErrorAction.RETRY
    if isinstance(error, ValidationError):
        logger.warning("poison message %s dropped", message.id)
        return ErrorAction.SKIP                      # restarts won't fix bad input
    if isinstance(error, TimeoutError):              # handle_timeout fired
        return ErrorAction.ESCALATE                  # hung = corrupted; let it crash
    return ErrorAction.ESCALATE                      # unknown = crash (the safe default)
```

| Action | Use when | Cost you accept |
|---|---|---|
| `RETRY` | Transient, likely to succeed soon | Agent blocked between attempts (FIFO preserved) |
| `SKIP` | Poison input; restart can't help | The message's work never happens |
| `ESCALATE` *(default)* | Unknown / state may be corrupted | Restart: un-checkpointed state is discarded |
| `STOP` | The agent's job is done or unrecoverable by restart | Agent leaves the tree gracefully |

## Transport — "how far apart are your agents?"

| Level | Transport | Choose when | You give up |
|---|---|---|---|
| 1 | `in_process` *(default)* | One Python process is enough; dev, tests, most single-node apps | Nothing — start here |
| 2 | `zmq` | Multi-*process* on one machine: isolate a CPU-heavy or crash-prone agent into a Worker | Brokerless — you manage the proxy addresses |
| 3 | `nats` | Multi-*machine*, or you want a broker with reconnect/durability (`jetstream: true`) | Running a NATS server |

The same agent code runs at every level — switch in `topology.yaml`, not in code. Move up a level
when: (a) one agent's CPU work stalls others (Level 1 → 2), or (b) you need horizontal scale or
machine-level fault isolation (→ 3).

## State persistence — "what must survive?"

**The contract: only checkpointed state survives a restart** ([details](messaging.md#the-restart-state-contract-v080)).

| Need | Store | Notes |
|---|---|---|
| Survive supervisor restarts (same process) | `InMemoryStateStore` *(default)* | Zero setup; dies with the process |
| Survive process restarts / deploys | `sqlite` (contrib) | Single node; give Workers the same `db_path` |
| Survive node loss; shared across machines | `postgres` (contrib) | The production choice for distributed trees |
| State contains secrets / PII at rest | wrap with `EncryptingStateStore` (`civitas[encryption]`) | Same `CIVITAS_STATE_KEY` on every process; key loss = data loss |

Checkpoint **after completing a unit of work**, not on every mutation — the checkpoint is your
recovery point, and a half-done unit checkpointed is a half-done unit restored.

## Mailbox sizing — "what's your burst profile?"

`mailbox_size` (default 1000) is your backpressure valve: a full mailbox **blocks senders**.

- **Bursty producers, steady consumer:** size ≥ your worst burst, or the burst back-propagates.
- **Steady flow:** default is fine; if the mailbox trends full, the agent is under-provisioned —
  scale with dynamic spawning or a Worker, don't just widen the buffer.
- **Never** size it "huge to be safe": a 100k-deep mailbox is 100k messages of latency and a
  restart-survival liability. Bounded-and-blocking is a feature; see
  [backpressure hazards](messaging.md#bounded-mailboxes-backpressure-can-deadlock-too).

## Static children vs. dynamic spawning — "is the population fixed?"

| | Static children | `DynamicSupervisor` + `spawn()` |
|---|---|---|
| Population | Known at deploy time | Per-task / per-tenant / elastic |
| Config | Topology YAML | `await self.spawn(WorkerAgent, f"job-{id}", config={...})` |
| Governance | — | `max_children`, per-spawner quotas, `spawner_allowlist`, `on_spawn_requested` veto hook |

Use dynamic spawning for fan-out work (one child per document/job/session); cap it: set
`max_children` to what your process can actually host, and per-spawner quotas
(`max_children_per_spawner`) when multiple agents share the pool. Prefer `wait=False`
(`spawn_nowait`) for bulk spawning — a slow `on_start()` otherwise blocks the spawn call.

## Suspension — "when do humans gate the loop?"

`runtime.suspend(name, reason)` / `resume(name, approver)` is the built-in HITL primitive:

- **Use it for**: approval gates before irreversible actions; incident freeze ("stop that agent
  *now*, keep its mailbox"); staged rollouts.
- **Semantics**: takes effect at the next message boundary (never mid-`handle()`); business
  messages buffer (senders eventually feel backpressure); heartbeats still ack'd (suspended ≠
  crashed); the marker is checkpointed, so restarts stay suspended; `resume()` **requires a named
  approver** — that's your audit trail.
- **Don't use it as** flow control — that's what bounded mailboxes are for.

```python
await runtime.suspend("trader", reason="daily-loss-limit-hit")
# ... human reviews ...
await runtime.resume("trader", approver="risk-officer@example.com")
```

## GenServer vs. AgentProcess — "is it an agent or a service?"

| | `AgentProcess` | `GenServer` |
|---|---|---|
| Identity | Autonomous actor: reasons, calls LLMs/tools | Stateful *service*: registry, counter, session store, rate-limiter |
| API | `handle()` + `send`/`ask` | `handle_call` (sync) / `handle_cast` (async) / `handle_info` (timers via `send_after`) |
| LLM/tools injected | Yes | No |

If you're writing `if message.payload["op"] == "get": ... elif op == "put": ...` inside an
`AgentProcess`, you wanted a `GenServer`.

## Security — "which tier are you deploying?"

| Deployment | Turn on |
|---|---|
| Single process, trusted host | Nothing — `in_process` never crosses a wire |
| Multi-process/machine on a shared network | `security.signing` (Ed25519 per-agent message signing) + transport encryption (ZMQ CURVE / NATS TLS) |
| Exposed HTTP/WS/gRPC edge | Gateway auth: `require_jwt` and/or mTLS (`client_cert_mode` / `mtls_source: proxy_header` behind a TLS-terminating proxy) — see [Gateway](gateway.md) |
| Compliance / forensics | `audit:` sink (JSONL file, syslog, or OTLP) — every route, spawn, suspend/resume, and secret access is an event |
| Secrets in topology YAML | `${VAR}` substitution — never literals in the file |

## Observability — "console, or collector?"

- **Dev:** nothing to configure — spans print to console.
- **Prod:** set `OTEL_EXPORTER_OTLP_ENDPOINT` → Jaeger/Grafana/anything OTLP. Zero code changes.
- Watch for: `civitas.agent.retry` spans (transient-failure rate), `supervisor.restart` spans
  (crash rate — alert on bursts), `civitas.handle.timeout=true` attribute (hung vs. buggy).

---

## Worked profile: a production research service

```yaml
transport: { type: nats, url: nats://prod:4222, jetstream: true }

plugins:
  state: { type: postgres, config: { dsn: ${DATABASE_URL} } }
  models:
    - { type: anthropic, config: { default_model: claude-sonnet-4-6 } }

security:
  signing: { enabled: true }

audit: { type: jsonl, path: /var/log/civitas/audit.jsonl }

supervision:
  name: root
  strategy: ONE_FOR_ONE           # top level: services are independent
  max_restarts: 5
  restart_window: 60
  backoff: EXPONENTIAL
  children:
    - agent: { name: api, type: myapp.Gateway }        # HTTPGateway subclass
    - supervisor:
        name: research                                  # the pipeline dies together
        strategy: REST_FOR_ONE
        children:
          - agent: { name: planner,    type: myapp.Planner,    handle_timeout: 120 }
          - agent: { name: researcher, type: myapp.Researcher, handle_timeout: 300, process: worker }
          - agent: { name: writer,     type: myapp.Writer,     handle_timeout: 120 }
    - name: jobs                                        # elastic fan-out pool
      type: dynamic_supervisor
      max_children: 50
      max_children_per_spawner: 10
```

Why each choice: NATS+JetStream (multi-machine, broker durability) · Postgres store (state
survives node loss) · signing on (shared network) · REST_FOR_ONE for the pipeline (writer output
depends on researcher state) · researcher in a Worker (heaviest CPU/IO, isolate it) ·
`handle_timeout` sized per stage · dynamic pool capped at what a node hosts.
