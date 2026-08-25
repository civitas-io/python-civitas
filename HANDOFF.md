# Handoff: python-civitas

**Purpose of this doc:** resume work cold, after a context compaction, without re-deriving
anything already decided. Read this first, then follow the links — don't re-read the whole repo
linearly. Deep, dated engineering history lives in [`CHANGELOG.md`](CHANGELOG.md) and
[`docs/milestones.md`](docs/milestones.md) (Part 1 = shipped history, do not edit; Part 2 =
the active backlog).

**Cross-project context**: this project is one of three real pillars in the `civitas-io` org
(Civitas = runtime, this repo, Presidium = governance, Fabrica = context layer). The private
`civitas-io/context` repo is the cross-repo reasoning substrate. `civitas-io/presidium` and
`civitas-io/fabrica` each have their own `HANDOFF.md` — read all three when working across
repo boundaries.

## `AgentProcess.connect_mcp()` was broken since inception -- fixed 2026-08-25, real code not yet released

**Real, currently-blocking bug, reported by a downstream project team against `civitas` 0.11.3 /
`fabrica-context` 0.4.0 / `presidium` 0.6.0** -- verified directly against source before agreeing,
not taken on faith. `connect_mcp()` has always tried `from fabrica.mcp.tool import MCPTool`; that
module never existed in `civitas-io/fabrica` (confirmed via `git log -S "class MCPTool"` finding
nothing real). Every real call raised `ModuleNotFoundError` immediately -- despite
`docs/mcp.md` presenting it as a fully working, documented feature, and
`docs/milestones.md` marking `MCPTool(ToolProvider)` **✅ done** in shipped history. It never was;
that row is only accurate again as of this fix.

**This repo's own real, related fixes** (the actual missing `MCPTool` class lives in
`civitas-io/fabrica`, not here -- see that repo's own `HANDOFF.md`):

- **`civitas.sandbox.config.SandboxConfig.enabled` now defaults `True` (fail-closed), was
  `False`.** Found while scoping: fabrica's own, independently-defined `SandboxConfig` already
  defaulted `enabled=True` -- the two silently disagreed on a security-relevant default the
  whole time, undetected because nothing ever exercised both together until this fix (which
  unifies fabrica's copy into a re-export of this one). `SandboxConfig` also gained
  `allow_unsandboxed: bool = False`, migrated in from fabrica's version, for the same
  unification.
- Fixed `pip install fabrica[mcp]` -> `pip install fabrica-context` in `connect_mcp()`'s own
  `ImportError` message and `civitas.mcp`'s module docstring -- wrong package name AND a
  nonexistent extra (`mcp` is a hard core dependency of `fabrica-context`, not gated behind one).
- `civitas.mcp.types.MCPToolError`'s docstring corrected: confirmed dead -- nothing in civitas
  core raises or catches it; the real MCP call path raises `fabrica.mcp.errors.MCPToolError`
  instead (different class, same name). Not removed, a real separate semver decision.

See `CHANGELOG.md`'s `[Unreleased]` entry for the full detail. **Real, working, verified end to
end** -- a real reproduction test (calling `connect_mcp()` against a real running MCP server,
asserting tools register into `self.tools` and execute for real) lives in `civitas-io/fabrica`'s
own test suite (this repo's own dev environment deliberately doesn't install fabrica). **Not yet
released** -- `civitas.sandbox.config.SandboxConfig.enabled`'s default flip is a real, deliberate
breaking change; needs a version bump (suggest `0.12.0`, matching this org's own precedent of
treating a deliberate, correctly-reasoned breaking fix as a MINOR bump pre-1.0, e.g. presidium's
`CelPolicyEngine` default-deny flip) before `fabrica-context` can bump its own `civitas>=0.11.0`
floor to pick it up.

---

## Status as of 2026-08-24: **`civitas` v0.11.3 is live on PyPI**, real mTLS in direct mode

```
pip install civitas   # 0.11.3
```

Confirmed via a real fresh-venv install (both with and without `civitas[http]`). GitHub Release:
[`v0.11.3`](https://github.com/civitas-io/python-civitas/releases/tag/v0.11.3).

**What shipped this session (R10, closing GH #25's `direct`-mode half):** `mtls_source="direct"`
(the default) HTTP mTLS now actually works — uvicorn never exposed the client certificate from
its own TLS handshake to the ASGI app (a known, previously only half-fixed gap; `proxy_header`
mode worked, `direct` didn't). New `civitas.gateway._tls_protocol.TlsAwareHttpToolsProtocol`
reads the real peer certificate straight off the TLS transport — verified empirically (a minimal,
real asyncio TLS server) before any implementation code was written, then proven end to end
against a real running `HTTPGateway` and against `civitas-io/presidium`'s own real mTLS test
suite via a local editable install. Full design and every real bug found along the way:
[`docs/design/gateway-http-mtls-direct.md`](docs/design/gateway-http-mtls-direct.md).

**v0.11.2 → v0.11.3 same-day patch**: v0.11.2's own release verification (a real fresh-venv
install) immediately caught a second real, live bug — `import civitas` failed entirely with
`civitas[http]` installed, because the new `_tls_protocol.py` imported `uvicorn` eagerly at
module level, defeating this codebase's own established discipline of only ever importing
`uvicorn` lazily inside `HTTPGateway.on_start()`. Fixed and re-released the same day, not left
broken. **Pattern worth remembering**: this is the second time this exact class of bug (a new
module eagerly importing an optional dependency, then itself imported eagerly by an
always-loaded chain) has hit this org this session — the first was Presidium's own `aiosqlite`
incident. Watch for this shape specifically whenever adding a new module gated behind an extra.

**Org profile README fixed** (`civitas-io/.github`): was missing `fabrica` entirely from the
repos table (still described Fabrica as living inside `civitas-contrib`), gave real install
instructions for a private repo (`tessera`), and claimed `pip install promptshrink` worked
despite that project being spec-only. All fixed, plus a real "latest release + date" column
added for every repo that has one.

---

## Real, working pre-commit hooks -- installed, verified, not just configured

**2026-08-24**: `.pre-commit-config.yaml` existed here since June but the hook was never actually
installed (`.git/hooks/pre-commit` didn't exist). Now real: `uv run pre-commit install &&
uv run pre-commit install --hook-type pre-push` wires up ruff/ruff-format/gitleaks on every
commit, `mypy` (pinned `--python 3.12`, matching CI's own mypy job exactly -- a real,
confirmed Python-3.14-specific pyzmq/mypy stub false positive was found and fixed while wiring
this up) and the full test suite on every push. See `.pre-commit-config.yaml`'s own comments for
the full reasoning (`--frozen` avoiding lockfile-mutation-as-a-side-effect, etc.) -- a future
session picking this repo back up should just run the two `pre-commit install` commands above,
not re-derive any of this.

## Real bugs found and fixed this session — the pattern matters more than the specifics

- **`RegistryServer`/`AgentProcess` attribute collision, `PolicyEvaluatorServer` enum-vs-string
  bug, `check_grant()`'s inherited `"tool:"` prefix bug, `payload_extra` dispatch not working for
  user-declared routes** — all found in `civitas-io/presidium`, not this repo, but all found by
  actually running code against this repo's real, current source, never by inspection alone.
- **The mTLS gap itself (GH #25/R10)** — found because a downstream consumer (Presidium) wrote a
  real end-to-end test instead of trusting that config assembly = working behavior.
- **Two packaging bugs in this repo** (the `_tls_protocol.py` eager-uvicorn-import above, and its
  sibling in Presidium) — both found by the release's own fresh-venv verification step, not
  skipped as a formality.

## GH #26 — Streamable HTTP MCP transport: DONE and CLOSED

**Real, implemented, both sides, benchmarked, and closed**
(commit `052b567` here, `f402656`/`16877b7` in `civitas-io/fabrica`):
[github.com/civitas-io/python-civitas/issues/26](https://github.com/civitas-io/python-civitas/issues/26)
(closed). `MCPServerConfig.transport` has a real `"streamable_http"` value; Fabrica's
`MCPClient.connect()` has a real third branch using the `mcp` SDK's own
`streamable_http_client`, verified end to end against a real running server (not
mocked) -- see `civitas-io/fabrica`'s own `HANDOFF.md`/commits for the full detail,
including a real anyio/asyncio cancellation interop finding along the way.

**Real perf benchmarks, run on the real homelab (AMD Ryzen 9 3900X), matched
dependency versions**: `streamable_http` p50 2.01ms / 673 calls-s @ concurrency=10,
vs. `stdio` 0.69ms/2356 and `sse` 1.32ms/991 -- roughly 3x `stdio`'s latency, ~1.5x
`sse`'s, as expected given it layers a full HTTP request/response on an already-open
stream. No memory growth on any transport at 2000 calls. One real, notable finding,
honestly flagged as a still-open question: throughput doesn't meaningfully scale
past ~5 concurrent callers sharing one `ClientSession`, on any transport. Full
methodology, raw JSON, every finding:
[`civitas-io/fabrica`'s `SPIKE-mcp-transport-benchmark.md`](https://github.com/civitas-io/fabrica/blob/main/specs/archive/spikes/SPIKE-mcp-transport-benchmark.md).

**Historical detail below, now superseded by the above** (kept for the original
reasoning trail):

- `civitas/mcp/types.py`'s `MCPServerConfig.transport` is `Literal["stdio", "sse"]` — no
  Streamable HTTP (a single `POST`/`GET`/`DELETE` endpoint, no separate SSE-upgrade endpoint),
  which is what most current remote MCP servers actually ship, often *instead of* classic SSE.
- The actual client construction lives in **`civitas-io/fabrica`**'s `src/fabrica/mcp/client.py`
  (`MCPClient.connect()`) — confirmed directly against that repo's real, current source, not the
  old `civitas-contrib` location the issue was originally filed against (stale reference in the
  issue itself, worth fixing when this is picked up).
- The official `mcp` Python SDK already ships `mcp.client.streamable_http.streamablehttp_client`,
  mirroring `mcp.client.sse.sse_client`'s shape closely — per the issue filer's own assessment,
  "a fairly contained addition": a third `transport` literal value in this repo, and a branch in
  Fabrica's `MCPClient.connect()` constructing `streamablehttp_client(url)` instead of
  `sse_client(url)`.
- **Real, concrete motivating case already hit in practice** (per the issue): a self-hosted MCP
  server exposing HTTP mode as a single `/mcp` endpoint (the Streamable HTTP shape) cannot be
  connected to at all today.

**Also requested for this piece of work**: real performance benchmarks (concurrency, latency,
throughput, memory load) for the Streamable HTTP transport path specifically, to be added to the
relevant README(s) once real numbers exist — not estimated, not synthetic-only. **The homelab
(a real Linux machine) is available if a Linux-specific measurement is needed** — mirrors this
session's own established discipline of validating real capabilities on real hardware rather than
assuming behavior (see e.g. the Firecracker jailer/vsock work in `civitas-io/fabrica`'s own
`specs/archive/spikes/`).

**Suggested real sequence, not yet started:**
1. Add the third `transport: Literal["stdio", "sse", "streamable_http"]` value + validation in
   `civitas/mcp/types.py` (this repo).
2. Implement the corresponding branch in `civitas-io/fabrica`'s `MCPClient.connect()`.
3. Real, empirical benchmarks (not assumed) comparing `sse`/`streamable_http` under realistic
   concurrency — latency, throughput, memory — against a real MCP server (the homelab, if Linux
   matters for the result).
4. Update both repos' READMEs with the real numbers.
5. Close GH #26, fix its own stale `civitas-contrib` path reference while there.

## M-LAST (real performance benchmarking) -- DONE, 2026-08-25

Real benchmarks against a real, standalone `civitas` server, on real hardware (a MacBook +
`darkenergy`, a separate Linux homelab host, direct Tailscale connection), replicating TM Dev
Lab's own published k6/Docker methodology shape exactly for the directly-comparable row. Full
results: [`docs/design/performance-benchmark.md`](docs/design/performance-benchmark.md); real,
reusable harness at `benchmarks/`.

**Headline finding**: civitas's own `HTTPGateway` throughput at a directly-comparable CPU-bound
workload (936.8 req/sec, `n=20` Fibonacci) beats Node.js (559) and Python/FastMCP (292) in TM Dev
Lab's own published table, despite civitas's own run paying a real cross-host network-hop latency
tax their same-host Docker-bridge setup never had to pay -- reported honestly alongside the
equally real fact that civitas's average latency (46.7ms) is higher than all four of their
published implementations, including Python/FastMCP (26.45ms).

Also covered: real mTLS overhead (+2.7% latency, -2.7% throughput -- small, once a connection's
handshake is amortized), the message bus under real `zmq` at increasing concurrency (real
saturation point around 10x concurrent senders), and `DynamicSupervisor` spawn latency under
real external load (11ms p50 at low concurrency, 32ms p50 at 50 concurrent spawns/sec).

**Two real, honest limitations named, not hidden**: a genuine cross-host run for the message-bus
benchmark specifically was blocked by a real firewall constraint on the shared homelab host
(resolved by running both processes co-located instead -- still real, separate OS processes,
meeting M-LAST's own stated minimum bar); a real, unreproduced anomaly (491/500 failed spawn
requests, once, at concurrency=1) is named as a candidate for a future root-cause pass if it
recurs, not swept under the rug.

## Other real, open work

**Zero open tracked issues against this repo specifically, as of 2026-08-25** (GH #26 closed,
M-LAST done). This repo's real backlog is now empty.
