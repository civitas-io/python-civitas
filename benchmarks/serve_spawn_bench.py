#!/usr/bin/env python3
"""DynamicSupervisor spawn-latency benchmark server -- a real HTTPGateway
route (POST /v1/spawn) backed by Runtime.spawn(), driven by the same real,
external load generator (ab/k6) as Benchmark 1 -- not in-process asyncio
tasks measuring the same process's own supervision tree, the exact mistake
M-LAST requirement 1 names, even though spawn is itself an in-process
supervision-tree operation with no wire protocol of its own; routing it
through a real HTTP round trip is what makes the LOAD GENERATION real and
external, matching this benchmark suite's own established rigor.

Each request spawns one, uniquely-named, minimal child agent and despawns
it immediately after -- so a long-running load test doesn't accumulate an
unbounded number of live children (a fair, steady-state measurement, not
one that degrades from tree size growth as a confound).

Usage:
    python benchmarks/serve_spawn_bench.py --port 8095
"""

from __future__ import annotations

import argparse
import asyncio
import itertools

from civitas import AgentProcess
from civitas.messages import Message


class ChildAgent(AgentProcess):
    """Defined at MODULE level, not nested inside main() -- real, found-
    while-testing requirement: Runtime.spawn() resolves the class via
    `importlib` from its own `__module__`/`__qualname__` (civitas/runtime.py:
    `class_path = f"{agent_class.__module__}.{agent_class.__qualname__}"`).
    A class nested inside a function produces an unresolvable qualname like
    `__main__.main.<locals>.ChildAgent` -- importlib can't re-import that.
    Run as a script, this module IS `__main__`, and `ChildAgent` at true
    module level resolves correctly as `__main__.ChildAgent`.
    """

    async def handle(self, message: Message) -> Message | None:
        return None


_counter = itertools.count()


async def main(args: argparse.Namespace) -> None:
    from civitas import Runtime, Supervisor
    from civitas.gateway import GatewayConfig, HTTPGateway
    from civitas.supervisor import DynamicSupervisor

    counter = _counter

    class SpawnRequestAgent(AgentProcess):  # type: ignore[misc]
        """Backs POST /v1/spawn -- spawns one uniquely-named child via
        Runtime.spawn(), despawns it immediately, replies with success.
        Uses the injected `runtime` reference (set after construction,
        since Runtime itself doesn't exist yet when agents are built)."""

        runtime: Runtime | None = None

        async def handle(self, message: Message) -> Message | None:
            if message.type != "spawn_request" or self.runtime is None:
                return self.reply({"error": "not ready"})
            name = f"spawn-bench-{next(counter)}"
            try:
                await self.runtime.spawn("dynamic", ChildAgent, name=name, wait=True)
                await self.runtime.despawn("dynamic", name)
                return self.reply({"status": "ok", "name": name})
            except Exception as exc:  # noqa: BLE001
                return self.reply({"status": "error", "reason": str(exc)})

    routes = [{"method": "POST", "path": "/v1/spawn", "agent": "spawner", "mode": "call"}]
    config = GatewayConfig(host=args.host, port=args.port, routes=routes)
    gateway = HTTPGateway("api", config=config)
    dynamic_sup = DynamicSupervisor("dynamic")
    spawn_agent = SpawnRequestAgent("spawner")

    supervisor = Supervisor("root", children=[gateway, dynamic_sup, spawn_agent])
    runtime = Runtime(supervisor=supervisor)
    spawn_agent.runtime = runtime

    print(f"civitas spawn-latency benchmark server: http://{args.host}:{args.port}")
    print("route: POST /v1/spawn {} -- spawns + despawns one child per request")

    await runtime.start()
    try:
        await asyncio.Event().wait()
    finally:
        await runtime.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8095)
    parsed_args = parser.parse_args()
    asyncio.run(main(parsed_args))
