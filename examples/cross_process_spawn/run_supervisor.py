"""Cross-process dynamic spawn (design/cross-process-spawn.md, R6) — Supervisor.

Spawns a child into run_worker.py's DynamicSupervisor across the wire — no new
protocol, the existing civitas.dynamic.spawn request/reply just routes over ZMQ
like any other message.

Start THIS script first (it owns the ZMQ proxy, zmq_start_proxy=True below),
then run_worker.py in a second terminal — see that file's docstring for why
the order matters.

Deliberately does NOT declare a local `workers` DynamicSupervisor of its own (a
YAML topology with a `process: worker`-tagged node would be the more familiar
declarative shape, but `Runtime.from_config()`/`civitas run --topology` build
EVERY node locally regardless of `process:` — found writing this example,
tracked in docs/milestones.md, not solved here). This script instead mirrors
the pattern proven correct by tests/integration/test_cross_process_spawn.py:
an empty local supervisor tree, and `runtime.spawn("workers", ...)` targeting
the REMOTE DynamicSupervisor purely by the name it announces itself under.

Usage (start this one first, then run_worker.py in a second terminal):
    python examples/cross_process_spawn/run_supervisor.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# See this file's own module docstring, and run_worker.py's comment: agents.py
# must be importable identically in BOTH processes by the SAME dotted path,
# since the worker resolves agent_class.__module__ + __qualname__ itself.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from agents import EchoWorker  # noqa: E402

from civitas import Runtime, Supervisor  # noqa: E402
from civitas.errors import SpawnError  # noqa: E402

PUB_ADDR = "tcp://127.0.0.1:15561"
SUB_ADDR = "tcp://127.0.0.1:15562"


async def _wait_for_worker_announcement(runtime: Runtime, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if runtime._registry is not None and runtime._registry.lookup("workers") is not None:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(
        "'workers' was never announced — is run_worker.py running? "
        "Start it first: python examples/cross_process_spawn/run_worker.py"
    )


async def main() -> None:
    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="zmq",
        zmq_pub_addr=PUB_ADDR,
        zmq_sub_addr=SUB_ADDR,
        zmq_start_proxy=True,
    )
    await runtime.start()

    try:
        print("Waiting for run_worker.py's 'workers' DynamicSupervisor to announce itself...")
        await _wait_for_worker_announcement(runtime)
        print("Found it. Spawning 'child-1' into it, cross-process...")

        await runtime.spawn("workers", EchoWorker, "child-1")
        print("Spawned. Asking it a question (routed to the worker process)...")

        reply = await runtime.ask("child-1", {"hello": "from the supervisor process"}, timeout=5.0)
        print(f"Reply: {reply.payload}")

        await runtime.despawn("workers", "child-1")
        print("Despawned 'child-1'.")
    except (TimeoutError, SpawnError) as exc:
        print(f"Error: {exc}")
        raise
    finally:
        await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
