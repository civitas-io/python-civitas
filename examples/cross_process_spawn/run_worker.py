"""Cross-process dynamic spawn (design/cross-process-spawn.md, R6) — Worker.

Hosts a DynamicSupervisor that run_supervisor.py spawns children into across
the wire.

Start run_supervisor.py FIRST (it owns the ZMQ proxy, zmq_start_proxy=True) in
one terminal, then this script SECOND, in another (found running this exact
pair: reversing the order means this Worker tries to connect before any proxy
exists to connect to, and the supervisor's 10s wait for this process's
announcement times out):

    python examples/cross_process_spawn/run_supervisor.py   # first
    python examples/cross_process_spawn/run_worker.py       # second
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Python adds a script's own directory to sys.path[0] automatically, so a
# plain `from agents import ...` (agents.py is a sibling file) works in any
# install mode -- see run_supervisor.py's own comment for why this matters
# for a cross-process spawn target specifically (agent_class.__module__ must
# resolve identically in BOTH processes).
sys.path.insert(0, str(Path(__file__).resolve().parent))

from civitas import DynamicSupervisor, Worker  # noqa: E402

PUB_ADDR = "tcp://127.0.0.1:15561"
SUB_ADDR = "tcp://127.0.0.1:15562"


async def main() -> None:
    worker = Worker(
        agents=[DynamicSupervisor("workers")],
        transport="zmq",
        zmq_pub_addr=PUB_ADDR,
        zmq_sub_addr=SUB_ADDR,
    )
    await worker.start()
    print("Worker running, hosting the 'workers' DynamicSupervisor. Press Ctrl+C to stop.")

    try:
        await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
