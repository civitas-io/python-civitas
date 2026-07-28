"""Level 2 — Worker process.

Connects to the ZMQ proxy started by run_supervisor.py and hosts
worker_a and worker_b.

    uv run python examples/deployment/level2_multi_process/run_worker.py
"""

import asyncio
import sys
from pathlib import Path

# Python adds a script's OWN directory to sys.path[0] automatically, so a
# plain `from agents import WorkerAgent` (agents.py is a sibling file) works
# in any install mode -- deliberately NOT the examples.deployment... dotted
# path run_supervisor.py needs for topology.yaml resolution, which requires
# an explicit sys.path fix there (see that file's comment); this script has
# no such requirement and shouldn't take on that fragility for no reason.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from agents import WorkerAgent  # noqa: E402

from civitas.worker import Worker

# Worker has no from_config() classmethod (only Runtime does) -- built
# directly here, matching topology.yaml's zmq addresses and the two
# worker-process agents declared there.


async def main() -> None:
    worker = Worker(
        agents=[WorkerAgent("worker_a"), WorkerAgent("worker_b")],
        transport="zmq",
        zmq_pub_addr="tcp://127.0.0.1:5559",
        zmq_sub_addr="tcp://127.0.0.1:5560",
    )
    await worker.start()
    print("Worker process running (worker_a, worker_b). Press Ctrl+C to stop.")

    try:
        await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
