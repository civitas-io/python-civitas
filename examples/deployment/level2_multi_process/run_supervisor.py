"""Level 2 — Supervisor process.

Starts the ZMQ proxy and runs the frontend agent.
Run this before run_worker.py.

    uv run python examples/deployment/level2_multi_process/run_supervisor.py
"""

import asyncio
import sys
from pathlib import Path

# topology.yaml's `type: examples.deployment.level2_multi_process.agents.
# FrontendAgent` resolves via plain importlib (civitas/runtime.py's
# _resolve_class), which needs the repo root on sys.path -- true for an
# editable dev install (implicit), NOT for a normal `pip install civitas`
# (found by the examples smoke test's Docker/Linux run, v0.9.2: this failed
# there with "No module named 'examples'" even though it "worked" locally).
# Making this script self-contained rather than relying on install mode.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from civitas import Runtime  # noqa: E402

TOPOLOGY = Path(__file__).parent / "topology.yaml"


async def main() -> None:
    # process_filter=None (v0.9.2.1 bugfix): build only THIS process's own
    # (untagged) agents. Before the fix, from_config() had no awareness of
    # topology.yaml's `process: worker` tags at all and built worker_a/
    # worker_b locally too -- duplicating what run_worker.py, in its own
    # process, also builds for itself. This example is exactly what
    # surfaced that bug (docs/milestones.md v0.9.2.1).
    runtime = Runtime.from_config(TOPOLOGY, process_filter=None)
    await runtime.start()
    print("Supervisor running. Start the worker process, then send a request.")
    print("Press Ctrl+C to stop.\n")

    # In a real app, a web server or queue consumer would drive this.
    # Here we just wait for keyboard interrupt.
    try:
        await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
