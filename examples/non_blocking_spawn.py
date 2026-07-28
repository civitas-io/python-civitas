"""Non-blocking dynamic spawn (B4, design/non-blocking-spawn.md) — v0.9.2.

By default, ``spawn()``/``spawn_into()`` wait for the child to reach RUNNING before
returning (``wait=True``), raising ``SpawnError`` if its ``on_start()`` fails. This
example shows the non-blocking alternative: ``wait=False`` (or ``spawn_nowait()``)
returns as soon as the child's task exists, without waiting for ``on_start()`` to
finish — useful when a child's startup is slow (a DB connection, a model warm-up)
and the spawner has other work to get on with immediately. A later start failure is
delivered asynchronously via ``on_child_terminated()``, not as an exception from the
spawn call itself.

Usage:
    python examples/non_blocking_spawn.py
"""

from __future__ import annotations

import asyncio

from civitas import AgentProcess, DynamicSupervisor, Runtime, Supervisor
from civitas.messages import Message


class SlowStartWorker(AgentProcess):
    """Simulates a slow, successful startup (e.g. warming up a model)."""

    async def on_start(self) -> None:
        await asyncio.sleep(0.3)

    async def handle(self, message: Message) -> Message | None:
        return None


class DoomedWorker(AgentProcess):
    """Simulates a slow startup that then fails — the async-failure path."""

    async def on_start(self) -> None:
        await asyncio.sleep(0.2)
        raise RuntimeError("simulated startup failure")

    async def handle(self, message: Message) -> Message | None:
        return None


class Orchestrator(AgentProcess):
    """Spawns children without blocking on their (slow) startup."""

    async def on_child_terminated(self, name: str, reason: str) -> None:
        # This is where a wait=False spawn's startup FAILURE surfaces — there is
        # no exception to catch at the spawn() call site itself.
        print(f"[orchestrator] '{name}' terminated asynchronously: {reason}")

    async def handle(self, message: Message) -> Message | None:
        return None


async def main() -> None:
    orchestrator = Orchestrator("orchestrator")
    dyn = DynamicSupervisor("workers")
    runtime = Runtime(supervisor=Supervisor("root", children=[dyn, orchestrator]))
    await runtime.start()

    print("Spawning a slow-but-successful worker with wait=False...")
    t0 = asyncio.get_event_loop().time()
    name = await orchestrator.spawn(SlowStartWorker, "slow-worker", wait=False)
    elapsed = asyncio.get_event_loop().time() - t0
    print(f"  spawn() returned '{name}' in {elapsed * 1000:.1f}ms (did NOT wait for on_start)")

    print("\nSpawning a worker whose startup will fail, also with wait=False...")
    await orchestrator.spawn(DoomedWorker, "doomed-worker", wait=False)
    print("  spawn() returned immediately, before the failure below is even known:")

    # Give both children's on_start() time to actually finish, so the async
    # failure above has time to fire and print before we tear down.
    await asyncio.sleep(0.6)

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
