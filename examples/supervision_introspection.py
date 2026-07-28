"""Supervision introspection (v0.9.0 D6, design/supervision-endgame.md) — v0.9.2.

Every ``Supervisor`` (static or dynamic) is an addressable actor with its own
mailbox — query one live with the reserved ``civitas.supervision.status`` message
type for a snapshot: children, their status, restart-window occupancy, and lifetime
restart counts. Observability only (no side effects); this is exactly the same query
`civitas top`'s ``/topology`` endpoint uses internally.

Usage:
    python examples/supervision_introspection.py
"""

from __future__ import annotations

import asyncio
import json

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message


class FlakyWorker(AgentProcess):
    """Crashes once on the first message, then behaves — enough to put a real
    restart count and a nonzero crashes_in_window on the introspection snapshot."""

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "crash":
            raise RuntimeError("simulated crash")
        return self.reply({"ok": True})


class SteadyWorker(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"ok": True})


async def main() -> None:
    root = Supervisor(
        "root",
        children=[FlakyWorker("flaky"), SteadyWorker("steady")],
        max_restarts=5,
        backoff_base=0.01,  # fast restart for this short-lived demo
    )
    runtime = Runtime(supervisor=root)
    await runtime.start()

    print("Snapshot before any crash:")
    status = await runtime.ask("root", {}, message_type="civitas.supervision.status")
    print(json.dumps(status.payload, indent=2))

    print("\nSending a message that makes 'flaky' crash and restart...")
    try:
        await runtime.send("flaky", {"cmd": "crash"})
    except Exception:
        pass
    await asyncio.sleep(0.2)  # let the crash + restart complete

    print("\nSnapshot after the crash (flaky's restart_count, root's crashes_in_window):")
    status = await runtime.ask("root", {}, message_type="civitas.supervision.status")
    print(json.dumps(status.payload, indent=2))

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
