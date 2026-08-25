#!/usr/bin/env python3
"""Message-bus benchmark -- the agent-under-test side, using civitas.worker.
Worker, not Runtime -- a real, load-bearing finding from building this:
ONLY Worker publishes the `_agency.register` cross-process discovery
broadcast (civitas/runtime.py's own comment: "Workers publish
_agency.register on startup"); a plain Runtime-hosted agent never
announces itself to remote peers at all. Mirrors examples/deployment/
level2_multi_process/run_worker.py's own exact, proven pattern.

A Worker does not start its own ZMQ proxy -- it CONNECTS OUT to one
already running elsewhere (bus_client.py's Runtime starts it). Run
bus_client.py FIRST (it starts the proxy), then this script.

Usage:
    python benchmarks/bus_server.py --transport zmq --coordinator-ip 100.79.90.66
"""

from __future__ import annotations

import argparse
import asyncio

from civitas import AgentProcess
from civitas.messages import Message
from civitas.worker import Worker


class TargetAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        if message.type == "ping":
            return self.reply({"pong": True, "echo": message.payload.get("seq")})
        return None


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=["zmq", "nats"], default="zmq")
    parser.add_argument("--coordinator-ip", required=True)
    parser.add_argument("--pub-port", type=int, default=15559)
    parser.add_argument("--sub-port", type=int, default=15560)
    parser.add_argument("--nats-url", default=None)
    args = parser.parse_args()

    worker_kwargs: dict[str, object] = {"transport": args.transport}
    if args.transport == "zmq":
        worker_kwargs.update(
            zmq_pub_addr=f"tcp://{args.coordinator_ip}:{args.pub_port}",
            zmq_sub_addr=f"tcp://{args.coordinator_ip}:{args.sub_port}",
        )
    else:
        worker_kwargs.update(nats_servers=args.nats_url or f"nats://{args.coordinator_ip}:4222")

    worker = Worker(agents=[TargetAgent("target")], **worker_kwargs)  # type: ignore[arg-type]

    print(f"message-bus benchmark worker: transport={args.transport}, connecting to coordinator")
    print("target agent 'target' ready for 'ping' messages")

    await worker.start()
    try:
        await asyncio.Event().wait()
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
