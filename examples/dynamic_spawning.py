"""Dynamic agent spawning example.

Demonstrates DynamicSupervisor: an OrchestratorAgent spawns ResearchAgent
workers on demand, monitors them, and despawns them when work is done.
Run with:
    uv run python examples/dynamic_spawning.py
"""

from __future__ import annotations

import asyncio
import logging

from civitas import (
    AgentProcess,
    Message,
    Runtime,
)

logging.basicConfig(level=logging.INFO, format="%(name)s  %(message)s")
logger = logging.getLogger("demo")

TOPOLOGY = {
    "supervision": {
        "name": "root",
        "strategy": "ONE_FOR_ONE",
        "children": [
            {
                "type": "topology_server",
                "name": "topo_server",
                "config": {"host": "127.0.0.1", "port": 6789},
            },
            {
                "type": "dynamic_supervisor",
                "name": "workers",
                "config": {"max_children": 10},
            },
            {
                "type": "agent",
                "name": "orchestrator",
                "module": "__main__",
                "class": "OrchestratorAgent",
            },
        ],
    }
}


class ResearchAgent(AgentProcess):
    """Minimal worker — accepts a topic, logs it, then terminates."""

    async def on_start(self) -> None:
        logger.info("[%s] started", self.name)

    async def handle(self, message: Message) -> None:
        if message.type == "research.do":
            topic = message.payload.get("topic", "?")
            logger.info("[%s] researching: %s", self.name, topic)
            await asyncio.sleep(0.5)
            logger.info("[%s] done", self.name)

    async def on_stop(self) -> None:
        logger.info("[%s] stopped", self.name)


class OrchestratorAgent(AgentProcess):
    """Spawns ResearchAgent workers, waits for them to finish, then shuts down."""

    async def on_child_terminated(self, name: str, reason: str) -> None:
        logger.info("[%s] child '%s' terminated (%s)", self.name, name, reason)

    async def on_start(self) -> None:
        topics = ["climate policy", "renewable energy", "carbon capture"]

        worker_names: list[str] = []
        for i, _topic in enumerate(topics):
            worker_name = f"researcher-{i}"
            await self.spawn(
                worker_name,
                ResearchAgent,
                init_kwargs={"name": worker_name},
            )
            worker_names.append(worker_name)
            logger.info("[orchestrator] spawned %s", worker_name)

        await asyncio.sleep(0.2)

        # Send work to each worker
        for worker_name, topic in zip(worker_names, topics, strict=True):
            await self.send(worker_name, Message(type="research.do", payload={"topic": topic}))

        # Give workers time to finish
        await asyncio.sleep(1.5)

        # Despawn finished workers
        for worker_name in worker_names:
            await self.despawn(worker_name)
            logger.info("[orchestrator] despawned %s", worker_name)

        logger.info("[orchestrator] all done — shutting down")


async def main() -> None:
    runtime = Runtime(TOPOLOGY)
    try:
        await runtime.start()
        # Let the orchestrator finish its work
        await asyncio.sleep(4.0)
    finally:
        await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
