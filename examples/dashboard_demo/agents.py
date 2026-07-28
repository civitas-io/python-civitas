"""Agents for the ``civitas top`` dashboard demo (v0.9.1, dashboard-v2 Phase E).

Deliberately noisy/colorful, not realistic: each agent exists to light up a
different part of the dashboard so you can SEE the feature working, not to
model a real workload. Run alongside ``topology.yaml`` — see
``examples/dashboard_demo/README.md`` for the two-terminal walkthrough.
"""

from __future__ import annotations

import asyncio
import random

from civitas.messages import Message
from civitas.process import AgentProcess


class ChattyWorker(AgentProcess):
    """Reports a fake LLM call every ~2s — lights up the detail panel's
    tokens/cost/last_model fields and the process resource panel's CPU%.
    """

    async def on_start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def on_stop(self) -> None:
        self._task.cancel()

    async def handle(self, message: Message) -> None:
        return None

    async def _loop(self) -> None:
        models = ["claude-sonnet-4-6", "gpt-4o", "claude-haiku"]
        while True:
            await asyncio.sleep(2.0)
            with self.llm_span(random.choice(models)) as span:
                tokens_in = random.randint(200, 2000)
                tokens_out = random.randint(50, 800)
                span.set_attribute("civitas.llm.tokens_in", tokens_in)
                span.set_attribute("civitas.llm.tokens_out", tokens_out)
                span.set_attribute("civitas.llm.cost_usd", tokens_out * 0.000015)


class FlakyWorker(AgentProcess):
    """Crashes roughly every ~8s — lights up the tree's red/crashed dot,
    restart_count, and the supervisor's crashes_in_window.
    """

    async def on_start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def on_stop(self) -> None:
        self._task.cancel()

    async def handle(self, message: Message) -> None:
        return None

    async def _loop(self) -> None:
        await asyncio.sleep(random.uniform(4.0, 8.0))
        raise RuntimeError("simulated failure for the dashboard demo")


class SpawnerAgent(AgentProcess):
    """Spawns and despawns a rotating cast of dynamic children — lights up
    the tree's "(dynamic, N live)" count and dynamic-supervisor branch.
    """

    async def on_start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def on_stop(self) -> None:
        self._task.cancel()

    async def handle(self, message: Message) -> None:
        return None

    async def _loop(self) -> None:
        counter = 0
        live: list[str] = []
        while True:
            await asyncio.sleep(3.0)
            if len(live) >= 2:
                victim = live.pop(0)
                await self.despawn(victim)
                continue
            counter += 1
            name = f"job-{counter}"
            await self.spawn(ChattyWorker, name)
            live.append(name)
