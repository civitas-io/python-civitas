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


class ApprovalWorker(AgentProcess):
    """v0.9.4: periodically decides mid-``handle()`` that it needs a human
    approval before proceeding — the realistic HITL pattern (self-suspension
    from within message handling, not suspended from an external task) —
    lights up the tree/detail-panel's distinct HITL-approval color (blue),
    separate from an ordinary governance-pause SUSPENDED (grey), via
    ``suspend_for_approval()``. Auto-"approves" itself after ~20s purely for
    the demo's own sake; a real caller would be a human/Presidium via
    ``resume()``.

    Found while building this demo (not assumed): calling suspend_for_approval()
    from a plain ``asyncio.create_task()`` background loop (the pattern every
    OTHER agent in this file uses) never actually transitions the agent —
    ``suspend()`` intentionally only takes effect at the message loop's next
    boundary (S2, docs/design/durable-suspension.md), which only re-checks
    when a message arrives. An agent idling with nothing in its mailbox never
    wakes up to notice. The self-sent-message pattern below is the correct
    shape for a periodic/timer-driven self-suspend; ``resume()`` itself is
    NOT boundary-deferred (it transitions synchronously), so it's safe to call
    directly from the background task.
    """

    async def on_start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def on_stop(self) -> None:
        self._task.cancel()

    async def handle(self, message: Message) -> None:
        if message.type == "_demo.request_approval":
            await self.suspend_for_approval("needs $500 spend approval")
        return None

    async def _loop(self) -> None:
        while True:
            await self._mailbox.put(Message(type="_demo.request_approval"))
            await asyncio.sleep(20.0)
            await self.resume("demo-auto-approver")
            await asyncio.sleep(10.0)
