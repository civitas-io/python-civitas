"""Agent spawned cross-process by run_supervisor.py into run_worker.py's
DynamicSupervisor. A real, separately-importable sibling module (not
`__main__`) — needed because the worker process must resolve this SAME class
by dotted path from `agent_class.__module__`/`__qualname__`, and "__main__"
means something different in each process."""

from __future__ import annotations

from civitas import AgentProcess
from civitas.messages import Message


class EchoWorker(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload})
