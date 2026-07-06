"""StateStore protocol and InMemoryStateStore implementation."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class StateStore(Protocol):
    """Protocol for agent state persistence."""

    async def get(self, agent_name: str) -> dict[str, Any] | None:
        """Retrieve the persisted state for an agent, or None if absent."""
        ...

    async def set(self, agent_name: str, state: dict[str, Any]) -> None:
        """Persist state for an agent."""
        ...

    async def delete(self, agent_name: str) -> None:
        """Remove persisted state for an agent."""
        ...

    async def list_agents(self) -> list[str]:
        """Return all agent names with persisted state."""
        ...

    async def close(self) -> None:
        """Release resources held by the store (connections, file handles)."""
        ...


class InMemoryStateStore:
    """Default state store — in-memory, no persistence. State is lost on restart."""

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}

    async def get(self, agent_name: str) -> dict[str, Any] | None:
        """Retrieve the in-memory state for an agent."""
        return self._data.get(agent_name)

    async def set(self, agent_name: str, state: dict[str, Any]) -> None:
        """Store state for an agent in memory (shallow copy to prevent aliasing).

        Without copying, subsequent mutations to the caller's dict would also
        mutate the stored checkpoint, making restore-from-checkpoint a no-op.
        """
        self._data[agent_name] = dict(state)

    async def delete(self, agent_name: str) -> None:
        """Remove state for an agent from memory."""
        self._data.pop(agent_name, None)

    async def list_agents(self) -> list[str]:
        """Return all agent names with persisted state, sorted."""
        return sorted(self._data.keys())

    async def close(self) -> None:
        """No-op — in-memory store holds no external resources."""
