"""Unit tests for Worker — lifecycle guards, restart command handler, prebuilt components."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from civitas.errors import ConfigurationError
from civitas.messages import Message
from civitas.process import AgentProcess
from civitas.serializer import MsgpackSerializer
from civitas.worker import Worker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class NullAgent(AgentProcess):
    async def handle(self, message: Message) -> None:
        return None


class _FakeTransport:
    """Minimal transport spec — no wait_ready by default."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def subscribe(self, topic: str, handler: object) -> None: ...
    async def publish(self, topic: str, data: bytes) -> None: ...


def _mock_cs(*, with_wait_ready: bool = False) -> MagicMock:
    """Return a minimal mock ComponentSet that satisfies Worker.start().

    By default the transport does NOT expose wait_ready so that hasattr()
    returns False (matching the common case).  Pass with_wait_ready=True
    to include a mocked wait_ready for the dedicated test.
    """
    serializer = MsgpackSerializer()
    cs = MagicMock()
    cs.serializer = serializer
    # Use spec so MagicMock only auto-creates attributes that exist on _FakeTransport
    cs.transport = MagicMock(spec=_FakeTransport)
    cs.transport.start = AsyncMock()
    cs.transport.subscribe = AsyncMock()
    cs.transport.publish = AsyncMock()
    cs.transport.stop = AsyncMock()
    if with_wait_ready:
        cs.transport.wait_ready = AsyncMock()
    cs.registry = MagicMock()
    cs.registry.register = MagicMock()
    cs.bus = MagicMock()
    cs.bus.setup_agent = AsyncMock()
    cs.inject = MagicMock()
    return cs


# ---------------------------------------------------------------------------
# start() — guard paths
# ---------------------------------------------------------------------------


class TestWorkerStart:
    async def test_invalid_transport_raises(self) -> None:
        """Worker.start() raises ConfigurationError for unknown transport types."""
        worker = Worker(agents=[], transport="http")
        with pytest.raises(ConfigurationError, match="Unknown transport"):
            await worker.start()

    async def test_prebuilt_components_skips_build(self) -> None:
        """When components= is provided, build_component_set is never called."""
        cs = _mock_cs()

        agent = NullAgent("a")
        agent._start = AsyncMock()  # type: ignore[method-assign]

        worker = Worker(agents=[agent], transport="http", components=cs)

        with patch("civitas.worker.build_component_set") as mock_build:
            await worker.start()

        mock_build.assert_not_called()
        assert worker._started is True

    async def test_wait_ready_called_when_transport_has_it(self) -> None:
        """Worker.start() calls transport.wait_ready() when the method exists."""
        cs = _mock_cs(with_wait_ready=True)

        agent = NullAgent("a")
        agent._start = AsyncMock()  # type: ignore[method-assign]

        worker = Worker(agents=[agent], transport="http", components=cs)
        await worker.start()

        cs.transport.wait_ready.assert_awaited_once()

    async def test_wait_ready_not_called_when_absent(self) -> None:
        """Worker.start() does not call wait_ready() if the transport lacks it."""
        cs = _mock_cs()  # wait_ready not present by default (spec restricts it)

        agent = NullAgent("a")
        agent._start = AsyncMock()  # type: ignore[method-assign]

        worker = Worker(agents=[agent], transport="http", components=cs)
        # Should not raise
        await worker.start()
        assert worker._started is True

    async def test_stop_before_start_is_noop(self) -> None:
        """Worker.stop() is safe to call when the worker was never started."""
        worker = Worker(agents=[], transport="http")
        await worker.stop()  # must not raise


# ---------------------------------------------------------------------------
# _on_restart_command — handler paths
# ---------------------------------------------------------------------------


class TestOnRestartCommand:
    def _make_started_worker(self) -> tuple[Worker, NullAgent]:
        """Return a Worker with internal state manually initialised (no real start)."""
        agent = NullAgent("bot")
        worker = Worker(agents=[agent], max_restarts=2)
        serializer = MsgpackSerializer()
        worker._serializer = serializer
        worker._registry = MagicMock()
        worker._registry.register = MagicMock()
        worker._registry.deregister = MagicMock()
        worker._bus = MagicMock()
        worker._bus.setup_agent = AsyncMock()
        return worker, agent

    async def test_unknown_agent_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Restart command for an unknown agent logs a warning and returns."""
        worker, _ = self._make_started_worker()
        serializer = worker._serializer
        assert serializer is not None
        msg = Message(type="_agency.restart", payload={"agent_name": "ghost"})
        data = serializer.serialize(msg)

        with caplog.at_level(logging.WARNING, logger="civitas.worker"):
            await worker._on_restart_command(data)

        assert any("unknown agent" in r.message for r in caplog.records)

    async def test_exceeded_max_restarts_logs_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Restart command is rejected when the agent has hit max_restarts."""
        worker, agent = self._make_started_worker()
        # Saturate restart counter
        worker._restart_counts["bot"] = 2  # equals max_restarts

        serializer = worker._serializer
        assert serializer is not None
        msg = Message(type="_agency.restart", payload={"agent_name": "bot"})
        data = serializer.serialize(msg)

        with caplog.at_level(logging.ERROR, logger="civitas.worker"):
            await worker._on_restart_command(data)

        assert any("exceeded max_restarts" in r.message for r in caplog.records)

    async def test_successful_restart_increments_counter(self) -> None:
        """Successful restart increments restart_counts and starts a FRESH
        incarnation (D1a, v0.9.0) — the old object is stopped and replaced."""
        worker, agent = self._make_started_worker()
        agent._stop = AsyncMock()  # type: ignore[method-assign]

        replacement = NullAgent("bot")
        replacement._start = AsyncMock()  # type: ignore[method-assign]

        serializer = worker._serializer
        assert serializer is not None
        msg = Message(type="_agency.restart", payload={"agent_name": "bot"})
        data = serializer.serialize(msg)

        with patch("civitas.worker._fresh_incarnation", return_value=replacement):
            await worker._on_restart_command(data)

        assert worker._restart_counts["bot"] == 1
        agent._stop.assert_awaited_once()
        replacement._start.assert_awaited_once()
        assert worker._agents["bot"] is replacement  # object swapped

    async def test_restart_failure_logs_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        """If the restart raises, the exception is logged and does not propagate."""
        worker, agent = self._make_started_worker()
        agent._stop = AsyncMock(side_effect=RuntimeError("crash"))  # type: ignore[method-assign]

        serializer = worker._serializer
        assert serializer is not None
        msg = Message(type="_agency.restart", payload={"agent_name": "bot"})
        data = serializer.serialize(msg)

        with caplog.at_level(logging.ERROR, logger="civitas.worker"):
            await worker._on_restart_command(data)

        assert any("failed to restart" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# wait_until_stopped
# ---------------------------------------------------------------------------


async def test_wait_until_stopped_unblocks_after_stop() -> None:
    """wait_until_stopped() returns once stop() is called."""
    cs = _mock_cs()
    agent = NullAgent("a")
    agent._start = AsyncMock()  # type: ignore[method-assign]
    agent._stop = AsyncMock()  # type: ignore[method-assign]

    worker = Worker(agents=[agent], transport="http", components=cs)
    await worker.start()

    stop_task = asyncio.create_task(worker.stop())
    await worker.wait_until_stopped()
    await stop_task
    assert worker._started is False
