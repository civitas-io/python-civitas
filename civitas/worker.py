"""Worker — lightweight process host for remote agents.

A Worker connects to an existing transport broker (ZMQ proxy or NATS server)
and hosts one or more agents. It provides the same wiring as Runtime (bus,
registry, tracer, serializer) but without the supervision tree — supervision
is handled remotely by the Runtime process.

Usage from a subprocess:

    import asyncio
    from civitas.worker import Worker
    from myagents import MyAgent

    # ZMQ worker
    worker = Worker(
        agents=[MyAgent("my_agent")],
        transport="zmq",
        zmq_pub_addr="tcp://127.0.0.1:5559",
        zmq_sub_addr="tcp://127.0.0.1:5560",
    )

    # NATS worker
    worker = Worker(
        agents=[MyAgent("my_agent")],
        transport="nats",
        nats_servers="nats://localhost:4222",
    )
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from civitas.components import ComponentSet, build_component_set
from civitas.errors import ConfigurationError
from civitas.messages import Message
from civitas.observability.otel_agent import run_otel_agent
from civitas.process import AgentProcess, ProcessStatus
from civitas.serializer import Serializer

logger = logging.getLogger(__name__)


class Worker:
    """Hosts agents in a worker process, connecting to an existing broker.

    Supports ZMQ (connect to proxy) and NATS (connect to server) transports.

    The Worker provides:
    - Transport connectivity (ZMQ or NATS)
    - Local registry and message bus
    - Heartbeat auto-response (handled by AgentProcess._message_loop)
    - Restart command handling (_agency.restart messages)
    """

    def __init__(
        self,
        agents: list[AgentProcess],
        transport: str = "zmq",
        zmq_pub_addr: str = "tcp://127.0.0.1:5559",
        zmq_sub_addr: str = "tcp://127.0.0.1:5560",
        nats_servers: str | list[str] = "nats://localhost:4222",
        nats_jetstream: bool = False,
        serializer: Serializer | None = None,
        model_provider: Any = None,
        tool_registry: Any = None,
        state_store: Any = None,
        exporters: list[Any] | None = None,
        max_restarts: int = 3,
        components: ComponentSet | None = None,
    ) -> None:
        self._transport_type = transport
        self._zmq_pub_addr = zmq_pub_addr
        self._zmq_sub_addr = zmq_sub_addr
        self._nats_servers = nats_servers
        self._nats_jetstream = nats_jetstream
        self._custom_serializer = serializer
        self._model_provider = model_provider
        self._tool_registry = tool_registry
        self._state_store = state_store
        self._exporters = exporters or []
        self._max_restarts = max_restarts
        self._components = components
        self._otel_agent_task: asyncio.Task[None] | None = None

        # O(1) agent lookup by name (F02-8)
        self._agents: dict[str, AgentProcess] = {a.name: a for a in agents}
        self._restart_counts: dict[str, int] = {a.name: 0 for a in agents}

        # Set during start()
        self._serializer: Serializer | None = None
        self._transport: Any = None
        self._registry: Any = None
        self._bus: Any = None
        self._started = False
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the worker: connect to proxy, wire agents, start loops."""
        # Workers never use in-process transport — they connect to an existing broker
        if self._components is None and self._transport_type not in ("zmq", "nats"):
            raise ConfigurationError(
                f"Unknown transport: {self._transport_type!r}. Expected 'zmq' or 'nats'."
            )

        # Build or use provided ComponentSet
        if self._components is not None:
            cs = self._components
        else:
            cs = build_component_set(
                transport_type=self._transport_type,
                serializer=self._custom_serializer,
                model_provider=self._model_provider,
                tool_registry=self._tool_registry,
                state_store=self._state_store,
                exporters=self._exporters,
                zmq_pub_addr=self._zmq_pub_addr,
                zmq_sub_addr=self._zmq_sub_addr,
                zmq_start_proxy=False,  # Workers connect to an existing proxy
                nats_servers=self._nats_servers,
                nats_jetstream=self._nats_jetstream,
            )

        # Expose on self for _on_restart_command and stop()
        self._serializer = cs.serializer
        self._transport = cs.transport
        self._registry = cs.registry
        self._bus = cs.bus

        # Drain span_queue via OTELAgent when exporters are configured (FD-07/FD-09)
        if cs.span_queue is not None and cs.export_backend is not None:
            self._otel_agent_task = asyncio.create_task(
                run_otel_agent(cs.span_queue, cs.export_backend)
            )

        # Start transport first — must be running before setup_agent (F02-16)
        await self._transport.start()

        # Wait for ZMQ subscriptions to propagate (slow joiner).
        # Also lets the worker's PUB socket establish its connection to the proxy
        # before we publish registration announcements below.
        if hasattr(self._transport, "wait_ready"):
            await self._transport.wait_ready()

        # Wire and subscribe agents
        for agent in self._agents.values():
            cs.inject(agent)
            self._registry.register(agent.name)
            await self._bus.setup_agent(agent)

        # Subscribe to restart commands and register in registry so bus.route() can find it (F03-2)
        await self._transport.subscribe("_agency.worker.restart", self._on_restart_command)
        self._registry.register("_agency.worker.restart")

        # Start agent message loops
        for agent in self._agents.values():
            await agent._start()

        # Announce agents (and the restart handler) to the runtime's registry for
        # cross-process routing. Published AFTER wait_ready so the PUB socket
        # connection is stable. The brief sleep gives the runtime's receiver loop
        # time to process the announcements before worker.start() returns.
        announce_names = list(self._agents) + ["_agency.worker.restart"]
        for name in announce_names:
            maybe_agent: AgentProcess | None = self._agents.get(name)
            payload: dict[str, Any] = {"name": name}
            if maybe_agent is not None:
                payload["capabilities"] = list(maybe_agent.capabilities)
                payload["capability_metadata"] = dict(maybe_agent.capability_metadata)
            await self._transport.publish(
                "_agency.register",
                self._serializer.serialize(Message(type="_agency.register", payload=payload)),
            )
        await asyncio.sleep(0.1)

        self._started = True

    async def _on_restart_command(self, data: bytes) -> None:
        """Handle restart commands from the supervisor."""
        if self._serializer is None:
            raise RuntimeError("Worker not started")
        msg = self._serializer.deserialize(data)
        target_name = msg.payload.get("agent_name", "")

        agent = self._agents.get(target_name)
        if agent is None:
            logger.warning("Worker: restart command for unknown agent %r", target_name)
            return

        restart_count = self._restart_counts.get(target_name, 0)
        if restart_count >= self._max_restarts:
            logger.error(
                "Worker: agent %r exceeded max_restarts (%d), not restarting",
                target_name,
                self._max_restarts,
            )
            return

        try:
            await agent._stop()
            agent._status = ProcessStatus.INITIALIZING
            if self._registry is not None:
                self._registry.deregister(agent.name)
                self._registry.register(agent.name)
            if self._bus is not None:
                await self._bus.setup_agent(agent)
            await agent._start()
            self._restart_counts[target_name] = restart_count + 1
            logger.info(
                "Worker: restarted agent %r (attempt %d/%d)",
                target_name,
                self._restart_counts[target_name],
                self._max_restarts,
            )
        except Exception:
            logger.exception("Worker: failed to restart agent %r", target_name)

    async def stop(self) -> None:
        """Stop all agents and disconnect from the proxy."""
        if not self._started:
            return

        for agent in reversed(list(self._agents.values())):
            await agent._stop()

        # Deregister agents from the runtime's registry before disconnecting
        if self._serializer is not None and self._transport is not None:
            for name in self._agents:
                try:
                    await self._transport.publish(
                        "_agency.deregister",
                        self._serializer.serialize(
                            Message(type="_agency.deregister", payload={"name": name})
                        ),
                    )
                except Exception:  # noqa: BLE001 — best-effort during shutdown
                    pass

        if self._transport is not None:
            await self._transport.stop()

        if self._otel_agent_task is not None:
            self._otel_agent_task.cancel()
            await asyncio.gather(self._otel_agent_task, return_exceptions=True)
            self._otel_agent_task = None

        self._started = False
        self._stop_event.set()

    async def wait_until_stopped(self) -> None:
        """Block until stop() is called. Useful for long-running workers."""
        await self._stop_event.wait()

    @property
    def started(self) -> bool:
        return self._started
