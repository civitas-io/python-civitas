"""AgentProcess — the fundamental unit of computation in Civitas."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from civitas.audit.types import AuditEvent, AuditSink
from civitas.errors import ConfigurationError, ErrorAction, SpawnError
from civitas.messages import Message, _new_span_id, _uuid7
from civitas.observability.tracer import Span
from civitas.plugins.loader import resolve_plugin_class

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from civitas.bus import MessageBus
    from civitas.observability.metrics import MetricsSink
    from civitas.observability.tracer import Tracer
    from civitas.plugins.model import ModelProvider
    from civitas.plugins.state import StateStore
    from civitas.plugins.tools import ToolRegistry
    from civitas.registry import Registry


class ProcessStatus(Enum):
    """Lifecycle states for an AgentProcess."""

    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    CRASHED = "CRASHED"


class Mailbox:
    """Bounded async queue for incoming messages with priority support.

    High-priority system messages (priority > 0) are placed at the front.
    Normal messages follow FIFO order. Backpressure is applied when the
    mailbox is full — the sender awaits until space is available.
    """

    def __init__(self, maxsize: int = 1000) -> None:
        self._queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=maxsize)
        self._priority_queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=100)
        self._notify: asyncio.Event = asyncio.Event()

    async def put(self, message: Message) -> None:
        """Enqueue a message. Priority messages bypass the normal queue."""
        if message.priority > 0:
            await self._priority_queue.put(message)
            self._notify.set()
        else:
            await self._queue.put(message)
            self._notify.set()

    async def get(self) -> Message:
        """Dequeue the next message. Priority messages are served first.

        Messages whose ttl has elapsed are discarded with a warning instead
        of being returned; the search continues for the next message.
        """
        while True:
            message: Message | None = None
            if not self._priority_queue.empty():
                message = self._priority_queue.get_nowait()
            elif not self._queue.empty():
                message = self._queue.get_nowait()

            if message is not None:
                if message.ttl is not None and message.timestamp + message.ttl < time.time():
                    logger.warning(
                        "Mailbox: discarding expired message %s (type=%r, ttl=%.1fs)",
                        message.id,
                        message.type,
                        message.ttl,
                    )
                    continue
                return message

            # Wait for a notification
            self._notify.clear()
            # Double-check after clearing (avoid race)
            if not self._priority_queue.empty() or not self._queue.empty():
                continue
            await self._notify.wait()

    def empty(self) -> bool:
        """Return True if both priority and normal queues are empty."""
        return self._priority_queue.empty() and self._queue.empty()


class AgentProcess:
    """Base class for all agent processes in Civitas.

    Developers subclass this and override lifecycle hooks:
    - on_start(): called once before the first message
    - handle(message): called for every incoming message
    - on_error(error, message): called when handle() raises
    - on_stop(): called on graceful shutdown (always — even on crash)

    Messaging methods available inside hooks:
    - send(recipient, payload, message_type): fire-and-forget
    - ask(recipient, payload, message_type, timeout): request-reply
    - send_capable(capability, payload, message_type): route to any capable agent
    - broadcast(pattern, payload): send to all matching agents
    - reply(payload): return from handle() for request-reply

    Observability helpers (call from inside handle()):
    - llm_span(model, **attrs): context manager for LLM call spans
    - tool_span(tool_name, **attrs): context manager for tool call spans

    Capability declaration (class-level, inherited and overridable):
        capabilities: list[str] = ["text.summarize", "text.translate"]
        capability_metadata: dict[str, Any] = {
            "text.summarize": {"description": "...", "version": "1"}
        }
    """

    # Capability tags this agent advertises. Subclasses override at the class level.
    # YAML topology can also set these, taking precedence over the class attribute.
    capabilities: list[str] = []

    # Free-form metadata per capability tag. Passed through to registry listeners
    # (e.g. Presidium) verbatim — the runtime never interprets this dict.
    capability_metadata: dict[str, Any] = {}

    def __init__(
        self,
        name: str,
        mailbox_size: int = 1000,
        max_retries: int = 3,
        shutdown_timeout: float = 30.0,
    ) -> None:
        self.name = name
        self.id: str = _uuid7()
        self.state: dict[str, Any] = {}
        self._status = ProcessStatus.INITIALIZING
        self._mailbox = Mailbox(maxsize=mailbox_size)
        self._task: asyncio.Task[None] | None = None
        self._max_retries = max_retries
        self._shutdown_timeout = shutdown_timeout

        # Injected by Runtime/Worker during setup
        self._bus: MessageBus | None = None
        self._tracer: Tracer | None = None
        self._registry: Registry | None = None
        self.llm: ModelProvider | None = None
        self.tools: ToolRegistry | None = None
        self.store: StateStore | None = None

        # Per-agent credential map — populated from topology credentials: block.
        # Keys are provider names (e.g. "anthropic"); values are credential strings.
        self._credentials: dict[str, str] = {}

        # Audit sink — injected by ComponentSet, None when auditing is disabled.
        self._audit_sink: AuditSink | None = None

        # Metrics sink — injected by ComponentSet, None when no collector is attached.
        self._metrics: MetricsSink | None = None

        # Set by Runtime to the nearest DynamicSupervisor ancestor name (if any)
        self._dynamic_supervisor_name: str | None = None

        # MCP clients opened via connect_mcp() — keyed by server name
        self._mcp_clients: dict[str, Any] = {}

        # Current message context for reply/tracing
        self._current_message: Message | None = None
        self._current_handle_span: Span | None = None

        # Signalled when the message loop enters RUNNING
        self._running_event: asyncio.Event | None = None

    @property
    def status(self) -> ProcessStatus:
        return self._status

    # ------------------------------------------------------------------
    # Credential helpers (M4.2c)
    # ------------------------------------------------------------------

    def get_credential(self, provider_name: str) -> str | None:
        """Return the per-agent credential value for ``provider_name``, or None.

        Credentials are set via the ``credentials:`` block in the topology YAML::

            agents:
              - name: research_agent
                credentials:
                  anthropic: ${RESEARCH_AGENT_ANTHROPIC_KEY}
        """
        value = self._credentials.get(provider_name)
        _sink = getattr(self, "_audit_sink", None)
        if value is not None and _sink is not None:
            event = AuditEvent(
                event="secret.access",
                ts=datetime.now(UTC).isoformat(),
                agent=self.name,
                signer_id="",
                details={"provider": provider_name},
            )
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_sink.emit(event))
            except RuntimeError:
                pass  # no running loop — skip audit in sync context
        return value

    def model_for(self, provider_name: str) -> ModelProvider:
        """Return a ModelProvider scoped to this agent's credentials.

        If a per-agent credential is configured for ``provider_name``, a new
        provider instance is created with that credential. Otherwise falls back
        to the globally injected ``self.llm``.

        Raises:
            ConfigurationError: if no credential and no global provider.
        """
        api_key = self._credentials.get(provider_name)
        if api_key is not None:
            cls = resolve_plugin_class("model", provider_name)
            provider: ModelProvider = cls(api_key=api_key)
            return provider
        if self.llm is not None:
            return self.llm
        raise ConfigurationError(
            f"No model provider available for '{provider_name}'. "
            f"Set agent credentials.{provider_name} in topology or inject a global provider."
        )

    async def receive(self, message: Message) -> None:
        """Deliver an inbound message to this agent's mailbox."""
        await self._mailbox.put(message)

    # ------------------------------------------------------------------
    # Lifecycle hooks — override in subclasses
    # ------------------------------------------------------------------

    async def on_start(self) -> None:
        """Called once before the first message. Initialize self.state here."""

    async def handle(self, message: Message) -> Message | None:
        """Called for every incoming message.

        Return self.reply(...) for request-reply. Return None for fire-and-forget.
        """
        return None

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        """Called when handle() raises an exception.

        Return an ErrorAction. Default: ESCALATE (crash, let supervisor decide).
        """
        return ErrorAction.ESCALATE

    async def on_stop(self) -> None:
        """Called on graceful shutdown. Always called — even on crash."""

    async def on_correction(self, message: Message) -> None:
        """Called when this agent receives a civitas.eval.correction signal.

        Override to react to nudge/redirect corrections from an EvalAgent.
        The signal severity and reason are in message.payload["severity"] and
        message.payload["reason"]. halt corrections never reach this hook —
        they stop the message loop directly.
        """

    async def on_child_terminated(self, name: str, reason: str) -> None:
        """Called when a dynamically spawned child is permanently removed.

        reason is one of: "restarts_exhausted", "despawned", "clean_exit".
        Default implementation logs a warning. Override to re-spawn, alert, etc.
        """
        logger.warning("[%s] dynamic child '%s' terminated: %s", self.name, name, reason)

    # ------------------------------------------------------------------
    # State persistence — checkpoint and restore
    # ------------------------------------------------------------------

    async def checkpoint(self) -> None:
        """Save self.state to the configured StateStore.

        Call this from handle() after completing a meaningful unit of work.
        On restart, self.state is automatically restored from the last checkpoint.
        Agents that never call checkpoint() incur zero overhead.
        """
        if self.store is not None:
            await self.store.set(self.name, self.state)

    async def _restore_state(self) -> None:
        """Restore self.state from the StateStore if a checkpoint exists."""
        if self.store is not None:
            saved = await self.store.get(self.name)
            if saved is not None:
                self.state = saved

    # ------------------------------------------------------------------
    # Messaging methods — call from inside hooks
    # ------------------------------------------------------------------

    async def send(
        self,
        recipient: str,
        payload: dict[str, Any],
        message_type: str = "message",
    ) -> None:
        """Fire-and-forget: send a message to another agent by name."""
        if self._bus is None:
            raise RuntimeError("AgentProcess not wired to a MessageBus")
        trace_id = ""
        parent_span_id: str | None = None
        if self._current_message is not None:
            trace_id = self._current_message.trace_id
            parent_span_id = self._current_message.span_id

        message = Message(
            type=message_type,
            sender=self.name,
            recipient=recipient,
            payload=payload,
            trace_id=trace_id,
            span_id=_new_span_id(),
            parent_span_id=parent_span_id,
        )
        await self._bus.route(message)
        if self._metrics is not None:
            self._metrics.message_sent(self.name)

    async def ask(
        self,
        recipient: str,
        payload: dict[str, Any],
        message_type: str = "message",
        timeout: float = 30.0,
    ) -> Message:
        """Request-reply: send a message and await a response."""
        if self._bus is None:
            raise RuntimeError("AgentProcess not wired to a MessageBus")
        trace_id = ""
        parent_span_id: str | None = None
        if self._current_message is not None:
            trace_id = self._current_message.trace_id
            parent_span_id = self._current_message.span_id

        correlation_id = _uuid7()
        message = Message(
            type=message_type,
            sender=self.name,
            recipient=recipient,
            payload=payload,
            correlation_id=correlation_id,
            trace_id=trace_id,
            span_id=_new_span_id(),
            parent_span_id=parent_span_id,
        )
        if self._metrics is not None:
            self._metrics.message_sent(self.name)
        return await self._bus.request(message, timeout=timeout)

    async def send_capable(
        self,
        capability: str,
        payload: dict[str, Any],
        message_type: str = "message",
    ) -> None:
        """Fire-and-forget to any agent advertising the given capability tag.

        Picks a random entry from all registered agents (local and remote)
        that declare ``capability``. Raises ``CapabilityNotFoundError`` if no
        matching agent is registered.
        """
        from civitas.errors import CapabilityNotFoundError

        if self._registry is None:
            raise RuntimeError("AgentProcess not wired to a Registry")
        candidates = self._registry.find_by_capability(capability)
        if not candidates:
            raise CapabilityNotFoundError(capability)
        import random

        entry = random.choice(candidates)
        await self.send(entry.address, payload, message_type)

    async def call(
        self,
        name: str,
        payload: dict[str, Any],
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        """Synchronous call to a GenServer. Blocks until reply or timeout."""
        reply = await self.ask(name, payload, timeout=timeout)
        return reply.payload

    async def cast(self, name: str, payload: dict[str, Any]) -> None:
        """Fire-and-forget cast to a GenServer. Returns immediately."""
        await self.send(name, {**payload, "__cast__": True})

    async def emit_eval(
        self,
        event_type: str,
        payload: dict[str, Any],
        eval_agent: str = "eval_agent",
    ) -> None:
        """Emit an observable event to the named EvalAgent.

        No-op if the message bus is not wired (safe to call in tests).
        The eval_agent parameter is the registered name of the EvalAgent
        process in the supervision tree (default: "eval_agent").
        """
        if self._bus is None:
            return
        await self.send(
            eval_agent,
            {"agent_name": self.name, "event_type": event_type, **payload},
            message_type="civitas.eval.event",
        )

    async def spawn(
        self,
        agent_class: type,
        name: str,
        config: dict[str, Any] | None = None,
    ) -> str:
        """Spawn a dynamic agent via the nearest ancestor DynamicSupervisor.

        Sends a civitas.dynamic.spawn message and awaits confirmation.
        Raises SpawnError if no DynamicSupervisor ancestor exists or spawn is denied.
        Returns the agent name on success.
        """
        if self._dynamic_supervisor_name is None:
            raise SpawnError("No DynamicSupervisor ancestor found in supervision tree")
        class_path = f"{agent_class.__module__}.{agent_class.__qualname__}"
        reply = await self.ask(
            self._dynamic_supervisor_name,
            {"class_path": class_path, "name": name, "config": config or {}, "spawner": self.name},
            message_type="civitas.dynamic.spawn",
        )
        if reply.payload.get("status") != "ok":
            raise SpawnError(reply.payload.get("reason", "spawn failed"))
        return name

    async def despawn(self, name: str) -> None:
        """Hard-stop a dynamic child immediately.

        Cancels the agent's task. on_stop() still fires. Pending ask() callers
        into the agent receive SpawnError. The slot is freed immediately.
        """
        if self._dynamic_supervisor_name is None:
            raise SpawnError("No DynamicSupervisor ancestor found in supervision tree")
        reply = await self.ask(
            self._dynamic_supervisor_name,
            {"name": name},
            message_type="civitas.dynamic.despawn",
        )
        if reply.payload.get("status") != "ok":
            raise SpawnError(reply.payload.get("reason", "despawn failed"))

    async def stop(
        self,
        name: str,
        drain: str = "current",
        timeout: float = 30.0,
    ) -> None:
        """Soft-stop a dynamic child. Awaitable — returns when fully stopped.

        drain="current" — finishes the message currently being handled, then stops.
        drain="all"     — drains the full mailbox, then stops.
        timeout         — fallback hard stop if drain isn't complete in time.
        """
        if self._dynamic_supervisor_name is None:
            raise SpawnError("No DynamicSupervisor ancestor found in supervision tree")
        reply = await self.ask(
            self._dynamic_supervisor_name,
            {"name": name, "drain": drain, "timeout": timeout},
            message_type="civitas.dynamic.stop",
            timeout=timeout + 5.0,
        )
        if reply.payload.get("status") != "ok":
            raise SpawnError(reply.payload.get("reason", "stop failed"))

    async def connect_mcp(self, config: Any) -> None:
        """Connect to an MCP server and register its tools into self.tools.

        Idempotent: disconnects any existing client for config.name before reconnecting.
        Tools are addressable as mcp://server_name/tool_name after this call.
        """
        existing = self._mcp_clients.get(config.name)
        if existing is not None:
            if self.tools is not None:
                self.tools.deregister_prefix(f"mcp://{config.name}/")
            try:
                await existing.disconnect()
            except Exception:
                pass

        try:
            from fabrica.mcp.client import MCPClient
            from fabrica.mcp.tool import MCPTool
        except ImportError as exc:
            raise ConfigurationError(
                "MCP support requires fabrica. Install it with: pip install fabrica[mcp]"
            ) from exc

        client = MCPClient(config, audit_sink=self._audit_sink, agent_name=self.name)
        await client.connect()
        schemas = await client.list_tools()

        if self.tools is not None:
            for schema in schemas:
                mcp_tool = MCPTool(
                    client,
                    schema,
                    tracer=self._tracer,
                    audit_sink=self._audit_sink,
                    agent_name=self.name,
                )
                self.tools.register(mcp_tool)

        self._mcp_clients[config.name] = client

    async def broadcast(self, pattern: str, payload: dict[str, Any]) -> None:
        """Send a message to all agents matching a glob pattern."""
        if self._bus is None:
            raise RuntimeError("AgentProcess not wired to a MessageBus")
        targets = self._bus.lookup_all(pattern)
        for target in targets:
            await self.send(target.name, payload)

    def reply(self, payload: dict[str, Any]) -> Message:
        """Create a reply message. Return this from handle() for request-reply."""
        if self._current_message is None:
            raise RuntimeError("reply() called outside of handle()")
        msg = self._current_message
        return Message(
            type=payload.get("type", "reply"),
            sender=self.name,
            recipient=msg.reply_to or msg.sender,
            payload=payload,
            correlation_id=msg.correlation_id,
            trace_id=msg.trace_id,
            span_id=_new_span_id(),
            parent_span_id=msg.span_id,
        )

    # ------------------------------------------------------------------
    # Observability helpers — call from inside handle()
    # ------------------------------------------------------------------

    @contextmanager
    def llm_span(self, model: str, **attributes: Any) -> Iterator[Span]:
        """Context manager that creates an LLM call span parented to handle().

        Usage:
            with self.llm_span("claude-sonnet", tokens_in=1200) as span:
                response = await self.llm.chat(...)
                span.set_attribute("civitas.llm.tokens_out", ...)
        """
        if self._tracer is None:
            yield Span(name="llm", trace_id="", span_id="")
            return

        parent_span_id = (
            self._current_handle_span.span_id
            if self._current_handle_span is not None
            else (self._current_message.span_id if self._current_message else None)
        )
        trace_id = self._current_message.trace_id if self._current_message else ""
        span = self._tracer.start_span(
            "civitas.llm.chat",
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            attributes={"civitas.llm.model": model, **attributes},
        )
        try:
            yield span
        except Exception as exc:
            span.set_error(exc)
            raise
        finally:
            span.end()

    @contextmanager
    def tool_span(self, tool_name: str, **attributes: Any) -> Iterator[Span]:
        """Context manager that creates a tool invocation span parented to handle().

        Usage:
            with self.tool_span("web_search") as span:
                result = await self.tools.invoke("web_search", ...)
                span.set_attribute("civitas.tool.result_size_bytes", len(result))
        """
        if self._tracer is None:
            yield Span(name="tool", trace_id="", span_id="")
            return

        parent_span_id = (
            self._current_handle_span.span_id
            if self._current_handle_span is not None
            else (self._current_message.span_id if self._current_message else None)
        )
        trace_id = self._current_message.trace_id if self._current_message else ""
        span = self._tracer.start_span(
            "civitas.tool.invoke",
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            attributes={"civitas.tool.name": tool_name, **attributes},
        )
        try:
            yield span
        except Exception as exc:
            span.set_error(exc)
            raise
        finally:
            span.end()

    # ------------------------------------------------------------------
    # Internal lifecycle — called by Supervisor / Runtime
    # ------------------------------------------------------------------

    async def _start(self) -> None:
        """Initialize the agent and start the message loop as a task."""
        self._status = ProcessStatus.INITIALIZING
        self._running_event = asyncio.Event()
        await self._restore_state()

        # Emit agent.start span
        start_span = None
        if self._tracer is not None:
            start_span = self._tracer.start_span(
                "civitas.agent.start",
                attributes={"civitas.agent.name": self.name, "civitas.agent.id": self.id},
            )

        try:
            await self.on_start()
        except Exception as exc:
            # on_start() failed — the message loop (and its finally block)
            # never runs, so run the equivalent cleanup here. Mirrors OTP's
            # terminate/2-after-failed-init/1 semantics.
            if start_span is not None:
                start_span.set_error(exc)
                start_span.end()
            self._status = ProcessStatus.CRASHED
            await self.on_stop()
            for _client in list(self._mcp_clients.values()):
                try:
                    await _client.disconnect()
                except Exception:
                    pass
            self._mcp_clients.clear()
            raise

        if start_span is not None:
            start_span.end()

        self._task = asyncio.create_task(self._message_loop(), name=self.name)
        # Wait until the message loop has entered RUNNING
        await self._running_event.wait()

    async def _message_loop(self) -> None:
        """Main loop: dequeue messages and dispatch to handle()."""
        self._status = ProcessStatus.RUNNING
        if self._running_event is not None:
            self._running_event.set()

        try:
            while self._status == ProcessStatus.RUNNING:
                message = await self._mailbox.get()
                if message.type in ("_agency.shutdown", "civitas.eval.halt"):
                    break
                if message.type == "_agency.heartbeat":
                    # Auto-respond to heartbeat pings from supervisor
                    if self._bus is not None:
                        ack = Message(
                            type="_agency.heartbeat_ack",
                            sender=self.name,
                            recipient=message.reply_to or message.sender,
                            correlation_id=message.correlation_id,
                            trace_id=message.trace_id,
                        )
                        await self._bus.route(ack)
                    continue
                if message.type == "civitas.dynamic.terminated":
                    await self.on_child_terminated(
                        message.payload.get("child_name", ""),
                        message.payload.get("reason", ""),
                    )
                    continue
                self._current_message = message
                await self._dispatch(message)
                self._current_message = None
        finally:
            self._current_message = None
            self._current_handle_span = None
            # Preserve CRASHED — only move to STOPPING for normal/requested exits
            crashed = self._status == ProcessStatus.CRASHED
            if not crashed:
                self._status = ProcessStatus.STOPPING

            # Emit agent.stop span — always, including on crash
            if self._tracer is not None:
                stop_span = self._tracer.start_span(
                    "civitas.agent.stop",
                    attributes={
                        "civitas.agent.name": self.name,
                        "civitas.agent.id": self.id,
                        "civitas.agent.final_status": self._status.value,
                    },
                )

            await self.on_stop()

            for _client in list(self._mcp_clients.values()):
                try:
                    await _client.disconnect()
                except Exception:
                    pass
            self._mcp_clients.clear()

            if self._tracer is not None:
                stop_span.end()

            if not crashed:
                self._status = ProcessStatus.STOPPED

    async def _dispatch(self, message: Message) -> None:
        """Wrap a single handle() call in a span, apply error action."""
        dispatch_start = time.time()
        # Start handle span
        handle_span: Span | None = None
        if self._tracer is not None:
            handle_span = self._tracer.start_span(
                "civitas.agent.handle",
                trace_id=message.trace_id,
                parent_span_id=message.span_id,
                attributes={
                    "civitas.agent.name": self.name,
                    "civitas.message.type": message.type,
                    "civitas.handle.attempt": message.attempt,
                },
            )
        self._current_handle_span = handle_span

        try:
            if message.type == "civitas.eval.correction":
                await self.on_correction(message)
                if handle_span is not None:
                    handle_span.set_attribute("civitas.handle.result", "success")
                return
            result = await self.handle(message)
            if handle_span is not None:
                handle_span.set_attribute("civitas.handle.result", "success")
            if result is not None and message.correlation_id and self._bus is not None:
                await self._bus.route(result)
        except Exception as exc:
            if handle_span is not None:
                handle_span.set_error(exc)
                handle_span.set_attribute("civitas.handle.result", "error")
            if self._metrics is not None:
                self._metrics.agent_error(self.name)
            action = await self.on_error(exc, message)
            if handle_span is not None:
                handle_span.set_attribute("civitas.handle.result", f"error.{action.value.lower()}")
            await self._apply_error_action(action, exc, message)
        finally:
            if self._metrics is not None:
                self._metrics.message_handled(self.name, (time.time() - dispatch_start) * 1000)
            if handle_span is not None:
                handle_span.end()
            self._current_handle_span = None

    async def _apply_error_action(
        self, action: ErrorAction, exc: Exception, message: Message
    ) -> None:
        """Apply the error action returned by on_error()."""
        if action == ErrorAction.RETRY:
            message.attempt += 1
            if message.attempt > self._max_retries:
                # Max retries exceeded — escalate instead of looping forever
                self._status = ProcessStatus.CRASHED
                raise exc
            # Emit retry span
            if self._tracer is not None:
                retry_span = self._tracer.start_span(
                    "civitas.agent.retry",
                    trace_id=message.trace_id,
                    attributes={
                        "civitas.agent.name": self.name,
                        "civitas.message.type": message.type,
                        "civitas.handle.attempt": message.attempt,
                        "civitas.max_retries": self._max_retries,
                        "error.type": type(exc).__name__,
                    },
                )
                retry_span.end()
            await self._mailbox.put(message)
        elif action == ErrorAction.SKIP:
            pass  # discard message, continue
        elif action == ErrorAction.STOP:
            self._status = ProcessStatus.STOPPING
        elif action == ErrorAction.ESCALATE:
            self._status = ProcessStatus.CRASHED
            raise exc  # propagate to supervisor via task exception

    async def _stop(self) -> None:
        """Request graceful shutdown by sending a shutdown system message."""
        if self._status not in (ProcessStatus.RUNNING, ProcessStatus.INITIALIZING):
            return
        shutdown_msg = Message(
            type="_agency.shutdown",
            sender="_agency",
            recipient=self.name,
            priority=1,
        )
        await self._mailbox.put(shutdown_msg)
        if self._task is not None and not self._task.done():
            try:
                async with asyncio.timeout(self._shutdown_timeout):
                    await self._task
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass
