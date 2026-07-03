"""Supervisor — monitors child processes and applies restart strategies on failure."""

from __future__ import annotations

import asyncio
import importlib
import logging
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING, Any

from civitas.messages import Message, _new_span_id, _uuid7
from civitas.process import AgentProcess, ProcessStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from civitas.bus import MessageBus
    from civitas.observability.tracer import Tracer
    from civitas.registry import Registry


class HeartbeatTimeout(Exception):
    """Raised when a remote agent fails to respond to heartbeat pings."""

    def __init__(self, agent_name: str, missed: int) -> None:
        self.agent_name = agent_name
        self.missed = missed
        super().__init__(f"Agent '{agent_name}' missed {missed} heartbeats")


class RestartStrategy(Enum):
    """Strategy used by a Supervisor when a child process crashes."""

    ONE_FOR_ONE = "ONE_FOR_ONE"
    ONE_FOR_ALL = "ONE_FOR_ALL"
    REST_FOR_ONE = "REST_FOR_ONE"


class BackoffPolicy(Enum):
    """Delay strategy applied between successive restart attempts."""

    CONSTANT = "CONSTANT"
    LINEAR = "LINEAR"
    EXPONENTIAL = "EXPONENTIAL"


class Supervisor:
    """Manages child processes with restart strategies.

    When a child crashes, the supervisor applies the configured restart
    strategy. If max_restarts is exceeded within restart_window, the
    supervisor escalates to its parent or stops permanently.
    """

    def __init__(
        self,
        name: str,
        children: list[AgentProcess | Supervisor] | None = None,
        strategy: str = "ONE_FOR_ONE",
        max_restarts: int = 3,
        restart_window: float = 60.0,
        backoff: str = "CONSTANT",
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
    ) -> None:
        self.name = name
        self.children: list[AgentProcess | Supervisor] = children or []
        self.strategy = RestartStrategy(strategy)
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self.backoff = BackoffPolicy(backoff)
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max

        # Internal state
        self._restart_timestamps: deque[float] = deque()  # F03-10: deque for O(1) sliding window
        self._restart_counts: dict[str, int] = {}
        self._child_tasks: dict[str, asyncio.Task[None]] = {}
        self._children_by_name: dict[str, AgentProcess | Supervisor] = {  # F03-11: O(1) lookup
            c.name: c for c in self.children
        }
        self._pending_crash_tasks: set[asyncio.Task[None]] = set()  # F03-4: track handlers
        self._running = False
        self._parent: Supervisor | None = None
        self._crash_callbacks: list[Callable[[str, Exception], Awaitable[None]]] = []

        # Injected by Runtime
        self._bus: MessageBus | None = None
        self._registry: Registry | None = None
        self._tracer: Tracer | None = None

        # Heartbeat monitoring for remote agents
        self._remote_children: set[str] = set()
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._missed_heartbeats: dict[str, int] = {}
        self._remote_child_config: dict[str, dict[str, float | int]] = {}  # F03-3: per-child config

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start all children and begin monitoring them."""
        self._running = True

        # Set parent references for child supervisors
        for child in self.children:
            if isinstance(child, Supervisor):
                child._parent = self

        # Start children bottom-up (supervisors first start their children)
        for child in self.children:
            if isinstance(child, Supervisor):
                await child.start()
            else:
                await self._start_child(child)

        # Start heartbeat monitoring for remote children
        await self._start_heartbeat_monitor()

    async def stop(self) -> None:
        """Stop all children gracefully."""
        self._running = False

        # Cancel pending crash handlers before tearing down children (F03-4)
        for t in list(self._pending_crash_tasks):
            t.cancel()
        if self._pending_crash_tasks:
            await asyncio.gather(*self._pending_crash_tasks, return_exceptions=True)

        await self._stop_heartbeat_monitor()
        for child in reversed(self.children):
            if isinstance(child, Supervisor):
                await child.stop()
            else:
                await child._stop()

    def add_crash_callback(self, callback: Callable[[str, Exception], Awaitable[None]]) -> None:
        """Register a callback invoked with (child_name, exception) on every crash.

        Runs before the restart strategy is applied. A callback that raises
        is logged and does not prevent the restart strategy from running.
        """
        self._crash_callbacks.append(callback)

    async def _start_child(self, agent: AgentProcess) -> None:
        """Start a single child agent and monitor its task."""
        await agent._start()
        if agent._task is not None:
            self._child_tasks[agent.name] = agent._task

            def _make_callback(n: str) -> Callable[[asyncio.Task[None]], None]:
                def _cb(t: asyncio.Task[None]) -> None:
                    self._on_child_done(n, t)

                return _cb

            agent._task.add_done_callback(_make_callback(agent.name))

    def _on_child_done(self, name: str, task: asyncio.Task[None]) -> None:
        """Callback when a child task completes (crash or normal exit)."""
        if not self._running:
            return
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            t = asyncio.create_task(
                self._handle_crash(
                    name, exc if isinstance(exc, Exception) else RuntimeError(str(exc))
                )
            )  # F03-4
            self._pending_crash_tasks.add(t)
            t.add_done_callback(self._pending_crash_tasks.discard)

    # ------------------------------------------------------------------
    # Remote child / heartbeat support
    # ------------------------------------------------------------------

    def add_remote_child(
        self,
        name: str,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 2.0,
        missed_heartbeats_threshold: int = 3,
    ) -> None:
        """Register a remote child for heartbeat-based monitoring.

        Remote children are agents running in a Worker process. They are
        monitored via periodic heartbeat pings instead of task callbacks.
        """
        self._remote_children.add(name)
        self._missed_heartbeats[name] = 0
        # F03-3: per-child config stored in dict, not shared scalars
        self._remote_child_config[name] = {
            "interval": heartbeat_interval,
            "timeout": heartbeat_timeout,
            "threshold": missed_heartbeats_threshold,
        }

    async def _start_heartbeat_monitor(self) -> None:
        """Start the heartbeat monitoring loop for remote children."""
        if not self._remote_children:
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self) -> None:
        """Periodically ping remote children and detect crashes."""
        while self._running:
            # Compute sleep interval before the loop — minimum across all children
            sleep_interval = min(
                (float(cfg.get("interval", 5.0)) for cfg in self._remote_child_config.values()),
                default=5.0,
            )

            for name in list(self._remote_children):
                if not self._running:
                    break
                cfg = self._remote_child_config.get(name, {})
                timeout = float(cfg.get("timeout", 2.0))
                threshold = int(cfg.get("threshold", 3))

                try:
                    heartbeat = Message(
                        type="_agency.heartbeat",
                        sender=self.name,
                        recipient=name,
                        correlation_id=_uuid7(),
                        span_id=_new_span_id(),
                    )
                    if self._bus is None:
                        break
                    # F03-14: rely on bus.request timeout, no redundant wait_for wrapper
                    await self._bus.request(heartbeat, timeout=timeout)
                    # Got ack — reset missed counter
                    self._missed_heartbeats[name] = 0
                except TimeoutError:
                    self._missed_heartbeats[name] = self._missed_heartbeats.get(name, 0) + 1
                    missed = self._missed_heartbeats[name]
                    if missed >= threshold:
                        await self._handle_crash(name, HeartbeatTimeout(name, missed))
                        self._missed_heartbeats[name] = 0
                except asyncio.CancelledError:
                    raise  # propagate to stop the task cleanly (F03-7)
                except Exception as exc:
                    logger.warning(  # F03-7: don't crash loop on unexpected errors
                        "[%s] heartbeat error for %s: %s", self.name, name, exc
                    )

            await asyncio.sleep(sleep_interval)

    async def _stop_heartbeat_monitor(self) -> None:
        """Stop the heartbeat monitor task."""
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    # ------------------------------------------------------------------
    # Crash handling
    # ------------------------------------------------------------------

    async def _handle_crash(self, name: str, exc: Exception) -> None:
        """Apply the restart strategy after a child crash."""
        now = time.time()

        # Update crash log for the child
        self._restart_counts.setdefault(name, 0)
        self._restart_counts[name] += 1

        # F03-10: deque-based sliding window — O(1) append and popleft
        cutoff = now - self.restart_window
        self._restart_timestamps.append(now)
        while self._restart_timestamps and self._restart_timestamps[0] <= cutoff:
            self._restart_timestamps.popleft()

        for callback in self._crash_callbacks:
            try:
                await callback(name, exc)
            except Exception:
                logger.exception("Supervisor %r: crash callback raised", self.name)

        # Check if we've exceeded max restarts
        if len(self._restart_timestamps) > self.max_restarts:
            await self._escalate(name, exc)
            return

        # Log the restart
        restart_num = self._restart_counts[name]
        if self._tracer:
            span = self._tracer.start_span(
                "supervisor.restart",
                attributes={
                    "civitas.supervisor": self.name,
                    "civitas.child": name,
                    "civitas.restart_count": restart_num,
                    "civitas.strategy": self.strategy.value,
                    "civitas.error": str(exc),
                },
            )
            span.end()
        else:
            logger.info(
                "[%s] Restart %d/%d: %s crashed (%s)",
                self.name,
                restart_num,
                self.max_restarts,
                name,
                exc,
            )

        # Apply backoff delay
        delay = self._compute_backoff(restart_num)
        if delay > 0:
            await asyncio.sleep(delay)

        # Apply restart strategy
        if self.strategy == RestartStrategy.ONE_FOR_ONE:
            await self._restart_child(name)
        elif self.strategy == RestartStrategy.ONE_FOR_ALL:
            await self._restart_all_children()
        elif self.strategy == RestartStrategy.REST_FOR_ONE:
            await self._restart_rest_for_one(name)

    async def _restart_child(self, name: str) -> None:
        """Restart a single child by name (local or remote)."""
        # Remote child — send restart command via message bus
        if name in self._remote_children:
            await self._restart_remote_child(name)
            return

        agent = self._find_child(name)
        if agent is None or isinstance(agent, Supervisor):
            return

        # Re-initialize the agent
        agent._status = ProcessStatus.INITIALIZING
        if self._registry is not None:
            self._registry.deregister(name)
            self._registry.register(name)
        await self._start_child(agent)

    async def _restart_remote_child(self, name: str) -> None:
        """Send a restart command to a remote worker via ZMQ."""
        if self._bus is None:
            return
        restart_msg = Message(
            type="_agency.restart",
            sender=self.name,
            recipient="_agency.worker.restart",
            payload={"agent_name": name},
        )
        await self._bus.route(restart_msg)

    async def _restart_all_children(self) -> None:
        """Stop and restart all children (ONE_FOR_ALL)."""
        # F03-5: stop all children that are not already stopped/stopping/crashed
        for child in self.children:
            if isinstance(child, Supervisor):
                await child.stop()
            elif child._status not in (
                ProcessStatus.STOPPED,
                ProcessStatus.STOPPING,
                ProcessStatus.CRASHED,
            ):
                await child._stop()

        # Restart all
        for child in self.children:
            if isinstance(child, Supervisor):
                await child.start()
            else:
                child._status = ProcessStatus.INITIALIZING
                if self._registry is not None:
                    self._registry.deregister(child.name)
                    self._registry.register(child.name)
                await self._start_child(child)

    async def _restart_rest_for_one(self, name: str) -> None:
        """Restart the crashed child and all children after it (REST_FOR_ONE)."""
        found = False
        to_restart: list[AgentProcess | Supervisor] = []

        for child in self.children:
            child_name = child.name
            if child_name == name:
                found = True
            if found:
                to_restart.append(child)

        # F03-5: stop downstream children that are not already stopped/stopping/crashed
        for child in reversed(to_restart):
            if isinstance(child, Supervisor):
                await child.stop()
            elif child._status not in (
                ProcessStatus.STOPPED,
                ProcessStatus.STOPPING,
                ProcessStatus.CRASHED,
            ):
                await child._stop()

        # Restart in order
        for child in to_restart:
            if isinstance(child, Supervisor):
                await child.start()
            else:
                child._status = ProcessStatus.INITIALIZING
                if self._registry is not None:
                    self._registry.deregister(child.name)
                    self._registry.register(child.name)
                await self._start_child(child)

    async def _escalate(self, name: str, exc: Exception) -> None:
        """Max restarts exceeded — escalate to parent or stop permanently."""
        logger.warning(
            "[%s] Max restarts (%d) exceeded for %s. Escalating.",
            self.name,
            self.max_restarts,
            name,
        )
        if self._parent is not None:
            # Escalate: parent treats this supervisor as crashed
            await self._parent._handle_crash(self.name, exc)
        else:
            # F03-6: agent is already CRASHED (task done); don't mutate status directly.
            # Log the permanent failure — agent stays CRASHED, no further restarts.
            agent = self._find_child(name)
            if agent is not None and not isinstance(agent, Supervisor):
                logger.error(
                    "[%s] Agent %r permanently stopped after exceeding max_restarts (%d).",
                    self.name,
                    name,
                    self.max_restarts,
                )

    # ------------------------------------------------------------------
    # Backoff
    # ------------------------------------------------------------------

    def _compute_backoff(self, restart_count: int) -> float:
        """Compute the delay before restarting, based on backoff policy."""
        if self.backoff == BackoffPolicy.CONSTANT:
            delay = self.backoff_base
        elif self.backoff == BackoffPolicy.LINEAR:
            delay = self.backoff_base * restart_count
        elif self.backoff == BackoffPolicy.EXPONENTIAL:
            delay = self.backoff_base * (2 ** (restart_count - 1))
            # Add jitter (up to 25%)
            delay += delay * random.random() * 0.25
        else:
            delay = self.backoff_base

        return min(delay, self.backoff_max)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_child(self, name: str) -> AgentProcess | Supervisor | None:
        """Find a child by name — O(1) via supplementary dict (F03-11)."""
        return self._children_by_name.get(name)

    def all_agents(self) -> list[AgentProcess]:
        """Recursively collect all AgentProcess instances in the tree."""
        agents: list[AgentProcess] = []
        for child in self.children:
            if isinstance(child, Supervisor):
                agents.extend(child.all_agents())
            else:
                agents.append(child)
        return agents

    def all_supervisors(self) -> list[Supervisor]:
        """Recursively collect all Supervisor instances (including self)."""
        supervisors: list[Supervisor] = [self]
        for child in self.children:
            if isinstance(child, Supervisor):
                supervisors.extend(child.all_supervisors())
        return supervisors


class RestartMode(Enum):
    """Restart policy for dynamic children."""

    PERMANENT = "permanent"
    TRANSIENT = "transient"
    NEVER = "never"


class DynamicSupervisor(AgentProcess):
    """Dynamic supervisor — starts empty, children added at runtime via spawn().

    Declared as a static child in topology YAML under ``type: dynamic_supervisor``.
    Only its children change at runtime. Enforces ONE_FOR_ONE restart semantics —
    no escalation to parent on restart exhaustion; fires on_child_terminated instead.

    Agents call self.spawn() / self.despawn() / self.stop() to manage children.
    All requests travel as bus messages (civitas.dynamic.*) so the same API works
    in-process (v0.4) and cross-process (v0.5).
    """

    def __init__(
        self,
        name: str,
        max_children: int | None = None,
        max_total_spawns: int | None = None,
        restart: str = "transient",
        max_restarts: int = 3,
        restart_window: float = 60.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self.max_children = max_children
        self.max_total_spawns = max_total_spawns
        self._restart_mode = RestartMode(restart)
        self._ds_max_restarts = max_restarts
        self._ds_restart_window = restart_window

        # Live child tracking
        self._dynamic_children: dict[str, AgentProcess] = {}
        self._child_tasks: dict[str, asyncio.Task[None]] = {}
        self._spawner_names: dict[str, str] = {}
        self._child_restart_counts: dict[str, int] = {}
        self._child_restart_timestamps: dict[str, deque[float]] = {}
        self._total_spawns: int = 0
        self._pending_child_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------
    # Governance hook — override in subclasses
    # ------------------------------------------------------------------

    async def on_spawn_requested(
        self, agent_class: type, name: str, config: dict[str, Any]
    ) -> bool:
        """Governance veto hook. Return False to deny the spawn request.

        Default implementation approves all requests. Subclass to enforce
        allowlists, rate limits, or policy checks.
        """
        return True

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    async def handle(self, message: Message) -> Message | None:  # noqa: PLR0911
        if message.type == "civitas.dynamic.spawn":
            return await self._handle_spawn(message)
        if message.type == "civitas.dynamic.despawn":
            return await self._handle_despawn(message)
        if message.type == "civitas.dynamic.stop":
            return await self._handle_stop(message)
        return None

    async def _handle_spawn(self, message: Message) -> Message | None:
        payload = message.payload
        class_path: str = payload.get("class_path", "")
        child_name: str = payload.get("name", "")
        config: dict[str, Any] = payload.get("config", {})
        spawner: str = payload.get("spawner", "")

        if child_name in self._dynamic_children:
            return self.reply(
                {"status": "error", "reason": f"agent '{child_name}' already running"}
            )
        if self.max_children is not None and len(self._dynamic_children) >= self.max_children:
            return self.reply(
                {"status": "error", "reason": f"max_children ({self.max_children}) reached"}
            )
        if self.max_total_spawns is not None and self._total_spawns >= self.max_total_spawns:
            return self.reply(
                {"status": "error", "reason": f"max_total_spawns ({self.max_total_spawns}) reached"}
            )

        # Resolve class from dotted path
        module_path, _, class_name = class_path.rpartition(".")
        if not module_path:
            return self.reply({"status": "error", "reason": f"invalid class path: '{class_path}'"})
        try:
            module = importlib.import_module(module_path)
            agent_class: type[AgentProcess] = getattr(module, class_name)
        except Exception as exc:
            return self.reply({"status": "error", "reason": f"cannot import '{class_path}': {exc}"})

        approved = await self.on_spawn_requested(agent_class, child_name, config)
        if not approved:
            return self.reply({"status": "error", "reason": "spawn denied by governance policy"})

        # Instantiate and wire
        agent = agent_class(name=child_name)
        agent._bus = self._bus
        agent._tracer = self._tracer
        agent._registry = self._registry
        agent._dynamic_supervisor_name = self.name  # children spawn into their DynSup
        agent.llm = self.llm
        agent.tools = self.tools
        agent.store = self.store

        if self._registry is not None:
            self._registry.register(child_name)
        if self._bus is not None:
            await self._bus.setup_agent(agent)

        await agent._start()

        self._dynamic_children[child_name] = agent
        self._spawner_names[child_name] = spawner
        self._total_spawns += 1

        if agent._task is not None:
            self._child_tasks[child_name] = agent._task

            def _make_cb(n: str) -> Callable[[asyncio.Task[None]], None]:
                def _cb(t: asyncio.Task[None]) -> None:
                    self._on_child_done(n, t)

                return _cb

            agent._task.add_done_callback(_make_cb(child_name))

        logger.info("[%s] spawned '%s' (%s)", self.name, child_name, class_path)
        return self.reply({"status": "ok", "name": child_name})

    async def _handle_despawn(self, message: Message) -> Message | None:
        name = message.payload.get("name", "")
        agent = self._dynamic_children.get(name)
        if agent is None:
            return self.reply({"status": "error", "reason": f"no dynamic agent '{name}'"})

        task = self._child_tasks.get(name)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        self._remove_child(name)
        if self._registry is not None:
            self._registry.deregister(name)
        logger.info("[%s] despawned '%s'", self.name, name)
        return self.reply({"status": "ok"})

    async def _handle_stop(self, message: Message) -> Message | None:
        name = message.payload.get("name", "")
        drain = message.payload.get("drain", "current")
        timeout = float(message.payload.get("timeout", 30.0))

        agent = self._dynamic_children.get(name)
        if agent is None:
            return self.reply({"status": "error", "reason": f"no dynamic agent '{name}'"})

        task = self._child_tasks.get(name)

        if drain == "all":
            # Normal-priority shutdown — queued behind pending messages
            shutdown_msg = Message(
                type="_agency.shutdown",
                sender=self.name,
                recipient=name,
                priority=0,
            )
            await agent._mailbox.put(shutdown_msg)
        else:
            # drain="current": priority shutdown, finishes current then stops
            await agent._stop()

        # Wait with timeout; fall back to hard cancel
        if task is not None and not task.done():
            try:
                async with asyncio.timeout(timeout):
                    await task
            except TimeoutError:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        self._remove_child(name)
        if self._registry is not None:
            self._registry.deregister(name)
        logger.info("[%s] stopped '%s' (drain=%s)", self.name, name, drain)
        return self.reply({"status": "ok"})

    # ------------------------------------------------------------------
    # Child monitoring and restart
    # ------------------------------------------------------------------

    def _on_child_done(self, name: str, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return  # despawned / hard-stopped — already removed
        raw_exc = task.exception()
        exc = raw_exc if isinstance(raw_exc, Exception) else None
        t = asyncio.create_task(self._handle_child_exit(name, exc))
        self._pending_child_tasks.add(t)
        t.add_done_callback(self._pending_child_tasks.discard)

    async def _handle_child_exit(self, name: str, exc: Exception | None) -> None:
        if name not in self._dynamic_children:
            return  # already removed by stop/despawn handler

        crashed = exc is not None

        if self._restart_mode == RestartMode.NEVER:
            self._remove_child(name)
            await self._notify_spawner(name, "restarts_exhausted" if crashed else "clean_exit")
            return

        if self._restart_mode == RestartMode.TRANSIENT and not crashed:
            self._remove_child(name)
            await self._notify_spawner(name, "clean_exit")
            return

        # permanent or transient+crashed: attempt restart
        now = time.time()
        self._child_restart_counts.setdefault(name, 0)
        self._child_restart_counts[name] += 1

        self._child_restart_timestamps.setdefault(name, deque())
        ts = self._child_restart_timestamps[name]
        cutoff = now - self._ds_restart_window
        ts.append(now)
        while ts and ts[0] <= cutoff:
            ts.popleft()

        if len(ts) > self._ds_max_restarts:
            # Exhausted — remove and notify spawner; do NOT escalate to parent supervisor
            self._remove_child(name)
            await self._notify_spawner(name, "restarts_exhausted")
            logger.warning(
                "[%s] child '%s' exhausted restarts (%d) — removed",
                self.name,
                name,
                self._ds_max_restarts,
            )
            return

        agent = self._dynamic_children.get(name)
        if agent is None:
            return

        logger.info(
            "[%s] restarting '%s' (attempt %d/%d)",
            self.name,
            name,
            self._child_restart_counts[name],
            self._ds_max_restarts,
        )
        agent._status = ProcessStatus.INITIALIZING
        await agent._start()

        if agent._task is not None:
            self._child_tasks[name] = agent._task

            def _make_cb(n: str) -> Callable[[asyncio.Task[None]], None]:
                def _cb(t: asyncio.Task[None]) -> None:
                    self._on_child_done(n, t)

                return _cb

            agent._task.add_done_callback(_make_cb(name))

    def _remove_child(self, name: str) -> None:
        self._dynamic_children.pop(name, None)
        self._child_tasks.pop(name, None)

    async def _notify_spawner(self, child_name: str, reason: str) -> None:
        spawner_name = self._spawner_names.get(child_name)
        if not spawner_name or self._bus is None:
            return
        await self.send(
            spawner_name,
            {"child_name": child_name, "reason": reason},
            message_type="civitas.dynamic.terminated",
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def all_dynamic_agents(self) -> list[AgentProcess]:
        """Return the currently live dynamic children."""
        return list(self._dynamic_children.values())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def on_stop(self) -> None:
        """Cancel all dynamic children on shutdown."""
        for name, _agent in list(self._dynamic_children.items()):
            task = self._child_tasks.get(name)
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        for t in list(self._pending_child_tasks):
            t.cancel()
        if self._pending_child_tasks:
            await asyncio.gather(*self._pending_child_tasks, return_exceptions=True)
