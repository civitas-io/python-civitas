"""ComponentSet — shared wiring for Runtime and Worker.

Both Runtime (single-process orchestrator) and Worker (multi-process agent host)
need the same core wiring: transport, registry, serializer, tracer, bus, and
state store. ComponentSet extracts this into a single place.

Usage (pre-built):
    cs = ComponentSet(
        transport=InProcessTransport(serializer),
        registry=LocalRegistry(),
        serializer=MsgpackSerializer(),
        tracer=Tracer(),
        store=InMemoryStateStore(),
    )
    runtime = Runtime(supervisor=..., components=cs)

Usage (from config strings, the common case):
    # Runtime and Worker call build_component_set() internally when no
    # ComponentSet is provided. Direct use is for advanced wiring only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from civitas.audit.types import AuditSink
from civitas.bus import MessageBus
from civitas.config import settings
from civitas.observability.metrics import MetricsSink
from civitas.observability.tracer import Tracer
from civitas.plugins.state import InMemoryStateStore
from civitas.registry import LocalRegistry, Registry
from civitas.serializer import JsonSerializer, MsgpackSerializer, Serializer
from civitas.transport import Transport

if TYPE_CHECKING:
    from civitas.process import AgentProcess
    from civitas.security.config import NatsTlsConfig, ZmqCurveConfig


@dataclass
class ComponentSet:
    """Assembled infrastructure wiring for a single Runtime or Worker.

    MessageBus is derived automatically from the other four fields in
    __post_init__ — callers should not construct it separately.

    Attributes:
        transport:      Transport layer (InProcess, ZMQ, or NATS).
        registry:       LocalRegistry for agent name → address mapping.
        serializer:     Serializer used by transport and bus.
        tracer:         Tracer instance for span emission.
        store:          StateStore for agent checkpoint/restore. None means
                        no persistence; callers should default to
                        InMemoryStateStore when appropriate.
        model_provider: Injected into agent.llm at startup.
        tool_registry:  Injected into agent.tools at startup.
        bus:            MessageBus built from the other four fields.
    """

    transport: Any  # Transport protocol
    registry: Registry
    serializer: Serializer
    tracer: Tracer
    store: Any = None  # StateStore | None
    model_provider: Any = None
    tool_registry: Any = None
    audit_sink: AuditSink | None = None
    metrics: MetricsSink | None = None
    span_queue: Any = None  # SpanQueue | None — set when exporters is non-empty
    export_backend: Any = None  # ExportBackend | None — drains span_queue via OTELAgent
    bus: MessageBus = field(init=False)

    def __post_init__(self) -> None:
        self.bus = MessageBus(
            transport=self.transport,
            registry=self.registry,
            serializer=self.serializer,
            tracer=self.tracer,
            audit_sink=self.audit_sink,
        )

    def inject(self, agent: AgentProcess) -> None:
        """Inject bus and plugin references into an agent process."""
        agent._bus = self.bus
        agent._tracer = self.tracer
        agent._registry = self.registry
        agent.llm = self.model_provider
        agent.tools = self.tool_registry
        agent.store = self.store
        agent._audit_sink = self.audit_sink
        agent._metrics = self.metrics


def build_component_set(
    transport_type: str = "in_process",
    serializer: Serializer | None = None,
    model_provider: Any = None,
    tool_registry: Any = None,
    state_store: Any = None,
    audit_sink: AuditSink | None = None,
    metrics: MetricsSink | None = None,
    exporters: list[Any] | None = None,
    zmq_pub_addr: str = "tcp://127.0.0.1:5559",
    zmq_sub_addr: str = "tcp://127.0.0.1:5560",
    zmq_start_proxy: bool = True,
    zmq_curve_config: ZmqCurveConfig | None = None,
    nats_servers: str | list[str] = "nats://localhost:4222",
    nats_jetstream: bool = False,
    nats_stream_name: str = "AGENCY",
    nats_tls_config: NatsTlsConfig | None = None,
) -> ComponentSet:
    """Build a ComponentSet from primitive configuration values.

    Called by Runtime.start() and Worker.start() when no pre-built
    ComponentSet is provided. Handles serializer selection, transport
    construction, and store defaulting.
    """
    # Serializer
    if serializer is not None:
        built_serializer = serializer
    elif settings.serializer == "json":
        built_serializer = JsonSerializer()
    else:
        built_serializer = MsgpackSerializer()

    # Tracer — when exporters are configured (plugins.exporters in topology YAML),
    # spans flow through SpanQueue -> OTELAgent -> ExportBackend instead of the
    # Tracer's own direct OTLP path, so the two never run simultaneously (FD-09).
    # Runtime/Worker are responsible for starting/stopping the OTELAgent task.
    built_span_queue: Any = None
    built_export_backend: Any = None
    if exporters:
        from civitas.observability.export_backend import FanOutBackend
        from civitas.observability.span_queue import SpanQueue

        built_span_queue = SpanQueue()
        built_export_backend = FanOutBackend(exporters) if len(exporters) > 1 else exporters[0]
    built_tracer = Tracer(span_queue=built_span_queue)

    # Transport — imports are intentionally scoped here: ZMQ and NATS are optional
    # extras (pyzmq, nats-py) that may not be installed. Importing at module level
    # would cause ImportError on every civitas import for users without those extras.
    built_transport: Transport
    if transport_type == "zmq":
        from civitas.transport.zmq import ZMQTransport

        built_transport = ZMQTransport(
            built_serializer,
            pub_addr=zmq_pub_addr,
            sub_addr=zmq_sub_addr,
            start_proxy=zmq_start_proxy,
            curve_config=zmq_curve_config,
        )
    elif transport_type == "nats":
        from civitas.transport.nats import NATSTransport

        built_transport = NATSTransport(
            built_serializer,
            servers=nats_servers,
            jetstream=nats_jetstream,
            stream_name=nats_stream_name,
            tls_config=nats_tls_config,
        )
    else:
        from civitas.transport.inprocess import InProcessTransport

        built_transport = InProcessTransport(built_serializer)

    # Registry
    built_registry = LocalRegistry()

    # State store default
    if state_store is None:
        state_store = InMemoryStateStore()

    return ComponentSet(
        transport=built_transport,
        registry=built_registry,
        serializer=built_serializer,
        tracer=built_tracer,
        store=state_store,
        model_provider=model_provider,
        tool_registry=tool_registry,
        audit_sink=audit_sink,
        metrics=metrics,
        span_queue=built_span_queue,
        export_backend=built_export_backend,
    )
