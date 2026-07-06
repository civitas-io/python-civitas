"""Unit tests for ComponentSet — injection wiring, including MetricsSink (FD-01/FD-03)."""

from __future__ import annotations

from unittest.mock import MagicMock

from civitas.components import ComponentSet, build_component_set
from civitas.observability.tracer import Tracer
from civitas.process import AgentProcess
from civitas.registry import LocalRegistry
from civitas.serializer import MsgpackSerializer
from civitas.transport.inprocess import InProcessTransport


class _Agent(AgentProcess):
    async def handle(self, message: object) -> None:
        return None


def _make_component_set(**overrides: object) -> ComponentSet:
    serializer = MsgpackSerializer()
    defaults: dict[str, object] = {
        "transport": InProcessTransport(serializer),
        "registry": LocalRegistry(),
        "serializer": serializer,
        "tracer": Tracer(),
    }
    defaults.update(overrides)
    return ComponentSet(**defaults)  # type: ignore[arg-type]


class TestComponentSetInject:
    def test_metrics_injected_into_agent(self) -> None:
        sink = MagicMock()
        cs = _make_component_set(metrics=sink)
        agent = _Agent("a")
        cs.inject(agent)
        assert agent._metrics is sink

    def test_metrics_none_by_default(self) -> None:
        cs = _make_component_set()
        agent = _Agent("a")
        cs.inject(agent)
        assert agent._metrics is None

    def test_existing_fields_still_injected_alongside_metrics(self) -> None:
        sink = MagicMock()
        cs = _make_component_set(metrics=sink)
        agent = _Agent("a")
        cs.inject(agent)
        assert agent._bus is cs.bus
        assert agent._tracer is cs.tracer
        assert agent._registry is cs.registry


class TestBuildComponentSet:
    def test_metrics_passed_through(self) -> None:
        sink = MagicMock()
        cs = build_component_set(metrics=sink)
        assert cs.metrics is sink

    def test_metrics_none_by_default(self) -> None:
        cs = build_component_set()
        assert cs.metrics is None

    def test_no_exporters_leaves_span_queue_none(self) -> None:
        """Without exporters, Tracer keeps its default (non-queue) behavior (FD-07/FD-09)."""
        cs = build_component_set()
        assert cs.span_queue is None
        assert cs.export_backend is None
        assert cs.tracer._span_queue is None

    def test_single_exporter_used_directly(self) -> None:
        """A single exporter is used as-is, not wrapped in FanOutBackend (FD-07)."""
        backend = MagicMock()
        cs = build_component_set(exporters=[backend])
        assert cs.span_queue is not None
        assert cs.export_backend is backend
        assert cs.tracer._span_queue is cs.span_queue

    def test_multiple_exporters_wrapped_in_fanout(self) -> None:
        """Multiple exporters are combined via FanOutBackend (FD-07)."""
        from civitas.observability.export_backend import FanOutBackend

        backend_a, backend_b = MagicMock(), MagicMock()
        cs = build_component_set(exporters=[backend_a, backend_b])
        assert isinstance(cs.export_backend, FanOutBackend)
        assert cs.export_backend._backends == [backend_a, backend_b]
