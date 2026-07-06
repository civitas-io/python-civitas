"""Unit tests for M4.2e — Audit Log."""

from __future__ import annotations

import asyncio
import json
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas import AgentProcess
from civitas.audit import AuditEvent, AuditSink, JsonlFileSink, NullSink, sink_from_config
from civitas.messages import Message


# Module-level agent class so Runtime.from_config() can resolve it by dotted path.
class _AuditTestAgent(AgentProcess):
    async def handle(self, msg: Message) -> None:
        pass


# ---------------------------------------------------------------------------
# AuditSink protocol conformance
# ---------------------------------------------------------------------------


class TestAuditSinkProtocol:
    def test_null_sink_satisfies_protocol(self):
        assert isinstance(NullSink(), AuditSink)

    @pytest.mark.asyncio
    async def test_json_file_sink_satisfies_protocol(self, tmp_path: Path):
        sink = JsonlFileSink(tmp_path / "audit.jsonl")
        assert isinstance(sink, AuditSink)
        await sink.close()


# ---------------------------------------------------------------------------
# NullSink
# ---------------------------------------------------------------------------


class TestNullSink:
    @pytest.mark.asyncio
    async def test_emit_does_not_raise(self):
        sink = NullSink()
        await sink.emit(
            AuditEvent(
                event="message.route",
                ts="2026-05-03T00:00:00Z",
                agent="a",
                signer_id="a",
                details={},
            )
        )

    @pytest.mark.asyncio
    async def test_flush_does_not_raise(self):
        await NullSink().flush()

    @pytest.mark.asyncio
    async def test_close_does_not_raise(self):
        await NullSink().close()


# ---------------------------------------------------------------------------
# JsonlFileSink
# ---------------------------------------------------------------------------


class TestJsonlFileSink:
    def _event(self, name: str = "test.event") -> AuditEvent:
        return AuditEvent(
            event=name,
            ts="2026-05-03T00:00:00Z",
            agent="agent_a",
            signer_id="",
            details={"k": "v"},
        )

    @pytest.mark.asyncio
    async def test_emit_writes_jsonl_line(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path)
        await sink.emit(self._event())
        await sink.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["event"] == "test.event"
        assert record["agent"] == "agent_a"

    @pytest.mark.asyncio
    async def test_multiple_events_all_written(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path)
        for i in range(5):
            await sink.emit(self._event(f"event.{i}"))
        await sink.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 5
        events = [json.loads(line)["event"] for line in lines]
        assert events == [f"event.{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_flush_drains_buffer(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path, flush_interval=9999.0)  # won't auto-flush
        await sink.emit(self._event())
        await sink.flush()
        assert path.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_sync_writes_option(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path, sync_writes=True)
        await sink.emit(self._event())
        await sink.close()
        assert path.read_text().strip() != ""

    @pytest.mark.asyncio
    async def test_appends_to_existing_file(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        path.write_text('{"existing": true}\n')
        sink = JsonlFileSink(path)
        await sink.emit(self._event())
        await sink.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"existing": True}

    @pytest.mark.asyncio
    async def test_batch_size_triggers_flush(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path, batch_size=3, flush_interval=9999.0)
        for i in range(3):
            await sink.emit(self._event(f"e{i}"))
        # Give the drain loop a tick to process the batch
        await asyncio.sleep(0.05)
        await sink.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3

    @pytest.mark.asyncio
    async def test_sighup_rotates_file(self, tmp_path: Path):
        path = tmp_path / "audit.jsonl"
        sink = JsonlFileSink(path)
        await sink.emit(self._event("before.rotate"))
        await sink.flush()

        # Simulate SIGHUP by setting the rotate flag directly
        sink._rotate_flag = True
        await sink.emit(self._event("after.rotate"))
        await sink.close()

        lines = path.read_text().strip().splitlines()
        assert any("after.rotate" in line for line in lines)


# ---------------------------------------------------------------------------
# sink_from_config factory
# ---------------------------------------------------------------------------


class TestSinkFromConfig:
    def test_null_sink(self):
        s = sink_from_config({"sink": "null"})
        assert isinstance(s, NullSink)

    def test_default_is_null(self):
        s = sink_from_config({})
        assert isinstance(s, NullSink)

    def test_jsonl_sink(self, tmp_path: Path):
        s = sink_from_config({"sink": "jsonl", "path": str(tmp_path / "a.jsonl")})
        assert isinstance(s, JsonlFileSink)

    def test_jsonl_sink_sync_writes(self, tmp_path: Path):
        s = sink_from_config(
            {"sink": "jsonl", "path": str(tmp_path / "a.jsonl"), "sync_writes": True}
        )
        assert isinstance(s, JsonlFileSink)
        assert s._sync_writes is True


# ---------------------------------------------------------------------------
# MessageBus — audit emission on route()
# ---------------------------------------------------------------------------


class TestMessageBusAudit:
    @pytest.mark.asyncio
    async def test_route_emits_message_route_event(self):
        from civitas.bus import MessageBus
        from civitas.messages import Message

        sink = NullSink()
        sink.emit = AsyncMock()

        transport = MagicMock()
        transport.has_reply_address.return_value = False
        transport.publish = AsyncMock()

        registry = MagicMock()
        registry.lookup.return_value = MagicMock(address="agent_b")

        tracer = MagicMock()
        tracer.start_send_span.return_value = MagicMock()

        bus = MessageBus(
            transport=transport,
            registry=registry,
            serializer=MagicMock(serialize=MagicMock(return_value=b"")),
            tracer=tracer,
            audit_sink=sink,
        )

        msg = Message(sender="agent_a", recipient="agent_b", type="task")
        await bus.route(msg)

        sink.emit.assert_awaited_once()
        event = sink.emit.call_args[0][0]
        assert event["event"] == "message.route"
        assert event["agent"] == "agent_a"
        assert event["details"]["recipient"] == "agent_b"

    @pytest.mark.asyncio
    async def test_route_no_audit_when_sink_is_none(self):
        from civitas.bus import MessageBus
        from civitas.messages import Message

        transport = MagicMock()
        transport.has_reply_address.return_value = False
        transport.publish = AsyncMock()
        registry = MagicMock()
        registry.lookup.return_value = MagicMock(address="b")
        tracer = MagicMock()
        tracer.start_send_span.return_value = MagicMock()

        bus = MessageBus(
            transport=transport,
            registry=registry,
            serializer=MagicMock(serialize=MagicMock(return_value=b"")),
            tracer=tracer,
            audit_sink=None,
        )
        msg = Message(sender="a", recipient="b")
        await bus.route(msg)  # must not raise


# ---------------------------------------------------------------------------
# AgentProcess — secret.access audit on get_credential()
# ---------------------------------------------------------------------------


class TestAgentProcessAudit:
    @pytest.mark.asyncio
    async def test_get_credential_emits_secret_access(self):
        from civitas import AgentProcess

        class MyAgent(AgentProcess):
            async def handle(self, msg: Message) -> None:
                pass

        agent = MyAgent("tester")
        agent._credentials = {"anthropic": "sk-secret"}

        sink = NullSink()
        sink.emit = AsyncMock()
        agent._audit_sink = sink

        result = agent.get_credential("anthropic")
        assert result == "sk-secret"

        await asyncio.sleep(0)  # let create_task run
        sink.emit.assert_awaited_once()
        event = sink.emit.call_args[0][0]
        assert event["event"] == "secret.access"
        assert event["agent"] == "tester"
        assert event["details"]["provider"] == "anthropic"

    @pytest.mark.asyncio
    async def test_get_credential_no_emit_when_missing(self):
        from civitas import AgentProcess

        class MyAgent(AgentProcess):
            async def handle(self, msg: Message) -> None:
                pass

        agent = MyAgent("tester")
        sink = NullSink()
        sink.emit = AsyncMock()
        agent._audit_sink = sink

        result = agent.get_credential("nonexistent")
        assert result is None
        await asyncio.sleep(0)
        sink.emit.assert_not_awaited()


# ---------------------------------------------------------------------------
# ComponentSet — audit_sink threaded through
# ---------------------------------------------------------------------------


class TestComponentSetAudit:
    def test_audit_sink_wired_to_bus(self):
        from civitas.components import build_component_set

        sink = NullSink()
        cs = build_component_set(audit_sink=sink)
        assert cs.bus._audit_sink is sink

    def test_audit_sink_injected_into_agent(self):
        from civitas import AgentProcess
        from civitas.components import build_component_set

        class MyAgent(AgentProcess):
            async def handle(self, msg: Message) -> None:
                pass

        sink = NullSink()
        cs = build_component_set(audit_sink=sink)
        agent = MyAgent("a")
        cs.inject(agent)
        assert agent._audit_sink is sink


# ---------------------------------------------------------------------------
# Runtime.from_config — audit: block parsed
# ---------------------------------------------------------------------------


class TestRuntimeAuditParsing:
    def test_audit_jsonl_parsed(self, tmp_path: Path):
        from civitas import Runtime

        audit_path = tmp_path / "audit.jsonl"
        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent(f"""\
            supervision:
              name: root
              children:
                - agent:
                    name: a
                    type: tests.unit.test_audit._AuditTestAgent
            audit:
              sink: jsonl
              path: {audit_path}
            """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._audit_sink is not None
        assert isinstance(rt._audit_sink, JsonlFileSink)

    def test_no_audit_block_gives_none(self, tmp_path: Path):
        from civitas import Runtime

        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - agent:
                    name: a
                    type: tests.unit.test_audit._AuditTestAgent
            """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._audit_sink is None
