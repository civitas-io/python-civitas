"""Unit tests for Runtime — config parsing, tree rendering, agent lookup, lifecycle guards."""

from __future__ import annotations

import logging
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from civitas import AgentProcess, Runtime, Supervisor
from civitas.components import build_component_set
from civitas.config import Settings
from civitas.errors import ConfigurationError as AgencyConfigError
from civitas.messages import Message
from civitas.serializer import JsonSerializer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class NullAgent(AgentProcess):
    async def handle(self, message: Message) -> None:
        return None


# ---------------------------------------------------------------------------
# Settings — frozen at instantiation, validated, SecretStr
# ---------------------------------------------------------------------------


class TestSettings:
    def test_default_serializer_is_msgpack(self):
        s = Settings(env={})
        assert s.serializer == "msgpack"

    def test_json_serializer_accepted(self):
        s = Settings(env={"AGENCY_SERIALIZER": "json"})
        assert s.serializer == "json"

    def test_invalid_serializer_raises(self):
        with pytest.raises(AgencyConfigError, match="AGENCY_SERIALIZER"):
            Settings(env={"AGENCY_SERIALIZER": "grpc"})

    def test_frozen_at_instantiation(self):
        # Values are attributes, not dynamic property lookups
        s = Settings(env={"AGENCY_SERIALIZER": "json"})
        assert isinstance(s.serializer, str)

    def test_secret_str_masked_in_repr(self):
        s = Settings(env={"ANTHROPIC_API_KEY": "sk-secret"})
        r = repr(s.anthropic_api_key)
        assert "sk-secret" not in r
        assert "**" in r

    def test_secret_str_masked_in_str(self):
        s = Settings(env={"OPENAI_API_KEY": "openai-abc"})
        assert "openai-abc" not in str(s.openai_api_key)

    def test_secret_str_get_returns_value(self):
        s = Settings(env={"ANTHROPIC_API_KEY": "sk-real"})
        assert s.anthropic_api_key.get() == "sk-real"

    def test_secret_str_none_is_falsy(self):
        s = Settings(env={})
        assert not s.anthropic_api_key

    def test_secret_str_with_value_is_truthy(self):
        s = Settings(env={"ANTHROPIC_API_KEY": "x"})
        assert s.anthropic_api_key

    def test_otel_endpoint_none_when_missing(self):
        s = Settings(env={})
        assert s.otel_endpoint is None

    def test_nats_url_default(self):
        s = Settings(env={})
        assert s.nats_url == "nats://localhost:4222"


# ---------------------------------------------------------------------------
# Runtime.print_tree
# ---------------------------------------------------------------------------


class TestPrintTree:
    def test_no_supervisor_returns_placeholder(self):
        rt = Runtime()
        assert rt.print_tree() == "(no supervision tree)"

    def test_flat_tree(self):
        rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a"), NullAgent("b")]))
        tree = rt.print_tree()
        assert "root" in tree
        assert "a" in tree
        assert "b" in tree

    def test_nested_tree_shows_child_supervisor(self):
        child_sup = Supervisor("workers", children=[NullAgent("w1")])
        root = Supervisor("root", children=[NullAgent("gatekeeper"), child_sup])
        rt = Runtime(supervisor=root)
        tree = rt.print_tree()
        assert "workers" in tree
        assert "w1" in tree
        assert "gatekeeper" in tree


# ---------------------------------------------------------------------------
# Runtime.get_agent
# ---------------------------------------------------------------------------


class TestGetAgent:
    def test_returns_none_before_start(self):
        agent = NullAgent("a")
        rt = Runtime(supervisor=Supervisor("root", children=[agent]))
        assert rt.get_agent("a") is None

    def test_returns_none_for_no_supervisor(self):
        rt = Runtime()
        assert rt.get_agent("anything") is None

    @pytest.mark.asyncio
    async def test_returns_agent_after_start(self):
        agent = NullAgent("a")
        rt = Runtime(supervisor=Supervisor("root", children=[agent]))
        await rt.start()
        try:
            assert rt.get_agent("a") is agent
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_returns_none_for_missing_name(self):
        rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
        await rt.start()
        try:
            assert rt.get_agent("missing") is None
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_returns_none_after_stop(self):
        rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
        await rt.start()
        await rt.stop()
        assert rt.get_agent("a") is None


# ---------------------------------------------------------------------------
# Runtime.on_crash / set_metrics (FD-01/FD-03)
# ---------------------------------------------------------------------------


class TestOnCrash:
    def test_delegates_to_root_supervisor(self):
        sup = Supervisor("root", children=[NullAgent("a")])
        rt = Runtime(supervisor=sup)
        received = []

        async def callback(name: str, exc: Exception) -> None:
            received.append(name)

        rt.on_crash(callback)
        assert sup._crash_callbacks == [callback]

    def test_noop_with_no_supervisor(self):
        rt = Runtime()

        async def callback(name: str, exc: Exception) -> None:
            pass

        rt.on_crash(callback)  # must not raise


class TestSetMetrics:
    @pytest.mark.asyncio
    async def test_metrics_injected_into_agents(self):
        agent = NullAgent("a")
        rt = Runtime(supervisor=Supervisor("root", children=[agent]))
        sink = object()
        rt.set_metrics(sink)
        await rt.start()
        try:
            assert agent._metrics is sink
        finally:
            await rt.stop()


class TestExportersWiring:
    """Runtime(exporters=[...]) drains real spans through OTELAgent (FD-07/FD-09)."""

    @pytest.mark.asyncio
    async def test_exporter_receives_spans_end_to_end(self):
        from civitas.observability.span_queue import SpanData

        received: list[SpanData] = []

        class RecordingBackend:
            async def export(self, spans: list[SpanData]) -> None:
                received.extend(spans)

            async def shutdown(self) -> None:
                pass

        agent = NullAgent("a")
        rt = Runtime(
            supervisor=Supervisor("root", children=[agent]),
            exporters=[RecordingBackend()],
        )
        await rt.start()
        assert rt._otel_agent_task is not None
        await rt.send("a", {})
        await rt.stop()  # drains any spans still queued before returning

        assert rt._otel_agent_task is None
        assert len(received) > 0

    @pytest.mark.asyncio
    async def test_no_exporters_no_otel_agent_task(self):
        agent = NullAgent("a")
        rt = Runtime(supervisor=Supervisor("root", children=[agent]))
        await rt.start()
        try:
            assert rt._otel_agent_task is None
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# Runtime.start / stop lifecycle guards
# ---------------------------------------------------------------------------


class TestLifecycleGuards:
    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
        await rt.start()
        try:
            # Second start should be a no-op, not raise
            await rt.start()
            assert rt._started is True
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_on_unstarted_runtime_is_safe(self):
        rt = Runtime()
        # Should not raise
        await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_started_flag(self):
        rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
        await rt.start()
        await rt.stop()
        assert rt._started is False


# ---------------------------------------------------------------------------
# Runtime.from_config — YAML parsing
# ---------------------------------------------------------------------------


class TestFromConfig:
    def test_missing_supervision_key_raises(self, tmp_path: Path):
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text(
            textwrap.dedent("""\
            transport:
              type: in_process
        """)
        )
        with pytest.raises(AgencyConfigError, match="supervision"):
            Runtime.from_config(bad_yaml)

    def test_typo_key_raises(self, tmp_path: Path):
        bad_yaml = tmp_path / "typo.yaml"
        bad_yaml.write_text(
            textwrap.dedent("""\
            supervison:
              name: root
        """)
        )
        with pytest.raises(AgencyConfigError, match="supervision"):
            Runtime.from_config(bad_yaml)

    def test_bad_agent_type_raises_configuration_error(self, tmp_path: Path):
        bad_yaml = tmp_path / "bad_type.yaml"
        bad_yaml.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - type: "nonexistent.module.FakeAgent"
                  name: "fake"
        """)
        )
        with pytest.raises(AgencyConfigError, match="Cannot load agent type"):
            Runtime.from_config(bad_yaml)

    def test_unresolvable_short_name_raises(self, tmp_path: Path):
        bad_yaml = tmp_path / "short.yaml"
        bad_yaml.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - type: "NoModule"
                  name: "x"
        """)
        )
        with pytest.raises((AgencyConfigError, ValueError)):
            Runtime.from_config(bad_yaml)

    def test_valid_minimal_config(self, tmp_path: Path):
        """A supervision key with no children produces a runtime without error."""
        yaml_file = tmp_path / "minimal.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
        """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._root_supervisor is not None
        assert rt._root_supervisor.name == "root"

    def test_agent_classes_dict_resolution(self, tmp_path: Path):
        yaml_file = tmp_path / "agents.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - type: "NullAgent"
                  name: "worker"
        """)
        )
        rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
        agents = rt._root_supervisor.all_agents()
        assert len(agents) == 1
        assert agents[0].name == "worker"
        assert isinstance(agents[0], NullAgent)

    def test_presidium_key_accepted(self, tmp_path: Path):
        yaml_file = tmp_path / "presidium.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            presidium:
              registry:
                trust_domain: acme.com
            supervision:
              name: root
        """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._root_supervisor is not None


class TestFromConfigDict:
    def test_produces_identical_runtime(self, tmp_path: Path):
        yaml_file = tmp_path / "test.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - type: "NullAgent"
                  name: "worker"
        """)
        )
        rt_file = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
        config = {
            "supervision": {"name": "root", "children": [{"type": "NullAgent", "name": "worker"}]}
        }
        rt_dict = Runtime.from_config_dict(config, agent_classes={"NullAgent": NullAgent})
        assert rt_file._root_supervisor.name == rt_dict._root_supervisor.name
        assert len(rt_file._root_supervisor.all_agents()) == len(
            rt_dict._root_supervisor.all_agents()
        )

    def test_rejects_unknown_keys(self):
        config = {"bogus": True, "supervision": {"name": "root"}}
        with pytest.raises(AgencyConfigError, match="bogus"):
            Runtime.from_config_dict(config)

    def test_accepts_presidium_key(self):
        config = {
            "presidium": {"registry": {"trust_domain": "acme.com"}},
            "supervision": {"name": "root"},
        }
        rt = Runtime.from_config_dict(config)
        assert rt._root_supervisor is not None

    def test_minimal_config(self):
        config = {"supervision": {"name": "root"}}
        rt = Runtime.from_config_dict(config)
        assert rt._root_supervisor.name == "root"

    def test_eval_agent_exporters_require_civitas_contrib(self):
        """v0.9.1 top-up: an eval_agent's exporters: list is built by importing
        civitas_contrib.eval.exporters, which is NOT a core dependency (the
        model-provider split, v0.4.0). This is the only branch of
        _build_exporters testable in core CI without installing contrib — the
        per-kind bodies (arize/langfuse/braintrust/langsmith/fiddler) are
        unreachable in this environment: the import happens before the kind
        dispatch, so every kind hits this same ImportError first. Documented
        ceiling, matching the cli/state.py precedent (v0.8.2 G3).
        """
        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "type": "eval_agent",
                        "name": "evaluator",
                        "exporters": [{"type": "arize"}],
                    }
                ],
            }
        }
        with pytest.raises(AgencyConfigError, match="civitas-contrib"):
            Runtime.from_config_dict(config)


class TestProcessFilter:
    """v0.9.2.1 bugfix: `process:`-tagged nodes were never filtered at all --
    `Runtime.from_config()` built EVERY node regardless, duplicating whatever
    a real Worker process also builds for a `process:`-tagged node (confirmed
    on examples/deployment/level2_multi_process/run_supervisor.py). See
    civitas/runtime.py's `process_filter` docstring for the full contract.
    """

    MULTI_PROCESS_CONFIG = {
        "supervision": {
            "name": "root",
            "children": [
                {"type": "NullAgent", "name": "frontend"},
                {"type": "NullAgent", "name": "worker_a", "process": "worker"},
                {"type": "NullAgent", "name": "worker_b", "process": "worker"},
            ],
        }
    }

    def test_default_builds_everything_unchanged(self):
        """process_filter="*" (the default) must be a complete no-op --
        every caller before this fix gets IDENTICAL behavior."""
        rt = Runtime.from_config_dict(
            self.MULTI_PROCESS_CONFIG, agent_classes={"NullAgent": NullAgent}
        )
        names = {a.name for a in rt._root_supervisor.all_agents()}
        assert names == {"frontend", "worker_a", "worker_b"}

    def test_none_builds_only_untagged_nodes(self):
        """The actual bug fix: process_filter=None ("I am the coordinator")
        must exclude every process:-tagged node."""
        rt = Runtime.from_config_dict(
            self.MULTI_PROCESS_CONFIG, agent_classes={"NullAgent": NullAgent}, process_filter=None
        )
        names = {a.name for a in rt._root_supervisor.all_agents()}
        assert names == {"frontend"}

    def test_named_process_builds_only_matching_nodes(self):
        """process_filter="worker" -- symmetry with Worker's own
        _find_process_agents filtering, exposed on Runtime too."""
        rt = Runtime.from_config_dict(
            self.MULTI_PROCESS_CONFIG,
            agent_classes={"NullAgent": NullAgent},
            process_filter="worker",
        )
        names = {a.name for a in rt._root_supervisor.all_agents()}
        assert names == {"worker_a", "worker_b"}

    def test_nested_supervisor_stays_transparent(self):
        """A nested supervisor is never itself process:-tagged (matches
        cli/run.py's _find_process_agents convention) -- it must always be
        descended into, regardless of process_filter, so a tagged LEAF several
        levels deep is still found (or correctly excluded)."""
        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "supervisor": {
                            "name": "nested",
                            "children": [
                                {"type": "NullAgent", "name": "deep_untagged"},
                                {"type": "NullAgent", "name": "deep_worker", "process": "worker"},
                            ],
                        }
                    },
                ],
            }
        }
        rt_coordinator = Runtime.from_config_dict(
            config, agent_classes={"NullAgent": NullAgent}, process_filter=None
        )
        assert {a.name for a in rt_coordinator._root_supervisor.all_agents()} == {"deep_untagged"}

        rt_worker = Runtime.from_config_dict(
            config, agent_classes={"NullAgent": NullAgent}, process_filter="worker"
        )
        assert {a.name for a in rt_worker._root_supervisor.all_agents()} == {"deep_worker"}

    def test_dynamic_supervisor_process_tag_is_filtered(self):
        """dynamic_supervisor nodes carry process: directly on the node (not
        nested under an agent: key) -- confirmed via cli/run.py's own
        _find_process_agents, which checks it the same way."""
        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {"type": "dynamic_supervisor", "name": "workers", "process": "worker"},
                ],
            }
        }
        rt = Runtime.from_config_dict(config, process_filter=None)
        assert rt._root_supervisor.all_agents() == []

        rt_worker = Runtime.from_config_dict(config, process_filter="worker")
        assert [a.name for a in rt_worker._root_supervisor.all_agents()] == ["workers"]


# ---------------------------------------------------------------------------
# F12-9 — Example YAML topology files are valid against the config schema
# ---------------------------------------------------------------------------

_EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"


# A fallback agent_classes dict that resolves any type string to a NullAgent
# stub, so from_config() can parse example files without real implementations.
class _AnyClasses(dict):  # type: ignore[type-arg]
    """Accepts any type string and returns NullAgent."""

    def __contains__(self, key: object) -> bool:
        return True

    def __getitem__(self, key: object) -> type[AgentProcess]:
        return NullAgent


_ANY = _AnyClasses({"_": NullAgent})  # non-empty so `agent_classes or {}` keeps it


@pytest.mark.parametrize(
    "example_file",
    [
        "topology.yaml",
        "multi_process.yaml",
        "distributed.yaml",
    ],
)
def test_example_topology_parses(example_file: str) -> None:
    """Example YAML files parse without ConfigurationError (F12-9)."""
    path = _EXAMPLES_DIR / example_file
    rt = Runtime.from_config(path, agent_classes=_ANY)
    assert rt._root_supervisor is not None
    assert rt._root_supervisor.name  # non-empty name


def test_example_production_yaml_supervision_structure() -> None:
    """production.yaml supervision tree parses; plugin loading is mocked (F12-9)."""
    mock_loaded = {"model_providers": [], "state_store": None, "exporters": []}
    path = _EXAMPLES_DIR / "production.yaml"
    with patch("civitas.plugins.loader.load_plugins_from_config", return_value=mock_loaded):
        rt = Runtime.from_config(path, agent_classes=_ANY)
    assert rt._root_supervisor is not None
    assert rt._root_supervisor.name == "root"


# ---------------------------------------------------------------------------
# build_component_set — serializer selection branches (components.py)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Runtime.all_agents — no-supervisor branch
# ---------------------------------------------------------------------------


def test_all_agents_no_supervisor_returns_empty() -> None:
    """all_agents() returns [] when no supervisor is configured."""
    rt = Runtime()
    assert rt.all_agents() == []


def test_all_agents_with_supervisor_returns_agents() -> None:
    """all_agents() returns the agent list from the supervision tree (line 199)."""
    agent = NullAgent("a")
    rt = Runtime(supervisor=Supervisor("root", children=[agent]))
    result = rt.all_agents()
    assert agent in result


# ---------------------------------------------------------------------------
# Runtime.from_config — plugin loading branches
# ---------------------------------------------------------------------------


def test_from_config_loads_single_model_provider(tmp_path: Path) -> None:
    """YAML with one model provider injects it into the Runtime constructor."""
    yaml_file = tmp_path / "plugins.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
        plugins:
          models:
            - type: anthropic
    """)
    )

    fake_provider = MagicMock()
    mock_loaded = {"model_providers": [fake_provider], "state_store": None, "exporters": []}

    with patch("civitas.runtime.load_plugins_from_config", return_value=mock_loaded):
        rt = Runtime.from_config(yaml_file, agent_classes=_ANY)

    assert rt._model_provider is fake_provider


def test_from_config_multiple_providers_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """When YAML has >1 model providers, a warning is logged and the first is used."""
    yaml_file = tmp_path / "two_providers.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
        plugins:
          models:
            - type: anthropic
            - type: openai
    """)
    )

    p1, p2 = MagicMock(), MagicMock()
    mock_loaded = {"model_providers": [p1, p2], "state_store": None, "exporters": []}

    with patch("civitas.runtime.load_plugins_from_config", return_value=mock_loaded):
        with caplog.at_level(logging.WARNING, logger="civitas.runtime"):
            rt = Runtime.from_config(yaml_file, agent_classes=_ANY)

    assert rt._model_provider is p1
    assert any("Multiple model providers" in r.message for r in caplog.records)


def test_from_config_loads_state_store(tmp_path: Path) -> None:
    """YAML with a state store injects it into the Runtime constructor."""
    yaml_file = tmp_path / "state.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
        plugins:
          state:
            type: in_memory
    """)
    )

    fake_store = MagicMock()
    mock_loaded = {"model_providers": [], "state_store": fake_store, "exporters": []}

    with patch("civitas.runtime.load_plugins_from_config", return_value=mock_loaded):
        rt = Runtime.from_config(yaml_file, agent_classes=_ANY)

    assert rt._state_store is fake_store


# ---------------------------------------------------------------------------
# build_component_set — serializer selection branches (components.py)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Runtime.start — no-supervisor early return, pre-built components
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_no_supervisor_sets_started_flag() -> None:
    """start() with no supervisor sets _started=True and returns early (lines 264-265)."""
    rt = Runtime()
    await rt.start()
    try:
        assert rt._started is True
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_stop_no_supervisor_skips_supervisor_stop() -> None:
    """stop() skips supervisor.stop() when root supervisor is None (branch 329->333)."""
    rt = Runtime()
    await rt.start()
    await rt.stop()  # must not raise; no supervisor to stop
    assert rt._started is False


@pytest.mark.asyncio
async def test_start_uses_prebuilt_components() -> None:
    """Runtime started with components= skips build_component_set (line 239)."""
    # Build a real in-process ComponentSet, then pass it explicitly so that
    # build_component_set should NOT be called internally.
    cs = build_component_set()
    agent = NullAgent("a")
    rt = Runtime(supervisor=Supervisor("root", children=[agent]), components=cs)
    with patch("civitas.runtime.build_component_set") as mock_build:
        await rt.start()
        mock_build.assert_not_called()
    await rt.stop()


# ---------------------------------------------------------------------------
# Runtime.ask / Runtime.send — not-started guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ask_before_start_raises() -> None:
    """ask() raises RuntimeError when runtime is not started (line 369)."""
    rt = Runtime()
    with pytest.raises(RuntimeError, match="not started"):
        await rt.ask("nobody", {})


@pytest.mark.asyncio
async def test_send_before_start_raises() -> None:
    """send() raises RuntimeError when runtime is not started (line 391)."""
    rt = Runtime()
    with pytest.raises(RuntimeError, match="not started"):
        await rt.send("nobody", {})


# ---------------------------------------------------------------------------
# Runtime.from_config — unknown node type, zmq/nats transport branches
# ---------------------------------------------------------------------------


def test_from_config_unknown_node_raises(tmp_path: Path) -> None:
    """_build_node() raises ValueError for nodes with no recognized keys (line 142)."""
    yaml_file = tmp_path / "bad_node.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
          children:
            - completely_unknown_key: value
    """)
    )
    with pytest.raises((ValueError, AgencyConfigError)):
        Runtime.from_config(yaml_file)


def test_from_config_zmq_transport_all_params(tmp_path: Path) -> None:
    """YAML with zmq transport + all config keys is parsed (branches 166->168, 168->170, 170->181)."""
    yaml_file = tmp_path / "zmq.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
        transport:
          type: zmq
          pub_addr: "tcp://127.0.0.1:5559"
          sub_addr: "tcp://127.0.0.1:5560"
          start_proxy: true
    """)
    )
    rt = Runtime.from_config(yaml_file)
    assert rt._transport_type == "zmq"
    assert rt._zmq_pub_addr == "tcp://127.0.0.1:5559"
    assert rt._zmq_sub_addr == "tcp://127.0.0.1:5560"
    assert rt._zmq_start_proxy is True


def test_from_config_nats_transport_with_jetstream(tmp_path: Path) -> None:
    """YAML with nats transport + jetstream key is parsed (branch 173->175)."""
    yaml_file = tmp_path / "nats.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
        transport:
          type: nats
          servers: "nats://localhost:4222"
          jetstream: true
          stream_name: MYSTREAM
    """)
    )
    rt = Runtime.from_config(yaml_file)
    assert rt._transport_type == "nats"
    assert rt._nats_jetstream is True
    assert rt._nats_stream_name == "MYSTREAM"


# ---------------------------------------------------------------------------
# build_component_set — serializer selection branches (components.py)
# ---------------------------------------------------------------------------


def test_build_component_set_custom_serializer() -> None:
    """build_component_set uses the provided serializer instance directly."""
    custom = JsonSerializer()
    cs = build_component_set(serializer=custom)
    assert cs.serializer is custom


def test_build_component_set_json_serializer_from_settings() -> None:
    """build_component_set picks JsonSerializer when settings.serializer == 'json'."""
    with patch("civitas.components.settings") as mock_settings:
        mock_settings.serializer = "json"
        cs = build_component_set()
    assert isinstance(cs.serializer, JsonSerializer)


# ---------------------------------------------------------------------------
# _extract_agent_credentials — flat node format (lines 55-57)
# ---------------------------------------------------------------------------


class TestExtractAgentCredentialsFlatNode:
    def test_flat_node_with_credentials_and_name(self):
        from civitas.runtime import _extract_agent_credentials

        config = {
            "supervision": {
                "children": [
                    {
                        "name": "flat_agent",
                        "credentials": {"anthropic": "sk-ant-test"},
                    }
                ]
            }
        }
        result = _extract_agent_credentials(config)
        assert result == {"flat_agent": {"anthropic": "sk-ant-test"}}

    def test_flat_node_without_credentials_excluded(self):
        from civitas.runtime import _extract_agent_credentials

        config = {"supervision": {"children": [{"name": "no_creds"}]}}
        result = _extract_agent_credentials(config)
        assert result == {}


# ---------------------------------------------------------------------------
# _extract_public_keys — flat node format (lines 78-79)
# ---------------------------------------------------------------------------


class TestExtractPublicKeysFlatNode:
    def test_flat_node_with_public_key(self):
        from civitas.runtime import _extract_public_keys

        config = {"supervision": {"children": [{"name": "remote_agent", "public_key": "abc123=="}]}}
        result = _extract_public_keys(config)
        assert result == {"remote_agent": "abc123=="}

    def test_flat_node_without_public_key_excluded(self):
        from civitas.runtime import _extract_public_keys

        config = {"supervision": {"children": [{"name": "no_key"}]}}
        result = _extract_public_keys(config)
        assert result == {}


# ---------------------------------------------------------------------------
# from_config — module.class flat node format (lines 302-304)
# ---------------------------------------------------------------------------


def test_from_config_module_class_node_format(tmp_path: Path) -> None:
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
          children:
            - type: gen_server
              module: tests.unit.test_runtime
              class: NullAgent
              name: my_agent
        """)
    )
    rt = Runtime.from_config(yaml_file)
    agents = rt.all_agents()
    assert any(a.name == "my_agent" for a in agents)


# ---------------------------------------------------------------------------
# from_config — ZMQ transport kwargs (lines 334-339)
# ---------------------------------------------------------------------------


def test_from_config_zmq_transport_kwargs(tmp_path: Path) -> None:
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        transport:
          type: zmq
          pub_addr: tcp://0.0.0.0:5570
          sub_addr: tcp://0.0.0.0:5571
          start_proxy: false
        supervision:
          name: root
          children:
            - agent:
                name: a
                type: tests.unit.test_runtime.NullAgent
        """)
    )
    rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
    assert rt._transport_type == "zmq"
    assert rt._zmq_pub_addr == "tcp://0.0.0.0:5570"
    assert rt._zmq_sub_addr == "tcp://0.0.0.0:5571"
    assert rt._zmq_start_proxy is False


# ---------------------------------------------------------------------------
# stop() — state_store.close() and tracer.flush() (lines 617-628)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_closes_state_store() -> None:
    mock_store = AsyncMock()
    rt = Runtime(
        supervisor=Supervisor("root", children=[NullAgent("a")]),
        state_store=mock_store,
    )
    await rt.start()
    await rt.stop()
    mock_store.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_flushes_tracer() -> None:
    rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
    await rt.start()
    tracer = rt._tracer
    with patch.object(tracer, "flush") as mock_flush:
        await rt.stop()
    mock_flush.assert_called_once()


# ---------------------------------------------------------------------------
# Agent credentials wired during start() (line 513)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_credentials_wired_from_topology(tmp_path: Path) -> None:
    import os

    os.environ["TEST_CRED_KEY"] = "sk-test-value"
    try:
        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - agent:
                    name: credagent
                    type: tests.unit.test_runtime.NullAgent
                    credentials:
                      anthropic: ${TEST_CRED_KEY}
            """)
        )
        rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
        await rt.start()
        try:
            agent = rt.get_agent("credagent")
            assert agent is not None
            assert agent._credentials == {"anthropic": "sk-test-value"}
        finally:
            await rt.stop()
    finally:
        del os.environ["TEST_CRED_KEY"]


# ---------------------------------------------------------------------------
# despawn / stop_agent failure → SpawnError (lines 735, 752)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_despawn_failure_raises_spawn_error() -> None:
    from civitas.errors import SpawnError

    rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
    await rt.start()
    try:
        error_reply = MagicMock()
        error_reply.payload = {"status": "error", "reason": "not found"}
        with patch.object(rt, "ask", return_value=error_reply):
            with pytest.raises(SpawnError, match="not found"):
                await rt.despawn("root", "missing")
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_stop_agent_failure_raises_spawn_error() -> None:
    from civitas.errors import SpawnError

    rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
    await rt.start()
    try:
        error_reply = MagicMock()
        error_reply.payload = {"status": "error", "reason": "no such agent"}
        with patch.object(rt, "ask", return_value=error_reply):
            with pytest.raises(SpawnError, match="no such agent"):
                await rt.stop_agent("root", "missing")
    finally:
        await rt.stop()


# ---------------------------------------------------------------------------
# MCP connection error during start() is logged, not raised (lines 589-594)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_connection_error_logged_not_raised() -> None:
    from civitas.mcp.types import MCPServerConfig

    rt = Runtime(supervisor=Supervisor("root", children=[NullAgent("a")]))
    rt._mcp_configs = [
        MCPServerConfig(name="bad", transport="stdio", command="/nonexistent/server")
    ]
    # Should start without raising even though MCP connect fails
    await rt.start()
    await rt.stop()


# ---------------------------------------------------------------------------
# print_tree() — covers labels for DynamicSupervisor, TopologyAgent,
# EvalAgent, HTTPGateway, GenServer
# ---------------------------------------------------------------------------


def test_print_tree_various_node_labels() -> None:
    # v0.9.5: TopologyServer removed; TopologyAgent (a _TopologyIntrospection)
    # now carries the [topo] tag.
    from civitas.evalloop import EvalAgent
    from civitas.gateway import GatewayConfig, HTTPGateway
    from civitas.genserver import GenServer
    from civitas.supervisor import DynamicSupervisor
    from civitas.topology_server import TopologyAgent

    class MyServer(GenServer):
        async def handle_call(self, msg):  # type: ignore[override]
            return None

    children = [
        DynamicSupervisor("dyn"),
        TopologyAgent("topo"),
        EvalAgent("eval_a"),
        HTTPGateway("http_gw", GatewayConfig()),
        MyServer("srv"),
    ]
    rt = Runtime(supervisor=Supervisor("root", children=children))
    tree = rt.print_tree()
    assert "[dyn]" in tree
    assert "[topo]" in tree
    assert "[eval]" in tree
    assert "[http]" in tree
    assert "[srv]" in tree


# ---------------------------------------------------------------------------
# from_config — MCP sandbox block parsed (line 369)
# ---------------------------------------------------------------------------


def test_from_config_mcp_sandbox_parsed(tmp_path: Path) -> None:
    from civitas.sandbox.config import SandboxConfig

    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
          children:
            - agent:
                name: a
                type: tests.unit.test_runtime.NullAgent
        mcp:
          servers:
            - name: shell
              transport: stdio
              command: /bin/sh
              sandbox:
                enabled: true
                network: deny
                filesystem:
                  - /workspace:rw
        """)
    )
    rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
    assert len(rt._mcp_configs) == 1
    sb = rt._mcp_configs[0].sandbox
    assert sb is not None
    assert isinstance(sb, SandboxConfig)
    assert sb.enabled is True


# ---------------------------------------------------------------------------
# from_config — fiddler exporter parsed (lines 228-236)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# from_config — topology_server node (lines 256-262)
# ---------------------------------------------------------------------------


def test_from_config_topology_server_node(tmp_path: Path) -> None:
    # v0.9.5 (topology-gateway-merge.md D3/D3a): a topology_server node now
    # builds a dedicated sub-supervisor over a TopologyAgent (data provider)
    # AND an internally-owned HTTPGateway (HTTP + auth), replacing the old
    # single zero-auth TopologyServer. Test updated from asserting "exactly one
    # TopologyServer" to asserting the new two-process shape.
    from civitas.gateway import HTTPGateway
    from civitas.topology_server import TopologyAgent

    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        supervision:
          name: root
          children:
            - type: topology_server
              name: topo
              config:
                host: 127.0.0.1
                port: 7000
        """)
    )
    rt = Runtime.from_config(yaml_file)
    agents = rt.all_agents()
    topo_agents = [a for a in agents if isinstance(a, TopologyAgent)]
    gateways = [a for a in agents if isinstance(a, HTTPGateway)]
    assert len(topo_agents) == 1
    assert topo_agents[0].name == "topo"
    # The internally-owned gateway serves the seven fixed routes at the
    # configured host/port, pointing at the TopologyAgent by name.
    assert len(gateways) == 1
    gw = gateways[0]
    assert gw.name == "topo_gateway"
    assert gw._gw_config.host == "127.0.0.1"
    assert gw._gw_config.port == 7000
    assert gw._gw_config.topology_agent == "topo"
    # Wrapped in a dedicated sub-supervisor (Option A).
    assert any(s.name == "topo_supervisor" for s in rt._root_supervisor.all_supervisors())


# ---------------------------------------------------------------------------
# from_config — zmq transport without optional kwargs (branches 334->336 etc.)
# ---------------------------------------------------------------------------


def test_from_config_zmq_transport_no_optional_kwargs(tmp_path: Path) -> None:
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        transport:
          type: zmq
        supervision:
          name: root
          children:
            - agent:
                name: a
                type: tests.unit.test_runtime.NullAgent
        """)
    )
    rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
    assert rt._transport_type == "zmq"
    # No optional pub/sub/proxy keys in YAML — branches 334->336, 336->338, 338->349 taken


# ---------------------------------------------------------------------------
# from_config — nats transport with jetstream + stream_name (branch 341->343)
# ---------------------------------------------------------------------------


def test_from_config_nats_transport_with_jetstream_v2(tmp_path: Path) -> None:
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        transport:
          type: nats
          servers:
            - nats://localhost:4222
          jetstream: true
          stream_name: civitas
        supervision:
          name: root
          children:
            - agent:
                name: a
                type: tests.unit.test_runtime.NullAgent
        """)
    )
    rt = Runtime.from_config(yaml_file, agent_classes={"NullAgent": NullAgent})
    assert rt._transport_type == "nats"
