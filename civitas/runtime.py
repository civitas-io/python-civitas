"""Runtime — wires components together, manages lifecycle."""

from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import yaml

from civitas.audit.sinks import sink_from_config
from civitas.components import ComponentSet, build_component_set
from civitas.dashboard.collector import MetricsCollector
from civitas.errors import (
    ConfigurationError,
    DeserializationError,
    MessageRoutingError,
    SignatureError,
    SpawnError,
)
from civitas.evalloop import EvalAgent, EvalExporter
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.genserver import GenServer
from civitas.mcp.types import MCPServerConfig
from civitas.messages import Message, _new_span_id, _uuid7
from civitas.observability.otel_agent import run_otel_agent
from civitas.plugins.loader import load_plugins_from_config
from civitas.process import (
    DYNAMIC_SUPERVISOR_CAPABILITY,
    SUPERVISOR_CAPABILITY,
    AgentProcess,
    SuspendCategory,
)
from civitas.sandbox.config import SandboxConfig
from civitas.secrets.substitution import substitute_vars
from civitas.security.config import GatewayAuthConfig, SecurityConfig
from civitas.security.identity import AgentIdentity
from civitas.security.registry import KeyRegistry
from civitas.security.signing import MessageSigner, SigningSerializer
from civitas.serializer import Serializer
from civitas.supervisor import DynamicSupervisor, Supervisor
from civitas.topology_server import TopologyAgent, _TopologyIntrospection

logger = logging.getLogger(__name__)


def _extract_agent_capabilities(
    config: dict[str, Any],
) -> dict[str, tuple[list[str], dict[str, Any]]]:
    """Walk the supervision tree and collect {agent_name: (capabilities, metadata)} entries."""
    result: dict[str, tuple[list[str], dict[str, Any]]] = {}

    def _scan(node: dict[str, Any]) -> None:
        if "agent" in node:
            cfg = node["agent"]
            caps = cfg.get("capabilities")
            meta = cfg.get("capability_metadata", {})
            if caps and isinstance(caps, list):
                result[cfg["name"]] = ([str(c) for c in caps], dict(meta) if meta else {})
        elif "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                _scan(child)
        elif "capabilities" in node and "name" in node:
            caps = node["capabilities"]
            meta = node.get("capability_metadata", {})
            if isinstance(caps, list):
                result[node["name"]] = ([str(c) for c in caps], dict(meta) if meta else {})

    sup = config.get("supervision") or config.get("supervisor", {})
    sup_dict: dict[str, Any] = sup if isinstance(sup, dict) else {}
    for child in sup_dict.get("children", []):
        _scan(child)
    return result


def _extract_agent_credentials(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    """Walk the supervision tree and collect {agent_name: {provider: credential}} entries."""
    result: dict[str, dict[str, str]] = {}

    def _scan(node: dict[str, Any]) -> None:
        if "agent" in node:
            cfg = node["agent"]
            creds = cfg.get("credentials")
            if creds and isinstance(creds, dict):
                result[cfg["name"]] = {str(k): str(v) for k, v in creds.items()}
        elif "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                _scan(child)
        elif "credentials" in node and "name" in node:
            creds = node["credentials"]
            if isinstance(creds, dict):
                result[node["name"]] = {str(k): str(v) for k, v in creds.items()}

    sup = config.get("supervision") or config.get("supervisor", {})
    sup_dict: dict[str, Any] = sup if isinstance(sup, dict) else {}
    for child in sup_dict.get("children", []):
        _scan(child)
    return result


def _extract_public_keys(config: dict[str, Any]) -> dict[str, str]:
    """Walk the supervision tree and collect {agent_name: public_key_b64} entries."""
    result: dict[str, str] = {}

    def _scan(node: dict[str, Any]) -> None:
        if "agent" in node:
            cfg = node["agent"]
            if "public_key" in cfg:
                result[cfg["name"]] = cfg["public_key"]
        elif "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                _scan(child)
        elif "public_key" in node and "name" in node:
            result[node["name"]] = node["public_key"]

    sup = config.get("supervision") or config.get("supervisor", {})
    sup_dict: dict[str, Any] = sup if isinstance(sup, dict) else {}
    for child in sup_dict.get("children", []):
        _scan(child)
    return result


class Runtime:
    """Assembles and manages the full Civitas runtime.

    Startup sequence (from Implementation Guide §3):
    1. Read configuration
    2. Create Serializer
    3. Create Tracer
    4. Create Transport
    5. Create Registry
    6. Create MessageBus
    7. Create plugin instances
    8. Instantiate / wire all AgentProcesses
    9. Register all AgentProcesses in Registry
    10. Start Transport
    11. Walk supervision tree bottom-up, start each agent
    12. Start all Supervisors
    13. Runtime is ready
    """

    def __init__(
        self,
        supervisor: Supervisor | None = None,
        transport: str = "in_process",
        serializer: Serializer | None = None,
        model_provider: Any = None,
        tool_registry: Any = None,
        state_store: Any = None,
        metrics: Any = None,
        exporters: list[Any] | None = None,
        zmq_pub_addr: str = "tcp://127.0.0.1:5559",
        zmq_sub_addr: str = "tcp://127.0.0.1:5560",
        zmq_start_proxy: bool = True,
        nats_servers: str | list[str] = "nats://localhost:4222",
        nats_jetstream: bool = False,
        nats_stream_name: str = "AGENCY",
        components: ComponentSet | None = None,
    ) -> None:
        self._root_supervisor = supervisor
        self._transport_type = transport
        self._custom_serializer = serializer
        self._model_provider = model_provider
        self._tool_registry = tool_registry
        self._state_store = state_store
        self._metrics = metrics
        self._exporters = exporters or []
        self._components = components
        self._otel_agent_task: asyncio.Task[None] | None = None

        # ZMQ-specific config
        self._zmq_pub_addr = zmq_pub_addr
        self._zmq_sub_addr = zmq_sub_addr
        self._zmq_start_proxy = zmq_start_proxy

        # NATS-specific config
        self._nats_servers = nats_servers
        self._nats_jetstream = nats_jetstream
        self._nats_stream_name = nats_stream_name

        # MCP server configs parsed from topology YAML
        self._mcp_configs: list[Any] = []

        # Security config — populated by from_config() when a security: block is present
        self._security_config: Any = None
        self._topology_public_keys: dict[str, str] = {}

        # Signing infrastructure — built in start() when signing is enabled on a
        # distributed transport; used to verify cross-process announcements (R6 · D8).
        self._key_registry: Any = None
        self._message_signer: Any = None
        self._signing_on: bool = False

        # Per-agent credentials — populated by from_config() from credentials: blocks
        self._agent_credentials: dict[str, dict[str, str]] = {}

        # Per-agent capabilities — populated by from_config() from capabilities: blocks
        self._agent_capabilities: dict[str, tuple[list[str], dict[str, Any]]] = {}

        # Audit sink — populated by from_config() when an audit: block is present
        self._audit_sink: Any = None

        # Transport security (ZMQ CURVE / NATS TLS) — populated by from_config()
        self._transport_security: Any = None

        # Set during start() — exposed for stop(), ask()/send(), and get_agent()
        self._serializer: Serializer | None = None
        self._tracer: Any = None
        self._transport: Any = None
        self._registry: Any = None
        self._bus: Any = None
        self._agents_by_name: dict[str, AgentProcess] = {}  # F04-10: O(1) live process lookup
        self._started = False

    _KNOWN_CONFIG_KEYS = {
        "transport",
        "plugins",
        "supervision",
        "supervisor",
        "mcp",
        "security",
        "audit",
        "presidium",
    }

    @classmethod
    def from_config(
        cls,
        path: str | Path,
        agent_classes: dict[str, type[AgentProcess]] | None = None,
        *,
        process_filter: str | None = "*",
    ) -> Runtime:
        """Build a Runtime from a YAML topology file.

        The ``agent_classes`` dict maps type strings (e.g. "MyAgent") to the
        actual Python class. If not provided, types are resolved via
        ``importlib`` from dotted module paths (e.g. "myapp.agents.MyAgent").

        ``process_filter`` (v0.9.2.1 bugfix): a topology node (an ``agent``,
        ``dynamic_supervisor``, or flat dotted-path node) MAY carry a
        ``process: <name>`` tag meaning "this belongs to a different OS
        process, started separately." Before this fix, ``from_config`` had
        no awareness of that tag at all and built EVERY node into the local
        tree regardless — silently duplicating whatever a real Worker
        process elsewhere also builds for itself (confirmed:
        ``deployment/level2_multi_process/run_supervisor.py``, run alone,
        registered ``worker_a``/``worker_b`` locally even though they're
        ``process: worker``-tagged).

        - ``"*"`` (default): no filtering — build every node regardless of
          any ``process:`` tag. Unchanged from every release before this
          one; existing callers see no behavior change.
        - ``None``: build only UNTAGGED nodes (no ``process:`` key at all)
          — the coordinator/supervisor role in a multi-process topology.
        - Any other string: build only nodes tagged ``process: <that string>``
          — matches what ``Worker``'s own construction path already does via
          ``_find_process_agents``, exposed here for symmetry/completeness.
        """
        config = yaml.safe_load(Path(path).read_text())
        config = substitute_vars(config)
        return cls.from_config_dict(
            config, agent_classes=agent_classes, process_filter=process_filter
        )

    @classmethod
    def from_config_dict(
        cls,
        config: dict[str, Any],
        agent_classes: dict[str, type[AgentProcess]] | None = None,
        *,
        process_filter: str | None = "*",
    ) -> Runtime:
        """Build a Runtime from an already-parsed config dict.

        Same as ``from_config`` but accepts a dict instead of a file path.
        Useful for governance layers (e.g. Presidium) that need to extract
        their own config block before passing the rest to the runtime. See
        ``from_config``'s docstring for ``process_filter``'s semantics.
        """
        unknown = set(config.keys()) - cls._KNOWN_CONFIG_KEYS
        if unknown:
            raise ConfigurationError(
                f"Unknown top-level config keys: {sorted(unknown)!r}. "
                f"Valid keys are: {sorted(cls._KNOWN_CONFIG_KEYS)!r}"
            )
        classes = agent_classes or {}

        def _resolve_class(type_str: str) -> type[AgentProcess]:
            if type_str in classes:
                return classes[type_str]
            # Try dotted import path: "myapp.agents.MyAgent"
            module_path, _, class_name = type_str.rpartition(".")
            if not module_path:
                raise ValueError(
                    f"Cannot resolve agent type '{type_str}'. "
                    f"Provide it in agent_classes or use a dotted path."
                )
            try:
                module = importlib.import_module(module_path)
                return cast(type[AgentProcess], getattr(module, class_name))
            except (ImportError, AttributeError) as exc:
                raise ConfigurationError(
                    f"Cannot load agent type '{type_str}': {exc}. "
                    f"Check that the module is installed and the class name is correct."
                ) from exc

        def _build_exporters(cfgs: list[dict[str, Any]]) -> list[EvalExporter]:
            result: list[EvalExporter] = []
            for cfg in cfgs:
                kind = cfg.get("type", "")
                try:
                    from civitas_contrib.eval.exporters import (
                        ArizeExporter,
                        BraintrustExporter,
                        FiddlerExporter,
                        LangfuseExporter,
                        LangSmithExporter,
                    )
                except ImportError as exc:
                    raise ConfigurationError(
                        f"Eval exporter '{kind}' requires civitas-contrib. "
                        "Install it with: pip install civitas-contrib"
                    ) from exc
                if kind == "arize":
                    result.append(
                        ArizeExporter(
                            endpoint=cfg.get("endpoint", "http://localhost:6006/v1/traces"),
                            service_name=cfg.get("service_name", "civitas"),
                        )
                    )
                elif kind == "langfuse":
                    result.append(
                        LangfuseExporter(
                            public_key=cfg["public_key"],
                            secret_key=cfg["secret_key"],
                            host=cfg.get("host", "https://cloud.langfuse.com"),
                        )
                    )
                elif kind == "braintrust":
                    result.append(
                        BraintrustExporter(
                            api_key=cfg["api_key"],
                            project=cfg.get("project", "civitas"),
                        )
                    )
                elif kind == "langsmith":
                    result.append(
                        LangSmithExporter(
                            api_key=cfg["api_key"],
                            project=cfg.get("project", "civitas"),
                        )
                    )
                elif kind == "fiddler":
                    result.append(
                        FiddlerExporter(
                            url=cfg["url"],
                            token=cfg["token"],
                            org_id=cfg["org_id"],
                            project_id=cfg["project_id"],
                            model_id=cfg["model_id"],
                        )
                    )
                else:
                    logger.warning("Unknown eval exporter type '%s' — skipping", kind)
            return result

        def _node_process_tag(node: dict[str, Any]) -> str | None:
            """The ``process:`` tag for a node, wherever its shape puts it
            (nested ``agent:`` dict vs. every other flatter node type) — v0.9.2.1
            bugfix. Returns None for an untagged node (the common,
            single-process case)."""
            if "agent" in node:
                tag = node["agent"].get("process")
            else:
                tag = node.get("process")
            return str(tag) if tag is not None else None

        def _node_belongs_to(node: dict[str, Any]) -> bool:
            """Whether ``node`` should be built under the active
            ``process_filter`` (v0.9.2.1 bugfix). ``"*"`` builds everything
            (today's default, unchanged); otherwise the node's own
            ``process:`` tag (None for untagged) must match exactly."""
            if process_filter == "*":
                return True
            return _node_process_tag(node) == process_filter

        def _build_node(node: dict[str, Any]) -> AgentProcess | Supervisor | None:
            # Supervisor nodes are always transparent/structural, regardless of
            # process_filter -- mirroring cli/run.py's _find_process_agents,
            # which always descends into a nested supervisor unconditionally
            # and only ever checks process: on agent/dynamic_supervisor LEAF
            # nodes. Filtering a supervisor itself would silently discard its
            # entire subtree, including any leaf that DOES belong here.
            if "supervisor" in node:
                sup_cfg = node["supervisor"]
                children = [
                    c
                    for c in (_build_node(c) for c in sup_cfg.get("children", []))
                    if c is not None
                ]
                return Supervisor(
                    name=sup_cfg["name"],
                    children=children,
                    strategy=sup_cfg.get("strategy", "ONE_FOR_ONE").upper(),
                    max_restarts=sup_cfg.get("max_restarts", 3),
                    restart_window=sup_cfg.get("restart_window", 60.0),
                    backoff=sup_cfg.get("backoff", "CONSTANT").upper(),
                    backoff_base=sup_cfg.get("backoff_base", 1.0),
                    backoff_max=sup_cfg.get("backoff_max", 60.0),
                )
            if not _node_belongs_to(node):
                return None
            if node.get("type") == "topology_server":
                # v0.9.5 (topology-gateway-merge.md D3/D3a): a topology_server
                # node now builds a dedicated sub-supervisor over a TopologyAgent
                # (privileged-injected, the data provider) and an
                # internally-owned HTTPGateway (which serves the seven fixed
                # introspection routes with the same auth stack as any
                # http_gateway). The old zero-auth TopologyServer's own HTTP
                # server is gone. host default stays 127.0.0.1 (TopologyServer's
                # default), NOT HTTPGateway's 0.0.0.0 -- behavior-preserving.
                cfg = node.get("config", {})
                agent_name = node.get("name", "topology_server")
                # v0.9.6 (control-plane-writes.md, D6c attach_to): with
                # attach_to set, build ONLY the TopologyAgent -- no dedicated
                # gateway. The named http_gateway (declared separately, with
                # topology_agent: <this name> in its own config) serves the
                # routes on its existing port. Single-pass, no cross-node
                # mutation: the link is by name in the gateway's own config.
                if cfg.get("attach_to"):
                    return TopologyAgent(name=agent_name)
                auth_block = cfg.get("auth", {})
                auth_cfg = GatewayAuthConfig.from_dict(auth_block)
                topo_agent = TopologyAgent(name=agent_name)
                gw_config = GatewayConfig(
                    host=cfg.get("host", "127.0.0.1"),
                    port=cfg.get("port", 6789),
                    tls_cert=cfg.get("tls_cert"),
                    tls_key=cfg.get("tls_key"),
                    tls_ca_cert=auth_cfg.tls_ca_cert,
                    client_cert_mode=auth_cfg.client_cert_mode,
                    mtls_source=auth_cfg.mtls_source,
                    trusted_proxy_cidrs=auth_cfg.trusted_proxy_cidrs,
                    topology_agent=agent_name,
                    topology_prefix=cfg.get("prefix", ""),
                    # D5: the auth: block's middleware list becomes ROUTE
                    # middleware on the six non-/health routes (/health stays
                    # auth-free). NOT global middleware, which would also gate
                    # /health.
                    topology_middleware=auth_block.get("middleware", []),
                    # An introspection endpoint has no user-facing API docs
                    # surface -- the old TopologyServer served none either.
                    docs_enabled=False,
                )
                gateway = HTTPGateway(name=f"{agent_name}_gateway", config=gw_config)
                return Supervisor(
                    name=f"{agent_name}_supervisor",
                    children=[topo_agent, gateway],
                    strategy="ONE_FOR_ONE",
                )
            elif node.get("type") == "dynamic_supervisor" and "name" in node:
                return DynamicSupervisor(
                    name=node["name"],
                    max_children=node.get("max_children"),
                    max_total_spawns=node.get("max_total_spawns"),
                    max_children_per_spawner=node.get("max_children_per_spawner"),
                    max_total_spawns_per_spawner=node.get("max_total_spawns_per_spawner"),
                    restart=node.get("restart", "transient"),
                    max_restarts=node.get("max_restarts", 3),
                    restart_window=node.get("restart_window", 60.0),
                )
            elif node.get("type") == "eval_agent" and "name" in node:
                return EvalAgent(
                    name=node["name"],
                    max_corrections_per_window=node.get("max_corrections_per_window", 10),
                    window_seconds=node.get("window_seconds", 60.0),
                    exporters=_build_exporters(node.get("exporters", [])),
                )
            elif node.get("type") == "http_gateway" and "name" in node:
                cfg_dict = node.get("config", {})
                auth_cfg = GatewayAuthConfig.from_dict(cfg_dict.get("auth", {}))
                docs_cfg = cfg_dict.get("docs", {})
                gw_config = GatewayConfig(
                    host=cfg_dict.get("host", "0.0.0.0"),
                    port=cfg_dict.get("port", 8080),
                    port_quic=cfg_dict.get("port_quic"),
                    tls_cert=cfg_dict.get("tls_cert"),
                    tls_key=cfg_dict.get("tls_key"),
                    tls_ca_cert=auth_cfg.tls_ca_cert,
                    client_cert_mode=auth_cfg.client_cert_mode,
                    mtls_source=auth_cfg.mtls_source,
                    trusted_proxy_cidrs=auth_cfg.trusted_proxy_cidrs,
                    request_timeout=cfg_dict.get("request_timeout", 30.0),
                    enable_http3=cfg_dict.get("enable_http3", False),
                    routes=cfg_dict.get("routes", []),
                    middleware=cfg_dict.get("middleware", []),
                    docs_enabled=docs_cfg.get("enabled"),
                    docs_path=docs_cfg.get("path", "/docs"),
                    # v0.9.6 (D6c attach_to): an http_gateway can ALSO serve a
                    # TopologyAgent's introspection/control routes on its own
                    # port -- set topology_agent to that agent's name (paired
                    # with a topology_server node using attach_to: <this
                    # gateway>). topology_middleware here gates those routes
                    # (the auth: block gates the user's own routes).
                    topology_agent=cfg_dict.get("topology_agent"),
                    topology_prefix=cfg_dict.get("topology_prefix", ""),
                    topology_middleware=cfg_dict.get("topology_middleware", []),
                )
                return HTTPGateway(name=node["name"], config=gw_config)
            elif "agent" in node:
                agent_cfg = node["agent"]
                agent_cls = _resolve_class(agent_cfg["type"])
                agent_kwargs: dict[str, Any] = {"name": agent_cfg["name"]}
                # H6: opt-in per-message watchdog — only passed when configured, so
                # subclasses with narrow __init__ signatures stay compatible.
                if "handle_timeout" in agent_cfg:
                    agent_kwargs["handle_timeout"] = float(agent_cfg["handle_timeout"])
                return agent_cls(**agent_kwargs)
            elif (
                node.get("type") in ("gen_server", "agent") and "module" in node and "class" in node
            ):
                cls_path = f"{node['module']}.{node['class']}"
                agent_cls = _resolve_class(cls_path)
                return agent_cls(name=node["name"])
            elif "type" in node and "name" in node:
                # Flat dotted-path format: {type: "module.Class", name: "agent_name"}
                agent_cls = _resolve_class(node["type"])
                return agent_cls(name=node["name"])
            else:
                raise ValueError(f"Unknown node type in config: {node}")

        sup_cfg = config.get("supervision") or config.get("supervisor")
        if not sup_cfg:
            raise ConfigurationError("YAML topology must define a top-level 'supervision' key.")
        # Top-level is always a supervisor
        children = [
            c for c in (_build_node(c) for c in sup_cfg.get("children", [])) if c is not None
        ]
        root = Supervisor(
            name=sup_cfg.get("name", "root"),
            children=children,
            strategy=sup_cfg.get("strategy", "ONE_FOR_ONE").upper(),
            max_restarts=sup_cfg.get("max_restarts", 3),
            restart_window=sup_cfg.get("restart_window", 60.0),
            backoff=sup_cfg.get("backoff", "CONSTANT").upper(),
            backoff_base=sup_cfg.get("backoff_base", 1.0),
            backoff_max=sup_cfg.get("backoff_max", 60.0),
        )

        # Transport config
        transport_cfg = config.get("transport", {})
        transport_type = transport_cfg.get("type", "in_process")

        kwargs: dict[str, Any] = {"supervisor": root, "transport": transport_type}
        if transport_type == "zmq":
            if "pub_addr" in transport_cfg:
                kwargs["zmq_pub_addr"] = transport_cfg["pub_addr"]
            if "sub_addr" in transport_cfg:
                kwargs["zmq_sub_addr"] = transport_cfg["sub_addr"]
            if "start_proxy" in transport_cfg:
                kwargs["zmq_start_proxy"] = transport_cfg["start_proxy"]
        elif transport_type == "nats":
            if "servers" in transport_cfg:
                kwargs["nats_servers"] = transport_cfg["servers"]
            if "jetstream" in transport_cfg:
                kwargs["nats_jetstream"] = transport_cfg["jetstream"]
            if "stream_name" in transport_cfg:
                kwargs["nats_stream_name"] = transport_cfg["stream_name"]

        # Plugin config
        if "plugins" in config:
            loaded = load_plugins_from_config(config)
            if loaded["model_providers"]:
                if len(loaded["model_providers"]) > 1:
                    logger.warning(
                        "Multiple model providers found in YAML; using the first one. "
                        "Additional providers are ignored."
                    )
                kwargs["model_provider"] = loaded["model_providers"][0]
            if loaded["state_store"] is not None:
                kwargs["state_store"] = loaded["state_store"]
            if loaded["exporters"]:
                kwargs["exporters"] = loaded["exporters"]

        runtime = cls(**kwargs)

        # MCP server config — parsed here, connected during start()
        mcp_section = config.get("mcp", {})
        if mcp_section.get("servers"):
            for srv in mcp_section["servers"]:
                sandbox = None
                if srv.get("sandbox"):
                    sandbox = SandboxConfig.from_dict(srv["sandbox"])
                runtime._mcp_configs.append(
                    MCPServerConfig(
                        name=srv["name"],
                        transport=srv["transport"],
                        command=srv.get("command"),
                        args=srv.get("args", []),
                        env=srv.get("env"),
                        url=srv.get("url"),
                        sandbox=sandbox,
                    )
                )

        # Per-agent credentials — parsed here, applied in start()
        runtime._agent_credentials = _extract_agent_credentials(config)

        # Per-agent capabilities — parsed here, applied in start()
        runtime._agent_capabilities = _extract_agent_capabilities(config)

        # Security config — parsed here, applied in start()
        security_section = config.get("security")
        if security_section:
            runtime._security_config = SecurityConfig.from_dict(security_section)
            runtime._topology_public_keys = _extract_public_keys(config)

        # Audit sink — parsed here, threaded into ComponentSet during start()
        audit_section = config.get("audit")
        if audit_section:
            runtime._audit_sink = sink_from_config(audit_section)

        # Transport security (CURVE / TLS) — parsed from security.transport block
        if security_section:
            from civitas.security.config import TransportSecurityConfig

            transport_section = security_section.get("transport", {})
            if transport_section:
                runtime._transport_security = TransportSecurityConfig.from_dict(transport_section)

        return runtime

    def all_agents(self) -> list[AgentProcess]:
        """Return all AgentProcess instances in the supervision tree."""
        if self._root_supervisor is None:
            return []
        return self._root_supervisor.all_agents()

    def on_crash(self, callback: Callable[[str, Exception], Awaitable[None]]) -> None:
        """Register a callback invoked with (agent_name, exception) on every crash.

        Runs before the supervisor applies its restart strategy. Only observes
        crashes handled directly by the root supervisor — a crash retried
        successfully by a nested child supervisor without escalating does
        not surface here. Call before start().
        """
        if self._root_supervisor is not None:
            self._root_supervisor.add_crash_callback(callback)

    def set_metrics(self, metrics: Any) -> None:
        """Attach a MetricsSink (e.g. the dashboard's MetricsCollector).

        Must be called before start() — the sink is injected into agents as
        part of ComponentSet assembly during startup.
        """
        self._metrics = metrics

    def print_tree(self) -> str:
        """Return an ASCII representation of the supervision tree."""
        if self._root_supervisor is None:
            return "(no supervision tree)"

        lines: list[str] = []

        def _walk(node: Supervisor | AgentProcess, prefix: str, is_last: bool) -> None:
            connector = "└── " if is_last else "├── "
            if isinstance(node, Supervisor):
                label = f"[sup] {node.name} ({node.strategy.value})"
            else:
                status = node.status.value if hasattr(node, "status") else "?"
                if isinstance(node, DynamicSupervisor):
                    prefix_tag = "[dyn]"
                elif isinstance(node, _TopologyIntrospection):
                    prefix_tag = "[topo]"
                elif isinstance(node, EvalAgent):
                    prefix_tag = "[eval]"
                elif isinstance(node, HTTPGateway):
                    prefix_tag = "[http]"
                elif isinstance(node, GenServer):
                    prefix_tag = "[srv]"
                else:
                    prefix_tag = "[agent]"
                label = f"{prefix_tag} {node.name} ({status})"
            lines.append(f"{prefix}{connector}{label}")

            if isinstance(node, Supervisor):
                child_prefix = prefix + ("    " if is_last else "│   ")
                for i, child in enumerate(node.children):
                    _walk(child, child_prefix, i == len(node.children) - 1)

        # Root
        root = self._root_supervisor
        lines.append(f"[sup] {root.name} ({root.strategy.value})")
        for i, child in enumerate(root.children):
            _walk(child, "", i == len(root.children) - 1)

        return "\n".join(lines)

    async def start(self) -> None:
        """Start the runtime following the canonical initialization sequence."""
        if self._started:
            return

        # v0.9.1 (dashboard-v2, D-DASH-4): auto-provide a MetricsCollector when a
        # TopologyServer is present and the caller didn't already attach their own
        # sink via set_metrics()/metrics= — the dashboard needs SOMETHING to read
        # via /metrics. Must happen before build_component_set() below, which
        # captures self._metrics by value; Runtime(metrics=my_sink) callers are
        # unaffected (self._metrics is already set, this block is a no-op for them).
        if (
            self._metrics is None
            and self._components is None
            and self._root_supervisor is not None
            and any(
                isinstance(a, _TopologyIntrospection) for a in self._root_supervisor.all_agents()
            )
        ):
            self._metrics = MetricsCollector()
            self._metrics.runtime_started()
            # register_agent() is required before message_handled()/message_sent()
            # will record anything for a name (MetricsCollector no-ops for an
            # unregistered agent) — matches what the old CLI dashboard.py did
            # manually. Dynamically-spawned children are NOT covered by this loop
            # (all_agents() only sees statically-declared children) — documented
            # gap, not a spawn-time hook (design dashboard-v2.md addendum).
            for agent in self._root_supervisor.all_agents():
                self._metrics.register_agent(agent.name)
            # v0.9.1 (D-DASH addendum, 2026-07-26): agent_restarted() existed on
            # MetricsSink/MetricsCollector but was NEVER called from anywhere in
            # civitas/ — the exact same class of gap FD-01 was for llm_call()
            # (Phase C). The old CLI dashboard.py was the only caller, wired
            # manually via on_crash(); reproduced here so restart_history and
            # per-agent restart counts populate for ANY TopologyServer-having
            # Runtime, not just the (now-removed, Phase F) standalone CLI path.
            metrics_for_crash_callback = self._metrics

            async def _record_restart_for_dashboard(name: str, exc: Exception) -> None:
                metrics_for_crash_callback.agent_restarted(name, type(exc).__name__)

            self.on_crash(_record_restart_for_dashboard)

        # Steps 2–6: build or use provided ComponentSet.
        # Note: if a pre-built ComponentSet is provided, its transport must support
        # being started by this call — transport.start() is always called below. (F04-11)
        if self._components is not None:
            cs = self._components
        else:
            ts = self._transport_security
            cs = build_component_set(
                transport_type=self._transport_type,
                serializer=self._custom_serializer,
                model_provider=self._model_provider,
                tool_registry=self._tool_registry,
                state_store=self._state_store,
                audit_sink=self._audit_sink,
                metrics=self._metrics,
                exporters=self._exporters,
                zmq_pub_addr=self._zmq_pub_addr,
                zmq_sub_addr=self._zmq_sub_addr,
                zmq_start_proxy=self._zmq_start_proxy,
                zmq_curve_config=ts.zmq if ts is not None and ts.zmq.enabled else None,
                nats_servers=self._nats_servers,
                nats_jetstream=self._nats_jetstream,
                nats_stream_name=self._nats_stream_name,
                nats_tls_config=ts.nats if ts is not None and ts.nats.enabled else None,
            )

        # Expose on self for stop(), ask(), send(), and get_agent()
        self._serializer = cs.serializer
        self._tracer = cs.tracer
        self._transport = cs.transport
        self._registry = cs.registry
        self._bus = cs.bus
        self._state_store = cs.store

        # Drain span_queue via OTELAgent when exporters are configured (FD-07/FD-09)
        if cs.span_queue is not None and cs.export_backend is not None:
            self._otel_agent_task = asyncio.create_task(
                run_otel_agent(cs.span_queue, cs.export_backend)
            )

        if self._root_supervisor is None:
            self._started = True
            return

        # 8. Inject dependencies into all AgentProcesses
        all_agents = self._root_supervisor.all_agents()

        # Security: build signing infrastructure if configured for non-InProcess transports.
        # InProcess transport skips signing entirely (D9 — same OS process, no wire to protect).
        if (
            self._security_config is not None
            and self._security_config.signing.enabled
            and self._transport_type != "in_process"
        ):
            key_dir = self._security_config.identity.key_dir
            identities: dict[str, AgentIdentity] = {}
            for agent in all_agents:
                # A DynamicSupervisor needs an identity to sign the cluster-wide
                # child announcements it publishes (R6 · D8); the topology
                # introspection unit (read-only, never signs) is exempt. v0.9.5:
                # that unit is now a TopologyAgent PLUS its internally-owned
                # HTTPGateway -- exempt the gateway too (identified by its
                # topology_agent config, so a normal user http_gateway is
                # unaffected), so a signed non-auto deployment isn't forced to
                # provision a new key for the auto-created gateway.
                if isinstance(agent, _TopologyIntrospection):
                    continue
                if isinstance(agent, HTTPGateway) and agent._gw_config.topology_agent is not None:
                    continue
                if self._security_config.identity.mode == "auto":
                    identities[agent.name] = AgentIdentity.load_or_generate(agent.name, key_dir)
                else:
                    identities[agent.name] = AgentIdentity.load(agent.name, key_dir)

            registry = KeyRegistry()
            for name, identity in identities.items():
                registry.register(name, identity.verify_key)
            for name, pub_b64 in self._topology_public_keys.items():
                if name not in registry:
                    registry.register_b64(name, pub_b64)

            signer = MessageSigner(identities, registry, self._security_config.signing)
            signing_ser = SigningSerializer(signer, self._security_config.signing)
            self._serializer = signing_ser
            cs.bus._serializer = signing_ser
            # v0.9.2.1 bugfix: the transport holds its OWN private serializer
            # reference (needed for request()'s internal reply_to round-trip)
            # separate from the bus's — without this, ask() over a signing-
            # enabled ZMQ/NATS transport silently corrupted every request into
            # a blank message (empty sender/correlation_id), which made the
            # reply-routing check in AgentProcess._dispatch() no-op with no
            # exception anywhere — just a plain ask() TimeoutError. See
            # Transport.set_serializer's docstring for the full root cause.
            cs.bus._transport.set_serializer(signing_ser)
            self._key_registry = registry
            self._message_signer = signer
            self._signing_on = True

        for agent in all_agents:
            cs.inject(agent)
            # Wire per-agent credentials from topology credentials: blocks
            if self._agent_credentials:
                agent._credentials = self._agent_credentials.get(agent.name, {})

        # Inject into supervisors (supervisor-specific wiring, not via ComponentSet)
        # D1a (v0.9.0): also hand every supervisor a RE-INVOKABLE wiring callback
        # (fresh incarnations must be wired exactly like startup wiring) and a
        # replaced-callback that keeps Runtime's O(1) map + TopologyServer
        # references fresh. User-held object references go stale by design (Q1:
        # route by name, never by object).
        def _wire_child(agent: AgentProcess) -> None:
            cs.inject(agent)
            if self._agent_credentials:
                agent._credentials = self._agent_credentials.get(agent.name, {})

        def _on_child_replaced(name: str, new_agent: AgentProcess) -> None:
            self._agents_by_name[name] = new_agent
            if isinstance(new_agent, _TopologyIntrospection):
                new_agent._root_supervisor = self._root_supervisor
                new_agent._agents = self._agents_by_name
                new_agent._metrics_collector = (
                    self._metrics if isinstance(self._metrics, MetricsCollector) else None
                )

        for sup in self._root_supervisor.all_supervisors():
            sup._bus = cs.bus
            sup._registry = cs.registry
            sup._tracer = cs.tracer
            sup._wire_child = _wire_child
            sup.add_child_replaced_callback(_on_child_replaced)

        # Wire _dynamic_supervisor_name for all agents based on the static topology.
        # Each agent receives the name of the nearest DynamicSupervisor in its
        # ancestor-or-sibling subtree, enabling self.spawn() without explicit naming.
        def _wire_dyn_sup(
            node: Supervisor | AgentProcess,
            nearest_dyn: str | None,
        ) -> None:
            if isinstance(node, DynamicSupervisor):
                node._dynamic_supervisor_name = node.name  # spawns into itself
            elif isinstance(node, Supervisor):
                dyn_child = next(
                    (c for c in node.children if isinstance(c, DynamicSupervisor)), None
                )
                new_nearest = dyn_child.name if dyn_child is not None else nearest_dyn
                for child in node.children:
                    _wire_dyn_sup(child, new_nearest)
            else:
                node._dynamic_supervisor_name = nearest_dyn

        _wire_dyn_sup(self._root_supervisor, None)

        # 9. Register all AgentProcesses in Registry; build O(1) name→process map (F04-10)
        for agent in all_agents:
            yaml_caps = self._agent_capabilities.get(agent.name)
            if yaml_caps is not None:
                caps, meta = yaml_caps
            else:
                caps = list(agent.capabilities)
                meta = dict(agent.capability_metadata)
            if isinstance(agent, DynamicSupervisor) and DYNAMIC_SUPERVISOR_CAPABILITY not in caps:
                caps = [*caps, DYNAMIC_SUPERVISOR_CAPABILITY]
            self._registry.register(agent.name, capabilities=caps, capability_metadata=meta)
        self._agents_by_name = {a.name: a for a in all_agents}

        # Inject topology introspection references before supervision tree starts
        for agent in all_agents:
            if isinstance(agent, _TopologyIntrospection):
                agent._root_supervisor = self._root_supervisor
                agent._agents = self._agents_by_name
                # v0.9.1 (D-DASH-2/D-DASH-4): only a MetricsCollector has the
                # .snapshot the /metrics endpoint reads — a custom MetricsSink
                # (message_handled/message_sent/... only) leaves this None,
                # which /metrics reports explicitly rather than guessing.
                agent._metrics_collector = (
                    self._metrics if isinstance(self._metrics, MetricsCollector) else None
                )

        # 10. Start Transport
        await self._transport.start()

        # Set up transport subscriptions for each agent
        for agent in all_agents:
            await self._bus.setup_agent(agent)

        # Subscribe to cross-process agent announcements from Worker processes.
        # Workers publish _agency.register on startup so this runtime's bus can
        # route messages to remote agents without a shared registry service.
        await self._transport.subscribe("_agency.register", self._on_remote_register)
        await self._transport.subscribe("_agency.deregister", self._on_remote_deregister)

        # H9 (#33): '_runtime' sink. Runtime-initiated messages carry
        # sender="_runtime", which is not an agent — an agent doing the natural
        # `self.send(message.sender, ...)` used to crash on MessageRoutingError.
        # The sink converts that into a logged drop (and a fail-fast error reply
        # for ask()). Not an AgentProcess: bare subscription, no mailbox, no
        # lifecycle, nothing to supervise. Glob broadcasts never reach it —
        # underscore names are excluded from patterns (registry C6 rule).
        self._registry.register("_runtime")
        await self._transport.subscribe("_runtime", self._on_runtime_addressed)

        # Wait for subscriptions to propagate (ZMQ slow joiner mitigation)
        if hasattr(self._transport, "wait_ready"):
            await self._transport.wait_ready()

        # Connect MCP servers declared in topology YAML to all agents
        if self._mcp_configs:
            for agent in all_agents:
                for mcp_cfg in self._mcp_configs:
                    try:
                        await agent.connect_mcp(mcp_cfg)
                    except Exception as exc:
                        logger.warning(
                            "Failed to connect agent '%s' to MCP server '%s': %s",
                            agent.name,
                            mcp_cfg.name,
                            exc,
                        )

        # D6 (v0.9.0 E4 Phase A): supervisors are now addressable actors too
        # (design supervision-endgame.md §6) — register + wire their transport
        # subscription before the tree starts, exactly like agents. Own-loop-
        # first ordering (D-E4-3) is enforced inside Supervisor.start() itself.
        all_supervisors = self._root_supervisor.all_supervisors()
        for sup in all_supervisors:
            self._registry.register(sup.name, capabilities=[SUPERVISOR_CAPABILITY])
            await self._bus.setup_agent(sup)

        # 11-12. Start supervision tree (supervisors start their children)
        await self._root_supervisor.start()

        # 13. Runtime is ready
        self._started = True

    async def stop(self) -> None:
        """Shutdown sequence: stop agents, transport, flush tracer."""
        if not self._started:
            return

        # 1. Stop supervision tree (sends shutdown, awaits on_stop for each agent)
        if self._root_supervisor is not None:
            await self._root_supervisor.stop()

        # 2. Stop Transport
        if self._transport is not None:
            await self._transport.stop()

        # 3. Close StateStore
        if self._state_store is not None and hasattr(self._state_store, "close"):
            await self._state_store.close()

        # 4. Flush Tracer
        if self._tracer is not None:
            self._tracer.flush()

        # 5. Stop OTELAgent — cancel triggers its own drain-remaining-spans logic
        if self._otel_agent_task is not None:
            self._otel_agent_task.cancel()
            await asyncio.gather(self._otel_agent_task, return_exceptions=True)
            self._otel_agent_task = None

        # 6. Close Audit sink
        if self._audit_sink is not None:
            await self._audit_sink.close()

        if self._registry is not None:
            self._registry.deregister("_runtime")
        self._agents_by_name.clear()
        self._started = False

    # ------------------------------------------------------------------
    # Cross-process discovery — remote register/deregister handlers (R6)
    # ------------------------------------------------------------------

    async def _on_remote_register(self, data: bytes) -> None:
        """Verify and apply an ``_agency.register`` announcement (R6 · D8/D9/D13).

        When signing is on the announcement is verified during deserialization
        against the trusted keyset — an unsigned or unknown-signer announcement
        raises and is dropped. The authenticated ``sender`` is the owning
        supervisor; the registry rejects name takeover by a different owner and
        stale/reordered epochs. A verified child public key is recorded so this
        process can later verify the child's own signed messages.
        """
        if self._serializer is None or self._registry is None:
            return
        try:
            msg = self._serializer.deserialize(data)
        except SignatureError:
            logger.warning("Dropping _agency.register with missing/invalid signature")
            return
        except DeserializationError:
            logger.warning("Dropping malformed _agency.register announcement")
            return

        name = msg.payload.get("name", "")
        if not name:
            return
        pubkey = str(msg.payload.get("pubkey", "") or "")
        epoch = int(msg.payload.get("epoch", 0) or 0)
        try:
            self._registry.register_remote(
                name,
                capabilities=msg.payload.get("capabilities"),
                capability_metadata=msg.payload.get("capability_metadata"),
                owner=msg.sender,
                pubkey=pubkey,
                epoch=epoch,
                health_channel=str(msg.payload.get("health_channel", "") or ""),
            )
        except ValueError as exc:
            logger.warning("Rejecting remote registration for %r: %s", name, exc)
            return

        if pubkey and self._key_registry is not None:
            try:
                self._key_registry.register_b64(name, pubkey)
            except (ValueError, TypeError):
                logger.warning("Ignoring malformed announced public key for %r", name)

    async def _on_runtime_addressed(self, data: bytes) -> None:
        """Sink for messages addressed to '_runtime' (H9, #33).

        WARNING-logs every occurrence (it is always a code smell — data meant
        for the caller must travel via reply()/ask), and answers request-reply
        messages with an error reply so ``ask("_runtime", ...)`` fails fast with
        a reason instead of timing out.
        """
        if self._serializer is None:
            return
        try:
            msg = self._serializer.deserialize(data)
        except (DeserializationError, SignatureError):
            return
        logger.warning(
            "Message %r from %r was addressed to '_runtime' and dropped — "
            "Runtime-initiated messages have no routable sender; use reply() / "
            "the ask() reply path to return data to the caller.",
            msg.type,
            msg.sender or "<unknown>",
        )
        if msg.correlation_id and (msg.reply_to or msg.sender) and self._bus is not None:
            error_reply = Message(
                type="reply",
                sender="_runtime",
                recipient=msg.reply_to or msg.sender,
                payload={
                    "status": "error",
                    "error": "'_runtime' is not an agent — Runtime-initiated messages "
                    "have no routable sender; use reply()/ask to return data to the caller",
                },
                correlation_id=msg.correlation_id,
                trace_id=msg.trace_id,
                span_id=_new_span_id(),
                parent_span_id=msg.span_id,
            )
            try:
                await self._bus.route(error_reply)
            except MessageRoutingError:
                logger.debug("error reply from '_runtime' sink undeliverable")

    async def _on_remote_deregister(self, data: bytes) -> None:
        """Verify and apply an ``_agency.deregister`` announcement (R6 · D13)."""
        if self._serializer is None or self._registry is None:
            return
        try:
            msg = self._serializer.deserialize(data)
        except SignatureError:
            logger.warning("Dropping _agency.deregister with missing/invalid signature")
            return
        except DeserializationError:
            return

        name = msg.payload.get("name", "")
        if not name:
            return
        epoch = int(msg.payload.get("epoch", 0) or 0)
        self._registry.deregister_remote(name, epoch=epoch)

    # ------------------------------------------------------------------
    # Public API — process lookup, send, and ask
    # ------------------------------------------------------------------

    def get_agent(self, name: str) -> AgentProcess | None:
        """Return the live AgentProcess instance by name, or None.

        O(1) lookup via the agents-by-name dict built during start().
        Use this when you need to inspect process state (e.g. status).
        For routing messages use runtime.send/ask instead.
        """
        return self._agents_by_name.get(name)

    async def call(
        self,
        agent_name: str,
        payload: dict[str, Any],
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Synchronous call to a GenServer. Blocks until reply or timeout."""
        reply = await self.ask(agent_name, payload, timeout=timeout)
        return reply.payload

    async def cast(self, agent_name: str, payload: dict[str, Any]) -> None:
        """Fire-and-forget cast to a GenServer. Returns immediately."""
        await self.send(agent_name, {**payload, "__cast__": True})

    async def ask(
        self,
        agent_name: str,
        payload: dict[str, Any],
        timeout: float | None = 30.0,
        message_type: str = "message",
        *,
        fail_if_suspended: bool = False,
    ) -> Message:
        """Send a message to an agent and await a reply.

        ``timeout`` (v0.10.0): ``None``/``-1``/any value ``<= 0`` waits
        indefinitely — the HITL case (ask an agent that suspends for approval;
        the reply arrives hours/days later on resume). See ``AgentProcess.ask``.

        ``fail_if_suspended`` (v0.10.0, D2): raise ``AgentSuspendedError``
        immediately instead of waiting, if the target is suspended.
        """
        if self._bus is None or self._tracer is None:
            raise RuntimeError("Runtime not started")

        trace_id = self._tracer.new_trace_id()
        message = Message(
            type=message_type,
            sender="_runtime",
            recipient=agent_name,
            payload=payload,
            correlation_id=_uuid7(),
            trace_id=trace_id,
            span_id=_new_span_id(),
        )
        return cast(
            Message,
            await self._bus.request(message, timeout=timeout, fail_if_suspended=fail_if_suspended),
        )

    async def send(
        self,
        agent_name: str,
        payload: dict[str, Any],
        message_type: str = "message",
    ) -> None:
        """Fire-and-forget: send a message to an agent."""
        if self._bus is None or self._tracer is None:
            raise RuntimeError("Runtime not started")

        trace_id = self._tracer.new_trace_id()
        message = Message(
            type=message_type,
            sender="_runtime",
            recipient=agent_name,
            payload=payload,
            trace_id=trace_id,
            span_id=_new_span_id(),
        )
        await self._bus.route(message)

    # ------------------------------------------------------------------
    # Dynamic spawning — external entry points for non-agent callers
    # ------------------------------------------------------------------

    async def spawn(
        self,
        supervisor_name: str,
        agent_class: type[AgentProcess],
        name: str,
        config: dict[str, Any] | None = None,
        *,
        wait: bool = True,
    ) -> str:
        """Spawn a dynamic agent via the named DynamicSupervisor.

        Returns the agent name on success. Raises SpawnError on failure.

        With ``wait=False`` the call returns as soon as the child's task exists,
        before ``on_start()`` completes; a later start failure is delivered to the
        spawner via ``on_child_terminated`` (R1 · D2).
        """
        class_path = f"{agent_class.__module__}.{agent_class.__qualname__}"
        reply = await self.ask(
            supervisor_name,
            {
                "class_path": class_path,
                "name": name,
                "config": config or {},
                "spawner": "_runtime",
                "wait": wait,
                "spawn_id": _uuid7(),
            },
            message_type="civitas.dynamic.spawn",
        )
        if reply.payload.get("status") != "ok":
            reason = reply.payload.get("reason") or reply.payload.get("error") or "spawn failed"
            raise SpawnError(reason)
        return name

    async def spawn_nowait(
        self,
        supervisor_name: str,
        agent_class: type[AgentProcess],
        name: str,
        config: dict[str, Any] | None = None,
    ) -> str:
        """Spawn a dynamic agent without blocking on its ``on_start()`` (R1 · D2).

        Alias for ``spawn(..., wait=False)``.
        """
        return await self.spawn(supervisor_name, agent_class, name, config, wait=False)

    async def despawn(self, supervisor_name: str, name: str) -> None:
        """Hard-stop a dynamic child via the named DynamicSupervisor."""
        reply = await self.ask(
            supervisor_name,
            {"name": name},
            message_type="civitas.dynamic.despawn",
        )
        if reply.payload.get("status") != "ok":
            raise SpawnError(reply.payload.get("reason", "despawn failed"))

    async def stop_agent(
        self,
        supervisor_name: str,
        name: str,
        drain: str = "current",
        timeout: float = 30.0,
    ) -> None:
        """Soft-stop a dynamic child via the named DynamicSupervisor."""
        reply = await self.ask(
            supervisor_name,
            {"name": name, "drain": drain, "timeout": timeout},
            message_type="civitas.dynamic.stop",
            timeout=timeout + 5.0,
        )
        if reply.payload.get("status") != "ok":
            raise SpawnError(reply.payload.get("reason", "stop failed"))

    # ------------------------------------------------------------------
    # Durable suspension — external entry points (Presidium HITL)
    # ------------------------------------------------------------------

    async def suspend(
        self, name: str, reason: str = "", category: SuspendCategory = SuspendCategory.OTHER
    ) -> None:
        """Suspend an agent by name (S10).

        Delivers a priority ``_agency.suspend`` control message; the agent
        transitions to SUSPENDED at its next loop boundary (non-blocking).
        Suspending an already-suspended agent keeps its original ``since`` and
        updates the reason. ``ask()`` into a suspended agent times out — that is
        intended; use ``send()`` plus polling for long approvals.

        ``category`` (v0.9.4) is the same additive, backward-compatible
        parameter as ``AgentProcess.suspend()`` — this is the cross-process/
        by-name entry point, so it needs its own copy of the same parameter;
        without it, only a same-process direct ``agent.suspend_for_approval()``
        call could ever categorize a suspend.
        """
        if self._bus is None or self._tracer is None:
            raise RuntimeError("Runtime not started")
        message = Message(
            type="_agency.suspend",
            sender="_runtime",
            recipient=name,
            payload={"reason": reason, "category": category.value},
            trace_id=self._tracer.new_trace_id(),
            span_id=_new_span_id(),
            priority=1,
        )
        await self._bus.route(message)

    async def resume(self, name: str, approver: str) -> None:
        """Resume a suspended agent by name (S6/S10). Requires a non-empty approver.

        Delivers a priority ``_agency.resume`` control message carrying the
        approver identity. Resuming a not-suspended agent is a safe no-op, but
        an approver is still required. The approver is recorded in the resume
        AuditEvent.

        Raises:
            ValueError: if ``approver`` is empty — a checkpointed pending_action
                is never authorization on its own; a named approver is required.
        """
        if not approver:
            raise ValueError("resume() requires a non-empty approver")
        if self._bus is None or self._tracer is None:
            raise RuntimeError("Runtime not started")
        message = Message(
            type="_agency.resume",
            sender="_runtime",
            recipient=name,
            payload={"approver": approver},
            trace_id=self._tracer.new_trace_id(),
            span_id=_new_span_id(),
            priority=1,
        )
        await self._bus.route(message)

    async def restart_agent(self, name: str, reason: str = "", initiated_by: str = "") -> None:
        """Force-restart (kill) an agent by name (v0.9.6, control-plane-writes.md §6).

        Delivers a priority ``_agency.force_restart`` control message; the agent
        raises out of its task and its supervisor restarts it per the SAME
        policy any crash follows (transient/permanent/restart-budget all
        honored) -- the OTP-idiomatic 'let it crash', not a bespoke restart
        path. ``initiated_by`` (the authenticated control-plane actor) is
        recorded in the agent.force_restart AuditEvent. Not supported for
        Supervisors (a subtree-wide restart is deferred as too blunt).
        """
        if self._bus is None or self._tracer is None:
            raise RuntimeError("Runtime not started")
        message = Message(
            type="_agency.force_restart",
            sender="_runtime",
            recipient=name,
            payload={"reason": reason, "initiated_by": initiated_by},
            trace_id=self._tracer.new_trace_id(),
            span_id=_new_span_id(),
            priority=1,
        )
        await self._bus.route(message)
