"""Civitas — The Production Runtime for Python Agents.

Public API:
    AgentProcess      — subclass to create agent processes
    GenServer         — OTP-style generic server for stateful service processes
    Supervisor        — monitors children, applies restart strategies
    DynamicSupervisor — runtime child spawning with capacity limits
    Runtime           — wires components, manages lifecycle
    Worker            — hosts agents in a remote worker process
    ComponentSet      — assembled infrastructure wiring (transport, bus, registry, tracer)
    StateStore        — protocol for agent state persistence backends
    EncryptingStateStore — wraps any StateStore to encrypt values at rest (civitas[encryption])
    Message           — standard message envelope
    CivitasError      — base exception
    ErrorAction       — enum: RETRY, SKIP, ESCALATE, STOP
    SignatureError    — raised on missing, invalid, or replayed message signatures
    CapabilityNotFoundError — raised when no agent declares the requested capability
    SecurityConfig    — security block parsed from topology YAML
    SecretsProvider   — protocol for secret resolution
    substitute_vars   — resolve ${VAR} patterns in YAML config dicts
    SandboxConfig     — per-MCP-server sandbox configuration
    FilesystemMount   — bind-mount entry for SandboxConfig
    AuditEvent        — structured audit record TypedDict
    AuditSink         — protocol for audit sinks
    NullSink          — no-op sink for tests / disabled auditing
    JsonlFileSink     — append-only JSONL file sink with batching and SIGHUP rotation
    SyslogSink        — emit audit events to syslog
    OtlpSink          — emit audit events as OTEL log records (requires civitas[otel])
    GatewayConfig     — HTTP gateway configuration
    HTTPGateway       — supervised ASGI edge process
    GatewayRequest    — request envelope passed to middleware handlers
    GatewayResponse   — response envelope returned from middleware handlers
    NextMiddleware    — type alias for the next() callable in a middleware chain
    RoutingEntry      — registry entry with name, address, capabilities
    RegistryListener  — async callable notified on register/deregister events
"""

from __future__ import annotations

from civitas.audit import AuditEvent, AuditSink, JsonlFileSink, NullSink, OtlpSink, SyslogSink
from civitas.components import ComponentSet
from civitas.errors import (
    CapabilityNotFoundError,
    CivitasError,
    ErrorAction,
    SignatureError,
    SlowConsumerError,
    SpawnError,
    StreamError,
    StreamInterrupted,
    StreamTimeout,
)
from civitas.evalloop import CorrectionSignal, EvalAgent, EvalEvent, EvalExporter
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware
from civitas.genserver import GenServer
from civitas.messages import Message
from civitas.plugins.encrypted_store import EncryptingStateStore
from civitas.plugins.state import StateStore
from civitas.process import AgentProcess
from civitas.registry import RegistryListener, RoutingEntry
from civitas.runtime import Runtime
from civitas.sandbox import FilesystemMount, SandboxConfig
from civitas.secrets import SecretsProvider, substitute_vars
from civitas.security.config import SecurityConfig
from civitas.supervisor import DynamicSupervisor, Supervisor
from civitas.topology_server import TopologyAgent
from civitas.worker import Worker

__all__ = [
    "AgentProcess",
    "AuditEvent",
    "AuditSink",
    "CapabilityNotFoundError",
    "ComponentSet",
    "CivitasError",
    "CorrectionSignal",
    "DynamicSupervisor",
    "EncryptingStateStore",
    "ErrorAction",
    "EvalAgent",
    "EvalEvent",
    "EvalExporter",
    "FilesystemMount",
    "GatewayConfig",
    "GatewayRequest",
    "GatewayResponse",
    "GenServer",
    "HTTPGateway",
    "JsonlFileSink",
    "Message",
    "NextMiddleware",
    "NullSink",
    "OtlpSink",
    "RegistryListener",
    "RoutingEntry",
    "Runtime",
    "SandboxConfig",
    "SecurityConfig",
    "SecretsProvider",
    "SignatureError",
    "SlowConsumerError",
    "SpawnError",
    "StateStore",
    "StreamError",
    "StreamInterrupted",
    "StreamTimeout",
    "Supervisor",
    "SyslogSink",
    "TopologyAgent",
    "Worker",
    "substitute_vars",
]
