"""Plugin loader — discovers and instantiates plugins from configuration.

Plugins are resolved in order:
1. Python entrypoints (pip-installed packages register via entry_points)
2. Dotted import paths (e.g. "myapp.plugins.MyProvider")
3. Built-in plugins (e.g. "console", "in_memory")

Entrypoint groups:
    civitas.model     — ModelProvider implementations
    civitas.exporter  — Tracer/exporter implementations
    civitas.state     — StateStore implementations
    civitas.transport — Transport implementations
"""

from __future__ import annotations

import importlib
import logging
from importlib.metadata import entry_points
from typing import Any, cast

from civitas.config import load_state_key

# PluginError lives in civitas.errors (CivitasError subclass) — re-exported here
# for backward compatibility so existing imports from this module still work.
from civitas.errors import PluginError
from civitas.plugins.encrypted_store import EncryptingStateStore
from civitas.plugins.state import InMemoryStateStore
from civitas.security.config import StateEncryptionConfig

__all__ = ["PluginError", "resolve_plugin_class", "load_plugin", "load_plugins_from_config"]

logger = logging.getLogger(__name__)


# Built-in plugin mappings (name → dotted import path)
_BUILTINS: dict[str, dict[str, str]] = {
    "model": {
        "anthropic": "civitas_contrib.plugins.anthropic.AnthropicProvider",
        "openai": "civitas_contrib.plugins.openai.OpenAIProvider",
        "gemini": "civitas_contrib.plugins.gemini.GeminiProvider",
        "mistral": "civitas_contrib.plugins.mistral.MistralProvider",
        "litellm": "civitas_contrib.plugins.litellm.LiteLLMProvider",
    },
    "exporter": {
        "console": "civitas.observability.export_backend.ConsoleBackend",
    },
    "state": {
        "in_memory": "civitas.plugins.state.InMemoryStateStore",
        "sqlite": "civitas_contrib.plugins.sqlite_store.SQLiteStateStore",
        "postgres": "civitas_contrib.plugins.postgres_store.PostgresStateStore",
        "encrypted": "civitas.plugins.encrypted_store.EncryptingStateStore",
    },
    "transport": {
        "in_process": "civitas.transport.inprocess.InProcessTransport",
        "zmq": "civitas.transport.zmq.ZMQTransport",
        "nats": "civitas.transport.nats.NATSTransport",
    },
}

# Entrypoint group names
_ENTRYPOINT_GROUPS: dict[str, str] = {
    "model": "civitas.model",
    "exporter": "civitas.exporter",
    "state": "civitas.state",
    "transport": "civitas.transport",
}


def resolve_plugin_class(plugin_type: str, name: str) -> type[Any]:
    """Resolve a plugin name to a Python class.

    Resolution order:
    1. Python entrypoints (civitas.model, civitas.exporter, etc.)
    2. Built-in name mapping
    3. Dotted import path (e.g. "myapp.plugins.MyProvider")
    """
    # 1. Try entrypoints
    group = _ENTRYPOINT_GROUPS.get(plugin_type, f"civitas.{plugin_type}")
    eps = entry_points(group=group)
    for ep in eps:
        if ep.name == name:
            try:
                return cast(type[Any], ep.load())
            except Exception as exc:
                raise PluginError(plugin_type, name, str(exc)) from exc

    # 2. Try built-in mapping
    builtins = _BUILTINS.get(plugin_type, {})
    if name in builtins:
        dotted_path = builtins[name]
        return _import_dotted(plugin_type, name, dotted_path)

    # 3. Try as a dotted import path
    if "." in name:
        return _import_dotted(plugin_type, name, name)

    raise PluginError(
        plugin_type,
        name,
        f"Unknown plugin '{name}'. Not found in entrypoints, built-ins, "
        f"or as a dotted import path.",
    )


def load_plugin(plugin_type: str, name: str, config: dict[str, Any] | None = None) -> Any:
    """Load and instantiate a plugin.

    Args:
        plugin_type: Category (model, exporter, state, transport).
        name: Plugin name or dotted import path.
        config: Optional kwargs passed to the plugin constructor.
    """
    cls = resolve_plugin_class(plugin_type, name)
    kwargs = config or {}
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise PluginError(
            plugin_type, name, f"Constructor error: {exc}. Check the plugin config."
        ) from exc


def load_plugins_from_config(config: dict[str, Any]) -> dict[str, Any]:
    """Load all plugins from a YAML configuration dict.

    Expected YAML structure:
        plugins:
          models:
            - type: anthropic
              config:
                api_key: "sk-..."
                default_model: "claude-sonnet-4-20250514"
          exporters:
            - type: console
            - type: otel
              config:
                endpoint: "http://localhost:4317"
          state:
            type: in_memory

    Returns a dict with keys: model_providers, exporters, state_store.
    """
    plugins_cfg = config.get("plugins", {})
    result: dict[str, Any] = {
        "model_providers": [],
        "exporters": [],
        "state_store": None,
    }

    # Models
    for model_cfg in plugins_cfg.get("models", []):
        name = model_cfg.get("type")
        if not name:
            raise PluginError(
                "model", "<missing>", "Plugin config entry is missing a 'type' field."
            )
        plugin_config = model_cfg.get("config", {})
        provider = load_plugin("model", name, plugin_config)
        result["model_providers"].append(provider)

    # Exporters
    for exp_cfg in plugins_cfg.get("exporters", []):
        name = exp_cfg.get("type")
        if not name:
            raise PluginError(
                "exporter", "<missing>", "Plugin config entry is missing a 'type' field."
            )
        plugin_config = exp_cfg.get("config", {})
        exporter = load_plugin("exporter", name, plugin_config)
        result["exporters"].append(exporter)

    # State store
    state_cfg = plugins_cfg.get("state")
    if state_cfg is not None:
        name = state_cfg.get("type", "in_memory")
        plugin_config = state_cfg.get("config", {})
        if name == "encrypted":
            result["state_store"] = _build_encrypted_store(plugin_config)
        else:
            result["state_store"] = load_plugin("state", name, plugin_config)

    return result


def _build_encrypted_store(plugin_config: dict[str, Any]) -> Any:
    """Build an EncryptingStateStore that wraps a nested inner state store.

    The nested ``store`` block is instantiated through the normal loader path,
    then wrapped. Key bytes are resolved from the environment at build time and
    a missing key fails loud (ConfigurationError).
    """
    inner_cfg = plugin_config.get("store")
    if not isinstance(inner_cfg, dict) or "type" not in inner_cfg:
        raise PluginError(
            "state",
            "encrypted",
            "Encrypted state store requires a nested 'store' config with a 'type'.",
        )
    inner = load_plugin("state", inner_cfg["type"], inner_cfg.get("config", {}))

    enc_cfg = StateEncryptionConfig.from_dict(plugin_config)
    keys = {enc_cfg.current_key_id: load_state_key(enc_cfg.key_env)}
    for key_id, env_name in enc_cfg.keys.items():
        keys[key_id] = load_state_key(env_name)

    if isinstance(inner, InMemoryStateStore):
        logger.warning(
            "Encrypting an InMemoryStateStore gives no at-rest protection — "
            "state lives only in memory."
        )

    return EncryptingStateStore(
        inner,
        keys=keys,
        current_key_id=enc_cfg.current_key_id,
        allow_plaintext_read=enc_cfg.allow_plaintext_read,
    )


def _import_dotted(plugin_type: str, name: str, dotted_path: str) -> type[Any]:
    """Import a class from a dotted module path like 'myapp.plugins.MyClass'."""
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise PluginError(
            plugin_type,
            name,
            f"Invalid dotted path '{dotted_path}'. Expected format: 'module.path.ClassName'.",
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise PluginError(
            plugin_type, name, f"Cannot import module '{module_path}': {exc}"
        ) from exc

    cls = getattr(module, class_name, None)
    if cls is None:
        raise PluginError(
            plugin_type, name, f"Module '{module_path}' has no attribute '{class_name}'."
        )
    return cast(type[Any], cls)
