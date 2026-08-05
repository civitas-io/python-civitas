"""Unit tests for civitas.plugins.loader — entrypoint resolution, constructor errors."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from civitas.plugins.loader import (
    PluginError,
    load_plugin,
    load_plugins_from_config,
    resolve_plugin_class,
)
from civitas.plugins.state import InMemoryStateStore

# ---------------------------------------------------------------------------
# Entrypoint resolution (lines 71-76 in loader.py)
# ---------------------------------------------------------------------------


def test_resolve_via_entrypoint() -> None:
    """When an installed entrypoint matches the requested name, it is loaded."""

    class _FakeProvider:
        pass

    fake_ep = MagicMock()
    fake_ep.name = "myprovider"
    fake_ep.load.return_value = _FakeProvider

    with patch("civitas.plugins.loader.entry_points", return_value=[fake_ep]):
        cls = resolve_plugin_class("model", "myprovider")

    assert cls is _FakeProvider
    fake_ep.load.assert_called_once()


def test_resolve_entrypoint_load_error_raises_plugin_error() -> None:
    """If the entrypoint's load() raises, a PluginError is produced."""

    fake_ep = MagicMock()
    fake_ep.name = "broken"
    fake_ep.load.side_effect = ImportError("missing dep")

    with patch("civitas.plugins.loader.entry_points", return_value=[fake_ep]):
        with pytest.raises(PluginError, match="missing dep"):
            resolve_plugin_class("model", "broken")


def test_resolve_entrypoint_name_mismatch_falls_through() -> None:
    """An entrypoint whose name doesn't match is skipped; resolution continues."""
    # Provide an entrypoint that does NOT match, then let the built-in mapping handle it
    wrong_ep = MagicMock()
    wrong_ep.name = "other_provider"  # != "in_memory"

    with patch("civitas.plugins.loader.entry_points", return_value=[wrong_ep]):
        cls = resolve_plugin_class("state", "in_memory")

    assert cls is InMemoryStateStore
    wrong_ep.load.assert_not_called()


def test_resolve_sqlite_span_store_exporter_is_core() -> None:
    """v0.11.1 (B4): SpanStores are usable as declarative exporters; the sqlite
    one resolves to the core SQLiteSpanStore."""
    cls = resolve_plugin_class("exporter", "sqlite")
    assert cls.__module__ == "civitas.observability.sqlite_backend"
    assert cls.__name__ == "SQLiteSpanStore"


def test_driver_backed_stores_map_to_contrib_paths() -> None:
    """v0.11.1: postgres/mysql span-store exporters and the mysql state store
    resolve to civitas-contrib (lazily -- ImportError only if used without it)."""
    from civitas.plugins.loader import _BUILTINS

    assert _BUILTINS["exporter"]["postgres"].startswith("civitas_contrib.")
    assert _BUILTINS["exporter"]["mysql"].startswith("civitas_contrib.")
    assert _BUILTINS["state"]["mysql"] == "civitas_contrib.plugins.mysql_store.MySQLStateStore"


def test_resolve_sqlite_state_store_is_core_not_contrib() -> None:
    """v0.11.0 (B4): SQLiteStateStore moved to core, so `type: sqlite` resolves
    to the core module and works WITHOUT civitas-contrib installed."""
    cls = resolve_plugin_class("state", "sqlite")
    assert cls.__module__ == "civitas.plugins.sqlite_store"
    assert cls.__name__ == "SQLiteStateStore"


def test_import_dotted_no_module_part_raises() -> None:
    """A dotted path with an empty module part (e.g. '.MyClass') raises PluginError."""
    # "." in ".MyClass" is True, so resolve_plugin_class will call _import_dotted,
    # which then finds module_path="" and raises PluginError at line 177.
    with pytest.raises(PluginError, match="Invalid dotted path"):
        resolve_plugin_class("model", ".MyClass")


# ---------------------------------------------------------------------------
# Constructor TypeError → PluginError (line 108-111 in loader.py)
# ---------------------------------------------------------------------------


def test_load_plugin_constructor_type_error() -> None:
    """load_plugin wraps a constructor TypeError in a PluginError."""
    # in_memory takes no config kwargs — passing an unexpected kwarg triggers TypeError
    with pytest.raises(PluginError, match="Constructor error"):
        load_plugin("state", "in_memory", {"totally_invalid_kwarg": True})


# ---------------------------------------------------------------------------
# V4 (#42) — top-ups for the previously-unmeasured error paths
# ---------------------------------------------------------------------------


def test_unknown_plugin_name_raises_with_hint() -> None:
    """A bare name not in entrypoints/builtins/dotted form fails with guidance."""
    with pytest.raises(PluginError, match="Unknown plugin 'no_such_plugin'"):
        resolve_plugin_class("model", "no_such_plugin")


def test_dotted_path_missing_attribute_raises() -> None:
    """A valid module without the named class is a loud PluginError."""
    with pytest.raises(PluginError, match="has no attribute 'NoSuchClass'"):
        resolve_plugin_class("state", "civitas.plugins.state.NoSuchClass")


def test_dotted_path_unimportable_module_raises() -> None:
    with pytest.raises(PluginError, match="Cannot import module"):
        resolve_plugin_class("state", "civitas.no_such_module.Thing")


def test_config_models_entry_missing_type_raises() -> None:
    with pytest.raises(PluginError, match="missing a 'type' field"):
        load_plugins_from_config({"plugins": {"models": [{"config": {}}]}})


def test_config_exporters_entry_missing_type_raises() -> None:
    with pytest.raises(PluginError, match="missing a 'type' field"):
        load_plugins_from_config({"plugins": {"exporters": [{"config": {}}]}})


def test_config_state_entry_missing_type_defaults_to_in_memory() -> None:
    """Deliberate asymmetry vs models/exporters: a state entry without 'type'
    falls back to the in-memory store rather than raising."""
    loaded = load_plugins_from_config({"plugins": {"state": {"config": {}}}})
    assert isinstance(loaded["state_store"], InMemoryStateStore)


def test_config_without_plugins_section_returns_empty() -> None:
    loaded = load_plugins_from_config({})
    assert loaded == {"model_providers": [], "exporters": [], "state_store": None}
