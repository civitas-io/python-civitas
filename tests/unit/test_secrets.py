"""Unit tests for M4.2c — Credential Isolation.

Covers: substitute_vars, EnvSecretsProvider, FileSecretsProvider,
_extract_agent_credentials, AgentProcess.get_credential, AgentProcess.model_for.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from civitas.errors import ConfigurationError
from civitas.messages import Message
from civitas.secrets.providers import EnvSecretsProvider, FileSecretsProvider, SecretsProvider
from civitas.secrets.substitution import substitute_vars

# ---------------------------------------------------------------------------
# substitute_vars
# ---------------------------------------------------------------------------


class TestSubstituteVars:
    def test_simple_string(self):
        result = substitute_vars("${MY_VAR}", env={"MY_VAR": "hello"})
        assert result == "hello"

    def test_embedded_string(self):
        result = substitute_vars("prefix_${X}_suffix", env={"X": "mid"})
        assert result == "prefix_mid_suffix"

    def test_multiple_vars_in_one_string(self):
        result = substitute_vars("${A}+${B}", env={"A": "foo", "B": "bar"})
        assert result == "foo+bar"

    def test_missing_var_raises(self):
        with pytest.raises(ConfigurationError, match="MY_MISSING"):
            substitute_vars("${MY_MISSING}", env={})

    def test_dict_values_resolved(self):
        obj = {"key": "${VAL}"}
        assert substitute_vars(obj, env={"VAL": "42"}) == {"key": "42"}

    def test_dict_keys_not_substituted(self):
        obj = {"${KEY}": "value"}
        result = substitute_vars(obj, env={"KEY": "k"})
        assert "${KEY}" in result  # keys are passed through unchanged

    def test_list_items_resolved(self):
        obj = ["${A}", "${B}"]
        assert substitute_vars(obj, env={"A": "x", "B": "y"}) == ["x", "y"]

    def test_nested_dict_list(self):
        obj = {"outer": [{"inner": "${Z}"}]}
        assert substitute_vars(obj, env={"Z": "zed"}) == {"outer": [{"inner": "zed"}]}

    def test_int_passthrough(self):
        assert substitute_vars(42, env={}) == 42

    def test_float_passthrough(self):
        assert substitute_vars(3.14, env={}) == 3.14

    def test_bool_passthrough(self):
        assert substitute_vars(True, env={}) is True

    def test_none_passthrough(self):
        assert substitute_vars(None, env={}) is None

    def test_no_vars_string_unchanged(self):
        assert substitute_vars("plain string", env={}) == "plain string"

    def test_uses_os_environ_by_default(self, monkeypatch):
        monkeypatch.setenv("CIVITAS_TEST_VAR_XYZ", "from_env")
        result = substitute_vars("${CIVITAS_TEST_VAR_XYZ}")
        assert result == "from_env"

    def test_custom_env_overrides_os_environ(self, monkeypatch):
        monkeypatch.setenv("CIVITAS_TEST_VAR_XYZ", "os_value")
        result = substitute_vars("${CIVITAS_TEST_VAR_XYZ}", env={"CIVITAS_TEST_VAR_XYZ": "custom"})
        assert result == "custom"


# ---------------------------------------------------------------------------
# EnvSecretsProvider
# ---------------------------------------------------------------------------


class TestEnvSecretsProvider:
    def test_get_present(self, monkeypatch):
        monkeypatch.setenv("TEST_SECRET_KEY", "s3cr3t")
        p = EnvSecretsProvider()
        assert p.get("TEST_SECRET_KEY") == "s3cr3t"

    def test_get_missing_returns_none(self, monkeypatch):
        monkeypatch.delenv("TEST_SECRET_KEY_MISSING", raising=False)
        p = EnvSecretsProvider()
        assert p.get("TEST_SECRET_KEY_MISSING") is None

    def test_require_present(self, monkeypatch):
        monkeypatch.setenv("TEST_REQUIRED_KEY", "value123")
        p = EnvSecretsProvider()
        assert p.require("TEST_REQUIRED_KEY") == "value123"

    def test_require_missing_raises(self, monkeypatch):
        monkeypatch.delenv("TEST_MISSING_REQUIRED", raising=False)
        p = EnvSecretsProvider()
        with pytest.raises(ConfigurationError, match="TEST_MISSING_REQUIRED"):
            p.require("TEST_MISSING_REQUIRED")

    def test_implements_protocol(self):
        p = EnvSecretsProvider()
        assert isinstance(p, SecretsProvider)


# ---------------------------------------------------------------------------
# FileSecretsProvider
# ---------------------------------------------------------------------------


class TestFileSecretsProvider:
    def _write_secrets(self, tmp_path: Path, content: str) -> Path:
        f = tmp_path / "secrets.env"
        f.write_text(textwrap.dedent(content))
        return f

    def test_loads_key_value_pairs(self, tmp_path):
        path = self._write_secrets(
            tmp_path,
            """\
            ANTHROPIC_API_KEY=sk-ant-abc123
            OPENAI_API_KEY=sk-openai-xyz
        """,
        )
        p = FileSecretsProvider(path)
        assert p.get("ANTHROPIC_API_KEY") == "sk-ant-abc123"
        assert p.get("OPENAI_API_KEY") == "sk-openai-xyz"

    def test_ignores_comment_lines(self, tmp_path):
        path = self._write_secrets(
            tmp_path,
            """\
            # This is a comment
            KEY=value
        """,
        )
        p = FileSecretsProvider(path)
        assert p.get("KEY") == "value"
        assert p.get("# This is a comment") is None

    def test_ignores_blank_lines(self, tmp_path):
        path = self._write_secrets(
            tmp_path,
            """\

            KEY=val

        """,
        )
        p = FileSecretsProvider(path)
        assert p.get("KEY") == "val"

    def test_get_missing_returns_none(self, tmp_path):
        path = self._write_secrets(tmp_path, "KEY=val\n")
        p = FileSecretsProvider(path)
        assert p.get("NOPE") is None

    def test_require_present(self, tmp_path):
        path = self._write_secrets(tmp_path, "SECRET=myvalue\n")
        p = FileSecretsProvider(path)
        assert p.require("SECRET") == "myvalue"

    def test_require_missing_raises(self, tmp_path):
        path = self._write_secrets(tmp_path, "KEY=val\n")
        p = FileSecretsProvider(path)
        with pytest.raises(ConfigurationError, match="MISSING_KEY"):
            p.require("MISSING_KEY")

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="not found"):
            FileSecretsProvider(tmp_path / "nonexistent.env")

    def test_value_with_equals_sign(self, tmp_path):
        path = self._write_secrets(tmp_path, "URL=http://example.com?a=1&b=2\n")
        p = FileSecretsProvider(path)
        assert p.get("URL") == "http://example.com?a=1&b=2"

    def test_implements_protocol(self, tmp_path):
        path = self._write_secrets(tmp_path, "K=v\n")
        p = FileSecretsProvider(path)
        assert isinstance(p, SecretsProvider)

    def test_accepts_path_object(self, tmp_path):
        path = tmp_path / "s.env"
        path.write_text("K=v\n")
        p = FileSecretsProvider(path)
        assert p.get("K") == "v"

    def test_accepts_string_path(self, tmp_path):
        path = tmp_path / "s.env"
        path.write_text("K=v\n")
        p = FileSecretsProvider(str(path))
        assert p.get("K") == "v"


# ---------------------------------------------------------------------------
# _extract_agent_credentials
# ---------------------------------------------------------------------------


class TestExtractAgentCredentials:
    def _call(self, config: dict) -> dict:
        from civitas.runtime import _extract_agent_credentials

        return _extract_agent_credentials(config)

    def test_agent_with_credentials(self):
        config = {
            "supervision": {
                "children": [
                    {
                        "agent": {
                            "name": "my_agent",
                            "credentials": {"anthropic": "sk-ant-xyz"},
                        }
                    }
                ]
            }
        }
        result = self._call(config)
        assert result == {"my_agent": {"anthropic": "sk-ant-xyz"}}

    def test_agent_without_credentials_excluded(self):
        config = {"supervision": {"children": [{"agent": {"name": "bare_agent"}}]}}
        result = self._call(config)
        assert result == {}

    def test_nested_supervisor_children(self):
        config = {
            "supervision": {
                "children": [
                    {
                        "supervisor": {
                            "children": [
                                {
                                    "agent": {
                                        "name": "nested_agent",
                                        "credentials": {"openai": "sk-oai-abc"},
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
        result = self._call(config)
        assert result == {"nested_agent": {"openai": "sk-oai-abc"}}

    def test_multiple_agents_multiple_providers(self):
        config = {
            "supervision": {
                "children": [
                    {
                        "agent": {
                            "name": "agent_a",
                            "credentials": {"anthropic": "key_a"},
                        }
                    },
                    {
                        "agent": {
                            "name": "agent_b",
                            "credentials": {"anthropic": "key_b", "openai": "key_c"},
                        }
                    },
                ]
            }
        }
        result = self._call(config)
        assert result["agent_a"] == {"anthropic": "key_a"}
        assert result["agent_b"] == {"anthropic": "key_b", "openai": "key_c"}

    def test_empty_supervision(self):
        config = {"supervision": {"children": []}}
        assert self._call(config) == {}

    def test_no_supervision_key(self):
        assert self._call({}) == {}

    def test_credentials_cast_to_str(self):
        config = {
            "supervision": {
                "children": [
                    {
                        "agent": {
                            "name": "a",
                            "credentials": {"provider": 12345},
                        }
                    }
                ]
            }
        }
        result = self._call(config)
        assert result["a"]["provider"] == "12345"


# ---------------------------------------------------------------------------
# AgentProcess.get_credential and model_for
# ---------------------------------------------------------------------------


class _DummyAgent:
    """Minimal stand-in to test credential methods without a real AgentProcess."""

    pass


class TestAgentProcessCredentials:
    def _make_agent(self):
        from civitas import AgentProcess

        class SimpleAgent(AgentProcess):
            async def handle(self, message: Message) -> None:
                pass

        agent = SimpleAgent.__new__(SimpleAgent)
        agent._credentials = {}
        agent.llm = None
        return agent

    def test_get_credential_returns_value(self):
        agent = self._make_agent()
        agent._credentials = {"anthropic": "sk-ant-test"}
        assert agent.get_credential("anthropic") == "sk-ant-test"

    def test_get_credential_missing_returns_none(self):
        agent = self._make_agent()
        agent._credentials = {}
        assert agent.get_credential("anthropic") is None

    def test_model_for_per_agent_credential(self):
        agent = self._make_agent()
        agent._credentials = {"anthropic": "sk-ant-per-agent"}

        mock_provider_cls = MagicMock()
        mock_provider_instance = MagicMock()
        mock_provider_cls.return_value = mock_provider_instance

        with patch("civitas.process.resolve_plugin_class", return_value=mock_provider_cls):
            result = agent.model_for("anthropic")

        mock_provider_cls.assert_called_once_with(api_key="sk-ant-per-agent")
        assert result is mock_provider_instance

    def test_model_for_falls_back_to_global_llm(self):
        agent = self._make_agent()
        agent._credentials = {}
        mock_llm = MagicMock()
        agent.llm = mock_llm

        result = agent.model_for("anthropic")
        assert result is mock_llm

    def test_model_for_raises_when_no_credential_no_llm(self):
        agent = self._make_agent()
        agent._credentials = {}
        agent.llm = None

        with pytest.raises(ConfigurationError, match="anthropic"):
            agent.model_for("anthropic")

    def test_model_for_prefers_credential_over_global_llm(self):
        agent = self._make_agent()
        agent._credentials = {"anthropic": "sk-ant-per-agent"}
        agent.llm = MagicMock()

        mock_cls = MagicMock()
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance

        with patch("civitas.process.resolve_plugin_class", return_value=mock_cls):
            result = agent.model_for("anthropic")

        assert result is mock_instance
