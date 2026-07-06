"""Unit tests for MCP integration — types and topology YAML parsing."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from civitas.mcp.types import MCPServerConfig, MCPToolError, MCPToolSchema
from civitas.plugins.tools import ToolRegistry

# ---------------------------------------------------------------------------
# MCPServerConfig validation
# ---------------------------------------------------------------------------


class TestMCPServerConfig:
    def test_stdio_requires_command(self):
        with pytest.raises(ValueError, match="requires 'command'"):
            MCPServerConfig(name="x", transport="stdio")

    def test_sse_requires_url(self):
        with pytest.raises(ValueError, match="requires 'url'"):
            MCPServerConfig(name="x", transport="sse")

    def test_unknown_transport_raises(self):
        with pytest.raises(ValueError, match="unknown transport"):
            MCPServerConfig(name="x", transport="grpc", command="cmd")  # type: ignore[arg-type]

    def test_stdio_valid(self):
        cfg = MCPServerConfig(name="github", transport="stdio", command="npx")
        assert cfg.name == "github"
        assert cfg.transport == "stdio"
        assert cfg.command == "npx"
        assert cfg.args == []
        assert cfg.env is None

    def test_sse_valid(self):
        cfg = MCPServerConfig(name="remote", transport="sse", url="http://localhost:8080")
        assert cfg.url == "http://localhost:8080"

    def test_stdio_with_args_and_env(self):
        cfg = MCPServerConfig(
            name="fs",
            transport="stdio",
            command="npx",
            args=["-y", "@mcp/fs", "/tmp"],
            env={"TOKEN": "abc"},
        )
        assert cfg.args == ["-y", "@mcp/fs", "/tmp"]
        assert cfg.env == {"TOKEN": "abc"}


# ---------------------------------------------------------------------------
# MCPToolError
# ---------------------------------------------------------------------------


class TestMCPToolError:
    def test_str_includes_tool_name_and_detail(self):
        err = MCPToolError("create_issue", "not found")
        assert "create_issue" in str(err)
        assert "not found" in str(err)

    def test_attributes(self):
        err = MCPToolError("search", "timeout")
        assert err.tool_name == "search"
        assert err.detail == "timeout"


# ---------------------------------------------------------------------------
# MCPToolSchema
# ---------------------------------------------------------------------------


class TestMCPToolSchema:
    def test_fields(self):
        schema = MCPToolSchema(
            name="create_issue",
            description="Creates a GitHub issue",
            input_schema={"type": "object", "properties": {"title": {"type": "string"}}},
        )
        assert schema.name == "create_issue"
        assert schema.description == "Creates a GitHub issue"
        assert "title" in schema.input_schema["properties"]


# ---------------------------------------------------------------------------
# ToolRegistry.deregister_prefix
# ---------------------------------------------------------------------------


class TestToolRegistryDeregisterPrefix:
    def test_removes_all_matching_tools(self):
        registry = ToolRegistry()
        for name in ("mcp://github/create_issue", "mcp://github/list_issues", "mcp://fs/read"):
            t = MagicMock()
            t.name = name
            registry.register(t)

        registry.deregister_prefix("mcp://github/")
        assert registry.get("mcp://github/create_issue") is None
        assert registry.get("mcp://github/list_issues") is None
        assert registry.get("mcp://fs/read") is not None

    def test_no_op_for_unknown_prefix(self):
        registry = ToolRegistry()
        registry.deregister_prefix("mcp://nonexistent/")  # should not raise


# ---------------------------------------------------------------------------
# Runtime.from_config — mcp.servers parsed into _mcp_configs
# ---------------------------------------------------------------------------


class TestRuntimeMcpConfig:
    def test_mcp_servers_parsed_from_yaml(self, tmp_path: Path):
        topology = textwrap.dedent("""\
            supervision:
              name: root
              strategy: ONE_FOR_ONE
              children: []
            mcp:
              servers:
                - name: github
                  transport: stdio
                  command: npx
                  args: ["-y", "@mcp/server-github"]
                  env:
                    GITHUB_TOKEN: "tok"
                - name: remote
                  transport: sse
                  url: "http://localhost:8080/sse"
        """)
        cfg_file = tmp_path / "topology.yaml"
        cfg_file.write_text(topology)

        from civitas import Runtime

        runtime = Runtime.from_config(cfg_file)
        assert len(runtime._mcp_configs) == 2

        gh = runtime._mcp_configs[0]
        assert gh.name == "github"
        assert gh.transport == "stdio"
        assert gh.command == "npx"
        assert gh.args == ["-y", "@mcp/server-github"]
        assert gh.env == {"GITHUB_TOKEN": "tok"}

        remote = runtime._mcp_configs[1]
        assert remote.name == "remote"
        assert remote.transport == "sse"
        assert remote.url == "http://localhost:8080/sse"

    def test_no_mcp_section_leaves_empty_list(self, tmp_path: Path):
        topology = textwrap.dedent("""\
            supervision:
              name: root
              strategy: ONE_FOR_ONE
              children: []
        """)
        cfg_file = tmp_path / "topology.yaml"
        cfg_file.write_text(topology)

        from civitas import Runtime

        runtime = Runtime.from_config(cfg_file)
        assert runtime._mcp_configs == []
