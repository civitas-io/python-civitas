"""Unit tests for sandbox configuration (SandboxConfig, FilesystemMount)."""

from __future__ import annotations

import pytest

from civitas.sandbox.config import FilesystemMount, SandboxConfig

# ---------------------------------------------------------------------------
# SandboxConfig
# ---------------------------------------------------------------------------


class TestSandboxConfig:
    def test_defaults(self):
        cfg = SandboxConfig()
        assert cfg.enabled is False
        assert cfg.network == "deny"
        assert cfg.filesystem == []

    def test_from_dict_minimal(self):
        cfg = SandboxConfig.from_dict({"enabled": True})
        assert cfg.enabled is True
        assert cfg.network == "deny"
        assert cfg.filesystem == []

    def test_from_dict_full(self):
        cfg = SandboxConfig.from_dict(
            {
                "enabled": True,
                "network": "allow",
                "filesystem": ["/workspace:rw", "/data:ro"],
            }
        )
        assert cfg.enabled is True
        assert cfg.network == "allow"
        assert len(cfg.filesystem) == 2
        assert cfg.filesystem[0] == FilesystemMount("/workspace", "rw")
        assert cfg.filesystem[1] == FilesystemMount("/data", "ro")

    def test_from_dict_filesystem_defaults_ro(self):
        cfg = SandboxConfig.from_dict({"filesystem": ["/models:"]})
        assert cfg.filesystem[0].mode == "ro"

    def test_from_dict_filesystem_dict_form(self):
        cfg = SandboxConfig.from_dict({"filesystem": [{"path": "/workspace", "mode": "rw"}]})
        assert cfg.filesystem[0] == FilesystemMount("/workspace", "rw")

    def test_from_dict_filesystem_dict_defaults_ro(self):
        cfg = SandboxConfig.from_dict({"filesystem": [{"path": "/data"}]})
        assert cfg.filesystem[0].mode == "ro"

    def test_invalid_network_raises(self):
        with pytest.raises(ValueError, match="network"):
            SandboxConfig(network="block")

    def test_from_dict_invalid_network_raises(self):
        with pytest.raises(ValueError, match="network"):
            SandboxConfig.from_dict({"network": "half"})

    def test_enabled_false_by_default_in_from_dict(self):
        cfg = SandboxConfig.from_dict({})
        assert cfg.enabled is False


class TestFilesystemMount:
    def test_defaults_ro(self):
        m = FilesystemMount("/path")
        assert m.mode == "ro"

    def test_rw(self):
        m = FilesystemMount("/path", "rw")
        assert m.mode == "rw"

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="mode"):
            FilesystemMount("/path", "exec")
