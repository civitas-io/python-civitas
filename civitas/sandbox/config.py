"""Sandbox configuration — parsed from the 'sandbox:' block in MCP server YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FilesystemMount:
    """A single filesystem path mounted into the sandbox.

    Attributes:
        path: Absolute path on the host.
        mode: ``"ro"`` (read-only, default) or ``"rw"`` (read-write).
    """

    path: str
    mode: str = "ro"

    def __post_init__(self) -> None:
        if self.mode not in ("ro", "rw"):
            raise ValueError(f"FilesystemMount mode must be 'ro' or 'rw', got '{self.mode}'")


@dataclass
class SandboxConfig:
    """Per-MCP-server sandbox profile.

    Example topology YAML::

        mcp:
          servers:
            - name: shell_tool
              transport: stdio
              command: /usr/local/bin/shell_mcp
              sandbox:
                enabled: true
                network: deny
                filesystem:
                  - /workspace:rw
                  - /etc/ssl/certs:ro

    Attributes:
        enabled: When False the process runs unsandboxed. Default: False.
        network: ``"deny"`` blocks all outbound network access;
                 ``"allow"`` leaves the network namespace shared with the host.
        filesystem: Explicit bind-mounts added on top of the base read-only root.
    """

    enabled: bool = False
    network: str = "deny"
    filesystem: list[FilesystemMount] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.network not in ("deny", "allow"):
            raise ValueError(
                f"SandboxConfig network must be 'deny' or 'allow', got '{self.network}'"
            )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SandboxConfig:
        """Parse a ``sandbox:`` YAML block into a ``SandboxConfig``.

        Raises:
            ValueError: if the network value or a mount mode is invalid.
        """
        mounts: list[FilesystemMount] = []
        for entry in data.get("filesystem", []):
            if isinstance(entry, str):
                path, _, mode = entry.partition(":")
                mounts.append(FilesystemMount(path=path, mode=mode or "ro"))
            else:
                mounts.append(FilesystemMount(path=entry["path"], mode=entry.get("mode", "ro")))

        return cls(
            enabled=bool(data.get("enabled", False)),
            network=str(data.get("network", "deny")),
            filesystem=mounts,
        )
