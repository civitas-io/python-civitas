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
        enabled: When False the process runs unsandboxed. Default: True
            (fail-closed) -- changed from False in a real, deliberate breaking
            fix: a bare ``SandboxConfig()`` previously meant *unsandboxed*,
            the opposite of this org's own repeatedly-stated fail-closed-by-
            default platform-wide principle, and a real, security-relevant
            divergence from fabrica's own independently-defined
            ``SandboxConfig`` (which already defaulted ``enabled=True``) --
            found while scoping the ``connect_mcp()``/``MCPTool`` fix. A
            caller who genuinely wants an unsandboxed MCP server subprocess
            must now say so explicitly (``enabled=False``), matching this
            codebase's own established ``allow_ungoverned``/
            ``allow_unsandboxed`` opt-out convention elsewhere.
        network: ``"deny"`` blocks all outbound network access;
                 ``"allow"`` leaves the network namespace shared with the host.
        filesystem: Explicit bind-mounts added on top of the base read-only root.
        allow_unsandboxed: Added 2026-08-25, migrated in from fabrica's own,
            previously-independently-defined ``SandboxConfig`` while
            unifying the two into one canonical type (see this fix's own
            changelog entry). Civitas core itself never reads this field --
            it exists here purely so a single ``SandboxConfig`` instance
            carries everything ``fabrica.mcp.isolation.SrtIsolation`` needs
            (real isolation unavailable, e.g. ``srt`` not installed, AND
            the caller has explicitly opted out of the fail-closed default)
            without fabrica needing a second, divergence-prone type. Same
            fail-closed-by-default / explicit-opt-in shape as
            ``NullPresidiumClient.allow_ungoverned`` elsewhere in this org.
    """

    enabled: bool = True
    network: str = "deny"
    filesystem: list[FilesystemMount] = field(default_factory=list)
    allow_unsandboxed: bool = False

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
            enabled=bool(data.get("enabled", True)),
            network=str(data.get("network", "deny")),
            filesystem=mounts,
            allow_unsandboxed=bool(data.get("allow_unsandboxed", False)),
        )
