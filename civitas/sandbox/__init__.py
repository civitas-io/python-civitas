"""Sandbox configuration types for MCP subprocess isolation.

The BubblewrapSandbox implementation has moved to fabrica:
    pip install fabrica
    from fabrica.sandbox import BubblewrapSandbox
"""

from civitas.sandbox.config import FilesystemMount, SandboxConfig

__all__ = ["FilesystemMount", "SandboxConfig"]
