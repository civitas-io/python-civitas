"""civitas.mcp — MCP configuration types.

The MCP client implementation has moved to fabrica:
    pip install fabrica-context
    from fabrica.mcp.client import MCPClient
    from fabrica.mcp.tool import MCPTool
    from civitas.mcp.types import MCPServerConfig

fabrica.mcp.types.MCPServerConfig/MCPToolSchema re-export civitas's own
types directly (not a second, independently-defined copy) -- both
`from fabrica.mcp.types import MCPServerConfig` and
`from civitas.mcp.types import MCPServerConfig` give you the same class.
"""
