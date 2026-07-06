"""OpenAPI 3.1 spec builder and Swagger UI HTML generator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from civitas.gateway.core import GatewayConfig
    from civitas.gateway.router import RouteTable

_SWAGGER_HTML = """\
<!DOCTYPE html>
<html>
<head>
  <title>Civitas Gateway</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({{
      url: "{openapi_url}",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout"
    }})
  </script>
</body>
</html>"""


def swagger_html(openapi_url: str) -> str:
    return _SWAGGER_HTML.format(openapi_url=openapi_url)


def build_spec(route_table: RouteTable, config: GatewayConfig) -> dict[str, Any]:
    """Build an OpenAPI 3.1 spec from *route_table*."""
    paths: dict[str, Any] = {}

    for entry in route_table.entries():
        oapi_path = entry.path_pattern
        if oapi_path not in paths:
            paths[oapi_path] = {}

        method = entry.method.lower()

        path_params: list[dict[str, Any]] = [
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
            for is_param, name in entry.segments
            if is_param
        ]

        operation: dict[str, Any] = {
            "tags": [entry.agent],
            "summary": f"{entry.mode.upper()} → {entry.agent}",
            "operationId": (
                f"{entry.agent}__{entry.method.lower()}"
                f"__{entry.path_pattern.replace('/', '_').strip('_')}"
            ),
            "parameters": path_params,
            "responses": {
                "404": {"description": "Agent not found"},
                "504": {"description": "Upstream timeout"},
            },
        }

        # Request body schema
        req_schema: dict[str, Any] = {"type": "object"}
        if entry.request_schema is not None:
            try:
                req_schema = entry.request_schema.model_json_schema()
            except Exception:
                pass

        if method in ("post", "put", "patch"):
            operation["requestBody"] = {
                "required": entry.request_schema is not None,
                "content": {"application/json": {"schema": req_schema}},
            }

        # Response schema
        resp_body: dict[str, Any] = {"type": "object"}
        if entry.response_schema is not None:
            try:
                resp_body = entry.response_schema.model_json_schema()
            except Exception:
                pass

        if entry.mode == "cast":
            operation["responses"]["202"] = {"description": "Accepted (fire-and-forget)"}
        else:
            operation["responses"]["200"] = {
                "description": "Successful response",
                "content": {"application/json": {"schema": resp_body}},
            }

        if entry.request_schema is not None:
            operation["responses"]["422"] = {"description": "Request validation error"}
        if entry.response_schema is not None:
            operation["responses"]["500"] = {"description": "Response validation error"}

        paths[oapi_path][method] = operation

    return {
        "openapi": "3.1.0",
        "info": {"title": "Civitas Gateway API", "version": "1.0.0"},
        "paths": paths,
    }
