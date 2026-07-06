# HTTP Gateway

`HTTPGateway` exposes Civitas agents as a REST API. It is a supervised `AgentProcess` that starts a uvicorn ASGI server and translates inbound HTTP requests into `call` or `cast` messages on the bus. Agents behind the gateway never see HTTP — they handle `Message` like any other agent. The gateway handles routing, request parsing, response serialization, middleware, OpenAPI generation, and optional HTTP/3.

```bash
pip install 'civitas[http]'      # HTTP/1.1 + HTTP/2 (uvicorn + pydantic)
pip install 'civitas[http3]'     # adds QUIC / HTTP/3
```

---

## Minimal setup

```python
from civitas import AgentProcess, Runtime, Supervisor
from civitas.gateway import GatewayConfig, HTTPGateway
from civitas.messages import Message

class EchoAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})

config = GatewayConfig(
    host="127.0.0.1",
    port=8080,
    routes=[
        {"method": "POST", "path": "/v1/echo", "agent": "echo", "mode": "call"},
    ],
)

runtime = Runtime(
    supervisor=Supervisor("root", children=[
        HTTPGateway("api", config=config),
        EchoAgent("echo"),
    ])
)
await runtime.start()
```

```bash
curl -X POST http://127.0.0.1:8080/v1/echo \
     -H 'Content-Type: application/json' \
     -d '{"text": "hello"}'
# → {"echo": "hello"}
```

---

## Route configuration

Routes are declared in `GatewayConfig.routes` as a list of dicts:

| Field | Type | Description |
|---|---|---|
| `method` | `str` | HTTP method: `"GET"`, `"POST"`, `"PUT"`, `"DELETE"`, etc. |
| `path` | `str` | URL path, optionally with `{param}` placeholders |
| `agent` | `str` | Name of the target agent |
| `mode` | `"call"` \| `"cast"` | `call` waits for a reply; `cast` returns 202 immediately |

Path parameters are extracted and merged into the message payload automatically:

```python
routes=[
    # {session_id} is extracted into message.payload["session_id"]
    {"method": "GET",  "path": "/sessions/{session_id}",         "agent": "session_store", "mode": "call"},
    {"method": "POST", "path": "/sessions/{session_id}/messages", "agent": "chat",          "mode": "call"},
    {"method": "DELETE","path": "/sessions/{session_id}",         "agent": "session_store", "mode": "cast"},
]
```

Query parameters are also merged into the payload. If both `path_params` and `query_params` contain the same key, path params win.

---

## Default routes

If `routes` is empty, the gateway registers three default routes for every agent in the supervision tree:

| Method | Path | Mode | Description |
|---|---|---|---|
| `POST` | `/agents/{name}` | `call` | Send a message and wait for reply |
| `POST` | `/agents/{name}/cast` | `cast` | Fire-and-forget, returns 202 |
| `GET` | `/agents/{name}/state` | `call` | Fetch agent state (agent must handle `type: "state"`) |

The default routes are a development convenience. For production, declare explicit routes.

---

## GatewayConfig reference

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"0.0.0.0"` | Bind address |
| `port` | `int` | `8080` | HTTP port |
| `port_quic` | `int \| None` | `None` | UDP port for HTTP/3 / QUIC |
| `tls_cert` | `str \| None` | `None` | Path to TLS certificate file |
| `tls_key` | `str \| None` | `None` | Path to TLS private key file |
| `request_timeout` | `float` | `30.0` | Seconds before a call times out with 504 |
| `enable_http3` | `bool` | `False` | Enable QUIC / HTTP/3 (requires TLS + port_quic) |
| `routes` | `list[dict]` | `[]` | Route declarations (empty = default routes) |
| `middleware` | `list[str]` | `[]` | Dotted import paths for middleware callables |
| `docs_enabled` | `bool` | `True` | Serve Swagger UI at `docs_path` |
| `docs_path` | `str` | `"/docs"` | Base path for API docs |

---

## The `@route` decorator

The `@route` decorator colocates route metadata with the agent method it describes. This is for documentation and IDE navigation — the YAML config is authoritative at runtime:

```python
from civitas.gateway import route, contract
from pydantic import BaseModel

class ChatRequest(BaseModel):
    session_id: str
    message: str

class ChatResponse(BaseModel):
    reply: str
    tokens_used: int

class ChatAgent(AgentProcess):

    @route("POST", "/v1/chat/{session_id}", mode="call")
    @contract(request=ChatRequest, response=ChatResponse)
    async def handle(self, message: Message) -> Message | None:
        # message.payload is validated against ChatRequest
        session_id = message.payload["session_id"]
        text = message.payload["message"]

        response = await self.llm.chat(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": text}],
        )
        # reply dict is validated against ChatResponse before sending
        return self.reply({
            "reply": response.content,
            "tokens_used": response.tokens_in + response.tokens_out,
        })
```

`@route` attaches path and method to the function as metadata. `@contract` enables Pydantic validation: a bad request body returns `422 Unprocessable Entity`; a reply that fails `ChatResponse` validation returns `500`.

---

## Middleware

Middleware are async callables that wrap every request before it reaches the agent. Declare them as dotted import paths in `GatewayConfig.middleware`. They execute in order — first entry is the outermost wrapper.

```python
# myapp/middleware.py

from civitas.gateway import GatewayRequest, GatewayResponse

async def require_api_key(request: GatewayRequest, next_fn) -> GatewayResponse:
    if request.headers.get("x-api-key") != "secret":
        return GatewayResponse(status=401, body={"error": "unauthorized"})
    return await next_fn(request)

async def log_requests(request: GatewayRequest, next_fn) -> GatewayResponse:
    import logging, time
    t0 = time.monotonic()
    response = await next_fn(request)
    elapsed = (time.monotonic() - t0) * 1000
    logging.getLogger("gateway").info("%s %s → %d (%.1fms)",
        request.method, request.path, response.status, elapsed)
    return response
```

```python
config = GatewayConfig(
    port=8080,
    middleware=[
        "myapp.middleware.require_api_key",  # runs first
        "myapp.middleware.log_requests",
    ],
    routes=[...],
)
```

To short-circuit the chain (e.g. for auth failures), return a `GatewayResponse` directly without calling `next_fn`.

### GatewayRequest fields

| Field | Type | Description |
|---|---|---|
| `method` | `str` | HTTP method (`"GET"`, `"POST"`, etc.) |
| `path` | `str` | Request path, e.g. `"/v1/chat/abc123"` |
| `path_params` | `dict[str, str]` | Extracted `{param}` values from the route pattern |
| `query_params` | `dict[str, str]` | URL query string parameters |
| `headers` | `dict[str, str]` | Lowercase header names |
| `body` | `dict` | Parsed JSON request body |
| `client_ip` | `str` | Remote client IP address |
| `gateway` | `HTTPGateway` | Reference to the gateway process |

### GatewayResponse fields

| Field | Type | Description |
|---|---|---|
| `status` | `int` | HTTP status code (default `200`) |
| `body` | `dict` | Response body, serialized to JSON |
| `headers` | `dict[str, str]` | Additional response headers |

---

## OpenAPI and Swagger UI

The gateway generates an OpenAPI spec from the declared routes and any `@contract` decorators. Documentation is enabled by default:

```
GET /docs              → Swagger UI
GET /docs/openapi.json → Raw OpenAPI spec
```

Disable it in production:

```python
GatewayConfig(port=8080, docs_enabled=False, routes=[...])
```

Routes with `@contract` decorators appear with full request and response schemas in the spec. Routes without contracts show generic `object` schemas.

---

## Topology YAML

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: api
      type: http_gateway
      config:
        host: "0.0.0.0"
        port: 8080
        request_timeout: 15.0
        docs_enabled: true
        middleware:
          - myapp.middleware.require_api_key
        routes:
          - method: POST
            path: /v1/chat/{session_id}
            agent: chat
            mode: call
          - method: GET
            path: /v1/sessions/{session_id}
            agent: session_store
            mode: call
    - name: chat
      type: myapp.agents.ChatAgent
    - name: session_store
      type: myapp.agents.SessionStore
```

---

## HTTP/3 / QUIC

Enable HTTP/3 with TLS certificates and a UDP port:

```python
config = GatewayConfig(
    host="0.0.0.0",
    port=8443,
    port_quic=8444,
    tls_cert="/etc/certs/server.crt",
    tls_key="/etc/certs/server.key",
    enable_http3=True,
    routes=[...],
)
```

The gateway injects `Alt-Svc: h3=":8444"; ma=3600` into every HTTP/1.1 and HTTP/2 response. Clients that support HTTP/3 will upgrade automatically on the next request.

`enable_http3` requires `pip install 'civitas[http3]'` and valid TLS credentials — the gateway will raise `ValueError` at startup if either is missing.

---

## Trace context and message type

Two special headers influence gateway behavior:

**`traceparent`** (W3C trace context) — if present, the gateway extracts the `trace_id` and `parent_span_id` from the header and stamps them onto the outgoing message. This connects external traces to the Civitas trace tree.

**`X-Civitas-Type`** — overrides the `type` field on the outgoing message. By default the gateway sets `type` to the route's path pattern (e.g. `"/v1/chat/{session_id}"`). Use this header to dispatch to a specific handler in a multi-type agent.

---

## What the gateway does not do

**No WebSocket support.** The gateway handles request-reply HTTP only. For streaming or bidirectional communication, connect directly to the Civitas bus or use the SSE transport (planned).

**No TLS termination proxy.** The gateway can serve TLS directly via uvicorn's SSL support, but it is not a reverse proxy. Put nginx or Caddy in front if you need load balancing, certificate management, or connection pooling at scale.

**No authentication built in.** Auth is middleware. The gateway has no concept of users, API keys, or JWTs — implement that in a middleware callable and declare it in `GatewayConfig.middleware`.

---

## See also

- [messaging.md](messaging.md) — call vs. cast semantics, message routing
- [supervision.md](supervision.md) — supervising the gateway alongside agents
- [observability.md](observability.md) — W3C trace context propagation
- [topology.md](topology.md) — YAML topology configuration
