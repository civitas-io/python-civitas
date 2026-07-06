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

## WebSocket & gRPC auth

When JWT (`require_jwt`) and/or mTLS (`client_cert_mode`) auth is configured for the HTTP surface, that protection is **automatically inherited** by the WebSocket and gRPC surfaces — no extra config keys. This closes the gap where an operator who configured `require_jwt` got HTTP-only protection while WS and gRPC stayed open.

- **WebSocket (JWT only).** The bearer token rides the `Sec-WebSocket-Protocol` handshake header as a single pinned subprotocol, `civitas.bearer.<jwt>`. It is verified **before** the socket is accepted; a missing or invalid token closes the handshake (WS close code `4401`) and the negotiated subprotocol is echoed on success. WS mTLS is not yet supported (see #25) — a gateway configured with `client_cert_mode` but no JWT logs a startup warning that its WS routes are unauthenticated.
- **gRPC (JWT + mTLS).** A server interceptor verifies the `authorization: Bearer <jwt>` metadata entry and, when `client_cert_mode="required"`, enforces transport-level mTLS (`require_client_auth`) plus the same DN allowlist HTTP uses (`CIVITAS_GATEWAY_MTLS_ALLOWED_DNS`). The `Health` and `ServerReflection` services are exempt so load-balancer probes and reflection clients keep working. `client_cert_mode="optional"` is rejected at startup for gRPC (grpc.aio has no `CERT_OPTIONAL` equivalent), and enforcing JWT over a plaintext (non-TLS) gRPC port is refused (the token would travel in cleartext).

See [design/gateway-ws-grpc-auth.md](design/gateway-ws-grpc-auth.md) for the full rationale (decisions D1–D11).

---

## HTTP mTLS via a reverse proxy

`require_client_cert` authorizes a request on the client certificate's full subject DN. uvicorn never exposes the client certificate from its own TLS handshake to the ASGI app ([uvicorn#400](https://github.com/encode/uvicorn/issues/400)), so terminating mTLS **directly** at uvicorn cannot populate a client cert — every request is rejected `401` even with a valid certificate. This is the default `mtls_source="direct"`: kept for backward compatibility, still non-functional against uvicorn (a known limitation of that mode).

The working pattern is to terminate TLS at a reverse proxy and have it forward the verified client certificate to civitas as the IETF-standard [RFC 9440](https://www.rfc-editor.org/rfc/rfc9440.html) `Client-Cert` header. civitas decodes it, extracts the DN, and feeds it into the **unchanged** DN-allowlist authorization.

Enable it with two new `GatewayConfig` fields:

```python
config = GatewayConfig(
    port=8080,
    mtls_source="proxy_header",                       # trust an upstream proxy's Client-Cert header
    trusted_proxy_cidrs=frozenset({"10.0.0.0/8"}),    # ...only from these peer IPs
    client_cert_mode="none",                          # required in proxy_header mode
    middleware=["civitas.gateway.mtls.require_client_cert"],  # required — authorizes the DN
)
```

The DN allowlist is unchanged: set `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` (semicolon-separated full subject DNs). In topology YAML:

```yaml
- name: api
  type: http_gateway
  config:
    port: 8080
    middleware:
      - civitas.gateway.mtls.require_client_cert
    auth:
      mtls:
        mtls_source: proxy_header
        trusted_proxy_cidrs:
          - 10.0.0.0/8
```

### What changes in this mode

- **Trust is keyed on the true TCP peer IP.** civitas checks the immediate peer against `trusted_proxy_cidrs` **before** reading the header; a request from outside the set has its `Client-Cert` ignored entirely (treated as no certificate → `401`). civitas forces uvicorn's `proxy_headers=False` here, so a client-supplied `X-Forwarded-For` can never rewrite the peer IP the check reads.
- **`client_ip` becomes the proxy's IP.** Because uvicorn's proxy-header handling is disabled, `GatewayRequest.client_ip` (used by rate limiting and access logs) is the proxy's address, not the originating client's. Recovering the real client IP from `X-Forwarded-For` for those consumers is a deliberate non-goal in this mode.
- **`client_cert_mode` must be `"none"`.** Requesting a direct-TLS client cert *and* trusting a proxy-forwarded one on the same HTTP surface is contradictory (validated at config time). If the same deployment also needs gRPC with direct required mTLS, run gRPC on a separate `HTTPGateway` instance with `client_cert_mode="required"` and `mtls_source` left at its default.
- **`require_client_cert` must be in `middleware`.** Otherwise the certificate is extracted every request but never authorized — a silently open gateway. The gateway refuses to start in that combination.
- **Keep `trusted_proxy_cidrs` as narrow as the topology allows** (the proxy's actual address(es)). Widening it re-opens the header-injection risk the trust check exists to close.

### Wire format (RFC 9440)

The proxy MUST send the leaf certificate as:

```
Client-Cert: :<base64-DER>:
```

— the base64 encoding of the **raw DER** certificate (no line breaks), delimited with a colon on each side (an [RFC 8941](https://www.rfc-editor.org/rfc/rfc8941.html) Structured-Field Byte Sequence). This is **base64 DER, not PEM**. Per [RFC 9440 §4](https://www.rfc-editor.org/rfc/rfc9440.html#section-4) the proxy MUST **strip any client-supplied `Client-Cert`/`Client-Cert-Chain` header and set its own** — civitas has no visibility into what the proxy received, so this strip-then-set sanitization is the operator's responsibility. civitas does not read `Client-Cert-Chain`.

> RFC 9440 emission is still nascent across proxies. As of 2026-07, nginx has no native support ([nginx#178](https://github.com/nginx/nginx/issues/178)), Envoy's native `Client-Cert` filter is [in review upstream](https://github.com/envoyproxy/envoy/pull/45412) (its shipping mechanism is XFCC, a different format), and Traefik emits a base64-DER cert under a different header name. The examples below produce the exact `Client-Cert: :<base64-DER>:` civitas expects; verify directive syntax against your proxy's version.

### nginx

nginx has no built-in RFC 9440 support, but a PEM certificate body *is* base64 DER once the `-----BEGIN/END-----` armor and line breaks are removed. `$ssl_client_raw_cert` (a [documented variable](https://nginx.org/en/docs/http/ngx_http_ssl_module.html#var_ssl_client_raw_cert), populated when `ssl_verify_client` is on) gives the PEM; an [njs](https://nginx.org/en/docs/njs/) function wraps it per RFC 9440:

```javascript
// /etc/nginx/njs/client_cert.js
function client_cert(r) {
    var pem = r.variables.ssl_client_raw_cert;
    if (!pem) {
        return "";  // no cert -> empty header -> civitas sees no cert -> 401
    }
    // PEM body IS base64(DER); strip the armor + whitespace and wrap per RFC 9440 §2.2.
    var b64 = pem.replace(/-----[^-]+-----/g, "").replace(/\s+/g, "");
    return ":" + b64 + ":";
}
export default { client_cert };
```

```nginx
http {
    js_path "/etc/nginx/njs/";
    js_import cc from client_cert.js;
    js_set $rfc9440_client_cert cc.client_cert;

    server {
        listen 443 ssl;
        server_name api.example.com;
        ssl_certificate     /etc/nginx/tls/server.crt;
        ssl_certificate_key /etc/nginx/tls/server.key;

        # Terminate mTLS against your dedicated private client CA.
        ssl_client_certificate /etc/nginx/tls/client-ca.crt;
        ssl_verify_client      on;

        location / {
            # strip-then-set: proxy_set_header overwrites any client-supplied value (RFC 9440 §4).
            proxy_set_header Client-Cert       $rfc9440_client_cert;
            proxy_set_header Client-Cert-Chain "";
            proxy_pass http://127.0.0.1:8080;
        }
    }
}
```

### Envoy

Envoy's built-in client-cert forwarding uses the [XFCC](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_conn_man/headers#x-forwarded-client-cert) (`x-forwarded-client-cert`) header, whose `Cert` element is URL-encoded PEM — a different format civitas does not parse. A native RFC 9440 `Client-Cert` filter is [in review upstream](https://github.com/envoyproxy/envoy/pull/45412); until it ships in your build, emit the header with a Lua filter. `forward_client_cert_details: SANITIZE_SET` strips any inbound XFCC so it can't be spoofed (RFC 9440 §4):

```yaml
http_filters:
  - name: envoy.filters.http.lua
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua
      default_source_code:
        inline_string: |
          function envoy_on_request(request_handle)
            -- strip-then-set: drop any client-supplied values first (RFC 9440 §4).
            request_handle:headers():remove("client-cert")
            request_handle:headers():remove("client-cert-chain")
            local ssl = request_handle:streamInfo():downstreamSslConnection()
            if ssl == nil or not ssl:peerCertificatePresented() then
              return
            end
            -- urlEncodedPemEncodedPeerCertificate() -> URL-encoded PEM; decode, drop the
            -- armor, keep the base64(DER) body, wrap per RFC 9440 §2.2.
            local pem = ssl:urlEncodedPemEncodedPeerCertificate():gsub("%%(%x%x)", function(h)
              return string.char(tonumber(h, 16))
            end)
            local b64 = pem:gsub("%-+[^%-]+%-+", ""):gsub("%s+", "")
            request_handle:headers():add("client-cert", ":" .. b64 .. ":")
          end
  # ... your router filter ...
```

The `DownstreamTlsContext` on the listener still terminates mTLS (`require_client_certificate: true` with your client CA in `validation_context`). Verify the Lua SSL methods against your Envoy version, or adopt the native filter above once released.

### Traefik

Traefik's [`passTLSClientCert`](https://doc.traefik.io/traefik/middlewares/http/passtlsclientcert/) middleware (`pem: true`) forwards the client certificate as base64 DER (the PEM body with delimiters and newlines removed) in the `X-Forwarded-Tls-Client-Cert` header:

```yaml
# dynamic configuration
http:
  middlewares:
    client-cert:
      passTLSClientCert:
        pem: true          # -> X-Forwarded-Tls-Client-Cert: <base64-DER>
  routers:
    api:
      rule: "Host(`api.example.com`)"
      middlewares:
        - client-cert
      tls:
        options: mtls      # a TLSOption with clientAuth.clientAuthType: RequireAndVerifyClientCert
      service: civitas-api
```

The encoding is right (base64 DER), but the header **name** (`X-Forwarded-Tls-Client-Cert`, not `Client-Cert`) and the missing colon delimiters mean it is not directly RFC-9440-consumable, and Traefik's built-in middleware cannot rename a header or wrap its value. Bridge it to `Client-Cert: :<base64-DER>:` with a Traefik [plugin](https://plugins.traefik.io/) (Yaegi) or a one-hop sidecar (e.g. the nginx `map`/njs snippet above reading `$http_x_forwarded_tls_client_cert`) that also strips any client-supplied `Client-Cert`. This is the one proxy of the three without a turn-key RFC 9440 story today.

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
