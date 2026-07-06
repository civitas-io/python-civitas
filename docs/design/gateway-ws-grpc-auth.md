# Gateway Auth — WebSocket & gRPC Surfaces (#17)

**Status:** 📝 Draft v1 — Oracle-reviewed; **fold-in to v2 NOT done yet** (picking back up in a fresh
session). Design-first (same gate as R1–R7).
**Source:** [GH #17](https://github.com/civitas-io/python-civitas/issues/17); `docs/design/gateway-auth.md` §3 Non-goals (R3, v0.7.0)
**Builds on:** R3 gateway JWT + mTLS (`civitas/gateway/jwt_auth.py`, `civitas/gateway/mtls.py`)

> **Oracle review summary (not yet folded into the doc body below — v1 text still reflects the
> pre-review design):**
> - 🔴 **BLOCKER — confirmed empirically, filed separately as [#25](https://github.com/civitas-io/python-civitas/issues/25):**
>   uvicorn never populates the ASGI TLS extension `scope["extensions"]["tls"]` — verified live
>   against a real uvicorn server with `--ssl-cert-reqs 2` (CERT_REQUIRED) and a valid client cert:
>   `{"extension_keys": [], "tls": null}`. **HTTP `require_client_cert` (R3) has likely never worked
>   in production**, and this design's D1 "WS reuses the same TLS-terminated ASGI scope as HTTP"
>   inherits the identical break. **Action for v2: drop WS-mTLS-via-ASGI-scope from scope until #25
>   ships a fix** (candidates: reverse-proxy header forwarding, custom uvicorn protocol subclass, or
>   hypercorn). gRPC mTLS is unaffected — it reads `context.auth_context()`, not the ASGI scope.
> - 🔴 **BLOCKER — D6 doesn't generalize:** auto-inherit must key on `self._jwt_verifier is not None`
>   / `client_cert_mode != "none"`, **not** the global-only middleware list — route-scoped JWT
>   (a supported pattern elsewhere in the code) would otherwise silently get unauthenticated WS/gRPC.
> - 🟡 **Should-fix (v2 TODO):** gRPC interceptor needs a Health/Reflection carve-out (F4);
>   `require_client_auth=True` is binary in Python's gRPC and contradicts D8's clean
>   `UNAUTHENTICATED` mapping for certless JWT-only clients (F5); D8 has no misconfig error code,
>   losing R3's "loud misconfig, never allow-all" invariant (F6); `tls_ca_cert` isn't plumbed into
>   `GrpcServer` and an insecure gRPC port would auto-enforce JWT over plaintext unless guarded (F7);
>   D4's `rfc4514_string()` DN format isn't guaranteed to match the (also-unverified) HTTP allowlist
>   format — derive both from the same leaf-PEM formatter (F2).
> - 🟢 **Nice-to-have:** `_AUTH_MIDDLEWARE_PATHS` string-match is fragile beyond exact dotted paths;
>   the interceptor needs to wrap the handler to reach `context.auth_context()`; WS subprotocol
>   token encoding needs pinning (e.g. `["civitas.bearer", "<jwt>"]`); accept must echo a sentinel
>   subprotocol per RFC 6455, never the token itself.
> - **D7 (api-key out of scope) — keep the scope cut, but strengthen the mitigation:** the
>   enforcement trigger must NOT reuse the full `_AUTH_MIDDLEWARE_PATHS` (which includes
>   `require_api_key`) — an api-key-only deploy would otherwise look "hardened" (docs auto-off) while
>   WS/gRPC stay wide open. Emit a startup WARNING when api-key-only auth is configured alongside
>   WS/gRPC being enabled.

---

## 1. Problem

R3 (v0.7.0) added JWT and mTLS auth, enforced **only on HTTP** via the ASGI middleware chain. Two
surfaces bypass that chain entirely and are unauthenticated regardless of HTTP config:

- **WebSocket** — `_handle_websocket` accepts the upgrade unconditionally before any check.
- **gRPC** — `grpc.aio.server()` has zero interceptors; mTLS isn't even requested at the transport level.

This is a **silent gap**: an operator who configures `require_jwt` assuming it protects "the
gateway" gets HTTP-only protection. Same failure *shape* as the R3 M1 fail-open bug — auth silently
not applying — just at the surface level instead of the middleware-load level.

## 2. Current behavior (ground truth)

- `_handle_websocket` (`asgi.py:210-224`): `send({"type": "websocket.accept"})` runs before any
  auth check; no `GatewayRequest`/`build_chain` on this path at all.
- `GrpcServer.start()` (`grpc_server.py:161-189`): bare `grpc.aio.server()`, no interceptors.
  `add_secure_port` (178-182) passes only `ssl_server_credentials([(key, cert)])` — no
  `root_certificates`, no `require_client_auth=True`. Client certs aren't requested.
- `JwtVerifier.verify(token: str) -> dict[str, Any]` (`jwt_auth.py:121`) is **already
  transport-agnostic** — a pure function of a token string, no HTTP coupling. Directly reusable.
- The mTLS DN-allowlist check (`mtls.py:52-56`: `dn = request.client_cert.get("dn"); if dn not in
  allowed: 403`) is correct logic but inlined inside an ASGI-shaped middleware function.
- `HTTPGateway` already has a **precedent for cross-cutting auth detection**: `_AUTH_MIDDLEWARE_PATHS`
  (`core.py`) is a frozenset of the three HTTP auth middleware dotted paths, checked today only to
  flip the `/docs` default off when auth is configured. This is the exact mechanism §4 D6 reuses.

## 3. Design approach

Extract the two verifiers' core logic to be transport-agnostic (JWT already is; add a
`_check_dn(dn, allowed)` helper next to the existing HTTP check), then add one enforcement point per
surface:

- **WebSocket**: read the token from the `Sec-WebSocket-Protocol` upgrade header (or the peer cert
  from the TLS-terminated ASGI scope, same as HTTP) and verify **before** `websocket.accept` — a
  failure closes the socket without ever completing the upgrade.
- **gRPC**: a `grpc.aio.ServerInterceptor` reading the `authorization` invocation-metadata entry
  (JWT) and/or `require_client_auth=True` + `context.auth_context()` (mTLS), aborting
  `UNAUTHENTICATED` before the servicer method runs.

**Auto-inherit, not separate config** (D6): reuse `_AUTH_MIDDLEWARE_PATHS` — if `require_jwt` (or
`require_client_cert`) is present in the gateway's global HTTP middleware list, the corresponding
check auto-applies to WS/gRPC too. No new YAML keys; closes the gap by default instead of requiring
an operator to remember a second config surface (the R3 M1 lesson).

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** | **WS token transport = `Sec-WebSocket-Protocol` subprotocol header**, not `?token=` query param. | Query params leak into proxy/access logs and browser history; the subprotocol header is set by the WS handshake itself and isn't URL-visible. mTLS reuses the same TLS-terminated scope as HTTP — no new transport needed. |
| **D2** ⚠️ | **WS fails BEFORE accept.** Invalid/missing token or cert → `send({"type": "websocket.close", "code": 4401})` — never send `websocket.accept`. | No unauthenticated socket ever opens (vs. accept-then-kick, which briefly exposes an open channel). `4401` mirrors HTTP 401 and the existing `4404` (unknown route) convention already in `asgi.py:222`. |
| **D3** | **gRPC = one `grpc.aio.ServerInterceptor`** reading `authorization` metadata, reusing `JwtVerifier.verify()` unchanged. Abort `UNAUTHENTICATED` pre-dispatch (never reaches `_AgentServicer`). | Matches the issue's own proposal; `verify()` needs zero changes since it was already transport-agnostic. |
| **D4** ⚠️ | **gRPC mTLS needs transport-level wiring, not just detection.** `add_secure_port` must gain `root_certificates=<CA bytes>, require_client_auth=True` (today: neither) before any peer cert exists to check. DN extraction: parse `context.auth_context()["x509_pem_cert"][0]` via `cryptography.x509.load_pem_x509_certificate(...).subject.rfc4514_string()` to match the allowlist's DN string format. | Python's `grpc.aio` doesn't expose a ready-made "full subject DN" the way ASGI/uvicorn does — this is a genuinely awkward corner of the gRPC Python API. **Flagging for Oracle**: verify this is the right extraction path and that `rfc4514_string()` produces DN strings comparable to whatever `mtls.py`'s existing HTTP-side allowlist entries actually contain. |
| **D5** | **Shared predicate, not shared HTTP-shaped function.** Extract `_check_dn(dn: str \| None, allowed: frozenset[str]) -> None` (raises on failure) out of `mtls.py`; HTTP/WS/gRPC each build their own request-shaped call and share this one predicate + `JwtVerifier.verify()`. | Avoids three copies of "is this DN in the allowlist" drifting apart; each transport keeps its own idiomatic error mapping (`GatewayResponse` / `websocket.close` / `context.abort`). |
| **D6** ★ | **Auto-inherit from the global HTTP middleware list** via `_AUTH_MIDDLEWARE_PATHS`, not a separate `ws_middleware:`/`grpc_interceptors:` YAML block. | Repeats the R3 M1 lesson: auth silently not applying is a bug, not a feature; a second easy-to-forget config surface would recreate exactly this issue. Reusing existing detection code is also less new surface to review. |
| **D7** ⚠️ | **Residual gap, documented not silently dropped: `require_api_key` is explicitly OUT of scope** (matches the issue text — "reuse JwtVerifier and the DN allowlist"). An API-key-only deployment (no JWT/mTLS) still gets **unauthenticated WS/gRPC** after this change. | Honesty over false completeness. Tracked as an explicit fast-follow in §8, not hidden — the whole point of this issue is not leaving silent gaps. |
| **D8** | Failure mapping: WS → close code `4401`; gRPC → `UNAUTHENTICATED` (bad/missing token) or `PERMISSION_DENIED` (valid token, DN not in allowlist) — mirrors HTTP's 401 vs 403 split. | Matches issue text; consistent with existing HTTP semantics. |

★ pivotal · ⚠️ needs careful review

## 5. Threat model

- **Downgrade risk**: a client that can't/won't send the subprotocol header degrades to the *old*
  unauthenticated behavior only if D6's detection is bypassed — mitigated by D2 (fail-closed on
  missing token, same as HTTP's existing "no allowlist configured → 500, never allow-all" pattern).
- **mTLS trust anchor**: same operator responsibility as R3 — `GatewayConfig.tls_ca_cert` must be a
  dedicated private CA (documented already in `mtls.py`'s module docstring); this design reuses that
  same config for gRPC's `root_certificates`, not a new one.
- **D7 residual**: explicitly called out above — API-key-only deployments remain unauthenticated on
  WS/gRPC after this ships.

## 6. Config

No new YAML keys. `CIVITAS_JWT_*` and `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` (existing, R3) are read by
the new WS/gRPC enforcement points exactly as they are by the HTTP middleware today; `_AUTH_MIDDLEWARE_PATHS`
(existing) is the sole trigger for turning enforcement on.

## 7. Test plan (outline)

- WS: missing/invalid/valid token via subprotocol header → close(4401) / close(4401) / accept.
- WS: mTLS-configured gateway, no client cert / wrong DN / correct DN → close(4401)/close(4401)/accept.
- WS: no HTTP auth middleware configured → unchanged (still open), proving D6 doesn't regress the default.
- gRPC: missing/invalid/valid JWT metadata → `UNAUTHENTICATED` / `UNAUTHENTICATED` / dispatched.
- gRPC: mTLS configured, no client cert / wrong DN / correct DN → `UNAUTHENTICATED` (or handshake
  failure if `require_client_auth` rejects at TLS layer) / `PERMISSION_DENIED` / dispatched.
- gRPC: DN extraction round-trip test against a real cert fixture (validates D4's `rfc4514_string()` claim).
- Existing G2/G3 WS/gRPC suites stay green when no auth is configured (regression guard for D6).

## 8. Non-goals / fast-follows

- `require_api_key` on WS/gRPC (D7) — tracked as a fast-follow, not silently dropped.
- Per-route (as opposed to gateway-global) WS/gRPC auth — out of scope; HTTP's route-scoped
  middleware doesn't have a WS/gRPC equivalent concept to hang off of yet.

## 9. References

- GH #17; `docs/design/gateway-auth.md` (R3); `civitas/gateway/{jwt_auth,mtls,asgi,grpc_server,core}.py`.
