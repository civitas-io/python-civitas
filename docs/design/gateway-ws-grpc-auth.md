# Gateway Auth — WebSocket & gRPC Surfaces (#17)

**Status:** ✅ Approved v2 — Oracle blockers + should-fix items folded in; maintainer signed off
2026-07-06 on all six open calls (§9: Q1 refuse, Q2 hard error, Q3 exempt both, Q4 defer, Q5 add
warning, Q6 regenerate diagram now). Plan → Momus → implementation next.
**Source:** [GH #17](https://github.com/civitas-io/python-civitas/issues/17); `docs/design/gateway-auth.md` §3 Non-goals (R3, v0.7.0)
**Builds on:** R3 gateway JWT + mTLS (`civitas/gateway/jwt_auth.py`, `civitas/gateway/mtls.py`)
**Blocked (partially) by:** [GH #25](https://github.com/civitas-io/python-civitas/issues/25) — HTTP mTLS is
non-functional on uvicorn; WS-mTLS shared the identical broken mechanism and is dropped from this
design's scope until #25 ships a fix.

> **v2 changelog (Oracle review, folded in):**
> - 🔴→✅ **WS-mTLS-via-ASGI-scope dropped from scope.** Confirmed empirically (#25): uvicorn never
>   populates `scope["extensions"]["tls"]`, so the ASGI TLS extension WS-mTLS would have reused does
>   not exist on this stack. WS auth in this design is now **JWT-only** (D1/D2); WS-mTLS is a
>   **non-goal pending #25** (§8), not a silently dropped feature. gRPC mTLS is unaffected — it never
>   depended on the ASGI scope, and is now fully wired (D4).
> - 🔴→✅ **D6 rewritten to not generalize incorrectly.** Auto-inherit no longer keys on
>   `_AUTH_MIDDLEWARE_PATHS` (a dotted-path list that (a) only reflects the *global* HTTP middleware
>   chain and (b) conflates `require_api_key` with `require_jwt`/`require_client_cert`). It now keys
>   directly on **`self._jwt_verifier is not None`** and **`self._gw_config.client_cert_mode != "none"`**
>   — the same two signals `on_start` already uses to decide whether HTTP auth is real, computed from
>   `_configured_middleware_paths()` (global **+** route-scoped), so route-scoped-only JWT correctly
>   auto-inherits onto WS/gRPC instead of silently staying open.
> - 🟡→✅ **F4 (gRPC Health/Reflection carve-out):** the interceptor now exempts the
>   `grpc.health.v1.Health` and `grpc.reflection.v1alpha.ServerReflection` services by full service
>   name (D3) — health probes and reflection clients never carried the bearer/cert this design adds.
> - 🟡→✅ **F5 (`require_client_auth` is binary vs. D8's clean `UNAUTHENTICATED`):** resolved by making
>   `client_cert_mode="optional"` an explicit **`ConfigurationError` at startup when `grpc_enabled`**
>   (D9) — Python's `grpc.aio` has no CERT_OPTIONAL equivalent, so "optional" is an HTTP-only mode;
>   gRPC mTLS is `"none"` or `"required"` only, never a silent reinterpretation.
> - 🟡→✅ **F6 (D8 missing misconfig code):** added `INTERNAL` (gRPC) / close code `1011` (WS) for the
>   "enforcement auto-triggered but the DN allowlist is empty" misconfig case — mirrors HTTP's existing
>   500 and preserves R3's "loud misconfig, never allow-all" invariant on the new surfaces too (D8).
> - 🟡→✅ **F7 (`tls_ca_cert` not plumbed; insecure port could auto-enforce JWT over plaintext):**
>   `tls_ca_cert` + `client_cert_mode` now flow into `GrpcServer` (D4); a new startup guard (D10)
>   refuses to start if JWT auto-inherit would run over a plaintext (`add_insecure_port`) gRPC channel.
> - 🟡→✅ **F2 (D4's DN format claim unverified):** both transports' DN extraction now goes through one
>   shared `_dn_from_pem(pem: bytes) -> str` helper (`cryptography`'s `rfc4514_string()`), so gRPC and
>   any future #25-unblocked HTTP/WS mTLS are guaranteed to agree on DN string format **by
>   construction**, not by assumption (D4/D5).
> - 🟢 **Nice-to-haves folded into mechanics, not left as loose ends:** D1/D2 now pin the exact
>   subprotocol values and the accept-echo behavior; D3 spells out that the interceptor must wrap the
>   handler (not just inspect `handler_call_details`) to reach `context.auth_context()`.
> - **D7 residual note:** the false-hardening risk Oracle flagged (an api-key-only deploy's
>   `_AUTH_MIDDLEWARE_PATHS` membership making WS/gRPC auto-inherit *look* triggered) is now
>   structurally impossible — D6's rewrite never consults `_AUTH_MIDDLEWARE_PATHS`. The startup-WARNING
>   enhancement Oracle also suggested for the api-key-only case is deferred (§9, Q4 — maintainer
>   agreed); the closely related **mTLS-only + WS-routes-configured** case gets a warning now (D11,
>   §9 Q5 — maintainer requested it, since it's the more likely real-world misconfig in v2).
> - **Diagram regenerated** (`docs/assets/gateway-ws-grpc-auth.svg`) to match v2's mechanism (§9, Q6).

---

## 1. Problem

R3 (v0.7.0) added JWT and mTLS auth, enforced **only on HTTP** via the ASGI middleware chain. Two
surfaces bypass that chain entirely and are unauthenticated regardless of HTTP config:

- **WebSocket** — `_handle_websocket` accepts the upgrade unconditionally before any check.
- **gRPC** — `grpc.aio.server()` has zero interceptors; mTLS isn't even requested at the transport level.

This is a **silent gap**: an operator who configures `require_jwt` assuming it protects "the
gateway" gets HTTP-only protection. Same failure *shape* as the R3 M1 fail-open bug — auth silently
not applying — just at the surface level instead of the middleware-load level.

A third, related gap surfaced *during this design's review* and is **not** fixed here: HTTP
`require_client_cert` itself has likely never worked against a real uvicorn deployment (#25). WS was
going to inherit that exact broken mechanism (D1 v1), so v2 scopes WS-mTLS out entirely rather than
build on a foundation known not to work (see changelog above and §8).

## 2. Current behavior (ground truth)

- `_handle_websocket` (`asgi.py:210-224`): `send({"type": "websocket.accept"})` runs before any
  auth check; no `GatewayRequest`/`build_chain` on this path at all.
- `GrpcServer.start()` (`grpc_server.py:161-189`): bare `grpc.aio.server()`, no interceptors.
  `add_secure_port` (178-182) passes only `ssl_server_credentials([(key, cert)])` — no
  `root_certificates`, no `require_client_auth=True`. Client certs aren't requested. `tls_ca_cert`
  and `client_cert_mode` are not passed into `GrpcServer` at all today.
- `JwtVerifier.verify(token: str) -> dict[str, Any]` (`jwt_auth.py:121`) is **already
  transport-agnostic** — a pure function of a token string, no HTTP coupling. Directly reusable.
- The mTLS DN-allowlist check (`mtls.py:52-56`: `dn = request.client_cert.get("dn"); if dn not in
  allowed: 403`) is correct logic but inlined inside an ASGI-shaped middleware function, and — per
  `_client_cert_from_scope` (`asgi.py:111-124`) — is fed by `scope["extensions"]["tls"]`, which #25
  proved uvicorn **never populates**. The check itself (`dn not in allowed`) is fine; what's broken
  is everything upstream of it on the HTTP/WS ASGI path.
- `HTTPGateway` has **two differently-scoped auth-detection helpers**, and this is exactly the
  BLOCKER-2 source: `_auth_configured()` (`core.py:99-102`) checks `client_cert_mode` plus **only**
  `self._gw_config.middleware` (the global list) against `_AUTH_MIDDLEWARE_PATHS`; it exists solely to
  flip the `/docs` default off. `_configured_middleware_paths()` (`core.py:252-257`) checks the global
  list **plus every route's own `entry.middleware`**, and is what `on_start` (`core.py:169`) already
  uses to decide whether to build `self._jwt_verifier` at all. v1's D6 said "reuse
  `_AUTH_MIDDLEWARE_PATHS`" without specifying *which* of these two check the reuse meant — v2 keys
  WS/gRPC auto-inherit off the **already-correctly-scoped** signals (`self._jwt_verifier is not None`,
  `self._gw_config.client_cert_mode != "none"`) instead of re-deriving from either middleware-path set.

## 3. Design approach

Extract the two verifiers' core logic to be transport-agnostic (JWT already is; add a
`_check_dn(dn, allowed)` predicate and a `_dn_from_pem(pem)` extractor next to the existing HTTP
check in `mtls.py`), then add one enforcement point per surface:

- **WebSocket (JWT only, v2):** read the bearer token from the `Sec-WebSocket-Protocol` upgrade
  header and verify **before** `websocket.accept` — a failure closes the socket without ever
  completing the upgrade. WS mTLS is a non-goal pending #25 (§8) — a gateway with mTLS-only auth
  (no JWT) configured gets **no WS protection** in v2; see Q5 (§9) for whether that combination
  should emit a startup warning.
- **gRPC (JWT + mTLS, v2 — both now fully wired):** a `grpc.aio.ServerInterceptor` reading the
  `authorization` invocation-metadata entry (JWT) and, when `client_cert_mode="required"`,
  transport-level `require_client_auth=True` + `context.auth_context()` (mTLS DN check), aborting
  before the servicer method runs — except for `Health`/`Reflection`, which are exempted (D3/F4).

**Auto-inherit, not separate config** (D6, rewritten): if a JWT verifier was built (JWT is
configured anywhere — global or route-scoped HTTP middleware) or `client_cert_mode != "none"`, the
corresponding check auto-applies to WS/gRPC too. No new YAML keys; closes the gap by default instead
of requiring an operator to remember a second config surface (the R3 M1 lesson) — and, unlike v1,
does so off signals that are already correctly scoped rather than a dotted-path membership test.

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** ⟳ | **WS token transport = `Sec-WebSocket-Protocol` subprotocol header**, value `["civitas.bearer.<jwt>"]` (single pinned subprotocol string carrying the token as a suffix — no `?token=` query param, no second protocol-list entry). **WS-mTLS-via-ASGI-scope is dropped from v2 scope** (blocked on #25). | Query params leak into proxy/access logs and browser history; the subprotocol header is set by the WS handshake itself and isn't URL-visible. A single pinned string (vs. a `["civitas.bearer", "<jwt>"]` two-element list) avoids ambiguity about which list element is the token when a client offers multiple subprotocols. |
| **D2** ⚠️⟳ | **WS fails BEFORE accept, JWT check only.** Missing/invalid token → `send({"type": "websocket.close", "code": 4401})` — never send `websocket.accept`. On success, `websocket.accept` **echoes the negotiated subprotocol** (`civitas.bearer.<jwt>`) back per RFC 6455 — it must echo *a* subprotocol from the offered list, never omit `subprotocol` (which some clients treat as "none accepted") and never fabricate a different one. | No unauthenticated socket ever opens. `4401` mirrors HTTP 401 and the existing `4404` (unknown route) convention already in `asgi.py:222`. Echoing the actual negotiated value (not a sentinel) is required by the WS handshake spec — the token stays out of response *headers* (it was only ever in the request), so echoing it back in the subprotocol ack is not a new leak. |
| **D3** ⟳ | **gRPC = one `grpc.aio.ServerInterceptor`**, `intercept_service(continuation, handler_call_details)`. Two mechanics fixes vs. v1: (1) **Health/Reflection carve-out (F4)** — if `handler_call_details.method.split("/")[1]` is `grpc.health.v1.Health` or `grpc.reflection.v1alpha.ServerReflection`, call `continuation(handler_call_details)` unmodified, no check. (2) **Handler-wrapping for mTLS (nice-to-have → required mechanic):** `handler_call_details` alone exposes only invocation metadata (enough for the JWT check); the peer certificate is only reachable via `context.auth_context()` **inside** the RPC handler. So when `client_cert_mode="required"`, the interceptor must call `continuation(...)` to get the real `RPCMethodHandler`, then wrap its handler function to check `context.auth_context()` before delegating — it cannot reject on `handler_call_details` alone for the mTLS case. JWT check still short-circuits on `handler_call_details.invocation_metadata` before even calling `continuation`. | Matches the issue's own proposal; `verify()` needs zero changes since it was already transport-agnostic. The carve-out prevents load-balancer health probes and reflection clients — which never carry a bearer token or a client cert intended for this check — from being locked out by a feature they don't know exists. |
| **D4** ⚠️⟳ | **gRPC mTLS is now fully wired, not just detected.** `GrpcServer` gains `tls_ca_cert: str \| None` and `client_cert_mode: str` constructor params (passed from `GatewayConfig`, same fields HTTP already uses). In `start()`, when `client_cert_mode == "required"`: load the CA bytes and call `grpc.ssl_server_credentials([(key, cert)], root_certificates=ca_bytes, require_client_auth=True)`. DN extraction: `context.auth_context()["x509_pem_cert"][0]` → **`_dn_from_pem(pem)`** (new shared helper in `mtls.py`: `x509.load_pem_x509_certificate(pem).subject.rfc4514_string()`) — the **same function** any future #25-unblocked HTTP/WS mTLS path must also call, so DN string format is guaranteed identical across transports by construction (F2), not by an unverified claim that two independent extraction paths happen to agree. `client_cert_mode == "optional"` is refused for gRPC at config time (D9/F5) rather than approximated. | Python's `grpc.aio` doesn't expose a ready-made "full subject DN" the way ASGI/uvicorn's spec *claims* to (but doesn't, per #25) — extracting via `cryptography` directly, through one shared function, removes the format-drift risk Oracle flagged instead of just asserting it away. |
| **D5** ⟳ | **Shared predicates, not shared HTTP-shaped functions.** `mtls.py` now exports two transport-agnostic helpers: `_check_dn(dn: str \| None, allowed: frozenset[str]) -> None` (raises on failure, unchanged from v1) and **`_dn_from_pem(pem: bytes) -> str`** (new, F2). HTTP/gRPC each build their own request-shaped call around these two shared primitives + `JwtVerifier.verify()`; each transport keeps its own idiomatic error mapping (`GatewayResponse` / `websocket.close` / `context.abort`). WS does not use `_check_dn`/`_dn_from_pem` in v2 (mTLS out of scope for WS). | Avoids copies of "is this DN in the allowlist" and "how do I turn a cert into a DN string" drifting apart across transports — now two shared functions instead of one, since v1 only extracted the predicate and left extraction implicit/per-transport. |
| **D6** ★⟳ | **Auto-inherit keys directly on verifier/mode presence, not a middleware-path list.** WS enforcement turns on iff `self._jwt_verifier is not None`. gRPC JWT enforcement turns on iff `self._jwt_verifier is not None`; gRPC mTLS enforcement turns on iff `self._gw_config.client_cert_mode != "none"`. `self._jwt_verifier` is already built (`core.py:169-170`) from `_JWT_MIDDLEWARE_PATH in self._configured_middleware_paths()` — global **and** route-scoped — so this fix is "read the field that's already scoped correctly," not new detection logic. | Repeats the R3 M1 lesson: auth silently not applying is a bug, not a feature. v1's `_AUTH_MIDDLEWARE_PATHS` reuse was ambiguous about which of `core.py`'s two differently-scoped helper methods it meant, and either choice had a real bug (global-only misses route-scoped JWT; the frozenset also includes `require_api_key`, conflating an auth type WS/gRPC don't implement with the ones they do). Keying on the concrete objects (`_jwt_verifier`, `client_cert_mode`) sidesteps both problems and needs no new state. |
| **D7** ⚠️ | **Residual gap, documented not silently dropped: `require_api_key` is explicitly OUT of scope** for WS/gRPC (matches the issue text — "reuse `JwtVerifier` and the DN allowlist"). An API-key-only deployment (no JWT/mTLS) still gets **unauthenticated WS/gRPC** after this change. **v2 note:** D6's rewrite removes the false-hardening risk Oracle flagged (an api-key-only deploy's `_AUTH_MIDDLEWARE_PATHS` membership no longer has any bearing on WS/gRPC auto-inherit, since D6 no longer reads that frozenset at all) — the gap is now correctly detected as "unenforced" rather than silently miscategorized as "hardened." A startup WARNING for this specific combination remains a nice-to-have; see Q4 (§9). | Honesty over false completeness. Tracked as an explicit fast-follow in §8, not hidden — the whole point of this issue is not leaving silent gaps. |
| **D8** ⚠️⟳ | **Failure mapping, extended with a misconfig tier (F6):** WS → close `4401` (bad/missing token) or **close `1011`** (RFC 6455 "Internal Error" — reserved for a WS-side misconfig; not reachable today since `JwtVerifier` construction already raises `ConfigurationError` at startup on any bad JWT config, so there is no runtime "enforcement on but broken" state for WS yet, but the code path exists for symmetry with gRPC's `INTERNAL` and for whatever WS-mTLS misconfig #25 eventually introduces). gRPC → `UNAUTHENTICATED` (bad/missing JWT metadata, checked in-process by the interceptor) or `PERMISSION_DENIED` (valid cert, DN not in allowlist) or **`INTERNAL`** (enforcement auto-triggered by `client_cert_mode != "none"` but `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` is empty — mirrors HTTP's existing 500 for the identical misconfig, `mtls.py:47-51`). **Correction from v1:** under `require_client_auth=True` (the only mode gRPC mTLS supports, D9), a client presenting **no certificate never reaches the interceptor at all** — the TLS handshake itself fails (connection reset / TLS alert), not a gRPC status code. Only "valid cert, wrong DN" reaches `PERMISSION_DENIED`; v1's test-plan wording ("`UNAUTHENTICATED` (or handshake failure...)") is resolved definitively to *always* handshake failure for the no-cert case. | Matches issue text; consistent with existing HTTP semantics; closes the "loud misconfig, never allow-all" invariant gap Oracle flagged (F6) and removes the v1 ambiguity about which failure mode the missing-cert case actually produces. |
| **D9** ★ (NEW, F5) | **`client_cert_mode="optional"` is refused for gRPC at config/startup time** (`ConfigurationError`, raised in `HTTPGateway.on_start` when `grpc_enabled and client_cert_mode == "optional"`): "gRPC does not support client_cert_mode='optional' (grpc.aio has no CERT_OPTIONAL equivalent — require_client_auth is a single bool for the whole port); use 'required' or 'none' for a gateway with grpc_enabled." Since `client_cert_mode` is one field shared by both surfaces, this means `"optional"` is simply incompatible with `grpc_enabled=True`, full stop — it remains valid for HTTP-only gateways. | Python's `grpc.aio.ssl_server_credentials` `require_client_auth` is binary — there's no way to accept-but-not-require a client cert at the gRPC transport level the way uvicorn's `CERT_OPTIONAL` does for HTTP. Silently treating "optional" as "required" (stricter than configured) or as "none" (weaker than configured) are both surprising; refusing to start is the only option consistent with "loud misconfig, never guess." |
| **D10** ★ (NEW, F7) | **Insecure-gRPC-port + JWT-auto-inherit guard.** At `on_start`, if `grpc_enabled` and JWT auto-inherit would apply (`self._jwt_verifier is not None`) and the gRPC port has no TLS configured (`not (tls_cert and tls_key)`, i.e. `GrpcServer.start()` would call `add_insecure_port`), raise `ConfigurationError`: "JWT auth cannot be enforced over an insecure (plaintext) gRPC port — the bearer token would be sent in cleartext metadata; configure tls_cert/tls_key for the gRPC surface, or disable JWT for this gateway." | A JWT bearer token sent as gRPC invocation metadata over `add_insecure_port` is trivially interceptable on the wire — enforcing "auth" that ships the credential in the clear is worse than no enforcement (false sense of security). Matches R3's existing philosophy of refusing silently-broken security configurations rather than starting anyway. |
| **D11** (NEW, maintainer sign-off Q5) | **Startup WARNING: mTLS-only + WS routes configured.** In `on_start`, if `self._gw_config.client_cert_mode != "none"` and `self._jwt_verifier is None` and `self._gw_config.ws_routes` is non-empty, `logger.warning(...)`: "client_cert_mode is set but no JWT verifier is configured; WS routes %r will be served with NO authentication (WS mTLS is not yet supported — see #25/#17)." Non-fatal — WS still serves, matching D1/D2's documented scope cut; this only makes the gap loud instead of silent. | The maintainer flagged this as the most likely real-world way an operator ends up with silently-open WS in v2: they configured *some* gateway auth (mTLS) and reasonably assume it covers "the gateway," exactly the R3 M1 failure shape this whole issue exists to close for HTTP/gRPC — WS still has an uncovered corner until #25, so it gets a log line instead of nothing. |

★ pivotal · ⚠️ needs careful review · ⟳ revised from v1

## 5. Threat model

- **Downgrade risk (WS):** a client that can't/won't send the subprotocol header degrades to the
  *old* unauthenticated behavior only if D6's detection is bypassed — mitigated by D2 (fail-closed on
  missing token, same as HTTP's existing "no allowlist configured → 500, never allow-all" pattern).
- **WS-mTLS gap (v2, explicit):** a gateway configured with `client_cert_mode != "none"` and no JWT
  gets **zero WS enforcement** — the mTLS check that HTTP/gRPC get does not extend to WS until #25
  ships a working client-cert delivery mechanism. This is a known, documented gap (§8), not a silent
  one; see Q5 (§9) for whether it warrants an additional startup warning.
- **gRPC health/reflection carve-out:** intentionally unauthenticated by design (D3/F4) — this is a
  small, well-known attack surface (service names and health status), not request routing to agent
  logic. Operators who don't want reflection exposed at all already have `grpc_reflection=False`
  (existing `GatewayConfig` field); the auth interceptor is not the right control point for that
  decision, and adding one would be redundant with a knob that already exists.
- **mTLS trust anchor:** same operator responsibility as R3 — `GatewayConfig.tls_ca_cert` must be a
  dedicated private CA (documented already in `mtls.py`'s module docstring); this design reuses that
  same config for gRPC's `root_certificates` (D4), not a new one.
- **No-cert TLS-handshake failure (gRPC, D8 correction):** under `require_client_auth=True`, a
  certless client is rejected by the TLS layer, before any application code — including our
  interceptor — runs. This is a stronger, not weaker, guarantee than an app-layer `UNAUTHENTICATED`
  (the connection never completes at all), but means such failures won't appear in gRPC-level
  status-code metrics; operators should watch TLS handshake failure counters separately.
- **D7 residual:** explicitly called out above — API-key-only deployments remain unauthenticated on
  WS/gRPC after this ships, and (v2) are now *correctly* detected as such rather than risking
  misclassification as hardened.

## 6. Config

No new YAML keys. `CIVITAS_JWT_*` and `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` (existing, R3) are read by
the new WS/gRPC enforcement points exactly as they are by the HTTP middleware today.
`GatewayConfig.tls_ca_cert` and `GatewayConfig.client_cert_mode` (existing fields, previously
HTTP-only) now also flow into `GrpcServer` (D4). Two new startup-time `ConfigurationError`s are
introduced (D9, D10) — both are refusals to start, never silent behavior changes, consistent with
every other misconfig check in `GatewayConfig.__post_init__` / `HTTPGateway.on_start`.

## 7. Test plan (outline)

- WS: missing/invalid/valid token via subprotocol header → close(4401) / close(4401) / accept
  (echoing the negotiated subprotocol, not omitting it — D2).
- WS: no HTTP auth middleware configured (no JWT verifier) → unchanged (still open), proving D6
  doesn't regress the default.
- WS: mTLS-only gateway (no JWT), WS route configured → WS stays open **and** `on_start` logs the
  D11 warning (assert both: the documented gap and the loud-not-silent mitigation).
- gRPC: missing/invalid/valid JWT metadata → `UNAUTHENTICATED` / `UNAUTHENTICATED` / dispatched.
- gRPC: `client_cert_mode="required"`, no client cert → **TLS handshake fails** (connection-level,
  not a gRPC status — assert the channel never completes, not that a specific status code is
  returned) / wrong DN → `PERMISSION_DENIED` / correct DN → dispatched.
- gRPC: `client_cert_mode="required"`, `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` empty → `INTERNAL` (F6).
- gRPC: `client_cert_mode="optional"` + `grpc_enabled=True` → `ConfigurationError` at startup (D9).
- gRPC: JWT auto-inherit + no `tls_cert`/`tls_key` on the gRPC port → `ConfigurationError` at
  startup (D10).
- gRPC: `Health.Check` and `ServerReflectionInfo` RPCs succeed with **no** token/cert even when JWT
  and/or mTLS enforcement is active on `Agent` RPCs (D3/F4 carve-out).
- gRPC: DN extraction round-trip test — `_dn_from_pem` against a real cert fixture produces a string
  that exact-matches an allowlist entry populated from the same cert's PEM via the identical helper
  (validates D4/F2's "same function" guarantee directly, not just by inspection).
- `_dn_from_pem` unit test: same PEM in, same DN string out, called from two call sites (simulating
  gRPC + a stubbed future HTTP/WS caller) — byte-for-byte identical (F2 regression guard).
- Existing G2/G3 WS/gRPC suites stay green when no auth is configured (regression guard for D6).

## 8. Non-goals / fast-follows

- `require_api_key` on WS/gRPC (D7) — tracked as a fast-follow, not silently dropped.
- **WS mTLS — non-goal pending [#25](https://github.com/civitas-io/python-civitas/issues/25).** Once
  #25 lands a working client-cert delivery mechanism (proxy header forwarding, a custom uvicorn
  protocol subclass, or a hypercorn migration), WS mTLS should reuse this design's `_check_dn` +
  `_dn_from_pem` (D5) rather than re-deriving DN extraction from whatever #25 ships.
- Per-route (as opposed to gateway-global) WS/gRPC auth — out of scope; HTTP's route-scoped
  middleware doesn't have a WS/gRPC equivalent concept to hang off of yet.
- `_AUTH_MIDDLEWARE_PATHS` string-match fragility beyond exact dotted paths (v1 nice-to-have,
  untouched by this design since D6 no longer depends on it for WS/gRPC — it still backs the
  HTTP-only `/docs` auto-off default in `core.py:99-102`, unchanged).
- Startup WARNING for api-key-only + WS/gRPC enabled (D7) — deferred; see §9 Q4. (The mTLS-only +
  WS-routes case is **not** deferred — see D11.)

## 9. Resolved decisions (maintainer sign-off — 2026-07-06)

- ✅ **Q1 — D9 stands as specified:** `client_cert_mode="optional"` + `grpc_enabled` refuses to start.
- ✅ **Q2 — D10 stands as specified:** JWT auto-inherit over a plaintext gRPC port is a hard
  `ConfigurationError`, not a warning.
- ✅ **Q3 — D3's carve-out exempts both `Health` and `Reflection`.**
- ✅ **Q4 — D7's api-key-only startup warning is deferred** (separate fast-follow, not part of this
  design's implementation scope).
- ✅ **Q5 — Added as D11:** `on_start` emits a `logger.warning` when mTLS-only auth (no JWT) is
  configured alongside non-empty `ws_routes`.
- ✅ **Q6 — Diagram regenerated** (`docs/assets/gateway-ws-grpc-auth.svg`) to reflect v2's mechanism
  (JWT-only WS, verifier/mode-keyed D6, Health/Reflection carve-out, D9/D10/D11 guards).

## 10. References

- GH #17; GH #25; `docs/design/gateway-auth.md` (R3);
  `civitas/gateway/{jwt_auth,mtls,asgi,grpc_server,core}.py`.
