# Gateway Auth — JWT + mTLS (v0.7.0 · R3)

**Status:** ✅ Approved — Oracle-reviewed; maintainer signed off 2026-07-04 (go with recommendations). Implementation in progress.
**Source:** v0.6.0 §G5 (API-key shipped; "JWT + mTLS remain integration points")
**Related:** [`gateway-api-surface.md`](gateway-api-surface.md), [`security-hardening.md`](security-hardening.md), [`http-gateway.md`](http-gateway.md)
**Roadmap:** [`milestones.md`](../milestones.md) v0.7.0 R3

> **v2 changelog (Oracle review — code inspection found fail-open holes):** Added **M1** (the middleware
> loader silently drops a middleware it can't load → serves unauthenticated; must be **fatal**); **M2**
> `PyJWKClient` blocks the event loop (wrap in `to_thread`, one client at startup); **M3** `exp`/`aud`/`iss`
> are only checked *if present* unless `options={"require":[...]}` — tokens without `exp` never expire;
> **M4** WebSocket / gRPC / `/docs` **bypass the chain** — enumerate as the security boundary; **M5** fixed
> the 401/403 matrix (aud/iss → 401); **M6** add `GatewayRequest.auth` so authN can feed authZ; **M7**
> mTLS config preconditions; **M8** dedicated-CA mandate + full-DN exact match. Plus: token-size cap,
> leeway cap, rate-limit before JWT, `WWW-Authenticate` on 401, never read identity from headers.

---

## 1. Problem

G5 shipped first-party **API-key** gateway auth. JWT and mTLS were deferred. R3 makes both first-party:
**JWT bearer verification** (opt-in `civitas[jwt]`) and **mTLS client-cert auth** — and closes a fail-open
gap that would otherwise undermine all three.

## 2. Current behavior (ground truth, line refs)

- **Middleware model** (`gateway/types.py`): `(GatewayRequest, NextMiddleware) -> GatewayResponse`; short-circuit by returning a `GatewayResponse`. `require_api_key` (`gateway/auth.py:29-41`) is the template (NOT Starlette). Chain built in `middleware.py:build_chain`, run in `asgi.py` (~305).
- **⚠️ Loader fails open** (`asgi.py:128-132` global, `:148-159` per-route): `except Exception: logger.exception(...)` then **continues without the middleware**. A security middleware that fails to import/construct is silently dropped → gateway serves **unauthenticated**.
- **`GatewayRequest`** (`types.py:13-24`): `method,path,path_params,query_params,headers,body,client_ip,gateway` — no TLS field, no auth/claims field.
- **TLS** (`gateway/core.py`): uvicorn gets `ssl_certfile/ssl_keyfile` only (no `ssl_cert_reqs`/`ssl_ca_certs`). HTTP/3 (`h3.py`) server-cert only; **aioquic hardcodes client certs off**.
- **Bypass paths:** WebSocket (`asgi._handle_websocket`, ~186-230) never builds the chain; gRPC (`grpc_server.py`) dispatches straight to the bus; `/docs`+`/openapi.json` served (~249-256) **before** the chain (~307).
- **Config** (`config.py`): `Settings`+`SecretStr`. **Extras** (`pyproject.toml`): `http/http3/grpc/security/fast`. **Security dataclasses** (`security/config.py`): `from_dict`+`build_ssl_context`. **Errors**: no `AuthError`; return `GatewayResponse(401/403/500)`. `request.headers` is lowercased (`asgi.py:244`). `ratelimit.py` middleware exists.

## 3. Goals / Non-goals

**Goals:** first-party JWT + mTLS gateway middleware, secure-by-default, fail-closed; make middleware-load
failures fatal (M1); minimal/opt-in deps.

**Non-goals (explicit security boundary — M4):** token *issuance* / auth server; OAuth2/OIDC flows; HTTP/3
mTLS (aioquic can't); **and R3 auth does NOT cover WebSocket routes, the gRPC surface, or `/docs`+`/openapi.json`** — these bypass the ASGI middleware chain. Documented + a follow-up issue filed for WS/gRPC auth. **Never** derive client identity from request headers (a forwarded `X-SSL-Client-*` is client-spoofable).

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **M1** ⚠️ | **Middleware-load failures are FATAL at startup.** `on_start` (not construction) resolves the chain; a failed `load_middleware` (missing PyJWT, `ConfigurationError`, typo) **crashes the gateway** instead of being logged-and-skipped. | Today's fail-open silently serves unauthenticated. This is the enabling correctness fix for the whole feature. |
| **D1** | JWT lib = **PyJWT[cryptography]≥2.13**, opt-in `jwt = ["pyjwt[cryptography]>=2.13"]`, lazy import w/ `ConfigurationError`. | Best-maintained; algorithm-confusion defense built in; 2.13 JWKS cache fix. |
| **D2** | `require_jwt`: `Authorization: Bearer` (parse case-insensitively off lowercased `authorization`); explicit `algorithms` (**default `["RS256"]`**); **`options={"require":["exp","iss","aud"]}`** (M3) + `verify_aud/iss`; bounded `leeway` (**cap ≤120s**); **cap token length ~8KB** pre-decode. JWKS via `PyJWKClient` **or** static key — exactly one source. | M3: PyJWT doesn't require claims unless told; a token w/o `exp` never expires. |
| **M2** | **Verifier built + validated eagerly in `on_start`** (one `PyJWKClient`); the per-request signing-key lookup is wrapped in **`asyncio.to_thread`** (blocking urllib). | Don't stall the event loop on JWKS refetch; "refuse to start" must mean startup, not first-request 500. |
| **D3** | Threat rules (§6): reject `alg=none`; no RS/HS mixing (enforced at config); `aud`+`iss` required; `https://` JWKS only; order `rate_limit` **before** `require_jwt` (throttle unknown-`kid` JWKS refetch amplification). | The JWT footguns, encoded not left to users. |
| **D4** | **mTLS plumbing:** add `GatewayRequest.client_cert: dict\|None=None`, populated in `asgi.py` from `scope["extensions"]["tls"]` **only when a leaf is present** (else `None`). `require_client_cert` authorizes on the **full Subject DN, exact match** (not CN substring) against an allowlist; fails **closed** on `None`. SAN parsing deferred (needs only `cryptography`, no new server plumbing). | Middleware has no TLS access today; one optional field bridges it. Full-DN exact avoids CN spoofing. |
| **M8** | **Dedicated private CA is mandatory** for client auth: `ssl_ca_certs` is the trust anchor — a public/broad CA means *any* cert it signed passes authN and the DN allowlist becomes the sole (spoofable) gate. Document loudly. | TLS proves "signed by a trusted CA," not "is this identity." |
| **D5/M7** | `GatewayConfig`: `tls_ca_cert`, `client_cert_mode: "none"\|"optional"\|"required"`. `core.py` passes `ssl_ca_certs` + `ssl_cert_reqs`. `__post_init__` (raise **`ConfigurationError`**): reject unknown mode; `mode!=none ⇒ tls_ca_cert AND tls_cert+tls_key set`; **`mode!=none` + `enable_http3` ⇒ error** (aioquic can't enforce client certs → silent bypass). `required`=TLS-layer authN of the chain (still needs the middleware for DN authZ); `optional`=per-route, middleware fails closed. | Honest about the h3 gap; preconditions prevent misconfig. |
| **M6** | Add **`GatewayRequest.auth: dict\|None=None`**; `require_jwt` sets verified claims, `require_client_cert` sets cert info. Do **not** merge into the dispatched `payload` (reserved-key collisions). AuthZ (scopes/`sub`) can stay deferred, but the field ships now. | Without it, authN can't feed authZ — a dead end. |
| **M5/D8** | **Status matrix** (RFC 6750): missing/malformed/expired/bad-sig/**bad-aud/bad-iss** JWT → **401** (+ `WWW-Authenticate: Bearer error="invalid_token"`); missing cert (optional) → **401**; valid cert DN not allowlisted → **403**; valid token, insufficient scope → 403; misconfig → 500. Never log token/cert contents (log `sub`/`kid`/DN at most). | Fixes the D2/D8 contradiction; standard semantics. |
| **D6** | Composable; recommended order `rate_limit → mTLS → JWT → API-key`. A middleware is a no-op only if **not added to the chain**; a listed-but-unconfigured verifier → 500 (fail-closed, after M1). | Users pick any subset. |
| **D7** | `settings`: `CIVITAS_JWT_*` (jwks_url, audience, issuer, algorithms, public_key/secret `SecretStr`); `GatewayAuthConfig.from_dict` (mirrors `NatsTlsConfig`). | Consistent config surface. |

## 5. Threat model (condensed)

**JWT:** explicit `algorithms` + `require:[exp,iss,aud]` + `verify_aud/iss` (M3/D2); reject `alg=none`, no RS/HS mixing; **PyJWT does not fetch keys from `jku`/`x5u`/`jwk`/`x5c` headers** (confirmed — `kid` is only a selector into the trusted set) → add a regression test that such tokens are ignored; JWKS URL is operator-config (not SSRF) but pin `https://`; **no revocation before `exp`** → keep `exp` short (document); cap token size + leeway; rate-limit before JWT to blunt unknown-`kid` refetch amplification.

**mTLS:** cert verification is fatal at the TLS layer under `CERT_REQUIRED` (bad cert never reaches ASGI); under `CERT_OPTIONAL`, `require_client_cert` must reject `None`; only the **leaf** is exposed → authorize on issuer-anchored full DN with a **dedicated CA** (M8); HTTP/3 has no client-cert enforcement → **fail-loud at config** (M7); `alt-svc` advertises h3 whenever enabled, so under `optional`+h3 an mTLS-protected route is unreachable over h3 (fail-closed but confusing — document).

## 6. Resolved decisions (maintainer sign-off — 2026-07-04: "go with recommendations")

1. ✅ Add opt-in `civitas[jwt] → pyjwt[cryptography]>=2.13`.
2. ✅ **M1**: gateway middleware-load failures are **FATAL** (fixes the live fail-open auth-bypass). **Folded into this R3 PR** (not a separate fast-track).
3. ✅ **M4**: R3 covers HTTP request routes only; **WebSocket, gRPC, `/docs` are NOT protected** — documented boundary; a follow-up issue is filed for WS/gRPC auth; `docs_enabled` defaults to `False` when any gateway auth middleware is configured.
4. ✅ JWT supports **both** JWKS URL and static key (recommend RS256+JWKS).
5. ✅ `client_cert_mode` (`none` default; `optional`+middleware; `required`); DN-allowlist now, SAN deferred.
6. ✅ Scope: verification-only; mTLS on uvicorn only (fail-loud on http3); never trust forwarded identity headers.

## 7. Test plan (outline)

- **M1:** a middleware that raises on load → gateway `on_start` **crashes** (no silent unauthenticated serving).
- **JWT:** valid RS256 (JWKS + static) → 200, claims on `request.auth`; expired / missing-`exp` (M3) / `nbf`-future → 401; bad `aud`/`iss` → **401**; `alg=none` + RS/HS-confusion + `jku`/`x5c`-header tokens → 401/ignored; oversized token → 401; missing/malformed header → 401 (+`WWW-Authenticate`); unconfigured → 500; JWKS `kid` rotation refreshes; `pyjwt` absent → `ConfigurationError` at startup (M1); JWKS lookup offloaded (no event-loop block).
- **mTLS:** `asgi.py` sets `client_cert` from a simulated TLS scope; **plus a real-uvicorn TLS integration test** (verifies the extension is actually emitted); allowlisted DN → 200, unlisted → 403, absent under `optional` → 401; `core.py` passes `ssl_cert_reqs`/`ssl_ca_certs`; `mode!=none`+`enable_http3` → `ConfigurationError`; `mode!=none` without `tls_ca_cert` → `ConfigurationError`.
- **Compose:** rate_limit→mTLS→JWT→API-key; `request.auth` populated; order preserved.

## 8. Implementer checklist

- **M1:** move global middleware resolution into `on_start`; make load failures propagate (fatal). Keep per-route resolution but fail fatally too (or resolve all at startup).
- `gateway/jwt_auth.py` (`require_jwt` + `JwtVerifier`: eager init, one `PyJWKClient`, `to_thread` lookup, `require`/`leeway`/size caps); `gateway/mtls.py` (`require_client_cert`, full-DN exact, fail-closed).
- `GatewayRequest.client_cert` + `GatewayRequest.auth` (types.py); populate `client_cert` in `asgi.py` from the TLS ext.
- `GatewayConfig`: `tls_ca_cert`, `client_cert_mode`; `core.py` `ssl_ca_certs`/`ssl_cert_reqs`; `__post_init__` preconditions (`ConfigurationError`).
- `config.py` `CIVITAS_JWT_*`; `GatewayAuthConfig.from_dict`; `pyproject.toml` `jwt` extra.
- Docs: security-boundary section (WS/gRPC/`/docs` not covered), dedicated-CA mandate, `docs_enabled=False` in prod; `CHANGELOG`; `AGENTS.md` install matrix. File follow-up issue: WS + gRPC auth.
