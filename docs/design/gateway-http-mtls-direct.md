# HTTP mTLS in Direct Mode — closing the other half of #25

**Status:** 🚧 In progress — design finalized, implementation starting this session.
**Source:** [GH #25](https://github.com/civitas-io/python-civitas/issues/25) (originally closed by
`mtls_source="proxy_header"`, `gateway-http-mtls-proxy.md`, v0.7.3 — that design explicitly left
`direct` mode "unchanged, still non-functional," see its own §1/Milestones R9 row); surfaced again
2026-08-23 from `civitas-io/presidium`, which needed direct-mode mTLS to actually work for a
self-hostable, single-process governance server (no reverse proxy required) and found — the same
day, independently, before finding this doc's own prior art — that `require_client_cert` never
succeeds against a real uvicorn deployment. See `civitas-io/presidium/docs/design/presidium-server.md`
and its own roadmap's M7 section for the consuming side of this work.
**Builds on:** `civitas/gateway/mtls.py`'s existing `_client_cert_from_scope()`, `_dn_from_der()`,
`_check_dn()` — all correct and unchanged; the gap is entirely on uvicorn's side of the boundary,
not civitas's authorization logic.

## 1. Problem

`docs/gateway.md`'s own "HTTP mTLS via a reverse proxy" section already documents the root cause
precisely: uvicorn never exposes the client certificate from its own TLS handshake to the ASGI
app ([uvicorn#400](https://github.com/encode/uvicorn/issues/400)). `civitas.gateway.asgi.
_client_cert_from_scope()` reads `scope["extensions"]["tls"]["client_cert_chain"]` — the
documented, spec-shaped ASGI TLS extension — but nothing in uvicorn's `HttpToolsProtocol`/
`H11Protocol` ever populates it. Confirmed empirically this session, not just from the docs:
`grep`-ing uvicorn's entire installed source tree for `getpeercert`/`extensions`/`tls` returns
zero matches in any protocol implementation.

**Concrete, live consequence, reproduced end to end**: a real self-signed CA, a real server leaf,
a real client leaf signed by that CA, `client_cert_mode="required"` — the TLS handshake itself
succeeds (uvicorn's `ssl_cert_reqs=CERT_REQUIRED` does work; a client presenting no cert or a
cert from an untrusted CA is correctly refused at the transport layer). But `require_client_cert`
still returns `401 {"error": "client certificate required"}` for the fully valid, correctly
signed, allowlisted client — because `request.client_cert` is always `None`. **mTLS in `direct`
mode currently locks out every legitimate client, not just illegitimate ones** — worse than
"theater," a real functional dead end for anyone who can't or doesn't want to run a
TLS-terminating reverse proxy in front of civitas.

`mtls_source="proxy_header"` (R9, v0.7.3) is a real, working fix — but it requires a materially
different deployment topology (a real reverse proxy doing real TLS termination). Presidium's own
M7 milestone wants a genuinely self-hostable, single-process server; forcing a mandatory proxy
dependency onto every deployment to get mTLS at all is a real, avoidable cost this design removes.

## 2. The mechanism, verified empirically before writing any implementation code

Standard Python `ssl`/`asyncio` already exposes exactly what's needed — uvicorn's ASGI layer just
never forwards it. Verified with a minimal, real asyncio TLS server (no civitas, no uvicorn) in
this session, not assumed from documentation:

```python
ssl_obj = transport.get_extra_info("ssl_object")
der = ssl_obj.getpeercert(binary_form=True)  # real DER bytes of the client's leaf cert
```

Confirmed: the DN extracted from `der` via civitas's own existing `_dn_from_der()` is
byte-identical to the DN of the certificate the client actually presented. This is the same
primitive `ssl.SSLSocket.getpeercert()` gRPC's own transport already relies on indirectly (via
`grpc`'s C-core) — Python's stdlib has always had this; uvicorn simply never wires it into the
ASGI scope it builds.

`uvicorn.Config.http` already accepts `type[asyncio.Protocol]` directly (not just the string
names `"h11"`/`"httptools"`/`"auto"`) — a real, existing, documented extension point, not a
private API being relied on. `httptools` is the concrete implementation `"auto"` resolves to in
this repo's dependency set (`uvicorn.config.HTTP_PROTOCOLS["auto"]` picks it when installed, which
it is here).

## 3. Design approach

A new module, `civitas/gateway/_tls_protocol.py`, defines `TlsAwareHttpToolsProtocol`, a thin
subclass of `uvicorn.protocols.http.httptools_impl.HttpToolsProtocol`:

- **`connection_made(transport)`**: call `super().connection_made(transport)`, then capture
  `transport.get_extra_info("ssl_object")` once per connection (`self._civitas_ssl_object`) — a
  plaintext connection has no SSL object, so this is `None` for non-TLS gateways and costs nothing.
- **`on_message_begin()`**: call `super().on_message_begin()` (builds the fresh per-request
  `self.scope` dict, unchanged), then, only if `self._civitas_ssl_object is not None`: call
  `getpeercert(binary_form=True)`; if it returns real bytes, set
  `self.scope["extensions"] = {"tls": {"client_cert_chain": [der], "client_cert_name":
  _dn_from_der(der)}}` — reusing the exact shared DN extractor gRPC's mTLS path already uses
  (`mtls.py`'s own `_dn_from_der`), so DN string format is guaranteed identical across every
  transport by construction, not by an unverified claim that two paths happen to agree (the same
  principle `gateway-ws-grpc-auth.md`'s D4 already established for HTTP vs. gRPC).
- **`HTTPGateway`'s own uvicorn `Config(...)` construction** (`core.py`, next to the existing
  `ssl_certfile`/`ssl_cert_reqs` lines): pass `http=TlsAwareHttpToolsProtocol` instead of leaving
  the default `"auto"` string, but **only** when `client_cert_mode != "none"` and
  `mtls_source == "direct"` — a plaintext gateway or a `proxy_header` deployment gets uvicorn's
  ordinary default protocol, completely unaffected by this change.

No change to `_client_cert_from_scope()`, `require_client_cert`, `_check_dn()`, or
`_dn_from_der()` — they already expect exactly this shape (confirmed by `_client_cert_from_scope`'s
existing, pre-#25 implementation); this closes the gap in what actually *delivers* the data they
already correctly consume.

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **Subclass `HttpToolsProtocol` specifically, not `H11Protocol` or a protocol-agnostic shim.** | `httptools` is the concrete implementation this repo's dependency set actually resolves to (`uvicorn.config.HTTP_PROTOCOLS["auto"]`); a dual-implementation shim adds real maintenance surface for a code path (`h11` fallback, used only when `httptools` isn't installed) this repo doesn't exercise. Revisit only if `httptools` is ever dropped as a dependency. |
| D2 | **Only swap the protocol class when `client_cert_mode != "none"` AND `mtls_source == "direct"`.** | Keeps the change fully inert for the two deployment shapes that don't need it (plaintext, `proxy_header`) — zero behavioral or performance change for any existing deployment. |
| D3 | **Reuse `mtls.py`'s existing `_dn_from_der()`, do not add a second DN-extraction path.** | Matches `gateway-ws-grpc-auth.md`'s own D4 principle: one shared function, so HTTP-direct, HTTP-proxy-header, and gRPC's independently-necessary extraction all agree on DN format by construction. |
| D4 | **Capture the SSL object once in `connection_made`, not on every request.** | A connection's peer certificate cannot change mid-connection (TLS renegotiation is disabled by default and civitas does not enable it) — matches HTTP/1.1 keep-alive's existing one-handshake-many-requests shape; avoids a redundant `get_extra_info()` call per request. |
| D5 | **No change to the "no certificate" or "wrong DN" failure paths.** | `require_client_cert`'s existing `_NoCertificate`/`_Forbidden`/`_MtlsMisconfigured` handling is correct today and untested only because it never had real data reaching it — this design supplies the missing data, not new authorization logic. |

## 5. Threat model

No new trust primitive is introduced. The trust anchor is unchanged: `tls_ca_cert` must be a
dedicated private CA (per `mtls.py`'s own long-standing module docstring) — this design does not
change what "signed by a trusted CA" means, only makes the already-correct DN-allowlist check
after that point actually reachable. `ssl.CERT_REQUIRED` (uvicorn's own, already-correct handling
of `client_cert_mode="required"`) continues to reject a missing or untrusted-CA certificate at the
TLS layer itself, before any of this design's code runs — confirmed empirically this session (see
§6 test plan) that this half was never actually broken; only the app-layer authorization on top of
a *successful* handshake was.

## 6. Test plan

Mirrors the four real handshake scenarios `civitas-io/presidium` wrote (and found this gap with)
in `packages/presidium-contrib/tests/integration/test_presidium_server_mtls.py`, run here against
a real `HTTPGateway` + real uvicorn directly (closing the gap in this repo, not just downstream):

1. Trusted client cert (signed by the configured CA), DN in the allowlist → `200`.
2. Client cert signed by the **same** trusted CA, DN **not** in the allowlist → `403` (proves the
   TLS-trust layer and the DN-authorization layer are both real and distinct, not conflated).
3. No client certificate presented → TLS handshake itself fails (never reaches the ASGI app).
4. Client cert signed by a **different**, untrusted CA → TLS handshake itself fails.

Plus: a plaintext (no TLS) gateway and a `mtls_source="proxy_header"` gateway both continue
unaffected (protocol class unchanged in both cases) — regression coverage for D2.

## 7. Non-goals / fast-follows

- **WS mTLS via this mechanism** — `gateway-ws-grpc-auth.md` §8 deferred WS mTLS pending #25;
  this design closes the HTTP half. The same `scope["extensions"]["tls"]` shape is available to a
  WS upgrade handshake the identical way (uvicorn's WS protocol implementations would need the
  same treatment) — a natural, low-risk fast-follow, not bundled here to keep this change's own
  scope (HTTP direct-mode only) tight and reviewable, matching the precedent
  `gateway-http-mtls-proxy.md` §8 already set for its own scope cut.
- **`H11Protocol` support** — see D1; only needed if `httptools` stops being the resolved default.
- **HTTP/3** — unaffected; `enable_http3` is already, separately, incompatible with any
  `client_cert_mode` (aioquic cannot enforce client certs), unchanged by this design.

## 8. References

- [uvicorn#400](https://github.com/encode/uvicorn/issues/400) — the underlying upstream gap.
- [GH #25](https://github.com/civitas-io/python-civitas/issues/25) — this repo's tracking issue
  (reopened for the `direct`-mode half; `proxy_header` half already shipped, v0.7.3).
- `docs/design/gateway-http-mtls-proxy.md` — the `proxy_header` sibling design; both now exist
  side by side as two independently valid deployment modes for the same underlying authorization
  logic (`mtls.py`, unchanged by either).
- `civitas-io/presidium/docs/design/presidium-server.md` and its roadmap's M7 section — the
  consuming side that surfaced this gap in practice.
