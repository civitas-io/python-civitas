"""TlsAwareHttpToolsProtocol — closes the `direct`-mode half of GH #25.

uvicorn never exposes the client certificate from its own TLS handshake to
the ASGI app (uvicorn#400 — https://github.com/encode/uvicorn/issues/400):
none of its HTTP protocol implementations populate
``scope["extensions"]["tls"]``, the shape
:func:`civitas.gateway.asgi._client_cert_from_scope` already reads. This
subclass supplies exactly that, using nothing but the standard library's
own, already-available TLS introspection — no private uvicorn API, no
monkeypatching.

See docs/design/gateway-http-mtls-direct.md for the full design and the
empirical verification this mechanism was checked against before writing
any of this (a minimal, real asyncio TLS server proving
``ssl_object.getpeercert(binary_form=True)`` returns the real peer
certificate DER, byte-identical to the certificate the client actually
presented).

Only wired in by :class:`civitas.gateway.core.HTTPGateway` when
``client_cert_mode != "none"`` **and** ``mtls_source == "direct"`` — a
plaintext gateway or a ``proxy_header`` deployment gets uvicorn's ordinary
default protocol, completely unaffected by this module existing.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

from civitas.gateway.mtls import _dn_from_der

if TYPE_CHECKING:
    import ssl


class TlsAwareHttpToolsProtocol(HttpToolsProtocol):
    """``HttpToolsProtocol`` that populates the ASGI TLS extension from the
    real peer certificate uvicorn's own TLS transport already has, but
    never forwards.

    ``httptools`` is the concrete implementation this repo's dependency set
    resolves ``uvicorn.Config(http="auto")`` to (confirmed:
    ``uvicorn.config.HTTP_PROTOCOLS["auto"]`` picks it when installed,
    which it always is here) — see docs/design/gateway-http-mtls-direct.md
    D1 for why only this implementation is subclassed, not ``H11Protocol``.
    """

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore[override]
        super().connection_made(transport)
        # A connection's peer certificate cannot change mid-connection (TLS
        # renegotiation is disabled by default and civitas does not enable
        # it) -- captured once here, not re-fetched per request (D4).
        self._civitas_ssl_object: ssl.SSLObject | None = transport.get_extra_info("ssl_object")

    def on_message_begin(self) -> None:
        super().on_message_begin()
        ssl_object = getattr(self, "_civitas_ssl_object", None)
        if ssl_object is None:
            return  # plaintext connection -- nothing to add
        der = ssl_object.getpeercert(binary_form=True)
        if not der:
            return  # CERT_OPTIONAL with no cert presented; CERT_REQUIRED
            # never reaches here without one -- the TLS handshake itself
            # already refused the connection in that case.
        self.scope["extensions"] = {
            "tls": {"client_cert_chain": [der], "client_cert_name": _dn_from_der(der)}
        }


def build_tls_aware_http_kwarg() -> dict[str, Any]:
    """Return the ``uvicorn.Config`` kwarg that wires in
    :class:`TlsAwareHttpToolsProtocol` -- a single, named seam so
    ``civitas/gateway/core.py`` states the *intent* ("use the TLS-aware
    protocol") rather than repeating the class reference inline.
    """
    return {"http": TlsAwareHttpToolsProtocol}
