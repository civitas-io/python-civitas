"""Async HTTP client for polling a ``TopologyServer`` (v0.9.1, dashboard-v2 Phase E).

Deliberately NOT ``urllib``/``requests``/``httpx`` — this module is used from inside a
Textual ``@work`` background task sharing the app's own asyncio event loop. Phase D
found a real deadlock from exactly this mistake (a blocking ``urllib.request.urlopen()``
call inside an async test on the same loop the server needed to answer on) — see
``docs/design/dashboard-v2.md`` Phase D implementation notes. This client reuses the
same ``asyncio.open_connection``-based pattern the test suite already uses to talk to
``TopologyServer`` (``tests/unit/test_topology_server.py``'s ``_http_get`` helper),
promoted to a real, reusable, production module instead of a test-only helper.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any


class DashboardConnectionError(Exception):
    """Raised when a TopologyServer endpoint is unreachable or answers unexpectedly.

    Callers (the app's polling workers) catch this specifically to flip a
    "reconnecting…" state rather than letting the exception propagate and kill the
    poll loop — matches ``topology show``'s graceful-unreachable framing, but
    persistent instead of one-shot (design §7's polling-worker requirement).
    """


async def fetch_json(host: str, port: int, path: str, timeout: float = 3.0) -> tuple[int, Any]:
    """GET ``path`` from ``host:port`` and return ``(status_code, parsed_json)``.

    Raises :class:`DashboardConnectionError` for anything that means "this endpoint
    is not answering right now" (connection refused, timeout, malformed response) —
    never lets a raw ``OSError``/``TimeoutError``/``json.JSONDecodeError`` escape, so
    callers have exactly one exception type to catch.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
    except (OSError, TimeoutError) as exc:
        raise DashboardConnectionError(f"cannot connect to {host}:{port}: {exc}") from exc

    try:
        request = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
        writer.write(request.encode())
        await writer.drain()

        # TopologyServer always sends Connection: close and then closes its end
        # (civitas/topology_server.py's _handle_connection) — reading to EOF is
        # correct and simpler than parsing Content-Length, and matches the
        # existing test helper's approach for these small JSON bodies.
        chunks: list[bytes] = []
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(65536), timeout=timeout)
                if not chunk:
                    break
                chunks.append(chunk)
        except TimeoutError as exc:
            raise DashboardConnectionError(f"timed out reading from {host}:{port}") from exc
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    raw = b"".join(chunks)
    header_end = raw.find(b"\r\n\r\n")
    if header_end == -1:
        raise DashboardConnectionError(f"malformed HTTP response from {host}:{port}")
    headers_raw = raw[:header_end].decode(errors="replace")
    body = raw[header_end + 4 :]
    status_line = headers_raw.splitlines()[0] if headers_raw else ""
    parts = status_line.split()
    if len(parts) < 2 or not parts[1].isdigit():
        raise DashboardConnectionError(f"malformed status line from {host}:{port}: {status_line!r}")
    status_code = int(parts[1])

    try:
        parsed = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise DashboardConnectionError(f"invalid JSON from {host}:{port}{path}: {exc}") from exc

    return status_code, parsed
