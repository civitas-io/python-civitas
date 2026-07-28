"""Message signing over a REAL transport, with an actual message round trip
(v0.9.2.1 bugfix regression test).

This is the exact gap that let the bug ship silently: every existing signing
test (``tests/unit/test_transport_security.py``) only exercised YAML parsing
into ``SecurityConfig`` and Runtime.start()'s wiring flags
(``rt._signing_on``, ``isinstance(rt._serializer, SigningSerializer)``) — none
of them ever sent a real signed message and checked it actually arrived.
``ask()`` over a signing-enabled ZMQ (and NATS) transport silently timed out
with no exception anywhere, because ``ZMQTransport``/``NATSTransport`` each
held their own private, never-updated serializer reference for their
``request()`` method's internal reply_to round-trip (see
``civitas/transport/__init__.py``'s ``Transport.set_serializer`` docstring
for the full root cause).
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from civitas import AgentProcess, Runtime
from civitas.messages import Message
from tests.conftest import wait_for

pytest.importorskip("zmq", reason="pyzmq not installed — skipping signed-ZMQ tests")


class _Echo(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


class _Driver(AgentProcess):
    """Drives the demo's one signed ask() and stashes the result for the test
    to observe — mirrors examples/secured_messaging.py's own necessary
    pattern: `_runtime` (the sender identity `runtime.ask()` uses from
    outside any agent) never gets a signing identity by design, so a real
    agent must be the one asking."""

    result: dict | None = None
    error: Exception | None = None

    async def on_start(self) -> None:
        try:
            reply = await self.ask("echo", {"text": "signed end-to-end"}, timeout=5.0)
            _Driver.result = reply.payload
        except Exception as exc:  # noqa: BLE001 - captured for the test to assert on
            _Driver.error = exc

    async def handle(self, message: Message) -> Message | None:
        return None


def _build_config(transport: dict, key_dir: str, *, allow_unsigned: bool) -> dict:
    _Driver.result = None
    _Driver.error = None
    return {
        "transport": transport,
        "security": {
            "identity": {"mode": "auto", "key_dir": key_dir},
            "signing": {
                "enabled": True,
                "algorithm": "ed25519",
                "require_verification": True,
                "allow_unsigned": allow_unsigned,
            },
        },
        "supervision": {
            "name": "root",
            "children": [
                {
                    "type": "agent",
                    "module": "tests.integration.test_signed_transport",
                    "class": "_Echo",
                    "name": "echo",
                },
                {
                    "type": "agent",
                    "module": "tests.integration.test_signed_transport",
                    "class": "_Driver",
                    "name": "driver",
                },
            ],
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_unsigned", [True, False])
async def test_signed_ask_over_real_zmq_succeeds(allow_unsigned: bool) -> None:
    """The actual regression: a signed ask() must complete, not time out,
    over a real ZMQ transport, in both allow_unsigned modes."""
    key_dir = tempfile.mkdtemp(prefix="civitas-signed-zmq-test-")
    try:
        # Genuinely random free ports (not fixed) so the two parametrized runs
        # (allow_unsigned True/False) never collide, matching this repo's own
        # NATS-fixture precedent (G1, v0.8.2) for the exact same reason.
        with socket.socket() as s1, socket.socket() as s2:
            s1.bind(("127.0.0.1", 0))
            s2.bind(("127.0.0.1", 0))
            pub_port, sub_port = s1.getsockname()[1], s2.getsockname()[1]

        config = _build_config(
            {
                "type": "zmq",
                "pub_addr": f"tcp://127.0.0.1:{pub_port}",
                "sub_addr": f"tcp://127.0.0.1:{sub_port}",
                "start_proxy": True,
            },
            key_dir,
            allow_unsigned=allow_unsigned,
        )
        runtime = Runtime.from_config_dict(config)
        await runtime.start()
        try:
            assert runtime._signing_on is True

            await wait_for(
                lambda: _Driver.result is not None or _Driver.error is not None, timeout=5.0
            )

            assert _Driver.error is None, f"signed ask() failed: {_Driver.error!r}"
            assert _Driver.result == {"echo": "signed end-to-end"}
        finally:
            await runtime.stop()
    finally:
        shutil.rmtree(key_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# NATS — same regression, same fixture pattern as test_m2_2_nats.py
# ---------------------------------------------------------------------------


def _find_nats_server() -> str | None:
    return shutil.which("nats-server")


@pytest.fixture(scope="module")
def nats_server():
    binary = _find_nats_server()
    if binary is None:
        pytest.skip("nats-server not found on PATH")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        port = _s.getsockname()[1]

    proc = subprocess.Popen(
        [binary, "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            proc.terminate()
            pytest.skip("nats-server did not become ready in time")
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5.0)


@pytest.mark.asyncio
async def test_signed_ask_over_real_nats_succeeds(nats_server: int) -> None:
    """Same regression as the ZMQ test above, over NATS — NATSTransport has
    the byte-for-byte identical pre-fix serializer bug."""
    pytest.importorskip("nats", reason="nats-py not installed")
    key_dir = tempfile.mkdtemp(prefix="civitas-signed-nats-test-")
    try:
        config = _build_config(
            {"type": "nats", "servers": f"nats://127.0.0.1:{nats_server}"},
            key_dir,
            allow_unsigned=False,
        )
        runtime = Runtime.from_config_dict(config)
        await runtime.start()
        try:
            assert runtime._signing_on is True
            await wait_for(
                lambda: _Driver.result is not None or _Driver.error is not None, timeout=5.0
            )
            assert _Driver.error is None, f"signed ask() failed: {_Driver.error!r}"
            assert _Driver.result == {"echo": "signed end-to-end"}
        finally:
            await runtime.stop()
    finally:
        shutil.rmtree(key_dir, ignore_errors=True)
