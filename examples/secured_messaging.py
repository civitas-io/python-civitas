"""Message signing (SECURITY.md, design/security-hardening.md) — v0.9.2.

Requires: pip install civitas civitas[zmq,security]

Three parts:

1. Topology-level configuration — a real ``Runtime`` with a ``security:`` block
   over a ZMQ transport, proving the signing infrastructure actually activates
   (``runtime._signing_on``, keys generated on disk). The in-process transport
   skips signing entirely (same OS process, no wire to protect); a non-in-process
   transport is required for signing to wire up at all.

2. The actual cryptographic guarantee, demonstrated directly against the public
   ``AgentIdentity`` / ``KeyRegistry`` / ``MessageSigner`` primitives — sign a
   message, verify it (succeeds), tamper with it, verify again (fails).

3. A real, live, signed agent-to-agent ``ask()`` over a real ZMQ transport —
   until v0.9.2.1, this silently timed out with no exception anywhere
   (``ZMQTransport``/``NATSTransport`` each held their own private serializer
   reference, never updated when ``Runtime.start()`` activated signing, which
   corrupted every ``ask()``'s internal reply_to round-trip into a blank
   message). Fixed in v0.9.2.1 — see ``civitas/transport/__init__.py``'s
   ``Transport.set_serializer`` docstring for the full root cause. Driven
   from inside an agent's ``on_start()``, not from ``main()`` via
   ``runtime.ask()`` directly: signing requires every sender to have a
   keypair, and only real registered agents get one — ``runtime.ask()``'s
   sender is the bare, non-agent ``"_runtime"`` sink (H9, #33), which never
   has an identity by design.

Usage:
    python examples/secured_messaging.py
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path

from civitas import AgentProcess, Runtime
from civitas.errors import SignatureError
from civitas.messages import Message
from civitas.security.config import SigningConfig
from civitas.security.identity import AgentIdentity
from civitas.security.registry import KeyRegistry
from civitas.security.signing import MessageSigner


class EchoAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


class Driver(AgentProcess):
    """Sends the demo's one signed message and prints the result itself —
    see this file's module docstring (Part 3) for why main() can't drive
    this directly on a signing-enabled topology."""

    async def on_start(self) -> None:
        # Both agents start concurrently; a brief pause lets "echo"'s own ZMQ
        # subscription propagate through the PUB/SUB fanout before the first
        # request is sent (the "slow joiner" pattern).
        await asyncio.sleep(0.3)
        reply = await self.ask("echo", {"text": "this message was Ed25519-signed"}, timeout=5.0)
        print(f"Reply: {reply.payload['echo']!r}")


def demonstrate_signing_and_tamper_detection() -> None:
    """Part 2 — the actual guarantee, at the layer proven correct by this
    repo's own unit tests (tests/unit/test_security.py)."""
    identity_a = AgentIdentity.generate("agent-a")
    identity_b = AgentIdentity.generate("agent-b")

    registry = KeyRegistry()
    registry.register("agent-a", identity_a.verify_key)
    registry.register("agent-b", identity_b.verify_key)

    signer = MessageSigner(
        {"agent-a": identity_a},
        registry,
        SigningConfig(enabled=True, require_verification=True),
    )

    envelope = signer.sign(
        {"sender": "agent-a", "recipient": "agent-b", "payload": {"amount": 100}}
    )
    print("Signed envelope produced by 'agent-a'.")

    verified = signer.verify(envelope)
    print(f"Verification succeeded: {verified['payload']}")

    # A FRESH envelope (own nonce) for the tamper demo -- reusing the one just
    # verified above would trip nonce-replay protection instead of signature
    # verification, which is a real but DIFFERENT guarantee than the one this
    # is meant to show (a bug in this exact demo, caught by actually reading
    # the printed rejection reason instead of just checking "it raised").
    tampered_envelope = signer.sign(
        {"sender": "agent-a", "recipient": "agent-b", "payload": {"amount": 100}}
    )
    print("\nTampering with a freshly-signed payload (changing amount 100 -> 999)...")
    tampered_envelope["msg"]["payload"]["amount"] = 999
    try:
        signer.verify(tampered_envelope)
        print("  UNEXPECTED: tampered message verified anyway (this would be a bug)")
    except SignatureError as exc:
        print(f"  Correctly rejected: {exc}")


async def demonstrate_topology_wiring() -> None:
    """Part 1 — a real Runtime with security: configured over ZMQ, proving the
    infrastructure activates (config parsing + key generation)."""
    key_dir = tempfile.mkdtemp(prefix="civitas-secured-demo-")
    try:
        config = {
            "transport": {
                "type": "zmq",
                "pub_addr": "tcp://127.0.0.1:15559",
                "sub_addr": "tcp://127.0.0.1:15560",
                "start_proxy": True,
            },
            "security": {
                "identity": {"mode": "auto", "key_dir": key_dir},
                "signing": {"enabled": True, "algorithm": "ed25519"},
            },
            "supervision": {
                "name": "root",
                "children": [
                    # module: "__main__" (not a dotted examples.* path) -- resolves
                    # to THIS running script regardless of install mode or cwd,
                    # matching examples/dynamic_spawning.py's proven convention.
                    {"type": "agent", "module": "__main__", "class": "EchoAgent", "name": "echo"},
                ],
            },
        }
        runtime = Runtime.from_config_dict(config)
        await runtime.start()

        print(f"Signing enabled: {runtime._signing_on}")
        generated_keys = await asyncio.to_thread(lambda: list(Path(key_dir).glob("*/id_ed25519")))
        print(f"Ed25519 keys auto-generated for: {[p.parent.name for p in generated_keys]}")

        await runtime.stop()
    finally:
        shutil.rmtree(key_dir, ignore_errors=True)


async def demonstrate_live_signed_ask() -> None:
    """Part 3 — a real, live, signed agent-to-agent ask() over real ZMQ
    (fixed in v0.9.2.1 — see module docstring)."""
    key_dir = tempfile.mkdtemp(prefix="civitas-secured-demo-live-")
    try:
        config = {
            "transport": {
                "type": "zmq",
                "pub_addr": "tcp://127.0.0.1:15561",
                "sub_addr": "tcp://127.0.0.1:15562",
                "start_proxy": True,
            },
            "security": {
                "identity": {"mode": "auto", "key_dir": key_dir},
                "signing": {"enabled": True, "algorithm": "ed25519", "require_verification": True},
            },
            "supervision": {
                "name": "root",
                "children": [
                    {"type": "agent", "module": "__main__", "class": "EchoAgent", "name": "echo"},
                    {"type": "agent", "module": "__main__", "class": "Driver", "name": "driver"},
                ],
            },
        }
        runtime = Runtime.from_config_dict(config)
        await runtime.start()
        await asyncio.sleep(1.0)  # let Driver.on_start()'s settle-wait + ask() complete and print
        await runtime.stop()
    finally:
        shutil.rmtree(key_dir, ignore_errors=True)


async def main() -> None:
    print("=== Part 1: topology-level configuration ===")
    await demonstrate_topology_wiring()

    print("\n=== Part 2: the actual signing guarantee ===")
    demonstrate_signing_and_tamper_detection()

    print("\n=== Part 3: a real, live, signed ask() over real ZMQ ===")
    await demonstrate_live_signed_ask()


if __name__ == "__main__":
    asyncio.run(main())
