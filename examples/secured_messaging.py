"""Message signing (SECURITY.md, design/security-hardening.md) — v0.9.2.

Requires: pip install civitas civitas[zmq,security]

Two parts:

1. Topology-level configuration — a real ``Runtime`` with a ``security:`` block
   over a ZMQ transport, proving the signing infrastructure actually activates
   (``runtime._signing_on``, keys generated on disk). The in-process transport
   skips signing entirely (same OS process, no wire to protect); a non-in-process
   transport is required for signing to wire up at all.

2. The actual cryptographic guarantee, demonstrated directly against the public
   ``AgentIdentity`` / ``KeyRegistry`` / ``MessageSigner`` primitives — sign a
   message, verify it (succeeds), tamper with it, verify again (fails). This is
   deliberately NOT run through a live agent-to-agent ``ask()`` over ZMQ: doing so
   surfaced a real, separate, previously-unexercised bug (a signed request/reply
   round trip silently times out — tracked in docs/milestones.md, not solved here)
   that no existing test had ever caught, because no test exercises signing over a
   real transport end-to-end either. Part 2 demonstrates the exact same signing/
   verification code Part 1's Runtime wires up internally, at the layer that is
   actually proven correct by this repository's own test suite.

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
    infrastructure activates (config parsing + key generation), without
    driving an actual signed request/reply round trip (see module docstring)."""
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


async def main() -> None:
    print("=== Part 1: topology-level configuration ===")
    await demonstrate_topology_wiring()

    print("\n=== Part 2: the actual signing guarantee ===")
    demonstrate_signing_and_tamper_detection()


if __name__ == "__main__":
    asyncio.run(main())
