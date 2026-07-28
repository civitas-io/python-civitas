"""gRPC gateway (design/grpc-gateway.md) — v0.9.2.

Requires: pip install 'civitas[grpc]'

One generic ``Agent`` gRPC service proxies any agent by name — callers need no
per-agent ``.proto`` or civitas SDK, just the one shared ``civitas.proto`` this
package ships (``civitas/gateway/proto/civitas.proto``). Same ``HTTPGateway``
process HTTP already uses; ``grpc_enabled=True`` adds the gRPC server as a second
listener alongside it (or on its own — HTTP is not required).

Usage:
    python examples/grpc_gateway.py
"""

from __future__ import annotations

import asyncio

import grpc
from google.protobuf.struct_pb2 import Struct

from civitas import AgentProcess, Runtime, Supervisor
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.proto import civitas_pb2, civitas_pb2_grpc
from civitas.messages import Message


class EchoAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


def _struct_from_dict(payload: dict) -> Struct:
    struct = Struct()
    struct.update(payload)
    return struct


async def main() -> None:
    config = GatewayConfig(port=8081, grpc_enabled=True, grpc_port=50051)
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[HTTPGateway("api", config=config), EchoAgent("echo")],
        )
    )
    await runtime.start()
    print("gRPC gateway listening on 127.0.0.1:50051 (HTTP on 8081, unused here)")

    # A real external client, over a real gRPC channel — no civitas import on
    # this side in a genuine remote-caller scenario, just the shared .proto.
    channel = grpc.aio.insecure_channel("127.0.0.1:50051")
    try:
        stub = civitas_pb2_grpc.AgentStub(channel)
        request = civitas_pb2.AgentRequest(
            recipient="echo",
            type="grpc.request",
            payload=_struct_from_dict({"text": "hello over gRPC"}),
        )
        reply = await stub.Invoke(request)
        print(f"Reply payload: {dict(reply.payload)}")
    finally:
        await channel.close()

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
