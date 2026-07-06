"""HTTP Gateway example.

Exposes an EchoAgent over HTTP using the Civitas HTTPGateway.

Run:
    pip install 'civitas[http]'
    python examples/http_gateway.py

Then in another terminal:
    curl -X POST http://127.0.0.1:8080/v1/echo -H 'Content-Type: application/json' \
         -d '{"text": "hello"}'
    # → {"echo": "hello"}

    # Swagger UI:
    open http://127.0.0.1:8080/docs
"""

from __future__ import annotations

import asyncio
import signal

from civitas import AgentProcess, Runtime, Supervisor
from civitas.gateway import GatewayConfig, HTTPGateway, route
from civitas.messages import Message


class EchoAgent(AgentProcess):
    """Simple echo agent — replies with whatever text it receives."""

    @route("POST", "/v1/echo")
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


async def main() -> None:
    config = GatewayConfig(
        host="127.0.0.1",
        port=8080,
        request_timeout=10.0,
        routes=[
            {"method": "POST", "path": "/v1/echo", "agent": "echo", "mode": "call"},
        ],
        docs_enabled=True,
    )

    supervisor = Supervisor(
        "root",
        children=[
            HTTPGateway("api", config=config),
            EchoAgent("echo"),
        ],
    )
    runtime = Runtime(supervisor=supervisor)
    await runtime.start()

    print("Gateway running at http://127.0.0.1:8080")
    print("Docs at          http://127.0.0.1:8080/docs")
    print("Press Ctrl+C to stop.")

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)

    await stop_event.wait()
    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
