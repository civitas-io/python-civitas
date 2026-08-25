#!/usr/bin/env python3
"""Real, standalone HTTPGateway benchmark server -- two routes matching TM
Dev Lab's own MCP-server benchmark tool set exactly (for direct
comparability on the rows that transfer), plus real, optional mTLS
(client_cert_mode="required", the R10 work) for the civitas-specific
variant that has no external comparison target.

Real, load-bearing detail found while writing this: civitas.config.settings
is a module-level singleton, snapshotted from os.environ ONCE at first
import of civitas.config (civitas/config.py: `settings = Settings()` runs at
module load). CIVITAS_GATEWAY_MTLS_ALLOWED_DNS must therefore be set in
os.environ BEFORE any `civitas` import happens at all -- not after argparse
runs in a normal top-down script. Argument parsing and the env var write
happen first in this file; every `civitas` import is deferred into main().

Usage:
    python benchmarks/serve_gateway.py --port 8090
    python benchmarks/serve_gateway.py --port 8443 --mtls --cert-dir /tmp/bench-certs \\
        --allowed-dn "CN=bench-client,O=civitas-bench"
"""

from __future__ import annotations

import argparse
import asyncio
import os


def _fibonacci(n: int) -> int:
    """Deliberately the naive recursive implementation -- matching TM Dev
    Lab's own stated tool ("CPU-intensive recursive computation") exactly,
    not an optimized iterative/memoized version, so the comparison measures
    the same workload shape their published numbers do.
    """
    if n < 2:
        return n
    return _fibonacci(n - 1) + _fibonacci(n - 2)


async def main(args: argparse.Namespace) -> None:
    from civitas import AgentProcess, Runtime, Supervisor
    from civitas.gateway import GatewayConfig, HTTPGateway
    from civitas.messages import Message

    class BenchAgent(AgentProcess):  # type: ignore[misc]
        """Serves both benchmark routes -- one agent, two message types,
        matching how a real, small deployment would typically shape a
        single backend agent behind a gateway.

        Real, found-while-testing detail: HTTPGateway defaults every
        route's message `type` to the generic "http.request" (civitas/
        gateway/asgi.py) unless the caller sends a real `X-Civitas-Type`
        header -- both k6_gateway_bench.js and any manual curl test against
        this server must set that header to "fibonacci"/"echo" for this
        agent's own dispatch-by-type logic below to route correctly.
        """

        async def handle(self, message: Message) -> Message | None:
            if message.type == "fibonacci":
                n = int(message.payload.get("n", 20))
                return self.reply({"result": _fibonacci(n), "server": "civitas"})
            if message.type == "echo":
                payload = message.payload
                enriched = {**payload, "server": "civitas", "echoed_keys": list(payload.keys())}
                return self.reply(enriched)
            return self.reply({"error": f"unknown message type {message.type!r}"})

    routes = [
        {"method": "POST", "path": "/v1/fibonacci", "agent": "bench", "mode": "call"},
        {"method": "POST", "path": "/v1/echo", "agent": "bench", "mode": "call"},
    ]

    gateway_kwargs: dict[str, object] = {"host": args.host, "port": args.port, "routes": routes}
    if args.mtls:
        gateway_kwargs.update(
            tls_cert=f"{args.cert_dir}/server.pem",
            tls_key=f"{args.cert_dir}/server.key",
            tls_ca_cert=f"{args.cert_dir}/ca.pem",
            client_cert_mode="required",
            middleware=["civitas.gateway.mtls.require_client_cert"],
        )

    config = GatewayConfig(**gateway_kwargs)  # type: ignore[arg-type]
    gateway = HTTPGateway("api", config=config)
    bench_agent = BenchAgent("bench")

    supervisor = Supervisor("root", children=[gateway, bench_agent])
    runtime = Runtime(supervisor=supervisor)

    scheme = "https" if args.mtls else "http"
    print(f"civitas HTTPGateway benchmark server: {scheme}://{args.host}:{args.port}")
    print("routes: POST /v1/fibonacci {n: int}, POST /v1/echo {...}")

    await runtime.start()
    try:
        await asyncio.Event().wait()
    finally:
        await runtime.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--mtls", action="store_true")
    parser.add_argument("--cert-dir", default=None)
    parser.add_argument("--allowed-dn", default=None)
    parsed_args = parser.parse_args()

    if parsed_args.mtls:
        assert parsed_args.cert_dir and parsed_args.allowed_dn, (
            "--cert-dir and --allowed-dn required with --mtls"
        )
        # Must happen before ANY civitas import -- see module docstring.
        os.environ["CIVITAS_GATEWAY_MTLS_ALLOWED_DNS"] = parsed_args.allowed_dn

    asyncio.run(main(parsed_args))
