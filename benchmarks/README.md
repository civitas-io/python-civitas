# M-LAST real performance benchmarks

Real, kept (not deleted after use, per this org's spike-code convention), reproducible harness
backing `docs/design/performance-benchmark.md`. Three real surfaces, matching M-LAST's own
requirement 6 ("cover the real surfaces that matter for this repo specifically"):

- **`serve_gateway.py`** -- a real, standalone `HTTPGateway` server (`/v1/fibonacci`,
  `/v1/echo`), plain HTTP or real mTLS (`--mtls`). Matches TM Dev Lab's own published MCP-server
  benchmark tool set exactly, for direct comparability.
- **`k6_gateway_bench.js`** -- the k6 load profile, deliberately identical in shape to TM Dev
  Lab's own published methodology (10s ramp to 50 VUs, sustained load, 10s ramp-down, `<5%` error
  threshold).
- **`gen_certs.py`** -- real self-signed CA + server + client leaf certificates for the mTLS
  variant (mirrors `tests/integration/test_gateway_http_mtls_direct.py`'s own cert-generation
  pattern).
- **`Dockerfile.bench`** -- for running `serve_gateway.py` under TM Dev Lab's own exact per-server
  resource constraint (`docker run --cpus=1.0 --memory=1g ...`).
- **`bus_client.py`** / **`bus_server.py`** -- the message-bus benchmark. `bus_client.py` is the
  coordinator (starts the real ZMQ/NATS proxy, hosts the real, independent sender load);
  `bus_server.py` is a real `civitas.worker.Worker` process hosting the agent under test. Real,
  separate OS processes -- start `bus_client.py` first (see its own module docstring for why).
- **`serve_spawn_bench.py`** -- `DynamicSupervisor` spawn-latency benchmark, a real `HTTPGateway`
  route (`/v1/spawn`) so the load generator (`ab`/k6) is a real, external, separate process, not
  in-process asyncio tasks measuring the same process's own supervision tree.

## Usage

```bash
# Benchmark 1: HTTPGateway (plain HTTP)
uv run python benchmarks/serve_gateway.py --port 8090
BASE_URL=http://<host>:8090 WORKLOAD=fibonacci FIB_N=20 k6 run benchmarks/k6_gateway_bench.js

# Benchmark 1: HTTPGateway (real mTLS)
uv run python benchmarks/gen_certs.py --out-dir /tmp/certs --server-ip <host-ip>
uv run python benchmarks/serve_gateway.py --port 8443 --mtls --cert-dir /tmp/certs \
    --allowed-dn "$(grep ALLOWED_DNS /tmp/certs-output)"
BASE_URL=https://<host>:8443 WORKLOAD=fibonacci CLIENT_CERT=/tmp/certs/client.pem \
    CLIENT_KEY=/tmp/certs/client.key k6 run benchmarks/k6_gateway_bench.js

# Benchmark 2: message bus (start the coordinator FIRST)
uv run python benchmarks/bus_client.py --transport zmq --bind-ip 0.0.0.0 --concurrency 10 --duration 30
uv run python benchmarks/bus_server.py --transport zmq --coordinator-ip <coordinator-ip>

# Benchmark 3: spawn latency
uv run python benchmarks/serve_spawn_bench.py --port 8095
ab -n 500 -c 10 -p payload.json -T application/json -H "X-Civitas-Type: spawn_request" \
    http://<host>:8095/v1/spawn
```

See `docs/design/performance-benchmark.md` for the real, dated results and the explicit ranking
against TM Dev Lab's own published table this harness produced -- this directory is the reusable
*mechanism*, not the findings themselves.
