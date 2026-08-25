// k6 load profile -- deliberately IDENTICAL to TM Dev Lab's own published
// methodology (https://www.tmdevlab.com/mcp-server-performance-benchmark.html
// section 2.3), so the resulting numbers are a genuine, apples-to-apples
// comparison against their published Java/Go/Node.js/Python(FastMCP) table,
// not a differently-shaped test that merely resembles it.
//
// Usage:
//   BASE_URL=http://100.82.206.105:8090 WORKLOAD=fibonacci k6 run benchmarks/k6_gateway_bench.js
//   BASE_URL=https://100.82.206.105:8443 WORKLOAD=echo k6 run \
//       -e CLIENT_CERT=/tmp/bench-certs/client.pem -e CLIENT_KEY=/tmp/bench-certs/client.key \
//       -e CA_CERT=/tmp/bench-certs/ca.pem benchmarks/k6_gateway_bench.js

import http from "k6/http";
import { check } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8090";
const WORKLOAD = __ENV.WORKLOAD || "fibonacci";
// n=30 -- a real, mid-range value from TM Dev Lab's own stated 0-40
// parameter range, not the trivial end (matches their own "CPU-intensive"
// framing meaningfully, without pushing wall-clock time for a single
// benchmark round past what's practical to repeat 3x).
const FIB_N = parseInt(__ENV.FIB_N || "30", 10);

export const options = {
  stages: [
    { duration: "10s", target: 50 }, // ramp-up to 50 VUs
    { duration: "5m", target: 50 }, // sustained load
    { duration: "10s", target: 0 }, // ramp-down
  ],
  thresholds: {
    http_req_failed: ["rate<0.05"], // error rate < 5%, matching TM Dev Lab exactly
  },
  tlsAuth: __ENV.CLIENT_CERT
    ? [
        {
          cert: open(__ENV.CLIENT_CERT),
          key: open(__ENV.CLIENT_KEY),
        },
      ]
    : undefined,
  // Real, found-while-testing limitation: vanilla k6 has no direct "trust
  // this custom CA" option the way it has `tlsAuth` for presenting a
  // CLIENT cert -- verifying our own self-signed CA's SERVER cert isn't
  // straightforward without a k6 extension. Server-side client-cert
  // enforcement (the actual mTLS mechanism under test, require_client_cert)
  // is unaffected by this -- the server still requires and validates the
  // real client cert above; only the client's OWN verification of the
  // server's cert is skipped, disclosed here, not silently assumed benign.
  insecureSkipTLSVerify: true,
};

export default function () {
  const path = WORKLOAD === "fibonacci" ? "/v1/fibonacci" : "/v1/echo";
  const body =
    WORKLOAD === "fibonacci"
      ? JSON.stringify({ n: FIB_N })
      : JSON.stringify({ text: "hello", session_id: "bench" });

  const res = http.post(`${BASE_URL}${path}`, body, {
    // civitas's own HTTPGateway (civitas/gateway/asgi.py) reads the target
    // message TYPE from this header, defaulting to the generic
    // "http.request" otherwise -- both benchmark routes share one agent
    // (BenchAgent), so this is how it tells the two workloads apart.
    headers: { "Content-Type": "application/json", "X-Civitas-Type": WORKLOAD },
  });

  check(res, {
    "status is 200": (r) => r.status === 200,
  });
}
