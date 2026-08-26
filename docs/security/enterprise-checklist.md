# Enterprise Adoption Checklist

Use this checklist before deploying Civitas in a production environment that handles sensitive data, operates under compliance requirements, or is externally accessible.

Items are grouped by deployment level. Work through them in order — each level builds on the one before.

---

## Level 1 — All deployments

### Configuration hygiene

- [ ] All API keys and secrets use `!ENV VAR_NAME` in topology YAML — no hardcoded credentials
- [ ] Topology YAML is not committed to a public repository
- [ ] `gitleaks` pre-commit hook is installed and has scanned the repository history
- [ ] `pip-audit` passes with no known vulnerabilities in the dependency tree
- [ ] All dependencies are pinned to exact or minimum-bounded versions (`>=`, not open-ended)
- [ ] `SECURITY.md` exists and points to your internal disclosure contact

### Runtime configuration

- [ ] `logging.DEBUG` is disabled in production (agent payloads are logged at DEBUG level)
- [ ] Agent state (`self.state`) contains no secrets — credentials are read from environment at `on_start()`
- [ ] SQLite state file is stored on an encrypted filesystem or volume, or encryption extension is applied
- [ ] SQLite file permissions are `600` (owner read/write only)
- [ ] MCP server `command` and `args` are reviewed and come from trusted sources only

### Observability

- [ ] OTEL exporter is configured and spans are reaching a durable backend (Jaeger, Grafana Tempo, Datadog)
- [ ] Span retention is configured to meet your audit log requirements (90 days minimum for most compliance frameworks)
- [ ] `civitas.message.sender` and `civitas.message.recipient` attributes are present in retained spans

---

## Level 2 — Multi-process deployments (ZMQTransport)

All Level 1 items, plus:

### Network isolation

- [ ] ZMQ proxy binds to `127.0.0.1` (not `0.0.0.0`) — only local processes can connect
- [ ] ZMQ port is not exposed through firewall rules or container port mappings
- [ ] Worker processes run as a dedicated OS user (not `root`)
- [ ] Worker process OS user does not have write access to the Runtime process's files

### Planned (M4.2 — not yet available)

- [ ] ZMQ CURVE authentication enabled — each Worker has a keypair; Runtime validates public keys
- [ ] ZMQ CURVE encryption enabled — all inter-process traffic is encrypted
- [ ] Message signing enabled — HMAC verification on every routed message

---

## Level 3 — Distributed deployments (NATSTransport)

All Level 1 and 2 items, plus:

### NATS server configuration

- [ ] TLS enabled on NATS server with a certificate from a trusted CA
- [ ] NATS authentication configured (credential files or NKeys — not username/password)
- [ ] Each Worker has its own NATS credential file with scoped subject permissions
- [ ] NATS accounts configured to isolate Civitas subjects from other applications on the same server
- [ ] NATS server not publicly accessible — only reachable from Worker host machines
- [ ] NATS credential files are mounted as Kubernetes Secrets or Docker Swarm secrets, not baked into images

### Network

- [ ] All Worker hosts are in a private network segment
- [ ] Firewall rules prevent direct agent-to-agent connections — all traffic via NATS

---

## Level 4 — External traffic (HTTPGateway, v0.4+)

All Level 1–3 items, plus:

### Gateway security

- [ ] TLS configured on `HTTPGateway` (`tls_cert`, `tls_key`) — no plaintext HTTP in production
- [ ] Authentication middleware applied to all routes (API key, JWT, or mTLS)
- [ ] `RateLimiter(GenServer)` middleware applied to prevent request flooding
- [ ] `request_timeout` configured to prevent slow-client attacks (recommend ≤ 30 seconds)
- [ ] OpenAPI docs disabled (`docs.enabled: false`) or served on a non-public path in production
- [ ] HTTP response headers include: `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`

### Input validation

- [ ] All public routes have `@contract` decorators with Pydantic request models
- [ ] Path parameters are validated before use — not passed directly to filesystem, SQL, or shell calls
- [ ] Request body size limits are configured at the load balancer or reverse proxy layer

### Deployment

- [ ] `HTTPGateway` is behind a load balancer or reverse proxy (nginx, Envoy, AWS ALB)
- [ ] TLS termination is at the load balancer for high-traffic deployments; in-process for low-volume
- [ ] DDoS protection is configured at the network edge (Cloudflare, AWS Shield, etc.)

---

## Compliance-specific guidance

### SOC 2 Type II

| Control area | Civitas capability |
|-------------|-------------------|
| Availability | Supervisor restart policies, escalation chain, bounded mailboxes |
| Confidentiality | NATS TLS, ZMQ CURVE (M4.2), `!ENV` credential injection |
| Integrity | Message signing (M4.2), OTEL audit trail |
| Processing integrity | `EvalAgent` corrective loop, `@contract` request validation |
| Privacy | Avoid storing PII in `self.state`; configure OTEL span attribute filtering |

### GDPR / data residency

- Deploy with `NATSTransport` in a single-region NATS cluster to ensure messages do not cross regional boundaries.
- Do not log message payloads to OTEL spans if payloads may contain personal data — configure span attribute allowlists at the OTLP collector.

### HIPAA

- Use Level 3 or Level 4 deployment with all checklist items above.
- Ensure NATS and OTLP exporters are configured with HIPAA-compliant endpoints.
- Enable audit logging retention of ≥ 6 years via OTLP backend configuration.
- Do not store PHI in `StateStore` without encryption at rest.

---

## Pre-deployment security review

Before going live in a regulated environment, conduct the following review:

1. **Run the security CI workflow locally:** `gh workflow run security.yml`
2. **Review Bandit report:** zero HIGH+ findings in `civitas/`
3. **Review Semgrep report:** zero ERROR-severity findings
4. **Run pip-audit:** `uv run pip-audit --strict`
5. **Scan git history:** `gitleaks detect --source . --log-opts="--all"`
6. **Review SBOM:** confirm all transitive dependencies are expected
7. **Review threat model:** confirm all High-risk items have mitigations in your deployment
8. **Request external audit** (recommended before v1.0 production deployment)

---

## Contacts

Security issues: **security@civitas.io**
Responsible disclosure policy: [SECURITY.md](https://github.com/civitas-io/python-civitas/blob/main/SECURITY.md)
