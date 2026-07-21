# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.7.x (latest) | ✅ Security fixes |
| < 0.7.0 | ❌ No longer supported |

Civitas is pre-1.0. Minor releases may include security fixes alongside new features. We strongly recommend staying on the latest release.

---

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities by email to **security@civitas.io** (or **jerynmathew@gmail.com** until the alias is live).

Please include:

- A description of the vulnerability and its potential impact
- Steps to reproduce (or a proof-of-concept)
- The Civitas version affected
- Any relevant environment details (OS, Python version, transport backend)

PGP encryption is not required but welcome. If you need our public key, request it in your first message.

---

## Response SLAs

| Stage | Target |
|-------|--------|
| Acknowledgement | Within 2 business days |
| Initial assessment | Within 5 business days |
| Patch for `CRITICAL` / `HIGH` | Within 14 days of confirmation |
| Patch for `MEDIUM` | Within 60 days of confirmation |
| Patch for `LOW` | Best effort; batched into next minor release |

We follow [CVSS v3.1](https://www.first.org/cvss/v3.1/specification-document) for severity scoring.

---

## Disclosure policy

We follow **coordinated disclosure**:

1. Reporter submits the vulnerability privately.
2. We confirm receipt within 2 business days.
3. We investigate and produce a fix.
4. We publish a security advisory on GitHub and release a patched version.
5. Reporter may publish details **90 days** after initial report, or sooner if we agree.

We credit reporters in the security advisory unless anonymity is requested.

---

## Security advisories

Published at: **[GitHub Security Advisories](https://github.com/civitas-io/python-civitas/security/advisories)**

Each advisory includes:
- CVE identifier (requested from MITRE via GitHub)
- CVSS score and vector
- Affected versions
- Patch version
- Mitigation / workaround (if available before patch)

---

## Scope

**In scope:**

- `civitas` core runtime (`civitas/`)
- All official transport plugins (ZMQ, NATS)
- All official LLM provider plugins (Anthropic, LiteLLM, etc.)
- `civitas[http]` HTTP Gateway
- CLI (`civitas/cli/`)

**Out of scope:**

- Third-party MCP servers connected via `connect_mcp()`
- User-supplied agent code
- Hosted infrastructure not run by the Civitas project

---

## Security release notes

Security fixes are tagged `security` in the [CHANGELOG](CHANGELOG.md) and announced in the GitHub release. Subscribe to repository releases for notifications.
