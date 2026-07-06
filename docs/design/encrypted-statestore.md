# Encrypted StateStore at Rest (v0.7.0 · R4)

**Status:** ✅ Approved — maintainer signed off 2026-07-04 (go with recommendations). Oracle review unavailable (timed out ×2); self-reviewed (§7.5). Implementation queued after R3; Momus reviews the implementation plan.
**Source:** [`security-hardening.md`](security-hardening.md) (deferred item); [`milestones.md`](../milestones.md) v0.7.0 R4
**Related:** [`durable-suspension.md`](durable-suspension.md) (suspend marker in `self.state`)

---

## 1. Problem

Agent `self.state` is persisted verbatim by the configured `StateStore` (SQLite/Postgres in contrib write
plaintext JSON). Anyone with disk/DB access reads all agent state. R4 adds **encryption at rest** as a
**backend-agnostic `EncryptingStateStore` wrapper** that transparently encrypts values in any store.

## 2. Current behavior (ground truth, line refs)

- **`StateStore` protocol** (`plugins/state.py:8-31`, `@runtime_checkable`): **async** `get(name)->dict|None`, `set(name, state: dict)->None`, `delete(name)`, `list_agents()->list[str]`, `close()`. `InMemoryStateStore` copies dicts (`dict(state)`).
- **Boundary is a dict, not bytes** (KEY): `AgentProcess.checkpoint()` calls `store.set(self.name, self.state)` with a **raw dict** (`process.py:399-407`); `_restore_state()` assigns `self.state = store.get(...)` (a **dict**). **Serialization is the inner store's job** — contrib `SQLiteStateStore.set` does `json.dumps(state)` internally. ⇒ the wrapper **cannot hand raw ciphertext bytes** to the inner store; it must pass a JSON-serializable **dict**.
- **Suspend marker** `_civitas.suspended` lives *inside* `self.state` (`process.py:208, 471-475`) → encrypted/decrypted atomically with user state; restore checks it after decrypt (`process.py:1104`).
- **Wiring**: `build_component_set(state_store=...)` defaults to `InMemoryStateStore` (`components.py:174-176`); `Runtime.from_config` → `load_plugins_from_config` → loader `_BUILTINS["state"]` (`in_memory`/`sqlite`/`postgres`) (`plugins/loader.py:40-44,164-169`).
- **CLI**: `civitas state migrate` copies dict-by-dict (`get`→`set`, `cli/state.py:134-164`); `state list` prints stored values.
- **Existing crypto**: pynacl in `civitas[security]` (`pyproject.toml:61`), used for Ed25519 signing (`security/signing.py`). `SecretStr` masks secrets (`config.py:20-38`). `SecurityConfig.from_dict` pattern (`security/config.py:104-128`). `StateStore` is **not** exported from `civitas/__init__.py`.

## 3. Goals / Non-goals

**Goals:** transparent, backend-agnostic encryption of persisted state values (authenticated); key from env;
key rotation; fail-loud on tamper/wrong-key; opt-in.

**Non-goals:** encrypting agent *names* (needed plaintext for `list_agents`); a KMS/HSM integration (env/secret-manager delivers the key); at-rest encryption of the *message bus* (out of scope); automatic migration of pre-existing plaintext state (explicit `state migrate` re-encrypt).

## 4. Design

`EncryptingStateStore(inner: StateStore, ...)` implements `StateStore` and wraps any inner store. It
encrypts **values only**; agent names pass through so `list_agents()`/indexing work.

**Dict boundary (D3):** since the inner store serializes a dict, the wrapper stores a **one-key envelope dict**:
```python
# set(name, state):  plaintext = canonical_serialize(state)  (msgpack/json bytes)
#   env = version(1B) || key_id(1B) || nonce(12B) || ciphertext+tag   # AAD = name
#   inner.set(name, {"__civitas_enc__": base64(env)})
# get(name):  raw = inner.get(name)
#   if raw is None: return None
#   if "__civitas_enc__" not in raw: <plaintext-legacy handling — §D7>
#   decrypt(base64decode(raw["__civitas_enc__"]), aad=name) -> deserialize -> dict
```
Base64 keeps it JSON-safe for text backends; binary backends still round-trip it.

## 5. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** ⚠️ | **Primitive = `cryptography` `ChaCha20Poly1305`** (AEAD, 12-byte nonce, RFC 7539), opt-in extra `encryption = ["cryptography>=41"]`, lazy import. Use **AAD = agent name**. | AEAD **with AAD** binds ciphertext to its agent → prevents swapping one agent's blob into another. pynacl `SecretBox` lacks a public AAD API (see §7 Q1). |
| **D2** | `EncryptingStateStore(inner)` implements the async `StateStore` protocol; delegates `delete`/`list_agents`/`close`; encrypts in `set`, decrypts in `get`. Values only. | Backend-agnostic; one wrapper covers InMemory/SQLite/Postgres/future. |
| **D3** | **Envelope-in-dict**: store `{"__civitas_enc__": base64(version‖key_id‖nonce‖ct)}`; never pass raw bytes to the inner store (protocol is `dict`). Inner plaintext serialized with **json** (matches the existing contract that `self.state` is JSON-serializable — contrib stores already `json.dumps` it); `__civitas_enc__` is a reserved top-level key. | The inner store re-serializes a dict; raw bytes would break text backends; json keeps parity with today's persist contract. |
| **D4** | **Key mgmt**: `CIVITAS_STATE_KEY` = base64 32-byte key (via `SecretStr`); **no KDF** (deployment delivers a real key). **Rotation** via a key ring `{key_id: key}` + `current_key_id`; new writes use current, reads select by envelope `key_id`. Wrong key / tamper → `InvalidTag` → raise a clear error (**never** return plaintext/None silently). | Simple, deployment-friendly; rotation without mass re-encrypt; fail-loud. |
| **D5** | **Nonce** = 12 random bytes per `set` (prepended). Never counter-based (backup rollback ⇒ nonce reuse ⇒ catastrophic). | AEAD nonce-uniqueness; random is safe at state volumes. |
| **D6** | **Wiring/config**: loader `type: encrypted` wrapping a nested inner store, e.g. `plugins.state: {type: encrypted, config: {key_env: CIVITAS_STATE_KEY, store: {type: sqlite, config: {...}}}}`; applied in the loader/`build_component_set`. Export `EncryptingStateStore` **and** `StateStore` from `civitas/__init__.py`. | Composes with any backend; explicit, discoverable. |
| **D7** | **Legacy/plaintext + migration**: on `get`, a value lacking `__civitas_enc__` is treated per a `require_encrypted` flag — default **strict** (raise: "unencrypted state found; run `civitas state migrate` to re-encrypt"), opt-in `allow_plaintext_read` for gradual migration. `civitas state migrate` re-encrypts transparently (src get→dst set through the wrapper). `state list` shows names + `<encrypted>` for values. | No silent plaintext acceptance; clear migration path. |
| **D8** | **Never log** key or plaintext; `SecretStr` for the key; errors reference agent name + `key_id`, not contents. Document **key loss = data loss**; key belongs in a secret manager. | Standard secret hygiene. |

## 6. Threat model

Confidentiality of state at rest against disk/DB read. **AEAD** gives integrity+authenticity (tamper →
`InvalidTag` → loud failure). **AAD=agent-name** stops cross-agent blob swapping (an attacker moving
agent A's ciphertext into agent B's row fails to decrypt). **Nonce** random-unique per write (D5). **Not**
in scope: an attacker with the live key/process memory; side channels; encrypting names. **Key loss** is
unrecoverable (documented). Enabling encryption over existing plaintext without migration → strict failure
(D7), not silent bypass.

## 7. Resolved decisions (maintainer sign-off — 2026-07-04: "go with recommendations")

1. ✅ `cryptography` **ChaCha20Poly1305 + AAD=agent-name**; opt-in `civitas[encryption]` (note R3 already introduces `cryptography` via `pyjwt[cryptography]`).
2. ✅ Dedicated `encryption` extra.
3. ✅ Config: loader `type: encrypted` wrapping a nested `store` (D6).
4. ✅ Legacy plaintext: **strict default** + opt-in `allow_plaintext_read` (+ dual-read migration sequence, §7.5).
5. ✅ Rotation v1: multi-key decrypt + current-key encrypt; defer a bulk re-wrap tool (`state migrate` suffices).

## 7.5 Self-review hardening (Oracle timed out ×2 — applied by author)

Substituting for the Oracle pass, applying the same lens R3 got:

- **Serialize inner plaintext with `json`** (not msgpack): `self.state` is already required to be JSON-serializable (contrib stores `json.dumps` it today), so json keeps exact parity — encryption must not silently accept types a plaintext deployment would reject. `__civitas_enc__` is a documented **reserved** top-level key (same convention as `_civitas.suspended`); collision risk is negligible.
- **Missing `CIVITAS_STATE_KEY` → `ConfigurationError` at startup** (fail-loud), never a silent fall-through to plaintext. Unknown envelope `key_id` and `InvalidTag` → raise with agent-name + `key_id` (never contents).
- **Multi-process key distribution (Worker):** every process (Runtime + Workers) needs the key. During rotation, **all** processes must carry the **full key ring** (decrypt-any) while `current_key_id` advances (encrypt-current) — otherwise a Worker can't decrypt peers' state. Document this.
- **In-place migration ≠ `state migrate`:** `civitas state migrate src dst` is **cross-store**. To enable encryption on a *live, same-backend* deployment, use the **dual-read sequence**: (1) deploy with encryption + `allow_plaintext_read=true` (reads legacy plaintext, writes ciphertext on next checkpoint → lazy rewrite); (2) optionally force a full rewrite; (3) flip to strict (`allow_plaintext_read=false`). A dedicated `civitas state reencrypt` is a possible future convenience (not v1).
- **`InMemoryStateStore` + encryption = no at-rest protection** (memory only) — allowed but log an INFO/WARN so operators aren't misled.
- **Never** put the key in a checkpoint/trace/log/`repr` (SecretStr); errors and `state list` show `<encrypted>` / metadata only.

## 8. Test plan (outline)

- Roundtrip set→get through InMemory + a fake inner store; value on disk is ciphertext, not plaintext.
- Nonce uniqueness: two `set`s of identical state → different envelopes.
- Tamper: flip a ciphertext byte → `get` raises (never returns partial/plaintext).
- AAD: move agent A's envelope under agent B's name → decrypt fails.
- Key rotation: data written under `key_id=0` still decrypts after adding `key_id=1` as current; new writes carry `key_id=1`; unknown `key_id` → clear error.
- Suspend marker survives a roundtrip (encrypted atomically; restore comes up SUSPENDED).
- Legacy: plaintext value + strict mode → raises; `allow_plaintext_read` → reads it (and re-encrypts on next `set`).
- `civitas state migrate` plaintext→encrypted re-encrypts; `state list` shows `<encrypted>`.
- `cryptography` absent → clear `ConfigurationError` at startup.

## 9. Implementer checklist

- `civitas/plugins/state.py` (or new `plugins/encrypted_store.py`): `EncryptingStateStore` (lazy `cryptography` import; key ring; envelope encode/decode; AAD; strict/allow-plaintext).
- Loader `_BUILTINS["state"]["encrypted"]` + nested-store construction (`plugins/loader.py`); wire in `build_component_set`/`from_config`.
- `config.py`: `CIVITAS_STATE_KEY` `SecretStr`; a `StateEncryptionConfig.from_dict`.
- Export `EncryptingStateStore` + `StateStore` from `civitas/__init__.py` (`__all__`).
- `pyproject.toml`: `encryption` extra.
- `cli/state.py`: `state list` masks encrypted values; confirm `migrate` re-encrypts.
- Tests per §8; `CHANGELOG [Unreleased]`; docstrings; AGENTS.md install matrix + key-management doc.
