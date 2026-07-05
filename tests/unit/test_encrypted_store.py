"""Unit tests for EncryptingStateStore and its config/loader wiring (v0.7.0 R4)."""

from __future__ import annotations

import base64
import logging
import os
import sys
from typing import Any
from unittest.mock import patch

import pytest

import civitas
from civitas.config import SecretStr, decode_state_key, load_state_key, settings
from civitas.errors import ConfigurationError, PluginError, StateDecryptionError
from civitas.plugins.encrypted_store import ENVELOPE_KEY, EncryptingStateStore
from civitas.plugins.state import InMemoryStateStore, StateStore
from civitas.runtime import Runtime
from civitas.security.config import StateEncryptionConfig


def _key() -> bytes:
    return os.urandom(32)


def _store(**kwargs: Any) -> tuple[EncryptingStateStore, InMemoryStateStore]:
    inner = InMemoryStateStore()
    key = kwargs.pop("key", None) or _key()
    keys = kwargs.pop("keys", {0: key})
    current = kwargs.pop("current_key_id", 0)
    return EncryptingStateStore(inner, keys=keys, current_key_id=current, **kwargs), inner


# ---------------------------------------------------------------------------
# Roundtrip + envelope shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_roundtrip_returns_original_dict() -> None:
    store, _ = _store()
    await store.set("agent_a", {"count": 3, "note": "hello"})
    assert await store.get("agent_a") == {"count": 3, "note": "hello"}


@pytest.mark.asyncio
async def test_inner_receives_envelope_not_plaintext() -> None:
    store, inner = _store()
    await store.set("agent_a", {"secret": "top"})
    stored = inner._data["agent_a"]
    assert set(stored.keys()) == {ENVELOPE_KEY}
    assert "secret" not in stored
    assert "top" not in str(stored)


@pytest.mark.asyncio
async def test_get_missing_returns_none() -> None:
    store, _ = _store()
    assert await store.get("nobody") is None


@pytest.mark.asyncio
async def test_satisfies_state_store_protocol() -> None:
    store, _ = _store()
    assert isinstance(store, StateStore)


# ---------------------------------------------------------------------------
# Nonce uniqueness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nonce_uniqueness_identical_state() -> None:
    store, inner = _store()
    await store.set("a", {"x": 1})
    env1 = inner._data["a"][ENVELOPE_KEY]
    await store.set("a", {"x": 1})
    env2 = inner._data["a"][ENVELOPE_KEY]
    assert env1 != env2


# ---------------------------------------------------------------------------
# Integrity: tamper, AAD binding, envelope version
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tamper_ciphertext_raises() -> None:
    store, inner = _store()
    await store.set("a", {"x": 1})
    env = bytearray(base64.b64decode(inner._data["a"][ENVELOPE_KEY]))
    env[-1] ^= 0x01
    inner._data["a"][ENVELOPE_KEY] = base64.b64encode(bytes(env)).decode()
    with pytest.raises(StateDecryptionError):
        await store.get("a")


@pytest.mark.asyncio
async def test_aad_swap_between_agents_raises() -> None:
    store, inner = _store()
    await store.set("agent_a", {"x": 1})
    inner._data["agent_b"] = dict(inner._data["agent_a"])
    with pytest.raises(StateDecryptionError):
        await store.get("agent_b")


@pytest.mark.asyncio
async def test_unknown_envelope_version_raises() -> None:
    store, inner = _store()
    await store.set("a", {"x": 1})
    env = bytearray(base64.b64decode(inner._data["a"][ENVELOPE_KEY]))
    env[0] = 99
    inner._data["a"][ENVELOPE_KEY] = base64.b64encode(bytes(env)).decode()
    with pytest.raises(StateDecryptionError):
        await store.get("a")


@pytest.mark.asyncio
async def test_error_message_omits_plaintext() -> None:
    store, inner = _store()
    await store.set("agent_a", {"very_secret_value": "leak_me"})
    env = bytearray(base64.b64decode(inner._data["agent_a"][ENVELOPE_KEY]))
    env[-1] ^= 0x01
    inner._data["agent_a"][ENVELOPE_KEY] = base64.b64encode(bytes(env)).decode()
    with pytest.raises(StateDecryptionError) as exc:
        await store.get("agent_a")
    assert "leak_me" not in str(exc.value)
    assert "agent_a" in str(exc.value)


# ---------------------------------------------------------------------------
# Key rotation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_key_rotation_decrypts_old_and_writes_current() -> None:
    k0, k1 = _key(), _key()
    inner = InMemoryStateStore()

    old = EncryptingStateStore(inner, keys={0: k0}, current_key_id=0)
    await old.set("a", {"v": 1})
    assert base64.b64decode(inner._data["a"][ENVELOPE_KEY])[1] == 0

    rotated = EncryptingStateStore(inner, keys={0: k0, 1: k1}, current_key_id=1)
    assert await rotated.get("a") == {"v": 1}

    await rotated.set("a", {"v": 2})
    assert base64.b64decode(inner._data["a"][ENVELOPE_KEY])[1] == 1
    assert await rotated.get("a") == {"v": 2}


@pytest.mark.asyncio
async def test_unknown_key_id_raises() -> None:
    k0, k1 = _key(), _key()
    inner = InMemoryStateStore()
    EncryptingStateStore(inner, keys={0: k0}, current_key_id=0)
    writer = EncryptingStateStore(inner, keys={0: k0}, current_key_id=0)
    await writer.set("a", {"v": 1})

    reader = EncryptingStateStore(inner, keys={1: k1}, current_key_id=1)
    with pytest.raises(StateDecryptionError):
        await reader.get("a")


def test_current_key_id_not_in_ring_raises() -> None:
    with pytest.raises(ConfigurationError):
        EncryptingStateStore(InMemoryStateStore(), keys={0: _key()}, current_key_id=5)


# ---------------------------------------------------------------------------
# Suspend marker survives
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_suspend_marker_survives_roundtrip() -> None:
    store, _ = _store()
    state = {
        "count": 5,
        "_civitas.suspended": {"reason": "hold", "since": 123.0, "approver": None},
    }
    await store.set("a", state)
    restored = await store.get("a")
    assert restored == state
    assert restored is not None
    assert "_civitas.suspended" in restored


# ---------------------------------------------------------------------------
# Legacy plaintext handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_plaintext_strict_raises() -> None:
    store, inner = _store()
    inner._data["a"] = {"plain": 1}
    with pytest.raises(StateDecryptionError):
        await store.get("a")


@pytest.mark.asyncio
async def test_legacy_plaintext_allow_reads_and_reencrypts() -> None:
    key = _key()
    inner = InMemoryStateStore()
    inner._data["a"] = {"plain": 1}
    store = EncryptingStateStore(inner, keys={0: key}, current_key_id=0, allow_plaintext_read=True)
    assert await store.get("a") == {"plain": 1}

    await store.set("a", {"plain": 2})
    assert ENVELOPE_KEY in inner._data["a"]
    assert await store.get("a") == {"plain": 2}


# ---------------------------------------------------------------------------
# Delegation of delete / list_agents / close
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_and_list_and_close_delegate() -> None:
    store, _ = _store()
    await store.set("a", {"x": 1})
    await store.set("b", {"y": 2})
    assert await store.list_agents() == ["a", "b"]
    await store.delete("a")
    assert await store.list_agents() == ["b"]
    await store.close()


# ---------------------------------------------------------------------------
# cryptography absent
# ---------------------------------------------------------------------------


def test_cryptography_absent_raises_configuration_error() -> None:
    with patch.dict(sys.modules, {"cryptography.hazmat.primitives.ciphers.aead": None}):
        with pytest.raises(ConfigurationError):
            EncryptingStateStore(InMemoryStateStore(), keys={0: _key()}, current_key_id=0)


# ---------------------------------------------------------------------------
# Key config helpers
# ---------------------------------------------------------------------------


def test_decode_state_key_valid() -> None:
    raw = _key()
    assert decode_state_key(base64.b64encode(raw).decode()) == raw


def test_decode_state_key_short_raises() -> None:
    with pytest.raises(ConfigurationError):
        decode_state_key(base64.b64encode(os.urandom(16)).decode())


def test_decode_state_key_invalid_base64_raises() -> None:
    with pytest.raises(ConfigurationError):
        decode_state_key("@@ not base64 @@")


def test_load_state_key_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "state_key", SecretStr(None))
    with pytest.raises(ConfigurationError):
        load_state_key()


def test_load_state_key_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _key()
    monkeypatch.setattr(settings, "state_key", SecretStr(base64.b64encode(raw).decode()))
    assert load_state_key() == raw


# ---------------------------------------------------------------------------
# StateEncryptionConfig
# ---------------------------------------------------------------------------


def test_state_encryption_config_defaults() -> None:
    cfg = StateEncryptionConfig.from_dict({})
    assert cfg.key_env == "CIVITAS_STATE_KEY"
    assert cfg.current_key_id == 0
    assert cfg.keys == {}
    assert cfg.allow_plaintext_read is False


def test_state_encryption_config_full() -> None:
    cfg = StateEncryptionConfig.from_dict(
        {
            "key_env": "MY_KEY",
            "current_key_id": 2,
            "keys": {0: "OLD", 1: "MID"},
            "allow_plaintext_read": True,
        }
    )
    assert cfg.key_env == "MY_KEY"
    assert cfg.current_key_id == 2
    assert cfg.keys == {0: "OLD", 1: "MID"}
    assert cfg.allow_plaintext_read is True


# ---------------------------------------------------------------------------
# Loader — nested store construction + wiring
# ---------------------------------------------------------------------------


def test_build_encrypted_store_missing_inner_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from civitas.plugins.loader import _build_encrypted_store

    monkeypatch.setattr(settings, "state_key", SecretStr(base64.b64encode(_key()).decode()))
    with pytest.raises(PluginError):
        _build_encrypted_store({})


def test_build_encrypted_store_in_memory_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from civitas.plugins.loader import _build_encrypted_store

    monkeypatch.setattr(settings, "state_key", SecretStr(base64.b64encode(_key()).decode()))
    with caplog.at_level(logging.WARNING):
        store = _build_encrypted_store({"store": {"type": "in_memory"}})
    assert isinstance(store, EncryptingStateStore)
    assert any("at-rest protection" in r.message for r in caplog.records)


def test_build_encrypted_store_rotation_resolves_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from civitas.plugins.loader import _build_encrypted_store

    k_cur, k_old = _key(), _key()
    monkeypatch.setattr(settings, "state_key", SecretStr(base64.b64encode(k_cur).decode()))
    monkeypatch.setenv("OLD_STATE_KEY", base64.b64encode(k_old).decode())
    store = _build_encrypted_store(
        {
            "store": {"type": "in_memory"},
            "current_key_id": 1,
            "keys": {0: "OLD_STATE_KEY"},
        }
    )
    assert store._current_key_id == 1
    assert set(store._keys.keys()) == {0, 1}


@pytest.mark.asyncio
async def test_runtime_from_config_encrypts_at_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "state_key", SecretStr(base64.b64encode(_key()).decode()))
    config = {
        "supervision": {"name": "root", "children": []},
        "plugins": {
            "state": {
                "type": "encrypted",
                "config": {
                    "store": {"type": "in_memory"},
                    "allow_plaintext_read": False,
                },
            }
        },
    }
    runtime = Runtime.from_config_dict(config)
    store = runtime._state_store
    assert isinstance(store, EncryptingStateStore)

    await store.set("a", {"x": 1})
    assert ENVELOPE_KEY in store._inner._data["a"]
    assert await store.get("a") == {"x": 1}


def test_public_exports() -> None:
    assert civitas.EncryptingStateStore is EncryptingStateStore
    assert "EncryptingStateStore" in civitas.__all__
    assert "StateStore" in civitas.__all__


# ---------------------------------------------------------------------------
# CLI — state list masks encrypted values
# ---------------------------------------------------------------------------


def test_cli_state_list_masks_encrypted(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from typer.testing import CliRunner

    from civitas.cli import state as state_mod

    db = tmp_path / "s.db"
    db.write_text("")
    agents = ["enc", "plain"]
    states: dict[str, dict[str, Any] | None] = {
        "enc": {ENVELOPE_KEY: "SUPERSECRETCIPHERTEXT"},
        "plain": {"visible": "data"},
    }

    async def _fake_load(path: str) -> tuple[list[str], dict[str, dict[str, Any] | None]]:
        return agents, states

    monkeypatch.setattr(state_mod, "_load_all_states", _fake_load)
    result = CliRunner().invoke(state_mod.state_app, ["list", "--db", str(db)])
    assert result.exit_code == 0
    assert "<encrypted>" in result.stdout
    assert "SUPERSECRETCIPHERTEXT" not in result.stdout
