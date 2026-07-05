"""Tests for cross-process dynamic spawn (v0.7.0 · R6).

Cross-process spawn needs a non-in-process transport. Two harnesses are used:

* ``_distributed_supervisor`` wires a ``DynamicSupervisor`` onto a bridged
  transport so ``_is_distributed()`` is True and announcements can be captured
  on the same shared broker — used for the focused announce / identity tests.
* ``_make_harness`` builds a full two-process emulation: a ``Runtime`` (process
  A) and a ``Worker`` (process B) each with their own bus/registry sharing one
  in-memory pub/sub broker, so ``_agency.register`` announcements really cross
  between two registries — used for the end-to-end spawn / ask / terminate tests.

The signing paths are unit-tested at the piece level (announce signing, receive
verification/ownership, per-incarnation identity) because the Worker does not yet
build signing infrastructure from config; a real ZMQ end-to-end lives in
tests/integration/test_m2_9_cross_process_spawn.py.
"""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Awaitable, Callable
from typing import Any

import msgpack
import pytest

from civitas import AgentProcess, DynamicSupervisor, Runtime, Supervisor, Worker
from civitas.bus import MessageBus
from civitas.components import ComponentSet
from civitas.errors import ConfigurationError, SpawnError
from civitas.messages import Message, _uuid7
from civitas.observability.tracer import Tracer
from civitas.plugins.state import InMemoryStateStore
from civitas.process import DYNAMIC_SUPERVISOR_CAPABILITY, ProcessStatus
from civitas.registry import LocalRegistry
from civitas.security.config import SigningConfig
from civitas.serializer import MsgpackSerializer
from tests.conftest import EchoAgent, wait_for

try:
    import nacl.signing  # noqa: F401

    from civitas.security.identity import AgentIdentity
    from civitas.security.registry import KeyRegistry
    from civitas.security.signing import MessageSigner, SigningSerializer

    _HAS_NACL = True
except ImportError:
    _HAS_NACL = False

_requires_nacl = pytest.mark.skipif(not _HAS_NACL, reason="pynacl not installed")


# ---------------------------------------------------------------------------
# Bridged in-memory transport — emulates ZMQ pub/sub across two processes
# ---------------------------------------------------------------------------


class _SharedBroker:
    """Fans a publish out to every connected transport's subscribers."""

    def __init__(self) -> None:
        self._transports: list[_BridgedTransport] = []

    def connect(self, transport: _BridgedTransport) -> None:
        self._transports.append(transport)

    def disconnect(self, transport: _BridgedTransport) -> None:
        if transport in self._transports:
            self._transports.remove(transport)

    async def deliver(self, topic: str, data: bytes) -> None:
        for transport in list(self._transports):
            await transport._local_deliver(topic, data)


class _BridgedTransport:
    """Transport that reaches peers through a shared in-memory broker.

    A distinct class from InProcessTransport so ``_is_distributed()`` sees a
    cross-process transport, while still running entirely in one event loop.
    """

    def __init__(self, serializer: Any, broker: _SharedBroker) -> None:
        self._serializer = serializer
        self._broker = broker
        self._handlers: dict[str, Callable[[bytes], Awaitable[None]]] = {}
        self._reply_queues: dict[str, asyncio.Queue[bytes]] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._broker.connect(self)
        self._started = True

    async def wait_ready(self) -> None:
        return None

    async def stop(self) -> None:
        self._started = False
        self._broker.disconnect(self)
        self._handlers.clear()
        self._reply_queues.clear()

    async def subscribe(self, address: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        self._handlers[address] = handler

    async def unsubscribe(self, address: str) -> None:
        self._handlers.pop(address, None)

    async def publish(self, address: str, data: bytes) -> None:
        if address in self._reply_queues:
            await self._reply_queues[address].put(data)
            return
        await self._broker.deliver(address, data)

    async def _local_deliver(self, topic: str, data: bytes) -> None:
        if topic in self._reply_queues:
            await self._reply_queues[topic].put(data)
            return
        handler = self._handlers.get(topic)
        if handler is not None:
            await handler(data)

    async def request(self, address: str, data: bytes, timeout: float) -> bytes:
        reply_address = f"_reply.{_uuid7()}"
        reply_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        self._reply_queues[reply_address] = reply_queue
        try:
            message = self._serializer.deserialize(data)
            message.reply_to = reply_address
            data = self._serializer.serialize(message)
            await self._broker.deliver(address, data)
            async with asyncio.timeout(timeout):
                return await reply_queue.get()
        finally:
            self._reply_queues.pop(reply_address, None)

    def has_reply_address(self, address: str) -> bool:
        return address in self._reply_queues


# ---------------------------------------------------------------------------
# Test agents
# ---------------------------------------------------------------------------


class CleanExitOnMsgAgent(AgentProcess):
    """Stops cleanly on the first business message it handles."""

    async def handle(self, message: Message) -> Message | None:
        self._status = ProcessStatus.STOPPING
        return None


class RecordingDriver(AgentProcess):
    """Calls spawn_into on request and records child terminations."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.terminated: list[tuple[str, str]] = []

    async def on_child_terminated(self, name: str, reason: str) -> None:
        self.terminated.append((name, reason))

    async def handle(self, message: Message) -> Message | None:
        p = message.payload
        module_path, _, class_name = str(p["class_path"]).rpartition(".")
        cls = getattr(importlib.import_module(module_path), class_name)
        try:
            name = await self.spawn_into(
                p["target"], cls, p["child"], p.get("config"), wait=p.get("wait", True)
            )
            return self.reply({"ok": True, "name": name})
        except SpawnError as exc:
            return self.reply({"ok": False, "error": str(exc)})


# ---------------------------------------------------------------------------
# Harness helpers
# ---------------------------------------------------------------------------


async def _distributed_supervisor(
    serializer: Any,
    *,
    llm: Any = None,
    tools: Any = None,
    store: Any = None,
) -> tuple[DynamicSupervisor, _BridgedTransport, LocalRegistry, dict[str, list[bytes]]]:
    broker = _SharedBroker()
    reg = LocalRegistry()
    transport = _BridgedTransport(serializer, broker)
    tracer = Tracer()
    bus = MessageBus(transport, reg, serializer, tracer)
    await transport.start()

    sup = DynamicSupervisor("workers")
    sup._bus = bus
    sup._registry = reg
    sup._tracer = tracer
    sup.llm = llm
    sup.tools = tools
    sup.store = store
    reg.register("workers", capabilities=[DYNAMIC_SUPERVISOR_CAPABILITY])
    await bus.setup_agent(sup)

    captured: dict[str, list[bytes]] = {"_agency.register": [], "_agency.deregister": []}

    async def _cap_reg(data: bytes) -> None:
        captured["_agency.register"].append(data)

    async def _cap_dereg(data: bytes) -> None:
        captured["_agency.deregister"].append(data)

    await transport.subscribe("_agency.register", _cap_reg)
    await transport.subscribe("_agency.deregister", _cap_dereg)
    return sup, transport, reg, captured


async def _dispatch(sup: DynamicSupervisor, msg: Message) -> Message | None:
    sup._current_message = msg
    result = await sup.handle(msg)
    sup._current_message = None
    return result


async def _spawn(
    sup: DynamicSupervisor,
    name: str,
    *,
    class_path: str = "tests.conftest.EchoAgent",
    wait: bool = True,
    spawn_id: str = "",
    spawner: str = "x",
    config: dict[str, Any] | None = None,
) -> Message | None:
    msg = Message(
        type="civitas.dynamic.spawn",
        sender=spawner,
        recipient="workers",
        payload={
            "class_path": class_path,
            "name": name,
            "config": config or {},
            "spawner": spawner,
            "wait": wait,
            "spawn_id": spawn_id,
        },
    )
    return await _dispatch(sup, msg)


async def _despawn(sup: DynamicSupervisor, name: str) -> Message | None:
    msg = Message(
        type="civitas.dynamic.despawn", sender="x", recipient="workers", payload={"name": name}
    )
    return await _dispatch(sup, msg)


async def _stop_sup(sup: DynamicSupervisor, transport: _BridgedTransport) -> None:
    await sup.on_stop()
    await transport.stop()


class _Harness:
    def __init__(self, rt: Runtime, worker: Worker, sup: DynamicSupervisor) -> None:
        self.rt = rt
        self.worker = worker
        self.sup = sup

    async def stop(self) -> None:
        await self.worker.stop()
        await self.rt.stop()


async def _make_harness(
    *,
    llm: Any = None,
    tools: Any = None,
    extra_a_children: list[Any] | None = None,
) -> _Harness:
    broker = _SharedBroker()
    ser_a = MsgpackSerializer()
    cs_a = ComponentSet(
        transport=_BridgedTransport(ser_a, broker),
        registry=LocalRegistry(),
        serializer=ser_a,
        tracer=Tracer(),
        store=InMemoryStateStore(),
    )
    rt = Runtime(supervisor=Supervisor("root", children=extra_a_children or []), components=cs_a)
    await rt.start()

    ser_b = MsgpackSerializer()
    cs_b = ComponentSet(
        transport=_BridgedTransport(ser_b, broker),
        registry=LocalRegistry(),
        serializer=ser_b,
        tracer=Tracer(),
        store=InMemoryStateStore(),
        model_provider=llm,
        tool_registry=tools,
    )
    sup = DynamicSupervisor("workers")
    worker = Worker(agents=[sup], components=cs_b)
    await worker.start()
    await wait_for(
        lambda: rt._registry.lookup("workers") is not None, timeout=3.0, msg="workers announce"
    )
    return _Harness(rt, worker, sup)


def _signing_serializer(local: dict[str, Any], trusted: dict[str, Any]) -> tuple[Any, Any, Any]:
    cfg = SigningConfig(enabled=True, require_verification=True, allow_unsigned=False)
    kr = KeyRegistry()
    for name, identity in local.items():
        kr.register(name, identity.verify_key)
    for name, verify_key in trusted.items():
        kr.register(name, verify_key)
    signer = MessageSigner(dict(local), kr, cfg)
    return SigningSerializer(signer, cfg), signer, kr


# ---------------------------------------------------------------------------
# Task 1 — worker-hosted DynamicSupervisor + full-ComponentSet assertion
# ---------------------------------------------------------------------------


class TestWorkerHosted:
    async def test_worker_hosted_supervisor_full_componentset(self) -> None:
        broker = _SharedBroker()
        ser = MsgpackSerializer()
        llm, tools, store = object(), object(), InMemoryStateStore()
        cs_b = ComponentSet(
            transport=_BridgedTransport(ser, broker),
            registry=LocalRegistry(),
            serializer=ser,
            tracer=Tracer(),
            store=store,
            model_provider=llm,
            tool_registry=tools,
        )
        sup = DynamicSupervisor("workers")
        worker = Worker(agents=[sup], components=cs_b)
        await worker.start()
        try:
            assert sup.status == ProcessStatus.RUNNING
            assert sup._is_distributed() is True
            assert sup._registry is sup._bus._registry
            req = Message(
                type="civitas.dynamic.spawn",
                sender="_t",
                recipient="workers",
                payload={
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "child-1",
                    "config": {},
                    "spawner": "_t",
                    "wait": True,
                    "spawn_id": "s1",
                },
                correlation_id=_uuid7(),
            )
            reply = await cs_b.bus.request(req, timeout=5.0)
            assert reply.payload["status"] == "ok"
            child = sup._dynamic_children["child-1"].agent
            assert child.llm is llm
            assert child.tools is tools
            assert child.store is store
        finally:
            await worker.stop()

    async def test_worker_hosted_supervisor_ok_when_registry_matches(self) -> None:
        broker = _SharedBroker()
        ser = MsgpackSerializer()
        transport = _BridgedTransport(ser, broker)
        await transport.start()
        reg = LocalRegistry()
        bus = MessageBus(transport, reg, ser, Tracer())
        sup = DynamicSupervisor("workers")
        sup._bus = bus
        sup._registry = reg
        await sup.on_start()  # must not raise
        await transport.stop()

    async def test_worker_hosted_supervisor_rejects_mismatched_registry(self) -> None:
        broker = _SharedBroker()
        ser = MsgpackSerializer()
        transport = _BridgedTransport(ser, broker)
        await transport.start()
        bus = MessageBus(transport, LocalRegistry(), ser, Tracer())
        sup = DynamicSupervisor("workers")
        sup._bus = bus
        sup._registry = LocalRegistry()  # different instance than bus._registry
        with pytest.raises(ConfigurationError, match="distributed registry"):
            await sup.on_start()
        await transport.stop()

    async def test_worker_hosted_supervisor_requires_registry(self) -> None:
        broker = _SharedBroker()
        ser = MsgpackSerializer()
        transport = _BridgedTransport(ser, broker)
        await transport.start()
        bus = MessageBus(transport, LocalRegistry(), ser, Tracer())
        sup = DynamicSupervisor("workers")
        sup._bus = bus
        sup._registry = None
        with pytest.raises(ConfigurationError):
            await sup.on_start()
        await transport.stop()

    async def test_inprocess_supervisor_on_start_is_noop(self) -> None:
        sup = DynamicSupervisor("workers")  # no bus → not distributed
        await sup.on_start()  # must not raise
        assert sup._is_distributed() is False


# ---------------------------------------------------------------------------
# Task 2 — announce children cluster-wide, after-start, epoch, deregister
# ---------------------------------------------------------------------------


class TestAnnounce:
    async def test_announce_published_after_running_with_epoch(self) -> None:
        sup, transport, _reg, captured = await _distributed_supervisor(MsgpackSerializer())
        try:
            reply = await _spawn(sup, "child-1", spawn_id="s1")
            assert reply is not None and reply.payload["status"] == "ok"
            assert reply.payload["ready"] is True
            assert len(captured["_agency.register"]) == 1
            ann = MsgpackSerializer().deserialize(captured["_agency.register"][0])
            assert ann.type == "_agency.register"
            assert ann.sender == "workers"
            assert ann.payload["name"] == "child-1"
            assert ann.payload["epoch"] >= 1
            assert ann.payload["pubkey"] == ""
            assert "capabilities" in ann.payload
            assert "capability_metadata" in ann.payload
            rec = sup._dynamic_children["child-1"]
            assert rec.agent.status == ProcessStatus.RUNNING
            assert rec.announced is True
        finally:
            await _stop_sup(sup, transport)

    async def test_announce_wait_false_after_start(self) -> None:
        sup, transport, _reg, captured = await _distributed_supervisor(MsgpackSerializer())
        try:
            reply = await _spawn(sup, "child-2", spawn_id="s2", wait=False)
            assert reply is not None and reply.payload["status"] == "ok"
            await wait_for(
                lambda: len(captured["_agency.register"]) == 1,
                timeout=3.0,
                msg="announce after start",
            )
            ann = MsgpackSerializer().deserialize(captured["_agency.register"][0])
            assert ann.payload["name"] == "child-2"
            assert sup._dynamic_children["child-2"].announced is True
        finally:
            await _stop_sup(sup, transport)

    async def test_despawn_publishes_deregister_with_epoch(self) -> None:
        sup, transport, _reg, captured = await _distributed_supervisor(MsgpackSerializer())
        try:
            await _spawn(sup, "child-1", spawn_id="s1")
            epoch = (
                MsgpackSerializer().deserialize(captured["_agency.register"][0]).payload["epoch"]
            )
            reply = await _despawn(sup, "child-1")
            assert reply is not None and reply.payload["status"] == "ok"
            assert len(captured["_agency.deregister"]) == 1
            dereg = MsgpackSerializer().deserialize(captured["_agency.deregister"][0])
            assert dereg.type == "_agency.deregister"
            assert dereg.payload["name"] == "child-1"
            assert dereg.payload["epoch"] == epoch
        finally:
            await _stop_sup(sup, transport)

    async def test_inprocess_spawn_no_announce(self) -> None:
        dyn = DynamicSupervisor("workers")
        rt = Runtime(supervisor=Supervisor("root", children=[dyn]))
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, "child-1")
            assert dyn._is_distributed() is False
            assert dyn._dynamic_children["child-1"].announced is False
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# Task 3 — registry ownership + epoch (register_remote / deregister_remote)
# ---------------------------------------------------------------------------


class TestOwnership:
    def test_ownership_rejects_local_name(self) -> None:
        reg = LocalRegistry()
        reg.register("a")
        with pytest.raises(ValueError, match="already registered as local"):
            reg.register_remote("a", owner="w1")

    def test_ownership_rejects_different_owner(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=1)
        with pytest.raises(ValueError, match="already owned"):
            reg.register_remote("c", owner="w2", pubkey="k2", epoch=2)

    def test_ownership_rejects_pubkey_conflict(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=1)
        with pytest.raises(ValueError, match="public key conflict"):
            reg.register_remote("c", owner="w1", pubkey="k2", epoch=1)

    def test_ownership_same_owner_same_epoch_idempotent(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=1)
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=1)
        entry = reg.lookup("c")
        assert entry is not None and entry.epoch == 1

    def test_ownership_newer_epoch_same_owner_updates(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=1)
        reg.register_remote("c", owner="w1", pubkey="k2", epoch=2)
        entry = reg.lookup("c")
        assert entry is not None and entry.pubkey == "k2" and entry.epoch == 2

    def test_ownership_stale_epoch_dropped(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=5)
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=3)
        entry = reg.lookup("c")
        assert entry is not None and entry.epoch == 5

    def test_ownership_no_resurrection_after_deregister(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=5)
        reg.deregister_remote("c", epoch=5)
        assert reg.lookup("c") is None
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=5)  # same epoch, late reorder
        assert reg.lookup("c") is None
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=3)  # older
        assert reg.lookup("c") is None
        reg.register_remote("c", owner="w1", pubkey="k1", epoch=6)  # fresh incarnation
        assert reg.lookup("c") is not None

    def test_deregister_remote_ignores_stale_epoch(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", owner="w1", epoch=5)
        reg.deregister_remote("c", epoch=3)
        assert reg.lookup("c") is not None

    def test_deregister_remote_leaves_local_untouched(self) -> None:
        reg = LocalRegistry()
        reg.register("c")
        reg.deregister_remote("c", epoch=1)
        entry = reg.lookup("c")
        assert entry is not None and entry.is_local is True

    def test_register_remote_backcompat_no_owner(self) -> None:
        reg = LocalRegistry()
        reg.register_remote("c", capabilities=["x"])
        reg.register_remote("c", capabilities=["x"])  # idempotent, no epoch/owner
        entry = reg.lookup("c")
        assert entry is not None and entry.is_local is False
        assert "x" in entry.capabilities


# ---------------------------------------------------------------------------
# Task 3 — runtime receive-side verification (_on_remote_register/deregister)
# ---------------------------------------------------------------------------


def _receiver_runtime(serializer: Any, key_registry: Any = None) -> Runtime:
    rt = Runtime()
    rt._registry = LocalRegistry()
    rt._serializer = serializer
    rt._key_registry = key_registry
    return rt


class TestReceiveVerification:
    async def test_unsigned_announce_dropped_when_signing_on(self) -> None:
        if not _HAS_NACL:
            pytest.skip("pynacl not installed")
        workers = AgentIdentity.generate("workers")
        ser_verify, _signer, _kr = _signing_serializer({}, {"workers": workers.verify_key})
        rt = _receiver_runtime(ser_verify, KeyRegistry())
        plain = MsgpackSerializer()
        data = plain.serialize(
            Message(type="_agency.register", sender="workers", payload={"name": "evil", "epoch": 1})
        )
        await rt._on_remote_register(data)
        assert rt._registry.lookup("evil") is None

    async def test_unknown_signer_announce_dropped(self) -> None:
        if not _HAS_NACL:
            pytest.skip("pynacl not installed")
        workers = AgentIdentity.generate("workers")
        attacker = AgentIdentity.generate("attacker")
        ser_verify, _s, _kr = _signing_serializer({}, {"workers": workers.verify_key})
        ser_attacker, _s2, _kr2 = _signing_serializer({"attacker": attacker}, {})
        rt = _receiver_runtime(ser_verify, KeyRegistry())
        data = ser_attacker.serialize(
            Message(
                type="_agency.register", sender="attacker", payload={"name": "evil2", "epoch": 1}
            )
        )
        await rt._on_remote_register(data)
        assert rt._registry.lookup("evil2") is None

    async def test_signed_announce_registers_and_stores_child_pubkey(self) -> None:
        if not _HAS_NACL:
            pytest.skip("pynacl not installed")
        workers = AgentIdentity.generate("workers")
        ser_sign, _s, _kr = _signing_serializer({"workers": workers}, {})
        ser_verify, _s2, kr_verify = _signing_serializer({}, {"workers": workers.verify_key})
        rt_key_registry = KeyRegistry()
        rt = _receiver_runtime(ser_verify, rt_key_registry)
        child = AgentIdentity.generate("child")
        data = ser_sign.serialize(
            Message(
                type="_agency.register",
                sender="workers",
                payload={
                    "name": "child",
                    "pubkey": child.public_key_b64(),
                    "epoch": 1,
                    "capabilities": ["text.summarize"],
                },
            )
        )
        await rt._on_remote_register(data)
        entry = rt._registry.lookup("child")
        assert entry is not None
        assert entry.owner == "workers"
        assert entry.epoch == 1
        assert "text.summarize" in entry.capabilities
        assert rt_key_registry.get("child") is not None

    async def test_receive_rejects_owner_takeover(self) -> None:
        if not _HAS_NACL:
            pytest.skip("pynacl not installed")
        w1 = AgentIdentity.generate("w1")
        w2 = AgentIdentity.generate("w2")
        ser_w1, _a, _b = _signing_serializer({"w1": w1}, {})
        ser_w2, _c, _d = _signing_serializer({"w2": w2}, {})
        ser_verify, _e, _f = _signing_serializer({}, {"w1": w1.verify_key, "w2": w2.verify_key})
        rt = _receiver_runtime(ser_verify, KeyRegistry())
        c1 = AgentIdentity.generate("c1")
        c2 = AgentIdentity.generate("c2")
        await rt._on_remote_register(
            ser_w1.serialize(
                Message(
                    type="_agency.register",
                    sender="w1",
                    payload={"name": "shared", "pubkey": c1.public_key_b64(), "epoch": 1},
                )
            )
        )
        await rt._on_remote_register(
            ser_w2.serialize(
                Message(
                    type="_agency.register",
                    sender="w2",
                    payload={"name": "shared", "pubkey": c2.public_key_b64(), "epoch": 2},
                )
            )
        )
        entry = rt._registry.lookup("shared")
        assert entry is not None and entry.owner == "w1"

    async def test_deregister_removes_remote_route(self) -> None:
        rt = _receiver_runtime(MsgpackSerializer(), None)
        rt._registry.register_remote("child", owner="workers", epoch=1)
        data = MsgpackSerializer().serialize(
            Message(
                type="_agency.deregister",
                sender="workers",
                payload={"name": "child", "epoch": 1},
            )
        )
        await rt._on_remote_deregister(data)
        assert rt._registry.lookup("child") is None

    async def test_register_without_signing_uses_name_only(self) -> None:
        rt = _receiver_runtime(MsgpackSerializer(), None)
        data = MsgpackSerializer().serialize(
            Message(
                type="_agency.register",
                sender="workers",
                payload={"name": "child", "capabilities": ["x"], "epoch": 1},
            )
        )
        await rt._on_remote_register(data)
        entry = rt._registry.lookup("child")
        assert entry is not None and "x" in entry.capabilities


# ---------------------------------------------------------------------------
# Task 4 — per-incarnation child identity
# ---------------------------------------------------------------------------


@_requires_nacl
class TestIdentity:
    async def test_announce_signed_carries_minted_child_pubkey(self) -> None:
        workers = AgentIdentity.generate("workers")
        ser, signer, _kr = _signing_serializer({"workers": workers}, {})
        sup, transport, _reg, captured = await _distributed_supervisor(ser)
        try:
            reply = await _spawn(sup, "child-1", spawn_id="s1")
            assert reply is not None and reply.payload["status"] == "ok"
            raw = captured["_agency.register"][0]
            envelope = msgpack.unpackb(raw, raw=False)
            assert envelope["v"] == 2
            assert envelope["sig"]["signer"] == "workers"
            assert envelope["sig"]["value"]  # non-empty Ed25519 signature
            ann = ser.deserialize(raw)
            assert ann.payload["pubkey"]
            assert "child-1" in signer._identities
            assert ann.payload["pubkey"] == signer._identities["child-1"].public_key_b64()
        finally:
            await _stop_sup(sup, transport)

    async def test_identity_per_incarnation_distinct_pubkeys(self) -> None:
        workers = AgentIdentity.generate("workers")
        ser, _signer, _kr = _signing_serializer({"workers": workers}, {})
        sup, transport, _reg, captured = await _distributed_supervisor(ser)
        try:
            await _spawn(sup, "child-1", spawn_id="s1")
            pub1 = ser.deserialize(captured["_agency.register"][0]).payload["pubkey"]
            await _despawn(sup, "child-1")
            await _spawn(sup, "child-1", spawn_id="s2")
            pub2 = ser.deserialize(captured["_agency.register"][-1]).payload["pubkey"]
            assert pub1 and pub2 and pub1 != pub2
        finally:
            await _stop_sup(sup, transport)

    async def test_peer_verifies_child_message_against_announced_key(self) -> None:
        workers = AgentIdentity.generate("workers")
        ser, _signer, _kr = _signing_serializer({"workers": workers}, {})
        sup, transport, _reg, captured = await _distributed_supervisor(ser)
        try:
            await _spawn(sup, "child-1", spawn_id="s1")
            child_pub = ser.deserialize(captured["_agency.register"][0]).payload["pubkey"]
            peer_kr = KeyRegistry()
            peer_kr.register_b64("child-1", child_pub)
            peer_ser, _ps, _pk = _signing_serializer({}, {})
            peer_ser._signer._registry = peer_kr
            signed = ser.serialize(Message(type="message", sender="child-1", payload={"hi": 1}))
            verified = peer_ser.deserialize(signed)
            assert verified.payload == {"hi": 1}
        finally:
            await _stop_sup(sup, transport)


# ---------------------------------------------------------------------------
# Task 5 — spawn_id idempotency + termination authority
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_idempotency_same_spawn_id_returns_existing(self) -> None:
        dyn = DynamicSupervisor("workers")
        rt = Runtime(supervisor=Supervisor("root", children=[dyn]))
        await rt.start()
        try:
            payload = {
                "class_path": "tests.conftest.EchoAgent",
                "name": "child-1",
                "config": {},
                "spawner": "_t",
                "wait": True,
                "spawn_id": "same-token",
            }
            r1 = await rt.ask("workers", dict(payload), message_type="civitas.dynamic.spawn")
            r2 = await rt.ask("workers", dict(payload), message_type="civitas.dynamic.spawn")
            assert r1.payload["status"] == "ok"
            assert r2.payload["status"] == "ok"
            assert dyn._total_spawns == 1
            assert len(dyn._dynamic_children) == 1
        finally:
            await rt.stop()

    async def test_different_spawn_id_same_name_rejected(self) -> None:
        dyn = DynamicSupervisor("workers")
        rt = Runtime(supervisor=Supervisor("root", children=[dyn]))
        await rt.start()
        try:
            base = {
                "class_path": "tests.conftest.EchoAgent",
                "name": "child-1",
                "config": {},
                "spawner": "_t",
                "wait": True,
            }
            r1 = await rt.ask(
                "workers", {**base, "spawn_id": "t1"}, message_type="civitas.dynamic.spawn"
            )
            r2 = await rt.ask(
                "workers", {**base, "spawn_id": "t2"}, message_type="civitas.dynamic.spawn"
            )
            assert r1.payload["status"] == "ok"
            assert r2.payload["status"] == "error"
            assert "already running" in r2.payload["reason"]
        finally:
            await rt.stop()

    async def test_spawn_into_includes_spawn_id(self) -> None:
        recorded: dict[str, Any] = {}

        class CapturingSup(DynamicSupervisor):
            async def handle(self, message: Message) -> Message | None:
                if message.type == "civitas.dynamic.spawn":
                    recorded["payload"] = dict(message.payload)
                return await super().handle(message)

        driver = RecordingDriver("driver")
        sup = CapturingSup("workers")
        rt = Runtime(supervisor=Supervisor("root", children=[driver, sup]))
        await rt.start()
        try:
            reply = await rt.ask(
                "driver",
                {
                    "op": "spawn_into",
                    "target": "workers",
                    "child": "child-1",
                    "class_path": "tests.conftest.EchoAgent",
                },
            )
            assert reply.payload["ok"] is True
            assert recorded["payload"].get("spawn_id")
            assert len(recorded["payload"]["spawn_id"]) >= 32
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# Integrated cross-process — full two-process emulation
# ---------------------------------------------------------------------------


class TestCrossProcessIntegration:
    async def test_cross_process_spawn_ask_and_despawn(self) -> None:
        h = await _make_harness()
        try:
            entry = h.rt._registry.lookup("workers")
            assert entry is not None
            assert DYNAMIC_SUPERVISOR_CAPABILITY in entry.capabilities
            name = await h.rt.spawn("workers", EchoAgent, "child-1")
            assert name == "child-1"
            assert h.rt._registry.lookup("child-1") is not None  # announced into A
            reply = await h.rt.ask("child-1", {"msg": "hi"}, timeout=5.0)
            assert reply.payload["echo"]["msg"] == "hi"
            await h.rt.despawn("workers", "child-1")
            assert h.rt._registry.lookup("child-1") is None  # deregistered from A
        finally:
            await h.stop()

    async def test_cross_process_termination_notifies_and_deregisters(self) -> None:
        driver = RecordingDriver("driver")
        h = await _make_harness(extra_a_children=[driver])
        try:
            # Let the worker route notifications back to the runtime-side spawner.
            h.worker._registry.register_remote("driver", owner="", epoch=0)
            reply = await h.rt.ask(
                "driver",
                {
                    "op": "spawn_into",
                    "target": "workers",
                    "child": "child-x",
                    "class_path": ("tests.unit.test_cross_process_spawn.CleanExitOnMsgAgent"),
                },
            )
            assert reply.payload["ok"] is True
            assert h.rt._registry.lookup("child-x") is not None
            await h.rt.send("child-x", {"go": 1})
            await wait_for(
                lambda: h.rt._registry.lookup("child-x") is None,
                timeout=3.0,
                msg="cross-process deregister",
            )
            await wait_for(
                lambda: any(n == "child-x" for n, _ in driver.terminated),
                timeout=3.0,
                msg="on_child_terminated",
            )
            assert driver.terminated[0][1] == "clean_exit"
        finally:
            await h.stop()
