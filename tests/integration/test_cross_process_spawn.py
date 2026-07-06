"""M2.9 — Cross-process dynamic spawn over real ZMQ (v0.7.0 · R6).

A Runtime (process A) and a Worker (process B) share a ZMQ proxy. The Worker
hosts a DynamicSupervisor; a spawn routed from A places the child in B, the
child is announced cluster-wide, A can ask it, and despawn deregisters it.
"""

import os
import tempfile

import pytest

pytest.importorskip("zmq", reason="pyzmq not installed — skipping ZMQ cross-process spawn tests")

from civitas import DynamicSupervisor, Runtime, Supervisor, Worker
from civitas.process import DYNAMIC_SUPERVISOR_CAPABILITY
from tests.conftest import EchoAgent, wait_for


@pytest.fixture
def zmq_addrs():
    d = tempfile.mkdtemp()
    frontend = f"ipc://{d}/frontend.sock"
    backend = f"ipc://{d}/backend.sock"
    yield frontend, backend
    for f in os.listdir(d):
        os.unlink(os.path.join(d, f))
    os.rmdir(d)


async def test_cross_process_spawn_ask_and_despawn_over_zmq(zmq_addrs):
    frontend, backend = zmq_addrs
    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()

    worker = Worker(
        agents=[DynamicSupervisor("workers")],
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
    )
    await worker.start()

    try:
        # The worker-hosted supervisor is discoverable as a DynamicSupervisor.
        await wait_for(
            lambda: runtime._registry.lookup("workers") is not None,
            timeout=5.0,
            msg="workers announced",
        )
        entry = runtime._registry.lookup("workers")
        assert entry is not None
        assert DYNAMIC_SUPERVISOR_CAPABILITY in entry.capabilities

        # Spawn a child in the remote supervisor and route to it from A.
        name = await runtime.spawn("workers", EchoAgent, "child-1")
        assert name == "child-1"
        await wait_for(
            lambda: runtime._registry.lookup("child-1") is not None,
            timeout=5.0,
            msg="child announced cluster-wide",
        )
        reply = await runtime.ask("child-1", {"msg": "cross-process"}, timeout=5.0)
        assert reply.payload["echo"]["msg"] == "cross-process"

        # Despawn deregisters the child cluster-wide.
        await runtime.despawn("workers", "child-1")
        await wait_for(
            lambda: runtime._registry.lookup("child-1") is None,
            timeout=5.0,
            msg="child deregistered cluster-wide",
        )
    finally:
        await worker.stop()
        await runtime.stop()
