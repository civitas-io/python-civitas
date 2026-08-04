"""Examples smoke test (v0.9.2) — proves every runnable example in `examples/`
actually runs, instead of trusting that it does.

Found the reason this exists the hard way: `examples/dynamic_spawning.py` shipped with
three separate broken API calls (v0.9.1) and `examples/deployment/level2_multi_process/
run_worker.py` shipped calling a `Worker.from_config()` classmethod that has never
existed (v0.9.2) — both silent, because no example file in this repo had ever been
exercised by the test suite. This file is the structural fix: not exhaustive coverage
of what each example DOES, just proof that each one runs to completion (or shuts down
cleanly) without crashing.

Three distinct shapes, because examples come in three distinct shapes:

- **Run-to-completion scripts** — start, do their demo, exit on their own. Simple:
  run, assert exit code 0.
- **Long-running servers** — `http_gateway.py` and the Level 2 multi-process
  supervisor/worker scripts `await asyncio.sleep(3600)` or `stop_event.wait()`; they
  never exit on their own by design (a real server doesn't either). These are
  launched, given a moment to prove they didn't crash on startup, sent SIGINT
  (confirmed by hand to be caught by all three — some only handle SIGINT via
  `KeyboardInterrupt`, not SIGTERM, which has no handler and kills them outright),
  and must then exit cleanly (0) within a bounded wait.
- **Paired long-running scripts** — `cross_process_spawn`'s supervisor/worker only
  make sense started together, in a specific order (the proxy owner first — found
  running this exact pair the other way around, which just times out). One dedicated
  test starts both, lets the supervisor run its full demo to completion (proving the
  actual cross-process spawn round trip succeeded, not just "didn't crash"), then
  signals the worker and checks its clean shutdown too.

Excluded, each for a real reason (not silently omitted — see EXCLUDED_EXAMPLES):
real external API calls, an external MCP server process, extra packages not in any
core extras group, or genuine multi-host distributed transports.
"""

from __future__ import annotations

import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# --- Run-to-completion: start, demo, exit on their own. ---------------------
RUN_TO_COMPLETION = [
    "examples/hello_agent.py",
    "examples/supervised_agent.py",
    "examples/supervision_tree.py",
    "examples/dynamic_spawning.py",
    "examples/eval_agent.py",
    "examples/observable_pipeline.py",
    "examples/research_assistant.py",  # uses a mock LLM by default, no API key
    "examples/research_pipeline.py",
    "examples/rate_limiter.py",
    "examples/quickstart/01_hello_agent.py",
    "examples/quickstart/02_supervised_agent.py",
    "examples/quickstart/03_multi_agent.py",
    "examples/quickstart/04_with_llm.py",  # mock provider by default, no --live
    "examples/patterns/fan_out_fan_in.py",
    "examples/patterns/human_in_the_loop.py",  # simulates approvals, no real stdin
    "examples/patterns/pipeline.py",
    "examples/patterns/router.py",
    "examples/deployment/level1_single_process/main.py",
    "examples/non_blocking_spawn.py",
    "examples/supervision_introspection.py",
    "examples/custom_plugin.py",
    "examples/streaming_response.py",
    "examples/secured_messaging.py",  # needs civitas[zmq,security], both in CI's dev extras
    "examples/grpc_gateway.py",  # grpcio* ships as part of the dev extra itself
    "examples/gateway_auth.py",  # pyjwt[crypto] ships as part of the dev extra itself
    "examples/control_plane_auth.py",  # civitas[http] (uvicorn) ships in the dev extra
]

# --- Long-running servers: launch, confirm no startup crash, SIGINT, must exit
# cleanly within the wait below. Never expected to exit on their own. --------
LONG_RUNNING_SERVERS = [
    "examples/http_gateway.py",
    "examples/deployment/level2_multi_process/run_supervisor.py",
    "examples/deployment/level2_multi_process/run_worker.py",
]

# --- Paired long-running scripts: see this module's docstring for why these two
# need a dedicated test rather than being run independently. -----------------
PAIRED_LONG_RUNNING = [
    "examples/cross_process_spawn/run_supervisor.py",
    "examples/cross_process_spawn/run_worker.py",
]

# --- Deliberately excluded, with the exact reason — not silently absent. ----
EXCLUDED_EXAMPLES = {
    "examples/self_sufficient_agent.py": "makes a real Anthropic API call, no mock fallback",
    "examples/mcp_agent.py": "needs an external MCP server process (npx)",
    "examples/frameworks/langgraph_on_civitas.py": "needs the langgraph package installed",
    "examples/frameworks/openai_sdk_on_civitas.py": "needs the openai-agents package installed",
    "examples/deployment/level3_distributed/run_supervisor.py": "needs a real NATS server",
    "examples/deployment/level3_distributed/run_worker.py": "needs a real NATS server",
    "examples/stateful_workflow.py": "writes a persistent civitas_state.db to cwd (its whole point is on-disk crash recovery); running in-tree would leave a state artifact. Imports core now, not contrib (v0.11.0 B4)",
}

# examples/deployment/level4_docker/agents.py is intentionally absent from every
# list above — it has no `if __name__ == "__main__":` guard at all (Docker
# Compose-only, imported by the container entrypoint, never run directly), so
# it's correctly outside this file's scope entirely, not merely excluded.


def _run_to_completion(rel_path: str, timeout: float = 30.0) -> None:
    result = subprocess.run(
        [sys.executable, rel_path],
        cwd=REPO_ROOT,
        capture_output=True,
        timeout=timeout,
    )
    assert result.returncode == 0, (
        f"{rel_path} exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout.decode(errors='replace')[-3000:]}\n"
        f"--- stderr ---\n{result.stderr.decode(errors='replace')[-3000:]}"
    )


def _run_long_running_server(
    rel_path: str, startup_wait: float = 2.0, shutdown_wait: float = 10.0
) -> None:
    proc = subprocess.Popen(
        [sys.executable, rel_path],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        # Give it a moment to bind ports / start its message loop. If it crashes
        # in that window (e.g. a port conflict, a bad import), fail loudly with
        # the actual output rather than waiting out the full shutdown timeout.
        try:
            out, _ = proc.communicate(timeout=startup_wait)
            pytest.fail(
                f"{rel_path} exited on its own during startup (expected to run "
                f"until signaled), exit={proc.returncode}\n{out.decode(errors='replace')[-3000:]}"
            )
        except subprocess.TimeoutExpired:
            pass  # still running after startup_wait — the expected case

        # Confirmed by hand (design notes, v0.9.2): SIGINT (not SIGTERM) is the
        # signal all three of these scripts actually handle today — some only
        # via `except KeyboardInterrupt`, which SIGTERM never raises.
        proc.send_signal(signal.SIGINT)
        try:
            out, _ = proc.communicate(timeout=shutdown_wait)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
            pytest.fail(
                f"{rel_path} did not shut down within {shutdown_wait}s of SIGINT "
                f"(had to kill it)\n{out.decode(errors='replace')[-3000:]}"
            )
        assert proc.returncode == 0, (
            f"{rel_path} did not exit cleanly after SIGINT, exit={proc.returncode}\n"
            f"{out.decode(errors='replace')[-3000:]}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@pytest.mark.parametrize("rel_path", RUN_TO_COMPLETION)
def test_example_runs_to_completion(rel_path: str) -> None:
    _run_to_completion(rel_path)


@pytest.mark.parametrize("rel_path", LONG_RUNNING_SERVERS)
def test_example_long_running_server_shuts_down_cleanly(rel_path: str) -> None:
    _run_long_running_server(rel_path)


def test_cross_process_spawn_pair_completes_and_shuts_down_cleanly() -> None:
    """cross_process_spawn's supervisor/worker only make sense started
    together, in a specific order (found running this exact pair: the proxy
    owner -- the supervisor, zmq_start_proxy=True -- must start FIRST, or the
    worker tries to connect before any proxy exists and the supervisor's own
    wait for its announcement just times out).

    Unlike the other long-running pair (Level 2), this one's supervisor script
    is written to run TO COMPLETION -- it proves the actual cross-process spawn
    round trip succeeded (spawn, ask, despawn) and exits 0 on its own, rather
    than needing to be signaled. Only the worker needs signaling afterward.
    """
    supervisor_path, worker_path = PAIRED_LONG_RUNNING
    supervisor = subprocess.Popen(
        [sys.executable, supervisor_path],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    worker: subprocess.Popen | None = None
    try:
        time.sleep(1.0)  # let the supervisor's proxy bind before the worker connects
        worker = subprocess.Popen(
            [sys.executable, worker_path],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            sup_out, _ = supervisor.communicate(timeout=20.0)
        except subprocess.TimeoutExpired:
            supervisor.kill()
            sup_out, _ = supervisor.communicate()
            pytest.fail(
                f"{supervisor_path} did not complete within 20s\n"
                f"{sup_out.decode(errors='replace')[-3000:]}"
            )
        assert supervisor.returncode == 0, (
            f"{supervisor_path} exited {supervisor.returncode}\n"
            f"{sup_out.decode(errors='replace')[-3000:]}"
        )

        # A brief settle buffer before signaling the worker -- found running
        # this exact pair back-to-back: signaling it the instant the
        # supervisor's cross-process traffic finishes is a genuine (if
        # narrow) race against the worker's own event loop mid-cleanup.
        time.sleep(0.5)
        worker.send_signal(signal.SIGINT)
        try:
            worker_out, _ = worker.communicate(timeout=10.0)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker_out, _ = worker.communicate()
            pytest.fail(
                f"{worker_path} did not shut down within 10s of SIGINT (had to kill it)\n"
                f"{worker_out.decode(errors='replace')[-3000:]}"
            )
        assert worker.returncode == 0, (
            f"{worker_path} did not exit cleanly after SIGINT, exit={worker.returncode}\n"
            f"{worker_out.decode(errors='replace')[-3000:]}"
        )
    finally:
        if worker is not None and worker.poll() is None:
            worker.kill()
            worker.wait()
        if supervisor.poll() is None:
            supervisor.kill()
            supervisor.wait()


def test_every_example_is_accounted_for() -> None:
    """Every top-level runnable script under examples/ is either tested above or
    explicitly excluded with a reason — a new example added later can't silently
    fall through both lists without this failing.

    "Runnable" = has an `if __name__ == "__main__":` guard. Modules like
    `agents.py`/`__init__.py` that only ever get imported are correctly outside
    this file's scope entirely.
    """
    accounted_for = (
        set(RUN_TO_COMPLETION)
        | set(LONG_RUNNING_SERVERS)
        | set(PAIRED_LONG_RUNNING)
        | set(EXCLUDED_EXAMPLES)
    )
    # 'if __name__ == "__main__":', not a bare "__main__" substring match --
    # examples/cross_process_spawn/agents.py's own docstring mentions the
    # string "__main__" in prose (explaining a DIFFERENT, real constraint
    # about cross-process class resolution) without having an actual runnable
    # guard, and a naive substring check flagged it as a false positive.
    _main_guard = re.compile(r'if\s+__name__\s*==\s*["\']__main__["\']\s*:')
    runnable = {
        str(p.relative_to(REPO_ROOT))
        for p in (REPO_ROOT / "examples").rglob("*.py")
        if _main_guard.search(p.read_text(encoding="utf-8"))
    }
    missing = runnable - accounted_for
    assert not missing, (
        f"New runnable example(s) found with no smoke-test coverage and no "
        f"documented exclusion reason: {sorted(missing)} — add to RUN_TO_COMPLETION, "
        f"LONG_RUNNING_SERVERS, PAIRED_LONG_RUNNING, or EXCLUDED_EXAMPLES in this file."
    )
    stale = accounted_for - runnable
    assert not stale, (
        f"Tracked example(s) no longer exist or lost their __main__ guard: {sorted(stale)}"
    )
