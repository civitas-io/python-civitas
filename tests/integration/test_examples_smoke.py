"""Examples smoke test (v0.9.2) — proves every runnable example in `examples/`
actually runs, instead of trusting that it does.

Found the reason this exists the hard way: `examples/dynamic_spawning.py` shipped with
three separate broken API calls (v0.9.1) and `examples/deployment/level2_multi_process/
run_worker.py` shipped calling a `Worker.from_config()` classmethod that has never
existed (v0.9.2) — both silent, because no example file in this repo had ever been
exercised by the test suite. This file is the structural fix: not exhaustive coverage
of what each example DOES, just proof that each one runs to completion (or shuts down
cleanly) without crashing.

Two distinct shapes, because examples come in two distinct shapes:

- **Run-to-completion scripts** — start, do their demo, exit on their own. Simple:
  run, assert exit code 0.
- **Long-running servers** — `http_gateway.py` and the Level 2 multi-process
  supervisor/worker scripts `await asyncio.sleep(3600)` or `stop_event.wait()`; they
  never exit on their own by design (a real server doesn't either). These are
  launched, given a moment to prove they didn't crash on startup, sent SIGINT
  (confirmed by hand to be caught by all three — some only handle SIGINT via
  `KeyboardInterrupt`, not SIGTERM, which has no handler and kills them outright),
  and must then exit cleanly (0) within a bounded wait.

Excluded, each for a real reason (not silently omitted — see EXCLUDED_EXAMPLES):
real external API calls, an external MCP server process, extra packages not in any
core extras group, or genuine multi-host distributed transports.
"""

from __future__ import annotations

import signal
import subprocess
import sys
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
]

# --- Long-running servers: launch, confirm no startup crash, SIGINT, must exit
# cleanly within the wait below. Never expected to exit on their own. --------
LONG_RUNNING_SERVERS = [
    "examples/http_gateway.py",
    "examples/deployment/level2_multi_process/run_supervisor.py",
    "examples/deployment/level2_multi_process/run_worker.py",
]

# --- Deliberately excluded, with the exact reason — not silently absent. ----
EXCLUDED_EXAMPLES = {
    "examples/self_sufficient_agent.py": "makes a real Anthropic API call, no mock fallback",
    "examples/mcp_agent.py": "needs an external MCP server process (npx)",
    "examples/frameworks/langgraph_on_civitas.py": "needs the langgraph package installed",
    "examples/frameworks/openai_sdk_on_civitas.py": "needs the openai-agents package installed",
    "examples/deployment/level3_distributed/run_supervisor.py": "needs a real NATS server",
    "examples/deployment/level3_distributed/run_worker.py": "needs a real NATS server",
    "examples/stateful_workflow.py": "needs civitas-contrib (SQLiteStateStore) — see the fix that gave it a real import path this same release",
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


def test_every_example_is_accounted_for() -> None:
    """Every top-level runnable script under examples/ is either tested above or
    explicitly excluded with a reason — a new example added later can't silently
    fall through both lists without this failing.

    "Runnable" = has an `if __name__ == "__main__":` guard. Modules like
    `agents.py`/`__init__.py` that only ever get imported are correctly outside
    this file's scope entirely.
    """
    accounted_for = set(RUN_TO_COMPLETION) | set(LONG_RUNNING_SERVERS) | set(EXCLUDED_EXAMPLES)
    runnable = {
        str(p.relative_to(REPO_ROOT))
        for p in (REPO_ROOT / "examples").rglob("*.py")
        if "__main__" in p.read_text(encoding="utf-8")
    }
    missing = runnable - accounted_for
    assert not missing, (
        f"New runnable example(s) found with no smoke-test coverage and no "
        f"documented exclusion reason: {sorted(missing)} — add to RUN_TO_COMPLETION, "
        f"LONG_RUNNING_SERVERS, or EXCLUDED_EXAMPLES in this file."
    )
    stale = accounted_for - runnable
    assert not stale, (
        f"Tracked example(s) no longer exist or lost their __main__ guard: {sorted(stale)}"
    )
