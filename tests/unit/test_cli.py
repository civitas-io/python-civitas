"""Unit tests for the civitas CLI via Typer's CliRunner (V5, #42).

The "dedicated CLI testing sprint" deferred since M3.1. In-process, no
subprocesses, no live runtime. Lesson from the integration gate's first CI
catch applied throughout: NEVER assert on Rich help text substrings (rendering
width is environment-dependent) — assert exit codes and file artifacts.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from typer.testing import CliRunner

from civitas.cli import app

runner = CliRunner()


TOPOLOGY = """
transport:
  type: in_process
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent:
        name: worker
        type: myapp.agents.Worker
"""


def _write_topology(tmp: Path, text: str = TOPOLOGY, name: str = "topology.yaml") -> Path:
    path = tmp / name
    path.write_text(text)
    return path


# ---------------------------------------------------------------------------
# civitas version
# ---------------------------------------------------------------------------


def test_version_matches_package_metadata():
    from importlib.metadata import version as pkg_version

    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert pkg_version("civitas") in result.output


# ---------------------------------------------------------------------------
# civitas init
# ---------------------------------------------------------------------------


def test_init_scaffolds_all_files(tmp_path, monkeypatch):
    # Contract: init takes a BARE project name, relative to cwd (a full path is
    # rejected by the identifier check — arguably a UX wart, noted in the plan).
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "myproj"])
    assert result.exit_code == 0, result.output
    target = tmp_path / "myproj"
    for f in ("pyproject.toml", "topology.yaml", "agents.py", "run.py", "README.md"):
        assert (target / f).exists(), f"missing scaffold file {f}"
    # The scaffolded topology must itself pass validation — dogfood check.
    vres = runner.invoke(app, ["topology", "validate", str(target / "topology.yaml")])
    assert vres.exit_code == 0, vres.output


def test_init_rejects_existing_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "taken").mkdir()
    result = runner.invoke(app, ["init", "taken"])
    assert result.exit_code != 0


def test_init_rejects_invalid_identifier(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "not-a-valid-module!"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# civitas topology validate / show / diff
# ---------------------------------------------------------------------------


def test_topology_validate_valid_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp))
        result = runner.invoke(app, ["topology", "validate", str(path)])
        assert result.exit_code == 0


def test_topology_validate_missing_file():
    result = runner.invoke(app, ["topology", "validate", "/nonexistent/topology.yaml"])
    assert result.exit_code != 0


def test_topology_validate_malformed_yaml():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp), text="supervision: [unclosed")
        result = runner.invoke(app, ["topology", "validate", str(path)])
        assert result.exit_code != 0


def test_topology_validate_missing_supervision_block():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp), text="transport:\n  type: in_process\n")
        result = runner.invoke(app, ["topology", "validate", str(path)])
        assert result.exit_code != 0


def test_topology_validate_unknown_strategy():
    bad = TOPOLOGY.replace("ONE_FOR_ONE", "SOME_FOR_NONE")
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp), text=bad)
        result = runner.invoke(app, ["topology", "validate", str(path)])
        assert result.exit_code != 0


def test_topology_show_renders():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp))
        result = runner.invoke(app, ["topology", "show", str(path)])
        assert result.exit_code == 0
        assert "worker" in result.output  # agent names are single tokens — wrap-safe


def test_topology_diff_reports_changes():
    changed = TOPOLOGY.replace("in_process", "nats").replace(
        "myapp.agents.Worker", "myapp.agents.WorkerV2"
    )
    with tempfile.TemporaryDirectory() as tmp:
        a = _write_topology(Path(tmp), name="a.yaml")
        b = _write_topology(Path(tmp), text=changed, name="b.yaml")
        result = runner.invoke(app, ["topology", "diff", str(a), str(b)])
        assert result.exit_code == 0
        assert "changed" in result.output.lower()


def test_topology_diff_identical_files():
    with tempfile.TemporaryDirectory() as tmp:
        a = _write_topology(Path(tmp), name="a.yaml")
        b = _write_topology(Path(tmp), name="b.yaml")
        result = runner.invoke(app, ["topology", "diff", str(a), str(b)])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# civitas state (core-only paths — sqlite-backed paths are integration, #40)
# ---------------------------------------------------------------------------


def test_state_list_without_database_is_friendly():
    result = runner.invoke(app, ["state", "list", "--db", "/nonexistent/state.db"])
    assert result.exit_code == 0  # informational, not an error


def test_state_clear_without_database():
    result = runner.invoke(app, ["state", "clear", "ghost", "--db", "/nonexistent/state.db"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# civitas deploy docker-compose
# ---------------------------------------------------------------------------


def test_deploy_generates_stack_with_nonroot_dockerfile():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_topology(Path(tmp))
        out = Path(tmp) / "deploy"
        result = runner.invoke(
            app, ["deploy", "docker-compose", "--topology", str(path), "--output", str(out)]
        )
        assert result.exit_code == 0
        dockerfile = (out / "Dockerfile").read_text()
        compose = yaml.safe_load((out / "docker-compose.yml").read_text())
        assert "USER civitas" in dockerfile  # v0.7.4 security fix stays fixed
        assert dockerfile.index("USER civitas") < dockerfile.index("ENTRYPOINT")
        assert "services" in compose
        assert (out / ".env").exists()


def test_deploy_missing_topology_fails():
    result = runner.invoke(
        app, ["deploy", "docker-compose", "--topology", "/nonexistent.yaml", "--output", "/tmp/x"]
    )
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# civitas security init-zmq
# ---------------------------------------------------------------------------


def test_security_init_zmq_generates_keypairs(tmp_path):
    import pytest

    pytest.importorskip("zmq", reason="pyzmq not installed")
    result = runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    names = {p.name for p in tmp_path.iterdir()}
    assert {"zmq_server.pub", "zmq_server.key", "zmq_client.pub", "zmq_client.key"} <= names


# ---------------------------------------------------------------------------
# civitas run (no live runtime — argument handling only)
# ---------------------------------------------------------------------------


def test_run_missing_topology_fails():
    result = runner.invoke(app, ["run", "--topology", "/nonexistent/topology.yaml"])
    assert result.exit_code != 0


def test_run_help_exits_clean():
    # Exit code only — help TEXT assertions are width-fragile (see module docstring).
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
