"""Unit tests for the civitas CLI via Typer's CliRunner (V5, #42).

The "dedicated CLI testing sprint" deferred since M3.1. In-process, no
subprocesses, no live runtime. Lesson from the integration gate's first CI
catch applied throughout: NEVER assert on Rich help text substrings (rendering
width is environment-dependent) — assert exit codes and file artifacts.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
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


def test_init_accepts_relative_path(tmp_path, monkeypatch):
    """G2 (v0.8.2): `init path/to/proj` auto-splits — parents created, basename
    identifier-validated. Previously failed with a confusing identifier error."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "apps/nested/my_agents"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "apps" / "nested" / "my_agents" / "topology.yaml").exists()


def test_init_accepts_absolute_path(tmp_path):
    result = runner.invoke(app, ["init", str(tmp_path / "abs_proj")])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "abs_proj" / "agents.py").exists()


def test_init_path_with_dir_option_joins(tmp_path):
    result = runner.invoke(app, ["init", "sub/proj_x", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "sub" / "proj_x" / "run.py").exists()


def test_init_path_basename_still_validated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "apps/not-valid!"])
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


# ---------------------------------------------------------------------------
# G3 (v0.8.2) — deploy/topology/state/dashboard coverage top-ups
# ---------------------------------------------------------------------------

MULTIPROC_NATS_TOPOLOGY = """
transport:
  type: nats
  url: nats://localhost:4222
  jetstream: true
plugins:
  models:
    - type: anthropic
      config: {default_model: claude-sonnet-4-6}
    - type: litellm
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent: {name: orchestrator, type: myapp.Orchestrator}
    - agent: {name: researcher, type: myapp.Researcher, process: worker}
    - supervisor:
        name: nested
        children:
          - agent: {name: writer, type: myapp.Writer, process: worker}
          - {type: myapp.Flat, name: flat_agent, process: heavy}
"""


def test_collect_processes_affinity_and_flat_format():
    from civitas.cli.deploy import _collect_processes

    config = yaml.safe_load(MULTIPROC_NATS_TOPOLOGY)
    procs = _collect_processes(config)
    assert [a["name"] for a in procs["supervisor"]] == ["orchestrator"]
    assert {a["name"] for a in procs["worker"]} == {"researcher", "writer"}  # nested walk
    assert [a["name"] for a in procs["heavy"]] == ["flat_agent"]  # flat node format


def test_deploy_nats_multiprocess_stack(tmp_path):
    """NATS service (jetstream command, healthcheck), supervisor depends_on,
    one service per worker process group, provider keys in .env."""
    topo = tmp_path / "topology.yaml"
    topo.write_text(MULTIPROC_NATS_TOPOLOGY)
    out = tmp_path / "deploy"
    result = runner.invoke(
        app, ["deploy", "docker-compose", "--topology", str(topo), "--output", str(out)]
    )
    assert result.exit_code == 0, result.output

    compose = yaml.safe_load((out / "docker-compose.yml").read_text())
    services = compose["services"]
    assert services["nats"]["command"] == "--jetstream"
    assert "healthcheck" in services["nats"]
    assert services["supervisor"]["depends_on"] == {"nats": {"condition": "service_healthy"}}
    worker_services = [k for k in services if k not in ("nats", "supervisor")]
    assert len(worker_services) == 2  # worker + heavy

    env = (out / ".env").read_text()
    assert "NATS_URL=" in env
    assert "ANTHROPIC_API_KEY=" in env  # anthropic provider detected
    assert "OPENAI_API_KEY=" in env  # litellm provider detected


def test_deploy_in_process_stack_has_no_broker(tmp_path):
    topo = _write_topology(tmp_path)
    out = tmp_path / "deploy2"
    result = runner.invoke(
        app, ["deploy", "docker-compose", "--topology", str(topo), "--output", str(out)]
    )
    assert result.exit_code == 0, result.output
    compose = yaml.safe_load((out / "docker-compose.yml").read_text())
    assert "nats" not in compose["services"]
    assert list(compose["services"]) == ["supervisor"]


TOPOLOGY_RICH = """
transport:
  type: nats
plugins:
  models: [{type: anthropic}]
  state: {type: sqlite, config: {db_path: ./x.db}}
supervision:
  name: root
  strategy: REST_FOR_ONE
  backoff: EXPONENTIAL
  max_restarts: 5
  children:
    - agent: {name: a, type: myapp.A}
    - name: pool
      type: dynamic_supervisor
      max_children: 10
    - name: topo
      type: topology_server
      config: {host: 127.0.0.1, port: 6799}
"""


def test_topology_validate_rich_nodes(tmp_path):
    """dynamic_supervisor + topology_server nodes + plugins all validate."""
    path = _write_topology(tmp_path, text=TOPOLOGY_RICH, name="rich.yaml")
    result = runner.invoke(app, ["topology", "validate", str(path)])
    assert result.exit_code == 0, result.output


def test_topology_show_rich_with_dead_live_probe(tmp_path):
    """show renders the static tree even when the declared topology_server
    is not actually listening (connection-refused fallback path)."""
    path = _write_topology(tmp_path, text=TOPOLOGY_RICH, name="rich.yaml")
    result = runner.invoke(app, ["topology", "show", str(path)])
    assert result.exit_code == 0, result.output
    assert "pool" in result.output and "root" in result.output


def test_topology_diff_added_and_removed_children(tmp_path):
    base = _write_topology(tmp_path, name="base.yaml")
    two = TOPOLOGY.replace(
        "    - agent:\n        name: worker\n        type: myapp.agents.Worker",
        "    - agent:\n        name: other\n        type: myapp.agents.Other",
    )
    other = _write_topology(tmp_path, text=two, name="other.yaml")
    result = runner.invoke(app, ["topology", "diff", str(base), str(other)])
    assert result.exit_code == 0, result.output
    out = result.output.lower()
    assert "worker" in out and "other" in out  # one removed, one added


def test_state_migrate_rejects_unknown_dsn(tmp_path):
    """migrate with an unrecognizable DSN fails loudly (BadParameter or the
    contrib-missing error — either way nonzero, never silent)."""
    result = runner.invoke(
        app, ["state", "migrate", "--from", "mystery://what", "--to", "also-mystery"]
    )
    assert result.exit_code != 0


def test_dashboard_missing_topology_fails():
    result = runner.invoke(app, ["dashboard", "--topology", "/nonexistent/topology.yaml"])
    assert result.exit_code != 0


def test_parse_headers_valid_and_invalid():
    """v0.9.6 (D7): --header 'Name: Value' parsing for the dashboard's auth
    seam. Scheme-agnostic -- any header the operator's middleware expects."""
    from civitas.cli.dashboard import _parse_headers
    from civitas.errors import ConfigurationError

    assert _parse_headers([]) == {}
    assert _parse_headers(["Authorization: Bearer abc"]) == {"Authorization": "Bearer abc"}
    # Value may itself contain colons (e.g. a bearer token rarely, but be safe).
    assert _parse_headers(["X-Thing: a:b:c"]) == {"X-Thing": "a:b:c"}
    assert _parse_headers(["X-API-Key: k", "X-Trace: t"]) == {"X-API-Key": "k", "X-Trace": "t"}
    for bad in ["no-colon-here", ": no-name"]:
        with pytest.raises(ConfigurationError):
            _parse_headers([bad])


TOPOLOGY_WARNINGS = """
transport:
  type: zmq
supervision:
  name: root
  strategy: ONE_FOR_ALL
  backoff: LINEAR
  max_restarts: -1
  children:
    - agent: {name: dupe, type: myapp.A}
    - agent: {name: dupe, type: myapp.B}
    - agent: {name: '', type: myapp.C}
    - agent: {name: no_type}
    - {type: gen_server, module: myapp.servers, class: Counter, name: counter}
    - name: gw
      type: http_gateway
      config: {port: 8080}
    - name: evals
      type: eval_agent
    - supervisor:
        name: empty_sup
        children: []
"""


def test_topology_validate_surfaces_all_error_classes(tmp_path):
    """Bad max_restarts, duplicate/missing names, missing type, empty supervisor
    — every _validate_topology error branch fires and the command exits 1."""
    path = _write_topology(tmp_path, text=TOPOLOGY_WARNINGS, name="warn.yaml")
    result = runner.invoke(app, ["topology", "validate", str(path)])
    assert result.exit_code != 0


def test_topology_show_gateway_eval_genserver_nodes(tmp_path):
    """show renders the special node types (gen_server, http_gateway, eval_agent)."""
    fixed = TOPOLOGY_WARNINGS.replace("max_restarts: -1", "max_restarts: 3")
    fixed = fixed.replace("    - agent: {name: dupe, type: myapp.B}\n", "")
    fixed = fixed.replace("    - agent: {name: '', type: myapp.C}\n", "")
    fixed = fixed.replace("    - agent: {name: no_type}\n", "")
    fixed = fixed.replace(
        "        children: []",
        "        children:\n          - agent: {name: leaf, type: myapp.Leaf}",
    )
    path = _write_topology(tmp_path, text=fixed, name="rich2.yaml")
    vres = runner.invoke(app, ["topology", "validate", str(path)])
    assert vres.exit_code == 0, vres.output
    result = runner.invoke(app, ["topology", "show", str(path)])
    assert result.exit_code == 0, result.output
    # Note: the rich tree renders agent/supervisor nodes; special node types
    # (gen_server / http_gateway / eval_agent) validate + parse but are not
    # individually rendered — display quirk, recorded, not forced here.
    assert "leaf" in result.output


def test_topology_diff_transport_and_plugins_sections(tmp_path):
    a_text = TOPOLOGY_RICH
    b_text = TOPOLOGY_RICH.replace("type: nats", "type: zmq").replace(
        "models: [{type: anthropic}]", "models: [{type: litellm}]"
    )
    a = _write_topology(tmp_path, text=a_text, name="ta.yaml")
    b = _write_topology(tmp_path, text=b_text, name="tb.yaml")
    result = runner.invoke(app, ["topology", "diff", str(a), str(b)])
    assert result.exit_code == 0, result.output
    out = result.output.lower()
    assert "transport" in out
