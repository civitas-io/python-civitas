"""civitas deploy — generate deployment artifacts from topology.

Generates docker-compose.yml, Dockerfile, and .env from a topology YAML.
The generator reads process affinity from the topology to determine which
agents run in which container.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer
import yaml
from rich.panel import Panel

from civitas.cli.app import console, err_console

deploy_app = typer.Typer(
    name="deploy",
    help="Generate deployment artifacts.",
    no_args_is_help=True,
)


# ---------------------------------------------------------------------------
# Docker Compose generator
# ---------------------------------------------------------------------------


def _collect_processes(config: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    """Collect agents grouped by process affinity.

    Returns a dict mapping process name to list of agent configs.
    Agents without a ``process`` key are assigned to ``"supervisor"``.
    """
    processes: dict[str, list[dict[str, str]]] = {"supervisor": []}
    sup = config.get("supervision", config.get("supervisor", {}))

    def _walk(node: dict[str, Any]) -> None:
        if "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                _walk(child)
        elif "agent" in node:
            a = node["agent"]
            process_name = a.get("process", "supervisor")
            processes.setdefault(process_name, []).append(a)
        elif "type" in node and "name" in node:
            # Flat format: {type: "module.Class", name: "agent_name", process: "worker"}
            process_name = node.get("process", "supervisor")
            processes.setdefault(process_name, []).append(node)

    for child in sup.get("children", []):
        _walk(child)

    return processes


def _generate_dockerfile(transport_type: str = "in_process") -> str:
    """Generate a Dockerfile for Civitas containers."""
    extras = {"nats": "nats", "zmq": "zmq"}.get(transport_type, "")
    install_target = f'".[{extras}]"' if extras else '"."'
    return f"""\
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY civitas/ civitas/
COPY examples/ examples/

RUN pip install --no-cache-dir -e {install_target} && \\
    useradd --create-home --uid 1000 civitas

# Never run agents as root — a compromised agent process would own the container.
USER civitas

ENTRYPOINT ["civitas", "run"]
"""


def _generate_docker_compose(
    config: dict[str, Any],
    topology_filename: str,
) -> str:
    """Generate docker-compose.yml from a topology config."""
    transport_cfg = config.get("transport", {})
    transport_type = transport_cfg.get("type", "in_process")

    processes = _collect_processes(config)
    services: dict[str, Any] = {}

    # NATS service (if using NATS transport)
    if transport_type == "nats":
        services["nats"] = {
            "image": "nats:latest",
            "ports": ["4222:4222", "8222:8222"],
            "command": "--jetstream" if transport_cfg.get("jetstream") else "",
            "restart": "unless-stopped",
            "healthcheck": {
                "test": ["CMD-SHELL", "wget -qO- http://localhost:8222/healthz || exit 1"],
                "interval": "5s",
                "timeout": "3s",
                "retries": 3,
            },
        }
        # Clean empty command
        if not services["nats"]["command"]:
            del services["nats"]["command"]

    # Supervisor service
    sup_service: dict[str, Any] = {
        "build": ".",
        "command": ["--topology", topology_filename],
        "volumes": [f"./{topology_filename}:/app/{topology_filename}:ro"],
        "restart": "unless-stopped",
        "environment": {
            "AGENCY_SERIALIZER": "${AGENCY_SERIALIZER:-msgpack}",
        },
    }
    if transport_type == "nats":
        sup_service["depends_on"] = {"nats": {"condition": "service_healthy"}}
        sup_service["environment"]["NATS_URL"] = "nats://nats:4222"
    services["supervisor"] = sup_service

    # Worker services (one per process group)
    for process_name, agents in processes.items():
        if process_name == "supervisor":
            continue

        agent_names = [a["name"] for a in agents]
        worker_service: dict[str, Any] = {
            "build": ".",
            "command": ["--topology", topology_filename, "--process", process_name],
            "volumes": [f"./{topology_filename}:/app/{topology_filename}:ro"],
            "restart": "unless-stopped",
            "labels": {
                "civitas.process": process_name,
                "civitas.agents": ",".join(agent_names),
            },
            "environment": {
                "AGENCY_SERIALIZER": "${AGENCY_SERIALIZER:-msgpack}",
            },
        }
        if transport_type == "nats":
            worker_service["depends_on"] = {
                "nats": {"condition": "service_healthy"},
                "supervisor": {"condition": "service_started"},
            }
            worker_service["environment"]["NATS_URL"] = "nats://nats:4222"

        services[f"worker-{process_name}"] = worker_service

    compose = {
        "services": services,
    }

    # Add NATS network if using NATS
    if transport_type == "nats":
        compose["networks"] = {
            "default": {"name": "civitas-network"},
        }

    return yaml.dump(compose, default_flow_style=False, sort_keys=False)


def _generate_env_file(config: dict[str, Any]) -> str:
    """Generate a .env file with runtime configuration."""
    lines = [
        "# Civitas runtime configuration",
        "# Generated by: civitas deploy docker-compose",
        "",
        "AGENCY_SERIALIZER=msgpack",
        "",
    ]

    transport_cfg = config.get("transport", {})
    if transport_cfg.get("type") == "nats":
        lines.append("# NATS (handled by docker-compose networking)")
        lines.append("NATS_URL=nats://nats:4222")
        lines.append("")

    # Plugin-related env vars
    plugins_cfg = config.get("plugins", {})
    for provider in plugins_cfg.get("model_providers", plugins_cfg.get("models", [])):
        ptype = provider.get("type", "")
        if "anthropic" in ptype.lower():
            lines.append("# Anthropic API key (required for AnthropicProvider)")
            lines.append("ANTHROPIC_API_KEY=")
            lines.append("")
        if "litellm" in ptype.lower():
            lines.append("# LLM API keys (set as needed for LiteLLM)")
            lines.append("OPENAI_API_KEY=")
            lines.append("GEMINI_API_KEY=")
            lines.append("")

    for exporter in plugins_cfg.get("exporters", plugins_cfg.get("observability", [])):
        etype = exporter.get("type", "")
        if "otel" in etype.lower():
            lines.append("# OpenTelemetry collector endpoint")
            lines.append("OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317")
            lines.append("")
        if "fiddler" in etype.lower():
            lines.append("# Fiddler API key")
            lines.append("FIDDLER_API_KEY=")
            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


@deploy_app.command("docker-compose")
def docker_compose(
    topology: str = typer.Option("topology.yaml", "--topology", "-t", help="Topology YAML file"),
    output: str = typer.Option("./deploy", "--output", "-o", help="Output directory"),
) -> None:
    """Generate docker-compose.yml from a topology file."""
    topology_path = Path(topology)
    if not topology_path.exists():
        err_console.print(f"[red]Error:[/red] Topology file '{topology}' not found.")
        raise typer.Exit(1)

    config = yaml.safe_load(topology_path.read_text())

    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate files
    topology_filename = topology_path.name

    # Copy topology into output dir
    (output_dir / topology_filename).write_text(topology_path.read_text())

    # Dockerfile
    transport_type = config.get("transport", {}).get("type", "in_process")
    dockerfile_content = _generate_dockerfile(transport_type)
    (output_dir / "Dockerfile").write_text(dockerfile_content)

    # docker-compose.yml
    compose_content = _generate_docker_compose(config, topology_filename)
    (output_dir / "docker-compose.yml").write_text(compose_content)

    # .env
    env_content = _generate_env_file(config)
    (output_dir / ".env").write_text(env_content)

    # Summary
    processes = _collect_processes(config)
    worker_count = len([p for p in processes if p != "supervisor"])
    agent_count = sum(len(agents) for agents in processes.values())

    files_generated = [
        ("docker-compose.yml", f"supervisor + {worker_count} workers"),
        ("Dockerfile", "agent image"),
        (".env", "runtime config"),
        (topology_filename, "topology (copied)"),
    ]

    file_list = "\n".join(
        f"    [cyan]{name:<25}[/cyan] [dim]{desc}[/dim]" for name, desc in files_generated
    )

    console.print(
        Panel.fit(
            f"[green]✔ Generated deployment artifacts[/green]\n\n"
            f"{file_list}\n\n"
            f"  [dim]{agent_count} agents across {worker_count + 1} containers[/dim]\n\n"
            f"  Run with: [bold]cd {output} && docker compose up[/bold]",
            title="civitas deploy",
            border_style="green",
        )
    )
