"""civitas run — run an agent topology."""

from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from typing import Any, cast

import typer
import yaml
from rich.tree import Tree

from civitas import AgentProcess, DynamicSupervisor, Runtime
from civitas.cli.app import app, console, err_console, register_shutdown, success
from civitas.plugins.loader import load_plugins_from_config
from civitas.worker import Worker


def _find_process_agents(config: dict[str, Any], process_name: str) -> list[dict[str, Any]]:
    """Find agents (and worker-hosted dynamic supervisors) assigned to a process."""
    agents: list[dict[str, Any]] = []
    sup_cfg = config.get("supervision", config.get("supervisor", {}))

    def _walk(node: dict[str, Any]) -> None:
        if "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                _walk(child)
        elif "agent" in node:
            acfg = node["agent"]
            if acfg.get("process") == process_name:
                agents.append(acfg)
        elif node.get("type") == "dynamic_supervisor" and node.get("process") == process_name:
            # A DynamicSupervisor can be hosted in a Worker so it spawns children
            # cross-process (R6 · D1); it is a typed node, not an `agent:` wrapper.
            agents.append(node)

    for child in sup_cfg.get("children", []):
        _walk(child)

    return agents


def _build_worker_agent(acfg: dict[str, Any]) -> AgentProcess:
    """Instantiate one worker-hosted node from its topology config."""
    if acfg.get("type") == "dynamic_supervisor":
        return DynamicSupervisor(
            name=acfg["name"],
            max_children=acfg.get("max_children"),
            max_total_spawns=acfg.get("max_total_spawns"),
            restart=acfg.get("restart", "transient"),
            max_restarts=acfg.get("max_restarts", 3),
            restart_window=acfg.get("restart_window", 60.0),
        )
    cls = _resolve_agent_class(acfg["type"])
    return cast(AgentProcess, cls(name=acfg["name"]))


def _resolve_agent_class(type_str: str) -> type:
    """Resolve a dotted type string to an agent class."""
    module_path, _, class_name = type_str.rpartition(".")
    if not module_path:
        err_console.print(
            f"[red]Error:[/red] '{type_str}' is not a dotted path (e.g. 'myapp.agents.MyAgent')."
        )
        raise typer.Exit(code=1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        err_console.print(f"[red]Error:[/red] cannot import '{module_path}': {exc}")
        raise typer.Exit(code=1) from exc
    cls = getattr(module, class_name, None)
    if cls is None:
        err_console.print(f"[red]Error:[/red] '{module_path}' has no attribute '{class_name}'.")
        raise typer.Exit(code=1)
    return cast(type, cls)


def _build_startup_tree(config: dict[str, Any]) -> Tree:
    """Build a Rich Tree of the supervision structure for startup display."""
    sup = config.get("supervision", config.get("supervisor", {}))
    root_name = sup.get("name", "root")
    strategy = sup.get("strategy", "ONE_FOR_ONE").upper()

    tree = Tree(f"[bold cyan][sup][/bold cyan] {root_name} [dim]({strategy})[/dim]")

    def _add(parent: Tree, children: list[dict[str, Any]]) -> None:
        for child in children:
            if "supervisor" in child:
                s = child["supervisor"]
                branch = parent.add(
                    f"[bold cyan][sup][/bold cyan] {s.get('name', '?')} "
                    f"[dim]({s.get('strategy', 'ONE_FOR_ONE').upper()})[/dim]"
                )
                _add(branch, s.get("children", []))
            elif "agent" in child:
                a = child["agent"]
                label = f"[green]{a.get('name', '?')}[/green] [dim]{a.get('type', '?')}[/dim]"
                if "process" in a:
                    label += f" [yellow]@{a['process']}[/yellow]"
                parent.add(label)

    _add(tree, sup.get("children", []))
    return tree


async def _run_supervisor(config: dict[str, Any], topology_path: Path) -> None:
    """Run the full runtime as the supervisor process."""
    runtime = Runtime.from_config(str(topology_path))

    tree = _build_startup_tree(config)
    console.print()
    console.print(tree)
    console.print()

    await runtime.start()
    success("Runtime started — Ctrl+C to stop")

    stop_event = asyncio.Event()
    register_shutdown(stop_event)

    await stop_event.wait()
    console.print("\n  [yellow]Shutting down...[/yellow]")
    await runtime.stop()
    success("Stopped")


async def _run_worker(config: dict[str, Any], process_name: str) -> None:
    """Run a worker process hosting a subset of agents."""
    agents_cfg = _find_process_agents(config, process_name)
    if not agents_cfg:
        err_console.print(f"[red]Error:[/red] No agents found for process '{process_name}'.")
        raise typer.Exit(1)

    agents = [_build_worker_agent(acfg) for acfg in agents_cfg]

    transport_cfg = config.get("transport", {})
    transport_type = transport_cfg.get("type", "zmq")
    kwargs: dict[str, Any] = {"agents": agents, "transport": transport_type}

    if transport_type == "zmq":
        if "pub_addr" in transport_cfg:
            kwargs["zmq_pub_addr"] = transport_cfg["pub_addr"]
        if "sub_addr" in transport_cfg:
            kwargs["zmq_sub_addr"] = transport_cfg["sub_addr"]
    elif transport_type == "nats":
        if "servers" in transport_cfg:
            kwargs["nats_servers"] = transport_cfg["servers"]
        if "jetstream" in transport_cfg:
            kwargs["nats_jetstream"] = transport_cfg["jetstream"]

    if "plugins" in config:
        loaded = load_plugins_from_config(config)
        if loaded["model_providers"]:
            kwargs["model_provider"] = loaded["model_providers"][0]
        if loaded["state_store"] is not None:
            kwargs["state_store"] = loaded["state_store"]
        if loaded["exporters"]:
            kwargs["exporters"] = loaded["exporters"]

    worker = Worker(**kwargs)
    console.print(f"  [blue]Worker '{process_name}':[/blue] hosting {[a.name for a in agents]}")
    await worker.start()
    success("Worker started — Ctrl+C to stop")

    # F09-7: use stop_event + signal handlers (same as _run_supervisor) so
    # SIGTERM (container stop, systemctl stop) is handled gracefully.
    stop_event = asyncio.Event()
    register_shutdown(stop_event)

    await stop_event.wait()
    console.print("\n  [yellow]Shutting down worker...[/yellow]")
    await worker.stop()
    success("Stopped")


@app.command()
def run(
    topology: str = typer.Option("topology.yaml", "--topology", "-t", help="Topology YAML file"),
    transport: str | None = typer.Option(
        None, "--transport", help="Override transport (in_process, zmq, nats)"
    ),
    process: str | None = typer.Option(
        None, "--process", "-p", help="Process name for worker mode"
    ),
    nats_url: str | None = typer.Option(None, "--nats-url", help="NATS server URL"),
) -> None:
    """Run an agent topology."""
    topology_path = Path(topology)
    if not topology_path.exists():
        err_console.print(f"[red]Error:[/red] Topology file '{topology}' not found.")
        raise typer.Exit(1)

    config = yaml.safe_load(topology_path.read_text())

    if transport is not None:
        config.setdefault("transport", {})["type"] = transport
    if nats_url is not None:
        config.setdefault("transport", {})["servers"] = nats_url

    if process is not None:
        asyncio.run(_run_worker(config, process))
    else:
        asyncio.run(_run_supervisor(config, topology_path))
