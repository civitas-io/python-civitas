"""civitas dashboard — live terminal dashboard for a running topology."""

from __future__ import annotations

import asyncio
from pathlib import Path

import typer
from rich.live import Live

from civitas import Runtime
from civitas.cli.app import app, console, err_console, register_shutdown, success
from civitas.dashboard.collector import MetricsCollector
from civitas.dashboard.renderer import render_dashboard


async def _record_restart(collector: MetricsCollector, name: str, exc: Exception) -> None:
    collector.agent_restarted(name, type(exc).__name__)


async def _run_dashboard(
    topology_path: Path,
    refresh_rate: float,
) -> None:
    """Start the runtime with metrics collection and render a live dashboard."""
    collector = MetricsCollector()
    runtime = Runtime.from_config(str(topology_path))
    runtime.set_metrics(collector)
    runtime.on_crash(lambda name, exc: _record_restart(collector, name, exc))

    await runtime.start()
    collector.runtime_started()

    # Register all agents and set initial status
    for agent in runtime.all_agents():
        collector.register_agent(agent.name)
        collector.agent_status_changed(agent.name, agent.status.value)

    stop_event = asyncio.Event()
    register_shutdown(stop_event)

    with Live(render_dashboard(collector), refresh_per_second=1 / refresh_rate, console=console):
        while not stop_event.is_set():
            # Update agent statuses
            for agent in runtime.all_agents():
                collector.agent_status_changed(agent.name, agent.status.value)

            await asyncio.sleep(refresh_rate)

    console.print("\n  [yellow]Shutting down...[/yellow]")
    await runtime.stop()
    success("Stopped")


@app.command()
def dashboard(
    topology: str = typer.Option("topology.yaml", "--topology", "-t", help="Topology YAML file"),
    refresh: float = typer.Option(1.0, "--refresh", "-r", help="Refresh rate in seconds"),
) -> None:
    """Launch a live terminal dashboard for a running topology."""
    topology_path = Path(topology)
    if not topology_path.exists():
        err_console.print(f"[red]Error:[/red] Topology file '{topology}' not found.")
        raise typer.Exit(1)

    asyncio.run(_run_dashboard(topology_path, refresh))
