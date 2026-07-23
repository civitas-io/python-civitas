"""civitas init — scaffold a new Civitas project."""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from string import Template

import typer
from rich.panel import Panel

from civitas.cli.app import app, console, err_console

_TEMPLATE_FILES = [
    ("pyproject.toml", "pyproject.toml.tmpl"),
    ("topology.yaml", "topology.yaml.tmpl"),
    ("agents.py", "agents.py.tmpl"),
    ("run.py", "run.py.tmpl"),
    ("README.md", "README.md.tmpl"),
]


def _load_template(name: str) -> Template:
    """Load a template file from the _templates package."""
    ref = resources.files("civitas.cli._templates").joinpath(name)
    return Template(ref.read_text(encoding="utf-8"))


@app.command()
def init(
    name: str = typer.Argument(
        help="Project name, or a path ending in the project name (e.g. apps/my_agents)"
    ),
    directory: str | None = typer.Option(None, "--dir", "-d", help="Parent directory"),
) -> None:
    """Scaffold a new Civitas project."""
    # G2 (v0.8.2): `init path/to/proj` used to hit the identifier check with a
    # confusing error because the WHOLE path was validated. Auto-split instead:
    # everything up to the basename joins the parent directory; only the
    # basename must be a valid Python identifier (it names the module/class).
    name_path = Path(name)
    parent = Path(directory) if directory else Path.cwd()
    if name_path.parent != Path("."):
        parent = name_path.parent if name_path.is_absolute() else parent / name_path.parent
        name = name_path.name

    # F09-6: validate name as a Python identifier (used as class name and file name)
    if not name.isidentifier():
        err_console.print(
            f"[red]Error:[/red] '{name}' is not a valid Python identifier. "
            "Use letters, digits, and underscores only; do not start with a digit."
        )
        raise typer.Exit(1)

    project_dir = parent / name

    if project_dir.exists():
        err_console.print(f"[red]Error:[/red] Directory '{project_dir}' already exists.")
        raise typer.Exit(1)

    project_dir.mkdir(parents=True)

    for filename, tmpl_name in _TEMPLATE_FILES:
        tmpl = _load_template(tmpl_name)
        content = tmpl.safe_substitute(project_name=name)  # F09-5: safe_substitute
        (project_dir / filename).write_text(content)

    console.print(
        Panel.fit(
            f"[green]✔ Created {name}[/green]\n\n"
            f"  Next steps:\n"
            f"    cd {name}\n"
            f"    pip install -e .\n"
            f"    civitas run\n",
            title="civitas init",
            border_style="green",
        )
    )
