"""civitas security — key management and transport security scaffolding.

Commands:
    civitas security init zmq   — generate ZMQ CURVE server + client keypairs
    civitas security init nats  — print NATS TLS YAML scaffold
"""

from __future__ import annotations

from pathlib import Path

import typer

from civitas.cli.app import console, error, info, success, warn

security_app = typer.Typer(
    name="security",
    help="Security key management for ZMQ CURVE and NATS TLS.",
    no_args_is_help=True,
)

init_app = typer.Typer(
    name="init",
    help="Scaffold transport security keys and configuration.",
    no_args_is_help=True,
)

security_app.add_typer(init_app, name="init")


@init_app.command("zmq")
def init_zmq(
    key_dir: Path = typer.Option(
        Path("./civitas-keys"),
        "--key-dir",
        "-d",
        help="Directory to write generated keypairs.",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing keys."),
) -> None:
    """Generate ZMQ CURVE server and client keypairs.

    Writes four files to --key-dir:
      zmq_server.pub / zmq_server.key  — proxy server keypair
      zmq_client.pub / zmq_client.key  — connecting client keypair

    Copy the YAML snippet printed below into your topology file.
    """
    try:
        import zmq
    except ImportError:
        error("pyzmq is not installed. Run: pip install 'civitas[zmq]'")
        raise typer.Exit(1) from None

    key_dir.mkdir(parents=True, exist_ok=True)

    server_pub_path = key_dir / "zmq_server.pub"
    server_key_path = key_dir / "zmq_server.key"
    client_pub_path = key_dir / "zmq_client.pub"
    client_key_path = key_dir / "zmq_client.key"

    existing = [
        p
        for p in (server_pub_path, server_key_path, client_pub_path, client_key_path)
        if p.exists()
    ]
    if existing and not force:
        warn(f"Keys already exist in {key_dir}. Use --force to overwrite.")
        for p in existing:
            info(f"  {p}")
        raise typer.Exit(1)

    server_pub, server_secret = zmq.curve_keypair()
    client_pub, client_secret = zmq.curve_keypair()

    server_pub_z85 = server_pub.decode()
    server_secret_z85 = server_secret.decode()
    client_pub_z85 = client_pub.decode()
    client_secret_z85 = client_secret.decode()

    server_pub_path.write_text(server_pub_z85 + "\n")
    server_key_path.write_text(server_secret_z85 + "\n")
    client_pub_path.write_text(client_pub_z85 + "\n")
    client_key_path.write_text(client_secret_z85 + "\n")

    # Restrict permissions on secret key files
    server_key_path.chmod(0o600)
    client_key_path.chmod(0o600)

    success(f"Server keypair → {server_pub_path}, {server_key_path}")
    success(f"Client keypair → {client_pub_path}, {client_key_path}")

    console.print("\n[bold]Add to your topology YAML:[/bold]\n")
    console.print(
        f"""\
security:
  transport:
    zmq:
      curve:
        enabled: true
        server_public_key: "{server_pub_z85}"
        server_secret_key: "{server_secret_z85}"
        client_public_key: "{client_pub_z85}"
        client_secret_key: "{client_secret_z85}"
"""
    )
    console.print("[dim]Keep server_secret_key and client_secret_key out of version control.[/dim]")


@init_app.command("nats")
def init_nats(
    cert: Path = typer.Option(
        Path("./civitas-keys/nats.crt"),
        "--cert",
        help="Path to TLS certificate (PEM).",
    ),
    key: Path = typer.Option(
        Path("./civitas-keys/nats.key"),
        "--key",
        help="Path to TLS private key (PEM).",
    ),
    ca: Path = typer.Option(
        Path("./civitas-keys/ca.crt"),
        "--ca",
        help="Path to CA certificate (PEM) for server verification.",
    ),
) -> None:
    """Print NATS TLS configuration YAML scaffold.

    Provide your own TLS certificate, key, and CA bundle obtained from
    your PKI infrastructure or a tool like 'mkcert' or 'step'.

    For nkeys authentication, also add the nkey_seed field (requires
    nkeys-py: pip install 'civitas[nkeys]').
    """
    console.print("\n[bold]Add to your topology YAML:[/bold]\n")
    console.print(
        f"""\
security:
  transport:
    nats:
      tls:
        enabled: true
        cert: {cert}
        key: {key}
        ca: {ca}
      # nkey_seed: "SUAM..."  # optional — requires civitas[nkeys]
"""
    )
    info("Generate a self-signed cert with mkcert:")
    console.print("  mkcert -install && mkcert -cert-file nats.crt -key-file nats.key localhost")
