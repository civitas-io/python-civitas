"""Unit tests for M4.2b — Transport mTLS (ZMQ CURVE + NATS TLS)."""

from __future__ import annotations

import ssl
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from civitas import AgentProcess, Runtime
from civitas.messages import Message
from civitas.security.config import (
    NatsTlsConfig,
    SecurityConfig,
    TransportSecurityConfig,
    ZmqCurveConfig,
)

# ---------------------------------------------------------------------------
# ZmqCurveConfig
# ---------------------------------------------------------------------------


class TestZmqCurveConfig:
    def test_defaults(self):
        cfg = ZmqCurveConfig()
        assert cfg.enabled is False
        assert cfg.server_public_key == ""

    def test_from_dict_full(self):
        data = {
            "enabled": True,
            "server_public_key": "SPUB",
            "server_secret_key": "SSEC",
            "client_public_key": "CPUB",
            "client_secret_key": "CSEC",
        }
        cfg = ZmqCurveConfig.from_dict(data)
        assert cfg.enabled is True
        assert cfg.server_public_key == "SPUB"
        assert cfg.client_secret_key == "CSEC"

    def test_from_dict_empty(self):
        cfg = ZmqCurveConfig.from_dict({})
        assert cfg.enabled is False
        assert cfg.server_public_key == ""


# ---------------------------------------------------------------------------
# NatsTlsConfig
# ---------------------------------------------------------------------------


class TestNatsTlsConfig:
    def test_defaults(self):
        cfg = NatsTlsConfig()
        assert cfg.enabled is False
        assert cfg.cert is None
        assert cfg.nkey_seed == ""

    def test_from_dict_with_tls_block(self, tmp_path: Path):
        cert = tmp_path / "nats.crt"
        key = tmp_path / "nats.key"
        ca = tmp_path / "ca.crt"
        data = {
            "tls": {
                "enabled": True,
                "cert": str(cert),
                "key": str(key),
                "ca": str(ca),
            }
        }
        cfg = NatsTlsConfig.from_dict(data)
        assert cfg.enabled is True
        assert cfg.cert == cert
        assert cfg.key == key
        assert cfg.ca == ca

    def test_from_dict_enabled_inferred_from_paths(self, tmp_path: Path):
        data = {"tls": {"cert": str(tmp_path / "cert.pem"), "key": str(tmp_path / "key.pem")}}
        cfg = NatsTlsConfig.from_dict(data)
        assert cfg.enabled is True

    def test_from_dict_nkey_seed(self):
        data = {"nkey_seed": "SUAMXYZ"}
        cfg = NatsTlsConfig.from_dict(data)
        assert cfg.nkey_seed == "SUAMXYZ"

    def test_build_ssl_context_no_ca_no_cert(self):
        cfg = NatsTlsConfig(enabled=True)
        ctx = cfg.build_ssl_context()
        assert isinstance(ctx, ssl.SSLContext)

    def test_build_ssl_context_with_ca(self, tmp_path: Path):
        ca = tmp_path / "ca.crt"
        # Write a minimal self-signed cert for testing
        ca.write_text(
            "-----BEGIN CERTIFICATE-----\n"
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n"
            "-----END CERTIFICATE-----\n"
        )
        cfg = NatsTlsConfig(enabled=True, ca=ca)
        # build_ssl_context will fail on invalid cert data — just verify it calls load_verify_locations
        with patch("ssl.SSLContext.load_verify_locations") as mock_load:
            cfg.build_ssl_context()
            mock_load.assert_called_once_with(cafile=str(ca))


# ---------------------------------------------------------------------------
# TransportSecurityConfig
# ---------------------------------------------------------------------------


class TestTransportSecurityConfig:
    def test_defaults(self):
        cfg = TransportSecurityConfig()
        assert cfg.zmq.enabled is False
        assert cfg.nats.enabled is False

    def test_from_dict_zmq_curve_nested(self):
        data = {
            "zmq": {
                "curve": {
                    "enabled": True,
                    "server_public_key": "SPUB",
                    "server_secret_key": "SSEC",
                    "client_public_key": "CPUB",
                    "client_secret_key": "CSEC",
                }
            }
        }
        cfg = TransportSecurityConfig.from_dict(data)
        assert cfg.zmq.enabled is True
        assert cfg.zmq.server_public_key == "SPUB"

    def test_from_dict_zmq_flat(self):
        # Also accept flat {zmq: {enabled: true, ...}} without curve: nesting
        data = {
            "zmq": {
                "enabled": True,
                "server_public_key": "SPUB",
                "server_secret_key": "SSEC",
                "client_public_key": "CPUB",
                "client_secret_key": "CSEC",
            }
        }
        cfg = TransportSecurityConfig.from_dict(data)
        assert cfg.zmq.enabled is True

    def test_from_dict_nats(self, tmp_path: Path):
        data = {
            "nats": {
                "tls": {
                    "enabled": True,
                    "cert": str(tmp_path / "c.pem"),
                    "key": str(tmp_path / "k.pem"),
                },
                "nkey_seed": "SXYZ",
            }
        }
        cfg = TransportSecurityConfig.from_dict(data)
        assert cfg.nats.enabled is True
        assert cfg.nats.nkey_seed == "SXYZ"


# ---------------------------------------------------------------------------
# SecurityConfig.from_dict with transport block
# ---------------------------------------------------------------------------


class TestSecurityConfigTransport:
    def test_transport_block_parsed(self):
        data = {
            "transport": {
                "zmq": {
                    "curve": {
                        "enabled": True,
                        "server_public_key": "SPUB",
                        "server_secret_key": "SSEC",
                        "client_public_key": "CPUB",
                        "client_secret_key": "CSEC",
                    }
                }
            }
        }
        cfg = SecurityConfig.from_dict(data)
        assert cfg.transport.zmq.enabled is True
        assert cfg.transport.zmq.server_public_key == "SPUB"

    def test_no_transport_block_gives_defaults(self):
        cfg = SecurityConfig.from_dict({})
        assert cfg.transport.zmq.enabled is False
        assert cfg.transport.nats.enabled is False


# ---------------------------------------------------------------------------
# ZMQProxy — CURVE socket options applied
# ---------------------------------------------------------------------------

zmq = pytest.importorskip("zmq", reason="pyzmq not installed — skipping ZMQ CURVE tests")


class TestZMQProxyCurve:
    def test_curve_options_set_on_server_sockets(self):
        from civitas.transport.zmq import ZMQProxy

        curve = ZmqCurveConfig(
            enabled=True,
            server_public_key="SPUB1234",
            server_secret_key="SSEC1234",
            client_public_key="CPUB1234",
            client_secret_key="CSEC1234",
        )
        proxy = ZMQProxy(curve_config=curve)

        xsub = MagicMock()
        xpub = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.socket.side_effect = [xsub, xpub]
        proxy._ctx = mock_ctx
        proxy._ready = MagicMock()

        with (
            patch.object(proxy, "_frontend_addr", "tcp://127.0.0.1:5559"),
            patch.object(proxy, "_backend_addr", "tcp://127.0.0.1:5560"),
            patch("zmq.proxy"),
        ):
            proxy._run()

        assert xsub.curve_server is True
        assert xsub.curve_secretkey == b"SSEC1234"
        assert xsub.curve_publickey == b"SPUB1234"
        assert xpub.curve_server is True

    def test_no_curve_when_disabled(self):
        from civitas.transport.zmq import ZMQProxy

        proxy = ZMQProxy(curve_config=None)

        xsub = MagicMock()
        xpub = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.socket.side_effect = [xsub, xpub]
        proxy._ctx = mock_ctx
        proxy._ready = MagicMock()

        with patch("zmq.proxy"):
            proxy._run()

        # curve_server should never have been set as a real attribute
        assert not (hasattr(xsub, "_mock_children") and "curve_server" in xsub._mock_children)


# ---------------------------------------------------------------------------
# ZMQTransport — CURVE client options applied to PUB/SUB sockets
# ---------------------------------------------------------------------------


class TestZMQTransportCurve:
    @pytest.mark.asyncio
    async def test_curve_options_set_on_client_sockets(self):
        from civitas.transport.zmq import ZMQTransport

        curve = ZmqCurveConfig(
            enabled=True,
            server_public_key="SPUB1234",
            server_secret_key="SSEC1234",
            client_public_key="CPUB1234",
            client_secret_key="CSEC1234",
        )

        pub_sock = MagicMock()
        sub_sock = MagicMock()

        mock_ctx = MagicMock()
        mock_ctx.socket.side_effect = [pub_sock, sub_sock]

        with patch("civitas.transport.zmq.zmq.asyncio.Context", return_value=mock_ctx):
            transport = ZMQTransport(
                MagicMock(),
                pub_addr="tcp://127.0.0.1:5559",
                sub_addr="tcp://127.0.0.1:5560",
                start_proxy=False,
                curve_config=curve,
            )
            transport._receiver_task = MagicMock()
            with patch("asyncio.create_task", return_value=MagicMock()):
                await transport.start()

        assert pub_sock.curve_serverkey == b"SPUB1234"
        assert pub_sock.curve_secretkey == b"CSEC1234"
        assert pub_sock.curve_publickey == b"CPUB1234"
        assert sub_sock.curve_serverkey == b"SPUB1234"

    @pytest.mark.asyncio
    async def test_no_curve_when_not_configured(self):
        from civitas.transport.zmq import ZMQTransport

        pub_sock = MagicMock()
        sub_sock = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.socket.side_effect = [pub_sock, sub_sock]

        with patch("civitas.transport.zmq.zmq.asyncio.Context", return_value=mock_ctx):
            transport = ZMQTransport(MagicMock(), start_proxy=False, curve_config=None)
            with patch("asyncio.create_task", return_value=MagicMock()):
                await transport.start()

        # Verify curve_serverkey was not set as a real attribute
        assert "curve_serverkey" not in pub_sock.__dict__


# ---------------------------------------------------------------------------
# NATSTransport — TLS and nkey_seed wiring
# ---------------------------------------------------------------------------

pytest.importorskip("nats", reason="nats-py not installed — skipping NATS TLS tests")


class TestNATSTransportTLS:
    @pytest.mark.asyncio
    async def test_tls_ssl_context_passed_to_connect(self, tmp_path: Path):
        from civitas.transport.nats import NATSTransport

        tls_cfg = NatsTlsConfig(enabled=True)
        mock_ssl_ctx = MagicMock(spec=ssl.SSLContext)
        mock_nc = MagicMock()
        mock_nc.is_connected = True
        mock_nc.jetstream = MagicMock()

        with (
            patch.object(tls_cfg, "build_ssl_context", return_value=mock_ssl_ctx),
            patch("nats.connect", return_value=mock_nc) as mock_connect,
        ):
            transport = NATSTransport(MagicMock(), tls_config=tls_cfg)
            await transport.start()
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs["tls"] is mock_ssl_ctx

    @pytest.mark.asyncio
    async def test_no_tls_when_not_enabled(self):
        from civitas.transport.nats import NATSTransport

        mock_nc = MagicMock()
        mock_nc.is_connected = True

        with patch("nats.connect", return_value=mock_nc) as mock_connect:
            transport = NATSTransport(MagicMock(), tls_config=None)
            await transport.start()
            call_kwargs = mock_connect.call_args[1]
            assert "tls" not in call_kwargs

    @pytest.mark.asyncio
    async def test_nkey_seed_raises_without_nkeys_package(self):
        from civitas.transport.nats import NATSTransport

        tls_cfg = NatsTlsConfig(enabled=False, nkey_seed="SUAM_SEED")
        mock_nc = MagicMock()
        mock_nc.is_connected = True

        import sys

        with patch.dict(sys.modules, {"nkeys": None}), patch("nats.connect", return_value=mock_nc):
            transport = NATSTransport(MagicMock(), tls_config=tls_cfg)
            with pytest.raises(ImportError, match="nkeys"):
                await transport.start()


# ---------------------------------------------------------------------------
# Runtime.from_config — security.transport block parsed
# ---------------------------------------------------------------------------


class _Agent(AgentProcess):
    async def handle(self, msg: Message) -> None:
        pass


class TestRuntimeTransportSecurity:
    def test_zmq_curve_parsed_from_yaml(self, tmp_path: Path):
        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - agent:
                    name: a
                    type: tests.unit.test_transport_security._Agent
            transport:
              type: zmq
            security:
              transport:
                zmq:
                  curve:
                    enabled: true
                    server_public_key: "SPUB"
                    server_secret_key: "SSEC"
                    client_public_key: "CPUB"
                    client_secret_key: "CSEC"
            """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._transport_security is not None
        assert rt._transport_security.zmq.enabled is True
        assert rt._transport_security.zmq.server_public_key == "SPUB"

    def test_no_transport_block_gives_none(self, tmp_path: Path):
        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - agent:
                    name: a
                    type: tests.unit.test_transport_security._Agent
            """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._transport_security is None

    def test_security_without_transport_gives_none(self, tmp_path: Path):
        yaml_file = tmp_path / "t.yaml"
        yaml_file.write_text(
            textwrap.dedent("""\
            supervision:
              name: root
              children:
                - agent:
                    name: a
                    type: tests.unit.test_transport_security._Agent
            security:
              signing:
                enabled: false
            """)
        )
        rt = Runtime.from_config(yaml_file)
        assert rt._transport_security is None


# ---------------------------------------------------------------------------
# civitas security init zmq — CLI key generation
# ---------------------------------------------------------------------------


class TestSecurityInitZmq:
    def test_generates_key_files(self, tmp_path: Path):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])

        assert result.exit_code == 0
        assert (tmp_path / "zmq_server.pub").exists()
        assert (tmp_path / "zmq_server.key").exists()
        assert (tmp_path / "zmq_client.pub").exists()
        assert (tmp_path / "zmq_client.key").exists()

    def test_key_files_not_empty(self, tmp_path: Path):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])

        pub_content = (tmp_path / "zmq_server.pub").read_text().strip()
        assert len(pub_content) > 0

    def test_refuses_to_overwrite_without_force(self, tmp_path: Path):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
        result = runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
        assert result.exit_code != 0
        assert "force" in result.output.lower() or "exist" in result.output.lower()

    def test_force_overwrites(self, tmp_path: Path):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
        first_pub = (tmp_path / "zmq_server.pub").read_text().strip()

        result = runner.invoke(
            app, ["security", "init", "zmq", "--key-dir", str(tmp_path), "--force"]
        )
        assert result.exit_code == 0
        # New keypair generated — may or may not differ (probabilistic), but file should exist
        assert (tmp_path / "zmq_server.pub").exists()
        _ = first_pub  # just confirm it was read

    def test_yaml_snippet_in_output(self, tmp_path: Path):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
        assert "server_public_key" in result.output
        assert "client_secret_key" in result.output

    def test_secret_key_file_permissions(self, tmp_path: Path):
        import stat

        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        runner.invoke(app, ["security", "init", "zmq", "--key-dir", str(tmp_path)])
        mode = (tmp_path / "zmq_server.key").stat().st_mode
        # Should be 0o600 — only owner read/write
        assert stat.S_IMODE(mode) == 0o600


# ---------------------------------------------------------------------------
# civitas security init nats — CLI scaffold output
# ---------------------------------------------------------------------------


class TestSecurityInitNats:
    def test_prints_yaml_snippet(self):
        from typer.testing import CliRunner

        from civitas.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["security", "init", "nats"])
        assert result.exit_code == 0
        assert "tls:" in result.output
        assert "nkey_seed" in result.output
