#!/usr/bin/env python3
"""Real self-signed CA + server + client leaf certificates for the mTLS
HTTPGateway benchmark variant. Mirrors tests/integration/
test_gateway_http_mtls_direct.py's own cert-generation pattern exactly (a
real SubjectKeyIdentifier/AuthorityKeyIdentifier pair, a KeyUsage extension
asserting keyCertSign/cRLSign on the CA -- the two real gotchas modern
OpenSSL enforces, already found and fixed there).

Usage:
    python benchmarks/gen_certs.py --out-dir /tmp/bench-certs --server-ip 100.82.206.105
"""

from __future__ import annotations

import argparse
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def _rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _key_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )


def _make_ca(common_name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _rsa_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    ski = x509.SubjectKeyIdentifier.from_public_key(key.public_key())
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(ski, critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _make_leaf(
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    common_name: str,
    *,
    ips: list[str] | None = None,
) -> tuple[bytes, bytes]:
    key = _rsa_key()
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "civitas-bench"),
        ]
    )
    now = datetime.datetime.now(datetime.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
    )
    if ips:
        san_entries: list[x509.GeneralName] = [x509.DNSName("localhost")]
        san_entries.extend(x509.IPAddress(ipaddress.ip_address(ip)) for ip in ips)
        builder = builder.add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
    aki = x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key())
    builder = builder.add_extension(aki, critical=False)
    cert = builder.sign(ca_key, hashes.SHA256())
    return _key_pem(key), cert.public_bytes(serialization.Encoding.PEM)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument(
        "--server-ip",
        action="append",
        default=["127.0.0.1"],
        help="Repeatable -- IP SAN(s) for the server leaf cert.",
    )
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    ca_key, ca_cert = _make_ca("civitas-bench-ca")
    (out / "ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))

    server_key, server_cert = _make_leaf(ca_key, ca_cert, "bench-server", ips=args.server_ip)
    (out / "server.key").write_bytes(server_key)
    (out / "server.pem").write_bytes(server_cert)

    client_key, client_cert = _make_leaf(ca_key, ca_cert, "bench-client")
    (out / "client.key").write_bytes(client_key)
    (out / "client.pem").write_bytes(client_cert)

    client_dn = x509.load_pem_x509_certificate(client_cert).subject.rfc4514_string()
    print(f"Certs written to {out}/")
    print(f"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS={client_dn}")


if __name__ == "__main__":
    main()
