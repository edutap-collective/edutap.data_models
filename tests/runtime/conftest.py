"""Throwaway certificate material for the transport tests.

Generated per test rather than committed as fixture files. A certificate in the
repository expires, and the test that depends on it fails one morning for a reason
that has nothing to do with the change that triggered the run.

``cryptography`` is a test-only dependency for exactly this. It is deliberately not a
runtime one: this package is imported by every other eduTAP package, and what it
pulls in they all pull in -- the rule written down in ``pyproject.toml``.
"""

import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.x509.oid import NameOID

KEY_PASSWORD = "test-key-password"


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _write_material(tmp_path, *, password: str | None) -> dict:
    """Write a self-signed CA and a client certificate it signed."""
    now = datetime.datetime.now(datetime.UTC)

    ca_key = ed25519.Ed25519PrivateKey.generate()
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("edutap-test-ca"))
        .issuer_name(_name("edutap-test-ca"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, None)
    )

    client_key = ed25519.Ed25519PrivateKey.generate()
    client_cert = (
        x509.CertificateBuilder()
        .subject_name(_name("edutap-test-client"))
        .issuer_name(ca_cert.subject)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(ca_key, None)
    )

    encryption: serialization.KeySerializationEncryption = (
        serialization.BestAvailableEncryption(password.encode())
        if password
        else serialization.NoEncryption()
    )

    ca_file = tmp_path / "ca.pem"
    cert_file = tmp_path / "client.pem"
    key_file = tmp_path / "client.key"

    ca_file.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_file.write_bytes(client_cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        client_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=encryption,
        )
    )

    return {"ca_file": ca_file, "cert_file": cert_file, "key_file": key_file}


@pytest.fixture
def client_material(tmp_path) -> dict:
    """CA, client certificate and an unencrypted key, as settings fields."""
    return _write_material(tmp_path, password=None)


@pytest.fixture
def encrypted_client_material(tmp_path) -> dict:
    """The same, with the key encrypted under :func:`key_password`."""
    return _write_material(tmp_path, password=KEY_PASSWORD)


@pytest.fixture
def key_password() -> str:
    """The password the encrypted key was written with.

    A fixture rather than an import from this module: without ``__init__.py`` pytest
    imports each test file as a top-level module, and a relative import out of it has
    no parent package to resolve against.
    """
    return KEY_PASSWORD


# Ed25519 rather than RSA: the key generation is what a test of this kind spends its
# time on, and an RSA-2048 keypair per test adds up across the suite. What is being
# tested is the wiring of paths into an SSL context, not the algorithm.
