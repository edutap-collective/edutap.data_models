"""How a client reaches the broker: in the clear, or with a certificate.

One place for it, and the reason is the same one that put the message contract in
this package. Every eduTAP service that speaks to Kafka needs this, the settings
field names are already shared, and TLS setup copied into each service is TLS setup
that drifts -- the copy nobody revisits is the one still verifying nothing.

The context is built from the standard library rather than from
``aiokafka.helpers.create_ssl_context``, and that is deliberate: this package is
imported by every other one, and the Kafka driver stays with the services that build
consumers and producers. The helper is a thin wrapper over ``ssl`` -- a default
client context plus a loaded certificate chain -- so nothing is lost by not using it.
"""

import ssl
from pathlib import Path
from typing import Any

from ..settings import KafkaSettings

#: The three files that have to arrive together, named as the settings field they
#: come from -- an error that says ``key_file`` points at the variable a deployment
#: sets, which ``keyfile`` or ``the private key`` would not.
_REQUIRED = ("ca_file", "cert_file", "key_file")


def transport_options(settings: KafkaSettings) -> dict[str, Any]:
    """Return the transport half of a consumer's or producer's options.

    Merged into the options each service builds::

        return {"bootstrap_servers": ..., **transport_options(settings)}

    Empty when no certificate material is configured. That is the development case
    against a broker without TLS, and the only case in which plaintext is a decision
    rather than an accident.

    Raises:
        ValueError: if only *some* of the three files are configured, or one of them
            is not on disk. Both are deployment errors, and both have to abort the
            start rather than fall back. Against an SSL-only broker a fallback to
            plaintext does not produce an unencrypted connection, it produces one
            that fails during the handshake -- and that error names the broker, not
            the secret nobody mounted.

    """
    configured = {name: getattr(settings, name) for name in _REQUIRED}
    present = {name: path for name, path in configured.items() if path is not None}

    if not present:
        return {}

    missing = [name for name in _REQUIRED if name not in present]
    if missing:
        raise ValueError(
            "Kafka mTLS needs "
            + ", ".join(_REQUIRED)
            + " together. Configured: "
            + ", ".join(sorted(present))
            + "; missing: "
            + ", ".join(missing)
            + "."
        )

    for name, path in present.items():
        _require_readable(name, path)

    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=str(present["ca_file"]),
    )
    # `password=None` and `password=""` are not the same to OpenSSL: the empty string
    # is an empty password, and offering one for a key that carries no encryption is
    # rejected. The settings default is "" because a pydantic-settings field wants a
    # value, so it is translated here rather than there.
    try:
        context.load_cert_chain(
            certfile=str(present["cert_file"]),
            keyfile=str(present["key_file"]),
            password=settings.password or None,
        )
    except ssl.SSLError as error:
        # OpenSSL says `[SSL] PEM lib`, and that is all it says -- for a wrong key
        # password, a key that does not belong to the certificate, and a file that
        # holds something other than PEM alike. Raised as it comes, it is the kind
        # of message that sends somebody looking at the broker.
        raise ValueError(
            f"Kafka mTLS: cert_file {present['cert_file']} and key_file "
            f"{present['key_file']} could not be loaded ({error}). Usual causes: a "
            "wrong or missing password for an encrypted key, a key that does not "
            "belong to the certificate, or a file that is not PEM."
        ) from error

    return {"security_protocol": "SSL", "ssl_context": context}


def _require_readable(name: str, path: Path) -> None:
    """Fail with the field name attached, before ``ssl`` fails without it."""
    if not path.is_file():
        raise ValueError(
            f"Kafka mTLS: {name} is set to {path}, which is not a file. "
            "In a container this is usually a secret that was declared but not "
            "mounted, or mounted under a different name."
        )
