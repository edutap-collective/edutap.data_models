"""Settings building blocks that mean the same thing in every eduTAP container.

Mixins rather than a finished settings class: each package keeps its own
``env_prefix`` and inherits only what it needs. The point is that the *field names*
are identical everywhere, so a deployment sets ``<PREFIX>ENVIRONMENT`` and knows what
it did, whichever service it is configuring.

That is not how it grew: today the same idea appears as ``LMU_EDUTAP_SENTRY_DSN_FILE``
(a Docker secret, one service) and as ``GOOGLE_CALLBACK_SENTRY_DSN`` (a plain
variable in the production overlay, another service).

What is **not** here is the error tracker and the trace exporter. They moved to
``edutap.observability_settings``, together with the options that decide what may
leave a process -- options that were chosen against measurements and are worth
nothing separated from the ``sentry_sdk.init()`` call that applies them. What stays
is the one field a service needs whether or not it reports anywhere: which
environment it believes it is running in.
"""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class ServiceSettings(BaseSettings):
    """What every eduTAP service knows about itself, reporting or not."""

    #: Labels every error report and every exported span. Free text rather than a
    #: closed set: ``production``, ``staging`` and ``dev`` are ours, and another
    #: university's naming is not ours to constrain.
    #:
    #: Defaults to ``production`` on purpose. An unset value must not masquerade as
    #: development -- events filed under development are the ones nobody goes
    #: looking for.
    environment: str = "production"

    #: The off switch for tracing, metrics and log export. On by default:
    #: instrumentation that is off unless enabled is instrumentation nobody notices
    #: is broken. *Where* the data goes is decided by the OTLP endpoint, not here;
    #: this exists so a test run or a service flooding the collector can be silenced
    #: deliberately rather than by unsetting the endpoint and hoping.
    telemetry_enabled: bool = True

    #: Verbose or quiet. A closed set, so a misspelled level fails at startup
    #: instead of silently falling back to whatever the logging library defaults to.
    log_level: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = "INFO"


class KafkaSettings(BaseSettings):
    """Broker address, topic prefix and consumer group."""

    #: Comma-separated broker list. One value per deployment, mapped onto each
    #: package's own prefix by the compose file.
    bootstrap_servers: str = "kafka:9092"

    #: Environment prefix of every topic this service touches, e.g.
    #: ``edutap.production``.
    #:
    #: Deliberately without a default: a missing value must abort the start rather
    #: than let the service write quietly into another environment's topics. The
    #: same reasoning as ``webhook_secret`` in ``edutap.webhook_heidi``.
    topic_prefix: str = Field(...)

    #: Consumer group of this service. ``None`` for pure producers.
    consumer_group: str | None = None

    #: Certificate authority the broker's certificate is checked against.
    #:
    #: These three, and the password below, are the client half of mTLS. They are
    #: paths rather than material because that is how an orchestrator delivers a
    #: secret -- a file, mounted read-only, never an environment variable. Under the
    #: shared ``EDUTAP_KAFKA_`` prefix the deployment therefore sets
    #: ``EDUTAP_KAFKA_CA_FILE`` and its two siblings, and means the same thing in
    #: every service.
    #:
    #: All three default to ``None`` together, which is the development case: a
    #: broker without TLS, and nothing claimed. Setting *some* of them is a
    #: deployment error -- see :func:`edutap.data_models.runtime.transport_options`.
    ca_file: Path | None = None

    #: This client's certificate. Its subject is the Kafka principal the broker
    #: authorises, so which certificate is mounted decides what this service may
    #: read and write.
    cert_file: Path | None = None

    #: The private key belonging to :attr:`cert_file`.
    key_file: Path | None = None

    #: Password of an encrypted :attr:`key_file`. Empty means the key is not
    #: encrypted, which is not the same as an empty password -- the distinction is
    #: made where the context is built, not here.
    password: str = ""

    def topic(self, name: str) -> str:
        """Return the full topic name for a logical name.

        >>> KafkaSettings(topic_prefix="edutap.production").topic("pass.state")
        'edutap.production.pass.state'
        """
        return f"{self.topic_prefix}.{name}"
