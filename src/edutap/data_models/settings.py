"""Settings building blocks that mean the same thing in every eduTAP container.

Mixins rather than a finished settings class: each package keeps its own
``env_prefix`` and inherits only what it needs. The point is that the *field names*
are identical everywhere, so a deployment sets ``<PREFIX>SENTRY_DSN`` and knows what
it did, whichever service it is configuring.

That is not how it grew: today the same idea appears as ``LMU_EDUTAP_SENTRY_DSN_FILE``
(a Docker secret, one service) and as ``GOOGLE_CALLBACK_SENTRY_DSN`` (a plain
variable in the production overlay, another service).
"""

from pydantic import Field
from pydantic_settings import BaseSettings


class SentrySettings(BaseSettings):
    """Error tracking, one project per service."""

    #: A DSN identifies one project, so every service gets its own rather than
    #: sharing one. It is a write-only ingest key and thus low-sensitivity: a plain
    #: environment variable is enough, a Docker secret would be effort without gain.
    #: Empty means the integration stays off.
    sentry_dsn: str = ""

    #: Environment name reported alongside every event. Without it, staging and
    #: production errors land in one undifferentiated stream.
    sentry_environment: str = ""


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

    def topic(self, name: str) -> str:
        """Return the full topic name for a logical name.

        >>> KafkaSettings(topic_prefix="edutap.production").topic("pass.state")
        'edutap.production.pass.state'
        """
        return f"{self.topic_prefix}.{name}"
