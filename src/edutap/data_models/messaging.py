"""The Kafka message contract: header block, key rule and logical topic names.

Six packages produce or consume on the same bus. This module exists so the contract
is written once rather than six times -- six copies of a header block diverge, and
the divergence is invisible until a consumer silently stops recognising a message.

See the Kafka topic schema design record for why the names look as they do.
"""

from collections.abc import Iterable
from datetime import UTC, datetime

#: The producing service, e.g. ``apple_wallet_vas_web_service``.
#:
#: Set by the client and therefore **not** an authentication: it says what a service
#: claims. What can be relied on is who was allowed to write to the topic at all.
HEADER_PRODUCER = "edutap-producer"

#: Contract and version of the payload, e.g. ``pass-state/v1``.
HEADER_SCHEMA = "edutap-schema"

#: Idempotency key. A consumer that sees the same value twice has seen the same
#: event twice.
HEADER_EVENT_ID = "edutap-event-id"

#: When the event happened **at the sender**, RFC 3339.
#:
#: Load-bearing rather than decorative: the ``pass_state`` watermark compares against
#: it, so it has to stay stable across every retry. As a header it can be read
#: without deserialising the body.
HEADER_OCCURRED_AT = "edutap-occurred-at"

#: The discriminator where one topic carries several kinds of message:
#: ``create`` | ``update`` | ``deactivate`` on ``pass.command``,
#: ``registered`` | ``unregistered`` on ``device.registration``.
HEADER_ACTION = "edutap-action"

#: Image tag of the producing service. Optional.
HEADER_PRODUCER_VERSION = "edutap-producer-version"

#: Passed through across service boundaries. Optional.
HEADER_CORRELATION_ID = "edutap-correlation-id"

#: Logical topic names, without the environment prefix.
#:
#: The prefix is configuration (see :class:`edutap.data_models.settings.KafkaSettings`);
#: what a topic *is* belongs here. Three dimensions: ``person`` is the university's
#: internal path, ``pass`` the pass lifecycle, ``device`` the layer between a pass and
#: the devices holding it.
TOPIC_PERSON = "person"
TOPIC_PASS_COMMAND = "pass.command"
TOPIC_PASS_STATE = "pass.state"
TOPIC_PASS_LOG = "pass.log"
TOPIC_DEVICE_NOTIFY = "device.notify"
TOPIC_DEVICE_REGISTRATION = "device.registration"

#: Suffix of the dead letter queue belonging to an input topic.
DLQ_SUFFIX = "dlq"


def dlq_of(topic: str) -> str:
    """Return the dead letter topic belonging to ``topic``.

    One DLQ per *input topic*, not per source: several callback services feeding
    ``pass.state`` still share one ``pass.state.dlq``, and which of them wrote a
    given record is in its ``edutap-producer`` header.
    """
    return f"{topic}.{DLQ_SUFFIX}"


def build_headers(
    *,
    producer: str,
    schema: str,
    event_id: str,
    occurred_at: datetime,
    action: str | None = None,
    producer_version: str | None = None,
    correlation_id: str | None = None,
) -> list[tuple[str, bytes]]:
    """Build the header block every eduTAP message carries.

    Returns the shape ``aiokafka`` expects: a list of ``(name, bytes)`` pairs.

    ``occurred_at`` must be timezone-aware. A naive timestamp would be rendered
    without an offset, and the watermark comparison on the consumer side would then
    silently compare values from different clocks.
    """
    if occurred_at.tzinfo is None:
        raise ValueError("occurred_at must be timezone-aware")

    pairs: list[tuple[str, str]] = [
        (HEADER_PRODUCER, producer),
        (HEADER_SCHEMA, schema),
        (HEADER_EVENT_ID, event_id),
        (HEADER_OCCURRED_AT, occurred_at.astimezone(UTC).isoformat()),
    ]
    for name, value in (
        (HEADER_ACTION, action),
        (HEADER_PRODUCER_VERSION, producer_version),
        (HEADER_CORRELATION_ID, correlation_id),
    ):
        if value is not None:
            pairs.append((name, value))
    return [(name, value.encode()) for name, value in pairs]


def read_header(headers: Iterable[tuple[str, bytes]], name: str) -> str | None:
    """Return one header value as text, or ``None`` if it is absent."""
    for key, value in headers:
        if key == name:
            return value.decode()
    return None
