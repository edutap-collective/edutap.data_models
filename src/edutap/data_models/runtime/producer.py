"""How a message leaves a service: the durability options and the send itself.

The consuming half of this runtime has been here since 0.2.0. This is the other one,
and it exists because of a specific failure that the missing half allowed.

``build_headers()`` has been available just as long, and calling it was *optional*.
``lmu_edutap_backend`` publishes to ``pass.command`` without it -- a body with no
envelope. The broker accepts that record, the producer reports success, and every
consumer refuses it on the first mandatory header and parks it. Nothing fails at the
point where somebody would look.

So :func:`publish` does not take a finished header block. It takes the envelope's
fields and builds it, which is the difference between a rule that is written down and
a rule that cannot be broken. The cost is a wide signature; the alternative costs a
silent dead letter queue.

Like the rest of this package it is written against a protocol, so ``aiokafka`` stays
with the service that builds the producer.
"""

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol

from ..messaging import build_headers


class Producer(Protocol):
    """What this runtime needs of a producer: something that publishes and confirms.

    Deliberately narrow. ``send_and_wait`` is the whole surface, and it is the
    *waiting* one -- see :func:`publish`.

    Lives here rather than in :mod:`edutap.data_models.runtime.dlq`, where it was
    first written: the dead letter queue is one caller of a producer, not the reason
    the concept exists. :mod:`~edutap.data_models.runtime` re-exports it unchanged,
    so nothing that imported it before has to move.
    """

    async def send_and_wait(
        self,
        topic: str,
        *,
        key: bytes | None = None,
        value: bytes | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
    ) -> Any:
        """Publish one record and wait for the broker to confirm it."""
        ...


def producer_options(bootstrap_servers: str) -> dict[str, Any]:
    """Return how a producer is configured, before it is built.

    Returned rather than applied inline so a test can assert the exact set without a
    broker -- the same shape the consuming side uses.

    The broker address is passed in rather than read from a settings object: which
    settings class a service has is its own business, and a library that knows one of
    them knows a service.

    ``acks="all"`` is the load-bearing entry. An HTTP endpoint answers its caller on
    the strength of this write; acknowledged by the leader alone means a leader that
    dies before its replicas catch up takes the record with it -- after a person has
    been told their pass exists.

    ``enable_idempotence`` stops the driver's own retry from turning one command into
    two. It is not a substitute for ``edutap-event-id``: that header covers a *caller*
    that sends twice, this covers the transport.
    """
    return {
        "bootstrap_servers": bootstrap_servers,
        "acks": "all",
        "enable_idempotence": True,
    }


async def publish(
    producer: Producer,
    topic: str,
    *,
    key: bytes | None,
    value: bytes,
    producer_name: str,
    schema: str,
    event_id: str,
    occurred_at: datetime,
    action: str | None = None,
    producer_version: str | None = None,
    correlation_id: str | None = None,
) -> None:
    """Send one record with the envelope every eduTAP message carries.

    The envelope is built here, from the arguments, rather than accepted as a
    finished block. That is the point of the function: a caller cannot publish
    without it, and the header block is exactly what the one existing producer
    forgot.

    ``occurred_at`` must be timezone-aware -- :func:`build_headers` raises otherwise,
    and it raises *before* the broker is touched. A record that has been accepted
    cannot be unsent, and a consumer that finds a clockless timestamp has no move
    left but the dead letter topic.

    Failures are not caught. What a lost write means belongs to the caller: in
    ``lmu_edutap_backend`` it means the endpoint fails although the pass already
    exists, which was chosen deliberately over a silently dropped command that would
    leave a pass the database never hears about.

    Args:
        producer: a started producer. Building and stopping it is the service's,
            because that is where the driver lives.
        topic: the full topic name, prefix included. Use
            :meth:`edutap.data_models.settings.KafkaSettings.topic` to build it.
        key: the partition key, or ``None``. Records sharing a key keep their order.
        value: the serialised payload.
        producer_name: this service, for ``edutap-producer``. Named apart from the
            ``producer`` argument on purpose -- one is the client, the other is a
            claim about who is sending, and conflating them reads as if the client
            knew its own name.
        schema: the payload contract, e.g.
            :data:`~edutap.data_models.messaging.SCHEMA_PASS_COMMAND`.
        event_id: the idempotency key. A consumer that sees the same value twice has
            seen the same event twice, so it identifies the *event* rather than the
            attempt -- a retry of one command keeps its id, a second command gets a
            new one.
        occurred_at: when the event happened at the sender. Timezone-aware; see above.
        action: the discriminator where a topic carries several kinds of message.
            Which values a topic allows is the consumer's table, not this function's.
        producer_version: image tag of the sending service. Optional.
        correlation_id: passed through across service boundaries. Optional.

    """
    await producer.send_and_wait(
        topic,
        key=key,
        value=value,
        headers=build_headers(
            producer=producer_name,
            schema=schema,
            event_id=event_id,
            occurred_at=occurred_at,
            action=action,
            producer_version=producer_version,
            correlation_id=correlation_id,
        ),
    )
